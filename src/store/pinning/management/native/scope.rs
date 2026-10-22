//! Typed, indexed memberships. Returned originals include private cells and NULL.
//! These reads prove database equality, not request authorization.
use super::{
    ManagementError, NativeKey, NativeRow, NativeTable, Result,
    meter::{Budget, CELL_SLICE, PAGE_BYTES, PAGE_ROWS, ResidentGuard},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Selector {
    Exact(NativeKey),
    ExactVersion {
        id: String,
    },
    ObjectVersionOwner {
        bucket: String,
        key: String,
        object_id: String,
    },
    PublicVersionBinding {
        bucket: String,
        key: String,
        version: Option<String>,
    },
    VersionConstraints {
        version_row_id: String,
    },
    OwnerLeases {
        object_id: String,
    },
    LeaseTargets {
        lease_id: String,
    },
    LeaseRequirements {
        lease_id: String,
    },
    ResourceFamily {
        provider: String,
        cid: String,
    },
    JobAttempts {
        job_id: String,
    },
    ManifestBindings {
        version_row_id: String,
    },
    Route {
        provider: String,
    },
    RouteDomain {
        allocation_domain: String,
    },
    CanonicalResourceFamily {
        allocation_domain: String,
        cid: String,
    },
    AuthoritySet {
        selectors: Vec<super::super::records::locator::AuthoritySelector>,
    },
    RecordMembers {
        database_scope: String,
        plan_id: String,
    },
    LegacyOperationMembers {
        operation_id: String,
    },
    LegacyReversals {
        operation_id: String,
    },
    RootReference {
        batch_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeSpec {
    catalog_revision: String,
    selectors: BTreeSet<Selector>,
}
impl ScopeSpec {
    pub fn new(selectors: Vec<Selector>) -> Result<Self> {
        if selectors.len() > 128 {
            return Err(ManagementError::Budget);
        }
        Ok(Self {
            catalog_revision: super::super::schema::VERSION.into(),
            selectors: selectors.into_iter().collect(),
        })
    }
    pub fn selectors(&self) -> &BTreeSet<Selector> {
        &self.selectors
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Membership {
    pub selector: Selector,
    pub keys: Vec<NativeKey>,
}

/// Public equality evidence, not imported authority. Empty memberships remain
/// explicit and are re-executed during the final transaction fence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeEvidence {
    pub spec: ScopeSpec,
    pub memberships: Vec<Membership>,
    pub authority: Vec<super::super::records::locator::AuthorityProof>,
    pub operational_keys: Vec<(String, Vec<String>)>,
    pub projection_only: bool,
}
impl ScopeEvidence {
    pub(super) fn validate(&self, frontier: &[NativeKey], versions: &[String]) -> Result<()> {
        if versions.last() != Some(&self.spec.catalog_revision)
            || !self
                .spec
                .selectors
                .iter()
                .eq(self.memberships.iter().map(|m| &m.selector))
            || self
                .memberships
                .iter()
                .any(|m| m.keys.windows(2).any(|pair| pair[0] >= pair[1]))
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let keys: BTreeSet<_> = self.memberships.iter().flat_map(|m| &m.keys).collect();
        if keys != frontier.iter().collect() {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(())
    }
}

pub struct ScopedRead {
    spec: ScopeSpec,
    rows: BTreeMap<NativeKey, NativeRow>,
    memberships: BTreeMap<Selector, Vec<NativeKey>>,
    pub database_scope: String,
    pub database_identity: String,
    _resident: Vec<ResidentGuard>,
    authority: Vec<super::super::records::locator::AuthorityProof>,
    operational_keys: BTreeMap<String, BTreeSet<String>>,
}
impl ScopedRead {
    pub fn rows(&self) -> &BTreeMap<NativeKey, NativeRow> {
        &self.rows
    }
    pub(crate) fn release_rows(&mut self) {
        self.rows.clear();
        self._resident.clear();
    }
    pub fn memberships(&self) -> &BTreeMap<Selector, Vec<NativeKey>> {
        &self.memberships
    }
    pub fn spec(&self) -> &ScopeSpec {
        &self.spec
    }
    pub fn authority_proofs(&self) -> &[super::super::records::locator::AuthorityProof] {
        &self.authority
    }
    pub fn operational_keys(&self, domain: &str) -> Option<&BTreeSet<String>> {
        self.operational_keys.get(domain)
    }

    /// Consumes this store-produced read; never deserializes or recaptures cells.
    /// The resulting frame is tagged scoped and retains private SQL NULL/text.
    pub fn into_native_frame(self, budget: &mut Budget) -> Result<super::NativeFrame> {
        budget.check()?;
        let mut frame = super::NativeFrame {
            schema_versions: super::history::current_versions(),
            database_scope: self.database_scope,
            database_identity: self.database_identity,
            tables: super::current_catalog().to_vec(),
            frontier: Vec::with_capacity(self.rows.len()),
            private: Vec::new(),
            scoped: Some(ScopeEvidence {
                spec: self.spec,
                memberships: self
                    .memberships
                    .into_iter()
                    .map(|(selector, keys)| Membership { selector, keys })
                    .collect(),
                authority: self.authority,
                operational_keys: self
                    .operational_keys
                    .into_iter()
                    .map(|(domain, keys)| (domain, keys.into_iter().collect()))
                    .collect(),
                projection_only: false,
            }),
            _resident: self
                ._resident
                .into_iter()
                .map(std::sync::Arc::new)
                .collect(),
        };
        for (key, original) in self.rows {
            let table = frame
                .tables
                .iter_mut()
                .find(|table| table.table == key.table)
                .ok_or(ManagementError::Schema)?;
            let mut public = NativeRow::new();
            let mut hidden = NativeRow::new();
            for (column, cell) in original {
                if super::confidential(&key.table, &column) {
                    public.insert(column.clone(), None);
                    hidden.insert(column, cell);
                } else {
                    public.insert(column, cell);
                }
            }
            if !hidden.is_empty() {
                frame
                    .private
                    .push((key.clone(), std::sync::Arc::new(hidden)));
            }
            frame.frontier.push(key);
            table.rows.push(public);
        }
        frame.validate()?;
        Ok(frame)
    }
}

pub(super) struct Query {
    pub table: &'static NativeTable,
    pub predicate: String,
    pub values: Vec<Value>,
}
pub(super) fn table(name: &str) -> Result<&'static NativeTable> {
    super::current_catalog()
        .iter()
        .find(|t| t.table == name)
        .ok_or(ManagementError::Schema)
}
pub(crate) fn bound_value(table: &NativeTable, column: &str, value: &str) -> Result<Value> {
    let definition = table
        .definitions
        .iter()
        .find(|c| c.name == column)
        .ok_or(ManagementError::Schema)?;
    match definition.postgres_type.as_str() {
        "bigint" => Ok(value
            .parse::<i64>()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .into()),
        "integer" => Ok(value
            .parse::<i32>()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .into()),
        "boolean" => Ok(match value {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => return Err(ManagementError::InvalidEvidence),
        }
        .into()),
        "text" => Ok(value.into()),
        _ => Err(ManagementError::InvalidEvidence),
    }
}
fn query(name: &str, terms: &[(&str, Option<&str>)]) -> Result<Query> {
    let table = table(name)?;
    let mut values = Vec::new();
    let mut predicates = Vec::new();
    for (column, value) in terms {
        if !table.columns.iter().any(|c| c == column) {
            return Err(ManagementError::Schema);
        }
        predicates.push(if let Some(value) = value {
            values.push(bound_value(table, column, value)?);
            format!("\"{column}\"=${}", values.len())
        } else {
            format!("\"{column}\" IS NULL")
        });
    }
    Ok(Query {
        table,
        predicate: predicates.join(" AND "),
        values,
    })
}
pub(super) fn queries(selector: &Selector) -> Result<Vec<Query>> {
    let one = match selector {
        Selector::Exact(key) => {
            let t = table(&key.table)?;
            if t.primary_key.len() != key.values.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            query(
                &key.table,
                &t.primary_key
                    .iter()
                    .zip(&key.values)
                    .map(|(c, v)| (c.as_str(), Some(v.as_str())))
                    .collect::<Vec<_>>(),
            )?
        }
        Selector::ExactVersion { id } => query("object_versions", &[("id", Some(id))])?,
        Selector::ObjectVersionOwner {
            bucket,
            key,
            object_id,
        } => query(
            "object_versions",
            &[
                ("bucket", Some(bucket)),
                ("key", Some(key)),
                ("object_id", Some(object_id)),
            ],
        )?,
        Selector::PublicVersionBinding {
            bucket,
            key,
            version,
        } => query(
            "object_versions",
            &[
                ("bucket", Some(bucket)),
                ("key", Some(key)),
                ("version_id", version.as_deref()),
            ],
        )?,
        Selector::VersionConstraints { version_row_id } => {
            return Ok(vec![
                query(
                    "lifecycle_actions",
                    &[("target_version_row_id", Some(version_row_id))],
                )?,
                query(
                    "lifecycle_transitions",
                    &[("target_version_row_id", Some(version_row_id))],
                )?,
                query(
                    "residency_references",
                    &[("version_row_id", Some(version_row_id))],
                )?,
            ]);
        }
        Selector::OwnerLeases { object_id } => {
            query("pin_leases", &[("owner_object_id", Some(object_id))])?
        }
        Selector::LeaseTargets { lease_id } => {
            query("pin_lease_targets", &[("lease_id", Some(lease_id))])?
        }
        Selector::LeaseRequirements { lease_id } => {
            query("pin_lease_requirements", &[("lease_id", Some(lease_id))])?
        }
        Selector::ResourceFamily { provider, cid } => {
            return [
                "remote_pins",
                "remote_pin_ledger",
                "pin_resource_history",
                "pin_jobs",
                "pin_lease_targets",
                "pin_submit_observations",
                "pin_replica_observations",
            ]
            .iter()
            .map(|name| {
                let cid_column = if matches!(
                    *name,
                    "pin_submit_observations" | "pin_replica_observations"
                ) {
                    "expected_cid"
                } else {
                    "cid"
                };
                query(
                    name,
                    &[("provider", Some(provider)), (cid_column, Some(cid))],
                )
            })
            .collect();
        }
        Selector::JobAttempts { job_id } => {
            query("pin_submit_history", &[("job_id", Some(job_id))])?
        }
        Selector::ManifestBindings { version_row_id } => query(
            "zip_manifest_entries",
            &[("version_row_id", Some(version_row_id))],
        )?,
        Selector::Route { provider } => {
            query("pin_provider_routes", &[("provider", Some(provider))])?
        }
        Selector::RouteDomain { .. }
        | Selector::CanonicalResourceFamily { .. }
        | Selector::AuthoritySet { .. } => return Err(ManagementError::InvalidEvidence),
        Selector::RecordMembers {
            database_scope,
            plan_id,
        } => query(
            "pin_management_records",
            &[
                ("database_scope", Some(database_scope)),
                ("plan_id", Some(plan_id)),
            ],
        )?,
        Selector::LegacyOperationMembers { operation_id } => query(
            "pin_legacy_migration_items",
            &[("operation_id", Some(operation_id))],
        )?,
        Selector::LegacyReversals { operation_id } => {
            query("pin_legacy_migrations", &[("reverses", Some(operation_id))])?
        }
        Selector::RootReference { batch_id } => {
            query("zip_root_references", &[("batch_id", Some(batch_id))])?
        }
    };
    Ok(vec![one])
}

pub async fn writer_boundary(tx: &DatabaseTransaction) -> Result<()> {
    super::super::admission::lock_sqlite_writer_in_caller_transaction(tx)
        .await
        .map_err(|_| ManagementError::Database)
}

/// B's ordered business row locks must precede this tail phantom fence on PG.
pub async fn fence_memberships(tx: &DatabaseTransaction, spec: &ScopeSpec) -> Result<()> {
    writer_boundary(tx).await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        let mut tables = BTreeSet::<String>::new();
        for selector in &spec.selectors {
            if matches!(
                selector,
                Selector::RouteDomain { .. }
                    | Selector::CanonicalResourceFamily { .. }
                    | Selector::AuthoritySet { .. }
            ) {
                tables.extend(
                    super::super::schema::storage::SOURCE_TABLES
                        .iter()
                        .map(|s| (*s).to_owned()),
                );
                tables.extend(
                    [
                        "pin_management_locator",
                        "pin_management_source_control",
                        "pin_management_pending_scope",
                    ]
                    .into_iter()
                    .map(str::to_owned),
                );
                if matches!(selector, Selector::CanonicalResourceFamily { .. }) {
                    for q in queries(&Selector::ResourceFamily {
                        provider: String::new(),
                        cid: String::new(),
                    })? {
                        tables.insert(q.table.table.clone());
                    }
                }
            } else {
                for q in queries(selector)? {
                    tables.insert(q.table.table.clone());
                }
            }
        }
        for table in tables {
            tx.execute_unprepared(&format!("LOCK TABLE \"{table}\" IN SHARE MODE NOWAIT"))
                .await
                .map_err(super::super::nowait_error)?;
        }
    }
    Ok(())
}

pub async fn collect_scope<C: ConnectionTrait>(
    tx: &C,
    spec: &ScopeSpec,
    budget: &mut Budget,
) -> Result<ScopedRead> {
    if spec.catalog_revision != super::super::schema::VERSION {
        return Err(ManagementError::Schema);
    }
    if !crate::store::initialization::read_context::active(tx).await? {
        super::super::schema::validate(tx).await?;
    }
    super::super::catalog::validate(tx, super::current_catalog()).await?;
    let (database_scope, database_identity) = binding(tx).await?;
    let mut read = ScopedRead {
        spec: spec.clone(),
        rows: BTreeMap::new(),
        memberships: BTreeMap::new(),
        database_scope,
        database_identity,
        _resident: Vec::new(),
        authority: Vec::new(),
        operational_keys: BTreeMap::new(),
    };
    let mut domain_sources = BTreeMap::<String, Vec<NativeKey>>::new();
    for selector in &spec.selectors {
        let mut members = Vec::new();
        let selected_queries = match selector {
            Selector::AuthoritySet { selectors } => {
                let view =
                    super::super::records::locator::discover_authority(tx, selectors, budget)
                        .await?;
                for selector in selectors {
                    if let super::super::records::locator::AuthoritySelector::Domain(domain)
                    | super::super::records::locator::AuthoritySelector::RouteDomain(domain) =
                        selector
                    {
                        read.operational_keys
                            .insert(domain.clone(), view.operational_keys(domain));
                    }
                }
                let (rows, proof, resident) = view.into_scope_parts();
                for (key, row) in rows {
                    members.push(key.clone());
                    read.rows.insert(key, row);
                }
                read.authority.push(proof);
                read._resident.extend(resident);
                Vec::new()
            }
            Selector::RouteDomain { allocation_domain }
            | Selector::CanonicalResourceFamily {
                allocation_domain, ..
            } => {
                use super::super::records::locator::{AuthoritySelector, discover_authority};
                if let Some(keys) = domain_sources.get(allocation_domain) {
                    members.extend(keys.iter().cloned());
                } else {
                    let view = discover_authority(
                        tx,
                        &[AuthoritySelector::RouteDomain(allocation_domain.clone())],
                        budget,
                    )
                    .await?;
                    let mut operational = view.operational_keys(allocation_domain);
                    operational.insert(allocation_domain.clone());
                    read.operational_keys
                        .insert(allocation_domain.clone(), operational);
                    let (rows, proof, resident) = view.into_scope_parts();
                    domain_sources
                        .insert(allocation_domain.clone(), rows.keys().cloned().collect());
                    for (key, row) in rows {
                        members.push(key.clone());
                        read.rows.insert(key, row);
                    }
                    read.authority.push(proof);
                    read._resident.extend(resident);
                }
                let operational = read
                    .operational_keys
                    .get(allocation_domain)
                    .ok_or(ManagementError::InvalidEvidence)?;
                let mut selected = Vec::new();
                for provider in operational {
                    selected.extend(queries(&Selector::Route {
                        provider: provider.clone(),
                    })?);
                    if let Selector::CanonicalResourceFamily { cid, .. } = selector {
                        selected.extend(queries(&Selector::ResourceFamily {
                            provider: provider.clone(),
                            cid: cid.clone(),
                        })?);
                    }
                }
                selected
            }
            _ => queries(selector)?,
        };
        for q in selected_queries {
            let mut after = None;
            loop {
                let page =
                    read_page_reusing(tx, &q, after.as_ref(), budget, Some(&read.rows)).await?;
                let eof = page.eof;
                after = page.last.clone();
                members.extend(page.reused);
                for (key, row) in page.rows {
                    members.push(key.clone());
                    read.rows.insert(key, row);
                }
                read._resident.extend(page.resident);
                if eof {
                    break;
                }
            }
        }
        members.sort();
        members.dedup();
        read.memberships.insert(selector.clone(), members);
    }
    budget.check()?;
    read.authority = normalize_proofs(read.authority)?;
    Ok(read)
}

pub async fn revalidate_scope(
    tx: &DatabaseTransaction,
    reviewed: &ScopedRead,
    budget: &mut Budget,
) -> Result<ScopedRead> {
    revalidate_scope_originals(tx, reviewed, &reviewed.rows, budget).await
}

pub(crate) async fn revalidate_scope_originals(
    tx: &DatabaseTransaction,
    reviewed: &ScopedRead,
    originals: &BTreeMap<NativeKey, NativeRow>,
    budget: &mut Budget,
) -> Result<ScopedRead> {
    fence_memberships(tx, &reviewed.spec).await?;
    let fresh = collect_scope(tx, &reviewed.spec, budget).await?;
    if fresh
        .rows
        .iter()
        .any(|(key, row)| originals.get(key) != Some(row))
        || fresh.memberships != reviewed.memberships
        || fresh.database_scope != reviewed.database_scope
        || fresh.database_identity != reviewed.database_identity
        || fresh.authority != reviewed.authority
        || fresh.operational_keys != reviewed.operational_keys
    {
        return Err(ManagementError::Changed);
    }
    Ok(fresh)
}

pub async fn revalidate_frame_scope(
    tx: &DatabaseTransaction,
    reviewed: &super::NativeFrame,
    budget: &mut Budget,
) -> Result<super::NativeFrame> {
    let evidence = reviewed
        .scope_evidence()
        .ok_or(ManagementError::InvalidEvidence)?;
    if evidence.projection_only {
        return Err(ManagementError::InvalidEvidence);
    }
    fence_memberships(tx, &evidence.spec).await?;
    check_frame_scope(tx, reviewed, budget).await
}

/// Read-only equality check. A writer must already hold the membership fences.
pub(crate) async fn check_frame_scope<C: ConnectionTrait>(
    tx: &C,
    reviewed: &super::NativeFrame,
    budget: &mut Budget,
) -> Result<super::NativeFrame> {
    let evidence = reviewed
        .scope_evidence()
        .ok_or(ManagementError::InvalidEvidence)?;
    if evidence.projection_only {
        return Err(ManagementError::InvalidEvidence);
    }
    let fresh = collect_scope(tx, &evidence.spec, budget)
        .await?
        .into_native_frame(budget)?;
    if fresh != *reviewed {
        return Err(ManagementError::Changed);
    }
    Ok(fresh)
}

fn normalize_proofs(
    proofs: Vec<super::super::records::locator::AuthorityProof>,
) -> Result<Vec<super::super::records::locator::AuthorityProof>> {
    use super::super::records::locator::{AuthorityMembership, AuthorityProof};
    if proofs.is_empty() {
        return Ok(Vec::new());
    }
    let mut memberships = BTreeMap::new();
    let mut seals = BTreeMap::new();
    for proof in proofs {
        for member in proof.memberships {
            if memberships
                .get(&member.selector)
                .is_some_and(|old| old != &member.sources)
            {
                return Err(ManagementError::Changed);
            }
            memberships.insert(member.selector, member.sources);
        }
        for seal in proof.seals {
            if seals.get(&seal.source).is_some_and(|old| old != &seal) {
                return Err(ManagementError::Changed);
            }
            seals.insert(seal.source.clone(), seal);
        }
    }
    Ok(vec![AuthorityProof {
        memberships: memberships
            .into_iter()
            .map(|(selector, sources)| AuthorityMembership { selector, sources })
            .collect(),
        seals: seals.into_values().collect(),
    }])
}

fn merge_rows(
    read: &mut ScopedRead,
    rows: &BTreeMap<NativeKey, NativeRow>,
    budget: &mut Budget,
) -> Result<()> {
    for (key, row) in rows {
        if let Some(old) = read.rows.get(key) {
            if old != row {
                return Err(ManagementError::Changed);
            }
            continue;
        }
        let bytes = row
            .iter()
            .map(|(c, v)| c.len() + v.as_ref().map_or(0, String::len) + 128)
            .sum::<usize>();
        read._resident.push(budget.reserve(bytes * 2)?);
        budget.work.copied_bytes = budget
            .work
            .copied_bytes
            .checked_add(bytes as u64)
            .ok_or(ManagementError::Budget)?;
        read.rows.insert(key.clone(), row.clone());
    }
    Ok(())
}

pub(super) fn merge_reads(
    reads: &[ScopedRead],
    authorities: &[super::super::records::locator::AuthorityView],
    budget: &mut Budget,
) -> Result<super::NativeFrame> {
    merge_reads_originals(reads, authorities, None, budget)
}

pub(crate) fn merge_reads_originals(
    reads: &[ScopedRead],
    authorities: &[super::super::records::locator::AuthorityView],
    originals: Option<&BTreeMap<NativeKey, NativeRow>>,
    budget: &mut Budget,
) -> Result<super::NativeFrame> {
    let binding = reads
        .first()
        .map(|r| (&r.database_scope, &r.database_identity))
        .or_else(|| {
            authorities
                .first()
                .map(|r| (&r.database_scope, &r.database_identity))
        })
        .ok_or(ManagementError::InvalidEvidence)?;
    let mut merged = ScopedRead {
        spec: ScopeSpec {
            catalog_revision: super::super::schema::VERSION.into(),
            selectors: BTreeSet::new(),
        },
        rows: BTreeMap::new(),
        memberships: BTreeMap::new(),
        database_scope: binding.0.clone(),
        database_identity: binding.1.clone(),
        _resident: Vec::new(),
        authority: Vec::new(),
        operational_keys: BTreeMap::new(),
    };
    if let Some(originals) = originals {
        merge_rows(&mut merged, originals, budget)?;
    }
    for read in reads {
        if read.database_scope != merged.database_scope
            || read.database_identity != merged.database_identity
        {
            return Err(ManagementError::Changed);
        }
        merge_rows(&mut merged, &read.rows, budget)?;
        merged
            .spec
            .selectors
            .extend(read.spec.selectors.iter().cloned());
        for (selector, keys) in &read.memberships {
            if merged
                .memberships
                .get(selector)
                .is_some_and(|old| old != keys)
            {
                return Err(ManagementError::Changed);
            }
            merged.memberships.insert(selector.clone(), keys.clone());
        }
        merged.authority.extend(read.authority.clone());
        for (domain, keys) in &read.operational_keys {
            if merged
                .operational_keys
                .get(domain)
                .is_some_and(|old| old != keys)
            {
                return Err(ManagementError::Changed);
            }
            merged.operational_keys.insert(domain.clone(), keys.clone());
        }
    }
    for authority in authorities {
        if authority.database_scope != merged.database_scope
            || authority.database_identity != merged.database_identity
        {
            return Err(ManagementError::Changed);
        }
        merge_rows(&mut merged, &authority.originals, budget)?;
        let selector = Selector::AuthoritySet {
            selectors: authority.requested_selectors().to_vec(),
        };
        merged.spec.selectors.insert(selector.clone());
        merged
            .memberships
            .insert(selector, authority.originals.keys().cloned().collect());
        merged.authority.push(authority.proof.clone());
        for selector in authority.requested_selectors() {
            if let super::super::records::locator::AuthoritySelector::Domain(domain)
            | super::super::records::locator::AuthoritySelector::RouteDomain(domain) = selector
            {
                merged
                    .operational_keys
                    .insert(domain.clone(), authority.operational_keys(domain));
            }
        }
    }
    merged.authority = normalize_proofs(merged.authority)?;
    merged.into_native_frame(budget)
}

pub async fn binding<C: ConnectionTrait>(db: &C) -> Result<(String, String)> {
    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT database_scope FROM pin_management_records WHERE kind='database_scope' LIMIT 2",
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if rows.len() != 1 {
        return Err(ManagementError::Schema);
    }
    Ok((
        rows[0]
            .try_get("", "database_scope")
            .map_err(|_| ManagementError::Schema)?,
        super::database_identity(db).await?,
    ))
}

/// Internal exact original lookup after the outer reader validated the catalog.
pub(crate) async fn read_exact<C: ConnectionTrait>(
    tx: &C,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<(NativeRow, Vec<ResidentGuard>)> {
    let q = queries(&Selector::Exact(key.clone()))?.remove(0);
    let mut page = read_page(tx, &q, None, budget).await?;
    if !page.eof || page.rows.len() != 1 {
        return Err(ManagementError::InvalidEvidence);
    }
    let (actual, row) = page.rows.remove(0);
    if actual != *key {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok((row, page.resident))
}

pub(super) struct Page {
    pub rows: Vec<(NativeKey, NativeRow)>,
    reused: Vec<NativeKey>,
    pub resident: Vec<ResidentGuard>,
    pub last: Option<NativeKey>,
    pub eof: bool,
}
pub(super) fn octets(backend: DatabaseBackend, column: &str) -> String {
    if backend == DatabaseBackend::Sqlite {
        format!("CAST(\"{column}\" AS BLOB)")
    } else {
        format!("convert_to(CAST(\"{column}\" AS TEXT),'UTF8')")
    }
}

pub(super) async fn read_page<C: ConnectionTrait>(
    db: &C,
    q: &Query,
    after: Option<&NativeKey>,
    budget: &mut Budget,
) -> Result<Page> {
    read_page_reusing(db, q, after, budget, None).await
}

async fn read_page_reusing<C: ConnectionTrait>(
    db: &C,
    q: &Query,
    after: Option<&NativeKey>,
    budget: &mut Budget,
    originals: Option<&BTreeMap<NativeKey, NativeRow>>,
) -> Result<Page> {
    let backend = db.get_database_backend();
    let upgrade = crate::store::initialization::read_context::active(db).await?;
    let t = q.table;
    let mut predicate = q.predicate.clone();
    let mut values = q.values.clone();
    if let Some(after) = after {
        if after.table != t.table || after.values.len() != t.primary_key.len() {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut slots = Vec::new();
        for (column, value) in t.primary_key.iter().zip(&after.values) {
            values.push(bound_value(t, column, value)?);
            slots.push(format!("${}", values.len()));
        }
        predicate.push_str(&format!(
            " AND ({}) > ({})",
            t.primary_key
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(","),
            slots.join(",")
        ));
    }
    let order = t
        .primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let mut projection = t
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("CAST(length({}) AS BIGINT) AS n{i}", octets(backend, c)))
        .collect::<Vec<_>>();
    projection.extend(t.primary_key.iter().enumerate().map(|(i, c)| {
        format!(
            "CASE WHEN length({})<=1024 THEN CAST(\"{c}\" AS TEXT) END AS k{i}",
            octets(backend, c)
        )
    }));
    budget.charge(1, 0, 0, 0)?;
    let _metadata =
        budget.reserve(PAGE_ROWS * (t.columns.len() * 32 + t.primary_key.len() * 1024))?;
    let metadata = db
        .query_all(Statement::from_sql_and_values(
            backend,
            format!(
                "SELECT {} FROM \"{}\" WHERE {predicate} ORDER BY {order} LIMIT {PAGE_ROWS}",
                projection.join(","),
                t.table
            ),
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    let mut page = Page {
        rows: Vec::new(),
        reused: Vec::new(),
        resident: Vec::new(),
        last: after.cloned(),
        eof: metadata.len() < PAGE_ROWS,
    };
    let mut payload = 0;
    for meta in metadata {
        let key = NativeKey {
            table: t.table.clone(),
            values: (0..t.primary_key.len())
                .map(|i| {
                    meta.try_get::<Option<String>>("", &format!("k{i}"))
                        .map_err(|_| ManagementError::Database)?
                        .ok_or(ManagementError::Budget)
                })
                .collect::<Result<_>>()?,
        };
        let lengths: Vec<Option<i64>> = (0..t.columns.len())
            .map(|i| {
                meta.try_get("", &format!("n{i}"))
                    .map_err(|_| ManagementError::Database)
            })
            .collect::<Result<_>>()?;
        // Scope collection uses one caller-owned consistent read boundary.
        // Overlapping selectors still re-read membership keys but need not
        // materialize a second copy of an original already read in this call.
        if let Some(original) = originals.and_then(|rows| rows.get(&key)) {
            if t.columns.iter().zip(&lengths).any(|(column, length)| {
                original
                    .get(column)
                    .map(|cell| cell.as_ref().map(|value| value.len() as i64))
                    != Some(*length)
            }) {
                return Err(ManagementError::Changed);
            }
            budget.charge(
                0,
                1,
                t.columns.len() as u64,
                key.values.iter().map(|v| v.len() as u64).sum(),
            )?;
            page.last = Some(key.clone());
            page.reused.push(key);
            continue;
        }
        let bytes = lengths.iter().try_fold(0usize, |sum, n| {
            sum.checked_add(usize::try_from(n.unwrap_or(0)).map_err(|_| ManagementError::Budget)?)
                .ok_or(ManagementError::Budget)
        })?;
        if bytes > PAGE_BYTES && !upgrade {
            return Err(ManagementError::Budget);
        }
        if payload + bytes > PAGE_BYTES && !page.rows.is_empty() {
            page.eof = false;
            break;
        }
        budget.charge(0, 1, t.columns.len() as u64, bytes as u64)?;
        page.resident.push(budget.reserve(
            bytes * 2 + t.columns.len() * 128 + key.values.iter().map(String::len).sum::<usize>(),
        )?);
        let exact = queries(&Selector::Exact(key.clone()))?.remove(0);
        let mut row = NativeRow::new();
        for (column, length) in t.columns.iter().zip(lengths) {
            let value = if let Some(length) = length {
                let mut bytes = Vec::with_capacity(length as usize);
                let mut offset = 0;
                while offset < length {
                    budget.charge(1, 0, 0, 0)?;
                    let slice = if backend == DatabaseBackend::Sqlite {
                        format!(
                            "substr({}, {}, {CELL_SLICE})",
                            octets(backend, column),
                            offset + 1
                        )
                    } else {
                        format!(
                            "substring({} FROM {} FOR {CELL_SLICE})",
                            octets(backend, column),
                            offset + 1
                        )
                    };
                    let data = db
                        .query_one(Statement::from_sql_and_values(
                            backend,
                            format!(
                                "SELECT {slice} AS chunk FROM \"{}\" WHERE {}",
                                t.table, exact.predicate
                            ),
                            exact.values.clone(),
                        ))
                        .await
                        .map_err(|_| ManagementError::Database)?
                        .ok_or(ManagementError::Changed)?;
                    let chunk: Vec<u8> = data
                        .try_get("", "chunk")
                        .map_err(|_| ManagementError::InvalidEvidence)?;
                    if chunk.is_empty() || chunk.len() > CELL_SLICE {
                        return Err(ManagementError::Changed);
                    }
                    offset += chunk.len() as i64;
                    bytes.extend(chunk);
                }
                if bytes.len() != length as usize {
                    return Err(ManagementError::Changed);
                }
                Some(String::from_utf8(bytes).map_err(|_| ManagementError::InvalidEvidence)?)
            } else {
                None
            };
            row.insert(column.clone(), value);
        }
        payload += bytes;
        page.last = Some(key.clone());
        page.rows.push((key, row));
    }
    budget.work.pages += 1;
    Ok(page)
}
