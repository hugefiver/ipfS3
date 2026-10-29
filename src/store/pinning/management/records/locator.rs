//! Indexed provenance, with independent dirty tombstones. Consumers must still
//! validate every returned original and meet all permissions; absence is exact.
use super::*;
use native::{
    NativeKey, NativeRow,
    meter::Budget,
    scope::{self, ScopeSpec, Selector},
};
use std::collections::{BTreeMap, BTreeSet};
mod classify;
mod discovery;
mod narrow;
mod upgrade;
pub use classify::{DependencyKind, SourceClassification};
pub(in crate::store::pinning) use discovery::discover_observed_authority;
pub(in crate::store::pinning) use discovery::discover_quota_authority;
pub(in crate::store::pinning) use discovery::discover_runtime_authority;
pub use discovery::{
    AuthorityMembership, AuthorityProof, AuthorityView, SourceSeal, discover_authority,
    fence_authority, load_authority, revalidate_authority,
};
pub(crate) use discovery::{check_authority, check_proof};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AuthoritySelector {
    Job(String),
    Lease(String),
    Target(String),
    Route(String),
    Domain(String),
    RecordMembers(String, String),
    MigrationSlot(String, String, String),
    LegacyPoint(String, Vec<String>, String),
    ExactVersion(String),
    LegacyOperation(String),
    Dependency(NativeKey),
    RouteDomain(String),
    RegisteredRoute(String),
    RegisteredDomain(String),
    RetirementDomain(String),
    LegacyNamespace(String),
    LegacyResource(String, String, Option<String>),
    LegacyJob(String, String),
    AttestationCid(String),
}
impl AuthoritySelector {
    fn sql(&self) -> Result<(&'static str, String)> {
        if let Self::Domain(domain) | Self::RouteDomain(domain) = self {
            validate_domain_key(domain)?;
        }
        let (kind, value) = match self {
            Self::Job(v) => ("job", serde_json::to_string(v)),
            Self::Lease(v) => ("lease", serde_json::to_string(v)),
            Self::Target(v) => ("target", serde_json::to_string(v)),
            Self::Route(v) => ("route", serde_json::to_string(v)),
            Self::Domain(v) => ("domain", serde_json::to_string(v)),
            Self::RecordMembers(a, b) => ("record_members", serde_json::to_string(&(a, b))),
            Self::MigrationSlot(a, b, c) => ("migration", serde_json::to_string(&(a, b, c))),
            Self::LegacyPoint(a, b, c) => ("legacy", serde_json::to_string(&(a, b, c))),
            Self::ExactVersion(v) => ("version", serde_json::to_string(v)),
            Self::LegacyOperation(v) => ("legacy_operation", serde_json::to_string(v)),
            Self::Dependency(v) => ("dependency", serde_json::to_string(v)),
            Self::RouteDomain(v) => ("route_domain", serde_json::to_string(v)),
            Self::RegisteredRoute(v) => ("registered_route", serde_json::to_string(v)),
            Self::RegisteredDomain(v) => ("registered_domain", serde_json::to_string(v)),
            Self::RetirementDomain(v) => ("retirement_domain", serde_json::to_string(v)),
            Self::LegacyNamespace(v) => ("legacy_namespace", serde_json::to_string(v)),
            Self::LegacyResource(t, p, c) => ("legacy_resource", serde_json::to_string(&(t, p, c))),
            Self::LegacyJob(p, id) => ("legacy_job", serde_json::to_string(&(p, id))),
            Self::AttestationCid(cid) => ("attestation_cid", serde_json::to_string(cid)),
        };
        Ok((kind, value.map_err(|_| ManagementError::InvalidEvidence)?))
    }
}
pub fn canonical_domain(route: &crate::pinning::identity::ProviderRouteSnapshot) -> Result<String> {
    super::super::admission::allocation_domain(route).map_err(|_| ManagementError::InvalidEvidence)
}
fn validate_domain_key(domain: &str) -> Result<()> {
    let raw = domain
        .strip_prefix("domain:")
        .ok_or(ManagementError::InvalidEvidence)?;
    let tuple: (String, String, crate::pinning::identity::RemoteResourceType) =
        serde_json::from_str(raw).map_err(|_| ManagementError::InvalidEvidence)?;
    if tuple.0.is_empty()
        || tuple.1.is_empty()
        || serde_json::to_string(&tuple).map_err(|_| ManagementError::InvalidEvidence)? != raw
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
pub fn source_key(key: &NativeKey) -> Result<String> {
    if !super::super::schema::storage::SOURCE_TABLES.contains(&key.table.as_str()) {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(format!(
        "{}:{}",
        key.table,
        serde_json::to_string(&key.values).map_err(|_| ManagementError::InvalidEvidence)?
    ))
}
pub(crate) fn key_from_source(source: &str) -> Result<NativeKey> {
    let (table, values) = source
        .split_once(':')
        .ok_or(ManagementError::InvalidEvidence)?;
    let key = NativeKey {
        table: table.into(),
        values: serde_json::from_str(values).map_err(|_| ManagementError::InvalidEvidence)?,
    };
    if source_key(&key)? != source {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(key)
}
fn required<'a>(row: &'a NativeRow, column: &str) -> Result<&'a str> {
    native::value(row, column).ok_or(ManagementError::InvalidEvidence)
}

fn route_selectors(
    route: &crate::pinning::identity::ProviderRouteSnapshot,
    set: &mut BTreeSet<AuthoritySelector>,
) -> Result<()> {
    set.insert(AuthoritySelector::Route(route.provider_id.clone()));
    set.insert(AuthoritySelector::Domain(format!(
        "domain:{}",
        serde_json::to_string(&(&route.backend, &route.scope, route.resource_type()))
            .map_err(|_| ManagementError::InvalidEvidence)?
    )));
    Ok(())
}

/// Classification is total for supported originals. Unknown encodings remain
/// global-pending; the caller cannot supply a more permissive selector list.
fn basic_selectors(key: &NativeKey, row: &NativeRow) -> Result<BTreeSet<AuthoritySelector>> {
    let mut set = BTreeSet::new();
    match key.table.as_str() {
        "pin_provider_routes" => {
            let route: crate::pinning::identity::ProviderRouteSnapshot =
                serde_json::from_str(required(row, "snapshot")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            set.insert(AuthoritySelector::Route(required(row, "provider")?.into()));
            route_selectors(&route, &mut set)?;
        }
        "pin_management_retirements" => {
            let domain = required(row, "allocation_domain")?;
            if domain.starts_with("domain:") {
                set.insert(AuthoritySelector::Domain(domain.into()));
            }
            set.insert(AuthoritySelector::RecordMembers(
                required(row, "database_scope")?.into(),
                required(row, "plan_id")?.into(),
            ));
        }
        "pin_management_records" => {
            set.insert(AuthoritySelector::RecordMembers(
                required(row, "database_scope")?.into(),
                required(row, "plan_id")?.into(),
            ));
            if required(row, "kind")? == "database_scope" {
                return Ok(set);
            }
            let leases: Vec<String> = serde_json::from_str(required(row, "intent_ids")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            let jobs: Vec<String> = serde_json::from_str(required(row, "job_ids")?)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            for lease in leases {
                set.insert(AuthoritySelector::Lease(lease));
            }
            for job in jobs {
                set.insert(AuthoritySelector::Job(job));
            }
            if required(row, "state")? == "applied" {
                let body = required(row, "result")?
                    .strip_prefix("S7R1\n")
                    .ok_or(ManagementError::InvalidEvidence)?;
                let decoded = results::decode_facts(body, &mut DecodeBudget::default())?;
                let results::DecodedResult::Facts(action, _, _) = decoded;
                for lease in &action.receipt.intent_ids {
                    set.insert(AuthoritySelector::Lease(lease.clone()));
                }
                for target in action.targets {
                    set.insert(AuthoritySelector::Target(target));
                }
                for job in action.jobs {
                    set.insert(AuthoritySelector::Job(job.job_id));
                    if let Some(lease) = job.lease_id {
                        set.insert(AuthoritySelector::Lease(lease));
                    }
                    if let Some(target) = job.target_id {
                        set.insert(AuthoritySelector::Target(target));
                    }
                    route_selectors(&job.route, &mut set)?;
                    for object in [job.owner, job.source].into_iter().flatten() {
                        set.insert(AuthoritySelector::ExactVersion(object.private_version_id));
                    }
                }
                for point in action.migration_points {
                    set.insert(AuthoritySelector::Lease(point.lease_id.clone()));
                    set.insert(AuthoritySelector::MigrationSlot(
                        point.lease_id,
                        point.cid,
                        point.slot,
                    ));
                    set.insert(AuthoritySelector::Target(point.source_target_id));
                    set.insert(AuthoritySelector::Target(point.destination_target_id));
                    route_selectors(&point.source_route, &mut set)?;
                    route_selectors(&point.destination_route, &mut set)?;
                }
            } else if required(row, "kind")? == "evidence_preparation" {
                let RecordPayload::Evidence { attestation, .. } =
                    inspect_original_row(&key.table, row)?
                else {
                    return Err(ManagementError::InvalidEvidence);
                };
                set.insert(AuthoritySelector::ExactVersion(
                    attestation.identity.private_version_id,
                ));
            } else if required(row, "kind")? != "action" {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        "pin_legacy_migrations" => {
            let declaration: crate::store::pinning::legacy_migration::Declaration =
                serde_json::from_str(required(row, "declaration")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            set.insert(AuthoritySelector::LegacyOperation(
                required(row, "id")?.into(),
            ));
            if let Some(reverses) = native::value(row, "reverses") {
                set.insert(AuthoritySelector::LegacyOperation(reverses.into()));
            }
            for namespace in declaration.namespaces {
                set.insert(AuthoritySelector::Route(namespace.source));
                set.insert(AuthoritySelector::Route(namespace.target));
                route_selectors(&namespace.target_route, &mut set)?;
                for binding in namespace.bindings {
                    legacy_selector(&binding, &mut set)?;
                }
            }
        }
        "pin_legacy_migration_items" => {
            let bindings: Vec<crate::store::pinning::legacy_migration::Binding> =
                serde_json::from_str(required(row, "bindings")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            set.insert(AuthoritySelector::LegacyOperation(
                required(row, "operation_id")?.into(),
            ));
            let original_key: BTreeMap<String, String> =
                serde_json::from_str(required(row, "original_key")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            for binding in bindings {
                if binding.table != required(row, "table_kind")? || binding.key != original_key {
                    return Err(ManagementError::InvalidEvidence);
                }
                legacy_selector(&binding, &mut set)?;
            }
        }
        _ => return Err(ManagementError::InvalidEvidence),
    }
    Ok(set)
}

fn legacy_selector(
    binding: &crate::store::pinning::legacy_migration::Binding,
    set: &mut BTreeSet<AuthoritySelector>,
) -> Result<()> {
    let table = native::current_catalog()
        .iter()
        .find(|t| t.table == binding.table)
        .ok_or(ManagementError::InvalidEvidence)?;
    let values = table
        .primary_key
        .iter()
        .map(|c| {
            binding
                .key
                .get(c)
                .cloned()
                .ok_or(ManagementError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>>>()?;
    if binding.key.len() != values.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    set.insert(AuthoritySelector::LegacyPoint(
        binding.table.clone(),
        values,
        binding.provider.clone(),
    ));
    match binding.table.as_str() {
        "pin_jobs" | "pin_invocation_routes" => {
            if let Some(id) = binding.key.get("id").or_else(|| binding.key.get("job_id")) {
                set.insert(AuthoritySelector::Job(id.clone()));
            }
        }
        "pin_leases" => {
            if let Some(id) = binding.key.get("id") {
                set.insert(AuthoritySelector::Lease(id.clone()));
            }
        }
        "pin_lease_targets" => {
            if let Some(id) = binding.key.get("id") {
                set.insert(AuthoritySelector::Target(id.clone()));
            }
        }
        _ => {}
    }
    if let Some(route) = &binding.historical_route {
        route_selectors(route, set)?;
    }
    Ok(())
}

/// Explicit maintenance or original writer only. Does not verify business
/// permission: sealing certifies classification and exact source bytes only.
pub async fn rebuild_source(
    tx: &DatabaseTransaction,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<()> {
    if crate::store::initialization::read_context::active(tx).await? {
        return Err(ManagementError::Unauthorized);
    }
    rebuild_classified_sources(tx, &BTreeSet::from([key.clone()]), budget, None).await
}

pub(crate) async fn rebuild_runtime_source(
    tx: &crate::store::RuntimeTransaction,
    key: &NativeKey,
    budget: &mut Budget,
    work: DecodeWork<'_>,
) -> Result<()> {
    if crate::store::initialization::read_context::active(tx.db()).await? {
        return Err(ManagementError::Unauthorized);
    }
    rebuild_classified_sources(
        tx.db(),
        &BTreeSet::from([key.clone()]),
        budget,
        Some((tx, work)),
    )
    .await
}

/// Backfill requires initialization's validated capability for this exact
/// transaction and database identity. It does not make ordinary reads ready.
pub(crate) async fn rebuild_source_for_upgrade(
    tx: &impl crate::store::initialization::TransactionConnection,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<()> {
    if !crate::store::initialization::read_context::active(tx).await? {
        return Err(ManagementError::Unauthorized);
    }
    rebuild_classified_sources(tx, &BTreeSet::from([key.clone()]), budget, None).await
}

/// One atomic writer validates each shared receipt dependency group once.
pub(crate) async fn rebuild_sources(
    tx: &DatabaseTransaction,
    keys: &BTreeSet<NativeKey>,
    budget: &mut Budget,
) -> Result<()> {
    if crate::store::initialization::read_context::active(tx).await? {
        return Err(ManagementError::Unauthorized);
    }
    rebuild_classified_sources(tx, keys, budget, None).await
}

async fn rebuild_classified_sources(
    tx: &impl crate::store::initialization::TransactionConnection,
    keys: &BTreeSet<NativeKey>,
    budget: &mut Budget,
    runtime: Option<(&crate::store::RuntimeTransaction, DecodeWork<'_>)>,
) -> Result<()> {
    if keys.is_empty() {
        return budget.check();
    }
    discovery::fence_authority_in(tx).await?;
    let spec = ScopeSpec::new(keys.iter().cloned().map(Selector::Exact).collect())?;
    scope::fence_memberships_in(tx, &spec).await?;
    let read = match runtime {
        Some((owner, work)) => scope::collect_runtime_scope(owner, &spec, budget, work).await?,
        None => scope::collect_scope(tx, &spec, budget).await?,
    };
    if !read.rows().keys().eq(keys.iter()) {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut classifications = BTreeMap::new();
    for (key, row) in read.rows() {
        let classification = upgrade::classify_for_write(tx, key, row, budget).await?;
        classifications.insert(key.clone(), classification);
    }
    discovery::validate_sources_dependencies(tx, read.rows(), &classifications, budget).await?;
    for (key, classification) in classifications {
        replace_source_locators(tx, &key, classification.selectors, budget).await?;
    }
    match runtime {
        Some((owner, work)) => scope::revalidate_runtime_scope(owner, &read, budget, work).await?,
        None => scope::revalidate_scope_in(tx, &read, budget).await?,
    };
    for key in keys {
        seal_source(tx, key, budget).await?;
    }
    budget.check()
}

async fn replace_source_locators(
    tx: &impl crate::store::initialization::TransactionConnection,
    key: &NativeKey,
    selectors: BTreeSet<AuthoritySelector>,
    budget: &mut Budget,
) -> Result<()> {
    let source = source_key(key)?;
    budget.charge(2, 0, 0, 0)?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"INSERT INTO pin_management_source_control(source_key) VALUES ($1) ON CONFLICT(source_key) DO NOTHING",[source.clone().into()])).await.map_err(|_|ManagementError::Database)?;
    if tx.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT source_key FROM pin_management_source_control WHERE source_key=$1 FOR UPDATE NOWAIT",[source.clone().into()])).await.map_err(super::super::nowait_error)?.ok_or(ManagementError::InvalidEvidence)?;
    }
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "DELETE FROM pin_management_locator WHERE source_key=$1",
        [source.clone().into()],
    ))
    .await
    .map_err(|_| ManagementError::Database)?;
    let mut expected = Vec::new();
    for selector in selectors {
        let (kind, value) = selector.sql()?;
        budget.charge(1, 1, 2, value.len() as u64)?;
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_locator VALUES ($1,$2,$3)",
            [kind.into(), value.clone().into(), source.clone().into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
        expected.push((kind.to_owned(), value));
    }
    expected.sort();
    let actual=tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT selector_kind,selector_key FROM pin_management_locator WHERE source_key=$1 ORDER BY selector_kind,selector_key",[source.clone().into()])).await.map_err(|_|ManagementError::Database)?.iter().map(|r|Ok((r.try_get::<String>("","selector_kind").map_err(|_|ManagementError::InvalidEvidence)?,r.try_get::<String>("","selector_key").map_err(|_|ManagementError::InvalidEvidence)?))).collect::<Result<Vec<_>>>()?;
    if actual != expected {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

async fn seal_source(
    tx: &impl crate::store::initialization::TransactionConnection,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<()> {
    let source = source_key(key)?;
    budget.charge(3, 1, 2, 0)?;
    let control=tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT source_generation,locator_generation FROM pin_management_source_control WHERE source_key=$1",[source.clone().into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    let sg: i64 = control
        .try_get("", "source_generation")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let lg: i64 = control
        .try_get("", "locator_generation")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let changed=tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_source_control SET sealed_source=$1,sealed_locator=$2 WHERE source_key=$3 AND source_generation=$1 AND locator_generation=$2",[sg.into(),lg.into(),source.clone().into()])).await.map_err(|_|ManagementError::Database)?;
    if changed.rows_affected() != 1 {
        return Err(ManagementError::Changed);
    }
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"DELETE FROM pin_management_pending_scope WHERE source_key=$1 AND source_generation<=$2 AND locator_generation<=$3",[source.into(),sg.into(),lg.into()])).await.map_err(|_|ManagementError::Database)?;
    budget.check()
}

#[cfg(test)]
mod upgrade_tests;
