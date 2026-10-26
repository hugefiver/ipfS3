//! Read-only discovery and separately acquired final-write phantom fences.
use super::*;
use native::meter::{PAGE_ROWS, ResidentGuard};
use sea_orm::DatabaseBackend;
mod classification;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSeal {
    pub source: NativeKey,
    pub source_key: String,
    pub source_generation: i64,
    pub locator_generation: i64,
    pub sealed_source_generation: i64,
    pub sealed_locator_generation: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityMembership {
    pub selector: AuthoritySelector,
    pub sources: Vec<NativeKey>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProof {
    pub memberships: Vec<AuthorityMembership>,
    pub seals: Vec<SourceSeal>,
}
pub struct AuthorityView {
    pub originals: BTreeMap<NativeKey, NativeRow>,
    pub proof: AuthorityProof,
    pub classifications: BTreeMap<NativeKey, SourceClassification>,
    pub database_scope: String,
    pub database_identity: String,
    requested: Vec<AuthoritySelector>,
    _resident: Vec<ResidentGuard>,
}
impl AuthorityView {
    pub fn requested_selectors(&self) -> &[AuthoritySelector] {
        &self.requested
    }
    pub fn operational_keys(&self, domain: &str) -> BTreeSet<String> {
        self.classifications
            .values()
            .filter_map(|class| class.operational_keys.get(domain))
            .flatten()
            .cloned()
            .collect()
    }
    pub(crate) fn into_scope_parts(
        self,
    ) -> (
        BTreeMap<NativeKey, NativeRow>,
        AuthorityProof,
        Vec<ResidentGuard>,
    ) {
        (self.originals, self.proof, self._resident)
    }
}

fn seal(row: &sea_orm::QueryResult) -> Result<SourceSeal> {
    let source_key: String = row
        .try_get("", "source_key")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let sg: i64 = row
        .try_get("", "source_generation")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let lg: i64 = row
        .try_get("", "locator_generation")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let ss: i64 = row
        .try_get("", "sealed_source")
        .map_err(|_| ManagementError::Changed)?;
    let sl: i64 = row
        .try_get("", "sealed_locator")
        .map_err(|_| ManagementError::Changed)?;
    if sg < 0 || lg < 0 || sg != ss || lg != sl {
        return Err(ManagementError::Changed);
    }
    Ok(SourceSeal {
        source: key_from_source(&source_key)?,
        source_key,
        source_generation: sg,
        locator_generation: lg,
        sealed_source_generation: ss,
        sealed_locator_generation: sl,
    })
}

async fn membership<C: ConnectionTrait>(
    tx: &C,
    selector: &AuthoritySelector,
    budget: &mut Budget,
) -> Result<Vec<SourceSeal>> {
    let (kind, key) = selector.sql()?;
    budget.charge(1, 0, 0, key.len() as u64)?;
    if tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT 1 FROM pin_management_pending_scope WHERE (selector_kind='unknown' AND selector_key='*') OR (selector_kind=$1 AND selector_key=$2) LIMIT 1",[kind.into(),key.clone().into()])).await.map_err(|_|ManagementError::Database)?.is_some() {return Err(ManagementError::Changed);}
    let _resident = budget.reserve(PAGE_ROWS * 4352)?;
    let mut after = String::new();
    let mut seals = Vec::new();
    loop {
        budget.charge(1, 0, 0, 0)?;
        let length = if tx.get_database_backend() == DatabaseBackend::Sqlite {
            "length(CAST(l.source_key AS BLOB))"
        } else {
            "octet_length(l.source_key)"
        };
        let rows=tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),format!("SELECT CASE WHEN {length}<=4096 THEN l.source_key END AS source_key,c.source_generation,c.locator_generation,c.sealed_source,c.sealed_locator FROM pin_management_locator l LEFT JOIN pin_management_source_control c ON c.source_key=l.source_key WHERE l.selector_kind=$1 AND l.selector_key=$2 AND l.source_key>$3 ORDER BY l.source_key LIMIT {PAGE_ROWS}"),[kind.into(),key.clone().into(),after.clone().into()])).await.map_err(|_|ManagementError::Database)?;
        let eof = rows.len() < PAGE_ROWS;
        budget.charge(0, rows.len() as u64, rows.len() as u64 * 5, 0)?;
        for row in rows {
            let s = seal(&row)?;
            budget.charge(0, 0, 0, s.source_key.len() as u64)?;
            after = s.source_key.clone();
            seals.push(s);
        }
        if eof {
            break;
        }
    }
    Ok(seals)
}

async fn exact_seal<C: ConnectionTrait>(
    tx: &C,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<SourceSeal> {
    budget.charge(1, 1, 5, 0)?;
    let row=tx.query_one(Statement::from_sql_and_values(tx.get_database_backend(),"SELECT source_key,source_generation,locator_generation,sealed_source,sealed_locator FROM pin_management_source_control WHERE source_key=$1",[source_key(key)?.into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::InvalidEvidence)?;
    seal(&row)
}

/// No DDL, DML or LOCK TABLE. PG callers provide a consistent read snapshot;
/// final-write callers acquire fence_authority after their ordered business locks.
pub async fn load_authority<C: ConnectionTrait>(
    tx: &C,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
) -> Result<AuthorityView> {
    discover_authority(tx, selectors, budget).await
}

pub async fn discover_authority<C: ConnectionTrait>(
    tx: &C,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
) -> Result<AuthorityView> {
    crate::store::pinning::management::schema::validate(tx).await?;
    discover_validated(tx, selectors, budget).await
}

async fn discover_validated<C: ConnectionTrait>(
    tx: &C,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
) -> Result<AuthorityView> {
    if selectors.is_empty() || selectors.len() > 128 {
        return Err(ManagementError::InvalidEvidence);
    }
    let (database_scope, database_identity) = scope::binding(tx).await?;
    let mut view = AuthorityView {
        originals: BTreeMap::new(),
        proof: AuthorityProof::default(),
        classifications: BTreeMap::new(),
        database_scope,
        database_identity,
        requested: selectors.to_vec(),
        _resident: Vec::new(),
    };
    let mut pending: BTreeSet<_> = selectors.iter().cloned().collect();
    let mut memberships = BTreeMap::new();
    let mut sources = BTreeMap::<NativeKey, SourceSeal>::new();
    loop {
        while let Some(selector) = pending.pop_first() {
            if memberships.contains_key(&selector) {
                continue;
            }
            let matched = membership(tx, &selector, budget).await?;
            let mut keys = Vec::new();
            for seal in matched {
                keys.push(seal.source.clone());
                if sources.get(&seal.source).is_some_and(|old| old != &seal) {
                    return Err(ManagementError::Changed);
                }
                sources.insert(seal.source.clone(), seal);
            }
            keys.sort();
            memberships.insert(selector, keys);
        }
        let unread = sources
            .keys()
            .find(|key| !view.originals.contains_key(*key))
            .cloned();
        let Some(key) = unread else {
            break;
        };
        let (row, resident) = scope::read_exact(tx, &key, budget).await?;
        let (key, row, resident, mut classification) =
            classification::classify_row(key, row, resident, budget).await?;
        narrow::bind_retirement_domain(tx, &key, &row, &mut classification, budget).await?;
        validate_locator_set(tx, &key, &classification, budget).await?;
        for lookup in &classification.membership_lookups {
            if !memberships.contains_key(lookup) {
                pending.insert(lookup.clone());
            }
        }
        for dependency in classification.dependencies.keys() {
            if !sources.contains_key(dependency) {
                sources.insert(
                    dependency.clone(),
                    exact_seal(tx, dependency, budget).await?,
                );
            }
        }
        view.originals.insert(key.clone(), row);
        view.classifications.insert(key, classification);
        view._resident.extend(resident);
    }
    validate_graph(&view.originals, &view.classifications, &memberships)?;
    view.proof.memberships = memberships
        .into_iter()
        .map(|(selector, sources)| AuthorityMembership { selector, sources })
        .collect();
    view.proof.seals = sources.into_values().collect();
    budget.check()?;
    Ok(view)
}

pub(in crate::store::pinning) async fn discover_observed_authority(
    window: &crate::store::pinning::management::admission::worker_observation::WorkerReadWindow,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
) -> Result<AuthorityView> {
    crate::store::pinning::management::schema::validate_observed(window).await?;
    discover_validated(window.db(), selectors, budget).await
}

pub(in crate::store::pinning) async fn discover_quota_authority(
    window: &crate::store::pinning::quota::production::QuotaReadWindow<'_>,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
) -> Result<AuthorityView> {
    crate::store::pinning::management::schema::validate_quota(window).await?;
    discover_validated(window.db(), selectors, budget).await
}

pub(in crate::store::pinning) async fn discover_runtime_authority(
    tx: &crate::store::RuntimeTransaction,
    selectors: &[AuthoritySelector],
    budget: &mut Budget,
    work: super::super::DecodeWork<'_>,
) -> Result<AuthorityView> {
    tx.check_schema(work).await.map_err(|error| match error {
        crate::error::AppError::ManagementBudget => ManagementError::Budget,
        _ => ManagementError::Schema,
    })?;
    discover_validated(tx.db(), selectors, budget).await
}

async fn validate_locator_set<C: ConnectionTrait>(
    tx: &C,
    key: &NativeKey,
    classification: &SourceClassification,
    budget: &mut Budget,
) -> Result<()> {
    let mut expected = classification
        .selectors
        .iter()
        .map(|s| {
            let (kind, key) = s.sql()?;
            Ok((kind.to_owned(), key))
        })
        .collect::<Result<Vec<_>>>()?;
    expected.sort();
    budget.charge(1, 0, 0, 0)?;
    let rows=tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),format!("SELECT selector_kind,selector_key FROM pin_management_locator WHERE source_key=$1 ORDER BY selector_kind,selector_key LIMIT {}",expected.len()+1),[source_key(key)?.into()])).await.map_err(|_|ManagementError::Database)?;
    budget.charge(0, rows.len() as u64, rows.len() as u64 * 2, 0)?;
    let actual = rows
        .iter()
        .map(|row| {
            Ok((
                row.try_get::<String>("", "selector_kind")
                    .map_err(|_| ManagementError::InvalidEvidence)?,
                row.try_get::<String>("", "selector_key")
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if actual != expected {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

pub async fn fence_authority(tx: &DatabaseTransaction) -> Result<()> {
    scope::writer_boundary(tx).await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        let mut tables = crate::store::pinning::management::schema::storage::SOURCE_TABLES.to_vec();
        tables.extend([
            "pin_management_locator",
            "pin_management_source_control",
            "pin_management_pending_scope",
        ]);
        tables.sort();
        tx.execute_unprepared(&format!(
            "LOCK TABLE {} IN SHARE MODE NOWAIT",
            tables.join(",")
        ))
        .await
        .map_err(crate::store::pinning::management::nowait_error)?;
    }
    Ok(())
}
pub async fn revalidate_authority(
    tx: &DatabaseTransaction,
    reviewed: &AuthorityView,
    budget: &mut Budget,
) -> Result<AuthorityView> {
    fence_authority(tx).await?;
    check_authority(tx, reviewed, budget).await
}

/// Revalidate already loaded immutable originals under the caller's authority
/// fence. Every selector membership and transitive source seal stays exact.
pub(crate) async fn check_proof<C: ConnectionTrait>(
    tx: &C,
    reviewed: &AuthorityProof,
    budget: &mut Budget,
) -> Result<()> {
    let seals = reviewed
        .seals
        .iter()
        .map(|seal| (&seal.source, seal))
        .collect::<BTreeMap<_, _>>();
    if seals.len() != reviewed.seals.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    for member in &reviewed.memberships {
        let mut fresh = membership(tx, &member.selector, budget).await?;
        fresh.sort_by(|left, right| left.source.cmp(&right.source));
        if !fresh
            .iter()
            .map(|seal| &seal.source)
            .eq(member.sources.iter())
            || fresh
                .iter()
                .any(|seal| seals.get(&seal.source).is_none_or(|old| **old != *seal))
        {
            return Err(ManagementError::Changed);
        }
    }
    for seal in &reviewed.seals {
        if exact_seal(tx, &seal.source, budget).await? != *seal {
            return Err(ManagementError::Changed);
        }
    }
    budget.check()
}
/// Read-only equality check; caller-owned write fences remain mandatory for publication.
pub(crate) async fn check_authority<C: ConnectionTrait>(
    tx: &C,
    reviewed: &AuthorityView,
    budget: &mut Budget,
) -> Result<AuthorityView> {
    let fresh = discover_authority(tx, &reviewed.requested, budget).await?;
    if fresh.database_scope != reviewed.database_scope
        || fresh.database_identity != reviewed.database_identity
        || fresh.originals != reviewed.originals
        || fresh.proof != reviewed.proof
        || fresh.classifications != reviewed.classifications
    {
        return Err(ManagementError::Changed);
    }
    Ok(fresh)
}

fn validate_graph(
    rows: &BTreeMap<NativeKey, NativeRow>,
    classes: &BTreeMap<NativeKey, SourceClassification>,
    memberships: &BTreeMap<AuthoritySelector, Vec<NativeKey>>,
) -> Result<()> {
    fn visit(
        key: &NativeKey,
        classes: &BTreeMap<NativeKey, SourceClassification>,
        stack: &mut BTreeSet<NativeKey>,
        done: &mut BTreeSet<NativeKey>,
    ) -> Result<()> {
        if stack.contains(key) || stack.len() >= 32 {
            return Err(ManagementError::InvalidEvidence);
        }
        if done.contains(key) {
            return Ok(());
        }
        stack.insert(key.clone());
        for dependency in classes
            .get(key)
            .ok_or(ManagementError::InvalidEvidence)?
            .dependencies
            .keys()
        {
            visit(dependency, classes, stack, done)?;
        }
        stack.remove(key);
        done.insert(key.clone());
        Ok(())
    }
    let mut done = BTreeSet::new();
    for (key, class) in classes {
        visit(key, classes, &mut BTreeSet::new(), &mut done)?;
        for dependency in class.dependencies.keys() {
            if dependency.table == "pin_management_records" && key.table == dependency.table {
                let source = &rows[key];
                let target = rows
                    .get(dependency)
                    .ok_or(ManagementError::InvalidEvidence)?;
                for column in ["database_scope", "native_identity", "principal"] {
                    if source.get(column) != target.get(column) {
                        return Err(ManagementError::Unauthorized);
                    }
                }
                if class.dependencies.get(dependency) == Some(&DependencyKind::Predecessor) {
                    if native::value(target, "kind") != Some("action")
                        || native::value(target, "state") != Some("applied")
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    if key.values.get(1) == dependency.values.get(1) {
                        let current = class
                            .item_position
                            .ok_or(ManagementError::InvalidEvidence)?;
                        let previous = classes[dependency]
                            .item_position
                            .ok_or(ManagementError::InvalidEvidence)?;
                        if previous >= current {
                            return Err(ManagementError::InvalidEvidence);
                        }
                    }
                }
            }
        }
        if key.table == "pin_management_records"
            && native::value(&rows[key], "kind") == Some("action")
            && let Some(pointer) =
                required(&rows[key], "native_before")?.strip_prefix(archive::LOCATOR)
        {
            let (anchor, core_hash, manifest_hash): (archive::Key, String, String) =
                DecodeBudget::default().typed(pointer)?;
            let anchor = NativeKey {
                table: anchor.0,
                values: anchor.1,
            };
            let original = rows.get(&anchor).ok_or(ManagementError::InvalidEvidence)?;
            let raw = required(original, "native_before")?;
            let mut work = DecodeBudget::default();
            let sections = archive::Sections::parse(raw, &mut work)?;
            let head: archive::Head = work.typed(&raw[sections.head])?;
            if core_hash != head.5 || manifest_hash != head.6 {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        if !class.ordered_plan_members.is_empty() {
            let selector =
                AuthoritySelector::RecordMembers(key.values[0].clone(), key.values[1].clone());
            let actual = memberships
                .get(&selector)
                .ok_or(ManagementError::InvalidEvidence)?;
            let mut expected = class.ordered_plan_members.clone();
            expected.sort();
            // Associated retirement sources remain in the authority proof, but
            // are not action records in the original ordered plan member set.
            let action_members = actual
                .iter()
                .filter(|member| member.table == key.table)
                .cloned()
                .collect::<Vec<_>>();
            if action_members != expected {
                return Err(ManagementError::InvalidEvidence);
            }
            for (position, member) in class.ordered_plan_members.iter().enumerate() {
                if !archive::binding_matches(
                    required(&rows[member], "binding")?,
                    required(&rows[key], "binding")?,
                ) {
                    return Err(ManagementError::InvalidEvidence);
                }
                let current = classes
                    .get(member)
                    .ok_or(ManagementError::InvalidEvidence)?;
                if current.item_position.is_some_and(|p| p != position) {
                    return Err(ManagementError::InvalidEvidence);
                }
                for predecessor in &current.same_plan_predecessors {
                    if *predecessor >= position
                        || native::value(&rows[&class.ordered_plan_members[*predecessor]], "state")
                            != Some("applied")
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                }
            }
        }
        if key.table == "pin_legacy_migrations" {
            let expected = required(&rows[key], "item_count")?
                .parse::<usize>()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            let selector = AuthoritySelector::LegacyOperation(key.values[0].clone());
            let actual = memberships
                .get(&selector)
                .ok_or(ManagementError::InvalidEvidence)?;
            let mut ordinals = actual
                .iter()
                .filter(|member| {
                    member.table == "pin_legacy_migration_items"
                        && member.values.first() == key.values.first()
                })
                .map(|member| {
                    member
                        .values
                        .get(1)
                        .ok_or(ManagementError::InvalidEvidence)?
                        .parse::<usize>()
                        .map_err(|_| ManagementError::InvalidEvidence)
                })
                .collect::<Result<Vec<_>>>()?;
            ordinals.sort();
            if ordinals.len() != expected || !ordinals.iter().copied().eq(0..expected) {
                return Err(ManagementError::InvalidEvidence);
            }
        }
        if key.table == "pin_legacy_migration_items" {
            validate_legacy_item(&rows[key], rows)?;
        }
    }
    Ok(())
}

fn validate_legacy_item(row: &NativeRow, rows: &BTreeMap<NativeKey, NativeRow>) -> Result<()> {
    use crate::store::pinning::legacy_migration as legacy;
    let operation = NativeKey {
        table: "pin_legacy_migrations".into(),
        values: vec![required(row, "operation_id")?.into()],
    };
    let operation = rows
        .get(&operation)
        .ok_or(ManagementError::InvalidEvidence)?;
    let mut work = DecodeBudget::default();
    let confirmation: legacy::Confirmation = work.typed(required(operation, "confirmation")?)?;
    let key: BTreeMap<String, String> = work.typed(required(row, "original_key")?)?;
    let bindings: Vec<legacy::Binding> = work.typed(required(row, "bindings")?)?;
    let table = required(row, "table_kind")?;
    let expected = confirmation
        .declaration
        .namespaces
        .iter()
        .flat_map(|n| &n.bindings)
        .filter(|b| b.table == table && b.key == key)
        .cloned()
        .collect::<Vec<_>>();
    if bindings != expected {
        return Err(ManagementError::InvalidEvidence);
    }
    let before = confirmation
        .before
        .iter()
        .find(|image| image.table == table)
        .ok_or(ManagementError::InvalidEvidence)?;
    let after = confirmation
        .after
        .iter()
        .find(|image| image.table == table)
        .ok_or(ManagementError::InvalidEvidence)?;
    let mut after_key = key.clone();
    if matches!(
        table,
        "remote_pins" | "remote_pin_ledger" | "pin_provider_usage"
    ) && let Some(provider) = key.get("provider")
        && let Some(namespace) = confirmation
            .declaration
            .namespaces
            .iter()
            .find(|n| &n.source == provider)
    {
        after_key.insert("provider".into(), namespace.target.clone());
    }
    let find = |image: &legacy::TableImage,
                key: &BTreeMap<String, String>,
                allow_absent: bool|
     -> Result<Option<NativeRow>> {
        if image.rows.is_empty() {
            if key.is_empty() || allow_absent {
                return Ok(None);
            }
            return Err(ManagementError::InvalidEvidence);
        }
        let matching = image
            .rows
            .iter()
            .filter(|candidate| {
                image.primary_key.len() == key.len()
                    && key.iter().all(|(column, value)| {
                        native::value(*candidate, column) == Some(value.as_str())
                    })
            })
            .collect::<Vec<_>>();
        if matching.is_empty() && allow_absent {
            return Ok(None);
        }
        if matching.len() != 1 {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(Some(matching[0].clone()))
    };
    let expected_before = find(before, &key, false)?;
    let expected_after = find(after, &after_key, true)?;
    let actual_before: Option<NativeRow> = work.typed(required(row, "before_image")?)?;
    let actual_after: Option<NativeRow> = work.typed(required(row, "after_image")?)?;
    let pair = if required(operation, "direction")? == "rollback" {
        (expected_after, expected_before)
    } else {
        (expected_before, expected_after)
    };
    if (actual_before, actual_after) != pair {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}

pub(super) async fn validate_sources_dependencies(
    tx: &DatabaseTransaction,
    source_rows: &BTreeMap<NativeKey, NativeRow>,
    source_classes: &BTreeMap<NativeKey, SourceClassification>,
    budget: &mut Budget,
) -> Result<()> {
    // Publication cannot depend on a missing source. A source may still be dirty
    // inside this same atomic publication transaction; ordinary readers require seals.
    let bytes = source_rows
        .values()
        .flat_map(|row| row.iter())
        .map(|(column, value)| column.len() + value.as_ref().map_or(0, String::len) + 128)
        .sum::<usize>();
    let _source_resident = budget.reserve(bytes.checked_mul(2).ok_or(ManagementError::Budget)?)?;
    budget.work.copied_bytes = budget
        .work
        .copied_bytes
        .checked_add(bytes as u64)
        .ok_or(ManagementError::Budget)?;
    let mut rows = source_rows.clone();
    let mut classes = source_classes.clone();
    let mut todo = classes
        .values()
        .flat_map(|class| class.dependencies.keys().cloned())
        .collect::<BTreeSet<_>>();
    let mut memberships = BTreeMap::new();
    let mut retained = Vec::new();
    let mut group_reads = Vec::new();
    loop {
        while let Some(next) = todo.pop_first() {
            if classes.contains_key(&next) {
                continue;
            }
            let (row, guards) = scope::read_exact(tx, &next, budget).await?;
            let class = upgrade::classify_for_write(tx, &next, &row, budget).await?;
            todo.extend(class.dependencies.keys().cloned());
            rows.insert(next.clone(), row);
            classes.insert(next, class);
            retained.extend(guards);
        }
        let group = classes
            .values()
            .flat_map(|class| &class.membership_lookups)
            .find(|selector| {
                matches!(
                    selector,
                    AuthoritySelector::RecordMembers(..) | AuthoritySelector::LegacyOperation(..)
                ) && !memberships.contains_key(*selector)
            })
            .cloned();
        let Some(group) = group else {
            break;
        };
        let selectors = match &group {
            AuthoritySelector::RecordMembers(scope_id, plan) => vec![Selector::RecordMembers {
                database_scope: scope_id.clone(),
                plan_id: plan.clone(),
            }],
            AuthoritySelector::LegacyOperation(operation) => vec![
                Selector::Exact(NativeKey {
                    table: "pin_legacy_migrations".into(),
                    values: vec![operation.clone()],
                }),
                Selector::LegacyOperationMembers {
                    operation_id: operation.clone(),
                },
                Selector::LegacyReversals {
                    operation_id: operation.clone(),
                },
            ],
            _ => return Err(ManagementError::InvalidEvidence),
        };
        let selected = scope::collect_scope(tx, &ScopeSpec::new(selectors)?, budget).await?;
        let keys = selected.rows().keys().cloned().collect::<Vec<_>>();
        for (member, row) in selected.rows() {
            if classes.contains_key(member) {
                continue;
            }
            let class = upgrade::classify_for_write(tx, member, row, budget).await?;
            todo.extend(class.dependencies.keys().cloned());
            rows.insert(member.clone(), row.clone());
            classes.insert(member.clone(), class);
        }
        memberships.insert(group, keys);
        group_reads.push(selected);
    }
    validate_graph(&rows, &classes, &memberships)
}
