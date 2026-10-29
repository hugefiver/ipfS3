//! Isolated Stage 7 original decoder. These values are historical data, never
//! a new confirmation, a sealed retirement inventory, or execution authority.
use super::*;
use crate::pinning::management as model;
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

mod archive;
mod references;
pub(super) use references::validate_frozen_binding;
mod protocol;
mod result;
pub(super) use archive::convert;
use protocol::{Binding, Key, Source};

pub use result::{Receipt, RecordedAction};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementCapture {
    pub inventory: model::ProviderInventory,
    pub routes: Vec<model::RouteSnapshot>,
    pub targets: Vec<model::TargetSnapshot>,
    pub remotes: Vec<model::RemoteSnapshot>,
    pub historical_remotes: Vec<model::RemoteSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_references: Vec<model::RootReferenceSnapshot>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Capture {
    Object(Box<model::ObjectCapture>),
    Provider(Box<RetirementCapture>),
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEntry {
    pub id: model::IdempotencyId,
    pub request: model::ItemRequest,
    pub status: model::PlanStatus,
    pub capture: Option<Capture>,
    pub step: Option<model::PlannedStep>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedItem<E = PlanEntry> {
    pub binding: E,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotData<E> {
    pub observed_at: i64,
    pub database_now: Option<DateTime<Utc>>,
    pub configuration: Option<model::ConfigurationBinding>,
    pub objects: Vec<model::ObjectSnapshot>,
    pub routes: Vec<model::RouteSnapshot>,
    pub targets: Vec<model::TargetSnapshot>,
    pub remotes: Vec<model::RemoteSnapshot>,
    pub historical_remotes: Vec<model::RemoteSnapshot>,
    pub unknown_resources: Vec<NativeKey>,
    pub ledger_domains: Vec<model::LedgerDomainSnapshot>,
    pub inventories: Vec<model::ProviderInventory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_references: Vec<model::RootReferenceSnapshot>,
    pub applied: Vec<AppliedItem<E>>,
}

pub type Snapshot = SnapshotData<PlanEntry>;

#[derive(Serialize, Deserialize)]
struct Core(
    model::PlanRequest,
    Vec<PlanEntry>,
    model::Summary,
    Snapshot,
    DateTime<Utc>,
    DateTime<Utc>,
    Vec<records::MigrationPoint>,
    String,
);

/// All envelope coordinates, memberships, hashes, before cells and confirmation
/// timestamps have been checked. A business result still needs its operation
/// verifier; protocol validity alone never creates a trusted committed action.
pub struct Review {
    pub database_scope: String,
    pub database_identity: String,
    pub principal: String,
    pub catalog: Vec<String>,
    pub request: model::PlanRequest,
    pub entries: Vec<PlanEntry>,
    pub summary: model::Summary,
    pub snapshot: Snapshot,
    pub staged_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub acknowledgements: Option<Vec<String>>,
    pub migration_points: Vec<records::MigrationPoint>,
    pub rows: BTreeMap<NativeKey, NativeRow>,
    before_dependencies: BTreeSet<NativeKey>,
    originals: BTreeMap<NativeKey, Arc<NativeRow>>,
    pub source: NativeKey,
    core_identity: String,
    archive_owner: Key,
    inventory_identity: String,
    _resident: Vec<native::meter::ResidentGuard>,
}

impl Review {
    pub fn verify_inventory(&self, raw: &str) -> Result<()> {
        let (owner, digest): (Key, String) = records::ReadSession::default().typed(
            raw.strip_prefix("S7I1\n")
                .ok_or(ManagementError::InvalidEvidence)?,
        )?;
        if owner != self.archive_owner || digest != self.inventory_identity {
            return Err(ManagementError::Confirmation);
        }
        Ok(())
    }
    pub fn recorded_result(&self, key: &NativeKey) -> Result<Option<RecordedAction>> {
        result::recorded(self, key)
    }
    pub fn source_keys(&self) -> impl Iterator<Item = &NativeKey> {
        self.originals.keys()
    }
    pub fn original(&self, key: &NativeKey) -> Option<&NativeRow> {
        self.originals.get(key).map(AsRef::as_ref)
    }
    pub fn contains_legacy_retirement(&self) -> bool {
        self.entries
            .iter()
            .any(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
    }
}

/// Available only inside initialization's identity-bound upgrade transaction.
/// The selected record's own scope/identity may be foreign; it remains data.
pub(crate) async fn read(
    tx: &impl TransactionConnection,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<Review> {
    read_from(tx, key, None, budget).await
}

async fn read_from(
    tx: &impl TransactionConnection,
    key: &NativeKey,
    frozen: Option<&BTreeMap<NativeKey, NativeRow>>,
    budget: &mut Budget,
) -> Result<Review> {
    if !crate::store::initialization::read_context::active(tx).await? {
        return Err(ManagementError::Unauthorized);
    }
    if key.table != management::schema::RECORDS
        || key.values.len() != 4
        || key.values[3] != "action"
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut decode = records::ReadSession::default();
    let mut originals = BTreeMap::<NativeKey, Arc<NativeRow>>::new();
    let mut sources = BTreeMap::<Key, Source>::new();
    let mut plans = BTreeSet::from([key.values[1].clone()]);
    let mut loaded = BTreeSet::new();
    let mut residents = Vec::new();
    let mut binding_identity = None;
    while let Some(plan) = plans.pop_first() {
        if !loaded.insert(plan.clone()) {
            continue;
        }
        let spec = native::scope::ScopeSpec::new(vec![native::scope::Selector::RecordMembers {
            database_scope: key.values[0].clone(),
            plan_id: plan.clone(),
        }])?;
        let selected = if frozen.is_none() {
            Some(native::scope::collect_scope(tx, &spec, budget).await?)
        } else {
            None
        };
        let rows =
            frozen.unwrap_or_else(|| selected.as_ref().expect("selected SQL originals").rows());
        for (source_key, row) in rows.iter().filter(|(source_key, _)| {
            source_key.table == management::schema::RECORDS
                && source_key.values.len() == 4
                && source_key.values[0] == key.values[0]
                && source_key.values[1] == plan
        }) {
            if native::value(row, "kind") != Some("action") {
                continue;
            }
            let bytes = row.values().flatten().map(String::len).sum::<usize>();
            residents.push(budget.reserve(bytes + row.len() * 128)?);
            decode.copy(row)?;
            originals.insert(source_key.clone(), Arc::new(row.clone()));
        }
        for (source_key, row) in &originals {
            let own = Key::from(source_key);
            if sources.contains_key(&own) {
                continue;
            }
            let raw = protocol::cell(row, "native_before")?;
            if let Some(locator) = raw.strip_prefix("S7L1\n") {
                let (anchor, _, _): (Key, String, String) = decode.typed(locator)?;
                anchor.check(&key.values[0])?;
                if anchor.1[1] != source_key.values[1] {
                    return Err(ManagementError::Confirmation);
                }
                if !loaded.contains(&anchor.1[1]) {
                    plans.insert(anchor.1[1].clone());
                }
                continue;
            }
            let source = Source::open(row.clone(), source_key, &mut decode)?;
            if source.head.0 != key.values[0] {
                return Err(ManagementError::Confirmation);
            }
            let identity = (&source.head.1, &source.head.2);
            if let Some((database_identity, principal)) = &binding_identity {
                if identity != (database_identity, principal) {
                    return Err(ManagementError::Confirmation);
                }
            } else {
                binding_identity = Some((identity.0.clone(), identity.1.clone()));
            }
            for dependency in source.dependencies(&mut decode)? {
                dependency.check(&source.head.0)?;
                if !loaded.contains(&dependency.1[1]) {
                    plans.insert(dependency.1[1].clone());
                }
            }
            sources.insert(own, source);
        }
    }
    protocol::validate_graph(&sources, &originals, &mut decode)?;
    let root = originals.get(key).ok_or(ManagementError::InvalidEvidence)?;
    let owner = protocol::owner(root, key, &sources, &mut decode)?;
    let (core, confirmed_at, acks) = check_core(root, owner, &sources, &originals, &mut decode)?;
    for (source_key, source) in &sources {
        if source_key == &owner.head.3 {
            continue;
        }
        let original = originals
            .get(&source_key.native())
            .ok_or(ManagementError::InvalidEvidence)?;
        check_core(original, source, &sources, &originals, &mut decode)?;
        protocol::materialize(source, &sources, &originals, &mut decode, budget)?;
    }
    let (rows, before_dependencies) =
        protocol::materialize(owner, &sources, &originals, &mut decode, budget)?;
    residents.push(
        budget.reserve(
            rows.values()
                .map(|row| row.values().flatten().map(String::len).sum::<usize>() + row.len() * 128)
                .sum(),
        )?,
    );
    Ok(Review {
        database_scope: owner.head.0.clone(),
        database_identity: owner.head.1.clone(),
        principal: owner.head.2.clone(),
        catalog: owner.head.4.clone(),
        request: core.0,
        entries: core.1,
        summary: core.2,
        snapshot: core.3,
        staged_at: core.4,
        expires_at: core.5,
        confirmed_at,
        acknowledgements: acks,
        migration_points: core.6,
        rows,
        before_dependencies,
        originals,
        source: key.clone(),
        core_identity: owner.head.5.clone(),
        archive_owner: owner.head.3.clone(),
        inventory_identity: owner.head.6.clone(),
        _resident: residents,
    })
}

type CheckedCore = (Core, Option<DateTime<Utc>>, Option<Vec<String>>);

fn check_core(
    root: &NativeRow,
    owner: &Source,
    sources: &BTreeMap<Key, Source>,
    originals: &BTreeMap<NativeKey, Arc<NativeRow>>,
    decode: &mut records::ReadSession,
) -> Result<CheckedCore> {
    let core: Core = decode.typed(owner.core())?;
    model::validate_historical_request(&core.0).map_err(|_| ManagementError::Confirmation)?;
    if core.7 != owner.head.6
        || core.4 >= core.5
        || core.1.len() != core.0.items.len()
        || core
            .1
            .windows(2)
            .any(|pair| pair[0].request.item_id >= pair[1].request.item_id)
        || core.1.iter().any(|entry| {
            matches!(entry.status, model::PlanStatus::Blocked(_))
                || !core.0.items.contains(&entry.request)
                || entry.id.as_str()
                    != format!("management-v1:{}:{}", core.0.plan_id, entry.request.item_id)
        })
        || core.1.first().is_none_or(|entry| {
            owner.head.3.1[1] != core.0.plan_id || owner.head.3.1[2] != entry.request.item_id
        })
    {
        return Err(ManagementError::Confirmation);
    }
    let anchor = originals
        .get(&NativeKey {
            table: owner.head.3.0.clone(),
            values: owner.head.3.1.clone(),
        })
        .ok_or(ManagementError::InvalidEvidence)?;
    let anchor_binding = protocol::cell(anchor, "binding")?;
    if !records::binding_matches(protocol::cell(root, "binding")?, anchor_binding) {
        return Err(ManagementError::Confirmation);
    }
    let binding: Binding = decode.typed(
        anchor_binding
            .strip_prefix("S7B1\n")
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    let (confirmed_at, acks) =
        binding.validate(&owner.head.3, &owner.head.5, core.4, core.5, &core.1)?;
    let mut members = 0;
    for (member, row) in originals
        .iter()
        .filter(|(k, _)| k.values[1] == core.0.plan_id)
    {
        members += 1;
        if !core
            .0
            .items
            .iter()
            .any(|item| item.item_id == member.values[2])
            || !records::binding_matches(protocol::cell(row, "binding")?, anchor_binding)
            || protocol::cell(row, "native_identity")? != owner.head.1
            || protocol::cell(row, "principal")? != owner.head.2
            || native::parse_time(protocol::cell(row, "expires_at")?)? != core.5
        {
            return Err(ManagementError::Confirmation);
        }
        let created = native::parse_time(protocol::cell(row, "created_at")?)?;
        if created < core.4 || created >= core.5 || confirmed_at.is_some_and(|at| created > at) {
            return Err(ManagementError::Confirmation);
        }
        match native::value(row, "state") {
            Some("prepared")
                if native::value(row, "result").is_none()
                    && native::value(row, "completed_at").is_none()
                    && native::value(row, "intent_ids") == Some("[]")
                    && native::value(row, "job_ids") == Some("[]") => {}
            Some("applied") if confirmed_at.is_some() && native::value(row, "result").is_some() => {
                let completed = native::parse_time(protocol::cell(row, "completed_at")?)?;
                if completed < confirmed_at.ok_or(ManagementError::Confirmation)?
                    || completed < created
                    || completed >= core.5
                {
                    return Err(ManagementError::Confirmation);
                }
            }
            _ => return Err(ManagementError::Confirmation),
        }
        if protocol::owner(row, member, sources, decode)?.head.3 != owner.head.3 {
            return Err(ManagementError::Confirmation);
        }
    }
    if members != core.0.items.len() {
        return Err(ManagementError::Confirmation);
    }
    Ok((core, confirmed_at, acks))
}

/// Discriminates the incompatible provider shape before a current-DTO reader is
/// used. Both branches still perform their complete strict decoding afterwards.
pub(crate) async fn encoding(
    tx: &impl TransactionConnection,
    key: &NativeKey,
    budget: &mut Budget,
) -> Result<Option<bool>> {
    if !crate::store::initialization::read_context::active(tx).await? {
        return Err(ManagementError::Unauthorized);
    }
    let (row, _resident) = original::read(tx, key, budget).await?;
    let raw = protocol::cell(&row, "native_before")?;
    if !raw.starts_with("S7F1\n") && !raw.starts_with("S7L1\n") {
        return Ok(None);
    }
    let mut work = records::ReadSession::default();
    let (row, key) =
        if let Some(locator) = protocol::cell(&row, "native_before")?.strip_prefix("S7L1\n") {
            let (anchor, _, _): (Key, String, String) = work.typed(locator)?;
            anchor.check(&key.values[0])?;
            let key = anchor.native();
            let (row, _resident) = original::read(tx, &key, budget).await?;
            (row, key)
        } else {
            (row, key.clone())
        };
    let source = Source::open(Arc::new(row), &key, &mut work)?;
    let core: serde_json::Value = work.typed(source.core())?;
    fn provider(value: &serde_json::Value, historical: bool) -> bool {
        match value {
            serde_json::Value::Array(values) => {
                values.iter().any(|value| provider(value, historical))
            }
            serde_json::Value::Object(values) => {
                values.get("provider").is_some_and(|value| {
                    value.get("inventory").is_some()
                        && (!historical || value.get("routes").is_some())
                }) || values.values().any(|value| provider(value, historical))
            }
            _ => false,
        }
    }
    Ok(if provider(&core, true) {
        Some(true)
    } else if provider(&core, false) {
        None
    } else {
        Some(false)
    })
}

pub(crate) async fn retirement(
    tx: &impl TransactionConnection,
    row: &NativeRow,
    budget: &mut Budget,
) -> Result<Review> {
    let key = NativeKey {
        table: management::schema::RECORDS.into(),
        values: ["database_scope", "plan_id", "item_id", "record_kind"]
            .iter()
            .map(|column| protocol::cell(row, column).map(str::to_owned))
            .collect::<Result<_>>()?,
    };
    let review = read(tx, &key, budget).await?;
    let action = review
        .original(&key)
        .ok_or(ManagementError::InvalidEvidence)?;
    if protocol::cell(row, "database_scope")? != review.database_scope
        || protocol::cell(row, "native_identity")? != review.database_identity
        || protocol::cell(row, "principal")? != review.principal
        || protocol::cell(row, "binding")? != protocol::cell(action, "binding")?
        || native::value(action, "state") != Some("applied")
    {
        return Err(ManagementError::Confirmation);
    }
    review.verify_inventory(protocol::cell(row, "inventory")?)?;
    let recorded = review
        .recorded_result(&key)?
        .ok_or(ManagementError::Confirmation)?;
    let model::Subject::Provider { route } = &recorded.receipt.binding.request.subject else {
        return Err(ManagementError::Confirmation);
    };
    let Some(Capture::Provider(capture)) = &recorded.receipt.binding.capture else {
        return Err(ManagementError::Confirmation);
    };
    if capture.inventory.provider_id != route.provider_id {
        return Err(ManagementError::Confirmation);
    }
    let mut domains = Vec::new();
    let mut work = records::ReadSession::default();
    let unresolved: Vec<String> = work.typed(protocol::cell(row, "unresolved")?)?;
    let created = native::parse_time(protocol::cell(row, "created_at")?)?;
    if review.confirmed_at.is_none_or(|at| created < at)
        || created >= review.expires_at
        || unresolved.len() > native::MAX_ROWS
        || unresolved.iter().any(String::is_empty)
        || unresolved.iter().collect::<BTreeSet<_>>().len() != unresolved.len()
        || !matches!(
            native::value(row, "state"),
            Some("retired" | "needs_attention")
        )
    {
        return Err(ManagementError::Confirmation);
    }
    for (key, row) in &review.rows {
        if key.table == "pin_provider_routes" {
            let snapshot: crate::pinning::identity::ProviderRouteSnapshot =
                work.typed(protocol::cell(row, "snapshot")?)?;
            if route.original.as_ref() == Some(&snapshot) {
                domains.push(protocol::cell(row, "provider")?);
            }
        }
    }
    if domains.len() != 1 || native::value(row, "allocation_domain") != Some(domains[0]) {
        return Err(ManagementError::Confirmation);
    }
    let expected = match &recorded.receipt.binding.request.action {
        model::Action::Retire
        | model::Action::RetireWith {
            selection:
                model::RetirementSelection::Retain
                | model::RetirementSelection::ReleaseRootReferences { .. },
        } => "retain",
        model::Action::RetireWith {
            selection: model::RetirementSelection::Migrate { .. },
        } => "migrate",
        model::Action::RetireWith {
            selection: model::RetirementSelection::ControlledCleanup { .. },
        } => "controlled_cleanup",
        _ => return Err(ManagementError::Confirmation),
    };
    if native::value(row, "disposition") != Some(expected) {
        return Err(ManagementError::Confirmation);
    }
    Ok(review)
}
