//! Request-derived relationship closure. No database-wide capture or discovery.
use super::super::{Subject, TargetSelection};
use super::*;
use crate::store::pinning::management::{
    self as storage,
    native::{
        NativeFrame, NativeKey, NativeRow,
        meter::{Budget, ResidentGuard, WorkMeter},
        scope::{self, ScopeSpec, ScopedRead, Selector},
        value,
    },
    records::locator::{self, AuthoritySelector, AuthorityView},
};
use std::collections::{BTreeMap, BTreeSet};

#[path = "scoped/edges.rs"]
mod edges;

/// In-process equality evidence, not an execution grant or imported authority.
/// Empty business and authority memberships are retained independently.
pub struct PlanningScope {
    pub database_scope: String,
    pub database_identity: String,
    pub memberships: BTreeMap<Selector, Vec<NativeKey>>,
    pub authority_memberships: BTreeMap<AuthoritySelector, Vec<NativeKey>>,
    originals: BTreeMap<NativeKey, NativeRow>,
    reads: Vec<ScopedRead>,
    authority: Vec<AuthorityView>,
    _resident: Vec<ResidentGuard>,
    work: WorkMeter,
    peak_resident: usize,
}
impl PlanningScope {
    /// Project a scope whose complete raw rows and predicate memberships were
    /// revalidated under the caller's held business and source fences.
    pub(crate) fn planning_snapshot(
        &self,
        config: &super::super::local_config::ManagementConfig,
        request: &PlanRequest,
        now: chrono::DateTime<chrono::Utc>,
        work: &SharedDecodeBudget,
    ) -> Result<super::super::Snapshot> {
        let mut budget = Budget::default();
        let frame = scope::merge_reads_originals(
            &self.reads,
            &self.authority,
            Some(&self.originals),
            &mut budget,
        )
        .map_err(map_error)?;
        let objects = super::read::objects(&frame, request, work)?;
        let rows = super::read::Rows::project(&frame, now, work)?;
        let view = super::project::assemble(
            now.timestamp(),
            objects,
            rows,
            &config.pinning,
            request,
            work,
        )?;
        let result = super::native_plan::project(
            now,
            &view,
            frame,
            &config.pinning,
            Some(config),
            request,
            work,
        )?;
        storage::admission::authority::charge(&budget, DecodeWork::shared(work))
            .map_err(map_error)?;
        Ok(result)
    }
    pub(crate) async fn capture_current(
        tx: &sea_orm::DatabaseTransaction,
        config: &ValidatedPinningConfig,
        request: &PlanRequest,
        work: &SharedDecodeBudget,
        budget: &mut Budget,
    ) -> Result<Self> {
        let (_, mut scope) = collect(tx, config, request, work, budget).await?;
        scope.record_measurements(budget);
        Ok(scope)
    }
    /// Capture the post-image after ordered business locks. Each newly reached
    /// predicate is fenced before reading, so its rows remain stable to commit.
    pub(crate) async fn capture_fenced(
        tx: &sea_orm::DatabaseTransaction,
        config: &ValidatedPinningConfig,
        request: &PlanRequest,
        original: &PlanningScope,
        work: &SharedDecodeBudget,
        budget: &mut Budget,
    ) -> Result<Self> {
        let (_, mut scope) =
            collect_inner(tx, config, request, work, budget, Some(original)).await?;
        scope.record_measurements(budget);
        Ok(scope)
    }
    pub fn row_count(&self) -> usize {
        self.originals.len()
    }
    pub fn membership_count(&self) -> usize {
        self.memberships.len() + self.authority_memberships.len()
    }
    pub fn has_empty_membership(&self) -> bool {
        self.memberships
            .values()
            .chain(self.authority_memberships.values())
            .any(Vec::is_empty)
    }
    pub fn originals(&self) -> &BTreeMap<NativeKey, NativeRow> {
        &self.originals
    }
    pub fn business_reads(&self) -> &[ScopedRead] {
        &self.reads
    }
    pub fn authority_reads(&self) -> &[AuthorityView] {
        &self.authority
    }
    /// Cumulative storage budget counters for this collection, not database-engine
    /// rows scanned or the distinct projected row count. Decode work is separate.
    pub fn work(&self) -> WorkMeter {
        self.work
    }
    /// Peak reservations recorded by the same collection budget, not process RSS.
    pub fn peak_resident(&self) -> usize {
        self.peak_resident
    }
    pub(super) fn record_measurements(&mut self, budget: &Budget) {
        self.work = budget.work;
        self.peak_resident = budget.peak_resident();
    }

    /// Call only after B2's ordered business locks. The storage tail fences
    /// protect phantom inserts, including memberships that were originally empty.
    pub async fn revalidate(
        &self,
        tx: &sea_orm::DatabaseTransaction,
        budget: &mut Budget,
    ) -> Result<()> {
        for read in &self.reads {
            scope::revalidate_scope_originals(tx, read, &self.originals, budget)
                .await
                .map_err(map_error)?;
        }
        for reviewed in &self.authority {
            locator::fence_authority(tx).await.map_err(map_error)?;
            locator::check_proof(tx, &reviewed.proof, budget)
                .await
                .map_err(map_error)?;
        }
        if scope::binding(tx).await.map_err(map_error)?
            != (self.database_scope.clone(), self.database_identity.clone())
        {
            return Err(SnapshotError::Changed);
        }
        Ok(())
    }
}

pub(super) fn map_error(error: storage::ManagementError) -> SnapshotError {
    match error {
        storage::ManagementError::Budget => SnapshotError::LimitExceeded,
        storage::ManagementError::Schema => SnapshotError::UnsupportedEvidence,
        storage::ManagementError::Changed => SnapshotError::Changed,
        storage::ManagementError::Database => SnapshotError::DatabaseUnavailable,
        _ => SnapshotError::InvalidEvidence,
    }
}

#[derive(Default)]
struct Pending {
    business: BTreeSet<Selector>,
    authority: BTreeSet<AuthoritySelector>,
    full_leases: BTreeSet<String>,
    /// Canonical resource domains with their action-relevant CID only.
    resources: BTreeSet<(String, String, RemoteResourceType, String)>,
    processed: BTreeSet<NativeKey>,
}
impl Pending {
    fn exact(&mut self, table: &str, values: Vec<String>) {
        self.business.insert(Selector::Exact(NativeKey {
            table: table.into(),
            values,
        }));
    }
    fn identity(&mut self, identity: &ExactObject) {
        self.business.insert(Selector::ExactVersion {
            id: identity.private_version_id.clone(),
        });
        self.business.insert(Selector::PublicVersionBinding {
            bucket: identity.bucket.clone(),
            key: identity.key.clone(),
            version: match &identity.public_version {
                PublicVersionId::Null => None,
                PublicVersionId::Version(id) => Some(id.clone()),
            },
        });
        self.exact("objects", vec![identity.object_id.clone()]);
        self.exact("buckets", vec![identity.bucket.clone()]);
        self.exact(
            "pin_extension_decisions",
            vec![identity.private_version_id.clone()],
        );
        self.exact(
            "version_residencies",
            vec![identity.private_version_id.clone()],
        );
        self.business.insert(Selector::VersionConstraints {
            version_row_id: identity.private_version_id.clone(),
        });
        self.business.insert(Selector::OwnerLeases {
            object_id: identity.object_id.clone(),
        });
        self.business.insert(Selector::ManifestBindings {
            version_row_id: identity.private_version_id.clone(),
        });
        self.authority.insert(AuthoritySelector::ExactVersion(
            identity.private_version_id.clone(),
        ));
    }
    fn route(&mut self, route: &ProviderRouteSnapshot, cid: &str) -> Result<()> {
        let allocation_domain = storage::admission::allocation_domain(route)
            .map_err(|_| SnapshotError::InvalidEvidence)?;
        self.resources.insert((
            route.backend.clone(),
            route.scope.clone(),
            route.resource_type(),
            cid.into(),
        ));
        self.business.insert(Selector::RouteDomain {
            allocation_domain: allocation_domain.clone(),
        });
        self.business.insert(Selector::CanonicalResourceFamily {
            allocation_domain: allocation_domain.clone(),
            cid: canonical(cid)?,
        });
        if let Some(cid) = cid_v0(cid) {
            self.business.insert(Selector::CanonicalResourceFamily {
                allocation_domain,
                cid,
            });
        }
        Ok(())
    }
    fn family(&mut self, provider: &str, cid: &str) {
        self.business.insert(Selector::ResourceFamily {
            provider: provider.into(),
            cid: cid.into(),
        });
        if let Some(cid) = cid_v0(cid) {
            self.business.insert(Selector::ResourceFamily {
                provider: provider.into(),
                cid,
            });
        }
        self.business.insert(Selector::Route {
            provider: provider.into(),
        });
        self.exact("pin_provider_usage", vec![provider.into()]);
        self.exact("pin_management_retirements", vec![provider.into()]);
    }
    fn native_object_resources(&mut self, evidence: &PlanningScope) -> Result<()> {
        let mut selectors = BTreeSet::new();
        for (key, row) in &evidence.originals {
            if key.table != "objects" {
                continue;
            }
            let cid = value(row, "cid").ok_or(SnapshotError::InvalidEvidence)?;
            let canonical_cid = canonical(cid)?;
            for selector in &self.business {
                match selector {
                    Selector::CanonicalResourceFamily {
                        allocation_domain,
                        cid: selected,
                    } if canonical(selected)? == canonical_cid => {
                        selectors.insert(Selector::CanonicalResourceFamily {
                            allocation_domain: allocation_domain.clone(),
                            cid: cid.into(),
                        });
                    }
                    Selector::ResourceFamily {
                        provider,
                        cid: selected,
                    } if canonical(selected)? == canonical_cid => {
                        selectors.insert(Selector::ResourceFamily {
                            provider: provider.clone(),
                            cid: cid.into(),
                        });
                    }
                    _ => {}
                }
            }
        }
        self.business.extend(selectors);
        Ok(())
    }
    fn job_sources(&mut self, evidence: &PlanningScope) -> Result<()> {
        for (key, job) in &evidence.originals {
            if key.table != "pin_jobs"
                || !matches!(value(job, "operation"), Some("submit" | "poll"))
                || !matches!(
                    value(job, "state"),
                    Some("pending" | "retry" | "running" | "blocked")
                )
            {
                continue;
            }
            let lease_id = value(job, "lease_id").ok_or(SnapshotError::InvalidEvidence)?;
            let Some(lease) = evidence.originals.get(&NativeKey {
                table: "pin_leases".into(),
                values: vec![lease_id.into()],
            }) else {
                continue;
            };
            let owner = value(lease, "owner_object_id").ok_or(SnapshotError::InvalidEvidence)?;
            let object_key = NativeKey {
                table: "objects".into(),
                values: vec![owner.into()],
            };
            self.business.insert(Selector::Exact(object_key.clone()));
            if self.business.iter().any(|selector| {
                let Selector::ExactVersion { id } = selector else {
                    return false;
                };
                evidence
                    .originals
                    .get(&NativeKey {
                        table: "object_versions".into(),
                        values: vec![id.clone()],
                    })
                    .is_some_and(|version| value(version, "object_id") == Some(owner))
            }) {
                continue;
            }
            if let Some(object) = evidence.originals.get(&object_key) {
                let bucket = value(object, "bucket").ok_or(SnapshotError::InvalidEvidence)?;
                let key = value(object, "key").ok_or(SnapshotError::InvalidEvidence)?;
                self.exact("buckets", vec![bucket.into()]);
                self.business.insert(Selector::ObjectVersionOwner {
                    bucket: bucket.into(),
                    key: key.into(),
                    object_id: owner.into(),
                });
            }
            for (version_key, version) in &evidence.originals {
                if version_key.table == "object_versions"
                    && value(version, "object_id") == Some(owner)
                {
                    let id = value(version, "id").ok_or(SnapshotError::InvalidEvidence)?;
                    self.authority
                        .insert(AuthoritySelector::ExactVersion(id.into()));
                    self.exact("pin_extension_decisions", vec![id.into()]);
                    self.exact("version_residencies", vec![id.into()]);
                    self.business.insert(Selector::VersionConstraints {
                        version_row_id: id.into(),
                    });
                    self.business.insert(Selector::ManifestBindings {
                        version_row_id: id.into(),
                    });
                }
            }
        }
        Ok(())
    }

    fn lease(&mut self, id: &str, full: bool) {
        self.exact("pin_leases", vec![id.into()]);
        self.business.insert(Selector::LeaseRequirements {
            lease_id: id.into(),
        });
        self.authority.insert(AuthoritySelector::Lease(id.into()));
        if full {
            self.full_leases.insert(id.into());
            self.business.insert(Selector::LeaseTargets {
                lease_id: id.into(),
            });
        }
    }
}

fn cid_v0(value: &str) -> Option<String> {
    let cid = cid::Cid::try_from(value).ok()?;
    (cid.codec() == 0x70)
        .then(|| cid::Cid::new_v0(*cid.hash()).ok())
        .flatten()
        .map(|cid| cid.to_string())
}

pub(super) async fn collect(
    tx: &sea_orm::DatabaseTransaction,
    config: &ValidatedPinningConfig,
    request: &PlanRequest,
    work: &SharedDecodeBudget,
    budget: &mut Budget,
) -> Result<(NativeFrame, PlanningScope)> {
    let (frame, scope) = collect_inner(tx, config, request, work, budget, None).await?;
    Ok((frame.ok_or(SnapshotError::InvalidEvidence)?, scope))
}

async fn collect_inner(
    tx: &sea_orm::DatabaseTransaction,
    config: &ValidatedPinningConfig,
    request: &PlanRequest,
    work: &SharedDecodeBudget,
    budget: &mut Budget,
    original: Option<&PlanningScope>,
) -> Result<(Option<NativeFrame>, PlanningScope)> {
    let fenced = original.is_some();
    let (database_scope, database_identity) = scope::binding(tx).await.map_err(map_error)?;
    let mut evidence = PlanningScope {
        database_scope,
        database_identity,
        memberships: BTreeMap::new(),
        authority_memberships: BTreeMap::new(),
        originals: BTreeMap::new(),
        reads: Vec::new(),
        authority: Vec::new(),
        _resident: Vec::new(),
        work: WorkMeter::default(),
        peak_resident: 0,
    };
    let mut pending = Pending::default();
    if let Some(original) = original {
        for selector in original.memberships.keys() {
            DecodeWork::shared(work).copy(selector).map_err(map_error)?;
        }
        for selector in original.authority_memberships.keys() {
            DecodeWork::shared(work).copy(selector).map_err(map_error)?;
        }
        pending
            .business
            .extend(original.memberships.keys().cloned());
        pending
            .authority
            .extend(original.authority_memberships.keys().cloned());
    }
    pending.authority.insert(AuthoritySelector::RecordMembers(
        evidence.database_scope.clone(),
        request.plan_id.clone(),
    ));
    for item in &request.items {
        if let Subject::Provider { route } = &item.subject {
            // Only the explicit mutation closure is resident. Completeness of
            // the domain is independently established by the sealed inventory.
            let original = route
                .original
                .as_ref()
                .ok_or(SnapshotError::InvalidEvidence)?;
            let domain = storage::admission::allocation_domain(original)
                .map_err(|_| SnapshotError::InvalidEvidence)?;
            pending.business.insert(Selector::RouteDomain {
                allocation_domain: domain.clone(),
            });
            pending.exact("pin_management_retirements", vec![domain.clone()]);
            match &item.action {
                Action::RetireWith {
                    selection: super::super::RetirementSelection::Migrate { targets },
                } => {
                    for target in targets {
                        pending.exact("pin_lease_targets", vec![target.source_target_id.clone()]);
                        pending.lease(&target.lease_id, true);
                        pending.family(&domain, &target.cid);
                        pending.authority.insert(AuthoritySelector::RecordMembers(
                            evidence.database_scope.clone(),
                            target.plan_id.clone(),
                        ));
                    }
                }
                Action::RetireWith {
                    selection: super::super::RetirementSelection::ControlledCleanup { resources },
                } => {
                    for resource in resources {
                        pending.family(&domain, &resource.key.cid);
                    }
                }
                Action::RetireWith {
                    selection:
                        super::super::RetirementSelection::ReleaseRootReferences { references },
                } => {
                    for key in references {
                        pending.exact(&key.table, key.values.clone());
                        pending.exact("zip_root_builds", key.values[..3].to_vec());
                        pending.exact("zip_batches", vec![key.values[0].clone()]);
                    }
                }
                _ => {}
            }
            continue;
        }
        let Subject::Object { identity, target } = &item.subject else {
            unreachable!()
        };
        canonical(&identity.cid)?;
        pending.identity(identity);
        let cid = match target {
            TargetSelection::New(route) => {
                if let Some(raw) = &route.original {
                    pending.route(raw, &identity.cid)?;
                }
                for provider in &config.providers {
                    if super::route(&provider.route_snapshot()) == *route
                        || provider.identity.provider_id == route.provider_id
                    {
                        pending.route(&provider.route_snapshot(), &identity.cid)?;
                        pending.family(&provider.identity.allocation_key(), &identity.cid);
                    }
                }
                identity.cid.as_str()
            }
            TargetSelection::Existing { target_id, source } => {
                pending.exact("pin_lease_targets", vec![target_id.clone()]);
                pending
                    .authority
                    .insert(AuthoritySelector::Target(target_id.clone()));
                if let Some(source) = source {
                    canonical(&source.cid)?;
                    pending.identity(source);
                }
                source
                    .as_ref()
                    .map_or(identity.cid.as_str(), |source| source.cid.as_str())
            }
        };
        if let Action::Create(intent) | Action::Backfill(intent) | Action::Reapply(intent) =
            &item.action
        {
            for provider in &config.providers {
                if intent
                    .requirements
                    .provider_ids
                    .contains(&provider.identity.provider_id)
                {
                    pending.route(&provider.route_snapshot(), cid)?;
                    pending.family(&provider.identity.allocation_key(), cid);
                }
            }
        }
        if let Action::Migrate { destination } = &item.action {
            if let Some(raw) = &destination.original {
                pending.route(raw, cid)?;
            }
            for provider in &config.providers {
                if provider.identity.provider_id == destination.provider_id {
                    pending.route(&provider.route_snapshot(), cid)?;
                    pending.family(&provider.identity.allocation_key(), cid);
                }
            }
        }
    }
    loop {
        budget.check().map_err(map_error)?;
        let selectors = pending
            .business
            .iter()
            .filter(|selector| !evidence.memberships.contains_key(*selector))
            .cloned()
            .collect::<Vec<_>>();
        for batch in selectors.chunks(128) {
            let spec = ScopeSpec::new(batch.to_vec()).map_err(map_error)?;
            if fenced {
                scope::fence_memberships(tx, &spec)
                    .await
                    .map_err(map_error)?;
            }
            let mut read = scope::collect_scope(tx, &spec, budget)
                .await
                .map_err(map_error)?;
            if read.database_scope != evidence.database_scope
                || read.database_identity != evidence.database_identity
            {
                return Err(SnapshotError::Changed);
            }
            evidence.memberships.extend(read.memberships().clone());
            insert(&mut evidence, read.rows(), budget, work)?;
            read.release_rows();
            evidence.reads.push(read);
        }
        edges::expand(&mut pending, &evidence, request, work)?;
        pending.native_object_resources(&evidence)?;
        pending.job_sources(&evidence)?;
        if pending
            .business
            .iter()
            .any(|selector| !evidence.memberships.contains_key(selector))
        {
            continue;
        }
        let new_authority = pending
            .authority
            .iter()
            .any(|selector| !evidence.authority_memberships.contains_key(selector));
        let selectors = if new_authority {
            pending.authority.iter().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let previous_seals = if new_authority {
            let previous = std::mem::take(&mut evidence.authority);
            let mut seals = BTreeMap::new();
            for view in &previous {
                for seal in &view.proof.seals {
                    DecodeWork::shared(work).copy(seal).map_err(map_error)?;
                    if seals
                        .insert(seal.source.clone(), seal.clone())
                        .is_some_and(|old| old != *seal)
                    {
                        return Err(SnapshotError::Changed);
                    }
                }
            }
            seals
        } else {
            BTreeMap::new()
        };
        for batch in selectors.chunks(128) {
            // Discovery retains each empty membership while sharing originals
            // across selectors naming the same authority graph.
            if fenced {
                locator::fence_authority(tx).await.map_err(map_error)?;
            }
            let read = locator::load_authority(tx, batch, budget)
                .await
                .map_err(map_error)?;
            if read.database_scope != evidence.database_scope
                || read.database_identity != evidence.database_identity
            {
                return Err(SnapshotError::Changed);
            }
            for membership in &read.proof.memberships {
                if evidence
                    .authority_memberships
                    .get(&membership.selector)
                    .is_some_and(|prior| prior != &membership.sources)
                {
                    return Err(SnapshotError::Changed);
                }
                evidence
                    .authority_memberships
                    .insert(membership.selector.clone(), membership.sources.clone());
            }
            insert(&mut evidence, &read.originals, budget, work)?;
            evidence.authority.push(read);
        }
        for (key, seal) in previous_seals {
            if !evidence
                .authority
                .iter()
                .flat_map(|view| &view.proof.seals)
                .any(|current| current.source == key && *current == seal)
            {
                return Err(SnapshotError::Changed);
            }
        }
        edges::expand(&mut pending, &evidence, request, work)?;
        pending.native_object_resources(&evidence)?;
        pending.job_sources(&evidence)?;
        if pending
            .business
            .iter()
            .all(|selector| evidence.memberships.contains_key(selector))
            && pending
                .authority
                .iter()
                .all(|selector| evidence.authority_memberships.contains_key(selector))
        {
            // A fenced post-image grants no authority. Its caller compares every
            // original against the executor's validated exact write set and
            // checks source transitions. All original predicates are retained.
            if fenced {
                return Ok((None, evidence));
            }
            let frame = scope::merge_reads_originals(
                &evidence.reads,
                &evidence.authority,
                Some(&evidence.originals),
                budget,
            )
            .map_err(map_error)?;
            edges::trusted(&mut pending, &evidence, &frame, work)?;
            if pending
                .business
                .iter()
                .all(|selector| evidence.memberships.contains_key(selector))
                && pending
                    .authority
                    .iter()
                    .all(|selector| evidence.authority_memberships.contains_key(selector))
            {
                crate::store::pinning::management::actions::points::planning_points_with_work(
                    &frame,
                    work.clone(),
                )
                .map_err(map_error)?;
                return Ok((Some(frame), evidence));
            }
        }
    }
}

fn insert(
    evidence: &mut PlanningScope,
    rows: &BTreeMap<NativeKey, NativeRow>,
    budget: &mut Budget,
    work: &SharedDecodeBudget,
) -> Result<()> {
    for (key, row) in rows {
        if let Some(original) = evidence.originals.get(key) {
            if original != row {
                return Err(SnapshotError::Changed);
            }
            continue;
        }
        let bytes = row
            .iter()
            .map(|(column, value)| column.len() + value.as_ref().map_or(0, String::len))
            .sum::<usize>();
        evidence
            ._resident
            .push(budget.reserve(bytes + row.len() * 128).map_err(map_error)?);
        budget.work.copied_bytes = budget
            .work
            .copied_bytes
            .checked_add(bytes as u64)
            .ok_or(SnapshotError::LimitExceeded)?;
        DecodeWork::shared(work)
            .copy(&(key, row))
            .map_err(map_error)?;
        evidence.originals.insert(key.clone(), row.clone());
    }
    Ok(())
}
