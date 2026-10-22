use super::{
    spool::{get, one},
    *,
};
use crate::store::pinning::management::native::meter::Budget;
use sea_orm::DatabaseTransaction;

/// Only the explicitly selected write/dependency closure is resident.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetirementMutationScope {
    pub routes: Vec<RouteSnapshot>,
    pub targets: Vec<TargetSnapshot>,
    pub remotes: Vec<RemoteSnapshot>,
    pub historical_remotes: Vec<RemoteSnapshot>,
    pub root_references: Vec<RootReferenceSnapshot>,
}

pub struct RetirementReview<'a> {
    pub(super) spool: &'a InventorySpool,
    checkpoint: SourceCheckpoint,
    totals: InventoryTotals,
}

/// Current responsibility projection, separate from any frozen receipt. The
/// executor still evaluates selected migration destinations at its fresh clock.
pub struct CurrentRetirementInventory {
    pub observed_at: chrono::DateTime<chrono::Utc>,
    pub responsibility: RetirementStep,
}
/// Owned, non-serializable source prerequisite for bounded transaction closures.
/// It contains neither a spool handle nor any full responsibility collection.
pub struct RetirementFence {
    reference: RetirementInventoryRef,
    checkpoint: SourceCheckpoint,
    totals: InventoryTotals,
}
/// Exact writes checked against the live inventory before publication. Finish
/// inside the same caller-owned transaction, while its source fences are held.
/// The resulting checkpoint must be persisted with the exact transitions and
/// receipts atomically; it is not a newly reduced current-status inventory.
pub struct RetirementTransition {
    checkpoint: SourceCheckpoint,
    changes: Vec<SourceTransition>,
}
impl RetirementTransition {
    pub async fn finish(
        self,
        tx: &DatabaseTransaction,
        budget: &mut Budget,
        limits: &RunLimits,
    ) -> Result<SourceCheckpoint> {
        let run = super::spool::Run::new(limits);
        run.check()?;
        let checkpoint = self
            .checkpoint
            .verify_transition_after(tx, &self.changes, budget)
            .await?;
        run.check()?;
        Ok(checkpoint)
    }
}
impl RetirementFence {
    pub fn reference(&self) -> &RetirementInventoryRef {
        &self.reference
    }
    /// The executor supplies only its bounded, exact original/final rows and
    /// actual write counts. This verifies prerequisites, not business permission.
    pub async fn begin_transition(
        self,
        tx: &DatabaseTransaction,
        changes: Vec<SourceTransition>,
        budget: &mut Budget,
        limits: &RunLimits,
    ) -> Result<RetirementTransition> {
        self.seal(tx, limits).await?;
        self.checkpoint
            .verify_transition_before(tx, &changes, budget)
            .await?;
        super::spool::Run::new(limits).check()?;
        Ok(RetirementTransition {
            checkpoint: self.checkpoint,
            changes,
        })
    }
    /// Call on the prepared review inside the executor's bounded transaction.
    /// Only current-source fencing occurs here: no spool IO or reduction. This
    /// is an inventory prerequisite, not authorization for an arbitrary mutation.
    pub async fn seal(&self, tx: &DatabaseTransaction, limits: &RunLimits) -> Result<()> {
        let run = super::spool::Run::new(limits);
        run.check()?;
        if self.checkpoint.is_status() {
            return Err(ManagementError::InvalidEvidence);
        }
        self.checkpoint.fence(tx).await?;
        run.check()
    }
    pub async fn current_status(
        &self,
        tx: &DatabaseTransaction,
        limits: &RunLimits,
    ) -> Result<CurrentRetirementInventory> {
        let run = super::spool::Run::new(limits);
        run.check()?;
        if !self.checkpoint.is_status() {
            return Err(ManagementError::InvalidEvidence);
        }
        self.checkpoint.check(tx).await?;
        run.check()?;
        Ok(CurrentRetirementInventory {
            observed_at: self.checkpoint.observed_at,
            responsibility: retirement_step(&self.totals),
        })
    }
}
impl<'a> RetirementReview<'a> {
    pub(crate) fn retain_source(&self) -> InventorySpool {
        InventorySpool {
            task: self.spool.task.clone(),
            path: self.spool.path.clone(),
            shared_route: self.spool.shared_route.clone(),
            db: self.spool.db.clone(),
            reference: self.spool.reference.clone(),
            source_scanned: self.spool.source_scanned,
            reduced_here: std::sync::atomic::AtomicBool::new(true),
            verification: Default::default(),
            shared_source: self.spool.shared_source.clone(),
            shared_verification: Default::default(),
        }
    }
    pub fn prepare_fence(&self) -> Result<RetirementFence> {
        if !self.spool.source_scanned {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(RetirementFence {
            reference: self.reference().clone(),
            checkpoint: self.checkpoint.clone(),
            totals: self.totals.clone(),
        })
    }
    pub async fn seal(&self, tx: &DatabaseTransaction, limits: &RunLimits) -> Result<()> {
        self.prepare_fence()?.seal(tx, limits).await
    }
    pub async fn current_status(
        &self,
        tx: &DatabaseTransaction,
        limits: &RunLimits,
    ) -> Result<CurrentRetirementInventory> {
        self.prepare_fence()?.current_status(tx, limits).await
    }
}
impl InventorySpool {
    /// Frozen review uses the archived original clock/configuration, never the
    /// current registry. This handle alone cannot authorize a database write.
    pub async fn frozen_review(&self) -> Result<RetirementReview<'_>> {
        if !self.reduced_here.load(std::sync::atomic::Ordering::Acquire) {
            return Err(ManagementError::InvalidEvidence);
        }
        let checkpoint = self.checkpoint().await?;
        let row = one(&self.db, "SELECT phase FROM state WHERE id=1", vec![])
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        if get::<i64>(&row, "phase")? != 2 || !checkpoint.complete() {
            return Err(ManagementError::InvalidEvidence);
        }
        let totals = self
            .lookup("totals", "inventory")
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        Ok(RetirementReview {
            spool: self,
            checkpoint,
            totals,
        })
    }
}
impl RetirementReview<'_> {
    pub fn reference(&self) -> &RetirementInventoryRef {
        self.spool.reference()
    }
    pub fn checkpoint(&self) -> &SourceCheckpoint {
        &self.checkpoint
    }
    pub fn totals(&self) -> &InventoryTotals {
        &self.totals
    }
    /// Membership in the full reduced responsibility set, independently of the
    /// explicitly selected mutation roots. Released originals remain archived.
    pub async fn contains_root_responsibility(&self, key: &NativeKey) -> Result<bool> {
        if key.table != "zip_root_references" || key.values.len() != 6 {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(self
            .spool
            .lookup::<NativeKey>("root", &encode(key)?)
            .await?
            .as_ref()
            == Some(key))
    }
    /// One exact registered source row anchors the canonical retirement marker;
    /// the disk inventory, not this point, proves the complete alias membership.
    pub async fn route_anchor(&self) -> Result<NativeKey> {
        self.spool
            .lookup("registered_anchor", &encode(&self.checkpoint.route)?)
            .await?
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub fn step(&self) -> RetirementStep {
        retirement_step(&self.totals)
    }
    pub async fn configuration(&self) -> Result<ConfigurationBinding> {
        self.spool
            .lookup("configuration", "inventory")
            .await?
            .ok_or(ManagementError::InvalidEvidence)
    }

    pub async fn capture(
        &self,
        request: &ItemRequest,
        mut mutation: RetirementMutationScope,
    ) -> Result<RetirementCapture> {
        super::super::validate_request(&PlanRequest {
            plan_id: "inventory".into(),
            items: vec![request.clone()],
        })
        .map_err(|_| ManagementError::InvalidEvidence)?;
        let Subject::Provider { route } = &request.subject else {
            return Err(ManagementError::InvalidEvidence);
        };
        if route != &self.checkpoint.route {
            return Err(ManagementError::Confirmation);
        }
        mutation.routes = vec![
            self.spool
                .lookup("registered_identity", &encode(route)?)
                .await?
                .ok_or(ManagementError::InvalidEvidence)?,
        ];
        if matches!(
            request.action,
            Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup { .. }
            }
        ) {
            mutation.remotes = self.bounded_facts("current").await?;
            for remote in &mutation.remotes {
                if !mutation
                    .routes
                    .iter()
                    .any(|route| route.identity == remote.route)
                {
                    mutation.routes.push(
                        self.spool
                            .lookup("route_revision", &encode(&remote.route)?)
                            .await?
                            .ok_or(ManagementError::InvalidEvidence)?,
                    );
                }
            }
        }
        if let Action::RetireWith {
            selection: RetirementSelection::ReleaseRootReferences { references },
        } = &request.action
        {
            mutation.root_references.clear();
            for key in references {
                let original = self
                    .spool
                    .row(key)
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                let build = self
                    .spool
                    .row(&NativeKey {
                        table: "zip_root_builds".into(),
                        values: key.values[..3].to_vec(),
                    })
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                let batch = self
                    .spool
                    .row(&NativeKey {
                        table: "zip_batches".into(),
                        values: vec![key.values[0].clone()],
                    })
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                let owner = crate::store::pinning::management::native::value(&batch, "owner")
                    .ok_or(ManagementError::InvalidEvidence)?
                    .to_owned();
                let verification_recorded = crate::store::pinning::management::native::value(
                    &original,
                    "verification_receipt",
                )
                .is_some_and(|v| !v.is_empty());
                mutation.root_references.push(RootReferenceSnapshot {
                    key: key.clone(),
                    owner,
                    reference_tuple: public_row(&key.table, original),
                    build_tuple: public_row("zip_root_builds", build),
                    batch_tuple: public_row("zip_batches", batch),
                    verification_recorded,
                });
            }
        }
        self.validate_selection(request, &mutation).await?;
        Ok(RetirementCapture {
            inventory: self.reference().clone(),
            mutation_scope: mutation,
            totals: self.totals.clone(),
        })
    }

    async fn bounded_facts<T: serde::de::DeserializeOwned>(&self, kind: &str) -> Result<Vec<T>> {
        let mut after = String::new();
        let mut result = Vec::new();
        let mut bytes = 0usize;
        while let Some(row)=one(&self.spool.db,"SELECT identity,payload FROM facts WHERE kind=$1 AND identity>$2 ORDER BY identity LIMIT 1",vec![kind.into(),after.into()]).await? {
            if result.len()==super::super::MAX_ITEMS {return Err(ManagementError::Budget)}
            after=get(&row,"identity")?;let payload:String=get(&row,"payload")?;
            bytes=bytes.checked_add(payload.len()).ok_or(ManagementError::Budget)?;
            if bytes>super::super::MAX_TEXT_BYTES {return Err(ManagementError::Budget)}
            result.push(decode(&payload)?);
        }
        Ok(result)
    }

    async fn validate_selection(
        &self,
        request: &ItemRequest,
        mutation: &RetirementMutationScope,
    ) -> Result<()> {
        let selection = match &request.action {
            Action::Retire => &RetirementSelection::Retain,
            Action::RetireWith { selection } => selection,
            _ => return Err(ManagementError::InvalidEvidence),
        };
        // Limits apply to the requested atomic mutation, not the inventory.
        if mutation.targets.len() + mutation.remotes.len() + mutation.root_references.len()
            > super::super::MAX_ITEMS
        {
            return Err(ManagementError::Budget);
        }
        match selection {
            RetirementSelection::Retain => {
                if !mutation.targets.is_empty()
                    || !mutation.remotes.is_empty()
                    || !mutation.historical_remotes.is_empty()
                    || !mutation.root_references.is_empty()
                {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
            RetirementSelection::Migrate { targets } => {
                if !self.totals.attribution_complete
                    || self.totals.roots != 0
                    || targets.len() != self.totals.active_targets
                    || targets.is_empty()
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                for target in targets {
                    let row = one(
                        &self.spool.db,
                        "SELECT lease_id,cid,active FROM target_edges WHERE target_id=$1",
                        vec![target.source_target_id.clone().into()],
                    )
                    .await?
                    .ok_or(ManagementError::InvalidEvidence)?;
                    if get::<String>(&row, "lease_id")? != target.lease_id
                        || get::<String>(&row, "cid")? != target.cid
                        || get::<i64>(&row, "active")? != 1
                    {
                        return Err(ManagementError::Confirmation);
                    }
                    let Some(proof) = mutation.targets.iter().find(|proof| {
                        proof.id == target.source_target_id
                            && proof.lease.id == target.lease_id
                            && proof.cid == target.cid
                    }) else {
                        return Err(ManagementError::InvalidEvidence);
                    };
                    let original = self
                        .spool
                        .lookup::<NativeRow>("target", &proof.id)
                        .await?
                        .ok_or(ManagementError::InvalidEvidence)?;
                    if original != proof.native_tuple {
                        return Err(ManagementError::Changed);
                    }
                }
            }
            RetirementSelection::ControlledCleanup { resources } => {
                if !self.totals.attribution_complete
                    || self.totals.roots != 0
                    || self.totals.historical_resources != 0
                    || resources.len() != self.totals.current_resources
                {
                    return Err(ManagementError::InvalidEvidence);
                }
                let mut after = String::new();
                let mut visited = 0;
                while let Some(row)=one(&self.spool.db,"SELECT identity,payload FROM facts WHERE kind='current' AND identity>$1 ORDER BY identity LIMIT 1",vec![after.into()]).await? {
                    after=get(&row,"identity")?;
                    let remote:RemoteSnapshot=decode(&get::<String>(&row,"payload")?)?;
                    if remote.ownership!=Ownership::ApplicationCreated||remote.application_references!=Some(0)||remote.inflight_operations!=Some(0)||remote.has_unknown_responsibility()||remote.retained||remote.cleanup_pending||remote.effect!=SideEffect::Present||matches!(remote.route.resource_type,ResourceType::RpcPin|ResourceType::ClusterPin)
                        ||!resources.iter().any(|r|r.key==remote.key&&r.route==remote.route&&r.epoch==remote.epoch&&Some(&r.resource_id)==remote.resource_id.as_ref()) {return Err(ManagementError::InvalidEvidence)}
                    let route=self.spool.lookup::<RouteSnapshot>("route_revision",&encode(&remote.route)?).await?.ok_or(ManagementError::InvalidEvidence)?;
                    if !route.available||route.cleanup!=CleanupMode::Managed {return Err(ManagementError::InvalidEvidence)}
                    visited+=1;
                }
                if visited != resources.len() {
                    return Err(ManagementError::InvalidEvidence);
                }
            }
            RetirementSelection::ReleaseRootReferences { references } => {
                if references.len() != mutation.root_references.len() {
                    return Err(ManagementError::InvalidEvidence);
                }
                for key in references {
                    let proof = mutation
                        .root_references
                        .iter()
                        .find(|proof| &proof.key == key)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    if !proof.verification_recorded
                        || self
                            .spool
                            .lookup::<NativeKey>("root", &encode(key)?)
                            .await?
                            .as_ref()
                            != Some(key)
                        || self
                            .spool
                            .row(key)
                            .await?
                            .map(|row| public_row(&key.table, row))
                            .as_ref()
                            != Some(&proof.reference_tuple)
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    let build = NativeKey {
                        table: "zip_root_builds".into(),
                        values: key.values[..3].to_vec(),
                    };
                    let batch = NativeKey {
                        table: "zip_batches".into(),
                        values: vec![key.values[0].clone()],
                    };
                    if self
                        .spool
                        .row(&build)
                        .await?
                        .map(|row| public_row(&build.table, row))
                        .as_ref()
                        != Some(&proof.build_tuple)
                        || self
                            .spool
                            .row(&batch)
                            .await?
                            .map(|row| public_row(&batch.table, row))
                            .as_ref()
                            != Some(&proof.batch_tuple)
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    use crate::store::pinning::management::native::value;
                    if !super::super::root_references::logically_active(&proof.reference_tuple)
                        || proof.owner.is_empty()
                        || value(&proof.batch_tuple, "owner") != Some(&proof.owner)
                        || !matches!(
                            value(&proof.reference_tuple, "state"),
                            Some("adopted" | "retained")
                        )
                        || value(&proof.build_tuple, "status") != Some("verified")
                    {
                        return Err(ManagementError::InvalidEvidence);
                    }
                }
            }
        }
        Ok(())
    }
}
fn retirement_step(totals: &InventoryTotals) -> RetirementStep {
    RetirementStep {
        stop_new_allocations: true,
        needs_attention: !totals.attribution_complete
            || !totals.routes_available
            || totals.unknown > 0,
        leases: totals.leases,
        retained: totals.retained,
        unknown: totals.unknown,
        inflight: totals.inflight,
        cleanup_pending: totals.cleanup_pending,
        root_references: totals.roots,
    }
}

fn public_row(table: &str, mut row: NativeRow) -> NativeRow {
    for (column, cell) in &mut row {
        if crate::store::pinning::management::native::confidential(table, column) {
            *cell = None;
        }
    }
    row
}
