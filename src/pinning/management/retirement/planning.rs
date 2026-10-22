use super::*;
use crate::{
    pinning::management::DryRunPlan,
    store::pinning::management::{
        records::SharedDecodeBudget,
        transaction::{ManagementTransaction, TransactionFailure},
    },
};
use sea_orm::DatabaseConnection;

pub struct PreparedPlanning {
    pub plan: DryRunPlan,
    pub(super) workspace: Option<Workspace>,
    pub(super) union: Option<PlanUnion>,
    pub(super) spools: Vec<InventorySpool>,
    pub(super) task_envelope: Option<(u64, u64)>,
}
impl PreparedPlanning {
    /// Keep unrecorded fresh-capture work inside the hard task ceiling while
    /// confirmation advances. Use the original limits for preflight/accounting.
    pub fn reserve_capture(
        &self,
        budget: &crate::store::pinning::management::native::meter::Budget,
        limits: &RunLimits,
    ) -> Result<RunLimits> {
        match self.spools.first().and_then(|spool| spool.task.as_ref()) {
            Some(task) => task.reserve_capture(budget, limits),
            None => Ok(limits.with_task_caps(limits.total_rows, limits.total_bytes)),
        }
    }
    /// Charge a fresh CLI capture when an invocation ends Incomplete without an
    /// output handoff. Ready invocations pass that same Budget to preflight.
    pub async fn account_capture(
        &self,
        budget: &mut crate::store::pinning::management::native::meter::Budget,
        limits: &RunLimits,
    ) -> Result<()> {
        let Some(task) = self.spools.first().and_then(|spool| spool.task.as_ref()) else {
            return Ok(());
        };
        task.account_capture(limits, budget).await
    }
    pub async fn collect(
        db: &DatabaseConnection,
        request: &PlanRequest,
        local: &Snapshot,
        work: SharedDecodeBudget,
    ) -> Result<Self> {
        let mut limits = RunLimits::default();
        limits.pages = 512;
        Self::collect_with_limits(db, request, local, work, &limits).await
    }
    pub async fn collect_with_limits(
        db: &DatabaseConnection,
        request: &PlanRequest,
        local: &Snapshot,
        work: SharedDecodeBudget,
        limits: &RunLimits,
    ) -> Result<Self> {
        super::spool::Run::new(limits).check()?;
        if !request
            .items
            .iter()
            .any(|item| matches!(item.subject, Subject::Provider { .. }))
        {
            let plan = super::super::dry_run_with_work(request, local, work).map_err(safe)?;
            return Ok(Self {
                plan,
                workspace: None,
                union: None,
                spools: Vec::new(),
                task_envelope: None,
            });
        }
        let mut workspace = Workspace::create()?;
        let mut spools: Vec<InventorySpool> = Vec::new();
        let mut union = None;
        let result = async {
            for item in &request.items {
                let Subject::Provider { route } = &item.subject else {
                    continue;
                };
                let checkpoint = if let Some(primary) = spools.first() {
                    let mut checkpoint: SourceCheckpoint = primary.checkpoint().await?.rewind();
                    checkpoint.route = route.clone();
                    checkpoint
                } else {
                    let mut tx = ManagementTransaction::begin(
                        db,
                        super::spool::Run::new(limits).remaining(),
                        limits.cancellation.clone(),
                    )
                    .await
                    .map_err(TransactionFailure::management_error)?;
                    let route = route.clone();
                    let checkpoint = tx
                        .run(move |tx, _| {
                            Box::pin(async move { SourceCheckpoint::start(tx, route).await })
                        })
                        .await
                        .map_err(TransactionFailure::management_error)?;
                    tx.rollback()
                        .await
                        .map_err(TransactionFailure::management_error)?;
                    checkpoint
                };
                let (path, file) = workspace.reserve().await?;
                let mut spool = InventorySpool::create(&path, file, &checkpoint).await?;
                if let Some(primary) = spools.first() {
                    spool
                        .share_original(primary, checkpoint.route.clone(), true)
                        .await?;
                }
                spools.push(spool);
                let spool = spools.last().ok_or(ManagementError::InvalidEvidence)?;
                super::spool::task_run(&spool.db, limits).await?;
                let caps = checkpoint::task_caps(
                    (limits.total_rows, limits.total_bytes),
                    limits,
                    checkpoint::total_work(&spools, None).await?,
                    spool.task_work().await?,
                )?;
                if spool.scan(db, &caps).await? == Progress::Pending {
                    return Err(ManagementError::Budget);
                }
                let caps = checkpoint::task_caps(
                    (limits.total_rows, limits.total_bytes),
                    limits,
                    checkpoint::total_work(&spools, None).await?,
                    spool.task_work().await?,
                )?;
                if spool
                    .reduce(
                        local
                            .configuration
                            .as_ref()
                            .ok_or(ManagementError::Confirmation)?,
                        &caps,
                    )
                    .await?
                    == Progress::Pending
                {
                    return Err(ManagementError::Budget);
                }
            }
            let mut shared_work = checkpoint::total_work(&spools, None).await?;
            if let Some((primary, others)) = spools.split_first_mut() {
                for spool in others {
                    let before = spool.task_work().await?;
                    let caps = checkpoint::task_caps(
                        (limits.total_rows, limits.total_bytes),
                        limits,
                        shared_work,
                        before,
                    )?;
                    if spool.verify_shared_source(primary, &caps).await? == Progress::Pending {
                        return Err(ManagementError::Budget);
                    }
                    let after = spool.task_work().await?;
                    shared_work.rows += after.rows - before.rows;
                    shared_work.bytes += after.bytes - before.bytes;
                }
            }
            let (path, file) = workspace.reserve().await?;
            union = Some(PlanUnion::create(&path, file).await?);
            let union_ref = union.as_ref().ok_or(ManagementError::InvalidEvidence)?;
            union_ref.start_task(limits).await?;
            let task = std::sync::Arc::new(super::task_budget::TaskBudget::new(
                &spools,
                union_ref,
                (limits.total_rows, limits.total_bytes),
            ));
            for spool in &mut spools {
                spool.task = Some(task.clone());
            }
            let mut reviews = Vec::new();
            for spool in &spools {
                reviews.push(spool.frozen_review().await?);
            }
            let mutations = mutation_scopes(request, local);
            let caps = checkpoint::task_caps(
                (limits.total_rows, limits.total_bytes),
                limits,
                task.total().await?,
                union_ref.task_work().await?,
            )?;
            let plan = super::super::dry_run_retirement(
                request,
                local,
                &reviews,
                &mutations,
                union.as_ref().ok_or(ManagementError::InvalidEvidence)?,
                &caps,
            )
            .await
            .map_err(safe)?
            .ok_or(ManagementError::Budget)?;
            Ok(plan)
        }
        .await;
        let plan = match result {
            Ok(plan) => plan,
            Err(error) => {
                close_resources(Some(workspace), &spools, union).await?;
                return Err(error);
            }
        };
        Ok(Self {
            plan,
            workspace: Some(workspace),
            union,
            spools,
            task_envelope: None,
        })
    }
    pub async fn close(self) -> Result<()> {
        close_resources(
            if self.task_envelope.is_some() {
                None
            } else {
                self.workspace
            },
            &self.spools,
            self.union,
        )
        .await
    }
    pub fn checkpoint_path(&self) -> Option<&std::path::Path> {
        if self.task_envelope.is_some() {
            self.workspace.as_ref().map(Workspace::path)
        } else {
            None
        }
    }
    pub async fn task_work(&self) -> Result<TaskWork> {
        checkpoint::total_work(&self.spools, self.union.as_ref()).await
    }
    /// Explicitly remove only this task's tracked files after closing handles.
    pub async fn cleanup(self) -> Result<()> {
        close_resources(self.workspace, &self.spools, self.union).await
    }
    pub async fn export<W: std::io::Write>(
        &self,
        writer: &mut super::super::wire::v3::Writer<W>,
        header: super::super::wire::v3::planning::Header<'_>,
        request: &PlanRequest,
        (snapshot, scope): (&Snapshot, Option<&super::super::db_snapshot::PlanningScope>),
        acks: Option<&[String]>,
        limits: &RunLimits,
    ) -> Result<()> {
        use super::super::wire::v3::{Section, planning};
        super::spool::Run::new(limits).check()?;
        let Some(primary) = self.plan.retirement_sources.first() else {
            return planning::local(writer, header, request, snapshot, &self.plan, acks, scope)
                .map_err(safe);
        };
        let capped;
        let limits = if let Some(task) = &primary.task {
            capped = task.caps(primary, limits).await?;
            &capped
        } else {
            limits
        };
        planning::prefix(
            writer,
            header,
            request,
            &snapshot.configuration,
            self.plan.items(),
            *self.plan.summary(),
            acks,
        )
        .map_err(safe)?;
        for entry in self.plan.items() {
            super::spool::Run::new(limits).check()?;
            let Some(Capture::Provider(capture)) = &entry.capture else {
                continue;
            };
            let spool = self
                .plan
                .retirement_sources
                .iter()
                .find(|spool| spool.reference() == &capture.inventory)
                .ok_or(ManagementError::Confirmation)?;
            let review = spool.frozen_review().await?;
            if spool.reference() == primary.reference() {
                review.stage_evidence(&request.plan_id, entry).await?;
            } else {
                review
                    .stage_evidence_into(primary, &request.plan_id, entry)
                    .await?;
            }
        }
        primary.write_archive_sections(writer, limits).await?;
        for (i, entry) in self.plan.items().iter().enumerate() {
            planning::fact(
                writer,
                Section::Result,
                "management_capture",
                i,
                &entry.capture,
            )
            .map_err(safe)?;
        }
        Ok(())
    }
}
pub(super) async fn close_resources(
    mut workspace: Option<Workspace>,
    spools: &[InventorySpool],
    union: Option<PlanUnion>,
) -> Result<()> {
    let mut result = Ok(());
    if let Some(workspace) = &mut workspace
        && let Err(error) = super::confirmation::reconcile_retained(workspace, spools).await
    {
        result = Err(error);
    }
    for spool in spools {
        if let Err(error) = spool.close().await {
            result = Err(error);
        }
    }
    if let Some(union) = union
        && let Err(error) = union.close().await
    {
        result = Err(error);
    }
    if let Some(workspace) = workspace
        && result.is_ok()
        && let Err(error) = workspace.cleanup()
    {
        result = Err(error);
    }
    result
}
pub(super) fn safe(error: SafeError) -> ManagementError {
    if error == SafeError::LimitExceeded {
        ManagementError::Budget
    } else if error == SafeError::SnapshotChanged {
        ManagementError::Changed
    } else {
        ManagementError::Confirmation
    }
}

pub(super) fn mutation_scopes(
    request: &PlanRequest,
    local: &Snapshot,
) -> Vec<(String, RetirementMutationScope)> {
    request
        .items
        .iter()
        .filter_map(|item| {
            if let Action::RetireWith {
                selection: RetirementSelection::Migrate { targets },
            } = &item.action
            {
                Some((
                    item.item_id.clone(),
                    RetirementMutationScope {
                        targets: local
                            .targets
                            .iter()
                            .filter(|target| {
                                targets
                                    .iter()
                                    .any(|selected| selected.source_target_id == target.id)
                            })
                            .cloned()
                            .collect(),
                        ..Default::default()
                    },
                ))
            } else {
                None
            }
        })
        .collect()
}
