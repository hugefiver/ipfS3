//! Explicit, operator-selected local planning progress. Never an import or authority.
use super::*;
use crate::store::pinning::management::{
    native,
    transaction::{ManagementTransaction, TransactionFailure},
};
use sea_orm::DatabaseConnection;
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

const METADATA: &str = "planning.json";
const UNION: &str = "union.sqlite";
const MAX_METADATA: usize = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenPlanning {
    version: u32,
    principal: String,
    request: PlanRequest,
    local: Snapshot,
    origin: SourceCheckpoint,
    rows_limit: u64,
    bytes_limit: u64,
}

/// Private metadata for capturing a fresh local Snapshot before resume. The
/// caller supplies its authenticated principal and current configuration again.
/// Reading this input does not attest a previous scan or grant write permission.
pub struct PlanningCheckpointInput {
    pub principal: String,
    pub request: PlanRequest,
    pub configuration: ConfigurationBinding,
    pub database_scope: String,
    pub database_identity: String,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

pub enum PlanningProgress {
    Incomplete,
    Complete(Box<PreparedPlanning>),
}

/// Local task progress is accepted only from an explicitly selected, verified
/// owner-private workspace. This API is not an archive/import reader. Confirm
/// and apply still require the original source/identity/vector fences and ACKs.
pub struct PlanningCheckpoint {
    workspace: Option<Workspace>,
    frozen: FrozenPlanning,
    local: Snapshot,
    spools: Vec<InventorySpool>,
    union: Option<PlanUnion>,
}

/// Read-only committed authority paired with its retained output accounting.
pub struct CommittedPlanning {
    binding: crate::store::pinning::management::records::ConfirmedBinding,
    workspace: Workspace,
    spools: Vec<InventorySpool>,
    union: PlanUnion,
}
impl CommittedPlanning {
    /// Persist capture work when recovery ends before the output handoff.
    /// Successful exports take the original Budget and account for it themselves.
    pub async fn account_capture(
        &self,
        budget: &mut crate::store::pinning::management::native::meter::Budget,
        limits: &RunLimits,
    ) -> Result<()> {
        let task = self
            .spools
            .first()
            .and_then(|spool| spool.task.as_ref())
            .ok_or(ManagementError::InvalidEvidence)?;
        task.account_capture(limits, budget).await
    }
    pub fn binding(&self) -> &crate::store::pinning::management::records::ConfirmedBinding {
        &self.binding
    }
    pub async fn task_work(&self) -> Result<TaskWork> {
        total_work(&self.spools, Some(&self.union)).await
    }
    /// A new recovery invocation supplies its fresh-capture Budget. The capture
    /// and complete output are charged once; either outcome returns that Budget.
    pub async fn export_file(
        &self,
        db: &DatabaseConnection,
        output: (
            File,
            crate::store::pinning::management::native::meter::Budget,
        ),
        projection: super::super::wire::v3::Projection,
        limits: &RunLimits,
    ) -> (
        Result<()>,
        crate::store::pinning::management::native::meter::Budget,
    ) {
        let Some(task) = self.spools.first().and_then(|spool| spool.task.as_deref()) else {
            return (Err(ManagementError::InvalidEvidence), output.1);
        };
        super::output::confirmed_file(
            Some(task),
            db,
            output,
            &self.binding,
            projection,
            true,
            limits,
        )
        .await
    }
    /// Release protected handles while preserving the original task and partials.
    pub async fn close(self) -> Result<PathBuf> {
        let path = self.workspace.path().to_owned();
        planning::close_resources(None, &self.spools, Some(self.union)).await?;
        drop(self.workspace);
        Ok(path)
    }
}

impl PlanningCheckpoint {
    /// Discover a committed original using protected local coordinates and DB
    /// authority. No planning cursor, confirmation time or ACK is renewed.
    pub async fn committed(
        path: &Path,
        db: &DatabaseConnection,
        auth: &crate::store::pinning::management::records::LocalAuthorization,
        request: &PlanRequest,
        local: &Snapshot,
        limits: &RunLimits,
    ) -> Result<Option<crate::store::pinning::management::records::ConfirmedBinding>> {
        let Some(task) = Self::committed_task(
            path,
            db,
            auth,
            request,
            local,
            limits,
            &mut native::meter::Budget::default(),
        )
        .await?
        else {
            return Ok(None);
        };
        let binding = task.binding.clone();
        task.close().await?;
        Ok(Some(binding))
    }
    /// Keep this handle through reexport so retries consume the original meter.
    /// Retained cursors remain untrusted and are not used to reconstruct authority.
    /// Once local ownership is bound, capture is charged before authority reads.
    /// Keep this same Budget on None for planning or on Some for recovery output.
    pub async fn committed_task(
        path: &Path,
        db: &DatabaseConnection,
        auth: &crate::store::pinning::management::records::LocalAuthorization,
        request: &PlanRequest,
        local: &Snapshot,
        limits: &RunLimits,
        capture: &mut native::meter::Budget,
    ) -> Result<Option<CommittedPlanning>> {
        super::super::validate::label(auth.principal()).map_err(planning::safe)?;
        super::super::validate_request(request).map_err(planning::safe)?;
        super::super::validate::validate_snapshot(local).map_err(planning::safe)?;
        if local.configuration.is_none() || provider_routes(request).next().is_none() {
            return Err(ManagementError::InvalidEvidence);
        }
        let (mut workspace, frozen, _) = open_metadata(path).await?;
        if frozen.principal != auth.principal() || frozen.request != *request {
            return Err(ManagementError::Confirmation);
        }
        if frozen.local.configuration != local.configuration
            || frozen.origin.database_scope != auth.database_scope()
            || frozen.origin.database_identity != auth.database_identity()
        {
            return Err(ManagementError::Changed);
        }
        let mut inventories = Vec::new();
        let mut spools = Vec::new();
        let mut union = None;
        let result = async {
            for (index, _) in provider_routes(request).enumerate() {
                drop(workspace.open_named(&source_name(index))?);
                let mut spool =
                    InventorySpool::resume(&workspace.named(&source_name(index))?).await?;
                if let Some(primary) = spools.first() {
                    let route = provider_routes(request)
                        .nth(index)
                        .ok_or(ManagementError::InvalidEvidence)?;
                    spool.share_original(primary, route.clone(), false).await?;
                }
                inventories.push(spool.reference().clone());
                spools.push(spool);
            }
            drop(workspace.open_named(UNION)?);
            union = Some(PlanUnion::resume_task(&workspace.named(UNION)?).await?);
            confirmation::reconcile_retained(&mut workspace, &spools).await?;
            let task = std::sync::Arc::new(super::task_budget::TaskBudget::new(
                &spools,
                union.as_ref().ok_or(ManagementError::InvalidEvidence)?,
                (frozen.rows_limit, frozen.bytes_limit),
            ));
            for spool in &mut spools {
                spool.task = Some(task.clone());
            }
            let configuration = local
                .configuration
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?;
            let captured = task.adopt_output(limits, capture, true).await?;
            captured.finish(capture.work).await?;
            let read_limits = task.reserve_capture(capture, limits)?;
            let (accounting, session) = task.begin_read(&read_limits).await?;
            let work = std::sync::Arc::new(std::sync::Mutex::new(session));
            let result = crate::store::pinning::management::records::load_checkpoint_confirmation(
                db,
                auth,
                request,
                crate::store::pinning::management::records::CheckpointEvidence {
                    origin: &frozen.origin,
                    inventories: &inventories,
                    configuration,
                },
                &read_limits,
                work.clone(),
            )
            .await;
            let used = work
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .task_usage()?;
            accounting.finish(used).await?;
            result
        }
        .await;
        let binding = match result {
            Ok(Some(binding)) => binding,
            result => {
                planning::close_resources(None, &spools, union).await?;
                return result.map(|_| None);
            }
        };
        let union = union.ok_or(ManagementError::InvalidEvidence)?;
        Ok(Some(CommittedPlanning {
            binding,
            workspace,
            spools,
            union,
        }))
    }
    /// Reserve one new protected directory under an existing parent. No source
    /// DB mutation, provider registration, probe, or confirmation is performed.
    pub async fn create(
        path: &Path,
        db: &DatabaseConnection,
        principal: &str,
        request: &PlanRequest,
        local: &Snapshot,
        limits: &RunLimits,
    ) -> Result<Self> {
        validate_input(principal, request, local)?;
        let route = provider_routes(request)
            .next()
            .ok_or(ManagementError::InvalidEvidence)?
            .clone();
        let origin = read_binding(db, local.clone(), Some(route), None, limits).await?;
        if local
            .database_now
            .is_none_or(|now| now > origin.observed_at)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        let mut workspace = Workspace::create_at(path)?;
        let mut spools = Vec::new();
        let mut union = None;
        let frozen = FrozenPlanning {
            version: 2,
            principal: principal.into(),
            request: request.clone(),
            local: local.clone(),
            origin,
            rows_limit: limits.total_rows,
            bytes_limit: limits.total_bytes,
        };
        let result = async {
            workspace.lock(true).await?;
            for (index, route) in provider_routes(request).enumerate() {
                let mut checkpoint = frozen.origin.clone();
                checkpoint.route = route.clone();
                let (path, file) = workspace.reserve_named(&source_name(index)).await?;
                let mut inventory = InventorySpool::create(&path, file, &checkpoint).await?;
                if let Some(primary) = spools.first() {
                    inventory
                        .share_original(primary, route.clone(), true)
                        .await?;
                }
                spool::task_run(&inventory.db, limits).await?;
                spools.push(inventory);
            }
            let (path, file) = workspace.reserve_named(UNION).await?;
            union = Some(PlanUnion::create(&path, file).await?);
            union
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?
                .start_task(limits)
                .await?;
            let (_, file) = workspace.reserve_named(METADATA).await?;
            let bytes = write_metadata(file, &frozen)?;
            let primary = spools.first().ok_or(ManagementError::InvalidEvidence)?;
            let run = spool::task_run(&primary.db, limits).await?;
            spool::budget::charge(&primary.db, &run, 0, bytes as u64).await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            planning::close_resources(Some(workspace), &spools, union).await?;
            return Err(error);
        }
        Ok(Self {
            workspace: Some(workspace),
            frozen,
            local: local.clone(),
            spools,
            union,
        })
    }

    /// Read only bounded metadata at fixed task-owned names. No path from the
    /// metadata is opened. This handle closes without removing the workspace.
    pub async fn input(path: &Path) -> Result<PlanningCheckpointInput> {
        let (workspace, frozen, _) = open_metadata(path).await?;
        let configuration = frozen
            .local
            .configuration
            .clone()
            .ok_or(ManagementError::InvalidEvidence)?;
        let input = PlanningCheckpointInput {
            principal: frozen.principal,
            request: frozen.request,
            configuration,
            database_scope: frozen.origin.database_scope,
            database_identity: frozen.origin.database_identity,
            observed_at: frozen.origin.observed_at,
        };
        drop(workspace);
        Ok(input)
    }

    /// `local` must be freshly captured from this DB with the caller's current
    /// configuration. Frozen clocks are retained, never advanced on resume.
    pub async fn resume(
        path: &Path,
        db: &DatabaseConnection,
        principal: &str,
        request: &PlanRequest,
        local: &Snapshot,
        limits: &RunLimits,
    ) -> Result<Self> {
        validate_input(principal, request, local)?;
        spool::Run::new(limits).check()?;
        let (mut workspace, frozen, metadata_bytes) = open_metadata(path).await?;
        if frozen.principal != principal || frozen.request != *request {
            return Err(ManagementError::Confirmation);
        }
        let mut local = local.clone();
        local.observed_at = frozen.local.observed_at;
        local.database_now = frozen.local.database_now;
        if encode(&local)? != encode(&frozen.local)? {
            return Err(ManagementError::Changed);
        }
        read_binding(db, local.clone(), None, Some(frozen.origin.clone()), limits).await?;
        let mut spools = Vec::new();
        let mut union = None;
        let result = async {
            for (index, route) in provider_routes(request).enumerate() {
                drop(workspace.open_named(&source_name(index))?);
                let path = workspace.named(&source_name(index))?;
                let mut inventory = InventorySpool::resume_task(&path).await?;
                if let Some(primary) = spools.first() {
                    inventory
                        .share_original(primary, route.clone(), false)
                        .await?;
                }
                let mut expected = frozen.origin.clone();
                expected.route = route.clone();
                if inventory.checkpoint().await?.rewind() != expected {
                    inventory.close().await?;
                    return Err(ManagementError::Changed);
                }
                spools.push(inventory);
            }
            drop(workspace.open_named(UNION)?);
            union = Some(PlanUnion::resume_task(&workspace.named(UNION)?).await?);
            let primary = spools.first().ok_or(ManagementError::InvalidEvidence)?;
            super::confirmation::reconcile_retained(&mut workspace, &spools).await?;
            let used = total_work(&spools, union.as_ref()).await?;
            let own = primary.task_work().await?;
            let caps = task_caps((frozen.rows_limit, frozen.bytes_limit), limits, used, own)?;
            let run = spool::task_run(&primary.db, &caps).await?;
            spool::budget::charge(&primary.db, &run, 0, metadata_bytes as u64).await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            planning::close_resources(None, &spools, union).await?;
            return Err(error);
        }
        Ok(Self {
            workspace: Some(workspace),
            frozen,
            local,
            spools,
            union,
        })
    }
    pub fn path(&self) -> Result<&Path> {
        self.workspace
            .as_ref()
            .map(Workspace::path)
            .ok_or(ManagementError::InvalidEvidence)
    }
    pub fn request(&self) -> &PlanRequest {
        &self.frozen.request
    }
    pub fn snapshot(&self) -> &Snapshot {
        &self.local
    }

    /// Supply a new RunLimits for every invocation. Incomplete is not a plan or
    /// confirmation; it retains all accepted pages and cumulative task meters.
    pub async fn advance(
        &mut self,
        db: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<PlanningProgress> {
        if self.workspace.is_none() {
            return Err(ManagementError::InvalidEvidence);
        }
        if limits.cancellation.is_cancelled() {
            return Err(ManagementError::Budget);
        }
        if spool::Run::new(limits).remaining().is_zero() {
            return Ok(PlanningProgress::Incomplete);
        }
        let production = limits.for_production();
        if !production.production_available() {
            return Ok(PlanningProgress::Incomplete);
        }
        let result = self.advance_inner(db, &production).await;
        match result {
            Err(ManagementError::Budget)
                if !limits.cancellation.is_cancelled()
                    && spool::Run::new(limits).remaining().is_zero() =>
            {
                Ok(PlanningProgress::Incomplete)
            }
            result => result,
        }
    }
    async fn advance_inner(
        &mut self,
        db: &DatabaseConnection,
        limits: &RunLimits,
    ) -> Result<PlanningProgress> {
        check_origin(db, self.frozen.origin.clone(), limits).await?;
        let configuration = self
            .local
            .configuration
            .as_ref()
            .ok_or(ManagementError::Confirmation)?;
        for spool in &self.spools {
            limits.check()?;
            if !limits.production_available() {
                return Ok(PlanningProgress::Incomplete);
            }
            let caps = task_caps(
                (self.frozen.rows_limit, self.frozen.bytes_limit),
                limits,
                total_work(&self.spools, self.union.as_ref()).await?,
                spool.task_work().await?,
            )?;
            if spool.scan(db, &caps).await? == Progress::Pending {
                return Ok(PlanningProgress::Incomplete);
            }
            limits.check()?;
            if !limits.production_available() {
                return Ok(PlanningProgress::Incomplete);
            }
            let caps = task_caps(
                (self.frozen.rows_limit, self.frozen.bytes_limit),
                limits,
                total_work(&self.spools, self.union.as_ref()).await?,
                spool.task_work().await?,
            )?;
            if spool.reduce(configuration, &caps).await? == Progress::Pending {
                return Ok(PlanningProgress::Incomplete);
            }
        }
        let mut shared_work = total_work(&self.spools, self.union.as_ref()).await?;
        if let Some((primary, others)) = self.spools.split_first_mut() {
            for spool in others {
                limits.check()?;
                if !limits.production_available() {
                    return Ok(PlanningProgress::Incomplete);
                }
                let before = spool.task_work().await?;
                let caps = task_caps(
                    (self.frozen.rows_limit, self.frozen.bytes_limit),
                    limits,
                    shared_work,
                    before,
                )?;
                if spool.verify_shared_source(primary, &caps).await? == Progress::Pending {
                    return Ok(PlanningProgress::Incomplete);
                }
                let after = spool.task_work().await?;
                shared_work.rows += after.rows - before.rows;
                shared_work.bytes += after.bytes - before.bytes;
            }
        }
        let task = std::sync::Arc::new(super::task_budget::TaskBudget::new(
            &self.spools,
            self.union
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?,
            (self.frozen.rows_limit, self.frozen.bytes_limit),
        ));
        for spool in &mut self.spools {
            spool.task = Some(task.clone());
        }
        let mut reviews = Vec::new();
        for spool in &self.spools {
            reviews.push(spool.frozen_review().await?);
        }
        let mutations = planning::mutation_scopes(&self.frozen.request, &self.local);
        limits.check()?;
        if !limits.production_available() {
            return Ok(PlanningProgress::Incomplete);
        }
        let union = self
            .union
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        let caps = task_caps(
            (self.frozen.rows_limit, self.frozen.bytes_limit),
            limits,
            total_work(&self.spools, Some(union)).await?,
            union.task_work().await?,
        )?;
        let plan = super::super::dry_run_retirement(
            &self.frozen.request,
            &self.local,
            &reviews,
            &mutations,
            union,
            &caps,
        )
        .await
        .map_err(planning::safe)?;
        let Some(plan) = plan else {
            return Ok(PlanningProgress::Incomplete);
        };
        check_origin(db, self.frozen.origin.clone(), limits).await?;
        Ok(PlanningProgress::Complete(Box::new(PreparedPlanning {
            plan,
            workspace: self.workspace.take(),
            union: self.union.take(),
            spools: std::mem::take(&mut self.spools),
            task_envelope: Some((self.frozen.rows_limit, self.frozen.bytes_limit)),
        })))
    }
    pub async fn task_work(&self) -> Result<Vec<TaskWork>> {
        let mut work = Vec::new();
        for spool in &self.spools {
            work.push(spool.task_work().await?);
        }
        Ok(work)
    }
    pub async fn total_work(&self) -> Result<TaskWork> {
        total_work(&self.spools, self.union.as_ref()).await
    }
    pub async fn account_capture(
        &self,
        budget: &mut crate::store::pinning::management::native::meter::Budget,
        limits: &RunLimits,
    ) -> Result<()> {
        let task = super::task_budget::TaskBudget::new(
            &self.spools,
            self.union
                .as_ref()
                .ok_or(ManagementError::InvalidEvidence)?,
            (self.frozen.rows_limit, self.frozen.bytes_limit),
        );
        task.account_capture(limits, budget).await
    }
    pub fn reserve_capture(
        &self,
        budget: &crate::store::pinning::management::native::meter::Budget,
        limits: &RunLimits,
    ) -> Result<RunLimits> {
        limits.reserve_capture((self.frozen.rows_limit, self.frozen.bytes_limit), budget)
    }
    /// Close pools/guards only. The same explicit path can be resumed later.
    pub async fn close(self) -> Result<PathBuf> {
        let path = self.path()?.to_owned();
        planning::close_resources(None, &self.spools, self.union).await?;
        drop(self.workspace);
        Ok(path)
    }
    pub async fn cleanup(self) -> Result<()> {
        planning::close_resources(self.workspace, &self.spools, self.union).await
    }
}

fn validate_input(principal: &str, request: &PlanRequest, local: &Snapshot) -> Result<()> {
    super::super::validate::label(principal).map_err(planning::safe)?;
    super::super::validate_request(request).map_err(planning::safe)?;
    super::super::validate::validate_snapshot(local).map_err(planning::safe)?;
    if local.native_frame.is_none()
        || local
            .database_now
            .is_none_or(|now| now.timestamp() != local.observed_at)
        || local.configuration.is_none()
        || provider_routes(request).next().is_none()
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
fn provider_routes(request: &PlanRequest) -> impl Iterator<Item = &RouteIdentity> {
    request.items.iter().filter_map(|item| {
        if let Subject::Provider { route } = &item.subject {
            Some(route)
        } else {
            None
        }
    })
}
fn source_name(index: usize) -> String {
    format!("source-{index:03}.sqlite")
}

async fn read_binding(
    db: &DatabaseConnection,
    local: Snapshot,
    route: Option<RouteIdentity>,
    expected: Option<SourceCheckpoint>,
    limits: &RunLimits,
) -> Result<SourceCheckpoint> {
    let mut tx = ManagementTransaction::begin(
        db,
        spool::Run::new(limits).remaining(),
        limits.cancellation.clone(),
    )
    .await
    .map_err(TransactionFailure::management_error)?;
    let origin = tx
        .run(move |tx, _| {
            Box::pin(async move {
                let frame = local
                    .native_frame
                    .as_ref()
                    .ok_or(ManagementError::InvalidEvidence)?;
                native::scope::check_frame_scope(tx, frame, &mut native::meter::Budget::default())
                    .await?;
                if let Some(expected) = expected {
                    let mut fresh = SourceCheckpoint::start(tx, expected.route.clone()).await?;
                    if expected.observed_at > fresh.observed_at {
                        return Err(ManagementError::InvalidEvidence);
                    }
                    fresh.observed_at = expected.observed_at;
                    if fresh != expected {
                        return Err(ManagementError::Changed);
                    }
                    Ok(expected)
                } else {
                    SourceCheckpoint::start(tx, route.ok_or(ManagementError::InvalidEvidence)?)
                        .await
                }
            })
        })
        .await
        .map_err(TransactionFailure::management_error)?;
    tx.rollback()
        .await
        .map_err(TransactionFailure::management_error)?;
    Ok(origin)
}
async fn check_origin(
    db: &DatabaseConnection,
    origin: SourceCheckpoint,
    limits: &RunLimits,
) -> Result<()> {
    let mut tx = ManagementTransaction::begin(
        db,
        spool::Run::new(limits).remaining(),
        limits.cancellation.clone(),
    )
    .await
    .map_err(TransactionFailure::management_error)?;
    tx.run(move |tx, _| Box::pin(async move { origin.check(tx).await }))
        .await
        .map_err(TransactionFailure::management_error)?;
    tx.rollback()
        .await
        .map_err(TransactionFailure::management_error)
}
async fn open_metadata(path: &Path) -> Result<(Workspace, FrozenPlanning, usize)> {
    let mut workspace = Workspace::open(path)?;
    workspace.lock(false).await?;
    let mut file = workspace.open_named(METADATA)?;
    if file
        .metadata()
        .map_err(|_| ManagementError::Database)?
        .len()
        > MAX_METADATA as u64
    {
        return Err(ManagementError::Budget);
    }
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut chunk)
            .map_err(|_| ManagementError::Database)?;
        if n == 0 {
            break;
        }
        if n > MAX_METADATA - bytes.len() {
            return Err(ManagementError::Budget);
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    let frozen: FrozenPlanning =
        serde_json::from_slice(&bytes).map_err(|_| ManagementError::InvalidEvidence)?;
    if frozen.version != 2 || !frozen.origin.is_start() {
        return Err(ManagementError::InvalidEvidence);
    }
    if frozen
        .local
        .database_now
        .is_none_or(|now| now > frozen.origin.observed_at)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    validate_input(&frozen.principal, &frozen.request, &frozen.local)?;
    Ok((workspace, frozen, bytes.len()))
}
fn write_metadata(file: File, frozen: &FrozenPlanning) -> Result<usize> {
    struct Bounded {
        file: File,
        written: usize,
    }
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_METADATA - self.written {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            let n = self.file.write(bytes)?;
            self.written += n;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    let mut output = Bounded { file, written: 0 };
    serde_json::to_writer(&mut output, frozen).map_err(|_| ManagementError::Budget)?;
    output
        .flush()
        .and_then(|_| output.file.sync_all())
        .map_err(|_| ManagementError::Database)?;
    Ok(output.written)
}

pub(super) async fn total_work(
    spools: &[InventorySpool],
    union: Option<&PlanUnion>,
) -> Result<TaskWork> {
    let mut total = TaskWork {
        rows: 0,
        bytes: 0,
        pages: 0,
    };
    for spool in spools {
        let work = spool.task_work().await?;
        total.rows = total
            .rows
            .checked_add(work.rows)
            .ok_or(ManagementError::Budget)?;
        total.bytes = total
            .bytes
            .checked_add(work.bytes)
            .ok_or(ManagementError::Budget)?;
        total.pages = total
            .pages
            .checked_add(work.pages)
            .ok_or(ManagementError::Budget)?;
    }
    if let Some(union) = union {
        let work = union.task_work().await?;
        total.rows = total
            .rows
            .checked_add(work.rows)
            .ok_or(ManagementError::Budget)?;
        total.bytes = total
            .bytes
            .checked_add(work.bytes)
            .ok_or(ManagementError::Budget)?;
        total.pages = total
            .pages
            .checked_add(work.pages)
            .ok_or(ManagementError::Budget)?;
    }
    Ok(total)
}
pub(super) fn task_caps(
    envelope: (u64, u64),
    limits: &RunLimits,
    total: TaskWork,
    own: TaskWork,
) -> Result<RunLimits> {
    let rows = envelope
        .0
        .min(limits.total_rows)
        .checked_sub(total.rows)
        .ok_or(ManagementError::Budget)?;
    let bytes = envelope
        .1
        .min(limits.total_bytes)
        .checked_sub(total.bytes)
        .ok_or(ManagementError::Budget)?;
    Ok(limits.with_task_caps(
        own.rows.checked_add(rows).ok_or(ManagementError::Budget)?,
        own.bytes
            .checked_add(bytes)
            .ok_or(ManagementError::Budget)?,
    ))
}
