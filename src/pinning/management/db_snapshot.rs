//! Read-only request-scoped native evidence and its semantic projection.
//! Readability comes only from explicit trusted preparation. Collection performs
//! no provider calls, migrations, business writes or full-registry capture.

use crate::store::pinning::management::records::{DecodeWork, SharedDecodeBudget};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel,
    TransactionTrait,
};

use super::{
    Action, AppliedItem, DecisionState, ExactObject, MAX_TEXT_BYTES, PlanRequest, PublicVersionId,
    ResourceType, RouteIdentity, SafeError, SelectionEvidence, Snapshot, validate_request,
};
use crate::pinning::{
    config::ValidatedPinningConfig,
    identity::{ProviderRouteSnapshot, RemoteResourceType},
    provider::canonical_resource_cid,
};

#[path = "db_snapshot/evidence.rs"]
mod evidence;
#[path = "db_snapshot/legacy.rs"]
mod legacy;
#[path = "db_snapshot/native_plan.rs"]
mod native_plan;
#[path = "db_snapshot/park.rs"]
mod park;
#[path = "db_snapshot/project.rs"]
mod project;
#[path = "db_snapshot/read.rs"]
mod read;
#[path = "db_snapshot/scoped.rs"]
mod scoped;
pub use evidence::*;
pub use scoped::PlanningScope;

/// Static errors only. DB/provider/serde text and input are never returned/logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot request validation failed: {0}")]
    Validation(SafeError),
    #[error("snapshot database unavailable (details withheld)")]
    DatabaseUnavailable,
    #[error("snapshot database backend unsupported")]
    UnsupportedBackend,
    #[error("snapshot exact object unavailable")]
    MissingExactObject,
    #[error("snapshot target unavailable")]
    MissingTarget,
    #[error("snapshot database evidence invalid (details withheld)")]
    InvalidEvidence,
    #[error("snapshot CID invalid (details withheld)")]
    InvalidCid,
    #[error("snapshot inventory budget exceeded")]
    LimitExceeded,
    #[error("snapshot lacks authoritative management evidence; needs attention")]
    UnsupportedEvidence,
    #[error("snapshot scoped evidence changed")]
    Changed,
    #[error("snapshot requires a sealed global inventory")]
    GlobalInventoryRequired,
}

type Result<T> = std::result::Result<T, SnapshotError>;

pub async fn collect(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    request: &PlanRequest,
) -> Result<DatabaseSnapshot> {
    collect_inner(db, config, None, request, Default::default()).await
}

/// Production capture with full pure validated configuration; no RPC fallback.
pub async fn collect_with_config(
    db: &DatabaseConnection,
    config: &super::local_config::ManagementConfig,
    request: &PlanRequest,
) -> Result<DatabaseSnapshot> {
    collect_with_session(db, config, request, Default::default()).await
}
pub async fn collect_with_session(
    db: &DatabaseConnection,
    config: &super::local_config::ManagementConfig,
    request: &PlanRequest,
    work: SharedDecodeBudget,
) -> Result<DatabaseSnapshot> {
    collect_inner(db, &config.pinning, Some(config), request, work).await
}

#[derive(Debug, thiserror::Error)]
pub enum ControlledCaptureError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("snapshot transaction stopped (details withheld)")]
    Stopped(crate::store::pinning::management::transaction::TransactionFailure),
}

/// Read-only capture under the invocation's absolute hard deadline. Engine
/// interruption is drained and rollback acknowledged before returning a stop.
pub async fn collect_with_control(
    db: &DatabaseConnection,
    config: &super::local_config::ManagementConfig,
    request: &PlanRequest,
    deadline: std::time::Instant,
    cancellation: crate::store::pinning::management::transaction::CancellationToken,
    work: SharedDecodeBudget,
) -> std::result::Result<DatabaseSnapshot, ControlledCaptureError> {
    use crate::store::pinning::management::transaction::ManagementTransaction;
    validate_request(request).map_err(SnapshotError::Validation)?;
    let mut tx = ManagementTransaction::begin_read_only(
        db,
        deadline.saturating_duration_since(std::time::Instant::now()),
        cancellation.clone(),
    )
    .await
    .map_err(ControlledCaptureError::Stopped)?;
    let result = tx
        .run_with((config, request, work), |tx, control, context| {
            Box::pin(async move {
                control.shorten(deadline.saturating_duration_since(std::time::Instant::now()));
                Ok(
                    collect_in_transaction_with_session(
                        tx,
                        context.0,
                        context.1,
                        context.2.clone(),
                    )
                    .await,
                )
            })
        })
        .await
        .map_err(ControlledCaptureError::Stopped)?;
    tx.rollback()
        .await
        .map_err(ControlledCaptureError::Stopped)?;
    use crate::store::pinning::management::transaction::{StopCause, TransactionFailure};
    let stopped = if cancellation.is_cancelled() {
        Some(StopCause::Cancelled)
    } else if std::time::Instant::now() >= deadline {
        Some(StopCause::Deadline)
    } else {
        None
    };
    if let Some(cause) = stopped {
        return Err(ControlledCaptureError::Stopped(TransactionFailure {
            cause,
            rollback_confirmed: true,
        }));
    }
    result.map_err(ControlledCaptureError::Snapshot)
}

async fn collect_inner(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    management_config: Option<&super::local_config::ManagementConfig>,
    request: &PlanRequest,
    work: SharedDecodeBudget,
) -> Result<DatabaseSnapshot> {
    validate_request(request).map_err(SnapshotError::Validation)?;
    let backend = db.get_database_backend();
    let tx = match backend {
        DatabaseBackend::Sqlite => db.begin().await,
        DatabaseBackend::Postgres => {
            db.begin_with_config(
                Some(IsolationLevel::RepeatableRead),
                Some(AccessMode::ReadOnly),
            )
            .await
        }
        _ => return Err(SnapshotError::UnsupportedBackend),
    }
    .map_err(|_| SnapshotError::DatabaseUnavailable)?;
    let result = collect_transaction(&tx, config, management_config, request, work).await;
    tx.rollback()
        .await
        .map_err(|_| SnapshotError::DatabaseUnavailable)?;
    result
}

pub(crate) async fn collect_in_transaction_with_session(
    tx: &sea_orm::DatabaseTransaction,
    config: &super::local_config::ManagementConfig,
    request: &PlanRequest,
    work: SharedDecodeBudget,
) -> Result<DatabaseSnapshot> {
    validate_request(request).map_err(SnapshotError::Validation)?;
    collect_transaction(tx, &config.pinning, Some(config), request, work).await
}

async fn collect_transaction(
    tx: &sea_orm::DatabaseTransaction,
    config: &ValidatedPinningConfig,
    management_config: Option<&super::local_config::ManagementConfig>,
    request: &PlanRequest,
    work: SharedDecodeBudget,
) -> Result<DatabaseSnapshot> {
    let mut budget = crate::store::pinning::management::native::meter::Budget::default();
    let (native_frame, mut scope) =
        scoped::collect(tx, config, request, &work, &mut budget).await?;
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| SnapshotError::DatabaseUnavailable)?;
    let objects = read::objects(&native_frame, request, &work)?;
    let rows = read::Rows::project(&native_frame, now, &work)?;
    let mut result = project::assemble(now.timestamp(), objects, rows, config, request, &work)?;
    result.planning = Some(native_plan::project(
        now,
        &result,
        native_frame,
        config,
        management_config,
        request,
        &work,
    )?);
    if let Some(planning) = &result.planning {
        if planning
            .objects
            .iter()
            .all(|object| object.readable == Some(true))
        {
            result
                .needs_attention
                .retain(|gap| *gap != EvidenceGap::ReadabilityNotProbed);
        }
        if planning
            .objects
            .iter()
            .all(|object| object.permitted.is_some())
        {
            result
                .needs_attention
                .retain(|gap| *gap != EvidenceGap::StoredByteSafetyUnavailable);
        }
        if planning
            .targets
            .iter()
            .any(|target| target.requirements.is_none())
        {
            result
                .needs_attention
                .push(EvidenceGap::TargetRequirementsUnavailable);
        }
        if planning
            .inventories
            .iter()
            .any(|inventory| !inventory.complete)
        {
            result
                .needs_attention
                .push(EvidenceGap::ProviderRootAttributionUnavailable);
        }
    }
    budget.check().map_err(scoped::map_error)?;
    scope.record_measurements(&budget);
    result.scope = Some(std::sync::Arc::new(scope));
    Ok(result)
}

fn canonical(cid: &str) -> Result<String> {
    if cid.len() > 512 {
        return Err(SnapshotError::InvalidCid);
    }
    canonical_resource_cid(cid).map_err(|_| SnapshotError::InvalidCid)
}

fn route(raw: &ProviderRouteSnapshot) -> RouteIdentity {
    RouteIdentity {
        provider_id: raw.provider_id.clone(),
        backend: raw.backend.clone(),
        scope: raw.scope.clone(),
        storage_domain: raw.storage_domain.clone(),
        api_profile: raw.api_profile.clone(),
        strategy: raw.strategy.clone(),
        resource_type: resource_type(raw.resource_type()),
        credential_revision: raw.credential_revision,
        endpoint_revision: raw.endpoint_revision,
        original: None,
    }
}

fn resource_type(raw: RemoteResourceType) -> ResourceType {
    match raw {
        RemoteResourceType::PsaRequest => ResourceType::PsaRequest,
        RemoteResourceType::HostedFile => ResourceType::HostedFile,
        RemoteResourceType::RpcPin => ResourceType::RpcPin,
        RemoteResourceType::ClusterPin => ResourceType::ClusterPin,
    }
}

fn number(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| SnapshotError::InvalidEvidence)
}

fn decode<T: serde::de::DeserializeOwned + Send + Sync + 'static>(
    value: &str,
    work: &SharedDecodeBudget,
) -> Result<std::sync::Arc<T>> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(SnapshotError::LimitExceeded);
    }
    work.lock()
        .map_err(|_| SnapshotError::InvalidEvidence)?
        .typed_shared(value)
        .map_err(|error| {
            if error == crate::store::pinning::management::ManagementError::Budget {
                SnapshotError::LimitExceeded
            } else {
                SnapshotError::InvalidEvidence
            }
        })
}
