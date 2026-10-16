//! Stage 7 read-only native database reader, NOT the management executor.
//!
//! The caller supplies an existing connection, effective validated configuration,
//! and an explicit, authorized manifest. This leaf opens no connections, performs
//! no migrations, changes no pool/session settings and issues no business writes.
//! SQLite uses BEGIN + SELECT; PostgreSQL uses REPEATABLE READ READ ONLY. Owned
//! evidence leaves the transaction before any future planning or network work.
//!
//! Original target tuples and global PK frontiers fence planning. Readability
//! comes only from explicit trusted local preparation, never a dry-run probe,
//! metadata-size substitution, imported bool or aggregate revision counter.
//!
//! Internal pin inventories are bounded globally (4096 rows / 4 MiB text across
//! tables), then filtered to the explicit objects and requested provider domains.
//! This deliberately fails closed on oversized inventories; it does not silently
//! page/truncate and claim completeness. No prefix/object discovery is performed.

use crate::store::pinning::management::records::{DecodeWork, SharedDecodeBudget};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, IsolationLevel, Statement,
    TransactionTrait,
};

use super::{
    Action, AppliedItem, DecisionState, ExactObject, MAX_RECORDS, MAX_TEXT_BYTES, PlanRequest,
    PublicVersionId, ResourceType, RouteIdentity, SafeError, SelectionEvidence, Snapshot,
    validate_request,
};
use crate::pinning::{
    config::ValidatedPinningConfig,
    identity::{ProviderRouteSnapshot, RemoteResourceType},
    provider::canonical_resource_cid,
};

#[path = "db_snapshot/evidence.rs"]
mod evidence;
#[path = "db_snapshot/native_plan.rs"]
mod native_plan;
#[path = "db_snapshot/project.rs"]
mod project;
#[path = "db_snapshot/read.rs"]
mod read;
pub use evidence::*;

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
    let native_frame =
        crate::store::pinning::management::native::capture_with_work(tx, DecodeWork::shared(&work))
            .await
            .map_err(|error| match error {
                crate::store::pinning::management::ManagementError::Budget => {
                    SnapshotError::LimitExceeded
                }
                crate::store::pinning::management::ManagementError::Schema => {
                    SnapshotError::UnsupportedEvidence
                }
                _ => SnapshotError::InvalidEvidence,
            })?;
    let observed_at = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| SnapshotError::DatabaseUnavailable)?
        .timestamp();
    let mut budget = read::Budget::new(work.clone());
    let objects = read::objects(tx, request, &mut budget).await?;
    let rows = read::Rows::load(tx, &mut budget).await?;
    let mut result = project::assemble(observed_at, objects, rows, config, request, &work)?;
    result.planning = Some(
        native_plan::project(
            tx,
            &result,
            native_frame,
            config,
            management_config,
            request,
            &work,
        )
        .await?,
    );
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
