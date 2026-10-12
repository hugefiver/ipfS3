//! Old internal DTO adapter. All ZIP transaction work lives in zip.rs.
use super::{
    PublicationObject,
    zip::{self, CapturedZipPublication, ZipExecutionSettlement, ZipPublicationAuthority},
};
use crate::{
    error::AppResult,
    pinning::{
        config::{ProviderLimitMap, ValidatedPinningConfig},
        policy::PublicationPolicy,
        zip_policy::ValidatedZipOutputRules,
    },
    store::{
        entities::zip_batch,
        import::ownership::StandardMutationGuard,
        object_version::PublicationResult,
        zip::{self as storage, execution},
    },
    zip::{
        batch::{FinalZipManifest, ManifestFile},
        options::ZipTargets,
    },
};
use sea_orm::{DatabaseConnection, EntityTrait};

#[derive(Debug, Clone)]
pub struct ZipV2Success {
    pub object: PublicationObject,
    pub path: String,
    pub object_key: String,
    pub cid: String,
    pub size: i64,
    /// Retained for source compatibility, not executable authorization.
    pub policy: PublicationPolicy,
}

#[derive(Debug, Clone)]
pub struct ZipV2Publication {
    pub claim: execution::Claim,
    pub source: Option<PublicationObject>,
    pub source_policy: Option<PublicationPolicy>,
    pub source_guard: Option<StandardMutationGuard>,
    pub successes: Vec<ZipV2Success>,
    pub targets: ZipTargets,
    pub captured_rule_revision: Option<String>,
    pub root_outcome: storage::RootOutcome,
    pub terminal_result: String,
}

#[derive(Debug, Clone)]
pub enum ZipV2PublicationResult {
    Published(Vec<(String, PublicationResult)>),
    Fenced,
}

pub async fn publish_zip_v2(
    db: &DatabaseConnection,
    request: ZipV2Publication,
    _rules: &ValidatedZipOutputRules,
    _config: &ValidatedPinningConfig,
    _limits: &ProviderLimitMap,
) -> AppResult<ZipV2PublicationResult> {
    publish(db, request, ZipExecutionSettlement::Direct).await
}

impl ZipV2Publication {
    pub async fn publish_mpu(
        self,
        db: &DatabaseConnection,
        contract: String,
        _rules: &ValidatedZipOutputRules,
        _config: &ValidatedPinningConfig,
        _limits: &ProviderLimitMap,
    ) -> AppResult<ZipV2PublicationResult> {
        publish(db, self, ZipExecutionSettlement::Mpu { contract }).await
    }

    pub async fn publish_import(
        self,
        db: &DatabaseConnection,
        _rules: &ValidatedZipOutputRules,
        _config: &ValidatedPinningConfig,
        _limits: &ProviderLimitMap,
    ) -> AppResult<ZipV2PublicationResult> {
        publish(db, self, ZipExecutionSettlement::Import).await
    }
}

async fn publish(
    db: &DatabaseConnection,
    request: ZipV2Publication,
    settlement: ZipExecutionSettlement,
) -> AppResult<ZipV2PublicationResult> {
    let row = zip_batch::Entity::find_by_id(&request.claim.batch_id)
        .one(db)
        .await?
        .ok_or_else(|| super::invalid_publication("missing ZIP capture; needs_attention"))?;
    let capture = zip::read_capture(&row.captured_options)?;
    if capture
        .v2_options()
        .is_none_or(|o| o.targets != request.targets)
        || request.successes.iter().any(|s| {
            s.object.key != s.object_key || s.object.cid != s.cid || s.object.logical_size != s.size
        })
    {
        return Err(super::invalid_publication(
            "ZIP compatibility DTO contradicts capture or manifest",
        ));
    }
    let claim = request.claim.clone();
    let source_exists = request.source.is_some();
    let manifest = FinalZipManifest {
        successful: request
            .successes
            .iter()
            .map(|s| ManifestFile {
                relative_path: s.path.clone(),
                object_key: s.object_key.clone(),
                cid: s.cid.clone(),
                size: s.size,
            })
            .collect(),
        ..Default::default()
    };
    let result = zip::publish_captured_zip(
        db,
        CapturedZipPublication {
            batch_id: claim.batch_id.clone(),
            source: request.source,
            source_tags: request.source_policy.map(|p| p.tags).unwrap_or_default(),
            successes: request.successes.into_iter().map(|s| s.object).collect(),
            manifest,
            capture,
            authority: ZipPublicationAuthority::Execution {
                claim: claim.clone(),
                source_guard: request.source_guard,
                settlement,
            },
            root_outcome: request.root_outcome,
            terminal_result: request.terminal_result,
        },
    )
    .await;
    if matches!(result, Err(crate::error::AppError::StaleContentMutation)) {
        let fenced = if source_exists {
            execution::fence(db, &claim).await
        } else {
            execution::fence_if_lost(db, &claim).await
        };
        if let Err(error) = fenced {
            tracing::warn!(%error, "ZIP guard reconciliation deferred");
        }
    }
    result.map(ZipV2PublicationResult::Published)
}
