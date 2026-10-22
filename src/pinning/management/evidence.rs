//! Explicit bounded offline proof path; never called by collect/dry_run.
use super::{ExactObject, local_config::ManagementConfig};
use crate::{
    kubo::{
        DirectoryProofBudgets, KuboClient, LocalDirectoryRootKind,
        LocalDirectoryVerificationReceipt, cat,
    },
    store::pinning::management::{
        ManagementError, Result,
        native::{
            self, NativeRow,
            meter::{Budget, Limits},
        },
        records::{self, LocalAuthorization, RecordKind, RecordState, RecordWrite},
    },
};
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, TransactionTrait};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

mod scope;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReadBinding {
    pub node_identity: String,
    pub local_reads_only: bool,
    /// Actual read configuration is held privately, not exposed as a URL/token.
    #[serde(skip)]
    endpoint: String,
    pub idle_timeout_millis: u64,
}
impl SourceReadBinding {
    pub fn endpoint_matches(&self, client: &KuboClient) -> bool {
        self.endpoint == client.base_url()
    }
    pub(crate) fn original_endpoint_matches(&self, endpoint: &str) -> bool {
        self.endpoint == endpoint
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadAttestation {
    pub identity: ExactObject,
    pub object_tuple: NativeRow,
    pub version_tuple: NativeRow,
    pub source: SourceReadBinding,
    pub stored_bytes: Option<u64>,
    pub proof: ReadProof,
    /// Private trusted-record observations; not quota bytes or an upload grant.
    /// Absent in both historical file and size-less directory records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directory_receipt: Option<LocalDirectoryVerificationReceipt>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadProof {
    FileCleanEof,
    /// Historical residency-only proof. It never authorizes a stored byte count.
    CompleteLocalDirectory,
    /// Complete offline typed directory/HAMT root closure. stored_bytes is the
    /// sum of unique reachable blocks' raw serialized payloads, NOT CAR framing,
    /// UnixFS logical file size, third-party billing, or global block dedup.
    CompleteLocalDirectoryDagBlocksV1,
}
impl ReadProof {
    pub fn is_directory(self) -> bool {
        matches!(
            self,
            Self::CompleteLocalDirectory | Self::CompleteLocalDirectoryDagBlocksV1
        )
    }
    pub fn has_stored_byte_count(self) -> bool {
        matches!(
            self,
            Self::FileCleanEof | Self::CompleteLocalDirectoryDagBlocksV1
        )
    }
}
impl ReadAttestation {
    pub fn directory_root_kind(&self) -> Option<LocalDirectoryRootKind> {
        self.directory_receipt
            .as_ref()
            .map(LocalDirectoryVerificationReceipt::root_kind)
    }
    fn validate_shape(&self) -> Result<()> {
        if !self.source.local_reads_only
            || self.source.idle_timeout_millis == 0
            || self.expires_at <= self.observed_at
            || self.expires_at - self.observed_at > chrono::Duration::hours(1)
            || self.proof.has_stored_byte_count() != self.stored_bytes.is_some()
        {
            return Err(ManagementError::InvalidEvidence);
        }
        match (self.proof, &self.directory_receipt) {
            (ReadProof::FileCleanEof | ReadProof::CompleteLocalDirectory, None) => Ok(()),
            (ReadProof::CompleteLocalDirectoryDagBlocksV1, Some(receipt))
                if receipt.valid_shape()
                    && receipt.cid() == self.identity.cid
                    && receipt.node_identity() == self.source.node_identity
                    && Some(receipt.stored_block_bytes()) == self.stored_bytes =>
            {
                Ok(())
            }
            _ => Err(ManagementError::InvalidEvidence),
        }
    }
}

pub struct PrepareRequest {
    pub record_id: String,
    pub identity: ExactObject,
    pub expected_node: String,
    pub max_stored_bytes: u64,
    pub ttl: chrono::Duration,
    pub directory: bool,
}

fn safe_object(
    frame: &native::NativeFrame,
    identity: &ExactObject,
) -> Result<(NativeRow, NativeRow)> {
    let object = frame
        .row("objects", &[&identity.object_id])
        .ok_or(ManagementError::Changed)?;
    let version = frame
        .row("object_versions", &[&identity.private_version_id])
        .ok_or(ManagementError::Changed)?;
    let public = match &identity.public_version {
        super::PublicVersionId::Null => None,
        super::PublicVersionId::Version(v) => Some(v.as_str()),
    };
    if native::value(object, "bucket") != Some(&identity.bucket)
        || native::value(object, "key") != Some(&identity.key)
        || native::value(version, "bucket") != Some(&identity.bucket)
        || native::value(version, "key") != Some(&identity.key)
        || native::value(version, "object_id") != Some(&identity.object_id)
        || native::value(version, "version_id") != public
        || native::value(version, "kind") != Some("object")
        || native::value(object, "cid")
            .and_then(|c| crate::pinning::provider::canonical_resource_cid(c).ok())
            .as_deref()
            != Some(&identity.cid)
    {
        return Err(ManagementError::Changed);
    }
    if frame
        .table("object_versions")
        .ok_or(ManagementError::InvalidEvidence)?
        .rows
        .iter()
        .filter(|row| {
            native::value(row, "bucket") == Some(&identity.bucket)
                && native::value(row, "key") == Some(&identity.key)
                && native::value(row, "version_id") == public
        })
        .count()
        != 1
    {
        return Err(ManagementError::Changed);
    }
    Ok((object.clone(), version.clone()))
}

/// Same RID is immutable. It cannot be recaptured as today's replacement body.
pub async fn prepare(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    authorization: &LocalAuthorization,
    client: &KuboClient,
    request: &PrepareRequest,
    cancel: CancellationToken,
) -> Result<ReadAttestation> {
    if request.record_id.is_empty()
        || request.record_id.len() > 64
        || request.ttl <= chrono::Duration::zero()
        || request.ttl > chrono::Duration::hours(1)
        || config.source_node() != Some(request.expected_node.as_str())
        || request.max_stored_bytes == 0
        || client.base_url() != config.source_endpoint
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let idle_timeout = client.stream_idle_timeout();
    let idle_timeout_millis =
        u64::try_from(idle_timeout.as_millis()).map_err(|_| ManagementError::InvalidEvidence)?;
    if idle_timeout_millis == 0 {
        return Err(ManagementError::InvalidEvidence);
    }
    let ttl_nanos = request
        .ttl
        .num_nanoseconds()
        .ok_or(ManagementError::InvalidEvidence)?;
    let legacy_binding = serde_json::to_string(&(
        &request.identity,
        &request.expected_node,
        request.max_stored_bytes,
        request.directory,
        ttl_nanos,
    ))
    .map_err(|_| ManagementError::InvalidEvidence)?;
    // Only new directory records acquire this profile. A historical size-less
    // RID must replay its original binding even if it predates these budgets.
    let budgets = if request.directory {
        DirectoryProofBudgets::for_payload(request.max_stored_bytes).ok()
    } else {
        None
    };
    let binding = if let Some(budgets) = budgets {
        Some(
            serde_json::to_string(&(
                &request.identity,
                &request.expected_node,
                request.max_stored_bytes,
                request.directory,
                ttl_nanos,
                ReadProof::CompleteLocalDirectoryDagBlocksV1,
                budgets,
                &config.source_endpoint,
                (idle_timeout.as_secs(), idle_timeout.subsec_nanos()),
            ))
            .map_err(|_| ManagementError::InvalidEvidence)?,
        )
    } else if request.directory {
        None
    } else {
        Some(legacy_binding.clone())
    };
    let content_scope = scope::content(&request.identity)?;
    let mut budget = Budget::new(Limits {
        elapsed: request
            .ttl
            .to_std()
            .map_err(|_| ManagementError::InvalidEvidence)?,
        ..Limits::default()
    });
    authorization.check(db).await?;
    let tx = scope::read_transaction(db).await?;
    let frame = scope::capture(&tx, &content_scope, &mut budget).await?;
    let (object_tuple, version_tuple) = safe_object(&frame, &request.identity)?;
    let full_before = (
        frame
            .original_row("objects", &[&request.identity.object_id])
            .ok_or(ManagementError::Changed)?,
        frame
            .original_row("object_versions", &[&request.identity.private_version_id])
            .ok_or(ManagementError::Changed)?,
    );
    if let Some(old) = load(
        &tx,
        authorization,
        &request.record_id,
        binding.as_deref(),
        &legacy_binding,
        &full_before,
        request.directory,
    )
    .await?
    {
        tx.rollback().await.map_err(|_| ManagementError::Database)?;
        let now = crate::store::database_clock::database_now(db)
            .await
            .map_err(|_| ManagementError::Database)?;
        if old.identity != request.identity
            || old.object_tuple != object_tuple
            || old.version_tuple != version_tuple
            || old.source.node_identity != request.expected_node
            || old.source.endpoint != config.source_endpoint
            || old.source.idle_timeout_millis != idle_timeout_millis
            || old
                .stored_bytes
                .is_some_and(|size| size > request.max_stored_bytes)
            || old.proof.is_directory() != request.directory
            || old
                .directory_receipt
                .as_ref()
                .is_some_and(|receipt| Some(receipt.budgets()) != budgets)
            || cancel.is_cancelled()
        {
            return Err(ManagementError::Changed);
        }
        if now < old.observed_at || now >= old.expires_at {
            return Err(ManagementError::Expired);
        }
        let offline = client.clone().with_local_reads_only();
        let node = if old.proof == ReadProof::CompleteLocalDirectoryDagBlocksV1 {
            offline
                .directory_proof_node_identity(&cancel)
                .await
                .map_err(|_| ManagementError::InvalidEvidence)?
        } else {
            tokio::select! {_=cancel.cancelled()=>return Err(ManagementError::InvalidEvidence),result=offline.local_node_identity()=>result.map_err(|_|ManagementError::InvalidEvidence)?}
        };
        if node != old.source.node_identity {
            return Err(ManagementError::Changed);
        }
        let tx = scope::read_transaction(db).await?;
        authorization.check(&tx).await?;
        let fresh = scope::capture(&tx, &content_scope, &mut budget).await?;
        if fresh != frame
            || safe_object(&fresh, &request.identity)?
                != (old.object_tuple.clone(), old.version_tuple.clone())
        {
            return Err(ManagementError::Changed);
        }
        let now = crate::store::database_clock::database_now(&tx)
            .await
            .map_err(|_| ManagementError::Database)?;
        if now < old.observed_at || now >= old.expires_at || cancel.is_cancelled() {
            return Err(ManagementError::Expired);
        }
        tx.rollback().await.map_err(|_| ManagementError::Database)?;
        return Ok(old);
    }
    let binding = binding.ok_or(ManagementError::Budget)?;
    let started = crate::store::database_clock::database_now(&tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    tx.rollback().await.map_err(|_| ManagementError::Database)?;
    let offline = client.clone().with_local_reads_only();
    let node = if request.directory {
        request.expected_node.clone()
    } else {
        tokio::select! { _=cancel.cancelled()=>return Err(ManagementError::InvalidEvidence), result=offline.local_node_identity()=>result.map_err(|_| ManagementError::InvalidEvidence)? }
    };
    if node != request.expected_node {
        return Err(ManagementError::Changed);
    }
    let (stored_bytes, proof, directory_receipt) = if let Some(budgets) = budgets {
        let receipt = offline
            .verify_local_directory(
                &request.identity.cid,
                &request.expected_node,
                budgets,
                cancel.clone(),
            )
            .await
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if receipt.node_identity() != node || receipt.cid() != request.identity.cid {
            return Err(ManagementError::Changed);
        }
        (
            Some(receipt.stored_block_bytes()),
            ReadProof::CompleteLocalDirectoryDagBlocksV1,
            Some(receipt),
        )
    } else {
        let stream = tokio::select! { _=cancel.cancelled()=>return Err(ManagementError::InvalidEvidence), result=cat::stream_cat(&offline,&request.identity.cid,None)=>result.map_err(|_| ManagementError::InvalidEvidence)? };
        tokio::pin!(stream);
        let mut size = 0_u64;
        loop {
            let next = tokio::select! { _=cancel.cancelled()=>return Err(ManagementError::InvalidEvidence), next=stream.next()=>next };
            match next {
                Some(Ok(bytes)) => {
                    size = size
                        .checked_add(bytes.len() as u64)
                        .ok_or(ManagementError::Budget)?;
                    if size > request.max_stored_bytes {
                        return Err(ManagementError::Budget);
                    }
                }
                Some(Err(_)) => return Err(ManagementError::InvalidEvidence),
                None => break,
            }
        }
        if matches!(
            native::value(&object_tuple, "encrypted"),
            Some("0" | "false")
        ) && native::value(&object_tuple, "size").and_then(|size| size.parse::<u64>().ok())
            != Some(size)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        (Some(size), ReadProof::FileCleanEof, None)
    };
    // Directory receipts already bracket the clean EOF with no-redirect node
    // checks. File reads retain their original before/after identity behavior.
    let after_node = if request.directory {
        directory_receipt
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?
            .node_identity()
            .to_owned()
    } else {
        tokio::select! { _=cancel.cancelled()=>return Err(ManagementError::InvalidEvidence), result=offline.local_node_identity()=>result.map_err(|_| ManagementError::InvalidEvidence)? }
    };
    if node != after_node
        || cancel.is_cancelled()
        || client.base_url() != config.source_endpoint
        || config.source_node() != Some(node.as_str())
        || client.stream_idle_timeout() != idle_timeout
    {
        return Err(ManagementError::Changed);
    }
    let tx = db.begin().await.map_err(|_| ManagementError::Database)?;
    if tx.get_database_backend() == DatabaseBackend::Sqlite {
        native::scope::writer_boundary(&tx).await?;
    } else {
        tx.execute_unprepared("LOCK TABLE objects,object_versions,pin_management_records IN SHARE ROW EXCLUSIVE MODE NOWAIT").await.map_err(|_| ManagementError::Database)?;
    }
    authorization.check(&tx).await?;
    let fresh = scope::capture(&tx, &content_scope, &mut budget).await?;
    if fresh != frame
        || safe_object(&fresh, &request.identity)? != (object_tuple.clone(), version_tuple.clone())
    {
        return Err(ManagementError::Changed);
    }
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let expires_at = started
        .checked_add_signed(request.ttl)
        .ok_or(ManagementError::InvalidEvidence)?;
    if now < started || now >= expires_at || cancel.is_cancelled() {
        return Err(ManagementError::Expired);
    }
    let attestation = ReadAttestation {
        identity: request.identity.clone(),
        object_tuple,
        version_tuple,
        source: SourceReadBinding {
            node_identity: node,
            local_reads_only: true,
            endpoint: config.source_endpoint.clone(),
            idle_timeout_millis,
        },
        stored_bytes,
        proof,
        directory_receipt,
        observed_at: now,
        expires_at,
    };
    attestation.validate_shape()?;
    // Private endpoint is persisted only in this trusted DB result, never a file.
    let result = serde_json::to_string(&(&attestation, &config.source_endpoint))
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let before =
        serde_json::to_string(&full_before).map_err(|_| ManagementError::InvalidEvidence)?;
    records::insert_in_transaction(
        &tx,
        authorization,
        RecordWrite {
            plan_id: &request.record_id,
            item_id: &request.identity.private_version_id,
            kind: RecordKind::EvidencePreparation,
            binding: &binding,
            native_before: &before,
            result: Some(&result),
            intent_ids: &[],
            job_ids: &[],
            state: RecordState::Prepared,
            expires_at,
        },
    )
    .await?;
    #[cfg(test)]
    tests::after_attestation_insert(&tx).await;
    if scope::capture(&tx, &content_scope, &mut budget).await? != frame {
        return Err(ManagementError::Changed);
    }
    let commit_now = crate::store::database_clock::database_now(&tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    if commit_now < now || commit_now >= expires_at || cancel.is_cancelled() {
        return Err(ManagementError::Expired);
    }
    tx.commit().await.map_err(|_| ManagementError::Database)?;
    Ok(attestation)
}

#[cfg(test)]
mod tests;

async fn load<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    id: &str,
    binding: Option<&str>,
    legacy_binding: &str,
    expected_before: &(NativeRow, NativeRow),
    directory: bool,
) -> Result<Option<ReadAttestation>> {
    let work = records::SharedDecodeBudget::default();
    let (rows, _) = records::bounded_projection(
        db,
        "FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND kind='evidence_preparation'",
        &[("principal", "principal"), ("native_identity", "native_identity"), ("binding", "binding"), ("native_before", "native_before"), ("result", "result")],
        "item_id",
        vec![auth.database_scope().into(), id.into()],
        (1, native::MAX_BYTES),
        records::DecodeWork::shared(&work),
    ).await?;
    let row = rows.into_iter().next();
    let Some(row) = row else { return Ok(None) };
    if row
        .try_get::<String>("", "principal")
        .map_err(|_| ManagementError::InvalidEvidence)?
        != auth.principal()
        || row
            .try_get::<String>("", "native_identity")
            .map_err(|_| ManagementError::InvalidEvidence)?
            != auth.database_identity()
    {
        return Err(ManagementError::Unauthorized);
    }
    let stored_binding = row
        .try_get::<String>("", "binding")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    if binding.is_none_or(|binding| stored_binding != binding) && stored_binding != legacy_binding {
        return Err(ManagementError::Changed);
    }
    let before: String = row
        .try_get("", "native_before")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let original: (NativeRow, NativeRow) = work
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .typed(&before)?;
    if &original != expected_before {
        return Err(ManagementError::Changed);
    }
    let result: String = row
        .try_get("", "result")
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let (mut attestation, endpoint): (ReadAttestation, String) = work
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?
        .typed(&result)?;
    // A legacy RID can only replay its legacy proof; today's configuration must
    // never reinterpret it as a new DAG-byte reservation or recapture its body.
    let expected = if directory && stored_binding == legacy_binding {
        ReadProof::CompleteLocalDirectory
    } else if directory {
        ReadProof::CompleteLocalDirectoryDagBlocksV1
    } else {
        ReadProof::FileCleanEof
    };
    if attestation.proof != expected {
        return Err(ManagementError::Changed);
    }
    attestation.validate_shape()?;
    attestation.source.endpoint = endpoint;
    Ok(Some(attestation))
}

pub(crate) fn from_persisted_with_work(
    result: &str,
    source_endpoint: Option<&str>,
    work: &mut crate::store::pinning::management::records::DecodeBudget,
) -> Result<ReadAttestation> {
    let (mut attestation, endpoint): (ReadAttestation, String) = work.typed(result)?;
    attestation.validate_shape()?;
    if source_endpoint.is_some_and(|expected| endpoint != expected) {
        return Err(ManagementError::Changed);
    }
    attestation.source.endpoint = endpoint;
    Ok(attestation)
}
pub(crate) fn from_persisted_shared_with_work(
    result: &str,
    source_endpoint: Option<&str>,
    work: &mut records::ReadSession,
) -> Result<std::sync::Arc<ReadAttestation>> {
    let attestation = work.shared(result, |work| {
        let (mut attestation, endpoint): (ReadAttestation, String) = work.typed(result)?;
        attestation.source.endpoint = endpoint;
        Ok(attestation)
    })?;
    attestation.validate_shape()?;
    if source_endpoint
        .is_some_and(|expected| !attestation.source.original_endpoint_matches(expected))
    {
        return Err(ManagementError::Changed);
    }
    Ok(attestation)
}
