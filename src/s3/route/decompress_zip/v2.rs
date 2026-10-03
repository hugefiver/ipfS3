//! Authenticated direct ZIP v2, with independently fenced optional source publication.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{Buf as _, Bytes};
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt as _;
use s3s::{Body, S3Request, S3Response, S3Result};
use sea_orm::TransactionTrait;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    error::AppError,
    pinning::{
        policy::PublicationPolicy,
        zip_policy::{ZipPublishedOutput, ZipTargets as PolicyTargets},
    },
    state::AppState,
    store::{
        pinning::publication::{self, ZipV2Publication, ZipV2PublicationResult, ZipV2Success},
        zip::{self, execution},
    },
    zip::options::{ZipTargets, ZipV2Options},
};

use super::{
    ParsedDecompressPut, has_sse_header, reject_archive_key_collision, zip_limits_for_call,
};

const LEASE_SECONDS: i64 = 60;
const CANDIDATE_RETENTION_TIMEOUT: Duration = Duration::from_secs(1);

/// Shared direct/MPU initial-root seam. Work is dropped on loss, not drained:
/// only the caller-owned candidate slot and its original claim survive it.
pub(super) async fn build_zip_root(
    state: &AppState,
    id: &str,
    manifest: &crate::zip::batch::FinalZipManifest,
    enabled: bool,
    cancel: &CancellationToken,
) -> S3Result<zip::RootOutcome> {
    let mut claim = None;
    let mut candidate = None;
    let outcome = {
        let work = build_root_inner(
            state,
            id,
            manifest,
            enabled,
            cancel,
            &mut claim,
            &mut candidate,
        );
        tokio::pin!(work);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => None,
            outcome = &mut work => Some(outcome),
        }
    };
    if let (Some(root), Some(candidate)) = (&claim, candidate) {
        let retained = tokio::time::timeout(
            CANDIDATE_RETENTION_TIMEOUT,
            zip::retain_candidate(
                state.store.db(),
                root,
                &candidate.node_identity,
                "hot",
                &candidate.cid,
            ),
        )
        .await;
        if !matches!(retained, Ok(Ok(()))) {
            tracing::warn!(
                batch_id = id,
                "ZIP candidate retention failed or timed out; original root intent remains recoverable"
            );
            if outcome.is_some() && !cancel.is_cancelled() {
                return Ok(zip::RootOutcome::ClaimedFailed {
                    claim: root.clone(),
                    code: "root_receipt_failed",
                });
            }
        }
    }
    // Cancellation cannot be converted into a root warning that publishes files.
    if cancel.is_cancelled() {
        return Err(conflict());
    }
    outcome.ok_or_else(conflict)
}

async fn build_root_inner(
    state: &AppState,
    id: &str,
    manifest: &crate::zip::batch::FinalZipManifest,
    enabled: bool,
    cancel: &CancellationToken,
    current: &mut Option<zip::RootClaim>,
    candidate: &mut Option<crate::kubo::directory::DirectoryCandidate>,
) -> zip::RootOutcome {
    use crate::kubo::directory::{DirectoryBuildError, build_directory_capturing_candidate};
    use zip::RootOutcome;
    if !enabled {
        return RootOutcome::Disabled;
    }
    if manifest.successful.is_empty() {
        return RootOutcome::Empty;
    }
    if let Some(code) = manifest.root_error {
        return RootOutcome::Failed { code };
    }
    let db = state.store.db();
    let claim = match zip::claim_root(db, id, &uuid::Uuid::new_v4().to_string(), 600).await {
        Ok(claim) => claim,
        Err(_) => {
            return RootOutcome::Failed {
                code: "root_intent_failed",
            };
        }
    };
    *current = Some(claim.clone());
    if zip::mark_invoked(db, &claim).await.is_err() {
        return RootOutcome::ClaimedFailed {
            claim,
            code: "root_intent_failed",
        };
    }
    let root = match build_directory_capturing_candidate(
        &state.kubo,
        &manifest.directory_files(),
        cancel,
        candidate,
    )
    .await
    {
        Ok(Some(root)) => root,
        Ok(None) => {
            return RootOutcome::ClaimedFailed {
                claim,
                code: "invalid_manifest",
            };
        }
        Err(error) => {
            let code = match error.reason() {
                DirectoryBuildError::PathConflict => "path_conflict",
                DirectoryBuildError::InvalidManifest => "invalid_manifest",
                DirectoryBuildError::BlockTooLarge => "directory_block_too_large",
                DirectoryBuildError::HashCollision => "directory_hash_collision",
                _ => "directory_build_failed",
            };
            return RootOutcome::ClaimedFailed { claim, code };
        }
    };
    if cancel.is_cancelled() {
        return RootOutcome::ClaimedFailed {
            claim,
            code: "directory_build_failed",
        };
    }
    let node = root.local_residency.node_identity;
    let cid = root.cid;
    if zip::retain_candidate(db, &claim, &node, "hot", &cid)
        .await
        .is_err()
    {
        return RootOutcome::ClaimedFailed {
            claim,
            code: "root_receipt_failed",
        };
    }
    *candidate = None;
    let receipt = serde_json::json!({"node_identity": node, "cid": cid}).to_string();
    if cancel.is_cancelled()
        || zip::verify_root(db, &claim, &node, "hot", &cid, &receipt)
            .await
            .is_err()
    {
        return RootOutcome::ClaimedFailed {
            claim,
            code: "root_receipt_failed",
        };
    }
    RootOutcome::Verified {
        claim,
        node_identity: node,
        tier: "hot".into(),
        cid,
    }
}

fn conflict() -> s3s::S3Error {
    let mut error = s3s::s3_error!(
        OperationAborted,
        "ZIP v2 token or output ownership conflict"
    );
    error.set_status_code(StatusCode::CONFLICT);
    error
}

fn store_error(error: AppError) -> s3s::S3Error {
    match error {
        AppError::InvalidZipParameter(_) | AppError::StaleContentMutation => conflict(),
        other => other.into(),
    }
}

async fn admitted_manifest(
    state: &AppState,
    snapshot: &execution::Snapshot,
    prefix: &str,
    publish_source: bool,
) -> S3Result<(
    crate::zip::batch::FinalZipManifest,
    Option<crate::store::import::ownership::StandardMutationGuard>,
)> {
    use crate::zip::batch::{FinalZipManifest, ManifestFailure, ManifestFile};

    let stored = zip::snapshot(state.store.db(), &snapshot.id)
        .await?
        .ok_or_else(conflict)?;
    let batch = &stored.batch;
    if batch.state != "open"
        || !batch.manifest_prepared
        || batch.owner != snapshot.owner
        || batch.source != snapshot.source
        || batch.token != snapshot.token
        || batch.bucket != snapshot.bucket
        || batch.archive_key != snapshot.source_key
        || batch.captured_options != snapshot.captured_options
        || batch.fingerprint != "pending"
        || batch.source_published
        || stored.entries.len() > crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize
    {
        return Err(conflict());
    }
    let source_guard = if publish_source {
        let generation = batch
            .input_identity
            .strip_prefix("zip-v2-source-gen:")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|generation| *generation > 0)
            .ok_or_else(conflict)?;
        Some(crate::store::import::ownership::StandardMutationGuard {
            bucket: snapshot.bucket.clone(),
            key: snapshot.source_key.clone(),
            mutation_id: format!("zip-v2-source:{}", snapshot.id),
            expected_generation: generation,
            mutation_prefix: None,
        })
    } else {
        if batch.input_identity != "pending" {
            return Err(conflict());
        }
        None
    };
    let mut manifest = FinalZipManifest::default();
    for entry in stored.entries {
        if entry.version_row_id.is_some() {
            return Err(conflict());
        }
        match (entry.object_key, entry.cid, entry.size, entry.error_code) {
            (Some(key), Some(cid), Some(size), None) if size >= 0 => {
                if key.strip_prefix(prefix) != Some(entry.path.as_str()) {
                    manifest.root_error = Some("invalid_manifest");
                }
                manifest.successful.push(ManifestFile {
                    relative_path: entry.path,
                    object_key: key,
                    cid,
                    size,
                });
            }
            (None, None, None, Some(code)) => {
                let code = match code.as_str() {
                    "entry_read_failed" => "entry_read_failed",
                    "entry_upload_failed" => "entry_upload_failed",
                    "entry_failed" => "entry_failed",
                    _ => return Err(conflict()),
                };
                manifest.failed.push(ManifestFailure {
                    path: entry.path,
                    code,
                });
            }
            _ => return Err(conflict()),
        }
    }
    Ok((manifest, source_guard))
}

fn failure(message: &'static str) -> s3s::S3Error {
    s3s::s3_error!(InvalidRequest, "{message}")
}

// Hash the immutable *signed semantics*, not the Authorization signature/date or
// claimed HTTP payload hash. A different body has to be independently read and
// compared to input_sha256 even when its extracted output is identical.
fn request_contract(
    req: &S3Request<Body>,
    parsed: &ParsedDecompressPut,
    options: &ZipV2Options,
    principal: &str,
) -> S3Result<String> {
    let authorization = req
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .ok_or_else(|| failure("ZIP v2 requires SigV4"))?;
    let signed = authorization
        .split(", ")
        .find_map(|field| field.strip_prefix("SignedHeaders="))
        .ok_or_else(|| failure("ZIP v2 requires signed headers"))?;
    let mut headers = BTreeMap::new();
    for name in signed.split(';') {
        if matches!(
            name,
            "authorization" | "x-amz-date" | "x-amz-content-sha256" | "content-length" | "host"
        ) {
            continue;
        }
        let values = req
            .headers
            .get_all(name)
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(str::to_owned)
                    .map_err(|_| failure("invalid signed ZIP header"))
            })
            .collect::<S3Result<Vec<_>>>()?;
        headers.insert(name.to_owned(), values);
    }
    // An unsigned metadata header cannot modify a v2 source publication later.
    if req.headers.keys().any(|name| {
        name.as_str().starts_with("x-amz-meta-")
            && !signed
                .split(';')
                .any(|signed_name| signed_name == name.as_str())
    }) {
        return Err(failure("ZIP v2 metadata must be signed"));
    }
    if options.publish_source
        && req.headers.contains_key(http::header::CONTENT_TYPE)
        && !signed.split(';').any(|name| name == "content-type")
    {
        return Err(failure("ZIP v2 source Content-Type must be signed"));
    }
    let controls = serde_json::json!({
        "principal": principal, "bucket": parsed.bucket, "source_key": parsed.key,
        "target_prefix": parsed.target_prefix,
        "publish_source": options.publish_source,
        "publish_extracted": options.publish_extracted, "targets": options.targets,
        "token": options.token, "root_override": options.root_override,
        "result_version": options.result_version, "signed_headers": headers,
    });
    serde_json::to_string(&controls)
        .map_err(|_| s3s::s3_error!(InternalError, "ZIP v2 contract encoding failed"))
}

struct InputDigest {
    hash: Sha256,
    size: i64,
    max_bytes: u64,
    eof: bool,
    failed: bool,
    limit_exceeded: bool,
    tail: Vec<u8>,
}

impl InputDigest {
    fn new(max_bytes: u64) -> Self {
        Self {
            hash: Sha256::new(),
            size: 0,
            max_bytes,
            eof: false,
            failed: false,
            limit_exceeded: false,
            tail: Vec::new(),
        }
    }

    fn add(&mut self, bytes: &[u8]) -> S3Result<()> {
        let next = i64::try_from(bytes.len())
            .ok()
            .and_then(|len| self.size.checked_add(len))
            .filter(|size| *size as u64 <= self.max_bytes);
        let Some(next) = next else {
            self.failed = true;
            self.limit_exceeded = true;
            return Err(failure("ZIP v2 raw input byte limit exceeded"));
        };
        // Reject the entire over-limit chunk before hashing or forwarding it.
        self.size = next;
        self.hash.update(bytes);
        const TAIL: usize = 65_557; // EOCD + maximum ZIP comment.
        if bytes.len() >= TAIL {
            self.tail.clear();
            self.tail.extend_from_slice(&bytes[bytes.len() - TAIL..]);
        } else {
            let keep = self
                .tail
                .len()
                .saturating_add(bytes.len())
                .saturating_sub(TAIL);
            self.tail.drain(..keep);
            self.tail.extend_from_slice(bytes);
        }
        Ok(())
    }

    fn finish(&self, headers: &HeaderMap) -> S3Result<(String, i64)> {
        if self.limit_exceeded {
            return Err(failure("ZIP v2 raw input byte limit exceeded"));
        }
        if !self.eof || self.failed {
            return Err(s3s::s3_error!(
                IncompleteBody,
                "ZIP v2 input did not reach EOF"
            ));
        }
        let actual = hex::encode(self.hash.clone().finalize());
        // Header-SigV4 payload digest is also an independent transport check.
        let signed_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| failure("ZIP v2 requires a signed payload SHA256"))?;
        if signed_hash.len() != 64
            || !signed_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !signed_hash.eq_ignore_ascii_case(&actual)
        {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "ZIP v2 payload digest mismatch"
            ));
        }
        // The streaming ZIP extractor reads local entries. Verify the complete
        // uploaded input also ends in a structurally bounded EOCD/comment.
        if !self
            .tail
            .windows(4)
            .enumerate()
            .any(|(position, signature)| {
                signature == b"PK\x05\x06"
                    && self
                        .tail
                        .get(position + 20..position + 22)
                        .and_then(|bytes| bytes.try_into().ok())
                        .map(u16::from_le_bytes)
                        .is_some_and(|comment| {
                            position + 22 + usize::from(comment) == self.tail.len()
                        })
            })
        {
            return Err(failure("ZIP v2 input lacks a complete ZIP trailer"));
        }
        Ok((actual, self.size))
    }
}

// Consume every HTTP frame, including the final chunk/trailer before marking EOF.
// A frame error is never interpreted as a clean end of the ZIP stream.
async fn hash_body(mut body: Body, digest: &mut InputDigest) -> S3Result<()> {
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| {
            digest.failed = true;
            s3s::s3_error!(IncompleteBody, "ZIP v2 input stream failed")
        })?;
        if let Ok(mut bytes) = frame.into_data() {
            while bytes.has_remaining() {
                let chunk = bytes.chunk();
                digest.add(chunk)?;
                let len = chunk.len();
                bytes.advance(len);
            }
        }
    }
    digest.eof = true;
    Ok(())
}

struct Lease {
    cancelled: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Lease {
    fn start(state: Arc<AppState>, claim: execution::Claim) -> Self {
        let cancelled = CancellationToken::new();
        let stop = cancelled.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        match crate::store::import::ownership::renew_zip_v2_direct_group(state.store.db(), &claim, LEASE_SECONDS).await {
                            Ok(true) => {},
                            Ok(false) | Err(_) => { stop.cancel(); break; }
                        }
                    }
                }
            }
        });
        Self { cancelled, task }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.cancelled.cancel();
        self.task.abort();
    }
}

fn ack_xml(
    id: &str,
    sha: &str,
    published: usize,
    failed: usize,
    root: &str,
    source: Option<(&str, Option<&str>)>,
) -> String {
    let (source_published, archive) = match source {
        Some((cid, version)) => (
            "true",
            format!(
                "<ArchiveETag>\"{cid}\"</ArchiveETag>{}",
                version
                    .map(|id| format!("<VersionId>{id}</VersionId>"))
                    .unwrap_or_default()
            ),
        ),
        None => ("false", String::new()),
    };
    format!(
        "<ZipBatchResult><BatchId>{id}</BatchId><SourcePublished>{source_published}</SourcePublished>{archive}<InputSHA256>{sha}</InputSHA256><PublishedCount>{published}</PublishedCount><FailedCount>{failed}</FailedCount><RootStatus>{root}</RootStatus></ZipBatchResult>"
    )
}

fn response_from_terminal(
    id: &str,
    execution: &execution::Snapshot,
    root: &zip::BatchSnapshot,
) -> S3Result<S3Response<Body>> {
    if execution.state != "completed"
        || root.batch.state != "published"
        || root.batch.id != id
        || root.batch.owner != execution.owner
        || root.batch.bucket != execution.bucket
        || root.batch.archive_key != execution.source_key
    {
        return Err(conflict());
    }
    let receipt: serde_json::Value =
        serde_json::from_str(execution.terminal_result.as_deref().ok_or_else(conflict)?)
            .map_err(|_| conflict())?;
    let sha = execution.input_sha256.as_deref().ok_or_else(conflict)?;
    if receipt.get("input_sha256").and_then(|value| value.as_str()) != Some(sha) {
        return Err(conflict());
    }
    let published = root
        .entries
        .iter()
        .filter(|entry| entry.cid.is_some() && entry.version_row_id.is_some())
        .count();
    let failed = root
        .entries
        .iter()
        .filter(|entry| entry.error_code.is_some())
        .count();
    if published == 0 && !root.batch.source_published {
        return Err(failure("ZIP v2 did not publish any outputs"));
    }
    if receipt.get("published_count").and_then(|v| v.as_u64()) != Some(published as u64)
        || receipt.get("failed_count").and_then(|v| v.as_u64()) != Some(failed as u64)
    {
        return Err(conflict());
    }
    let source = if root.batch.source_published {
        let cid = receipt
            .get("source_cid")
            .and_then(|v| v.as_str())
            .ok_or_else(conflict)?;
        let size = receipt
            .get("source_size")
            .and_then(|v| v.as_i64())
            .ok_or_else(conflict)?;
        if cid.is_empty()
            || size != execution.input_art_size.ok_or_else(conflict)?
            || Some(cid) != execution.input_art_cid.as_deref()
            || receipt.get("source_policy").is_none()
        {
            return Err(conflict());
        }
        let version = receipt.get("source_version_id").ok_or_else(conflict)?;
        if !version.is_null() && version.as_str().is_none() {
            return Err(conflict());
        }
        Some((cid, version.as_str()))
    } else {
        if receipt.get("source_cid").is_some() {
            return Err(conflict());
        }
        None
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    headers.insert(
        "x-ipfs-s3-zip-batch-id",
        http::HeaderValue::from_str(id).map_err(|_| conflict())?,
    );
    // Root CID is omitted here unless the separate committed receipt proves adoption.
    let root_status = if matches!(root.batch.root_status.as_str(), "complete" | "partial")
        && !root.references.iter().any(|reference| {
            reference.state == "adopted"
                && reference.verification_receipt.is_some()
                && Some(reference.cid.as_str()) == root.batch.root_cid.as_deref()
                && reference.revision == root.batch.root_revision
                && reference.epoch == root.batch.root_epoch
                && root.builds.iter().any(|build| {
                    build.revision == reference.revision
                        && build.epoch == reference.epoch
                        && build.status == "verified"
                })
        }) {
        "failed"
    } else {
        &root.batch.root_status
    };
    headers.insert(
        "x-ipfs-s3-zip-root-status",
        http::HeaderValue::from_str(root_status).map_err(|_| conflict())?,
    );
    if let Some((cid, version)) = source {
        headers.insert(
            http::header::ETAG,
            http::HeaderValue::from_str(&format!("\"{cid}\"")).map_err(|_| conflict())?,
        );
        if let Some(version) = version {
            headers.insert(
                "x-amz-version-id",
                http::HeaderValue::from_str(version).map_err(|_| conflict())?,
            );
        }
    }
    if let Some(cid) = root
        .batch
        .root_cid
        .as_deref()
        .filter(|_| matches!(root_status, "complete" | "partial"))
    {
        headers.insert(
            "x-ipfs-s3-zip-root-cid",
            http::HeaderValue::from_str(cid).map_err(|_| conflict())?,
        );
    }
    Ok(S3Response::with_headers(
        Body::from(ack_xml(id, sha, published, failed, root_status, source)),
        headers,
    ))
}

async fn committed_response(
    state: &AppState,
    id: &str,
    execution: &execution::Snapshot,
) -> S3Result<S3Response<Body>> {
    let root = zip::snapshot(state.store.db(), id)
        .await?
        .ok_or_else(conflict)?;
    response_from_terminal(id, execution, &root)
}

pub(super) async fn put(
    state: &Arc<AppState>,
    req: S3Request<Body>,
    parsed: ParsedDecompressPut,
    options: ZipV2Options,
    max_decompressed_bytes: u64,
) -> S3Result<S3Response<Body>> {
    crate::s3::http::reject_write_conditions(&req.headers, false)?;
    crate::s3::ops::storage_class::require_standard_write_headers(&req.headers)?;
    if has_sse_header(&req.headers) {
        return Err(failure("ZIP v2 does not support encryption"));
    }
    if !crate::store::bucket::exists(state.store.db(), &parsed.bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found"));
    }
    let source_tags = if options.publish_source {
        super::parse_publication_tags(&req.headers)?
    } else {
        Vec::new()
    };
    let source_content_type = req
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let source_metadata = if options.publish_source {
        crate::s3::ops::object::extract_custom_metadata(&req.headers)
    } else {
        None
    };
    let principal = crate::s3::ops::object::principal_id(&req)?;
    let contract = request_contract(&req, &parsed, &options, &principal)?;
    let fingerprint = hex::encode(Sha256::digest(contract.as_bytes()));
    let rules = state.pinning.zip_output_rules();
    let rule_revision = rules.revision().to_owned();
    let captured = serde_json::json!({"options": options, "target_prefix": parsed.target_prefix, "rule_revision": rule_revision}).to_string();
    let request = execution::Admission {
        id: uuid::Uuid::new_v4().to_string(),
        owner: principal.clone(),
        source: "direct".into(),
        token: options.token.clone(),
        request_fingerprint: fingerprint.clone(),
        request_contract: contract,
        bucket: parsed.bucket.clone(),
        source_key: parsed.key.clone(),
        captured_options: captured.clone(),
    };
    let execution = execution::admit(state.store.db(), &request)
        .await
        .map_err(store_error)?;
    let id = execution.id.clone();
    let snapshot: serde_json::Value =
        serde_json::from_str(&execution.captured_options).map_err(|_| conflict())?;
    let saved_options: ZipV2Options =
        serde_json::from_value(snapshot.get("options").cloned().ok_or_else(conflict)?)
            .map_err(|_| conflict())?;
    let captured_rule_revision = snapshot
        .get("rule_revision")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(conflict)?
        .to_owned();
    if saved_options.token != options.token
        || saved_options.root_override != options.root_override
        || saved_options.targets != options.targets
        || saved_options.publish_source != options.publish_source
        || saved_options.publish_extracted != options.publish_extracted
        || saved_options.result_version != options.result_version
        || snapshot
            .get("target_prefix")
            .and_then(serde_json::Value::as_str)
            != Some(parsed.target_prefix.as_str())
    {
        return Err(conflict());
    }
    let mut input = InputDigest::new(state.pinning.zip_extraction_limits().max_archive_bytes());
    // Terminal and concurrently owned attempts must still read their *entire*
    // incoming body before a replay/conflict decision. No Kubo add for replay.
    if execution.state == "completed"
        || execution.state == "fenced"
        || execution
            .lease_until
            .is_some_and(|until| until > chrono::Utc::now())
    {
        hash_body(req.input, &mut input).await?;
        let (sha, _) = input.finish(&req.headers)?;
        let verified = execution::read_for_replay(state.store.db(), &request, &sha)
            .await
            .map_err(store_error)?;
        if let Some(verified) = verified.filter(|snapshot| snapshot.state == "completed") {
            return committed_response(state, &id, &verified).await;
        }
        return Err(conflict());
    }
    // Never perform Kubo IO against a changed remote authorization snapshot.
    // Local-only requests have no remote intent, but still retain this revision.
    if options.targets != ZipTargets::None && captured_rule_revision != rules.revision() {
        return Err(conflict());
    }
    let worker = uuid::Uuid::new_v4().to_string();
    let Some(claim) = execution::claim(state.store.db(), &id, &worker, LEASE_SECONDS)
        .await
        .map_err(store_error)?
    else {
        hash_body(req.input, &mut input).await?;
        input.finish(&req.headers)?;
        return Err(conflict());
    };
    let lease = Lease::start(state.clone(), claim.clone());
    let mut recovered_source_guard = None;
    let (archive, sha, manifest) = if execution.state == "admitted" {
        tokio::select! {
            result = hash_body(req.input, &mut input) => result?,
            _ = lease.cancelled.cancelled() => return Err(conflict()),
        }
        let (sha, size) = input.finish(&req.headers)?;
        let recovered = execution::read_for_replay(state.store.db(), &request, &sha)
            .await
            .map_err(store_error)?
            .filter(|row| {
                row.state == "admitted"
                    && row.epoch == claim.epoch
                    && row.worker.as_deref() == Some(&claim.worker)
                    && row.input_art_size == Some(size)
            })
            .ok_or_else(conflict)?;
        let cid = recovered.input_art_cid.clone().ok_or_else(conflict)?;
        let (manifest, source_guard) = admitted_manifest(
            state,
            &recovered,
            &parsed.target_prefix,
            options.publish_source,
        )
        .await?;
        recovered_source_guard = source_guard;
        (
            crate::s3::ops::object::StoredObject { cid, size },
            sha,
            manifest,
        )
    } else {
        let digest = Arc::new(Mutex::new(input));
        let stream_digest = digest.clone();
        let stream = async_stream::stream! {
            let mut body = req.input;
            while let Some(frame) = body.frame().await {
                match frame {
                    Ok(frame) => {
                        if let Ok(mut data) = frame.into_data() {
                            while data.has_remaining() {
                                let bytes = Bytes::copy_from_slice(data.chunk());
                                let len = bytes.len();
                                let result = stream_digest.lock().expect("ZIP digest mutex").add(&bytes);
                                if result.is_err() { stream_digest.lock().expect("ZIP digest mutex").failed = true; yield Err(std::io::Error::other("ZIP v2 input too large")); return; }
                                data.advance(len);
                                yield Ok::<Bytes, std::io::Error>(bytes);
                            }
                        }
                    },
                    Err(_) => { stream_digest.lock().expect("ZIP digest mutex").failed = true; yield Err(std::io::Error::other("ZIP v2 input stream failed")); return; }
                }
            }
            stream_digest.lock().expect("ZIP digest mutex").eof = true;
        };
        let archive = tokio::select! {
            result = crate::s3::ops::object::add_plain_object_stream(state, Box::pin(stream)) => result,
            _ = lease.cancelled.cancelled() => return Err(conflict()),
        }
        .map_err(|error| {
            let digest = digest.lock().expect("ZIP digest mutex");
            if digest.limit_exceeded {
                failure("ZIP v2 raw input byte limit exceeded")
            } else if digest.failed {
                s3s::s3_error!(IncompleteBody, "ZIP v2 body or signed trailer failed")
            } else {
                error.into()
            }
        })?;
        let (sha, bytes) = digest
            .lock()
            .expect("ZIP digest mutex")
            .finish(&req.headers)?;
        if archive.size != bytes {
            return Err(s3s::s3_error!(
                IncompleteBody,
                "ZIP v2 archive byte count differs"
            ));
        }
        execution::bind_clean_input(state.store.db(), &claim, &sha, &archive.cid, bytes)
            .await
            .map_err(store_error)?;
        let manifest = if options.publish_extracted {
            let archive_stream =
                crate::kubo::cat::stream_cat(&state.kubo, &archive.cid, None).await?;
            let limits = zip_limits_for_call(state, max_decompressed_bytes);
            let outcome = tokio::select! {
                result = crate::zip::extract::extract_zip_stream_with_limits(state, &parsed.target_prefix, archive_stream, limits) => result?,
                _ = lease.cancelled.cancelled() => return Err(conflict()),
            };
            reject_archive_key_collision(&parsed.key, &outcome.entries)?;
            crate::zip::batch::final_zip_manifest(
                &outcome.entries,
                &outcome.failures,
                &parsed.target_prefix,
            )
        } else {
            crate::zip::batch::final_zip_manifest(&[], &[], &parsed.target_prefix)
        };
        (archive, sha, manifest)
    };
    let source_preview = options.publish_source.then(|| ZipPublishedOutput {
        bucket: parsed.bucket.clone(),
        key: parsed.key.clone(),
        version_id: "unpublished-preview".into(),
        cid: archive.cid.clone(),
    });
    let planned = if options.targets == ZipTargets::None {
        Vec::new()
    } else {
        let outputs = manifest
            .successful
            .iter()
            .map(|file| ZipPublishedOutput {
                bucket: parsed.bucket.clone(),
                key: file.object_key.clone(),
                // Only bucket/key and policy inputs predict intents. Publication
                // replans with the actual newly inserted private version row.
                version_id: "unpublished-preview".into(),
                cid: file.cid.clone(),
            })
            .collect::<Vec<_>>();
        rules
            .plan(
                PolicyTargets {
                    source: matches!(options.targets, ZipTargets::Source | ZipTargets::Both),
                    extracted: matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both),
                },
                source_preview.clone(),
                &outputs,
            )
            .map_err(|_| failure("ZIP v2 output rules rejected the manifest"))?
            .outputs
    };
    let planned = planned
        .into_iter()
        .map(|decision| {
            (
                decision.output.key,
                decision
                    .intents
                    .into_iter()
                    .map(|intent| intent.intent)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let source_guard = if execution.state == "admitted" {
        recovered_source_guard
    } else {
        let batch = zip::BatchAdmission {
            id: id.clone(),
            owner: principal,
            source: "direct".into(),
            token: options.token.clone(),
            fingerprint: "pending".into(),
            bucket: parsed.bucket.clone(),
            archive_key: parsed.key.clone(),
            input_identity: "pending".into(),
            captured_options: execution.captured_options.clone(),
        };
        let tx = state.store.db().begin().await.map_err(AppError::from)?;
        crate::store::import::ownership::lock_bucket_for_ownership(&tx, &parsed.bucket).await?;
        let items = manifest
            .successful
            .iter()
            .map(|file| execution::ManifestItem::Success {
                path: file.relative_path.clone(),
                object_key: file.object_key.clone(),
                cid: file.cid.clone(),
                size: file.size,
            })
            .chain(
                manifest
                    .failed
                    .iter()
                    .map(|file| execution::ManifestItem::Failure {
                        path: file.path.clone(),
                        code: file.code.into(),
                    }),
            )
            .collect::<Vec<_>>();
        let ids = manifest
            .successful
            .iter()
            .map(|file| (file.object_key.clone(), uuid::Uuid::new_v4().to_string()))
            .collect::<BTreeMap<_, _>>();
        let admission = async {
            let source_guard = if options.publish_source {
                Some(
                    crate::store::import::ownership::admit_zip_v2_source_in_transaction(
                        &tx,
                        &parsed.bucket,
                        &parsed.key,
                        &ids.keys().cloned().collect(),
                        &format!("zip-v2-source:{id}"),
                        chrono::Utc::now(),
                    )
                    .await?,
                )
            } else {
                None
            };
            execution::admit_manifest_in_transaction(&tx, &claim, &items, &ids).await?;
            zip::BatchAdmission::admit_in_transaction(&tx, &batch).await?;
            zip::ManifestItem::prepare_manifest_in_transaction(
                &tx,
                &id,
                &manifest.manifest_items(),
            )
            .await?;
            if let Some(guard) = &source_guard {
                crate::store::import::ownership::capture_zip_v2_source_guard_in_transaction(
                    &tx, &claim, guard,
                )
                .await?;
            }
            Ok::<_, AppError>(source_guard)
        }
        .await;
        let source_guard = match admission {
            Ok(guard) => guard,
            Err(error) => {
                tx.rollback().await.map_err(AppError::from)?;
                return Err(store_error(error));
            }
        };
        tx.commit().await.map_err(AppError::from)?;
        source_guard
    };
    if !crate::store::import::ownership::renew_zip_v2_direct_group(
        state.store.db(),
        &claim,
        LEASE_SECONDS,
    )
    .await
    .map_err(store_error)?
    {
        return Err(conflict());
    }
    let root_outcome = build_zip_root(
        state,
        &id,
        &manifest,
        saved_options.build_root(),
        &lease.cancelled,
    )
    .await?;
    let successes = manifest
        .successful
        .iter()
        .map(|file| ZipV2Success {
            object: publication::PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &parsed.bucket,
                &file.object_key,
                file.cid.clone(),
                file.size,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            path: file.relative_path.clone(),
            object_key: file.object_key.clone(),
            cid: file.cid.clone(),
            size: file.size,
            policy: PublicationPolicy {
                tags: Vec::new(),
                leases: planned.get(&file.object_key).cloned().unwrap_or_default(),
            },
        })
        .collect::<Vec<_>>();
    let terminal = serde_json::json!({"input_sha256": sha, "published_count": manifest.successful.len(), "failed_count": manifest.failed.len(), "status": if manifest.successful.is_empty() && !options.publish_source { "failed" } else { "completed" }}).to_string();
    let publication = ZipV2Publication {
        claim: claim.clone(),
        source: options.publish_source.then(|| {
            publication::PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                &parsed.bucket,
                &parsed.key,
                archive.cid.clone(),
                archive.size,
                source_content_type,
                source_metadata,
                false,
                None,
                None,
                chrono::Utc::now(),
            )
        }),
        source_policy: options.publish_source.then(|| PublicationPolicy {
            tags: source_tags,
            leases: planned.get(&parsed.key).cloned().unwrap_or_default(),
        }),
        source_guard,
        successes,
        targets: options.targets,
        captured_rule_revision: Some(captured_rule_revision),
        root_outcome,
        terminal_result: terminal,
    };
    if lease.cancelled.is_cancelled() {
        return Err(conflict());
    }
    match publication::publish_zip_v2(
        state.store.db(),
        publication,
        rules,
        state.pinning.effective_config(),
        state.pinning.provider_limits(),
    )
    .await
    .map_err(store_error)?
    {
        ZipV2PublicationResult::Published(_) => {}
        ZipV2PublicationResult::Fenced => return Err(conflict()),
    }
    let completed = execution::read(state.store.db(), &id)
        .await
        .map_err(store_error)?
        .ok_or_else(conflict)?;
    committed_response(state, &id, &completed).await
}

fn legacy_status_response(
    snapshot: &zip::BatchSnapshot,
    settled_zero_status: Option<&str>,
) -> S3Response<Body> {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    // Authorization, XML and headers share one transactionally consistent
    // snapshot. In particular, a root-only retry cannot change just the headers.
    super::insert_root_headers_from_snapshot(&mut headers, snapshot);
    let escape = |value: &str| quick_xml::escape::escape(value).into_owned();
    let mut body = format!(
        "<ZipBatchStatus><BatchId>{}</BatchId><State>{}</State><SourcePublished>{}</SourcePublished><RootStatus>{}</RootStatus>",
        escape(&snapshot.batch.id),
        escape(settled_zero_status.unwrap_or(&snapshot.batch.state)),
        snapshot.batch.source_published,
        escape(&snapshot.batch.root_status),
    );
    if let Some(status) = settled_zero_status {
        body.push_str(&format!("<BatchStatus>{}</BatchStatus>", escape(status)));
    }
    if let Some(code) = &snapshot.batch.root_error_code {
        body.push_str(&format!("<RootWarning>{}</RootWarning>", escape(code)));
    }
    if let Some(cid) = headers
        .get("x-ipfs-s3-zip-root-cid")
        .and_then(|value| value.to_str().ok())
    {
        body.push_str(&format!("<RootCID>{}</RootCID>", escape(cid)));
    }
    body.push_str("</ZipBatchStatus>");
    S3Response::with_headers(Body::from(body), headers)
}

pub(super) async fn status(state: &AppState, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    let (bucket, key) = super::parse_path_bucket_key(&req.uri)?;
    let query = crate::s3::query::decoded_query_pairs(&req.uri)?;
    if query.len() != 1
        || query[0].0 != "ipfs3-zip-batch"
        || uuid::Uuid::parse_str(&query[0].1).is_err()
    {
        return Err(failure("invalid ZIP v2 batch query"));
    }
    let principal = crate::s3::ops::object::principal_id(&req)?;
    let id = &query[0].1;
    let found = execution::read(state.store.db(), id)
        .await
        .map_err(store_error)?
        .filter(|batch| {
            batch.source == "direct"
                && batch.owner == principal
                && batch.bucket == bucket
                && batch.source_key == key
        });
    let Some(found) = found else {
        let legacy = zip::snapshot(state.store.db(), id)
            .await?
            .filter(|snapshot| {
                matches!(snapshot.batch.source.as_str(), "direct" | "mpu")
                    && snapshot.batch.owner == principal
                    && snapshot.batch.bucket == bucket
                    && snapshot.batch.archive_key == key
            })
            .ok_or_else(|| s3s::s3_error!(NoSuchKey, "ZIP batch not found"))?;
        let v2_mpu_intake = if legacy.batch.source == "mpu" {
            crate::store::multipart::v2_zip::read_by_upload(state.store.db(), id)
                .await
                .map_err(store_error)?
        } else {
            None
        };
        let settled_zero_status = if legacy.batch.source == "mpu"
            && v2_mpu_intake.is_some()
            && legacy.batch.state == "published"
            && !legacy.batch.source_published
            && legacy.entries.iter().all(|entry| entry.cid.is_none())
        {
            let intake = v2_mpu_intake.as_ref().ok_or_else(conflict)?;
            let options: ZipV2Options =
                serde_json::from_str(&intake.captured_options).map_err(|_| conflict())?;
            if intake.owner != principal
                || intake.bucket != bucket
                || intake.archive_key != key
                || intake.active_upload_id.is_some()
                || options.publish_source
            {
                return Err(conflict());
            }
            if legacy.batch.root_cid.is_some()
                || !matches!(legacy.batch.root_status.as_str(), "disabled" | "empty")
            {
                return Err(conflict());
            }
            let completed = execution::read(state.store.db(), id)
                .await
                .map_err(store_error)?
                .filter(|execution| {
                    execution.source == "mpu"
                        && execution.state == "completed"
                        && execution.owner == principal
                        && execution.bucket == bucket
                        && execution.source_key == key
                })
                .ok_or_else(conflict)?;
            let terminal: serde_json::Value =
                serde_json::from_str(completed.terminal_result.as_deref().ok_or_else(conflict)?)
                    .map_err(|_| conflict())?;
            let status = terminal["status"].as_str().ok_or_else(conflict)?;
            if !matches!(status, "failed" | "empty")
                || terminal["published_count"] != 0
                || terminal["failed_count"]
                    != legacy
                        .entries
                        .iter()
                        .filter(|entry| entry.error_code.is_some())
                        .count()
                || terminal.get("source_version_id").is_some()
            {
                return Err(conflict());
            }
            Some(status.to_owned())
        } else {
            None
        };
        return Ok(legacy_status_response(
            &legacy,
            settled_zero_status.as_deref(),
        ));
    };
    if found.state == "completed" {
        if let Some(root) = zip::snapshot(state.store.db(), id).await?
            && root.batch.state == "published"
            && root.batch.owner == principal
            && root.batch.bucket == bucket
            && root.batch.archive_key == key
            && !root.batch.source_published
            && root.entries.iter().all(|entry| entry.cid.is_none())
        {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/xml"),
            );
            return Ok(S3Response::with_headers(
                Body::from(format!(
                    "<ZipBatchStatus><BatchId>{id}</BatchId><State>failed</State></ZipBatchStatus>"
                )),
                headers,
            ));
        }
        return committed_response(state, id, &found).await;
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    // A status read never consults the current source object, Kubo, or a pin API.
    let state_name = match found.state.as_str() {
        "pending" | "admitted" => "pending",
        "fenced" => "failed",
        _ => return Err(conflict()),
    };
    let xml = format!(
        "<ZipBatchStatus><BatchId>{id}</BatchId><State>{state_name}</State></ZipBatchStatus>"
    );
    Ok(S3Response::with_headers(Body::from(xml), headers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, store::entities::zip_batch};
    use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set};

    fn empty_zip() -> Bytes {
        let mut bytes = vec![0; 22];
        bytes[..4].copy_from_slice(b"PK\x05\x06");
        Bytes::from(bytes)
    }

    fn payload_headers(bytes: &[u8]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            http::HeaderValue::from_str(&hex::encode(Sha256::digest(bytes))).unwrap(),
        );
        headers
    }

    #[tokio::test]
    async fn input_digest_requires_clean_eof_even_after_a_complete_zip_trailer() {
        let bytes = empty_zip();
        let headers = payload_headers(&bytes);
        let body = Body::http_body_unsync(http_body_util::StreamBody::new(
            futures_util::stream::iter([
                Ok(hyper::body::Frame::data(bytes.clone())),
                Err(std::io::Error::other("late HTTP frame failure")),
            ]),
        ));
        let mut input = InputDigest::new(bytes.len() as u64);
        assert!(hash_body(body, &mut input).await.is_err());
        assert!(!input.eof);
        assert!(input.failed);
        assert!(input.finish(&headers).is_err());
        // Even incorrectly marking EOF cannot turn a failed stream clean.
        input.eof = true;
        assert!(input.finish(&headers).is_err());
    }

    #[tokio::test]
    async fn input_digest_accepts_exact_limit_only_after_the_final_http_frame() {
        let bytes = empty_zip();
        let headers = payload_headers(&bytes);
        let mut input = InputDigest::new(bytes.len() as u64);
        input.add(&bytes).unwrap();
        assert!(input.finish(&headers).is_err());
        let trailers = Body::http_body_unsync(http_body_util::StreamBody::new(
            futures_util::stream::iter([Ok::<_, std::io::Error>(
                hyper::body::Frame::<Bytes>::trailers(HeaderMap::new()),
            )]),
        ));
        hash_body(trailers, &mut input).await.unwrap();
        assert_eq!(input.finish(&headers).unwrap().1, bytes.len() as i64);
        assert!(input.add(b"x").is_err());
        assert_eq!(input.size, bytes.len() as i64);
        assert!(input.finish(&headers).is_err());
    }

    #[tokio::test]
    async fn legacy_status_headers_and_xml_use_the_authorized_snapshot_despite_later_root_change() {
        let state = AppState::new(&Config::default_for_test()).await.unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let row = zip::admit(
            state.store.db(),
            &zip::BatchAdmission {
                id: id.clone(),
                owner: "authorized-owner".into(),
                source: "direct".into(),
                token: "legacy-status-snapshot".into(),
                fingerprint: "legacy-input".into(),
                bucket: "test-bkt".into(),
                archive_key: "source.zip".into(),
                input_identity: "archive-cid".into(),
                captured_options: "{}".into(),
            },
        )
        .await
        .unwrap();
        let mut row = row.into_active_model();
        row.state = Set("published".into());
        row.terminal_result = Set(Some("{}".into()));
        row.root_status = Set("failed".into());
        row.root_error_code = Set(Some("root_unavailable".into()));
        row.update(state.store.db()).await.unwrap();
        let authorized = zip::snapshot(state.store.db(), &id)
            .await
            .unwrap()
            .filter(|snapshot| {
                snapshot.batch.owner == "authorized-owner"
                    && snapshot.batch.bucket == "test-bkt"
                    && snapshot.batch.archive_key == "source.zip"
            })
            .unwrap();
        // A root-only retry commits after authorization and before rendering.
        let mut changed = authorized.batch.clone().into_active_model();
        changed.root_status = Set("empty".into());
        changed.root_error_code = Set(None);
        changed.root_revision = Set(authorized.batch.root_revision + 1);
        changed.update(state.store.db()).await.unwrap();
        assert_eq!(
            zip_batch::Entity::find_by_id(&id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .root_status,
            "empty"
        );
        let response = legacy_status_response(&authorized, None);
        assert_eq!(response.headers["x-ipfs-s3-zip-root-status"], "failed");
        assert_eq!(
            response.headers["x-ipfs-s3-zip-root-warning"],
            "root_unavailable"
        );
        assert!(response.headers.get("x-ipfs-s3-zip-root-cid").is_none());
        let body = response.output.collect().await.unwrap().to_bytes();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<RootStatus>failed</RootStatus>"), "{xml}");
        assert!(
            xml.contains("<RootWarning>root_unavailable</RootWarning>"),
            "{xml}"
        );
        assert!(!xml.contains("<RootCID>"), "{xml}");
    }
}
