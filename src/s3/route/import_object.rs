mod request;

use std::sync::Arc;

use http::{HeaderMap, Method, StatusCode, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};
use sha2::Digest as _;

use self::request::{
    ParsedImportSource, V2ImportContract, collect_import_xml, encode_continuation_token,
    legacy_request_fingerprint, optional_single_header, parse_client_token, parse_import_xml,
    parse_metadata, parse_status_query, parse_submit_query, parse_tags, reject_sse_headers,
    request_fingerprint, request_zip_fingerprint, v2_import_contract,
    validate_submission_content_type, validate_v2_import_headers,
};
use crate::{
    error::AppError,
    import::{
        ImportSource,
        pipeline::ImportCoordinator,
        response::{StatusResultPage, ZipRootStatus, accepted_xml, status_xml_with_root},
    },
    pinning::{
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision},
        policy::PublicationContext,
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    state::AppState,
    store::entities::import_job,
    store::import::{
        jobs::{self, NewImportJob, SubmitImportOutcome},
        ownership, results,
    },
    store::zip::{execution, import_intake},
    zip::options::{ZipOptions, parse_authenticated_zip_controls},
};

pub struct ImportObjectRoute {
    state: Arc<AppState>,
    coordinator: Arc<ImportCoordinator>,
    root_default: bool,
}

impl ImportObjectRoute {
    pub fn new(state: Arc<AppState>, coordinator: Arc<ImportCoordinator>) -> Self {
        Self::with_root_default(state, coordinator, true)
    }

    pub fn with_root_default(
        state: Arc<AppState>,
        coordinator: Arc<ImportCoordinator>,
        root_default: bool,
    ) -> Self {
        Self {
            state,
            coordinator,
            root_default,
        }
    }

    pub(super) async fn call_authenticated(
        &self,
        req: S3Request<Body>,
    ) -> S3Result<S3Response<Body>> {
        if !self.coordinator.enabled() {
            return Err(AppError::ImportDisabled.into());
        }
        match req.method {
            Method::POST => self.submit(req).await,
            Method::GET => self.status(req).await,
            _ => Err(s3s::s3_error!(
                MethodNotAllowed,
                "unsupported import route method"
            )),
        }
    }

    async fn submit(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        crate::s3::ops::storage_class::require_standard_write_headers(&req.headers)?;
        let parsed_query = parse_submit_query(&req.uri)?;
        reject_sse_headers(&req.headers)?;
        validate_submission_content_type(&req.headers)?;
        let client_token = parse_client_token(&req.headers)?;
        let object_content_type =
            optional_single_header(&req.headers, "x-ipfs3-object-content-type")?;
        let tags = parse_tags(&req.headers, parsed_query.decompress_prefix.is_some())?;
        let controls = parse_authenticated_zip_controls(
            &req.headers,
            &crate::s3::query::decoded_query_pairs(&req.uri)?,
            &tags,
            parsed_query.decompress_prefix.is_some(),
            self.root_default,
        )?;
        let root_capture = controls.as_ref().map(|controls| controls.root);
        let metadata = parse_metadata(&req.headers);
        let zip_options = controls
            .as_ref()
            .map_or(ZipOptions::Legacy, |controls| controls.options.clone());
        // New requests have no client digest protocol. Historical jobs retain
        // their expected digest in the persisted intake and source decoder.
        let mut expected_sha256 = None;
        if let ZipOptions::V2(options) = &zip_options {
            validate_v2_import_headers(options, client_token.as_deref())?;
            if parsed_query.decompress_prefix.is_none() {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "unsupported ZIP v2 import publication mode"
                ));
            }
        }

        let body = collect_import_xml(&mut req.input).await?;
        let parsed_source = parse_import_xml(&body)?;
        let source = match parsed_source {
            ParsedImportSource::Cid(cid) => ImportSource::Cid(cid),
            ParsedImportSource::Url(url) => ImportSource::Url(url),
        };
        let principal = crate::s3::ops::object::principal_id(&req)?;
        if let ZipOptions::V2(mut options) = zip_options {
            let prefix = parsed_query
                .decompress_prefix
                .as_deref()
                .expect("v2 prefix validated");
            let saved = execution::read_by_identity(
                self.state.store.db(),
                &principal,
                "import",
                &options.token,
            )
            .await?;
            let id = saved
                .as_ref()
                .map_or_else(|| uuid::Uuid::new_v4().to_string(), |row| row.id.clone());
            let zip_intent = if let Some(saved) = &saved {
                let historical = crate::zip::options::decode_historical_zip_options(
                    &saved.captured_options,
                    None,
                )
                .map_err(|_| AppError::ImportIdempotencyConflict)?;
                let parsed = controls.as_ref().expect("v2 controls validated");
                let old = historical.options;
                if options.publish_source != old.publish_source
                    || options.publish_extracted != old.publish_extracted
                    || options.token != old.token
                    || options.root_override != old.root_override
                    || (parsed.targets_explicit && options.targets != old.targets)
                {
                    return Err(AppError::ImportIdempotencyConflict.into());
                }
                let value: serde_json::Value = serde_json::from_str(&saved.captured_options)
                    .map_err(|_| AppError::ImportIdempotencyConflict)?;
                if let Some(intent) = value.get("zip_intent") {
                    let intent = crate::pinning::zip_policy::CapturedZipIntent::decode_json(
                        &intent.to_string(),
                    )
                    .map_err(|_| AppError::ImportIdempotencyConflict)?;
                    if intent.origin().principal_id != principal
                        || intent.origin().request_id != id
                        || intent.bucket() != parsed_query.bucket
                        || intent.source_key() != parsed_query.key
                        || intent.target_prefix() != prefix
                        || intent.v2_options().as_ref() != Some(&old)
                        || intent.controls().targets_explicit != parsed.targets_explicit
                    {
                        return Err(AppError::ImportIdempotencyConflict.into());
                    }
                }
                let original: serde_json::Value = serde_json::from_str(&saved.request_contract)
                    .map_err(|_| AppError::ImportIdempotencyConflict)?;
                let array = original
                    .as_array()
                    .filter(|array| array.len() == 16 && array[0] == "import-zip-v2")
                    .ok_or(AppError::ImportIdempotencyConflict)?;
                expected_sha256 = serde_json::from_value(array[6].clone())
                    .map_err(|_| AppError::ImportIdempotencyConflict)?;
                options = old;
                None
            } else {
                let intent = capture_zip_intent(
                    self.state.pinning.effective_config(),
                    self.state.pinning.decompress_zip_config(),
                    self.state.pinning.control_mode(),
                    ZipCaptureContext {
                        bucket: &parsed_query.bucket,
                        source_key: &parsed_query.key,
                        target_prefix: prefix,
                        tags: &tags,
                    },
                    controls.as_ref().expect("v2 controls validated"),
                    DecisionOrigin::new(&principal, &id),
                )
                .map_err(|_| {
                    AppError::InvalidPinningRequest("invalid ZIP intent capture".into())
                })?;
                options = intent.v2_options().ok_or(AppError::InvalidImportRequest)?;
                Some(intent)
            };
            let (mut contract, source_descriptor) = v2_import_contract(&V2ImportContract {
                source: &source,
                principal: &principal,
                bucket: &parsed_query.bucket,
                key: &parsed_query.key,
                prefix,
                expected: expected_sha256.as_deref(),
                options: &options,
                object_content_type: object_content_type.as_deref(),
                metadata: &metadata,
                tags: &tags,
            })?;
            if let Some(saved) = &saved {
                if saved.owner != principal
                    || saved.source != "import"
                    || saved.token != options.token
                    || saved.bucket != parsed_query.bucket
                    || saved.source_key != parsed_query.key
                    || saved.request_fingerprint
                        != hex::encode(sha2::Sha256::digest(saved.request_contract.as_bytes()))
                    || serde_json::from_str::<serde_json::Value>(&contract).ok()
                        != serde_json::from_str::<serde_json::Value>(&saved.request_contract).ok()
                {
                    return Err(AppError::ImportIdempotencyConflict.into());
                }
                contract = saved.request_contract.clone();
            }
            let captured_options = saved.as_ref().map_or_else(
                || {
                    serde_json::json!({
                        "options": &options,
                        "target_prefix": prefix,
                        "rule_revision": zip_intent.as_ref().map(|intent| intent.policy_revision()),
                        "zip_intent": zip_intent,
                        "source_content_type": object_content_type,
                        "source_metadata": metadata,
                        "source_tags": tags,
                        "accepted_response": {
                            "version": 1,
                            "xml": accepted_xml(&id, "pending", "accepted"),
                        },
                    })
                    .to_string()
                },
                |saved| saved.captured_options.clone(),
            );
            let intake = import_intake::Request {
                admission: execution::Admission {
                    id,
                    owner: principal,
                    source: "import".to_owned(),
                    token: options.token.clone(),
                    request_fingerprint: hex::encode(sha2::Sha256::digest(contract.as_bytes())),
                    request_contract: contract,
                    bucket: parsed_query.bucket,
                    source_key: parsed_query.key,
                    captured_options,
                },
                prefix: prefix.to_owned(),
                source_descriptor,
                expected_sha256,
            };
            if let Some(status) = import_intake::preflight(self.state.store.db(), &intake).await? {
                return accepted_v2_response(&req.uri, &status);
            }
            if let ImportSource::Url(url) = &source {
                self.coordinator.authorize_url_for_submission(url).await?;
            }
            let status = import_intake::admit_store(&self.state.store, &intake).await?;
            return accepted_v2_response(&req.uri, &status);
        }
        if let Some(token) = client_token.as_deref()
            && import_intake::legacy_token_exists(
                self.state.store.db(),
                &parsed_query.bucket,
                &parsed_query.key,
                token,
            )
            .await?
        {
            return Err(AppError::ImportIdempotencyConflict.into());
        }
        let prior_fingerprint = request_fingerprint(
            &source,
            &principal,
            object_content_type.as_deref(),
            &metadata,
            &tags,
            parsed_query.decompress_prefix.as_deref(),
        )?;
        let request_fingerprint = match (parsed_query.decompress_prefix.as_deref(), root_capture) {
            (Some(prefix), Some(capture)) => request_zip_fingerprint(
                &source,
                &principal,
                object_content_type.as_deref(),
                &metadata,
                &tags,
                prefix,
                capture,
            )?,
            _ => prior_fingerprint.clone(),
        };

        if let Some(token) = client_token.as_deref() {
            match ownership::preflight_idempotent_submission(
                self.state.store.db(),
                &parsed_query.bucket,
                &parsed_query.key,
                token,
                &request_fingerprint,
            )
            .await
            {
                Ok(Some(existing)) => return accepted_response(&req.uri, &existing),
                Ok(None) => {}
                Err(AppError::ImportIdempotencyConflict) => {
                    if parsed_query.decompress_prefix.is_some() {
                        match ownership::preflight_idempotent_submission(
                            self.state.store.db(),
                            &parsed_query.bucket,
                            &parsed_query.key,
                            token,
                            &prior_fingerprint,
                        )
                        .await
                        {
                            Ok(Some(existing))
                                if existing.root_capture_json.is_none()
                                    && existing.zip_intent_json.is_none() =>
                            {
                                return accepted_response(&req.uri, &existing);
                            }
                            Ok(Some(_)) | Err(AppError::ImportIdempotencyConflict) => {}
                            Ok(None) => {}
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let legacy_fingerprint = legacy_request_fingerprint(
                        &source,
                        object_content_type.as_deref(),
                        &metadata,
                        &tags,
                        parsed_query.decompress_prefix.as_deref(),
                    )?;
                    match ownership::preflight_idempotent_submission(
                        self.state.store.db(),
                        &parsed_query.bucket,
                        &parsed_query.key,
                        token,
                        &legacy_fingerprint,
                    )
                    .await
                    {
                        Ok(Some(existing))
                            if existing.pin_decision_json.is_none()
                                && existing.zip_intent_json.is_none()
                                && existing.root_capture_json.is_none() =>
                        {
                            // Stage2 did not persist a principal. Preserve the existing
                            // authenticated gate and reject an explicitly different owner.
                            let bucket = crate::store::bucket::get(
                                self.state.store.db(),
                                &parsed_query.bucket,
                            )
                            .await?;
                            if bucket
                                .owner
                                .as_deref()
                                .is_some_and(|owner| owner != principal)
                            {
                                return Err(AppError::ImportIdempotencyConflict.into());
                            }
                            return accepted_response(&req.uri, &existing);
                        }
                        Ok(Some(_)) | Err(AppError::ImportIdempotencyConflict) => {
                            return Err(AppError::ImportIdempotencyConflict.into());
                        }
                        Ok(None) => {} // A retained token disappeared; use normal admission.
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }

        let job_id = uuid::Uuid::new_v4().to_string();
        let zip_intent = if let (Some(prefix), Some(controls)) =
            (parsed_query.decompress_prefix.as_deref(), controls.as_ref())
        {
            Some(
                capture_zip_intent(
                    self.state.pinning.effective_config(),
                    self.state.pinning.decompress_zip_config(),
                    self.state.pinning.control_mode(),
                    ZipCaptureContext {
                        bucket: &parsed_query.bucket,
                        source_key: &parsed_query.key,
                        target_prefix: prefix,
                        tags: &tags,
                    },
                    controls,
                    DecisionOrigin::new(&principal, &job_id),
                )
                .map_err(|_| {
                    AppError::InvalidPinningRequest("invalid ZIP intent capture".into())
                })?,
            )
        } else {
            None
        };
        let mut decision = if let Some(intent) = &zip_intent {
            intent.source_decision().clone()
        } else {
            self.state
                .pinning
                .policy()
                .evaluate_publication_decision(
                    PublicationContext {
                        bucket: &parsed_query.bucket,
                        key: &parsed_query.key,
                        tags: &tags,
                        is_decompress_zip: parsed_query.decompress_prefix.is_some(),
                    },
                    DecisionOrigin::new(&principal, &job_id),
                )
                .map_err(AppError::from)?
                .1
        };
        if zip_intent.is_none() {
            decision
                .capture_durable_revision(
                    self.state.pinning.effective_config(),
                    self.state.pinning.control_mode(),
                )
                .map_err(|error| AppError::InvalidPinningRequest(error.to_owned()))?;
        }
        if let ImportSource::Url(url) = &source {
            self.coordinator
                .authorize_url_for_submission(url)
                .await
                .map_err(s3s::S3Error::from)?;
        }
        let request = NewImportJob {
            id: job_id,
            bucket: parsed_query.bucket,
            key: parsed_query.key,
            source,
            request_fingerprint,
            client_token,
            object_content_type,
            metadata,
            tags,
            decompress_prefix: parsed_query.decompress_prefix,
        };
        let submitted = ownership::submit_store(
            &self.state.store,
            request,
            Some(decision),
            zip_intent,
            chrono::Utc::now(),
        )
        .await;
        let job = match submitted.map_err(import_intake::map_token_conflict)? {
            SubmitImportOutcome::Created(job) | SubmitImportOutcome::Replayed(job) => job,
        };

        accepted_response(&req.uri, &job)
    }

    async fn status(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let parsed = parse_status_query(&req.uri)?;
        let principal = crate::s3::ops::object::principal_id(&req)?;
        if let Some(status) = import_intake::read_for_path(
            self.state.store.db(),
            &parsed.job_id,
            &principal,
            &parsed.bucket,
            &parsed.key,
        )
        .await?
        {
            if parsed.continuation_sequence.is_some() {
                return Err(AppError::InvalidImportRequest.into());
            }
            return status_v2_response(&status);
        }
        let job = jobs::get_for_path(
            self.state.store.db(),
            &parsed.job_id,
            &parsed.bucket,
            &parsed.key,
        )
        .await?
        .ok_or(AppError::NoSuchImportJob)?;

        let result_page = if job.state == "completed" && job.decompress_prefix.is_some() {
            Some(
                results::page(
                    self.state.store.db(),
                    &job.id,
                    parsed.continuation_sequence,
                    parsed.max_results,
                )
                .await?,
            )
        } else {
            None
        };
        let next_token = result_page
            .as_ref()
            .and_then(|page| page.next_sequence)
            .map(|sequence| encode_continuation_token(&job.id, sequence));
        let snapshot = if job.state == "completed"
            && (job.root_capture_json.is_some() || job.zip_intent_json.is_some())
        {
            crate::store::zip::snapshot(self.state.store.db(), &job.id).await?
        } else {
            None
        };
        let root = snapshot
            .as_ref()
            .filter(|snapshot| {
                snapshot.batch.state == "published"
                    && snapshot.batch.source == "import"
                    && snapshot.batch.token == job.id
                    && snapshot.batch.bucket == job.bucket
                    && snapshot.batch.archive_key == job.key
            })
            .map(|snapshot| {
                let batch = &snapshot.batch;
                let verified_cid = batch.root_cid.as_deref().filter(|cid| {
                    matches!(batch.root_status.as_str(), "complete" | "partial")
                        && snapshot.references.iter().any(|reference| {
                            reference.state == "adopted"
                                && reference.verification_receipt.is_some()
                                && reference.cid == *cid
                                && reference.revision == batch.root_revision
                                && reference.epoch == batch.root_epoch
                                && snapshot.builds.iter().any(|build| {
                                    build.revision == reference.revision
                                        && build.epoch == reference.epoch
                                        && build.status == "verified"
                                })
                        })
                });
                ZipRootStatus {
                    status: if matches!(batch.root_status.as_str(), "complete" | "partial")
                        && verified_cid.is_none()
                    {
                        "failed"
                    } else {
                        &batch.root_status
                    },
                    cid: verified_cid,
                    error_code: batch.root_error_code.as_deref().filter(|code| {
                        code.bytes()
                            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
                    }),
                }
            });
        let xml = status_xml_with_root(
            &job,
            result_page.as_ref().map(|page| StatusResultPage {
                rows: &page.rows,
                next_continuation_token: next_token.as_deref(),
            }),
            root,
        );
        let mut response = S3Response::new(Body::from(xml));
        if let Some(decision) = job
            .decision()
            .map_err(|_| AppError::Internal("invalid persisted import pin decision".to_owned()))?
        {
            pin_decision_headers(&mut response.headers, &decision, true);
        }
        response.headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        Ok(response)
    }
}

fn accepted_v2_response(uri: &Uri, status: &import_intake::Status) -> S3Result<S3Response<Body>> {
    let mut response = S3Response::with_status(
        Body::from(status.accepted_xml.clone()),
        StatusCode::ACCEPTED,
    );
    response.headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    response.headers.insert(
        http::header::LOCATION,
        http::HeaderValue::from_str(&format!("{}?ipfs3-import={}", uri.path(), status.id))
            .map_err(|_| AppError::InvalidImportRequest)?,
    );
    response.headers.insert(
        "x-ipfs3-import-job-id",
        http::HeaderValue::from_str(&status.id).map_err(|_| AppError::InvalidImportRequest)?,
    );
    Ok(response)
}

fn status_v2_response(status: &import_intake::Status) -> S3Result<S3Response<Body>> {
    // Digest fields are a signed promise or independently measured byte stream.
    // Only the sealed source receipt below is a published object identity.
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><IPFS3ImportStatus><JobId>{}</JobId><State>{}</State><Phase>{}</Phase>",
        status.id,
        status.state,
        if status.published_source.is_some() {
            "published"
        } else {
            "source-free"
        },
    );
    if let Some(expected) = &status.expected_sha256 {
        xml.push_str(&format!("<ExpectedSHA256>{expected}</ExpectedSHA256>"));
    }
    xml.push_str(&format!(
        "<MeasuredSHA256>{}</MeasuredSHA256>",
        status.measured_sha256.as_deref().unwrap_or("unknown")
    ));
    if let Some(source) = &status.published_source {
        xml.push_str(&format!(
            "<ETag>{}</ETag><Size>{}</Size>",
            quick_xml::escape::escape(&source.cid),
            source.size,
        ));
        if let Some(version) = &source.version_id {
            xml.push_str(&format!(
                "<VersionId>{}</VersionId>",
                quick_xml::escape::escape(version),
            ));
        }
    }
    xml.push_str("</IPFS3ImportStatus>");
    let mut response = S3Response::new(Body::from(xml));
    response.headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    Ok(response)
}

fn accepted_response(uri: &Uri, job: &import_job::Model) -> S3Result<S3Response<Body>> {
    let xml = accepted_xml(&job.id, &job.state, &job.phase);
    let mut response = S3Response::with_status(Body::from(xml), StatusCode::ACCEPTED);
    if let Some(decision) = stored_decision(job)? {
        // 202 acknowledges durable admission, not completion of any remote pin.
        pin_decision_headers(&mut response.headers, &decision, false);
    }
    response.headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/xml"),
    );
    response.headers.insert(
        http::header::LOCATION,
        http::HeaderValue::from_str(&format!("{}?ipfs3-import={}", uri.path(), job.id))
            .map_err(|_| s3s::s3_error!(InternalError, "invalid import status location"))?,
    );
    response.headers.insert(
        "x-ipfs3-import-job-id",
        http::HeaderValue::from_str(&job.id)
            .map_err(|_| s3s::s3_error!(InternalError, "invalid import job identifier"))?,
    );
    Ok(response)
}

fn stored_decision(job: &import_job::Model) -> S3Result<Option<ExtensionDecision>> {
    crate::store::import::jobs::decision_from_job(job)
        .map_err(|_| AppError::Internal("invalid persisted import pin decision".to_owned()).into())
}

fn pin_decision_headers(headers: &mut HeaderMap, decision: &ExtensionDecision, status: bool) {
    if status {
        let effect = match decision.effect {
            DecisionEffect::Accepted => "accepted",
            DecisionEffect::Skipped => "skipped",
            DecisionEffect::NoIntent => "no_intent",
        };
        headers.insert(
            "x-ipfs3-pin-decision",
            http::HeaderValue::from_static(effect),
        );
    }
    headers.extend(crate::s3::ops::object::pin_warning_headers(
        decision.warning,
    ));
}

#[async_trait::async_trait]
impl S3Route for ImportObjectRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut http::Extensions,
    ) -> bool {
        is_import_request(method, uri)
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        self.check_access(&mut req).await?;
        self.call_authenticated(req).await
    }
}

pub(crate) fn is_import_request(method: &Method, uri: &Uri) -> bool {
    matches!(*method, Method::POST | Method::GET)
        && crate::s3::query::query_key_is_present(uri, "ipfs3-import")
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod durable_admission_tests {
    use super::*;
    use crate::config::{OptionalPinControlMode, PinningConfig, PolicyConfig, ProviderConfig};
    use crate::pinning::config::ValidatedPinningConfig;
    use crate::store::{
        Store,
        entities::{import_job, standard_mutation_lease},
    };
    use sea_orm::{Database, EntityTrait, PaginatorTrait};
    use std::collections::HashMap;

    #[tokio::test]
    async fn legacy_remote_intent_fails_before_import_job_guard_or_kubo() {
        let kubo = wiremock::MockServer::start().await;
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        let config = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                providers: vec![ProviderConfig {
                    cluster: None,
                    name: "remote".into(),
                    kind: "pinata".into(),
                    token_env: Some("PINNING_TOKEN".into()),
                    endpoint: None,
                    api: None,
                    strategy: None,
                    upload_endpoint: None,
                    enabled: true,
                    priority: 1,
                    max_bytes: 1024,
                    max_pins: 20,
                    requests_per_second: None,
                }],
                policies: vec![PolicyConfig {
                    bucket: "bucket".into(),
                    prefix: "".into(),
                    trigger: "always".into(),
                    provider_mode: "one".into(),
                    providers: vec!["remote".into()],
                    default_duration: "1h".into(),
                    max_duration: "24h".into(),
                    allow_decompressed: false,
                }],
                ..Default::default()
            },
            |_| Some("secret-never-in-error".into()),
        )
        .unwrap();
        let pinning = crate::pinning::coordinator::PinningCoordinator::build_with_kubo_and_mode(
            config,
            None,
            OptionalPinControlMode::Warn,
        )
        .unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning,
        });
        let imports = crate::import::ImportConfig {
            enabled: true,
            ..Default::default()
        };
        let imports = imports.validate().unwrap();
        let downloader =
            crate::import::downloader::SourceDownloader::production(Arc::new(imports.clone()));
        let route =
            ImportObjectRoute::new(state.clone(), ImportCoordinator::new(imports, downloader));
        let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
        let mut req = S3Request {
            input: Body::from(format!(
                "<IPFS3ImportRequest><CID>{cid}</CID></IPFS3ImportRequest>"
            )),
            method: Method::POST,
            uri: "/bucket/key?ipfs3-import".parse().unwrap(),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test".into(),
                secret_key: s3s::auth::SecretKey::from("test"),
            }),
            region: Some("us-east-1".parse().unwrap()),
            service: Some("s3".into()),
            trailing_headers: None,
        };
        req.headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        let error = route.call(req).await.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert!(!error.to_string().contains("secret-never-in-error"));
        assert_eq!(
            import_job::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert!(kubo.received_requests().await.unwrap().is_empty());
    }
}
