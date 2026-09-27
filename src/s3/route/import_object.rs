mod request;

use std::sync::Arc;

use http::{HeaderMap, Method, StatusCode, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use self::request::{
    ParsedImportSource, collect_import_xml, encode_continuation_token, legacy_request_fingerprint,
    optional_single_header, parse_client_token, parse_import_xml, parse_metadata,
    parse_status_query, parse_submit_query, parse_tags, reject_sse_headers, request_fingerprint,
    validate_submission_content_type,
};
use crate::{
    error::AppError,
    import::{
        ImportSource,
        pipeline::ImportCoordinator,
        response::{StatusResultPage, accepted_xml, status_xml},
    },
    pinning::{
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision},
        policy::PublicationContext,
    },
    state::AppState,
    store::entities::import_job,
    store::import::{
        jobs::{self, NewImportJob, SubmitImportOutcome},
        ownership, results,
    },
};

pub struct ImportObjectRoute {
    state: Arc<AppState>,
    coordinator: Arc<ImportCoordinator>,
}

impl ImportObjectRoute {
    pub fn new(state: Arc<AppState>, coordinator: Arc<ImportCoordinator>) -> Self {
        Self { state, coordinator }
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
        let tags = parse_tags(&req.headers)?;
        let metadata = parse_metadata(&req.headers);

        let body = collect_import_xml(&mut req.input).await?;
        let parsed_source = parse_import_xml(&body)?;
        let source = match parsed_source {
            ParsedImportSource::Cid(cid) => ImportSource::Cid(cid),
            ParsedImportSource::Url(url) => ImportSource::Url(url),
        };
        let principal = crate::s3::ops::object::principal_id(&req)?;
        let request_fingerprint = request_fingerprint(
            &source,
            &principal,
            object_content_type.as_deref(),
            &metadata,
            &tags,
            parsed_query.decompress_prefix.as_deref(),
        )?;

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
                        Ok(Some(existing)) if existing.pin_decision_json.is_none() => {
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
        let (_, mut decision) = self
            .state
            .pinning
            .policy()
            .evaluate_publication_decision(
                PublicationContext {
                    bucket: &parsed_query.bucket,
                    key: &parsed_query.key,
                    tags: &tags,
                    is_decompress_zip: parsed_query.decompress_prefix.is_some(),
                },
                DecisionOrigin::new(principal, &job_id),
            )
            .map_err(AppError::from)?;
        decision
            .capture_durable_revision(
                self.state.pinning.effective_config(),
                self.state.pinning.control_mode(),
            )
            .map_err(|error| AppError::InvalidPinningRequest(error.to_owned()))?;
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
        let job = match ownership::submit_decided(
            self.state.store.db(),
            request,
            decision,
            chrono::Utc::now(),
        )
        .await?
        {
            SubmitImportOutcome::Created(job) | SubmitImportOutcome::Replayed(job) => job,
        };

        accepted_response(&req.uri, &job)
    }

    async fn status(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let parsed = parse_status_query(&req.uri)?;
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
        let xml = status_xml(
            &job,
            result_page.as_ref().map(|page| StatusResultPage {
                rows: &page.rows,
                next_continuation_token: next_token.as_deref(),
            }),
        );
        let mut response = S3Response::new(Body::from(xml));
        if let Some(decision) = stored_decision(&job)? {
            pin_decision_headers(&mut response.headers, &decision, true);
        }
        response.headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/xml"),
        );
        Ok(response)
    }
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
    job.pin_decision_json
        .as_deref()
        .map(|json| {
            let decision: ExtensionDecision = serde_json::from_str(json).map_err(|_| {
                AppError::Internal("invalid persisted import pin decision".to_owned())
            })?;
            decision.validate_snapshot().map_err(|_| {
                AppError::Internal("invalid persisted import pin decision".to_owned())
            })?;
            if decision.origin.request_id != job.id {
                return Err(
                    AppError::Internal("invalid persisted import pin decision".to_owned()).into(),
                );
            }
            Ok(decision)
        })
        .transpose()
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
