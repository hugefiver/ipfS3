mod request;

use std::sync::Arc;

use http::{HeaderMap, Method, StatusCode, Uri};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use self::request::{
    ParsedImportSource, collect_import_xml, encode_continuation_token, optional_single_header,
    parse_client_token, parse_import_xml, parse_metadata, parse_status_query, parse_submit_query,
    parse_tags, reject_sse_headers, request_fingerprint, validate_submission_content_type,
};
use crate::{
    error::AppError,
    import::{
        ImportSource,
        pipeline::ImportCoordinator,
        response::{StatusResultPage, accepted_xml, status_xml},
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
        let request_fingerprint = request_fingerprint(
            &source,
            object_content_type.as_deref(),
            &metadata,
            &tags,
            parsed_query.decompress_prefix.as_deref(),
        )?;

        if let Some(token) = client_token.as_deref()
            && let Some(existing) = ownership::preflight_idempotent_submission(
                self.state.store.db(),
                &parsed_query.bucket,
                &parsed_query.key,
                token,
                &request_fingerprint,
            )
            .await?
        {
            return accepted_response(&req.uri, &existing);
        }

        if let ImportSource::Url(url) = &source {
            self.coordinator
                .authorize_url_for_submission(url)
                .await
                .map_err(s3s::S3Error::from)?;
        }

        let request = NewImportJob {
            id: uuid::Uuid::new_v4().to_string(),
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
        let job =
            match ownership::submit(self.state.store.db(), request, chrono::Utc::now()).await? {
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
