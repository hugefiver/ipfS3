use std::{collections::BTreeMap, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::TryStreamExt;
use reqwest::{Body as ReqwestBody, Client, RequestBuilder, Response, StatusCode, Url, multipart};
use serde::{Deserialize, Serialize};

use crate::{
    kubo::KuboClient,
    pinning::{
        config::{PinataApi, PinataProviderOptions, PinataStrategy, SecretToken},
        provider::{
            FindPin, PinningProvider, ProviderError, ProviderErrorClass, RemotePin,
            RemotePinStatus, SubmitPin,
        },
        psa::PsaClient,
    },
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FAILURE_REASON_CHARS: usize = 256;
const MAX_SUCCESS_RESPONSE_BYTES: usize = 1024 * 1024;
const V3_REQUEST_ID_PREFIX: &str = "pinata-v3:";
const LEGACY_REQUEST_ID_PREFIX: &str = "pinata-legacy:";

#[cfg(test)]
#[path = "pinata_stage1_tests.rs"]
mod stage1_tests;

pub const PINATA_BASE_URL: &str = "https://api.pinata.cloud/v3";
pub const PINATA_UPLOAD_BASE_URL: &str = "https://uploads.pinata.cloud/v3";
pub const PINATA_LEGACY_BASE_URL: &str = "https://api.pinata.cloud";

pub fn build_pinata(name: String, token: SecretToken, endpoint: Option<String>) -> PinataClient {
    build_pinata_with_options(
        name,
        token,
        endpoint,
        PinataProviderOptions {
            api: PinataApi::V3,
            strategy: PinataStrategy::Cid,
            upload_endpoint: None,
        },
        None,
    )
}

pub fn build_pinata_with_options(
    name: String,
    token: SecretToken,
    endpoint: Option<String>,
    options: PinataProviderOptions,
    kubo: Option<KuboClient>,
) -> PinataClient {
    let endpoint = endpoint.unwrap_or_else(|| match options.api {
        PinataApi::V3 => PINATA_BASE_URL.to_owned(),
        PinataApi::Legacy => PINATA_LEGACY_BASE_URL.to_owned(),
    });
    PinataClient::new(name, endpoint, options, kubo, token)
}

#[derive(Clone)]
pub struct PinataClient {
    name: String,
    v3_base_url: Result<Url, ()>,
    legacy_base_url: Result<Url, ()>,
    upload_base_url: Result<Url, ()>,
    token: SecretToken,
    http: Client,
    upload_http: Client,
    upload_idle: Duration,
    api: PinataApi,
    strategy: PinataStrategy,
    kubo: Option<KuboClient>,
    legacy_psa: Option<PsaClient>,
    legacy_request_psa: Option<PsaClient>,
}

impl PinataClient {
    fn historical(&self, api: &str, strategy: &str) -> Result<Self, ProviderError> {
        let mut historical = self.clone();
        if api == "psa" && strategy == "cid" {
            historical.legacy_psa = self
                .legacy_psa
                .clone()
                .or_else(|| self.legacy_request_psa.clone());
            if historical.legacy_psa.is_none() {
                return Err(protocol_error("historical provider route unavailable"));
            }
            return Ok(historical);
        }
        historical.legacy_psa = None;
        historical.api = match api {
            "pinata_v3" => PinataApi::V3,
            "pinata_legacy" => PinataApi::Legacy,
            _ => return Err(protocol_error("historical provider route unavailable")),
        };
        historical.strategy = match strategy {
            "cid" => PinataStrategy::Cid,
            "upload" => PinataStrategy::Upload,
            _ => return Err(protocol_error("historical provider route unavailable")),
        };
        Ok(historical)
    }
    pub fn new(
        name: String,
        endpoint: String,
        options: PinataProviderOptions,
        kubo: Option<KuboClient>,
        token: SecretToken,
    ) -> Self {
        let http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("default Pinata HTTP client must build");
        // Streaming uploads must not inherit a whole-request deadline; large objects
        // legitimately outlive it, so only the connect phase is bounded.
        let upload_http = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("Pinata upload HTTP client must build");
        Self::with_http(name, endpoint, options, kubo, token, http, upload_http)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[allow(clippy::too_many_arguments)]
    fn with_http(
        name: String,
        endpoint: String,
        options: PinataProviderOptions,
        kubo: Option<KuboClient>,
        token: SecretToken,
        http: Client,
        upload_http: Client,
    ) -> Self {
        let normalized = normalize_base(endpoint.clone());
        let upload_endpoint = options
            .upload_endpoint
            .clone()
            .unwrap_or_else(|| PINATA_UPLOAD_BASE_URL.to_owned());
        let v3_endpoint = match options.api {
            PinataApi::V3 => normalized.clone(),
            PinataApi::Legacy => pinata_v3_endpoint(&endpoint),
        };
        let legacy_endpoint = match options.api {
            PinataApi::V3 => pinata_legacy_endpoint(&endpoint),
            PinataApi::Legacy => normalized.clone(),
        };
        let legacy_psa = normalized
            .ends_with("/psa")
            .then(|| PsaClient::new(name.clone(), normalized, token.clone()));
        let legacy_request_psa = legacy_psa.is_none().then(|| {
            PsaClient::new(
                name.clone(),
                pinata_legacy_psa_endpoint(&endpoint),
                token.clone(),
            )
        });
        Self {
            name,
            v3_base_url: parse_base_url(&v3_endpoint),
            legacy_base_url: parse_base_url(&legacy_endpoint),
            upload_base_url: parse_base_url(&upload_endpoint),
            token,
            http,
            upload_http,
            upload_idle: DEFAULT_TIMEOUT,
            api: options.api,
            strategy: options.strategy,
            kubo,
            legacy_psa,
            legacy_request_psa,
        }
    }

    #[cfg(test)]
    pub(crate) fn base_url(&self) -> &str {
        match self.api {
            PinataApi::V3 => self
                .v3_base_url
                .as_ref()
                .map(Url::as_str)
                .unwrap_or_default(),
            PinataApi::Legacy => self
                .legacy_base_url
                .as_ref()
                .map(Url::as_str)
                .unwrap_or_default(),
        }
    }

    fn validated_v3_base_url(&self) -> Result<&Url, ProviderError> {
        self.v3_base_url
            .as_ref()
            .map_err(|_| protocol_error("provider endpoint is invalid"))
    }

    fn validated_legacy_base_url(&self) -> Result<&Url, ProviderError> {
        self.legacy_base_url
            .as_ref()
            .map_err(|_| protocol_error("provider endpoint is invalid"))
    }

    fn validated_upload_base_url(&self) -> Result<&Url, ProviderError> {
        self.upload_base_url
            .as_ref()
            .map_err(|_| protocol_error("provider upload endpoint is invalid"))
    }

    fn pin_by_cid_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_v3_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["files", "public", "pin_by_cid"]);
        Ok(url)
    }

    fn pin_by_cid_request_url(&self, provider_id: &str) -> Result<Url, ProviderError> {
        self.validate_provider_field(provider_id)?;
        let mut url = self.pin_by_cid_url()?;
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .push(provider_id);
        Ok(url)
    }

    fn files_url(&self, cid: Option<&str>, page_token: Option<&str>) -> Result<Url, ProviderError> {
        let mut url = self.validated_v3_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["files", "public"]);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", "100");
            if let Some(cid) = cid {
                query.append_pair("cid", cid);
            }
            if let Some(page_token) = page_token {
                query.append_pair("pageToken", page_token);
            }
        }
        Ok(url)
    }

    fn queue_url(&self, cid: Option<&str>, page_token: Option<&str>) -> Result<Url, ProviderError> {
        let mut url = self.pin_by_cid_url()?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", "100");
            if let Some(cid) = cid {
                query.append_pair("cid", cid);
            }
            if let Some(page_token) = page_token {
                query.append_pair("pageToken", page_token);
            }
        }
        Ok(url)
    }

    fn file_url(&self, provider_id: &str) -> Result<Url, ProviderError> {
        self.validate_provider_field(provider_id)?;
        let mut url = self.validated_v3_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["files", "public", provider_id]);
        Ok(url)
    }

    fn upload_files_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_upload_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider upload endpoint is invalid"))?
            .push("files");
        Ok(url)
    }

    fn legacy_pin_by_hash_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_legacy_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "pinByHash"]);
        Ok(url)
    }

    fn legacy_pin_file_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_legacy_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "pinFileToIPFS"]);
        Ok(url)
    }

    fn legacy_unpin_url(&self, cid: &str) -> Result<Url, ProviderError> {
        self.validate_provider_field(cid)?;
        let mut url = self.validated_legacy_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "unpin", cid]);
        Ok(url)
    }

    fn legacy_pin_list_url(&self, cid: &str) -> Result<Url, ProviderError> {
        let mut url = self.validated_legacy_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["data", "pinList"]);
        url.query_pairs_mut()
            .append_pair("hashContains", cid)
            .append_pair("pageLimit", "100")
            .append_pair("status", "pinned");
        Ok(url)
    }

    fn legacy_pin_jobs_url(&self, cid: &str, offset: usize) -> Result<Url, ProviderError> {
        let mut url = self.validated_legacy_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "pinJobs"]);
        url.query_pairs_mut()
            .append_pair("ipfs_pin_hash", cid)
            .append_pair("limit", "100")
            .append_pair("offset", &offset.to_string());
        Ok(url)
    }

    async fn execute(
        &self,
        request: RequestBuilder,
        operation: Operation,
    ) -> Result<Response, ProviderError> {
        request
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(|error| self.transport_error(operation, &error))
    }

    async fn execute_upload(
        &self,
        url: Url,
        form: multipart::Form,
    ) -> Result<Response, ProviderError> {
        use std::sync::{Arc, Mutex};
        let content_type = format!("multipart/form-data; boundary={}", form.boundary());
        let progress = Arc::new(Mutex::new(tokio::time::Instant::now()));
        let sending = progress.clone();
        // Observe the whole multipart body as the HTTP transport polls it, not
        // merely a source reader. Backpressure/stalled socket writes stop polls.
        let body = form.into_stream().inspect_ok(move |chunk| {
            if !chunk.is_empty() {
                *sending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    tokio::time::Instant::now();
            }
        });
        let request = self
            .upload_http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(ReqwestBody::wrap_stream(body));
        let send = self.execute(request, Operation::Submit);
        tokio::pin!(send);
        loop {
            let deadline = *progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                + self.upload_idle;
            tokio::select! {
                result = &mut send => return result,
                _ = tokio::time::sleep_until(deadline) => {
                    let last = *progress.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if last.elapsed() >= self.upload_idle { return Err(upload_idle_error()); }
                }
            }
        }
    }

    async fn require_success(
        &self,
        response: Response,
        operation: Operation,
    ) -> Result<Response, ProviderError> {
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let class = match status.as_u16() {
            401 => ProviderErrorClass::Authentication,
            403 => super::provider::classify_forbidden_response(response, self.upload_idle).await,
            404 if operation == Operation::Get => ProviderErrorClass::NotFound,
            // Intentional divergence from the PSA baseline, which maps only 507 to
            // Quota: Pinata reports free-tier plan limits as 402.
            402 | 507 => ProviderErrorClass::Quota,
            409 if operation == Operation::Submit => ProviderErrorClass::Ambiguous,
            429 => ProviderErrorClass::RateLimited,
            _ if status.is_server_error() => ProviderErrorClass::Transient,
            _ if status.is_client_error() => ProviderErrorClass::InvalidInput,
            _ => ProviderErrorClass::Protocol,
        };
        Err(ProviderError {
            class,
            message: format!("provider returned HTTP status {}", status.as_u16()),
            retry_after: (class == ProviderErrorClass::RateLimited)
                .then_some(retry_after)
                .flatten(),
        })
    }

    async fn decode<T: for<'de> Deserialize<'de>>(
        &self,
        response: Response,
        operation: Operation,
    ) -> Result<T, ProviderError> {
        let body = self.read_success_body(response, operation).await?;
        serde_json::from_slice(&body)
            .map_err(|_| protocol_error("provider returned an invalid success response"))
    }

    async fn read_success_body(
        &self,
        mut response: Response,
        operation: Operation,
    ) -> Result<Vec<u8>, ProviderError> {
        if response
            .content_length()
            .is_some_and(|length| length > MAX_SUCCESS_RESPONSE_BYTES as u64)
        {
            return Err(protocol_error(
                "provider success response exceeded size limit",
            ));
        }

        let mut body = Vec::new();
        while let Some(chunk) = tokio::time::timeout(self.upload_idle, response.chunk())
            .await
            .map_err(|_| upload_idle_error())?
            .map_err(|error| self.transport_error(operation, &error))?
        {
            let body_length = body
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| protocol_error("provider success response exceeded size limit"))?;
            if body_length > MAX_SUCCESS_RESPONSE_BYTES {
                return Err(protocol_error(
                    "provider success response exceeded size limit",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    async fn list_files(&self, cid: Option<&str>) -> Result<Vec<PinataFile>, ProviderError> {
        let mut page_token = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut expected_count = None;
        let mut files = Vec::new();
        for _ in 0..10 {
            let response = self
                .execute(
                    self.http.get(self.files_url(cid, page_token.as_deref())?),
                    Operation::Find,
                )
                .await?;
            let response = self.require_success(response, Operation::Find).await?;
            let page: PinataData<PinataFilesPage> = self.decode(response, Operation::Find).await?;
            check_page_count(&mut expected_count, page.data.count)?;
            files.extend(page.data.files);
            page_token = page.data.next_page_token.filter(|token| !token.is_empty());
            if page_token
                .as_ref()
                .is_some_and(|token| !seen.insert(token.clone()))
            {
                return Err(protocol_error("provider returned a pagination cycle"));
            }
            if page_token.is_none() {
                if expected_count.is_some_and(|count| count != files.len()) {
                    return Err(protocol_error("provider returned an incomplete file list"));
                }
                return Ok(files);
            }
        }
        Err(protocol_error("provider returned an incomplete file list"))
    }

    async fn list_queue(&self, cid: Option<&str>) -> Result<Vec<PinataJob>, ProviderError> {
        let mut page_token = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut expected_count = None;
        let mut jobs = Vec::new();
        for _ in 0..10 {
            let response = self
                .execute(
                    self.http.get(self.queue_url(cid, page_token.as_deref())?),
                    Operation::Find,
                )
                .await?;
            let response = self.require_success(response, Operation::Find).await?;
            let page: PinataData<PinataQueuePage> = self.decode(response, Operation::Find).await?;
            check_page_count(&mut expected_count, page.data.count)?;
            jobs.extend(page.data.jobs);
            page_token = page.data.next_page_token.filter(|token| !token.is_empty());
            if page_token
                .as_ref()
                .is_some_and(|token| !seen.insert(token.clone()))
            {
                return Err(protocol_error("provider returned a pagination cycle"));
            }
            if page_token.is_none() {
                if expected_count.is_some_and(|count| count != jobs.len()) {
                    return Err(protocol_error("provider returned an incomplete pin queue"));
                }
                return Ok(jobs);
            }
        }
        Err(protocol_error("provider returned an incomplete pin queue"))
    }

    async fn list_legacy_pins(&self, cid: &str) -> Result<Vec<LegacyPinRow>, ProviderError> {
        let response = self
            .execute(
                self.http.get(self.legacy_pin_list_url(cid)?),
                Operation::Find,
            )
            .await?;
        let response = self.require_success(response, Operation::Find).await?;
        let list: LegacyPinList = self.decode(response, Operation::Find).await?;
        if list.rows.len() >= 100 || list.count.is_some_and(|count| count != list.rows.len()) {
            return Err(protocol_error(
                "provider returned an incomplete legacy file list",
            ));
        }
        Ok(list.rows)
    }

    async fn list_legacy_jobs(&self, cid: &str) -> Result<Vec<LegacyPinJob>, ProviderError> {
        let mut jobs = Vec::new();
        for page in 0..10 {
            let response = self
                .execute(
                    self.http.get(self.legacy_pin_jobs_url(cid, page * 100)?),
                    Operation::Find,
                )
                .await?;
            let response = self.require_success(response, Operation::Find).await?;
            let list: LegacyPinJobs = self.decode(response, Operation::Find).await?;
            let count = list.count;
            let page_len = list.rows.len();
            jobs.extend(list.rows);
            if jobs.len() == count {
                return Ok(jobs);
            }
            if jobs.len() > count || page_len < 100 {
                return Err(protocol_error("provider returned an incomplete pin queue"));
            }
        }
        Err(protocol_error("provider returned an incomplete pin queue"))
    }

    async fn submit_v3_cid(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let body = PinataPinByCidRequest {
            cid: &request.cid,
            name: &request.name,
            keyvalues: &request.metadata,
        };
        let response = self
            .execute(
                self.http.post(self.pin_by_cid_url()?).json(&body),
                Operation::Submit,
            )
            .await?;
        let response = self.require_success(response, Operation::Submit).await?;
        let response: PinataData<PinataJob> = self.decode(response, Operation::Submit).await?;
        let remote = self.remote_from_job_with_metadata(response.data, &request.metadata)?;
        if remote.cid != request.cid {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        Ok(remote)
    }

    async fn submit_legacy_cid(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let body = LegacyPinByHashRequest {
            hash_to_pin: &request.cid,
            pinata_metadata: LegacyPinMetadata {
                name: &request.name,
                keyvalues: &request.metadata,
            },
        };
        let response = self
            .execute(
                self.http.post(self.legacy_pin_by_hash_url()?).json(&body),
                Operation::Submit,
            )
            .await?;
        let response = self.require_success(response, Operation::Submit).await?;
        let pinned: LegacyPinResponse = self.decode(response, Operation::Submit).await?;
        self.remote_from_legacy_pin_response(pinned, &request.cid, &request.metadata)
    }

    async fn submit_upload(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        match self.api {
            PinataApi::V3 => self.submit_v3_upload(request).await,
            PinataApi::Legacy => self.submit_legacy_upload(request).await,
        }
    }

    async fn submit_v3_upload(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let part = self.content_part(&request).await?;
        let keyvalues = serde_json::to_string(&request.metadata)
            .map_err(|_| protocol_error("pin metadata could not be encoded"))?;
        let form = multipart::Form::new()
            .part("file", part)
            .text("network", "public".to_owned())
            .text("name", request.name.clone())
            .text("keyvalues", keyvalues);
        let response = self.execute_upload(self.upload_files_url()?, form).await?;
        let response = self.require_success(response, Operation::Submit).await?;
        let uploaded: PinataData<PinataFileUpload> =
            self.decode(response, Operation::Submit).await?;
        let uploaded = uploaded.data;
        self.validate_provider_field(&uploaded.id)?;
        self.validate_provider_field(&uploaded.cid)?;
        if uploaded.cid != request.cid {
            self.cleanup_uploaded_file(&uploaded.id).await;
            return Err(upload_correlation_error());
        }
        self.remote_from_v3_upload(uploaded, &request.metadata)
    }

    async fn submit_legacy_upload(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let part = self.content_part(&request).await?;
        let metadata = serde_json::to_string(&LegacyPinMetadata {
            name: &request.name,
            keyvalues: &request.metadata,
        })
        .map_err(|_| protocol_error("pin metadata could not be encoded"))?;
        let options = serde_json::json!({ "cidVersion": 1 }).to_string();
        let form = multipart::Form::new()
            .part("file", part)
            .text("pinataMetadata", metadata)
            .text("pinataOptions", options);
        let response = self
            .execute_upload(self.legacy_pin_file_url()?, form)
            .await?;
        let response = self.require_success(response, Operation::Submit).await?;
        let pinned: LegacyPinResponse = self.decode(response, Operation::Submit).await?;
        self.validate_provider_field(&pinned.ipfs_hash)?;
        if pinned.ipfs_hash != request.cid {
            self.cleanup_uploaded_legacy_pin(&pinned.ipfs_hash).await;
            return Err(upload_correlation_error());
        }
        self.remote_from_legacy_pin_response(pinned, &request.cid, &request.metadata)
    }

    /// Best-effort removal of an upload-strategy artifact whose CID did not match
    /// the requested CID; failures must not mask the original mismatch error.
    async fn cleanup_uploaded_file(&self, provider_id: &str) {
        let Ok(url) = self.file_url(provider_id) else {
            return;
        };
        let _ = self.execute(self.http.delete(url), Operation::Unpin).await;
    }

    async fn cleanup_uploaded_legacy_pin(&self, cid: &str) {
        let Ok(url) = self.legacy_unpin_url(cid) else {
            return;
        };
        let _ = self.execute(self.http.delete(url), Operation::Unpin).await;
    }

    async fn content_part(&self, request: &SubmitPin) -> Result<multipart::Part, ProviderError> {
        let kubo = self.kubo.as_ref().ok_or_else(|| {
            protocol_error("Pinata upload strategy requires a Kubo content source")
        })?;
        let stream = tokio::time::timeout(
            self.upload_idle,
            crate::kubo::cat::stream_cat(kubo, &request.cid, None),
        )
        .await
        .map_err(|_| ProviderError {
            class: ProviderErrorClass::NotSubmitted,
            message: "provider upload source idle before dispatch".into(),
            retry_after: None,
        })?
        .map_err(|_| ProviderError {
            class: ProviderErrorClass::NotSubmitted,
            message: "Kubo content could not be read for provider upload".to_owned(),
            retry_after: None,
        })?;
        let mapped =
            stream.map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) });
        multipart::Part::stream(ReqwestBody::wrap_stream(mapped))
            .file_name(safe_file_name(&request.name))
            .mime_str("application/octet-stream")
            .map_err(|_| protocol_error("provider upload part could not be built"))
    }

    fn remote_from_job_with_metadata(
        &self,
        job: PinataJob,
        metadata: &BTreeMap<String, String>,
    ) -> Result<RemotePin, ProviderError> {
        let request_id = encode_request_id(&job.id, &job.cid, metadata);
        self.remote_from_job_with_request_id(job, request_id)
    }

    fn remote_from_job_with_request_id(
        &self,
        job: PinataJob,
        request_id: String,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&job.id)?;
        self.validate_provider_field(&job.cid)?;
        let (status, failure_reason) = self.normalize_status(&job.status)?;
        Ok(RemotePin {
            request_id,
            cid: job.cid,
            status,
            raw_status: job.status,
            failure_reason,
        })
    }

    fn remote_from_file(
        &self,
        file: PinataFile,
        request_id: String,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&file.id)?;
        self.validate_provider_field(&file.cid)?;
        Ok(RemotePin {
            request_id,
            cid: file.cid,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        })
    }

    fn remote_from_v3_upload(
        &self,
        file: PinataFileUpload,
        metadata: &BTreeMap<String, String>,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&file.id)?;
        self.validate_provider_field(&file.cid)?;
        Ok(RemotePin {
            request_id: encode_request_id_with_prefix(
                V3_REQUEST_ID_PREFIX,
                &file.id,
                &file.cid,
                metadata,
            ),
            cid: file.cid,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        })
    }

    fn remote_from_legacy_pin_response(
        &self,
        pinned: LegacyPinResponse,
        expected_cid: &str,
        metadata: &BTreeMap<String, String>,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&pinned.ipfs_hash)?;
        if pinned.ipfs_hash != expected_cid {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        let provider_id = pinned.id.as_deref().unwrap_or(&pinned.ipfs_hash);
        self.validate_provider_field(provider_id)?;
        let (status, failure_reason) = self.normalize_status(&pinned.status)?;
        Ok(RemotePin {
            request_id: encode_request_id_with_prefix(
                LEGACY_REQUEST_ID_PREFIX,
                provider_id,
                &pinned.ipfs_hash,
                metadata,
            ),
            cid: pinned.ipfs_hash,
            status,
            raw_status: pinned.status,
            failure_reason,
        })
    }

    fn remote_from_legacy_row(
        &self,
        row: LegacyPinRow,
        request_id: String,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&row.ipfs_pin_hash)?;
        Ok(RemotePin {
            request_id,
            cid: row.ipfs_pin_hash,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        })
    }

    fn remote_from_legacy_row_with_metadata(
        &self,
        row: LegacyPinRow,
        metadata: &BTreeMap<String, String>,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&row.id)?;
        self.validate_provider_field(&row.ipfs_pin_hash)?;
        let request_id = encode_request_id_with_prefix(
            LEGACY_REQUEST_ID_PREFIX,
            &row.id,
            &row.ipfs_pin_hash,
            metadata,
        );
        self.remote_from_legacy_row(row, request_id)
    }

    fn remote_from_legacy_job_with_request_id(
        &self,
        job: LegacyPinJob,
        request_id: String,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&job.id)?;
        self.validate_provider_field(&job.ipfs_pin_hash)?;
        let (status, failure_reason) = self.normalize_status(&job.status)?;
        Ok(RemotePin {
            request_id,
            cid: job.ipfs_pin_hash,
            status,
            raw_status: job.status,
            failure_reason,
        })
    }

    fn remote_from_legacy_job_with_metadata(
        &self,
        job: LegacyPinJob,
        metadata: &BTreeMap<String, String>,
    ) -> Result<RemotePin, ProviderError> {
        let request_id = encode_request_id_with_prefix(
            LEGACY_REQUEST_ID_PREFIX,
            &job.id,
            &job.ipfs_pin_hash,
            metadata,
        );
        self.remote_from_legacy_job_with_request_id(job, request_id)
    }

    fn normalize_status(
        &self,
        status: &str,
    ) -> Result<(RemotePinStatus, Option<String>), ProviderError> {
        let normalized = match status {
            "queued" => RemotePinStatus::Queued,
            "prechecking" | "searching" | "retrieving" | "retreiving" | "backfilled" => {
                RemotePinStatus::Pinning
            }
            "pinned" => RemotePinStatus::Pinned,
            "expired" | "over_free_limit" | "over_max_size" | "invalid_object"
            | "bad_host_node" => RemotePinStatus::Failed,
            _ => return Err(protocol_error("provider returned an unknown pin status")),
        };
        let failure_reason = (normalized == RemotePinStatus::Failed)
            .then(|| self.bounded_token_free_reason(status))
            .flatten();
        Ok((normalized, failure_reason))
    }

    fn request_ref(&self, request_id: &str) -> Result<PinataRequestRef, ProviderError> {
        if let Some(encoded) = request_id.strip_prefix(V3_REQUEST_ID_PREFIX) {
            let parts = encoded.split(':').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err(protocol_error("provider request ID is invalid"));
            }
            let provider_id = decode_request_component(parts[0])?;
            let cid = decode_request_component(parts[1])?;
            let metadata = decode_request_metadata(parts[2])?;
            self.validate_provider_field(&provider_id)?;
            self.validate_provider_field(&cid)?;
            return Ok(PinataRequestRef {
                api: PinataApi::V3,
                provider_id,
                cid: Some(cid),
                metadata: Some(metadata),
            });
        }
        if let Some(encoded) = request_id.strip_prefix(LEGACY_REQUEST_ID_PREFIX) {
            let parts = encoded.split(':').collect::<Vec<_>>();
            if parts.len() != 3 {
                return Err(protocol_error("provider request ID is invalid"));
            }
            let provider_id = decode_request_component(parts[0])?;
            let cid = decode_request_component(parts[1])?;
            let metadata = decode_request_metadata(parts[2])?;
            self.validate_provider_field(&provider_id)?;
            self.validate_provider_field(&cid)?;
            return Ok(PinataRequestRef {
                api: PinataApi::Legacy,
                provider_id,
                cid: Some(cid),
                metadata: Some(metadata),
            });
        }
        self.validate_provider_field(request_id)?;
        Err(ProviderError {
            class: ProviderErrorClass::Protocol,
            message: "legacy Pinata PSA request ID requires a /psa endpoint or migration"
                .to_owned(),
            retry_after: None,
        })
    }

    fn validate_provider_field(&self, value: &str) -> Result<(), ProviderError> {
        if value.trim().is_empty()
            || matches!(value, "." | "..")
            || value.contains(self.token.expose())
            || value.to_ascii_lowercase().contains("bearer ")
            || value
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(protocol_error(
                "provider response contains an unsafe pin field",
            ));
        }
        Ok(())
    }

    fn bounded_token_free_reason(&self, reason: &str) -> Option<String> {
        if reason.trim().is_empty()
            || reason.contains(self.token.expose())
            || reason.to_ascii_lowercase().contains("bearer ")
        {
            return None;
        }
        Some(reason.chars().take(MAX_FAILURE_REASON_CHARS).collect())
    }

    fn transport_error(&self, operation: Operation, error: &reqwest::Error) -> ProviderError {
        let class = if error.is_builder() || error.is_connect() {
            ProviderErrorClass::NotSubmitted
        } else if operation == Operation::Submit && error.is_timeout() {
            ProviderErrorClass::Ambiguous
        } else {
            ProviderErrorClass::Transient
        };
        let message = if class == ProviderErrorClass::Protocol {
            "provider request could not be built"
        } else {
            "provider transport request failed"
        };
        ProviderError {
            class,
            message: message.to_owned(),
            retry_after: None,
        }
    }
}

#[async_trait::async_trait]
impl PinningProvider for PinataClient {
    fn name(&self) -> &str {
        &self.name
    }

    fn invocation_route(&self) -> (&'static str, &'static str) {
        if self.legacy_psa.is_some() {
            return ("psa", "cid");
        }
        (
            match self.api {
                PinataApi::V3 => "pinata_v3",
                PinataApi::Legacy => "pinata_legacy",
            },
            match self.strategy {
                PinataStrategy::Cid => "cid",
                PinataStrategy::Upload => "upload",
            },
        )
    }

    async fn find_historical(
        &self,
        query: FindPin,
        api: &str,
        strategy: &str,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        if api == "psa" {
            return match self
                .legacy_psa
                .as_ref()
                .or(self.legacy_request_psa.as_ref())
            {
                Some(psa) if strategy == "cid" => psa.find(query).await,
                _ => Err(protocol_error("historical provider route unavailable")),
            };
        }
        let mut historical = self.clone();
        historical.legacy_psa = None;
        historical.api = match api {
            "pinata_v3" => PinataApi::V3,
            "pinata_legacy" => PinataApi::Legacy,
            _ => return Err(protocol_error("historical provider route unavailable")),
        };
        historical.strategy = match strategy {
            "cid" => PinataStrategy::Cid,
            "upload" => PinataStrategy::Upload,
            _ => return Err(protocol_error("historical provider route unavailable")),
        };
        historical.find(query).await
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        if let Some(legacy) = &self.legacy_psa {
            return legacy.submit(request).await;
        }
        match (self.api, self.strategy) {
            (PinataApi::V3, PinataStrategy::Cid) => self.submit_v3_cid(request).await,
            (PinataApi::Legacy, PinataStrategy::Cid) => self.submit_legacy_cid(request).await,
            (_, PinataStrategy::Upload) => self.submit_upload(request).await,
        }
    }

    async fn get_historical(
        &self,
        request_id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<RemotePin, ProviderError> {
        self.historical(api, strategy)?.get(request_id).await
    }

    async fn unpin_historical(
        &self,
        request_id: &str,
        api: &str,
        strategy: &str,
    ) -> Result<(), ProviderError> {
        self.historical(api, strategy)?.unpin(request_id).await
    }

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError> {
        if let Some(legacy) = &self.legacy_psa {
            return legacy.get(request_id).await;
        }
        if !request_id.starts_with(V3_REQUEST_ID_PREFIX)
            && !request_id.starts_with(LEGACY_REQUEST_ID_PREFIX)
        {
            self.validate_provider_field(request_id)?;
            if let Some(legacy) = &self.legacy_request_psa {
                return legacy.get(request_id).await;
            }
        }
        let reference = self.request_ref(request_id)?;
        if reference.api == PinataApi::V3 {
            for file in self.list_files(reference.cid.as_deref()).await? {
                reference.ensure_file_correlation(&file)?;
                if !reference.matches_file(&file) {
                    continue;
                }
                return self.remote_from_file(file, request_id.to_owned());
            }
            for job in self.list_queue(reference.cid.as_deref()).await? {
                reference.ensure_job_correlation(&job)?;
                if job.id != reference.provider_id {
                    continue;
                }
                return self.remote_from_job_with_request_id(job, request_id.to_owned());
            }
        }
        if reference.api == PinataApi::Legacy
            && let Some(cid) = reference.cid.as_deref()
        {
            for row in self.list_legacy_pins(cid).await? {
                if row.ipfs_pin_hash != cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if reference.matches_legacy(&row) {
                    return self.remote_from_legacy_row(row, request_id.to_owned());
                }
            }
            for job in self.list_legacy_jobs(cid).await? {
                if job.ipfs_pin_hash != cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if job.id == reference.provider_id {
                    return self.remote_from_legacy_job_with_request_id(job, request_id.to_owned());
                }
            }
        }
        Err(ProviderError {
            class: ProviderErrorClass::NotFound,
            message: "provider request was not found".to_owned(),
            retry_after: None,
        })
    }

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        if let Some(legacy) = &self.legacy_psa {
            return legacy.find(query).await;
        }
        let mut remotes = Vec::new();
        if self.api == PinataApi::V3 {
            let files = self.list_files(Some(&query.cid)).await?;
            for file in files {
                if file.cid != query.cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if metadata_contains(&file.keyvalues, &query.metadata) {
                    let request_id = encode_request_id(&file.id, &file.cid, &query.metadata);
                    remotes.push(self.remote_from_file(file, request_id)?);
                }
            }
            let known_ids = remotes
                .iter()
                .filter_map(|remote| self.request_ref(&remote.request_id).ok())
                .map(|reference| reference.provider_id)
                .collect::<std::collections::BTreeSet<_>>();
            let jobs = if self.strategy == PinataStrategy::Cid {
                self.list_queue(Some(&query.cid)).await?
            } else {
                Vec::new()
            };
            for job in jobs {
                if job.cid != query.cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if !known_ids.contains(&job.id)
                    && metadata_contains(&job.keyvalues, &query.metadata)
                {
                    remotes.push(self.remote_from_job_with_metadata(job, &query.metadata)?);
                }
            }
        }
        if remotes.iter().any(|remote| remote.cid != query.cid) {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        if self.api == PinataApi::Legacy {
            for row in self.list_legacy_pins(&query.cid).await? {
                if row.ipfs_pin_hash != query.cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if metadata_contains(&row.metadata.keyvalues, &query.metadata) {
                    remotes.push(self.remote_from_legacy_row_with_metadata(row, &query.metadata)?);
                }
            }
            let jobs = if self.strategy == PinataStrategy::Cid {
                self.list_legacy_jobs(&query.cid).await?
            } else {
                Vec::new()
            };
            for job in jobs {
                if job.ipfs_pin_hash != query.cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if metadata_contains(&job.keyvalues, &query.metadata) {
                    remotes.push(self.remote_from_legacy_job_with_metadata(job, &query.metadata)?);
                }
            }
        }
        Ok(remotes)
    }

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        if let Some(legacy) = &self.legacy_psa {
            return legacy.unpin(request_id).await;
        }
        if !request_id.starts_with(V3_REQUEST_ID_PREFIX)
            && !request_id.starts_with(LEGACY_REQUEST_ID_PREFIX)
        {
            self.validate_provider_field(request_id)?;
            if let Some(legacy) = &self.legacy_request_psa {
                return legacy.unpin(request_id).await;
            }
        }
        let reference = self.request_ref(request_id)?;
        if reference.api == PinataApi::Legacy {
            let cid = reference.cid.as_deref().unwrap_or(&reference.provider_id);
            let response = self
                .execute(
                    self.http.delete(self.legacy_unpin_url(cid)?),
                    Operation::Unpin,
                )
                .await?;
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(());
            }
            self.require_success(response, Operation::Unpin).await?;
            return Ok(());
        }
        let response = self
            .execute(
                self.http.delete(self.file_url(&reference.provider_id)?),
                Operation::Unpin,
            )
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            if let Some(cid) = reference.cid.as_deref() {
                if self.delete_matching_file_fallback(&reference).await? {
                    return Ok(());
                }
                let mut pending = false;
                for job in self.list_queue(Some(cid)).await? {
                    if job.id != reference.provider_id {
                        continue;
                    }
                    reference.ensure_job_correlation(&job)?;
                    if pinata_status_is_terminal(&job.status) {
                        continue;
                    }
                    pending = true;
                    let cancel = self
                        .execute(
                            self.http
                                .delete(self.pin_by_cid_request_url(&reference.provider_id)?),
                            Operation::Unpin,
                        )
                        .await?;
                    if cancel.status() != StatusCode::NOT_FOUND {
                        self.require_success(cancel, Operation::Unpin).await?;
                        return Ok(());
                    }
                }
                if pending {
                    return Err(ProviderError {
                        class: ProviderErrorClass::Transient,
                        message: "provider pin request is not deletable yet".to_owned(),
                        retry_after: None,
                    });
                }
            }
            return Ok(());
        }
        self.require_success(response, Operation::Unpin).await?;
        Ok(())
    }
}

impl PinataClient {
    async fn delete_matching_file_fallback(
        &self,
        reference: &PinataRequestRef,
    ) -> Result<bool, ProviderError> {
        let Some(cid) = reference.cid.as_deref() else {
            return Ok(false);
        };
        let Some(metadata) = reference.metadata.as_ref() else {
            return Ok(false);
        };
        let mut deleted = false;
        for file in self.list_files(Some(cid)).await? {
            if file.cid != cid {
                return Err(protocol_error("provider response failed pin correlation"));
            }
            if !metadata_contains(&file.keyvalues, metadata) {
                continue;
            }
            let response = self
                .execute(self.http.delete(self.file_url(&file.id)?), Operation::Unpin)
                .await?;
            if response.status() != StatusCode::NOT_FOUND {
                self.require_success(response, Operation::Unpin).await?;
            }
            deleted = true;
        }
        Ok(deleted)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Submit,
    Get,
    Find,
    Unpin,
}

struct PinataRequestRef {
    api: PinataApi,
    provider_id: String,
    cid: Option<String>,
    metadata: Option<BTreeMap<String, String>>,
}

impl PinataRequestRef {
    fn ensure_file_correlation(&self, file: &PinataFile) -> Result<(), ProviderError> {
        if self.cid.as_deref().is_some_and(|cid| file.cid != cid) {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        Ok(())
    }

    fn ensure_job_correlation(&self, job: &PinataJob) -> Result<(), ProviderError> {
        if self.cid.as_deref().is_some_and(|cid| job.cid != cid) {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        Ok(())
    }

    fn matches_file(&self, file: &PinataFile) -> bool {
        file.id == self.provider_id
            || self.cid.as_deref() == Some(file.cid.as_str())
                && self
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata_contains(&file.keyvalues, metadata))
    }

    fn matches_legacy(&self, row: &LegacyPinRow) -> bool {
        row.id == self.provider_id
            || self.cid.as_deref() == Some(row.ipfs_pin_hash.as_str())
                && self
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata_contains(&row.metadata.keyvalues, metadata))
    }
}

#[derive(Serialize)]
struct PinataPinByCidRequest<'a> {
    cid: &'a str,
    name: &'a str,
    keyvalues: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PinataData<T> {
    data: T,
}

#[derive(Deserialize)]
struct PinataJob {
    id: String,
    cid: String,
    status: String,
    #[serde(default)]
    keyvalues: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PinataQueuePage {
    #[serde(default)]
    count: Option<usize>,
    #[serde(default, deserialize_with = "nullable_list")]
    jobs: Vec<PinataJob>,
    #[serde(default, alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct PinataFile {
    id: String,
    cid: String,
    #[serde(default)]
    keyvalues: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PinataFilesPage {
    #[serde(default)]
    count: Option<usize>,
    #[serde(default, deserialize_with = "nullable_list")]
    files: Vec<PinataFile>,
    #[serde(default, alias = "nextPageToken")]
    next_page_token: Option<String>,
}

fn nullable_list<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

fn check_page_count(
    expected: &mut Option<usize>,
    current: Option<usize>,
) -> Result<(), ProviderError> {
    if let Some(current) = current {
        if expected.is_some_and(|previous| previous != current) {
            return Err(protocol_error("provider returned conflicting page counts"));
        }
        *expected = Some(current);
    }
    Ok(())
}

#[derive(Deserialize)]
struct PinataFileUpload {
    id: String,
    cid: String,
}

#[derive(Serialize)]
struct LegacyPinByHashRequest<'a> {
    #[serde(rename = "hashToPin")]
    hash_to_pin: &'a str,
    #[serde(rename = "pinataMetadata")]
    pinata_metadata: LegacyPinMetadata<'a>,
}

#[derive(Serialize)]
struct LegacyPinMetadata<'a> {
    name: &'a str,
    keyvalues: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct LegacyPinResponse {
    #[serde(rename = "IpfsHash", alias = "ipfsHash")]
    ipfs_hash: String,
    #[serde(rename = "ID", alias = "id", default)]
    id: Option<String>,
    #[serde(default = "pinned_status")]
    status: String,
}

#[derive(Deserialize)]
struct LegacyPinList {
    #[serde(default)]
    count: Option<usize>,
    #[serde(default, deserialize_with = "nullable_list")]
    rows: Vec<LegacyPinRow>,
}

#[derive(Deserialize)]
struct LegacyPinRow {
    #[serde(default)]
    id: String,
    #[serde(rename = "ipfs_pin_hash")]
    ipfs_pin_hash: String,
    #[serde(default)]
    metadata: LegacyPinRowMetadata,
}

#[derive(Default, Deserialize)]
struct LegacyPinRowMetadata {
    #[serde(default)]
    keyvalues: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct LegacyPinJobs {
    #[serde(default)]
    count: usize,
    #[serde(default, deserialize_with = "nullable_list")]
    rows: Vec<LegacyPinJob>,
}

#[derive(Deserialize)]
struct LegacyPinJob {
    id: String,
    ipfs_pin_hash: String,
    status: String,
    #[serde(default)]
    keyvalues: BTreeMap<String, String>,
}

fn pinned_status() -> String {
    "pinned".to_owned()
}

fn encode_request_id(provider_id: &str, cid: &str, metadata: &BTreeMap<String, String>) -> String {
    encode_request_id_with_prefix(V3_REQUEST_ID_PREFIX, provider_id, cid, metadata)
}

fn encode_request_id_with_prefix(
    prefix: &str,
    provider_id: &str,
    cid: &str,
    metadata: &BTreeMap<String, String>,
) -> String {
    let metadata = serde_json::to_vec(metadata).expect("Pinata metadata must serialize");
    format!(
        "{prefix}{}:{}:{}",
        URL_SAFE_NO_PAD.encode(provider_id),
        URL_SAFE_NO_PAD.encode(cid),
        URL_SAFE_NO_PAD.encode(metadata)
    )
}

fn safe_file_name(name: &str) -> String {
    let candidate = name
        .rsplit(['/', '\\'])
        .find(|segment| !segment.is_empty())
        .unwrap_or("object");
    candidate
        .chars()
        .map(|character| {
            if character.is_control() || character.is_whitespace() {
                '_'
            } else {
                character
            }
        })
        .collect()
}

fn decode_request_component(value: &str) -> Result<String, ProviderError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| protocol_error("provider request ID is invalid"))?;
    String::from_utf8(bytes).map_err(|_| protocol_error("provider request ID is invalid"))
}

fn decode_request_metadata(value: &str) -> Result<BTreeMap<String, String>, ProviderError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| protocol_error("provider request ID is invalid"))?;
    serde_json::from_slice(&bytes).map_err(|_| protocol_error("provider request ID is invalid"))
}

fn metadata_contains(
    actual: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
) -> bool {
    expected
        .iter()
        .all(|(key, value)| actual.get(key) == Some(value))
}

fn pinata_status_is_terminal(status: &str) -> bool {
    matches!(
        status,
        "pinned"
            | "expired"
            | "over_free_limit"
            | "over_max_size"
            | "invalid_object"
            | "bad_host_node"
    )
}

fn parse_base_url(endpoint: &str) -> Result<Url, ()> {
    let url = Url::parse(endpoint).map_err(|_| ())?;
    if url.cannot_be_a_base()
        || !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(());
    }
    Url::parse(&normalize_base(endpoint.to_owned())).map_err(|_| ())
}

fn normalize_base(mut endpoint: String) -> String {
    let scheme_end = endpoint
        .find("://")
        .map(|index| index + 3)
        .unwrap_or_default();
    while endpoint.ends_with('/') && endpoint.len() > scheme_end {
        endpoint.pop();
    }
    endpoint
}

fn pinata_v3_endpoint(endpoint: &str) -> String {
    let normalized = normalize_base(endpoint.to_owned());
    let Ok(mut url) = Url::parse(&normalized) else {
        return normalized;
    };
    let path_ends_with_v3 = url.path().rsplit('/').next() == Some("v3");
    if path_ends_with_v3 {
        return normalized;
    }
    let Some(mut segments) = url.path_segments_mut().ok() else {
        return normalized;
    };
    segments.pop_if_empty();
    segments.push("v3");
    drop(segments);
    normalize_base(url.to_string())
}

fn pinata_legacy_endpoint(endpoint: &str) -> String {
    let normalized = normalize_base(endpoint.to_owned());
    let Ok(mut url) = Url::parse(&normalized) else {
        return normalized;
    };
    let path_ends_with_v3 = url.path().rsplit('/').next() == Some("v3");
    if !path_ends_with_v3 {
        return normalized;
    }
    let Some(mut segments) = url.path_segments_mut().ok() else {
        return normalized;
    };
    segments.pop_if_empty();
    segments.pop();
    drop(segments);
    normalize_base(url.to_string())
}

fn pinata_legacy_psa_endpoint(endpoint: &str) -> String {
    let normalized = normalize_base(endpoint.to_owned());
    let Ok(mut url) = Url::parse(&normalized) else {
        return normalized;
    };
    let path_ends_with_v3 = url.path().rsplit('/').next() == Some("v3");
    let Some(mut segments) = url.path_segments_mut().ok() else {
        return normalized;
    };
    segments.pop_if_empty();
    if path_ends_with_v3 {
        segments.pop();
    }
    segments.push("psa");
    drop(segments);
    normalize_base(url.to_string())
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn protocol_error(message: &str) -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Protocol,
        message: message.to_owned(),
        retry_after: None,
    }
}

fn upload_idle_error() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Ambiguous,
        message: "provider upload idle deadline exceeded; remote effect unknown".into(),
        retry_after: None,
    }
}

/// An upload-strategy CID mismatch is a deterministic chunking/DAG parity defect:
/// retrying re-uploads the whole object and leaks another remote pin, so it is
/// terminal rather than recoverable.
fn upload_correlation_error() -> ProviderError {
    ProviderError {
        class: ProviderErrorClass::Terminal,
        message: "provider upload response failed pin correlation".to_owned(),
        retry_after: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::{
        kubo::KuboClient,
        pinning::{
            config::{PinataApi, PinataProviderOptions, PinataStrategy},
            pinata::{PINATA_BASE_URL, build_pinata, build_pinata_with_options, encode_request_id},
            provider::{FindPin, PinningProvider, ProviderErrorClass, RemotePinStatus, SubmitPin},
            psa::test_token,
        },
    };

    const TOKEN: &str = "provider-token";

    #[tokio::test]
    async fn stage1_upload_idle_bounds_response_wait() {
        let server = MockServer::start().await;
        let kubo_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![42; 64]))
            .mount(&kubo_server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v3/files"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(3))
                    .set_body_json(json!({"data":{"id":"id","cid":"cid"}})),
            )
            .mount(&server)
            .await;
        let mut client = build_pinata_with_options(
            "pinata".into(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/v3", server.uri())),
            },
            Some(KuboClient::new(kubo_server.uri())),
        );
        client.upload_idle = Duration::from_millis(100);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.submit(SubmitPin {
                cid: "cid".into(),
                name: "name".into(),
                metadata: BTreeMap::new(),
            }),
        )
        .await;
        let error = result
            .expect("idle upload must release its slot before outer deadline")
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Ambiguous);
        assert_eq!(requests(&server).await.len(), 1);
    }

    #[tokio::test]
    async fn stage1_nullable_lists_and_camel_pagination_are_complete() {
        for empty in [json!(null), json!([])] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v3/files/public"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"data":{"files":empty}})),
                )
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/v3/files/public/pin_by_cid"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"data":{"jobs":empty}})),
                )
                .mount(&server)
                .await;
            assert!(
                provider(&server)
                    .find(FindPin::for_job("cid", "job-7"))
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(requests(&server).await.len(), 2);
        }
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data":{"files":[],"nextPageToken":"again"}})),
            )
            .mount(&server)
            .await;
        assert!(
            provider(&server)
                .find(FindPin::for_job("cid", "job-7"))
                .await
                .is_err()
        );
        assert_eq!(
            requests(&server).await.len(),
            2,
            "cycle must be detected before repeating HTTP"
        );
    }

    #[tokio::test]
    async fn stage1_upload_find_never_calls_cid_queue() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":{"files":[]}})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let mut client = provider(&server);
        client.strategy = PinataStrategy::Upload;
        assert!(
            client
                .find(FindPin::for_job("cid", "job-7"))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(requests(&server).await.len(), 1);
    }

    #[tokio::test]
    async fn stage1_forbidden_is_not_auth_and_never_echoes_body_secrets() {
        for (code, expected) in [
            ("PLAN_RESTRICTED", "PlanRestricted"),
            ("PERMISSION_DENIED", "PermissionDenied"),
            ("unexpected", "UnknownForbidden"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST")).respond_with(ResponseTemplate::new(403).set_body_json(json!({"error":{"code":code,"message":"eyJhbGci.JWT-SENTINEL SSE-C-SENTINEL ?credential=QUERY-SENTINEL"}}))).mount(&server).await;
            let error = provider(&server)
                .submit(SubmitPin {
                    cid: "cid".into(),
                    name: "name".into(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap_err();
            assert_eq!(format!("{:?}", error.class), expected);
            let rendered = format!("{error:?} {error}");
            for secret in ["JWT-SENTINEL", "SSE-C-SENTINEL", "QUERY-SENTINEL"] {
                assert!(!rendered.contains(secret));
            }
            assert_eq!(requests(&server).await.len(), 1);
        }
    }

    fn provider(server: &MockServer) -> crate::pinning::pinata::PinataClient {
        build_pinata(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3///", server.uri())),
        )
    }

    fn job(id: &str, status: &str, cid: &str) -> serde_json::Value {
        json!({
            "id": id,
            "cid": cid,
            "status": status,
            "keyvalues": {
                "gateway_job_id": "job-7",
                "gateway_lease_id": "lease-7",
            },
        })
    }

    async fn requests(server: &MockServer) -> Vec<wiremock::Request> {
        server.received_requests().await.unwrap()
    }

    fn multipart_part(request: &wiremock::Request, name: &str) -> Option<String> {
        let content_type = request.headers.get("content-type")?.to_str().ok()?;
        let boundary = content_type.split("boundary=").nth(1)?;
        let body = String::from_utf8_lossy(&request.body).into_owned();
        body.split(&format!("--{boundary}"))
            .filter_map(|part| part.split_once("\r\n\r\n"))
            .find(|(headers, _)| headers.contains(&format!("name=\"{name}\"")))
            .map(|(_, value)| value.trim_end_matches("\r\n").to_owned())
    }

    #[test]
    fn pinata_uses_the_v3_default_and_normalizes_an_override() {
        let provider = build_pinata(
            "pinata".to_owned(),
            test_token("provider-token"),
            Some("http://example.test/v3///".to_owned()),
        );

        assert_eq!(PINATA_BASE_URL, "https://api.pinata.cloud/v3");
        assert_eq!(provider.base_url(), "http://example.test/v3");
    }

    #[tokio::test]
    async fn submit_get_find_and_unpin_use_the_v3_resource_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": job("request-7", "prechecking", "bafy-target"),
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "files": [{
                        "id": "request-7",
                        "cid": "bafy-target",
                        "keyvalues": {
                            "gateway_job_id": "job-7",
                            "gateway_lease_id": "lease-7",
                        },
                    }],
                },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "jobs": [] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/request-7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": null })))
            .mount(&server)
            .await;

        let provider = provider(&server);
        let submitted = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::from([
                    ("gateway_job_id".to_owned(), "job-7".to_owned()),
                    ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
                ]),
            })
            .await
            .unwrap();
        let expected_request_id = encode_request_id(
            "request-7",
            "bafy-target",
            &BTreeMap::from([
                ("gateway_job_id".to_owned(), "job-7".to_owned()),
                ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
            ]),
        );
        assert_eq!(submitted.request_id, expected_request_id);
        assert_eq!(submitted.status, RemotePinStatus::Pinning);
        assert_eq!(submitted.raw_status, "prechecking");
        assert_eq!(
            provider.get(&submitted.request_id).await.unwrap().status,
            RemotePinStatus::Pinned
        );
        assert_eq!(
            provider
                .find(FindPin::for_job("bafy-target", "job-7"))
                .await
                .unwrap()[0]
                .status,
            RemotePinStatus::Pinned
        );
        provider.unpin(&submitted.request_id).await.unwrap();

        let received = requests(&server).await;
        assert!(received.iter().all(|request| {
            request
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                == Some("Bearer provider-token")
        }));
        let post = received
            .iter()
            .find(|request| request.method.as_str() == "POST")
            .unwrap();
        assert_eq!(post.url.path(), "/v3/files/public/pin_by_cid");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&post.body).unwrap(),
            json!({
                "cid": "bafy-target",
                "name": "bucket/key",
                "keyvalues": {
                    "gateway_job_id": "job-7",
                    "gateway_lease_id": "lease-7",
                },
            })
        );
        assert!(received.iter().any(|request| {
            request.method.as_str() == "GET" && request.url.path() == "/v3/files/public"
        }));
        assert!(received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/v3/files/public/request-7"
        }));
    }

    #[tokio::test]
    async fn find_uses_the_pin_by_cid_queue_when_the_file_is_not_pinned_yet() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "files": [] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "jobs": [job("request-7", "searching", "bafy-target")] },
            })))
            .mount(&server)
            .await;

        let found = provider(&server)
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].request_id,
            encode_request_id(
                "request-7",
                "bafy-target",
                &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            )
        );
        assert_eq!(found[0].status, RemotePinStatus::Pinning);
        let received = requests(&server).await;
        for request in received {
            let query = request
                .url
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect::<BTreeMap<_, _>>();
            assert_eq!(query.get("cid"), Some(&"bafy-target".to_owned()));
            assert_eq!(query.get("limit"), Some(&"100".to_owned()));
        }
    }

    #[tokio::test]
    async fn get_preserves_the_original_request_id_when_queue_metadata_changes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "files": [] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "jobs": [{
                        "id": "request-7",
                        "cid": "bafy-target",
                        "status": "searching",
                        "keyvalues": {
                            "gateway_job_id": "job-7",
                            "provider_extra": "provider-token"
                        }
                    }]
                },
            })))
            .mount(&server)
            .await;

        let original_request_id = encode_request_id(
            "request-7",
            "bafy-target",
            &BTreeMap::from([
                ("gateway_job_id".to_owned(), "job-7".to_owned()),
                ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
            ]),
        );

        let remote = provider(&server).get(&original_request_id).await.unwrap();

        assert_eq!(remote.request_id, original_request_id);
        assert_eq!(remote.status, RemotePinStatus::Pinning);
        assert!(!remote.request_id.contains("provider-token"));
    }

    #[tokio::test]
    async fn legacy_psa_request_ids_use_the_derived_psa_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/psa/pins/legacy-psa-request-7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "requestid": "legacy-psa-request-7",
                "status": "pinned",
                "pin": {
                    "cid": "bafy-target",
                    "name": "bucket/key",
                    "origins": [],
                    "meta": {}
                },
                "info": {},
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/psa/pins/legacy-psa-request-7"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let provider = provider(&server);

        let remote = provider.get("legacy-psa-request-7").await.unwrap();
        provider.unpin("legacy-psa-request-7").await.unwrap();

        assert_eq!(remote.request_id, "legacy-psa-request-7");
        assert_eq!(remote.status, RemotePinStatus::Pinned);
        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "GET"
                && request.url.path() == "/psa/pins/legacy-psa-request-7"
        }));
        assert!(received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/psa/pins/legacy-psa-request-7"
        }));
        assert!(!received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/v3/files/public/legacy-psa-request-7"
        }));
    }

    #[tokio::test]
    async fn stage2_historical_reference_routes_survive_current_psa_profile() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":{"files":[{"id":"file-7","cid":"bafy-target","keyvalues":{"gateway_job_id":"job-7"}}]}})))
            .expect(1).mount(&server).await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/file-7"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let mut current = provider(&server);
        current.legacy_psa = current.legacy_request_psa.clone();
        assert_eq!(current.invocation_route(), ("psa", "cid"));
        let id = encode_request_id(
            "file-7",
            "bafy-target",
            &BTreeMap::from([("gateway_job_id".into(), "job-7".into())]),
        );
        let observed = current
            .get_historical(&id, "pinata_v3", "cid")
            .await
            .unwrap();
        assert_eq!(observed.status, RemotePinStatus::Pinned);
        current
            .unpin_historical(&id, "pinata_v3", "cid")
            .await
            .unwrap();
        assert!(
            requests(&server)
                .await
                .iter()
                .all(|request| !request.url.path().starts_with("/psa"))
        );
    }

    #[tokio::test]
    async fn delete_fallback_rejects_cid_mismatches_before_deleting_files() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/request-7"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "files": [{
                        "id": "wrong-file-id",
                        "cid": "bafy-other",
                        "keyvalues": {
                            "gateway_job_id": "job-7",
                            "gateway_lease_id": "lease-7",
                        }
                    }]
                },
            })))
            .mount(&server)
            .await;

        let error = provider(&server)
            .unpin(&encode_request_id(
                "request-7",
                "bafy-target",
                &BTreeMap::from([
                    ("gateway_job_id".to_owned(), "job-7".to_owned()),
                    ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
                ]),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.class, ProviderErrorClass::Protocol);
        let received = requests(&server).await;
        assert!(!received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/v3/files/public/wrong-file-id"
        }));
    }

    #[tokio::test]
    async fn v3_upload_strategy_streams_kubo_content_to_the_upload_api() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "id": "file-7",
                    "cid": "bafy-target"
                }
            })))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
        );
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap();

        assert_eq!(remote.status, RemotePinStatus::Pinned);
        assert_eq!(remote.cid, "bafy-target");
        assert!(remote.request_id.starts_with(super::V3_REQUEST_ID_PREFIX));
        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/uploads/v3/files"
        }));
    }

    #[tokio::test]
    async fn legacy_upload_strategy_uses_pin_file_to_ipfs_and_unpins_by_cid() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello legacy"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/pinning/pinFileToIPFS"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "IpfsHash": "bafy-target",
                "ID": "legacy-file-7",
                "PinSize": 12
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pinning/unpin/bafy-target"))
            .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(server.uri()),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Upload,
                upload_endpoint: None,
            },
            Some(KuboClient::new(kubo.uri())),
        );
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap();
        provider.unpin(&remote.request_id).await.unwrap();

        assert_eq!(remote.status, RemotePinStatus::Pinned);
        assert!(
            remote
                .request_id
                .starts_with(super::LEGACY_REQUEST_ID_PREFIX)
        );
        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/pinning/pinFileToIPFS"
        }));
        assert!(received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/pinning/unpin/bafy-target"
        }));
    }

    #[tokio::test]
    async fn v3_upload_cid_mismatch_deletes_the_remote_file_and_is_terminal() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "id": "file-7",
                    "cid": "bafy-different",
                    "note": "provider-body-marker"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/file-7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": null })))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
        );
        let error = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();

        assert_eq!(error.class, ProviderErrorClass::Terminal);
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("provider-body-marker"));
        assert!(!rendered.contains("bafy-different"));
        assert!(!rendered.contains(TOKEN));
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "DELETE"
                    && request.url.path() == "/v3/files/public/file-7")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn legacy_upload_cid_mismatch_unpins_the_remote_pin_and_is_terminal() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello legacy"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/pinning/pinFileToIPFS"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "IpfsHash": "bafy-different",
                "ID": "legacy-file-7",
                "note": "provider-body-marker"
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pinning/unpin/bafy-different"))
            .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(server.uri()),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Upload,
                upload_endpoint: None,
            },
            Some(KuboClient::new(kubo.uri())),
        );
        let error = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();

        assert_eq!(error.class, ProviderErrorClass::Terminal);
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("provider-body-marker"));
        assert!(!rendered.contains("bafy-different"));
        assert!(!rendered.contains(TOKEN));
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "DELETE"
                    && request.url.path() == "/pinning/unpin/bafy-different")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn cid_strategy_correlation_mismatch_never_deletes_remote_artifacts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": job("request-7", "searching", "bafy-different"),
            })))
            .mount(&server)
            .await;

        let error = provider(&server)
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();

        assert_eq!(error.class, ProviderErrorClass::Protocol);
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "DELETE")
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn upload_cid_mismatch_cleanup_failure_still_reports_the_terminal_mismatch() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "id": "file-7", "cid": "bafy-different" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/file-7"))
            .respond_with(ResponseTemplate::new(500).set_body_string("provider-body-marker"))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
        );
        let error = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();

        assert_eq!(error.class, ProviderErrorClass::Terminal);
        assert!(!format!("{error:?}").contains("provider-body-marker"));
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.method.as_str() == "DELETE")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn upload_requests_are_not_bound_by_the_standard_request_timeout() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(200))
                    .set_body_json(json!({ "data": { "id": "file-7", "cid": "bafy-target" } })),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(200))
                    .set_body_json(json!({ "data": job("request-7", "searching", "bafy-target") })),
            )
            .mount(&server)
            .await;

        let standard = reqwest::Client::builder()
            .timeout(Duration::from_millis(20))
            .build()
            .unwrap();
        let upload = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(200))
            .build()
            .unwrap();
        let request = SubmitPin {
            cid: "bafy-target".to_owned(),
            name: "bucket/key.txt".to_owned(),
            metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
        };

        let uploaded = crate::pinning::pinata::PinataClient::with_http(
            "pinata".to_owned(),
            format!("{}/v3", server.uri()),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
            test_token(TOKEN),
            standard.clone(),
            upload.clone(),
        )
        .submit(request.clone())
        .await
        .unwrap();
        assert_eq!(uploaded.cid, "bafy-target");

        let error = crate::pinning::pinata::PinataClient::with_http(
            "pinata".to_owned(),
            format!("{}/v3", server.uri()),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Cid,
                upload_endpoint: None,
            },
            None,
            test_token(TOKEN),
            standard,
            upload,
        )
        .submit(request)
        .await
        .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Ambiguous);
    }

    #[tokio::test]
    async fn provider_ids_that_are_dot_segments_are_rejected_before_any_follow_up_request() {
        for unsafe_id in [".", ".."] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v3/files/public/pin_by_cid"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": {
                        "id": unsafe_id,
                        "cid": "bafy-target",
                        "status": "searching",
                        "keyvalues": { "gateway_job_id": "job-7" },
                    },
                })))
                .mount(&server)
                .await;

            let error = provider(&server)
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
                })
                .await
                .unwrap_err();

            assert_eq!(error.class, ProviderErrorClass::Protocol, "{unsafe_id}");
            assert_eq!(
                error.message, "provider response contains an unsafe pin field",
                "{unsafe_id}"
            );
            let received = requests(&server).await;
            assert_eq!(
                received
                    .iter()
                    .filter(|request| request.method.as_str() != "POST")
                    .count(),
                0,
                "{unsafe_id}"
            );
        }
    }

    #[tokio::test]
    async fn v3_upload_sends_the_recovery_metadata_in_its_multipart_body() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "id": "file-7", "cid": "bafy-target" }
            })))
            .mount(&server)
            .await;

        build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
        )
        .submit(SubmitPin {
            cid: "bafy-target".to_owned(),
            name: "bucket/key.txt".to_owned(),
            metadata: BTreeMap::from([
                ("gateway_job_id".to_owned(), "job-7".to_owned()),
                ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
            ]),
        })
        .await
        .unwrap();

        let received = requests(&server).await;
        let upload = received
            .iter()
            .find(|request| request.url.path() == "/uploads/v3/files")
            .unwrap();
        assert_eq!(
            upload
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer provider-token")
        );
        assert_eq!(multipart_part(upload, "network").as_deref(), Some("public"));
        assert_eq!(
            multipart_part(upload, "name").as_deref(),
            Some("bucket/key.txt")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &multipart_part(upload, "keyvalues").unwrap()
            )
            .unwrap(),
            json!({ "gateway_job_id": "job-7", "gateway_lease_id": "lease-7" })
        );
        assert!(multipart_part(upload, "file").is_some());
    }

    #[tokio::test]
    async fn legacy_upload_sends_the_recovery_metadata_in_its_multipart_body() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello legacy"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/pinning/pinFileToIPFS"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "IpfsHash": "bafy-target",
                "ID": "legacy-file-7"
            })))
            .mount(&server)
            .await;

        build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(server.uri()),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Upload,
                upload_endpoint: None,
            },
            Some(KuboClient::new(kubo.uri())),
        )
        .submit(SubmitPin {
            cid: "bafy-target".to_owned(),
            name: "bucket/key.txt".to_owned(),
            metadata: BTreeMap::from([
                ("gateway_job_id".to_owned(), "job-7".to_owned()),
                ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
            ]),
        })
        .await
        .unwrap();

        let received = requests(&server).await;
        let upload = received
            .iter()
            .find(|request| request.url.path() == "/pinning/pinFileToIPFS")
            .unwrap();
        assert_eq!(
            upload
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer provider-token")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &multipart_part(upload, "pinataMetadata").unwrap()
            )
            .unwrap(),
            json!({
                "name": "bucket/key.txt",
                "keyvalues": { "gateway_job_id": "job-7", "gateway_lease_id": "lease-7" },
            })
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &multipart_part(upload, "pinataOptions").unwrap()
            )
            .unwrap(),
            json!({ "cidVersion": 1 })
        );
        assert!(multipart_part(upload, "file").is_some());
    }

    #[tokio::test]
    async fn v3_upload_recovers_an_ambiguous_submit_through_find() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello from kubo"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/uploads/v3/files"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(2))
                    .set_body_json(json!({ "data": { "id": "file-7", "cid": "bafy-target" } })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "files": [{
                        "id": "file-7",
                        "cid": "bafy-target",
                        "keyvalues": { "gateway_job_id": "job-7" },
                    }],
                },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "jobs": [] },
            })))
            .mount(&server)
            .await;

        let provider = crate::pinning::pinata::PinataClient::with_http(
            "pinata".to_owned(),
            format!("{}/v3", server.uri()),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Upload,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            Some(KuboClient::new(kubo.uri())),
            test_token(TOKEN),
            reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
            reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
        );
        let error = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Ambiguous);

        let found = provider
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].request_id,
            encode_request_id(
                "file-7",
                "bafy-target",
                &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            )
        );
        assert_eq!(found[0].status, RemotePinStatus::Pinned);
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.url.path() == "/uploads/v3/files")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn legacy_upload_recovers_an_ambiguous_submit_through_find() {
        let server = MockServer::start().await;
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello legacy"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/pinning/pinFileToIPFS"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(2))
                    .set_body_json(json!({ "IpfsHash": "bafy-target", "ID": "legacy-file-7" })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data/pinList"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rows": [{
                    "id": "legacy-file-7",
                    "ipfs_pin_hash": "bafy-target",
                    "metadata": { "keyvalues": { "gateway_job_id": "job-7" } }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pinning/pinJobs"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "count": 0, "rows": [] })),
            )
            .mount(&server)
            .await;

        let provider = crate::pinning::pinata::PinataClient::with_http(
            "pinata".to_owned(),
            server.uri(),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Upload,
                upload_endpoint: None,
            },
            Some(KuboClient::new(kubo.uri())),
            test_token(TOKEN),
            reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap(),
            reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
        );
        let error = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Ambiguous);

        let found = provider
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap();

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].request_id,
            super::encode_request_id_with_prefix(
                super::LEGACY_REQUEST_ID_PREFIX,
                "legacy-file-7",
                "bafy-target",
                &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            )
        );
        assert_eq!(found[0].status, RemotePinStatus::Pinned);
        let received = requests(&server).await;
        assert_eq!(
            received
                .iter()
                .filter(|request| request.url.path() == "/pinning/pinFileToIPFS")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn submit_http_error_statuses_map_to_their_provider_error_classes() {
        for (status, expected) in [
            (401, ProviderErrorClass::Authentication),
            (403, ProviderErrorClass::UnknownForbidden),
            (402, ProviderErrorClass::Quota),
            (507, ProviderErrorClass::Quota),
            (409, ProviderErrorClass::Ambiguous),
            (429, ProviderErrorClass::RateLimited),
            (400, ProviderErrorClass::InvalidInput),
            (404, ProviderErrorClass::InvalidInput),
            (500, ProviderErrorClass::Transient),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v3/files/public/pin_by_cid"))
                .respond_with(ResponseTemplate::new(status).set_body_string("provider-token"))
                .mount(&server)
                .await;

            let error = provider(&server)
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap_err();

            assert_eq!(error.class, expected, "{status}");
            assert_eq!(
                error.message,
                format!("provider returned HTTP status {status}"),
                "{status}"
            );
            assert!(!format!("{error:?}").contains(TOKEN), "{status}");
        }
    }

    #[tokio::test]
    async fn legacy_cid_strategy_uses_pin_by_hash_and_pin_list_find() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pinning/pinByHash"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "IpfsHash": "bafy-target",
                "ID": "legacy-request-7",
                "status": "prechecking"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data/pinList"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rows": [{
                    "id": "legacy-request-7",
                    "ipfs_pin_hash": "bafy-target",
                    "metadata": {
                        "keyvalues": { "gateway_job_id": "job-7" }
                    }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pinning/pinJobs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "count": 0,
                "rows": []
            })))
            .mount(&server)
            .await;

        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(server.uri()),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Cid,
                upload_endpoint: None,
            },
            None,
        );
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key.txt".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap();
        let found = provider
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap();

        assert_eq!(remote.status, RemotePinStatus::Pinning);
        assert_eq!(remote.raw_status, "prechecking");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].cid, "bafy-target");
        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "POST" && request.url.path() == "/pinning/pinByHash"
        }));
        assert!(received.iter().any(|request| {
            request.method.as_str() == "GET" && request.url.path() == "/data/pinList"
        }));
    }

    #[tokio::test]
    async fn legacy_pin_jobs_are_polled_until_the_pin_is_listed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data/pinList"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rows": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pinning/pinJobs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "count": 1,
                "rows": [{
                    "id": "legacy-request-7",
                    "ipfs_pin_hash": "bafy-target",
                    "status": "retrieving",
                    "keyvalues": { "gateway_job_id": "job-7" }
                }]
            })))
            .mount(&server)
            .await;
        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(server.uri()),
            PinataProviderOptions {
                api: PinataApi::Legacy,
                strategy: PinataStrategy::Cid,
                upload_endpoint: None,
            },
            None,
        );
        let request_id = super::encode_request_id_with_prefix(
            super::LEGACY_REQUEST_ID_PREFIX,
            "legacy-request-7",
            "bafy-target",
            &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
        );

        let remote = provider.get(&request_id).await.unwrap();
        let found = provider
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap();

        assert_eq!(remote.request_id, request_id);
        assert_eq!(remote.status, RemotePinStatus::Pinning);
        assert_eq!(remote.raw_status, "retrieving");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].status, RemotePinStatus::Pinning);
    }

    #[tokio::test]
    async fn prefixed_request_ids_route_to_their_original_api_flavor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data/pinList"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rows": [{
                    "id": "legacy-request-7",
                    "ipfs_pin_hash": "bafy-target",
                    "metadata": {
                        "keyvalues": { "gateway_job_id": "job-7" }
                    }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pinning/unpin/bafy-target"))
            .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
            .mount(&server)
            .await;
        let provider = build_pinata_with_options(
            "pinata".to_owned(),
            test_token(TOKEN),
            Some(format!("{}/v3", server.uri())),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Cid,
                upload_endpoint: None,
            },
            None,
        );
        let request_id = super::encode_request_id_with_prefix(
            super::LEGACY_REQUEST_ID_PREFIX,
            "legacy-request-7",
            "bafy-target",
            &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
        );

        let remote = provider.get(&request_id).await.unwrap();
        provider.unpin(&request_id).await.unwrap();

        assert_eq!(remote.status, RemotePinStatus::Pinned);
        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "GET" && request.url.path() == "/data/pinList"
        }));
        assert!(received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/pinning/unpin/bafy-target"
        }));
        assert!(!received.iter().any(|request| {
            request.method.as_str() == "GET" && request.url.path() == "/v3/files/public"
        }));
    }

    #[tokio::test]
    async fn v3_unpin_cancels_pending_pin_by_cid_requests() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/request-7"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "files": [] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "jobs": [job("request-7", "searching", "bafy-target")] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/pin_by_cid/request-7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": null })))
            .mount(&server)
            .await;

        provider(&server)
            .unpin(&encode_request_id(
                "request-7",
                "bafy-target",
                &BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            ))
            .await
            .unwrap();

        let received = requests(&server).await;
        assert!(received.iter().any(|request| {
            request.method.as_str() == "DELETE"
                && request.url.path() == "/v3/files/public/pin_by_cid/request-7"
        }));
    }

    #[tokio::test]
    async fn maps_pinata_terminal_queue_statuses_without_leaking_tokens() {
        for (status, expected) in [
            ("queued", RemotePinStatus::Queued),
            ("retrieving", RemotePinStatus::Pinning),
            ("retreiving", RemotePinStatus::Pinning),
            ("backfilled", RemotePinStatus::Pinning),
            ("expired", RemotePinStatus::Failed),
            ("over_free_limit", RemotePinStatus::Failed),
            ("over_max_size", RemotePinStatus::Failed),
            ("invalid_object", RemotePinStatus::Failed),
            ("bad_host_node", RemotePinStatus::Failed),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v3/files/public/pin_by_cid"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": job("request-7", status, "bafy-target"),
                })))
                .mount(&server)
                .await;

            let remote = provider(&server)
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap();

            assert_eq!(remote.status, expected, "{status}");
            assert_eq!(remote.raw_status, status);
            assert!(!format!("{remote:?}").contains(TOKEN));
        }

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": job("request-7", "provider-token", "bafy-target"),
            })))
            .mount(&server)
            .await;
        let error = provider(&server)
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        assert!(!format!("{error:?}").contains(TOKEN));
    }

    #[tokio::test]
    async fn submit_timeout_is_ambiguous_and_delete_404_waits_for_pending_queue_jobs() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(json!({ "data": job("request-7", "searching", "bafy-target") })),
            )
            .mount(&server)
            .await;
        let fast_http = reqwest::Client::builder()
            .timeout(Duration::from_millis(10))
            .build()
            .unwrap();
        let error = crate::pinning::pinata::PinataClient::with_http(
            "test".to_owned(),
            format!("{}/v3", server.uri()),
            PinataProviderOptions {
                api: PinataApi::V3,
                strategy: PinataStrategy::Cid,
                upload_endpoint: Some(format!("{}/uploads/v3", server.uri())),
            },
            None,
            test_token(TOKEN),
            fast_http.clone(),
            fast_http,
        )
        .submit(SubmitPin {
            cid: "bafy-target".to_owned(),
            name: "bucket/key".to_owned(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Ambiguous);

        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/request-7"))
            .respond_with(ResponseTemplate::new(404).set_body_string("provider-token"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "files": [] },
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "jobs": [job("request-7", "searching", "bafy-target")] },
            })))
            .mount(&server)
            .await;
        let error = provider(&server)
            .unpin(&encode_request_id(
                "request-7",
                "bafy-target",
                &BTreeMap::from([
                    ("gateway_job_id".to_owned(), "job-7".to_owned()),
                    ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
                ]),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Transient);
        assert!(!format!("{error:?}").contains(TOKEN));
    }
}
