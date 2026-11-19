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
            PinningProvider, ProviderError, ProviderErrorClass, RemotePin, RemotePinStatus,
            SubmitPin,
        },
        psa::PsaClient,
    },
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FAILURE_REASON_CHARS: usize = 256;
const MAX_SUCCESS_RESPONSE_BYTES: usize = 1024 * 1024;
const V3_QUEUE_PREFIX: &str = "pinata-v3:queue:";
pub(crate) const V3_FILE_PREFIX: &str = "pinata-v3:file:";
const REUSED_PREFIX: &str = "pinata:reused:";
const LEGACY_REQUEST_ID_PREFIX: &str = "pinata-legacy:cid:";

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
    base_url: Result<Url, ()>,
    upload_base_url: Result<Url, ()>,
    token: SecretToken,
    http: Client,
    upload_http: Client,
    upload_idle: Duration,
    api: PinataApi,
    strategy: PinataStrategy,
    kubo: Option<KuboClient>,
    psa: Option<PsaClient>,
}

impl PinataClient {
    pub fn new(
        name: String,
        endpoint: String,
        options: PinataProviderOptions,
        kubo: Option<KuboClient>,
        token: SecretToken,
    ) -> Self {
        let http = Client::builder()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("default Pinata HTTP client must build");
        // Streaming uploads must not inherit a whole-request deadline; large objects
        // legitimately outlive it, so only the connect phase is bounded.
        let upload_http = Client::builder()
            .retry(reqwest::retry::never())
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
        let endpoint = normalize_base(endpoint);
        let psa = endpoint
            .ends_with("/psa")
            .then(|| PsaClient::new(name.clone(), endpoint.clone(), token.clone()));
        Self {
            name,
            base_url: parse_base_url(&endpoint),
            upload_base_url: parse_base_url(
                options
                    .upload_endpoint
                    .as_deref()
                    .unwrap_or(PINATA_UPLOAD_BASE_URL),
            ),
            token,
            http,
            upload_http,
            upload_idle: DEFAULT_TIMEOUT,
            api: options.api,
            strategy: options.strategy,
            kubo,
            psa,
        }
    }

    #[cfg(test)]
    pub(crate) fn base_url(&self) -> &str {
        self.base_url.as_ref().map(Url::as_str).unwrap_or_default()
    }

    fn validated_base_url(&self) -> Result<&Url, ProviderError> {
        self.base_url
            .as_ref()
            .map_err(|_| protocol_error("provider endpoint is invalid"))
    }

    fn validated_upload_base_url(&self) -> Result<&Url, ProviderError> {
        self.upload_base_url
            .as_ref()
            .map_err(|_| protocol_error("provider upload endpoint is invalid"))
    }

    fn pin_by_cid_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_base_url()?.clone();
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

    fn files_url(&self, cid: &str, page_token: Option<&str>) -> Result<Url, ProviderError> {
        let mut url = self.validated_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["files", "public"]);
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", "100").append_pair("cid", cid);
            if let Some(page_token) = page_token {
                query.append_pair("pageToken", page_token);
            }
        }
        Ok(url)
    }

    fn file_url(&self, provider_id: &str) -> Result<Url, ProviderError> {
        self.validate_provider_field(provider_id)?;
        let mut url = self.validated_base_url()?.clone();
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
        let mut url = self.validated_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "pinByHash"]);
        Ok(url)
    }

    fn legacy_pin_file_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .extend(["pinning", "pinFileToIPFS"]);
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

    async fn completed_file_id(
        &self,
        cid: &str,
        metadata: &BTreeMap<String, String>,
    ) -> Result<String, ProviderError> {
        if !metadata
            .get("gateway_job_id")
            .is_some_and(|id| !id.is_empty())
        {
            return Err(protocol_error(
                "queue handle has no file correlation metadata",
            ));
        }
        // A queue request can disappear when it becomes a file. Only its own
        // submission metadata can authorize replacing the queue ID with a file ID.
        let mut page_token = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut expected_count = None;
        let mut file_count = 0;
        let mut file_id = None;
        for _ in 0..10 {
            let response = self
                .execute(
                    self.http.get(self.files_url(cid, page_token.as_deref())?),
                    Operation::Unpin,
                )
                .await?;
            let response = self.require_success(response, Operation::Unpin).await?;
            let page: PinataData<PinataFilesPage> = self.decode(response, Operation::Unpin).await?;
            check_page_count(&mut expected_count, page.data.count)?;
            file_count += page.data.files.len();
            for file in page.data.files {
                if file.cid != cid {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                if metadata_contains(&file.keyvalues, metadata)
                    && file_id.replace(file.id).is_some()
                {
                    return Err(protocol_error("multiple files match the pin request"));
                }
            }
            page_token = page.data.next_page_token.filter(|token| !token.is_empty());
            if page_token
                .as_ref()
                .is_some_and(|token| !seen.insert(token.clone()))
            {
                return Err(protocol_error("provider returned a pagination cycle"));
            }
            if page_token.is_none() {
                if expected_count.is_some_and(|count| count != file_count) {
                    return Err(protocol_error("provider returned an incomplete file list"));
                }
                return file_id.ok_or_else(|| ProviderError {
                    class: ProviderErrorClass::Transient,
                    message: "provider has no file bound to the queue handle".to_owned(),
                    retry_after: None,
                });
            }
        }
        Err(protocol_error("provider returned an incomplete file list"))
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
        let job = response.data;
        self.validate_provider_field(&job.id)?;
        self.validate_provider_field(&job.cid)?;
        if job.cid != request.cid {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        let (status, failure_reason) = self.normalize_status(&job.status)?;
        Ok(RemotePin {
            request_id: encode_queue_handle(&job.id, &job.cid, &request.metadata),
            cid: job.cid,
            status,
            raw_status: job.status,
            failure_reason,
        })
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
        self.remote_from_legacy_pin_response(pinned, &request.cid)
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
            if uploaded.is_duplicate == Some(false) {
                // Cleanup must not mask the CID mismatch or remove an existing file.
                let _ = self.delete_file(&uploaded.id).await;
            }
            return Err(upload_correlation_error());
        }
        let prefix = if uploaded.is_duplicate == Some(true) {
            REUSED_PREFIX
        } else {
            V3_FILE_PREFIX
        };
        Ok(RemotePin {
            request_id: format!("{prefix}{}", URL_SAFE_NO_PAD.encode(uploaded.id)),
            cid: uploaded.cid,
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".to_owned(),
            failure_reason: None,
        })
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
            return Err(upload_correlation_error());
        }
        self.remote_from_legacy_pin_response(pinned, &request.cid)
    }

    async fn delete_file(&self, provider_id: &str) -> Result<(), ProviderError> {
        let response = self
            .execute(
                self.http.delete(self.file_url(provider_id)?),
                Operation::Unpin,
            )
            .await?;
        if response.status() != StatusCode::NOT_FOUND {
            self.require_success(response, Operation::Unpin).await?;
        }
        Ok(())
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

    fn remote_from_legacy_pin_response(
        &self,
        pinned: LegacyPinResponse,
        expected_cid: &str,
    ) -> Result<RemotePin, ProviderError> {
        self.validate_provider_field(&pinned.ipfs_hash)?;
        if pinned.ipfs_hash != expected_cid {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        let prefix = if pinned.is_duplicate {
            REUSED_PREFIX
        } else {
            LEGACY_REQUEST_ID_PREFIX
        };
        let (status, failure_reason) = self.normalize_status(&pinned.status)?;
        Ok(RemotePin {
            request_id: format!("{prefix}{}", URL_SAFE_NO_PAD.encode(&pinned.ipfs_hash)),
            cid: pinned.ipfs_hash,
            status,
            raw_status: pinned.status,
            failure_reason,
        })
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

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        if let Some(psa) = &self.psa {
            return psa.submit(request).await;
        }
        match (self.api, self.strategy) {
            (PinataApi::V3, PinataStrategy::Cid) => self.submit_v3_cid(request).await,
            (PinataApi::Legacy, PinataStrategy::Cid) => self.submit_legacy_cid(request).await,
            (PinataApi::V3, PinataStrategy::Upload) => self.submit_v3_upload(request).await,
            (PinataApi::Legacy, PinataStrategy::Upload) => self.submit_legacy_upload(request).await,
        }
    }

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        if let Some(psa) = &self.psa {
            return psa.unpin(request_id).await;
        }
        if request_id.starts_with(REUSED_PREFIX) {
            return Ok(());
        }
        if self.api == PinataApi::Legacy {
            let encoded = request_id
                .strip_prefix(LEGACY_REQUEST_ID_PREFIX)
                .ok_or_else(|| protocol_error("invalid legacy CID handle"))?;
            let cid = decode_request_component(encoded)?;
            self.validate_provider_field(&cid)?;
            let mut url = self.validated_base_url()?.clone();
            url.path_segments_mut()
                .map_err(|_| protocol_error("provider endpoint is invalid"))?
                .extend(["pinning", "unpin", &cid]);
            let response = self
                .execute(self.http.delete(url), Operation::Unpin)
                .await?;
            if response.status() != StatusCode::NOT_FOUND {
                self.require_success(response, Operation::Unpin).await?;
            }
            return Ok(());
        }
        if let Some(encoded) = request_id.strip_prefix(V3_FILE_PREFIX) {
            return self.delete_file(&decode_request_component(encoded)?).await;
        }
        let queue = request_id
            .strip_prefix(V3_QUEUE_PREFIX)
            .ok_or_else(|| protocol_error("provider request ID is invalid"))?;
        let (id, cid, metadata) = decode_queue_handle(queue)?;
        self.validate_provider_field(&cid)?;
        let response = self
            .execute(
                self.http.delete(self.pin_by_cid_request_url(&id)?),
                Operation::Unpin,
            )
            .await?;
        if response.status() != StatusCode::NOT_FOUND {
            self.require_success(response, Operation::Unpin).await?;
            return Ok(());
        }
        let file_id = self.completed_file_id(&cid, &metadata).await?;
        self.delete_file(&file_id).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Submit,
    Unpin,
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
    is_duplicate: Option<bool>,
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
    #[serde(rename = "isDuplicate", alias = "is_duplicate", default)]
    is_duplicate: bool,
    #[serde(default = "pinned_status")]
    status: String,
}

fn pinned_status() -> String {
    "pinned".to_owned()
}

fn encode_queue_handle(
    provider_id: &str,
    cid: &str,
    metadata: &BTreeMap<String, String>,
) -> String {
    let metadata = serde_json::to_vec(metadata).expect("Pinata metadata must serialize");
    format!(
        "{V3_QUEUE_PREFIX}{}:{}:{}",
        URL_SAFE_NO_PAD.encode(provider_id),
        URL_SAFE_NO_PAD.encode(cid),
        URL_SAFE_NO_PAD.encode(metadata)
    )
}

fn decode_queue_handle(
    encoded: &str,
) -> Result<(String, String, BTreeMap<String, String>), ProviderError> {
    let mut parts = encoded.split(':');
    let (Some(id), Some(cid), Some(metadata), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(protocol_error("provider request ID is invalid"));
    };
    let metadata = decode_request_component(metadata)?;
    let metadata = serde_json::from_str(&metadata)
        .map_err(|_| protocol_error("provider request ID is invalid"))?;
    Ok((
        decode_request_component(id)?,
        decode_request_component(cid)?,
        metadata,
    ))
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

fn metadata_contains(
    actual: &BTreeMap<String, String>,
    expected: &BTreeMap<String, String>,
) -> bool {
    expected
        .iter()
        .all(|(key, value)| actual.get(key) == Some(value))
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
            pinata::{
                PINATA_BASE_URL, build_pinata, build_pinata_with_options, encode_queue_handle,
            },
            provider::{PinningProvider, ProviderErrorClass, RemotePinStatus, SubmitPin},
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
    async fn configured_psa_endpoint_uses_exact_request_acks() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/psa/pins"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({
                "requestid": "request-7", "status": "queued", "pin": {"cid": "bafy-target"},
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/psa/pins/request-7"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let provider = build_pinata(
            "pinata".into(),
            test_token(TOKEN),
            Some(format!("{}/psa/", server.uri())),
        );
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".into(),
                name: "bucket/key".into(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(remote.status, RemotePinStatus::Queued);
        provider.unpin(&remote.request_id).await.unwrap();
        assert_eq!(requests(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn legacy_cid_uses_pin_by_hash_without_polling() {
        let server = MockServer::start().await;
        Mock::given(method("POST")).and(path("/pinning/pinByHash"))
            .and(wiremock::matchers::body_json(json!({
                "hashToPin": "bafy-target", "pinataMetadata": {"name": "bucket/key", "keyvalues": {}},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ipfsHash": "bafy-target", "id": "request-7", "status": "prechecking",
            })))
            .expect(1).mount(&server).await;
        let provider = build_pinata_with_options(
            "pinata".into(),
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
                cid: "bafy-target".into(),
                name: "bucket/key".into(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(remote.status, RemotePinStatus::Pinning);
        assert_eq!(requests(&server).await.len(), 1);
    }

    #[tokio::test]
    async fn missing_queue_never_deletes_an_unrelated_file_of_the_same_cid() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/pin_by_cid/request-7"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {
                    "files": [{
                        "id": "wrong-file-id",
                        "cid": "bafy-target",
                        "keyvalues": {
                            "gateway_job_id": "job-7",
                            "gateway_lease_id": "other-lease",
                        }
                    }]
                },
            })))
            .mount(&server)
            .await;

        let error = provider(&server)
            .unpin(&encode_queue_handle(
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
        let received = requests(&server).await;
        assert_eq!(received.len(), 2);
        assert_eq!(received[1].method.as_str(), "GET");
        assert_eq!(received[1].url.path(), "/v3/files/public");
    }

    #[tokio::test]
    async fn completed_queue_is_unpinned_by_its_correlated_file_id() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": {"id": "queue-1", "cid": "bafy-target", "status": "prechecking"},
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/pin_by_cid/queue-1"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/files/public"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"data":{"files":[
                    {"id":"other","cid":"bafy-target","keyvalues":{}},
                    {"id":"ours","cid":"bafy-target","keyvalues":{"gateway_job_id":"job"}}
                ]}})),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/ours"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        let provider = provider(&server);
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".into(),
                name: "bucket/key".into(),
                metadata: BTreeMap::from([("gateway_job_id".into(), "job".into())]),
            })
            .await
            .unwrap();
        provider.unpin(&remote.request_id).await.unwrap();
        assert_eq!(requests(&server).await.len(), 4);
    }

    #[tokio::test]
    async fn v3_upload_streams_kubo_content_and_deletes_the_exact_file() {
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
                    "cid": "bafy-target",
                    "is_duplicate": false,
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/file-7"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
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
        provider.unpin(&remote.request_id).await.unwrap();
        assert_eq!(requests(&server).await.len(), 2);
    }

    #[tokio::test]
    async fn legacy_upload_ack_deletes_by_returned_cid_without_inspection() {
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
        Mock::given(method("DELETE"))
            .and(path("/pinning/unpin/bafy-target"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;
        provider.unpin(&remote.request_id).await.unwrap();

        assert_eq!(remote.status, RemotePinStatus::Pinned);
        assert!(
            remote
                .request_id
                .starts_with(super::LEGACY_REQUEST_ID_PREFIX)
        );
        assert_eq!(requests(&server).await.len(), 2);
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
                    "is_duplicate": false,
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
    async fn legacy_upload_cid_mismatch_leaves_the_shared_pin_untouched() {
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
            0
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
                "data": { "id": "file-7", "cid": "bafy-different", "is_duplicate": false }
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
    async fn v3_submit_ack_supplies_the_exact_queue_cancel_handle() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .and(wiremock::matchers::body_json(json!({
                "cid": "bafy-target", "name": "bucket/key",
                "keyvalues": {"gateway_job_id": "job-7"},
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": job("request-7", "prechecking", "bafy-target"),
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/files/public/pin_by_cid/request-7"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1)
            .mount(&server)
            .await;

        let provider = provider(&server);
        let remote = provider
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
            })
            .await
            .unwrap();
        assert_eq!(remote.status, RemotePinStatus::Pinning);
        provider.unpin(&remote.request_id).await.unwrap();
        let received = requests(&server).await;
        assert_eq!(received.len(), 2);
        assert!(received.iter().all(|request| {
            request
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                == Some("Bearer provider-token")
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
    async fn submit_timeout_is_ambiguous_and_does_not_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/files/public/pin_by_cid"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(300))
                    .set_body_json(json!({ "data": job("request-7", "searching", "bafy-target") })),
            )
            .mount(&server)
            .await;
        let fast_http = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
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
        assert_eq!(requests(&server).await.len(), 1);
    }
}
