use std::{collections::BTreeMap, time::Duration};

use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};

use crate::pinning::{
    config::SecretToken,
    provider::{
        FindPin, PinningProvider, ProviderError, ProviderErrorClass, RemotePin, RemotePinStatus,
        SubmitPin,
    },
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FAILURE_REASON_CHARS: usize = 256;
const MAX_SUCCESS_RESPONSE_BYTES: usize = 1024 * 1024;

pub struct PsaClient {
    name: String,
    base_url: Result<Url, ()>,
    token: SecretToken,
    http: Client,
}

impl PsaClient {
    pub fn new(name: String, endpoint: String, token: SecretToken) -> Self {
        let http = Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .expect("default PSA HTTP client must build");
        Self::with_http(name, endpoint, token, http)
    }

    fn with_http(name: String, endpoint: String, token: SecretToken, http: Client) -> Self {
        Self {
            name,
            base_url: parse_base_url(&endpoint),
            token,
            http,
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

    fn pins_url(&self) -> Result<Url, ProviderError> {
        let mut url = self.validated_base_url()?.clone();
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .push("pins");
        Ok(url)
    }

    fn pin_url(&self, request_id: &str) -> Result<Url, ProviderError> {
        self.validate_request_id(request_id)?;
        let mut url = self.pins_url()?;
        url.path_segments_mut()
            .map_err(|_| protocol_error("provider endpoint is invalid"))?
            .push(request_id);
        Ok(url)
    }

    fn find_url(&self, cid: &str, metadata: &str) -> Result<Url, ProviderError> {
        let mut url = self.pins_url()?;
        url.query_pairs_mut()
            .append_pair("cid", cid)
            .append_pair("meta", metadata);
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

    fn require_success(
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
            401 | 403 => ProviderErrorClass::Authentication,
            404 if operation == Operation::Get => ProviderErrorClass::NotFound,
            409 if operation == Operation::Submit => ProviderErrorClass::Ambiguous,
            429 => ProviderErrorClass::RateLimited,
            507 => ProviderErrorClass::Quota,
            _ if status.is_server_error() => ProviderErrorClass::Transient,
            _ if status.is_client_error() => ProviderErrorClass::Terminal,
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

    async fn decode_status(
        &self,
        response: Response,
        operation: Operation,
    ) -> Result<PsaStatus, ProviderError> {
        let body = self.read_success_body(response, operation).await?;
        serde_json::from_slice(&body)
            .map_err(|_| protocol_error("provider returned an invalid success response"))
    }

    async fn decode_list(
        &self,
        response: Response,
        operation: Operation,
    ) -> Result<PsaList, ProviderError> {
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
        while let Some(chunk) = response
            .chunk()
            .await
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

    fn remote_pin(&self, status: PsaStatus) -> Result<RemotePin, ProviderError> {
        self.validate_request_id(&status.requestid)?;
        if self.unsafe_response_field(&status.pin.cid) {
            return Err(protocol_error(
                "provider response contains an unsafe pin field",
            ));
        }

        let normalized = match status.status.as_str() {
            "queued" => RemotePinStatus::Queued,
            "pinning" => RemotePinStatus::Pinning,
            "pinned" => RemotePinStatus::Pinned,
            "failed" => RemotePinStatus::Failed,
            _ => return Err(protocol_error("provider returned an unknown pin status")),
        };
        let failure_reason = (normalized == RemotePinStatus::Failed)
            .then(|| self.failure_reason(&status.info))
            .flatten();

        Ok(RemotePin {
            request_id: status.requestid,
            cid: status.pin.cid,
            status: normalized,
            raw_status: status.status,
            failure_reason,
        })
    }

    fn failure_reason(&self, info: &BTreeMap<String, serde_json::Value>) -> Option<String> {
        ["reason", "error", "message"]
            .into_iter()
            .filter_map(|key| info.get(key).and_then(serde_json::Value::as_str))
            .find_map(|reason| self.bounded_token_free_reason(reason))
    }

    fn bounded_token_free_reason(&self, reason: &str) -> Option<String> {
        if self.unsafe_response_field(reason) {
            return None;
        }

        Some(reason.chars().take(MAX_FAILURE_REASON_CHARS).collect())
    }

    fn transport_error(&self, operation: Operation, error: &reqwest::Error) -> ProviderError {
        let class = if error.is_builder() {
            ProviderErrorClass::Protocol
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

    fn unsafe_response_field(&self, value: &str) -> bool {
        value.trim().is_empty()
            || value.contains(self.token.expose())
            || value.to_ascii_lowercase().contains("bearer ")
    }

    fn validate_request_id(&self, request_id: &str) -> Result<(), ProviderError> {
        if request_id.trim().is_empty()
            || matches!(request_id, "." | "..")
            || request_id
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
            || request_id.contains(self.token.expose())
            || request_id.to_ascii_lowercase().contains("bearer ")
        {
            return Err(protocol_error("provider request ID is invalid"));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl PinningProvider for PsaClient {
    fn name(&self) -> &str {
        &self.name
    }

    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError> {
        let body = PsaPin {
            cid: &request.cid,
            name: &request.name,
            origins: Vec::new(),
            meta: &request.metadata,
        };
        let url = self.pins_url()?;
        let response = self
            .execute(self.http.post(url).json(&body), Operation::Submit)
            .await?;
        let status = self
            .decode_status(
                self.require_success(response, Operation::Submit)?,
                Operation::Submit,
            )
            .await?;
        let remote_pin = self.remote_pin(status)?;
        if remote_pin.cid != request.cid {
            return Err(protocol_error("provider response failed pin correlation"));
        }
        Ok(remote_pin)
    }

    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError> {
        let url = self.pin_url(request_id)?;
        let response = self.execute(self.http.get(url), Operation::Get).await?;
        let status = self
            .decode_status(
                self.require_success(response, Operation::Get)?,
                Operation::Get,
            )
            .await?;
        let remote_pin = self.remote_pin(status)?;
        if remote_pin.request_id != request_id {
            return Err(protocol_error(
                "provider response request ID did not match the requested ID",
            ));
        }
        Ok(remote_pin)
    }

    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError> {
        let metadata = serde_json::to_string(&query.metadata)
            .map_err(|_| protocol_error("pin metadata could not be encoded"))?;
        let response = self
            .execute(
                self.http.get(self.find_url(&query.cid, &metadata)?),
                Operation::Find,
            )
            .await?;
        let response = self.require_success(response, Operation::Find)?;
        let list = self.decode_list(response, Operation::Find).await?;
        let result_count = u64::try_from(list.results.len())
            .map_err(|_| protocol_error("provider returned an incomplete pin list"))?;
        if list.count != result_count {
            return Err(protocol_error("provider returned an incomplete pin list"));
        }
        list.results
            .into_iter()
            .map(|status| {
                if status.pin.cid != query.cid
                    || query
                        .metadata
                        .iter()
                        .any(|(key, value)| status.pin.meta.get(key) != Some(value))
                {
                    return Err(protocol_error("provider response failed pin correlation"));
                }
                self.remote_pin(status)
            })
            .collect()
    }

    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError> {
        let url = self.pin_url(request_id)?;
        let response = self
            .execute(self.http.delete(url), Operation::Unpin)
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        self.require_success(response, Operation::Unpin)?;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Operation {
    Submit,
    Get,
    Find,
    Unpin,
}

#[derive(Serialize)]
struct PsaPin<'a> {
    cid: &'a str,
    name: &'a str,
    origins: Vec<String>,
    meta: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PsaStatus {
    requestid: String,
    status: String,
    pin: PsaResponsePin,
    #[serde(default)]
    info: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct PsaResponsePin {
    cid: String,
    #[serde(default)]
    meta: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PsaList {
    count: u64,
    results: Vec<PsaStatus>,
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

#[cfg(test)]
pub(crate) fn test_token(value: &str) -> SecretToken {
    use crate::{
        config::{PinningConfig, ProviderConfig},
        pinning::config::ValidatedPinningConfig,
    };

    let config = PinningConfig {
        worker_interval: "5s".to_owned(),
        worker_concurrency: 1,
        providers: vec![ProviderConfig {
            name: "test".to_owned(),
            kind: "pinata".to_owned(),
            token_env: Some("TEST_TOKEN".to_owned()),
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority: 0,
            max_bytes: 1,
            max_pins: 1,
            requests_per_second: None,
        }],
        policies: Vec::new(),
    };
    ValidatedPinningConfig::from_raw(&config, |_| Some(value.to_owned()))
        .unwrap()
        .providers
        .into_iter()
        .next()
        .unwrap()
        .token
        .unwrap()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::pinning::{
        config::ProviderKind,
        filebase::build_filebase,
        provider::{FindPin, PinningProvider, ProviderErrorClass, RemotePinStatus, SubmitPin},
    };

    use super::PsaClient;

    const TOKEN: &str = "provider-token";

    fn provider_for(kind: ProviderKind, server: &MockServer) -> PsaClient {
        match kind {
            ProviderKind::Filebase => build_filebase(
                "filebase".to_owned(),
                super::test_token(TOKEN),
                Some(format!("{}/v1/ipfs", server.uri())),
            ),
            ProviderKind::Pinata | ProviderKind::Noop => {
                unreachable!("PSA tests only cover Filebase")
            }
        }
    }

    fn status(request_id: &str, state: &str, cid: &str) -> serde_json::Value {
        status_with_meta(request_id, state, cid, BTreeMap::new())
    }

    fn status_with_meta(
        request_id: &str,
        state: &str,
        cid: &str,
        metadata: BTreeMap<String, String>,
    ) -> serde_json::Value {
        json!({
            "requestid": request_id,
            "status": state,
            "pin": { "cid": cid, "name": "bucket/key", "origins": [], "meta": metadata },
            "info": {},
        })
    }

    async fn requests(server: &MockServer) -> Vec<wiremock::Request> {
        server.received_requests().await.unwrap()
    }

    async fn mount_contract(server: &MockServer, prefix: &str) {
        let pins = format!("{prefix}/pins");
        let request = format!("{pins}/request-7");
        Mock::given(method("POST"))
            .and(path(pins.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-7",
                "queued",
                "bafy-target",
            )))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(request.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-7",
                "pinned",
                "bafy-target",
            )))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(pins.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "count": 1,
                "results": [status_with_meta(
                    "request-7",
                    "pinning",
                    "bafy-target",
                    BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]),
                )],
            })))
            .mount(server)
            .await;
        Mock::given(method("DELETE"))
            .and(path(request))
            .respond_with(ResponseTemplate::new(204))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn filebase_obeys_the_psa_contract() {
        for (kind, prefix) in [(ProviderKind::Filebase, "/v1/ipfs")] {
            let server = MockServer::start().await;
            mount_contract(&server, prefix).await;
            let provider = provider_for(kind, &server);
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
            assert_eq!(submitted.request_id, "request-7");
            assert_eq!(submitted.status, RemotePinStatus::Queued);
            assert_eq!(
                provider.get("request-7").await.unwrap().status,
                RemotePinStatus::Pinned
            );
            assert_eq!(
                provider
                    .find(FindPin::for_job("bafy-target", "job-7"))
                    .await
                    .unwrap()[0]
                    .status,
                RemotePinStatus::Pinning
            );
            provider.unpin("request-7").await.unwrap();

            let received = requests(&server).await;
            assert_eq!(received.len(), 4);
            for request in &received {
                assert_eq!(
                    request
                        .headers
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    "Bearer provider-token"
                );
            }
            let pins = format!("{prefix}/pins");
            let post = received
                .iter()
                .find(|request| request.method.as_str() == "POST" && request.url.path() == pins)
                .unwrap();
            assert_eq!(
                post.headers.get("authorization").unwrap().to_str().unwrap(),
                "Bearer provider-token"
            );
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&post.body).unwrap(),
                json!({
                    "cid": "bafy-target",
                    "name": "bucket/key",
                    "origins": [],
                    "meta": {
                        "gateway_job_id": "job-7",
                        "gateway_lease_id": "lease-7",
                    },
                })
            );
            let list = received
                .iter()
                .find(|request| request.method.as_str() == "GET" && request.url.path() == pins)
                .unwrap();
            let query = list
                .url
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect::<Vec<_>>();
            assert_eq!(
                query,
                vec![
                    ("cid".to_owned(), "bafy-target".to_owned()),
                    (
                        "meta".to_owned(),
                        "{\"gateway_job_id\":\"job-7\"}".to_owned(),
                    ),
                ]
            );
            assert!(received.iter().any(|request| {
                request.method.as_str() == "GET"
                    && request.url.path() == format!("{pins}/request-7")
            }));
            assert!(received.iter().any(|request| {
                request.method.as_str() == "DELETE"
                    && request.url.path() == format!("{pins}/request-7")
            }));
        }
    }

    #[tokio::test]
    async fn preserves_all_supported_statuses_and_rejects_unknown_ones() {
        for state in ["queued", "pinning", "pinned", "failed"] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/pins"))
                .respond_with(ResponseTemplate::new(200).set_body_json(status(
                    "request-7",
                    state,
                    "bafy-target",
                )))
                .mount(&server)
                .await;
            let actual = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap();
            assert_eq!(actual.raw_status, state);
            assert_eq!(
                actual.status,
                match state {
                    "queued" => RemotePinStatus::Queued,
                    "pinning" => RemotePinStatus::Pinning,
                    "pinned" => RemotePinStatus::Pinned,
                    "failed" => RemotePinStatus::Failed,
                    _ => unreachable!(),
                }
            );
        }

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-7",
                "unknown",
                "bafy-target",
            )))
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
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
    async fn malformed_and_missing_request_ids_are_protocol_errors_without_fabrication() {
        for body in [
            "not-json".to_owned(),
            json!({ "status": "queued", "pin": { "cid": "bafy-target" } }).to_string(),
            status("", "queued", "bafy-target").to_string(),
            status("   ", "queued", "bafy-target").to_string(),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/pins"))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .mount(&server)
                .await;
            let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol);
            assert!(!format!("{error:?}").contains("not-json"));
            assert!(!format!("{error:?}").contains(TOKEN));
        }
    }

    #[tokio::test]
    async fn classifies_http_errors_and_hides_provider_bodies() {
        for (status_code, expected, retry_after) in [
            (401, ProviderErrorClass::Authentication, None),
            (403, ProviderErrorClass::Authentication, None),
            (409, ProviderErrorClass::Ambiguous, None),
            (429, ProviderErrorClass::RateLimited, Some("17")),
            (507, ProviderErrorClass::Quota, None),
            (500, ProviderErrorClass::Transient, None),
            (418, ProviderErrorClass::Terminal, None),
        ] {
            let server = MockServer::start().await;
            let mut response = ResponseTemplate::new(status_code)
                .set_body_string("provider-token response-body-must-not-leak");
            if let Some(retry_after) = retry_after {
                response = response.insert_header("retry-after", retry_after);
            }
            Mock::given(method("POST"))
                .and(path("/pins"))
                .respond_with(response)
                .mount(&server)
                .await;
            let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.class, expected, "HTTP {status_code}");
            assert_eq!(
                error.retry_after,
                retry_after.map(|seconds| Duration::from_secs(seconds.parse().unwrap()))
            );
            for rendered in [
                format!("{error:?}"),
                error.to_string(),
                error.message.clone(),
            ] {
                assert!(!rendered.contains(TOKEN));
                assert!(!rendered.contains("response-body-must-not-leak"));
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "not-a-delay")
                    .set_body_string("provider-token"),
            )
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::RateLimited);
        assert_eq!(error.retry_after, None);
    }

    #[tokio::test]
    async fn get_404_is_not_found_and_delete_404_is_idempotent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pins/request-7"))
            .respond_with(ResponseTemplate::new(404).set_body_string("provider-token"))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pins/request-7"))
            .respond_with(ResponseTemplate::new(404).set_body_string("provider-token"))
            .mount(&server)
            .await;
        let provider = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN));
        assert_eq!(
            provider.get("request-7").await.unwrap_err().class,
            ProviderErrorClass::NotFound
        );
        provider.unpin("request-7").await.unwrap();
    }

    #[tokio::test]
    async fn submit_timeout_is_ambiguous_and_other_transport_errors_are_transient() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(status("request-7", "queued", "bafy-target")),
            )
            .mount(&server)
            .await;
        let fast_http = reqwest::Client::builder()
            .timeout(Duration::from_millis(10))
            .build()
            .unwrap();
        let error = PsaClient::with_http(
            "test".to_owned(),
            server.uri(),
            super::test_token(TOKEN),
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

        let error = PsaClient::new(
            "test".to_owned(),
            "http://127.0.0.1:1".to_owned(),
            super::test_token(TOKEN),
        )
        .get("request-7")
        .await
        .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Transient);
        assert!(!format!("{error:?}").contains(TOKEN));
    }

    #[tokio::test]
    async fn failure_reason_is_bounded_and_never_contains_the_bearer_token() {
        let server = MockServer::start().await;
        let mut failed = status("request-7", "failed", "bafy-target");
        failed["info"] = json!({ "reason": "provider-token leaked from upstream" });
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(failed))
            .mount(&server)
            .await;
        let remote = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(remote.failure_reason, None);

        let server = MockServer::start().await;
        let mut failed = status("request-8", "failed", "bafy-target");
        failed["info"] = json!({ "reason": "x".repeat(300) });
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(failed))
            .mount(&server)
            .await;
        let remote = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(remote.failure_reason.unwrap().chars().count(), 256);
    }

    #[tokio::test]
    async fn request_ids_are_one_encoded_path_segment() {
        for (request_id, encoded) in [
            ("part/child", "part%2Fchild"),
            ("../", "..%2F"),
            ("phase?next", "phase%3Fnext"),
            ("fragment#next", "fragment%23next"),
            ("already%2Fencoded", "already%252Fencoded"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_json(status(
                    request_id,
                    "pinned",
                    "bafy-target",
                )))
                .mount(&server)
                .await;
            let provider = PsaClient::new(
                "test".to_owned(),
                format!("{}/psa///", server.uri()),
                super::test_token(TOKEN),
            );

            provider.get(request_id).await.unwrap();

            let received = requests(&server).await;
            assert_eq!(received.len(), 1, "request ID {request_id:?}");
            assert_eq!(
                received[0].url.path(),
                format!("/psa/pins/{encoded}"),
                "request ID {request_id:?}",
            );
            assert_eq!(received[0].url.query(), None, "request ID {request_id:?}");
            assert_eq!(
                received[0].url.fragment(),
                None,
                "request ID {request_id:?}"
            );
        }
    }

    #[tokio::test]
    async fn caller_request_ids_are_validated_without_remote_requests() {
        for request_id in [
            "",
            "   ",
            ".",
            "..",
            "request id",
            "request\u{2003}id",
            "request\tid",
            "request\rid",
            "request\u{001f}id",
            TOKEN,
            "bEaReR upstream-secret",
        ] {
            let server = MockServer::start().await;
            let provider =
                PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN));

            for error in [
                provider.get(request_id).await.unwrap_err(),
                provider.unpin(request_id).await.unwrap_err(),
            ] {
                assert_eq!(error.class, ProviderErrorClass::Protocol, "{request_id:?}");
                for rendered in [format!("{error:?}"), error.to_string(), error.message] {
                    assert!(!rendered.contains(TOKEN));
                    assert!(!rendered.to_ascii_lowercase().contains("bearer "));
                }
            }
            assert!(requests(&server).await.is_empty(), "{request_id:?}");
        }
    }

    #[tokio::test]
    async fn get_requires_the_response_request_id_to_match_the_requested_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pins/request-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-b",
                "pinned",
                "bafy-target",
            )))
            .mount(&server)
            .await;

        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .get("request-a")
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        for rendered in [format!("{error:?}"), error.to_string(), error.message] {
            assert!(!rendered.contains(TOKEN));
            assert!(!rendered.contains("request-b"));
        }
    }

    #[tokio::test]
    async fn invalid_endpoints_and_bearer_builder_errors_are_protocol_without_requests() {
        for endpoint_suffix in ["?unexpected=query", "#unexpected-fragment"] {
            let server = MockServer::start().await;
            let provider = PsaClient::new(
                "test".to_owned(),
                format!("{}/psa{endpoint_suffix}", server.uri()),
                super::test_token(TOKEN),
            );
            let error = provider.get("request-7").await.unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol);
            assert!(!format!("{error:?}").contains(TOKEN));
            assert!(requests(&server).await.is_empty());
        }

        for endpoint in ["mailto:provider@example.test", "not a URL"] {
            let error = PsaClient::new(
                "test".to_owned(),
                endpoint.to_owned(),
                super::test_token(TOKEN),
            )
            .get("request-7")
            .await
            .unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol);
            assert!(!format!("{error:?}").contains(TOKEN));
        }

        let server = MockServer::start().await;
        let invalid_token = "provider-token\r\nforged-header";
        let error = PsaClient::new(
            "test".to_owned(),
            server.uri(),
            super::test_token(invalid_token),
        )
        .get("request-7")
        .await
        .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        for rendered in [format!("{error:?}"), error.to_string(), error.message] {
            assert!(!rendered.contains("provider-token"));
            assert!(!rendered.contains("forged-header"));
        }
        assert!(requests(&server).await.is_empty());
    }

    #[tokio::test]
    async fn unsafe_response_fields_are_rejected_without_leaking_tokens_or_bearers() {
        for (request_id, cid) in [
            (".", "bafy-target"),
            ("..", "bafy-target"),
            ("request\u{2003}id", "bafy-target"),
            ("request\tid", "bafy-target"),
            ("provider-token", "bafy-target"),
            ("Bearer upstream-secret", "bafy-target"),
            ("request-7", "provider-token"),
            ("request-7", "bEaReR upstream-secret"),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/pins/request-7"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(status(request_id, "failed", cid)),
                )
                .mount(&server)
                .await;
            let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .get("request-7")
                .await
                .unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol);
            for rendered in [format!("{error:?}"), error.to_string(), error.message] {
                assert!(!rendered.contains(TOKEN));
                assert!(
                    !rendered
                        .to_ascii_lowercase()
                        .contains("bearer upstream-secret")
                );
            }
        }

        for reason in ["provider-token from upstream", "bEaReR upstream-secret"] {
            let server = MockServer::start().await;
            let mut failed = status("request-7", "failed", "bafy-target");
            failed["info"] = json!({ "reason": reason });
            Mock::given(method("GET"))
                .and(path("/pins/request-7"))
                .respond_with(ResponseTemplate::new(200).set_body_json(failed))
                .mount(&server)
                .await;
            let remote = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .get("request-7")
                .await
                .unwrap();
            assert_eq!(remote.failure_reason, None);
            let rendered = format!("{remote:?}");
            assert!(!rendered.contains(TOKEN));
            assert!(
                !rendered
                    .to_ascii_lowercase()
                    .contains("bearer upstream-secret")
            );
        }
    }

    #[tokio::test]
    async fn reconciliation_results_must_correlate_and_lists_must_be_complete() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-7",
                "pinned",
                "bafy-unrelated",
            )))
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);

        let metadata = BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]);
        for result in [
            status_with_meta("request-7", "pinned", "bafy-unrelated", metadata.clone()),
            status("request-7", "pinned", "bafy-target"),
            status_with_meta(
                "request-7",
                "pinned",
                "bafy-target",
                BTreeMap::from([("gateway_job_id".to_owned(), "wrong-job".to_owned())]),
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/pins"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "count": 1,
                    "results": [result],
                })))
                .mount(&server)
                .await;
            let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .find(FindPin {
                    cid: "bafy-target".to_owned(),
                    metadata: metadata.clone(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol);
        }

        for count in [0_usize, 1, 2] {
            let server = MockServer::start().await;
            let results = (0..count)
                .map(|index| {
                    status_with_meta(
                        &format!("request-{index}"),
                        "pinned",
                        "bafy-target",
                        metadata.clone(),
                    )
                })
                .collect::<Vec<_>>();
            Mock::given(method("GET"))
                .and(path("/pins"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "count": count,
                    "results": results,
                })))
                .mount(&server)
                .await;
            let found = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
                .find(FindPin {
                    cid: "bafy-target".to_owned(),
                    metadata: metadata.clone(),
                })
                .await
                .unwrap();
            assert_eq!(found.len(), count);
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "count": 2,
                "results": [status_with_meta(
                    "request-7",
                    "pinned",
                    "bafy-target",
                    metadata,
                )],
            })))
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
    }

    #[tokio::test]
    async fn success_response_bodies_are_capped_without_leaking_contents() {
        let metadata = BTreeMap::from([("gateway_job_id".to_owned(), "job-7".to_owned())]);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status(
                "request-7",
                "pinned",
                "bafy-target",
            )))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "count": 1,
                "results": [status_with_meta(
                    "request-7",
                    "pinned",
                    "bafy-target",
                    metadata.clone(),
                )],
            })))
            .mount(&server)
            .await;
        let provider = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN));
        assert_eq!(
            provider
                .submit(SubmitPin {
                    cid: "bafy-target".to_owned(),
                    name: "bucket/key".to_owned(),
                    metadata: BTreeMap::new(),
                })
                .await
                .unwrap()
                .request_id,
            "request-7"
        );
        assert_eq!(
            provider
                .find(FindPin {
                    cid: "bafy-target".to_owned(),
                    metadata: metadata.clone(),
                })
                .await
                .unwrap()
                .len(),
            1
        );

        let oversized_padding = format!("{TOKEN} success-body-{}", "x".repeat(1_048_576));
        let server = MockServer::start().await;
        let oversized_status = json!({
            "requestid": "request-7",
            "status": "pinned",
            "pin": { "cid": "bafy-target" },
            "info": {},
            "padding": oversized_padding,
        });
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(oversized_status))
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .submit(SubmitPin {
                cid: "bafy-target".to_owned(),
                name: "bucket/key".to_owned(),
                metadata: BTreeMap::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        for rendered in [format!("{error:?}"), error.to_string(), error.message] {
            assert!(!rendered.contains(TOKEN));
            assert!(!rendered.contains("success-body"));
        }

        let server = MockServer::start().await;
        let oversized_list = json!({
            "count": 0,
            "results": [],
            "padding": format!("{TOKEN} success-body-{}", "x".repeat(1_048_576)),
        });
        Mock::given(method("GET"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(oversized_list))
            .mount(&server)
            .await;
        let error = PsaClient::new("test".to_owned(), server.uri(), super::test_token(TOKEN))
            .find(FindPin::for_job("bafy-target", "job-7"))
            .await
            .unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        for rendered in [format!("{error:?}"), error.to_string(), error.message] {
            assert!(!rendered.contains(TOKEN));
            assert!(!rendered.contains("success-body"));
        }
    }
}
