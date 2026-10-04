use super::{
    RpcAuth,
    body::ResponseBody,
    error::{error, protocol},
};
use crate::pinning::provider::{ProviderError, ProviderErrorClass};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url, header::HeaderValue};
use std::{sync::Arc, time::Duration};

/// Resolve and authorize the same addresses that the connection will use. Do
/// not pre-check DNS and then let another resolver reconnect to a private IP.
struct PublicResolver;

impl reqwest::dns::Resolve for PublicResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let addresses: Vec<_> = tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !super::config::public_ip(address.ip()))
            {
                return Err(Box::new(std::io::Error::other(
                    "RPC destination is not a public network address",
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

const MAX_CONTROL_BYTES: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RpcTimeouts {
    pub connect: Duration,
    pub control: Duration,
    pub idle: Duration,
}

impl Default for RpcTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(30),
            control: Duration::from_secs(300),
            idle: Duration::from_secs(120),
        }
    }
}

#[derive(Clone)]
pub(super) struct Transport {
    base: Url,
    auth: Option<HeaderValue>,
    pub control: Client,
    pub streaming: Client,
    pub timeouts: RpcTimeouts,
}

pub(super) struct ControlResponse {
    pub status: StatusCode,
    pub bytes: Vec<u8>,
    pub retry_after: Option<Duration>,
}

impl ControlResponse {
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, ProviderError> {
        serde_json::from_slice(&self.bytes).map_err(|_| protocol("invalid RPC response"))
    }
    pub fn check(&self, mutation: bool) -> Result<(), ProviderError> {
        if self.status == StatusCode::OK {
            return Ok(());
        }
        let class = match self.status.as_u16() {
            401 if !mutation => ProviderErrorClass::Authentication,
            403 if !mutation => ProviderErrorClass::UnknownForbidden,
            // An add/import/pin may have partially succeeded. Neither HTTP 400
            // nor a generic auth/throttling response proves no side effect.
            // A proxy may generate those responses after forwarding a write.
            400 if !mutation => ProviderErrorClass::InvalidInput,
            429 if !mutation => ProviderErrorClass::RateLimited,
            500..=599 => ProviderErrorClass::Transient,
            _ => ProviderErrorClass::Protocol,
        };
        Err(ProviderError {
            class,
            message: format!("provider returned HTTP status {}", self.status.as_u16()),
            retry_after: self.retry_after,
        })
    }
}

impl Transport {
    pub fn new(
        endpoint: &str,
        auth: Option<&RpcAuth>,
        timeouts: RpcTimeouts,
    ) -> Result<Self, ProviderError> {
        Self::with_network_policy(endpoint, auth, timeouts, true)
    }

    pub fn with_network_policy(
        endpoint: &str,
        auth: Option<&RpcAuth>,
        timeouts: RpcTimeouts,
        allow_private_network: bool,
    ) -> Result<Self, ProviderError> {
        super::config::validate_endpoint(endpoint, allow_private_network).map_err(|_| {
            error(
                ProviderErrorClass::InvalidInput,
                "invalid or unauthorized administrator RPC endpoint",
            )
        })?;
        if timeouts.connect.is_zero() || timeouts.control.is_zero() || timeouts.idle.is_zero() {
            return Err(error(
                ProviderErrorClass::InvalidInput,
                "RPC timeouts must be positive",
            ));
        }
        let mut base = Url::parse(endpoint).map_err(|_| {
            error(
                ProviderErrorClass::InvalidInput,
                "invalid trusted RPC endpoint",
            )
        })?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(error(
                ProviderErrorClass::InvalidInput,
                "invalid trusted RPC endpoint",
            ));
        }
        let path = base.path().trim_end_matches('/').to_owned();
        base.set_path(&path);
        let builder = || {
            let builder = Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(timeouts.connect);
            if allow_private_network {
                builder
            } else {
                builder.dns_resolver(Arc::new(PublicResolver))
            }
        };
        let control = builder()
            .timeout(timeouts.control)
            .build()
            .map_err(|_| protocol("RPC client initialization failed"))?;
        let streaming = builder()
            .build()
            .map_err(|_| protocol("RPC client initialization failed"))?;
        Ok(Self {
            base,
            auth: auth.map(RpcAuth::header).transpose()?,
            control,
            streaming,
            timeouts,
        })
    }

    pub fn request(
        &self,
        streaming: bool,
        command: &str,
        query: &[(&str, &str)],
    ) -> RequestBuilder {
        let mut url = self.base.clone();
        let base_path = self.base.path().trim_end_matches('/');
        let path = if base_path.ends_with("/api/v0") {
            format!("{base_path}/{command}")
        } else {
            format!("{base_path}/api/v0/{command}")
        };
        url.set_path(&path);
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let mut request = if streaming {
            &self.streaming
        } else {
            &self.control
        }
        .post(url);
        if let Some(auth) = &self.auth {
            request = request.header(reqwest::header::AUTHORIZATION, auth.clone());
        }
        request
    }

    pub async fn control(
        &self,
        command: &str,
        query: &[(&str, &str)],
        mutation: bool,
    ) -> Result<ControlResponse, ProviderError> {
        tokio::time::timeout(self.timeouts.control, async {
            let response = self
                .request(false, command, query)
                .send()
                .await
                .map_err(|e| send_error(e, mutation))?;
            collect_control(response).await
        })
        .await
        .map_err(|_| {
            error(
                ProviderErrorClass::Transient,
                "RPC control request timed out",
            )
        })?
    }

    pub async fn source_response(
        &self,
        command: &str,
        query: &[(&str, &str)],
    ) -> Result<Response, ProviderError> {
        let response = tokio::time::timeout(
            self.timeouts.idle,
            self.request(true, command, query).send(),
        )
        .await
        .map_err(|_| {
            error(
                ProviderErrorClass::Transient,
                "RPC source response timed out",
            )
        })?
        .map_err(|e| send_error(e, false))?;
        if response.status() != StatusCode::OK {
            return Err(protocol("RPC source response failed"));
        }
        Ok(response)
    }
}

pub(super) fn send_error(error_value: reqwest::Error, mutation: bool) -> ProviderError {
    error(
        if mutation && error_value.is_connect() {
            ProviderErrorClass::NotSubmitted
        } else {
            ProviderErrorClass::Transient
        },
        "RPC transport request failed",
    )
}

pub(super) async fn collect_control(response: Response) -> Result<ControlResponse, ProviderError> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs);
    let mut body = ResponseBody::new(response, Some(MAX_CONTROL_BYTES))?;
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(ControlResponse {
        status,
        bytes,
        retry_after,
    })
}
