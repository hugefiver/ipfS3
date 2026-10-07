use super::super::{ClusterRestProfile, network};
use super::{ClusterFailureCode as Code, ClusterTransferLimits, ClusterWriteFailure as Failure};
use bytes::Bytes;
use http_body_util::BodyExt as _;
use reqwest::{Client, Method, RequestBuilder, Response};

pub(super) struct Http {
    client: Client,
    pub profile: ClusterRestProfile,
    pub limits: ClusterTransferLimits,
}
impl Http {
    pub fn new(
        profile: ClusterRestProfile,
        limits: ClusterTransferLimits,
    ) -> Result<Self, Failure> {
        let client = network::client(
            &profile.base,
            profile.network_policy,
            limits.connect_timeout,
            None,
        )
        .map_err(|_| Failure::new(Code::InvalidConfiguration))?;
        Ok(Self {
            client,
            profile,
            limits,
        })
    }
    pub fn request(&self, method: Method, path: &str, query: &[(&str, &str)]) -> RequestBuilder {
        let mut url = self.profile.base.clone();
        url.set_path(&format!("{}{path}", url.path().trim_end_matches('/')));
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let mut request = self
            .client
            .request(method, url)
            .header(reqwest::header::CACHE_CONTROL, "no-cache, no-store")
            .header(reqwest::header::TE, "trailers");
        if let Some(auth) = &self.profile.authorization {
            request = request.header(reqwest::header::AUTHORIZATION, auth.clone());
        }
        request
    }
    pub fn rpc(&self, command: &str, query: &[(&str, &str)]) -> RequestBuilder {
        let prefix = if self
            .profile
            .base
            .path()
            .trim_end_matches('/')
            .ends_with("/api/v0")
        {
            ""
        } else {
            "/api/v0"
        };
        self.request(Method::POST, &format!("{prefix}/{command}"), query)
    }
    pub async fn source(&self, command: &str, query: &[(&str, &str)]) -> Result<Response, Failure> {
        let response =
            tokio::time::timeout(self.limits.idle_timeout, self.rpc(command, query).send())
                .await
                .map_err(|_| Failure::new(Code::IdleTimeout))?
                .map_err(|_| Failure::new(Code::SourceFailure))?;
        check(&response)?;
        Ok(response)
    }
    pub async fn execute(&self, request: reqwest::Request) -> Result<Response, Failure> {
        self.client
            .execute(request)
            .await
            .map_err(|_| Failure::new(Code::Transport))
    }
}

pub(super) fn check(response: &Response) -> Result<(), Failure> {
    let status = response.status().as_u16();
    if status != 200 {
        return Err(Failure {
            code: if (300..400).contains(&status) {
                Code::RedirectRejected
            } else {
                Code::HttpStatus
            },
            http_status: Some(status),
        });
    }
    Ok(())
}

pub(super) struct Body {
    inner: reqwest::Body,
    expected: Option<u64>,
    received: u64,
    limit: u64,
    header_error: bool,
}
impl Body {
    pub fn new(response: Response, limit: u64) -> Result<Self, Failure> {
        let header_error = response.headers().contains_key("x-stream-error")
            || response.headers().contains_key("content-range");
        let expected = response.content_length();
        if expected.is_some_and(|length| length > limit) {
            return Err(Failure::new(Code::ResourceLimit));
        }
        Ok(Self {
            inner: response.into(),
            expected,
            received: 0,
            limit,
            header_error,
        })
    }
    pub async fn next(&mut self) -> Result<Option<Bytes>, Failure> {
        loop {
            let Some(frame) = self.inner.frame().await else {
                if self.header_error {
                    return Err(Failure::new(Code::Protocol));
                }
                if self.expected.is_some_and(|n| n != self.received) {
                    return Err(Failure::new(Code::Protocol));
                }
                return Ok(None);
            };
            let frame = frame.map_err(|_| Failure::new(Code::Transport))?;
            match frame.into_data() {
                Ok(data) => {
                    self.received = self
                        .received
                        .checked_add(data.len() as u64)
                        .ok_or_else(|| Failure::new(Code::ResourceLimit))?;
                    if self.received > self.limit {
                        return Err(Failure::new(Code::ResourceLimit));
                    }
                    if !data.is_empty() {
                        return Ok(Some(data));
                    }
                }
                Err(frame) => {
                    let trailers = frame
                        .into_trailers()
                        .map_err(|_| Failure::new(Code::Protocol))?;
                    if trailers.contains_key("x-stream-error") {
                        return Err(Failure::new(Code::Protocol));
                    }
                }
            }
        }
    }
    pub async fn collect(mut self) -> Result<Vec<u8>, Failure> {
        let mut out = Vec::new();
        while let Some(data) = self.next().await? {
            out.extend_from_slice(&data);
        }
        Ok(out)
    }
}
