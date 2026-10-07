use super::{ClusterRestProfile, RestOperation, VerificationCode, VerificationFailure, network};
use reqwest::{
    Client,
    header::{AUTHORIZATION, CACHE_CONTROL},
};
use serde::de::DeserializeOwned;

pub(super) struct Transport {
    http: Client,
    pub profile: ClusterRestProfile,
}

impl Transport {
    pub fn new(profile: ClusterRestProfile) -> Result<Self, VerificationFailure> {
        let http = network::client(
            &profile.base,
            profile.network_policy,
            profile.limits.connect_timeout,
            Some(profile.limits.total_timeout),
        )
        .map_err(|_| {
            VerificationFailure::new(RestOperation::Configure, VerificationCode::Transport)
        })?;
        Ok(Self { http, profile })
    }

    #[cfg(test)]
    pub(crate) fn with_resolver(
        profile: ClusterRestProfile,
        resolver: std::sync::Arc<dyn reqwest::dns::Resolve>,
    ) -> Result<Self, VerificationFailure> {
        let http = network::client_with_resolver(
            &profile.base,
            profile.network_policy,
            profile.limits.connect_timeout,
            Some(profile.limits.total_timeout),
            resolver,
        )
        .map_err(|_| {
            VerificationFailure::new(RestOperation::Configure, VerificationCode::Transport)
        })?;
        Ok(Self { http, profile })
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        operation: RestOperation,
        path: &str,
        global: bool,
    ) -> Result<T, VerificationFailure> {
        let fail = |code| VerificationFailure::new(operation, code);
        let mut url = self.profile.base.clone();
        url.set_path(&format!(
            "{}{path}",
            self.profile.base.path().trim_end_matches('/')
        ));
        // Only a fixed REST-origin clone, never Url::join with caller-controlled input.
        if global {
            url.query_pairs_mut().append_pair("local", "false");
        }
        let mut request = self
            .http
            .get(url)
            .header(CACHE_CONTROL, "no-cache, no-store");
        if let Some(auth) = &self.profile.authorization {
            request = request.header(AUTHORIZATION, auth.clone());
        }
        let mut response = tokio::time::timeout(self.profile.limits.idle_timeout, request.send())
            .await
            .map_err(|_| fail(VerificationCode::IdleTimeout))?
            .map_err(|_| fail(VerificationCode::Transport))?;
        let status = response.status().as_u16();
        // Error bodies, Location and free error strings are deliberately not read.
        if status != 200 {
            let code = match status {
                300..=399 => VerificationCode::RedirectRejected,
                401 => VerificationCode::Authentication,
                403 => VerificationCode::UnknownForbidden,
                404 => VerificationCode::NotFound,
                429 => VerificationCode::RateLimited,
                _ => VerificationCode::HttpStatus,
            };
            return Err(VerificationFailure {
                operation,
                code,
                http_status: Some(status),
            });
        }
        let max = self.profile.limits.max_body_bytes;
        if response
            .content_length()
            .is_some_and(|len| len > max as u64)
        {
            return Err(fail(VerificationCode::ResponseTooLarge));
        }
        let mut bytes = Vec::new();
        loop {
            let chunk = tokio::time::timeout(self.profile.limits.idle_timeout, response.chunk())
                .await
                .map_err(|_| fail(VerificationCode::IdleTimeout))?
                .map_err(|_| fail(VerificationCode::Transport))?;
            let Some(chunk) = chunk else {
                break;
            };
            if chunk.len() > max.saturating_sub(bytes.len()) {
                return Err(fail(VerificationCode::ResponseTooLarge));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| fail(VerificationCode::MalformedResponse))
    }
}
