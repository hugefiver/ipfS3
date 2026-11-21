mod session;
#[cfg(test)]
mod tests;

use std::{collections::HashMap, sync::Arc};

use http::{HeaderMap, Uri};
pub use session::ProcessCredentials;
use session::SessionSigner;

use crate::{config::Config, iam::Policies};

use s3s::S3Result;
use s3s::auth::{S3Auth, SecretKey};

use crate::state::AppState;

#[derive(Clone, Default)]
pub struct GatewayCredentials {
    static_keys: HashMap<String, SecretKey>,
    sessions: Option<SessionSigner>,
    pub policies: Policies,
}

impl From<HashMap<String, SecretKey>> for GatewayCredentials {
    fn from(static_keys: HashMap<String, SecretKey>) -> Self {
        Self {
            static_keys,
            ..Self::default()
        }
    }
}

impl FromIterator<(String, SecretKey)> for GatewayCredentials {
    fn from_iter<T: IntoIterator<Item = (String, SecretKey)>>(iter: T) -> Self {
        HashMap::from_iter(iter).into()
    }
}

impl GatewayCredentials {
    pub fn from_config(cfg: &Config) -> anyhow::Result<Self> {
        let auth = &cfg.auth;
        let sessions = auth
            .session_signing_key
            .as_deref()
            .map(SessionSigner::from_hex)
            .transpose()?;
        let mut static_keys = HashMap::new();
        for credential in &auth.credentials {
            anyhow::ensure!(
                sessions.is_none() || !session::is_temporary_id(&credential.access_key),
                "static credential uses a reserved temporary key ID"
            );
            static_keys.insert(
                credential.access_key.clone(),
                SecretKey::from(credential.secret_key.as_str()),
            );
        }
        for (index, rule) in auth.policy.iter().enumerate() {
            anyhow::ensure!(
                rule.user()
                    .is_none_or(|user| static_keys.contains_key(user)),
                "auth.policy rule {} refers to an unknown user",
                index + 1
            );
        }
        Ok(Self {
            static_keys,
            sessions,
            policies: Policies::new(&auth.policy)?,
        })
    }

    pub(crate) fn requires_session_token(&self, id: &str) -> bool {
        self.sessions.is_some() && session::is_temporary_id(id)
    }

    fn secret_key(&self, id: &str) -> S3Result<SecretKey> {
        if let Some(key) = self.static_keys.get(id) {
            return Ok(key.clone());
        }
        if let Some(signer) = &self.sessions
            && session::is_temporary_id(id)
        {
            return Ok(signer.secret(id).into());
        }
        Err(s3s::s3_error!(InvalidAccessKeyId, "unknown access key"))
    }

    pub fn issue_session(
        &self,
        subject: &str,
        duration_seconds: u32,
    ) -> anyhow::Result<ProcessCredentials> {
        let signer = self
            .sessions
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("session signing is disabled"))?;
        anyhow::ensure!(
            self.static_keys.contains_key(subject),
            "unknown session subject"
        );
        anyhow::ensure!(
            (1..=43_200).contains(&duration_seconds),
            "session duration must be 1 through 43200 seconds"
        );
        let expiration =
            chrono::Utc::now() + chrono::Duration::seconds(i64::from(duration_seconds));
        Ok(signer.issue(subject, expiration))
    }

    /// Resolve only a credential already authenticated by s3s.
    pub fn principal(&self, verified_ak: &str, headers: &HeaderMap, uri: &Uri) -> S3Result<String> {
        Ok(self
            .session_subject(verified_ak, headers, uri)?
            .unwrap_or_else(|| verified_ak.to_owned()))
    }

    /// A configured static ID is already the subject; only sessions need a separate binding.
    pub(crate) fn session_subject(
        &self,
        verified_ak: &str,
        headers: &HeaderMap,
        uri: &Uri,
    ) -> S3Result<Option<String>> {
        if let Some(signer) = &self.sessions
            && session::is_temporary_id(verified_ak)
        {
            let token = session::request_token(headers, uri)?;
            let subject = signer.verify(verified_ak, &token, chrono::Utc::now().timestamp())?;
            if !self.static_keys.contains_key(&subject) {
                return Err(s3s::s3_error!(InvalidToken));
            }
            return Ok(Some(subject));
        }
        if self.static_keys.contains_key(verified_ak) {
            return Ok(None);
        }
        Err(s3s::s3_error!(InvalidAccessKeyId))
    }
}

pub struct GatewayAuth {
    state: Arc<AppState>,
}

impl GatewayAuth {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3Auth for GatewayAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.state.credentials.secret_key(access_key)
    }
}
