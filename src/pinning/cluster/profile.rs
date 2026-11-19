use super::error::{ClusterFailureCode, ClusterWriteFailure};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::header::HeaderValue;
use std::fmt;

fn invalid() -> ClusterWriteFailure {
    ClusterWriteFailure::new(ClusterFailureCode::InvalidConfiguration)
}

/// Administrator-declared pinset identity, independent of endpoint and credentials.
/// This identifies a logical pinset, NOT a physical failure domain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterPinset {
    pub backend: String,
    pub scope: String,
}

impl ClusterPinset {
    pub fn new(backend: &str, scope: &str) -> Result<Self, ClusterWriteFailure> {
        if [backend, scope].iter().any(|v| {
            v.is_empty()
                || v.len() > 128
                || !v
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        }) {
            return Err(invalid());
        }
        Ok(Self {
            backend: backend.into(),
            scope: scope.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterResourceKey {
    pub pinset: ClusterPinset,
    /// Canonical CIDv1, preserving codec and multihash (CIDv0 aliases collapse).
    pub cid: String,
}

#[derive(Clone, PartialEq, Eq)]
pub enum ClusterAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

impl fmt::Debug for ClusterAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterAuth([REDACTED])")
    }
}

impl ClusterAuth {
    pub(crate) fn header(&self) -> Result<HeaderValue, ClusterWriteFailure> {
        let text = match self {
            Self::Bearer(token)
                if !token.is_empty()
                    && token.len() <= 8192
                    && token.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                format!("Bearer {token}")
            }
            Self::Basic { username, password }
                if !username.contains(':')
                    && !username.contains(['\r', '\n'])
                    && !password.contains(['\r', '\n'])
                    && username.len() + password.len() <= 8192 =>
            {
                format!(
                    "Basic {}",
                    STANDARD.encode(format!("{username}:{password}"))
                )
            }
            _ => return Err(invalid()),
        };
        let mut value = HeaderValue::from_str(&text).map_err(|_| invalid())?;
        value.set_sensitive(true);
        Ok(value)
    }
}
