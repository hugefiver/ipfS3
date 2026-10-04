use super::error::error;
use crate::pinning::provider::{ProviderError, ProviderErrorClass};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::header::HeaderValue;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcProfile {
    Kubo,
    Filebase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcStrategy {
    Cid,
    Upload,
    Car,
}

/// Secrets are never included in Debug or transport errors.
#[derive(Clone)]
pub enum RpcAuth {
    Bearer(String),
    Basic { username: String, password: String },
}

impl fmt::Debug for RpcAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bearer(_) => f.write_str("Bearer([REDACTED])"),
            Self::Basic { .. } => f.write_str("Basic([REDACTED])"),
        }
    }
}

impl RpcAuth {
    pub(super) fn header(&self) -> Result<HeaderValue, ProviderError> {
        let value = match self {
            Self::Bearer(token)
                if !token.is_empty() && token.bytes().all(|b| b.is_ascii_graphic()) =>
            {
                format!("Bearer {token}")
            }
            Self::Basic { username, password }
                if !username.is_empty()
                    && !password.is_empty()
                    && !username.contains(':')
                    && !username.chars().any(char::is_control)
                    && !password.chars().any(char::is_control) =>
            {
                format!(
                    "Basic {}",
                    STANDARD.encode(format!("{username}:{password}"))
                )
            }
            _ => {
                return Err(error(
                    ProviderErrorClass::InvalidInput,
                    "invalid RPC authentication configuration",
                ));
            }
        };
        let mut value = HeaderValue::from_str(&value).map_err(|_| {
            error(
                ProviderErrorClass::InvalidInput,
                "invalid RPC authentication configuration",
            )
        })?;
        value.set_sensitive(true);
        Ok(value)
    }
}

impl RpcProfile {
    pub fn api_profile(self) -> &'static str {
        match self {
            Self::Kubo => "kubo",
            Self::Filebase => "filebase-rpc",
        }
    }
    pub fn validate(self, strategy: RpcStrategy) -> Result<(), ProviderError> {
        if self == Self::Filebase && strategy != RpcStrategy::Upload {
            return Err(error(
                ProviderErrorClass::InvalidInput,
                "Filebase RPC supports upload only; CAR remains experimental",
            ));
        }
        Ok(())
    }

    pub(super) fn id(self) -> &'static str {
        match self {
            Self::Kubo => "kubo",
            Self::Filebase => "filebase",
        }
    }
}

impl RpcStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cid => "cid",
            Self::Upload => "upload",
            Self::Car => "car",
        }
    }
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "cid" => Some(Self::Cid),
            "upload" => Some(Self::Upload),
            "car" => Some(Self::Car),
            _ => None,
        }
    }
}
