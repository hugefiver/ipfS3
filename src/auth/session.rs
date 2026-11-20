use std::borrow::Cow;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, SecondsFormat, Utc};
use hmac::{Hmac, KeyInit as _, Mac as _};
use http::{HeaderMap, Uri, header};
use s3s::{S3Error, S3Result};
use serde::Serialize;
use sha2::Sha256;

const ID_PREFIX: &str = "IPS3T1";
const TOKEN_HEADER: &str = "x-amz-security-token";

#[derive(Clone)]
pub(super) struct SessionSigner([u8; 32]);

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ProcessCredentials {
    pub version: u8,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: String,
}

pub(super) fn is_temporary_id(id: &str) -> bool {
    id.strip_prefix(ID_PREFIX)
        .is_some_and(|suffix| suffix.len() == 32 && suffix.bytes().all(|b| b.is_ascii_hexdigit()))
}

impl SessionSigner {
    pub(super) fn from_hex(encoded: &str) -> anyhow::Result<Self> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(encoded, &mut bytes)
            .map_err(|_| anyhow::anyhow!("session signing key must be 32 hex-encoded bytes"))?;
        Ok(Self(bytes))
    }

    fn mac(&self, domain: &[u8], id: &str) -> Hmac<Sha256> {
        let mut mac = Hmac::new_from_slice(&self.0).expect("HMAC accepts a 32-byte key");
        mac.update(domain);
        mac.update(id.as_bytes());
        mac
    }

    pub(super) fn secret(&self, id: &str) -> String {
        URL_SAFE_NO_PAD.encode(
            self.mac(b"ipfs-s3/session-secret/v1\0", id)
                .finalize()
                .into_bytes(),
        )
    }

    fn token_mac(&self, id: &str, payload: &str) -> Hmac<Sha256> {
        let mut mac = self.mac(b"ipfs-s3/session-token/v1\0", id);
        mac.update(b"\0");
        mac.update(payload.as_bytes());
        mac
    }

    pub(super) fn issue(&self, subject: &str, expiration: DateTime<Utc>) -> ProcessCredentials {
        let id = format!("{ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
        let mut claims = expiration.timestamp().to_be_bytes().to_vec();
        claims.extend_from_slice(subject.as_bytes());
        let payload = URL_SAFE_NO_PAD.encode(claims);
        let tag = self.token_mac(&id, &payload).finalize().into_bytes();
        ProcessCredentials {
            version: 1,
            secret_access_key: self.secret(&id),
            access_key_id: id,
            session_token: format!("{payload}.{}", URL_SAFE_NO_PAD.encode(tag)),
            expiration: expiration.to_rfc3339_opts(SecondsFormat::Secs, true),
        }
    }

    pub(super) fn verify(&self, id: &str, token: &str, now: i64) -> S3Result<String> {
        let (payload, tag) = token.split_once('.').ok_or_else(invalid_token)?;
        let tag = URL_SAFE_NO_PAD.decode(tag).map_err(|_| invalid_token())?;
        self.token_mac(id, payload)
            .verify_slice(&tag)
            .map_err(|_| invalid_token())?;
        let claims = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| invalid_token())?;
        let (expiration, subject) = claims.split_first_chunk::<8>().ok_or_else(invalid_token)?;
        if now >= i64::from_be_bytes(*expiration) {
            return Err(s3s::s3_error!(ExpiredToken));
        }
        Ok(std::str::from_utf8(subject)
            .map_err(|_| invalid_token())?
            .to_owned())
    }
}

pub(super) fn request_token<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> S3Result<Cow<'a, str>> {
    let mut query_token = None;
    let mut presigned = false;
    for (name, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        match name.as_ref() {
            "X-Amz-Security-Token" => {
                if query_token.replace(value).is_some() {
                    return Err(invalid_token());
                }
            }
            "X-Amz-Signature" => presigned = true,
            "Signature" | "AWSAccessKeyId" => return Err(invalid_token()),
            _ => {}
        }
    }
    if presigned {
        if headers.contains_key(header::AUTHORIZATION) || headers.contains_key(TOKEN_HEADER) {
            return Err(invalid_token());
        }
        return query_token.ok_or_else(invalid_token);
    }
    if query_token.is_some() {
        return Err(invalid_token());
    }

    // s3s verifies the signature; require the token to be among the fields it covered.
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("AWS4-HMAC-SHA256 "))
        .ok_or_else(invalid_token)?;
    let mut signed = auth
        .split(',')
        .filter_map(|p| p.trim().strip_prefix("SignedHeaders="));
    if !signed
        .next()
        .is_some_and(|s| s.split(';').any(|h| h == TOKEN_HEADER))
        || signed.next().is_some()
    {
        return Err(invalid_token());
    }
    let mut values = headers.get_all(TOKEN_HEADER).iter();
    let token = values
        .next()
        .and_then(|v| v.to_str().ok())
        .ok_or_else(invalid_token)?;
    if values.next().is_some() {
        return Err(invalid_token());
    }
    Ok(Cow::Borrowed(token))
}

fn invalid_token() -> S3Error {
    s3s::s3_error!(InvalidToken, "missing or invalid session token")
}
