//! Minimal SigV4 sender for the ignored real lifecycle-transition test target.

use std::collections::BTreeMap;

use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use http::{HeaderMap, HeaderValue, header};
use sha2::{Digest, Sha256};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
type HmacSha256 = Hmac<Sha256>;

#[allow(clippy::too_many_arguments)]
pub async fn send_sigv4(
    method: reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    extra_headers: HeaderMap,
    secret_key: &str,
) -> reqwest::Response {
    let endpoint = normalize_endpoint(endpoint);
    let canonical_uri = canonical_uri(bucket, key);
    let canonical_query = canonical_query(
        query
            .iter()
            .map(|&(name, value)| (name.to_owned(), value.to_owned())),
    );
    let payload_hash = hex::encode(Sha256::digest(&body));
    let headers = signed_headers(
        &method,
        &canonical_uri,
        &canonical_query,
        &endpoint.authority,
        &payload_hash,
        extra_headers,
        secret_key,
        Utc::now(),
    );
    let url = request_url(&endpoint, &canonical_uri, &canonical_query);
    reqwest::Client::new()
        .request(method, url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .expect("send real transition SigV4 request")
}

struct Endpoint {
    base_url: String,
    authority: String,
}

fn normalize_endpoint(endpoint: &str) -> Endpoint {
    let uri: http::Uri = endpoint
        .trim()
        .parse()
        .expect("endpoint must be a valid absolute HTTP URI");
    let scheme = uri
        .scheme_str()
        .expect("endpoint must include an HTTP scheme")
        .to_ascii_lowercase();
    assert!(matches!(scheme.as_str(), "http" | "https"));
    let authority = uri
        .authority()
        .expect("endpoint must include an authority")
        .as_str()
        .to_ascii_lowercase();
    Endpoint {
        base_url: format!("{scheme}://{authority}"),
        authority,
    }
}

fn request_url(endpoint: &Endpoint, canonical_uri: &str, canonical_query: &str) -> String {
    if canonical_query.is_empty() {
        format!("{}{canonical_uri}", endpoint.base_url)
    } else {
        format!("{}{canonical_uri}?{canonical_query}", endpoint.base_url)
    }
}

fn canonical_uri(bucket: &str, key: &str) -> String {
    let bucket = rfc3986_encode(bucket);
    if key.is_empty() {
        return format!("/{bucket}");
    }
    let key = key
        .split('/')
        .map(rfc3986_encode)
        .collect::<Vec<_>>()
        .join("/");
    format!("/{bucket}/{key}")
}

fn canonical_query<I>(query: I) -> String
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut encoded = query
        .into_iter()
        .map(|(name, value)| (rfc3986_encode(&name), rfc3986_encode(&value)))
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn rfc3986_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

#[allow(clippy::too_many_arguments)]
fn signed_headers(
    method: &reqwest::Method,
    canonical_uri: &str,
    canonical_query: &str,
    authority: &str,
    payload_hash: &str,
    mut headers: HeaderMap,
    secret_key: &str,
    now: chrono::DateTime<Utc>,
) -> HeaderMap {
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    headers.insert(header::HOST, HeaderValue::try_from(authority).unwrap());
    headers.insert("x-amz-date", HeaderValue::try_from(&timestamp).unwrap());
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::try_from(payload_hash).unwrap(),
    );
    let (canonical_headers, signed_header_names) = canonicalize_headers(&headers);
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_header_names}\n{payload_hash}"
    );
    let scope = format!("{date}/{REGION}/{SERVICE}/{TERMINATOR}");
    let string_to_sign = format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, REGION.as_bytes());
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes());
    let signing_key = hmac_sha256(&service_key, TERMINATOR.as_bytes());
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::try_from(format!(
            "{ALGORITHM} Credential=test/{scope}, SignedHeaders={signed_header_names}, Signature={signature}"
        ))
        .unwrap(),
    );
    headers
}

fn canonicalize_headers(headers: &HeaderMap) -> (String, String) {
    let mut canonical = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers {
        canonical
            .entry(name.as_str().to_ascii_lowercase())
            .or_default()
            .push(
                value
                    .to_str()
                    .expect("SigV4 headers are ASCII")
                    .split_ascii_whitespace()
                    .collect::<Vec<_>>()
                    .join(" "),
            );
    }
    let names = canonical.keys().cloned().collect::<Vec<_>>().join(";");
    let values = canonical
        .into_iter()
        .map(|(name, values)| format!("{name}:{}\n", values.join(",")))
        .collect();
    (values, names)
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}
