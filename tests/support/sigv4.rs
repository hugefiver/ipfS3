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
    let payload_hash = sha256_hex(&body);
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
        .expect("send SigV4 request")
}

#[allow(clippy::too_many_arguments)]
pub async fn send_sigv4_chunked_http1(
    method: reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    query: &[(&str, &str)],
    chunks: Vec<bytes::Bytes>,
    mut extra_headers: HeaderMap,
    secret_key: &str,
) -> reqwest::Response {
    let endpoint = normalize_endpoint(endpoint);
    let canonical_uri = canonical_uri(bucket, key);
    let canonical_query = canonical_query(
        query
            .iter()
            .map(|&(name, value)| (name.to_owned(), value.to_owned())),
    );
    let payload_hash = sha256_chunks_hex(&chunks);
    let payload_len = chunks
        .iter()
        .try_fold(0usize, |total, chunk| total.checked_add(chunk.len()))
        .expect("chunked test payload length overflow");
    extra_headers.remove(header::CONTENT_LENGTH);
    extra_headers.insert(
        "x-amz-decoded-content-length",
        HeaderValue::try_from(payload_len.to_string())
            .expect("decoded content length is a valid header"),
    );
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
    let stream =
        futures_util::stream::iter(chunks.into_iter().map(Ok::<bytes::Bytes, std::io::Error>));

    reqwest::Client::builder()
        .http1_only()
        .build()
        .expect("build HTTP/1.1-only SigV4 client")
        .request(method, url)
        .version(reqwest::Version::HTTP_11)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .expect("send chunked SigV4 request")
}

#[allow(clippy::too_many_arguments)]
pub fn presign_sigv4_query(
    method: &reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    custom_query: &[(&str, &str)],
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    expires: u32,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    let endpoint = normalize_endpoint(endpoint);
    let canonical_uri = canonical_uri(bucket, key);
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let scope = credential_scope(&date);
    let mut query: Vec<_> = custom_query
        .iter()
        .map(|&(name, value)| (name.to_owned(), value.to_owned()))
        .collect();
    query.extend([
        ("X-Amz-Algorithm".to_owned(), ALGORITHM.to_owned()),
        (
            "X-Amz-Credential".to_owned(),
            format!("{access_key}/{scope}"),
        ),
        ("X-Amz-Date".to_owned(), timestamp.clone()),
        ("X-Amz-Expires".to_owned(), expires.to_string()),
        ("X-Amz-SignedHeaders".to_owned(), "host".to_owned()),
    ]);
    if let Some(token) = session_token {
        query.push(("X-Amz-Security-Token".to_owned(), token.to_owned()));
    }
    let canonical_query = canonical_query(query);
    let canonical_headers = format!("host:{}\n", endpoint.authority);
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\nhost\nUNSIGNED-PAYLOAD"
    );
    let signature = sign_canonical_request(secret_key, &date, &timestamp, &canonical_request);
    let url = request_url(&endpoint, &canonical_uri, &canonical_query);

    format!("{url}&X-Amz-Signature={signature}")
}

struct Endpoint {
    base_url: String,
    authority: String,
}

fn normalize_endpoint(endpoint: &str) -> Endpoint {
    let endpoint = endpoint.trim();
    let endpoint = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    let uri: http::Uri = endpoint
        .parse()
        .expect("endpoint must be a valid absolute HTTP URI");
    let scheme = uri
        .scheme_str()
        .expect("endpoint must include an HTTP scheme")
        .to_ascii_lowercase();
    assert!(
        matches!(scheme.as_str(), "http" | "https"),
        "endpoint scheme must be HTTP or HTTPS"
    );
    let authority = uri
        .authority()
        .expect("endpoint must include an authority")
        .as_str()
        .to_ascii_lowercase();
    if let Some(path_and_query) = uri.path_and_query() {
        assert!(
            path_and_query.path() == "/" && path_and_query.query().is_none(),
            "endpoint must not include a path or query"
        );
    }

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
    let mut encoded: Vec<_> = query
        .into_iter()
        .map(|(name, value)| (rfc3986_encode(&name), rfc3986_encode(&value)))
        .collect();
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
    headers.insert(
        header::HOST,
        HeaderValue::try_from(authority).expect("endpoint authority is a valid Host header"),
    );
    headers.insert(
        "x-amz-date",
        HeaderValue::try_from(&timestamp).expect("SigV4 timestamp is a valid header"),
    );
    headers.insert(
        "x-amz-content-sha256",
        HeaderValue::try_from(payload_hash).expect("SHA-256 hash is a valid header"),
    );

    let (canonical_headers, signed_headers) = canonicalize_headers(&headers);
    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let scope = credential_scope(&date);
    let signature = sign_canonical_request(secret_key, &date, &timestamp, &canonical_request);
    let authorization = format!(
        "{ALGORITHM} Credential=test/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    headers.insert(
        header::AUTHORIZATION,
        HeaderValue::try_from(authorization).expect("SigV4 authorization is a valid header"),
    );
    headers
}

fn canonicalize_headers(headers: &HeaderMap) -> (String, String) {
    let mut canonical = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in headers {
        canonical
            .entry(name.as_str().to_ascii_lowercase())
            .or_default()
            .push(normalize_header_value(
                value.to_str().expect("SigV4 headers must be valid ASCII"),
            ));
    }

    let signed_headers = canonical.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = canonical
        .into_iter()
        .map(|(name, values)| format!("{name}:{}\n", values.join(",")))
        .collect();
    (canonical_headers, signed_headers)
}

fn normalize_header_value(value: &str) -> String {
    value
        .split(|character: char| character.is_ascii_whitespace())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn credential_scope(date: &str) -> String {
    format!("{date}/{REGION}/{SERVICE}/{TERMINATOR}")
}

fn sign_canonical_request(
    secret_key: &str,
    date: &str,
    timestamp: &str,
    canonical_request: &str,
) -> String {
    let scope = credential_scope(date);
    let string_to_sign = format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let date_key = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, REGION.as_bytes());
    let service_key = hmac_sha256(&region_key, SERVICE.as_bytes());
    let signing_key = hmac_sha256(&service_key, TERMINATOR.as_bytes());

    hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()))
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sha256_chunks_hex(chunks: &[bytes::Bytes]) -> String {
    let mut hasher = Sha256::new();
    for chunk in chunks {
        hasher.update(chunk.as_ref());
    }
    hex::encode(hasher.finalize())
}

#[test]
fn presign_sigv4_query_includes_custom_query_before_signature() {
    let now = chrono::DateTime::parse_from_rfc3339("2026-07-07T12:00:00Z")
        .expect("fixed RFC 3339 timestamp")
        .with_timezone(&chrono::Utc);
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        "http://127.0.0.1:9000",
        "test-bkt",
        "archive.zip",
        &[
            ("decompress-zip", "prefix/nested/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        now,
    );
    let uri: http::Uri = url.parse().expect("presigned URL is a valid URI");
    let query = uri.query().expect("presigned URL query");
    let signature = query
        .split('&')
        .position(|pair| pair.starts_with("X-Amz-Signature="))
        .expect("signature query pair");
    let pairs: Vec<_> = query.split('&').collect();

    for expected in [
        "decompress-zip=prefix%2Fnested%2F",
        "decompress-zip-result=true",
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "X-Amz-Credential=test%2F20260707%2Fus-east-1%2Fs3%2Faws4_request",
        "X-Amz-Date=20260707T120000Z",
        "X-Amz-Expires=900",
        "X-Amz-SignedHeaders=host",
    ] {
        let position = pairs
            .iter()
            .position(|pair| *pair == expected)
            .unwrap_or_else(|| panic!("missing presigned query pair: {expected}"));
        assert!(
            position < signature,
            "{expected} must precede the signature"
        );
    }

    let changed = presign_sigv4_query(
        &reqwest::Method::PUT,
        "http://127.0.0.1:9000",
        "test-bkt",
        "archive.zip",
        &[("decompress-zip", "other/")],
        "test",
        "test",
        None,
        900,
        now,
    );
    assert_ne!(
        query
            .split('&')
            .find(|pair| pair.starts_with("X-Amz-Signature=")),
        changed
            .split('?')
            .nth(1)
            .expect("changed presigned URL query")
            .split('&')
            .find(|pair| pair.starts_with("X-Amz-Signature=")),
        "custom query tuples must affect the signature"
    );
}

#[test]
fn canonical_uri_omits_trailing_slash_for_bucket_operations() {
    assert_eq!(canonical_uri("test-bkt", ""), "/test-bkt");
    assert_eq!(
        canonical_uri("test bkt", "nested/path with space.txt"),
        "/test%20bkt/nested/path%20with%20space.txt"
    );
}
