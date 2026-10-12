// Reuse the existing signer without changing its frozen API or vectors. Its
// private helpers let this test vary only the authenticated wire spelling.
include!("sigv4.rs");

pub async fn send_encoded_zip_query(
    h: &crate::support::stage9_zip_controls_review::Fixture,
    entry: crate::support::stage9_zip_controls_review::Entry,
    query: &[(&str, &str)],
) -> reqwest::Response {
    use crate::support::{
        decompress::{S3TestEndpoint, legal_single_entry_zip},
        stage9_zip_controls_review::{BUCKET, Entry, KEY},
        stage9_zip_replay_source::URL,
    };
    let mut extra = HeaderMap::new();
    let (method, body) = match entry {
        Entry::ZipPut => (reqwest::Method::PUT, legal_single_entry_zip()),
        Entry::Mpu => (reqwest::Method::POST, Vec::new()),
        Entry::Import => {
            extra.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/xml"),
            );
            (
                reqwest::Method::POST,
                format!("<IPFS3ImportRequest><URL>{URL}</URL></IPFS3ImportRequest>").into_bytes(),
            )
        }
        Entry::Ordinary => panic!("encoded ZIP-entry test requires a ZIP entry"),
    };
    let endpoint = normalize_endpoint(h.endpoint());
    let uri = canonical_uri(BUCKET, KEY);
    let query = canonical_query(
        query
            .iter()
            .map(|&(name, value)| (name.into(), value.into())),
    );
    let headers = signed_headers(
        &method,
        &uri,
        &query,
        &endpoint.authority,
        &sha256_hex(&body),
        extra,
        "test",
        Utc::now(),
    );
    let url =
        request_url(&endpoint, &uri, &query).replacen("decompress-zip=", "decompress%2Dzip=", 1);
    reqwest::Client::new()
        .request(method, url)
        .headers(headers)
        .body(body)
        .send()
        .await
        .unwrap()
}
