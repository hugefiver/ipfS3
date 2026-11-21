mod negotiation;

use super::super::{mount_cat_body, publish_versioned_read_object, valid_sse_c_headers};
use super::{policy_state, signed::serve_object_http};
use crate::config::Config;
use crate::pinning::tags::ObjectTag;
use crate::s3::sigv4::presign_sigv4_query;
use chrono::Utc;
use http::{Method, StatusCode, header};
use reqwest::{Client, RequestBuilder, redirect::Policy};
use wiremock::matchers::{method, path};
use wiremock::{Mock, ResponseTemplate};

const DOWNLOAD_GATEWAY: &str = "X-IPFS3-Download-Gateway";
const REDIRECT_CONFIG: &str = r#"
[download_redirect]
gateway_url = 'https://gateway.example'

[[download_redirect.rules]]
user = 'test-owner'
bucket = 'bucket'
prefix = 'source'

[[download_redirect.rules]]
user = 'test-owner'
bucket = 'bucket'
prefix = 'archive/'
filename_glob = 'report-?.txt'
mode = 'on_request'

[[download_redirect.rules]]
user = 'test-owner'
bucket = 'bucket'
prefix = 'source-proxied/'
mode = 'proxy'
"#;

fn download_request(
    client: &Client,
    endpoint: &str,
    method: Method,
    key: &str,
    query: &[(&str, &str)],
) -> RequestBuilder {
    let url = presign_sigv4_query(
        &method,
        endpoint,
        "bucket",
        key,
        query,
        "test-owner",
        "test",
        None,
        60,
        Utc::now(),
    );
    client.request(method, url)
}

#[tokio::test]
async fn download_redirect_uses_the_authorized_version_and_validates_range() {
    let (state, kubo, historical) = policy_state().await;
    let gateway = kubo.uri();
    let cfg: Config =
        toml::from_str(&REDIRECT_CONFIG.replace("https://gateway.example", &gateway)).unwrap();
    let (endpoint, server) = serve_object_http(&state, cfg.download_redirect).await;
    let client = Client::builder().redirect(Policy::none()).build().unwrap();
    let version = [("versionId", historical.as_str())];
    let location = format!("{gateway}/ipfs/QmPolicyHistorical");

    for range in [None, Some("bytes=1-2")] {
        let mut request = download_request(&client, &endpoint, Method::GET, "source.txt", &version);
        if let Some(range) = range {
            request = request.header(header::RANGE, range);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(response.headers()[header::LOCATION], location.as_str());
        assert!(response.bytes().await.unwrap().is_empty());
    }

    for (query, preference, range, status, code) in [
        (
            &version[..],
            "unknown",
            Some("bytes=4-5"),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
        ),
        (
            &[][..],
            "require",
            None,
            StatusCode::FORBIDDEN,
            "AccessDenied",
        ),
        (
            &version[..],
            "unknown",
            None,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
        ),
    ] {
        let mut request = download_request(&client, &endpoint, Method::GET, "source.txt", query)
            .header(DOWNLOAD_GATEWAY, preference);
        if let Some(range) = range {
            request = request.header(header::RANGE, range);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status, "{code}");
        assert!(!response.headers().contains_key(header::LOCATION), "{code}");
        let xml = response.text().await.unwrap();
        assert!(xml.contains(&format!("<Code>{code}</Code>")), "{xml}");
    }

    let head = download_request(&client, &endpoint, Method::HEAD, "source.txt", &version)
        .header(DOWNLOAD_GATEWAY, "require")
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert!(!head.headers().contains_key(header::LOCATION));
    assert_eq!(head.headers()[header::CONTENT_LENGTH], "4");
    assert_eq!(head.headers()["x-amz-version-id"], historical.as_str());
    assert!(head.bytes().await.unwrap().is_empty());

    Mock::given(method("GET"))
        .and(path("/ipfs/QmPolicyHistorical"))
        .respond_with(ResponseTemplate::new(200).set_body_string("old!"))
        .mount(&kubo)
        .await;
    let followed = download_request(
        &Client::new(),
        &endpoint,
        Method::GET,
        "source.txt",
        &version,
    )
    .send()
    .await
    .unwrap();
    assert_eq!(followed.status(), StatusCode::OK);
    assert_eq!(followed.url().as_str(), location);
    assert_eq!(followed.bytes().await.unwrap().as_ref(), b"old!");

    assert!(
        kubo.received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/cat")
    );

    mount_cat_body(&kubo, "QmPolicyHistorical", b"old!".to_vec()).await;
    let proxied = download_request(&client, &endpoint, Method::GET, "source.txt", &version)
        .header(DOWNLOAD_GATEWAY, "proxy")
        .send()
        .await
        .unwrap();
    assert_eq!(proxied.status(), StatusCode::OK);
    assert!(!proxied.headers().contains_key(header::LOCATION));
    assert_eq!(proxied.bytes().await.unwrap().as_ref(), b"old!");

    let customer_key_read =
        download_request(&client, &endpoint, Method::GET, "source.txt", &version)
            .headers(valid_sse_c_headers())
            .send()
            .await
            .unwrap();
    assert_eq!(customer_key_read.status(), StatusCode::OK);
    assert!(!customer_key_read.headers().contains_key(header::LOCATION));
    assert_eq!(
        customer_key_read.headers()[header::CACHE_CONTROL],
        "no-store"
    );
    assert!(
        customer_key_read
            .headers()
            .get_all(header::VARY)
            .iter()
            .flat_map(|value| value.to_str().unwrap().split(','))
            .all(|field| !field
                .trim()
                .eq_ignore_ascii_case("x-amz-server-side-encryption-customer-key"))
    );
    assert_eq!(customer_key_read.bytes().await.unwrap().as_ref(), b"old!");
    server.abort();
}

#[tokio::test]
async fn download_redirect_preserves_unmatched_encrypted_and_disabled_reads() {
    let (state, kubo, historical) = policy_state().await;
    publish_versioned_read_object(
        &state,
        "unmatched",
        "elsewhere.txt",
        "QmUnmatched",
        false,
        None,
        None,
        vec![ObjectTag::new("owner", "team")],
    )
    .await;
    mount_cat_body(&kubo, "QmUnmatched", b"body".to_vec()).await;

    let object_key = state.master_key.generate_object_key();
    publish_versioned_read_object(
        &state,
        "encrypted",
        "source-encrypted.txt",
        "QmEncrypted",
        true,
        Some(state.master_key.wrap(&object_key).unwrap()),
        None,
        vec![ObjectTag::new("owner", "team")],
    )
    .await;
    let ciphertext =
        crate::crypto::aes_gcm::encrypt_chunk(&object_key, &[0x11; 12], b"safe").unwrap();
    mount_cat_body(&kubo, "QmEncrypted", ciphertext.to_vec()).await;

    let client = Client::builder().redirect(Policy::none()).build().unwrap();
    let enabled: Config = toml::from_str(REDIRECT_CONFIG).unwrap();
    let (endpoint, server) = serve_object_http(&state, enabled.download_redirect).await;

    for (key, body, encryption) in [
        ("elsewhere.txt", b"body", None),
        ("source-encrypted.txt", b"safe", Some("AES256")),
    ] {
        let response = download_request(&client, &endpoint, Method::GET, key, &[])
            .header(DOWNLOAD_GATEWAY, "prefer")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{key}");
        assert!(!response.headers().contains_key(header::LOCATION), "{key}");
        assert_eq!(
            response
                .headers()
                .get("x-amz-server-side-encryption")
                .map(|value| value.to_str().unwrap()),
            encryption,
            "{key}",
        );
        assert_eq!(response.bytes().await.unwrap().as_ref(), body, "{key}");
    }
    server.abort();

    mount_cat_body(&kubo, "QmPolicyHistorical", b"old!".to_vec()).await;
    let (endpoint, server) = serve_object_http(&state, None).await;
    let response = download_request(
        &client,
        &endpoint,
        Method::GET,
        "source.txt",
        &[("versionId", historical.as_str())],
    )
    .send()
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(header::LOCATION));
    assert_eq!(response.bytes().await.unwrap().as_ref(), b"old!");
    server.abort();
}
