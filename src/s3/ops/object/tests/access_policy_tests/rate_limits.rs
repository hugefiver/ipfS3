use super::super::{mount_cat_body, publish_versioned_read_object_with_size, test_state};
use super::signed::serve_object_http_with_rate_limits;
use crate::auth::GatewayCredentials;
use crate::config::Config;
use crate::s3::sigv4::{presign_sigv4_query, send_sigv4};
use crate::state::AppState;
use http::{HeaderMap, Method, StatusCode, header};
use reqwest::{Client, Response, redirect::Policy};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const LIST_V2: &[(&str, &str)] = &[("list-type", "2")];
const PIN_STATUS: &[(&str, &str)] = &[("ipfs3-pin-status", "")];

fn client_on(peer: &str) -> Client {
    Client::builder()
        .no_proxy()
        .local_address(peer.parse::<IpAddr>().unwrap())
        .pool_max_idle_per_host(0)
        .build()
        .unwrap()
}

async fn public_bucket() -> (Arc<AppState>, MockServer) {
    let kubo = MockServer::start().await;
    let state = test_state(kubo.uri()).await;
    crate::store::bucket::create_with_acl(
        state.store.db(),
        "bucket",
        Some("test"),
        Some("public-read"),
    )
    .await
    .unwrap();
    (state, kubo)
}

async fn assert_slow_down(response: Response) {
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    let xml = response.text().await.unwrap();
    assert!(xml.contains("<Code>SlowDown</Code>"), "{xml}");
}

async fn signed_list(endpoint: &str, forwarded: &str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", forwarded.parse().unwrap());
    send_sigv4(
        Method::GET,
        endpoint,
        "bucket",
        "",
        LIST_V2,
        vec![],
        headers,
        "test",
    )
    .await
}

fn presigned_get(
    endpoint: &str,
    key: &str,
    query: &[(&str, &str)],
    access_key: &str,
    secret_key: &str,
    token: Option<&str>,
) -> String {
    presign_sigv4_query(
        &Method::GET,
        endpoint,
        "bucket",
        key,
        query,
        access_key,
        secret_key,
        token,
        60,
        chrono::Utc::now(),
    )
}

#[tokio::test]
async fn signed_and_presigned_requests_share_the_user_quota_across_routes() {
    let (mut state, _kubo) = public_bucket().await;
    let mut config: Config = toml::from_str(
        r#"
        [rate_limits]
        trusted_proxies = ['127.0.0.1']
        [rate_limits.authenticated]
        requests_per_second = 1
        [rate_limits.anonymous]
        requests_per_second = 2
        [[auth.credentials]]
        access_key = 'test'
        secret_key = 'test'
        [[auth.credentials]]
        access_key = 'other'
        secret_key = 'other-secret'
        "#,
    )
    .unwrap();
    config.auth.session_signing_key = Some("ab".repeat(32));
    Arc::get_mut(&mut state).unwrap().credentials =
        GatewayCredentials::from_config(&config).unwrap();
    let session = state.credentials.issue_session("test", 300).unwrap();
    publish_versioned_read_object_with_size(
        &state,
        "pin-status-target",
        "plain",
        "QmPlain",
        4,
        false,
        None,
        None,
        vec![],
    )
    .await;
    let (first, first_server) =
        serve_object_http_with_rate_limits(&state, None, config.rate_limits.clone()).await;
    let (second, second_server) =
        serve_object_http_with_rate_limits(&state, None, config.rate_limits).await;
    let client = Client::new();
    let session_pin_status = |endpoint: &str| {
        client
            .get(presigned_get(
                endpoint,
                "plain",
                PIN_STATUS,
                &session.access_key_id,
                &session.secret_access_key,
                Some(&session.session_token),
            ))
            .header("x-forwarded-for", "198.51.100.2")
    };

    assert_eq!(
        signed_list(&first, "198.51.100.1").await.status(),
        StatusCode::OK
    );
    assert_slow_down(session_pin_status(&first).send().await.unwrap()).await;
    assert_slow_down(
        client
            .get(presigned_get(&first, "", LIST_V2, "test", "test", None))
            .header("x-forwarded-for", "198.51.100.3")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let other = client
        .get(presigned_get(
            &first,
            "",
            LIST_V2,
            "other",
            "other-secret",
            None,
        ))
        .header("x-forwarded-for", "198.51.100.1")
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), StatusCode::OK);

    let anonymous_list = || {
        client
            .get(format!("{first}/bucket?list-type=2"))
            .header("x-forwarded-for", "198.51.100.1")
    };
    for _ in 0..2 {
        assert_eq!(
            anonymous_list().send().await.unwrap().status(),
            StatusCode::OK
        );
    }
    assert_slow_down(anonymous_list().send().await.unwrap()).await;

    let pin_status = session_pin_status(&second).send().await.unwrap();
    assert_eq!(pin_status.status(), StatusCode::OK);
    assert_eq!(
        pin_status.headers()[header::CONTENT_TYPE],
        "application/json"
    );
    assert_slow_down(signed_list(&second, "198.51.100.1").await).await;
    first_server.abort();
    second_server.abort();
}

#[tokio::test]
async fn anonymous_quota_uses_the_peer_ip_without_trusted_proxies() {
    let (state, _kubo) = public_bucket().await;
    let config: Config = toml::from_str(
        r#"
        [rate_limits]
        trusted_proxies = []
        [rate_limits.anonymous]
        requests_per_second = 1
        "#,
    )
    .unwrap();
    let (endpoint, server) =
        serve_object_http_with_rate_limits(&state, None, config.rate_limits).await;
    let first_peer = client_on("127.0.0.1");
    let second_peer = client_on("127.0.0.2");
    let list = |client: &Client, forwarded: &str| {
        client
            .get(format!("{endpoint}/bucket?list-type=2"))
            .header("forwarded", format!("for={forwarded}"))
            .header("x-forwarded-for", forwarded)
            .header("x-real-ip", forwarded)
    };

    let first = list(&first_peer, "198.51.100.1").send().await.unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_slow_down(list(&first_peer, "198.51.100.2").send().await.unwrap()).await;
    let independent = list(&second_peer, "198.51.100.2").send().await.unwrap();
    assert_eq!(independent.status(), StatusCode::OK);
    server.abort();
}

#[tokio::test]
async fn anonymous_quota_resolves_forwarded_addresses_from_trusted_peers() {
    let (state, _kubo) = public_bucket().await;
    let config: Config = toml::from_str(
        r#"
        [rate_limits]
        trusted_proxies = ['127.0.0.1', '203.0.113.0/24']
        [rate_limits.anonymous]
        requests_per_second = 1
        "#,
    )
    .unwrap();
    let (endpoint, server) =
        serve_object_http_with_rate_limits(&state, None, config.rate_limits).await;
    let trusted = client_on("127.0.0.1");
    let untrusted = client_on("127.0.0.2");
    let url = format!("{endpoint}/bucket?list-type=2");
    let ok = StatusCode::OK;
    let limited = StatusCode::SERVICE_UNAVAILABLE;
    let cases: [(&Client, &[&str], Option<&str>, StatusCode); _] = [
        (
            &trusted,
            &["198.51.100.1", "198.51.100.2, 203.0.113.1, 203.0.113.2"],
            Some("198.51.100.6"),
            ok,
        ),
        (
            &trusted,
            &["::ffff:198.51.100.2, ::ffff:203.0.113.1"],
            None,
            limited,
        ),
        (
            &trusted,
            &["198.51.100.1, 198.51.100.3", "203.0.113.1, 203.0.113.2"],
            None,
            ok,
        ),
        (&trusted, &["198.51.100.3"], None, limited),
        (&trusted, &[], Some("198.51.100.4"), ok),
        (&trusted, &["198.51.100.4"], None, limited),
        (&trusted, &[], None, ok),
        (
            &trusted,
            &["198.51.100.5, not-an-ip, 203.0.113.1"],
            Some("198.51.100.6"),
            limited,
        ),
        (&trusted, &[], Some("not-an-ip"), limited),
        (&untrusted, &[], None, ok),
        (&untrusted, &["198.51.100.7"], Some("198.51.100.8"), limited),
    ];
    for (client, forwarded, real_ip, status) in cases {
        let mut request = client.get(&url);
        for value in forwarded {
            request = request.header("x-forwarded-for", *value);
        }
        if let Some(value) = real_ip {
            request = request.header("x-real-ip", value);
        }
        let response = request.send().await.unwrap();
        assert_eq!(
            response.status(),
            status,
            "XFF={forwarded:?}, X-Real-IP={real_ip:?}"
        );
    }
    server.abort();
}

#[tokio::test]
async fn download_quota_paces_proxy_bytes_without_charging_redirects_or_unsent_range_bytes() {
    let (state, kubo) = public_bucket().await;
    const BODY: &[u8] = b"hello world!";
    for (key, cid, size) in [
        ("plain", "QmPlain", BODY.len() as i64),
        ("large", "QmLarge", 4096),
    ] {
        publish_versioned_read_object_with_size(
            &state,
            key,
            key,
            cid,
            size,
            false,
            None,
            None,
            vec![],
        )
        .await;
    }
    mount_cat_body(&kubo, "QmPlain", BODY.to_vec()).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("arg", "QmLarge"))
        .and(query_param("offset", "24"))
        .and(query_param("length", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_string("rs"))
        .expect(1)
        .mount(&kubo)
        .await;
    let config: Config = toml::from_str(
        r#"
        [rate_limits.anonymous]
        proxy_download_bytes_per_second = 20
        [download_redirect]
        gateway_url = 'https://gateway.example'
        [[download_redirect.rules]]
        bucket = 'bucket'
        prefix = 'large'
        "#,
    )
    .unwrap();
    let (endpoint, server) =
        serve_object_http_with_rate_limits(&state, config.download_redirect, config.rate_limits)
            .await;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let started = Instant::now();
    let full = client
        .get(format!("{endpoint}/bucket/plain"))
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.bytes().await.unwrap().as_ref(), BODY);
    let elapsed = started.elapsed();
    // Check for pacing on the wire; the precise token schedule is covered at the limiter seam.
    assert!(
        elapsed >= Duration::from_millis(300),
        "proxy GET completed in {elapsed:?}"
    );

    let large = format!("{endpoint}/bucket/large");
    let redirect = client.get(&large).send().await.unwrap();
    assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        redirect.headers()[header::LOCATION],
        "https://gateway.example/ipfs/QmLarge"
    );
    assert!(redirect.bytes().await.unwrap().is_empty());
    assert_eq!(kubo.received_requests().await.unwrap().len(), 1);

    let partial = client
        .get(large)
        .header("x-ipfs3-download-gateway", "proxy")
        .header(header::RANGE, "bytes=24-25")
        .send()
        .await
        .unwrap();
    assert_eq!(partial.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(partial.headers()[header::CONTENT_RANGE], "bytes 24-25/4096");
    assert_eq!(partial.bytes().await.unwrap().as_ref(), b"rs");
    server.abort();
}
