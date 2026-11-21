use super::super::super::{kubo_server, test_state};
use super::{
    DOWNLOAD_GATEWAY, REDIRECT_CONFIG, download_request, mount_cat_body, policy_state,
    publish_versioned_read_object, serve_object_http,
};
use crate::{config::Config, pinning::tags::ObjectTag, store::bucket};
use http::{Method, StatusCode, header};
use reqwest::{Client, redirect::Policy};

#[tokio::test]
async fn download_negotiation_respects_rule_modes_and_filename_selectors() {
    const MATCHED: &str = "archive/deep/report-1.txt";
    const WRONG_BASENAME: &str = "archive/report-1.txt/other.txt";
    const WRONG_PREFIX: &str = "elsewhere/report-1.txt";
    const FORCED_PROXY: &str = "source-proxied/item.txt";

    let (state, kubo, _) = policy_state().await;
    for key in [MATCHED, WRONG_BASENAME, WRONG_PREFIX, FORCED_PROXY] {
        publish_versioned_read_object(
            &state,
            key,
            key,
            "QmNegotiated",
            false,
            None,
            None,
            vec![ObjectTag::new("owner", "team")],
        )
        .await;
    }
    mount_cat_body(&kubo, "QmNegotiated", b"body".to_vec()).await;
    let cfg: Config = toml::from_str(REDIRECT_CONFIG).unwrap();
    let (endpoint, server) = serve_object_http(&state, cfg.download_redirect).await;
    let client = Client::builder().redirect(Policy::none()).build().unwrap();

    for (key, preference, status) in [
        (MATCHED, None, StatusCode::OK),
        (MATCHED, Some("prefer"), StatusCode::TEMPORARY_REDIRECT),
        (MATCHED, Some("require"), StatusCode::TEMPORARY_REDIRECT),
        (WRONG_BASENAME, Some("prefer"), StatusCode::OK),
        (WRONG_PREFIX, Some("prefer"), StatusCode::OK),
        (FORCED_PROXY, Some("prefer"), StatusCode::OK),
        (FORCED_PROXY, Some("require"), StatusCode::NOT_ACCEPTABLE),
    ] {
        let mut request = download_request(&client, &endpoint, Method::GET, key, &[]);
        if let Some(preference) = preference {
            request = request.header(DOWNLOAD_GATEWAY, preference);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), status, "{key} with {preference:?}");
        if status == StatusCode::TEMPORARY_REDIRECT {
            assert_eq!(
                response.headers()[header::LOCATION],
                "https://gateway.example/ipfs/QmNegotiated",
            );
        } else if status == StatusCode::OK {
            assert!(!response.headers().contains_key(header::LOCATION));
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"body");
        } else {
            let xml = response.text().await.unwrap();
            assert!(xml.contains("<Code>NotAcceptable</Code>"), "{xml}");
        }
    }
    server.abort();
}

#[tokio::test]
async fn anonymous_download_negotiation_obeys_bucket_acl() {
    let kubo = kubo_server("unused").await;
    let state = test_state(kubo.uri()).await;
    let db = state.store.db();
    bucket::create_with_acl(db, "bucket", Some("test-owner"), Some("private"))
        .await
        .unwrap();
    publish_versioned_read_object(
        &state,
        "public-test",
        "item.txt",
        "QmAnonymous",
        false,
        None,
        None,
        vec![],
    )
    .await;

    let cfg: Config = toml::from_str(
        r#"
[download_redirect]
gateway_url = 'https://gateway.example'
[[download_redirect.rules]]
bucket = 'bucket'
anonymous = true
mode = 'on_request'
"#,
    )
    .unwrap();
    let (endpoint, server) = serve_object_http(&state, cfg.download_redirect).await;
    let client = Client::builder().redirect(Policy::none()).build().unwrap();
    let url = format!("{endpoint}/bucket/item.txt");
    let request = || client.get(url.as_str()).header(DOWNLOAD_GATEWAY, "require");

    let denied = request().send().await.unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(!denied.headers().contains_key(header::LOCATION));

    bucket::set_acl_for_owner(db, "bucket", "test-owner", "public-read")
        .await
        .unwrap();
    let redirected = request().send().await.unwrap();
    assert_eq!(redirected.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(
        redirected.headers()[header::LOCATION],
        "https://gateway.example/ipfs/QmAnonymous",
    );
    server.abort();
}
