use crate::support::{
    cors::{default_import_coordinator, start_gateway},
    decompress::{KuboScript, start_kubo_harness},
    sigv4::{presign_sigv4_query, send_sigv4},
};
use http::{HeaderMap, Method, StatusCode};
use ipfs_s3_gateway::{config::Config, state::AppState};
use std::collections::HashMap;

const BUCKET: &str = "media-bucket";

#[tokio::test]
async fn bucket_defaults_compose_with_directory_grants_and_version_tags() {
    let payload = b"published before ACL changes";
    let cid = "QmBucketAclPayload";
    let kubo = start_kubo_harness(KuboScript::repeated_add(
        cid,
        4,
        HashMap::from([(cid.to_owned(), payload.to_vec())]),
    ))
    .await;
    let config: Config = toml::from_str(&format!(
        r#"
[kubo]
rpc_url = {:?}
[storage]
database_url = 'sqlite::memory:'
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[[auth.credentials]]
access_key = 'reader'
secret_key = 'reader-key'
[[auth.policy]]
effect = 'allow'
user = 'test'
actions = ['CreateBucket']
[[auth.policy]]
effect = 'allow'
user = 'reader'
actions = ['ListBuckets']
[[auth.policy]]
effect = 'allow'
user = 'reader'
bucket = 'media-bucket'
prefix = 'shared/'
actions = ['GetObject']
[[auth.policy]]
effect = 'deny'
bucket = 'media-bucket'
actions = ['GetObject']
existing_tags = {{ hidden = 'yes' }}
"#,
        kubo.server.uri()
    ))
    .unwrap();
    let state = AppState::new(&config).await.unwrap();
    let server = start_gateway(state, default_import_coordinator()).await;
    let endpoint = &server.endpoint;
    owner_put(endpoint, "", &[], HeaderMap::new(), &[]).await;
    for key in ["shared/doc", "plain", "marked", "shared/z"] {
        let mut headers = HeaderMap::new();
        if matches!(key, "marked" | "shared/z") {
            headers.insert("x-amz-tagging", "hidden=yes".parse().unwrap());
        }
        owner_put(endpoint, key, &[], headers, payload).await;
    }
    let client = reqwest::Client::new();
    let plain = format!("{endpoint}/{BUCKET}/plain");
    let marked = format!("{endpoint}/{BUCKET}/marked");
    let status = async |url: &str| client.get(url).send().await.unwrap().status();
    let reader = reader_url(endpoint, BUCKET, "plain");
    assert_eq!(status(&plain).await, StatusCode::FORBIDDEN);
    assert_eq!(status(&reader).await, StatusCode::FORBIDDEN);
    assert_eq!(
        status(&reader_url(endpoint, BUCKET, "shared/doc")).await,
        StatusCode::OK
    );

    category(endpoint, "authenticated-read").await;
    assert_eq!(status(&plain).await, StatusCode::FORBIDDEN);
    assert_eq!(status(&reader).await, StatusCode::OK);
    category(endpoint, "public-read").await;
    let response = client.get(&plain).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    let wrong_secret = send_sigv4(
        Method::GET,
        endpoint,
        BUCKET,
        "plain",
        &[],
        vec![],
        HeaderMap::new(),
        "wrong-secret",
    )
    .await;
    assert_eq!(wrong_secret.status(), StatusCode::FORBIDDEN);
    assert_eq!(status(&marked).await, StatusCode::FORBIDDEN);
    let directory = send_sigv4(
        Method::GET,
        endpoint,
        BUCKET,
        "",
        &[
            ("list-type", "2"),
            ("prefix", "shared/"),
            ("max-keys", "1"),
            ("ipfs3-directory-cid", "true"),
        ],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(
        directory.status(),
        StatusCode::FORBIDDEN,
        "tag denial beyond the XML page"
    );
    assert!(!directory.headers().contains_key("x-ipfs3-directory-cid"));
    let listed = client
        .get(reader_url(endpoint, "", ""))
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    assert!(!listed.text().await.unwrap().contains(BUCKET));

    category(endpoint, "private").await;
    assert_eq!(status(&plain).await, StatusCode::FORBIDDEN);
    assert_eq!(status(&reader).await, StatusCode::FORBIDDEN);
    assert_eq!(
        status(&reader_url(endpoint, BUCKET, "shared/doc")).await,
        StatusCode::OK
    );
    server.shutdown().await;
}

#[tokio::test]
async fn anonymous_multipart_uses_current_bucket_grant_at_each_stage() {
    use crate::common::xml_text;
    use crate::support::decompress::start_harness;

    let cid = "QmAclUploadBody";
    let harness = start_harness(KuboScript::repeated_add(
        cid,
        2,
        HashMap::from([(cid.to_owned(), b"part".to_vec())]),
    ))
    .await;
    let endpoint = &harness.endpoint;
    let mut acl = HeaderMap::new();
    acl.insert("x-amz-acl", "public-read-write".parse().unwrap());
    owner_put(endpoint, "", &[], acl, &[]).await;
    let client = reqwest::Client::new();
    let key = "multipart";
    let url = format!("{endpoint}/{BUCKET}/{key}");
    let signed = send_sigv4(
        Method::POST,
        endpoint,
        BUCKET,
        key,
        &[("uploads", "")],
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(signed.status(), StatusCode::OK);
    let signed_id = xml_text(&signed.text().await.unwrap(), "UploadId").unwrap();
    let part = async |id: &str| {
        client
            .put(format!("{url}?partNumber=1&uploadId={id}"))
            .body("part")
            .send()
            .await
            .unwrap()
    };
    assert_eq!(part(&signed_id).await.status(), StatusCode::FORBIDDEN);

    let created = client.post(format!("{url}?uploads")).send().await.unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let id = xml_text(&created.text().await.unwrap(), "UploadId").unwrap();
    let wrong_key = client
        .put(format!(
            "{endpoint}/{BUCKET}/other?partNumber=1&uploadId={id}"
        ))
        .body("part")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        xml_text(&wrong_key.text().await.unwrap(), "Code").as_deref(),
        Some("NoSuchUpload")
    );
    let uploaded = part(&id).await;
    assert_eq!(uploaded.status(), StatusCode::OK);
    let etag = uploaded.headers()["etag"].to_str().unwrap();
    let body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let complete = async || {
        client
            .post(format!("{url}?uploadId={id}"))
            .header("content-type", "application/xml")
            .body(body.clone())
            .send()
            .await
            .unwrap()
    };
    category(endpoint, "private").await;
    assert_eq!(part(&id).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(complete().await.status(), StatusCode::FORBIDDEN);
    category(endpoint, "public-read-write").await;
    let finished = complete().await;
    assert_eq!(
        finished.status(),
        StatusCode::OK,
        "{}",
        finished.text().await.unwrap()
    );
    assert_eq!(
        client
            .get(&url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"part"
    );
    assert_eq!(
        client.delete(&url).send().await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
}

async fn owner_put(
    endpoint: &str,
    key: &str,
    query: &[(&str, &str)],
    headers: HeaderMap,
    body: &[u8],
) {
    let response = send_sigv4(
        Method::PUT,
        endpoint,
        BUCKET,
        key,
        query,
        body.to_vec(),
        headers,
        "test",
    )
    .await;
    assert!(
        response.status().is_success(),
        "{}: {}",
        response.status(),
        response.text().await.unwrap()
    );
}

async fn category(endpoint: &str, acl: &'static str) {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-acl", acl.parse().unwrap());
    owner_put(endpoint, "", &[("acl", "")], headers, &[]).await;
}

fn reader_url(endpoint: &str, bucket: &str, key: &str) -> String {
    presign_sigv4_query(
        &Method::GET,
        endpoint,
        bucket,
        key,
        &[],
        "reader",
        "reader-key",
        None,
        60,
        chrono::Utc::now(),
    )
}
