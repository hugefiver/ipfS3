use crate::support::{cors::start_harness, sigv4::send_sigv4};
use http::{HeaderMap, Method, StatusCode};
use sea_orm::ConnectionTrait;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{path, query_param},
};

const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
const FLAG: (&str, &str) = ("ipfs3-directory-cid", "true");
const HEADER: &str = "x-ipfs3-directory-cid";

async fn list(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    extra: &[(&str, &str)],
) -> reqwest::Response {
    let mut query = vec![
        ("list-type", "2"),
        ("prefix", prefix),
        ("delimiter", "/"),
        ("max-keys", "1"),
    ];
    query.extend_from_slice(extra);
    send_sigv4(
        Method::GET,
        endpoint,
        bucket,
        "",
        &query,
        vec![],
        HeaderMap::new(),
        "test",
    )
    .await
}

#[tokio::test]
async fn directory_rejects_unsigned_invalid_and_unrepresentable_snapshots() {
    let h = start_harness().await;
    let db = h.state.store.db();
    db.execute_unprepared("UPDATE buckets SET canned_acl='public-read'")
        .await
        .unwrap();
    let public = format!("{}/{}?list-type=2&prefix=p%2F", h.endpoint, h.bucket);
    assert_eq!(
        reqwest::get(&public).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        reqwest::get(format!("{public}&ipfs3-directory-cid=true"))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    for key in [
        "p/a",
        "p/z",
        "conflict/file",
        "conflict/file/child",
        "invalid/empty//file",
        "marker/",
    ] {
        h.publish_plain_object(key, LEAF, 0).await;
    }
    db.execute_unprepared("UPDATE objects SET encrypted=true WHERE key='p/z'")
        .await
        .unwrap();
    for (prefix, extra, code) in [
        ("p/", vec![FLAG], "InvalidRequest"),
        ("conflict/", vec![FLAG], "InvalidRequest"),
        ("invalid/", vec![FLAG], "InvalidRequest"),
        ("marker/", vec![FLAG], "InvalidRequest"),
        ("missing/", vec![FLAG], "NoSuchKey"),
        ("p", vec![FLAG], "InvalidArgument"),
        ("p/", vec![FLAG, ("start-after", "p/a")], "InvalidArgument"),
        (
            "p/",
            vec![FLAG, ("continuation-token", "p/a")],
            "InvalidArgument",
        ),
        (
            "p/",
            vec![("ipfs3-directory-cid", "false")],
            "InvalidArgument",
        ),
    ] {
        let result = list(&h.endpoint, &h.bucket, prefix, &extra).await;
        assert!(!result.headers().contains_key(HEADER));
        assert_eq!(
            crate::common::xml_text(&result.text().await.unwrap(), "Code").as_deref(),
            Some(code),
            "{prefix} {extra:?}"
        );
    }
    db.execute_unprepared("UPDATE objects SET encrypted=false WHERE key='p/z'")
        .await
        .unwrap();
    db.execute_unprepared(&format!("INSERT INTO physical_residencies(tier,cid,node_identity,verification_state,verification_receipt,verified_at,created_at,updated_at) VALUES ('cold','{LEAF}','cold-node','verified','{{\"cid\":\"{LEAF}\",\"node_identity\":\"cold-node\"}}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)")).await.unwrap();
    db.execute_unprepared("UPDATE version_residencies SET primary_tier='cold',storage_class='STANDARD_IA' WHERE object_id IN (SELECT id FROM objects WHERE key='p/z')").await.unwrap();
    let cold = list(&h.endpoint, &h.bucket, "p/", &[FLAG]).await;
    assert_eq!(
        crate::common::xml_text(&cold.text().await.unwrap(), "Code").as_deref(),
        Some("InvalidObjectState")
    );
    assert_eq!(h.kubo_request_count().await, 0);
}

#[tokio::test]
async fn signed_directory_is_complete_beyond_xml_page_without_pinning() {
    let h = start_harness().await;
    for key in ["p/a", "p/目录/猫", "P/outside"] {
        h.publish_plain_object(key, LEAF, 0).await;
    }
    Mock::given(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{LEAF}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash":LEAF,"CumulativeSize":0})),
        )
        .expect(1)
        .mount(&h.kubo)
        .await;
    Mock::given(path("/api/v0/dag/put"))
        .and(query_param("pin", "false"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Cid":{"/":ROOT}})),
        )
        .expect(2)
        .mount(&h.kubo)
        .await;
    let ordinary = list(&h.endpoint, &h.bucket, "p/", &[]).await;
    assert!(!ordinary.headers().contains_key(HEADER));
    let xml = ordinary.text().await.unwrap();
    assert!(xml.contains("<Key>p/a</Key>"));
    assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
    assert_eq!(h.kubo_request_count().await, 0);
    let result = list(&h.endpoint, &h.bucket, "p/", &[FLAG]).await;
    assert_eq!(result.status(), StatusCode::OK);
    assert_eq!(result.headers()[HEADER], ROOT);
    assert_eq!(result.text().await.unwrap(), xml);
    let requests = h.kubo.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    let child = String::from_utf8_lossy(&requests[1].body);
    let root = String::from_utf8_lossy(&requests[2].body);
    assert!(child.contains("\"Name\":\"猫\""));
    assert!(root.contains("\"Name\":\"目录\"") && root.contains("\"Name\":\"a\""));
    assert!(!root.contains("outside"));
}
