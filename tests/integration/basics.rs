use super::common::{
    assert_latest_absent, assert_s3_error, delete_headers, delete_xml,
    install_unversioned_content_version, kubo_query_args, scripted, seed_latest,
    signed_delete_objects, signed_get, signed_head, signed_put, standard_script, test_bucket,
    xml_element_values, xml_text,
};
use crate::support::decompress::{TestHarness, assert_no_kubo_calls, start_harness};
use crate::support::sigv4::send_sigv4;
use chrono::Utc;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::ConnectionTrait;

fn bad_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("wrong"), Some("wrong"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

#[tokio::test]
async fn test_aws_bucket_name_validation_rejects_before_store_or_kubo() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let invalid_buckets = [
        "ab".to_owned(),
        "UPPERCASE".to_owned(),
        "under_score".to_owned(),
        ".leading-dot".to_owned(),
        "trailing-hyphen-".to_owned(),
        "adjacent..dot".to_owned(),
        "192.168.0.1".to_owned(),
        "a".repeat(64),
    ];

    for bucket in invalid_buckets {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidBucketName", "").await;
        assert!(
            !store::bucket::exists(harness.state.store.db(), &bucket)
                .await
                .expect("check bucket row"),
            "invalid bucket {bucket} must not have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_aws_bucket_name_validation_accepts_lowercase_dot_and_hyphen() {
    let harness = start_harness(scripted(&[], vec![])).await;

    for bucket in ["abc", "valid-bucket", "valid.bucket"] {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "create bucket {bucket}");
        assert!(
            store::bucket::exists(harness.state.store.db(), bucket)
                .await
                .expect("check bucket row"),
            "valid bucket {bucket} must have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_create_and_put_and_get_plain_object() {
    let harness = start_harness(standard_script(1)).await;
    let bucket = test_bucket(&harness);

    let put = bucket
        .put_object("hello.txt", b"hello world")
        .await
        .expect("put object");
    assert_eq!(put.status_code(), 200);
    assert!(
        put.headers()
            .get("etag")
            .expect("etag header")
            .contains("QmTestCid")
    );

    let get = bucket.get_object("hello.txt").await.expect("get object");
    assert_eq!(get.status_code(), 200);
    assert_eq!(get.as_slice(), b"hello world");
}

#[tokio::test]
async fn test_harness_captures_kubo_file_bytes_and_updates_cat_body() {
    let payload = b"captured exact bytes".to_vec();
    let harness = start_harness(scripted(&["QmCaptured"], vec![])).await;

    let put = signed_put(
        &harness,
        "captured.bin",
        &[],
        payload.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body("QmCaptured", payload.clone());
    let get = signed_get(&harness, "captured.bin").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.expect("GET body").as_ref(), &payload);
}

#[tokio::test]
async fn test_client_compat_head_nested_key_signed_on_localhost() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "nested/path/file.txt", "QmNestedCid", 11).await;
    let response = send_sigv4(
        reqwest::Method::HEAD,
        &harness.endpoint,
        &harness.bucket,
        "nested/path/file.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .expect("Content-Length"),
        "11"
    );
    assert!(
        response
            .headers()
            .get(http::header::ETAG)
            .expect("ETag")
            .to_str()
            .expect("ETag is text")
            .contains("QmNestedCid")
    );
}

#[tokio::test]
async fn test_head_range_changes_only_content_length_and_never_calls_kubo() {
    let harness = start_harness(standard_script(0)).await;
    store::object::upsert(
        harness.state.store.db(),
        "head-range-id",
        &harness.bucket,
        "range.bin",
        "QmRange",
        11,
        Some("text/plain"),
        "QmRange",
        Some(serde_json::json!({"color": "blue"})),
        true,
        Some("wrapped-fixture"),
        None,
        false,
    )
    .await
    .expect("seed ranged HEAD object");
    install_unversioned_content_version(&harness, "range.bin", Utc::now()).await;

    let full = signed_head(&harness, "range.bin", None).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()[http::header::CONTENT_LENGTH], "11");
    assert_eq!(full.headers()[http::header::ETAG], "\"QmRange\"");
    assert_eq!(full.headers()[http::header::CONTENT_TYPE], "text/plain");
    assert_eq!(full.headers()["x-amz-server-side-encryption"], "AES256");
    assert_eq!(full.headers()["x-amz-meta-color"], "blue");
    assert!(full.headers().get(http::header::LAST_MODIFIED).is_some());

    let ranged = signed_head(&harness, "range.bin", Some("bytes=2-5")).await;
    assert_eq!(ranged.status(), StatusCode::OK);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(ranged.headers().get(http::header::CONTENT_RANGE).is_none());
    for name in [
        http::header::ETAG.as_str(),
        http::header::CONTENT_TYPE.as_str(),
        http::header::LAST_MODIFIED.as_str(),
        "x-amz-server-side-encryption",
        "x-amz-meta-color",
    ] {
        assert_eq!(
            ranged.headers().get(name),
            full.headers().get(name),
            "{name}"
        );
    }
    assert!(full.bytes().await.expect("full HEAD body").is_empty());
    assert!(ranged.bytes().await.expect("ranged HEAD body").is_empty());

    let unsatisfied = signed_head(&harness, "range.bin", Some("bytes=20-30")).await;
    assert_eq!(unsatisfied.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(
        unsatisfied
            .bytes()
            .await
            .expect("unsatisfied HEAD body")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_list_objects() {
    let harness = start_harness(standard_script(2)).await;
    let bucket = test_bucket(&harness);
    bucket
        .put_object("obj1.txt", b"hello world")
        .await
        .expect("put obj1");
    bucket
        .put_object("obj2.txt", b"hello world")
        .await
        .expect("put obj2");

    let pages = bucket
        .list(String::new(), None)
        .await
        .expect("list objects");
    assert_eq!(
        pages.iter().map(|page| page.contents.len()).sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn test_list_objects_with_delimiter_returns_common_prefixes() {
    let harness = start_harness(standard_script(4)).await;
    let bucket = test_bucket(&harness);
    for key in [
        "a.txt",
        "photos/cat.jpg",
        "photos/dog.jpg",
        "videos/clip.mp4",
    ] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list(String::new(), Some("/".to_string()))
        .await
        .expect("list with delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["a.txt"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/", "videos/"]);
}

#[tokio::test]
async fn test_list_objects_with_prefix_and_delimiter_returns_one_level() {
    let harness = start_harness(standard_script(3)).await;
    let bucket = test_bucket(&harness);
    for key in ["photos/cat.jpg", "photos/dog.jpg", "photos/2024/jan.jpg"] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list("photos/".to_string(), Some("/".to_string()))
        .await
        .expect("list with prefix and delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["photos/cat.jpg", "photos/dog.jpg"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/2024/"]);
}

#[tokio::test]
async fn test_wrong_credentials_rejected() {
    let harness = start_harness(standard_script(0)).await;
    let result = bad_bucket(&harness)
        .put_object("hello.txt", b"hello world")
        .await;
    assert!(result.is_err(), "wrong credentials must be rejected");
}

#[tokio::test]
async fn test_client_compat_get_bucket_location_is_standard_us_east_1() {
    let harness = start_harness(standard_script(0)).await;
    let raw = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(raw.status(), StatusCode::OK);
    let body = raw.bytes().await.expect("GetBucketLocation body");
    let mut deserializer = s3s::xml::Deserializer::new(body.as_ref());
    let decoded = <s3s::dto::GetBucketLocationOutput as s3s::xml::Deserialize>::deserialize(
        &mut deserializer,
    )
    .expect("decode GetBucketLocationOutput with s3s 0.14 restXml");
    deserializer
        .expect_eof()
        .expect("GetBucketLocation XML EOF");
    assert_eq!(decoded.location_constraint, None);

    let body_text = std::str::from_utf8(body.as_ref()).expect("GetBucketLocation UTF-8 XML");
    assert!(body_text.contains("<LocationConstraint"), "{body_text}");
    assert!(!body_text.contains("us-east-1"), "{body_text}");

    let missing = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_s3_error(
        missing,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
}

#[tokio::test]
async fn test_client_compat_list_v1_delimiter_marker_pages_without_replay() {
    let harness = start_harness(standard_script(0)).await;
    for key in ["a", "photos/1", "photos/2", "videos/1"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delimiter", "/"), ("max-keys", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.text().await.expect("first ListObjects body");
    assert_eq!(xml_element_values(&first_body, "Key"), vec!["a"]);
    assert_eq!(
        xml_element_values(&first_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["photos/"]
    );
    assert_eq!(
        xml_text(&first_body, "IsTruncated").as_deref(),
        Some("true")
    );
    let marker = xml_text(&first_body, "NextMarker").expect("NextMarker");
    assert_eq!(marker, "photos/2");

    let second = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("delimiter", "/"),
            ("marker", marker.as_str()),
            ("max-keys", "2"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = second.text().await.expect("second ListObjects body");
    assert!(xml_element_values(&second_body, "Key").is_empty());
    assert_eq!(
        xml_element_values(&second_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["videos/"]
    );
    assert_eq!(
        xml_text(&second_body, "IsTruncated").as_deref(),
        Some("false")
    );
    assert!(xml_text(&second_body, "NextMarker").is_none());
}

#[tokio::test]
async fn test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination() {
    let harness = start_harness(standard_script(0)).await;
    let raw_prefix = "prefix/";
    let raw_object = "prefix/a%2F(é)";
    let raw_common_key = "prefix/dir%2F(é)/one";
    for key in [raw_object, raw_common_key, "prefix/z"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_prefix),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first_v1.status(), StatusCode::OK);
    let first_v1_body = first_v1.text().await.expect("v1 URL-encoded body");
    assert_eq!(
        xml_text(&first_v1_body, "Name").as_deref(),
        Some("test-bkt")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Prefix").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Delimiter").as_deref(),
        Some("%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Marker").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_element_values(&first_v1_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(
        xml_element_values(&first_v1_body, "Prefix").contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "NextMarker").as_deref(),
        Some("prefix%2Fdir%252F%28%C3%A9%29%2Fone")
    );

    let second_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_common_key),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second_v1.status(), StatusCode::OK);
    let second_v1_body = second_v1.text().await.expect("second v1 URL-encoded body");
    assert_eq!(
        xml_element_values(&second_v1_body, "Key"),
        vec!["prefix%2Fz"]
    );
    assert!(xml_element_values(&second_v1_body, "CommonPrefixes").is_empty());
    assert!(xml_text(&second_v1_body, "NextMarker").is_none());

    let v2 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("list-type", "2"),
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("continuation-token", raw_prefix),
            ("start-after", "ignored/%2F(é)"),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(v2.status(), StatusCode::OK);
    let v2_body = v2.text().await.expect("v2 URL-encoded body");
    assert_eq!(xml_text(&v2_body, "Name").as_deref(), Some("test-bkt"));
    assert_eq!(xml_text(&v2_body, "Prefix").as_deref(), Some("prefix%2F"));
    assert_eq!(xml_text(&v2_body, "Delimiter").as_deref(), Some("%2F"));
    assert_eq!(
        xml_text(&v2_body, "StartAfter").as_deref(),
        Some("ignored%2F%252F%28%C3%A9%29")
    );
    assert_eq!(
        xml_text(&v2_body, "ContinuationToken").as_deref(),
        Some(raw_prefix),
        "continuation tokens remain opaque"
    );
    assert_eq!(
        xml_text(&v2_body, "NextContinuationToken").as_deref(),
        Some(raw_common_key),
        "the raw cursor identity is not URL-projected"
    );
    assert_eq!(
        xml_element_values(&v2_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(xml_element_values(&v2_body, "Prefix").contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F"));
}

#[tokio::test]
async fn test_client_compat_delete_objects_is_retry_safe_and_ordered() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "a", "QmA", 1).await;
    seed_latest(&harness, "b", "QmB", 1).await;

    let response = signed_delete_objects(&harness, &["a", "missing", "a", "b"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("DeleteObjects body");
    let deleted = xml_element_values(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["a", "missing", "a", "b"]);
    assert!(xml_element_values(&body, "Error").is_empty());
    assert_latest_absent(&harness, "a").await;
    assert_latest_absent(&harness, "b").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_quiet_hides_successes() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "quiet", "QmQuiet", 5).await;

    let response = signed_delete_objects(&harness, &["quiet", "missing"], true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("quiet DeleteObjects body");
    assert!(xml_element_values(&body, "Deleted").is_empty());
    assert!(xml_element_values(&body, "Error").is_empty());
    assert_latest_absent(&harness, "quiet").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_continues_after_store_error() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "before", "QmBefore", 6).await;
    seed_latest(&harness, "fail", "QmFail", 4).await;
    seed_latest(&harness, "after", "QmAfter", 5).await;
    harness
        .state
        .store
        .db()
        .execute_unprepared(
            "CREATE TRIGGER fail_one_batch_delete BEFORE UPDATE OF is_latest ON objects \
             WHEN OLD.bucket = 'test-bkt' AND OLD.key = 'fail' AND NEW.is_latest = FALSE \
             BEGIN SELECT RAISE(FAIL, 'injected delete failure'); END",
        )
        .await
        .expect("install delete failure trigger");

    let response = signed_delete_objects(&harness, &["before", "fail", "after"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial DeleteObjects body");
    let deleted = xml_element_values(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["before", "after"]);
    let errors = xml_element_values(&body, "Error");
    assert_eq!(errors.len(), 1);
    assert_eq!(xml_text(errors[0], "Key").as_deref(), Some("fail"));
    assert_eq!(
        xml_text(errors[0], "Code").as_deref(),
        Some("InternalError")
    );
    assert_eq!(
        xml_text(errors[0], "Message").as_deref(),
        Some("failed to delete object")
    );
    assert_latest_absent(&harness, "before").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "fail")
        .await
        .expect("failed item remains latest");
    assert_latest_absent(&harness, "after").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_missing_bucket_is_request_error() {
    let harness = start_harness(standard_script(0)).await;
    let body = delete_xml(&["a"], false);
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await;

    assert_s3_error(
        response,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}
