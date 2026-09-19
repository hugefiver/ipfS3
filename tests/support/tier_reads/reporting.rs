use std::{collections::HashMap, sync::Arc};

use base64::Engine as _;
use chrono::{Duration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    crypto::ObjectKey,
    kubo::LocalResidencyVerificationReceipt,
    state::AppState,
    store::{
        self, Store,
        entities::{
            object, object_version, physical_residency, residency_reference, version_residency,
        },
        object_version::BucketVersioningState,
    },
};
use sea_orm::{ConnectionTrait, Database, EntityTrait, Set};
use wiremock::{Mock, ResponseTemplate, matchers};

use crate::support::{
    decompress::{KuboScript, S3ServerHandle, start_kubo_harness, start_s3_server},
    sigv4::send_sigv4,
};

use super::{
    BUCKET, COLD_BYTES, COLD_NODE_ID, COLD_VERSION_CID, HOT_BYTES, HOT_VERSION_CID, NULL_BYTES,
    NULL_KEY, NULL_VERSION_CID, VERSIONED_KEY, seed_immutable_residencies,
};

mod concurrency;
mod receipts;

const SHARED_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyka";
const COLD_LIST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyke";
const HOT_LIST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyki";
const NESTED_LIST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvykm";
const DELETED_LIST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvykq";
const SSE_S3_CID: &str = HOT_VERSION_CID;
// Cold reads validate and parse the CID before probing the receipt-bound Kubo node.
const SSE_C_CID: &str = COLD_VERSION_CID;

const SHARED_OLD_VERSION: &str = "00000000-0000-0000-0000-000000000101";
const SHARED_NEW_VERSION: &str = "00000000-0000-0000-0000-000000000102";
const DELETE_CONTENT_VERSION: &str = "00000000-0000-0000-0000-000000000201";
const DELETE_MARKER_VERSION: &str = "00000000-0000-0000-0000-000000000202";
const SSE_S3_VERSION: &str = "00000000-0000-0000-0000-000000000301";
const SSE_C_VERSION: &str = "00000000-0000-0000-0000-000000000302";

const SHARED_KEY: &str = "report/shared name.bin";
const DELETED_KEY: &str = "report/deleted.bin";
const SSE_S3_KEY: &str = "report/encrypted-sse-s3.bin";
const SSE_C_KEY: &str = "report/encrypted-sse-c.bin";
const ENCRYPTED_PLAINTEXT: &[u8] = b"encrypted-reporting-body";

struct ReportingHarness {
    endpoint: String,
    state: Arc<AppState>,
    _kubo: wiremock::MockServer,
    server: S3ServerHandle,
}

#[derive(Clone)]
struct EncryptionEnvelope {
    encrypted: bool,
    key_wrap: Option<String>,
    sse_c_key_fingerprint: Option<String>,
}

impl EncryptionEnvelope {
    fn plain() -> Self {
        Self {
            encrypted: false,
            key_wrap: None,
            sse_c_key_fingerprint: None,
        }
    }
}

struct SeededRow {
    object_id: String,
    version_row_id: String,
}

#[tokio::test]
async fn signed_plain_get_and_head_report_selected_version_storage_class() {
    let harness = start_reporting_harness(HashMap::from([
        (COLD_VERSION_CID.to_owned(), COLD_BYTES.to_vec()),
        (HOT_VERSION_CID.to_owned(), HOT_BYTES.to_vec()),
        (NULL_VERSION_CID.to_owned(), NULL_BYTES.to_vec()),
    ]))
    .await;
    let versions = seed_immutable_residencies(&harness.state).await;

    struct ReadCase<'a> {
        label: &'static str,
        key: &'static str,
        version_id: Option<&'a str>,
        expected_version_id: &'a str,
        storage_class: &'static str,
        cid: &'static str,
        body: &'static [u8],
    }

    let cases = [
        ReadCase {
            label: "exact historical cold version",
            key: VERSIONED_KEY,
            version_id: Some(&versions.historical),
            expected_version_id: &versions.historical,
            storage_class: "STANDARD_IA",
            cid: COLD_VERSION_CID,
            body: COLD_BYTES,
        },
        ReadCase {
            label: "latest hot version",
            key: VERSIONED_KEY,
            version_id: None,
            expected_version_id: &versions.current,
            storage_class: "STANDARD",
            cid: HOT_VERSION_CID,
            body: HOT_BYTES,
        },
        ReadCase {
            label: "exact current hot version",
            key: VERSIONED_KEY,
            version_id: Some(&versions.current),
            expected_version_id: &versions.current,
            storage_class: "STANDARD",
            cid: HOT_VERSION_CID,
            body: HOT_BYTES,
        },
        ReadCase {
            label: "latest null cold version",
            key: NULL_KEY,
            version_id: None,
            expected_version_id: "null",
            storage_class: "STANDARD_IA",
            cid: NULL_VERSION_CID,
            body: NULL_BYTES,
        },
        ReadCase {
            label: "exact null cold version",
            key: NULL_KEY,
            version_id: Some("null"),
            expected_version_id: "null",
            storage_class: "STANDARD_IA",
            cid: NULL_VERSION_CID,
            body: NULL_BYTES,
        },
    ];

    for case in cases {
        let full_get = signed_request(
            &harness.endpoint,
            reqwest::Method::GET,
            case.key,
            case.version_id,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(full_get.status(), StatusCode::OK, "{} GET", case.label);
        assert_object_headers(
            &full_get,
            case.storage_class,
            case.cid,
            case.expected_version_id,
            case.label,
        );
        assert_eq!(
            full_get.bytes().await.expect("full GET body").as_ref(),
            case.body,
            "{} GET body",
            case.label
        );

        let mut range = HeaderMap::new();
        range.insert(http::header::RANGE, HeaderValue::from_static("bytes=1-4"));
        let range_get = signed_request(
            &harness.endpoint,
            reqwest::Method::GET,
            case.key,
            case.version_id,
            range,
        )
        .await;
        assert_eq!(
            range_get.status(),
            StatusCode::PARTIAL_CONTENT,
            "{} Range GET",
            case.label
        );
        assert_object_headers(
            &range_get,
            case.storage_class,
            case.cid,
            case.expected_version_id,
            case.label,
        );
        assert_eq!(range_get.headers()[http::header::CONTENT_LENGTH], "4");
        assert_eq!(
            range_get.headers()[http::header::CONTENT_RANGE],
            format!("bytes 1-4/{}", case.body.len())
        );
        assert_eq!(
            range_get.bytes().await.expect("Range GET body").as_ref(),
            &case.body[1..=4],
            "{} Range GET body",
            case.label
        );

        let full_head = signed_request(
            &harness.endpoint,
            reqwest::Method::HEAD,
            case.key,
            case.version_id,
            HeaderMap::new(),
        )
        .await;
        assert_eq!(full_head.status(), StatusCode::OK, "{} HEAD", case.label);
        assert_object_headers(
            &full_head,
            case.storage_class,
            case.cid,
            case.expected_version_id,
            case.label,
        );
        assert_eq!(
            full_head.headers()[http::header::CONTENT_LENGTH],
            case.body.len().to_string()
        );

        let mut head_range = HeaderMap::new();
        head_range.insert(http::header::RANGE, HeaderValue::from_static("bytes=1-4"));
        let range_head = signed_request(
            &harness.endpoint,
            reqwest::Method::HEAD,
            case.key,
            case.version_id,
            head_range,
        )
        .await;
        assert_eq!(
            range_head.status(),
            StatusCode::OK,
            "{} Range HEAD",
            case.label
        );
        assert_object_headers(
            &range_head,
            case.storage_class,
            case.cid,
            case.expected_version_id,
            case.label,
        );
        assert_eq!(range_head.headers()[http::header::CONTENT_LENGTH], "4");
    }

    harness.server.shutdown().await;
}

#[tokio::test]
async fn signed_object_and_version_lists_report_immutable_storage_classes() {
    let harness = start_reporting_harness(HashMap::new()).await;
    let db = harness.state.store.db();
    store::bucket::set_versioning_state(db, BUCKET, BucketVersioningState::Enabled)
        .await
        .expect("enable reporting fixture versioning");
    let now = Utc::now();

    for (tier, cid, verified) in [
        ("cold", COLD_LIST_CID, true),
        ("hot", HOT_LIST_CID, false),
        ("cold", SHARED_CID, true),
        ("hot", SHARED_CID, false),
        ("cold", NESTED_LIST_CID, true),
        ("hot", DELETED_LIST_CID, false),
    ] {
        seed_physical(db, tier, cid, verified, now).await;
    }

    seed_content_version(
        db,
        "report/a-cold.bin",
        COLD_LIST_CID,
        Some("00000000-0000-0000-0000-000000000001"),
        1,
        true,
        "cold",
        "STANDARD_IA",
        11,
        now - Duration::seconds(8),
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_content_version(
        db,
        "report/b-hot.bin",
        HOT_LIST_CID,
        Some("00000000-0000-0000-0000-000000000002"),
        1,
        true,
        "hot",
        "STANDARD",
        12,
        now - Duration::seconds(7),
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_content_version(
        db,
        SHARED_KEY,
        SHARED_CID,
        Some(SHARED_OLD_VERSION),
        1,
        false,
        "cold",
        "STANDARD_IA",
        13,
        now - Duration::seconds(6),
        Some(now - Duration::seconds(5)),
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_content_version(
        db,
        SHARED_KEY,
        SHARED_CID,
        Some(SHARED_NEW_VERSION),
        2,
        true,
        "hot",
        "STANDARD",
        13,
        now - Duration::seconds(5),
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_content_version(
        db,
        "report/nested/cold.bin",
        NESTED_LIST_CID,
        Some("00000000-0000-0000-0000-000000000003"),
        1,
        true,
        "cold",
        "STANDARD_IA",
        14,
        now - Duration::seconds(4),
        None,
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_content_version(
        db,
        DELETED_KEY,
        DELETED_LIST_CID,
        Some(DELETE_CONTENT_VERSION),
        1,
        false,
        "hot",
        "STANDARD",
        15,
        now - Duration::seconds(3),
        Some(now - Duration::seconds(2)),
        EncryptionEnvelope::plain(),
    )
    .await;
    seed_delete_marker(db, DELETED_KEY, DELETE_MARKER_VERSION, 2, now).await;

    let current_marker_get = signed_request(
        &harness.endpoint,
        reqwest::Method::GET,
        DELETED_KEY,
        None,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current_marker_get.status(), StatusCode::NOT_FOUND);
    assert_delete_marker_headers(&current_marker_get, false);
    assert!(
        current_marker_get
            .text()
            .await
            .expect("current marker GET body")
            .contains("<Code>NoSuchKey</Code>")
    );

    let current_marker_head = signed_request(
        &harness.endpoint,
        reqwest::Method::HEAD,
        DELETED_KEY,
        None,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current_marker_head.status(), StatusCode::NOT_FOUND);
    assert_delete_marker_headers(&current_marker_head, false);

    let exact_marker_get = signed_request(
        &harness.endpoint,
        reqwest::Method::GET,
        DELETED_KEY,
        Some(DELETE_MARKER_VERSION),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(exact_marker_get.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_delete_marker_headers(&exact_marker_get, true);
    assert!(
        exact_marker_get
            .text()
            .await
            .expect("exact marker GET body")
            .contains("<Code>MethodNotAllowed</Code>")
    );

    let exact_marker_head = signed_request(
        &harness.endpoint,
        reqwest::Method::HEAD,
        DELETED_KEY,
        Some(DELETE_MARKER_VERSION),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(exact_marker_head.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_delete_marker_headers(&exact_marker_head, true);

    for operation in [ListOperation::V1, ListOperation::V2] {
        let query = match operation {
            ListOperation::V1 => vec![("prefix", "report/")],
            ListOperation::V2 => vec![("list-type", "2"), ("prefix", "report/")],
        };
        let response = signed_bucket_get(&harness.endpoint, &query).await;
        assert_eq!(response.status(), StatusCode::OK, "{operation:?}");
        let xml = response.text().await.expect("object list XML");
        assert_listed_object(&xml, "report/a-cold.bin", "STANDARD_IA");
        assert_listed_object(&xml, "report/b-hot.bin", "STANDARD");
        assert_listed_object(&xml, SHARED_KEY, "STANDARD");
        assert_listed_object(&xml, "report/nested/cold.bin", "STANDARD_IA");
        assert!(
            !xml.contains(DELETED_KEY),
            "delete-marker latest key must be absent from ordinary {operation:?}: {xml}"
        );
    }

    let versions = signed_bucket_get(&harness.endpoint, &[("versions", "")]).await;
    assert_eq!(versions.status(), StatusCode::OK);
    let versions_xml = versions.text().await.expect("version list XML");
    assert_listed_version(
        &versions_xml,
        SHARED_KEY,
        SHARED_NEW_VERSION,
        SHARED_CID,
        "STANDARD",
    );
    assert_listed_version(
        &versions_xml,
        SHARED_KEY,
        SHARED_OLD_VERSION,
        SHARED_CID,
        "STANDARD_IA",
    );
    let marker = xml_sections(&versions_xml, "DeleteMarker")
        .into_iter()
        .find(|section| xml_text(section, "VersionId").as_deref() == Some(DELETE_MARKER_VERSION))
        .unwrap_or_else(|| panic!("missing seeded delete marker: {versions_xml}"));
    assert_eq!(xml_text(marker, "Key").as_deref(), Some(DELETED_KEY));
    assert!(
        xml_text(marker, "StorageClass").is_none(),
        "delete markers must not synthesize StorageClass: {marker}"
    );

    let first_page = signed_bucket_get(
        &harness.endpoint,
        &[("versions", ""), ("prefix", SHARED_KEY), ("max-keys", "1")],
    )
    .await;
    assert_eq!(first_page.status(), StatusCode::OK);
    let first_xml = first_page.text().await.expect("first version page XML");
    assert_listed_version(
        &first_xml,
        SHARED_KEY,
        SHARED_NEW_VERSION,
        SHARED_CID,
        "STANDARD",
    );
    assert_eq!(xml_text(&first_xml, "IsTruncated").as_deref(), Some("true"));
    let next_key = xml_text(&first_xml, "NextKeyMarker").expect("next key marker");
    let next_version = xml_text(&first_xml, "NextVersionIdMarker").expect("next version marker");
    assert_eq!(next_key, SHARED_KEY);
    assert_eq!(next_version, SHARED_OLD_VERSION);

    let second_page = signed_bucket_get(
        &harness.endpoint,
        &[
            ("versions", ""),
            ("prefix", SHARED_KEY),
            ("key-marker", &next_key),
            ("version-id-marker", &next_version),
            ("max-keys", "1"),
        ],
    )
    .await;
    assert_eq!(second_page.status(), StatusCode::OK);
    let second_xml = second_page.text().await.expect("second version page XML");
    assert_listed_version(
        &second_xml,
        SHARED_KEY,
        SHARED_OLD_VERSION,
        SHARED_CID,
        "STANDARD_IA",
    );

    let encoded_delimiter = signed_bucket_get(
        &harness.endpoint,
        &[
            ("versions", ""),
            ("prefix", "report/"),
            ("delimiter", "/"),
            ("encoding-type", "url"),
        ],
    )
    .await;
    assert_eq!(encoded_delimiter.status(), StatusCode::OK);
    let encoded_xml = encoded_delimiter
        .text()
        .await
        .expect("encoded delimiter version list XML");
    assert!(encoded_xml.contains("<Prefix>report%2F</Prefix>"));
    assert!(encoded_xml.contains("<Key>report%2Fshared%20name.bin</Key>"));
    assert!(encoded_xml.contains("<Prefix>report%2Fnested%2F</Prefix>"));
    assert_listed_version(
        &encoded_xml,
        "report%2Fshared%20name.bin",
        SHARED_OLD_VERSION,
        SHARED_CID,
        "STANDARD_IA",
    );

    assert!(
        harness._kubo.received_requests().await.unwrap().is_empty(),
        "list reporting and marker responses must not probe Kubo"
    );

    harness.server.shutdown().await;
}

#[tokio::test]
async fn signed_encrypted_get_and_head_report_class_without_mutating_envelopes() {
    let sse_s3_object_key = ObjectKey { bytes: [9; 32] };
    let sse_c_object_key = ObjectKey { bytes: [7; 32] };
    let sse_s3_ciphertext = fixed_ciphertext(&sse_s3_object_key, [0x31; 12]);
    let sse_c_ciphertext = fixed_ciphertext(&sse_c_object_key, [0x42; 12]);
    let harness = start_reporting_harness(HashMap::from([
        (SSE_S3_CID.to_owned(), sse_s3_ciphertext),
        (SSE_C_CID.to_owned(), sse_c_ciphertext),
    ]))
    .await;
    let db = harness.state.store.db();
    store::bucket::set_versioning_state(db, BUCKET, BucketVersioningState::Enabled)
        .await
        .expect("enable encrypted reporting fixture versioning");
    let now = Utc::now();
    seed_physical(db, "cold", SSE_S3_CID, true, now).await;
    seed_physical(db, "cold", SSE_C_CID, true, now).await;

    let sse_s3_wrap = harness
        .state
        .master_key
        .wrap(&sse_s3_object_key)
        .expect("wrap reporting SSE-S3 object key");
    let sse_s3 = seed_content_version(
        db,
        SSE_S3_KEY,
        SSE_S3_CID,
        Some(SSE_S3_VERSION),
        1,
        true,
        "cold",
        "STANDARD_IA",
        ENCRYPTED_PLAINTEXT.len(),
        now,
        None,
        EncryptionEnvelope {
            encrypted: true,
            key_wrap: Some(sse_s3_wrap),
            sse_c_key_fingerprint: None,
        },
    )
    .await;
    let sse_c_fingerprint = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&sse_c_object_key);
    let sse_c = seed_content_version(
        db,
        SSE_C_KEY,
        SSE_C_CID,
        Some(SSE_C_VERSION),
        1,
        true,
        "cold",
        "STANDARD_IA",
        ENCRYPTED_PLAINTEXT.len(),
        now,
        None,
        EncryptionEnvelope {
            encrypted: true,
            key_wrap: None,
            sse_c_key_fingerprint: Some(sse_c_fingerprint),
        },
    )
    .await;

    let sse_s3_before = immutable_snapshot(db, &sse_s3).await;
    let sse_c_before = immutable_snapshot(db, &sse_c).await;
    let sse_c_headers = sse_c_headers([7; 32]);

    let sse_s3_get = signed_request(
        &harness.endpoint,
        reqwest::Method::GET,
        SSE_S3_KEY,
        Some(SSE_S3_VERSION),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(sse_s3_get.status(), StatusCode::OK);
    assert_object_headers(
        &sse_s3_get,
        "STANDARD_IA",
        SSE_S3_CID,
        SSE_S3_VERSION,
        "SSE-S3 GET",
    );
    assert_eq!(
        sse_s3_get.headers()["x-amz-server-side-encryption"],
        "AES256"
    );
    assert_eq!(
        sse_s3_get.bytes().await.expect("SSE-S3 GET body").as_ref(),
        ENCRYPTED_PLAINTEXT
    );

    let sse_s3_head = signed_request(
        &harness.endpoint,
        reqwest::Method::HEAD,
        SSE_S3_KEY,
        Some(SSE_S3_VERSION),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(sse_s3_head.status(), StatusCode::OK);
    assert_object_headers(
        &sse_s3_head,
        "STANDARD_IA",
        SSE_S3_CID,
        SSE_S3_VERSION,
        "SSE-S3 HEAD",
    );
    assert_eq!(
        sse_s3_head.headers()["x-amz-server-side-encryption"],
        "AES256"
    );

    let sse_c_get = signed_request(
        &harness.endpoint,
        reqwest::Method::GET,
        SSE_C_KEY,
        Some(SSE_C_VERSION),
        sse_c_headers.clone(),
    )
    .await;
    assert_eq!(sse_c_get.status(), StatusCode::OK);
    assert_object_headers(
        &sse_c_get,
        "STANDARD_IA",
        SSE_C_CID,
        SSE_C_VERSION,
        "SSE-C GET",
    );
    assert_sse_c_response_headers(&sse_c_get, [7; 32]);
    assert_eq!(
        sse_c_get.bytes().await.expect("SSE-C GET body").as_ref(),
        ENCRYPTED_PLAINTEXT
    );

    let sse_c_head = signed_request(
        &harness.endpoint,
        reqwest::Method::HEAD,
        SSE_C_KEY,
        Some(SSE_C_VERSION),
        sse_c_headers.clone(),
    )
    .await;
    assert_eq!(sse_c_head.status(), StatusCode::OK);
    assert_object_headers(
        &sse_c_head,
        "STANDARD_IA",
        SSE_C_CID,
        SSE_C_VERSION,
        "SSE-C HEAD",
    );
    assert_sse_c_response_headers(&sse_c_head, [7; 32]);

    for (key, cid, version, mut headers) in [
        (SSE_S3_KEY, SSE_S3_CID, SSE_S3_VERSION, HeaderMap::new()),
        (SSE_C_KEY, SSE_C_CID, SSE_C_VERSION, sse_c_headers),
    ] {
        headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=2-6"));
        let response = signed_request(
            &harness.endpoint,
            reqwest::Method::GET,
            key,
            Some(version),
            headers,
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_object_headers(
            &response,
            "STANDARD_IA",
            cid,
            version,
            "encrypted Range GET",
        );
        assert_eq!(response.headers()[http::header::CONTENT_LENGTH], "5");
        assert_eq!(
            response.headers()[http::header::CONTENT_RANGE],
            format!("bytes 2-6/{}", ENCRYPTED_PLAINTEXT.len())
        );
        assert_eq!(response.bytes().await.unwrap(), &ENCRYPTED_PLAINTEXT[2..7]);
    }

    assert_eq!(immutable_snapshot(db, &sse_s3).await, sse_s3_before);
    assert_eq!(immutable_snapshot(db, &sse_c).await, sse_c_before);

    harness.server.shutdown().await;
}

#[derive(Clone, Copy, Debug)]
enum ListOperation {
    V1,
    V2,
}

async fn start_reporting_harness(cat_bodies: HashMap<String, Vec<u8>>) -> ReportingHarness {
    let kubo = start_kubo_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies,
    })
    .await
    .server;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/id"))
        .and(matchers::query_param("peerid-base", "b58mh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ID": COLD_NODE_ID
        })))
        .mount(&kubo)
        .await;

    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect storage-class reporting SQLite database");
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .expect("enable reporting SQLite foreign keys");
    store::run_migrations(&db)
        .await
        .expect("run reporting migrations");
    store::bucket::create(&db, BUCKET, Some("tier-read-owner"))
        .await
        .expect("create reporting bucket");
    let kubo_uri = kubo.uri();
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo_uri.clone()),
        cold_kubo: Some(ipfs_s3_gateway::kubo::KuboClient::new(kubo_uri)),
        store: Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::MasterKey::from_hex(&"0".repeat(64))
            .expect("reporting master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;

    ReportingHarness {
        endpoint: server.endpoint.clone(),
        state,
        _kubo: kubo,
        server,
    }
}

async fn signed_request(
    endpoint: &str,
    method: reqwest::Method,
    key: &str,
    version_id: Option<&str>,
    headers: HeaderMap,
) -> reqwest::Response {
    let query = version_id.map_or_else(Vec::new, |version| vec![("versionId", version)]);
    send_sigv4(
        method,
        endpoint,
        BUCKET,
        key,
        &query,
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_bucket_get(endpoint: &str, query: &[(&str, &str)]) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        endpoint,
        BUCKET,
        "",
        query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

fn assert_object_headers(
    response: &reqwest::Response,
    storage_class: &str,
    cid: &str,
    version_id: &str,
    label: &str,
) {
    assert_eq!(
        response
            .headers()
            .get("x-amz-storage-class")
            .and_then(|value| value.to_str().ok()),
        Some(storage_class),
        "{label} storage class"
    );
    assert_eq!(
        response.headers()[http::header::ETAG],
        format!("\"{cid}\""),
        "{label} ETag"
    );
    assert_eq!(
        response.headers()["x-amz-version-id"],
        version_id,
        "{label} version"
    );
}

fn assert_delete_marker_headers(response: &reqwest::Response, exact: bool) {
    assert_eq!(response.headers()["x-amz-delete-marker"], "true");
    assert_eq!(
        response.headers()["x-amz-version-id"],
        DELETE_MARKER_VERSION
    );
    assert!(
        response.headers().get("x-amz-storage-class").is_none(),
        "delete-marker reads must not synthesize a storage class"
    );
    assert_eq!(
        response
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some(),
        exact,
        "only an exact delete-marker selection reports Last-Modified"
    );
}

fn assert_listed_object(xml: &str, key: &str, storage_class: &str) {
    let section = xml_sections(xml, "Contents")
        .into_iter()
        .find(|section| xml_text(section, "Key").as_deref() == Some(key))
        .unwrap_or_else(|| panic!("missing listed object {key}: {xml}"));
    assert_eq!(
        xml_text(section, "StorageClass").as_deref(),
        Some(storage_class),
        "listed object {key}: {section}"
    );
}

fn assert_listed_version(xml: &str, key: &str, version_id: &str, cid: &str, storage_class: &str) {
    let section = xml_sections(xml, "Version")
        .into_iter()
        .find(|section| {
            xml_text(section, "Key").as_deref() == Some(key)
                && xml_text(section, "VersionId").as_deref() == Some(version_id)
        })
        .unwrap_or_else(|| panic!("missing listed version {key} {version_id}: {xml}"));
    let expected_etag = format!("\"{cid}\"");
    assert_eq!(
        xml_text(section, "ETag").as_deref(),
        Some(expected_etag.as_str()),
        "listed version ETag: {section}"
    );
    assert_eq!(
        xml_text(section, "StorageClass").as_deref(),
        Some(storage_class),
        "listed version storage class: {section}"
    );
}

fn xml_sections<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut remaining = xml;
    let mut sections = Vec::new();
    while let Some(start) = remaining.find(&open) {
        let after_open = &remaining[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            panic!("unterminated <{tag}> in XML: {xml}");
        };
        sections.push(&after_open[..end]);
        remaining = &after_open[end + close.len()..];
    }
    sections
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_sections(xml, tag).into_iter().next().map(str::to_owned)
}

#[allow(clippy::too_many_arguments)]
async fn seed_content_version(
    db: &sea_orm::DatabaseConnection,
    key: &str,
    cid: &str,
    public_version_id: Option<&str>,
    sequence: i64,
    is_latest: bool,
    tier: &str,
    storage_class: &str,
    size: usize,
    created_at: chrono::DateTime<Utc>,
    became_noncurrent_at: Option<chrono::DateTime<Utc>>,
    encryption: EncryptionEnvelope,
) -> SeededRow {
    let object_id = uuid::Uuid::new_v4().to_string();
    let version_row_id = uuid::Uuid::new_v4().to_string();
    object::Entity::insert(object::ActiveModel {
        id: Set(object_id.clone()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        cid: Set(cid.to_owned()),
        size: Set(i64::try_from(size).expect("reporting fixture size fits i64")),
        content_type: Set(Some("application/octet-stream".to_owned())),
        etag: Set(cid.to_owned()),
        metadata: Set(None),
        encrypted: Set(encryption.encrypted),
        key_wrap: Set(encryption.key_wrap),
        sse_c_key_fingerprint: Set(encryption.sse_c_key_fingerprint),
        multipart: Set(false),
        is_latest: Set(is_latest),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed reporting object");
    object_version::Entity::insert(object_version::ActiveModel {
        id: Set(version_row_id.clone()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        version_id: Set(public_version_id.map(str::to_owned)),
        kind: Set("object".to_owned()),
        object_id: Set(Some(object_id.clone())),
        sequence: Set(sequence),
        is_latest: Set(is_latest),
        lifecycle_age_started_at: Set(created_at),
        became_noncurrent_at: Set(became_noncurrent_at),
        created_at: Set(created_at),
        updated_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed reporting object version");
    version_residency::Entity::insert(version_residency::ActiveModel {
        version_row_id: Set(version_row_id.clone()),
        object_id: Set(object_id.clone()),
        primary_tier: Set(tier.to_owned()),
        storage_class: Set(storage_class.to_owned()),
        cid: Set(cid.to_owned()),
        revision: Set(1),
        created_at: Set(created_at),
        updated_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed reporting version residency");
    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set("version".to_owned()),
        owner_id: Set(version_row_id.clone()),
        reason: Set("retained_version".to_owned()),
        version_row_id: Set(version_row_id.clone()),
        object_id: Set(object_id.clone()),
        tier: Set(tier.to_owned()),
        cid: Set(cid.to_owned()),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed reporting residency reference");

    SeededRow {
        object_id,
        version_row_id,
    }
}

async fn seed_delete_marker(
    db: &sea_orm::DatabaseConnection,
    key: &str,
    version_id: &str,
    sequence: i64,
    created_at: chrono::DateTime<Utc>,
) {
    object_version::Entity::insert(object_version::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        version_id: Set(Some(version_id.to_owned())),
        kind: Set("delete_marker".to_owned()),
        object_id: Set(None),
        sequence: Set(sequence),
        is_latest: Set(true),
        lifecycle_age_started_at: Set(created_at),
        became_noncurrent_at: Set(None),
        created_at: Set(created_at),
        updated_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed reporting delete marker");
}

async fn seed_physical(
    db: &sea_orm::DatabaseConnection,
    tier: &str,
    cid: &str,
    verified: bool,
    now: chrono::DateTime<Utc>,
) {
    let (node_identity, verification_state, verification_receipt, verified_at) = if verified {
        (
            Some(COLD_NODE_ID.to_owned()),
            "verified".to_owned(),
            Some(
                serde_json::to_string(&LocalResidencyVerificationReceipt {
                    node_identity: COLD_NODE_ID.to_owned(),
                    cid: cid.to_owned(),
                })
                .expect("serialize reporting cold verification receipt"),
            ),
            Some(now),
        )
    } else {
        (None, "pending".to_owned(), None, None)
    };
    physical_residency::Entity::insert(physical_residency::ActiveModel {
        tier: Set(tier.to_owned()),
        cid: Set(cid.to_owned()),
        node_identity: Set(node_identity),
        verification_state: Set(verification_state),
        verification_receipt: Set(verification_receipt),
        verified_at: Set(verified_at),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await
    .expect("seed reporting physical residency");
}

type ImmutableSnapshot = (
    object::Model,
    object_version::Model,
    version_residency::Model,
);

async fn immutable_snapshot(
    db: &sea_orm::DatabaseConnection,
    row: &SeededRow,
) -> ImmutableSnapshot {
    (
        object::Entity::find_by_id(&row.object_id)
            .one(db)
            .await
            .expect("snapshot object query")
            .expect("snapshot object"),
        object_version::Entity::find_by_id(&row.version_row_id)
            .one(db)
            .await
            .expect("snapshot version query")
            .expect("snapshot version"),
        version_residency::Entity::find_by_id(&row.version_row_id)
            .one(db)
            .await
            .expect("snapshot residency query")
            .expect("snapshot residency"),
    )
}

fn fixed_ciphertext(key: &ObjectKey, nonce: [u8; 12]) -> Vec<u8> {
    ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(key, &nonce, ENCRYPTED_PLAINTEXT)
        .expect("encrypt reporting fixture chunk")
        .to_vec()
}

fn sse_c_headers(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("SSE-C key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("SSE-C key MD5 header"),
    );
    headers
}

fn assert_sse_c_response_headers(response: &reqwest::Response, key: [u8; 32]) {
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    assert_eq!(
        response.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        response.headers()["x-amz-server-side-encryption-customer-key-md5"],
        md5_b64
    );
    assert!(
        response
            .headers()
            .get("x-amz-server-side-encryption")
            .is_none(),
        "SSE-C must not be reported as SSE-S3"
    );
}
