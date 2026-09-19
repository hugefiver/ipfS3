//! Signed real-service acceptance support for immutable tier-routed reads.

mod eof;
mod reporting;

use std::{collections::HashMap, sync::Arc};

use chrono::{Duration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
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

use super::{
    decompress::{KuboScript, S3ServerHandle, start_kubo_harness, start_s3_server},
    sigv4::send_sigv4,
};

const BUCKET: &str = "tier-read-bucket";
const COLD_NODE_ID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
const COLD_VERSION_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const HOT_VERSION_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
const NULL_VERSION_CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

const VERSIONED_KEY: &str = "immutable/versioned.bin";
const NULL_KEY: &str = "immutable/null.bin";
const FAILURE_KEY: &str = "immutable/cold-failure.bin";
const COLD_BYTES: &[u8] = b"historical-cold-bytes";
const HOT_BYTES: &[u8] = b"current-hot-bytes";
const NULL_BYTES: &[u8] = b"null-cold-bytes";

struct SeededVersions {
    historical: String,
    current: String,
}

struct TierReadHarness {
    endpoint: String,
    state: Arc<AppState>,
    hot: wiremock::MockServer,
    cold: wiremock::MockServer,
    server: S3ServerHandle,
}

pub async fn assert_signed_tier_reads() {
    let harness = start_harness().await;
    let versions = seed_immutable_residencies(&harness.state).await;

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=5-12"));
    let historical = signed_get(
        &harness.endpoint,
        VERSIONED_KEY,
        Some(&versions.historical),
        range_headers,
    )
    .await;
    assert_eq!(historical.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(historical.headers()[http::header::CONTENT_LENGTH], "8");
    assert_eq!(
        historical.headers()[http::header::CONTENT_RANGE],
        format!("bytes 5-12/{}", COLD_BYTES.len())
    );
    assert_eq!(
        historical.headers()[http::header::ETAG],
        format!("\"{COLD_VERSION_CID}\"")
    );
    assert_eq!(
        historical.headers()["x-amz-version-id"],
        versions.historical
    );
    assert_eq!(
        historical.bytes().await.expect("historical range body"),
        &COLD_BYTES[5..=12]
    );

    let current = signed_get(&harness.endpoint, VERSIONED_KEY, None, HeaderMap::new()).await;
    assert_eq!(current.status(), StatusCode::OK);
    assert_eq!(
        current.headers()[http::header::ETAG],
        format!("\"{HOT_VERSION_CID}\"")
    );
    assert_eq!(current.headers()["x-amz-version-id"], versions.current);
    assert_eq!(current.bytes().await.expect("current hot body"), HOT_BYTES);

    for version_id in [None, Some("null")] {
        let response = signed_get(&harness.endpoint, NULL_KEY, version_id, HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-amz-version-id"], "null");
        assert_eq!(
            response.headers()[http::header::ETAG],
            format!("\"{NULL_VERSION_CID}\"")
        );
        assert_eq!(
            response.bytes().await.expect("null-version body"),
            NULL_BYTES
        );
    }

    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/id"))
        .and(matchers::query_param("peerid-base", "b58mh"))
        .respond_with(
            ResponseTemplate::new(StatusCode::SERVICE_UNAVAILABLE.as_u16())
                .set_body_string("cold rpc at https://private.invalid/?token=do-not-leak"),
        )
        .with_priority(1)
        .expect(1)
        .mount(&harness.cold)
        .await;

    let failed = signed_get(&harness.endpoint, FAILURE_KEY, None, HeaderMap::new()).await;
    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let failed_body = failed.text().await.expect("cold failure XML body");
    assert!(failed_body.contains("<Code>InternalError</Code>"));
    assert!(failed_body.contains("<Message>internal storage backend error</Message>"));
    for forbidden in [
        "private.invalid",
        "do-not-leak",
        "cold_not_configured",
        "tier_unavailable",
        &harness.cold.uri(),
    ] {
        assert!(
            !failed_body.contains(forbidden),
            "cold error leaked private detail {forbidden:?}: {failed_body}"
        );
    }

    assert_tier_requests(&harness).await;
    harness.server.shutdown().await;
}

pub async fn assert_signed_fixed_length_gets_wait_for_kubo_eof() {
    eof::assert_fixed_length_gets_wait_for_kubo_eof().await;
}

async fn start_harness() -> TierReadHarness {
    let hot = start_kubo_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::from([
            (HOT_VERSION_CID.to_owned(), HOT_BYTES.to_vec()),
            // Deliberately available on hot: immutable cold routing must still never use it.
            (COLD_VERSION_CID.to_owned(), b"wrong-hot-fallback".to_vec()),
        ]),
    })
    .await
    .server;
    let cold = start_kubo_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::from([
            (COLD_VERSION_CID.to_owned(), COLD_BYTES.to_vec()),
            (NULL_VERSION_CID.to_owned(), NULL_BYTES.to_vec()),
        ]),
    })
    .await
    .server;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/cat"))
        .and(matchers::query_param("arg", COLD_VERSION_CID))
        .and(matchers::query_param("offset", "5"))
        .and(matchers::query_param("length", "8"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(&COLD_BYTES[5..=12]))
        .with_priority(1)
        .expect(1)
        .mount(&cold)
        .await;
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/id"))
        .and(matchers::query_param("peerid-base", "b58mh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ID": COLD_NODE_ID
        })))
        .expect(3)
        .up_to_n_times(3)
        .mount(&cold)
        .await;

    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect tier-read SQLite database");
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .expect("enable tier-read SQLite foreign keys");
    store::run_migrations(&db)
        .await
        .expect("run tier-read migrations");
    store::bucket::create(&db, BUCKET, Some("tier-read-owner"))
        .await
        .expect("create tier-read bucket");
    let state = Arc::new(AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(hot.uri()),
        cold_kubo: Some(ipfs_s3_gateway::kubo::KuboClient::new(cold.uri())),
        store: Store::new(db),
        credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64))
            .expect("tier-read master key"),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    let server =
        start_s3_server(state.clone(), Arc::new(tokio::sync::Mutex::new(Vec::new()))).await;

    TierReadHarness {
        endpoint: server.endpoint.clone(),
        state,
        hot,
        cold,
        server,
    }
}

async fn seed_immutable_residencies(state: &Arc<AppState>) -> SeededVersions {
    let db = state.store.db();
    let now = Utc::now();
    seed_physical(db, "cold", COLD_VERSION_CID, true, now).await;
    seed_physical(db, "hot", HOT_VERSION_CID, false, now).await;
    seed_physical(db, "cold", NULL_VERSION_CID, true, now).await;

    let historical = uuid::Uuid::new_v4().to_string();
    let current = uuid::Uuid::new_v4().to_string();
    seed_version(
        db,
        VERSIONED_KEY,
        COLD_VERSION_CID,
        Some(&historical),
        1,
        false,
        "cold",
        "STANDARD_IA",
        now - Duration::seconds(2),
        Some(now - Duration::seconds(1)),
    )
    .await;
    seed_version(
        db,
        VERSIONED_KEY,
        HOT_VERSION_CID,
        Some(&current),
        2,
        true,
        "hot",
        "STANDARD",
        now,
        None,
    )
    .await;
    seed_version(
        db,
        NULL_KEY,
        NULL_VERSION_CID,
        None,
        1,
        true,
        "cold",
        "STANDARD_IA",
        now,
        None,
    )
    .await;
    seed_version(
        db,
        FAILURE_KEY,
        COLD_VERSION_CID,
        Some(&uuid::Uuid::new_v4().to_string()),
        1,
        true,
        "cold",
        "STANDARD_IA",
        now,
        None,
    )
    .await;
    store::bucket::set_versioning_state(db, BUCKET, BucketVersioningState::Suspended)
        .await
        .expect("enable explicit null-version reads");

    SeededVersions {
        historical,
        current,
    }
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
                .expect("serialize cold verification receipt"),
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
    .expect("seed physical residency");
}

#[allow(clippy::too_many_arguments)]
async fn seed_version(
    db: &sea_orm::DatabaseConnection,
    key: &str,
    cid: &str,
    public_version_id: Option<&str>,
    sequence: i64,
    is_latest: bool,
    tier: &str,
    storage_class: &str,
    created_at: chrono::DateTime<Utc>,
    became_noncurrent_at: Option<chrono::DateTime<Utc>>,
) {
    let object_id = uuid::Uuid::new_v4().to_string();
    let version_row_id = uuid::Uuid::new_v4().to_string();
    let size = match cid {
        COLD_VERSION_CID => COLD_BYTES.len(),
        HOT_VERSION_CID => HOT_BYTES.len(),
        NULL_VERSION_CID => NULL_BYTES.len(),
        _ => unreachable!("known tier-read fixture CID"),
    };
    object::Entity::insert(object::ActiveModel {
        id: Set(object_id.clone()),
        bucket: Set(BUCKET.to_owned()),
        key: Set(key.to_owned()),
        cid: Set(cid.to_owned()),
        size: Set(i64::try_from(size).expect("fixture size fits i64")),
        content_type: Set(Some("application/octet-stream".to_owned())),
        etag: Set(cid.to_owned()),
        metadata: Set(None),
        encrypted: Set(false),
        key_wrap: Set(None),
        sse_c_key_fingerprint: Set(None),
        multipart: Set(false),
        is_latest: Set(is_latest),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed immutable object");
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
    .expect("seed immutable version index");
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
    .expect("seed immutable version residency");
    residency_reference::Entity::insert(residency_reference::ActiveModel {
        owner_kind: Set("version".to_owned()),
        owner_id: Set(version_row_id.clone()),
        reason: Set("retained_version".to_owned()),
        version_row_id: Set(version_row_id),
        object_id: Set(object_id),
        tier: Set(tier.to_owned()),
        cid: Set(cid.to_owned()),
        created_at: Set(created_at),
    })
    .exec(db)
    .await
    .expect("seed immutable retained-version reference");
}

async fn signed_get(
    endpoint: &str,
    key: &str,
    version_id: Option<&str>,
    headers: HeaderMap,
) -> reqwest::Response {
    let query = version_id.map_or_else(Vec::new, |version| vec![("versionId", version)]);
    send_sigv4(
        reqwest::Method::GET,
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

async fn assert_tier_requests(harness: &TierReadHarness) {
    let hot = harness
        .hot
        .received_requests()
        .await
        .expect("hot request log");
    let hot_cats = hot
        .iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(hot_cats.len(), 1, "only the current hot version uses hot");
    assert_eq!(
        query_value(hot_cats[0], "offline"),
        None,
        "hot read behavior is unchanged"
    );
    assert_eq!(
        query_value(hot_cats[0], "arg").as_deref(),
        Some(HOT_VERSION_CID)
    );

    let cold = harness
        .cold
        .received_requests()
        .await
        .expect("cold request log");
    let cold_cats = cold
        .iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(cold_cats.len(), 3);
    assert!(
        cold_cats
            .iter()
            .all(|request| query_value(request, "offline").as_deref() == Some("true")),
        "cold cat must not fetch missing blocks from swarm"
    );
    assert_eq!(
        query_value(cold_cats[0], "arg").as_deref(),
        Some(COLD_VERSION_CID)
    );
    assert_eq!(query_value(cold_cats[0], "offset").as_deref(), Some("5"));
    assert_eq!(query_value(cold_cats[0], "length").as_deref(), Some("8"));
    assert_eq!(
        cold_cats[1..]
            .iter()
            .map(|request| query_value(request, "arg"))
            .collect::<Vec<_>>(),
        vec![
            Some(NULL_VERSION_CID.to_owned()),
            Some(NULL_VERSION_CID.to_owned())
        ]
    );
    assert_eq!(
        cold.iter()
            .filter(|request| request.url.path() == "/api/v0/id")
            .count(),
        4,
        "every cold selection verifies the receipt-bound node"
    );
}

fn query_value(request: &wiremock::Request, name: &str) -> Option<String> {
    request
        .url
        .query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}
