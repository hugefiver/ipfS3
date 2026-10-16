#[path = "support/cluster_provider_real_car.rs"]
mod car_fixture;
#[path = "support/stage7_management_directory_stream.rs"]
mod stream_fixture;
#[path = "support/stage7_management.rs"]
pub mod support;

use ipfs_s3_gateway::{
    kubo::{KuboClient, LocalDirectoryRootKind},
    pinning::management::{
        ExactObject,
        evidence::{self, ReadProof},
    },
    store::pinning::management::{native, records},
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

// Two different directory names reference the same raw block. Neither logical
// file size nor CAR framing is part of the within-root unique payload receipt.
type DirectoryBlocks = Vec<(String, Vec<u8>)>;

fn shared_directory() -> (String, DirectoryBlocks, Vec<u8>) {
    let child = car_fixture::raw_cid(b"hello", 0x55);
    let (_, single) = car_fixture::directory(&child);
    let mut root = single.clone();
    let mut other = single[4..].to_vec();
    let name = other
        .windows(5)
        .position(|bytes| bytes == b"entry")
        .unwrap();
    other[name..name + 5].copy_from_slice(b"other");
    root.extend(other);
    let cid = car_fixture::raw_cid(&root, 0x70);
    let blocks = vec![(cid.clone(), root), (child, b"hello".to_vec())];
    let car = car_fixture::encode(std::slice::from_ref(&cid), &blocks);
    (cid, blocks, car)
}

async fn source(server: &MockServer, cid: &str, car: Vec<u8>) {
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID": support::NODE})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .and(query_param("arg", cid))
        .and(query_param("type", "recursive"))
        .and(query_param("offline", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Keys": {(cid): {"Type": "recursive"}}})),
        )
        .mount(server)
        .await;
    Mock::given(method("POST")).and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{cid}"))).and(query_param("with-local", "true")).and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":cid,"WithLocality":true,"Local":true,"SizeLocal":"999999","Type":"directory"})))
        .mount(server).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/export"))
        .and(query_param("arg", cid))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(car))
        .mount(server)
        .await;
}

async fn database(
    server: &MockServer,
    cid: &str,
) -> (
    tempfile::TempDir,
    DatabaseConnection,
    ipfs_s3_gateway::config::Config,
    ipfs_s3_gateway::pinning::management::local_config::ManagementConfig,
    ExactObject,
) {
    let raw = support::config(&server.uri());
    let config = support::management_config(&raw);
    let (directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish_cid(&db, &config, "directory", cid, false).await;
    (directory, db, raw, config, identity)
}

fn request(identity: ExactObject) -> evidence::PrepareRequest {
    evidence::PrepareRequest {
        record_id: "directory-proof".into(),
        identity,
        expected_node: support::NODE.into(),
        max_stored_bytes: 107,
        ttl: chrono::Duration::minutes(5),
        directory: true,
    }
}

#[tokio::test]
async fn complete_offline_stream_bills_unique_raw_blocks_not_car_frames_or_logical_size() {
    let (cid, _, car) = shared_directory();
    assert_eq!(car.len(), 241);
    let server = MockServer::start().await;
    source(&server, &cid, car).await;
    let (_directory, db, raw, config, identity) = database(&server, &cid).await;
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let client = KuboClient::new(server.uri());
    let mut request = request(identity);
    let before = native::capture(&db).await.unwrap();
    let proof = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(proof.proof, ReadProof::CompleteLocalDirectoryDagBlocksV1);
    assert_eq!(proof.stored_bytes, Some(107));
    assert_eq!(native::value(&proof.object_tuple, "size"), Some("10"));
    assert_eq!(
        proof.directory_root_kind(),
        Some(LocalDirectoryRootKind::Directory)
    );
    let receipt = proof.directory_receipt.as_ref().unwrap();
    assert_eq!(receipt.stored_block_bytes(), 107);
    assert_eq!(receipt.serialized_car_bytes(), 241);
    assert_eq!(receipt.block_count(), 2);
    assert!(
        before.evidence_inventory() == native::capture(&db).await.unwrap().evidence_inventory()
    );
    let replay = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(proof == replay); // no new walk and no TTL extension
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v0/dag/export")
            .count(),
        1
    );
    request.max_stored_bytes += 1;
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    request.max_stored_bytes -= 1;
    db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"changed\"}'")
        .await
        .unwrap();
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| matches!(
                r.url.path(),
                "/api/v0/id" | "/api/v0/pin/ls" | "/api/v0/files/stat" | "/api/v0/dag/export"
            ))
    );
}

#[tokio::test]
async fn incomplete_corrupt_unrelated_duplicate_and_multiroot_cars_never_attest() {
    let (cid, blocks, _) = shared_directory();
    for (case, car) in car_fixture::invalid_cars(&cid, &blocks) {
        let server = MockServer::start().await;
        source(&server, &cid, car).await;
        let (_directory, db, raw, config, identity) = database(&server, &cid).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let before = native::capture(&db).await.unwrap();
        assert!(
            evidence::prepare(
                &db,
                &config,
                &auth,
                &KuboClient::new(server.uri()),
                &request(identity),
                CancellationToken::new()
            )
            .await
            .is_err(),
            "{case}"
        );
        assert!(before == native::capture(&db).await.unwrap(), "{case}");
    }
}

#[tokio::test]
async fn caller_directory_flag_and_stat_directory_claim_cannot_type_a_file_root() {
    let cid = car_fixture::raw_cid(b"hello", 0x55);
    let car = car_fixture::encode(
        std::slice::from_ref(&cid),
        &[(cid.clone(), b"hello".to_vec())],
    );
    let server = MockServer::start().await;
    source(&server, &cid, car).await;
    let (_directory, db, raw, config, identity) = database(&server, &cid).await;
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let before = native::capture(&db).await.unwrap();
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &KuboClient::new(server.uri()),
            &request(identity),
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(before == native::capture(&db).await.unwrap());
}

#[tokio::test]
async fn payload_budget_and_future_framing_overflow_fail_without_records() {
    for cap in [106, u64::MAX] {
        let (cid, _, car) = shared_directory();
        let server = MockServer::start().await;
        source(&server, &cid, car).await;
        let (_directory, db, raw, config, identity) = database(&server, &cid).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let before = native::capture(&db).await.unwrap();
        let mut request = request(identity);
        request.max_stored_bytes = cap;
        assert!(
            evidence::prepare(
                &db,
                &config,
                &auth,
                &KuboClient::new(server.uri()),
                &request,
                CancellationToken::new()
            )
            .await
            .is_err()
        );
        assert!(before == native::capture(&db).await.unwrap());
        if cap == u64::MAX {
            assert!(server.received_requests().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn fully_received_car_still_needs_clean_eof_same_peer_fresh_tuples_authorization_and_ttl() {
    for case in [
        "header",
        "car_budget_header",
        "trailer",
        "partial_trailer",
        "late_eof",
        "idle",
        "cancel",
        "node_before",
        "node_after",
        "metadata",
        "version",
        "authorization",
        "expired",
    ] {
        let (cid, _, car) = shared_directory();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let raw = support::config(&endpoint);
        let config = support::management_config(&raw);
        let (_directory, db) = support::database().await;
        support::setup(&db, &config).await;
        let identity = support::publish_cid(&db, &config, "directory", &cid, false).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let cancel = CancellationToken::new();
        let task = stream_fixture::serve(
            listener,
            case,
            db.clone(),
            cid,
            car,
            cancel.clone(),
            support::NODE,
        );
        let mut request = request(identity);
        if case == "expired" {
            request.ttl = chrono::Duration::milliseconds(20);
        }
        let before = native::capture(&db).await.unwrap();
        let idle = std::time::Duration::from_millis(if case == "idle" { 30 } else { 1000 });
        let client =
            KuboClient::new_with_timeouts(endpoint, std::time::Duration::from_secs(5), idle);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            evidence::prepare(&db, &config, &auth, &client, &request, cancel),
        )
        .await
        .unwrap();
        assert!(result.is_err(), "{case}");
        let count = db.query_one(sea_orm::Statement::from_string(sea_orm::DatabaseBackend::Sqlite, "SELECT COUNT(*) AS count FROM pin_management_records WHERE kind='evidence_preparation'".to_owned())).await.unwrap().unwrap().try_get::<i64>("", "count").unwrap();
        assert_eq!(count, 0, "{case}");
        if !matches!(case, "metadata" | "version" | "authorization") {
            assert!(before == native::capture(&db).await.unwrap(), "{case}");
        }
        if matches!(case, "metadata" | "version") {
            assert!(
                before.evidence_inventory()
                    != native::capture(&db).await.unwrap().evidence_inventory(),
                "{case}"
            );
        }
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn hamt_and_dag_cbor_children_use_the_same_complete_payload_validator() {
    let raw = car_fixture::raw_cid(b"hello", 0x55);
    let mut cbor = b"\xa1\x61l\xd8\x2a\x58\x25\x00".to_vec();
    cbor.extend(cid::Cid::try_from(raw.as_str()).unwrap().to_bytes());
    let child = car_fixture::raw_cid(&cbor, 0x71);
    let (directory, directory_bytes) = car_fixture::directory(&child);
    let hamt_bytes = b"\x0a\x07\x08\x05\x28\x22\x30\x80\x02".to_vec();
    let hamt = car_fixture::raw_cid(&hamt_bytes, 0x70);
    for (root, blocks, kind, expected) in [
        (
            directory.clone(),
            vec![
                (directory, directory_bytes),
                (child, cbor),
                (raw, b"hello".to_vec()),
            ],
            LocalDirectoryRootKind::Directory,
            102,
        ),
        (
            hamt.clone(),
            vec![(hamt, hamt_bytes)],
            LocalDirectoryRootKind::HamtShard,
            9,
        ),
    ] {
        let car = car_fixture::encode(std::slice::from_ref(&root), &blocks);
        let server = MockServer::start().await;
        source(&server, &root, car).await;
        let (_directory, db, raw, config, identity) = database(&server, &root).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let proof = evidence::prepare(
            &db,
            &config,
            &auth,
            &KuboClient::new(server.uri()),
            &request(identity),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(proof.stored_bytes, Some(expected));
        assert_eq!(proof.directory_root_kind(), Some(kind));
    }
}

#[tokio::test]
async fn old_size_less_directory_rid_decodes_and_replays_without_upgrade_or_ttl_extension() {
    let (cid, _, car) = shared_directory();
    let server = MockServer::start().await;
    source(&server, &cid, car).await;
    let (_directory, db, raw, config, identity) = database(&server, &cid).await;
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let client = KuboClient::new(server.uri());
    let mut request = request(identity);
    let original = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut old = serde_json::to_value(&original).unwrap();
    old["proof"] = serde_json::json!("complete_local_directory");
    old["stored_bytes"] = serde_json::Value::Null;
    old.as_object_mut().unwrap().remove("directory_receipt");
    let legacy = serde_json::to_string(&(
        &request.identity,
        &request.expected_node,
        request.max_stored_bytes,
        request.directory,
        request.ttl.num_nanoseconds().unwrap(),
    ))
    .unwrap();
    let result = serde_json::to_string(&(old.clone(), server.uri())).unwrap();
    db.execute(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE pin_management_records SET binding=$1,result=$2 WHERE kind='evidence_preparation'",
        [legacy.into(), result.into()],
    ))
    .await
    .unwrap();
    let before = native::capture(&db).await.unwrap();
    let replay = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(replay.proof, ReadProof::CompleteLocalDirectory);
    assert_eq!(replay.stored_bytes, None);
    assert!(replay.directory_receipt.is_none());
    assert_eq!(replay.expires_at, original.expires_at);
    assert!(before == native::capture(&db).await.unwrap());
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v0/dag/export")
            .count(),
        1
    );
    // Historical bounds are not silently replaced by the new directory profile.
    // This RID remains size-less even when its old cap cannot form a v1 CAR cap.
    request.max_stored_bytes = u64::MAX;
    let legacy = serde_json::to_string(&(
        &request.identity,
        &request.expected_node,
        request.max_stored_bytes,
        request.directory,
        request.ttl.num_nanoseconds().unwrap(),
    ))
    .unwrap();
    db.execute(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE pin_management_records SET binding=$1 WHERE kind='evidence_preparation'",
        [legacy.into()],
    ))
    .await
    .unwrap();
    let before = native::capture(&db).await.unwrap();
    let replay = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(replay.stored_bytes, None);
    assert_eq!(replay.expires_at, original.expires_at);
    assert!(before == native::capture(&db).await.unwrap());
    old["stored_bytes"] = serde_json::json!(107); // old kind cannot acquire today's meaning
    let result = serde_json::to_string(&(old, server.uri())).unwrap();
    db.execute(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE pin_management_records SET result=$1 WHERE kind='evidence_preparation'",
        [result.into()],
    ))
    .await
    .unwrap();
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn new_rid_binds_endpoint_idle_peer_exact_object_and_proof_kind_without_recapture() {
    let (cid, _, car) = shared_directory();
    let server = MockServer::start().await;
    source(&server, &cid, car).await;
    let (_directory, db, raw, config, identity) = database(&server, &cid).await;
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let client = KuboClient::new(server.uri());
    let mut request = request(identity);
    evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let before = native::capture(&db).await.unwrap();
    let calls = server.received_requests().await.unwrap().len();
    let changed_idle = KuboClient::new_with_timeouts(
        server.uri(),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(2),
    );
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &changed_idle,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let same_millis_different_idle = KuboClient::new_with_timeouts(
        server.uri(),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_nanos(120_000_000_001),
    );
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &same_millis_different_idle,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let other = MockServer::start().await;
    let mut changed_raw = raw.clone();
    changed_raw.kubo.rpc_url = other.uri();
    let changed_config = support::management_config(&changed_raw);
    assert!(
        evidence::prepare(
            &db,
            &changed_config,
            &auth,
            &KuboClient::new(other.uri()),
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    let changed_config =
        ipfs_s3_gateway::pinning::management::local_config::compile(&raw, |_| None)
            .unwrap()
            .with_source_node("DifferentPeer".into())
            .unwrap();
    request.expected_node = "DifferentPeer".into();
    assert!(
        evidence::prepare(
            &db,
            &changed_config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    request.expected_node = support::NODE.into();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        evidence::prepare(&db, &config, &auth, &client, &request, cancel)
            .await
            .is_err()
    );
    assert!(before == native::capture(&db).await.unwrap());
    assert_eq!(server.received_requests().await.unwrap().len(), calls);
    assert!(other.received_requests().await.unwrap().is_empty());

    // A stored new binding cannot be downgraded to file or legacy directory
    // interpretation by changing the saved proof tag alone.
    db.execute_unprepared("UPDATE pin_management_records SET result=json_remove(json_set(result,'$[0].proof','file_clean_eof'),'$[0].directory_receipt') WHERE kind='evidence_preparation'").await.unwrap();
    let before = native::capture(&db).await.unwrap();
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(before == native::capture(&db).await.unwrap());
    assert_eq!(server.received_requests().await.unwrap().len(), calls);
    let replacement =
        support::publish_cid(&db, &config, &request.identity.key, support::CID, false).await;
    request.identity = replacement;
    let before = native::capture(&db).await.unwrap();
    assert!(
        evidence::prepare(
            &db,
            &config,
            &auth,
            &client,
            &request,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(before == native::capture(&db).await.unwrap());
    assert_eq!(server.received_requests().await.unwrap().len(), calls);
}

#[tokio::test]
async fn file_clean_eof_keeps_legacy_shape_and_ciphertext_payload_count() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ciphertext-17byte"))
        .mount(&server)
        .await;
    let (_directory, db, raw, config, identity) = database(&server, support::CID).await;
    db.execute_unprepared("UPDATE objects SET encrypted=TRUE")
        .await
        .unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let client = KuboClient::new(server.uri());
    let mut request = request(identity);
    request.directory = false;
    request.max_stored_bytes = 17;
    let proof = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(proof.proof, ReadProof::FileCleanEof);
    assert_eq!(proof.stored_bytes, Some(17));
    assert_eq!(native::value(&proof.object_tuple, "size"), Some("10"));
    assert!(proof.directory_receipt.is_none());
    assert!(
        serde_json::to_value(&proof)
            .unwrap()
            .get("directory_receipt")
            .is_none()
    );
    let replay = evidence::prepare(
        &db,
        &config,
        &auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(proof == replay);
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == "/api/v0/cat")
            .count(),
        1
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| matches!(r.url.path(), "/api/v0/id" | "/api/v0/cat"))
    );
}

#[tokio::test]
async fn directory_control_export_and_replay_redirects_cannot_drop_offline_or_change_endpoint() {
    for redirected in [
        "/api/v0/id",
        "/api/v0/pin/ls",
        "/api/v0/files/stat",
        "/api/v0/dag/export",
        "replay",
    ] {
        let (cid, _, car) = shared_directory();
        let server = MockServer::start().await;
        let destination = MockServer::start().await;
        source(&server, &cid, car).await;
        let (_directory, db, raw, config, identity) = database(&server, &cid).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let client = KuboClient::new(server.uri());
        let request = request(identity);
        if redirected == "replay" {
            evidence::prepare(
                &db,
                &config,
                &auth,
                &client,
                &request,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        }
        Mock::given(path(if redirected == "replay" {
            "/api/v0/id"
        } else {
            redirected
        }))
        .respond_with(ResponseTemplate::new(307).insert_header(
            "Location",
            format!("{}/replacement?offline=false", destination.uri()),
        ))
        .with_priority(1)
        .mount(&server)
        .await;
        let before = native::capture(&db).await.unwrap();
        assert!(
            evidence::prepare(
                &db,
                &config,
                &auth,
                &client,
                &request,
                CancellationToken::new()
            )
            .await
            .is_err(),
            "{redirected}"
        );
        assert!(
            before == native::capture(&db).await.unwrap(),
            "{redirected}"
        );
        assert!(
            destination.received_requests().await.unwrap().is_empty(),
            "{redirected}"
        );
    }
}
