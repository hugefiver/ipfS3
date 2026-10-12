//! Direct/MPU capture and replay seams, independent of the frozen QA suites.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}

use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{
    pinning::zip_policy::CapturedZipIntent,
    store::zip::{self, execution},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use sha2::{Digest, Sha256};
use support::{
    stage9_zip_replay::{Entry, Outcome, run},
    stage9_zip_replay_requests::{self as request, Ledger},
};

#[tokio::test]
async fn historical_direct_receipt_keeps_contract_and_complete_raw_digest_without_new_headers() {
    run(Entry::Direct, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let archive = Outcome::Complete.archive();
            let headers = request::controls("historical-direct");
            let first = request::put(&h, archive.clone(), headers.clone(), "out/").await;
            assert_eq!(first.status, StatusCode::OK, "{first:?}");
            let id = first.id(false);
            let original = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
            assert_eq!(original.input_sha256.as_deref(), Some(hex::encode(Sha256::digest(&archive)).as_str()));
            assert_eq!(original.input_art_size, Some(archive.len() as i64));
            let mut contract: serde_json::Value = serde_json::from_str(&original.request_contract).unwrap();
            let object = contract.as_object_mut().unwrap();
            for key in ["contract_version", "source", "content_type", "metadata", "tags"] {
                object.remove(key);
            }
            let digest = hex::encode(Sha256::digest(&archive));
            let mut signed = serde_json::Map::new();
            let original_contract: serde_json::Value = serde_json::from_str(&original.request_contract).unwrap();
            if let Some(content_type) = original_contract["content_type"].as_str() {
                signed.insert("content-type".into(), serde_json::json!([content_type]));
            }
            for (name, value) in &headers {
                signed.insert(name.to_string(), serde_json::json!([value.to_str().unwrap()]));
            }
            signed.insert("x-ipfs3-zip-expected-sha256".into(), serde_json::json!([digest]));
            object.insert("signed_headers".into(), serde_json::Value::Object(signed));
            let contract = contract.to_string();
            let mut captured: serde_json::Value = serde_json::from_str(&original.captured_options).unwrap();
            captured.as_object_mut().unwrap().remove("zip_intent");
            captured["expected_sha256"] = serde_json::json!(digest);
            let captured = captured.to_string();
            // Seed the old immutable evidence only in this owned fixture DB.
            let tx = h.state.store.db().begin().await.unwrap();
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "UPDATE zip_v2_executions SET request_contract=?,request_fingerprint=?,captured_options=? WHERE id=?",
                vec![contract.clone().into(), hex::encode(Sha256::digest(contract.as_bytes())).into(), captured.clone().into(), id.clone().into()],
            )).await.unwrap();
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "UPDATE zip_batches SET captured_options=? WHERE id=?", vec![captured.into(), id.clone().into()],
            )).await.unwrap();
            tx.commit().await.unwrap();
            let before = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            h.restart(false).await;
            assert_eq!(request::put(&h, archive.clone(), headers.clone(), "out/").await, first);
            let mut changed = archive.clone();
            let length = changed.len();
            changed[length - 2..].copy_from_slice(&1u16.to_le_bytes());
            changed.push(b'!');
            assert_eq!(request::put(&h, changed, headers.clone(), "out/").await.status, StatusCode::CONFLICT);
            let mut changed_controls = headers.clone();
            changed_controls.insert("x-ipfs3-zip-publish-source", "true".parse().unwrap());
            assert_eq!(request::put(&h, archive.clone(), changed_controls, "out/").await.status, StatusCode::CONFLICT);
            let mut removed_control = headers;
            removed_control.insert("x-ipfs3-zip-expected-sha256", digest.parse().unwrap());
            assert_eq!(request::put(&h, archive, removed_control, "out/").await.status, StatusCode::BAD_REQUEST);
            assert_eq!(execution::read(h.state.store.db(), &id).await.unwrap().unwrap(), before);
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(h.adds().await, adds);
        }).catch_unwind().await;
        (h, result)
    }).await;
}

#[tokio::test]
async fn legacy_direct_first_admission_persists_the_shared_intent() {
    assert_direct_capture(false).await;
}

#[tokio::test]
async fn v2_direct_first_admission_persists_the_shared_intent() {
    assert_direct_capture(true).await;
}

async fn assert_direct_capture(v2: bool) {
    run(Entry::Direct, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
            let headers = if v2 {
                request::minimal(Some("captured-direct"))
            } else {
                HeaderMap::new()
            };
            let first = request::put(&h, Outcome::Complete.archive(), headers, "out/").await;
            assert_eq!(first.status, StatusCode::OK, "{first:?}");
            let id = first.id(false);
            let batch = zip::snapshot(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap()
                .batch;
            let saved: serde_json::Value = serde_json::from_str(&batch.captured_options).unwrap();
            assert!(
                saved.get("zip_intent").is_some(),
                "missing capture segment for v2={v2}: {saved}"
            );
            let capture = CapturedZipIntent::decode_json(&saved["zip_intent"].to_string()).unwrap();
            assert_eq!(capture.origin().request_id, id);
            assert_eq!(capture.origin().principal_id, "test");
            assert!(capture.controls().auto_pin_outputs);
            assert!(!capture.controls().targets.source);
            assert!(capture.controls().targets.extracted);
            assert!(!capture.controls().root.enabled());
            if v2 {
                let execution = execution::read(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(execution.captured_options, batch.captured_options);
            }
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}

#[tokio::test]
async fn legacy_mpu_create_and_restart_complete_keep_the_original_capture() {
    run(Entry::Mpu, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let created = request::create(&h, HeaderMap::new(), "out/").await;
            assert_eq!(created.status, StatusCode::OK, "{created:?}");
            let id = created.upload_id();
            let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                .await
                .unwrap();
            let capture_json = upload
                .zip_intent_json
                .as_ref()
                .expect("first Create capture");
            let capture = CapturedZipIntent::decode_json(capture_json).unwrap();
            let batch = zip::read(h.state.store.db(), &id).await.unwrap().unwrap();
            let envelope: serde_json::Value =
                serde_json::from_str(&batch.captured_options).unwrap();
            assert_eq!(
                CapturedZipIntent::decode_json(&envelope["zip_intent"].to_string()).unwrap(),
                capture
            );
            assert_eq!(capture.origin().request_id, id);
            assert_eq!(capture.origin().principal_id, "test");
            assert!(capture.controls().auto_pin_outputs);
            let etag = request::upload(&h, &id, Outcome::Complete.archive()).await;
            h.restart(false).await;
            let retained = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                .await
                .unwrap();
            assert_eq!(retained.zip_intent_json, upload.zip_intent_json);
            let first = request::complete(&h, &id, &etag, HeaderMap::new()).await;
            assert_eq!(first.status, StatusCode::OK, "{first:?}");
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            h.restart(false).await;
            assert_eq!(
                request::complete(&h, &id, &etag, HeaderMap::new()).await,
                first
            );
            let mut changed = HeaderMap::new();
            changed.insert("x-amz-tagging", "note=changed".parse().unwrap());
            assert_eq!(
                request::complete(&h, &id, &etag, changed).await.status,
                StatusCode::CONFLICT
            );
            assert_eq!(
                zip::read(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap()
                    .captured_options,
                batch.captured_options
            );
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(h.adds().await, adds);
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}

#[tokio::test]
async fn legacy_and_v2_mpu_failed_create_roll_back_admission_and_capture() {
    run(Entry::Mpu, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
            let db = h.state.store.db();
            let before = Ledger::read(&h).await;
            db.execute(Statement::from_string(DatabaseBackend::Sqlite,
                "CREATE TRIGGER r25_fail_zip_upload BEFORE INSERT ON multipart_uploads BEGIN SELECT RAISE(ABORT,'R25 injected Create failure'); END".to_owned(),
            )).await.unwrap();
            for headers in [HeaderMap::new(), request::minimal(Some("rollback-v2"))] {
                let failed = request::create(&h, headers, "out/").await;
                assert_eq!(failed.status, StatusCode::INTERNAL_SERVER_ERROR, "{failed:?}");
                let rows = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
                    "SELECT (SELECT COUNT(*) FROM multipart_uploads) AS uploads, (SELECT COUNT(*) FROM zip_batches) AS batches, (SELECT COUNT(*) FROM zip_v2_executions) AS executions, (SELECT COUNT(*) FROM zip_v2_mpu_intakes) AS intakes".to_owned(),
                )).await.unwrap().unwrap();
                for name in ["uploads", "batches", "executions", "intakes"] {
                    assert_eq!(rows.try_get::<i64>("", name).unwrap(), 0, "{name} must roll back with Create");
                }
                assert_eq!(Ledger::read(&h).await, before);
                assert_eq!(h.adds().await, 0);
            }
            db.execute(Statement::from_string(DatabaseBackend::Sqlite,
                "DROP TRIGGER r25_fail_zip_upload".to_owned(),
            )).await.unwrap();
        }).catch_unwind().await;
        (h, result)
    }).await;
}

#[tokio::test]
async fn r25_native_mpu_route_fence_preserves_first_capture_and_has_no_io() {
    for v2 in [false, true] {
        run(Entry::Mpu, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
                let headers = if v2 { request::minimal(Some("route-fence-v2")) } else { HeaderMap::new() };
                let created = request::create(&h, headers, "out/").await;
                assert_eq!(created.status, StatusCode::OK, "{created:?}");
                let id = created.upload_id();
                let etag = request::upload(&h, &id, Outcome::Complete.archive()).await;
                let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id).await.unwrap();
                let parts = ipfs_s3_gateway::store::multipart::list_parts(h.state.store.db(), &id).await.unwrap();
                let batch = zip::read(h.state.store.db(), &id).await.unwrap();
                let before_execution = execution::read(h.state.store.db(), &id).await.unwrap();
                let original_capture = batch.as_ref().map(|batch| &batch.captured_options)
                    .or_else(|| before_execution.as_ref().map(|execution| &execution.captured_options))
                    .expect("first capture is persisted before source I/O").clone();
                let ledger = Ledger::read(&h).await;
                let io = h.kubo.received_requests().await.unwrap().len();
                let provider = &h.state.pinning.effective_config().providers[0];
                let mut unavailable = provider.identity.clone();
                unavailable.credential_revision += 1;
                ipfs_s3_gateway::store::pinning::ledger::register_route(h.state.store.db(), &provider.name, &unavailable)
                    .await.unwrap();
                for _ in 0..2 {
                    let denied = request::complete(&h, &id, &etag, HeaderMap::new()).await;
                    assert_eq!(denied.status, StatusCode::BAD_REQUEST, "{denied:?}");
                    assert!(denied.body.contains("<Code>InvalidArgument</Code>"), "{denied:?}");
                    assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
                    assert_eq!(Ledger::read(&h).await, ledger);
                    assert_eq!(ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id).await.unwrap(), upload);
                    assert_eq!(ipfs_s3_gateway::store::multipart::list_parts(h.state.store.db(), &id).await.unwrap(), parts);
                    assert_eq!(zip::read(h.state.store.db(), &id).await.unwrap(), batch);
                    assert_eq!(execution::read(h.state.store.db(), &id).await.unwrap(), before_execution);
                    let guards = h.state.store.db().query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                        "SELECT (SELECT COUNT(*) FROM standard_mutation_leases) AS guards, (SELECT COUNT(*) FROM import_destinations) AS admissions, (SELECT COUNT(*) FROM zip_v2_mpu_completions WHERE upload_id=?) AS completions",
                        vec![id.clone().into()],
                    )).await.unwrap().unwrap();
                    for name in ["guards", "admissions", "completions"] {
                        assert_eq!(guards.try_get::<i64>("", name).unwrap(), 0, "{name}");
                    }
                }
                ipfs_s3_gateway::store::pinning::ledger::register_route(h.state.store.db(), &provider.name, &provider.identity)
                    .await.unwrap();
                let completed = request::complete(&h, &id, &etag, HeaderMap::new()).await;
                assert_eq!(completed.status, StatusCode::OK, "{completed:?}");
                assert_eq!(zip::read(h.state.store.db(), &id).await.unwrap().unwrap().captured_options, original_capture);
                let after = Ledger::read(&h).await;
                let io = h.kubo.received_requests().await.unwrap().len();
                ipfs_s3_gateway::store::pinning::ledger::register_route(h.state.store.db(), &provider.name, &unavailable)
                    .await.unwrap();
                assert_eq!(request::complete(&h, &id, &etag, HeaderMap::new()).await, completed);
                assert_eq!(Ledger::read(&h).await, after);
                assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
                ipfs_s3_gateway::store::pinning::ledger::register_route(h.state.store.db(), &provider.name, &provider.identity)
                    .await.unwrap();
        }).catch_unwind().await;
        (h, result)
    }).await;
    }
}

async fn r25_server_with_frozen_store(
    original: &std::sync::Arc<ipfs_s3_gateway::state::AppState>,
    config: ipfs_s3_gateway::pinning::config::ValidatedPinningConfig,
    zip_config: &ipfs_s3_gateway::config::DecompressZipConfig,
) -> support::cors::S3ServerHandle {
    let pinning =
        ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::build_with_decompress_config(
            config,
            Some(original.kubo.clone()),
            original.pinning.control_mode(),
            zip_config,
        )
        .unwrap();
    let state = std::sync::Arc::new(ipfs_s3_gateway::state::AppState {
        kubo: original.kubo.clone(),
        cold_kubo: original.cold_kubo.clone(),
        store: ipfs_s3_gateway::store::Store::new(original.store.db().clone()),
        credentials: original.credentials.clone(),
        master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning,
    });
    state
        .pinning
        .register_identities(&state.store)
        .await
        .unwrap();
    support::decompress::start_s3_server(state, std::sync::Arc::default()).await
}

#[tokio::test]
async fn r25_native_v2_mpu_empty_rule_capture_ignores_new_allow_and_deny() {
    use support::{
        sigv4::send_sigv4,
        stage9_zip_replay::{BUCKET, KEY},
    };
    for (index, effect) in ["allow", "deny"].into_iter().enumerate() {
        run(Entry::Mpu, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
                let mut zip_config = h.state.pinning.decompress_zip_config().clone();
                zip_config.auto_pin_outputs = Some(true);
                zip_config.pin_output_rules_provided = true;
                zip_config.pin_output_rules.clear();
                let server = r25_server_with_frozen_store(&h.state, h.state.pinning.effective_config().clone(), &zip_config).await;
                let intake = AssertUnwindSafe(async {
                    let mut controls = request::controls(&format!("empty-original-{index}"));
                    controls.insert("x-ipfs3-zip-targets", "extracted".parse().unwrap());
                    let created = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, BUCKET, KEY,
                        &[("uploads", ""), ("decompress-zip", "out/")], Vec::new(), controls, "test").await).await;
                    assert_eq!(created.status, StatusCode::OK, "{created:?}");
                    let id = created.upload_id();
                    let uploaded = request::Receipt::read(send_sigv4(reqwest::Method::PUT, &server.endpoint, BUCKET, KEY,
                        &[("uploadId", &id), ("partNumber", "1")], Outcome::Complete.archive(), HeaderMap::new(), "test").await).await;
                    assert_eq!(uploaded.status, StatusCode::OK, "{uploaded:?}");
                    let etag = uploaded.headers[http::header::ETAG].to_str().unwrap().to_owned();
                    let original = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
                    let saved: serde_json::Value = serde_json::from_str(&original.captured_options).unwrap();
                    let capture = CapturedZipIntent::decode_json(&saved["zip_intent"].to_string()).unwrap();
                    assert!(!capture.controls().publish_source);
                    assert!(capture.controls().targets.extracted, "target bit alone is not authorization");
                    assert!(capture.source_decision().effective_intents.is_empty());
                    (id, etag, original)
                }).catch_unwind().await;
                server.shutdown().await;
                let (id, etag, original) = intake.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                let mut current = h.state.pinning.effective_config().clone();
                current.providers[0].identity.endpoint_revision += 1;
                current.providers[0].identity.credential_revision += 1;
                current.policies[0].default_duration = ipfs_s3_gateway::pinning::config::LeaseDuration::parse("2h").unwrap();
                let rule: ipfs_s3_gateway::pinning::zip_policy::ZipOutputRuleConfig = toml::from_str(
                    &format!("name='new-current-{effect}'\npriority=1\nbucket='test-bkt'\nprefix='out/'\neffect='{effect}'\n{}", if effect == "allow" { "policy_index=0\n" } else { "" }),
                ).unwrap();
                zip_config.pin_output_rules = vec![rule];
                let server = r25_server_with_frozen_store(&h.state, current, &zip_config).await;
                let verified = AssertUnwindSafe(async {
                let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>").into_bytes();
                let completed = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, BUCKET, KEY,
                    &[("uploadId", &id)], xml.clone(), HeaderMap::new(), "test").await).await;
                assert_eq!(completed.status, StatusCode::OK, "{completed:?}");
                let final_execution = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
                assert_eq!(final_execution.captured_options, original.captured_options);
                assert_eq!(zip::read(h.state.store.db(), &id).await.unwrap().unwrap().captured_options, original.captured_options);
                let after = Ledger::read(&h).await;
                let remote = h.state.store.db().query_one(Statement::from_string(DatabaseBackend::Sqlite,
                    "SELECT (SELECT COUNT(*) FROM pin_leases) AS leases, (SELECT COUNT(*) FROM pin_lease_targets) AS targets, (SELECT COUNT(*) FROM remote_pins) AS remotes, (SELECT COUNT(*) FROM pin_jobs) AS jobs, (SELECT COALESCE(SUM(reserved_bytes),0) FROM pin_provider_usage) AS bytes, (SELECT COALESCE(SUM(reserved_pins),0) FROM pin_provider_usage) AS pins".to_owned(),
                )).await.unwrap().unwrap();
                for field in ["leases", "targets", "remotes", "jobs", "bytes", "pins"] {
                    assert_eq!(remote.try_get::<i64>("", field).unwrap(), 0, "{effect}: {field}");
                }
                assert_eq!(send_sigv4(reqwest::Method::GET, &server.endpoint, BUCKET, KEY,
                    &[], Vec::new(), HeaderMap::new(), "test").await.status(), StatusCode::NOT_FOUND);
                let io = h.kubo.received_requests().await.unwrap().len();
                assert_eq!(request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, BUCKET, KEY,
                    &[("uploadId", &id)], xml, HeaderMap::new(), "test").await).await, completed);
                assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
                assert_eq!(Ledger::read(&h).await, after);
                }).catch_unwind().await;
                server.shutdown().await;
                verified.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        }).catch_unwind().await;
        (h, result)
    }).await;
    }
}

#[tokio::test]
async fn v2_mpu_create_persists_the_shared_intent_in_the_same_capture_segment() {
    run(Entry::Mpu, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            let created = request::create(&h, request::minimal(Some("captured-mpu")), "out/").await;
            assert_eq!(created.status, StatusCode::OK, "{created:?}");
            let id = created.upload_id();
            let execution = execution::read(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap();
            let intake =
                ipfs_s3_gateway::store::multipart::v2_zip::read_by_upload(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(intake.captured_options, execution.captured_options);
            let envelope: serde_json::Value =
                serde_json::from_str(&execution.captured_options).unwrap();
            let capture = CapturedZipIntent::decode_json(
                &envelope
                    .get("zip_intent")
                    .expect("v2 Create must persist the intent atomically, not only bare options")
                    .to_string(),
            )
            .unwrap();
            assert_eq!(capture.origin().request_id, id);
            assert_eq!(capture.origin().principal_id, "test");
            assert!(capture.controls().auto_pin_outputs);
            assert!(capture.controls().targets.extracted);
            assert!(!capture.controls().targets_explicit);
            let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                .await
                .unwrap();
            assert_eq!(
                CapturedZipIntent::decode_json(upload.zip_intent_json.as_ref().unwrap()).unwrap(),
                capture
            );
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            h.restart(false).await;
            assert_eq!(
                request::create(&h, request::minimal(Some("captured-mpu")), "out/").await,
                created
            );
            for (name, value) in [
                ("x-ipfs3-zip-targets", "extracted"),
                ("x-amz-meta-note", "changed"),
                ("x-amz-tagging", "note=changed"),
            ] {
                let mut changed = request::minimal(Some("captured-mpu"));
                changed.insert(
                    http::header::HeaderName::from_static(name),
                    value.parse().unwrap(),
                );
                let replay = request::create(&h, changed, "out/").await;
                assert_eq!(replay.status, StatusCode::CONFLICT, "{name}: {replay:?}");
            }
            assert_eq!(
                ipfs_s3_gateway::store::multipart::v2_zip::read_by_upload(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap()
                    .captured_options,
                intake.captured_options
            );
            assert_eq!(
                execution::read(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap(),
                execution
            );
            assert_eq!(
                ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .zip_intent_json,
                upload.zip_intent_json
            );
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(h.adds().await, adds);
            let etag = request::upload(&h, &id, Outcome::Complete.archive()).await;
            let complete = request::complete(&h, &id, &etag, HeaderMap::new()).await;
            assert_eq!(
                complete.status,
                StatusCode::OK,
                "captured v2 Complete: {complete:?}; execution={:?}; batch={:?}; adds={}",
                execution::read(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .map(|s| (s.state, s.input_sha256, s.input_art_cid, s.epoch)),
                zip::snapshot(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .map(|s| (
                        s.batch.state,
                        s.batch.manifest_prepared,
                        s.batch.source_published,
                        s.batch.root_status,
                        s.entries.len(),
                        s.batch.input_identity
                    )),
                h.adds().await
            );
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}

fn r25_p2_archive() -> Vec<u8> {
    let mut archive = support::decompress::legal_single_entry_zip();
    let eocd = archive.len() - 22;
    let central = u32::from_le_bytes(archive[eocd + 16..eocd + 20].try_into().unwrap()) as usize;
    let central_size = u32::from_le_bytes(archive[eocd + 12..eocd + 16].try_into().unwrap());
    assert_eq!(&archive[30..38], b"file.txt");
    assert_eq!(&archive[central + 46..central + 54], b"file.txt");
    archive.splice(30..38, b"a/file.txt".iter().copied());
    archive[26..28].copy_from_slice(&10u16.to_le_bytes());
    let central = central + 2;
    archive[central + 28..central + 30].copy_from_slice(&10u16.to_le_bytes());
    archive.splice(central + 46..central + 54, b"a/file.txt".iter().copied());
    let eocd = archive.len() - 22;
    archive[eocd + 12..eocd + 16].copy_from_slice(&(central_size + 2).to_le_bytes());
    archive[eocd + 16..eocd + 20].copy_from_slice(&(central as u32).to_le_bytes());
    archive
}

async fn r25_p2_interrupted_mpu() -> (
    support::decompress::TestHarness,
    ipfs_s3_gateway::pinning::config::ValidatedPinningConfig,
    ipfs_s3_gateway::config::DecompressZipConfig,
    String,
    Vec<u8>,
) {
    use support::{
        decompress::{AddReply, KuboScript},
        sigv4::send_sigv4,
    };
    let archive = r25_p2_archive();
    let h = support::decompress::start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok("QmOutput"),
            ],
            cat_bodies: [
                ("QmPart".into(), archive.clone()),
                ("QmArchive".into(), archive.clone()),
            ]
            .into(),
        },
        false,
    )
    .await;
    let config: ipfs_s3_gateway::config::Config = toml::from_str(
        r#"
        [decompress_zip]
        auto_pin_outputs=true
        unixfs_directory_root=false
        [[pinning.providers]]
        name='alpha'
        kind='noop'
        priority=1
        max_bytes=1000000
        max_pins=100
        [[pinning.providers]]
        name='beta'
        kind='noop'
        priority=2
        max_bytes=1000000
        max_pins=100
        [[pinning.policies]]
        bucket='test-bkt'
        prefix='out/a/'
        trigger='always'
        provider_mode='one'
        providers=['alpha']
        default_duration='1h'
        max_duration='30d'
        allow_decompressed=true
        [[pinning.policies]]
        bucket='test-bkt'
        prefix='out/b/'
        trigger='always'
        provider_mode='one'
        providers=['beta']
        default_duration='1h'
        max_duration='30d'
        allow_decompressed=true
    "#,
    )
    .unwrap();
    let frozen =
        ipfs_s3_gateway::pinning::config::ValidatedPinningConfig::from_config(&config, |_| None)
            .unwrap();
    let server =
        r25_server_with_frozen_store(&h.state, frozen.clone(), &config.decompress_zip).await;
    let prepared = AssertUnwindSafe(async {
        let mut controls = request::controls("r25-p2-admitted");
        controls.insert("x-ipfs3-zip-targets", "extracted".parse().unwrap());
        let created = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, &h.bucket, "archive.zip",
            &[("uploads", ""), ("decompress-zip", "out/")], Vec::new(), controls, "test").await).await;
        assert_eq!(created.status, StatusCode::OK, "{created:?}");
        let id = created.upload_id();
        let uploaded = send_sigv4(reqwest::Method::PUT, &server.endpoint, &h.bucket, "archive.zip",
            &[("uploadId", &id), ("partNumber", "1")], archive.clone(), HeaderMap::new(), "test").await;
        assert_eq!(uploaded.status(), StatusCode::OK);
        let etag = uploaded.headers()[http::header::ETAG].to_str().unwrap();
        let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>").into_bytes();
        h.state.store.db().execute_unprepared("CREATE TRIGGER r25_p2_interrupt BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT,'native publication interrupted'); END").await.unwrap();
        let interrupted = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, &h.bucket, "archive.zip",
            &[("uploadId", &id)], xml.clone(), HeaderMap::new(), "test").await).await;
        assert_eq!(interrupted.status, StatusCode::INTERNAL_SERVER_ERROR, "{interrupted:?}");
        h.state.store.db().execute_unprepared("DROP TRIGGER r25_p2_interrupt").await.unwrap();
        let original = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
        assert_eq!(original.state, "admitted");
        assert_eq!(original.input_sha256, Some(hex::encode(Sha256::digest(&archive))));
        assert_eq!(original.input_art_size, Some(archive.len() as i64));
        let retained = zip::snapshot(h.state.store.db(), &id).await.unwrap().unwrap();
        assert!(retained.batch.manifest_prepared);
        assert_eq!(retained.batch.state, "open");
        assert!(!retained.batch.source_published);
        assert_eq!(retained.entries.len(), 1);
        assert_eq!(retained.entries[0].object_key.as_deref(), Some("out/a/file.txt"));
        assert_eq!(retained.entries[0].cid.as_deref(), Some("QmOutput"));
        let guards = h.state.store.db().query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM zip_v2_targets WHERE batch_id=? AND object_key='out/a/file.txt'", vec![id.clone().into()],
        )).await.unwrap().unwrap();
        assert_eq!(guards.try_get::<i64>("", "n").unwrap(), 1);
        assert_eq!(h.captured_add_file_bytes().len(), 3);
        h.state.store.db().execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE zip_v2_executions SET lease_until=datetime('now','-1 minute') WHERE id=? AND state='admitted'", vec![id.clone().into()],
        )).await.unwrap();
        (id, xml)
    }).catch_unwind().await;
    server.shutdown().await;
    let (id, xml) = prepared.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    (h, frozen, config.decompress_zip, id, xml)
}

#[tokio::test]
async fn r25_p2_admitted_mpu_unmatched_provider_drift() {
    use support::sigv4::send_sigv4;
    let (h, mut current, zip_config, id, xml) = r25_p2_interrupted_mpu().await;
    let original = execution::read(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let capture = zip::read(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap()
        .captured_options;
    let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
        .await
        .unwrap();
    let parts = ipfs_s3_gateway::store::multipart::list_parts(h.state.store.db(), &id)
        .await
        .unwrap();
    current.providers[1].identity.endpoint_revision += 1;
    current.providers[1].identity.credential_revision += 1;
    let io = h.kubo.received_requests().await.unwrap().len();
    let server = r25_server_with_frozen_store(&h.state, current, &zip_config).await;
    let checked = AssertUnwindSafe(async {
        let completed = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, &h.bucket, "archive.zip",
            &[("uploadId", &id)], xml.clone(), HeaderMap::new(), "test").await).await;
        assert_eq!(completed.status, StatusCode::OK, "{completed:?}");
        assert_eq!(h.kubo.received_requests().await.unwrap().len(), io, "admitted retry must not CAT/add/pin/build again");
        let row = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.captured_options, original.captured_options);
        assert_eq!(row.request_contract, original.request_contract);
        assert_eq!(row.input_sha256, original.input_sha256);
        assert_eq!(row.input_art_cid, original.input_art_cid);
        assert_eq!(zip::read(h.state.store.db(), &id).await.unwrap().unwrap().captured_options, capture);
        assert_eq!(ipfs_s3_gateway::store::object::get_latest(h.state.store.db(), &h.bucket, "out/a/file.txt").await.unwrap().cid, "QmOutput");
        assert!(ipfs_s3_gateway::store::object::get_latest(h.state.store.db(), &h.bucket, "archive.zip").await.is_err());
        let effect = h.state.store.db().query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT (SELECT COUNT(*) FROM pin_leases WHERE source='automatic') AS leases, (SELECT COUNT(*) FROM remote_pins WHERE provider='beta') AS beta, (SELECT COUNT(*) FROM pin_jobs WHERE provider='beta') AS jobs, (SELECT COUNT(*) FROM objects) AS objects".to_owned(),
        )).await.unwrap().unwrap();
        assert_eq!(effect.try_get::<i64>("", "leases").unwrap(), 1);
        for field in ["beta", "jobs"] { assert_eq!(effect.try_get::<i64>("", field).unwrap(), 0); }
        assert_eq!(effect.try_get::<i64>("", "objects").unwrap(), 1);
        assert_eq!(request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, &h.bucket, "archive.zip",
            &[("uploadId", &id)], xml, HeaderMap::new(), "test").await).await, completed);
        assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
        // Original upload/part snapshots were read from native first intake, never replaced to authorize retry.
        assert_eq!(CapturedZipIntent::decode_json(upload.zip_intent_json.as_deref().unwrap()).unwrap().origin().request_id, id);
        assert_eq!(parts.len(), 1);
    }).catch_unwind().await;
    server.shutdown().await;
    checked.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

#[tokio::test]
async fn r25_p2_admitted_mpu_exact_provider_drift_preserves_capture_and_blocks_builder() {
    use support::sigv4::send_sigv4;
    for drift in ["endpoint", "registered", "disabled", "retired"] {
        let (h, mut current, zip_config, id, xml) = r25_p2_interrupted_mpu().await;
        let original = execution::read(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        let batch = zip::snapshot(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
            .await
            .unwrap();
        let parts = ipfs_s3_gateway::store::multipart::list_parts(h.state.store.db(), &id)
            .await
            .unwrap();
        match drift {
            "endpoint" => current.providers[0].identity.endpoint_revision += 1,
            "disabled" => current.providers[0].limits.enabled = false,
            _ => {}
        }
        let server = r25_server_with_frozen_store(&h.state, current.clone(), &zip_config).await;
        let checked = AssertUnwindSafe(async {
            if drift == "registered" {
                let mut route = current.providers[0].route_snapshot();
                route.credential_revision += 1;
                h.state.store.db().execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "UPDATE pin_provider_routes SET snapshot=? WHERE provider='alpha'",
                    vec![serde_json::to_string(&route).unwrap().into()],
                )).await.unwrap();
            } else if drift == "retired" {
                h.state.store.db().execute_unprepared("UPDATE pin_provider_routes SET retired=TRUE WHERE provider='alpha'").await.unwrap();
            }
            let io = h.kubo.received_requests().await.unwrap().len();
            let response = request::Receipt::read(send_sigv4(reqwest::Method::POST, &server.endpoint, &h.bucket, "archive.zip",
                &[("uploadId", &id)], xml.clone(), HeaderMap::new(), "test").await).await;
            assert_eq!(response.status, StatusCode::BAD_REQUEST, "{drift}: {response:?}");
            assert!(response.body.contains("<Code>InvalidArgument</Code>"), "{drift}: {response:?}");
            assert_eq!(h.kubo.received_requests().await.unwrap().len(), io, "{drift}: no builder/CAT/add/pin");
            let row = execution::read(h.state.store.db(), &id).await.unwrap().unwrap();
            assert_eq!(row.state, "admitted");
            // Claim/group scheduling metadata may advance, never capture/input/parts or publication.
            assert!(row.epoch > original.epoch);
            assert_eq!(row.request_contract, original.request_contract);
            assert_eq!(row.request_fingerprint, original.request_fingerprint);
            assert_eq!(row.captured_options, original.captured_options);
            assert_eq!(row.input_sha256, original.input_sha256);
            assert_eq!(row.input_art_cid, original.input_art_cid);
            assert_eq!(row.input_art_size, original.input_art_size);
            assert_eq!(ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id).await.unwrap(), upload);
            assert_eq!(ipfs_s3_gateway::store::multipart::list_parts(h.state.store.db(), &id).await.unwrap(), parts);
            let retained = zip::snapshot(h.state.store.db(), &id).await.unwrap().unwrap();
            assert_eq!(retained.entries, batch.entries);
            assert_eq!(retained.batch.captured_options, batch.batch.captured_options);
            assert_eq!(retained.batch.state, "open");
            let counts = h.state.store.db().query_one(Statement::from_string(DatabaseBackend::Sqlite,
                "SELECT (SELECT COUNT(*) FROM objects) AS objects,(SELECT COUNT(*) FROM object_versions) AS versions,(SELECT COUNT(*) FROM pin_leases) AS leases,(SELECT COUNT(*) FROM pin_lease_targets) AS targets,(SELECT COUNT(*) FROM remote_pins) AS remotes,(SELECT COUNT(*) FROM pin_jobs) AS jobs".to_owned(),
            )).await.unwrap().unwrap();
            for name in ["objects", "versions", "leases", "targets", "remotes", "jobs"] { assert_eq!(counts.try_get::<i64>("", name).unwrap(), 0, "{drift}/{name}"); }
        }).catch_unwind().await;
        server.shutdown().await;
        checked.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    }
}

#[tokio::test]
async fn historical_mpu_receipt_rejects_inconsistent_captured_token_before_replay() {
    run(Entry::Mpu, Outcome::Complete, |mut h| async move {
        let result = AssertUnwindSafe(async {
            use ipfs_s3_gateway::store::multipart::v2_zip;

            let created = request::create(&h, request::controls("historical-mpu-token"), "out/").await;
            assert_eq!(created.status, StatusCode::OK, "{created:?}");
            let id = created.upload_id();
            let archive = Outcome::Complete.archive();
            let etag = request::upload(&h, &id, archive.clone()).await;
            let intake = v2_zip::read_by_upload(h.state.store.db(), &id).await.unwrap().unwrap();
            let options = ipfs_s3_gateway::zip::options::decode_historical_zip_options(&intake.captured_options, None).unwrap().options;
            let original_options = serde_json::to_string(&options).unwrap();
            let parts = [s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some(s3s::dto::ETag::Strong(etag.trim_matches('"').to_owned())),
                ..Default::default()
            }];
            assert!(matches!(v2_zip::capture_complete(h.state.store.db(), &intake, &intake.owner, &intake.bucket, &intake.archive_key, &parts).await.unwrap(), v2_zip::CompleteState::Pending));
            let xml = "<CompleteMultipartUploadResult><ETag>historical-mpu</ETag></CompleteMultipartUploadResult>";
            let headers = serde_json::json!({"content-type": "application/xml"}).to_string();
            // Original historical terminal evidence only; this fixture does not
            // invoke or grant active publication without a captured intent.
            let tx = h.state.store.db().begin().await.unwrap();
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "UPDATE zip_v2_mpu_completions SET response_xml=?,response_headers=? WHERE upload_id=?",
                vec![xml.into(), headers.into(), id.clone().into()],
            )).await.unwrap();
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "UPDATE zip_v2_mpu_intakes SET active_upload_id=NULL,captured_options=? WHERE original_upload_id=?",
                vec![original_options.clone().into(), id.clone().into()],
            )).await.unwrap();
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "UPDATE zip_v2_executions SET state='completed',captured_options=?,input_sha256=?,input_art_cid=?,input_art_size=?,terminal_result=? WHERE id=?",
                vec![original_options.clone().into(), hex::encode(Sha256::digest(&archive)).into(), etag.trim_matches('"').to_owned().into(), (archive.len() as i64).into(), "{}".into(), id.clone().into()],
            )).await.unwrap();
            tx.commit().await.unwrap();
            h.restart(false).await;
            let receipt = request::complete(&h, &id, &etag, HeaderMap::new()).await;
            assert_eq!(receipt.status, StatusCode::OK, "valid historical receipt: {receipt:?}");
            assert_eq!(receipt.body, xml);
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            let mut changed = options;
            changed.token = "different-captured-token".into();
            for (captured, expected) in [(serde_json::to_string(&changed).unwrap(), StatusCode::CONFLICT), (original_options, StatusCode::OK)] {
                let tx = h.state.store.db().begin().await.unwrap();
                tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "UPDATE zip_v2_mpu_intakes SET captured_options=? WHERE original_upload_id=?",
                    vec![captured.clone().into(), id.clone().into()],
                )).await.unwrap();
                tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "UPDATE zip_v2_executions SET captured_options=? WHERE id=?",
                    vec![captured.into(), id.clone().into()],
                )).await.unwrap();
                tx.commit().await.unwrap();
                let replay = request::complete(&h, &id, &etag, HeaderMap::new()).await;
                assert_eq!(replay.status, expected, "terminal receipt must validate the captured token before return: {replay:?}");
                if expected == StatusCode::OK {
                    assert_eq!(replay, receipt);
                }
                assert_eq!(Ledger::read(&h).await, ledger);
                assert_eq!(h.adds().await, adds);
            }
        }).catch_unwind().await;
        (h, result)
    }).await;
}
