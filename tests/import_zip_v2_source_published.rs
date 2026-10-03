//! Real SigV4 intake plus the shared import worker, mock HTTPS source and Kubo.
#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{
    entities::{object, object_version, standard_mutation_lease},
    zip::{self, execution, import_intake},
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait, Statement};
use sha2::{Digest, Sha256};
use support::{
    decompress::{duplicate_entry_zip, legal_single_entry_zip},
    import::{
        ImportHarness, ImportHarnessConfig, ImportPublicationBlockControl, TestHttpsReply,
        get_import_status, start_import_harness,
    },
    sigv4::send_sigv4,
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn controls(token: &str, extracted: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("content-type", "application/xml"),
        ("x-ipfs3-client-token", token),
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "true"),
        (
            "x-ipfs3-zip-publish-extracted",
            if extracted { "true" } else { "false" },
        ),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
        ("x-ipfs3-object-content-type", "application/zip"),
        ("x-amz-meta-label", "signed-source"),
        (
            "x-amz-tagging",
            if extracted {
                "label=source&ipfs-s3%3Apin=true&ipfs-s3%3Azip-root=false"
            } else {
                // Leave root at its enabled default: source-only must override it.
                "label=source&ipfs-s3%3Apin=true"
            },
        ),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

fn cid_xml() -> String {
    format!("<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>")
}

async fn submit(
    h: &ImportHarness,
    token: &str,
    extracted: bool,
    xml: &str,
    sha: Option<&str>,
) -> String {
    let mut headers = controls(token, extracted);
    if let Some(sha) = sha {
        headers.insert(
            "x-ipfs3-zip-expected-sha256",
            HeaderValue::from_str(sha).unwrap(),
        );
    }
    let response = send_sigv4(
        reqwest::Method::POST,
        &h.endpoint,
        &h.bucket,
        "archive.zip",
        &[("ipfs3-import", ""), ("decompress-zip", "out/")],
        xml.as_bytes().to_vec(),
        headers,
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert!(!response.headers().contains_key("etag"));
    assert!(!response.headers().contains_key("x-amz-version-id"));
    let id = response.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let accepted = response.text().await.unwrap();
    assert!(!accepted.contains("<ETag>"));
    assert!(!accepted.contains("<VersionId>"));
    id
}

async fn wait(h: &ImportHarness, id: &str) -> &'static str {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = import_intake::read_for_path(
                h.state.store.db(),
                id,
                "test",
                &h.bucket,
                "archive.zip",
            )
            .await
            .unwrap()
            .unwrap();
            if status.state != "pending" {
                break status.state;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("import reached terminal state")
}

async fn sealed_receipt(h: &ImportHarness, id: &str) -> Option<String> {
    h.state
        .store
        .db()
        .query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT receipt_metadata FROM zip_v2_import_requests WHERE batch_id=?",
            [id.to_owned().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "receipt_metadata")
        .unwrap()
}

#[tokio::test]
async fn source_only_skips_extraction_and_root_and_reports_only_a_real_version() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    // A duplicate ZIP would be rejected by extraction. Source-only must not parse it.
    let archive = duplicate_entry_zip();
    h.set_cat_body(CID, archive.clone());
    let id = submit(&h, "source-only", false, &cid_xml(), None).await;
    assert_eq!(wait(&h, &id).await, "ready");
    let db = h.state.store.db();
    let source = object::Entity::find().one(db).await.unwrap().unwrap();
    assert_eq!(source.key, "archive.zip");
    assert_eq!(source.cid, CID);
    assert_eq!(source.size, archive.len() as i64);
    assert_eq!(source.content_type.as_deref(), Some("application/zip"));
    assert_eq!(source.metadata.unwrap()["label"], "signed-source");
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 1);
    let batch = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert!(batch.batch.source_published);
    assert_eq!(batch.batch.root_status, "disabled");
    assert!(batch.batch.root_cid.is_none());
    assert!(batch.entries.is_empty());
    assert_eq!(h.kubo_call_count("/api/v0/add").await, 0);
    assert_eq!(h.kubo_call_count("/api/v0/cat").await, 2);
    let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(status.into_body()).unwrap();
    assert!(status.contains(&format!("<ETag>{CID}</ETag>")));
    assert!(status.contains(&format!(
        "<MeasuredSHA256>{}</MeasuredSHA256>",
        hex::encode(Sha256::digest(&archive))
    )));
    assert!(!status.contains("<RootCID>"));
    let io = h.kubo_total_call_count().await;
    assert_eq!(submit(&h, "source-only", false, &cid_xml(), None).await, id);
    assert_eq!(h.kubo_total_call_count().await, io);
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 1);
    h.shutdown().await;
}

#[tokio::test]
async fn source_only_admission_attests_bytes_and_freezes_a_real_guard_without_extraction_io() {
    let gate = Arc::new(ImportPublicationBlockControl::new());
    let h = start_import_harness(ImportHarnessConfig {
        execution_observer: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let archive = duplicate_entry_zip();
    h.set_cat_body(CID, archive.clone());
    let id = submit(&h, "source-only-admission", false, &cid_xml(), None).await;
    gate.wait_until_blocked(&id).await;
    let db = h.state.store.db();
    let snapshot = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(snapshot.state, "admitted");
    assert_eq!(snapshot.input_art_cid.as_deref(), Some(CID));
    assert_eq!(snapshot.input_art_size, Some(archive.len() as i64));
    assert_eq!(
        snapshot.input_sha256.as_deref(),
        Some(hex::encode(Sha256::digest(&archive)).as_str())
    );
    let mirror = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert!(mirror.entries.is_empty());
    assert!(mirror.builds.is_empty());
    assert!(mirror.batch.root_cid.is_none());
    assert!(!mirror.batch.source_published);
    let guard = import_intake::source_guard(&snapshot, &mirror.batch)
        .unwrap()
        .unwrap();
    assert_eq!(guard.key, "archive.zip");
    assert_eq!(guard.mutation_id, format!("zip-v2-source:{id}"));
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(db)
            .await
            .unwrap(),
        1
    );
    assert_eq!(h.kubo_call_count("/api/v0/add").await, 0);
    assert_eq!(h.kubo_call_count("/api/v0/cat").await, 2);
    // 202/replay is still admission only, even after bytes have been attested.
    let io = h.kubo_total_call_count().await;
    assert_eq!(
        submit(&h, "source-only-admission", false, &cid_xml(), None).await,
        id
    );
    assert_eq!(h.kubo_total_call_count().await, io);
    let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
    assert!(
        !String::from_utf8(status.into_body())
            .unwrap()
            .contains("<ETag>")
    );
    // Abort without entering the shared publisher so this test independently
    // proves our admission seam even while its owner integrates import source.
    db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until=datetime('now','-1 second') WHERE key='archive.zip'").await.unwrap();
    assert_eq!(wait(&h, &id).await, "failed");
    gate.release();
    h.shutdown().await;
}

#[tokio::test]
async fn source_and_extracted_url_replay_uses_first_full_snapshot_and_zero_io() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    let archive = legal_single_entry_zip();
    let sha = hex::encode(Sha256::digest(&archive));
    h.set_cat_body(CID, archive.clone());
    h.source.set_reply(
        "/archive.zip?secret=private",
        TestHttpsReply::chunked(archive),
    );
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        h.source.url("/archive.zip?secret=private")
    );
    let id = submit(&h, "source-both", true, &xml, Some(&sha)).await;
    assert_eq!(wait(&h, &id).await, "ready");
    assert_eq!(
        object::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        2
    );
    let before = h.kubo_total_call_count().await;
    h.source.set_reply(
        "/archive.zip?secret=private",
        TestHttpsReply::chunked(b"changed".to_vec()),
    );
    assert_eq!(submit(&h, "source-both", true, &xml, Some(&sha)).await, id);
    let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(status.into_body()).unwrap();
    assert!(status.contains("<State>ready</State>"));
    assert!(!status.contains("private"));
    assert_eq!(h.source.requests().len(), 1);
    assert_eq!(h.kubo_total_call_count().await, before);
    assert_eq!(
        object_version::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        2
    );
    h.shutdown().await;
}

#[tokio::test]
async fn source_only_url_attests_the_signed_snapshot_without_extraction_and_replays_without_io() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    // Duplicate paths deliberately distinguish publication of the intact source
    // from an accidental extraction attempt, which must reject this archive.
    let archive = duplicate_entry_zip();
    let sha = hex::encode(Sha256::digest(&archive));
    h.source
        .set_reply("/source-only.zip", TestHttpsReply::chunked(archive.clone()));
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        h.source.url("/source-only.zip")
    );
    let id = submit(&h, "source-only-url-snapshot", false, &xml, Some(&sha)).await;
    assert_eq!(wait(&h, &id).await, "ready");
    let db = h.state.store.db();
    let snapshot = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(snapshot.input_sha256.as_deref(), Some(sha.as_str()));
    assert_eq!(snapshot.input_art_size, Some(archive.len() as i64));
    let source = object::Entity::find().one(db).await.unwrap().unwrap();
    assert_eq!(source.key, "archive.zip");
    assert_eq!(source.cid, CID);
    assert_eq!(source.size, archive.len() as i64);
    assert_eq!(source.content_type.as_deref(), Some("application/zip"));
    assert_eq!(source.metadata.unwrap()["label"], "signed-source");
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 1);
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(db)
            .await
            .unwrap(),
        0
    );
    let batch = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert!(batch.batch.source_published);
    assert!(batch.entries.is_empty());
    assert!(batch.builds.is_empty());
    assert!(batch.references.is_empty());
    assert_eq!(batch.batch.root_status, "disabled");
    assert!(batch.batch.root_cid.is_none());
    assert_eq!(h.kubo_call_count("/api/v0/add").await, 1);
    assert_eq!(h.kubo_call_count("/api/v0/cat").await, 0);
    let receipt = sealed_receipt(&h, &id).await;
    let io = h.kubo.received_requests().await.unwrap().len();
    h.source.set_reply(
        "/source-only.zip",
        TestHttpsReply::chunked(b"changed source".to_vec()),
    );
    assert_eq!(
        submit(&h, "source-only-url-snapshot", false, &xml, Some(&sha)).await,
        id
    );
    let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(status.into_body()).unwrap();
    assert!(status.contains(&format!("<ExpectedSHA256>{sha}</ExpectedSHA256>")));
    assert!(status.contains(&format!("<MeasuredSHA256>{sha}</MeasuredSHA256>")));
    assert!(status.contains(&format!("<ETag>{CID}</ETag>")));
    assert!(!status.contains("<RootCID>"));
    assert_eq!(h.source.requests().len(), 1);
    assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
    assert_eq!(sealed_receipt(&h, &id).await, receipt);
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 1);
    h.shutdown().await;
}

#[tokio::test]
async fn source_only_url_sha_mismatch_publishes_nothing_and_replays_without_io() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    h.source.set_reply(
        "/bad.zip",
        TestHttpsReply::chunked(legal_single_entry_zip()),
    );
    let xml = format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        h.source.url("/bad.zip")
    );
    let sha = "a".repeat(64);
    let id = submit(&h, "source-mismatch", false, &xml, Some(&sha)).await;
    assert_eq!(wait(&h, &id).await, "failed");
    let io = h.kubo_total_call_count().await;
    assert_eq!(
        submit(&h, "source-mismatch", false, &xml, Some(&sha)).await,
        id
    );
    assert_eq!(h.kubo_total_call_count().await, io);
    assert_eq!(h.source.requests().len(), 1);
    assert_eq!(
        object::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        0
    );
    h.shutdown().await;
}

#[tokio::test]
async fn source_and_output_manifest_admission_rolls_back_as_one_transaction() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    h.set_cat_body(CID, legal_single_entry_zip());
    let db = h.state.store.db();
    db.execute_unprepared("CREATE TRIGGER reject_source_marker BEFORE UPDATE OF input_identity ON zip_batches WHEN NEW.input_identity LIKE 'zip-v2-source-gen:%' BEGIN SELECT RAISE(ABORT, 'source_marker_failure'); END").await.unwrap();
    let id = submit(&h, "source-admission-rollback", true, &cid_xml(), None).await;
    assert_eq!(wait(&h, &id).await, "failed");
    assert!(zip::snapshot(db, &id).await.unwrap().is_none());
    let manifest = db
        .query_one(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM zip_v2_manifest".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.try_get::<i64>("", "n").unwrap(), 0);
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    let io = h.kubo_total_call_count().await;
    assert_eq!(
        submit(&h, "source-admission-rollback", true, &cid_xml(), None).await,
        id
    );
    assert_eq!(h.kubo_total_call_count().await, io);
    h.shutdown().await;
}

#[tokio::test]
async fn admitted_source_heartbeat_renews_the_exact_source_guard_with_execution_and_outputs() {
    let gate = Arc::new(ImportPublicationBlockControl::new());
    let h = start_import_harness(ImportHarnessConfig {
        execution_observer: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    h.set_cat_body(CID, legal_single_entry_zip());
    let id = submit(&h, "source-heartbeat", true, &cid_xml(), None).await;
    gate.wait_until_blocked(&id).await;
    let db = h.state.store.db();
    db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until=datetime('now','+2 seconds') WHERE key='archive.zip'").await.unwrap();
    // Observe the real worker heartbeat, not an explicit test-side renew call.
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let row = db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS n FROM standard_mutation_leases WHERE key='archive.zip' AND julianday(lease_until)>julianday('now','+60 seconds')".to_owned()))
                .await.unwrap().unwrap();
            if row.try_get::<i64>("", "n").unwrap() == 1 { break; }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    }).await.unwrap();
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(db)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        execution::read(db, &id).await.unwrap().unwrap().state,
        "admitted"
    );
    // A now-expired source guard must not be revived by a future heartbeat.
    db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until=datetime('now','-1 second') WHERE key='archive.zip'").await.unwrap();
    assert_eq!(wait(&h, &id).await, "failed");
    let row = db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Sqlite,
        "SELECT COUNT(*) AS n FROM standard_mutation_leases WHERE key='archive.zip' AND julianday(lease_until)>julianday('now')".to_owned()))
        .await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    gate.release();
    h.shutdown().await;
}

#[tokio::test]
async fn source_receipt_replays_original_enabled_version_after_a_later_signed_overwrite() {
    let h = start_import_harness(ImportHarnessConfig::default()).await;
    ipfs_s3_gateway::store::bucket::set_versioning_state(
        h.state.store.db(),
        &h.bucket,
        ipfs_s3_gateway::store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    h.set_cat_body(CID, duplicate_entry_zip());
    let id = submit(&h, "source-version", false, &cid_xml(), None).await;
    assert_eq!(wait(&h, &id).await, "ready");
    let status =
        import_intake::read_for_path(h.state.store.db(), &id, "test", &h.bucket, "archive.zip")
            .await
            .unwrap()
            .unwrap();
    let published = status.published_source.unwrap();
    let version = published.version_id.as_deref().unwrap();
    let replacement = send_sigv4(
        reqwest::Method::PUT,
        &h.endpoint,
        &h.bucket,
        "archive.zip",
        &[],
        b"a later object".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::OK);
    assert_ne!(
        replacement.headers()["x-amz-version-id"].to_str().unwrap(),
        version
    );
    let io = h.kubo_total_call_count().await;
    assert_eq!(
        submit(&h, "source-version", false, &cid_xml(), None).await,
        id
    );
    let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(status.into_body()).unwrap();
    assert!(status.contains(&format!("<VersionId>{version}</VersionId>")));
    assert!(status.contains(&format!("<Size>{}</Size>", published.size)));
    assert_eq!(h.kubo_total_call_count().await, io);
    assert_eq!(
        object_version::Entity::find()
            .count(h.state.store.db())
            .await
            .unwrap(),
        2
    );
    h.shutdown().await;
}

#[tokio::test]
async fn lost_source_guard_never_readmits_or_publishes_either_output() {
    let gate = Arc::new(ImportPublicationBlockControl::new());
    let h = start_import_harness(ImportHarnessConfig {
        execution_observer: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    h.set_cat_body(CID, legal_single_entry_zip());
    let id = submit(&h, "source-stolen", true, &cid_xml(), None).await;
    gate.wait_until_blocked(&id).await;
    let db = h.state.store.db();
    let snapshot = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(snapshot.state, "admitted");
    db.execute(Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,
        "UPDATE import_destinations SET mutation_id='successor' WHERE bucket=? AND key='archive.zip'",
        [h.bucket.clone().into()])).await.unwrap();
    let io = h.kubo_total_call_count().await;
    gate.release();
    assert_eq!(wait(&h, &id).await, "failed");
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(h.kubo_total_call_count().await, io);
    let destination = db
        .query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT mutation_id FROM import_destinations WHERE bucket=? AND key='archive.zip'",
            [h.bucket.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        destination.try_get::<String>("", "mutation_id").unwrap(),
        "successor"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn late_receipt_failure_rolls_back_source_versions_and_guards_then_recovers_without_io() {
    let gate = Arc::new(ImportPublicationBlockControl::new());
    let h = start_import_harness(ImportHarnessConfig {
        execution_observer: Some(gate.clone()),
        max_attempts: 3,
        ..Default::default()
    })
    .await;
    h.set_cat_body(CID, legal_single_entry_zip());
    let id = submit(&h, "source-late-rollback", true, &cid_xml(), None).await;
    gate.wait_until_blocked(&id).await;
    let db = h.state.store.db();
    db.execute_unprepared("CREATE TRIGGER block_source_import_ready BEFORE UPDATE OF job_state ON zip_v2_import_requests WHEN NEW.job_state='ready' BEGIN SELECT RAISE(ABORT, 'late_receipt_failure'); END").await.unwrap();
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = execution::read(db, &id).await.unwrap().unwrap();
            if snapshot.epoch > 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 0);
    assert!(sealed_receipt(&h, &id).await.is_none());
    assert!(
        execution::read(db, &id)
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .is_none()
    );
    assert!(sealed_receipt(&h, &id).await.is_none());
    assert!(
        execution::read(db, &id)
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .is_none()
    );
    assert_eq!(
        standard_mutation_lease::Entity::find()
            .count(db)
            .await
            .unwrap(),
        2
    );
    let mirror = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(mirror.batch.state, "open");
    assert!(
        mirror
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );
    let io = h.kubo_total_call_count().await;
    db.execute_unprepared("DROP TRIGGER block_source_import_ready")
        .await
        .unwrap();
    assert_eq!(wait(&h, &id).await, "ready");
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 2);
    assert_eq!(h.kubo_total_call_count().await, io);
    h.shutdown().await;
}

#[tokio::test]
async fn each_target_uses_its_own_automatic_rule_intersection_without_source_manual_inheritance() {
    use ipfs_s3_gateway::{
        config::Config,
        import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
        pinning::{
            config::ValidatedPinningConfig,
            coordinator::normalize_validated_config,
            zip_policy::{ZipOutputRuleConfig, ZipRuleEffect},
        },
        store::{self, entities::pin_lease},
    };
    use sea_orm::{ColumnTrait, QueryFilter};
    use support::decompress::{
        AddReply, KuboScript, legal_two_entry_zip, start_kubo_harness, start_s3_server_with_imports,
    };
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };

    // Valid scoped rules deny source independently of each extracted key. Its
    // signed manual pin tag must not fill a denied automatic-rule intersection.
    for (target, source_rule_matches, expected_key) in [
        ("source", true, Some("archive.zip")),
        ("both", false, Some("out/first.txt")),
        ("extracted", false, Some("out/first.txt")),
        ("none", false, None),
    ] {
        let kubo = start_kubo_harness(KuboScript {
            add_replies: vec![AddReply::Ok(CID), AddReply::Ok(CID)],
            cat_bodies: std::collections::HashMap::from([(CID.into(), legal_two_entry_zip())]),
        })
        .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Progress\":1,\"Bytes\":15}}\n{{\"Pins\":[\"{CID}\"]}}\n"
            )))
            .with_priority(1)
            .mount(&kubo.server)
            .await;
        let raw = format!(
            r#"
            [kubo]
            rpc_url = {:?}
            [storage]
            database_url = "sqlite::memory:"
            [decompress_zip]
            unixfs_directory_root = false
            [[pinning.providers]]
            name = "alpha"
            kind = "noop"
            priority = 1
            max_bytes = 1000000
            max_pins = 100
            [[pinning.policies]]
            bucket = "test-bkt"
            prefix = "archive.zip"
            trigger = "always"
            provider_mode = "one"
            providers = ["alpha"]
            default_duration = "1h"
            max_duration = "2h"
            allow_decompressed = true
            [[pinning.policies]]
            bucket = "test-bkt"
            prefix = "out/"
            trigger = "always"
            provider_mode = "one"
            providers = ["alpha"]
            default_duration = "1h"
            max_duration = "2h"
            allow_decompressed = true
        "#,
            kubo.server.uri()
        );
        let mut cfg: Config = toml::from_str(&raw).unwrap();
        let normalized = normalize_validated_config(
            ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap(),
        )
        .unwrap();
        cfg.decompress_zip.pin_output_rules = vec![
            ZipOutputRuleConfig {
                name: "source-intersection".into(),
                priority: 10,
                bucket: "test-bkt".into(),
                prefix: "archive.zip".into(),
                effect: if source_rule_matches {
                    ZipRuleEffect::Allow
                } else {
                    ZipRuleEffect::Deny
                },
                policy_id: source_rule_matches.then(|| normalized.policies[0].identity.clone()),
            },
            ZipOutputRuleConfig {
                name: "first-only".into(),
                priority: 20,
                bucket: "test-bkt".into(),
                prefix: "out/first.txt".into(),
                effect: ZipRuleEffect::Allow,
                policy_id: Some(normalized.policies[1].identity.clone()),
            },
            ZipOutputRuleConfig {
                name: "second-denied".into(),
                priority: 30,
                bucket: "test-bkt".into(),
                prefix: "out/second.txt".into(),
                effect: ZipRuleEffect::Deny,
                policy_id: None,
            },
        ];
        let state = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
        store::bucket::create(state.store.db(), "test-bkt", None)
            .await
            .unwrap();
        let validated = ImportConfig {
            worker_concurrency: 2,
            poll_interval_ms: 10,
            lease_duration_secs: 2,
            max_attempts: 1,
            ..ImportConfig::default()
        }
        .validate()
        .unwrap();
        let imports = ImportCoordinator::new(
            validated.clone(),
            SourceDownloader::production(Arc::new(validated)),
        );
        let server = start_s3_server_with_imports(
            state.clone(),
            Arc::new(tokio::sync::Mutex::new(Vec::new())),
            imports.clone(),
        )
        .await;
        let worker = imports.start(state.clone(), tokio_util::sync::CancellationToken::new());
        let mut headers = controls("source-target-policy", true);
        headers.insert(
            "x-ipfs3-zip-targets",
            HeaderValue::from_str(target).unwrap(),
        );
        let accepted = send_sigv4(
            reqwest::Method::POST,
            &server.endpoint,
            "test-bkt",
            "archive.zip",
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            cid_xml().into_bytes(),
            headers,
            "test",
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED, "target={target}");
        let id = accepted.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let terminal = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = import_intake::read_for_path(
                    state.store.db(),
                    &id,
                    "test",
                    "test-bkt",
                    "archive.zip",
                )
                .await
                .unwrap()
                .unwrap();
                if status.state != "pending" {
                    break status.state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(terminal, "ready", "target={target}");
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            3
        );
        let leases = pin_lease::Entity::find()
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(
            leases.len(),
            usize::from(expected_key.is_some()),
            "target={target}"
        );
        if let Some(key) = expected_key {
            let expected = object::Entity::find()
                .filter(object::Column::Key.eq(key))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(leases[0].owner_object_id, expected.id, "target={target}");
            assert_eq!(leases[0].source, "automatic");
        }
        worker.shutdown(Duration::from_secs(2)).await;
        server.shutdown().await;
    }
}

#[tokio::test]
async fn root_only_recovery_after_signed_source_delete_or_overwrite_preserves_the_original_receipt()
{
    use ipfs_s3_gateway::kubo::directory::build_directory;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

    for operation in [reqwest::Method::DELETE, reqwest::Method::PUT] {
        let h = start_import_harness(ImportHarnessConfig::default()).await;
        let archive = legal_single_entry_zip();
        h.set_cat_body(CID, archive.clone());
        let token = format!("source-root-retry-{operation}");
        let mut headers = controls(&token, true);
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_static("label=source&ipfs-s3%3Apin=true&ipfs-s3%3Azip-root=true"),
        );
        let accepted = send_sigv4(
            reqwest::Method::POST,
            &h.endpoint,
            &h.bucket,
            "archive.zip",
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            cid_xml().into_bytes(),
            headers.clone(),
            "test",
        )
        .await;
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let id = accepted.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(wait(&h, &id).await, "ready");
        let db = h.state.store.db();
        let published = zip::snapshot(db, &id).await.unwrap().unwrap();
        assert!(published.batch.source_published);
        assert_eq!(published.batch.root_status, "failed");
        let receipt = sealed_receipt(&h, &id).await;
        let terminal = execution::read(db, &id)
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .unwrap();
        let binding: serde_json::Value = serde_json::from_str(&terminal).unwrap();
        let source_version = binding["source_version_row_id"].as_str().unwrap();
        assert!(
            object_version::Entity::find_by_id(source_version)
                .one(db)
                .await
                .unwrap()
                .is_some()
        );
        let mut replacement = HeaderMap::new();
        replacement.insert("x-amz-meta-label", HeaderValue::from_static("replacement"));
        let body = if operation == reqwest::Method::PUT {
            archive
        } else {
            vec![]
        };
        let changed = send_sigv4(
            operation.clone(),
            &h.endpoint,
            &h.bucket,
            "archive.zip",
            &[],
            body,
            replacement,
            "test",
        )
        .await;
        assert_eq!(
            changed.status(),
            if operation == reqwest::Method::PUT {
                StatusCode::OK
            } else {
                StatusCode::NO_CONTENT
            }
        );
        // In an unversioned bucket, the original row has really disappeared.
        assert!(
            object_version::Entity::find_by_id(source_version)
                .one(db)
                .await
                .unwrap()
                .is_none()
        );
        let versions = object_version::Entity::find().count(db).await.unwrap();
        let objects = object::Entity::find().count(db).await.unwrap();
        let cat_calls = h.kubo_call_count("/api/v0/cat").await;
        let adds = h.kubo_call_count("/api/v0/add").await;
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .count(db)
                .await
                .unwrap(),
            0
        );

        // Repair only Kubo's directory RPCs; the actual builder performs DAG CID,
        // recursive pin, path resolution and full local-DAG verification.
        for (endpoint, body) in [
            ("/api/v0/id", "{\"ID\":\"localNode\"}".to_owned()),
            (
                "/api/v0/dag/put",
                format!("{{\"Cid\":{{\"/\":\"{ROOT}\"}}}}"),
            ),
            ("/api/v0/resolve", format!("{{\"Path\":\"/ipfs/{CID}\"}}")),
            (
                "/api/v0/pin/ls",
                format!("{{\"Keys\":{{\"{ROOT}\":{{\"Type\":\"recursive\"}}}}}}"),
            ),
        ] {
            Mock::given(method("POST"))
                .and(path(endpoint))
                .respond_with(ResponseTemplate::new(200).set_body_string(body))
                .mount(&h.kubo)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("with-local", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Hash\":\"{ROOT}\",\"WithLocality\":true,\"Local\":true}}"
            )))
            .with_priority(2)
            .mount(&h.kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{CID}\",\"CumulativeSize\":0}}")),
            )
            .mount(&h.kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", ROOT))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{ROOT}\"]}}")),
            )
            .with_priority(1)
            .mount(&h.kubo)
            .await;
        db.execute(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until=datetime('now','-1 minute') WHERE batch_id=?",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        let claim = zip::claim_root(db, &id, "source-root-only", 60)
            .await
            .unwrap();
        let snapshot = zip::snapshot(db, &id).await.unwrap().unwrap();
        let files = zip::recovery::files(&snapshot, &claim).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "file.txt");
        zip::mark_invoked(db, &claim).await.unwrap();
        let verified = build_directory(
            &h.state.kubo,
            &files,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(verified.cid, ROOT);
        let node = verified.local_residency.node_identity.clone();
        zip::retain_candidate(db, &claim, &node, "hot", &verified.cid)
            .await
            .unwrap();
        zip::verify_root(
            db,
            &claim,
            &node,
            "hot",
            &verified.cid,
            &serde_json::to_string(&verified.local_residency).unwrap(),
        )
        .await
        .unwrap();
        zip::recovery::settle_verified(db, &snapshot, claim, node, verified.cid)
            .await
            .unwrap();

        let recovered = zip::snapshot(db, &id).await.unwrap().unwrap();
        assert_eq!(recovered.batch.root_status, "complete");
        assert_eq!(recovered.batch.root_cid.as_deref(), Some(ROOT));
        assert!(
            recovered
                .references
                .iter()
                .any(|r| r.cid == ROOT && r.state == "adopted" && r.verification_receipt.is_some())
        );
        assert_eq!(
            execution::read(db, &id)
                .await
                .unwrap()
                .unwrap()
                .terminal_result
                .as_deref(),
            Some(terminal.as_str())
        );
        assert_eq!(sealed_receipt(&h, &id).await, receipt);
        assert_eq!(
            object_version::Entity::find().count(db).await.unwrap(),
            versions
        );
        assert_eq!(object::Entity::find().count(db).await.unwrap(), objects);
        assert_eq!(h.kubo_call_count("/api/v0/add").await, adds);
        assert_eq!(h.kubo_call_count("/api/v0/cat").await, cat_calls);
        assert!(h.source.requests().is_empty());
        let status = get_import_status(&h, &h.bucket, "archive.zip", &id, None, None).await;
        let xml = String::from_utf8(status.into_body()).unwrap();
        assert!(xml.contains(&format!("<ETag>{CID}</ETag>")), "{xml}");
        assert!(xml.contains("<Phase>published</Phase>"), "{xml}");
        let status = import_intake::read_for_path(db, &id, "test", &h.bucket, "archive.zip")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.root_status.as_deref(), Some("complete"));
        assert_eq!(status.published_source.unwrap().cid, CID);
        let io = h.kubo.received_requests().await.unwrap().len();
        let replay = send_sigv4(
            reqwest::Method::POST,
            &h.endpoint,
            &h.bucket,
            "archive.zip",
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            cid_xml().into_bytes(),
            headers,
            "test",
        )
        .await;
        assert_eq!(replay.status(), StatusCode::ACCEPTED);
        assert_eq!(replay.headers()["x-ipfs3-import-job-id"], id);
        assert!(!replay.headers().contains_key("etag"));
        assert_eq!(h.kubo.received_requests().await.unwrap().len(), io);
        assert_eq!(sealed_receipt(&h, &id).await, receipt);
        h.shutdown().await;
    }
}
