//! Published source conserves its captured authority, quota and replay receipt.
#[path = "support/r25_review_wire_fixtures_historical.rs"]
mod historical;
#[allow(dead_code)]
#[path = "support/r25_review_wire_fixtures_import.rs"]
mod r25_fixture;
#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use ipfs_s3_gateway::store::zip::{execution, import_intake};
use r25_fixture::{
    ImportHarness, ImportHarnessConfig, ImportPublicationBlockControl, TestHttpsReply,
    get_import_status, start_import_harness,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sha2::{Digest, Sha256};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

async fn sealed_receipt(harness: &ImportHarness, id: &str) -> String {
    harness
        .state
        .store
        .db()
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT receipt_metadata FROM zip_v2_import_requests WHERE batch_id=?",
            [id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "receipt_metadata")
        .unwrap()
}

#[tokio::test]
async fn published_original_source_has_one_version_manual_group_and_conserved_quota_on_replay() {
    let gate = Arc::new(ImportPublicationBlockControl::new());
    let harness = start_import_harness(ImportHarnessConfig {
        execution_observer: Some(gate.clone()),
        ..Default::default()
    })
    .await;
    let archive = support::decompress::duplicate_entry_zip();
    let measured = hex::encode(Sha256::digest(&archive));
    harness.source.set_reply(
        "/historical-source.zip",
        TestHttpsReply::chunked(archive.clone()),
    );
    let request = historical::source_only_request(
        harness.state.pinning.effective_config(),
        &harness.bucket,
        "historical-source",
        ipfs_s3_gateway::import::ImportSource::Url(
            harness
                .source
                .url("/historical-source.zip")
                .parse()
                .unwrap(),
        ),
        &measured,
    );
    let db = harness.state.store.db();
    let id = import_intake::admit(db, &request).await.unwrap().id;
    gate.wait_until_blocked(&id).await;
    let admitted = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(admitted.state, "admitted");
    assert_eq!(admitted.input_sha256.as_deref(), Some(measured.as_str()));
    assert_eq!(
        admitted.captured_options,
        request.admission.captured_options
    );
    gate.release();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status =
                import_intake::read_for_path(db, &id, "test", &harness.bucket, "archive.zip")
                    .await
                    .unwrap()
                    .unwrap();
            if status.state != "pending" {
                assert_eq!(status.state, "ready");
                assert_eq!(status.expected_sha256.as_deref(), Some(measured.as_str()));
                assert_eq!(status.measured_sha256.as_deref(), Some(measured.as_str()));
                assert_eq!(status.published_source.unwrap().cid, CID);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("original 10s terminal budget");
    r25_fixture::assert_manual_source(db, &id).await;
    let persisted = execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(
        persisted.captured_options,
        request.admission.captured_options
    );
    assert_eq!(persisted.input_art_size, Some(archive.len() as i64));
    assert_eq!(persisted.epoch, 1);
    let receipt = sealed_receipt(&harness, &id).await;
    let status = get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    let status = String::from_utf8(status.into_body()).unwrap();
    assert!(status.contains("<State>ready</State>"));
    assert!(status.contains(&format!("<ETag>{CID}</ETag>")));
    assert!(status.contains(&format!("<ExpectedSHA256>{measured}</ExpectedSHA256>")));
    assert!(status.contains(&format!("<MeasuredSHA256>{measured}</MeasuredSHA256>")));
    for (table, expected) in [
        ("objects", 1),
        ("object_versions", 1),
        ("pin_leases", 1),
        ("pin_lease_targets", 1),
        ("pin_jobs", 1),
        ("remote_pins", 1),
        ("standard_mutation_leases", 0),
    ] {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT COUNT(*) AS n FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i64>("", "n").unwrap(), expected, "{table}");
    }
    let usage = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n,SUM(reserved_bytes) AS bytes,SUM(reserved_pins) AS pins FROM pin_provider_usage".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usage.try_get::<i64>("", "n").unwrap(), 1);
    assert_eq!(
        usage.try_get::<i64>("", "bytes").unwrap(),
        archive.len() as i64
    );
    assert_eq!(usage.try_get::<i64>("", "pins").unwrap(), 1);
    assert_eq!(harness.source.requests().len(), 1);
    assert_eq!(harness.kubo_call_count("/api/v0/add").await, 1);
    assert_eq!(harness.kubo_call_count("/api/v0/pin/add").await, 1);
    assert_eq!(harness.kubo_call_count("/api/v0/cat").await, 0);
    let io = harness.kubo_total_call_count().await;
    harness.source.set_reply(
        "/historical-source.zip",
        TestHttpsReply::chunked(b"changed".to_vec()),
    );
    assert_eq!(import_intake::admit(db, &request).await.unwrap().id, id);
    assert_eq!(execution::read(db, &id).await.unwrap().unwrap(), persisted);
    let replay = sealed_receipt(&harness, &id).await;
    assert_eq!(replay, receipt);
    assert_eq!(harness.kubo_total_call_count().await, io);
    assert_eq!(harness.source.requests().len(), 1);
    let replay_usage = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT SUM(reserved_bytes) AS bytes,SUM(reserved_pins) AS pins FROM pin_provider_usage".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay_usage.try_get::<i64>("", "bytes").unwrap(),
        archive.len() as i64
    );
    assert_eq!(replay_usage.try_get::<i64>("", "pins").unwrap(), 1);
    harness.shutdown().await;
}
