//! Complete forge coverage against a real first-intake MPU and bound versions.
#[path = "stage9_zip_root_capture_review/provenance.rs"]
mod provenance;
#[path = "stage9_zip_root_capture_review/support.rs"]
mod review;
#[allow(dead_code)]
mod support;

use ipfs_s3_gateway::store::zip;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test]
async fn all_original_root_capture_forges_are_rejected_without_short_circuiting() {
    let (harness, snapshot, claim) = review::published_mpu().await;
    assert!(zip::recovery::files(&snapshot, &claim).is_ok());
    let captured: Value = serde_json::from_str(&snapshot.batch.captured_options).unwrap();
    let before = review::counters(&harness).await;
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let mut accepted = Vec::new();
    let mut checked = 0;
    for (pointer, value) in [
        ("/options/result_version", json!(1)),
        ("/options/result_version", json!("2")),
        ("/options/publish_source", json!(true)),
        ("/options/publish_source", json!("false")),
        ("/options/publish_extracted", json!(false)),
        ("/options/publish_extracted", json!(1)),
        ("/options/targets", json!("source")),
        ("/options/targets", json!("both")),
        ("/options/targets", json!("all")),
        ("/options/token", json!("other")),
        ("/options/token", json!("")),
        ("/options/token", json!("bad/token")),
        ("/options/root_enabled", json!(false)),
        ("/options/root_override", json!(false)),
        ("/options/root_override", json!("true")),
        ("/target_prefix", json!("other/")),
        ("/schema_version", json!(2)),
        ("/rule_revision", json!("different")),
        ("/zip_intent/bucket", json!("another")),
        ("/zip_intent/source_key", json!("another.zip")),
        ("/zip_intent/target_prefix", json!("other/")),
        (
            "/zip_intent/source_decision/origin/principal_id",
            json!("another"),
        ),
        (
            "/zip_intent/source_decision/origin/request_id",
            json!("another"),
        ),
        ("/zip_intent/controls/admission_token", json!("other")),
        ("/zip_intent/controls/root", json!({"configured":false})),
        ("/zip_intent/schema_version", json!(2)),
        (
            "/options",
            json!({"root_enabled":true,"publish_extracted":true}),
        ),
        ("/zip_intent", json!({"root_capture":{"configured":true}})),
    ] {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut value_json = captured.clone();
        *value_json.pointer_mut(pointer).unwrap() = value.clone();
        forged.batch.captured_options = value_json.to_string();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("pointer={pointer} value={value} rejected={rejected}");
        checked += 1;
        if !rejected {
            accepted.push(format!("{pointer}={value}"));
        }
    }
    // The original pointer array has 28 variants, including both partial captures.
    assert_eq!(checked, 28);
    for field in captured.as_object().unwrap().keys() {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut value = captured.clone();
        value.as_object_mut().unwrap().remove(field);
        forged.batch.captured_options = value.to_string();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("missing={field} rejected={rejected}");
        if !rejected {
            accepted.push(format!("missing={field}"));
        }
    }
    for source in ["direct", "import"] {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        forged.batch.source = source.into();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("source={source} rejected={rejected}");
        if !rejected {
            accepted.push(format!("source={source}"));
        }
    }
    for token in ["bad/token".to_owned(), "x".repeat(129)] {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut value = captured.clone();
        value["options"]["token"] = token.clone().into();
        value["zip_intent"]["controls"]["admission_token"] = token.clone().into();
        forged.batch.token = token.clone();
        forged.batch.captured_options = value.to_string();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("agreed-token={token} rejected={rejected}");
        if !rejected {
            accepted.push(format!("agreed-token={token}"));
        }
    }
    for field in captured["options"].as_object().unwrap().keys() {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut value = captured.clone();
        value["options"].as_object_mut().unwrap().remove(field);
        forged.batch.captured_options = value.to_string();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("missing-option={field} rejected={rejected}");
        if !rejected {
            accepted.push(format!("missing-option={field}"));
        }
    }
    let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    forged.batch.source_published = true;
    assert!(zip::recovery::files(&forged, &claim).is_err());
    eprintln!("source-published-mismatch=true rejected=true");
    for (field, value) in [
        ("root_enabled", json!(true)),
        ("root_capture", json!({"configured":true})),
        ("publish_source", json!(false)),
        ("token", json!("mpu-root-review")),
        ("legacy_zip_intent", json!({"root_capture":{"tagged":true}})),
    ] {
        let mut forged = zip::snapshot(harness.state.store.db(), &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut value_json = captured.clone();
        value_json[field] = value.clone();
        forged.batch.captured_options = value_json.to_string();
        let rejected = zip::recovery::files(&forged, &claim).is_err();
        eprintln!("mixed-marker={field} value={value} rejected={rejected}");
        if !rejected {
            accepted.push(format!("mixed-marker={field}"));
        }
    }
    assert_eq!(review::counters(&harness).await, before);
    assert_eq!(harness.kubo.received_requests().await.unwrap().len(), calls);
    assert!(
        accepted.is_empty(),
        "all variants reached; accepted={accepted:?}"
    );
}

#[tokio::test]
async fn malformed_persisted_root_capture_is_fenced_before_dag_or_leases_or_quota() {
    for (pointer, value) in [
        ("/options/result_version", json!(1)),
        (
            "/zip_intent/source_decision/origin/request_id",
            json!("another"),
        ),
        (
            "/options",
            json!({"root_enabled":true,"publish_extracted":true}),
        ),
    ] {
        let (harness, snapshot, claim) = review::published_mpu().await;
        let mut captured: Value = serde_json::from_str(&snapshot.batch.captured_options).unwrap();
        *captured.pointer_mut(pointer).unwrap() = value;
        let bytes = captured.to_string();
        let db = harness.state.store.db();
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_batches SET captured_options=? WHERE id=?",
            vec![bytes.clone().into(), claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
            vec![claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        let before = review::counters(&harness).await;
        let calls = harness.kubo.received_requests().await.unwrap().len();
        let additions = harness.captured_add_file_bytes();
        assert_eq!(
            ipfs_s3_gateway::zip::recovery::run_page(
                db,
                &harness.state.kubo,
                &CancellationToken::new()
            )
            .await
            .unwrap(),
            1
        );
        let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        assert_eq!(
            harness.kubo.received_requests().await.unwrap().len(),
            calls,
            "no Kubo at all for {pointer}"
        );
        assert_eq!(harness.captured_add_file_bytes(), additions);
        assert_eq!(review::counters(&harness).await, before);
        assert_eq!(after.entries, snapshot.entries);
        assert_eq!(after.batch.captured_options, bytes);
        assert_eq!(after.batch.terminal_result, snapshot.batch.terminal_result);
        assert!(!after.batch.source_published);
        // The original execution/capture binder also rejects settlement of
        // this damaged batch. Native claim failure must never invoke Kubo.
        assert_eq!(after.batch.root_status, "failed");
        assert_eq!(after.batch.root_error_code, snapshot.batch.root_error_code);
        assert_eq!(after.builds.last().unwrap().status, "intent");
        assert!(after.batch.root_cid.is_none());
        eprintln!(
            "persisted={pointer} no-extra-rpc=true counters={before:?} captured-bytes-unchanged=true root=failed build=intent native-settlement-fenced=true"
        );
    }
}
