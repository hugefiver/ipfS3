use super::{stage9_zip_support::setup, v2_mpu::intake_with_source};
use ipfs_s3_gateway::store::{
    import::ownership,
    multipart::v2_zip,
    zip::{self, BatchAdmission, execution},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde_json::Value;
use std::collections::BTreeMap;

const ID: &str = "mpu-renew";

async fn prepared(source: bool) -> (DatabaseConnection, execution::Claim) {
    let db = setup().await;
    let (request, capture) = intake_with_source(ID, true, false, source);
    v2_zip::create_or_replay_with_zip_intent(&db, &request, &capture)
        .await
        .unwrap();
    let claim = execution::claim(&db, ID, "renew-worker", 60)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(&db, &claim, &"d".repeat(64), "archive-cid", 5)
        .await
        .unwrap();
    let row = execution::read(&db, ID).await.unwrap().unwrap();
    zip::admit(
        &db,
        &BatchAdmission {
            id: ID.into(),
            owner: request.owner,
            source: "mpu".into(),
            token: request.token,
            fingerprint: "pending".into(),
            bucket: request.bucket,
            archive_key: request.key,
            input_identity: "pending".into(),
            captured_options: row.captured_options,
        },
    )
    .await
    .unwrap();
    let manifest = ["a", "b"].map(|name| execution::ManifestItem::Success {
        path: name.into(),
        object_key: format!("out/{name}"),
        cid: "output-cid".into(),
        size: 5,
    });
    zip::prepare_manifest(
        &db,
        ID,
        &manifest
            .iter()
            .map(|item| match item {
                execution::ManifestItem::Success {
                    path,
                    object_key,
                    cid,
                    size,
                } => zip::ManifestItem::Success {
                    path: path.clone(),
                    object_key: object_key.clone(),
                    cid: cid.clone(),
                    size: *size,
                },
                _ => unreachable!(),
            })
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    let ids = BTreeMap::from([
        ("out/a".into(), "guard-a".into()),
        ("out/b".into(), "guard-b".into()),
    ]);
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &manifest, &ids)
        .await
        .unwrap();
    if source {
        let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
            .await
            .unwrap();
        let guard = ownership::admit_zip_v2_source_in_transaction(
            &tx,
            "bucket",
            "out/archive.zip",
            &ids.keys().cloned().collect(),
            &format!("zip-v2-source:{ID}"),
            now,
        )
        .await
        .unwrap();
        ownership::capture_zip_v2_source_guard_in_transaction(&tx, &claim, &guard)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    // Valid but short native leases expose real renewal without sleeping or
    // changing the production lease checks/timeouts.
    db.execute_unprepared(
        "UPDATE standard_mutation_leases SET lease_until=datetime('now','+5 seconds')",
    )
    .await
    .unwrap();
    (db, claim)
}

async fn leases(db: &DatabaseConnection) -> BTreeMap<String, String> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT mutation_id,lease_until FROM standard_mutation_leases ORDER BY mutation_id"
            .to_owned(),
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "mutation_id").unwrap(),
            row.try_get("", "lease_until").unwrap(),
        )
    })
    .collect()
}

async fn store_options(db: &DatabaseConnection, value: &Value) {
    for table in ["zip_v2_executions", "zip_v2_mpu_intakes"] {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("UPDATE {table} SET captured_options=?"),
            vec![value.to_string().into()],
        ))
        .await
        .unwrap();
    }
}

async fn rejected_without_partial_renew(db: &DatabaseConnection, claim: &execution::Claim) {
    let before = leases(db).await;
    assert!(
        !ownership::renew_zip_v2_direct_group(db, claim, 60)
            .await
            .unwrap()
    );
    assert_eq!(
        leases(db).await,
        before,
        "source and all output lease writes must roll back"
    );
    assert_eq!(
        execution::read(db, ID).await.unwrap().unwrap().state,
        "fenced"
    );
}

#[tokio::test]
async fn original_mpu_envelope_renews_source_present_and_absent_with_all_output_guards() {
    for source in [true, false] {
        let (db, claim) = prepared(source).await;
        let before = leases(&db).await;
        assert_eq!(before.len(), if source { 3 } else { 2 });
        assert!(
            ownership::renew_zip_v2_direct_group(&db, &claim, 60)
                .await
                .unwrap()
        );
        let after = leases(&db).await;
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            before.keys().collect::<Vec<_>>()
        );
        assert!(after.iter().all(|(id, until)| until > &before[id]));
        let row = execution::read(&db, ID).await.unwrap().unwrap();
        assert_eq!(row.state, "admitted");
        assert_eq!(row.epoch, claim.epoch);
        assert_eq!(row.worker.as_deref(), Some(claim.worker.as_str()));
    }
}

#[tokio::test]
async fn historical_bare_options_require_original_early_capture_not_null_fresh_authority() {
    for has_capture in [true, false] {
        let (db, claim) = prepared(false).await;
        let row = execution::read(&db, ID).await.unwrap().unwrap();
        let envelope: Value = serde_json::from_str(&row.captured_options).unwrap();
        store_options(&db, &envelope["options"]).await;
        if has_capture {
            assert!(
                ownership::renew_zip_v2_direct_group(&db, &claim, 60)
                    .await
                    .unwrap()
            );
        } else {
            db.execute_unprepared(
                "UPDATE multipart_uploads SET zip_intent_json=NULL,pin_decision_json=NULL",
            )
            .await
            .unwrap();
            rejected_without_partial_renew(&db, &claim).await;
        }
    }
}

#[tokio::test]
async fn malformed_or_contradictory_mpu_envelope_never_falls_back_to_bare_options() {
    for damage in [
        "schema",
        "type",
        "top-level",
        "contradiction",
        "missing-options",
    ] {
        let (db, claim) = prepared(true).await;
        let row = execution::read(&db, ID).await.unwrap().unwrap();
        let mut envelope: Value = serde_json::from_str(&row.captured_options).unwrap();
        match damage {
            "schema" => envelope["schema_version"] = 2.into(),
            "type" => envelope["options"]["publish_source"] = "true".into(),
            "top-level" => envelope["publish_source"] = false.into(),
            "contradiction" => envelope["options"]["publish_source"] = false.into(),
            "missing-options" => {
                let options = envelope.as_object_mut().unwrap().remove("options").unwrap();
                envelope
                    .as_object_mut()
                    .unwrap()
                    .extend(options.as_object().unwrap().clone());
            }
            _ => unreachable!(),
        }
        store_options(&db, &envelope).await;
        rejected_without_partial_renew(&db, &claim).await;
    }
}

#[tokio::test]
async fn source_and_output_guard_drift_fences_group_without_partial_lease_renewal() {
    for damage in [
        "UPDATE zip_batches SET input_identity='zip-v2-source-gen:2'",
        "UPDATE zip_v2_targets SET expected_generation=expected_generation+1 WHERE object_key='out/b'",
        "UPDATE standard_mutation_leases SET lease_until=datetime('now','-1 seconds') WHERE mutation_id='guard-b'",
        "UPDATE zip_v2_mpu_intakes SET token='different-token'",
    ] {
        let (db, claim) = prepared(true).await;
        db.execute_unprepared(damage).await.unwrap();
        rejected_without_partial_renew(&db, &claim).await;
    }
}
