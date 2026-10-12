use super::{migration, native_catalog, store};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use sea_orm_migration::{MigrationTrait, SchemaManager};

fn historical_decision(request: &str) -> String {
    use crate::pinning::{
        config::ValidatedPinningConfig,
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision, config_revision},
        policy::PublicationPolicy,
        tags::PinControl,
    };
    let config = crate::config::Config::default_for_test();
    let validated = ValidatedPinningConfig::from_config(&config, |_| None).unwrap();
    let decision = ExtensionDecision::capture(
        DecisionOrigin::new("historical-owner", request),
        config_revision(&validated, config.pinning_control.unavailable),
        DecisionEffect::NoIntent,
        None,
        PinControl::Absent,
        &PublicationPolicy {
            tags: vec![],
            leases: vec![],
        },
    )
    .unwrap();
    serde_json::to_string_pretty(&decision).unwrap()
}

async fn catalog_metadata(db: &DatabaseConnection) -> Vec<(String, String, Option<String>)> {
    db.query_all(Statement::from_string(
        DatabaseBackend::Sqlite,
        "SELECT type,name,sql FROM sqlite_schema ORDER BY type,name",
    ))
    .await
    .unwrap()
    .iter()
    .map(|r| {
        (
            r.try_get("", "type").unwrap(),
            r.try_get("", "name").unwrap(),
            r.try_get("", "sql").unwrap(),
        )
    })
    .collect()
}

#[tokio::test]
async fn native_historical_captures_are_not_filled_or_rewritten_by_inventory_or_real_upgrade() {
    let db = native_catalog(migration::SCHEMA).await;
    let mpu_decision = historical_decision("historical-mpu");
    let import_decision = historical_decision("historical-job");
    let root_capture =
        serde_json::to_string_pretty(&crate::pinning::tags::ZipRootCapture::Configured(false))
            .unwrap();
    let batch_options = "{  \"root_enabled\": false, \"target_prefix\": \"expanded/\" }";
    db.execute_unprepared("INSERT INTO buckets(name) VALUES ('historical-bucket')")
        .await
        .unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO multipart_uploads(upload_id,object_id,bucket,key,pin_decision_json) VALUES ('historical-mpu','historical-object','historical-bucket','archive.zip',$1)",
        [mpu_decision.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO import_jobs(id,bucket,key,source_type,source_value,request_fingerprint,metadata_json,tags_json,pin_decision_json,root_capture_json,decompress_prefix,state,phase,attempts,next_attempt_at,providers_observed,pin_nodes_processed,pin_bytes_processed,downloaded_bytes,ipfs_add_bytes,entries_processed,entries_succeeded,entries_failed,decompressed_bytes,created_at,updated_at) VALUES ('historical-job','historical-bucket','import.zip','cid','bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku','historical-fingerprint','{}','[]',$1,$2,'expanded/','queued','queued',0,CURRENT_TIMESTAMP,0,0,0,0,0,0,0,0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [import_decision.clone().into(),root_capture.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO import_destinations(bucket,key,generation,owner_job_id,updated_at) VALUES ('historical-bucket','import.zip',1,'historical-job',CURRENT_TIMESTAMP)",
        [])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES ('historical-job','historical-owner','import','historical-job','historical-fingerprint','historical-bucket','import.zip','historical-input',$1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [batch_options.into()])).await.unwrap();
    let versions = super::super::schema(&db, false).await.unwrap();
    let metadata = catalog_metadata(&db).await;
    let before = super::super::read(&db, &versions).await.unwrap();
    assert_eq!(catalog_metadata(&db).await, metadata);
    assert_eq!(super::super::schema(&db, false).await.unwrap(), versions);
    assert_eq!(super::super::read(&db, &versions).await.unwrap(), before);
    for (table, column, payload) in [
        (
            "multipart_uploads",
            "pin_decision_json",
            mpu_decision.as_str(),
        ),
        ("import_jobs", "pin_decision_json", import_decision.as_str()),
        ("import_jobs", "root_capture_json", root_capture.as_str()),
        ("zip_batches", "captured_options", batch_options),
    ] {
        assert_eq!(
            before.iter().find(|t| t.table == table).unwrap().rows[0][column].as_deref(),
            Some(payload)
        );
    }

    store::run_migrations(&db).await.unwrap();
    let upgraded_versions = super::super::schema(&db, true).await.unwrap();
    let upgraded = super::super::read(&db, &upgraded_versions).await.unwrap();
    let mut historical_shape = upgraded.clone();
    let scope = historical_shape
        .iter()
        .find(|table| table.table == "pin_management_records")
        .unwrap();
    assert_eq!(scope.rows.len(), 1);
    assert_eq!(scope.rows[0]["kind"].as_deref(), Some("database_scope"));
    assert!(
        historical_shape
            .iter()
            .find(|table| table.table == "pin_management_retirements")
            .unwrap()
            .rows
            .is_empty()
    );
    historical_shape.retain(|table| {
        !matches!(
            table.table.as_str(),
            "pin_management_records" | "pin_management_retirements"
        )
    });
    for (table, column) in [
        ("multipart_uploads", "zip_intent_json"),
        ("import_jobs", "zip_intent_json"),
        ("zip_batches", "legacy_input_proof_json"),
    ] {
        let image = historical_shape
            .iter_mut()
            .find(|t| t.table == table)
            .unwrap();
        assert!(image.columns.iter().any(|c| c == column));
        assert_eq!(image.rows[0].remove(column), Some(None));
        image.columns.retain(|c| c != column);
    }
    assert_eq!(
        historical_shape, before,
        "all native historical columns and rows remain unchanged"
    );
    let upgraded_metadata = catalog_metadata(&db).await;
    assert!(
        store::migrations::m20261008_000001_zip_intent_capture::Migration
            .down(&SchemaManager::new(&db))
            .await
            .is_err()
    );
    assert_eq!(catalog_metadata(&db).await, upgraded_metadata);
    assert_eq!(
        super::super::schema(&db, true).await.unwrap(),
        upgraded_versions
    );
    assert_eq!(
        super::super::read(&db, &upgraded_versions).await.unwrap(),
        upgraded
    );
}
