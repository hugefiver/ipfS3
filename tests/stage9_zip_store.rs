#[path = "stage9_zip_support/renew.rs"]
mod renew;
mod stage9_zip_support;
#[path = "stage9_zip_support/v2_mpu.rs"]
mod v2_mpu;
use ipfs_s3_gateway::{
    config::PinningConfig,
    pinning::{config::ValidatedPinningConfig, tags::ObjectTag},
    store::{
        self,
        entities::{
            object, object_version, pin_extension_decision, pin_job, pin_lease, pin_lease_target,
            pin_provider_usage, remote_pin,
        },
        pinning::publication,
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait};
use stage9_zip_support::*;

async fn sqlite_publication_snapshot(
    db: &sea_orm::DatabaseConnection,
) -> std::collections::BTreeMap<String, Vec<serde_json::Value>> {
    use sea_orm::{DatabaseBackend, Statement};
    let tables = db.query_all(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE type='table' AND substr(name,1,7)!='sqlite_' ORDER BY name".to_owned(),
    )).await.unwrap();
    let mut snapshot = std::collections::BTreeMap::new();
    for table in tables {
        let name: String = table.try_get("", "name").unwrap();
        // Identifiers come only from this migrated SQLite schema. Preserve each
        // native cell's type as well as value, including any blob bytes.
        let quoted = format!("\"{}\"", name.replace('"', "\"\""));
        let columns = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("PRAGMA table_info({quoted})"),
            ))
            .await
            .unwrap();
        let fields = columns.into_iter().map(|column| {
            let column: String = column.try_get("", "name").unwrap();
            let key = column.replace('\'', "''");
            let id = format!("\"{}\"", column.replace('"', "\"\""));
            format!("'{key}',json_object('type',typeof({id}),'value',CASE WHEN typeof({id})='blob' THEN hex({id}) ELSE {id} END)")
        }).collect::<Vec<_>>().join(",");
        let rows = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT json_object({fields}) AS row_json FROM {quoted} ORDER BY row_json"),
            ))
            .await
            .unwrap();
        snapshot.insert(
            name,
            rows.into_iter()
                .map(|row| {
                    serde_json::from_str(&row.try_get::<String>("", "row_json").unwrap()).unwrap()
                })
                .collect(),
        );
    }
    snapshot
}

#[tokio::test]
async fn r25_store_nonfinal_duplicate_collection_rejected_without_admitted_state_changes() {
    use ipfs_s3_gateway::zip::batch::{FinalZipManifest, ManifestFile};
    use publication::{CapturedZipPublication, LegacyZipAuthority, ZipPublicationAuthority};
    let db = setup().await;
    let (request, batch, capture, guard) = prepared_with_outputs(
        &db,
        &config(),
        vec![],
        vec![
            ("first".into(), object("first", "out/same.txt", "first-cid")),
            (
                "second".into(),
                object("second", "out/same.txt", "second-cid"),
            ),
        ],
    )
    .await;
    let manifest = FinalZipManifest {
        successful: request
            .entries
            .iter()
            .zip(&batch.entries)
            .map(|(object, binding)| ManifestFile {
                relative_path: binding.path.clone(),
                object_key: object.key.clone(),
                cid: object.cid.clone(),
                size: object.logical_size,
            })
            .collect(),
        ..Default::default()
    };
    let before = sqlite_publication_snapshot(&db).await;
    assert_eq!(before["zip_batches"].len(), 1);
    assert_eq!(before["zip_manifest_entries"].len(), 2);
    let error = publication::publish_captured_zip(
        &db,
        CapturedZipPublication {
            batch_id: batch.batch_id,
            source: Some(request.archive.object),
            source_tags: request.archive.tags,
            successes: request.entries,
            manifest,
            capture,
            authority: ZipPublicationAuthority::Legacy(Box::new(LegacyZipAuthority {
                standard_guard: Some(guard),
                upload_target: None,
                import_guard: None,
                result_rows: vec![],
                replay: None,
            })),
            root_outcome: batch.root_outcome,
            terminal_result: batch.terminal_result,
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_)
    ));
    assert_eq!(sqlite_publication_snapshot(&db).await, before);
}

#[tokio::test]
async fn r25_store_duplicate_final_winner_owns_only_last_exact_cid_and_version() {
    use ipfs_s3_gateway::zip::{batch::final_zip_manifest, response::ExtractedEntry};
    use sea_orm::{ColumnTrait, QueryFilter};
    let db = setup().await;
    store::bucket::set_versioning_state(
        &db,
        "bucket",
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    let config = config();
    let raw = [
        object("first", "out/same.txt", "first-cid"),
        object("second", "out/same.txt", "second-cid"),
    ];
    let manifest = final_zip_manifest(
        &raw.iter()
            .map(|entry| ExtractedEntry {
                key: entry.key.clone(),
                cid: entry.cid.clone(),
                size: entry.logical_size,
            })
            .collect::<Vec<_>>(),
        &[],
        "out/",
    );
    assert_eq!(manifest.successful.len(), 1);
    assert_eq!(manifest.successful[0].cid, "second-cid");
    let winners = manifest
        .successful
        .iter()
        .map(|file| {
            let exact = raw
                .iter()
                .find(|object| object.key == file.object_key && object.cid == file.cid)
                .unwrap();
            (file.relative_path.clone(), exact.clone())
        })
        .collect();
    let (request, batch, capture, guard) =
        prepared_with_outputs(&db, &config, vec![], winners).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let latest = store::object::get_latest(&db, "bucket", "out/same.txt")
        .await
        .unwrap();
    assert_eq!(
        (latest.id.as_str(), latest.cid.as_str()),
        ("second", "second-cid")
    );
    assert!(
        object::Entity::find_by_id("first")
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    let versions = object_version::Entity::find()
        .filter(object_version::Column::Key.eq("out/same.txt"))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].object_id.as_deref(), Some("second"));
    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(
        (
            leases[0].source.as_str(),
            leases[0].owner_object_id.as_str()
        ),
        ("automatic", "second")
    );
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].cid, "second-cid");
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(
        snapshot.entries[0].version_row_id.as_deref(),
        Some(versions[0].id.as_str())
    );
}

#[tokio::test]
async fn r25_store_exact_outputs_own_automatic_shared_cid_once() {
    let db = setup().await;
    let config = config();
    let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    assert!(capture.controls().targets.extracted);
    assert_eq!(
        publication::decode_zip_capture_json(
            &serde_json::json!({"zip_intent": capture}).to_string()
        )
        .unwrap(),
        capture
    );
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 2);
    assert!(
        leases
            .iter()
            .all(|l| l.source == "automatic" && ["a", "b"].contains(&l.owner_object_id.as_str()))
    );
    assert_eq!(
        pin_lease_target::Entity::find().count(&db).await.unwrap(),
        2
    );
    let usage = pin_provider_usage::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (5, 1));
    assert_eq!(object::Entity::find().count(&db).await.unwrap(), 3);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
    let decisions = pin_extension_decision::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert_eq!(decisions.len(), 3);
    assert_eq!(
        decisions
            .iter()
            .map(|d| &d.control_revision)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_some()));
}

#[tokio::test]
async fn r25_store_tail_manifest_fault_rolls_back_versions_leases_quota_jobs_and_bindings() {
    let db = setup().await;
    let config = config();
    let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    db.execute_unprepared("CREATE TRIGGER r25_tail BEFORE UPDATE OF state ON zip_batches WHEN NEW.state='published' BEGIN SELECT RAISE(ABORT,'r25 tail failure'); END").await.unwrap();
    let error = publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("r25 tail failure"));
    assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_lease_target::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(
        pin_provider_usage::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(pin_job::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 0);
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(snapshot.batch.terminal_result.is_none());
    assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_none()));
}

#[tokio::test]
async fn r25_store_frozen_capture_does_not_recompute_current_config() {
    let db = setup().await;
    let (request, batch, capture, guard) = prepared(&db, &config(), vec![]).await;
    let changed = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    assert!(changed.providers.is_empty());
    publish(&db, &changed, request, batch, &capture, guard)
        .await
        .unwrap();
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 2);
}

#[tokio::test]
async fn r25_store_manual_decompressed_retains_archive_owner_beside_output_automatic() {
    let db = setup().await;
    let config = config();
    let tags = vec![
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("ipfs-s3:content", "decompressed"),
    ];
    let (request, batch, capture, guard) = prepared(&db, &config, tags).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 3);
    assert!(
        leases
            .iter()
            .any(|l| l.source == "manual" && l.owner_object_id == "archive")
    );
    assert!(
        leases
            .iter()
            .filter(|l| l.source == "automatic")
            .all(|l| ["a", "b"].contains(&l.owner_object_id.as_str()))
    );
    assert_eq!(
        pin_lease_target::Entity::find().count(&db).await.unwrap(),
        3
    );
    let usage = pin_provider_usage::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (5, 1));
}

#[tokio::test]
async fn r25_store_mismatched_final_binding_cannot_bypass_durable_capture() {
    let db = setup().await;
    let config = config();
    let (mut request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    request.entries[0].cid = "not-the-prepared-cid".into();
    assert!(
        publish(&db, &config, request, batch, &capture, guard)
            .await
            .is_err()
    );
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
}

#[test]
fn r25_store_revision_only_historical_capture_requires_attention() {
    let error = publication::decode_zip_capture_json(r#"{"rule_revision":"old","options":{}}"#)
        .unwrap_err();
    assert!(error.to_string().contains("needs_attention"));
}

#[tokio::test]
async fn r25_store_durable_mpu_capture_cannot_replace_output_ownership_guard() {
    use ipfs_s3_gateway::{
        lifecycle::model::MultipartUploadTargetIdentity,
        store::entities::zip_batch,
        zip::batch::{FinalZipManifest, ManifestFile},
    };
    use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
    let db = setup().await;
    let (mut request, batch, _, guard) = prepared(&db, &config(), vec![]).await;
    let capture = intake_capture("batch");
    store::multipart::create_upload_with_zip_intent(
        &db,
        "batch",
        "archive",
        "bucket",
        "out/archive.zip",
        "none",
        None,
        None,
        None,
        None,
        &[],
        Some("out/"),
        true,
        Some(capture.source_decision()),
        Some(&capture),
    )
    .await
    .unwrap();
    let upload = store::multipart::get_upload(&db, "batch").await.unwrap();
    let mut row = zip_batch::Entity::find_by_id(&batch.batch_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    row.source = Set("mpu".into());
    row.token = Set(upload.upload_id.clone());
    row.input_identity = Set(upload.upload_id.clone());
    row.captured_options = Set(serde_json::json!({"zip_intent": capture}).to_string());
    row.update(&db).await.unwrap();
    let parts = vec![s3s::dto::CompletedPart {
        part_number: Some(1),
        ..Default::default()
    }];
    store::zip::BatchAdmission::capture_mpu_complete(
        &db,
        "principal",
        "bucket",
        "out/archive.zip",
        "batch",
        &parts,
    )
    .await
    .unwrap();
    store::zip::BatchAdmission::capture_prepared_archive(
        &db,
        "principal",
        "bucket",
        "out/archive.zip",
        "batch",
        &parts,
        "archive-cid",
    )
    .await
    .unwrap();
    request.archive.object.multipart = true;
    let manifest = FinalZipManifest {
        successful: request
            .entries
            .iter()
            .map(|o| ManifestFile {
                relative_path: o.key.trim_start_matches("out/").into(),
                object_key: o.key.clone(),
                cid: o.cid.clone(),
                size: o.logical_size,
            })
            .collect(),
        ..Default::default()
    };
    let request = publication::CapturedZipPublication {
        batch_id: batch.batch_id,
        source: Some(request.archive.object),
        source_tags: request.archive.tags,
        successes: request.entries,
        manifest,
        capture,
        authority: publication::ZipPublicationAuthority::Legacy(Box::new(
            publication::LegacyZipAuthority {
                standard_guard: None,
                upload_target: Some(MultipartUploadTargetIdentity {
                    upload_id: upload.upload_id.clone(),
                    bucket: upload.bucket,
                    key: upload.key,
                    initiated_at: upload.created_at,
                }),
                import_guard: None,
                result_rows: vec![],
                replay: Some(publication::ZipCompletedReplay {
                    owner: "principal".into(),
                    parts: parts.clone(),
                    response_xml: "<CompleteMultipartUploadResult/>".into(),
                    server_side_encryption: None,
                    pin_warning: None,
                }),
            },
        )),
        root_outcome: batch.root_outcome,
        terminal_result: serde_json::json!({"archive_cid": "archive-cid"}).to_string(),
    };
    let error = publication::publish_captured_zip(&db, request.clone())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("missing ZIP publication ownership guard"),
        "{error}"
    );
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert!(
        store::multipart::get_upload(&db, &upload.upload_id)
            .await
            .is_ok()
    );
    let mut guarded = request;
    let publication::ZipPublicationAuthority::Legacy(authority) = &mut guarded.authority else {
        unreachable!()
    };
    authority.standard_guard = Some(guard);
    publication::publish_captured_zip(&db, guarded)
        .await
        .unwrap();
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
    assert!(store::multipart::get_upload(&db, "batch").await.is_err());
    store::zip::BatchAdmission::replay_lookup(
        &db,
        "principal",
        "bucket",
        "out/archive.zip",
        "batch",
        &parts,
    )
    .await
    .unwrap();
    let sealed = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT response_xml FROM zip_mpu_replays WHERE batch_id='batch'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sealed.try_get::<String>("", "response_xml").unwrap(),
        "<CompleteMultipartUploadResult/>"
    );
}

fn intake_capture(id: &str) -> ipfs_s3_gateway::pinning::zip_policy::CapturedZipIntent {
    use ipfs_s3_gateway::{
        config::{DecompressZipConfig, OptionalPinControlMode},
        pinning::{
            decision::DecisionOrigin,
            zip_policy::{ZipCaptureContext, capture_zip_intent},
        },
        zip::options::parse_authenticated_zip_controls,
    };
    let parsed = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        false,
    )
    .unwrap()
    .unwrap();
    capture_zip_intent(
        &config(),
        &DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        },
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "out/archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &parsed,
        DecisionOrigin::new("principal", id),
    )
    .unwrap()
}

#[tokio::test]
async fn r25_mpu_create_capture_and_original_decision_share_transaction() {
    use sea_orm::TransactionTrait;
    let db = setup().await;
    let capture = intake_capture("upload");
    let admission = legacy_admission(&capture);
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    store::multipart::create_upload_with_zip_intent(
        &tx,
        "upload",
        "source",
        "bucket",
        "out/archive.zip",
        "none",
        None,
        None,
        None,
        None,
        &[],
        Some("out/"),
        true,
        Some(capture.source_decision()),
        Some(&capture),
    )
    .await
    .unwrap();
    let admitted = store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &admission)
        .await
        .unwrap();
    assert_eq!(admitted.fingerprint, admission.fingerprint);
    assert_eq!(admitted.input_identity, "upload");
    tx.rollback().await.unwrap();
    assert!(store::multipart::get_upload(&db, "upload").await.is_err());
    assert!(store::zip::read(&db, "upload").await.unwrap().is_none());
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    store::multipart::create_upload_with_zip_intent(
        &tx,
        "upload",
        "source",
        "bucket",
        "out/archive.zip",
        "none",
        None,
        None,
        None,
        None,
        &[],
        Some("out/"),
        true,
        Some(capture.source_decision()),
        Some(&capture),
    )
    .await
    .unwrap();
    store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &admission)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let upload = store::multipart::get_upload(&db, "upload").await.unwrap();
    assert_eq!(
        publication::decode_zip_capture_json(upload.zip_intent_json.as_deref().unwrap()).unwrap(),
        capture
    );
    assert_eq!(
        store::multipart::decision_from_upload(&upload)
            .unwrap()
            .as_ref(),
        Some(capture.source_decision())
    );
    let batch = store::zip::read(&db, "upload").await.unwrap().unwrap();
    assert_eq!(batch.captured_options, admission.captured_options);
    assert_eq!(batch.fingerprint, admission.fingerprint);
    assert_eq!(batch.input_identity, admission.input_identity);
    let replacement = legacy_admission(&intake_capture("upload"));
    assert_ne!(replacement.captured_options, admission.captured_options);
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let replay = store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &replacement)
        .await
        .unwrap();
    assert_eq!(replay.captured_options, admission.captured_options);
    let mut conflicting = replacement;
    conflicting.fingerprint = "different-legacy-fingerprint".into();
    assert!(
        store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &conflicting)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    store::multipart::create_upload(
        &db,
        "old",
        "old-source",
        "bucket",
        "old-key",
        "none",
        None,
        None,
        None,
        None,
        &[],
        None,
        false,
    )
    .await
    .unwrap();
    assert!(
        store::multipart::get_upload(&db, "old")
            .await
            .unwrap()
            .zip_intent_json
            .is_none()
    );
}

fn legacy_admission(
    capture: &ipfs_s3_gateway::pinning::zip_policy::CapturedZipIntent,
) -> store::zip::BatchAdmission {
    let id = &capture.origin().request_id;
    store::zip::BatchAdmission {
        id: id.clone(),
        owner: capture.origin().principal_id.clone(),
        source: "mpu".into(),
        token: id.clone(),
        fingerprint: format!("mpu:{id}:bucket:out/archive.zip:out/:false"),
        bucket: "bucket".into(),
        archive_key: "out/archive.zip".into(),
        input_identity: id.clone(),
        captured_options: serde_json::json!({
            "root_enabled": false, "target_prefix": "out/", "zip_intent": capture,
        })
        .to_string(),
    }
}

#[tokio::test]
async fn r25_legacy_admission_rejects_v2_pending_identity_without_writing() {
    use sea_orm::TransactionTrait;
    let db = setup().await;
    let original = legacy_admission(&intake_capture("upload"));
    for (fingerprint, input_identity) in [
        ("pending", "upload"),
        (original.fingerprint.as_str(), "pending"),
        (original.fingerprint.as_str(), "zip-v2-source-gen:1"),
    ] {
        let tx = db.begin().await.unwrap();
        store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
            .await
            .unwrap();
        let mut request = original.clone();
        request.fingerprint = fingerprint.into();
        request.input_identity = input_identity.into();
        assert!(
            store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &request)
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        assert!(store::zip::read(&db, "upload").await.unwrap().is_none());
    }
}

#[tokio::test]
async fn r25_legacy_batch_admission_tail_fault_rolls_back_upload_and_capture() {
    use sea_orm::TransactionTrait;
    let db = setup().await;
    let capture = intake_capture("upload");
    db.execute_unprepared("CREATE TRIGGER r25_admission_tail BEFORE INSERT ON zip_batches BEGIN SELECT RAISE(ABORT,'r25 admission tail'); END")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    store::multipart::create_upload_with_zip_intent(
        &tx,
        "upload",
        "source",
        "bucket",
        "out/archive.zip",
        "none",
        None,
        None,
        None,
        None,
        &[],
        Some("out/"),
        true,
        Some(capture.source_decision()),
        Some(&capture),
    )
    .await
    .unwrap();
    let error =
        store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &legacy_admission(&capture))
            .await
            .unwrap_err();
    assert!(error.to_string().contains("r25 admission tail"));
    tx.rollback().await.unwrap();
    assert!(store::multipart::get_upload(&db, "upload").await.is_err());
    assert!(store::zip::read(&db, "upload").await.unwrap().is_none());
}

#[tokio::test]
async fn r25_import_intake_freezes_capture_and_idempotency_never_overwrites_it() {
    use ipfs_s3_gateway::{
        import::ImportSource,
        store::import::{
            jobs::{NewImportJob, SubmitImportOutcome},
            ownership,
        },
    };
    let db = setup().await;
    let capture = intake_capture("job");
    let request = NewImportJob {
        id: "job".into(),
        bucket: "bucket".into(),
        key: "out/archive.zip".into(),
        source: ImportSource::Cid("QmYwAPJzv5CZsnAzt8auVZRnGiRAzMqCNX4GKUhQoMqR3T".into()),
        request_fingerprint: "same-authenticated-request".into(),
        client_token: Some("token".into()),
        object_content_type: None,
        metadata: Default::default(),
        tags: vec![],
        decompress_prefix: Some("out/".into()),
    };
    let created = match ownership::submit_captured_zip(
        &db,
        request.clone(),
        capture.clone(),
        chrono::Utc::now(),
    )
    .await
    .unwrap()
    {
        SubmitImportOutcome::Created(job) => job,
        _ => panic!("expected fresh job"),
    };
    assert_eq!(
        publication::decode_zip_capture_json(created.zip_intent_json.as_deref().unwrap()).unwrap(),
        capture
    );
    let replacement = intake_capture("job");
    assert_ne!(capture, replacement);
    let replayed =
        match ownership::submit_captured_zip(&db, request, replacement, chrono::Utc::now())
            .await
            .unwrap()
        {
            SubmitImportOutcome::Replayed(job) => job,
            _ => panic!("expected replay"),
        };
    assert_eq!(replayed.zip_intent_json, created.zip_intent_json);
    assert_eq!(replayed.pin_decision_json, created.pin_decision_json);
}

#[tokio::test]
async fn r25_legacy_url_input_proof_is_complete_immutable_and_claim_fenced() {
    use ipfs_s3_gateway::{
        import::ImportSource,
        store::import::{jobs, ownership},
    };
    use sea_orm::TransactionTrait;
    let db = setup().await;
    let capture = intake_capture("url-job");
    ownership::submit_captured_zip(
        &db,
        jobs::NewImportJob {
            id: "url-job".into(),
            bucket: "bucket".into(),
            key: "out/archive.zip".into(),
            source: ImportSource::Url("https://example.com/archive.zip".parse().unwrap()),
            request_fingerprint: "authenticated-url-request".into(),
            client_token: None,
            object_content_type: None,
            metadata: Default::default(),
            tags: vec![],
            decompress_prefix: Some("out/".into()),
        },
        capture.clone(),
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    let mut admission = legacy_admission(&capture);
    admission.source = "import".into();
    admission.fingerprint = "import:url-job:bucket:out/archive.zip:out/:false".into();
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    store::zip::BatchAdmission::admit_legacy_in_transaction(&tx, &admission)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let now = chrono::Utc::now();
    let claim = jobs::claim_due(&db, "worker", now, now + chrono::Duration::minutes(5), 1)
        .await
        .unwrap()
        .remove(0)
        .claim;
    assert!(
        publication::read_legacy_import_input(&db, "url-job")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        publication::bind_legacy_import_input(&db, &claim, "CID-is-not-a-digest", "archive-cid", 5)
            .await
            .is_err()
    );
    db.execute_unprepared("CREATE TRIGGER r25_input_tail BEFORE UPDATE OF legacy_input_proof_json ON zip_batches BEGIN SELECT RAISE(ABORT,'r25 input tail'); END").await.unwrap();
    let sha = "a".repeat(64);
    assert!(
        publication::bind_legacy_import_input(&db, &claim, &sha, "archive-cid", 5)
            .await
            .unwrap_err()
            .to_string()
            .contains("r25 input tail")
    );
    assert!(
        publication::read_legacy_import_input(&db, "url-job")
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DROP TRIGGER r25_input_tail")
        .await
        .unwrap();
    let proof = publication::bind_legacy_import_input(&db, &claim, &sha, "archive-cid", 5)
        .await
        .unwrap();
    assert_eq!(
        (proof.sha256(), proof.cid(), proof.size()),
        (sha.as_str(), "archive-cid", 5)
    );
    assert_eq!(
        publication::read_legacy_import_input(&db, "url-job")
            .await
            .unwrap(),
        Some(proof.clone())
    );
    assert!(
        publication::bind_legacy_import_input(&db, &claim, &"b".repeat(64), "archive-cid", 5)
            .await
            .is_err()
    );
    db.execute_unprepared(
        "UPDATE import_jobs SET locked_until='2000-01-01 00:00:00+00:00' WHERE id='url-job'",
    )
    .await
    .unwrap();
    let next = jobs::claim_due(
        &db,
        "replacement",
        now,
        now + chrono::Duration::minutes(5),
        1,
    )
    .await
    .unwrap()
    .remove(0)
    .claim;
    assert!(
        publication::bind_legacy_import_input(&db, &claim, &sha, "archive-cid", 5)
            .await
            .is_err()
    );
    assert_eq!(
        publication::bind_legacy_import_input(&db, &next, &sha, "archive-cid", 5)
            .await
            .unwrap(),
        proof
    );
    let batch = store::zip::read(&db, "url-job").await.unwrap().unwrap();
    assert_eq!(batch.input_identity, admission.input_identity);
    assert_eq!(batch.fingerprint, admission.fingerprint);
    assert_eq!(batch.captured_options, admission.captured_options);
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
}
