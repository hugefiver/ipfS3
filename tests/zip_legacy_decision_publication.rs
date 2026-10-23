#[allow(dead_code)]
mod stage9_zip_support;

use ipfs_s3_gateway::{
    config::{DecompressZipConfig, OptionalPinControlMode},
    lifecycle::model::MultipartUploadTargetIdentity,
    pinning::{
        decision::DecisionOrigin,
        tags::{ObjectTag, PinControl},
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    store::{
        self,
        entities::{object_version, pin_lease, zip_batch},
        pinning::publication::{
            self, CapturedZipPublication, LegacyZipAuthority, ZipPublicationAuthority,
        },
    },
    zip::{
        batch::{FinalZipManifest, ManifestFile},
        options::parse_authenticated_zip_controls,
    },
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseBackend, EntityTrait, IntoActiveModel,
    PaginatorTrait, Set, Statement,
};
use stage9_zip_support::{config, prepared, setup};

#[tokio::test]
async fn mpu_final_publication_rereads_decision_and_accepts_single_authority() {
    let db = setup().await;
    let config = config();
    let (mut objects, batch, _, guard) = prepared(&db, &config, vec![]).await;
    let controls = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        false,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        &config,
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
        &controls,
        DecisionOrigin::new("principal", "batch"),
    )
    .unwrap();
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
    assert_eq!(
        store::multipart::decision_from_upload(&upload)
            .unwrap()
            .as_ref(),
        Some(capture.source_decision())
    );
    let mut row = zip_batch::Entity::find_by_id(&batch.batch_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    row.source = Set("mpu".into());
    row.token = Set(upload.upload_id.clone());
    row.input_identity = Set(upload.upload_id.clone());
    row.captured_options = Set(serde_json::json!({"zip_intent":capture}).to_string());
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
    objects.archive.object.multipart = true;
    let manifest = FinalZipManifest {
        successful: objects
            .entries
            .iter()
            .map(|object| ManifestFile {
                relative_path: object.key.trim_start_matches("out/").into(),
                object_key: object.key.clone(),
                cid: object.cid.clone(),
                size: object.logical_size,
            })
            .collect(),
        ..Default::default()
    };
    let request = CapturedZipPublication {
        batch_id: batch.batch_id,
        source: Some(objects.archive.object),
        source_tags: objects.archive.tags,
        successes: objects.entries,
        manifest,
        capture: capture.clone(),
        authority: ZipPublicationAuthority::Legacy(Box::new(LegacyZipAuthority {
            standard_guard: Some(guard),
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
                parts,
                response_xml: "<CompleteMultipartUploadResult/>".into(),
                server_side_encryption: None,
                pin_warning: None,
            }),
        })),
        root_outcome: batch.root_outcome,
        terminal_result: serde_json::json!({"archive_cid":"archive-cid"}).to_string(),
    };
    let mut conflicting = capture.source_decision().clone();
    assert_eq!(conflicting.control, PinControl::Absent);
    conflicting
        .replay_policy(request.source_tags.clone())
        .unwrap();
    conflicting.control = PinControl::Cancel;
    conflicting.validate_snapshot().unwrap();
    conflicting
        .replay_policy(vec![ObjectTag::new("ipfs-s3:pin", "false")])
        .unwrap();
    assert_eq!(conflicting.origin, *capture.origin());
    assert_ne!(conflicting.control, capture.source_decision().control);
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE multipart_uploads SET pin_decision_json=? WHERE upload_id=?",
        [
            serde_json::to_string(&conflicting).unwrap().into(),
            "batch".into(),
        ],
    ))
    .await
    .unwrap();
    assert!(matches!(
        publication::publish_captured_zip(&db, request.clone()).await,
        Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
    ));
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert!(store::multipart::get_upload(&db, "batch").await.is_ok());
    assert_eq!(
        store::zip::read(&db, "batch").await.unwrap().unwrap().state,
        "open"
    );

    // NULL is valid here because the persisted ZIP capture itself owns authority.
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE multipart_uploads SET pin_decision_json=NULL WHERE upload_id=?",
        ["batch".into()],
    ))
    .await
    .unwrap();
    publication::publish_captured_zip(&db, request)
        .await
        .unwrap();
    assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 3);
    assert!(store::multipart::get_upload(&db, "batch").await.is_err());
}
