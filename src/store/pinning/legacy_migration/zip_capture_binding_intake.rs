use super::{BUCKET, CID, Intake};
use crate::{
    config::Config,
    import::ImportSource,
    pinning::{
        config::ValidatedPinningConfig,
        decision::DecisionOrigin,
        policy::LeaseSource,
        zip_policy::{
            CapturedZipIntent, ZipBoundOutput, ZipCaptureContext, capture_zip_intent,
            select_zip_intents,
        },
    },
    store::{
        self,
        import::{jobs::NewImportJob, ownership},
    },
    zip::{
        batch::{FinalZipManifest, ManifestFile},
        options::parse_authenticated_zip_controls,
    },
};
use sea_orm::{DatabaseConnection, TransactionTrait};

pub(super) async fn persist(
    db: &DatabaseConnection,
    config: &Config,
    kind: Intake,
    id: &str,
    source_key: &str,
) -> CapturedZipIntent {
    let validated = ValidatedPinningConfig::from_config(config, |_| None).unwrap();
    let parsed = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        config.decompress_zip.unixfs_directory_root,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        &validated,
        &config.decompress_zip,
        config.pinning_control.unavailable,
        ZipCaptureContext {
            bucket: BUCKET,
            source_key,
            target_prefix: "out/",
            tags: &[],
        },
        &parsed,
        DecisionOrigin::new("tenant", id),
    )
    .unwrap();
    assert!(capture.source_decision().effective_intents.is_empty());
    assert!(capture.controls().targets.extracted);
    let output = ZipBoundOutput {
        object_id: "output-owner".into(),
        bucket: BUCKET.into(),
        key: "out/file.txt".into(),
        version_id: "null".into(),
        cid: CID.into(),
        logical_size: 7,
    };
    let manifest = FinalZipManifest {
        successful: vec![ManifestFile {
            relative_path: "file.txt".into(),
            object_key: output.key.clone(),
            cid: output.cid.clone(),
            size: output.logical_size,
        }],
        ..Default::default()
    };
    let source = ZipBoundOutput {
        object_id: "source-owner".into(),
        bucket: BUCKET.into(),
        key: source_key.into(),
        version_id: "null".into(),
        cid: CID.into(),
        logical_size: 7,
    };
    let selected = select_zip_intents(&capture, &manifest, Some(&source), &[output]).unwrap();
    assert_eq!(selected.leases.len(), 1);
    assert_eq!(selected.leases[0].intent.source, LeaseSource::Automatic);
    assert_eq!(selected.leases[0].intent.providers, ["old"]);
    match kind {
        Intake::Mpu => {
            let transaction = db.begin().await.unwrap();
            ownership::lock_bucket_for_ownership(&transaction, BUCKET)
                .await
                .unwrap();
            store::multipart::create_upload_with_zip_intent(
                &transaction,
                id,
                &format!("object-{id}"),
                BUCKET,
                source_key,
                "none",
                None,
                None,
                None,
                None,
                &[],
                Some("out/"),
                capture.controls().result_enabled,
                Some(capture.source_decision()),
                Some(&capture),
            )
            .await
            .unwrap();
            transaction.commit().await.unwrap();
        }
        Intake::Import => {
            ownership::submit_captured_zip(
                db,
                NewImportJob {
                    id: id.into(),
                    bucket: BUCKET.into(),
                    key: source_key.into(),
                    source: ImportSource::Cid(CID.into()),
                    request_fingerprint: "original-intake-contract".into(),
                    client_token: Some(id.into()),
                    object_content_type: None,
                    metadata: Default::default(),
                    tags: vec![],
                    decompress_prefix: Some("out/".into()),
                },
                capture.clone(),
                store::database_clock::database_now(db).await.unwrap(),
            )
            .await
            .unwrap();
        }
    }
    capture
}
