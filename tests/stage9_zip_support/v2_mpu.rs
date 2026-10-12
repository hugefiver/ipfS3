use super::stage9_zip_support::{config, setup};
use ipfs_s3_gateway::store::{
    entities::{object, pin_lease},
    import::ownership,
    pinning::publication::{
        self, CapturedZipPublication, ZipExecutionSettlement, ZipPublicationAuthority,
    },
    zip::{self as storage, BatchAdmission, RootOutcome},
};
use ipfs_s3_gateway::{
    config::{DecompressZipConfig, OptionalPinControlMode},
    pinning::{
        decision::DecisionOrigin,
        zip_policy::{CapturedZipIntent, ZipCaptureContext, capture_zip_intent},
    },
    store::{
        multipart::{
            self,
            v2_zip::{self, Intake},
        },
        zip::execution,
    },
    zip::options::{self, parse_authenticated_zip_controls},
};
use sea_orm::ConnectionTrait;
use sea_orm::{
    DatabaseBackend, DatabaseConnection, EntityTrait, PaginatorTrait, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

fn intake(id: &str, auto: bool, root: bool) -> (Intake, CapturedZipIntent) {
    intake_with_source(id, auto, root, false)
}

pub(super) fn intake_with_source(
    id: &str,
    auto: bool,
    root: bool,
    source: bool,
) -> (Intake, CapturedZipIntent) {
    let mut headers = http::HeaderMap::new();
    headers.insert(options::ZIP_CONTRACT, "v2".parse().unwrap());
    headers.insert(options::ZIP_TOKEN, "mpu-frozen-token".parse().unwrap());
    headers.insert(options::PUBLISH_SOURCE, source.to_string().parse().unwrap());
    let parsed = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        root,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        &config(),
        &DecompressZipConfig {
            auto_pin_outputs: Some(auto),
            unixfs_directory_root: root,
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
    .unwrap();
    (
        Intake {
            upload_id: id.into(),
            object_id: format!("object-{id}"),
            owner: "principal".into(),
            token: "mpu-frozen-token".into(),
            bucket: "bucket".into(),
            key: "out/archive.zip".into(),
            target_prefix: "out/".into(),
            request_contract: format!(
                "authenticated normalized MPU source={source}/extracted=true contract"
            ),
            options: capture.v2_options().unwrap(),
            captured_config: "{}".into(),
            rule_revision: "original-revision".into(),
            content_type: None,
            metadata: None,
            tags: vec![],
        },
        capture,
    )
}

async fn seed_historical_bare(db: &DatabaseConnection, request: &Intake) {
    // A pre-upgrade first-intake row, not a production NULL-admission fallback.
    let options = serde_json::to_string(&request.options).unwrap();
    let fingerprint = hex::encode(Sha256::digest(request.request_contract.as_bytes()));
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, &request.bucket)
        .await
        .unwrap();
    execution::admit_in_transaction(
        &tx,
        &execution::Admission {
            id: request.upload_id.clone(),
            owner: request.owner.clone(),
            source: "mpu".into(),
            token: request.token.clone(),
            request_fingerprint: fingerprint.clone(),
            request_contract: request.request_contract.clone(),
            bucket: request.bucket.clone(),
            source_key: request.key.clone(),
            captured_options: options.clone(),
        },
    )
    .await
    .unwrap();
    multipart::create_upload_with_decision(
        &tx,
        &request.upload_id,
        &request.object_id,
        &request.bucket,
        &request.key,
        "none",
        None,
        None,
        None,
        None,
        &[],
        Some(&request.target_prefix),
        true,
        None,
    )
    .await
    .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO zip_v2_mpu_intakes (original_upload_id,active_upload_id,execution_id,owner,token,bucket,archive_key,target_prefix,request_fingerprint,captured_options,captured_config,rule_revision,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        vec![request.upload_id.clone().into(),request.upload_id.clone().into(),request.upload_id.clone().into(),request.owner.clone().into(),request.token.clone().into(),request.bucket.clone().into(),request.key.clone().into(),request.target_prefix.clone().into(),fingerprint.into(),options.into(),request.captured_config.clone().into(),request.rule_revision.clone().into(),now.into()]
    )).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn r25_v2_mpu_envelope_and_historical_bare_preserve_empty_receipt_not_null_admission() {
    for historical in [false, true] {
        let db = setup().await;
        let (request, capture) = intake("mpu-empty", true, false);
        if historical {
            seed_historical_bare(&db, &request).await;
        } else {
            v2_zip::create_or_replay_with_zip_intent(&db, &request, &capture)
                .await
                .unwrap();
        }
        let record = v2_zip::read_by_upload(&db, &request.upload_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.options().unwrap(), request.options);
        if historical {
            assert_eq!(record.zip_intent().unwrap(), None);
            let (later, later_capture) = intake("new-request", true, false);
            assert!(
                v2_zip::create_or_replay_with_zip_intent(&db, &later, &later_capture)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("needs_attention")
            );
            assert!(
                multipart::get_upload(&db, &request.upload_id)
                    .await
                    .unwrap()
                    .zip_intent_json
                    .is_none()
            );
        }
        let parts = [s3s::dto::CompletedPart {
            part_number: Some(1),
            e_tag: Some(s3s::dto::ETag::Strong("part-cid".into())),
            ..Default::default()
        }];
        multipart::upsert_part(&db, &request.upload_id, 1, "part-cid", 5, "part-cid")
            .await
            .unwrap();
        assert!(matches!(
            v2_zip::capture_complete(
                &db,
                &record,
                &request.owner,
                &request.bucket,
                &request.key,
                &parts
            )
            .await
            .unwrap(),
            v2_zip::CompleteState::Pending
        ));
        let contract = v2_zip::complete_request_contract(&parts).unwrap();
        let claim = execution::claim(&db, &request.upload_id, "mpu-store-test", 60)
            .await
            .unwrap()
            .unwrap();
        let sha = "c".repeat(64);
        execution::bind_clean_input(&db, &claim, &sha, "archive-cid", 5)
            .await
            .unwrap();
        let tx = db.begin().await.unwrap();
        ownership::lock_bucket_for_ownership(&tx, "bucket")
            .await
            .unwrap();
        execution::admit_manifest_in_transaction(&tx, &claim, &[], &Default::default())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        storage::admit(
            &db,
            &BatchAdmission {
                id: request.upload_id.clone(),
                owner: request.owner.clone(),
                source: "mpu".into(),
                token: request.token.clone(),
                fingerprint: "pending".into(),
                bucket: request.bucket.clone(),
                archive_key: request.key.clone(),
                input_identity: "pending".into(),
                captured_options: record.captured_options.clone(),
            },
        )
        .await
        .unwrap();
        storage::prepare_manifest(&db, &request.upload_id, &[])
            .await
            .unwrap();
        let terminal = serde_json::json!({"input_sha256": sha, "status": "empty", "published_count": 0, "failed_count": 0}).to_string();
        if historical {
            // Settle the old already-measured empty execution using original
            // native primitives; no objects, leases or new intent are authorized.
            let tx = db.begin().await.unwrap();
            ownership::lock_bucket_for_ownership(&tx, "bucket")
                .await
                .unwrap();
            storage::publish(
                &tx,
                &request.upload_id,
                &[],
                false,
                &terminal,
                RootOutcome::Disabled,
            )
            .await
            .unwrap();
            execution::complete_in_transaction(&tx, &claim, &terminal)
                .await
                .unwrap();
            v2_zip::finalize_complete_in_transaction(&tx, &claim, &contract, None)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        } else {
            publication::publish_captured_zip(
                &db,
                CapturedZipPublication {
                    batch_id: request.upload_id.clone(),
                    source: None,
                    source_tags: vec![],
                    successes: vec![],
                    manifest: Default::default(),
                    capture,
                    authority: ZipPublicationAuthority::Execution {
                        claim,
                        source_guard: None,
                        settlement: ZipExecutionSettlement::Mpu { contract },
                    },
                    root_outcome: RootOutcome::Disabled,
                    terminal_result: terminal,
                },
            )
            .await
            .unwrap();
        }
        let v2_zip::CompleteState::Completed(receipt) = v2_zip::capture_complete(
            &db,
            &record,
            &request.owner,
            &request.bucket,
            &request.key,
            &parts,
        )
        .await
        .unwrap() else {
            panic!("expected durable empty failure receipt");
        };
        assert!(
            receipt
                .xml
                .contains("<SourcePublished>false</SourcePublished>")
        );
        assert!(receipt.xml.contains("<PublishedCount>0</PublishedCount>"));
        assert_eq!(receipt.headers["x-ipfs-s3-zip-batch-status"], "empty");
        assert!(receipt.headers.contains_key("date"));
        let v2_zip::CompleteState::Completed(replay) = v2_zip::capture_complete(
            &db,
            &record,
            &request.owner,
            &request.bucket,
            &request.key,
            &parts,
        )
        .await
        .unwrap() else {
            panic!("expected same-part replay");
        };
        assert_eq!(receipt, replay);
        assert!(
            multipart::get_upload(&db, &request.upload_id)
                .await
                .is_err()
        );
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    }
    let db = setup().await;
    let (request, _) = intake("fresh-no-capture", true, false);
    assert!(
        v2_zip::create_or_replay(&db, &request)
            .await
            .unwrap_err()
            .to_string()
            .contains("needs_attention")
    );
    assert!(
        execution::read(&db, &request.upload_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn r25_v2_mpu_create_capture_is_atomic_and_token_replay_keeps_first_intent() {
    let db = setup().await;
    let (first, capture) = intake("mpu-first", true, false);
    db.execute_unprepared("CREATE TRIGGER fail_v2_create_tail BEFORE INSERT ON zip_v2_mpu_intakes BEGIN SELECT RAISE(FAIL, 'v2 create tail'); END;").await.unwrap();
    assert!(matches!(
        v2_zip::create_or_replay_with_zip_intent(&db, &first, &capture).await,
        Err(ipfs_s3_gateway::error::AppError::Database(_))
    ));
    assert!(
        execution::read(&db, &first.upload_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(multipart::get_upload(&db, &first.upload_id).await.is_err());
    assert!(
        v2_zip::read_by_upload(&db, &first.upload_id)
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("DROP TRIGGER fail_v2_create_tail")
        .await
        .unwrap();
    assert_eq!(
        v2_zip::create_or_replay_with_zip_intent(&db, &first, &capture)
            .await
            .unwrap(),
        first.upload_id
    );
    let upload = multipart::get_upload(&db, &first.upload_id).await.unwrap();
    assert_eq!(
        upload.zip_intent_json.as_deref(),
        Some(
            ipfs_s3_gateway::store::pinning::publication::serialize_zip_capture(&capture)
                .unwrap()
                .as_str()
        )
    );
    assert_eq!(
        multipart::decision_from_upload(&upload).unwrap().as_ref(),
        Some(capture.source_decision())
    );
    let record = v2_zip::read_by_upload(&db, &first.upload_id)
        .await
        .unwrap()
        .unwrap();
    let execution = execution::read(&db, &first.upload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.captured_options, execution.captured_options);
    let envelope: serde_json::Value = serde_json::from_str(&record.captured_options).unwrap();
    assert_eq!(
        envelope["options"],
        serde_json::to_value(&first.options).unwrap()
    );
    assert_eq!(envelope["target_prefix"], "out/");
    assert_eq!(envelope["rule_revision"], first.rule_revision);
    assert_eq!(
        envelope["zip_intent"],
        serde_json::to_value(&capture).unwrap()
    );
    let (mut later, later_capture) = intake("mpu-later", false, true);
    later.captured_config = "{\"changed\":true}".into();
    later.rule_revision = "later-revision".into();
    assert_eq!(
        v2_zip::create_or_replay_with_zip_intent(&db, &later, &later_capture)
            .await
            .unwrap_or_else(|error| panic!(
                "replay {error:?}: original {:?}, later {:?}",
                first.options, later.options
            )),
        first.upload_id
    );
    assert!(
        execution::read(&db, &later.upload_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(multipart::get_upload(&db, &later.upload_id).await.is_err());
    assert_eq!(
        v2_zip::read_by_upload(&db, &first.upload_id)
            .await
            .unwrap()
            .unwrap()
            .captured_options,
        record.captured_options
    );
    assert_eq!(
        v2_zip::create_or_replay(&db, &later).await.unwrap(),
        first.upload_id
    );
    assert_eq!(record.options().unwrap(), first.options);
    assert_eq!(record.zip_intent().unwrap(), Some(capture));
    later
        .request_contract
        .push_str(" different signed semantics");
    assert!(
        v2_zip::create_or_replay_with_zip_intent(&db, &later, &later_capture)
            .await
            .is_err()
    );
}
