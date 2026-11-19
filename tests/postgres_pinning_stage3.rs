//! Publication transactions on SQLite and real PostgreSQL 17/18. Each PostgreSQL
//! test requires an explicit URL and creates/drops its own UUID-named schema.
#[path = "support/pin_rows.rs"]
mod pin_rows;
#[path = "support/postgres_pinning_stage3.rs"]
mod postgres;
#[allow(dead_code)]
mod stage9_zip_support;

use std::collections::HashMap;

use chrono::Utc;
use ipfs_s3_gateway::{
    config::OptionalPinControlMode,
    error::AppError,
    import::ImportSource,
    lifecycle::model::MultipartUploadTargetIdentity,
    pinning::{
        config::ValidatedPinningConfig,
        decision::{DecisionEffect, DecisionOrigin, ExtensionDecision},
        policy::{PinPolicyEvaluator, PublicationContext, PublicationPolicy},
        tags::ObjectTag,
    },
    store::{
        self,
        entities::{import_destination, import_job, object, object_version},
        import::{
            jobs,
            ownership::{self, ExpectedImportTarget, ImportPublicationGuard},
        },
        multipart::CommitCompletedUploadError,
        pinning::{
            decision::read_for_version,
            publication::{
                self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
            },
        },
    },
};
use postgres::isolated;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, Statement,
};
use stage9_zip_support::{ARCHIVE_CID as CID, config, prepared, publish_captured, setup};

const BUCKET: &str = "bucket";
const ORDINARY_KEY: &str = "out/ordinary";
const MPU_KEY: &str = "out/mpu";
const IMPORT_KEY: &str = "out/import";

fn captured(
    config: &ValidatedPinningConfig,
    key: &str,
    admission: &str,
) -> (PublicationPolicy, ExtensionDecision) {
    let tags = [
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("private", "do-not-leak"),
    ];
    let (policy, decision) = PinPolicyEvaluator::with_mode(config, OptionalPinControlMode::Warn)
        .evaluate_publication_decision(
            PublicationContext {
                bucket: BUCKET,
                key,
                tags: &tags,
                is_decompress_zip: false,
            },
            DecisionOrigin::new("stage3-test", admission),
        )
        .unwrap();
    assert_eq!(decision.effect, DecisionEffect::Accepted);
    (policy, decision)
}

fn request(id: &str, key: &str, policy: PublicationPolicy) -> PublicationRequest {
    let object = PublicationObject::from_put(
        id.into(),
        BUCKET,
        key,
        CID.into(),
        1,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    PublicationRequest {
        tags: policy.tags.clone(),
        object_target: PinTargetSpec {
            cid: object.cid.clone(),
            logical_size: object.logical_size,
        },
        object,
        policy,
    }
}

fn captured_args<'a>(
    decision: &'a ExtensionDecision,
    config: &'a ValidatedPinningConfig,
) -> DecidedPublish<'a> {
    DecidedPublish {
        decision,
        config,
        mode: OptionalPinControlMode::Warn,
        limits: &config.provider_limits,
    }
}

async fn assert_no_published_objects(db: &DatabaseConnection) {
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(pin_rows::count(db, "pin_refs").await, 0);
    assert_eq!(pin_rows::count(db, "pin_resources").await, 0);
}

fn assert_database_fault(error: AppError, marker: &str) {
    assert!(
        matches!(&error, AppError::Database(message) if message.contains(marker)),
        "expected injected database fault {marker}, got {error:?}"
    );
}

async fn assert_object_and_zip_rollback(db: &DatabaseConnection) {
    let (reject, restore) = match db.get_database_backend() {
        DatabaseBackend::Postgres => (
            "CREATE FUNCTION stage3_reject_ref() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'stage3-pin-ref'; END $$;
             CREATE TRIGGER stage3_reject_ref BEFORE INSERT ON pin_refs
             FOR EACH ROW EXECUTE FUNCTION stage3_reject_ref()",
            "DROP TRIGGER stage3_reject_ref ON pin_refs",
        ),
        _ => (
            "CREATE TRIGGER stage3_reject_ref BEFORE INSERT ON pin_refs
             BEGIN SELECT RAISE(ABORT, 'stage3-pin-ref'); END",
            "DROP TRIGGER stage3_reject_ref",
        ),
    };
    db.execute_unprepared(reject).await.unwrap();
    let config = config();
    let (policy, decision) = captured(&config, ORDINARY_KEY, "ordinary-object");
    let ordinary = request("ordinary-object", ORDINARY_KEY, policy);
    let error = publication::publish_decided_object(
        db,
        ordinary.clone(),
        captured_args(&decision, &config),
    )
    .await
    .unwrap_err();
    assert_database_fault(error, "stage3-pin-ref");
    assert_no_published_objects(db).await;

    let (objects, batch, capture, guard) = prepared(db, &config, vec![]).await;
    let error = publish_captured(
        db,
        &config,
        objects.clone(),
        batch.clone(),
        &capture,
        guard.clone(),
    )
    .await
    .unwrap_err();
    assert_database_fault(error, "stage3-pin-ref");
    assert_no_published_objects(db).await;
    let snapshot = store::zip::snapshot(db, &batch.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "open");
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );

    db.execute_unprepared(restore).await.unwrap();
    let results = publish_captured(db, &config, objects, batch, &capture, guard)
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    let refs = pin_rows::refs(db).await;
    assert_eq!(
        refs.iter()
            .map(|r| r.owner_object_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    let resources = pin_rows::resources(db).await;
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].cid, stage9_zip_support::OUTPUT_CID);

    let published =
        publication::publish_decided_object(db, ordinary, captured_args(&decision, &config))
            .await
            .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&published.object_id))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_for_version(db, &version.id).await.unwrap(),
        Some(decision)
    );
    assert!(pin_rows::refs(db).await.iter().any(|r| {
        r.owner_object_id == published.object_id && r.cid == CID && r.source == "manual"
    }));
}

async fn assert_mpu_rollback(db: &DatabaseConnection) {
    let config = config();
    let (policy, decision) = captured(&config, MPU_KEY, "upload-stage3");
    store::multipart::create_upload_with_decision(
        db,
        "upload-stage3",
        "multipart-object",
        BUCKET,
        MPU_KEY,
        "none",
        None,
        None,
        None,
        None,
        &policy.tags,
        None,
        false,
        Some(&decision),
    )
    .await
    .unwrap();
    let saved = store::multipart::get_upload(db, "upload-stage3")
        .await
        .unwrap();
    assert_eq!(
        store::multipart::decision_from_upload(&saved).unwrap(),
        Some(decision.clone())
    );

    let reject = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            let kind: String = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
                "SELECT pg_typeof(pin_decision_json)::text AS kind FROM multipart_uploads WHERE upload_id='upload-stage3'"))
                .await.unwrap().unwrap().try_get("", "kind").unwrap();
            assert_eq!(kind, "jsonb");
            "CREATE FUNCTION stage3_reject_mpu() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'stage3-upload-delete'; END $$;
             CREATE TRIGGER stage3_reject_mpu BEFORE DELETE ON multipart_uploads
             FOR EACH ROW EXECUTE FUNCTION stage3_reject_mpu()"
        }
        _ => {
            "CREATE TRIGGER stage3_reject_mpu BEFORE DELETE ON multipart_uploads
              BEGIN SELECT RAISE(ABORT, 'stage3-upload-delete'); END"
        }
    };
    db.execute_unprepared(reject).await.unwrap();
    let target = MultipartUploadTargetIdentity {
        bucket: saved.bucket.clone(),
        key: saved.key.clone(),
        upload_id: saved.upload_id.clone(),
        initiated_at: saved.created_at,
    };
    let mut completion = request("completed-object", MPU_KEY, policy);
    completion.object.multipart = true;
    let error = publication::publish_decided_completed_upload(
        db,
        &target,
        completion,
        None,
        captured_args(&decision, &config),
    )
    .await
    .unwrap_err();
    let CommitCompletedUploadError::RolledBack { source, .. } = error else {
        panic!("expected known completion rollback, got {error:?}");
    };
    assert_database_fault(source, "stage3-upload-delete");
    assert_no_published_objects(db).await;
    assert_eq!(
        store::multipart::get_upload(db, &saved.upload_id)
            .await
            .unwrap(),
        saved
    );
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 or 18 test container"]
async fn pg17_object_and_zip_pin_ref_failure_roll_back_publication() {
    isolated(|db| async move {
        store::bucket::create(&db, BUCKET, None).await.unwrap();
        assert_object_and_zip_rollback(&db).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 or 18 test container"]
async fn pg17_mpu_jsonb_roundtrip_and_failed_completion_preserves_upload() {
    isolated(|db| async move {
        store::bucket::create(&db, BUCKET, None).await.unwrap();
        assert_mpu_rollback(&db).await;
    })
    .await;
}

async fn assert_import_rollback(db: &DatabaseConnection) {
    let config = config();
    let (policy, decision) = captured(&config, IMPORT_KEY, "job-stage3");
    let submitted = ownership::submit_decided(
        db,
        jobs::NewImportJob {
            id: "job-stage3".into(),
            bucket: BUCKET.into(),
            key: IMPORT_KEY.into(),
            source: ImportSource::Cid(CID.into()),
            request_fingerprint: "fingerprint".into(),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags: policy.tags.clone(),
            decompress_prefix: None,
        },
        decision.clone(),
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(matches!(submitted, jobs::SubmitImportOutcome::Created(_)));
    let saved = import_job::Entity::find_by_id("job-stage3")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let text = saved.pin_decision_json.as_deref().unwrap();
    assert_eq!(
        serde_json::from_str::<ExtensionDecision>(text).unwrap(),
        decision
    );
    assert!(!text.contains("do-not-leak"));
    let now = Utc::now();
    let claimed = jobs::claim_due(
        db,
        "stage3-worker",
        now,
        now + chrono::Duration::minutes(2),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let destination_key = (BUCKET.to_owned(), IMPORT_KEY.to_owned());
    let destination = import_destination::Entity::find_by_id(destination_key.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let guard = ImportPublicationGuard {
        job_id: "job-stage3".into(),
        worker_id: "stage3-worker".into(),
        claim_epoch: claimed.claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: BUCKET.into(),
            key: IMPORT_KEY.into(),
            generation: destination.generation,
        }],
    };
    let reject = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            let kind: String = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
                "SELECT pg_typeof(pin_decision_json)::text AS kind FROM import_job_requests WHERE job_id='job-stage3'"))
                .await.unwrap().unwrap().try_get("", "kind").unwrap();
            assert_eq!(kind, "text");
            "CREATE FUNCTION stage3_reject_import() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN IF NEW.state = 'completed' THEN RAISE EXCEPTION 'stage3-import-completion'; END IF;
             RETURN NEW; END $$;
             CREATE TRIGGER stage3_reject_import BEFORE UPDATE ON import_jobs
             FOR EACH ROW EXECUTE FUNCTION stage3_reject_import()"
        }
        _ => {
            "CREATE TRIGGER stage3_reject_import BEFORE UPDATE ON import_jobs
              WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT, 'stage3-import-completion'); END"
        }
    };
    db.execute_unprepared(reject).await.unwrap();
    let error = publication::publish_decided_import_object(
        db,
        request("import-object", IMPORT_KEY, policy),
        guard,
        Vec::new(),
        Utc::now(),
        captured_args(&decision, &config),
    )
    .await
    .unwrap_err();
    assert_database_fault(error, "stage3-import-completion");
    assert_no_published_objects(db).await;
    let job = import_job::Entity::find_by_id("job-stage3")
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "running");
    assert_eq!(job.pin_decision_json, saved.pin_decision_json);
    let current = import_destination::Entity::find_by_id(destination_key)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current, destination);
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 or 18 test container"]
async fn pg17_import_text_decision_and_failed_final_update_roll_back_publication() {
    isolated(|db| async move {
        store::bucket::create(&db, BUCKET, None).await.unwrap();
        assert_import_rollback(&db).await;
    })
    .await;
}

#[tokio::test]
async fn sqlite_pin_ref_failure_rolls_back_object_and_zip_publication() {
    assert_object_and_zip_rollback(&setup().await).await;
}

#[tokio::test]
async fn sqlite_late_mpu_and_import_failures_do_not_orphan_pin_rows() {
    let db = setup().await;
    assert_mpu_rollback(&db).await;
    assert_import_rollback(&db).await;
}
