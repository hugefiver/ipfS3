//! Real PostgreSQL 17 evidence. Each ignored test requires an explicit test URL
//! and creates/drops only its own UUID-named schema (never the public schema).
#[path = "stage3_migrations.rs"]
mod migrations;

use std::{collections::HashMap, future::Future, panic::AssertUnwindSafe};

use chrono::Utc;
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    config::{OptionalPinControlMode, PinningConfig},
    import::ImportSource,
    lifecycle::model::MultipartUploadTargetIdentity,
    pinning::{
        config::{ProviderLimitMap, ValidatedPinningConfig},
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
        pinning::{
            decision as stored_decision,
            publication::{
                self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
                ZipPublicationRequest,
            },
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, PaginatorTrait, QueryFilter, Statement, TransactionTrait,
};

const BUCKET: &str = "stage3-bucket";
const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

async fn isolated<F, Fut>(migrate: bool, body: F)
where
    F: FnOnce(DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("set IPFS_S3_TEST_POSTGRES_URL to an authorized, isolated PostgreSQL 17 endpoint");
    let admin = Database::connect(&url)
        .await
        .expect("test PostgreSQL must be reachable");
    let version = admin
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version",
        ))
        .await
        .unwrap()
        .unwrap();
    let version: String = version.try_get("", "server_version").unwrap();
    assert!(
        version.starts_with("17."),
        "requires PostgreSQL 17, got {version}"
    );

    let schema = format!("stage3_evidence_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema.starts_with("stage3_evidence_")
            && schema
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(url);
        options.min_connections(1).max_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
        db.execute_unprepared("SET statement_timeout TO '15s'")
            .await
            .unwrap();
        if migrate {
            store::run_migrations(&db).await.unwrap();
        }
        body(db.clone()).await;
        db.close().await.unwrap();
    })
    .catch_unwind()
    .await;
    // Only this test's generated schema is removed, including on assertion failure.
    admin
        .execute_unprepared("SET statement_timeout TO '15s'")
        .await
        .unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn captured(
    key: &str,
    admission: &str,
) -> (PublicationPolicy, ExtensionDecision, ValidatedPinningConfig) {
    let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let tags = vec![
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("private", "do-not-leak"),
    ];
    let (policy, decision) = PinPolicyEvaluator::with_mode(&config, OptionalPinControlMode::Warn)
        .evaluate_publication_decision(
            PublicationContext {
                bucket: BUCKET,
                key,
                tags: &tags,
                is_decompress_zip: true,
            },
            DecisionOrigin::new("stage3-test", admission),
        )
        .unwrap();
    assert_eq!(decision.effect, DecisionEffect::Skipped);
    (policy, decision, config)
}

fn request(id: &str, key: &str, policy: PublicationPolicy) -> PublicationRequest {
    let object = PublicationObject::from_put(
        id.to_owned(),
        BUCKET,
        key,
        CID.to_owned(),
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
            logical_size: 1,
        },
        object,
        policy,
    }
}

fn captured_args<'a>(
    decision: &'a ExtensionDecision,
    config: &'a ValidatedPinningConfig,
    limits: &'a ProviderLimitMap,
) -> DecidedPublish<'a> {
    DecidedPublish {
        decision,
        config,
        mode: OptionalPinControlMode::Warn,
        limits,
    }
}

async fn count(db: &DatabaseConnection, table: &str) -> u64 {
    match table {
        "objects" => object::Entity::find().count(db).await.unwrap(),
        "object_versions" => object_version::Entity::find().count(db).await.unwrap(),
        "pin_extension_decisions" => {
            ipfs_s3_gateway::store::entities::pin_extension_decision::Entity::find()
                .count(db)
                .await
                .unwrap()
        }
        other => panic!("unexpected count table {other}"),
    }
}

async fn assert_no_published_objects(db: &DatabaseConnection) {
    for table in ["objects", "object_versions", "pin_extension_decisions"] {
        assert_eq!(count(db, table).await, 0, "orphan in {table}");
    }
}

async fn seeded_bucket(db: &DatabaseConnection) {
    store::bucket::create(db, BUCKET, None).await.unwrap();
}

fn assert_pg_constraint(error: DbErr, sqlstate: &str, constraint: &str) {
    let detail = format!("{error:?}");
    assert!(
        detail.contains(&format!("code: \"{sqlstate}\"")) && detail.contains(constraint),
        "expected PostgreSQL SQLSTATE {sqlstate} from {constraint}, got {detail}"
    );
}

fn decision_insert(
    version: &str,
    owner: &str,
    revision: &str,
    config: &str,
    effect: &str,
    snapshot: &str,
) -> Statement {
    Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO pin_extension_decisions (version_row_id,object_id,control_revision,config_revision,effect,snapshot) VALUES ($1,$2,$3,$4,$5,$6)",
        [
            version.into(),
            owner.into(),
            revision.into(),
            config.into(),
            effect.into(),
            snapshot.into(),
        ],
    )
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 test container"]
async fn pg17_stage2_upgrade_retains_raw_tags_without_backfilling_decisions() {
    isolated(false, |db| async move {
        migrations::upgrade_with_historical_raw_tags(&db).await;
        let columns = db.query_all(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT table_name, data_type FROM information_schema.columns WHERE table_schema=current_schema() AND column_name='pin_decision_json' ORDER BY table_name"
        )).await.unwrap();
        let shape: Vec<(String, String)> = columns.iter().map(|r| (r.try_get("", "table_name").unwrap(), r.try_get("", "data_type").unwrap())).collect();
        assert_eq!(shape, [("import_jobs".into(), "text".into()), ("multipart_uploads".into(), "jsonb".into())]);
    }).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 test container"]
async fn pg17_exact_version_owner_fk_checks_and_snapshot_read() {
    isolated(true, |db| async move {
        seeded_bucket(&db).await;
        let limits = ProviderLimitMap::new();
        for (id, key) in [("owner-one", "one"), ("owner-two", "two")] {
            let (policy, decision, config) = captured(key, id);
            publication::publish_decided_object(
                &db,
                request(id, key, policy),
                captured_args(&decision, &config, &limits),
            )
            .await
            .unwrap();
        }
        let (policy, free_decision, _) = captured("three", "owner-three");
        publication::publish_object(&db, request("owner-three", "three", policy), &limits)
            .await
            .unwrap();
        let row = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq("owner-one"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let unclaimed = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq("owner-three"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unclaimed.object_id.as_deref(), Some("owner-three"));
        assert!(
            stored_decision::read_for_version(&db, &unclaimed.id)
                .await
                .unwrap()
                .is_none(),
            "wrong-owner fixture must have an unoccupied version row"
        );
        let restored = stored_decision::read_for_version(&db, &row.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.origin.request_id, "owner-one");
        assert_eq!(restored.effect, DecisionEffect::Skipped);
        let snapshot = serde_json::to_string(&free_decision).unwrap();
        let bad_source = decision_insert(
            "missing-version",
            "owner-one",
            "revision-missing",
            &free_decision.config_revision,
            "skipped",
            &snapshot,
        );
        assert_pg_constraint(
            db.execute(bad_source).await.unwrap_err(),
            "23503",
            "fk_pin_extension_decisions_owner",
        );
        // Both the version and owner exist independently, but this exact pair does not.
        let bad_owner = decision_insert(
            &unclaimed.id,
            "owner-two",
            "revision-wrong-owner",
            &free_decision.config_revision,
            "skipped",
            &snapshot,
        );
        assert_pg_constraint(
            db.execute(bad_owner).await.unwrap_err(),
            "23503",
            "fk_pin_extension_decisions_owner",
        );
        assert_eq!(count(&db, "pin_extension_decisions").await, 2);
        let short_revision = decision_insert(
            &unclaimed.id,
            "owner-three",
            "revision-short",
            "short",
            "skipped",
            &snapshot,
        );
        assert_pg_constraint(
            db.execute(short_revision).await.unwrap_err(),
            "23514",
            "ck_pin_extension_decisions_revision",
        );
        let bad_effect = decision_insert(
            &unclaimed.id,
            "owner-three",
            "revision-effect",
            &free_decision.config_revision,
            "maybe",
            &snapshot,
        );
        assert_pg_constraint(
            db.execute(bad_effect).await.unwrap_err(),
            "23514",
            "pin_extension_decisions_effect_check",
        );
        let original_revision = restored.control_revision;
        let duplicate = decision_insert(
            &unclaimed.id,
            "owner-three",
            &original_revision,
            &free_decision.config_revision,
            "skipped",
            &snapshot,
        );
        assert_pg_constraint(
            db.execute(duplicate).await.unwrap_err(),
            "23505",
            "pin_extension_decisions_control_revision_key",
        );

        // The same version/owner pair accepts a fully valid capture; leave no test decision behind.
        let transaction = db.begin().await.unwrap();
        transaction
            .execute(decision_insert(
                &unclaimed.id,
                "owner-three",
                &free_decision.control_revision,
                &free_decision.config_revision,
                "skipped",
                &snapshot,
            ))
            .await
            .unwrap();
        assert_eq!(
            stored_decision::read_for_version(&transaction, &unclaimed.id)
                .await
                .unwrap(),
            Some(free_decision)
        );
        transaction.rollback().await.unwrap();
        assert!(
            stored_decision::read_for_version(&db, &unclaimed.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(count(&db, "pin_extension_decisions").await, 2);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 test container"]
async fn pg17_object_and_zip_decision_insert_failure_roll_back_all_rows() {
    isolated(true, |db| async move {
        seeded_bucket(&db).await;
        db.execute_unprepared("CREATE FUNCTION stage3_reject_decision() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'stage3-decision-insert'; END $$; CREATE TRIGGER stage3_reject BEFORE INSERT ON pin_extension_decisions FOR EACH ROW EXECUTE FUNCTION stage3_reject_decision()").await.unwrap();
        let limits = ProviderLimitMap::new();
        let (policy, decision, config) = captured("ordinary", "ordinary-object");
        let error = publication::publish_decided_object(&db, request("ordinary-object", "ordinary", policy), captured_args(&decision, &config, &limits)).await.unwrap_err();
        assert!(format!("{error:?}").contains("stage3-decision-insert"), "{error:?}");
        assert_no_published_objects(&db).await;
        let (policy, decision, config) = captured("archive.zip", "archive-object");
        let entry = PublicationObject::from_put("entry-object".into(), BUCKET, "unzipped/item", CID.into(), 1, None, None, false, None, None, Utc::now());
        let error = publication::publish_decided_zip(&db, ZipPublicationRequest {
            archive: request("archive-object", "archive.zip", policy), entries: vec![entry],
        }, None, captured_args(&decision, &config, &limits)).await.unwrap_err();
        assert!(format!("{error:?}").contains("stage3-decision-insert"), "{error:?}");
        assert_no_published_objects(&db).await;
    }).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 test container"]
async fn pg17_mpu_jsonb_roundtrip_and_failed_completion_preserves_upload() {
    isolated(true, |db| async move {
        seeded_bucket(&db).await;
        let (policy, decision, config) = captured("mpu", "upload-stage3");
        store::multipart::create_upload_with_decision(&db, "upload-stage3", "multipart-object", BUCKET, "mpu", "none", None, None, None, None, &policy.tags, None, false, Some(&decision)).await.unwrap();
        let saved = store::multipart::get_upload(&db, "upload-stage3").await.unwrap();
        assert_eq!(store::multipart::decision_from_upload(&saved).unwrap().unwrap(), decision);
        let kind = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT pg_typeof(pin_decision_json)::text AS kind FROM multipart_uploads WHERE upload_id='upload-stage3'"
        )).await.unwrap().unwrap();
        assert_eq!(kind.try_get::<String>("", "kind").unwrap(), "jsonb");
        let target = MultipartUploadTargetIdentity { bucket: BUCKET.into(), key: "mpu".into(), upload_id: saved.upload_id.clone(), initiated_at: saved.created_at };
        db.execute_unprepared("CREATE FUNCTION stage3_reject_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'stage3-upload-delete'; END $$; CREATE TRIGGER stage3_fail_upload BEFORE DELETE ON multipart_uploads FOR EACH ROW EXECUTE FUNCTION stage3_reject_delete()").await.unwrap();
        let limits = ProviderLimitMap::new();
        let error = publication::publish_decided_completed_upload(&db, &target, request("completed-object", "mpu", policy), None, captured_args(&decision, &config, &limits)).await.unwrap_err();
        assert!(format!("{error:?}").contains("stage3-upload-delete"), "{error:?}");
        assert_no_published_objects(&db).await;
        let still_there = store::multipart::get_upload(&db, "upload-stage3").await.unwrap();
        assert_eq!(store::multipart::decision_from_upload(&still_there).unwrap(), Some(decision));
    }).await;
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL to an isolated PostgreSQL 17 test container"]
async fn pg17_import_text_decision_and_failed_final_update_roll_back_publication() {
    isolated(true, |db| async move {
        seeded_bucket(&db).await;
        let (policy, decision, config) = captured("import", "job-stage3");
        let now = Utc::now();
        let submitted = ownership::submit_decided(&db, jobs::NewImportJob {
            id: "job-stage3".into(), bucket: BUCKET.into(), key: "import".into(),
            source: ImportSource::Cid(CID.into()), request_fingerprint: "fingerprint".into(),
            client_token: None, object_content_type: None, metadata: HashMap::new(),
            tags: policy.tags.clone(), decompress_prefix: None,
        }, decision.clone(), now).await.unwrap();
        assert!(matches!(submitted, jobs::SubmitImportOutcome::Created(_)));
        let saved = import_job::Entity::find_by_id("job-stage3").one(&db).await.unwrap().unwrap();
        let text = saved.pin_decision_json.as_deref().unwrap();
        assert_eq!(serde_json::from_str::<ExtensionDecision>(text).unwrap(), decision);
        assert!(!text.contains("do-not-leak"));
        let kind = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT pg_typeof(pin_decision_json)::text AS kind FROM import_jobs WHERE id='job-stage3'"
        )).await.unwrap().unwrap();
        assert_eq!(kind.try_get::<String>("", "kind").unwrap(), "text");
        let claimed = jobs::claim_due(&db, "stage3-worker", Utc::now(), Utc::now() + chrono::Duration::minutes(2), 1).await.unwrap().pop().unwrap();
        let destination = import_destination::Entity::find_by_id((BUCKET.to_owned(), "import".to_owned())).one(&db).await.unwrap().unwrap();
        let guard = ImportPublicationGuard { job_id: "job-stage3".into(), worker_id: "stage3-worker".into(), claim_epoch: claimed.claim.claim_epoch,
            targets: vec![ExpectedImportTarget { bucket: BUCKET.into(), key: "import".into(), generation: destination.generation }],
        };
        db.execute_unprepared("CREATE FUNCTION stage3_reject_finish() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state = 'completed' THEN RAISE EXCEPTION 'stage3-import-completion'; END IF; RETURN NEW; END $$; CREATE TRIGGER stage3_fail_import BEFORE UPDATE ON import_jobs FOR EACH ROW EXECUTE FUNCTION stage3_reject_finish()").await.unwrap();
        let limits = ProviderLimitMap::new();
        let error = publication::publish_decided_import_object(&db, request("import-object", "import", policy), guard, Vec::new(), Utc::now(), captured_args(&decision, &config, &limits)).await.unwrap_err();
        assert!(format!("{error:?}").contains("stage3-import-completion"), "{error:?}");
        assert_no_published_objects(&db).await;
        let job = import_job::Entity::find_by_id("job-stage3").one(&db).await.unwrap().unwrap();
        assert_eq!(job.state, "running");
        assert_eq!(job.pin_decision_json, Some(text.to_owned()));
        let current = import_destination::Entity::find_by_id((BUCKET.to_owned(), "import".to_owned())).one(&db).await.unwrap().unwrap();
        assert_eq!(current.owner_job_id.as_deref(), Some("job-stage3"));
    }).await;
}

#[tokio::test]
async fn sqlite_decision_write_failure_rolls_back_object_and_zip_publication() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    seeded_bucket(&db).await;
    db.execute_unprepared("CREATE TRIGGER stage3_reject BEFORE INSERT ON pin_extension_decisions BEGIN SELECT RAISE(ABORT, 'stage3-decision-insert'); END").await.unwrap();
    let limits = ProviderLimitMap::new();
    let (policy, decision, config) = captured("ordinary", "ordinary-object");
    let result = publication::publish_decided_object(
        &db,
        request("ordinary-object", "ordinary", policy),
        captured_args(&decision, &config, &limits),
    )
    .await;
    assert!(result.is_err());
    assert_no_published_objects(&db).await;
    let (policy, decision, config) = captured("archive.zip", "archive-object");
    let entry = PublicationObject::from_put(
        "entry-object".into(),
        BUCKET,
        "unzipped/item",
        CID.into(),
        1,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    let result = publication::publish_decided_zip(
        &db,
        ZipPublicationRequest {
            archive: request("archive-object", "archive.zip", policy),
            entries: vec![entry],
        },
        None,
        captured_args(&decision, &config, &limits),
    )
    .await;
    assert!(result.is_err());
    assert_no_published_objects(&db).await;
}

#[tokio::test]
async fn sqlite_late_mpu_and_import_failures_do_not_orphan_decisions() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    seeded_bucket(&db).await;
    let limits = ProviderLimitMap::new();

    let (policy, decision, config) = captured("mpu", "upload-stage3");
    store::multipart::create_upload_with_decision(
        &db,
        "upload-stage3",
        "multipart-object",
        BUCKET,
        "mpu",
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
    let saved = store::multipart::get_upload(&db, "upload-stage3")
        .await
        .unwrap();
    let target = MultipartUploadTargetIdentity {
        bucket: BUCKET.into(),
        key: "mpu".into(),
        upload_id: saved.upload_id.clone(),
        initiated_at: saved.created_at,
    };
    db.execute_unprepared("CREATE TRIGGER stage3_upload_abort BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'stage3-upload-delete'); END")
        .await
        .unwrap();
    let result = publication::publish_decided_completed_upload(
        &db,
        &target,
        request("completed-object", "mpu", policy),
        None,
        captured_args(&decision, &config, &limits),
    )
    .await;
    assert!(result.is_err());
    assert_no_published_objects(&db).await;
    assert!(
        store::multipart::get_upload(&db, "upload-stage3")
            .await
            .is_ok()
    );

    let (policy, decision, config) = captured("import", "job-stage3");
    let now = Utc::now();
    ownership::submit_decided(
        &db,
        jobs::NewImportJob {
            id: "job-stage3".into(),
            bucket: BUCKET.into(),
            key: "import".into(),
            source: ImportSource::Cid(CID.into()),
            request_fingerprint: "fingerprint".into(),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags: policy.tags.clone(),
            decompress_prefix: None,
        },
        decision.clone(),
        now,
    )
    .await
    .unwrap();
    let claimed = jobs::claim_due(
        &db,
        "stage3-worker",
        Utc::now(),
        Utc::now() + chrono::Duration::minutes(2),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let destination = import_destination::Entity::find_by_id((BUCKET.to_owned(), "import".into()))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let guard = ImportPublicationGuard {
        job_id: "job-stage3".into(),
        worker_id: "stage3-worker".into(),
        claim_epoch: claimed.claim.claim_epoch,
        targets: vec![ExpectedImportTarget {
            bucket: BUCKET.into(),
            key: "import".into(),
            generation: destination.generation,
        }],
    };
    db.execute_unprepared("CREATE TRIGGER stage3_import_abort BEFORE UPDATE ON import_jobs WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT, 'stage3-import-completion'); END")
        .await
        .unwrap();
    let result = publication::publish_decided_import_object(
        &db,
        request("import-object", "import", policy),
        guard,
        Vec::new(),
        Utc::now(),
        captured_args(&decision, &config, &limits),
    )
    .await;
    assert!(result.is_err());
    assert_no_published_objects(&db).await;
    let saved = import_job::Entity::find_by_id("job-stage3")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.state, "running");
    assert_eq!(
        serde_json::from_str::<ExtensionDecision>(saved.pin_decision_json.as_deref().unwrap())
            .unwrap(),
        decision
    );
}
