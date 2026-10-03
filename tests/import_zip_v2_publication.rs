use std::collections::BTreeMap;

use chrono::Utc;
use ipfs_s3_gateway::{
    config::PinningConfig,
    pinning::{
        config::ValidatedPinningConfig, policy::PublicationPolicy,
        zip_policy::ValidatedZipOutputRules,
    },
    store::{
        self,
        entities::{object, object_version, pin_lease, zip_manifest_entry},
        pinning::publication::{
            PublicationObject, ZipV2Publication, ZipV2PublicationResult, ZipV2Success,
        },
        zip::{self, execution, import_intake},
    },
    zip::options::ZipTargets,
};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

async fn setup() -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("import-publication.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let first = store::connect_database(&url).await.unwrap();
    store::run_migrations(&first).await.unwrap();
    store::bucket::create(&first, "bucket", None).await.unwrap();
    let second = store::connect_database(&url).await.unwrap();
    (dir, first, second)
}

async fn prepared(db: &DatabaseConnection, outputs: bool) -> (execution::Claim, ZipV2Publication) {
    let contract = "{\"contract\":\"import\"}";
    let options = serde_json::json!({"options":{"publish_source":false,"publish_extracted":true,"targets":"none","token":"import-token","root_override":null,"root_enabled":true,"result_version":2},"rule_revision":null}).to_string();
    let admission = execution::Admission {
        id: "import-execution".into(),
        owner: "principal".into(),
        source: "import".into(),
        token: "import-token".into(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
        request_contract: contract.into(),
        bucket: "bucket".into(),
        source_key: "source.zip".into(),
        captured_options: options.clone(),
    };
    import_intake::admit(
        db,
        &import_intake::Request {
            admission: admission.clone(),
            prefix: "out/".into(),
            source_descriptor: "https://host/source?credential=secret".into(),
            expected_sha256: Some("a".repeat(64)),
        },
    )
    .await
    .unwrap();
    let claim = execution::claim(db, &admission.id, "worker", 60)
        .await
        .unwrap()
        .unwrap();
    import_intake::bind_verified_input(db, &claim, &"a".repeat(64), "input-cid", 99)
        .await
        .unwrap();
    let items = if outputs {
        vec![execution::ManifestItem::Success {
            path: "file.txt".into(),
            object_key: "out/file.txt".into(),
            cid: "file-cid".into(),
            size: 7,
        }]
    } else {
        vec![]
    };
    let mutations = if outputs {
        BTreeMap::from([("out/file.txt".into(), "guard-output".into())])
    } else {
        BTreeMap::new()
    };
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &items, &mutations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: admission.id,
            owner: admission.owner,
            source: admission.source,
            token: admission.token,
            fingerprint: "pending".into(),
            bucket: "bucket".into(),
            archive_key: "source.zip".into(),
            input_identity: "pending".into(),
            captured_options: options,
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        db,
        &claim.batch_id,
        &if outputs {
            vec![zip::ManifestItem::Success {
                path: "file.txt".into(),
                object_key: "out/file.txt".into(),
                cid: "file-cid".into(),
                size: 7,
            }]
        } else {
            vec![]
        },
    )
    .await
    .unwrap();
    let successes = if outputs {
        vec![ZipV2Success {
            object: PublicationObject::from_put(
                "published-object".into(),
                "bucket",
                "out/file.txt",
                "file-cid".into(),
                7,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            path: "file.txt".into(),
            object_key: "out/file.txt".into(),
            cid: "file-cid".into(),
            size: 7,
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
        }]
    } else {
        vec![]
    };
    let publication = ZipV2Publication {
        claim: claim.clone(), source: None, source_policy: None, source_guard: None,
        successes, targets: ZipTargets::None, captured_rule_revision: None,
        root_outcome: if outputs { zip::RootOutcome::Failed { code: "root_build_failed" } } else { zip::RootOutcome::Empty },
        terminal_result: serde_json::json!({"input_sha256":"a".repeat(64),"published_count":if outputs {1} else {0},"failed_count":0,"status":if outputs {"completed"} else {"failed"}}).to_string(),
    };
    (claim, publication)
}

fn policy() -> (ValidatedPinningConfig, ValidatedZipOutputRules) {
    let config = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    (config, rules)
}

async fn row(db: &DatabaseConnection) -> (String, Option<String>) {
    let found = db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Sqlite,
        "SELECT job_state,receipt_metadata FROM zip_v2_import_requests WHERE batch_id='import-execution'"))
        .await.unwrap().unwrap();
    (
        found.try_get("", "job_state").unwrap(),
        found.try_get("", "receipt_metadata").unwrap(),
    )
}

#[tokio::test]
async fn late_import_receipt_failure_rolls_back_versions_guards_and_both_terminals() {
    let (_dir, first, second) = setup().await;
    let (claim, publication) = prepared(&first, true).await;
    first.execute_unprepared("CREATE TRIGGER block_import_ready BEFORE UPDATE OF job_state ON zip_v2_import_requests WHEN NEW.job_state='ready' BEGIN SELECT RAISE(ABORT, 'injected_late_failure'); END").await.unwrap();
    let (config, rules) = policy();
    assert!(
        import_intake::publish_import_zip_v2(
            &second,
            publication,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(row(&first).await, ("pending".into(), None));
    assert_eq!(
        execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "admitted"
    );
    assert_eq!(
        zip::snapshot(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .batch
            .state,
        "open"
    );
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 0);
    assert!(
        zip_manifest_entry::Entity::find()
            .all(&first)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );
    assert_eq!(
        import_intake::read_for_path(&first, &claim.batch_id, "principal", "bucket", "source.zip")
            .await
            .unwrap()
            .unwrap()
            .state,
        "pending"
    );
}

#[tokio::test]
async fn commit_unknown_replay_reads_original_receipt_without_source_or_root_rewrite() {
    let (_dir, first, second) = setup().await;
    let (claim, publication) = prepared(&first, true).await;
    let (config, rules) = policy();
    assert!(matches!(
        import_intake::publish_import_zip_v2(
            &second,
            publication.clone(),
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .unwrap(),
        ZipV2PublicationResult::Published(_)
    ));
    let receipt = row(&first).await;
    assert_eq!(receipt.0, "ready");
    let serialized = receipt.1.as_deref().unwrap();
    assert!(!serialized.contains("credential"));
    assert!(!serialized.contains("source_version_id"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(serialized).unwrap()["root_status"],
        "failed"
    );
    assert_eq!(
        zip::snapshot(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .batch
            .root_status,
        "failed"
    );
    let status =
        import_intake::read_for_path(&first, &claim.batch_id, "principal", "bucket", "source.zip")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(status.state, "ready");
    assert_eq!(status.root_status.as_deref(), Some("failed"));
    assert!(
        import_intake::read_for_path(&first, &claim.batch_id, "other", "bucket", "source.zip")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        import_intake::read_for_path(&first, &claim.batch_id, "principal", "bucket", "other.zip")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        import_intake::publish_import_zip_v2(
            &second,
            publication,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(row(&first).await, receipt);
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 1);
}

#[tokio::test]
async fn zero_output_completion_and_fenced_failure_are_atomic() {
    let (_dir, first, second) = setup().await;
    let (claim, publication) = prepared(&first, false).await;
    let (config, rules) = policy();
    import_intake::publish_import_zip_v2(
        &second,
        publication,
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    assert_eq!(row(&first).await.0, "ready");
    assert_eq!(
        execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
    assert_eq!(
        import_intake::read_for_path(&first, &claim.batch_id, "principal", "bucket", "source.zip")
            .await
            .unwrap()
            .unwrap()
            .state,
        "ready"
    );
    assert!(
        !import_intake::fail_if_owned(&second, &claim, import_intake::ImportFailure::InvalidZip)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn failure_is_fenced_with_static_code_and_rejects_stale_owner() {
    let (_dir, first, second) = setup().await;
    let contract = "{\"contract\":\"invalid\"}";
    let admission = execution::Admission {
        id: "invalid-input".into(),
        owner: "principal".into(),
        source: "import".into(),
        token: "invalid-token".into(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
        request_contract: contract.into(),
        bucket: "bucket".into(),
        source_key: "invalid.zip".into(),
        captured_options: "{}".into(),
    };
    import_intake::admit(
        &first,
        &import_intake::Request {
            admission,
            prefix: "out/".into(),
            source_descriptor: "https://host/?password=secret".into(),
            expected_sha256: Some("b".repeat(64)),
        },
    )
    .await
    .unwrap();
    let old = execution::claim(&first, "invalid-input", "worker-old", 60)
        .await
        .unwrap()
        .unwrap();
    first.execute_unprepared("UPDATE zip_v2_executions SET lease_until=datetime('now', '-10 seconds') WHERE id='invalid-input'").await.unwrap();
    let current = execution::claim(&second, "invalid-input", "worker-new", 60)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !import_intake::fail_if_owned(&first, &old, import_intake::ImportFailure::InvalidZip)
            .await
            .unwrap()
    );
    first.execute_unprepared("CREATE TRIGGER reject_import_failure BEFORE UPDATE OF job_state ON zip_v2_import_requests WHEN NEW.job_state='failed' BEGIN SELECT RAISE(ABORT, 'injected_failure'); END").await.unwrap();
    assert!(
        import_intake::fail_if_owned(&second, &current, import_intake::ImportFailure::InvalidZip)
            .await
            .is_err()
    );
    assert_eq!(
        execution::read(&first, "invalid-input")
            .await
            .unwrap()
            .unwrap()
            .state,
        "pending"
    );
    let pending = first.query_one(Statement::from_string(sea_orm::DatabaseBackend::Sqlite, "SELECT job_state,receipt_metadata FROM zip_v2_import_requests WHERE batch_id='invalid-input'")).await.unwrap().unwrap();
    assert_eq!(
        pending.try_get::<String>("", "job_state").unwrap(),
        "pending"
    );
    assert!(
        pending
            .try_get::<Option<String>>("", "receipt_metadata")
            .unwrap()
            .is_none()
    );
    first
        .execute_unprepared("DROP TRIGGER reject_import_failure")
        .await
        .unwrap();
    assert!(
        import_intake::fail_if_owned(
            &second,
            &current,
            import_intake::ImportFailure::ExpectedSha256Mismatch {
                measured_sha256: &"a".repeat(64)
            }
        )
        .await
        .unwrap()
    );
    assert_eq!(
        execution::read(&first, "invalid-input")
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .as_deref(),
        Some("expected_sha256_mismatch")
    );
    let failed = first.query_one(Statement::from_string(sea_orm::DatabaseBackend::Sqlite, "SELECT job_state,receipt_metadata FROM zip_v2_import_requests WHERE batch_id='invalid-input'")).await.unwrap().unwrap();
    assert_eq!(failed.try_get::<String>("", "job_state").unwrap(), "failed");
    let receipt: String = failed.try_get("", "receipt_metadata").unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&receipt).unwrap(),
        serde_json::json!({"code":"expected_sha256_mismatch"})
    );
    assert!(!receipt.contains("password"));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert!(
        !import_intake::fail_if_owned(&second, &current, import_intake::ImportFailure::InvalidZip)
            .await
            .unwrap()
    );
    assert_eq!(
        import_intake::read_for_path(
            &first,
            "invalid-input",
            "principal",
            "bucket",
            "invalid.zip"
        )
        .await
        .unwrap()
        .unwrap()
        .state,
        "failed"
    );
}

#[tokio::test]
async fn invalid_import_terminal_cannot_persist_secret_or_fabricated_source_version() {
    let (_dir, first, second) = setup().await;
    let (claim, mut publication) = prepared(&first, true).await;
    publication.terminal_result = serde_json::json!({
        "input_sha256":"a".repeat(64),"published_count":1,"failed_count":0,
        "status":"completed","source_version_id":"fabricated","raw_error":"https://host/?password=secret"
    }).to_string();
    let (config, rules) = policy();
    assert!(
        import_intake::publish_import_zip_v2(
            &second,
            publication,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(row(&first).await, ("pending".into(), None));
    assert_eq!(
        execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "admitted"
    );
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn stale_publication_epoch_and_prefix_substitution_cannot_ready_import() {
    let (_dir, first, second) = setup().await;
    let (claim, publication) = prepared(&first, true).await;
    let (config, rules) = policy();
    let mut wrong_prefix = publication.clone();
    wrong_prefix.successes[0].path = "other.txt".into();
    assert!(
        import_intake::publish_import_zip_v2(
            &second,
            wrong_prefix,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    first.execute_unprepared("UPDATE zip_v2_executions SET lease_until=datetime('now', '-10 seconds') WHERE id='import-execution'").await.unwrap();
    let replacement = execution::claim(&second, &claim.batch_id, "new-worker", 60)
        .await
        .unwrap()
        .unwrap();
    assert!(replacement.epoch > claim.epoch);
    assert!(
        import_intake::publish_import_zip_v2(
            &second,
            publication,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(row(&first).await, ("pending".into(), None));
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn root_only_retry_does_not_rewrite_import_receipt_or_execution_terminal() {
    let (_dir, first, second) = setup().await;
    let (claim, publication) = prepared(&first, true).await;
    let (config, rules) = policy();
    import_intake::publish_import_zip_v2(
        &second,
        publication,
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    let original_receipt = row(&first).await;
    let original_terminal = execution::read(&first, &claim.batch_id)
        .await
        .unwrap()
        .unwrap()
        .terminal_result;
    let root_claim = zip::claim_root(&first, &claim.batch_id, "root-worker", 60)
        .await
        .unwrap();
    let tx = first.begin().await.unwrap();
    let terminal = serde_json::json!({"input_sha256":"a".repeat(64),"published_count":1,"failed_count":0,"status":"completed","root_status":"failed","root_warning":"root_retry_failed"}).to_string();
    root_claim
        .settle_failed_retry(&tx, &terminal, "root_retry_failed")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(row(&first).await, original_receipt);
    assert_eq!(
        execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .terminal_result,
        original_terminal
    );
    assert_eq!(
        zip::snapshot(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .batch
            .terminal_result
            .as_deref(),
        Some(terminal.as_str())
    );
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        1
    );
}
