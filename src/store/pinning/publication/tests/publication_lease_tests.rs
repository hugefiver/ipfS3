use super::*;
use crate::store::{
    entities::{multipart_upload, standard_mutation_lease, zip_batch},
    zip as storage,
};

async fn fixture(
    db: &DatabaseConnection,
    id: &str,
    root: bool,
) -> (
    ZipPublicationRequest,
    ZipBatchPublication,
    StandardMutationGuard,
    MultipartUploadTargetIdentity,
) {
    let mut archive = object("lease-archive", "archive.zip", "bafy-archive", 7);
    archive.multipart = true;
    let archive = request(
        archive,
        vec![tag("ipfs-s3:pin", "true")],
        vec![intent(
            LeaseSource::Manual,
            ProviderMode::All,
            &["pinata", "filebase"],
            ContentMode::Decompressed,
        )],
    );
    let (mut admitted, target) = zip_fixture::upload(db, id, &archive, "out/").await;
    if root {
        let mut value = serde_json::to_value(&admitted.capture).unwrap();
        value["controls"]["root"] =
            serde_json::to_value(crate::pinning::tags::ZipRootCapture::Configured(true)).unwrap();
        admitted.capture =
            crate::pinning::zip_policy::CapturedZipIntent::decode_json(&value.to_string()).unwrap();
        zip_batch::Entity::update_many()
            .col_expr(
                zip_batch::Column::CapturedOptions,
                Expr::value(serde_json::json!({"zip_intent": admitted.capture}).to_string()),
            )
            .filter(zip_batch::Column::Id.eq(id))
            .exec(db)
            .await
            .unwrap();
        multipart_upload::Entity::update_many()
            .col_expr(
                multipart_upload::Column::ZipIntentJson,
                Expr::value(serde_json::to_string(&admitted.capture).unwrap()),
            )
            .filter(multipart_upload::Column::UploadId.eq(id))
            .exec(db)
            .await
            .unwrap();
    }
    let guard = admitted.completion_guard(db).await;
    let mut request = ZipPublicationRequest {
        archive,
        entries: vec![
            object("lease-a", "out/a", "bafy-a", 3),
            object("lease-b", "out/b", "bafy-b", 4),
        ],
    };
    let mut batch = admitted.prepare(db, &mut request).await;
    if root {
        let claim = storage::claim_root(db, id, "lease-worker", 600)
            .await
            .unwrap();
        storage::mark_invoked(db, &claim).await.unwrap();
        storage::retain_candidate(db, &claim, "node", "hot", "bafy-root")
            .await
            .unwrap();
        storage::verify_root(
            db,
            &claim,
            "node",
            "hot",
            "bafy-root",
            "recursive-pin-proof",
        )
        .await
        .unwrap();
        batch.root_outcome = storage::RootOutcome::Verified {
            claim,
            node_identity: "node".into(),
            tier: "hot".into(),
            cid: "bafy-root".into(),
        };
        db.execute_unprepared("UPDATE zip_root_builds SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ','now','+10 seconds')").await.unwrap();
    }
    db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until = strftime('%Y-%m-%dT%H:%M:%fZ','now','+10 seconds')").await.unwrap();
    (request, batch, guard, target)
}

async fn publish(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    batch: ZipBatchPublication,
    guard: StandardMutationGuard,
    target: MultipartUploadTargetIdentity,
) -> Result<PublicationResult, TransactionError<AppError>> {
    publication_attempt(
        db.into(),
        request.archive,
        request.entries,
        Some(batch),
        Some(target),
        Some(guard),
        None,
        vec![],
        None,
        None,
        limits(),
        None,
        Some(zip_fixture::replay()),
        &Default::default(),
    )
    .await
}

#[tokio::test]
async fn legacy_mpu_publication_renews_live_leases_before_objects_and_adopts_complete_receipt() {
    let db = setup().await;
    let (request, batch, guard, target) = fixture(&db, "lease-success", true).await;
    db.execute_unprepared("CREATE TRIGGER require_renewal BEFORE INSERT ON objects BEGIN SELECT CASE WHEN EXISTS (SELECT 1 FROM standard_mutation_leases WHERE julianday(lease_until) < julianday('now','+100 seconds')) THEN RAISE(ABORT,'standard lease not maintained') END; SELECT CASE WHEN EXISTS (SELECT 1 FROM zip_root_builds WHERE julianday(lease_until) < julianday('now','+590 seconds')) THEN RAISE(ABORT,'root lease not maintained') END; END").await.unwrap();
    publish(&db, request, batch, guard, target).await.unwrap();
    let snapshot = storage::snapshot(&db, "lease-success")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "complete");
    assert_eq!(snapshot.batch.root_cid.as_deref(), Some("bafy-root"));
    assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_some()));
    assert_eq!(snapshot.entries.len(), 2);
    assert_eq!(snapshot.references[0].state, "adopted");
    assert!(snapshot.references[0].verification_receipt.is_some());
    assert_eq!(rows::<object::Entity>(&db).await, 3);
    assert_eq!(rows::<object_version::Entity>(&db).await, 3);
    assert_eq!(targets_for_object(&db, "lease-archive").await.len(), 4);
    assert!(multipart::get_upload(&db, "lease-success").await.is_err());
    assert!(
        multipart::list_parts(&db, "lease-success")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(rows::<standard_mutation_lease::Entity>(&db).await, 0);
    let receipt = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT COUNT(*) AS n FROM zip_mpu_replays WHERE batch_id='lease-success' AND archive_cid='bafy-archive' AND response_xml IS NOT NULL".to_owned())).await.unwrap().unwrap();
    assert_eq!(receipt.try_get::<i64>("", "n").unwrap(), 1);
}

#[tokio::test]
async fn legacy_mpu_periodic_maintenance_revisits_frontier_targets_and_both_binding_loops() {
    let db = setup().await;
    for provider in ["pinata", "filebase"] {
        let identity = crate::pinning::identity::ProviderIdentity::legacy(
            provider, "noop", None, "psa", "psa",
        );
        ledger::register_route(&db, provider, &identity)
            .await
            .unwrap();
    }
    for (id, key, cid, size) in [
        ("shared-a", "prior/a", "bafy-a", 3),
        ("shared-b", "prior/b", "bafy-b", 4),
    ] {
        let prior = request(
            object(id, key, cid, size),
            vec![tag("ipfs-s3:pin", "true")],
            vec![intent(
                LeaseSource::Manual,
                ProviderMode::All,
                &["pinata", "filebase"],
                ContentMode::Object,
            )],
        );
        publish_object(&db, prior, &limits()).await.unwrap();
    }
    db.execute_unprepared("UPDATE remote_pins SET status='failed',request_id='failed-'||provider||cid,failure_attempts=8,next_retry_at=NULL; DELETE FROM pin_jobs;").await.unwrap();
    let (request, batch, guard, target) = fixture(&db, "lease-periodic", true).await;
    db.execute_unprepared("CREATE TABLE renewal_count(n INTEGER NOT NULL); INSERT INTO renewal_count VALUES(0); CREATE TABLE lease_observations(stage TEXT NOT NULL,n INTEGER NOT NULL); CREATE TRIGGER count_renewal AFTER UPDATE ON standard_mutation_leases BEGIN UPDATE renewal_count SET n=n+1; END; CREATE TRIGGER observe_root_first AFTER UPDATE ON zip_root_builds WHEN julianday(OLD.lease_until)<julianday('now','+100 seconds') BEGIN INSERT INTO lease_observations SELECT 'root-first',n FROM renewal_count; END;").await.unwrap();
    for (table, stage, event, condition) in [
        ("objects", "object", "INSERT", ""),
        ("pin_lease_targets", "target", "INSERT", ""),
        ("remote_pins", "projection", "UPDATE", ""),
        (
            "remote_pins",
            "failed",
            "UPDATE",
            "WHEN OLD.failure_attempts>0 AND NEW.failure_attempts=0",
        ),
        (
            "zip_manifest_entries",
            "binding",
            "UPDATE",
            "WHEN NEW.version_row_id IS NOT NULL",
        ),
    ] {
        db.execute_unprepared(&format!("CREATE TRIGGER observe_{stage} BEFORE {event} ON {table} {condition} BEGIN INSERT INTO lease_observations SELECT '{stage}',n FROM renewal_count; END")).await.unwrap();
    }
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .insert("lease-periodic".into());
    let result = publish(&db, request, batch, guard, target).await;
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .remove("lease-periodic");
    result.unwrap();
    let observations = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT stage,n FROM lease_observations ORDER BY rowid".to_owned(),
        ))
        .await
        .unwrap();
    let observed = observations
        .iter()
        .map(|row| {
            (
                row.try_get::<String>("", "stage").unwrap(),
                row.try_get::<i64>("", "n").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let root_first = observed
        .iter()
        .find(|(stage, _)| stage == "root-first")
        .unwrap()
        .1;
    assert!(
        root_first > 1,
        "version frontier must maintain the standard lease before the original batch lock"
    );
    for stage in ["object", "target", "failed", "projection", "binding"] {
        let counts = observed
            .iter()
            .filter(|(name, _)| name == stage)
            .map(|(_, n)| *n)
            .collect::<Vec<_>>();
        assert!(counts.len() >= 2, "{stage}: {observed:?}");
        assert!(
            counts.windows(2).all(|pair| pair[1] >= pair[0])
                && counts.windows(2).any(|pair| pair[1] > pair[0]),
            "maintenance inside {stage}: {observed:?}"
        );
    }
    let last_target = observed
        .iter()
        .filter(|(stage, _)| stage == "projection")
        .map(|(_, n)| *n)
        .max()
        .unwrap();
    let first_binding = observed
        .iter()
        .find(|(stage, _)| stage == "binding")
        .unwrap()
        .1;
    assert!(
        first_binding > last_target + 2,
        "exact-version validation must maintain before the manifest write loop"
    );
}

#[tokio::test]
async fn legacy_mpu_shared_cid_projection_maintains_between_each_owned_lease_and_target() {
    let db = setup().await;
    for provider in ["pinata", "filebase"] {
        let identity = crate::pinning::identity::ProviderIdentity::legacy(
            provider, "noop", None, "psa", "psa",
        );
        ledger::register_route(&db, provider, &identity)
            .await
            .unwrap();
    }
    for index in 0..3 {
        let prior = request(
            object(
                &format!("hot-owner-{index}"),
                &format!("prior/{index}"),
                "bafy-a",
                3,
            ),
            vec![tag("ipfs-s3:pin", "true")],
            [LeaseSource::Automatic, LeaseSource::Manual]
                .into_iter()
                .map(|source| {
                    intent(
                        source,
                        ProviderMode::All,
                        &["pinata", "filebase"],
                        ContentMode::Object,
                    )
                })
                .collect(),
        );
        publish_object(&db, prior, &limits()).await.unwrap();
    }
    let (request, batch, guard, target) = fixture(&db, "lease-hot-cid", true).await;
    let source_lease = lease_id("lease-archive", LeaseSource::Manual);
    db.execute_unprepared(&format!("CREATE TABLE shared_projection_probe(armed INTEGER NOT NULL); INSERT INTO shared_projection_probe VALUES(0); CREATE TABLE shared_projection_visits(kind TEXT NOT NULL,id TEXT NOT NULL); CREATE TRIGGER arm_shared_projection AFTER INSERT ON pin_lease_targets WHEN NEW.lease_id='{source_lease}' AND NEW.cid='bafy-b' AND NEW.provider='filebase' BEGIN UPDATE shared_projection_probe SET armed=1; END")).await.unwrap();
    for (table, kind) in [("pin_leases", "lease"), ("pin_lease_targets", "target")] {
        db.execute_unprepared(&format!("CREATE TRIGGER require_shared_{kind}_renewal BEFORE UPDATE ON {table} WHEN (SELECT armed FROM shared_projection_probe)=1 BEGIN SELECT CASE WHEN EXISTS(SELECT 1 FROM standard_mutation_leases WHERE julianday(lease_until)<julianday('now','+100 seconds')) THEN RAISE(ABORT,'shared projection needs renewal') END; END; CREATE TRIGGER consume_shared_{kind}_budget AFTER UPDATE ON {table} WHEN (SELECT armed FROM shared_projection_probe)=1 BEGIN INSERT INTO shared_projection_visits VALUES('{kind}',NEW.id); UPDATE standard_mutation_leases SET lease_until=strftime('%Y-%m-%dT%H:%M:%fZ','now','+10 seconds'); END")).await.unwrap();
    }
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .insert("lease-hot-cid".into());
    let result = publish(&db, request, batch, guard, target).await;
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .remove("lease-hot-cid");
    result.unwrap();
    let owners = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT COUNT(DISTINCT l.owner_object_id) AS owners,COUNT(DISTINCT l.id) AS leases,COUNT(DISTINCT t.id) AS targets FROM pin_leases l JOIN pin_lease_targets t ON t.lease_id=l.id WHERE t.cid='bafy-a' AND l.owner_object_id LIKE 'hot-owner-%' AND EXISTS(SELECT 1 FROM shared_projection_visits v WHERE v.kind='lease' AND v.id=l.id) AND EXISTS(SELECT 1 FROM shared_projection_visits v WHERE v.kind='target' AND v.id=t.id)".to_owned())).await.unwrap().unwrap();
    assert_eq!(owners.try_get::<i64>("", "owners").unwrap(), 3);
    assert_eq!(owners.try_get::<i64>("", "leases").unwrap(), 6);
    assert_eq!(owners.try_get::<i64>("", "targets").unwrap(), 12);
    let snapshot = storage::snapshot(&db, "lease-hot-cid")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "complete");
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_some())
    );
    assert_eq!(snapshot.references[0].state, "adopted");
    assert!(multipart::get_upload(&db, "lease-hot-cid").await.is_err());
}

#[tokio::test]
async fn legacy_mpu_periodic_maintenance_never_resurrects_expired_claims() {
    for table in ["standard_mutation_leases", "zip_root_builds"] {
        let db = setup().await;
        let (request, batch, guard, target) = fixture(&db, "lease-expire-inside", true).await;
        let before = storage::snapshot(&db, "lease-expire-inside")
            .await
            .unwrap()
            .unwrap();
        let lease_before = standard_mutation_lease::Entity::find()
            .one(&db)
            .await
            .unwrap();
        db.execute_unprepared(&format!("CREATE TRIGGER expire_during_publication AFTER INSERT ON objects BEGIN UPDATE {table} SET lease_until='2000-01-01T00:00:00Z'; END")).await.unwrap();
        storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
            .lock()
            .unwrap()
            .insert("lease-expire-inside".into());
        let result = publish(&db, request, batch, guard, target).await;
        storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
            .lock()
            .unwrap()
            .remove("lease-expire-inside");
        assert!(result.is_err(), "{table}");
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .one(&db)
                .await
                .unwrap(),
            lease_before
        );
        assert_eq!(
            storage::snapshot(&db, "lease-expire-inside")
                .await
                .unwrap()
                .unwrap()
                .builds,
            before.builds
        );
        assert_eq!(rows::<object::Entity>(&db).await, 0);
        assert_eq!(rows::<object_version::Entity>(&db).await, 0);
        assert!(
            multipart::get_upload(&db, "lease-expire-inside")
                .await
                .is_ok()
        );
    }
}

#[tokio::test]
async fn legacy_mpu_rejects_stale_standard_and_root_identities_without_publication() {
    for invalid in [
        "expired",
        "missing",
        "owner",
        "generation",
        "destination",
        "prefix",
        "root-expired",
        "root-worker",
        "root-revision",
        "root-epoch",
        "root-unknown",
        "root-failed",
        "root-batch",
    ] {
        let db = setup().await;
        let (request, mut batch, mut guard, target) = fixture(&db, "lease-invalid", true).await;
        match invalid {
            "expired" => {
                db.execute_unprepared(
                    "UPDATE standard_mutation_leases SET lease_until='2000-01-01T00:00:00Z'",
                )
                .await
                .unwrap();
            }
            "missing" => {
                db.execute_unprepared("DELETE FROM standard_mutation_leases")
                    .await
                    .unwrap();
            }
            "owner" => {
                db.execute_unprepared("UPDATE import_destinations SET mutation_id='replacement'")
                    .await
                    .unwrap();
            }
            "generation" => guard.expected_generation += 1,
            "destination" => guard.key = "different.zip".into(),
            "prefix" => guard.mutation_prefix = Some("different/".into()),
            "root-expired" => {
                db.execute_unprepared(
                    "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z'",
                )
                .await
                .unwrap();
            }
            "root-unknown" | "root-failed" => {
                db.execute_unprepared(&format!("UPDATE zip_root_builds SET status='{0}', error_code=CASE WHEN '{0}'='failed' THEN 'injected' ELSE NULL END", invalid.strip_prefix("root-").unwrap())).await.unwrap();
            }
            _ => {
                let storage::RootOutcome::Verified { claim, .. } = &mut batch.root_outcome else {
                    unreachable!()
                };
                match invalid {
                    "root-worker" => claim.worker = "different".into(),
                    "root-revision" => claim.revision += 1,
                    "root-epoch" => claim.epoch += 1,
                    _ => claim.batch_id = "different-batch".into(),
                }
            }
        }
        let lease_before = standard_mutation_lease::Entity::find()
            .one(&db)
            .await
            .unwrap();
        let before = storage::snapshot(&db, "lease-invalid")
            .await
            .unwrap()
            .unwrap();
        assert!(
            publish(&db, request, batch, guard, target).await.is_err(),
            "{invalid}"
        );
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .one(&db)
                .await
                .unwrap(),
            lease_before,
            "{invalid}"
        );
        let after = storage::snapshot(&db, "lease-invalid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.builds, before.builds, "{invalid}");
        assert_eq!(after.batch.state, "open");
        assert_eq!(rows::<object::Entity>(&db).await, 0);
        assert_eq!(rows::<object_version::Entity>(&db).await, 0);
        assert!(multipart::get_upload(&db, "lease-invalid").await.is_ok());
        assert_eq!(
            multipart::list_parts(&db, "lease-invalid")
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn legacy_mpu_without_verified_outcome_does_not_renew_another_root_claim() {
    let db = setup().await;
    let (request, mut batch, guard, target) = fixture(&db, "lease-no-root-owner", true).await;
    let before = storage::snapshot(&db, "lease-no-root-owner")
        .await
        .unwrap()
        .unwrap();
    batch.root_outcome = storage::RootOutcome::Failed {
        code: "root_intent_failed",
    };
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .insert("lease-no-root-owner".into());
    let result = publish(&db, request, batch, guard, target).await;
    storage::publication_lease::RENEW_EVERY_CHECKPOINT_FOR_BATCH
        .lock()
        .unwrap()
        .remove("lease-no-root-owner");
    result.unwrap();
    let after = storage::snapshot(&db, "lease-no-root-owner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch.state, "published");
    assert_eq!(after.batch.root_status, "failed");
    assert_eq!(after.builds, before.builds);
    assert_eq!(after.references[0].state, "retained");
    assert_eq!(rows::<object_version::Entity>(&db).await, 3);
    assert!(
        multipart::get_upload(&db, "lease-no-root-owner")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_mpu_failed_publication_rolls_back_renewals_and_all_effects() {
    for failure in ["object", "allocation", "root", "settlement"] {
        let db = setup().await;
        let (request, batch, guard, target) = fixture(&db, "lease-rollback", true).await;
        let before = storage::snapshot(&db, "lease-rollback")
            .await
            .unwrap()
            .unwrap();
        let lease = standard_mutation_lease::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let trigger = match failure {
            "object" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON objects BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
            "allocation" => {
                "CREATE TRIGGER fail_publication BEFORE INSERT ON pin_leases BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
            "root" => {
                "CREATE TRIGGER fail_publication BEFORE UPDATE ON zip_root_references WHEN NEW.state='adopted' BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
            _ => {
                "CREATE TRIGGER fail_publication BEFORE DELETE ON standard_mutation_leases BEGIN SELECT RAISE(ABORT,'injected'); END"
            }
        };
        db.execute_unprepared(trigger).await.unwrap();
        assert!(
            publish(&db, request, batch, guard, target).await.is_err(),
            "{failure}"
        );
        let after = storage::snapshot(&db, "lease-rollback")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.builds[0].lease_until, before.builds[0].lease_until);
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            lease
        );
        assert_eq!(after.batch.state, "open");
        assert!(after.entries.iter().all(|e| e.version_row_id.is_none()));
        assert_eq!(after.references[0].state, "retained");
        assert_eq!(rows::<object::Entity>(&db).await, 0);
        assert_eq!(rows::<object_version::Entity>(&db).await, 0);
        assert_eq!(rows::<pin_lease::Entity>(&db).await, 0);
        assert_eq!(rows::<pin_lease_target::Entity>(&db).await, 0);
        assert_eq!(rows::<pin_provider_usage::Entity>(&db).await, 0);
        assert_eq!(rows::<remote_pin::Entity>(&db).await, 0);
        assert_eq!(rows::<pin_job::Entity>(&db).await, 0);
        assert_eq!(
            rows::<crate::store::entities::physical_residency::Entity>(&db).await,
            0
        );
        assert_eq!(
            rows::<crate::store::entities::version_residency::Entity>(&db).await,
            0
        );
        assert_eq!(
            rows::<crate::store::entities::residency_reference::Entity>(&db).await,
            0
        );
        let receipt = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT archive_cid FROM zip_mpu_replays WHERE batch_id='lease-rollback'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        assert!(
            receipt
                .try_get::<Option<String>>("", "archive_cid")
                .unwrap()
                .is_none()
        );
        assert!(multipart::get_upload(&db, "lease-rollback").await.is_ok());
        assert_eq!(
            multipart::list_parts(&db, "lease-rollback")
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
