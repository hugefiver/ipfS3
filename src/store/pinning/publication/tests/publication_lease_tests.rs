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
    fixture_with_entries(db, id, root, 2).await
}

async fn fixture_with_entries(
    db: &DatabaseConnection,
    id: &str,
    root: bool,
    entry_count: usize,
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
        vec![
            tag("ipfs-s3:pin", "true"),
            tag("ipfs-s3:content", "decompressed"),
        ],
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
    for index in 2..entry_count {
        let (cid, size) = match index % 80 {
            0 => ("bafy-a".to_owned(), 3),
            1 => ("bafy-b".to_owned(), 4),
            value => (format!("bafy-{value}"), 3),
        };
        request.entries.push(object(
            &format!("lease-{index:04}"),
            &format!("out/{index:04}"),
            &cid,
            size,
        ));
    }
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
    let decided =
        zip_fixture::current_snapshot(db, &batch.batch_id, &request.archive, &limits()).await;
    publication_attempt(
        db,
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
        Some(decided),
        Some(zip_fixture::replay()),
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
        format!("SELECT COUNT(*) AS n FROM zip_mpu_replays WHERE batch_id='lease-success' AND archive_cid='{}' AND response_xml IS NOT NULL", fixture_cid("bafy-archive")))).await.unwrap().unwrap();
    assert_eq!(receipt.try_get::<i64>("", "n").unwrap(), 1);
}

#[tokio::test]
async fn large_zip_keeps_the_original_claim_and_binds_each_version() {
    let db = setup().await;
    let (request, batch, guard, target) =
        fixture_with_entries(&db, "lease-periodic", true, 901).await;
    publish(&db, request, batch, guard, target).await.unwrap();
    let snapshot = storage::snapshot(&db, "lease-periodic")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.entries.len(), 901);
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_some())
    );
    assert_eq!(snapshot.references[0].state, "adopted");
    assert_eq!(targets_for_object(&db, "lease-archive").await.len(), 160);
    assert_eq!(rows::<object::Entity>(&db).await, 902);
    assert_eq!(rows::<object_version::Entity>(&db).await, 902);
}

#[tokio::test]
async fn lost_guard_rolls_back_shared_cid_references() {
    let db = setup().await;
    publish_object(
        &db,
        automatic_and_manual_request("hot-owner", "prior", "bafy-a"),
        &limits(),
    )
    .await
    .unwrap();
    let prior = targets_for_object(&db, "hot-owner").await;
    let (request, batch, guard, target) = fixture(&db, "shared-root", true).await;
    db.execute_unprepared("CREATE TRIGGER lose_guard AFTER INSERT ON pin_refs WHEN NEW.owner_object_id='lease-archive' BEGIN UPDATE standard_mutation_leases SET lease_until='2000-01-01T00:00:00Z'; END").await.unwrap();
    assert!(publish(&db, request, batch, guard, target).await.is_err());
    assert_eq!(targets_for_object(&db, "hot-owner").await, prior);
    assert!(targets_for_object(&db, "lease-archive").await.is_empty());
    assert_eq!(
        storage::snapshot(&db, "shared-root")
            .await
            .unwrap()
            .unwrap()
            .batch
            .state,
        "open"
    );
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
                "CREATE TRIGGER fail_publication BEFORE INSERT ON pin_refs BEGIN SELECT RAISE(ABORT,'injected'); END"
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
        assert_eq!(sql_count(&db, "SELECT COUNT(*) n FROM pin_refs").await, 0);
        assert_eq!(
            sql_count(&db, "SELECT COUNT(*) n FROM pin_resources").await,
            0
        );
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
