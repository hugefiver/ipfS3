use super::{RootResult, prepare_and_build_root};
use crate::{
    crypto::key::MasterKey,
    import::{ImportExecutionError, ImportSource, pipeline::JobCancellation, test_support},
    kubo::KuboClient,
    pinning::coordinator::PinningCoordinator,
    state::AppState,
    store::{
        self, Store,
        entities::{import_job_result, object},
        import::{
            jobs::{self, NewImportJob},
            ownership,
        },
        zip::{self, BatchAdmission, RootOutcome},
    },
    zip::batch::{FinalZipManifest, ManifestFile},
};
use chrono::Utc;
use sea_orm::{ConnectionTrait, Database, EntityTrait, PaginatorTrait, Statement};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wiremock::MockServer;

#[tokio::test]
async fn import_recovery_reuses_only_unreleased_verified_root_before_publication() {
    let server = MockServer::start().await;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let state = AppState {
        kubo: KuboClient::new(server.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: Default::default(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: PinningCoordinator::disabled_for_test(),
    };
    let db = state.store.db();
    let request = NewImportJob {
        id: "released-root-import".into(),
        bucket: "bucket".into(),
        key: "archive.zip".into(),
        source: ImportSource::Cid(
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
        ),
        request_fingerprint: "root-recovery-fingerprint".into(),
        client_token: None,
        object_content_type: Some("application/zip".into()),
        metadata: Default::default(),
        tags: vec![],
        decompress_prefix: Some("out/".into()),
    };
    let capture = test_support::capture_legacy(&state, &request, true);
    let root_capture = capture.controls().root;
    let now = Utc::now();
    ownership::submit_captured_zip(db, request, capture.clone(), now)
        .await
        .unwrap();
    let claimed = jobs::claim_due(
        db,
        "recovery-import-worker",
        now,
        now + chrono::Duration::seconds(120),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    let job = claimed.job;
    let claim = claimed.claim;
    zip::admit(db, &BatchAdmission {
        id: job.id.clone(), owner: capture.origin().principal_id.clone(), source: "import".into(),
        token: job.id.clone(), fingerprint: job.request_fingerprint.clone(),
        bucket: job.bucket.clone(), archive_key: job.key.clone(), input_identity: job.id.clone(),
        captured_options: serde_json::json!({"zip_intent": capture, "target_prefix": "out/", "root_capture": root_capture}).to_string(),
    }).await.unwrap();
    let manifest = FinalZipManifest {
        successful: vec![ManifestFile {
            relative_path: "file.txt".into(),
            object_key: "out/file.txt".into(),
            cid: "leaf".into(),
            size: 1,
        }],
        ..Default::default()
    };
    zip::prepare_manifest(db, &job.id, &manifest.manifest_items())
        .await
        .unwrap();
    let root_claim = zip::claim_root(db, &job.id, "prior-root-worker", 120)
        .await
        .unwrap();
    zip::mark_invoked(db, &root_claim).await.unwrap();
    zip::retain_candidate(db, &root_claim, "local-node", "hot", "root")
        .await
        .unwrap();
    zip::verify_root(
        db,
        &root_claim,
        "local-node",
        "hot",
        "root",
        "verified-dag-proof",
    )
    .await
    .unwrap();
    let cancellation = JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    };

    // Exercise the recovery fast path itself with a different import worker,
    // not a surrogate query for whether a reference is active.
    let recovered = tokio::time::timeout(
        Duration::from_secs(5),
        prepare_and_build_root(&state, &job, &claim, &manifest, root_capture, &cancellation),
    )
    .await
    .expect("an active verified root is reused without rebuilding")
    .unwrap();
    let RootResult::Unclaimed(RootOutcome::Verified {
        claim: reused,
        node_identity,
        tier,
        cid,
    }) = recovered
    else {
        panic!("verified unpublished root must be recovered");
    };
    assert_eq!(reused.batch_id, root_claim.batch_id);
    assert_eq!(reused.revision, root_claim.revision);
    assert_eq!(reused.epoch, root_claim.epoch);
    assert_eq!(reused.worker, root_claim.worker);
    assert_eq!(
        (node_identity.as_str(), tier.as_str(), cid.as_str()),
        ("local-node", "hot", "root")
    );
    let verified = zip::snapshot(db, &job.id).await.unwrap().unwrap();
    assert_eq!(verified.batch.state, "open");
    assert_eq!(verified.builds[0].status, "verified");
    assert_eq!(verified.references[0].state, "retained");
    assert!(verified.references[0].logical_released_at.is_none());

    let released_at = Utc::now();
    let released = db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE zip_root_references SET logical_released_at=$1 WHERE batch_id=$2 AND revision=$3 AND epoch=$4 AND node_identity=$5 AND tier=$6 AND cid=$7 AND logical_released_at IS NULL",
        vec![released_at.into(), root_claim.batch_id.clone().into(), root_claim.revision.into(), root_claim.epoch.into(), "local-node".into(), "hot".into(), "root".into()],
    )).await.unwrap();
    assert_eq!(released.rows_affected(), 1);
    let before = zip::snapshot(db, &job.id).await.unwrap().unwrap();
    assert!(before.builds[0].lease_until > released_at + chrono::Duration::seconds(5));
    assert_eq!(before.references[0].logical_released_at, Some(released_at));

    // A live root lease prevents a successor. Recovery must wait for that
    // authority instead of returning the released reference as Verified.
    let recovery =
        prepare_and_build_root(&state, &job, &claim, &manifest, root_capture, &cancellation);
    tokio::pin!(recovery);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut recovery)
            .await
            .is_err(),
        "released reference cannot satisfy the verified-root recovery fast path"
    );
    cancellation.ownership_lost.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), recovery)
        .await
        .unwrap();
    assert!(matches!(result, Err(ImportExecutionError::Superseded)));

    let after = zip::snapshot(db, &job.id).await.unwrap().unwrap();
    assert_eq!(after.batch, before.batch);
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.builds, before.builds);
    assert_eq!(after.references, before.references);
    assert!(after.batch.terminal_result.is_none());
    assert!(
        after
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );
    assert_eq!(object::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(
        import_job_result::Entity::find().count(db).await.unwrap(),
        0
    );
    assert!(server.received_requests().await.unwrap().is_empty());
}
