#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}

use std::{panic::AssertUnwindSafe, sync::Arc};

use chrono::Utc;
use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{
    import::{
        ImportConfig, ImportExecutionError, ImportFailureCode,
        downloader::{SourceDownloader, StrictPublicAddressPolicy},
        pipeline::{ImportCoordinator, ImportExecutionObserver, JobCancellation, execute_job},
    },
    pinning::{
        tags::{ObjectTag, PinControl},
        zip_policy::CapturedZipIntent,
    },
    store::{
        entities::import_job,
        import::jobs::{self, ClaimedImportJob},
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use support::{
    stage9_zip_replay::{Entry, Fixture, Outcome, run},
    stage9_zip_replay_requests::{self as request, Ledger},
    stage9_zip_replay_source::{ORIGIN, URL},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Standalone {
    Absent,
    Matching,
    CancelConflict,
    MissingAuthority,
}

fn standalone(capture: &CapturedZipIntent, state: Standalone) -> Option<String> {
    if matches!(state, Standalone::Absent | Standalone::MissingAuthority) {
        return None;
    }
    let mut decision = capture.source_decision().clone();
    if matches!(state, Standalone::CancelConflict) {
        assert_eq!(decision.control, PinControl::Absent);
        decision.replay_policy(Vec::new()).unwrap();
        decision.control = PinControl::Cancel;
        decision
            .replay_policy(vec![ObjectTag::new("ipfs-s3:pin", "false")])
            .unwrap();
        assert_eq!(decision.origin, *capture.origin());
        assert_ne!(decision.control, capture.source_decision().control);
    }
    decision.validate_snapshot().unwrap();
    Some(serde_json::to_string(&decision).unwrap())
}

async fn import_decision(db: &DatabaseConnection, id: &str, json: Option<String>) {
    let updated = db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE import_job_requests SET pin_decision_json=? WHERE job_id=?",
            [json.into(), id.into()],
        ))
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
}

async fn claim_import(h: &Fixture) -> ClaimedImportJob {
    let accepted = request::import(h, &request::url_xml(URL), HeaderMap::new()).await;
    assert_eq!(accepted.status, StatusCode::ACCEPTED, "{accepted:?}");
    let now = Utc::now();
    let claimed = jobs::claim_due(
        h.state.store.db(),
        "decision-check",
        now,
        now + chrono::Duration::seconds(60),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(claimed.job.id, accepted.id(true));
    claimed
}

fn cancellation() -> JobCancellation {
    JobCancellation {
        shutdown: CancellationToken::new(),
        ownership_lost: CancellationToken::new(),
    }
}

#[tokio::test]
async fn import_execution_rejects_conflicting_valid_decisions_before_source_io() {
    for state in [
        Standalone::CancelConflict,
        Standalone::MissingAuthority,
        Standalone::Absent,
        Standalone::Matching,
    ] {
        run(Entry::Import, Outcome::Complete, |h| async move {
            let result = AssertUnwindSafe(async {
                let mut claimed = claim_import(&h).await;
                let capture =
                    CapturedZipIntent::decode_json(claimed.job.zip_intent_json.as_deref().unwrap())
                        .unwrap();
                import_decision(
                    h.state.store.db(),
                    &claimed.job.id,
                    standalone(&capture, state),
                )
                .await;
                if matches!(state, Standalone::MissingAuthority) {
                    h.state
                        .store
                        .db()
                        .execute(Statement::from_sql_and_values(
                            DatabaseBackend::Sqlite,
                            "UPDATE import_job_requests SET zip_intent_json=NULL WHERE job_id=?",
                            [claimed.job.id.clone().into()],
                        ))
                        .await
                        .unwrap();
                }
                claimed.job = import_job::Entity::find_by_id(&claimed.job.id)
                    .one(h.state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
                let before = Ledger::read(&h).await;
                let rpc = h.kubo.received_requests().await.unwrap().len();
                let outcome = execute_job(
                    h.imports.clone(),
                    h.state.clone(),
                    claimed.job,
                    claimed.claim,
                    cancellation(),
                )
                .await;
                if matches!(
                    state,
                    Standalone::CancelConflict | Standalone::MissingAuthority
                ) {
                    assert!(
                        matches!(outcome, Err(ImportExecutionError::Terminal(failure))
                        if failure.code == ImportFailureCode::PublicationFailed)
                    );
                    assert_eq!(h.source.calls(), 0);
                    assert_eq!(h.kubo.received_requests().await.unwrap().len(), rpc);
                    assert_eq!(Ledger::read(&h).await, before);
                } else {
                    outcome.unwrap();
                    assert_eq!(h.source.calls(), 1);
                    assert_eq!(
                        request::get(&h, "out/file.txt").await,
                        (StatusCode::OK, b"single entry bytes".to_vec())
                    );
                }
            })
            .catch_unwind()
            .await;
            (h, result)
        })
        .await;
    }
}

#[tokio::test]
async fn mpu_complete_rejects_conflicting_valid_decisions_before_assembly_io() {
    for state in [
        Standalone::CancelConflict,
        Standalone::MissingAuthority,
        Standalone::Absent,
        Standalone::Matching,
    ] {
        run(Entry::Mpu, Outcome::Complete, |h| async move {
            let result = AssertUnwindSafe(async {
                let created = request::create(&h, HeaderMap::new(), "out/").await;
                assert_eq!(created.status, StatusCode::OK, "{created:?}");
                let id = created.upload_id();
                let etag = request::upload(&h, &id, Outcome::Complete.archive()).await;
                let upload = ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                    .await
                    .unwrap();
                let capture =
                    CapturedZipIntent::decode_json(upload.zip_intent_json.as_deref().unwrap())
                        .unwrap();
                h.state
                    .store
                    .db()
                    .execute(Statement::from_sql_and_values(
                        DatabaseBackend::Sqlite,
                        "UPDATE multipart_uploads SET pin_decision_json=? WHERE upload_id=?",
                        [standalone(&capture, state).into(), id.clone().into()],
                    ))
                    .await
                    .unwrap();
                if matches!(state, Standalone::MissingAuthority) {
                    h.state
                        .store
                        .db()
                        .execute(Statement::from_sql_and_values(
                            DatabaseBackend::Sqlite,
                            "UPDATE multipart_uploads SET zip_intent_json=NULL WHERE upload_id=?",
                            [id.clone().into()],
                        ))
                        .await
                        .unwrap();
                }
                let rpc = h.kubo.received_requests().await.unwrap().len();
                let before = Ledger::read(&h).await;
                let completed = request::complete(&h, &id, &etag, HeaderMap::new()).await;
                if matches!(
                    state,
                    Standalone::CancelConflict | Standalone::MissingAuthority
                ) {
                    assert!(!completed.status.is_success(), "{completed:?}");
                    assert_eq!(h.kubo.received_requests().await.unwrap().len(), rpc);
                    assert_eq!(Ledger::read(&h).await, before);
                    assert!(
                        ipfs_s3_gateway::store::multipart::get_upload(h.state.store.db(), &id)
                            .await
                            .is_ok()
                    );
                } else {
                    assert_eq!(completed.status, StatusCode::OK, "{completed:?}");
                    assert!(completed.body.contains("<Key>out/file.txt</Key>"));
                    assert_eq!(
                        request::get(&h, "out/file.txt").await,
                        (StatusCode::OK, b"single entry bytes".to_vec())
                    );
                }
            })
            .catch_unwind()
            .await;
            (h, result)
        })
        .await;
    }
}

struct ConflictBeforePublication {
    db: DatabaseConnection,
    decision: String,
    reached: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl ImportExecutionObserver for ConflictBeforePublication {
    async fn before_publication(&self, id: &str) {
        import_decision(&self.db, id, Some(self.decision.clone())).await;
        self.reached
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn import_final_publication_rereads_decisions_after_preparation() {
    run(Entry::Import, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
            let claimed = claim_import(&h).await;
            let capture =
                CapturedZipIntent::decode_json(claimed.job.zip_intent_json.as_deref().unwrap())
                    .unwrap();
            let observer = Arc::new(ConflictBeforePublication {
                db: h.state.store.db().clone(),
                decision: standalone(&capture, Standalone::CancelConflict).unwrap(),
                reached: std::sync::atomic::AtomicBool::new(false),
            });
            let config = ImportConfig {
                allowed_https_origins: vec![ORIGIN.into()],
                ..Default::default()
            }
            .validate()
            .unwrap();
            let downloader = SourceDownloader::with_components(
                Arc::new(config.clone()),
                h.source.clone(),
                Arc::new(StrictPublicAddressPolicy),
                h.source.clone(),
            );
            let coordinator =
                ImportCoordinator::new_with_observer(config, downloader, observer.clone());
            let before = Ledger::read(&h).await;
            let id = claimed.job.id.clone();
            assert!(
                execute_job(
                    coordinator,
                    h.state.clone(),
                    claimed.job,
                    claimed.claim,
                    cancellation()
                )
                .await
                .is_err()
            );
            assert!(observer.reached.load(std::sync::atomic::Ordering::SeqCst));
            assert_eq!(h.source.calls(), 1);
            assert_eq!(Ledger::read(&h).await, before);
            let job = import_job::Entity::find_by_id(&id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert!(job.final_cid.is_none());
            assert_ne!(job.state, "completed");
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}
