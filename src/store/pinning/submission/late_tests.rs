use super::*;
use crate::{
    pinning::{
        identity::ProviderIdentity,
        provider::{ProviderErrorClass, RemotePin},
    },
    store::{
        self,
        pinning::{leases, ledger},
    },
};
use chrono::Duration;
use sea_orm::DatabaseConnection;

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const PROVIDER: &str = "late-rpc";

struct Invocation {
    db: DatabaseConnection,
    claimed: ClaimedPinJob,
    id: String,
    now: DateTime<Utc>,
}

impl Invocation {
    async fn new(api: &str) -> Self {
        let db = store::connect_database("sqlite::memory:").await.unwrap();
        Self::with_database(db, api).await
    }

    async fn with_database(db: DatabaseConnection, api: &str) -> Self {
        store::run_migrations(&db).await.unwrap();
        let now = store::database_clock::database_now(&db).await.unwrap();
        let identity = ProviderIdentity::legacy(PROVIDER, "kubo", None, api, "cid");
        ledger::register_route(&db, PROVIDER, &identity)
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO buckets (name) VALUES ('bucket')")
            .await
            .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
             VALUES ('object', 'bucket', 'key', '{CID}', 100, '{CID}', TRUE)"
        ))
        .await
        .unwrap();
        pin_lease::Entity::insert(pin_lease::ActiveModel {
            id: Set("lease".into()),
            owner_object_id: Set("object".into()),
            source: Set("automatic".into()),
            policy_id: Set("policy".into()),
            provider_mode: Set("all".into()),
            content_mode: Set("full".into()),
            created_at: Set(now),
            last_touched_at: Set(now),
            expires_at: Set(now + Duration::hours(1)),
            generation: Set(1),
            state: Set("active".into()),
        })
        .exec(&db)
        .await
        .unwrap();
        pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
            id: Set("target".into()),
            lease_id: Set("lease".into()),
            cid: Set(CID.into()),
            logical_size: Set(100),
            provider: Set(PROVIDER.into()),
            state: Set("waiting".into()),
            created_at: Set(now),
            last_touched_at: Set(now),
        })
        .exec(&db)
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO remote_pins \
             (provider, cid, cid_size, status, epoch, failure_attempts, last_touched_at) \
             VALUES ('{PROVIDER}', '{CID}', 100, 'reserved', 1, 0, '{}')",
            now.to_rfc3339()
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO pin_provider_usage (provider, reserved_bytes, reserved_pins) \
             VALUES ('{PROVIDER}', 100, 1)"
        ))
        .await
        .unwrap();
        let preparation = db.begin().await.unwrap();
        ledger::capture_reservation(&preparation, PROVIDER, CID)
            .await
            .unwrap();
        jobs::enqueue_job(
            &preparation,
            jobs::submit_job(PROVIDER, CID, "lease", "target", 1, now),
        )
        .await
        .unwrap();
        preparation.commit().await.unwrap();
        let claimed = jobs::claim_due_jobs(&db, now, Duration::minutes(1), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let tx = db.begin().await.unwrap();
        assert_eq!(
            jobs::prepare_submit_call(&tx, &claimed, now).await.unwrap(),
            jobs::SubmitCallDecision::ReadyToCall
        );
        jobs::record_submit_invocation(&tx, &claimed, api, "cid", now)
            .await
            .unwrap();
        let id = begin(&tx, &claimed, now).await.unwrap().unwrap();
        tx.commit().await.unwrap();
        Self {
            db,
            claimed,
            id,
            now,
        }
    }

    async fn cancel(&self) {
        let tx = self.db.begin().await.unwrap();
        assert_eq!(
            leases::cancel_lease(&tx, "lease", self.now).await.unwrap(),
            leases::GenerationDecision::Current
        );
        tx.commit().await.unwrap();
        let lease = pin_lease::Entity::find_by_id("lease")
            .one(&self.db)
            .await
            .unwrap()
            .unwrap();
        let target = pin_lease_target::Entity::find_by_id("target")
            .one(&self.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.state, "cancelled");
        assert_eq!(lease.generation, 2);
        assert_eq!(target.state, "released");
    }

    async fn assert_record(&self, observation: SubmitObservation, projectable: bool) {
        let lease = pin_lease::Entity::find_by_id("lease")
            .one(&self.db)
            .await
            .unwrap();
        let target = pin_lease_target::Entity::find_by_id("target")
            .one(&self.db)
            .await
            .unwrap();
        let remote = remote_pin::Entity::find().one(&self.db).await.unwrap();
        let ledger = ledger::get(&self.db, PROVIDER, CID).await.unwrap();
        let jobs = pin_job::Entity::find()
            .order_by_asc(pin_job::Column::Id)
            .all(&self.db)
            .await
            .unwrap();
        let history = jobs::submission_history(&self.db, &self.claimed.model.id)
            .await
            .unwrap();
        let successful = observation
            .result
            .as_ref()
            .is_ok_and(|pin| pin.status != RemotePinStatus::Failed);
        assert_eq!(
            record(&self.db, &self.claimed, &self.id, &observation, self.now)
                .await
                .unwrap(),
            projectable
        );
        let saved = latest(&self.db, &self.claimed.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            saved.effect,
            if observation.effect == SubmitEffect::Observed {
                "observed"
            } else {
                "unknown"
            }
        );
        assert_eq!(
            saved.outcome,
            if successful {
                if observation.resources[0].resource_type == RemoteResourceType::ClusterPin {
                    "accepted"
                } else {
                    "matched"
                }
            } else {
                "unknown"
            }
        );
        assert_eq!(saved.needs_attention, !successful || !projectable);
        assert_eq!(saved.observed_at, Some(self.now));
        assert_eq!(
            saved.safe_error,
            observation
                .result
                .as_ref()
                .err()
                .map(|error| typed_error(error, observation.effect))
        );
        let resources: Vec<ResourceEvidence> = serde_json::from_str(&saved.resources).unwrap();
        assert_eq!(resources.len(), observation.resources.len());
        for (saved, actual) in resources.iter().zip(&observation.resources) {
            assert_eq!(&saved.resource, actual);
            assert!(saved.key.is_some());
        }
        assert_eq!(
            pin_lease::Entity::find_by_id("lease")
                .one(&self.db)
                .await
                .unwrap(),
            lease
        );
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target")
                .one(&self.db)
                .await
                .unwrap(),
            target
        );
        assert_eq!(
            remote_pin::Entity::find().one(&self.db).await.unwrap(),
            remote
        );
        assert_eq!(ledger::get(&self.db, PROVIDER, CID).await.unwrap(), ledger);
        assert_eq!(
            pin_job::Entity::find()
                .order_by_asc(pin_job::Column::Id)
                .all(&self.db)
                .await
                .unwrap(),
            jobs
        );
        assert_eq!(
            jobs::submission_history(&self.db, &self.claimed.model.id)
                .await
                .unwrap(),
            history
        );
        assert!(!dispatch_allowed(&self.db, &self.id).await.unwrap());
        let tx = self.db.begin().await.unwrap();
        assert!(jobs::record_submit_invocation(&tx, &self.claimed,
            if observation.resources.first().is_some_and(|resource| resource.resource_type == RemoteResourceType::ClusterPin) { "cluster-rest" } else { "kubo" },
            "cid", self.now).await.is_err());
        tx.rollback().await.unwrap();
    }
}

fn unknown() -> SubmitObservation {
    SubmitObservation {
        result: Err(ProviderError {
            class: ProviderErrorClass::Transient,
            message: "provider returned HTTP status 503".into(),
            retry_after: None,
        }),
        resources: vec![],
        effect: SubmitEffect::Unknown,
    }
}

#[tokio::test]
async fn cancelled_unknown_receipt_records_facts_without_projection() {
    for cancelled in [false, true] {
        let invocation = Invocation::new("kubo").await;
        if cancelled {
            invocation.cancel().await;
        }
        let tx = invocation.db.begin().await.unwrap();
        assert!(
            jobs::fence_job_claim(
                &tx,
                &invocation.claimed.model.id,
                invocation.claimed.model.locked_until.unwrap()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            jobs::check_target_job_generation(&tx, &invocation.claimed.model)
                .await
                .unwrap(),
            !cancelled
        );
        assert!(
            store::pinning::management::actions::job_authority::projection_allowed(
                &tx,
                &invocation.claimed.model
            )
            .await
            .unwrap()
        );
        tx.commit().await.unwrap();
        let projectable = record(
            &invocation.db,
            &invocation.claimed,
            &invocation.id,
            &unknown(),
            invocation.now,
        )
        .await
        .unwrap();
        let saved = latest(&invocation.db, &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.outcome, "unknown");
        assert_eq!(saved.effect, "unknown");
        assert!(saved.needs_attention);
        assert_eq!(saved.observed_at, Some(invocation.now));
        assert_eq!(projectable, !cancelled, "cancelled={cancelled}");
    }
}

fn observed(api: &str, success: bool) -> SubmitObservation {
    let cluster = api == "cluster-rest";
    SubmitObservation {
        result: Ok(RemotePin {
            request_id: CID.into(),
            cid: CID.into(),
            status: if !success {
                RemotePinStatus::Failed
            } else if cluster {
                RemotePinStatus::Pinning
            } else {
                RemotePinStatus::Pinned
            },
            raw_status: if success { "pinned" } else { "failed" }.into(),
            failure_reason: None,
        }),
        resources: vec![ObservedResource {
            resource_type: if cluster {
                RemoteResourceType::ClusterPin
            } else {
                RemoteResourceType::RpcPin
            },
            cid: CID.into(),
            request_id: CID.into(),
            status: if !success {
                ObservedResourceStatus::PinError
            } else if cluster {
                ObservedResourceStatus::PinAccepted
            } else {
                ObservedResourceStatus::RecursiveVerified
            },
            ownership: Ownership::Unknown,
            replica_evidence: None,
        }],
        effect: SubmitEffect::Observed,
    }
}

#[derive(Clone, Copy)]
enum Lifetime {
    Live,
    Cancelled,
    Expired,
    Elapsed,
    NewGeneration,
    ReleasedTarget,
}

async fn check_callbacks(lifetime: Lifetime) {
    for api in ["kubo", "cluster-rest"] {
        for observation in [unknown(), observed(api, false), observed(api, true)] {
            let invocation = Invocation::new(api).await;
            match lifetime {
                Lifetime::Live => {}
                Lifetime::Cancelled => invocation.cancel().await,
                Lifetime::Expired | Lifetime::Elapsed => {
                    pin_lease::Entity::update_many()
                        .col_expr(pin_lease::Column::ExpiresAt, Expr::value(invocation.now))
                        .filter(pin_lease::Column::Id.eq("lease"))
                        .exec(&invocation.db)
                        .await
                        .unwrap();
                    if matches!(lifetime, Lifetime::Expired) {
                        let tx = invocation.db.begin().await.unwrap();
                        assert_eq!(
                            leases::expire_due_leases(&tx, invocation.now)
                                .await
                                .unwrap(),
                            vec!["lease"]
                        );
                        tx.commit().await.unwrap();
                    }
                }
                Lifetime::NewGeneration => {
                    invocation
                        .db
                        .execute_unprepared("UPDATE pin_leases SET generation=2 WHERE id='lease'")
                        .await
                        .unwrap();
                }
                Lifetime::ReleasedTarget => {
                    invocation
                        .db
                        .execute_unprepared(
                            "UPDATE pin_lease_targets SET state='released' WHERE id='target'",
                        )
                        .await
                        .unwrap();
                }
            }
            invocation
                .assert_record(observation, matches!(lifetime, Lifetime::Live))
                .await;
        }
    }
}

#[tokio::test]
async fn live_rpc_and_cluster_callbacks_keep_projection_authority() {
    check_callbacks(Lifetime::Live).await;
}

#[tokio::test]
async fn cancelled_rpc_and_cluster_callbacks_are_facts_only() {
    check_callbacks(Lifetime::Cancelled).await;
}

#[tokio::test]
async fn expired_rpc_and_cluster_callbacks_are_facts_only() {
    check_callbacks(Lifetime::Expired).await;
    check_callbacks(Lifetime::Elapsed).await;
}

#[tokio::test]
async fn changed_generation_or_released_target_callbacks_are_facts_only() {
    check_callbacks(Lifetime::NewGeneration).await;
    check_callbacks(Lifetime::ReleasedTarget).await;
}

#[tokio::test]
async fn dispatched_rpc_unknown_callback_after_cancellation_is_facts_only() {
    use crate::pinning::{
        ipfs_rpc::{IpfsRpcProvider, RpcProfile, RpcProviderOptions, RpcStrategy, RpcTimeouts},
        provider::{PinningProvider, SubmitPin},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&target)
        .await;
    let provider = IpfsRpcProvider::from_config(
        PROVIDER.into(),
        target.uri(),
        None,
        &RpcProviderOptions {
            profile: RpcProfile::Kubo,
            strategy: RpcStrategy::Cid,
            auth: None,
            allow_private_network: true,
            timeouts: RpcTimeouts::default(),
        },
    )
    .unwrap();
    let invocation = Invocation::new("kubo").await;
    assert!(
        dispatch_allowed(&invocation.db, &invocation.id)
            .await
            .unwrap()
    );
    let observation = PinningProvider::submit_observed(
        &provider,
        SubmitPin {
            cid: CID.into(),
            name: invocation.claimed.model.id.clone(),
            metadata: Default::default(),
        },
    )
    .await;
    assert_eq!(observation.effect, SubmitEffect::Unknown);
    invocation.cancel().await;
    invocation.assert_record(observation, false).await;
    target.verify().await;
}

#[derive(Debug, PartialEq)]
struct ProjectionState {
    lease: pin_lease::Model,
    target: pin_lease_target::Model,
    remote: remote_pin::Model,
    ledger: remote_pin_ledger::Model,
    job: pin_job::Model,
    history: jobs::history::Model,
}

impl ProjectionState {
    async fn read(db: &DatabaseConnection, job: &str) -> Self {
        Self {
            lease: pin_lease::Entity::find_by_id("lease")
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            target: pin_lease_target::Entity::find_by_id("target")
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            remote: remote_pin::Entity::find_by_id((PROVIDER.to_owned(), CID.to_owned()))
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            ledger: ledger::get(db, PROVIDER, CID).await.unwrap().unwrap(),
            job: pin_job::Entity::find_by_id(job)
                .one(db)
                .await
                .unwrap()
                .unwrap(),
            history: jobs::submission_history(db, job).await.unwrap().unwrap(),
        }
    }
}

async fn check_unknown_projection_after_receipt_commit(lifetime: Lifetime) {
    use crate::pinning::{
        config::ValidatedPinningConfig,
        coordinator::PinningCoordinator,
        worker::{RPC_RECEIPT_COMMITTED, finish_rpc_observation_for_test},
    };
    use std::sync::Arc;
    use tokio::sync::Notify;

    let coordinator = PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&crate::config::Config::default_for_test(), |_| None)
            .unwrap(),
    )
    .unwrap();
    for api in ["kubo", "cluster-rest"] {
        for observation in [unknown(), observed(api, false)] {
            let Invocation {
                db, claimed, id, ..
            } = Invocation::new(api).await;
            let store = store::Store::new(db);
            let committed = Arc::new(Notify::new());
            let resume = Arc::new(Notify::new());
            let finish = RPC_RECEIPT_COMMITTED.scope(
                (committed.clone(), resume.clone()),
                Box::pin(finish_rpc_observation_for_test(
                    &store,
                    &coordinator,
                    claimed.clone(),
                    observation,
                    id.clone(),
                )),
            );
            let change_lifecycle = async {
                committed.notified().await;
                // Reading through the released connection and committing another
                // transaction proves this injection follows receipt commit.
                let facts = latest(store.db(), &claimed.model.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(facts.outcome, "unknown");
                assert!(facts.observed_at.is_some() && facts.needs_attention);
                let before = ProjectionState::read(store.db(), &claimed.model.id).await;
                assert_eq!(before.target.state, "waiting");
                assert_eq!(before.ledger.effect, "unknown");
                assert!(before.ledger.last_error.is_none());
                assert_eq!(before.history.state, "active");
                assert!(before.history.last_error.is_none());
                let tx = store.db().begin().await.unwrap();
                let now = store::database_clock::database_now(&tx).await.unwrap();
                match lifetime {
                    Lifetime::Live => {}
                    Lifetime::Cancelled => {
                        assert_eq!(
                            leases::cancel_lease(&tx, "lease", now).await.unwrap(),
                            leases::GenerationDecision::Current
                        );
                    }
                    Lifetime::Expired | Lifetime::Elapsed => {
                        pin_lease::Entity::update_many()
                            .col_expr(
                                pin_lease::Column::ExpiresAt,
                                Expr::value(now - Duration::seconds(1)),
                            )
                            .filter(pin_lease::Column::Id.eq("lease"))
                            .exec(&tx)
                            .await
                            .unwrap();
                        if matches!(lifetime, Lifetime::Expired) {
                            assert_eq!(
                                leases::expire_due_leases(&tx, now).await.unwrap(),
                                vec!["lease"]
                            );
                        }
                    }
                    Lifetime::NewGeneration => {
                        tx.execute_unprepared(
                            "UPDATE pin_leases SET generation=2 WHERE id='lease'",
                        )
                        .await
                        .unwrap();
                    }
                    Lifetime::ReleasedTarget => unreachable!(),
                }
                assert!(
                    jobs::fence_job_claim(
                        &tx,
                        &claimed.model.id,
                        claimed.model.locked_until.unwrap()
                    )
                    .await
                    .unwrap()
                );
                tx.commit().await.unwrap();
                let after_change = ProjectionState::read(store.db(), &claimed.model.id).await;
                resume.notify_one();
                (facts, after_change)
            };
            let (finished, (facts, after_change)) =
                tokio::time::timeout(std::time::Duration::from_secs(15), async {
                    tokio::join!(finish, change_lifecycle)
                })
                .await
                .expect("receipt commit/projection test did not reach its seam");
            assert!(finished.unwrap().is_none());
            assert_eq!(
                latest(store.db(), &claimed.model.id)
                    .await
                    .unwrap()
                    .unwrap(),
                facts
            );
            let after = ProjectionState::read(store.db(), &claimed.model.id).await;
            if matches!(lifetime, Lifetime::Live) {
                assert_eq!(after.target.state, "degraded");
                assert_eq!(after.ledger.effect, "unknown");
                assert!(after.ledger.last_error.is_some());
                assert_eq!(after.history.state, "needs_attention");
                assert!(after.history.last_error.is_some());
                assert_eq!(after.job.state, "running");
                assert!(after.job.locked_until.is_none());
                assert_eq!(after.job.submit_phase.as_deref(), Some("recovering"));
                assert_eq!(after.lease, after_change.lease);
                assert_eq!(after.remote, after_change.remote);
            } else {
                assert_eq!(
                    after.history.state, after_change.history.state,
                    "late callback must not park history after lifecycle loss"
                );
                assert_eq!(
                    after, after_change,
                    "a lifecycle change after receipt commit must prevent all late projection"
                );
            }
            assert!(!dispatch_allowed(store.db(), &id).await.unwrap());
            let tx = store.db().begin().await.unwrap();
            assert!(
                jobs::record_submit_invocation(&tx, &claimed, api, "cid", Utc::now())
                    .await
                    .is_err()
            );
            tx.rollback().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cancelled_after_receipt_commit_blocks_unknown_worker_projection() {
    check_unknown_projection_after_receipt_commit(Lifetime::Cancelled).await;
}

#[tokio::test]
async fn expired_after_receipt_commit_blocks_unknown_worker_projection() {
    check_unknown_projection_after_receipt_commit(Lifetime::Expired).await;
    check_unknown_projection_after_receipt_commit(Lifetime::Elapsed).await;
}

#[tokio::test]
async fn changed_generation_after_receipt_commit_blocks_unknown_worker_projection() {
    check_unknown_projection_after_receipt_commit(Lifetime::NewGeneration).await;
}

#[tokio::test]
async fn live_after_receipt_commit_keeps_unknown_worker_projection() {
    check_unknown_projection_after_receipt_commit(Lifetime::Live).await;
}

#[tokio::test]
async fn fact_completion_uses_original_identity_and_duplicate_callback_cannot_overwrite() {
    let invocation = Invocation::new("kubo").await;
    let original = capture_fact(&invocation.db, &invocation.claimed, &invocation.id)
        .await
        .unwrap();
    let mut wrong = invocation.claimed.clone();
    wrong.model.locked_until = Some(invocation.now + Duration::hours(2));
    assert!(
        capture_fact(&invocation.db, &wrong, &invocation.id)
            .await
            .is_err()
    );
    wrong = invocation.claimed.clone();
    wrong.model.provider = "another-provider".into();
    assert!(
        capture_fact(&invocation.db, &wrong, &invocation.id)
            .await
            .is_err()
    );
    let store = store::Store::new(invocation.db);
    let first = complete_fact(&store, &original, &observed("kubo", true), invocation.now)
        .await
        .unwrap();
    assert!(
        complete_fact(
            &store,
            &original,
            &unknown(),
            invocation.now + Duration::seconds(1)
        )
        .await
        .is_err()
    );
    assert_eq!(
        latest(store.db(), &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap(),
        first
    );
    assert!(
        first.needs_attention,
        "a fact alone cannot settle business authority"
    );
}

#[tokio::test]
async fn fact_completion_cas_rejects_changed_invocation_identity() {
    for assignment in [
        "id='different-invocation'",
        "job_id='different-job'",
        "submit_call=2",
        "provider='different-provider'",
        "expected_cid='different-cid'",
        "route='{}'",
        "remote_epoch=2",
        "claim_until='2099-01-01T00:00:00Z'",
    ] {
        let invocation = Invocation::new("kubo").await;
        let original = capture_fact(&invocation.db, &invocation.claimed, &invocation.id)
            .await
            .unwrap();
        invocation
            .db
            .execute_unprepared(&format!(
                "UPDATE main.pin_submit_observations SET {assignment}"
            ))
            .await
            .unwrap();
        let store = store::Store::new(invocation.db);
        assert!(
            complete_fact(&store, &original, &observed("kubo", true), invocation.now)
                .await
                .is_err(),
            "{assignment}"
        );
        let row = store
            .db()
            .query_one(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT observed_at FROM main.pin_submit_observations",
            ))
            .await
            .unwrap()
            .unwrap();
        assert!(
            row.try_get::<Option<DateTime<Utc>>>("", "observed_at")
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn fact_completion_is_main_qualified_and_independent_of_readiness() {
    let invocation = Invocation::new("kubo").await;
    let original = capture_fact(&invocation.db, &invocation.claimed, &invocation.id)
        .await
        .unwrap();
    invocation
        .db
        .execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    invocation.db.execute_unprepared("CREATE TEMP TABLE pin_submit_observations AS SELECT * FROM main.pin_submit_observations").await.unwrap();
    let store = store::Store::new(invocation.db);
    let first = complete_fact(&store, &original, &observed("kubo", true), invocation.now)
        .await
        .unwrap();
    assert_eq!(
        latest(store.db(), &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap(),
        first
    );
    let shadow = store
        .db()
        .query_one(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT observed_at FROM temp.pin_submit_observations",
        ))
        .await
        .unwrap()
        .unwrap();
    assert!(
        shadow
            .try_get::<Option<DateTime<Utc>>>("", "observed_at")
            .unwrap()
            .is_none()
    );
    assert_eq!(first.outcome, "matched");
    assert!(first.needs_attention);
}

#[tokio::test]
async fn generic_record_remains_atomic_with_its_callers_transaction() {
    let invocation = Invocation::new("kubo").await;
    let tx = invocation.db.begin().await.unwrap();
    assert!(
        record(
            &tx,
            &invocation.claimed,
            &invocation.id,
            &observed("kubo", true),
            invocation.now
        )
        .await
        .unwrap()
    );
    assert!(
        latest(&tx, &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap()
            .observed_at
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert!(
        latest(&invocation.db, &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap()
            .observed_at
            .is_none()
    );
}

async fn postgres_invocation() -> (Invocation, DatabaseConnection, String) {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL")
        .expect("explicit authorized isolated PostgreSQL endpoint required");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    assert!(matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1")
    ));
    assert_ne!(parsed.port(), Some(55467));
    let admin = store::connect_database(&url).await.unwrap();
    let schema = format!("rpc_fact_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(schema.clone());
    // TEMP objects and capture/completion must exercise the same connection.
    options.max_connections(1);
    let db = match sea_orm::Database::connect(options).await {
        Ok(db) => db,
        Err(error) => {
            drop_postgres_invocation_schema(&admin, &schema).await;
            admin.close().await.unwrap();
            panic!("isolated receipt fixture connection failed: {error}");
        }
    };
    match std::panic::AssertUnwindSafe(Invocation::with_database(db.clone(), "kubo"))
        .catch_unwind()
        .await
    {
        Ok(invocation) => (invocation, admin, schema),
        Err(panic) => {
            db.close().await.unwrap();
            drop_postgres_invocation_schema(&admin, &schema).await;
            admin.close().await.unwrap();
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
async fn bounded_candidate_pages_progress_past_inflight_and_unwakeable_history() {
    use sea_orm::IntoActiveModel;
    let invocation = Invocation::new("kubo").await;
    let captured = capture_fact(&invocation.db, &invocation.claimed, &invocation.id)
        .await
        .unwrap();
    let tx = invocation.db.begin().await.unwrap();
    jobs::park_submit(
        &tx,
        &invocation.claimed,
        "needs_attention",
        jobs::RPC_RECOVERY_PARK_REASON,
        invocation.now,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let store = store::Store::new(invocation.db);
    let receipt = complete_fact(&store, &captured, &observed("kubo", true), invocation.now)
        .await
        .unwrap();
    // Retained observation history can outlive its job. Such rows, including
    // incomplete invocations, must not prevent later pages from making progress.
    for (id, completed) in [("!first-inflight", false), ("!second-orphan", true)] {
        let mut history = receipt.clone().into_active_model();
        history.id = Set(id.into());
        history.job_id = Set(id.into());
        if !completed {
            history.effect = Set("unknown".into());
            history.outcome = Set("in_flight".into());
            history.observed_at = Set(None);
            history.resources = Set("[]".into());
        }
        evidence::Entity::insert(history)
            .exec(store.db())
            .await
            .unwrap();
    }
    let mut cursor = String::new();
    let mut found = Vec::new();
    for _ in 0..3 {
        let (next, candidates) = parked_receipt_candidates(store.db(), &cursor, 1)
            .await
            .unwrap();
        let next = next.expect("each page must advance past one persisted identity");
        assert!(next > cursor);
        cursor = next;
        found.extend(candidates);
    }
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].id, invocation.claimed.model.id);
    let work = crate::store::pinning::management::records::SharedDecodeBudget::default();
    let tx = store
        .begin_runtime(crate::store::pinning::management::records::DecodeWork::shared(&work))
        .await
        .unwrap();
    assert!(
        wake_runtime_parked(
            &tx,
            &found[0].id,
            invocation.now,
            crate::store::pinning::management::records::DecodeWork::shared(&work)
        )
        .await
        .unwrap()
    );
    tx.commit().await.unwrap();
    let awakened = pin_job::Entity::find_by_id(&found[0].id)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(awakened.state, "pending");
    assert_eq!(awakened.submit_phase.as_deref(), Some("recovering"));
    assert_eq!(
        latest(store.db(), &awakened.id).await.unwrap().unwrap(),
        receipt
    );
    assert!(
        parked_receipt_candidates(store.db(), &cursor, 1)
            .await
            .unwrap()
            .0
            .is_none()
    );
}

async fn finish_postgres_invocation(
    db: &DatabaseConnection,
    admin: DatabaseConnection,
    schema: &str,
    result: Result<(), Box<dyn std::any::Any + Send>>,
) {
    db.clone().close().await.unwrap();
    drop_postgres_invocation_schema(&admin, schema).await;
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn drop_postgres_invocation_schema(admin: &DatabaseConnection, schema: &str) {
    let suffix = schema
        .strip_prefix("rpc_fact_")
        .expect("task-owned schema required");
    assert!(suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "opt-in: requires a new authorized isolated IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL"]
async fn postgres_fact_capture_and_completion_bind_persistent_custom_schema_not_temp() {
    use futures_util::FutureExt;
    let (invocation, admin, schema) = postgres_invocation().await;
    let result = std::panic::AssertUnwindSafe(async {
        invocation.db.execute_unprepared(&format!("CREATE TEMP TABLE pin_submit_observations AS SELECT * FROM \"{schema}\".pin_submit_observations")).await.unwrap();
        invocation.db.execute_unprepared("UPDATE pg_temp.pin_submit_observations SET route='{}'").await.unwrap();
        let captured = capture_fact(&invocation.db, &invocation.claimed, &invocation.id).await.unwrap();
        assert_eq!(captured.relation.postgres_identity.as_ref().unwrap().0, schema);
        invocation.db.execute_unprepared("SET search_path=pg_temp,public").await.unwrap();
        let receipt = complete_fact(&store::Store::new(invocation.db.clone()), &captured, &observed("kubo", true), invocation.now).await.unwrap();
        let persistent = invocation.db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Postgres, format!("SELECT * FROM \"{schema}\".pin_submit_observations"))).await.unwrap().unwrap();
        assert_eq!(evidence::Model::from_query_result(&persistent, "").unwrap(), receipt);
        let shadow = invocation.db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Postgres, "SELECT observed_at FROM pg_temp.pin_submit_observations")).await.unwrap().unwrap();
        assert!(shadow.try_get::<Option<DateTime<Utc>>>("", "observed_at").unwrap().is_none());

        // A replacement relation with copied invocation fields is not the
        // persistent relation captured before dispatch.
        invocation.db.execute_unprepared(&format!("ALTER TABLE \"{schema}\".pin_submit_observations RENAME TO saved_receipts; CREATE TABLE \"{schema}\".pin_submit_observations AS SELECT * FROM \"{schema}\".saved_receipts; UPDATE \"{schema}\".pin_submit_observations SET observed_at=NULL")).await.unwrap();
        assert!(complete_fact(&store::Store::new(invocation.db.clone()), &captured, &unknown(), invocation.now).await.is_err());
        let replacement = invocation.db.query_one(Statement::from_string(sea_orm::DatabaseBackend::Postgres, format!("SELECT observed_at FROM \"{schema}\".pin_submit_observations"))).await.unwrap().unwrap();
        assert!(replacement.try_get::<Option<DateTime<Utc>>>("", "observed_at").unwrap().is_none());
    }).catch_unwind().await;
    finish_postgres_invocation(&invocation.db, admin, &schema, result).await;
}

#[tokio::test]
#[ignore = "opt-in: requires a new authorized isolated IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL"]
async fn postgres_generic_record_authority_sql_error_is_error_and_rolls_back_receipt() {
    use futures_util::FutureExt;
    let (invocation, admin, schema) = postgres_invocation().await;
    let result = std::panic::AssertUnwindSafe(async {
        let direct = RPC_AUTHORITY_SQL_FAILURE
            .scope(
                (),
                record(
                    &invocation.db,
                    &invocation.claimed,
                    &invocation.id,
                    &unknown(),
                    invocation.now,
                ),
            )
            .await
            .expect_err(
                "top-level record must not accept PostgreSQL COMMIT-as-ROLLBACK as Ok(false)",
            );
        assert!(
            direct
                .to_string()
                .contains("rpc_receipt_missing_authority_relation"),
            "the injected authority relation must cause the error: {direct}"
        );
        let receipt = latest(&invocation.db, &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.outcome, "in_flight");
        assert!(receipt.observed_at.is_none() && receipt.needs_attention);

        let tx = invocation.db.begin().await.unwrap();
        let result = RPC_AUTHORITY_SQL_FAILURE
            .scope(
                (),
                record(
                    &tx,
                    &invocation.claimed,
                    &invocation.id,
                    &unknown(),
                    invocation.now,
                ),
            )
            .await;
        let error =
            result.expect_err("an aborted PostgreSQL transaction is not a projection denial");
        assert!(
            error
                .to_string()
                .contains("rpc_receipt_missing_authority_relation"),
            "the injected authority relation must cause the error: {error}"
        );
        // The generic call's savepoint rollback keeps its caller usable and
        // atomic, rather than accepting PostgreSQL's COMMIT-as-ROLLBACK reply.
        assert!(
            latest(&tx, &invocation.claimed.model.id)
                .await
                .unwrap()
                .unwrap()
                .observed_at
                .is_none()
        );
        tx.commit().await.unwrap();
        let receipt = latest(&invocation.db, &invocation.claimed.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.outcome, "in_flight");
        assert!(receipt.observed_at.is_none() && receipt.needs_attention);
    })
    .catch_unwind()
    .await;
    finish_postgres_invocation(&invocation.db, admin, &schema, result).await;
}
