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
        ledger::capture_reservation(&db, PROVIDER, CID)
            .await
            .unwrap();
        jobs::enqueue_job(
            &db,
            jobs::submit_job(PROVIDER, CID, "lease", "target", 1, now),
        )
        .await
        .unwrap();
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
