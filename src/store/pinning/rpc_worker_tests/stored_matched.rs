use super::*;
use futures_util::FutureExt;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DatabaseTransaction, Statement};
use std::panic::{AssertUnwindSafe, resume_unwind};

#[tokio::test]
#[ignore = "opt-in: requires an authorized isolated IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL"]
async fn stored_matched_settles_while_unrelated_rows_locked() {
    with_postgres(false).await;
}

#[tokio::test]
#[ignore = "opt-in: requires an authorized isolated IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL"]
async fn stored_matched_settles_while_unrelated_rows_locked_after_reclaim() {
    with_postgres(true).await;
}

async fn with_postgres(reclaim: bool) {
    let url = std::env::var("IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL")
        .expect("authorized isolated PostgreSQL endpoint required");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    assert!(matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1")
    ));
    assert_ne!(parsed.port(), Some(55467));
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("rpc_matched_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();

    let mut scoped_db = None;
    let result = AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(url);
        options
            .max_connections(4)
            .set_schema_search_path(schema.clone());
        let db = Database::connect(options).await.unwrap();
        scoped_db = Some(db.clone());
        exercise(db, reclaim).await;
    })
    .catch_unwind()
    .await;

    if let Some(db) = scoped_db {
        db.close().await.unwrap();
    }
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        resume_unwind(panic);
    }
}

async fn exercise(db: DatabaseConnection, reclaim: bool) {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", CID))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let raw = rpc_config(
        &target.uri(),
        &source.uri(),
        RpcProfile::Kubo,
        RpcStrategy::Cid,
    );
    let fixture = fixture_on_database(raw, db).await;
    let db = fixture.store.db();
    let claim = claim_submit(&fixture, Utc::now()).await;
    let original = claim.model.clone();
    let sibling_id = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();
    pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
        id: Set(sibling_id.clone()),
        lease_id: Set(original.lease_id.clone().unwrap()),
        cid: Set(OTHER.into()),
        logical_size: Set(100),
        provider: Set(fixture.provider.clone()),
        state: Set("quota_waiting".into()),
        last_error: Set(None),
        created_at: Set(now),
        last_touched_at: Set(now),
    })
    .exec(db)
    .await
    .unwrap();
    let dispatched = tokio::time::timeout(
        Duration::from_secs(15),
        dispatch_submit(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claim,
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("original Submit did not complete its RPC POST")
    .unwrap()
    .unwrap();
    let recorded = if reclaim {
        // Stop at the durable HTTP result, before any worker projection.
        let receipt = ledger::submission::complete_fact(
            &fixture.store,
            dispatched.rpc_invocation.as_ref().unwrap(),
            &dispatched.observation,
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(receipt.outcome, "matched");
        assert!(receipt.needs_attention);
        assert_eq!(
            remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.into()))
                .one(db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "reserved"
        );
        Some(receipt.id)
    } else {
        None
    };

    let holder = lock_unrelated_rows(db, &sibling_id).await;
    if reclaim {
        let expired = dispatched.claimed.model.locked_until.unwrap() + ChronoDuration::seconds(1);
        let claim = tokio::time::timeout(Duration::from_secs(15), claim_submit(&fixture, expired))
            .await
            .expect("the original Submit could not be reclaimed");
        assert_eq!(claim.model.id, original.id);
        assert!(claim.reclaimed);
        assert_eq!(claim.model.submit_phase.as_deref(), Some("recovering"));
        fixture.run(claim).await.unwrap();
    } else {
        let continuation = tokio::time::timeout(
            Duration::from_secs(15),
            finish_submit_observation(
                &fixture.store,
                &fixture.coordinator,
                dispatched.claimed,
                dispatched.observation,
                dispatched.capture,
                dispatched.rpc_invocation,
            ),
        )
        .await
        .expect("the original worker waited on unrelated locks")
        .unwrap();
        assert!(continuation.is_none(), "matched must not start recovery");
    }

    assert_settled(&fixture, &original, recorded.as_deref()).await;
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].url.path(), "/api/v0/pin/add");
    holder.rollback().await.unwrap();
}

async fn claim_submit(fixture: &RpcFixture, now: DateTime<Utc>) -> ClaimedPinJob {
    jobs::claim_due_worker_jobs_store(
        &fixture.store,
        now,
        ChronoDuration::seconds(30),
        10,
        jobs::WorkerClaimSchedule {
            in_flight_by_provider: &Default::default(),
            last_served_ticket_by_provider: &Default::default(),
            configured_priorities: &Default::default(),
            active_job_ids: &Default::default(),
            lane: jobs::WorkerClaimLane::All,
        },
    )
    .await
    .unwrap()
    .into_iter()
    .find(|claim| claim.model.operation == "submit")
    .expect("the published Submit must be claimable")
}

async fn lock_unrelated_rows(db: &DatabaseConnection, sibling_id: &str) -> DatabaseTransaction {
    let tx = db.begin().await.unwrap();
    tx.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT id FROM pin_lease_targets WHERE id=$1 FOR UPDATE",
        [sibling_id.into()],
    ))
    .await
    .unwrap()
    .expect("same-lease target with another CID must be locked");
    // An unrelated domain writer conflicts with discovery's table fence, not ordinary reads.
    tx.execute_unprepared("LOCK TABLE pin_provider_routes IN ROW EXCLUSIVE MODE")
        .await
        .unwrap();
    tx
}

async fn assert_settled(fixture: &RpcFixture, original: &pin_job::Model, recorded: Option<&str>) {
    let db = fixture.store.db();
    let job = pin_job::Entity::find_by_id(original.id.clone())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
    assert!(job.locked_until.is_none());
    let history = jobs::submission_history(db, &original.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.state, "settled");
    assert_eq!((history.submit_calls, history.recovery_queries), (1, 0));
    let receipts = fixture.evidence().await;
    let [receipt] = receipts.as_slice() else {
        panic!("expected exactly one original RPC receipt, got {receipts:?}");
    };
    assert_eq!(receipt.job_id, original.id);
    assert_eq!(receipt.outcome, "matched");
    assert!(receipt.observed_at.is_some());
    assert!(!receipt.needs_attention);
    if let Some(id) = recorded {
        assert_eq!(receipt.id, id);
    }
    let remote = remote_pin::Entity::find_by_id((original.provider.clone(), original.cid.clone()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.epoch, receipt.remote_epoch);
    assert_eq!(remote.status, "pinned");
    let target = pin_lease_target::Entity::find_by_id(original.target_id.clone().unwrap())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    fixture.assert_held().await;
}
