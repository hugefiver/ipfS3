#[allow(dead_code)]
#[path = "support/stage7_management_worker.rs"]
mod support;

use ipfs_s3_gateway::{
    pinning::{provider::ObservedResource, worker::ManagementJobScope},
    store::{
        Store,
        entities::{pin_job, pin_replica_observation, pin_submit_observation},
        pinning::{
            jobs, ledger,
            management::{actions, records},
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Statement, TransactionTrait,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

async fn tick(fixture: &support::Fixture, scope: &Arc<ManagementJobScope>) {
    let coordinator = support::coordinator(fixture);
    let store = Store::new(fixture.db.clone());
    let scope = scope.clone();
    tokio::spawn(async move {
        let cancellation = CancellationToken::new();
        Box::pin(ipfs_s3_gateway::pinning::worker::management_tick(
            &coordinator,
            &store,
            &scope,
            &cancellation,
        ))
        .await
        .unwrap();
    })
    .await
    .unwrap();
}

async fn due(fixture: &support::Fixture, job_id: &str) {
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET next_attempt_at=$1 WHERE id=$2",
            [
                (chrono::Utc::now() - chrono::Duration::seconds(1)).into(),
                job_id.into(),
            ],
        ))
        .await
        .unwrap();
}

async fn posts(fixture: &support::Fixture) -> usize {
    fixture
        .server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|request| request.method.as_str() == "POST" && request.url.path() == "/psa/pins")
        .count()
}

async fn rejected() -> (
    support::Fixture,
    Arc<ManagementJobScope>,
    records::JobAuthority,
) {
    let fixture = Box::pin(support::psa_fixture()).await;
    let identity = Box::pin(fixture.publish("safe-backoff", false)).await;
    let binding =
        Box::pin(fixture.confirm(&fixture.create_request("safe-backoff", identity, false))).await;
    let applied = Box::pin(actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    ))
    .await
    .unwrap();
    let seed = applied.items[0]
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap()
        .clone();
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&fixture.server)
        .await;
    let scope = Arc::new(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap(),
    );
    Box::pin(tick(&fixture, &scope)).await;
    assert_eq!(posts(&fixture).await, 1);
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.submit_phase.as_deref(), Some("recovery_backoff"));
    (fixture, scope, seed)
}

async fn claim(fixture: &support::Fixture, job_id: &str) -> jobs::ClaimedPinJob {
    due(fixture, job_id).await;
    let claimed = jobs::claim_due_jobs(
        &fixture.db,
        chrono::Utc::now(),
        chrono::Duration::minutes(1),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(claimed.model.id, job_id);
    claimed
}

async fn denied<C: ConnectionTrait>(db: &C, job: &pin_job::Model, defect: &str) {
    let permit = Box::pin(actions::job_authority::check_before_call(db, job)).await;
    assert!(
        matches!(
            permit,
            Ok(Some(actions::job_authority::JobPermit::LedgerOnly(_))) | Err(_)
        ),
        "{defect}"
    );
}

async fn corrupt_replica_fixture(tx: &sea_orm::DatabaseTransaction) {
    // Only this disposable DB transaction bypasses append-only storage so the
    // consumer's fail-closed handling of damaged persisted facts is exercised.
    tx.execute_unprepared("DROP TRIGGER pin_replica_observations_no_update")
        .await
        .unwrap();
    tx.execute_unprepared("DROP TRIGGER pin_replica_observations_no_delete")
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_rejection_continues_same_authority_and_budget_through_eighth_post() {
    let (fixture, scope, seed) = Box::pin(rejected()).await;
    let original = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.submit_calls, 1);
    assert_eq!(original.effect, "not_created");
    for call in 2..=8 {
        due(&fixture, &seed.job_id).await;
        Box::pin(tick(&fixture, &scope)).await;
        assert_eq!(posts(&fixture).await, call as usize);
        let history = jobs::submission_history(&fixture.db, &seed.job_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(history.submit_calls, call);
        assert_eq!(history.started_at, original.started_at);
        assert_eq!(history.correlation, original.correlation);
        assert_eq!(history.api, original.api);
        assert_eq!(history.strategy, original.strategy);
        assert_eq!(history.effect, "not_created");
        assert!(history.recovery_queries >= original.recovery_queries);
    }
    let submit = pin_submit_observation::Entity::find()
        .filter(pin_submit_observation::Column::JobId.eq(&seed.job_id))
        .order_by_asc(pin_submit_observation::Column::SubmitCall)
        .all(&fixture.db)
        .await
        .unwrap();
    let replica = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&seed.job_id))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(submit.len(), 8);
    assert_eq!(replica.len(), 8);
    for (index, fact) in submit.iter().enumerate() {
        assert_eq!(fact.submit_call, index as i32 + 1);
        assert_eq!(
            fact.id,
            format!("{}:call:{}", seed.job_id, fact.submit_call)
        );
        assert_eq!(fact.provider, seed.provider);
        assert_eq!(fact.expected_cid, seed.cid);
        assert_eq!(
            serde_json::from_str::<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>(
                &fact.route
            )
            .unwrap(),
            seed.route
        );
        assert!(!fact.needs_attention);
        assert_eq!(fact.effect, "not_created");
        assert_eq!(fact.outcome, "Rejected");
        assert_eq!(fact.remote_epoch, submit[0].remote_epoch);
        assert!(
            serde_json::from_str::<Vec<ledger::submission::ResourceEvidence>>(&fact.resources)
                .unwrap()
                .is_empty()
        );
        let counterparts: Vec<_> = replica
            .iter()
            .filter(|row| row.observed_at == fact.observed_at.unwrap())
            .collect();
        assert_eq!(counterparts.len(), 1);
        let counterpart = counterparts[0];
        assert_eq!(counterpart.job_id, fact.job_id);
        assert_eq!(counterpart.provider, fact.provider);
        assert_eq!(counterpart.expected_cid, fact.expected_cid);
        assert_eq!(counterpart.route, fact.route);
        assert_eq!(counterpart.remote_epoch, fact.remote_epoch);
        assert_eq!(counterpart.claim_until, fact.claim_until);
        assert_eq!(counterpart.lease_id, seed.lease_id);
        assert_eq!(counterpart.target_id, seed.target_id);
        assert_eq!(counterpart.expected_generation, seed.lease_generation);
        assert_eq!(counterpart.phase, "submit");
        assert_eq!(counterpart.outcome, fact.outcome);
        assert_eq!(counterpart.safe_error, fact.safe_error);
        assert!(!counterpart.projectable);
        assert!(
            serde_json::from_str::<Vec<ObservedResource>>(&counterpart.resources)
                .unwrap()
                .is_empty()
        );
    }
    assert!(
        submit
            .windows(2)
            .all(|calls| calls[0].claim_until != calls[1].claim_until)
    );
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        actions::job_authority::lookup(&fixture.db, &job)
            .await
            .unwrap()
            .as_ref()
            == Some(&seed)
    );
    due(&fixture, &seed.job_id).await;
    Box::pin(tick(&fixture, &scope)).await;
    assert_eq!(posts(&fixture).await, 8);
    let after = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.submit_calls, 8);
    assert_eq!(after.started_at, original.started_at);
}

#[tokio::test]
async fn latest_call_requires_unique_same_append_replica_and_current_native_claim() {
    let (fixture, _, seed) = Box::pin(rejected()).await;
    let claimed = claim(&fixture, &seed.job_id).await;
    assert!(
        matches!(Box::pin(actions::job_authority::check_before_call(&fixture.db, &claimed.model)).await.unwrap(),
        Some(actions::job_authority::JobPermit::Submit(authority)) if authority == seed)
    );
    let initial_history = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    // Each transaction begins with the actual producer's backoff evidence. A
    // proof re-read in that caller transaction must see the change, not a bool
    // retained from the valid preflight above.
    for mutation in [
        "DELETE FROM pin_replica_observations",
        "UPDATE pin_replica_observations SET claim_until='2099-01-01T00:00:00Z'",
        "UPDATE pin_replica_observations SET observed_at='2000-01-01T00:00:00Z'",
        "UPDATE pin_replica_observations SET provider='foreign'",
        "UPDATE pin_replica_observations SET expected_cid='foreign-cid'",
        "UPDATE pin_replica_observations SET route='{}'",
        "UPDATE pin_replica_observations SET remote_epoch=remote_epoch+1",
        "UPDATE pin_replica_observations SET lease_id='foreign'",
        "UPDATE pin_replica_observations SET target_id='foreign'",
        "UPDATE pin_replica_observations SET expected_generation=expected_generation+1",
        "UPDATE pin_replica_observations SET phase='poll'",
        "UPDATE pin_replica_observations SET outcome='Unknown'",
        "UPDATE pin_replica_observations SET safe_error=NULL",
        "UPDATE pin_replica_observations SET resources='[{}]'",
        "UPDATE pin_replica_observations SET projectable=1",
        "DELETE FROM pin_submit_observations",
        "UPDATE pin_submit_observations SET id='foreign-call-id'",
        "UPDATE pin_submit_observations SET submit_call=2",
        "UPDATE pin_submit_observations SET remote_epoch=remote_epoch+1",
        "UPDATE pin_submit_observations SET needs_attention=1",
        "UPDATE pin_submit_observations SET observed_at=NULL",
        "UPDATE pin_submit_observations SET effect='unknown',outcome='Unknown'",
        "UPDATE pin_submit_observations SET outcome='in_flight'",
        "UPDATE pin_submit_observations SET resources='[{}]'",
        "UPDATE pin_jobs SET locked_until='2099-01-01T00:00:00Z'",
        "UPDATE pin_jobs SET locked_until=NULL",
        "UPDATE pin_jobs SET expected_generation=expected_generation+1",
        "UPDATE pin_submit_history SET effect='unknown'",
        "UPDATE pin_submit_history SET state='needs_attention'",
        "UPDATE pin_submit_history SET started_at='2000-01-01T00:00:00Z'",
        "UPDATE remote_pins SET status='queued',request_id='existing-request'",
        "UPDATE remote_pin_ledger SET effect='unknown'",
        "UPDATE pin_invocation_routes SET route='{}'",
        "UPDATE pin_invocation_routes SET remote_epoch=remote_epoch+1",
        "UPDATE pin_leases SET state='expired'",
        "UPDATE pin_leases SET source='foreign'",
        "UPDATE pin_leases SET generation=generation+1",
        "UPDATE pin_leases SET expires_at='2000-01-01T00:00:00Z'",
        "UPDATE objects SET size=size+1",
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) SELECT provider,cid,epoch,'{\"confirmed_absent\":true}' FROM remote_pins",
    ] {
        let tx = fixture.db.begin().await.unwrap();
        if mutation.contains("pin_replica_observations") {
            corrupt_replica_fixture(&tx).await;
        }
        tx.execute_unprepared(mutation).await.unwrap();
        denied(&tx, &claimed.model, mutation).await;
        tx.rollback().await.unwrap();
    }
    async fn original_actions<C: ConnectionTrait>(db: &C) -> Vec<String> {
        db.query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT json_array(database_scope,plan_id,item_id,binding,result) AS original FROM pin_management_records WHERE kind='action' ORDER BY database_scope,plan_id,item_id",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "original").unwrap())
        .collect()
    }
    let originals = original_actions(&fixture.db).await;
    assert!(!originals.is_empty());
    for mutation in [
        "DELETE FROM pin_management_records WHERE kind='action'",
        "UPDATE pin_management_records SET binding='{}' WHERE kind='action'",
        "UPDATE pin_management_records SET result='{}' WHERE kind='action'",
    ] {
        let tx = fixture.db.begin().await.unwrap();
        assert!(tx.execute_unprepared(mutation).await.is_err());
        assert_eq!(original_actions(&tx).await, originals);
        tx.rollback().await.unwrap();
    }
    let duplicate = "INSERT INTO pin_replica_observations(job_id,provider,expected_cid,route,remote_epoch,claim_until,lease_id,target_id,expected_generation,phase,resources,outcome,safe_error,projectable,observed_at) SELECT job_id,provider,expected_cid,route,remote_epoch,claim_until,lease_id,target_id,expected_generation,phase,resources,outcome,safe_error,projectable,observed_at FROM pin_replica_observations";
    for conflict in [
        None,
        Some("claim_until='2099-01-01T00:00:00Z'"),
        Some("observed_at='2000-01-01T00:00:00Z'"),
    ] {
        let tx = fixture.db.begin().await.unwrap();
        corrupt_replica_fixture(&tx).await;
        tx.execute_unprepared(duplicate).await.unwrap();
        if let Some(conflict) = conflict {
            tx.execute_unprepared(&format!("UPDATE pin_replica_observations SET {conflict} WHERE sequence=(SELECT MAX(sequence) FROM pin_replica_observations)")).await.unwrap();
        }
        denied(&tx, &claimed.model, "ambiguous same-append counterpart").await;
        tx.rollback().await.unwrap();
    }
    for table in ["pin_submit_observations", "pin_replica_observations"] {
        let tx = fixture.db.begin().await.unwrap();
        if table == "pin_replica_observations" {
            corrupt_replica_fixture(&tx).await;
        }
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!("UPDATE {table} SET resources=$1"),
            [
                "x".repeat(ipfs_s3_gateway::store::pinning::management::native::MAX_BYTES + 1)
                    .into(),
            ],
        ))
        .await
        .unwrap();
        denied(&tx, &claimed.model, "oversized fact payload").await;
        tx.rollback().await.unwrap();
    }
    assert_eq!(posts(&fixture).await, 1);
    let after = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.submit_calls, initial_history.submit_calls);
    assert_eq!(after.started_at, initial_history.started_at);
    assert!(
        matches!(Box::pin(actions::job_authority::check_before_call(&fixture.db, &claimed.model)).await.unwrap(),
        Some(actions::job_authority::JobPermit::Submit(authority)) if authority == seed)
    );
}

#[tokio::test]
async fn latest_unknown_or_older_identity_conflict_cannot_borrow_rejection_authority() {
    let (fixture, scope, seed) = Box::pin(rejected()).await;
    due(&fixture, &seed.job_id).await;
    Box::pin(tick(&fixture, &scope)).await;
    assert_eq!(posts(&fixture).await, 2);
    let claimed = claim(&fixture, &seed.job_id).await;
    for mutation in [
        "UPDATE pin_submit_observations SET outcome='Unknown',effect='unknown',needs_attention=1 WHERE submit_call=2",
        "UPDATE pin_submit_observations SET outcome='in_flight',observed_at=NULL WHERE submit_call=2",
        "DELETE FROM pin_submit_observations WHERE submit_call=2",
        "UPDATE pin_submit_observations SET provider='foreign' WHERE submit_call=1",
        "UPDATE pin_submit_observations SET expected_cid='foreign-cid' WHERE submit_call=1",
        "UPDATE pin_submit_observations SET route='{}' WHERE submit_call=1",
        "UPDATE pin_submit_observations SET submit_call=3 WHERE submit_call=1",
        "UPDATE pin_jobs SET submit_phase='recovering'",
    ] {
        let tx = fixture.db.begin().await.unwrap();
        tx.execute_unprepared(mutation).await.unwrap();
        denied(&tx, &claimed.model, mutation).await;
        tx.rollback().await.unwrap();
    }
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_submit_observations SET resources=$1",
        [
            "x".repeat(ipfs_s3_gateway::store::pinning::management::native::MAX_BYTES / 2 + 1)
                .into(),
        ],
    ))
    .await
    .unwrap();
    denied(&tx, &claimed.model, "cumulative byte budget").await;
    tx.rollback().await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let fact = pin_submit_observation::Entity::find()
        .filter(pin_submit_observation::Column::SubmitCall.eq(1))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    for call in 3..=9 {
        use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
        let mut extra = fact.clone().into_active_model();
        extra.id = Set(format!("{}:call:{call}", seed.job_id));
        extra.submit_call = Set(call);
        extra.insert(&tx).await.unwrap();
    }
    denied(&tx, &claimed.model, "8+1 candidate budget").await;
    tx.rollback().await.unwrap();
    assert_eq!(posts(&fixture).await, 2);
    assert_eq!(
        jobs::submission_history(&fixture.db, &seed.job_id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        2
    );
}
