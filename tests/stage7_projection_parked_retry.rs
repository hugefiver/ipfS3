#[path = "support/stage7_management_actions.rs"]
pub mod support;

use ipfs_s3_gateway::{
    pinning::management::{db_snapshot::JobState, *},
    store::entities::pin_job,
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};

async fn parked(fixture: &support::Fixture) -> PlanRequest {
    let identity = fixture.publish("projection-parked", true).await;
    let job = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared(
        "UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'",
    ).await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",
        [job.id.into(), now.into()],
    )).await.unwrap();
    fixture
        .existing_request("projection-retry", identity, Action::Retry)
        .await
}

fn source(plan: &DryRunPlan) -> RemoteDirective {
    let Some(PlannedStep::Object(step)) = &plan.items()[0].step else {
        panic!("expected an object step");
    };
    step.source
}

#[tokio::test]
async fn trusted_parked_submit_is_running_but_not_projected_as_inflight() {
    let fixture = support::Fixture::new().await;
    let request = parked(&fixture).await;
    let stored = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let captured = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap();
    assert!(captured.routes.iter().all(|route| route.available));
    assert_eq!(captured.jobs[0].state, JobState::Running);
    assert_eq!(captured.jobs[0].submission_effect, SideEffect::Rejected);
    assert_eq!(captured.remotes[0].inflight_jobs, Some(0));
    assert_eq!(captured.remotes[0].effect, SideEffect::Rejected);
    let plan = dry_run(&request, &captured.planning_snapshot().unwrap()).unwrap();
    assert_eq!(source(&plan), RemoteDirective::SubmitWithFence);
    assert_eq!(
        pin_job::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        stored
    );
}

#[tokio::test]
async fn running_submit_without_complete_park_proof_remains_inflight() {
    for mutation in [
        "DELETE FROM pin_submit_history",
        "UPDATE pin_submit_history SET effect='unknown',state='needs_attention'",
        "UPDATE pin_submit_history SET submit_calls=8",
        "UPDATE pin_jobs SET locked_until=next_attempt_at",
        "UPDATE pin_jobs SET expected_generation=expected_generation+1",
        "UPDATE pin_invocation_routes SET remote_epoch=remote_epoch+1",
        "UPDATE remote_pin_ledger SET effect='unknown'",
    ] {
        let fixture = support::Fixture::new().await;
        let request = parked(&fixture).await;
        fixture.db.execute_unprepared(mutation).await.unwrap();
        let stored = pin_job::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let captured = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
            .await
            .unwrap();
        assert_eq!(captured.remotes[0].inflight_jobs, Some(1), "{mutation}");
        assert_eq!(
            captured.remotes[0].effect,
            SideEffect::InFlight,
            "{mutation}"
        );
        let plan = dry_run(&request, &captured.planning_snapshot().unwrap()).unwrap();
        assert_eq!(
            source(&plan),
            RemoteDirective::WaitForInflight,
            "{mutation}"
        );
        assert_eq!(
            pin_job::Entity::find()
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap(),
            stored
        );
    }
}

#[tokio::test]
async fn parked_proof_does_not_make_a_mismatched_historical_configuration_available() {
    let fixture = support::Fixture::new().await;
    let request = parked(&fixture).await;
    let mut raw = fixture.raw.clone();
    raw.pinning_identity.providers[0].credential_revision += 1;
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let captured = db_snapshot::collect_with_config(&fixture.db, &config, &request)
        .await
        .unwrap();
    let historical = fixture.config.pinning.providers[0].route_snapshot();
    let route = captured
        .routes
        .iter()
        .find(|route| route.identity.credential_revision == historical.credential_revision)
        .unwrap();
    assert!(!route.available);
    assert!(!route.allocation_allowed);
    let plan = dry_run(&request, &captured.planning_snapshot().unwrap()).unwrap();
    assert!(matches!(plan.items()[0].status, PlanStatus::Blocked(_)));
}
