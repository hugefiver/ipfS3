#[path = "support/stage7_management_actions.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::{pin_job, pin_provider_usage, remote_pin},
        pinning::{
            jobs,
            management::{actions, records},
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};

#[tokio::test]
async fn confirmed_retry_reactivates_exact_rejected_park_without_reserving_again() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("parked-rejected", true).await;
    let old = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[old.id.clone().into(),now.into()])).await.unwrap();
    let request = fixture
        .existing_request("repair", identity, Action::Retry)
        .await;
    let binding = fixture.confirm(&request).await;
    let Some(PlannedStep::Object(step)) = &binding.confirmation.bindings[0].step else {
        panic!("object step")
    };
    assert_eq!(step.source, RemoteDirective::SubmitWithFence);
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert_eq!(applied.items[0].state, records::ActionState::Queued);
    assert_eq!(applied.items[0].receipt.job_ids, vec![old.id.clone()]);
    let repaired = pin_job::Entity::find_by_id(old.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(repaired.state, "pending");
    assert_eq!(repaired.submit_phase.as_deref(), Some("ready"));
    assert!(repaired.locked_until.is_none());
    let history = jobs::submission_history(&fixture.db, &old.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.effect, "not_created");
    assert_eq!(history.submit_calls, 1);
    assert_eq!(history.state, "active");
    assert_eq!(history.started_at, now);
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        remote
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        usage
    );
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn unknown_parked_retry_preserves_the_park_and_has_no_new_post_or_reservation() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("parked-unknown", true).await;
    let old = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='unknown'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='unknown'")
        .await
        .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','unknown','needs_attention',1,0,$2)",[old.id.clone().into(),now.into()])).await.unwrap();
    let parked = pin_job::Entity::find_by_id(old.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let request = fixture
        .existing_request("unknown-repair", identity, Action::Retry)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(
        applied.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );
    assert_eq!(
        pin_job::Entity::find_by_id(old.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        parked
    );
    assert_eq!(
        jobs::submission_history(&fixture.db, &old.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        usage
    );
}
