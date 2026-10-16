#[path = "support/stage7_management_actions.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::{pin_job, pin_replica_observation},
        pinning::management::{actions, records},
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Set, Statement};

#[tokio::test]
async fn readonly_job_attention_is_current_and_does_not_require_ledger_mutation() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("attention", true).await;
    let request = fixture
        .existing_request("attention", identity, Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let id = &applied.items[0].jobs[0].job_id;
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_jobs SET state='running',locked_until=NULL,last_error='isolated' WHERE id=$1",[id.clone().into()])).await.unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::NeedsAttention
    );
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET state='done',last_error=NULL WHERE id=$1",
            [id.clone().into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::ReadOnly
    );
}

#[tokio::test]
async fn readonly_scoped_unknown_fact_overrides_the_unchanged_resource_ledger() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("typed-fact", true).await;
    let request = fixture
        .existing_request("typed-fact", identity, Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let authority = &applied.items[0].jobs[0];
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    pin_replica_observation::Entity::insert(pin_replica_observation::ActiveModel {
        sequence: Default::default(),
        job_id: Set(authority.job_id.clone()),
        provider: Set(authority.provider.clone()),
        expected_cid: Set(authority.cid.clone()),
        route: Set(serde_json::to_string(&authority.route).unwrap()),
        remote_epoch: Set(authority.remote_epoch),
        claim_until: Set(now),
        lease_id: Set(None),
        target_id: Set(None),
        expected_generation: Set(None),
        phase: Set("reconcile".into()),
        resources: Set("[]".into()),
        outcome: Set("Unknown".into()),
        safe_error: Set(None),
        projectable: Set(false),
        observed_at: Set(now),
    })
    .exec(&fixture.db)
    .await
    .unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET state='done',locked_until=NULL WHERE id=$1",
            [authority.job_id.clone().into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::NeedsAttention
    );
}

#[tokio::test]
async fn retired_domain_accepts_unknown_readonly_retry_without_submit_authority() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("retired-retry", true).await;
    fixture
        .db
        .execute_unprepared("UPDATE pin_provider_routes SET retired=TRUE")
        .await
        .unwrap();
    let request = fixture
        .existing_request("retired-retry", identity, Action::Retry)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.items[0].jobs.is_empty());
    assert!(
        applied.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );
    assert_eq!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .iter()
            .filter(|job| job.operation == "submit")
            .count(),
        1
    );
}

#[tokio::test]
async fn retirement_status_tracks_current_responsibility_not_the_stored_acceptance() {
    let fixture = support::Fixture::new().await;
    fixture.publish("retirement", true).await;
    let remote = ipfs_s3_gateway::store::entities::remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .confirmed_remote(&remote.provider, &remote.cid)
        .await;
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let request = PlanRequest {
        plan_id: "retirement-current".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(route),
            },
        }],
    };
    let binding = fixture.confirm(&request).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='unknown'")
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::NeedsAttention
    );
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='confirmed'")
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::Accepted
    );
}
