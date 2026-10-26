#[path = "support/stage75_recorded_provider.rs"]
mod historical;
#[path = "support/stage75_legacy_import.rs"]
mod legacy_import;
#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::pinning::management::retirement::RunLimits;
use ipfs_s3_gateway::store::pinning::management::{actions, native, records};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};

fn limits() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits
}

async fn remote(fixture: &support::Fixture) -> String {
    let original = fixture.config.pinning.providers[0].route_snapshot();
    let allocation: String = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT provider FROM pin_provider_routes WHERE snapshot=$1",
            [serde_json::to_string(&original).unwrap().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "provider")
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,'resource',10,'pinned',1,0,CURRENT_TIMESTAMP)",
        [allocation.clone().into(), support::CID.into()],
    )).await.unwrap();
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'application_created','confirmed')",
        [allocation.clone().into(), support::CID.into(), serde_json::to_string(&original).unwrap().into()],
    )).await.unwrap();
    allocation
}

async fn current(
    fixture: &support::Fixture,
    request: &ipfs_s3_gateway::pinning::management::PlanRequest,
    frozen: &serde_json::Value,
) -> actions::status::RecordedItemStatus {
    let plan = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let current = actions::status::recorded_with_work(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &plan,
        "retire",
        &limits(),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    assert_eq!(serde_json::to_value(plan.actions()).unwrap(), *frozen);
    let replay = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(serde_json::to_value(replay.actions()).unwrap(), *frozen);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
    current
}

#[tokio::test]
async fn confirmed_without_resource_id_needs_attention_without_rewriting_apply_replay() {
    let fixture = support::Fixture::new().await;
    let request = historical::install(&fixture.db, &fixture.config).await;
    let plan = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let frozen = serde_json::to_value(plan.actions()).unwrap();
    let allocation = remote(&fixture).await;
    let healthy = current(&fixture, &request, &frozen).await;
    assert_eq!(healthy.state, records::ActionState::Accepted);
    assert_eq!(healthy.retirement.unwrap().responsibility.unknown, 0);

    fixture
        .db
        .execute_unprepared("UPDATE remote_pins SET request_id=NULL")
        .await
        .unwrap();
    let unresolved = current(&fixture, &request, &frozen).await;
    let responsibility = unresolved.retirement.unwrap().responsibility;
    assert_eq!(responsibility.unknown, 1);
    assert!(responsibility.needs_attention);
    assert_eq!(unresolved.state, records::ActionState::NeedsAttention);

    fixture
        .db
        .execute_unprepared("UPDATE remote_pins SET request_id='resource'")
        .await
        .unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='retained'")
        .await
        .unwrap();
    let retained = current(&fixture, &request, &frozen).await;
    assert_eq!(retained.state, records::ActionState::Accepted);
    assert_eq!(retained.retirement.unwrap().responsibility.retained, 1);
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,submit_phase,state,attempts,next_attempt_at,created_at,updated_at) VALUES('cleanup','unpin',$1,$2,1,NULL,'pending',0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [allocation.into(), support::CID.into()],
    )).await.unwrap();
    let queued = current(&fixture, &request, &frozen).await;
    assert_eq!(queued.state, records::ActionState::Queued);
    let responsibility = queued.retirement.unwrap().responsibility;
    assert_eq!(responsibility.unknown, 0);
    assert_eq!(responsibility.cleanup_pending, 1);
}

#[tokio::test]
async fn unavailable_resource_route_needs_attention_but_proven_clearance_keeps_apply_replay() {
    let mut fixture = support::Fixture::new().await;
    let request = historical::install(&fixture.db, &fixture.config).await;
    let plan = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let frozen = serde_json::to_value(plan.actions()).unwrap();
    let allocation = remote(&fixture).await;
    assert_eq!(
        current(&fixture, &request, &frozen).await.state,
        records::ActionState::Accepted
    );

    fixture.raw.pinning_identity.providers[0].endpoint_revision += 1;
    fixture.config = support::base::management_config(&fixture.raw);
    let unavailable = current(&fixture, &request, &frozen).await;
    assert_eq!(unavailable.state, records::ActionState::NeedsAttention);
    let responsibility = unavailable.retirement.unwrap().responsibility;
    assert_eq!(responsibility.unknown, 0);
    assert!(responsibility.needs_attention);

    let ledger = ipfs_s3_gateway::store::entities::remote_pin_ledger::Entity::find_by_id((
        allocation.clone(),
        support::CID.to_owned(),
    ))
    .one(&fixture.db)
    .await
    .unwrap()
    .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES($1,$2,0,$3)",
        [allocation.into(), support::CID.into(), serde_json::json!({"ledger":ledger,"request_id":"old-resource","confirmed_absent":true}).to_string().into()],
    )).await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pins SET request_id=NULL,status='absent'")
        .await
        .unwrap();
    for effect in ["not_created", "absent"] {
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "UPDATE remote_pin_ledger SET effect=$1",
                [effect.into()],
            ))
            .await
            .unwrap();
        assert_eq!(
            current(&fixture, &request, &frozen).await.state,
            records::ActionState::Accepted
        );
    }
}
