#[path = "support/stage75_recorded_status.rs"]
mod converted;
#[path = "support/stage75_recorded_provider.rs"]
mod historical;
#[path = "support/stage75_legacy_import.rs"]
mod legacy_import;
#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    pinning::management::retirement::RunLimits,
    store::{
        initialization,
        pinning::management::{ManagementError, actions, native, records, schema},
    },
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

fn limits() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits
}

async fn status(
    fixture: &support::Fixture,
    plan: &records::RecordedPlan,
) -> actions::status::RecordedItemStatus {
    let item_id = match &plan.actions()[0] {
        records::RecordedResult::Current(action) => &action.receipt.binding.request.item_id,
        records::RecordedResult::Historical(action) => &action.receipt.binding.request.item_id,
    };
    actions::status::recorded_with_work(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        plan,
        item_id,
        &limits(),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn converted_provider_status_observes_new_responsibility_without_rewriting_the_receipt() {
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
    assert!(matches!(
        &plan.actions()[0],
        records::RecordedResult::Historical(_)
    ));
    let before = native::capture(&fixture.db).await.unwrap();
    let current = status(&fixture, &plan).await;
    assert_eq!(current.state, records::ActionState::Accepted);
    assert_eq!(
        current.retirement.as_ref().unwrap().responsibility.unknown,
        0
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);

    let provider = &fixture.config.pinning.providers[0];
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,NULL,10,'pinning',1,0,CURRENT_TIMESTAMP)",
        [provider.name.clone().into(), support::CID.into()],
    )).await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'unknown','unknown')",
        [provider.name.clone().into(), support::CID.into(), serde_json::to_string(&provider.route_snapshot()).unwrap().into()],
    )).await.unwrap();
    tx.commit().await.unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let current = status(&fixture, &plan).await;
    assert_eq!(current.state, records::ActionState::NeedsAttention);
    assert!(current.retirement.as_ref().unwrap().responsibility.unknown > 0);
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    let restored = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(serde_json::to_value(restored.actions()).unwrap(), frozen);

    let mut partial = limits();
    partial.pages = 0;
    assert!(matches!(
        actions::status::recorded_with_work(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &plan,
            "retire",
            &partial,
            records::SharedDecodeBudget::default()
        )
        .await,
        Err(ManagementError::Budget)
    ));
    let cancelled = limits();
    cancelled.cancellation.cancel();
    assert!(matches!(
        actions::status::recorded_with_work(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &plan,
            "retire",
            &cancelled,
            records::SharedDecodeBudget::default()
        )
        .await,
        Err(ManagementError::Budget)
    ));
    let other = support::Fixture::new().await;
    assert!(matches!(
        actions::status::recorded_with_work(
            &other.db,
            &other.config,
            &other.auth,
            &plan,
            "retire",
            &limits(),
            records::SharedDecodeBudget::default()
        )
        .await,
        Err(ManagementError::Unauthorized)
    ));
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
    let key = native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.auth.database_scope().into(),
            request.plan_id.clone(),
            "retire".into(),
            "action".into(),
        ],
    };
    let deleted = fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='record_members'",
        [records::locator::source_key(&key).unwrap().into()],
    )).await.unwrap();
    assert!(deleted.rows_affected() > 0);
    assert!(matches!(
        actions::status::recorded_with_work(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &plan,
            "retire",
            &limits(),
            records::SharedDecodeBudget::default()
        )
        .await,
        Err(ManagementError::Changed)
    ));
}

#[tokio::test]
async fn converted_object_status_rechecks_errors_and_expiry_but_replay_keeps_its_original_state() {
    let mut fixture = support::Fixture::new().await;
    let object = fixture.publish("recorded-current", false).await;
    let request = fixture.create_request("recorded-current", object, false);
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    converted::convert_current(&fixture.db, &fixture.auth).await;
    let plan = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        &plan.actions()[0],
        records::RecordedResult::Current(_)
    ));
    let frozen = serde_json::to_value(plan.actions()).unwrap();
    fixture.raw.pinning.providers[0].max_pins += 1;
    fixture.config = support::base::management_config(&fixture.raw);
    let current = status(&fixture, &plan).await;
    assert_eq!(current.state, records::ActionState::Queued);
    assert!(current.retirement.is_none());
    let job = &applied.items[0].receipt.job_ids[0];
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET last_error='observed failure' WHERE id=$1",
            [job.clone().into()],
        ))
        .await
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    assert_eq!(
        status(&fixture, &plan).await.state,
        records::ActionState::NeedsAttention
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET last_error=NULL WHERE id=$1",
            [job.clone().into()],
        ))
        .await
        .unwrap();
    let lease = &applied.items[0].receipt.intent_ids[0];
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_leases SET expires_at='2000-01-01T00:00:00Z' WHERE id=$1",
            [lease.clone().into()],
        ))
        .await
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    assert_eq!(
        status(&fixture, &plan).await.state,
        records::ActionState::NeedsAttention
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    let replay = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(serde_json::to_value(replay.actions()).unwrap(), frozen);
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path().starts_with("/api/v0/"))
    );
}

#[tokio::test]
async fn converted_replay_rejects_an_extra_same_plan_action() {
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
    let table = native::registry()
        .into_iter()
        .find(|table| table.table == schema::RECORDS)
        .unwrap();
    let columns = table
        .columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(",");
    let projection = table
        .columns
        .iter()
        .map(|column| {
            if column == "item_id" {
                "'unreviewed'".to_owned()
            } else {
                format!("\"{column}\"")
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        format!("INSERT INTO pin_management_records({columns}) SELECT {projection} FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND item_id='retire' AND kind='action'"),
        [fixture.auth.database_scope().into(), request.plan_id.clone().into()],
    )).await.unwrap();
    assert_recorded_rejected(&fixture, &request, &plan, "retire").await;
}

async fn assert_recorded_rejected(
    fixture: &support::Fixture,
    request: &ipfs_s3_gateway::pinning::management::PlanRequest,
    plan: &records::RecordedPlan,
    item: &str,
) {
    let before = native::capture(&fixture.db).await.unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    assert!(
        matches!(
            records::load_recorded_plan(
                &fixture.db,
                &fixture.auth,
                request,
                &mut native::meter::Budget::default(),
            )
            .await,
            Err(ManagementError::Changed)
        ),
        "converted replay must reject changed members/sources"
    );
    assert!(
        matches!(
            actions::status::recorded_with_work(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                plan,
                item,
                &limits(),
                records::SharedDecodeBudget::default(),
            )
            .await,
            Err(ManagementError::Changed)
        ),
        "loaded status must recheck changed members/sources"
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        calls
    );
}

async fn converted_dependent_plan() -> (
    support::Fixture,
    ipfs_s3_gateway::pinning::management::PlanRequest,
    records::RecordedPlan,
    native::NativeKey,
) {
    use ipfs_s3_gateway::pinning::management::Action;
    let fixture = support::Fixture::new().await;
    let object = fixture.publish("dependency", false).await;
    let original = fixture.create_request("source-plan", object.clone(), false);
    let binding = fixture.confirm(&original).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let request = fixture
        .existing_request(
            "dependent-plan",
            object,
            Action::Renew {
                expires_at: chrono::Utc::now().timestamp() + 5400,
            },
        )
        .await;
    let binding = fixture.confirm(&request).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    converted::convert_current(&fixture.db, &fixture.auth).await;
    let plan = records::load_recorded_plan(
        &fixture.db,
        &fixture.auth,
        &request,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let source_key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            fixture.auth.database_scope().into(),
            original.plan_id,
            original.items[0].item_id.clone(),
            "action".into(),
        ],
    };
    // The real converted dependent archive must name this source in its manifest;
    // an unrelated historical row is not a current source dependency.
    let key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            fixture.auth.database_scope().into(),
            request.plan_id.clone(),
            request.items[0].item_id.clone(),
            "action".into(),
        ],
    };
    let source = initialization::history::recover_source(
        &fixture.db,
        &key,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let row: native::NativeRow = initialization::history::read_part(
        &fixture.db,
        &source.original,
        &mut native::meter::Budget::default(),
    )
    .await
    .unwrap();
    let raw = native::value(&row, "native_before").unwrap();
    let length = |at| usize::from_str_radix(&raw[at..at + 8], 16).unwrap();
    let start = 39 + length(29) * 8 + length(5) + length(13);
    type Version = (Vec<String>, (String, Vec<String>), usize, usize, String);
    let (_, tables): (Vec<String>, Vec<(String, Vec<Version>)>) =
        serde_json::from_str(&raw[start..start + length(21)]).unwrap();
    assert!(
        tables
            .iter()
            .flat_map(|(_, rows)| rows)
            .any(|(_, source, _, _, _)| source.0 == source_key.table
                && source.1 == source_key.values)
    );
    assert_eq!(
        status(&fixture, &plan).await.item_id,
        request.items[0].item_id
    );
    (fixture, request, plan, source_key)
}

#[tokio::test]
async fn converted_replay_and_status_reject_a_dirty_cross_plan_source() {
    let (fixture, request, plan, source) = converted_dependent_plan().await;
    let changed = fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_source_control SET source_generation=source_generation+1 WHERE source_key=$1",
        [records::locator::source_key(&source).unwrap().into()],
    )).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    assert_recorded_rejected(&fixture, &request, &plan, &request.items[0].item_id).await;
}

#[tokio::test]
async fn converted_replay_and_status_reject_a_missing_cross_plan_locator() {
    let (fixture, request, plan, source) = converted_dependent_plan().await;
    let changed = fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='record_members'",
        [records::locator::source_key(&source).unwrap().into()],
    )).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    assert_recorded_rejected(&fixture, &request, &plan, &request.items[0].item_id).await;
}
