//! Runs only in a fresh schema allocated by the parent, never a shared database.
#[path = "support/stage75_recorded_status.rs"]
mod converted;
#[path = "support/stage75_recorded_provider.rs"]
mod historical;
#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    pinning::management::retirement::RunLimits,
    store::pinning::management::{ManagementError, actions, native, records, schema},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
#[ignore = "requires parent-owned fresh IPFS_S3_STAGE75_RECORDED_STATUS_DATABASE_URL"]
async fn postgres_converted_status_uses_a_read_only_repeatable_snapshot_and_keeps_replay_original()
{
    let url = std::env::var("IPFS_S3_STAGE75_RECORDED_STATUS_DATABASE_URL").unwrap();
    let db = Database::connect(url).await.unwrap();
    let scope = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS scope, (SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()) AS tables",
    )).await.unwrap().unwrap();
    let schema = scope.try_get::<String>("", "scope").unwrap();
    assert!(schema.starts_with("stage75_") || schema.starts_with("stage7_"));
    assert_eq!(
        scope.try_get::<i64>("", "tables").unwrap(),
        0,
        "require a fresh parent-owned schema"
    );
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let raw = support::base::config(&server.uri());
    let config = support::base::management_config(&raw);
    support::base::setup(&db, &config).await;
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let mut fixture = support::Fixture {
        directory: tempfile::tempdir().unwrap(),
        db,
        server,
        raw,
        config,
        auth,
    };
    let object = fixture.publish("pg-converted-status", false).await;
    let request = fixture.create_request("pg-converted-status", object, false);
    let binding = fixture.confirm(&request).await;
    let original = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
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
    let frozen = serde_json::to_value(plan.actions()).unwrap();
    fixture.raw.pinning.providers[0].max_pins += 1;
    fixture.config = support::base::management_config(&fixture.raw);
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE pin_jobs SET last_error='pg observed failure' WHERE id=$1",
            [original.items[0].receipt.job_ids[0].clone().into()],
        ))
        .await
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let status = actions::status::recorded_with_work(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &plan,
        &request.items[0].item_id,
        &Default::default(),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert_eq!(status.state, records::ActionState::NeedsAttention);
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
            .all(|call| call.url.path().starts_with("/api/v0/"))
    );
}

#[tokio::test]
#[ignore = "requires another parent-owned fresh IPFS_S3_STAGE75_RECORDED_STATUS_DATABASE_URL"]
async fn postgres_historical_provider_status_rechecks_current_responsibility_without_rewriting_replay()
 {
    let url = std::env::var("IPFS_S3_STAGE75_RECORDED_STATUS_DATABASE_URL").unwrap();
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let guard = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS empty",
    )).await.unwrap().unwrap();
    assert!(
        guard
            .try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(
        guard.try_get::<bool>("", "empty").unwrap(),
        "preserve occupied schemas; require a fresh allocation"
    );
    let exclusive = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(), 0)) AS held",
    )).await.unwrap().unwrap().try_get::<bool>("", "held").unwrap();
    assert!(exclusive, "require exclusive ownership of the fresh schema");

    let server = MockServer::start().await;
    let raw = support::base::config(&server.uri());
    let config = support::base::management_config(&raw);
    support::base::setup(&db, &config).await;
    let physical = native::scope::binding(&db).await.unwrap();
    assert!(!physical.1.starts_with("diagnostic:"));
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    assert_eq!(auth.database_identity(), physical.1);
    let request = historical::install(&db, &config).await;
    let plan =
        records::load_recorded_plan(&db, &auth, &request, &mut native::meter::Budget::default())
            .await
            .unwrap()
            .unwrap();
    assert!(
        matches!(&plan.actions()[0], records::RecordedResult::Historical(action)
        if action.state == records::ActionState::Accepted)
    );
    let frozen = serde_json::to_value(plan.actions()).unwrap();
    let limits = || {
        let mut limits = RunLimits::default();
        limits.pages = 512;
        limits
    };
    let before = native::capture(&db).await.unwrap();
    let accepted = actions::status::recorded_with_work(
        &db,
        &config,
        &auth,
        &plan,
        "retire",
        &limits(),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert_eq!(accepted.state, records::ActionState::Accepted);
    assert_eq!(
        accepted.retirement.as_ref().unwrap().responsibility.unknown,
        0
    );
    assert!(native::capture(&db).await.unwrap() == before);

    let provider = &config.pinning.providers[0];
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,NULL,10,'pinned',1,0,CURRENT_TIMESTAMP)",
        [provider.name.clone().into(), support::CID.into()],
    )).await.unwrap();
    tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'application_created','confirmed')",
        [provider.name.clone().into(), support::CID.into(), serde_json::to_string(&provider.route_snapshot()).unwrap().into()],
    )).await.unwrap();
    tx.commit().await.unwrap();
    let before = native::capture(&db).await.unwrap();
    let attention = actions::status::recorded_with_work(
        &db,
        &config,
        &auth,
        &plan,
        "retire",
        &limits(),
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert_eq!(attention.state, records::ActionState::NeedsAttention);
    assert!(
        attention
            .retirement
            .as_ref()
            .unwrap()
            .responsibility
            .unknown
            > 0
    );
    let replay =
        records::load_recorded_plan(&db, &auth, &request, &mut native::meter::Budget::default())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(serde_json::to_value(replay.actions()).unwrap(), frozen);
    let cancelled = limits();
    cancelled.cancellation.cancel();
    assert!(matches!(
        actions::status::recorded_with_work(
            &db,
            &config,
            &auth,
            &plan,
            "retire",
            &cancelled,
            records::SharedDecodeBudget::default(),
        )
        .await,
        Err(ManagementError::Budget)
    ));
    assert!(native::capture(&db).await.unwrap() == before);
    assert!(server.received_requests().await.unwrap().is_empty());

    let key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            auth.database_scope().into(),
            request.plan_id.clone(),
            "retire".into(),
            "action".into(),
        ],
    };
    let deleted = db.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='record_members'",
        [records::locator::source_key(&key).unwrap().into()],
    )).await.unwrap();
    assert_eq!(deleted.rows_affected(), 1);
    let before = native::capture(&db).await.unwrap();
    assert!(matches!(
        actions::status::recorded_with_work(
            &db,
            &config,
            &auth,
            &plan,
            "retire",
            &limits(),
            records::SharedDecodeBudget::default(),
        )
        .await,
        Err(ManagementError::Changed)
    ));
    assert!(matches!(
        records::load_recorded_plan(&db, &auth, &request, &mut native::meter::Budget::default(),)
            .await,
        Err(ManagementError::Changed)
    ));
    assert_eq!(serde_json::to_value(plan.actions()).unwrap(), frozen);
    assert!(native::capture(&db).await.unwrap() == before);
    assert!(server.received_requests().await.unwrap().is_empty());
    db.close().await.unwrap();
}
