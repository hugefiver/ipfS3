//! Fresh parent-owned exclusive stage7_ schema, using the existing apply fixture
//! contract. Authority is created by prepare/confirm/apply; SQL only gates IO and
//! attempts a rolled-back mutation of that real authority.
#[allow(dead_code)]
#[path = "support/stage7_management_apply_postgres.rs"]
mod support;

use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::{
        coordinator::PinningCoordinator,
        worker::{ManagementJobScope, management_tick},
    },
    store::{
        Store,
        entities::{pin_job, pin_lease_target, pin_submit_observation, remote_pin},
        pinning::management::{actions, records},
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, QueryFilter, RuntimeErr, Statement, TransactionTrait,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

async fn connection() -> DatabaseConnection {
    let url = std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL")
        .expect("parent supplies a fresh empty exclusive stage7_ schema");
    let mut options = ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared("SET statement_timeout='8s'")
        .await
        .unwrap();
    db
}

async fn pid<C: ConnectionTrait>(db: &C) -> i32 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid() AS pid",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "pid")
    .unwrap()
}

async fn wait_until_blocked_by(db: &DatabaseConnection, waiter: i32, blocker: i32) {
    loop {
        let row = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT $2 = ANY(pg_blocking_pids($1)) AS blocked",
                [waiter.into(), blocker.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        if row.try_get::<bool>("", "blocked").unwrap() {
            break;
        }
        tokio::task::yield_now().await;
    }
}

async fn blocked_by(db: &DatabaseConnection, waiter: i32, blocker: i32) {
    tokio::time::timeout(
        Duration::from_secs(3),
        wait_until_blocked_by(db, waiter, blocker),
    )
    .await
    .expect("observe the actual backend lock dependency, not a timing delay");
}

#[tokio::test]
#[ignore = "requires fresh parent-owned exclusive IPFS_S3_STAGE7_APPLY_DATABASE_URL"]
async fn managed_completion_holds_authority_until_remote_projection_commits() {
    let fixture = Box::pin(support::Fixture::new()).await;
    let identity = Box::pin(fixture.prepared_object("completion-authority-tail")).await;
    let request = fixture.create_request(identity);
    let binding = Box::pin(fixture.confirm(&request)).await;
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
    let scope = Arc::new(
        Box::pin(ManagementJobScope::from_applied(
            &fixture.db,
            &fixture.auth,
            &binding,
        ))
        .await
        .unwrap(),
    );
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();

    // This remote-table gate runs inside actual completion after authority
    // checks, before the remote/target projection becomes visible to readers.
    fixture
        .db
        .execute_unprepared(
            r#"
        CREATE FUNCTION stage75_consumer_completion_gate() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.status='pinned' AND OLD.status<>'pinned' THEN
                PERFORM pg_advisory_xact_lock(750075, 1);
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER stage75_consumer_completion_gate BEFORE UPDATE ON remote_pins
            FOR EACH ROW EXECUTE FUNCTION stage75_consumer_completion_gate()
    "#,
        )
        .await
        .unwrap();
    let observer = connection().await;
    let mutator = connection().await;
    let locator_mutator = connection().await;
    let worker_pid = pid(&fixture.db).await;
    let mutator_pid = pid(&mutator).await;
    let locator_mutator_pid = pid(&locator_mutator).await;
    let blocker = fixture.writer.begin().await.unwrap();
    let blocker_pid = pid(&blocker).await;
    blocker
        .execute_unprepared("SELECT pg_advisory_xact_lock(750075, 1)")
        .await
        .unwrap();

    let store = Store::new(fixture.db.clone());
    let mut worker = tokio::spawn(async move {
        Box::pin(management_tick(
            &coordinator,
            &store,
            &scope,
            &CancellationToken::new(),
        ))
        .await
    });
    tokio::select! {
        () = wait_until_blocked_by(&observer, worker_pid, blocker_pid) => {},
        finished = &mut worker => match finished {
            Ok(Ok(report)) => panic!(
                "worker completed before the remote projection gate: {:?}",
                report.items.iter().map(|item| &item.state).collect::<Vec<_>>()
            ),
            Ok(Err(error)) => panic!("worker failed before the remote projection gate: {error:?}"),
            Err(error) => panic!("worker task failed before the remote projection gate: {error}"),
        },
    }
    let before = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&observer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, "reserved");
    assert!(before.request_id.is_none());
    assert!(
        !pin_submit_observation::Entity::find()
            .filter(pin_submit_observation::Column::JobId.eq(&seed.job_id))
            .all(&observer)
            .await
            .unwrap()
            .is_empty(),
        "the real Noop call's facts commit independently before projection"
    );

    let plan_id = request.plan_id.clone();
    let mutation = tokio::spawn(async move {
        let tx = mutator.begin().await.unwrap();
        let error = tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "UPDATE pin_management_records SET native_before=native_before || ' ' WHERE plan_id=$1 AND kind='action'",
            [plan_id.into()])).await.unwrap_err();
        let DbErr::Exec(RuntimeErr::SqlxError(error)) = error else {
            panic!("expected the fixture's immutable archive rejection: {error:?}");
        };
        assert_eq!(
            error.as_database_error().unwrap().code().unwrap().as_ref(),
            "P0001"
        );
        tx.rollback().await.unwrap();
        mutator.close().await.unwrap();
    });
    let source = records::locator::source_key(
        &ipfs_s3_gateway::store::pinning::management::native::NativeKey {
            table: "pin_management_records".into(),
            values: vec![
                fixture.auth.database_scope().into(),
                request.plan_id.clone(),
                request.items[0].item_id.clone(),
                "action".into(),
            ],
        },
    )
    .unwrap();
    let locator_mutation =
        tokio::spawn(async move {
            let tx = locator_mutator.begin().await.unwrap();
            let result = tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "DELETE FROM pin_management_locator WHERE source_key=$1 AND selector_kind='job'",
            [source.into()])).await.unwrap();
            assert!(result.rows_affected() > 0);
            tx.rollback().await.unwrap();
            locator_mutator.close().await.unwrap();
        });
    // Without the completion tail fence this mutation finishes while the worker
    // remains at the remote gate, invalidating its already-reviewed authority.
    blocked_by(&observer, mutator_pid, worker_pid).await;
    blocked_by(&observer, locator_mutator_pid, worker_pid).await;
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(8), mutation)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(8), locator_mutation)
        .await
        .unwrap()
        .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(8), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.items[0].state, records::ActionState::RemoteComplete);
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&observer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "pinned");
    assert!(remote.request_id.is_some());
    let target = pin_lease_target::Entity::find_by_id(seed.target_id.clone().unwrap())
        .one(&observer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    let job = pin_job::Entity::find_by_id(seed.job_id)
        .one(&observer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
    assert!(job.locked_until.is_none());
    assert!(
        records::lookup_committed(
            &observer,
            &fixture.auth,
            &binding,
            &request.items[0].item_id
        )
        .await
        .unwrap()
        .is_some()
    );

    fixture.db.execute_unprepared(
        "DROP TRIGGER stage75_consumer_completion_gate ON remote_pins; DROP FUNCTION stage75_consumer_completion_gate()",
    ).await.unwrap();
    observer.close().await.unwrap();
    fixture.close().await;
}
