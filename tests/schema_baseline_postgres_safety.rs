use ipfs_s3_gateway::store::{
    initialization::{self, UpgradeRequest},
    migrations::{Historical, baseline, history},
    pinning::management::{native, records},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use sea_orm_migration::MigratorTrait;
use std::time::Duration;
#[path = "support/baseline_postgres.rs"]
mod postgres;

fn sql(text: impl Into<String>) -> Statement {
    Statement::from_string(DatabaseBackend::Postgres, text)
}
async fn start(
    db: &DatabaseConnection,
    source: &'static str,
) -> tokio::task::JoinHandle<Result<(), sea_orm::DbErr>> {
    let (scope, identity) = native::scope::binding(db).await.unwrap();
    let db = db.clone();
    tokio::spawn(async move {
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: &identity,
                source_version: source,
                target_version: baseline::VERSION,
            },
        )
        .await
    })
}
async fn waiting(db: &DatabaseConnection, class: i32, object: i32) -> i32 {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(row) = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
                "SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid::bigint=$1 AND objid::bigint=$2 AND NOT granted",
                [(class as i64).into(), (object as i64).into()])).await.unwrap() {
                return row.try_get("", "pid").unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("maintenance must reach the deliberately blocked statement")
}
async fn old_ledger_without_stamp(db: &DatabaseConnection) {
    let versions = db
        .query_all(sql("SELECT version FROM seaql_migrations ORDER BY version"))
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.try_get::<String>("", "version").unwrap())
        .collect::<Vec<_>>();
    assert!(history::is_m23(&versions));
    let row = db.query_one(sql("SELECT (SELECT COUNT(*) FROM pin_management_archive WHERE archive_id LIKE 'history:upgrade:%')+(SELECT COUNT(*) FROM pin_management_inline_archive WHERE archive_id LIKE 'history:upgrade:%') AS n")).await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}
async fn pool_is_clean(db: &DatabaseConnection) {
    let expected = db
        .query_one(sql("SELECT current_schema() AS name"))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "name")
        .unwrap();
    let pool = db.get_postgres_connection_pool();
    let mut connections = Vec::new();
    for _ in 0..4 {
        let mut connection = pool.acquire().await.unwrap();
        let (schema, statement, lock): (String, String, String) = sqlx::query_as("SELECT current_schema(),current_setting('statement_timeout'),current_setting('lock_timeout')").fetch_one(&mut *connection).await.unwrap();
        assert_eq!(schema, expected);
        assert_eq!((statement.as_str(), lock.as_str()), ("0", "0"));
        connections.push(connection);
    }
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn cancelled_explicit_upgrade_drains_connection_and_keeps_source() {
    postgres::with_schema(|db| async move {
        Historical::up(&db, None).await.unwrap();
        let blocker = db.begin().await.unwrap();
        blocker
            .execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
            .await
            .unwrap();
        let task = start(&db, history::M23).await;
        let pid = waiting(&db, 1229997651, 1395879239).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let row = db
                    .query_one(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND xact_start IS NOT NULL",
                        [pid.into()],
                    ))
                    .await
                    .unwrap();
                if row.is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cancelled transaction must be drained or disconnected");
        blocker.rollback().await.unwrap();
        old_ledger_without_stamp(&db).await;
        pool_is_clean(&db).await;
        start(&db, history::M23).await.await.unwrap().unwrap();
        initialization::open(&db).await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL; exercises the real 60s unit budget"]
async fn explicit_upgrade_lock_deadline_rolls_back_and_restores_pool() {
    postgres::with_schema(|db| async move {
        Historical::up(&db, None).await.unwrap();
        let blocker = db.begin().await.unwrap();
        blocker
            .execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
            .await
            .unwrap();
        let task = start(&db, history::M23).await;
        waiting(&db, 1229997651, 1395879239).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(70), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        blocker.rollback().await.unwrap();
        old_ledger_without_stamp(&db).await;
        pool_is_clean(&db).await;
        start(&db, history::M23).await.await.unwrap().unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn explicit_upgrade_uncertain_commit_is_not_retried() {
    for interrupt in ["pg_cancel_backend", "pg_terminate_backend"] {
        postgres::with_schema(move |db| async move {
        Historical::up(&db, None).await.unwrap();
        let marker = (uuid::Uuid::new_v4().as_u128() as u32 & 0x7fff_ffff) as i32;
        db.execute_unprepared(&format!("CREATE SEQUENCE upgrade_attempts; CREATE FUNCTION wait_upgrade_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('upgrade_attempts'); PERFORM pg_advisory_xact_lock({marker},1); RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER wait_upgrade_commit AFTER INSERT ON seaql_migrations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wait_upgrade_commit()")).await.unwrap();
        let blocker = db.begin().await.unwrap();
        blocker.execute_unprepared(&format!("SELECT pg_advisory_xact_lock({marker},1)")).await.unwrap();
        let task = start(&db, history::M23).await;
        let pid = waiting(&db, marker, 1).await;
        db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres, format!("SELECT {interrupt}($1)"), [pid.into()])).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(8), task).await.unwrap().unwrap().unwrap_err();
        assert!(error.to_string().contains("COMMIT outcome is uncertain"));
        blocker.rollback().await.unwrap();
        assert_eq!(db.query_one(sql("SELECT last_value FROM upgrade_attempts")).await.unwrap().unwrap().try_get::<i64>("", "last_value").unwrap(), 1);
        old_ledger_without_stamp(&db).await;
        pool_is_clean(&db).await;
        db.execute_unprepared("DROP TRIGGER wait_upgrade_commit ON seaql_migrations; DROP FUNCTION wait_upgrade_commit(); DROP SEQUENCE upgrade_attempts").await.unwrap();
        start(&db, history::M23).await.await.unwrap().unwrap();
        initialization::open(&db).await.unwrap();
    }).await;
    }
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn failed_original_rolls_back_only_its_unit_and_never_publishes_ready() {
    postgres::with_schema(|db| async move {
        Historical::up(&db, Some(history::prefix(history::M13).unwrap().len() as u32)).await.unwrap();
        db.execute_unprepared("INSERT INTO pin_provider_routes VALUES ('broken','{not valid json','private',FALSE)").await.unwrap();
        assert!(start(&db, history::M13).await.await.unwrap().is_err());
        let row = db.query_one(sql("SELECT state FROM pin_management_readiness")).await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("", "state").unwrap(), "backfill");
        let key = native::NativeKey { table:"pin_provider_routes".into(), values:vec!["broken".into()] };
        let source = records::locator::source_key(&key).unwrap();
        assert!(db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres, "SELECT 1 FROM pin_management_source_control WHERE source_key=$1", [source.into()])).await.unwrap().is_none());
        assert_eq!(db.query_one(sql("SELECT snapshot FROM pin_provider_routes WHERE provider='broken'")).await.unwrap().unwrap().try_get::<String>("", "snapshot").unwrap(), "{not valid json");
        assert!(db.query_one(sql("SELECT 1 FROM pin_management_inline_archive WHERE archive_id='history:upgrade:m20261027_000001_schema_baseline'")).await.unwrap().is_some());
        assert!(initialization::open(&db).await.is_err());
        pool_is_clean(&db).await;
    }).await;
}
