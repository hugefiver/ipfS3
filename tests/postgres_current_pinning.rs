use ipfs_s3_gateway::store::{
    initialization,
    migrations::{baseline, m20261108_000001_current_pinning_config as current},
};
use sea_orm::{ConnectionTrait, DbErr, RuntimeErr, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

#[path = "support/current_pinning_postgres.rs"]
mod fixture;
#[path = "support/baseline_postgres.rs"]
mod postgres;

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn old_baseline_upgrades_through_public_entry_and_preserves_history() {
    postgres::with_schema(|db| async move {
        fixture::baseline(&db).await;
        let before = fixture::snapshot(&db).await;
        let readonly = db.begin().await.unwrap();
        readonly
            .execute_unprepared("SET TRANSACTION READ ONLY")
            .await
            .unwrap();
        assert_eq!(
            initialization::classify(&readonly).await.unwrap(),
            initialization::SchemaState::UpgradeRequired
        );
        readonly.rollback().await.unwrap();
        assert!(initialization::open(&db).await.is_err());
        assert_eq!(before, fixture::snapshot(&db).await);
        fixture::upgrade(&db).await.unwrap();
        initialization::open(&db).await.unwrap();
        fixture::assert_history(&db).await;
        fixture::assert_current_guards(&db).await;
        let versions = fixture::strings(
            &db,
            "SELECT version AS value FROM seaql_migrations ORDER BY version",
        )
        .await;
        assert_eq!(versions, [baseline::VERSION, current::VERSION]);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn direct_migration_preserves_additional_state_checks() {
    postgres::with_schema(|db| async move {
        fixture::baseline(&db).await;
        db.execute_unprepared("ALTER TABLE pin_jobs ADD CONSTRAINT application_job_guard CHECK(state<>'running' OR attempts>0); ALTER TABLE pin_lease_targets ADD CONSTRAINT application_target_guard CHECK(state<>'pinned' OR logical_size>0)").await.unwrap();
        let before = fixture::snapshot(&db).await;
        assert!(fixture::upgrade(&db).await.is_err());
        assert_eq!(before, fixture::snapshot(&db).await);
        let tx = db.begin().await.unwrap();
        current::Migration.up(&SchemaManager::new(&tx)).await.unwrap();
        tx.commit().await.unwrap();
        for rejected in [
            "UPDATE pin_jobs SET state='running',attempts=0",
            "UPDATE pin_lease_targets SET state='pinned',logical_size=0",
        ] {
            let error = db.execute_unprepared(rejected).await.unwrap_err();
            assert_sqlstate(error, "23514");
        }
        db.execute_unprepared("UPDATE pin_jobs SET state='running',attempts=1; UPDATE pin_lease_targets SET state='pinned',logical_size=1; UPDATE pin_jobs SET state='error'; UPDATE pin_lease_targets SET state='error'").await.unwrap();
    }).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn second_table_mismatch_or_duplicate_rolls_back_executed_ddl() {
    for target_check in [
        "ALTER TABLE pin_lease_targets DROP CONSTRAINT pin_lease_targets_state_check; ALTER TABLE pin_lease_targets ADD CONSTRAINT changed_state CHECK(state IN ('waiting','submitted','pinned','degraded','quota_waiting','quota_blocked','evicted','re leased'))",
        "ALTER TABLE pin_lease_targets ADD CONSTRAINT duplicate_state CHECK(state IN ('waiting','submitted','pinned','degraded','quota_waiting','quota_blocked','evicted','released'))",
    ] {
        postgres::with_schema(|db| async move {
            fixture::baseline(&db).await;
            db.execute_unprepared(target_check).await.unwrap();
            let before = fixture::snapshot(&db).await;
            assert!(fixture::upgrade(&db).await.is_err());
            assert_eq!(before, fixture::snapshot(&db).await);
            let tx = db.begin().await.unwrap();
            let error = current::Migration.up(&SchemaManager::new(&tx)).await.unwrap_err();
            assert!(matches!(error, DbErr::Migration(_)), "{error:?}");
            assert_eq!(fixture::strings(&tx, "SELECT column_name AS value FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='pin_extension_decisions' AND column_name='config_revision') OR (table_name='zip_v2_mpu_intakes' AND column_name='rule_revision') OR (table_name='pin_lease_targets' AND column_name='last_error')) ORDER BY column_name").await, ["last_error"]);
            tx.execute_unprepared("UPDATE pin_jobs SET state='error'").await.unwrap();
            assert_eq!(fixture::strings(&tx, "SELECT state AS value FROM pin_jobs").await, ["error"]);
            tx.rollback().await.unwrap();
            assert_eq!(before, fixture::snapshot(&db).await);
            fixture::assert_history(&db).await;
            assert_sqlstate(db.execute_unprepared("UPDATE pin_jobs SET state='error'").await.unwrap_err(), "23514");
        }).await;
    }
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn public_upgrade_ledger_failure_restores_schema_data_and_ledger() {
    postgres::with_schema(|db| async move {
        fixture::baseline(&db).await;
        db.execute_unprepared(&format!(r#"
            CREATE SEQUENCE current_pinning_ledger_attempts;
            CREATE FUNCTION reject_current_pinning_version() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.version = '{}' THEN
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='pin_extension_decisions' AND column_name='config_revision') OR (table_name='zip_v2_mpu_intakes' AND column_name='rule_revision')))
                       OR NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='pin_lease_targets' AND column_name='last_error') THEN
                        RAISE EXCEPTION 'migration column changes missing' USING ERRCODE='P7412';
                    END IF;
                    UPDATE pin_jobs SET state='error';
                    UPDATE pin_lease_targets SET state='error',last_error='transaction-local error';
                    PERFORM nextval('current_pinning_ledger_attempts');
                    RAISE EXCEPTION 'current pinning ledger insertion rejected' USING ERRCODE='P7411';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER reject_current_pinning_version BEFORE INSERT ON seaql_migrations
                FOR EACH ROW EXECUTE FUNCTION reject_current_pinning_version();
        "#, current::VERSION)).await.unwrap();
        let before = fixture::snapshot(&db).await;
        assert_sqlstate(fixture::upgrade(&db).await.unwrap_err(), "P7411");
        assert_eq!(fixture::strings(&db, "SELECT last_value::text AS value FROM current_pinning_ledger_attempts WHERE is_called").await, ["1"]);
        assert_eq!(before, fixture::snapshot(&db).await);
        fixture::assert_history(&db).await;
        assert!(initialization::open(&db).await.is_err());
        db.execute_unprepared("DROP TRIGGER reject_current_pinning_version ON seaql_migrations; DROP FUNCTION reject_current_pinning_version(); DROP SEQUENCE current_pinning_ledger_attempts").await.unwrap();
        fixture::upgrade(&db).await.unwrap();
        initialization::open(&db).await.unwrap();
    }).await;
}

fn assert_sqlstate(error: DbErr, expected: &str) {
    let DbErr::Exec(RuntimeErr::SqlxError(sqlx::Error::Database(error))) = error else {
        panic!("expected PostgreSQL execution error, got {error:?}");
    };
    assert_eq!(error.code().as_deref(), Some(expected));
}
