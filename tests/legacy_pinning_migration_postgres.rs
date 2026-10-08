//! Run only under the parent's owned PG fixture. Not a SQLite substitute.
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    pinning::identity::{CleanupMode, ProviderIdentity},
    store::{
        self,
        pinning::{ledger, legacy_migration, quota},
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait,
};
use serde_json::json;
use std::{future::Future, panic::AssertUnwindSafe};

async fn isolated<F, Fut>(body: F)
where
    F: FnOnce(DatabaseConnection, DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    assert_eq!(
        std::env::var("IPFS_S3_R26_PG_OWNED").as_deref(),
        Ok("legacy-r26-fixture-only"),
        "explicit parent-owned fixture marker required"
    );
    let url =
        std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("authorized isolated PG URL required");
    let admin = Database::connect(&url).await.unwrap();
    let schema = format!("r26_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut connections = vec![];
        for _ in 0..2 {
            let mut options = ConnectOptions::new(&url);
            options.max_connections(1).min_connections(1);
            let db = Database::connect(options).await.unwrap();
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET TIME ZONE 'UTC'").await.unwrap();
            db.execute_unprepared("SET statement_timeout='8s'")
                .await
                .unwrap();
            connections.push(db);
        }
        store::run_migrations(&connections[0]).await.unwrap();
        body(connections[0].clone(), connections[1].clone()).await;
        for db in connections {
            db.close().await.unwrap();
        }
    })
    .catch_unwind()
    .await;
    // Fail-closed exact run-created schema ownership check before cleanup.
    assert!(schema.starts_with("r26_") && schema.len() == 36);
    let owned=admin.query_one(Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres,
        "SELECT 1 FROM pg_namespace WHERE nspname=$1 AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)",[schema.clone().into()])).await.unwrap();
    assert!(
        owned.is_some(),
        "cleanup target no longer owned by this fixture"
    );
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

async fn fixture(db: &DatabaseConnection) -> legacy_migration::Declaration {
    let identity = ProviderIdentity {
        provider_id: "r26-account".into(),
        display_name: "fixture".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: CleanupMode::Retain,
    };
    ledger::register_route(db, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    serde_json::from_value(json!({"version":1,"database_scope":"owned-pg-fixture","ttl_seconds":300,"maintenance":{"all_writers_stopped":true,"all_workers_drained":true},"namespaces":[{"source":"old","target":identity.allocation_key(),"backend":"noop","scope":"account","resource_type":identity.resource_type(),"binding_only":false,"target_route":identity.route_snapshot(),"bindings":[{"table":"pin_provider_usage","key":{"provider":"old"},"provider":"old","historical_route":null}]}]})).unwrap()
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn fresh_apply_nowait_insert_phantom_and_atomic_receipt_failure() {
    isolated(|a,b|async move {
        let declaration=fixture(&a).await;
        let plan=legacy_migration::inventory(&a,&declaration).await.unwrap();
        let writer=b.begin().await.unwrap();
        writer.execute_unprepared("INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('detached','phantom',1,'{}')").await.unwrap();
        assert!(legacy_migration::apply(&a,&declaration,&plan).await.is_err(),"tail NOWAIT must fence an uncommitted insert, including unrelated namespaces");
        writer.rollback().await.unwrap();
        assert_eq!(legacy_migration::inventory(&a,&declaration).await.unwrap().before,plan.before);
        assert_eq!(quota::read_usage(&a,"old").await.unwrap().unwrap().reserved_pins,3);
        b.execute_unprepared("CREATE FUNCTION r26_fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned fixture'; END $$").await.unwrap();
        b.execute_unprepared("CREATE TRIGGER r26_fail_receipt BEFORE INSERT ON pin_legacy_migration_items FOR EACH ROW EXECUTE FUNCTION r26_fail_receipt()").await.unwrap();
        assert!(legacy_migration::apply(&a,&declaration,&plan).await.is_err());
        let current=legacy_migration::inventory(&a,&declaration).await.unwrap();
        assert_eq!(current.before,plan.before);assert_eq!(current.receipt_frontier,plan.receipt_frontier);
        b.execute_unprepared("DROP TRIGGER r26_fail_receipt ON pin_legacy_migration_items").await.unwrap();
        let receipt=legacy_migration::apply(&a,&declaration,&plan).await.unwrap();
        assert_eq!(legacy_migration::apply(&b,&declaration,&plan).await.unwrap(),receipt);
        let reverse=legacy_migration::rollback(&b,&receipt.id,&plan).await.unwrap();
        assert_eq!(legacy_migration::rollback(&a,&receipt.id,&plan).await.unwrap(),reverse);
    }).await;
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn route_row_nowait_and_committed_drift_are_fresh_fences() {
    isolated(|a,b|async move {
        let declaration=fixture(&a).await;
        let plan=legacy_migration::inventory(&a,&declaration).await.unwrap();
        let writer=b.begin().await.unwrap();
        writer.execute_unprepared("UPDATE pin_provider_usage SET reserved_bytes=98 WHERE provider='old'").await.unwrap();
        assert!(legacy_migration::apply(&a,&declaration,&plan).await.is_err());
        writer.commit().await.unwrap();
        assert!(legacy_migration::apply(&a,&declaration,&plan).await.is_err(),"fresh transaction must observe committed drift rather than old dry-run RR");
        assert_eq!(quota::read_usage(&a,"old").await.unwrap().unwrap().reserved_bytes,98);
        let fresh=legacy_migration::inventory(&a,&declaration).await.unwrap();
        legacy_migration::apply(&a,&declaration,&fresh).await.unwrap();
        b.execute_unprepared("INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('new','work',1,'{}')").await.unwrap();
        assert!(legacy_migration::rollback(&a,&fresh.operation_id,&fresh).await.is_err());
    }).await;
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn registered_r26_damaged_receipt_catalog_is_refused_without_business_writes() {
    for damage in [
        "DROP TABLE pin_legacy_migration_items",
        "ALTER TABLE pin_legacy_migration_items DROP CONSTRAINT pin_legacy_migration_items_pkey",
        "ALTER TABLE pin_legacy_migration_items DROP CONSTRAINT pin_legacy_migration_items_ordinal_check",
        "ALTER TABLE pin_legacy_migration_items DISABLE TRIGGER pin_legacy_item_bound",
        "ALTER TABLE pin_legacy_migrations DISABLE TRIGGER pin_legacy_migrations_immutable",
    ] {
        isolated(|a,b|async move {
            let declaration=fixture(&a).await;
            legacy_migration::inventory(&a,&declaration).await.unwrap();
            b.execute_unprepared(damage).await.unwrap();
            assert!(legacy_migration::inventory(&a,&declaration).await.is_err(),"damaged known-version schema must be refused: {damage}");
            let usage=quota::read_usage(&a,"old").await.unwrap().unwrap();
            assert_eq!((usage.reserved_bytes,usage.reserved_pins),(97,3));
            let receipts:i64=a.query_one(Statement::from_string(sea_orm::DatabaseBackend::Postgres,
                "SELECT COUNT(*) AS n FROM pin_legacy_migrations")).await.unwrap().unwrap().try_get("","n").unwrap();
            assert_eq!(receipts,0);
            let registered:i64=a.query_one(Statement::from_string(sea_orm::DatabaseBackend::Postgres,
                "SELECT COUNT(*) AS n FROM seaql_migrations WHERE version='m20261007_000001_legacy_pinning_migration'"))
                .await.unwrap().unwrap().try_get("","n").unwrap();
            assert_eq!(registered,1);
        }).await;
    }
}

#[tokio::test]
#[ignore = "parent owned PostgreSQL fixture required"]
async fn historical_account_conflict_and_new_allocation_lifetime_are_fenced() {
    isolated(|a,_|async move {
        let mut declaration=fixture(&a).await;
        let mut wrong=declaration.namespaces[0].target_route.clone();wrong.scope="foreign-account".into();
        a.execute(Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres,
            "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ('old','cid',$1,'unknown','unknown')",[serde_json::to_string(&wrong).unwrap().into()])).await.unwrap();
        declaration.namespaces[0].bindings.push(serde_json::from_value(json!({"table":"remote_pin_ledger","key":{"provider":"old","cid":"cid"},"provider":"old","historical_route":wrong})).unwrap());
        assert!(legacy_migration::inventory(&a,&declaration).await.is_err());
        assert_eq!(quota::read_usage(&a,"old").await.unwrap().unwrap().reserved_bytes,97);
    }).await;
    isolated(|a,_|async move {
        let mut declaration=fixture(&a).await;
        a.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old','cid',7,'absent',42,CURRENT_TIMESTAMP)").await.unwrap();
        a.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('old','cid','unknown','absent')").await.unwrap();
        a.execute_unprepared("INSERT INTO pin_jobs(id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('old-submit','submit','old','cid','old-lease','old-target',1,'done',CURRENT_TIMESTAMP,'ready')").await.unwrap();
        for table in ["remote_pins","remote_pin_ledger"] {
            declaration.namespaces[0].bindings.push(serde_json::from_value(json!({"table":table,"key":{"provider":"old","cid":"cid"},"provider":"old","historical_route":null})).unwrap());
        }
        declaration.namespaces[0].bindings.push(serde_json::from_value(json!({"table":"pin_jobs","key":{"id":"old-submit"},"provider":"old","historical_route":null})).unwrap());
        let plan=legacy_migration::inventory(&a,&declaration).await.unwrap();
        legacy_migration::apply(&a,&declaration,&plan).await.unwrap();
        let target=declaration.namespaces[0].target.clone();
        let limits=[(target.clone(),ipfs_s3_gateway::pinning::config::ProviderLimits{priority:1,max_bytes:1000,max_pins:100,enabled:true})].into();
        let txn=a.begin().await.unwrap();
        assert_eq!(quota::reserve_unique(&txn,&target,"cid",7,&limits,store::database_clock::database_now(&txn).await.unwrap()).await.unwrap(),quota::ReservationOutcome::Reserved);
        txn.commit().await.unwrap();
        a.execute(Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres,
            "INSERT INTO pin_jobs(id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('new-submit','submit',$1,'cid','new-lease','new-target',1,'pending',CURRENT_TIMESTAMP,'ready')",[target.clone().into()])).await.unwrap();
        ledger::capture_invocation(&a,"new-submit",&target,"cid").await.unwrap();
        use sea_orm::EntityTrait;
        let jobs=ipfs_s3_gateway::store::entities::pin_job::Entity::find().all(&a).await.unwrap();
        assert!(!legacy_migration::job_io_allowed(&a,jobs.iter().find(|j|j.id=="old-submit").unwrap()).await.unwrap());
        assert!(legacy_migration::job_io_allowed(&a,jobs.iter().find(|j|j.id=="new-submit").unwrap()).await.unwrap());
        let usage=quota::read_usage(&a,&target).await.unwrap().unwrap();assert_eq!((usage.reserved_bytes,usage.reserved_pins),(104,4));
    }).await;
}
