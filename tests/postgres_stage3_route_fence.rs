//! PostgreSQL 17 two-connection route/publication serialization; no default skip masquerades as proof.
#[path = "stage3_route_fence.rs"]
mod fixture;

use futures_util::FutureExt;
use ipfs_s3_gateway::store::{
    self,
    pinning::{
        ledger,
        publication::{self, DecidedPublish},
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    TransactionTrait,
};
use std::{future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

async fn isolated<F, Fut>(body: F)
where
    F: FnOnce(DatabaseConnection, DatabaseConnection, DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("requires an authorized isolated PostgreSQL 17 URL");
    let admin = Database::connect(&url).await.unwrap();
    let version: String = admin
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "server_version")
        .unwrap();
    assert!(version.starts_with("17."), "requires PostgreSQL 17");
    let schema = format!("stage3_route_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut connections = Vec::new();
        for _ in 0..3 {
            let mut options = ConnectOptions::new(&url);
            options.min_connections(1).max_connections(1);
            let db = Database::connect(options).await.unwrap();
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET statement_timeout TO '8s'")
                .await
                .unwrap();
            connections.push(db);
        }
        store::run_migrations(&connections[0]).await.unwrap();
        body(
            connections[0].clone(),
            connections[1].clone(),
            connections[2].clone(),
        )
        .await;
        for db in connections {
            db.close().await.unwrap();
        }
    })
    .catch_unwind()
    .await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn decided<'a>(
    runtime: &'a ipfs_s3_gateway::pinning::coordinator::PinningCoordinator,
    decision: &'a ipfs_s3_gateway::pinning::decision::ExtensionDecision,
) -> DecidedPublish<'a> {
    DecidedPublish {
        decision,
        config: runtime.effective_config(),
        mode: runtime.control_mode(),
        limits: runtime.provider_limits(),
    }
}

#[tokio::test]
#[ignore = "requires explicit IPFS_S3_TEST_POSTGRES_URL pointing to PostgreSQL 17"]
async fn route_update_and_publication_serialize_both_directions_without_deadlock() {
    isolated(|a, b, observer| async move {
        store::bucket::create(&a, "route-fence", None).await.unwrap();
        let runtime = fixture::runtime();
        let key = runtime.effective_config().providers[0].identity.allocation_key();
        let r1 = runtime.effective_config().providers[0].identity.clone();
        let mut r2 = r1.clone();
        r2.credential_revision += 1;
        ledger::register_route(&a, &key, &r1).await.unwrap();

        // B changes R1 to R2 but does not commit. A must wait for B's row lock,
        // then reject without a partially visible version, lease, job or quota.
        let tx = b.begin().await.unwrap();
        ledger::register_route(&tx, &key, &r2).await.unwrap();
        let prior = fixture::counts(&observer).await;
        let runtime_a = Arc::clone(&runtime);
        let a_copy = a.clone();
        let mut pending = tokio::spawn(async move {
            let (request, decision) = fixture::captured(&runtime_a, "update-first", fixture::CID);
            publication::publish_decided_object(&a_copy, request, decided(&runtime_a, &decision)).await
        });
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut pending).await.is_err(),
            "publication unexpectedly passed B's uncommitted route UPDATE");
        assert_eq!(fixture::counts(&observer).await, prior);
        tx.commit().await.unwrap();
        let rejected = tokio::time::timeout(Duration::from_secs(5), pending).await.unwrap().unwrap();
        assert!(rejected.unwrap_err().to_string().contains("route"));
        assert_eq!(fixture::counts(&observer).await, prior);

        // A already owns the route lock; B's UPDATE cannot overtake its commit.
        ledger::register_route(&b, &key, &r1).await.unwrap();
        // The trigger stalls the publication after the route check and before
        // inserting a lease. A separate advisory holder releases it deterministically.
        observer.execute_unprepared("CREATE FUNCTION pause_route_lease() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(77991137); RETURN NEW; END $$").await.unwrap();
        observer.execute_unprepared("CREATE TRIGGER pause_route_lease_before_insert BEFORE INSERT ON pin_leases FOR EACH ROW EXECUTE FUNCTION pause_route_lease()").await.unwrap();
        observer.execute_unprepared("SELECT pg_advisory_lock(77991137)").await.unwrap();
        let runtime_a = Arc::clone(&runtime);
        let a_copy = a.clone();
        let publishing = tokio::spawn(async move {
            let (request, decision) = fixture::captured(&runtime_a, "publication-first", fixture::CID);
            publication::publish_decided_object(&a_copy, request, decided(&runtime_a, &decision)).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let row = observer.query_one(Statement::from_string(DatabaseBackend::Postgres,
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'advisory' AND query LIKE '%pin_leases%') AS waiting"))
                    .await.unwrap().unwrap();
                if row.try_get::<bool>("", "waiting").unwrap() { break; }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("publication never reached route-locked lease frontier");
        let b_copy = b.clone();
        let mut updating = tokio::spawn(async move { ledger::register_route(&b_copy, &key, &r2).await });
        assert!(tokio::time::timeout(Duration::from_millis(200), &mut updating).await.is_err(),
            "R2 UPDATE crossed A's locked route before commit");
        observer.execute_unprepared("SELECT pg_advisory_unlock(77991137)").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), publishing).await.unwrap().unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), updating).await.unwrap().unwrap().unwrap();
        let after = fixture::counts(&observer).await;
        assert_eq!(after[0], prior[0] + 1);
        assert_eq!(after[2], prior[2] + 1);
        assert_eq!(after[3], prior[3] + 1);
    }).await;
}
