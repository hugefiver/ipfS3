#[allow(dead_code)]
#[path = "support/stage6_cluster_fixture.rs"]
mod fixture;
#[path = "support/sql_trace.rs"]
mod sql_trace;

use chrono::{Duration as ChronoDuration, Utc};
use ipfs_s3_gateway::store::{
    Store,
    entities::{pin_job, pin_lease_target},
    pinning::ledger,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Expr};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn sqlite_cluster_projection_and_reclaimed_desired_lease_avoid_structure_reads() {
    exercise("sqlite::memory:").await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_cluster_projection_and_reclaimed_desired_lease_avoid_structure_reads() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("explicit test PostgreSQL URL");
    let admin = sea_orm::Database::connect(&url).await.unwrap();
    let schema = format!("cluster_warm_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut scoped_url = url::Url::parse(&url).unwrap();
    scoped_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let outcome = std::panic::AssertUnwindSafe(exercise(scoped_url.as_str()))
        .catch_unwind()
        .await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn exercise(database: &str) {
    let mut fixture = fixture::Fixture::configured("rest", false, database, |config| {
        config.pinning.policies[0].provider_mode = "one".into();
        let cluster = config.pinning.providers[0].cluster.as_mut().unwrap();
        cluster.min_replicas = 2;
        cluster.verifier_limits.max_age_secs = Some(60);
    })
    .await;
    let lease = fixture.put("observed-cluster").await;
    let mut db = fixture.state.store.db().clone();
    let trace = sql_trace::SqlTrace::default();
    trace.attach(&mut db);
    let store = Store::new(db);
    fixture
        .state
        .pinning
        .register_identities(&store)
        .await
        .unwrap();
    trace.take("Cluster cold qualification/registration", false);

    // The first peer set is insufficient; the next poll meets the frozen threshold.
    fixture.mode.store(1, Ordering::SeqCst);
    fixture.worker = Some(
        fixture
            .state
            .pinning
            .start(store.clone(), tokio_util::sync::CancellationToken::new()),
    );
    let partial = fixture.wait("Insufficient", 0).await;
    fixture.stop().await;
    trace.take("Cluster warm pinning projection", true);
    assert!(partial.projectable);
    assert!(
        !ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );

    fixture.mode.store(0, Ordering::SeqCst);
    fixture.due().await;
    fixture.worker = Some(
        fixture
            .state
            .pinning
            .start(store.clone(), tokio_util::sync::CancellationToken::new()),
    );
    let pinned = fixture.wait("Complete", partial.sequence).await;
    fixture.stop().await;
    trace.take(
        "Cluster warm pinned projection/completion/One convergence",
        true,
    );
    let desired = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease))
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(desired.state, "pinned");
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::State.eq("pending"))
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let expired = Utc::now() - ChronoDuration::seconds(1);
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value("running"))
        .col_expr(pin_job::Column::LockedUntil, Expr::value(Some(expired)))
        .col_expr(pin_job::Column::NextAttemptAt, Expr::value(expired))
        .filter(pin_job::Column::Id.eq(&poll.id))
        .exec(fixture.state.store.db())
        .await
        .unwrap();
    fixture.worker = Some(
        fixture
            .state
            .pinning
            .start(store.clone(), tokio_util::sync::CancellationToken::new()),
    );
    let recovered = fixture.wait("Complete", pinned.sequence).await;
    fixture.stop().await;
    trace.take("Cluster warm reclaimed desired-lease recovery", true);
    assert_eq!(recovered.job_id, poll.id);
    assert!(recovered.projectable);
    let reclaimed = pin_job::Entity::find_by_id(&poll.id)
        .one(fixture.state.store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(recovered.claim_until > expired);
    assert_ne!(reclaimed.state, "running");
    assert!(
        ledger::lease_status(fixture.state.store.db(), &lease)
            .await
            .unwrap()
            .unwrap()
            .all_targets_pinned
    );
    store.db().clone().close().await.unwrap();
}
