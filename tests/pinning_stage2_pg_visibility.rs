//! Deterministic PostgreSQL publication interleaving at remote insert -> ledger capture.
//! Run with `IPFS_S3_TEST_POSTGRES_URL` set to an isolated test database.
use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures_util::FutureExt;
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{
            object, pin_lease, pin_lease_target, pin_provider_route, pin_provider_usage,
            remote_pin, remote_pin_ledger,
        },
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, EntityTrait,
    Statement,
};

const CID: &str = "bafy-stage2-visibility";

fn runtime() -> Arc<PinningCoordinator> {
    let config: Config = toml::from_str(
        r#"
[pinning_identity]
primary_storage_domain = 'local'
[[pinning_identity.providers]]
config_name = 'visibility'
provider_id = 'visibility-account'
display_name = 'Visibility'
backend = 'noop'
scope = 'account'
storage_domain = 'remote'
credential_revision = 1
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
[[pinning.providers]]
name = 'visibility'
kind = 'noop'
priority = 1
max_bytes = 80
max_pins = 1
"#,
    )
    .unwrap();
    PinningCoordinator::build(ValidatedPinningConfig::from_config(&config, |_| None).unwrap())
        .unwrap()
}

fn request(bucket: &str, provider: &str) -> PublicationRequest {
    PublicationRequest {
        object: PublicationObject::from_put(
            uuid::Uuid::new_v4().to_string(),
            bucket,
            "object",
            CID.into(),
            80,
            None,
            None,
            false,
            None,
            None,
            Utc::now(),
        ),
        tags: vec![],
        policy: PublicationPolicy {
            tags: vec![],
            leases: vec![LeaseIntent {
                source: LeaseSource::Automatic,
                policy_id: "visibility".into(),
                provider_mode: ProviderMode::All,
                providers: vec![provider.into()],
                content_mode: ContentMode::Object,
                duration: LeaseDuration::parse("1h").unwrap(),
            }],
        },
        object_target: PinTargetSpec {
            cid: CID.into(),
            logical_size: 80,
        },
    }
}

async fn connect(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    // One backend per publication: pg_stat_activity PID identifies its transaction.
    options.min_connections(1).max_connections(1);
    Database::connect(options).await.unwrap()
}

async fn pid(db: &DatabaseConnection) -> i64 {
    db.query_one(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT pg_backend_pid()::bigint AS value",
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "value")
    .unwrap()
}

async fn wait_for(control: &DatabaseConnection, sql: String, checkpoint: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let reached: bool = control
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    sql.clone(),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "reached")
                .unwrap();
            if reached {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {checkpoint}"));
}

async fn run_branch(
    control: &DatabaseConnection,
    a: &DatabaseConnection,
    b: &DatabaseConnection,
    schema: &str,
    rollback: bool,
) {
    store::run_migrations(a).await.unwrap();
    store::bucket::create(a, "bucket-a", None).await.unwrap();
    store::bucket::create(a, "bucket-b", None).await.unwrap();
    let runtime = runtime();
    runtime
        .register_identities(&Store::new(a.clone()))
        .await
        .unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    let a_pid = pid(a).await;
    let b_pid = pid(b).await;
    assert_ne!(a_pid, b_pid, "publications must use distinct PG backends");
    let lock_key = i64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());

    // The trigger fires only after remote_pins INSERT and usage UPDATE have
    // completed, inside A's same publication transaction. The control backend
    // holds this session advisory lock until both actual PG waits are observed.
    let reject_a = if rollback {
        format!(
            "IF pg_backend_pid() = {a_pid} THEN RAISE EXCEPTION 'stage2 injected rollback'; END IF;"
        )
    } else {
        String::new()
    };
    control.execute_unprepared(&format!(
        "CREATE FUNCTION {schema}.gate_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({lock_key}); {reject_a} RETURN NEW; END $$; CREATE TRIGGER gate_ledger BEFORE INSERT ON {schema}.remote_pin_ledger FOR EACH ROW EXECUTE FUNCTION {schema}.gate_ledger(); SELECT pg_advisory_lock({lock_key})"
    )).await.unwrap();

    let limits = runtime.provider_limits().clone();
    let a_db = a.clone();
    let a_request = request("bucket-a", &provider);
    let a_task =
        tokio::spawn(async move { publication::publish_object(&a_db, a_request, &limits).await });
    let arrival = std::panic::AssertUnwindSafe(wait_for(control, format!(
        "SELECT COALESCE((SELECT wait_event_type='Lock' AND wait_event='advisory' AND {control_pid} = ANY(pg_blocking_pids({a_pid})) AND query ILIKE '%remote_pin_ledger%' FROM pg_stat_activity WHERE pid={a_pid}), false) AND EXISTS (SELECT 1 FROM pg_locks WHERE pid={a_pid} AND relation='{schema}.remote_pins'::regclass AND mode='RowExclusiveLock' AND granted) AND EXISTS (SELECT 1 FROM pg_locks WHERE pid={a_pid} AND relation='{schema}.pin_provider_usage'::regclass AND mode='RowExclusiveLock' AND granted) AS reached",
        control_pid = pid(control).await,
    ), "A at post-remote ledger trigger holding remote and usage locks")).catch_unwind().await;
    if let Err(error) = arrival {
        a_task.abort();
        std::panic::resume_unwind(error);
    }
    let invisible: bool = control.query_one(Statement::from_string(DatabaseBackend::Postgres, format!(
        "SELECT NOT EXISTS (SELECT 1 FROM {schema}.remote_pins) AND NOT EXISTS (SELECT 1 FROM {schema}.remote_pin_ledger) AS value"
    ))).await.unwrap().unwrap().try_get("", "value").unwrap();
    assert!(
        invisible,
        "A's inserted remote and ledger must remain invisible before commit"
    );

    // B uses another bucket/key but the same provider/CID; its wait must be
    // on provider usage, not the bucket lock shared by same-bucket tests.
    let limits = runtime.provider_limits().clone();
    let b_db = b.clone();
    let b_request = request("bucket-b", &provider);
    let b_task =
        tokio::spawn(async move { publication::publish_object(&b_db, b_request, &limits).await });
    let wait = std::panic::AssertUnwindSafe(wait_for(control, format!(
        "SELECT COALESCE((SELECT wait_event_type='Lock' AND {a_pid} = ANY(pg_blocking_pids({b_pid})) AND query ILIKE '%pin_provider_usage%' FROM pg_stat_activity WHERE pid={b_pid}), false) AS reached"
    ), "B blocked by A on provider usage")).catch_unwind().await;
    // Always unblock the workers even when the expected wait was not reached.
    control
        .execute_unprepared(&format!("SELECT pg_advisory_unlock({lock_key})"))
        .await
        .unwrap();
    if let Err(error) = wait {
        a_task.abort();
        b_task.abort();
        std::panic::resume_unwind(error);
    }
    println!(
        "stage2 PG: A waited in ledger INSERT trigger after remote INSERT; B waited on provider usage, blocked by A; branch={}",
        if rollback { "ROLLBACK" } else { "COMMIT" }
    );

    let (a_result, b_result) = tokio::time::timeout(Duration::from_secs(10), async {
        (a_task.await.unwrap(), b_task.await.unwrap())
    })
    .await
    .expect("publication transactions did not complete after release");
    if rollback {
        assert!(
            a_result
                .unwrap_err()
                .to_string()
                .contains("stage2 injected rollback"),
            "A must fail specifically at the test-owned ledger trigger"
        );
    } else {
        a_result.expect("A must commit");
    }
    b_result.expect("B must publish after A resolves");

    let expected = if rollback { 1 } else { 2 };
    let objects = object::Entity::find().all(a).await.unwrap();
    assert_eq!(objects.len(), expected);
    assert!(objects.iter().any(|row| row.bucket == "bucket-b"));
    assert_eq!(
        objects.iter().any(|row| row.bucket == "bucket-a"),
        !rollback
    );
    assert_eq!(
        pin_lease::Entity::find().all(a).await.unwrap().len(),
        expected
    );
    assert_eq!(
        pin_lease_target::Entity::find().all(a).await.unwrap().len(),
        expected
    );
    let remotes = remote_pin::Entity::find().all(a).await.unwrap();
    let ledgers = remote_pin_ledger::Entity::find().all(a).await.unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(ledgers.len(), 1);
    assert_eq!(
        (&remotes[0].provider, remotes[0].cid.as_str()),
        (&provider, CID)
    );
    assert_eq!(
        (&ledgers[0].provider, ledgers[0].cid.as_str()),
        (&provider, CID)
    );
    assert_eq!(remotes[0].epoch, expected as i64);
    assert_eq!(remotes[0].status, "reserved");
    assert_eq!(ledgers[0].effect, "reserved");
    let route = pin_provider_route::Entity::find_by_id(provider.clone())
        .one(a)
        .await
        .unwrap()
        .unwrap();
    assert!(!route.retired);
    assert!(
        ledgers[0].route.as_deref() == Some(route.snapshot.as_str()),
        "ledger must carry the registered route"
    );
    let usage = pin_provider_usage::Entity::find_by_id(provider)
        .one(a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_pins, usage.reserved_bytes), (1, 80));
}

async fn exercise(rollback: bool) {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("provide an isolated IPFS_S3_TEST_POSTGRES_URL explicitly");
    let schema = format!("stage2_visibility_{}", uuid::Uuid::new_v4().simple());
    let control = connect(&url).await;
    control
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let mut scoped = url::Url::parse(&url).unwrap();
        scoped
            .query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let a = connect(scoped.as_str()).await;
        let b = connect(scoped.as_str()).await;
        let branch = std::panic::AssertUnwindSafe(run_branch(&control, &a, &b, &schema, rollback))
            .catch_unwind()
            .await;
        control
            .execute_unprepared("SELECT pg_advisory_unlock_all()")
            .await
            .unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
        if let Err(error) = branch {
            std::panic::resume_unwind(error);
        }
    })
    .catch_unwind()
    .await;
    // Also covers a panic during scoped connection setup or connection close.
    control
        .execute_unprepared("SELECT pg_advisory_unlock_all()")
        .await
        .unwrap();
    assert!(
        schema.starts_with("stage2_visibility_")
            && schema
                .bytes()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
    );
    control
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    control.close().await.unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_ledger_gap_commit_reuses_single_route_and_capacity() {
    exercise(false).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_ledger_gap_rollback_allocates_route_and_capacity_atomically() {
    exercise(true).await;
}
