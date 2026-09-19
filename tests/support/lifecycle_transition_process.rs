//! OS-process lifecycle-transition crash/restart fixture.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
    routing::any,
};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::FutureExt as _;
use http::{HeaderMap, header};
use http_body_util::BodyExt as _;
use ipfs_s3_gateway::{
    config::LifecycleWorkerConfig,
    kubo::KuboClient,
    lifecycle::{
        config::canonical_json,
        model::{
            CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
            CanonicalRuleSelector, CurrentTransition, LifecycleRuleStatus,
        },
        worker::start_worker_with_tiers,
    },
    pinning::policy::PublicationPolicy,
    residency::{PhysicalVerification, VersionResidencyIdentity},
    store::{
        self, Store,
        pinning::publication::{
            PinTargetSpec, PublicationObject, PublicationRequest, publish_object,
        },
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    TransactionTrait,
};
use tokio::{
    net::TcpListener,
    process::{Child, Command},
    sync::Notify,
};
use tokio_util::sync::CancellationToken;

#[path = "pg_residency.rs"]
#[allow(dead_code)]
mod pg_residency;

#[path = "lifecycle_transition_real_sigv4.rs"]
mod sigv4;

#[path = "lifecycle_transition_process_gateway.rs"]
mod process_gateway;

use process_gateway::{
    ProcessGateway, assert_gateway_no_such_key, assert_gateway_object, delete_gateway_object,
};

const TEST_NAME: &str = "lifecycle_transition_process_crash_restart_f1";
const CHILD_ROLE_ENV: &str = "IPFS_S3_TRANSITION_PROCESS_CHILD";
const CHILD_SCHEMA_ENV: &str = "IPFS_S3_TRANSITION_PROCESS_SCHEMA";
const CHILD_COLD_ENV: &str = "IPFS_S3_TRANSITION_PROCESS_COLD_URL";
const CHILD_LEASE_ENV: &str = "IPFS_S3_TRANSITION_PROCESS_LEASE_SECS";
const WAIT: Duration = Duration::from_secs(40);

pub async fn run() {
    if let Ok(child_id) = std::env::var(CHILD_ROLE_ENV) {
        child_main(&child_id).await;
    } else {
        parent_main().await;
    }
}

async fn child_main(child_id: &str) {
    let postgres_url = required_env("IPFS_S3_TEST_POSTGRES_URL");
    let schema = required_env(CHILD_SCHEMA_ENV);
    assert_valid_schema(&schema);
    let hot_url = endpoint_env("IPFS_S3_TRANSITION_HOT_URL");
    let cold_url = endpoint_env(CHILD_COLD_ENV);
    let lease_secs = required_env(CHILD_LEASE_ENV)
        .parse::<u64>()
        .expect("child lease seconds must be an integer");

    let db = connect(&postgres_url, Some(&schema), 1).await;
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "INSERT INTO transition_process_control (child_id, command, status, updated_at) \
         VALUES ($1, 'run', 'ready', clock_timestamp()) \
         ON CONFLICT (child_id) DO UPDATE SET command = 'run', status = 'ready', \
         updated_at = clock_timestamp()",
        [child_id.into()],
    ))
    .await
    .expect("publish child readiness");

    let config = LifecycleWorkerConfig {
        poll_interval_ms: 25,
        scan_page_size: 25,
        scan_lease_secs: 2,
        action_lease_secs: lease_secs,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 2,
    }
    .validate()
    .expect("validate child lifecycle worker configuration");
    let handle = start_worker_with_tiers(
        Store::new(db.clone()),
        config,
        CancellationToken::new(),
        KuboClient::new(hot_url),
        Some(KuboClient::new(cold_url)),
    );

    loop {
        let command = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT command FROM transition_process_control WHERE child_id = $1",
                [child_id.into()],
            ))
            .await
            .expect("read child control command")
            .expect("child control row remains present")
            .try_get::<String>("", "command")
            .expect("decode child command");
        if command == "stop" {
            handle.shutdown(Duration::from_secs(5)).await;
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "UPDATE transition_process_control SET status = 'stopped', \
                 updated_at = clock_timestamp() WHERE child_id = $1",
                [child_id.into()],
            ))
            .await
            .expect("publish graceful child shutdown");
            db.close().await.expect("close child PostgreSQL pool");
            return;
        }
        assert_eq!(command, "run", "unknown child control command");
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn parent_main() {
    let postgres_url = required_env("IPFS_S3_TEST_POSTGRES_URL");
    let hot_url = endpoint_env("IPFS_S3_TRANSITION_HOT_URL");
    let cold_url = endpoint_env("IPFS_S3_TRANSITION_COLD_URL");
    assert_ne!(hot_url, cold_url, "hot and cold Kubo URLs must differ");

    let base = connect(&postgres_url, None, 1).await;
    assert_pg17(&base).await;
    let hot = KuboClient::new(hot_url.clone());
    let cold = KuboClient::new(cold_url.clone());
    let hot_node = hot
        .local_node_identity()
        .await
        .expect("hot Kubo must answer /api/v0/id");
    let cold_node = cold
        .local_node_identity()
        .await
        .expect("cold Kubo must answer /api/v0/id");
    assert_ne!(
        hot_node, cold_node,
        "hot and cold Kubo node IDs must differ"
    );

    let schema = format!("transition_process_{}", uuid::Uuid::new_v4().simple());
    assert_valid_schema(&schema);
    base.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create isolated process-transition schema");
    base.close().await.expect("close prerequisite connection");
    let mut schema_cleanup = SchemaCleanup::new(postgres_url.clone(), schema.clone());
    let db = connect(&postgres_url, Some(&schema), 1).await;
    store::run_migrations(&db)
        .await
        .expect("migrate isolated process-transition schema");
    db.execute_unprepared(
        "CREATE TABLE transition_process_control (\
           child_id TEXT PRIMARY KEY, command TEXT NOT NULL, status TEXT NOT NULL, \
           updated_at TIMESTAMPTZ NOT NULL\
         )",
    )
    .await
    .expect("create test-only process control table");

    let proxy = KuboProxy::start(cold_url.clone()).await;
    let gateway = ProcessGateway::start(db.clone(), hot.clone(), cold.clone()).await;
    let gateway_endpoint = gateway.endpoint().to_owned();
    let outcome = AssertUnwindSafe(async {
        crash_checkpoint_chain(
            &postgres_url,
            &schema,
            &db,
            &hot,
            &cold,
            &proxy,
            &gateway_endpoint,
        )
        .await;
        stale_actor_is_fenced(
            &postgres_url,
            &schema,
            &db,
            &hot,
            &cold,
            &proxy,
            &gateway_endpoint,
        )
        .await;
        cancellation_and_retry(
            &postgres_url,
            &schema,
            &db,
            &hot,
            &cold,
            &proxy,
            &gateway_endpoint,
        )
        .await;
    })
    .catch_unwind()
    .await;

    proxy.disarm();
    if outcome.is_ok() {
        proxy.assert_no_pin_rm();
    }
    proxy.shutdown().await;
    if outcome.is_err() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    gateway.shutdown().await;
    db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("drop isolated process-transition schema");
    schema_cleanup.disarm();
    db.close().await.expect("close process-transition database");
    eprintln!("transition-process evidence event=schema-cleanup result=complete");
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

async fn crash_checkpoint_chain(
    postgres_url: &str,
    schema: &str,
    db: &DatabaseConnection,
    hot: &KuboClient,
    cold: &KuboClient,
    proxy: &KuboProxy,
    gateway_endpoint: &str,
) {
    let object = seed_transition(db, hot, "checkpoint-chain").await;
    let bucket = object.bucket.clone();
    let import_gate = proxy.arm("/api/v0/dag/import", 1);
    let mut prepare_child = spawn_child(
        "checkpoint-prepare",
        postgres_url,
        schema,
        proxy.endpoint(),
        2,
    )
    .await;
    import_gate.wait().await;
    let prepared = wait_saga_checkpoint(db, &bucket, "prepare").await;
    assert!(prepared.verification_receipt.is_none());
    assert!(prepared.publication_receipt.is_none());
    cold.verify_local_residency(&prepared.cid)
        .await
        .expect("proxy gate is reached only after real cold import completed");

    let before_heartbeat = action(db, &bucket).await;
    let heartbeat_started = Instant::now();
    tokio::time::sleep(Duration::from_secs(31)).await;
    let after_heartbeat = action(db, &bucket).await;
    assert_eq!(after_heartbeat.claim_epoch, before_heartbeat.claim_epoch);
    assert!(
        after_heartbeat
            .lease_until
            .expect("claimed action has a heartbeat lease")
            > before_heartbeat
                .lease_until
                .expect("initial claim has a lease"),
        "production worker heartbeat must renew while copy I/O is held"
    );
    assert!(heartbeat_started.elapsed() > Duration::from_secs(30));
    eprintln!(
        "transition-process evidence event=heartbeat checkpoint=prepare epoch={} held_secs={}",
        after_heartbeat.claim_epoch,
        heartbeat_started.elapsed().as_secs()
    );
    kill_child(&mut prepare_child).await;
    import_gate.release();
    proxy.disarm();
    assert_eq!(saga(db, &bucket).await.checkpoint, "prepare");
    let first_epoch = after_heartbeat.claim_epoch;
    eprintln!(
        "transition-process evidence event=checkpoint-survived checkpoint=prepare epoch={first_epoch}"
    );
    wait_lease_expired(db, &bucket).await;

    let verify_gate = proxy.arm("/api/v0/files/stat", 2);
    let mut copy_child =
        spawn_child("checkpoint-copy", postgres_url, schema, proxy.endpoint(), 2).await;
    verify_gate.wait().await;
    let copied = wait_saga_checkpoint(db, &bucket, "copy").await;
    assert!(copied.verification_receipt.is_none());
    let second_epoch = action(db, &bucket).await.claim_epoch;
    assert!(second_epoch > first_epoch, "restart must claim a new epoch");
    eprintln!(
        "transition-process evidence event=restart checkpoint=copy old_epoch={first_epoch} new_epoch={second_epoch}"
    );
    kill_child(&mut copy_child).await;
    verify_gate.release();
    proxy.disarm();
    assert_eq!(saga(db, &bucket).await.checkpoint, "copy");
    wait_lease_expired(db, &bucket).await;

    let publish_gate = AdvisoryTrigger::install(db, postgres_url, schema, "publish", 7_301).await;
    let mut verify_child = spawn_child(
        "checkpoint-verify",
        postgres_url,
        schema,
        proxy.endpoint(),
        2,
    )
    .await;
    wait_blocked_checkpoint(db, &bucket, "verify").await;
    let verified = saga(db, &bucket).await;
    assert!(verified.verification_receipt.is_some());
    assert!(verified.publication_receipt.is_none());
    let third_epoch = action(db, &bucket).await.claim_epoch;
    assert!(third_epoch > second_epoch);
    eprintln!(
        "transition-process evidence event=restart checkpoint=verify old_epoch={second_epoch} new_epoch={third_epoch}"
    );
    kill_child(&mut verify_child).await;
    publish_gate.release(db).await;
    assert_eq!(saga(db, &bucket).await.checkpoint, "verify");
    wait_lease_expired(db, &bucket).await;

    let cleanup_gate = AdvisoryTrigger::install(db, postgres_url, schema, "cleanup", 7_302).await;
    let mut publish_child = spawn_child(
        "checkpoint-publish",
        postgres_url,
        schema,
        proxy.endpoint(),
        2,
    )
    .await;
    wait_blocked_checkpoint(db, &bucket, "publish").await;
    let published = saga(db, &bucket).await;
    assert!(published.verification_receipt.is_some());
    let publication = published
        .publication_receipt
        .as_deref()
        .expect("publication receipt must commit before cleanup starts");
    let publication_json: serde_json::Value =
        serde_json::from_str(publication).expect("publication receipt is JSON");
    let fourth_epoch = action(db, &bucket).await.claim_epoch;
    assert_eq!(publication_json["claim_epoch"].as_i64(), Some(fourth_epoch));
    assert_residency(db, &bucket, "cold", "STANDARD_IA").await;
    eprintln!(
        "transition-process evidence event=restart checkpoint=publish old_epoch={third_epoch} new_epoch={fourth_epoch}"
    );
    kill_child(&mut publish_child).await;
    cleanup_gate.release(db).await;
    assert_eq!(saga(db, &bucket).await.checkpoint, "publish");
    assert_gateway_object(
        gateway_endpoint,
        &object,
        "STANDARD_IA",
        cold,
        "publish-checkpoint recovery",
    )
    .await;
    wait_lease_expired(db, &bucket).await;

    let mut cleanup_child = spawn_child(
        "checkpoint-cleanup",
        postgres_url,
        schema,
        proxy.endpoint(),
        2,
    )
    .await;
    wait_action_state(db, &bucket, "succeeded").await;
    let cleaned = wait_saga_checkpoint(db, &bucket, "cleanup").await;
    assert_eq!(cleaned.settlement_kind.as_deref(), Some("cleanup_complete"));
    let cleanup_epoch = action(db, &bucket).await.claim_epoch;
    assert!(cleanup_epoch > fourth_epoch);
    eprintln!(
        "transition-process evidence event=cleanup checkpoint=cleanup old_epoch={fourth_epoch} new_epoch={cleanup_epoch} result=complete"
    );
    assert_gateway_object(
        gateway_endpoint,
        &object,
        "STANDARD_IA",
        cold,
        "cleanup-checkpoint recovery",
    )
    .await;
    graceful_stop(db, &mut cleanup_child, "checkpoint-cleanup").await;
}

async fn stale_actor_is_fenced(
    postgres_url: &str,
    schema: &str,
    db: &DatabaseConnection,
    hot: &KuboClient,
    cold: &KuboClient,
    proxy: &KuboProxy,
    gateway_endpoint: &str,
) {
    let object = seed_transition(db, hot, "stale-actor").await;
    let bucket = object.bucket.clone();
    let import_gate = proxy.arm("/api/v0/dag/import", 1);
    let mut old_child = spawn_child("stale-old", postgres_url, schema, proxy.endpoint(), 1).await;
    import_gate.wait().await;
    let old = action(db, &bucket).await;

    let blocker_db = connect(postgres_url, Some(schema), 1).await;
    let blocker = blocker_db
        .begin()
        .await
        .expect("begin stale-actor row lock");
    blocker
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT id FROM lifecycle_actions WHERE id = $1 FOR UPDATE",
            [old.id.clone().into()],
        ))
        .await
        .expect("lock claimed action against heartbeat and takeover");
    wait_for_blocked_backend(db).await;
    wait_until(db, "old lease expiration", || async {
        action(db, &bucket).await.lease_expired
    })
    .await;

    let mut replacement = spawn_child("stale-new", postgres_url, schema, proxy.endpoint(), 1).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    blocker
        .commit()
        .await
        .expect("release stale-actor row lock");
    blocker_db.close().await.expect("close blocker pool");
    let new_epoch = wait_new_epoch(db, &bucket, old.claim_epoch).await;
    assert!(
        old_child
            .child
            .try_wait()
            .expect("query stale worker process")
            .is_none(),
        "stale worker process must remain alive while its epoch is fenced"
    );
    import_gate.release();
    proxy.disarm();
    wait_action_state(db, &bucket, "succeeded").await;
    let finished = saga(db, &bucket).await;
    let receipt: serde_json::Value = serde_json::from_str(
        finished
            .publication_receipt
            .as_deref()
            .expect("replacement publishes"),
    )
    .expect("publication receipt JSON");
    assert_eq!(receipt["claim_epoch"].as_i64(), Some(new_epoch));
    assert_ne!(receipt["claim_epoch"].as_i64(), Some(old.claim_epoch));
    eprintln!(
        "transition-process evidence event=stale-fenced old_epoch={} new_epoch={} stale_process=alive",
        old.claim_epoch, new_epoch
    );
    assert_gateway_object(
        gateway_endpoint,
        &object,
        "STANDARD_IA",
        cold,
        "stale-actor replacement publication",
    )
    .await;
    graceful_stop(db, &mut old_child, "stale-old").await;
    graceful_stop(db, &mut replacement, "stale-new").await;
}

async fn cancellation_and_retry(
    postgres_url: &str,
    schema: &str,
    db: &DatabaseConnection,
    hot: &KuboClient,
    cold: &KuboClient,
    proxy: &KuboProxy,
    gateway_endpoint: &str,
) {
    let cancel_object = seed_transition(db, hot, "cancelled-policy").await;
    let cancel_bucket = cancel_object.bucket.clone();
    let gate = proxy.arm("/api/v0/dag/import", 1);
    let mut cancel_child =
        spawn_child("cancel-policy", postgres_url, schema, proxy.endpoint(), 2).await;
    gate.wait().await;
    store::lifecycle_config::delete_configuration(db, &cancel_bucket)
        .await
        .expect("delete transition policy while copy is in flight");
    gate.release();
    proxy.disarm();
    wait_action_state(db, &cancel_bucket, "cancelled").await;
    let cancelled = saga(db, &cancel_bucket).await;
    assert!(cancelled.publication_receipt.is_none());
    assert_eq!(cancelled.settlement_kind.as_deref(), Some("cancelled"));
    assert_residency(db, &cancel_bucket, "hot", "STANDARD").await;
    eprintln!(
        "transition-process evidence event=policy-delete checkpoint={} publication=absent result=cancelled",
        cancelled.checkpoint
    );
    assert_gateway_object(
        gateway_endpoint,
        &cancel_object,
        "STANDARD",
        hot,
        "stale-policy cancellation",
    )
    .await;
    graceful_stop(db, &mut cancel_child, "cancel-policy").await;

    let deleted_object = seed_transition(db, hot, "deleted-target").await;
    let deleted_bucket = deleted_object.bucket.clone();
    let deleted_gate = proxy.arm("/api/v0/dag/import", 1);
    let mut deleted_child =
        spawn_child("deleted-target", postgres_url, schema, proxy.endpoint(), 2).await;
    deleted_gate.wait().await;
    delete_gateway_object(gateway_endpoint, &deleted_object).await;
    deleted_gate.release();
    proxy.disarm();
    wait_action_state(db, &deleted_bucket, "cancelled").await;
    assert_no_publication(db, &deleted_bucket).await;
    assert_gateway_no_such_key(gateway_endpoint, &deleted_object).await;
    eprintln!(
        "transition-process evidence event=target-delete publication=absent result=cancelled"
    );
    graceful_stop(db, &mut deleted_child, "deleted-target").await;

    let retry_object = seed_transition(db, hot, "retry-restart").await;
    let retry_bucket = retry_object.bucket.clone();
    let mut failing = spawn_child(
        "retry-failing",
        postgres_url,
        schema,
        "http://127.0.0.1:1",
        2,
    )
    .await;
    wait_until(db, "transition retry", || async {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT state, attempts, failure_class FROM lifecycle_actions WHERE bucket = $1 \
             ORDER BY created_at DESC LIMIT 1",
            [retry_bucket.clone().into()],
        ))
        .await
        .expect("poll transition retry")
        .is_some_and(|row| {
            row.try_get::<String>("", "state").ok().as_deref() == Some("pending")
                && row
                    .try_get::<i64>("", "attempts")
                    .is_ok_and(|attempts| attempts >= 1)
                && row
                    .try_get::<Option<String>>("", "failure_class")
                    .is_ok_and(|failure| failure.is_some())
        })
    })
    .await;
    graceful_stop(db, &mut failing, "retry-failing").await;
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE lifecycle_actions SET next_attempt_at = clock_timestamp() - interval '1 second' \
         WHERE bucket = $1 AND state = 'pending'",
        [retry_bucket.clone().into()],
    ))
    .await
    .expect("make persisted retry immediately due");
    let mut recovered =
        spawn_child("retry-recovered", postgres_url, schema, proxy.endpoint(), 2).await;
    wait_action_state(db, &retry_bucket, "succeeded").await;
    assert!(action(db, &retry_bucket).await.attempts >= 2);
    assert_residency(db, &retry_bucket, "cold", "STANDARD_IA").await;
    eprintln!(
        "transition-process evidence event=retry-restart attempts={} result=succeeded",
        action(db, &retry_bucket).await.attempts
    );
    assert_gateway_object(
        gateway_endpoint,
        &retry_object,
        "STANDARD_IA",
        cold,
        "persisted retry publication",
    )
    .await;
    graceful_stop(db, &mut recovered, "retry-recovered").await;

    let exhausted_object = seed_transition(db, hot, "retry-exhausted").await;
    let exhausted_bucket = exhausted_object.bucket.clone();
    let mut exhausted = spawn_child(
        "retry-exhausted",
        postgres_url,
        schema,
        "http://127.0.0.1:1",
        2,
    )
    .await;
    for expected_attempts in 1..=8_i64 {
        let expected_state = if expected_attempts == 8 {
            "cancelled"
        } else {
            "pending"
        };
        wait_until(db, "retry exhaustion attempt", || async {
            action_outcome(db, &exhausted_bucket)
                .await
                .is_some_and(|row| row.attempts >= expected_attempts && row.state == expected_state)
        })
        .await;
        eprintln!(
            "transition-process evidence event=retry-attempt attempt={expected_attempts} state={expected_state}"
        );
        if expected_attempts < 8 {
            let updated = db
                .execute(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "UPDATE lifecycle_actions SET next_attempt_at = clock_timestamp() - interval '1 second' \
                     WHERE bucket = $1 AND state = 'pending' AND attempts = $2",
                    [exhausted_bucket.clone().into(), expected_attempts.into()],
                ))
                .await
                .expect("make next exhaustion attempt immediately due");
            assert_eq!(
                updated.rows_affected(),
                1,
                "retry attempt must persist before advancing exhaustion"
            );
        }
    }
    let terminal = action_outcome(db, &exhausted_bucket)
        .await
        .expect("exhausted action exists");
    assert_eq!(terminal.state, "cancelled");
    assert_eq!(terminal.attempts, 8);
    assert_eq!(
        terminal.failure_class.as_deref(),
        Some("internal_dependency")
    );
    assert_eq!(
        terminal.last_error_redacted.as_deref(),
        Some("transition_attempts_exhausted")
    );
    assert_residency(db, &exhausted_bucket, "hot", "STANDARD").await;
    assert_no_publication(db, &exhausted_bucket).await;
    eprintln!(
        "transition-process evidence event=retry-exhausted attempts=8 publication=absent result=cancelled"
    );
    assert_gateway_object(
        gateway_endpoint,
        &exhausted_object,
        "STANDARD",
        hot,
        "retry exhaustion",
    )
    .await;
    graceful_stop(db, &mut exhausted, "retry-exhausted").await;
}

struct SeededTransition {
    bucket: String,
    key: String,
    body: Vec<u8>,
    cid: String,
    public_version_id: String,
}

async fn seed_transition(
    db: &DatabaseConnection,
    hot: &KuboClient,
    label: &str,
) -> SeededTransition {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let bucket = format!("proc-{label}-{suffix}");
    let key = "object.bin".to_owned();
    let object_id = format!("object-{suffix}");
    pg_residency::create_bucket(
        db,
        &bucket,
        // Expose the public null version in GET/HEAD evidence rather than the
        // intentionally hidden unversioned projection.
        store::object_version::BucketVersioningState::Suspended,
    )
    .await;
    let configuration = CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some("process-transition".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: None,
            noncurrent_version_expiration: None,
            transition: Some(CurrentTransition::Date {
                utc_midnight: Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            }),
            noncurrent_version_transition: None,
            abort_incomplete_multipart_upload: None,
        }],
    };
    store::lifecycle_config::put_configuration(
        db,
        &bucket,
        &canonical_json(&configuration).expect("serialize transition policy"),
    )
    .await
    .expect("store transition policy");

    let body = format!("process-transition::{label}::{suffix}").into_bytes();
    let logical_size = i64::try_from(body.len()).expect("process fixture size fits i64");
    let cid = kubo_add(hot, body.clone()).await;
    let verification = hot
        .verify_local_residency(&cid)
        .await
        .expect("new hot object must be recursively pinned and local");
    publish_object(
        db,
        process_publication_request(&object_id, &bucket, &key, &cid, logical_size),
        &pg_residency::provider_limits(),
    )
    .await
    .expect("publish correctly sized process fixture object");
    let version = pg_residency::version_for_object(db, &object_id).await;
    let public_version_id = version
        .version_id
        .clone()
        .unwrap_or_else(|| "null".to_owned());
    let verification_json =
        serde_json::to_string(&verification).expect("serialize hot verification receipt");
    let txn = db.begin().await.expect("begin hot verification attach");
    store::residency::attach_hot_in_transaction(
        &txn,
        &VersionResidencyIdentity::new(version.id, object_id, cid.clone()),
        &PhysicalVerification::verified(verification.node_identity, verification_json),
    )
    .await
    .expect("attach verified hot residency");
    txn.commit().await.expect("commit verified hot residency");
    SeededTransition {
        bucket,
        key,
        body,
        cid,
        public_version_id,
    }
}

fn process_publication_request(
    object_id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    logical_size: i64,
) -> PublicationRequest {
    let object = PublicationObject::from_put(
        object_id.to_owned(),
        bucket,
        key,
        cid.to_owned(),
        logical_size,
        Some("application/octet-stream".to_owned()),
        None,
        false,
        None,
        None,
        Utc::now(),
    );
    PublicationRequest {
        object_target: PinTargetSpec {
            cid: object.cid.clone(),
            logical_size: object.logical_size,
        },
        object,
        tags: Vec::new(),
        policy: PublicationPolicy {
            tags: Vec::new(),
            leases: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::process_publication_request;

    #[test]
    fn process_publication_size_matches_actual_payload() {
        let payload = b"process-transition::checkpoint-chain::fixture";
        let request = process_publication_request(
            "object-fixture",
            "proc-fixture",
            "object.bin",
            "bafkreigh2akiscaildc6e3nwj5o5wpl3f5aa5h7uh3bvt6buef2vity",
            payload.len() as i64,
        );

        assert_eq!(request.object.logical_size, payload.len() as i64);
        assert_eq!(request.object_target.logical_size, payload.len() as i64);
    }
}

async fn kubo_add(kubo: &KuboClient, bytes: Vec<u8>) -> String {
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(bytes).file_name("process-fixture.bin"),
    );
    let response = kubo
        .upload_http()
        .post(format!(
            "{}/api/v0/add?cid-version=1&pin=true&wrap-with-directory=false",
            kubo.base_url()
        ))
        .multipart(form)
        .send()
        .await
        .expect("send fixture add to hot Kubo");
    assert!(response.status().is_success(), "hot Kubo add failed");
    let body = response.text().await.expect("read Kubo add response");
    body.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|value| value["Hash"].as_str().map(str::to_owned))
        .next_back()
        .expect("Kubo add response must contain Hash")
}

struct ChildProcess {
    id: String,
    child: Child,
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.start_kill();
            eprintln!(
                "transition-process evidence event=panic-cleanup actor={} action=kill-requested",
                self.id
            );
        }
    }
}

async fn spawn_child(
    id: &str,
    postgres_url: &str,
    schema: &str,
    cold_url: &str,
    lease_secs: u64,
) -> ChildProcess {
    let mut child = Command::new(std::env::current_exe().expect("locate integration test binary"))
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--ignored")
        .arg("--nocapture")
        .env(CHILD_ROLE_ENV, id)
        .env(CHILD_SCHEMA_ENV, schema)
        .env(CHILD_COLD_ENV, cold_url)
        .env(CHILD_LEASE_ENV, lease_secs.to_string())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn lifecycle worker child test process");
    wait_until_control(postgres_url, schema, id, "ready").await;
    assert!(child.try_wait().expect("query child status").is_none());
    eprintln!("transition-process evidence event=child-ready actor={id} lease_secs={lease_secs}");
    ChildProcess {
        id: id.to_owned(),
        child,
    }
}

async fn kill_child(child: &mut ChildProcess) {
    eprintln!(
        "transition-process evidence event=process-kill actor={} action=requested",
        child.id
    );
    child
        .child
        .kill()
        .await
        .unwrap_or_else(|error| panic!("kill worker child {}: {error}", child.id));
    let status = tokio::time::timeout(Duration::from_secs(10), child.child.wait())
        .await
        .expect("killed child must be reaped")
        .expect("wait for killed child");
    assert!(
        !status.success(),
        "killed child unexpectedly exited successfully"
    );
    eprintln!(
        "transition-process evidence event=process-kill actor={} result=reaped",
        child.id
    );
}

async fn graceful_stop(db: &DatabaseConnection, child: &mut ChildProcess, id: &str) {
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE transition_process_control SET command = 'stop', updated_at = clock_timestamp() \
         WHERE child_id = $1",
        [id.into()],
    ))
    .await
    .expect("request graceful worker shutdown");
    let status = tokio::time::timeout(Duration::from_secs(10), child.child.wait())
        .await
        .expect("graceful child shutdown timed out")
        .expect("wait for graceful child");
    assert!(status.success(), "graceful child {id} failed: {status}");
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT status FROM transition_process_control WHERE child_id = $1",
            [id.into()],
        ))
        .await
        .expect("read graceful shutdown status")
        .expect("child control row exists");
    assert_eq!(row.try_get::<String>("", "status").unwrap(), "stopped");
    eprintln!("transition-process evidence event=shutdown actor={id} result=stopped");
}

async fn wait_until_control(postgres_url: &str, schema: &str, id: &str, expected: &str) {
    let db = connect(postgres_url, Some(schema), 1).await;
    wait_until(&db, "child readiness", || async {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT status FROM transition_process_control WHERE child_id = $1",
            [id.into()],
        ))
        .await
        .expect("query child readiness")
        .and_then(|row| row.try_get::<String>("", "status").ok())
        .is_some_and(|status| status == expected)
    })
    .await;
    db.close().await.expect("close readiness connection");
}

#[derive(Debug)]
struct ActionRow {
    id: String,
    attempts: i64,
    claim_epoch: i64,
    lease_until: Option<DateTime<Utc>>,
    lease_expired: bool,
}

async fn action(db: &DatabaseConnection, bucket: &str) -> ActionRow {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT id, attempts, claim_epoch, lease_until, \
                    lease_until IS NOT NULL AND lease_until <= clock_timestamp() AS lease_expired \
             FROM lifecycle_actions WHERE bucket = $1 ORDER BY created_at DESC LIMIT 1",
            [bucket.into()],
        ))
        .await
        .expect("query lifecycle action")
        .unwrap_or_else(|| panic!("lifecycle worker did not schedule an action for {bucket}"));
    ActionRow {
        id: row.try_get("", "id").unwrap(),
        attempts: row.try_get("", "attempts").unwrap(),
        claim_epoch: row.try_get("", "claim_epoch").unwrap(),
        lease_until: row.try_get("", "lease_until").unwrap(),
        lease_expired: row.try_get("", "lease_expired").unwrap(),
    }
}

struct ActionOutcome {
    state: String,
    attempts: i64,
    failure_class: Option<String>,
    last_error_redacted: Option<String>,
}

async fn action_outcome(db: &DatabaseConnection, bucket: &str) -> Option<ActionOutcome> {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT state, attempts, failure_class, last_error_redacted FROM lifecycle_actions \
         WHERE bucket = $1 ORDER BY created_at DESC LIMIT 1",
        [bucket.into()],
    ))
    .await
    .expect("query lifecycle action outcome")
    .map(|row| ActionOutcome {
        state: row.try_get("", "state").unwrap(),
        attempts: row.try_get("", "attempts").unwrap(),
        failure_class: row.try_get("", "failure_class").unwrap(),
        last_error_redacted: row.try_get("", "last_error_redacted").unwrap(),
    })
}

#[derive(Debug)]
struct SagaRow {
    checkpoint: String,
    cid: String,
    verification_receipt: Option<String>,
    publication_receipt: Option<String>,
    settlement_kind: Option<String>,
}

async fn saga(db: &DatabaseConnection, bucket: &str) -> SagaRow {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT checkpoint, destination_cid, verification_receipt, publication_receipt, \
                    settlement_kind FROM lifecycle_transitions WHERE bucket = $1 \
             ORDER BY created_at DESC LIMIT 1",
            [bucket.into()],
        ))
        .await
        .expect("query lifecycle transition")
        .unwrap_or_else(|| panic!("transition saga does not exist for {bucket}"));
    SagaRow {
        checkpoint: row.try_get("", "checkpoint").unwrap(),
        cid: row.try_get("", "destination_cid").unwrap(),
        verification_receipt: row.try_get("", "verification_receipt").unwrap(),
        publication_receipt: row.try_get("", "publication_receipt").unwrap(),
        settlement_kind: row.try_get("", "settlement_kind").unwrap(),
    }
}

async fn wait_saga_checkpoint(db: &DatabaseConnection, bucket: &str, checkpoint: &str) -> SagaRow {
    wait_until(db, "transition checkpoint", || async {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT checkpoint FROM lifecycle_transitions WHERE bucket = $1",
            [bucket.into()],
        ))
        .await
        .expect("poll transition checkpoint")
        .and_then(|row| row.try_get::<String>("", "checkpoint").ok())
        .is_some_and(|value| value == checkpoint)
    })
    .await;
    saga(db, bucket).await
}

async fn wait_action_state(db: &DatabaseConnection, bucket: &str, expected: &str) {
    wait_until(db, "lifecycle action state", || async {
        let row = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT state FROM lifecycle_actions WHERE bucket = $1 ORDER BY created_at DESC LIMIT 1",
                [bucket.into()],
            ))
            .await
            .expect("poll lifecycle action state");
        row.and_then(|row| row.try_get::<String>("", "state").ok())
            .is_some_and(|state| state == expected)
    })
    .await;
}

async fn wait_lease_expired(db: &DatabaseConnection, bucket: &str) {
    wait_until(db, "claim lease expiry", || async {
        action(db, bucket).await.lease_expired
    })
    .await;
}

async fn wait_new_epoch(db: &DatabaseConnection, bucket: &str, old: i64) -> i64 {
    wait_until(db, "new claim epoch", || async {
        action(db, bucket).await.claim_epoch > old
    })
    .await;
    action(db, bucket).await.claim_epoch
}

async fn assert_residency(
    db: &DatabaseConnection,
    bucket: &str,
    expected_tier: &str,
    expected_class: &str,
) {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT vr.primary_tier, vr.storage_class FROM version_residencies vr \
             JOIN object_versions ov ON ov.id = vr.version_row_id \
             WHERE ov.bucket = $1 ORDER BY ov.created_at DESC LIMIT 1",
            [bucket.into()],
        ))
        .await
        .expect("query process-fixture residency")
        .expect("process-fixture residency exists");
    assert_eq!(
        row.try_get::<String>("", "primary_tier").unwrap(),
        expected_tier
    );
    assert_eq!(
        row.try_get::<String>("", "storage_class").unwrap(),
        expected_class
    );
}

async fn assert_no_publication(db: &DatabaseConnection, bucket: &str) {
    let row = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT count(*)::bigint AS published FROM lifecycle_transitions \
             WHERE bucket = $1 AND publication_receipt IS NOT NULL",
            [bucket.into()],
        ))
        .await
        .expect("query transition publication count")
        .expect("transition publication count row");
    assert_eq!(row.try_get::<i64>("", "published").unwrap(), 0);
}

async fn wait_blocked_checkpoint(db: &DatabaseConnection, bucket: &str, checkpoint: &str) {
    wait_saga_checkpoint(db, bucket, checkpoint).await;
    wait_for_blocked_backend(db).await;
}

async fn wait_for_blocked_backend(db: &DatabaseConnection) {
    wait_until(db, "PostgreSQL advisory/row lock waiter", || async {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT count(*)::bigint AS blocked FROM pg_stat_activity \
                 WHERE pid <> pg_backend_pid() AND cardinality(pg_blocking_pids(pid)) > 0"
                    .to_owned(),
            ))
            .await
            .expect("query PostgreSQL blocked backends")
            .expect("blocked backend count row");
        row.try_get::<i64>("", "blocked").unwrap() > 0
    })
    .await;
}

async fn wait_until<F, Fut>(db: &DatabaseConnection, label: &str, mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let result = tokio::time::timeout(WAIT, async {
        loop {
            if predicate().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if result.is_err() {
        let activity = db
            .query_all(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT state, wait_event_type, wait_event FROM pg_stat_activity \
                 WHERE datname = current_database()"
                    .to_owned(),
            ))
            .await;
        panic!(
            "timed out waiting for {label}; pg activity available={}",
            activity.is_ok()
        );
    }
}

struct AdvisoryTrigger {
    blocker: DatabaseConnection,
    trigger: String,
    function: String,
    key: i64,
}

impl AdvisoryTrigger {
    async fn install(
        db: &DatabaseConnection,
        postgres_url: &str,
        schema: &str,
        checkpoint: &str,
        key: i64,
    ) -> Self {
        assert!(matches!(checkpoint, "publish" | "cleanup"));
        let function = format!("process_gate_{checkpoint}");
        let trigger = format!("process_gate_{checkpoint}_trigger");
        let old = if checkpoint == "publish" {
            "verify"
        } else {
            "publish"
        };
        db.execute_unprepared(&format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$; \
             CREATE TRIGGER {trigger} BEFORE UPDATE ON lifecycle_transitions \
             FOR EACH ROW WHEN (OLD.checkpoint = '{old}' AND NEW.checkpoint = '{checkpoint}') \
             EXECUTE FUNCTION {function}()"
        ))
        .await
        .expect("install schema-local transition checkpoint gate");
        let blocker = connect(postgres_url, Some(schema), 1).await;
        blocker
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT pg_advisory_lock($1)",
                [key.into()],
            ))
            .await
            .expect("hold transition checkpoint advisory lock");
        Self {
            blocker,
            trigger,
            function,
            key,
        }
    }

    async fn release(self, db: &DatabaseConnection) {
        let unlocked = self
            .blocker
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT pg_advisory_unlock($1) AS unlocked",
                [self.key.into()],
            ))
            .await
            .expect("release checkpoint advisory lock")
            .expect("advisory unlock row")
            .try_get::<bool>("", "unlocked")
            .expect("decode advisory unlock result");
        assert!(unlocked);
        self.blocker.close().await.expect("close advisory blocker");
        db.execute_unprepared(&format!(
            "DROP TRIGGER {} ON lifecycle_transitions; DROP FUNCTION {}()",
            self.trigger, self.function
        ))
        .await
        .expect("drop schema-local transition checkpoint gate");
    }
}

struct ProxyGate {
    path: String,
    occurrence: usize,
    seen: AtomicUsize,
    arrived: AtomicBool,
    released: AtomicBool,
    arrived_notify: Notify,
    release_notify: Notify,
}

impl ProxyGate {
    fn new(path: &str, occurrence: usize) -> Arc<Self> {
        assert!(path.starts_with('/'));
        assert!(occurrence > 0);
        Arc::new(Self {
            path: path.to_owned(),
            occurrence,
            seen: AtomicUsize::new(0),
            arrived: AtomicBool::new(false),
            released: AtomicBool::new(false),
            arrived_notify: Notify::new(),
            release_notify: Notify::new(),
        })
    }

    async fn maybe_wait(&self, path: &str) {
        if path != self.path {
            return;
        }
        let occurrence = self.seen.fetch_add(1, Ordering::AcqRel) + 1;
        if occurrence != self.occurrence {
            return;
        }
        self.arrived.store(true, Ordering::Release);
        self.arrived_notify.notify_waiters();
        while !self.released.load(Ordering::Acquire) {
            self.release_notify.notified().await;
        }
    }

    async fn wait(&self) {
        tokio::time::timeout(WAIT, async {
            while !self.arrived.load(Ordering::Acquire) {
                self.arrived_notify.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("proxy did not observe gated {}", self.path));
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
}

struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    gate: Mutex<Option<Arc<ProxyGate>>>,
    pin_rm_requests: AtomicUsize,
}

struct KuboProxy {
    endpoint: String,
    state: Arc<ProxyState>,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl KuboProxy {
    async fn start(upstream: String) -> Self {
        let state = Arc::new(ProxyState {
            upstream,
            client: reqwest::Client::builder()
                .build()
                .expect("build Kubo proxy client"),
            gate: Mutex::new(None),
            pin_rm_requests: AtomicUsize::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Kubo transition proxy");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let app = Router::new()
            .route("/{*path}", any(proxy_request))
            .with_state(state.clone());
        let join = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
                .expect("serve Kubo transition proxy");
        });
        Self {
            endpoint,
            state,
            shutdown,
            join,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn arm(&self, path: &str, occurrence: usize) -> Arc<ProxyGate> {
        let gate = ProxyGate::new(path, occurrence);
        let mut installed = self.state.gate.lock().unwrap();
        assert!(installed.is_none(), "only one Kubo proxy gate may be armed");
        *installed = Some(gate.clone());
        gate
    }

    fn disarm(&self) {
        if let Some(gate) = self.state.gate.lock().unwrap().take() {
            gate.release();
        }
    }

    fn assert_no_pin_rm(&self) {
        let requests = self.state.pin_rm_requests.load(Ordering::Acquire);
        assert_eq!(
            requests, 0,
            "lifecycle transition process suite must never issue cold Kubo pin/rm"
        );
        eprintln!(
            "transition-process evidence event=proxy-audit operation=pin-rm requests={requests}"
        );
    }

    async fn shutdown(mut self) {
        self.disarm();
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), &mut self.join)
            .await
            .expect("Kubo proxy shutdown timed out")
            .expect("Kubo proxy task panicked");
    }
}

impl Drop for KuboProxy {
    fn drop(&mut self) {
        self.disarm();
        self.shutdown.cancel();
        self.join.abort();
    }
}

async fn proxy_request(State(state): State<Arc<ProxyState>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    if path == "/api/v0/pin/rm" {
        state.pin_rm_requests.fetch_add(1, Ordering::AcqRel);
    }
    let suffix = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or_else(|| parts.uri.path());
    let target = format!("{}{}", state.upstream, suffix);
    let bytes = body
        .collect()
        .await
        .expect("collect test proxy request")
        .to_bytes();
    let mut outbound = state.client.request(parts.method, target);
    for (name, value) in &parts.headers {
        if name != header::HOST && name != header::CONTENT_LENGTH {
            outbound = outbound.header(name, value);
        }
    }
    let upstream = outbound
        .body(bytes)
        .send()
        .await
        .expect("forward request to real cold Kubo");
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let body = upstream.bytes().await.expect("read cold Kubo response");
    let gate = state.gate.lock().unwrap().clone();
    if status.is_success()
        && let Some(gate) = gate
    {
        gate.maybe_wait(&path).await;
    }
    response(status, headers, body)
}

fn response(status: reqwest::StatusCode, headers: HeaderMap, body: bytes::Bytes) -> Response {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        if let Some(name) = name
            && name != header::CONTENT_LENGTH
            && name != header::TRANSFER_ENCODING
            && name != header::CONNECTION
        {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from(body))
        .expect("build proxy response")
}

struct SchemaCleanup {
    postgres_url: String,
    schema: Option<String>,
}

impl SchemaCleanup {
    fn new(postgres_url: String, schema: String) -> Self {
        assert_valid_schema(&schema);
        Self {
            postgres_url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for SchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !valid_schema(&schema) {
            eprintln!(
                "transition-process evidence event=panic-cleanup resource=schema result=invalid-name"
            );
            return;
        }
        let postgres_url = self.postgres_url.clone();
        let cleanup = std::thread::Builder::new()
            .name("transition-process-schema-cleanup".to_owned())
            .spawn(move || {
                std::thread::sleep(Duration::from_millis(500));
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return false;
                };
                runtime.block_on(async move {
                    let mut options = ConnectOptions::new(postgres_url);
                    options.max_connections(1).min_connections(1);
                    let Ok(Ok(db)) =
                        tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
                            .await
                    else {
                        return false;
                    };
                    let _ = db.execute_unprepared("SET lock_timeout = '20s'").await;
                    let dropped = tokio::time::timeout(
                        Duration::from_secs(25),
                        db.execute_unprepared(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")),
                    )
                    .await
                    .is_ok_and(|result| result.is_ok());
                    let _ = db.close().await;
                    dropped
                })
            });
        let cleaned = cleanup
            .and_then(|thread| {
                thread
                    .join()
                    .map_err(|_| std::io::Error::other("schema cleanup thread panicked"))
            })
            .unwrap_or(false);
        eprintln!(
            "transition-process evidence event=panic-cleanup resource=schema result={}",
            if cleaned { "dropped" } else { "failed" }
        );
    }
}

async fn connect(
    postgres_url: &str,
    schema: Option<&str>,
    max_connections: u32,
) -> DatabaseConnection {
    let mut options = ConnectOptions::new(postgres_url.to_owned());
    options
        .max_connections(max_connections)
        .min_connections(1)
        .connect_timeout(Duration::from_secs(10));
    let db = tokio::time::timeout(Duration::from_secs(15), Database::connect(options))
        .await
        .expect("PostgreSQL connection timed out")
        .expect("connect to runner-owned transition PostgreSQL");
    if let Some(schema) = schema {
        assert_valid_schema(schema);
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .expect("select isolated process-transition schema");
    }
    db
}

async fn assert_pg17(db: &DatabaseConnection) {
    let version = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version_num".to_owned(),
        ))
        .await
        .expect("query PostgreSQL version")
        .expect("PostgreSQL version row")
        .try_get::<String>("", "server_version_num")
        .expect("decode PostgreSQL version")
        .parse::<u32>()
        .expect("numeric PostgreSQL version");
    assert!(
        (170_000..180_000).contains(&version),
        "process fixture requires PostgreSQL 17, got {version}"
    );
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("NOT RUN: {name} is required"))
}

fn endpoint_env(name: &str) -> String {
    let value = required_env(name).trim().trim_end_matches('/').to_owned();
    let parsed = url::Url::parse(&value)
        .unwrap_or_else(|_| panic!("{name} must be an absolute HTTP(S) URL"));
    assert!(
        matches!(parsed.scheme(), "http" | "https")
            && parsed.host_str().is_some()
            && (parsed.path().is_empty() || parsed.path() == "/")
            && parsed.query().is_none()
            && parsed.fragment().is_none(),
        "{name} must be an origin-only HTTP(S) URL"
    );
    value
}

fn assert_valid_schema(schema: &str) {
    assert!(
        valid_schema(schema),
        "invalid isolated process-transition schema"
    );
}

fn valid_schema(schema: &str) -> bool {
    schema
        .strip_prefix("transition_process_")
        .is_some_and(|suffix| {
            suffix.len() == 32
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}
