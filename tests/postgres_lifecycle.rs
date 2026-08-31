use std::{sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use ipfs_s3_gateway::{
    config::{LifecycleWorkerConfig, ValidatedLifecycleConfig},
    lifecycle::model::{
        CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
        CanonicalRuleSelector, CurrentExpiration, LifecycleActionKind, LifecycleRuleStatus,
        LifecycleScanCursor, LifecycleScanSource, NewLifecycleAction, RuleIdentity,
        VersionTargetIdentity,
    },
    store::{
        self, Store,
        database_clock::database_now,
        entities::{import_destination, lifecycle_action, object, object_version},
        lifecycle_action::{
            claim_due, claim_due_with_max_attempts, idempotency_key, insert_idempotent,
            lock_claim_for_execution, mark_succeeded,
        },
        lifecycle_config::{claim_next_scan, finish_scan_page, put_configuration},
        migrations::{
            m20250701_000001_init, m20260707_000001_decompress_zip,
            m20260720_000001_sse_c_key_fingerprint, m20260721_000001_multi_provider_pinning,
            m20260729_000001_ipfs3_import, m20260729_000002_postgres_utc_timestamps,
            m20260730_000001_standard_mutation_fence, m20260813_000001_postgres_json_columns,
            m20260826_000001_lifecycle_expiration,
        },
        object_version::{PublicVersionId, VersionKind},
    },
};
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    DbErr, EntityTrait, QueryFilter, Statement, sea_query::Expr,
};
use sea_orm_migration::{MigrationTrait, MigratorTrait, SchemaManager};
use tokio_util::sync::CancellationToken;

struct PreLifecycleMigrator;

impl MigratorTrait for PreLifecycleMigrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
        ]
    }
}

struct OwnedPgSchemaCleanup {
    url: String,
    schema: Option<String>,
}

impl OwnedPgSchemaCleanup {
    fn new(url: String, schema: String) -> Self {
        assert!(is_owned_lifecycle_schema(&schema));
        Self {
            url,
            schema: Some(schema),
        }
    }

    fn disarm(&mut self) {
        self.schema = None;
    }
}

impl Drop for OwnedPgSchemaCleanup {
    fn drop(&mut self) {
        let Some(schema) = self.schema.take() else {
            return;
        };
        if !is_owned_lifecycle_schema(&schema) {
            return;
        }
        let url = self.url.clone();
        if let Ok(thread) = std::thread::Builder::new()
            .name("postgres-lifecycle-schema-cleanup".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(async move {
                    let mut options = ConnectOptions::new(url);
                    options.max_connections(1).min_connections(1);
                    let Ok(Ok(db)) =
                        tokio::time::timeout(Duration::from_secs(10), Database::connect(options))
                            .await
                    else {
                        return;
                    };
                    let _ = tokio::time::timeout(
                        Duration::from_secs(10),
                        db.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE")),
                    )
                    .await;
                    let _ = tokio::time::timeout(Duration::from_secs(10), db.close()).await;
                });
            })
        {
            let _ = thread.join();
        }
    }
}

struct PgFixture {
    db: DatabaseConnection,
    schema: String,
    url: String,
    cleanup_guard: OwnedPgSchemaCleanup,
}

impl PgFixture {
    async fn independent_connection(&self) -> DatabaseConnection {
        let mut options = ConnectOptions::new(self.url.clone());
        options.max_connections(1).min_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {}", self.schema))
            .await
            .unwrap();
        db
    }

    async fn cleanup(mut self) {
        self.db
            .execute_unprepared(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
        self.cleanup_guard.disarm();
        self.db.close().await.unwrap();
    }
}

fn is_owned_lifecycle_schema(schema: &str) -> bool {
    schema.strip_prefix("lifecycle_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn lifecycle_fixture() -> Option<PgFixture> {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        eprintln!("skipping PostgreSQL lifecycle tests: IPFS_S3_TEST_POSTGRES_URL is unset");
        return None;
    };
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    let schema = format!("lifecycle_{}", uuid::Uuid::new_v4().simple());
    let cleanup_guard = OwnedPgSchemaCleanup::new(url.clone(), schema.clone());
    db.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    PreLifecycleMigrator::up(&db, None).await.unwrap();
    db.execute_unprepared("INSERT INTO buckets (name) VALUES ('lifecycle-bucket')")
        .await
        .unwrap();
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest, created_at) \
         VALUES ('legacy-hidden', 'lifecycle-bucket', 'key', 'cid-hidden', 1, 'cid-hidden', FALSE, \
                 '2026-08-25 00:00:00+00:00'), \
                ('current-object', 'lifecycle-bucket', 'key', 'cid-current', 1, 'cid-current', TRUE, \
                 '2026-08-25 00:01:00+00:00')",
    )
    .await
    .unwrap();
    store::run_migrations(&db).await.unwrap();
    Some(PgFixture {
        db,
        schema,
        url,
        cleanup_guard,
    })
}

async fn table_exists(db: &DatabaseConnection, table: &str) -> bool {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT 1 FROM information_schema.tables \
         WHERE table_schema = current_schema() AND table_name = $1",
        [table.to_owned().into()],
    ))
    .await
    .unwrap()
    .is_some()
}

#[tokio::test]
async fn postgres_lifecycle_migration_and_database_clock_are_engine_owned() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let before = Utc::now();
    let now = database_now(&fixture.db).await.unwrap();
    let after = Utc::now();

    assert!(
        now >= before - chrono::Duration::seconds(1) && now <= after + chrono::Duration::seconds(1),
        "PostgreSQL clock_timestamp() must be current"
    );
    assert!(table_exists(&fixture.db, "bucket_lifecycle_configs").await);
    assert!(table_exists(&fixture.db, "lifecycle_actions").await);
    let row = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT lifecycle_age_started_at, became_noncurrent_at FROM object_versions",
        ))
        .await
        .unwrap()
        .unwrap();
    let age: chrono::DateTime<chrono::Utc> = row.try_get("", "lifecycle_age_started_at").unwrap();
    let became_noncurrent: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("", "became_noncurrent_at").unwrap();
    assert_eq!(
        age,
        chrono::DateTime::parse_from_rfc3339("2026-08-25T00:01:00Z")
            .unwrap()
            .with_timezone(&Utc)
    );
    assert_eq!(became_noncurrent, None);

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_down_refuses_durable_configuration() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    fixture
        .db
        .execute_unprepared(
            "INSERT INTO bucket_lifecycle_configs \
             (bucket, canonical_json, revision, scan_lease_epoch, created_at, updated_at) \
             VALUES ('lifecycle-bucket', NULL, 1, 0, clock_timestamp(), clock_timestamp())",
        )
        .await
        .unwrap();

    let result = m20260826_000001_lifecycle_expiration::Migration
        .down(&SchemaManager::new(&fixture.db))
        .await;
    assert!(matches!(result, Err(DbErr::Migration(_))));
    assert!(table_exists(&fixture.db, "bucket_lifecycle_configs").await);

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_claim_uses_database_clock_locks_and_epoch_fences() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let canonical = r#"{"schema_version":1,"rules":[]}"#;
    put_configuration(&fixture.db, "lifecycle-bucket", canonical)
        .await
        .unwrap();

    let first_scan = claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
            .await
            .unwrap()
            .is_none()
    );
    fixture
        .db
        .execute_unprepared(
            "UPDATE bucket_lifecycle_configs \
             SET scan_lease_until = clock_timestamp() - INTERVAL '1 second' \
             WHERE bucket = 'lifecycle-bucket'",
        )
        .await
        .unwrap();
    let replacement_scan = claim_next_scan(&fixture.db, ChronoDuration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replacement_scan.lease_epoch, first_scan.lease_epoch + 1);
    let cursor = LifecycleScanCursor {
        source: LifecycleScanSource::Current,
        bucket: "lifecycle-bucket".to_owned(),
        key: "key".to_owned(),
        sequence: 1,
        version_row_id: "row-1".to_owned(),
    };
    assert!(
        !finish_scan_page(&fixture.db, &first_scan, Some(&cursor), false)
            .await
            .unwrap()
    );
    assert!(
        finish_scan_page(&fixture.db, &replacement_scan, Some(&cursor), false)
            .await
            .unwrap()
    );

    let now = database_now(&fixture.db).await.unwrap();
    let action = NewLifecycleAction {
        idempotency_key: String::new(),
        bucket: "lifecycle-bucket".to_owned(),
        config_revision: replacement_scan.config_revision,
        rule_identity: RuleIdentity::Ordinal(0),
        action_kind: LifecycleActionKind::ExpireCurrent,
        target: VersionTargetIdentity {
            bucket: "lifecycle-bucket".to_owned(),
            key: "key".to_owned(),
            version_row_id: "row-1".to_owned(),
            public_version_id: PublicVersionId::Opaque(
                "00000000-0000-4000-8000-000000000001".to_owned(),
            ),
            kind: VersionKind::Object,
            object_id: Some("current-object".to_owned()),
            sequence: 1,
        },
        due_at: now - ChronoDuration::seconds(1),
    };
    assert!(
        insert_idempotent(&fixture.db, action.clone(), now)
            .await
            .unwrap()
    );
    assert!(!insert_idempotent(&fixture.db, action, now).await.unwrap());
    let first_action = claim_due(&fixture.db, "pg-worker-a", ChronoDuration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        lock_claim_for_execution(&fixture.db, &first_action)
            .await
            .unwrap()
            .is_some()
    );
    fixture
        .db
        .execute_unprepared(&format!(
            "UPDATE lifecycle_actions \
             SET lease_until = clock_timestamp() - INTERVAL '1 second' \
             WHERE id = '{}'",
            first_action.action.id
        ))
        .await
        .unwrap();
    let replacement_action = claim_due(&fixture.db, "pg-worker-b", ChronoDuration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(replacement_action.claim_epoch, first_action.claim_epoch + 1);
    assert!(
        !mark_succeeded(
            &fixture.db,
            &first_action,
            database_now(&fixture.db).await.unwrap()
        )
        .await
        .unwrap()
    );

    fixture.cleanup().await;
}

#[tokio::test]
async fn postgres_lifecycle_final_recovery_crash_is_bounded_and_preserves_foreign_tokens() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let version = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("lifecycle-bucket"))
        .filter(object_version::Column::Key.eq("key"))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let configured_max_attempts = 2_i64;
    let terminal_attempt = configured_max_attempts + 1;

    for token_case in ["owned", "user"] {
        let now = database_now(&fixture.db).await.unwrap();
        let mut pending = NewLifecycleAction {
            idempotency_key: String::new(),
            bucket: "lifecycle-bucket".to_owned(),
            config_revision: 1,
            rule_identity: RuleIdentity::Id(format!("terminal-{token_case}")),
            action_kind: LifecycleActionKind::ExpireCurrent,
            target: VersionTargetIdentity {
                bucket: "lifecycle-bucket".to_owned(),
                key: "key".to_owned(),
                version_row_id: version.id.clone(),
                public_version_id: PublicVersionId::Null,
                kind: VersionKind::Object,
                object_id: version.object_id.clone(),
                sequence: version.sequence,
            },
            due_at: now - ChronoDuration::seconds(1),
        };
        pending.idempotency_key = idempotency_key(&pending).unwrap();
        let key = pending.idempotency_key.clone();
        assert!(insert_idempotent(&fixture.db, pending, now).await.unwrap());
        let action = lifecycle_action::Entity::find()
            .filter(lifecycle_action::Column::IdempotencyKey.eq(key))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(lifecycle_action::Column::State, Expr::value("claimed"))
            .col_expr(
                lifecycle_action::Column::Attempts,
                Expr::value(terminal_attempt),
            )
            .col_expr(
                lifecycle_action::Column::ClaimEpoch,
                Expr::value(terminal_attempt),
            )
            .col_expr(
                lifecycle_action::Column::ClaimedBy,
                Expr::value(Some("crashed-final-worker".to_owned())),
            )
            .col_expr(
                lifecycle_action::Column::LeaseUntil,
                Expr::value(Some(now - ChronoDuration::seconds(1))),
            )
            .filter(lifecycle_action::Column::Id.eq(&action.id))
            .exec(&fixture.db)
            .await
            .unwrap();
        let token = if token_case == "owned" {
            format!("lifecycle:{}:{terminal_attempt}", action.id)
        } else {
            "user-mutation-token".to_owned()
        };
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "INSERT INTO import_destinations \
                 (bucket, key, generation, owner_job_id, mutation_id, mutation_prefix, updated_at) \
                 VALUES ($1, $2, 1, NULL, $3, NULL, clock_timestamp()) \
                 ON CONFLICT (bucket, key) DO UPDATE SET mutation_id = EXCLUDED.mutation_id, \
                 mutation_prefix = NULL, updated_at = clock_timestamp()",
                [
                    "lifecycle-bucket".into(),
                    "key".into(),
                    token.clone().into(),
                ],
            ))
            .await
            .unwrap();

        assert!(
            claim_due_with_max_attempts(
                &fixture.db,
                "must-not-reclaim",
                ChronoDuration::seconds(30),
                configured_max_attempts,
                1,
            )
            .await
            .unwrap()
            .is_empty()
        );
        let terminal = lifecycle_action::Entity::find_by_id(&action.id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.state, "failed_safe");
        assert_eq!(terminal.attempts, terminal_attempt);
        let destination = import_destination::Entity::find_by_id((
            "lifecycle-bucket".to_owned(),
            "key".to_owned(),
        ))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
        if token_case == "owned" {
            assert_eq!(destination.mutation_id, None);
        } else {
            assert_eq!(destination.mutation_id.as_deref(), Some(token.as_str()));
        }
    }

    fixture.cleanup().await;
}

fn abort_reclaim_worker_config() -> ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 1,
        scan_page_size: 1,
        scan_lease_secs: 30,
        action_lease_secs: 1,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 60,
    }
    .validate()
    .unwrap()
}

fn abort_reclaim_configuration() -> CanonicalLifecycleConfiguration {
    CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some("abort-reclaim".to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: Some(CurrentExpiration::Date {
                utc_midnight: chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            }),
            noncurrent_version_expiration: None,
        }],
    }
}

#[tokio::test]
async fn postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch() {
    let Some(fixture) = lifecycle_fixture().await else {
        return;
    };
    let store_a = Store::new(fixture.independent_connection().await);
    let store_b = Store::new(fixture.independent_connection().await);
    let configuration =
        ipfs_s3_gateway::lifecycle::config::canonical_json(&abort_reclaim_configuration()).unwrap();
    let revision = put_configuration(store_a.db(), "lifecycle-bucket", &configuration)
        .await
        .unwrap();
    let target_version = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq("lifecycle-bucket"))
        .filter(object_version::Column::ObjectId.eq("current-object"))
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert!(target_version.is_latest);
    assert_eq!(target_version.version_id, None);
    assert_eq!(target_version.object_id.as_deref(), Some("current-object"));

    let due_at = chrono::DateTime::parse_from_rfc3339("2000-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let now = database_now(store_a.db()).await.unwrap();
    let mut pending = NewLifecycleAction {
        idempotency_key: String::new(),
        bucket: "lifecycle-bucket".to_owned(),
        config_revision: revision,
        rule_identity: RuleIdentity::Id("abort-reclaim".to_owned()),
        action_kind: LifecycleActionKind::ExpireCurrent,
        target: VersionTargetIdentity {
            bucket: "lifecycle-bucket".to_owned(),
            key: "key".to_owned(),
            version_row_id: target_version.id.clone(),
            public_version_id: PublicVersionId::Null,
            kind: VersionKind::Object,
            object_id: Some("current-object".to_owned()),
            sequence: target_version.sequence,
        },
        due_at,
    };
    pending.idempotency_key = idempotency_key(&pending).unwrap();
    let action_idempotency_key = pending.idempotency_key.clone();
    assert!(insert_idempotent(store_a.db(), pending, now).await.unwrap());
    let seeded = lifecycle_action::Entity::find()
        .filter(lifecycle_action::Column::IdempotencyKey.eq(action_idempotency_key))
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seeded.state, "pending");

    let start_a = Arc::new(tokio::sync::Barrier::new(2));
    let gate_a = ipfs_s3_gateway::lifecycle::worker::LifecycleAfterClaimGate::new("worker-a");
    let worker_a_store = store_a.clone();
    let worker_a_gate = gate_a.clone();
    let worker_a_start = start_a.clone();
    let worker_a = tokio::spawn(async move {
        worker_a_start.wait().await;
        ipfs_s3_gateway::lifecycle::worker::start_worker_for_test(
            worker_a_store,
            abort_reclaim_worker_config(),
            CancellationToken::new(),
            ipfs_s3_gateway::lifecycle::worker::LifecycleWorkerTestControl {
                worker_id: "worker-a".to_owned(),
                after_claim: Some(worker_a_gate),
            },
        )
    });
    start_a.wait().await;
    let worker_a = tokio::time::timeout(Duration::from_secs(15), worker_a)
        .await
        .expect("worker A must start within 15 seconds")
        .unwrap();
    let claim_a = tokio::time::timeout(Duration::from_secs(15), gate_a.wait_claim())
        .await
        .expect("worker A must claim the pending action within 15 seconds");
    let lease_until = claim_a.action.lease_until.expect("worker A claim lease");
    let claimed_before_abort = lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_before_abort.state, "claimed");
    assert_eq!(claimed_before_abort.claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(claimed_before_abort.claim_epoch, claim_a.claim_epoch);

    let aborted_a = worker_a.abort_for_test();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), aborted_a)
        .await
        .expect("worker A abort join must settle within 15 seconds")
        .expect_err("aborted worker A join must be cancelled");
    assert!(cancelled.is_cancelled());
    let claimed_after_abort = lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
        .one(store_a.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_after_abort.state, "claimed");
    assert_eq!(claimed_after_abort.claimed_by.as_deref(), Some("worker-a"));
    assert_eq!(claimed_after_abort.claim_epoch, claim_a.claim_epoch);
    assert_eq!(claimed_after_abort.lease_until, Some(lease_until));

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if database_now(store_b.db()).await.unwrap() > lease_until {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("database clock must pass worker A lease within 15 seconds");

    let gate_b = ipfs_s3_gateway::lifecycle::worker::LifecycleAfterClaimGate::new("worker-b");
    let worker_b = ipfs_s3_gateway::lifecycle::worker::start_worker_for_test(
        store_b.clone(),
        abort_reclaim_worker_config(),
        CancellationToken::new(),
        ipfs_s3_gateway::lifecycle::worker::LifecycleWorkerTestControl {
            worker_id: "worker-b".to_owned(),
            after_claim: Some(gate_b.clone()),
        },
    );
    let claim_b = tokio::time::timeout(Duration::from_secs(15), gate_b.wait_claim())
        .await
        .expect("worker B must reclaim the expired action within 15 seconds");
    assert_eq!(claim_b.worker_id, "worker-b");
    assert_eq!(claim_b.claim_epoch, claim_a.claim_epoch + 1);
    let claimed_by_b = lifecycle_action::Entity::find_by_id(claim_b.action.id.clone())
        .one(store_b.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed_by_b.claimed_by.as_deref(), Some("worker-b"));
    assert_eq!(claimed_by_b.claim_epoch, claim_b.claim_epoch);

    let terminal_notification = Arc::new(tokio::sync::Notify::new());
    let terminal_observer = {
        let db = store_b.db().clone();
        let action_id = claim_b.action.id.clone();
        let terminal_notification = terminal_notification.clone();
        tokio::spawn(async move {
            loop {
                let row = lifecycle_action::Entity::find_by_id(action_id.clone())
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
                if row.state == "succeeded" {
                    terminal_notification.notify_one();
                    return row;
                }
                tokio::task::yield_now().await;
            }
        })
    };
    let terminal_arrived = terminal_notification.notified();
    gate_b.release();
    tokio::time::timeout(Duration::from_secs(15), terminal_arrived)
        .await
        .expect("worker B must complete exactly one terminal action within 15 seconds");
    let terminal = terminal_observer.await.unwrap();
    worker_b.shutdown(Duration::from_secs(1)).await;

    let terminal_rows = lifecycle_action::Entity::find()
        .all(store_b.db())
        .await
        .unwrap();
    assert_eq!(terminal_rows.len(), 1);
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(terminal.claimed_by, None);
    assert_eq!(terminal.lease_until, None);
    let exact_version_deleted = object_version::Entity::find_by_id(target_version.id.clone())
        .one(store_b.db())
        .await
        .unwrap()
        .is_none();
    let target_object_projection = object::Entity::find_by_id("current-object")
        .one(store_b.db())
        .await
        .unwrap();
    assert!(
        exact_version_deleted,
        "worker B must delete the exact due version"
    );
    assert!(
        target_object_projection
            .as_ref()
            .is_none_or(|projection| !projection.is_latest),
        "worker B must remove the exact current object projection"
    );

    let stale_terminal_write = mark_succeeded(
        store_a.db(),
        &claim_a,
        database_now(store_a.db()).await.unwrap(),
    )
    .await
    .unwrap();
    assert!(
        !stale_terminal_write,
        "stale terminal CAS must affect zero rows"
    );
    let terminal_after_stale_write =
        lifecycle_action::Entity::find_by_id(claim_a.action.id.clone())
            .one(store_a.db())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(terminal_after_stale_write.state, "succeeded");
    assert_eq!(terminal_after_stale_write.claimed_by, None);
    assert_eq!(terminal_after_stale_write.lease_until, None);
    assert_eq!(terminal_after_stale_write, terminal);
    assert!(
        object_version::Entity::find_by_id(target_version.id)
            .one(store_a.db())
            .await
            .unwrap()
            .is_none(),
        "stale completion must not restore the deleted version"
    );

    drop(store_b);
    drop(store_a);
    fixture.cleanup().await;
}
