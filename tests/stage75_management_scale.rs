//! Local management must not materialize unrelated object/version inventories.
#[path = "support/stage75_metrics.rs"]
mod metrics;
#[allow(dead_code)]
mod postgres {
    use super::support::base as foundation;
    include!("support/stage7_management_apply_postgres_body.rs");
}
#[path = "support/stage7_management_actions.rs"]
pub mod support;

#[global_allocator]
static ALLOCATOR: metrics::CountingAllocator = metrics::CountingAllocator;

use ipfs_s3_gateway::{
    pinning::management::{
        Action, ItemRequest, PlanRequest, PlanStatus, Subject, TargetSelection, db_snapshot,
        dry_run, local_config::ManagementConfig,
    },
    store::entities::{pin_lease, pin_lease_target},
    store::pinning::management::{actions, records},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement,
    TransactionTrait,
};
use std::time::Instant;

const UNRELATED_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test]
async fn offline_preparation_and_original_replay_are_scoped_to_the_exact_version() {
    use ipfs_s3_gateway::{kubo::KuboClient, pinning::management::evidence};
    use tokio_util::sync::CancellationToken;

    let mut fixture = support::Fixture::new().await;
    let meter = metrics::QueryMeter::install(&mut fixture.db);
    let identity = support::base::publish(&fixture.db, &fixture.config, "proof", false).await;
    let client = KuboClient::new(fixture.server.uri());
    let request = evidence::PrepareRequest {
        record_id: "original-proof".into(),
        identity: identity.clone(),
        expected_node: support::NODE.into(),
        max_stored_bytes: 10,
        ttl: chrono::Duration::minutes(15),
        directory: false,
    };
    let original = evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &client,
        &request,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut first = 1;
    let mut baseline = None;
    for total in [1_000, 10_000, 100_000] {
        grow_unrelated(&fixture.db, first, total).await;
        first = total + 1;
        meter.begin();
        let started = Instant::now();
        let replay = evidence::prepare(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &client,
            &request,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let measurement = meter.finish();
        assert!(replay == original);
        eprintln!(
            "preparation replay current_objects={total} elapsed={:?} queries={}",
            started.elapsed(),
            measurement.count
        );
        if let Some(queries) = baseline {
            assert!(measurement.count <= queries + 2);
        } else {
            baseline = Some(measurement.count);
        }
        let fresh_request = evidence::PrepareRequest {
            record_id: format!("fresh-proof-{total}"),
            identity: identity.clone(),
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        };
        let fresh = evidence::prepare(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &client,
            &fresh_request,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(fresh.stored_bytes, Some(10));
    }
}

/// Fixture construction is outside the measured operation. Each added current
/// object has its own exact version and a CID unrelated to the selected object.
async fn grow_unrelated(db: &DatabaseConnection, first: u32, last: u32) {
    for start in (first..=last).step_by(1000) {
        insert_unrelated_batch(db, start, (start + 999).min(last)).await;
    }
}

async fn insert_unrelated_batch(db: &DatabaseConnection, first: u32, last: u32) {
    let tx = db.begin().await.unwrap();
    let series = format!(
        "WITH RECURSIVE numbers(n) AS (SELECT {first} UNION ALL SELECT n+1 FROM numbers WHERE n < {last})"
    );
    tx.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "{series} INSERT INTO objects \
             (id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) \
             SELECT 'scale-object-' || n, 'bucket', 'scale/' || n, $1, 10, $1, \
             FALSE, FALSE, TRUE, CURRENT_TIMESTAMP FROM numbers"
        ),
        [UNRELATED_CID.into()],
    ))
    .await
    .unwrap();
    tx.execute(Statement::from_string(
        db.get_database_backend(),
        format!(
            "{series} INSERT INTO object_versions \
             (id,bucket,key,version_id,kind,object_id,sequence,is_latest,\
              lifecycle_age_started_at,created_at,updated_at) \
             SELECT 'scale-version-' || n, 'bucket', 'scale/' || n, NULL, 'object', \
             'scale-object-' || n, 1, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, \
             CURRENT_TIMESTAMP FROM numbers"
        ),
    ))
    .await
    .unwrap();
    tx.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "{series} INSERT INTO objects \
             (id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) \
             SELECT 'scale-old-object-' || n, 'bucket', 'scale/' || n, $1, 10, $1, \
             FALSE, FALSE, FALSE, CURRENT_TIMESTAMP FROM numbers WHERE n % 10 = 0"
        ),
        [UNRELATED_CID.into()],
    ))
    .await
    .unwrap();
    tx.execute(Statement::from_string(
        db.get_database_backend(),
        format!(
            "{series} INSERT INTO object_versions \
             (id,bucket,key,version_id,kind,object_id,sequence,is_latest,\
              lifecycle_age_started_at,created_at,updated_at) \
             SELECT 'scale-old-version-' || n, 'bucket', 'scale/' || n, 'old-' || n, 'object', \
             'scale-old-object-' || n, 0, FALSE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, \
             CURRENT_TIMESTAMP FROM numbers WHERE n % 10 = 0"
        ),
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn local_create_and_committed_replay_survive_unrelated_growth() {
    let fixture = support::Fixture::new().await;
    let history = fixture
        .publish_cid("history-owner", support::CID, false)
        .await;
    let history_request = fixture.create_request("scale-history", history.clone(), false);
    let history_binding = fixture.confirm(&history_request).await;
    let historical = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &history_binding,
    )
    .await
    .unwrap();
    assert!(!historical.items[0].receipt.intent_ids.is_empty());
    assert!(!historical.items[0].jobs.is_empty());
    let lease = pin_lease::Entity::find_by_id(historical.items[0].receipt.intent_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let renewal_request = fixture
        .existing_request(
            "scale-renew",
            history,
            Action::Renew {
                expires_at: lease.expires_at.timestamp() + 1800,
            },
        )
        .await;
    let retry_request = retry_request(&renewal_request);

    let identity = fixture.publish("selected", false).await;
    let request = fixture.create_request("scale-local-create", identity, false);
    let binding = fixture.confirm(&request).await;
    let mut first = 1;
    let mut baseline = None;
    for total in [1_000, 10_000, 100_000] {
        grow_unrelated(&fixture.db, first, total).await;
        first = total + 1;
        let create = measure_capture(&fixture.db, &fixture.config, &request, total).await;
        let renew = measure_capture(&fixture.db, &fixture.config, &renewal_request, total).await;
        let retry = measure_capture(&fixture.db, &fixture.config, &retry_request, total).await;
        if let Some((create_base, renew_base, retry_base)) = &baseline {
            create.assert_local(create_base);
            renew.assert_local(renew_base);
            retry.assert_local(retry_base);
        } else {
            baseline = Some((create, renew, retry));
        }
    }
    // The confirmation precedes unrelated inserts: fresh membership must be
    // scoped, not invalidated merely by changes elsewhere in the database.
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.replayed);
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == applied.items);
    let historical_replay = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &history_binding,
    )
    .await
    .unwrap();
    assert!(historical_replay.replayed);
    assert!(historical_replay.items == historical.items);
    let renewal_binding = fixture.confirm(&renewal_request).await;
    actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &renewal_binding,
    )
    .await
    .unwrap();
    let renewed = pin_lease::Entity::find_by_id(lease.id)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.created_at, lease.created_at);
    assert_eq!(
        renewed.expires_at.timestamp(),
        lease.expires_at.timestamp() + 1800
    );
}

struct CaptureSample {
    queries: usize,
    rows: usize,
    memberships: usize,
    nodes: usize,
    bytes: usize,
    allocated: usize,
    peak_resident: usize,
    storage: Option<ipfs_s3_gateway::store::pinning::management::native::meter::WorkMeter>,
}

fn retry_request(renewal: &PlanRequest) -> PlanRequest {
    let mut request = renewal.clone();
    request.plan_id = format!("{}-retry", request.plan_id);
    request.items[0].action = Action::Retry;
    request
}

impl CaptureSample {
    fn assert_local(&self, baseline: &Self) {
        // The selected ownership/route/dependency set is unchanged. Permit
        // bounded cache and instrumentation variation, not inventory growth.
        assert!(
            self.queries <= baseline.queries * 2 + 8,
            "query count grew with unrelated inventory"
        );
        assert_eq!(
            (
                self.rows,
                self.memberships,
                self.nodes,
                self.bytes,
                self.peak_resident,
            ),
            (
                baseline.rows,
                baseline.memberships,
                baseline.nodes,
                baseline.bytes,
                baseline.peak_resident,
            ),
            "local scope, decode work and reservation peak must not change with unrelated inventory"
        );
        assert!(
            self.allocated <= baseline.allocated * 2 + 1024 * 1024,
            "copies grew with unrelated inventory"
        );
        if let (Some(current), Some(original)) = (&self.storage, &baseline.storage) {
            assert_eq!(
                (
                    current.sql,
                    current.rows,
                    current.cells,
                    current.raw_bytes,
                    current.copied_bytes,
                    current.output_bytes,
                    current.pages,
                ),
                (
                    original.sql,
                    original.rows,
                    original.cells,
                    original.raw_bytes,
                    original.copied_bytes,
                    original.output_bytes,
                    original.pages,
                ),
                "storage work must not change with unrelated inventory"
            );
        }
    }
}

async fn measure_capture(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    request: &PlanRequest,
    total: u32,
) -> CaptureSample {
    // A fresh pool separates connection/statement-cache startup from the warm
    // repeat. This does not evict the filesystem or PostgreSQL server cache.
    let url = match db.get_database_backend() {
        sea_orm::DatabaseBackend::Sqlite => {
            let rows = db
                .query_all(Statement::from_string(
                    sea_orm::DatabaseBackend::Sqlite,
                    "PRAGMA database_list",
                ))
                .await
                .unwrap();
            let file: String = rows
                .iter()
                .find(|row| row.try_get::<String>("", "name").unwrap() == "main")
                .unwrap()
                .try_get("", "file")
                .unwrap();
            assert!(!file.is_empty());
            format!("sqlite://{}?mode=rw", file.replace('\\', "/"))
        }
        sea_orm::DatabaseBackend::Postgres => {
            std::env::var("IPFS_S3_STAGE7_APPLY_DATABASE_URL").unwrap()
        }
        _ => unreachable!(),
    };
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let mut probe = sea_orm::Database::connect(options).await.unwrap();
    let meter = metrics::QueryMeter::install(&mut probe);
    let first =
        measure_capture_once(&probe, config, request, &meter, total, "new_connection").await;
    let warm = measure_capture_once(&probe, config, request, &meter, total, "warm").await;
    warm.assert_local(&first);
    probe.close().await.unwrap();
    first
}

async fn measure_capture_once(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    request: &PlanRequest,
    meter: &metrics::QueryMeter,
    total: u32,
    cache: &str,
) -> CaptureSample {
    let work = records::SharedDecodeBudget::default();
    meter.begin();
    let memory = metrics::MemorySample::begin();
    let started = Instant::now();
    let snapshot = db_snapshot::collect_with_session(db, config, request, work.clone())
        .await
        .unwrap_or_else(|error| panic!("local capture at {total} unrelated objects: {error:?}"));
    let storage = snapshot
        .work()
        .expect("local capture must retain its work meter");
    let peak_resident = snapshot.peak_resident().unwrap();
    let scope = snapshot.scope().unwrap();
    let rows = scope.row_count();
    let memberships = scope.membership_count();
    let plan = dry_run(request, &snapshot.planning_snapshot().unwrap()).unwrap();
    assert_eq!(plan.items().len(), 1);
    assert!(!matches!(plan.items()[0].status, PlanStatus::Blocked(_)));
    let memory = memory.finish();
    let queries = meter.finish();
    let (nodes, bytes) = work.lock().unwrap().usage();
    let sample = CaptureSample {
        queries: queries.count,
        rows,
        memberships,
        nodes,
        bytes,
        allocated: memory.rust_allocated,
        peak_resident,
        storage: Some(storage),
    };
    eprintln!(
        "local_capture plan={} cache={cache} unrelated_objects={total} elapsed={:?} decode={:?} \
         queries={} sql_elapsed={:?} rust_peak_delta={} rust_allocated={} \
         rss_initial={:?} rss_sampled_peak={:?} storage={storage:?} storage_peak_resident={peak_resident}",
        request.plan_id,
        started.elapsed(),
        work.lock().unwrap().usage(),
        queries.count,
        queries.elapsed,
        memory.rust_peak_delta,
        memory.rust_allocated,
        memory.rss_initial,
        memory.rss_sampled_peak,
    );
    if total == 100_000 && cache == "warm" {
        explain_queries(db, queries).await;
    }
    sample
}

async fn explain_queries(db: &DatabaseConnection, queries: metrics::QuerySample) {
    for (sql, (count, elapsed, mut statement)) in queries.statements {
        if !sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
            continue;
        }
        let sqlite = db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite;
        statement.sql = if sqlite {
            format!("EXPLAIN QUERY PLAN {sql}")
        } else {
            format!("EXPLAIN (ANALYZE, BUFFERS) {sql}")
        };
        let plan = db.query_all(statement).await.unwrap();
        let details: Vec<String> = plan
            .iter()
            .map(|row| {
                row.try_get("", if sqlite { "detail" } else { "QUERY PLAN" })
                    .unwrap()
            })
            .collect();
        eprintln!("query count={count} elapsed={elapsed:?} sql={sql} plan={details:?}");
    }
}

#[tokio::test]
async fn ordinary_publication_with_applied_history_has_local_cost() {
    use ipfs_s3_gateway::{
        kubo::KuboClient, pinning::coordinator::PinningCoordinator, store::Store,
    };
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    let mut fixture = support::Fixture::new().await;
    let meter = metrics::QueryMeter::install(&mut fixture.db);
    let owner = fixture.publish("history-owner", false).await;
    let request = fixture.create_request("publication-history", owner, false);
    let binding = fixture.confirm(&request).await;
    let history = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let mut first = 1;
    let mut baseline = None;
    let mut worker_baseline = None;
    let store = Store::new(fixture.db.clone());
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let history_scope = ipfs_s3_gateway::pinning::worker::ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        &binding,
    )
    .await
    .unwrap();
    coordinator
        .management_tick(&store, &history_scope, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find_by_id(&history.items[0].targets[0])
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    for (index, total) in [1_000, 10_000, 100_000].into_iter().enumerate() {
        grow_unrelated(&fixture.db, first, total).await;
        first = total + 1;
        // Distinct resources keep the selected closure identical in shape at
        // each scale rather than adding owners to the previous probe's CID.
        let hash = cid::multihash::Multihash::<64>::wrap(0x12, &[index as u8 + 1; 32]).unwrap();
        let cid = cid::Cid::new_v1(0x55, hash).to_string();
        meter.begin();
        let memory = metrics::MemorySample::begin();
        let started = Instant::now();
        let published = support::base::publish_cid(
            &fixture.db,
            &fixture.config,
            &format!("ordinary-{total}"),
            &cid,
            true,
        )
        .await;
        let elapsed = started.elapsed();
        let memory = memory.finish();
        let queries = meter.finish();
        let sample = CaptureSample {
            queries: queries.count,
            rows: 0,
            memberships: 0,
            nodes: 0,
            bytes: 0,
            allocated: memory.rust_allocated,
            peak_resident: 0,
            storage: None,
        };
        if let Some(baseline) = &baseline {
            sample.assert_local(baseline);
        } else {
            baseline = Some(sample);
        }
        eprintln!(
            "ordinary_publication unrelated_objects={total} elapsed={elapsed:?} \
             queries={} sql_elapsed={:?} rust_peak_delta={} rust_allocated={} \
             rss_initial={:?} rss_sampled_peak={:?}",
            queries.count,
            queries.elapsed,
            memory.rust_peak_delta,
            memory.rust_allocated,
            memory.rss_initial,
            memory.rss_sampled_peak,
        );
        let leases = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(&published.object_id))
            .all(&fixture.db)
            .await
            .unwrap();
        assert_eq!(leases.len(), 1);
        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&leases[0].id))
            .all(&fixture.db)
            .await
            .unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].cid, published.cid);
        if total == 100_000 {
            explain_queries(&fixture.db, queries).await;
        }
        meter.begin();
        let memory = metrics::MemorySample::begin();
        let started = Instant::now();
        let worker = coordinator.start(store.clone(), CancellationToken::new());
        let completed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let target = pin_lease_target::Entity::find_by_id(&targets[0].id)
                    .one(&fixture.db)
                    .await
                    .unwrap()
                    .unwrap();
                if target.state == "pinned" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        worker.shutdown(Duration::from_secs(1)).await;
        completed.expect("ordinary worker must complete the published target");
        let memory = memory.finish();
        let queries = meter.finish();
        let sample = CaptureSample {
            queries: queries.count,
            rows: 0,
            memberships: 0,
            nodes: 0,
            bytes: 0,
            allocated: memory.rust_allocated,
            peak_resident: 0,
            storage: None,
        };
        if let Some(baseline) = &worker_baseline {
            sample.assert_local(baseline);
        } else {
            worker_baseline = Some(sample);
        }
        eprintln!(
            "ordinary_worker unrelated_objects={total} elapsed={:?} queries={} sql_elapsed={:?} \
             rust_peak_delta={} rust_allocated={} rss_initial={:?} rss_sampled_peak={:?}",
            started.elapsed(),
            queries.count,
            queries.elapsed,
            memory.rust_peak_delta,
            memory.rust_allocated,
            memory.rss_initial,
            memory.rss_sampled_peak,
        );
        if total == 100_000 {
            explain_queries(&fixture.db, queries).await;
        }
    }
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(history.items == replay.items);
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn postgres_local_create_and_replay_survive_unrelated_growth() {
    let fixture = postgres::Fixture::new().await;
    let history = fixture.prepared_object("history-owner").await;
    let history_request = fixture.create_request(history.clone());
    let history_binding = fixture.confirm(&history_request).await;
    let historical = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &history_binding,
    )
    .await
    .unwrap();
    assert!(!historical.items[0].receipt.intent_ids.is_empty());
    assert!(!historical.items[0].jobs.is_empty());
    let lease = pin_lease::Entity::find_by_id(historical.items[0].receipt.intent_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease.id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let renewal_request = PlanRequest {
        plan_id: uuid::Uuid::new_v4().to_string(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action: Action::Renew {
                expires_at: lease.expires_at.timestamp() + 1800,
            },
            subject: Subject::Object {
                identity: history,
                target: TargetSelection::Existing {
                    target_id: target.id,
                    source: None,
                },
            },
        }],
    };
    let retry_request = retry_request(&renewal_request);
    let identity = fixture.prepared_object("selected").await;
    let request = fixture.create_request(identity);
    let binding = fixture.confirm(&request).await;
    let mut first = 1;
    let mut baseline = None;
    for total in [1_000, 10_000, 100_000] {
        grow_unrelated(&fixture.db, first, total).await;
        first = total + 1;
        let create = measure_capture(&fixture.db, &fixture.config, &request, total).await;
        let renew = measure_capture(&fixture.db, &fixture.config, &renewal_request, total).await;
        let retry = measure_capture(&fixture.db, &fixture.config, &retry_request, total).await;
        if let Some((create_base, renew_base, retry_base)) = &baseline {
            create.assert_local(create_base);
            renew.assert_local(renew_base);
            retry.assert_local(retry_base);
        } else {
            baseline = Some((create, renew, retry));
        }
    }
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.replayed);
    let writer_auth = fixture.reauthorize_writer().await;
    let replay = actions::apply(&fixture.writer, &fixture.config, &writer_auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == applied.items);
    let historical_replay = actions::apply(
        &fixture.writer,
        &fixture.config,
        &writer_auth,
        &history_binding,
    )
    .await
    .unwrap();
    assert!(historical_replay.replayed);
    assert!(historical_replay.items == historical.items);
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &renewal_request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&renewal_request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|item| item.status == PlanStatus::Ready)
    );
    let renewal_binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&renewal_request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    actions::apply(
        &fixture.writer,
        &fixture.config,
        &writer_auth,
        &renewal_binding,
    )
    .await
    .unwrap();
    let renewed = pin_lease::Entity::find_by_id(lease.id)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.created_at, lease.created_at);
    assert_eq!(
        renewed.expires_at.timestamp(),
        lease.expires_at.timestamp() + 1800
    );
    fixture.close().await;
}
