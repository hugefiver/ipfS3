//! Global inventory work may grow; resident state and each source transaction may not.
#![allow(dead_code)]

#[path = "support/stage75_metrics.rs"]
mod metrics;
#[path = "support/stage7_management_actions.rs"]
mod support;

use futures_util::TryStreamExt;
use ipfs_s3_gateway::{
    pinning::management::{
        Action, Capture, ItemRequest, PlanRequest, RouteIdentity, Snapshot, Subject, db_snapshot,
        retirement::{
            InventorySpool, PlanningCheckpoint, PlanningProgress, PreparedPlanning, Progress,
            RunLimits,
        },
        wire::v3::{self, Frame, Projection, Reader},
    },
    store::pinning::management::{ManagementError, actions, native},
};
use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::{
    fs::File,
    path::Path,
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOCATOR: metrics::CountingAllocator = metrics::CountingAllocator;

fn reserve(path: &Path) -> File {
    let mut options = File::options();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}

async fn populate(db: &DatabaseConnection, provider: &str, owners: usize) {
    let tx = db.begin().await.unwrap();
    let numbers =
        format!("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<{owners})");
    for sql in [
        format!(
            "{numbers} INSERT INTO objects(id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) SELECT 'inventory-owner-'||i,'bucket','inventory/雪-☃/'||i,$1,10,$1,FALSE,FALSE,TRUE,CURRENT_TIMESTAMP FROM n"
        ),
        format!(
            "{numbers} INSERT INTO object_versions(id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) SELECT 'inventory-version-'||i,'bucket','inventory/雪-☃/'||i,NULL,'object','inventory-owner-'||i,1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM n WHERE $1 IS NOT NULL"
        ),
        format!(
            "{numbers} INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state) SELECT 'inventory-lease-'||i,'inventory-owner-'||i,'automatic','scale','all','object',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'2099-01-01 00:00:00+00:00',1,'active' FROM n WHERE $1 IS NOT NULL"
        ),
    ] {
        tx.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            [support::CID.into()],
        ))
        .await
        .unwrap();
    }
    tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        format!("{numbers} INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) SELECT 'inventory-target-'||i,'inventory-lease-'||i,$1,10,$2,'waiting',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM n"),
        vec![support::CID.into(), provider.into()])).await.unwrap();
    tx.commit().await.unwrap();
}

async fn capture(
    fixture: &support::Fixture,
    request: &PlanRequest,
    run: &mut RunLimits,
) -> (
    Snapshot,
    native::meter::Budget,
    native::meter::ResidentGuard,
) {
    let mut budget = v3::files::stream::budget();
    let deadline = Instant::now() + Duration::from_secs(30);
    run.constrain_deadline(deadline, &mut budget).unwrap();
    let collected = db_snapshot::collect_with_control(
        &fixture.db,
        &fixture.config,
        request,
        deadline,
        run.cancellation.clone(),
        Default::default(),
    )
    .await
    .unwrap();
    let (local, scope) = collected.planning_with_scope().unwrap();
    assert!(local.native_frame.is_some());
    let work = scope.work();
    budget
        .charge(work.sql, work.rows, work.cells, work.raw_bytes)
        .unwrap();
    budget.work.copied_bytes = work.copied_bytes;
    budget.work.output_bytes = work.output_bytes;
    budget.work.pages = work.pages;
    let resident = budget.reserve(scope.peak_resident()).unwrap();
    (local, budget, resident)
}

async fn resume_prepared(
    fixture: &support::Fixture,
    path: &Path,
    request: &PlanRequest,
    started: Instant,
) -> (
    PreparedPlanning,
    Snapshot,
    native::meter::Budget,
    RunLimits,
    native::meter::ResidentGuard,
) {
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(1800),
            "checkpoint made insufficient bounded progress"
        );
        let mut run = RunLimits::default();
        let (local, mut budget, resident) = capture(fixture, request, &mut run).await;
        let mut task =
            PlanningCheckpoint::resume(path, &fixture.db, "operator", request, &local, &run)
                .await
                .unwrap();
        let local = task.snapshot().clone();
        let caps = task.reserve_capture(&budget, &run).unwrap();
        match task.advance(&fixture.db, &caps).await.unwrap() {
            PlanningProgress::Complete(prepared) => {
                return (*prepared, local, budget, run, resident);
            }
            PlanningProgress::Incomplete => {
                task.account_capture(&mut budget, &run).await.unwrap();
                task.close().await.unwrap();
            }
        }
    }
}

fn cell_identity(hash: &mut Sha256, key: &str, column: &str) {
    for value in [key, column] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
}
fn cell_end(hash: &mut Sha256, length: Option<u64>) {
    hash.update(length.unwrap_or(0).to_be_bytes());
    hash.update([u8::from(length.is_some())]);
}

fn failed_memory(memory: &mut Option<metrics::MemorySample>) {
    if let Some(memory) = memory.take().map(metrics::MemorySample::finish) {
        eprintln!(
            "global incomplete rust_peak_delta={} rust_allocated={} rss_initial={:?} rss_sampled_peak={:?}",
            memory.rust_peak_delta,
            memory.rust_allocated,
            memory.rss_initial,
            memory.rss_sampled_peak
        );
    }
}

// Compare every original cell, including SQL NULL, without materializing keys or
// recapturing the live database. Both streams use canonical wire-key order.
async fn verify_original(spool: &Path, output: &Path, owners: usize) {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(spool)
                .read_only(true),
        )
        .await
        .unwrap();
    sqlx::query("PRAGMA cache_size=-2048; PRAGMA temp_store=FILE")
        .execute(&pool)
        .await
        .unwrap();
    let mut rows = sqlx::query("SELECT p.row_key,p.column_name,p.offset,p.length,p.bytes FROM wire_keys k JOIN parts p ON p.table_name=k.table_name AND p.row_key=k.row_key ORDER BY k.table_name,k.sort_key,p.column_name,p.offset").fetch(&pool);
    let mut original = Sha256::new();
    let mut expected_offset = 0u64;
    while let Some(row) = rows.try_next().await.unwrap() {
        let key: String = row.get("row_key");
        let column: String = row.get("column_name");
        let offset = row.get::<i64, _>("offset") as u64;
        let length = row.get::<Option<i64>, _>("length").map(|n| n as u64);
        let bytes: Vec<u8> = row.get("bytes");
        assert_eq!(offset, expected_offset);
        if offset == 0 {
            cell_identity(&mut original, &key, &column);
        }
        original.update(&bytes);
        expected_offset += bytes.len() as u64;
        if expected_offset == length.unwrap_or(0) {
            cell_end(&mut original, length);
            expected_offset = 0;
        }
    }
    assert_eq!(expected_offset, 0);
    drop(rows);
    pool.close().await;
    let mut reader = Reader::new(
        File::open(output).unwrap(),
        Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )
    .unwrap();
    reader.enable_buffering().unwrap();
    let mut actual = Sha256::new();
    let (mut native_row, mut key, mut length) = (false, String::new(), 0u64);
    let (mut owner_rows, mut native_rows, mut members, mut nulls, mut eof, mut unicode) =
        (0, 0, 0, 0, 0, 0);
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            v3::Event::Frame(Frame::Row {
                section, key: row, ..
            }) => {
                native_row = section == v3::Section::Native;
                if native_row {
                    native_rows += 1;
                    if row.table == "objects" && row.values[0].starts_with("inventory-owner-") {
                        owner_rows += 1;
                    }
                    key = serde_json::to_string(&row).unwrap();
                } else if section == v3::Section::Membership {
                    match row.values.get(2).map(String::as_str) {
                        Some("eof") => eof += 1,
                        Some("member") => members += 1,
                        _ => panic!("unexpected inventory membership"),
                    }
                }
            }
            v3::Event::Frame(Frame::Cell { column }) if native_row => {
                cell_identity(&mut actual, &key, &column);
                length = 0;
            }
            v3::Event::Chunk(bytes) if native_row => {
                if bytes
                    .windows("雪-☃".len())
                    .any(|value| value == "雪-☃".as_bytes())
                {
                    unicode += 1;
                }
                actual.update(&bytes);
                length += bytes.len() as u64;
            }
            v3::Event::Frame(Frame::CellEnd { value }) if native_row => {
                assert!(value != v3::CellValue::Redacted);
                let value = if value == v3::CellValue::Null {
                    nulls += 1;
                    None
                } else {
                    Some(length)
                };
                cell_end(&mut actual, value);
            }
            _ => {}
        }
    }
    assert!(reader.verified());
    assert_eq!(original.finalize(), actual.finalize());
    assert_eq!(owner_rows, owners);
    assert_eq!(members, native_rows);
    assert!(nulls > 0);
    assert!(unicode >= owners);
    assert_eq!(eof, native::registry().len());
}

#[tokio::test]
async fn hundred_thousand_owner_inventory_resumes_and_exports_without_materializing() {
    owner_inventory(100_000).await;
}

#[tokio::test]
async fn operational_allocation_owners_reach_inventory_totals_and_export() {
    owner_inventory(3).await;
}

#[tokio::test]
async fn owner_relations_accumulate_across_reducer_page_boundaries() {
    owner_inventory(130).await;
}

async fn owner_inventory(owners: usize) {
    let mut fixture = support::Fixture::new().await;
    let object = fixture.publish("inventory-history", false).await;
    let request = fixture.create_request("inventory-history", object, false);
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.items[0].jobs.is_empty());
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let configuration = local.configuration.unwrap();
    let route = configuration.routes[0].clone();
    let allocation = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT provider FROM pin_lease_targets LIMIT 1",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "provider")
        .unwrap();
    assert_ne!(allocation, route.provider_id);
    let request = PlanRequest {
        plan_id: "large-retirement-review".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(route),
            },
        }],
    };
    populate(&fixture.db, &allocation, owners).await;
    let mut memory = (owners == 100_000).then(metrics::MemorySample::begin);
    let count = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM objects WHERE is_latest=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert!(count.try_get::<i64>("", "n").unwrap() >= owners as i64);

    let meter = metrics::QueryMeter::install(&mut fixture.db);
    meter.begin();
    let started = Instant::now();
    let mut one_page = RunLimits::default();
    one_page.pages = 1;
    let (local, mut budget, _resident) = capture(&fixture, &request, &mut one_page).await;
    let task_path = fixture.directory.path().join("large-planning");
    let mut task = PlanningCheckpoint::create(
        &task_path,
        &fixture.db,
        "operator",
        &request,
        &local,
        &one_page,
    )
    .await
    .unwrap();
    let caps = task.reserve_capture(&budget, &one_page).unwrap();
    assert!(matches!(
        task.advance(&fixture.db, &caps).await.unwrap(),
        PlanningProgress::Incomplete
    ));
    task.account_capture(&mut budget, &one_page).await.unwrap();
    assert_eq!(one_page.pages_remaining(), 0);
    task.close().await.unwrap();
    drop(_resident);
    let path = task_path.join("source-000.sqlite");

    // Reopening both handles must not rely on a retained transaction or RAM cursor.
    let source_path = fixture
        .db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT file FROM pragma_database_list WHERE name='main'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "file")
        .unwrap();
    let competitor = Database::connect(format!("sqlite:{source_path}?mode=rw"))
        .await
        .unwrap();
    competitor
        .execute_unprepared("PRAGMA busy_timeout=100")
        .await
        .unwrap();
    let lock_started = Instant::now();
    let writer = competitor.begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE objects SET size=size WHERE 1=0")
        .await
        .unwrap();
    writer.commit().await.unwrap();
    eprintln!(
        "global inventory between-page no-op lock probe acquired/released in {:?}",
        lock_started.elapsed()
    );
    competitor.close().await.unwrap();
    let mut spool = InventorySpool::resume(&path).await.unwrap();
    let checkpoint = spool.checkpoint().await.unwrap();
    let cancelled = RunLimits::default();
    cancelled.cancellation.cancel();
    assert!(matches!(
        spool.scan(&fixture.db, &cancelled).await,
        Err(ManagementError::Budget)
    ));
    let mut calls = 1usize;
    while spool
        .scan(&fixture.db, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {
        calls += 1;
        assert!(
            started.elapsed() < Duration::from_secs(1800),
            "global scan made insufficient bounded progress"
        );
        if calls.is_multiple_of(16) {
            eprintln!("global scan calls={calls} elapsed={:?}", started.elapsed());
        }
    }
    let scan_elapsed = started.elapsed();
    let scan_queries = meter.finish();
    meter.begin();
    let scan_phases = spool.phase_work().await.unwrap();
    let scan_points = spool.query_work().await.unwrap();
    eprintln!(
        "global scan outer_callback_queries={} outer_callback_elapsed={:?} work={:?} phases={:?} SQL_points={scan_points:?}",
        scan_queries.count,
        scan_queries.elapsed,
        spool.task_work().await.unwrap(),
        scan_phases
    );
    let source_calls = scan_points
        .iter()
        .filter(|work| matches!(work.phase.as_str(), "source_keyset" | "source_cells"))
        .map(|work| work.calls)
        .sum::<u64>();
    let source_pages = scan_phases
        .iter()
        .find(|work| work.phase == "source_read")
        .unwrap()
        .pages;
    assert!(
        source_calls <= source_pages * 4 + checkpoint.revisions.len() as u64 * 4,
        "source keyset/cell SQL must be amortized by bounded pages, not one query per source row"
    );
    let verification_started = Instant::now();
    while spool
        .verify_source(&fixture.db, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    eprintln!(
        "global scan={scan_elapsed:?} source_revalidation={:?}",
        verification_started.elapsed()
    );
    eprintln!(
        "global verified SQL_points={:?}",
        spool.query_work().await.unwrap()
    );
    let reduction_started = Instant::now();
    while spool
        .reduce(&configuration, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {
        assert!(
            started.elapsed() < Duration::from_secs(1800),
            "global reduction made insufficient bounded progress"
        );
    }
    let review = spool.frozen_review().await.unwrap();
    eprintln!(
        "global reduction={:?} work={:?} phases={:?}",
        reduction_started.elapsed(),
        spool.task_work().await.unwrap(),
        spool.phase_work().await.unwrap()
    );
    assert_eq!(review.totals().leases, owners + 1);
    assert_eq!(review.totals().targets, owners + 1);
    assert_eq!(review.totals().active_targets, owners + 1);
    assert_eq!(review.totals().unique_cids, 1);
    let fence = review.prepare_fence().unwrap();
    spool.close().await.unwrap();
    drop(spool);
    let union_started = Instant::now();
    let (mut prepared, mut local, mut budget, mut run, mut resident) =
        resume_prepared(&fixture, &task_path, &request, started).await;
    let Some(Capture::Provider(capture)) = &prepared.plan.items()[0].capture else {
        panic!("retirement plan must retain the original inventory");
    };
    assert!(capture.inventory == *fence.reference());
    eprintln!("global union={:?}", union_started.elapsed());
    let output = fixture.directory.path().join("large-private.v3");
    let export_started = Instant::now();
    loop {
        let (result, returned) = prepared
            .prepare_output(
                budget,
                v3::planning::Header {
                    projection: Projection::PrivateRecovery,
                    database_scope: &checkpoint.database_scope,
                    database_identity: &checkpoint.database_identity,
                    principal: "operator",
                    catalog: &checkpoint.catalog,
                    created_at: checkpoint.observed_at.to_rfc3339(),
                    expires_at: None,
                },
                &request,
                &local,
                &run,
            )
            .await;
        budget = returned;
        let progress = match result {
            Ok(progress) => progress,
            Err(error) => {
                eprintln!(
                    "persistent output preparation elapsed={:?} error={error:?} budget={:?} task={:?}",
                    export_started.elapsed(),
                    budget.work,
                    prepared.task_work().await.unwrap()
                );
                failed_memory(&mut memory);
                panic!("persistent output preparation must complete: {error:?}");
            }
        };
        if progress == Progress::Archived {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(1800),
            "persistent output made insufficient bounded progress"
        );
        if progress == Progress::Pending && run.pages_remaining() > 0 && run.production_available()
        {
            continue;
        }
        prepared.close().await.unwrap();
        drop(resident);
        (prepared, local, budget, run, resident) =
            resume_prepared(&fixture, &task_path, &request, started).await;
    }
    // Delivery is a real invocation of the same sealed task, not a replacement
    // budget on an open producer. Capture, reopen, copy and full verification
    // share this invocation's original hard deadline and cumulative Budget.
    prepared.close().await.unwrap();
    drop(resident);
    (prepared, local, budget, run, resident) =
        resume_prepared(&fixture, &task_path, &request, started).await;
    let (ready, returned) = prepared
        .prepare_output(
            budget,
            v3::planning::Header {
                projection: Projection::PrivateRecovery,
                database_scope: &checkpoint.database_scope,
                database_identity: &checkpoint.database_identity,
                principal: "operator",
                catalog: &checkpoint.catalog,
                created_at: checkpoint.observed_at.to_rfc3339(),
                expires_at: None,
            },
            &request,
            &local,
            &run,
        )
        .await;
    budget = returned;
    assert_eq!(ready.unwrap(), Progress::Archived);
    let initial = budget.work;
    let before = prepared.task_work().await.unwrap();
    let (result, returned) = prepared
        .export_prepared_output(
            (reserve(&output), budget),
            Projection::PrivateRecovery,
            &run,
        )
        .await;
    let after = prepared.task_work().await.unwrap();
    eprintln!(
        "global export={:?} result={result:?} writer_work={:?} work={:?}",
        export_started.elapsed(),
        returned.work,
        after
    );
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial.raw_bytes + returned.work.copied_bytes
            - initial.copied_bytes
    );
    if let Err(error) = result {
        failed_memory(&mut memory);
        panic!("single-budget copy and full verification must complete: {error:?}");
    }
    prepared.close().await.unwrap();
    drop(resident);
    let output_bytes = std::fs::metadata(&output).unwrap().len();
    if owners == 100_000 {
        assert!(output_bytes > 32 * 1024 * 1024);
    }
    verify_original(&path, &output, owners).await;
    let tx = fixture.db.begin().await.unwrap();
    fence.seal(&tx, &RunLimits::default()).await.unwrap();
    tx.rollback().await.unwrap();
    fixture
        .db
        .execute_unprepared(&format!(
            "UPDATE objects SET size=11 WHERE id='inventory-owner-{owners}'"
        ))
        .await
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    assert!(matches!(
        fence.seal(&tx, &RunLimits::default()).await,
        Err(ManagementError::Changed)
    ));
    tx.rollback().await.unwrap();
    let queries = meter.finish();
    if let Some(memory) = memory.map(metrics::MemorySample::finish) {
        eprintln!(
            "global added_owners={owners} scan={scan_elapsed:?} total={:?} outer_callback_queries={} output_bytes={output_bytes} rust_peak_delta={} rust_allocated={} rss_initial={:?} rss_sampled_peak={:?}",
            started.elapsed(),
            queries.count + scan_queries.count,
            memory.rust_peak_delta,
            memory.rust_allocated,
            memory.rss_initial,
            memory.rss_sampled_peak
        );
        assert!(
            memory.rust_peak_delta < 64 * 1024 * 1024,
            "global resident allocations must stay page-bounded"
        );
    }
}
