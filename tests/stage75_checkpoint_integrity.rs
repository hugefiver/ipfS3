#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

#[path = "stage75_checkpoint_integrity/output_regressions.rs"]
mod output_regressions;

#[path = "stage75_checkpoint_integrity/capture_finalization.rs"]
mod capture_finalization;

#[path = "stage75_checkpoint_integrity/short_cells.rs"]
mod short_cells;

use ipfs_s3_gateway::{
    config::Config,
    pinning::management::{
        Action, ItemRequest, NativeKey, PlanRequest, PlanStatus, RetirementSelection,
        RouteIdentity, Snapshot, Subject, db_snapshot, local_config,
        retirement::{
            ConfirmationProgress, InventorySpool, InventoryTotals, PlanningCheckpoint,
            PlanningProgress, PreparedPlanning, Progress, RunLimits, SourceCheckpoint,
        },
        wire::v3::{self, Projection, Writer, planning::Header},
    },
    store::pinning::{
        ledger,
        management::{ManagementError, native, records},
    },
};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement, TransactionTrait};
use std::{path::Path, process::Output};

fn limits(rows: u64) -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.total_rows = rows;
    limits
}

fn request(fixture: &support::Fixture) -> PlanRequest {
    PlanRequest {
        plan_id: "checkpoint-integrity".into(),
        items: fixture
            .config
            .pinning
            .providers
            .iter()
            .enumerate()
            .map(|(index, provider)| ItemRequest {
                item_id: format!("provider-{index}"),
                subject: Subject::Provider {
                    route: RouteIdentity::from_original(provider.route_snapshot()),
                },
                action: Action::RetireWith {
                    selection: RetirementSelection::Retain,
                },
            })
            .collect(),
    }
}

async fn fixture(backfill: bool) -> (support::Fixture, records::LocalAuthorization) {
    let mut fixture = support::Fixture::new(backfill).await;
    let object_items = fixture.request.items.clone();
    fixture.request = request(&fixture);
    if backfill {
        fixture.request.items.extend(object_items);
    }
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    (fixture, auth)
}

async fn two_provider_fixture(backfill: bool) -> (support::Fixture, records::LocalAuthorization) {
    let (mut fixture, _) = fixture(backfill).await;
    let object_items = fixture
        .request
        .items
        .iter()
        .filter(|item| !matches!(item.subject, Subject::Provider { .. }))
        .cloned()
        .collect::<Vec<_>>();
    let mut config: toml::Value =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let providers = config["pinning"]["providers"].as_array_mut().unwrap();
    let mut second = providers[0].clone();
    second["name"] = "second".into();
    providers.push(second);
    let identities = config["pinning_identity"]["providers"]
        .as_array_mut()
        .unwrap();
    let mut second = identities[0].clone();
    second["config_name"] = "second".into();
    second["provider_id"] = "second-provider".into();
    second["scope"] = "second-account".into();
    identities.push(second);
    let contents = toml::to_string(&config).unwrap();
    let raw: Config = toml::from_str(&contents).unwrap();
    fixture.config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let second = &fixture.config.pinning.providers[1].identity;
    ledger::register_route(&fixture.db, &second.allocation_key(), second)
        .await
        .unwrap();
    std::fs::write(&fixture.config_path, contents).unwrap();
    fixture.request = request(&fixture);
    if backfill {
        // The acknowledged object uses the first provider; retire the other
        // domain so the mixed plan does not request conflicting allocations.
        fixture.request.items.retain(|item| {
            matches!(
                &item.subject, Subject::Provider { route } if route.provider_id == "second-provider"
            )
        });
        fixture.request.items.extend(object_items);
        fixture.prepare().await;
    }
    assert!(!fixture.unrelated_queue().await.is_empty());
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    (fixture, auth)
}

#[tokio::test]
async fn confirmed_provider_items_share_one_complete_original_document() {
    use ipfs_s3_gateway::pinning::management::{
        Capture,
        retirement::{FrozenRetirementImport, RetirementEvidenceRefs},
    };
    let (fixture, auth) = two_provider_fixture(false).await;
    let (local, prepared) = complete(
        &fixture,
        &auth,
        "common-original",
        RunLimits::default().total_rows,
    )
    .await;
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &auth,
        (&fixture.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let mut original = None;
    let mut root = None;
    for entry in prepared.plan.items() {
        let Some(Capture::Provider(capture)) = &entry.capture else {
            continue;
        };
        let mut bytes = Vec::new();
        records::copy_original_inventory(
            &fixture.db,
            &binding,
            entry,
            &mut bytes,
            &mut v3::files::stream::budget(),
        )
        .await
        .unwrap();
        if let Some(original) = &original {
            assert_eq!(
                &bytes, original,
                "each item must resolve the same self-contained root"
            );
        } else {
            let mut reader = v3::Reader::new(
                bytes.as_slice(),
                Projection::PrivateRecovery,
                v3::files::stream::budget(),
            )
            .unwrap();
            let (mut native, mut members, mut eof, mut inventories) = (0, 0, 0, 0);
            while let Some(event) = reader.next_frame().unwrap() {
                if let v3::Event::Frame(v3::Frame::Row { section, key, .. }) = event {
                    match section {
                        v3::Section::Native => native += 1,
                        v3::Section::Membership if key.values[2] == "eof" => eof += 1,
                        v3::Section::Membership => members += 1,
                        v3::Section::Inventory if key.table == "retirement_inventory" => {
                            inventories += 1
                        }
                        _ => {}
                    }
                }
            }
            assert!(reader.verified());
            assert!(native > 0);
            assert_eq!(members, native);
            assert_eq!(eof, native::registry().len());
            assert_eq!(inventories, 2);
            original = Some(bytes.clone());
        }
        let cursor = records::load_original_inventory(
            &fixture.db,
            &binding,
            entry,
            tempfile::tempfile().unwrap(),
            v3::files::stream::budget(),
        )
        .await
        .unwrap();
        let reference = cursor.archive_claim().unwrap().clone();
        if let Some(root) = &root {
            assert!(root == &reference);
        } else {
            assert!(
                v3::archive_cursor::load(
                    &fixture.db,
                    &reference,
                    tempfile::tempfile().unwrap(),
                    Projection::PrivateRecovery,
                    v3::files::stream::budget(),
                )
                .await
                .is_err()
            );
            root = Some(reference);
        }
        let path = fixture.file(&format!("restored-{}.sqlite", entry.request.item_id));
        let reservation = v3::files::reserve(path.to_str().unwrap())
            .await
            .unwrap()
            .into_std()
            .await;
        let mut importer =
            FrozenRetirementImport::create(cursor, &path, reservation, capture.inventory.clone())
                .await
                .unwrap();
        while importer
            .advance(&limits(RunLimits::default().total_rows))
            .await
            .unwrap()
            == Progress::Pending
        {}
        let (spool, _, _) = importer.into_parts().unwrap();
        let refs = RetirementEvidenceRefs::for_entry(&fixture.request.plan_id, entry)
            .unwrap()
            .unwrap();
        assert!(
            spool
                .restore_capture(&fixture.request.plan_id, &entry.request, &refs)
                .await
                .unwrap()
                == **capture
        );
        spool.close().await.unwrap();
    }
    assert!(original.is_some());
    prepared.cleanup().await.unwrap();
}

async fn snapshot(fixture: &support::Fixture) -> Snapshot {
    db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap()
}

async fn capture_with_independent_copy(
    fixture: &support::Fixture,
) -> (
    Snapshot,
    native::meter::Budget,
    native::meter::ResidentGuard,
) {
    let collected = db_snapshot::collect_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.request,
        Default::default(),
    )
    .await
    .unwrap();
    let (local, scope) = collected.planning_with_scope().unwrap();
    let measured = scope.work();
    assert!(measured.raw_bytes > 0);
    assert!(
        measured.copied_bytes > 0,
        "the real scope merge must clone source rows"
    );
    let mut budget = v3::files::stream::budget();
    budget
        .charge(
            measured.sql,
            measured.rows,
            measured.cells,
            measured.raw_bytes,
        )
        .unwrap();
    budget.work.copied_bytes = measured.copied_bytes;
    budget.work.output_bytes = measured.output_bytes;
    budget.work.pages = measured.pages;
    let resident = budget.reserve(scope.peak_resident()).unwrap();
    (local, budget, resident)
}

#[tokio::test]
async fn capture_independent_copy_must_fit_the_retained_remaining_bytes() {
    let (fixture, auth) = fixture(false).await;
    let (_, prepared) = complete(
        &fixture,
        &auth,
        "copy-envelope",
        RunLimits::default().total_rows,
    )
    .await;
    let (_, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    let before = prepared.task_work().await.unwrap();
    let capture = budget.work;
    let remaining = capture.raw_bytes + capture.copied_bytes / 2;
    assert!(remaining >= capture.raw_bytes);
    assert!(remaining < capture.raw_bytes + capture.copied_bytes);
    let mut bounded = RunLimits::default();
    bounded.total_bytes = before.bytes + remaining;
    let reservation = prepared.reserve_capture(&budget, &bounded);
    let accounted = prepared.account_capture(&mut budget, &bounded).await;
    assert!(matches!(accounted, Err(ManagementError::Budget)));
    match reservation {
        Ok(caps) => assert!(caps.total_bytes < before.bytes),
        Err(ManagementError::Budget) => {}
        Err(error) => panic!("unexpected capture reservation error: {error:?}"),
    }
    assert_eq!(
        (budget.work.raw_bytes, budget.work.copied_bytes),
        (capture.raw_bytes, capture.copied_bytes)
    );
    assert!(prepared.task_work().await.unwrap().bytes <= bounded.total_bytes);
    prepared.close().await.unwrap();
    assert_eq!(confirmations(&fixture).await, 0);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn capture_copy_is_charged_once_after_bound_none_probe_and_resume() {
    let (fixture, auth) = fixture(false).await;
    let original = fixture.frame().await;
    let (_, prepared) = complete(
        &fixture,
        &auth,
        "copy-none",
        RunLimits::default().total_rows,
    )
    .await;
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    prepared.close().await.unwrap();
    let (local, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    let measured = budget.work;
    let limits = RunLimits::default();
    let wrong = PlanningCheckpoint::create(
        &fixture.file("copy-wrong-principal"),
        &fixture.db,
        "different-admin",
        &fixture.request,
        &local,
        &limits,
    )
    .await
    .unwrap();
    let wrong_path = wrong.close().await.unwrap();
    let untouched = retained_authority_work(&wrong_path).await;
    let denied = PlanningCheckpoint::committed_task(
        &wrong_path,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &limits,
        &mut budget,
    )
    .await;
    assert!(matches!(denied, Err(ManagementError::Confirmation)));
    assert_eq!(retained_authority_work(&wrong_path).await, untouched);

    let before = retained_authority_work(&retained).await;
    let mut no_capture = v3::files::stream::budget();
    assert!(
        PlanningCheckpoint::committed_task(
            &retained,
            &fixture.db,
            &auth,
            &fixture.request,
            &local,
            &limits,
            &mut no_capture
        )
        .await
        .unwrap()
        .is_none()
    );
    let after = retained_authority_work(&retained).await;
    let authority = (after.0 - before.0, after.1 - before.1);
    let before = after;
    assert!(
        PlanningCheckpoint::committed_task(
            &retained,
            &fixture.db,
            &auth,
            &fixture.request,
            &local,
            &limits,
            &mut budget
        )
        .await
        .unwrap()
        .is_none()
    );
    let after = retained_authority_work(&retained).await;
    assert_eq!(after.0 - before.0, authority.0 + measured.rows as i64);
    assert_eq!(
        after.1 - before.1,
        authority.1 + (measured.raw_bytes + measured.copied_bytes) as i64
    );
    let checkpoint = PlanningCheckpoint::resume(
        &retained,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &limits,
    )
    .await
    .unwrap();
    let before = checkpoint.total_work().await.unwrap();
    let caps = checkpoint.reserve_capture(&budget, &limits).unwrap();
    assert_eq!(
        (caps.total_rows, caps.total_bytes),
        (limits.total_rows, limits.total_bytes)
    );
    checkpoint
        .account_capture(&mut budget, &limits)
        .await
        .unwrap();
    assert_eq!(checkpoint.total_work().await.unwrap(), before);
    assert_eq!(
        (budget.work.raw_bytes, budget.work.copied_bytes),
        (measured.raw_bytes, measured.copied_bytes)
    );
    checkpoint.close().await.unwrap();
    assert_eq!(confirmations(&fixture).await, 0);
    assert!(fixture.frame().await == original);
}

#[tokio::test]
async fn capture_copy_is_charged_once_across_committed_recovery_and_export() {
    let (fixture, binding, retained, _) = committed_without_output().await;
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let (local, mut budget, _resident) = capture_with_independent_copy(&fixture).await;
    let capture = budget.work;
    let limits = RunLimits::default();
    let original = fixture.frame().await;
    let before = retained_authority_work(&retained).await;
    let mut no_capture = v3::files::stream::budget();
    let task = PlanningCheckpoint::committed_task(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &limits,
        &mut no_capture,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(task.binding() == &binding);
    task.close().await.unwrap();
    let after = retained_authority_work(&retained).await;
    let authority = (after.0 - before.0, after.1 - before.1);
    let before = after;
    let task = PlanningCheckpoint::committed_task(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &limits,
        &mut budget,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(task.binding() == &binding);
    let after = retained_authority_work(&retained).await;
    assert_eq!(after.0 - before.0, authority.0 + capture.rows as i64);
    assert_eq!(
        after.1 - before.1,
        authority.1 + (capture.raw_bytes + capture.copied_bytes) as i64
    );
    let before = task.task_work().await.unwrap();
    task.account_capture(&mut budget, &limits).await.unwrap();
    assert_eq!(task.task_work().await.unwrap(), before);
    assert_eq!(
        (budget.work.raw_bytes, budget.work.copied_bytes),
        (capture.raw_bytes, capture.copied_bytes)
    );
    let initial = budget.work;
    let (result, returned) = task
        .export_file(
            &fixture.db,
            (tempfile::tempfile().unwrap(), budget),
            Projection::Review,
            &limits,
        )
        .await;
    result.unwrap();
    let after = task.task_work().await.unwrap();
    assert!(returned.work.output_bytes > initial.output_bytes);
    // Physical output is observed separately from raw and independent-copy work.
    assert_eq!(
        after.bytes - before.bytes,
        returned.work.raw_bytes - initial.raw_bytes
            + (returned.work.copied_bytes - initial.copied_bytes)
    );
    assert_eq!(after.rows - before.rows, returned.work.rows - initial.rows);
    task.close().await.unwrap();
    assert!(fixture.frame().await == original);
}

async fn origin(fixture: &support::Fixture) -> SourceCheckpoint {
    let tx = fixture.db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    checkpoint
}

async fn complete(
    fixture: &support::Fixture,
    auth: &records::LocalAuthorization,
    name: &str,
    rows: u64,
) -> (Snapshot, PreparedPlanning) {
    let local = snapshot(fixture).await;
    let mut task = PlanningCheckpoint::create(
        &fixture.file(name),
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &limits(rows),
    )
    .await
    .unwrap();
    let frozen = task.snapshot().clone();
    loop {
        if let PlanningProgress::Complete(prepared) =
            task.advance(&fixture.db, &limits(rows)).await.unwrap()
        {
            return (frozen, *prepared);
        }
    }
}

fn acknowledgements(prepared: &PreparedPlanning) -> Vec<String> {
    prepared
        .plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect()
}

async fn confirmations(fixture: &support::Fixture) -> i64 {
    fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_management_records WHERE plan_id=$1 AND kind='action'",
            [fixture.request.plan_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn open_spool(path: &Path) -> DatabaseConnection {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false),
        )
        .await
        .unwrap();
    sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool)
}

enum Damage {
    Original,
    Membership,
    Totals,
}

async fn corrupt_retained_source(damage: Damage) {
    let (fixture, auth) = fixture(false).await;
    let before = fixture.frame().await;
    let original = origin(&fixture).await;
    let local = snapshot(&fixture).await;
    let task = PlanningCheckpoint::create(
        &fixture.file("retained"),
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &RunLimits::default(),
    )
    .await
    .unwrap();
    let retained = task.close().await.unwrap();
    let path = retained.join("source-000.sqlite");

    // Complete real source/reducer work before the union has bound a plan. No
    // phase, cursor, source vector, work counter or local protection is forged.
    let mut spool = InventorySpool::resume(&path).await.unwrap();
    while spool
        .scan(&fixture.db, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    while spool
        .verify_source(&fixture.db, &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    while spool
        .reduce(local.configuration.as_ref().unwrap(), &RunLimits::default())
        .await
        .unwrap()
        == Progress::Pending
    {}
    assert!(spool.frozen_review().await.is_ok());
    spool.close().await.unwrap();
    drop(spool);

    let stored = open_spool(&path).await;
    let key = serde_json::to_string(&NativeKey {
        table: "objects".into(),
        values: vec![fixture.identity.object_id.clone()],
    })
    .unwrap();
    let changed = match damage {
        Damage::Original => {
            let row = stored.query_one(Statement::from_sql_and_values(
                stored.get_database_backend(),
                "SELECT ordinal,bytes FROM parts WHERE table_name='objects' AND row_key=$1 AND column_name='metadata' AND offset=0",
                [key.into()],
            )).await.unwrap().unwrap();
            let ordinal: i64 = row.try_get("", "ordinal").unwrap();
            let bytes: Vec<u8> = row.try_get("", "bytes").unwrap();
            let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            value["private"] = "changed-private-original".into();
            let replacement = serde_json::to_vec(&value).unwrap();
            assert_ne!(replacement, bytes);
            let length = i64::try_from(replacement.len()).unwrap();
            stored
                .execute(Statement::from_sql_and_values(
                    stored.get_database_backend(),
                    "UPDATE parts SET bytes=$1,length=$2 WHERE ordinal=$3",
                    [replacement.into(), length.into(), ordinal.into()],
                ))
                .await
                .unwrap()
                .rows_affected()
        }
        Damage::Membership => stored
            .execute(Statement::from_sql_and_values(
                stored.get_database_backend(),
                "DELETE FROM members WHERE table_name='objects' AND row_key=$1",
                [key.into()],
            ))
            .await
            .unwrap()
            .rows_affected(),
        Damage::Totals => {
            let row = stored
                .query_one(Statement::from_string(
                    stored.get_database_backend(),
                    "SELECT payload FROM facts WHERE kind='totals' AND identity='inventory'",
                ))
                .await
                .unwrap()
                .unwrap();
            let mut totals: InventoryTotals =
                serde_json::from_str(&row.try_get::<String>("", "payload").unwrap()).unwrap();
            totals.leases += 1;
            stored
                .execute(Statement::from_sql_and_values(
                    stored.get_database_backend(),
                    "UPDATE facts SET payload=$1 WHERE kind='totals' AND identity='inventory'",
                    [serde_json::to_string(&totals).unwrap().into()],
                ))
                .await
                .unwrap()
                .rows_affected()
        }
    };
    assert_eq!(changed, 1);
    stored.close().await.unwrap();
    let current = origin(&fixture).await;
    assert!(current.revisions == original.revisions);
    assert_eq!(current.database_scope, original.database_scope);
    assert_eq!(current.database_identity, original.database_identity);
    assert!(fixture.frame().await == before);

    let local = snapshot(&fixture).await;
    let rejected = async {
        let mut task = PlanningCheckpoint::resume(
            &retained,
            &fixture.db,
            auth.principal(),
            &fixture.request,
            &local,
            &RunLimits::default(),
        )
        .await?;
        let frozen = task.snapshot().clone();
        loop {
            match task
                .advance(&fixture.db, &limits(RunLimits::default().total_rows))
                .await
            {
                Ok(PlanningProgress::Incomplete) => {}
                Ok(PlanningProgress::Complete(prepared)) => {
                    let result = loop {
                        let advanced = prepared
                            .advance_confirmation(
                                &fixture.db,
                                &fixture.config,
                                &auth,
                                &fixture.request,
                                &frozen,
                                &limits(RunLimits::default().total_rows),
                            )
                            .await;
                        match advanced.progress {
                            Err(error) => break Err(error),
                            Ok(ConfirmationProgress::Incomplete) => continue,
                            Ok(ConfirmationProgress::Ready(ready)) => {
                                let confirmed = records::confirm_ready(
                                    &fixture.db,
                                    &fixture.config,
                                    &auth,
                                    records::ReadyPublication {
                                        ready: &ready,
                                        acknowledgements: &acknowledgements(&prepared),
                                        ttl: chrono::Duration::seconds(30),
                                    },
                                    Default::default(),
                                    &limits(RunLimits::default().total_rows),
                                )
                                .await;
                                break confirmed.progress.map(|_| ());
                            }
                            Ok(ConfirmationProgress::Confirmed(_)) => break Ok(()),
                        }
                    };
                    prepared.close().await.unwrap();
                    return result;
                }
                Err(error) => {
                    task.close().await.unwrap();
                    return Err(error);
                }
            }
        }
    }
    .await;
    assert!(
        matches!(
            rejected,
            Err(ManagementError::Changed
                | ManagementError::InvalidEvidence
                | ManagementError::Confirmation)
        ),
        "corrupt retained originals/derived facts must not become a confirmation: {rejected:?}"
    );
    assert_eq!(confirmations(&fixture).await, 0);
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn retained_original_bytes_cannot_be_confirmed_with_an_unchanged_live_vector() {
    corrupt_retained_source(Damage::Original).await;
}

#[tokio::test]
async fn retained_complete_membership_cannot_drop_an_original_before_confirmation() {
    corrupt_retained_source(Damage::Membership).await;
}

#[tokio::test]
async fn retained_reducer_totals_cannot_replace_the_semantics_of_unchanged_originals() {
    corrupt_retained_source(Damage::Totals).await;
}

#[tokio::test]
async fn confirmation_resumes_single_page_slices_through_canonical_and_ready() {
    confirmation_slices(false).await;
}

#[tokio::test]
async fn mixed_confirmation_reopens_after_local_union_merge_before_finish() {
    confirmation_slices(true).await;
}

async fn confirmation_slices(mixed: bool) {
    let (fixture, auth) = two_provider_fixture(mixed).await;
    let calls_before_confirmation = fixture.remote_calls().await;
    if mixed {
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,'existing-remote',10,'pinned',1,0,$3)",
            vec![fixture.config.pinning.providers[0].identity.allocation_key().into(),fixture.identity.cid.clone().into(),chrono::Utc::now().into()])).await.unwrap();
        ledger::capture_reservation(
            &fixture.db,
            &fixture.config.pinning.providers[0]
                .identity
                .allocation_key(),
            &fixture.identity.cid,
        )
        .await
        .unwrap();
    }
    let rows = RunLimits::default().total_rows;
    let (frozen, mut prepared) = complete(&fixture, &auth, "confirmation-slices", rows).await;
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    let canonical_union = retained.join(format!(
        "canonical-union-{}.sqlite",
        prepared
            .plan
            .items()
            .iter()
            .find_map(|entry| {
                if let Some(ipfs_s3_gateway::pinning::management::Capture::Provider(capture)) =
                    &entry.capture
                {
                    Some(capture.inventory.inventory_id.clone())
                } else {
                    None
                }
            })
            .unwrap()
    ));
    let mut merged_before_finish = false;
    if mixed {
        assert!(!frozen.remotes.is_empty());
    }
    let mut incomplete = 0;
    let mut staging_commits = 0;
    loop {
        let mut slice = limits(rows);
        slice.pages = 1;
        let advanced = prepared
            .advance_confirmation(
                &fixture.db,
                &fixture.config,
                &auth,
                &fixture.request,
                &frozen,
                &slice,
            )
            .await;
        staging_commits += advanced.effects.database_commits;
        assert!(!advanced.effects.commit_unknown);
        match advanced
            .progress
            .unwrap_or_else(|error| panic!("{:?}: {error:?}", advanced.phase))
        {
            ConfirmationProgress::Incomplete => {
                incomplete += 1;
                assert_eq!(confirmations(&fixture).await, 0);
                prepared.close().await.unwrap();
                if mixed && canonical_union.exists() {
                    let db = open_spool(&canonical_union).await;
                    if let Some(row) = db.query_one(Statement::from_string(db.get_database_backend(),
                        "SELECT stage,after_key FROM output WHERE id=1 AND NOT EXISTS(SELECT 1 FROM merged WHERE complete=0) AND EXISTS(SELECT 1 FROM quotes)")).await.unwrap()
                        && row.try_get::<i64>("","stage").unwrap()==0 && row.try_get::<String>("","after_key").unwrap().is_empty() {
                            merged_before_finish = true;
                            let guard = db.query_one(Statement::from_string(db.get_database_backend(),"SELECT dirty FROM canonical_cache_guard WHERE id=1")).await.unwrap().unwrap();
                            assert_eq!(guard.try_get::<i64>("","dirty").unwrap(),0);
                    }
                    db.close().await.unwrap();
                }
                let mut task = PlanningCheckpoint::resume(
                    &retained,
                    &fixture.db,
                    auth.principal(),
                    &fixture.request,
                    &snapshot(&fixture).await,
                    &limits(rows),
                )
                .await
                .unwrap();
                prepared = loop {
                    if let PlanningProgress::Complete(prepared) =
                        task.advance(&fixture.db, &limits(rows)).await.unwrap()
                    {
                        break *prepared;
                    }
                };
            }
            ConfirmationProgress::Ready(ready) => {
                assert!(incomplete > 0);
                assert!(staging_commits > 0);
                assert_eq!(confirmations(&fixture).await, 0);
                drop(ready);
                prepared.close().await.unwrap();
                let mut task = PlanningCheckpoint::resume(
                    &retained,
                    &fixture.db,
                    auth.principal(),
                    &fixture.request,
                    &snapshot(&fixture).await,
                    &limits(rows),
                )
                .await
                .unwrap();
                prepared = loop {
                    if let PlanningProgress::Complete(prepared) =
                        task.advance(&fixture.db, &limits(rows)).await.unwrap()
                    {
                        break *prepared;
                    }
                };
                let resumed = prepared
                    .advance_confirmation(
                        &fixture.db,
                        &fixture.config,
                        &auth,
                        &fixture.request,
                        &frozen,
                        &limits(rows),
                    )
                    .await;
                assert_eq!(resumed.effects.database_commits, 0);
                assert!(!resumed.effects.commit_unknown);
                let ConfirmationProgress::Ready(ready) = resumed.progress.unwrap() else {
                    panic!("an accepted Ready root must survive reopening");
                };
                let result = records::confirm_ready(
                    &fixture.db,
                    &fixture.config,
                    &auth,
                    records::ReadyPublication {
                        ready: &ready,
                        acknowledgements: &acknowledgements(&prepared),
                        ttl: chrono::Duration::seconds(30),
                    },
                    Default::default(),
                    &limits(rows),
                )
                .await;
                assert!(matches!(
                    result.progress.unwrap(),
                    ConfirmationProgress::Confirmed(_)
                ));
                assert_eq!(result.effects.database_commits, 1);
                prepared.cleanup().await.unwrap();
                break;
            }
            ConfirmationProgress::Confirmed(_) => panic!("staging cannot publish a confirmation"),
        }
    }
    assert_eq!(
        confirmations(&fixture).await,
        fixture.request.items.len() as i64
    );
    assert_eq!(fixture.remote_calls().await, calls_before_confirmation);
    if mixed {
        assert!(merged_before_finish);
    }
}

async fn preflight(
    fixture: &support::Fixture,
    auth: &records::LocalAuthorization,
    snapshot: &Snapshot,
    prepared: &PreparedPlanning,
    rows: u64,
    budget: native::meter::Budget,
) -> native::meter::Budget {
    let acks = acknowledgements(prepared);
    let output_before = budget.work.output_bytes;
    let run = limits(rows);
    let caps = prepared.reserve_capture(&budget, &run).unwrap();
    let advanced = prepared
        .advance_confirmation(
            &fixture.db,
            &fixture.config,
            auth,
            &fixture.request,
            snapshot,
            &caps,
        )
        .await;
    let ConfirmationProgress::Ready(ready) = advanced.progress.unwrap() else {
        panic!("small fixture must finish archive preparation");
    };
    let (result, budget) = ready
        .preflight(
            &fixture.db,
            budget,
            Header {
                projection: Projection::Review,
                database_scope: auth.database_scope(),
                database_identity: auth.database_identity(),
                principal: auth.principal(),
                catalog: &snapshot.native_frame.as_ref().unwrap().schema_versions,
                created_at: snapshot.database_now.unwrap().to_rfc3339(),
                expires_at: None,
            },
            &acks,
            &run,
        )
        .await;
    result.unwrap();
    assert_eq!(budget.work.output_bytes, output_before);
    budget
}

#[tokio::test]
async fn confirmation_publication_cannot_reset_the_shared_spools_and_union_task_envelope() {
    let (calibration, auth) = two_provider_fixture(false).await;
    let rows = RunLimits::default().total_rows;
    let (local, prepared) = complete(&calibration, &auth, "calibration", rows).await;
    let wire = preflight(
        &calibration,
        &auth,
        &local,
        &prepared,
        rows,
        v3::files::stream::budget(),
    )
    .await;
    let preflight_work = prepared.task_work().await.unwrap();
    let binding = records::confirm(
        &calibration.db,
        &calibration.config,
        &auth,
        (&calibration.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let published = prepared.task_work().await.unwrap();
    let mut ceiling = preflight_work.rows;
    for index in 0..calibration.request.items.len() {
        let spool = InventorySpool::resume(
            &prepared
                .checkpoint_path()
                .unwrap()
                .join(format!("source-{index:03}.sqlite")),
        )
        .await
        .unwrap();
        ceiling = ceiling.max(spool.task_work().await.unwrap().rows);
        spool.close().await.unwrap();
    }
    assert!(ceiling < rows);
    assert!(published.rows > ceiling);
    let (result, output_budget) = prepared
        .export_confirmed_file(
            &calibration.db,
            (tempfile::tempfile().unwrap(), wire),
            &binding,
            Projection::Review,
            &limits(rows),
        )
        .await;
    result.unwrap();
    eprintln!(
        "checkpoint confirmation calibration preflight={preflight_work:?} published={published:?} explicit_shared_row_ceiling={ceiling} wire_work={:?}",
        output_budget.work
    );
    prepared.cleanup().await.unwrap();

    let (fixture, auth) = two_provider_fixture(false).await;
    let (local, prepared) = complete(&fixture, &auth, "bounded", ceiling).await;
    let _wire = preflight(
        &fixture,
        &auth,
        &local,
        &prepared,
        ceiling,
        v3::files::stream::budget(),
    )
    .await;
    let result = records::confirm(
        &fixture.db,
        &fixture.config,
        &auth,
        (&fixture.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await;
    let used = prepared.task_work().await.unwrap();
    prepared.close().await.unwrap();
    assert!(
        matches!(result, Err(ManagementError::Budget)),
        "publication must reject a genuinely exhausted aggregate envelope, not renew each spool's allowance"
    );
    assert!(used.rows <= ceiling);
    assert_eq!(confirmations(&fixture).await, 0);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn final_output_and_committed_retry_share_the_original_task_allowance() {
    let rows = RunLimits::default().total_rows;
    let (calibration, auth) = two_provider_fixture(false).await;
    let (local, prepared) = complete(&calibration, &auth, "output-calibration", rows).await;
    let wire = preflight(
        &calibration,
        &auth,
        &local,
        &prepared,
        rows,
        v3::files::stream::budget(),
    )
    .await;
    let binding = records::confirm(
        &calibration.db,
        &calibration.config,
        &auth,
        (&calibration.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let published = prepared.task_work().await.unwrap();
    let initial = wire.work;
    let (result, output) = prepared
        .export_confirmed_file(
            &calibration.db,
            (tempfile::tempfile().unwrap(), wire),
            &binding,
            Projection::Review,
            &limits(rows),
        )
        .await;
    result.unwrap();
    let complete_work = prepared.task_work().await.unwrap();
    assert_eq!(
        complete_work.rows - published.rows,
        output.work.rows - initial.rows
    );
    assert_eq!(
        complete_work.bytes - published.bytes,
        output.work.raw_bytes - initial.raw_bytes
            + (output.work.copied_bytes - initial.copied_bytes)
    );
    assert!(output.work.rows - initial.rows > 2);
    let ceiling = published.rows + (output.work.rows - initial.rows) / 2;
    prepared.cleanup().await.unwrap();

    let (fixture, auth) = two_provider_fixture(false).await;
    let (local, prepared) = complete(&fixture, &auth, "bounded-output", ceiling).await;
    let wire = preflight(
        &fixture,
        &auth,
        &local,
        &prepared,
        ceiling,
        v3::files::stream::budget(),
    )
    .await;
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &auth,
        (&fixture.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let before = prepared.task_work().await.unwrap();
    assert!(before.rows < ceiling);
    let frame = native::capture(&fixture.db).await.unwrap();
    let partial_path = fixture.file("bounded-output.bin");
    let file = v3::files::reserve(partial_path.to_str().unwrap())
        .await
        .unwrap()
        .into_std()
        .await;
    let (result, _wire) = prepared
        .export_confirmed_file(
            &fixture.db,
            (file, wire),
            &binding,
            Projection::Review,
            &limits(ceiling),
        )
        .await;
    assert!(matches!(result, Err(ManagementError::Budget)));
    let spent = prepared.task_work().await.unwrap();
    assert!(spent.rows > before.rows);
    assert!(spent.rows <= ceiling);
    let partial = std::fs::read(&partial_path).unwrap();
    assert!(!partial.is_empty());
    assert!(
        v3::document::inspect(
            std::io::Cursor::new(&partial),
            Projection::Review,
            v3::files::stream::budget(),
        )
        .is_err()
    );
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    prepared.close().await.unwrap();

    let fresh = snapshot(&fixture).await;
    assert_eq!(spent.rows, ceiling);
    let retry = PlanningCheckpoint::committed_task(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &fresh,
        &limits(ceiling),
        &mut v3::files::stream::budget(),
    )
    .await;
    assert!(matches!(retry, Err(ManagementError::Budget)));
    assert_eq!(std::fs::read(&partial_path).unwrap(), partial);
    assert!(retained.is_dir());
    assert!(native::capture(&fixture.db).await.unwrap() == frame);
    assert_eq!(
        confirmations(&fixture).await,
        fixture.request.items.len() as i64
    );
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn ordinary_global_collection_accounts_complete_file_output_once() {
    let (fixture, auth) = two_provider_fixture(false).await;
    let (local, scope) =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
            .await
            .unwrap()
            .planning_with_scope()
            .unwrap();
    let measured = scope.work();
    let mut budget = v3::files::stream::budget();
    budget
        .charge(
            measured.sql,
            measured.rows,
            measured.cells,
            measured.raw_bytes,
        )
        .unwrap();
    budget.work.copied_bytes = measured.copied_bytes;
    budget.work.output_bytes = measured.output_bytes;
    budget.work.pages = measured.pages;
    assert!(scope.peak_resident() > 0);
    let _source_resident = budget.reserve(scope.peak_resident()).unwrap();
    let original_cancellation = budget.cancellation();
    let prepared = PreparedPlanning::collect_with_limits(
        &fixture.db,
        &fixture.request,
        &local,
        records::SharedDecodeBudget::default(),
        &limits(RunLimits::default().total_rows),
    )
    .await
    .unwrap();
    assert!(prepared.checkpoint_path().is_none());
    let before = prepared.task_work().await.unwrap();
    let (result, mut budget) = prepared
        .export_file(
            (tempfile::tempfile().unwrap(), budget),
            Header {
                projection: Projection::Review,
                database_scope: auth.database_scope(),
                database_identity: auth.database_identity(),
                principal: auth.principal(),
                catalog: &local.native_frame.as_ref().unwrap().schema_versions,
                created_at: local.database_now.unwrap().to_rfc3339(),
                expires_at: None,
            },
            &fixture.request,
            (&local, Some(&scope)),
            None,
            &limits(RunLimits::default().total_rows),
        )
        .await;
    result.unwrap();
    let after = prepared.task_work().await.unwrap();
    assert_eq!(after.rows - before.rows, budget.work.rows);
    assert_eq!(
        after.bytes - before.bytes,
        budget.work.raw_bytes + budget.work.copied_bytes
    );
    assert!(after.pages > before.pages);
    assert!(budget.work.copied_bytes >= measured.copied_bytes);
    original_cancellation.store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(budget.check().is_err());
    original_cancellation.store(false, std::sync::atomic::Ordering::Relaxed);
    budget.check().unwrap();
    assert!(
        budget
            .reserve(v3::files::stream::limits().resident)
            .is_err()
    );

    let (_, retry_scope) =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
            .await
            .unwrap()
            .planning_with_scope()
            .unwrap();
    let capture = retry_scope.work();
    let mut ceiling = v3::files::stream::limits();
    ceiling.bytes = capture.raw_bytes + 64;
    let mut retry_budget = native::meter::Budget::new(ceiling);
    retry_budget
        .charge(capture.sql, capture.rows, capture.cells, capture.raw_bytes)
        .unwrap();
    retry_budget.work.copied_bytes = capture.copied_bytes;
    retry_budget.work.output_bytes = capture.output_bytes;
    retry_budget.work.pages = capture.pages;
    let before_failure = prepared.task_work().await.unwrap();
    let (result, retry_budget) = prepared
        .prepare_output(
            retry_budget,
            Header {
                projection: Projection::Review,
                database_scope: auth.database_scope(),
                database_identity: auth.database_identity(),
                principal: auth.principal(),
                catalog: &local.native_frame.as_ref().unwrap().schema_versions,
                created_at: local.database_now.unwrap().to_rfc3339(),
                expires_at: None,
            },
            &fixture.request,
            &local,
            &limits(RunLimits::default().total_rows),
        )
        .await;
    assert!(matches!(result, Err(ManagementError::Budget)));
    assert!(retry_budget.check().is_err());
    let failed = prepared.task_work().await.unwrap();
    assert_eq!(failed.rows - before_failure.rows, retry_budget.work.rows);
    assert_eq!(
        failed.bytes - before_failure.bytes,
        retry_budget.work.raw_bytes + capture.copied_bytes
    );
    prepared.close().await.unwrap();
}

async fn committed_without_output() -> (
    support::Fixture,
    records::ConfirmedBinding,
    std::path::PathBuf,
    v3::document::DocumentHead,
) {
    let (fixture, auth) = two_provider_fixture(true).await;
    let (local, prepared) = complete(
        &fixture,
        &auth,
        "committed",
        RunLimits::default().total_rows,
    )
    .await;
    let retained = prepared.checkpoint_path().unwrap().to_owned();
    assert!(!acknowledgements(&prepared).is_empty());
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &auth,
        (&fixture.request, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    prepared.close().await.unwrap();
    let mut writer = Writer::new(
        tempfile::tempfile().unwrap(),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    let fresh = snapshot(&fixture).await;
    let recovered = PlanningCheckpoint::committed(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &fresh,
        &RunLimits::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(recovered == binding);
    v3::planning::confirmed(&fixture.db, &mut writer, &binding, Projection::Review)
        .await
        .unwrap();
    let (file, budget) = writer.finish_with_budget().unwrap();
    let (original, file, _) = v3::document::inspect(file, Projection::Review, budget).unwrap();
    drop(file);
    assert_eq!(
        confirmations(&fixture).await,
        fixture.request.items.len() as i64
    );
    assert!(retained.is_dir());
    assert!(!fixture.file("recovered.v3").exists());
    (fixture, binding, retained, original)
}

async fn resume_confirmation(
    fixture: &support::Fixture,
    retained: &Path,
    output: &Path,
    ttl: &str,
    acks: &[String],
) -> Output {
    let request = fixture.request_file();
    let mut args = vec![
        "--pinning-manage",
        request.to_str().unwrap(),
        "--source-node",
        support::NODE,
        "--resume",
        retained.to_str().unwrap(),
        "--scan-pages",
        "512",
        "--confirm",
        output.to_str().unwrap(),
        "--ttl-seconds",
        ttl,
    ];
    for item in acks {
        args.extend(["--ack-item", item.as_str()]);
    }
    support::output(&mut fixture.command(&args)).await
}

#[tokio::test]
async fn committed_confirmation_can_be_reexported_after_the_response_and_final_output_are_lost() {
    let (fixture, binding, retained, original_header) = committed_without_output().await;
    let original = fixture.frame().await;
    let remote_calls = fixture.remote_calls().await;
    let partial = fixture.file("partial.v3");
    std::fs::write(&partial, b"unpublished partial output").unwrap();
    let occupied = resume_confirmation(
        &fixture,
        &retained,
        &partial,
        "30",
        &binding.confirmation.candidate_item_ids,
    )
    .await;
    assert!(!occupied.status.success());
    assert_eq!(
        std::fs::read(&partial).unwrap(),
        b"unpublished partial output"
    );
    assert!(retained.is_dir());
    assert!(fixture.frame().await == original);

    let recovered = fixture.file("recovered.v3");
    let output = resume_confirmation(
        &fixture,
        &retained,
        &recovered,
        "30",
        &binding.confirmation.candidate_item_ids,
    )
    .await;
    let report = support::report(&output);
    assert_eq!(report["artifact_exported"], true);
    let document = support::document(&recovered);
    assert!(document.request.as_ref() == Some(&fixture.request));
    assert_eq!(
        document.acknowledgements.as_deref(),
        Some(binding.confirmation.candidate_item_ids.as_slice())
    );
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap(),
        chrono::DateTime::parse_from_rfc3339(&original_header.created_at).unwrap()
    );
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap(),
        binding.expires_at
    );
    assert_eq!(
        confirmations(&fixture).await,
        fixture.request.items.len() as i64
    );
    assert!(fixture.frame().await == original);
    assert_eq!(
        std::fs::read(&partial).unwrap(),
        b"unpublished partial output"
    );
    assert_eq!(fixture.remote_calls().await, remote_calls);
}

async fn archive_storage(fixture: &support::Fixture) -> (i64, i64, i64) {
    let row = fixture.db.query_one(Statement::from_string(fixture.db.get_database_backend(),
        "SELECT (SELECT COUNT(*) FROM pin_management_archive) AS archives, (SELECT COUNT(*) FROM pin_management_archive_chunks) AS chunks, (SELECT COUNT(*) FROM pin_management_records) AS records"))
        .await.unwrap().unwrap();
    (
        row.try_get("", "archives").unwrap(),
        row.try_get("", "chunks").unwrap(),
        row.try_get("", "records").unwrap(),
    )
}

async fn frozen_document(
    fixture: &support::Fixture,
    binding: &records::ConfirmedBinding,
) -> Vec<u8> {
    let mut writer =
        Writer::new(Vec::new(), Projection::Review, v3::files::stream::budget()).unwrap();
    v3::planning::confirmed(&fixture.db, &mut writer, binding, Projection::Review)
        .await
        .unwrap();
    writer.finish().unwrap()
}

async fn assert_read_only_original_reexport(
    fixture: &support::Fixture,
    binding: &records::ConfirmedBinding,
    retained: &Path,
    expected: &[u8],
) {
    let before = fixture.frame().await;
    let storage = archive_storage(fixture).await;
    let remote_calls = fixture.remote_calls().await;
    let partial = fixture.file("unpublished-original.v3");
    std::fs::write(&partial, b"operator partial").unwrap();
    let recovered = fixture.file("recovered-after-history.v3");
    let output = resume_confirmation(
        fixture,
        retained,
        &recovered,
        "30",
        &binding.confirmation.candidate_item_ids,
    )
    .await;
    // Every recovery attempt leaves business rows and archive storage unchanged.
    assert!(fixture.frame().await == before);
    assert_eq!(archive_storage(fixture).await, storage);
    assert_eq!(fixture.remote_calls().await, remote_calls);
    assert_eq!(std::fs::read(&partial).unwrap(), b"operator partial");
    let report = support::report(&output);
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["effects"]["database_commits"], 0);
    assert_eq!(report["effects"]["commit_unknown"], false);
    assert_eq!(report["recovered_original_confirmation"], true);
    assert_eq!(std::fs::read(&recovered).unwrap(), expected);
    let document = support::document(&recovered);
    assert_eq!(
        document.acknowledgements.as_deref(),
        Some(binding.confirmation.candidate_item_ids.as_slice())
    );
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap(),
        binding.expires_at
    );
    assert_eq!(
        chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap(),
        binding.confirmed_at
    );
}

#[tokio::test]
async fn committed_output_recovery_preserves_original_after_unrelated_plan_history() {
    let (fixture, binding, retained, _) = committed_without_output().await;
    let expected = frozen_document(&fixture, &binding).await;
    let provider = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| {
            matches!(
                entry.capture,
                Some(ipfs_s3_gateway::pinning::management::Capture::Provider(_))
            )
        })
        .unwrap();
    let mut private_before = Vec::new();
    records::copy_original_inventory(
        &fixture.db,
        &binding,
        provider,
        &mut private_before,
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let later = PlanRequest {
        plan_id: "independent-later-history".into(),
        items: fixture
            .request
            .items
            .iter()
            .filter(|item| matches!(item.subject, Subject::Object { .. }))
            .cloned()
            .collect(),
    };
    assert!(!later.items.is_empty());
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &later)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = PreparedPlanning::collect(&fixture.db, &later, &local, Default::default())
        .await
        .unwrap();
    let before = archive_storage(&fixture).await;
    let history = records::confirm(
        &fixture.db,
        &fixture.config,
        &auth,
        (&later, &local, &prepared.plan),
        acknowledgements(&prepared),
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    assert!(*history.request == later);
    prepared.close().await.unwrap();
    assert!(archive_storage(&fixture).await.2 > before.2);
    let mut private_after = Vec::new();
    records::copy_original_inventory(
        &fixture.db,
        &binding,
        provider,
        &mut private_after,
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    assert_eq!(private_after, private_before);
    assert_read_only_original_reexport(&fixture, &binding, &retained, &expected).await;
}

#[tokio::test]
async fn committed_output_recovery_preserves_original_after_live_object_replacement() {
    let (fixture, binding, retained, _) = committed_without_output().await;
    let expected = frozen_document(&fixture, &binding).await;
    fixture.replace_object().await;
    assert_read_only_original_reexport(&fixture, &binding, &retained, &expected).await;
}

async fn retained_authority_work(retained: &Path) -> (i64, i64) {
    let db = open_spool(&retained.join("source-000.sqlite")).await;
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT rows_used,bytes_used FROM task_budget WHERE id=1",
        ))
        .await
        .unwrap()
        .unwrap();
    let work = (
        row.try_get("", "rows_used").unwrap(),
        row.try_get("", "bytes_used").unwrap(),
    );
    db.close().await.unwrap();
    work
}

#[tokio::test]
async fn committed_authority_reads_charge_success_and_repeated_original_corruption_failures() {
    let (fixture, binding, retained, _) = committed_without_output().await;
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let local = snapshot(&fixture).await;
    let original = fixture.frame().await;
    let before = retained_authority_work(&retained).await;
    let task = PlanningCheckpoint::committed_task(
        &retained,
        &fixture.db,
        &auth,
        &fixture.request,
        &local,
        &RunLimits::default(),
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(task.binding() == &binding);
    task.close().await.unwrap();
    let after = retained_authority_work(&retained).await;
    assert!(after.0 > before.0 && after.1 > before.1);
    assert!(fixture.frame().await == original);
    let damaged = fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_records SET binding='!' || substr(binding,2) WHERE plan_id=$1 AND item_id=$2 AND kind='action' AND state='prepared'",
        [fixture.request.plan_id.clone().into(), fixture.request.items[0].item_id.clone().into()])).await.unwrap();
    assert_eq!(damaged.rows_affected(), 1);
    let changed = fixture.frame().await;
    let storage = archive_storage(&fixture).await;
    for _ in 0..2 {
        let before = retained_authority_work(&retained).await;
        let result = PlanningCheckpoint::committed_task(
            &retained,
            &fixture.db,
            &auth,
            &fixture.request,
            &local,
            &RunLimits::default(),
            &mut v3::files::stream::budget(),
        )
        .await;
        assert!(result.is_err());
        let after = retained_authority_work(&retained).await;
        assert!(after.0 > before.0 && after.1 > before.1);
        assert!(fixture.frame().await == changed);
        assert_eq!(archive_storage(&fixture).await, storage);
    }
}

async fn committed_parameters_remain_original(ttl: &str, replacement_acks: Option<Vec<String>>) {
    let (fixture, binding, retained, original_header) = committed_without_output().await;
    let before = fixture.frame().await;
    let remote_calls = fixture.remote_calls().await;
    let acks = replacement_acks.unwrap_or_else(|| binding.confirmation.candidate_item_ids.to_vec());
    let recovered = fixture.file("conflicting-parameters.v3");
    let output = resume_confirmation(&fixture, &retained, &recovered, ttl, &acks).await;
    if output.status.success() {
        let document = support::document(&recovered);
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(&document.created_at).unwrap(),
            chrono::DateTime::parse_from_rfc3339(&original_header.created_at).unwrap()
        );
        assert_eq!(
            chrono::DateTime::parse_from_rfc3339(document.expires_at.as_ref().unwrap()).unwrap(),
            binding.expires_at
        );
        assert_eq!(
            document.acknowledgements.as_deref(),
            Some(binding.confirmation.candidate_item_ids.as_slice())
        );
    } else {
        assert!(retained.is_dir());
    }
    assert_eq!(
        confirmations(&fixture).await,
        fixture.request.items.len() as i64
    );
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, remote_calls);
}

#[tokio::test]
async fn committed_output_recovery_cannot_renew_the_original_ttl() {
    committed_parameters_remain_original("900", None).await;
}

#[tokio::test]
async fn committed_output_recovery_cannot_replace_the_original_acknowledgements() {
    committed_parameters_remain_original("30", Some(vec!["provider-1".into()])).await;
}
