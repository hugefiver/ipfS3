//! File-backed acceptance helpers; source data and artifacts are never bulk DTOs.
#[path = "stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
pub mod cli;
use cli::publication;

use futures_util::TryStreamExt;
use ipfs_s3_gateway::{
    config::Config,
    pinning::management::{
        Action, Capture, ItemRequest, NativeKey, PlanEntry, PlanRequest, RetirementSelection,
        RouteIdentity, Subject, db_snapshot, local_config,
        retirement::{
            InventorySpool, PlanningCheckpoint, RetirementInventoryRef, RunLimits, TaskWork,
        },
        wire::v3::{self, CellValue, Event, Frame, Projection, Reader, Section},
    },
    store::pinning::{ledger, management::native},
};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement, StreamTrait, TransactionTrait};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    path::Path,
    process::Output,
    time::Instant,
};

pub async fn fixture() -> cli::Fixture {
    let mut fixture = cli::Fixture::new(false).await;
    let mut raw: toml::Value =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let providers = raw["pinning"]["providers"].as_array_mut().unwrap();
    let mut second = providers
        .iter()
        .find(|p| p["name"].as_str() == Some("fixture"))
        .unwrap()
        .clone();
    second["name"] = "second".into();
    providers.push(second);
    let identities = raw["pinning_identity"]["providers"].as_array_mut().unwrap();
    let mut second = identities
        .iter()
        .find(|p| p["config_name"].as_str() == Some("fixture"))
        .unwrap()
        .clone();
    second["config_name"] = "second".into();
    second["provider_id"] = "scale-second-provider".into();
    second["scope"] = "scale-second-account".into();
    identities.push(second);
    let contents = toml::to_string(&raw).unwrap();
    let raw: Config = toml::from_str(&contents).unwrap();
    fixture.config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(cli::NODE.into())
        .unwrap();
    std::fs::write(&fixture.config_path, contents).unwrap();

    // The existing publication helper accepts one selected provider. Resolve
    // config names to stable identities before selecting normalized providers.
    for name in ["fixture", "second"] {
        let provider_id = &raw
            .pinning_identity
            .providers
            .iter()
            .find(|provider| provider.config_name == name)
            .unwrap()
            .provider_id;
        let mut selected = local_config::compile(&raw, |_| None)
            .unwrap()
            .with_source_node(cli::NODE.into())
            .unwrap();
        selected
            .pinning
            .providers
            .retain(|p| &p.identity.provider_id == provider_id);
        assert_eq!(selected.pinning.providers.len(), 1);
        let identity = &selected.pinning.providers.first().unwrap().identity;
        ledger::register_route(&fixture.db, &identity.allocation_key(), identity)
            .await
            .unwrap();
        let object = publication::publish(&fixture.db, &selected, name, true).await;
        publication::write_current_decision(&fixture.db, &fixture.config, &object).await;
    }
    let routes: Vec<_> = ["second", "fixture"]
        .into_iter()
        .map(|name| {
            let provider_id = &raw
                .pinning_identity
                .providers
                .iter()
                .find(|provider| provider.config_name == name)
                .unwrap()
                .provider_id;
            let provider = fixture
                .config
                .pinning
                .providers
                .iter()
                .find(|p| &p.identity.provider_id == provider_id)
                .unwrap();
            RouteIdentity::from_original(provider.route_snapshot())
        })
        .collect();
    assert!(routes[0] != routes[1]);
    assert_ne!(routes[0].provider_id, routes[1].provider_id);
    assert_ne!(routes[0].scope, routes[1].scope);
    fixture.request = PlanRequest {
        plan_id: "cli-real-100k-two-provider".into(),
        items: routes
            .into_iter()
            .map(|route| ItemRequest {
                item_id: route.provider_id.clone(),
                subject: Subject::Provider { route },
                action: Action::RetireWith {
                    selection: RetirementSelection::Retain,
                },
            })
            .collect(),
    };
    fixture
}

/// Only unrelated ordinary objects/versions are SQL-seeded. Related ownership,
/// routes, decisions and publication receipts come from the real fixture APIs.
pub async fn seed_unrelated(db: &DatabaseConnection) {
    let started = Instant::now();
    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    for first in (1..=100_000).step_by(1000) {
        let last = first + 999;
        let series = format!(
            "WITH RECURSIVE numbers(n) AS (SELECT {first} UNION ALL SELECT n+1 FROM numbers WHERE n < {last})"
        );
        let tx = db.begin().await.unwrap();
        for (prefix, latest, condition, version, sequence) in [
            ("scale", "TRUE", "", "NULL", 1),
            ("scale-old", "FALSE", " WHERE n % 10 = 0", "'old-' || n", 0),
        ] {
            tx.execute(Statement::from_sql_and_values(db.get_database_backend(), format!(
                "{series} INSERT INTO objects \
                 (id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) \
                 SELECT '{prefix}-object-' || n,'bucket','scale/' || n,$1,10,$1,FALSE,FALSE,{latest},CURRENT_TIMESTAMP FROM numbers{condition}"
            ), [CID.into()])).await.unwrap();
            tx.execute(Statement::from_string(db.get_database_backend(), format!(
                "{series} INSERT INTO object_versions \
                 (id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) \
                 SELECT '{prefix}-version-' || n,'bucket','scale/' || n,{version},'object','{prefix}-object-' || n,{sequence},{latest},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM numbers{condition}"
            ))).await.unwrap();
        }
        tx.commit().await.unwrap();
    }
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT (SELECT COUNT(*) FROM objects WHERE id LIKE 'scale-object-%') AS current_objects, \
         (SELECT COUNT(*) FROM objects WHERE id LIKE 'scale-old-object-%') AS historical_objects, \
         (SELECT COUNT(*) FROM object_versions WHERE id LIKE 'scale-version-%') AS current_versions, \
         (SELECT COUNT(*) FROM object_versions WHERE id LIKE 'scale-old-version-%') AS historical_versions"
    )).await.unwrap().unwrap();
    for column in ["current_objects", "current_versions"] {
        assert_eq!(row.try_get::<i64>("", column).unwrap(), 100_000);
    }
    for column in ["historical_objects", "historical_versions"] {
        assert_eq!(row.try_get::<i64>("", column).unwrap(), 10_000);
    }
    eprintln!("100k fixture seed wall={:?}", started.elapsed());
}

pub async fn run(
    fixture: &cli::Fixture,
    checkpoint: &Path,
    resume: bool,
    pages: &str,
    output: &Path,
    confirm: bool,
) -> Output {
    let request = fixture.request_file();
    let mut args = vec![
        "--pinning-manage",
        request.to_str().unwrap(),
        "--source-node",
        cli::NODE,
        if resume { "--resume" } else { "--checkpoint" },
        checkpoint.to_str().unwrap(),
        "--scan-pages",
        pages,
        if confirm { "--confirm" } else { "--output" },
        output.to_str().unwrap(),
    ];
    if confirm {
        args.extend(["--ttl-seconds", "30"]);
    }
    let started = Instant::now();
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(150),
        fixture.command(&args).output(),
    )
    .await
    .expect("scale CLI exceeded its process budget")
    .unwrap();
    fixture.assert_redacted(&output);
    eprintln!(
        "CLI resume={resume} confirm={confirm} scan_pages={pages} wall={:?} status={}",
        started.elapsed(),
        output.status
    );
    output
}

pub fn incomplete(output: &Output, checkpoint: &Path) -> serde_json::Value {
    let report = cli::report(output);
    assert_eq!(report["outcome"], "incomplete");
    assert_eq!(report["artifact_exported"], false);
    assert_eq!(report["confirmation_committed"], false);
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["effects"]["commit_unknown"], false);
    assert_eq!(report["source_rpc"], "not_performed");
    assert_eq!(report["provider_rpc"], "not_performed");
    assert!(report.get("summary").is_none());
    assert!(report.get("items").is_none());
    assert!(report.get("interrupted").is_none());
    assert_eq!(
        Path::new(report["checkpoint"].as_str().unwrap()),
        checkpoint.canonicalize().unwrap()
    );
    report
}

pub async fn task_work(fixture: &cli::Fixture, checkpoint: &Path) -> (TaskWork, u64) {
    let mut sum = TaskWork {
        rows: 0,
        bytes: 0,
        pages: 0,
    };
    let mut export_steps = 0;
    for index in 0..2 {
        let spool = InventorySpool::resume(&checkpoint.join(format!("source-{index:03}.sqlite")))
            .await
            .unwrap();
        let work = spool.task_work().await.unwrap();
        let phases = spool.phase_work().await.unwrap();
        export_steps += phases
            .iter()
            .filter(|phase| phase.phase == "export")
            .map(|phase| phase.pages)
            .sum::<u64>();
        eprintln!(
            "source={index} work={work:?} queries={:?} phases={:?}",
            spool.query_work().await.unwrap(),
            phases
        );
        sum.rows += work.rows;
        sum.bytes += work.bytes;
        sum.pages += work.pages;
        spool.close().await.unwrap();
    }
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth =
        ipfs_s3_gateway::store::pinning::management::records::authorize_local(&raw, &fixture.db)
            .await
            .unwrap();
    let local = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let task = PlanningCheckpoint::resume(
        checkpoint,
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &local,
        &cli_limits(),
    )
    .await
    .unwrap();
    let total = task.total_work().await.unwrap();
    task.close().await.unwrap();
    let union = TaskWork {
        rows: total.rows.checked_sub(sum.rows).unwrap(),
        bytes: total.bytes.checked_sub(sum.bytes).unwrap(),
        pages: total.pages.checked_sub(sum.pages).unwrap(),
    };
    eprintln!("cumulative_task_work={total:?} union_work={union:?}");
    (total, export_steps)
}

pub fn cli_limits() -> RunLimits {
    let hard = v3::files::stream::limits();
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.total_rows = hard.rows;
    limits.total_bytes = hard.bytes;
    limits.elapsed = hard.elapsed;
    limits
}

pub fn advanced(before: TaskWork, after: TaskWork) {
    assert!(
        after.rows >= before.rows && after.bytes >= before.bytes && after.pages >= before.pages
    );
    assert!(
        after.rows > before.rows || after.bytes > before.bytes,
        "no cumulative task work advanced"
    );
    let limits = cli_limits();
    assert!(after.rows <= limits.total_rows && after.bytes <= limits.total_bytes);
    // TaskWork.pages includes accounting operations, not unique accepted pages.
}

pub async fn revisions(db: &DatabaseConnection) -> BTreeMap<String, i64> {
    db.query_all(Statement::from_string(
        db.get_database_backend(),
        "SELECT source_table,revision FROM pin_management_revision ORDER BY source_table",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "source_table").unwrap(),
            row.try_get("", "revision").unwrap(),
        )
    })
    .collect()
}

pub async fn confirmation_rows(db: &DatabaseConnection, plan_id: &str) -> Vec<native::NativeRow> {
    let table = native::registry()
        .into_iter()
        .find(|t| t.table == "pin_management_records")
        .unwrap();
    let columns = table
        .columns
        .iter()
        .map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        format!("SELECT {columns} FROM pin_management_records WHERE plan_id=$1 ORDER BY item_id,kind"),
        [plan_id.into()])).await.unwrap();
    assert!(
        rows.len() <= 2,
        "only the two original confirmation rows are expected"
    );
    rows.into_iter()
        .map(|row| {
            table
                .columns
                .iter()
                .map(|column| {
                    (
                        column.clone(),
                        row.try_get::<Option<String>>("", column).unwrap(),
                    )
                })
                .collect()
        })
        .collect()
}

pub struct SourceKeys {
    path: std::path::PathBuf,
    counts: BTreeMap<String, u64>,
}

impl SourceKeys {
    /// Capture SQL identities once with a database stream, keeping only one key
    /// resident. This is an expectation file, never an authority or a DB clone.
    pub async fn capture(db: &DatabaseConnection, path: &Path) -> Self {
        let mut output = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap(),
        );
        let mut counts = BTreeMap::new();
        for table in native::registry() {
            let count = db
                .query_one(Statement::from_string(
                    db.get_database_backend(),
                    format!("SELECT COUNT(*) AS n FROM \"{}\"", table.table),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "n")
                .unwrap() as u64;
            let columns = table
                .primary_key
                .iter()
                .map(|c| format!("CAST(\"{c}\" AS TEXT) AS \"{c}\""))
                .collect::<Vec<_>>()
                .join(",");
            let order = table
                .primary_key
                .iter()
                .map(|c| format!("CAST(\"{c}\" AS TEXT) COLLATE BINARY"))
                .collect::<Vec<_>>()
                .join(",");
            let mut rows = db
                .stream(Statement::from_string(
                    db.get_database_backend(),
                    format!("SELECT {columns} FROM \"{}\" ORDER BY {order}", table.table),
                ))
                .await
                .unwrap();
            let mut streamed = 0;
            while let Some(row) = rows.try_next().await.unwrap() {
                let key = NativeKey {
                    table: table.table.clone(),
                    values: table
                        .primary_key
                        .iter()
                        .map(|c| row.try_get::<String>("", c).unwrap())
                        .collect(),
                };
                serde_json::to_writer(&mut output, &key).unwrap();
                output.write_all(b"\n").unwrap();
                streamed += 1;
            }
            assert_eq!(streamed, count);
            counts.insert(table.table, count);
        }
        output.flush().unwrap();
        Self {
            path: path.into(),
            counts,
        }
    }

    pub fn verify(&self, path: &Path, fixture: &cli::Fixture) -> FileClaims {
        let started = Instant::now();
        let mut native_keys = BufReader::new(File::open(&self.path).unwrap());
        let mut member_keys = BufReader::new(File::open(&self.path).unwrap());
        let mut reader = Reader::new(
            File::open(path).unwrap(),
            Projection::Review,
            v3::files::stream::budget(),
        )
        .unwrap();
        reader.enable_buffering().unwrap();
        let mut native_counts = BTreeMap::<String, u64>::new();
        let mut member_counts = BTreeMap::<String, u64>::new();
        let mut eof = Vec::new();
        let mut inventories = BTreeMap::<String, BTreeSet<String>>::new();
        let mut membership_ids = BTreeSet::new();
        let mut claims = FileClaims::default();
        let mut row: Option<(Section, NativeKey)> = None;
        let mut column = String::new();
        let mut private_cell = false;
        let mut redacted_metadata = false;
        while let Some(event) = reader.next_frame().unwrap() {
            match event {
                Event::Frame(Frame::Header {
                    created_at,
                    expires_at,
                    ..
                }) => {
                    claims.created_at = created_at;
                    claims.expires_at = expires_at;
                }
                Event::Frame(Frame::Request { request }) => {
                    assert!(request.as_ref() == Some(&fixture.request))
                }
                Event::Frame(Frame::Acknowledgements { item_ids }) => claims.acks = item_ids,
                Event::Frame(Frame::Item { entry }) => claims.items.push(*entry),
                Event::Frame(Frame::Row { section, key, .. }) => {
                    match section {
                        Section::Native => {
                            assert_eq!(next_key(&mut native_keys).as_ref(), Some(&key));
                            *native_counts.entry(key.table.clone()).or_default() += 1;
                        }
                        Section::Membership => {
                            assert_eq!(key.table, "retirement_membership");
                            assert!(key.values.len() >= 3);
                            membership_ids.insert(key.values[0].clone());
                            let table = &key.values[1];
                            if key.values[2] == "eof" {
                                assert_eq!(key.values.len(), 3);
                                assert!(!eof.contains(table));
                                eof.push(table.clone());
                            } else {
                                assert_eq!(key.values[2], "member");
                                assert_eq!(
                                    next_key(&mut member_keys),
                                    Some(NativeKey {
                                        table: table.clone(),
                                        values: key.values[3..].to_vec(),
                                    })
                                );
                                *member_counts.entry(table.clone()).or_default() += 1;
                            }
                        }
                        Section::Inventory if key.table == "retirement_inventory" => {
                            assert_eq!(key.values.len(), 1);
                            assert!(
                                inventories
                                    .insert(key.values[0].clone(), BTreeSet::new())
                                    .is_none()
                            );
                        }
                        _ => {}
                    }
                    row = Some((section, key));
                }
                Event::Frame(Frame::Cell { column: next }) => {
                    column = next;
                    private_cell = row.as_ref().is_some_and(|(section, key)| {
                        *section != Section::Native
                            || (key.table == "objects"
                                && key.values == [fixture.identity.object_id.clone()]
                                && column == "metadata")
                    });
                }
                Event::Chunk(_) => {
                    assert!(!private_cell, "Review must not expose private cell chunks");
                }
                Event::Frame(Frame::CellEnd { value }) => {
                    if private_cell {
                        assert!(matches!(value, CellValue::Redacted | CellValue::Null));
                    }
                    if let Some((section, key)) = &row {
                        if *section == Section::Native
                            && key.table == "objects"
                            && key.values == [fixture.identity.object_id.clone()]
                            && column == "metadata"
                        {
                            assert!(matches!(value, CellValue::Redacted));
                            redacted_metadata = true;
                        }
                        if *section == Section::Inventory
                            && key.table == "retirement_inventory"
                            && matches!(column.as_str(), "reference" | "membership_reference")
                        {
                            assert!(matches!(value, CellValue::Redacted));
                            assert!(
                                inventories
                                    .get_mut(&key.values[0])
                                    .unwrap()
                                    .insert(column.clone())
                            );
                        }
                    }
                }
                Event::Frame(Frame::RowEnd) => row = None,
                _ => {}
            }
        }
        assert!(reader.verified());
        assert!(next_key(&mut native_keys).is_none());
        assert!(next_key(&mut member_keys).is_none());
        for (table, count) in &self.counts {
            assert_eq!(
                native_counts.get(table).copied().unwrap_or(0),
                *count,
                "native {table}"
            );
            assert_eq!(
                member_counts.get(table).copied().unwrap_or(0),
                *count,
                "membership {table}"
            );
        }
        assert_eq!(eof, self.counts.keys().cloned().collect::<Vec<_>>());
        assert_eq!(inventories.len(), 2);
        assert_eq!(
            membership_ids.len(),
            1,
            "one shared complete membership, not one per provider"
        );
        for cells in inventories.values() {
            assert_eq!(cells.len(), 2);
        }
        claims.inventory_ids = inventories.into_keys().collect();
        claims.membership_ids = membership_ids;
        assert!(redacted_metadata);
        assert_eq!(claims.items.len(), 2);
        let (_, budget) = reader.into_verified().unwrap();
        eprintln!(
            "verified Review path={} wall={:?} file_bytes={} reader_work={:?} native={} members={}",
            path.display(),
            started.elapsed(),
            std::fs::metadata(path).unwrap().len(),
            budget.work,
            native_counts.values().sum::<u64>(),
            member_counts.values().sum::<u64>()
        );
        claims
    }
}

/// Private references are checked only at the existing typed original-loader
/// boundary; Review exposes their row identities but never their payloads.
pub fn verify_inventory_references<S: Read + Seek>(
    cursor: &mut v3::archive_cursor::ReviewCursor<S>,
    entries: &[PlanEntry],
) -> BTreeMap<String, String> {
    let mut inventories = BTreeMap::<String, BTreeMap<String, RetirementInventoryRef>>::new();
    let mut membership_ids = BTreeSet::new();
    let mut row = None;
    let mut column = String::new();
    let mut payload = Vec::new();
    while let Some(event) = cursor.next_event().unwrap() {
        match event {
            Event::Frame(Frame::Row { section, key, .. }) => {
                if section == Section::Membership {
                    assert_eq!(key.table, "retirement_membership");
                    assert!(key.values.len() >= 3);
                    membership_ids.insert(key.values[0].clone());
                }
                row = if section == Section::Inventory && key.table == "retirement_inventory" {
                    assert_eq!(key.values.len(), 1);
                    let id = key.values[0].clone();
                    assert!(inventories.insert(id.clone(), BTreeMap::new()).is_none());
                    Some(id)
                } else {
                    None
                };
            }
            Event::Frame(Frame::Cell { column: next }) => {
                column = next;
                payload.clear();
            }
            Event::Chunk(bytes)
                if row.is_some()
                    && matches!(column.as_str(), "reference" | "membership_reference") =>
            {
                assert!(payload.len() + bytes.len() <= v3::FRAME_BYTES);
                payload.extend_from_slice(&bytes);
            }
            Event::Frame(Frame::CellEnd { value })
                if row.is_some()
                    && matches!(column.as_str(), "reference" | "membership_reference") =>
            {
                assert!(matches!(value, CellValue::Text));
                let reference = serde_json::from_slice(&payload).unwrap();
                assert!(
                    inventories
                        .get_mut(row.as_ref().unwrap())
                        .unwrap()
                        .insert(column.clone(), reference)
                        .is_none()
                );
            }
            Event::Frame(Frame::RowEnd) => row = None,
            _ => {}
        }
    }
    assert!(cursor.verified());
    assert_eq!(inventories.len(), 2);
    assert_eq!(membership_ids.len(), 1);
    let captures = entries
        .iter()
        .map(|entry| {
            let Some(Capture::Provider(capture)) = &entry.capture else {
                panic!("original inventory must bind a provider capture");
            };
            (capture.inventory.inventory_id.clone(), &capture.inventory)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(captures.len(), 2);
    let mut relationships = BTreeMap::new();
    let mut shared_membership = None;
    for (id, cells) in inventories {
        assert_eq!(cells.len(), 2);
        let reference = &cells["reference"];
        let membership = &cells["membership_reference"];
        assert_eq!(reference.inventory_id, id);
        assert!(captures.get(&id).copied() == Some(reference));
        assert!(membership_ids.contains(&membership.inventory_id));
        if let Some(shared) = &shared_membership {
            assert!(shared == membership);
        } else {
            shared_membership = Some(membership.clone());
        }
        relationships.insert(id, membership.inventory_id.clone());
    }
    relationships
}

fn next_key(input: &mut impl BufRead) -> Option<NativeKey> {
    let mut line = String::new();
    if input.read_line(&mut line).unwrap() == 0 {
        None
    } else {
        assert!(line.len() <= v3::FRAME_BYTES);
        Some(serde_json::from_str(&line).unwrap())
    }
}

#[derive(Default)]
pub struct FileClaims {
    pub created_at: String,
    pub expires_at: Option<String>,
    pub acks: Option<Vec<String>>,
    pub items: Vec<v3::ItemFact>,
    pub inventory_ids: BTreeSet<String>,
    pub membership_ids: BTreeSet<String>,
}

pub fn same_file_semantics(first: &Path, second: &Path) {
    let mut first = Reader::new(
        File::open(first).unwrap(),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    let mut second = Reader::new(
        File::open(second).unwrap(),
        Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    first.enable_buffering().unwrap();
    second.enable_buffering().unwrap();
    same_semantics(&mut first, &mut second);
}

fn same_semantics<L: Read, R: Read>(first: &mut Reader<L>, second: &mut Reader<R>) {
    let mut left = first.next_frame().unwrap();
    let mut right = second.next_frame().unwrap();
    let (mut left_offset, mut right_offset) = (0, 0);
    loop {
        match (&left, &right) {
            (Some(Event::Chunk(a)), Some(Event::Chunk(b))) => {
                let n = (a.len() - left_offset).min(b.len() - right_offset);
                assert_eq!(
                    &a[left_offset..left_offset + n],
                    &b[right_offset..right_offset + n]
                );
                left_offset += n;
                right_offset += n;
                if left_offset == a.len() {
                    left_offset = 0;
                    left = first.next_frame().unwrap();
                }
                if right_offset == b.len() {
                    right_offset = 0;
                    right = second.next_frame().unwrap();
                }
            }
            (Some(Event::Frame(a)), Some(Event::Frame(b))) => {
                assert!(
                    a == b,
                    "archive control facts or ordered cell/row values differ"
                );
                left = first.next_frame().unwrap();
                right = second.next_frame().unwrap();
            }
            (None, None) => {
                assert!(first.verified() && second.verified());
                break;
            }
            _ => panic!("archive semantic event sequences differ"),
        }
    }
}

#[cfg(test)]
mod semantic_tests {
    use super::*;

    fn archive(compact: bool, text: &[u8], created: &str) -> Vec<u8> {
        let mut writer =
            v3::Writer::new(Vec::new(), Projection::Review, v3::files::stream::budget()).unwrap();
        for frame in [
            Frame::Header {
                schema_version: v3::SCHEMA_VERSION,
                projection: Projection::Review,
                database_scope: "scope".into(),
                database_identity: "identity".into(),
                principal: "principal".into(),
                applied_trust: v3::AppliedTrust::UntrustedClaimsOnly,
                catalog: vec!["catalog".into()],
                created_at: created.into(),
                expires_at: None,
            },
            Frame::Request { request: None },
            Frame::Configuration {
                configuration: None,
            },
            Frame::Acknowledgements { item_ids: None },
            Frame::Summary {
                summary: Default::default(),
            },
            Frame::Row {
                section: Section::Native,
                key: NativeKey {
                    table: "objects".into(),
                    values: vec!["a".into()],
                },
                cells: 3,
            },
        ] {
            writer.frame(&frame).unwrap();
        }
        if compact {
            writer.complete_cell("cid", text, CellValue::Text).unwrap();
        } else {
            writer
                .frame(&Frame::Cell {
                    column: "cid".into(),
                })
                .unwrap();
            for byte in text.chunks(1) {
                writer.cell_output().write_all(byte).unwrap();
            }
            writer
                .frame(&Frame::CellEnd {
                    value: CellValue::Text,
                })
                .unwrap();
        }
        writer
            .complete_cell("metadata", b"secret", CellValue::Text)
            .unwrap();
        writer.complete_cell("size", b"", CellValue::Null).unwrap();
        writer.frame(&Frame::RowEnd).unwrap();
        writer.finish().unwrap()
    }
    fn compare(left: &[u8], right: &[u8]) {
        let mut left = Reader::new(left, Projection::Review, v3::files::stream::budget()).unwrap();
        let mut right =
            Reader::new(right, Projection::Review, v3::files::stream::budget()).unwrap();
        same_semantics(&mut left, &mut right);
    }
    #[test]
    fn complete_content_is_equal_despite_physical_chunk_and_frame_differences() {
        compare(
            &archive(true, "原文-é\0尾".as_bytes(), "original"),
            &archive(false, "原文-é\0尾".as_bytes(), "original"),
        );
    }
    #[test]
    #[should_panic]
    fn changed_public_value_is_not_semantically_equal() {
        compare(
            &archive(true, b"original", "time"),
            &archive(false, b"changed!", "time"),
        );
    }
    #[test]
    #[should_panic]
    fn changed_control_fact_is_not_semantically_equal() {
        compare(
            &archive(true, b"original", "time"),
            &archive(false, b"original", "different"),
        );
    }
}
