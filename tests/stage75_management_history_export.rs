use ipfs_s3_gateway::store::{self, pinning::management::schema};
use ipfs_s3_gateway::{
    pinning::management::{
        NativeKey,
        wire::v3::{
            self, files,
            history::{self, Export, Progress},
        },
    },
    store::{
        initialization,
        pinning::management::{
            native::{self, meter::Budget},
            records::segmented::{self, ArchiveHeader, ArchiveRef},
        },
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};
use std::{
    io::{Read, Seek, SeekFrom, Write},
    sync::atomic::Ordering,
};
use tokio_util::sync::CancellationToken;

#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod fixture;

fn budget() -> Budget {
    files::stream::budget()
}
fn ceilings() -> native::meter::Limits {
    files::stream::limits()
}
async fn open_task(path: &std::path::Path) -> DatabaseConnection {
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

fn clean_suspended_task(path: &std::path::Path) {
    // Exact artifacts reserved by this test's Export, never a recursive cleanup.
    let parent = path.parent().unwrap().to_owned();
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(parent).unwrap();
}

#[test]
fn durable_budget_cannot_reset_work_deadline_or_operator_ceiling() {
    let mut original = Budget::default();
    original.charge(2, 3, 4, 5).unwrap();
    let checkpoint = original.checkpoint().unwrap();
    let mut restored = Budget::resume(checkpoint.clone(), Default::default()).unwrap();
    assert_eq!(restored.work.sql, 2);
    assert_eq!(restored.work.rows, 3);
    restored.charge(1, 1, 1, 1).unwrap();
    assert_eq!(restored.work.raw_bytes, 6);
    let mut expired = serde_json::to_value(&checkpoint).unwrap();
    expired["deadline_millis"] = expired["recorded_at_millis"].clone();
    assert!(Budget::resume(serde_json::from_value(expired).unwrap(), Default::default()).is_err());
    let smaller = native::meter::Limits {
        sql: 1,
        ..Default::default()
    };
    assert!(Budget::resume(checkpoint, smaller).is_err());
    original.cancellation().store(true, Ordering::Relaxed);
    assert!(original.checkpoint().is_err());
}

async fn complete(db: &DatabaseConnection) -> Export {
    let mut export = Export::start(db, "history operator", budget(), CancellationToken::new())
        .await
        .unwrap();
    let mut previous = export.budget().unwrap().work.raw_bytes;
    let mut pending = 0;
    while export
        .advance(db, 1, CancellationToken::new())
        .await
        .unwrap()
        == Progress::Pending
    {
        assert!(export.budget().unwrap().work.raw_bytes >= previous);
        previous = export.budget().unwrap().work.raw_bytes;
        pending += 1;
    }
    assert!(pending > 0);
    export
}

fn key(scope: &str, plan: &str) -> NativeKey {
    NativeKey {
        table: schema::RECORDS.into(),
        values: vec![scope.into(), plan.into(), "item".into(), "action".into()],
    }
}

async fn foreign(db: &DatabaseConnection, plan: &str, before: &str) {
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_management_records VALUES('foreign-scope',$1,'item','action','foreign-principal','foreign-db',' raw binding ',$2,NULL,'[]','[]','stopped','2020-01-01 01:02:03.123456789','2020-01-02 01:02:03.000000001',NULL)",
        vec![plan.into(),before.into()])).await.unwrap();
}

async fn remove_storage(db: &DatabaseConnection) {
    let backend = db.get_database_backend();
    for (name, _, _, _) in schema::storage::triggers(backend) {
        db.execute_unprepared(&format!("DROP TRIGGER {name}"))
            .await
            .unwrap();
    }
    for name in ["s75_applied_no_delete", "s75_applied_no_update"] {
        db.execute_unprepared(&format!("DROP TRIGGER {name}"))
            .await
            .unwrap();
    }
    for (name, _) in schema::storage::indexes() {
        db.execute_unprepared(&format!("DROP INDEX {name}"))
            .await
            .unwrap();
    }
    for (name, _) in schema::storage::tables(backend).into_iter().rev() {
        db.execute_unprepared(&format!("DROP TABLE {name}"))
            .await
            .unwrap();
    }
    db.execute(Statement::from_sql_and_values(
        backend,
        "DELETE FROM seaql_migrations WHERE version=$1",
        [schema::VERSION.into()],
    ))
    .await
    .unwrap();
}

async fn revision(db: &impl ConnectionTrait, table: &str) -> i64 {
    db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT revision FROM pin_management_revision WHERE source_table=$1",
        [table.into()],
    ))
    .await
    .unwrap()
    .expect("export collection has a revision")
    .try_get("", "revision")
    .unwrap()
}

async fn database() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    db
}

#[tokio::test]
async fn archive_collection_revisions_are_transactional_and_maintenance_is_validated() {
    let db = database().await;
    for table in ["pin_management_archive", "pin_management_archive_chunks"] {
        assert_eq!(revision(&db, table).await, 0);
    }
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("INSERT INTO pin_management_archive VALUES('test','S75A1','scope','identity','{}','staging',0,0,NULL)").await.unwrap();
    tx.execute_unprepared("INSERT INTO pin_management_archive_chunks VALUES('test',0,x'00ff')")
        .await
        .unwrap();
    assert_eq!(revision(&tx, "pin_management_archive").await, 1);
    assert_eq!(revision(&tx, "pin_management_archive_chunks").await, 1);
    tx.execute_unprepared(
        "UPDATE pin_management_archive SET header=header WHERE archive_id='test'",
    )
    .await
    .unwrap();
    assert_eq!(revision(&tx, "pin_management_archive").await, 2);
    tx.execute_unprepared(
        "UPDATE pin_management_archive SET state='sealed' WHERE archive_id='test'",
    )
    .await
    .unwrap();
    assert_eq!(revision(&tx, "pin_management_archive").await, 3);
    assert!(
        tx.execute_unprepared(
            "UPDATE pin_management_archive SET header='replacement' WHERE archive_id='test'"
        )
        .await
        .is_err()
    );
    assert!(
        tx.execute_unprepared("INSERT INTO pin_management_archive_chunks VALUES('test',1,x'00')")
            .await
            .is_err()
    );
    assert_eq!(revision(&tx, "pin_management_archive").await, 3);
    assert_eq!(revision(&tx, "pin_management_archive_chunks").await, 1);
    tx.rollback().await.unwrap();
    assert_eq!(revision(&db, "pin_management_archive").await, 0);
    assert_eq!(revision(&db, "pin_management_archive_chunks").await, 0);
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DROP TRIGGER s75_export_pin_management_archive_insert")
        .await
        .unwrap();
    assert!(schema::validate(&tx).await.is_err());
    tx.rollback().await.unwrap();
    schema::validate(&db).await.unwrap();
}

#[tokio::test]
async fn mixed_db_history_restores_original_cells_and_archive_parts_without_target_writes() {
    let fixture = fixture::Fixture::new().await;
    let object = fixture.publish("history-native", false).await;
    let request = fixture.create_request("native-committed", object, false);
    let binding = fixture.confirm(&request).await;
    let applied = ipfs_s3_gateway::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    )
    .await
    .unwrap();
    foreign(&fixture.db, "converted", "  foreign 原文\0 \n").await;
    foreign(&fixture.db, "converted-empty", "").await;
    remove_storage(&fixture.db).await;
    initialization::upgrade_existing(
        &fixture.db,
        initialization::UpgradeRequest {
            database_scope: fixture.auth.database_scope(),
            native_identity: fixture.auth.database_identity(),
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    let index = initialization::history::recover_source(
        &fixture.db,
        &key("foreign-scope", "converted"),
        &mut budget(),
    )
    .await
    .unwrap()
    .unwrap();
    let native_key = NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            fixture.auth.database_scope().into(),
            request.plan_id.clone(),
            request.items[0].item_id.clone(),
            "action".into(),
        ],
    };
    let native_index =
        initialization::history::recover_source(&fixture.db, &native_key, &mut budget())
            .await
            .unwrap()
            .unwrap();
    fixture.publish("fresh-native-current", false).await;
    foreign(&fixture.db, "current", " current 私密\0data ").await;
    let long = "界\0raw\n".repeat(40_000);
    foreign(&fixture.db, "long", &long).await;
    for domain in ["foreign-retirement-1", "foreign-retirement-2"] {
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "INSERT INTO pin_management_retirements VALUES($1,'foreign-scope','current','item','foreign-principal','foreign-db','binding',' original inventory 私密 ','retain','[]','retired','2020-02-01 01:02:03.123456789','action')",[domain.into()])).await.unwrap();
    }
    // Retain an unfinished archive as data, not a sealed or executable receipt.
    fixture.db.execute_unprepared("INSERT INTO pin_management_archive VALUES('unfinished','S75A1','scope','identity','{}','staging',0,0,NULL)").await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.v3");
    let path = path.to_str().unwrap();
    let mut export = complete(&fixture.db).await;
    export.write_private(path).await.unwrap();
    let mut reader = v3::Reader::new(
        std::fs::File::open(path).unwrap(),
        v3::Projection::PrivateRecovery,
        budget(),
    )
    .unwrap();
    while reader.next_frame().unwrap().is_some() {}
    assert!(reader.verified());
    assert!(export.write_private(path).await.is_err());
    // A separate live DB is deliberately never supplied to the restore API.
    let other = database().await;
    let other_scope = native::scope::binding(&other).await.unwrap();
    let source_before = native::scope::binding(&fixture.db).await.unwrap();
    let mut recovered = history::restore_private(path, budget()).await.unwrap();
    assert!(recovered.head.request.is_none());
    assert_eq!(recovered.head.database_identity, source_before.1);
    let mut bytes = Vec::new();
    recovered
        .cell(&key("foreign-scope", "long"), "native_before", &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, long.as_bytes());
    bytes.clear();
    assert!(
        recovered
            .cell(&key("foreign-scope", "current"), "result", &mut bytes)
            .await
            .unwrap()
            .is_none()
    );
    assert!(bytes.is_empty());
    assert_eq!(
        recovered
            .cell(
                &key("foreign-scope", "converted-empty"),
                "native_before",
                &mut bytes
            )
            .await
            .unwrap(),
        Some(0)
    );
    let original: native::NativeRow = recovered.read_part(&index.original).await.unwrap();
    let restored_index = recovered
        .converted_source(&index.source, index.source_generation)
        .await
        .unwrap();
    assert!(restored_index.original == index.original);
    assert_eq!(
        original["native_before"].as_deref(),
        Some("  foreign 原文\0 \n")
    );
    assert!(original["result"].is_none());
    assert_eq!(
        original["expires_at"].as_deref(),
        Some("2020-01-02 01:02:03.000000001")
    );
    // Check every original archive body, including normal review/membership/
    // result archives and conversion parts, at a bounded raw-byte seam.
    let references=fixture.db.query_all(Statement::from_string(fixture.db.get_database_backend(),"SELECT archive_id,digest,bytes,chunks FROM pin_management_archive WHERE state='sealed' ORDER BY archive_id")).await.unwrap();
    let mut reviews = 0;
    for (number, row) in references.into_iter().enumerate() {
        let reference = ArchiveRef {
            archive_id: row.try_get("", "archive_id").unwrap(),
            digest: row.try_get("", "digest").unwrap(),
            bytes: row.try_get("", "bytes").unwrap(),
            chunks: row.try_get("", "chunks").unwrap(),
        };
        let original_path = dir.path().join(format!("original-{number}.bin"));
        let mut original_file = files::reserve(original_path.to_str().unwrap())
            .await
            .unwrap()
            .into_std()
            .await;
        let expected_header =
            segmented::read_archive(&fixture.db, &reference, &mut original_file, &mut budget())
                .await
                .unwrap();
        original_file.seek(SeekFrom::Start(0)).unwrap();
        struct Compare<'a>(&'a mut std::fs::File);
        impl Write for Compare<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                assert!(bytes.len() <= v3::CELL_BYTES);
                let mut original = [0; v3::CELL_BYTES];
                self.0.read_exact(&mut original[..bytes.len()])?;
                assert_eq!(&original[..bytes.len()], bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let restored_header = recovered
            .read_archive(&reference, &mut Compare(&mut original_file))
            .await
            .unwrap();
        assert!(restored_header == expected_header);
        if let Ok(original_request) = serde_json::from_str::<
            ipfs_s3_gateway::pinning::management::PlanRequest,
        >(&restored_header.request)
            && original_request.plan_id == request.plan_id
        {
            assert!(restored_header.expires_at.is_some());
            reviews += 1;
        }
        assert_eq!(original_file.read(&mut [0; 1]).unwrap(), 0);
        original_file.seek(SeekFrom::Start(0)).unwrap();
        let mut magic = [0; v3::MAGIC.len()];
        if original_file.read(&mut magic).unwrap() == magic.len() && magic == *v3::MAGIC {
            original_file.seek(SeekFrom::Start(0)).unwrap();
            let mut review =
                v3::Reader::new(original_file, v3::Projection::PrivateRecovery, budget()).unwrap();
            while let Some(event) = review.next_frame().unwrap() {
                if let v3::Event::Frame(v3::Frame::Request {
                    request: Some(request),
                }) = event
                    && request.plan_id == "native-committed"
                {
                    reviews += 1;
                }
            }
            assert!(review.verified());
        }
    }
    assert!(reviews > 0, "the real frozen native review was recovered");
    let restored_native = recovered
        .converted_source(&native_key, native_index.source_generation)
        .await
        .unwrap();
    let review = restored_native.review.as_ref().unwrap();
    let original_review = native_index.review.as_ref().unwrap();
    assert_eq!(review.staged_at, original_review.staged_at);
    assert_eq!(review.expires_at, original_review.expires_at);
    assert_eq!(review.confirmed_at, original_review.confirmed_at);
    assert_eq!(review.acknowledgements, original_review.acknowledgements);
    let restored_request: ipfs_s3_gateway::pinning::management::PlanRequest =
        recovered.read_part(&review.request).await.unwrap();
    assert!(restored_request == request);
    let restored_result: ipfs_s3_gateway::store::pinning::management::records::CommittedAction =
        recovered
            .read_part(restored_native.committed.as_ref().unwrap())
            .await
            .unwrap();
    assert!(restored_result == applied.items[0]);
    for dependency in &review.rows {
        let expected: native::NativeRow =
            initialization::history::read_part(&fixture.db, &dependency.original, &mut budget())
                .await
                .unwrap();
        let actual: native::NativeRow = recovered.read_part(&dependency.original).await.unwrap();
        assert_eq!(actual, expected);
    }
    let original_current = fixture
        .db
        .query_all(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT * FROM pin_management_records ORDER BY database_scope,plan_id,item_id,kind",
        ))
        .await
        .unwrap();
    for row in original_current {
        let key = NativeKey {
            table: schema::RECORDS.into(),
            values: ["database_scope", "plan_id", "item_id", "kind"]
                .map(|c| row.try_get("", c).unwrap())
                .to_vec(),
        };
        for column in &native::registry()
            .into_iter()
            .find(|t| t.table == schema::RECORDS)
            .unwrap()
            .columns
        {
            let expected: Option<String> = row.try_get("", column).unwrap();
            bytes.clear();
            let value = recovered.cell(&key, column, &mut bytes).await.unwrap();
            assert_eq!(value.is_some(), expected.is_some());
            assert_eq!(bytes, expected.unwrap_or_default().as_bytes());
        }
    }
    for domain in ["foreign-retirement-1", "foreign-retirement-2"] {
        bytes.clear();
        recovered
            .cell(
                &NativeKey {
                    table: schema::RETIREMENTS.into(),
                    values: vec![domain.into()],
                },
                "inventory",
                &mut bytes,
            )
            .await
            .unwrap();
        assert_eq!(bytes, b" original inventory \xe7\xa7\x81\xe5\xaf\x86 ");
    }
    assert!(recovered.archive("unfinished").await.is_err());
    let replay = ipfs_s3_gateway::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    )
    .await
    .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
    assert_eq!(native::scope::binding(&other).await.unwrap(), other_scope);
    assert_eq!(
        native::scope::binding(&fixture.db).await.unwrap(),
        source_before
    );
    let restored_path = dir.path().join("restored-private.v3");
    let before = recovered.budget().work.raw_bytes;
    let work = recovered
        .export_private(restored_path.to_str().unwrap())
        .await
        .unwrap();
    assert!(work.work.raw_bytes > before);
    let mut recreated = v3::Reader::new(
        std::fs::File::open(&restored_path).unwrap(),
        v3::Projection::PrivateRecovery,
        work,
    )
    .unwrap();
    while recreated.next_frame().unwrap().is_some() {}
    assert!(recreated.verified());
    export.close().await.unwrap();
}

#[tokio::test]
async fn history_collection_drift_cancellation_and_truncation_never_publish_a_recovery() {
    let db = database().await;
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("history.v3");
    let output = output.to_str().unwrap();
    let mut export = Export::start(&db, "operator", budget(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        export
            .advance(&db, 1, CancellationToken::new())
            .await
            .unwrap(),
        Progress::Pending
    );
    db.execute_unprepared("INSERT INTO pin_management_archive VALUES('new-behind-cursor','S75A1','scope','identity','{}','staging',0,0,NULL)").await.unwrap();
    assert!(
        export
            .advance(&db, 1, CancellationToken::new())
            .await
            .is_err()
    );
    assert!(export.write_private(output).await.is_err());
    export.close().await.unwrap();
    let b = budget();
    let cancel = b.cancellation();
    let mut export = Export::start(&db, "operator", b, CancellationToken::new())
        .await
        .unwrap();
    cancel.store(true, Ordering::Relaxed);
    assert!(
        export
            .advance(&db, 1, CancellationToken::new())
            .await
            .is_err()
    );
    assert!(export.write_private(output).await.is_err());
    export.close().await.unwrap();
    let mut export = complete(&db).await;
    export.write_private(output).await.unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(output)
        .unwrap();
    let n = file.metadata().unwrap().len();
    file.set_len(n - 1).unwrap();
    file.flush().unwrap();
    drop(file);
    assert!(history::restore_private(output, budget()).await.is_err());
    let task = export.suspend().await.unwrap();
    let spool = open_task(&task).await;
    spool
        .execute_unprepared("DELETE FROM sealed_wire WHERE ordinal=0")
        .await
        .unwrap();
    spool.close().await.unwrap();
    let mut resumed = Export::resume(&task, ceilings()).await.unwrap();
    let invalid = dir.path().join("corrupt-cache.v3");
    assert!(
        resumed
            .write_private(invalid.to_str().unwrap())
            .await
            .is_err()
    );
    assert!(
        history::restore_private(invalid.to_str().unwrap(), budget())
            .await
            .is_err()
    );
    resumed.close().await.unwrap();
    clean_suspended_task(&task);
}

#[tokio::test]
async fn full_history_streams_more_than_old_row_and_byte_limits_with_one_budget() {
    let db = database().await;
    let header = ArchiveHeader {
        database_scope: "original".into(),
        database_identity: "original-db".into(),
        principal: "original-operator".into(),
        catalog: vec!["original-catalog".into()],
        request: "null".into(),
        configuration: "null".into(),
        acknowledgements: vec![],
        created_at: "original clock".into(),
        expires_at: Some("original TTL".into()),
    };
    // Generate the multi-page archive without retaining its contents in memory.
    struct Chunks {
        remaining: usize,
    }
    impl Read for Chunks {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Ok(0);
            }
            let n = buf.len().min(1100);
            buf[..n].fill(0xff);
            self.remaining -= 1;
            Ok(n)
        }
    }
    let tx = db.begin().await.unwrap();
    let reference = segmented::write_archive(
        &tx,
        "unreferenced-history",
        &header,
        &mut Chunks { remaining: 4097 },
        &mut budget(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.v3");
    let path = path.to_str().unwrap();
    let mut export = complete(&db).await;
    assert!(export.budget().unwrap().work.rows > 4096);
    assert!(export.budget().unwrap().work.raw_bytes > 4 * 1024 * 1024);
    let before = export.budget().unwrap().work.raw_bytes;
    export.write_private(path).await.unwrap();
    assert!(export.budget().unwrap().work.raw_bytes > before);
    assert!(export.budget().unwrap().peak_resident() <= 32 * 1024 * 1024);
    let mut recovered = history::restore_private(path, budget()).await.unwrap();
    struct Sink(u64);
    impl Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert!(bytes.iter().all(|b| *b == 0xff));
            assert!(bytes.len() <= v3::CELL_BYTES);
            self.0 += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct PartialOutput {
        remaining: usize,
        written: u64,
        zero: bool,
    }
    impl Write for PartialOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 && !self.zero {
                return Err(std::io::Error::other("output unavailable"));
            }
            let n = self.remaining.min(bytes.len());
            self.remaining -= n;
            self.written += n as u64;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut lookup_output = None;
    for (remaining, zero) in [(0, false), (7, false), (7, true)] {
        let mut output = PartialOutput {
            remaining,
            written: 0,
            zero,
        };
        let before = recovered.budget().work.output_bytes;
        assert!(
            recovered
                .read_archive(&reference, &mut output)
                .await
                .is_err()
        );
        assert_eq!(output.written, remaining as u64);
        let billed = recovered.budget().work.output_bytes - before;
        let lookup = *lookup_output.get_or_insert(billed);
        assert_eq!(billed, lookup + output.written);
    }
    let mut sink = Sink(0);
    assert!(recovered.read_archive(&reference, &mut sink).await.unwrap() == header);
    assert_eq!(sink.0, 4097 * 1100);
    assert!(recovered.budget().peak_resident() <= 32 * 1024 * 1024);
    recovered.close().await.unwrap();
    export.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_continues_in_another_process_and_sealed_export_ignores_new_db_history() {
    const TASK: &str = "IPFS_S3_HISTORY_TEST_TASK";
    const DATABASE: &str = "IPFS_S3_HISTORY_TEST_DATABASE";
    const OUTPUT: &str = "IPFS_S3_HISTORY_TEST_OUTPUT";
    if let Some(task) = std::env::var_os(TASK) {
        let db = Database::connect(std::env::var(DATABASE).unwrap())
            .await
            .unwrap();
        let path = std::path::PathBuf::from(task);
        let mut export = Export::resume(&path, ceilings()).await.unwrap();
        let before = export.budget().unwrap().work.raw_bytes;
        while export
            .advance(&db, 1, CancellationToken::new())
            .await
            .unwrap()
            == Progress::Pending
        {}
        assert!(export.budget().unwrap().work.raw_bytes > before);
        export
            .write_private(&std::env::var(OUTPUT).unwrap())
            .await
            .unwrap();
        assert_eq!(export.suspend().await.unwrap(), path);
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("source.sqlite");
    let url = format!(
        "sqlite://{}?mode=rwc",
        database_path.to_string_lossy().replace('\\', "/")
    );
    let db = Database::connect(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    foreign(&db, "before-restart", &"原\0文".repeat(40_000)).await;
    let mut export = Export::start(
        &db,
        "checkpoint operator",
        budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        export
            .advance(&db, 1, CancellationToken::new())
            .await
            .unwrap(),
        Progress::Pending
    );
    let before = export.budget().unwrap().work.raw_bytes;
    let task = export.suspend().await.unwrap();
    let mut low = ceilings();
    low.bytes = 1;
    assert!(Export::resume(&task, low).await.is_err());
    let output = directory.path().join("child-private.v3");
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "checkpoint_continues_in_another_process_and_sealed_export_ignores_new_db_history",
            "--nocapture",
        ])
        .env(TASK, &task)
        .env(DATABASE, &url)
        .env(OUTPUT, &output)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "child failed: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    foreign(&db, "after-seal", "not in the old archive").await;
    let mut resumed = Export::resume(&task, ceilings()).await.unwrap();
    assert!(resumed.budget().unwrap().work.raw_bytes > before);
    let restored = directory.path().join("recreated-private.v3");
    resumed
        .write_private(restored.to_str().unwrap())
        .await
        .unwrap();
    resumed.close().await.unwrap();
    let mut recovery = history::restore_private(restored.to_str().unwrap(), budget())
        .await
        .unwrap();
    assert!(
        recovery
            .cell(
                &key("foreign-scope", "after-seal"),
                "native_before",
                &mut std::io::sink()
            )
            .await
            .is_err()
    );
    assert_eq!(
        recovery
            .cell(
                &key("foreign-scope", "before-restart"),
                "native_before",
                &mut std::io::sink()
            )
            .await
            .unwrap(),
        Some(("原\0文".len() * 40_000) as u64)
    );
    recovery.close().await.unwrap();
    clean_suspended_task(&task);
}

#[tokio::test]
async fn reopened_pending_spool_is_compared_with_original_cells_and_rejects_corruption() {
    let db = database().await;
    foreign(&db, "original", "原始 text").await;
    let mut export = Export::start(&db, "operator", budget(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        export
            .advance(&db, 1, CancellationToken::new())
            .await
            .unwrap(),
        Progress::Pending
    );
    let task = export.suspend().await.unwrap();
    let spool = open_task(&task).await;
    spool.execute_unprepared("UPDATE parts SET payload=x'66' WHERE table_name='pin_management_records' AND column_name='native_before' AND offset=0").await.unwrap();
    spool.close().await.unwrap();
    let mut resumed = Export::resume(&task, ceilings()).await.unwrap();
    let result = loop {
        match resumed.advance(&db, 1, CancellationToken::new()).await {
            Ok(Progress::Pending) => {}
            other => break other,
        }
    };
    assert!(result.is_err());
    resumed.close().await.unwrap();
    assert!(Export::resume(&task, ceilings()).await.is_err());
    clean_suspended_task(&task);
}
