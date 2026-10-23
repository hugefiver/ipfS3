use ipfs_s3_gateway::{
    pinning::management::{
        NativeKey,
        wire::v3::{
            archive_cursor::{self, ReviewCursor},
            *,
        },
    },
    store::pinning::management::{
        native::meter::{Budget, Limits},
        records::segmented::{self, ArchiveHeader, ArchiveRef},
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, TransactionTrait};
use std::{
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
    time::Duration,
};

use ipfs_s3_gateway::pinning::management::wire::v3::files;

fn budget() -> Budget {
    Budget::new(Limits {
        bytes: 512 * 1024 * 1024,
        rows: 100_000,
        sql: 100_000,
        resident: 16 * 1024 * 1024,
        elapsed: Duration::from_secs(120),
    })
}
fn header() -> ArchiveHeader {
    ArchiveHeader {
        database_scope: "original-foreign-scope".into(),
        database_identity: "original-physical-db".into(),
        principal: "original-operator".into(),
        catalog: vec!["original-catalog".into()],
        request: "null".into(),
        configuration: "null".into(),
        acknowledgements: vec![],
        created_at: "2026-01-01 01:02:03.000000123".into(),
        expires_at: Some("original expiry".into()),
    }
}
fn key(id: &str) -> NativeKey {
    NativeKey {
        table: "objects".into(),
        values: vec![id.into()],
    }
}
fn bytes(pieces: usize) -> (Vec<u8>, Budget) {
    let mut writer = Writer::new(Vec::new(), Projection::PrivateRecovery, budget()).unwrap();
    emit(&mut writer, pieces);
    writer.finish_with_budget().unwrap()
}
fn emit<W: Write>(writer: &mut Writer<W>, pieces: usize) {
    let h = header();
    writer
        .frame(&Frame::Header {
            schema_version: SCHEMA_VERSION,
            projection: Projection::PrivateRecovery,
            database_scope: h.database_scope,
            database_identity: h.database_identity,
            principal: h.principal,
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: h.catalog,
            created_at: h.created_at,
            expires_at: h.expires_at,
        })
        .unwrap();
    writer.frame(&Frame::Request { request: None }).unwrap();
    writer
        .frame(&Frame::Configuration {
            configuration: None,
        })
        .unwrap();
    writer
        .frame(&Frame::Acknowledgements { item_ids: None })
        .unwrap();
    writer
        .frame(&Frame::Summary {
            summary: Default::default(),
        })
        .unwrap();
    writer
        .frame(&Frame::Row {
            section: Section::Native,
            key: key("a"),
            cells: 3,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "content_type".into(),
        })
        .unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Null,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "metadata".into(),
        })
        .unwrap();
    writer
        .cell_output()
        .write_all(b"  { \"private\" : \"do not export\" }\n")
        .unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer
        .frame(&Frame::Cell {
            column: "size".into(),
        })
        .unwrap();
    for _ in 0..pieces {
        writer.cell_output().write_all(&[b'x'; CELL_BYTES]).unwrap();
    }
    writer
        .cell_output()
        .write_all("界\0original bytes".as_bytes())
        .unwrap();
    writer
        .frame(&Frame::CellEnd {
            value: CellValue::Text,
        })
        .unwrap();
    writer.frame(&Frame::RowEnd).unwrap();
    for section in [
        Section::Membership,
        Section::Inventory,
        Section::Result,
        Section::AppliedClaim,
    ] {
        writer
            .frame(&Frame::Row {
                section,
                key: key("a"),
                cells: 0,
            })
            .unwrap();
        writer.frame(&Frame::RowEnd).unwrap();
    }
}

// Minimal disposable archive storage exercises the existing public byte API,
// without repeating A's schema/trigger/readiness acceptance tests.
async fn db() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_archive(archive_id TEXT PRIMARY KEY,format TEXT,database_scope TEXT,native_identity TEXT,header TEXT,state TEXT,chunks INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,digest TEXT DEFAULT '')").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_archive_chunks(archive_id TEXT,ordinal INTEGER,payload BLOB,PRIMARY KEY(archive_id,ordinal))").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_inline_archive(archive_id TEXT PRIMARY KEY,format TEXT,database_scope TEXT,native_identity TEXT,header TEXT,bytes INTEGER,digest TEXT,payload BLOB)").await.unwrap();
    db
}

struct Spool(Cursor<Vec<u8>>);
impl Spool {
    fn empty() -> Self {
        Self(Cursor::new(Vec::new()))
    }
}
impl Read for Spool {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.0.read(out)
    }
}
impl Seek for Spool {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
impl Write for Spool {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(
            bytes.len() <= CELL_BYTES,
            "archive must reach spool in bounded raw chunks"
        );
        self.0.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn store(db: &DatabaseConnection, id: &str, raw: Vec<u8>) -> ArchiveRef {
    let tx = db.begin().await.unwrap();
    let reference =
        segmented::write_archive(&tx, id, &header(), &mut raw.as_slice(), &mut budget())
            .await
            .unwrap();
    tx.commit().await.unwrap();
    reference
}

#[tokio::test]
async fn small_private_archive_seals_loads_exports_and_checks_representation_and_binding() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
    let (raw, work) = bytes(0);
    assert!(raw.len() < CELL_BYTES);
    let staged =
        archive_cursor::stage_import(Cursor::new(raw.clone()), Projection::PrivateRecovery, work)
            .unwrap();
    let tx = db.begin().await.unwrap();
    let (reference, work) = archive_cursor::seal_private(&tx, "small-v3", &header(), staged)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(reference.chunks, 0);
    let cursor = archive_cursor::load(
        &db,
        &reference,
        Spool::empty(),
        Projection::PrivateRecovery,
        work,
    )
    .await
    .unwrap();
    assert_eq!(
        cursor
            .export(Vec::new(), Projection::PrivateRecovery)
            .unwrap()
            .0,
        raw
    );
    let mut wrong_representation = reference.clone();
    wrong_representation.chunks = 1;
    let mut wrong_length = reference.clone();
    wrong_length.bytes += 1;
    let mut wrong_digest = reference.clone();
    wrong_digest.digest = "0".repeat(64);
    for wrong in [wrong_representation, wrong_length, wrong_digest] {
        assert!(
            archive_cursor::load(
                &db,
                &wrong,
                Spool::empty(),
                Projection::PrivateRecovery,
                budget()
            )
            .await
            .is_err()
        );
    }
    let mut wrong_header = header();
    wrong_header.principal = "not the body principal".into();
    let tx = db.begin().await.unwrap();
    let wrong = segmented::write_archive(
        &tx,
        "wrong-header",
        &wrong_header,
        &mut raw.as_slice(),
        &mut budget(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        archive_cursor::load(
            &db,
            &wrong,
            Spool::empty(),
            Projection::PrivateRecovery,
            budget()
        )
        .await
        .is_err()
    );
    let mut corrupt = raw.clone();
    let at = corrupt
        .windows(13)
        .position(|part| part == b"do not export")
        .unwrap();
    corrupt[at] = b'x';
    let wrong = store(&db, "wrong-body", corrupt).await;
    assert_eq!(wrong.chunks, 0);
    assert!(
        archive_cursor::load(
            &db,
            &wrong,
            Spool::empty(),
            Projection::PrivateRecovery,
            budget()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn private_runtime_bytes_seal_load_and_export_without_snapshot_or_budget_reset() {
    let db = db().await;
    let (original, budget) = bytes(270);
    let staged = archive_cursor::stage_import(
        Cursor::new(original.clone()),
        Projection::PrivateRecovery,
        budget,
    )
    .unwrap();
    assert!(staged.archive_claim().is_none());
    let tx = db.begin().await.unwrap();
    let (reference, budget) = archive_cursor::seal_private(&tx, "runtime-v3", &header(), staged)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = budget.work.raw_bytes;
    let cursor = archive_cursor::load(
        &db,
        &reference,
        Spool::empty(),
        Projection::PrivateRecovery,
        budget,
    )
    .await
    .unwrap();
    assert!(cursor.archive_claim() == Some(&reference));
    assert!(cursor.budget().work.raw_bytes > before + original.len() as u64);
    let (private, budget) = cursor
        .export(Vec::new(), Projection::PrivateRecovery)
        .unwrap();
    assert_eq!(private, original);
    assert!(budget.work.raw_bytes > before + 2 * original.len() as u64);
    let cursor = archive_cursor::load(
        &db,
        &reference,
        Spool::empty(),
        Projection::PrivateRecovery,
        budget,
    )
    .await
    .unwrap();
    let (public, budget) = cursor.export(Vec::new(), Projection::Review).unwrap();
    assert!(!public.windows(13).any(|p| p == b"do not export"));
    let mut reader = Reader::new(Cursor::new(public), Projection::Review, budget).unwrap();
    let mut raw_bytes = 0;
    let mut null = false;
    let mut redacted = false;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Chunk(bytes) => {
                assert!(bytes.len() <= CELL_BYTES);
                raw_bytes += bytes.len();
            }
            Event::Frame(Frame::CellEnd {
                value: CellValue::Null,
            }) => null = true,
            Event::Frame(Frame::CellEnd {
                value: CellValue::Redacted,
            }) => redacted = true,
            _ => {}
        }
    }
    assert!(null && redacted && reader.verified());
    assert_eq!(raw_bytes, 270 * CELL_BYTES + "界\0original bytes".len());
}

#[tokio::test]
async fn valid_storage_digest_does_not_admit_legacy_truncated_or_corrupt_v3() {
    let db = db().await;
    let (raw, _) = bytes(1);
    let mut corrupt = raw.clone();
    let at = corrupt.windows(32).position(|p| p == [b'x'; 32]).unwrap();
    corrupt[at] = b'y';
    for (id, invalid) in [
        ("legacy", br#"{"schema_version":2}"#.to_vec()),
        ("truncated", raw[..raw.len() - 1].to_vec()),
        ("corrupt", corrupt),
    ] {
        let reference = store(&db, id, invalid).await;
        assert!(
            archive_cursor::load(
                &db,
                &reference,
                Spool::empty(),
                Projection::PrivateRecovery,
                budget()
            )
            .await
            .is_err()
        );
    }
}

#[tokio::test]
async fn missing_storage_chunk_and_header_body_mismatch_never_return_a_cursor() {
    let db = db().await;
    let (raw, _) = bytes(2);
    let reference = store(&db, "missing", raw.clone()).await;
    db.execute_unprepared(
        "DELETE FROM pin_management_archive_chunks WHERE archive_id='missing' AND ordinal=1",
    )
    .await
    .unwrap();
    assert!(
        archive_cursor::load(
            &db,
            &reference,
            Spool::empty(),
            Projection::PrivateRecovery,
            budget()
        )
        .await
        .is_err()
    );
    let mut wrong = header();
    wrong.principal = "different local claim".into();
    let staged =
        archive_cursor::stage_import(Cursor::new(raw), Projection::PrivateRecovery, budget())
            .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        archive_cursor::seal_private(&tx, "mismatch", &wrong, staged)
            .await
            .is_err()
    );
    assert!(
        tx.query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT 1 FROM pin_management_archive WHERE archive_id='mismatch'"
        ))
        .await
        .unwrap()
        .is_none()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn reader_mode_is_explicit_and_public_import_cannot_recover_withheld_bytes() {
    let (raw, budget) = bytes(1);
    assert!(
        archive_cursor::stage_import(Cursor::new(raw.clone()), Projection::Review, budget).is_err()
    );
    let staged = archive_cursor::stage_import(
        Cursor::new(raw),
        Projection::PrivateRecovery,
        self::budget(),
    )
    .unwrap();
    let (public, budget) = staged
        .into_cursor()
        .export(Vec::new(), Projection::Review)
        .unwrap();
    let imported =
        archive_cursor::stage_import(Cursor::new(public), Projection::Review, budget).unwrap();
    assert!(
        imported
            .into_cursor()
            .export(Vec::new(), Projection::PrivateRecovery)
            .is_err()
    );
}

#[tokio::test]
async fn load_and_projection_preserve_cumulative_cancellation_and_work_limits() {
    let db = db().await;
    let (raw, _) = bytes(1);
    let reference = store(&db, "budget", raw.clone()).await;
    let mut tiny = Budget::new(Limits {
        bytes: raw.len() as u64 + 2048,
        resident: 16 * 1024 * 1024,
        ..Limits::default()
    });
    tiny.charge(0, 0, 0, raw.len() as u64).unwrap();
    assert!(
        archive_cursor::load(
            &db,
            &reference,
            Spool::empty(),
            Projection::PrivateRecovery,
            tiny
        )
        .await
        .is_err()
    );
    let cursor: ReviewCursor<Spool> = archive_cursor::load(
        &db,
        &reference,
        Spool::empty(),
        Projection::PrivateRecovery,
        budget(),
    )
    .await
    .unwrap();
    cursor
        .budget()
        .cancellation()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(cursor.export(Vec::new(), Projection::Review).is_err());
}

#[tokio::test]
async fn actual_cli_file_adapters_seal_and_export_to_new_protected_files_only() {
    let db = db().await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("private.v3");
    let frozen = directory.path().join("db-spool.v3");
    let output = directory.path().join("review.v3");
    let mut writer = files::stream::writer(
        source.to_str().unwrap(),
        Projection::PrivateRecovery,
        budget(),
    )
    .unwrap();
    emit(&mut writer, 1);
    let budget = files::stream::finish(writer).unwrap();
    let staged = files::stream::stage_temporary(
        source.to_str().unwrap(),
        Projection::PrivateRecovery,
        budget,
    )
    .unwrap();
    let tx = db.begin().await.unwrap();
    let (reference, budget) =
        archive_cursor::seal_private(&tx, "files", &header(), staged.imported().unwrap())
            .await
            .unwrap();
    tx.commit().await.unwrap();
    let cursor = archive_cursor::load(
        &db,
        &reference,
        files::reserve(frozen.to_str().unwrap())
            .await
            .unwrap()
            .into_std()
            .await,
        Projection::PrivateRecovery,
        budget,
    )
    .await
    .unwrap();
    let budget =
        files::stream::export_archive(cursor, output.to_str().unwrap(), Projection::Review)
            .unwrap();
    let public = std::fs::read(&output).unwrap();
    assert!(!public.windows(13).any(|p| p == b"do not export"));
    let mut reader = Reader::new(Cursor::new(public.clone()), Projection::Review, budget).unwrap();
    while reader.next_frame().unwrap().is_some() {}
    let (_, budget) = reader.into_verified().unwrap();
    assert!(budget.work.raw_bytes > public.len() as u64);
    assert!(files::reserve(frozen.to_str().unwrap()).await.is_err());
    let again = directory.path().join("another-spool.v3");
    let cursor = archive_cursor::load(
        &db,
        &reference,
        files::reserve(again.to_str().unwrap())
            .await
            .unwrap()
            .into_std()
            .await,
        Projection::PrivateRecovery,
        self::budget(),
    )
    .await
    .unwrap();
    assert!(
        files::stream::export_archive(cursor, output.to_str().unwrap(), Projection::Review)
            .is_err()
    );
    assert_eq!(std::fs::read(output).unwrap(), public);
    let missing = directory.path().join("missing");
    assert!(
        files::reserve(missing.join("spool.v3").to_str().unwrap())
            .await
            .is_err()
    );
    assert!(!missing.exists());
}

#[tokio::test]
async fn source_changed_after_staging_cannot_publish_a_terminal_output() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };
    struct FaultSpool {
        bytes: Cursor<Vec<u8>>,
        corrupt: Arc<AtomicBool>,
    }
    impl Read for FaultSpool {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = self.bytes.read(out)?;
            if self.corrupt.load(Ordering::Relaxed)
                && let Some(at) = out[..n].windows(16).position(|p| p == [b'x'; 16])
            {
                out[at] = b'y';
            }
            Ok(n)
        }
    }
    impl Seek for FaultSpool {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.bytes.seek(pos)
        }
    }
    impl Write for FaultSpool {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let db = db().await;
    let (raw, _) = bytes(1);
    let reference = store(&db, "changed-source", raw).await;
    let corrupt = Arc::new(AtomicBool::new(false));
    let spool = FaultSpool {
        bytes: Cursor::new(Vec::new()),
        corrupt: corrupt.clone(),
    };
    let cursor = archive_cursor::load(
        &db,
        &reference,
        spool,
        Projection::PrivateRecovery,
        budget(),
    )
    .await
    .unwrap();
    corrupt.store(true, Ordering::Relaxed);
    let partial = Arc::new(Mutex::new(Vec::new()));
    assert!(
        cursor
            .export(Capture(partial.clone()), Projection::Review)
            .is_err()
    );
    let mut reader = Reader::new(
        Cursor::new(partial.lock().unwrap().clone()),
        Projection::Review,
        budget(),
    )
    .unwrap();
    loop {
        match reader.next_frame() {
            Ok(Some(_)) => {}
            Ok(None) => panic!("partial output acquired a valid terminal marker"),
            Err(_) => break,
        }
    }
    assert!(!reader.verified());
}
