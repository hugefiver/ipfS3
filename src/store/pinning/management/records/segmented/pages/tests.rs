use super::*;
use sea_orm::{Database, DatabaseConnection};

async fn fixture() -> (DatabaseConnection, ArchiveRef) {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_archive(archive_id TEXT PRIMARY KEY,header TEXT,format TEXT,state TEXT,chunks INTEGER,bytes INTEGER,digest TEXT)").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_archive_chunks(archive_id TEXT,ordinal INTEGER,payload BLOB,PRIMARY KEY(archive_id,ordinal))").await.unwrap();
    db.execute_unprepared("CREATE TABLE pin_management_inline_archive(archive_id TEXT PRIMARY KEY,header TEXT,format TEXT,bytes INTEGER,digest TEXT,payload BLOB)").await.unwrap();
    let header = ArchiveHeader {
        database_scope: "scope".into(),
        database_identity: "identity".into(),
        principal: "principal".into(),
        catalog: vec!["catalog".into()],
        request: "request".into(),
        configuration: "configuration".into(),
        acknowledgements: vec![],
        created_at: "original".into(),
        expires_at: None,
    };
    let raw = serde_json::to_string(&header).unwrap();
    let mut digest = Sha256::new_with_prefix(b"S75A1\0");
    digest.update((raw.len() as u64).to_be_bytes());
    digest.update(raw.as_bytes());
    for ordinal in 0u64..17 {
        let payload = vec![ordinal as u8; 2];
        digest.update(ordinal.to_be_bytes());
        digest.update(2u64.to_be_bytes());
        digest.update(&payload);
        db.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES('archive',$1,$2)",
            [(ordinal as i64).into(), payload.into()],
        ))
        .await
        .unwrap();
    }
    let reference = ArchiveRef {
        archive_id: "archive".into(),
        chunks: 17,
        bytes: 34,
        digest: hex::encode(digest.finalize()),
    };
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_management_archive VALUES('archive',$1,'S75A1','sealed',17,34,$2)",
        [raw.into(), reference.digest.clone().into()],
    ))
    .await
    .unwrap();
    (db, reference)
}

#[tokio::test]
async fn bounded_pages_verify_count_order_digest_and_extra_only_at_storage_eof() {
    for mutation in [
        None,
        Some("DELETE FROM pin_management_archive_chunks WHERE ordinal=3"),
        Some("UPDATE pin_management_archive_chunks SET payload=x'ffff' WHERE ordinal=3"),
        Some("UPDATE pin_management_archive_chunks SET ordinal=-1 WHERE ordinal=3"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',17,x'ff')"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',0.5,x'ff')"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',15.5,x'ff')"),
        Some("UPDATE pin_management_archive_chunks SET ordinal=0.5 WHERE ordinal=0"),
        Some("UPDATE pin_management_archive_chunks SET payload=x'0000' WHERE ordinal=1"),
    ] {
        let (db, reference) = fixture().await;
        if let Some(sql) = mutation {
            db.execute_unprepared(sql).await.unwrap();
        }
        let mut budget = Budget::default();
        let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
        let initial = budget.work;
        let mut chunks = 0;
        let result = loop {
            match pages.next(&db, &mut budget).await {
                Ok(Some(page)) => {
                    assert!(page.chunks.len() <= 16);
                    chunks += page.chunks.len();
                }
                other => break other,
            }
        };
        if mutation.is_none() {
            assert!(matches!(result, Ok(None)));
            assert_eq!(chunks, 17);
            assert_eq!(
                budget.work.raw_bytes - initial.raw_bytes,
                reference.bytes as u64
            );
            assert_eq!(budget.work.rows - initial.rows, 2 * reference.chunks as u64);
        } else {
            assert!(matches!(result, Err(ManagementError::InvalidEvidence)));
        }
        assert!(budget.work.sql > 0 && budget.work.rows > 0);
    }
}

#[tokio::test]
async fn raw_archive_ranges_reject_fractional_missing_extra_digest_and_payload_damage() {
    for mutation in [
        None,
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',0.5,x'ff')"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',15.5,x'ff')"),
        Some("UPDATE pin_management_archive_chunks SET ordinal=0.5 WHERE ordinal=0"),
        Some("DELETE FROM pin_management_archive_chunks WHERE ordinal=3"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',17,x'ff')"),
        Some("INSERT INTO pin_management_archive_chunks VALUES('archive',-1,x'ff')"),
        Some("UPDATE pin_management_archive_chunks SET payload=x'0000' WHERE ordinal=1"),
        Some("UPDATE pin_management_archive_chunks SET payload=zeroblob(65537) WHERE ordinal=0"),
        Some("UPDATE pin_management_archive_chunks SET payload=x'' WHERE ordinal=0"),
    ] {
        let (db, reference) = fixture().await;
        if let Some(sql) = mutation {
            db.execute_unprepared(sql).await.unwrap();
        }
        let mut budget = Budget::default();
        let mut output = Vec::new();
        let result = read_archive_raw(&db, &reference, &mut output, &mut budget).await;
        if mutation.is_none() {
            assert!(result.is_ok());
            assert_eq!(output, (0u8..17).flat_map(|n| [n; 2]).collect::<Vec<_>>());
            assert_eq!(budget.work.output_bytes, 34);
            assert_eq!(budget.work.sql, 7);
            assert!(budget.peak_resident() < 3 * PAGE_BYTES);
            assert_eq!(budget.work.rows, 2 + 2 * reference.chunks as u64);
        } else {
            assert!(matches!(result, Err(ManagementError::InvalidEvidence)));
        }
        assert!(budget.work.raw_bytes > 0);
        assert!(budget.work.sql > 0 && budget.work.rows > 0);
    }
}

#[tokio::test]
async fn below_limit_archive_roundtrips_once_with_bounded_resident_memory() {
    use sea_orm::TransactionTrait;

    struct Output(usize);
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert!(bytes.len() <= CELL_SLICE);
            assert!(bytes.iter().all(|byte| *byte == b'x'));
            self.0 += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let (db, _) = fixture().await;
    db.execute_unprepared("ALTER TABLE pin_management_archive ADD COLUMN database_scope TEXT; ALTER TABLE pin_management_archive ADD COLUMN native_identity TEXT")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT header FROM pin_management_archive WHERE archive_id='archive'",
        ))
        .await
        .unwrap()
        .unwrap();
    let header: ArchiveHeader =
        serde_json::from_str(&row.try_get::<String>("", "header").unwrap()).unwrap();
    let bytes = 15 * PAGE_BYTES + 1;
    let mut input = std::io::repeat(b'x').take(bytes as u64);
    let tx = db.begin().await.unwrap();
    let reference = write_archive(&tx, "large", &header, &mut input, &mut Budget::default())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut budget = Budget::new(native::meter::Limits {
        resident: 3 * PAGE_BYTES,
        ..Default::default()
    });
    let mut output = Output(0);
    let raw = read_archive_raw(&db, &reference, &mut output, &mut budget)
        .await
        .unwrap();
    assert_eq!(output.0, bytes);
    assert!(serde_json::from_str::<ArchiveHeader>(&raw).unwrap() == header);
    let reads = (reference.chunks as u64).div_ceil((PAGE_BYTES / CELL_SLICE) as u64);
    assert_eq!(budget.work.sql, 2 + 2 * reads + 1);
    assert_eq!(budget.work.raw_bytes, raw.len() as u64 + bytes as u64);
    assert!(budget.work.raw_bytes < native::meter::Limits::default().bytes);
    assert!(budget.peak_resident() < bytes);
}

#[tokio::test]
async fn short_final_page_fits_the_remaining_byte_allowance() {
    let (db, reference) = fixture().await;
    let mut budget = Budget::default();
    let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
    budget
        .restrict_remaining(
            100,
            reference.bytes as u64,
            std::time::Duration::from_secs(30),
        )
        .unwrap();
    let before = budget.work;
    let mut bytes = 0;
    while let Some(page) = pages.next(&db, &mut budget).await.unwrap() {
        bytes += page.chunks.iter().map(Vec::len).sum::<usize>();
    }
    assert_eq!(bytes, reference.bytes as usize);
    assert_eq!(
        budget.work.raw_bytes - before.raw_bytes,
        reference.bytes as u64
    );
    assert_eq!(budget.work.rows - before.rows, 2 * reference.chunks as u64);
    assert!(budget.check().is_ok());
}

#[tokio::test]
async fn page_failures_keep_charges_and_cancellation_is_not_a_new_allowance() {
    let (db, reference) = fixture().await;
    let mut budget = Budget::default();
    let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
    let prior = budget.work;
    budget
        .restrict_remaining(100, 1, std::time::Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        pages.next(&db, &mut budget).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(budget.work.raw_bytes - prior.raw_bytes, 16 * 2);
    assert!(budget.work.sql > prior.sql);
    assert!(budget.check().is_err());

    let mut budget = Budget::default();
    let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
    let before = budget.work;
    budget
        .cancellation()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(matches!(
        pages.next(&db, &mut budget).await,
        Err(ManagementError::Budget)
    ));
    assert_eq!(budget.work.sql, before.sql);
    assert_eq!(budget.work.raw_bytes, before.raw_bytes);
}

#[tokio::test]
async fn invalid_page_metadata_retains_its_allowance_without_fetching_payload() {
    for mutation in [
        "DELETE FROM pin_management_archive_chunks WHERE ordinal=3",
        "UPDATE pin_management_archive_chunks SET payload=X'' WHERE ordinal=0",
        "UPDATE pin_management_archive_chunks SET payload=X'' WHERE ordinal=15",
    ] {
        let (db, reference) = fixture().await;
        db.execute_unprepared(mutation).await.unwrap();
        let mut budget = Budget::default();
        let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
        let before = budget.work;
        assert!(matches!(
            pages.next(&db, &mut budget).await,
            Err(ManagementError::InvalidEvidence)
        ));
        assert_eq!(budget.work.raw_bytes, before.raw_bytes);
        assert_eq!(
            budget.work.rows - before.rows,
            (PAGE_BYTES / CELL_SLICE) as u64 + 1
        );
    }
}

#[tokio::test]
async fn an_extra_malformed_ordinal_cannot_make_the_last_query_underpaid() {
    let (db, reference) = fixture().await;
    // The lookahead is scalar metadata, not an extra materialized payload.
    db.execute_unprepared(
        "INSERT INTO pin_management_archive_chunks VALUES('archive',16.5,zeroblob(65536))",
    )
    .await
    .unwrap();
    let mut budget = Budget::default();
    let (_header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
    drop(pages.next(&db, &mut budget).await.unwrap().unwrap());
    let before = budget.work;
    assert!(matches!(
        pages.next(&db, &mut budget).await,
        Err(ManagementError::InvalidEvidence)
    ));
    assert_eq!(budget.work.raw_bytes, before.raw_bytes);
    assert_eq!(budget.work.rows - before.rows, 2);
}

async fn header_fixture(raw: &str, inline: bool) -> (DatabaseConnection, ArchiveRef) {
    let (db, mut reference) = fixture().await;
    if inline {
        db.execute_unprepared(
            "DELETE FROM pin_management_archive_chunks; DELETE FROM pin_management_archive",
        )
        .await
        .unwrap();
        let payload = vec![1u8];
        reference.chunks = 0;
        reference.bytes = 1;
        reference.digest = super::super::inline::digest(raw, &payload);
        db.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO pin_management_inline_archive VALUES('archive',$1,'S75I2',1,$2,$3)",
            [raw.into(), reference.digest.clone().into(), payload.into()],
        ))
        .await
        .unwrap();
    } else {
        let mut digest = Sha256::new_with_prefix(b"S75A1\0");
        digest.update((raw.len() as u64).to_be_bytes());
        digest.update(raw.as_bytes());
        for ordinal in 0u64..17 {
            digest.update(ordinal.to_be_bytes());
            digest.update(2u64.to_be_bytes());
            digest.update([ordinal as u8; 2]);
        }
        reference.digest = hex::encode(digest.finalize());
        db.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE pin_management_archive SET header=$1,digest=$2 WHERE archive_id='archive'",
            [raw.into(), reference.digest.clone().into()],
        ))
        .await
        .unwrap();
    }
    (db, reference)
}

#[tokio::test]
async fn compact_header_collections_cannot_allocate_past_the_resident_budget() {
    let array = format!("[{}\"\"]", "\"\",".repeat(199_999));
    for inline in [false, true] {
        for field in ["catalog", "acknowledgements"] {
            let (catalog, acknowledgements) = if field == "catalog" {
                (array.as_str(), "[]")
            } else {
                ("[]", array.as_str())
            };
            // Request is deliberately the wrong type, after the large vector.
            // Resource rejection must precede the ordinary typed decode error.
            let raw = format!(
                r#"{{"database_scope":"scope","database_identity":"identity","principal":"principal","catalog":{catalog},"acknowledgements":{acknowledgements},"request":false,"configuration":"none","created_at":"original","expires_at":null}}"#
            );
            assert!(raw.len() < PAGE_BYTES);
            let (db, reference) = header_fixture(&raw, inline).await;
            let mut budget = Budget::new(native::meter::Limits {
                resident: 6 * PAGE_BYTES,
                ..Default::default()
            });
            assert!(matches!(
                Pages::open(&db, &reference, &mut budget).await,
                Err(ManagementError::Budget)
            ));
            assert!(budget.check().is_err());
        }
    }
}

#[tokio::test]
async fn large_single_string_headers_keep_their_values_and_release_owned_memory() {
    let expected = ArchiveHeader {
        database_scope: "scope".into(),
        database_identity: "identity".into(),
        principal: "principal".into(),
        catalog: vec!["catalog".into()],
        request: "x".repeat(PAGE_BYTES / 2),
        configuration: "configuration".into(),
        acknowledgements: vec!["ack".into()],
        created_at: "original".into(),
        expires_at: Some("expiry".into()),
    };
    let object = serde_json::to_string(&expected).unwrap();
    let positional = serde_json::to_string(&(
        &expected.database_scope,
        &expected.database_identity,
        &expected.principal,
        &expected.catalog,
        &expected.request,
        &expected.configuration,
        &expected.acknowledgements,
        &expected.created_at,
        &expected.expires_at,
    ))
    .unwrap();
    for raw in [object, positional] {
        for inline in [false, true] {
            let (db, reference) = header_fixture(&raw, inline).await;
            let limit = 6 * PAGE_BYTES;
            let mut budget = Budget::new(native::meter::Limits {
                resident: limit,
                ..Default::default()
            });
            let (header, mut pages) = Pages::open(&db, &reference, &mut budget).await.unwrap();
            assert!(header.value == expected);
            while pages.next(&db, &mut budget).await.unwrap().is_some() {}
            drop(pages);
            assert!(header.value == expected);
            drop(header);
            let _released = budget.reserve(limit).unwrap();
        }
    }
}
