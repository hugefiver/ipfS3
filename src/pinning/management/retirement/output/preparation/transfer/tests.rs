use super::*;

async fn fixture() -> (DatabaseConnection, segmented::ArchiveRef) {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    for sql in [
        "CREATE TABLE pin_management_archive(archive_id TEXT PRIMARY KEY,format TEXT,state TEXT,header TEXT,chunks BIGINT,bytes BIGINT,digest TEXT)",
        "CREATE TABLE pin_management_archive_chunks(archive_id TEXT,ordinal BIGINT,payload BLOB,PRIMARY KEY(archive_id,ordinal)) WITHOUT ROWID",
    ] {
        spool::exec(&db, sql, vec![]).await.unwrap();
    }
    let mut hash = Sha256::new_with_prefix(b"S75A1\0");
    hash.update(6u64.to_be_bytes());
    hash.update(b"header");
    for ordinal in 0..17i64 {
        let chunk = vec![ordinal as u8; v3::CELL_BYTES];
        hash.update((ordinal as u64).to_be_bytes());
        hash.update((chunk.len() as u64).to_be_bytes());
        hash.update(&chunk);
        spool::exec(
            &db,
            "INSERT INTO pin_management_archive_chunks VALUES('output',$1,$2)",
            vec![ordinal.into(), chunk.into()],
        )
        .await
        .unwrap();
    }
    let reference = segmented::ArchiveRef {
        archive_id: "output".into(),
        chunks: 17,
        bytes: (17 * v3::CELL_BYTES) as i64,
        digest: hex::encode(hash.finalize()),
    };
    spool::exec(
        &db,
        "INSERT INTO pin_management_archive VALUES('output','S75A1','sealed','header',$1,$2,$3)",
        vec![
            reference.chunks.into(),
            reference.bytes.into(),
            reference.digest.clone().into(),
        ],
    )
    .await
    .unwrap();
    (db, reference)
}

#[tokio::test]
async fn complete_transport_spans_bounded_batches_and_retains_payment() {
    let (db, reference) = fixture().await;
    let mut budget = v3::files::stream::budget();
    let mut output = Vec::new();
    copy(&db, &reference, "header", &mut output, &mut budget)
        .await
        .unwrap();
    assert_eq!(output.len(), reference.bytes as usize);
    for (ordinal, chunk) in output.chunks(v3::CELL_BYTES).enumerate() {
        assert!(chunk.iter().all(|&byte| byte == ordinal as u8));
    }
    assert!(budget.work.sql < reference.chunks as u64);
    assert_eq!(budget.work.rows, 18);
    assert_eq!(budget.work.raw_bytes, reference.bytes as u64 + 6);
    assert_eq!(budget.work.output_bytes, reference.bytes as u64);
    assert!(budget.peak_resident() <= 2 * v3::FRAME_BYTES);
}

#[tokio::test]
async fn oversized_text_payload_is_not_returned_by_the_bounded_query() {
    let (db, reference) = fixture().await;
    let text = "界".repeat(v3::CELL_BYTES / 2 + 1);
    spool::exec(
        &db,
        "UPDATE pin_management_archive_chunks SET payload=$1 WHERE ordinal=0",
        vec![text.into()],
    )
    .await
    .unwrap();
    let rows = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            BATCH_SQL,
            [reference.archive_id.clone().into(), 0i64.into(), 16i64.into()],
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 16);
    assert_eq!(rows[0].try_get::<Option<String>>("", "payload").unwrap(), None);
    let mut budget = v3::files::stream::budget();
    assert!(
        copy(&db, &reference, "header", &mut std::io::sink(), &mut budget)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn changed_or_incomplete_transport_never_succeeds() {
    for sql in [
        "UPDATE pin_management_archive SET header='changed'",
        "DELETE FROM pin_management_archive_chunks WHERE ordinal=15",
        "INSERT INTO pin_management_archive_chunks VALUES('output',17,X'01')",
        "UPDATE pin_management_archive_chunks SET payload=zeroblob(65537) WHERE ordinal=15",
        "UPDATE pin_management_archive_chunks SET payload=X'' WHERE ordinal=15",
        "UPDATE pin_management_archive_chunks SET payload=zeroblob(65536) WHERE ordinal=16",
        "UPDATE pin_management_archive_chunks SET payload=X'01' WHERE ordinal=16",
    ] {
        let (db, reference) = fixture().await;
        spool::exec(&db, sql, vec![]).await.unwrap();
        let mut budget = v3::files::stream::budget();
        assert!(
            copy(&db, &reference, "header", &mut std::io::sink(), &mut budget)
                .await
                .is_err(),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn output_failure_and_small_allowance_are_not_successful_transfers() {
    let (db, reference) = fixture().await;
    let mut budget = v3::files::stream::budget();
    let mut empty = [];
    let mut output = std::io::Cursor::new(empty.as_mut_slice());
    assert!(
        copy(&db, &reference, "header", &mut output, &mut budget)
            .await
            .is_err()
    );
    assert_eq!(budget.work.output_bytes, reference.bytes as u64);
    let mut budget = Budget::default();
    let _resident = budget.reserve(8 * v3::FRAME_BYTES - 1).unwrap();
    assert!(
        copy(&db, &reference, "header", &mut std::io::sink(), &mut budget)
            .await
            .is_err()
    );
}
