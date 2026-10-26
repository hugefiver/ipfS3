use super::*;
use std::io::{Read, Seek, SeekFrom};

async fn original() -> (
    support::Fixture,
    records::ConfirmedBinding,
    segmented::ArchiveRef,
    segmented::ArchiveHeader,
    Vec<u8>,
) {
    let (fixture, binding, _, _) = committed_without_output().await;
    let entry = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .unwrap();
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
    drop(cursor);
    let mut source = Vec::new();
    let header = segmented::read_archive(
        &fixture.db,
        &reference,
        &mut source,
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    (fixture, binding, reference, header, source)
}

async fn install_segmented(
    id: &str,
    header: &segmented::ArchiveHeader,
    source: &[u8],
    damage: Option<&str>,
) -> DatabaseConnection {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
    let tx = db.begin().await.unwrap();
    // Alternate header whitespace is part of the transport digest.
    let raw = serde_json::to_string_pretty(header).unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES($1,'S75A1',$2,$3,$4,'staging')",
        [id.into(), header.database_scope.clone().into(), header.database_identity.clone().into(), raw.clone().into()],
    )).await.unwrap();
    let mut digest = Sha256::new_with_prefix(b"S75A1\0");
    digest.update((raw.len() as u64).to_be_bytes());
    digest.update(raw.as_bytes());
    let mut chunks = 0i64;
    let chunk_size = if damage == Some("boundary_fraction") {
        source.len().div_ceil(17).min(native::meter::CELL_SLICE)
    } else {
        native::meter::CELL_SLICE
    };
    for (ordinal, chunk) in source.chunks(chunk_size).enumerate() {
        digest.update((ordinal as u64).to_be_bytes());
        digest.update((chunk.len() as u64).to_be_bytes());
        digest.update(chunk);
        chunks += 1;
        if damage == Some("missing") && ordinal == 0 {
            continue;
        }
        let payload = if damage == Some("payload") && ordinal == 0 {
            Vec::new()
        } else if damage == Some("digest") && ordinal == 0 {
            let mut changed = chunk.to_vec();
            changed[0] ^= 1;
            changed
        } else {
            chunk.to_vec()
        };
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES($1,$2,$3)",
            [id.into(), (ordinal as i64).into(), payload.into()],
        ))
        .await
        .unwrap();
    }
    if damage == Some("fraction") {
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES($1,0.5,$2)",
            [id.into(), vec![b'x'].into()],
        ))
        .await
        .unwrap();
    }
    if damage == Some("boundary_fraction") {
        assert!(chunks >= 17);
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES($1,15.5,$2)",
            [id.into(), vec![b'x'].into()],
        ))
        .await
        .unwrap();
    }
    if damage == Some("extra") {
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO pin_management_archive_chunks VALUES($1,$2,$3)",
            [id.into(), chunks.into(), vec![b'x'].into()],
        ))
        .await
        .unwrap();
    }
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_management_archive SET state='sealed',chunks=$1,bytes=$2,digest=$3 WHERE archive_id=$4",
        [chunks.into(), (source.len() as i64).into(), hex::encode(digest.finalize()).into(), id.into()],
    )).await.unwrap();
    tx.commit().await.unwrap();
    db
}

#[tokio::test]
async fn original_inventory_loader_rejects_fractional_extra_and_damaged_storage_membership() {
    let (_fixture, binding, reference, header, source) = original().await;
    let entry = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .unwrap();
    for damage in [
        None,
        Some("fraction"),
        Some("boundary_fraction"),
        Some("missing"),
        Some("extra"),
        Some("digest"),
        Some("payload"),
    ] {
        let db = install_segmented(&reference.archive_id, &header, &source, damage).await;
        let result = records::load_original_inventory(
            &db,
            &binding,
            entry,
            tempfile::tempfile().unwrap(),
            v3::files::stream::budget(),
        )
        .await;
        if damage.is_some() {
            assert!(
                matches!(result, Err(ManagementError::InvalidEvidence)),
                "{damage:?}"
            );
        } else {
            let mut cursor = result.unwrap();
            while cursor.next_event().unwrap().is_some() {}
            let (mut file, _) = cursor.into_verified().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut actual = Vec::new();
            file.read_to_end(&mut actual).unwrap();
            assert_eq!(actual, source);
        }
    }
}

#[tokio::test]
async fn original_inventory_loader_preserves_identical_inline_and_segmented_body_bytes() {
    let (_fixture, binding, reference, header, source) = original().await;
    let entry = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| matches!(entry.capture, Some(Capture::Provider(_))))
        .unwrap();
    let mut reader = v3::Reader::new(
        source.as_slice(),
        Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )
    .unwrap();
    let mut writer = Writer::new(
        Vec::new(),
        Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )
    .unwrap();
    loop {
        let Some(v3::Event::Frame(frame)) = reader.next_frame().unwrap() else {
            panic!("control frame");
        };
        writer.frame(&frame).unwrap();
        if matches!(frame, v3::Frame::Summary { .. }) {
            break;
        }
    }
    let body = writer.finish().unwrap();
    assert!(body.len() <= native::meter::CELL_SLICE);
    let segmented = install_segmented(&reference.archive_id, &header, &body, None).await;
    let inline = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&inline)
        .await
        .unwrap();
    let tx = inline.begin().await.unwrap();
    let inline_ref = segmented::write_archive(
        &tx,
        &reference.archive_id,
        &header,
        &mut body.as_slice(),
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    assert_eq!(inline_ref.chunks, 0);
    tx.commit().await.unwrap();
    for db in [&inline, &segmented] {
        let mut cursor = records::load_original_inventory(
            db,
            &binding,
            entry,
            tempfile::tempfile().unwrap(),
            v3::files::stream::budget(),
        )
        .await
        .unwrap();
        while cursor.next_event().unwrap().is_some() {}
        let (mut file, _) = cursor.into_verified().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, body);
    }
}
