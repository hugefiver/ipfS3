use super::*;
use ipfs_s3_gateway::{
    pinning::management::{Capture, SafeError},
    store::pinning::management::records::segmented,
};
use sha2::{Digest, Sha256};

fn records(bytes: &[u8]) -> Vec<(u8, &[u8], usize)> {
    let mut at = v3::MAGIC.len();
    let mut result = Vec::new();
    while at < bytes.len() {
        let length = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        result.push((bytes[at + 4], &bytes[at + 5..at + 4 + length], at));
        at += 4 + length;
    }
    assert_eq!(at, bytes.len());
    result
}

fn append(bytes: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    bytes.extend_from_slice(&(payload.len() as u32 + 1).to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(payload);
}

fn excluded_invalid_cell(original: &[u8]) -> Vec<u8> {
    let physical = records(original);
    let (_, _, trailer) = physical.last().unwrap();
    let mut body = original[..*trailer].to_vec();
    append(
        &mut body,
        0,
        &serde_json::to_vec(&v3::Frame::Row {
            section: v3::Section::Result,
            key: NativeKey {
                table: "excluded_original".into(),
                values: vec!["row".into()],
            },
            cells: 1,
        })
        .unwrap(),
    );
    // Correct kind7 framing, invalid UTF-8 in the excluded cell's text.
    append(&mut body, 7, b"\0\x05\x01value\xff");
    append(&mut body, 6, b"");
    let trailer = v3::Frame::Trailer {
        frames: physical.len() as u64 - 1 + 3,
        bytes: body.len() as u64,
        sha256: hex::encode(Sha256::digest(&body)),
    };
    append(&mut body, 0, &serde_json::to_vec(&trailer).unwrap());
    body
}

async fn private_output(
    db: &DatabaseConnection,
    binding: &records::ConfirmedBinding,
) -> Result<Vec<u8>, SafeError> {
    let mut writer = Writer::new(
        Vec::new(),
        Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )?;
    v3::planning::confirmed(db, &mut writer, binding, Projection::PrivateRecovery).await?;
    writer.finish()
}

#[tokio::test]
async fn confirmed_copy_sections_preserves_short_records_and_validates_excluded_sections() {
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
    let short_source = records(&source)
        .into_iter()
        .filter(|(kind, _, _)| *kind == 7)
        .map(|(_, payload, _)| payload.to_vec())
        .collect::<Vec<_>>();
    assert!(
        !short_source.is_empty(),
        "the real native export must produce kind7"
    );
    assert!(records(&source).iter().any(|(kind, _, _)| *kind == 2));
    let mut tight = v3::files::stream::limits();
    // Two codec states, the read cache, the document, and one codec's peak
    // workspace fit. The live decoded event and output workspace must overlap.
    tight.resident = 7 * v3::FRAME_BYTES
        + v3::CELL_BYTES
        + 8 * (header.request.len() + header.configuration.len());
    let mut single_reader = v3::Reader::new(
        source.as_slice(),
        Projection::PrivateRecovery,
        native::meter::Budget::new(tight),
    )
    .unwrap();
    single_reader.enable_buffering().unwrap();
    while single_reader.next_frame().unwrap().is_some() {}
    assert!(single_reader.verified());
    drop(single_reader);
    let mut constrained = Writer::new(
        Vec::new(),
        Projection::PrivateRecovery,
        native::meter::Budget::new(tight),
    )
    .unwrap();
    let copied = v3::planning::confirmed(
        &fixture.db,
        &mut constrained,
        &binding,
        Projection::PrivateRecovery,
    )
    .await;
    assert!(
        matches!(copied, Err(SafeError::LimitExceeded)),
        "live input/output overlap must exceed the resident ceiling: {copied:?}"
    );
    let spent = constrained.abort();
    assert!(spent.check().is_err());
    assert!(spent.work.raw_bytes > source.len() as u64);
    assert!(spent.work.output_bytes > 0);
    let output = private_output(&fixture.db, &binding).await.unwrap();
    let short_output = records(&output)
        .into_iter()
        .filter(|(kind, _, _)| *kind == 7)
        .map(|(_, payload, _)| payload.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(short_output, short_source);
    let mut reader = v3::Reader::new(
        output.as_slice(),
        Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )
    .unwrap();
    while reader.next_frame().unwrap().is_some() {}
    assert!(reader.verified());
    assert_eq!(private_output(&fixture.db, &binding).await.unwrap(), output);

    // Publish a correctly sealed transport with a malformed excluded Result
    // section in a separate fixture database; no immutable guard is disabled.
    let malformed = excluded_invalid_cell(&source);
    let isolated = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&isolated)
        .await
        .unwrap();
    let tx = isolated.begin().await.unwrap();
    let damaged = segmented::write_archive(
        &tx,
        &reference.archive_id,
        &header,
        &mut malformed.as_slice(),
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut transported = Vec::new();
    segmented::read_archive(
        &isolated,
        &damaged,
        &mut transported,
        &mut v3::files::stream::budget(),
    )
    .await
    .unwrap();
    assert_eq!(transported, malformed);
    assert!(matches!(
        private_output(&isolated, &binding).await,
        Err(SafeError::InvalidInput)
    ));
}
