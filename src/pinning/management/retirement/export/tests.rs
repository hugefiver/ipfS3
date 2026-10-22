use super::*;
use crate::pinning::management::wire::v3::{self, Projection};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct StopAfterChunk {
    bytes: Vec<u8>,
    clock: Arc<Mutex<Instant>>,
    end: Instant,
}
impl Write for StopAfterChunk {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(bytes);
        if bytes == b"accepted cell bytes" {
            *self.clock.lock().unwrap() = self.end;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn planning_soft_stop_pauses_after_complete_chunk_and_reopens_without_gaps() {
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    spool
        .reduced_here
        .store(true, std::sync::atomic::Ordering::Release);
    let clock = Arc::new(Mutex::new(Instant::now()));
    let mut limits = RunLimits::default().for_production();
    limits.production_clock = Some(clock.clone());
    let sink = StopAfterChunk {
        bytes: Vec::new(),
        clock,
        end: Instant::now() + Duration::from_secs(30),
    };
    let mut writer = Writer::new(sink, Projection::Review, Budget::default()).unwrap();
    writer
        .frame(&Frame::Header {
            schema_version: v3::SCHEMA_VERSION,
            projection: Projection::Review,
            applied_trust: v3::AppliedTrust::UntrustedClaimsOnly,
            database_scope: "scope".into(),
            database_identity: "identity".into(),
            principal: "operator".into(),
            catalog: vec!["fixture".into()],
            created_at: "2026-01-01T00:00:00Z".into(),
            expires_at: None,
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
    let mut cursor = RetirementArchiveCursor {
        section: 3,
        ..Default::default()
    };
    let catalog = native::current_catalog();
    let layout = catalog
        .iter()
        .find(|layout| {
            layout
                .primary_key
                .iter()
                .all(|column| !native::confidential(&layout.table, column))
        })
        .unwrap();
    let column = layout
        .columns
        .iter()
        .find(|column| !native::confidential(&layout.table, column))
        .unwrap();
    let Event::Frame(row) = cursor.begin(
        Section::Native,
        NativeKey {
            table: layout.table.clone(),
            values: vec!["0".into(); layout.primary_key.len()],
        },
        vec![column.to_string()],
        Source::Inline("accepted cell bytes".into()),
    ) else {
        unreachable!()
    };
    writer.frame(&row).unwrap();
    let mut export = ArchiveExport {
        reference: spool.reference().clone(),
        cursor,
        complete: false,
        planning_facts_written: true,
    };
    assert_eq!(
        export
            .advance_planning_page(&spool, &mut writer, &Snapshot::default(), &limits)
            .await
            .unwrap(),
        Progress::Pending
    );
    assert!(!limits.production_available());
    limits.check().unwrap();
    assert!(!limits.cancellation.is_cancelled());
    let (paused, budget) = writer.pause();
    let (sink, continuation) = paused.unwrap();
    let encoded = encode(&export).unwrap();
    let before = spool.task_work().await.unwrap();
    spool.close().await.unwrap();
    let reopened = InventorySpool::resume(&path).await.unwrap();
    reopened
        .reduced_here
        .store(true, std::sync::atomic::Ordering::Release);
    let mut export: ArchiveExport = decode(&encoded).unwrap();
    let mut writer = Writer::continue_metered(sink.bytes, continuation, budget)
        .map_err(|(error, _)| error)
        .unwrap();
    assert_eq!(
        export
            .advance_planning_page(
                &reopened,
                &mut writer,
                &Snapshot::default(),
                &RunLimits::default()
            )
            .await
            .unwrap(),
        Progress::Archived
    );
    let bytes = writer.finish().unwrap();
    let mut reader =
        v3::Reader::new(bytes.as_slice(), Projection::Review, Budget::default()).unwrap();
    let mut payload = Vec::new();
    let mut rows = 0;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            Event::Chunk(bytes) => payload.extend_from_slice(&bytes),
            Event::Frame(Frame::Row { .. }) => rows += 1,
            _ => {}
        }
    }
    assert!(reader.verified());
    assert_eq!(payload, b"accepted cell bytes");
    assert_eq!(rows, 1);
    assert!(reopened.task_work().await.unwrap().pages > before.pages);
    reopened.close().await.unwrap();
    workspace.cleanup().unwrap();
}
