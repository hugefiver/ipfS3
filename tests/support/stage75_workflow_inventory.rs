use crate::support::Fixture;
use ipfs_s3_gateway::{
    pinning::management::{
        Capture, PlanRequest, Snapshot, db_snapshot,
        retirement::{
            ArchiveCursor, FrozenRetirementImport, InventorySpool, PreparedPlanning, Progress,
            RunLimits,
        },
        wire::v3::{
            self, AppliedTrust, Event, Frame, Projection, Reader, Writer, planning::Header,
        },
    },
    store::pinning::management::{
        native::meter::{Budget, Limits},
        records::SharedDecodeBudget,
    },
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use std::{path::Path, time::Duration};

fn reserve(path: &Path) -> std::fs::File {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}

fn limits() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.elapsed = Duration::from_secs(30);
    limits
}

fn budget() -> Budget {
    Budget::new(Limits {
        rows: 100_000,
        bytes: 128 * 1024 * 1024,
        sql: 1_000_000,
        resident: 16 * 1024 * 1024,
        elapsed: Duration::from_secs(30),
    })
}

pub struct FrozenInventory {
    spool: InventorySpool,
}

pub struct WorkflowPlanning {
    pub snapshot: Snapshot,
    pub prepared: PreparedPlanning,
    pub inventory: FrozenInventory,
    pub work: SharedDecodeBudget,
}

impl WorkflowPlanning {
    pub async fn close(self) {
        let prepared = self.prepared.close().await;
        let inventory = self.inventory.spool.close().await;
        prepared.unwrap();
        inventory.unwrap();
    }
}

pub async fn plan_and_restore(fixture: &Fixture, request: &PlanRequest) -> WorkflowPlanning {
    fixture.db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect,first_error,last_error) VALUES('foreign-workflow','QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE','unknown','unknown','历史_PRIVATE_ORIGINAL','')")
        .await.unwrap();
    let mut run = limits();
    let mut output_budget = budget();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    run.constrain_deadline(deadline, &mut output_budget)
        .unwrap();
    let work = SharedDecodeBudget::default();
    let captured = db_snapshot::collect_with_control(
        &fixture.db,
        &fixture.config,
        request,
        deadline,
        run.cancellation.clone(),
        work.clone(),
    )
    .await
    .unwrap();
    let (snapshot, scope) = captured.planning_with_scope().unwrap();
    let measured = scope.work();
    output_budget
        .charge(
            measured.sql,
            measured.rows,
            measured.cells,
            measured.raw_bytes,
        )
        .unwrap();
    output_budget.work.copied_bytes = measured.copied_bytes;
    output_budget.work.output_bytes = measured.output_bytes;
    output_budget.work.pages = measured.pages;
    let _source_resident = output_budget.reserve(scope.peak_resident()).unwrap();
    let prepared =
        PreparedPlanning::collect_with_limits(&fixture.db, request, &snapshot, work.clone(), &run)
            .await
            .unwrap();
    assert_eq!(prepared.plan.items().len(), 1);
    let Some(Capture::Provider(capture)) = &prepared.plan.items()[0].capture else {
        unreachable!()
    };
    assert!(capture.totals.leases > 0);
    assert!(capture.totals.current_resources > 0);
    let expected = capture.inventory.clone();
    let frame = snapshot.native_frame.as_ref().unwrap();
    let canonical = fixture.directory.path().join("workflow-private.v3");
    let (exported, output_budget) = prepared
        .export_file(
            (reserve(&canonical), output_budget),
            Header {
                projection: Projection::PrivateRecovery,
                database_scope: fixture.auth.database_scope(),
                database_identity: fixture.auth.database_identity(),
                principal: fixture.auth.principal(),
                catalog: &frame.schema_versions,
                created_at: snapshot.database_now.unwrap().to_rfc3339(),
                expires_at: None,
            },
            request,
            (&snapshot, Some(&scope)),
            None,
            &run,
        )
        .await;
    exported.unwrap();
    output_budget.check().unwrap();
    let cursor = v3::archive_cursor::stage_import(
        std::fs::File::open(&canonical).unwrap(),
        Projection::PrivateRecovery,
        budget(),
    )
    .unwrap()
    .into_cursor();
    let path = fixture.directory.path().join("workflow-inventory.sqlite");
    let mut importer = FrozenRetirementImport::create(cursor, &path, reserve(&path), expected)
        .await
        .unwrap();
    while importer.advance(&limits()).await.unwrap() == Progress::Pending {}
    let (spool, _canonical, import_budget) = importer.into_parts().unwrap();
    import_budget.check().unwrap();
    assert!(spool.frozen_review().await.unwrap().totals() == &capture.totals);
    let inventory = FrozenInventory { spool };
    assert_frozen(fixture, &inventory).await;
    WorkflowPlanning {
        snapshot,
        prepared,
        inventory,
        work,
    }
}

pub async fn assert_frozen(fixture: &Fixture, inventory: &FrozenInventory) {
    let original = &inventory.spool;
    let path = fixture
        .directory
        .path()
        .join(format!("workflow-restored-{}.sqlite", uuid::Uuid::new_v4()));
    let checkpoint = original.checkpoint().await.unwrap();
    let mut restored = InventorySpool::create(&path, reserve(&path), &checkpoint.rewind())
        .await
        .unwrap();
    let run = limits();
    assert_eq!(
        restored.reconstruct(original, &run).await.unwrap(),
        Progress::Archived
    );
    let configuration = original
        .frozen_review()
        .await
        .unwrap()
        .configuration()
        .await
        .unwrap();
    assert_eq!(
        restored.reduce(&configuration, &run).await.unwrap(),
        Progress::Reduced
    );
    assert!(
        restored.frozen_review().await.unwrap().totals()
            == original.frozen_review().await.unwrap().totals()
    );
    let mut left = ArchiveCursor::default();
    let mut right = ArchiveCursor::default();
    let mut cells = 0;
    let mut private = Vec::new();
    let mut null = false;
    let mut empty = false;
    loop {
        let a = original.read_part(&mut left).await.unwrap();
        let b = restored.read_part(&mut right).await.unwrap();
        assert!(
            a == b,
            "reconstruction must preserve every original byte, NULL and empty cell"
        );
        let Some(part) = a else { break };
        cells += 1;
        if part.table == "remote_pin_ledger"
            && part.key.values.first().map(String::as_str) == Some("foreign-workflow")
        {
            if part.column == "route" {
                assert_eq!(part.length, None);
                assert!(part.bytes.is_empty());
                null = true;
            }
            if part.column == "last_error" {
                assert_eq!(part.length, Some(0));
                assert!(part.bytes.is_empty());
                empty = true;
            }
        }
        if part.table == "remote_pin_ledger"
            && part.column == "first_error"
            && part.key.values.first().map(String::as_str) == Some("foreign-workflow")
        {
            private.extend(part.bytes);
        }
    }
    assert!(cells > 0);
    assert!(
        null && empty,
        "the original must contain both NULL and empty text"
    );
    assert_eq!(private, "历史_PRIVATE_ORIGINAL".as_bytes());
    for projection in [Projection::PrivateRecovery, Projection::Review] {
        let mut writer = Writer::new(Vec::new(), projection, budget()).unwrap();
        writer
            .frame(&Frame::Header {
                schema_version: v3::SCHEMA_VERSION,
                projection,
                database_scope: checkpoint.database_scope.clone(),
                database_identity: checkpoint.database_identity.clone(),
                principal: "isolated-workflow".into(),
                applied_trust: AppliedTrust::UntrustedClaimsOnly,
                catalog: checkpoint.catalog.clone(),
                created_at: checkpoint.observed_at.to_rfc3339(),
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
        restored
            .write_archive_sections(&mut writer, &limits())
            .await
            .unwrap();
        let bytes = writer.finish().unwrap();
        let mut reader = Reader::new(bytes.as_slice(), projection, budget()).unwrap();
        let mut saw_private = false;
        let mut redacted = 0;
        while let Some(event) = reader.next_frame().unwrap() {
            match event {
                Event::Chunk(chunk) => {
                    if String::from_utf8_lossy(&chunk).contains("历史_PRIVATE_ORIGINAL") {
                        saw_private = true;
                    }
                    if projection == Projection::Review {
                        assert!(!String::from_utf8_lossy(&chunk).contains("PRIVATE_ORIGINAL"));
                        assert!(!String::from_utf8_lossy(&chunk).contains("PRIVATE_CURRENT"));
                    }
                }
                Event::Frame(Frame::CellEnd {
                    value: v3::CellValue::Redacted,
                }) => redacted += 1,
                _ => {}
            }
        }
        assert!(reader.verified());
        if projection == Projection::PrivateRecovery {
            assert!(saw_private);
        } else {
            assert!(!saw_private);
            assert!(redacted > 0);
        }
    }
    let tx = fixture.db.begin().await.unwrap();
    assert!(
        original
            .frozen_review()
            .await
            .unwrap()
            .seal(&tx, &limits())
            .await
            .is_err(),
        "an imported inventory is not fresh execution authority"
    );
    assert!(
        restored
            .frozen_review()
            .await
            .unwrap()
            .seal(&tx, &limits())
            .await
            .is_err(),
        "a frozen import is not fresh execution authority"
    );
    tx.rollback().await.unwrap();
    restored.close().await.unwrap();
}
