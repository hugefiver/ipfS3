use super::*;
use crate::{
    pinning::management::DryRunPlan,
    store::pinning::management::{
        native,
        records::segmented::{self, progress},
    },
};
use sea_orm::TransactionTrait;
use sha2::{Digest, Sha256};
use std::time::Instant;
use v3::{CellValue, Frame, Section};

const OUTPUT: &str = "planning-review-output";
const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn value(table: &str, column: &str, index: usize) -> Option<String> {
    let old = index > 100_000;
    let n = if old { (index - 100_000) * 10 } else { index };
    let prefix = if old { "scale-old" } else { "scale" };
    Some(match column {
        "id" => format!(
            "{prefix}-{}-{n}",
            if table == "objects" {
                "object"
            } else {
                "version"
            }
        ),
        "bucket" => "bucket".into(),
        "key" => format!("scale/{n}"),
        "cid" | "etag" => CID.into(),
        "size" => "10".into(),
        "encrypted" | "multipart" => "0".into(),
        "is_latest" | "sequence" => if old { "0" } else { "1" }.into(),
        "created_at" | "updated_at" | "lifecycle_age_started_at" => "2026-10-23 12:00:00".into(),
        "kind" => "object".into(),
        "object_id" => format!("{prefix}-object-{n}"),
        "version_id" if old => format!("old-{n}"),
        _ => return None,
    })
}

async fn append(
    db: &DatabaseConnection,
    raw: &str,
    position: &mut progress::Position,
    hash: &mut Sha256,
    bytes: &[u8],
    sealed: bool,
) {
    let mut ordinal = position.chunks as u64;
    for chunk in bytes.chunks(v3::CELL_BYTES) {
        hash.update(ordinal.to_be_bytes());
        hash.update((chunk.len() as u64).to_be_bytes());
        hash.update(chunk);
        ordinal += 1;
    }
    let cursor = if sealed {
        hex::encode(hash.clone().finalize())
    } else {
        "fixture".into()
    };
    let tx = db.begin().await.unwrap();
    *position = progress::append(&tx, OUTPUT, raw, position, &cursor, bytes, sealed)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// Same 100k current + 10k historical objects and versions as the real CLI
/// acceptance fixture, without source capture, reducer or per-cell spool SQL.
#[tokio::test]
#[ignore = "large sealed Review delivery under the CLI's unchanged limits"]
async fn sealed_review_100k_delivery() {
    let started = Instant::now();
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let mut spools = vec![InventorySpool::allocate(&path, file, None).await.unwrap()];
    let (path, file) = workspace.reserve().await.unwrap();
    let union = PlanUnion::create(&path, file).await.unwrap();
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.total_bytes = 4 * 1024 * 1024 * 1024;
    spool::task_run(&spools[0].db, &limits).await.unwrap();
    union.start_task(&limits).await.unwrap();
    spools[0].task = Some(Arc::new(task_budget::TaskBudget::new(
        &spools,
        &union,
        (limits.total_rows, limits.total_bytes),
    )));
    let db = &spools[0].db;
    let inventory_id = spools[0].reference().inventory_id.clone();
    for sql in [
        "CREATE TABLE pin_management_archive(archive_id TEXT PRIMARY KEY,format TEXT NOT NULL,database_scope TEXT NOT NULL,native_identity TEXT NOT NULL,header TEXT NOT NULL,state TEXT NOT NULL,chunks BIGINT NOT NULL DEFAULT 0,bytes BIGINT NOT NULL DEFAULT 0,digest TEXT NOT NULL DEFAULT '')",
        "CREATE TABLE pin_management_archive_chunks(archive_id TEXT NOT NULL,ordinal BIGINT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(archive_id,ordinal)) WITHOUT ROWID",
    ] {
        spool::exec(db, sql, vec![]).await.unwrap();
    }
    let header = segmented::ArchiveHeader {
        database_scope: "scope".into(),
        database_identity: "identity".into(),
        principal: "operator".into(),
        catalog: vec!["sealed-fixture".into()],
        request: "fixture".into(),
        configuration: "null".into(),
        acknowledgements: vec![],
        created_at: "2026-10-23T12:00:00Z".into(),
        expires_at: None,
    };
    let raw = serde_json::to_string(
        &serde_json::json!({"projection": Projection::Review, "archive_header": header}),
    )
    .unwrap();
    let tx = db.begin().await.unwrap();
    let mut position = progress::create(&tx, OUTPUT, &raw, "scope", "identity", "fixture")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut hash = Sha256::new_with_prefix(b"S75A1\0");
    hash.update((raw.len() as u64).to_be_bytes());
    hash.update(raw.as_bytes());
    let mut writer =
        Writer::new(Vec::new(), Projection::Review, v3::files::stream::budget()).unwrap();
    for frame in [
        Frame::Header {
            schema_version: v3::SCHEMA_VERSION,
            projection: Projection::Review,
            database_scope: header.database_scope.clone(),
            database_identity: header.database_identity.clone(),
            principal: header.principal.clone(),
            catalog: header.catalog.clone(),
            created_at: header.created_at.clone(),
            expires_at: None,
            applied_trust: v3::AppliedTrust::UntrustedClaimsOnly,
        },
        Frame::Request { request: None },
        Frame::Configuration {
            configuration: None,
        },
        Frame::Acknowledgements { item_ids: None },
        Frame::Summary {
            summary: Default::default(),
        },
    ] {
        writer.frame(&frame).unwrap();
    }
    let mut rows = 0;
    let mut cells = 0;
    for section in [Section::Native, Section::Membership] {
        for table in native::current_catalog()
            .iter()
            .filter(|t| matches!(t.table.as_str(), "object_versions" | "objects"))
        {
            let mut indices = (1..=110_000).collect::<Vec<_>>();
            indices.sort_by_cached_key(|&index| value(&table.table, "id", index).unwrap());
            let logical_events = if section == Section::Native {
                2 + table
                    .columns
                    .iter()
                    .map(|column| {
                        if value(&table.table, column, 1).is_some() {
                            3
                        } else {
                            2
                        }
                    })
                    .sum::<usize>()
            } else {
                5
            };
            let page_rows = 256.min(4096 / logical_events);
            for (page, indices) in indices.chunks(page_rows).enumerate() {
                for (offset, &index) in indices.iter().enumerate() {
                    let ordinal = page * page_rows + offset;
                    let id = value(&table.table, "id", index).unwrap();
                    let key = if section == Section::Native {
                        NativeKey {
                            table: table.table.clone(),
                            values: vec![id],
                        }
                    } else {
                        NativeKey {
                            table: "retirement_membership".into(),
                            values: vec![
                                inventory_id.clone(),
                                table.table.clone(),
                                "member".into(),
                                id,
                            ],
                        }
                    };
                    writer
                        .frame(&Frame::Row {
                            section,
                            key,
                            cells: if section == Section::Native {
                                table.columns.len() as u32
                            } else {
                                1
                            },
                        })
                        .unwrap();
                    if section == Section::Native {
                        for column in &table.columns {
                            let value = value(&table.table, column, index);
                            writer
                                .complete_cell(
                                    column,
                                    value.as_deref().unwrap_or_default().as_bytes(),
                                    if value.is_some() {
                                        CellValue::Text
                                    } else {
                                        CellValue::Null
                                    },
                                )
                                .unwrap();
                            cells += 1;
                        }
                    } else {
                        writer
                            .complete_cell(
                                "position",
                                ordinal.to_string().as_bytes(),
                                CellValue::Text,
                            )
                            .unwrap();
                        cells += 1;
                    }
                    writer.frame(&Frame::RowEnd).unwrap();
                    rows += 1;
                }
                let (paused, _) = writer.pause();
                let (bytes, continuation) = paused.unwrap();
                append(db, &raw, &mut position, &mut hash, &bytes, false).await;
                writer =
                    Writer::continue_metered(Vec::new(), continuation, v3::files::stream::budget())
                        .map_err(|(error, _)| error)
                        .unwrap();
            }
        }
    }
    let bytes = writer.finish().unwrap();
    append(db, &raw, &mut position, &mut hash, &bytes, true).await;
    eprintln!(
        "sealed fixture wall={:?} rows={rows} cells={cells} bytes={} chunks={}",
        started.elapsed(),
        position.bytes,
        position.chunks
    );
    assert_eq!(rows, 440_000);
    assert!(cells >= 3_000_000);
    let prepared = PreparedPlanning {
        plan: DryRunPlan::from_canonical("sealed-fixture".into(), vec![], Default::default()),
        workspace: Some(workspace),
        union: Some(union),
        spools,
        task_envelope: None,
    };
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits.total_bytes = 4 * 1024 * 1024 * 1024;
    let (result, budget) = prepared
        .export_prepared_output(
            (
                v3::files::stream::temporary().unwrap(),
                v3::files::stream::budget(),
            ),
            Projection::Review,
            &limits,
        )
        .await;
    eprintln!(
        "sealed delivery result={result:?} work={:?} peak={} task={:?}",
        budget.work,
        budget.peak_resident(),
        prepared.task_work().await.unwrap()
    );
    prepared.cleanup().await.unwrap();
    result.unwrap();
}
