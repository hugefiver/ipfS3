#[path = "stage75_confirmation_history/payload.rs"]
mod payload;
#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    config::Config,
    pinning::management::{
        Action, ItemRequest, NativeKey, RetirementSelection, RouteIdentity, Subject, db_snapshot,
        retirement::{ConfirmationProgress, PlanningCheckpoint, PlanningProgress, RunLimits},
        wire::v3::{
            self, files,
            history::{self, Export, Progress},
        },
    },
    store::pinning::management::records,
};
use sea_orm::{ConnectionTrait, Statement};
use std::io::Write;
use tokio_util::sync::CancellationToken;

fn limits() -> RunLimits {
    let mut limits = RunLimits::default();
    limits.pages = 512;
    limits
}

async fn source_complete_prefix(db: &sea_orm::DatabaseConnection) -> bool {
    let row=db.query_one(Statement::from_string(db.get_database_backend(),"SELECT digest FROM pin_management_archive WHERE archive_id LIKE 'checkpoint:%' AND state='staging'")).await.unwrap();
    let Some(row) = row else {
        return false;
    };
    let state: serde_json::Value =
        serde_json::from_str(&row.try_get::<String>("", "digest").unwrap()).unwrap();
    state["stage"] == "source"
        && state["source"]["source"]["row"].is_null()
        && state["source"]["source"]["table"].as_u64()
            == Some(
                state["source"]["source"]["revisions"]
                    .as_object()
                    .unwrap()
                    .len() as u64,
            )
}

fn rewrite_cell(
    source: &std::path::Path,
    target: &std::path::Path,
    key: &NativeKey,
    column: &str,
    replacement: &[u8],
) {
    let mut reader = v3::Reader::new(
        std::fs::File::open(source).unwrap(),
        v3::Projection::PrivateRecovery,
        files::stream::budget(),
    )
    .unwrap();
    let mut writer = files::stream::writer(
        target.to_str().unwrap(),
        v3::Projection::PrivateRecovery,
        files::stream::budget(),
    )
    .unwrap();
    let mut matching_row = false;
    let mut replacing = false;
    let mut replaced = false;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            v3::Event::Frame(v3::Frame::Trailer { .. }) => {}
            v3::Event::Frame(frame) => {
                if let v3::Frame::Row { key: current, .. } = &frame {
                    matching_row = current == key;
                }
                if let v3::Frame::Cell { column: current } = &frame {
                    replacing = matching_row && current == column;
                }
                writer.frame(&frame).unwrap();
                if matches!(frame, v3::Frame::Cell { .. }) && replacing {
                    writer.cell_output().write_all(replacement).unwrap();
                    replaced = true;
                }
            }
            v3::Event::Chunk(bytes) if !replacing => {
                writer.cell_output().write_all(&bytes).unwrap()
            }
            v3::Event::Chunk(_) => {}
        }
    }
    assert!(reader.verified());
    assert!(replaced);
    files::stream::finish(writer).unwrap();
}

#[tokio::test]
async fn opaque_staging_history_preserves_bytes_and_declared_confirmation_cannot_fall_back() {
    let fixture = support::Fixture::new(false).await;
    let opaque = format!(
        "opaque 私密 S75-confirm-prefix-1 {}",
        "x".repeat(1024 * 1024)
    );
    let headers = [
        opaque,
        "null".into(),
        r#"{"other":{"format":"S75-confirm-prefix-1"},"text":"format: S75-confirm-prefix-1"}"#
            .into(),
        r#""opaque S75-confirm-prefix-1""#.into(),
        r#"{"x":truXXX,"format":"S75-confirm-prefix-1"}"#.into(),
        r#"{"x":[},"format":"S75-confirm-prefix-1"}"#.into(),
        r#"{"x":1e+,"format":"S75-confirm-prefix-1"}"#.into(),
        format!(r#"{{"x":"{}","other":true}}"#, "x".repeat(1024 * 1024)),
        format!(
            r#"{{"x":{}0{},"other":true}}"#,
            "[".repeat(4100),
            "]".repeat(4100)
        ),
    ];
    let payload = b"unfinished private chunk\0\xff";
    for (index, header) in headers.iter().enumerate() {
        let id = format!("checkpoint:opaque-{index}");
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state,chunks,bytes,digest) VALUES($1,'S75A1','foreign-scope','foreign-identity',$2,'staging',1,$3,NULL)",
            vec![id.clone().into(),header.clone().into(),(payload.len() as i64).into()])).await.unwrap();
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "INSERT INTO pin_management_archive_chunks VALUES($1,0,$2)",
                vec![id.into(), payload.to_vec().into()],
            ))
            .await
            .unwrap();
    }
    let mut export = Export::start(
        &fixture.db,
        "history",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    while export
        .advance(&fixture.db, 64, CancellationToken::new())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let path = fixture.file("opaque-staging.private");
    export.write_private(path.to_str().unwrap()).await.unwrap();
    let mut recovered = history::restore_private(path.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap();
    for (index, header) in headers.iter().enumerate() {
        let id = format!("checkpoint:opaque-{index}");
        let key = NativeKey {
            table: "pin_management_archive".into(),
            values: vec![id.clone()],
        };
        let mut raw = Vec::new();
        recovered.cell(&key, "header", &mut raw).await.unwrap();
        assert_eq!(raw, header.as_bytes());
        assert_eq!(
            recovered
                .cell(&key, "digest", &mut Vec::new())
                .await
                .unwrap(),
            None
        );
        raw.clear();
        recovered.cell(&key, "state", &mut raw).await.unwrap();
        assert_eq!(raw, b"staging");
        assert!(recovered.archive(&id).await.is_err());
        let key = NativeKey {
            table: "pin_management_archive_chunks".into(),
            values: vec![id, "0".into()],
        };
        raw.clear();
        recovered.cell(&key, "payload", &mut raw).await.unwrap();
        assert_eq!(hex::decode(raw).unwrap(), payload);
    }
    recovered.close().await.unwrap();
    export.close().await.unwrap();

    // These remain staging, so only the explicit declaration can select strict
    // confirmation verification. Neither a malformed suffix nor a duplicate
    // format may turn a declared checkpoint back into an opaque archive.
    for (index, header) in [
        r#"{"format":"S75-confirm-prefix-1","broken": "#.to_owned(),
        r#"{"format":"S75-confirm-prefix-1","format":"opaque"}"#.into(),
        r#"{"format":"opaque","format":"S75-confirm-prefix-1"}"#.into(),
        r#"{"\u0066ormat":"S75-confirm-prefix-1"}"#.into(),
        r#"{"format":"S75-confirm-unknown"}"#.into(),
        format!(r#"{{"format":"S75-confirm-{}"}}"#, "x".repeat(400)),
        format!(r#"{{"format":"S75-confirm-{}"#, "x".repeat(400)),
        format!(
            r#"{{"x":{}0{},"format":"S75-confirm-unknown"}}"#,
            "[".repeat(4100),
            "]".repeat(4100)
        ),
    ]
    .iter()
    .enumerate()
    {
        let corrupt = fixture.file(&format!("declared-staging-{index}.private"));
        let key = NativeKey {
            table: "pin_management_archive".into(),
            values: vec!["checkpoint:opaque-0".into()],
        };
        rewrite_cell(&path, &corrupt, &key, "header", header.as_bytes());
        assert!(
            history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
                .await
                .is_err(),
            "declared confirmation must not use opaque staging semantics"
        );
    }
    fixture.db.execute_unprepared("INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES('declared-bad','S75A1','foreign-scope','foreign-identity','{\"format\":\"S75-confirm-prefix-1\"}','staging')").await.unwrap();
    let mut export = Export::start(
        &fixture.db,
        "history",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let rejected = loop {
        match export
            .advance(&fixture.db, 64, CancellationToken::new())
            .await
        {
            Err(_) => break true,
            Ok(Progress::Sealed) => break false,
            Ok(Progress::Pending) => {}
        }
    };
    assert!(rejected);
    export.close().await.unwrap();
}

#[tokio::test]
async fn ready_confirmation_private_history_round_trip_preserves_original_evidence() {
    let mut fixture = support::Fixture::new(false).await;
    let private = format!(
        r#"{{"private":"{}秘密🔒"}}"#,
        "x".repeat(65535 - r#"{"private":""#.len())
    );
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES('foreign-history','private-history',1,$1),('foreign-history','empty-history',1,'')",[private.into()])).await.unwrap();
    fixture.request.items = vec![ItemRequest {
        item_id: "retire".into(),
        subject: Subject::Provider {
            route: RouteIdentity::from_original(
                fixture.config.pinning.providers[0].route_snapshot(),
            ),
        },
        action: Action::RetireWith {
            selection: RetirementSelection::Retain,
        },
    }];
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &fixture.request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let mut task = PlanningCheckpoint::create(
        &fixture.file("confirmation-history-task"),
        &fixture.db,
        auth.principal(),
        &fixture.request,
        &snapshot,
        &limits(),
    )
    .await
    .unwrap();
    let snapshot = task.snapshot().clone();
    let prepared = loop {
        if let PlanningProgress::Complete(prepared) =
            task.advance(&fixture.db, &limits()).await.unwrap()
        {
            break prepared;
        }
    };
    let mut slice = limits();
    slice.pages = 1;
    assert!(matches!(
        prepared
            .advance_confirmation(
                &fixture.db,
                &fixture.config,
                &auth,
                &fixture.request,
                &snapshot,
                &slice
            )
            .await
            .progress
            .unwrap(),
        ConfirmationProgress::Incomplete
    ));
    let mut staging = Export::start(
        &fixture.db,
        "history",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    while staging
        .advance(&fixture.db, 64, CancellationToken::new())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let staging_path = fixture.file("confirmation-history-staging.private");
    staging
        .write_private(staging_path.to_str().unwrap())
        .await
        .unwrap();
    history::restore_private(staging_path.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    staging.close().await.unwrap();
    let mut saw_source_complete = source_complete_prefix(&fixture.db).await;
    loop {
        let mut slice = limits();
        if !saw_source_complete {
            slice.pages = 1;
        }
        match prepared
            .advance_confirmation(
                &fixture.db,
                &fixture.config,
                &auth,
                &fixture.request,
                &snapshot,
                &slice,
            )
            .await
            .progress
            .unwrap()
        {
            ConfirmationProgress::Incomplete => {
                if !saw_source_complete && source_complete_prefix(&fixture.db).await {
                    let mut export = Export::start(
                        &fixture.db,
                        "history",
                        files::stream::budget(),
                        CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                    while export
                        .advance(&fixture.db, 64, CancellationToken::new())
                        .await
                        .unwrap()
                        == Progress::Pending
                    {}
                    let path = fixture.file("confirmation-source-complete-staging.private");
                    export.write_private(path.to_str().unwrap()).await.unwrap();
                    history::restore_private(path.to_str().unwrap(), files::stream::budget())
                        .await
                        .unwrap()
                        .close()
                        .await
                        .unwrap();
                    export.close().await.unwrap();
                    saw_source_complete = true;
                }
            }
            ConfirmationProgress::Ready(_) => break,
            ConfirmationProgress::Confirmed(_) => {
                panic!("history staging must not publish confirmation")
            }
        }
    }
    assert!(saw_source_complete);
    let root = fixture.db.query_one(Statement::from_string(fixture.db.get_database_backend(),
        "SELECT archive_id,header,digest FROM pin_management_archive WHERE archive_id LIKE 'checkpoint:%' AND state='sealed'"
    )).await.unwrap().unwrap();
    let id: String = root.try_get("", "archive_id").unwrap();
    let header: String = root.try_get("", "header").unwrap();
    let state: String = root.try_get("", "digest").unwrap();
    let chunks = fixture.db.query_all(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "SELECT ordinal,payload FROM pin_management_archive_chunks WHERE archive_id=$1 ORDER BY ordinal", [id.clone().into()]
    )).await.unwrap();
    let mut export = Export::start(
        &fixture.db,
        "history",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    while export
        .advance(&fixture.db, 64, CancellationToken::new())
        .await
        .unwrap()
        == Progress::Pending
    {}
    let path = fixture.file("confirmation-history.private");
    export.write_private(path.to_str().unwrap()).await.unwrap();
    let mut recovered = history::restore_private(path.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap();
    let restore_bytes = recovered.budget().work.raw_bytes;
    let key = NativeKey {
        table: "pin_management_archive".into(),
        values: vec![id.clone()],
    };
    for (column, expected) in [("header", &header), ("digest", &state)] {
        let mut bytes = Vec::new();
        recovered.cell(&key, column, &mut bytes).await.unwrap();
        assert_eq!(bytes, expected.as_bytes());
    }
    let mut original_payload = Vec::new();
    for chunk in chunks {
        let ordinal: i64 = chunk.try_get("", "ordinal").unwrap();
        let payload: Vec<u8> = chunk.try_get("", "payload").unwrap();
        let key = NativeKey {
            table: "pin_management_archive_chunks".into(),
            values: vec![id.clone(), ordinal.to_string()],
        };
        let mut encoded = Vec::new();
        recovered.cell(&key, "payload", &mut encoded).await.unwrap();
        assert_eq!(hex::decode(encoded).unwrap(), payload);
        original_payload.extend_from_slice(&payload);
    }
    assert!(recovered.archive(&id).await.is_err());
    let mut wrong_stage: serde_json::Value = serde_json::from_str(&state).unwrap();
    wrong_stage["stage"] = "source".into();
    let corrupt = fixture.file("confirmation-history-nonready.private");
    rewrite_cell(
        &path,
        &corrupt,
        &key,
        "digest",
        &serde_json::to_vec(&wrong_stage).unwrap(),
    );
    assert!(
        history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
            .await
            .is_err()
    );
    let missing = fixture.file("confirmation-history-missing-nullable.private");
    let mut missing_field: serde_json::Value = serde_json::from_str(&header).unwrap();
    missing_field
        .as_object_mut()
        .unwrap()
        .remove("local_database_now");
    rewrite_cell(
        &path,
        &missing,
        &key,
        "header",
        &serde_json::to_vec(&missing_field).unwrap(),
    );
    assert!(
        history::restore_private(missing.to_str().unwrap(), files::stream::budget())
            .await
            .is_err()
    );
    let duplicate = fixture.file("confirmation-history-duplicate.private");
    let duplicated = header.replacen('{', "{\"format\":\"S75-confirm-prefix-1\",", 1);
    rewrite_cell(&path, &duplicate, &key, "header", duplicated.as_bytes());
    assert!(
        history::restore_private(duplicate.to_str().unwrap(), files::stream::budget())
            .await
            .is_err()
    );
    let second = fixture.file("confirmation-history-again.private");
    recovered
        .export_private(second.to_str().unwrap())
        .await
        .unwrap();
    let mut recovered = history::restore_private(second.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    recovered.cell(&key, "digest", &mut bytes).await.unwrap();
    assert_eq!(bytes, state.as_bytes());
    recovered.close().await.unwrap();
    let values = payload::values(&original_payload);
    for page in values.iter().filter(|value| value.is_object()) {
        for side in ["before", "after"] {
            let original = page[side]["source"]["route"]["original"]
                .as_object()
                .unwrap();
            assert!(
                original
                    .get("secret_ref")
                    .is_some_and(serde_json::Value::is_null)
            );
            assert!(!original.contains_key("confirmation"));
        }
    }
    let cells = values
        .iter()
        .filter_map(|value| value.get("parts"))
        .flat_map(|parts| parts.as_array().unwrap())
        .flat_map(|row| row[2].as_array().unwrap())
        .collect::<Vec<_>>();
    assert!(
        cells
            .iter()
            .any(|cell| cell[3].as_array().unwrap().len() == 65536
                && cell[3].as_array().unwrap().last().unwrap() == 0xe7)
    );
    assert!(cells.iter().any(|cell| {
        cell[1] == 65536
            && cell[3]
                .as_array()
                .unwrap()
                .first()
                .is_some_and(|byte| *byte == 0xa7)
    }));
    assert!(cells.iter().any(|cell| cell[2].is_null()));
    assert!(
        cells
            .iter()
            .any(|cell| cell[2] == 0 && cell[3].as_array().unwrap().is_empty())
    );
    let legacy_values = payload::flat(&values);
    let legacy_payload = payload::encode(&legacy_values);
    let legacy = fixture.file("confirmation-history-flat.private");
    payload::rewrite(&path, &legacy, &id, &legacy_payload, 0);
    let mut restored = history::restore_private(legacy.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap();
    payload::assert_chunks(&mut restored, &id, &legacy_payload).await;
    assert!(restored.archive(&id).await.is_err());
    let legacy_again = fixture.file("confirmation-history-flat-again.private");
    restored
        .export_private(legacy_again.to_str().unwrap())
        .await
        .unwrap();
    let mut restored =
        history::restore_private(legacy_again.to_str().unwrap(), files::stream::budget())
            .await
            .unwrap();
    payload::assert_chunks(&mut restored, &id, &legacy_payload).await;
    restored.close().await.unwrap();

    let mut missing_length = legacy_values;
    let first = missing_length
        .iter_mut()
        .find_map(|value| {
            value
                .get_mut("parts")
                .and_then(|parts| parts.as_array_mut())
                .and_then(|parts| parts.first_mut())
        })
        .unwrap();
    first.as_object_mut().unwrap().remove("length");
    let invalid_flat = fixture.file("confirmation-history-flat-missing-length.private");
    payload::rewrite(
        &path,
        &invalid_flat,
        &id,
        &payload::encode(&missing_length),
        0,
    );
    assert!(
        history::restore_private(invalid_flat.to_str().unwrap(), files::stream::budget())
            .await
            .is_err()
    );
    for mutation in [
        "before-secret-ref",
        "after-secret-ref",
        "flat-before-secret-ref",
        "unknown-format",
        "group-without-format",
        "source-chain",
        "wire-prefix",
    ] {
        let mut damaged = if mutation == "flat-before-secret-ref" {
            payload::flat(&values)
        } else {
            values.clone()
        };
        match mutation {
            "before-secret-ref" | "after-secret-ref" | "flat-before-secret-ref" => {
                let side = if mutation == "after-secret-ref" {
                    "after"
                } else {
                    "before"
                };
                assert!(
                    damaged[0][side]["source"]["route"]["original"]
                        .as_object_mut()
                        .unwrap()
                        .remove("secret_ref")
                        .unwrap()
                        .is_null()
                );
            }
            "unknown-format" => damaged[0]["format"] = "unknown".into(),
            "group-without-format" => {
                damaged[0].as_object_mut().unwrap().remove("format");
            }
            "source-chain" => {
                damaged[0]["after"]["position"] =
                    (damaged[0]["after"]["position"].as_i64().unwrap() + 1).into()
            }
            "wire-prefix" => {
                let wire = damaged
                    .iter_mut()
                    .filter_map(|value| value.as_array_mut())
                    .find(|rows| rows.first().is_some_and(|row| row.get("table").is_some()))
                    .unwrap();
                assert!(wire.len() > 1);
                wire.remove(0);
            }
            _ => unreachable!(),
        }
        let corrupt = fixture.file(&format!("confirmation-history-{mutation}.private"));
        payload::rewrite(&path, &corrupt, &id, &payload::encode(&damaged), 0);
        assert!(
            history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
                .await
                .is_err(),
            "{mutation}"
        );
    }
    for (name, bytes, first) in [
        (
            "truncated",
            original_payload[..original_payload.len() - 1].to_vec(),
            0,
        ),
        (
            "garbage",
            [original_payload.as_slice(), b"garbage"].concat(),
            0,
        ),
        ("wrong-ordinal", original_payload.clone(), 1),
    ] {
        let corrupt = fixture.file(&format!("confirmation-history-{name}.private"));
        payload::rewrite(&path, &corrupt, &id, &bytes, first);
        assert!(
            history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
                .await
                .is_err(),
            "{name}"
        );
    }
    let mut changed_header: serde_json::Value = serde_json::from_str(&header).unwrap();
    changed_header["inventories"][0]["archive_id"] = "wrong-inventory".into();
    let mut changed_state: serde_json::Value = serde_json::from_str(&state).unwrap();
    changed_state["source"]["source"]["database_identity"] = "wrong-identity".into();
    let duplicate_revisions =
        header.replacen("\"revisions\":{", "\"revisions\":{\"objects\":0,", 1);
    assert_ne!(duplicate_revisions, header);
    for (name, column, value) in [
        (
            "header-id",
            "header",
            serde_json::to_vec(&changed_header).unwrap(),
        ),
        (
            "source-identity",
            "digest",
            serde_json::to_vec(&changed_state).unwrap(),
        ),
        ("root-identity", "database_scope", b"wrong-scope".to_vec()),
        (
            "duplicate-revisions",
            "header",
            duplicate_revisions.into_bytes(),
        ),
    ] {
        let corrupt = fixture.file(&format!("confirmation-history-{name}.private"));
        rewrite_cell(&path, &corrupt, &key, column, &value);
        assert!(
            history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
                .await
                .is_err(),
            "{name}"
        );
    }
    let count = legacy_payload.len().div_ceil(v3::CELL_BYTES);
    for (name, count) in [("missing-chunk", count - 1), ("extra-chunk", count + 1)] {
        let corrupt = fixture.file(&format!("confirmation-history-{name}.private"));
        rewrite_cell(
            &legacy,
            &corrupt,
            &key,
            "chunks",
            count.to_string().as_bytes(),
        );
        assert!(
            history::restore_private(corrupt.to_str().unwrap(), files::stream::budget())
                .await
                .is_err(),
            "{name}"
        );
    }
    let ordinary=fixture.db.query_one(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "SELECT archive_id,header FROM pin_management_archive WHERE state='sealed' AND archive_id<>$1 LIMIT 1",[id.clone().into()])).await.unwrap().unwrap();
    let ordinary_key = NativeKey {
        table: "pin_management_archive".into(),
        values: vec![ordinary.try_get::<String>("", "archive_id").unwrap()],
    };
    let disguised = ordinary.try_get::<String>("", "header").unwrap().replacen(
        '{',
        "{\"format\":\"S75-confirm-prefix-1\",",
        1,
    );
    let ordinary_corrupt = fixture.file("ordinary-archive-disguised.private");
    rewrite_cell(
        &path,
        &ordinary_corrupt,
        &ordinary_key,
        "header",
        disguised.as_bytes(),
    );
    assert!(
        history::restore_private(ordinary_corrupt.to_str().unwrap(), files::stream::budget())
            .await
            .is_err()
    );

    use ipfs_s3_gateway::store::pinning::management::native::meter::Budget;
    let mut ceiling = files::stream::limits();
    ceiling.bytes = restore_bytes - 1;
    assert!(
        history::restore_private(path.to_str().unwrap(), Budget::new(ceiling))
            .await
            .is_err()
    );
    let mut ceiling = files::stream::limits();
    ceiling.resident = 10 * 1024 * 1024;
    assert!(
        history::restore_private(path.to_str().unwrap(), Budget::new(ceiling))
            .await
            .is_err()
    );
    let budget = files::stream::budget();
    budget
        .cancellation()
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        history::restore_private(path.to_str().unwrap(), budget)
            .await
            .is_err()
    );
    let mut ceiling = files::stream::limits();
    ceiling.elapsed = std::time::Duration::ZERO;
    assert!(
        history::restore_private(path.to_str().unwrap(), Budget::new(ceiling))
            .await
            .is_err()
    );
    export.close().await.unwrap();
    prepared.cleanup().await.unwrap();
}
