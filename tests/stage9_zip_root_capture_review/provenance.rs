use super::{review, *};
use ipfs_s3_gateway::store::{multipart::v2_zip, zip::execution};

async fn original_receipt_fields(
    db: &sea_orm::DatabaseConnection,
    id: &str,
) -> Vec<Option<String>> {
    let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT owner,token,execution_id,bucket,archive_key,response_xml,response_headers FROM zip_v2_mpu_completions WHERE upload_id=?", [id.into()])).await.unwrap().unwrap();
    [
        "owner",
        "token",
        "execution_id",
        "bucket",
        "archive_key",
        "response_xml",
        "response_headers",
    ]
    .iter()
    .map(|field| row.try_get("", field).unwrap())
    .collect()
}

#[tokio::test]
async fn every_noncanonical_part_shape_is_fenced_before_root_invocation() {
    let canonical: Value = serde_json::from_str(
        &v2_zip::complete_request_contract(&[s3s::dto::CompletedPart {
            part_number: Some(1),
            ..Default::default()
        }])
        .unwrap(),
    )
    .unwrap();
    let mut variants = vec![
        (
            "etag42".to_owned(),
            json!([{
                "part_number":1,"etag":42,"crc32":null,"crc32c":null,"crc64nvme":null,"sha1":null,"sha256":null
            }]),
        ),
        ("only-part-number".into(), json!([{"part_number":1}])),
    ];
    for (label, etag) in [
        ("etag-string", json!("strong")),
        ("etag-empty-array", json!([])),
        ("etag-one-item", json!(["strong"])),
        ("etag-three-items", json!(["strong", "", null])),
        ("etag-unknown-strength", json!(["opaque", ""])),
        ("etag-strength-number", json!([1, ""])),
        ("etag-value-null", json!(["strong", null])),
    ] {
        let mut parts = canonical.clone();
        parts[0]["etag"] = etag;
        variants.push((label.into(), parts));
    }
    for field in ["etag", "crc32", "crc32c", "crc64nvme", "sha1", "sha256"] {
        let mut parts = canonical.clone();
        parts[0].as_object_mut().unwrap().remove(field);
        variants.push((format!("missing-{field}"), parts));
    }
    for field in ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"] {
        for (label, value) in [("number", json!(42)), ("array", json!([""]))] {
            let mut parts = canonical.clone();
            parts[0][field] = value;
            variants.push((format!("{field}-{label}"), parts));
        }
    }
    let mut unknown = canonical.clone();
    unknown[0]["freeform"] = json!("");
    variants.push(("unknown-field".into(), unknown));
    let mut mixed = canonical.clone();
    mixed[0].as_object_mut().unwrap().remove("sha256");
    mixed[0]["freeform"] = json!("");
    variants.push(("mixed-missing-and-unknown".into(), mixed));
    variants.push((
        "nonobject-part".into(),
        json!([canonical[0].clone(), "part"]),
    ));
    let total = variants.len();
    let mut violations = Vec::new();
    for (label, parts) in variants {
        let (harness, _, claim) = review::published_mpu().await;
        let db = harness.state.store.db();
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
            [claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        let before = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        let receipt = original_receipt_fields(db, &claim.batch_id).await;
        let intake = format!(
            "{:?}",
            v2_zip::read_by_upload(db, &claim.batch_id)
                .await
                .unwrap()
                .unwrap()
        );
        let execution = execution::read(db, &claim.batch_id).await.unwrap().unwrap();
        let counts = review::counters(&harness).await;
        let calls = harness.kubo.received_requests().await.unwrap().len();
        let additions = harness.captured_add_file_bytes();
        let bytes = parts.to_string();
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_v2_mpu_completions SET part_contract=? WHERE upload_id=?",
            [bytes.clone().into(), claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        assert_eq!(
            ipfs_s3_gateway::zip::recovery::run_page(
                db,
                &harness.state.kubo,
                &CancellationToken::new()
            )
            .await
            .unwrap(),
            1
        );
        let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        let requests = harness.kubo.received_requests().await.unwrap();
        let extra: Vec<_> = requests[calls..].iter().map(|r| r.url.path()).collect();
        let checks = [
            ("zero-rpc", extra.is_empty()),
            (
                "build-intent",
                after.builds.last().unwrap().status == "intent",
            ),
            (
                "capture",
                after.batch.captured_options == before.batch.captured_options,
            ),
            (
                "terminal",
                after.batch.terminal_result == before.batch.terminal_result,
            ),
            ("manifest", after.entries == before.entries),
            ("references", after.references == before.references),
            (
                "root-metadata",
                after.batch.root_status == before.batch.root_status
                    && after.batch.root_cid == before.batch.root_cid
                    && after.batch.root_error_code == before.batch.root_error_code,
            ),
            (
                "source",
                after.batch.source_published == before.batch.source_published,
            ),
            ("lease-quota", review::counters(&harness).await == counts),
            (
                "receipt-xml-headers",
                original_receipt_fields(db, &claim.batch_id).await == receipt,
            ),
            (
                "intake",
                format!(
                    "{:?}",
                    v2_zip::read_by_upload(db, &claim.batch_id)
                        .await
                        .unwrap()
                        .unwrap()
                ) == intake,
            ),
            (
                "execution",
                execution::read(db, &claim.batch_id).await.unwrap().unwrap() == execution,
            ),
            (
                "uploaded-bytes",
                harness.captured_add_file_bytes() == additions,
            ),
        ];
        let failures: Vec<_> = checks
            .iter()
            .filter_map(|(name, valid)| (!valid).then_some(*name))
            .collect();
        eprintln!(
            "part-shape={label} value={parts} extra-rpc={extra:?} build={} failures={failures:?}",
            after.builds.last().unwrap().status
        );
        if !failures.is_empty() {
            violations.push(format!("{label}: {failures:?}"));
        }
        let stored = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT part_contract FROM zip_v2_mpu_completions WHERE upload_id=?",
                [claim.batch_id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "part_contract")
            .unwrap();
        assert_eq!(stored, bytes);
    }
    assert!(
        violations.is_empty(),
        "all {total} persisted part variants reached; violations={violations:?}"
    );
}

#[tokio::test]
async fn writer_generated_part_shapes_preserve_root_recovery_and_opaque_strings() {
    use s3s::dto::{CompletedPart, ETag};
    let mut opaque = CompletedPart {
        part_number: Some(1),
        checksum_crc32: Some("".into()),
        checksum_crc32c: Some("雪☃".into()),
        checksum_crc64nvme: Some("not-base64".into()),
        checksum_sha1: Some(" ".into()),
        checksum_sha256: Some("opaque:/+=".into()),
        ..Default::default()
    };
    let mut variants = vec![(
        "original-strong-null-checksums",
        CompletedPart {
            part_number: Some(1),
            e_tag: Some(ETag::Strong("QmPart".into())),
            ..Default::default()
        },
    )];
    variants.push(("etag-null-all-checksum-strings", opaque.clone()));
    opaque.e_tag = Some(ETag::Strong("".into()));
    opaque.checksum_sha1 = None;
    variants.push(("strong-empty-mixed-checksums", opaque.clone()));
    opaque.e_tag = Some(ETag::Weak("雪☃".into()));
    opaque.checksum_sha1 = Some(" ".into());
    variants.push(("weak-unicode-all-checksum-strings", opaque));
    for (label, part) in variants {
        let (harness, _, claim) = review::published_mpu().await;
        let db = harness.state.store.db();
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
            [claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        let before = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        let receipt = original_receipt_fields(db, &claim.batch_id).await;
        let execution = execution::read(db, &claim.batch_id).await.unwrap().unwrap();
        let intake = format!(
            "{:?}",
            v2_zip::read_by_upload(db, &claim.batch_id)
                .await
                .unwrap()
                .unwrap()
        );
        let counts = review::counters(&harness).await;
        let calls = harness.kubo.received_requests().await.unwrap().len();
        let canonical = v2_zip::complete_request_contract(&[part]).unwrap();
        let parsed: Value = serde_json::from_str(&canonical).unwrap();
        // Root retry reads stored JSON semantically; capture/receipt bytes stay frozen.
        let stored = if label == "weak-unicode-all-checksum-strings" {
            let entries: Vec<_> = parsed[0]
                .as_object()
                .unwrap()
                .iter()
                .rev()
                .map(|(key, value)| format!("{}: {}", serde_json::to_string(key).unwrap(), value))
                .collect();
            format!("[\n {{ {} }} \n]", entries.join(",\n"))
        } else {
            canonical.clone()
        };
        assert_eq!(serde_json::from_str::<Value>(&stored).unwrap(), parsed);
        if label == "original-strong-null-checksums" {
            let original = db
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "SELECT part_contract FROM zip_v2_mpu_completions WHERE upload_id=?",
                    [claim.batch_id.clone().into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<String>("", "part_contract")
                .unwrap();
            assert_eq!(
                original, canonical,
                "real first native Complete emitted exactly this writer output"
            );
        } else {
            // Qualified shape probe only; never recapture or relabel history/options.
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE zip_v2_mpu_completions SET part_contract=? WHERE upload_id=?",
                [stored.clone().into(), claim.batch_id.clone().into()],
            ))
            .await
            .unwrap();
        }
        review::directory_rpc(&harness.kubo).await;
        assert_eq!(
            ipfs_s3_gateway::zip::recovery::run_page(
                db,
                &harness.state.kubo,
                &CancellationToken::new()
            )
            .await
            .unwrap(),
            1
        );
        let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        assert_eq!(after.batch.root_status, "complete");
        assert_eq!(after.batch.root_cid.as_deref(), Some(review::ROOT));
        assert_eq!(after.batch.captured_options, before.batch.captured_options);
        assert_eq!(after.entries, before.entries);
        assert_eq!(after.batch.source_published, before.batch.source_published);
        assert_eq!(review::counters(&harness).await, counts);
        assert_eq!(original_receipt_fields(db, &claim.batch_id).await, receipt);
        assert_eq!(
            execution::read(db, &claim.batch_id).await.unwrap().unwrap(),
            execution
        );
        assert_eq!(
            format!(
                "{:?}",
                v2_zip::read_by_upload(db, &claim.batch_id)
                    .await
                    .unwrap()
                    .unwrap()
            ),
            intake
        );
        let requests = harness.kubo.received_requests().await.unwrap();
        assert!(
            requests[calls..]
                .iter()
                .any(|r| r.url.path() == "/api/v0/dag/put")
        );
        assert!(
            requests[calls..]
                .iter()
                .all(|r| !["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&r.url.path()))
        );
        eprintln!(
            "writer-part={label} canonical={canonical} root=complete frozen-capture/receipt/intake/execution/manifest=true counters={counts:?}"
        );
    }
}

#[tokio::test]
async fn stripped_modern_mpu_capture_is_rejected_before_any_root_retry_rpc() {
    let (harness, snapshot, claim) = review::published_mpu().await;
    let db = harness.state.store.db();
    let original: Value = serde_json::from_str(&snapshot.batch.captured_options).unwrap();
    let bytes = original["options"].to_string();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE zip_batches SET captured_options=? WHERE id=?",
        vec![bytes.clone().into(), claim.batch_id.clone().into()],
    ))
    .await
    .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
        vec![claim.batch_id.clone().into()],
    ))
    .await
    .unwrap();
    let stripped = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
    assert!(
        zip::recovery::files(&stripped, &claim).is_ok(),
        "complete bare is a valid shape, not proof of original provenance"
    );
    let intake = v2_zip::read_by_upload(db, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    let execution = execution::read(db, &claim.batch_id).await.unwrap().unwrap();
    let before = review::counters(&harness).await;
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let additions = harness.captured_add_file_bytes();
    assert_eq!(
        ipfs_s3_gateway::zip::recovery::run_page(
            db,
            &harness.state.kubo,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        1
    );
    let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
    let requests = harness.kubo.received_requests().await.unwrap();
    let extra: Vec<_> = requests[calls..].iter().map(|r| r.url.path()).collect();
    eprintln!(
        "stripped-modern shape=Ok extra-rpc={extra:?} before={before:?} after={:?} build={} root={} capture-bytes-preserved={}",
        review::counters(&harness).await,
        after.builds.last().unwrap().status,
        after.batch.root_status,
        after.batch.captured_options == bytes
    );
    assert!(
        extra.is_empty(),
        "stripped original modern envelope must fail BEFORE Kubo: {extra:?}"
    );
    assert_eq!(review::counters(&harness).await, before);
    assert_eq!(harness.captured_add_file_bytes(), additions);
    assert_eq!(after.entries, snapshot.entries);
    assert_eq!(after.batch.captured_options, bytes);
    assert_eq!(after.batch.terminal_result, snapshot.batch.terminal_result);
    assert_eq!(
        after.batch.source_published,
        snapshot.batch.source_published
    );
    assert_eq!(after.batch.root_status, snapshot.batch.root_status);
    assert_eq!(after.batch.root_error_code, snapshot.batch.root_error_code);
    assert_eq!(after.batch.root_cid, snapshot.batch.root_cid);
    assert_eq!(after.builds.last().unwrap().status, "intent");
    let after_intake = v2_zip::read_by_upload(db, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_intake.captured_options, intake.captured_options);
    assert_eq!(after_intake.request_fingerprint, intake.request_fingerprint);
    assert_eq!(after_intake.execution_id, intake.execution_id);
    assert_eq!(after_intake.active_upload_id, intake.active_upload_id);
    assert_eq!(
        execution::read(db, &claim.batch_id).await.unwrap().unwrap(),
        execution
    );
}

#[tokio::test]
async fn missing_or_inconsistent_original_mpu_proof_never_invokes_root_io() {
    let mut violations = Vec::new();
    for change in [
        "MISSING_INTAKE",
        "DELETE FROM zip_v2_mpu_completions WHERE upload_id=?",
        "UPDATE zip_v2_executions SET state='fenced' WHERE id=?",
        "UPDATE zip_v2_executions SET owner='another' WHERE id=?",
        "UPDATE zip_v2_executions SET terminal_result='{}' WHERE id=?",
        "UPDATE zip_v2_mpu_completions SET part_contract='{}' WHERE upload_id=?",
        "UPDATE zip_v2_mpu_completions SET response_headers='{}' WHERE upload_id=?",
        "DELETE_BOTH",
        "DELETE_BOTH_STRIPPED",
    ] {
        let (harness, snapshot, claim) = review::published_mpu().await;
        let db = harness.state.store.db();
        let mut expected_capture = snapshot.batch.captured_options.clone();
        if change == "DELETE_BOTH_STRIPPED" {
            let captured: Value = serde_json::from_str(&expected_capture).unwrap();
            expected_capture = captured["options"].to_string();
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE zip_batches SET captured_options=? WHERE id=?",
                [
                    expected_capture.clone().into(),
                    claim.batch_id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        }
        if matches!(
            change,
            "DELETE_BOTH" | "DELETE_BOTH_STRIPPED" | "MISSING_INTAKE"
        ) {
            let mut deletes = vec![
                "DELETE FROM zip_v2_mpu_completions WHERE upload_id=?",
                "DELETE FROM zip_v2_mpu_intakes WHERE original_upload_id=?",
            ];
            if change.starts_with("DELETE_BOTH") {
                deletes.extend([
                    "DELETE FROM zip_v2_targets WHERE batch_id=?",
                    "DELETE FROM zip_v2_manifest WHERE batch_id=?",
                    "DELETE FROM zip_v2_executions WHERE id=?",
                ]);
            }
            for sql in deletes {
                db.execute(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    sql,
                    [claim.batch_id.clone().into()],
                ))
                .await
                .unwrap();
            }
        } else {
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                change,
                [claim.batch_id.clone().into()],
            ))
            .await
            .unwrap();
        }
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
            [claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        let before = review::counters(&harness).await;
        let calls = harness.kubo.received_requests().await.unwrap().len();
        assert_eq!(
            ipfs_s3_gateway::zip::recovery::run_page(
                db,
                &harness.state.kubo,
                &CancellationToken::new()
            )
            .await
            .unwrap(),
            1
        );
        let requests = harness.kubo.received_requests().await.unwrap();
        let extra: Vec<_> = requests[calls..].iter().map(|r| r.url.path()).collect();
        let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
        eprintln!(
            "paired-proof={change} extra-rpc={extra:?} build={} counters={:?}",
            after.builds.last().unwrap().status,
            review::counters(&harness).await
        );
        if !extra.is_empty() {
            violations.push(change);
        }
        assert_eq!(review::counters(&harness).await, before);
        assert_eq!(after.batch.captured_options, expected_capture);
        assert_eq!(after.batch.terminal_result, snapshot.batch.terminal_result);
        assert_eq!(after.entries, snapshot.entries);
        assert_eq!(after.batch.root_cid, snapshot.batch.root_cid);
        assert_eq!(after.batch.root_status, snapshot.batch.root_status);
        assert_eq!(after.batch.root_error_code, snapshot.batch.root_error_code);
        assert_eq!(after.references, snapshot.references);
        assert_eq!(
            after.batch.source_published,
            snapshot.batch.source_published
        );
        assert_eq!(after.builds.last().unwrap().status, "intent");
    }
    assert!(
        violations.is_empty(),
        "all proof variants reached: {violations:?}"
    );
}

#[tokio::test]
async fn native_mpu_foreign_keys_refuse_an_intake_with_missing_execution() {
    let (harness, snapshot, claim) = review::published_mpu().await;
    let db = harness.state.store.db();
    let before = review::counters(&harness).await;
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let error = db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM zip_v2_executions WHERE id=?",
            [claim.batch_id.clone().into()],
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("FOREIGN KEY"), "{error}");
    let error = db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_v2_mpu_completions SET part_contract='' WHERE upload_id=?",
            [claim.batch_id.clone().into()],
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("CHECK constraint"), "{error}");
    assert!(
        execution::read(db, &claim.batch_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        v2_zip::read_by_upload(db, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .captured_options,
        snapshot.batch.captured_options
    );
    assert_eq!(review::counters(&harness).await, before);
    assert_eq!(harness.kubo.received_requests().await.unwrap().len(), calls);
    eprintln!(
        "missing-execution-with-intake native-foreign-key-refused=true empty-parts-check-refused=true no-constraint-override=true"
    );
}

#[tokio::test]
async fn completed_source_version_deletion_does_not_revoke_original_root_recovery() {
    let (harness, snapshot, claim) = review::published_mpu_with_source(true).await;
    let db = harness.state.store.db();
    let terminal: Value =
        serde_json::from_str(snapshot.batch.terminal_result.as_deref().unwrap()).unwrap();
    let source_version = terminal["source_version_row_id"].as_str().unwrap();
    let source = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT object_id FROM object_versions WHERE id=?",
            [source_version.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "object_id")
        .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM object_versions WHERE id=?",
        [source_version.into()],
    ))
    .await
    .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM objects WHERE id=?",
        [source.into()],
    ))
    .await
    .unwrap();
    let before = review::counters(&harness).await;
    let calls = harness.kubo.received_requests().await.unwrap().len();
    let execution = execution::read(db, &claim.batch_id).await.unwrap().unwrap();
    review::directory_rpc(&harness.kubo).await;
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE zip_root_builds SET lease_until='2000-01-01T00:00:00Z' WHERE batch_id=?",
        [claim.batch_id.clone().into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        ipfs_s3_gateway::zip::recovery::run_page(
            db,
            &harness.state.kubo,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        1
    );
    let after = zip::snapshot(db, &claim.batch_id).await.unwrap().unwrap();
    assert_eq!(after.batch.root_status, "complete");
    assert_eq!(after.batch.root_cid.as_deref(), Some(review::ROOT));
    assert!(after.batch.source_published);
    assert_eq!(
        after.batch.captured_options,
        snapshot.batch.captured_options
    );
    assert_eq!(after.entries, snapshot.entries);
    assert_eq!(
        execution::read(db, &claim.batch_id).await.unwrap().unwrap(),
        execution
    );
    assert_eq!(review::counters(&harness).await, before);
    let requests = harness.kubo.received_requests().await.unwrap();
    assert!(
        requests[calls..]
            .iter()
            .all(|r| !["/api/v0/add", "/api/v0/cat", "/api/v0/pin/rm"].contains(&r.url.path()))
    );
    let mut result: Value =
        serde_json::from_str(after.batch.terminal_result.as_deref().unwrap()).unwrap();
    let mut original = terminal;
    for field in ["root_status", "root_warning", "root_cid"] {
        result.as_object_mut().unwrap().remove(field);
        original.as_object_mut().unwrap().remove(field);
    }
    assert_eq!(result, original);
    eprintln!(
        "deleted-original-source-version root=complete source_published=true capture/entries/execution/source-metadata-frozen=true counters={before:?}"
    );
}

#[tokio::test]
async fn first_native_historical_bare_mpu_complete_binding_still_recovers_root() {
    use ipfs_s3_gateway::{kubo::KuboClient, store};
    use sea_orm::TransactionTrait;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let capture = json!({"publish_source":false,"publish_extracted":true,"targets":"extracted",
        "token":"historical-root","root_override":null,"root_enabled":true,"result_version":2})
    .to_string();
    zip::admit(
        &db,
        &zip::BatchAdmission {
            id: "historical-root".into(),
            owner: "owner".into(),
            source: "mpu".into(),
            token: "historical-root".into(),
            fingerprint: "original-native-admission".into(),
            bucket: "bucket".into(),
            archive_key: "archive.zip".into(),
            input_identity: "historical-root".into(),
            captured_options: capture.clone(),
        },
    )
    .await
    .unwrap();
    let parts = [s3s::dto::CompletedPart {
        part_number: Some(1),
        e_tag: Some(s3s::dto::ETag::Strong("QmPart".into())),
        ..Default::default()
    }];
    zip::BatchAdmission::capture_mpu_complete(
        &db,
        "owner",
        "bucket",
        "archive.zip",
        "historical-root",
        &parts,
    )
    .await
    .unwrap();
    zip::BatchAdmission::capture_prepared_archive(
        &db,
        "owner",
        "bucket",
        "archive.zip",
        "historical-root",
        &parts,
        "QmArchive",
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        &db,
        "historical-root",
        &[zip::ManifestItem::Success {
            path: "file.txt".into(),
            object_key: "out/file.txt".into(),
            cid: LEAF.into(),
            size: 0,
        }],
    )
    .await
    .unwrap();
    db.execute_unprepared(&format!("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('original','bucket','out/file.txt','{LEAF}',0,'{LEAF}')")).await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('original-version','bucket','out/file.txt','object','original',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let tx = db.begin().await.unwrap();
    let version = zip::binding_for_published_object(&tx, "file.txt", "original")
        .await
        .unwrap();
    assert_eq!(version.version_row_id, "original-version");
    zip::publish(&tx,"historical-root",&[version],false,"{\"archive_cid\":\"QmArchive\",\"root_status\":\"failed\",\"root_warning\":\"directory_build_failed\",\"root_cid\":null}",zip::RootOutcome::Failed {code:"directory_build_failed"}).await.unwrap();
    tx.commit().await.unwrap();
    db.execute_unprepared(
        "UPDATE zip_batches SET updated_at='2000-01-01T00:00:00Z' WHERE id='historical-root'",
    )
    .await
    .unwrap();
    let before = zip::snapshot(&db, "historical-root")
        .await
        .unwrap()
        .unwrap();
    let counts = review::db_counters(&db).await;
    assert!(
        execution::read(&db, "historical-root")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        v2_zip::read_by_upload(&db, "historical-root")
            .await
            .unwrap()
            .is_none()
    );
    let kubo = wiremock::MockServer::start().await;
    review::directory_rpc(&kubo).await;
    assert_eq!(
        ipfs_s3_gateway::zip::recovery::run_page(
            &db,
            &KuboClient::new(kubo.uri()),
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        1
    );
    let after = zip::snapshot(&db, "historical-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch.root_status, "complete");
    assert_eq!(after.batch.root_cid.as_deref(), Some(review::ROOT));
    assert_eq!(after.batch.captured_options, capture);
    assert_eq!(after.entries, before.entries);
    assert!(!after.batch.source_published);
    assert_eq!(review::db_counters(&db).await, counts);
    assert!(
        kubo.received_requests().await.unwrap().iter().all(|r| ![
            "/api/v0/add",
            "/api/v0/cat",
            "/api/v0/pin/rm"
        ]
        .contains(&r.url.path()))
    );
    eprintln!(
        "historical-first-native-bare root=complete original_complete/prepared/bound-version=true no-capture-upgrade=true counters={counts:?}"
    );
}
