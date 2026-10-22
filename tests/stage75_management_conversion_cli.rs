use ipfs_s3_gateway::{
    pinning::management::{
        Summary,
        wire::{conversion, v3},
    },
    store::pinning::management::native,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Output},
};

fn legacy_document() -> Vec<u8> {
    let route = json!({"provider_id":"historical-provider","backend":"pinata","scope":"foreign-scope","storage_domain":"original-domain","api_profile":"pinata-v3","strategy":"cid","resource_type":"hosted_file","credential_revision":1,"endpoint_revision":2,"original":null});
    let request =
        json!({"item_id":"retire","action":"retire","subject":{"provider":{"route":route}}});
    let inventory = json!({"provider_id":"historical-provider","frontier":[{"table":"pin_jobs","values":["original-job"]}],"complete":true,"allocation_stopped":false,"attribution_complete":true,"leases":[],"root_reference_ids":[]});
    let entry = json!({"id":"management-v1:original-plan:retire","request":request,"status":"ready","capture":{"provider":{"inventory":inventory,"routes":[],"targets":[],"remotes":[],"historical_remotes":[]}},"step":{"retire":{"stop_new_allocations":true,"needs_attention":false,"leases":0,"retained":0,"unknown":0,"inflight":0,"cleanup_pending":0,"root_references":0}}});
    let summary = Summary {
        items: 1,
        ..Default::default()
    };
    let document = json!({"schema_version":2,"applied_trust":"untrusted_claims_only","request":{"plan_id":"original-plan","items":[request]},"snapshot":{"observed_at":123,"database_now":"2020-01-01T00:00:00Z","configuration":null,"objects":[],"routes":[],"targets":[],"remotes":[],"historical_remotes":[],"unknown_resources":[],"ledger_domains":[],"inventories":[inventory],"applied":[{"binding":entry}]},"native":null,"plan":{"items":[entry],"summary":summary},"confirmation":[],"migration_points":[]});
    // Preserve whitespace and spelling, not merely an equivalent reserialization.
    format!(" \n{}\n", serde_json::to_string_pretty(&document).unwrap()).into_bytes()
}

fn with_native(before_root_release: bool) -> Vec<u8> {
    let mut document: Value = serde_json::from_slice(&legacy_document()).unwrap();
    let mut tables = serde_json::to_value(native::registry()).unwrap();
    if before_root_release {
        let root = tables
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|t| t["table"] == "zip_root_references")
            .unwrap();
        let columns = [
            "batch_id",
            "cid",
            "created_at",
            "epoch",
            "node_identity",
            "revision",
            "state",
            "tier",
            "updated_at",
            "verification_receipt",
        ];
        root["columns"] = json!(columns);
        root["definitions"] = Value::Array(columns.iter().map(|name| {
            let (sqlite, postgres) = match *name {
                "revision" | "epoch" => ("BIGINT", "bigint"),
                "created_at" | "updated_at" => ("TIMESTAMP", "timestamp with time zone"),
                _ => ("TEXT", "text"),
            };
            json!({"name":name,"sqlite_type":sqlite,"postgres_type":postgres,"nullable":*name == "verification_receipt"})
        }).collect());
    }
    let records = tables
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|t| t["table"] == "pin_management_records")
        .unwrap();
    let mut row: serde_json::Map<String, Value> = records["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|column| (column.as_str().unwrap().into(), Value::Null))
        .collect();
    for (column, value) in [
        ("database_scope", "foreign-scope"),
        ("plan_id", "foreign-plan"),
        ("item_id", "foreign-item"),
        ("kind", "action"),
        ("native_identity", "foreign-db"),
        ("principal", "original-principal"),
        ("binding", "original ACK+binding"),
        ("native_before", "PRIVATE_FOREIGN_原文\n  \u{0}"),
        ("created_at", "2020-01-01 01:02:03.000001+00:00"),
        ("expires_at", "2020-01-01 01:03:03.000002+00:00"),
        ("result", "original claimed result"),
    ] {
        row.insert(column.into(), value.into());
    }
    row.insert("native_before".into(), private_text().into());
    records["rows"] = json!([row]);
    // Historical catalog identity is part of the input protocol, not the local
    // runtime's migration list (which can acquire later versions).
    let mut versions: Vec<String> = "m20250701_000001_init
        m20260707_000001_decompress_zip m20260720_000001_sse_c_key_fingerprint
        m20260721_000001_multi_provider_pinning m20260729_000001_ipfs3_import
        m20260729_000002_postgres_utc_timestamps m20260730_000001_standard_mutation_fence
        m20260813_000001_postgres_json_columns m20260825_000001_object_versioning
        m20260826_000001_lifecycle_expiration m20260831_000001_bucket_cors
        m20260901_000001_lifecycle_abort_multipart m20260912_000001_residency_references
        m20260912_000002_lifecycle_transition m20260919_000001_standard_mutation_lease
        m20260920_000001_pin_submit_history m20260920_000002_pin_identity_ledger
        m20260920_000003_pin_extension_decision m20260920_000004_multipart_pin_decision
        m20260920_000005_import_pin_decision m20260920_000006_pin_submit_correlation
        m20260927_000001_zip_batches m20260927_000002_import_zip_root_capture
        m20260927_000003_zip_mpu_replay m20260927_000004_zip_v2_execution
        m20260927_000005_zip_v2_import m20260927_000006_zip_v2_mpu_intake
        m20260927_000007_zip_v2_mpu_completion m20261004_000001_rpc_submission_ledger
        m20261004_000002_cluster_replica_contract m20261007_000001_legacy_pinning_migration
        m20261008_000001_zip_intent_capture m20261012_000001_pinning_management
        m20261013_000001_zip_root_logical_release"
        .split_whitespace()
        .map(str::to_owned)
        .collect();
    let last = if before_root_release {
        "m20261012_000001_pinning_management"
    } else {
        "m20261013_000001_zip_root_logical_release"
    };
    versions.truncate(versions.iter().position(|v| v == last).unwrap() + 1);
    document["native"] = json!({"schema_versions":versions,"database_scope":"original-file-scope","database_identity":"original-file-db","tables":tables,"frontier":[{"table":"pin_management_records","values":["foreign-scope","foreign-plan","foreign-item","action"]}]});
    serde_json::to_vec_pretty(&document).unwrap()
}

fn private_text() -> String {
    format!(
        "PRIVATE_FOREIGN_原文\n  \u{0}{}",
        "界".repeat(v3::CELL_BYTES)
    )
}

fn recover(path: &Path) -> conversion::RecoveredV2 {
    conversion::recover_v3(
        std::fs::File::open(path).unwrap(),
        v3::files::stream::budget(),
    )
    .unwrap()
    .0
}

fn run(dir: &Path, args: &[&str]) -> Output {
    let rpc = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    rpc.set_nonblocking(true).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
        .args(args)
        .current_dir(dir)
        .env("IPFS_S3_CONFIG", dir.join("invalid.toml"))
        .env(
            "IPFS_S3_DATABASE_URL",
            format!(
                "sqlite://{}?mode=rwc",
                dir.join("must-not-exist.db").display()
            ),
        )
        .env(
            "IPFS_S3_KUBO_RPC_URL",
            format!("http://{}", rpc.local_addr().unwrap()),
        )
        .env("TMP", dir)
        .env("TEMP", dir)
        .output()
        .unwrap();
    assert!(matches!(rpc.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock));
    output
}

#[test]
fn normal_v2_is_rejected_but_explicit_file_only_conversion_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("invalid.toml"), "invalid config [PRIVATE").unwrap();
    let input = dir.path().join("old.json");
    let output = dir.path().join("converted.v3");
    std::fs::write(&input, legacy_document()).unwrap();
    let rejected = run(
        dir.path(),
        &[
            "--pinning-manage",
            "--import",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ],
    );
    assert!(!rejected.status.success());
    assert!(!output.exists());
    let converted = run(
        dir.path(),
        &[
            "--pinning-manage",
            "--convert-v2",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
            "--private-recovery",
        ],
    );
    assert!(
        converted.status.success(),
        "{}",
        String::from_utf8_lossy(&converted.stderr)
    );
    let report: Value = serde_json::from_slice(&converted.stdout).unwrap();
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["artifact_trust"], "untrusted_claims_only");
    assert!(std::fs::read(&output).unwrap().starts_with(v3::MAGIC));
    assert!(!dir.path().join("must-not-exist.db").exists());
    let original = recover(&output);
    assert_eq!(original.original(), legacy_document());
    assert_eq!(original.document().request.plan_id, "original-plan");
    assert_eq!(original.document().snapshot.applied.len(), 1);
    assert!(
        matches!(&original.document().plan.items[0].capture, Some(ipfs_s3_gateway::store::initialization::history::legacy::Capture::Provider(capture)) if capture.inventory.frontier[0].values == ["original-job"])
    );
    let (head, _, _) = v3::document::inspect(
        std::fs::File::open(&output).unwrap(),
        v3::Projection::PrivateRecovery,
        v3::files::stream::budget(),
    )
    .unwrap();
    assert!(head.request.is_none() && head.acknowledgements.is_none() && head.expires_at.is_none());
    assert_eq!(head.created_at, conversion::NOT_RECORDED);
    assert_eq!(head.principal, conversion::NOT_RECORDED);
    for operation in ["--apply", "--status", "--review"] {
        assert!(
            !run(
                dir.path(),
                &["--pinning-manage", operation, output.to_str().unwrap()]
            )
            .status
            .success()
        );
    }
    assert!(
        run(
            dir.path(),
            &[
                "--pinning-manage",
                "--review",
                output.to_str().unwrap(),
                "--private-input"
            ]
        )
        .status
        .success()
    );
    let before = std::fs::read(&output).unwrap();
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--convert-v2",
                input.to_str().unwrap(),
                "--output",
                output.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert_eq!(std::fs::read(&output).unwrap(), before);
    let missing_parent = dir.path().join("missing").join("no.v3");
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--convert-v2",
                input.to_str().unwrap(),
                "--output",
                missing_parent.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert!(!dir.path().join("missing").exists());
}

#[test]
fn private_recovery_preserves_both_historical_root_catalogs_and_foreign_null_cells() {
    for before_root_release in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("old.json");
        let output = dir.path().join("converted.v3");
        let bytes = with_native(before_root_release);
        std::fs::write(&input, &bytes).unwrap();
        let converted = run(
            dir.path(),
            &[
                "--pinning-manage",
                "--convert-v2",
                input.to_str().unwrap(),
                "--private-recovery",
                "--output",
                output.to_str().unwrap(),
            ],
        );
        assert!(
            converted.status.success(),
            "{}",
            String::from_utf8_lossy(&converted.stderr)
        );
        let recovered = recover(&output);
        assert_eq!(recovered.original(), bytes);
        let original = recovered.document().native.as_ref().unwrap();
        assert_eq!(original.database_identity, "original-file-db");
        let row = &original.table("pin_management_records").unwrap().rows[0];
        assert_eq!(
            row["native_before"].as_deref(),
            Some(private_text().as_str())
        );
        assert_eq!(row["completed_at"], None);
        assert_eq!(row["principal"].as_deref(), Some("original-principal"));
        assert_eq!(
            row["expires_at"].as_deref(),
            Some("2020-01-01 01:03:03.000002+00:00")
        );
        assert_eq!(
            row["created_at"].as_deref(),
            Some("2020-01-01 01:02:03.000001+00:00")
        );
        let public = dir.path().join("public.v3");
        assert!(
            run(
                dir.path(),
                &[
                    "--pinning-manage",
                    "--export",
                    output.to_str().unwrap(),
                    "--private-input",
                    "--output",
                    public.to_str().unwrap()
                ]
            )
            .status
            .success()
        );
        assert!(
            run(
                dir.path(),
                &["--pinning-manage", "--review", public.to_str().unwrap()]
            )
            .status
            .success()
        );
        let public_bytes = std::fs::read(&public).unwrap();
        assert!(!String::from_utf8_lossy(&public_bytes).contains("PRIVATE_FOREIGN"));
        assert!(
            conversion::recover_v3(public_bytes.as_slice(), v3::files::stream::budget()).is_err()
        );
        assert!(
            !run(
                dir.path(),
                &["--pinning-manage", "--apply", public.to_str().unwrap()]
            )
            .status
            .success()
        );
        let copied = dir.path().join("private-copy.v3");
        assert!(
            run(
                dir.path(),
                &[
                    "--pinning-manage",
                    "--import",
                    output.to_str().unwrap(),
                    "--private-recovery",
                    "--output",
                    copied.to_str().unwrap()
                ]
            )
            .status
            .success()
        );
        assert_eq!(recover(&copied).original(), bytes);
        assert!(!dir.path().join("must-not-exist.db").exists());
    }
}

#[test]
fn invalid_legacy_documents_and_missing_opt_in_never_publish_an_archive() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("bad.json");
    let output = dir.path().join("not-created.v3");
    std::fs::write(&input, legacy_document()).unwrap();
    for extra in [
        vec![],
        vec!["--private-input"],
        vec!["--private-recovery", "--private-recovery"],
        vec!["--private-recovery", "--ttl-seconds", "900"],
    ] {
        let mut args = vec![
            "--pinning-manage",
            "--convert-v2",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ];
        args.extend(extra);
        assert!(!run(dir.path(), &args).status.success());
        assert!(!output.exists());
    }
    let good = legacy_document();
    let mut unknown: Value = serde_json::from_slice(&good).unwrap();
    unknown["unexpected"] = json!(true);
    let mut nested = unknown.clone();
    nested.as_object_mut().unwrap().remove("unexpected");
    nested["plan"]["items"][0]["capture"]["provider"]["unexpected"] = json!(true);
    let mut wrong_version = serde_json::from_slice::<Value>(&good).unwrap();
    wrong_version["schema_version"] = json!(999);
    let duplicate = String::from_utf8(good.clone())
        .unwrap()
        .replacen(
            "\"schema_version\": 2",
            "\"schema_version\":2,\"schema_version\":2",
            1,
        )
        .into_bytes();
    let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65)).into_bytes();
    let too_many_nodes = format!("[{}0]", "0,".repeat(131072)).into_bytes();
    let mut duplicate_map = with_native(false);
    duplicate_map = String::from_utf8(duplicate_map)
        .unwrap()
        .replacen(
            "\"principal\": \"original-principal\"",
            "\"principal\":\"original-principal\",\"principal\":null",
            1,
        )
        .into_bytes();
    for bytes in [
        serde_json::to_vec(&unknown).unwrap(),
        serde_json::to_vec(&nested).unwrap(),
        serde_json::to_vec(&wrong_version).unwrap(),
        duplicate,
        duplicate_map,
        deep,
        too_many_nodes,
        b"{not-json".to_vec(),
        vec![0xff],
        vec![b' '; 16 * 1024 * 1024 + 1],
    ] {
        std::fs::write(&input, bytes).unwrap();
        assert!(
            !run(
                dir.path(),
                &[
                    "--pinning-manage",
                    "--convert-v2",
                    input.to_str().unwrap(),
                    "--output",
                    output.to_str().unwrap(),
                    "--private-recovery"
                ]
            )
            .status
            .success()
        );
        assert!(!output.exists());
    }
}

#[test]
fn rewritten_claims_remain_data_and_corrupt_trailers_cannot_be_reviewed_or_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let mut document: Value = serde_json::from_slice(&legacy_document()).unwrap();
    document["snapshot"]["applied"][0]["binding"]["id"] = json!("FORGED-LOCAL-RECEIPT");
    let input = dir.path().join("claims.json");
    let output = dir.path().join("claims.v3");
    std::fs::write(&input, serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(
        run(
            dir.path(),
            &[
                "--pinning-manage",
                "--convert-v2",
                input.to_str().unwrap(),
                "--output",
                output.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert_eq!(
        recover(&output).document().snapshot.applied[0]
            .binding
            .id
            .as_str(),
        "FORGED-LOCAL-RECEIPT"
    );
    let review = dir.path().join("review.v3");
    assert!(
        run(
            dir.path(),
            &[
                "--pinning-manage",
                "--export",
                output.to_str().unwrap(),
                "--output",
                review.to_str().unwrap(),
                "--private-input"
            ]
        )
        .status
        .success()
    );
    assert!(
        !run(
            dir.path(),
            &["--pinning-manage", "--apply", review.to_str().unwrap()]
        )
        .status
        .success()
    );
    let mut bytes = std::fs::read(&output).unwrap();
    let length = bytes.len();
    // Alter the trailer digest while retaining valid JSON and framing.
    let digest = bytes
        .windows(10)
        .rposition(|part| part == b"\"sha256\":\"")
        .unwrap()
        + 10;
    bytes[digest] = if bytes[digest] == b'0' { b'1' } else { b'0' };
    assert!(conversion::recover_v3(bytes.as_slice(), v3::files::stream::budget()).is_err());
    let corrupt = dir.path().join("corrupt.v3");
    std::fs::write(&corrupt, &bytes).unwrap();
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--review",
                corrupt.to_str().unwrap(),
                "--private-input"
            ]
        )
        .status
        .success()
    );
    assert!(conversion::recover_v3(&bytes[..length - 1], v3::files::stream::budget()).is_err());
    assert!(!dir.path().join("must-not-exist.db").exists());
}
