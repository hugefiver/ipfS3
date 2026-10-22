use ipfs_s3_gateway::pinning::management::wire::v3::files;
use ipfs_s3_gateway::{
    pinning::management::{NativeKey, wire::v3::*},
    store::pinning::management::native::meter::{Budget, Limits},
};
use std::{
    fs::File,
    io::Write,
    path::Path,
    process::{Command, Output},
};
fn budget() -> Budget {
    Budget::new(Limits {
        bytes: 512 * 1024 * 1024,
        rows: 100_000,
        resident: 32 * 1024 * 1024,
        ..Default::default()
    })
}
fn archive(path: &Path, mode: Projection, pieces: usize) {
    let mut w = files::stream::writer(path.to_str().unwrap(), mode, budget()).unwrap();
    for frame in [
        Frame::Header {
            schema_version: 3,
            projection: mode,
            database_scope: "frozen-scope".into(),
            database_identity: "frozen-identity".into(),
            principal: "frozen-principal".into(),
            applied_trust: AppliedTrust::UntrustedClaimsOnly,
            catalog: vec!["original-catalog".into()],
            created_at: "original exact clock".into(),
            expires_at: Some("original expiry".into()),
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
        w.frame(&frame).unwrap();
    }
    w.frame(&Frame::Row {
        section: Section::Native,
        key: NativeKey {
            table: "objects".into(),
            values: vec!["original".into()],
        },
        cells: 2,
    })
    .unwrap();
    w.frame(&Frame::Cell {
        column: "metadata".into(),
    })
    .unwrap();
    w.cell_output().write_all(b"PRIVATE_RECOVERY_CELL").unwrap();
    w.frame(&Frame::CellEnd {
        value: CellValue::Text,
    })
    .unwrap();
    w.frame(&Frame::Cell {
        column: "public_fixture".into(),
    })
    .unwrap();
    for _ in 0..pieces {
        w.cell_output().write_all(&[b' '; CELL_BYTES]).unwrap();
    }
    w.frame(&Frame::CellEnd {
        value: CellValue::Text,
    })
    .unwrap();
    w.frame(&Frame::RowEnd).unwrap();
    files::stream::finish(w).unwrap();
}
fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
        .args(args)
        .current_dir(dir)
        .env("IPFS_S3_CONFIG", dir.join("invalid.toml"))
        .env("TMP", dir)
        .env("TEMP", dir)
        .output()
        .unwrap()
}
fn valid(path: &Path, mode: Projection) {
    let mut reader = Reader::new(File::open(path).unwrap(), mode, budget()).unwrap();
    while reader.next_frame().unwrap().is_some() {}
    assert!(reader.verified());
}
#[test]
fn real_file_commands_stream_large_frozen_originals_without_config_or_authority() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("invalid.toml"), "INVALID PRIVATE_CONFIG").unwrap();
    let source = dir.path().join("source.v3");
    archive(&source, Projection::PrivateRecovery, 257);
    let recovered = dir.path().join("recovered.v3");
    let imported = run(
        dir.path(),
        &[
            "--pinning-manage",
            "--import",
            source.to_str().unwrap(),
            "--output",
            recovered.to_str().unwrap(),
            "--private-recovery",
        ],
    );
    assert!(
        imported.status.success(),
        "{}",
        String::from_utf8_lossy(&imported.stderr)
    );
    assert_eq!(
        std::fs::read(&source).unwrap(),
        std::fs::read(&recovered).unwrap()
    );
    valid(&recovered, Projection::PrivateRecovery);
    let report: serde_json::Value = serde_json::from_slice(&imported.stdout).unwrap();
    assert_eq!(report["database_writes"], false);
    assert_eq!(report["artifact_trust"], "untrusted_claims_only");
    let public = dir.path().join("public.v3");
    let exported = run(
        dir.path(),
        &[
            "--pinning-manage",
            "--export",
            recovered.to_str().unwrap(),
            "--output",
            public.to_str().unwrap(),
            "--private-input",
        ],
    );
    assert!(
        exported.status.success(),
        "{}",
        String::from_utf8_lossy(&exported.stderr)
    );
    valid(&public, Projection::Review);
    assert!(
        !std::fs::read(&public)
            .unwrap()
            .windows(b"PRIVATE_RECOVERY_CELL".len())
            .any(|s| s == b"PRIVATE_RECOVERY_CELL")
    );
    let review = run(
        dir.path(),
        &["--pinning-manage", "--review", public.to_str().unwrap()],
    );
    assert!(review.status.success());
    assert!(!String::from_utf8_lossy(&review.stdout).contains("PRIVATE_RECOVERY_CELL"));
}
#[test]
fn malformed_private_and_conflicting_outputs_cannot_be_published() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("invalid.toml"), "INVALID PRIVATE_CONFIG").unwrap();
    let source = dir.path().join("source.v3");
    archive(&source, Projection::PrivateRecovery, 0);
    assert!(
        !run(
            dir.path(),
            &["--pinning-manage", "--review", source.to_str().unwrap()]
        )
        .status
        .success()
    );
    let existing = dir.path().join("existing.v3");
    std::fs::write(&existing, b"USER_CONTENT").unwrap();
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--export",
                source.to_str().unwrap(),
                "--output",
                existing.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert_eq!(std::fs::read(&existing).unwrap(), b"USER_CONTENT");
    let missing = dir.path().join("missing").join("out.v3");
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--export",
                source.to_str().unwrap(),
                "--output",
                missing.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert!(!missing.parent().unwrap().exists());
    let corrupt = dir.path().join("corrupt.v3");
    let mut bytes = std::fs::read(&source).unwrap();
    bytes.pop();
    std::fs::write(&corrupt, bytes).unwrap();
    let out = dir.path().join("must-not-publish.v3");
    assert!(
        !run(
            dir.path(),
            &[
                "--pinning-manage",
                "--import",
                corrupt.to_str().unwrap(),
                "--output",
                out.to_str().unwrap(),
                "--private-recovery"
            ]
        )
        .status
        .success()
    );
    assert!(!out.exists());
}
