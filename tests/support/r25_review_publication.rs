use super::*;
use ipfs_s3_gateway::{
    config::{DecompressZipConfig, OptionalPinControlMode},
    pinning::{
        decision::DecisionOrigin,
        zip_policy::{CapturedZipIntent, ZipCaptureContext, capture_zip_intent},
    },
    zip::options::{self, parse_authenticated_zip_controls},
};
use std::collections::BTreeMap;

pub(super) fn capture(root_enabled: bool, targets: ZipTargets) -> CapturedZipIntent {
    let automatic = targets == ZipTargets::Extracted;
    let config = if automatic {
        pin_config()
    } else {
        ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap()
    };
    let zip = DecompressZipConfig {
        auto_pin_outputs: Some(automatic),
        unixfs_directory_root: root_enabled,
        pin_output_rules: if automatic {
            output_rule_config(&config)
        } else {
            vec![]
        },
        ..Default::default()
    };
    let mut headers = http::HeaderMap::new();
    headers.insert(options::ZIP_CONTRACT, "v2".parse().unwrap());
    headers.insert(options::ZIP_TOKEN, "v2-token".parse().unwrap());
    headers.insert(options::PUBLISH_SOURCE, "false".parse().unwrap());
    headers.insert(
        options::ZIP_TARGETS,
        if automatic { "extracted" } else { "none" }
            .parse()
            .unwrap(),
    );
    let controls = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        root_enabled,
    )
    .unwrap()
    .unwrap();
    capture_zip_intent(
        &config,
        &zip,
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "source.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("principal", "v2-execution"),
    )
    .unwrap()
}

pub(super) async fn historical_revision_only(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    targets: ZipTargets,
    revision: Option<&str>,
) -> execution::Claim {
    // These are original pre-R25 options/revision bytes, not a current capture
    // dressed up as historical permission. No provider/policy evidence exists.
    let envelope = serde_json::json!({"options": {
        "publish_source":false,"publish_extracted":true,"targets":targets,"token":"v2-token",
        "root_override":null,"root_enabled":false,"result_version":2
    }, "rule_revision": revision})
    .to_string();
    prepare_with_envelope(db, successes, &[], envelope).await
}

pub(super) async fn prepare_with_envelope(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    failures: &[(&str, &str)],
    captured_options: String,
) -> execution::Claim {
    let contract = "{\"contract\":\"v2\"}";
    let admission = execution::Admission {
        id: "v2-execution".into(),
        owner: "principal".into(),
        source: "direct".into(),
        token: "v2-token".into(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
        request_contract: contract.into(),
        bucket: "bucket".into(),
        source_key: "source.zip".into(),
        captured_options,
    };
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    // The native intake writes signed identity and the complete first capture
    // together, before claim/input/manifest admission. Never backfill history.
    execution::admit_in_transaction(&tx, &admission)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let claim = execution::claim(db, &admission.id, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &"a".repeat(64), "input-cid", 99)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let items = successes
        .iter()
        .map(|(path, key, cid)| execution::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .chain(
            failures
                .iter()
                .map(|(path, code)| execution::ManifestItem::Failure {
                    path: (*path).into(),
                    code: (*code).into(),
                }),
        )
        .collect::<Vec<_>>();
    let mutations = successes
        .iter()
        .map(|(_, key, _)| ((*key).into(), format!("guard-{key}")))
        .collect::<BTreeMap<_, _>>();
    execution::admit_manifest_in_transaction(&tx, &claim, &items, &mutations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    zip::admit(
        db,
        &zip::BatchAdmission {
            id: admission.id,
            owner: admission.owner,
            source: admission.source,
            token: admission.token,
            fingerprint: "pending".into(),
            bucket: "bucket".into(),
            archive_key: "source.zip".into(),
            input_identity: "pending".into(),
            captured_options: admission.captured_options,
        },
    )
    .await
    .unwrap();
    let entries = successes
        .iter()
        .map(|(path, key, cid)| zip::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .chain(
            failures
                .iter()
                .map(|(path, code)| zip::ManifestItem::Failure {
                    path: (*path).into(),
                    code: (*code).into(),
                }),
        )
        .collect::<Vec<_>>();
    zip::prepare_manifest(db, &claim.batch_id, &entries)
        .await
        .unwrap();
    claim
}
