use super::*;
use crate::{
    pinning::{
        coordinator::PinningCoordinator,
        decision::DecisionOrigin,
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    store::{
        self,
        zip::{self, BatchAdmission, ManifestItem, VersionBinding},
    },
    zip::options::{ZipTargets, ZipV2Options, parse_authenticated_zip_controls},
};
use serde_json::{Value, json};

fn options() -> ZipV2Options {
    ZipV2Options {
        publish_source: false,
        publish_extracted: true,
        targets: ZipTargets::None,
        token: "root-token".into(),
        root_override: None,
        root_enabled: true,
        result_version: 2,
    }
}

async fn native(
    capture: Value,
    source: &str,
    published_source: bool,
    partial: bool,
) -> (DatabaseConnection, BatchSnapshot, RootClaim) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    zip::admit(
        &db,
        &BatchAdmission {
            id: "root-token".into(),
            owner: "owner".into(),
            source: source.into(),
            token: "root-token".into(),
            fingerprint: "original".into(),
            bucket: "bucket".into(),
            archive_key: "archive.zip".into(),
            input_identity: "root-token".into(),
            captured_options: capture.to_string(),
        },
    )
    .await
    .unwrap();
    if source == "mpu" {
        let parts = [s3s::dto::CompletedPart {
            part_number: Some(1),
            e_tag: Some(s3s::dto::ETag::Strong("QmPart".into())),
            ..Default::default()
        }];
        BatchAdmission::capture_mpu_complete(
            &db,
            "owner",
            "bucket",
            "archive.zip",
            "root-token",
            &parts,
        )
        .await
        .unwrap();
        BatchAdmission::capture_prepared_archive(
            &db,
            "owner",
            "bucket",
            "archive.zip",
            "root-token",
            &parts,
            "QmArchive",
        )
        .await
        .unwrap();
    }
    let mut manifest = vec![ManifestItem::Success {
        path: "child".into(),
        object_key: "out/child".into(),
        cid: "original-cid".into(),
        size: 3,
    }];
    if partial {
        manifest.push(ManifestItem::Failure {
            path: "failed-child".into(),
            code: "entry_failed".into(),
        });
    }
    zip::prepare_manifest(&db, "root-token", &manifest)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('object','bucket','out/child','original-cid',3,'original-cid')").await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('version','bucket','out/child','object','object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    if published_source {
        db.execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('source','bucket','archive.zip','QmArchive',12,'QmArchive')").await.unwrap();
        db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,created_at,updated_at,lifecycle_age_started_at) VALUES ('source-version','bucket','archive.zip','object','source',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    }
    let tx = db.begin().await.unwrap();
    let binding: VersionBinding = zip::binding_for_published_object(&tx, "child", "object")
        .await
        .unwrap();
    assert_eq!(binding.version_row_id, "version");
    zip::publish(&tx, "root-token", &[binding], published_source,
        "{\"archive_cid\":\"QmArchive\",\"root_status\":\"failed\",\"root_warning\":\"directory_build_failed\",\"root_cid\":null}",
        RootOutcome::Failed { code: "directory_build_failed" }).await.unwrap();
    tx.commit().await.unwrap();
    let claim = zip::claim_root(&db, "root-token", "worker", 60)
        .await
        .unwrap();
    let snapshot = zip::snapshot(&db, "root-token").await.unwrap().unwrap();
    assert_eq!(
        snapshot
            .entries
            .iter()
            .filter(|e| e.version_row_id.as_deref() == Some("version"))
            .count(),
        1
    );
    (db, snapshot, claim)
}

#[tokio::test]
async fn original_complete_bare_mpu_strict_predicate_and_all_flat_forges_remain() {
    let captured = serde_json::to_value(options()).unwrap();
    let (db, mut snapshot, claim) = native(captured.clone(), "mpu", false, false).await;
    assert_eq!(files(&snapshot, &claim).unwrap().len(), 1);
    let mut accepted = Vec::new();
    for (field, value) in [
        ("result_version", json!(1)),
        ("result_version", json!("2")),
        ("publish_source", json!(true)),
        ("publish_source", json!("false")),
        ("publish_extracted", json!(false)),
        ("publish_extracted", json!(1)),
        ("targets", json!("source")),
        ("targets", json!("both")),
        ("targets", json!("all")),
        ("token", json!("other")),
        ("token", json!("")),
        ("token", json!("bad/token")),
        ("root_enabled", json!(false)),
        ("root_override", json!(false)),
        ("root_override", json!("true")),
        ("target_prefix", json!("out/")),
        (
            "options",
            json!({"root_enabled":true,"publish_extracted":true}),
        ),
        ("root_capture", json!({"configured":true})),
    ] {
        let mut forged = captured.clone();
        forged[field] = value.clone();
        snapshot.batch.captured_options = forged.to_string();
        let rejected = files(&snapshot, &claim).is_err();
        eprintln!("flat={field} value={value} rejected={rejected}");
        if !rejected {
            accepted.push(format!("{field}={value}"));
        }
    }
    for field in captured.as_object().unwrap().keys() {
        let mut forged = captured.clone();
        forged.as_object_mut().unwrap().remove(field);
        snapshot.batch.captured_options = forged.to_string();
        if files(&snapshot, &claim).is_ok() {
            accepted.push(format!("missing {field}"));
        }
    }
    assert!(
        accepted.is_empty(),
        "all old flat variants reached: {accepted:?}"
    );
    for (source, targets) in [
        (false, ZipTargets::Extracted),
        (true, ZipTargets::Source),
        (true, ZipTargets::Both),
    ] {
        let valid = ZipV2Options {
            publish_source: source,
            targets,
            root_override: Some(true),
            ..options()
        };
        let (_, original, original_claim) =
            native(serde_json::to_value(valid).unwrap(), "mpu", source, false).await;
        assert_eq!(files(&original, &original_claim).unwrap().len(), 1);
    }
    let persisted = zip::snapshot(&db, "root-token").await.unwrap().unwrap();
    assert_eq!(persisted.batch.captured_options, captured.to_string());
}

#[tokio::test]
async fn complete_historical_nested_and_legacy_root_bindings_need_no_remote_upgrade() {
    for (capture, source) in [
        (
            json!({"options":options(),"target_prefix":"out/","rule_revision":"original-revision"}),
            "direct",
        ),
        (
            json!({"options":options(),"target_prefix":"out/","rule_revision":"original-revision","expected_sha256":"ab".repeat(32)}),
            "import",
        ),
        (
            json!({"root_enabled":true,"target_prefix":"out/"}),
            "direct",
        ),
        (
            json!({"root_capture":{"configured":true},"target_prefix":"out/"}),
            "import",
        ),
        (
            json!({"root_capture":{"tagged":true},"target_prefix":"out/"}),
            "mpu",
        ),
    ] {
        let (db, mut snapshot, claim) = native(capture.clone(), source, false, true).await;
        let files = files(&snapshot, &claim).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "child");
        assert_eq!(files[0].cid, "original-cid");
        assert_eq!(
            snapshot.entries.len(),
            2,
            "failed entry is not a fabricated root leaf"
        );
        // A later current object cannot change the pure original-version input.
        db.execute_unprepared("UPDATE objects SET cid='current-cid' WHERE id='object'")
            .await
            .unwrap();
        assert_eq!(
            super::files(&snapshot, &claim).unwrap()[0].cid,
            "original-cid"
        );
        snapshot
            .entries
            .iter_mut()
            .find(|e| e.cid.is_some())
            .unwrap()
            .path = "invalid/0".into();
        assert!(super::files(&snapshot, &claim).is_err());
        assert_eq!(
            zip::read(&db, "root-token")
                .await
                .unwrap()
                .unwrap()
                .captured_options,
            capture.to_string()
        );
    }
}

#[tokio::test]
async fn partial_nested_mixed_markers_and_disabled_controls_never_regrant_root() {
    for capture in [
        json!({"options":{"root_enabled":true,"publish_extracted":true},"target_prefix":"out/","root_enabled":true}),
        json!({"options":options(),"target_prefix":"out/","rule_revision":"old","root_capture":{"tagged":true}}),
        json!({"root_enabled":true,"root_capture":{"configured":false},"target_prefix":"out/"}),
        json!({"root_enabled":true,"target_prefix":"out/","publish_source":false}),
        json!({"root_capture":{"configured":"true"},"target_prefix":"out/"}),
        json!({"root_enabled":true,"target_prefix":"out/","controls":{"root":{"configured":true}}}),
        json!({"root_enabled":true,"target_prefix":"out/","bucket":"another"}),
        json!({"options":options(),"target_prefix":"out/"}),
        serde_json::to_value(ZipV2Options {
            publish_source: true,
            publish_extracted: false,
            ..options()
        })
        .unwrap(),
        serde_json::to_value(ZipV2Options {
            root_enabled: false,
            ..options()
        })
        .unwrap(),
    ] {
        let published = capture
            .get("publish_source")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let (_, snapshot, claim) = native(capture.clone(), "mpu", published, false).await;
        assert!(
            files(&snapshot, &claim).is_err(),
            "not permission: {capture}"
        );
    }
}

#[tokio::test]
async fn current_direct_capture_verifies_original_origin_options_and_exact_version_fences() {
    let coordinator = PinningCoordinator::disabled_for_test();
    let mut headers = http::HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-token", "root-token"),
        ("x-ipfs3-zip-publish-source", "false"),
    ] {
        headers.insert(name, value.parse().unwrap());
    }
    let controls = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    let intent = capture_zip_intent(
        coordinator.effective_config(),
        coordinator.decompress_zip_config(),
        coordinator.control_mode(),
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("owner", "root-token"),
    )
    .unwrap();
    let captured = json!({"options":intent.v2_options(),"target_prefix":"out/","rule_revision":intent.policy_revision(),"zip_intent":intent});
    let (_, mut snapshot, claim) = native(captured.clone(), "direct", false, false).await;
    assert_eq!(files(&snapshot, &claim).unwrap().len(), 1);
    for pointer in [
        "/zip_intent/source_decision/origin/principal_id",
        "/zip_intent/source_decision/origin/request_id",
        "/zip_intent/bucket",
        "/zip_intent/source_key",
        "/zip_intent/target_prefix",
        "/rule_revision",
    ] {
        let mut value = captured.clone();
        *value.pointer_mut(pointer).unwrap() = "other".into();
        snapshot.batch.captured_options = value.to_string();
        assert!(files(&snapshot, &claim).is_err(), "{pointer}");
    }
    snapshot.batch.captured_options = captured.to_string();
    snapshot.batch.root_revision += 1;
    assert!(files(&snapshot, &claim).is_err());
    snapshot.batch.root_revision = claim.revision;
    snapshot.batch.root_epoch += 1;
    assert!(files(&snapshot, &claim).is_err());
    snapshot.batch.root_epoch = claim.epoch;
    snapshot.entries[0].version_row_id = None;
    assert!(files(&snapshot, &claim).is_err());
}

#[tokio::test]
async fn captured_legacy_root_origin_is_checked_without_replacing_configured_or_tagged_history() {
    let coordinator = PinningCoordinator::disabled_for_test();
    let controls = parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    let intent = capture_zip_intent(
        coordinator.effective_config(),
        coordinator.decompress_zip_config(),
        coordinator.control_mode(),
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("owner", "root-token"),
    )
    .unwrap();
    let captured = json!({"root_enabled":true,"target_prefix":"out/","result_enabled":true,"tags":[],"zip_intent":intent});
    let (_, mut snapshot, claim) = native(captured.clone(), "direct", true, false).await;
    assert_eq!(files(&snapshot, &claim).unwrap().len(), 1);
    let mut forged = captured;
    forged["zip_intent"]["source_decision"]["origin"]["principal_id"] = "another".into();
    snapshot.batch.captured_options = forged.to_string();
    assert!(files(&snapshot, &claim).is_err());
}
