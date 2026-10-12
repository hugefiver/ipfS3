mod stage9_zip_support;
use chrono::Utc;
use ipfs_s3_gateway::{
    config::{DecompressZipConfig, OptionalPinControlMode},
    import::SupersedeReason,
    pinning::{
        decision::DecisionOrigin,
        policy::PublicationPolicy,
        zip_policy::{ZipCaptureContext, capture_zip_intent},
    },
    store::{
        self,
        entities::{object as object_entity, pin_lease, pin_provider_usage},
        import::ownership,
        object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
        pinning::publication::{
            self, CapturedZipPublication, PinTargetSpec, PublicationRequest,
            ZipExecutionSettlement, ZipPublicationAuthority,
        },
        zip::{self as storage, execution},
    },
    zip::{
        batch::{FinalZipManifest, ManifestFile},
        options::{self, parse_authenticated_zip_controls},
    },
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, TransactionTrait};
use sha2::{Digest, Sha256};
use stage9_zip_support::*;
use std::collections::BTreeMap;

async fn overwrite(db: &DatabaseConnection, key: &str, id: &str) {
    let guard = ownership::admit_content_mutation(
        db,
        "bucket",
        key,
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let object = object(id, key, "replacement");
    publication::publish_standard_object(
        db,
        PublicationRequest {
            object_target: PinTargetSpec {
                cid: object.cid.clone(),
                logical_size: object.logical_size,
            },
            object,
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
        },
        guard,
        &config().provider_limits,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn r25_automatic_owners_follow_only_exact_displaced_versions() {
    for state in [
        BucketVersioningState::Unversioned,
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ] {
        let db = setup().await;
        if state != BucketVersioningState::Unversioned {
            store::bucket::set_versioning_state(&db, "bucket", state)
                .await
                .unwrap();
        }
        let config = config();
        let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
        publish(&db, &config, request, batch, &capture, guard)
            .await
            .unwrap();
        overwrite(&db, "out/archive.zip", "new-source").await;
        assert!(
            pin_lease::Entity::find()
                .all(&db)
                .await
                .unwrap()
                .iter()
                .all(|l| l.state == "active")
        );
        overwrite(&db, "out/a", "new-a").await;
        let leases = pin_lease::Entity::find().all(&db).await.unwrap();
        let a = leases.iter().find(|l| l.owner_object_id == "a").unwrap();
        let b = leases.iter().find(|l| l.owner_object_id == "b").unwrap();
        assert_eq!(
            a.state,
            if state == BucketVersioningState::Enabled {
                "active"
            } else {
                "cancelled"
            }
        );
        assert_eq!(b.state, "active");
        let usage = pin_provider_usage::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (5, 1));
    }
}

#[tokio::test]
async fn r25_enabled_delete_marker_preserves_owner_exact_delete_ends_only_one() {
    let db = setup().await;
    store::bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
        .await
        .unwrap();
    let config = config();
    let (request, batch, capture, guard) = prepared(&db, &config, vec![]).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let version =
        store::object_version::resolve_version(&db, "bucket", "out/a", &VersionSelector::Current)
            .await
            .unwrap();
    let guard = ownership::admit_content_mutation(
        &db,
        "bucket",
        "out/a",
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    publication::delete_version_with_leases_guarded(
        &db,
        "bucket",
        "out/a",
        VersionSelector::Current,
        guard,
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(
        pin_lease::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .iter()
            .all(|l| l.state == "active")
    );
    let guard = ownership::admit_content_mutation(
        &db,
        "bucket",
        "out/a",
        None,
        SupersedeReason::DeleteObject,
        Utc::now(),
    )
    .await
    .unwrap();
    publication::delete_version_with_leases_guarded(
        &db,
        "bucket",
        "out/a",
        VersionSelector::Exact(PublicVersionId::parse_s3(&version.public_version_id).unwrap()),
        guard,
        Utc::now(),
    )
    .await
    .unwrap();
    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(
        leases
            .iter()
            .find(|l| l.owner_object_id == "a")
            .unwrap()
            .state,
        "cancelled"
    );
    assert_eq!(
        leases
            .iter()
            .find(|l| l.owner_object_id == "b")
            .unwrap()
            .state,
        "active"
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
}

async fn prepared_source_false(db: &DatabaseConnection, empty: bool) -> CapturedZipPublication {
    let config = config();
    for provider in &config.providers {
        store::pinning::ledger::register_configured_route(
            db,
            &provider.identity.allocation_key(),
            provider,
        )
        .await
        .unwrap();
    }
    let mut headers = http::HeaderMap::new();
    headers.insert(options::ZIP_CONTRACT, "v2".parse().unwrap());
    headers.insert(options::PUBLISH_SOURCE, "false".parse().unwrap());
    headers.insert(options::ZIP_TOKEN, "source-false-token".parse().unwrap());
    let parsed = parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        false,
    )
    .unwrap()
    .unwrap();
    let capture = capture_zip_intent(
        &config,
        &DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        },
        OptionalPinControlMode::Warn,
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "out/archive.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &parsed,
        DecisionOrigin::new("principal", "v2-batch"),
    )
    .unwrap();
    let mut captured = serde_json::to_value(capture.v2_options().unwrap()).unwrap();
    captured
        .as_object_mut()
        .unwrap()
        .insert("zip_intent".into(), serde_json::to_value(&capture).unwrap());
    let captured = captured.to_string();
    let contract = "authenticated source-false direct fixture";
    execution::admit(
        db,
        &execution::Admission {
            id: "v2-batch".into(),
            owner: "principal".into(),
            source: "direct".into(),
            token: capture.controls().admission_token.clone(),
            request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
            request_contract: contract.into(),
            bucket: "bucket".into(),
            source_key: "out/archive.zip".into(),
            captured_options: captured.clone(),
        },
    )
    .await
    .unwrap();
    let claim = execution::claim(db, "v2-batch", "store-test", 60)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &"1".repeat(64), "archive-cid", 5)
        .await
        .unwrap();
    let successes = if empty {
        vec![]
    } else {
        vec![
            object("a", "out/a", "shared-cid"),
            object("b", "out/b", "shared-cid"),
        ]
    };
    let files = successes
        .iter()
        .map(|o| ManifestFile {
            relative_path: o.key.trim_start_matches("out/").into(),
            object_key: o.key.clone(),
            cid: o.cid.clone(),
            size: o.logical_size,
        })
        .collect::<Vec<_>>();
    let manifest = files
        .iter()
        .map(|f| execution::ManifestItem::Success {
            path: f.relative_path.clone(),
            object_key: f.object_key.clone(),
            cid: f.cid.clone(),
            size: f.size,
        })
        .collect::<Vec<_>>();
    let mutation_ids = files
        .iter()
        .map(|f| (f.object_key.clone(), uuid::Uuid::new_v4().to_string()))
        .collect::<BTreeMap<_, _>>();
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(&tx, &claim, &manifest, &mutation_ids)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    storage::admit(
        db,
        &storage::BatchAdmission {
            id: "v2-batch".into(),
            owner: "principal".into(),
            source: "direct".into(),
            token: capture.controls().admission_token.clone(),
            fingerprint: "pending".into(),
            bucket: "bucket".into(),
            archive_key: "out/archive.zip".into(),
            input_identity: "pending".into(),
            captured_options: captured,
        },
    )
    .await
    .unwrap();
    storage::prepare_manifest(
        db,
        "v2-batch",
        &files
            .iter()
            .map(|f| storage::ManifestItem::Success {
                path: f.relative_path.clone(),
                object_key: f.object_key.clone(),
                cid: f.cid.clone(),
                size: f.size,
            })
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();
    CapturedZipPublication {
        batch_id: "v2-batch".into(),
        source: None,
        source_tags: vec![],
        successes,
        manifest: FinalZipManifest {
            successful: files,
            ..Default::default()
        },
        capture,
        authority: ZipPublicationAuthority::Execution {
            claim,
            source_guard: None,
            settlement: ZipExecutionSettlement::Direct,
        },
        root_outcome: storage::RootOutcome::Disabled,
        terminal_result: "{}".into(),
    }
}

#[tokio::test]
async fn r25_source_false_shared_core_has_no_virtual_archive_or_empty_lease() {
    for empty in [false, true] {
        let db = setup().await;
        let request = prepared_source_false(&db, empty).await;
        let error = db.execute_unprepared("UPDATE zip_v2_executions SET input_art_cid=NULL,input_art_size=NULL WHERE id='v2-batch'").await.unwrap_err();
        assert!(error.to_string().contains("ck_zip_v2_input"), "{error}");
        assert_eq!(object_entity::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        publication::publish_captured_zip(&db, request)
            .await
            .unwrap();
        assert_eq!(
            object_entity::Entity::find().count(&db).await.unwrap(),
            if empty { 0 } else { 2 }
        );
        assert_eq!(
            pin_lease::Entity::find().count(&db).await.unwrap(),
            if empty { 0 } else { 2 }
        );
        let terminal = execution::read(&db, "v2-batch").await.unwrap().unwrap();
        assert_eq!(terminal.state, "completed");
        let replay: serde_json::Value =
            serde_json::from_str(terminal.terminal_result.as_deref().unwrap()).unwrap();
        assert!(replay.get("source_cid").is_none());
        assert!(
            !storage::read(&db, "v2-batch")
                .await
                .unwrap()
                .unwrap()
                .source_published
        );
    }
}

#[tokio::test]
async fn r25_execution_completion_fault_restores_entire_common_publication() {
    let db = setup().await;
    let request = prepared_source_false(&db, false).await;
    db.execute_unprepared("CREATE TRIGGER r25_execution_tail BEFORE UPDATE OF state ON zip_v2_executions WHEN NEW.state='completed' BEGIN SELECT RAISE(ABORT,'execution tail'); END").await.unwrap();
    assert!(
        publication::publish_captured_zip(&db, request.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("execution tail")
    );
    assert_eq!(object_entity::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_provider_usage::Entity::find().count(&db).await.unwrap(),
        0
    );
    assert_eq!(
        execution::read(&db, "v2-batch")
            .await
            .unwrap()
            .unwrap()
            .state,
        "admitted"
    );
    let batch = storage::snapshot(&db, "v2-batch").await.unwrap().unwrap();
    assert_eq!(batch.batch.state, "open");
    assert!(batch.entries.iter().all(|e| e.version_row_id.is_none()));
    db.execute_unprepared("DROP TRIGGER r25_execution_tail")
        .await
        .unwrap();
    publication::publish_captured_zip(&db, request)
        .await
        .unwrap();
}
