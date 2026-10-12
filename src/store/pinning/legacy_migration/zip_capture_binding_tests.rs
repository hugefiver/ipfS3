#[path = "zip_capture_binding_fixture.rs"]
mod fixture;

use crate::{
    pinning::identity::RemoteResourceType,
    store::pinning::{ledger, legacy_migration as migration, quota},
};
use fixture::{CID, Fixture, Intake, POINTERS};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

async fn missing_bindings(kind: Intake) {
    let fixture = Fixture::new(kind, false).await;
    let before = fixture.images().await;
    let omitted = fixture.omitted();
    let result = migration::inventory(&fixture.db, &omitted).await;
    assert_eq!(fixture.images().await, before);
    assert!(
        migration::snapshot::frontier(
            &fixture.db,
            &migration::snapshot::schema(&fixture.db, false)
                .await
                .unwrap()
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert!(
        result.is_err(),
        "{kind:?}: omitted original output-only ZIP intent bindings were accepted"
    );
    let plan = migration::inventory(&fixture.db, &fixture.declaration)
        .await
        .unwrap();
    let mut forged = plan.clone();
    forged.declaration = omitted.clone();
    assert!(
        migration::apply(&fixture.db, &omitted, &forged)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.images().await,
        before,
        "incomplete declaration must leave zero business writes"
    );
    assert!(
        migration::snapshot::frontier(&fixture.db, &plan.schema)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.read_raw().await, fixture.raw);
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn original_mpu_output_only_capture_requires_complete_bindings_without_writes() {
    missing_bindings(Intake::Mpu).await;
}

#[tokio::test]
async fn original_import_output_only_capture_requires_complete_bindings_without_writes() {
    missing_bindings(Intake::Import).await;
}

#[tokio::test]
async fn original_mpu_and_import_zip_bindings_are_durable_point_proofs_not_json_rewrites_or_global_aliases()
 {
    for kind in [Intake::Mpu, Intake::Import] {
        let fixture = Fixture::new(kind, false).await;
        assert!(
            migration::resolve_legacy_binding(&fixture.db, fixture.table, &fixture.key, "old")
                .await
                .unwrap()
                .is_none()
        );
        let plan = migration::inventory(&fixture.db, &fixture.declaration)
            .await
            .expect("complete original ZIP pointers must be accepted");
        let image = plan
            .before
            .iter()
            .find(|i| i.table == fixture.table)
            .unwrap();
        let after = plan
            .after
            .iter()
            .find(|i| i.table == fixture.table)
            .unwrap();
        assert_eq!(
            image, after,
            "captured row is historical immutable evidence"
        );
        assert_eq!(
            image.rows[0]["zip_intent_json"].as_deref(),
            Some(fixture.raw.as_str())
        );
        println!("{kind:?} RAW_BEFORE={}", fixture.raw);
        let receipt = migration::apply(&fixture.db, &fixture.declaration, &plan)
            .await
            .unwrap();
        assert_eq!(
            migration::apply(&fixture.db, &fixture.declaration, &plan)
                .await
                .unwrap(),
            receipt
        );
        let row = fixture.db.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "SELECT bindings,before_image,after_image FROM pin_legacy_migration_items WHERE operation_id=$1 AND table_kind=$2 AND original_key=$3",
            [receipt.id.clone().into(),fixture.table.into(),serde_json::to_string(&fixture.key).unwrap().into()])).await.unwrap().unwrap();
        let raw_bindings: String = row.try_get("", "bindings").unwrap();
        let bindings: Vec<migration::Binding> = serde_json::from_str(&raw_bindings).unwrap();
        let expected: Vec<_> = fixture.declaration.namespaces[0]
            .bindings
            .iter()
            .filter(|b| b.table == fixture.table)
            .cloned()
            .collect();
        assert_eq!(bindings, expected);
        assert_eq!(
            bindings
                .iter()
                .filter_map(|b| b.pointer.as_deref())
                .collect::<Vec<_>>(),
            POINTERS
        );
        let original: Option<migration::NativeRow> =
            serde_json::from_str(&row.try_get::<String>("", "before_image").unwrap()).unwrap();
        let final_image: Option<migration::NativeRow> =
            serde_json::from_str(&row.try_get::<String>("", "after_image").unwrap()).unwrap();
        assert_eq!(original, final_image);
        assert_eq!(
            original.unwrap()["zip_intent_json"].as_deref(),
            Some(fixture.raw.as_str())
        );
        let binding =
            migration::resolve_legacy_binding(&fixture.db, fixture.table, &fixture.key, "old")
                .await
                .unwrap()
                .unwrap();
        let target = &fixture.declaration.namespaces[0].target;
        assert_eq!(&binding.operational_key, target);
        assert_eq!(binding.receipt_id, receipt.id);
        assert_eq!(
            binding.provenance,
            "migration_declaration_not_remote_evidence"
        );
        assert!(binding.historical_route.is_none());
        assert!(binding.historical_epoch.is_none());
        assert!(!binding.closed_at_migration);
        assert!(binding.release_epochs_at_migration.is_empty());
        assert!(
            !ledger::cleanup_allowed(&fixture.db, target, CID)
                .await
                .unwrap()
        );
        assert!(
            migration::resource_binding(&fixture.db, target, CID)
                .await
                .unwrap()
                .is_none()
        );
        let usage = quota::read_usage(&fixture.db, target)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
        assert!(
            fixture
                .db
                .query_one(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "SELECT provider FROM pin_provider_usage WHERE provider='old'"
                ))
                .await
                .unwrap()
                .is_none()
        );
        let historical_usage = quota::read_usage(&fixture.db, "old")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                historical_usage.reserved_bytes,
                historical_usage.reserved_pins
            ),
            (97, 3)
        );
        assert_eq!(&historical_usage.provider, target);
        assert!(
            migration::resolve_legacy_binding(
                &fixture.db,
                fixture.table,
                &fixture.key,
                "unrelated"
            )
            .await
            .unwrap()
            .is_none()
        );
        fixture.later_capture().await;
        let late_key = [("upload_id".into(), "post-receipt".into())].into();
        assert!(
            migration::resolve_legacy_binding(&fixture.db, "multipart_uploads", &late_key, "old")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            migration::captured_provider(&fixture.db, "multipart_uploads", &late_key, "old")
                .await
                .unwrap(),
            "old"
        );
        assert_eq!(fixture.read_raw().await, fixture.raw);
        println!("{kind:?} RAW_AFTER={}", fixture.read_raw().await);
        println!(
            "{kind:?} NATIVE_ITEM_BINDINGS={raw_bindings}; POINT_KEY={:?}; PROVENANCE={}",
            fixture.key, binding.provenance
        );
        fixture.db.close().await.unwrap();
    }
}

#[tokio::test]
async fn zip_capture_original_known_backend_type_and_account_cannot_be_overridden_but_legacy_unknown_scope_remains_unknown()
 {
    for kind in [Intake::Mpu, Intake::Import] {
        let fixture = Fixture::new(kind, true).await;
        // Isolate the persisted production capture so other registered-route rows
        // cannot hide a missing captured-column scan.
        let images: Vec<_> = fixture
            .images()
            .await
            .into_iter()
            .filter(|i| i.table == fixture.table)
            .collect();
        let namespace = &fixture.declaration.namespaces[0];
        migration::closure_identity::validate(&images, namespace).unwrap();
        for conflict in ["backend", "resource_type", "scope"] {
            let mut wrong = namespace.clone();
            match conflict {
                "backend" => wrong.backend = "filebase".into(),
                "resource_type" => wrong.resource_type = RemoteResourceType::RpcPin,
                _ => wrong.scope = "different-account".into(),
            }
            let error = migration::closure_identity::validate(&images, &wrong)
                .expect_err("original captured known identity must contradict the declaration");
            assert!(
                error
                    .to_string()
                    .contains("historical backend/resource type/account contradicts declaration")
            );
            println!("{kind:?} KNOWN_{conflict}_REJECT={error}");
        }
        assert_eq!(fixture.read_raw().await, fixture.raw);
        fixture.db.close().await.unwrap();

        let legacy = Fixture::new(kind, false).await;
        let images: Vec<_> = legacy
            .images()
            .await
            .into_iter()
            .filter(|i| i.table == legacy.table)
            .collect();
        migration::closure_identity::validate(&images, &legacy.declaration.namespaces[0]).unwrap();
        assert_eq!(legacy.read_raw().await, legacy.raw);
        legacy.db.close().await.unwrap();
    }
}
