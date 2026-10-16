use super::super::evidence;
use super::*;
use crate::{
    pinning::management::*,
    store::{self, pinning::management::actions},
};
use sea_orm_migration::MigratorTrait;

fn historical(value: &mut serde_json::Value) {
    if let Some(object) = value.as_object_mut() {
        if object
            .get("root_references")
            .is_some_and(serde_json::Value::is_array)
        {
            object.remove("root_references");
        }
        if object.contains_key("schema_versions")
            && object.contains_key("tables")
            && object.contains_key("frontier")
        {
            let versions = object
                .get_mut("schema_versions")
                .unwrap()
                .as_array_mut()
                .unwrap();
            assert_eq!(
                versions.pop().unwrap(),
                "m20261013_000001_zip_root_logical_release"
            );
            assert_eq!(
                versions.last().unwrap(),
                "m20261012_000001_pinning_management"
            );
            for table in object.get_mut("tables").unwrap().as_array_mut().unwrap() {
                if table["table"] != "zip_root_references" {
                    continue;
                }
                table["columns"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|column| column != "logical_released_at");
                table["definitions"]
                    .as_array_mut()
                    .unwrap()
                    .retain(|definition| definition["name"] != "logical_released_at");
                for row in table["rows"].as_array_mut().unwrap() {
                    row.as_object_mut().unwrap().remove("logical_released_at");
                }
            }
        }
        for nested in object.values_mut() {
            historical(nested);
        }
    } else if let Some(array) = value.as_array_mut() {
        for nested in array {
            historical(nested);
        }
    }
}

async fn historical_records(db: &sea_orm::DatabaseConnection, plan: &str) -> ConfirmedBinding {
    use super::super::archive as flat;
    let frame = native::capture(db).await.unwrap();
    let source = evidence::Evidence::from_frame(&frame).unwrap();
    let rows = source.plan_rows(plan);
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    let restored = source
        .binding(row, &mut evidence::Budget::default())
        .unwrap();
    let work = super::super::SharedDecodeBudget::default();
    let raw = native::value(row, "native_before").unwrap();
    let sections = flat::Sections::parse(raw, &mut work.lock().unwrap()).unwrap();
    let mut head: serde_json::Value = serde_json::from_str(&raw[sections.head]).unwrap();
    let mut core: serde_json::Value = serde_json::from_str(&raw[sections.core]).unwrap();
    historical(&mut core);
    let mut manifest: serde_json::Value = serde_json::from_str(&raw[sections.manifest]).unwrap();
    head[4].as_array_mut().unwrap().pop();
    manifest[0] = head[4].clone();
    let ordinal = native::current_catalog()
        .iter()
        .position(|table| table.table == "zip_root_references")
        .unwrap();
    let column = native::current_catalog()[ordinal]
        .columns
        .iter()
        .position(|column| column == "logical_released_at")
        .unwrap();
    let mut leaves = Vec::new();
    for (slot, range) in sections.leaves.into_iter().enumerate() {
        let mut leaf: serde_json::Value = serde_json::from_str(&raw[range]).unwrap();
        if leaf[0].as_u64() == Some(ordinal as u64) {
            leaf[2].as_array_mut().unwrap().remove(column);
        }
        let encoded = serde_json::to_string(&leaf).unwrap();
        let hash = flat::identity(b"stage7/leaf/1\0", encoded.as_bytes());
        head[7][slot] = serde_json::json!(hash);
        for table in manifest[1].as_array_mut().unwrap() {
            for version in table[1].as_array_mut().unwrap() {
                if version[1] == head[3] && version[2].as_u64() == Some(slot as u64) {
                    version[3] = serde_json::json!(encoded.len());
                    version[4] = serde_json::json!(hash);
                }
            }
        }
        leaves.push(encoded);
    }
    let manifest = serde_json::to_string(&manifest).unwrap();
    head[6] = serde_json::json!(flat::identity(b"stage7/manifest/1\0", manifest.as_bytes()));
    core[7] = head[6].clone();
    let core = serde_json::to_string(&core).unwrap();
    head[5] = serde_json::json!(flat::identity(b"stage7/core/1\0", core.as_bytes()));
    let before = flat::assemble(
        &serde_json::to_string(&head).unwrap(),
        &core,
        &manifest,
        &leaves,
        &mut work.lock().unwrap(),
    )
    .unwrap();
    let binding = format!(
        "S7B1\n{}",
        serde_json::json!([
            1,
            head[3],
            head[5],
            restored.confirmed_at,
            &*restored.confirmation.candidate_item_ids
        ])
    );
    for retirement in &frame.table("pin_management_retirements").unwrap().rows {
        if native::value(retirement, "plan_id") != Some(plan) {
            continue;
        }
        let domain = native::value(retirement, "allocation_domain").unwrap();
        let inventory =
            super::super::archive::encode_inventory(&before, &mut work.lock().unwrap()).unwrap();
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),"UPDATE pin_management_retirements SET binding=$1,inventory=$2 WHERE allocation_domain=$3",[binding.clone().into(),inventory.into(),domain.into()])).await.unwrap();
    }
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),"UPDATE pin_management_records SET binding=$1,native_before=$2 WHERE plan_id=$3 AND kind='action'",[binding.clone().into(),before.into(),plan.into()])).await.unwrap();
    let source = evidence::Evidence::from_frame(&native::capture(db).await.unwrap()).unwrap();
    source
        .binding(&source.plan_rows(plan)[0], &mut evidence::Budget::default())
        .unwrap()
}

fn public_native(binding: &ConfirmedBinding) -> native::NativeFrame {
    let review = binding.review();
    let mut bytes = Vec::new();
    wire::encode_review(
        &mut super::super::ReadSession::default(),
        &review,
        review.candidate_item_ids(),
        &mut bytes,
    )
    .unwrap();
    wire::decode_plan(&bytes)
        .unwrap()
        .snapshot_without_applied()
        .native_frame
        .clone()
        .unwrap()
}

#[tokio::test]
async fn committed_management_history_survives_root_marker_upgrade_without_new_business() {
    let fixture = actions::tests::fixture("historical-create").await;
    let applied = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let create = historical_records(&fixture.db, "historical-create").await;
    let root = actions::tests::seed_verified_root(&fixture).await;
    let request = PlanRequest {
        plan_id: "historical-retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let binding = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let retired = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let retirement = historical_records(&fixture.db, "historical-retire").await;
    store::migrator::Migrator::down(&fixture.db, Some(1))
        .await
        .unwrap();
    store::migrator::Migrator::up(&fixture.db, None)
        .await
        .unwrap();
    let frame = public_native(&create);
    frame.validate().unwrap();
    assert_eq!(
        frame.validate_current().err(),
        Some(ManagementError::Changed)
    );
    assert!(
        !frame
            .table("zip_root_references")
            .unwrap()
            .columns
            .iter()
            .any(|column| column == "logical_released_at")
    );
    let root_frame = public_native(&retirement);
    assert!(
        !root_frame
            .row(
                &root.table,
                &root.values.iter().map(String::as_str).collect::<Vec<_>>()
            )
            .unwrap()
            .contains_key("logical_released_at")
    );
    assert!(root_frame.validate().is_ok());
    for defect in 0..4 {
        let mut invalid = root_frame.clone();
        let table = invalid
            .tables
            .iter_mut()
            .find(|table| table.table == "zip_root_references")
            .unwrap();
        match defect {
            0 => {
                table
                    .columns
                    .retain(|column| column != "verification_receipt");
                table
                    .definitions
                    .retain(|column| column.name != "verification_receipt");
                for row in &mut table.rows {
                    row.remove("verification_receipt");
                }
            }
            1 => {
                table
                    .definitions
                    .iter_mut()
                    .find(|column| column.name == "created_at")
                    .unwrap()
                    .sqlite_type = "TEXT".into()
            }
            2 => {
                table
                    .definitions
                    .iter_mut()
                    .find(|column| column.name == "cid")
                    .unwrap()
                    .nullable = true
            }
            3 => table.primary_key.swap(1, 2),
            _ => unreachable!(),
        }
        assert!(invalid.validate().is_err());
    }
    let before = native::capture(&fixture.db).await.unwrap();
    assert!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &create)
            .await
            .unwrap()
            .items
            == applied.items
    );
    assert!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &retirement)
            .await
            .unwrap()
            .items
            == retired.items
    );
    actions::status::lookup(&fixture.db, &fixture.auth, &create, "object")
        .await
        .unwrap()
        .unwrap();
    actions::status::lookup(&fixture.db, &fixture.auth, &retirement, "provider")
        .await
        .unwrap()
        .unwrap();
    super::super::read_retirement(
        &fixture.db,
        &fixture.auth,
        &fixture.config.pinning.providers[0].name,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(before == native::capture(&fixture.db).await.unwrap());
    assert!(
        super::super::super::admission::check_provider_open(
            &fixture.db,
            &fixture.config.pinning.providers[0].name
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn historical_unexecuted_binding_cannot_become_a_current_fence() {
    let fixture = actions::tests::fixture("historical-pending").await;
    let old = historical_records(&fixture.db, "historical-pending").await;
    let before = native::capture(&fixture.db).await.unwrap();
    assert_eq!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &old)
            .await
            .err(),
        Some(ManagementError::Changed)
    );
    assert!(
        super::super::lookup_committed(&fixture.db, &fixture.auth, &old, "object")
            .await
            .unwrap()
            .is_none()
    );
    assert!(before == native::capture(&fixture.db).await.unwrap());
    let mut unknown = public_native(&old);
    unknown.schema_versions.pop();
    assert!(unknown.validate().is_err());
}
