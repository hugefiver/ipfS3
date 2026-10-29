use ipfs_s3_gateway::{
    pinning::identity::ProviderIdentity,
    store::{
        self,
        pinning::{ledger, legacy_migration, quota},
    },
};
use sea_orm::EntityTrait;
use sea_orm::{ConnectionTrait, DatabaseConnection};
use sea_orm::{DatabaseBackend, Statement};
use serde_json::json;

async fn old_usage_is_absent(db: &DatabaseConnection) -> bool {
    ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old".to_owned())
        .one(db)
        .await
        .unwrap()
        .is_none()
}

fn add_binding(
    decl: &mut legacy_migration::Declaration,
    table: &str,
    key: serde_json::Value,
    route: Option<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>,
    pointer: Option<&str>,
) {
    let source = decl.namespaces[0].source.clone();
    decl.namespaces[0].bindings.push(
        serde_json::from_value(json!({"table":table,"key":key,
        "provider":source,"historical_route":route,"pointer":pointer}))
        .unwrap(),
    );
}

async fn seed_usage(db: &DatabaseConnection) {
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
}

async fn fixture() -> (DatabaseConnection, ProviderIdentity) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    let identity = ProviderIdentity {
        provider_id: "explicit-account".into(),
        display_name: "configured".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: ipfs_s3_gateway::pinning::identity::CleanupMode::Retain,
    };
    ledger::register_route(&db, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    (db, identity)
}

fn declaration(identity: &ProviderIdentity, source: &str) -> legacy_migration::Declaration {
    serde_json::from_value(json!({
        "version": 1, "database_scope": "fixture", "ttl_seconds": 300,
        "maintenance": {"all_writers_stopped": true, "all_workers_drained": true},
        "namespaces": [{"source": source, "target": identity.allocation_key(),
            "backend": identity.backend, "scope": identity.scope,
            "resource_type": identity.resource_type(), "binding_only": source == identity.allocation_key(),
            "target_route": identity.route_snapshot(), "bindings": [{
                "table": "pin_provider_usage", "key": {"provider": source},
                "provider": source, "historical_route": null
            }]}]
    })).unwrap()
}

#[tokio::test]
async fn multirow_apply_and_reverse_preserve_every_native_receipt_image_and_binding() {
    use ipfs_s3_gateway::store::entities::{
        pin_legacy_migration as operation, pin_legacy_migration_item as item,
    };
    use sea_orm::{ColumnTrait, QueryFilter, QueryOrder};
    let (db, identity) = fixture().await;
    const ROWS: i64 = 48;
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',$1,$2)",
        [(ROWS * 7).into(), ROWS.into()],
    )).await.unwrap();
    let mut decl = declaration(&identity, "old");
    for index in (0..ROWS).rev() {
        let cid = format!("cid-{index}");
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old',$1,7,'pinned',$2,'2026-10-07T00:00:00.123456789Z')",
            [cid.clone().into(), (index + 1).into()],
        )).await.unwrap();
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('old',$1,'application_created','unknown')",
            [cid.clone().into()],
        )).await.unwrap();
        for table in ["remote_pins", "remote_pin_ledger"] {
            add_binding(
                &mut decl,
                table,
                json!({"provider":"old", "cid":cid}),
                None,
                None,
            );
        }
    }
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    let receipt = legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    assert_eq!(receipt.id, plan.operation_id);
    assert_eq!(
        legacy_migration::apply(&db, &decl, &plan).await.unwrap(),
        receipt
    );
    let items = item::Entity::find()
        .filter(item::Column::OperationId.eq(&receipt.id))
        .order_by_asc(item::Column::Ordinal)
        .all(&db)
        .await
        .unwrap();
    let committed = operation::Entity::find_by_id(receipt.id.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<legacy_migration::Confirmation>(&committed.confirmation).unwrap(),
        plan
    );
    assert_eq!(
        items.len(),
        plan.before
            .iter()
            .map(|image| image.rows.len().max(1))
            .sum::<usize>()
    );
    let mut ordinal = 0;
    for image in &plan.before {
        let after = plan
            .after
            .iter()
            .find(|after| after.table == image.table)
            .unwrap();
        let original_rows = if image.rows.is_empty() {
            vec![None]
        } else {
            image.rows.iter().map(Some).collect()
        };
        for row in original_rows {
            let stored = &items[ordinal];
            assert_eq!(stored.table_kind, image.table);
            assert_eq!(
                serde_json::from_str::<Option<legacy_migration::NativeRow>>(&stored.before_image)
                    .unwrap()
                    .as_ref(),
                row
            );
            let key: std::collections::BTreeMap<String, String> = row
                .into_iter()
                .flat_map(|row| {
                    image.primary_key.iter().filter_map(move |column| {
                        row.get(column)
                            .and_then(|v| v.as_ref())
                            .map(|v| (column.clone(), v.clone()))
                    })
                })
                .collect();
            assert_eq!(
                serde_json::from_str::<std::collections::BTreeMap<String, String>>(
                    &stored.original_key
                )
                .unwrap(),
                key
            );
            let expected_bindings: Vec<_> = decl
                .namespaces
                .iter()
                .flat_map(|ns| &ns.bindings)
                .filter(|binding| binding.table == image.table && binding.key == key)
                .cloned()
                .collect();
            assert_eq!(
                serde_json::from_str::<Vec<legacy_migration::Binding>>(&stored.bindings).unwrap(),
                expected_bindings
            );
            let mut after_key = key;
            if matches!(
                image.table.as_str(),
                "remote_pins" | "remote_pin_ledger" | "pin_provider_usage"
            ) {
                after_key.insert("provider".into(), identity.allocation_key());
            }
            let expected_after = after.rows.iter().find(|row| {
                after_key
                    .iter()
                    .all(|(column, value)| row.get(column).and_then(|v| v.as_ref()) == Some(value))
            });
            assert_eq!(
                serde_json::from_str::<Option<legacy_migration::NativeRow>>(&stored.after_image)
                    .unwrap()
                    .as_ref(),
                expected_after
            );
            ordinal += 1;
        }
    }
    let reverse = legacy_migration::rollback(&db, &receipt.id, &plan)
        .await
        .unwrap();
    assert_eq!(reverse.reverses.as_deref(), Some(receipt.id.as_str()));
    let reverse_items = item::Entity::find()
        .filter(item::Column::OperationId.eq(&reverse.id))
        .order_by_asc(item::Column::Ordinal)
        .all(&db)
        .await
        .unwrap();
    assert_eq!(reverse_items.len(), items.len());
    for (original, reverse) in items.iter().zip(&reverse_items) {
        assert_eq!(reverse.original_key, original.original_key);
        assert_eq!(reverse.bindings, original.bindings);
        assert_eq!(reverse.before_image, original.after_image);
        assert_eq!(reverse.after_image, original.before_image);
    }
    assert_eq!(
        legacy_migration::rollback(&db, &receipt.id, &plan)
            .await
            .unwrap(),
        reverse
    );
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        ROWS * 7
    );
    let remotes = ipfs_s3_gateway::store::entities::remote_pin::Entity::find()
        .all(&db)
        .await
        .unwrap();
    assert_eq!(remotes.len(), ROWS as usize);
    assert!(remotes.iter().all(|row| row.provider == "old"));
}

#[tokio::test]
async fn usage_debt_moves_once_and_rollback_is_db_receipted() {
    let (db, identity) = fixture().await;
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let declaration = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
    let receipt = legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    assert!(old_usage_is_absent(&db).await);
    let moved = quota::read_usage(&db, &identity.allocation_key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((moved.reserved_bytes, moved.reserved_pins), (97, 3));
    assert_eq!(
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .unwrap(),
        receipt
    );
    let reverse = legacy_migration::rollback(&db, &receipt.id, &plan)
        .await
        .unwrap();
    assert_eq!(
        legacy_migration::rollback(&db, &receipt.id, &plan)
            .await
            .unwrap(),
        reverse
    );
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
    assert!(
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn detached_evidence_and_client_forgery_are_not_success_proof() {
    let (db, identity) = fixture().await;
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let declaration = declaration(&identity, "old");
    let mut plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    plan.operation_id = "client-forgery".into();
    plan.before.clear();
    assert!(
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .is_err()
    );
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}

#[tokio::test]
async fn rekey_keeps_old_reads_and_does_not_revive_source_or_create_cleanup_authority() {
    let (db, identity) = fixture().await;
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old','cid',7,'pinned',42,'2026-10-07T00:00:00.123456789Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('old','cid','application_created','unknown')").await.unwrap();
    let mut declaration = declaration(&identity, "old");
    for table in ["remote_pins", "remote_pin_ledger"] {
        declaration.namespaces[0].bindings.push(serde_json::from_value(json!({
            "table":table, "key":{"provider":"old","cid":"cid"}, "provider":"old","historical_route":null
        })).unwrap());
    }
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    assert_eq!(
        ledger::get(&db, "old", "cid")
            .await
            .unwrap()
            .unwrap()
            .effect,
        "unknown"
    );
    assert!(ledger::status(&db, "old", "cid").await.unwrap().is_some());
    assert!(
        !ledger::cleanup_allowed(&db, &identity.allocation_key(), "cid")
            .await
            .unwrap()
    );
    ledger::register_route(&db, "old", &identity).await.unwrap();
    let now = store::database_clock::database_now(&db).await.unwrap();
    let limits = [(
        "old".into(),
        ipfs_s3_gateway::pinning::config::ProviderLimits {
            priority: 1,
            max_bytes: 1000,
            max_pins: 100,
            enabled: true,
        },
    )]
    .into();
    assert!(
        quota::reserve_unique(&db, "old", "new", 1, &limits, now)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rollback_refuses_new_work_and_no_partial_move_on_target_collision() {
    let (db, identity) = fixture().await;
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    let receipt = legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    db.execute_unprepared("INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('unrelated','late',1,'{}')").await.unwrap();
    assert!(
        legacy_migration::rollback(&db, &receipt.id, &plan)
            .await
            .is_err()
    );
    assert!(old_usage_is_absent(&db).await);
    assert_eq!(
        quota::read_usage(&db, &identity.allocation_key())
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}

#[tokio::test]
async fn global_capacity_guard_remains_and_undeclared_blocker_still_denies() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let target = identity.allocation_key();
    let limits = [(
        target.clone(),
        ipfs_s3_gateway::pinning::config::ProviderLimits {
            priority: 1,
            max_bytes: 1000,
            max_pins: 100,
            enabled: true,
        },
    )]
    .into();
    let now = store::database_clock::database_now(&db).await.unwrap();
    assert!(
        quota::reserve_unique(&db, &target, "new", 1, &limits, now)
            .await
            .is_err()
    );
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    assert_eq!(
        quota::reserve_unique(&db, &target, "new", 1, &limits, now)
            .await
            .unwrap(),
        quota::ReservationOutcome::Reserved
    );
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ('undeclared','another','unknown','unknown')").await.unwrap();
    assert!(
        quota::reserve_unique(&db, &target, "newer", 1, &limits, now)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn binding_only_domain_null_route_preserves_usage_and_unknown() {
    let (db, identity) = fixture().await;
    let target = identity.allocation_key();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ($1,97,3)",
        [target.clone().into()],
    ))
    .await
    .unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES ($1,'cid','unknown','unknown')",[target.clone().into()])).await.unwrap();
    let mut decl = declaration(&identity, &target);
    add_binding(
        &mut decl,
        "remote_pin_ledger",
        json!({"provider":target,"cid":"cid"}),
        None,
        None,
    );
    add_binding(
        &mut decl,
        "pin_provider_routes",
        json!({"provider":target}),
        Some(identity.route_snapshot()),
        None,
    );
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    let ledger = ledger::get(&db, &target, "cid").await.unwrap().unwrap();
    assert_eq!(
        (ledger.effect.as_str(), ledger.ownership.as_str()),
        ("unknown", "unknown")
    );
    assert!(ledger.route.is_some());
    assert_eq!(
        quota::read_usage(&db, &target)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        3
    );
}

#[tokio::test]
async fn target_disjoint_usage_merge_and_reverse_keep_unexplained_debt() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let target = identity.allocation_key();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ($1,13,2)",
        [target.clone().into()],
    ))
    .await
    .unwrap();
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    let receipt = legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    let usage = quota::read_usage(&db, &target).await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (110, 5));
    legacy_migration::rollback(&db, &receipt.id, &plan)
        .await
        .unwrap();
    assert_eq!(
        quota::read_usage(&db, &target)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        13
    );
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}

#[tokio::test]
async fn all_epoch_history_is_original_and_union_is_point_scoped() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let payload = "{  \"ledger\": { \"error\": \"original bytes\" } }";
    let mut decl = declaration(&identity, "old");
    for epoch in [1_i64, 7, 42] {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('old','cid',$1,$2)",[epoch.into(),payload.into()])).await.unwrap();
        add_binding(
            &mut decl,
            "pin_resource_history",
            json!({"provider":"old","cid":"cid","epoch":epoch.to_string()}),
            None,
            None,
        );
    }
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    let rows = legacy_migration::release_history(&db, &identity.allocation_key(), "cid")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert!(
        rows.iter()
            .all(|r| r.provider == "old" && r.ledger == payload)
    );
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('old','cid',99,$1)",
        [payload.into()],
    ))
    .await
    .unwrap();
    assert_eq!(
        legacy_migration::release_history(&db, &identity.allocation_key(), "cid")
            .await
            .unwrap()
            .len(),
        3,
        "unreceipted identity is not an alias"
    );
}

#[tokio::test]
async fn receipt_insert_failure_rolls_back_every_business_image() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    db.execute_unprepared("CREATE TRIGGER r26_fail_receipt BEFORE INSERT ON pin_legacy_migration_items BEGIN SELECT RAISE(ABORT,'fixture'); END").await.unwrap();
    assert!(legacy_migration::apply(&db, &decl, &plan).await.is_err());
    let current = legacy_migration::inventory(&db, &decl).await.unwrap();
    assert_eq!(current.before, plan.before);
    assert_eq!(current.receipt_frontier, plan.receipt_frontier);
    db.execute_unprepared("DROP TRIGGER r26_fail_receipt")
        .await
        .unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
}

#[tokio::test]
async fn final_native_image_drift_rolls_back_apply_and_reverse_without_receipts() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    db.execute_unprepared("CREATE TRIGGER r26_native_drift AFTER INSERT ON pin_provider_usage BEGIN INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('unrelated','trigger',1,'{}'); END").await.unwrap();
    assert!(legacy_migration::apply(&db, &decl, &plan).await.is_err());
    let current = legacy_migration::inventory(&db, &decl).await.unwrap();
    assert_eq!(current.before, plan.before);
    assert_eq!(current.receipt_frontier, plan.receipt_frontier);
    db.execute_unprepared("DROP TRIGGER r26_native_drift")
        .await
        .unwrap();
    let receipt = legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    db.execute_unprepared("CREATE TRIGGER r26_native_drift AFTER INSERT ON pin_provider_usage BEGIN INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('unrelated','trigger',1,'{}'); END").await.unwrap();
    assert!(
        legacy_migration::rollback(&db, &receipt.id, &plan)
            .await
            .is_err()
    );
    let rollback_plan = legacy_migration::rollback_inventory(&db, &receipt.id, &decl)
        .await
        .unwrap();
    assert_eq!(rollback_plan.after, plan.after);
    assert_eq!(rollback_plan.before, plan.before);
    assert!(old_usage_is_absent(&db).await);
    db.execute_unprepared("DROP TRIGGER r26_native_drift")
        .await
        .unwrap();
    legacy_migration::rollback(&db, &receipt.id, &plan)
        .await
        .unwrap();
}

#[tokio::test]
async fn changed_target_route_and_native_time_and_ttl_refuse_without_partial_write() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let decl = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    let mut expired = plan.clone();
    expired.expires_at = "2000-01-01T00:00:00Z".into();
    assert!(legacy_migration::apply(&db, &decl, &expired).await.is_err());
    let mut drifted = identity.clone();
    drifted.endpoint_revision += 1;
    ledger::register_route(&db, &identity.allocation_key(), &drifted)
        .await
        .unwrap();
    assert!(legacy_migration::apply(&db, &decl, &plan).await.is_err());
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}

#[tokio::test]
async fn live_worker_even_expired_claim_and_missing_class_binding_refuse() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared("INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,locked_until,submit_phase) VALUES ('legacy-job','unpin','old','cid',1,'running','2000-01-01T00:00:00Z','2000-01-01T00:00:01Z',NULL)").await.unwrap();
    let mut decl = declaration(&identity, "old");
    assert!(legacy_migration::inventory(&db, &decl).await.is_err());
    add_binding(
        &mut decl,
        "pin_jobs",
        json!({"id":"legacy-job"}),
        None,
        None,
    );
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    assert!(legacy_migration::apply(&db, &decl, &plan).await.is_err());
    assert_eq!(
        legacy_migration::inventory(&db, &decl)
            .await
            .unwrap()
            .before,
        plan.before
    );
}

#[tokio::test]
async fn shared_cid_collision_and_underaccounting_are_not_reconciliation() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old','same',7,'pinned',1,CURRENT_TIMESTAMP)").await.unwrap();
    let target = identity.allocation_key();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ($1,'same',7,'pinned',2,CURRENT_TIMESTAMP)",[target.clone().into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ($1,7,1)",
        [target.into()],
    ))
    .await
    .unwrap();
    let mut decl = declaration(&identity, "old");
    add_binding(
        &mut decl,
        "remote_pins",
        json!({"provider":"old","cid":"same"}),
        None,
        None,
    );
    assert!(legacy_migration::inventory(&db, &decl).await.is_err());
    db.execute_unprepared(
        "UPDATE pin_provider_usage SET reserved_bytes=0,reserved_pins=0 WHERE provider='old'",
    )
    .await
    .unwrap();
    assert!(legacy_migration::inventory(&db, &decl).await.is_err());
    assert_eq!(
        ledger::status(&db, "old", "same")
            .await
            .unwrap()
            .unwrap()
            .epoch,
        1
    );
}

#[tokio::test]
async fn detached_observation_debt_is_not_lost_when_job_and_owner_are_missing() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let route = serde_json::to_string(&identity.route_snapshot()).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO pin_submit_observations(id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at,observed_at) VALUES ('detached','missing-job',1,'old','unexpected',$1,1,'2026-10-07T00:00:00.123456789Z','unknown','[]','unknown',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[route.into()])).await.unwrap();
    let mut decl = declaration(&identity, "old");
    assert!(
        legacy_migration::inventory(&db, &decl).await.is_err(),
        "detached evidence is a required class"
    );
    add_binding(
        &mut decl,
        "pin_submit_observations",
        json!({"id":"detached"}),
        Some(identity.route_snapshot()),
        None,
    );
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    let observations = ledger::submission::observations(&db, &identity.allocation_key())
        .await
        .unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].provider, "old");
    assert!(
        ledger::submission::has_debt(&db, &identity.allocation_key())
            .await
            .unwrap()
    );
    assert_eq!(observations[0].resources, "[]");
}

#[tokio::test]
async fn frozen_candidates_completion_and_original_contract_survive_rekey() {
    use ipfs_s3_gateway::store::entities::pin_lease_requirement::contract::{self, BirthContract};
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    db.execute_unprepared("INSERT INTO buckets(name) VALUES ('owner-bucket')")
        .await
        .unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES ('owner','owner-bucket','object',$1,7,$1)",[cid.into()])).await.unwrap();
    db.execute_unprepared("INSERT INTO object_versions(id,bucket,key,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('version','owner-bucket','object','object','owner',1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state) VALUES ('lease','owner','manual','policy','all','full','2026-10-07T00:00:00.123456789Z','2026-10-07T00:00:00.123456789Z','2099-01-01T00:00:00Z',9,'active')").await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) VALUES ('target','lease',$1,7,'old','pinned',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[cid.into()])).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old',$1,7,'pinned',42,'2026-10-07T00:00:00.123456789Z')",[cid.into()])).await.unwrap();
    let route = serde_json::to_string(&identity.route_snapshot()).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ('old',$1,$2,'unknown','confirmed')",[cid.into(),route.into()])).await.unwrap();
    let birth = BirthContract {
        provider_mode: "all".into(),
        content_mode: "full".into(),
        content_cids: [cid.into()].into(),
        candidate_order: vec!["old".into()],
        candidates: [("old".into(), Some(identity.route_snapshot()))].into(),
    };
    let raw = serde_json::to_string(&birth).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_lease_requirements(lease_id,owner_object_id,contract,created_at) VALUES ('lease','owner',$1,'2026-10-07T00:00:00.123456789Z')",[raw.clone().into()])).await.unwrap();
    let mut decl = declaration(&identity, "old");
    add_binding(
        &mut decl,
        "remote_pins",
        json!({"provider":"old","cid":cid}),
        None,
        None,
    );
    add_binding(
        &mut decl,
        "remote_pin_ledger",
        json!({"provider":"old","cid":cid}),
        Some(identity.route_snapshot()),
        None,
    );
    add_binding(
        &mut decl,
        "pin_lease_targets",
        json!({"id":"target"}),
        None,
        None,
    );
    assert!(
        legacy_migration::inventory(&db, &decl).await.is_err(),
        "frozen candidates cannot be omitted"
    );
    for pointer in [
        "/contract/candidates/@key/old",
        "/contract/candidate_order/0",
    ] {
        add_binding(
            &mut decl,
            "pin_lease_requirements",
            json!({"lease_id":"lease"}),
            None,
            Some(pointer),
        );
    }
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    assert_eq!(
        contract::frozen_candidates(&db, "lease")
            .await
            .unwrap()
            .unwrap(),
        vec![identity.allocation_key()]
    );
    assert_eq!(
        contract::get(&db, "lease")
            .await
            .unwrap()
            .unwrap()
            .candidate_order,
        vec!["old"]
    );
    let now = store::database_clock::database_now(&db).await.unwrap();
    assert!(
        contract::target_confirmed(&db, "lease", &identity.allocation_key(), cid, now)
            .await
            .unwrap()
    );
    assert_eq!(
        contract::completion(&db, "lease", now).await.unwrap(),
        Some((true, true, 1))
    );
    let stored: String = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT contract FROM pin_lease_requirements WHERE lease_id='lease'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "contract")
        .unwrap();
    assert_eq!(stored, raw);
}

#[tokio::test]
async fn detached_stage6_replica_observation_requires_binding_and_stays_original() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let route = serde_json::to_string(&identity.route_snapshot()).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_replica_observations(sequence,job_id,provider,expected_cid,route,remote_epoch,claim_until,phase,resources,outcome,projectable,observed_at) VALUES (1,'missing','old','cid',$1,8,'2026-10-07T00:00:00.123456789Z','poll','[]','Unknown',0,'2026-10-07T00:00:00.123456789Z')",[route.clone().into()])).await.unwrap();
    let mut decl = declaration(&identity, "old");
    assert!(legacy_migration::inventory(&db, &decl).await.is_err());
    add_binding(
        &mut decl,
        "pin_replica_observations",
        json!({"sequence":"1"}),
        Some(identity.route_snapshot()),
        None,
    );
    let plan = legacy_migration::inventory(&db, &decl).await.unwrap();
    legacy_migration::apply(&db, &decl, &plan).await.unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT provider,route,resources FROM pin_replica_observations WHERE sequence=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<String>("", "provider").unwrap(), "old");
    assert_eq!(row.try_get::<String>("", "route").unwrap(), route);
    assert_eq!(row.try_get::<String>("", "resources").unwrap(), "[]");
}

#[tokio::test]
async fn rollback_can_refresh_db_clock_confirmation_without_rewriting_original_receipt() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    let declaration = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    let receipt = legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    let mut inverse = declaration.clone();
    inverse.rollback_receipt = Some(receipt.id.clone());
    inverse.ttl_seconds = 600;
    let reverse_plan = legacy_migration::inventory(&db, &inverse).await.unwrap();
    assert_eq!(
        reverse_plan.rollback_of.as_deref(),
        Some(receipt.id.as_str())
    );
    assert_eq!(reverse_plan.before, plan.before);
    assert_eq!(reverse_plan.after, plan.after);
    assert!(
        legacy_migration::apply(&db, &inverse, &reverse_plan)
            .await
            .is_err()
    );
    let reverse = legacy_migration::rollback(&db, &receipt.id, &reverse_plan)
        .await
        .unwrap();
    assert_eq!(
        legacy_migration::rollback(&db, &receipt.id, &reverse_plan)
            .await
            .unwrap(),
        reverse
    );
    assert_eq!(
        quota::read_usage(&db, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}

#[tokio::test]
async fn persisted_parking_is_not_an_expired_live_claim_and_cannot_gain_unpin() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared("INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase,last_error) VALUES ('parked','unpin','old','cid',1,'running','2000-01-01T00:00:00Z',NULL,'historical identity unavailable; needs_attention')").await.unwrap();
    let mut declaration = declaration(&identity, "old");
    add_binding(
        &mut declaration,
        "pin_jobs",
        json!({"id":"parked"}),
        None,
        None,
    );
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    let job = ipfs_s3_gateway::store::entities::pin_job::Entity::find_by_id("parked")
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.provider, identity.allocation_key());
    assert!(job.locked_until.is_none());
    assert!(!legacy_migration::job_io_allowed(&db, &job).await.unwrap());
}

#[tokio::test]
async fn original_poll_api_is_not_replaced_by_current_configuration() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old','cid',7,'pinned',42,'2026-10-07T00:00:00.123456789Z')").await.unwrap();
    let route = serde_json::to_string(&identity.route_snapshot()).unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ('old','cid',$1,'unknown','confirmed')",[route.into()])).await.unwrap();
    let mut declaration = declaration(&identity, "old");
    add_binding(
        &mut declaration,
        "remote_pins",
        json!({"provider":"old","cid":"cid"}),
        None,
        None,
    );
    add_binding(
        &mut declaration,
        "remote_pin_ledger",
        json!({"provider":"old","cid":"cid"}),
        Some(identity.route_snapshot()),
        None,
    );
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    let now = store::database_clock::database_now(&db).await.unwrap();
    let job = ipfs_s3_gateway::store::entities::pin_job::Model {
        id: "new-poll".into(),
        operation: "poll".into(),
        provider: identity.allocation_key(),
        cid: "cid".into(),
        lease_id: Some("lease".into()),
        target_id: Some("target".into()),
        expected_generation: Some(1),
        expected_remote_epoch: None,
        state: "pending".into(),
        attempts: 0,
        next_attempt_at: now,
        locked_until: None,
        submit_phase: None,
        last_error: None,
        created_at: now,
        updated_at: now,
    };
    assert!(legacy_migration::job_io_allowed(&db, &job).await.unwrap());
    let mut changed = identity.clone();
    changed.api_profile = "psa".into();
    ledger::register_route(&db, &identity.allocation_key(), &changed)
        .await
        .unwrap();
    assert!(!legacy_migration::job_io_allowed(&db, &job).await.unwrap());
    assert_eq!(
        ledger::get(&db, &job.provider, &job.cid)
            .await
            .unwrap()
            .unwrap()
            .route
            .unwrap(),
        serde_json::to_string(&identity.route_snapshot()).unwrap()
    );
}

#[tokio::test]
async fn sqlite_two_connection_write_intent_and_committed_frontier_refuse_atomically() {
    use sea_orm::{ConnectOptions, Database, TransactionTrait};
    let directory = tempfile::tempdir().unwrap();
    let owner = uuid::Uuid::new_v4().to_string();
    let marker = directory.path().join("owner.txt");
    std::fs::write(&marker, &owner).unwrap();
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), owner);
    // Both SQLite file/journal parents belong to this unique tempfile fixture.
    let file = directory.path().join("capacity.sqlite");
    let url = format!(
        "sqlite://{}?mode=rwc",
        file.display().to_string().replace('\\', "/")
    );
    let mut options = ConnectOptions::new(&url);
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.busy_timeout(std::time::Duration::from_millis(20)));
    let a = Database::connect(options.clone()).await.unwrap();
    let b = Database::connect(options).await.unwrap();
    store::run_migrations(&a).await.unwrap();
    let (_, identity) = fixture().await;
    ledger::register_route(&a, &identity.allocation_key(), &identity)
        .await
        .unwrap();
    seed_usage(&a).await;
    let declaration = declaration(&identity, "old");
    let plan = legacy_migration::inventory(&a, &declaration).await.unwrap();
    let writer = b.begin().await.unwrap();
    writer
        .execute_unprepared("UPDATE pin_provider_usage SET reserved_bytes=98 WHERE provider='old'")
        .await
        .unwrap();
    assert!(
        legacy_migration::apply(&a, &declaration, &plan)
            .await
            .is_err()
    );
    assert_eq!(
        legacy_migration::inventory(&a, &declaration)
            .await
            .unwrap()
            .before,
        plan.before
    );
    writer.commit().await.unwrap();
    assert!(
        legacy_migration::apply(&a, &declaration, &plan)
            .await
            .is_err()
    );
    let changed = legacy_migration::inventory(&a, &declaration).await.unwrap();
    assert!(changed.receipt_frontier.is_empty());
    assert_eq!(
        quota::read_usage(&a, "old")
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        98
    );
    legacy_migration::apply(&a, &declaration, &changed)
        .await
        .unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn same_cid_reuses_one_accounted_hold_without_promoting_declaration_to_pin_proof() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old','cid',7,'pinned',42,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect,remote_pinned_at) VALUES ('old','cid','unknown','retained',CURRENT_TIMESTAMP)").await.unwrap();
    let mut declaration = declaration(&identity, "old");
    for table in ["remote_pins", "remote_pin_ledger"] {
        add_binding(
            &mut declaration,
            table,
            json!({"provider":"old","cid":"cid"}),
            None,
            None,
        );
    }
    let plan = legacy_migration::inventory(&db, &declaration)
        .await
        .unwrap();
    legacy_migration::apply(&db, &declaration, &plan)
        .await
        .unwrap();
    let target = identity.allocation_key();
    let limits = [(
        target.clone(),
        ipfs_s3_gateway::pinning::config::ProviderLimits {
            priority: 1,
            max_bytes: 1000,
            max_pins: 100,
            enabled: true,
        },
    )]
    .into();
    let now = store::database_clock::database_now(&db).await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            quota::reserve_unique(&db, &target, "cid", 7, &limits, now)
                .await
                .unwrap(),
            quota::ReservationOutcome::Reused
        );
    }
    let usage = quota::read_usage(&db, &target).await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
    let evidence = ledger::get(&db, &target, "cid").await.unwrap().unwrap();
    assert_eq!(
        (evidence.effect.as_str(), evidence.ownership.as_str()),
        ("retained", "unknown")
    );
    assert!(
        !ledger::route_matches(&db, &target, "cid", Some(&identity))
            .await
            .unwrap()
    );
    assert!(!ledger::cleanup_allowed(&db, &target, "cid").await.unwrap());
    assert_eq!(
        ledger::status(&db, &target, "cid")
            .await
            .unwrap()
            .unwrap()
            .route_provenance,
        "migration_declaration_not_remote_evidence"
    );
}

async fn replace_empty_receipt_table(db: &DatabaseConnection, table: &str, old: &str, new: &str) {
    let sql: String = db
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=$1",
            [table.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "sql")
        .unwrap();
    assert!(
        sql.contains(old),
        "fixture must actually remove the requested guarantee"
    );
    let triggers: Vec<String> = db
        .query_all(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE type='trigger' AND tbl_name=$1 ORDER BY name",
            [table.into()],
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get("", "sql").unwrap())
        .collect();
    db.execute_unprepared(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    db.execute_unprepared(&sql.replacen(old, new, 1))
        .await
        .unwrap();
    for trigger in triggers {
        db.execute_unprepared(&trigger).await.unwrap();
    }
}

#[tokio::test]
async fn registered_r26_missing_either_receipt_table_is_not_a_known_old_schema() {
    for table in ["pin_legacy_migration_items", "pin_legacy_migrations"] {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        let declaration = declaration(&identity, "old");
        let before = legacy_migration::inventory(&db, &declaration)
            .await
            .unwrap();
        if table == "pin_legacy_migrations" {
            db.execute_unprepared("DROP TABLE pin_legacy_migration_items")
                .await
                .unwrap();
        }
        db.execute_unprepared(&format!("DROP TABLE {table}"))
            .await
            .unwrap();
        let registered:i64=db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM seaql_migrations WHERE version='m20261007_000001_legacy_pinning_migration'"))
            .await.unwrap().unwrap().try_get("","n").unwrap();
        assert_eq!(registered, 1);
        assert!(
            legacy_migration::inventory(&db, &declaration)
                .await
                .is_err()
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            97
        );
        assert!(before.receipt_frontier.is_empty());
    }
}

#[tokio::test]
async fn registered_r26_receipt_pk_and_constraints_are_verified_not_only_column_names() {
    for (old, new) in [
        (", PRIMARY KEY(operation_id,ordinal)", ""),
        (", UNIQUE(operation_id,table_kind,original_key)", ""),
        (
            " REFERENCES pin_legacy_migrations(id) ON DELETE RESTRICT",
            "",
        ),
        (" CHECK(ordinal>=0)", ""),
        ("ordinal INTEGER NOT NULL", "ordinal INTEGER"),
        ("ordinal INTEGER NOT NULL", "ordinal TEXT NOT NULL"),
    ] {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        let declaration = declaration(&identity, "old");
        replace_empty_receipt_table(&db, "pin_legacy_migration_items", old, new).await;
        assert!(
            legacy_migration::inventory(&db, &declaration)
                .await
                .is_err(),
            "same column names must not hide damaged guarantee: {old}"
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            97
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .reserved_pins,
            3
        );
        assert!(
            ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn registered_r26_operation_receipt_constraints_are_verified_read_only() {
    for (old, new) in [
        ("id TEXT PRIMARY KEY NOT NULL", "id TEXT NOT NULL"),
        (
            "reverses TEXT UNIQUE REFERENCES",
            "reverses TEXT REFERENCES",
        ),
        (" CHECK(direction IN ('apply','rollback'))", ""),
        (" CHECK(item_count>0 AND item_count<=10100)", ""),
        (
            ", CHECK ((direction='apply' AND reverses IS NULL) OR (direction='rollback' AND reverses IS NOT NULL))",
            "",
        ),
    ] {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        replace_empty_receipt_table(&db, "pin_legacy_migrations", old, new).await;
        assert!(
            legacy_migration::inventory(&db, &declaration(&identity, "old"))
                .await
                .is_err(),
            "damaged operation receipt guarantee: {old}"
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            97
        );
    }
}

#[cfg(test)]
mod review_regressions {
    use super::*;
    use ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot;
    use sea_orm::{ColumnTrait, QueryFilter};
    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

    async fn seed_ledger(
        db: &DatabaseConnection,
        cid: &str,
        route: Option<&ProviderRouteSnapshot>,
        effect: &str,
    ) {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ('old',$1,$2,'unknown',$3)",
            [cid.into(),route.map(|r|serde_json::to_string(r).unwrap()).into(),effect.into()])).await.unwrap();
    }

    #[tokio::test]
    async fn complete_bindings_do_not_override_known_backend_type_or_account() {
        for conflict in ["backend", "resource_type", "scope"] {
            let (db, identity) = fixture().await;
            seed_usage(&db).await;
            let mut wrong = identity.route_snapshot();
            match conflict {
                "backend" => wrong.backend = "foreign-backend".into(),
                "resource_type" => wrong.api_profile = "pinata-v3".into(),
                _ => wrong.scope = "different-account".into(),
            };
            seed_ledger(&db, "good", Some(&identity.route_snapshot()), "unknown").await;
            seed_ledger(&db, "conflicting", Some(&wrong), "unknown").await;
            let mut declaration = declaration(&identity, "old");
            add_binding(
                &mut declaration,
                "remote_pin_ledger",
                json!({"provider":"old","cid":"good"}),
                Some(identity.route_snapshot()),
                None,
            );
            add_binding(
                &mut declaration,
                "remote_pin_ledger",
                json!({"provider":"old","cid":"conflicting"}),
                Some(wrong.clone()),
                None,
            );
            assert!(
                legacy_migration::inventory(&db, &declaration)
                    .await
                    .is_err(),
                "complete bindings cannot override {conflict}"
            );
            let rows = ipfs_s3_gateway::store::entities::remote_pin_ledger::Entity::find()
                .all(&db)
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert!(
                rows.iter()
                    .all(|r| r.provider == "old" && r.effect == "unknown")
            );
            assert_eq!(
                rows.iter()
                    .find(|r| r.cid == "conflicting")
                    .unwrap()
                    .route
                    .as_deref(),
                Some(serde_json::to_string(&wrong).unwrap().as_str())
            );
            assert_eq!(
                quota::read_usage(&db, "old")
                    .await
                    .unwrap()
                    .unwrap()
                    .reserved_bytes,
                97
            );
            assert!(
                ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                    .all(&db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn invocation_and_detached_resource_identity_are_in_the_closure() {
        for detached in [false, true] {
            let (db, identity) = fixture().await;
            seed_usage(&db).await;
            let mut declaration = declaration(&identity, "old");
            if detached {
                let resources=json!([{"resource":{"resource_type":"hosted_file","cid":CID,"request_id":"foreign","status":"reported","ownership":"unknown"},
                    "key":{"backend":"noop","scope":"different-account","resource_type":"hosted_file","resource_id":CID}}]).to_string();
                db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "INSERT INTO pin_submit_observations(id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at) VALUES ('detached-conflict','missing',1,'old',$1,$2,1,CURRENT_TIMESTAMP,'unknown',$3,'unknown',1,CURRENT_TIMESTAMP)",
                    [CID.into(),serde_json::to_string(&identity.route_snapshot()).unwrap().into(),resources.into()])).await.unwrap();
                add_binding(
                    &mut declaration,
                    "pin_submit_observations",
                    json!({"id":"detached-conflict"}),
                    Some(identity.route_snapshot()),
                    None,
                );
            } else {
                db.execute_unprepared("INSERT INTO pin_jobs(id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('archived-job','unpin','old','cid',1,'done',CURRENT_TIMESTAMP,NULL)").await.unwrap();
                let mut wrong = identity.route_snapshot();
                wrong.scope = "different-account".into();
                db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "INSERT INTO pin_invocation_routes(job_id,route,remote_epoch) VALUES ('archived-job',$1,1)",[serde_json::to_string(&wrong).unwrap().into()])).await.unwrap();
                add_binding(
                    &mut declaration,
                    "pin_jobs",
                    json!({"id":"archived-job"}),
                    None,
                    None,
                );
            }
            assert!(
                legacy_migration::inventory(&db, &declaration)
                    .await
                    .is_err(),
                "associated invocation/detached resource fact must not be replaced by a declaration"
            );
            assert_eq!(
                quota::read_usage(&db, "old")
                    .await
                    .unwrap()
                    .unwrap()
                    .reserved_pins,
                3
            );
            assert!(
                ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                    .all(&db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    async fn unknown_fixture() -> (
        DatabaseConnection,
        ProviderIdentity,
        legacy_migration::Declaration,
        ProviderRouteSnapshot,
    ) {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        let mut old = identity.route_snapshot();
        old.provider_id = "legacy:old".into();
        old.scope = "legacy:old".into();
        old.storage_domain = "legacy:old".into();
        seed_ledger(&db, "cid", Some(&old), "unknown").await;
        let mut declaration = declaration(&identity, "old");
        add_binding(
            &mut declaration,
            "remote_pin_ledger",
            json!({"provider":"old","cid":"cid"}),
            Some(old.clone()),
            None,
        );
        (db, identity, declaration, old)
    }

    #[tokio::test]
    async fn legacy_scope_is_unknown_not_a_false_account_conflict() {
        let (db, identity, declaration, old) = unknown_fixture().await;
        let plan = legacy_migration::inventory(&db, &declaration)
            .await
            .unwrap();
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .unwrap();
        assert_eq!(
            ledger::get(&db, &identity.allocation_key(), "cid")
                .await
                .unwrap()
                .unwrap()
                .route
                .unwrap(),
            serde_json::to_string(&old).unwrap()
        );
        assert!(
            !ledger::route_matches(&db, &identity.allocation_key(), "cid", Some(&identity))
                .await
                .unwrap()
        );
        let moved = ledger::get(&db, &identity.allocation_key(), "cid")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (moved.ownership.as_str(), moved.effect.as_str()),
            ("unknown", "unknown")
        );
        let usage = quota::read_usage(&db, &identity.allocation_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
        assert!(
            !ledger::cleanup_allowed(&db, &identity.allocation_key(), "cid")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn unknown_preservation_requires_complete_bindings_and_unambiguous_original_alias() {
        let (db, identity, mut declaration, old) = unknown_fixture().await;
        let complete = declaration.clone();
        declaration.namespaces[0].bindings.pop();
        assert!(
            legacy_migration::inventory(&db, &declaration)
                .await
                .is_err()
        );
        // The complete declaration qualifies before any conflicting registry fact.
        legacy_migration::inventory(&db, &complete).await.unwrap();
        ledger::register_route(&db, "old", &identity).await.unwrap();
        let mut complete = complete;
        add_binding(
            &mut complete,
            "pin_provider_routes",
            json!({"provider":"old"}),
            Some(identity.route_snapshot()),
            None,
        );
        assert!(legacy_migration::inventory(&db, &complete).await.is_err());
        assert_eq!(
            ledger::get(&db, "old", "cid")
                .await
                .unwrap()
                .unwrap()
                .route
                .unwrap(),
            serde_json::to_string(&old).unwrap()
        );
        let usage = ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
    }

    #[tokio::test]
    async fn unknown_preservation_does_not_bypass_current_authority_failures() {
        for fault in [
            "readiness",
            "global_unknown",
            "pending",
            "seal",
            "membership",
            "original",
        ] {
            let (db, identity, declaration, old) = unknown_fixture().await;
            let plan = legacy_migration::inventory(&db, &declaration)
                .await
                .unwrap();
            let source = ipfs_s3_gateway::store::pinning::management::records::locator::source_key(
                &ipfs_s3_gateway::store::pinning::management::native::NativeKey {
                    table: "pin_provider_routes".into(),
                    values: vec![identity.allocation_key()],
                },
            )
            .unwrap();
            match fault {
                "readiness" => {
                    db.execute_unprepared(
                        "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
                    )
                    .await
                    .unwrap();
                }
                "global_unknown" | "pending" => {
                    let (kind, key) = if fault == "global_unknown" {
                        ("unknown", "*".to_owned())
                    } else {
                        (
                            "registered_route",
                            serde_json::to_string(&identity.allocation_key()).unwrap(),
                        )
                    };
                    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                        "INSERT INTO pin_management_pending_scope(selector_kind,selector_key,source_key,source_generation,locator_generation) SELECT $1,$2,source_key,source_generation,locator_generation FROM pin_management_source_control WHERE source_key=$3",
                        [kind.into(), key.into(), source.clone().into()])).await.unwrap();
                }
                "seal" => {
                    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "UPDATE pin_management_source_control SET sealed_source=NULL WHERE source_key=$1", [source.clone().into()])).await.unwrap();
                }
                "membership" => {
                    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                    "INSERT INTO pin_management_locator(selector_kind,selector_key,source_key) VALUES ('registered_route','\"unrecorded-alias\"',$1)", [source.into()])).await.unwrap();
                }
                _ => {
                    db.execute_unprepared("UPDATE pin_management_records SET binding='{invalid' WHERE kind='database_scope'").await.unwrap();
                }
            }
            if fault != "readiness" {
                ipfs_s3_gateway::store::pinning::management::schema::validate(&db)
                    .await
                    .unwrap();
            }
            assert!(
                legacy_migration::inventory(&db, &declaration)
                    .await
                    .is_err(),
                "{fault}"
            );
            assert!(
                legacy_migration::apply(&db, &declaration, &plan)
                    .await
                    .is_err(),
                "{fault}"
            );
            let usage =
                ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
            let ledger = ipfs_s3_gateway::store::entities::remote_pin_ledger::Entity::find_by_id((
                "old".to_owned(),
                "cid".to_owned(),
            ))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                (ledger.ownership.as_str(), ledger.effect.as_str()),
                ("unknown", "unknown")
            );
            assert_eq!(ledger.route.unwrap(), serde_json::to_string(&old).unwrap());
            assert!(
                ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                    .all(&db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn unknown_preservation_keeps_source_alias_and_destination_retirement_checks() {
        for source_alias in [true, false] {
            let (db, identity, mut declaration, old) = unknown_fixture().await;
            if source_alias {
                let mut registered = identity.clone();
                registered.provider_id = old.provider_id.clone();
                registered.scope = old.scope.clone();
                registered.storage_domain = old.storage_domain.clone();
                ledger::register_route(&db, "old", &registered)
                    .await
                    .unwrap();
                add_binding(
                    &mut declaration,
                    "pin_provider_routes",
                    json!({"provider":"old"}),
                    Some(old),
                    None,
                );
            }
            let plan = legacy_migration::inventory(&db, &declaration)
                .await
                .unwrap();
            let key = if source_alias {
                "old".to_owned()
            } else {
                identity.allocation_key()
            };
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE pin_provider_routes SET retired=1 WHERE provider=$1",
                [key.into()],
            ))
            .await
            .unwrap();
            assert!(
                legacy_migration::inventory(&db, &declaration)
                    .await
                    .is_err()
            );
            assert!(
                legacy_migration::apply(&db, &declaration, &plan)
                    .await
                    .is_err()
            );
            let usage =
                ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
            assert!(
                ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                    .all(&db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn frozen_candidate_and_target_epoch_history_conflicts_refuse_whole_inventory() {
        for target_history in [false, true] {
            let (db, identity) = fixture().await;
            seed_usage(&db).await;
            let mut wrong = identity.route_snapshot();
            wrong.scope = "different-account".into();
            let mut declaration = declaration(&identity, "old");
            if target_history {
                let archive=json!({"ledger":{"provider":identity.allocation_key(),"cid":"target-past","route":serde_json::to_string(&wrong).unwrap()},"confirmed_absent":true}).to_string();
                db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ($1,'target-past',1,$2)",[identity.allocation_key().into(),archive.into()])).await.unwrap();
            } else {
                let contract=json!({"provider_mode":"all","content_mode":"full","content_cids":[CID],"candidate_order":["old"],"candidates":{"old":wrong}}).to_string();
                db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_lease_requirements(lease_id,owner_object_id,contract,created_at) VALUES ('frozen','original-owner',$1,CURRENT_TIMESTAMP)",[contract.into()])).await.unwrap();
                for pointer in [
                    "/contract/candidates/@key/old",
                    "/contract/candidate_order/0",
                ] {
                    add_binding(
                        &mut declaration,
                        "pin_lease_requirements",
                        json!({"lease_id":"frozen"}),
                        None,
                        Some(pointer),
                    );
                }
            }
            assert!(
                legacy_migration::inventory(&db, &declaration)
                    .await
                    .is_err()
            );
            assert_eq!(
                quota::read_usage(&db, "old")
                    .await
                    .unwrap()
                    .unwrap()
                    .reserved_bytes,
                97
            );
            assert!(
                ipfs_s3_gateway::store::entities::pin_legacy_migration::Entity::find()
                    .all(&db)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn declaration_does_not_confirm_a_null_route_even_if_effect_was_confirmed() {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        seed_ledger(&db, CID, None, "confirmed").await;
        let mut declaration = declaration(&identity, "old");
        add_binding(
            &mut declaration,
            "remote_pin_ledger",
            json!({"provider":"old","cid":CID}),
            None,
            None,
        );
        let plan = legacy_migration::inventory(&db, &declaration)
            .await
            .unwrap();
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .unwrap();
        assert!(
            !ipfs_s3_gateway::store::entities::pin_lease_requirement::contract::target_confirmed(
                &db,
                "pre-stage6",
                &identity.allocation_key(),
                CID,
                store::database_clock::database_now(&db).await.unwrap()
            )
            .await
            .unwrap()
        );
        assert_eq!(
            ledger::get(&db, &identity.allocation_key(), CID)
                .await
                .unwrap()
                .unwrap()
                .effect,
            "confirmed"
        );
    }

    async fn migrated_live_hold() -> (DatabaseConnection, ProviderIdentity) {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old',$1,7,'reserved',42,CURRENT_TIMESTAMP)",[CID.into()])).await.unwrap();
        seed_ledger(&db, CID, None, "reserved").await;
        let mut declaration = declaration(&identity, "old");
        for table in ["remote_pins", "remote_pin_ledger"] {
            add_binding(
                &mut declaration,
                table,
                json!({"provider":"old","cid":CID}),
                None,
                None,
            );
        }
        let plan = legacy_migration::inventory(&db, &declaration)
            .await
            .unwrap();
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .unwrap();
        (db, identity)
    }

    #[tokio::test]
    async fn reference_epoch_and_fresh_job_id_are_not_new_allocation_proof() {
        let (db, identity) = migrated_live_hold().await;
        let target = identity.allocation_key();
        let now = store::database_clock::database_now(&db).await.unwrap();
        let limits = [(
            target.clone(),
            ipfs_s3_gateway::pinning::config::ProviderLimits {
                priority: 1,
                max_bytes: 1000,
                max_pins: 100,
                enabled: true,
            },
        )]
        .into();
        assert_eq!(
            quota::reserve_unique(&db, &target, CID, 7, &limits, now)
                .await
                .unwrap(),
            quota::ReservationOutcome::Reused
        );
        let binding = legacy_migration::resource_binding(&db, &target, CID)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(binding.historical_epoch, Some(42));
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_jobs(id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('reference-submit','submit',$1,$2,'lease','target',1,'pending',CURRENT_TIMESTAMP,'ready')",[target.clone().into(),CID.into()])).await.unwrap();
        ledger::capture_invocation(&db, "reference-submit", &target, CID)
            .await
            .unwrap();
        let job = ipfs_s3_gateway::store::entities::pin_job::Entity::find_by_id("reference-submit")
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(!legacy_migration::job_io_allowed(&db, &job).await.unwrap());
        let usage = quota::read_usage(&db, &target).await.unwrap().unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
    }

    #[tokio::test]
    async fn post_receipt_confirmed_release_and_reallocation_end_the_old_resource_mask() {
        use sea_orm::TransactionTrait;
        let (db, identity) = migrated_live_hold().await;
        let target = identity.allocation_key();
        let now = store::database_clock::database_now(&db).await.unwrap();
        let txn = db.begin().await.unwrap();
        assert_eq!(
            quota::confirmed_release(&txn, &target, CID, 42, now)
                .await
                .unwrap(),
            quota::ConfirmedReleaseOutcome::Released
        );
        txn.commit().await.unwrap();
        let limits = [(
            target.clone(),
            ipfs_s3_gateway::pinning::config::ProviderLimits {
                priority: 1,
                max_bytes: 1000,
                max_pins: 100,
                enabled: true,
            },
        )]
        .into();
        let txn = db.begin().await.unwrap();
        assert_eq!(
            quota::reserve_unique(&txn, &target, CID, 7, &limits, now)
                .await
                .unwrap(),
            quota::ReservationOutcome::Reserved
        );
        txn.commit().await.unwrap();
        assert!(
            legacy_migration::resource_binding(&db, &target, CID)
                .await
                .unwrap()
                .is_none()
        );
        let history = legacy_migration::release_history(&db, &target, CID)
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].epoch, 42);
        let usage = quota::read_usage(&db, &target).await.unwrap().unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
    }

    #[tokio::test]
    async fn migrated_absent_lifetime_allows_new_submit_but_never_revives_old_submit() {
        use ipfs_s3_gateway::{
            config::Config,
            pinning::{
                config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
                coordinator::PinningCoordinator,
                policy::{LeaseIntent, LeaseSource, PublicationPolicy},
                tags::ContentMode,
            },
            store::{
                Store,
                pinning::publication::{
                    self, PinTargetSpec, PublicationObject, PublicationRequest,
                },
            },
        };
        use std::time::Duration;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"requestid":"fresh-r26","status":"pinned","pin":{"cid":CID}}),
            ))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"count":0,"results":[]})))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let text = format!(
            "[pinning]\nworker_interval='1s'\n[[pinning.providers]]\nname='configured'\nkind='filebase'\npriority=1\nmax_bytes=1000\nmax_pins=100\ntoken_env='R26_TEST_TOKEN'\nendpoint='{}'\n[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='configured'\nprovider_id='explicit-account'\ndisplay_name='configured'\nbackend='filebase'\nscope='account'\nstorage_domain='fixture'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:R26_TEST_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='retain'\n",
            server.uri()
        );
        let config: Config = toml::from_str(&text).unwrap();
        let runtime = PinningCoordinator::build(
            ValidatedPinningConfig::from_config(&config, |_| Some("test-only-token".into()))
                .unwrap(),
        )
        .unwrap();
        let identity = runtime.effective_config().providers[0].identity.clone();
        let (db, _) = fixture().await;
        runtime
            .register_identities(&Store::new(db.clone()))
            .await
            .unwrap();
        seed_usage(&db).await;
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('old',$1,7,'absent',42,CURRENT_TIMESTAMP)",[CID.into()])).await.unwrap();
        seed_ledger(&db, CID, None, "absent").await;
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO pin_jobs(id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('legacy-submit','submit','old',$1,'legacy-lease','legacy-target',1,'done',CURRENT_TIMESTAMP,'ready')",[CID.into()])).await.unwrap();
        let mut declaration = declaration(&identity, "old");
        for table in ["remote_pins", "remote_pin_ledger"] {
            add_binding(
                &mut declaration,
                table,
                json!({"provider":"old","cid":CID}),
                None,
                None,
            );
        }
        add_binding(
            &mut declaration,
            "pin_jobs",
            json!({"id":"legacy-submit"}),
            None,
            None,
        );
        let plan = legacy_migration::inventory(&db, &declaration)
            .await
            .unwrap();
        legacy_migration::apply(&db, &declaration, &plan)
            .await
            .unwrap();
        store::bucket::create(&db, "r26-lifetimes", None)
            .await
            .unwrap();
        publication::publish_object(
            &db,
            PublicationRequest {
                object: PublicationObject::from_put(
                    "new-owner".into(),
                    "r26-lifetimes",
                    "new",
                    CID.into(),
                    7,
                    None,
                    None,
                    false,
                    None,
                    None,
                    chrono::Utc::now(),
                ),
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![LeaseIntent {
                        source: LeaseSource::Automatic,
                        policy_id: "r26-new-lifetime".into(),
                        provider_mode: ProviderMode::All,
                        providers: vec![identity.allocation_key()],
                        content_mode: ContentMode::Object,
                        duration: LeaseDuration::parse("1h").unwrap(),
                    }],
                },
                object_target: PinTargetSpec {
                    cid: CID.into(),
                    logical_size: 7,
                },
            },
            runtime.provider_limits(),
        )
        .await
        .unwrap();
        let new_job = ipfs_s3_gateway::store::entities::pin_job::Entity::find()
            .filter(ipfs_s3_gateway::store::entities::pin_job::Column::Operation.eq("submit"))
            .filter(ipfs_s3_gateway::store::entities::pin_job::Column::Id.ne("legacy-submit"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let old_job =
            ipfs_s3_gateway::store::entities::pin_job::Entity::find_by_id("legacy-submit")
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert!(
            !legacy_migration::job_io_allowed(&db, &old_job)
                .await
                .unwrap()
        );
        assert!(
            legacy_migration::job_io_allowed(&db, &new_job)
                .await
                .unwrap(),
            "normal new allocation must not inherit the old receipt's lifetime quarantine"
        );
        let worker = runtime.start(
            Store::new(db.clone()),
            tokio_util::sync::CancellationToken::new(),
        );
        let completed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let row = ipfs_s3_gateway::store::entities::remote_pin::Entity::find_by_id((
                    identity.allocation_key(),
                    CID.to_owned(),
                ))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
                if row.status == "pinned" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        worker.shutdown(Duration::from_secs(2)).await;
        assert!(
            completed.is_ok(),
            "fresh Submit must actually execute through the worker"
        );
        server.verify().await;
        let usage = quota::read_usage(&db, &identity.allocation_key())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (104, 4));
        assert!(
            !legacy_migration::job_io_allowed(&db, &old_job)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn registered_r26_missing_immutable_or_item_bound_trigger_is_refused_read_only() {
    for trigger in [
        "pin_legacy_migrations_no_update",
        "pin_legacy_migrations_no_delete",
        "pin_legacy_migration_items_no_update",
        "pin_legacy_migration_items_no_delete",
        "pin_legacy_item_bound",
    ] {
        let (db, identity) = fixture().await;
        seed_usage(&db).await;
        let declaration = declaration(&identity, "old");
        db.execute_unprepared(&format!("DROP TRIGGER {trigger}"))
            .await
            .unwrap();
        assert!(
            legacy_migration::inventory(&db, &declaration)
                .await
                .is_err(),
            "missing receipt protection: {trigger}"
        );
        assert_eq!(
            ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            97
        );
    }
}

#[tokio::test]
async fn unregistered_receipt_tables_are_not_a_known_schema() {
    let (db, identity) = fixture().await;
    seed_usage(&db).await;
    db.execute_unprepared(
        "DELETE FROM seaql_migrations WHERE version='m20261027_000001_schema_baseline'",
    )
    .await
    .unwrap();
    assert!(
        legacy_migration::inventory(&db, &declaration(&identity, "old"))
            .await
            .is_err()
    );
    assert_eq!(
        ipfs_s3_gateway::store::entities::pin_provider_usage::Entity::find_by_id("old")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        97
    );
}
