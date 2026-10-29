use super::{file_catalog, migration, native_catalog};
use crate::store::pinning::{ledger, management::schema as management_schema, quota};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, Statement, TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, SchemaManager};
use std::collections::BTreeSet;

const ENDPOINTS: [&str; 4] = [
    "m20261004_000001_rpc_submission_ledger",
    "m20261004_000002_cluster_replica_contract",
    migration::SCHEMA,
    "m20261008_000001_zip_intent_capture",
];

pub(super) async fn declaration(db: &DatabaseConnection) -> migration::Declaration {
    let identity = crate::pinning::identity::ProviderIdentity {
        provider_id: "r26-account".into(),
        display_name: "R26 fixture".into(),
        backend: "noop".into(),
        scope: "account".into(),
        storage_domain: "fixture".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "noop".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: crate::pinning::identity::CleanupMode::Retain,
    };
    if crate::store::initialization::classify(db).await.unwrap()
        == crate::store::initialization::SchemaState::Current
    {
        ledger::register_route(db, &identity.allocation_key(), &identity)
            .await
            .unwrap();
    } else {
        super::seed_historical_route(db, &identity).await;
    }
    db.execute_unprepared(
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)",
    )
    .await
    .unwrap();
    serde_json::from_value(serde_json::json!({
        "version": 1, "database_scope": "fixture", "ttl_seconds": 300,
        "maintenance": {"all_writers_stopped": true, "all_workers_drained": true},
        "namespaces": [{"source": "old", "target": identity.allocation_key(),
            "backend": "noop", "scope": "account", "resource_type": identity.resource_type(),
            "binding_only": false, "target_route": identity.route_snapshot(), "bindings": [{
                "table": "pin_provider_usage", "key": {"provider": "old"},
                "provider": "old", "historical_route": null
            }]}]
    }))
    .unwrap()
}

pub(super) async fn receipt_rows(db: &DatabaseConnection) -> Vec<migration::NativeRow> {
    db.query_all(Statement::from_string(
        db.get_database_backend(),
        "SELECT operation_id,CAST(ordinal AS TEXT) AS ordinal,table_kind,original_key,before_image,after_image,bindings FROM pin_legacy_migration_items ORDER BY operation_id,ordinal",
    ))
    .await
    .unwrap()
    .iter()
    .map(|row| {
        ["operation_id", "ordinal", "table_kind", "original_key", "before_image", "after_image", "bindings"]
            .into_iter()
            .map(|column| (column.into(), row.try_get("", column).unwrap()))
            .collect()
    })
    .collect()
}

#[tokio::test]
async fn all_four_historical_endpoints_keep_absent_management_and_original_authority() {
    for endpoint in ENDPOINTS {
        let (_directory, db) = file_catalog(endpoint).await;
        let declaration = declaration(&db).await;
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        assert_eq!(plan.schema.last().map(String::as_str), Some(endpoint));
        assert!(
            plan.before
                .iter()
                .all(|table| !table.table.starts_with("pin_management_"))
        );
        assert!(!migration::source_is_migrated(&db, "old").await.unwrap());
        assert!(
            migration::resolve_legacy_binding(
                &db,
                "pin_provider_usage",
                &[("provider".into(), "old".into())].into(),
                "old",
            )
            .await
            .unwrap()
            .is_none()
        );
        let reader = db.begin().await.unwrap();
        reader
            .execute_unprepared("PRAGMA query_only=ON")
            .await
            .unwrap();
        assert_eq!(
            migration::resource_key(&reader, "old", "unmapped-cid")
                .await
                .unwrap(),
            "old"
        );
        assert!(
            migration::resource_binding(&reader, "old", "unmapped-cid")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            migration::release_history(&reader, "old", "unmapped-cid")
                .await
                .unwrap()
                .is_empty()
        );
        reader
            .execute_unprepared("PRAGMA query_only=OFF")
            .await
            .unwrap();
        reader.rollback().await.unwrap();
        for table in ["pin_replica_observations", "pin_lease_requirements"] {
            assert_eq!(
                plan.before
                    .iter()
                    .find(|image| image.table == table)
                    .unwrap()
                    .columns
                    .is_empty(),
                endpoint == ENDPOINTS[0]
            );
        }
        if matches!(
            endpoint,
            migration::SCHEMA | "m20261008_000001_zip_intent_capture"
        ) {
            let receipt = migration::apply(&db, &declaration, &plan).await.unwrap();
            assert_eq!(
                migration::apply(&db, &declaration, &plan).await.unwrap(),
                receipt
            );
            assert!(migration::source_is_migrated(&db, "old").await.unwrap());
            let resolved = migration::resolve_legacy_binding(
                &db,
                "pin_provider_usage",
                &[("provider".into(), "old".into())].into(),
                "old",
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(resolved.operational_key, declaration.namespaces[0].target);
            assert_eq!(resolved.receipt_id, receipt.id);
            let exhausted =
                crate::store::pinning::management::records::SharedDecodeBudget::default();
            exhausted
                .lock()
                .unwrap()
                .consume(crate::pinning::management::wire::MAX_WIRE_NODES, 0)
                .unwrap();
            assert!(matches!(
                migration::source_is_migrated_with_work(
                    &db,
                    "old",
                    crate::store::pinning::management::records::DecodeWork::shared(&exhausted),
                )
                .await,
                Err(crate::store::pinning::management::ManagementError::Budget)
            ));
            assert_eq!(
                migration::historical_keys(
                    &db,
                    "pin_provider_usage",
                    &declaration.namespaces[0].target,
                    None,
                )
                .await
                .unwrap(),
                [declaration.namespaces[0].target.clone(), "old".into()]
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            );
            let reverse = migration::rollback(&db, &receipt.id, &plan).await.unwrap();
            assert_eq!(
                migration::rollback(&db, &receipt.id, &plan).await.unwrap(),
                reverse
            );
            assert!(!migration::source_is_migrated(&db, "old").await.unwrap());
            assert!(
                migration::resolve_legacy_binding(
                    &db,
                    "pin_provider_usage",
                    &[("provider".into(), "old".into())].into(),
                    "old",
                )
                .await
                .unwrap()
                .is_none()
            );
        } else {
            assert!(migration::apply(&db, &declaration, &plan).await.is_err());
            assert_eq!(
                super::super::read(&db, &plan.schema).await.unwrap(),
                plan.before
            );
            super::upgrade(&db).await;
            assert!(migration::apply(&db, &declaration, &plan).await.is_err());
            let fresh = migration::inventory(&db, &declaration).await.unwrap();
            migration::apply(&db, &declaration, &fresh).await.unwrap();
        }
        assert_eq!(
            crate::store::entities::pin_provider_usage::Entity::find_by_id("old")
                .one(&db)
                .await
                .unwrap()
                .map(|row| (row.reserved_bytes, row.reserved_pins)),
            if matches!(
                endpoint,
                migration::SCHEMA | "m20261008_000001_zip_intent_capture"
            ) {
                Some((97, 3))
            } else {
                None
            }
        );
        super::upgrade(&db).await;
        assert_eq!(
            quota::read_usage(&db, "old")
                .await
                .unwrap()
                .map(|row| (row.reserved_bytes, row.reserved_pins)),
            Some((97, 3))
        );
    }
}

#[tokio::test]
async fn historical_resource_identity_reads_preserve_mapping_reversal_and_shared_budget() {
    use crate::store::pinning::management::{ManagementError, records};
    for endpoint in &ENDPOINTS[2..] {
        let (_directory, db) = file_catalog(endpoint).await;
        let mut declaration = declaration(&db).await;
        db.execute_unprepared("INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('old','historical-resource',1,'reserved',1,0,CURRENT_TIMESTAMP)")
            .await.unwrap();
        let release_image =
            r#"{"confirmed_absent":true,"ledger":{"provider":"old","cid":"historical-resource"}}"#;
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ('old','historical-resource',0,$1)",
            [release_image.into()])).await.unwrap();
        for (table, epoch) in [("remote_pins", None), ("pin_resource_history", Some("0"))] {
            let mut key = std::collections::BTreeMap::from([
                ("provider".into(), "old".into()),
                ("cid".into(), "historical-resource".into()),
            ]);
            if let Some(epoch) = epoch {
                key.insert("epoch".into(), epoch.into());
            }
            declaration.namespaces[0].bindings.push(migration::Binding {
                table: table.into(),
                key,
                provider: "old".into(),
                pointer: None,
                historical_route: None,
            });
        }
        let target = &declaration.namespaces[0].target;
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
        let reader = db.begin().await.unwrap();
        reader
            .execute_unprepared("PRAGMA query_only=ON")
            .await
            .unwrap();
        assert_eq!(
            migration::admission_historical_keys(&reader, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            BTreeSet::from([target.clone(), "old".into()])
        );
        for provider in ["old", target.as_str()] {
            let budget = records::SharedDecodeBudget::default();
            assert_eq!(
                migration::resource_key_budgeted(
                    &reader,
                    provider,
                    "historical-resource",
                    records::DecodeWork::shared(&budget)
                )
                .await
                .unwrap(),
                *target
            );
            let binding = migration::resource_binding(&reader, provider, "historical-resource")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(binding.operational_key, *target);
            assert_eq!(binding.receipt_id, applied.id);
            assert_eq!(binding.historical_epoch, Some(1));
            let releases = migration::release_history(&reader, provider, "historical-resource")
                .await
                .unwrap();
            assert_eq!(releases.len(), 1);
            assert_eq!(releases[0].provider, "old");
            assert_eq!(releases[0].cid, "historical-resource");
            assert_eq!(releases[0].epoch, 0);
            assert_eq!(releases[0].ledger, release_image);
        }
        let exhausted = records::SharedDecodeBudget::default();
        exhausted
            .lock()
            .unwrap()
            .consume(crate::pinning::management::wire::MAX_WIRE_NODES, 0)
            .unwrap();
        let work = records::DecodeWork::shared(&exhausted);
        assert!(matches!(
            migration::resource_key_budgeted(&reader, "old", "historical-resource", work).await,
            Err(ManagementError::Budget)
        ));
        assert!(matches!(
            migration::resource_binding_with_work(&reader, "old", "historical-resource", work)
                .await,
            Err(ManagementError::Budget)
        ));
        assert!(matches!(
            migration::release_history_with_work(&reader, "old", "historical-resource", work).await,
            Err(ManagementError::Budget)
        ));
        reader
            .execute_unprepared("PRAGMA query_only=OFF")
            .await
            .unwrap();
        reader.rollback().await.unwrap();
        migration::rollback(&db, &applied.id, &plan).await.unwrap();
        let reader = db.begin().await.unwrap();
        reader
            .execute_unprepared("PRAGMA query_only=ON")
            .await
            .unwrap();
        assert_eq!(
            migration::admission_historical_keys(&reader, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            BTreeSet::from([target.clone()])
        );
        assert_eq!(
            migration::resource_key(&reader, "old", "historical-resource")
                .await
                .unwrap(),
            "old"
        );
        assert!(
            migration::resource_binding(&reader, "old", "historical-resource")
                .await
                .unwrap()
                .is_none()
        );
        let releases = migration::release_history(&reader, "old", "historical-resource")
            .await
            .unwrap();
        assert_eq!(releases.len(), 1);
        assert_eq!(releases[0].ledger, release_image);
        reader
            .execute_unprepared("PRAGMA query_only=OFF")
            .await
            .unwrap();
        reader.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn admission_receipt_keys_preserve_each_table_alias_and_shared_receipt_freshness() {
    use crate::store::pinning::management::{ManagementError, native, records};
    for endpoint in [ENDPOINTS[3], management_schema::VERSION] {
        let (_directory, db) = file_catalog(endpoint).await;
        let mut declaration = declaration(&db).await;
        if endpoint == management_schema::VERSION {
            declaration.database_scope = native::scope::binding(&db).await.unwrap().0;
        }
        let template = declaration.namespaces[0].clone();
        for source in ["old", "submit-only", "history-only"] {
            let mut ns = template.clone();
            ns.source = source.into();
            if source != "old" {
                ns.bindings.clear();
            }
            if source != "history-only" {
                let id = format!("admission-{source}");
                db.execute(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    "INSERT INTO pin_submit_observations(id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,effect,resources,outcome,needs_attention,started_at) VALUES ($1,$1,1,$2,'admission-cid',$3,1,CURRENT_TIMESTAMP,'unknown','[]','in_flight',1,CURRENT_TIMESTAMP)",
                    [
                        id.clone().into(),
                        source.into(),
                        serde_json::to_string(&ns.target_route).unwrap().into(),
                    ],
                ))
                .await
                .unwrap();
                ns.bindings.push(migration::Binding {
                    table: "pin_submit_observations".into(),
                    key: [("id".into(), id)].into(),
                    provider: source.into(),
                    pointer: None,
                    historical_route: Some(ns.target_route.clone()),
                });
            }
            if source != "submit-only" {
                let image = serde_json::json!({
                    "confirmed_absent": true,
                    "ledger": {"provider": source, "cid": "admission-cid"}
                })
                .to_string();
                db.execute(Statement::from_sql_and_values(
                    db.get_database_backend(),
                    "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES ($1,'admission-cid',0,$2)",
                    [source.into(), image.into()],
                ))
                .await
                .unwrap();
                ns.bindings.push(migration::Binding {
                    table: "pin_resource_history".into(),
                    key: [
                        ("provider".into(), source.into()),
                        ("cid".into(), "admission-cid".into()),
                        ("epoch".into(), "0".into()),
                    ]
                    .into(),
                    provider: source.into(),
                    pointer: None,
                    historical_route: None,
                });
            }
            if source == "old" {
                declaration.namespaces[0] = ns;
            } else {
                declaration.namespaces.push(ns);
            }
        }
        let target = &template.target;
        assert_eq!(
            migration::admission_historical_keys(&db, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([target.clone()])
        );
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
        let budget = records::SharedDecodeBudget::default();
        let context = records::DecodeWork::shared(&budget);
        let expected = BTreeSet::from([
            target.clone(),
            "old".into(),
            "submit-only".into(),
            "history-only".into(),
        ]);
        assert_eq!(
            migration::admission_historical_keys(&db, target, &context)
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        let nodes = budget.lock().unwrap().usage().0;
        let limited = records::SharedDecodeBudget::default();
        let prior = crate::pinning::management::wire::MAX_WIRE_NODES - nodes + 1;
        limited.lock().unwrap().consume(prior, 0).unwrap();
        let work = records::DecodeWork::shared(&limited);
        assert!(matches!(
            migration::admission_historical_keys(&db, target, &work).await,
            Err(ManagementError::Budget)
        ));
        assert!(limited.lock().unwrap().usage().0 > prior);
        assert!(matches!(
            migration::admission_historical_keys(&db, target, &work).await,
            Err(ManagementError::Budget)
        ));
        let fault = db.begin().await.unwrap();
        assert!(
            fault
                .execute(Statement::from_sql_and_values(
                    fault.get_database_backend(),
                    "DELETE FROM pin_legacy_migration_items WHERE operation_id=$1 AND ordinal=0",
                    [applied.id.clone().into()],
                ))
                .await
                .is_err()
        );
        assert_eq!(
            migration::admission_historical_keys(&fault, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        fault.rollback().await.unwrap();
        assert_eq!(
            migration::admission_historical_keys(&db, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            expected
        );
        migration::rollback(&db, &applied.id, &plan).await.unwrap();
        assert_eq!(
            migration::admission_historical_keys(&db, target, &())
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([target.clone()])
        );
    }
}

#[tokio::test]
async fn historical_receipt_reads_require_the_complete_active_frontier() {
    let (_directory, db) = file_catalog(ENDPOINTS[3]).await;
    let declaration = declaration(&db).await;
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    migration::apply(&db, &declaration, &plan).await.unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "INSERT INTO pin_legacy_migrations SELECT id||'-incomplete',direction,reverses,database_scope,db_clock,schema_version,declaration,confirmation,item_count FROM pin_legacy_migrations",
    )
    .await
    .unwrap();
    assert!(migration::source_is_migrated(&tx, "old").await.is_err());
    assert!(
        migration::resource_key(&tx, "old", "unmapped-cid")
            .await
            .is_err()
    );
    assert!(
        migration::resource_binding(&tx, "old", "unmapped-cid")
            .await
            .is_err()
    );
    assert!(
        migration::resolve_legacy_binding(
            &tx,
            "pin_provider_usage",
            &[("provider".into(), "old".into())].into(),
            "old",
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn old_committed_receipt_replays_after_upgrade_without_new_table_items_or_history_changes() {
    for endpoint in &ENDPOINTS[2..] {
        let (_directory, db) = file_catalog(endpoint).await;
        let declaration = declaration(&db).await;
        let plan = migration::inventory(&db, &declaration).await.unwrap();
        let receipt = migration::apply(&db, &declaration, &plan).await.unwrap();
        let items = receipt_rows(&db).await;
        let original = super::super::super::receipt::lookup(&db, &receipt.id)
            .await
            .unwrap()
            .unwrap();
        assert!(items.iter().all(|row| {
            !row["table_kind"]
                .as_deref()
                .unwrap()
                .starts_with("pin_management_")
        }));
        super::upgrade(&db).await;
        assert_eq!(
            migration::apply(&db, &declaration, &plan).await.unwrap(),
            receipt
        );
        assert_eq!(receipt_rows(&db).await, items);
        assert_eq!(
            super::super::super::receipt::lookup(&db, &receipt.id)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        let resolved = migration::resolve_legacy_binding(
            &db,
            "pin_provider_usage",
            &[("provider".into(), "old".into())].into(),
            "old",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(resolved.operational_key, declaration.namespaces[0].target);
        assert!(migration::rollback(&db, &receipt.id, &plan).await.is_err());
        let mut rollback = declaration.clone();
        rollback.rollback_receipt = Some(receipt.id.clone());
        assert!(
            migration::rollback_inventory(&db, &receipt.id, &rollback)
                .await
                .is_err()
        );
        assert_eq!(receipt_rows(&db).await, items);
    }
}

#[tokio::test]
async fn historical_prefix_rejects_unregistered_management_tables_without_repair() {
    for endpoint in ENDPOINTS {
        let db = native_catalog(endpoint).await;
        crate::store::migrations::m20261012_000001_pinning_management::Migration
            .up(&SchemaManager::new(&db))
            .await
            .unwrap();
        assert!(super::super::schema(&db, false).await.is_err());
        assert!(migration::source_is_migrated(&db, "old").await.is_err());
        let registered: i64 = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM seaql_migrations WHERE version='m20261012_000001_pinning_management'"))
            .await.unwrap().unwrap().try_get("", "n").unwrap();
        assert_eq!(registered, 0);
    }
}

#[tokio::test]
async fn receipt_reads_recheck_prefix_integrity_and_never_fall_back_from_current_damage() {
    for endpoint in [ENDPOINTS[3], management_schema::VERSION] {
        for damage in [
            "CREATE TEMP VIEW pin_legacy_migrations AS SELECT * FROM main.pin_legacy_migrations",
            "CREATE TEMP VIEW pin_legacy_migration_items AS SELECT * FROM main.pin_legacy_migration_items",
            "CREATE TEMP VIEW seaql_migrations AS SELECT * FROM main.seaql_migrations",
            "ALTER TABLE pin_provider_routes ADD COLUMN unknown_authority TEXT",
            if endpoint == management_schema::VERSION {
                "DELETE FROM seaql_migrations WHERE version='m20261027_000001_schema_baseline'"
            } else {
                "DELETE FROM seaql_migrations WHERE version='m20261004_000002_cluster_replica_contract'"
            },
            "INSERT INTO seaql_migrations(version,applied_at) VALUES ('m99999999_000001_unknown',0)",
            if endpoint == management_schema::VERSION {
                "UPDATE pin_management_readiness SET state='backfill' WHERE id=1"
            } else {
                "CREATE TABLE pin_management_readiness (id INTEGER PRIMARY KEY)"
            },
            if endpoint == management_schema::VERSION {
                "INSERT INTO pin_management_pending_scope VALUES ('unknown','*','unsealed',1,0)"
            } else {
                "CREATE TEMP TABLE pin_management_pending_scope (shadow TEXT)"
            },
            if endpoint == management_schema::VERSION {
                "DROP TABLE pin_management_locator"
            } else {
                "CREATE INDEX s75_partial ON pin_legacy_migrations(id)"
            },
        ] {
            let db = native_catalog(endpoint).await;
            assert!(!migration::source_is_migrated(&db, "old").await.unwrap());
            let tx = db.begin().await.unwrap();
            let changed = tx.execute_unprepared(damage).await.unwrap();
            if damage.starts_with("DELETE FROM seaql_migrations") {
                assert_eq!(changed.rows_affected(), 1, "{endpoint}: {damage}");
            }
            assert!(
                migration::source_is_migrated(&tx, "old").await.is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                migration::resolve_legacy_binding(
                    &tx,
                    "pin_provider_usage",
                    &[("provider".into(), "old".into())].into(),
                    "old",
                )
                .await
                .is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                migration::resource_key(&tx, "old", "unmapped-cid")
                    .await
                    .is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                migration::resource_binding(&tx, "old", "unmapped-cid")
                    .await
                    .is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                migration::release_history(&tx, "old", "unmapped-cid")
                    .await
                    .is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                migration::admission_historical_keys(&tx, "old", &())
                    .await
                    .is_err(),
                "{endpoint}: {damage}"
            );
            tx.rollback().await.unwrap();
        }
    }
}

#[tokio::test]
async fn management_endpoint_keeps_r26_budget_limits_and_accepts_more_than_stage7_native_rows() {
    let db = native_catalog(management_schema::VERSION).await;
    let declaration = declaration(&db).await;
    db.execute_unprepared(
        "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<4200) INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) SELECT 'unrelated-'||n,0,0 FROM numbers",
    ).await.unwrap();
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    assert_eq!(migration::admission_schema(&db).await.unwrap(), plan.schema);
    assert!(
        plan.before
            .iter()
            .map(|table| table.rows.len())
            .sum::<usize>()
            > 4096
    );
    assert_eq!(migration::MAX_ROWS, 10_000);
    assert_eq!(migration::MAX_BYTES, 8 * 1024 * 1024);
}

#[tokio::test]
async fn registered_management_requires_exact_columns_keys_constraints_and_no_shadow() {
    for damage in [
        "DROP TABLE pin_management_retirements",
        "ALTER TABLE pin_management_records ADD COLUMN shadow TEXT",
        "ALTER TABLE pin_management_records DROP COLUMN native_before",
        "CREATE TEMP TABLE pin_management_records (shadow TEXT)",
        "CREATE TEMP VIEW pin_management_retirements AS SELECT 'shadow' AS allocation_domain",
        "CREATE TEMP TABLE seaql_migrations (version TEXT NOT NULL PRIMARY KEY)",
        "CREATE UNIQUE INDEX shadow_management_key ON pin_management_records(plan_id,item_id,kind)",
        "UPDATE seaql_migrations SET version='m20261027_000002_unknown' WHERE version='m20261027_000001_schema_baseline'",
        "DELETE FROM seaql_migrations WHERE version='m20261027_000001_schema_baseline'",
    ] {
        let db = native_catalog(management_schema::VERSION).await;
        let changed = db.execute_unprepared(damage).await.unwrap();
        if damage.starts_with("UPDATE seaql_migrations")
            || damage.starts_with("DELETE FROM seaql_migrations")
        {
            assert_eq!(changed.rows_affected(), 1, "{damage}");
        }
        assert!(super::super::schema(&db, false).await.is_err(), "{damage}");
    }
    for replacement in [
        management_schema::records_ddl(DatabaseBackend::Sqlite).replace(
            "PRIMARY KEY (database_scope,plan_id,item_id,kind)",
            "PRIMARY KEY (database_scope,plan_id,item_id)",
        ),
        management_schema::records_ddl(DatabaseBackend::Sqlite)
            .replace("binding TEXT NOT NULL", "binding TEXT"),
        management_schema::records_ddl(DatabaseBackend::Sqlite).replace(
            "kind IN ('database_scope','evidence_preparation','action')",
            "length(kind)>0",
        ),
    ] {
        let db = native_catalog(management_schema::VERSION).await;
        db.execute_unprepared("DROP TABLE pin_management_retirements")
            .await
            .unwrap();
        db.execute_unprepared("DROP TABLE pin_management_records")
            .await
            .unwrap();
        db.execute_unprepared(&replacement).await.unwrap();
        db.execute_unprepared(&management_schema::retirements_ddl(DatabaseBackend::Sqlite))
            .await
            .unwrap();
        assert!(super::super::schema(&db, false).await.is_err());
    }
}

#[test]
fn prefix_specific_fence_order_keeps_retirement_between_usage_and_routes_and_records_in_tail() {
    for endpoint in ENDPOINTS.into_iter().chain([management_schema::VERSION]) {
        let versions = vec![endpoint.into()];
        let order = super::super::super::fence::lock_order(&versions);
        let names = order
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        let mut first = vec![
            "buckets",
            "objects",
            "object_versions",
            "pin_leases",
            "pin_lease_targets",
            "remote_pins",
            "pin_provider_usage",
        ];
        if endpoint == management_schema::VERSION {
            first.push(management_schema::RETIREMENTS);
        }
        first.push("pin_provider_routes");
        assert_eq!(&names[..first.len()], first);
        assert_eq!(order[4].1, "created_at,id");
        assert_eq!(
            names.contains(&management_schema::RECORDS),
            endpoint == management_schema::VERSION
        );
        assert_eq!(
            names.contains(&management_schema::RETIREMENTS),
            endpoint == management_schema::VERSION
        );
        if endpoint == management_schema::VERSION {
            assert!(
                names
                    .iter()
                    .position(|name| *name == management_schema::RECORDS)
                    .unwrap()
                    > names.iter().position(|name| *name == "pin_jobs").unwrap()
            );
        }
    }
}

#[tokio::test]
async fn admission_schema_returns_exact_prefixes_and_does_not_cache_historical_absence() {
    for endpoint in ENDPOINTS.into_iter().chain([management_schema::VERSION]) {
        let db = native_catalog(endpoint).await;
        let versions = migration::admission_schema(&db).await.unwrap();
        assert_eq!(versions, super::super::schema(&db, false).await.unwrap());
        assert_eq!(versions.last().map(String::as_str), Some(endpoint));
        if endpoint != management_schema::VERSION {
            assert!(db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
                "SELECT 1 FROM main.sqlite_schema WHERE name IN ('pin_management_records','pin_management_retirements') LIMIT 1"))
                .await.unwrap().is_none());
        }
    }
    let (_directory, db) = file_catalog(ENDPOINTS[3]).await;
    let old = migration::admission_schema(&db).await.unwrap();
    super::upgrade(&db).await;
    let current = migration::admission_schema(&db).await.unwrap();
    assert!(
        old.iter()
            .map(String::as_str)
            .eq(crate::store::migrations::history::prefix(ENDPOINTS[3])
                .unwrap()
                .iter()
                .copied())
    );
    assert_eq!(current, [management_schema::VERSION]);
    assert_eq!(
        current.last().map(String::as_str),
        Some(management_schema::VERSION)
    );
    db.execute_unprepared("DROP TABLE pin_management_retirements")
        .await
        .unwrap();
    assert!(migration::admission_schema(&db).await.is_err());
}

#[tokio::test]
async fn admission_schema_rejects_migration_and_route_shadows_and_native_route_damage() {
    use crate::store::pinning::management::{catalog, native};
    let route = native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_provider_routes")
        .unwrap();
    for endpoint in [ENDPOINTS[0], management_schema::VERSION] {
        for damage in [
            "CREATE TEMP VIEW seaql_migrations AS SELECT * FROM main.seaql_migrations",
            "CREATE TEMP VIEW pin_provider_routes AS SELECT * FROM main.pin_provider_routes",
            "CREATE TEMP TABLE pin_provider_routes (shadow TEXT)",
            "ALTER TABLE pin_provider_routes ADD COLUMN unknown_authority TEXT",
            "DROP TABLE pin_provider_routes",
            "ATTACH DATABASE ':memory:' AS attached; CREATE TABLE attached.pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL); DROP TABLE main.pin_provider_routes",
            "DROP TABLE pin_provider_routes; CREATE VIEW pin_provider_routes AS SELECT '' AS provider,'' AS snapshot,'' AS display_name,0 AS retired",
            "DROP TABLE pin_provider_routes; CREATE TABLE Pin_Provider_Routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot BLOB NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired INTEGER NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL PRIMARY KEY,snapshot TEXT,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL)",
            "DROP TABLE pin_provider_routes; CREATE TABLE pin_provider_routes (provider TEXT NOT NULL,snapshot TEXT NOT NULL,display_name TEXT NOT NULL,retired BOOLEAN NOT NULL,PRIMARY KEY(provider,display_name))",
        ] {
            let db = native_catalog(endpoint).await;
            let original = migration::admission_schema(&db).await.unwrap();
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(damage).await.unwrap();
            assert!(
                migration::admission_schema(&tx).await.is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                catalog::validate_table(&tx, route).await.is_err(),
                "{endpoint}: {damage}"
            );
            assert!(
                catalog::validate(&tx, std::slice::from_ref(route))
                    .await
                    .is_err(),
                "{endpoint}: {damage}"
            );
            tx.rollback().await.unwrap();
            assert_eq!(migration::admission_schema(&db).await.unwrap(), original);
        }
    }
}

#[tokio::test]
async fn sqlite_route_type_checks_preserve_virtual_and_shadow_classification() {
    use crate::store::pinning::management::{catalog, native};
    let db = native_catalog(management_schema::VERSION).await;
    let fts5 = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT 1 FROM pragma_module_list WHERE name='fts5'",
        ))
        .await
        .unwrap()
        .is_some();
    if !fts5 {
        eprintln!(
            "installed SQLite has no FTS5 module; virtual/shadow classification not exercised"
        );
        return;
    }
    let route = native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_provider_routes")
        .unwrap();
    let original = migration::admission_schema(&db).await.unwrap();
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DROP TABLE pin_provider_routes; CREATE VIRTUAL TABLE pin_provider_routes USING fts5(provider,snapshot,display_name,retired)")
        .await.unwrap();
    let kind: String = tx
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT type FROM pragma_table_list($1) WHERE schema='main' AND name=$1",
            ["pin_provider_routes".into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "type")
        .unwrap();
    assert_eq!(kind, "virtual");
    assert!(migration::admission_schema(&tx).await.is_err());
    assert!(catalog::validate_table(&tx, route).await.is_err());
    assert!(
        catalog::validate(&tx, std::slice::from_ref(route))
            .await
            .is_err()
    );

    // A real module-owned shadow exercises the pragma argument's type boundary.
    // FTS5 cannot create a shadow whose suffix is exactly `routes`.
    let row = tx.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "SELECT name,type,type='table' AS ordinary FROM pragma_table_list($1) WHERE schema='main' AND name='pin_provider_routes_data'",
        ["PIN_PROVIDER_ROUTES_DATA".into()]))
        .await.unwrap().unwrap();
    assert_eq!(
        row.try_get::<String>("", "name").unwrap(),
        "pin_provider_routes_data"
    );
    assert_eq!(row.try_get::<String>("", "type").unwrap(), "shadow");
    assert_eq!(row.try_get::<i32>("", "ordinary").unwrap(), 0);
    tx.rollback().await.unwrap();
    assert_eq!(migration::admission_schema(&db).await.unwrap(), original);
    catalog::validate_table(&db, route).await.unwrap();
}

#[tokio::test]
async fn sqlite_classification_reuses_metadata_without_extra_receipt_queries() {
    use crate::store::pinning::management::{catalog, native};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    for endpoint in ENDPOINTS.into_iter().chain([management_schema::VERSION]) {
        let mut db = native_catalog(endpoint).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        db.set_metric_callback(move |_| {
            observed.fetch_add(1, Ordering::Relaxed);
        });
        let route = native::current_catalog()
            .iter()
            .find(|table| table.table == "pin_provider_routes")
            .unwrap();

        let versions = super::super::schema(&db, false).await.unwrap();
        catalog::validate_table(&db, route).await.unwrap();
        let standalone_queries = calls.swap(0, Ordering::Relaxed);

        assert_eq!(migration::admission_schema(&db).await.unwrap(), versions);
        let admission_queries = calls.swap(0, Ordering::Relaxed);
        assert!(
            admission_queries < standalone_queries,
            "{endpoint}: classification={admission_queries}, standalone={standalone_queries}"
        );
        assert_eq!(
            migration::admission_schema::receipt_schema(&db)
                .await
                .unwrap(),
            versions
        );
        assert_eq!(
            calls.swap(0, Ordering::Relaxed),
            admission_queries,
            "{endpoint}"
        );
    }
}
