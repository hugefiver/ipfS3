use ipfs_s3_gateway::store::{
    initialization::{self, SchemaState, UpgradeRequest},
    pinning::management::{native, schema},
};
use sea_orm::{ConnectionTrait, Database, Statement, TransactionTrait};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod action_fixture;

struct Stage7;
impl MigratorTrait for Stage7 {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        use ipfs_s3_gateway::store::migrations::*;
        vec![
            Box::new(m20250701_000001_init::Migration),
            Box::new(m20260707_000001_decompress_zip::Migration),
            Box::new(m20260720_000001_sse_c_key_fingerprint::Migration),
            Box::new(m20260721_000001_multi_provider_pinning::Migration),
            Box::new(m20260729_000001_ipfs3_import::Migration),
            Box::new(m20260729_000002_postgres_utc_timestamps::Migration),
            Box::new(m20260730_000001_standard_mutation_fence::Migration),
            Box::new(m20260813_000001_postgres_json_columns::Migration),
            Box::new(m20260825_000001_object_versioning::Migration),
            Box::new(m20260826_000001_lifecycle_expiration::Migration),
            Box::new(m20260831_000001_bucket_cors::Migration),
            Box::new(m20260901_000001_lifecycle_abort_multipart::Migration),
            Box::new(m20260912_000001_residency_references::Migration),
            Box::new(m20260912_000002_lifecycle_transition::Migration),
            Box::new(m20260919_000001_standard_mutation_lease::Migration),
            Box::new(m20260920_000001_pin_submit_history::Migration),
            Box::new(m20260920_000002_pin_identity_ledger::Migration),
            Box::new(m20260920_000003_pin_extension_decision::Migration),
            Box::new(m20260920_000004_multipart_pin_decision::Migration),
            Box::new(m20260920_000005_import_pin_decision::Migration),
            Box::new(m20260920_000006_pin_submit_correlation::Migration),
            Box::new(m20260927_000001_zip_batches::Migration),
            Box::new(m20260927_000002_import_zip_root_capture::Migration),
            Box::new(m20260927_000003_zip_mpu_replay::Migration),
            Box::new(m20260927_000004_zip_v2_execution::Migration),
            Box::new(m20260927_000005_zip_v2_import::Migration),
            Box::new(m20260927_000006_zip_v2_mpu_intake::Migration),
            Box::new(m20260927_000007_zip_v2_mpu_completion::Migration),
            Box::new(m20261004_000001_rpc_submission_ledger::Migration),
            Box::new(m20261004_000002_cluster_replica_contract::Migration),
            Box::new(m20261007_000001_legacy_pinning_migration::Migration),
            Box::new(m20261008_000001_zip_intent_capture::Migration),
            Box::new(m20261012_000001_pinning_management::Migration),
            Box::new(m20261013_000001_zip_root_logical_release::Migration),
        ]
    }
}

async fn old_database() -> (tempfile::TempDir, sea_orm::DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("history.sqlite");
    let db = Database::connect(format!(
        "sqlite://{}?mode=rwc",
        path.to_string_lossy().replace('\\', "/")
    ))
    .await
    .unwrap();
    Stage7::up(&db, None).await.unwrap();
    (directory, db)
}

fn history_budget() -> native::meter::Budget {
    native::meter::Budget::new(native::meter::Limits {
        rows: 65536,
        bytes: 128 * 1024 * 1024,
        sql: 65536,
        resident: 32 * 1024 * 1024,
        ..Default::default()
    })
}

#[tokio::test]
async fn review_regression_opaque_foreign_original_is_not_local_authority() {
    use ipfs_s3_gateway::store::initialization::history;
    let (_directory, db) = old_database().await;
    db.execute_unprepared("INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES('foreign-scope','foreign-plan','foreign-item','action','foreign-db','foreign-principal','foreign-binding','foreign-私密-before','foreign-result','[]','[]','applied',CURRENT_TIMESTAMP,datetime(CURRENT_TIMESTAMP,'+1 hour'),CURRENT_TIMESTAMP)").await.unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    initialization::upgrade_existing(
        &db,
        UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    let key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            "foreign-scope".into(),
            "foreign-plan".into(),
            "foreign-item".into(),
            "action".into(),
        ],
    };
    let mut budget = history_budget();
    let index = history::recover_source(&db, &key, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert!(index.review.is_none() && index.committed.is_none() && index.recorded.is_none());
    let original: native::NativeRow = history::read_part(&db, &index.original, &mut budget)
        .await
        .unwrap();
    assert_eq!(original["binding"].as_deref(), Some("foreign-binding"));
    assert_eq!(
        original["native_before"].as_deref(),
        Some("foreign-私密-before")
    );
    assert_eq!(original["result"].as_deref(), Some("foreign-result"));
    assert!(
        history::recover_committed(&db, &key, "foreign-binding", &mut budget)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn review_regression_frozen_foreign_text_survives_current_row_change_or_delete() {
    use ipfs_s3_gateway::store::initialization::history;
    for delete in [false, true] {
        let fixture = action_fixture::Fixture::new().await;
        remove_fixture_storage(&fixture.db).await;
        let foreign =
            write_old_provider_history(&fixture, "old-provider-confirmation", false, true, false)
                .await;
        let local =
            write_old_provider_history(&fixture, "old-provider-confirmation", false, false, false)
                .await;
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "DELETE FROM pin_management_records WHERE database_scope=$1 AND kind='action'",
                [foreign.scope.clone().into()],
            ))
            .await
            .unwrap();
        if !delete {
            let replacement = write_old_provider_history(
                &fixture,
                "old-provider-confirmation",
                false,
                true,
                false,
            )
            .await;
            assert_ne!(replacement.before, foreign.before);
        }
        initialization::upgrade_existing(
            &fixture.db,
            UpgradeRequest {
                database_scope: fixture.auth.database_scope(),
                native_identity: fixture.auth.database_identity(),
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await
        .unwrap();
        let key = native::NativeKey {
            table: local.owner.0,
            values: local.owner.1,
        };
        let foreign_key = native::NativeKey {
            table: foreign.owner.0,
            values: foreign.owner.1,
        };
        let mut budget = history_budget();
        let index = history::recover_source(&fixture.db, &key, &mut budget)
            .await
            .unwrap()
            .unwrap();
        let row = index
            .review
            .as_ref()
            .unwrap()
            .rows
            .iter()
            .find(|row| row.key == foreign_key)
            .unwrap();
        let original: native::NativeRow =
            history::read_part(&fixture.db, &row.original, &mut budget)
                .await
                .unwrap();
        assert_eq!(
            original["native_before"].as_deref(),
            Some(foreign.before.as_str())
        );
        let review = history::recover_frozen_review(&fixture.db, row, &mut budget)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(review.database_scope, foreign.scope);
        assert_eq!(review.database_identity, foreign.identity);
        let request: ipfs_s3_gateway::pinning::management::PlanRequest =
            history::read_part(&fixture.db, &review.request, &mut budget)
                .await
                .unwrap();
        assert!(request.items[0] == foreign.entry.request);
        assert_eq!(review.confirmed_at, Some(foreign.now));
    }
}

#[tokio::test]
async fn frozen_foreign_versions_keep_distinct_identity_and_principal_with_same_core() {
    use ipfs_s3_gateway::store::initialization::history;

    fn rebind_head(raw: &str, identity: &str, principal: &str) -> String {
        assert!(raw.starts_with("S7F1\n"));
        let head_len = usize::from_str_radix(&raw[5..13], 16).unwrap();
        let count = usize::from_str_radix(&raw[29..37], 16).unwrap();
        let start = 39 + count * 8;
        let mut head: serde_json::Value =
            serde_json::from_str(&raw[start..start + head_len]).unwrap();
        head[1] = identity.into();
        head[2] = principal.into();
        let head = serde_json::to_string(&head).unwrap();
        format!(
            "S7F1\n{:08x}{}{head}{}",
            head.len(),
            &raw[13..start],
            &raw[start + head_len..]
        )
    }

    // Each part of the foreign subject binding must independently separate
    // frozen versions, even when the anchor and immutable core are identical.
    for (identity2, principal2) in [
        ("foreign-history-database", "foreign-principal-2"),
        ("foreign-history-database-2", "foreign-principal-1"),
    ] {
        let fixture = action_fixture::Fixture::new().await;
        remove_fixture_storage(&fixture.db).await;
        let foreign = write_old_provider_history(&fixture, "foreign-f", false, true, false).await;
        let principal1 = "foreign-principal-1";
        let before1 = rebind_head(&foreign.before, &foreign.identity, principal1);
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET native_before=$1,principal=$2 WHERE database_scope=$3 AND plan_id=$4 AND kind='action'",
            [before1.clone().into(), principal1.into(), foreign.scope.clone().into(), "foreign-f".into()])).await.unwrap();
        let a = write_old_provider_history(&fixture, "local-review-a", false, false, false).await;

        let before2 = rebind_head(&before1, identity2, principal2);
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET native_before=$1,native_identity=$2,principal=$3 WHERE database_scope=$4 AND plan_id=$5 AND kind='action'",
            [before2.clone().into(), identity2.into(), principal2.into(), foreign.scope.clone().into(), "foreign-f".into()])).await.unwrap();
        let b = write_old_provider_history(&fixture, "local-review-b", false, false, false).await;
        initialization::upgrade_existing(
            &fixture.db,
            UpgradeRequest {
                database_scope: fixture.auth.database_scope(),
                native_identity: fixture.auth.database_identity(),
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await
        .unwrap();

        let b_key = native::NativeKey {
            table: b.owner.0,
            values: b.owner.1,
        };
        let a_key = native::NativeKey {
            table: a.owner.0,
            values: a.owner.1,
        };
        let f_key = native::NativeKey {
            table: foreign.owner.0,
            values: foreign.owner.1,
        };
        let mut budget = history_budget();
        let b_source = history::recover_source(&fixture.db, &b_key, &mut budget)
            .await
            .unwrap()
            .unwrap();
        assert!(b_source.committed.is_none() && b_source.recorded.is_none());
        let b_review = b_source.review.unwrap();
        let a_row = b_review.rows.iter().find(|row| row.key == a_key).unwrap();
        let a_review = history::recover_frozen_review(&fixture.db, a_row, &mut budget)
            .await
            .unwrap()
            .unwrap();
        let mut versions = Vec::new();
        for (review, before, identity, principal) in [
            (&b_review, &before2, identity2, principal2),
            (&a_review, &before1, foreign.identity.as_str(), principal1),
        ] {
            let row = review.rows.iter().find(|row| row.key == f_key).unwrap();
            assert!(row.recorded.is_none());
            let original: native::NativeRow =
                history::read_part(&fixture.db, &row.original, &mut budget)
                    .await
                    .unwrap();
            assert_eq!(original["native_before"].as_deref(), Some(before.as_str()));
            assert_eq!(original["native_identity"].as_deref(), Some(identity));
            assert_eq!(original["principal"].as_deref(), Some(principal));
            let frozen = history::recover_frozen_review(&fixture.db, row, &mut budget)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frozen.database_scope, foreign.scope);
            assert_eq!(frozen.database_identity, identity);
            assert_eq!(frozen.principal, principal);
            assert_eq!(frozen.anchor.as_ref(), Some(&f_key));
            versions.push(frozen);
        }
        assert_eq!(versions[0].core_identity, versions[1].core_identity);
    }
}

#[tokio::test]
async fn explicit_upgrade_preserves_identity_and_is_repeatable() {
    let (_directory, db) = old_database().await;
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    for _ in 0..2 {
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: &identity,
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            initialization::classify(&db).await.unwrap(),
            SchemaState::Current
        );
        assert_eq!(
            native::scope::binding(&db).await.unwrap(),
            (scope.clone(), identity.clone())
        );
    }
}

#[tokio::test]
async fn wrong_identity_does_not_prepare_schema() {
    let (_directory, db) = old_database().await;
    let (scope, _) = native::scope::binding(&db).await.unwrap();
    assert!(
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: "wrong-database",
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            }
        )
        .await
        .is_err()
    );
    assert!(
        db.query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT 1 FROM sqlite_schema WHERE name='pin_management_readiness'"
        ))
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn nonempty_upgrade_converts_original_and_replays_without_rewriting_it() {
    let (_directory, db) = old_database().await;
    nonempty_upgrade(db, None).await;
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive PostgreSQL schema"]
async fn postgres_nonempty_upgrade_preserves_originals_and_nulls() {
    let url = std::env::var("IPFS_S3_STAGE75_HISTORY_DATABASE_URL").unwrap();
    let mut config: ipfs_s3_gateway::config::Config = toml::from_str("").unwrap();
    config.storage.database_url = url.clone();
    config.crypto.master_key = "7".repeat(64);
    config.kubo.rpc_url = "http://127.0.0.1:1".into();
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .min_connections(1)
        .max_connections(1)
        .sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    let row = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS empty, pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(),0)) AS claimed"))
        .await.unwrap().unwrap();
    assert!(
        row.try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(row.try_get::<bool>("", "empty").unwrap());
    assert!(row.try_get::<bool>("", "claimed").unwrap());
    Stage7::up(&db, None).await.unwrap();
    assert!(initialization::open(&db).await.is_err());
    let absent = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT to_regclass(current_schema() || '.pin_management_readiness') IS NULL AS absent",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<bool>("", "absent")
        .unwrap();
    assert!(absent, "ordinary opening must not prepare an upgrade");
    nonempty_upgrade(db, Some(&config)).await;
}

async fn nonempty_upgrade(
    db: sea_orm::DatabaseConnection,
    gateway: Option<&ipfs_s3_gateway::config::Config>,
) {
    use ipfs_s3_gateway::{
        pinning::identity::{CleanupMode, ProviderRouteSnapshot},
        store::initialization::history,
    };
    let route = ProviderRouteSnapshot {
        provider_id: "route".into(),
        backend: "noop".into(),
        scope: "isolated".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: Some("env:PRIVATE_HISTORY_SECRET".into()),
        api_profile: "noop".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    };
    let raw = format!("\n{}  ", serde_json::to_string_pretty(&route).unwrap());
    let display = "private display ".repeat(350_000);
    assert!(display.len() >= 5 * 1024 * 1024);
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_provider_routes VALUES ('route',$1,$2,FALSE)",
        [raw.clone().into(), display.clone().into()],
    ))
    .await
    .unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    if let Some(config) = gateway {
        let tables = Statement::from_string(
            db.get_database_backend(),
            "SELECT string_agg(table_name, ',' ORDER BY table_name) AS names FROM information_schema.tables WHERE table_schema=current_schema()",
        );
        let before: String = db
            .query_one(tables.clone())
            .await
            .unwrap()
            .unwrap()
            .try_get("", "names")
            .unwrap();
        let error = ipfs_s3_gateway::state::AppState::new(config)
            .await
            .err()
            .expect("ordinary gateway startup must reject the old nonempty schema");
        assert!(matches!(
            error.downcast_ref::<sea_orm::DbErr>(),
            Some(sea_orm::DbErr::Migration(_))
        ));
        let after: String = db
            .query_one(tables)
            .await
            .unwrap()
            .unwrap()
            .try_get("", "names")
            .unwrap();
        assert_eq!(after, before);
        assert_eq!(
            native::scope::binding(&db).await.unwrap(),
            (scope.clone(), identity.clone())
        );
        let original = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT snapshot,display_name FROM pin_provider_routes WHERE provider='route'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.try_get::<String>("", "snapshot").unwrap(), raw);
        assert_eq!(
            original.try_get::<String>("", "display_name").unwrap(),
            display
        );
    }
    let key = native::NativeKey {
        table: "pin_provider_routes".into(),
        values: vec!["route".into()],
    };
    let mut first_digest = None;
    for _ in 0..2 {
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: &identity,
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await
        .unwrap();
        let mut budget = history_budget();
        let converted = history::recover_source(&db, &key, &mut budget)
            .await
            .unwrap()
            .unwrap();
        assert!(converted.committed.is_none());
        let original: native::NativeRow = history::read_part(&db, &converted.original, &mut budget)
            .await
            .unwrap();
        assert_eq!(original["snapshot"].as_deref(), Some(raw.as_str()));
        assert_eq!(original["display_name"].as_deref(), Some(display.as_str()));
        let identity_key = native::NativeKey {
            table: schema::RECORDS.into(),
            values: vec![
                scope.clone(),
                "schema".into(),
                "identity".into(),
                "database_scope".into(),
            ],
        };
        let identity_index = history::recover_source(&db, &identity_key, &mut budget)
            .await
            .unwrap()
            .unwrap();
        let identity_original: native::NativeRow =
            history::read_part(&db, &identity_index.original, &mut budget)
                .await
                .unwrap();
        assert_eq!(identity_original.get("result"), Some(&None));
        assert_eq!(identity_original.get("expires_at"), Some(&None));
        let current = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT snapshot FROM pin_provider_routes WHERE provider='route'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "snapshot")
            .unwrap();
        assert_eq!(current, raw);
        if let Some(expected) = &first_digest {
            assert_eq!(&converted.original.digest, expected);
        }
        first_digest = Some(converted.original.digest);
        assert_eq!(
            initialization::classify(&db).await.unwrap(),
            SchemaState::Current
        );
    }
    if let Some(config) = gateway {
        postgres_upgrade_obeys_archive_write_fences(
            &db,
            &config.storage.database_url,
            &scope,
            &identity,
        )
        .await;
        let mut budget = history_budget();
        let converted = history::recover_source(&db, &key, &mut budget)
            .await
            .unwrap()
            .unwrap();
        let original: native::NativeRow = history::read_part(&db, &converted.original, &mut budget)
            .await
            .unwrap();
        assert_eq!(original["snapshot"].as_deref(), Some(raw.as_str()));
        assert_eq!(original["display_name"].as_deref(), Some(display.as_str()));
        assert_eq!(
            native::scope::binding(&db).await.unwrap(),
            (scope, identity)
        );
    }
}

async fn postgres_upgrade_obeys_archive_write_fences(
    db: &sea_orm::DatabaseConnection,
    url: &str,
    scope: &str,
    identity: &str,
) {
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .min_connections(1)
        .max_connections(1)
        .sqlx_logging(false);
    let blocker = Database::connect(options).await.unwrap();
    let request = || UpgradeRequest {
        database_scope: scope,
        native_identity: identity,
        source_version: "m20261013_000001_zip_root_logical_release",
        target_version: schema::VERSION,
    };
    for table in schema::storage::EXPORT_TABLES {
        db.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
            .await
            .unwrap();
        let writer = blocker.begin().await.unwrap();
        writer
            .execute_unprepared(&format!(
                "LOCK TABLE \"{table}\" IN ROW EXCLUSIVE MODE NOWAIT"
            ))
            .await
            .unwrap();
        let blocked = initialization::upgrade_existing(db, request()).await;
        writer.rollback().await.unwrap();
        match blocked {
            Err(sea_orm::DbErr::Exec(sea_orm::RuntimeErr::SqlxError(sqlx::Error::Database(
                error,
            )))) => assert_eq!(error.code().as_deref(), Some("55P03")),
            other => {
                panic!("archive writer must block readiness publication for {table}: {other:?}")
            }
        }
        let state: String = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT state FROM pin_management_readiness WHERE id=1",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "state")
            .unwrap();
        assert_eq!(state, "backfill");
        initialization::upgrade_existing(db, request())
            .await
            .unwrap();
        assert_eq!(
            initialization::classify(db).await.unwrap(),
            SchemaState::Current
        );
    }
    blocker.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_original_remains_preserved_and_unavailable_after_failed_backfill() {
    let (_directory, db) = old_database().await;
    db.execute_unprepared(
        "INSERT INTO pin_provider_routes VALUES ('broken','{not valid json','private',0)",
    )
    .await
    .unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    assert!(
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: &identity,
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            }
        )
        .await
        .is_err()
    );
    let original = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT snapshot FROM pin_provider_routes WHERE provider='broken'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "snapshot")
        .unwrap();
    assert_eq!(original, "{not valid json");
    assert_eq!(
        initialization::classify(&db).await.unwrap(),
        SchemaState::UpgradeRequired
    );
    assert!(initialization::open(&db).await.is_err());
}

// The fixture's real confirmation/execution writers produce the retained S7
// originals. Only the disposable database's S75 storage is removed to exercise
// explicit preparation/backfill; business rows and their bytes are untouched.
async fn remove_fixture_storage(db: &sea_orm::DatabaseConnection) {
    let backend = db.get_database_backend();
    for (name, _, _, _) in schema::storage::triggers(backend) {
        db.execute_unprepared(&format!("DROP TRIGGER {name}"))
            .await
            .unwrap();
    }
    for name in ["s75_applied_no_delete", "s75_applied_no_update"] {
        db.execute_unprepared(&format!("DROP TRIGGER {name}"))
            .await
            .unwrap();
    }
    for (name, _) in schema::storage::indexes() {
        db.execute_unprepared(&format!("DROP INDEX {name}"))
            .await
            .unwrap();
    }
    for (name, _) in schema::storage::tables(backend).into_iter().rev() {
        db.execute_unprepared(&format!("DROP TABLE {name}"))
            .await
            .unwrap();
    }
    db.execute(Statement::from_sql_and_values(
        backend,
        "DELETE FROM seaql_migrations WHERE version=$1",
        [schema::VERSION.into()],
    ))
    .await
    .unwrap();
}

// Preserve the real writer's complete semantic review and add legal JSON
// whitespace to its private core. All affected protocol identities are rebound;
// the resulting >1 MiB original is still a confirmed, unexecuted S7 plan.
async fn enlarge_confirmed_original(db: &sea_orm::DatabaseConnection, plan: &str) -> String {
    use sha2::{Digest, Sha256};
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),"SELECT native_before,binding FROM pin_management_records WHERE plan_id=$1 AND item_id='a' AND state='prepared'",[plan.into()])).await.unwrap().unwrap();
    let raw: String = row.try_get("", "native_before").unwrap();
    assert!(raw.starts_with("S7F1\n"));
    let size = |offset| usize::from_str_radix(&raw[offset..offset + 8], 16).unwrap();
    let (head_len, core_len, manifest_len, count) = (size(5), size(13), size(21), size(29));
    let start = 39 + count * 8;
    let mut head: serde_json::Value = serde_json::from_str(&raw[start..start + head_len]).unwrap();
    let mut core = raw[start + head_len..start + head_len + core_len].to_owned();
    core.push_str(&" ".repeat(1024 * 1024));
    let mut digest = Sha256::new();
    digest.update(b"stage7/core/1\0");
    digest.update(core.as_bytes());
    let hash = hex::encode(digest.finalize());
    head[5] = hash.clone().into();
    let head = serde_json::to_string(&head).unwrap();
    let directory = &raw[38..38 + count * 8];
    let tail = &raw[start + head_len + core_len..];
    let enlarged = format!(
        "S7F1\n{:08x}{:08x}{:08x}{:08x}\n{directory}\n{head}{core}{tail}",
        head.len(),
        core.len(),
        manifest_len,
        count
    );
    let binding: String = row.try_get("", "binding").unwrap();
    let mut binding: serde_json::Value =
        serde_json::from_str(binding.strip_prefix("S7B1\n").unwrap()).unwrap();
    binding[2] = hash.clone().into();
    let binding = format!("S7B1\n{}", serde_json::to_string(&binding).unwrap());
    let locator: String = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT native_before FROM pin_management_records WHERE plan_id=$1 AND item_id='b'",
            [plan.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "native_before")
        .unwrap();
    let mut locator: serde_json::Value =
        serde_json::from_str(locator.strip_prefix("S7L1\n").unwrap()).unwrap();
    locator[1] = hash.into();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),"UPDATE pin_management_records SET binding=$1,native_before=CASE WHEN item_id='a' THEN $2 ELSE $3 END WHERE plan_id=$4 AND state='prepared'",[binding.into(),enlarged.clone().into(),format!("S7L1\n{}",serde_json::to_string(&locator).unwrap()).into(),plan.into()])).await.unwrap();
    enlarged
}

async fn confirm_history(
    fixture: &action_fixture::Fixture,
    request: &ipfs_s3_gateway::pinning::management::PlanRequest,
) -> ipfs_s3_gateway::store::pinning::management::records::ConfirmedBinding {
    use ipfs_s3_gateway::{
        pinning::management::{self as m, PlanStatus},
        store::pinning::management::records,
    };
    let snapshot = m::db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = m::dry_run(request, &snapshot).unwrap();
    let statuses = plan
        .items()
        .iter()
        .map(|entry| &entry.status)
        .collect::<Vec<_>>();
    assert!(
        statuses
            .iter()
            .all(|status| !matches!(status, PlanStatus::Blocked(_))),
        "{}: {statuses:?}, routes={}",
        request.plan_id,
        serde_json::to_string(&snapshot.routes).unwrap()
    );
    let acks = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (request, &snapshot, &plan),
        acks,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap()
}

fn history_request(
    fixture: &action_fixture::Fixture,
    plan: &str,
    object: ipfs_s3_gateway::pinning::management::ExactObject,
) -> ipfs_s3_gateway::pinning::management::PlanRequest {
    use ipfs_s3_gateway::pinning::management::{RouteIdentity, Subject, TargetSelection};
    let mut request = fixture.create_request(plan, object, false);
    if let Subject::Object {
        target: TargetSelection::New(route),
        ..
    } = &mut request.items[0].subject
    {
        *route = RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot());
    }
    request
}

#[tokio::test]
async fn real_confirmed_and_committed_history_recovers_exact_results_and_original_review() {
    verify_real_history_conversion(false).await;
}

#[tokio::test]
async fn same_plan_committed_outputs_remain_readable_after_conversion() {
    verify_real_history_conversion(true).await;
}

async fn verify_real_history_conversion(shared_dependency: bool) {
    use ipfs_s3_gateway::{
        pinning::management::PlanRequest,
        store::{
            initialization::history,
            pinning::management::{actions, records},
        },
    };
    let fixture = action_fixture::Fixture::new().await;
    let object =
        action_fixture::base::publish(&fixture.db, &fixture.config, "original", false).await;
    let private = "{  \"private\":\"原文 PRIVATE\", \"nullable\":null }";
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE objects SET metadata=$1 WHERE id=$2",
            [private.into(), object.object_id.clone().into()],
        ))
        .await
        .unwrap();
    ipfs_s3_gateway::pinning::management::evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri()),
        &ipfs_s3_gateway::pinning::management::evidence::PrepareRequest {
            record_id: "original-proof".into(),
            identity: object.clone(),
            expected_node: action_fixture::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut request = history_request(&fixture, "committed-history", object.clone());
    if shared_dependency {
        let sharing_object = fixture.publish("sharing-history-resource", false).await;
        let mut sharing_remote = history_request(&fixture, "committed-history", sharing_object)
            .items
            .remove(0);
        sharing_remote.item_id = "second".into();
        request.items.push(sharing_remote);
    }
    let dependent_index = usize::from(shared_dependency);
    let binding = confirm_history(&fixture, &request).await;
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert_eq!(result.items.len(), request.items.len());
    let third = fixture.publish("pending-third", false).await;
    let mut a = fixture
        .existing_request(
            "pending-history",
            object.clone(),
            ipfs_s3_gateway::pinning::management::Action::Renew {
                expires_at: chrono::Utc::now().timestamp() + 86400,
            },
        )
        .await
        .items
        .remove(0);
    let mut b = history_request(&fixture, "pending-history", third)
        .items
        .remove(0);
    a.item_id = "a".into();
    b.item_id = "b".into();
    let pending_request = PlanRequest {
        plan_id: "pending-history".into(),
        items: vec![a, b],
    };
    let pending = confirm_history(&fixture, &pending_request).await;
    let large_original = enlarge_confirmed_original(&fixture.db, &pending_request.plan_id).await;
    let originals = fixture.db.query_all(Statement::from_string(fixture.db.get_database_backend(),"SELECT plan_id,item_id,binding,native_before,result,created_at,expires_at FROM pin_management_records WHERE kind='action' ORDER BY plan_id,item_id")).await.unwrap();
    assert!(originals.iter().any(|row| {
        row.try_get::<String>("", "native_before")
            .unwrap()
            .starts_with("S7F1\n")
    }));
    assert!(originals.iter().any(|row| {
        row.try_get::<String>("", "native_before")
            .unwrap()
            .starts_with("S7L1\n")
    }));
    assert!(originals.iter().all(|row| {
        row.try_get::<String>("", "binding")
            .unwrap()
            .starts_with("S7B1\n")
    }));
    assert!(originals.iter().any(|row| {
        row.try_get::<Option<String>>("", "result")
            .unwrap()
            .is_some_and(|result| result.starts_with("S7R1\n"))
    }));
    let second_result = originals
        .iter()
        .find(|row| {
            row.try_get::<String>("", "plan_id").unwrap() == request.plan_id
                && row.try_get::<String>("", "item_id").unwrap()
                    == request.items[dependent_index].item_id
        })
        .unwrap()
        .try_get::<String>("", "result")
        .unwrap();
    let (_, facts, _): (history::legacy::RecordedAction, serde_json::Value, String) =
        serde_json::from_str(second_result.strip_prefix("S7R1\n").unwrap()).unwrap();
    if shared_dependency {
        assert!(
            facts[1]
                .as_array()
                .unwrap()
                .iter()
                .any(|read| read[1][0] == 2)
        );
    }
    let key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            fixture.auth.database_scope().into(),
            request.plan_id.clone(),
            "object".into(),
            "action".into(),
        ],
    };
    let original_binding = originals
        .iter()
        .find(|row| row.try_get::<String>("", "plan_id").unwrap() == request.plan_id)
        .unwrap()
        .try_get::<String>("", "binding")
        .unwrap();
    let jobs_before: i64 = fixture
        .db
        .query_one(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_jobs",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    remove_fixture_storage(&fixture.db).await;
    initialization::upgrade_existing(
        &fixture.db,
        UpgradeRequest {
            database_scope: fixture.auth.database_scope(),
            native_identity: fixture.auth.database_identity(),
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    let recovery_limits = native::meter::Limits {
        rows: 65536,
        bytes: 128 * 1024 * 1024,
        sql: 65536,
        resident: 32 * 1024 * 1024,
        ..Default::default()
    };
    let mut budget = native::meter::Budget::new(recovery_limits);
    let recovered = history::recover_committed(&fixture.db, &key, &original_binding, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert!(recovered == result.items[0]);
    let mut second_key = key.clone();
    second_key.values[2] = request.items[dependent_index].item_id.clone();
    if shared_dependency {
        assert!(
            history::recover_committed(&fixture.db, &second_key, &original_binding, &mut budget)
                .await
                .unwrap()
                .unwrap()
                == result.items[dependent_index]
        );
    }
    for row in &originals {
        let key = native::NativeKey {
            table: schema::RECORDS.into(),
            values: vec![
                fixture.auth.database_scope().into(),
                row.try_get("", "plan_id").unwrap(),
                row.try_get("", "item_id").unwrap(),
                "action".into(),
            ],
        };
        let mut work = native::meter::Budget::default();
        let index = history::recover_source(&fixture.db, &key, &mut work)
            .await
            .unwrap()
            .unwrap();
        let restored: native::NativeRow =
            history::read_part(&fixture.db, &index.original, &mut work)
                .await
                .unwrap();
        for column in [
            "binding",
            "native_before",
            "result",
            "created_at",
            "expires_at",
        ] {
            assert_eq!(
                restored[column],
                row.try_get::<Option<String>>("", column).unwrap()
            );
        }
        let review = index.review.unwrap();
        let binding: serde_json::Value = serde_json::from_str(
            restored["binding"]
                .as_deref()
                .unwrap()
                .strip_prefix("S7B1\n")
                .unwrap(),
        )
        .unwrap();
        let acknowledgements: Vec<String> = serde_json::from_value(binding[4].clone()).unwrap();
        assert_eq!(review.acknowledgements, Some(acknowledgements));
        assert_eq!(
            review.expires_at,
            row.try_get::<chrono::DateTime<chrono::Utc>>("", "expires_at")
                .unwrap()
        );
    }
    let mut budget = native::meter::Budget::new(recovery_limits);
    let index = history::recover_source(&fixture.db, &key, &mut budget)
        .await
        .unwrap()
        .unwrap();
    let review = index.review.unwrap();
    let original_object = native::NativeKey {
        table: "objects".into(),
        values: vec![object.object_id],
    };
    assert_eq!(
        history::original_cell(
            &fixture.db,
            &review,
            &original_object,
            "metadata",
            &mut budget
        )
        .await
        .unwrap(),
        Some(Some(private.into()))
    );
    let pending_key = native::NativeKey {
        table: schema::RECORDS.into(),
        values: vec![
            fixture.auth.database_scope().into(),
            pending.request.plan_id.clone(),
            "a".into(),
            "action".into(),
        ],
    };
    assert!(
        history::requires_new_confirmation(
            &fixture.db,
            &pending_key,
            &mut native::meter::Budget::new(recovery_limits)
        )
        .await
        .unwrap()
    );
    let mut budget = native::meter::Budget::new(recovery_limits);
    let pending_index = history::recover_source(&fixture.db, &pending_key, &mut budget)
        .await
        .unwrap()
        .unwrap();
    let frozen_record = pending_index
        .review
        .as_ref()
        .unwrap()
        .rows
        .iter()
        .find(|row| row.key == key)
        .unwrap();
    let frozen_review = history::recover_frozen_review(&fixture.db, frozen_record, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frozen_review.acknowledgements, review.acknowledgements);
    let frozen_result: history::legacy::RecordedAction = history::read_part(
        &fixture.db,
        frozen_record.recorded.as_ref().unwrap(),
        &mut budget,
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(&frozen_result).unwrap(),
        serde_json::to_value(&result.items[0]).unwrap()
    );
    let pending_original: native::NativeRow =
        history::read_part(&fixture.db, &pending_index.original, &mut budget)
            .await
            .unwrap();
    assert_eq!(
        pending_original["native_before"].as_deref(),
        Some(large_original.as_str())
    );
    assert!(
        history::recover_committed(&fixture.db, &pending_key, &original_binding, &mut budget)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        fixture
            .db
            .query_one(Statement::from_string(
                fixture.db.get_database_backend(),
                "SELECT COUNT(*) AS n FROM pin_jobs"
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap(),
        jobs_before
    );
    let original: native::NativeRow = history::read_part(&fixture.db, &index.original, &mut budget)
        .await
        .unwrap();
    assert_eq!(
        original["binding"].as_deref(),
        Some(original_binding.as_str())
    );
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .is_some()
    );
}

#[test]
fn historical_retirement_inventory_is_data_not_sealed_authority() {
    use ipfs_s3_gateway::{pinning::management as model, store::initialization::history::legacy};
    let capture = legacy::RetirementCapture {
        inventory: model::ProviderInventory {
            provider_id: "historical-provider".into(),
            frontier: vec![],
            complete: true,
            attribution_complete: false,
            allocation_stopped: false,
            leases: vec![],
            root_reference_ids: vec![],
        },
        routes: vec![],
        targets: vec![],
        remotes: vec![],
        historical_remotes: vec![],
        root_references: vec![],
    };
    let original = serde_json::to_vec(&capture).unwrap();
    let recovered: legacy::RetirementCapture = serde_json::from_slice(&original).unwrap();
    assert!(recovered == capture);
    assert!(serde_json::from_slice::<model::RetirementCapture>(&original).is_err());
}

#[tokio::test]
async fn old_provider_confirmation_converts_without_sealing_new_execution_authority() {
    verify_old_provider_sql_history(false, false, false).await;
}

#[tokio::test]
async fn old_provider_committed_sql_history_restores_receipt_and_inventory() {
    verify_old_provider_sql_history(true, false, false).await;
}

async fn history_business_rows(
    db: &sea_orm::DatabaseConnection,
) -> std::collections::BTreeMap<native::NativeKey, native::NativeRow> {
    let mut originals = std::collections::BTreeMap::new();
    for table in native::registry() {
        let columns = table
            .columns
            .iter()
            .filter(|column| {
                table.table != "zip_root_references" || *column != "logical_released_at"
            })
            .collect::<Vec<_>>();
        let projection = columns
            .iter()
            .map(|column| format!("CAST(\"{column}\" AS TEXT) AS \"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        for row in db
            .query_all(Statement::from_string(
                db.get_database_backend(),
                format!("SELECT {projection} FROM \"{}\"", table.table),
            ))
            .await
            .unwrap()
        {
            let row: native::NativeRow = columns
                .iter()
                .map(|column| {
                    (
                        (*column).clone(),
                        row.try_get::<Option<String>>("", column).unwrap(),
                    )
                })
                .collect();
            let key = native::NativeKey {
                table: table.table.clone(),
                values: table
                    .primary_key
                    .iter()
                    .map(|column| row[column].clone().unwrap())
                    .collect(),
            };
            assert!(originals.insert(key, row).is_none());
        }
    }
    originals
}

#[tokio::test]
async fn committed_root_history_recovers_after_explicit_upgrade_without_new_business() {
    use ipfs_s3_gateway::store::initialization::history;
    let (_directory, db) = old_database().await;
    // Stage7 ends exactly at the root marker migration, not S75 storage.
    Stage7::down(&db, Some(1)).await.unwrap();
    let raw = action_fixture::base::config("http://127.0.0.1:5001");
    let config = action_fixture::base::management_config(&raw);
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_provider_routes VALUES($1,$2,'Original route',FALSE)",
        [
            config.pinning.providers[0].identity.allocation_key().into(),
            serde_json::to_string(&config.pinning.providers[0].route_snapshot())
                .unwrap()
                .into(),
        ],
    ))
    .await
    .unwrap();
    db.execute_unprepared("INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,created_at,updated_at) VALUES('old-root','原owner','direct','token','fingerprint','bucket','root.zip','input','{}','2026-10-10T01:02:03.456789Z','2026-10-10T01:02:04.456789Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO zip_root_builds(batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) VALUES('old-root',1,1,'old-worker','2026-10-10T02:02:03.456789Z','verified','2026-10-10T01:02:03.456789Z','2026-10-10T01:02:04.456789Z')").await.unwrap();
    db.execute_unprepared("INSERT INTO zip_root_references(batch_id,revision,epoch,node_identity,tier,cid,state,verification_receipt,created_at,updated_at) VALUES('old-root',1,1,'old-node','hot','verified-root','adopted','原private-receipt','2026-10-10T01:02:03.456789Z','2026-10-10T01:02:04.456789Z'),('old-root',1,1,'old-node','cold','uncertain-root','retained',NULL,'2026-10-10T01:02:03.456789Z','2026-10-10T01:02:04.456789Z')").await.unwrap();
    let (scope, identity) = native::scope::binding(&db).await.unwrap();
    let source = write_old_provider_original(
        (&db, &config, &scope, &identity),
        "old-root-retirement",
        true,
        false,
        false,
    )
    .await;
    let originals = history_business_rows(&db).await;
    Stage7::up(&db, None).await.unwrap();
    assert_eq!(history_business_rows(&db).await, originals);
    assert!(initialization::open(&db).await.is_err());
    assert!(
        initialization::upgrade_existing(
            &db,
            UpgradeRequest {
                database_scope: &scope,
                native_identity: "wrong-database",
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            }
        )
        .await
        .is_err()
    );
    assert_eq!(history_business_rows(&db).await, originals);
    initialization::upgrade_existing(
        &db,
        UpgradeRequest {
            database_scope: &scope,
            native_identity: &identity,
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    let key = native::NativeKey {
        table: source.owner.0,
        values: source.owner.1,
    };
    let mut budget = history_budget();
    let index = history::recover_source(&db, &key, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert!(index.historical && index.committed.is_none());
    let original: native::NativeRow = history::read_part(&db, &index.original, &mut budget)
        .await
        .unwrap();
    assert_eq!(original, originals[&key]);
    assert_eq!(
        original["native_before"].as_deref(),
        Some(source.before.as_str())
    );
    assert_eq!(
        original["binding"].as_deref(),
        Some(source.binding.as_str())
    );
    let review = index.review.unwrap();
    assert_eq!(
        review.catalog.last().unwrap(),
        "m20261012_000001_pinning_management"
    );
    assert_eq!(review.expires_at, source.expires);
    assert!(review.expires_at < chrono::Utc::now());
    assert_eq!(review.confirmed_at, Some(source.now));
    let entries: Vec<history::legacy::PlanEntry> =
        history::read_part(&db, &review.entries, &mut budget)
            .await
            .unwrap();
    assert!(entries == vec![source.entry]);
    let root_rows = review
        .rows
        .iter()
        .filter(|row| {
            ["zip_batches", "zip_root_builds", "zip_root_references"]
                .contains(&row.key.table.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(root_rows.len(), 4);
    for row in &root_rows {
        let original: native::NativeRow = history::read_part(&db, &row.original, &mut budget)
            .await
            .unwrap();
        assert_eq!(original, originals[&row.key]);
        assert!(!original.contains_key("logical_released_at"));
    }
    let recovered = history::recover_recorded(&db, &key, &source.binding, &mut budget)
        .await
        .unwrap();
    assert!(recovered == source.expected_result);
    assert!(
        history::recover_recorded(&db, &key, "wrong-binding", &mut budget)
            .await
            .is_err()
    );
    let mut corrupt = root_rows[0].original.clone();
    corrupt.digest = "0".repeat(64);
    assert!(
        history::read_part::<_, native::NativeRow>(&db, &corrupt, &mut budget)
            .await
            .is_err()
    );
    let markers = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT logical_released_at FROM zip_root_references",
        ))
        .await
        .unwrap();
    assert_eq!(markers.len(), 2);
    assert!(markers.iter().all(|row| {
        row.try_get::<Option<String>>("", "logical_released_at")
            .unwrap()
            .is_none()
    }));
    assert_eq!(history_business_rows(&db).await, originals);
    initialization::open(&db).await.unwrap();
}

#[tokio::test]
async fn foreign_provider_result_remains_readable_without_local_receipt() {
    verify_old_provider_sql_history(true, true, false).await;
}

#[tokio::test]
async fn invalid_inventory_pointer_keeps_history_unavailable_and_can_resume() {
    verify_old_provider_sql_history(true, false, true).await;
}

async fn verify_old_provider_sql_history(committed: bool, foreign: bool, invalid_inventory: bool) {
    verify_old_provider_sql_history_case(committed, foreign, invalid_inventory, false).await;
}

#[tokio::test]
async fn action_outcome_conflict_with_valid_digest_keeps_upgrade_unavailable() {
    verify_old_provider_sql_history_case(true, false, false, true).await;
}

async fn verify_old_provider_sql_history_case(
    committed: bool,
    foreign: bool,
    invalid_inventory: bool,
    conflicting_outcome: bool,
) {
    let fixture = action_fixture::Fixture::new().await;
    remove_fixture_storage(&fixture.db).await;
    let source = write_old_provider_history(
        &fixture,
        "old-provider-confirmation",
        committed,
        foreign,
        conflicting_outcome,
    )
    .await;
    let OldProviderHistory {
        owner,
        domain,
        manifest_hash,
        ..
    } = &source;
    if conflicting_outcome {
        let attempt = initialization::upgrade_existing(
            &fixture.db,
            UpgradeRequest {
                database_scope: fixture.auth.database_scope(),
                native_identity: fixture.auth.database_identity(),
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await;
        assert!(
            attempt.is_err(),
            "receipt action and facts outcome must agree even with a valid digest"
        );
        assert!(initialization::open(&fixture.db).await.is_err());
        return;
    }
    if invalid_inventory {
        let invalid = format!(
            "S7I1\n{}",
            serde_json::to_string(&(&owner, "0".repeat(64))).unwrap()
        );
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "UPDATE pin_management_retirements SET inventory=$1 WHERE allocation_domain=$2",
                [invalid.clone().into(), domain.clone().into()],
            ))
            .await
            .unwrap();
        let attempt = initialization::upgrade_existing(
            &fixture.db,
            UpgradeRequest {
                database_scope: fixture.auth.database_scope(),
                native_identity: fixture.auth.database_identity(),
                source_version: "m20261013_000001_zip_root_logical_release",
                target_version: schema::VERSION,
            },
        )
        .await;
        assert!(attempt.is_err());
        assert!(initialization::open(&fixture.db).await.is_err());
        let retained: String = fixture
            .db
            .query_one(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "SELECT inventory FROM pin_management_retirements WHERE allocation_domain=$1",
                [domain.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "inventory")
            .unwrap();
        assert_eq!(retained, invalid);
        let correct = format!(
            "S7I1\n{}",
            serde_json::to_string(&(&owner, &manifest_hash)).unwrap()
        );
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "UPDATE pin_management_retirements SET inventory=$1 WHERE allocation_domain=$2",
                [correct.into(), domain.clone().into()],
            ))
            .await
            .unwrap();
    }
    verify_provider_upgrade(&fixture, committed, foreign, source).await;
}

struct OldProviderHistory {
    scope: String,
    identity: String,
    owner: (String, Vec<String>),
    domain: String,
    entry: ipfs_s3_gateway::store::initialization::history::legacy::PlanEntry,
    now: chrono::DateTime<chrono::Utc>,
    expires: chrono::DateTime<chrono::Utc>,
    before: String,
    binding: String,
    manifest_hash: String,
    expected_result:
        Option<ipfs_s3_gateway::store::initialization::history::legacy::RecordedAction>,
}

async fn write_old_provider_history(
    fixture: &action_fixture::Fixture,
    plan: &str,
    committed: bool,
    foreign: bool,
    conflicting_outcome: bool,
) -> OldProviderHistory {
    write_old_provider_original(
        (
            &fixture.db,
            &fixture.config,
            fixture.auth.database_scope(),
            fixture.auth.database_identity(),
        ),
        plan,
        committed,
        foreign,
        conflicting_outcome,
    )
    .await
}

async fn write_old_provider_original(
    (db, config, local_scope, local_identity): (
        &sea_orm::DatabaseConnection,
        &ipfs_s3_gateway::pinning::management::local_config::ManagementConfig,
        &str,
        &str,
    ),
    plan: &str,
    committed: bool,
    foreign: bool,
    conflicting_outcome: bool,
) -> OldProviderHistory {
    use ipfs_s3_gateway::{
        pinning::management as model,
        store::{initialization::history, pinning::management::native},
    };
    use sha2::{Digest, Sha256};
    let scope = if foreign {
        "foreign-history-scope"
    } else {
        local_scope
    };
    let identity = if foreign {
        "foreign-history-database"
    } else {
        local_identity
    };
    let owner = (
        schema::RECORDS.to_owned(),
        vec![
            scope.to_owned(),
            plan.to_owned(),
            "retire".to_owned(),
            "action".to_owned(),
        ],
    );
    let catalog = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await
        .unwrap()
        .iter()
        .map(|row| row.try_get::<String>("", "version").unwrap())
        .collect::<Vec<_>>();
    let mut tables = native::registry();
    if catalog.last().unwrap() == "m20261012_000001_pinning_management" {
        let root = tables
            .iter_mut()
            .find(|table| table.table == "zip_root_references")
            .unwrap();
        root.columns = root
            .columns
            .iter()
            .filter(|column| *column != "logical_released_at")
            .cloned()
            .collect::<Vec<_>>()
            .into();
        root.definitions = root
            .definitions
            .iter()
            .filter(|column| column.name != "logical_released_at")
            .cloned()
            .collect::<Vec<_>>()
            .into();
    }
    let digest = |domain: &[u8], raw: &str| {
        let mut hash = Sha256::new();
        hash.update(domain);
        hash.update(raw.as_bytes());
        hex::encode(hash.finalize())
    };
    let mut leaves = Vec::new();
    let mut manifest_tables = Vec::new();
    let mut frontier = Vec::new();
    for (ordinal, table) in tables.iter().enumerate() {
        let projection = table
            .columns
            .iter()
            .map(|column| format!("CAST(\"{column}\" AS TEXT) AS \"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        let order = table
            .primary_key
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        let rows = db
            .query_all(Statement::from_string(
                db.get_database_backend(),
                format!(
                    "SELECT {projection} FROM \"{}\" ORDER BY {order}",
                    table.table
                ),
            ))
            .await
            .unwrap();
        if ["pin_leases", "pin_lease_targets", "remote_pins"].contains(&table.table.as_str()) {
            assert!(rows.is_empty());
        }
        let mut versions = Vec::new();
        for row in rows {
            let mut cells = table
                .columns
                .iter()
                .map(|column| row.try_get::<Option<String>>("", column).unwrap())
                .collect::<Vec<_>>();
            if foreign
                && table.table == schema::RECORDS
                && row.try_get::<String>("", "kind").unwrap() == "database_scope"
            {
                for (column, value) in table.columns.iter().zip(&mut cells) {
                    if column == "database_scope" {
                        *value = Some(scope.into());
                    }
                    if column == "native_identity" {
                        *value = Some(identity.into());
                    }
                }
            }
            let key = table
                .primary_key
                .iter()
                .map(|column| {
                    cells[table
                        .columns
                        .iter()
                        .position(|name| name == column)
                        .unwrap()]
                    .clone()
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let local_action = table.table == schema::RECORDS
                && row.try_get::<String>("", "kind").unwrap() == "action"
                && row.try_get::<String>("", "database_scope").unwrap() == scope
                && row.try_get::<String>("", "native_identity").unwrap() == identity
                && row.try_get::<String>("", "principal").unwrap() == "local_configuration";
            let mut cells = cells
                .into_iter()
                .map(|cell| serde_json::to_value(cell).unwrap())
                .collect::<Vec<_>>();
            if local_action {
                let index = table
                    .columns
                    .iter()
                    .position(|column| column == "native_before")
                    .unwrap();
                let before = cells[index].as_str().unwrap();
                let source = (table.table.clone(), key.clone());
                let tuple = serde_json::to_vec(&(&source, "native_before", before.len())).unwrap();
                let mut hash = Sha256::new();
                hash.update(b"stage7/before/1\0");
                hash.update((tuple.len() as u64).to_be_bytes());
                hash.update(&tuple);
                hash.update(before.as_bytes());
                cells[index] =
                    serde_json::json!([source, before.len(), hex::encode(hash.finalize())]);
            }
            let leaf = serde_json::to_string(&(ordinal, &key, cells)).unwrap();
            versions.push((
                key.clone(),
                owner.clone(),
                leaves.len(),
                leaf.len(),
                digest(b"stage7/leaf/1\0", &leaf),
            ));
            frontier.push(native::NativeKey {
                table: table.table.clone(),
                values: key,
            });
            leaves.push(leaf);
        }
        manifest_tables.push((table.table.clone(), versions));
    }
    frontier.sort();
    let manifest = serde_json::to_string(&(&catalog, manifest_tables)).unwrap();
    let manifest_hash = digest(b"stage7/manifest/1\0", &manifest);
    let original = config.pinning.providers[0].route_snapshot();
    let encoded_route = serde_json::to_string(&original).unwrap();
    let domain: String = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT provider FROM pin_provider_routes WHERE snapshot=$1",
            [encoded_route.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "provider")
        .unwrap();
    let route = model::RouteSnapshot {
        identity: model::RouteIdentity::from_original(original.clone()),
        original: Some(original.clone()),
        available: true,
        allocation_allowed: true,
        cleanup: match original.cleanup {
            ipfs_s3_gateway::pinning::identity::CleanupMode::Retain => model::CleanupMode::Retain,
            ipfs_s3_gateway::pinning::identity::CleanupMode::Managed => model::CleanupMode::Managed,
        },
        exclusive_management: false,
    };
    let root_reference_ids = frontier
        .iter()
        .filter(|key| key.table == "zip_root_references")
        .cloned()
        .collect::<Vec<_>>();
    let root_count = root_reference_ids.len();
    let inventory = model::ProviderInventory {
        provider_id: original.provider_id.clone(),
        root_reference_ids,
        frontier,
        complete: true,
        attribution_complete: root_count == 0,
        allocation_stopped: false,
        leases: vec![],
    };
    let item = model::ItemRequest {
        item_id: "retire".into(),
        action: model::Action::Retire,
        subject: model::Subject::Provider {
            route: route.identity.clone(),
        },
    };
    let request = model::PlanRequest {
        plan_id: plan.into(),
        items: vec![item.clone()],
    };
    let entry = history::legacy::PlanEntry {
        id: serde_json::from_value(serde_json::json!(format!("management-v1:{plan}:retire")))
            .unwrap(),
        request: item,
        status: model::PlanStatus::Ready,
        capture: Some(history::legacy::Capture::Provider(Box::new(
            history::legacy::RetirementCapture {
                inventory: inventory.clone(),
                routes: vec![route.clone()],
                targets: vec![],
                remotes: vec![],
                historical_remotes: vec![],
                root_references: vec![],
            },
        ))),
        step: Some(model::PlannedStep::Retire(model::RetirementStep {
            stop_new_allocations: true,
            needs_attention: root_count != 0,
            leases: 0,
            retained: 0,
            unknown: 0,
            inflight: 0,
            cleanup_pending: 0,
            root_references: root_count,
        })),
    };
    let now = chrono::Utc::now() - chrono::Duration::days(1);
    let expires = now + chrono::Duration::minutes(5);
    let snapshot = history::legacy::Snapshot {
        observed_at: now.timestamp(),
        database_now: Some(now),
        configuration: Some(model::ConfigurationBinding {
            routes: config
                .pinning
                .providers
                .iter()
                .map(|provider| provider.route_snapshot())
                .collect(),
            policies: config.pinning.policies.clone(),
            limits: config
                .pinning
                .provider_limits
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            zip_rules: Some(config.zip_rules.clone()),
            source_node: config.source_node().map(str::to_owned),
        }),
        objects: vec![],
        routes: vec![route],
        targets: vec![],
        remotes: vec![],
        historical_remotes: vec![],
        unknown_resources: vec![],
        ledger_domains: vec![],
        inventories: vec![inventory],
        root_references: vec![],
        applied: vec![],
    };
    let core = serde_json::to_string(&(
        &request,
        vec![entry.clone()],
        model::Summary {
            items: 1,
            ..Default::default()
        },
        snapshot,
        now,
        expires,
        Vec::<ipfs_s3_gateway::store::pinning::management::records::MigrationPoint>::new(),
        &manifest_hash,
    ))
    .unwrap();
    let core_hash = digest(b"stage7/core/1\0", &core);
    let head = serde_json::to_string(&(
        scope,
        identity,
        "local_configuration",
        &owner,
        &catalog,
        &core_hash,
        &manifest_hash,
        leaves
            .iter()
            .map(|leaf| digest(b"stage7/leaf/1\0", leaf))
            .collect::<Vec<_>>(),
    ))
    .unwrap();
    let directory = leaves
        .iter()
        .map(|leaf| format!("{:08x}", leaf.len()))
        .collect::<String>();
    let before = format!(
        "S7F1\n{:08x}{:08x}{:08x}{:08x}\n{directory}\n{head}{core}{manifest}{}",
        head.len(),
        core.len(),
        manifest.len(),
        leaves.len(),
        leaves.concat()
    );
    let binding = format!(
        "S7B1\n{}",
        serde_json::to_string(&(1u8, &owner, &core_hash, now, Vec::<String>::new())).unwrap()
    );
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES($1,$2,'retire','action',$3,'local_configuration',$4,$5,NULL,'[]','[]','prepared',$6,$7,NULL)",
        [scope.into(),plan.into(),identity.into(),binding.clone().into(),before.clone().into(),now.to_rfc3339().into(),expires.to_rfc3339().into()])).await.unwrap();
    let expected_result = if committed {
        Some(
            commit_old_retirement(
                db,
                &domain,
                &owner,
                &entry,
                OldRetirementEvidence {
                    binding: &binding,
                    manifest_hash: &manifest_hash,
                    staged: now,
                    identity,
                },
                conflicting_outcome,
            )
            .await,
        )
    } else {
        None
    };
    OldProviderHistory {
        scope: scope.into(),
        identity: identity.into(),
        owner,
        domain,
        entry,
        now,
        expires,
        before,
        binding,
        manifest_hash,
        expected_result,
    }
}

async fn verify_provider_upgrade(
    fixture: &action_fixture::Fixture,
    committed: bool,
    foreign: bool,
    source: OldProviderHistory,
) {
    use ipfs_s3_gateway::store::initialization::history;
    let OldProviderHistory {
        scope,
        identity,
        owner,
        domain,
        entry,
        now,
        expires,
        before,
        binding,
        manifest_hash,
        expected_result,
    } = source;
    initialization::upgrade_existing(
        &fixture.db,
        UpgradeRequest {
            database_scope: fixture.auth.database_scope(),
            native_identity: fixture.auth.database_identity(),
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    let count_archives = || async {
        fixture
            .db
            .query_one(Statement::from_string(
                fixture.db.get_database_backend(),
                "SELECT COUNT(*) AS n FROM pin_management_archive",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap()
    };
    let archived = count_archives().await;
    initialization::upgrade_existing(
        &fixture.db,
        UpgradeRequest {
            database_scope: fixture.auth.database_scope(),
            native_identity: fixture.auth.database_identity(),
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    assert_eq!(count_archives().await, archived);
    let key = native::NativeKey {
        table: owner.0,
        values: owner.1,
    };
    let mut budget = native::meter::Budget::default();
    let index = history::recover_source(&fixture.db, &key, &mut budget)
        .await
        .unwrap()
        .unwrap();
    assert!(index.historical && index.committed.is_none());
    assert_eq!(index.recorded.is_some(), committed);
    let original: native::NativeRow = history::read_part(&fixture.db, &index.original, &mut budget)
        .await
        .unwrap();
    assert_eq!(original["native_before"].as_deref(), Some(before.as_str()));
    assert_eq!(original["binding"].as_deref(), Some(binding.as_str()));
    assert_eq!(original["result"].is_some(), committed);
    let review = index.review.unwrap();
    assert_eq!(review.database_scope, scope);
    assert_eq!(review.database_identity, identity);
    let entries: Vec<history::legacy::PlanEntry> =
        history::read_part(&fixture.db, &review.entries, &mut budget)
            .await
            .unwrap();
    assert!(entries == vec![entry]);
    assert_eq!(review.expires_at, expires);
    assert_eq!(review.confirmed_at, Some(now));
    assert_eq!(review.acknowledgements, Some(vec![]));
    assert_eq!(
        history::requires_new_confirmation(
            &fixture.db,
            &key,
            &mut native::meter::Budget::default()
        )
        .await
        .unwrap(),
        !committed
    );
    if let Some(expected) = expected_result {
        let data: history::legacy::RecordedAction = history::read_part(
            &fixture.db,
            index.recorded.as_ref().unwrap(),
            &mut native::meter::Budget::default(),
        )
        .await
        .unwrap();
        assert!(data == expected);
        let restored = history::recover_recorded(
            &fixture.db,
            &key,
            &binding,
            &mut native::meter::Budget::default(),
        )
        .await;
        if foreign {
            assert!(matches!(
                restored,
                Err(ipfs_s3_gateway::store::pinning::management::ManagementError::Confirmation)
            ));
        } else {
            assert!(restored.unwrap().unwrap() == expected);
        }
        let marker_key = native::NativeKey {
            table: schema::RETIREMENTS.into(),
            values: vec![domain.clone()],
        };
        let mut work = native::meter::Budget::default();
        let marker = history::recover_source(&fixture.db, &marker_key, &mut work)
            .await
            .unwrap()
            .unwrap();
        let marker_original: native::NativeRow =
            history::read_part(&fixture.db, &marker.original, &mut work)
                .await
                .unwrap();
        let expected_inventory = format!(
            "S7I1\n{}",
            serde_json::to_string(&((&key.table, &key.values), &manifest_hash)).unwrap()
        );
        assert_eq!(
            marker_original["inventory"].as_deref(),
            Some(expected_inventory.as_str())
        );
        assert!(marker.review.is_some());
    }
    let retired: i64 = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT CAST(retired AS BIGINT) AS retired FROM pin_provider_routes WHERE provider=$1",
            [domain.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "retired")
        .unwrap();
    assert_eq!(retired, i64::from(committed));
    let jobs: i64 = fixture
        .db
        .query_one(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_jobs",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(jobs, 0);
}

struct OldRetirementEvidence<'a> {
    binding: &'a str,
    manifest_hash: &'a str,
    staged: chrono::DateTime<chrono::Utc>,
    identity: &'a str,
}

async fn commit_old_retirement(
    db: &sea_orm::DatabaseConnection,
    domain: &str,
    owner: &(String, Vec<String>),
    entry: &ipfs_s3_gateway::store::initialization::history::legacy::PlanEntry,
    original: OldRetirementEvidence<'_>,
    conflicting_outcome: bool,
) -> ipfs_s3_gateway::store::initialization::history::legacy::RecordedAction {
    use ipfs_s3_gateway::store::{
        initialization::history::legacy,
        pinning::management::{native, records},
    };
    use sea_orm::TransactionTrait;
    use sha2::{Digest, Sha256};
    let OldRetirementEvidence {
        binding,
        manifest_hash,
        staged,
        identity,
    } = original;
    let tx = db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_provider_routes SET retired=1 WHERE provider=$1",
        [domain.into()],
    ))
    .await
    .unwrap();
    let inventory = format!(
        "S7I1\n{}",
        serde_json::to_string(&(owner, manifest_hash)).unwrap()
    );
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
        "INSERT INTO pin_management_retirements(allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind) VALUES($1,$2,$3,$4,'local_configuration',$5,$6,$7,'retain','[]','retired',$8,'action')",
        [domain.into(),owner.1[0].clone().into(),owner.1[1].clone().into(),owner.1[2].clone().into(),identity.into(),binding.into(),inventory.into(),staged.to_rfc3339().into()])).await.unwrap();
    let result = legacy::RecordedAction {
        receipt: legacy::Receipt {
            binding: entry.clone(),
            intent_ids: vec![],
            job_ids: vec![],
        },
        state: records::ActionState::Accepted,
        targets: vec![],
        root_releases: vec![],
        jobs: vec![],
        migration_points: vec![],
        decision: None,
    };
    let tables = [schema::RETIREMENTS, "pin_provider_routes"];
    let mut outputs = Vec::new();
    for (slot, name) in tables.iter().enumerate() {
        let table = native::registry()
            .into_iter()
            .find(|table| &table.table == name)
            .unwrap();
        let projection = table
            .columns
            .iter()
            .map(|column| format!("CAST(\"{column}\" AS TEXT) AS \"{column}\""))
            .collect::<Vec<_>>()
            .join(",");
        let row = tx
            .query_one(Statement::from_sql_and_values(
                tx.get_database_backend(),
                format!(
                    "SELECT {projection} FROM \"{name}\" WHERE \"{}\"=$1",
                    table.primary_key[0]
                ),
                [domain.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        let cells = table
            .columns
            .iter()
            .map(|column| row.try_get::<Option<String>>("", column).unwrap())
            .collect::<Vec<_>>();
        outputs.push(serde_json::json!([(name, [domain]), slot, cells]));
    }
    let facts = serde_json::json!([
        0,
        [
            [[schema::RETIREMENTS, [domain]], [0]],
            [["pin_provider_routes", [domain]], [1]]
        ],
        outputs,
        [],
        [],
        if conflicting_outcome {
            serde_json::json!(["cancel", [1, 0], [1, 1]])
        } else {
            serde_json::json!(["retire", [1, 0], []])
        },
        []
    ]);
    #[derive(serde::Serialize)]
    struct WithoutState<'a> {
        receipt: &'a legacy::Receipt,
        targets: &'a [String],
        jobs: &'a [records::JobAuthority],
        migration_points: &'a [records::MigrationPoint],
        decision: &'a Option<records::OperatorDecision>,
    }
    let fields = WithoutState {
        receipt: &result.receipt,
        targets: &result.targets,
        jobs: &result.jobs,
        migration_points: &result.migration_points,
        decision: &result.decision,
    };
    let mut hash = Sha256::new();
    hash.update(b"stage7/facts/1\0");
    hash.update(
        serde_json::to_vec(&(
            &owner.1[0],
            identity,
            "local_configuration",
            &owner.1[1],
            &owner.1[2],
            fields,
            &facts,
        ))
        .unwrap(),
    );
    let encoded = format!(
        "S7R1\n{}",
        serde_json::to_string(&(&result, &facts, hex::encode(hash.finalize()))).unwrap()
    );
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET state='applied',result=$1,completed_at=$2 WHERE database_scope=$3 AND plan_id=$4 AND item_id=$5 AND kind='action'",[encoded.into(),staged.to_rfc3339().into(),owner.1[0].clone().into(),owner.1[1].clone().into(),owner.1[2].clone().into()])).await.unwrap();
    tx.commit().await.unwrap();
    result
}
