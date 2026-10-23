use ipfs_s3_gateway::store::{
    initialization::{self, UpgradeRequest},
    pinning::management::{records, schema},
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
#[path = "stage75_legacy_import.rs"]
mod legacy_import;

pub async fn convert_current(db: &DatabaseConnection, auth: &records::LocalAuthorization) {
    let backend = db.get_database_backend();
    for (name, table, _, _) in schema::storage::triggers(backend) {
        let sql = if backend == DatabaseBackend::Postgres {
            format!("DROP TRIGGER {name} ON {table}")
        } else {
            format!("DROP TRIGGER {name}")
        };
        db.execute_unprepared(&sql).await.unwrap();
        if backend == DatabaseBackend::Postgres {
            db.execute_unprepared(&format!("DROP FUNCTION {name}()"))
                .await
                .unwrap();
        }
    }
    for name in ["s75_applied_no_delete", "s75_applied_no_update"] {
        let sql = if backend == DatabaseBackend::Postgres {
            format!("DROP TRIGGER {name} ON pin_management_records")
        } else {
            format!("DROP TRIGGER {name}")
        };
        db.execute_unprepared(&sql).await.unwrap();
    }
    if backend == DatabaseBackend::Postgres {
        for table in ipfs_s3_gateway::store::pinning::management::native::registry() {
            db.execute_unprepared(&format!(
                "DROP TRIGGER s75_{}_truncate ON \"{}\"",
                table.table, table.table
            ))
            .await
            .unwrap();
        }
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
    if backend == DatabaseBackend::Postgres {
        for name in [
            "s75_no_truncate",
            "s75_immutable",
            "s75_archive_guard",
            "s75_chunk_guard",
            "s75_archive_id_claim",
        ] {
            db.execute_unprepared(&format!("DROP FUNCTION {name}()"))
                .await
                .unwrap();
        }
    }
    legacy_import::restore(db).await;
    db.execute(Statement::from_sql_and_values(
        backend,
        "DELETE FROM seaql_migrations WHERE version IN ($1,'m20261017_000001_management_storage')",
        [schema::VERSION.into()],
    ))
    .await
    .unwrap();
    initialization::upgrade_existing(
        db,
        UpgradeRequest {
            database_scope: auth.database_scope(),
            native_identity: auth.database_identity(),
            source_version: "m20261013_000001_zip_root_logical_release",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
}
