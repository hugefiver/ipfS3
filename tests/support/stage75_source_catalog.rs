//! Historical wire catalog derived from the real predecessor DDL, not current entities.
use ipfs_s3_gateway::store::{
    migrations::{Historical, history},
    pinning::management::{catalog::NativeColumn, native::NativeTable},
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use sea_orm_migration::MigratorTrait;

pub async fn tables(versions: &[String]) -> Vec<NativeTable> {
    assert!(history::is_prefix(versions));
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Historical::up(&db, Some(versions.len() as u32))
        .await
        .unwrap();
    let names = "buckets,objects,object_versions,pin_leases,pin_lease_targets,remote_pins,pin_provider_usage,pin_provider_routes,remote_pin_ledger,pin_jobs,pin_submit_history,pin_invocation_routes,pin_resource_history,pin_submit_observations,pin_replica_observations,pin_lease_requirements,pin_extension_decisions,pin_legacy_migrations,pin_legacy_migration_items,multipart_uploads,multipart_parts,import_jobs,import_job_targets,import_job_results,import_destinations,import_prefix_claims,zip_batches,zip_manifest_entries,zip_root_builds,zip_root_references,standard_mutation_leases,lifecycle_actions,lifecycle_transitions,physical_residencies,version_residencies,residency_references,zip_v2_executions,zip_v2_manifest,zip_v2_targets,zip_v2_import_requests,zip_v2_mpu_intakes,zip_v2_mpu_completions,pin_management_records,pin_management_retirements";
    let mut tables = Vec::new();
    for table in names.split(',') {
        let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT name,type,\"notnull\" AS required,pk FROM pragma_table_info($1) ORDER BY name",[table.into()])).await.unwrap();
        let mut keys = Vec::new();
        let mut definitions = Vec::new();
        for row in rows {
            let name: String = row.try_get("", "name").unwrap();
            let kind: String = row.try_get::<String>("", "type").unwrap().to_uppercase();
            let pk: i32 = row.try_get("", "pk").unwrap();
            if pk > 0 {
                keys.push((pk, name.clone()));
            }
            let postgres_type = match kind.as_str() {
                "BIGINT" => "bigint",
                "INTEGER" if table == "pin_replica_observations" && name == "sequence" => "bigint",
                "INTEGER" => "integer",
                "BOOLEAN" => "boolean",
                "TIMESTAMP" | "TIMESTAMP_WITH_TIMEZONE_TEXT" => "timestamp with time zone",
                _ if matches!(table, "objects" | "multipart_uploads")
                    && matches!(name.as_str(), "metadata" | "tags_json") =>
                {
                    "jsonb"
                }
                _ if matches!(
                    table,
                    "standard_mutation_leases"
                        | "pin_management_records"
                        | "pin_management_retirements"
                        | "zip_root_references"
                ) && matches!(
                    name.as_str(),
                    "created_at"
                        | "expires_at"
                        | "completed_at"
                        | "lease_until"
                        | "logical_released_at"
                ) =>
                {
                    "timestamp with time zone"
                }
                _ => "text",
            };
            definitions.push(NativeColumn {
                name,
                sqlite_type: kind,
                postgres_type: postgres_type.into(),
                nullable: row.try_get::<i32>("", "required").unwrap() == 0,
            });
        }
        keys.sort();
        tables.push(NativeTable {
            table: table.into(),
            columns: definitions
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .into(),
            primary_key: keys.into_iter().map(|(_, n)| n).collect::<Vec<_>>().into(),
            rows: vec![],
            definitions: definitions.into(),
        });
    }
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    db.close().await.unwrap();
    tables
}
