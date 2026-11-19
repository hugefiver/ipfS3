use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

// Children precede their parents for SQLite, which performs an implicit delete on DROP TABLE.
const PIN_TABLES: &[&str] = &[
    "pin_cid_comparisons",
    "pin_management_archive_identity",
    "pin_management_inline_archive",
    "pin_management_archive_chunks",
    "pin_management_archive",
    "pin_management_pending_scope",
    "pin_management_locator",
    "pin_management_source_control",
    "pin_management_revision",
    "pin_management_readiness",
];

const PIN_NATIVE_SOURCES: &[&str] = &[
    "pin_management_retirements",
    "pin_management_records",
    "pin_legacy_migration_items",
    "pin_legacy_migrations",
    "pin_replica_observations",
    "pin_lease_requirements",
    "pin_submit_observations",
    "pin_extension_decisions",
    "remote_pin_ledger",
    "pin_provider_routes",
    "pin_resource_history",
    "pin_invocation_routes",
    "pin_submit_history",
    "pin_provider_usage",
    "pin_jobs",
    "remote_pins",
    "pin_lease_targets",
    "pin_leases",
];

// The old management native registry (excluding the pin-only tables).
const SHARED_REVISION_SOURCES: &[&str] = &[
    "buckets",
    "objects",
    "object_versions",
    "multipart_uploads",
    "multipart_parts",
    "import_jobs",
    "import_job_requests",
    "import_job_targets",
    "import_job_results",
    "import_destinations",
    "import_prefix_claims",
    "zip_batches",
    "zip_manifest_entries",
    "zip_root_builds",
    "zip_root_references",
    "standard_mutation_leases",
    "lifecycle_actions",
    "lifecycle_transitions",
    "physical_residencies",
    "version_residencies",
    "residency_references",
    "zip_v2_executions",
    "zip_v2_manifest",
    "zip_v2_targets",
    "zip_v2_import_requests",
    "zip_v2_mpu_intakes",
    "zip_v2_mpu_completions",
];

const SHARED_PIN_INDEXES: &[(&str, &str)] = &[
    ("object_versions", "uq_object_versions_decision_owner"),
    ("object_versions", "s75_version_public"),
    ("zip_manifest_entries", "s75_manifest_version"),
    ("lifecycle_actions", "s75_lifecycle_version"),
    ("lifecycle_transitions", "s75_transition_version"),
    ("residency_references", "s75_residency_version"),
];

const PG_GUARD_FUNCTIONS: &[&str] = &[
    "bound_legacy_migration_item",
    "reject_legacy_migration_mutation",
    "reject_pin_history_mutation",
    "s75_archive_guard",
    "s75_archive_id_claim",
    "s75_chunk_guard",
    "s75_immutable",
    "s75_no_truncate",
];

const EXPORT_SOURCES: &[&str] = &[
    "pin_management_archive",
    "pin_management_archive_chunks",
    "pin_management_inline_archive",
];

const REVISION_EVENTS: &[&str] = &["insert", "update", "delete"];

pub(super) async fn run<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let shared = SHARED_REVISION_SOURCES
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(",");
    let postgres = backend == DatabaseBackend::Postgres;
    let (schema, catalog) = match backend {
        DatabaseBackend::Sqlite => {
            let pins = PIN_TABLES
                .iter()
                .chain(PIN_NATIVE_SOURCES)
                .map(|name| format!("'{name}'"))
                .collect::<Vec<_>>()
                .join(",");
            // SQLite's implicit delete must not apply foreign key actions to retained tables.
            if db.query_one(Statement::from_string(backend, format!(
                r#"SELECT m.name FROM main.sqlite_schema m JOIN pragma_foreign_key_list(m.name, 'main') f
                   WHERE m.type='table' AND m.name NOT IN ({pins}) AND lower(f."table") IN ({pins})
                   UNION ALL SELECT name FROM main.sqlite_schema WHERE type='table' AND lower(name) IN ({pins}) AND name NOT IN ({pins})
                   LIMIT 1"#,
            ))).await?.is_some() {
                return Err(DbErr::Migration("legacy pin tables have external SQLite foreign keys or name conflicts".into()));
            }
            (
                "main".to_owned(),
                format!(
                    "SELECT type AS kind, tbl_name AS owner, name FROM main.sqlite_schema
                 WHERE type IN ('trigger','index') AND tbl_name IN ({shared})",
                ),
            )
        }
        DatabaseBackend::Postgres => {
            let schema: String = db
                .query_one(Statement::from_string(
                    backend,
                    "SELECT pg_catalog.quote_ident(pg_catalog.current_schema()) AS name",
                ))
                .await?
                .ok_or_else(|| DbErr::Migration("missing PostgreSQL migration schema".into()))?
                .try_get("", "name")?;
            (schema, format!(
                "WITH owners AS (
                     SELECT c.oid,c.relname FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                     WHERE n.nspname=pg_catalog.current_schema() AND c.relname IN ({shared})
                 )
                 SELECT 'trigger' AS kind,o.relname::text AS owner,t.tgname::text AS name FROM owners o
                 JOIN pg_catalog.pg_trigger t ON o.oid=t.tgrelid WHERE NOT t.tgisinternal
                 UNION ALL
                 SELECT 'index',o.relname::text,c.relname::text FROM owners o
                 JOIN pg_catalog.pg_index i ON o.oid=i.indrelid
                 JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid",
            ))
        }
        _ => {
            return Err(DbErr::Migration(
                "unsupported legacy pinning database".into(),
            ));
        }
    };

    let mut before_tables = String::new();
    let mut after_tables = String::new();
    for row in db
        .query_all(Statement::from_string(backend, catalog))
        .await?
    {
        let kind: String = row.try_get("", "kind")?;
        let owner: String = row.try_get("", "owner")?;
        let name: String = row.try_get("", "name")?;
        match kind.as_str() {
            "trigger"
                if REVISION_EVENTS
                    .iter()
                    .any(|event| name == format!("s75_{owner}_{event}"))
                    || postgres && name == format!("s75_{owner}_truncate") =>
            {
                if postgres {
                    before_tables.push_str(&format!(
                        "DROP TRIGGER \"{name}\" ON {schema}.\"{owner}\";\n"
                    ));
                } else {
                    before_tables.push_str(&format!("DROP TRIGGER {schema}.\"{name}\";\n"));
                }
            }
            "index" if SHARED_PIN_INDEXES.contains(&(owner.as_str(), name.as_str())) => {
                after_tables.push_str(&format!("DROP INDEX {schema}.\"{name}\";\n"));
            }
            _ => {}
        }
    }
    if !postgres {
        // A rollback receipt references its apply receipt with ON DELETE RESTRICT.
        before_tables.push_str("PRAGMA defer_foreign_keys=ON;\n");
    }
    for table in PIN_TABLES.iter().chain(PIN_NATIVE_SOURCES) {
        before_tables.push_str(&format!("DROP TABLE IF EXISTS {schema}.\"{table}\";\n"));
    }
    // The decision-owner index can back the legacy decision foreign key.
    before_tables.push_str(&after_tables);
    if postgres {
        for table in SHARED_REVISION_SOURCES.iter().chain(PIN_NATIVE_SOURCES) {
            for event in REVISION_EVENTS {
                before_tables.push_str(&format!(
                    "DROP FUNCTION IF EXISTS {schema}.\"s75_{table}_{event}\"();\n"
                ));
            }
        }
        for table in EXPORT_SOURCES {
            for event in REVISION_EVENTS {
                before_tables.push_str(&format!(
                    "DROP FUNCTION IF EXISTS {schema}.\"s75_export_{table}_{event}\"();\n"
                ));
            }
        }
        for event in REVISION_EVENTS {
            before_tables.push_str(&format!(
                "DROP FUNCTION IF EXISTS {schema}.\"s75_locator_{event}\"();\n"
            ));
        }
        for name in PG_GUARD_FUNCTIONS {
            before_tables.push_str(&format!("DROP FUNCTION IF EXISTS {schema}.\"{name}\"();\n"));
        }
    }
    db.execute_unprepared(&before_tables).await?;
    Ok(())
}
