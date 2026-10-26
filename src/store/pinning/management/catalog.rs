//! Known migration catalog, not type-affinity guesses or a schema repair path.
use super::{ManagementError, Result, native::NativeTable};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, Statement,
    sea_query::{Alias, ColumnDef, ColumnType, SqliteQueryBuilder, Table},
};
use serde::{Deserialize, Serialize};
mod constraints;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeColumn {
    pub name: String,
    pub sqlite_type: String,
    pub postgres_type: String,
    pub nullable: bool,
}

fn timestamp_type() -> String {
    // Ask the installed query builder for the migrations' exact SQLite spelling.
    let sql = Table::create()
        .table(Alias::new("t"))
        .col(ColumnDef::new(Alias::new("c")).timestamp_with_time_zone())
        .to_string(SqliteQueryBuilder);
    sql.split_once("\"c\" ")
        .expect("static column SQL")
        .1
        .trim_end_matches(')')
        .trim()
        .to_ascii_uppercase()
}
fn column(table: &str, name: &str, kind: &str, nullable: bool) -> NativeColumn {
    let postgres_type = match kind {
        "BIGINT" => "bigint",
        "INTEGER" => "integer",
        "BOOLEAN" => "boolean",
        "JSON" => "jsonb",
        "TIME" => "timestamp with time zone",
        _ => "text",
    }
    .into();
    let sqlite_type = match kind {
        "JSON" => "TEXT".into(),
        "TIME" if table == "zip_root_references" && name == "logical_released_at" => "TEXT".into(),
        "TIME"
            if matches!(
                table,
                "standard_mutation_leases"
                    | "pin_management_records"
                    | "pin_management_retirements"
            ) =>
        {
            "TEXT".into()
        }
        "TIME"
            if matches!(
                table,
                "pin_leases"
                    | "pin_lease_targets"
                    | "remote_pins"
                    | "pin_jobs"
                    | "pin_provider_usage"
                    | "import_jobs"
                    | "import_prefix_claims"
                    | "import_job_targets"
                    | "import_job_results"
                    | "import_destinations"
                    | "pin_submit_history"
                    | "remote_pin_ledger"
                    | "pin_submit_observations"
                    | "pin_lease_requirements"
                    | "pin_replica_observations"
            ) =>
        {
            timestamp_type()
        }
        "TIME" => "TIMESTAMP".into(),
        "BIGINT" if table == "pin_replica_observations" && name == "sequence" => "INTEGER".into(),
        kind => kind.into(),
    };
    NativeColumn {
        name: name.into(),
        sqlite_type,
        postgres_type,
        nullable,
    }
}
pub(crate) fn entity_column(
    table: &str,
    name: &str,
    definition: &sea_orm::ColumnDef,
) -> NativeColumn {
    let kind = match definition.get_column_type() {
        ColumnType::String(_) | ColumnType::Text => "TEXT",
        ColumnType::Integer => "INTEGER",
        ColumnType::BigInteger => "BIGINT",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Json | ColumnType::JsonBinary => "JSON",
        ColumnType::TimestampWithTimeZone | ColumnType::Timestamp | ColumnType::DateTime => "TIME",
        _ => "UNSUPPORTED",
    };
    column(table, name, kind, definition.is_null())
}
pub(crate) fn raw_column(table: &str, name: &str) -> NativeColumn {
    let kind = if matches!(
        name,
        "epoch" | "expected_generation" | "input_art_size" | "size"
    ) {
        "BIGINT"
    } else if matches!(
        name,
        "created_at" | "updated_at" | "lease_until" | "expires_at" | "completed_at"
    ) {
        "TIME"
    } else {
        "TEXT"
    };
    let nullable = match table {
        "zip_v2_executions" => matches!(
            name,
            "input_sha256"
                | "input_art_cid"
                | "input_art_size"
                | "worker"
                | "lease_until"
                | "terminal_result"
        ),
        "zip_v2_manifest" => matches!(name, "object_key" | "cid" | "size" | "error_code"),
        "zip_v2_import_requests" => matches!(name, "expected_sha256" | "receipt_metadata"),
        "zip_v2_mpu_intakes" => name == "active_upload_id",
        "zip_v2_mpu_completions" => matches!(name, "response_xml" | "response_headers"),
        "pin_management_records" => matches!(name, "result" | "expires_at" | "completed_at"),
        "zip_batches" => name == "legacy_input_proof_json",
        _ => false,
    };
    column(table, name, kind, nullable)
}

pub async fn validate<C: ConnectionTrait>(db: &C, tables: &[NativeTable]) -> Result<()> {
    for table in tables {
        reject_shadow(db, &table.table).await?;
        if db.get_database_backend() == DatabaseBackend::Sqlite {
            sqlite(db, table).await?;
        } else {
            postgres(db, table).await?;
        }
    }
    reject_shadow(db, "seaql_migrations").await?;
    constraints::validate(db, tables).await?;
    immutable_history(db).await
}
/// Single-table catalog only: no current-schema constraints or history probes.
pub(crate) async fn validate_table<C: ConnectionTrait>(db: &C, table: &NativeTable) -> Result<()> {
    if db.get_database_backend() == DatabaseBackend::Sqlite && table.table == "pin_provider_routes"
    {
        let shape = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE name IN ('seaql_migrations',$1) AND type IN ('table','view')) AS shadow,EXISTS(SELECT 1 FROM pragma_table_list($1) WHERE schema='main' AND name=$1 AND type='table') AS main_table",
                [table.table.clone().into()],
            ))
            .await
            .map_err(|_| ManagementError::Schema)?
            .ok_or(ManagementError::Schema)?;
        if shape
            .try_get::<i32>("", "shadow")
            .map_err(|_| ManagementError::Schema)?
            != 0
            || shape
                .try_get::<i32>("", "main_table")
                .map_err(|_| ManagementError::Schema)?
                != 1
        {
            return Err(ManagementError::Schema);
        }
        return sqlite_columns(db, table).await;
    }
    reject_shadow(db, "seaql_migrations").await?;
    reject_shadow(db, &table.table).await?;
    match db.get_database_backend() {
        DatabaseBackend::Sqlite => sqlite(db, table).await,
        DatabaseBackend::Postgres => postgres(db, table).await,
        _ => Err(ManagementError::Schema),
    }
}
async fn sqlite<C: ConnectionTrait>(db: &C, table: &NativeTable) -> Result<()> {
    if table.table == "pin_provider_routes" {
        let shape = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT type FROM pragma_table_list($1) WHERE schema='main' AND name=$1",
                [table.table.clone().into()],
            ))
            .await
            .map_err(|_| ManagementError::Schema)?
            .ok_or(ManagementError::Schema)?;
        if shape
            .try_get::<String>("", "type")
            .map_err(|_| ManagementError::Schema)?
            != "table"
        {
            return Err(ManagementError::Schema);
        }
    }
    sqlite_columns(db, table).await
}
/// Column/PK proof only; the caller separately proves table kind and no shadow.
pub(in crate::store::pinning) async fn sqlite_columns<C: ConnectionTrait>(
    db: &C,
    table: &NativeTable,
) -> Result<()> {
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT name,type,\"notnull\" AS required,pk,hidden FROM pragma_table_xinfo($1) ORDER BY name",[table.table.clone().into()])).await.map_err(|_|ManagementError::Schema)?;
    if rows.len() != table.definitions.len() {
        return Err(ManagementError::Schema);
    }
    let mut keys = Vec::new();
    for (row, expected) in rows.iter().zip(&table.definitions) {
        let name: String = row
            .try_get("", "name")
            .map_err(|_| ManagementError::Schema)?;
        let ty: String = row
            .try_get("", "type")
            .map_err(|_| ManagementError::Schema)?;
        let required: i32 = row
            .try_get("", "required")
            .map_err(|_| ManagementError::Schema)?;
        let pk: i32 = row.try_get("", "pk").map_err(|_| ManagementError::Schema)?;
        if name != expected.name
            || ty.to_ascii_uppercase() != expected.sqlite_type
            || (required != 0) == expected.nullable
            || row
                .try_get::<i32>("", "hidden")
                .map_err(|_| ManagementError::Schema)?
                != 0
        {
            return Err(ManagementError::Schema);
        }
        if pk > 0 {
            keys.push((pk, name));
        }
    }
    keys.sort();
    if keys.into_iter().map(|(_, name)| name).collect::<Vec<_>>() != *table.primary_key {
        return Err(ManagementError::Schema);
    }
    Ok(())
}
async fn postgres<C: ConnectionTrait>(db: &C, table: &NativeTable) -> Result<()> {
    let shape=db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.relhassubclass FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1",[table.table.clone().into()])).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
    if shape
        .try_get::<String>("", "kind")
        .map_err(|_| ManagementError::Schema)?
        != "r"
        || shape
            .try_get::<String>("", "persistence")
            .map_err(|_| ManagementError::Schema)?
            != "p"
    {
        return Err(ManagementError::Schema);
    }
    for flag in [
        "relrowsecurity",
        "relforcerowsecurity",
        "relispartition",
        "relhassubclass",
    ] {
        if shape
            .try_get::<bool>("", flag)
            .map_err(|_| ManagementError::Schema)?
        {
            return Err(ManagementError::Schema);
        }
    }
    let rows=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT a.attname::text AS name,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,a.attisdropped,a.attgenerated::text AS generated FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attname LIMIT 128",[table.table.clone().into()])).await.map_err(|_|ManagementError::Schema)?;
    if rows.len() != table.definitions.len() {
        return Err(ManagementError::Schema);
    }
    for (row, expected) in rows.iter().zip(&table.definitions) {
        if row
            .try_get::<String>("", "name")
            .map_err(|_| ManagementError::Schema)?
            != expected.name
            || row
                .try_get::<String>("", "type")
                .map_err(|_| ManagementError::Schema)?
                != expected.postgres_type
            || row
                .try_get::<bool>("", "attnotnull")
                .map_err(|_| ManagementError::Schema)?
                == expected.nullable
            || row
                .try_get::<bool>("", "attisdropped")
                .map_err(|_| ManagementError::Schema)?
            || !row
                .try_get::<String>("", "generated")
                .map_err(|_| ManagementError::Schema)?
                .is_empty()
        {
            return Err(ManagementError::Schema);
        }
    }
    let keys=db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT a.attname::text AS name FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(k.conkey) WITH ORDINALITY AS key(attnum,ordinal) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=key.attnum WHERE n.nspname=current_schema() AND c.relname=$1 AND k.contype='p' AND k.convalidated AND NOT k.condeferrable ORDER BY key.ordinal",[table.table.clone().into()])).await.map_err(|_|ManagementError::Schema)?;
    let keys = keys
        .iter()
        .map(|row| {
            row.try_get::<String>("", "name")
                .map_err(|_| ManagementError::Schema)
        })
        .collect::<Result<Vec<_>>>()?;
    if keys != *table.primary_key {
        return Err(ManagementError::Schema);
    }
    Ok(())
}
async fn reject_shadow<C: ConnectionTrait>(db: &C, table: &str) -> Result<()> {
    let sql = if db.get_database_backend() == DatabaseBackend::Sqlite {
        "SELECT 1 FROM sqlite_temp_schema WHERE type IN ('table','view') AND name=$1"
    } else {
        "SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace=pg_my_temp_schema() AND relname=$1"
    };
    if db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            [table.into()],
        ))
        .await
        .map_err(|_| ManagementError::Schema)?
        .is_some()
    {
        return Err(ManagementError::Schema);
    }
    Ok(())
}
async fn immutable_history<C: ConnectionTrait>(db: &C) -> Result<()> {
    for table in [
        "pin_lease_requirements",
        "pin_replica_observations",
        "pin_legacy_migrations",
        "pin_legacy_migration_items",
    ] {
        if db.get_database_backend() == DatabaseBackend::Sqlite {
            for operation in ["update", "delete"] {
                let name = format!("{table}_no_{operation}");
                let row=db.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"SELECT sql FROM sqlite_schema WHERE type='trigger' AND name=$1 AND tbl_name=$2",[name.clone().into(),table.into()])).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
                let sql: String = row
                    .try_get("", "sql")
                    .map_err(|_| ManagementError::Schema)?;
                let message = if table.starts_with("pin_legacy_") {
                    "immutable legacy migration receipt"
                } else {
                    "immutable pin history"
                };
                let expected = format!(
                    "CREATE TRIGGER {name} BEFORE {} ON {table} BEGIN SELECT RAISE(ABORT, '{message}'); END",
                    operation.to_ascii_uppercase()
                );
                if compact(&sql) != compact(&expected) {
                    return Err(ManagementError::Schema);
                }
            }
        } else {
            let message = if table.starts_with("pin_legacy_") {
                "immutable legacy migration receipt"
            } else {
                "immutable pin history"
            };
            let row=db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,"SELECT t.tgtype::integer AS type,t.tgenabled::text AS enabled,p.prosrc AS body,t.tgqual IS NULL AS unconditional,t.tgattr::text AS columns,encode(t.tgargs,'hex') AS args,p.prosecdef,p.proconfig IS NULL AS default_config,p.pronargs=0 AS no_args,p.prorettype='trigger'::regtype AS returns_trigger,pn.nspname=current_schema() AS local_function FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND t.tgname=$2 AND NOT t.tgisinternal",[table.into(),format!("{table}_immutable").into()])).await.map_err(|_|ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
            if row
                .try_get::<i32>("", "type")
                .map_err(|_| ManagementError::Schema)?
                != 27
                || row
                    .try_get::<String>("", "enabled")
                    .map_err(|_| ManagementError::Schema)?
                    != "O"
                || compact(
                    &row.try_get::<String>("", "body")
                        .map_err(|_| ManagementError::Schema)?,
                ) != compact(&format!("BEGIN RAISE EXCEPTION '{message}'; END"))
                || !row
                    .try_get::<String>("", "columns")
                    .map_err(|_| ManagementError::Schema)?
                    .is_empty()
                || !row
                    .try_get::<String>("", "args")
                    .map_err(|_| ManagementError::Schema)?
                    .is_empty()
                || row
                    .try_get::<bool>("", "prosecdef")
                    .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
            for flag in [
                "unconditional",
                "default_config",
                "no_args",
                "returns_trigger",
                "local_function",
            ] {
                if !row
                    .try_get::<bool>("", flag)
                    .map_err(|_| ManagementError::Schema)?
                {
                    return Err(ManagementError::Schema);
                }
            }
        }
    }
    Ok(())
}
fn compact(sql: &str) -> String {
    let mut literal = false;
    let mut result = String::new();
    for ch in sql.chars() {
        if ch == '\'' {
            literal = !literal;
            result.push(ch);
        } else if literal {
            result.push(ch);
        } else if !ch.is_whitespace() && ch != '"' {
            result.extend(ch.to_lowercase());
        }
    }
    result
}
