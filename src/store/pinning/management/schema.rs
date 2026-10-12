//! Read-only schema validation. Only the registered migrator creates these tables.
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::MigratorTrait;
use super::{ManagementError, Result};

pub const VERSION: &str = "m20261012_000001_pinning_management";
pub const RECORDS: &str = "pin_management_records";
pub const RETIREMENTS: &str = "pin_management_retirements";

pub fn records_ddl(backend: DatabaseBackend) -> String {
    let time = if backend == DatabaseBackend::Postgres { "TIMESTAMPTZ" } else { "TEXT" };
    format!("CREATE TABLE pin_management_records (\
        database_scope TEXT NOT NULL, plan_id TEXT NOT NULL, item_id TEXT NOT NULL,\
        kind TEXT NOT NULL, principal TEXT NOT NULL, native_identity TEXT NOT NULL,\
        binding TEXT NOT NULL, native_before TEXT NOT NULL, result TEXT,\
        intent_ids TEXT NOT NULL, job_ids TEXT NOT NULL, state TEXT NOT NULL,\
        created_at {time} NOT NULL, expires_at {time}, completed_at {time},\
        PRIMARY KEY (database_scope,plan_id,item_id,kind),\
        CONSTRAINT ck_management_kind CHECK (kind IN ('database_scope','evidence_preparation','action')),\
        CONSTRAINT ck_management_state CHECK (state IN ('identity','prepared','applied','stopped','needs_attention')),\
        CONSTRAINT ck_management_identity CHECK (length(database_scope)>0 AND length(plan_id)>0 AND length(item_id)>0),\
        CONSTRAINT ck_management_binding CHECK ((kind='database_scope' AND state='identity' AND principal='' AND expires_at IS NULL AND completed_at IS NULL AND result IS NULL AND plan_id='schema' AND item_id='identity') OR (kind<>'database_scope' AND state<>'identity' AND length(principal)>0 AND length(native_identity)>0 AND expires_at IS NOT NULL)),\
        CONSTRAINT ck_management_result CHECK ((kind='action' AND state='prepared' AND result IS NULL AND completed_at IS NULL) OR (kind='evidence_preparation' AND state='prepared' AND result IS NOT NULL AND completed_at IS NOT NULL) OR (kind='action' AND state='applied' AND result IS NOT NULL AND completed_at IS NOT NULL) OR state IN ('identity','stopped','needs_attention'))\
    )")
}

pub fn retirements_ddl(backend: DatabaseBackend) -> String {
    let time = if backend == DatabaseBackend::Postgres { "TIMESTAMPTZ" } else { "TEXT" };
    format!("CREATE TABLE pin_management_retirements (\
        allocation_domain TEXT NOT NULL PRIMARY KEY, database_scope TEXT NOT NULL,\
        plan_id TEXT NOT NULL, item_id TEXT NOT NULL, principal TEXT NOT NULL,\
        native_identity TEXT NOT NULL, binding TEXT NOT NULL, inventory TEXT NOT NULL,\
        disposition TEXT NOT NULL, unresolved TEXT NOT NULL, state TEXT NOT NULL,\
        created_at {time} NOT NULL, record_kind TEXT NOT NULL DEFAULT 'action',\
        CONSTRAINT ck_management_retirement_identity CHECK (length(allocation_domain)>0 AND length(database_scope)>0 AND length(principal)>0),\
        CONSTRAINT ck_management_disposition CHECK (disposition IN ('retain','migrate','controlled_cleanup')),\
        CONSTRAINT ck_management_retirement_state CHECK (state IN ('retired','needs_attention')),\
        CONSTRAINT ck_management_record_kind CHECK (record_kind='action'),\
        FOREIGN KEY (database_scope,plan_id,item_id,record_kind) REFERENCES pin_management_records(database_scope,plan_id,item_id,kind)\
    )")
}

fn normalized(sql: &str) -> String {
    sql.chars().filter(|c| !c.is_whitespace() && *c != '"').flat_map(char::to_lowercase).collect()
}

pub async fn validate<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    let backend = db.get_database_backend();
    if !matches!(backend, DatabaseBackend::Sqlite | DatabaseBackend::Postgres) { return Err(ManagementError::Schema); }
    let rows = db.query_all(Statement::from_string(backend,
        "SELECT version FROM seaql_migrations ORDER BY version LIMIT 65"))
        .await.map_err(|_| ManagementError::Schema)?;
    let versions: Vec<String> = rows.iter().map(|r| r.try_get("", "version"))
        .collect::<std::result::Result<_, _>>().map_err(|_| ManagementError::Schema)?;
    let known: Vec<String> = crate::store::migrator::Migrator::migrations().iter().map(|m| m.name().to_owned()).collect();
    if versions != known || versions.last().map(String::as_str) != Some(VERSION) { return Err(ManagementError::Schema); }
    match backend {
        DatabaseBackend::Sqlite => {
            for (name, ddl) in [(RECORDS, records_ddl(backend)), (RETIREMENTS, retirements_ddl(backend))] {
                let rows = db.query_all(Statement::from_sql_and_values(backend,
                    "SELECT type,sql FROM sqlite_schema WHERE tbl_name=$1 ORDER BY type,name", [name.into()]))
                    .await.map_err(|_| ManagementError::Schema)?;
                let mut found = false;
                for row in rows {
                    let kind: String = row.try_get("", "type").map_err(|_| ManagementError::Schema)?;
                    let sql: Option<String> = row.try_get("", "sql").map_err(|_| ManagementError::Schema)?;
                    if kind == "table" && sql.as_deref().is_some_and(|sql| normalized(sql) == normalized(&ddl)) && !found { found = true; }
                    else if kind != "index" || sql.is_some() { return Err(ManagementError::Schema); }
                }
                if !found { return Err(ManagementError::Schema); }
            }
        }
        DatabaseBackend::Postgres => postgres::validate(db).await?,
        _ => return Err(ManagementError::Schema),
    }
    Ok(versions)
}

mod postgres;
