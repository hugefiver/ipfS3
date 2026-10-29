//! Read-only schema validation. Only the registered migrator creates these tables.
use super::{ManagementError, Result};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::MigratorTrait;

pub const VERSION: &str = crate::store::migrations::baseline::VERSION;
mod installation;
pub mod storage;
pub const RECORDS: &str = "pin_management_records";
pub const RETIREMENTS: &str = "pin_management_retirements";

pub fn records_ddl(backend: DatabaseBackend) -> String {
    let time = if backend == DatabaseBackend::Postgres {
        "TIMESTAMPTZ"
    } else {
        "TEXT"
    };
    format!(
        "CREATE TABLE pin_management_records (\
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
    )"
    )
}

pub fn retirements_ddl(backend: DatabaseBackend) -> String {
    let time = if backend == DatabaseBackend::Postgres {
        "TIMESTAMPTZ"
    } else {
        "TEXT"
    };
    format!(
        "CREATE TABLE pin_management_retirements (\
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
    )"
    )
}

fn normalized(sql: &str) -> String {
    sql.chars()
        .filter(|c| !c.is_whitespace() && *c != '"')
        .flat_map(char::to_lowercase)
        .collect()
}

pub async fn validate<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    validate_state(db, false).await
}

pub(in crate::store::pinning) async fn validate_observed(
    window: &super::admission::worker_observation::WorkerReadWindow,
) -> Result<Vec<String>> {
    if let Some(versions) = window.runtime_versions().await {
        return versions.map_err(|error| match error {
            crate::error::AppError::ManagementBudget => ManagementError::Budget,
            _ => ManagementError::Schema,
        });
    }
    storage::observed_control(window).await
}

pub(in crate::store::pinning) async fn validate_quota(
    window: &crate::store::pinning::quota::production::QuotaReadWindow<'_>,
) -> Result<Vec<String>> {
    storage::quota_control(window).await
}

/// Reuse versions from the same SQLite classification read, not a prior call.
/// This still reads the complete current storage catalog, guards and readiness.
pub(in crate::store::pinning) async fn validate_sqlite_with_versions<C: ConnectionTrait>(
    db: &C,
    versions: &[String],
) -> Result<()> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return Err(ManagementError::Schema);
    }
    validate_current_versions(versions)?;
    storage::validate_schema(db, false).await
}

fn validate_current_versions(versions: &[String]) -> Result<()> {
    let known: Vec<String> = crate::store::migrator::Migrator::migrations()
        .iter()
        .map(|migration| migration.name().to_owned())
        .collect();
    if versions != known.as_slice() || versions.last().map(String::as_str) != Some(VERSION) {
        return Err(ManagementError::Schema);
    }
    Ok(())
}

/// Validates the complete installed layout without publishing it for runtime use.
/// Only explicit, identity-bound conversion can read a backfill database.
pub(crate) async fn validate_for_upgrade<C: ConnectionTrait>(db: &C) -> Result<Vec<String>> {
    let versions = validate_state(db, true).await?;
    validate_installation_catalog(db).await?;
    Ok(versions)
}

/// Historical layout validation does not qualify a Store or authorize execution.
pub(crate) async fn validate_m23_layout<C: ConnectionTrait>(db: &C, backfill: bool) -> Result<()> {
    reject_sqlite_shadows(db).await?;
    storage::validate_schema(db, backfill).await?;
    if db.get_database_backend() == DatabaseBackend::Postgres {
        postgres::validate(db).await?;
    }
    validate_installation_catalog(db).await
}

/// Cold installation qualification includes tables outside the management
/// evidence projection. They must not become archive members or revision inputs.
pub(crate) async fn validate_installation_catalog<C: ConnectionTrait>(db: &C) -> Result<()> {
    super::catalog::validate(db, installation_catalog()).await?;
    installation::validate(db).await
}

fn installation_catalog() -> &'static [super::native::NativeTable] {
    use super::native::{entity, raw};
    use crate::store::entities::*;
    static CATALOG: std::sync::LazyLock<Vec<super::native::NativeTable>> = std::sync::LazyLock::new(
        || {
            let mut tables = super::native::current_catalog().to_vec();
            tables.extend([
                entity::<bucket_cors_config::Entity>(),
                entity::<bucket_lifecycle_config::Entity>(),
                entity::<object_tag::Entity>(),
                entity::<residency_backfill::Entity>(),
                raw("zip_mpu_replays", "batch_id,request_fingerprint,request_contract,prepared_archive_cid,archive_cid,archive_size,public_version_id,server_side_encryption,response_xml,response_headers_json", "batch_id"),
            ]);
            tables
        },
    );
    &CATALOG
}

async fn validate_state<C: ConnectionTrait>(db: &C, backfill: bool) -> Result<Vec<String>> {
    reject_sqlite_shadows(db).await?;
    let versions = read_versions(db).await?;
    storage::validate_schema(db, backfill).await?;
    if db.get_database_backend() == DatabaseBackend::Postgres {
        postgres::validate(db).await?;
    }
    Ok(versions)
}

// Identifier folding is ASCII; BLOB equality cannot invoke overridden collations.
async fn reject_sqlite_shadows<C: ConnectionTrait>(db: &C) -> Result<()> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }
    let mut names = std::collections::BTreeSet::new();
    names.extend(
        installation_catalog()
            .iter()
            .map(|table| table.table.as_str()),
    );
    let tables = storage::tables(DatabaseBackend::Sqlite);
    names.extend(tables.iter().map(|(name, _)| *name));
    names.extend(storage::SOURCE_TABLES.iter().copied());
    names.extend([RECORDS, RETIREMENTS, "seaql_migrations"]);
    let names = names
        .into_iter()
        .map(|name| format!("X'{}'", hex::encode(name.to_ascii_lowercase().as_bytes())))
        .collect::<Vec<_>>()
        .join(",");
    if db.query_one(Statement::from_string(DatabaseBackend::Sqlite, format!(
        "SELECT 1 FROM sqlite_temp_schema WHERE type IN ('table','view') AND CAST(lower(name) AS BLOB) IN ({names}) LIMIT 1"
    ))).await.map_err(|_| ManagementError::Schema)?.is_some() {
        return Err(ManagementError::Schema);
    }
    Ok(())
}

pub(in crate::store::pinning) async fn read_versions<C: ConnectionTrait>(
    db: &C,
) -> Result<Vec<String>> {
    let backend = db.get_database_backend();
    if !matches!(backend, DatabaseBackend::Sqlite | DatabaseBackend::Postgres) {
        return Err(ManagementError::Schema);
    }
    let known: Vec<String> = crate::store::migrator::Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect();
    let version_bytes = known.iter().map(String::len).max().unwrap_or(0);
    let length = if backend == DatabaseBackend::Sqlite {
        "length(CAST(version AS BLOB))"
    } else {
        "octet_length(version)"
    };
    let rows = db
        .query_all(Statement::from_string(
            backend,
            format!("SELECT CASE WHEN {length}<={version_bytes} THEN version ELSE NULL END AS version FROM seaql_migrations ORDER BY seaql_migrations.version LIMIT 65"),
        ))
        .await
        .map_err(|_| ManagementError::Schema)?;
    let versions: Vec<String> = rows
        .iter()
        .map(|r| r.try_get("", "version"))
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| ManagementError::Schema)?;
    if versions != known || versions.last().map(String::as_str) != Some(VERSION) {
        return Err(ManagementError::Schema);
    }
    Ok(versions)
}

mod postgres;
