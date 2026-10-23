//! Ordinary open is read-only for every nonempty schema. Upgrades are explicit.
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;
pub mod history;
pub(crate) mod read_context;
mod upgrade;
#[cfg(test)]
#[path = "initialization/document_tests.rs"]
mod document_tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaState {
    Empty,
    Current,
    UpgradeRequired,
}
fn required() -> DbErr {
    DbErr::Migration("management schema upgrade required; ordinary startup is read-only".into())
}

async fn names<C: ConnectionTrait>(db: &C) -> Result<Vec<String>, DbErr> {
    let sql = match db.get_database_backend() {
        DatabaseBackend::Sqlite => {
            "SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
        }
        DatabaseBackend::Postgres => {
            "SELECT c.relname AS name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','f') ORDER BY c.relname"
        }
        _ => return Err(required()),
    };
    db.query_all(Statement::from_string(db.get_database_backend(), sql))
        .await?
        .iter()
        .map(|r| r.try_get("", "name"))
        .collect()
}

pub async fn classify<C: ConnectionTrait>(db: &C) -> Result<SchemaState, DbErr> {
    let tables = names(db).await?;
    if tables.is_empty() {
        return Ok(SchemaState::Empty);
    }
    if !tables.iter().any(|t| t == "pin_management_readiness") {
        return Ok(SchemaState::UpgradeRequired);
    }
    if super::pinning::management::schema::validate(db)
        .await
        .is_err()
    {
        return Ok(SchemaState::UpgradeRequired);
    }
    Ok(SchemaState::Current)
}

pub async fn validate_existing<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    if classify(db).await? != SchemaState::Current {
        return Err(required());
    }
    validate_catalog(db).await
}

// Called immediately after this operation validates the current storage schema.
// No installed-schema result is retained across operations or a DDL boundary.
async fn validate_catalog<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    super::pinning::management::catalog::validate(
        db,
        super::pinning::management::native::current_catalog(),
    )
    .await
    .map_err(|_| required())
}

pub async fn open(db: &DatabaseConnection) -> Result<(), DbErr> {
    match classify(db).await? {
        SchemaState::Empty => initialize_empty(db).await,
        SchemaState::Current => validate_catalog(db).await,
        SchemaState::UpgradeRequired => Err(required()),
    }
}

pub async fn initialize_empty(db: &DatabaseConnection) -> Result<(), DbErr> {
    match classify(db).await? {
        SchemaState::Current => return validate_catalog(db).await,
        SchemaState::UpgradeRequired => return Err(required()),
        SchemaState::Empty => {}
    }
    let tx = db.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
            .await?;
    } else {
        // DDL obtains the caller-owned writer boundary; no read snapshot precedes it.
        tx.execute_unprepared("CREATE TABLE IF NOT EXISTS _ipfs_s3_migration_lock (id INTEGER PRIMARY KEY NOT NULL CHECK(id=1))").await?;
        tx.execute_unprepared("INSERT OR IGNORE INTO _ipfs_s3_migration_lock VALUES(1)")
            .await?;
        tx.execute_unprepared("UPDATE _ipfs_s3_migration_lock SET id=id WHERE 1=0")
            .await?;
    }
    let remaining = names(&tx)
        .await?
        .into_iter()
        .filter(|n| n != "_ipfs_s3_migration_lock")
        .collect::<Vec<_>>();
    if !remaining.is_empty() {
        validate_existing(&tx).await?;
    } else {
        super::migrator::Migrator::up(&tx, None).await?;
        validate_existing(&tx).await?;
    }
    tx.commit().await
}

pub struct UpgradeRequest<'a> {
    pub native_identity: &'a str,
    pub database_scope: &'a str,
    pub source_version: &'a str,
    pub target_version: &'a str,
}

/// Explicit local maintenance entry, never called by AppState. Preparation and
/// each original are separate transactions. A failed backfill remains unavailable
/// and can only be resumed by another identity-bound explicit upgrade request.
pub async fn upgrade_existing(
    db: &DatabaseConnection,
    request: UpgradeRequest<'_>,
) -> Result<(), DbErr> {
    use super::pinning::management::{catalog, native, schema};
    if !matches!(
        request.source_version,
        "m20261013_000001_zip_root_logical_release" | "m20261017_000001_management_storage"
    ) || request.target_version != schema::VERSION
    {
        return Err(required());
    }
    let tx = db.begin().await?;
    native::scope::writer_boundary(&tx)
        .await
        .map_err(|_| required())?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
            .await?;
    }
    let (scope, identity) = native::scope::binding(&tx).await.map_err(|_| required())?;
    if scope != request.database_scope
        || identity != request.native_identity
        || identity.starts_with("diagnostic:")
    {
        return Err(required());
    }
    let versions: Vec<String> = tx
        .query_all(Statement::from_string(
            tx.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await?
        .iter()
        .map(|r| r.try_get("", "version"))
        .collect::<Result<_, _>>()?;
    let known = super::migrator::Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect::<Vec<_>>();
    let preparing = versions != known;
    if preparing {
        let end = known
            .iter()
            .position(|v| v == request.source_version)
            .ok_or_else(required)?
            + 1;
        if versions != known[..end] {
            return Err(required());
        }
        let old_catalog = native::archived_catalog(&versions).map_err(|_| required())?;
        catalog::validate(&tx, old_catalog)
            .await
            .map_err(|_| required())?;
        if tx.get_database_backend() == DatabaseBackend::Postgres {
            // The target registry may contain tables not present in this source.
            let tables = old_catalog
                .iter()
                .map(|t| format!("\"{}\"", t.table))
                .collect::<Vec<_>>()
                .join(",");
            tx.execute_unprepared(&format!(
                "LOCK TABLE {tables},seaql_migrations IN ACCESS EXCLUSIVE MODE NOWAIT"
            ))
            .await?;
            catalog::validate(&tx, old_catalog)
                .await
                .map_err(|_| required())?;
        }
        let (scope, identity) = native::scope::binding(&tx).await.map_err(|_| required())?;
        if scope != request.database_scope || identity != request.native_identity {
            return Err(required());
        }
        super::migrator::Migrator::up(&tx, None).await?;
        tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
            .await?;
    }
    history::upgrade_stamp(&tx, &request, preparing)
        .await
        .map_err(|_| required())?;
    let state: String = tx
        .query_one(Statement::from_string(
            tx.get_database_backend(),
            "SELECT state FROM pin_management_readiness WHERE id=1 AND format='S75A1'",
        ))
        .await?
        .ok_or_else(required)?
        .try_get("", "state")?;
    if state == "ready" {
        validate_existing(&tx).await?;
        return tx.commit().await;
    }
    if state != "backfill" {
        return Err(required());
    }
    tx.commit().await?;
    upgrade::backfill(db, &request).await
}

#[cfg(test)]
mod stage75_storage_tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_open_rechecks_schema_after_ddl_without_writing() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        initialize_empty(&db).await.unwrap();
        let changes =
            || Statement::from_string(db.get_database_backend(), "SELECT total_changes() AS n");
        let before: i64 = db
            .query_one(changes())
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        open(&db).await.unwrap();
        let after: i64 = db
            .query_one(changes())
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(before, after);
        db.execute_unprepared("DROP TRIGGER s75_inline_no_update")
            .await
            .unwrap();
        assert!(open(&db).await.is_err());
        let refused: i64 = db
            .query_one(changes())
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(refused, before);
    }

    #[tokio::test]
    async fn stage7_startup_does_not_write_migration_lock_or_identity() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let count = crate::store::migrator::Migrator::migrations()
            .iter()
            .position(|migration| migration.name() == "m20261017_000001_management_storage")
            .unwrap() as u32;
        crate::store::migrator::Migrator::up(&db, Some(count))
            .await
            .unwrap();
        let before = crate::store::pinning::management::native::scope::binding(&db)
            .await
            .unwrap();
        assert_eq!(classify(&db).await.unwrap(), SchemaState::UpgradeRequired);
        assert!(crate::store::run_migrations(&db).await.is_err());
        assert!(
            !names(&db)
                .await
                .unwrap()
                .contains(&"_ipfs_s3_migration_lock".to_owned())
        );
        assert!(
            !names(&db)
                .await
                .unwrap()
                .contains(&"pin_management_readiness".to_owned())
        );
        assert_eq!(
            crate::store::pinning::management::native::scope::binding(&db)
                .await
                .unwrap(),
            before
        );
    }
}
