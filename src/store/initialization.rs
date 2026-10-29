//! Ordinary open is read-only for every nonempty schema. Upgrades are explicit.
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, Statement, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;
#[cfg(test)]
#[path = "initialization/document_tests.rs"]
mod document_tests;
pub mod history;
mod maintenance;
pub(crate) mod postgres;
pub(crate) mod read_context;
mod upgrade;
pub(crate) use maintenance::TransactionConnection;
pub(crate) mod import_requests;

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
    super::pinning::management::schema::validate_installation_catalog(db)
        .await
        .map_err(|_| required())
}

pub async fn open(db: &DatabaseConnection) -> Result<(), DbErr> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return postgres::open(db).await;
    }
    match classify(db).await? {
        SchemaState::Empty => initialize_empty(db).await,
        SchemaState::Current => validate_catalog(db).await,
        SchemaState::UpgradeRequired => Err(required()),
    }
}

pub async fn initialize_empty(db: &DatabaseConnection) -> Result<(), DbErr> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return postgres::open(db).await;
    }
    match classify(db).await? {
        SchemaState::Current => return validate_catalog(db).await,
        SchemaState::UpgradeRequired => return Err(required()),
        SchemaState::Empty => {}
    }
    let tx = db.begin().await?;
    // DDL obtains the caller-owned writer boundary; no read snapshot precedes it.
    tx.execute_unprepared("CREATE TABLE IF NOT EXISTS _ipfs_s3_migration_lock (id INTEGER PRIMARY KEY NOT NULL CHECK(id=1))").await?;
    tx.execute_unprepared("INSERT OR IGNORE INTO _ipfs_s3_migration_lock VALUES(1)")
        .await?;
    tx.execute_unprepared("UPDATE _ipfs_s3_migration_lock SET id=id WHERE 1=0")
        .await?;
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
    use super::migrations::history as legacy;
    use super::pinning::management::schema;
    if !matches!(
        request.source_version,
        legacy::M13 | legacy::M17 | legacy::M23
    ) || request.target_version != schema::VERSION
    {
        return Err(required());
    }
    let owned = maintenance::Request::from(&request);
    let backfill = maintenance::run(db, move |tx| {
        Box::pin(async move { prepare_upgrade(tx, owned.borrowed()).await })
    })
    .await?;
    if backfill {
        upgrade::backfill(db, &request).await?;
    }
    Ok(())
}

async fn prepare_upgrade(
    tx: &impl TransactionConnection,
    request: UpgradeRequest<'_>,
) -> Result<bool, DbErr> {
    use super::migrations::history as legacy;
    use super::pinning::management::{catalog, native, schema};
    native::scope::writer_boundary_in(tx)
        .await
        .map_err(|_| required())?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
            .await?;
    }
    let (scope, identity) = native::scope::binding(tx).await.map_err(|_| required())?;
    if scope != request.database_scope
        || identity != request.native_identity
        || identity.starts_with("diagnostic:")
    {
        return Err(required());
    }
    let versions: Vec<String> = tx
        .query_all(Statement::from_string(
            tx.get_database_backend(),
            "SELECT CASE WHEN length(version)<=128 THEN version END AS version FROM seaql_migrations ORDER BY version LIMIT 65",
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
    let mut needs_backfill = false;
    if preparing {
        let old_final = legacy::is_m23(&versions);
        let source = legacy::prefix(request.source_version).ok_or_else(required)?;
        if !old_final
            && !versions
                .iter()
                .map(String::as_str)
                .eq(source.iter().copied())
        {
            return Err(required());
        }
        let old_catalog = native::archived_catalog(&versions).map_err(|_| required())?;
        catalog::validate(tx, old_catalog)
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
            catalog::validate(tx, old_catalog)
                .await
                .map_err(|_| required())?;
        }
        let (scope, identity) = native::scope::binding(tx).await.map_err(|_| required())?;
        if scope != request.database_scope || identity != request.native_identity {
            return Err(required());
        }
        if old_final {
            let state: String = tx
                .query_one(Statement::from_string(
                    tx.get_database_backend(),
                    "SELECT state FROM pin_management_readiness WHERE id=1 AND format='S75A1'",
                ))
                .await?
                .ok_or_else(required)?
                .try_get("", "state")?;
            if !matches!(state.as_str(), "ready" | "backfill") {
                return Err(required());
            }
            needs_backfill = state == "backfill";
            schema::validate_m23_layout(tx, needs_backfill)
                .await
                .map_err(|_| required())?;
            if request.source_version == legacy::M23 {
                if needs_backfill {
                    return Err(required());
                }
            } else {
                // The old target is immutable evidence, not an alias of the new one.
                history::upgrade_stamp(
                    tx,
                    &UpgradeRequest {
                        target_version: legacy::M23,
                        ..request
                    },
                    false,
                )
                .await
                .map_err(|_| required())?;
            }
        } else {
            if request.source_version == legacy::M13 {
                schema::storage::install(tx).await?;
            }
            import_requests::migrate(tx).await?;
            schema::storage::install_inline(tx).await?;
            tx.execute_unprepared(
                "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
            )
            .await?;
            needs_backfill = true;
        }
        tx.execute_unprepared("DELETE FROM seaql_migrations")
            .await?;
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "INSERT INTO seaql_migrations(version,applied_at) VALUES ($1,$2)",
            [
                schema::VERSION.into(),
                chrono::Utc::now().timestamp().into(),
            ],
        ))
        .await?;
    }
    history::upgrade_stamp(tx, &request, preparing)
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
        validate_existing(tx).await?;
        return Ok(false);
    }
    if state != "backfill" {
        return Err(required());
    }
    if preparing && !needs_backfill {
        return Err(required());
    }
    // Inline installation does not replace existing storage guards. Validate
    // both retained source storage and the complete prepared layout before any
    // preparation writes (including the ledger and stamp) can commit.
    schema::validate_for_upgrade(tx)
        .await
        .map_err(|_| required())?;
    Ok(true)
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
        let count = crate::store::migrations::Historical::migrations()
            .iter()
            .position(|migration| migration.name() == "m20261017_000001_management_storage")
            .unwrap() as u32;
        crate::store::migrations::Historical::up(&db, Some(count))
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
