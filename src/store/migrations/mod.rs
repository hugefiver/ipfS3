use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

mod baseline;
mod history;
mod remove_legacy;

#[cfg(any(test, feature = "historical-fixtures"))]
pub(crate) mod bridge;
#[cfg(any(test, feature = "historical-fixtures"))]
#[path = "../../../tests/support/historical_migrations/mod.rs"]
mod fixtures;
#[cfg(any(test, feature = "historical-fixtures"))]
pub use fixtures::*;

const FILE_PIN_VERSION: &str = "m20261120_000001_file_pins";
const BUCKET_ACL_VERSION: &str = "m20261121_000001_bucket_acl";
pub const VERSION: &str = "m20261122_000001_bucket_quota";
pub(super) const CREATE_LEDGER: &str = "CREATE TABLE IF NOT EXISTS seaql_migrations (version VARCHAR NOT NULL PRIMARY KEY, applied_at BIGINT NOT NULL)";

async fn index_file_pins<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let revision = match db.get_database_backend() {
        DatabaseBackend::Sqlite => "json_extract(pin_decision,'$.control_revision')",
        DatabaseBackend::Postgres => "(pin_decision::jsonb->>'control_revision')",
        _ => return Err(DbErr::Migration("unsupported database".into())),
    };
    db.execute_unprepared(&format!("CREATE UNIQUE INDEX object_versions_pin_revision ON object_versions({revision}) WHERE pin_decision IS NOT NULL")).await?;
    Ok(())
}

async fn install_bucket_acl<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute_unprepared("ALTER TABLE buckets ADD COLUMN canned_acl TEXT CHECK (canned_acl IN ('private', 'public-read', 'public-read-write', 'authenticated-read'))").await?;
    Ok(())
}

async fn install_bucket_quota<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let integer_check = if db.get_database_backend() == DatabaseBackend::Sqlite {
        " AND typeof(quota_bytes)='integer'"
    } else {
        ""
    };
    db.execute_unprepared(&format!(
        "ALTER TABLE buckets ADD COLUMN quota_bytes BIGINT CHECK (quota_bytes IS NULL OR (quota_bytes>=0{integer_check}));
         ALTER TABLE buckets ADD COLUMN used_bytes BIGINT NOT NULL DEFAULT 0"
    ))
    .await?;
    Ok(())
}

async fn record_current<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO seaql_migrations(version,applied_at) VALUES ($1,$2)",
        [VERSION.into(), chrono::Utc::now().timestamp().into()],
    ))
    .await?;
    Ok(())
}

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    baseline::install(db).await?;
    index_file_pins(db).await?;
    install_bucket_acl(db).await?;
    install_bucket_quota(db).await?;
    record_current(db).await
}

pub(super) async fn upgrade<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            "SELECT version FROM seaql_migrations ORDER BY version",
        ))
        .await?;
    let versions: Vec<String> = rows
        .iter()
        .map(|r| r.try_get("", "version"))
        .collect::<Result<_, _>>()?;
    let version = versions.last().map(String::as_str);
    match version {
        Some(VERSION) => return Ok(()),
        Some(FILE_PIN_VERSION | BUCKET_ACL_VERSION) => {}
        _ if history::supported(&versions) => {
            remove_legacy::run(db).await?;
            db.execute_unprepared("ALTER TABLE object_versions ADD COLUMN pin_decision TEXT")
                .await?;
            db.execute_unprepared(baseline::FILE_PINS).await?;
            index_file_pins(db).await?;
        }
        _ => {
            return Err(DbErr::Migration(
                "unsupported source schema: upgrade the previous gateway layout to m20261108_000001_current_pinning_config or later before the file pin upgrade".into(),
            ));
        }
    }
    if version != Some(BUCKET_ACL_VERSION) {
        install_bucket_acl(db).await?;
    }
    install_bucket_quota(db).await?;
    db.execute_unprepared(
        "UPDATE buckets SET used_bytes=totals.used_bytes
         FROM (SELECT v.bucket, CAST(SUM(o.size) AS BIGINT) AS used_bytes
               FROM object_versions v JOIN objects o ON o.id=v.object_id
               WHERE v.kind='object' GROUP BY v.bucket) AS totals
         WHERE buckets.name=totals.bucket",
    )
    .await?;
    record_current(db).await
}
