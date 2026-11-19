use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr};
mod guards;

pub(super) const FILE_PINS: &str = include_str!("../pinning/resources/schema.sql");

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let pg = match db.get_database_backend() {
        DatabaseBackend::Postgres => true,
        DatabaseBackend::Sqlite => false,
        _ => return Err(DbErr::Migration("unsupported database".into())),
    };
    for sql in [
        include_str!("baseline/core.sql"),
        include_str!("baseline/import.sql"),
        include_str!("baseline/lifecycle.sql"),
        include_str!("baseline/zip.sql"),
    ] {
        let sql = sql
            .replace("@TIME@", if pg { "TIMESTAMPTZ" } else { "TIMESTAMP" })
            .replace("@TZ@", if pg { "TIMESTAMPTZ" } else { "timestamp_with_timezone_text" })
            .replace("@JSON@", if pg { "JSONB" } else { "TEXT" })
            .replace("@LIFECYCLE_JSON@", "TEXT")
            .replace("@JSON_CHECK@", if pg { "CONSTRAINT ck_bucket_lifecycle_configs_canonical_json CHECK(canonical_json IS NULL OR pg_input_is_valid(canonical_json, 'jsonb'))," } else { "CONSTRAINT ck_bucket_lifecycle_configs_canonical_json CHECK(canonical_json IS NULL OR json_valid(canonical_json))," })
            .replace("@LEASE_TIME@", if pg { "TIMESTAMPTZ" } else { "TEXT" })
            .replace("@RELEASE_TIME@", if pg { "TIMESTAMPTZ" } else { "TEXT" })
            .replace("@HEX@", if pg { "~ '^[0-9a-f]{64}$'" } else { "NOT GLOB '*[^0-9a-f]*'" });
        db.execute_unprepared(&sql).await?;
    }
    guards::install(db).await?;
    db.execute_unprepared("INSERT INTO residency_backfill(id,claim_epoch,completed,updated_at) VALUES ('hot_verification',0,FALSE,CURRENT_TIMESTAMP)").await?;
    db.execute_unprepared(FILE_PINS).await?;
    Ok(())
}
