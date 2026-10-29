//! Final empty-database layout. Existing layouts use explicit source adapters.
use crate::store::pinning::management::schema;
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};
mod guards;

pub const VERSION: &str = "m20261027_000001_schema_baseline";

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let pg = match db.get_database_backend() {
        DatabaseBackend::Postgres => true,
        DatabaseBackend::Sqlite => false,
        _ => return Err(DbErr::Migration("unsupported baseline database".into())),
    };
    for sql in [
        include_str!("baseline/core.sql"),
        include_str!("baseline/pinning.sql"),
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
            .replace("@SEQUENCE@", if pg { "BIGSERIAL NOT NULL PRIMARY KEY" } else { "INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT" })
            .replace("@HEX@", if pg { "~ '^[0-9a-f]{64}$'" } else { "NOT GLOB '*[^0-9a-f]*'" });
        db.execute_unprepared(&sql).await?;
    }
    guards::install(db).await?;
    super::legacy_receipts::install(db).await?;
    db.execute_unprepared("INSERT INTO residency_backfill(id,claim_epoch,completed,updated_at) VALUES ('hot_verification',0,FALSE,CURRENT_TIMESTAMP)").await?;
    db.execute_unprepared(&schema::records_ddl(db.get_database_backend()))
        .await?;
    db.execute_unprepared(&schema::retirements_ddl(db.get_database_backend()))
        .await?;
    let now = crate::store::database_clock::database_now(db)
        .await
        .map_err(|e| DbErr::Migration(e.to_string()))?;
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES ($1,'schema','identity','database_scope','','','{}','{}',NULL,'[]','[]','identity',$2,NULL,NULL)",
        [uuid::Uuid::new_v4().to_string().into(), now.into()])).await?;
    schema::storage::install_fresh(db).await
}
