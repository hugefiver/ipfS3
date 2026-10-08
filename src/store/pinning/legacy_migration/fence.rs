//! Maintenance assertions, DB-native time fence and fixed lock order.
use super::{Confirmation, Declaration, TableImage, model::field, refused, snapshot};
use crate::error::AppResult;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

pub(super) fn maintenance(decl: &Declaration) -> AppResult<()> {
    if !decl.maintenance.all_writers_stopped || !decl.maintenance.all_workers_drained {
        return Err(refused(
            "all same-database writers must be stopped and all workers drained",
        ));
    }
    Ok(())
}

pub(super) fn drained(images: &[TableImage]) -> AppResult<()> {
    for image in images {
        for row in &image.rows {
            let active = match image.table.as_str() {
                "pin_jobs" => field(row, "state") == Some("running") && !parked(row, images),
                "pin_submit_observations" => {
                    field(row, "outcome") == Some("in_flight")
                        && images
                            .iter()
                            .filter(|i| i.table == "pin_jobs")
                            .flat_map(|i| &i.rows)
                            .any(|j| field(j, "id") == field(row, "job_id") && !parked(j, images))
                }
                "import_jobs" => {
                    field(row, "locked_by").is_some()
                        && !matches!(
                            field(row, "state"),
                            Some("completed" | "failed" | "cancelled")
                        )
                }
                "zip_v2_executions" => {
                    field(row, "worker").is_some()
                        && matches!(field(row, "state"), Some("pending" | "admitted"))
                }
                "standard_mutation_leases" => true,
                "lifecycle_actions" => field(row, "state") == Some("running"),
                _ => false,
            };
            if active {
                return Err(refused(
                    "worker/writer execution has not drained; claim expiry is not proof",
                ));
            }
        }
    }
    Ok(())
}

fn parked(row: &super::NativeRow, images: &[TableImage]) -> bool {
    field(row, "locked_until").is_none()
        && (field(row, "last_error") == Some("historical identity unavailable; needs_attention")
            || images
                .iter()
                .filter(|i| i.table == "pin_submit_history")
                .flat_map(|i| &i.rows)
                .any(|h| {
                    field(h, "job_id") == field(row, "id")
                        && field(h, "state") == Some("needs_attention")
                }))
}

pub(super) async fn validate_plan<C: ConnectionTrait>(
    db: &C,
    plan: &Confirmation,
    schema: &[String],
) -> AppResult<()> {
    if plan.version != 1
        || schema != plan.schema
        || snapshot::identity(db).await? != plan.database_identity
        || plan.operation_id.is_empty()
        || plan.operation_id.len() > 128
    {
        return Err(refused("confirmation scope/schema differs"));
    }
    let sql = if db.get_database_backend() == DatabaseBackend::Postgres {
        "SELECT 1 WHERE $1::timestamptz <= clock_timestamp() AND clock_timestamp() < $2::timestamptz AND $2::timestamptz <= $1::timestamptz + ($3::bigint * interval '1 second')"
    } else {
        "SELECT 1 WHERE julianday($1)<=julianday('now') AND julianday('now')<julianday($2) AND julianday($2)=julianday($1,'+' || $3 || ' seconds')"
    };
    if db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            [
                plan.db_clock.clone().into(),
                plan.expires_at.clone().into(),
                i64::from(plan.declaration.ttl_seconds).into(),
            ],
        ))
        .await?
        .is_none()
    {
        return Err(refused("confirmation DB-clock TTL expired or invalid"));
    }
    Ok(())
}

pub(super) async fn lock<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        // Write intent before any business read; no receipt row is mutated.
        db.execute_unprepared("UPDATE pin_legacy_migrations SET id=id WHERE 1=0")
            .await?;
        return Ok(());
    }
    db.execute_unprepared("SET LOCAL lock_timeout='1s'").await?;
    for (table, order) in [
        ("buckets", "name"),
        ("objects", "id"),
        ("object_versions", "id"),
        ("pin_leases", "id"),
        ("pin_lease_targets", "created_at,id"),
        ("remote_pins", "provider,cid"),
        ("pin_provider_usage", "provider"),
        ("pin_provider_routes", "provider"),
    ] {
        row_lock(db, table, order).await?;
    }
    let tables = snapshot::tables();
    for table in tables.iter().skip(8) {
        row_lock(db, &table.table, &table.primary_key.join(",")).await?;
    }
    // Tail write-excluding table locks fence insert phantoms, even empty captures.
    let names = tables
        .iter()
        .map(|t| t.table.as_str())
        .chain([
            "pin_legacy_migrations",
            "pin_legacy_migration_items",
            "seaql_migrations",
        ])
        .collect::<Vec<_>>()
        .join(",");
    db.execute_unprepared(&format!("LOCK TABLE {names} IN EXCLUSIVE MODE NOWAIT"))
        .await?;
    Ok(())
}

async fn row_lock<C: ConnectionTrait>(db: &C, table: &str, order: &str) -> AppResult<()> {
    let rows = db
        .query_all(Statement::from_string(
            db.get_database_backend(),
            format!(
                "SELECT 1 FROM {table} ORDER BY {order} LIMIT {} FOR UPDATE NOWAIT",
                super::MAX_ROWS + 1
            ),
        ))
        .await?;
    if rows.len() > super::MAX_ROWS {
        return Err(refused("lock frontier exceeds row budget"));
    }
    Ok(())
}
