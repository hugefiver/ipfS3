use chrono::Duration;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection,
    DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect, Statement, TransactionError,
    TransactionTrait, sea_query::Expr,
};

use crate::{
    error::{AppError, AppResult},
    residency::{
        ClaimedResidencyBackfill, MAX_RESIDENCY_BACKFILL_PAGE_SIZE, PendingHotResidency,
        PendingHotResidencyPage, ResidencyBackfillCursor, VersionResidencyIdentity,
    },
    store::{database_clock::database_now, entities::residency_backfill},
};

const BACKFILL_ID: &str = "hot_verification";
const MAX_BACKFILL_LEASE_SECONDS: i64 = 3_600;
const MAX_SQLITE_CLAIM_RETRIES: usize = 4;

pub async fn claim_residency_backfill(
    db: &DatabaseConnection,
    worker_id: &str,
    lease_for: Duration,
) -> AppResult<Option<ClaimedResidencyBackfill>> {
    if worker_id.is_empty() {
        return Err(AppError::InvalidArgument(
            "residency backfill worker ID must not be empty".to_owned(),
        ));
    }
    if lease_for <= Duration::zero() || lease_for > Duration::seconds(MAX_BACKFILL_LEASE_SECONDS) {
        return Err(AppError::InvalidArgument(format!(
            "residency backfill lease must be between 1 second and {MAX_BACKFILL_LEASE_SECONDS} seconds"
        )));
    }
    for attempt in 0..=MAX_SQLITE_CLAIM_RETRIES {
        let worker_id = worker_id.to_owned();
        let result = db
            .transaction(move |txn| {
                let worker_id = worker_id.clone();
                Box::pin(async move {
                    claim_residency_backfill_in_transaction(txn, &worker_id, lease_for).await
                })
            })
            .await;
        match result {
            Ok(claim) => return Ok(claim),
            Err(error)
                if db.get_database_backend() == DatabaseBackend::Sqlite
                    && sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_CLAIM_RETRIES =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt as u64 + 1)))
                    .await;
            }
            Err(error) => {
                return Err(match error {
                    TransactionError::Transaction(error) => error,
                    TransactionError::Connection(error) => error.into(),
                });
            }
        }
    }
    unreachable!("SQLite residency claim retry loop always returns or errors")
}

fn sqlite_contention(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database table is locked")
        || message.contains("database is busy")
        || message.contains("busy snapshot")
}

async fn claim_residency_backfill_in_transaction(
    txn: &DatabaseTransaction,
    worker_id: &str,
    lease_for: Duration,
) -> AppResult<Option<ClaimedResidencyBackfill>> {
    let now = database_now(txn).await?;
    let mut query = residency_backfill::Entity::find_by_id(BACKFILL_ID);
    if txn.get_database_backend() == DatabaseBackend::Postgres {
        query = query.lock_exclusive();
    }
    let row = query
        .one(txn)
        .await?
        .ok_or_else(|| AppError::Internal("residency backfill state is missing".to_owned()))?;
    if row.lease_until.is_some_and(|until| until > now) {
        return Ok(None);
    }
    let claim_epoch = row
        .claim_epoch
        .checked_add(1)
        .ok_or_else(|| AppError::Database("residency backfill epoch overflow".to_owned()))?;
    let lease_until = now.checked_add_signed(lease_for).ok_or_else(|| {
        AppError::InvalidArgument(
            "residency backfill lease is outside the database timestamp range".to_owned(),
        )
    })?;
    let updated = residency_backfill::Entity::update_many()
        .col_expr(
            residency_backfill::Column::ClaimEpoch,
            Expr::value(claim_epoch),
        )
        .col_expr(
            residency_backfill::Column::LeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .col_expr(
            residency_backfill::Column::ClaimedBy,
            Expr::value(Some(worker_id.to_owned())),
        )
        .col_expr(residency_backfill::Column::Completed, Expr::value(false))
        .col_expr(residency_backfill::Column::UpdatedAt, Expr::value(now))
        .filter(residency_backfill::Column::Id.eq(BACKFILL_ID))
        .filter(residency_backfill::Column::ClaimEpoch.eq(row.claim_epoch))
        .filter(
            Condition::any()
                .add(residency_backfill::Column::LeaseUntil.is_null())
                .add(residency_backfill::Column::LeaseUntil.lte(now)),
        )
        .exec(txn)
        .await?;
    if updated.rows_affected != 1 {
        return Ok(None);
    }
    Ok(Some(ClaimedResidencyBackfill {
        worker_id: worker_id.to_owned(),
        claim_epoch,
        cursor: if row.completed {
            None
        } else {
            row.cursor_version_row_id
                .map(|version_row_id| ResidencyBackfillCursor { version_row_id })
        },
        lease_until,
    }))
}

pub async fn pending_hot_residency_page<C: ConnectionTrait>(
    db: &C,
    cursor: Option<&ResidencyBackfillCursor>,
    limit: u64,
) -> AppResult<PendingHotResidencyPage> {
    if limit == 0 || limit > MAX_RESIDENCY_BACKFILL_PAGE_SIZE {
        return Err(AppError::InvalidArgument(format!(
            "residency backfill page size must be between 1 and {MAX_RESIDENCY_BACKFILL_PAGE_SIZE}"
        )));
    }
    if cursor.is_some_and(|cursor| cursor.version_row_id.is_empty()) {
        return Err(AppError::InvalidArgument(
            "invalid residency backfill cursor".to_owned(),
        ));
    }
    let backend = db.get_database_backend();
    let (sql, values) = match (backend, cursor) {
        (DatabaseBackend::Postgres, Some(cursor)) => (
            "SELECT version.version_row_id, version.object_id, version.cid \
             FROM version_residencies version \
             JOIN physical_residencies physical \
               ON physical.tier = version.primary_tier AND physical.cid = version.cid \
             JOIN object_versions owner \
               ON owner.id = version.version_row_id AND owner.object_id = version.object_id \
             JOIN objects object ON object.id = version.object_id AND object.cid = version.cid \
             WHERE version.primary_tier = 'hot' AND version.storage_class = 'STANDARD' \
               AND physical.verification_state IN ('pending', 'failed') \
               AND version.version_row_id > $1 \
             ORDER BY version.version_row_id ASC LIMIT $2",
            vec![cursor.version_row_id.clone().into(), (limit as i64).into()],
        ),
        (DatabaseBackend::Sqlite, Some(cursor)) => (
            "SELECT version.version_row_id, version.object_id, version.cid \
             FROM version_residencies version \
             JOIN physical_residencies physical \
               ON physical.tier = version.primary_tier AND physical.cid = version.cid \
             JOIN object_versions owner \
               ON owner.id = version.version_row_id AND owner.object_id = version.object_id \
             JOIN objects object ON object.id = version.object_id AND object.cid = version.cid \
             WHERE version.primary_tier = 'hot' AND version.storage_class = 'STANDARD' \
               AND physical.verification_state IN ('pending', 'failed') \
               AND version.version_row_id > ? \
             ORDER BY version.version_row_id ASC LIMIT ?",
            vec![cursor.version_row_id.clone().into(), (limit as i64).into()],
        ),
        (DatabaseBackend::Postgres, None) => (
            "SELECT version.version_row_id, version.object_id, version.cid \
             FROM version_residencies version \
             JOIN physical_residencies physical \
               ON physical.tier = version.primary_tier AND physical.cid = version.cid \
             JOIN object_versions owner \
               ON owner.id = version.version_row_id AND owner.object_id = version.object_id \
             JOIN objects object ON object.id = version.object_id AND object.cid = version.cid \
             WHERE version.primary_tier = 'hot' AND version.storage_class = 'STANDARD' \
               AND physical.verification_state IN ('pending', 'failed') \
             ORDER BY version.version_row_id ASC LIMIT $1",
            vec![(limit as i64).into()],
        ),
        (DatabaseBackend::Sqlite, None) => (
            "SELECT version.version_row_id, version.object_id, version.cid \
             FROM version_residencies version \
             JOIN physical_residencies physical \
               ON physical.tier = version.primary_tier AND physical.cid = version.cid \
             JOIN object_versions owner \
               ON owner.id = version.version_row_id AND owner.object_id = version.object_id \
             JOIN objects object ON object.id = version.object_id AND object.cid = version.cid \
             WHERE version.primary_tier = 'hot' AND version.storage_class = 'STANDARD' \
               AND physical.verification_state IN ('pending', 'failed') \
             ORDER BY version.version_row_id ASC LIMIT ?",
            vec![(limit as i64).into()],
        ),
        (DatabaseBackend::MySql, _) => {
            return Err(AppError::Internal(
                "residency backfill requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let rows = db
        .query_all(Statement::from_sql_and_values(backend, sql, values))
        .await?;
    let items = rows
        .into_iter()
        .map(|row| {
            Ok(PendingHotResidency {
                identity: VersionResidencyIdentity::new(
                    row.try_get::<String>("", "version_row_id")?,
                    row.try_get::<String>("", "object_id")?,
                    row.try_get::<String>("", "cid")?,
                ),
            })
        })
        .collect::<Result<Vec<_>, sea_orm::DbErr>>()?;
    let complete = items.len() < limit as usize;
    let next_cursor = if complete {
        None
    } else {
        items.last().map(|item| ResidencyBackfillCursor {
            version_row_id: item.identity.version_row_id.clone(),
        })
    };
    Ok(PendingHotResidencyPage {
        items,
        next_cursor,
        complete,
    })
}

pub async fn checkpoint_residency_backfill_in_transaction(
    txn: &DatabaseTransaction,
    claim: &ClaimedResidencyBackfill,
    next_cursor: Option<&ResidencyBackfillCursor>,
    complete: bool,
) -> AppResult<bool> {
    if claim.worker_id.is_empty()
        || claim.claim_epoch <= 0
        || (!complete && next_cursor.is_none_or(|cursor| cursor.version_row_id.is_empty()))
    {
        return Err(AppError::InvalidArgument(
            "invalid residency backfill checkpoint".to_owned(),
        ));
    }
    let now = database_now(txn).await?;
    let active_lease = match txn.get_database_backend() {
        DatabaseBackend::Postgres => "\"lease_until\" > clock_timestamp()",
        DatabaseBackend::Sqlite => "julianday(\"lease_until\") > julianday('now')",
        DatabaseBackend::MySql => {
            return Err(AppError::Internal(
                "residency backfill requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let updated = residency_backfill::Entity::update_many()
        .col_expr(
            residency_backfill::Column::CursorVersionRowId,
            Expr::value(if complete {
                None
            } else {
                next_cursor.map(|cursor| cursor.version_row_id.clone())
            }),
        )
        .col_expr(residency_backfill::Column::Completed, Expr::value(complete))
        .col_expr(
            residency_backfill::Column::LeaseUntil,
            Expr::value(Option::<chrono::DateTime<chrono::Utc>>::None),
        )
        .col_expr(
            residency_backfill::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(residency_backfill::Column::UpdatedAt, Expr::value(now))
        .filter(residency_backfill::Column::Id.eq(BACKFILL_ID))
        .filter(residency_backfill::Column::ClaimEpoch.eq(claim.claim_epoch))
        .filter(residency_backfill::Column::ClaimedBy.eq(&claim.worker_id))
        .filter(residency_backfill::Column::Completed.eq(false))
        .filter(Expr::cust(active_lease))
        .exec(txn)
        .await?;
    Ok(updated.rows_affected == 1)
}

/// Releases an active claim without moving its stable cursor.
///
/// This is used when runtime cancellation interrupts external verification IO.
/// The epoch and database-clock lease predicates prevent a stale worker from
/// releasing a replacement worker's claim.
pub async fn release_residency_backfill_in_transaction(
    txn: &DatabaseTransaction,
    claim: &ClaimedResidencyBackfill,
) -> AppResult<bool> {
    if claim.worker_id.is_empty() || claim.claim_epoch <= 0 {
        return Err(AppError::InvalidArgument(
            "invalid residency backfill release".to_owned(),
        ));
    }
    let now = database_now(txn).await?;
    let active_lease = match txn.get_database_backend() {
        DatabaseBackend::Postgres => "\"lease_until\" > clock_timestamp()",
        DatabaseBackend::Sqlite => "julianday(\"lease_until\") > julianday('now')",
        DatabaseBackend::MySql => {
            return Err(AppError::Internal(
                "residency backfill requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let updated = residency_backfill::Entity::update_many()
        .col_expr(
            residency_backfill::Column::LeaseUntil,
            Expr::value(Option::<chrono::DateTime<chrono::Utc>>::None),
        )
        .col_expr(
            residency_backfill::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(residency_backfill::Column::UpdatedAt, Expr::value(now))
        .filter(residency_backfill::Column::Id.eq(BACKFILL_ID))
        .filter(residency_backfill::Column::ClaimEpoch.eq(claim.claim_epoch))
        .filter(residency_backfill::Column::ClaimedBy.eq(&claim.worker_id))
        .filter(residency_backfill::Column::Completed.eq(false))
        .filter(Expr::cust(active_lease))
        .exec(txn)
        .await?;
    Ok(updated.rows_affected == 1)
}
