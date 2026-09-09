use chrono::Duration;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
    sea_query::{Condition, Expr, LockBehavior, LockType, OnConflict},
};

use crate::error::{AppError, AppResult};

use super::{
    database_clock::database_now,
    entities::{bucket, bucket_lifecycle_config},
    lifecycle_scan::{decode_cursor, encode_cursor},
};

use crate::lifecycle::model::{ClaimedLifecycleScan, LifecycleScanCursor};

pub const MAX_LIFECYCLE_SCAN_LEASE_SECONDS: i64 = 86_400;
const MAX_SQLITE_SCAN_CLAIM_RETRIES: usize = 4;

/// Replaces a bucket's active lifecycle configuration and returns its new revision.
pub async fn put_configuration(
    db: &DatabaseConnection,
    bucket_name: &str,
    canonical_json: &str,
) -> AppResult<i64> {
    let bucket_name = bucket_name.to_owned();
    let canonical_json = canonical_json.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket(txn, &bucket_name).await?;
            let now = database_now(txn).await?;
            let previous = lock_configuration(txn, &bucket_name).await?;
            let revision = next_revision(previous.as_ref())?;
            before_upsert_write(&bucket_name).await?;
            upsert_configuration(
                txn,
                &bucket_name,
                Some(canonical_json),
                revision,
                previous.map(|configuration| configuration.created_at),
                now,
            )
            .await?;
            Ok(revision)
        })
    })
    .await
    .map_err(normalize_transaction_error)
}

/// Returns the canonical active configuration. Missing and tombstoned rows are both absent.
pub async fn get_configuration<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<String> {
    bucket::Entity::find_by_id(bucket_name.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()))?;

    bucket_lifecycle_config::Entity::find_by_id(bucket_name.to_owned())
        .one(db)
        .await?
        .and_then(|configuration| configuration.canonical_json)
        .ok_or(AppError::NoSuchLifecycleConfiguration)
}

/// Writes (or advances) a lifecycle tombstone and returns its new revision.
pub async fn delete_configuration(db: &DatabaseConnection, bucket_name: &str) -> AppResult<i64> {
    let bucket_name = bucket_name.to_owned();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket(txn, &bucket_name).await?;
            let now = database_now(txn).await?;
            let previous = lock_configuration(txn, &bucket_name).await?;
            let revision = next_revision(previous.as_ref())?;
            before_delete_write(&bucket_name).await?;
            upsert_configuration(
                txn,
                &bucket_name,
                None,
                revision,
                previous.map(|configuration| configuration.created_at),
                now,
            )
            .await?;
            Ok(revision)
        })
    })
    .await
    .map_err(normalize_transaction_error)
}

/// Claims the next least-recently scanned active configuration using the database clock.
///
/// The returned epoch is the fence required by [`finish_scan_page`]. A lost or expired lease
/// never permits its previous owner to advance the successor configuration's cursor.
pub async fn claim_next_scan(
    db: &DatabaseConnection,
    lease_for: Duration,
) -> AppResult<Option<ClaimedLifecycleScan>> {
    validate_scan_lease(lease_for)?;

    for attempt in 0..=MAX_SQLITE_SCAN_CLAIM_RETRIES {
        let result = db
            .transaction(|txn| Box::pin(claim_next_scan_in_transaction(txn, lease_for)))
            .await;
        match result {
            Ok(claim) => return Ok(claim),
            Err(error)
                if db.get_database_backend() == DatabaseBackend::Sqlite
                    && is_sqlite_contention(&error.to_string())
                    && attempt < MAX_SQLITE_SCAN_CLAIM_RETRIES =>
            {
                sqlite_claim_retry_delay(attempt).await;
            }
            Err(error) => return Err(normalize_transaction_error(error)),
        }
    }
    unreachable!("SQLite lifecycle scan claim retry loop always returns or errors")
}

/// Fences completion of one scan page. Returns `false` when a lease was reclaimed, expired, or
/// invalidated by a replacement/tombstone configuration without modifying that replacement.
pub async fn finish_scan_page(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleScan,
    next_cursor: Option<&LifecycleScanCursor>,
    cycle_complete: bool,
) -> AppResult<bool> {
    let txn = db.begin().await?;
    let completed =
        finish_scan_page_in_transaction(&txn, claim, next_cursor, cycle_complete).await?;
    txn.commit().await?;
    Ok(completed)
}

/// Completes a page in its caller's transaction. The caller must roll back page inserts when
/// this returns `false`; only an exact, active lease may publish the page and its cursor.
pub(crate) async fn finish_scan_page_in_transaction(
    txn: &DatabaseTransaction,
    claim: &ClaimedLifecycleScan,
    next_cursor: Option<&LifecycleScanCursor>,
    cycle_complete: bool,
) -> AppResult<bool> {
    let bucket = claim.bucket.clone();
    let revision = claim.config_revision;
    let lease_epoch = claim.lease_epoch;
    let stored_cursor = if cycle_complete {
        None
    } else {
        next_cursor
            .map(|cursor| {
                let encoded = encode_cursor(cursor);
                decode_cursor(&encoded, &bucket).map_err(|_| stored_cursor_error())?;
                Ok::<String, AppError>(encoded)
            })
            .transpose()?
    };

    let now = database_now(txn).await?;
    let lease_is_active = match txn.get_database_backend() {
        DatabaseBackend::Postgres => "\"scan_lease_until\" > clock_timestamp()",
        DatabaseBackend::Sqlite => "julianday(\"scan_lease_until\") > julianday('now')",
        DatabaseBackend::MySql => {
            return Err(AppError::Internal(
                "lifecycle requires SQLite or PostgreSQL".to_owned(),
            ));
        }
    };
    let updated = bucket_lifecycle_config::Entity::update_many()
        .col_expr(
            bucket_lifecycle_config::Column::ScanCursor,
            Expr::value(stored_cursor),
        )
        .col_expr(
            bucket_lifecycle_config::Column::LastScannedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            bucket_lifecycle_config::Column::ScanLeaseUntil,
            Expr::value(Option::<chrono::DateTime<chrono::Utc>>::None),
        )
        .filter(bucket_lifecycle_config::Column::Bucket.eq(bucket))
        .filter(bucket_lifecycle_config::Column::Revision.eq(revision))
        .filter(bucket_lifecycle_config::Column::ScanLeaseEpoch.eq(lease_epoch))
        .filter(bucket_lifecycle_config::Column::CanonicalJson.is_not_null())
        .filter(Expr::cust(lease_is_active))
        .exec(txn)
        .await?;
    Ok(updated.rows_affected == 1)
}

async fn claim_next_scan_in_transaction<C: ConnectionTrait>(
    db: &C,
    lease_for: Duration,
) -> AppResult<Option<ClaimedLifecycleScan>> {
    let now = database_now(db).await?;
    let candidate = first_claimable_configuration(db, now).await?;
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let canonical_json = candidate.canonical_json.clone().ok_or_else(|| {
        AppError::Internal("claimed lifecycle configuration is tombstoned".to_owned())
    })?;
    let cursor = candidate
        .scan_cursor
        .as_deref()
        .map(|stored| decode_cursor(stored, &candidate.bucket).map_err(|_| stored_cursor_error()))
        .transpose()?;
    let lease_epoch = candidate
        .scan_lease_epoch
        .checked_add(1)
        .ok_or_else(|| AppError::Database("lifecycle scan lease epoch overflow".to_owned()))?;
    let lease_until = now.checked_add_signed(lease_for).ok_or_else(|| {
        AppError::InvalidArgument(
            "lifecycle scan lease is outside the database timestamp range".to_owned(),
        )
    })?;

    let updated = bucket_lifecycle_config::Entity::update_many()
        .col_expr(
            bucket_lifecycle_config::Column::ScanLeaseEpoch,
            Expr::value(lease_epoch),
        )
        .col_expr(
            bucket_lifecycle_config::Column::ScanLeaseUntil,
            Expr::value(Some(lease_until)),
        )
        .filter(bucket_lifecycle_config::Column::Bucket.eq(candidate.bucket.clone()))
        .filter(bucket_lifecycle_config::Column::Revision.eq(candidate.revision))
        .filter(bucket_lifecycle_config::Column::ScanLeaseEpoch.eq(candidate.scan_lease_epoch))
        .filter(scan_claimable_condition(now))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Ok(None);
    }

    Ok(Some(ClaimedLifecycleScan {
        bucket: candidate.bucket,
        config_revision: candidate.revision,
        canonical_json,
        cursor,
        lease_epoch,
        database_now: now,
        lease_until,
    }))
}

async fn first_claimable_configuration<C: ConnectionTrait>(
    db: &C,
    now: chrono::DateTime<chrono::Utc>,
) -> AppResult<Option<bucket_lifecycle_config::Model>> {
    let query = bucket_lifecycle_config::Entity::find()
        .filter(scan_claimable_condition(now))
        .order_by_asc(Expr::cust(
            "CASE WHEN \"last_scanned_at\" IS NULL THEN 0 ELSE 1 END",
        ))
        .order_by_asc(bucket_lifecycle_config::Column::LastScannedAt)
        .order_by_asc(bucket_lifecycle_config::Column::UpdatedAt)
        .order_by_asc(bucket_lifecycle_config::Column::Bucket)
        .limit(1);
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return Ok(query
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(db)
            .await?);
    }
    Ok(query.one(db).await?)
}

fn scan_claimable_condition(now: chrono::DateTime<chrono::Utc>) -> Condition {
    Condition::all()
        .add(bucket_lifecycle_config::Column::CanonicalJson.is_not_null())
        .add(
            Condition::any()
                .add(bucket_lifecycle_config::Column::ScanLeaseUntil.is_null())
                .add(bucket_lifecycle_config::Column::ScanLeaseUntil.lte(now)),
        )
}

fn validate_scan_lease(lease_for: Duration) -> AppResult<()> {
    if lease_for <= Duration::zero()
        || lease_for > Duration::seconds(MAX_LIFECYCLE_SCAN_LEASE_SECONDS)
    {
        return Err(AppError::InvalidArgument(format!(
            "lifecycle scan lease must be between 1 second and {MAX_LIFECYCLE_SCAN_LEASE_SECONDS} seconds"
        )));
    }
    Ok(())
}

fn stored_cursor_error() -> AppError {
    AppError::Internal("stored lifecycle scan cursor is invalid".to_owned())
}

fn is_sqlite_contention(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("database is locked") || error.contains("database is busy")
}

async fn sqlite_claim_retry_delay(attempt: usize) {
    let milliseconds = 1_u64.checked_shl(attempt.min(4) as u32).unwrap_or(16);
    tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
}

async fn lock_bucket<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<bucket::Model> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return bucket::Entity::find_by_id(bucket_name.to_owned())
            .lock_exclusive()
            .one(db)
            .await?
            .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()));
    }

    let locked = bucket::Entity::update_many()
        .col_expr(
            bucket::Column::CreatedAt,
            Expr::col(bucket::Column::CreatedAt).into(),
        )
        .filter(bucket::Column::Name.eq(bucket_name))
        .exec(db)
        .await?;
    if locked.rows_affected != 1 {
        return Err(AppError::NoSuchBucket(bucket_name.to_owned()));
    }
    bucket::Entity::find_by_id(bucket_name.to_owned())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchBucket(bucket_name.to_owned()))
}

async fn lock_configuration<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<Option<bucket_lifecycle_config::Model>> {
    let query = bucket_lifecycle_config::Entity::find_by_id(bucket_name.to_owned());
    if db.get_database_backend() == DatabaseBackend::Postgres {
        return Ok(query.lock_exclusive().one(db).await?);
    }
    Ok(query.one(db).await?)
}

fn next_revision(previous: Option<&bucket_lifecycle_config::Model>) -> AppResult<i64> {
    match previous {
        Some(configuration) => configuration.revision.checked_add(1).ok_or_else(|| {
            AppError::Database("lifecycle configuration revision overflow".to_owned())
        }),
        None => Ok(1),
    }
}

async fn upsert_configuration<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    canonical_json: Option<String>,
    revision: i64,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> AppResult<()> {
    let model = bucket_lifecycle_config::ActiveModel {
        bucket: Set(bucket_name.to_owned()),
        canonical_json: Set(canonical_json),
        revision: Set(revision),
        scan_cursor: Set(None),
        scan_lease_epoch: Set(0),
        scan_lease_until: Set(None),
        created_at: Set(created_at.unwrap_or(now)),
        updated_at: Set(now),
        last_scanned_at: Set(None),
    };
    bucket_lifecycle_config::Entity::insert(model)
        .on_conflict(
            OnConflict::column(bucket_lifecycle_config::Column::Bucket)
                .update_columns([
                    bucket_lifecycle_config::Column::CanonicalJson,
                    bucket_lifecycle_config::Column::Revision,
                    bucket_lifecycle_config::Column::ScanCursor,
                    bucket_lifecycle_config::Column::ScanLeaseEpoch,
                    bucket_lifecycle_config::Column::ScanLeaseUntil,
                    bucket_lifecycle_config::Column::UpdatedAt,
                    bucket_lifecycle_config::Column::LastScannedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

async fn before_upsert_write(bucket_name: &str) -> AppResult<()> {
    #[cfg(test)]
    test_hooks::fail_before_write(test_hooks::WriteOperation::Put, bucket_name).await?;
    #[cfg(not(test))]
    let _ = bucket_name;
    Ok(())
}

async fn before_delete_write(bucket_name: &str) -> AppResult<()> {
    #[cfg(test)]
    test_hooks::fail_before_write(test_hooks::WriteOperation::Delete, bucket_name).await?;
    #[cfg(not(test))]
    let _ = bucket_name;
    Ok(())
}

fn normalize_transaction_error(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => error.into(),
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{Arc, LazyLock, Mutex};

    use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

    use crate::error::{AppError, AppResult};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum WriteOperation {
        Put,
        Delete,
    }

    static TEST_LOCK: LazyLock<Arc<AsyncMutex<()>>> =
        LazyLock::new(|| Arc::new(AsyncMutex::new(())));
    #[derive(Debug)]
    struct PendingFailure {
        operation: WriteOperation,
        bucket_name: String,
    }

    static NEXT_FAILURE: LazyLock<Mutex<Option<PendingFailure>>> =
        LazyLock::new(|| Mutex::new(None));

    pub struct FailureScope {
        _serial: OwnedMutexGuard<()>,
    }

    pub async fn fail_next(operation: WriteOperation, bucket_name: &str) -> FailureScope {
        let serial = TEST_LOCK.clone().lock_owned().await;
        *NEXT_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(PendingFailure {
            operation,
            bucket_name: bucket_name.to_owned(),
        });
        FailureScope { _serial: serial }
    }

    pub async fn fail_before_write(operation: WriteOperation, bucket_name: &str) -> AppResult<()> {
        let mut failure = NEXT_FAILURE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failure.as_ref().is_some_and(|pending| {
            pending.operation == operation && pending.bucket_name == bucket_name
        }) {
            *failure = None;
            return Err(AppError::Database(
                "injected lifecycle configuration write failure".to_owned(),
            ));
        }
        Ok(())
    }

    impl Drop for FailureScope {
        fn drop(&mut self) {
            *NEXT_FAILURE
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter};

    use super::*;
    use crate::{
        error::AppError,
        lifecycle::{
            config::{canonical_json, from_canonical_json, validate_and_canonicalize},
            model::{CanonicalLifecycleConfiguration, LifecycleScanCursor, LifecycleScanSource},
        },
        store::{
            connect_database, entities::bucket_lifecycle_config, lifecycle_scan::encode_cursor,
            run_migrations,
        },
    };

    async fn setup() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        super::super::bucket::create(&db, "bucket", Some("owner"))
            .await
            .unwrap();
        db
    }

    fn json(rule_id: &str, days: i32) -> String {
        let input = serde_json::from_value(serde_json::json!({
            "rules": [{
                "id": rule_id,
                "prefix": "logs/",
                "status": "Enabled",
                "expiration": { "days": days }
            }]
        }))
        .unwrap();
        canonical_json(&validate_and_canonicalize(input).unwrap()).unwrap()
    }

    async fn row_for(db: &DatabaseConnection, bucket_name: &str) -> bucket_lifecycle_config::Model {
        bucket_lifecycle_config::Entity::find_by_id(bucket_name)
            .one(db)
            .await
            .unwrap()
            .unwrap()
    }

    async fn row(db: &DatabaseConnection) -> bucket_lifecycle_config::Model {
        row_for(db, "bucket").await
    }

    #[tokio::test]
    async fn put_replaces_active_configuration_with_monotonic_revision_and_resets_scan_state() {
        let db = setup().await;
        let first_json = json("first", 1);
        assert_eq!(
            put_configuration(&db, "bucket", &first_json).await.unwrap(),
            1
        );
        let first = row(&db).await;

        let stale_scan_time = database_now(&db).await.unwrap() - Duration::hours(1);
        bucket_lifecycle_config::Entity::update_many()
            .col_expr(
                bucket_lifecycle_config::Column::ScanCursor,
                Expr::value("stale-cursor"),
            )
            .col_expr(
                bucket_lifecycle_config::Column::ScanLeaseEpoch,
                Expr::value(9_i64),
            )
            .col_expr(
                bucket_lifecycle_config::Column::ScanLeaseUntil,
                Expr::value(stale_scan_time),
            )
            .col_expr(
                bucket_lifecycle_config::Column::LastScannedAt,
                Expr::value(stale_scan_time),
            )
            .filter(bucket_lifecycle_config::Column::Bucket.eq("bucket"))
            .exec(&db)
            .await
            .unwrap();

        let second_json = json("second", 2);
        assert_eq!(
            put_configuration(&db, "bucket", &second_json)
                .await
                .unwrap(),
            2
        );
        let second = row(&db).await;
        assert_eq!(second.canonical_json.as_deref(), Some(second_json.as_str()));
        assert_eq!(second.revision, 2);
        assert_eq!(second.created_at, first.created_at);
        assert_eq!(second.scan_cursor, None);
        assert_eq!(second.scan_lease_epoch, 0);
        assert_eq!(second.scan_lease_until, None);
        assert_eq!(second.last_scanned_at, None);
        assert_eq!(get_configuration(&db, "bucket").await.unwrap(), second_json);
    }

    #[tokio::test]
    async fn absent_tombstone_repeated_delete_and_later_put_remain_monotonic() {
        let db = setup().await;
        assert!(matches!(
            get_configuration(&db, "bucket").await,
            Err(AppError::NoSuchLifecycleConfiguration)
        ));

        assert_eq!(delete_configuration(&db, "bucket").await.unwrap(), 1);
        let first_tombstone = row(&db).await;
        assert_eq!(first_tombstone.canonical_json, None);
        assert_eq!(first_tombstone.revision, 1);
        assert!(matches!(
            get_configuration(&db, "bucket").await,
            Err(AppError::NoSuchLifecycleConfiguration)
        ));

        assert_eq!(delete_configuration(&db, "bucket").await.unwrap(), 2);
        let second_tombstone = row(&db).await;
        assert_eq!(second_tombstone.canonical_json, None);
        assert_eq!(second_tombstone.revision, 2);
        assert_eq!(second_tombstone.created_at, first_tombstone.created_at);

        let active = json("later", 3);
        assert_eq!(put_configuration(&db, "bucket", &active).await.unwrap(), 3);
        let replacement = row(&db).await;
        assert_eq!(replacement.canonical_json.as_deref(), Some(active.as_str()));
        assert_eq!(replacement.revision, 3);
        assert_eq!(replacement.created_at, first_tombstone.created_at);
    }

    #[tokio::test]
    async fn missing_bucket_is_rejected_without_creating_configuration() {
        let db = setup().await;
        let err = put_configuration(&db, "missing", &json("rule", 1))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NoSuchBucket(name) if name == "missing"));
        assert!(matches!(
            delete_configuration(&db, "missing").await,
            Err(AppError::NoSuchBucket(name)) if name == "missing"
        ));
        assert_eq!(
            bucket_lifecycle_config::Entity::find()
                .count(&db)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn injected_prewrite_failures_roll_back_prior_row_without_leaking_to_next_operation() {
        let db = setup().await;
        let bucket_name = "failure-bucket";
        super::super::bucket::create(&db, bucket_name, Some("owner"))
            .await
            .unwrap();
        let original = json("original", 1);
        put_configuration(&db, bucket_name, &original)
            .await
            .unwrap();
        bucket_lifecycle_config::Entity::update_many()
            .col_expr(
                bucket_lifecycle_config::Column::ScanCursor,
                Expr::value("checkpoint"),
            )
            .filter(bucket_lifecycle_config::Column::Bucket.eq(bucket_name))
            .exec(&db)
            .await
            .unwrap();
        let before = row_for(&db, bucket_name).await;

        let scope = test_hooks::fail_next(test_hooks::WriteOperation::Put, bucket_name).await;
        assert!(matches!(
            put_configuration(&db, bucket_name, &json("replacement", 2)).await,
            Err(AppError::Database(message)) if message == "injected lifecycle configuration write failure"
        ));
        drop(scope);
        let after_failed_put = row_for(&db, bucket_name).await;
        assert_eq!(after_failed_put.canonical_json, before.canonical_json);
        assert_eq!(after_failed_put.revision, before.revision);
        assert_eq!(after_failed_put.scan_cursor, before.scan_cursor);

        let scope = test_hooks::fail_next(test_hooks::WriteOperation::Delete, bucket_name).await;
        assert!(matches!(
            delete_configuration(&db, bucket_name).await,
            Err(AppError::Database(message)) if message == "injected lifecycle configuration write failure"
        ));
        drop(scope);
        let after_failed_delete = row_for(&db, bucket_name).await;
        assert_eq!(after_failed_delete.canonical_json, before.canonical_json);
        assert_eq!(after_failed_delete.revision, before.revision);
        assert_eq!(after_failed_delete.scan_cursor, before.scan_cursor);

        assert_eq!(
            put_configuration(&db, bucket_name, &json("next", 3))
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn concurrent_puts_get_distinct_consecutive_revisions_and_leave_one_complete_document() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lifecycle-config-race.db");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            path.display().to_string().replace('\\', "/")
        );
        let db = connect_database(&database_url).await.unwrap();
        run_migrations(&db).await.unwrap();
        super::super::bucket::create(&db, "bucket", Some("owner"))
            .await
            .unwrap();
        let first = json("first", 1);
        let second = json("second", 2);
        let left_db = db.clone();
        let right_db = db.clone();

        let (left, right) = tokio::join!(
            put_configuration(&left_db, "bucket", &first),
            put_configuration(&right_db, "bucket", &second),
        );
        let mut revisions = [left.unwrap(), right.unwrap()];
        revisions.sort_unstable();
        assert_eq!(revisions, [1, 2]);

        let persisted = get_configuration(&db, "bucket").await.unwrap();
        assert!(persisted == first || persisted == second);
        let parsed: CanonicalLifecycleConfiguration = from_canonical_json(&persisted).unwrap();
        assert_eq!(parsed.rules.len(), 1);
        assert!(matches!(
            parsed.rules[0].id.as_deref(),
            Some("first") | Some("second")
        ));
        assert_eq!(row(&db).await.revision, 2);
    }

    #[tokio::test]
    async fn lifecycle_claim_scan_has_one_winner_reclaims_expiry_and_fences_page_completion() {
        let db = setup().await;
        super::super::bucket::create(&db, "later-bucket", Some("owner"))
            .await
            .unwrap();
        let first_json = json("first", 1);
        let later_json = json("later", 1);
        put_configuration(&db, "bucket", &first_json).await.unwrap();
        put_configuration(&db, "later-bucket", &later_json)
            .await
            .unwrap();

        let first = claim_next_scan(&db, Duration::seconds(30))
            .await
            .unwrap()
            .expect("first active configuration is claimed");
        assert_eq!(first.bucket, "bucket");
        assert_eq!(first.canonical_json, first_json);
        assert!(first.cursor.is_none());
        assert_eq!(first.lease_epoch, 1);
        assert!(first.lease_until > first.database_now);

        let second = claim_next_scan(&db, Duration::seconds(30))
            .await
            .unwrap()
            .expect("the other unleased configuration is claimable");
        assert_eq!(second.bucket, "later-bucket");
        assert!(
            claim_next_scan(&db, Duration::seconds(30))
                .await
                .unwrap()
                .is_none()
        );

        let db_now = database_now(&db).await.unwrap();
        bucket_lifecycle_config::Entity::update_many()
            .col_expr(
                bucket_lifecycle_config::Column::ScanLeaseUntil,
                Expr::value(Some(db_now - Duration::seconds(1))),
            )
            .filter(bucket_lifecycle_config::Column::Bucket.eq("bucket"))
            .exec(&db)
            .await
            .unwrap();
        let takeover = claim_next_scan(&db, Duration::seconds(30))
            .await
            .unwrap()
            .expect("expired lease is reclaimable");
        assert_eq!(takeover.bucket, "bucket");
        assert_eq!(takeover.lease_epoch, first.lease_epoch + 1);

        let cursor = LifecycleScanCursor {
            source: LifecycleScanSource::Current,
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            sequence: Some(1),
            version_row_id: Some("version-row".to_owned()),
            multipart_created_at: None,
            multipart_upload_id: None,
        };
        assert!(
            !finish_scan_page(&db, &first, Some(&cursor), false)
                .await
                .unwrap(),
            "a stale lease epoch cannot write a cursor"
        );
        assert!(
            finish_scan_page(&db, &takeover, Some(&cursor), false)
                .await
                .unwrap()
        );
        let stored_cursor = encode_cursor(&cursor);
        assert_eq!(
            row(&db).await.scan_cursor.as_deref(),
            Some(stored_cursor.as_str())
        );

        let restart = claim_next_scan(&db, Duration::seconds(30))
            .await
            .unwrap()
            .expect("the completed page lease was cleared for a later page");
        assert_eq!(restart.bucket, "bucket");
        assert!(finish_scan_page(&db, &restart, None, true).await.unwrap());
        assert!(
            row(&db).await.scan_cursor.is_none(),
            "completed cycles restart at Current"
        );

        let replacement = json("replacement", 2);
        put_configuration(&db, "bucket", &replacement)
            .await
            .unwrap();
        assert!(
            !finish_scan_page(&db, &takeover, Some(&cursor), true)
                .await
                .unwrap(),
            "a replaced configuration invalidates every prior page claim"
        );
        let persisted = row(&db).await;
        assert_eq!(
            persisted.canonical_json.as_deref(),
            Some(replacement.as_str())
        );
        assert!(persisted.scan_cursor.is_none());
    }

    #[tokio::test]
    async fn lifecycle_claim_scan_race_has_exactly_one_winner_and_rejects_bad_stored_cursor() {
        let db = setup().await;
        put_configuration(&db, "bucket", &json("race", 1))
            .await
            .unwrap();
        let left_db = db.clone();
        let right_db = db.clone();
        let (left, right) = tokio::join!(
            claim_next_scan(&left_db, Duration::seconds(30)),
            claim_next_scan(&right_db, Duration::seconds(30)),
        );
        let winners = [left.unwrap(), right.unwrap()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(winners.len(), 1, "only one scanner owns an epoch at a time");

        let db_now = database_now(&db).await.unwrap();
        bucket_lifecycle_config::Entity::update_many()
            .col_expr(
                bucket_lifecycle_config::Column::ScanLeaseUntil,
                Expr::value(Some(db_now - Duration::seconds(1))),
            )
            .col_expr(
                bucket_lifecycle_config::Column::ScanCursor,
                Expr::value(Some("not-a-valid-cursor")),
            )
            .filter(bucket_lifecycle_config::Column::Bucket.eq("bucket"))
            .exec(&db)
            .await
            .unwrap();
        assert!(matches!(
            claim_next_scan(&db, Duration::seconds(30)).await,
            Err(AppError::Internal(message)) if message == "stored lifecycle scan cursor is invalid"
        ));
    }
}
