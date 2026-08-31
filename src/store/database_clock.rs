use chrono::{DateTime, Utc};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, Statement,
    sea_query::{Expr, SimpleExpr},
};

use crate::error::{AppError, AppResult};

pub async fn database_now<C: ConnectionTrait>(db: &C) -> AppResult<DateTime<Utc>> {
    let backend = db.get_database_backend();
    let sql = database_now_sql(backend)
        .ok_or_else(|| AppError::Internal("lifecycle requires SQLite or PostgreSQL".to_owned()))?;
    let row = db
        .query_one(Statement::from_string(backend, sql))
        .await?
        .ok_or_else(|| AppError::Internal("database clock returned no row".to_owned()))?;
    row.try_get("", "now")
        .map_err(|_| AppError::Internal("database clock result is invalid".to_owned()))
}

fn database_now_sql(backend: DatabaseBackend) -> Option<&'static str> {
    match backend {
        DatabaseBackend::Postgres => Some("SELECT clock_timestamp() AS now"),
        DatabaseBackend::Sqlite => Some("SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS now"),
        DatabaseBackend::MySql => None,
    }
}

#[allow(dead_code)] // Used by the lifecycle action claimer added in a later task.
fn action_lease_expired(backend: DatabaseBackend) -> SimpleExpr {
    match backend {
        DatabaseBackend::Postgres => Expr::cust("\"lease_until\" <= clock_timestamp()"),
        DatabaseBackend::Sqlite => Expr::cust("julianday(\"lease_until\") <= julianday('now')"),
        DatabaseBackend::MySql => Expr::cust("FALSE"),
    }
}

#[allow(dead_code)] // Used by the lifecycle scan claimer added in a later task.
fn scan_lease_expired(backend: DatabaseBackend) -> SimpleExpr {
    match backend {
        DatabaseBackend::Postgres => Expr::cust("\"scan_lease_until\" <= clock_timestamp()"),
        DatabaseBackend::Sqlite => {
            Expr::cust("julianday(\"scan_lease_until\") <= julianday('now')")
        }
        DatabaseBackend::MySql => Expr::cust("FALSE"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use chrono::{Duration, Timelike};
    use sea_orm::{Database, DatabaseBackend, sea_query::Query};

    use super::{action_lease_expired, database_now, database_now_sql, scan_lease_expired};

    #[tokio::test]
    async fn database_clock_sqlite_returns_engine_time_between_process_observations() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let observed_before = chrono::DateTime::<chrono::Utc>::from(SystemTime::now());
        let before = observed_before
            .with_nanosecond(0)
            .expect("zero nanoseconds are always valid");
        let now = database_now(&db).await.unwrap();
        let after = chrono::DateTime::<chrono::Utc>::from(SystemTime::now());

        assert!(
            now >= before && now <= after + Duration::seconds(1),
            "database time must be engine-owned"
        );
    }

    #[test]
    fn database_clock_backend_predicates_use_execution_time_database_clocks() {
        let postgres = Query::select()
            .expr(action_lease_expired(DatabaseBackend::Postgres))
            .to_string(sea_orm::sea_query::PostgresQueryBuilder);
        assert!(postgres.contains("clock_timestamp()"), "sql: {postgres}");
        assert!(!postgres.contains("CURRENT_TIMESTAMP"), "sql: {postgres}");

        let sqlite = Query::select()
            .expr(action_lease_expired(DatabaseBackend::Sqlite))
            .to_string(sea_orm::sea_query::SqliteQueryBuilder);
        assert!(
            sqlite.contains("julianday(\"lease_until\") <= julianday('now')"),
            "sql: {sqlite}"
        );

        let scan = Query::select()
            .expr(scan_lease_expired(DatabaseBackend::Sqlite))
            .to_string(sea_orm::sea_query::SqliteQueryBuilder);
        assert!(
            scan.contains("julianday(\"scan_lease_until\") <= julianday('now')"),
            "sql: {scan}"
        );
    }

    #[test]
    fn database_clock_postgres_query_uses_wall_clock() {
        let sql = database_now_sql(DatabaseBackend::Postgres).unwrap();
        assert_eq!(sql, "SELECT clock_timestamp() AS now");
    }

    #[test]
    fn database_clock_rejects_unsupported_backends() {
        assert!(database_now_sql(DatabaseBackend::MySql).is_none());
    }
}
