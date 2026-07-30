use chrono::{DateTime, Utc};
use sea_orm::{
    DatabaseBackend,
    sea_query::{Expr, SimpleExpr},
};

/// Database-clock lease predicate used by every claim-owned statement.
///
/// PostgreSQL's transaction clock is intentionally unsuitable here because a
/// transaction may have waited on a lock. SQLite timestamps are normalized
/// through `julianday`, avoiding lexical comparisons between timestamp text
/// encodings.
pub(crate) fn active_lease(backend: DatabaseBackend) -> SimpleExpr {
    match backend {
        DatabaseBackend::Postgres => Expr::cust("\"locked_until\" > clock_timestamp()"),
        DatabaseBackend::Sqlite => Expr::cust("julianday(\"locked_until\") > julianday('now')"),
        DatabaseBackend::MySql => Expr::cust("`locked_until` > UTC_TIMESTAMP(6)"),
    }
}

/// Validates a proposed renewal against the same execution-time database clock
/// as the active-lease fence.
pub(crate) fn lease_end_is_future(
    backend: DatabaseBackend,
    lease_until: DateTime<Utc>,
) -> SimpleExpr {
    match backend {
        DatabaseBackend::Postgres => Expr::cust_with_values("? > clock_timestamp()", [lease_until]),
        DatabaseBackend::Sqlite => {
            Expr::cust_with_values("julianday(?) > julianday('now')", [lease_until])
        }
        DatabaseBackend::MySql => Expr::cust_with_values("? > UTC_TIMESTAMP(6)", [lease_until]),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm::{DatabaseBackend, sea_query::Query};

    use crate::store::entities::import_job;

    use super::{active_lease, lease_end_is_future};

    #[test]
    fn postgres_lease_fences_use_wall_clock_not_transaction_clock() {
        let query = Query::select()
            .column(import_job::Column::Id)
            .from(import_job::Entity)
            .and_where(active_lease(DatabaseBackend::Postgres))
            .and_where(lease_end_is_future(DatabaseBackend::Postgres, Utc::now()))
            .to_owned();
        let sql = query.to_string(sea_orm::sea_query::PostgresQueryBuilder);

        assert_eq!(sql.matches("clock_timestamp()").count(), 2);
        assert!(!sql.to_ascii_lowercase().contains("current_timestamp"));
    }

    #[test]
    fn sqlite_lease_fences_normalize_timestamps_numerically() {
        let query = Query::select()
            .column(import_job::Column::Id)
            .from(import_job::Entity)
            .and_where(active_lease(DatabaseBackend::Sqlite))
            .and_where(lease_end_is_future(DatabaseBackend::Sqlite, Utc::now()))
            .to_owned();
        let sql = query.to_string(sea_orm::sea_query::SqliteQueryBuilder);

        assert_eq!(sql.matches("julianday").count(), 4);
        assert!(!sql.contains("\"locked_until\" >"));
    }
}
