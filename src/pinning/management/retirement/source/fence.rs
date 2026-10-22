use super::*;

pub(super) async fn lock_sources(tx: &impl ConnectionTrait) -> Result<()> {
    for table in native::current_catalog() {
        tx.execute_unprepared(&format!(
            "LOCK TABLE \"{}\" IN SHARE MODE NOWAIT",
            table.table
        ))
        .await
        .map_err(crate::store::pinning::management::nowait_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{DbErr, ExecResult, QueryResult, RuntimeErr};
    use sqlx::error::{DatabaseError, ErrorKind};
    use std::{
        borrow::Cow,
        error::Error,
        fmt,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug)]
    struct SqlState(&'static str);
    impl fmt::Display for SqlState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("statement refused")
        }
    }
    impl Error for SqlState {}
    impl DatabaseError for SqlState {
        fn message(&self) -> &str {
            "statement refused"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }
        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }
    }

    struct Refused {
        state: &'static str,
        attempts: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ConnectionTrait for Refused {
        fn get_database_backend(&self) -> DatabaseBackend {
            DatabaseBackend::Postgres
        }
        async fn execute(&self, _: Statement) -> std::result::Result<ExecResult, DbErr> {
            unreachable!()
        }
        async fn execute_unprepared(&self, _: &str) -> std::result::Result<ExecResult, DbErr> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(DbErr::Exec(RuntimeErr::SqlxError(sqlx::Error::Database(
                Box::new(SqlState(self.state)),
            ))))
        }
        async fn query_one(&self, _: Statement) -> std::result::Result<Option<QueryResult>, DbErr> {
            unreachable!()
        }
        async fn query_all(&self, _: Statement) -> std::result::Result<Vec<QueryResult>, DbErr> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn inventory_nowait_conflict_stops_at_the_first_source_and_is_changed() {
        for (state, expected) in [
            ("55P03", ManagementError::Changed),
            ("42501", ManagementError::Database),
            ("42P01", ManagementError::Database),
            ("25P02", ManagementError::Database),
        ] {
            let connection = Refused {
                state,
                attempts: AtomicUsize::new(0),
            };
            assert_eq!(lock_sources(&connection).await, Err(expected));
            assert_eq!(connection.attempts.load(Ordering::SeqCst), 1);
        }
    }
}
