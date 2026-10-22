use super::*;
use sea_orm::{DbErr, RuntimeErr};
use sqlx::error::{DatabaseError, ErrorKind};
use std::{borrow::Cow, error::Error, fmt};

#[derive(Debug)]
struct SqlState(&'static str);
impl fmt::Display for SqlState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("database statement rejected")
    }
}
impl Error for SqlState {}
impl DatabaseError for SqlState {
    fn message(&self) -> &str {
        "database statement rejected"
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

#[test]
fn row_and_table_nowait_refusals_preserve_the_fence_category() {
    for wrap in [DbErr::Query, DbErr::Exec] {
        let error = wrap(RuntimeErr::SqlxError(sqlx::Error::Database(Box::new(
            SqlState("55P03"),
        ))));
        assert_eq!(nowait_error(error), ManagementError::Changed);
    }
}

#[test]
fn aborted_transactions_sql_faults_and_transport_errors_are_not_fence_drift() {
    for code in ["25P02", "42501", "42P01", "08006"] {
        let error = DbErr::Exec(RuntimeErr::SqlxError(sqlx::Error::Database(Box::new(
            SqlState(code),
        ))));
        assert_eq!(nowait_error(error), ManagementError::Database);
    }
    assert_eq!(
        nowait_error(DbErr::Query(RuntimeErr::SqlxError(sqlx::Error::PoolClosed))),
        ManagementError::Database
    );
    assert_eq!(
        nowait_error(DbErr::Custom("55P03".into())),
        ManagementError::Database
    );
}
