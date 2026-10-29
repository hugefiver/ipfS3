//! One owned transaction per bounded maintenance unit, never a whole-history deadline.
use super::*;
use futures_util::future::BoxFuture;
use sea_orm::{DatabaseTransaction, ExecResult, QueryResult};

mod sealed {
    pub trait Sealed {}
    impl Sealed for sea_orm::DatabaseTransaction {}
    impl Sealed for super::Transaction {}
}

/// Writers accept an actual transaction, never an arbitrary pooled connection.
pub(crate) trait TransactionConnection: ConnectionTrait + sealed::Sealed {}
impl TransactionConnection for DatabaseTransaction {}
impl TransactionConnection for Transaction {}

#[derive(Clone)]
pub(super) struct Request {
    scope: String,
    identity: String,
    source: String,
    target: String,
}
impl From<&UpgradeRequest<'_>> for Request {
    fn from(request: &UpgradeRequest<'_>) -> Self {
        Self {
            scope: request.database_scope.into(),
            identity: request.native_identity.into(),
            source: request.source_version.into(),
            target: request.target_version.into(),
        }
    }
}
impl Request {
    pub(super) fn borrowed(&self) -> UpgradeRequest<'_> {
        UpgradeRequest {
            database_scope: &self.scope,
            native_identity: &self.identity,
            source_version: &self.source,
            target_version: &self.target,
        }
    }
}

pub(crate) enum Transaction {
    Sqlite(DatabaseTransaction),
    Postgres(postgres::Guarded),
}
impl Transaction {
    fn connection(&self) -> &dyn ConnectionTrait {
        match self {
            Self::Sqlite(tx) => tx,
            Self::Postgres(tx) => tx,
        }
    }
}
#[async_trait::async_trait]
impl ConnectionTrait for Transaction {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.connection().get_database_backend()
    }
    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.connection().execute(statement).await
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.connection().execute_unprepared(sql).await
    }
    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.connection().query_one(statement).await
    }
    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.connection().query_all(statement).await
    }
}

pub(super) async fn run<T, F>(db: &DatabaseConnection, work: F) -> Result<T, DbErr>
where
    T: Send + 'static,
    F: for<'a> FnOnce(&'a Transaction) -> BoxFuture<'a, Result<T, DbErr>> + Send + 'static,
{
    if db.get_database_backend() == DatabaseBackend::Postgres {
        postgres::maintenance(db, move |db| async move {
            let root = db.begin().await?;
            let tx = Transaction::Postgres(db);
            let output = work(&tx).await?;
            root.commit().await?;
            Ok(output)
        })
        .await
    } else {
        let tx = Transaction::Sqlite(db.begin().await?);
        let output = work(&tx).await?;
        let Transaction::Sqlite(tx) = tx else {
            unreachable!()
        };
        tx.commit().await?;
        Ok(output)
    }
}
