//! Store-owned layout qualification. Structural maintenance requires stop/restart.
//! Business authority is always read in the operation's transaction.
use super::management::{self, admission::work_error, records::DecodeWork};
use crate::{
    error::{AppError, AppResult},
    store::Store,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, TransactionTrait};

mod meter;
#[cfg(test)]
mod tests;

/// Only a completed validation on the enclosing Store's own connection can
/// construct this proof. It contains no business authority.
pub(in crate::store) struct StaticQualification {
    versions: Vec<String>,
}

/// Owns both the transaction and its Store identity; no bare connection can be
/// paired with another Store's qualification.
pub(crate) struct RuntimeTransaction {
    tx: DatabaseTransaction,
    owner: Store,
}

impl std::ops::Deref for RuntimeTransaction {
    type Target = DatabaseTransaction;
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for RuntimeTransaction {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.tx.get_database_backend()
    }
    async fn execute(&self, statement: Statement) -> Result<sea_orm::ExecResult, sea_orm::DbErr> {
        self.tx.execute(statement).await
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<sea_orm::ExecResult, sea_orm::DbErr> {
        self.tx.execute_unprepared(sql).await
    }
    async fn query_one(
        &self,
        statement: Statement,
    ) -> Result<Option<sea_orm::QueryResult>, sea_orm::DbErr> {
        self.tx.query_one(statement).await
    }
    async fn query_all(
        &self,
        statement: Statement,
    ) -> Result<Vec<sea_orm::QueryResult>, sea_orm::DbErr> {
        self.tx.query_all(statement).await
    }
}

fn invalid() -> AppError {
    work_error(management::ManagementError::Schema)
}

fn check(work: DecodeWork<'_>) -> AppResult<()> {
    work.with(|session| session.check()).map_err(work_error)
}

async fn rollback(tx: DatabaseTransaction, work: DecodeWork<'_>) -> AppResult<()> {
    // Cleanup must run even after the operation has exhausted its allowance.
    let charged = meter::cleanup_sql(work);
    let result = tx.rollback().await;
    charged.map_err(work_error)?;
    result?;
    check(work)
}

impl Store {
    /// Initialize and qualify the owned pool before exposing it to runtime work.
    /// `open` returns only after startup validation and connection cleanup succeed.
    pub(crate) async fn open(db: sea_orm::DatabaseConnection) -> Result<Self, sea_orm::DbErr> {
        crate::store::initialization::open(&db).await?;
        let store = Self::new(db);
        use sea_orm_migration::MigratorTrait;
        let versions = crate::store::migrator::Migrator::migrations()
            .iter()
            .map(|migration| migration.name().to_owned())
            .collect();
        store
            .inner
            .qualification
            .set(StaticQualification { versions })
            .map_err(|_| sea_orm::DbErr::Custom("Store already initialized".into()))?;
        Ok(store)
    }

    pub(crate) async fn begin_publication_runtime(
        &self,
        work: DecodeWork<'_>,
    ) -> Result<RuntimeTransaction, sea_orm::TransactionError<AppError>> {
        use sea_orm::TransactionError;
        self.qualify_runtime_schema(work)
            .await
            .map_err(TransactionError::Transaction)?;
        meter::before_sql(work)
            .map_err(work_error)
            .map_err(TransactionError::Transaction)?;
        let tx = self
            .db()
            .begin()
            .await
            .map_err(TransactionError::Connection)?;
        if let Err(error) = check(work) {
            let charged = meter::cleanup_sql(work);
            tx.rollback().await.map_err(TransactionError::Connection)?;
            charged
                .map_err(work_error)
                .map_err(TransactionError::Transaction)?;
            return Err(TransactionError::Transaction(error));
        }
        Ok(RuntimeTransaction {
            tx,
            owner: self.clone(),
        })
    }

    /// Qualify the trusted, fixed-schema pool once per Store identity, before
    /// checking out any business transaction.
    pub(crate) async fn qualify_runtime_schema(&self, work: DecodeWork<'_>) -> AppResult<()> {
        check(work)?;
        if !matches!(
            self.db().get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            return Err(invalid());
        }
        self.inner
            .qualification
            .get_or_try_init(|| async {
                check(work)?;
                meter::before_sql(work).map_err(work_error)?;
                let opened = self.db().begin().await;
                let tx = match opened {
                    Ok(tx) => tx,
                    Err(error) => {
                        check(work)?;
                        return Err(error.into());
                    }
                };
                let validation = async {
                    check(work)?;
                    let versions = if tx.get_database_backend() == DatabaseBackend::Sqlite {
                        let db = meter::SqliteRead::new(&tx, work);
                        validate(&db).await?
                    } else {
                        validate(&tx).await?
                    };
                    check(work)?;
                    Ok::<_, AppError>(versions)
                }
                .await;
                rollback(tx, work).await?;
                let versions = validation?;
                // The initializer's cancellation/deadline is checked after the
                // normal rollback and before OnceCell can publish the proof.
                check(work)?;
                Ok::<StaticQualification, AppError>(StaticQualification { versions })
            })
            .await?;
        // Waiters use their own remaining allowance, never the initializer's SQL debt.
        check(work)
    }

    pub(crate) async fn begin_runtime(
        &self,
        work: DecodeWork<'_>,
    ) -> AppResult<RuntimeTransaction> {
        self.qualify_runtime_schema(work).await?;
        if self.db().get_database_backend() == DatabaseBackend::Sqlite {
            meter::before_sql(work).map_err(work_error)?;
        }
        let opened = self.db().begin().await;
        let tx = match opened {
            Ok(tx) => tx,
            Err(error) => {
                check(work)?;
                return Err(error.into());
            }
        };
        if let Err(error) = check(work) {
            rollback(tx, work).await?;
            return Err(error);
        }
        // No live/business read precedes the caller's first writer fence.
        Ok(RuntimeTransaction {
            tx,
            owner: self.clone(),
        })
    }
}

impl RuntimeTransaction {
    pub(crate) fn into_db(self) -> DatabaseTransaction {
        self.tx
    }

    pub(crate) fn catalog<'a>(
        &'a self,
        work: DecodeWork<'a>,
    ) -> super::legacy_migration::ReceiptCatalog<'a> {
        super::legacy_migration::ReceiptCatalog::Runtime(self, work)
    }
    pub(crate) fn db(&self) -> &DatabaseTransaction {
        &self.tx
    }

    pub(crate) async fn schema_versions(&self, work: DecodeWork<'_>) -> AppResult<Vec<String>> {
        check(work)?;
        let proof = self.owner.inner.qualification.get().ok_or_else(invalid)?;
        Ok(proof.versions.clone())
    }

    pub(crate) async fn commit(self) -> AppResult<()> {
        self.tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn rollback(self) -> AppResult<()> {
        self.tx.rollback().await?;
        Ok(())
    }
}

async fn validate<C: ConnectionTrait>(db: &C) -> AppResult<Vec<String>> {
    let versions = management::schema::validate(db).await.map_err(work_error)?;
    management::schema::validate_installation_catalog(db)
        .await
        .map_err(work_error)?;
    Ok(versions)
}
