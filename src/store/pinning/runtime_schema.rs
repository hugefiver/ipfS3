//! Store-owned SQLite layout qualification. External DDL requires stop/restart.
//! Versions, readiness, TEMP shadows and business authority are never cached.
use super::management::{self, admission::work_error, native, records::DecodeWork};
use crate::{
    error::{AppError, AppResult},
    store::Store,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, TransactionTrait};

mod control;
mod meter;
#[cfg(test)]
mod tests;

/// Only a completed validation on the enclosing Store's own connection can
/// construct this proof. Retained controls contain compiled expectations only.
pub(in crate::store) struct StaticQualification {
    control: control::Compiled,
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

    /// Qualify SQLite once per Store identity, before checking out any business
    /// transaction. PostgreSQL deliberately receives no reusable layout proof.
    pub(crate) async fn qualify_runtime_schema(&self, work: DecodeWork<'_>) -> AppResult<()> {
        check(work)?;
        if self.db().get_database_backend() == DatabaseBackend::Postgres {
            return Ok(());
        }
        if self.db().get_database_backend() != DatabaseBackend::Sqlite {
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
                let control = control::Compiled::new();
                let validation = async {
                    check(work)?;
                    let db = meter::SqliteRead::new(&tx, work);
                    // Reject shadows before any validator resolves control names.
                    control::validate(&db, &control).await?;
                    let versions = super::legacy_migration::receipt_schema(&db).await?;
                    if versions.last().map(String::as_str) != Some(management::schema::VERSION) {
                        return Err(invalid());
                    }
                    management::catalog::validate(&db, native::current_catalog())
                        .await
                        .map_err(work_error)?;
                    check(work)
                }
                .await;
                rollback(tx, work).await?;
                validation?;
                // The initializer's cancellation/deadline is checked after the
                // normal rollback and before OnceCell can publish the proof.
                check(work)?;
                Ok::<StaticQualification, AppError>(StaticQualification { control })
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
        if self.tx.get_database_backend() == DatabaseBackend::Sqlite {
            super::legacy_migration::ReceiptCatalog::Runtime(self, work)
        } else {
            super::legacy_migration::ReceiptCatalog::full(&self.tx)
        }
    }
    pub(crate) fn db(&self) -> &DatabaseTransaction {
        &self.tx
    }

    /// Invoke after the caller's writer fence. Only static SQLite definitions
    /// are reused; control values and connection-local TEMP objects stay live.
    pub(crate) async fn check_schema(&self, work: DecodeWork<'_>) -> AppResult<()> {
        self.validate(work).await
    }

    async fn validate(&self, work: DecodeWork<'_>) -> AppResult<()> {
        check(work)?;
        let result = if self.tx.get_database_backend() == DatabaseBackend::Sqlite {
            let proof = self.owner.inner.qualification.get().ok_or_else(invalid)?;
            let db = meter::SqliteRead::new(&self.tx, work);
            control::validate(&db, &proof.control).await
        } else {
            self.postgres_versions().await.map(|_| ())
        };
        check(work)?;
        result
    }

    pub(crate) async fn schema_versions(&self, work: DecodeWork<'_>) -> AppResult<Vec<String>> {
        if self.tx.get_database_backend() == DatabaseBackend::Sqlite {
            self.validate(work).await?;
            let proof = self.owner.inner.qualification.get().ok_or_else(invalid)?;
            Ok(proof.control.known.clone())
        } else {
            check(work)?;
            let result = self.postgres_versions().await;
            check(work)?;
            result
        }
    }

    async fn postgres_versions(&self) -> AppResult<Vec<String>> {
        // PostgreSQL has no qualification shortcut: retain the full validators.
        let versions = super::legacy_migration::receipt_schema(&self.tx).await?;
        management::schema::validate(&self.tx)
            .await
            .map_err(work_error)?;
        management::catalog::validate(&self.tx, native::current_catalog())
            .await
            .map_err(work_error)?;
        Ok(versions)
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
