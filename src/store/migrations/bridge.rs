//! The native migrator and guarded empty startup execute the same migration bodies.
use crate::store::initialization::postgres::Guarded;
#[cfg(any(test, feature = "historical-fixtures"))]
use crate::store::initialization::postgres::GuardedTransaction;
#[cfg(any(test, feature = "historical-fixtures"))]
use sea_orm::sea_query::{IndexCreateStatement, TableAlterStatement, TableCreateStatement};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};
#[cfg(any(test, feature = "historical-fixtures"))]
use sea_orm::{DatabaseTransaction, StatementBuilder, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager, SchemaManagerConnection};

#[async_trait::async_trait]
pub(crate) trait SharedMigration: MigrationTrait {
    async fn up_body(&self, manager: &Manager<'_>) -> Result<(), DbErr>;
}

pub(crate) struct Manager<'a> {
    #[cfg(any(test, feature = "historical-fixtures"))]
    native: Option<&'a SchemaManager<'a>>,
    connection: Connection<'a>,
}
impl<'a> Manager<'a> {
    pub(crate) fn native(manager: &'a SchemaManager<'a>) -> Self {
        Self {
            #[cfg(any(test, feature = "historical-fixtures"))]
            native: Some(manager),
            connection: Connection::Native(manager.get_connection()),
        }
    }
    pub(crate) fn guarded(db: Guarded) -> Self {
        Self {
            #[cfg(any(test, feature = "historical-fixtures"))]
            native: None,
            connection: Connection::Guarded(db),
        }
    }
    pub(crate) fn get_connection(&self) -> &Connection<'a> {
        &self.connection
    }
}

#[cfg(any(test, feature = "historical-fixtures"))]
impl Manager<'_> {
    pub(crate) fn get_database_backend(&self) -> DatabaseBackend {
        self.connection.get_database_backend()
    }
    async fn execute<S: StatementBuilder>(&self, statement: S) -> Result<(), DbErr> {
        self.connection
            .execute(self.get_database_backend().build(&statement))
            .await
            .map(|_| ())
    }
    pub(crate) async fn create_table(&self, statement: TableCreateStatement) -> Result<(), DbErr> {
        if let Some(manager) = self.native {
            manager.create_table(statement).await
        } else {
            self.execute(statement).await
        }
    }
    pub(crate) async fn create_index(&self, statement: IndexCreateStatement) -> Result<(), DbErr> {
        if let Some(manager) = self.native {
            manager.create_index(statement).await
        } else {
            self.execute(statement).await
        }
    }
    pub(crate) async fn alter_table(&self, statement: TableAlterStatement) -> Result<(), DbErr> {
        if let Some(manager) = self.native {
            manager.alter_table(statement).await
        } else {
            self.execute(statement).await
        }
    }
    pub(crate) async fn has_table<T: AsRef<str>>(&self, table: T) -> Result<bool, DbErr> {
        if let Some(manager) = self.native {
            return manager.has_table(table).await;
        }
        let row = self.connection.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE' AND table_name=$1) AS present", [table.as_ref().into()])).await?;
        row.ok_or_else(crate::store::initialization::postgres::failed)?
            .try_get("", "present")
    }
    pub(crate) async fn has_column<T: AsRef<str>, C: AsRef<str>>(
        &self,
        table: T,
        column: C,
    ) -> Result<bool, DbErr> {
        if let Some(manager) = self.native {
            return manager.has_column(table, column).await;
        }
        let row = self.connection.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2) AS present", [table.as_ref().into(), column.as_ref().into()])).await?;
        row.ok_or_else(crate::store::initialization::postgres::failed)?
            .try_get("", "present")
    }
}

pub(crate) enum Connection<'a> {
    Native(&'a SchemaManagerConnection<'a>),
    Guarded(Guarded),
}
impl Connection<'_> {
    fn inner(&self) -> &dyn ConnectionTrait {
        match self {
            Self::Native(db) => *db,
            Self::Guarded(db) => db,
        }
    }
    #[cfg(any(test, feature = "historical-fixtures"))]
    pub(crate) async fn begin(&self) -> Result<Transaction, DbErr> {
        match self {
            Self::Native(db) => Ok(Transaction::Native(db.begin().await?)),
            Self::Guarded(db) => Ok(Transaction::Guarded(db.begin().await?)),
        }
    }
}
#[cfg(any(test, feature = "historical-fixtures"))]
pub(crate) enum Transaction {
    Native(DatabaseTransaction),
    Guarded(GuardedTransaction),
}
#[cfg(any(test, feature = "historical-fixtures"))]
impl From<DatabaseTransaction> for Transaction {
    fn from(db: DatabaseTransaction) -> Self {
        Self::Native(db)
    }
}
#[cfg(any(test, feature = "historical-fixtures"))]
impl Transaction {
    fn inner(&self) -> &dyn ConnectionTrait {
        match self {
            Self::Native(db) => db,
            Self::Guarded(db) => db.connection(),
        }
    }
    pub(crate) async fn commit(self) -> Result<(), DbErr> {
        match self {
            Self::Native(db) => db.commit().await,
            Self::Guarded(db) => db.commit().await,
        }
    }
    pub(crate) async fn rollback(self) -> Result<(), DbErr> {
        match self {
            Self::Native(db) => db.rollback().await,
            Self::Guarded(db) => db.rollback().await,
        }
    }
}

macro_rules! delegate_connection {
    ($type:ty) => {
        #[async_trait::async_trait]
        impl ConnectionTrait for $type {
            fn get_database_backend(&self) -> DatabaseBackend {
                self.inner().get_database_backend()
            }
            async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
                self.inner().execute(statement).await
            }
            async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
                self.inner().execute_unprepared(sql).await
            }
            async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
                self.inner().query_one(statement).await
            }
            async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
                self.inner().query_all(statement).await
            }
        }
    };
}
delegate_connection!(Connection<'_>);
#[cfg(any(test, feature = "historical-fixtures"))]
delegate_connection!(Transaction);

pub(crate) async fn initialize(db: &Guarded) -> Result<(), DbErr> {
    use sea_orm::{EntityTrait, Set};
    use sea_orm_migration::{MigratorTrait, seaql_migrations};
    crate::store::migrator::Migrator::install(db).await?;
    let manager = Manager::guarded(db.clone());
    for migration in super::registry::shared() {
        migration.up_body(&manager).await?;
        let applied_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| crate::store::initialization::postgres::failed())?
            .as_secs() as i64;
        seaql_migrations::Entity::insert(seaql_migrations::ActiveModel {
            version: Set(migration.name().to_owned()),
            applied_at: Set(applied_at),
        })
        .exec(db)
        .await?;
    }
    Ok(())
}
