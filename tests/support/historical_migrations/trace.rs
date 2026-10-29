use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DbErr, ExecResult, QueryResult, Statement,
};

struct InstallationTrace<'a> {
    db: &'a DatabaseConnection,
    statements: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl ConnectionTrait for InstallationTrace<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.db.get_database_backend()
    }
    async fn execute(&self, sql: Statement) -> Result<ExecResult, DbErr> {
        self.statements.lock().unwrap().push(sql.sql.clone());
        self.db.execute(sql).await
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.statements.lock().unwrap().push(sql.into());
        self.db.execute_unprepared(sql).await
    }
    async fn query_one(&self, sql: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.db.query_one(sql).await
    }
    async fn query_all(&self, sql: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.db.query_all(sql).await
    }
}

/// Exercise the shared installer on the real backend, including raw DDL that
/// SeaORM's prepared-statement metric callback does not observe.
pub async fn baseline_install_statements(db: &DatabaseConnection) -> Vec<String> {
    let trace = InstallationTrace {
        db,
        statements: std::sync::Mutex::default(),
    };
    crate::store::migrations::baseline::install(&trace)
        .await
        .unwrap();
    trace.statements.into_inner().unwrap()
}
