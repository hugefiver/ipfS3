use super::*;
use sea_orm::sqlx::{self, postgres::PgPoolOptions, sqlite::SqlitePoolOptions};
use sea_orm::{ConnectionTrait, SqlxPostgresConnector, SqlxSqliteConnector, Statement};

pub(super) struct Engine {
    pub db: DatabaseConnection,
    control: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    pub identity: Option<(i32, String)>,
    sqlite_deadline: Option<Arc<Mutex<Deadline>>>,
}

impl Engine {
    pub async fn open(db: &DatabaseConnection, deadline: Deadline) -> Result<Self> {
        match db.get_database_backend() {
            DatabaseBackend::Sqlite => {
                if db.get_sqlite_connection_pool().is_closed() {
                    return Err(ManagementError::Database);
                }
                let options = db
                    .get_sqlite_connection_pool()
                    .connect_options()
                    .as_ref()
                    .clone()
                    .busy_timeout(Duration::from_millis(25));
                let sqlite_deadline = Arc::new(Mutex::new(deadline.clone()));
                let current = sqlite_deadline.clone();
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(deadline.remaining())
                    .after_connect(move |connection, _| {
                        let current = current.clone();
                        Box::pin(async move {
                            connection
                                .lock_handle()
                                .await?
                                .set_progress_handler(1000, move || {
                                    current.lock().unwrap().continue_sqlite()
                                });
                            Ok(())
                        })
                    })
                    .connect_with(options)
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(Self {
                    db: SqlxSqliteConnector::from_sqlx_sqlite_pool(pool),
                    control: None,
                    identity: None,
                    sqlite_deadline: Some(sqlite_deadline),
                })
            }
            DatabaseBackend::Postgres => {
                if db.get_postgres_connection_pool().is_closed() {
                    return Err(ManagementError::Database);
                }
                let options = db
                    .get_postgres_connection_pool()
                    .connect_options()
                    .as_ref()
                    .clone();
                let pool = PgPoolOptions::new()
                    .max_connections(2)
                    .acquire_timeout(deadline.remaining())
                    .connect_with(options)
                    .await
                    .map_err(|_| ManagementError::Database)?;
                // Hold an independent cancellation connection before begin. This
                // never depends on spare capacity in the application's pool.
                let control = pool
                    .acquire()
                    .await
                    .map_err(|_| ManagementError::Database)?;
                Ok(Self {
                    db: SqlxPostgresConnector::from_sqlx_postgres_pool(pool),
                    control: Some(control),
                    identity: None,
                    sqlite_deadline: None,
                })
            }
            _ => Err(ManagementError::Database),
        }
    }

    pub fn rebind(&mut self, deadline: Deadline) {
        *self
            .sqlite_deadline
            .as_ref()
            .expect("SQLite invocation engine")
            .lock()
            .unwrap() = deadline;
    }

    pub async fn drain<F: std::future::Future>(
        &mut self,
        pending: std::pin::Pin<&mut F>,
    ) -> Option<F::Output> {
        let canceller = async {
            loop {
                if let (Some(control), Some((pid, birth))) = (&mut self.control, &self.identity) {
                    let cancel = sqlx::query("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND backend_start=$2::timestamptz")
                        .bind(pid).bind(birth).execute(&mut **control);
                    let _ = tokio::time::timeout(Duration::from_secs(1), cancel).await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            result = pending => Some(result),
            () = canceller => None,
            () = tokio::time::sleep(Duration::from_millis(200)) => None,
        }
    }
}

pub(super) async fn configure(tx: &DatabaseTransaction, deadline: &Deadline) -> Result<()> {
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        let millis = deadline
            .remaining()
            .as_millis()
            .saturating_add(10)
            .clamp(1, 30_010);
        tx.execute_unprepared(&format!(
            "SET LOCAL statement_timeout='{millis}ms'; SET LOCAL lock_timeout='{millis}ms'"
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    }
    Ok(())
}

pub(super) async fn identify(tx: &DatabaseTransaction) -> Result<Option<(i32, String)>> {
    if tx.get_database_backend() != DatabaseBackend::Postgres {
        return Ok(None);
    }
    let row = tx.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT pid, backend_start::text AS birth FROM pg_stat_activity WHERE pid=pg_backend_pid()"))
        .await.map_err(|_| ManagementError::Database)?.ok_or(ManagementError::Database)?;
    Ok(Some((
        row.try_get("", "pid")
            .map_err(|_| ManagementError::Database)?,
        row.try_get("", "birth")
            .map_err(|_| ManagementError::Database)?,
    )))
}
