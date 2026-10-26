//! PostgreSQL startup owns its connection until every dispatched response and cleanup finish.
//!
//! Same-backend recovery has a five-second cleanup budget. An unidentified or
//! unresponsive backend cannot be proved recovered within that budget; an uncertain
//! connection is disconnected instead of returned to the pool. A dispatched COMMIT
//! can have an uncertain outcome and is never retried here.
mod binding;
mod connection;
mod executor;

pub(crate) use connection::{Guarded, GuardedTransaction};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbErr};
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

const BUDGET: Duration = Duration::from_secs(60);
const CLEANUP: Duration = Duration::from_secs(5);

pub(crate) fn failed() -> DbErr {
    DbErr::Migration("PostgreSQL startup failed; database details redacted".into())
}

#[derive(Clone)]
struct Stop(Arc<StopState>);
struct StopState {
    deadline: Instant,
    reason: AtomicU8,
    wake: CancellationToken,
    stopped_at: std::sync::OnceLock<Instant>,
}
impl Stop {
    fn new(deadline: Instant) -> Self {
        Self(Arc::new(StopState {
            deadline,
            reason: AtomicU8::new(0),
            wake: CancellationToken::new(),
            stopped_at: std::sync::OnceLock::new(),
        }))
    }
    fn cancel(&self, reason: u8) {
        self.0.stopped_at.get_or_init(Instant::now);
        let _ = self
            .0
            .reason
            .compare_exchange(0, reason, Ordering::AcqRel, Ordering::Acquire);
        self.0.wake.cancel();
    }
    fn cleanup_deadline(&self) -> Instant {
        *self.0.stopped_at.get_or_init(Instant::now) + CLEANUP
    }
    fn check(&self) -> Result<(), DbErr> {
        if Instant::now() >= self.0.deadline {
            self.cancel(2);
        }
        if self.0.reason.load(Ordering::Acquire) == 0 {
            Ok(())
        } else {
            Err(self.error())
        }
    }
    fn error(&self) -> DbErr {
        if self.0.reason.load(Ordering::Acquire) == 2 {
            DbErr::Custom("PostgreSQL migration timeout failed".into())
        } else {
            failed()
        }
    }
    async fn wait(&self) {
        tokio::select! {
            _ = self.0.wake.cancelled() => {},
            _ = tokio::time::sleep_until(self.0.deadline) => self.cancel(2),
        }
    }
}
struct Caller(Stop);
impl Drop for Caller {
    fn drop(&mut self) {
        self.0.cancel(1);
    }
}

pub(super) async fn open(db: &DatabaseConnection) -> Result<(), DbErr> {
    let stop = Stop::new(Instant::now() + BUDGET);
    let caller = Caller(stop.clone());
    let pool = db.get_postgres_connection_pool().clone();
    let task = tokio::spawn(executor::own(pool, stop).with_current_subscriber());
    let result = task.await.map_err(|_| failed())?;
    drop(caller);
    result
}

async fn workflow(db: Guarded, stop: &Stop) -> Result<(), DbErr> {
    match super::classify(&db).await? {
        super::SchemaState::Current => return super::validate_catalog(&db).await,
        super::SchemaState::UpgradeRequired => return Err(super::required()),
        super::SchemaState::Empty => {}
    }
    let root = db.begin().await?;
    tracing::info!(
        migration_lock = "waiting",
        "Waiting for PostgreSQL migration lock"
    );
    if let Err(error) = db
        .execute_unprepared("SELECT pg_advisory_xact_lock(1229997651,1395879239)")
        .await
    {
        let category = if stop.0.reason.load(Ordering::Acquire) == 2 {
            "timeout"
        } else {
            "cancelled_or_failed"
        };
        tracing::warn!(
            migration_lock = "failure",
            category,
            "PostgreSQL migration lock failed"
        );
        return Err(error);
    }
    tracing::info!(
        migration_lock = "acquired",
        "Acquired PostgreSQL migration lock"
    );
    if super::names(&db).await?.is_empty() {
        crate::store::migrations::bridge::initialize(&db).await?;
    }
    super::validate_existing(&db).await?;
    root.commit().await
}
