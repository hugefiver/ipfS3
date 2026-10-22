//! Management-only final-write transactions with engine cancellation and an
//! explicitly acknowledged rollback. Callers must await cancellation/cleanup.
use super::{ManagementError, Result};
use futures_util::future::BoxFuture;
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    IsolationLevel, TransactionTrait,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
pub use tokio_util::sync::CancellationToken;

#[path = "transaction/engine.rs"]
mod engine;
#[path = "transaction/invocation.rs"]
mod invocation;
#[path = "transaction/lifecycle.rs"]
mod lifecycle;
pub(crate) use invocation::{ConfirmationEngine, ConfirmationTransaction};
#[cfg(test)]
#[path = "transaction/lifetime_tests.rs"]
mod lifetime_tests;
#[cfg(test)]
#[path = "transaction/ttl_tests.rs"]
mod ttl_tests;

pub const FINAL_WRITE_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    Deadline,
    Cancelled,
    Database,
    Management(ManagementError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionFailure {
    pub cause: StopCause,
    pub rollback_confirmed: bool,
}

impl TransactionFailure {
    pub fn management_error(self) -> ManagementError {
        match self.cause {
            StopCause::Management(error) => error,
            StopCause::Deadline => ManagementError::Budget,
            StopCause::Cancelled | StopCause::Database => ManagementError::Database,
        }
    }
}

struct Control {
    end: Mutex<(Instant, bool)>,
    cancel: CancellationToken,
    changed: tokio::sync::Notify,
    active: AtomicBool,
    interrupted: AtomicBool,
}

/// One operation-wide monotonic budget; DB-clock TTL may only shorten it.
#[derive(Clone)]
pub struct Deadline(Arc<Control>);

impl Deadline {
    fn new(limit: Duration, cancel: CancellationToken) -> Self {
        Self(Arc::new(Control {
            end: Mutex::new((Instant::now() + limit.min(FINAL_WRITE_LIMIT), false)),
            cancel,
            changed: tokio::sync::Notify::new(),
            active: AtomicBool::new(true),
            interrupted: AtomicBool::new(false),
        }))
    }
    pub fn shorten(&self, remaining: Duration) {
        self.shorten_with(remaining, false);
    }
    fn shorten_with(&self, remaining: Duration, business_expiry: bool) {
        let mut end = self.0.end.lock().unwrap();
        let candidate = Instant::now() + remaining;
        if candidate < end.0 {
            *end = (candidate, business_expiry);
        }
        drop(end);
        self.0.changed.notify_waiters();
    }
    pub(crate) async fn business_expiry(
        &self,
        tx: &DatabaseTransaction,
        expiry: sea_orm::prelude::DateTimeUtc,
    ) -> Result<()> {
        let started = Instant::now();
        let now = crate::store::database_clock::database_now(tx)
            .await
            .map_err(|_| ManagementError::Database)?;
        let remaining = (expiry - now)
            .to_std()
            .map_err(|_| ManagementError::Expired)?;
        self.shorten_with(remaining.saturating_sub(started.elapsed()), true);
        if self.reason().is_some() {
            return Err(ManagementError::Expired);
        }
        engine::configure(tx, self).await
    }
    fn remaining(&self) -> Duration {
        self.0
            .end
            .lock()
            .unwrap()
            .0
            .saturating_duration_since(Instant::now())
    }
    fn reason(&self) -> Option<StopCause> {
        if self.0.cancel.is_cancelled() {
            Some(StopCause::Cancelled)
        } else {
            let end = self.0.end.lock().unwrap();
            (Instant::now() >= end.0).then_some(if end.1 {
                StopCause::Management(ManagementError::Expired)
            } else {
                StopCause::Deadline
            })
        }
    }
    fn continue_sqlite(&self) -> bool {
        !self.0.active.load(Ordering::Acquire)
            || (!self.0.interrupted.load(Ordering::Acquire) && self.reason().is_none())
    }
    fn interrupt(&self) {
        self.0.interrupted.store(true, Ordering::Release);
    }
    fn allow_cleanup(&self) {
        self.0.active.store(false, Ordering::Release);
    }
    async fn stopped(&self) -> StopCause {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(reason) = self.reason() {
                return reason;
            }
            tokio::select! {
                () = self.0.cancel.cancelled() => {},
                () = tokio::time::sleep(self.remaining()) => {},
                () = &mut changed => {},
            }
        }
    }
}

pub struct ManagementTransaction {
    shared: Arc<lifecycle::Shared>,
}

impl ManagementTransaction {
    pub async fn begin(
        db: &DatabaseConnection,
        limit: Duration,
        cancel: CancellationToken,
    ) -> std::result::Result<Self, TransactionFailure> {
        Self::begin_config(
            db,
            limit,
            cancel,
            (IsolationLevel::ReadCommitted, AccessMode::ReadWrite),
        )
        .await
    }

    /// Select the PostgreSQL read-only snapshot before configuration and backend
    /// identification issue their first SQL statements.
    pub(crate) async fn begin_read_only(
        db: &DatabaseConnection,
        limit: Duration,
        cancel: CancellationToken,
    ) -> std::result::Result<Self, TransactionFailure> {
        Self::begin_config(
            db,
            limit,
            cancel,
            (IsolationLevel::RepeatableRead, AccessMode::ReadOnly),
        )
        .await
    }

    async fn begin_config(
        db: &DatabaseConnection,
        limit: Duration,
        cancel: CancellationToken,
        (isolation, access): (IsolationLevel, AccessMode),
    ) -> std::result::Result<Self, TransactionFailure> {
        let deadline = Deadline::new(limit, cancel);
        Self::begin_engine(db, deadline, None, (isolation, access)).await
    }

    async fn begin_engine(
        db: &DatabaseConnection,
        deadline: Deadline,
        retained: Option<engine::Engine>,
        (isolation, access): (IsolationLevel, AccessMode),
    ) -> std::result::Result<Self, TransactionFailure> {
        let engine = tokio::select! {
            result = async {
                if let Some(mut engine) = retained {
                    engine.rebind(deadline.clone());
                    Ok(engine)
                } else {
                    engine::Engine::open(db, deadline.clone()).await
                }
            } => result,
            reason = deadline.stopped() => return Err(TransactionFailure { cause: reason, rollback_confirmed: true }),
        }.map_err(|_| TransactionFailure { cause: StopCause::Database, rollback_confirmed: true })?;
        let tx = tokio::select! {
            result = engine.db.begin_with_config(
                (db.get_database_backend() == DatabaseBackend::Postgres).then_some(isolation),
                (db.get_database_backend() == DatabaseBackend::Postgres).then_some(access),
            ) => result.map_err(|_| TransactionFailure { cause: StopCause::Database, rollback_confirmed: true })?,
            reason = deadline.stopped() => return Err(TransactionFailure { cause: reason, rollback_confirmed: true }),
        };
        let mut this = Self {
            shared: lifecycle::Shared::start(tx, engine, deadline),
        };
        let identity = this
            .run(|tx, deadline| {
                Box::pin(async move {
                    engine::configure(tx, &deadline).await?;
                    engine::identify(tx).await
                })
            })
            .await?;
        this.shared
            .engine
            .lock()
            .await
            .as_mut()
            .expect("active engine")
            .identity = identity;
        Ok(this)
    }

    /// Interrupt and drain SQL before rollback. An unavailable PostgreSQL control
    /// channel has bounded cleanup; only an acknowledged rollback proves stopped.
    pub async fn run<T, F>(&mut self, work: F) -> std::result::Result<T, TransactionFailure>
    where
        F: for<'a> FnOnce(&'a DatabaseTransaction, Deadline) -> BoxFuture<'a, Result<T>>,
    {
        self.run_with((), move |tx, deadline, _| work(tx, deadline))
            .await
    }

    pub async fn run_with<C, T, F>(
        &mut self,
        context: C,
        work: F,
    ) -> std::result::Result<T, TransactionFailure>
    where
        F: for<'a> FnOnce(&'a DatabaseTransaction, Deadline, &'a C) -> BoxFuture<'a, Result<T>>,
    {
        let mut state = self.shared.state.lock().await;
        if let Some(outcome) = state.finished {
            return Err(outcome.failure());
        }
        let deadline = self.shared.deadline.clone();
        if let Some(cause) = deadline
            .reason()
            .or(state.busy.then_some(StopCause::Cancelled))
        {
            state.request = Some(lifecycle::Request::Stop(cause));
            self.shared.requested.notify_one();
            drop(state);
            return Err(self.shared.finished().await.failure());
        }
        state.busy = true;
        let tx = state.tx.as_ref().expect("active management transaction");
        let mut drained = true;
        let result = {
            let operation = work(tx, deadline.clone(), &context);
            tokio::pin!(operation);
            tokio::select! {
                biased;
                reason = deadline.stopped() => {
                    deadline.interrupt();
                    drained = self.shared.engine.lock().await.as_mut().expect("active engine").drain(operation.as_mut()).await.is_some();
                    Err(reason)
                },
                result = &mut operation => match (deadline.reason(), result) {
                    (Some(reason), _) => Err(reason),
                    (None, Ok(value)) => Ok(value),
                    (None, Err(error)) => Err(StopCause::Management(error)),
                },
            }
        };
        state.busy = !drained;
        match result {
            Ok(value) => Ok(value),
            Err(cause) => {
                state.request = Some(lifecycle::Request::Stop(cause));
                self.shared.requested.notify_one();
                drop(state);
                Err(self.shared.finished().await.failure())
            }
        }
    }

    pub async fn rollback(self) -> std::result::Result<(), TransactionFailure> {
        let cause = self.shared.deadline.reason().unwrap_or(StopCause::Database);
        self.shared.request(lifecycle::Request::Stop(cause)).await;
        let failure = self.shared.finished().await.failure();
        if failure.rollback_confirmed {
            Ok(())
        } else {
            Err(failure)
        }
    }

    pub async fn commit(self) -> std::result::Result<(), TransactionFailure> {
        self.shared.request(lifecycle::Request::Commit).await;
        match self.shared.finished().await {
            lifecycle::Outcome::Committed => Ok(()),
            outcome => Err(outcome.failure()),
        }
    }
}

impl Drop for ManagementTransaction {
    fn drop(&mut self) {
        if !self.shared.done.is_cancelled() {
            self.shared.abandoned.cancel();
        }
    }
}
