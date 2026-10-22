use super::*;
use sea_orm::Statement;
use tokio::sync::{Mutex as AsyncMutex, Notify};

#[derive(Clone, Copy)]
pub(super) enum Request {
    Commit,
    Rollback,
    Stop(StopCause),
}

#[derive(Clone, Copy)]
pub(super) enum Outcome {
    Committed,
    Failed(TransactionFailure),
}

impl Outcome {
    pub fn failure(self) -> TransactionFailure {
        match self {
            Self::Failed(failure) => failure,
            Self::Committed => TransactionFailure {
                cause: StopCause::Database,
                rollback_confirmed: false,
            },
        }
    }
}

pub(super) struct State {
    pub tx: Option<DatabaseTransaction>,
    pub busy: bool,
    pub request: Option<Request>,
    pub finished: Option<Outcome>,
    pub reclaimable: bool,
}

pub(super) struct Shared {
    pub state: AsyncMutex<State>,
    pub engine: AsyncMutex<Option<engine::Engine>>,
    pub deadline: Deadline,
    pub requested: Notify,
    pub abandoned: CancellationToken,
    pub done: CancellationToken,
}

impl Shared {
    pub fn start(tx: DatabaseTransaction, engine: engine::Engine, deadline: Deadline) -> Arc<Self> {
        let shared = Arc::new(Self {
            state: AsyncMutex::new(State {
                tx: Some(tx),
                busy: false,
                request: None,
                finished: None,
                reclaimable: false,
            }),
            engine: AsyncMutex::new(Some(engine)),
            deadline,
            requested: Notify::new(),
            abandoned: CancellationToken::new(),
            done: CancellationToken::new(),
        });
        tokio::spawn(shared.clone().watch());
        shared
    }

    pub async fn request(&self, request: Request) {
        let mut state = self.state.lock().await;
        if state.finished.is_none() && state.request.is_none() {
            state.request = Some(request);
            self.requested.notify_one();
        }
    }

    pub async fn finished(&self) -> Outcome {
        self.done.cancelled().await;
        self.state
            .lock()
            .await
            .finished
            .expect("watchdog records completion before acknowledging")
    }

    // This task alone consumes the transaction. Dropping a caller's future does
    // not drop a rollback/commit acknowledgement that is already in progress.
    async fn watch(self: Arc<Self>) {
        let forced = tokio::select! {
            biased;
            () = self.abandoned.cancelled() => Some(StopCause::Cancelled),
            cause = self.deadline.stopped() => Some(cause),
            () = self.requested.notified() => None,
        };
        if forced.is_some() {
            self.deadline.interrupt();
        }
        let mut state = self.state.lock().await;
        let cause = forced.or_else(|| self.deadline.reason());
        let request = if let Some(cause) = cause {
            Request::Stop(cause)
        } else {
            state.request.unwrap_or(Request::Stop(StopCause::Cancelled))
        };
        let outcome = match request {
            Request::Rollback => self.stop(&mut state, StopCause::Database).await,
            Request::Stop(cause) => self.stop(&mut state, cause).await,
            Request::Commit if state.busy => self.stop(&mut state, StopCause::Cancelled).await,
            Request::Commit => self.commit(&mut state).await,
        };
        state.reclaimable |= matches!(outcome, Outcome::Committed);
        state.finished = Some(outcome);
        self.done.cancel();
    }

    async fn stop(&self, state: &mut State, cause: StopCause) -> Outcome {
        self.deadline.interrupt();
        let tx = state
            .tx
            .take()
            .expect("watchdog exclusively owns transaction completion");
        let mut engine_guard = self.engine.lock().await;
        let engine = engine_guard.as_mut().expect("watchdog owns engine");
        // A command on this exact connection is ordered after any worker SQL
        // whose Rust future was dropped. Keep SQLite interruption armed until
        // this barrier actually acknowledges that the engine reached it.
        let settled = {
            let barrier = tx.query_one(Statement::from_string(
                tx.get_database_backend(),
                "SELECT 1",
            ));
            tokio::pin!(barrier);
            engine.drain(barrier.as_mut()).await
        };
        if matches!(settled, Some(Ok(_))) && tx.get_database_backend() == DatabaseBackend::Sqlite {
            self.deadline.allow_cleanup();
        }
        // Retain the actual DB command if its acknowledgement becomes unknown;
        // timing out the observer must not cancel the rollback command itself.
        let mut rollback = tokio::spawn(async move { tx.rollback().await });
        let rollback_confirmed = matches!(
            tokio::time::timeout(Duration::from_secs(2), &mut rollback).await,
            Ok(Ok(Ok(())))
        );
        state.reclaimable = matches!(state.request, Some(Request::Rollback))
            && !state.busy
            && self.deadline.reason().is_none()
            && !self.abandoned.is_cancelled()
            && matches!(settled, Some(Ok(_)))
            && rollback_confirmed;
        Outcome::Failed(TransactionFailure {
            cause,
            rollback_confirmed,
        })
    }

    async fn commit(&self, state: &mut State) -> Outcome {
        if let Some(cause) = self.deadline.reason() {
            return self.stop(state, cause).await;
        }
        let tx = state
            .tx
            .take()
            .expect("watchdog exclusively owns transaction completion");
        let mut pending = tokio::spawn(async move { tx.commit().await });
        tokio::select! {
            result = &mut pending => match result {
                Ok(Ok(())) => Outcome::Committed,
                _ => Outcome::Failed(TransactionFailure { cause: StopCause::Database, rollback_confirmed: false }),
            },
            cause = self.deadline.stopped() => {
                self.deadline.interrupt();
                let _ = self.engine.lock().await.as_mut().expect("watchdog owns engine").drain(std::pin::Pin::new(&mut pending)).await;
                Outcome::Failed(TransactionFailure { cause, rollback_confirmed: false })
            },
        }
    }
}
