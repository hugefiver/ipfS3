use super::{
    Stop, binding,
    connection::{Command, Guarded, Request, Response},
    failed,
};
use sea_orm::DbErr;
use sqlx::{Connection, Executor, PgConnection, PgPool, Row};
use std::future::Future;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

#[path = "ownership.rs"]
mod ownership;
use ownership::{OwnedMain, close_until};

struct Identity {
    pid: i32,
    started: chrono::DateTime<chrono::Utc>,
    database: String,
    role: String,
    statement_timeout: String,
    lock_timeout: String,
}

pub(super) async fn own(pool: PgPool, stop: Stop) -> Result<(), DbErr> {
    let workflow_stop = stop.clone();
    own_work(pool, stop, |db| async move {
        super::workflow(db, &workflow_stop).await
    })
    .await
}

async fn own_work<F, W>(pool: PgPool, stop: Stop, work: F) -> Result<(), DbErr>
where
    F: FnOnce(Guarded) -> W,
    W: Future<Output = Result<(), DbErr>>,
{
    let main = tokio::select! {
        biased;
        _ = stop.wait() => return Err(stop.error()),
        result = pool.acquire() => result.map_err(|_| failed())?,
    };
    let options = pool.connect_options();
    let mut control = tokio::select! {
        biased;
        _ = stop.wait() => return Err(stop.error()),
        result = PgConnection::connect_with(&options) => result.map_err(|_| failed())?,
    };
    // This connection never executes business SQL. Its server limits also bound a lost
    // cancellation response; they do not shorten any startup statement's budget.
    let setup = async {
        stop.check()?;
        let query = control.execute("SET statement_timeout='1s'; SET lock_timeout='1s'");
        tokio::pin!(query);
        let result = tokio::select! {
            biased;
            result = &mut query => result,
            _ = stop.wait() => {
                tokio::time::timeout_at(stop.cleanup_deadline(), &mut query)
                    .await.map_err(|_| failed())?
            }
        };
        result.map_err(|_| failed())?;
        stop.check()
    };
    if let Err(error) = setup.await {
        let _ = close_until(stop.cleanup_deadline(), control.close_hard()).await;
        return Err(error);
    }
    let mut main = OwnedMain::new(main);
    let identity = identify(&mut main, &stop).await;
    let identity = match identity {
        Ok(identity) => identity,
        Err(error) => {
            let deadline = stop.cleanup_deadline();
            let raw = main.detach();
            let _ = close_until(deadline, raw.close_hard()).await;
            let _ = close_until(deadline, control.close_hard()).await;
            return Err(error);
        }
    };
    let (db, requests) = Guarded::channel(stop.clone());
    let mut executor = Startup {
        main,
        control,
        identity,
        stop: stop.clone(),
        poisoned: false,
        stack: Vec::new(),
        next: 0,
        commit_dispatched: false,
    };
    let (work, served) = tokio::join!(work(db), executor.serve(requests));
    let uncertain_commit =
        executor.commit_dispatched && (work.is_err() || served.is_err() || stop.check().is_err());
    let cleanup = executor.finish().await;
    if uncertain_commit {
        return Err(DbErr::Migration(
            "PostgreSQL startup COMMIT outcome is uncertain; do not retry automatically".into(),
        ));
    }
    cleanup?;
    stop.check()?;
    served?;
    work
}

#[cfg(test)]
#[path = "executor_tests.rs"]
mod tests;

async fn identify(main: &mut PgConnection, stop: &Stop) -> Result<Identity, DbErr> {
    stop.check()?;
    let query = sqlx::query("SELECT pg_backend_pid() AS pid, backend_start, current_database() AS database, usename AS role, current_setting('statement_timeout') AS statement_timeout, current_setting('lock_timeout') AS lock_timeout FROM pg_catalog.pg_stat_activity WHERE pid=pg_backend_pid()").fetch_all(main);
    tokio::pin!(query);
    let rows = tokio::select! {
        biased;
        rows = &mut query => rows,
        _ = stop.wait() => {
            // Until identity arrives we cannot safely address a cancel request.
            tokio::time::timeout_at(stop.cleanup_deadline(), &mut query).await.map_err(|_| failed())?
        }
    }.map_err(|_| failed())?;
    let row = rows.first().ok_or_else(failed)?;
    Ok(Identity {
        pid: row.try_get("pid").map_err(|_| failed())?,
        started: row.try_get("backend_start").map_err(|_| failed())?,
        database: row.try_get("database").map_err(|_| failed())?,
        role: row.try_get("role").map_err(|_| failed())?,
        statement_timeout: row.try_get("statement_timeout").map_err(|_| failed())?,
        lock_timeout: row.try_get("lock_timeout").map_err(|_| failed())?,
    })
}

struct Startup {
    main: OwnedMain,
    control: PgConnection,
    identity: Identity,
    stop: Stop,
    poisoned: bool,
    stack: Vec<u64>,
    next: u64,
    commit_dispatched: bool,
}
impl Startup {
    async fn serve(&mut self, mut requests: mpsc::Receiver<Request>) -> Result<(), DbErr> {
        while let Some(request) = requests.recv().await {
            let result = self.dispatch(request.command).await;
            let failed = result.is_err();
            let _ = request.response.send(result);
            if failed {
                return Err(super::failed());
            }
        }
        Ok(())
    }

    async fn sql(&mut self, sql: &str) -> Result<sqlx::postgres::PgQueryResult, DbErr> {
        controlled(
            self.main.execute(sql),
            &mut self.control,
            &self.identity,
            &self.stop,
            &mut self.poisoned,
        )
        .await
    }

    async fn configure(&mut self) -> Result<(), DbErr> {
        self.stop.check()?;
        let millis = self
            .stop
            .0
            .deadline
            .saturating_duration_since(Instant::now())
            .as_nanos()
            .div_ceil(1_000_000)
            .max(1);
        self.sql(&format!(
            "SET statement_timeout='{millis}ms'; SET lock_timeout='{millis}ms'"
        ))
        .await?;
        self.stop.check()
    }

    async fn dispatch(&mut self, command: Command) -> Result<Response, DbErr> {
        self.stop.check()?;
        // An aborted transaction must be rolled back directly, without SET first.
        if !matches!(command, Command::Rollback(_)) {
            self.configure().await?;
        }
        self.stop.check()?;
        match command {
            Command::Unprepared(sql) => Ok(Response::Executed(self.sql(&sql).await?.into())),
            Command::Execute(statement) => {
                let args = binding::arguments(&statement)?;
                let result = controlled(
                    sqlx::query_with(&statement.sql, args).execute(&mut *self.main),
                    &mut self.control,
                    &self.identity,
                    &self.stop,
                    &mut self.poisoned,
                )
                .await?;
                Ok(Response::Executed(result.into()))
            }
            Command::Query(statement) => {
                let args = binding::arguments(&statement)?;
                let rows = controlled(
                    sqlx::query_with(&statement.sql, args).fetch_all(&mut *self.main),
                    &mut self.control,
                    &self.identity,
                    &self.stop,
                    &mut self.poisoned,
                )
                .await?;
                Ok(Response::Rows(rows.into_iter().map(Into::into).collect()))
            }
            Command::Begin => {
                self.next += 1;
                let id = self.next;
                let sql = if self.stack.is_empty() {
                    "BEGIN".to_owned()
                } else {
                    format!("SAVEPOINT startup_{id}")
                };
                self.stack.push(id);
                self.sql(&sql).await?;
                Ok(Response::Transaction(id))
            }
            Command::Commit(id) => {
                if self.stack.last() != Some(&id) {
                    return Err(failed());
                }
                if self.stack.len() == 1 {
                    let commit = async {
                        // The flag is set on first poll, after the dispatch stop check.
                        // Once this future starts, a lost response cannot prove rollback.
                        self.commit_dispatched = true;
                        self.main.execute("COMMIT").await
                    };
                    controlled(
                        commit,
                        &mut self.control,
                        &self.identity,
                        &self.stop,
                        &mut self.poisoned,
                    )
                    .await?;
                } else {
                    self.sql(&format!("RELEASE SAVEPOINT startup_{id}")).await?;
                }
                self.stack.pop();
                Ok(Response::Done)
            }
            Command::Rollback(id) => {
                if self.stack.last() != Some(&id) {
                    return Err(failed());
                }
                let sql = if self.stack.len() == 1 {
                    "ROLLBACK".to_owned()
                } else {
                    format!("ROLLBACK TO SAVEPOINT startup_{id}; RELEASE SAVEPOINT startup_{id}")
                };
                self.sql(&sql).await?;
                self.stack.pop();
                Ok(Response::Done)
            }
        }
    }

    async fn finish(mut self) -> Result<(), DbErr> {
        // Only non-poisoned connections have consumed every cancellation response.
        // An uncertain connection must not run cleanup SQL or return to the pool.
        let deadline = self.stop.cleanup_deadline();
        let result = if self.poisoned {
            Err(failed())
        } else {
            let cleanup = async {
                if !self.stack.is_empty() {
                    self.main.execute("ROLLBACK").await.map_err(|_| failed())?;
                }
                sqlx::query("SELECT set_config('statement_timeout',$1,false), set_config('lock_timeout',$2,false)")
                .bind(&self.identity.statement_timeout).bind(&self.identity.lock_timeout)
                .fetch_all(&mut *self.main).await.map_err(|_| failed())?;
                Ok::<_, DbErr>(())
            };
            tokio::time::timeout_at(deadline, cleanup)
                .await
                .map_err(|_| failed())
                .and_then(|r| r)
        };
        if result.is_err() {
            let raw = self.main.detach();
            let _ = close_until(deadline, raw.close_hard()).await;
        }
        close_until(deadline, self.control.close()).await?;
        result?;
        self.main.release_verified();
        Ok(())
    }
}

async fn cancel(control: &mut PgConnection, identity: &Identity) -> Result<bool, sqlx::Error> {
    // Identity matching reduces, but cannot atomically eliminate, the disconnect/PID
    // reuse race between the catalog check and pg_cancel_backend. The control uses
    // the same DB role; a successful signal alone never proves response or rollback.
    let rows = sqlx::query("SELECT CASE WHEN state='active' THEN pg_catalog.pg_cancel_backend(pid) ELSE false END AS signalled FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND backend_start=$2 AND datname=$3 AND usename=$4")
        .bind(identity.pid).bind(identity.started).bind(&identity.database).bind(&identity.role)
        .fetch_all(control).await?;
    // Missing/mismatched identity must never be followed by a delayed cancel.
    Ok(!rows.is_empty())
}

async fn controlled<F, T>(
    future: F,
    control: &mut PgConnection,
    identity: &Identity,
    stop: &Stop,
    poisoned: &mut bool,
) -> Result<T, DbErr>
where
    F: Future<Output = Result<T, sqlx::Error>>,
{
    stop.check()?;
    tokio::pin!(future);
    tokio::select! {
        biased;
        result = &mut future => {
            let result = result.map_err(|_| failed());
            stop.check()?;
            return result;
        }
        _ = stop.wait() => {},
    }
    let deadline = stop.cleanup_deadline();
    let drain = async {
        loop {
            let started = std::sync::atomic::AtomicBool::new(false);
            let cancellation = async {
                started.store(true, std::sync::atomic::Ordering::Release);
                cancel(control, identity).await
            };
            tokio::pin!(cancellation);
            tokio::select! {
                biased;
                result = &mut future => {
                    // Even a command that just completed may have a cancel in flight.
                    if started.load(std::sync::atomic::Ordering::Acquire) && cancellation.await.is_err() { *poisoned = true; }
                    return result;
                }
                result = &mut cancellation => {
                    match result {
                        Ok(true) => {},
                        Ok(false) | Err(_) => {
                            *poisoned = true;
                            return future.await;
                        }
                    }
                }
            }
            tokio::select! {
                result = &mut future => return result,
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            }
        }
    };
    if tokio::time::timeout_at(deadline, drain).await.is_err() {
        *poisoned = true;
    }
    Err(stop.error())
}
