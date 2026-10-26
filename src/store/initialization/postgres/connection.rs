use super::{Stop, failed};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};
use tokio::sync::{mpsc, oneshot};

pub(super) enum Command {
    Execute(Statement),
    Unprepared(String),
    Query(Statement),
    Begin,
    Commit(u64),
    Rollback(u64),
}
pub(super) enum Response {
    Executed(ExecResult),
    Rows(Vec<QueryResult>),
    Transaction(u64),
    Done,
}
pub(super) struct Request {
    pub command: Command,
    pub response: oneshot::Sender<Result<Response, DbErr>>,
}

#[derive(Clone)]
pub(crate) struct Guarded {
    sender: mpsc::Sender<Request>,
    stop: Stop,
}
impl Guarded {
    pub(super) fn channel(stop: Stop) -> (Self, mpsc::Receiver<Request>) {
        let (sender, receiver) = mpsc::channel(1);
        (Self { sender, stop }, receiver)
    }
    async fn request(&self, command: Command) -> Result<Response, DbErr> {
        self.stop.check()?;
        let (response, receive) = oneshot::channel();
        self.sender
            .send(Request { command, response })
            .await
            .map_err(|_| failed())?;
        receive.await.map_err(|_| failed())?
    }
    pub(crate) async fn begin(&self) -> Result<GuardedTransaction, DbErr> {
        match self.request(Command::Begin).await? {
            Response::Transaction(id) => Ok(GuardedTransaction {
                connection: self.clone(),
                id,
                finished: false,
            }),
            _ => Err(failed()),
        }
    }
}

pub(crate) struct GuardedTransaction {
    connection: Guarded,
    id: u64,
    finished: bool,
}
impl GuardedTransaction {
    pub(crate) fn connection(&self) -> &Guarded {
        &self.connection
    }
    pub(crate) async fn commit(mut self) -> Result<(), DbErr> {
        self.connection.request(Command::Commit(self.id)).await?;
        self.finished = true;
        Ok(())
    }
    pub(crate) async fn rollback(mut self) -> Result<(), DbErr> {
        self.connection.request(Command::Rollback(self.id)).await?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for GuardedTransaction {
    fn drop(&mut self) {
        if !self.finished {
            self.connection.stop.cancel(3);
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for Guarded {
    fn get_database_backend(&self) -> DatabaseBackend {
        DatabaseBackend::Postgres
    }
    async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        match self.request(Command::Execute(stmt)).await? {
            Response::Executed(result) => Ok(result),
            _ => Err(failed()),
        }
    }
    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        match self.request(Command::Unprepared(sql.to_owned())).await? {
            Response::Executed(result) => Ok(result),
            _ => Err(failed()),
        }
    }
    async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        // fetch_all drains the full response even when only the first row is wanted.
        Ok(self.query_all(stmt).await?.into_iter().next())
    }
    async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match self.request(Command::Query(stmt)).await? {
            Response::Rows(rows) => Ok(rows),
            _ => Err(failed()),
        }
    }
}
