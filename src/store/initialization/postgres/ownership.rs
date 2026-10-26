use super::failed;
use sea_orm::DbErr;
use sqlx::{PgConnection, Postgres, pool::PoolConnection};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
};
use tokio::time::Instant;

pub(super) struct OwnedMain(Option<PoolConnection<Postgres>>);

impl OwnedMain {
    pub(super) fn new(connection: PoolConnection<Postgres>) -> Self {
        Self(Some(connection))
    }

    pub(super) fn detach(&mut self) -> PgConnection {
        self.0.take().expect("owned startup connection").detach()
    }

    pub(super) fn release_verified(mut self) {
        drop(self.0.take().expect("owned startup connection"));
    }
}

impl Deref for OwnedMain {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.0.as_deref().expect("owned startup connection")
    }
}

impl DerefMut for OwnedMain {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_deref_mut().expect("owned startup connection")
    }
}

impl Drop for OwnedMain {
    fn drop(&mut self) {
        if let Some(connection) = self.0.take() {
            // Manual startup transactions have no SQLx transaction Drop rollback.
            drop(connection.detach());
        }
    }
}

pub(super) async fn close_until<F>(deadline: Instant, close: F) -> Result<(), DbErr>
where
    F: Future<Output = Result<(), sqlx::Error>>,
{
    tokio::time::timeout_at(deadline, close)
        .await
        .map_err(|_| failed())?
        .map_err(|_| failed())
}
