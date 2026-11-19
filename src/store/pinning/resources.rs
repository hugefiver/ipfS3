use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, Value};

use crate::error::AppResult;

mod batch;
mod claims;
mod failover;
mod placement;
mod refs;
#[cfg(test)]
mod tests;

pub use claims::{Action, Claim, Outcome, claim, settle};
pub use refs::{attach, attach_many, expire, manual, release_owner, replace_manual};

fn stmt(db: &impl ConnectionTrait, sql: &str, values: Vec<Value>) -> Statement {
    let backend = db.get_database_backend();
    let sql = match backend {
        DatabaseBackend::Sqlite => sql.replace('$', "?"),
        _ => sql.to_owned(),
    };
    Statement::from_sql_and_values(backend, sql, values)
}

async fn sqlite_writer(tx: &DatabaseTransaction) -> AppResult<()> {
    // Acquire the write transaction before taking a SQLite read snapshot.
    if tx.get_database_backend() == DatabaseBackend::Sqlite {
        tx.execute_unprepared("UPDATE pin_resources SET next_at = 0 WHERE 0")
            .await?;
    }
    Ok(())
}

async fn lock_resource(tx: &DatabaseTransaction, provider: &str, cid: &str) -> AppResult<()> {
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.query_one(stmt(
            tx,
            "SELECT cid FROM pin_resources WHERE provider=$1 AND cid=$2 FOR UPDATE",
            vec![provider.into(), cid.into()],
        ))
        .await?;
    }
    Ok(())
}

async fn reconcile(
    tx: &DatabaseTransaction,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let key = vec![provider.into(), cid.into(), now.timestamp_micros().into()];
    let live =
        "EXISTS (SELECT 1 FROM pin_refs r WHERE r.provider=$1 AND r.cid=$2 AND r.expires_at>$3)";
    tx.execute(stmt(tx, &format!("DELETE FROM pin_resources WHERE provider=$1 AND cid=$2 AND claim_token IS NULL AND handle IS NULL AND status IN ('waiting','failed','pending') AND NOT {live}"),key.clone())).await?;
    tx.execute(stmt(tx, &format!("UPDATE pin_resources SET status=CASE WHEN status='accepted' THEN 'pending' ELSE 'failed' END,action='delete',attempt=0,next_at=0 WHERE provider=$1 AND cid=$2 AND (status='accepted' OR (status='failed' AND action='submit' AND handle IS NOT NULL)) AND NOT {live}"),key.clone())).await?;
    // A delete already attempted may have succeeded despite a lost response.
    tx.execute(stmt(tx, &format!("UPDATE pin_resources SET
        status=CASE WHEN status='cleaned' THEN 'waiting' ELSE 'accepted' END,
        action=CASE WHEN status='cleaned' THEN 'submit' ELSE NULL END
        WHERE provider=$1 AND cid=$2 AND (status='cleaned' OR (status='pending' AND action='delete' AND attempt=0 AND claim_token IS NULL)) AND {live}"),key)).await?;
    Ok(())
}
