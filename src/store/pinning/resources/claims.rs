use super::{reconcile, sqlite_writer, stmt};
use crate::{error::AppResult, pinning::config::ProviderLimits};
use chrono::{DateTime, Utc};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, QueryResult,
    TransactionTrait,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Submit,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Claim {
    pub provider: String,
    pub cid: String,
    pub source_cid: String,
    pub token: String,
    pub action: Action,
    pub handle: Option<String>,
    pub attempt: i32,
}

#[derive(Debug, Clone)]
pub enum Outcome {
    Submitted(String),
    Rejected(String),
    Deleted,
    /// A submit may be retried only when it definitely had no remote effect.
    RetryAt(DateTime<Utc>),
    Failed,
    Unknown,
}

pub async fn claim(
    db: &DatabaseConnection,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTime<Utc>,
    claim_until: DateTime<Utc>,
) -> AppResult<Option<Claim>> {
    let tx = db.begin().await?;
    sqlite_writer(&tx).await?;
    let lock = if tx.get_database_backend() == DatabaseBackend::Postgres {
        " FOR UPDATE SKIP LOCKED"
    } else {
        ""
    };
    // Losing the sender never proves a POST was rejected. Preserve its token for a late ACK.
    tx.execute(stmt(&tx,&format!("UPDATE pin_resources SET status='unknown',claim_until=NULL WHERE provider=$1 AND cid IN (
        SELECT cid FROM pin_resources WHERE provider=$1 AND action='submit' AND status='pending' AND claim_until<=$2 ORDER BY claim_until LIMIT 1{lock})"),
        vec![provider.into(),now.timestamp_micros().into()])).await?;
    let due=tx.query_one(stmt(&tx,&format!("SELECT cid,COALESCE(source_cid,cid) AS source_cid,action,handle,attempt FROM pin_resources WHERE provider=$1 AND status IN ('pending','failed') AND next_at<=$2
        AND action='delete' AND (claim_token IS NULL OR claim_until<=$2) ORDER BY next_at,cid LIMIT 1{lock}"),vec![provider.into(),now.timestamp_micros().into()])).await?;
    let candidate = match due {
        Some(row) => Some(row),
        None if limits.enabled => claimable_submit(&tx, provider, limits, now, lock).await?,
        None => None,
    };
    let Some(row) = candidate else {
        tx.commit().await?;
        return Ok(None);
    };
    let token = uuid::Uuid::new_v4().to_string();
    let cid: String = row.try_get("", "cid")?;
    let action = if row.try_get::<String>("", "action")? == "delete" {
        Action::Delete
    } else {
        Action::Submit
    };
    let handle = row.try_get("", "handle")?;
    let attempt = row.try_get::<i32>("", "attempt")? + 1;
    tx.execute(stmt(&tx,"UPDATE pin_resources SET status='pending',claim_token=$3,claim_until=$4,attempt=$5 WHERE provider=$1 AND cid=$2",
        vec![provider.into(),cid.clone().into(),token.clone().into(),claim_until.timestamp_micros().into(),attempt.into()])).await?;
    tx.commit().await?;
    Ok(Some(Claim {
        provider: provider.into(),
        cid,
        source_cid: row.try_get("", "source_cid")?,
        token,
        action,
        handle,
        attempt,
    }))
}

async fn claimable_submit(
    tx: &DatabaseTransaction,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTime<Utc>,
    lock: &str,
) -> AppResult<Option<QueryResult>> {
    // Idle providers need neither a capacity scan nor a reservation lock.
    if tx.query_one(stmt(tx,
        "SELECT 1 FROM pin_resources WHERE provider=$1 AND status='waiting' AND next_at<=$2 LIMIT 1",
        vec![provider.into(), now.timestamp_micros().into()],
    )).await?.is_none() {
        return Ok(None);
    }
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        let locked = tx
            .query_one(stmt(
                tx,
                "SELECT pg_try_advisory_xact_lock(1844053111,hashtext($1)) AS locked",
                vec![provider.into()],
            ))
            .await?
            .unwrap();
        if !locked.try_get::<bool>("", "locked")? {
            return Ok(None);
        }
    }
    let row=tx.query_one(stmt(tx,"SELECT COUNT(*) AS pins, CAST(COALESCE(SUM(logical_size),0) AS BIGINT) AS bytes FROM pin_resources WHERE provider=$1 AND status!='cleaned' AND (status IN ('accepted','pending','unknown') OR handle IS NOT NULL)",vec![provider.into()])).await?.unwrap();
    let pins: i64 = row.try_get("", "pins")?;
    let bytes: i64 = row.try_get("", "bytes")?;
    if pins >= limits.max_pins || bytes > limits.max_bytes {
        return Ok(None);
    };
    Ok(tx.query_one(stmt(tx,&format!("SELECT cid,COALESCE(source_cid,cid) AS source_cid,action,handle,attempt FROM pin_resources p WHERE provider=$1 AND status='waiting' AND next_at<=$2 AND logical_size<=$3
        AND EXISTS (SELECT 1 FROM pin_refs r WHERE r.provider=p.provider AND r.cid=p.cid AND r.expires_at>$2)
        ORDER BY next_at,cid LIMIT 1{lock}"),vec![provider.into(),now.timestamp_micros().into(),(limits.max_bytes-bytes).into()])).await?)
}

pub async fn settle(
    db: &DatabaseConnection,
    claim: &Claim,
    outcome: Outcome,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let tx = db.begin().await?;
    let mut values = vec![
        claim.provider.clone().into(),
        claim.cid.clone().into(),
        claim.token.clone().into(),
    ];
    let advance = claim.action == Action::Submit && matches!(outcome, Outcome::Failed);
    let update = match (claim.action, outcome) {
        (Action::Submit, Outcome::Rejected(handle)) => {
            values.push(handle.into());
            "status='failed',handle=$4,action='submit',attempt=0,next_at=0,claim_token=NULL"
        }
        (Action::Submit, Outcome::Submitted(handle)) => {
            values.push(handle.into());
            "status='accepted',handle=$4,action=NULL,attempt=0,next_at=0,claim_token=NULL"
        }
        (Action::Delete, Outcome::Deleted) => {
            "status='cleaned',handle=NULL,action=NULL,attempt=0,next_at=0,claim_token=NULL"
        }
        (action, Outcome::RetryAt(when)) => {
            values.push(when.timestamp_micros().into());
            match action {
                Action::Submit => "status='waiting',next_at=$4,claim_token=NULL",
                Action::Delete => "status='pending',next_at=$4,claim_token=NULL",
            }
        }
        (_, Outcome::Failed) => "status='failed',action=NULL,claim_token=NULL",
        (_, Outcome::Unknown) => "status='unknown'",
        _ => {
            return Err(crate::error::AppError::Internal(
                "pin acknowledgement does not match claimed action".into(),
            ));
        }
    };
    let result=tx.execute(stmt(&tx,&format!("UPDATE pin_resources SET {update},claim_until=NULL WHERE provider=$1 AND cid=$2 AND claim_token=$3"),values)).await?;
    if result.rows_affected() != 0 {
        if advance {
            super::failover::advance(&tx, &claim.provider, &claim.cid, now).await?;
        }
        reconcile(&tx, &claim.provider, &claim.cid, now).await?;
    }
    tx.commit().await?;
    Ok(())
}
