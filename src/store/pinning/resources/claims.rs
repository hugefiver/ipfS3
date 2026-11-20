use super::{reconcile, stmt};
use crate::error::AppResult;
use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseConnection, TransactionTrait};

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
    pub(crate) eligible_at_micros: i64,
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

pub async fn settle(
    db: &DatabaseConnection,
    claim: &Claim,
    outcome: Outcome,
    now: DateTime<Utc>,
) -> AppResult<()> {
    settle_dispatch(db, claim, outcome, now).await.map(|_| ())
}

pub(crate) struct Settlement {
    pub applied: bool,
    pub due_delete: bool,
}

pub(crate) async fn settle_dispatch(
    db: &DatabaseConnection,
    claim: &Claim,
    outcome: Outcome,
    now: DateTime<Utc>,
) -> AppResult<Settlement> {
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
    let applied = result.rows_affected() != 0;
    let due_delete = if applied {
        if advance {
            super::failover::advance(&tx, &claim.provider, &claim.cid, now).await?;
        }
        reconcile(&tx, &claim.provider, &claim.cid, now).await?
    } else {
        false
    };
    tx.commit().await?;
    Ok(Settlement {
        applied,
        due_delete,
    })
}
