//! Transaction-bound read capability for explicit history conversion only.
use crate::store::pinning::management::{ManagementError, Result, native, schema};
use sea_orm::{ConnectionTrait, DatabaseTransaction, Statement};
use std::future::Future;

struct Context {
    transaction: usize,
    scope: String,
    identity: String,
}
tokio::task_local! { static UPGRADE_READ: Context; }

#[cfg(test)]
mod tests;

/// No caller outside initialization can create this capability. It neither
/// changes readiness nor propagates into spawned tasks or other transactions.
pub(super) async fn run<T>(
    tx: &DatabaseTransaction,
    scope: &str,
    identity: &str,
    body: impl Future<Output = Result<T>>,
) -> Result<T> {
    schema::validate_for_upgrade(tx).await?;
    let actual = native::scope::binding(tx).await?;
    if actual.0 != scope || actual.1 != identity || identity.starts_with("diagnostic:") {
        return Err(ManagementError::Unauthorized);
    }
    let output = UPGRADE_READ
        .scope(
            Context {
                transaction: tx as *const _ as usize,
                scope: scope.into(),
                identity: identity.into(),
            },
            body,
        )
        .await?;
    backfill(tx).await?;
    Ok(output)
}

pub(crate) async fn active<C: ConnectionTrait>(db: &C) -> Result<bool> {
    let selected = UPGRADE_READ
        .try_with(|context| {
            (context.transaction == db as *const C as usize)
                .then(|| (context.scope.clone(), context.identity.clone()))
        })
        .ok()
        .flatten();
    let Some(expected) = selected else {
        return Ok(false);
    };
    if native::scope::binding(db).await? != expected {
        return Err(ManagementError::Unauthorized);
    }
    backfill(db).await?;
    Ok(true)
}

async fn backfill<C: ConnectionTrait>(db: &C) -> Result<()> {
    if db.query_one(Statement::from_string(db.get_database_backend(), "SELECT 1 FROM pin_management_readiness WHERE id=1 AND format='S75A1' AND state='backfill'"))
        .await.map_err(|_| ManagementError::Schema)?.is_none() {
        return Err(ManagementError::Schema);
    }
    Ok(())
}
