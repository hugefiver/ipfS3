use std::time::Duration;

use sea_orm::ConnectionTrait;
use tokio::time::Instant;

use super::{RootClaim, root};
use crate::{
    error::{AppError, AppResult},
    store::import::ownership::{StandardMutationGuard, renew_standard_mutation_in_transaction},
};

#[cfg(test)]
pub(crate) static RENEW_EVERY_CHECKPOINT_FOR_BATCH: std::sync::Mutex<
    std::collections::BTreeSet<String>,
> = std::sync::Mutex::new(std::collections::BTreeSet::new());

/// Revisits only the source destination/lease and, after the original batch
/// lock, its current root build. No lock frontier is discovered here.
pub(crate) struct PublicationLease<'a> {
    guard: Option<&'a StandardMutationGuard>,
    root: Option<&'a RootClaim>,
    renewed_at: Instant,
    #[cfg(test)]
    every_checkpoint: bool,
    #[cfg(test)]
    stall_standard_renewal: bool,
}

impl<'a> PublicationLease<'a> {
    pub(crate) async fn start<C: ConnectionTrait>(
        tx: &C,
        guard: Option<&'a StandardMutationGuard>,
        _batch_id: &str,
    ) -> AppResult<Self> {
        let mut lease = Self {
            guard,
            root: None,
            renewed_at: Instant::now(),
            #[cfg(test)]
            every_checkpoint: RENEW_EVERY_CHECKPOINT_FOR_BATCH
                .lock()
                .unwrap()
                .contains(_batch_id),
            #[cfg(test)]
            stall_standard_renewal: false,
        };
        lease.renew(tx).await?;
        Ok(lease)
    }

    pub(crate) async fn root_locked<C: ConnectionTrait>(
        &mut self,
        tx: &C,
        batch_id: &str,
        claim: &'a RootClaim,
    ) -> AppResult<()> {
        if self.guard.is_none() {
            return Ok(());
        }
        if claim.batch_id != batch_id {
            return Err(super::stale());
        }
        root::renew_claim_in_transaction(tx, claim, 600).await?;
        self.root = Some(claim);
        Ok(())
    }

    pub(crate) async fn checkpoint<C: ConnectionTrait>(&mut self, tx: &C) -> AppResult<()> {
        let due = self.renewed_at.elapsed() >= Duration::from_secs(30);
        #[cfg(test)]
        let due = due || self.every_checkpoint;
        if self.guard.is_some() && due {
            self.renew(tx).await?;
        }
        Ok(())
    }

    async fn renew<C: ConnectionTrait>(&mut self, tx: &C) -> AppResult<()> {
        if let Some(guard) = self.guard {
            tokio::time::timeout(Duration::from_secs(30), async {
                #[cfg(test)]
                if self.stall_standard_renewal {
                    std::future::pending::<()>().await;
                }
                renew_standard_mutation_in_transaction(tx, guard).await
            })
            .await
            .map_err(|_| AppError::StaleContentMutation)??;
        }
        if let Some(claim) = self.root {
            root::renew_claim_in_transaction(tx, claim, 600).await?;
        }
        self.renewed_at = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        import::SupersedeReason,
        store::{self, entities::standard_mutation_lease, import::ownership},
    };
    use sea_orm::{Database, EntityTrait, TransactionError, TransactionTrait};

    #[tokio::test]
    async fn standard_renewal_timeout_returns_from_original_transaction_after_rollback() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        let guard = ownership::admit_content_mutation(
            &db,
            "bucket",
            "key",
            None,
            SupersedeReason::PutObject,
            chrono::Utc::now(),
        )
        .await
        .unwrap();
        db.execute_unprepared("UPDATE standard_mutation_leases SET lease_until=strftime('%Y-%m-%dT%H:%M:%fZ','now','+10 seconds')").await.unwrap();
        let before = standard_mutation_lease::Entity::find()
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let result = db.transaction(|tx| Box::pin(async move {
            ownership::lock_bucket_for_ownership(tx, "bucket").await?;
            let mut maintenance = PublicationLease::start(tx, Some(&guard), "timeout").await?;
            tx.execute_unprepared("INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES('partial','bucket','key','cid',1,'cid')").await?;
            maintenance.renewed_at -= Duration::from_secs(31);
            maintenance.stall_standard_renewal = true;
            tokio::time::pause();
            let result = maintenance.checkpoint(tx).await;
            tokio::time::resume();
            result
        })).await;
        assert!(matches!(
            result,
            Err(TransactionError::Transaction(
                AppError::StaleContentMutation
            ))
        ));
        assert_eq!(
            standard_mutation_lease::Entity::find()
                .one(&db)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert!(
            store::entities::object::Entity::find_by_id("partial")
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
    }
}
