use super::*;
use sea_orm::sea_query::SimpleExpr;
use std::{future::Future, sync::Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Running,
    Committing,
    Completed,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct RenewalGate {
    pub entered: tokio::sync::Notify,
    pub proceed: tokio::sync::Notify,
}

pub(super) fn mutation_lease_identity(guard: &StandardMutationGuard) -> Condition {
    Condition::all()
        .add(standard_mutation_lease::Column::Bucket.eq(&guard.bucket))
        .add(standard_mutation_lease::Column::Key.eq(&guard.key))
        .add(standard_mutation_lease::Column::MutationId.eq(&guard.mutation_id))
        .add(standard_mutation_lease::Column::Generation.eq(guard.expected_generation))
}

fn active_lease(backend: DatabaseBackend) -> SimpleExpr {
    match backend {
        DatabaseBackend::Postgres => Expr::cust("lease_until > clock_timestamp()"),
        DatabaseBackend::Sqlite => Expr::cust("julianday(lease_until) > julianday('now')"),
        DatabaseBackend::MySql => Expr::cust("FALSE"),
    }
}

pub(super) fn active_destination_lease(backend: DatabaseBackend) -> SimpleExpr {
    let clock = match backend {
        DatabaseBackend::Postgres => "l.lease_until > clock_timestamp()",
        DatabaseBackend::Sqlite => "julianday(l.lease_until) > julianday('now')",
        DatabaseBackend::MySql => "FALSE",
    };
    Expr::cust(format!(
        "EXISTS (SELECT 1 FROM standard_mutation_leases l WHERE \
        l.bucket = import_destinations.bucket AND l.key = import_destinations.key AND \
        l.mutation_id = import_destinations.mutation_id AND \
        l.generation = import_destinations.generation AND {clock})"
    ))
}

pub(super) async fn verify_mutation_lease<C: ConnectionTrait>(
    db: &C,
    guard: &StandardMutationGuard,
) -> AppResult<()> {
    if standard_mutation_lease::Entity::find()
        .filter(mutation_lease_identity(guard))
        .filter(active_lease(db.get_database_backend()))
        .one(db)
        .await?
        .is_none()
    {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

/// Renew only the still-current, unexpired identity under the bucket fence.
pub async fn renew_standard_mutation(
    db: &DatabaseConnection,
    guard: &StandardMutationGuard,
) -> AppResult<()> {
    let guard = guard.clone();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &guard.bucket).await?;
            verify_standard_mutation_guard(txn, &guard, &guard.bucket, &guard.key, &[]).await?;
            let until = crate::store::database_clock::database_now(txn).await?
                + chrono::Duration::seconds(STANDARD_MUTATION_LEASE_SECONDS);
            let updated = standard_mutation_lease::Entity::update_many()
                .col_expr(
                    standard_mutation_lease::Column::LeaseUntil,
                    Expr::value(until),
                )
                .filter(mutation_lease_identity(&guard))
                .filter(active_lease(txn.get_database_backend()))
                .exec(txn)
                .await?;
            if updated.rows_affected != 1 {
                return Err(AppError::StaleContentMutation);
            }
            Ok(())
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

pub async fn release_standard_mutation(
    db: &DatabaseConnection,
    guard: &StandardMutationGuard,
) -> AppResult<bool> {
    let guard = guard.clone();
    db.transaction(move |txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &guard.bucket).await?;
            let now = crate::store::database_clock::database_now(txn).await?;
            release_standard_mutation_in_transaction(txn, &guard, now).await
        })
    })
    .await
    .map_err(transaction_error_into_app)
}

/// Lifecycle owns bucket -> action locks before reaching this helper. Renew its
/// content fence in that same transaction, using destination -> lease order.
pub(crate) async fn renew_lifecycle_mutation_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    action_id: &str,
    epoch: i64,
) -> AppResult<()> {
    lock_bucket_for_ownership(db, bucket).await?;
    let token = format!("lifecycle:{action_id}:{epoch}");
    let Some(destination) = find_destination_for_update(db, bucket, key).await? else {
        return Ok(());
    };
    if destination.mutation_id.as_deref() != Some(token.as_str()) {
        return Ok(());
    }
    let guard = StandardMutationGuard {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        mutation_id: token,
        expected_generation: destination.generation,
        mutation_prefix: destination.mutation_prefix,
    };
    let until = crate::store::database_clock::database_now(db).await?
        + chrono::Duration::seconds(STANDARD_MUTATION_LEASE_SECONDS);
    let updated = standard_mutation_lease::Entity::update_many()
        .col_expr(
            standard_mutation_lease::Column::LeaseUntil,
            Expr::value(until),
        )
        .filter(mutation_lease_identity(&guard))
        .filter(active_lease(db.get_database_backend()))
        .exec(db)
        .await?;
    if updated.rows_affected != 1 {
        return Err(AppError::StaleContentMutation);
    }
    Ok(())
}

/// Called under the bucket lock. Missing lease rows are deliberately fail-closed;
/// migration backfills legacy tokens. Recovery never restores import ownership.
pub(super) async fn recover_expired_mutations_in_transaction<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
) -> AppResult<()> {
    loop {
        let leases = standard_mutation_lease::Entity::find()
            .filter(standard_mutation_lease::Column::Bucket.eq(bucket))
            .filter(active_lease(db.get_database_backend()).not())
            .order_by_asc(standard_mutation_lease::Column::Key)
            .limit(OWNERSHIP_BATCH_SIZE)
            .all(db)
            .await?;
        if leases.is_empty() {
            return Ok(());
        }
        let now = crate::store::database_clock::database_now(db).await?;
        for lease in leases {
            let guard = StandardMutationGuard {
                bucket: lease.bucket,
                key: lease.key,
                mutation_id: lease.mutation_id,
                expected_generation: lease.generation,
                mutation_prefix: None,
            };
            release_standard_mutation_in_transaction(db, &guard, now).await?;
        }
    }
}

/// Request-owned renewal. The task does not retain this handle. Drop requests
/// precise best-effort cleanup; explicit finish awaits cleanup, while DB expiry
/// is the independent recovery mechanism if the task/runtime/process vanishes.
#[derive(Debug)]
pub struct MutationLease {
    guard: StandardMutationGuard,
    stop: CancellationToken,
    lost: CancellationToken,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    phase: std::sync::Arc<tokio::sync::Mutex<Phase>>,
    #[cfg(test)]
    pub(super) renewal_observed: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pub(super) renewal_gate: std::sync::Arc<Mutex<Option<std::sync::Arc<RenewalGate>>>>,
}

impl PartialEq for MutationLease {
    fn eq(&self, other: &Self) -> bool {
        self.guard == other.guard
    }
}

impl MutationLease {
    pub fn start(db: &DatabaseConnection, guard: &StandardMutationGuard) -> Self {
        Self::start_with_interval(db, guard, Duration::from_secs(30))
    }

    pub(super) fn start_with_interval(
        db: &DatabaseConnection,
        guard: &StandardMutationGuard,
        interval: Duration,
    ) -> Self {
        let stop = CancellationToken::new();
        let lost = CancellationToken::new();
        let phase = std::sync::Arc::new(tokio::sync::Mutex::new(Phase::Running));
        let renewal_phase = phase.clone();
        #[cfg(test)]
        let renewal_observed = std::sync::Arc::new(tokio::sync::Notify::new());
        #[cfg(test)]
        let observed = renewal_observed.clone();
        #[cfg(test)]
        let renewal_gate = std::sync::Arc::new(Mutex::new(None::<std::sync::Arc<RenewalGate>>));
        #[cfg(test)]
        let task_gate = renewal_gate.clone();
        let (task_stop, task_lost, db, token) =
            (stop.clone(), lost.clone(), db.clone(), guard.clone());
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = task_stop.cancelled() => break,
                    _ = tokio::time::sleep(interval) => {
                        #[cfg(test)]
                        {
                            let gate = task_gate.lock().unwrap().take();
                            if let Some(gate) = gate {
                                gate.entered.notify_one();
                                gate.proceed.notified().await;
                            }
                        }
                        // Acquire before touching the database. A commit holds
                        // this gate until its real result is known, so a renewal
                        // cannot misclassify its normal token clear as loss.
                        let phase = tokio::select! {
                            biased;
                            _ = task_stop.cancelled() => break,
                            phase = renewal_phase.lock() => phase,
                        };
                        if *phase != Phase::Running {
                            #[cfg(test)]
                            observed.notify_one();
                            drop(phase);
                            task_stop.cancelled().await;
                            break;
                        }
                        let renewal = tokio::select! {
                            biased;
                            _ = task_stop.cancelled() => break,
                            result = tokio::time::timeout(Duration::from_secs(30), renew_standard_mutation(&db, &token)) => {
                                match result {
                                    Ok(result) => result,
                                    Err(_) => Err(AppError::StaleContentMutation),
                                }
                            },
                        };
                        if renewal.is_err() {
                            task_lost.cancel();
                            #[cfg(test)]
                            observed.notify_one();
                            break;
                        }
                        #[cfg(test)]
                        observed.notify_one();
                    }
                }
            }
            match tokio::time::timeout(
                Duration::from_secs(5),
                release_standard_mutation(&db, &token),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "mutation cleanup deferred to database lease expiry")
                }
                Err(_) => {
                    tracing::warn!("mutation cleanup timed out; deferred to database lease expiry")
                }
            }
        });
        Self {
            guard: guard.clone(),
            stop,
            lost,
            task: Mutex::new(Some(task)),
            phase,
            #[cfg(test)]
            renewal_observed,
            #[cfg(test)]
            renewal_gate,
        }
    }

    /// No total transfer timeout: renew independently of body progress. Lost
    /// ownership cancels the operation future; publication also fences in SQL.
    pub async fn run<T, E: From<AppError>>(
        &self,
        work: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        tokio::select! {
            biased;
            _ = self.lost.cancelled() => Err(AppError::StaleContentMutation.into()),
            result = work => result,
        }
    }

    pub async fn finish(&self) {
        self.stop.cancel();
        let task = self.task.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(task) = task
            && let Err(error) = task.await
        {
            tracing::warn!(%error, "mutation cleanup task failed; lease expiry remains fenced");
        }
    }

    /// Final DB-only phase: serialize with renewal, then keep its actual commit
    /// result authoritative. Missing tokens during Running remain a loss; only
    /// this explicit handoff suppresses renewal. SQL still checks lease/fence.
    pub async fn commit<T, E: From<AppError>>(
        &self,
        work: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let mut phase = self.phase.lock().await;
        if *phase != Phase::Running || self.lost.is_cancelled() {
            return Err(AppError::StaleContentMutation.into());
        }
        *phase = Phase::Committing;
        let result = work.await;
        *phase = Phase::Completed;
        result
    }
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub async fn run_mutation<T, E: From<AppError>, F: Future<Output = Result<T, E>>>(
    db: &DatabaseConnection,
    guard: &StandardMutationGuard,
    work: impl FnOnce(std::sync::Arc<MutationLease>) -> F,
) -> Result<T, E> {
    let lease = std::sync::Arc::new(MutationLease::start(db, guard));
    let result = lease.run(work(lease.clone())).await;
    lease.finish().await;
    result
}
