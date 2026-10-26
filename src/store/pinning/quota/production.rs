//! SQLite quota reads owned by one production publication transaction.
use super::*;
use crate::store::pinning::{legacy_migration, publication::PublicationTransaction};

pub(crate) struct QuotaReadWindow<'a> {
    owner: &'a mut PublicationTransaction,
    versions: Vec<String>,
}

impl QuotaReadWindow<'_> {
    pub(in crate::store::pinning) fn db(&self) -> &DatabaseTransaction {
        self.owner.db()
    }
    pub(in crate::store::pinning) fn versions(&self) -> &[String] {
        &self.versions
    }
}

enum Prepared {
    Outcome(ReservationOutcome),
    Reuse(remote_pin::Model),
    Allocate(Option<remote_pin::Model>, pin_provider_usage::Model),
    MissingUsage,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::store::pinning) async fn reserve(
    owner: &mut PublicationTransaction,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<ReservationOutcome> {
    admission::lock_sqlite_writer_in_caller_transaction(owner.db()).await?;
    if legacy_migration::source_is_migrated_with_work(owner.db(), provider, work)
        .await
        .map_err(admission::work_error)?
    {
        return Err(invalid_quota(
            "migrated legacy namespace cannot allocate or revive",
        ));
    }
    for attempt in 0..SQLITE_RETRY_LIMIT {
        let result = reserve_attempt(owner, provider, cid, cid_size, limits, now, work).await;
        match result {
            Ok(ReserveAttempt::Outcome(outcome)) => return Ok(outcome),
            Ok(ReserveAttempt::Waiting(_)) => {
                return Ok(ReservationOutcome::QuotaWaiting { evict: Vec::new() });
            }
            Ok(ReserveAttempt::Retry) => retry_delay(attempt).await,
            Err(AppError::Database(message)) if is_sqlite_contention(&message) => {
                retry_delay(attempt).await
            }
            Err(error) => return Err(error),
        }
    }
    Err(AppError::Database(
        "quota reservation exhausted retrying a concurrent SQLite write".to_owned(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn reserve_attempt(
    owner: &mut PublicationTransaction,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
    work: DecodeWork<'_>,
) -> AppResult<ReserveAttempt> {
    if super::super::ledger::allocation_cid_with_work(owner.db(), provider, cid, work).await? != cid
    {
        return Err(invalid_quota(
            "new RPC target must use the canonical allocation CID",
        ));
    }
    let registered = admission::provider_registered_with_work(owner.db(), provider, work).await?;
    let prelocked = if registered {
        let remote = acquire_existing_remote(owner.db(), provider, cid).await?;
        work.copy(provider).map_err(admission::work_error)?;
        lock_publication_usage_rows(owner.db(), &[provider.to_owned()]).await?;
        admission::lock_quota_tables(owner.db()).await?;
        Some(remote)
    } else {
        None
    };
    let versions = legacy_migration::receipt_schema(owner.db()).await?;
    if versions.last().map(String::as_str) != Some(super::super::management::schema::VERSION) {
        return reserve_unique_attempt_with_work(
            owner.db(),
            (provider, cid, cid_size),
            limits,
            now,
            false,
            work,
            None,
        )
        .await;
    }
    let window = QuotaReadWindow { owner, versions };
    let prepared = prepare(
        &window, provider, cid, cid_size, limits, prelocked, registered, work,
    )
    .await?;
    // No schema qualification, authority view or read-window alias crosses the
    // first grant/remote mutation. The enclosing publication still owns the writer.
    let QuotaReadWindow { owner, .. } = window;
    match prepared {
        Prepared::Outcome(outcome) => Ok(ReserveAttempt::Outcome(outcome)),
        Prepared::Reuse(remote) => {
            reuse_capacity_holding_remote(owner.db(), provider, cid, remote, now, work, None).await
        }
        Prepared::Allocate(remote, usage) => {
            let epoch = remote
                .as_ref()
                .map(|remote| next_epoch(remote.epoch))
                .transpose()?;
            let capacity = reserve_capacity_from_usage(
                owner.db(),
                provider,
                cid_size,
                provider_limits(limits, provider)?,
                usage,
            )
            .await?;
            match (remote, epoch) {
                (Some(remote), Some(epoch)) => {
                    reacquire_absent_with_capacity(
                        owner.db(),
                        (provider, cid),
                        cid_size,
                        remote,
                        epoch,
                        now,
                        work,
                        capacity,
                        None,
                    )
                    .await
                }
                _ => {
                    reserve_new_remote_with_capacity(
                        owner.db(),
                        (provider, cid, cid_size),
                        now,
                        work,
                        capacity,
                        None,
                    )
                    .await
                }
            }
        }
        Prepared::MissingUsage => {
            reserve_unique_attempt_with_work(
                owner.db(),
                (provider, cid, cid_size),
                limits,
                now,
                false,
                work,
                None,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare(
    window: &QuotaReadWindow<'_>,
    provider: &str,
    cid: &str,
    cid_size: i64,
    limits: &ProviderLimitMap,
    prelocked: Option<Option<remote_pin::Model>>,
    registered: bool,
    work: DecodeWork<'_>,
) -> AppResult<Prepared> {
    use legacy_migration::ReceiptCatalog;
    let catalog = ReceiptCatalog::Quota(window);
    admission::check_quota_provider(window, provider, registered, work).await?;
    match super::super::ledger::submission::admission_barrier_with_catalog(provider, catalog, work)
        .await?
    {
        super::super::ledger::submission::AdmissionBarrier::Temporary => {
            return Ok(Prepared::Outcome(ReservationOutcome::QuotaWaiting {
                evict: Vec::new(),
            }));
        }
        super::super::ledger::submission::AdmissionBarrier::Operator => {
            return Ok(Prepared::Outcome(ReservationOutcome::QuotaBlocked));
        }
        super::super::ledger::submission::AdmissionBarrier::Clear => {}
    }
    let limits = provider_limits(limits, provider)?;
    if cid_size < 0 {
        return Err(invalid_quota("CID size cannot be negative"));
    }
    if cid_size > limits.max_bytes {
        return Ok(Prepared::Outcome(ReservationOutcome::QuotaBlocked));
    }
    let existing = match prelocked {
        Some(remote) => remote,
        None => acquire_existing_remote(&catalog, provider, cid).await?,
    };
    if let Some(remote) = &existing {
        if is_capacity_holding_status(&remote.status) {
            let candidates = catalog
                .load(
                    &legacy_migration::resource_receipt_selectors(provider, cid),
                    work,
                )
                .await
                .map_err(admission::work_error)?;
            assert_reusable_route_from_candidates(provider, cid, work, Some(candidates), catalog)
                .await?;
            return Ok(Prepared::Reuse(remote.clone()));
        }
        if remote.status != REMOTE_STATUS_ABSENT {
            return Err(invalid_quota("remote pin has an unknown status"));
        }
    }
    assert_new_allocation_safe(&catalog, provider).await?;
    let key = [("provider".to_owned(), provider.to_owned())].into();
    let binding = legacy_migration::resolve_with_catalog(
        "pin_provider_usage",
        &key,
        provider,
        &work,
        catalog,
    )
    .await
    .map_err(admission::work_error)?;
    let operational = binding
        .as_ref()
        .map_or(provider, |binding| binding.operational_key.as_str());
    let Some(usage) =
        records::current_model::<pin_provider_usage::Entity, _>(&catalog, &[operational], work)
            .await
            .map_err(admission::work_error)?
    else {
        return Ok(Prepared::MissingUsage);
    };
    reservation_decision(
        ProviderUsage {
            reserved_bytes: usage.reserved_bytes,
            reserved_pins: usage.reserved_pins,
        },
        cid_size,
        limits,
    )?;
    Ok(Prepared::Allocate(existing, usage))
}
