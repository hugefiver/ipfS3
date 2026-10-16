use super::*;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderMode as AllocationMode},
        management::{Intent, ObjectCapture, ProviderMode},
        policy::{LeaseIntent, LeaseSource},
        tags::ContentMode,
    },
    store::{
        entities::{pin_lease, remote_pin},
        pinning::{
            leases,
            publication::{
                PinTargetSpec,
                management::{self as allocation, AbsoluteIntent},
            },
        },
    },
};
use chrono::{DateTime, Utc};
use sea_orm::DatabaseTransaction;

pub(super) async fn create(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    binding: records::BindingView<'_>,
    capture: &ObjectCapture,
    intent: &Intent,
    before: records::FrozenRows<'_>,
    result: &mut records::CommittedAction,
) -> Result<()> {
    let work = binding.decode_work();
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let started_at = time(intent.started_at)?;
    let expires_at = time(intent.expires_at)?;
    let bytes = stored_bytes(capture, now)?;
    let proof = work
        .with(|work| {
            accounting::current_proof_from_source(
                before,
                &config.source_endpoint,
                config.source_node(),
                &capture.object.identity,
                now,
                work,
            )
        })?
        .ok_or(ManagementError::InvalidEvidence)?;
    if capture.object.read_evidence.as_ref()
        != Some(&crate::pinning::management::ReadEvidence::from(
            proof.as_ref(),
        ))
    {
        return Err(ManagementError::Changed);
    }
    let count = accounting::count(
        capture
            .object
            .read_evidence
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    let stored_cid = native::value(
        before
            .row("objects", &[&capture.object.identity.object_id])
            .ok_or(ManagementError::Changed)?,
        "cid",
    )
    .ok_or(ManagementError::InvalidEvidence)?;
    let policy = policy(config, capture)?;
    if intent.requirements.policy_revision != policy.identity
        || !within_policy_duration(started_at, expires_at, policy.max_duration.as_seconds())
    {
        return Err(ManagementError::Confirmation);
    }
    work.with(|work| work.nodes(1))?;
    let mut providers = Vec::new();
    for id in &intent.requirements.provider_ids {
        let route = capture
            .requirement_routes
            .iter()
            .find(|route| route.identity.provider_id == *id)
            .ok_or(ManagementError::Confirmation)?;
        let original = route
            .original
            .as_ref()
            .ok_or(ManagementError::Confirmation)?;
        let provider = config
            .pinning
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == *id)
            .ok_or(ManagementError::Changed)?;
        work.copy(original)?;
        if &provider.route_snapshot() != original {
            return Err(ManagementError::Changed);
        }
        if providers.contains(&provider.name) {
            return Err(ManagementError::Confirmation);
        }
        work.copy(&provider.name)?;
        providers.push(provider.name.clone());
        if proof.proof.is_directory()
            && !matches!(provider.identity.strategy.as_str(), "cid" | "car")
        {
            return Err(ManagementError::Confirmation);
        }
        accounting::check_rows_with_work(
            before,
            &provider.name,
            stored_cid,
            count,
            work.budget().clone(),
        )?;
        check_accounting(tx, &provider.name, stored_cid, bytes, work).await?;
    }
    let duration = expires_at.signed_duration_since(started_at).num_seconds();
    work.copy(&intent.requirements.policy_revision)?;
    let lease_intent = LeaseIntent {
        source: LeaseSource::Manual,
        policy_id: intent.requirements.policy_revision.clone(),
        provider_mode: match intent.requirements.mode {
            ProviderMode::One => AllocationMode::One,
            ProviderMode::All => AllocationMode::All,
        },
        providers,
        content_mode: ContentMode::Object,
        duration: LeaseDuration::parse(&format!("{duration}s"))
            .map_err(|_| ManagementError::Confirmation)?,
    };
    work.with(|work| {
        work.nodes(1)?;
        work.bytes("management:".len() + 36)
    })?;
    let id = format!("management:{}", uuid::Uuid::new_v4());
    work.copy(stored_cid)?;
    let targets = [PinTargetSpec {
        cid: stored_cid.into(),
        logical_size: i64::try_from(bytes).map_err(|_| ManagementError::Budget)?,
    }];
    let allocated = allocation::allocate_existing_with_work(
        tx,
        AbsoluteIntent {
            lease_id: &id,
            owner_object_id: &capture.owner_identity.object_id,
            source: &id,
            intent: &lease_intent,
            targets: &targets,
            started_at,
            expires_at,
            requirements: &intent.requirements,
        },
        &config.pinning.provider_limits,
        now,
        work,
    )
    .await
    .map_err(database)?;
    work.copy(&id)?;
    result.receipt.intent_ids.push(id.clone());
    work.copy(&(
        &id,
        &capture.owner_identity,
        intent,
        bytes,
        &binding.confirmation.configuration,
    ))?;
    result.decision = Some(records::OperatorDecision {
        id: id.clone(),
        owner: capture.owner_identity.clone(),
        intent: intent.clone(),
        stored_bytes: bytes,
        configuration: binding
            .confirmation
            .configuration
            .clone()
            .ok_or(ManagementError::Confirmation)?,
    });
    for target in allocated {
        recovery::schedule_target(tx, capture, &target, before, result, work).await?;
        result.targets.push(target.id);
    }
    Ok(())
}

pub(super) async fn renew(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    capture: &ObjectCapture,
    expires_at: i64,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<leases::ManualLeaseRenewalOutcome> {
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let lease = records::current_model::<pin_lease::Entity, _>(tx, &[&target.lease.id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    if !same_lease(&target.lease, &lease)? {
        return Err(ManagementError::Changed);
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    let expiry = time(expires_at)?;
    let policy = policy(config, capture)?;
    if lease.owner_object_id != capture.owner_identity.object_id
        || lease.generation != target.lease.generation
        || lease.state != "active"
        || lease.expires_at <= now
        || !within_policy_duration(lease.created_at, expiry, policy.max_duration.as_seconds())
        || capture
            .requirement_routes
            .iter()
            .any(|route| !route.allocation_allowed)
    {
        return Err(ManagementError::Changed);
    }
    // C extends the existing renewal frontier with an exact confirmed source
    // rather than pretending every automatic/operator lease is a manual slot.
    let outcome = leases::renew_confirmed_lease_in_transaction(tx, &lease, expiry, now, work)
        .await
        .map_err(database)?;
    result.targets = target_ids(tx, &lease.id, work).await?;
    result.receipt.intent_ids.push(lease.id);
    Ok(outcome)
}

pub(super) async fn cancel(
    tx: &DatabaseTransaction,
    capture: &ObjectCapture,
    result: &mut records::CommittedAction,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let target = capture
        .target
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    let lease = records::current_model::<pin_lease::Entity, _>(tx, &[&target.lease.id], work)
        .await?
        .ok_or(ManagementError::Changed)?;
    if !same_lease(&target.lease, &lease)? {
        return Err(ManagementError::Changed);
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    leases::cancel_confirmed_lease_in_transaction(tx, &lease, now, work)
        .await
        .map_err(database)?;
    result.targets = target_ids(tx, &lease.id, work).await?;
    result.receipt.intent_ids.push(lease.id);
    Ok(())
}

fn same_lease(
    capture: &crate::pinning::management::LeaseSnapshot,
    lease: &pin_lease::Model,
) -> Result<bool> {
    let row = capture
        .native_tuple
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    Ok(native::value(row, "id") == Some(&lease.id)
        && native::value(row, "owner_object_id") == Some(&lease.owner_object_id)
        && native::value(row, "source") == Some(&lease.source)
        && native::value(row, "policy_id") == Some(&lease.policy_id)
        && native::value(row, "provider_mode") == Some(&lease.provider_mode)
        && native::value(row, "content_mode") == Some(&lease.content_mode)
        && native::value(row, "state") == Some(&lease.state)
        && capture.generation == lease.generation
        && native::parse_time(
            native::value(row, "created_at").ok_or(ManagementError::Confirmation)?,
        )? == lease.created_at
        && native::parse_time(
            native::value(row, "expires_at").ok_or(ManagementError::Confirmation)?,
        )? == lease.expires_at
        && native::parse_time(
            native::value(row, "last_touched_at").ok_or(ManagementError::Confirmation)?,
        )? == lease.last_touched_at)
}

pub(super) fn time(seconds: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(seconds, 0).ok_or(ManagementError::Confirmation)
}

fn within_policy_duration(start: DateTime<Utc>, expiry: DateTime<Utc>, maximum: u64) -> bool {
    let Some(maximum) = i64::try_from(maximum)
        .ok()
        .and_then(chrono::Duration::try_seconds)
    else {
        return false;
    };
    let duration = expiry.signed_duration_since(start);
    duration > chrono::Duration::zero() && duration <= maximum
}

pub(super) fn stored_bytes(capture: &ObjectCapture, now: DateTime<Utc>) -> Result<u64> {
    let proof = capture
        .object
        .read_evidence
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    if proof.identity != capture.object.identity
        || proof.observed_at > now
        || now >= proof.expires_at
    {
        return Err(ManagementError::Expired);
    }
    Ok(accounting::count(proof)?.bytes)
}

pub(super) async fn check_accounting(
    tx: &DatabaseTransaction,
    provider: &str,
    cid: &str,
    bytes: u64,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let cid = crate::store::pinning::ledger::allocation_cid_with_work(tx, provider, cid, work)
        .await
        .map_err(database)?;
    if let Some(remote) =
        records::current_model::<remote_pin::Entity, _>(tx, &[provider, &cid], work).await?
        && u64::try_from(remote.cid_size).ok() != Some(bytes)
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}

async fn target_ids(
    tx: &DatabaseTransaction,
    lease: &str,
    work: records::DecodeWork<'_>,
) -> Result<Vec<String>> {
    for _ in 0..2 {
        work.copy(lease)?;
    }
    let (rows, bytes) = records::bounded_projection(
        tx,
        "FROM pin_lease_targets WHERE lease_id=$1",
        &[("id", "id")],
        "id",
        vec![lease.into()],
        (native::MAX_ROWS, native::MAX_BYTES),
        work,
    )
    .await?;
    work.index_cells(rows.len(), bytes)?;
    rows.into_iter()
        .map(|row| {
            row.try_get::<String>("", "id")
                .map_err(|_| ManagementError::Database)
        })
        .collect()
}

fn policy<'a>(
    config: &'a ManagementConfig,
    capture: &ObjectCapture,
) -> Result<&'a crate::pinning::config::ValidatedPolicy> {
    config
        .pinning
        .policies
        .iter()
        .find(|policy| {
            (policy.bucket == "*" || policy.bucket == capture.owner_identity.bucket)
                && capture.owner_identity.key.starts_with(&policy.prefix)
        })
        .ok_or(ManagementError::Changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maximum_duration_keeps_native_nanoseconds_and_dto_seconds() {
        let start = DateTime::from_timestamp(0, 999_999_999).unwrap();
        assert!(!within_policy_duration(start, time(7201).unwrap(), 7200));
        assert!(within_policy_duration(start, time(7200).unwrap(), 7200));
        assert!(within_policy_duration(
            time(1).unwrap(),
            time(7201).unwrap(),
            7200
        ));
        assert!(!within_policy_duration(
            time(1).unwrap(),
            time(7202).unwrap(),
            7200
        ));
        assert!(!within_policy_duration(
            time(1).unwrap(),
            time(1).unwrap(),
            7200
        ));
    }
}
