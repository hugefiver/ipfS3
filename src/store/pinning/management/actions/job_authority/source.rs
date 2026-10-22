//! Source fences do not extend confirmation or attestation lifetimes.
use super::*;
use crate::pinning::{
    decision::DecisionEffect,
    management::{ReadEvidence, evidence::ReadAttestation},
    policy::PublicationContext,
    tags::ObjectTag,
};
use sea_orm::Statement;

pub(super) async fn historical_fresh<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    authority: &records::JobAuthority,
    now: DateTime<Utc>,
    work: records::SharedDecodeBudget,
) -> Result<bool> {
    for trusted in
        crate::store::pinning::management::admission::authority::actions_for_job(db, job, work)
            .await?
    {
        let action = trusted.action_view();
        for seed in &action.jobs {
            trusted.binding().copy(&(seed, &authority.job_id))?;
            let mut derived = seed.clone();
            derived.job_id.clone_from(&authority.job_id);
            if seed.directive == records::JobDirective::Submit
                && derived == *authority
                && birth_from_rows(
                    db,
                    trusted.binding(),
                    action,
                    seed,
                    trusted.binding().frozen_rows()?,
                    now,
                )
                .await?
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) async fn birth_from_rows<C: ConnectionTrait>(
    db: &C,
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
    frame: records::FrozenRows<'_>,
    now: DateTime<Utc>,
) -> Result<bool> {
    let entry = &action.receipt.binding;
    let Some(Capture::Object(capture)) = &entry.capture else {
        return Ok(false);
    };
    let Some(summary) = &capture.object.read_evidence else {
        return Ok(false);
    };
    let Some(identity) = authority.source.as_ref() else {
        return Ok(false);
    };
    let proof = if identity == &summary.identity {
        match binding
            .with_decode_work(|work| accounting::trusted_proof_with_work(frame, summary, work))
        {
            Ok(proof) => proof,
            Err(ManagementError::InvalidEvidence) => return Ok(false),
            Err(error) => return Err(error),
        }
    } else {
        let Some(proof) = binding.with_decode_work(|work| {
            accounting::fresh_original_proof_with_work(frame, identity, now, work)
        })?
        else {
            return Ok(false);
        };
        proof
    };
    for (table, id) in [
        ("objects", identity.object_id.as_str()),
        ("object_versions", identity.private_version_id.as_str()),
    ] {
        let current = records::current_row(db, table, &[id], binding.decode_work()).await?;
        if !frame.original_row_matches(table, &[id], &current) {
            return Ok(false);
        }
    }
    let completed = crate::store::database_clock::database_now(db)
        .await
        .map_err(database)?;
    let expected = (identity == &summary.identity).then_some(summary);
    if !fresh(&proof, expected, now) || !fresh(&proof, expected, completed) {
        return Ok(false);
    }
    Ok(!skipped(db, authority).await? || explicit_reapply(binding, action, authority))
}

fn fresh(proof: &ReadAttestation, summary: Option<&ReadEvidence>, now: DateTime<Utc>) -> bool {
    summary.is_none_or(|summary| summary.matches_attestation(proof))
        && proof.observed_at <= now
        && now < proof.expires_at
        && proof.proof.has_stored_byte_count()
}

async fn skipped<C: ConnectionTrait>(db: &C, authority: &records::JobAuthority) -> Result<bool> {
    for identity in authority.owner.iter().chain(authority.source.iter()) {
        let decision =
            crate::store::pinning::decision::read_for_version(db, &identity.private_version_id)
                .await
                .map_err(database)?;
        if decision.is_some_and(|decision| decision.effect == DecisionEffect::Skipped) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn explicit_reapply<'a>(
    binding: impl Into<records::BindingView<'a>>,
    action: &records::CommittedAction,
    authority: &records::JobAuthority,
) -> bool {
    let binding = binding.into();
    let entry = &action.receipt.binding;
    let (
        crate::pinning::management::Action::Reapply(intent),
        Some(Capture::Object(capture)),
        Some(decision),
    ) = (&entry.request.action, &entry.capture, &action.decision)
    else {
        return false;
    };
    binding.confirmation.bindings.contains(entry)
        && action.jobs.contains(authority)
        && authority.directive == records::JobDirective::Submit
        && authority.owner.as_ref() == Some(&capture.owner_identity)
        && authority.source.as_ref() == Some(&capture.object.identity)
        && decision.owner == capture.owner_identity
        && decision.intent == *intent
        && binding.confirmation.configuration.as_ref() == Some(&decision.configuration)
        && capture
            .object
            .read_evidence
            .as_ref()
            .and_then(|summary| accounting::count(summary).ok())
            .is_some_and(|count| count.bytes == decision.stored_bytes)
        && action.receipt.intent_ids.len() == 1
        && action.receipt.intent_ids.first() == Some(&decision.id)
        && authority.lease_id.as_ref() == Some(&decision.id)
        && authority.lease_source.as_ref() == Some(&decision.id)
        && authority
            .target_id
            .as_ref()
            .is_some_and(|target| action.targets.contains(target))
}

async fn denied<C: ConnectionTrait>(
    db: &C,
    authority: &records::JobAuthority,
    work: records::SharedDecodeBudget,
) -> Result<bool> {
    if !skipped(db, authority).await? {
        return Ok(false);
    }
    let job = pin_job::Entity::find_by_id(authority.job_id.clone())
        .one(db)
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Confirmation)?;
    for trusted in
        crate::store::pinning::management::admission::authority::actions_for_job(db, &job, work)
            .await?
    {
        for seed in &trusted.action_view().jobs {
            trusted.binding().copy(&(seed, &authority.job_id))?;
            let mut derived = seed.clone();
            derived.job_id.clone_from(&authority.job_id);
            if derived == *authority
                && explicit_reapply(trusted.binding(), trusted.action_view(), seed)
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub(super) async fn current<C: ConnectionTrait>(
    db: &C,
    authority: &records::JobAuthority,
    config: crate::pinning::management::local_config::ManagementConfigView<'_>,
    idle_timeout_millis: Option<u64>,
    now: DateTime<Utc>,
    work: records::SharedDecodeBudget,
) -> Result<bool> {
    let Some(identity) = &authority.source else {
        return Ok(false);
    };
    let frame = super::current_scope::capture(db, authority, work.clone()).await?;
    let rows = records::FrozenRows::Frame(&frame);
    let Some(proof) = records::DecodeWork::shared(&work).with(|work| {
        accounting::current_proof_from_source(
            rows,
            config.source_endpoint,
            config.source_node,
            identity,
            now,
            work,
        )
    })?
    else {
        return Ok(false);
    };
    if idle_timeout_millis != Some(proof.source.idle_timeout_millis)
        || !proof.proof.has_stored_byte_count()
        || denied(db, authority, work.clone()).await?
    {
        return Ok(false);
    }
    let decompressed = rows
        .table("zip_manifest_entries")
        .ok_or(ManagementError::Schema)?
        .rows
        .iter()
        .any(|row| {
            native::value(row, "version_row_id") == Some(&identity.private_version_id)
                && native::value(row, "error_code").is_none()
        });
    let tags = [ObjectTag::new("ipfs-s3:pin", "true")];
    let policy = match config.policy.evaluate_publication(PublicationContext {
        bucket: &identity.bucket,
        key: &identity.key,
        tags: &tags,
        is_decompress_zip: decompressed,
    }) {
        Ok(policy) => policy,
        Err(_) => return Ok(false),
    };
    if !policy.leases.iter().any(|lease| {
        lease.providers.iter().any(|name| {
            config.pinning.providers.iter().any(|provider| {
                provider.name == *name
                    && provider.limits.enabled
                    && !provider.identity.retired
                    && provider.route_snapshot() == authority.route
            })
        })
    }) {
        return Ok(false);
    }
    if decompressed {
        let output = crate::pinning::zip_policy::ZipPublishedOutput {
            bucket: identity.bucket.clone(),
            key: identity.key.clone(),
            version_id: identity.private_version_id.clone(),
            cid: identity.cid.clone(),
        };
        let plan = config
            .zip_rules
            .plan(
                crate::pinning::zip_policy::ZipTargets {
                    source: false,
                    extracted: true,
                },
                None,
                &[output],
            )
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if plan.outputs.is_empty()
            || plan.outputs.iter().any(|output| {
                output.warning == Some(crate::pinning::zip_policy::ZipPlanWarning::Denied)
            })
        {
            return Ok(false);
        }
    }
    if proof.proof.is_directory() && !matches!(authority.route.strategy.as_str(), "cid" | "car") {
        return Ok(false);
    }
    let provider =
        operational_resource_with_work(db, authority, records::DecodeWork::shared(&work)).await?;
    let count = accounting::count(&ReadEvidence::from(proof.as_ref()))?;
    records::DecodeWork::shared(&work).copy(&authority.cid)?;
    match accounting::check_current_with_work(frame, provider, authority.cid.clone(), count, work)
        .await
    {
        Ok(()) => {}
        Err(
            ManagementError::InvalidEvidence
            | ManagementError::Confirmation
            | ManagementError::Changed,
        ) => return Ok(false),
        Err(error) => return Err(error),
    }
    let completed = crate::store::database_clock::database_now(db)
        .await
        .map_err(database)?;
    Ok(proof.observed_at <= completed && completed < proof.expires_at)
}

/// Source/owner rows precede the existing lease/target/remote frontier. Their
/// exact private tuples cannot drift while publication is being revalidated.
pub(super) async fn lock(
    tx: &DatabaseTransaction,
    authority: &records::JobAuthority,
) -> Result<()> {
    if tx.get_database_backend() != sea_orm::DatabaseBackend::Postgres {
        return Ok(());
    }
    let mut buckets = std::collections::BTreeSet::new();
    let mut owners = std::collections::BTreeSet::new();
    let mut versions = std::collections::BTreeSet::new();
    for identity in authority.owner.iter().chain(authority.source.iter()) {
        buckets.insert(identity.bucket.as_str());
        owners.insert(identity.object_id.clone());
        versions.insert(identity.private_version_id.clone());
    }
    for name in buckets {
        tx.query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT name FROM buckets WHERE name=$1 FOR UPDATE NOWAIT",
            [name.into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    }
    for id in owners {
        tx.query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT id FROM objects WHERE id=$1 FOR UPDATE NOWAIT",
            [id.into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    }
    for id in versions {
        tx.query_one(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            "SELECT id FROM object_versions WHERE id=$1 FOR UPDATE NOWAIT",
            [id.into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    }
    Ok(())
}
