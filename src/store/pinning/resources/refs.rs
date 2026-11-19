use std::collections::BTreeSet;

use chrono::{DateTime, TimeDelta, Utc};
use sea_orm::{ConnectionTrait, DatabaseConnection, DatabaseTransaction, TransactionTrait};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ProviderMode},
        policy::{
            ExistingManualLease, ExistingManualLeaseState, LeaseIntent, LeaseSource,
            ManualLeaseMutation,
        },
        provider::canonical_resource_cid,
        tags::ContentMode,
    },
    store::pinning::publication::PinTargetSpec,
};

use super::{batch::RefBatch, lock_resource, reconcile, sqlite_writer, stmt};

pub async fn attach(
    tx: &DatabaseTransaction,
    owner: &str,
    target: &PinTargetSpec,
    intent: &LeaseIntent,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    attach_targets(tx, std::iter::once((owner, target, intent)), limits, now).await
}

pub async fn attach_many(
    tx: &DatabaseTransaction,
    requests: &[(String, PinTargetSpec, LeaseIntent)],
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    attach_targets(
        tx,
        requests
            .iter()
            .map(|(owner, target, intent)| (owner.as_str(), target, intent)),
        limits,
        now,
    )
    .await
}

async fn attach_targets<'a>(
    tx: &DatabaseTransaction,
    targets: impl Iterator<Item = (&'a str, &'a PinTargetSpec, &'a LeaseIntent)>,
    limits: &ProviderLimitMap,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut writes = RefBatch::default();
    let mut prepared = Vec::new();
    let mut candidates = BTreeSet::new();
    for (owner, target, intent) in targets {
        let cid = canonical_resource_cid(&target.cid)
            .map_err(|_| AppError::InvalidPinningRequest("invalid pin target CID".into()))?;
        let expiry = TimeDelta::try_seconds(intent.duration.as_seconds() as i64)
            .and_then(|ttl| now.checked_add_signed(ttl))
            .ok_or_else(|| {
                AppError::InvalidPinningRequest("pin expiry exceeds timestamp range".into())
            })?;
        let source = match intent.source {
            LeaseSource::Automatic => "automatic",
            LeaseSource::Manual => "manual",
        };
        let content = match intent.content_mode {
            ContentMode::Object => "object",
            ContentMode::Decompressed => "decompressed",
        };
        let mut providers = Vec::new();
        for name in &intent.providers {
            let limit = limits.get(name).ok_or_else(|| {
                AppError::InvalidPinningRequest(format!("pin provider `{name}` is not configured"))
            })?;
            if limit.enabled {
                providers.push(name.as_str());
            } else if intent.provider_mode == ProviderMode::All {
                return Err(AppError::InvalidPinningRequest(format!(
                    "pin provider `{name}` is disabled"
                )));
            }
        }
        if providers.is_empty() {
            return Err(AppError::InvalidPinningRequest(
                "no enabled pin provider".into(),
            ));
        }
        if intent.provider_mode == ProviderMode::One && providers.len() > 1 {
            providers.sort_by_key(|&p| (limits[p].priority, p));
            candidates.extend(providers.iter().map(|&p| p.to_owned()));
        }
        prepared.push((
            owner,
            cid,
            target,
            intent,
            providers,
            source,
            content,
            expiry.timestamp_micros(),
        ));
    }
    let mut placement = if candidates.is_empty() {
        super::placement::Placement::default()
    } else {
        sqlite_writer(tx).await?;
        let owners = prepared.iter().map(|target| target.0.to_owned()).collect();
        let cids = prepared.iter().map(|target| target.1.clone()).collect();
        super::placement::Placement::load(tx, &cids, &owners, &candidates, limits).await?
    };
    for (owner, cid, target, intent, mut providers, source, content, expiry) in prepared {
        let size = target.logical_size;
        let remaining = if intent.provider_mode == ProviderMode::One && providers.len() > 1 {
            let chosen = placement.select(
                owner,
                &cid,
                source,
                &intent.policy_id,
                content,
                size,
                &providers,
                limits,
            );
            providers.retain(|&p| p != chosen && placement.can_serve(p, &cid, size, &limits[p]));
            let remaining =
                serde_json::to_string(&providers).expect("provider names serialize as strings");
            providers.clear();
            providers.push(chosen);
            remaining
        } else {
            "[]".into()
        };
        for provider in providers {
            placement.record(
                provider,
                &cid,
                size,
                owner,
                source,
                &intent.policy_id,
                content,
            );
            writes
                .resources
                .entry((provider, cid.clone()))
                .and_modify(|(_, original)| {
                    // CIDv0 identifies the dag-pb leaf convention when both spellings are known.
                    if target.cid.starts_with("Qm") {
                        *original = &target.cid;
                    }
                })
                .or_insert((size, target.cid.as_str()));
            writes
                .refs
                .entry((
                    owner,
                    provider,
                    cid.clone(),
                    source,
                    intent.policy_id.as_str(),
                    content,
                ))
                .and_modify(|current| current.0 = current.0.max(expiry))
                .or_insert((expiry, remaining.clone()));
        }
    }
    writes.flush(tx).await
}

pub async fn manual(
    db: &impl ConnectionTrait,
    owner: &str,
    now: DateTime<Utc>,
) -> AppResult<Option<ExistingManualLease>> {
    let Some(row) = db.query_one(stmt(db,"SELECT policy_id,content_mode,expires_at FROM pin_refs WHERE owner_object_id=$1 AND source='manual' ORDER BY expires_at DESC,policy_id,content_mode LIMIT 1",vec![owner.into()])).await? else {return Ok(None)};
    let micros = row.try_get::<i64>("", "expires_at")?;
    let expires_at = DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| AppError::Database("invalid pin expiry".into()))?;
    let content_mode = match row.try_get::<String>("", "content_mode")?.as_str() {
        "decompressed" => ContentMode::Decompressed,
        "object" => ContentMode::Object,
        _ => return Err(AppError::Database("invalid pin content mode".into())),
    };
    Ok(Some(ExistingManualLease {
        id: owner.to_owned(),
        policy_id: row.try_get("", "policy_id")?,
        content_mode,
        expires_at,
        generation: micros,
        state: if expires_at > now {
            ExistingManualLeaseState::Active
        } else {
            ExistingManualLeaseState::Expired
        },
    }))
}

pub async fn replace_manual(
    tx: &DatabaseTransaction,
    owner: &str,
    mutation: &ManualLeaseMutation,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if *mutation == ManualLeaseMutation::Keep {
        return Ok(());
    }
    mutate_owner(tx, owner, Some(mutation), now).await
}

pub async fn release_owner(
    tx: &DatabaseTransaction,
    owner: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    mutate_owner(tx, owner, None, now).await
}

async fn mutate_owner(
    tx: &DatabaseTransaction,
    owner: &str,
    mutation: Option<&ManualLeaseMutation>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    sqlite_writer(tx).await?;
    let source = if mutation.is_some() {
        " AND source='manual'"
    } else {
        ""
    };
    if tx.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        tx.query_all(stmt(tx,&format!("SELECT p.cid FROM pin_resources p WHERE EXISTS (
            SELECT 1 FROM pin_refs r WHERE r.provider=p.provider AND r.cid=p.cid AND r.owner_object_id=$1{source})
            ORDER BY p.provider,p.cid FOR UPDATE"),vec![owner.into()])).await?;
    }
    let mut values = vec![owner.into()];
    let command = match mutation {
        Some(ManualLeaseMutation::Renew { retain_until }) => {
            values.push(retain_until.timestamp_micros().into());
            "UPDATE pin_refs SET expires_at=$2"
        }
        _ => "DELETE FROM pin_refs",
    };
    // A failed submit may have moved the reference while its original resource was locked.
    let changed = tx
        .query_all(stmt(
            tx,
            &format!("{command} WHERE owner_object_id=$1{source} RETURNING provider,cid"),
            values,
        ))
        .await?;
    if changed.is_empty() && matches!(mutation, Some(ManualLeaseMutation::Renew { .. })) {
        return Err(AppError::InvalidPinningRequest(
            "manual pin no longer exists".into(),
        ));
    }
    let mut affected = BTreeSet::new();
    for row in changed {
        affected.insert((
            row.try_get::<String>("", "provider")?,
            row.try_get::<String>("", "cid")?,
        ));
    }
    for (provider, cid) in affected {
        lock_resource(tx, &provider, &cid).await?;
        reconcile(tx, &provider, &cid, now).await?;
    }
    Ok(())
}

pub async fn expire(db: &DatabaseConnection, now: DateTime<Utc>, limit: u32) -> AppResult<u64> {
    let tx = db.begin().await?;
    sqlite_writer(&tx).await?;
    let lock = if tx.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
        " FOR UPDATE OF p SKIP LOCKED"
    } else {
        ""
    };
    let rows=tx.query_all(stmt(&tx,&format!("SELECT r.owner_object_id,r.provider,r.cid,r.source,r.policy_id,r.content_mode FROM pin_refs r JOIN pin_resources p USING (provider,cid) WHERE r.expires_at<=$1 ORDER BY r.expires_at LIMIT $2{lock}"),
        vec![now.timestamp_micros().into(),i64::from(limit).into()])).await?;
    let mut keys = BTreeSet::new();
    let mut removed = 0;
    for row in rows {
        let provider: String = row.try_get("", "provider")?;
        let cid: String = row.try_get("", "cid")?;
        let key = vec![
            provider.clone().into(),
            cid.clone().into(),
            row.try_get::<String>("", "owner_object_id")?.into(),
            row.try_get::<String>("", "source")?.into(),
            row.try_get::<String>("", "policy_id")?.into(),
            row.try_get::<String>("", "content_mode")?.into(),
            now.timestamp_micros().into(),
        ];
        removed+=tx.execute(stmt(&tx,"DELETE FROM pin_refs WHERE provider=$1 AND cid=$2 AND owner_object_id=$3 AND source=$4 AND policy_id=$5 AND content_mode=$6 AND expires_at<=$7",key)).await?.rows_affected();
        keys.insert((provider, cid));
    }
    for (provider, cid) in keys {
        reconcile(&tx, &provider, &cid, now).await?;
    }
    tx.commit().await?;
    Ok(removed)
}
