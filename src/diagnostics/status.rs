//! Local administrator-only status. Do not expose this module through HTTP or
//! serialize ORM models: they contain CIDs, raw provider bodies and secret hashes.
use anyhow::{Result, anyhow};
use ipfs_s3_gateway::{
    pinning::decision::{DecisionEffect, WarningCode},
    store::{
        entities::{pin_lease, pin_lease_target, remote_pin, remote_pin_ledger},
        object_version::{self, PublicVersionId, VersionKind, VersionSelector},
        pinning::decision,
    },
};
use sea_orm::{
    AccessMode, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, IsolationLevel, Statement, TransactionTrait,
};
use serde_json::{Value, json};

const PAGE_SIZE: usize = 50;
const MAX_OFFSET: u64 = 1_000_000;

fn unavailable() -> anyhow::Error {
    anyhow!("pinning status database unavailable (details withheld)")
}

fn invalid_selection() -> anyhow::Error {
    anyhow!("pinning status selection invalid or unavailable (details withheld)")
}

async fn connect_read_only(url: &str) -> Result<DatabaseConnection> {
    // Never accept an ephemeral SQLite database for an administrative read.
    if url.starts_with("sqlite:")
        && (url.starts_with("sqlite::memory:") || url.contains("mode=memory"))
    {
        return Err(unavailable());
    }
    if !url.starts_with("sqlite:")
        && !url.starts_with("postgres://")
        && !url.starts_with("postgresql://")
    {
        return Err(unavailable());
    }
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(1);
    if url.starts_with("sqlite:") {
        // Overrides even a configured `mode=rwc`; missing files must fail, never
        // be created by the diagnostic process. Pool size one also makes the
        // connection-level query_only guard cover every statement below.
        options.map_sqlx_sqlite_opts(|opts| opts.read_only(true).create_if_missing(false));
    }
    let db = Database::connect(options)
        .await
        .map_err(|_| unavailable())?;
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.execute_unprepared("PRAGMA query_only = ON")
            .await
            .map_err(|_| unavailable())?;
    } else {
        db.execute_unprepared("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            .await
            .map_err(|_| unavailable())?;
    }
    Ok(db)
}

fn category(raw: Option<&str>) -> &'static str {
    match raw {
        None => "none",
        Some("timeout") => "timeout",
        Some("remote_failed") => "remote_failed",
        Some("rate_limited") => "rate_limited",
        Some(_) => "other_redacted",
    }
}

fn safe<'a>(value: &'a str, allowed: &[&'a str]) -> &'a str {
    allowed
        .iter()
        .copied()
        .find(|known| *known == value)
        .unwrap_or("unknown")
}

fn decision_report(
    decision: Option<ipfs_s3_gateway::pinning::decision::ExtensionDecision>,
) -> Value {
    match decision {
        None => json!({"state": "unknown_legacy_absent", "effect": "unknown"}),
        Some(decision) if decision.legacy_unknown => {
            json!({"state": "unknown_legacy_recorded", "effect": "unknown"})
        }
        Some(decision) => json!({
            "state": "captured",
            "effect": match decision.effect {
                DecisionEffect::Accepted => "accepted",
                DecisionEffect::Skipped => "skipped",
                DecisionEffect::NoIntent => "no_intent",
            },
            "warning_category": match decision.warning {
                Some(WarningCode::NoMatchingPolicy) => "no_matching_policy",
                Some(WarningCode::NoAvailableProvider) => "no_available_provider",
                None => "none",
            },
            "intent_count": decision.effective_intents.len(),
        }),
    }
}

async fn entry(
    db: &sea_orm::DatabaseTransaction,
    lease_id: &str,
    target_id: Option<&str>,
    index: u64,
) -> Result<Value> {
    let lease = pin_lease::Entity::find_by_id(lease_id.to_owned())
        .one(db)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(unavailable)?;
    let target = match target_id {
        Some(id) => Some(
            pin_lease_target::Entity::find_by_id(id.to_owned())
                .one(db)
                .await
                .map_err(|_| unavailable())?
                .filter(|target| target.lease_id == lease.id)
                .ok_or_else(unavailable)?,
        ),
        None => None,
    };
    let mut result = json!({
        "entry_index": index,
        "lease": {
            "source": safe(&lease.source, &["manual", "automatic", "decompressed"]),
            "state": safe(&lease.state, &["active", "expired", "cancelled", "evicted"]),
            "provider_mode": safe(&lease.provider_mode, &["one", "all"]),
            "created_at": lease.created_at,
            "expires_at": lease.expires_at,
        },
        "local": {"metadata_published": true, "target_state": "none"},
        "remote": {"stored_status": "unknown", "ledger_effect": "unknown", "ownership": "unknown", "observation": "database_only_not_live"},
        "verification": {"gateway_verified_at": null, "content_verified_at": null},
    });
    let Some(target) = target else {
        return Ok(result);
    };
    result["local"]["target_state"] = json!(safe(
        &target.state,
        &[
            "waiting",
            "submitted",
            "pinned",
            "degraded",
            "quota_waiting",
            "quota_blocked",
            "evicted",
            "released"
        ]
    ));
    let pair = (target.provider.clone(), target.cid.clone());
    let remote = remote_pin::Entity::find_by_id(pair.clone())
        .one(db)
        .await
        .map_err(|_| unavailable())?;
    let ledger = remote_pin_ledger::Entity::find_by_id(pair)
        .one(db)
        .await
        .map_err(|_| unavailable())?;
    result["remote"] = json!({
        "stored_status": remote.as_ref().map_or("unknown", |row| safe(&row.status, &["reserved", "queued", "pinning", "pinned", "failed", "absent"])),
        "ledger_effect": ledger.as_ref().map_or("unknown", |row| safe(&row.effect, &["reserved", "not_created", "confirmed", "retained", "cleanup_pending", "absent", "unknown"])),
        "ownership": ledger.as_ref().map_or("unknown", |row| safe(&row.ownership, &["unknown", "application_created", "external_existing"])),
        "observation": "database_only_not_live",
        "first_observed_at": ledger.as_ref().and_then(|row| row.first_observed_at),
        "last_observed_at": ledger.as_ref().and_then(|row| row.last_observed_at),
        "remote_pinned_at": ledger.as_ref().and_then(|row| row.remote_pinned_at),
        "first_error_category": category(ledger.as_ref().and_then(|row| row.first_error.as_deref())),
        "last_error_category": category(ledger.as_ref().and_then(|row| row.last_error.as_deref()).or(remote.as_ref().and_then(|row| row.last_error_class.as_deref()))),
    });
    result["verification"] = json!({
        "gateway_verified_at": ledger.as_ref().and_then(|row| row.gateway_verified_at),
        "content_verified_at": ledger.as_ref().and_then(|row| row.content_verified_at),
    });
    Ok(result)
}

pub async fn snapshot(
    url: &str,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    cursor: Option<&str>,
) -> Result<Value> {
    let offset = match cursor {
        None => 0,
        Some(value)
            if value == "0"
                || (!value.starts_with('0')
                    && !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_digit())) =>
        {
            value.parse::<u64>().map_err(|_| invalid_selection())?
        }
        _ => return Err(invalid_selection()),
    };
    if offset > MAX_OFFSET || bucket.is_empty() || key.is_empty() {
        return Err(invalid_selection());
    }
    let selector = match version_id {
        Some(id) => {
            VersionSelector::Exact(PublicVersionId::parse_s3(id).map_err(|_| invalid_selection())?)
        }
        None => VersionSelector::Current,
    };
    let db = connect_read_only(url).await?;
    let txn = db
        .begin_with_config(
            if db.get_database_backend() == DatabaseBackend::Postgres {
                Some(IsolationLevel::RepeatableRead)
            } else {
                None
            },
            if db.get_database_backend() == DatabaseBackend::Postgres {
                Some(AccessMode::ReadOnly)
            } else {
                None
            },
        )
        .await
        .map_err(|_| unavailable())?;
    let version = object_version::resolve_version(&txn, bucket, key, &selector)
        .await
        .map_err(|_| invalid_selection())?;
    let effect = if version.kind == VersionKind::Object {
        decision_report(
            decision::read_for_version(&txn, &version.id)
                .await
                .map_err(|_| unavailable())?,
        )
    } else {
        json!({"state": "not_applicable_delete_marker", "effect": "none"})
    };
    let mut entries = Vec::new();
    let mut next_cursor = None;
    if let Some(owner) = version.object.as_ref() {
        // A LEFT JOIN retains leases without targets. Only 51 IDs are loaded;
        // the remaining fields are retrieved one by one within the same snapshot.
        let sql = if db.get_database_backend() == DatabaseBackend::Postgres {
            "SELECT l.id AS lease_id, t.id AS target_id FROM pin_leases l LEFT JOIN pin_lease_targets t ON t.lease_id = l.id WHERE l.owner_object_id = $1 ORDER BY l.created_at, l.id, t.created_at, t.id LIMIT $2 OFFSET $3"
        } else {
            "SELECT l.id AS lease_id, t.id AS target_id FROM pin_leases l LEFT JOIN pin_lease_targets t ON t.lease_id = l.id WHERE l.owner_object_id = ? ORDER BY l.created_at, l.id, t.created_at, t.id LIMIT ? OFFSET ?"
        };
        let rows = txn
            .query_all(Statement::from_sql_and_values(
                db.get_database_backend(),
                sql,
                vec![
                    owner.id.clone().into(),
                    ((PAGE_SIZE + 1) as i64).into(),
                    (offset as i64).into(),
                ],
            ))
            .await
            .map_err(|_| unavailable())?;
        if rows.len() > PAGE_SIZE {
            next_cursor = Some((offset + PAGE_SIZE as u64).to_string());
        }
        for (position, row) in rows.iter().take(PAGE_SIZE).enumerate() {
            let lease_id: String = row.try_get("", "lease_id").map_err(|_| unavailable())?;
            let target_id: Option<String> =
                row.try_get("", "target_id").map_err(|_| unavailable())?;
            entries.push(
                entry(
                    &txn,
                    &lease_id,
                    target_id.as_deref(),
                    offset + position as u64,
                )
                .await?,
            );
        }
    }
    txn.commit().await.map_err(|_| unavailable())?;
    Ok(json!({
        "database_evidence": "read_only_snapshot_of_configured_database_not_running_gateway_state",
        "selection": if version_id.is_some() { "explicit_version" } else { "current_version" },
        "object_kind": if version.kind == VersionKind::Object { "object" } else { "delete_marker" },
        "local": {"metadata_published": version.object.is_some()},
        "decision": effect,
        "entries": entries,
        "page_size": PAGE_SIZE,
        "next_cursor": next_cursor,
        "consistency": "one_read_only_database_transaction_per_page_concurrent_changes_can_shift_offsets",
        "remote_observation": "not_performed_stored_evidence_only",
    }))
}
