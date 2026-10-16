//! Admission at the existing MPU/import/ZIP transaction boundary. This is not
//! another policy decider, publication executor, or source-content proof.
use crate::{
    error::AppResult,
    pinning::{
        decision::ExtensionDecision, identity::ProviderRouteSnapshot, zip_policy::CapturedZipIntent,
    },
};
use sea_orm::{ConnectionTrait, DatabaseTransaction};
use std::collections::BTreeMap;

pub(crate) async fn lock_intake_in_caller_transaction<C: ConnectionTrait>(
    db: &C,
    decision: Option<&ExtensionDecision>,
    zip: Option<&CapturedZipIntent>,
) -> AppResult<()> {
    super::lock_sqlite_writer_in_caller_transaction(db).await?;
    let schema = super::locks::AdmissionSchema::read(db).await?;
    let mut routes = BTreeMap::<String, ProviderRouteSnapshot>::new();
    if let Some(zip) = zip {
        // The frozen selector already accounts for source=false, output rules
        // and frozen Cluster confirmation. Never derive old accounts from TOML.
        for candidate in zip
            .io_provider_candidates(None)
            .into_iter()
            .filter(|candidate| candidate.enabled)
        {
            if routes
                .insert(candidate.config_name.clone(), candidate.route.clone())
                .is_some_and(|old| old != candidate.route)
            {
                return Err(super::needs_attention());
            }
        }
    } else if let Some(decision) = decision {
        for key in decision
            .effective_intents
            .iter()
            .flat_map(|intent| &intent.providers)
        {
            if routes.contains_key(key) {
                continue;
            }
            let selected = super::domains::registered_routes(db, key).await?;
            if selected.is_empty() {
                if super::domains::unregistered_legacy(db, key).await? {
                    continue;
                }
                return Err(super::needs_attention());
            }
            for route in selected {
                routes.insert(key.clone(), route);
            }
        }
    }
    if routes.is_empty() {
        return schema.revalidate(db).await;
    }
    // No accounting sentinel or grant: absent rows are covered by the following
    // table fences, existing usage precedes retirement and route locks.
    super::locks::lock_existing_usage(db, &routes.keys().cloned().collect::<Vec<_>>()).await?;
    super::lock_in_caller_transaction(db, &routes.into_values().collect::<Vec<_>>()).await?;
    schema.revalidate(db).await
}

pub(crate) async fn lock_encoded_zip_intake_in_transaction(
    tx: &DatabaseTransaction,
    encoded: &str,
) -> AppResult<()> {
    super::lock_sqlite_writer_in_caller_transaction(tx).await?;
    let value: serde_json::Value =
        serde_json::from_str(encoded).map_err(|_| super::needs_attention())?;
    if value.get("zip_intent").is_none() && value.get("legacy_zip_intent").is_none() {
        // Retain only pre-registry metadata fixtures, not a production fallback
        // from missing original capture to today's executable policy/account.
        if super::domains::unregistered_legacy(tx, "zip-metadata-only").await? {
            return Ok(());
        }
        return Err(super::needs_attention());
    }
    let zip = crate::store::pinning::publication::decode_zip_capture_json(encoded)?;
    lock_intake_in_caller_transaction(tx, Some(zip.source_decision()), Some(&zip)).await
}
