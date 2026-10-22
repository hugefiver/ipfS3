//! Purpose-specific memberships share the same source generations and tombstones.
use super::*;

pub(super) async fn bind_retirement_domain<C: ConnectionTrait>(
    db: &C,
    key: &NativeKey,
    row: &NativeRow,
    class: &mut SourceClassification,
    budget: &mut Budget,
) -> Result<()> {
    if key.table != "pin_management_retirements" {
        return Ok(());
    }
    let action = NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            required(row, "database_scope")?.into(),
            required(row, "plan_id")?.into(),
            required(row, "item_id")?.into(),
            "action".into(),
        ],
    };
    let (original, _resident) = scope::read_exact(db, &action, budget).await?;
    let body = required(&original, "result")?
        .strip_prefix("S7R1\n")
        .ok_or(ManagementError::InvalidEvidence)?;
    let results::DecodedResult::Facts(action, _, _) =
        results::decode_facts(body, &mut DecodeBudget::default())?;
    let crate::pinning::management::Subject::Provider { route } =
        &action.receipt.binding.request.subject
    else {
        return Err(ManagementError::InvalidEvidence);
    };
    let domain = canonical_domain(
        route
            .original
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?,
    )?;
    class
        .selectors
        .insert(AuthoritySelector::Domain(domain.clone()));
    class
        .selectors
        .insert(AuthoritySelector::RetirementDomain(domain));
    Ok(())
}

pub(super) fn extend(
    key: &NativeKey,
    row: &NativeRow,
    set: &mut BTreeSet<AuthoritySelector>,
) -> Result<()> {
    match key.table.as_str() {
        "pin_provider_routes" => {
            set.insert(AuthoritySelector::RegisteredRoute(
                required(row, "provider")?.into(),
            ));
            let route: crate::pinning::identity::ProviderRouteSnapshot =
                serde_json::from_str(required(row, "snapshot")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            set.insert(AuthoritySelector::RegisteredDomain(canonical_domain(
                &route,
            )?));
        }
        "pin_management_retirements" => {
            set.insert(AuthoritySelector::RetirementDomain(
                required(row, "allocation_domain")?.into(),
            ));
        }
        "pin_management_records" if native::value(row, "kind") == Some("evidence_preparation") => {
            let RecordPayload::Evidence { attestation, .. } =
                inspect_original_row(&key.table, row)?
            else {
                return Err(ManagementError::InvalidEvidence);
            };
            let cid = crate::pinning::provider::canonical_resource_cid(&attestation.identity.cid)
                .map_err(|_| ManagementError::InvalidEvidence)?;
            set.insert(AuthoritySelector::AttestationCid(cid));
        }
        "pin_legacy_migrations" => {
            let declaration: crate::store::pinning::legacy_migration::Declaration =
                serde_json::from_str(required(row, "declaration")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            let confirmation: crate::store::pinning::legacy_migration::Confirmation =
                serde_json::from_str(required(row, "confirmation")?)
                    .map_err(|_| ManagementError::InvalidEvidence)?;
            for ns in declaration.namespaces {
                for provider in [&ns.source, &ns.target] {
                    set.insert(AuthoritySelector::LegacyNamespace(provider.clone()));
                    for binding in &ns.bindings {
                        set.insert(AuthoritySelector::LegacyResource(
                            binding.table.clone(),
                            provider.clone(),
                            None,
                        ));
                        let original = confirmation
                            .before
                            .iter()
                            .filter(|image| image.table == binding.table)
                            .flat_map(|image| &image.rows)
                            .find(|original| {
                                binding.key.iter().all(|(column, value)| {
                                    native::value(*original, column) == Some(value.as_str())
                                })
                            });
                        let cid = binding.key.get("cid").map(String::as_str).or_else(|| {
                            original.and_then(|original| {
                                native::value(original, "cid")
                                    .or_else(|| native::value(original, "expected_cid"))
                            })
                        });
                        if let Some(cid) = cid {
                            set.insert(AuthoritySelector::LegacyResource(
                                binding.table.clone(),
                                provider.clone(),
                                Some(cid.into()),
                            ));
                        }
                        if binding.table == "pin_jobs" {
                            let id = binding
                                .key
                                .get("id")
                                .ok_or(ManagementError::InvalidEvidence)?;
                            set.insert(AuthoritySelector::LegacyJob(provider.clone(), id.clone()));
                        }
                    }
                }
            }
            // Original jobs may be captured in the receipt images even when the
            // explicit migration binding is the resource, not the job row.
            for image in confirmation
                .before
                .iter()
                .filter(|image| image.table == "pin_jobs")
            {
                for job in &image.rows {
                    let provider = required(job, "provider")?;
                    let id = required(job, "id")?;
                    for ns in &confirmation.declaration.namespaces {
                        if provider == ns.source || provider == ns.target {
                            for name in [&ns.source, &ns.target] {
                                set.insert(AuthoritySelector::LegacyJob(name.clone(), id.into()));
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}
