//! Exact source and same-resource accounting closure for a worker read.
use super::*;
use native::{
    NativeKey,
    meter::Budget,
    scope::{ScopeSpec, Selector},
};
use records::locator::{self, AuthoritySelector, AuthorityView};
use std::collections::{BTreeMap, BTreeSet};

pub(super) async fn fence(
    tx: &DatabaseTransaction,
    job: &pin_job::Model,
    authority: &records::JobAuthority,
) -> Result<()> {
    let identity = authority
        .source
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let spec = ScopeSpec::new(vec![
        Selector::Exact(NativeKey {
            table: "objects".into(),
            values: vec![identity.object_id.clone()],
        }),
        Selector::ExactVersion {
            id: identity.private_version_id.clone(),
        },
        Selector::ManifestBindings {
            version_row_id: identity.private_version_id.clone(),
        },
        Selector::ResourceFamily {
            provider: job.provider.clone(),
            cid: job.cid.clone(),
        },
        Selector::OwnerLeases {
            object_id: identity.object_id.clone(),
        },
    ])?;
    native::scope::fence_memberships(tx, &spec).await
}

pub(super) async fn capture_catalog(
    catalog: ReceiptCatalog<'_>,
    authority: &records::JobAuthority,
    work: records::SharedDecodeBudget,
) -> Result<native::NativeFrame> {
    let mut budget = Budget::default();
    let result = capture_inner(catalog, authority, &work, &mut budget).await;
    crate::store::pinning::management::admission::authority::charge(
        &budget,
        records::DecodeWork::shared(&work),
    )?;
    result
}

#[cfg(test)]
async fn capture<C: ConnectionTrait>(
    db: &C,
    authority: &records::JobAuthority,
    work: records::SharedDecodeBudget,
) -> Result<native::NativeFrame> {
    capture_catalog(ReceiptCatalog::full(db), authority, work).await
}

async fn capture_inner(
    catalog: ReceiptCatalog<'_>,
    authority: &records::JobAuthority,
    work: &records::SharedDecodeBudget,
    budget: &mut Budget,
) -> Result<native::NativeFrame> {
    let db = &catalog;
    let context = records::DecodeWork::shared(work);
    let identity = authority
        .source
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let cid = crate::pinning::provider::canonical_resource_cid(&authority.cid)
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let domain = locator::canonical_domain(&authority.route)?;
    let provider = operational_resource_catalog(catalog, authority, context).await?;
    let mut predicates = BTreeSet::from([
        Selector::Exact(NativeKey {
            table: "objects".into(),
            values: vec![identity.object_id.clone()],
        }),
        Selector::ExactVersion {
            id: identity.private_version_id.clone(),
        },
        Selector::ManifestBindings {
            version_row_id: identity.private_version_id.clone(),
        },
    ]);
    let mut selectors = BTreeSet::from([AuthoritySelector::AttestationCid(cid.clone())]);
    let mut providers = BTreeSet::from([provider]);
    let mut reads = BTreeMap::new();
    let mut views = BTreeMap::<AuthoritySelector, AuthorityView>::new();
    loop {
        for provider in std::mem::take(&mut providers) {
            predicates.insert(Selector::ResourceFamily {
                provider,
                cid: cid.clone(),
            });
        }
        if let Some(predicate) = predicates.iter().find(|p| !reads.contains_key(*p)).cloned() {
            context.copy(&predicate)?;
            let spec = ScopeSpec::new(vec![predicate.clone()])?;
            let read = match catalog {
                ReceiptCatalog::Runtime(tx, _) => {
                    native::scope::collect_runtime_scope(tx, &spec, budget, context).await?
                }
                _ => native::scope::collect_scope(db, &spec, budget).await?,
            };
            for (key, row) in read.rows() {
                if key.table == "pin_lease_targets" {
                    let lease =
                        native::value(row, "lease_id").ok_or(ManagementError::InvalidEvidence)?;
                    selectors.insert(AuthoritySelector::Lease(lease.into()));
                    predicates.insert(Selector::Exact(NativeKey {
                        table: "pin_leases".into(),
                        values: vec![lease.into()],
                    }));
                }
            }
            reads.insert(predicate, read);
            continue;
        }
        let Some(selector) = selectors.iter().find(|s| !views.contains_key(*s)).cloned() else {
            break;
        };
        context.copy(&selector)?;
        let view = match catalog {
            ReceiptCatalog::Runtime(tx, _) => {
                locator::discover_runtime_authority(
                    tx,
                    std::slice::from_ref(&selector),
                    budget,
                    context,
                )
                .await?
            }
            _ => locator::discover_authority(db, std::slice::from_ref(&selector), budget).await?,
        };
        providers.extend(view.operational_keys(&domain));
        for (key, row) in &view.originals {
            if key.table == "pin_management_records"
                && native::value(row, "kind") == Some("evidence_preparation")
            {
                let records::RecordPayload::Evidence { attestation, .. } =
                    records::inspect_original_row_with_work(&key.table, row, context)?
                else {
                    return Err(ManagementError::InvalidEvidence);
                };
                predicates.insert(Selector::Exact(NativeKey {
                    table: "objects".into(),
                    values: vec![attestation.identity.object_id.clone()],
                }));
                predicates.insert(Selector::ExactVersion {
                    id: attestation.identity.private_version_id.clone(),
                });
            }
        }
        views.insert(selector, view);
    }
    let frame = native::NativeFrame::from_scoped_reads(
        &reads.into_values().collect::<Vec<_>>(),
        &views.into_values().collect::<Vec<_>>(),
        budget,
    )?;
    // Includes empty memberships, private NULL cells and every exact authority
    // generation. Worker write callers retain the ordered tail fences.
    native::scope::check_frame_scope(db, &frame, budget).await?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Statement;

    #[tokio::test]
    async fn current_source_excludes_unrelated_oversized_objects() {
        let fixture = crate::stage7_management_test_support::Fixture::new().await;
        let identity = fixture.publish("scoped-current-source", false).await;
        let other = fixture.publish("same-cid-other-owner", false).await;
        assert_eq!(identity.cid, other.cid);
        let binding = fixture
            .confirm(&fixture.create_request("scoped-current-source", identity.clone(), false))
            .await;
        let applied =
            super::super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
                .await
                .unwrap();
        let seed = applied.items[0]
            .jobs
            .iter()
            .find(|j| j.directive == records::JobDirective::Submit)
            .unwrap();
        fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
            "INSERT INTO objects(id,bucket,key,cid,size,etag,metadata,encrypted,multipart,is_latest,created_at) SELECT 'unrelated-large',bucket,'unrelated-large',cid,size,etag,$1,encrypted,multipart,is_latest,created_at FROM objects WHERE id=$2",
            [serde_json::json!({"padding":"x".repeat(native::MAX_BYTES+1)}).to_string().into(),identity.object_id.into()])).await.unwrap();
        let frame = capture(&fixture.db, seed, records::SharedDecodeBudget::default())
            .await
            .unwrap();
        assert!(frame.row("objects", &["unrelated-large"]).is_none());
        assert!(
            frame
                .row("object_versions", &[&identity.private_version_id])
                .is_some()
        );
        assert!(
            frame
                .row("object_versions", &[&other.private_version_id])
                .is_some()
        );
        let idle_timeout_millis = u64::try_from(
            crate::kubo::KuboClient::new(fixture.server.uri())
                .stream_idle_timeout()
                .as_millis(),
        )
        .unwrap();
        assert!(
            source::current(
                &fixture.db,
                seed,
                fixture.config.view(),
                Some(idle_timeout_millis),
                chrono::Utc::now(),
                records::SharedDecodeBudget::default()
            )
            .await
            .unwrap()
        );
    }
}
