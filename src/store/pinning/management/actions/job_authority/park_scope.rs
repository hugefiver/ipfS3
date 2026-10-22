//! Current allocation continuity for one parked resource, including empty sets.
use super::*;
use native::scope::{ScopeSpec, Selector};

pub(crate) async fn capture(
    tx: &DatabaseTransaction,
    job: &pin_job::Model,
    before: records::FrozenRows<'_>,
    work: records::DecodeWork<'_>,
) -> Result<native::NativeFrame> {
    let mut selectors = std::collections::BTreeSet::from([
        Selector::ResourceFamily {
            provider: job.provider.clone(),
            cid: job.cid.clone(),
        },
        Selector::Exact(native::NativeKey {
            table: "pin_provider_usage".into(),
            values: vec![job.provider.clone()],
        }),
    ]);
    for (table, row) in before.all_rows() {
        if native::value(row, "provider") != Some(&job.provider)
            || native::value(row, "cid") != Some(&job.cid)
        {
            continue;
        }
        match table.table.as_str() {
            "pin_lease_targets" => {
                let lease =
                    native::value(row, "lease_id").ok_or(ManagementError::InvalidEvidence)?;
                selectors.insert(Selector::Exact(native::NativeKey {
                    table: "pin_leases".into(),
                    values: vec![lease.into()],
                }));
                selectors.insert(Selector::LeaseRequirements {
                    lease_id: lease.into(),
                });
            }
            "pin_jobs" => {
                let id = native::value(row, "id").ok_or(ManagementError::InvalidEvidence)?;
                selectors.insert(Selector::JobAttempts { job_id: id.into() });
                selectors.insert(Selector::Exact(native::NativeKey {
                    table: "pin_invocation_routes".into(),
                    values: vec![id.into()],
                }));
            }
            _ => {}
        }
    }
    let selectors = selectors.into_iter().collect::<Vec<_>>();
    work.copy(&selectors)?;
    let spec = ScopeSpec::new(selectors)?;
    let mut budget = native::meter::Budget::default();
    let result = async {
        native::scope::fence_memberships(tx, &spec).await?;
        native::scope::collect_scope(tx, &spec, &mut budget)
            .await?
            .into_native_frame(&mut budget)
    }
    .await;
    crate::store::pinning::management::admission::authority::charge(&budget, work)?;
    result
}
