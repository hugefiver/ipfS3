//! Fresh local membership boundaries. Original reviewed authority remains the
//! record layer's responsibility; these reads never grant imported authority.
use super::*;
use crate::pinning::management::db_snapshot::{PlanningScope, SnapshotError};
use native::{NativeKey, NativeRow, meter::Budget};
use records::locator::AuthorityProof;
use sea_orm::DatabaseTransaction;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(super) type ExpectedRows<'a> = BTreeMap<NativeKey, Cow<'a, NativeRow>>;

pub(super) fn snapshot_error(error: SnapshotError) -> ManagementError {
    match error {
        SnapshotError::LimitExceeded => ManagementError::Budget,
        SnapshotError::DatabaseUnavailable => ManagementError::Database,
        SnapshotError::Changed => ManagementError::Changed,
        _ => ManagementError::InvalidEvidence,
    }
}

pub(super) async fn capture(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    binding: &records::ConfirmedBinding,
    work: records::SharedDecodeBudget,
) -> Result<Arc<PlanningScope>> {
    let mut budget = Budget::default();
    let scope =
        PlanningScope::capture_current(tx, &config.pinning, &binding.request, &work, &mut budget)
            .await
            .map_err(snapshot_error);
    super::super::admission::authority::charge(&budget, records::DecodeWork::shared(&work))?;
    scope.map(Arc::new)
}

pub(super) async fn validate_final(
    tx: &DatabaseTransaction,
    config: &ManagementConfig,
    binding: &records::ConfirmedBinding,
    initial: &PlanningScope,
    expected: &ExpectedRows<'_>,
    owned_sources: &BTreeSet<NativeKey>,
    work: records::SharedDecodeBudget,
) -> Result<()> {
    let context = records::DecodeWork::shared(&work);
    let mut budget = Budget::default();
    // The union keeps every original predicate, including empty memberships,
    // and expands new identities under their post-image predicate fences.
    let result = async {
        let fresh = Box::pin(PlanningScope::capture_fenced(
            tx,
            &config.pinning,
            &binding.request,
            initial,
            &work,
            &mut budget,
        ))
        .await
        .map_err(snapshot_error)?;
        if fresh.database_scope != initial.database_scope
            || fresh.database_identity != initial.database_identity
        {
            return Err(ManagementError::Changed);
        }
        if fresh.originals().len() != expected.len()
            || fresh.originals().iter().any(|(key, row)| {
                expected
                    .get(key)
                    .is_none_or(|expected| expected.as_ref() != row)
            })
        {
            return Err(ManagementError::Changed);
        }
        let before = initial.authority_reads().iter().map(|r| &r.proof).chain(
            initial
                .business_reads()
                .iter()
                .flat_map(|r| r.authority_proofs()),
        );
        let after = fresh.authority_reads().iter().map(|r| &r.proof).chain(
            fresh
                .business_reads()
                .iter()
                .flat_map(|r| r.authority_proofs()),
        );
        validate_proofs(before, after, owned_sources)?;
        budget.check()
    }
    .await;
    super::super::admission::authority::charge(&budget, context)?;
    result
}

fn validate_proofs<'a>(
    before: impl Iterator<Item = &'a AuthorityProof>,
    after: impl Iterator<Item = &'a AuthorityProof>,
    owned_sources: &BTreeSet<NativeKey>,
) -> Result<()> {
    let mut seals = BTreeMap::new();
    let mut memberships = BTreeMap::new();
    for proof in after {
        for seal in &proof.seals {
            if seals
                .insert(&seal.source, seal)
                .is_some_and(|old| old != seal)
            {
                return Err(ManagementError::Changed);
            }
        }
        for member in &proof.memberships {
            if memberships
                .insert(&member.selector, &member.sources)
                .is_some_and(|old| old != &member.sources)
            {
                return Err(ManagementError::Changed);
            }
        }
    }
    for proof in before {
        for seal in &proof.seals {
            let current = seals.get(&seal.source).ok_or(ManagementError::Changed)?;
            if !owned_sources.contains(&seal.source) && **current != *seal {
                return Err(ManagementError::Changed);
            }
        }
        for member in &proof.memberships {
            let current = memberships
                .get(&member.selector)
                .ok_or(ManagementError::Changed)?;
            let old = member.sources.iter().collect::<BTreeSet<_>>();
            let new = current.iter().collect::<BTreeSet<_>>();
            if old
                .symmetric_difference(&new)
                .any(|key| !owned_sources.contains(*key))
            {
                return Err(ManagementError::Changed);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::pinning::management::records::locator::{
        self, AuthorityMembership, AuthoritySelector, SourceSeal,
    };

    fn key(item: &str) -> NativeKey {
        NativeKey {
            table: "pin_management_records".into(),
            values: vec!["scope".into(), "plan".into(), item.into(), "action".into()],
        }
    }

    fn proof(keys: &[NativeKey], generation: i64) -> AuthorityProof {
        AuthorityProof {
            memberships: vec![AuthorityMembership {
                selector: AuthoritySelector::RecordMembers("scope".into(), "plan".into()),
                sources: keys.to_vec(),
            }],
            seals: keys
                .iter()
                .map(|key| SourceSeal {
                    source: key.clone(),
                    source_key: locator::source_key(key).unwrap(),
                    source_generation: generation,
                    locator_generation: generation,
                    sealed_source_generation: generation,
                    sealed_locator_generation: generation,
                })
                .collect(),
        }
    }

    #[test]
    fn receipt_transition_is_exact_not_a_plan_wide_exemption() {
        let own = key("owned");
        let before = proof(std::slice::from_ref(&own), 1);
        let after = proof(std::slice::from_ref(&own), 2);
        let owned = BTreeSet::from([own.clone()]);
        assert!(validate_proofs(std::iter::once(&before), std::iter::once(&after), &owned).is_ok());
        assert!(
            validate_proofs(
                std::iter::once(&before),
                std::iter::once(&after),
                &BTreeSet::new()
            )
            .is_err()
        );
        let phantom = proof(&[own, key("unreviewed-member")], 2);
        assert!(
            validate_proofs(std::iter::once(&before), std::iter::once(&phantom), &owned).is_err()
        );
    }

    #[test]
    fn empty_authority_membership_is_still_fenced_after_execution() {
        let before = proof(&[], 1);
        let after = proof(&[key("phantom")], 1);
        assert!(
            validate_proofs(
                std::iter::once(&before),
                std::iter::once(&after),
                &BTreeSet::new()
            )
            .is_err()
        );
        assert!(
            validate_proofs(
                std::iter::once(&before),
                std::iter::empty(),
                &BTreeSet::new()
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn final_scope_changed_attempts_charge_storage_into_the_same_session() {
        use sea_orm::TransactionTrait;

        let fixture = super::super::tests::fixture("final-scope-debt").await;
        let tx = fixture.db.begin().await.unwrap();
        let initial = capture(
            &tx,
            &fixture.config,
            &fixture.binding,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        assert!(!initial.originals().is_empty());
        let work = records::SharedDecodeBudget::default();
        for _ in 0..2 {
            let before = work.lock().unwrap().storage_usage();
            assert_eq!(
                validate_final(
                    &tx,
                    &fixture.config,
                    &fixture.binding,
                    &initial,
                    &ExpectedRows::new(),
                    &BTreeSet::new(),
                    work.clone(),
                )
                .await,
                Err(ManagementError::Changed)
            );
            let after = work.lock().unwrap().storage_usage();
            assert!(after.sql > before.sql);
            assert!(after.rows > before.rows);
            assert!(after.raw_bytes > before.raw_bytes);
        }
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn final_scope_capture_failure_preserves_error_and_settles_storage() {
        use sea_orm::{ConnectionTrait, TransactionTrait};

        let fixture =
            super::super::tests::fixture_with_automatic("final-scope-capture-error", true).await;
        let tx = fixture.db.begin().await.unwrap();
        let initial = capture(
            &tx,
            &fixture.config,
            &fixture.binding,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        let changed = tx
            .execute_unprepared("UPDATE remote_pin_ledger SET route='{}'")
            .await
            .unwrap();
        assert_eq!(changed.rows_affected(), 1);
        let mut measured = Budget::default();
        let original = PlanningScope::capture_fenced(
            &tx,
            &fixture.config.pinning,
            &fixture.binding.request,
            &initial,
            &records::SharedDecodeBudget::default(),
            &mut measured,
        )
        .await
        .err()
        .map(snapshot_error)
        .unwrap();
        assert_ne!(original, ManagementError::Budget);
        assert!(measured.work.sql > 0);
        let work = records::SharedDecodeBudget::default();
        let result = validate_final(
            &tx,
            &fixture.config,
            &fixture.binding,
            &initial,
            &ExpectedRows::new(),
            &BTreeSet::new(),
            work.clone(),
        )
        .await;
        assert_eq!(result, Err(original));
        assert_eq!(work.lock().unwrap().storage_usage().sql, measured.work.sql);
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn final_scope_budget_exhaustion_precedes_changed_and_retains_debt() {
        use native::meter::{Limits, WorkMeter};
        use sea_orm::TransactionTrait;

        let fixture = super::super::tests::fixture("final-scope-budget-error").await;
        let tx = fixture.db.begin().await.unwrap();
        let initial = capture(
            &tx,
            &fixture.config,
            &fixture.binding,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        let work = records::SharedDecodeBudget::default();
        work.lock()
            .unwrap()
            .charge_storage(WorkMeter {
                rows: Limits::default().rows - 1,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            validate_final(
                &tx,
                &fixture.config,
                &fixture.binding,
                &initial,
                &ExpectedRows::new(),
                &BTreeSet::new(),
                work.clone(),
            )
            .await,
            Err(ManagementError::Budget)
        );
        {
            let session = work.lock().unwrap();
            assert!(session.storage_usage().rows > Limits::default().rows);
            assert!(session.storage_usage().sql > 0);
            assert!(session.check().is_err());
        }
        tx.rollback().await.unwrap();
    }
}
