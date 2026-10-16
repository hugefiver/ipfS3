use super::*;
use crate::pinning::management::*;
use sea_orm::TransactionTrait;
#[path = "renew_mixed_tests.rs"]
mod renew_mixed_tests;

#[tokio::test]
async fn extended_renewal_binds_its_exact_observation_birth_and_preserves_old_work() {
    let fixture = super::super::tests::fixture_with_automatic("renew-birth", true).await;
    let lease = pin_lease::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = crate::store::entities::pin_lease_target::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let old_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "renew-exact-birth".into();
    request.items[0].action = Action::Renew {
        expires_at: lease.expires_at.timestamp() + 1800,
    };
    let Subject::Object {
        target: selection, ..
    } = &mut request.items[0].subject
    else {
        unreachable!()
    };
    *selection = TargetSelection::Existing {
        target_id: target.id,
        source: None,
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let before = binding.view().frozen_rows().unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = super::super::execution::ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &binding,
        before,
    )
    .unwrap();
    let executed = batch
        .execute_entry(&binding.confirmation.bindings[0])
        .await
        .unwrap();
    let action = executed.action_view();
    let authority = action
        .jobs
        .iter()
        .find(|authority| authority.directive == records::JobDirective::ReadOnlyObserve)
        .unwrap();
    let remote =
        remote_pin::Entity::find_by_id((authority.provider.clone(), authority.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
    let original_epoch = native::value(
        before
            .row("remote_pins", &[&authority.provider, &authority.cid])
            .unwrap(),
        "epoch",
    )
    .unwrap()
    .parse::<i64>()
    .unwrap();
    assert_eq!(authority.remote_epoch, original_epoch + 1);
    assert_eq!(remote.epoch, authority.remote_epoch);
    assert_eq!(authority.lease_generation, Some(lease.generation + 1));
    assert_eq!(authority.started_at, Some(lease.created_at));
    assert_eq!(
        authority.lease_source.as_deref(),
        Some(lease.source.as_str())
    );
    let job = pin_job::Entity::find_by_id(authority.job_id.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    validate_job_resolved(&tx, &job, authority).await.unwrap();
    assert_eq!(job.expected_remote_epoch, Some(remote.epoch));
    let proof = executed.validate_job(&binding, authority);
    assert!(
        proof.is_ok(),
        "real renewal birth epoch {original_epoch} -> {}: {proof:?}",
        authority.remote_epoch
    );
    for change in 0..10 {
        let mut invalid = authority.clone();
        match change {
            0 => invalid.remote_epoch = original_epoch,
            1 => invalid.remote_epoch += 1,
            2 => invalid.lease_generation = Some(lease.generation),
            3 => invalid.expires_at = Some(lease.expires_at),
            4 => invalid.route.scope = "foreign-account".into(),
            5 => invalid.resource_id = Some("foreign-request".into()),
            6 => invalid.owner.as_mut().unwrap().object_id = "foreign-owner".into(),
            7 => invalid.source.as_mut().unwrap().private_version_id = "foreign-source".into(),
            8 => invalid.lease_source = Some("foreign-source".into()),
            _ => invalid.target_id = Some("foreign-target".into()),
        }
        assert!(
            executed.validate_job(&binding, &invalid).is_err(),
            "invalid renewal fence {change} was admitted"
        );
    }
    records::write_executed_action_in_transaction(&fixture.auth, &binding, &executed, before)
        .await
        .unwrap();
    for original in &old_jobs {
        assert_eq!(
            pin_job::Entity::find_by_id(original.id.clone())
                .one(&tx)
                .await
                .unwrap()
                .as_ref(),
            Some(original)
        );
    }
    tx.rollback().await.unwrap();
    let applied = super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    super::super::execution::proof_tests::assert_restored(&fixture, &binding, &applied).await;
    for authority in &applied.items[0].jobs {
        let job = pin_job::Entity::find_by_id(authority.job_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(lookup(&fixture.db, &job).await.unwrap().as_ref() == Some(authority));
        assert!(matches!(
            check_before_call(&fixture.db, &job).await.unwrap(),
            Some(JobPermit::ReadOnly(_))
        ));
        assert!(!projection_allowed(&fixture.db, &job).await.unwrap());
    }
    assert!(
        super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap()
            .items
            == applied.items
    );
}
