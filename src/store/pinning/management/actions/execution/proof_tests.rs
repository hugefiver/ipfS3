use super::*;
use crate::pinning::management::*;
use crate::store::entities::{pin_job, pin_lease, pin_lease_target, remote_pin};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

async fn fixture(
    label: &str,
    mixed: bool,
    blocked: bool,
    kept: bool,
) -> super::super::tests::Fixture {
    let mut fixture = super::super::tests::fixture_with_automatic(label, true).await;
    if kept {
        fixture
            .db
            .execute_unprepared(
                "UPDATE pin_leases SET expires_at=strftime('%Y-%m-%d %H:%M:%S',expires_at)",
            )
            .await
            .unwrap();
    }
    let lease = pin_lease::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    if blocked {
        let provider = fixture.config.pinning.providers[0].name.clone();
        fixture.config.pinning.providers[0].limits.max_bytes = 9;
        fixture
            .config
            .pinning
            .provider_limits
            .get_mut(&provider)
            .unwrap()
            .max_bytes = 9;
    }
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = format!("{label}-renew");
    request.items[0].item_id = "a-create".into();
    let mut renew = request.items[0].clone();
    renew.item_id = "b-renew".into();
    renew.action = Action::Renew {
        expires_at: lease.expires_at.timestamp() + if kept { 0 } else { 1800 },
    };
    let Subject::Object {
        target: selection, ..
    } = &mut renew.subject
    else {
        unreachable!()
    };
    *selection = TargetSelection::Existing {
        target_id: target.id,
        source: None,
    };
    if mixed {
        request.items.push(renew);
    } else {
        request.items = vec![renew];
    }
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_)))
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    fixture.binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    fixture
}

pub(crate) async fn assert_restored(
    fixture: &super::super::tests::Fixture,
    binding: &records::ConfirmedBinding,
    applied: &ApplyResult,
) {
    let scope = super::super::super::admission::ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        binding,
    )
    .await
    .unwrap();
    scope.revalidate(&fixture.db).await.unwrap();
    let report = scope.report(&fixture.db).await.unwrap();
    assert_eq!(report.items.len(), applied.items.len());
    let history = native::capture(&fixture.db).await.unwrap();
    let actions = records::frame_actions(&history).unwrap();
    for action in &applied.items {
        let item = &action.receipt.binding.request.item_id;
        let trusted = records::lookup_trusted_committed(&fixture.db, &fixture.auth, binding, item)
            .await
            .unwrap()
            .unwrap();
        assert!(trusted.action_view() == action);
        let historical = actions
            .iter()
            .find(|stored| stored.record_key() == trusted.record_key())
            .unwrap();
        assert!(historical.action_view() == action);
        assert!(
            records::lookup_committed(&fixture.db, &fixture.auth, binding, item)
                .await
                .unwrap()
                .as_ref()
                == Some(action)
        );
        assert!(
            super::super::status::lookup(&fixture.db, &fixture.auth, binding, item)
                .await
                .unwrap()
                .is_some()
        );
        for authority in &action.jobs {
            trusted.validate_job(authority).unwrap();
            let job = pin_job::Entity::find_by_id(authority.job_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            assert!(
                super::super::job_authority::lookup(&fixture.db, &job)
                    .await
                    .unwrap()
                    .as_ref()
                    == Some(authority)
            );
            if matches!(action.receipt.binding.request.action, Action::Renew { .. }) {
                assert!(
                    !super::super::job_authority::projection_allowed(&fixture.db, &job)
                        .await
                        .unwrap()
                );
            }
        }
        let route = fixture.config.pinning.providers[0].route_snapshot();
        let domain = super::super::provider_points::Domain {
            allocation: fixture.config.pinning.providers[0].name.clone(),
            backend: route.backend.clone(),
            scope: route.scope.clone(),
            resource_type: route.resource_type(),
        };
        let namespace = super::super::provider_points::NamespaceChange {
            source: domain.clone(),
            destination: domain,
            destination_route: route,
        };
        let original =
            super::super::provider_points::original_record(&fixture.db, trusted.record_key())
                .await
                .unwrap()
                .unwrap();
        let inspected = super::super::provider_points::inspect(
            &fixture.db,
            trusted.record_key(),
            &original,
            &namespace,
        )
        .await
        .unwrap();
        assert!(inspected.class() == super::super::provider_points::RecordClass::Applied);
    }
    let replay = super::super::apply(&fixture.db, &fixture.config, &fixture.auth, binding)
        .await
        .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
}

#[tokio::test]
async fn actual_grant_prefix_and_entry_local_job_birth_restore_across_consumers() {
    let fixture = fixture("proof-grant", true, false, false).await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let initial = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let old_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    let creation = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    let advanced = remote_pin::Entity::find_by_id((initial.provider.clone(), initial.cid.clone()))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(advanced.epoch, initial.epoch + 1);
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &creation,
        before,
    )
    .await
    .unwrap();
    let prior = pin_job::Entity::find().all(&tx).await.unwrap();
    let renewal = batch
        .execute_entry(&fixture.binding.confirmation.bindings[1])
        .await
        .unwrap();
    let after = remote_pin::Entity::find_by_id((initial.provider.clone(), initial.cid.clone()))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.epoch, initial.epoch + 2);
    assert!(
        renewal
            .action_view()
            .jobs
            .iter()
            .all(|authority| authority.remote_epoch == after.epoch
                && !prior.iter().any(|job| job.id == authority.job_id))
    );
    assert!(
        renewal
            .operation
            .facts
            .3
            .iter()
            .all(|birth| !prior.iter().any(|job| native::value(
                renewal.operation.row(RowRef::Output(birth.0)).unwrap(),
                "id"
            ) == Some(&job.id)))
    );
    for job in &prior {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&tx)
                .await
                .unwrap()
                .as_ref(),
            Some(job)
        );
    }
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &renewal,
        before,
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    for job in &old_jobs {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .as_ref(),
            Some(job)
        );
    }
    assert_restored(&fixture, &fixture.binding, &applied).await;
}

#[tokio::test]
async fn kept_native_renewal_has_no_epoch_generation_or_birth() {
    let fixture = fixture("proof-kept", false, false, true).await;
    let lease = pin_lease::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert!(applied.items[0].jobs.is_empty());
    assert_eq!(
        pin_lease::Entity::find_by_id(lease.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .as_ref(),
        Some(&lease)
    );
    assert_eq!(
        remote_pin::Entity::find_by_id((remote.provider.clone(), remote.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .as_ref(),
        Some(&remote)
    );
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        jobs
    );
    assert_restored(&fixture, &fixture.binding, &applied).await;
}

pub(super) async fn observation_prefix_fixture() -> super::super::tests::Fixture {
    let mut fixture =
        super::super::tests::fixture_with_automatic("proof-entry-local-base", true).await;
    let automatic = pin_lease_target::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let lease = pin_lease::Entity::find_by_id(automatic.lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let created = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "proof-entry-local-renew".into();
    request.items[0].item_id = "a-observe".into();
    request.items[0].action = Action::Reconcile;
    let Subject::Object {
        target: selection, ..
    } = &mut request.items[0].subject
    else {
        unreachable!()
    };
    *selection = TargetSelection::Existing {
        target_id: created.items[0].targets[0].clone(),
        source: None,
    };
    let mut renew = request.items[0].clone();
    renew.item_id = "b-renew".into();
    renew.action = Action::Renew {
        expires_at: lease.expires_at.timestamp() + 1800,
    };
    let Subject::Object {
        target: selection, ..
    } = &mut renew.subject
    else {
        unreachable!()
    };
    *selection = TargetSelection::Existing {
        target_id: automatic.id,
        source: None,
    };
    request.items.push(renew);
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_)))
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    fixture.binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    fixture
}

#[tokio::test]
async fn preceding_entry_observation_keeps_its_original_authority_at_renewal() {
    let fixture = observation_prefix_fixture().await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    let creation = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    let original = creation
        .action_view()
        .jobs
        .iter()
        .find(|job| {
            job.directive == records::JobDirective::ReadOnlyObserve
                && before.row("pin_jobs", &[&job.job_id]).is_none()
        })
        .unwrap()
        .clone();
    let native = pin_job::Entity::find_by_id(original.job_id.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &creation,
        before,
    )
    .await
    .unwrap();
    let renewal = batch
        .execute_entry(&fixture.binding.confirmation.bindings[1])
        .await
        .unwrap();
    assert!(
        !renewal
            .action_view()
            .receipt
            .job_ids
            .contains(&original.job_id)
    );
    assert!(
        renewal
            .action_view()
            .jobs
            .iter()
            .any(|authority| authority.remote_epoch == original.remote_epoch + 1)
    );
    assert_eq!(
        pin_job::Entity::find_by_id(original.job_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .as_ref(),
        Some(&native)
    );
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &renewal,
        before,
    )
    .await
    .unwrap();
    assert!(
        super::super::job_authority::lookup(&tx, &native)
            .await
            .unwrap()
            .as_ref()
            == Some(&original)
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn invalid_second_execution_proof_rolls_back_native_work_and_written_prefix() {
    let fixture = fixture("proof-rollback", true, true, false).await;
    let leases = pin_lease::Entity::find().all(&fixture.db).await.unwrap();
    let targets = pin_lease_target::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let remotes = remote_pin::Entity::find().all(&fixture.db).await.unwrap();
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::ProofInvalidSecond,
    );
    let failure = super::super::apply_detailed(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .err()
    .unwrap();
    assert!(matches!(failure.cause, ManagementError::Confirmation));
    for entry in &fixture.binding.confirmation.bindings {
        assert!(
            records::lookup_committed(
                &fixture.db,
                &fixture.auth,
                &fixture.binding,
                &entry.request.item_id
            )
            .await
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(
        pin_lease::Entity::find().all(&fixture.db).await.unwrap(),
        leases
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        targets
    );
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        jobs
    );
    assert_eq!(
        remote_pin::Entity::find().all(&fixture.db).await.unwrap(),
        remotes
    );
}

#[tokio::test]
async fn persisted_proof_corruption_never_restores_or_downgrades_authority() {
    let fixture = fixture("proof-corrupt", true, true, false).await;
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let item = &applied.items[1].receipt.binding.request.item_id;
    let trusted =
        records::lookup_trusted_committed(&fixture.db, &fixture.auth, &fixture.binding, item)
            .await
            .unwrap()
            .unwrap();
    let key = trusted.record_key().clone();
    let original = super::super::provider_points::original_record(&fixture.db, &key)
        .await
        .unwrap()
        .unwrap();
    let packed = native::value(&original, "result").unwrap();
    // Use the real stored result after the storage-aware state CAS.
    let decoded = records::decode_action_result(packed).unwrap();
    assert!(decoded.1.as_ref() == Some(&applied.items[1]));
    let history = native::capture(&fixture.db).await.unwrap();
    let mut public: native::NativeFrame =
        serde_json::from_str(&serde_json::to_string(&history).unwrap()).unwrap();
    let table = public
        .tables
        .iter_mut()
        .find(|table| table.table == key.table)
        .unwrap();
    let row = table
        .rows
        .iter_mut()
        .find(|row| {
            native::value(row, "plan_id") == Some(&fixture.binding.request.plan_id)
                && native::value(row, "item_id") == Some(item)
        })
        .unwrap();
    for column in ["binding", "native_before", "result"] {
        row.insert(column.into(), original.get(column).unwrap().clone());
    }
    assert!(records::private_action(&public, &key).is_err());
    let mut wrong = key.clone();
    wrong.values[2] = "a-create".into();
    let private = history
        .private_material()
        .into_iter()
        .filter(|(candidate, _)| candidate == &wrong)
        .collect();
    public.restore_private(private).unwrap();
    assert!(public.has_private_material());
    assert!(records::private_action(&public, &key).is_err());
    let stored: serde_json::Value =
        serde_json::from_str(packed.strip_prefix("S7R1\n").unwrap()).unwrap();
    let invalid_fields = [
        ("/1/0", serde_json::json!(0)),
        ("/1/3", serde_json::json!([])),
        ("/1/4", serde_json::json!([])),
        ("/1/5", serde_json::json!(["cancel", [0, 0], [0, 0]])),
        (
            "/0/jobs/0/source/private_version_id",
            serde_json::json!("foreign-source"),
        ),
        (
            "/0/jobs/0/owner/object_id",
            serde_json::json!("foreign-owner"),
        ),
        (
            "/0/jobs/0/resource_id",
            serde_json::json!("foreign-resource"),
        ),
        (
            "/0/jobs/0/lease_source",
            serde_json::json!("foreign-source"),
        ),
        (
            "/0/jobs/0/expires_at",
            serde_json::json!("2099-01-01T00:00:00Z"),
        ),
        ("/0/jobs/0/lease_generation", serde_json::json!(99)),
        (
            "/0/jobs/0/route/scope",
            serde_json::json!("foreign-account"),
        ),
    ];
    for (pointer, replacement) in invalid_fields {
        let mut invalid = stored.clone();
        *invalid
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("missing executable field {pointer}")) = replacement;
        reject_stored_result(
            &fixture,
            &key,
            &format!("S7R1\n{}", serde_json::to_string(&invalid).unwrap()),
        )
        .await;
    }
    let mut unknown = stored.clone();
    unknown[0]
        .as_object_mut()
        .unwrap()
        .insert("unknown".into(), true.into());
    reject_stored_result(&fixture, &key, &format!("S7R1\n{unknown}")).await;
    // Birth history survives legitimate later lease/target/resource changes.
    let tx = fixture.db.begin().await.unwrap();
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    crate::store::pinning::leases::cancel_lease(
        &tx,
        applied.items[1].receipt.intent_ids.first().unwrap(),
        now,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        records::lookup_trusted_committed(&fixture.db, &fixture.auth, &fixture.binding, item)
            .await
            .unwrap()
            .is_some()
    );
    assert!(records::frame_actions(&native::capture(&fixture.db).await.unwrap()).is_ok());
    assert!(
        super::super::status::lookup(&fixture.db, &fixture.auth, &fixture.binding, item)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .unwrap()
        .replayed
    );
}

async fn reject_stored_result(
    fixture: &super::super::tests::Fixture,
    key: &native::NativeKey,
    result: &str,
) {
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action'",[result.into(),key.values[0].clone().into(),key.values[1].clone().into(),key.values[2].clone().into()])).await.unwrap();
    assert!(
        records::lookup_trusted_committed(&tx, &fixture.auth, &fixture.binding, &key.values[2])
            .await
            .is_err()
    );
    assert!(
        super::super::super::admission::ManagementJobScope::from_applied(
            &tx,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn executed_writer_rejects_native_birth_drift() {
    let fixture = fixture("proof-native-birth", false, false, false).await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    let executed = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    let action = executed.action_view();
    let job = &action.jobs[0];
    for (table, column, id_column, bad, good) in [
        (
            "pin_jobs",
            "operation",
            "id",
            "unpin".to_owned(),
            "reconcile".to_owned(),
        ),
        (
            "pin_invocation_routes",
            "remote_epoch",
            "job_id",
            (job.remote_epoch - 1).to_string(),
            job.remote_epoch.to_string(),
        ),
    ] {
        let sql = format!("UPDATE {table} SET {column}=$1 WHERE {id_column}=$2");
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            &sql,
            [bad.into(), job.job_id.clone().into()],
        ))
        .await
        .unwrap();
        assert!(
            records::write_executed_action_in_transaction(
                &fixture.auth,
                &fixture.binding,
                &executed,
                before
            )
            .await
            .is_err()
        );
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            &sql,
            [good.into(), job.job_id.clone().into()],
        ))
        .await
        .unwrap();
    }
    let lease_id = job.lease_id.clone().unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_leases SET source=$1 WHERE id=$2",
        ["foreign-native-source".into(), lease_id.clone().into()],
    ))
    .await
    .unwrap();
    assert!(
        records::write_executed_action_in_transaction(
            &fixture.auth,
            &fixture.binding,
            &executed,
            before
        )
        .await
        .is_err()
    );
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_leases SET source=$1 WHERE id=$2",
        [job.lease_source.clone().unwrap().into(), lease_id.into()],
    ))
    .await
    .unwrap();
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &executed,
        before,
    )
    .await
    .unwrap();
    assert!(
        records::lookup_trusted_committed(&tx, &fixture.auth, &fixture.binding, "b-renew")
            .await
            .unwrap()
            .unwrap()
            .action_view()
            == action
    );
    let action = action.clone();
    tx.commit().await.unwrap();
    let applied = ApplyResult {
        replayed: true,
        items: vec![action],
    };
    assert_restored(&fixture, &fixture.binding, &applied).await;
}

#[tokio::test]
async fn quota_blocked_prefix_preserves_exact_renewal_facts() {
    let fixture = fixture("proof-v1-mixed", true, true, false).await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    let creation = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &creation,
        before,
    )
    .await
    .unwrap();
    let renewal = batch
        .execute_entry(&fixture.binding.confirmation.bindings[1])
        .await
        .unwrap();
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &renewal,
        before,
    )
    .await
    .unwrap();
    let applied = ApplyResult {
        replayed: true,
        items: vec![
            creation.action_view().clone(),
            renewal.action_view().clone(),
        ],
    };
    tx.commit().await.unwrap();
    assert_restored(&fixture, &fixture.binding, &applied).await;
}

#[tokio::test]
async fn kept_without_jobs_restores_exact_receipt() {
    let fixture = fixture("proof-v1-kept", false, false, true).await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    let kept = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    assert!(kept.action_view().jobs.is_empty());
    records::write_executed_action_in_transaction(&fixture.auth, &fixture.binding, &kept, before)
        .await
        .unwrap();
    let action = kept.action_view().clone();
    tx.commit().await.unwrap();
    assert_restored(
        &fixture,
        &fixture.binding,
        &ApplyResult {
            replayed: true,
            items: vec![action],
        },
    )
    .await;
}

#[tokio::test]
async fn executed_create_retains_real_worker_submit_and_completion_with_source_context() {
    let fixture = super::super::tests::fixture("proof-worker-create").await;
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let seed = &applied.items[0].jobs[0];
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        super::super::job_authority::check_before_call(&fixture.db, &job)
            .await
            .unwrap(),
        Some(super::super::job_authority::JobPermit::Submit(_))
    ));
    assert!(
        !super::super::job_authority::fresh_source_allowed(&fixture.db, &job, None, None)
            .await
            .unwrap()
    );
    let coordinator = crate::pinning::coordinator::PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(crate::kubo::KuboClient::new(
            fixture.config.source_endpoint.clone(),
        )),
    )
    .unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let scope = crate::pinning::worker::ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    Box::pin(coordinator.management_tick(
        &crate::store::Store::new(fixture.db.clone()),
        &scope,
        &cancel,
    ))
    .await
    .unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    assert!(
        crate::store::entities::pin_lease_requirement::contract::completion(
            &fixture.db,
            &applied.items[0].receipt.intent_ids[0],
            now
        )
        .await
        .unwrap()
        .is_some_and(|(_, all, _)| all),
        "the real source-bound worker must consume an executed receipt"
    );
    assert_eq!(
        super::super::status::lookup(&fixture.db, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::RemoteComplete
    );
}
