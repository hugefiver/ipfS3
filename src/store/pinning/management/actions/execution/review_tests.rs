use super::*;
use crate::pinning::management::*;
use sea_orm::{ConnectionTrait, EntityTrait, Statement, TransactionTrait};
use tokio_util::sync::CancellationToken;

pub(super) async fn reconfirm(fixture: &mut super::super::tests::Fixture, request: &PlanRequest) {
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(request, &snapshot).unwrap();
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
        (request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_reapply_grants_new_source_permission_without_rewriting_skipped_decision() {
    use crate::{
        pinning::{
            decision::{DecisionEffect, DecisionOrigin, ExtensionDecision, WarningCode},
            policy::PublicationPolicy,
            tags::{ObjectTag, PinControl},
        },
        store::pinning::{decision, tags},
    };
    let mut fixture = super::super::tests::fixture("reapply-skipped-base").await;
    let Subject::Object { identity, .. } = &fixture.binding.request.items[0].subject else {
        unreachable!()
    };
    let identity = identity.clone();
    let tag_set = vec![ObjectTag::new("ipfs-s3:pin", "true")];
    let policy = PublicationPolicy {
        tags: tag_set.clone(),
        leases: vec![],
    };
    let skipped = ExtensionDecision::capture(
        DecisionOrigin::new("fixture", "skipped-request"),
        "1".repeat(64),
        DecisionEffect::Skipped,
        Some(WarningCode::NoAvailableProvider),
        PinControl::from_tags(&tag_set).unwrap(),
        &policy,
    )
    .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    tags::replace_object_tags(&tx, &identity.object_id, &tag_set)
        .await
        .unwrap();
    decision::write_for_object_in_transaction(&tx, &identity.object_id, &skipped)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "reapply-skipped".into();
    let Action::Create(intent) = request.items[0].action.clone() else {
        unreachable!()
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    assert!(
        matches!(
            dry_run(&request, &snapshot).unwrap().items()[0].status,
            PlanStatus::Blocked(_)
        ),
        "Create is not a skipped-decision override"
    );
    request.items[0].action = Action::Reapply(intent);
    reconfirm(&mut fixture, &request).await;
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
    let mut missing = executed.action_view().clone();
    missing.decision = None;
    assert!(
        super::super::job_authority::validate_executed_source(
            &tx,
            &fixture.binding,
            &missing,
            before
        )
        .await
        .is_err()
    );
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &executed,
        before,
    )
    .await
    .unwrap();
    let seed = executed
        .action_view()
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap()
        .clone();
    tx.commit().await.unwrap();
    let job = crate::store::entities::pin_job::Entity::find_by_id(seed.job_id.clone())
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
    let Capture::Object(capture) = request_capture(&fixture.binding) else {
        unreachable!()
    };
    let idle = capture
        .object
        .read_evidence
        .as_ref()
        .unwrap()
        .idle_timeout_millis;
    assert!(
        super::super::job_authority::fresh_source_allowed(
            &fixture.db,
            &job,
            Some(&fixture.config),
            Some(idle)
        )
        .await
        .unwrap()
    );
    assert_eq!(
        decision::read_for_version(&fixture.db, &identity.private_version_id)
            .await
            .unwrap(),
        Some(skipped)
    );
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "UPDATE pin_leases SET generation=generation+1 WHERE id=$1",
        [seed.lease_id.unwrap().into()],
    ))
    .await
    .unwrap();
    assert!(matches!(
        super::super::job_authority::check_before_call(&tx, &job)
            .await
            .unwrap(),
        Some(super::super::job_authority::JobPermit::LedgerOnly(_))
    ));
    tx.rollback().await.unwrap();
}

fn request_capture(binding: &records::ConfirmedBinding) -> &Capture {
    binding.confirmation.bindings[0].capture.as_ref().unwrap()
}

#[tokio::test]
async fn native_rejected_retry_prefix_preserves_resource_and_independent_renewal() {
    use crate::store::entities::pin_lease_target;
    let mut fixture = super::proof_tests::observation_prefix_fixture().await;
    let Some(Capture::Object(capture)) = &fixture.binding.confirmation.bindings[1].capture else {
        unreachable!()
    };
    let target = pin_lease_target::Entity::find_by_id(capture.target.as_ref().unwrap().id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "retry-prefix-renew".into();
    let Subject::Object {
        target: manual_selection,
        ..
    } = &request.items[0].subject
    else {
        unreachable!()
    };
    let manual_selection = manual_selection.clone();
    request.items[0].action = Action::Retry;
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
    let Subject::Object {
        target: selection, ..
    } = &mut request.items[1].subject
    else {
        unreachable!()
    };
    *selection = manual_selection;
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='done',locked_until=NULL; UPDATE pin_jobs SET state='running',submit_phase='recovering',last_error='rejected' WHERE operation='submit'; UPDATE remote_pin_ledger SET effect='not_created'").await.unwrap();
    let original = crate::store::entities::pin_job::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap()
        .into_iter()
        .find(|job| job.operation == "submit")
        .unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_invocation_routes SET remote_epoch=(SELECT epoch FROM remote_pins WHERE provider=$1 AND cid=$2) WHERE job_id=$3",[original.provider.clone().into(),original.cid.clone().into(),original.id.clone().into()])).await.unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[original.id.clone().into(),now.into()])).await.unwrap();
    reconfirm(&mut fixture, &request).await;
    let Some(PlannedStep::Object(step)) = &fixture.binding.confirmation.bindings[0].step else {
        unreachable!()
    };
    assert_eq!(step.source, RemoteDirective::SubmitWithFence);
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
    let retry = batch
        .execute_entry(&fixture.binding.confirmation.bindings[0])
        .await
        .unwrap();
    for read in retry
        .operation
        .facts
        .1
        .iter()
        .filter(|read| read.0.0 == "remote_pins")
    {
        assert!(
            retry.operation.before(&read.0).unwrap() == retry.operation.after(&read.0).unwrap()
        );
    }
    assert!(
        retry
            .action_view()
            .jobs
            .iter()
            .any(|job| job.directive == records::JobDirective::Submit && job.job_id == original.id)
    );
    records::write_executed_action_in_transaction(&fixture.auth, &fixture.binding, &retry, before)
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
    let mut invalid = renewal.operation.facts.as_ref().clone();
    let table = before.table("remote_pins").unwrap();
    let column = table.columns.iter().position(|c| c == "cid_size").unwrap();
    for output in invalid.2.iter_mut().filter(|o| o.0.0 == "remote_pins") {
        output.2.as_mut().unwrap()[column] = Some("999".into());
    }
    let mut cursor = Cursor::default();
    cursor
        .publish(&retry.operation, &retry.identity, (&fixture.binding).into())
        .unwrap();
    let invalid =
        OperationRead::resolve((&fixture.binding).into(), Arc::new(invalid), &cursor).unwrap();
    assert!(
        validate::closure(
            (&fixture.binding).into(),
            renewal.action_view(),
            &invalid,
            &cursor
        )
        .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn real_cancel_prefix_preserves_independent_renewal_and_rejects_extra_resource_changes() {
    let mut fixture = super::proof_tests::observation_prefix_fixture().await;
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "cancel-prefix-renew".into();
    request.items[0].action = Action::Cancel;
    reconfirm(&mut fixture, &request).await;
    let original_jobs = crate::store::entities::pin_job::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let before = fixture.binding.view().frozen_rows().unwrap();
    let mut batch = ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
        before,
    )
    .unwrap();
    for entry in &fixture.binding.confirmation.bindings {
        let executed = batch
            .execute_entry(entry)
            .await
            .unwrap_or_else(|error| panic!("{} execution: {error:?}", entry.request.item_id));
        records::write_executed_action_in_transaction(
            &fixture.auth,
            &fixture.binding,
            &executed,
            before,
        )
        .await
        .unwrap();
    }
    tx.rollback().await.unwrap();
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    for job in &original_jobs {
        assert_eq!(
            crate::store::entities::pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .as_ref(),
            Some(job)
        );
    }
    super::proof_tests::assert_restored(&fixture, &fixture.binding, &applied).await;
    let before = fixture.binding.view().frozen_rows().unwrap();
    let restored =
        records::lookup_trusted_committed(&fixture.db, &fixture.auth, &fixture.binding, "b-renew")
            .await
            .unwrap()
            .unwrap();
    let row = super::super::provider_points::original_record(&fixture.db, restored.record_key())
        .await
        .unwrap()
        .unwrap();
    let raw = native::value(&row, "result").unwrap();
    let (_, facts, _): (records::CommittedAction, ItemFacts, String) =
        serde_json::from_str(raw.strip_prefix("S7R1\n").unwrap()).unwrap();
    assert!(
        facts
            .1
            .iter()
            .any(|read| matches!(read.1, facts::Input::Output(..)))
    );
    assert!(
        before
            .row("pin_leases", &[&applied.items[0].receipt.intent_ids[0]])
            .is_some()
    );
    let mut invalid = facts;
    let column = before
        .table("remote_pins")
        .unwrap()
        .columns
        .iter()
        .position(|c| c == "failure_attempts")
        .unwrap();
    for output in invalid.2.iter_mut().filter(|o| o.0.0 == "remote_pins") {
        output.2.as_mut().unwrap()[column] = Some("99".into());
    }
    let hash = facts::identity((&fixture.binding).into(), &applied.items[1], &invalid).unwrap();
    let result = format!(
        "S7R1\n{}",
        serde_json::to_string(&(&applied.items[1], invalid, hash)).unwrap()
    );
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE plan_id=$2 AND item_id=$3 AND kind='action'",[result.into(),fixture.binding.request.plan_id.clone().into(),"b-renew".into()])).await.unwrap();
    assert!(
        records::lookup_trusted_committed(&tx, &fixture.auth, &fixture.binding, "b-renew")
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn shared_submit_retains_actual_source_distinct_from_owner_capture() {
    use crate::pinning::policy::PublicationPolicy;
    use crate::store::{
        entities::{object_version, pin_job},
        pinning::{
            jobs,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    };
    use sea_orm::{ColumnTrait, QueryFilter};
    let mut fixture = super::super::tests::fixture_with_automatic("v1-shared-base", true).await;
    let original = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[original.id.clone().into(),now.into()])).await.unwrap();
    let parked = pin_job::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        jobs::management_retryable_park(
            &fixture.db,
            &parked,
            &fixture.config.pinning.providers[0].route_snapshot()
        )
        .await
        .unwrap()
    );
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "v1-shared-new-owner".into();
    let Subject::Object { identity, .. } = &mut request.items[0].subject else {
        unreachable!()
    };
    let id = uuid::Uuid::new_v4().to_string();
    publication::publish_object(
        &fixture.db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.clone(),
                "bucket",
                "shared-owner",
                original.cid.clone(),
                10,
                None,
                None,
                false,
                None,
                None,
                now,
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
            object_target: PinTargetSpec {
                cid: original.cid.clone(),
                logical_size: 10,
            },
        },
        &fixture.config.pinning.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    identity.object_id = id;
    identity.private_version_id = version.id;
    identity.key = "shared-owner".into();
    identity.public_version = version
        .version_id
        .map_or(PublicVersionId::Null, PublicVersionId::Version);
    evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &crate::kubo::KuboClient::new(fixture.config.source_endpoint.clone()),
        &evidence::PrepareRequest {
            record_id: "shared-owner-read".into(),
            identity: identity.clone(),
            expected_node: fixture.config.source_node().unwrap().into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    reconfirm(&mut fixture, &request).await;
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
    let shared = action
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit && job.job_id == original.id)
        .unwrap();
    let Capture::Object(capture) = request_capture(&fixture.binding) else {
        unreachable!()
    };
    assert!(shared.source.as_ref() != Some(&capture.object.identity));
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &executed,
        before,
    )
    .await
    .unwrap();
    assert!(
        records::lookup_trusted_committed(&tx, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .unwrap()
            .action_view()
            == action
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn executed_submit_rejects_birth_after_read_attestation_expiry() {
    let mut fixture = super::super::tests::fixture("v1-expired-read-base").await;
    let Subject::Object { identity, .. } = &fixture.binding.request.items[0].subject else {
        unreachable!()
    };
    evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &crate::kubo::KuboClient::new(fixture.config.source_endpoint.clone()),
        &evidence::PrepareRequest {
            record_id: "short-read".into(),
            identity: identity.clone(),
            expected_node: fixture.config.source_node().unwrap().into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::seconds(10),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "v1-expired-read".into();
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
    assert!(
        action
            .jobs
            .iter()
            .any(|job| job.directive == records::JobDirective::Submit)
    );
    let Some(Capture::Object(capture)) = &action.receipt.binding.capture else {
        unreachable!()
    };
    let expires = capture.object.read_evidence.as_ref().unwrap().expires_at;
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    tokio::time::sleep(
        (expires - now).to_std().unwrap_or_default() + std::time::Duration::from_millis(20),
    )
    .await;
    fixture.binding.check(&tx, &fixture.auth).await.unwrap();
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
    assert!(
        records::lookup_committed(&tx, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .is_none()
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn readonly_prefix_rejects_consistent_epoch_chain_forgery_from_real_row() {
    let fixture = super::proof_tests::observation_prefix_fixture().await;
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let action = &applied.items[1];
    let key = native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.binding.database_scope.clone(),
            fixture.binding.request.plan_id.clone(),
            action.receipt.binding.request.item_id.clone(),
            "action".into(),
        ],
    };
    let row = super::super::provider_points::original_record(&fixture.db, &key)
        .await
        .unwrap()
        .unwrap();
    let (mut action, mut facts, _): (records::CommittedAction, ItemFacts, String) =
        serde_json::from_str(
            native::value(&row, "result")
                .unwrap()
                .strip_prefix("S7R1\n")
                .unwrap(),
        )
        .unwrap();
    for output in &mut facts.2 {
        let column = match output.0.0.as_str() {
            "remote_pins" => "epoch",
            "pin_jobs" => "expected_remote_epoch",
            "pin_invocation_routes" => "remote_epoch",
            _ => continue,
        };
        let position = fixture
            .binding
            .view()
            .frozen_rows()
            .unwrap()
            .table(&output.0.0)
            .unwrap()
            .columns
            .iter()
            .position(|c| c == column)
            .unwrap();
        output.2.as_mut().unwrap()[position] = Some("100".into());
    }
    action.jobs[0].remote_epoch = 100;
    let hash = facts::identity((&fixture.binding).into(), &action, &facts).unwrap();
    let result = format!(
        "S7R1\n{}",
        serde_json::to_string(&(action, facts, hash)).unwrap()
    );
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE database_scope=$2 AND plan_id=$3 AND item_id=$4 AND kind='action'",[result.into(),key.values[0].clone().into(),key.values[1].clone().into(),key.values[2].clone().into()])).await.unwrap();
    assert!(
        records::lookup_trusted_committed(&tx, &fixture.auth, &fixture.binding, &key.values[2])
            .await
            .is_err(),
        "a readonly entry cannot supply a self-consistent arbitrary epoch advance"
    );
    tx.rollback().await.unwrap();
}
