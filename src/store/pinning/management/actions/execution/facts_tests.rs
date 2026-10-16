use super::*;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn create_rejects_trigger_corruption_of_native_job_initial_state() {
    let fixture = super::super::tests::fixture("facts-job-birth-state").await;
    for change in [
        "created_at='2100-01-01 00:00:00',updated_at='2100-01-01 00:00:00',next_attempt_at='2100-01-01 00:00:00'",
        "created_at='2000-01-01 00:00:00',updated_at='2000-01-01 00:00:00',next_attempt_at='2000-01-01 00:00:00'",
        "state='done'",
        "submit_phase='calling'",
        "locked_until=created_at",
        "attempts=1",
        "last_error='unreviewed'",
        "next_attempt_at='2100-01-01 00:00:00'",
        "created_at='2000-01-01 00:00:00'",
        "operation='reconcile',lease_id=NULL,target_id=NULL,expected_generation=NULL,expected_remote_epoch=1,submit_phase=NULL",
    ] {
        fixture.db.execute_unprepared(&format!("CREATE TRIGGER invalid_job_birth AFTER INSERT ON pin_jobs BEGIN UPDATE pin_jobs SET {change} WHERE id=NEW.id; END")).await.unwrap();
        let before = native::capture(&fixture.db).await.unwrap();
        assert!(
            super::super::apply(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &fixture.binding
            )
            .await
            .is_err(),
            "invalid native job initialization: {change}"
        );
        assert!(before == native::capture(&fixture.db).await.unwrap());
        fixture
            .db
            .execute_unprepared("DROP TRIGGER invalid_job_birth")
            .await
            .unwrap();
    }
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert!(!applied.items[0].jobs.is_empty());
    let replay = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
}

#[tokio::test]
async fn native_reconcile_and_cleanup_insert_use_the_remaining_outer_work() {
    use crate::{
        error::AppError,
        store::{
            entities::{pin_job, remote_pin},
            pinning::jobs,
        },
    };
    let fixture = super::super::tests::fixture_with_automatic("facts-native-work", true).await;
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    for reconcile in [true, false] {
        let now = crate::store::database_clock::database_now(&fixture.db)
            .await
            .unwrap();
        let limited = records::SharedDecodeBudget::default();
        limited
            .lock()
            .unwrap()
            .consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 8)
            .unwrap();
        let tx = fixture.db.begin().await.unwrap();
        let result = if reconcile {
            let jobs::NewPinJob::Remote(job) =
                jobs::reconcile_job(&remote.provider, &remote.cid, remote.epoch, now)
            else {
                unreachable!()
            };
            jobs::ensure_reconcile_with_work(&tx, job, now, records::DecodeWork::shared(&limited))
                .await
                .map(|_| ())
        } else {
            jobs::insert_new_job_with_work(
                &tx,
                jobs::unpin_job(&remote.provider, &remote.cid, remote.epoch, now),
                now,
                records::DecodeWork::shared(&limited),
            )
            .await
            .map(|_| ())
        };
        assert!(matches!(result, Err(AppError::ManagementBudget)));
        tx.rollback().await.unwrap();
        assert!(before == native::capture(&fixture.db).await.unwrap());
        let work = records::SharedDecodeBudget::default();
        let tx = fixture.db.begin().await.unwrap();
        let id = if reconcile {
            let jobs::NewPinJob::Remote(job) =
                jobs::reconcile_job(&remote.provider, &remote.cid, remote.epoch, now)
            else {
                unreachable!()
            };
            let id = job.id.clone();
            jobs::ensure_reconcile_with_work(&tx, job, now, records::DecodeWork::shared(&work))
                .await
                .unwrap();
            id
        } else {
            let job = jobs::unpin_job(&remote.provider, &remote.cid, remote.epoch, now);
            let jobs::NewPinJob::Remote(details) = &job else {
                unreachable!()
            };
            let id = details.id.clone();
            assert!(
                jobs::insert_new_job_with_work(&tx, job, now, records::DecodeWork::shared(&work))
                    .await
                    .unwrap()
            );
            id
        };
        let job = pin_job::Entity::find_by_id(id)
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(job.state, "pending");
        assert!(work.lock().unwrap().usage().1 > 8);
        tx.rollback().await.unwrap();
        assert!(before == native::capture(&fixture.db).await.unwrap());
    }
}

#[tokio::test]
async fn create_rejects_trigger_writes_outside_its_semantic_effects() {
    let mut fixture =
        super::super::tests::fixture_with_automatic("facts-trigger-scope", true).await;
    fixture.db.execute_unprepared("INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,state,generation,created_at,expires_at,last_touched_at) SELECT 'unrelated',owner_object_id,'unrelated',policy_id,provider_mode,content_mode,state,generation,created_at,expires_at,last_touched_at FROM pin_leases LIMIT 1").await.unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "facts-trigger-reviewed".into();
    super::review_tests::reconfirm(&mut fixture, &request).await;
    for predicate in ["id='unrelated'", "source!='unrelated' AND id!=NEW.id"] {
        fixture.db.execute_unprepared(&format!("CREATE TRIGGER unrelated_lease_effect AFTER INSERT ON pin_leases WHEN NEW.id LIKE 'management:%' BEGIN UPDATE pin_leases SET policy_id='unreviewed' WHERE {predicate}; END")).await.unwrap();
        let before = native::capture(&fixture.db).await.unwrap();
        let rejected = super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding,
        )
        .await;
        assert!(
            rejected.is_err(),
            "a read dependency is not a writable lease effect"
        );
        assert!(before == native::capture(&fixture.db).await.unwrap());
        fixture
            .db
            .execute_unprepared("DROP TRIGGER unrelated_lease_effect")
            .await
            .unwrap();
    }
    assert!(
        super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .is_ok()
    );
}

async fn short_file_fixture(label: &str) -> super::super::tests::Fixture {
    use crate::pinning::management::{Subject, evidence};
    let mut fixture = super::super::tests::fixture_with_automatic(label, true).await;
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='done'; UPDATE remote_pins SET status='pinned',request_id='shared'; UPDATE remote_pin_ledger SET effect='confirmed',remote_pinned_at=CURRENT_TIMESTAMP").await.unwrap();
    let Subject::Object { identity, .. } = &fixture.binding.request.items[0].subject else {
        unreachable!()
    };
    evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &crate::kubo::KuboClient::new(fixture.config.source_endpoint.clone()),
        &evidence::PrepareRequest {
            record_id: "short-file-proof".into(),
            identity: identity.clone(),
            expected_node: fixture.config.source_node().unwrap().into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::seconds(30),
            directory: false,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = format!("{label}-reviewed");
    super::review_tests::reconfirm(&mut fixture, &request).await;
    fixture
}

#[tokio::test]
async fn jobless_file_allocation_rechecks_attestation_at_final_publication() {
    let positive = short_file_fixture("facts-file-fresh").await;
    let applied = super::super::apply(
        &positive.db,
        &positive.config,
        &positive.auth,
        &positive.binding,
    )
    .await
    .unwrap();
    assert!(applied.items[0].jobs.is_empty());
    let fixture = short_file_fixture("facts-file-expiry").await;
    let before = native::capture(&fixture.db).await.unwrap();
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::WaitForProofExpiry,
    );
    let result = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await;
    assert!(
        matches!(result, Err(ManagementError::Expired)),
        "a jobless file allocation cannot outlive its attestation: {:?}",
        result.as_ref().err()
    );
    assert!(before == native::capture(&fixture.db).await.unwrap());
    let Some(Capture::Object(capture)) = &positive.binding.confirmation.bindings[0].capture else {
        unreachable!()
    };
    assert!(
        capture.object.read_evidence.as_ref().unwrap().expires_at
            <= crate::store::database_clock::database_now(&positive.db)
                .await
                .unwrap()
    );
    let replay = super::super::apply(
        &positive.db,
        &positive.config,
        &positive.auth,
        &positive.binding,
    )
    .await
    .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
}

#[tokio::test]
async fn receipt_publication_cannot_reset_the_outer_apply_budget() {
    let fixture = super::super::tests::fixture("facts-apply-budget").await;
    let before = native::capture(&fixture.db).await.unwrap();
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::ExhaustSession,
    );
    let result = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await;
    assert!(matches!(result, Err(ManagementError::Budget)));
    assert!(before == native::capture(&fixture.db).await.unwrap());
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert_eq!(applied.items.len(), 1);
    let replay = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
}

#[tokio::test]
async fn shared_allocation_without_jobs_checks_contract_target_and_bytes() {
    let mut fixture =
        super::super::tests::fixture_with_automatic("facts-shared-no-job", true).await;
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='done'; UPDATE remote_pins SET status='pinned',request_id='shared'; UPDATE remote_pin_ledger SET effect='confirmed',remote_pinned_at=CURRENT_TIMESTAMP").await.unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "facts-shared-no-job-review".into();
    super::review_tests::reconfirm(&mut fixture, &request).await;
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
    assert!(executed.action_view().jobs.is_empty());
    assert!(executed.operation.facts.3.is_empty());
    for (table, column, replacement) in [
        ("pin_lease_requirements", "contract", "{}"),
        ("pin_lease_targets", "logical_size", "999"),
        ("pin_leases", "content_mode", "all"),
    ] {
        let mut facts = executed.operation.facts.as_ref().clone();
        let layout = before.table(table).unwrap();
        let column = layout
            .columns
            .iter()
            .position(|name| name == column)
            .unwrap();
        let output = facts
            .2
            .iter_mut()
            .find(|output| output.0.0 == table)
            .unwrap();
        output.2.as_mut().unwrap()[column] = Some(replacement.into());
        let op = OperationRead::resolve(
            (&fixture.binding).into(),
            Arc::new(facts),
            &Cursor::default(),
        )
        .unwrap();
        assert!(
            validate::closure(
                (&fixture.binding).into(),
                executed.action_view(),
                &op,
                &Cursor::default()
            )
            .is_err(),
            "invalid {table} accepted without a job"
        );
    }
    records::write_executed_action_in_transaction(
        &fixture.auth,
        &fixture.binding,
        &executed,
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
    assert!(applied.items[0].jobs.is_empty());
    let restored =
        records::lookup_trusted_committed(&fixture.db, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .unwrap();
    assert!(restored.action_view() == &applied.items[0]);
}

#[tokio::test]
async fn allocated_result_exposes_closed_item_facts() {
    let fixture = super::super::tests::fixture("facts-allocate").await;
    let applied = super::super::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let trusted =
        records::lookup_trusted_committed(&fixture.db, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .unwrap();
    let row = super::super::provider_points::original_record(&fixture.db, trusted.record_key())
        .await
        .unwrap()
        .unwrap();
    let raw = native::value(&row, "result").unwrap();
    let (action, facts, identity): (records::CommittedAction, serde_json::Value, String) =
        serde_json::from_str(raw.strip_prefix("S7R1\n").expect("formal result facts")).unwrap();
    assert!(action == applied.items[0]);
    assert_eq!(facts[0], 0);
    assert_eq!(facts[5][0], "allocate");
    assert_eq!(facts[4].as_array().unwrap().len(), action.jobs.len());
    assert_eq!(identity.len(), 64);
}
