//! Opt-in formal apply on native PostgreSQL, in one fresh parent-owned scope.
//! Run this target with --ignored; the parent supplies a unique empty stage7_
//! schema through IPFS_S3_STAGE7_APPLY_DATABASE_URL (search_path only that schema).
//! This suite never creates/drops a schema, starts a DB, or substitutes SQLite.
#[path = "support/stage7_management_apply_postgres.rs"]
mod support;

use futures_util::FutureExt;
use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::management::*,
    store::{
        entities::{object, pin_job, pin_lease, pin_lease_requirement, pin_lease_target},
        pinning::management::{ManagementError, actions, native, records},
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, Statement, TransactionTrait};
use std::{panic::AssertUnwindSafe, time::Duration};
use support::{Fixture, counts};
use tokio_util::sync::CancellationToken;

// One entry point prevents parallel cases from sharing or resetting the same
// parent-selected schema. Every case goes through formal plan/confirm/apply.
#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn native_postgres_formal_apply_transaction_protocol() {
    let mut fixture = Fixture::new().await;
    eprintln!(
        "native PG authorization: scope={} identity={}",
        fixture.auth.database_scope(),
        fixture.auth.database_identity()
    );
    let result = AssertUnwindSafe(async {
        eprintln!("phase: create and cross-connection replay");
        create_and_cross_connection_replay(&fixture).await;
        report_phase(&fixture, "create/replay").await;
        eprintln!("phase: atomic receipt publication fault");
        atomic_receipt_publication_fault(&mut fixture).await;
        report_phase(&fixture, "receipt rollback/retry/replay").await;
        eprintln!("phase: native microsecond proof and fence drift");
        native_microsecond_proof_and_fence_drift(&fixture).await;
        report_phase(&fixture, "microsecond drift").await;
        eprintln!("phase: lifecycle rows and callback tail NOWAIT");
        lifecycle_rows_and_callback_tail_are_nowait(&fixture).await;
        report_phase(&fixture, "NOWAIT lock protocol").await;
        eprintln!("phase: absent first insert and committed membership drift");
        absent_first_insert_and_committed_membership_drift(&fixture).await;
        report_phase(&fixture, "first-insert/membership drift").await;
        eprintln!("phase: renew after create and cross-connection replay");
        renew_after_create_and_cross_connection_replay(&fixture).await;
        report_phase(&fixture, "renew/replay").await;
    })
    .catch_unwind()
    .await;
    fixture.close().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn report_phase(fixture: &Fixture, phase: &str) {
    eprintln!(
        "verified {phase}: DB={:?} requests={:?}",
        counts(&fixture.db).await,
        fixture.request_counts().await
    );
}

async fn create_and_cross_connection_replay(fixture: &Fixture) {
    let identity = fixture.prepared_object("create-replay").await;
    let request = fixture.create_request(identity.clone());
    let binding = fixture.confirm(&request).await;
    let before = counts(&fixture.db).await;
    let calls = fixture.request_counts().await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(applied.items.len(), 1);
    let item = &applied.items[0];
    assert_eq!(item.receipt.intent_ids.len(), 1);
    assert_eq!(item.targets.len(), 1);
    assert!(!item.receipt.job_ids.is_empty());
    let lease = pin_lease::Entity::find_by_id(item.receipt.intent_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let Action::Create(intent) = &request.items[0].action else {
        unreachable!()
    };
    assert_eq!(lease.owner_object_id, identity.object_id);
    assert_eq!(lease.created_at.timestamp(), intent.started_at);
    assert_eq!(lease.expires_at.timestamp(), intent.expires_at);
    let target = pin_lease_target::Entity::find_by_id(item.targets[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.lease_id, lease.id);
    assert_eq!(target.logical_size, 10);
    let after = counts(&fixture.db).await;
    assert_eq!(after.leases, before.leases + 1);
    assert_eq!(after.targets, before.targets + 1);
    assert_eq!(after.requirements, before.requirements + 1);
    assert_eq!(after.jobs, before.jobs + item.receipt.job_ids.len() as i64);
    assert_eq!(after.applied, before.applied + 1);
    assert_eq!(
        (after.remotes, after.reserved_bytes, after.reserved_pins),
        (1, 10, 1)
    );
    let persisted = records::lookup_committed(&fixture.writer, &fixture.auth, &binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert!(persisted == *item);

    // Native uniqueness rejects duplicate action identity, without relying on
    // a constraint name, SQL spelling or error/log message.
    let duplicate = fixture
        .writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO pin_management_records SELECT * FROM pin_management_records
            WHERE plan_id=$1 AND item_id='object' AND kind='action'",
            [request.plan_id.clone().into()],
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate.sql_err(),
        Some(sea_orm::SqlErr::UniqueConstraintViolation(_))
    ));
    let frame = native::capture(&fixture.db).await.unwrap();
    let auth = fixture.reauthorize_writer().await;
    let replay = actions::apply(&fixture.writer, &fixture.config, &auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed && replay.items == applied.items);
    assert_eq!(counts(&fixture.db).await, after);
    assert!(native::capture(&fixture.db).await.unwrap() == frame);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn atomic_receipt_publication_fault(fixture: &mut Fixture) {
    let first = fixture.prepared_object("receipt-batch-first").await;
    let second = fixture.prepared_object("receipt-batch-second").await;
    let mut request = fixture.create_request(first);
    let mut second_item = fixture.create_request(second).items.remove(0);
    second_item.item_id = "second".into();
    request.items.push(second_item);
    let binding = fixture.confirm(&request).await;
    let before = counts(&fixture.db).await;
    let frame = native::capture(&fixture.db).await.unwrap();
    let calls = fixture.request_counts().await;
    fixture.install_receipt_fault(&request.plan_id).await;
    let failed = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await;
    let hit: bool = fixture
        .writer
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT is_called AS hit FROM stage7_management_apply_postgres_receipt_hit",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "hit")
        .unwrap();
    fixture.remove_receipt_fault().await;
    assert!(
        hit,
        "must reach COMMIT with both receipts, leases and jobs visible, not reject schema early"
    );
    assert!(failed.is_err());
    assert_eq!(counts(&fixture.db).await, before);
    assert!(
        native::capture(&fixture.db).await.unwrap() == frame,
        "no receipt or business prefix survives"
    );
    for item in &request.items {
        assert!(
            records::lookup_committed(&fixture.db, &fixture.auth, &binding, &item.item_id)
                .await
                .unwrap()
                .is_none()
        );
    }
    let retry = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!retry.replayed);
    assert_eq!(retry.items.len(), 2);
    let after = counts(&fixture.db).await;
    assert_eq!(after.leases, before.leases + 2);
    assert_eq!(after.targets, before.targets + 2);
    assert_eq!(after.requirements, before.requirements + 2);
    assert_eq!(after.applied, before.applied + 2);
    let job_ids: std::collections::BTreeSet<_> = retry
        .items
        .iter()
        .flat_map(|item| item.receipt.job_ids.iter())
        .collect();
    assert_eq!(after.jobs, before.jobs + job_ids.len() as i64);
    assert_eq!(
        (after.remotes, after.reserved_bytes, after.reserved_pins),
        (before.remotes, before.reserved_bytes, before.reserved_pins)
    );
    let replay = actions::apply(&fixture.writer, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed && replay.items == retry.items);
    assert_eq!(counts(&fixture.db).await, after);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn native_microsecond_proof_and_fence_drift(fixture: &Fixture) {
    let identity = fixture.publish("microsecond-drift").await;
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE objects SET created_at=TIMESTAMPTZ '2026-10-12 00:00:00.000001+00' WHERE id=$1",
            [identity.object_id.clone().into()],
        ))
        .await
        .unwrap();
    let proof_request = fixture.proof_request(&identity);
    fixture.prepare(&proof_request).await;
    let request = fixture.create_request(identity.clone());
    let binding = fixture.confirm(&request).await;
    let original = object::Entity::find_by_id(identity.object_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let calls = fixture.request_counts().await;
    let business = counts(&fixture.db).await;
    fixture
        .writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE objects SET created_at=created_at + INTERVAL '1 microsecond' WHERE id=$1",
            [identity.object_id.clone().into()],
        ))
        .await
        .unwrap();
    let changed = object::Entity::find_by_id(identity.object_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.created_at.timestamp(),
        changed.created_at.timestamp()
    );
    assert_eq!(
        changed.created_at - original.created_at,
        chrono::Duration::microseconds(1)
    );
    let drifted = native::capture(&fixture.db).await.unwrap();
    assert!(before.frontier == drifted.frontier && before != drifted);
    assert!(records::recheck_frame(&fixture.db, &before).await.is_err());
    assert!(
        evidence::prepare(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &KuboClient::new(fixture.server.uri()),
            &proof_request,
            CancellationToken::new()
        )
        .await
        .is_err(),
        "old proof cannot recapture a changed native object tuple"
    );
    assert!(matches!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await,
        Err(ManagementError::Changed)
    ));
    assert_eq!(counts(&fixture.db).await, business);
    assert!(native::capture(&fixture.db).await.unwrap() == drifted);
    assert_eq!(fixture.request_counts().await, calls);
    fixture
        .writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE objects SET created_at=$1 WHERE id=$2",
            [original.created_at.into(), identity.object_id.into()],
        ))
        .await
        .unwrap();
    // Exact native restoration retains the original proof and confirmed authority.
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    assert!(
        !actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(counts(&fixture.db).await.leases, business.leases + 1);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn lifecycle_rows_and_callback_tail_are_nowait(fixture: &Fixture) {
    let identity = fixture.prepared_object("nowait").await;
    let binding = fixture.confirm(&fixture.create_request(identity)).await;
    let frame = native::capture(&fixture.db).await.unwrap();
    let before = counts(&fixture.db).await;
    let calls = fixture.request_counts().await;
    let job = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();

    let writer = fixture.writer.begin().await.unwrap();
    writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE remote_pins SET status=status WHERE provider=$1 AND cid=$2",
            [job.provider.clone().into(), job.cid.clone().into()],
        ))
        .await
        .unwrap();
    assert_nowait_refusal(fixture, &binding).await;
    writer.rollback().await.unwrap();
    assert_eq!(counts(&fixture.db).await, before);
    assert!(native::capture(&fixture.db).await.unwrap() == frame);

    let callback = fixture.writer.begin().await.unwrap();
    callback
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE pin_jobs SET state=state WHERE id=$1",
            [job.id.into()],
        ))
        .await
        .unwrap();
    assert_nowait_refusal(fixture, &binding).await;
    // Reusing the apply connection also drains its rollback before checking that
    // the job-first callback can now take its remote lock without waiting.
    assert_eq!(counts(&fixture.db).await, before);
    callback
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT 1 AS held FROM remote_pins WHERE provider=$1 AND cid=$2 FOR UPDATE NOWAIT",
            [job.provider.into(), job.cid.into()],
        ))
        .await
        .unwrap()
        .unwrap();
    callback.rollback().await.unwrap();
    assert!(native::capture(&fixture.db).await.unwrap() == frame);
    assert!(
        !actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(counts(&fixture.db).await.leases, before.leases + 1);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn absent_first_insert_and_committed_membership_drift(fixture: &Fixture) {
    let identity = fixture.prepared_object("first-insert").await;
    let request = fixture.create_request(identity);
    let binding = fixture.confirm(&request).await;
    let frame = native::capture(&fixture.db).await.unwrap();
    assert!(frame.table("pin_resource_history").unwrap().rows.is_empty());
    let before = counts(&fixture.db).await;
    let calls = fixture.request_counts().await;
    let writer = fixture.writer.begin().await.unwrap();
    writer
        .execute_unprepared(
            "INSERT INTO pin_resource_history(provider,cid,epoch,ledger)
        VALUES ('stage7-apply-phantom','uncommitted-first-insert',1,'{}')",
        )
        .await
        .unwrap();
    assert_nowait_refusal(fixture, &binding).await;
    writer.rollback().await.unwrap();
    assert_eq!(counts(&fixture.db).await, before);
    assert!(native::capture(&fixture.db).await.unwrap() == frame);

    // Same row count, different primary-key membership. Fresh apply must see the
    // committed change, not reuse the confirmation's old repeatable-read frame.
    let old_job = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let replacement_id = uuid::Uuid::new_v4().to_string();
    fixture
        .writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE pin_jobs SET id=$1 WHERE id=$2",
            [replacement_id.clone().into(), old_job.id.clone().into()],
        ))
        .await
        .unwrap();
    let drifted = native::capture(&fixture.db).await.unwrap();
    assert_eq!(
        frame.table("pin_jobs").unwrap().rows.len(),
        drifted.table("pin_jobs").unwrap().rows.len()
    );
    assert!(frame.frontier != drifted.frontier);
    assert!(matches!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await,
        Err(ManagementError::Changed)
    ));
    assert_eq!(counts(&fixture.db).await, before);
    assert!(native::capture(&fixture.db).await.unwrap() == drifted);
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .is_none()
    );
    // Restore only this fixture's changed job; old receipts regain their exact ID.
    fixture
        .writer
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE pin_jobs SET id=$1 WHERE id=$2",
            [old_job.id.into(), replacement_id.into()],
        ))
        .await
        .unwrap();
    assert!(native::capture(&fixture.db).await.unwrap() == frame);
    assert!(
        !actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(counts(&fixture.db).await.leases, before.leases + 1);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn renew_after_create_and_cross_connection_replay(fixture: &Fixture) {
    let identity = fixture.prepared_object("renew-replay").await;
    let create = fixture
        .confirm(&fixture.create_request(identity.clone()))
        .await;
    let created = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &create)
        .await
        .unwrap();
    assert!(!created.replayed);
    assert_eq!(created.items.len(), 1);
    assert_eq!(created.items[0].receipt.intent_ids.len(), 1);
    assert_eq!(created.items[0].targets.len(), 1);
    let original = pin_lease::Entity::find_by_id(created.items[0].receipt.intent_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let birth = pin_lease_requirement::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find_by_id(created.items[0].targets[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.owner_object_id, identity.object_id);
    assert_eq!(target.lease_id, original.id);
    let expires_at = original.expires_at.timestamp() + 300;
    let request = PlanRequest {
        plan_id: uuid::Uuid::new_v4().to_string(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action: Action::Renew { expires_at },
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target.id.clone(),
                    source: None,
                },
            },
        }],
    };
    let binding = fixture.confirm(&request).await;
    let before = counts(&fixture.db).await;
    let calls = fixture.request_counts().await;
    let renewed = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!renewed.replayed);
    assert_eq!(renewed.items.len(), 1);
    let item = &renewed.items[0];
    assert_eq!(item.receipt.intent_ids, vec![original.id.clone()]);
    assert_eq!(item.targets, vec![target.id.clone()]);
    let extended = pin_lease::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(extended.expires_at.timestamp(), expires_at);
    assert_eq!(extended.generation, original.generation + 1);
    assert_eq!(extended.created_at, original.created_at);
    assert_eq!(extended.source, original.source);
    assert_eq!(extended.owner_object_id, original.owner_object_id);
    assert_eq!(extended.state, "active");
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(original.id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        birth
    );
    let after = counts(&fixture.db).await;
    assert_eq!(after.leases, before.leases);
    assert_eq!(after.targets, before.targets);
    assert_eq!(after.requirements, before.requirements);
    assert_eq!(after.applied, before.applied + 1);
    assert_eq!(
        (after.remotes, after.reserved_bytes, after.reserved_pins),
        (before.remotes, before.reserved_bytes, before.reserved_pins)
    );
    let persisted = records::lookup_committed(&fixture.writer, &fixture.auth, &binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert!(persisted == *item);
    let frame = native::capture(&fixture.db).await.unwrap();
    let auth = fixture.reauthorize_writer().await;
    let replay = actions::apply(&fixture.writer, &fixture.config, &auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed && replay.items == renewed.items);
    assert_eq!(counts(&fixture.db).await, after);
    assert!(native::capture(&fixture.db).await.unwrap() == frame);
    assert_eq!(fixture.request_counts().await, calls);
}

async fn assert_nowait_refusal(fixture: &Fixture, binding: &records::ConfirmedBinding) {
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, binding),
    )
    .await
    .expect("formal apply must fail NOWAIT, not wait for the writer or statement timeout");
    assert!(matches!(result, Err(ManagementError::Changed)));
}
