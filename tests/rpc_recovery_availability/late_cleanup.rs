use super::*;

async fn remote(f: &Fixture) -> remote_pin::Model {
    remote_pin::Entity::find_by_id((f.provider.clone(), CID.to_owned()))
        .one(f.store.db())
        .await
        .unwrap()
        .unwrap()
}

async fn cancel(f: &Fixture, lease: &str) {
    let tx = f.store.db().begin().await.unwrap();
    assert_eq!(
        leases::cancel_lease(&tx, lease, Utc::now()).await.unwrap(),
        leases::GenerationDecision::Current
    );
    tx.commit().await.unwrap();
}

async fn inactive_reconcile(f: &Fixture, epoch: i64) -> bool {
    pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&f.provider))
        .filter(pin_job::Column::Cid.eq(CID))
        .filter(pin_job::Column::Operation.eq("reconcile"))
        .filter(pin_job::Column::ExpectedRemoteEpoch.eq(epoch))
        .one(f.store.db())
        .await
        .unwrap()
        .is_some_and(|j| {
            j.locked_until.is_none() && matches!(j.state.as_str(), "running" | "error" | "done")
        })
}

#[derive(Clone, Copy)]
enum Boundary {
    SameLifetime,
    Restart,
    ArchivedRelease,
    OtherUnknown,
}

async fn end_during_submit(boundary: Boundary) {
    let f = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let target = f.publish("a", CID).await;
    let worker = f
        .coordinator
        .start(f.store.clone(), CancellationToken::new());
    held_start(&f).await;
    let original = f.expire_submit_claim().await;
    let recovery = f
        .coordinator
        .start(f.store.clone(), CancellationToken::new());
    let mut workers = vec![worker, recovery];
    wait_for_stage("submit relinquished its claim", || async {
        f.submit().await.locked_until.is_none()
    })
    .await;

    match boundary {
        Boundary::ArchivedRelease => {
            ledger::archive_confirmed_release(f.store.db(), &remote(&f).await)
                .await
                .unwrap();
        }
        Boundary::OtherUnknown => {
            let db = f.store.db();
            let inserted = db.execute_unprepared(
                "INSERT INTO pin_submit_observations (id,job_id,submit_call,provider,expected_cid,route,remote_epoch,claim_until,started_at,observed_at,effect,resources,outcome,needs_attention)
                 SELECT 'other:call:1','other',1,provider,expected_cid,route,remote_epoch,claim_until,started_at,started_at,'unknown','[]','unknown',TRUE FROM pin_submit_observations"
            ).await.unwrap();
            assert_eq!(inserted.rows_affected(), 1);
        }
        Boundary::SameLifetime | Boundary::Restart => {}
    }
    cancel(&f, original.lease_id.as_deref().unwrap()).await;
    let epoch = remote(&f).await.epoch;
    assert!(epoch > f.receipt().await.remote_epoch);
    wait_for_stage("current reconcile stopped before HTTP returned", || {
        inactive_reconcile(&f, epoch)
    })
    .await;
    assert_eq!(f.receipt().await.outcome, "in_flight");
    assert_eq!(f.target_state(&target).await, "released");
    f.held(100, 1).await;
    f.assert_posts(1);
    if matches!(boundary, Boundary::Restart) {
        f.store.db().execute_unprepared("CREATE TRIGGER miss_cleanup_wake BEFORE UPDATE OF state ON pin_jobs WHEN OLD.operation='reconcile' AND NEW.state='pending' BEGIN SELECT RAISE(ABORT, 'stop after fact commit'); END").await.unwrap();
    }
    if matches!(boundary, Boundary::SameLifetime | Boundary::Restart) {
        f.store.db().execute_unprepared(
            "CREATE TABLE cleanup_reconcile_claims(epoch INTEGER);
             CREATE TRIGGER record_cleanup_reconcile AFTER UPDATE ON pin_jobs
             WHEN NEW.operation='reconcile' AND NEW.state='running' AND OLD.locked_until IS NULL AND NEW.locked_until IS NOT NULL
             BEGIN INSERT INTO cleanup_reconcile_claims VALUES(NEW.expected_remote_epoch); END;"
        ).await.unwrap();
    }

    f.target.release.add_permits(1);
    wait_for_stage("late matched receipt", || async {
        f.receipt().await.outcome == "matched"
    })
    .await;
    match boundary {
        Boundary::SameLifetime | Boundary::Restart => {
            if matches!(boundary, Boundary::Restart) {
                for worker in workers.drain(..) {
                    worker.shutdown(Duration::from_secs(2)).await;
                }
                assert!(inactive_reconcile(&f, epoch).await);
                assert!(f.receipt().await.needs_attention);
                f.assert_posts(1);
                f.store
                    .db()
                    .execute_unprepared("DROP TRIGGER miss_cleanup_wake")
                    .await
                    .unwrap();
                workers.push(
                    f.coordinator
                        .start(f.store.clone(), CancellationToken::new()),
                );
            }
            wait_for_stage("historical remote pin/rm", || async {
                f.target.removes.load(Ordering::SeqCst) != 0
            })
            .await;
            f.held(100, 1).await;
            assert_ne!(remote(&f).await.status, "absent");
            let reclaimed = f
                .store
                .db()
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "SELECT 1 FROM cleanup_reconcile_claims WHERE epoch=$1",
                    [epoch.into()],
                ))
                .await
                .unwrap();
            assert!(
                reclaimed.is_some(),
                "cleanup must pass through a fresh current-epoch Reconcile claim"
            );
            f.target.remove_ack.add_permits(1);
            wait_for_stage("release after remote ACK", || async {
                remote(&f).await.status == "absent"
            })
            .await;
            f.held(0, 0).await;
            assert!(remote(&f).await.request_id.is_none());
            assert!(!f.receipt().await.needs_attention);
            f.assert_requests(1, 1);
        }
        Boundary::ArchivedRelease | Boundary::OtherUnknown => {
            // Let the scheduler finish other real work after the late result, without
            // counting its independent upload as a retry of the original CID.
            let independent = f.publish("independent", OTHER).await;
            wait_for_stage("independent CID pinned", || async {
                f.target_state(&independent).await == "pinned"
            })
            .await;
            f.held(200, 2).await;
            f.assert_posts(2);
            assert_ne!(remote(&f).await.status, "absent");
            if matches!(boundary, Boundary::OtherUnknown) {
                let unknown = ledger::submission::latest(f.store.db(), "other")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(unknown.outcome, "unknown");
                assert!(unknown.needs_attention);
            }
        }
    }
    for worker in workers {
        worker.shutdown(Duration::from_secs(2)).await;
    }
    assert_eq!(f.target_state(&target).await, "released");
    let job = f.submit().await;
    assert_eq!(job.id, original.id);
    assert!(job.locked_until.is_none() && job.state != "pending");
    assert_eq!(
        jobs::submission_history(f.store.db(), &job.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
}

#[tokio::test]
async fn late_matched_keeps_shared_cid_while_another_lease_is_active() {
    let f = Fixture::new((StatusCode::OK, format!("{{\"Hash\":\"{CID}\"}}\n"))).await;
    let first = f.publish("first", CID).await;
    let second = f.publish("second", CID).await;
    let worker = f
        .coordinator
        .start(f.store.clone(), CancellationToken::new());
    held_start(&f).await;
    let submit = f.submit().await;
    let owner = submit.target_id.as_deref().unwrap();
    let survivor = if owner == first { &second } else { &first };
    assert_eq!(f.target_state(survivor).await, "waiting");
    cancel(&f, submit.lease_id.as_deref().unwrap()).await;
    let epoch = remote(&f).await.epoch;
    f.target.release.add_permits(1);
    wait_for_stage("shared current reconcile finished", || async {
        f.receipt().await.outcome == "matched" && inactive_reconcile(&f, epoch).await
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(f.target_state(owner).await, "released");
    let snapshot = leases::remote_work_snapshot(f.store.db(), &f.provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.desired.len(), 1);
    assert_eq!(&snapshot.desired[0].target_id, survivor);
    f.held(100, 1).await;
    f.assert_posts(1);
}

#[tokio::test]
async fn ending_the_only_lease_during_native_kubo_pin_add_cleans_its_remote() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let f = Fixture::with_route(
        (
            StatusCode::OK,
            serde_json::json!({"Pins":[CID]}).to_string(),
        ),
        db,
        true,
    )
    .await;
    let target = f.publish("in-flight", CID).await;
    let worker = f
        .coordinator
        .start(f.store.clone(), CancellationToken::new());
    held_start(&f).await;
    let original = f.submit().await;
    cancel(&f, original.lease_id.as_deref().unwrap()).await;
    f.target.remove_ack.add_permits(1);
    f.target.release.add_permits(1);
    wait_for_stage("original response cleaned without a takeover", || async {
        remote(&f).await.status == "absent"
    })
    .await;
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(f.target_state(&target).await, "released");
    assert!(!f.receipt().await.needs_attention);
    f.held(0, 0).await;
    f.assert_requests(1, 1);
}

#[tokio::test]
async fn zero_desired_late_matched_wakes_current_reconcile_and_releases_after_ack() {
    end_during_submit(Boundary::SameLifetime).await;
}

#[tokio::test]
async fn restart_recovers_matched_receipt_after_failed_cleanup_wake() {
    end_during_submit(Boundary::Restart).await;
}

#[tokio::test]
async fn zero_desired_late_matched_never_unpins_across_release() {
    end_during_submit(Boundary::ArchivedRelease).await;
}

#[tokio::test]
async fn zero_desired_late_matched_never_unpins_over_other_unknown() {
    end_during_submit(Boundary::OtherUnknown).await;
}
