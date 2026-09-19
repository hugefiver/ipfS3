//! Run only against an explicitly supplied test PostgreSQL endpoint. Every test
//! owns a UUID schema; public/application tables are never migrated or deleted.
use chrono::{Duration, Utc};
use ipfs_s3_gateway::store::{self, entities::pin_job, pinning::jobs};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, EntityTrait,
    Statement, TransactionTrait,
};

async fn connect(url: &str, schema: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.min_connections(1).max_connections(1);
    let db = Database::connect(options).await.unwrap();
    assert!(
        schema.starts_with("stage1_pinning_")
            && schema
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
    db.execute_unprepared("SET statement_timeout TO '10s'")
        .await
        .unwrap();
    db
}

async fn scalar_i64(db: &DatabaseConnection, sql: String) -> i64 {
    db.query_one(Statement::from_string(DatabaseBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "value")
        .unwrap()
}

async fn wait_for(db: &DatabaseConnection, sql: String) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let value = db
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    sql.clone(),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<bool>("", "value")
                .unwrap();
            if value {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("database barrier was not reached");
}

async fn seed(db: &DatabaseConnection) -> jobs::ClaimedPinJob {
    db.execute_unprepared("INSERT INTO pin_jobs (id,operation,provider,cid,lease_id,target_id,expected_generation,state,next_attempt_at,submit_phase) VALUES ('job','submit','pinata','cid','lease','target',1,'pending','2026-01-01T00:00:00Z','ready')").await.unwrap();
    let claim = jobs::claim_due_jobs(db, Utc::now(), Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let txn = db.begin().await.unwrap();
    jobs::record_submit_invocation(&txn, &claim, "pinata_v3", "cid", Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    claim
}

async fn fixture() -> (String, String, DatabaseConnection) {
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect(
        "set an isolated test PostgreSQL URL explicitly; this ignored test never silently skips",
    );
    let schema = format!("stage1_pinning_{}", uuid::Uuid::new_v4().simple());
    let control = connect(&url, &schema).await;
    control
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    store::run_migrations(&control).await.unwrap();
    (url, schema, control)
}

async fn cleanup(control: DatabaseConnection, schema: &str) {
    control
        .execute_unprepared("SELECT pg_advisory_unlock_all()")
        .await
        .unwrap();
    assert!(
        schema.starts_with("stage1_pinning_")
            && schema
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    control
        .execute_unprepared("SET search_path TO public")
        .await
        .unwrap();
    control
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    control.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL pointing to an authorized test database"]
async fn postgres_stage1_park_fence_prevents_takeover_between_history_and_job_cas() {
    let (url, schema, control) = fixture().await;
    let parker = connect(&url, &schema).await;
    let successor = connect(&url, &schema).await;
    let observer = connect(&url, &schema).await;
    let claim = seed(&control).await;
    let park_pid = scalar_i64(&parker, "SELECT pg_backend_pid()::bigint AS value".into()).await;
    let next_pid = scalar_i64(
        &successor,
        "SELECT pg_backend_pid()::bigint AS value".into(),
    )
    .await;
    let key = i64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());
    control.execute_unprepared(&format!("CREATE FUNCTION gate_history() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$; CREATE TRIGGER gate_history BEFORE UPDATE ON pin_submit_history FOR EACH ROW EXECUTE FUNCTION gate_history(); SELECT pg_advisory_lock({key})")).await.unwrap();
    let task = tokio::spawn(async move {
        let mut attempts = tokio::task::JoinSet::new();
        attempts.spawn(async move {
            let result =
                jobs::park_submit(&parker, &claim, "needs_attention", "parked", Utc::now()).await;
            parker.close().await.unwrap();
            result.unwrap();
            0
        });
        // The parker has completed its fence and is blocked inside history UPDATE.
        wait_for(&observer, format!("SELECT COALESCE((SELECT wait_event='advisory' FROM pg_stat_activity WHERE pid={park_pid}), false) AS value")).await;
        attempts.spawn(async move {
            let claims = jobs::claim_due_jobs(
                &successor,
                Utc::now() + Duration::minutes(2),
                Duration::seconds(30),
                1,
            )
            .await
            .unwrap();
            successor.close().await.unwrap();
            claims.len()
        });
        // Real claim CAS must block on the same still-open transaction, rather
        // than succeed while the old parker later modifies the new owner's history.
        wait_for(
            &observer,
            format!("SELECT {park_pid} = ANY(pg_blocking_pids({next_pid})) AS value"),
        )
        .await;
        observer.close().await.unwrap();
        attempts
    });
    let barrier = task.await;
    control
        .execute_unprepared(&format!("SELECT pg_advisory_unlock({key})"))
        .await
        .unwrap();
    let result = match barrier {
        Ok(mut attempts) => {
            let mut result = Ok(());
            while let Some(joined) = attempts.join_next().await {
                match joined {
                    Ok(0) => {}
                    Ok(_) => result = Err("successor claimed parked work".to_owned()),
                    Err(error) => result = Err(error.to_string()),
                }
            }
            result
        }
        Err(error) => Err(error.to_string()),
    };
    let history = jobs::submission_history(&control, "job")
        .await
        .unwrap()
        .unwrap();
    let job = pin_job::Entity::find_by_id("job")
        .one(&control)
        .await
        .unwrap()
        .unwrap();
    cleanup(control, &schema).await;
    result.unwrap();
    assert_eq!(history.state, "needs_attention");
    assert_eq!(job.locked_until, None);
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL pointing to an authorized test database"]
async fn postgres_stage1_failed_park_cas_rolls_back_history() {
    let (url, schema, control) = fixture().await;
    let writer = connect(&url, &schema).await;
    let claim = seed(&control).await;
    let before = jobs::submission_history(&control, "job").await.unwrap();
    control.execute_unprepared("CREATE FUNCTION reject_park() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.locked_until IS NOT NULL AND NEW.locked_until IS NULL THEN RETURN NULL; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_park BEFORE UPDATE ON pin_jobs FOR EACH ROW EXECUTE FUNCTION reject_park()").await.unwrap();
    let result = jobs::park_submit(
        &writer,
        &claim,
        "needs_attention",
        "must-rollback",
        Utc::now(),
    )
    .await;
    let after = jobs::submission_history(&control, "job").await.unwrap();
    let after_job = pin_job::Entity::find_by_id("job")
        .one(&control)
        .await
        .unwrap()
        .unwrap();
    writer.close().await.unwrap();
    cleanup(control, &schema).await;
    assert!(
        result.unwrap_err().to_string().contains("stale"),
        "the injected final CAS must fail, not an earlier SQL statement"
    );
    assert_eq!(after, before);
    assert_eq!(after_job.locked_until, claim.model.locked_until);
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL pointing to an authorized test database"]
async fn postgres_stage1_repair_cannot_lose_wake_between_isolation_check_and_reconcile_park() {
    let (url, schema, control) = fixture().await;
    let reconciler = connect(&url, &schema).await;
    let repairer = connect(&url, &schema).await;
    let submit = seed(&control).await;
    let txn = control.begin().await.unwrap();
    jobs::record_submit_error(&txn, &submit, "not_created", "rejected")
        .await
        .unwrap();
    jobs::park_submit(&txn, &submit, "blocked", "rejected", Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    control.execute_unprepared("INSERT INTO remote_pins (provider,cid,cid_size,status,epoch,last_touched_at) VALUES ('pinata','cid',100,'reserved',1,CURRENT_TIMESTAMP); INSERT INTO pin_provider_usage (provider,reserved_bytes,reserved_pins) VALUES ('pinata',100,1); INSERT INTO pin_jobs (id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('reconcile','reconcile','pinata','cid',1,'pending','2026-01-01T00:00:00Z',NULL)").await.unwrap();
    let reconcile = jobs::claim_due_jobs(&control, Utc::now(), Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let reconcile_pid = scalar_i64(
        &reconciler,
        "SELECT pg_backend_pid()::bigint AS value".into(),
    )
    .await;
    let repair_pid = scalar_i64(&repairer, "SELECT pg_backend_pid()::bigint AS value".into()).await;
    let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
    let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
    let parked = tokio::spawn(async move {
        let txn = reconciler.begin().await.unwrap();
        assert!(
            jobs::fence_parked_submits(&txn, "pinata", "cid")
                .await
                .unwrap()
        );
        observed_tx.send(()).unwrap();
        continue_rx.await.unwrap();
        jobs::park_reconcile_for_submit_attention(&txn, &reconcile, Utc::now())
            .await
            .unwrap();
        txn.commit().await.unwrap();
        reconciler.close().await.unwrap();
    });
    observed_rx.await.unwrap();
    let mut repaired = tokio::spawn(async move {
        let result = jobs::resume_rejected_submit_after_repair(
            &repairer,
            "job",
            "pinata_v3",
            "cid",
            Utc::now(),
        )
        .await
        .unwrap();
        repairer.close().await.unwrap();
        result
    });
    // The buggy implementation lets repair commit here: its wake sees the
    // Reconcile's non-NULL claim and skips it, then the Reconcile parks forever.
    let early = tokio::select! {
        result = &mut repaired => Some(result),
        _ = wait_for(&control, format!("SELECT {reconcile_pid} = ANY(pg_blocking_pids({repair_pid})) AS value")) => None,
    };
    continue_tx.send(()).unwrap();
    let park_result = parked.await;
    let repair_result = match early {
        Some(result) => result,
        None => repaired.await,
    };
    let job = pin_job::Entity::find_by_id("reconcile")
        .one(&control)
        .await
        .unwrap()
        .unwrap();
    let submit = pin_job::Entity::find_by_id("job")
        .one(&control)
        .await
        .unwrap()
        .unwrap();
    let history = jobs::submission_history(&control, "job")
        .await
        .unwrap()
        .unwrap();
    let usage = scalar_i64(
        &control,
        "SELECT reserved_bytes AS value FROM pin_provider_usage WHERE provider='pinata'".into(),
    )
    .await;
    cleanup(control, &schema).await;
    park_result.unwrap();
    assert!(repair_result.unwrap());
    assert_eq!(submit.state, "pending");
    assert_eq!(history.state, "active");
    assert_eq!(usage, 100);
    assert_eq!(
        job.state, "pending",
        "completed repair must not leave Reconcile permanently isolated"
    );
    repair_winning_the_submit_lock_is_rechecked().await;
}

async fn repair_winning_the_submit_lock_is_rechecked() {
    let (url, schema, control) = fixture().await;
    let reconciler = connect(&url, &schema).await;
    let repairer = connect(&url, &schema).await;
    let submit = seed(&control).await;
    let txn = control.begin().await.unwrap();
    jobs::record_submit_error(&txn, &submit, "not_created", "rejected")
        .await
        .unwrap();
    jobs::park_submit(&txn, &submit, "blocked", "rejected", Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    control.execute_unprepared("INSERT INTO pin_jobs (id,operation,provider,cid,expected_remote_epoch,state,next_attempt_at,submit_phase) VALUES ('reconcile','reconcile','pinata','cid',1,'pending','2026-01-01T00:00:00Z',NULL)").await.unwrap();
    let claim = jobs::claim_due_jobs(&control, Utc::now(), Duration::seconds(30), 1)
        .await
        .unwrap()
        .remove(0);
    let reconcile_pid = scalar_i64(
        &reconciler,
        "SELECT pg_backend_pid()::bigint AS value".into(),
    )
    .await;
    let repair_pid = scalar_i64(&repairer, "SELECT pg_backend_pid()::bigint AS value".into()).await;
    let key = i64::from_be_bytes(uuid::Uuid::new_v4().as_bytes()[..8].try_into().unwrap());
    control.execute_unprepared(&format!("CREATE FUNCTION gate_repair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END $$; CREATE TRIGGER gate_repair BEFORE UPDATE ON pin_submit_history FOR EACH ROW EXECUTE FUNCTION gate_repair(); SELECT pg_advisory_lock({key})")).await.unwrap();
    let repair = tokio::spawn(async move {
        let result = jobs::resume_rejected_submit_after_repair(
            &repairer,
            "job",
            "pinata_v3",
            "cid",
            Utc::now(),
        )
        .await;
        repairer.close().await.unwrap();
        result
    });
    // Repair owns Submit, but has not changed the visible parked state yet.
    wait_for(&control, format!("SELECT COALESCE((SELECT wait_event='advisory' FROM pg_stat_activity WHERE pid={repair_pid}), false) AS value")).await;
    let mut reconcile = tokio::spawn(async move {
        let txn = reconciler.begin().await.unwrap();
        let isolated = jobs::fence_parked_submits(&txn, "pinata", "cid")
            .await
            .unwrap();
        if isolated {
            jobs::park_reconcile_for_submit_attention(&txn, &claim, Utc::now())
                .await
                .unwrap();
        } else {
            jobs::reschedule_reconcile_job(
                &txn,
                &claim.model.id,
                claim.model.locked_until.unwrap(),
                Utc::now(),
            )
            .await
            .unwrap();
        }
        txn.commit().await.unwrap();
        reconciler.close().await.unwrap();
        isolated
    });
    let early = tokio::select! {
        result = &mut reconcile => Some(result),
        _ = wait_for(&control, format!("SELECT {repair_pid} = ANY(pg_blocking_pids({reconcile_pid})) AS value")) => None,
    };
    control
        .execute_unprepared(&format!("SELECT pg_advisory_unlock({key})"))
        .await
        .unwrap();
    let repaired = repair.await;
    let isolated = match early {
        Some(result) => result,
        None => reconcile.await,
    };
    let job = pin_job::Entity::find_by_id("reconcile")
        .one(&control)
        .await
        .unwrap()
        .unwrap();
    cleanup(control, &schema).await;
    assert!(repaired.unwrap().unwrap());
    assert!(
        !isolated.unwrap(),
        "the row predicate must be rechecked after waiting for repair"
    );
    assert_eq!(job.state, "pending");
}
