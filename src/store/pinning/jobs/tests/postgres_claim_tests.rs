use sea_orm::Statement;

use super::*;
use crate::store::Store;

async fn peer(db: &DatabaseConnection) -> DatabaseConnection {
    let schema: String = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT current_schema() AS schema",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "schema")
        .unwrap();
    let mut options = ConnectOptions::new(std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap());
    options.set_schema_search_path(schema).max_connections(1);
    let db = Database::connect(options).await.unwrap();
    db.execute_unprepared("SET lock_timeout TO '1s'")
        .await
        .unwrap();
    db
}

async fn try_lock_job(tx: &sea_orm::DatabaseTransaction, id: &str) -> bool {
    tx.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT id FROM pin_jobs WHERE id=$1 FOR UPDATE SKIP LOCKED",
        [id.into()],
    ))
    .await
    .unwrap()
    .is_some()
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_generic_claim_fills_limit_behind_locked_head() {
    isolated_postgres::isolated(true, |db| async move {
        for (second, cid) in ["head", "first", "second", "third"].into_iter().enumerate() {
            enqueue_job(&db, unpin_job("generic", cid, 1, time(second as i64)))
                .await
                .unwrap();
        }
        let claimant = peer(&db).await;
        let holder = db.begin().await.unwrap();
        assert!(try_lock_job(&holder, &stable_unpin_id("generic", "head", 1)).await);

        for (limit, expected) in [(1, &["first"][..]), (2, &["second", "third"][..])] {
            let claims = claim_due_jobs(&claimant, time(5), Duration::seconds(30), limit)
                .await
                .unwrap();
            assert_eq!(
                claims
                    .iter()
                    .map(|job| job.model.cid.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        holder.rollback().await.unwrap();
        assert_eq!(
            job(&db, &stable_unpin_id("generic", "head", 1)).await.state,
            STATE_PENDING
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_worker_fair_claim_fills_limit_behind_locked_head() {
    isolated_postgres::isolated(true, |db| async move {
        let claimant = peer(&db).await;
        let store = Store::new(claimant);
        for (provider, cid, second) in [
            ("preferred", "head", 0),
            ("preferred", "first", 1),
            ("preferred", "second", 2),
            ("backup", "backup", 0),
        ] {
            enqueue_job(&db, unpin_job(provider, cid, 1, time(second)))
                .await
                .unwrap();
        }
        let seed = db.begin().await.unwrap();
        enqueue_job(
            &seed,
            submit_job("preferred", "other-lane", "lease", "target", 1, time(-2)),
        )
        .await
        .unwrap();
        seed.commit().await.unwrap();
        enqueue_job(&db, unpin_job("preferred", "active", 1, time(-1)))
            .await
            .unwrap();
        let holder = db.begin().await.unwrap();
        assert!(try_lock_job(&holder, &stable_unpin_id("preferred", "head", 1)).await);
        let schedule = WorkerClaimSchedule {
            lane: WorkerClaimLane::Ordinary(&BTreeSet::from(["preferred".to_owned()])),
            in_flight_by_provider: &BTreeMap::new(),
            last_served_ticket_by_provider: &BTreeMap::new(),
            configured_priorities: &BTreeMap::from([("preferred".to_owned(), 1)]),
            active_job_ids: &BTreeSet::from([stable_unpin_id("preferred", "active", 1)]),
        };

        for (limit, expected) in [(1, &["first"][..]), (2, &["second", "backup"][..])] {
            let claims = claim_due_worker_jobs_store(
                &store,
                time(5),
                Duration::seconds(30),
                limit,
                schedule,
            )
            .await
            .unwrap();
            assert_eq!(
                claims
                    .iter()
                    .map(|job| job.model.cid.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        holder.rollback().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_fair_claim_does_not_lock_unselected_rows() {
    isolated_postgres::isolated(true, |db| async move {
        for (provider, cid, second) in [
            ("selected", "head", 0),
            ("selected", "tail", 1),
            ("unselected", "head", 0),
            ("unselected", "tail", 1),
        ] {
            enqueue_job(&db, unpin_job(provider, cid, 1, time(second)))
                .await
                .unwrap();
        }
        let observer = peer(&db).await;
        let claimant = db.begin().await.unwrap();
        let claims = claim_due_jobs_fair(
            &claimant,
            time(5),
            Duration::seconds(30),
            1,
            &BTreeMap::new(),
            &BTreeMap::from([("unselected".to_owned(), 1)]),
            &BTreeMap::from([("unselected".to_owned(), 0)]),
        )
        .await
        .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].model.id, stable_unpin_id("selected", "head", 1));

        let probe = observer.begin().await.unwrap();
        assert!(!try_lock_job(&probe, &claims[0].model.id).await);
        for (provider, cid) in [
            ("selected", "tail"),
            ("unselected", "head"),
            ("unselected", "tail"),
        ] {
            assert!(
                try_lock_job(&probe, &stable_unpin_id(provider, cid, 1)).await,
                "held unselected row {provider}/{cid}"
            );
        }
        probe.rollback().await.unwrap();
        claimant.rollback().await.unwrap();
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_due_claim_releases_jobs_while_revision_writer_is_active() {
    isolated_postgres::isolated(true, |db| async move {
        for (cid, second) in [("claimable", 0), ("future", 60)] {
            enqueue_job(&db, unpin_job("provider", cid, 1, time(second)))
                .await
                .unwrap();
        }
        let claimant = peer(&db).await;
        let writer = db.begin().await.unwrap();
        // The normal pin_jobs write trigger holds the shared revision row.
        writer
            .execute_unprepared("UPDATE pin_jobs SET attempts=attempts+1 WHERE cid='future'")
            .await
            .unwrap();
        let outer = claimant.begin().await.unwrap();
        let id = stable_unpin_id("provider", "claimable", 1);
        let generic = claim_due_jobs(&outer, time(5), Duration::seconds(30), 1).await;
        let fair = claim_due_jobs_fair(
            &outer,
            time(5),
            Duration::seconds(30),
            1,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .await;
        assert!(
            try_lock_job(&writer, &id).await,
            "empty claims must release their candidate row locks"
        );
        writer.rollback().await.unwrap();
        assert!(
            generic
                .expect("generic claim waited on the revision writer")
                .is_empty()
        );
        assert!(
            fair.expect("fair claim waited on the revision writer")
                .is_empty()
        );
        outer.rollback().await.unwrap();
        let claims = claim_due_jobs(&claimant, time(5), Duration::seconds(30), 1)
            .await
            .unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].model.id, id);
    })
    .await;
}
