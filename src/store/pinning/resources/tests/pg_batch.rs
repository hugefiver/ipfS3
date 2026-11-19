use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::time::{sleep, timeout};

const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

async fn add_demand(
    db: &DatabaseConnection,
    limits: &ProviderLimitMap,
    provider: &str,
    file: &PinTargetSpec,
) {
    let demand = intent(LeaseSource::Manual, ProviderMode::One, &[provider]);
    let tx = db.begin().await.unwrap();
    attach(&tx, &file.cid, file, &demand, limits, time(0))
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires an isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_batch_reserves_pin_and_byte_quotas_across_connections() {
    for (provider, max_pins, max_bytes, size) in [
        ("pg-batch-pin-quota", 2, 10, 1),
        ("pg-batch-byte-quota", 3, 8, 4),
    ] {
        postgres::with_schema(|db| async move {
            db.execute_unprepared(include_str!("../schema.sql"))
                .await
                .unwrap();
            let limits = BTreeMap::from([(
                provider.to_owned(),
                ProviderLimits {
                    max_pins,
                    max_bytes,
                    ..limits()["alpha"].clone()
                },
            )]);
            let budgets = BTreeMap::from([(provider.to_owned(), 4)]);
            for seed in 1..=4 {
                add_demand(&db, &limits, provider, &target(seed, size)).await;
            }
            let page = candidate_page(&db, provider, &limits[provider], None, 4, time(0))
                .await
                .unwrap();
            assert_eq!(page.len(), 4);

            let gate = db.begin().await.unwrap();
            let key: i64 = gate
                .query_one(stmt(
                    &gate,
                    "SELECT pg_backend_pid()::bigint AS gate_key",
                    vec![],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "gate_key")
                .unwrap();
            gate.execute(stmt(
                &gate,
                "SELECT pg_advisory_xact_lock($1)",
                vec![key.into()],
            ))
            .await
            .unwrap();
            // Park the winner at its write, after it has read capacity and before it commits.
            db.execute_unprepared(&format!(
                "CREATE FUNCTION pause_pin_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN PERFORM pg_advisory_xact_lock({key}::bigint); RETURN NEW; END; $$;
                 CREATE TRIGGER pause_pin_reservation BEFORE UPDATE ON pin_resources
                 FOR EACH ROW WHEN (OLD.status = 'waiting' AND NEW.claim_token IS NOT NULL)
                 EXECUTE FUNCTION pause_pin_reservation();"
            ))
            .await
            .unwrap();
            let (first, competing) = tokio::join!(
                timeout(
                    LOCK_TIMEOUT * 3,
                    claim_batch(&db, &page, &limits, &budgets, 1, time(0), time(30)),
                ),
                async {
                    timeout(LOCK_TIMEOUT, async {
                        loop {
                            let row = gate
                                .query_one(stmt(
                                    &gate,
                                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory'
                                     AND NOT granted AND classid=0 AND objid::bigint=$1
                                     AND objsubid=1) AS waiting",
                                    vec![key.into()],
                                ))
                                .await
                                .unwrap()
                                .unwrap();
                            if row.try_get::<bool>("", "waiting").unwrap() {
                                break;
                            }
                            sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .expect("first claim must reach the reservation write");
                    let competing = timeout(
                        LOCK_TIMEOUT,
                        claim_batch(&db, &page, &limits, &budgets, 4, time(0), time(30)),
                    )
                    .await;
                    gate.rollback().await.unwrap();
                    competing
                        .expect("the competing claim must not wait for the winner")
                        .unwrap()
                },
            );
            let first = first
                .expect("first claim must finish after unlocking")
                .unwrap();
            assert_eq!(first.claims.len(), 1);
            assert!(competing.claims.is_empty());
            assert_eq!(competing.blocked, BTreeSet::from([provider.to_owned()]));

            // A retry uses the same pre-commit page while the first claim is still live.
            let retry = claim_batch(&db, &page, &limits, &budgets, 4, time(0), time(30))
                .await
                .unwrap();
            assert_eq!(retry.claims.len(), 1, "{provider}");
            assert_ne!(first.claims[0].cid, retry.claims[0].cid);
            let reserved = db
                .query_one(stmt(
                    &db,
                    "SELECT COUNT(*) AS pins, CAST(SUM(logical_size) AS BIGINT) AS bytes
                 FROM pin_resources WHERE provider=$1 AND status='pending' AND action='submit'",
                    vec![provider.into()],
                ))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reserved.try_get::<i64>("", "pins").unwrap(), 2);
            assert_eq!(reserved.try_get::<i64>("", "bytes").unwrap(), 2 * size);
        })
        .await;
    }
}

#[tokio::test]
#[ignore = "requires an isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_batch_busy_submit_provider_does_not_block_delete_or_other_provider() {
    postgres::with_schema(|db| async move {
        db.execute_unprepared(include_str!("../schema.sql"))
            .await
            .unwrap();
        let (busy, other) = ("pg-batch-busy", "pg-batch-other");
        let limits = BTreeMap::from([
            (busy.to_owned(), limits()["alpha"].clone()),
            (other.to_owned(), limits()["beta"].clone()),
        ]);
        let budgets = BTreeMap::from([(busy.to_owned(), 3), (other.to_owned(), 3)]);
        let old = target(1, 1);
        add_demand(&db, &limits, busy, &old).await;
        let page = candidate_page(&db, busy, &limits[busy], None, 3, time(0))
            .await
            .unwrap();
        let old_claim = claim_batch(&db, &page, &limits, &budgets, 3, time(0), time(30))
            .await
            .unwrap()
            .claims
            .pop()
            .unwrap();
        settle(
            &db,
            &old_claim,
            Outcome::Submitted("old-handle".into()),
            time(1),
        )
        .await
        .unwrap();
        let tx = db.begin().await.unwrap();
        release_owner(&tx, &old.cid, time(2)).await.unwrap();
        tx.commit().await.unwrap();

        let busy_submit = target(2, 1);
        let other_submit = target(3, 1);
        add_demand(&db, &limits, busy, &busy_submit).await;
        add_demand(&db, &limits, other, &other_submit).await;
        let mut page = candidate_page(&db, busy, &limits[busy], None, 3, time(2))
            .await
            .unwrap();
        assert_eq!(page.len(), 2);
        page.extend(
            candidate_page(&db, other, &limits[other], None, 3, time(2))
                .await
                .unwrap(),
        );
        assert_eq!(page.len(), 3);

        let held = db.begin().await.unwrap();
        held.execute(stmt(
            &held,
            "SELECT pg_advisory_xact_lock(1844053111,hashtext($1))",
            vec![busy.into()],
        ))
        .await
        .unwrap();
        let batch = timeout(
            LOCK_TIMEOUT,
            claim_batch(&db, &page, &limits, &budgets, 3, time(2), time(30)),
        )
        .await;
        held.rollback().await.unwrap();
        let batch = batch
            .expect("a busy submit provider must not block the batch")
            .unwrap();
        assert_eq!(batch.blocked, BTreeSet::from([busy.to_owned()]));
        assert_eq!(batch.claims.len(), 2);
        let delete = batch
            .claims
            .iter()
            .find(|claim| claim.provider == busy)
            .unwrap();
        assert_eq!(delete.action, Action::Delete);
        assert_eq!(delete.cid, old_claim.cid);
        assert_eq!(delete.handle.as_deref(), Some("old-handle"));
        let submit = batch
            .claims
            .iter()
            .find(|claim| claim.provider == other)
            .unwrap();
        assert_eq!(submit.action, Action::Submit);
        assert_eq!(submit.source_cid, other_submit.cid);

        let retry = claim_batch(&db, &page, &limits, &budgets, 3, time(2), time(30))
            .await
            .unwrap();
        assert_eq!(retry.claims.len(), 1);
        assert_eq!(retry.claims[0].source_cid, busy_submit.cid);
    })
    .await;
}

#[tokio::test]
#[ignore = "requires an isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_batch_skips_locked_stale_and_submit_rows() {
    postgres::with_schema(|db| async move {
        db.execute_unprepared(include_str!("../schema.sql"))
            .await
            .unwrap();
        let provider = "pg-batch-skip-locked";
        let limits = BTreeMap::from([(provider.to_owned(), limits()["alpha"].clone())]);
        let budgets = BTreeMap::from([(provider.to_owned(), 3)]);
        add_demand(&db, &limits, provider, &target(1, 1)).await;
        let page = candidate_page(&db, provider, &limits[provider], None, 3, time(0))
            .await
            .unwrap();
        let stale = claim_batch(&db, &page, &limits, &budgets, 3, time(0), time(5))
            .await
            .unwrap()
            .claims
            .pop()
            .unwrap();
        let locked_submit = target(2, 1);
        let ready_submit = target(3, 1);
        add_demand(&db, &limits, provider, &locked_submit).await;
        add_demand(&db, &limits, provider, &ready_submit).await;
        let locked_cid =
            crate::pinning::provider::canonical_resource_cid(&locked_submit.cid).unwrap();
        let mut page = candidate_page(&db, provider, &limits[provider], None, 3, time(5))
            .await
            .unwrap();
        assert_eq!(page.len(), 3);
        assert!(page.iter().any(|row| row.cid == stale.cid));
        assert!(page.iter().any(|row| row.cid == locked_cid));
        page.sort_by_key(|row| row.cid != stale.cid && row.cid != locked_cid);

        let held = db.begin().await.unwrap();
        let locked = held
            .query_all(stmt(
                &held,
                "SELECT cid FROM pin_resources WHERE provider=$1 AND cid IN ($2,$3) FOR UPDATE",
                vec![provider.into(), stale.cid.clone().into(), locked_cid.into()],
            ))
            .await
            .unwrap();
        assert_eq!(locked.len(), 2);
        let batch = timeout(
            LOCK_TIMEOUT,
            claim_batch(&db, &page, &limits, &budgets, 3, time(5), time(30)),
        )
        .await;
        held.rollback().await.unwrap();
        let batch = batch
            .expect("locked candidates must not block the ready submit")
            .unwrap();
        assert_eq!(batch.claims.len(), 1);
        assert_eq!(batch.claims[0].source_cid, ready_submit.cid);

        let retry = claim_batch(&db, &page, &limits, &budgets, 3, time(5), time(30))
            .await
            .unwrap();
        assert_eq!(retry.claims.len(), 1);
        assert_eq!(retry.claims[0].source_cid, locked_submit.cid);
        let expired = db
            .query_one(stmt(
                &db,
                "SELECT status,claim_token,claim_until FROM pin_resources WHERE provider=$1 AND cid=$2",
                vec![provider.into(), stale.cid.into()],
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.try_get::<String>("", "status").unwrap(), "unknown");
        assert_eq!(
            expired.try_get::<String>("", "claim_token").unwrap(),
            stale.token
        );
        assert_eq!(
            expired.try_get::<Option<i64>>("", "claim_until").unwrap(),
            None
        );
    })
    .await;
}
