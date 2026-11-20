use super::*;
use crate::pinning::ipfs_rpc::{IpfsRpcProvider, RpcProfile, RpcStrategy};
use crate::{
    config::PinningConfig,
    pinning::{
        config::{LeaseDuration, ProviderLimits, ProviderMode, ValidatedPinningConfig},
        policy::{LeaseIntent, LeaseSource},
        tags::ContentMode,
    },
    store::pinning::publication::PinTargetSpec,
};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_partial_json, method, path},
};

fn cid(seed: u8) -> String {
    cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::wrap(0x12, &[seed; 32]).unwrap(),
    )
    .to_string()
}

async fn wait_status(store: &Store, cid: &str, expected: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = store
                .db()
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "SELECT status FROM pin_resources WHERE cid=?",
                    [cid.into()],
                ))
                .await
                .unwrap();
            if row
                .map(|row| row.try_get::<String>("", "status").unwrap())
                .as_deref()
                == Some(expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

async fn noop_backlog(cids: &[String]) -> (Store, Arc<PinningCoordinator>) {
    let raw: PinningConfig = toml::from_str(
        r#"
        worker_interval = "60s"
        worker_concurrency = 3
        [[providers]]
        name = "noop"
        kind = "noop"
        priority = 1
        max_bytes = 1000
        max_pins = 10
    "#,
    )
    .unwrap();
    let coordinator =
        PinningCoordinator::build(ValidatedPinningConfig::from_raw(&raw, |_| None).unwrap())
            .unwrap();
    let store = Store::open(Database::connect("sqlite::memory:").await.unwrap())
        .await
        .unwrap();
    let intent = LeaseIntent {
        source: LeaseSource::Manual,
        provider_mode: ProviderMode::One,
        providers: vec!["noop".into()],
        policy_id: "one".into(),
        content_mode: ContentMode::Object,
        duration: LeaseDuration::parse("1h").unwrap(),
    };
    let rows = cids
        .iter()
        .map(|key| {
            (
                key.clone(),
                PinTargetSpec {
                    cid: key.clone(),
                    logical_size: 1,
                },
                intent.clone(),
            )
        })
        .collect::<Vec<_>>();
    let tx = store.db().begin().await.unwrap();
    resources::attach_many(&tx, &rows, coordinator.provider_limits(), Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();

    (store, coordinator)
}

#[tokio::test]
async fn provider_ack_is_observed_even_if_database_settlement_fails() {
    let (store, coordinator) = noop_backlog(&[cid(1)]).await;
    let now = Utc::now();
    let claim = resources::claim(
        store.db(),
        "noop",
        &coordinator.provider_limits()["noop"],
        now,
        now + TimeDelta::minutes(5),
    )
    .await
    .unwrap()
    .unwrap();
    store.db().execute_unprepared("CREATE TRIGGER reject_settlement BEFORE UPDATE OF handle ON pin_resources BEGIN SELECT RAISE(ABORT,'writeback failed'); END").await.unwrap();
    let metrics = Arc::new(Metrics::new(coordinator.effective_config()));
    perform(
        store,
        coordinator.provider("noop").unwrap(),
        claim,
        Some(metrics.pin_for("noop")),
    )
    .await;
    let text = metrics.render();
    assert!(text.contains(
        "ipfs_s3_pin_provider_duration_seconds_count{provider=\"noop\",action=\"submit\",outcome=\"submitted\"} 1\n"
    ));
    assert!(text.contains(
        "ipfs_s3_pin_settlement_duration_seconds_count{provider=\"noop\",action=\"submit\",outcome=\"error\"} 1\n"
    ));
}

#[tokio::test]
async fn ack_without_demand_wakes_cleanup_then_refills_the_freed_quota() {
    let cids = (0..11).map(cid).collect::<Vec<_>>();
    let (store, coordinator) = noop_backlog(&cids).await;
    let (released, waiting) = (&cids[0], &cids[1]);
    store.db().execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE pin_resources SET status='accepted',action=NULL,handle=cid WHERE cid NOT IN (?,?)", [released.clone().into(), waiting.clone().into()])).await.unwrap();
    store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE pin_resources SET next_at=next_at+1 WHERE cid=?",
            [waiting.clone().into()],
        ))
        .await
        .unwrap();
    // Remove demand just after reservation, without a process-local notification.
    store
        .db()
        .execute_unprepared(&format!(
            "CREATE TRIGGER lose_demand AFTER UPDATE OF claim_token ON pin_resources
        WHEN NEW.cid='{released}' AND NEW.action='submit' AND NEW.claim_token IS NOT NULL
        BEGIN DELETE FROM pin_refs WHERE cid=NEW.cid; END"
        ))
        .await
        .unwrap();
    let worker = coordinator.start(store.clone(), CancellationToken::new());
    wait_status(&store, released, "cleaned").await;
    wait_status(&store, waiting, "accepted").await;
    worker.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn repeated_work_hints_do_not_starve_a_fitting_candidate_after_an_oversized_page() {
    let last = resources::MAX_CANDIDATES as u8 + 1;
    let cids = (0..=last).map(cid).collect::<Vec<_>>();
    let (store, coordinator) = noop_backlog(&cids).await;
    let fit = &cids[usize::from(last)];
    store
        .db()
        .execute_unprepared("UPDATE pin_resources SET logical_size=2")
        .await
        .unwrap();
    store.db().execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE pin_resources SET logical_size=999,status='accepted',handle='resident',action=NULL WHERE cid=?", [cids[0].clone().into()])).await.unwrap();
    store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE pin_resources SET logical_size=1,next_at=next_at+1 WHERE cid=?",
            [fit.clone().into()],
        ))
        .await
        .unwrap();

    let notifier = coordinator.clone();
    let mut db = store.db().clone();
    // Deliver a publication hint even during each page read and each reservation.
    db.set_metric_callback(move |_| notifier.notify_pin_work());
    let worker = coordinator.start(Store::new(db), CancellationToken::new());
    wait_status(&store, fit, "accepted").await;
    worker.shutdown(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn unrestricted_backlog_reserves_multiple_resources_in_one_write_up_to_free_slots() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cids: Vec<_> = (1..=7).map(cid).collect();
    let (store, coordinator) = noop_backlog(&cids).await;
    let largest_write = Arc::new(AtomicUsize::new(0));
    let observed = largest_write.clone();
    let keys = cids.clone();
    let mut db = store.db().clone();
    db.set_metric_callback(move |info| {
        if info.statement.sql.to_ascii_uppercase().contains("UPDATE")
            && let Some(values) = &info.statement.values
        {
            let count = keys
                .iter()
                .filter(|cid| values.0.contains(&sea_orm::Value::from(cid.as_str())))
                .count();
            observed.fetch_max(count, Ordering::Relaxed);
        }
    });
    let worker = coordinator.start(Store::new(db), CancellationToken::new());
    for key in &cids {
        wait_status(&store, key, "accepted").await;
    }
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(largest_write.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn canonical_resource_uploads_with_original_cidv0_unixfs_profile() {
    use wiremock::matchers::query_param;
    const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let canonical = crate::pinning::provider::canonical_cid(V0).unwrap();
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Hash": V0, "Type": "file"})))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"stored file"))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .and(query_param("cid-version", "0"))
        .and(query_param("raw-leaves", "false"))
        .and(query_param("pin", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{V0}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let provider = IpfsRpcProvider::new(
        "rpc".into(),
        target.uri(),
        crate::kubo::KuboClient::new(source.uri()),
        RpcProfile::Kubo,
        RpcStrategy::Upload,
        None,
    )
    .unwrap();
    let store = Store::open(Database::connect("sqlite::memory:").await.unwrap())
        .await
        .unwrap();
    let limits = ProviderLimits {
        priority: 1,
        max_pins: 1,
        max_bytes: 100,
        enabled: true,
    };
    let tx = store.db().begin().await.unwrap();
    resources::attach(
        &tx,
        "file",
        &PinTargetSpec {
            cid: V0.into(),
            logical_size: 11,
        },
        &LeaseIntent {
            source: LeaseSource::Manual,
            provider_mode: ProviderMode::One,
            providers: vec!["rpc".into()],
            policy_id: "one".into(),
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        },
        &BTreeMap::from([("rpc".into(), limits.clone())]),
        Utc::now(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let stop = CancellationToken::new();
    let worker_store = store.clone();
    let worker_stop = stop.clone();
    let (_wake, receiver) = tokio::sync::watch::channel(());
    let worker = tokio::spawn(async move {
        dispatch::run(
            worker_store,
            vec![dispatch::Lane::new(
                Arc::new(provider),
                Duration::ZERO,
                None,
            )],
            &BTreeMap::from([("rpc".to_owned(), limits)]),
            1,
            Duration::from_secs(60),
            receiver,
            worker_stop,
        )
        .await;
    });
    wait_status(&store, &canonical, "accepted").await;
    stop.cancel();
    worker.await.unwrap();
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .body
            .windows(11)
            .any(|part| part == b"stored file")
    );
}

#[tokio::test]
async fn queued_ack_and_exact_delete_complete_while_another_post_is_stalled() {
    let server = MockServer::start().await;
    let raw: PinningConfig = toml::from_str(&format!(
        r#"
        worker_interval = "60s"
        worker_concurrency = 2
        [[providers]]
        name = "backup"
        kind = "filebase"
        endpoint = "{}"
        token_env = "BACKUP_TOKEN"
        requests_per_second = 2
        priority = 1
        max_bytes = 1000
        max_pins = 8
    "#,
        server.uri()
    ))
    .unwrap();
    let config = ValidatedPinningConfig::from_raw(&raw, |_| Some("test-token".into())).unwrap();
    let coordinator = PinningCoordinator::build(config).unwrap();
    let store = Store::open(Database::connect("sqlite::memory:").await.unwrap())
        .await
        .unwrap();
    let slow = cid(1);
    let good = cid(2);
    let unknown = cid(3);
    let (posts, mut arrivals) = tokio::sync::mpsc::unbounded_channel();
    for (key, response) in [
        (
            &slow,
            ResponseTemplate::new(202).set_delay(Duration::from_secs(60)),
        ),
        (
            &good,
            ResponseTemplate::new(202).set_body_json(
                json!({"requestid":"request-42", "status":"queued", "pin":{"cid": good}}),
            ),
        ),
        (&unknown, ResponseTemplate::new(503)),
    ] {
        let posts = posts.clone();
        let cid = key.clone();
        Mock::given(method("POST"))
            .and(path("/pins"))
            .and(body_partial_json(json!({"cid":key})))
            .respond_with(move |_: &wiremock::Request| {
                posts.send((cid.clone(), Instant::now())).unwrap();
                response.clone()
            })
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("DELETE"))
        .and(path("/pins/request-42"))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    let intent = LeaseIntent {
        source: LeaseSource::Manual,
        provider_mode: ProviderMode::One,
        providers: vec!["backup".into()],
        policy_id: "policy".into(),
        content_mode: ContentMode::Object,
        duration: LeaseDuration::parse("1h").unwrap(),
    };
    let eligible_at = Utc::now() - TimeDelta::seconds(1);
    let attach = async |key: &str| {
        let tx = store.db().begin().await.unwrap();
        resources::attach(
            &tx,
            key,
            &PinTargetSpec {
                cid: key.into(),
                logical_size: 1,
            },
            &intent,
            coordinator.provider_limits(),
            eligible_at,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    };
    attach(&slow).await;
    attach(&good).await;
    store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE pin_resources SET next_at=next_at+1 WHERE cid=?",
            [good.clone().into()],
        ))
        .await
        .unwrap();
    let metrics = Arc::new(Metrics::new(coordinator.effective_config()));
    let worker = coordinator.start_with_metrics(
        store.clone(),
        CancellationToken::new(),
        Some(metrics.clone()),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.0, slow);
    // The later insertion sorts before `good` in a fresh scan. The prefetched tail
    // still runs first, and holds no token while the provider is rate-limited.
    let waiting = store
        .db()
        .query_one(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT claim_token FROM pin_resources WHERE cid=?",
            [good.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        waiting
            .try_get::<Option<String>>("", "claim_token")
            .unwrap(),
        None
    );
    attach(&unknown).await;
    coordinator.notify_pin_work();
    let second = tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
        .await
        .unwrap()
        .unwrap();
    let third = tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((&second.0, &third.0), (&good, &unknown));
    assert!(second.1 - first.1 >= Duration::from_millis(400));
    assert!(third.1 - second.1 >= Duration::from_millis(400));
    wait_status(&store, &good, "accepted").await;
    wait_status(&store, &unknown, "unknown").await;
    let tx = store.db().begin().await.unwrap();
    resources::release_owner(&tx, &good, Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        3,
        "idle worker rescanned before its interval or a work hint"
    );
    coordinator.notify_pin_work();
    wait_status(&store, &good, "cleaned").await;
    tokio::time::timeout(
        Duration::from_secs(2),
        worker.shutdown(Duration::from_millis(30)),
    )
    .await
    .unwrap();
    server.verify().await;
    let text = metrics.render();
    for sample in [
        "ipfs_s3_pin_dispatches_total{provider=\"backup\",action=\"submit\"} 3",
        "ipfs_s3_pin_queue_wait_seconds_count{provider=\"backup\",action=\"submit\"} 3",
        "ipfs_s3_pin_queue_wait_seconds_count{provider=\"backup\",action=\"delete\"} 1",
        "ipfs_s3_pin_provider_duration_seconds_count{provider=\"backup\",action=\"submit\",outcome=\"submitted\"} 1",
        "ipfs_s3_pin_provider_duration_seconds_count{provider=\"backup\",action=\"submit\",outcome=\"unknown\"} 1",
        "ipfs_s3_pin_provider_duration_seconds_count{provider=\"backup\",action=\"delete\",outcome=\"deleted\"} 1",
        "ipfs_s3_pin_settlement_duration_seconds_count{provider=\"backup\",action=\"submit\",outcome=\"applied\"} 2",
    ] {
        assert!(text.lines().any(|line| line == sample), "{text}");
    }
    let wait = text
        .lines()
        .find_map(|line| {
            line.strip_prefix(
                "ipfs_s3_pin_queue_wait_seconds_sum{provider=\"backup\",action=\"submit\"} ",
            )
        })
        .unwrap()
        .parse::<f64>()
        .unwrap();
    assert!(wait >= 3.0, "{text}");
    assert!(!text.contains(&good) && !text.contains("request-42"));
}
