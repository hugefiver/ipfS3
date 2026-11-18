use super::*;
use axum::{Json, Router, extract::Query, routing::post};
use tokio::sync::{mpsc, oneshot};

async fn wait_pinned(fixture: &RpcFixture, cid: &str) -> pin_lease_target::Model {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let target = pin_lease_target::Entity::find()
                .filter(pin_lease_target::Column::Cid.eq(cid))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap();
            if target.state == "pinned" {
                return target;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("worker did not settle {cid}"))
}

#[tokio::test]
async fn worker_dispatches_cids_and_unpins_while_first_pin_add_receipt_is_held() {
    let source = MockServer::start().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (adds, mut pin_add) = mpsc::unbounded_channel();
    let (removals, mut pin_rm) = mpsc::unbounded_channel();
    let target = Router::new()
        .route(
            "/api/v0/pin/add",
            post(move |Query(query): Query<BTreeMap<String, String>>| {
                let adds = adds.clone();
                async move {
                    let cid = &query["arg"];
                    let (release, receipt) = oneshot::channel::<()>();
                    adds.send((cid.clone(), release)).unwrap();
                    receipt.await.unwrap();
                    Json(serde_json::json!({"Pins": [cid]}))
                }
            }),
        )
        .route(
            "/api/v0/pin/rm",
            post(move |Query(query): Query<BTreeMap<String, String>>| {
                let cid = query["arg"].clone();
                removals.send(cid.clone()).unwrap();
                async move { Json(serde_json::json!({"Pins": [cid]})) }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, target).await.unwrap() });
    let mut raw = rpc_config(&endpoint, &source.uri(), RpcProfile::Kubo, RpcStrategy::Cid);
    raw.pinning.worker_interval = "1s".into();
    raw.pinning.worker_concurrency = 1;
    raw.pinning.providers[0].max_pins = 2;
    raw.pinning.providers[0].max_bytes = 200;
    raw.pinning_rpc.providers[0].control_timeout_seconds = Some(60);
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let fixture = fixture_on_database(raw, db).await;
    fixture.publish("second", OTHER).await;

    let worker = start(
        fixture.coordinator.clone(),
        fixture.store.clone(),
        CancellationToken::new(),
    );
    let (first, release_first) = tokio::time::timeout(Duration::from_secs(10), pin_add.recv())
        .await
        .expect("first CID did not reach pin/add")
        .unwrap();
    let (second, release_second) = tokio::time::timeout(Duration::from_secs(10), pin_add.recv())
        .await
        .expect("second CID could not dispatch while the first receipt was held")
        .unwrap();
    assert_eq!(
        BTreeSet::from([first.as_str(), second.as_str()]),
        BTreeSet::from([CID, OTHER])
    );
    release_second.send(()).unwrap();
    let pinned = wait_pinned(&fixture, &second).await;
    let txn = fixture.store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &pinned.lease_id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let removed = tokio::time::timeout(Duration::from_secs(10), pin_rm.recv())
        .await
        .expect("same-provider unpin was blocked by the first receipt")
        .unwrap();
    assert_eq!(removed, second);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let resource = ledger::get(fixture.store.db(), &fixture.provider, &second)
                .await
                .unwrap()
                .unwrap();
            if resource.effect == "absent" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("unpin did not release the second CID");
    fixture.assert_held().await;

    let receipts = fixture.evidence().await;
    let pending = receipts
        .iter()
        .find(|row| row.expected_cid == first)
        .unwrap();
    assert_eq!(pending.outcome, "in_flight");
    assert!(pending.observed_at.is_none());
    let waiting = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Cid.eq(&first))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(waiting.state, "pinned");

    release_first.send(()).unwrap();
    wait_pinned(&fixture, &first).await;
    worker.shutdown(Duration::from_secs(5)).await;
    let receipts = fixture.evidence().await;
    assert_eq!(receipts.len(), 2);
    for cid in [first, second] {
        let settled = receipts.iter().find(|row| row.expected_cid == cid).unwrap();
        assert_eq!(settled.outcome, "matched");
        assert!(settled.observed_at.is_some());
        assert!(!settled.needs_attention);
    }
    assert!(pin_add.try_recv().is_err(), "duplicate pin/add POST");
    assert!(pin_rm.try_recv().is_err(), "duplicate pin/rm POST");
    server.abort();
}
