use std::{
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use super::common::{
    assert_no_kubo_pin_removes, assert_put_cid_headers, assert_signed_body, signed_copy,
    signed_delete_object_tagging, signed_get_object_tagging, signed_put_object_tagging,
    signed_put_with_tagging, tagging_pairs,
};
use crate::support::{
    decompress::AddReply,
    pin_rows,
    pinning::{PinningHarnessConfig, TEST_CID, start_pinning_harness},
};
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::pinning::provider::canonical_cid;
use sea_orm::ConnectionTrait;
use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{Layer, filter::filter_fn, layer::SubscriberExt};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{body_partial_json, header, method, path},
};

struct PinIdle(ThreadId, mpsc::UnboundedSender<()>);

impl<S: tracing::Subscriber> Layer<S> for PinIdle {
    fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if thread::current().id() == self.0 {
            let _ = self.1.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn signed_put_wakes_idle_pin_worker_without_bypassing_provider_rps() {
    const IDLE: Duration = Duration::from_secs(6);
    const SLOW: Duration = Duration::from_millis(400);
    const SECOND_CID: &str = "bafkqabcqov2fk";
    let (idle_tx, mut idle_rx) = mpsc::unbounded_channel();
    let trace_layer = PinIdle(thread::current().id(), idle_tx)
        .with_filter(filter_fn(|meta| meta.name() == "pin_worker_idle"));
    // Global callsite interest keeps sibling tests from suppressing event capture.
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(trace_layer))
        .unwrap();
    let (posts, mut arrivals) = mpsc::unbounded_channel();
    let mut config = PinningHarnessConfig::request_one();
    config.worker_interval = IDLE;
    config.requests_per_second = 1;
    config
        .kubo_script
        .add_replies
        .push(AddReply::Ok(SECOND_CID));
    config.pinata_script = [(TEST_CID, "first", SLOW), (SECOND_CID, "second", Duration::ZERO)]
        .into_iter()
        .map(|(cid, handle, delay)| {
            let posts = posts.clone();
            Mock::given(method("POST"))
                .and(path("/psa/pins"))
                .and(body_partial_json(serde_json::json!({"cid": cid})))
                .respond_with(move |_: &wiremock::Request| {
                    posts.send((cid, Instant::now())).unwrap();
                    ResponseTemplate::new(202).set_delay(delay).set_body_json(
                        serde_json::json!({"requestid": handle, "status": "queued", "pin": {"cid": cid}}),
                    )
                })
                .expect(1)
        })
        .collect();
    let harness = start_pinning_harness(config).await;
    let worker = harness
        .state
        .pinning
        .start(harness.state.store.clone(), CancellationToken::new());
    timeout(Duration::from_secs(3), idle_rx.recv())
        .await
        .expect("worker never completed an empty scan")
        .unwrap();

    let put_pin = async |key: &str, cid: &str| {
        let put =
            signed_put_with_tagging(&harness, key, key.as_bytes().to_vec(), "ipfs-s3%3Apin=true")
                .await;
        assert_eq!(put.status(), StatusCode::OK);
        assert_put_cid_headers(&put, cid);
    };
    let put_started = Instant::now();
    put_pin("first", TEST_CID).await;
    let (first_cid, first_post) = timeout(IDLE + Duration::from_secs(3), arrivals.recv())
        .await
        .expect("worker did not send the first provider POST")
        .unwrap();
    assert_eq!(first_cid, TEST_CID);
    let put_to_post = first_post.duration_since(put_started);

    put_pin("second", SECOND_CID).await;
    let (second_cid, second_post) = timeout(Duration::from_secs(3), arrivals.recv())
        .await
        .expect("worker did not send the second provider POST")
        .unwrap();
    assert_eq!(second_cid, SECOND_CID);
    let inter_post = second_post.duration_since(first_post);
    let db = harness.state.store.db();
    let rows = timeout(Duration::from_secs(3), async {
        loop {
            let rows = pin_rows::resources(db).await;
            if rows.len() == 2 && rows.iter().all(|row| row.status == "accepted") {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both POST acknowledgements should settle as accepted");
    let accepted = |cid, handle| {
        rows.iter().any(|row| {
            row.cid == canonical_cid(cid).unwrap() && row.handle.as_deref() == Some(handle)
        })
    };
    assert!(accepted(TEST_CID, "first"));
    assert!(accepted(SECOND_CID, "second"));
    worker.shutdown(Duration::from_secs(2)).await;
    assert_eq!(harness.pinata.received_requests().await.unwrap().len(), 2);
    harness.shutdown().await;

    assert!(
        put_to_post < IDLE / 2,
        "already-idle worker waited {put_to_post:?} for the first POST (idle {IDLE:?})"
    );
    assert!(
        inter_post >= Duration::from_millis(850),
        "POST gap {inter_post:?} bypassed 1 RPS"
    );
}

#[tokio::test]
async fn signed_publication_copy_renewal_and_final_expiry_use_the_accepted_handle() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script.push(
        Mock::given(method("DELETE"))
            .and(path("/psa/pins/request-1"))
            .and(header("Authorization", "Bearer pinning-test-token"))
            .respond_with(ResponseTemplate::new(202))
            .expect(1),
    );
    let harness = start_pinning_harness(config).await;
    let put = signed_put_with_tagging(
        &harness,
        "source.txt",
        b"body".to_vec(),
        "team=storage&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, TEST_CID);
    let copy = signed_copy(&harness, "source.txt", "copy.txt", HeaderMap::new()).await;
    assert_eq!(copy.status(), StatusCode::OK);
    let tags = signed_get_object_tagging(&harness, "copy.txt").await;
    assert_eq!(tags.status(), StatusCode::OK);
    assert_eq!(
        tagging_pairs(&tags.text().await.unwrap()),
        [
            ("ipfs-s3:pin".into(), "true".into()),
            ("team".into(), "storage".into())
        ]
    );
    let db = harness.state.store.db();
    let refs = pin_rows::refs(db).await;
    assert_eq!(refs.len(), 2);
    assert_ne!(refs[0].owner_object_id, refs[1].owner_object_id);
    let cid = canonical_cid(TEST_CID).unwrap();
    assert!(refs.iter().all(|r| r.cid == cid && r.source == "manual"));
    assert!(harness.pinata.received_requests().await.unwrap().is_empty());
    harness.run_until_resource_status("accepted").await;
    let resource = pin_rows::resources(db).await.remove(0);
    assert_eq!(
        (
            resource.cid,
            resource.logical_size,
            resource.handle.as_deref()
        ),
        (cid, 4, Some("request-1"))
    );
    let retain_until = (chrono::Utc::now() + chrono::Duration::hours(2)).timestamp_micros();
    let retain_tag = chrono::DateTime::from_timestamp_micros(retain_until)
        .unwrap()
        .to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "copy.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", &retain_tag),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    let renewed = pin_rows::refs(db).await;
    assert_eq!(renewed.len(), 2);
    assert!(renewed.iter().any(|r| r.expires_at == retain_until));
    assert_eq!(
        signed_delete_object_tagging(&harness, "source.txt")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(pin_rows::refs(db).await.len(), 1);
    assert_eq!(pin_rows::resources(db).await[0].action, None);
    assert_eq!(
        db.execute_unprepared("UPDATE pin_refs SET expires_at=1")
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    harness.run_until_resource_status("cleaned").await;
    let cleaned = pin_rows::resources(db).await.remove(0);
    assert_eq!((cleaned.handle, cleaned.action), (None, None));
    assert!(pin_rows::refs(db).await.is_empty());
    let calls = harness.pinata.received_requests().await.unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|r| (r.method.as_str(), r.url.path()))
            .collect::<Vec<_>>(),
        [("POST", "/psa/pins"), ("DELETE", "/psa/pins/request-1")]
    );
    assert_signed_body(&harness, "copy.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}
