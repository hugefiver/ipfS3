#[allow(dead_code)]
#[path = "support/cors.rs"]
mod cors;
#[allow(dead_code)]
#[path = "support/sigv4.rs"]
mod sigv4;

use http::{HeaderMap, HeaderValue, Method, StatusCode};
use sea_orm::ConnectionTrait;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

struct ReleaseOnDrop(Option<mpsc::Sender<()>>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_get_head_copy_release_snapshot_before_network_and_survive_source_delete() {
    for operation in [Method::GET, Method::HEAD, Method::PUT] {
        let harness = cors::start_harness().await;
        harness.seed_plain_object("source", CID, b"old").await;
        // A verified hot owner forces the runtime node check, including HEAD.
        harness.state.store.db().execute_unprepared(
            "UPDATE physical_residencies SET verification_state = 'verified', node_identity = 'node', verification_receipt = 'receipt', verified_at = CURRENT_TIMESTAMP"
        ).await.unwrap();
        let source = ipfs_s3_gateway::store::object_version::resolve_version(
            harness.state.store.db(),
            &harness.bucket,
            "source",
            &ipfs_s3_gateway::store::object_version::VersionSelector::Current,
        )
        .await
        .unwrap()
        .object
        .unwrap();
        ipfs_s3_gateway::store::pinning::tags::replace_object_tags(
            harness.state.store.db(),
            &source.id,
            &[ipfs_s3_gateway::pinning::tags::ObjectTag {
                key: "source-tag".into(),
                value: "old".into(),
            }],
        )
        .await
        .unwrap();
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&harness.kubo)
            .await;
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let gate = ReleaseOnDrop(Some(release_tx));
        let reached = Arc::new(Mutex::new(Some(reached_tx)));
        let release = Arc::new(Mutex::new(release_rx));
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(move |_: &wiremock::Request| {
                if let Some(sender) = reached.lock().unwrap().take() {
                    let _ = sender.send(());
                    release
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10))
                        .unwrap();
                }
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"node"}))
            })
            .mount(&harness.kubo)
            .await;
        let mut headers = HeaderMap::new();
        let key = if operation == Method::PUT {
            headers.insert(
                "x-amz-copy-source",
                HeaderValue::from_str(&format!("/{}/source", harness.bucket)).unwrap(),
            );
            "copy"
        } else {
            "source"
        };
        let endpoint = harness.endpoint.clone();
        let bucket = harness.bucket.clone();
        let request_method = operation.clone();
        let reader = tokio::spawn(async move {
            sigv4::send_sigv4(
                request_method,
                &endpoint,
                &bucket,
                key,
                &[],
                vec![],
                headers,
                "test",
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(3), reached_rx)
            .await
            .unwrap()
            .unwrap();
        // The harness's SQLite memory pool has one connection. A transaction
        // held across /id would prevent this signed DELETE from completing.
        let deleted = tokio::time::timeout(
            Duration::from_secs(3),
            sigv4::send_sigv4(
                Method::DELETE,
                &harness.endpoint,
                &harness.bucket,
                "source",
                &[],
                vec![],
                HeaderMap::new(),
                "test",
            ),
        )
        .await
        .expect("network wait must not retain the database transaction");
        assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
        drop(gate);
        let response = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        if operation == Method::GET {
            assert_eq!(response.bytes().await.unwrap().as_ref(), b"old");
        } else if operation == Method::HEAD {
            assert_eq!(response.headers()["etag"], format!("\"{CID}\""));
            assert_eq!(response.headers()["content-length"], "3");
        } else {
            let copy = ipfs_s3_gateway::store::object_version::read_snapshot(
                harness.state.store.db(),
                &harness.bucket,
                "copy",
                &ipfs_s3_gateway::store::object_version::VersionSelector::Current,
            )
            .await
            .unwrap();
            assert_eq!(copy.version.object.unwrap().cid, CID);
            assert_eq!(
                copy.tags,
                vec![ipfs_s3_gateway::pinning::tags::ObjectTag {
                    key: "source-tag".into(),
                    value: "old".into()
                }]
            );
        }
    }
}

#[tokio::test]
async fn signed_reads_fail_closed_on_real_residency_corruption_without_network() {
    let harness = cors::start_harness().await;
    harness.seed_plain_object("source", CID, b"old").await;
    harness
        .state
        .store
        .db()
        .execute_unprepared("DELETE FROM version_residencies")
        .await
        .unwrap();
    for operation in [Method::GET, Method::HEAD, Method::PUT] {
        let mut headers = HeaderMap::new();
        let key = if operation == Method::PUT {
            headers.insert(
                "x-amz-copy-source",
                HeaderValue::from_str(&format!("/{}/source", harness.bucket)).unwrap(),
            );
            "copy"
        } else {
            "source"
        };
        let response = sigv4::send_sigv4(
            operation,
            &harness.endpoint,
            &harness.bucket,
            key,
            &[],
            vec![],
            headers,
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
    assert_eq!(harness.kubo_request_count().await, 0);
}
