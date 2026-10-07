//! Source interruption/trailer contracts over real local HTTP framing. Not N/N.
use crate::{car, cluster, pinning};
use axum::{Router, body::Body, extract::Request, response::Response, routing::post};
use bytes::Bytes;
use cluster::provider::{
    ClusterProtocol, ClusterProvider, ClusterProviderOptions, ClusterSource, ClusterStrategy,
    ClusterWriteEffect,
};
use cluster::{ClusterPinset, ReplicaOutcome};
use http_body_util::{BodyExt, StreamBody};
use pinning::{identity::Ownership, provider::SubmitPin};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

struct Server {
    endpoint: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(router: Router) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server { endpoint, task }
}

#[tokio::test]
async fn cluster_provider_real_contract_source_interruption_or_error_trailer_never_completes() {
    let data = vec![0x51; 128 * 1024];
    let root = car::raw_cid(&data, 0x55);
    let valid = car::encode(std::slice::from_ref(&root), &[(root.clone(), data)]);
    for protocol in [ClusterProtocol::Proxy, ClusterProtocol::Rest] {
        for disconnect in [true, false] {
            let dispatched = Arc::new(Notify::new());
            let writes = Arc::new(AtomicUsize::new(0));
            let reads = Arc::new(AtomicUsize::new(0));
            let source = serve(Router::new().route("/api/v0/dag/export", post({
                let dispatched = dispatched.clone();
                let valid = valid.clone();
                let reads = reads.clone();
                move |request: Request| {
                    let dispatched = dispatched.clone(); let valid = valid.clone(); let reads = reads.clone();
                    async move {
                        reads.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(request.headers().get("te").unwrap(), "trailers");
                        let stream = async_stream::stream! {
                            yield Ok::<_, std::io::Error>(hyper::body::Frame::data(Bytes::copy_from_slice(&valid[..4096])));
                            // Synchronization, not sleep: prove an actual target
                            // POST before interrupting the source response body.
                            dispatched.notified().await;
                            if disconnect {
                                yield Err(std::io::Error::other("owned fixture source disconnected"));
                            } else {
                                yield Ok(hyper::body::Frame::data(Bytes::copy_from_slice(&valid[4096..])));
                                let mut trailers = http::HeaderMap::new();
                                trailers.insert("x-stream-error", http::HeaderValue::from_static("source export failed"));
                                yield Ok(hyper::body::Frame::trailers(trailers));
                            }
                        };
                        Response::builder().status(200).header("Trailer", "X-Stream-Error")
                            .body(Body::new(StreamBody::new(stream))).unwrap()
                    }
                }
            }))).await;
            let target = serve(Router::new().fallback({
                let writes = writes.clone();
                let dispatched = dispatched.clone();
                let root = root.clone();
                move |mut request: Request| {
                    let writes = writes.clone();
                    let dispatched = dispatched.clone();
                    let root = root.clone();
                    async move {
                        writes.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(
                            request.method(),
                            http::Method::POST,
                            "no Poll after source failure"
                        );
                        assert_eq!(
                            request.uri().path(),
                            if protocol == ClusterProtocol::Proxy {
                                "/api/v0/add"
                            } else {
                                "/add"
                            }
                        );
                        dispatched.notify_one();
                        while let Some(frame) = request.body_mut().frame().await {
                            if frame.is_err() {
                                return Response::builder()
                                    .status(400)
                                    .body(Body::empty())
                                    .unwrap();
                            }
                        }
                        let output = match protocol {
                            ClusterProtocol::Proxy => json!({"Name":"content.car", "Hash":root}),
                            ClusterProtocol::Rest => json!({"name":"content.car", "cid":root}),
                        };
                        Response::builder()
                            .status(200)
                            .header("Content-Type", "application/json")
                            .header("X-Chunked-Output", "1")
                            .body(Body::from(format!("{output}\n")))
                            .unwrap()
                    }
                }
            }))
            .await;
            let provider = ClusterProvider::new(ClusterProviderOptions {
                protocol,
                strategy: ClusterStrategy::Car,
                source: Some(ClusterSource {
                    endpoint: source.endpoint.clone(),
                    auth: None,
                }),
                proxy_endpoint: (protocol == ClusterProtocol::Proxy)
                    .then(|| target.endpoint.clone()),
                rest_endpoint: target.endpoint.clone(),
                allow_private_network: true,
                pinset: ClusterPinset::new("qa-fault", "one-pinset").unwrap(),
                ..ClusterProviderOptions::default()
            })
            .unwrap();
            let out = tokio::time::timeout(
                Duration::from_secs(10),
                provider.submit_observed(SubmitPin {
                    cid: root.clone(),
                    name: "not-sent".into(),
                    metadata: BTreeMap::new(),
                }),
            )
            .await
            .expect("source fault contract deadline");
            assert!(out.result.is_err(), "source fault was accepted: {out:?}");
            assert_eq!(out.effect, ClusterWriteEffect::Unknown);
            assert_eq!(out.replication_outcome(), ReplicaOutcome::Unknown);
            assert!(out.replicas.is_none());
            assert!(out.resources.iter().any(|r| r.resource.cid == root));
            assert!(
                out.resources
                    .iter()
                    .all(|r| r.ownership == Ownership::Unknown)
            );
            assert_eq!(reads.load(Ordering::SeqCst), 1);
            assert_eq!(
                writes.load(Ordering::SeqCst),
                1,
                "no repost/fallback/cleanup"
            );
        }
    }
}
