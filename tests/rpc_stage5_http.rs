//! Stage 5: signed S3 publication -> registered RPC worker -> stored-byte HTTP
//! transfer -> independent submission ledger. These are protocol mocks, not real
//! Kubo DAG/replica or provider-account evidence. ZIP directory building is off.

// Import helpers only, not support/mod.rs and its unrelated selftests.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        identity::{CleanupMode, Ownership, ProviderRouteSnapshot, RemoteResourceType},
        provider::{ObservedResourceStatus, canonical_resource_cid},
    },
    state::AppState,
    store::{
        self,
        entities::{object, object_version, pin_job, pin_lease, pin_lease_target, remote_pin},
        object_version::BucketVersioningState,
        pinning::{ledger, quota},
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, Request, ResponseTemplate, matchers::method};

const BUCKET: &str = "stage5-private-bucket";
const CUSTOMER_KEY: [u8; 32] = [0x6d; 32];
const MAX_FIXTURE_BYTES: usize = 4096;

#[derive(Clone, Debug)]
struct AddedFile {
    hash: String,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct NodeState {
    files: BTreeMap<String, Vec<u8>>,
    adds: Vec<AddedFile>,
    pins: BTreeSet<String>,
}

struct HttpNode {
    server: MockServer,
    data: Arc<Mutex<NodeState>>,
}

fn query(request: &Request) -> BTreeMap<String, String> {
    let pairs: Vec<_> = request.url.query_pairs().collect();
    let map: BTreeMap<_, _> = pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    assert_eq!(pairs.len(), map.len(), "duplicate RPC query parameters");
    map
}

fn assert_query(actual: &BTreeMap<String, String>, expected: &[(&str, &str)]) {
    assert_eq!(
        actual,
        &expected
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>()
    );
}

fn raw_cid(bytes: &[u8]) -> cid::Cid {
    assert!(bytes.len() <= MAX_FIXTURE_BYTES);
    let digest = Sha256::digest(bytes);
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap();
    cid::Cid::new_v1(0x55, hash)
}

/// Bounded single-file parser for the actual streamed multipart request. Neither
/// source nor target is handed a scripted payload or scripted Hash response.
fn multipart_file(request: &Request) -> Vec<u8> {
    assert!(request.body.len() <= MAX_FIXTURE_BYTES + 1024);
    assert_eq!(request.headers["transfer-encoding"], "chunked");
    assert!(!request.headers.contains_key("content-length"));
    let content_type = request.headers["content-type"].to_str().unwrap();
    let boundary = content_type
        .strip_prefix("multipart/form-data; boundary=")
        .expect("multipart content type");
    let start = format!("--{boundary}\r\n");
    let end = format!("\r\n--{boundary}--\r\n");
    assert!(request.body.starts_with(start.as_bytes()));
    assert!(request.body.ends_with(end.as_bytes()));
    let separator = request
        .body
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("multipart file headers end");
    let headers = std::str::from_utf8(&request.body[start.len()..separator]).unwrap();
    assert!(headers.contains("name=\"file\"; filename=\"object\""));
    assert!(!headers.contains(BUCKET));
    assert!(!headers.contains("private/"));
    assert!(!headers.contains(&STANDARD.encode(CUSTOMER_KEY)));
    let bytes = request.body[separator + 4..request.body.len() - end.len()].to_vec();
    assert!(bytes.len() <= MAX_FIXTURE_BYTES);
    assert!(
        !bytes
            .windows(boundary.len())
            .any(|bytes| bytes == boundary.as_bytes()),
        "fixture must contain exactly one file part"
    );
    bytes
}

impl HttpNode {
    async fn start(target: bool) -> Self {
        let server = MockServer::start().await;
        let data = Arc::new(Mutex::new(NodeState::default()));
        Mock::given(method("POST"))
            .respond_with({
                let data = data.clone();
                move |request: &Request| {
                    // Target credentials must not be inherited from SigV4/SSE-C.
                    assert!(!request.headers.contains_key("authorization"));
                    assert!(!request.headers.keys().any(|name| name.as_str().starts_with("x-amz-")));
                    let args = query(request);
                    let mut node = data.lock().unwrap();
                    let json = match request.url.path() {
                        "/api/v0/add" => {
                            let mut expected = vec![
                                ("cid-version", "1"),
                                ("pin", "false"),
                                ("wrap-with-directory", "false"),
                                ("progress", "true"),
                            ];
                            if target {
                                expected.extend([
                                    ("raw-leaves", "true"),
                                    ("chunker", "size-262144"),
                                    ("hash", "sha2-256"),
                                ]);
                            }
                            assert_query(&args, &expected);
                            let bytes = multipart_file(request);
                            let cid = raw_cid(&bytes);
                            // Exercise allocation normalization without rewriting
                            // the source object's public ETag/CID representation.
                            let hash = if !target && node.adds.len() % 2 == 0 {
                                cid.to_string_of_base(cid::multibase::Base::Base58Btc).unwrap()
                            } else {
                                cid.to_string()
                            };
                            node.files.insert(cid.to_string(), bytes.clone());
                            node.adds.push(AddedFile { hash: hash.clone(), bytes: bytes.clone() });
                            serde_json::json!({"Name":"object", "Hash":hash, "Size":bytes.len().to_string()})
                        }
                        "/api/v0/cat" => {
                            assert!(!target, "upload verification must not fetch target plaintext");
                            let cid = canonical_resource_cid(&args["arg"]).unwrap();
                            assert_query(&args, &[("arg", &args["arg"])]);
                            let bytes = node.files.get(&cid).expect("cat must read a real prior add");
                            return ResponseTemplate::new(200).set_body_bytes(bytes.clone());
                        }
                        "/api/v0/pin/add" => {
                            let cid = canonical_resource_cid(&args["arg"]).unwrap();
                            let mut expected = vec![("arg", args["arg"].as_str())];
                            if target {
                                expected.extend([("recursive", "true"), ("progress", "false")]);
                            }
                            assert_query(&args, &expected);
                            assert!(node.files.contains_key(&cid), "cannot pin bytes never uploaded");
                            node.pins.insert(cid.clone());
                            serde_json::json!({"Pins":[cid]})
                        }
                        "/api/v0/pin/ls" => {
                            assert!(target);
                            let cid = canonical_resource_cid(&args["arg"]).unwrap();
                            assert_query(&args, &[
                                ("arg", cid.as_str()), ("stream", "false"), ("names", "false"),
                                ("type", "all"), ("offline", "true"),
                            ]);
                            assert!(node.pins.contains(&cid), "no invented recursive evidence");
                            serde_json::json!({"Keys":{(cid):{"Type":"recursive"}}})
                        }
                        "/api/v0/files/stat" => {
                            let path = &args["arg"];
                            let cid = canonical_resource_cid(path.strip_prefix("/ipfs/").unwrap()).unwrap();
                            let mut expected = vec![("arg", path.as_str())];
                            if target {
                                expected.extend([("with-local", "true"), ("offline", "true")]);
                                assert!(node.pins.contains(&cid));
                            }
                            assert_query(&args, &expected);
                            let bytes = node.files.get(&cid).expect("stat must inspect actual stored bytes");
                            assert_eq!(raw_cid(bytes).to_string(), cid);
                            serde_json::json!({"Hash":cid,"Type":"file","Size":bytes.len(),"WithLocality":true,"Local":true})
                        }
                        "/api/v0/id" => {
                            assert!(target);
                            assert_query(&args, &[("peerid-base", "b58mh")]);
                            serde_json::json!({"ID":"QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE"})
                        }
                        endpoint => panic!("unexpected RPC, including forbidden cleanup/root work: {endpoint}"),
                    };
                    ResponseTemplate::new(200).set_body_string(format!("{json}\n"))
                }
            })
            .mount(&server)
            .await;
        Self { server, data }
    }

    fn added(&self) -> Vec<AddedFile> {
        self.data.lock().unwrap().adds.clone()
    }
}

struct Harness {
    source: HttpNode,
    target: HttpNode,
    state: Arc<AppState>,
    gateway: support::cors::S3ServerHandle,
    provider: String,
}

impl Harness {
    async fn start() -> Self {
        let source = HttpNode::start(false).await;
        let target = HttpNode::start(true).await;
        let config: Config = toml::from_str(&format!(
            r#"
            [kubo]
            rpc_url = '{}'
            [crypto]
            master_key = '{}'
            [decompress_zip]
            unixfs_directory_root = false
            [pinning]
            worker_interval = '1s'
            worker_concurrency = 1
            [[pinning.providers]]
            name = 'backup'
            kind = 'ipfs_rpc'
            api = 'rpc'
            strategy = 'upload'
            endpoint = '{}'
            priority = 1
            max_bytes = 8192
            max_pins = 8
            requests_per_second = 100
            [[pinning.policies]]
            bucket = '{BUCKET}'
            prefix = ''
            trigger = 'request'
            provider_mode = 'all'
            providers = ['backup']
            default_duration = '1h'
            max_duration = '2h'
            allow_decompressed = true
            [[pinning_rpc.providers]]
            config_name = 'backup'
            profile = 'kubo'
            auth = 'none'
            allow_private_network = true
            connect_timeout_seconds = 2
            control_timeout_seconds = 3
            idle_timeout_seconds = 3
            [pinning_identity]
            primary_storage_domain = 'kubo:primary'
            [[pinning_identity.providers]]
            config_name = 'backup'
            provider_id = 'stage5-http-backup'
            display_name = 'Independent HTTP mock'
            backend = 'kubo'
            scope = 'node:stage5-http-backup'
            storage_domain = 'kubo:stage5-http-backup'
            credential_revision = 1
            endpoint_revision = 1
            api_profile = 'kubo'
            strategy = 'upload'
            cleanup = 'retain'
            "#,
            source.server.uri(),
            "13".repeat(32),
            target.server.uri(),
        ))
        .unwrap();
        // The production AppState initializer validates and registers the actual
        // IpfsRpcProvider. No custom registry or replacement PinningProvider.
        let state = AppState::new(&config).await.unwrap();
        let provider = state.pinning.effective_config().providers[0].name.clone();
        assert_eq!(
            state
                .pinning
                .provider(&provider)
                .unwrap()
                .invocation_route(),
            ("kubo", "upload")
        );
        store::bucket::create(state.store.db(), BUCKET, Some("test"))
            .await
            .unwrap();
        store::bucket::set_versioning_state(
            state.store.db(),
            BUCKET,
            BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        let gateway = support::cors::start_gateway(
            state.clone(),
            support::cors::default_import_coordinator(),
        )
        .await;
        Self {
            source,
            target,
            state,
            gateway,
            provider,
        }
    }

    async fn send(
        &self,
        method: reqwest::Method,
        key: &str,
        query: &[(&str, &str)],
        body: Vec<u8>,
        headers: HeaderMap,
    ) -> reqwest::Response {
        support::sigv4::send_sigv4(
            method,
            &self.gateway.endpoint,
            BUCKET,
            key,
            query,
            body,
            headers,
            "test",
        )
        .await
    }

    async fn object(&self, key: &str) -> object::Model {
        object::Entity::find()
            .filter(object::Column::Bucket.eq(BUCKET))
            .filter(object::Column::Key.eq(key))
            .one(self.state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    async fn version(&self, object: &object::Model) -> object_version::Model {
        object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq(&object.id))
            .one(self.state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_public(
        &self,
        before: &object::Model,
        version: &object_version::Model,
        headers: HeaderMap,
    ) {
        assert_eq!(
            self.object(&before.key).await,
            *before,
            "worker rewrote published object"
        );
        assert_eq!(
            self.version(before).await,
            *version,
            "worker rewrote public version"
        );
        let response = self
            .send(reqwest::Method::HEAD, &before.key, &[], vec![], headers)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["etag"]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            before.cid
        );
        assert_eq!(
            response.headers()["x-amz-version-id"].to_str().unwrap(),
            version.version_id.as_deref().unwrap()
        );
    }

    async fn run_worker(&self, target_count: usize, resource_count: usize) {
        assert!(
            self.target
                .server
                .received_requests()
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            ledger::submission::observations(self.state.store.db(), &self.provider)
                .await
                .unwrap()
                .is_empty()
        );
        let cancellation = CancellationToken::new();
        let worker = self
            .state
            .pinning
            .start(self.state.store.clone(), cancellation.clone());
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let targets = pin_lease_target::Entity::find()
                    .all(self.state.store.db())
                    .await
                    .unwrap();
                let observations =
                    ledger::submission::observations(self.state.store.db(), &self.provider)
                        .await
                        .unwrap();
                if targets.len() == target_count
                    && targets.iter().all(|target| target.state == "pinned")
                    && observations.len() == resource_count
                    && observations.iter().all(|row| row.outcome == "matched")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        cancellation.cancel();
        worker.shutdown(Duration::from_secs(3)).await;
        result.expect("actual registered RPC worker did not reach pinned + matched ledger within bounded budget");
    }

    async fn assert_resources(&self, expected: &[(String, i64, usize)]) {
        let db = self.state.store.db();
        let identity = self
            .state
            .pinning
            .provider_identity(&self.provider)
            .unwrap();
        let remotes = remote_pin::Entity::find().all(db).await.unwrap();
        assert_eq!(remotes.len(), expected.len());
        let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
        assert_eq!(
            targets.len(),
            expected.iter().map(|(_, _, refs)| refs).sum::<usize>()
        );
        let observations = ledger::submission::observations(db, &self.provider)
            .await
            .unwrap();
        assert_eq!(observations.len(), expected.len());
        for (cid, size, refs) in expected {
            let remote = remotes.iter().find(|remote| &remote.cid == cid).unwrap();
            assert_eq!(remote.provider, self.provider);
            assert_eq!((remote.status.as_str(), remote.cid_size), ("pinned", *size));
            let references: Vec<_> = targets.iter().filter(|target| &target.cid == cid).collect();
            assert_eq!(references.len(), *refs);
            assert_eq!(
                references
                    .iter()
                    .map(|target| &target.lease_id)
                    .collect::<BTreeSet<_>>()
                    .len(),
                *refs
            );
            for target in references {
                assert_eq!(
                    (
                        target.provider.as_str(),
                        target.state.as_str(),
                        target.logical_size
                    ),
                    (self.provider.as_str(), "pinned", *size)
                );
                assert_eq!(
                    pin_lease::Entity::find_by_id(&target.lease_id)
                        .one(db)
                        .await
                        .unwrap()
                        .unwrap()
                        .state,
                    "active"
                );
            }
            let row = observations
                .iter()
                .find(|row| &row.expected_cid == cid)
                .unwrap();
            assert_eq!(
                (row.effect.as_str(), row.outcome.as_str(), row.submit_call),
                ("observed", "matched", 1)
            );
            assert!(!row.needs_attention);
            assert!(row.safe_error.is_none());
            assert!(row.observed_at.is_some());
            let route: ProviderRouteSnapshot = serde_json::from_str(&row.route).unwrap();
            assert_eq!(route, identity.route_snapshot());
            assert_eq!(route.resource_type(), RemoteResourceType::RpcPin);
            assert_eq!(route.cleanup, CleanupMode::Retain);
            let resources: Vec<ledger::submission::ResourceEvidence> =
                serde_json::from_str(&row.resources).unwrap();
            assert_eq!(resources.len(), 1);
            let resource = &resources[0];
            assert_eq!(resource.resource.resource_type, RemoteResourceType::RpcPin);
            assert_eq!(
                resource.resource.status,
                ObservedResourceStatus::RecursiveVerified
            );
            assert_eq!(resource.resource.ownership, Ownership::Unknown);
            assert_eq!(resource.resource.cid, *cid);
            assert_eq!(
                remote.request_id.as_deref(),
                Some(resource.resource.request_id.as_str())
            );
            let key = resource.key.as_ref().unwrap();
            assert_eq!(
                (&key.backend, &key.scope, &key.resource_id),
                (&route.backend, &route.scope, cid)
            );
            assert_eq!(key.resource_type, RemoteResourceType::RpcPin);
            let evidence = ledger::get(db, &self.provider, cid).await.unwrap().unwrap();
            assert_eq!(
                (evidence.ownership.as_str(), evidence.effect.as_str()),
                ("unknown", "confirmed")
            );
            assert!(evidence.remote_pinned_at.is_some());
            assert!(
                !ledger::cleanup_allowed(db, &self.provider, cid)
                    .await
                    .unwrap()
            );
        }
        let usage = quota::read_usage(db, &self.provider)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (usage.reserved_bytes, usage.reserved_pins),
            (
                expected.iter().map(|(_, size, _)| size).sum::<i64>(),
                expected.len() as i64
            )
        );

        // Independent wire evidence, not job counts: one streamed add + exact
        // recursive pin and stable/offline verification for each unique CID.
        let requests = self.target.server.received_requests().await.unwrap();
        for request in &requests {
            // Metadata is checked separately from object bytes: plain content is
            // intentionally transferred, but S3 identity/customer credentials are not.
            for metadata in request
                .headers
                .values()
                .map(|value| value.to_str().unwrap())
                .chain(std::iter::once(request.url.as_str()))
            {
                assert!(!metadata.contains(BUCKET));
                assert!(!metadata.contains("private/"));
                assert!(!metadata.contains(&STANDARD.encode(CUSTOMER_KEY)));
            }
        }
        let counts = requests
            .iter()
            .fold(BTreeMap::new(), |mut counts, request| {
                *counts.entry(request.url.path()).or_insert(0usize) += 1;
                counts
            });
        assert_eq!(
            counts,
            BTreeMap::from([
                ("/api/v0/add", expected.len()),
                ("/api/v0/pin/add", expected.len()),
                ("/api/v0/pin/ls", expected.len() * 2),
                ("/api/v0/files/stat", expected.len()),
                ("/api/v0/id", expected.len() * 2),
            ])
        );
        let adds = self.target.added();
        assert_eq!(adds.len(), expected.len());
        for added in &adds {
            let source = self.source.data.lock().unwrap();
            assert_eq!(
                &added.bytes,
                source.files.get(&added.hash).unwrap(),
                "remote transfer changed stored bytes"
            );
            assert_eq!(
                added.hash,
                raw_cid(&added.bytes).to_string(),
                "target Hash was not computed from streamed bytes"
            );
        }
        let source_requests = self.source.server.received_requests().await.unwrap();
        assert!(
            !source_requests
                .iter()
                .chain(&requests)
                .any(|request| request.url.path() == "/api/v0/pin/rm")
        );
        assert!(!source_requests.iter().any(|request| matches!(
            request.url.path(),
            "/api/v0/dag/put" | "/api/v0/block/stat" | "/api/v0/dag/stat" | "/api/v0/resolve"
        )));
    }
}

fn pin_headers(decompressed: bool) -> HeaderMap {
    HeaderMap::from_iter([(
        "x-amz-tagging".parse().unwrap(),
        HeaderValue::from_str(if decompressed {
            "ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed&ipfs-s3%3Azip-root=false"
        } else {
            "ipfs-s3%3Apin=true"
        })
        .unwrap(),
    )])
}

fn sse_c_headers() -> HeaderMap {
    HeaderMap::from_iter([
        (
            "x-amz-server-side-encryption-customer-algorithm"
                .parse()
                .unwrap(),
            HeaderValue::from_static("AES256"),
        ),
        (
            "x-amz-server-side-encryption-customer-key".parse().unwrap(),
            HeaderValue::from_str(&STANDARD.encode(CUSTOMER_KEY)).unwrap(),
        ),
        (
            "x-amz-server-side-encryption-customer-key-md5"
                .parse()
                .unwrap(),
            HeaderValue::from_str(&STANDARD.encode(md5::compute(CUSTOMER_KEY).0)).unwrap(),
        ),
    ])
}

fn etag(response: &reqwest::Response) -> String {
    response.headers()["etag"]
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_owned()
}

#[tokio::test]
async fn sigv4_put_shared_aliases_and_sse_c_transfer_actual_stored_ciphertext() {
    let harness = Harness::start().await;
    let plain = b"stage5 plain shared bytes".to_vec();
    let secret = b"stage5 secret plaintext must not leave decrypted".to_vec();
    let mut published = Vec::new();
    for (key, payload, encrypted) in [
        ("private/plain-one", &plain, false),
        ("private/plain-two", &plain, false),
        ("private/sse-c", &secret, true),
    ] {
        let mut headers = pin_headers(false);
        if encrypted {
            headers.extend(sse_c_headers());
        }
        let response = harness
            .send(reqwest::Method::PUT, key, &[], payload.clone(), headers)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let object = harness.object(key).await;
        let version = harness.version(&object).await;
        assert_eq!(object.etag, object.cid);
        assert_eq!(etag(&response), object.cid);
        assert_eq!(
            response.headers()["x-amz-version-id"].to_str().unwrap(),
            version.version_id.as_deref().unwrap()
        );
        assert_eq!(object.encrypted, encrypted);
        assert_eq!(object.size, payload.len() as i64);
        published.push((object, version, encrypted));
    }
    let source_adds = harness.source.added();
    assert_eq!(source_adds.len(), 3);
    assert_eq!(source_adds[0].bytes, plain);
    assert_eq!(source_adds[1].bytes, plain);
    assert_ne!(source_adds[0].hash, source_adds[1].hash);
    assert_eq!(
        canonical_resource_cid(&source_adds[0].hash).unwrap(),
        source_adds[1].hash
    );
    let ciphertext = &source_adds[2].bytes;
    assert!(ciphertext.len() > secret.len());
    assert!(
        !ciphertext
            .windows(secret.len())
            .any(|bytes| bytes == secret)
    );
    let response = harness
        .send(
            reqwest::Method::GET,
            "private/sse-c",
            &[],
            vec![],
            sse_c_headers(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), secret);

    harness.run_worker(3, 2).await;
    harness
        .assert_resources(&[
            (source_adds[1].hash.clone(), plain.len() as i64, 2),
            (
                canonical_resource_cid(&source_adds[2].hash).unwrap(),
                secret.len() as i64,
                1,
            ),
        ])
        .await;
    let remote_cipher = harness
        .target
        .added()
        .into_iter()
        .find(|added| added.hash == raw_cid(ciphertext).to_string())
        .unwrap();
    assert_eq!(remote_cipher.bytes, *ciphertext);
    assert_ne!(
        remote_cipher.bytes, secret,
        "SSE-C was downgraded to plaintext"
    );
    for (object, version, encrypted) in published {
        harness
            .assert_public(
                &object,
                &version,
                if encrypted {
                    sse_c_headers()
                } else {
                    HeaderMap::new()
                },
            )
            .await;
    }
    harness.gateway.shutdown().await;
}

#[tokio::test]
async fn sigv4_mpu_complete_and_zip_entry_reach_registered_worker_and_durable_ledger() {
    let harness = Harness::start().await;
    let body = b"short final multipart bytes".to_vec();
    let created = harness
        .send(
            reqwest::Method::POST,
            "private/mpu",
            &[("uploads", "")],
            vec![],
            pin_headers(false),
        )
        .await;
    assert_eq!(created.status(), StatusCode::OK);
    let xml = created.text().await.unwrap();
    let upload_id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    let part = harness
        .send(
            reqwest::Method::PUT,
            "private/mpu",
            &[("uploadId", upload_id), ("partNumber", "1")],
            body.clone(),
            HeaderMap::new(),
        )
        .await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = etag(&part);
    assert!(
        harness
            .target
            .server
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_lease_target::Entity::find()
            .all(harness.state.store.db())
            .await
            .unwrap()
            .is_empty(),
        "parts must not publish remote pin intents"
    );
    let complete = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{part_etag}\"</ETag></Part></CompleteMultipartUpload>"
    );
    let response = harness
        .send(
            reqwest::Method::POST,
            "private/mpu",
            &[("uploadId", upload_id)],
            complete.into_bytes(),
            HeaderMap::new(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let public_version = response.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let xml = response.text().await.unwrap();
    let mpu = harness.object("private/mpu").await;
    let mpu_version = harness.version(&mpu).await;
    assert!(xml.contains(&mpu.cid));
    assert_eq!(
        mpu_version.version_id.as_deref(),
        Some(public_version.as_str())
    );
    assert!(mpu.multipart);

    let archive = support::decompress::legal_single_entry_zip();
    let response = harness
        .send(
            reqwest::Method::PUT,
            "private/archive.zip",
            &[("decompress-zip", "private/expanded/")],
            archive.clone(),
            pin_headers(true),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "disabled");
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    let source = harness.object("private/archive.zip").await;
    assert_eq!(etag(&response), source.cid);
    let source_version = harness.version(&source).await;
    assert_eq!(
        response.headers()["x-amz-version-id"].to_str().unwrap(),
        source_version.version_id.as_deref().unwrap()
    );
    let entry = harness.object("private/expanded/file.txt").await;
    let entry_version = harness.version(&entry).await;
    let source_adds = harness.source.added();
    assert_eq!(
        source_adds
            .iter()
            .map(|added| added.bytes.as_slice())
            .collect::<Vec<_>>(),
        vec![
            body.as_slice(),
            body.as_slice(),
            archive.as_slice(),
            b"single entry bytes"
        ]
    );
    assert_eq!(mpu.cid, source_adds[1].hash);
    assert_eq!(source.cid, source_adds[2].hash);
    assert_eq!(entry.cid, source_adds[3].hash);
    harness.run_worker(2, 2).await;
    harness
        .assert_resources(&[
            (
                canonical_resource_cid(&mpu.cid).unwrap(),
                body.len() as i64,
                1,
            ),
            (
                canonical_resource_cid(&entry.cid).unwrap(),
                b"single entry bytes".len() as i64,
                1,
            ),
        ])
        .await;
    assert!(
        !harness
            .target
            .added()
            .iter()
            .any(|added| added.bytes == archive),
        "decompressed-only ZIP intent uploaded archive"
    );
    for (object, version) in [
        (&mpu, &mpu_version),
        (&source, &source_version),
        (&entry, &entry_version),
    ] {
        harness
            .assert_public(object, version, HeaderMap::new())
            .await;
    }

    // Terminal jobs are queue machinery, not ownership of the RPC evidence.
    let before = ledger::submission::observations(harness.state.store.db(), &harness.provider)
        .await
        .unwrap();
    let deleted = pin_job::Entity::delete_many()
        .filter(pin_job::Column::State.eq("done"))
        .exec(harness.state.store.db())
        .await
        .unwrap();
    assert!(
        deleted.rows_affected > 0,
        "durability proof must delete actual terminal jobs"
    );
    let after = ledger::submission::observations(harness.state.store.db(), &harness.provider)
        .await
        .unwrap();
    assert_eq!(
        after, before,
        "terminal job deletion erased independent submission evidence"
    );
    harness.gateway.shutdown().await;
}
