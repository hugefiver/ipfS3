//! Dedicated helper for the existing Cluster verifier target; no duplicate mocks.
//! Every write is restricted to the runner's fresh, loopback-only labeled fixture.
// Shared by two independent targets: each deliberately uses only its own phase.
#![allow(dead_code)]
use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Write,
    path::PathBuf,
    time::Duration,
};

const LABEL: &str = "ipfs_s3.cluster_real.run";
const BODY_LIMIT: usize = 256 * 1024;
const POLL_LIMIT: Duration = Duration::from_secs(90);

#[derive(Deserialize)]
struct Manifest {
    run_id: String,
    network: String,
    kubo_a: String,
    kubo_b: String,
    cluster_a: String,
    cluster_b: String,
    kubo_a_id: String,
    kubo_b_id: String,
    cluster_a_id: String,
    cluster_b_id: String,
    rest_a: String,
    rest_b: String,
    rpc_a: String,
    rpc_b: String,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    source_rpc: Option<String>,
    #[serde(default)]
    source_mode: Option<String>,
    #[serde(default)]
    proxy_a: Option<String>,
    host_forwarding: String,
    relays: Vec<RelayBinding>,
    #[serde(default)]
    kubo_image_id: Option<String>,
    #[serde(default)]
    cluster_image_id: Option<String>,
}

#[derive(Deserialize)]
struct RelayBinding {
    node: String,
    port: u16,
    url: String,
}

pub struct RealClusterFixture {
    manifest: Manifest,
    client: Client,
    evidence_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Snapshot {
    cid: String,
    allocations: Vec<String>,
    created: String,
    peer_map: BTreeMap<String, PeerState>,
}

#[derive(Debug, Deserialize)]
struct PeerState {
    status: String,
    error: String,
    timestamp: String,
}

impl Snapshot {
    pub fn pinned_peers(&self) -> BTreeSet<String> {
        self.peer_map
            .iter()
            .filter(|(_, state)| state.status == "pinned" && state.error.is_empty())
            .map(|(peer, _)| peer.clone())
            .collect()
    }
}

fn required(name: &str) -> Result<String> {
    let value =
        std::env::var(name).with_context(|| format!("missing explicit fixture env: {name}"))?;
    ensure!(
        !value.trim().is_empty(),
        "empty explicit fixture env: {name}"
    );
    Ok(value)
}

const PROVIDER_ENV: [&str; 9] = [
    "IPFS_S3_REAL_CLUSTER_PROVIDER",
    "IPFS_S3_REAL_CLUSTER",
    "IPFS_S3_REAL_CLUSTER_FIXTURE",
    "IPFS_S3_REAL_CLUSTER_REST_A",
    "IPFS_S3_REAL_CLUSTER_REST_B",
    "IPFS_S3_REAL_CLUSTER_RPC_A",
    "IPFS_S3_REAL_CLUSTER_RPC_B",
    "IPFS_S3_REAL_CLUSTER_SOURCE_RPC",
    "IPFS_S3_REAL_CLUSTER_PROXY_A",
];

fn validate_provider_environment(read: impl Fn(&str) -> Result<String>) -> Result<()> {
    // Check ALL presence/emptiness first, so every missing-env proof fails for
    // its own missing name, never for a previous invalid placeholder.
    let mut values = BTreeMap::new();
    for name in PROVIDER_ENV {
        let value = read(name)?;
        ensure!(
            !value.trim().is_empty(),
            "empty explicit fixture env: {name}"
        );
        values.insert(name, value);
    }
    for name in &PROVIDER_ENV[..2] {
        ensure!(
            values[name] == "1",
            "invalid explicit fixture env: {name}: must equal 1"
        );
    }
    ensure!(
        PathBuf::from(&values[PROVIDER_ENV[2]]).is_absolute(),
        "invalid explicit fixture env: {}: absolute manifest required",
        PROVIDER_ENV[2]
    );
    let mut ports = BTreeSet::new();
    for name in &PROVIDER_ENV[3..] {
        let url = loopback(&values[name])
            .with_context(|| format!("invalid explicit fixture env: {name}"))?;
        ensure!(
            ports.insert(url.port().unwrap()),
            "duplicate explicit fixture port: {name}"
        );
    }
    Ok(())
}

fn loopback(endpoint: &str) -> Result<Url> {
    let url = Url::parse(endpoint)?;
    ensure!(
        url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1")
            && url.port().is_some_and(|port| port != 0)
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "fixture endpoint must be explicit loopback HTTP origin"
    );
    Ok(url)
}

impl RealClusterFixture {
    /// Validate every explicit origin before reading a manifest or touching Docker.
    pub fn provider_environment() -> Result<()> {
        validate_provider_environment(required)
    }

    pub async fn from_provider_environment() -> Result<Self> {
        Self::provider_environment()?;
        let fixture = Self::from_environment().await?;
        ensure!(fixture.manifest.source_mode.as_deref() == Some("offline"));
        ensure!(fixture.manifest.source.is_some() && fixture.manifest.source_id.is_some());
        ensure!(fixture.manifest.source_rpc.is_some() && fixture.manifest.proxy_a.is_some());
        Ok(fixture)
    }

    pub async fn from_environment() -> Result<Self> {
        ensure!(
            required("IPFS_S3_REAL_CLUSTER")? == "1",
            "explicit real Cluster selection required"
        );
        let path = PathBuf::from(required("IPFS_S3_REAL_CLUSTER_FIXTURE")?);
        ensure!(
            path.is_absolute() && path.is_file(),
            "missing absolute fixture manifest"
        );
        let bytes = std::fs::read(&path)?;
        ensure!(bytes.len() <= 16 * 1024, "fixture manifest too large");
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(manifest.host_forwarding == "loopback-docker-exec-nc");
        let provider_phase = manifest.source.is_some();
        if provider_phase {
            Self::provider_environment()?;
        }
        ensure!(manifest.relays.len() == if provider_phase { 6 } else { 4 });
        ensure!(
            manifest
                .relays
                .iter()
                .map(|r| (&r.node, r.port))
                .collect::<BTreeSet<_>>()
                .len()
                == manifest.relays.len()
        );
        ensure!(
            manifest.run_id.len() == 32
                && manifest
                    .run_id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid run identity"
        );
        let prefix = format!("ipfs-s3-cluster-real-{}", manifest.run_id);
        for (name, suffix) in [
            (&manifest.network, "net"),
            (&manifest.kubo_a, "kubo-a"),
            (&manifest.kubo_b, "kubo-b"),
            (&manifest.cluster_a, "cluster-a"),
            (&manifest.cluster_b, "cluster-b"),
        ] {
            ensure!(
                name == &format!("{prefix}-{suffix}"),
                "foreign fixture resource name"
            );
        }
        if let Some(source) = &manifest.source {
            ensure!(source == &format!("{prefix}-source"), "foreign source name");
        }
        let mut endpoints = vec![
            ("IPFS_S3_REAL_CLUSTER_REST_A", &manifest.rest_a),
            ("IPFS_S3_REAL_CLUSTER_REST_B", &manifest.rest_b),
            ("IPFS_S3_REAL_CLUSTER_RPC_A", &manifest.rpc_a),
            ("IPFS_S3_REAL_CLUSTER_RPC_B", &manifest.rpc_b),
        ];
        if provider_phase {
            endpoints.push((
                "IPFS_S3_REAL_CLUSTER_SOURCE_RPC",
                manifest.source_rpc.as_ref().context("missing source RPC")?,
            ));
            endpoints.push((
                "IPFS_S3_REAL_CLUSTER_PROXY_A",
                manifest.proxy_a.as_ref().context("missing proxy RPC")?,
            ));
        }
        let mut ports = BTreeSet::new();
        for (name, expected) in endpoints {
            let actual = required(name)?;
            ensure!(&actual == expected, "endpoint differs from owned manifest");
            ensure!(
                ports.insert(loopback(&actual)?.port().unwrap()),
                "duplicate fixture port"
            );
        }
        let fixture = Self {
            manifest,
            client: Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(15))
                .build()?,
            evidence_path: path.with_file_name("observations.jsonl"),
        };
        let network = fixture
            .owned_inspect("network", &fixture.manifest.network)
            .await?;
        ensure!(
            network["Internal"] == true,
            "Docker network is not internal"
        );
        let mut nodes = vec![
            &fixture.manifest.kubo_a,
            &fixture.manifest.kubo_b,
            &fixture.manifest.cluster_a,
            &fixture.manifest.cluster_b,
        ];
        if let Some(source) = &fixture.manifest.source {
            nodes.push(source);
        }
        for name in nodes {
            let container = fixture.owned_inspect("container", name).await?;
            if provider_phase {
                let image =
                    if name == &fixture.manifest.cluster_a || name == &fixture.manifest.cluster_b {
                        fixture.manifest.cluster_image_id.as_deref()
                    } else {
                        fixture.manifest.kubo_image_id.as_deref()
                    }
                    .context("missing cached image ID")?;
                ensure!(
                    image.starts_with("sha256:") && container["Image"] == image,
                    "node image differs from pinned cache identity"
                );
            }
            let networks = container["NetworkSettings"]["Networks"]
                .as_object()
                .context("missing networks")?;
            ensure!(networks.len() == 1 && networks.contains_key(&fixture.manifest.network));
            ensure!(
                container["State"]["Running"] == true,
                "owned node is not running"
            );
            let mounts = container["Mounts"].as_array().context("missing mounts")?;
            ensure!(mounts.len() == 1, "unexpected fixture mounts");
            ensure!(mounts[0]["Type"] == "volume" && mounts[0]["Name"] == format!("{name}-data"));
            fixture
                .owned_inspect("volume", &format!("{name}-data"))
                .await?;
            let (port, endpoint) = if name == &fixture.manifest.kubo_a {
                (5001, &fixture.manifest.rpc_a)
            } else if name == &fixture.manifest.kubo_b {
                (5001, &fixture.manifest.rpc_b)
            } else if name == &fixture.manifest.cluster_a {
                (9094, &fixture.manifest.rest_a)
            } else if Some(name) == fixture.manifest.source.as_ref() {
                (
                    5001,
                    fixture
                        .manifest
                        .source_rpc
                        .as_ref()
                        .context("missing source RPC")?,
                )
            } else {
                (9094, &fixture.manifest.rest_b)
            };
            let bindings = container["NetworkSettings"]["Ports"]
                .as_object()
                .context("missing Docker port inventory")?;
            ensure!(
                bindings
                    .values()
                    .all(|b| b.is_null() || b.as_array().is_some_and(Vec::is_empty)),
                "Docker published ports are forbidden"
            );
            let published = &container["HostConfig"]["PortBindings"];
            ensure!(
                published.is_null() || published.as_object().is_some_and(serde_json::Map::is_empty)
            );
            let relay = fixture
                .manifest
                .relays
                .iter()
                .find(|relay| &relay.node == name)
                .context("missing exact owned relay")?;
            ensure!(
                relay.port == port && &relay.url == endpoint,
                "relay node/port/origin mismatch"
            );
            fixture.record(
                "owned-relay-binding",
                &format!("{}:{} origin={}", relay.node, relay.port, relay.url),
            )?;
        }
        if provider_phase {
            let proxy = fixture
                .manifest
                .relays
                .iter()
                .find(|relay| relay.port == 9095)
                .context("missing owned Proxy relay")?;
            ensure!(proxy.node == fixture.manifest.cluster_a);
            ensure!(Some(&proxy.url) == fixture.manifest.proxy_a.as_ref());
            fixture.record(
                "owned-proxy-relay-binding",
                &format!("{}:{} origin={}", proxy.node, proxy.port, proxy.url),
            )?;
        }
        Ok(fixture)
    }

    pub fn run_id(&self) -> &str {
        &self.manifest.run_id
    }
    pub fn rest_a(&self) -> &str {
        &self.manifest.rest_a
    }
    pub fn rest_b(&self) -> &str {
        &self.manifest.rest_b
    }
    pub fn peer_a(&self) -> &str {
        &self.manifest.cluster_a_id
    }
    pub fn rpc_a(&self) -> &str {
        &self.manifest.rpc_a
    }
    pub fn rpc_b(&self) -> &str {
        &self.manifest.rpc_b
    }
    pub fn source_rpc(&self) -> &str {
        self.manifest
            .source_rpc
            .as_deref()
            .expect("validated provider fixture")
    }
    pub fn proxy_a(&self) -> &str {
        self.manifest
            .proxy_a
            .as_deref()
            .expect("validated provider fixture")
    }
    pub fn source_id(&self) -> &str {
        self.manifest
            .source_id
            .as_deref()
            .expect("validated provider fixture")
    }
    pub fn kubo_ids(&self) -> [&str; 2] {
        [&self.manifest.kubo_a_id, &self.manifest.kubo_b_id]
    }

    pub async fn assert_source_offline(&self) -> Result<()> {
        let (status, bytes) = self
            .request(
                Method::POST,
                self.source_rpc(),
                "/api/v0/swarm/addrs/listen",
                None,
            )
            .await?;
        ensure!(status == StatusCode::INTERNAL_SERVER_ERROR);
        let error: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            error["Message"]
                .as_str()
                .context("missing offline error")?
                .contains("online mode"),
            "source daemon is not offline"
        );
        self.record(
            "source-offline-runtime",
            "swarm/addrs/listen HTTP 500 online-mode-required; source has no content listeners",
        )?;
        Ok(())
    }
    pub fn expected_peers(&self) -> BTreeSet<String> {
        BTreeSet::from([
            self.manifest.cluster_a_id.clone(),
            self.manifest.cluster_b_id.clone(),
        ])
    }

    pub fn record(&self, phase: &str, detail: &str) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.evidence_path)?;
        writeln!(
            file,
            "{}",
            json!({"phase": phase, "detail": detail, "observed_at": chrono::Utc::now()})
        )?;
        Ok(())
    }

    async fn docker(&self, arguments: &[&str]) -> Result<Vec<u8>> {
        let output = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new("docker")
                .args(arguments)
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        self.record(
            "docker",
            &format!(
                "{arguments:?} exit={:?} stderr={}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr)
            ),
        )?;
        ensure!(
            output.status.success(),
            "owned fixture Docker command failed"
        );
        Ok(output.stdout)
    }

    async fn owned_inspect(&self, kind: &str, name: &str) -> Result<Value> {
        let bytes = self.docker(&[kind, "inspect", name]).await?;
        let items: Vec<Value> = serde_json::from_slice(&bytes)?;
        ensure!(items.len() == 1, "ambiguous owned resource");
        let item = items.into_iter().next().unwrap();
        let labels = if kind == "container" {
            &item["Config"]["Labels"]
        } else {
            &item["Labels"]
        };
        ensure!(
            labels[LABEL] == self.manifest.run_id,
            "foreign resource label"
        );
        ensure!(
            item["Name"]
                .as_str()
                .context("missing resource name")?
                .trim_start_matches('/')
                == name
        );
        Ok(item)
    }

    async fn request(
        &self,
        method: Method,
        origin: &str,
        path: &str,
        form: Option<reqwest::multipart::Form>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let url = loopback(origin)?.join(path)?;
        let mut request = self
            .client
            .request(method.clone(), url)
            .header("Cache-Control", "no-cache, no-store");
        if let Some(form) = form {
            request = request.multipart(form);
        }
        let response = request.send().await?;
        let status = response.status();
        ensure!(
            response.content_length().unwrap_or(0) <= BODY_LIMIT as u64,
            "fixture body too large"
        );
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            ensure!(
                body.len() + chunk.len() <= BODY_LIMIT,
                "fixture body too large"
            );
            body.extend_from_slice(&chunk);
        }
        self.record(
            "http",
            &format!(
                "{method} {origin}{path} status={status} body={}",
                String::from_utf8_lossy(&body)
            ),
        )?;
        Ok((status, body))
    }

    pub(crate) async fn json(&self, method: Method, origin: &str, path: &str) -> Result<Value> {
        let (status, body) = self.request(method, origin, path, None).await?;
        ensure!(
            status.is_success(),
            "fixture HTTP contract failed ({status})"
        );
        Ok(serde_json::from_slice(&body)?)
    }

    pub(crate) async fn assert_allocation_absent(&self, rest: &str, cid: &str) -> Result<()> {
        let (status, _) = self
            .request(Method::GET, rest, &format!("/allocations/{cid}"), None)
            .await?;
        ensure!(
            status == StatusCode::NOT_FOUND,
            "forbidden original-CID fallback created an allocation"
        );
        self.record(
            "mismatch-original-allocation-absent",
            &format!(
                "rest={rest} cid={cid} HTTP=404 (fixture check, not ledger deletion authority)"
            ),
        )?;
        Ok(())
    }

    pub(crate) async fn assert_no_swarm(&self, rpc: &str) -> Result<()> {
        let config = self.json(Method::POST, rpc, "/api/v0/config/show").await?;
        ensure!(config["Bootstrap"] == json!([]));
        ensure!(config["AutoConf"]["Enabled"] == false);
        ensure!(config["Discovery"]["MDNS"]["Enabled"] == false);
        ensure!(config["Routing"]["Type"] == "none");
        ensure!(config["Addresses"]["Swarm"] == json!([]));
        ensure!(config["Swarm"]["RelayClient"]["Enabled"] == false);
        ensure!(config["Swarm"]["Transports"]["Network"]["Relay"] == false);
        ensure!(config["Swarm"]["DisableNatPortMap"] == true);
        ensure!(config["Plugins"]["Plugins"]["telemetry"]["Disabled"] == true);
        let listeners = self
            .json(Method::POST, rpc, "/api/v0/swarm/addrs/listen")
            .await?;
        ensure!(
            listeners["Strings"].is_null() || listeners["Strings"] == json!([]),
            "Kubo must not listen for content swarm connections"
        );
        let peers = self.json(Method::POST, rpc, "/api/v0/swarm/peers").await?;
        ensure!(
            peers["Peers"].is_null() || peers["Peers"] == json!([]),
            "Kubo content swarm must stay empty"
        );
        Ok(())
    }

    pub async fn validate_nodes(&self) -> Result<()> {
        ensure!(
            self.expected_peers().len() == 2 && self.manifest.kubo_a_id != self.manifest.kubo_b_id
        );
        for (rpc, expected) in [
            (&self.manifest.rpc_a, &self.manifest.kubo_a_id),
            (&self.manifest.rpc_b, &self.manifest.kubo_b_id),
        ] {
            ensure!(self.json(Method::POST, rpc, "/api/v0/version").await?["Version"] == "0.43.0");
            ensure!(self.json(Method::POST, rpc, "/api/v0/id").await?["ID"] == *expected);
            self.assert_no_swarm(rpc).await?;
        }
        for (rest, expected, kubo) in [
            (
                &self.manifest.rest_a,
                &self.manifest.cluster_a_id,
                &self.manifest.kubo_a_id,
            ),
            (
                &self.manifest.rest_b,
                &self.manifest.cluster_b_id,
                &self.manifest.kubo_b_id,
            ),
        ] {
            let identity = self.json(Method::GET, rest, "/id").await?;
            ensure!(identity["id"] == *expected && identity["ipfs"]["id"] == *kubo);
            ensure!(
                identity["error"] == ""
                    && identity["ipfs"]["error"].as_str().unwrap_or("").is_empty()
            );
            let version = self.json(Method::GET, rest, "/version").await?;
            let version = version["version"]
                .as_str()
                .context("missing Cluster version")?;
            ensure!(version == "1.1.6" || version.starts_with("1.1.6+"));
            let convergence = async {
                loop {
                    let (status, bytes) = self.request(Method::GET, rest, "/peers", None).await?;
                    ensure!(status.is_success());
                    let peers: Vec<Value> = std::str::from_utf8(&bytes)?
                        .lines()
                        .filter(|s| !s.trim().is_empty())
                        .map(serde_json::from_str)
                        .collect::<std::result::Result<_, _>>()?;
                    let ids = peers
                        .iter()
                        .map(|p| {
                            p["id"]
                                .as_str()
                                .context("missing Cluster peer")
                                .map(str::to_owned)
                        })
                        .collect::<Result<BTreeSet<_>>>()?;
                    ensure!(
                        ids.len() == peers.len() && ids.is_subset(&self.expected_peers()),
                        "foreign/duplicate Cluster peer"
                    );
                    for peer in &peers {
                        ensure!(
                            peer["error"] == ""
                                && peer["ipfs"]["error"].as_str().unwrap_or("").is_empty()
                        );
                        let expected_kubo = if peer["id"] == self.manifest.cluster_a_id {
                            &self.manifest.kubo_a_id
                        } else {
                            &self.manifest.kubo_b_id
                        };
                        ensure!(
                            peer["ipfs"]["id"] == *expected_kubo,
                            "Cluster/Kubo pairing changed"
                        );
                    }
                    if ids == self.expected_peers() {
                        let mut agreed = true;
                        for peer in &peers {
                            let view: Vec<String> =
                                serde_json::from_value(peer["cluster_peers"].clone())?;
                            ensure!(
                                view.len() <= 2
                                    && view.iter().all(|id| self.expected_peers().contains(id))
                            );
                            agreed &= view.len() == 2
                                && view.into_iter().collect::<BTreeSet<_>>()
                                    == self.expected_peers();
                        }
                        if agreed {
                            return Ok::<(), anyhow::Error>(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            };
            tokio::time::timeout(POLL_LIMIT, convergence)
                .await
                .context("two-peer membership convergence deadline")??;
            // In v1.1.6 user-allocations is only a priority list: allocate.go
            // still requires valid metrics from every candidate. The runner
            // explicitly configures balanced allocate_by=["freespace"].
            let allocation_ready = async {
                loop {
                    let metrics = self
                        .json(Method::GET, rest, "/monitor/metrics/freespace")
                        .await?;
                    if metrics.is_null() {
                        // Go's nil metric slice is an empty, not-ready inventory.
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        continue;
                    }
                    let metrics = metrics
                        .as_array()
                        .context("missing freespace metric array")?;
                    let mut valid = BTreeSet::new();
                    let now = chrono::Utc::now()
                        .timestamp_nanos_opt()
                        .context("clock out of range")?;
                    for metric in metrics {
                        ensure!(metric["name"] == "freespace");
                        let peer = metric["peer"].as_str().context("missing metric peer")?;
                        ensure!(
                            self.expected_peers().contains(peer),
                            "foreign allocator metric"
                        );
                        if metric["valid"] == true
                            && metric["expire"].as_i64().context("missing metric expiry")? > now
                        {
                            ensure!(valid.insert(peer.to_owned()), "duplicate allocator metric");
                        }
                    }
                    if valid == self.expected_peers() {
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            };
            tokio::time::timeout(POLL_LIMIT, allocation_ready)
                .await
                .context("two-peer allocator metric convergence deadline")??;
        }
        Ok(())
    }

    async fn preseed(&self, rpc: &str, payload: &[u8]) -> Result<String> {
        self.assert_no_swarm(rpc).await?;
        let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(payload))?;
        let expected = cid::Cid::new_v1(0x55, hash).to_string();
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(payload.to_vec()).file_name("fixture.raw"),
        );
        let (status, bytes) = self
            .request(
                Method::POST,
                rpc,
                "/api/v0/block/put?cid-codec=raw&mhtype=sha2-256&pin=false",
                Some(form),
            )
            .await?;
        ensure!(status.is_success());
        let result: Value = serde_json::from_slice(&bytes)?;
        ensure!(result["Key"] == expected, "preseed changed CID");
        let pins = self
            .json(Method::POST, rpc, "/api/v0/pin/ls?type=recursive")
            .await?;
        ensure!(
            !pins["Keys"]
                .as_object()
                .context("missing Kubo pin set")?
                .contains_key(&expected),
            "preseed must not pin"
        );
        Ok(expected)
    }

    pub async fn preseed_a(&self, payload: &[u8]) -> Result<String> {
        self.preseed(&self.manifest.rpc_a, payload).await
    }
    pub async fn preseed_b(&self, cid: &str, payload: &[u8]) -> Result<()> {
        ensure!(self.preseed(&self.manifest.rpc_b, payload).await? == cid);
        Ok(())
    }

    pub async fn assert_local_absent_on_b(&self, cid: &str) -> Result<()> {
        self.assert_no_swarm(&self.manifest.rpc_b).await?;
        let (status, bytes) = self
            .request(
                Method::POST,
                &self.manifest.rpc_b,
                "/api/v0/refs/local",
                None,
            )
            .await?;
        ensure!(status.is_success());
        for line in std::str::from_utf8(&bytes)?
            .lines()
            .filter(|s| !s.trim().is_empty())
        {
            let block: Value = serde_json::from_str(line)?;
            ensure!(block["Err"].as_str().unwrap_or("").is_empty());
            let local =
                cid::Cid::try_from(block["Ref"].as_str().context("missing local block CID")?)?;
            ensure!(
                local != cid::Cid::try_from(cid)?,
                "B already has the fixture block"
            );
        }
        Ok(())
    }

    pub async fn pin_in_cluster(&self, cid: &str) -> Result<()> {
        self.json(
            Method::POST,
            self.rest_a(),
            &format!(
                "/pins/{cid}?replication-min=2&replication-max=2&user-allocations={},{}",
                self.manifest.cluster_a_id, self.manifest.cluster_b_id
            ),
        )
        .await?;
        Ok(())
    }

    pub async fn recover_b(&self, cid: &str) -> Result<()> {
        self.json(
            Method::POST,
            self.rest_b(),
            &format!("/pins/{cid}/recover?local=true"),
        )
        .await?;
        Ok(())
    }

    pub async fn wait_for_pinned(&self, rest: &str, cid: &str, count: usize) -> Result<Snapshot> {
        let deadline = tokio::time::Instant::now() + POLL_LIMIT;
        let poll = async {
            loop {
                let (status, bytes) = self
                    .request(Method::GET, rest, &format!("/allocations/{cid}"), None)
                    .await?;
                if status == StatusCode::NOT_FOUND {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
                ensure!(status.is_success());
                let allocation: Value = serde_json::from_slice(&bytes)?;
                let peers: Vec<String> = serde_json::from_value(allocation["allocations"].clone())?;
                ensure!(
                    peers.len() == 2
                        && peers.into_iter().collect::<BTreeSet<_>>() == self.expected_peers()
                );
                ensure!(
                    allocation["cid"] == cid
                        && allocation["replication_factor_min"] == 2
                        && allocation["replication_factor_max"] == 2
                );
                let global = self
                    .json(Method::GET, rest, &format!("/pins/{cid}?local=false"))
                    .await?;
                let snapshot: Snapshot = serde_json::from_value(global)?;
                ensure!(snapshot.cid == cid && snapshot.allocations.len() == 2);
                ensure!(
                    snapshot
                        .allocations
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>()
                        == self.expected_peers()
                );
                ensure!(
                    snapshot.created
                        == allocation["timestamp"]
                            .as_str()
                            .context("missing allocation timestamp")?
                );
                ensure!(
                    snapshot.peer_map.keys().cloned().collect::<BTreeSet<_>>()
                        == self.expected_peers()
                );
                for state in snapshot.peer_map.values() {
                    chrono::DateTime::parse_from_rfc3339(&state.timestamp)?;
                }
                let reread = self
                    .json(Method::GET, rest, &format!("/allocations/{cid}"))
                    .await?;
                ensure!(
                    reread == allocation,
                    "allocation changed during acceptance snapshot"
                );
                self.record("raw-global-snapshot", &format!("{snapshot:?}"))?;
                // The initial B pin attempt must have finished with a real error
                // before preseed/recover, otherwise pin=false readback races a
                // still-running Cluster pin. Automatic recovery is set to 1h.
                let b_failed = matches!(
                    snapshot.peer_map[&self.manifest.cluster_b_id]
                        .status
                        .as_str(),
                    "pin_error" | "cluster_error"
                );
                if snapshot.pinned_peers().len() == count && (count != 1 || b_failed) {
                    return Ok(snapshot);
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        };
        match tokio::time::timeout_at(deadline, poll).await {
            Ok(result) => result,
            Err(_) => bail!("real Cluster never reached actual {count}/2 before polling deadline"),
        }
    }

    pub async fn assert_local_complete(&self, cid: &str, payload: &[u8]) -> Result<()> {
        for rpc in [&self.manifest.rpc_a, &self.manifest.rpc_b] {
            self.assert_no_swarm(rpc).await?;
            let pins = self
                .json(
                    Method::POST,
                    rpc,
                    &format!("/api/v0/pin/ls?arg={cid}&type=recursive"),
                )
                .await?;
            ensure!(pins["Keys"][cid]["Type"] == "recursive");
            let (status, bytes) = self
                .request(
                    Method::POST,
                    rpc,
                    &format!("/api/v0/block/get?arg={cid}&offline=true"),
                    None,
                )
                .await?;
            ensure!(
                status.is_success() && bytes == payload,
                "local complete raw-block bytes differ"
            );
        }
        Ok(())
    }

    pub async fn stop_owned_kubo_b(&self) -> Result<()> {
        let node = self
            .owned_inspect("container", &self.manifest.kubo_b)
            .await?;
        ensure!(node["State"]["Running"] == true);
        self.docker(&["container", "stop", "--time", "5", &self.manifest.kubo_b])
            .await?;
        let node = self
            .owned_inspect("container", &self.manifest.kubo_b)
            .await?;
        ensure!(
            node["State"]["Running"] == false,
            "allocated Kubo B did not stop"
        );
        Ok(())
    }

    pub async fn stop_owned_source(&self) -> Result<()> {
        let source = self
            .manifest
            .source
            .as_ref()
            .context("missing owned source")?;
        ensure!(self.owned_inspect("container", source).await?["State"]["Running"] == true);
        // Capture inspect/logs before the deliberate source stop as well as cleanup.
        let logs = self.docker(&["logs", source]).await?;
        std::fs::write(
            self.evidence_path.with_file_name("source.pre-stop.log"),
            logs,
        )?;
        self.record(
            "source-pre-stop-inspect",
            &self.owned_inspect("container", source).await?.to_string(),
        )?;
        self.docker(&["container", "stop", "--time", "5", source])
            .await?;
        ensure!(self.owned_inspect("container", source).await?["State"]["Running"] == false);
        self.record("source-stopped", "independent source container running=false; no target content preload, no pin/rm, unpin or GC")?;
        Ok(())
    }
}

#[cfg(test)]
mod environment_tests {
    use super::*;

    fn valid() -> BTreeMap<&'static str, String> {
        let mut values = BTreeMap::from([
            (PROVIDER_ENV[0], "1".into()),
            (PROVIDER_ENV[1], "1".into()),
            (
                PROVIDER_ENV[2],
                std::env::temp_dir()
                    .join("not-read-stage6-fixture.json")
                    .to_string_lossy()
                    .into_owned(),
            ),
        ]);
        for (i, name) in PROVIDER_ENV[3..].iter().enumerate() {
            values.insert(*name, format!("http://127.0.0.1:{}", 64001 + i));
        }
        values
    }

    fn check(values: &BTreeMap<&str, String>) -> Result<()> {
        validate_provider_environment(|name| {
            values
                .get(name)
                .cloned()
                .with_context(|| format!("missing explicit fixture env: {name}"))
        })
    }

    #[test]
    fn cluster_provider_real_all_environment_gates_are_pure_and_fail_closed() {
        assert!(check(&valid()).is_ok()); // no manifest read, Docker or network
        for name in PROVIDER_ENV {
            let mut values = valid();
            values.remove(name);
            assert!(
                check(&values)
                    .unwrap_err()
                    .to_string()
                    .contains(&format!("missing explicit fixture env: {name}"))
            );
            let mut values = valid();
            values.insert(name, " ".into());
            assert!(
                check(&values)
                    .unwrap_err()
                    .to_string()
                    .contains(&format!("empty explicit fixture env: {name}"))
            );
        }
        for name in &PROVIDER_ENV[..2] {
            for invalid in ["0", "true", " 1"] {
                let mut values = valid();
                values.insert(*name, invalid.into());
                assert!(check(&values).is_err());
            }
        }
        let mut values = valid();
        values.insert(PROVIDER_ENV[2], "relative-fixture.json".into());
        assert!(check(&values).is_err());
        for name in &PROVIDER_ENV[3..] {
            for invalid in [
                "http://localhost:1234",
                "http://127.0.0.1",
                "http://127.0.0.1:0",
                "https://127.0.0.1:1234",
                "http://127.0.0.1:1234/api/v0",
                "http://user:pass@127.0.0.1:1234",
                "http://127.0.0.1:1234?arg=x",
                "http://127.0.0.1:1234#x",
            ] {
                let mut values = valid();
                values.insert(*name, invalid.into());
                assert!(check(&values).is_err(), "{name} accepted {invalid}");
            }
        }
        let mut values = valid();
        values.insert(PROVIDER_ENV[4], values[PROVIDER_ENV[3]].clone());
        assert!(check(&values).is_err());
    }
}
