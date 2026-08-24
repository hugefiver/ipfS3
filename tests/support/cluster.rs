use anyhow::{Result, anyhow, ensure};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt, fs::OpenOptions, io::Write, path::Path, time::Duration};

const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterPeer {
    pub id: String,
    #[serde(default)]
    pub cluster_peers: Vec<String>,
    pub version: String,
    pub peername: String,
    #[serde(default)]
    pub error: String,
    pub ipfs: ClusterIpfsIdentity,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterIpfsIdentity {
    pub id: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PinAllocation {
    pub cid: String,
    pub replication_factor_min: i32,
    pub replication_factor_max: i32,
    #[serde(default)]
    pub allocations: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GlobalPinInfo {
    pub cid: String,
    #[serde(default)]
    pub allocations: Vec<String>,
    #[serde(default)]
    pub peer_map: std::collections::HashMap<String, PeerPinInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerPinInfo {
    pub status: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug)]
pub enum PinStatusObservation {
    Pending,
    Available(GlobalPinInfo),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeError {
    Transient(&'static str),
    Terminal(&'static str),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(category) => write!(formatter, "transient Cluster probe: {category}"),
            Self::Terminal(category) => {
                write!(formatter, "terminal Cluster contract error: {category}")
            }
        }
    }
}

impl std::error::Error for ProbeError {}

pub type ProbeResult<T> = std::result::Result<T, ProbeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEvidence {
    pub allocations: Vec<String>,
    pub pinned_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryState {
    pub schema: String,
    pub source: String,
    pub cid: String,
    pub body: Vec<u8>,
    pub peer_set_sha256: String,
}

pub struct ClusterClient {
    base_url: String,
    http: Client,
}

impl ClusterClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        let base_url = validate_loopback_http(endpoint, "cluster_endpoint_not_loopback_http")?;
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(CALL_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("cluster_client_build_failed"))?;

        Ok(Self { base_url, http })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn response(&self, path: &str) -> ProbeResult<reqwest::Response> {
        self.http
            .get(self.url(path))
            .send()
            .await
            .map_err(|_| ProbeError::Transient("request_transport"))
    }

    async fn response_bytes(response: reqwest::Response) -> ProbeResult<bytes::Bytes> {
        response
            .bytes()
            .await
            .map_err(|_| ProbeError::Transient("response_body_transport"))
    }

    fn decode_json<T: DeserializeOwned>(bytes: &[u8], category: &'static str) -> ProbeResult<T> {
        serde_json::from_slice(bytes).map_err(|_| ProbeError::Terminal(category))
    }

    pub async fn health_probe(&self) -> ProbeResult<()> {
        let response = self.response("/health").await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Err(ProbeError::Terminal("health_status_not_204"));
        }
        if !Self::response_bytes(response).await?.is_empty() {
            return Err(ProbeError::Terminal("health_204_body_not_empty"));
        }
        Ok(())
    }

    pub async fn peers_probe(&self) -> ProbeResult<Vec<ClusterPeer>> {
        let response = self.response("/peers").await?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("peers_status_not_success"));
        }
        let bytes = Self::response_bytes(response).await?;
        let text =
            std::str::from_utf8(&bytes).map_err(|_| ProbeError::Terminal("peers_body_not_utf8"))?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| Self::decode_json(line.as_bytes(), "peers_malformed_ndjson"))
            .collect()
    }

    pub async fn allocation_probe(&self, cid: &str) -> ProbeResult<Option<PinAllocation>> {
        let response = self.response(&format!("/allocations/{cid}")).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("allocation_status_not_success"));
        }
        let bytes = Self::response_bytes(response).await?;
        Self::decode_json(&bytes, "allocation_malformed_json").map(Some)
    }

    pub async fn pin_status_probe(&self, cid: &str) -> ProbeResult<PinStatusObservation> {
        let response = self.response(&format!("/pins/{cid}")).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(PinStatusObservation::Pending);
        }
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("pin_status_not_success"));
        }
        let bytes = Self::response_bytes(response).await?;
        Ok(PinStatusObservation::Available(Self::decode_json(
            &bytes,
            "pin_status_malformed_json",
        )?))
    }
}

#[derive(Debug)]
enum PeerView {
    Pending(usize),
    Ready(Vec<String>),
}

fn validate_loopback_http(endpoint: &str, category: &'static str) -> Result<String> {
    let endpoint = endpoint.trim_end_matches('/');
    let parsed = Url::parse(endpoint).map_err(|_| anyhow!(category))?;
    ensure!(parsed.scheme() == "http", "{category}");
    ensure!(parsed.host_str() == Some("127.0.0.1"), "{category}");
    ensure!(parsed.port().is_some(), "{category}");
    ensure!(parsed.username().is_empty(), "{category}");
    ensure!(parsed.password().is_none(), "{category}");
    ensure!(parsed.query().is_none(), "{category}");
    ensure!(parsed.fragment().is_none(), "{category}");
    ensure!(parsed.path() == "/", "{category}");
    Ok(endpoint.to_owned())
}

fn sorted_set(values: &[String]) -> Vec<String> {
    let mut unique = values
        .iter()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    unique.sort();
    unique
}

async fn probe_peer_view(client: &ClusterClient) -> ProbeResult<PeerView> {
    client.health_probe().await?;
    let peers = client.peers_probe().await?;
    if peers.len() > 2 {
        return Err(ProbeError::Terminal("third_peer_record"));
    }

    let mut ids = Vec::with_capacity(peers.len());
    for peer in &peers {
        if peer.error.is_empty()
            && !peer.id.is_empty()
            && !peer.peername.is_empty()
            && peer.ipfs.error.is_empty()
            && !peer.ipfs.id.is_empty()
        {
            // Peer values remain in memory for equality checks only.
        } else {
            return Err(ProbeError::Terminal("peer_record_invalid"));
        }
        if !is_release_1_1_6_version(&peer.version) {
            return Err(ProbeError::Terminal("peer_version_not_release_1_1_6"));
        }
        if peer.cluster_peers.len() > 2 {
            return Err(ProbeError::Terminal("third_peer_membership"));
        }
        ids.push(peer.id.clone());
    }

    let unique = sorted_set(&ids);
    if unique.len() != ids.len() {
        return Err(ProbeError::Terminal("duplicate_peer_id"));
    }
    if unique.len() < 2 || peers.iter().any(|peer| peer.cluster_peers.len() < 2) {
        return Ok(PeerView::Pending(unique.len()));
    }
    if peers
        .iter()
        .any(|peer| sorted_set(&peer.cluster_peers) != unique)
    {
        return Err(ProbeError::Terminal("peer_membership_contradiction"));
    }
    Ok(PeerView::Ready(unique))
}

pub async fn wait_for_shared_two_peer_view(
    a: &ClusterClient,
    b: &ClusterClient,
    timeout: Duration,
) -> ProbeResult<Vec<String>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (a_probe, b_probe) = tokio::join!(probe_peer_view(a), probe_peer_view(b));
        match (a_probe, b_probe) {
            (Err(error @ ProbeError::Terminal(_)), _)
            | (_, Err(error @ ProbeError::Terminal(_))) => {
                return Err(error);
            }
            (Ok(PeerView::Ready(a_ids)), Ok(PeerView::Ready(b_ids))) => {
                if a_ids != b_ids {
                    return Err(ProbeError::Terminal("contradictory_complete_rest_views"));
                }
                return Ok(a_ids);
            }
            (Ok(PeerView::Pending(a_count)), Ok(PeerView::Pending(b_count))) => {
                let _sanitized_counts = (a_count, b_count);
            }
            (Err(ProbeError::Transient(_)), _)
            | (_, Err(ProbeError::Transient(_)))
            | (Ok(PeerView::Pending(_)), Ok(PeerView::Ready(_)))
            | (Ok(PeerView::Ready(_)), Ok(PeerView::Pending(_))) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ProbeError::Terminal("two_peer_convergence_timeout"));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub fn peer_set_digest(values: &[String]) -> ProbeResult<String> {
    let ids = sorted_set(values);
    if ids.len() != 2 || ids.len() != values.len() {
        return Err(ProbeError::Terminal(
            "peer_digest_requires_exact_distinct_two",
        ));
    }

    let mut hash = Sha256::new();
    for id in ids {
        hash.update(id.as_bytes());
        hash.update([0]);
    }
    Ok(hex::encode(hash.finalize()))
}

pub async fn wait_for_two_pinned(
    client: &ClusterClient,
    cid: &str,
    expected_peers: &[String],
    timeout: Duration,
) -> ProbeResult<ReplicationEvidence> {
    let expected = sorted_set(expected_peers);
    if expected.len() != 2 || expected.len() != expected_peers.len() {
        return Err(ProbeError::Terminal("pin_wait_requires_exact_distinct_two"));
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let allocation = match client.allocation_probe(cid).await {
            Ok(Some(value)) => value,
            Ok(None) | Err(ProbeError::Transient(_)) => {
                wait_or_timeout(deadline, "two_pin_timeout").await?;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        if allocation.cid != cid
            || allocation.replication_factor_min != 2
            || allocation.replication_factor_max != 2
        {
            return Err(ProbeError::Terminal("allocation_contract_mismatch"));
        }
        let allocations = sorted_set(&allocation.allocations);
        if allocations.len() != allocation.allocations.len()
            || allocations.iter().any(|id| !expected.contains(id))
        {
            return Err(ProbeError::Terminal("allocation_peer_set_invalid"));
        }

        let status = match client.pin_status_probe(cid).await {
            Ok(PinStatusObservation::Available(value)) => value,
            Ok(PinStatusObservation::Pending) | Err(ProbeError::Transient(_)) => {
                wait_or_timeout(deadline, "two_pin_timeout").await?;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        let status_allocations = sorted_set(&status.allocations);
        if status.cid != cid
            || status_allocations.len() != status.allocations.len()
            || status_allocations != allocations
        {
            return Err(ProbeError::Terminal("pin_status_contract_mismatch"));
        }
        if status.peer_map.keys().any(|id| !expected.contains(id)) || status.peer_map.len() > 2 {
            return Err(ProbeError::Terminal("pin_status_peer_set_invalid"));
        }
        let known_states = [
            "pinned",
            "pinning",
            "unpinning",
            "unpinned",
            "remote",
            "pin_queued",
            "unpin_queued",
            "queued",
            "sharded",
        ];
        if status
            .peer_map
            .values()
            .any(|info| !info.error.is_empty() || !known_states.contains(&info.status.as_str()))
        {
            return Err(ProbeError::Terminal("pin_tracker_contract_error"));
        }
        let pinned = sorted_set(
            &status
                .peer_map
                .iter()
                .filter(|(_, info)| info.status == "pinned")
                .map(|(peer, _)| peer.clone())
                .collect::<Vec<_>>(),
        );
        if allocations == expected && pinned == expected {
            return Ok(ReplicationEvidence {
                allocations,
                pinned_peers: pinned,
            });
        }
        wait_or_timeout(deadline, "two_pin_timeout").await?;
    }
}

pub async fn wait_until_not_fully_pinned(
    client: &ClusterClient,
    cid: &str,
    expected_peer_digest: &str,
    timeout: Duration,
) -> ProbeResult<()> {
    if !is_hex_digest(expected_peer_digest) {
        return Err(ProbeError::Terminal(
            "outage_wait_requires_valid_peer_digest",
        ));
    }

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client.health_probe().await {
            Ok(()) => {}
            Err(ProbeError::Transient(_)) => {
                wait_or_timeout(deadline, "outage_observation_timeout").await?;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        }
        let allocation = match client.allocation_probe(cid).await {
            Ok(Some(value)) => value,
            Ok(None) => return Err(ProbeError::Terminal("outage_allocation_missing")),
            Err(ProbeError::Transient(_)) => {
                wait_or_timeout(deadline, "outage_observation_timeout").await?;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        let expected = sorted_set(&allocation.allocations);
        if expected.len() != 2
            || expected.len() != allocation.allocations.len()
            || peer_set_digest(&expected)? != expected_peer_digest
        {
            return Err(ProbeError::Terminal(
                "outage_allocation_peer_digest_mismatch",
            ));
        }
        if allocation.cid != cid
            || allocation.replication_factor_min != 2
            || allocation.replication_factor_max != 2
        {
            return Err(ProbeError::Terminal("outage_allocation_contract_mismatch"));
        }

        match client.pin_status_probe(cid).await {
            Ok(PinStatusObservation::Pending) => return Ok(()),
            Ok(PinStatusObservation::Available(status)) => {
                let status_allocations = sorted_set(&status.allocations);
                if status.cid != cid
                    || status_allocations.len() != status.allocations.len()
                    || status_allocations != expected
                    || status.peer_map.keys().any(|id| !expected.contains(id))
                    || status.peer_map.len() > 2
                {
                    return Err(ProbeError::Terminal("outage_pin_status_contract_mismatch"));
                }
                let documented_states = [
                    "cluster_error",
                    "pin_error",
                    "unpin_error",
                    "error",
                    "pinned",
                    "pinning",
                    "unpinning",
                    "unpinned",
                    "remote",
                    "pin_queued",
                    "unpin_queued",
                    "queued",
                    "sharded",
                    "unexpectedly_unpinned",
                ];
                if status
                    .peer_map
                    .values()
                    .any(|info| !documented_states.contains(&info.status.as_str()))
                {
                    return Err(ProbeError::Terminal("outage_unknown_tracker_state"));
                }
                if status.peer_map.values().any(|info| !info.error.is_empty()) {
                    return Ok(());
                }
                let pinned = sorted_set(
                    &status
                        .peer_map
                        .iter()
                        .filter(|(_, info)| info.status == "pinned" && info.error.is_empty())
                        .map(|(peer, _)| peer.clone())
                        .collect::<Vec<_>>(),
                );
                if pinned != expected {
                    return Ok(());
                }
            }
            Err(ProbeError::Transient(_)) => {}
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        }
        wait_or_timeout(deadline, "outage_observation_timeout").await?;
    }
}

pub async fn kubo_cat(endpoint: &str, cid: &str) -> Result<Vec<u8>> {
    let endpoint = validate_loopback_http(endpoint, "kubo_endpoint_not_loopback_http")?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(CALL_TIMEOUT)
        .build()
        .map_err(|_| anyhow!("kubo_client_build_failed"))?;
    let mut url = Url::parse(&format!("{endpoint}/api/v0/cat"))
        .map_err(|_| anyhow!("kubo_cat_url_invalid"))?;
    url.query_pairs_mut().append_pair("arg", cid);
    let response = client
        .post(url)
        .send()
        .await
        .map_err(|_| anyhow!("kubo_cat_request_transport"))?;
    ensure!(
        response.status().is_success(),
        "kubo_cat_status_not_success"
    );
    response
        .bytes()
        .await
        .map_err(|_| anyhow!("kubo_cat_response_body_transport"))
        .map(|body| body.to_vec())
}

impl RecoveryState {
    pub fn write_claimed(&self, path: &Path) -> Result<()> {
        ensure!(path.is_absolute(), "recovery_state_path_not_absolute");
        self.validate()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| anyhow!("recovery_state_serialize_failed"))?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|_| anyhow!("recovery_state_open_failed"))?;
        let metadata = file
            .metadata()
            .map_err(|_| anyhow!("recovery_state_metadata_failed"))?;
        ensure!(
            metadata.is_file() && metadata.len() == 0,
            "recovery_state_not_claimed_empty_regular_file"
        );
        file.write_all(&bytes)
            .map_err(|_| anyhow!("recovery_state_write_failed"))?;
        file.sync_all()
            .map_err(|_| anyhow!("recovery_state_sync_failed"))
    }

    pub fn read(path: &Path) -> Result<Self> {
        ensure!(path.is_absolute(), "recovery_state_path_not_absolute");
        let bytes = std::fs::read(path).map_err(|_| anyhow!("recovery_state_read_failed"))?;
        let state: Self =
            serde_json::from_slice(&bytes).map_err(|_| anyhow!("recovery_state_decode_failed"))?;
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == "ipfs3-cluster-recovery-v1",
            "recovery_state_schema_invalid"
        );
        ensure!(
            self.source == "s3-replication-retention-v1",
            "recovery_state_source_invalid"
        );
        ensure!(
            !self.cid.is_empty() && !self.body.is_empty(),
            "recovery_state_cid_or_body_empty"
        );
        ensure!(
            is_hex_digest(&self.peer_set_sha256),
            "recovery_state_peer_digest_invalid"
        );
        Ok(())
    }
}

async fn wait_or_timeout(
    deadline: tokio::time::Instant,
    timeout_category: &'static str,
) -> ProbeResult<()> {
    if tokio::time::Instant::now() >= deadline {
        return Err(ProbeError::Terminal(timeout_category));
    }
    tokio::time::sleep(POLL_INTERVAL).await;
    Ok(())
}

fn is_hex_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_release_1_1_6_version(version: &str) -> bool {
    let Some(metadata) = version.strip_prefix("1.1.6+") else {
        return version == "1.1.6";
    };
    !metadata.is_empty()
        && metadata.split('.').all(|identifier| {
            !identifier.is_empty()
                && identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

#[test]
fn release_version_validator_accepts_exact_release_and_build_metadata() {
    for version in [
        "1.1.6",
        "1.1.6+git2182e",
        "1.1.6+git2182e.20260824",
        "1.1.6+build-7",
    ] {
        assert!(is_release_1_1_6_version(version));
    }
    for version in [
        "1.1.6-*",
        "1.1.7",
        "1.1.6+",
        "1.1.6+.build",
        "1.1.6+build.",
        "1.1.6+build..meta",
        "1.1.6+build meta",
        "1.1.6+build/meta",
        "1.1.6+build_meta",
        "1.1.6+*",
    ] {
        assert!(!is_release_1_1_6_version(version));
    }
}
