//! Bounded, bottom-up UnixFS directory construction from a finalized file manifest.
//! Flat dag-pb directories graduate to UnixFS HAMTShard above the block budget.
#[path = "directory_hamt.rs"]
mod hamt;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use base64::Engine as _;
use http_body_util::BodyExt as _;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::{KuboClient, LocalResidencyVerificationReceipt, send_request};
use crate::error::AppError;

pub const MAX_DIRECTORY_BLOCK_BYTES: usize = 2 * 1024 * 1024;
const SHARD_THRESHOLD_BYTES: usize = 256 * 1024;
const MAX_FILES: usize = 10_000;
const MAX_MANIFEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 64;
const MAX_RPC_RESPONSE_BYTES: usize = 64 * 1024;
const BUILD_DEADLINE: Duration = Duration::from_secs(300);
const DIRECTORY_DATA: &[u8] = &[0x08, 0x01]; // protobuf UnixFS Data { Type: Directory (1) }
static BUILD_SLOTS: Semaphore = Semaphore::const_new(4);

/// One final published file, with a trusted, relative ZIP path and its original CID.
/// Duplicate winners must be resolved by the caller before invoking this builder.
#[derive(Clone, Debug)]
pub struct DirectoryFile {
    pub path: String,
    pub cid: String,
}

#[derive(Clone, Debug)]
pub struct DirectoryRoot {
    pub cid: String,
    pub local_residency: LocalResidencyVerificationReceipt,
}

/// An emitted root on this node, not a local-residency receipt or a publishable root.
/// A caller may retain this CID for later reconciliation, never mark it verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryCandidate {
    pub cid: String,
    pub node_identity: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DirectoryBuildError {
    #[error("path_conflict")]
    PathConflict,
    #[error("invalid_manifest")]
    InvalidManifest,
    #[error("directory_block_too_large")]
    BlockTooLarge,
    #[error("directory_hamt_hash_collision")]
    HashCollision,
    #[error("directory_build_canceled")]
    Canceled,
    #[error("directory_build_deadline_exceeded")]
    Deadline,
    #[error("directory_kubo_failure")]
    Kubo(#[from] AppError),
    #[error("directory_build_failed_after_root: {reason}")]
    KnownRoot {
        candidate: DirectoryCandidate,
        #[source]
        reason: Box<DirectoryBuildError>,
    },
}

impl DirectoryBuildError {
    pub fn candidate(&self) -> Option<&DirectoryCandidate> {
        match self {
            Self::KnownRoot { candidate, .. } => Some(candidate),
            _ => None,
        }
    }

    pub fn reason(&self) -> &DirectoryBuildError {
        match self {
            Self::KnownRoot { reason, .. } => reason,
            _ => self,
        }
    }
}

type BuildResult<T> = Result<T, DirectoryBuildError>;

#[derive(Clone)]
struct Link {
    name: String,
    cid: cid::Cid,
    size: u64,
}

#[derive(Serialize)]
struct DagJson<'a> {
    #[serde(rename = "Data")]
    data: DagBytes<'a>,
    #[serde(rename = "Links")]
    links: Vec<DagLink<'a>>,
}

#[derive(Serialize)]
struct DagBytes<'a> {
    #[serde(rename = "/")]
    bytes: Base64Bytes<'a>,
}

#[derive(Serialize)]
struct Base64Bytes<'a> {
    bytes: &'a str,
}

#[derive(Serialize)]
struct DagLink<'a> {
    #[serde(rename = "Hash")]
    hash: DagCid<'a>,
    #[serde(rename = "Name")]
    name: &'a str,
    #[serde(rename = "Tsize")]
    size: u64,
}

#[derive(Serialize)]
struct DagCid<'a> {
    #[serde(rename = "/")]
    cid: &'a str,
}

#[derive(Deserialize)]
struct DagStat {
    #[serde(rename = "TotalSize")]
    total_size: Option<serde_json::Value>,
    #[serde(rename = "DagStats")]
    dag_stats: Vec<StatRoot>,
}

#[derive(Deserialize)]
struct StatRoot {
    #[serde(rename = "Cid")]
    cid: String,
}

#[derive(Deserialize)]
struct BlockStat {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Size")]
    size: u64,
}

#[derive(Deserialize)]
struct FileStat {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "CumulativeSize")]
    cumulative_size: u64,
}

#[derive(Deserialize)]
struct PutResponse {
    #[serde(rename = "Cid")]
    cid: OwnedCid,
}

#[derive(Deserialize)]
struct OwnedCid {
    #[serde(rename = "/")]
    value: String,
}

#[derive(Deserialize)]
struct PinResponse {
    #[serde(rename = "Pins")]
    pins: Vec<String>,
}

#[derive(Deserialize)]
struct ResolveResponse {
    #[serde(rename = "Path")]
    path: String,
}

/// Returns `None` for no successful files. A receipt is returned only after
/// complete dag/put replies, recursive pin, per-path resolution, and the local
/// recursive-pin/DAG verification have completed. A failure after a complete
/// root dag/put reply carries an unverified candidate for durable retention.
/// Neither a candidate nor a receipt is a DB publication.
pub async fn build_directory(
    kubo: &KuboClient,
    files: &[DirectoryFile],
    cancel: &CancellationToken,
) -> Result<Option<DirectoryRoot>, DirectoryBuildError> {
    let mut candidate = None;
    build_directory_capturing_candidate(kubo, files, cancel, &mut candidate).await
}

/// Initial publishers keep this slot outside the cancellable/timeout future.
/// It is updated synchronously after the root's complete, validated dag/put
/// reply, so dropping the build cannot discard an already known candidate.
/// The slot is evidence only: retention still requires the original root claim.
pub(crate) async fn build_directory_capturing_candidate(
    kubo: &KuboClient,
    files: &[DirectoryFile],
    cancel: &CancellationToken,
    candidate: &mut Option<DirectoryCandidate>,
) -> Result<Option<DirectoryRoot>, DirectoryBuildError> {
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(DirectoryBuildError::Canceled),
        result = tokio::time::timeout(BUILD_DEADLINE, build(kubo, files, cancel, candidate)) =>
            result.unwrap_or(Err(DirectoryBuildError::Deadline)),
    };
    result.map_err(|reason| match candidate.clone() {
        Some(candidate) => DirectoryBuildError::KnownRoot {
            candidate,
            reason: Box::new(reason),
        },
        None => reason,
    })
}

async fn build(
    kubo: &KuboClient,
    files: &[DirectoryFile],
    cancel: &CancellationToken,
    candidate: &mut Option<DirectoryCandidate>,
) -> BuildResult<Option<DirectoryRoot>> {
    if files.len() > MAX_FILES {
        return Err(DirectoryBuildError::InvalidManifest);
    }
    if files.is_empty() {
        return Ok(None);
    }
    let _slot = tokio::select! {
        _ = cancel.cancelled() => return Err(DirectoryBuildError::Canceled),
        slot = BUILD_SLOTS.acquire() => slot.expect("directory builder semaphore is never closed"),
    };

    let mut used_bytes = 0_usize;
    let mut paths = BTreeSet::<Vec<String>>::new();
    let mut directories = BTreeMap::<Vec<String>, Vec<Link>>::new();
    directories.insert(Vec::new(), Vec::new());
    let mut entries = Vec::with_capacity(files.len());
    for file in files {
        let parts: Vec<String> = file.path.split('/').map(str::to_owned).collect();
        if parts.len() > MAX_DEPTH
            || parts.iter().any(|part| {
                part.is_empty() || part == "." || part == ".." || part.contains(['\\', '\0'])
            })
            || file.path.starts_with('/')
        {
            return Err(DirectoryBuildError::InvalidManifest);
        }
        used_bytes = used_bytes
            .checked_add(file.path.len() + file.cid.len())
            .filter(|bytes| *bytes <= MAX_MANIFEST_BYTES)
            .ok_or(DirectoryBuildError::InvalidManifest)?;
        let cid = parse_cid(&file.cid).map_err(|_| DirectoryBuildError::InvalidManifest)?;
        if !paths.insert(parts.clone()) {
            return Err(DirectoryBuildError::PathConflict);
        }
        for depth in 1..parts.len() {
            let parent = parts[..depth].to_vec();
            if !directories.contains_key(&parent) {
                used_bytes = used_bytes
                    .checked_add(parent.iter().map(String::len).sum::<usize>() + depth * 32 + 128)
                    .filter(|bytes| *bytes <= MAX_MANIFEST_BYTES)
                    .ok_or(DirectoryBuildError::InvalidManifest)?;
                if directories.len() >= MAX_FILES {
                    return Err(DirectoryBuildError::InvalidManifest);
                }
                directories.insert(parent, Vec::new());
            }
        }
        entries.push((parts, cid));
    }
    if paths.iter().any(|path| directories.contains_key(path)) {
        return Err(DirectoryBuildError::PathConflict);
    }

    // Memoize file DAG stats: many published paths can refer to the same CID.
    let mut sizes = BTreeMap::new();
    for (parts, cid) in &entries {
        if !sizes.contains_key(cid) {
            sizes.insert(*cid, file_size(kubo, cid, cancel).await?);
        }
        let name = parts.last().expect("validated non-empty path").clone();
        directories
            .get_mut(&parts[..parts.len() - 1])
            .expect("parent created")
            .push(Link {
                name,
                cid: *cid,
                size: sizes[cid],
            });
    }

    // A sorted list of directory paths, deepest first. Exactly one live RPC
    // call and one directory payload at a time; no per-file tasks or re-adds.
    let mut order: Vec<_> = directories.keys().cloned().collect();
    order.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    let mut root = None;
    for path in order {
        let mut links = directories.remove(&path).expect("directory enumerated");
        links.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        // Establish identity before the root write, so cancellation immediately
        // after its complete response still leaves an attributable candidate.
        let node_identity = if path.is_empty() {
            Some(kubo.local_node_identity().await?)
        } else {
            None
        };
        let (cid, size) = if block_size(&links, DIRECTORY_DATA)?.0 > SHARD_THRESHOLD_BYTES {
            put_shards(kubo, links, cancel).await?
        } else {
            put_node(kubo, &links, DIRECTORY_DATA, cancel).await?
        };
        if path.is_empty() {
            *candidate = Some(DirectoryCandidate {
                cid: cid.to_string(),
                node_identity: node_identity.expect("root identity captured before dag/put"),
            });
            root = Some(cid);
        } else {
            directories
                .get_mut(&path[..path.len() - 1])
                .expect("parent created")
                .push(Link {
                    name: path.last().expect("not root").clone(),
                    cid,
                    size,
                });
        }
    }
    let root = root.expect("nonempty manifest has root");
    let root_cid = root.to_string();
    let url = rpc_url(
        kubo,
        "pin/add",
        &[("arg", &root_cid), ("recursive", "true")],
    )?;
    let pin: PinResponse = rpc_json(kubo, kubo.http().post(url), cancel).await?;
    if pin.pins.len() != 1 || parse_cid(&pin.pins[0])? != root {
        return Err(DirectoryBuildError::Kubo(AppError::kubo_rpc_detail(
            "Kubo directory recursive pin was not confirmed",
        )));
    }

    // Kubo must actually resolve every original leaf by its path; a complete
    // local root alone cannot establish the requested mapping of names to CIDs.
    for (parts, expected) in entries {
        let path = format!("/ipfs/{}/{}", root_cid, parts.join("/"));
        let url = rpc_url(kubo, "resolve", &[("arg", &path)])?;
        let resolved: ResolveResponse = rpc_json(kubo, kubo.http().post(url), cancel).await?;
        let terminal = resolved
            .path
            .strip_prefix("/ipfs/")
            .filter(|suffix| !suffix.contains('/'))
            .ok_or_else(|| AppError::kubo_rpc_detail("invalid Kubo directory path resolution"))?;
        if parse_cid(terminal)? != expected {
            return Err(DirectoryBuildError::Kubo(AppError::kubo_rpc_detail(
                "Kubo directory path did not resolve to the original CID",
            )));
        }
    }
    let local_residency = tokio::select! {
        _ = cancel.cancelled() => return Err(DirectoryBuildError::Canceled),
        result = kubo.verify_local_residency(&root_cid) => result?,
    };
    Ok(Some(DirectoryRoot {
        cid: root_cid,
        local_residency,
    }))
}

fn parse_cid(text: &str) -> BuildResult<cid::Cid> {
    let parsed = cid::Cid::try_from(text).map_err(|_| {
        DirectoryBuildError::Kubo(AppError::kubo_rpc_detail("invalid Kubo directory CID"))
    })?;
    Ok(cid::Cid::new_v1(parsed.codec(), parsed.hash().to_owned()))
}

fn rpc_url(
    kubo: &KuboClient,
    endpoint: &str,
    params: &[(&str, &str)],
) -> BuildResult<reqwest::Url> {
    let mut url = reqwest::Url::parse(&format!("{}/api/v0/{endpoint}", kubo.base_url()))
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo RPC URL"))?;
    url.query_pairs_mut().extend_pairs(params.iter().copied());
    Ok(url)
}

async fn rpc_json<T: serde::de::DeserializeOwned>(
    kubo: &KuboClient,
    request: reqwest::RequestBuilder,
    cancel: &CancellationToken,
) -> BuildResult<T> {
    let bytes = rpc_bytes(kubo, request, cancel, false).await?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo directory response").into())
}

async fn rpc_bytes(
    kubo: &KuboClient,
    request: reqwest::RequestBuilder,
    cancel: &CancellationToken,
    allow_empty_raw_nan: bool,
) -> BuildResult<Vec<u8>> {
    let response = send_request(request, cancel).await?;
    if !response.status().is_success() {
        return Err(AppError::kubo_rpc_status(response.status()).into());
    }
    if response.headers().contains_key("x-stream-error")
        || response
            .content_length()
            .is_some_and(|n| n > MAX_RPC_RESPONSE_BYTES as u64)
    {
        return Err(
            AppError::kubo_rpc_detail("Kubo directory response invalid or too large").into(),
        );
    }
    let operation = response.url().path().to_owned();
    let mut body: reqwest::Body = response.into();
    let mut bytes = Vec::new();
    let mut saw_empty_raw_nan = false;
    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => return Err(DirectoryBuildError::Canceled),
            frame = tokio::time::timeout(kubo.stream_idle_timeout(), body.frame()) =>
                frame.map_err(|_| AppError::kubo_rpc_detail("Kubo directory response stalled"))?,
        };
        let Some(frame) = frame else { break };
        let frame = frame
            .map_err(|_| AppError::kubo_rpc_detail("Kubo directory response stream failed"))?;
        match frame.into_data() {
            Ok(data) => {
                if bytes.len().saturating_add(data.len()) > MAX_RPC_RESPONSE_BYTES {
                    return Err(
                        AppError::kubo_rpc_detail("Kubo directory response too large").into(),
                    );
                }
                bytes.extend_from_slice(&data);
            }
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers()
                    && let Some(error) = trailers.get("x-stream-error")
                {
                    if allow_empty_raw_nan
                        && !saw_empty_raw_nan
                        && bytes.is_empty()
                        && error == "json: unsupported value: NaN"
                    {
                        saw_empty_raw_nan = true;
                        continue;
                    }
                    return Err(AppError::kubo_rpc_detail(format!(
                        "Kubo directory {operation} response stream error"
                    ))
                    .into());
                }
            }
        }
    }
    if saw_empty_raw_nan && !bytes.is_empty() {
        return Err(AppError::kubo_rpc_detail("invalid Kubo empty raw DAG stat response").into());
    }
    Ok(bytes)
}

async fn dag_size(
    kubo: &KuboClient,
    cid: &cid::Cid,
    cancel: &CancellationToken,
) -> BuildResult<u64> {
    let cid_text = cid.to_string();
    let url = rpc_url(
        kubo,
        "dag/stat",
        &[("arg", &cid_text), ("progress", "false")],
    )?;
    // Kubo 0.43 emits no JSON at all for an empty raw leaf (NaN size).
    let bytes = rpc_bytes(kubo, kubo.http().post(url), cancel, cid.codec() == 0x55).await?;
    if !bytes.is_empty() {
        let stat: DagStat = serde_json::from_slice(&bytes)
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo directory DAG stat"))?;
        if stat.dag_stats.len() != 1 || parse_cid(&stat.dag_stats[0].cid)? != *cid {
            return Err(AppError::kubo_rpc_detail("Kubo directory DAG stat CID mismatch").into());
        }
        if let Some(size) = stat.total_size.as_ref().and_then(serde_json::Value::as_u64) {
            return Ok(size);
        }
        if cid.codec() != 0x55
            || !matches!(
                stat.total_size.as_ref().and_then(serde_json::Value::as_str),
                None | Some("NaN")
            )
        {
            return Err(AppError::kubo_rpc_detail("invalid Kubo directory DAG size").into());
        }
    }
    if cid.codec() == 0x55 {
        let url = rpc_url(
            kubo,
            "block/stat",
            &[("arg", &cid_text), ("offline", "true")],
        )?;
        let block: BlockStat = rpc_json(kubo, kubo.http().post(url), cancel).await?;
        if parse_cid(&block.key)? == *cid && block.size == 0 {
            return Ok(0);
        }
    }
    Err(AppError::kubo_rpc_detail("invalid Kubo directory DAG stat").into())
}

async fn file_size(
    kubo: &KuboClient,
    cid: &cid::Cid,
    cancel: &CancellationToken,
) -> BuildResult<u64> {
    let arg = format!("/ipfs/{cid}");
    let url = rpc_url(kubo, "files/stat", &[("arg", &arg), ("offline", "true")])?;
    let stat: BuildResult<FileStat> = rpc_json(kubo, kubo.http().post(url), cancel).await;
    match stat {
        Ok(stat) if parse_cid(&stat.hash)? == *cid => Ok(stat.cumulative_size),
        Ok(_) => Err(AppError::kubo_rpc_detail("Kubo directory file stat CID mismatch").into()),
        // Older or alternate Kubo endpoints may not expose a zero-length raw
        // leaf via files/stat. Keep the strictly checked empty-raw DAG fallback.
        Err(error) if cid.codec() == 0x55 && !matches!(error, DirectoryBuildError::Canceled) => {
            let size = dag_size(kubo, cid, cancel).await?;
            if size == 0 { Ok(size) } else { Err(error) }
        }
        Err(error) => Err(error),
    }
}

fn varint_len(mut number: u64) -> usize {
    let mut bytes = 1;
    while number >= 128 {
        bytes += 1;
        number >>= 7;
    }
    bytes
}

fn field_len(bytes: usize) -> usize {
    1 + varint_len(bytes as u64) + bytes
}

fn block_size(links: &[Link], data: &[u8]) -> BuildResult<(usize, u64)> {
    let mut length = field_len(data.len());
    let mut cumulative = 0_u64;
    for link in links {
        let hash = link.cid.to_bytes().len();
        let pb_link = field_len(hash) + field_len(link.name.len()) + 1 + varint_len(link.size);
        length = length
            .checked_add(field_len(pb_link))
            .ok_or(DirectoryBuildError::BlockTooLarge)?;
        cumulative = cumulative
            .checked_add(link.size)
            .ok_or(DirectoryBuildError::BlockTooLarge)?;
    }
    cumulative = cumulative
        .checked_add(length as u64)
        .ok_or(DirectoryBuildError::BlockTooLarge)?;
    Ok((length, cumulative))
}

async fn put_shards(
    kubo: &KuboClient,
    links: Vec<Link>,
    cancel: &CancellationToken,
) -> BuildResult<(cid::Cid, u64)> {
    let shards = hamt::shards(links)?;
    let mut built = Vec::<(cid::Cid, u64)>::with_capacity(shards.len());
    for shard in shards {
        let mut links = Vec::with_capacity(shard.links.len());
        for entry in shard.links {
            match entry {
                hamt::ShardLink::File(link) => links.push(link),
                hamt::ShardLink::Child { prefix, index } => {
                    let (cid, size) = built[index];
                    links.push(Link {
                        name: prefix,
                        cid,
                        size,
                    });
                }
            }
        }
        let data = hamt::data(&shard.bitmap);
        built.push(put_node(kubo, &links, &data, cancel).await?);
    }
    Ok(*built.last().expect("HAMT has a root"))
}

async fn put_node(
    kubo: &KuboClient,
    links: &[Link],
    data: &[u8],
    cancel: &CancellationToken,
) -> BuildResult<(cid::Cid, u64)> {
    let (length, cumulative) = block_size(links, data)?;
    if length > MAX_DIRECTORY_BLOCK_BYTES {
        return Err(DirectoryBuildError::BlockTooLarge);
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(data);
    let cids: Vec<String> = links.iter().map(|link| link.cid.to_string()).collect();
    let json_links: Vec<_> = links
        .iter()
        .zip(&cids)
        .map(|(link, cid)| DagLink {
            hash: DagCid { cid },
            name: &link.name,
            size: link.size,
        })
        .collect();
    let payload = serde_json::to_vec(&DagJson {
        data: DagBytes {
            bytes: Base64Bytes { bytes: &encoded },
        },
        links: json_links,
    })
    .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo directory payload"))?;
    // JSON overhead can be larger than the dag-pb block; never send unbounded metadata.
    if payload.len() > MAX_MANIFEST_BYTES {
        return Err(DirectoryBuildError::BlockTooLarge);
    }
    let url = rpc_url(
        kubo,
        "dag/put",
        &[
            ("input-codec", "dag-json"),
            ("store-codec", "dag-pb"),
            ("hash", "sha2-256"),
            // Protect every emitted directory/HAMT block immediately, before
            // the next RPC can race with GC. No per-file-leaf pin calls.
            ("pin", "true"),
        ],
    )?;
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(payload).file_name("directory.json"),
    );
    let response: PutResponse =
        rpc_json(kubo, kubo.http().post(url).multipart(form), cancel).await?;
    let reported = cid::Cid::try_from(response.cid.value.as_str())
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo directory CID"))?;
    if reported.version() != cid::Version::V1 {
        return Err(AppError::kubo_rpc_detail("Kubo directory DAG format mismatch").into());
    }
    let cid = parse_cid(&response.cid.value)?;
    if cid.codec() != 0x70 || cid.hash().code() != 0x12 {
        return Err(AppError::kubo_rpc_detail("Kubo directory DAG format mismatch").into());
    }
    Ok((cid, cumulative))
}
