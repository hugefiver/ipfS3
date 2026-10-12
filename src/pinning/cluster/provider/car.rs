//! Incremental single-root CARv1 validation. Payload buffering is one bounded
//! block; the bounded CID/link graph proves closure, not just the CAR header.
use super::{ClusterTransferLimits, ClusterWriteFailure as Failure, cbor, dagpb};
use cid::Cid;
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy)]
pub(crate) struct DirectoryCarLimits {
    pub max_payload_bytes: u64,
    pub max_car_bytes: u64,
    pub max_block_bytes: usize,
    pub max_dag_metadata_bytes: usize,
    pub max_blocks: u64,
}

#[derive(Clone, Copy)]
pub(crate) enum DirectoryRootKind { Directory, HamtShard }

pub(crate) struct DirectoryCarReceipt {
    pub stored_block_bytes: u64,
    pub serialized_car_bytes: u64,
    pub block_count: u64,
    pub root_kind: DirectoryRootKind,
}

struct StructuralLimits {
    max_block_bytes: usize,
    max_car_bytes: u64,
    max_dag_metadata_bytes: usize,
}

pub(crate) struct Car {
    root: Cid,
    limits: StructuralLimits,
    directory_limits: Option<DirectoryCarLimits>,
    root_kind: Option<DirectoryRootKind>,
    header: bool,
    length: Option<usize>,
    pending: Vec<u8>,
    graph: BTreeMap<Cid, Vec<Cid>>,
    total: u64,
    metadata: usize,
    payload_bytes: u64,
    block_count: u64,
}
impl Car {
    pub fn new(root: &str, limits: ClusterTransferLimits) -> Result<Self, Failure> {
        Self::with_limits(root, StructuralLimits {
            max_block_bytes: limits.max_block_bytes,
            max_car_bytes: limits.max_car_bytes,
            max_dag_metadata_bytes: limits.max_dag_metadata_bytes,
        }, None)
    }
    pub(crate) fn new_directory(root: &str, limits: DirectoryCarLimits) -> Result<Self, Failure> {
        Self::with_limits(root, StructuralLimits {
            max_block_bytes: limits.max_block_bytes,
            max_car_bytes: limits.max_car_bytes,
            max_dag_metadata_bytes: limits.max_dag_metadata_bytes,
        }, Some(limits))
    }
    fn with_limits(root: &str, limits: StructuralLimits, directory_limits: Option<DirectoryCarLimits>) -> Result<Self, Failure> {
        let root = Cid::try_from(root).map_err(|_| cbor::invalid())?;
        Ok(Self {
            root: cbor::canonical(root),
            limits,
            directory_limits,
            root_kind: None,
            header: false,
            length: None,
            pending: Vec::new(),
            graph: BTreeMap::new(),
            total: 0,
            metadata: 0,
            payload_bytes: 0,
            block_count: 0,
        })
    }
    pub fn feed(&mut self, mut bytes: &[u8]) -> Result<(), Failure> {
        self.total = self
            .total
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= self.limits.max_car_bytes)
            .ok_or_else(cbor::invalid)?;
        while !bytes.is_empty() {
            if let Some(length) = self.length {
                let take = (length - self.pending.len()).min(bytes.len());
                self.pending.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                if self.pending.len() == length {
                    let record = std::mem::take(&mut self.pending);
                    self.record(&record)?;
                    self.length = None;
                }
            } else {
                self.pending.push(bytes[0]);
                bytes = &bytes[1..];
                if let Some((length, _)) = dagpb::varint(&self.pending)? {
                    let limit = if self.header {
                        self.limits.max_block_bytes.saturating_add(128)
                    } else {
                        64 * 1024
                    };
                    let length: usize = length.try_into().map_err(|_| cbor::invalid())?;
                    if length == 0 || length > limit {
                        return Err(cbor::invalid());
                    }
                    self.pending.clear();
                    self.length = Some(length);
                }
            }
        }
        Ok(())
    }
    fn record(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        if !self.header {
            cbor::header(bytes, self.root)?;
            self.header = true;
            return Ok(());
        }
        let mut cursor = std::io::Cursor::new(bytes);
        let cid = cbor::canonical(Cid::read_bytes(&mut cursor).map_err(|_| cbor::invalid())?);
        let payload = &bytes[cursor.position() as usize..];
        if payload.len() > self.limits.max_block_bytes || self.graph.contains_key(&cid) {
            return Err(cbor::invalid());
        }
        let digest = cid.hash().digest();
        let valid = match cid.hash().code() {
            0 => digest == payload,
            0x12 => digest == Sha256::digest(payload).as_slice(),
            0x13 => digest == Sha512::digest(payload).as_slice(),
            _ => false,
        };
        if !valid {
            return Err(cbor::invalid());
        }
        let block_count = self.block_count.checked_add(1).ok_or_else(cbor::invalid)?;
        let payload_bytes = self.payload_bytes
            .checked_add(u64::try_from(payload.len()).map_err(|_| cbor::invalid())?)
            .ok_or_else(cbor::invalid)?;
        if self.directory_limits.is_some_and(|limits| block_count > limits.max_blocks || payload_bytes > limits.max_payload_bytes) {
            return Err(cbor::invalid());
        }
        let links = match cid.codec() {
            0x55 => Vec::new(),
            0x70 => dagpb::links(payload)?,
            0x71 => cbor::links(payload)?,
            _ => return Err(cbor::invalid()),
        };
        let record_metadata = links.len().checked_mul(128)
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(cbor::invalid)?;
        self.metadata = self
            .metadata
            .checked_add(record_metadata)
            .filter(|n| *n <= self.limits.max_dag_metadata_bytes)
            .ok_or_else(cbor::invalid)?;
        if self.directory_limits.is_some() && cid == self.root {
            if cid.codec() != 0x70 { return Err(cbor::invalid()); }
            self.root_kind = Some(directory_root_kind(payload)?);
        }
        self.graph.insert(cid, links);
        self.payload_bytes = payload_bytes;
        self.block_count = block_count;
        Ok(())
    }
    pub fn finish(&self) -> Result<(), Failure> {
        if !self.header || self.length.is_some() || !self.pending.is_empty() {
            return Err(cbor::invalid());
        }
        let mut visited = BTreeSet::new();
        let mut frontier = vec![self.root];
        while let Some(cid) = frontier.pop() {
            if !visited.insert(cid) {
                continue;
            }
            let links = self.graph.get(&cid).ok_or_else(cbor::invalid)?;
            frontier.extend(links.iter().copied().filter(|link| !visited.contains(link)));
        }
        if visited.len() != self.graph.len() {
            return Err(cbor::invalid());
        }
        Ok(())
    }
    /// A byte receipt is inaccessible until the same single-root closure check
    /// used by Cluster succeeds. Duplicate CAR records remain rejected; shared
    /// links in the graph therefore count each canonical block exactly once.
    pub(crate) fn finish_directory(&self) -> Result<DirectoryCarReceipt, Failure> {
        self.finish()?;
        Ok(DirectoryCarReceipt {
            stored_block_bytes: self.payload_bytes,
            serialized_car_bytes: self.total,
            block_count: self.block_count,
            root_kind: self.root_kind.ok_or_else(cbor::invalid)?,
        })
    }
}

fn number(bytes: &mut &[u8]) -> Result<u64, Failure> {
    let (value, length) = dagpb::varint(bytes)?.ok_or_else(cbor::invalid)?;
    *bytes = &bytes[length..];
    Ok(value)
}
fn data<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], Failure> {
    let length = usize::try_from(number(bytes)?).map_err(|_| cbor::invalid())?;
    if length > bytes.len() { return Err(cbor::invalid()); }
    let value = &bytes[..length];
    *bytes = &bytes[length..];
    Ok(value)
}

/// The hashed PBNode has already passed dagpb::links. Inspect its actual UnixFS
/// Data message, not files/stat Type or a caller's directory flag. No payload is
/// retained after this one bounded block is parsed.
fn directory_root_kind(mut node: &[u8]) -> Result<DirectoryRootKind, Failure> {
    let mut unixfs = None;
    while !node.is_empty() {
        match number(&mut node)? {
            10 if unixfs.is_none() => unixfs = Some(data(&mut node)?),
            18 => { data(&mut node)?; },
            _ => return Err(cbor::invalid()),
        }
    }
    let mut bytes = unixfs.ok_or_else(cbor::invalid)?;
    let mut seen = BTreeSet::new();
    let mut kind = None;
    let mut bitmap = None;
    let mut hash = None;
    let mut fanout = None;
    while !bytes.is_empty() {
        let tag = number(&mut bytes)?;
        if !seen.insert(tag) { return Err(cbor::invalid()); }
        match tag {
            8 => kind = Some(number(&mut bytes)?),
            18 => bitmap = Some(data(&mut bytes)?.len()),
            40 => hash = Some(number(&mut bytes)?),
            48 => fanout = Some(number(&mut bytes)?),
            56 => { number(&mut bytes)?; }, // mode
            66 => { validate_mtime(data(&mut bytes)?)?; },
            _ => return Err(cbor::invalid()), // file size / file blocks are not directory fields
        }
    }
    match kind {
        Some(1) if bitmap.unwrap_or(0) == 0 && hash.is_none() && fanout.is_none() => Ok(DirectoryRootKind::Directory),
        Some(5) if hash == Some(0x22) && fanout.is_some_and(|n| n >= 8 && n.is_power_of_two())
            && (bitmap.unwrap_or(0) as u64) <= fanout.unwrap_or(0) / 8 => Ok(DirectoryRootKind::HamtShard),
        _ => Err(cbor::invalid()),
    }
}

fn validate_mtime(mut bytes: &[u8]) -> Result<(), Failure> {
    let mut seconds = false;
    let mut nanos = false;
    while !bytes.is_empty() {
        match number(&mut bytes)? {
            8 if !seconds => { number(&mut bytes)?; seconds = true; },
            21 if !nanos && bytes.len() >= 4 => {
                let value = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| cbor::invalid())?);
                if value >= 1_000_000_000 { return Err(cbor::invalid()); }
                bytes = &bytes[4..]; nanos = true;
            },
            _ => return Err(cbor::invalid()),
        }
    }
    if !seconds { return Err(cbor::invalid()); }
    Ok(())
}
