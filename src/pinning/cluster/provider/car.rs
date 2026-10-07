//! Incremental single-root CARv1 validation. Payload buffering is one bounded
//! block; the bounded CID/link graph proves closure, not just the CAR header.
use super::{ClusterTransferLimits, ClusterWriteFailure as Failure, cbor, dagpb};
use cid::Cid;
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Car {
    root: Cid,
    limits: ClusterTransferLimits,
    header: bool,
    length: Option<usize>,
    pending: Vec<u8>,
    graph: BTreeMap<Cid, Vec<Cid>>,
    total: u64,
    metadata: usize,
}
impl Car {
    pub fn new(root: &str, limits: ClusterTransferLimits) -> Result<Self, Failure> {
        let root = Cid::try_from(root).map_err(|_| cbor::invalid())?;
        Ok(Self {
            root: cbor::canonical(root),
            limits,
            header: false,
            length: None,
            pending: Vec::new(),
            graph: BTreeMap::new(),
            total: 0,
            metadata: 0,
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
        let links = match cid.codec() {
            0x55 => Vec::new(),
            0x70 => dagpb::links(payload)?,
            0x71 => cbor::links(payload)?,
            _ => return Err(cbor::invalid()),
        };
        self.metadata = self
            .metadata
            .checked_add(512 + links.len().saturating_mul(128))
            .filter(|n| *n <= self.limits.max_dag_metadata_bytes)
            .ok_or_else(cbor::invalid)?;
        self.graph.insert(cid, links);
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
}
