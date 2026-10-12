//! Read-only, single-root offline directory proof. This is not an import, an
//! upload eligibility predicate, or a provider's transfer/billing profile.
use http_body_util::BodyExt as _;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::KuboClient;
use crate::{
    error::{AppError, AppResult},
    pinning::cluster::provider::{CarValidator, DirectoryCarLimits, DirectoryRootKind},
};

const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;
const MAX_BLOCKS: u64 = 65_536;
const MAX_DAG_METADATA_BYTES: usize = 32 * 1024 * 1024;
const MAX_HEADER_BYTES: u64 = 64 * 1024;
const MAX_CID_BYTES: u64 = 128;
const MAX_LENGTH_BYTES: u64 = 10;

/// Version-1 management proof cost envelope. Only max_payload_bytes is a quota
/// byte cap. CAR framing includes a bounded header plus CID/length overhead per
/// block; metadata and block count are separate memory/work safety limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryProofBudgets {
    max_payload_bytes: u64,
    max_car_bytes: u64,
    max_block_bytes: usize,
    max_blocks: u64,
    max_dag_metadata_bytes: usize,
}
impl DirectoryProofBudgets {
    pub fn for_payload(max_payload_bytes: u64) -> AppResult<Self> {
        if max_payload_bytes == 0 { return Err(invalid()); }
        let overhead = MAX_BLOCKS.checked_mul(MAX_CID_BYTES + MAX_LENGTH_BYTES)
            .and_then(|bytes| bytes.checked_add(MAX_HEADER_BYTES + MAX_LENGTH_BYTES))
            .ok_or_else(invalid)?;
        let max_car_bytes = max_payload_bytes.checked_add(overhead).ok_or_else(invalid)?;
        Ok(Self {
            max_payload_bytes,
            max_car_bytes,
            max_block_bytes: usize::try_from(max_payload_bytes.min(MAX_BLOCK_BYTES as u64)).map_err(|_| invalid())?,
            max_blocks: MAX_BLOCKS,
            max_dag_metadata_bytes: MAX_DAG_METADATA_BYTES,
        })
    }
    pub fn max_payload_bytes(&self) -> u64 { self.max_payload_bytes }
    pub fn max_car_bytes(&self) -> u64 { self.max_car_bytes }
    pub fn max_block_bytes(&self) -> usize { self.max_block_bytes }
    pub fn max_blocks(&self) -> u64 { self.max_blocks }
    pub fn max_dag_metadata_bytes(&self) -> usize { self.max_dag_metadata_bytes }
    fn valid(&self) -> bool { Self::for_payload(self.max_payload_bytes).ok().as_ref() == Some(self) }
    fn parser_limits(self) -> DirectoryCarLimits {
        DirectoryCarLimits {
            max_payload_bytes: self.max_payload_bytes, max_car_bytes: self.max_car_bytes,
            max_block_bytes: self.max_block_bytes, max_blocks: self.max_blocks,
            max_dag_metadata_bytes: self.max_dag_metadata_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDirectoryRootKind { Directory, HamtShard }

/// Constructed by the real offline RPC/clean-body/complete-CAR path only. Counts
/// are for this root, not a global shared-block ledger or a physical disk bill.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalDirectoryVerificationReceipt {
    node_identity: String,
    cid: String,
    root_kind: LocalDirectoryRootKind,
    stored_block_bytes: u64,
    serialized_car_bytes: u64,
    block_count: u64,
    budgets: DirectoryProofBudgets,
}
impl LocalDirectoryVerificationReceipt {
    pub fn node_identity(&self) -> &str { &self.node_identity }
    pub fn cid(&self) -> &str { &self.cid }
    pub fn root_kind(&self) -> LocalDirectoryRootKind { self.root_kind }
    pub fn stored_block_bytes(&self) -> u64 { self.stored_block_bytes }
    pub fn serialized_car_bytes(&self) -> u64 { self.serialized_car_bytes }
    pub fn block_count(&self) -> u64 { self.block_count }
    pub fn budgets(&self) -> DirectoryProofBudgets { self.budgets }
    pub(crate) fn valid_shape(&self) -> bool {
        self.budgets.valid() && !self.node_identity.is_empty() && cid::Cid::try_from(self.cid.as_str()).is_ok()
            && self.stored_block_bytes > 0 && self.stored_block_bytes <= self.budgets.max_payload_bytes
            && self.serialized_car_bytes > self.stored_block_bytes && self.serialized_car_bytes <= self.budgets.max_car_bytes
            && self.block_count > 0 && self.block_count <= self.budgets.max_blocks
    }
}

impl KuboClient {
    /// Current source fence for replaying a new directory RID; no CAR re-read or
    /// TTL extension. Unlike legacy reads, redirects cannot substitute a peer.
    pub(crate) async fn directory_proof_node_identity(&self, cancel: &CancellationToken) -> AppResult<String> {
        let control = proof_http(self.stream_idle_timeout(), false)?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(super::canceled_rpc_error()),
            result = offline_node_identity(&control, self.base_url()) => result,
        }
    }
    /// Export the exact requested root with offline=true, never materializing
    /// files or importing blocks. Pin/locality and node checks bracket the body;
    /// root type comes from the hashed UnixFS block, not Kubo's stat fields.
    pub async fn verify_local_directory(
        &self, cid: &str, expected_node: &str, budgets: DirectoryProofBudgets, cancel: CancellationToken,
    ) -> AppResult<LocalDirectoryVerificationReceipt> {
        if !budgets.valid() || self.stream_idle_timeout().is_zero() { return Err(invalid()); }
        let expected = cid::Cid::try_from(cid).map_err(|_| invalid())?;
        let proof = async {
            // Scoped clients deliberately do not follow redirects. Reusing the
            // default client's redirect policy could drop offline=true before
            // the final response URL is checked. No daemon setting is changed.
            let control = proof_http(self.stream_idle_timeout(), false)?;
            let download = proof_http(self.stream_idle_timeout(), true)?;
            let node_identity = offline_node_identity(&control, self.base_url()).await?;
            if node_identity != expected_node { return Err(invalid()); }
            self.verify_recursive_pin(&expected, cid, &control).await?;
            self.verify_complete_local_dag(&expected, cid, &control).await?;
            let mut url = reqwest::Url::parse(&format!("{}/api/v0/dag/export", self.base_url())).map_err(|_| invalid())?;
            url.query_pairs_mut().append_pair("arg", cid).append_pair("offline", "true");
            let response = tokio::time::timeout(self.stream_idle_timeout(), download.post(url).send())
                .await.map_err(|_| invalid())?.map_err(|_| invalid())?;
            if !response.status().is_success() { return Err(AppError::kubo_rpc_status(response.status())); }
            if response.headers().contains_key("x-stream-error") { return Err(invalid()); }
            let length = response.content_length();
            if length.is_some_and(|length| length > budgets.max_car_bytes) { return Err(invalid()); }
            let mut parser = CarValidator::new_directory(cid, budgets.parser_limits()).map_err(|_| invalid())?;
            let mut body: reqwest::Body = response.into();
            loop {
                let frame = match tokio::time::timeout(self.stream_idle_timeout(), body.frame()).await {
                    Ok(Some(Ok(frame))) => frame,
                    Ok(None) => break,
                    Ok(Some(Err(_))) | Err(_) => return Err(invalid()),
                };
                match frame.into_data() {
                    Ok(bytes) => parser.feed(&bytes).map_err(|_| invalid())?,
                    Err(frame) => match frame.into_trailers() {
                        Ok(trailers) if !trailers.contains_key("x-stream-error") => {},
                        Ok(_) | Err(_) => return Err(invalid()),
                    },
                }
            }
            // Even a fully parsed CAR is not authoritative before clean HTTP EOF.
            let receipt = parser.finish_directory().map_err(|_| invalid())?;
            if length.is_some_and(|length| length != receipt.serialized_car_bytes) { return Err(invalid()); }
            self.verify_recursive_pin(&expected, cid, &control).await?;
            if offline_node_identity(&control, self.base_url()).await? != node_identity || cancel.is_cancelled() { return Err(invalid()); }
            Ok(LocalDirectoryVerificationReceipt {
                node_identity, cid: cid.to_owned(),
                root_kind: match receipt.root_kind { DirectoryRootKind::Directory => LocalDirectoryRootKind::Directory, DirectoryRootKind::HamtShard => LocalDirectoryRootKind::HamtShard },
                stored_block_bytes: receipt.stored_block_bytes,
                serialized_car_bytes: receipt.serialized_car_bytes,
                block_count: receipt.block_count, budgets,
            })
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(super::canceled_rpc_error()),
            result = proof => result,
        }
    }
}

fn invalid() -> AppError { AppError::kubo_rpc_detail("incomplete or over-budget offline directory proof") }

fn proof_http(idle: std::time::Duration, download: bool) -> AppResult<reqwest::Client> {
    if idle.is_zero() { return Err(invalid()); }
    let builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none())
        .connect_timeout(idle.min(std::time::Duration::from_secs(30)));
    let builder = if download { builder.read_timeout(idle) } else { builder.timeout(idle) };
    builder.build().map_err(|_| invalid())
}

async fn offline_node_identity(http: &reqwest::Client, endpoint: &str) -> AppResult<String> {
    #[derive(Deserialize)]
    struct Identity { #[serde(rename = "ID")] id: String }
    let mut url = reqwest::Url::parse(&format!("{endpoint}/api/v0/id")).map_err(|_| invalid())?;
    url.query_pairs_mut().append_pair("peerid-base", "b58mh").append_pair("offline", "true");
    let response: Identity = super::verification::bounded_control_json(http.post(url), "offline directory identity").await?;
    if response.id.is_empty() || response.id.len() > 256 || !response.id.bytes().all(|byte|byte.is_ascii_alphanumeric()) { return Err(invalid()); }
    Ok(response.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn car(payload: &[u8]) -> (String, Vec<u8>) {
        let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(payload)).unwrap();
        let cid = cid::Cid::new_v1(0x70, hash);
        let mut header = b"\xa2\x65roots\x81\xd8\x2a\x58\x25\x00".to_vec();
        header.extend(cid.to_bytes());
        header.extend(b"\x67version\x01");
        let mut car = vec![header.len() as u8];
        car.extend(header);
        car.push((cid.to_bytes().len() + payload.len()) as u8);
        car.extend(cid.to_bytes());
        car.extend(payload);
        (cid.to_string(), car)
    }

    #[test]
    fn separate_parser_work_limits_and_typed_root_are_fail_closed() {
        let (cid, car) = car(b"\x0a\x02\x08\x01");
        assert_eq!(car.len(), 100);
        let limits = DirectoryCarLimits {max_payload_bytes:4, max_car_bytes:100, max_block_bytes:4, max_dag_metadata_bytes:512, max_blocks:1};
        let mut parser = CarValidator::new_directory(&cid, limits).unwrap();
        for byte in &car { parser.feed(&[*byte]).unwrap(); }
        let receipt = parser.finish_directory().unwrap();
        assert_eq!(receipt.stored_block_bytes, 4);
        assert_eq!(receipt.serialized_car_bytes, 100);
        assert_eq!(receipt.block_count, 1);
        for limited in [
            DirectoryCarLimits {max_payload_bytes:3, ..limits},
            DirectoryCarLimits {max_car_bytes:99, ..limits},
            DirectoryCarLimits {max_block_bytes:3, ..limits},
            DirectoryCarLimits {max_dag_metadata_bytes:511, ..limits},
            DirectoryCarLimits {max_blocks:0, ..limits},
        ] {
            let mut parser = CarValidator::new_directory(&cid, limited).unwrap();
            assert!(parser.feed(&car).is_err());
            assert!(parser.finish_directory().is_err());
        }
        for payload in [b"\x0a\x02\x08\x02".as_slice(), b"\x0a\x04\x08\x01\x08\x05".as_slice()] {
            let (cid, car) = self::car(payload);
            let mut parser = CarValidator::new_directory(&cid, DirectoryCarLimits {max_payload_bytes:32, max_car_bytes:256, max_block_bytes:32, ..limits}).unwrap();
            assert!(parser.feed(&car).is_err());
            assert!(parser.finish_directory().is_err());
        }
    }

    #[test]
    fn management_budgets_are_explicit_checked_and_not_cluster_defaults() {
        let limits = DirectoryProofBudgets::for_payload(107).unwrap();
        assert_eq!(limits.max_payload_bytes(), 107);
        assert_eq!(limits.max_car_bytes(), 9_109_621);
        assert_eq!(limits.max_block_bytes(), 107);
        assert_eq!(limits.max_blocks(), 65_536);
        assert_eq!(limits.max_dag_metadata_bytes(), 32 * 1024 * 1024);
        assert!(DirectoryProofBudgets::for_payload(0).is_err());
        assert!(DirectoryProofBudgets::for_payload(u64::MAX).is_err());
    }
}
