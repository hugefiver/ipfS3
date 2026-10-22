//! Management accounting basis from original trusted evidence and receipts.
//! This is root-local application hold, never physical release or provider billing.
use super::*;
use crate::pinning::management::{
    ReadEvidence,
    evidence::{ReadAttestation, ReadProof},
};
mod proofs;
mod reservations;
pub(crate) use proofs::current_proof_from_source;
pub(crate) use proofs::current_proof_with_work;
pub(crate) use proofs::{fresh_original_proof_with_work, trusted_proof_with_work};
pub(crate) use reservations::check_rows_with_work;

pub(crate) async fn check_current_with_work(
    rows: native::NativeFrame,
    provider: String,
    cid: String,
    expected: Count,
    work: records::SharedDecodeBudget,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        reservations::check_rows_with_work(
            records::FrozenRows::Frame(&rows),
            &provider,
            &cid,
            expected,
            work,
        )
    })
    .await
    .map_err(|_| ManagementError::InvalidEvidence)?
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Basis {
    FilePayload,
    DirectoryDagBlocksV1,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Count {
    pub basis: Basis,
    pub bytes: u64,
}
pub(crate) fn count(proof: &ReadEvidence) -> Result<Count> {
    count_fields(proof.proof, proof.stored_bytes)
}
fn attestation_count(proof: &ReadAttestation) -> Result<Count> {
    count_fields(
        proof.proof,
        crate::pinning::management::attested_stored_bytes(proof),
    )
}
fn count_fields(proof: ReadProof, bytes: Option<u64>) -> Result<Count> {
    let basis = match proof {
        ReadProof::FileCleanEof => Basis::FilePayload,
        ReadProof::CompleteLocalDirectoryDagBlocksV1 => Basis::DirectoryDagBlocksV1,
        ReadProof::CompleteLocalDirectory => return Err(ManagementError::InvalidEvidence),
    };
    let bytes = bytes.ok_or(ManagementError::InvalidEvidence)?;
    if basis == Basis::DirectoryDagBlocksV1 && bytes == 0 {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(Count { basis, bytes })
}
