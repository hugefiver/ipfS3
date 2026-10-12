//! Management accounting basis from original trusted evidence and receipts.
//! This is root-local application hold, never physical release or provider billing.
use super::*;
use crate::pinning::management::{evidence::{ReadAttestation,ReadProof},ReadEvidence};
mod proofs;
mod reservations;
pub(crate) use proofs::{current_proof,trusted_proof};
pub(crate) use reservations::check_frame;

#[derive(Clone,Copy,PartialEq,Eq)]
pub(crate) enum Basis {FilePayload,DirectoryDagBlocksV1}
#[derive(Clone,Copy,PartialEq,Eq)]
pub(crate) struct Count {pub basis:Basis,pub bytes:u64}
pub(crate) fn count(proof:&ReadEvidence)->Result<Count> {
    let basis=match proof.proof {ReadProof::FileCleanEof=>Basis::FilePayload,ReadProof::CompleteLocalDirectoryDagBlocksV1=>Basis::DirectoryDagBlocksV1,ReadProof::CompleteLocalDirectory=>return Err(ManagementError::InvalidEvidence)};
    let bytes=proof.stored_bytes.ok_or(ManagementError::InvalidEvidence)?;
    if basis==Basis::DirectoryDagBlocksV1&&bytes==0{return Err(ManagementError::InvalidEvidence);}
    Ok(Count {basis,bytes})
}
