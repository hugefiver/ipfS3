//! Explicit legacy file archival has no configuration or database dependencies.
use super::{files, input};
use anyhow::Result;
use ipfs_s3_gateway::pinning::management::wire::{MAX_WIRE_BYTES, conversion, v3::Projection};
use serde_json::{Value, json};

pub fn execute(path: &str, output: &str) -> Result<Value> {
    let file = std::fs::File::open(path).map_err(|_| input::invalid())?;
    let metadata = file.metadata().map_err(|_| input::invalid())?;
    if !metadata.is_file() || metadata.len() > MAX_WIRE_BYTES as u64 {
        return Err(input::invalid());
    }
    let mut budget = files::stream::budget();
    let original = conversion::read_v2(file, &mut budget).map_err(|_| input::invalid())?;
    // Input validation finishes before the new protected output is reserved.
    let writer = files::stream::writer(output, Projection::PrivateRecovery, budget)?;
    let (file, budget) = original
        .write_archive(writer)
        .map_err(|_| input::invalid())?;
    file.sync_all().map_err(|_| input::invalid())?;
    budget.check().map_err(|_| input::invalid())?;
    Ok(
        json!({"mode":"convert_v2","schema_version":3,"database_writes":false,
        "artifact_exported":true,"artifact_trust":"untrusted_claims_only",
        "archive_only":true,"private_recovery":true,
        "source_rpc":"not_performed","provider_rpc":"not_performed"}),
    )
}
