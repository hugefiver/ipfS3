use anyhow::Result;
use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::management::{evidence, local_config::ManagementConfig},
    store::pinning::management::records::LocalAuthorization,
};
use sea_orm::DatabaseConnection;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub async fn execute(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    client: &KuboClient,
    request: &evidence::PrepareRequest,
    cancellation: CancellationToken,
) -> Result<Value> {
    let proof = evidence::prepare(db, config, auth, client, request, cancellation).await?;
    // Never serialize the attestation's tuples, identity or private source config.
    Ok(json!({
        "mode": "prepare", "database_writes": true, "provider_rpc": "not_performed",
        "source_rpc": "explicit_offline_local_proof", "proof": proof.proof,
        "stored_bytes": proof.stored_bytes, "observed_at": proof.observed_at, "expires_at": proof.expires_at,
        "upload_size_proven": proof.proof == evidence::ReadProof::FileCleanEof && proof.stored_bytes.is_some(),
        "physical_release_bytes": 0,
    }))
}
