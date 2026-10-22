//! Explicit maintenance dispatch; never constructs Config or AppState and never
//! enters the ordinary management authorization/collector/worker paths.
use super::{UpgradeCommand, database, files};
use anyhow::{Result, anyhow};
use ipfs_s3_gateway::store::initialization::{self, UpgradeRequest};
use serde_json::json;

pub async fn run(command: UpgradeCommand) -> Result<()> {
    // This connector opens an existing target only: no Store bootstrap and no
    // create-if-missing. The storage API verifies all four bindings under lock.
    let db = database::connect(&command.database_url, false).await?;
    let result = initialization::upgrade_existing(
        &db,
        UpgradeRequest {
            native_identity: &command.native_identity,
            database_scope: &command.database_scope,
            source_version: &command.source_version,
            target_version: &command.target_version,
        },
    )
    .await
    .map_err(|_| anyhow!("management upgrade unavailable (details withheld)"));
    let closed = db.close().await.map_err(|_| database::unavailable());
    result?;
    closed?;
    files::print(&json!({
        "mode": "management_upgrade",
        "upgraded": true,
        "source_version": command.source_version,
        "target_version": command.target_version,
        "database_writes": true,
        "source_rpc": "not_performed",
        "provider_rpc": "not_performed"
    }))
    .await
}
