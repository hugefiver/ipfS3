use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::{
    self, ExactObject, PlanRequest, PublicVersionId, evidence, wire,
};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::io::AsyncReadExt;

use super::{Command, json_bounds};

pub fn invalid() -> anyhow::Error {
    anyhow!("management input invalid (details withheld)")
}

pub enum Input {
    Request(PlanRequest),
    Imported(Box<wire::ImportedPlan>),
    Prepare(evidence::PrepareRequest),
}

impl Input {
    pub fn source_node<'a>(&'a self, command: &'a Command) -> Option<&'a str> {
        match self {
            Self::Request(_) => match command {
                Command::DryRun { source_node, .. } | Command::Confirm { source_node, .. } => {
                    source_node.as_deref()
                }
                _ => None,
            },
            Self::Imported(imported) => imported
                .snapshot_without_applied()
                .configuration
                .as_ref()
                .and_then(|configuration| configuration.source_node.as_deref()),
            Self::Prepare(request) => Some(&request.expected_node),
        }
    }
}

async fn bytes(path: &str) -> Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await.map_err(|_| invalid())?;
    let metadata = file.metadata().await.map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() > wire::MAX_WIRE_BYTES as u64 {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    file.take((wire::MAX_WIRE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| invalid())?;
    if bytes.len() > wire::MAX_WIRE_BYTES {
        return Err(invalid());
    }
    Ok(bytes)
}

async fn document<T: DeserializeOwned>(path: &str) -> Result<T> {
    let bytes = bytes(path).await?;
    json_bounds::check(&bytes)?;
    // Deserialize directly: a Value intermediary would discard duplicate fields.
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}

pub async fn load(command: &Command) -> Result<Input> {
    match command {
        Command::DryRun { request, .. } | Command::Confirm { request, .. } => {
            let request: PlanRequest = document(request).await?;
            management::validate_request(&request).map_err(|_| invalid())?;
            Ok(Input::Request(request))
        }
        Command::Apply { confirmed, .. } | Command::Status { confirmed } => {
            let imported = wire::decode_plan(&bytes(confirmed).await?).map_err(|_| invalid())?;
            if imported.confirmation().is_none() || !imported.untrusted_applied_claims().is_empty()
            {
                return Err(invalid());
            }
            Ok(Input::Imported(Box::new(imported)))
        }
        Command::Prepare { request } => {
            let input: Preparation = document(request).await?;
            input.validate()?;
            Ok(Input::Prepare(evidence::PrepareRequest {
                record_id: input.record_id,
                identity: input.identity,
                expected_node: input.expected_node,
                max_stored_bytes: input.max_stored_bytes,
                ttl: chrono::Duration::seconds(i64::from(input.ttl_seconds)),
                directory: input.directory,
            }))
        }
        Command::Help => Err(invalid()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    record_id: String,
    identity: ExactObject,
    expected_node: String,
    max_stored_bytes: u64,
    ttl_seconds: u32,
    directory: bool,
}

fn text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn identifier(value: &str, maximum: usize) -> bool {
    text(value, maximum)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

impl Preparation {
    fn validate(&self) -> Result<()> {
        let identity = &self.identity;
        if !identifier(&self.record_id, 64)
            || !text(&self.expected_node, 256)
            || self.max_stored_bytes == 0
            || !(1..=3600).contains(&self.ttl_seconds)
            || !text(&identity.bucket, 255)
            || !text(&identity.key, 1024)
            || !identifier(&identity.private_version_id, 256)
            || !identifier(&identity.object_id, 256)
            || !text(&identity.cid, 512)
            || matches!(&identity.public_version, PublicVersionId::Version(id) if !text(id, 256) || id == "null")
        {
            return Err(invalid());
        }
        let canonical = ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&identity.cid)
            .map_err(|_| invalid())?;
        if canonical != identity.cid {
            return Err(invalid());
        }
        Ok(())
    }
}
