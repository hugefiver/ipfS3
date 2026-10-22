use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::{
    self, ExactObject, PlanRequest, PublicVersionId, evidence,
    wire::v3::{self, Projection, document::DocumentHead},
};
use serde::{Deserialize, de::DeserializeOwned};
use std::io::{self, Read};

use super::{Command, files};

pub fn invalid() -> anyhow::Error {
    anyhow!("management input invalid (details withheld)")
}

pub enum Input {
    Request(PlanRequest),
    Imported(Box<ImportedFile>),
    Prepare(evidence::PrepareRequest),
}
/// Claims and protected original bytes only. It cannot manufacture a DB binding.
pub struct ImportedFile {
    pub head: DocumentHead,
    pub staged: files::stream::StagedInput,
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
                .head
                .configuration
                .as_ref()
                .and_then(|configuration| configuration.source_node.as_deref()),
            Self::Prepare(request) => Some(&request.expected_node),
        }
    }
}

struct Bounded<R> {
    source: R,
    left: usize,
}
impl<R: Read> Read for Bounded<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let limit = bytes.len().min(self.left.saturating_add(1));
        let n = self.source.read(&mut bytes[..limit])?;
        self.left = self
            .left
            .checked_sub(n)
            .ok_or_else(|| io::Error::other("management input limit"))?;
        Ok(n)
    }
}
fn document<T: DeserializeOwned>(path: &str) -> Result<T> {
    let file = std::fs::File::open(path).map_err(|_| invalid())?;
    let metadata = file.metadata().map_err(|_| invalid())?;
    if !metadata.is_file() || metadata.len() > v3::FRAME_BYTES as u64 {
        return Err(invalid());
    }
    let source = std::io::BufReader::new(Bounded {
        source: file,
        left: v3::FRAME_BYTES,
    });
    let mut parser = serde_json::Deserializer::from_reader(source);
    let result = T::deserialize(&mut parser).map_err(|_| invalid())?;
    parser.end().map_err(|_| invalid())?;
    Ok(result)
}

pub async fn load(command: &Command) -> Result<Input> {
    let command = command.clone();
    tokio::task::spawn_blocking(move || load_blocking(&command))
        .await
        .map_err(|_| invalid())?
}
fn load_blocking(command: &Command) -> Result<Input> {
    match command {
        Command::DryRun { request, .. } | Command::Confirm { request, .. } => {
            let request: PlanRequest = document(request)?;
            if matches!(command, Command::Confirm { .. }) || command.checkpoint().is_some() {
                management::validate_request(&request).map_err(|_| invalid())?;
            }
            if command.checkpoint().is_some()
                && !request
                    .items
                    .iter()
                    .any(|item| matches!(item.subject, management::Subject::Provider { .. }))
            {
                return Err(invalid());
            }
            // Deserialization checks historical data bounds. Unrecorded requests
            // still pass the fresh planner's current execution limits.
            Ok(Input::Request(request))
        }
        Command::Apply { confirmed, .. } | Command::Status { confirmed } => {
            let (head, staged) = files::stream::stage_temporary(
                confirmed,
                Projection::Review,
                files::stream::budget(),
            )?
            .inspect()?;
            if head.request.is_none()
                || head.acknowledgements.is_none()
                || head.expires_at.is_none()
                || head.applied_claims
            {
                return Err(invalid());
            }
            Ok(Input::Imported(Box::new(ImportedFile { head, staged })))
        }
        Command::Prepare { request } => {
            let input: Preparation = document(request)?;
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
        Command::Help
        | Command::Review { .. }
        | Command::Export { .. }
        | Command::ConvertV2 { .. }
        | Command::Import { .. } => Err(invalid()),
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
