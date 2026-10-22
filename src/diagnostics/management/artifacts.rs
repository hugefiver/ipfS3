//! File-only commands consume frozen v3 bytes without Config or a database.
use super::{Command, files, input};
use anyhow::Result;
use ipfs_s3_gateway::pinning::management::wire::v3::Projection;
use serde_json::{Value, json};

#[path = "conversion.rs"]
mod conversion;

pub fn execute(command: &Command) -> Result<Value> {
    if let Command::ConvertV2 { artifact, output } = command {
        return conversion::execute(artifact, output);
    }
    let (mode, path, output, private_input, private_recovery) = match command {
        Command::Review {
            artifact,
            private_input,
        } => ("review", artifact, None, *private_input, false),
        Command::Export {
            artifact,
            output,
            private_input,
            private_recovery,
        } => (
            "export",
            artifact,
            Some(output),
            *private_input,
            *private_recovery,
        ),
        Command::Import {
            artifact,
            output,
            private_input,
            private_recovery,
        } => (
            "import",
            artifact,
            Some(output),
            *private_input,
            *private_recovery,
        ),
        _ => return Err(input::invalid()),
    };
    let source = if private_input {
        Projection::PrivateRecovery
    } else {
        Projection::Review
    };
    let target = if private_recovery {
        Projection::PrivateRecovery
    } else {
        Projection::Review
    };
    let (head, staged) =
        files::stream::stage_temporary(path, source, files::stream::budget())?.inspect()?;
    if let Some(output) = output {
        files::stream::export_archive(staged.cursor()?, output, target)?;
    }
    Ok(
        json!({"mode":mode,"schema_version":3,"database_writes":false,"artifact_exported":output.is_some(),"artifact_trust":"untrusted_claims_only","private_recovery":private_recovery,"source_rpc":"not_performed","provider_rpc":"not_performed","summary":head.summary,"items":head.items.iter().enumerate().map(|(i,item)|json!({"item_index":i,"status":item.status,"step":item.step})).collect::<Vec<_>>()}),
    )
}
