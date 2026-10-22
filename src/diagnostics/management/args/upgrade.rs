use super::operand;
use anyhow::{Result, anyhow};

const HELP: &str = "usage: ipfs-s3-gateway --pinning-manage-upgrade --database-url URL --native-identity ID --database-scope SCOPE --source-version VERSION --target-version VERSION\nExplicit existing-database upgrade only; every binding is required. No gateway, workers, identity registration or source/provider RPC.";

#[derive(Clone, PartialEq, Eq)]
pub struct UpgradeCommand {
    pub(crate) database_url: String,
    pub(crate) native_identity: String,
    pub(crate) database_scope: String,
    pub(crate) source_version: String,
    pub(crate) target_version: String,
}
impl std::fmt::Debug for UpgradeCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UpgradeCommand([REDACTED])")
    }
}

/// The maintenance flag cannot be an operand of another command. Parsing and
/// complete binding validation happen before any config or database is opened.
pub fn parse_upgrade(args: &[String]) -> Result<Option<UpgradeCommand>> {
    const FLAG: &str = "--pinning-manage-upgrade";
    if !args.iter().any(|argument| argument == FLAG) {
        return Ok(None);
    }
    let invalid = || anyhow!(HELP);
    if args.first().is_none_or(|argument| argument != FLAG) || args.len() != 11 {
        return Err(invalid());
    }
    let mut database_url = None;
    let mut native_identity = None;
    let mut database_scope = None;
    let mut source_version = None;
    let mut target_version = None;
    for pair in args[1..].chunks_exact(2) {
        let value = &pair[1];
        if !operand(value) {
            return Err(invalid());
        }
        let slot = match pair[0].as_str() {
            "--database-url" => &mut database_url,
            "--native-identity" => &mut native_identity,
            "--database-scope" => &mut database_scope,
            "--source-version" if value.len() <= 256 => &mut source_version,
            "--target-version" if value.len() <= 256 => &mut target_version,
            _ => return Err(invalid()),
        };
        if slot.replace(value.clone()).is_some() {
            return Err(invalid());
        }
    }
    Ok(Some(UpgradeCommand {
        database_url: database_url.ok_or_else(invalid)?,
        native_identity: native_identity.ok_or_else(invalid)?,
        database_scope: database_scope.ok_or_else(invalid)?,
        source_version: source_version.ok_or_else(invalid)?,
        target_version: target_version.ok_or_else(invalid)?,
    }))
}
