use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::MAX_ITEMS;

#[path = "args/upgrade.rs"]
mod upgrade;
pub use upgrade::{UpgradeCommand, parse_upgrade};

pub const HELP: &str = "usage: ipfs-s3-gateway --pinning-manage REQUEST.json [--source-node PEER] [--output PLAN.v3] [--checkpoint NEWDIR | --resume EXISTINGDIR] [--scan-pages 1..512]
       ipfs-s3-gateway --pinning-manage REQUEST.json [--source-node PEER] --confirm CONFIRMED.v3 [--ack-item ID ...] [--ttl-seconds 1..900] [--checkpoint NEWDIR | --resume EXISTINGDIR] [--scan-pages 1..512]
       ipfs-s3-gateway --pinning-manage --apply CONFIRMED.v3 [--run]
       ipfs-s3-gateway --pinning-manage --status CONFIRMED.v3
       ipfs-s3-gateway --pinning-manage --prepare PREPARE.json
       ipfs-s3-gateway --pinning-manage --review ARTIFACT.v3 [--private-input]
       ipfs-s3-gateway --pinning-manage --export ARTIFACT.v3 --output NEW.v3 [--private-input | --private-recovery]
       ipfs-s3-gateway --pinning-manage --import ARTIFACT.v3 --output NEW.v3 [--private-input | --private-recovery]
       ipfs-s3-gateway --pinning-manage --convert-v2 INPUT.json --output NEW.v3 --private-recovery
       ipfs-s3-gateway --pinning-manage --help
       ipfs-s3-gateway --pinning-manage-upgrade --database-url URL --native-identity ID --database-scope SCOPE --source-version VERSION --target-version VERSION

Default: dry_run, existing database read only; no migrations, registration, workers or RPC.
REQUEST.json is the public PlanRequest (plan_id, items), never an authorization.
--source-node is an explicit expected local peer, not a live probe or readability proof.
--output exports an unconfirmed public plan; --confirm explicitly stages/confirms in the DB.
Candidate items require separate --ack-item ID acknowledgements. Confirmation TTL defaults to 300 seconds.
Artifacts use create_new: no overwrite and no automatic parent directory creation.
Apply/status require the original exported confirmed document and matching locally authorized DB records.
An exact converted, fully committed historical request exports its frozen reply with --output, without reconfirming.
Converted status is a read-only view of current responsibility; apply/review/replay preserve the frozen committed reply and --run rejects history.
Artifacts use only management wire v3, not JSON/v2. File review/export/import never grant authority or write the DB.
--private-input explicitly reads private originals but exports a redacted review; --private-recovery explicitly preserves private originals.
File import validates and freezes an untrusted artifact, not a restored confirmation/receipt or an applied fact.
--convert-v2 is isolated file-only archival of bounded Stage 7 JSON, with mandatory private recovery opt-in.
Legacy JSON is limited to 16 MiB, 131072 nodes, depth 64 and the bounded typed decode arena.
Converted files preserve the complete original review and claims, never a local receipt or write permission.
Old unexecuted plans require a new plan ID and fresh planning/confirmation; cross-DB files are review-only.
Missing v2 identity/time metadata is marked not-recorded-in-wire-v2, never inferred or renewed.
Streaming file work is bounded by 8 GiB, 10 million rows, 32 MiB accounted resident memory and 120 seconds per operation.
Checkpoint/resume require Provider planning and an explicitly selected owner-private local workspace, never an imported artifact path.
--scan-pages limits pages per invocation (default 64 with checkpoint/resume), not cumulative task budgets.
Incomplete exits normally, retains the workspace and exports no plan or confirmation; resume requires the same request and current local authorization/configuration.
Resume revalidates the source DB identity/vector and retains original clocks and cumulative work; source changes require a fresh scan.
Complete uses the original export/confirmation rules and cleans up only its owned workspace files.
--run is opt-in: one receipt-scoped due-job pass, never a background/global worker or queue drain.
Planning/archive work budget: 120 seconds; other commands: 30 seconds, then cancellation and at most 5 seconds of owned-work drain.
Not-yet-due, claim-cap or interrupted work remains queued/waiting/needs_attention as reported by the action layer;
job counts and directory residency never imply remote completion or physical space release.
PREPARE.json: record_id, identity (ExactObject), expected_node, max_stored_bytes,
ttl_seconds (1..3600), directory (boolean). Explicit offline source proof and DB write only;
Directory proof counts unique reachable DAG block payload bytes, excluding CAR framing;
this logical byte hold does not authorize file-only upload or imply physical/provider billing bytes.
No HTTP endpoint is added. Errors omit paths, DB URLs, SQL and secret/provider detail.
Upgrade is a separate explicit maintenance operation on the named existing database only.
All four upgrade bindings are mandatory; it does not load gateway config or start the gateway,
register identities, start workers, or perform source/provider RPC. Ordinary startup never upgrades.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub path: String,
    pub resume: bool,
    pub scan_pages: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    DryRun {
        request: String,
        source_node: Option<String>,
        output: Option<String>,
        checkpoint: Option<Checkpoint>,
    },
    Confirm {
        request: String,
        source_node: Option<String>,
        output: String,
        acknowledgements: Vec<String>,
        ttl_seconds: u32,
        checkpoint: Option<Checkpoint>,
    },
    Apply {
        confirmed: String,
        run: bool,
    },
    Status {
        confirmed: String,
    },
    Prepare {
        request: String,
    },
    Review {
        artifact: String,
        private_input: bool,
    },
    Export {
        artifact: String,
        output: String,
        private_input: bool,
        private_recovery: bool,
    },
    Import {
        artifact: String,
        output: String,
        private_input: bool,
        private_recovery: bool,
    },
    ConvertV2 {
        artifact: String,
        output: String,
    },
}

impl Command {
    pub fn checkpoint(&self) -> Option<&Checkpoint> {
        match self {
            Self::DryRun { checkpoint, .. } | Self::Confirm { checkpoint, .. } => {
                checkpoint.as_ref()
            }
            _ => None,
        }
    }
}

fn usage() -> anyhow::Error {
    anyhow!(HELP)
}

fn operand(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 4096
        && !value.starts_with("--")
        && !value.chars().any(char::is_control)
}

fn acknowledgement(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

pub fn parse(args: &[String]) -> Result<Option<Command>> {
    if args.first().is_none_or(|flag| flag != "--pinning-manage") {
        return Ok(None);
    }
    if args.len() > 2 * MAX_ITEMS + 16 {
        return Err(usage());
    }
    let command = match &args[1..] {
        [flag] if flag == "--help" => Command::Help,
        [flag, file] if flag == "--apply" && operand(file) => Command::Apply {
            confirmed: file.clone(),
            run: false,
        },
        [flag, file, run] if flag == "--apply" && operand(file) && run == "--run" => {
            Command::Apply {
                confirmed: file.clone(),
                run: true,
            }
        }
        [flag, file] if flag == "--status" && operand(file) => Command::Status {
            confirmed: file.clone(),
        },
        [flag, file] if flag == "--prepare" && operand(file) => Command::Prepare {
            request: file.clone(),
        },
        [flag, file] if flag == "--review" && operand(file) => Command::Review {
            artifact: file.clone(),
            private_input: false,
        },
        [flag, file, private]
            if flag == "--review" && operand(file) && private == "--private-input" =>
        {
            Command::Review {
                artifact: file.clone(),
                private_input: true,
            }
        }
        [flag, file, rest @ ..]
            if matches!(flag.as_str(), "--export" | "--import" | "--convert-v2")
                && operand(file) =>
        {
            parse_file(flag, file, rest)?
        }
        [request, rest @ ..] if operand(request) => parse_request(request, rest)?,
        _ => return Err(usage()),
    };
    Ok(Some(command))
}

fn parse_file(flag: &str, artifact: &str, args: &[String]) -> Result<Command> {
    let mut output = None;
    let mut private_input = false;
    let mut private_recovery = false;
    let mut options = args.iter();
    while let Some(option) = options.next() {
        match option.as_str() {
            "--output" if output.is_none() => {
                output = Some(
                    options
                        .next()
                        .filter(|v| operand(v))
                        .ok_or_else(usage)?
                        .clone(),
                )
            }
            "--private-input" if !private_input && !private_recovery => private_input = true,
            "--private-recovery" if !private_input && !private_recovery => {
                private_input = true;
                private_recovery = true;
            }
            _ => return Err(usage()),
        }
    }
    let output = output.ok_or_else(usage)?;
    Ok(if flag == "--convert-v2" {
        if !private_recovery {
            return Err(usage());
        }
        Command::ConvertV2 {
            artifact: artifact.into(),
            output,
        }
    } else if flag == "--export" {
        Command::Export {
            artifact: artifact.into(),
            output,
            private_input,
            private_recovery,
        }
    } else {
        Command::Import {
            artifact: artifact.into(),
            output,
            private_input,
            private_recovery,
        }
    })
}

fn parse_request(request: &str, rest: &[String]) -> Result<Command> {
    let mut source_node = None;
    let mut output = None;
    let mut confirmation_output = None;
    let mut ttl = None;
    let mut checkpoint = None;
    let mut scan_pages = None;
    let mut acknowledgements = Vec::new();
    let mut options = rest.iter();
    while let Some(flag) = options.next() {
        let value = options
            .next()
            .filter(|value| operand(value))
            .ok_or_else(usage)?;
        match flag.as_str() {
            "--source-node" if source_node.is_none() && value.len() <= 256 => {
                source_node = Some(value.clone())
            }
            "--output" if output.is_none() => output = Some(value.clone()),
            "--confirm" if confirmation_output.is_none() => {
                confirmation_output = Some(value.clone())
            }
            "--checkpoint" | "--resume" if checkpoint.is_none() => {
                checkpoint = Some(Checkpoint {
                    path: value.clone(),
                    resume: flag == "--resume",
                    scan_pages: None,
                });
            }
            "--scan-pages" if scan_pages.is_none() => {
                if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(usage());
                }
                let pages = value.parse::<u64>().map_err(|_| usage())?;
                if !(1..=512).contains(&pages) {
                    return Err(usage());
                }
                scan_pages = Some(pages);
            }
            "--ttl-seconds" if ttl.is_none() => {
                if !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(usage());
                }
                let seconds = value.parse::<u32>().map_err(|_| usage())?;
                if !(1..=900).contains(&seconds) {
                    return Err(usage());
                }
                ttl = Some(seconds);
            }
            "--ack-item" if acknowledgements.len() < MAX_ITEMS && acknowledgement(value) => {
                if acknowledgements.contains(value) {
                    return Err(usage());
                }
                acknowledgements.push(value.clone());
            }
            _ => return Err(usage()),
        }
    }
    if let Some(checkpoint) = &mut checkpoint {
        checkpoint.scan_pages = scan_pages;
    } else if scan_pages.is_some() {
        return Err(usage());
    }
    if let Some(output_path) = confirmation_output {
        if output.is_some() {
            return Err(usage());
        }
        Ok(Command::Confirm {
            request: request.into(),
            source_node,
            output: output_path,
            acknowledgements,
            ttl_seconds: ttl.unwrap_or(300),
            checkpoint,
        })
    } else {
        if ttl.is_some() || !acknowledgements.is_empty() {
            return Err(usage());
        }
        Ok(Command::DryRun {
            request: request.into(),
            source_node,
            output,
            checkpoint,
        })
    }
}
