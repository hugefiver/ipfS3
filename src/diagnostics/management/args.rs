use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::MAX_ITEMS;

pub const HELP: &str = "usage: ipfs-s3-gateway --pinning-manage REQUEST.json [--source-node PEER] [--output PLAN.json]
       ipfs-s3-gateway --pinning-manage REQUEST.json [--source-node PEER] --confirm OUTPUT.json [--ack-item ID ...] [--ttl-seconds 1..900]
       ipfs-s3-gateway --pinning-manage --apply CONFIRMED.json [--run]
       ipfs-s3-gateway --pinning-manage --status CONFIRMED.json
       ipfs-s3-gateway --pinning-manage --prepare PREPARE.json
       ipfs-s3-gateway --pinning-manage --help

Default: dry_run, existing database read only; no migrations, registration, workers or RPC.
REQUEST.json is the public PlanRequest (plan_id, items), never an authorization.
--source-node is an explicit expected local peer, not a live probe or readability proof.
--output exports an unconfirmed public plan; --confirm explicitly stages/confirms in the DB.
Candidate items require separate --ack-item ID acknowledgements. Confirmation TTL defaults to 300 seconds.
Artifacts use create_new: no overwrite and no automatic parent directory creation.
Apply/status require the original exported confirmed document and matching locally authorized DB records.
--run is opt-in: one receipt-scoped due-job pass, never a background/global worker or queue drain.
Work budget: 30 seconds, then cancellation and at most 5 seconds of owned-work drain.
Not-yet-due, claim-cap or interrupted work remains queued/waiting/needs_attention as reported by the action layer;
job counts and directory residency never imply remote completion or physical space release.
PREPARE.json: record_id, identity (ExactObject), expected_node, max_stored_bytes,
ttl_seconds (1..3600), directory (boolean). Explicit offline source proof and DB write only;
Directory proof counts unique reachable DAG block payload bytes, excluding CAR framing;
this logical byte hold does not authorize file-only upload or imply physical/provider billing bytes.
No HTTP endpoint is added. Errors omit paths, DB URLs, SQL and secret/provider detail.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    DryRun {
        request: String,
        source_node: Option<String>,
        output: Option<String>,
    },
    Confirm {
        request: String,
        source_node: Option<String>,
        output: String,
        acknowledgements: Vec<String>,
        ttl_seconds: u32,
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
    if args.len() > 2 * MAX_ITEMS + 12 {
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
        [request, rest @ ..] if operand(request) => parse_request(request, rest)?,
        _ => return Err(usage()),
    };
    Ok(Some(command))
}

fn parse_request(request: &str, rest: &[String]) -> Result<Command> {
    let mut source_node = None;
    let mut output = None;
    let mut confirmation_output = None;
    let mut ttl = None;
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
        })
    } else {
        if ttl.is_some() || !acknowledgements.is_empty() {
            return Err(usage());
        }
        Ok(Command::DryRun {
            request: request.into(),
            source_node,
            output,
        })
    }
}
