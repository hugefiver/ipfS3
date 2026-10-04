use super::{
    IpfsRpcProvider, MAX_OBSERVED_RESOURCES, RpcProfile, RpcResourceStatus,
    body::ResponseBody,
    cid,
    error::{mismatch, protocol},
    streaming::{Progress, Transfer},
    submit_observation::SubmitEvidence,
    transport::{collect_control, send_error},
};
use crate::{kubo::NdjsonBuffer, pinning::provider::ProviderError};
use reqwest::multipart;
use serde::Deserialize;

impl IpfsRpcProvider {
    pub(super) async fn import_car(
        &self,
        requested: &str,
        evidence: &mut SubmitEvidence,
    ) -> Result<(), ProviderError> {
        let source_cid = cid::canonical(requested)?;
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| super::not_submitted("RPC source unavailable before submit"))?;
        // This is a complete network-capable DAG export, NOT tier-copy's
        // offline/local-only export (which is a different residency contract).
        let response = source
            .source_response(
                "dag/export",
                &[
                    ("arg", &source_cid),
                    ("offline", "false"),
                    ("progress", "false"),
                ],
            )
            .await
            .map_err(|_| super::not_submitted("RPC complete DAG export failed before submit"))?;
        let transfer = Transfer::new();
        let body = transfer
            .source_body(response, source.timeouts.idle)
            .map_err(|_| super::not_submitted("RPC complete DAG export failed before submit"))?;
        let form = multipart::Form::new().part(
            "file",
            multipart::Part::stream(body).file_name("export.car"),
        );
        let mut request = self
            .target
            .request(
                true,
                "dag/import",
                &[
                    ("pin-roots", "true"),
                    ("stats", "true"),
                    ("fast-provide-root", "false"),
                    ("fast-provide-dag", "false"),
                    ("fast-provide-wait", "false"),
                    ("encoding", "json"),
                    ("stream-channels", "true"),
                ],
            )
            .multipart(form)
            .build()
            .map_err(|_| super::not_submitted("RPC CAR request construction failed"))?;
        transfer
            .wrap_request(&mut request)
            .map_err(|_| super::not_submitted("RPC CAR request construction failed"))?;
        evidence.begin_write();
        let observed = transfer
            .progress
            .run(self.target.timeouts.idle, async {
                let response = self
                    .target
                    .streaming
                    .execute(request)
                    .await
                    .map_err(|e| send_error(e, true))?;
                if response.status() != reqwest::StatusCode::OK {
                    let response = collect_control(response).await?;
                    response.check(true)?;
                    return Err(protocol("unexpected RPC import status"));
                }
                let observed =
                    parse_import(response, &transfer.progress, self.profile, evidence).await?;
                evidence.clean(self.profile, observed.roots.iter().cloned())?;
                if let Err(error) = transfer.ensure_complete() {
                    evidence.unknown();
                    return Err(error);
                }
                if !observed.stats {
                    return Err(protocol("RPC import omitted terminal stats"));
                }
                if observed.roots.len() != 1 {
                    return Err(protocol("RPC import returned multiple roots"));
                }
                let (root, status) = observed.roots.into_iter().next().expect("length checked");
                if status == RpcResourceStatus::PinError {
                    return Err(protocol("RPC import root pin failed"));
                }
                Ok(root)
            })
            .await?;
        if !cid::equivalent(requested, &observed).unwrap_or(false) {
            return Err(mismatch());
        }
        // Import Root.PinErrorMsg and stats alone are not a recursive/local
        // residency proof. The common submit path performs that proof next.
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportRecord {
    #[serde(rename = "Root")]
    root: Option<ImportRoot>,
    #[serde(rename = "Stats")]
    stats: Option<ImportStats>,
}

#[derive(Deserialize)]
struct ImportRoot {
    #[serde(rename = "Cid")]
    cid: ImportCid,
    #[serde(rename = "PinErrorMsg")]
    pin_error: String,
}

#[derive(Deserialize)]
struct ImportCid {
    #[serde(rename = "/")]
    value: String,
}

#[derive(Deserialize)]
struct ImportStats {
    #[serde(rename = "BlockCount")]
    blocks: u64,
    #[serde(rename = "BlockBytesCount")]
    bytes: u64,
}

#[derive(Default)]
struct ImportEvidence {
    roots: Vec<(String, RpcResourceStatus)>,
    stats: bool,
}

impl ImportEvidence {
    fn record(
        &mut self,
        record: &[u8],
        progress: &Progress,
        profile: RpcProfile,
        evidence: &mut SubmitEvidence,
    ) -> Result<(), ProviderError> {
        let record: ImportRecord =
            serde_json::from_slice(record).map_err(|_| protocol("invalid RPC import record"))?;
        if self.stats || record.root.is_some() == record.stats.is_some() {
            return Err(protocol("invalid RPC import terminal sequence"));
        }
        if let Some(root) = record.root {
            let cid = cid::canonical(&root.cid.value)
                .map_err(|_| protocol("invalid RPC import root CID"))?;
            evidence.reported(profile, cid.clone())?;
            if self.roots.len() == MAX_OBSERVED_RESOURCES {
                return Err(protocol("RPC import root observation exceeds bound"));
            }
            let status = if root.pin_error.is_empty() {
                RpcResourceStatus::PinAccepted
            } else {
                RpcResourceStatus::PinError
            };
            if !self.roots.iter().any(|(existing, _)| existing == &cid) {
                progress.advance();
            }
            self.roots.push((cid, status));
        }
        if let Some(stats) = record.stats {
            if self.roots.is_empty() || stats.blocks == 0 {
                return Err(protocol(
                    "RPC import omitted complete single-root DAG evidence",
                ));
            }
            // A raw empty root can legitimately have zero block bytes.
            let _ = stats.bytes;
            self.stats = true;
            progress.advance();
        }
        Ok(())
    }
}

async fn parse_import(
    response: reqwest::Response,
    progress: &Progress,
    profile: RpcProfile,
    submit_evidence: &mut SubmitEvidence,
) -> Result<ImportEvidence, ProviderError> {
    let mut body = ResponseBody::new(response, Some(128 * 1024))?;
    let mut lines = NdjsonBuffer::new();
    let mut evidence = ImportEvidence::default();
    while let Some(data) = body.next().await? {
        lines
            .push(data)
            .map_err(|_| protocol("invalid RPC import framing"))?;
        while let Some(record) = lines
            .next_record()
            .map_err(|_| protocol("invalid RPC import framing"))?
        {
            evidence.record(&record, progress, profile, submit_evidence)?;
        }
    }
    if let Some(record) = lines.finish() {
        evidence.record(&record, progress, profile, submit_evidence)?;
    }
    Ok(evidence)
}
