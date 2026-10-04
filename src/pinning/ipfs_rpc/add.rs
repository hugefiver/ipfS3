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

#[derive(Deserialize)]
struct SourceStat {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "Type")]
    kind: String,
}

impl IpfsRpcProvider {
    pub(super) async fn upload(
        &self,
        requested: &str,
        evidence: &mut SubmitEvidence,
    ) -> Result<(), ProviderError> {
        let expected = cid::parse(requested)?;
        let source_cid = cid::canonical(requested)?;
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| super::not_submitted("RPC source unavailable before submit"))?;
        // No S3 plaintext/decryption path is involved. UnixFS directories are
        // explicitly rejected before cat and before any target mutation.
        let source_path = format!("/ipfs/{source_cid}");
        let stat = source
            .control("files/stat", &[("arg", &source_path)], false)
            .await
            .map_err(|_| super::not_submitted("RPC source inspection failed before submit"))?;
        stat.check(false)
            .map_err(|_| super::not_submitted("RPC source inspection failed before submit"))?;
        let stat: SourceStat = stat
            .json()
            .map_err(|_| super::not_submitted("RPC source inspection was invalid before submit"))?;
        if !cid::equivalent(&stat.hash, requested).unwrap_or(false) || stat.kind != "file" {
            return Err(super::not_submitted(
                "RPC byte upload requires a matching UnixFS file; directories require CAR",
            ));
        }
        let response = source
            .source_response("cat", &[("arg", &source_cid)])
            .await
            .map_err(|_| super::not_submitted("RPC stored byte read failed before submit"))?;
        let transfer = Transfer::new();
        let body = transfer
            .source_body(response, source.timeouts.idle)
            .map_err(|_| super::not_submitted("RPC stored byte read failed before submit"))?;
        let part = multipart::Part::stream(body).file_name("object");
        let form = multipart::Form::new().part("file", part);
        let version = if expected.version() == ::cid::Version::V0 {
            "0"
        } else {
            "1"
        };
        let raw_leaves = if expected.version() == ::cid::Version::V0 {
            "false"
        } else {
            "true"
        };
        let filebase_query = [("cid-version", version), ("wrap-with-directory", "false")];
        let kubo_query = [
            ("cid-version", version),
            ("wrap-with-directory", "false"),
            ("raw-leaves", raw_leaves),
            ("chunker", "size-262144"),
            ("hash", "sha2-256"),
            ("pin", "false"),
            ("progress", "true"),
        ];
        let query = if self.profile == RpcProfile::Filebase {
            &filebase_query[..]
        } else {
            &kubo_query[..]
        };
        let mut request = self
            .target
            .request(true, "add", query)
            .multipart(form)
            .build()
            .map_err(|_| super::not_submitted("RPC upload request construction failed"))?;
        transfer
            .wrap_request(&mut request)
            .map_err(|_| super::not_submitted("RPC upload request construction failed"))?;
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
                    return Err(protocol("unexpected RPC add status"));
                }
                let observed =
                    parse_add(response, &transfer.progress, self.profile, evidence).await?;
                let status = if self.profile == RpcProfile::Kubo {
                    RpcResourceStatus::Stored
                } else {
                    RpcResourceStatus::PinAccepted
                };
                evidence.clean(
                    self.profile,
                    observed.roots.iter().cloned().map(|cid| (cid, status)),
                )?;
                if let Err(error) = transfer.ensure_complete() {
                    evidence.unknown();
                    return Err(error);
                }
                if !observed.final_root {
                    return Err(protocol("RPC add omitted terminal root"));
                }
                if observed.roots.len() != 1 {
                    return Err(protocol("RPC add returned multiple roots"));
                }
                Ok(observed.roots.into_iter().next().expect("length checked"))
            })
            .await?;
        if !cid::equivalent(&observed, requested).unwrap_or(false) {
            return Err(mismatch());
        }
        // Filebase add already pins. Never follow it with unsupported pin/add.
        if self.profile == RpcProfile::Kubo {
            self.pin_cid(requested, evidence).await?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct AddRecord {
    #[serde(rename = "Bytes")]
    bytes: Option<u64>,
    #[serde(rename = "Hash")]
    hash: Option<String>,
    #[serde(rename = "Size")]
    size: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ZeroProgressRecord {
    #[serde(rename = "Name")]
    name: String,
}

#[derive(Default)]
struct AddEvidence {
    bytes: u64,
    roots: Vec<String>,
    final_root: bool,
}

impl AddEvidence {
    fn record(
        &mut self,
        line: &[u8],
        progress: &Progress,
        profile: RpcProfile,
        evidence: &mut SubmitEvidence,
    ) -> Result<(), ProviderError> {
        let record: AddRecord =
            serde_json::from_slice(line).map_err(|_| protocol("invalid RPC add record"))?;
        if record.hash.is_none() && record.bytes.is_none() {
            // Kubo omits Bytes=0 with progress=true. Only the exact uploaded
            // file name, before any byte progress/root, represents that zero;
            // accepting it must not advance idle or acknowledge a resource.
            if profile == RpcProfile::Kubo
                && self.bytes == 0
                && self.roots.is_empty()
                && serde_json::from_slice::<ZeroProgressRecord>(line)
                    .is_ok_and(|record| record.name == "object")
            {
                return Ok(());
            }
            return Err(protocol("unrecognized RPC add record"));
        }
        if let Some(bytes) = record.bytes {
            if bytes < self.bytes {
                return Err(protocol("nonmonotonic RPC add progress"));
            }
            if bytes > self.bytes {
                self.bytes = bytes;
                progress.advance();
            }
        }
        self.final_root = false;
        if let Some(root) = record.hash {
            let root = cid::canonical(&root).map_err(|_| protocol("invalid RPC add root CID"))?;
            evidence.reported(profile, root.clone())?;
            if record.size.is_some_and(|v| v.parse::<u64>().is_err()) {
                return Err(protocol("invalid RPC add root size"));
            }
            if self.roots.len() == MAX_OBSERVED_RESOURCES {
                return Err(protocol("RPC add root observation exceeds bound"));
            }
            if !self.roots.contains(&root) {
                progress.advance();
            }
            self.roots.push(root);
            self.final_root = true;
        }
        Ok(())
    }
}

async fn parse_add(
    response: reqwest::Response,
    progress: &Progress,
    profile: RpcProfile,
    submit_evidence: &mut SubmitEvidence,
) -> Result<AddEvidence, ProviderError> {
    let mut body = ResponseBody::new(response, None)?;
    let mut lines = NdjsonBuffer::new();
    let mut evidence = AddEvidence::default();
    while let Some(data) = body.next().await? {
        lines
            .push(data)
            .map_err(|_| protocol("invalid RPC add framing"))?;
        while let Some(record) = lines
            .next_record()
            .map_err(|_| protocol("invalid RPC add framing"))?
        {
            evidence.record(&record, progress, profile, submit_evidence)?;
        }
    }
    if let Some(record) = lines.finish() {
        evidence.record(&record, progress, profile, submit_evidence)?;
    }
    Ok(evidence)
}
