use super::{
    IpfsRpcProvider, RpcProfile,
    body::ResponseBody,
    cid,
    error::{mismatch, protocol},
    streaming::{Progress, Transfer},
    transport::{collect_control, send_error},
};
use crate::{kubo::NdjsonBuffer, pinning::provider::ProviderError};
use ::cid::{Cid, Version};
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
    pub(super) async fn upload(&self, requested: &Cid) -> Result<Cid, ProviderError> {
        let source_cid = cid::canonical(requested).to_string();
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
        if stat.kind != "file"
            || !cid::parse(&stat.hash).is_ok_and(|root| cid::equivalent(&root, requested))
        {
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
        // Filebase rejects a multipart part that declares no content type with an
        // opaque HTTP 500. reqwest omits it for a streamed part, so set it here.
        let part = multipart::Part::stream(body)
            .file_name("object")
            .mime_str("application/octet-stream")
            .map_err(|_| protocol("RPC upload part construction failed"))?;
        let form = multipart::Form::new().part("file", part);
        let (version, raw_leaves) = match requested.version() {
            Version::V0 => ("0", "false"),
            Version::V1 => ("1", "true"),
        };
        let filebase_query = [("cid-version", version), ("wrap-with-directory", "false")];
        let kubo_query = [
            ("cid-version", version),
            ("wrap-with-directory", "false"),
            ("raw-leaves", raw_leaves),
            ("chunker", "size-262144"),
            ("hash", "sha2-256"),
            ("pin", "true"),
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
        transfer
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
                let root = parse_add(response, &transfer.progress, self.profile, requested).await?;
                transfer.ensure_complete()?;
                Ok(root)
            })
            .await
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
struct AddAck {
    bytes: u64,
    root: Option<Cid>,
}

impl AddAck {
    fn record(
        &mut self,
        line: &[u8],
        progress: &Progress,
        profile: RpcProfile,
        expected: &Cid,
    ) -> Result<(), ProviderError> {
        if self.root.is_some() {
            return Err(protocol("RPC add returned a record after the root"));
        }
        let record: AddRecord =
            serde_json::from_slice(line).map_err(|_| protocol("invalid RPC add record"))?;
        if record.hash.is_none() && record.bytes.is_none() {
            // Kubo omits Bytes=0 with progress=true. Only the exact uploaded
            // file name before positive byte progress represents that zero.
            if profile == RpcProfile::Kubo
                && self.bytes == 0
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
        if let Some(root) = record.hash {
            let root = cid::parse_ack(&root)?;
            if !cid::equivalent(&root, expected) {
                return Err(mismatch());
            }
            if record.size.is_some_and(|v| v.parse::<u64>().is_err()) {
                return Err(protocol("invalid RPC add root size"));
            }
            self.root = Some(root);
            progress.advance();
        }
        Ok(())
    }
}

async fn parse_add(
    response: reqwest::Response,
    progress: &Progress,
    profile: RpcProfile,
    expected: &Cid,
) -> Result<Cid, ProviderError> {
    let mut body = ResponseBody::new(response, None)?;
    let mut lines = NdjsonBuffer::new();
    let mut ack = AddAck::default();
    while let Some(data) = body.next().await? {
        lines
            .push(data)
            .map_err(|_| protocol("invalid RPC add framing"))?;
        while let Some(record) = lines
            .next_record()
            .map_err(|_| protocol("invalid RPC add framing"))?
        {
            ack.record(&record, progress, profile, expected)?;
        }
    }
    if let Some(record) = lines.finish() {
        ack.record(&record, progress, profile, expected)?;
    }
    ack.root
        .ok_or_else(|| protocol("RPC add omitted terminal root"))
}
