use super::{
    IpfsRpcProvider,
    body::ResponseBody,
    cid,
    error::{mismatch, protocol},
    streaming::{Progress, Transfer},
    transport::{collect_control, send_error},
};
use crate::{kubo::NdjsonBuffer, pinning::provider::ProviderError};
use ::cid::Cid;
use reqwest::multipart;
use serde::Deserialize;

impl IpfsRpcProvider {
    pub(super) async fn import_car(&self, requested: &Cid) -> Result<Cid, ProviderError> {
        let source_cid = requested.to_string();
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
                    return Err(protocol("unexpected RPC import status"));
                }
                let root = parse_import(response, &transfer.progress, requested).await?;
                transfer.ensure_complete()?;
                Ok(root)
            })
            .await
    }
}

#[derive(Deserialize)]
enum ImportRecord {
    Root(ImportRoot),
    Stats {
        #[serde(rename = "BlockCount")]
        _blocks: u64,
        #[serde(rename = "BlockBytesCount")]
        _bytes: u64,
    },
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

enum ImportAck {
    AwaitingRoot,
    AwaitingStats(Cid),
    Complete(Cid),
}

impl ImportAck {
    fn record(
        &mut self,
        record: &[u8],
        progress: &Progress,
        expected: &Cid,
    ) -> Result<(), ProviderError> {
        let record: ImportRecord =
            serde_json::from_slice(record).map_err(|_| protocol("invalid RPC import record"))?;
        *self = match (&*self, record) {
            (Self::AwaitingRoot, ImportRecord::Root(root)) => {
                if !root.pin_error.is_empty() {
                    return Err(protocol("RPC import root pin failed"));
                }
                let root = cid::parse_ack(&root.cid.value)?;
                if !cid::equivalent(&root, expected) {
                    return Err(mismatch());
                }
                Self::AwaitingStats(root)
            }
            (Self::AwaitingStats(cid), ImportRecord::Stats { .. }) => Self::Complete(*cid),
            _ => return Err(protocol("invalid RPC import terminal sequence")),
        };
        progress.advance();
        Ok(())
    }
}

async fn parse_import(
    response: reqwest::Response,
    progress: &Progress,
    expected: &Cid,
) -> Result<Cid, ProviderError> {
    let mut body = ResponseBody::new(response, Some(128 * 1024))?;
    let mut lines = NdjsonBuffer::new();
    let mut ack = ImportAck::AwaitingRoot;
    while let Some(data) = body.next().await? {
        lines
            .push(data)
            .map_err(|_| protocol("invalid RPC import framing"))?;
        while let Some(record) = lines
            .next_record()
            .map_err(|_| protocol("invalid RPC import framing"))?
        {
            ack.record(&record, progress, expected)?;
        }
    }
    if let Some(record) = lines.finish() {
        ack.record(&record, progress, expected)?;
    }
    match ack {
        ImportAck::Complete(root) => Ok(root),
        _ => Err(protocol("RPC import omitted terminal stats")),
    }
}
