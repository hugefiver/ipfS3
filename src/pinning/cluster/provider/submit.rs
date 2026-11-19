use super::{
    ClusterFailureCode as Code, ClusterProtocol, ClusterProvider, ClusterResourceKey,
    ClusterStrategy, ClusterWriteFailure as Failure, ClusterWriteReceipt, canonical_cid,
    car::Car,
    http::Body,
    stream::{Progress, Transfer},
};
use crate::pinning::provider::SubmitPin;
use cid::{Cid, Version};
use reqwest::{Method, multipart};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
struct Stat {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "Type")]
    kind: String,
}

impl ClusterProvider {
    pub async fn submit(&self, request: SubmitPin) -> Result<ClusterWriteReceipt, Failure> {
        self.submit_with_cancel(request, &CancellationToken::new())
            .await
    }

    pub async fn submit_with_cancel(
        &self,
        request: SubmitPin,
        cancel: &CancellationToken,
    ) -> Result<ClusterWriteReceipt, Failure> {
        let requested: Cid = request
            .cid
            .parse()
            .map_err(|_| Failure::new(Code::InvalidCid))?;
        let canonical = canonical_cid(&requested);
        let cid = canonical.to_string();
        if cancel.is_cancelled() {
            return Err(Failure::new(Code::Cancelled));
        }
        let progress = Progress::new();
        let mut transfer = None;
        let request = if self.options.strategy == ClusterStrategy::Cid {
            match self.options.protocol {
                ClusterProtocol::Proxy => self
                    .target
                    .rpc("pin/add", &[("arg", &cid), ("type", "recursive")]),
                // v1.1.6 ParseCidOrFail does NOT parse PinOptions for this route.
                ClusterProtocol::Rest => {
                    self.target
                        .request(Method::POST, &format!("/pins/{cid}"), &[])
                }
            }
            .build()
            .map_err(|_| Failure::new(Code::Protocol))?
        } else {
            let (request, state) = progress
                .run(
                    self.options.transfer_limits.idle_timeout,
                    cancel,
                    self.upload_request(&cid, &requested),
                )
                .await?;
            transfer = Some(state);
            request
        };
        let progress = transfer.as_ref().map(|t| &t.progress).unwrap_or(&progress);
        let operation = async {
            let response = self.target.execute(request).await?;
            if self.options.strategy == ClusterStrategy::Cid {
                self.parse_pin(response, progress).await
            } else {
                self.parse_add(response, progress).await
            }
        };
        let result = if self.options.strategy == ClusterStrategy::Cid {
            tokio::time::timeout(
                self.options.transfer_limits.control_timeout,
                progress.run(self.options.transfer_limits.idle_timeout, cancel, operation),
            )
            .await
            .map_err(|_| Failure::new(Code::ControlTimeout))?
        } else {
            progress
                .run(self.options.transfer_limits.idle_timeout, cancel, operation)
                .await
        };
        if let Some(transfer) = &transfer
            && let Some(error) = transfer.failure()
        {
            return Err(error);
        }
        let reported = result?;
        if let Some(transfer) = &transfer {
            transfer.complete()?;
        }
        if canonical_cid(&reported) != canonical {
            return Err(Failure::new(Code::CidMismatch));
        }
        Ok(ClusterWriteReceipt {
            resource: ClusterResourceKey {
                pinset: self.options.pinset.clone(),
                cid,
            },
            request_id: reported.to_string(),
        })
    }

    async fn upload_request(
        &self,
        cid: &str,
        requested: &Cid,
    ) -> Result<(reqwest::Request, Transfer), Failure> {
        let source = self
            .source
            .as_ref()
            .ok_or_else(|| Failure::new(Code::InvalidConfiguration))?;
        let car = self.options.strategy == ClusterStrategy::Car;
        if !car {
            let bytes = tokio::time::timeout(self.options.transfer_limits.control_timeout, async {
                let stat = source
                    .source("files/stat", &[("arg", &format!("/ipfs/{cid}"))])
                    .await?;
                Body::new(stat, self.options.transfer_limits.max_response_bytes as u64)?
                    .collect()
                    .await
            })
            .await
            .map_err(|_| Failure::new(Code::ControlTimeout))??;
            let stat: Stat =
                serde_json::from_slice(&bytes).map_err(|_| Failure::new(Code::Protocol))?;
            let stat_cid = stat
                .hash
                .parse()
                .map_err(|_| Failure::new(Code::Protocol))?;
            if canonical_cid(&stat_cid) != canonical_cid(requested) {
                return Err(Failure::new(Code::CidMismatch));
            }
            if matches!(stat.kind.as_str(), "directory" | "hamt-sharded-directory") {
                return Err(Failure::new(Code::DirectoryRequiresCar));
            }
            if !matches!(stat.kind.as_str(), "file" | "raw") {
                return Err(Failure::new(Code::Protocol));
            }
        }
        let response = if car {
            source
                .source(
                    "dag/export",
                    &[("arg", cid), ("offline", "false"), ("progress", "false")],
                )
                .await?
        } else {
            source.source("cat", &[("arg", cid)]).await?
        };
        let transfer = Transfer::new();
        let validator = car
            .then(|| Car::new(cid, self.options.transfer_limits))
            .transpose()?;
        let body = transfer.source(response, self.options.transfer_limits, validator)?;
        // A fixed opaque filename; object name, tags and caller metadata are not sent.
        let filename = if car { "content.car" } else { "content" };
        let form =
            multipart::Form::new().part("file", multipart::Part::stream(body).file_name(filename));
        let format = if car { "car" } else { "unixfs" };
        let stream = if self.options.stream_channels {
            "true"
        } else {
            "false"
        };
        let v0 = requested.version() == Version::V0;
        let unixfs = self.options.unixfs.unwrap_or(super::ClusterUnixfsOptions {
            cid_version: if v0 { 0 } else { 1 },
            raw_leaves: !v0,
            chunk_size: 262144,
            trickle: false,
        });
        let chunker = format!("size-{}", unixfs.chunk_size);
        let mut query = vec![
            ("format", format),
            ("stream-channels", stream),
            ("no-pin", "false"),
            ("wrap-with-directory", "false"),
            ("progress", "false"),
            ("shard", "false"),
        ];
        if !car {
            if !matches!(requested.codec(), 0x55 | 0x70) || requested.hash().code() != 0x12 {
                return Err(Failure::new(Code::InvalidConfiguration));
            }
            query.extend([
                (
                    "cid-version",
                    if unixfs.cid_version == 0 { "0" } else { "1" },
                ),
                ("hash", "sha2-256"),
                (
                    "raw-leaves",
                    if unixfs.raw_leaves { "true" } else { "false" },
                ),
                ("chunker", chunker.as_str()),
                (
                    "layout",
                    if unixfs.trickle {
                        "trickle"
                    } else {
                        "balanced"
                    },
                ),
            ]);
        }
        let request = match self.options.protocol {
            ClusterProtocol::Proxy => self.target.rpc("add", &query),
            ClusterProtocol::Rest => self.target.request(Method::POST, "/add", &query),
        };
        let mut request = request
            .multipart(form)
            .build()
            .map_err(|_| Failure::new(Code::Protocol))?;
        transfer.wrap(&mut request)?;
        Ok((request, transfer))
    }
}
