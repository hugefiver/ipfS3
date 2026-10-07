use super::{
    ClusterFailureCode as Code, ClusterProvider, ClusterResourceStatus,
    ClusterWriteFailure as Failure, ClusterWriteObservation,
    http::{self, Body},
    stream::Progress,
};
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Deserialize)]
struct Pin {
    cid: String,
}
#[derive(Deserialize)]
struct Pins {
    #[serde(rename = "Pins")]
    pins: Vec<String>,
}

impl ClusterProvider {
    fn added(
        &self,
        record: &[u8],
        observation: &mut ClusterWriteObservation,
    ) -> Result<Option<String>, Failure> {
        // A malformed receipt is not a rollback: preserve a syntactically valid
        // CID even if another official DTO field is missing/wrong/unrecognized.
        if let Some(cid) = super::add_response::reported(self.options.protocol, record)
            && !cid.is_empty()
        {
            self.record(observation, &cid, ClusterResourceStatus::Reported)?;
        }
        let (cid, allocations) = super::add_response::parse(self.options.protocol, record)?;
        let Some(cid) = cid else {
            return Ok(None);
        };
        let resource = self.record(observation, &cid, ClusterResourceStatus::Reported)?;
        // These are historical allocation hints ONLY, never replica proof.
        if let Some(allocations) = allocations {
            let peers: Result<BTreeSet<_>, _> = allocations
                .iter()
                .map(|peer| {
                    super::super::peer::canonical(peer, super::super::RestOperation::Verify)
                })
                .collect();
            observation.resources[resource].reported_allocations =
                Some(peers.map_err(|_| Failure::new(Code::Protocol))?);
        }
        Ok(Some(observation.resources[resource].resource.cid.clone()))
    }
    pub(super) async fn parse_add(
        &self,
        response: reqwest::Response,
        observation: &mut ClusterWriteObservation,
        progress: &Progress,
    ) -> Result<String, Failure> {
        let status = http::check(&response).err();
        let result = self.read_add(response, observation, progress).await;
        status.map_or(result, Err)
    }
    async fn read_add(
        &self,
        response: reqwest::Response,
        observation: &mut ClusterWriteObservation,
        progress: &Progress,
    ) -> Result<String, Failure> {
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let valid_headers = content_type.split(';').next() == Some("application/json")
            && (!self.options.stream_channels
                || response
                    .headers()
                    .get("x-chunked-output")
                    .is_some_and(|v| v == "1"));
        let mut body = Body::new(
            response,
            self.options.transfer_limits.max_response_bytes as u64,
        )?;
        let mut pending = Vec::new();
        let mut roots = Vec::new();
        let mut total = 0usize;
        let mut stream_error = None;
        let mut parse_error = None;
        // A bad DTO is not rollback. Continue the bounded response read to
        // retain later reported CIDs, but never turn a partial parse into success.
        let mut collect = |record: &[u8], observation: &mut ClusterWriteObservation| match self
            .added(record, observation)
        {
            Ok(Some(cid)) => roots.push(cid),
            Ok(None) => {}
            Err(error) => {
                parse_error.get_or_insert(error);
            }
        };
        loop {
            match body.next().await {
                Ok(Some(bytes)) => {
                    total = total.saturating_add(bytes.len());
                    if total > self.options.transfer_limits.max_response_bytes {
                        return Err(Failure::new(Code::ResourceLimit));
                    }
                    pending.extend_from_slice(&bytes);
                    progress.advance();
                    if self.options.stream_channels {
                        while let Some(end) = pending.iter().position(|b| *b == b'\n') {
                            let line: Vec<_> = pending.drain(..=end).collect();
                            if line.iter().all(u8::is_ascii_whitespace) {
                                continue;
                            }
                            collect(&line, observation);
                        }
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    stream_error = Some(error);
                    break;
                }
            }
        }
        if !pending.iter().all(u8::is_ascii_whitespace) {
            if self.options.stream_channels {
                collect(&pending, observation);
            } else {
                let records: Vec<serde_json::Value> =
                    serde_json::from_slice(&pending).map_err(|_| Failure::new(Code::Protocol))?;
                for record in records {
                    let bytes =
                        serde_json::to_vec(&record).map_err(|_| Failure::new(Code::Protocol))?;
                    collect(&bytes, observation);
                }
            }
        }
        if let Some(error) = stream_error {
            return Err(error);
        }
        if let Some(error) = parse_error {
            return Err(error);
        }
        if !valid_headers || roots.len() != 1 {
            return Err(Failure::new(Code::Protocol));
        }
        Ok(roots.remove(0))
    }
    pub(super) async fn parse_pin(
        &self,
        response: reqwest::Response,
        observation: &mut ClusterWriteObservation,
        progress: &Progress,
    ) -> Result<String, Failure> {
        let status = http::check(&response).err();
        let result = self.read_pin(response, observation, progress).await;
        status.map_or(result, Err)
    }
    async fn read_pin(
        &self,
        response: reqwest::Response,
        observation: &mut ClusterWriteObservation,
        progress: &Progress,
    ) -> Result<String, Failure> {
        let mut body = Body::new(
            response,
            self.options.transfer_limits.max_response_bytes as u64,
        )?;
        let mut bytes = Vec::new();
        let mut stream_error = None;
        loop {
            match body.next().await {
                Ok(Some(data)) => {
                    bytes.extend_from_slice(&data);
                    progress.advance();
                }
                Ok(None) => break,
                Err(error) => {
                    stream_error = Some(error);
                    break;
                }
            }
        }
        let cids = match self.options.protocol {
            super::ClusterProtocol::Proxy => serde_json::from_slice::<Pins>(&bytes).map(|v| v.pins),
            super::ClusterProtocol::Rest => {
                serde_json::from_slice::<Pin>(&bytes).map(|v| vec![v.cid])
            }
        }
        .map_err(|_| Failure::new(Code::Protocol))?;
        for cid in &cids {
            self.record(observation, cid, ClusterResourceStatus::Reported)?;
        }
        if let Some(error) = stream_error {
            return Err(error);
        }
        if cids.len() != 1 {
            return Err(Failure::new(Code::Protocol));
        }
        super::canonical_cid(&cids[0])
    }
}
