use super::{
    ClusterFailureCode as Code, ClusterProtocol, ClusterProvider, ClusterWriteFailure as Failure,
    http::{self, Body},
    stream::Progress,
};
use cid::Cid;
use serde::Deserialize;

#[derive(Deserialize)]
struct Pin {
    cid: String,
}

#[derive(Deserialize)]
struct Pins {
    #[serde(rename = "Pins")]
    pins: [String; 1],
}

impl ClusterProvider {
    pub(super) async fn parse_add(
        &self,
        response: reqwest::Response,
        progress: &Progress,
    ) -> Result<Cid, Failure> {
        http::check(&response)?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok());
        if content_type.and_then(|value| value.split(';').next()) != Some("application/json")
            || (self.options.stream_channels
                && response
                    .headers()
                    .get("x-chunked-output")
                    .is_none_or(|v| v != "1"))
        {
            return Err(Failure::new(Code::Protocol));
        }
        let body = self.read_ack(response, progress).await?;
        super::add_response::parse(self.options.protocol, &body, self.options.stream_channels)
    }

    pub(super) async fn parse_pin(
        &self,
        response: reqwest::Response,
        progress: &Progress,
    ) -> Result<Cid, Failure> {
        http::check(&response)?;
        let body = self.read_ack(response, progress).await?;
        let cid = match self.options.protocol {
            ClusterProtocol::Proxy => serde_json::from_slice::<Pins>(&body).map(|v| {
                let [cid] = v.pins;
                cid
            }),
            ClusterProtocol::Rest => serde_json::from_slice::<Pin>(&body).map(|v| v.cid),
        }
        .map_err(|_| Failure::new(Code::Protocol))?;
        cid.parse().map_err(|_| Failure::new(Code::Protocol))
    }

    pub(super) async fn read_ack(
        &self,
        response: reqwest::Response,
        progress: &Progress,
    ) -> Result<Vec<u8>, Failure> {
        let mut body = Body::new(
            response,
            self.options.transfer_limits.max_response_bytes as u64,
        )?;
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await? {
            bytes.extend_from_slice(&chunk);
            progress.advance();
        }
        Ok(bytes)
    }
}
