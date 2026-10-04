use super::error::protocol;
use crate::pinning::provider::ProviderError;
use bytes::Bytes;
use http_body_util::BodyExt as _;

/// Retains trailer frames (bytes_stream would discard them) and counts actual
/// payload bytes through HTTP EOF. Never buffers an object or an entire CAR.
pub(super) struct ResponseBody {
    body: reqwest::Body,
    expected: Option<u64>,
    received: u64,
    limit: Option<u64>,
}

impl ResponseBody {
    pub fn new(response: reqwest::Response, limit: Option<u64>) -> Result<Self, ProviderError> {
        if response.headers().contains_key("x-stream-error") {
            return Err(protocol("RPC response reported stream failure"));
        }
        if response
            .headers()
            .contains_key(reqwest::header::CONTENT_RANGE)
        {
            return Err(protocol("RPC response reported partial content"));
        }
        let expected = response.content_length();
        if expected
            .zip(limit)
            .is_some_and(|(length, limit)| length > limit)
        {
            return Err(protocol("RPC response exceeds limit"));
        }
        Ok(Self {
            body: response.into(),
            expected,
            received: 0,
            limit,
        })
    }

    pub async fn next(&mut self) -> Result<Option<Bytes>, ProviderError> {
        loop {
            let Some(frame) = self.body.frame().await else {
                if self.expected.is_some_and(|length| length != self.received) {
                    return Err(protocol("RPC response ended before Content-Length"));
                }
                return Ok(None);
            };
            let frame = frame.map_err(|_| protocol("RPC response stream failed"))?;
            match frame.into_data() {
                Ok(data) => {
                    self.received = self
                        .received
                        .checked_add(data.len() as u64)
                        .ok_or_else(|| protocol("RPC response exceeds limit"))?;
                    if self.limit.is_some_and(|limit| self.received > limit) {
                        return Err(protocol("RPC response exceeds limit"));
                    }
                    if !data.is_empty() {
                        return Ok(Some(data));
                    }
                }
                Err(frame) => {
                    let trailers = frame
                        .into_trailers()
                        .map_err(|_| protocol("invalid RPC body frame"))?;
                    if trailers.contains_key("x-stream-error") {
                        return Err(protocol("RPC response trailer reported stream failure"));
                    }
                }
            }
        }
    }
}
