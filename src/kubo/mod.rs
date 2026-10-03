pub mod add;
pub mod cat;
pub mod client;
pub mod directory;
pub mod health;
pub mod pin;
pub mod routing;
pub mod tier_copy;
pub mod verification;

pub use client::KuboClient;
pub use verification::LocalResidencyVerificationReceipt;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::error::{AppError, AppResult};

const MAX_NDJSON_RECORD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KuboProgress {
    ProviderObserved { peer_id: String },
    AddBytes { bytes: u64 },
    PinProgress { nodes: u64, bytes: u64 },
}

pub type ProgressSender = tokio::sync::mpsc::Sender<KuboProgress>;

pub(crate) fn canceled_rpc_error() -> AppError {
    AppError::kubo_rpc_detail("Kubo RPC canceled")
}

pub(crate) async fn send_request(
    request: reqwest::RequestBuilder,
    cancel: &CancellationToken,
) -> AppResult<reqwest::Response> {
    tokio::select! {
        _ = cancel.cancelled() => Err(canceled_rpc_error()),
        response = request.send() => response.map_err(AppError::from),
    }
}

pub(crate) async fn next_response_frame<S>(
    stream: &mut S,
    kubo: &KuboClient,
    cancel: &CancellationToken,
) -> AppResult<Option<Bytes>>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    tokio::select! {
        _ = cancel.cancelled() => Err(canceled_rpc_error()),
        result = tokio::time::timeout(kubo.stream_idle_timeout(), stream.next()) => {
            match result {
                Ok(Some(Ok(frame))) => Ok(Some(frame)),
                Ok(Some(Err(_))) => Err(AppError::kubo_rpc_detail("Kubo response stream failed")),
                Ok(None) => Ok(None),
                Err(_) => Err(AppError::kubo_rpc_detail("Kubo response stream timed out")),
            }
        }
    }
}

pub(crate) async fn send_progress(
    progress: &ProgressSender,
    event: KuboProgress,
    cancel: &CancellationToken,
) -> AppResult<()> {
    tokio::select! {
        _ = cancel.cancelled() => Err(canceled_rpc_error()),
        result = progress.send(event) => result.map_err(|_| AppError::kubo_rpc_detail("Kubo progress receiver closed")),
    }
}

pub(crate) struct NdjsonBuffer {
    line: Vec<u8>,
    frame: Option<Bytes>,
    frame_offset: usize,
}

impl NdjsonBuffer {
    pub(crate) fn new() -> Self {
        Self {
            line: Vec::new(),
            frame: None,
            frame_offset: 0,
        }
    }

    pub(crate) fn push(&mut self, frame: Bytes) -> AppResult<()> {
        if self.frame.is_some() {
            return Err(AppError::kubo_rpc_detail("Kubo response parser misuse"));
        }
        self.frame = Some(frame);
        self.frame_offset = 0;
        Ok(())
    }

    pub(crate) fn next_record(&mut self) -> AppResult<Option<Vec<u8>>> {
        if self.frame.is_none() {
            return Ok(None);
        }
        while self.frame_offset < self.frame.as_ref().expect("frame checked above").len() {
            let byte = self.frame.as_ref().expect("frame checked above")[self.frame_offset];
            self.frame_offset += 1;
            if byte == b'\n' {
                if let Some(record) = self.take_line() {
                    return Ok(Some(record));
                }
                continue;
            }

            if self.line.len() == MAX_NDJSON_RECORD_BYTES {
                return Err(AppError::kubo_rpc_detail(
                    "Kubo response record exceeds limit",
                ));
            }
            self.line.push(byte);
        }
        self.frame = None;
        self.frame_offset = 0;
        Ok(None)
    }

    pub(crate) fn finish(&mut self) -> Option<Vec<u8>> {
        self.take_line()
    }

    fn take_line(&mut self) -> Option<Vec<u8>> {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.line.iter().all(u8::is_ascii_whitespace) {
            self.line.clear();
            return None;
        }
        Some(std::mem::take(&mut self.line))
    }
}
