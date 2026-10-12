//! Shared streaming verification of an already-bound ZIP artifact. Extraction
//! stops at local headers, so its owner must finish the tail before preparing.
use crate::{error::AppError, state::AppState};
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundCatError {
    Unavailable,
    Transport,
    Mismatch,
    Canceled,
}

struct MeasuredCat {
    stream: BoxStream<'static, Result<Bytes, std::io::Error>>,
    hasher: Sha256,
    size: u64,
    expected_size: u64,
    failure: Option<BoundCatError>,
}

#[derive(Clone)]
pub(crate) struct BoundCatTail {
    source: Arc<Mutex<MeasuredCat>>,
    digest: String,
    cancel: CancellationToken,
}

impl BoundCatTail {
    async fn next(&self) -> Result<Option<Bytes>, BoundCatError> {
        let mut source = self.source.lock().await;
        if let Some(error) = source.failure {
            return Err(error);
        }
        let next = tokio::select! {
            _ = self.cancel.cancelled() => return Err(BoundCatError::Canceled),
            next = source.stream.next() => next,
        };
        let bytes = match next {
            None => return Ok(None),
            Some(Ok(bytes)) => bytes,
            Some(Err(_)) => {
                source.failure = Some(BoundCatError::Transport);
                return Err(BoundCatError::Transport);
            }
        };
        let Some(size) = source
            .size
            .checked_add(bytes.len() as u64)
            .filter(|n| *n <= source.expected_size)
        else {
            source.failure = Some(BoundCatError::Mismatch);
            return Err(BoundCatError::Mismatch);
        };
        source.size = size;
        source.hasher.update(&bytes);
        Ok(Some(bytes))
    }

    pub(crate) async fn failure(&self) -> Option<BoundCatError> {
        self.source.lock().await.failure
    }

    pub(crate) async fn finish(&self) -> Result<(), BoundCatError> {
        while self.next().await?.is_some() {}
        let source = self.source.lock().await;
        if source.size != source.expected_size
            || hex::encode(source.hasher.clone().finalize()) != self.digest
        {
            return Err(BoundCatError::Mismatch);
        }
        Ok(())
    }
}

pub(crate) async fn open_bound_zip(
    state: &AppState,
    cid: &str,
    digest: &str,
    size: i64,
    cancel: CancellationToken,
) -> Result<
    (
        BoxStream<'static, Result<Bytes, std::io::Error>>,
        BoundCatTail,
    ),
    BoundCatError,
> {
    if size < 0 {
        return Err(BoundCatError::Mismatch);
    }
    let stream = tokio::select! {
        _ = cancel.cancelled() => return Err(BoundCatError::Canceled),
        stream = crate::kubo::cat::stream_cat(&state.kubo, cid, None) => stream.map_err(|error| match error {
            AppError::KuboRpc { status: Some(400..=499), .. } => BoundCatError::Unavailable,
            _ => BoundCatError::Transport,
        })?,
    };
    let tail = BoundCatTail {
        source: Arc::new(Mutex::new(MeasuredCat {
            stream: Box::pin(stream),
            hasher: Sha256::new(),
            size: 0,
            expected_size: size as u64,
            failure: None,
        })),
        digest: digest.to_owned(),
        cancel,
    };
    let for_extract = tail.clone();
    let input = async_stream::stream! {
        loop { match for_extract.next().await {
            Ok(Some(bytes)) => yield Ok(bytes),
            Ok(None) => break,
            Err(_) => { yield Err(std::io::Error::other("bound ZIP artifact read failed")); break; }
        }}
    };
    Ok((Box::pin(input), tail))
}
