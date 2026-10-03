//! Request-scoped raw ZIP input accounting before staging bytes in Kubo.
//! This bounds bytes accepted from a source stream, not Kubo's physical disk use.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;

#[derive(Debug, thiserror::Error)]
pub enum ZipInputError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Upstream(#[from] E),
    #[error("ZIP raw input byte limit exceeded")]
    LimitExceeded(#[source] io::Error),
}

#[derive(Default)]
struct InputState {
    total: AtomicU64,
    clean_eof: AtomicBool,
    limit_exceeded: AtomicBool,
}

/// The count is of forwarded bytes (not a Content-Length claim). A completed
/// upload is valid only if `clean_eof()` is true after consuming the stream.
#[derive(Clone)]
pub struct ZipInputProgress(Arc<InputState>);

impl ZipInputProgress {
    pub fn total_bytes(&self) -> u64 {
        self.0.total.load(Ordering::Relaxed)
    }

    pub fn clean_eof(&self) -> bool {
        self.0.clean_eof.load(Ordering::Acquire)
    }

    pub fn limit_exceeded(&self) -> bool {
        self.0.limit_exceeded.load(Ordering::Acquire)
    }
}

/// Forwards each `Bytes` chunk unchanged unless the entire chunk would exceed
/// `max_bytes`. The first over-limit chunk is never forwarded; the stream then
/// terminates. Source errors are preserved in `ZipInputError::Upstream`.
pub struct ZipInputBudget<S> {
    source: S,
    max_bytes: u64,
    state: Arc<InputState>,
    terminated: bool,
}

impl<S> ZipInputBudget<S> {
    pub fn new(source: S, max_bytes: u64) -> (Self, ZipInputProgress) {
        let state = Arc::new(InputState::default());
        (
            Self {
                source,
                max_bytes,
                state: state.clone(),
                terminated: false,
            },
            ZipInputProgress(state),
        )
    }
}

impl<S, E> Stream for ZipInputBudget<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::error::Error + 'static,
{
    type Item = Result<Bytes, ZipInputError<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.source).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.terminated = true;
                this.state.clean_eof.store(true, Ordering::Release);
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.terminated = true;
                Poll::Ready(Some(Err(ZipInputError::Upstream(error))))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                let total = this.state.total.load(Ordering::Relaxed);
                let next = u64::try_from(bytes.len())
                    .ok()
                    .and_then(|len| total.checked_add(len));
                match next {
                    Some(next) if next <= this.max_bytes => {
                        this.state.total.store(next, Ordering::Relaxed);
                        Poll::Ready(Some(Ok(bytes)))
                    }
                    _ => {
                        this.terminated = true;
                        this.state.limit_exceeded.store(true, Ordering::Release);
                        Poll::Ready(Some(Err(ZipInputError::LimitExceeded(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "ZIP raw archive input exceeds the byte limit",
                        )))))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream};

    #[tokio::test]
    async fn limit_flag_distinguishes_rejected_chunk_from_upstream_error_and_eof() {
        let (mut over, over_progress) = ZipInputBudget::new(
            stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"ab"))]),
            1,
        );
        assert!(matches!(
            over.next().await,
            Some(Err(ZipInputError::LimitExceeded(_)))
        ));
        assert!(over_progress.limit_exceeded());
        assert!(!over_progress.clean_eof());

        let (mut upstream, upstream_progress) = ZipInputBudget::new(
            stream::iter([Err::<Bytes, _>(io::Error::other("source failed"))]),
            1,
        );
        assert!(matches!(
            upstream.next().await,
            Some(Err(ZipInputError::Upstream(_)))
        ));
        assert!(!upstream_progress.limit_exceeded());
        assert!(!upstream_progress.clean_eof());

        let (mut clean, clean_progress) = ZipInputBudget::new(
            stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"a"))]),
            1,
        );
        assert_eq!(
            clean.next().await.unwrap().unwrap(),
            Bytes::from_static(b"a")
        );
        assert!(clean.next().await.is_none());
        assert!(clean_progress.clean_eof());
        assert!(!clean_progress.limit_exceeded());
    }
}
