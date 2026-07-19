use std::cell::Cell;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf};

const LOCAL_HEADER_LEN: usize = 30;
const LOCAL_HEADER_SIGNATURE: [u8; 4] = [0x50, 0x4b, 0x03, 0x04];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalHeaderMeta {
    pub general_purpose_flags: u16,
    pub compression_method: u16,
}

impl LocalHeaderMeta {
    pub fn uses_descriptor(self) -> bool {
        self.general_purpose_flags & (1 << 3) != 0
    }
}

struct ProbeState {
    generation: u64,
    armed: bool,
    header: Vec<u8>,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            generation: 0,
            armed: false,
            header: Vec::with_capacity(LOCAL_HEADER_LEN),
        }
    }
}

pub struct LocalHeaderObserver<R> {
    inner: BufReader<R>,
    shared: Arc<Mutex<ProbeState>>,
    seen_generation: u64,
    fill_observed: Cell<usize>,
}

#[derive(Clone)]
pub struct LocalHeaderProbe {
    shared: Arc<Mutex<ProbeState>>,
}

pub fn observe_local_headers<R>(reader: R) -> (LocalHeaderObserver<R>, LocalHeaderProbe)
where
    R: AsyncRead + Unpin,
{
    let shared = Arc::new(Mutex::new(ProbeState::new()));
    (
        LocalHeaderObserver {
            inner: BufReader::new(reader),
            shared: shared.clone(),
            seen_generation: 0,
            fill_observed: Cell::new(0),
        },
        LocalHeaderProbe { shared },
    )
}

impl LocalHeaderProbe {
    pub fn begin(&self) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.generation = state.generation.wrapping_add(1);
        state.armed = true;
        state.header.clear();
    }

    pub fn take(&self) -> io::Result<LocalHeaderMeta> {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.armed = false;
        if state.header.len() != LOCAL_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zip local header is shorter than 30 bytes",
            ));
        }
        if state.header[..4] != LOCAL_HEADER_SIGNATURE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zip local header signature is invalid",
            ));
        }

        Ok(LocalHeaderMeta {
            general_purpose_flags: u16::from_le_bytes([state.header[6], state.header[7]]),
            compression_method: u16::from_le_bytes([state.header[8], state.header[9]]),
        })
    }
}

fn observe_prefix(shared: &Arc<Mutex<ProbeState>>, bytes: &[u8]) -> usize {
    let mut state = shared.lock().unwrap_or_else(|error| error.into_inner());
    if !state.armed || state.header.len() == LOCAL_HEADER_LEN {
        return 0;
    }

    let length = bytes.len().min(LOCAL_HEADER_LEN - state.header.len());
    state.header.extend_from_slice(&bytes[..length]);
    length
}

impl<R> LocalHeaderObserver<R> {
    fn sync_generation(&mut self) {
        let generation = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .generation;
        if self.seen_generation != generation {
            self.seen_generation = generation;
            self.fill_observed.set(0);
        }
    }
}

impl<R> AsyncRead for LocalHeaderObserver<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.sync_generation();
        let before = buffer.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buffer) {
            Poll::Ready(Ok(())) => {
                observe_prefix(&this.shared, &buffer.filled()[before..]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> AsyncBufRead for LocalHeaderObserver<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        this.sync_generation();
        let shared = &this.shared;
        let fill_observed = &this.fill_observed;
        let previously_observed = fill_observed.get();

        match Pin::new(&mut this.inner).poll_fill_buf(cx) {
            Poll::Ready(Ok(buffer)) => {
                let start = previously_observed.min(buffer.len());
                let observed = observe_prefix(shared, &buffer[start..]);
                fill_observed.set(start + observed);
                Poll::Ready(Ok(buffer))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.sync_generation();
        Pin::new(&mut this.inner).consume(amount);
        this.fill_observed
            .set(this.fill_observed.get().saturating_sub(amount));
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use futures_util::stream;
    use tokio::io::AsyncReadExt;
    use tokio_util::io::StreamReader;

    use super::observe_local_headers;

    fn local_header(flags: u16, method: u16) -> Vec<u8> {
        let mut header = vec![0; 30];
        header[..4].copy_from_slice(&[0x50, 0x4b, 0x03, 0x04]);
        header[6..8].copy_from_slice(&flags.to_le_bytes());
        header[8..10].copy_from_slice(&method.to_le_bytes());
        header
    }

    async fn observe_fragmented_header(flags: u16, method: u16) {
        let chunks = local_header(flags, method)
            .into_iter()
            .map(|byte| Ok::<Bytes, io::Error>(Bytes::from(vec![byte])));
        let reader = StreamReader::new(stream::iter(chunks));
        let (mut observer, probe) = observe_local_headers(reader);

        probe.begin();
        let mut consumed = [0; 30];
        observer.read_exact(&mut consumed).await.unwrap();

        let meta = probe.take().unwrap();
        assert_eq!(meta.general_purpose_flags, flags);
        assert_eq!(meta.compression_method, method);
    }

    #[tokio::test]
    async fn observes_stored_header_from_one_byte_chunks() {
        observe_fragmented_header(0, 0).await;
    }

    #[tokio::test]
    async fn observes_deflate_descriptor_header_from_one_byte_chunks() {
        observe_fragmented_header(8, 8).await;
    }

    #[tokio::test]
    async fn observes_stored_descriptor_header_from_one_byte_chunks() {
        observe_fragmented_header(8, 0).await;
    }
}
