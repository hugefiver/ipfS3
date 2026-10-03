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
    armed: bool,
    header: Vec<u8>,
    descriptor: bool,
    position: u64,
    entries_left: u64,
    metadata_left: u64,
    prefix_bytes: u64,
    limit_exceeded: bool,
}

impl ProbeState {
    fn new() -> Self {
        Self {
            armed: false,
            header: Vec::with_capacity(LOCAL_HEADER_LEN),
            descriptor: false,
            position: 0,
            entries_left: super::extract::MAX_ARCHIVE_ENTRIES,
            metadata_left: super::extract::MAX_ARCHIVE_METADATA_BYTES,
            prefix_bytes: 0,
            limit_exceeded: false,
        }
    }
}

pub struct LocalHeaderObserver<R> {
    inner: BufReader<R>,
    shared: Arc<Mutex<ProbeState>>,
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
        },
        LocalHeaderProbe { shared },
    )
}

impl LocalHeaderProbe {
    pub(super) fn set_budget(&self, entries: u64, metadata: u64, prefix_bytes: usize) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.entries_left = entries;
        state.metadata_left = metadata;
        state.prefix_bytes = prefix_bytes as u64;
    }

    pub(super) fn limit_exceeded(&self) -> bool {
        self.shared
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .limit_exceeded
    }

    pub(super) fn position(&self) -> u64 {
        self.shared
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .position
    }

    pub(super) fn begin_descriptor(&self) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.armed = true;
        state.descriptor = true;
        state.header.clear();
    }

    pub(super) fn descriptor_matches(&self, crc: u32, compressed: u64, size: u64) -> bool {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.armed = false;
        let bytes = state.header.as_slice();
        let bytes = if bytes.starts_with(&0x0807_4b50_u32.to_le_bytes()) {
            &bytes[4..]
        } else {
            bytes
        };
        bytes.len() == 12
            && u32_at(bytes, 0) == crc
            && u64::from(u32_at(bytes, 4)) == compressed
            && u64::from(u32_at(bytes, 8)) == size
    }

    pub fn begin(&self) {
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.armed = true;
        state.descriptor = false;
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

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed ZIP field"),
    )
}

fn observe_consumed(shared: &Arc<Mutex<ProbeState>>, bytes: &[u8]) -> io::Result<()> {
    let mut state = shared.lock().unwrap_or_else(|error| error.into_inner());
    state.position += bytes.len() as u64;
    let length = if state.descriptor {
        16
    } else {
        LOCAL_HEADER_LEN
    };
    if !state.armed || state.header.len() == length {
        return Ok(());
    }
    let length = bytes.len().min(length - state.header.len());
    state.header.extend_from_slice(&bytes[..length]);
    if !state.descriptor
        && state.header.len() == LOCAL_HEADER_LEN
        && state.header[..4] == LOCAL_HEADER_SIGNATURE
    {
        let name = u16::from_le_bytes([state.header[26], state.header[27]]) as u64;
        let extra = u16::from_le_bytes([state.header[28], state.header[29]]) as u64;
        // Reserve fixed result/error bookkeeping plus copies of names, keys and
        // extra fields (including Unicode aliases) retained by parser/observers.
        let charge =
            4096_u64.saturating_add(8_u64.saturating_mul(name + extra + state.prefix_bytes));
        if state.entries_left == 0 || state.metadata_left < charge {
            state.limit_exceeded = true;
            return Err(io::Error::other("ZIP entry or metadata budget exceeded"));
        }
        state.entries_left -= 1;
        state.metadata_left -= charge;
    }
    Ok(())
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
        let cap = {
            let state = this
                .shared
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.armed && !state.descriptor && state.header.len() < LOCAL_HEADER_LEN {
                buffer
                    .remaining()
                    .min(LOCAL_HEADER_LEN - state.header.len())
            } else {
                buffer.remaining()
            }
        };
        let mut limited = ReadBuf::new(&mut buffer.initialize_unfilled()[..cap]);
        match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
            Poll::Ready(Ok(())) => {
                let count = limited.filled().len();
                let result = observe_consumed(&this.shared, limited.filled());
                buffer.advance(count);
                Poll::Ready(result)
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
        // async_zip reads fixed headers through AsyncRead; the decompressor uses
        // fill_buf/consume. Count only consumed bytes, never decoder read-ahead.
        Pin::new(&mut this.inner).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.shared
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .position += amount as u64;
        Pin::new(&mut this.inner).consume(amount);
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

    #[tokio::test]
    async fn metadata_reservation_exactly_at_64_mib_and_one_byte_below() {
        // 10,000 * 4096 fixed units + 8 * 3,268,608 name bytes = 64 MiB.
        // Reservation granularity is eight bytes; changing the budget by one
        // byte must reject before any variable-length name is read.
        let headers: Vec<_> = (0..10_000)
            .map(|index| {
                let mut header = local_header(0, 0);
                let name_len: u16 = if index < 8_608 { 327 } else { 326 };
                header[26..28].copy_from_slice(&name_len.to_le_bytes());
                Bytes::from(header)
            })
            .collect();
        for (budget, accepted) in [
            (super::super::extract::MAX_ARCHIVE_METADATA_BYTES, true),
            (super::super::extract::MAX_ARCHIVE_METADATA_BYTES - 1, false),
        ] {
            let reader = StreamReader::new(stream::iter(
                headers.iter().cloned().map(Ok::<Bytes, io::Error>),
            ));
            let (mut observer, probe) = observe_local_headers(reader);
            probe.set_budget(10_000, budget, 0);
            for index in 0..10_000 {
                probe.begin();
                let mut header = [0_u8; 30];
                let read = observer.read_exact(&mut header).await;
                if index == 9_999 && !accepted {
                    assert!(read.is_err());
                    assert!(probe.limit_exceeded());
                } else {
                    read.unwrap();
                    probe.take().unwrap();
                }
            }
        }
    }
}
