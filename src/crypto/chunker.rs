use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use crate::error::AppError;

use super::aes_gcm::{CHUNK_OVERHEAD, ChunkCipher};

pub const CHUNK_SIZE: usize = 256 * 1024;

/// Collect a byte stream into chunks of CHUNK_SIZE.
pub fn chunk_stream<S, E>(stream: S) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    frame_stream(stream, CHUNK_SIZE)
}

// Complete frames borrow their input storage; only a frame spanning input items
// needs a buffer. Never copy the unconsumed suffix of a large input item.
fn frame_stream<S, E>(stream: S, frame_size: usize) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    async_stream::stream! {
        let mut stream = stream;
        let mut buf = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(mut bytes) => {
                    while !bytes.is_empty() {
                        if buf.is_empty() && bytes.len() >= frame_size {
                            yield Ok(bytes.split_to(frame_size));
                        } else {
                            if buf.is_empty() {
                                buf.reserve(frame_size);
                            }
                            let len = (frame_size - buf.len()).min(bytes.len());
                            buf.extend_from_slice(&bytes.split_to(len));
                            if buf.len() == frame_size {
                                yield Ok(Bytes::from(std::mem::take(&mut buf)));
                            }
                        }
                    }
                }
                Err(e) => yield Err(e),
            }
        }
        if !buf.is_empty() {
            yield Ok(Bytes::from(buf));
        }
    }
}

pub fn encrypt_chunk_stream<S, E>(
    stream: S,
    ok: std::sync::Arc<super::key::ObjectKey>,
) -> impl Stream<Item = Result<Bytes, AppError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    use rand::Rng;

    async_stream::stream! {
        let cipher = ChunkCipher::new(&ok);
        let mut chunked = std::pin::pin!(chunk_stream::<_, E>(stream));
        while let Some(chunk) = chunked.next().await {
            match chunk {
                Ok(bytes) => {
                    let mut nonce = [0u8; 12];
                    rand::rng().fill_bytes(&mut nonce);
                    let encrypted = cipher.encrypt(&nonce, &bytes)?;
                    yield Ok(encrypted);
                }
                Err(e) => {
                    yield Err(AppError::Internal(e.into().to_string()));
                }
            }
        }
    }
}

pub fn decrypt_chunk_stream<S, E>(
    stream: S,
    ok: std::sync::Arc<super::key::ObjectKey>,
) -> impl Stream<Item = Result<Bytes, AppError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    // Each encrypted chunk on the wire is: nonce(12) + ciphertext + tag(16) =
    // CHUNK_SIZE + 28 bytes (for full chunks). The last chunk may be smaller.
    // HTTP/TCP packet boundaries do NOT align with chunk boundaries, so we must
    // buffer and split at CIPHER_CHUNK boundaries before decrypting.
    const CIPHER_CHUNK: usize = CHUNK_SIZE + CHUNK_OVERHEAD;
    async_stream::stream! {
        let cipher = ChunkCipher::new(&ok);
        let mut framed = std::pin::pin!(frame_stream(stream, CIPHER_CHUNK));
        while let Some(chunk) = framed.next().await {
            match chunk {
                Ok(bytes) => yield cipher.decrypt(&bytes),
                Err(e) => {
                    let error: Box<dyn std::error::Error + Send + Sync> = e.into();
                    if crate::error::has_kubo_stream_provenance(error.as_ref()) {
                        yield Err(AppError::kubo_rpc_detail("Kubo response stream failed"));
                    } else {
                        yield Err(AppError::Internal(error.to_string()));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
