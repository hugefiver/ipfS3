use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use crate::error::AppError;

pub const CHUNK_SIZE: usize = 256 * 1024;

/// Collect a byte stream into chunks of CHUNK_SIZE.
pub fn chunk_stream<S, E>(stream: S) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut buf = Vec::with_capacity(CHUNK_SIZE);
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buf.extend_from_slice(&bytes);
                    while buf.len() >= CHUNK_SIZE {
                        let split = buf.split_off(CHUNK_SIZE);
                        yield Ok(Bytes::from(std::mem::take(&mut buf)));
                        buf = split;
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
    object_id: String,
    part_number: u32,
) -> impl Stream<Item = Result<Bytes, AppError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    use super::aes_gcm::encrypt_chunk;
    let mut chunk_index: u64 = 0;
    async_stream::stream! {
        let mut chunked = Box::pin(chunk_stream::<_, E>(stream));
        while let Some(chunk) = chunked.next().await {
            match chunk {
                Ok(bytes) => {
                    let nonce = ok.derive_nonce(&object_id, part_number, chunk_index);
                    let encrypted = encrypt_chunk(&ok, &nonce, &bytes)?;
                    chunk_index += 1;
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
    use super::aes_gcm::decrypt_chunk;
    // Each encrypted chunk on the wire is: nonce(12) + ciphertext + tag(16) =
    // CHUNK_SIZE + 28 bytes (for full chunks). The last chunk may be smaller.
    // HTTP/TCP packet boundaries do NOT align with chunk boundaries, so we must
    // buffer and split at CIPHER_CHUNK boundaries before decrypting.
    const OVERHEAD: usize = 12 + 16; // nonce + GCM tag
    const CIPHER_CHUNK: usize = CHUNK_SIZE + OVERHEAD;
    let mut buf = Vec::with_capacity(CIPHER_CHUNK * 2);
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buf.extend_from_slice(&bytes);
                    // Decrypt all complete cipher chunks currently in the buffer.
                    while buf.len() >= CIPHER_CHUNK {
                        let rest = buf.split_off(CIPHER_CHUNK);
                        let cipher_chunk = std::mem::replace(&mut buf, rest);
                        match decrypt_chunk(&ok, &cipher_chunk) {
                            Ok(pt) => yield Ok(pt),
                            Err(e) => yield Err(e),
                        }
                    }
                }
                Err(e) => yield Err(AppError::Internal(e.into().to_string())),
            }
        }
        // Decrypt any remaining bytes (the final, possibly smaller chunk).
        if !buf.is_empty() {
            match decrypt_chunk(&ok, &buf) {
                Ok(pt) => yield Ok(pt),
                Err(e) => yield Err(e),
            }
        }
    }
}
