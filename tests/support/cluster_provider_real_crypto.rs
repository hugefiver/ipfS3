//! Real product encryption format; only bounded fixture bytes are collected.
use anyhow::{Result, ensure};
use bytes::Bytes;
use futures_util::{TryStreamExt, stream};
use ipfs_s3_gateway::crypto::{ObjectKey, chunker};
use std::sync::Arc;

fn key() -> Arc<ObjectKey> {
    Arc::new(ObjectKey { bytes: [0x71; 32] }) // disposable fixture key, never sent
}

pub async fn encrypt(plaintext: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        plaintext.len() <= 1024 * 1024,
        "crypto fixture exceeds budget"
    );
    let input = stream::iter([Ok::<_, std::io::Error>(Bytes::copy_from_slice(plaintext))]);
    let chunks: Vec<Bytes> = chunker::encrypt_chunk_stream(input, key())
        .try_collect()
        .await?;
    Ok(chunks.into_iter().flatten().collect())
}

pub async fn decrypt(stored: &[u8]) -> Result<Vec<u8>> {
    ensure!(
        stored.len() <= 1024 * 1024 + 128,
        "stored fixture exceeds budget"
    );
    // Deliberately non-aligned TCP-like boundaries, not a single AES chunk.
    let input = stream::iter(
        stored
            .chunks(4093)
            .map(|chunk| Ok::<_, std::io::Error>(Bytes::copy_from_slice(chunk))),
    );
    let chunks: Vec<Bytes> = chunker::decrypt_chunk_stream(input, key())
        .try_collect()
        .await?;
    Ok(chunks.into_iter().flatten().collect())
}
