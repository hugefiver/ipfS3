use super::*;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

fn ciphertext_fixture(size: usize) -> (Arc<crate::crypto::ObjectKey>, Vec<Bytes>) {
    let key = Arc::new(crate::crypto::ObjectKey { bytes: [0x42; 32] });
    let plaintext = vec![0x5a; crate::crypto::chunker::CHUNK_SIZE];
    let chunks = (0..size)
        .step_by(plaintext.len())
        .enumerate()
        .map(|(index, offset)| {
            let mut nonce = [0; 12];
            nonce[..8].copy_from_slice(&(index as u64).to_le_bytes());
            crate::crypto::aes_gcm::encrypt_chunk(
                &key,
                &nonce,
                &plaintext[..plaintext.len().min(size - offset)],
            )
            .unwrap()
        })
        .collect();
    (key, chunks)
}

struct RangeWork {
    read_bytes: u64,
    first_byte_read_bytes: u64,
    returned_bytes: u64,
    first_byte: Duration,
    completed: Duration,
}

async fn measure_range(
    key: Arc<crate::crypto::ObjectKey>,
    chunks: &[Bytes],
    range: (u64, u64),
) -> RangeWork {
    let read = Arc::new(AtomicU64::new(0));
    let observed = read.clone();
    let input = futures_util::stream::iter(chunks.to_vec()).map(move |chunk| {
        observed.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        Ok::<_, std::io::Error>(chunk)
    });
    let mut body = encrypted_object_body(input, key, Some(range), "authentication failed");
    let started = Instant::now();
    let mut first_byte = None;
    let mut first_byte_read_bytes = 0;
    let mut returned_bytes = 0;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap();
        if first_byte.is_none() {
            first_byte = Some(started.elapsed());
            first_byte_read_bytes = read.load(Ordering::Relaxed);
        }
        assert!(chunk.iter().all(|byte| *byte == 0x5a));
        returned_bytes += chunk.len() as u64;
    }
    let completed = started.elapsed();
    let read_bytes = read.load(Ordering::Relaxed);
    assert_eq!(returned_bytes, range.1 - range.0);
    assert_eq!(
        read_bytes,
        chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>()
    );
    RangeWork {
        read_bytes,
        first_byte_read_bytes,
        returned_bytes,
        first_byte: first_byte.unwrap(),
        completed,
    }
}

#[tokio::test]
async fn encrypted_range_success_authenticates_all_input_positions() {
    let boundary = crate::crypto::chunker::CHUNK_SIZE as u64;
    let size = boundary * 4 + 7;
    let (key, chunks) = ciphertext_fixture(size as usize);
    for range in [(0, 4), (boundary - 2, boundary + 2), (size - 4, size)] {
        let work = measure_range(key.clone(), &chunks, range).await;
        if range.0 == 0 || range.1 == size {
            assert_eq!(work.first_byte_read_bytes, work.read_bytes);
        } else {
            assert!(work.first_byte_read_bytes < work.read_bytes);
        }
    }
}

#[tokio::test]
#[ignore = "encrypted Range body dataflow cost measurement"]
async fn encrypted_range_body_cost() {
    for size in [8 * 1024 * 1024, 64 * 1024 * 1024] {
        let (key, chunks) = ciphertext_fixture(size);
        let boundary = crate::crypto::chunker::CHUNK_SIZE as u64;
        for (position, range) in [
            ("head", (0, 4096)),
            ("cross_chunk", (boundary - 2048, boundary + 2048)),
            ("tail", (size as u64 - 4096, size as u64)),
        ] {
            for sample in 0..3 {
                let work = measure_range(key.clone(), &chunks, range).await;
                eprintln!(
                    "encrypted_range_body plaintext_bytes={size} position={position} sample={sample} ciphertext_read={} response_bytes={} read_before_first_byte={} first_byte_us={} completion_us={}",
                    work.read_bytes,
                    work.returned_bytes,
                    work.first_byte_read_bytes,
                    work.first_byte.as_micros(),
                    work.completed.as_micros(),
                );
            }
        }
    }
}
