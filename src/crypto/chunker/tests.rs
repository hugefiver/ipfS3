use std::collections::HashSet;
use std::sync::Arc;

use futures_util::TryStreamExt;

use super::*;
use crate::crypto::ObjectKey;
use crate::crypto::aes_gcm::{decrypt_chunk, encrypt_chunk};

fn input(chunks: Vec<Bytes>) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin {
    futures_util::stream::iter(chunks.into_iter().map(Ok))
}

#[tokio::test]
async fn encrypt_chunk_stream_uses_distinct_random_nonces_for_equal_inputs() {
    let key = Arc::new(ObjectKey { bytes: [7; 32] });
    let source = || input(vec![Bytes::from_static(b"same part")]);

    let one = encrypt_chunk_stream(source(), key.clone())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let two = encrypt_chunk_stream(source(), key)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_ne!(&one[0][..12], &two[0][..12]);
    assert_ne!(one, two);
}

#[tokio::test]
async fn encrypt_chunk_stream_assigns_a_unique_nonce_to_each_chunk() {
    let encrypted = encrypt_chunk_stream(
        input(vec![Bytes::from(vec![0x55; CHUNK_SIZE * 3 + 1])]),
        Arc::new(ObjectKey { bytes: [9; 32] }),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    let nonces: HashSet<[u8; 12]> = encrypted
        .iter()
        .map(|chunk| chunk[..12].try_into().unwrap())
        .collect();

    assert_eq!(encrypted.len(), 4);
    assert_eq!(nonces.len(), encrypted.len());
}

#[tokio::test]
async fn chunk_stream_slices_large_input_without_copying_complete_chunks() {
    let source = Bytes::from(vec![0x5a; CHUNK_SIZE * 16 + 1]);
    let chunks = chunk_stream(input(vec![source.clone()]))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(chunks.len(), 17);
    for (index, chunk) in chunks[..16].iter().enumerate() {
        assert_eq!(chunk.len(), CHUNK_SIZE);
        // The original allocation remains alive, so equal addresses prove that
        // large input suffixes were not copied into separate output buffers.
        assert_eq!(chunk.as_ptr(), source[index * CHUNK_SIZE..].as_ptr());
        assert_eq!(
            chunk.as_ref(),
            &source[index * CHUNK_SIZE..(index + 1) * CHUNK_SIZE]
        );
    }
    assert_eq!(chunks[16].as_ref(), &[0x5a]);
}

#[tokio::test]
async fn streams_keep_framing_across_fragmented_input_and_ciphertext() {
    let key = Arc::new(ObjectKey { bytes: [0x38; 32] });
    let source = Bytes::from(
        (0..CHUNK_SIZE * 3 + 17)
            .map(|index| index as u8)
            .collect::<Vec<_>>(),
    );
    let fragments = vec![
        Bytes::new(),
        source.slice(..1),
        source.slice(1..CHUNK_SIZE - 1),
        source.slice(CHUNK_SIZE - 1..),
        Bytes::new(),
    ];
    let encrypted = encrypt_chunk_stream(input(fragments), key.clone())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(encrypted.len(), 4);
    for (index, chunk) in encrypted.iter().enumerate() {
        let start = index * CHUNK_SIZE;
        let end = (start + CHUNK_SIZE).min(source.len());
        assert_eq!(chunk.len(), end - start + CHUNK_OVERHEAD);
        assert_eq!(
            decrypt_chunk(&key, chunk).unwrap(),
            source.slice(start..end)
        );
    }

    let wire = Bytes::from(encrypted.concat());
    // Split within the nonce and tag, then coalesce several complete frames.
    let tag_split = CHUNK_SIZE + CHUNK_OVERHEAD - 1;
    let fragments = vec![
        wire.slice(..1),
        wire.slice(1..tag_split),
        Bytes::new(),
        wire.slice(tag_split..),
    ];
    let decrypted = decrypt_chunk_stream(input(fragments), key)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(decrypted.len(), 4);
    assert_eq!(decrypted.concat(), source.as_ref());
}

#[tokio::test]
async fn streams_handle_empty_input_and_exact_full_chunk_without_extra_tail() {
    let key = Arc::new(ObjectKey { bytes: [0x47; 32] });
    assert!(
        chunk_stream(input(vec![Bytes::new()]))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        encrypt_chunk_stream(input(Vec::new()), key.clone())
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        decrypt_chunk_stream(input(vec![Bytes::new()]), key.clone())
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );

    let source = Bytes::from(vec![0x64; CHUNK_SIZE]);
    let encrypted = encrypt_chunk_stream(input(vec![source.clone()]), key.clone())
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(encrypted.len(), 1);
    assert_eq!(encrypted[0].len(), CHUNK_SIZE + CHUNK_OVERHEAD);
    let decrypted = decrypt_chunk_stream(input(encrypted), key)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(decrypted, vec![source]);
}

#[tokio::test]
async fn decrypt_stream_accepts_fixed_legacy_ciphertext() {
    let legacy = hex::decode(concat!(
        "000000000000000000000000",
        "cea7403d4d606b6e074ec5d3baf39d18",
        "d0d1c8a799996bf0265b98b5d48ab919",
    ))
    .unwrap();
    let plaintext = decrypt_chunk_stream(
        input(vec![Bytes::from(legacy)]),
        Arc::new(ObjectKey { bytes: [0; 32] }),
    )
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(plaintext, vec![Bytes::from_static(&[0; 16])]);
}

#[tokio::test]
async fn decrypt_stream_rejects_wrong_key_and_truncated_tail_without_plaintext() {
    let key = Arc::new(ObjectKey { bytes: [0x38; 32] });
    let encrypted = encrypt_chunk(&key, &[0x61; 12], b"secret tail").unwrap();
    for (ciphertext, decryption_key) in [
        (encrypted.clone(), Arc::new(ObjectKey { bytes: [0x39; 32] })),
        (encrypted.slice(..encrypted.len() - 1), key.clone()),
        (encrypted.slice(..CHUNK_OVERHEAD - 1), key),
    ] {
        let results = decrypt_chunk_stream(input(vec![ciphertext]), decryption_key)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], Err(AppError::Crypto(_))));
    }
}

#[tokio::test]
async fn decrypt_stream_authenticates_each_frame_before_delivery() {
    let key = Arc::new(ObjectKey { bytes: [0x38; 32] });
    let full = vec![0x23; CHUNK_SIZE];
    let mut wire = encrypt_chunk(&key, &[1; 12], &full).unwrap().to_vec();
    let mut corrupt = encrypt_chunk(&key, &[2; 12], &full).unwrap().to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    wire.extend_from_slice(&corrupt);
    wire.extend_from_slice(&encrypt_chunk(&key, &[3; 12], b"tail").unwrap());
    let results = decrypt_chunk_stream(input(vec![Bytes::from(wire)]), key)
        .collect::<Vec<_>>()
        .await;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().unwrap().as_ref(), full.as_slice());
    assert!(matches!(&results[1], Err(AppError::Crypto(_))));
    assert_eq!(results[2].as_ref().unwrap().as_ref(), b"tail");
}

#[tokio::test]
async fn framing_preserves_buffered_data_across_input_errors() {
    let source = futures_util::stream::iter(vec![
        Ok(Bytes::from_static(b"before")),
        Err(std::io::Error::other("input failed")),
        Ok(Bytes::from_static(b"after")),
    ]);
    let results = chunk_stream(source).collect::<Vec<_>>().await;
    assert_eq!(results.len(), 2);
    assert!(results[0].is_err());
    assert_eq!(results[1].as_ref().unwrap().as_ref(), b"beforeafter");
}
