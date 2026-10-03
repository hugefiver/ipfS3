use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream};
use ipfs_s3_gateway::config::Config;
use ipfs_s3_gateway::zip::extract::{MAX_ARCHIVE_INPUT_BYTES, ZipExtractionLimits};
use ipfs_s3_gateway::zip::input_budget::{ZipInputBudget, ZipInputError};

// The same stream/error bounds accepted by add_plain_object_stream. This also
// covers boxed S3 bodies and concatenated MPU-part streams at the type boundary.
fn accepts_plain_add<S, E>(_: &S)
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
}

#[tokio::test]
async fn exact_bound_requires_clean_eof_and_counts_actual_chunks() {
    let (mut input, progress) = ZipInputBudget::new(
        stream::iter([
            Ok::<_, io::Error>(Bytes::from_static(b"ab")),
            Ok(Bytes::from_static(b"cde")),
        ]),
        5,
    );
    assert_eq!(
        input.next().await.unwrap().unwrap(),
        Bytes::from_static(b"ab")
    );
    assert!(!progress.clean_eof());
    assert_eq!(
        input.next().await.unwrap().unwrap(),
        Bytes::from_static(b"cde")
    );
    assert_eq!(progress.total_bytes(), 5);
    assert!(!progress.clean_eof());
    assert!(input.next().await.is_none());
    assert!(progress.clean_eof());
    assert!(input.next().await.is_none());
}

#[tokio::test]
async fn concatenated_part_stream_shares_one_archive_budget() {
    let first = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"part1"))]);
    let second = stream::iter([Ok::<_, io::Error>(Bytes::from_static(b"part2"))]);
    let (mut input, progress) = ZipInputBudget::new(first.chain(second), 9);
    accepts_plain_add(&input);
    assert_eq!(
        input.next().await.unwrap().unwrap(),
        Bytes::from_static(b"part1")
    );
    assert!(matches!(
        input.next().await,
        Some(Err(ZipInputError::LimitExceeded(_)))
    ));
    assert_eq!(progress.total_bytes(), 5);
    assert!(!progress.clean_eof());
}

#[tokio::test]
async fn one_byte_over_is_not_forwarded_or_counted() {
    let polls = Arc::new(AtomicUsize::new(0));
    let source = stream::iter([
        Ok::<_, io::Error>(Bytes::from_static(b"12345")),
        Ok(Bytes::from_static(b"6")),
        Ok(Bytes::from_static(b"should not poll")),
    ])
    .inspect({
        let polls = polls.clone();
        move |_| {
            polls.fetch_add(1, Ordering::SeqCst);
        }
    });
    let (mut input, progress) = ZipInputBudget::new(source, 5);
    assert_eq!(input.next().await.unwrap().unwrap().len(), 5);
    let error = input.next().await.unwrap().unwrap_err();
    let ZipInputError::LimitExceeded(error) = error else {
        panic!("expected raw ZIP input limit error");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(input.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(progress.total_bytes(), 5);
    assert!(!progress.clean_eof());
}

#[tokio::test]
async fn straddling_chunk_is_entirely_rejected() {
    let (mut input, progress) = ZipInputBudget::new(
        stream::iter([
            Ok::<_, io::Error>(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ]),
        5,
    );
    assert_eq!(
        input.next().await.unwrap().unwrap(),
        Bytes::from_static(b"abc")
    );
    assert!(matches!(
        input.next().await,
        Some(Err(ZipInputError::LimitExceeded(_)))
    ));
    assert_eq!(progress.total_bytes(), 3);
    assert!(!progress.clean_eof());
}

#[tokio::test]
async fn late_source_error_is_preserved_and_never_marks_eof() {
    let late = io::Error::new(io::ErrorKind::ConnectionReset, "upstream failed late");
    let (mut input, progress) =
        ZipInputBudget::new(stream::iter([Ok(Bytes::from_static(b"123")), Err(late)]), 5);
    assert_eq!(input.next().await.unwrap().unwrap().len(), 3);
    let Some(Err(error)) = input.next().await else {
        panic!("expected original late source error");
    };
    assert_eq!(error.to_string(), "upstream failed late");
    let ZipInputError::Upstream(error) = error else {
        panic!("expected unchanged upstream error");
    };
    assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(error.to_string(), "upstream failed late");
    assert!(input.next().await.is_none());
    assert_eq!(progress.total_bytes(), 3);
    assert!(!progress.clean_eof());
}

#[tokio::test]
async fn cancelling_drops_source_without_reporting_success() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_in_source = dropped.clone();
    let source = Box::pin(async_stream::stream! {
        let _guard = DropFlag(dropped_in_source);
        yield Ok::<_, io::Error>(Bytes::from_static(b"partial"));
        std::future::pending::<()>().await;
    });
    let flag = dropped.clone();
    let (mut input, progress) = ZipInputBudget::new(source, 10);
    assert_eq!(input.next().await.unwrap().unwrap().len(), 7);
    drop(input);
    assert!(flag.load(Ordering::SeqCst));
    assert_eq!(progress.total_bytes(), 7);
    assert!(!progress.clean_eof());
}

#[test]
fn configurable_raw_input_limit_preserves_decompression_defaults() {
    let defaults: Config = toml::from_str("").unwrap();
    assert_eq!(
        defaults.decompress_zip.limits.max_archive_bytes(),
        MAX_ARCHIVE_INPUT_BYTES
    );
    assert_eq!(
        defaults.decompress_zip.limits.max_decompressed_bytes(),
        8 * 1024 * 1024 * 1024
    );
    assert_eq!(defaults.decompress_zip.limits.max_entries(), 10_000);
    assert_eq!(
        defaults.decompress_zip.limits.max_metadata_bytes(),
        64 * 1024 * 1024
    );
    let configured: Config = toml::from_str("[decompress_zip]\nmax_archive_bytes = 7").unwrap();
    assert_eq!(configured.decompress_zip.limits.max_archive_bytes(), 7);
    assert_eq!(
        ZipExtractionLimits::default()
            .with_archive_bytes(7)
            .unwrap()
            .max_archive_bytes(),
        7
    );
    assert_eq!(
        ZipExtractionLimits::default()
            .with_archive_bytes(64 * 1024 * 1024 * 1024)
            .unwrap()
            .max_archive_bytes(),
        64 * 1024 * 1024 * 1024
    );
    assert!(
        ZipExtractionLimits::default()
            .with_archive_bytes(0)
            .is_err()
    );
    assert!(
        ZipExtractionLimits::default()
            .with_archive_bytes(64 * 1024 * 1024 * 1024 + 1)
            .is_err()
    );
    assert!(toml::from_str::<Config>("[decompress_zip]\nmax_archive_bytes = 68719476737").is_err());
}
