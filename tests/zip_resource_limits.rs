use bytes::Bytes;
use futures_util::stream;
use ipfs_s3_gateway::config::Config;
use ipfs_s3_gateway::crypto::key::MasterKey;
use ipfs_s3_gateway::kubo::KuboClient;
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store::Store;
use ipfs_s3_gateway::zip::extract::{
    ExtractionObserver, MAX_ARCHIVE_ENTRIES, MAX_ARCHIVE_METADATA_BYTES,
    MAX_DECOMPRESSED_ARCHIVE_BYTES, ObservedExtractionError, ZipExtractionLimits,
    extract_zip_stream_observed_with_limits,
};
use ipfs_s3_gateway::zip::response::{ExtractFailure, ExtractedEntry};
use sea_orm::Database;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use wiremock::MockServer;

async fn test_state() -> (Arc<AppState>, MockServer) {
    let kubo = MockServer::start().await;
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
    let state = Arc::new(AppState {
        kubo: KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::new(),
        master_key: MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap(),
        pinning: ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::disabled_for_test(),
    });
    (state, kubo)
}

fn deflated_directory_bomb() -> Vec<u8> {
    let name = b"bomb/";
    // Raw deflate of 1024 zero bytes (11 compressed bytes).
    let compressed = [
        0x63, 0x60, 0x18, 0x05, 0xa3, 0x60, 0x14, 0x8c, 0x54, 0x00, 0x00,
    ];
    let mut crc = !0u32;
    for _ in 0..1024 {
        crc ^= 0;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    crc = !crc;
    let mut archive = Vec::new();
    archive.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(&8_u16.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&crc.to_le_bytes());
    archive.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    archive.extend_from_slice(&1024_u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(name);
    archive.extend_from_slice(&compressed);
    let central_offset = archive.len() as u32;
    archive.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&20_u16.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive.extend_from_slice(&8_u16.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&crc.to_le_bytes());
    archive.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    archive.extend_from_slice(&1024_u32.to_le_bytes());
    archive.extend_from_slice(&(name.len() as u16).to_le_bytes());
    archive.extend_from_slice(&[0; 12]);
    archive.extend_from_slice(&0_u32.to_le_bytes());
    archive.extend_from_slice(name);
    let central_size = archive.len() as u32 - central_offset;
    archive.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    archive.extend_from_slice(&[0; 4]);
    archive.extend_from_slice(&1_u16.to_le_bytes());
    archive.extend_from_slice(&1_u16.to_le_bytes());
    archive.extend_from_slice(&central_size.to_le_bytes());
    archive.extend_from_slice(&central_offset.to_le_bytes());
    archive.extend_from_slice(&0_u16.to_le_bytes());
    archive
}

#[derive(Default)]
struct ByteObserver(u64);

#[async_trait::async_trait]
impl ExtractionObserver for ByteObserver {
    type Error = io::Error;
    async fn entry_started(&mut self, _: &str) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn entry_finished(&mut self, _: &ExtractedEntry) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn entry_failed(&mut self, _: &str, _: &ExtractFailure) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error> {
        self.0 += bytes;
        Ok(())
    }
}

#[tokio::test]
async fn compressed_directory_is_charged_by_streamed_inflated_bytes() {
    let (state, kubo) = test_state().await;
    let archive = deflated_directory_bomb();
    let chunks: Vec<_> = archive
        .chunks(3)
        .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk)))
        .collect();
    let mut observer = ByteObserver::default();
    let limits = ZipExtractionLimits::default()
        .with_single_entry_bytes(1023)
        .unwrap();
    let result = extract_zip_stream_observed_with_limits(
        &state,
        "",
        stream::iter(chunks),
        limits,
        &mut observer,
    )
    .await;
    assert!(
        matches!(result, Err(ObservedExtractionError::Limit(_))),
        "{result:?}"
    );
    assert_eq!(observer.0, 1024);
    assert!(kubo.received_requests().await.unwrap().is_empty());

    let mut observer = ByteObserver::default();
    let chunks = archive
        .chunks(3)
        .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk)))
        .collect::<Vec<_>>();
    let accepted = extract_zip_stream_observed_with_limits(
        &state,
        "",
        stream::iter(chunks),
        ZipExtractionLimits::default(),
        &mut observer,
    )
    .await
    .unwrap();
    assert_eq!(observer.0, 1024);
    assert!(accepted.entries.is_empty());
    assert!(accepted.failures.is_empty());
}

#[tokio::test]
async fn processing_deadline_cancels_stalled_stream_without_fake_idle_status() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let (state, kubo) = test_state().await;
    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_in_source = dropped.clone();
    let source = Box::pin(async_stream::stream! {
        let _guard = DropFlag(dropped_in_source);
        yield Ok::<_, io::Error>(Bytes::from_static(b"PK\x03\x04"));
        std::future::pending::<()>().await;
    });
    let limits = ZipExtractionLimits::default()
        .with_deadline(Duration::from_millis(30))
        .unwrap();
    let mut observer = ByteObserver::default();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        extract_zip_stream_observed_with_limits(&state, "", source, limits, &mut observer),
    )
    .await
    .expect("the configured deadline should complete independently of Kubo idle");
    let Err(ObservedExtractionError::Limit(error)) = result else {
        panic!("deadline must be a resource-limit error: {result:?}");
    };
    assert!(
        error
            .message()
            .unwrap_or_default()
            .contains("processing deadline")
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn active_slow_transfer_is_not_stalled_by_processing_defaults() {
    let (state, kubo) = test_state().await;
    let archive = deflated_directory_bomb();
    let source = Box::pin(async_stream::stream! {
        for chunk in archive.chunks(15) {
            // The streaming reader can finish after the local entry without
            // consuming the ZIP central directory; even those early chunks
            // must keep the no-deadline default alive for over 100 ms.
            tokio::time::sleep(Duration::from_millis(40)).await;
            yield Ok::<_, io::Error>(Bytes::copy_from_slice(chunk));
        }
    });
    let mut observer = ByteObserver::default();
    let start = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        extract_zip_stream_observed_with_limits(
            &state,
            "",
            source,
            ZipExtractionLimits::default(),
            &mut observer,
        ),
    )
    .await
    .expect("active archive must complete")
    .unwrap();
    assert!(start.elapsed() > Duration::from_millis(100));
    assert_eq!(observer.0, 1024);
    assert!(result.failures.is_empty());
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[test]
fn zip_resource_limits_are_publicly_configurable_without_changing_defaults() {
    let defaults: Config = toml::from_str("").unwrap();
    assert_eq!(
        defaults.decompress_zip.limits,
        ZipExtractionLimits::default()
    );
    assert_eq!(
        defaults.decompress_zip.limits.max_decompressed_bytes(),
        MAX_DECOMPRESSED_ARCHIVE_BYTES
    );
    assert_eq!(
        defaults.decompress_zip.limits.max_single_entry_bytes(),
        MAX_DECOMPRESSED_ARCHIVE_BYTES
    );
    assert_eq!(
        defaults.decompress_zip.limits.max_entries(),
        MAX_ARCHIVE_ENTRIES
    );
    assert_eq!(
        defaults.decompress_zip.limits.max_metadata_bytes(),
        MAX_ARCHIVE_METADATA_BYTES
    );
    assert_eq!(
        defaults.decompress_zip.limits.processing_deadline_secs(),
        None
    );

    let config: Config = toml::from_str(
        "[decompress_zip]\nmax_decompressed_bytes = 1099511627776\nmax_single_entry_bytes = 1099511627776\nmax_entries = 10000\nmax_metadata_bytes = 1073741824\nmax_staged_adds = 100000\nprocessing_deadline_secs = 604800",
    )
    .unwrap();
    assert_eq!(
        config.decompress_zip.limits.max_decompressed_bytes(),
        1 << 40
    );
    assert_eq!(
        config.decompress_zip.limits.max_entries(),
        MAX_ARCHIVE_ENTRIES
    );
    assert_eq!(
        config.decompress_zip.limits.processing_deadline_secs(),
        Some(604800)
    );
    let with_existing_setting: Config =
        toml::from_str("[decompress_zip]\nunixfs_directory_root = false\nmax_entries = 3").unwrap();
    assert!(!with_existing_setting.decompress_zip.unixfs_directory_root);
    assert_eq!(with_existing_setting.decompress_zip.limits.max_entries(), 3);

    assert!(
        toml::from_str::<Config>("[decompress_zip]\nmax_decompressed_bytes = 1099511627777")
            .is_err()
    );
    assert!(toml::from_str::<Config>("[decompress_zip]\nmax_entries = 10001").is_err());
    assert!(toml::from_str::<Config>("[decompress_zip]\nmax_entries = 100000").is_err());
    assert!(ZipExtractionLimits::default().with_entries(10_001).is_err());
    assert_eq!(
        ZipExtractionLimits::default()
            .with_entries(10_000)
            .unwrap()
            .max_entries(),
        MAX_ARCHIVE_ENTRIES
    );
}
