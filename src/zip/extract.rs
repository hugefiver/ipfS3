use std::convert::Infallible;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use s3s::{S3Error, S3Result};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::s3::ops::object::{StoredObject, add_plain_object_stream};
use crate::state::AppState;
use crate::zip::integrity::finish_entry;
use crate::zip::local_header::observe_local_headers;
use crate::zip::response::{ExtractFailure, ExtractedEntry};
use crate::zip::sanitize::{SanitizedEntry, sanitize_entry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractOutcome {
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}

#[derive(Debug, thiserror::Error)]
pub enum ObservedExtractionError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    #[error("ZIP archive extraction failed")]
    Archive(#[source] S3Error),
    #[error("ZIP archive exceeds the decompression limit")]
    Limit(#[source] S3Error),
    #[error("ZIP extraction observer failed")]
    Observer(#[source] E),
}

#[async_trait::async_trait]
pub trait ExtractionObserver: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error>;
    async fn entry_finished(&mut self, entry: &ExtractedEntry) -> Result<(), Self::Error>;
    async fn entry_failed(&mut self, key: &str, error: &ExtractFailure) -> Result<(), Self::Error>;
    async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error>;
}

struct NoopObserver;

#[async_trait::async_trait]
impl ExtractionObserver for NoopObserver {
    type Error = Infallible;

    async fn entry_started(&mut self, _key: &str) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn entry_finished(&mut self, _entry: &ExtractedEntry) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn entry_failed(
        &mut self,
        _key: &str,
        _error: &ExtractFailure,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn bytes_processed(&mut self, _bytes: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Total decompressed bytes a single `decompress-zip` archive may expand to.
///
/// Remote pin quotas bound only provider storage; this bounds the local Kubo
/// datastore against a compression bomb.
pub const MAX_DECOMPRESSED_ARCHIVE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Default limit for the actual source ZIP bytes, including ZIP framing.
/// Larger than the legacy 8 GiB decompression default so valid archives still fit.
pub const MAX_ARCHIVE_INPUT_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Includes directories, empty files and failed entries, not just successes.
pub const MAX_ARCHIVE_ENTRIES: u64 = 10_000;
/// Reservation units: 4096 per entry + 8 * (raw name + extra + prefix bytes).
/// Bounds retained results/observer keys as well as parser metadata. Not an RSS cap.
pub const MAX_ARCHIVE_METADATA_BYTES: u64 = 64 * 1024 * 1024;

const HARD_MAX_DECOMPRESSED_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
const HARD_MAX_ARCHIVE_INPUT_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const HARD_MAX_ENTRIES: u64 = 100_000;
const HARD_MAX_METADATA_BYTES: u64 = 1024 * 1024 * 1024;
const HARD_MAX_DEADLINE_SECS: u64 = 7 * 24 * 60 * 60;

/// Request-level ZIP budgets. Defaults preserve all legacy ZIP limits. The
/// optional absolute deadline is *not* an idle timeout: active transfers can
/// continue for hours when no deadline is configured. Kubo retains its own
/// independent idle/cancellation behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZipExtractionLimits {
    max_archive_bytes: u64,
    max_decompressed_bytes: u64,
    max_single_entry_bytes: u64,
    max_entries: u64,
    max_metadata_bytes: u64,
    max_staged_adds: u64,
    processing_deadline: Option<std::time::Duration>,
}

impl Default for ZipExtractionLimits {
    fn default() -> Self {
        Self {
            max_archive_bytes: MAX_ARCHIVE_INPUT_BYTES,
            max_decompressed_bytes: MAX_DECOMPRESSED_ARCHIVE_BYTES,
            max_single_entry_bytes: MAX_DECOMPRESSED_ARCHIVE_BYTES,
            max_entries: MAX_ARCHIVE_ENTRIES,
            max_metadata_bytes: MAX_ARCHIVE_METADATA_BYTES,
            max_staged_adds: MAX_ARCHIVE_ENTRIES,
            processing_deadline: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default)]
struct RawZipExtractionLimits {
    max_archive_bytes: u64,
    max_decompressed_bytes: u64,
    max_single_entry_bytes: u64,
    max_entries: u64,
    max_metadata_bytes: u64,
    max_staged_adds: u64,
    processing_deadline_secs: Option<u64>,
}

impl Default for RawZipExtractionLimits {
    fn default() -> Self {
        let limits = ZipExtractionLimits::default();
        Self {
            max_archive_bytes: limits.max_archive_bytes,
            max_decompressed_bytes: limits.max_decompressed_bytes,
            max_single_entry_bytes: limits.max_single_entry_bytes,
            max_entries: limits.max_entries,
            max_metadata_bytes: limits.max_metadata_bytes,
            max_staged_adds: limits.max_staged_adds,
            processing_deadline_secs: None,
        }
    }
}

impl<'de> Deserialize<'de> for ZipExtractionLimits {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = RawZipExtractionLimits::deserialize(deserializer)?;
        let limits = Self {
            max_archive_bytes: raw.max_archive_bytes,
            max_decompressed_bytes: raw.max_decompressed_bytes,
            max_single_entry_bytes: raw.max_single_entry_bytes,
            max_entries: raw.max_entries,
            max_metadata_bytes: raw.max_metadata_bytes,
            max_staged_adds: raw.max_staged_adds,
            processing_deadline: raw
                .processing_deadline_secs
                .map(std::time::Duration::from_secs),
        };
        limits.validate().map_err(D::Error::custom)?;
        Ok(limits)
    }
}

impl ZipExtractionLimits {
    fn validate(&self) -> Result<(), &'static str> {
        if !(1..=HARD_MAX_ARCHIVE_INPUT_BYTES).contains(&self.max_archive_bytes) {
            return Err("decompress_zip.max_archive_bytes is outside hard bounds");
        }
        if self.max_decompressed_bytes > HARD_MAX_DECOMPRESSED_BYTES {
            return Err("decompress_zip.max_decompressed_bytes exceeds hard bound");
        }
        if self.max_single_entry_bytes > HARD_MAX_DECOMPRESSED_BYTES {
            return Err("decompress_zip.max_single_entry_bytes exceeds hard bound");
        }
        // Publication and manifest replay support at most 10,000 records.
        // Do not admit an archive the final manifest cannot represent safely.
        if !(1..=MAX_ARCHIVE_ENTRIES).contains(&self.max_entries) {
            return Err("decompress_zip.max_entries is outside hard bounds");
        }
        if self.max_metadata_bytes > HARD_MAX_METADATA_BYTES {
            return Err("decompress_zip.max_metadata_bytes exceeds hard bound");
        }
        if !(1..=HARD_MAX_ENTRIES).contains(&self.max_staged_adds) {
            return Err("decompress_zip.max_staged_adds is outside hard bounds");
        }
        if self.processing_deadline.is_some_and(|deadline| {
            deadline.is_zero() || deadline > std::time::Duration::from_secs(HARD_MAX_DEADLINE_SECS)
        }) {
            return Err("decompress_zip.processing_deadline_secs is outside hard bounds");
        }
        Ok(())
    }

    pub fn max_archive_bytes(&self) -> u64 {
        self.max_archive_bytes
    }
    pub fn max_decompressed_bytes(&self) -> u64 {
        self.max_decompressed_bytes
    }
    pub fn max_single_entry_bytes(&self) -> u64 {
        self.max_single_entry_bytes
    }
    pub fn max_entries(&self) -> u64 {
        self.max_entries
    }
    pub fn max_metadata_bytes(&self) -> u64 {
        self.max_metadata_bytes
    }
    /// Bounds attempted per-entry Kubo adds; not Kubo disk usage or the number
    /// of retained CIDs. Failed/CRC-invalid adds can still leave Kubo data.
    pub fn max_staged_adds(&self) -> u64 {
        self.max_staged_adds
    }
    pub fn processing_deadline_secs(&self) -> Option<u64> {
        self.processing_deadline.map(|duration| duration.as_secs())
    }

    pub fn with_archive_bytes(mut self, bytes: u64) -> Result<Self, &'static str> {
        self.max_archive_bytes = bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_single_entry_bytes(mut self, bytes: u64) -> Result<Self, &'static str> {
        self.max_single_entry_bytes = bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_decompressed_bytes(mut self, bytes: u64) -> Result<Self, &'static str> {
        self.max_decompressed_bytes = bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_entries(mut self, entries: u64) -> Result<Self, &'static str> {
        self.max_entries = entries;
        self.validate()?;
        Ok(self)
    }

    pub fn with_metadata_bytes(mut self, bytes: u64) -> Result<Self, &'static str> {
        self.max_metadata_bytes = bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_staged_adds(mut self, adds: u64) -> Result<Self, &'static str> {
        self.max_staged_adds = adds;
        self.validate()?;
        Ok(self)
    }

    pub fn with_deadline(mut self, deadline: std::time::Duration) -> Result<Self, &'static str> {
        self.processing_deadline = Some(deadline);
        self.validate()?;
        Ok(self)
    }
}

#[derive(Clone, Copy)]
struct MetadataLimits {
    entries: u64,
    bytes: u64,
}

impl Default for MetadataLimits {
    fn default() -> Self {
        Self {
            entries: MAX_ARCHIVE_ENTRIES,
            bytes: MAX_ARCHIVE_METADATA_BYTES,
        }
    }
}

enum EntryTransferError<E> {
    Upload(S3Error),
    Read(io::Error),
    BudgetExceeded(BudgetKind),
    Observer(E),
}

#[derive(Clone, Copy)]
enum BudgetKind {
    Archive,
    Entry,
}

enum CopyFailure<E> {
    Io(io::Error),
    BudgetExceeded(BudgetKind),
    Observer(E),
}

/// Copy `reader` into `writer`, charging every byte against `remaining`.
///
/// Returns `CopyFailure::BudgetExceeded` as soon as the budget would go
/// negative, before those bytes reach `writer`. The caller is responsible for
/// shutting `writer` down afterwards; see `upload_entry_to_kubo`.
async fn copy_with_budget<R, W, O>(
    reader: &mut R,
    writer: &mut W,
    remaining: &mut u64,
    entry_remaining: &mut u64,
    observer: &mut O,
) -> Result<(), CopyFailure<O::Error>>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
    O: ExtractionObserver,
{
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await.map_err(CopyFailure::Io)?;
        if read == 0 {
            return Ok(());
        }
        let read =
            u64::try_from(read).map_err(|_| CopyFailure::BudgetExceeded(BudgetKind::Archive))?;
        observer
            .bytes_processed(read)
            .await
            .map_err(CopyFailure::Observer)?;
        if read > *remaining {
            return Err(CopyFailure::BudgetExceeded(BudgetKind::Archive));
        }
        if read > *entry_remaining {
            return Err(CopyFailure::BudgetExceeded(BudgetKind::Entry));
        }
        *remaining -= read;
        *entry_remaining -= read;
        let end = usize::try_from(read).expect("chunk length fits in usize");
        writer
            .write_all(&buffer[..end])
            .await
            .map_err(CopyFailure::Io)?;
        tokio::task::yield_now().await;
    }
}

async fn upload_entry_to_kubo<R, O>(
    state: &Arc<AppState>,
    reader: &mut R,
    remaining: &mut u64,
    entry_remaining: &mut u64,
    observer: &mut O,
) -> Result<StoredObject, EntryTransferError<O::Error>>
where
    R: futures_io::AsyncRead + Unpin + Send,
    O: ExtractionObserver,
{
    let (duplex_reader, mut duplex_writer) = tokio::io::duplex(64 * 1024);
    let mut upload = Box::pin(async {
        add_plain_object_stream(state, ReaderStream::new(duplex_reader))
            .await
            .map_err(S3Error::from)
    });
    let mut copy = Box::pin(async {
        let mut tokio_reader = reader.compat();
        copy_with_budget(
            &mut tokio_reader,
            &mut duplex_writer,
            remaining,
            entry_remaining,
            observer,
        )
        .await
    });

    tokio::select! {
        biased;
        upload_result = &mut upload => match upload_result {
            Err(upload) => {
                drop(copy);
                let _ = duplex_writer.shutdown().await;
                Err(EntryTransferError::Upload(upload))
            }
            Ok(stored) => {
                let copy_result = copy.await;
                let shutdown = duplex_writer.shutdown().await;
                match (copy_result, shutdown) {
                    (Ok(()), Ok(())) => Ok(stored),
                    (Ok(()), Err(error)) => Err(EntryTransferError::Read(error)),
                    (Err(CopyFailure::BudgetExceeded(kind)), _) => Err(EntryTransferError::BudgetExceeded(kind)),
                    (Err(CopyFailure::Io(error)), _) => Err(EntryTransferError::Read(error)),
                    (Err(CopyFailure::Observer(error)), _) => Err(EntryTransferError::Observer(error)),
                }
            }
        },
        copy_result = &mut copy => {
            drop(copy);
            let shutdown = duplex_writer.shutdown().await;
            match (copy_result, shutdown) {
                (Err(CopyFailure::BudgetExceeded(kind)), _) => Err(EntryTransferError::BudgetExceeded(kind)),
                (Err(CopyFailure::Observer(error)), _) => Err(EntryTransferError::Observer(error)),
                (Err(CopyFailure::Io(error)), _) => match upload.await {
                    Err(upload) if error.kind() == io::ErrorKind::BrokenPipe => {
                        Err(EntryTransferError::Upload(upload))
                    }
                    Err(_) | Ok(_) => Err(EntryTransferError::Read(error)),
                },
                (Ok(()), Err(error)) => Err(EntryTransferError::Read(error)),
                (Ok(()), Ok(())) => match upload.await {
                    Ok(stored) => Ok(stored),
                    Err(upload) => Err(EntryTransferError::Upload(upload)),
                },
            }
        }
    }
}

fn budget_rejection(reported_max: u64) -> S3Error {
    crate::error::AppError::ZipArchiveRejected(format!(
        "archive expands beyond the {reported_max} byte decompression limit"
    ))
    .into()
}

fn entry_budget_rejection() -> S3Error {
    crate::error::AppError::ZipArchiveRejected(
        "ZIP entry exceeds the single-entry decompression limit".to_owned(),
    )
    .into()
}

fn limit_error(kind: BudgetKind, reported_max: u64) -> S3Error {
    match kind {
        BudgetKind::Archive => budget_rejection(reported_max),
        BudgetKind::Entry => entry_budget_rejection(),
    }
}

fn resource_budget_rejection() -> S3Error {
    crate::error::AppError::ZipArchiveRejected("archive exceeds ZIP staged add budget".to_owned())
        .into()
}

fn backend_stream_error(error: &(dyn std::error::Error + 'static)) -> Option<S3Error> {
    crate::error::has_kubo_stream_provenance(error)
        .then(|| crate::error::AppError::kubo_rpc_detail("Kubo response stream failed").into())
}

fn failure(entry_name: &str, code: &str, message: impl std::fmt::Display) -> ExtractFailure {
    // Error text also lives in accumulated results and import observer records.
    // Format directly into a bounded buffer rather than allocate then truncate.
    struct Message(String);
    impl std::fmt::Write for Message {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            let mut end = value.len().min(512 - self.0.len());
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            self.0.push_str(&value[..end]);
            Ok(())
        }
    }
    let mut bounded = Message(String::with_capacity(512));
    let _ = std::fmt::write(&mut bounded, format_args!("{message}"));
    ExtractFailure {
        entry_name: entry_name.to_owned(),
        code: code.to_owned(),
        message: bounded.0,
    }
}

async fn record_failure<O: ExtractionObserver>(
    observer: &mut O,
    failures: &mut Vec<ExtractFailure>,
    key: &str,
    failure: ExtractFailure,
) -> Result<(), ObservedExtractionError<O::Error>> {
    observer
        .entry_failed(key, &failure)
        .await
        .map_err(ObservedExtractionError::Observer)?;
    failures.push(failure);
    Ok(())
}

pub async fn extract_zip_stream<S, E>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
) -> S3Result<ExtractOutcome>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    extract_zip_stream_with_limit(state, target_prefix, stream, MAX_DECOMPRESSED_ARCHIVE_BYTES)
        .await
}

pub async fn extract_zip_stream_with_limit<S, E>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    max_decompressed_bytes: u64,
) -> S3Result<ExtractOutcome>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut observer = NoopObserver;
    match extract_zip_stream_observed(
        state,
        target_prefix,
        stream,
        max_decompressed_bytes,
        &mut observer,
    )
    .await
    {
        Ok(outcome) => Ok(outcome),
        Err(ObservedExtractionError::Archive(error) | ObservedExtractionError::Limit(error)) => {
            Err(error)
        }
        Err(ObservedExtractionError::Observer(error)) => match error {},
    }
}

/// Explicit request-scoped ZIP budgets; the legacy wrappers keep their
/// pre-existing default behavior. Configured limits only apply when passed.
pub async fn extract_zip_stream_with_limits<S, E>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    limits: ZipExtractionLimits,
) -> S3Result<ExtractOutcome>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut observer = NoopObserver;
    match extract_zip_stream_observed_with_limits(
        state,
        target_prefix,
        stream,
        limits,
        &mut observer,
    )
    .await
    {
        Ok(outcome) => Ok(outcome),
        Err(ObservedExtractionError::Archive(error) | ObservedExtractionError::Limit(error)) => {
            Err(error)
        }
        Err(ObservedExtractionError::Observer(error)) => match error {},
    }
}

pub async fn extract_zip_stream_observed<S, E, O>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    max_decompressed_bytes: u64,
    observer: &mut O,
) -> Result<ExtractOutcome, ObservedExtractionError<O::Error>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
    O: ExtractionObserver,
{
    extract_observed_with_metadata_limits(
        state,
        target_prefix,
        stream,
        max_decompressed_bytes,
        observer,
        MetadataLimits::default(),
    )
    .await
}

pub async fn extract_zip_stream_observed_with_limits<S, E, O>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    limits: ZipExtractionLimits,
    observer: &mut O,
) -> Result<ExtractOutcome, ObservedExtractionError<O::Error>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
    O: ExtractionObserver,
{
    let extraction = extract_observed_with_limits(
        state,
        target_prefix,
        stream,
        observer,
        limits,
        limits.max_decompressed_bytes,
    );
    if let Some(deadline) = limits.processing_deadline {
        tokio::time::timeout(deadline, extraction)
            .await
            .map_err(|_| {
                ObservedExtractionError::Limit(
                    crate::error::AppError::ZipArchiveRejected(
                        "ZIP extraction processing deadline exceeded".to_owned(),
                    )
                    .into(),
                )
            })?
    } else {
        extraction.await
    }
}

async fn extract_observed_with_metadata_limits<S, E, O>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    max_decompressed_bytes: u64,
    observer: &mut O,
    limits: MetadataLimits,
) -> Result<ExtractOutcome, ObservedExtractionError<O::Error>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
    O: ExtractionObserver,
{
    let limits = ZipExtractionLimits {
        max_decompressed_bytes,
        // The legacy with_limit API accepted a caller-supplied archive budget
        // without a separate per-entry cap. Retain that exact behavior.
        max_single_entry_bytes: max_decompressed_bytes,
        max_entries: limits.entries,
        max_metadata_bytes: limits.bytes,
        ..ZipExtractionLimits::default()
    };
    extract_observed_with_limits(
        state,
        target_prefix,
        stream,
        observer,
        limits,
        MAX_DECOMPRESSED_ARCHIVE_BYTES,
    )
    .await
}

async fn extract_observed_with_limits<S, E, O>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    observer: &mut O,
    limits: ZipExtractionLimits,
    reported_max: u64,
) -> Result<ExtractOutcome, ObservedExtractionError<O::Error>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
    O: ExtractionObserver,
{
    let mut remaining = limits.max_decompressed_bytes;
    let mut staged_adds_left = limits.max_staged_adds;
    let mut source = StreamReader::new(stream.map_err(io::Error::other));
    // async_zip's forward-only reader insists on a local entry header and
    // rejects a valid zero-entry EOCD. Peek only the first four bytes; replay
    // them for every non-empty archive so its normal parser sees every byte.
    let mut signature = [0_u8; 4];
    let mut seen = 0;
    while seen < signature.len() {
        match source.read(&mut signature[seen..]).await {
            Ok(0) => break,
            Ok(n) => seen += n,
            Err(error) => {
                return Err(ObservedExtractionError::Archive(
                    backend_stream_error(&error).unwrap_or_else(|| {
                        crate::error::AppError::ZipArchiveRejected(format!(
                            "invalid zip archive: {error}"
                        ))
                        .into()
                    }),
                ));
            }
        }
    }
    if signature == *b"PK\x05\x06" {
        // A strict 22-byte EOCD with all counts, offsets, disk IDs and comment
        // length zero is the only empty archive accepted here. A trailer,
        // partial header, comment or ZIP64 structure must not forge a batch.
        let mut tail = [0_u8; 19];
        let mut tail_len = 0;
        while tail_len < tail.len() {
            match source.read(&mut tail[tail_len..]).await {
                Ok(0) => break,
                Ok(n) => tail_len += n,
                Err(error) => {
                    return Err(ObservedExtractionError::Archive(
                        backend_stream_error(&error).unwrap_or_else(|| {
                            crate::error::AppError::ZipArchiveRejected(format!(
                                "invalid zip archive: {error}"
                            ))
                            .into()
                        }),
                    ));
                }
            }
        }
        if tail_len == 18 && tail[..18].iter().all(|byte| *byte == 0) {
            return Ok(ExtractOutcome {
                entries: Vec::new(),
                failures: Vec::new(),
            });
        }
        return Err(ObservedExtractionError::Archive(
            crate::error::AppError::ZipArchiveRejected("invalid empty zip archive".into()).into(),
        ));
    }
    let source = std::io::Cursor::new(signature[..seen].to_vec()).chain(source);
    let (source, local_headers) = observe_local_headers(source);
    local_headers.set_budget(
        limits.max_entries,
        limits.max_metadata_bytes,
        target_prefix.len(),
    );
    let mut zip = async_zip::base::read::stream::ZipFileReader::with_tokio(source);
    let mut entries = Vec::new();
    let mut failures = Vec::new();

    loop {
        // Empty entries can complete without awaiting transport or writes.
        // Yield so an explicit processing deadline can interrupt CPU-heavy
        // archives and cancellation can promptly drop the extraction future.
        tokio::task::yield_now().await;
        local_headers.begin();
        let next = match zip.next_with_entry().await {
            Ok(next) => next,
            Err(error) => {
                if local_headers.limit_exceeded() {
                    return Err(ObservedExtractionError::Limit(
                        crate::error::AppError::ZipArchiveRejected(
                            "archive exceeds ZIP entry or metadata budget".to_owned(),
                        )
                        .into(),
                    ));
                }
                if let Some(error) = backend_stream_error(&error) {
                    return Err(ObservedExtractionError::Archive(error));
                }
                return Err(ObservedExtractionError::Archive(
                    crate::error::AppError::ZipArchiveRejected(format!(
                        "invalid zip archive: {error}"
                    ))
                    .into(),
                ));
            }
        };
        let Some(mut entry_reader) = next else {
            break;
        };
        let local = match local_headers.take() {
            Ok(local) => local,
            Err(error) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(ObservedExtractionError::Archive(error));
                }
                return Err(ObservedExtractionError::Archive(
                    crate::error::AppError::ZipArchiveRejected(format!(
                        "invalid zip local header: {error}"
                    ))
                    .into(),
                ));
            }
        };
        let compressed_start = local_headers.position();
        let remaining_before_entry = remaining;
        let mut entry_remaining = limits.max_single_entry_bytes;
        let entry = entry_reader.reader().entry();

        let supported = matches!(
            (entry.compression(), local.compression_method),
            (async_zip::Compression::Stored, 0) | (async_zip::Compression::Deflate, 8)
        );
        if !supported {
            return Err(ObservedExtractionError::Archive(
                crate::error::AppError::UnsupportedZipEntry(
                    "local-header compression method must match Stored(0) or Deflate(8)"
                        .to_string(),
                )
                .into(),
            ));
        }
        if local.compression_method == 0 && local.uses_descriptor() {
            return Err(ObservedExtractionError::Archive(
                crate::error::AppError::UnsupportedZipEntry(
                    "Stored entry uses general-purpose bit 3 (data descriptor)".to_string(),
                )
                .into(),
            ));
        }
        let name = entry
            .filename()
            .as_str()
            .map_err(|_| {
                ObservedExtractionError::Archive(
                    crate::error::AppError::InvalidZipEntry(
                        "entry name is not valid UTF-8".to_string(),
                    )
                    .into(),
                )
            })?
            .to_string();
        let sanitized = sanitize_entry(&name, target_prefix)
            .map_err(S3Error::from)
            .map_err(ObservedExtractionError::Archive)?;

        let key = match sanitized {
            SanitizedEntry::Directory => {
                // A `dir/`-named entry may still carry a payload; charge the
                // inflate work rather than skipping it uncounted.
                let directory_key = format!("{target_prefix}{}", name.trim_matches('/'));
                let drain_result = {
                    let mut reader = entry_reader.reader_mut().compat();
                    let mut sink = tokio::io::sink();
                    copy_with_budget(
                        &mut reader,
                        &mut sink,
                        &mut remaining,
                        &mut entry_remaining,
                        observer,
                    )
                    .await
                };
                match drain_result {
                    Ok(()) => {}
                    Err(CopyFailure::BudgetExceeded(kind)) => {
                        return Err(ObservedExtractionError::Limit(limit_error(
                            kind,
                            reported_max,
                        )));
                    }
                    Err(CopyFailure::Observer(error)) => {
                        return Err(ObservedExtractionError::Observer(error));
                    }
                    Err(CopyFailure::Io(error)) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(ObservedExtractionError::Archive(error));
                        }
                        record_failure(
                            observer,
                            &mut failures,
                            &directory_key,
                            failure(&name, "EntryReadFailed", error),
                        )
                        .await?;
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
                match finish_entry(
                    entry_reader,
                    &local_headers,
                    local.uses_descriptor(),
                    compressed_start,
                    remaining_before_entry - remaining,
                )
                .await
                {
                    Ok((ready, valid)) => {
                        if !valid {
                            record_failure(
                                observer,
                                &mut failures,
                                &directory_key,
                                failure(&name, "EntryReadFailed", "ZIP CRC or size mismatch"),
                            )
                            .await?;
                        }
                        zip = ready;
                        continue;
                    }
                    Err(error) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(ObservedExtractionError::Archive(error));
                        }
                        record_failure(
                            observer,
                            &mut failures,
                            &directory_key,
                            failure(&name, "EntryReadFailed", error),
                        )
                        .await?;
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
            }
            SanitizedEntry::File { key } => key,
        };

        if staged_adds_left == 0 {
            return Err(ObservedExtractionError::Limit(resource_budget_rejection()));
        }
        staged_adds_left -= 1;

        observer
            .entry_started(&key)
            .await
            .map_err(ObservedExtractionError::Observer)?;

        let stored = match upload_entry_to_kubo(
            state,
            entry_reader.reader_mut(),
            &mut remaining,
            &mut entry_remaining,
            observer,
        )
        .await
        {
            Ok(stored) => stored,
            Err(EntryTransferError::Upload(error)) => {
                record_failure(
                    observer,
                    &mut failures,
                    &key,
                    failure(&name, "EntryUploadFailed", error),
                )
                .await?;
                let drain_result = {
                    let mut reader = entry_reader.reader_mut().compat();
                    let mut sink = tokio::io::sink();
                    copy_with_budget(
                        &mut reader,
                        &mut sink,
                        &mut remaining,
                        &mut entry_remaining,
                        observer,
                    )
                    .await
                };
                match drain_result {
                    Ok(()) => {}
                    Err(CopyFailure::BudgetExceeded(kind)) => {
                        return Err(ObservedExtractionError::Limit(limit_error(
                            kind,
                            reported_max,
                        )));
                    }
                    Err(CopyFailure::Observer(error)) => {
                        return Err(ObservedExtractionError::Observer(error));
                    }
                    Err(CopyFailure::Io(error)) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(ObservedExtractionError::Archive(error));
                        }
                        record_failure(
                            observer,
                            &mut failures,
                            &key,
                            failure(&name, "EntryReadFailed", error),
                        )
                        .await?;
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
                match finish_entry(
                    entry_reader,
                    &local_headers,
                    local.uses_descriptor(),
                    compressed_start,
                    remaining_before_entry - remaining,
                )
                .await
                {
                    Ok((ready, valid)) => {
                        if !valid {
                            record_failure(
                                observer,
                                &mut failures,
                                &key,
                                failure(&name, "EntryReadFailed", "ZIP CRC or size mismatch"),
                            )
                            .await?;
                        }
                        zip = ready;
                        continue;
                    }
                    Err(error) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(ObservedExtractionError::Archive(error));
                        }
                        record_failure(
                            observer,
                            &mut failures,
                            &key,
                            failure(&name, "EntryReadFailed", error),
                        )
                        .await?;
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
            }
            Err(EntryTransferError::Read(error)) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(ObservedExtractionError::Archive(error));
                }
                record_failure(
                    observer,
                    &mut failures,
                    &key,
                    failure(&name, "EntryReadFailed", error),
                )
                .await?;
                return Ok(ExtractOutcome { entries, failures });
            }
            Err(EntryTransferError::BudgetExceeded(kind)) => {
                return Err(ObservedExtractionError::Limit(limit_error(
                    kind,
                    reported_max,
                )));
            }
            Err(EntryTransferError::Observer(error)) => {
                return Err(ObservedExtractionError::Observer(error));
            }
        };

        match finish_entry(
            entry_reader,
            &local_headers,
            local.uses_descriptor(),
            compressed_start,
            remaining_before_entry - remaining,
        )
        .await
        {
            Ok((ready, valid)) => {
                if !valid {
                    record_failure(
                        observer,
                        &mut failures,
                        &key,
                        failure(&name, "EntryReadFailed", "ZIP CRC or size mismatch"),
                    )
                    .await?;
                    zip = ready;
                    continue;
                }
                let extracted = ExtractedEntry {
                    key,
                    cid: stored.cid,
                    size: stored.size,
                };
                observer
                    .entry_finished(&extracted)
                    .await
                    .map_err(ObservedExtractionError::Observer)?;
                entries.push(extracted);
                zip = ready;
            }
            Err(error) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(ObservedExtractionError::Archive(error));
                }
                record_failure(
                    observer,
                    &mut failures,
                    &key,
                    failure(&name, "EntryReadFailed", error),
                )
                .await?;
                return Ok(ExtractOutcome { entries, failures });
            }
        }
    }

    Ok(ExtractOutcome { entries, failures })
}

#[cfg(test)]
mod tests {
    include!("hardening_tests.rs");
    use std::collections::HashMap;
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use bytes::Bytes;
    use futures_util::stream;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as AutoBuilder;
    use sea_orm::{Database, EntityTrait, PaginatorTrait};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        ExtractFailure, ExtractedEntry, ExtractionObserver, ObservedExtractionError,
        extract_zip_stream, extract_zip_stream_observed,
    };
    use crate::crypto::key::MasterKey;
    use crate::kubo::KuboClient;
    use crate::state::AppState;
    use crate::store::Store;

    const HELLO: &[u8] = b"hello";
    const HELLO_DEFLATED: &[u8] = &[0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
    const ZEROES: &[u8] = &[0; 1024];
    const ZEROES_DEFLATED: &[u8] = &[
        0x63, 0x60, 0x18, 0x05, 0xa3, 0x60, 0x14, 0x8c, 0x54, 0x00, 0x00,
    ];

    #[test]
    fn source_zip_limit_is_independent_of_decompression_limits() {
        let limits = super::ZipExtractionLimits::default()
            .with_archive_bytes(64 * 1024 * 1024 * 1024)
            .unwrap();
        assert_eq!(limits.max_archive_bytes(), 64 * 1024 * 1024 * 1024);
        assert_eq!(
            limits.max_decompressed_bytes(),
            super::MAX_DECOMPRESSED_ARCHIVE_BYTES
        );
        assert_eq!(limits.max_entries(), super::MAX_ARCHIVE_ENTRIES);
        assert_eq!(
            limits.max_metadata_bytes(),
            super::MAX_ARCHIVE_METADATA_BYTES
        );
    }

    #[tokio::test]
    async fn compressed_bomb_is_charged_on_inflate_not_compressed_length() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let archive = zip(&[ZipEntryFixture {
            name: b"bomb/",
            data: ZEROES,
            method: 8,
            descriptor: false,
        }]);
        let error = super::extract_zip_stream_with_limit(
            &state,
            "",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(archive))]),
            1023,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn default_deadline_does_not_expire_active_slow_source() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let archive = single_entry_zip(0, false);
        let source = Box::pin(async_stream::stream! {
            for chunk in archive.chunks(15) {
                tokio::time::sleep(Duration::from_millis(15)).await;
                yield Ok::<_, io::Error>(Bytes::copy_from_slice(chunk));
            }
        });
        let outcome = tokio::time::timeout(
            Duration::from_secs(3),
            super::extract_zip_stream_with_limits(
                &state,
                "p/",
                source,
                super::ZipExtractionLimits::default(),
            ),
        )
        .await
        .expect("slow but active input must finish")
        .unwrap();
        assert_eq!(outcome.entries.len(), 1);
        assert_kubo_call_counts(&kubo, 1, 1).await;
    }

    #[tokio::test]
    async fn configured_single_entry_limit_counts_real_streamed_bytes() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
            ],
            0,
        )
        .await;
        let mut observer = RecordingObserver::default();
        let limits = super::ZipExtractionLimits::default()
            .with_single_entry_bytes(2)
            .unwrap();
        let error = super::extract_zip_stream_observed_with_limits(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(single_entry_zip(
                8, true,
            )))]),
            limits,
            &mut observer,
        )
        .await;
        assert!(matches!(error, Err(ObservedExtractionError::Limit(_))));
        assert_eq!(observer.bytes, 5);
        assert!(requests_for(&kubo).await.iter().all(|request| {
            request.url.path() != "/api/v0/pin/add"
                && !request
                    .body
                    .windows(HELLO.len())
                    .any(|bytes| bytes == HELLO)
        }));
    }

    #[tokio::test]
    async fn configured_deadline_interrupts_stalled_source_without_relabeling_as_idle() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let limits = super::ZipExtractionLimits::default()
            .with_deadline(Duration::from_millis(25))
            .unwrap();
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
            yield Ok::<_, io::Error>(Bytes::from_static(b"PK\x03\x04"));
            std::future::pending::<()>().await;
        });
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            super::extract_zip_stream_with_limits(&state, "prefix/", source, limits),
        )
        .await
        .expect("processing deadline must interrupt stalled source")
        .unwrap_err();
        assert!(
            error
                .message()
                .unwrap_or_default()
                .contains("processing deadline")
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "deadline must drop the source"
        );
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn configured_staged_add_budget_skips_second_upload() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let entry = ZipEntryFixture {
            name: b"f",
            data: HELLO,
            method: 0,
            descriptor: false,
        };
        let limits = super::ZipExtractionLimits::default()
            .with_staged_adds(1)
            .unwrap();
        let error = super::extract_zip_stream_with_limits(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(zip(&[entry; 2])))]),
            limits,
        )
        .await;
        assert_eq!(error.unwrap_err().code().as_str(), "InvalidParameterValue");
        assert_kubo_call_counts(&kubo, 1, 1).await;
    }

    #[derive(Clone, Copy)]
    struct ZipEntryFixture<'a> {
        name: &'a [u8],
        data: &'a [u8],
        method: u16,
        descriptor: bool,
    }

    fn push_u16(out: &mut Vec<u8>, value: u16) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
            }
        }
        !crc
    }

    fn encoded_data(entry: ZipEntryFixture<'_>) -> &'_ [u8] {
        match (entry.method, entry.data) {
            (8, HELLO) => HELLO_DEFLATED,
            (8, ZEROES) => ZEROES_DEFLATED,
            (_, data) => data,
        }
    }

    fn zip(entries: &[ZipEntryFixture<'_>]) -> Vec<u8> {
        let mut output = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());

        for entry in entries {
            let compressed = encoded_data(*entry);
            let flags = u16::from(entry.descriptor) << 3;
            let crc = crc32(entry.data);
            offsets.push(output.len() as u32);

            push_u32(&mut output, 0x0403_4b50);
            push_u16(&mut output, 20);
            push_u16(&mut output, flags);
            push_u16(&mut output, entry.method);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, if entry.descriptor { 0 } else { crc });
            push_u32(
                &mut output,
                if entry.descriptor {
                    0
                } else {
                    compressed.len() as u32
                },
            );
            push_u32(
                &mut output,
                if entry.descriptor {
                    0
                } else {
                    entry.data.len() as u32
                },
            );
            push_u16(&mut output, entry.name.len() as u16);
            push_u16(&mut output, 0);
            output.extend_from_slice(entry.name);
            output.extend_from_slice(compressed);

            if entry.descriptor {
                push_u32(&mut output, 0x0807_4b50);
                push_u32(&mut output, crc);
                push_u32(&mut output, compressed.len() as u32);
                push_u32(&mut output, entry.data.len() as u32);
            }
        }

        let central_offset = output.len() as u32;
        for (entry, offset) in entries.iter().zip(offsets) {
            let compressed = encoded_data(*entry);
            let flags = u16::from(entry.descriptor) << 3;
            push_u32(&mut output, 0x0201_4b50);
            push_u16(&mut output, 20);
            push_u16(&mut output, 20);
            push_u16(&mut output, flags);
            push_u16(&mut output, entry.method);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, crc32(entry.data));
            push_u32(&mut output, compressed.len() as u32);
            push_u32(&mut output, entry.data.len() as u32);
            push_u16(&mut output, entry.name.len() as u16);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, 0);
            push_u32(&mut output, offset);
            output.extend_from_slice(entry.name);
        }

        let central_size = output.len() as u32 - central_offset;
        push_u32(&mut output, 0x0605_4b50);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, entries.len() as u16);
        push_u16(&mut output, entries.len() as u16);
        push_u32(&mut output, central_size);
        push_u32(&mut output, central_offset);
        push_u16(&mut output, 0);
        output
    }

    fn single_entry_zip(method: u16, descriptor: bool) -> Vec<u8> {
        zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
            method,
            descriptor,
        }])
    }

    fn single_entry_zip_named(name: &[u8]) -> Vec<u8> {
        zip(&[ZipEntryFixture {
            name,
            data: HELLO,
            method: 0,
            descriptor: false,
        }])
    }

    fn truncated_descriptor_zip() -> Vec<u8> {
        let entry = ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
            method: 8,
            descriptor: true,
        };
        let mut output = Vec::new();
        let compressed = encoded_data(entry);
        push_u32(&mut output, 0x0403_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 8);
        push_u16(&mut output, 8);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, 0);
        push_u32(&mut output, 0);
        push_u32(&mut output, 0);
        push_u16(&mut output, entry.name.len() as u16);
        push_u16(&mut output, 0);
        output.extend_from_slice(entry.name);
        output.extend_from_slice(compressed);
        push_u32(&mut output, 0x0807_4b50);
        push_u32(&mut output, crc32(entry.data));
        output
    }

    async fn test_state_with_kubo(kubo: KuboClient) -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        Arc::new(AppState {
            kubo,
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    async fn test_state(kubo_uri: String) -> Arc<AppState> {
        test_state_with_kubo(KuboClient::new(kubo_uri)).await
    }

    async fn extractor_state_with_add_responses(
        responses: Vec<ResponseTemplate>,
        expected_pins: usize,
    ) -> (Arc<AppState>, MockServer) {
        let kubo = MockServer::start().await;
        let response_index = Arc::new(AtomicUsize::new(0));
        let add_responses = Arc::new(responses);
        if !add_responses.is_empty() {
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with({
                    let response_index = response_index.clone();
                    let add_responses = add_responses.clone();
                    move |_: &wiremock::Request| {
                        let index = response_index.fetch_add(1, Ordering::SeqCst);
                        add_responses[index].clone()
                    }
                })
                .up_to_n_times(add_responses.len() as u64)
                .mount(&kubo)
                .await;
        }
        if expected_pins > 0 {
            Mock::given(method("POST"))
                .and(path("/api/v0/pin/add"))
                .respond_with(ResponseTemplate::new(200))
                .up_to_n_times(expected_pins as u64)
                .mount(&kubo)
                .await;
        }
        (test_state(kubo.uri()).await, kubo)
    }

    async fn extract_fixture(bytes: Vec<u8>) -> (crate::zip::extract::ExtractOutcome, MockServer) {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(bytes))]);
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            extract_zip_stream(&state, "prefix/", stream),
        )
        .await
        .expect("extractor must not hang")
        .unwrap();
        (outcome, kubo)
    }

    async fn requests_for(kubo: &MockServer) -> Vec<wiremock::Request> {
        kubo.received_requests().await.unwrap()
    }

    async fn assert_kubo_call_counts(kubo: &MockServer, add: usize, pin: usize) {
        let requests = requests_for(kubo).await;
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/add")
                .count(),
            add
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/pin/add")
                .count(),
            pin
        );
    }

    #[tokio::test]
    async fn stored_without_descriptor_is_accepted() {
        let (outcome, kubo) = extract_fixture(single_entry_zip(0, false)).await;
        assert_eq!(outcome.entries.len(), 1, "{outcome:?}");
        assert_eq!(outcome.entries[0].key, "prefix/file.txt");
        assert!(outcome.failures.is_empty());
        assert_kubo_call_counts(&kubo, 1, 1).await;
    }

    #[tokio::test]
    async fn deflate_with_descriptor_is_accepted() {
        let (outcome, kubo) = extract_fixture(single_entry_zip(8, true)).await;
        assert_eq!(outcome.entries.len(), 1, "{outcome:?}");
        assert_eq!(outcome.entries[0].size, 5);
        assert!(outcome.failures.is_empty());
        assert_kubo_call_counts(&kubo, 1, 1).await;
    }

    #[tokio::test]
    async fn stored_with_descriptor_is_rejected_before_entry_upload() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(single_entry_zip(
            0, true,
        )))]);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            extract_zip_stream(&state, "prefix/", stream),
        )
        .await
        .expect("Stored+descriptor must reject without reading an unbounded entry")
        .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert!(
            requests_for(&kubo)
                .await
                .iter()
                .all(|request| request.url.path() != "/api/v0/add")
        );
    }

    #[tokio::test]
    async fn unsupported_compression_method_is_rejected_before_entry_upload() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(single_entry_zip(
            12, false,
        )))]);

        let error = extract_zip_stream(&state, "prefix/", stream)
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert!(
            error
                .message()
                .is_some_and(|message| message.contains("compression not supported: 12"))
        );
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn invalid_utf8_filename_is_rejected_before_entry_upload() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let stream = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(
            single_entry_zip_named(b"\xff.txt"),
        ))]);

        let error = extract_zip_stream(&state, "prefix/", stream)
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert!(error.to_string().contains("invalid zip entry"));
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn entry_upload_failure_drains_entry_and_continues() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(500).set_body_string("add failed"),
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmSecond\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let archive = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
            ZipEntryFixture {
                name: b"second.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ]);
        let outcome = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(archive))]),
        )
        .await
        .unwrap();

        assert_eq!(outcome.entries.len(), 1, "{outcome:?}");
        assert_eq!(outcome.entries[0].cid, "QmSecond");
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.failures[0].entry_name, "first.txt");
        assert_eq!(outcome.failures[0].code, "EntryUploadFailed");
        assert_kubo_call_counts(&kubo, 2, 1).await;
    }

    #[tokio::test]
    async fn global_reject_after_one_entry_keeps_the_entry_pin() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmSharedEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let archive = zip(&[
            ZipEntryFixture {
                name: b"safe.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
            ZipEntryFixture {
                name: b"../escape.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ]);
        let error = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(archive))]),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_kubo_call_counts(&kubo, 1, 1).await;
        assert!(!requests_for(&kubo).await.iter().any(|request| {
            request.url.path() == "/api/v0/pin/rm"
                && request.url.query() == Some("arg=QmSharedEntry")
        }));
    }

    #[tokio::test]
    async fn entry_read_failure_after_pin_keeps_the_entry_pin() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmSharedEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let outcome = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from(
                truncated_descriptor_zip(),
            ))]),
        )
        .await
        .unwrap();
        assert_eq!(outcome.entries.len(), 0);
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.failures[0].code, "EntryReadFailed");
        assert_kubo_call_counts(&kubo, 1, 1).await;
        assert!(!requests_for(&kubo).await.iter().any(|request| {
            request.url.path() == "/api/v0/pin/rm"
                && request.url.query() == Some("arg=QmSharedEntry")
        }));
    }

    #[tokio::test]
    async fn directories_are_skipped_and_files_are_staged() {
        let (outcome, kubo) = extract_fixture(zip(&[
            ZipEntryFixture {
                name: b"dir/",
                data: b"",
                method: 0,
                descriptor: false,
            },
            ZipEntryFixture {
                name: b"dir/file.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ]))
        .await;
        assert_eq!(outcome.entries.len(), 1, "{outcome:?}");
        assert_eq!(outcome.entries[0].key, "prefix/dir/file.txt");
        assert_kubo_call_counts(&kubo, 1, 1).await;
    }

    #[tokio::test]
    async fn a_directory_entry_payload_is_charged_against_the_budget() {
        let (state, server) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let archive = zip(&[ZipEntryFixture {
            name: b"dir/",
            data: HELLO,
            method: 8,
            descriptor: false,
        }]);
        let stream = stream::iter(vec![Ok::<_, io::Error>(Bytes::from(archive))]);

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            super::extract_zip_stream_with_limit(&state, "prefix/", stream, 2),
        )
        .await
        .expect("extraction must not hang")
        .expect_err("a directory entry payload over the budget must be rejected");

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_kubo_call_counts(&server, 0, 0).await;
    }

    #[tokio::test]
    async fn an_empty_directory_entry_stays_within_a_tight_budget() {
        let (state, server) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let archive = zip(&[ZipEntryFixture {
            name: b"dir/",
            data: b"",
            method: 0,
            descriptor: false,
        }]);
        let stream = stream::iter(vec![Ok::<_, io::Error>(Bytes::from(archive))]);

        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            super::extract_zip_stream_with_limit(&state, "prefix/", stream, 0),
        )
        .await
        .expect("extraction must not hang")
        .expect("an empty directory entry must not consume the budget");

        assert!(outcome.entries.is_empty(), "{outcome:?}");
        assert!(outcome.failures.is_empty(), "{outcome:?}");
        assert_kubo_call_counts(&server, 0, 0).await;
    }

    #[tokio::test]
    async fn corrupt_archive_is_a_global_reject() {
        let (state, _) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let error = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from_static(
                b"not a zip",
            ))]),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
    }

    #[tokio::test]
    async fn strict_empty_eocd_requires_clean_stream_eof_even_across_chunks() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let mut eocd = [0_u8; 22];
        eocd[..4].copy_from_slice(b"PK\x05\x06");
        let chunks = vec![
            Ok::<_, io::Error>(Bytes::copy_from_slice(&eocd[..1])),
            Ok(Bytes::copy_from_slice(&eocd[1..5])),
            Ok(Bytes::copy_from_slice(&eocd[5..18])),
            Ok(Bytes::copy_from_slice(&eocd[18..])),
        ];
        let outcome = extract_zip_stream(&state, "out/", stream::iter(chunks))
            .await
            .unwrap();
        assert!(outcome.entries.is_empty() && outcome.failures.is_empty());
        assert_kubo_call_counts(&kubo, 0, 0).await;

        let truncated = stream::iter(vec![
            Ok(Bytes::copy_from_slice(&eocd)),
            Err(io::Error::other("late stream failure")),
        ]);
        assert!(extract_zip_stream(&state, "out/", truncated).await.is_err());
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }

    #[tokio::test]
    async fn stalled_kubo_archive_body_is_a_fixed_internal_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (first_chunk_sent, first_chunk_observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nPK\x03\x04\r\n",
                )
                .await
                .unwrap();
            first_chunk_sent.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let state = test_state_with_kubo(KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(300),
            Duration::from_millis(50),
        ))
        .await;
        let archive = crate::kubo::cat::stream_cat(&state.kubo, "QmStalled", None)
            .await
            .unwrap();
        first_chunk_observed.await.unwrap();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            extract_zip_stream(&state, "prefix/", Box::pin(archive)),
        )
        .await
        .expect("a stalled Kubo archive body must not hang extraction")
        .expect_err("a Kubo body failure must abort extraction");

        assert_eq!(error.code().as_str(), "InternalError");
        assert_eq!(
            error.message(),
            Some(crate::error::INTERNAL_STORAGE_BACKEND_ERROR)
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn extraction_stops_when_the_archive_exceeds_the_decompressed_budget() {
        let (state, server) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmFirst\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let archive = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
            ZipEntryFixture {
                name: b"second.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ]);
        let stream = stream::iter(vec![Ok::<_, io::Error>(Bytes::from(archive))]);

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            super::extract_zip_stream_with_limit(&state, "prefix/", stream, 7),
        )
        .await
        .expect("extraction must not hang")
        .expect_err("an archive over the budget must be rejected");

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        let message = error
            .message()
            .expect("budget rejection must have a message");
        assert!(
            message.contains(&super::MAX_DECOMPRESSED_ARCHIVE_BYTES.to_string()),
            "wrapper-visible message must retain the global limit: {message}"
        );
        assert!(
            !message.contains("the 7 byte decompression limit"),
            "custom limits must not change the compatibility message: {message}"
        );
        assert_kubo_call_counts(&server, 1, 1).await;
        assert!(
            requests_for(&server)
                .await
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm"),
            "a global budget reject must not remove a staged local pin"
        );
        assert_eq!(
            crate::store::entities::object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0,
            "the extractor is the pre-publication boundary"
        );
    }

    #[tokio::test]
    async fn early_entry_upload_failure_drains_into_the_global_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (release_source, release_stream) = oneshot::channel();
        let release_source = Arc::new(Mutex::new(Some(release_source)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = requests.clone();
        let server_release = release_source.clone();
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let requests = server_requests.clone();
                let release_source = server_release.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<hyper::body::Incoming>| {
                        let requests = requests.clone();
                        let release_source = release_source.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let call = {
                                let mut requests = requests.lock().unwrap();
                                requests.push(path.clone());
                                requests.len()
                            };
                            let response = match (call, path.as_str()) {
                                (1, "/api/v0/add") => {
                                    request.into_body().collect().await.unwrap();
                                    Response::new(Full::new(Bytes::from_static(
                                        b"{\"Hash\":\"QmFirst\",\"Size\":\"5\"}\n",
                                    )))
                                }
                                (2, "/api/v0/pin/add") => {
                                    request.into_body().collect().await.unwrap();
                                    Response::new(Full::new(Bytes::new()))
                                }
                                (3, "/api/v0/add") => {
                                    release_source
                                        .lock()
                                        .unwrap()
                                        .take()
                                        .unwrap()
                                        .send(())
                                        .unwrap();
                                    Response::builder()
                                        .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                                        .body(Full::new(Bytes::from_static(b"ignored Kubo body")))
                                        .unwrap()
                                }
                                _ => panic!("unexpected Kubo request #{call}: {path}"),
                            };
                            Ok::<_, std::convert::Infallible>(response)
                        }
                    });
                    AutoBuilder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(socket), service)
                        .await
                        .unwrap();
                });
            }
        });

        let state = test_state(endpoint).await;
        let archive = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
            ZipEntryFixture {
                name: b"failed.txt",
                data: HELLO,
                method: 0,
                descriptor: false,
            },
        ]);
        let second_entry_data_offset =
            30 + b"first.txt".len() + HELLO.len() + 30 + b"failed.txt".len();
        let (prefix, suffix) = archive.split_at(second_entry_data_offset + 1);
        let prefix = Bytes::copy_from_slice(prefix);
        let suffix = Bytes::copy_from_slice(suffix);
        let stream = Box::pin(async_stream::stream! {
            yield Ok::<_, io::Error>(prefix);
            release_stream.await.expect("Kubo must reject the second add before its body is consumed");
            tokio::task::yield_now().await;
            yield Ok::<_, io::Error>(suffix);
        });

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            super::extract_zip_stream_with_limit(&state, "prefix/", stream, 7),
        )
        .await
        .expect("early Kubo rejection must not hang extraction")
        .expect_err("draining the failed second entry must exhaust the archive budget");

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            ["/api/v0/add", "/api/v0/pin/add", "/api/v0/add"],
            "the second add must fail before its remaining entry bytes are drained"
        );
        assert_eq!(
            crate::store::entities::object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0,
            "the extractor must not publish object state"
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn extraction_accepts_an_archive_exactly_at_the_decompressed_budget() {
        let (state, server) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"),
            ],
            1,
        )
        .await;
        let archive = zip(&[ZipEntryFixture {
            name: b"first.txt",
            data: HELLO,
            method: 0,
            descriptor: false,
        }]);
        let stream = stream::iter(vec![Ok::<_, io::Error>(Bytes::from(archive))]);

        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            super::extract_zip_stream_with_limit(&state, "prefix/", stream, HELLO.len() as u64),
        )
        .await
        .expect("extraction must not hang")
        .expect("an archive exactly at the budget must be accepted");

        assert_eq!(outcome.entries.len(), 1);
        assert!(outcome.failures.is_empty());
        drop(server);
    }

    #[derive(Default)]
    struct RecordingObserver {
        events: Arc<Mutex<Vec<String>>>,
        bytes: u64,
    }

    #[async_trait::async_trait]
    impl ExtractionObserver for RecordingObserver {
        type Error = io::Error;

        async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error> {
            self.events.lock().unwrap().push(format!("start:{key}"));
            Ok(())
        }

        async fn entry_finished(&mut self, entry: &ExtractedEntry) -> Result<(), Self::Error> {
            self.events
                .lock()
                .unwrap()
                .push(format!("finish:{}", entry.key));
            Ok(())
        }

        async fn entry_failed(
            &mut self,
            key: &str,
            error: &ExtractFailure,
        ) -> Result<(), Self::Error> {
            self.events
                .lock()
                .unwrap()
                .push(format!("failed:{key}:{}", error.code));
            Ok(())
        }

        async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error> {
            self.bytes = self
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| io::Error::other("byte counter overflow"))?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn observer_sees_sanitized_key_before_kubo_add_and_truthful_bytes() {
        let kubo = MockServer::start().await;
        let events = Arc::new(Mutex::new(Vec::new()));
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with({
                let events = events.clone();
                move |_: &wiremock::Request| {
                    events.lock().unwrap().push("kubo-add".to_owned());
                    ResponseTemplate::new(200)
                        .set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n")
                }
            })
            .expect(1)
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&kubo)
            .await;
        let state = test_state(kubo.uri()).await;
        let mut observer = RecordingObserver {
            events: events.clone(),
            bytes: 0,
        };

        let outcome = extract_zip_stream_observed(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(single_entry_zip(
                0, false,
            )))]),
            HELLO.len() as u64,
            &mut observer,
        )
        .await
        .unwrap();

        assert_eq!(outcome.entries[0].key, "prefix/file.txt");
        assert_eq!(observer.bytes, HELLO.len() as u64);
        let events = events.lock().unwrap();
        let started = events
            .iter()
            .position(|event| event == "start:prefix/file.txt")
            .unwrap();
        let added = events.iter().position(|event| event == "kubo-add").unwrap();
        let finished = events
            .iter()
            .position(|event| event == "finish:prefix/file.txt")
            .unwrap();
        assert!(started < added, "observer events: {events:?}");
        assert!(added < finished, "observer events: {events:?}");
    }

    #[tokio::test]
    async fn observer_reports_the_full_overflowing_read_before_budget_rejection() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmOverflow\",\"Size\":\"0\"}\n"),
            ],
            0,
        )
        .await;
        let mut observer = RecordingObserver::default();

        let error = extract_zip_stream_observed(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(single_entry_zip(
                0, false,
            )))]),
            2,
            &mut observer,
        )
        .await
        .expect_err("a five-byte read must exceed a two-byte budget");

        let archive_error = match error {
            ObservedExtractionError::Limit(error) => error,
            ObservedExtractionError::Archive(error) => {
                panic!("budget rejection lost its typed limit classification: {error}")
            }
            ObservedExtractionError::Observer(error) => {
                panic!("observer error was reclassified: {error}")
            }
        };
        assert_eq!(observer.bytes, HELLO.len() as u64);
        let message = archive_error
            .message()
            .expect("budget rejection must have a message");
        assert!(message.contains(&super::MAX_DECOMPRESSED_ARCHIVE_BYTES.to_string()));
        assert!(!message.contains("the 2 byte decompression limit"));
        let requests = requests_for(&kubo).await;
        assert!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/add")
                .all(|request| !request
                    .body
                    .windows(HELLO.len())
                    .any(|bytes| bytes == HELLO)),
            "overflow bytes must never reach Kubo"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/add")
        );
        assert_eq!(
            crate::store::entities::object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0,
            "budget rejection must remain pre-publication"
        );
    }

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    #[error("overflow observer sentinel")]
    struct OverflowObserverSentinel;

    #[derive(Default)]
    struct RejectingOverflowObserver {
        started: Vec<String>,
        reads: Vec<u64>,
    }

    #[async_trait::async_trait]
    impl ExtractionObserver for RejectingOverflowObserver {
        type Error = OverflowObserverSentinel;

        async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error> {
            self.started.push(key.to_owned());
            Ok(())
        }

        async fn entry_finished(&mut self, _entry: &ExtractedEntry) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn entry_failed(
            &mut self,
            _key: &str,
            _error: &ExtractFailure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error> {
            self.reads.push(bytes);
            Err(OverflowObserverSentinel)
        }
    }

    #[tokio::test]
    async fn overflowing_read_preserves_observer_error_without_writing_overflow_bytes() {
        let (state, kubo) = extractor_state_with_add_responses(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmOverflow\",\"Size\":\"0\"}\n"),
            ],
            0,
        )
        .await;
        let mut observer = RejectingOverflowObserver::default();

        let error = extract_zip_stream_observed(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(single_entry_zip(
                0, false,
            )))]),
            2,
            &mut observer,
        )
        .await
        .expect_err("the sentinel observer must abort the overflowing read");

        match error {
            ObservedExtractionError::Observer(error) => {
                assert_eq!(error, OverflowObserverSentinel)
            }
            ObservedExtractionError::Archive(error) => {
                panic!("budget rejection incorrectly outranked the observer sentinel: {error}")
            }
            ObservedExtractionError::Limit(error) => {
                panic!("budget rejection incorrectly outranked the observer sentinel: {error}")
            }
        }
        assert_eq!(observer.started, ["prefix/file.txt"]);
        assert_eq!(observer.reads, [HELLO.len() as u64]);
        let requests = requests_for(&kubo).await;
        let add_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/add")
            .collect::<Vec<_>>();
        assert!(
            add_requests.iter().all(|request| !request
                .body
                .windows(HELLO.len())
                .any(|bytes| bytes == HELLO)),
            "observer-rejected overflow bytes must never reach Kubo"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/add")
        );
        assert_eq!(
            crate::store::entities::object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0,
            "observer rejection must remain pre-publication"
        );
    }

    struct RejectingObserver;

    #[async_trait::async_trait]
    impl ExtractionObserver for RejectingObserver {
        type Error = io::Error;

        async fn entry_started(&mut self, _key: &str) -> Result<(), Self::Error> {
            Err(io::Error::other("superseded"))
        }

        async fn entry_finished(&mut self, _entry: &ExtractedEntry) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn entry_failed(
            &mut self,
            _key: &str,
            _error: &ExtractFailure,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn bytes_processed(&mut self, _bytes: u64) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn observer_error_aborts_before_kubo_add_without_reclassification() {
        let (state, kubo) = extractor_state_with_add_responses(Vec::new(), 0).await;
        let mut observer = RejectingObserver;

        let error = extract_zip_stream_observed(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<_, io::Error>(Bytes::from(single_entry_zip(
                0, false,
            )))]),
            HELLO.len() as u64,
            &mut observer,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, ObservedExtractionError::Observer(_)));
        assert_kubo_call_counts(&kubo, 0, 0).await;
    }
}
