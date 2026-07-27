use std::io;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use s3s::{S3Error, S3Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::s3::ops::object::{StoredObject, add_plain_object_stream};
use crate::state::AppState;
use crate::zip::local_header::observe_local_headers;
use crate::zip::response::{ExtractFailure, ExtractedEntry};
use crate::zip::sanitize::{SanitizedEntry, sanitize_entry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractOutcome {
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}

/// Total decompressed bytes a single `decompress-zip` archive may expand to.
///
/// Remote pin quotas bound only provider storage; this bounds the local Kubo
/// datastore against a compression bomb.
pub const MAX_DECOMPRESSED_ARCHIVE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

enum EntryTransferError {
    Upload(S3Error),
    Read(io::Error),
    BudgetExceeded,
}

enum CopyFailure {
    Io(io::Error),
    BudgetExceeded,
}

/// Copy `reader` into `writer`, charging every byte against `remaining`.
///
/// Returns `CopyFailure::BudgetExceeded` as soon as the budget would go
/// negative, before those bytes reach `writer`. The caller is responsible for
/// shutting `writer` down afterwards; see `upload_entry_to_kubo`.
async fn copy_with_budget<R, W>(
    reader: &mut R,
    writer: &mut W,
    remaining: &mut u64,
) -> Result<(), CopyFailure>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await.map_err(CopyFailure::Io)?;
        if read == 0 {
            return Ok(());
        }
        let read = u64::try_from(read).map_err(|_| CopyFailure::BudgetExceeded)?;
        if read > *remaining {
            return Err(CopyFailure::BudgetExceeded);
        }
        *remaining -= read;
        let end = usize::try_from(read).expect("chunk length fits in usize");
        writer
            .write_all(&buffer[..end])
            .await
            .map_err(CopyFailure::Io)?;
    }
}

async fn upload_entry_to_kubo<R>(
    state: &Arc<AppState>,
    reader: &mut R,
    remaining: &mut u64,
) -> Result<StoredObject, EntryTransferError>
where
    R: futures_io::AsyncRead + Unpin + Send,
{
    let (duplex_reader, mut duplex_writer) = tokio::io::duplex(64 * 1024);
    let mut upload = Box::pin(async {
        add_plain_object_stream(state, ReaderStream::new(duplex_reader))
            .await
            .map_err(S3Error::from)
    });
    let mut copy = Box::pin(async {
        let mut tokio_reader = reader.compat();
        copy_with_budget(&mut tokio_reader, &mut duplex_writer, remaining).await
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
                    (Err(CopyFailure::BudgetExceeded), _) => Err(EntryTransferError::BudgetExceeded),
                    (Err(CopyFailure::Io(error)), _) => Err(EntryTransferError::Read(error)),
                }
            }
        },
        copy_result = &mut copy => {
            drop(copy);
            let shutdown = duplex_writer.shutdown().await;
            match (copy_result, shutdown) {
                (Err(CopyFailure::BudgetExceeded), _) => Err(EntryTransferError::BudgetExceeded),
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

fn budget_rejection() -> S3Error {
    crate::error::AppError::ZipArchiveRejected(format!(
        "archive expands beyond the {MAX_DECOMPRESSED_ARCHIVE_BYTES} byte decompression limit"
    ))
    .into()
}

fn backend_stream_error(error: &(dyn std::error::Error + 'static)) -> Option<S3Error> {
    crate::error::has_kubo_stream_provenance(error)
        .then(|| crate::error::AppError::kubo_rpc_detail("Kubo response stream failed").into())
}

fn failure(entry_name: &str, code: &str, message: impl ToString) -> ExtractFailure {
    ExtractFailure {
        entry_name: entry_name.to_owned(),
        code: code.to_owned(),
        message: message.to_string(),
    }
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
    let mut remaining = max_decompressed_bytes;
    let source = StreamReader::new(stream.map_err(io::Error::other));
    let (source, local_headers) = observe_local_headers(source);
    let mut zip = async_zip::base::read::stream::ZipFileReader::with_tokio(source);
    let mut entries = Vec::new();
    let mut failures = Vec::new();

    loop {
        local_headers.begin();
        let next = match zip.next_with_entry().await {
            Ok(next) => next,
            Err(error) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(error);
                }
                return Err(crate::error::AppError::ZipArchiveRejected(format!(
                    "invalid zip archive: {error}"
                ))
                .into());
            }
        };
        let Some(mut entry_reader) = next else {
            break;
        };
        let local = match local_headers.take() {
            Ok(local) => local,
            Err(error) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(error);
                }
                return Err(crate::error::AppError::ZipArchiveRejected(format!(
                    "invalid zip local header: {error}"
                ))
                .into());
            }
        };
        let entry = entry_reader.reader().entry().clone();

        let supported = matches!(
            (entry.compression(), local.compression_method),
            (async_zip::Compression::Stored, 0) | (async_zip::Compression::Deflate, 8)
        );
        if !supported {
            return Err(crate::error::AppError::UnsupportedZipEntry(
                "local-header compression method must match Stored(0) or Deflate(8)".to_string(),
            )
            .into());
        }
        if local.compression_method == 0 && local.uses_descriptor() {
            return Err(crate::error::AppError::UnsupportedZipEntry(
                "Stored entry uses general-purpose bit 3 (data descriptor)".to_string(),
            )
            .into());
        }
        let name = entry
            .filename()
            .as_str()
            .map_err(|_| {
                crate::error::AppError::InvalidZipEntry("entry name is not valid UTF-8".to_string())
            })?
            .to_string();
        let sanitized = sanitize_entry(&name, target_prefix).map_err(S3Error::from)?;

        let key = match sanitized {
            SanitizedEntry::Directory => {
                // A `dir/`-named entry may still carry a payload; charge the
                // inflate work rather than skipping it uncounted.
                let drain_result = {
                    let mut reader = entry_reader.reader_mut().compat();
                    let mut sink = tokio::io::sink();
                    copy_with_budget(&mut reader, &mut sink, &mut remaining).await
                };
                match drain_result {
                    Ok(()) => {}
                    Err(CopyFailure::BudgetExceeded) => return Err(budget_rejection()),
                    Err(CopyFailure::Io(error)) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(error);
                        }
                        failures.push(failure(&name, "EntryReadFailed", error));
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
                match entry_reader.done().await {
                    Ok(ready) => {
                        zip = ready;
                        continue;
                    }
                    Err(error) => {
                        if let Some(error) = backend_stream_error(&error) {
                            return Err(error);
                        }
                        failures.push(failure(&name, "EntryReadFailed", error));
                        return Ok(ExtractOutcome { entries, failures });
                    }
                }
            }
            SanitizedEntry::File { key } => key,
        };

        let stored =
            match upload_entry_to_kubo(state, entry_reader.reader_mut(), &mut remaining).await {
                Ok(stored) => stored,
                Err(EntryTransferError::Upload(error)) => {
                    failures.push(failure(&name, "EntryUploadFailed", error));
                    let drain_result = {
                        let mut reader = entry_reader.reader_mut().compat();
                        let mut sink = tokio::io::sink();
                        copy_with_budget(&mut reader, &mut sink, &mut remaining).await
                    };
                    match drain_result {
                        Ok(()) => {}
                        Err(CopyFailure::BudgetExceeded) => return Err(budget_rejection()),
                        Err(CopyFailure::Io(error)) => {
                            if let Some(error) = backend_stream_error(&error) {
                                return Err(error);
                            }
                            failures.push(failure(&name, "EntryReadFailed", error));
                            return Ok(ExtractOutcome { entries, failures });
                        }
                    }
                    match entry_reader.done().await {
                        Ok(ready) => {
                            zip = ready;
                            continue;
                        }
                        Err(error) => {
                            if let Some(error) = backend_stream_error(&error) {
                                return Err(error);
                            }
                            failures.push(failure(&name, "EntryReadFailed", error));
                            return Ok(ExtractOutcome { entries, failures });
                        }
                    }
                }
                Err(EntryTransferError::Read(error)) => {
                    if let Some(error) = backend_stream_error(&error) {
                        return Err(error);
                    }
                    failures.push(failure(&name, "EntryReadFailed", error));
                    return Ok(ExtractOutcome { entries, failures });
                }
                Err(EntryTransferError::BudgetExceeded) => return Err(budget_rejection()),
            };

        match entry_reader.done().await {
            Ok(ready) => {
                entries.push(ExtractedEntry {
                    key,
                    cid: stored.cid,
                    size: stored.size,
                });
                zip = ready;
            }
            Err(error) => {
                if let Some(error) = backend_stream_error(&error) {
                    return Err(error);
                }
                failures.push(failure(&name, "EntryReadFailed", error));
                return Ok(ExtractOutcome { entries, failures });
            }
        }
    }

    Ok(ExtractOutcome { entries, failures })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    use super::extract_zip_stream;
    use crate::crypto::key::MasterKey;
    use crate::kubo::KuboClient;
    use crate::state::AppState;
    use crate::store::Store;

    const HELLO: &[u8] = b"hello";
    const HELLO_DEFLATED: &[u8] = &[0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];

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
}
