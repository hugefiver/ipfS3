use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::Body as ReqwestBody;
use reqwest::multipart;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

use super::{
    KuboClient, KuboProgress, NdjsonBuffer, ProgressSender, next_response_frame, send_progress,
};
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
struct AddResponse {
    #[serde(rename = "Bytes")]
    bytes: Option<u64>,
    #[serde(rename = "Hash", default)]
    hash: Option<String>,
    #[serde(rename = "Size", default)]
    size: Option<String>,
}

#[derive(Debug)]
pub struct AddResult {
    pub cid: String,
    pub kubo_size: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum StreamAddError<E: std::error::Error + Send + Sync + 'static> {
    #[error("source stream failed")]
    Source(#[source] E),
    #[error("Kubo add failed")]
    Kubo(#[source] AppError),
    #[error("Kubo add was canceled")]
    Canceled,
}

const ADD_DUPLEX_CAPACITY: usize = 64 * 1024;

enum ProducerOutcome<E> {
    Complete,
    Source(E),
    ReceiverClosed,
    Canceled,
}

async fn pump_source<S, E>(
    stream: S,
    mut writer: tokio::io::DuplexStream,
    cancel: CancellationToken,
) -> ProducerOutcome<E>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::pin!(stream);
    loop {
        let item = tokio::select! {
            _ = cancel.cancelled() => return ProducerOutcome::Canceled,
            item = stream.next() => item,
        };
        match item {
            Some(Ok(bytes)) => {
                let write_result = tokio::select! {
                    _ = cancel.cancelled() => return ProducerOutcome::Canceled,
                    result = writer.write_all(&bytes) => result,
                };
                if write_result.is_err() {
                    return ProducerOutcome::ReceiverClosed;
                }
            }
            Some(Err(error)) => return ProducerOutcome::Source(error),
            None => {
                let _ = tokio::select! {
                    _ = cancel.cancelled() => return ProducerOutcome::Canceled,
                    result = writer.shutdown() => result,
                };
                return ProducerOutcome::Complete;
            }
        }
    }
}

pub async fn stream_add_with_progress<S, E>(
    kubo: &KuboClient,
    stream: S,
    cid_version: u8,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> Result<AddResult, StreamAddError<E>>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let (reader, writer) = tokio::io::duplex(ADD_DUPLEX_CAPACITY);
    let mut producer = Box::pin(pump_source(stream, writer, cancel.clone()));
    let body = ReqwestBody::wrap_stream(ReaderStream::new(reader));
    let part = multipart::Part::stream(body)
        .file_name("object")
        .mime_str("application/octet-stream")
        .expect("application/octet-stream is a valid MIME type");
    let form = multipart::Form::new().part("file", part);
    let url = format!(
        "{}/api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false&progress=true",
        kubo.base_url()
    );
    let mut request = Box::pin(kubo.upload_http().post(url).multipart(form).send());

    let (response, source_completed_before_headers) = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(StreamAddError::Canceled),
        outcome = &mut producer => {
            match outcome {
                ProducerOutcome::Source(error) => return Err(StreamAddError::Source(error)),
                ProducerOutcome::ReceiverClosed => return Err(StreamAddError::Kubo(
                    AppError::kubo_rpc_detail("Kubo add request closed before source completion"),
                )),
                ProducerOutcome::Canceled => return Err(StreamAddError::Canceled),
                ProducerOutcome::Complete => {
                    tokio::select! {
                        _ = cancel.cancelled() => return Err(StreamAddError::Canceled),
                        response = &mut request => (
                            response.map_err(AppError::from).map_err(StreamAddError::Kubo)?,
                            true,
                        ),
                    }
                }
            }
        }
        response = &mut request => (
            response.map_err(AppError::from).map_err(StreamAddError::Kubo)?,
            false,
        ),
    };

    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(
            operation = "add",
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(StreamAddError::Kubo(AppError::kubo_rpc_status(status)));
    }

    let mut parser = Box::pin(parse_add_response(response, kubo, &progress, &cancel));
    if source_completed_before_headers {
        return tokio::select! {
            _ = cancel.cancelled() => Err(StreamAddError::Canceled),
            result = &mut parser => result,
        };
    }

    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(StreamAddError::Canceled),
        outcome = &mut producer => match outcome {
            ProducerOutcome::Source(error) => Err(StreamAddError::Source(error)),
            ProducerOutcome::ReceiverClosed => Err(StreamAddError::Kubo(
                AppError::kubo_rpc_detail("Kubo add request closed before source completion"),
            )),
            ProducerOutcome::Canceled => Err(StreamAddError::Canceled),
            ProducerOutcome::Complete => tokio::select! {
                _ = cancel.cancelled() => Err(StreamAddError::Canceled),
                result = &mut parser => result,
            },
        },
        result = &mut parser => match result {
            Err(error) => Err(error),
            Ok(result) => tokio::select! {
                _ = cancel.cancelled() => Err(StreamAddError::Canceled),
                outcome = &mut producer => match outcome {
                    ProducerOutcome::Source(error) => Err(StreamAddError::Source(error)),
                    ProducerOutcome::ReceiverClosed => Err(StreamAddError::Kubo(
                        AppError::kubo_rpc_detail("Kubo add request closed before source completion"),
                    )),
                    ProducerOutcome::Canceled => Err(StreamAddError::Canceled),
                    ProducerOutcome::Complete => Ok(result),
                },
            },
        },
    }
}

async fn parse_add_response<E>(
    response: reqwest::Response,
    kubo: &KuboClient,
    progress: &ProgressSender,
    cancel: &CancellationToken,
) -> Result<AddResult, StreamAddError<E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let mut body = response.bytes_stream();
    let mut lines = NdjsonBuffer::new();
    let mut last_bytes = 0;
    let mut result = None;
    let mut final_record_was_root = false;
    loop {
        let frame = match next_response_frame(&mut body, kubo, cancel).await {
            Ok(frame) => frame,
            Err(_error) if cancel.is_cancelled() => return Err(StreamAddError::Canceled),
            Err(error) => return Err(StreamAddError::Kubo(error)),
        };
        let Some(frame) = frame else {
            break;
        };
        lines.push(frame).map_err(StreamAddError::Kubo)?;
        while let Some(record) = lines.next_record().map_err(StreamAddError::Kubo)? {
            final_record_was_root =
                process_add_record(&record, &mut last_bytes, &mut result, progress, cancel).await?;
        }
    }
    if let Some(record) = lines.finish() {
        final_record_was_root =
            process_add_record(&record, &mut last_bytes, &mut result, progress, cancel).await?;
    }
    if !final_record_was_root {
        return Err(StreamAddError::Kubo(AppError::kubo_rpc_detail(
            "Kubo add response omitted final root",
        )));
    }
    result.ok_or_else(|| StreamAddError::Kubo(AppError::kubo_rpc_detail("empty Kubo add response")))
}

async fn process_add_record<E>(
    record: &[u8],
    last_bytes: &mut u64,
    result: &mut Option<AddResult>,
    progress: &ProgressSender,
    cancel: &CancellationToken,
) -> Result<bool, StreamAddError<E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let response: AddResponse = serde_json::from_slice(record).map_err(|_| {
        StreamAddError::Kubo(AppError::kubo_rpc_detail("invalid Kubo add response"))
    })?;
    if let Some(bytes) = response.bytes {
        if bytes < *last_bytes {
            return Err(StreamAddError::Kubo(AppError::kubo_rpc_detail(
                "non-monotonic Kubo add progress",
            )));
        }
        *last_bytes = bytes;
        match send_progress(progress, KuboProgress::AddBytes { bytes }, cancel).await {
            Ok(()) => {}
            Err(_error) if cancel.is_cancelled() => return Err(StreamAddError::Canceled),
            Err(error) => return Err(StreamAddError::Kubo(error)),
        }
    }
    let is_root = if let Some(hash) = response.hash {
        if hash.is_empty() {
            return Err(StreamAddError::Kubo(AppError::kubo_rpc_detail(
                "invalid Kubo add response",
            )));
        }
        let kubo_size = response
            .size
            .map(|size| {
                size.parse().map_err(|_| {
                    StreamAddError::Kubo(AppError::kubo_rpc_detail("invalid Kubo add response"))
                })
            })
            .transpose()?;
        *result = Some(AddResult {
            cid: hash,
            kubo_size,
        });
        true
    } else {
        false
    };
    Ok(is_root)
}

#[derive(Debug)]
struct BoxedSourceError(Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for BoxedSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for BoxedSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub async fn stream_add<S, E>(kubo: &KuboClient, stream: S, cid_version: u8) -> AppResult<String>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let mapped = stream.map(|item| item.map_err(|error| BoxedSourceError(error.into())));
    let (progress, mut progress_receiver) = tokio::sync::mpsc::channel(32);
    let drain = tokio::spawn(async move { while progress_receiver.recv().await.is_some() {} });
    let outcome = stream_add_with_progress(
        kubo,
        mapped,
        cid_version,
        progress,
        CancellationToken::new(),
    )
    .await;
    let _ = drain.await;

    match outcome {
        Ok(result) => Ok(result.cid),
        Err(StreamAddError::Source(_)) => {
            Err(AppError::kubo_rpc_detail("Kubo add source stream failed"))
        }
        Err(StreamAddError::Kubo(error)) => Err(error),
        Err(StreamAddError::Canceled) => Err(super::canceled_rpc_error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::error::AppError;

    #[derive(Clone, Default)]
    struct TraceCapture(Arc<Mutex<Vec<u8>>>);

    impl Write for TraceCapture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceCapture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                return request;
            }
        }
    }

    async fn drain_request_body(socket: &mut tokio::net::TcpStream, headers: &[u8]) {
        let headers = String::from_utf8_lossy(headers).to_ascii_lowercase();
        if let Some(content_length) = headers.lines().find_map(|line| {
            line.strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        }) {
            let mut body = vec![0_u8; content_length];
            socket.read_exact(&mut body).await.unwrap();
            return;
        }

        loop {
            let mut size_line = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                size_line.push(byte[0]);
                if size_line.ends_with(b"\r\n") {
                    break;
                }
            }
            let size = usize::from_str_radix(
                std::str::from_utf8(&size_line[..size_line.len() - 2])
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap(),
                16,
            )
            .unwrap();
            if size == 0 {
                let mut terminator = [0_u8; 2];
                socket.read_exact(&mut terminator).await.unwrap();
                return;
            }
            let mut chunk = vec![0_u8; size + 2];
            socket.read_exact(&mut chunk).await.unwrap();
        }
    }

    async fn chunked_server(
        chunks: Vec<Vec<u8>>,
        keep_open: bool,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let headers = read_request_headers(&mut socket).await;
            drain_request_body(&mut socket, &headers).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            for chunk in chunks {
                socket
                    .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(&chunk).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                socket.flush().await.unwrap();
            }
            if keep_open {
                std::future::pending::<()>().await;
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        (endpoint, task)
    }

    async fn draining_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                if socket.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let mut buffer = [0_u8; 4096];
            while socket.read(&mut buffer).await.unwrap() != 0 {}
        });
        (endpoint, task)
    }

    async fn read_chunked_request_chunk(socket: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
        let mut size_line = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            size_line.push(byte[0]);
            if size_line.ends_with(b"\r\n") {
                break;
            }
        }
        let size = usize::from_str_radix(
            std::str::from_utf8(&size_line[..size_line.len() - 2])
                .unwrap()
                .split(';')
                .next()
                .unwrap(),
            16,
        )
        .unwrap();
        if size == 0 {
            let mut terminator = [0_u8; 2];
            socket.read_exact(&mut terminator).await.unwrap();
            return None;
        }
        let mut chunk = vec![0_u8; size + 2];
        socket.read_exact(&mut chunk).await.unwrap();
        chunk.truncate(size);
        Some(chunk)
    }

    async fn write_chunked_response_chunk(socket: &mut tokio::net::TcpStream, chunk: &[u8]) {
        socket
            .write_all(format!("{:X}\r\n", chunk.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(chunk).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        socket.flush().await.unwrap();
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
    enum DownloadError {
        #[error("source exceeded byte limit")]
        TooLarge,
        #[error("source stalled")]
        Stalled,
        #[error("source transfer was canceled")]
        Canceled,
    }

    #[tokio::test]
    async fn test_stream_add_parses_cid() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmRoot\",\"Size\":\"100\"}\n"),
            )
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let data: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::from("hello world"))];
        let s = stream::iter(data);

        let result = stream_add(&client, s, 1).await.unwrap();
        assert_eq!(result, "QmRoot");
    }

    #[tokio::test]
    async fn stream_add_accepts_a_source_stream_borrowing_local_data() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmBorrowed\",\"Size\":\"8\"}\n"),
            )
            .mount(&server)
            .await;

        let local_data = String::from("borrowed");
        let borrowed = local_data.as_str();
        let source = async_stream::stream! {
            yield Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(borrowed.as_bytes()));
        };

        let cid = stream_add(&KuboClient::new(server.uri()), source, 1)
            .await
            .expect("the compatibility wrapper must accept a borrowed source stream");
        assert_eq!(cid, "QmBorrowed");
    }

    #[tokio::test]
    async fn stream_add_reports_progress() {
        let (endpoint, server) = chunked_server(
            vec![
                b"{\"By".to_vec(),
                b"tes\":5}\r\n{\"Hash\":\"Qm".to_vec(),
                b"Root\",\"Size\":\"5\"}\n".to_vec(),
            ],
            false,
        )
        .await;

        let (progress, mut observed) = tokio::sync::mpsc::channel(4);
        let result = stream_add_with_progress(
            &KuboClient::new(endpoint),
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"hello"))]),
            1,
            progress,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("progress add must succeed");

        assert_eq!(result.cid, "QmRoot");
        assert_eq!(result.kubo_size, Some(5));
        assert_eq!(
            observed.recv().await,
            Some(super::super::KuboProgress::AddBytes { bytes: 5 })
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_add_drains_progress_while_source_waits_for_release() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let headers = read_request_headers(&mut socket).await;
            assert!(
                String::from_utf8_lossy(&headers)
                    .to_ascii_lowercase()
                    .contains("transfer-encoding: chunked")
            );

            let mut request = Vec::new();
            let mut progress_sent = false;
            while let Some(chunk) = read_chunked_request_chunk(&mut socket).await {
                request.extend_from_slice(&chunk);
                if !progress_sent
                    && request
                        .windows(b"first".len())
                        .any(|window| window == b"first")
                {
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .await
                        .unwrap();
                    write_chunked_response_chunk(&mut socket, b"{\"Bytes\":5}\n").await;
                    progress_sent = true;
                }
            }
            assert!(progress_sent, "the source's first chunk must reach Kubo");
            assert!(
                request
                    .windows(b"last".len())
                    .any(|window| window == b"last")
            );
            write_chunked_response_chunk(&mut socket, b"{\"Hash\":\"QmRoot\",\"Size\":\"9\"}\n")
                .await;
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });

        let (release_source, source_released) = tokio::sync::oneshot::channel();
        let source = async_stream::stream! {
            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"first"));
            source_released.await.expect("test progress consumer must release the source");
            yield Ok(Bytes::from_static(b"last"));
        };
        let (progress, mut observed) = tokio::sync::mpsc::channel(1);
        let client = KuboClient::new(endpoint);
        let mut add = Box::pin(stream_add_with_progress(
            &client,
            source,
            1,
            progress,
            CancellationToken::new(),
        ));

        let event = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                outcome = &mut add => panic!("add finished before progress: {outcome:?}"),
                event = observed.recv() => event.expect("Kubo must emit progress before request EOF"),
            }
        })
        .await
        .expect("add must drain Kubo progress while the source is waiting");
        assert_eq!(event, KuboProgress::AddBytes { bytes: 5 });
        release_source.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), &mut add)
            .await
            .expect("full-duplex add must finish after the source is released")
            .expect("full-duplex add must succeed");
        assert_eq!(result.cid, "QmRoot");
        assert_eq!(result.kubo_size, Some(9));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stream_add_preserves_exact_source_error_after_uploaded_chunks() {
        for expected in [
            DownloadError::TooLarge,
            DownloadError::Stalled,
            DownloadError::Canceled,
        ] {
            let (endpoint, server) = draining_server().await;
            let (progress, _observed) = tokio::sync::mpsc::channel(4);
            let outcome = tokio::time::timeout(
                Duration::from_secs(2),
                stream_add_with_progress(
                    &KuboClient::new(endpoint),
                    stream::iter(vec![
                        Ok(Bytes::from(vec![b'x'; ADD_DUPLEX_CAPACITY])),
                        Ok(Bytes::from_static(b"second")),
                        Err(expected),
                    ]),
                    1,
                    progress,
                    CancellationToken::new(),
                ),
            )
            .await
            .expect("the source error must not hang the upload")
            .expect_err("the source error must be returned");
            assert!(matches!(outcome, StreamAddError::Source(error) if error == expected));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn stream_add_rejects_malformed_and_oversized_ndjson_records() {
        for body in [
            b"{\"Bytes\":\n".to_vec(),
            vec![0xff, b'\n'],
            [
                vec![b'x'; super::super::MAX_NDJSON_RECORD_BYTES + 1],
                vec![b'\n'],
            ]
            .concat(),
            b"{\"Hash\":\"QmEarly\"}\n{\"Bytes\":1}\n".to_vec(),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
                .mount(&server)
                .await;
            let (progress, _observed) = tokio::sync::mpsc::channel(4);
            let error = stream_add_with_progress(
                &KuboClient::new(server.uri()),
                stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"input"))]),
                1,
                progress,
                CancellationToken::new(),
            )
            .await
            .expect_err("bad NDJSON must be rejected");
            assert!(matches!(
                error,
                StreamAddError::Kubo(AppError::KuboRpc { .. })
            ));
        }
    }

    #[tokio::test]
    async fn stream_add_cancels_and_times_out_when_kubo_stops_emitting_frames() {
        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let canceled = CancellationToken::new();
        canceled.cancel();
        let canceled_outcome = stream_add_with_progress(
            &KuboClient::new("http://127.0.0.1:1".to_owned()),
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::new())]),
            1,
            progress,
            canceled,
        )
        .await;
        assert!(matches!(canceled_outcome, Err(StreamAddError::Canceled)));

        let (endpoint, server) = chunked_server(Vec::new(), true).await;
        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(5),
            Duration::from_millis(50),
        );
        let stalled = stream_add_with_progress(
            &client,
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"input"))]),
            1,
            progress,
            CancellationToken::new(),
        )
        .await;
        assert!(matches!(
            stalled,
            Err(StreamAddError::Kubo(AppError::KuboRpc { .. }))
        ));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn test_stream_add_error_on_non_200() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(500).set_body_string("kubo-body-marker-do-not-leak"),
            )
            .mount(&server)
            .await;

        let capture = TraceCapture::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(capture.clone())
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default_guard = tracing::dispatcher::set_default(&dispatch);
        tracing::callsite::rebuild_interest_cache();

        let client = KuboClient::new(server.uri());
        let data: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::from("hello world"))];
        let s = stream::iter(data);

        let result = stream_add(&client, s, 1).await;
        let error = result.expect_err("non-2xx add must fail");
        let message = error.to_string();
        assert!(
            !message.contains("kubo-body-marker-do-not-leak"),
            "kubo response body must not leak into the error: {message}"
        );
        assert_eq!(message, "kubo rpc failure");
        assert!(matches!(
            error,
            AppError::KuboRpc {
                status: Some(500),
                ..
            }
        ));
        let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(
            !logs.contains("kubo-body-marker-do-not-leak"),
            "kubo response body must not leak into tracing: {logs}"
        );
    }
}
