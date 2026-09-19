use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http_body_util::BodyExt as _;
use tokio_util::sync::CancellationToken;

use super::{KuboClient, send_request};
use crate::error::{AppError, AppResult};

const STREAM_ERROR_HEADER: &str = "x-stream-error";

fn has_stream_error(headers: &reqwest::header::HeaderMap) -> bool {
    headers.contains_key(STREAM_ERROR_HEADER)
}

pub async fn stream_cat(
    kubo: &KuboClient,
    cid: &str,
    range: Option<(u64, u64)>,
) -> AppResult<impl Stream<Item = Result<Bytes, std::io::Error>> + use<>> {
    let mut url = if let Some((start, end)) = range {
        format!(
            "{}/api/v0/cat?arg={cid}&offset={start}&length={}",
            kubo.base_url(),
            end.saturating_sub(start)
        )
    } else {
        format!("{}/api/v0/cat?arg={cid}", kubo.base_url())
    };
    if kubo.local_reads_only() {
        // Kubo v0.43 GetApi applies Api.Offline(true), replacing the block
        // exchange for this entire UnixFS read, including linked/raw leaves.
        url.push_str("&offline=true");
    }

    let resp = kubo.download_http().post(&url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        tracing::warn!(
            operation = "cat",
            cid = %cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }

    let idle_timeout = kubo.stream_idle_timeout();
    if has_stream_error(resp.headers()) {
        return Err(AppError::kubo_rpc_detail(
            "Kubo cat reported stream failure",
        ));
    }
    let mut body: reqwest::Body = resp.into();
    let stream = async_stream::stream! {
        loop {
            let frame = match tokio::time::timeout(idle_timeout, body.frame()).await {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(_))) | Err(_) => {
                    yield Err(crate::error::kubo_stream_error());
                    return;
                }
                Ok(None) => return,
            };
            match frame.into_data() {
                Ok(data) => yield Ok(data),
                Err(frame) => match frame.into_trailers() {
                    Ok(trailers) if has_stream_error(&trailers) => {
                        yield Err(crate::error::kubo_stream_error());
                        return;
                    }
                    Ok(_) | Err(_) => {}
                },
            }
        }
    };
    Ok(Box::pin(stream))
}

pub async fn inspect_file(
    kubo: &KuboClient,
    cid: &str,
    cancel: CancellationToken,
) -> AppResult<u64> {
    let url = format!("{}/api/v0/cat?arg={cid}", kubo.base_url());
    let response = send_request(kubo.download_http().post(url), &cancel).await?;
    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(
            operation = "cat inspect",
            cid = %cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }
    if has_stream_error(response.headers()) {
        return Err(AppError::kubo_rpc_detail(
            "Kubo cat inspection reported stream failure",
        ));
    }

    let expected_size = response.content_length();
    let mut body: reqwest::Body = response.into();
    let mut size = 0_u64;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => return Err(super::canceled_rpc_error()),
            frame = tokio::time::timeout(kubo.stream_idle_timeout(), body.frame()) => frame,
        };
        let frame = match next {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(_))) => {
                return Err(AppError::kubo_rpc_detail("Kubo response stream failed"));
            }
            Ok(None) => break,
            Err(_) => {
                return Err(AppError::kubo_rpc_detail("Kubo response stream timed out"));
            }
        };
        match frame.into_data() {
            Ok(data) => {
                let chunk_len = u64::try_from(data.len())
                    .map_err(|_| AppError::kubo_rpc_detail("Kubo file size exceeds limit"))?;
                size = size
                    .checked_add(chunk_len)
                    .ok_or_else(|| AppError::kubo_rpc_detail("Kubo file size exceeds limit"))?;
            }
            Err(frame) => match frame.into_trailers() {
                Ok(trailers) if has_stream_error(&trailers) => {
                    return Err(AppError::kubo_rpc_detail(
                        "Kubo cat inspection reported stream failure",
                    ));
                }
                Ok(_) | Err(_) => {}
            },
        }
    }
    if expected_size.is_some_and(|expected| expected != size) {
        return Err(AppError::kubo_rpc_detail(
            "Kubo response stream ended before Content-Length",
        ));
    }
    Ok(size)
}

#[allow(dead_code)]
pub async fn cat_to_vec(
    kubo: &KuboClient,
    cid: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let stream = stream_cat(kubo, cid, None).await?;
    tokio::pin!(stream);
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AppError::kubo_rpc_detail("Kubo response stream failed"))?;
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::error::AppError;

    #[tokio::test]
    async fn initial_stream_error_is_rejected_before_returning_a_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("X-Stream-Error", "private backend failure")
                    .set_body_bytes(b"partial"),
            )
            .mount(&server)
            .await;
        let result = stream_cat(&KuboClient::new(server.uri()), "QmTest", None).await;
        let error = match result {
            Ok(_) => panic!("initial error must fail before constructing a body"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "kubo rpc failure");
        assert!(!format!("{error:?}").contains("private backend failure"));
    }

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

    async fn chunked_cat_server(
        chunks: Vec<Vec<u8>>,
        keep_open: bool,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
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

    async fn trailer_cat_server() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
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
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n5\r\nfirst\r\n0\r\nX-Stream-Error: late cat failure\r\n\r\n",
                )
                .await
                .unwrap();
        });
        (endpoint, task)
    }

    async fn fixed_length_cat_server(
        declared: usize,
        body: &'static [u8],
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
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
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {declared}\r\nConnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(body).await.unwrap();
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn test_stream_cat_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = cat_to_vec(&client, "QmTest").await.unwrap();
        assert_eq!(result, b"hello world");
    }

    #[tokio::test]
    async fn stream_cat_translates_half_open_range_to_offset_and_length() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", "QmTest"))
            .and(query_param("offset", "7"))
            .and(query_param("length", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_string("world"))
            .expect(1)
            .mount(&server)
            .await;

        let stream = stream_cat(&KuboClient::new(server.uri()), "QmTest", Some((7, 12)))
            .await
            .unwrap();
        tokio::pin!(stream);
        let mut bytes = Vec::new();
        while let Some(frame) = stream.next().await {
            bytes.extend_from_slice(&frame.unwrap());
        }
        assert_eq!(bytes, b"world");
    }

    #[tokio::test]
    async fn inspect_file_uses_content_length_or_streaming_count_without_collecting() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("logical"))
            .mount(&server)
            .await;
        let header_size = inspect_file(
            &KuboClient::new(server.uri()),
            "QmLogical",
            CancellationToken::new(),
        )
        .await
        .expect("content length should be accepted");
        assert_eq!(header_size, 7);

        let (endpoint, server) =
            chunked_cat_server(vec![b"lo".to_vec(), b"gical-size".to_vec()], false).await;
        let counted_size = inspect_file(
            &KuboClient::new(endpoint),
            "QmLogical",
            CancellationToken::new(),
        )
        .await
        .expect("chunked cat must be counted incrementally");
        assert_eq!(counted_size, 12);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn inspect_file_rejects_initial_and_trailer_stream_errors() {
        let initial = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("X-Stream-Error", "private backend failure")
                    .set_body_bytes(b"partial"),
            )
            .mount(&initial)
            .await;
        inspect_file(
            &KuboClient::new(initial.uri()),
            "QmInitialError",
            CancellationToken::new(),
        )
        .await
        .expect_err("initial X-Stream-Error must reject inspection even with Content-Length");

        let (endpoint, server) = trailer_cat_server().await;
        inspect_file(
            &KuboClient::new(endpoint),
            "QmLateError",
            CancellationToken::new(),
        )
        .await
        .expect_err("late X-Stream-Error must reject inspection after partial data");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn inspect_file_rejects_a_short_body_with_content_length() {
        let (endpoint, server) = fixed_length_cat_server(10, b"short").await;
        inspect_file(
            &KuboClient::new(endpoint),
            "QmShortBody",
            CancellationToken::new(),
        )
        .await
        .expect_err("Content-Length is not proof that all bytes arrived");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn inspect_file_rejects_directory_errors_and_honors_cancel_and_idle_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(400).set_body_string("directory marker"))
            .mount(&server)
            .await;
        let error = inspect_file(
            &KuboClient::new(server.uri()),
            "QmDirectory",
            CancellationToken::new(),
        )
        .await
        .expect_err("Kubo cat must reject a directory");
        assert!(matches!(
            error,
            AppError::KuboRpc {
                status: Some(400),
                ..
            }
        ));
        assert!(!error.to_string().contains("directory marker"));

        let canceled = CancellationToken::new();
        canceled.cancel();
        let error = inspect_file(
            &KuboClient::new("http://127.0.0.1:1".to_owned()),
            "QmCanceled",
            canceled,
        )
        .await
        .expect_err("cancelled inspection must not send a request");
        assert_eq!(error.to_string(), "kubo rpc failure");

        let (endpoint, server) = chunked_cat_server(Vec::new(), true).await;
        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(5),
            Duration::from_millis(50),
        );
        let error = inspect_file(&client, "QmStalled", CancellationToken::new())
            .await
            .expect_err("stalled inspection must time out");
        assert!(matches!(error, AppError::KuboRpc { .. }));
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn stalled_successful_cat_body_keeps_safe_kubo_provenance() {
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
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
                .await
                .unwrap();
            first_chunk_sent.send(()).unwrap();
            std::future::pending::<()>().await;
        });

        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(300),
            Duration::from_millis(50),
        );
        let stream = stream_cat(&client, "QmStalled", None).await.unwrap();
        first_chunk_observed.await.unwrap();
        tokio::pin!(stream);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
        let error = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("a stalled Kubo body must not hang")
            .expect("the body stream must produce an error")
            .expect_err("the next chunk must time out");

        assert_eq!(
            error.to_string(),
            crate::error::INTERNAL_STORAGE_BACKEND_ERROR
        );
        assert!(
            crate::error::has_kubo_stream_provenance(&error),
            "Kubo body failure must stay identifiable without exposing transport details: {error}"
        );
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn late_cat_error_trailer_becomes_a_safe_stream_error() {
        let (endpoint, server) = trailer_cat_server().await;
        let stream = stream_cat(&KuboClient::new(endpoint), "QmLateError", None)
            .await
            .unwrap();
        tokio::pin!(stream);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
        let error = stream
            .next()
            .await
            .expect("trailer must emit a terminal error")
            .expect_err("X-Stream-Error must not be accepted as EOF");
        assert!(crate::error::has_kubo_stream_provenance(&error));
        assert!(stream.next().await.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_cat_error_does_not_leak_response_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(
                ResponseTemplate::new(502).set_body_string("kubo-body-marker-do-not-leak"),
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
        let error = stream_cat(&client, "QmTest", None)
            .await
            .err()
            .expect("non-2xx cat must fail");
        let message = error.to_string();
        assert!(
            !message.contains("kubo-body-marker-do-not-leak"),
            "kubo response body must not leak into the error: {message}"
        );
        assert_eq!(message, "kubo rpc failure");
        assert!(matches!(
            error,
            AppError::KuboRpc {
                status: Some(502),
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
