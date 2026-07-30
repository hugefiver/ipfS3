use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;

use super::{KuboClient, next_response_frame, send_request};
use crate::error::{AppError, AppResult};

pub async fn stream_cat(
    kubo: &KuboClient,
    cid: &str,
    range: Option<(u64, u64)>,
) -> AppResult<impl Stream<Item = Result<Bytes, std::io::Error>> + use<>> {
    let url = if let Some((start, end)) = range {
        format!(
            "{}/api/v0/cat?arg={cid}&bytes={start}-{}",
            kubo.base_url(),
            end.saturating_sub(1)
        )
    } else {
        format!("{}/api/v0/cat?arg={cid}", kubo.base_url())
    };

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

    let stream = resp
        .bytes_stream()
        .map(|result| result.map_err(|_| crate::error::kubo_stream_error()));
    Ok(stream)
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
    if let Some(size) = response.content_length() {
        // Dropping the response here closes its body without materializing file
        // data. Kubo's cat response supplies the logical file bytes directly.
        return Ok(size);
    }

    let mut body = response.bytes_stream();
    let mut size = 0_u64;
    loop {
        let Some(frame) = next_response_frame(&mut body, kubo, &cancel).await? else {
            break;
        };
        let chunk_len = u64::try_from(frame.len())
            .map_err(|_| AppError::kubo_rpc_detail("Kubo file size exceeds limit"))?;
        size = size
            .checked_add(chunk_len)
            .ok_or_else(|| AppError::kubo_rpc_detail("Kubo file size exceeds limit"))?;
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
