use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use super::client::KuboClient;
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
