use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{
    KuboClient, KuboProgress, NdjsonBuffer, ProgressSender, next_response_frame, send_progress,
    send_request,
};
use crate::error::{AppError, AppResult};

#[derive(Deserialize)]
struct PinResponse {
    #[serde(rename = "Progress")]
    progress: Option<u64>,
    #[serde(rename = "Bytes")]
    bytes: Option<u64>,
    #[serde(rename = "Pins")]
    pins: Option<Vec<String>>,
}

pub async fn pin_add_with_progress(
    kubo: &KuboClient,
    cid: &str,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> AppResult<()> {
    let canonical_cid = cid::Cid::try_from(cid)
        .map_err(|_| AppError::kubo_rpc_detail("invalid CID for Kubo pin"))?
        .to_string();
    let url = format!(
        "{}/api/v0/pin/add?arg={canonical_cid}&recursive=true&progress=true",
        kubo.base_url()
    );
    let response = send_request(kubo.upload_http().post(url), &cancel).await?;
    if !response.status().is_success() {
        let status = response.status();
        tracing::warn!(
            operation = "pin add",
            cid = %canonical_cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }

    let mut body = response.bytes_stream();
    let mut lines = NdjsonBuffer::new();
    let mut last_progress = 0;
    let mut last_bytes = 0;
    let mut final_record_confirmed_requested_pin = false;
    loop {
        let Some(frame) = next_response_frame(&mut body, kubo, &cancel).await? else {
            break;
        };
        lines.push(frame)?;
        while let Some(record) = lines.next_record()? {
            final_record_confirmed_requested_pin = process_pin_record(
                &record,
                &canonical_cid,
                &mut last_progress,
                &mut last_bytes,
                &progress,
                &cancel,
            )
            .await?;
        }
    }
    if let Some(record) = lines.finish() {
        final_record_confirmed_requested_pin = process_pin_record(
            &record,
            &canonical_cid,
            &mut last_progress,
            &mut last_bytes,
            &progress,
            &cancel,
        )
        .await?;
    }
    if !final_record_confirmed_requested_pin {
        return Err(AppError::kubo_rpc_detail(
            "Kubo pin response omitted requested CID",
        ));
    }
    Ok(())
}

async fn process_pin_record(
    record: &[u8],
    requested_cid: &str,
    last_progress: &mut u64,
    last_bytes: &mut u64,
    progress: &ProgressSender,
    cancel: &CancellationToken,
) -> AppResult<bool> {
    let response: PinResponse = serde_json::from_slice(record)
        .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo pin response"))?;
    match (response.progress, response.bytes) {
        (Some(nodes), Some(bytes)) => {
            if nodes < *last_progress || bytes < *last_bytes {
                return Err(AppError::kubo_rpc_detail("non-monotonic Kubo pin progress"));
            }
            *last_progress = nodes;
            *last_bytes = bytes;
            send_progress(progress, KuboProgress::PinProgress { nodes, bytes }, cancel).await?;
        }
        (None, None) => {}
        _ => return Err(AppError::kubo_rpc_detail("invalid Kubo pin response")),
    }
    let mut confirmed_requested_pin = false;
    if let Some(pins) = response.pins {
        for pin in pins {
            let canonical_pin = cid::Cid::try_from(pin.as_str())
                .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo pin response"))?
                .to_string();
            if canonical_pin == requested_cid {
                confirmed_requested_pin = true;
            }
        }
    }
    Ok(confirmed_requested_pin)
}

pub async fn pin_add(kubo: &KuboClient, cid: &str) -> AppResult<()> {
    let url = format!("{}/api/v0/pin/add?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        tracing::warn!(
            operation = "pin add",
            cid = %cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }
    Ok(())
}

pub async fn pin_rm(kubo: &KuboClient, cid: &str) -> AppResult<()> {
    let url = format!("{}/api/v0/pin/rm?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        tracing::warn!(
            operation = "pin rm",
            cid = %cid,
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    async fn chunked_server(chunks: Vec<Vec<u8>>) -> (String, tokio::task::JoinHandle<()>) {
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
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        (endpoint, task)
    }

    async fn delayed_chunked_server(
        chunks: Vec<Vec<u8>>,
        delay: Duration,
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
                tokio::time::sleep(delay).await;
            }
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        (endpoint, task)
    }

    #[tokio::test]
    async fn test_pin_add_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = pin_add(&client, "QmTest").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn pin_add_reports_progress() {
        let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
        let (endpoint, server) = chunked_server(vec![
            b"{\"Progress\":3,".to_vec(),
            format!("\"Bytes\":7}}\r\n{{\"Pins\":[\"{cid}\"]}}\n").into_bytes(),
        ])
        .await;

        let (progress, mut observed) = tokio::sync::mpsc::channel(4);
        pin_add_with_progress(
            &KuboClient::new(endpoint),
            cid,
            progress,
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .expect("progress pin must succeed");

        assert_eq!(
            observed.recv().await,
            Some(super::super::KuboProgress::PinProgress { nodes: 3, bytes: 7 })
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn progress_pin_ignores_the_control_plane_whole_request_timeout() {
        let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
        let (endpoint, server) = delayed_chunked_server(
            vec![
                b"{\"Progress\":1,\"Bytes\":1}\n".to_vec(),
                b"{\"Progress\":2,\"Bytes\":2}\n".to_vec(),
                format!("{{\"Pins\":[\"{cid}\"]}}\n").into_bytes(),
            ],
            Duration::from_millis(80),
        )
        .await;
        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_millis(50),
            Duration::from_millis(200),
        );
        let (progress, _observed) = tokio::sync::mpsc::channel(4);

        pin_add_with_progress(&client, cid, progress, CancellationToken::new())
            .await
            .expect("streaming pin progress must not inherit the control request deadline");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn pin_add_requires_requested_final_pin_and_honors_cancellation() {
        let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Pins\":[\"QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE\"]}\n",
            ))
            .mount(&server)
            .await;
        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let error = pin_add_with_progress(
            &KuboClient::new(server.uri()),
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
            progress,
            CancellationToken::new(),
        )
        .await
        .expect_err("a response without the requested CID must fail");
        assert!(matches!(error, AppError::KuboRpc { .. }));

        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let canceled = CancellationToken::new();
        canceled.cancel();
        let error = pin_add_with_progress(
            &KuboClient::new("http://127.0.0.1:1".to_owned()),
            cid,
            progress,
            canceled,
        )
        .await
        .expect_err("canceled pin must fail");
        assert_eq!(error.to_string(), "kubo rpc failure");
    }

    #[tokio::test]
    async fn pin_add_rejects_malformed_progress_and_redacts_non_success() {
        let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Progress\":1}\n"))
            .mount(&server)
            .await;
        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let error = pin_add_with_progress(
            &KuboClient::new(server.uri()),
            cid,
            progress,
            CancellationToken::new(),
        )
        .await
        .expect_err("partial progress frames are invalid");
        assert!(matches!(error, AppError::KuboRpc { .. }));

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(503).set_body_string("private-body"))
            .mount(&server)
            .await;
        let (progress, _observed) = tokio::sync::mpsc::channel(1);
        let error = pin_add_with_progress(
            &KuboClient::new(server.uri()),
            cid,
            progress,
            CancellationToken::new(),
        )
        .await
        .expect_err("non-success pin must fail");
        assert_eq!(error.to_string(), "kubo rpc failure");
        assert!(!error.to_string().contains("private-body"));
    }

    #[tokio::test]
    async fn test_pin_rm_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = pin_rm(&client, "QmTest").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pin_add_error_does_not_leak_response_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
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
        let error = pin_add(&client, "QmTest")
            .await
            .expect_err("non-2xx pin add must fail");
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

    #[tokio::test]
    async fn test_pin_rm_error_does_not_leak_response_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(
                ResponseTemplate::new(503).set_body_string("kubo-body-marker-do-not-leak"),
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
        let error = pin_rm(&client, "QmTest")
            .await
            .expect_err("non-2xx pin rm must fail");
        let message = error.to_string();
        assert!(
            !message.contains("kubo-body-marker-do-not-leak"),
            "kubo response body must not leak into the error: {message}"
        );
        assert_eq!(message, "kubo rpc failure");
        assert!(matches!(
            error,
            AppError::KuboRpc {
                status: Some(503),
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
