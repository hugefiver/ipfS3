use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use reqwest::Body as ReqwestBody;
use reqwest::multipart;
use serde::Deserialize;

use super::client::KuboClient;
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    pub hash: String,
    #[serde(rename = "Size", default)]
    #[allow(dead_code)]
    pub size: String,
}

pub async fn stream_add<S, E>(kubo: &KuboClient, stream: S, cid_version: u8) -> AppResult<String>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    // Kubo /api/v0/add requires multipart/form-data with a file part.
    let mapped = stream.map_err(|e| {
        let boxed: Box<dyn std::error::Error + Send + Sync> = e.into();
        boxed
    });
    let body = ReqwestBody::wrap_stream(mapped);

    let part = multipart::Part::stream(body)
        .file_name("object")
        .mime_str("application/octet-stream")?;
    let form = multipart::Form::new().part("file", part);

    let url = format!(
        "{}/api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false",
        kubo.base_url()
    );

    let resp = kubo.upload_http().post(&url).multipart(form).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        tracing::warn!(
            operation = "add",
            status = status.as_u16(),
            "kubo rpc call failed"
        );
        return Err(AppError::kubo_rpc_status(status));
    }

    let text = resp.text().await?;
    let mut last_hash: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: AddResponse = serde_json::from_str(line)
            .map_err(|_| AppError::kubo_rpc_detail("invalid Kubo add response"))?;
        last_hash = Some(parsed.hash);
    }
    last_hash.ok_or_else(|| AppError::kubo_rpc_detail("empty Kubo add response"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
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
