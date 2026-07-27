use super::client::KuboClient;
use crate::error::{AppError, AppResult};

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
