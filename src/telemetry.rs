use axum::{extract::Request, middleware::Next, response::Response};
use tracing_subscriber::{
    Layer as _, filter::filter_fn, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};
use uuid::Uuid;

const APPLICATION_TARGET: &str = "ipfs_s3_gateway";
const SENSITIVE_EVENT_FIELDS: &[&str] = &[
    "authorization",
    "body",
    "bucket",
    "config",
    "cookie",
    "error",
    "headers",
    "key",
    "request",
    "response",
    "secret",
    "token",
    "uri",
    "url",
];

pub(crate) fn init() {
    let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
    let environment =
        tracing_subscriber::EnvFilter::builder().parse_lossy(if rust_log.is_empty() {
            "info".to_owned()
        } else {
            format!("info,{rust_log}")
        });
    let application_only = filter_fn(is_safe_application_event);
    let output = tracing_subscriber::fmt::layer()
        .with_filter(environment)
        .with_filter(application_only);

    tracing_subscriber::registry().with(output).init();
}

pub(crate) async fn record_request_status(request: Request, next: Next) -> Response {
    let request_id = Uuid::new_v4();
    let response = next.run(request).await;
    tracing::info!(
        request_id = %request_id,
        status = response.status().as_u16(),
        "request completed"
    );
    response
}

fn is_application_target(target: &str) -> bool {
    target == APPLICATION_TARGET
        || target
            .strip_prefix(APPLICATION_TARGET)
            .is_some_and(|suffix| suffix.starts_with("::"))
}

fn is_safe_application_event(metadata: &tracing::Metadata<'_>) -> bool {
    is_application_target(metadata.target())
        && metadata
            .fields()
            .iter()
            .all(|field| !is_sensitive_event_field(field.name()))
}

fn is_sensitive_event_field(name: &str) -> bool {
    SENSITIVE_EVENT_FIELDS.contains(&name)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

    use super::{is_application_target, is_safe_application_event, is_sensitive_event_field};

    #[derive(Clone)]
    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.lock().unwrap().flush()
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedWriter {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.0.clone())
        }
    }

    #[test]
    fn application_target_allowlist_is_namespace_bounded() {
        assert!(is_application_target("ipfs_s3_gateway"));
        assert!(is_application_target("ipfs_s3_gateway::pinning::worker"));
        assert!(!is_application_target("ipfs_s3_gateway_dependency"));
        assert!(!is_application_target("s3s::service"));
        assert!(!is_application_target("hyper::proto"));
        assert!(!is_application_target("reqwest::connect"));
    }

    #[test]
    fn sensitive_request_and_free_form_error_fields_are_blocked() {
        for field in [
            "authorization",
            "body",
            "bucket",
            "config",
            "cookie",
            "error",
            "headers",
            "key",
            "request",
            "response",
            "secret",
            "token",
            "uri",
            "url",
        ] {
            assert!(is_sensitive_event_field(field), "field={field}");
        }
        for field in ["failure_class", "operation", "request_id", "status"] {
            assert!(!is_sensitive_event_field(field), "field={field}");
        }
    }

    #[test]
    fn hard_filter_rejects_sensitive_app_fields_and_dependency_targets() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let layer = tracing_subscriber::fmt::layer()
            .without_time()
            .with_ansi(false)
            .with_writer(CapturedWriter(output.clone()))
            .with_filter(tracing_subscriber::filter::filter_fn(
                is_safe_application_event,
            ));
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                target: "ipfs_s3_gateway::test",
                error = "FREE_FORM_ERROR_SENTINEL",
                "unsafe app event"
            );
            tracing::warn!(
                target: "ipfs_s3_gateway::test",
                failure_class = "database",
                "safe app event"
            );
            tracing::warn!(
                target: "s3s::service",
                failure_class = "dependency",
                "dependency event"
            );
        });

        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("safe app event"), "logs: {logs}");
        assert!(logs.contains("failure_class=\"database\""), "logs: {logs}");
        assert!(!logs.contains("FREE_FORM_ERROR_SENTINEL"), "logs: {logs}");
        assert!(!logs.contains("dependency event"), "logs: {logs}");
    }
}
