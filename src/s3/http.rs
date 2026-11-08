use axum::body::HttpBody as _;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::header::CONTENT_LENGTH;

/// Rejects browser forms, then bridges decoded or proven-empty POST lengths.
///
/// `x-amz-decoded-content-length` is part of the signed request and gives s3s
/// the exact decoded body length without changing the observed wire framing.
pub async fn bridge_chunked_content_length(mut request: Request, next: Next) -> Response {
    // s3s parses browser forms before authentication and may aggregate gigabytes.
    // This must stay outside the s3s service, not in its custom route callback.
    // s3s selects form authentication by method/media type before inspecting
    // the route. Do not exempt query keys: that would reopen aggregation via
    // ?uploads, ?uploadId or a custom route. Their normal XML POSTs are untouched.
    if request.method() == http::Method::POST
        && request
            .headers()
            .get_all(http::header::CONTENT_TYPE)
            .iter()
            .any(|value| {
                value.to_str().is_ok_and(|value| {
                    value
                        .split(';')
                        .next()
                        .unwrap_or_default()
                        .trim()
                        .eq_ignore_ascii_case("multipart/form-data")
                })
            })
    {
        return Response::builder()
            .status(http::StatusCode::BAD_REQUEST)
            .header(http::header::CONTENT_TYPE, "application/xml")
            .body(axum::body::Body::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidRequest</Code><Message>Browser multipart/form-data uploads are not supported</Message></Error>",
            ))
            .expect("static admission response");
    }
    if !request.headers().contains_key(CONTENT_LENGTH)
        && let Some(length) = request
            .headers()
            .get("x-amz-decoded-content-length")
            .cloned()
        && !length.as_bytes().is_empty()
        && length.as_bytes().iter().all(u8::is_ascii_digit)
    {
        request.headers_mut().insert(CONTENT_LENGTH, length);
    }
    if request.method() == http::Method::POST
        && !request.headers().contains_key(CONTENT_LENGTH)
        && !request
            .headers()
            .contains_key(http::header::TRANSFER_ENCODING)
        && !request
            .headers()
            .contains_key("x-amz-decoded-content-length")
        && request.body().is_end_stream()
    {
        request
            .headers_mut()
            .insert(CONTENT_LENGTH, http::HeaderValue::from_static("0"));
    }

    next.run(request).await
}

/// Conditions must not disappear when a custom route builds an s3s DTO. Check
/// raw header presence (including empty values) as well as typed DTO presence.
pub(crate) fn reject_write_conditions(headers: &http::HeaderMap, typed: bool) -> s3s::S3Result<()> {
    if typed
        || headers.contains_key(http::header::IF_MATCH)
        || headers.contains_key(http::header::IF_NONE_MATCH)
    {
        return Err(s3s::s3_error!(
            InvalidRequest,
            "conditional writes are not supported"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{Router, body::Body, middleware::from_fn, routing::any};
    use http::{HeaderMap, HeaderValue, header::TRANSFER_ENCODING};
    use tower::ServiceExt as _;

    use super::*;

    const OBSERVED_CONTENT_LENGTH: &str = "x-observed-content-length";

    async fn reflect_content_length(headers: HeaderMap) -> Response {
        let mut response = Response::new(Body::empty());
        if let Some(length) = headers.get(CONTENT_LENGTH) {
            response
                .headers_mut()
                .insert(OBSERVED_CONTENT_LENGTH, length.clone());
        }
        response
    }

    async fn observed_content_length(
        content_length: Option<&'static str>,
        decoded_content_length: Option<&'static str>,
        chunked: bool,
    ) -> Option<HeaderValue> {
        let app = Router::new()
            .route("/", any(reflect_content_length))
            .layer(from_fn(bridge_chunked_content_length));
        let mut request = Request::builder().uri("/").body(Body::empty()).unwrap();
        if let Some(length) = content_length {
            request
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_static(length));
        }
        if let Some(length) = decoded_content_length {
            request.headers_mut().insert(
                "x-amz-decoded-content-length",
                HeaderValue::from_static(length),
            );
        }
        if chunked {
            request
                .headers_mut()
                .insert(TRANSFER_ENCODING, HeaderValue::from_static("chunked"));
        }

        app.oneshot(request)
            .await
            .unwrap()
            .headers()
            .get(OBSERVED_CONTENT_LENGTH)
            .cloned()
    }

    async fn bridged_headers(request: Request) -> HeaderMap {
        Router::new()
            .route(
                "/",
                any(|headers: HeaderMap| async move {
                    let mut response = Response::new(Body::empty());
                    response.extensions_mut().insert(headers);
                    response
                }),
            )
            .layer(from_fn(bridge_chunked_content_length))
            .oneshot(request)
            .await
            .unwrap()
            .extensions_mut()
            .remove::<HeaderMap>()
            .unwrap()
    }

    #[tokio::test]
    async fn supplies_zero_for_proven_empty_posts_without_framing() {
        for version in [http::Version::HTTP_11, http::Version::HTTP_2] {
            let request = Request::builder()
                .method(http::Method::POST)
                .version(version)
                .uri("/")
                .header("authorization", "signed-request")
                .header("x-amz-content-sha256", "signed-payload")
                .header("x-amz-meta-name", "metadata")
                .body(Body::empty())
                .unwrap();
            let original = request.headers().clone();
            let headers = bridged_headers(request).await;
            assert_eq!(
                headers.get(CONTENT_LENGTH),
                Some(&HeaderValue::from_static("0"))
            );
            for (name, value) in &original {
                assert_eq!(headers.get(name), Some(value));
            }
        }
    }

    #[tokio::test]
    async fn does_not_infer_zero_from_http2_missing_framing_or_poll_streams() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let pending = futures_util::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending::<Option<Result<bytes::Bytes, std::io::Error>>>
        });
        for body in [Body::from("payload"), Body::from_stream(pending)] {
            let request = Request::builder()
                .method(http::Method::POST)
                .version(http::Version::HTTP_2)
                .uri("/")
                .body(body)
                .unwrap();
            assert!(!bridged_headers(request).await.contains_key(CONTENT_LENGTH));
        }
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn empty_post_preserves_framing_and_decoded_length_validation() {
        for (name, value, expected) in [
            ("content-length", "7", Some("7")),
            ("transfer-encoding", "chunked", None),
            ("x-amz-decoded-content-length", "12x", None),
            ("x-amz-decoded-content-length", "", None),
            ("x-amz-decoded-content-length", "-1", None),
            ("x-amz-decoded-content-length", "0", Some("0")),
            ("x-amz-decoded-content-length", "11", Some("11")),
        ] {
            let request = Request::builder()
                .method(http::Method::POST)
                .uri("/")
                .header(name, value)
                .body(Body::empty())
                .unwrap();
            let headers = bridged_headers(request).await;
            assert_eq!(headers.get(name).unwrap(), value);
            assert_eq!(
                headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()),
                expected
            );
        }
    }

    #[tokio::test]
    async fn empty_post_form_admission_precedes_zero_bridge() {
        let app = Router::new()
            .route("/", any(|| async { http::StatusCode::IM_A_TEAPOT }))
            .layer(from_fn(bridge_chunked_content_length));
        let request = Request::builder()
            .method(http::Method::POST)
            .uri("/?uploads")
            .header("content-type", "Multipart/Form-Data ; boundary=upload")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            http::StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn empty_non_post_is_not_given_content_length() {
        let request = Request::builder().uri("/").body(Body::empty()).unwrap();
        assert!(!bridged_headers(request).await.contains_key(CONTENT_LENGTH));
    }

    #[tokio::test]
    async fn preserves_existing_content_length() {
        assert_eq!(
            observed_content_length(Some("7"), Some("11"), true).await,
            Some(HeaderValue::from_static("7"))
        );
    }

    #[tokio::test]
    async fn supplies_valid_decoded_length_for_chunked_and_non_chunked_requests() {
        for chunked in [true, false] {
            assert_eq!(
                observed_content_length(None, Some("11"), chunked).await,
                Some(HeaderValue::from_static("11"))
            );
        }
    }

    #[tokio::test]
    async fn leaves_content_length_absent_without_decoded_length() {
        assert_eq!(observed_content_length(None, None, true).await, None);
    }

    #[tokio::test]
    async fn rejects_invalid_or_empty_decoded_length() {
        for length in ["12x", ""] {
            assert_eq!(
                observed_content_length(None, Some(length), true).await,
                None
            );
        }
    }
}
