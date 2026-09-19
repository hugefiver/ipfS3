use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::header::CONTENT_LENGTH;

/// Supplies s3s with the signed decoded length when `Content-Length` is absent.
///
/// `x-amz-decoded-content-length` is part of the signed request and gives s3s
/// the exact decoded body length without changing the observed wire framing.
pub async fn bridge_chunked_content_length(mut request: Request, next: Next) -> Response {
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

    next.run(request).await
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
