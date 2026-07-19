use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};

/// Supplies s3s with the signed decoded length for a chunked request.
///
/// Hyper correctly removes `Content-Length` from HTTP/1.1 chunked requests,
/// but s3s needs an exact length to verify a single-chunk SigV4 payload hash.
/// `x-amz-decoded-content-length` is part of the signed request and describes
/// the decoded body without changing the observed wire framing.
pub async fn bridge_chunked_content_length(mut request: Request, next: Next) -> Response {
    let is_chunked = request
        .headers()
        .get(TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        });

    if is_chunked
        && !request.headers().contains_key(CONTENT_LENGTH)
        && let Some(length) = request
            .headers()
            .get("x-amz-decoded-content-length")
            .cloned()
        && length.as_bytes().iter().all(|byte| byte.is_ascii_digit())
    {
        request.headers_mut().insert(CONTENT_LENGTH, length);
    }

    next.run(request).await
}
