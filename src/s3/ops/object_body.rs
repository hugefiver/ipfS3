//! Fixed Content-Length must not hide a late backend/crypto/trailer failure.

use bytes::Bytes;
use futures_util::StreamExt;
use s3s::{S3Result, dto::StreamingBlob};

use crate::error::INTERNAL_STORAGE_BACKEND_ERROR;

/// Hold only the final response byte until upstream clean EOF. Earlier bytes
/// remain backpressured; no object/range buffer is accumulated. Empty responses
/// cannot carry a later stream error, so their upstream is drained before the
/// response is constructed.
pub(super) async fn finish_get_body(
    mut body: StreamingBlob,
    length: u64,
) -> S3Result<StreamingBlob> {
    if length == 0 {
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) if bytes.is_empty() => {}
                _ => {
                    return Err(s3s::s3_error!(
                        InternalError,
                        "internal storage backend error"
                    ));
                }
            }
        }
        return Ok(StreamingBlob::from(s3s::Body::empty()));
    }

    let stream = async_stream::stream! {
        let mut remaining = length;
        let mut tail = None;
        while let Some(chunk) = body.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(error) => {
                    yield Err(match error.downcast::<std::io::Error>() {
                        Ok(error) => *error,
                        Err(error) => std::io::Error::other(error),
                    });
                    return;
                }
            };
            if bytes.is_empty() {
                continue;
            }
            if bytes.len() as u64 > remaining {
                yield Err(std::io::Error::other(INTERNAL_STORAGE_BACKEND_ERROR));
                return;
            }
            remaining -= bytes.len() as u64;
            if remaining == 0 {
                let split = bytes.len() - 1;
                // Copy a single byte, rather than retaining a large input frame.
                tail = Some(Bytes::copy_from_slice(&bytes[split..]));
                if split != 0 {
                    yield Ok(bytes.slice(..split));
                }
            } else {
                yield Ok(bytes);
            }
        }
        if let Some(tail) = tail {
            yield Ok(tail);
        } else {
            yield Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                INTERNAL_STORAGE_BACKEND_ERROR,
            ));
        }
    };
    Ok(StreamingBlob::wrap(stream))
}
