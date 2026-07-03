pub mod noop;

use crate::error::AppResult;

/// Pinning service abstraction. In MVP, only NoopPinningService is used.
/// Future implementations (Pinata, etc.) can plug in here.
#[async_trait::async_trait]
#[allow(dead_code)]
pub trait PinningService: Send + Sync + 'static {
    /// Pin a CID on the remote pinning service.
    async fn pin(&self, cid: &str, name: Option<&str>) -> AppResult<()>;

    /// Unpin a CID from the remote pinning service.
    async fn unpin(&self, cid: &str) -> AppResult<()>;
}
