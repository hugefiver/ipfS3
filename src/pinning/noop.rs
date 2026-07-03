#![allow(dead_code)]

pub struct NoopPinningService;

impl NoopPinningService {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopPinningService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl super::PinningService for NoopPinningService {
    async fn pin(&self, _cid: &str, _name: Option<&str>) -> crate::error::AppResult<()> {
        // Noop: relies on local Kubo pin only.
        Ok(())
    }

    async fn unpin(&self, _cid: &str) -> crate::error::AppResult<()> {
        Ok(())
    }
}
