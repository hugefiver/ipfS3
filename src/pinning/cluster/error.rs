#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterFailureCode {
    InvalidConfiguration,
    InvalidCid,
    Protocol,
    Transport,
    IdleTimeout,
    ControlTimeout,
    Cancelled,
    HttpStatus,
    RedirectRejected,
    CidMismatch,
    InvalidCar,
    DirectoryRequiresCar,
    SourceFailure,
    IncompleteUpload,
    ResourceLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Cluster write failed: {code:?} (HTTP {http_status:?})")]
pub struct ClusterWriteFailure {
    pub code: ClusterFailureCode,
    pub http_status: Option<u16>,
}

impl ClusterWriteFailure {
    pub(super) fn new(code: ClusterFailureCode) -> Self {
        Self {
            code,
            http_status: None,
        }
    }
}
