use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestOperation {
    Configure,
    Version,
    Allocations,
    GlobalPins,
    Verify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerificationCode {
    MissingRestConfiguration,
    InvalidConfiguration,
    InvalidCid,
    UnsupportedVersion,
    Authentication,
    UnknownForbidden,
    NotFound,
    RateLimited,
    HttpStatus,
    RedirectRejected,
    Transport,
    TotalTimeout,
    IdleTimeout,
    MalformedResponse,
    ResponseTooLarge,
    CidMismatch,
    AllocationChanged,
    StaleObservation,
}

/// Contains only bounded enums and a numeric status, never a URL, body or error source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerificationFailure {
    pub operation: RestOperation,
    pub code: VerificationCode,
    pub http_status: Option<u16>,
}

impl VerificationFailure {
    pub(super) fn new(operation: RestOperation, code: VerificationCode) -> Self {
        Self {
            operation,
            code,
            http_status: None,
        }
    }
}

impl fmt::Display for VerificationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cluster REST verification: {:?}/{:?}",
            self.operation, self.code
        )?;
        if let Some(status) = self.http_status {
            write!(f, " (HTTP {status})")?;
        }
        Ok(())
    }
}

impl std::error::Error for VerificationFailure {}
