use crate::pinning::provider::{ProviderError, ProviderErrorClass};

pub(super) fn error(class: ProviderErrorClass, message: &'static str) -> ProviderError {
    ProviderError {
        class,
        message: message.into(),
        retry_after: None,
    }
}

pub(super) fn protocol(message: &'static str) -> ProviderError {
    error(ProviderErrorClass::Protocol, message)
}

pub(super) fn mismatch() -> ProviderError {
    protocol("RPC returned a root CID different from the requested object")
}
