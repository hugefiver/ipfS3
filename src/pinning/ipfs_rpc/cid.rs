use super::{RpcProfile, error::error};
use crate::pinning::provider::{ProviderError, ProviderErrorClass};

pub(super) fn parse(value: &str) -> Result<::cid::Cid, ProviderError> {
    ::cid::Cid::try_from(value)
        .map_err(|_| error(ProviderErrorClass::InvalidInput, "invalid RPC CID"))
}

pub(super) fn canonical(value: &str) -> Result<String, ProviderError> {
    crate::pinning::provider::canonical_cid(value)
}

pub(super) fn equivalent(left: &str, right: &str) -> Result<bool, ProviderError> {
    crate::pinning::provider::cids_equivalent(left, right)
}

pub(super) fn request_id(profile: RpcProfile, cid: &str) -> Result<String, ProviderError> {
    Ok(format!("rpc-pin:{}:{}", profile.id(), canonical(cid)?))
}

pub(super) fn decode(profile: RpcProfile, id: &str) -> Result<String, ProviderError> {
    let prefix = format!("rpc-pin:{}:", profile.id());
    let cid = id.strip_prefix(&prefix).ok_or_else(|| {
        error(
            ProviderErrorClass::InvalidInput,
            "invalid RPC pin reference",
        )
    })?;
    let canonical = canonical(cid)?;
    if cid != canonical {
        return Err(error(
            ProviderErrorClass::InvalidInput,
            "noncanonical RPC pin reference",
        ));
    }
    Ok(canonical)
}
