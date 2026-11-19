use super::{
    RpcProfile,
    error::{error, protocol},
};
use crate::pinning::provider::{ProviderError, ProviderErrorClass};
use ::cid::{Cid, Version, multibase};

pub(super) fn parse(value: &str) -> Result<Cid, ProviderError> {
    parse_bare(value).ok_or_else(|| error(ProviderErrorClass::InvalidInput, "invalid RPC CID"))
}

fn parse_bare(value: &str) -> Option<Cid> {
    if value.starts_with("Qm") {
        let cid = Cid::try_from(value).ok()?;
        return (cid.to_string() == value).then_some(cid);
    }
    // Cid::try_from(str) also accepts /ipfs/ paths and ignores trailing bytes.
    let (_, bytes) = multibase::decode(value).ok()?;
    let mut bytes = bytes.as_slice();
    let cid = Cid::read_bytes(&mut bytes).ok()?;
    (bytes.is_empty() && cid.version() == Version::V1).then_some(cid)
}

pub(super) fn canonical(cid: &Cid) -> Cid {
    Cid::new_v1(cid.codec(), *cid.hash())
}

pub(super) fn parse_ack(value: &str) -> Result<Cid, ProviderError> {
    parse_bare(value).ok_or_else(|| protocol("invalid RPC response CID"))
}

pub(super) fn equivalent(left: &Cid, right: &Cid) -> bool {
    left.codec() == right.codec() && left.hash() == right.hash()
}

pub(super) fn decode(profile: RpcProfile, id: &str) -> Result<Cid, ProviderError> {
    let prefix = format!("rpc-pin:{}:", profile.id());
    let value = id.strip_prefix(&prefix).ok_or_else(|| {
        error(
            ProviderErrorClass::InvalidInput,
            "invalid RPC pin reference",
        )
    })?;
    let cid = parse(value)?;
    if value != cid.to_string() {
        return Err(error(
            ProviderErrorClass::InvalidInput,
            "noncanonical RPC pin reference",
        ));
    }
    Ok(cid)
}
