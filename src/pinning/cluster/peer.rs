use super::{RestOperation, VerificationCode, VerificationFailure};
use std::collections::BTreeSet;

/// Cluster api/types.go uses libp2p peer.ID. Collapse base58 multihash and
/// CIDv1 libp2p-key aliases to that ID, rather than counting text representations.
pub(super) fn canonical(
    value: &str,
    operation: RestOperation,
) -> Result<String, VerificationFailure> {
    let invalid = || VerificationFailure::new(operation, VerificationCode::MalformedResponse);
    if value.is_empty() || value.len() > 128 || !value.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(invalid());
    }
    let hash = match ::cid::Cid::try_from(value) {
        Ok(cid) if cid.version() == ::cid::Version::V0 || cid.codec() == 0x72 => *cid.hash(),
        _ => {
            let (_, bytes) =
                ::cid::multibase::decode(format!("z{value}")).map_err(|_| invalid())?;
            ::cid::multihash::Multihash::<64>::from_bytes(&bytes).map_err(|_| invalid())?
        }
    };
    // The libp2p peer ID forms are SHA-256 or an inline public-key identity hash.
    if !((hash.code() == 0x12 && hash.size() == 32)
        || (hash.code() == 0 && (1..=42).contains(&hash.size())))
    {
        return Err(invalid());
    }
    let encoded = ::cid::multibase::encode(::cid::multibase::Base::Base58Btc, hash.to_bytes());
    Ok(encoded[1..].to_owned())
}

pub(super) fn set(
    peers: Vec<String>,
    operation: RestOperation,
) -> Result<BTreeSet<String>, VerificationFailure> {
    peers
        .into_iter()
        .map(|id| canonical(&id, operation))
        .collect()
}
