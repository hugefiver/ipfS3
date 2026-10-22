use super::*;
use serde::de::DeserializeOwned;

pub(super) fn resident(header: &ArchiveHeader) -> Result<usize> {
    // The public storage API bounds serialized headers to 1 MiB. Bound the
    // typed vectors too, before retaining them alongside codec state.
    if header.catalog.len() > 256
        || header.acknowledgements.len() > crate::pinning::management::MAX_ITEMS
        || header.request.len() > super::super::FRAME_BYTES
        || header.configuration.len() > super::super::FRAME_BYTES
    {
        return Err(SafeError::LimitExceeded);
    }
    let mut lengths = [
        &header.database_scope,
        &header.database_identity,
        &header.principal,
        &header.request,
        &header.configuration,
        &header.created_at,
    ]
    .into_iter()
    .chain(header.catalog.iter())
    .chain(header.acknowledgements.iter())
    .chain(header.expires_at.iter())
    .map(String::len);
    lengths.try_fold(16 * 1024usize, |sum, n| {
        sum.checked_add(n).ok_or(SafeError::LimitExceeded)
    })
}
fn equal<T: DeserializeOwned + PartialEq>(raw: &str, value: &T, budget: &mut Budget) -> Result<()> {
    if raw.len() > super::super::FRAME_BYTES {
        return Err(SafeError::LimitExceeded);
    }
    budget.charge(0, 0, 1, raw.len() as u64).map_err(storage)?;
    let _resident = budget
        .reserve(5 * super::super::FRAME_BYTES)
        .map_err(storage)?;
    super::super::strict::check(raw.as_bytes())?;
    let original: T = serde_json::from_str(raw).map_err(|_| SafeError::InvalidInput)?;
    if original != *value {
        return Err(SafeError::IdempotencyConflict);
    }
    Ok(())
}
pub(super) fn check(header: &ArchiveHeader, frame: &Frame, budget: &mut Budget) -> Result<()> {
    match frame {
        Frame::Header {
            database_scope,
            database_identity,
            principal,
            catalog,
            created_at,
            expires_at,
            ..
        } => {
            if database_scope != &header.database_scope
                || database_identity != &header.database_identity
                || principal != &header.principal
                || catalog != &header.catalog
                || created_at != &header.created_at
                || expires_at != &header.expires_at
            {
                return Err(SafeError::IdempotencyConflict);
            }
        }
        Frame::Request { request } => equal(&header.request, request, budget)?,
        Frame::Configuration { configuration } => {
            equal(&header.configuration, configuration, budget)?
        }
        Frame::Acknowledgements { item_ids } => {
            if item_ids.as_deref().unwrap_or_default() != header.acknowledgements.as_slice() {
                return Err(SafeError::ConfirmationMismatch);
            }
        }
        _ => {}
    }
    Ok(())
}
