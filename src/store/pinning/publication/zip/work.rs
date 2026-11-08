use super::*;
use crate::pinning::zip_policy::select_zip_intents;
use crate::store::pinning::management::{
    admission::work_error,
    native::meter::{ResidentGuard, WorkMeter},
    records::{DecodeWork, ReadSession, SharedDecodeBudget},
};

pub(super) fn begin_attempt(session: &SharedDecodeBudget) -> AppResult<()> {
    session
        .lock()
        .map_err(|_| invalid())?
        .begin_publication_attempt()
        .map_err(work_error)
}

pub(super) fn candidate_bytes(request: &CapturedZipPublication) -> AppResult<usize> {
    request
        .source
        .iter()
        .chain(&request.successes)
        .try_fold(0usize, |total, object| {
            total
                .checked_add(object.id.len())
                .and_then(|n| n.checked_add(object.id.len()))
                .and_then(|n| n.checked_add(object.bucket.len()))
                .and_then(|n| n.checked_add(object.key.len()))
                .and_then(|n| n.checked_add(object.cid.len()))
                .and_then(|n| n.checked_add(512))
                .ok_or(AppError::ManagementBudget)
        })
}

pub(super) fn select(
    capture: &CapturedZipIntent,
    manifest: &FinalZipManifest,
    source: Option<&ZipBoundOutput>,
    successful: &[ZipBoundOutput],
    work: DecodeWork<'_>,
) -> AppResult<(
    crate::pinning::zip_policy::ZipIntentSelection,
    ResidentGuard,
)> {
    let bytes = work
        .with(|session| {
            session.publication_preflight(|session| {
                let bounds = allocation::CaptureBounds::measure(capture, session)?;
                bounds.selection_bytes(capture, source, successful, session)
            })
        })
        .map_err(work_error)?;
    let resident = sql::retain_copy(work, bytes)?;
    let selected =
        select_zip_intents(capture, manifest, source, successful).map_err(|_| invalid())?;
    work.with(|session| session.check()).map_err(work_error)?;
    Ok((selected, resident))
}

pub(super) fn session(
    request: &CapturedZipPublication,
    selection: &CapturedZipIntent,
) -> AppResult<SharedDecodeBudget> {
    // Start the deadline before validation or shape scanning. Only tighten this session.
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        ReadSession::for_zip_publication(65_536).map_err(work_error)?,
    ));
    let work = DecodeWork::shared(&session);
    let bounds = work
        .with(|session| allocation::CaptureBounds::measure(&request.capture, session))
        .map_err(work_error)?;
    let tags = work
        .with(|session| session.measure(&request.source_tags))
        .map_err(work_error)?;
    let tags = tags
        .0
        .checked_mul(32)
        .and_then(|n| n.checked_add(tags.1))
        .and_then(|n| n.checked_mul(2))
        .ok_or(AppError::ManagementBudget)?;
    let validation_bytes = bounds
        .capture_bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(request.terminal_result.len().checked_mul(32)?))
        .and_then(|n| n.checked_add(request.successes.len().checked_mul(512)?))
        .and_then(|n| n.checked_add(tags))
        .ok_or(AppError::ManagementBudget)?;
    let validation_resident = sql::retain_copy(work, validation_bytes)?;
    validate(request)?;
    drop(validation_resident);
    let selection_bounds = work
        .with(|session| allocation::CaptureBounds::measure(selection, session))
        .map_err(work_error)?;
    let shape =
        selection_bounds.shape(selection, request.successes.len(), request.source.is_some())?;
    work.with(|session| session.zip_publication_shape(shape))
        .map_err(work_error)?;
    sql::charge(
        work,
        WorkMeter {
            raw_bytes: u64::try_from(bounds.capture_bytes)
                .map_err(|_| AppError::ManagementBudget)?,
            ..Default::default()
        },
    )?;
    Ok(session)
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;
