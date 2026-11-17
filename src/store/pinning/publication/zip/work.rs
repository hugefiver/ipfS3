use super::*;
use crate::pinning::zip_policy::select_zip_intents;
use crate::store::pinning::management::{
    admission::work_error,
    records::{DecodeWork, ReadSession, SharedDecodeBudget},
};

pub(super) fn begin_attempt(session: &SharedDecodeBudget) -> AppResult<()> {
    session
        .lock()
        .map_err(|_| invalid())?
        .begin_publication_attempt()
        .map_err(work_error)
}

pub(super) fn select(
    capture: &CapturedZipIntent,
    manifest: &FinalZipManifest,
    source: Option<&ZipBoundOutput>,
    successful: &[ZipBoundOutput],
    work: DecodeWork<'_>,
) -> AppResult<crate::pinning::zip_policy::ZipIntentSelection> {
    work.with(|session| session.check()).map_err(work_error)?;
    let selected =
        select_zip_intents(capture, manifest, source, successful).map_err(|_| invalid())?;
    work.with(|session| session.check()).map_err(work_error)?;
    Ok(selected)
}

pub(super) fn session() -> SharedDecodeBudget {
    std::sync::Arc::new(std::sync::Mutex::new(ReadSession::for_publication()))
}

#[cfg(test)]
#[path = "work_tests.rs"]
mod tests;
