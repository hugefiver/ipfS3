//! Exact public review comparison, independent of JSON layout and chunk splitting.
use super::{archive_cursor::ReviewCursor, *};
use crate::{
    pinning::management::SafeError,
    store::pinning::management::{native::meter::Budget, records::ConfirmedBinding},
};
use std::io::{Read, Seek, SeekFrom};

type Result<T> = std::result::Result<T, SafeError>;

#[cfg(test)]
mod tests;

/// Generate only from the authenticated immutable binding, never today's rows,
/// configuration or clock. Both streams must reach their verified terminal EOF.
pub fn confirmed<S: Read + Seek>(
    mut supplied: ReviewCursor<S>,
    binding: &ConfirmedBinding,
) -> Result<Budget> {
    let spool = files::stream::temporary().map_err(|_| SafeError::InvalidInput)?;
    let mut writer = Writer::new(spool, Projection::Review, supplied.take_budget())?;
    planning::local_confirmed(&mut writer, binding, Projection::Review)?;
    let (mut spool, budget) = writer.finish_with_budget()?;
    supplied.restore_budget(budget);
    spool
        .seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::InvalidInput)?;
    public_stream(supplied, spool)
}

pub async fn confirmed_database<S: Read + Seek>(
    db: &sea_orm::DatabaseConnection,
    mut supplied: ReviewCursor<S>,
    binding: &ConfirmedBinding,
) -> Result<Budget> {
    let file = files::stream::temporary().map_err(|_| SafeError::UnknownEvidence)?;
    let mut writer = Writer::new(file, Projection::Review, supplied.take_budget())?;
    planning::confirmed(db, &mut writer, binding, Projection::Review).await?;
    let (mut file, budget) = writer.finish_with_budget()?;
    supplied.restore_budget(budget);
    file.seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::UnknownEvidence)?;
    public_stream(supplied, file)
}

pub async fn recorded_database<S: Read + Seek>(
    db: &sea_orm::DatabaseConnection,
    mut supplied: ReviewCursor<S>,
    plan: &crate::store::pinning::management::records::RecordedPlan,
) -> Result<Budget> {
    let file = files::stream::temporary().map_err(|_| SafeError::UnknownEvidence)?;
    let mut writer = Writer::new(file, Projection::Review, supplied.take_budget())?;
    planning::recorded(db, &mut writer, plan, Projection::Review).await?;
    let (mut file, budget) = writer.finish_with_budget()?;
    supplied.restore_budget(budget);
    file.seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::UnknownEvidence)?;
    public_stream(supplied, file)
}

/// Equality is not authority: callers must obtain `original` from an authenticated
/// local archive. No file claim, digest or successful comparison grants authority.
/// One cumulative budget follows both decoders; at most two events are retained.
pub fn public_stream<S: Read + Seek, R: Read>(
    supplied: ReviewCursor<S>,
    original: R,
) -> Result<Budget> {
    let mut supplied = supplied.project_to(Projection::Review)?;
    let mut original = Reader::new(original, Projection::Review, supplied.take_budget())?;
    supplied.restore_budget(original.take_budget());
    let _events = supplied
        .budget_mut()
        .reserve(10 * FRAME_BYTES + 2 * CELL_BYTES)
        .map_err(|_| SafeError::LimitExceeded)?;
    let (mut left, mut right) = (None, None);
    let (mut left_offset, mut right_offset) = (0, 0);
    loop {
        if left.is_none() {
            left = supplied.next_event()?;
            left_offset = 0;
        }
        if right.is_none() {
            original.restore_budget(supplied.take_budget());
            let event = original.next_frame();
            supplied.restore_budget(original.take_budget());
            right = event?;
            right_offset = 0;
        }
        match (&left, &right) {
            (None, None) if supplied.verified() && original.verified() => {
                return supplied.into_verified().map(|(_, budget)| budget);
            }
            (Some(Event::Frame(a)), Some(Event::Frame(b))) if a == b => {
                if matches!(
                    a,
                    Frame::Row {
                        section: Section::AppliedClaim,
                        ..
                    }
                ) {
                    return Err(SafeError::ConfirmationMismatch);
                }
                left = None;
                right = None;
            }
            (Some(Event::Chunk(a)), Some(Event::Chunk(b))) => {
                let n = (a.len() - left_offset).min(b.len() - right_offset);
                if a[left_offset..left_offset + n] != b[right_offset..right_offset + n] {
                    return Err(SafeError::ConfirmationMismatch);
                }
                left_offset += n;
                right_offset += n;
                if left_offset == a.len() {
                    left = None;
                }
                if right_offset == b.len() {
                    right = None;
                }
            }
            _ => return Err(SafeError::ConfirmationMismatch),
        }
    }
}
