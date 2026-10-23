//! Strict original DTO variants. Neither variant is a new execution binding.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum RecordedEntry {
    Current(management::PlanEntry),
    Historical(legacy::PlanEntry),
}

#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum Captured<'a> {
    Current(&'a management::Capture),
    Historical(&'a legacy::Capture),
}

impl RecordedEntry {
    pub(crate) fn request(&self) -> &management::ItemRequest {
        match self {
            Self::Current(e) => &e.request,
            Self::Historical(e) => &e.request,
        }
    }
    pub(crate) fn id(&self) -> &management::IdempotencyId {
        match self {
            Self::Current(e) => &e.id,
            Self::Historical(e) => &e.id,
        }
    }
    pub(crate) fn status(&self) -> management::PlanStatus {
        match self {
            Self::Current(e) => e.status,
            Self::Historical(e) => e.status,
        }
    }
    pub(crate) fn step(&self) -> &Option<management::PlannedStep> {
        match self {
            Self::Current(e) => &e.step,
            Self::Historical(e) => &e.step,
        }
    }
    pub(crate) fn capture(&self) -> Option<Captured<'_>> {
        match self {
            Self::Current(e) => e.capture.as_ref().map(Captured::Current),
            Self::Historical(e) => e.capture.as_ref().map(Captured::Historical),
        }
    }
}
