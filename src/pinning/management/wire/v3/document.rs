//! Bounded public header/decision claims beside a frozen, still untrusted spool.
use super::*;
use crate::{pinning::management::SafeError, store::pinning::management::native::meter::Budget};
use std::io::{Read, Seek, SeekFrom};

pub struct DocumentHead {
    _resident: Vec<crate::store::pinning::management::native::meter::ResidentGuard>,
    pub database_scope: String,
    pub database_identity: String,
    pub principal: String,
    pub catalog: Vec<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub request: Option<PlanRequest>,
    pub configuration: Option<ConfigurationBinding>,
    pub acknowledgements: Option<Vec<String>>,
    pub items: Vec<ItemFact>,
    pub summary: Summary,
    pub applied_claims: bool,
}
/// No native rows, cells, memberships or inventories are collected here.
pub fn inspect<S: Read + Seek>(
    mut source: S,
    mode: Projection,
    budget: Budget,
) -> Result<(DocumentHead, S, Budget), SafeError> {
    source
        .seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::InvalidInput)?;
    let mut reader = Reader::new(source, mode, budget)?;
    let Some(Event::Frame(Frame::Header {
        database_scope,
        database_identity,
        principal,
        catalog,
        created_at,
        expires_at,
        ..
    })) = reader.next_frame()?
    else {
        return Err(SafeError::InvalidInput);
    };
    let header_cost = reader.last_frame_resident() + 8192;
    let resident = reader
        .budget_mut()
        .reserve(header_cost)
        .map_err(|_| SafeError::LimitExceeded)?;
    let mut head = DocumentHead {
        _resident: vec![resident],
        database_scope,
        database_identity,
        principal,
        catalog,
        created_at,
        expires_at,
        request: None,
        configuration: None,
        acknowledgements: None,
        items: Vec::new(),
        summary: Default::default(),
        applied_claims: false,
    };
    while let Some(event) = reader.next_frame()? {
        if matches!(
            &event,
            Event::Frame(
                Frame::Request { .. }
                    | Frame::Configuration { .. }
                    | Frame::Acknowledgements { .. }
                    | Frame::Item { .. }
            )
        ) {
            let cost = reader.last_frame_resident();
            head._resident.push(
                reader
                    .budget_mut()
                    .reserve(cost)
                    .map_err(|_| SafeError::LimitExceeded)?,
            );
        }
        match event {
            Event::Frame(Frame::Request { request }) => head.request = request,
            Event::Frame(Frame::Configuration { configuration }) => {
                head.configuration = configuration
            }
            Event::Frame(Frame::Acknowledgements { item_ids }) => head.acknowledgements = item_ids,
            Event::Frame(Frame::Item { entry }) => {
                if head
                    .request
                    .as_ref()
                    .is_none_or(|r| !r.items.iter().any(|item| item == &entry.request))
                {
                    return Err(SafeError::InvalidInput);
                }
                head.items.push(*entry);
            }
            Event::Frame(Frame::Summary { summary }) => {
                if head
                    .request
                    .as_ref()
                    .is_some_and(|r| r.items.len() != head.items.len())
                {
                    return Err(SafeError::InvalidInput);
                }
                head.summary = summary;
            }
            Event::Frame(Frame::Row {
                section: Section::AppliedClaim,
                ..
            }) => head.applied_claims = true,
            _ => {}
        }
    }
    let (mut source, budget) = reader.into_verified()?;
    source
        .seek(SeekFrom::Start(0))
        .map_err(|_| SafeError::InvalidInput)?;
    Ok((head, source, budget))
}
