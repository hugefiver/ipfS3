use super::*;
use super::super::codec::{owned, physical::{CheckedRecord, Decoder, Sink, Value}};
use crate::store::pinning::management::records::{self, ConfirmedBinding};
use crate::pinning::management::PlanEntry;

struct Original {
    builder: owned::Builder,
    budget: Budget,
    document: records::OriginalInventoryDocument,
}
impl Sink for Original {
    fn budget(&mut self) -> &mut Budget { &mut self.budget }
    fn record(&mut self, record: CheckedRecord<'_>) -> Result<()> {
        if let Value::Frame(frame) = &record.value {
            self.document.check_frame(frame, &mut self.budget).map_err(storage)?;
        }
        self.builder.record(&record, &mut self.budget)
    }
}

pub(crate) async fn load<C: ConnectionTrait>(
    db: &C, binding: &ConfirmedBinding, entry: &PlanEntry, mut budget: Budget,
) -> Result<ReviewCursor<OwnedSource>> {
    let (document, mut pages) = records::open_original_inventory_document(db, binding, entry, &mut budget, false).await.map_err(storage)?;
    let reference = document.reference().clone();
    let builder = owned::Builder::new(&mut budget)?;
    let mut decoder = Decoder::new(Projection::PrivateRecovery, &mut budget)?;
    let mut original = Original { builder, budget, document };
    while let Some(page) = pages.next(db, &mut original.budget).await.map_err(storage)? {
        for chunk in &page.chunks {
            original.builder.write_raw(chunk, &mut original.budget)?;
            decoder.feed(chunk, &mut original)?;
        }
    }
    decoder.finish(&original.budget)?;
    drop(decoder);
    drop(pages);
    drop(original.document);
    let reader = original.builder.finish(original.budget)?;
    Ok(ReviewCursor {
        reader, source_projection: Projection::PrivateRecovery,
        projection: projection::ProjectionState::new(Projection::PrivateRecovery),
        reference_claim: Some(reference), started: false,
    })
}
