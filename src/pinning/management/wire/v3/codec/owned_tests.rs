use super::*;
use crate::pinning::management::wire::v3::codec::{owned, physical::{CheckedRecord, Decoder, Sink}};

struct Build { builder: owned::Builder, budget: Budget }
impl Sink for Build {
    fn budget(&mut self) -> &mut Budget { &mut self.budget }
    fn record(&mut self, record: CheckedRecord<'_>) -> Result<()> { self.builder.record(&record, &mut self.budget) }
}
fn owned(bytes: &[u8]) -> Result<Reader<owned::OwnedSource>> {
    let mut budget = files::stream::budget();
    let builder = owned::Builder::new(&mut budget)?;
    let mut decoder = Decoder::new(Projection::PrivateRecovery, &mut budget)?;
    let mut sink = Build { builder, budget };
    for chunk in bytes.chunks(31) {
        sink.builder.write_raw(chunk, &mut sink.budget)?;
        decoder.feed(chunk, &mut sink)?;
    }
    decoder.finish(&sink.budget)?;
    drop(decoder);
    sink.builder.finish(sink.budget)
}

#[test]
fn owned_index_traverses_every_event_and_requires_terminal_consumption() {
    for compact in [true, false] {
        let bytes = archive(compact);
        let mut reader = owned(&bytes).unwrap();
        assert!(!reader.verified());
        let mut events = Vec::new();
        while let Some(event) = reader.next_frame().unwrap() {
            assert!(!reader.verified());
            events.push(match event {
                Event::Frame(frame) => serde_json::to_vec(&frame).unwrap(),
                Event::Chunk(mut bytes) => { bytes.insert(0, 0xff); bytes }
            });
        }
        assert!(reader.verified());
        assert_eq!(events, logical(&bytes, Projection::PrivateRecovery).unwrap());
        assert!(reader.into_verified().is_ok());
    }
}

#[test]
fn owned_index_cancellation_and_incomplete_source_never_verify() {
    let bytes = archive(true);
    assert!(owned(&bytes[..bytes.len()-1]).is_err());
    let mut reader = owned(&bytes).unwrap();
    reader.budget().cancellation().store(true, Ordering::Relaxed);
    assert!(reader.next_frame().is_err());
    assert!(!reader.verified());
    assert!(reader.into_verified().is_err());
}
