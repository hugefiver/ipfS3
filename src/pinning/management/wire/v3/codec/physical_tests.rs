use super::*;
use crate::pinning::management::wire::v3::codec::physical::{CheckedRecord, Decoder, Sink};

struct Copy<W>(Writer<W>);
impl<W: Write> Sink for Copy<W> {
    fn budget(&mut self) -> &mut Budget { self.0.budget_mut() }
    fn record(&mut self, record: CheckedRecord<'_>) -> Result<()> {
        if record.terminal() { Ok(()) } else { self.0.checked_record(&record) }
    }
}
fn stream(bytes: &[u8], page: usize) -> Result<Vec<u8>> {
    let mut sink = Copy(Writer::new(Vec::new(), Projection::PrivateRecovery, files::stream::budget())?);
    let mut decoder = Decoder::new(Projection::PrivateRecovery, sink.budget())?;
    for page in bytes.chunks(page) { decoder.feed(page, &mut sink)?; }
    decoder.finish(sink.0.budget())?;
    sink.0.finish()
}
fn old_encoding(canonical: &[u8]) -> Vec<u8> {
    let mut output = MAGIC.to_vec();
    let mut at = MAGIC.len();
    let mut frames = 0;
    loop {
        let n = u32::from_be_bytes(canonical[at..at+4].try_into().unwrap()) as usize;
        let kind = canonical[at+4];
        let payload = &canonical[at+5..at+4+n];
        if kind == 0 {
            let frame: Frame = serde_json::from_slice(payload).unwrap();
            if matches!(frame, Frame::Trailer { .. }) { break; }
            // Sorted fields, whitespace and alternate escapes are valid v3 input.
            let value = serde_json::to_value(&frame).unwrap();
            let text = serde_json::to_string_pretty(&value).unwrap().replace("scope", "sc\\u006fpe");
            record(&mut output, kind, text.as_bytes());
        } else { record(&mut output, kind, payload); }
        frames += 1; at += n+4;
    }
    let trailer = Frame::Trailer { frames, bytes: output.len() as u64, sha256: hex::encode(Sha256::digest(&output)) };
    record(&mut output, 0, &serde_json::to_vec(&trailer).unwrap()); output
}

#[test]
fn checked_stream_replays_current_writer_bytes_even_for_old_json() {
    for compact in [true, false] {
        let canonical = archive(compact);
        let old = old_encoding(&canonical);
        assert_ne!(old, canonical);
        assert_eq!(logical(&old, Projection::PrivateRecovery).unwrap(), logical(&canonical, Projection::PrivateRecovery).unwrap());
        for page in [1, 13, 65536] {
            assert_eq!(stream(&old, page).unwrap(), canonical);
            assert_eq!(stream(&canonical, page).unwrap(), canonical);
        }
    }
}

#[test]
fn checked_stream_requires_hash_terminal_eof_and_retains_failure_work() {
    let canonical = archive(true);
    let mut extra = canonical.clone(); extra.push(0);
    let mut corrupt = canonical.clone();
    let at = corrupt.windows(3).position(|s| s == b"cid").unwrap();
    corrupt[at] = b'b';
    for bad in [&extra[..], &corrupt[..], &canonical[..canonical.len()-1]] {
        assert!(stream(bad, 19).is_err());
    }
    let mut sink = Copy(Writer::new(Vec::new(), Projection::PrivateRecovery, files::stream::budget()).unwrap());
    let mut decoder = Decoder::new(Projection::PrivateRecovery, sink.budget()).unwrap();
    decoder.feed(&canonical[..MAGIC.len()+2], &mut sink).unwrap();
    let before = sink.0.budget().work;
    sink.0.budget().cancellation().store(true, Ordering::Relaxed);
    assert!(decoder.feed(&canonical[MAGIC.len()+2..], &mut sink).is_err());
    assert!(decoder.finish(sink.0.budget()).is_err());
    assert_eq!(sink.0.budget().work.output_bytes, before.output_bytes);
    assert!(sink.0.budget().work.copied_bytes >= 2);
}
