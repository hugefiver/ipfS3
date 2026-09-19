use async_zip::base::read::{
    WithEntry,
    stream::{Reading, Ready, ZipFileReader},
};

use super::local_header::LocalHeaderProbe;

/// Finish framing even on a checksum mismatch, so later independent entries can
/// still succeed. Framing/I/O errors remain terminal for this archive's scan.
pub(super) async fn finish_entry<'a, R>(
    mut reader: ZipFileReader<Reading<'a, R, WithEntry<'a>>>,
    probe: &LocalHeaderProbe,
    descriptor: bool,
    compressed_start: u64,
    size: u64,
) -> async_zip::error::Result<(ZipFileReader<Ready<R>>, bool)>
where
    R: futures_io::AsyncBufRead + Unpin + 'a,
{
    let compressed = probe.position() - compressed_start;
    let crc = reader.reader_mut().compute_hash();
    let entry = reader.reader().entry();
    let valid = descriptor
        || (entry.crc32() == crc
            && entry.uncompressed_size() == size
            && entry.compressed_size() == compressed);
    if descriptor {
        probe.begin_descriptor();
    }
    let ready = reader.done().await?;
    let valid = valid && (!descriptor || probe.descriptor_matches(crc, compressed, size));
    Ok((ready, valid))
}
