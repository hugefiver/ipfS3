# Streaming ZIP safety boundary

Both HTTP decompression and ZIP import call the shared extractor. Its defaults
are 8 GiB decompressed bytes, 10,000 local entries and 64 MiB of metadata
reservation units per archive. The existing custom byte limit does not disable
the entry/metadata limits.

Each local header reserves **4096 + 8 × (raw filename bytes + local extra-field
bytes + target-prefix bytes)** before async_zip receives the completed fixed
header and can allocate variable fields. Directories, empty files, upload
failures and subsequently rejected entries all pay; reservations are never
refunded. Fixed overhead covers result/error records; variable overhead covers
raw/Unicode names, keys and copies retained by the parser and import observer.
Failure messages are capped at 512 UTF-8 bytes. This is conservative accounting,
not an exact allocator/RSS guarantee. Individual variable ZIP fields also have
their format's 65535-byte bound; the central directory is not accumulated.

Compatibility cost: archives with more than 10,000 entries or many long names /
large extra fields can now fail even with very little decompressed content.
Final prefix-plus-entry keys over 1024 UTF-8 bytes are rejected before upload or
observer admission. Existing traversal rules and caller prefix normalization
are unchanged. Metadata/count exhaustion is an archive-level `Limit`, like
the decompressed-byte limit; callers already map it to their limit failure.

CRC32 is computed incrementally by async_zip's entry reader, and uncompressed
bytes by the shared copy budget. Compressed size uses bytes actually consumed
by the decoder, not buffered/read-ahead bytes. Header CRC and sizes, or signed /
unsigned 32-bit Deflate descriptor CRC and sizes, must match before any
`entry_finished` callback or successful result. Mismatches with intact framing
produce `EntryReadFailed` and scanning continues; framing/I/O failures stop the
scan with existing per-entry failure semantics. This does not introduce a
whole-archive atomic transaction.

Stored without descriptor and Deflate remain supported. Stored + descriptor
remains rejected (no self-delimiting stream); ZIP64 descriptors are not newly
supported by async_zip 0.0.18. Kubo upload/pinning may precede integrity checking,
so corrupt staged content can remain locally pinned, but cannot be published
as an object. No pin removal is attempted because CIDs can be shared. Payloads
continue through the 64 KiB copy/duplex buffers, never whole-entry collection.

The implementation uses async_zip 0.0.18 `ZipEntryReader::compute_hash()` and
observes the descriptor consumed by `ZipFileReader::done()` (which itself does
not validate integrity). No additional CRC dependency is necessary.
