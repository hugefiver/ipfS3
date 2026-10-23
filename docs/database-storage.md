# Hybrid database storage

Management authority remains in the relational source catalog. Archive payloads
are immutable storage, not independent authorization sources. Their revisions
participate in history-export checkpoints; writing an archive does not recursively
capture its own payload as a new management source.

## Storage and integrity

Each plan's approved anchor item owns the frozen proposal and confirmed ACK
document. Other items retain their exact identities, creation times, expiry,
results, and native-before locator, with an `S7R2` integrity reference to the
anchor's exact confirmed bytes. Membership validation resolves the anchor and
checks every item; a reference alone never supplies confirmation authority.
Existing full per-item bindings remain readable without rewriting their original
text. Result publication compares the selected item's original binding in the
same transaction as the business mutation and receipt.

The anchor is the plan-level aggregate: a second plan table is not required to
share the approved proposal and acknowledgements. Only the proposal timestamp is
shared. An item's original creation time and expiry are never inferred from its
siblings or recomputed from a shared timestamp.

`pin_management_inline_archive` stores complete documents whose serialized body is
at most 64 KiB and whose header plus body fits a 1 MiB read page. Its `S75I2` digest
binds the format, inline representation, exact header bytes, and body length and
bytes. The immutable row is published in the caller's transaction. Small-document
reads and writes each use one SQL statement and do not fetch chunk rows.

Larger bodies use `pin_management_archive` and
`pin_management_archive_chunks`. `S75A1` retains its exact segmented format and
integrity algorithm, including chunk ordinals and lengths. Incremental staging
also uses this representation. The 64 KiB inline threshold is not a rejection
limit for existing or new large archives. A segmented reader must verify the
trailer before its sink is published.

Both representations reserve `archive_id` through the primary key of
`pin_management_archive_identity` in their insert transaction, including unsealed
staging roots. PostgreSQL's unique-key conflict serializes competing inserts even
under repeatable-read snapshots. This immutable identity index is rebuilt from
root IDs during isolated upgrade; it is not an authority or history payload source.
Private v3 loading verifies either representation, its header/body binding, and
the complete v3 stream before exposing a cursor.

Bodies are bytes, not normalized database JSON. Original whitespace, duplicate
JSON keys, binary data, SQL-cell null markers, and original timestamp text remain
recoverable. Headers and payloads are length-bounded in SQL before materialization.
Private history export includes both archive representations and restores their
exact bytes without applying them to a live database.

## Initialization and isolated upgrade

The registered migration `m20261023_000001_inline_documents` contains the SQLite
and PostgreSQL DDL. It adds inline storage and its immutable/revision guards, and
moves frozen import input into `import_job_requests`. It does not rewrite or
delete existing archive roots, chunks, or historical records.
No destructive downgrade is provided.

Ordinary startup initializes an empty database. It performs only read-only schema
checks for a nonempty database and rejects an old or incomplete schema with
upgrade-required. Operators must quiesce writers and workers and retain a separate
backup before invoking the explicit local upgrade entry.

`store::initialization::upgrade_existing` accepts the exact source versions
`m20261013_000001_zip_root_logical_release` and
`m20261017_000001_management_storage`, the current target version, and the original
database scope and native identity. Import layout conversion runs in the isolated
schema transaction: SQL projects at most 256 bounded IDs per page, then copies
their cold cells within the database without driver materialization or JSON
normalization. The source columns are removed only after all copies succeed;
failure rolls the layout and cells back together. SQLite column removal rewrites
the source table and requires maintenance time and free disk space. Management
history conversion uses bounded source reads and per-original transactions.
The identity-bound upgrade stamp is target-specific;
retry resumes the same upgrade without replacing prior upgrade originals.
Readiness is published only after source/locator coverage and revisions pass the
existing final fence. Incomplete conversion leaves the database unavailable to
ordinary startup.

Historical committed results remain recoverable under their original identities.
Recorded recovery retains the verified original anchor and core identity and reads
current and historical provider captures as their respective strict DTOs. It never
constructs a replacement execution capture. Conversion indexes use a versioned
namespace, leaving earlier indexes and original archive references intact.
Converted unexecuted plans retain their original review material, not renewed
execution authority: new execution requires a new plan ID. Upgrade does not renew
TTL, alter item creation times, re-execute committed business work, or automatically
delete originals or backups. Production upgrade requires separate authorization.

The existing CLI entry is `--pinning-manage-upgrade` with `--database-url`,
`--native-identity`, `--database-scope`, `--source-version`, and `--target-version`.
It opens an existing database without ordinary gateway initialization or provider
RPCs. Use the preserved original identity and a supported source version; the
target is `m20261023_000001_inline_documents`. Keep the operator's independent
backup until restore has been verified. No migration deletes that backup.

PostgreSQL preparation has an unresolved migration-lock timeout, error-redaction,
and transaction-cancellation safety limitation. A timeout or cancelled caller is
not proof of rollback or of a connection being safe to reuse; a commit whose
acknowledgement was lost has an unknown outcome. PostgreSQL preparation is not
accepted for production use until these guarantees are verified. Retain the
independent backup and establish the database outcome before retrying a failed
or interrupted upgrade.

## Frozen intake and import hot state

New ZIP imports and multipart intakes retain the frozen source decision in their
ZIP capture rather than serializing a second decision document. ZIP imports also
retain the root option in that capture. Ordinary and historical uncaptured
intakes keep their explicit standalone variants. Readers reject contradictory
historical duplicate fields; they never infer executable intent from tags or
current configuration. Multipart parts remain independent relational rows.

`import_jobs` contains scheduling, claim epochs, progress, terminal results,
destination identity, idempotency keys, and original timestamps. Its one-to-one
`import_job_requests` row owns the source value, content type, metadata, tags,
and captured pin/ZIP inputs. Submission writes both within the ownership
transaction; expiry deletes the request through the existing job lifetime.
Request revisions participate in current native snapshots. Private management
history exports carry cold requests only when frozen inside original management
reviews; they do not enumerate the live request table. The live cold table is
neither a management history source nor a second management action authority.

This request aggregate keeps original SQL text and nullable cells rather than
wrapping them in another JSON serialization. There is no internal JSON predicate
to justify JSONB or a second normalized body. Its layout is bound by the registered
schema catalog; historical catalogs retain the predecessor's exact column shape.
New ZIP captures still use their existing typed, versioned domain document.

Claim selection projects only bounded job IDs and source types, ordered by due
time, creation time, and ID. A claim page contains at most 256 candidates. CAS
updates acquire jobs in ascending ID order; returned jobs retain due-time order.
Only successful claims are loaded in full. SQL byte-length projections select
small joined rows into pages of at most 256 rows and 1 MiB. Larger requests are
read as 64 KiB byte slices, batched across up to 15 jobs per payload page, rather
than issuing a new full-request read per job. UTF-8 is decoded after reassembly,
so a slice can cross a code point without changing the original text. A request
larger than 1 MiB is not rejected merely for crossing a page boundary. Resident
assembly remains bounded; pathological requests can exhaust that budget without
publishing partial claims.

Renewal and progress updates retain their existing claim-epoch fences and never
load or rewrite request bodies. Status has a separate projection: it reads hot
counters and only the frozen capture fields needed for response headers and ZIP
status, never source descriptors, object metadata or tags. SQL bounds every
selected status cell before decoding; larger captures use the bounded cell reader
inside the same read transaction. Execution uses the complete joined projection.
Ownership and publication fences select only hot claim state. Bucket supersession
pages IDs with SQL byte bounds; extraction separately reads only its frozen prefix.
New intake writes go through the import store, not arbitrary full-model updates.
