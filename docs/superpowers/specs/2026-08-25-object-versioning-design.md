# Object Versioning Design

**Status:** Approved design
**Date:** 2026-08-25
**Roadmap scope:** v0.6, bucket versioning, ListObjectVersions, and delete markers

## Summary

This delivery adds the minimum complete public versioning feature: a bucket can
be unversioned, enabled, or suspended; clients can list every public version;
and a simple delete in a version-aware bucket creates a delete marker. These
are one atomic feature, not three independent roadmap boxes. Enabling or
suspending without a way to inspect retained versions leaves recovery opaque.
Listing versions without delete markers misreports the current key state.
Delete markers without version-aware reads and exact deletion would make a
deleted key impossible to explain or restore.

The gateway will add a separate public `object_versions` index while retaining
`objects` as immutable content metadata and the projection used by ordinary
object reads and listings. The index gives each newly created public version an
opaque UUID distinct from the internal object UUID. It records delete markers
without inventing content rows, and it preserves the existing pin-tag and lease
ownership model, which is keyed by the internal object ID.

## Scope and Decisions

### Considered approaches

1. **Separate `object_versions` public index, selected.** Keep immutable
   content in `objects`, add an ordered version record for each public object
   version or marker, and keep `objects.is_latest` as the ordinary-listing
   projection. This isolates S3 version semantics from IPFS and pinning
   ownership, permits a marker with no content, and gives ListObjectVersions a
   stable ordered source.
2. **Add public-version columns directly to `objects`, rejected.** This makes
   a delete marker either a fake content object or a special case scattered
   through encryption, tags, leases, and Kubo access. It also conflates the
   UUID used internally by tags and leases with a public identifier.
3. **Reuse historical `objects.is_latest = false` rows as versions, rejected.**
   Earlier overwrites deliberately retained storage rows only as an internal
   implementation detail. They have no public version ID, ordering contract,
   or marker representation. Exposing them would change historical public data
   and could expose rows that have already had their lease ended.

The selected design keeps the current S3, encryption, and pinning boundaries
intact. No public version identifier is an internal object identifier.

### Bucket states

`buckets.versioning_status` is nullable. `NULL` means **Unversioned**, not an
unknown or partially migrated state. The only stored non-null values are the
case-sensitive S3 values `Enabled` and `Suspended`. PutBucketVersioning accepts
only `Enabled` and `Suspended`; it never restores `NULL`. GetBucketVersioning
returns no `<Status>` for `NULL`, `Enabled` for enabled buckets, and `Suspended`
for suspended buckets.

| State | New content publication | Simple DELETE | Exact `versionId` operations | ListObjectVersions |
|---|---|---|---|---|
| Unversioned (`NULL`) | Preserve current overwrite behavior while replacing one hidden literal-null index row so a later first enable exposes the actual current object. | Preserve current idempotent removal behavior and remove that hidden row. | Reject with `InvalidArgument`; no public version boundary exists. | Return an empty version result for an existing bucket, with no legacy history exposed. |
| Enabled | Create a new opaque UUID public version and retain every earlier public version. | Create a new opaque UUID delete marker, even when no current content exists. | Address the exact public version, including literal `null` for migrated legacy current content. | Return object versions and delete markers in stable S3 order. |
| Suspended | Replace the single literal-`null` version with the new content. Existing opaque versions remain retained. | Replace the literal-`null` version with a literal-`null` delete marker. | Address an opaque historical version or literal `null`. | Return retained opaque versions plus the current or historical literal-`null` entry. |

The only public null version ID is the literal query and XML value `null`.
It is never generated as a UUID and is never represented to clients as an
empty string, database `NULL`, or the internal object ID.

## Data Model and Migration

### Schema

The v0.6 migration adds `versioning_status TEXT NULL` to `buckets` and creates
this logical `object_versions` table:

| Column | Meaning |
|---|---|
| `id TEXT PRIMARY KEY` | Internal UUID for the version-index row. Never returned by S3. |
| `bucket TEXT NOT NULL` | Referencing `buckets(name)`. |
| `key TEXT NOT NULL` | S3 object key. |
| `version_id TEXT NULL` | Public ID. `NULL` in storage denotes the public literal `null`; non-null values are generated opaque UUIDs. |
| `kind TEXT NOT NULL` | Exactly `object` or `delete_marker`. |
| `object_id TEXT NULL` | Internal `objects.id` for `object`, absent for `delete_marker`. |
| `sequence BIGINT NOT NULL` | Monotonic per `(bucket, key)` ordering value. |
| `is_latest BOOLEAN NOT NULL` | Whether this record determines the key's current S3 state. |
| `created_at TIMESTAMP NOT NULL` | Version creation time returned by ListObjectVersions. |
| `updated_at TIMESTAMP NOT NULL` | Mutation timestamp for the version-index row, retained for audit and migration recovery. |

The migration creates indexes for exact selection by `(bucket, key, version_id)`,
per-key sequence ordering, and bucket-wide ListObjectVersions ordering. It
uses a partial unique index to permit at most one `is_latest = true` row per
`(bucket, key)`, and a uniqueness constraint on `(bucket, key, sequence)`.
Because SQL nulls do not collide in an ordinary unique constraint, a second
partial unique index on `(bucket, key)` where `version_id IS NULL` permits at
most one literal-null slot. It also enforces that an `object` has a non-null
`object_id`, a `delete_marker` has a null `object_id`, and no non-null public
ID is reused within a bucket/key.
Application validation enforces `kind` values on both SQLite and PostgreSQL;
database checks are added where both engines can express them consistently.

`objects` remains the authoritative immutable content record. It gains no
public version ID. Its `is_latest` field becomes the projection of the current
content state: exactly one row is true when the current version is `object`,
and no row is true when the current version is a delete marker or no current
object exists. All ordinary GetObject, HeadObject, ListObjects, and ListObjectsV2
queries must continue to select only `objects.is_latest = true`.

### Invariants

1. For a version-aware key, `object_versions.is_latest` has zero or one row;
   after a successful enabled or suspended publication/delete it has exactly
   one row.
2. A latest `object` row points to exactly one immutable `objects` record and
   that record has `objects.is_latest = true`. A latest marker has no object
   record and makes every `objects` record for that key non-latest.
3. An `object_versions` row never changes its public ID, sequence, kind, or
   `object_id`. Replacing a suspended null version creates its successor only
   after ending the affected null content lease, then removes or supersedes the
   prior null index entry inside the same transaction.
4. Tags, pin leases, targets, and remote-pin accounting remain owned by
   `objects.id`. A version-index row neither owns a lease nor creates a pin
   target. A marker has no content, CID, tag set, or lease.
5. Every public opaque version ID is a newly generated UUID. The only public
   `null` is the documented null-version representation.

### Backfill and rollback

The migration leaves all buckets unversioned by setting their new status to
database `NULL`. For every legacy `objects` row with `is_latest = true`, it
inserts one `object_versions` row with `version_id = NULL`, `kind = object`,
the existing internal `object_id`, a deterministic per-key sequence, and
`is_latest = true`. S3 exposes that row only after the bucket enters Enabled
or Suspended state, where it is rendered as version ID `null`.

While a bucket remains unversioned, each successful overwrite replaces this
hidden null index row in the same transaction as the `objects.is_latest`
projection, and a successful simple delete removes it. The row remains hidden
from ListObjectVersions and explicit `versionId` requests. This maintenance is
required so enabling the bucket later exposes the object that is current at
the transition, not whichever object happened to be current during migration.

Legacy non-latest `objects` rows receive no index entry. They remain hidden
from explicit reads, version listing, ordinary listings, tags, and deletion.
This is a strict compatibility and security boundary, not a best-effort
backfill. No migration may infer public history from them.

The migration runs in a transaction where each engine supports it. It validates
the backfill count against current legacy latest rows before committing. The
down migration is available only before any bucket is enabled or suspended and
before any non-null public version or marker exists. It verifies that condition,
then drops version indexes/table and `versioning_status`. If production v0.6
data exists, rollback is an application deployment rollback with the schema
left in place; schema destruction is refused rather than losing public version
history or dangling content leases.

## Publication, Reads, and Deletion

### One publication boundary

Every content-producing path must converge on a new version-aware publication
transaction, replacing direct calls that write only the latest `objects` row.
The paths are standard PutObject, CopyObject destination publication,
CompleteMultipartUpload, successful CID/HTTPS import publication, atomic ZIP
archive and extracted-entry publication, and any future helper that constructs
`PublicationObject::from_put`. The transaction must continue to combine the
content mutation fence, latest-projection change, tags, lease creation or
release, provider quota accounting, and job enqueueing.

The selected version state is read and locked after input validation and before
the publication transaction. For an enabled bucket, the transaction demotes
the previous version-index latest row, clears the old `objects.is_latest`,
inserts the new content and opaque public-version row, projects the new content
as latest, and preserves the previous version's tags and lease. For a suspended
bucket, it atomically ends any affected null-content lease through the existing
guarded lease-release path, replaces the null index entry, writes the new
content/null version, and installs new publication tags and leases. An
unversioned bucket retains the existing replacement and guarded-release path
while replacing its hidden null index row atomically.

Kubo `pin/add` remains before metadata publication and there is still no
compensating Kubo `pin/rm`. A failed database publication may leave a pin, but
must not remove a CID that another object or version can need. Version retention
does not alter the no-pin-rm deletion policy.

### Explicit and current reads

Without `versionId`, GetObject and HeadObject resolve the current version-index
row in enabled or suspended buckets. Current content is read through the
`objects.is_latest` projection. Current delete markers return `NoSuchKey` with
HTTP 404 and these headers:

```text
x-amz-delete-marker: true
x-amz-version-id: <opaque UUID or null>
```

With `versionId`, GetObject and HeadObject resolve the matching public index
row. A content version uses its immutable `object_id` and its stored encryption
metadata. An explicit delete-marker version returns `MethodNotAllowed` with
HTTP 405 and these headers:

```text
x-amz-delete-marker: true
x-amz-version-id: <opaque UUID or null>
Last-Modified: <marker creation time>
```

All marker responses use S3 error XML and the normal request identifiers.
They do not call Kubo, accept Range, expose a CID, or attempt SSE-C
authentication. A missing bucket remains `NoSuchBucket`; an unknown explicit
version is `NoSuchVersion`; a version ID on an unversioned bucket is
`InvalidArgument`. Storage and Kubo failures stay redacted through `AppError`
to S3 error conversion. Logs may include bucket/key and a public version ID,
but never SSE-C material, wrapped keys, decrypted data, or backend credentials.

### DeleteObject and DeleteObjects

Simple DeleteObject means no `versionId`. In an enabled bucket it atomically
creates a new opaque delete marker and returns that ID in `x-amz-version-id`
with `x-amz-delete-marker: true`. In a suspended bucket it replaces the null
version with a null delete marker, ends the displaced null content version's
lease if present, and returns `x-amz-version-id: null` with the marker header.
It never removes retained opaque versions. In an unversioned bucket it keeps
the existing idempotent delete behavior and does not create a marker.

Exact DeleteObject means a supplied `versionId`. It permanently removes only
that public version-index row. Exact deletion of a content version ends only
that version's tags and lease through its internal object ID, then removes its
content projection if it was latest. Exact deletion of a marker removes no
lease and returns `x-amz-delete-marker: true` when the deleted version is a
marker. If exact deletion removes the latest row, the next newest retained
index row becomes current and `objects.is_latest` is rebuilt atomically for it,
or the key becomes absent if no row remains. Exact deletion never creates a
replacement marker.

DeleteObjects preserves the incoming 1,000-item limit, order, quiet mode, and
per-item response behavior. Each `ObjectIdentifier.version_id` selects the
simple or exact rule above. A successful `DeletedObject` includes `version_id`
and `delete_marker` where s3s exposes those fields. Per-item `NoSuchVersion`,
`InvalidArgument`, mutation-fence conflict, and internal failures are reported
without aborting unrelated request entries. Repeated entries are processed in
request order, not deduplicated by key, because different version IDs can name
different retained versions.

### CopyObject and tagging

CopyObject parses a source `versionId` from `x-amz-copy-source` when supplied.
It reads that exact content version and rejects a source marker with the same
405 marker headers as explicit GetObject. An omitted source version resolves
the source current state and likewise rejects a current marker as 404. Source
SSE-C validation is performed against the selected immutable object.

The destination always follows its own bucket state. An enabled destination
gets a newly generated opaque version ID, even if source and destination share
the same CID. A suspended destination replaces only its literal-null version.
An unversioned destination retains overwrite behavior. Copy tag directive
`COPY` reads tags from the selected source internal object, and `REPLACE`
creates destination tags for the new destination object version. Copy does not
share an object ID, tag set, or lease with its source.

GetObjectTagging, PutObjectTagging, and DeleteObjectTagging accept an explicit
public version ID for enabled and suspended buckets and resolve it to the
matching content object's internal ID. They return `NoSuchVersion` for absent
versions and `MethodNotAllowed` for markers; markers have no tag set. Omitted
version ID targets current content and returns 404 for a current marker. The
tagging transaction must lock the resolved immutable object and retain existing
lease snapshot checks. It must not fall back from an explicit version to the
latest object.

## Listing and Bucket Operations

### Ordinary object listings

ListObjects and ListObjectsV2 remain current-state APIs. They list only
`objects.is_latest = true`, so they hide noncurrent versions and all markers.
Their existing prefix, delimiter, encoding, continuation, and last-writer
behavior are unchanged. A marker as current state makes its key absent from
ordinary lists.

### ListObjectVersions

ListObjectVersions is implemented through the s3s 0.14 `S3` trait and its
`ListObjectVersionsInput` and `ListObjectVersionsOutput` DTOs, with the handler
delegating to a dedicated versioning operation module. Before implementation,
the plan must compile against the locked s3s 0.14 API and use its exact field
names for `Version`, `DeleteMarker`, `key_marker`, `version_id_marker`, and
their next-marker fields. No handwritten route or XML serializer may bypass
s3s SigV4 routing and DTO serialization.

The query orders first by key ascending and then by sequence descending. It
uses the tuple `(key, sequence, public version ID)` as its stable internal
cursor. Per the AWS API contract, returned `NextKeyMarker` and
`NextVersionIdMarker` name the first version or marker not returned because of
`max-keys`; a subsequent request starts with that exact entry. The
implementation fetches one extra visible row to determine truncation and uses
that row for the next-marker pair. It validates that a supplied marker pair
resolves to the same bucket/prefix order. A malformed or inconsistent pair is
`InvalidArgument`, not a silently widened scan.

`prefix`, `delimiter`, `max-keys`, and `encoding-type=url` follow the existing
ordinary-listing conventions. Delimiter common prefixes and versions consume
the same `max-keys` budget. A common prefix is emitted once per page and the
cursor advances past every version hidden under it, preventing duplicate
prefixes or an infinite page. `IsLatest` is true exactly for the one current
index row per key. `Version` entries project stored object metadata; marker
entries project marker creation time, key, version ID, owner when supported by
the existing DTO, and `IsLatest`, but no content fields.

Unversioned buckets return a valid empty ListObjectVersions result. This keeps
the legacy backfill private until the bucket has a version-aware state and
avoids manufacturing version history for an unversioned API contract.

### Bucket versioning and deletion

PutBucketVersioning changes only `versioning_status` after validating bucket
existence and the requested status. Enabling from suspended preserves all
retained versions and causes later publications to use opaque IDs. Suspending
from enabled preserves opaque history and designates the one null slot for new
content or a new marker. Repeating the current status is successful and does
not rewrite versions. First enable makes the maintained hidden null row, if
present, publicly addressable as version ID `null`; it never exposes legacy
non-latest rows. The v0.6 implementation rejects MFA Delete configuration
elements rather than ignoring them.

DeleteBucket requires no active multipart upload, import work, current object,
retained public version, or delete marker. In a version-aware bucket, clients
must exactly delete every retained content version and every marker before the
bucket can be deleted. The operation must inspect `object_versions`, not only
the `objects.is_latest` projection. Legacy hidden non-latest object rows remain
hidden and do not become a public deletion obligation; implementation must
retain the existing database cleanup ownership for those rows.

## Concurrency, Engine Support, and Errors

PostgreSQL obtains an exclusive lock on the relevant current version/index row
and uses the existing canonical pinning lifecycle lock order for content
publication and lease changes. SQLite performs the same transition inside its
single-writer transaction and retries only documented serialization, busy, or
unique-index contention with bounded backoff. Neither backend uses a
read-then-write sequence outside the transaction to select the next sequence,
latest state, or null slot.

Concurrent writes to one key have a single serialized winner order. A stale
import or other content mutation fence loses with the existing
`StaleContentMutation` mapping and cannot publish a version after a newer
delete or write. Concurrent exact delete, tag replacement, and publication
must revalidate the selected index/object row under lock; a missing or changed
row yields `NoSuchVersion` or the existing conflict result, never an operation
on a newly current version. Migration tests run both SQLite and PostgreSQL
where the repository already supports each engine, including upgrade, backfill,
constraint, and rejected destructive-down cases.

All public errors are S3 errors with redacted backend detail. The mapping must
distinguish `NoSuchBucket`, `NoSuchKey`, `NoSuchVersion`, `InvalidArgument`,
`MethodNotAllowed`, `OperationAborted`, and `InternalError`. Database SQL,
Kubo request bodies, raw provider responses, object encryption keys, and
internal IDs remain absent from client errors and structured logs.

## Tests and Client Evidence

Implementation is test-driven. Unit and integration coverage must prove:

1. Bucket status transitions, response XML, idempotent status writes, rejected
   invalid/MFA Delete configuration, and `NULL` unversioned behavior.
2. SQLite and PostgreSQL migration/backfill invariants, especially literal-null
   current legacy rows and the permanent invisibility of legacy non-latest rows.
3. Enabled and suspended put, copy, multipart completion, import, ZIP
   publication, current/explicit get and head, marker 404/405 headers, and
   SSE-S3/SSE-C selection by public version.
4. Simple and exact DeleteObject/DeleteObjects semantics, response fields,
   duplicate request ordering, quiet mode, marker deletion, lease retention,
   null replacement lease release, and no Kubo `pin/rm`.
5. Version-specific tagging and leases, including marker rejection and proof
   that tags and leases retain internal object IDs rather than public IDs.
6. Ordinary listing invisibility, ListObjectVersions ordering, marker/version
   projection, stable continuation markers, delimiter page boundaries, URL
   encoding, and races with writes/deletes.
7. Bucket deletion blocking until every public version and marker is gone, plus
   migration and transaction rollback under injected failure.
8. Parallel SQLite file-backed and PostgreSQL contention tests that demonstrate
   one latest version, no leaked null slot, no stale-publication resurrection,
   and no deadlock with pinning lifecycle work.

Real-client evidence is required after automated tests, using the existing
Docker-based validation environment only in the implementation task. It must
record AWS CLI or a supported SDK performing enable, two writes, simple delete,
ListObjectVersions, explicit historical get, marker 404/405 behavior, exact
marker delete, suspended null overwrite, and version-aware bucket cleanup.
Evidence must include commands, redacted output assertions, versions of the
client and gateway, and an honest `NOT RUN` state until executed. This design
task does not run Docker or live tests.

README and ROADMAP may be updated only after all static, unit, integration,
database, and required real-client evidence gates pass. The roadmap boxes are
checked together in the same reviewed change because the three public behaviors
are one atomic feature. No README, ROADMAP, implementation plan, source, or
test file changes are authorized by this specification task.

## Non-goals

- MFA Delete configuration or MFA device verification.
- Lifecycle rules.
- CORS configuration.
- Object Lock, retention, or legal holds.
- Pin reclamation, Kubo `pin/rm`, garbage collection, or a content deletion
  lifecycle.
- Replication, cross-region version replication, restoration APIs, inventory,
  or event notifications.
- Exposing legacy non-latest `objects` rows as public history.
- Changing S3 authentication, encryption formats, ETag-as-CID behavior, or the
  existing tag and lease ownership boundary.

## Acceptance Criteria

1. The three v0.6 roadmap checkboxes are delivered and documented as one
   atomic public versioning feature with the exact state and operation matrix
   in this specification.
2. New content versions use opaque public UUIDs, legacy/current and suspended
   slots use the literal public `null`, and internal object IDs are never
   returned as version IDs.
3. `object_versions` and `objects.is_latest` satisfy the stated invariants on
   SQLite and PostgreSQL through success, failure, migration, and concurrency
   scenarios.
4. All content publication paths use the version-aware atomic publication
   boundary and preserve tag/lease ownership and no-pin-rm safety.
5. Current and explicit reads, copies, tags, simple/exact deletes, multi-delete,
   ordinary listings, ListObjectVersions pagination/delimiter behavior, and
   bucket deletion have the specified S3-visible behavior.
6. Delete markers return the specified 404/405 headers and never create a CID,
   content object, tag set, or lease.
7. Automated tests and the real-client evidence gate pass before README and
   ROADMAP are changed, and the roadmap boxes change together.
8. `git diff --check` passes and the implementation changed-path audit is
   limited to the approved plan boundary. This design document itself authorizes
   no implementation or version-control write.

## Official Sources

- AWS S3 PutBucketVersioning: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html>
- AWS S3 GetBucketVersioning: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html>
- AWS S3 ListObjectVersions: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html>
- AWS S3 DeleteObject, delete markers, and version IDs: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html>
- AWS S3 DeleteObjects: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html>
- AWS S3 CopyObject: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
- AWS S3 versioning behavior and suspended buckets: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html>
- AWS S3 working with delete markers: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html>
- AWS S3 MFA Delete: <https://docs.aws.amazon.com/AmazonS3/latest/userguide/MultiFactorAuthenticationDelete.html>

## Git and Review Boundary

This specification authorizes no production, test, documentation-state,
roadmap, plan, or configuration edit. A future implementation must start from
an approved implementation plan, remain within that plan's explicit changed
path boundary, preserve the security and versioning boundaries above, and stop
for a design revision if s3s 0.14 or client evidence contradicts this contract.
It must not stage, commit, push, tag, or otherwise write Git history without
separate explicit user authorization.

*Author's note: Written for the engineer planning v0.6, so they can implement one coherent S3 versioning contract without exposing legacy rows or breaking pin leases.*
