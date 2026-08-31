# Lifecycle Expiration Design

**Status:** Approved design
**Date:** 2026-08-26
**Roadmap scope:** Lifecycle program phase A, independently deliverable expiration subset

## Summary

This project delivers a complete, durable S3 Bucket Lifecycle configuration
surface for expiration actions. It stores one canonical, revisioned
configuration per bucket, evaluates it with bounded database-clock scans, and
executes safe, idempotent actions through the established version-aware
ownership boundary. It supports current `Expiration`,
`NoncurrentVersionExpiration`, and `ExpiredObjectDeleteMarker`.

The project deliberately does not claim full lifecycle support. PUT rejects
`Transition`, `NoncurrentVersionTransition`, and
`AbortIncompleteMultipartUpload` with `InvalidRequest`, and a rejected request
persists no portion of its XML. The next program project adds multipart abort;
storage-class transition waits for the residency, dual-Kubo, and saga projects
defined in `2026-08-26-lifecycle-program-design.md`.

## Scope and Decisions

### Selected approach

Lifecycle configuration has a dedicated control-plane table and lifecycle work
has a dedicated durable-action table. The evaluator uses stable cursors and
scan leases to discover eligible targets. The worker leases individual actions,
then rechecks the configuration revision, rule, target identity, current or
noncurrent state, filter, tags, size, and conflict winner before calling a
privileged deletion operation. The worker uses database UTC time for every
persisted due-time and eligibility comparison.

This separates a customer-visible retention policy from pin-provider work.
`pin_jobs` remains responsible only for provider `submit`, `poll`, `unpin`, and
`reconcile` work under pin-lease fences. A lifecycle record must instead retain
the configuration revision, rule ID, immutable version or marker identity,
eligibility, and S3 deletion semantics. Reusing `pin_jobs` would make a rule
replacement look like provider cancellation and could corrupt retries.

### Rejected approaches

1. **An in-memory daily sweep, rejected.** It loses progress and can repeat
   destructive selection after restart. Durable records make crash recovery and
   multi-gateway claims auditable.
2. **Delete by `(bucket, key)`, rejected.** The key can be republished between
   scan and execution. An immutable `object_versions` row identity and sequence
   are required to keep an action from touching the successor.
3. **Direct `pin_rm`, rejected.** CIDs are shared across object rows, retained
   versions, and pinning ownership. Lifecycle deletion changes metadata and
   uses existing guarded lease handling; it does not physically unpin content.
4. **Partial acceptance of future actions, rejected.** Silently retaining only
   expiration from an otherwise invalid document would leave clients believing
   a transition or multipart abort was configured.

## S3 API Contract

The s3s 0.14 `S3` implementation adds these exact trait methods and delegates
them to `s3/ops/lifecycle.rs`:

```text
delete_bucket_lifecycle
get_bucket_lifecycle_configuration
put_bucket_lifecycle_configuration
```

No handwritten route or XML serializer bypasses s3s SigV4 routing and DTO XML
serialization. Implementation must compile against the locked s3s 0.14 DTO
field names before code is accepted.

| Operation | Success | Required behavior |
|---|---|---|
| `put_bucket_lifecycle_configuration` | 200 | Validate the entire XML document, canonically serialize it, then replace the bucket configuration and increase its revision in one transaction. |
| `get_bucket_lifecycle_configuration` | 200 | Return the exact supported canonical rule model as lifecycle XML. |
| `get_bucket_lifecycle_configuration`, absent configuration | `NoSuchLifecycleConfiguration`, 404 | Return normal S3 error XML. |
| `delete_bucket_lifecycle` | 204 | Remove the configuration in one transaction. Existing actions become stale and cancel when claimed or executed. |

The repository currently has a single bucket-owner model. For PUT, GET, and
DELETE, an omitted `x-amz-expected-bucket-owner` is accepted. When supplied,
it must equal the stored bucket owner; a mismatch returns S3 `AccessDenied`
with HTTP 403. The gateway does not invent IAM policy evaluation or directory
bucket behavior. A missing bucket remains `NoSuchBucket`.

## Rule Model and Validation

Each bucket has zero or one active configuration, with one to 1,000 rules. A
rule ID is optional. When present it is at most 255 characters and unique within
the configuration; rules without IDs use their canonical ordinal only as an
internal revision-scoped identity. Status is case-sensitive and exactly
`Enabled` or `Disabled`. The implementation must reject duplicate present IDs,
more than 1,000 rules, unknown status, and duplicate or contradictory fields
that reach the typed DTO with `InvalidRequest` before it changes the database.
Malformed XML is rejected by s3s before the trait method as `MalformedXML`.
s3s 0.14 ignores unknown direct children of the root lifecycle element before
the handler receives the DTO; this phase documents and tests that framework
compatibility limit instead of adding a handwritten authenticated raw-body
route or claiming strict unknown-element rejection.

Each rule contains at least one supported action. It may use legacy top-level
`Prefix`, or a modern `Filter`, but not both. If legacy `Prefix` is absent, the
modern `Filter` element is required; an explicitly empty `Filter` means all
objects. A modern filter is exactly one of an empty filter, `Prefix`, `Tag`, a
single object-size bound, or `And`. `And` contains at most one prefix, zero or
more tags with distinct keys, and at most one lower and one upper size bound.
Tag matches require exact key and value. A key-only tag means an empty value,
matching the repository's stored tag model. A prefix match is byte-for-byte.
`ObjectSizeGreaterThan` and `ObjectSizeLessThan` are nonnegative bytes,
exclusive, and, when both exist, lower is strictly less than upper. A filter
checks an object's publication tags, not provider-policy tags or marker data.

An `Expiration` may specify exactly one of `Date`, `Days`, or
`ExpiredObjectDeleteMarker=true`. `Date` is a valid ISO 8601 UTC midnight.
`Days` is a positive whole number. `ExpiredObjectDeleteMarker=false` is
rejected because it describes no action. An expired-object-delete-marker action
cannot coexist in the same `Expiration` with `Date` or `Days`, and it cannot use
a tag-based filter because a delete marker has no object tags.

A `NoncurrentVersionExpiration` requires positive whole-number
`NoncurrentDays`. `NewerNoncurrentVersions` is an optional additional threshold
from 1 through 100 and requires an explicit modern `Filter`, as required by
AWS. When supplied, both the age and newer-version thresholds must be exceeded.
These numbers apply to public retained content versions and delete markers,
ordered by immutable sequence, not to hidden legacy object rows.

The following elements make the entire PUT invalid: `Transition`,
`NoncurrentVersionTransition`, and `AbortIncompleteMultipartUpload`. All
unsupported storage classes are therefore rejected, including GLACIER and
archive names. No transition is represented as metadata in this phase.

The validator builds one internal canonical model before any database write.
Canonical JSON orders object keys and rule fields deterministically, preserves
rule order, represents absent optional values explicitly, and is the sole value
stored and re-serialized by GET. It rejects action combinations AWS treats as
conflicts: duplicate action elements, multiple current expiry timing choices,
multiple noncurrent-expiration elements, contradictory legacy and modern
filters, invalid size ranges, and `NewerNoncurrentVersions` without `Filter`.

## Persistence and Migration

### Lifecycle configuration

The migration creates `bucket_lifecycle_configs` with this logical schema:

| Column | Meaning |
|---|---|
| `bucket` | Primary key and bucket foreign key. |
| `canonical_json` | Nullable validated complete configuration; NULL is a deleted configuration tombstone, never partial request XML. |
| `revision` | Positive, monotonically increasing configuration generation retained across DELETE and a later PUT. |
| `scan_cursor` | Nullable opaque stable cursor for the next bounded evaluator page. |
| `scan_lease_epoch` | Monotonic epoch for scanner fencing. |
| `scan_lease_until` | Nullable database-clock deadline for a scanner claim. |
| `created_at` | Database UTC creation timestamp. |
| `updated_at` | Database UTC replacement timestamp. |
| `last_scanned_at` | Nullable database UTC timestamp after a completed bounded page. |

The cursor encodes source class, bucket/key ordering, version sequence, and
immutable row ID so it can resume without widening a scan. A replacement resets
the cursor under the new revision. A delete increments the revision, clears
`canonical_json` and scan state, and retains the tombstone so a later PUT
advances again; all old-revision actions cancel during revalidation.

### Lifecycle actions

The migration creates `lifecycle_actions`, deliberately separate from
`pin_jobs`:

| Column | Meaning |
|---|---|
| `id` | Internal UUID action identity. |
| `idempotency_key` | Unique bucket, revision, rule, action kind, target identity, and due-boundary key. |
| `bucket`, `config_revision`, `rule_id` | The policy snapshot that selected this action. |
| `action_kind` | `expire_current`, `expire_noncurrent`, or `delete_expired_marker`. |
| `target_version_row_id` | Non-null exact object-version or marker row ID. |
| `target_object_id`, `target_sequence` | Immutable identity checks for content versions. |
| `due_at` | Database UTC eligibility boundary. |
| `state` | `pending`, `claimed`, `succeeded`, `cancelled`, or `failed_safe`. |
| `attempts`, `next_attempt_at` | Bounded replay accounting using database time. |
| `claim_epoch`, `lease_until`, `claimed_by` | Multi-gateway claim fence and lease. |
| `failure_class`, `last_error_redacted` | Safe retry classification and non-sensitive diagnosis. |
| `created_at`, `updated_at`, `finished_at` | Audit timestamps. |

Indexes select due pending work, recover expired claims, find one action by
idempotency key, and inspect an individual bucket or target. Database checks
and application validation restrict enumerated states on SQLite and PostgreSQL.
An action with a current or noncurrent content target must retain the version
row ID, object ID, and sequence; a marker action retains the marker row ID and
sequence. This prevents identity loss when a key is reused.

### Noncurrent timestamp

`object_versions` gains `lifecycle_age_started_at` and nullable
`became_noncurrent_at`. Future publications set both the version `created_at`
and `lifecycle_age_started_at` from the same database UTC clock; migration uses
the existing public version `created_at` as the best available historical age
origin. A row that is latest has a NULL
noncurrent timestamp. The atomic publication transaction writes the database's
UTC time to the previously latest row precisely when it demotes that row. A
promoted row becomes latest and has `became_noncurrent_at = NULL`. A permanently
deleted row is not retained for lifecycle scheduling.

Migration backfill copies each public version's existing `created_at` into its
lifecycle age origin and examines, for each `(bucket, key)`, the public indexed
successor with the next higher sequence. A non-latest public content or marker
row receives its successor's `created_at` as `became_noncurrent_at`; the latest
row remains `NULL`. A legacy non-latest `objects` row without a public
`object_versions` row is ambiguous: it remains invisible, is never made public,
and is never lifecycle scheduled. The migration verifies its backfill count
inside the transaction.

The down migration is allowed only if no active or tombstoned lifecycle
configuration, lifecycle action, non-null `became_noncurrent_at`, or lifecycle
age that differs from its original version timestamp exists. It verifies this
condition, drops the new tables and columns, and refuses destructive rollback
otherwise. A deployment rollback after real lifecycle data leaves schema in
place rather than losing retention history or durable action evidence.

## Evaluation and Scheduling

The evaluator runs from a lifecycle worker registered in `main.rs` with the
root cancellation token. Like existing workers, it receives a child token,
stops new scans and claims on cancellation, and drains active work within the
configured graceful shutdown period. It is a production worker, not an HTTP
request side effect.

Each evaluator iteration gets `now` from the database and claims one bucket's
scan lease transactionally. It reads a fixed-size page from the appropriate
published sources, advances the opaque cursor transactionally, and releases or
extends the lease. Pages are bounded by a configuration setting with a safe
default; one large bucket cannot monopolize a process or a database lock.
Multiple gateways can race, but only the winning scan lease owner writes the
cursor for that epoch. An expired lease is reclaimable.

For `Days` and `NoncurrentDays`, the due boundary is the first UTC midnight
after the stated number of full days from the database-recorded creation or
`became_noncurrent_at` instant. For `Date`, the supplied UTC midnight is due.
Past-due existing targets are eligible on the next scan. Disabled rules produce
no new action. The worker does not use its host clock to decide eligibility.

Before insertion, the evaluator applies the filter to the candidate's bucket,
key, stored content size, and version-specific current tag set. It treats
markers as zero-byte entries with no tags, so tag filters never match them. It
considers only public `object_versions` records and
their associated `objects` content. It excludes ambiguous legacy rows.

When several rules select a target at one due boundary, the evaluator and
worker apply AWS-compatible precedence: exact permanent noncurrent deletion
wins; a future transition would outrank current-marker creation, but is absent
in this phase; current expiration is last. Within an action type, the earliest
due time wins. The unique idempotency key and a database unique constraint make
scan replay harmless.

## Execution and Version Semantics

An action worker first claims a due action in one transaction, increasing its
claim epoch and setting its lease. Immediately before mutation it locks or
otherwise fences the action and target, reads database `now`, and revalidates:

1. The configuration exists and its revision equals `config_revision`.
2. The named rule exists, remains `Enabled`, and still contains the action.
3. The bucket, key, exact version-row ID, object ID where applicable, and
   sequence still match the durable target identity.
4. The target is still current or noncurrent as required, has reached its due
   boundary, and remains the conflict winner.
5. Prefix, filter structure, size, and all tag predicates still match. Tags
   are reread at execution time, not copied from the scan.
6. The privileged ownership admission and content-mutation guard accept the
   intended version-aware mutation.

A failed revalidation marks the action `cancelled`. A retryable admission or
database failure records a redacted error and a bounded database-clock retry.
An action whose desired state was already reached by a competing exact delete
is `succeeded` idempotently. A worker whose claim epoch is stale cannot update
the record after a new owner claims it.

### Current Expiration

Current `Expiration` normally acts on a current content version. If the current
entry is a delete marker, Date/Days expiration may remove it only when it is the
sole retained public version and the marker's lifecycle age is due; otherwise
it performs no action.

| Bucket versioning state | Lifecycle result |
|---|---|
| Unversioned | Permanently remove the one current object through the guarded version-aware lifecycle delete path. The hidden null index projection is maintained, and no delete marker is made. |
| Enabled | Perform the existing version-aware simple-delete semantic through the lifecycle admission point. It creates a new opaque delete marker, demotes the content version, and retains that content as a noncurrent public version. |
| Suspended | Perform the existing null-version simple-delete semantic through the lifecycle admission point. It replaces the current null content version with a null delete marker and applies the established guarded handling for the displaced null content version. |

No case invokes Kubo `pin_rm`. The existing ownership admission, lease ending,
tag handling, and shared-CID safety rules remain authoritative.

Date/Days sole-marker cleanup and explicit
`ExpiredObjectDeleteMarker=true` both use the exact marker-deletion action. The
explicit form is eligible as soon as the marker is the sole version; the timed
form uses the configured marker age boundary.

### NoncurrentVersionExpiration

This action applies to a public noncurrent content version or noncurrent delete
marker. It is an exact, permanent deletion of that version-row identity, not a
simple delete and never a new marker. `NewerNoncurrentVersions = N` means the
target must have more than N newer noncurrent public versions or markers for the
same bucket and key at execution time, counting by sequence; the target must
also exceed `NoncurrentDays`. A current entry, a missing version, a promoted
successor, or a changed filter match cancels the action. Marker targets have
zero size, no object tags, no object ID, and no lease lifecycle.

The operation uses the existing exact version-aware deletion and promotion
guard. If a concurrent mutation made another row current, the guard rechecks
the selected row rather than deleting a replacement. For content it ends only
ownership that belongs to the exact immutable object and retains the
no-`pin_rm` policy for shared CIDs; for a marker it removes only the exact index
row and performs no content lifecycle work.

### ExpiredObjectDeleteMarker

This action requires an enabled rule with `ExpiredObjectDeleteMarker=true` and
an exact current marker target. At execution, the marker must be the sole
remaining public version for its `(bucket, key)`: no public content version,
noncurrent marker, or other retained version may exist. The marker is then
permanently removed through the version-aware guard. If any version exists, or
the marker is no longer current, the action cancels. The operation has no CID,
object row, tags, lease, or Kubo request.

## Failure, Security, and Error Semantics

Claims, action updates, version mutation, and configuration revision checks use
the repository's SQLite and PostgreSQL transaction conventions. PostgreSQL
uses row locks and database uniqueness; SQLite uses its single-writer
transaction with bounded retry only for documented busy, serialization, or
unique-contention cases. No read-then-write sequence selects a target outside
the transaction fence.

The failure classifier distinguishes retryable temporary database contention
and internal dependencies from safe terminal cancellation, validation failure,
and completed idempotent outcomes. It never loops indefinitely. Logs include
the action ID, bucket, key, public version ID when available, revision, and
redacted failure class. They exclude SQL text, internal object UUIDs, raw Kubo
or provider bodies, backend credentials, SSE-C material, wrapped keys, and
decrypted content.

Public API errors use normal S3 XML and request identifiers. Unsupported action
elements and typed validation conflicts are `InvalidRequest`; malformed XML is
the framework-native `MalformedXML`, and no rejected configuration is partially
written. Missing configuration is
`NoSuchLifecycleConfiguration` with HTTP 404; owner mismatch is `AccessDenied`
with HTTP 403; missing bucket is `NoSuchBucket`. Worker faults are not exposed
through an S3 request after PUT has succeeded.

## Tests and Evidence

Implementation is test-driven. The test suite must cover:

1. s3s-routed signed PUT, GET, and DELETE XML, full replacement, absent GET
   404, DELETE 204, expected-owner checks, and a rejected PUT leaving the prior
   canonical JSON and revision unchanged.
2. Validation boundaries: 1,000 rules, ID length and uniqueness, enabled and
   disabled statuses, legacy prefix, empty filter, tag, `And`, exclusive size
   bounds, forbidden future actions, every expiration combination, and
   noncurrent count/filter constraints.
3. Canonical JSON round trip and GET XML, including no partial configuration
   record after framework-rejected malformed XML or handler-rejected conflicts;
   root unknown-child characterization remains explicit and stable.
4. SQLite and PostgreSQL migration upgrade, successor-time backfill, latest
   nullness, ambiguous legacy non-public rows, safe down migration, and refusal
   of destructive down migration after lifecycle data exists.
5. UTC midnight calculations with database-provided instants, past-due existing
   objects, bounded cursors, scan lease takeover, and idempotent action inserts.
6. Current expiration in unversioned, enabled, and suspended buckets, including
   a current marker with retained history as a no-op, timed sole-marker cleanup,
   and explicit EODM cleanup; noncurrent exact permanent deletion covers both
   content versions and noncurrent delete markers.
7. Revalidation after configuration replace or delete, rule disablement, tag
   mutation, size or prefix mismatch, current replacement, promotion, exact
   delete, and conflicting actions. Tag tests must prove execution rereads tags.
8. Multi-gateway claim races, stale epoch completion, lease-expiry replay,
   process stop after claim, database retry, and one terminal outcome under
   concurrent publication or deletion.
9. Shared CID, retained versions, SSE-S3, SSE-C, tags, leases, and proof that
   no lifecycle path calls `pin_rm` or bypasses the ownership admission guard.
10. Signed live AWS CLI or SDK evidence in the implementation environment for
    PUT/GET/DELETE and each expiration action. Record commands, client version,
    gateway revision, redacted XML assertions, and an honest `NOT RUN` result
    until that environment is actually used.

The design task does not run Docker or live services. A completed phase may add
an accurate README statement that the expiration subset is supported after all
listed evidence passes. `ROADMAP.md:77` remains unchecked because transition
and the remaining lifecycle program phases are not complete.

## Likely Changed Paths

The future implementation plan should limit changes to paths justified by the
work, expected to include:

```text
src/s3/handler.rs
src/s3/ops/mod.rs
src/s3/ops/lifecycle.rs
src/store/entities/bucket_lifecycle_config.rs
src/store/entities/lifecycle_action.rs
src/store/entities/object_version.rs
src/store/migrations/m20260826_000001_lifecycle_expiration.rs
src/store/pinning/publication.rs
src/store/object_version.rs
src/store/mod.rs
src/lifecycle/mod.rs
src/lifecycle/config.rs
src/lifecycle/evaluator.rs
src/lifecycle/worker.rs
src/lifecycle/actions.rs
src/main.rs
src/config.rs
tests/integration.rs
tests/postgres_versioning.rs
tests/multi_gateway.rs
tests/e2e.rs
tests/compose.lifecycle-expiration-validation.yml
README.md
```

The exact migration timestamp, test paths, and generated entity registration
must follow repository conventions. `ROADMAP.md` is not changed by this phase.
No source or test edit is authorized by this design document alone.

## Non-goals

- `Transition`, `NoncurrentVersionTransition`, and every storage class.
- `AbortIncompleteMultipartUpload`, which is the next program project.
- Metadata-only transitions, Pinata or Filebase as a cold tier, Glacier restore,
  or tiering of remote pins.
- CORS, Object Lock, legal holds, retention, MFA Delete, replication, IAM, or
  bucket-policy lifecycle authorization.
- Kubo `pin_rm`, garbage collection, general CID reclamation, or changing the
  existing ETag-as-CID and encryption behavior.
- Exposing or scheduling ambiguous legacy non-latest `objects` rows.

## Acceptance Criteria

1. The s3s 0.14 trait methods named in this document expose atomic PUT, GET,
   and DELETE lifecycle configuration behavior with S3 XML errors.
2. Only the stated expiration subset is accepted. Every unsupported lifecycle
   action rejects the full PUT without a partial saved configuration.
3. Both engines retain canonical configuration revisions, bounded scan state,
   durable idempotent actions, claim epochs, and safe retry outcomes.
4. All eligibility and execution decisions use database time and UTC midnight
   boundaries, survive worker crash, and remain safe across gateway instances.
5. Current expiration follows all three bucket versioning states;
   noncurrent expiration permanently deletes the exact eligible version; and an
   expired marker is removed only when it is the sole remaining public version.
6. Revision, rule, filter, tags, size, identity, sequence, and version state
   are reread before mutation. Stale actions cancel rather than touch successors.
7. Existing ownership and version-aware guards remain in charge. No lifecycle
   code directly calls `pin_rm`, and shared CID, CID/ETag, and encryption
   invariants hold.
8. Unit, SQLite, PostgreSQL, signed API, migration, concurrency, and required
   live-client evidence pass before a supported-subset statement is published.
9. The full program retains `ROADMAP.md:77` as unchecked until all program
   phases are complete and final evidence has passed.

## Official Sources

- [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)
- [GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)
- [DeleteBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html)
- [Lifecycle configuration elements and filters](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)
- [Lifecycle expiration considerations and conflicts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)

## Git and Review Boundary

This approved design authorizes no implementation, README, ROADMAP, test, plan,
configuration, or Git-history change. A future implementation requires an
approved plan, must remain inside that plan's changed-path boundary, and must
stop for a design revision if the locked s3s 0.14 API or live-client evidence
contradicts this contract. It must not stage, commit, push, tag, or otherwise
write Git history without explicit user authorization.

*Author's note: Written for the engineer implementing the first lifecycle delivery, so expiration is safe to ship without pretending transition support exists.*
