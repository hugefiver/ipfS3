# Lifecycle Abort Incomplete Multipart Upload Design

**Status:** Approved design
**Date:** 2026-09-01
**Roadmap scope:** Lifecycle program phase B, abort incomplete multipart uploads
**Baseline:** `91e164d9c91b325f9df059cf90bdf5024f675922` (`91e164d`, clean); Phase A is `ff96b3f`; the current bucket CORS commit is unrelated.

## Summary

This phase enables `AbortIncompleteMultipartUpload` in the lifecycle control
plane released by Phase A. It adds an immutable multipart-upload target to the
existing durable action system, scans active uploads after version sources, and
deletes an exact incomplete upload through one bucket-locked database
primitive. It does not change expiration behavior.

The implementation must keep lifecycle work separate from content mutation.
An incomplete upload has not published an object version, so a lifecycle abort
does not take the standard content-mutation admission token and cannot block a
PUT or CopyObject. It does, however, serialize with CompleteMultipartUpload,
explicit abort, and bucket deletion through the bucket lock.

`ROADMAP.md` keeps its Lifecycle item unchecked. Transition,
NoncurrentVersionTransition, physical residency, CORS, lifecycle abort
response headers, and ListMultipartUploads are outside this phase.

## Approaches and Decision

### Selected: polymorphic lifecycle action target

`lifecycle_actions` gains a typed target shape. Existing expiration actions
keep their version identity. Abort actions carry a
`MultipartUploadTargetIdentity` containing bucket, key, upload ID, and the
database-recorded initiation timestamp. A `LifecycleTargetIdentity` enum makes
the distinction explicit in the domain model and in worker revalidation.

This keeps one claim, lease, retry, revision-fence, and action-audit system for
all lifecycle work. The action row remains the durable record that a specific
rule, at a specific configuration revision, was entitled to act on a specific
immutable target at a specific due boundary.

### Rejected: separate multipart action table

A separate table would duplicate state transitions, lease recovery, retries,
diagnostics, worker selection, and configuration invalidation. It would also
force operational tooling to join two incompatible lifecycle queues. The
different target shape is small and is enforced by a database check, so a
second queue is not warranted.

### Rejected: sentinel version fields

Encoding an upload as a fake version row, a zero sequence, or a synthetic
public version ID would weaken the exact-target invariant. It would make old
version idempotency encodings ambiguous and permit an accidental version-path
operation. A discriminated target is clearer and can be validated by both the
database and Rust types.

## Configuration Contract

### Canonical rule model

Each canonical lifecycle rule receives an optional
`abort_incomplete_multipart_upload` action with exactly one field:

```text
days_after_initiation: u32
```

The inbound value must be an integer from 1 through `i32::MAX`, which is the
largest value representable by the locked s3s 0.14 DTO. The canonical Rust
field remains `u32`, but shared semantic validation rejects larger stored
values so PUT, GET, scan, and execution have one representable contract.
`Days=0` is rejected with `InvalidRequest`. AWS and CloudFormation material can
be read as ambiguous on zero in adjacent lifecycle contexts; this gateway
deliberately chooses a positive, full-day minimum so the action has a stable
age contract.

A rule must still contain at least one supported action. A valid prefix or
all-objects rule may contain abort together with existing supported expiration
actions. No action is discarded from a mixed valid rule. Unsupported transition
actions remain a whole-document `InvalidRequest`, exactly as in Phase A.

An abort action narrows the selector accepted for that rule:

| Selector form | Abort action allowed | Reason |
|---|---:|---|
| Empty modern `Filter` | Yes | It selects every incomplete upload. |
| Modern `Filter` with one `Prefix` | Yes | It can be evaluated from the upload key. |
| Legacy top-level `Prefix` | Yes | It can be evaluated from the upload key. |
| `Tag` | No | AWS lifecycle abort rules prohibit tag filtering, and an incomplete upload is not a published tagged object. |
| `And` | No | It can contain tags or size predicates and is not an abort-safe selector in this phase. |
| Object-size selector | No | An incomplete upload has no final object size. |

The restriction applies only when the rule contains abort. Existing expiration
rules with tag, `And`, or size selectors remain supported and retain Phase A
semantics. A disabled rule is stored and returned normally, but schedules no
abort or expiration action.

Canonical JSON retains schema version `1`: the new optional action has a serde
default of `None`, so existing Phase A JSON remains readable without a data
rewrite. New documents serialize the optional action deterministically. Every
decoded stored document, including an old document, is semantically revalidated
before GET, scan, or execution uses it. A malformed or no-longer-valid stored
document is treated as an internal control-plane fault, is redacted in
diagnostics, and is never interpreted permissively.

### S3 surface and deliberate omissions

The existing s3s-routed lifecycle PUT, GET, and DELETE operations remain the
only lifecycle API changes. GET serializes the supported abort action in the
normal lifecycle configuration XML. PUT validates the complete document before
it replaces the configuration, so an invalid abort selector or day count leaves
the previous configuration and revision unchanged.

This phase does not add `x-amz-abort-date` or `x-amz-abort-rule-id` response
headers. It also does not add `ListMultipartUploads`. These are optional or
unsupported surfaces, not prerequisites for a correct durable lifecycle abort
engine. The existing explicit AbortMultipartUpload API remains available.

## Time and Identity

### Database time

Multipart initiation is lifecycle-relevant state. `store::multipart::create_upload`
must obtain `created_at` from `database_now`, rather than `Utc::now()`, inside
the database-backed creation path. The initiation timestamp is never changed by
UploadPart. Part uploads, replacement parts, and retries do not reset the
clock.

For an abort rule with `days_after_initiation = N`, due time is calculated from
that initiation timestamp with Phase A's existing deterministic rule: the next
UTC midnight after `N` full days have elapsed. The evaluator and worker use the
same helper and the database clock. A process clock is suitable only for sleep
and cancellation, never for persisted due times or eligibility.

### Target and candidate types

The lifecycle model gains:

```rust
pub struct MultipartUploadTargetIdentity {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub initiated_at: DateTime<Utc>,
}

pub enum LifecycleTargetIdentity {
    Version(VersionTargetIdentity),
    MultipartUpload(MultipartUploadTargetIdentity),
}

pub enum LifecycleCandidate {
    Version(/* existing version candidate fields */),
    MultipartUpload(/* exact target and initiation age */),
}
```

`LifecycleActionKind` adds `AbortIncompleteMultipartUpload`. The new-action
and stored-action conversion accepts a `LifecycleTargetIdentity`, not a version
identity with optional fields. Version actions preserve their current
idempotency-key encoding byte for byte so an upgrade cannot create duplicate
Phase A work. An MPU action key deterministically includes bucket,
configuration revision, rule identity, action kind, upload ID, initiation
timestamp, and due boundary. It must be stable across retries and different
gateway processes, with unambiguous length-prefixing or canonical serialization
rather than delimiter-dependent concatenation.

## Durable Schema and Migration

### Migration shape

Append `m20260901_000001_lifecycle_abort_multipart` after
`m20260831_000001_bucket_cors`. It changes only the lifecycle action
representation. It must not rewrite bucket configurations, multipart uploads,
object versions, object content, pins, or provider work.

`lifecycle_actions` becomes polymorphic with these additions and nullability
changes:

| Column | Requirement |
|---|---|
| `target_type` | Non-null `version` or `multipart_upload`; existing rows backfill to `version`. |
| `target_version_row_id`, `target_public_version_id`, `target_sequence` | Nullable only to permit the multipart shape. Existing values remain unchanged. |
| `target_object_id` | Remains nullable because delete-marker version targets have no object. |
| `target_upload_id` | Nullable, non-null only for the multipart shape. |
| `target_upload_created_at` | Nullable, non-null only for the multipart shape; PostgreSQL type is `TIMESTAMPTZ`. |

The action-kind check adds `abort_incomplete_multipart_upload`. An exact target
shape check requires one and only one of the following:

```text
target_type = 'version'
  AND target_version_row_id IS NOT NULL
  AND target_public_version_id IS NOT NULL
  AND target_sequence IS NOT NULL
  AND target_upload_id IS NULL
  AND target_upload_created_at IS NULL

target_type = 'multipart_upload'
  AND target_version_row_id IS NULL
  AND target_public_version_id IS NULL
  AND target_object_id IS NULL
  AND target_sequence IS NULL
  AND target_upload_id IS NOT NULL
  AND target_upload_created_at IS NOT NULL
```

A companion kind-to-target check requires the three existing expiration kinds
to use `version` and requires `abort_incomplete_multipart_upload` to use
`multipart_upload`. `object_key`, bucket, revision, rule, due time, lease, and
terminal-state checks remain required for both shapes.

There is deliberately no foreign key from an MPU action to
`multipart_uploads`. Completion or explicit abort removes the upload row. A
foreign-key cascade would erase the lifecycle action and its audit outcome;
instead, execution observes the missing exact target and records terminal
`AlreadySatisfied` success.

PostgreSQL uses transactional `ALTER TABLE`, a backfill, explicit constraint
replacement, and indexes appropriate to both target shapes. Its lifecycle and
multipart timestamps are `TIMESTAMPTZ`. SQLite performs a transactional table
rebuild because nullable changes and check replacement cannot rely on a broad
ALTER implementation. The rebuild copies every old row, sets
`target_type = 'version'`, recreates all old checks and indexes unchanged, then
adds the new check and MPU-target index. Tests must inject a failure before the
swap and prove that the old table and data survive. The down migration refuses
to discard any multipart-target action or abort action; it may reverse only
when no such state exists, preserving the Phase A down-safety policy.

### Entity and data preservation rules

The SeaORM lifecycle-action entity exposes nullable version fields, target
type, and MPU fields. Conversion code rejects impossible nullable combinations
even after database reads. Existing action IDs, state, claims, idempotency
keys, due dates, error diagnostics, and every version-target column retain
their exact values through migration. No migration ever touches a CID or issues
Kubo or provider requests.

## Scan and Evaluation

### Cursor protocol

The source order becomes `Current`, `Noncurrent`, then `Multipart`. Current
and noncurrent scans retain their exact Phase A order. Multipart scans include
only rows still present in `multipart_uploads`, ordered by:

```text
key ASC, created_at ASC, upload_id ASC
```

The stored cursor is backward compatible with Phase A JSON. It retains the
existing version cursor fields for version sources and adds an explicit optional
`multipart_created_at` timestamp plus an optional multipart upload ID for the
multipart source. It does not put a timestamp into `sequence`, synthesize a
version row ID, or reuse a version cursor field as an MPU identity. Decoding an
old cursor yields its same current or noncurrent position. New validation
requires the exact field set for its source and rejects a mixed, missing, or
wrong-bucket cursor.

The multipart tuple-after predicate is the strict lexicographic comparison of
`(key, created_at, upload_id)`. The scanner advances its cursor and inserts
actions in the same lease-fenced transaction used by Phase A. This gives stable
bounded pages, no duplicate or loop from a resumed old cursor, and normal
snapshot-free progress: an upload inserted before the stored cursor waits for a
fresh cycle, while one inserted after it can be seen in the current cycle.

### Evaluation

Version candidate evaluation and expiration conflict rules are unchanged. MPU
candidates consider only enabled rules that contain an abort action and select
all uploads or prefix-matching uploads. They do not inspect object tags,
object size, published versions, or expiration conflict precedence. The
candidate's `initiated_at` produces its due boundary, and one matching rule
creates at most one idempotent abort action for that immutable upload identity
and due time. Repeated scans are harmless under the unique key.

## Execution and Shared Abort Primitive

### Worker transaction

The generic lifecycle worker loop, action claims, bounded retries, and recovery
worker are reused. No new worker is introduced. An MPU action execution uses
this final transaction order:

1. Lock and fence the claimed action first, including its claim epoch.
2. Read database `now`.
3. Acquire the bucket ownership lock or fence used by multipart completion and
   bucket deletion.
4. Revalidate the active configuration, exact revision, named rule, and enabled
   abort action.
5. Revalidate exact upload identity, initiation timestamp, due time, and the
   permitted all-or-prefix selector.
6. Run the guarded exact abort primitive.
7. Write terminal success or cancellation, with the claim fence, in the same
   transaction.

There is no standard content-mutation admission token in this sequence. The
upload is not current object state. Database contention, serialization errors,
and equivalent transient store failures follow the existing bounded retry and
safe-failure classifier. Diagnostics identify the action and public bucket/key
context but redact SQL, internal IDs, credentials, encryption material, raw
Kubo or provider bodies, and content.

A missing exact upload, whether it completed, was explicitly aborted, or was
already lifecycle-aborted, yields `AlreadySatisfied` and terminal success.
Identity mismatch, a replaced or deleted configuration, revision mismatch,
missing or disabled rule, selector mismatch, or a target that is not yet due
yields terminal `cancelled`. These outcomes do not call Kubo, a pin provider,
or `pin_rm`.

### Store primitive and public abort route

Add a caller-owned transaction primitive with this contract:

```rust
pub async fn abort_exact_incomplete_upload_in_transaction<C: ConnectionTrait>(
    txn: &C,
    target: &MultipartUploadTargetIdentity,
) -> AppResult<AbortExactIncompleteUploadResult>;

pub enum AbortExactIncompleteUploadResult {
    Applied,
    AlreadySatisfied,
    Stale,
}
```

Its caller must already hold the bucket lock. It performs one conditional
delete matching upload ID, bucket, key, and creation timestamp. The existing
foreign-key cascade deletes parts. It never recreates an upload, publishes an
object, changes a pin, or unpins a CID. `AlreadySatisfied` means the exact
upload no longer exists; `Stale` means the upload ID exists but its immutable
location or initiation identity does not match.

Route the explicit `AbortMultipartUpload` operation through this same primitive
inside a bucket-locked transaction. Preserve its public behavior: an absent
upload or a bucket/key mismatch remains `NoSuchUpload`, rather than being
translated to lifecycle success. Thus the internal primitive is shared without
making a client retry indistinguishable from a durable lifecycle action.

## Race and Failure Semantics

CompleteMultipartUpload's final database publication and both explicit and
lifecycle abort acquire the same bucket lock. If abort wins, it deletes the
upload before final publication and CompleteMultipartUpload fails with
`NoSuchUpload`; no object version is published. If completion wins, it deletes
the upload as part of publication and the later lifecycle action succeeds as
`AlreadySatisfied`. The existing no-`pin_rm` policy means a root or part CID
created before a losing race can remain pinned or otherwise retained. That is
acceptable until a separate reference-safe reclamation design exists.

UploadPart may finish its Kubo add or pin race after abort. Its final part-row
insert or upsert must require that the upload still exists and fail with
`NoSuchUpload` after the abort, so it cannot resurrect the upload. Retention of
the already-created CID is safe under the same no-`pin_rm` policy. Bucket
deletion remains serialized by the bucket lock and keeps its active-MPU
protection. Explicit abort and lifecycle abort have intentionally different
idempotency surfaces: the former returns the S3 error for an absent or mismatch
upload, while the latter terminalizes durable work successfully when the desired
absence is observed.

## Tests and Evidence

Implementation is test-driven. The acceptance matrix must cover:

1. Canonical configuration parsing, GET round trips, schema-version backward
   compatibility, semantic stored-JSON revalidation, day boundaries from 1 to
   `i32::MAX`, rejected zero and larger stored values, and a rule with no
   action.
2. Empty and prefix selectors, legacy and modern prefix forms, mixed abort plus
   expiration rules, disabled rules, and whole-document rejection for abort
   paired with Tag, And, or object-size filtering.
3. Migration upgrade and down safety on SQLite and fresh PostgreSQL 17:
   existing version actions keep byte-identical IDs and keys, target-shape
   checks reject hybrids, indexes survive a SQLite rebuild, and injected
   migration failure preserves rows and constraints.
4. Cursor encode/decode compatibility with stored Phase A cursors, source order,
   multipart tuple ordering, stable bounded pages, no duplicates or loops,
   before-cursor insertion deferred to a fresh cycle, and incomplete-only
   scanning.
5. Evaluator due-time calculation from database initiation time, no reset after
   UploadPart, prefix revalidation, one idempotent abort action, action crash
   recovery, lease reclaim, stale claim epochs, and bounded retry behavior.
6. The shared explicit-abort primitive and signed S3 API behavior, including
   absent and mismatch `NoSuchUpload` preservation, cascading parts, and no
   upload resurrection.
7. UploadPart, CompleteMultipartUpload, explicit abort, lifecycle abort, and
   bucket-delete races. Cover both race winners, no accidental publication,
   retained losing-race CID acceptance, and proof that no path calls `pin_rm`.
8. SQLite multiworker and fresh PostgreSQL 17 multiworker and multigateway
   contention, including configuration replacement, rule disablement, bucket
   ownership fencing, serialization retry, and terminal action state.
9. Signed lifecycle-management API coverage and AWS CLI lifecycle configuration
   evidence. Run live evidence only in the implementation environment, after
   local regression gates, and record redacted commands, client versions,
   gateway revision, and observed results before any README capability claim.
10. The full relevant regression suite. Protect `Cargo.toml`, the default
    deployment profiles, and unrelated service configuration unless runtime
    evidence demonstrates a necessary change.

The design task itself runs neither Docker nor a live gateway. `ROADMAP.md`
remains unchecked even after this phase passes because transition and the later
lifecycle program phases remain unfinished.

## Likely Changed Paths

The implementation plan should keep its boundary small and justify every path.
Expected paths are:

```text
src/lifecycle/model.rs
src/lifecycle/config.rs
src/lifecycle/evaluator.rs
src/lifecycle/actions.rs
src/store/lifecycle_scan.rs
src/store/lifecycle_action.rs
src/store/multipart.rs
src/store/entities/lifecycle_action.rs
src/store/migrations/mod.rs
src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs
src/s3/ops/lifecycle.rs
src/s3/ops/multipart.rs
src/store/pinning/publication.rs
tests/integration.rs
tests/postgres_lifecycle.rs
tests/multi_gateway.rs
README.md
```

The exact test files may follow repository conventions, but no README update is
permitted until the stated live evidence exists. `ROADMAP.md`, CORS code,
transition code, residency code, Cargo configuration, and default deployment
profiles are outside the expected boundary.

## Security, Operations, and Git Boundary

Lifecycle remains database-authoritative metadata work. It does not alter
SigV4, bucket authorization, CID-as-ETag, SSE-S3, SSE-C, object metadata, or
physical content residency. S3 request errors continue to use normal XML
errors; asynchronous action failures are kept in redacted durable diagnostics.
SQLite's transactional writer serialization and PostgreSQL row locks are both
part of the correctness contract, not implementation details to bypass with
read-then-delete logic.

Before delivery, run the identity-bound Oracle and Reviewer checks against the
final changed paths, then make one final semantic commit. Do not push or tag.
This approved design authorizes no implementation, staging, commit, push, tag,
Docker run, live service run, README update, or ROADMAP change by itself.

## Official AWS References

- [AbortIncompleteMultipartUpload element](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortIncompleteMultipartUpload.html)
- [Lifecycle configuration examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)
- [Lifecycle configuration elements and filters](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)
- [Multipart upload overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [AbortMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)
- [CompleteMultipartUpload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [ListMultipartUploads API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html)

*Author's note: Written for the Rust engineer implementing Phase B, so they can add lifecycle cleanup without turning an unfinished upload into a published object or a pin-reclamation claim.*
