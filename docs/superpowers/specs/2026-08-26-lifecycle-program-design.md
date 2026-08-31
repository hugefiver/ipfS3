# Lifecycle Program Design

**Status:** Approved design
**Date:** 2026-08-26
**Roadmap scope:** v0.6 lifecycle rules, expiration and transition

## Summary

Lifecycle is a control-plane program, not a metadata label. It evaluates a
bucket's S3 lifecycle configuration against published object versions, records
the resulting work durably, and carries out each action through the existing
version, ownership, lease, and pinning boundaries. The program delivers
expiration first, then adds residency and a physically separate cold Kubo tier
before it exposes any storage-class transition.

`ROADMAP.md:77` remains unchecked until every phase below has passed its
evidence gate. Completing the first phase may document a supported expiration
subset in the README or release material, but it must not imply that the
roadmap's complete `expiration, transition` item is finished.

## Decisions

### Selected architecture

Each bucket has one revisioned lifecycle configuration in the database. A
bounded, database-clock-driven evaluator scans published objects and versions,
then creates idempotent durable lifecycle actions. Stateless workers claim
those actions with leases, repeat every eligibility check immediately before a
mutation, and invoke a narrow version-aware lifecycle deletion or transition
operation. This makes multiple gateway processes safe and makes a crash a
replay problem rather than a data-loss problem.

The control plane is shared by expiration and transition. Physical data
placement is not. `STANDARD` content is in the hot Kubo service; future
`STANDARD_IA` content is in a separately configured cold Kubo service. The
database records residency and the required references for both services, then
the transition saga changes public residency only after the cold copy is
verified. A lifecycle transition never means merely changing a database field.

### Rejected approaches

1. **Metadata-only transition, rejected.** A storage-class value that leaves
   bytes solely in hot Kubo cannot survive hot-tier loss or prove a cold-tier
   copy exists. It would misrepresent `STANDARD_IA` to an S3 client.
2. **Pinata or Filebase as the cold tier, rejected.** Those providers are the
   gateway's remote pinning targets, not a Kubo-compatible, gateway-controlled
   storage tier with the synchronous read, integrity, ownership, retry, and
   deletion semantics required for S3 transitions.
3. **Reuse `pin_jobs`, rejected.** Pin jobs express provider submission,
   polling, unpin, and reconciliation under lease generation fences. Lifecycle
   work has a rule revision, a version identity, an eligibility date, and an
   S3-visible mutation outcome. Mixing the two would couple retry semantics and
   let lifecycle cancellation affect provider pin work.
4. **One large delivery, rejected.** A visible storage-class API before cold
   residency exists would promise behavior the gateway cannot perform. The
   ordered projects below keep every public claim true when shipped.

## Program Interfaces

### Lifecycle configuration contract

The S3 surface is always routed and XML-serialized by s3s 0.14. The handler
implements these exact `S3` trait methods:

```text
delete_bucket_lifecycle
get_bucket_lifecycle_configuration
put_bucket_lifecycle_configuration
```

The configuration is an atomic bucket replacement. It contains at most 1,000
rules and has a monotonically increasing database revision that survives a
configuration deletion. DELETE writes a tombstoned revision rather than
forgetting the generation, so a later PUT cannot reuse the revision held by an
old action. A completed replacement or deletion invalidates actions created for
an older revision at execution time. No request writes a partial configuration.

Rules select versions through a common filter model: a legacy `Prefix`, or a
`Filter` containing an empty selector, one prefix, one tag, one size bound, or
an `And` of one prefix, distinct tags, and size bounds. Every worker checks the
same canonical filter evaluator used by validation. Rule status is exactly
`Enabled` or `Disabled`.

### Evaluator and action contract

The evaluator accepts a bucket configuration revision and a database `now`. It
walks a bounded, stable cursor over current objects, noncurrent version rows,
and delete-marker candidates. It emits a durable action only when a rule is
enabled and its action is eligible at a UTC midnight boundary. An action is
addressed by a unique idempotency key containing the bucket, configuration
revision, rule ID, action kind, and immutable target identity.

The worker receives an action ID and claim epoch. It locks the action, obtains
the database clock, reads the current configuration, target version and tags,
then either cancels it as stale or executes one guarded mutation. It never
trusts a previous scan. A successful action is terminal; a retryable failure
gets a bounded, database-clock due time; a target that has disappeared or is
no longer eligible is terminally cancelled. Claim epochs prevent an expired
worker from completing a newer worker's claim.

### Residency contract

The later residency layer gives each immutable content version a primary
residency and zero or more physical references. At program completion,
`STANDARD` identifies a verified hot-Kubo reference and `STANDARD_IA`
identifies a verified cold-Kubo reference. The public object identity remains
the existing CID and ETag value. The encryption envelope, SSE-S3 key wrapping,
SSE-C validation, plaintext and encrypted read behavior do not change with
residency.

### Transition saga contract

Only one source and destination pair is supported: `STANDARD` to
`STANDARD_IA`. PUT rejects every other lifecycle storage class, including all
Glacier classes, archive classes, and reverse or lateral transitions. A
transition action has durable prepare, copy, verify, publish, and cleanup
states. It streams or Kubo-copies content to cold Kubo, verifies that the
returned or fetched CID equals the hot CID, atomically publishes the cold
reference and storage class, then removes only the now-unneeded hot residency
reference according to the reference-count rules. A failed or interrupted saga
replays from its durable state. It does not expose `STANDARD_IA` until verify
and publication commit.

## Cross-phase Invariants

1. Metadata remains in SeaORM-managed database tables; content remains
   content-addressed in Kubo. Lifecycle neither changes CID computation nor
   creates a different ETag convention.
2. An object version's encryption metadata remains attached to that immutable
   content version. A worker never decrypts, rewrites, or re-encrypts an object
   merely to expire or transition it.
3. A delete or residency release goes through the existing ownership admission,
   content-mutation guard, tag handling, and version-aware deletion boundary.
   It does not directly call `pin_rm`.
4. A CID can be referenced by several objects, versions, leases, targets, or
   residencies. A physical pin or residency reference is removed only after its
   authoritative reference count reaches zero. The gateway keeps its current
   no-`pin_rm` safety policy until a later, separately designed reclamation
   feature proves that condition.
5. An action is bound to an immutable version-row identity or marker identity,
   never just `(bucket, key)`. A later write with the same key cannot be
   mutated by stale lifecycle work.
6. All persisted lifecycle time and eligibility comparisons use the database
   clock, in UTC. Process time is only suitable for sleep and cancellation.
7. Configuration replacement, configuration deletion, rule disablement, tag
   changes, version replacement, and competing gateway workers are expected.
   Revalidation converts obsolete work into cancellation, not an error against
   the replacement target.
8. Public errors stay S3 XML errors. Logs and action diagnostics must redact
   encryption keys, Kubo credentials, provider credentials, raw backend bodies,
   and internal IDs not already public through the S3 response.
9. CORS is unrelated to lifecycle and remains a non-goal of every phase.

## Delivery Sequence

### A. Expiration control plane

This independent project provides configuration API persistence, bounded
evaluation, durable action claiming, current-version `Expiration`,
`NoncurrentVersionExpiration`, and `ExpiredObjectDeleteMarker`. It rejects
`Transition`, `NoncurrentVersionTransition`, and
`AbortIncompleteMultipartUpload` during PUT with `InvalidRequest`; it stores
nothing from that rejected request. It establishes the lifecycle tables,
revision and cancellation model, noncurrent timestamps, and worker framework.
Its detailed contract is
`2026-08-26-lifecycle-expiration-design.md`.

**Interface released to later phases:** canonical configuration parsing and
serialization, rule/filter evaluator, database clock source, scan cursor and
lease, action claim protocol, action state machine, immutable target identity,
configuration-revision fence, and the privileged version-aware lifecycle
deletion admission point.

**Gate:** unit, SQLite, PostgreSQL, signed API, migration, concurrency, and
live AWS-client evidence demonstrate the advertised expiration subset. README
may name that subset. `ROADMAP.md:77` stays unchecked.

### B. Abort incomplete multipart uploads

This project enables the already-defined control-plane action
`AbortIncompleteMultipartUpload`. It adds a versioned-action target for a
multipart upload, scans only incomplete uploads, and invokes the existing
guarded abort path. It does not add object expiration behavior or transition
support.

**Interface consumed:** Phase A configuration revision, filters, action claims,
UTC eligibility, cancellation, and redaction policy.

**Gate:** repeated scan and crash replay abort only the same eligible upload;
completed, explicitly aborted, or newly initiated uploads survive as required.

### C. Residency reference layer

This project models physical residency independently from public object and
version identity. It backfills verified hot references, records shared-CID
ownership without inferring that one key owns the bytes, and creates safe
reference-count and admission APIs. No S3 storage-class transition is accepted
or reported in this phase.

**Interface consumed:** Phase A's action identity and guarded mutation model.
**Interface released:** residency reference lookup, mutation fence, physical
reference count, and an atomic public-residency publication transaction.

**Gate:** SQLite and PostgreSQL prove shared-CID references cannot be released
by one version's deletion, including version promotion and lifecycle expiry.

### D. Dual-Kubo tier

This project adds distinct, independently configured hot and cold Kubo clients
and health/read routing. It proves a cold reference can be read and CID-checked
without treating remote pin providers as a tier. The only public class names
prepared are `STANDARD` and `STANDARD_IA`; configuration API still rejects
transition rules until the saga exists.

**Interface consumed:** Phase C residency records. **Interface released:**
named Kubo-tier clients, verified cold-copy primitive, health/error taxonomy,
and tier-aware read selection.

**Gate:** fault injection proves a failed cold add, CID mismatch, unavailable
cold node, and restart cannot publish a false cold residency.

### E. Transition saga

This project accepts only a real `STANDARD` to `STANDARD_IA` lifecycle
transition and drives the durable saga defined above. It applies the conflict
precedence below, supports rule replacement cancellation, and reports the
stored public class only after atomic verification and publication.

**Interface consumed:** phases A through D. It does not accept archive, restore,
or provider-backed pseudo-transition behavior.

**Gate:** live AWS-client evidence and process-kill replay show that a class
change has a verified cold copy, returns the original CID/ETag and encrypted
bytes, and cannot strand a shared CID.

### F. Final evidence and documentation

This project runs the complete evidence matrix, records exact client and
gateway versions plus redacted command output, updates public capability
documentation, and reviews failure paths. Only after all prior gates and this
evidence pass may the lifecycle checkbox at `ROADMAP.md:77` be checked.

## Eligibility and Conflict Semantics

For `Days` and `NoncurrentDays`, eligibility is the first UTC midnight after
the specified number of whole days has elapsed from the authoritative creation
or `became_noncurrent_at` timestamp. For a date action, the supplied UTC
midnight is the eligibility boundary. A due action may run after that boundary,
but never before it. A newly installed rule applies to existing eligible data
on the next bounded scan.

For overlapping enabled rules on one target and day, permanent deletion wins;
transition is next; current-version delete-marker creation is last. Within one
action category, the earliest due time wins. The evaluator creates at most the
winning permanent-delete action for an identical target and due boundary. The
worker recomputes the winner before execution. This prevents an expensive cold
copy when expiration is already entitled to permanently delete the target.

`NoncurrentVersionExpiration` is always an exact permanent deletion of a
noncurrent content version or noncurrent delete marker, not a new marker
creation. Current `Expiration` follows the bucket's versioning state. A current
marker is never treated as content: a Date/Days rule may remove it only when it
is the sole retained version and its own age is due, while an explicit
expired-object-delete-marker action may remove that sole marker immediately.

## Failure, Recovery, and Operations

Lifecycle actions have explicit `pending`, `claimed`, `succeeded`, `cancelled`,
and retryable-failure states. The durable record includes attempts, next due
time, safe failure class, claim epoch, lease deadline, last redacted error, and
terminal timestamp. Retryable database serialization, temporary Kubo, and
transient admission failures are retried with bounded backoff. Invalid
configuration, lost target identity, revision mismatch, failed filter match,
or an already completed idempotent outcome becomes a terminal cancellation or
success, as appropriate. Max-attempt exhaustion is visible to operators as a
safe terminal failure and does not turn into an unbounded loop.

Every gateway process may scan and claim work. Scan leases and action claim
leases are database transactions, so PostgreSQL locking and SQLite's writer
serialization have one authoritative owner at a time. Lease expiry permits a
later process to replay. Startup and shutdown integrate a lifecycle worker with
the root `CancellationToken` in `main.rs`, use a child token, stop taking new
claims on cancellation, and drain in-flight actions within the existing
graceful-shutdown policy. A forced stop leaves a claimed action to be reclaimed
after its lease expires.

## Program Test Matrix

Every phase adds focused unit tests and SQLite integration tests. Phases that
change a migration, lock protocol, or cross-gateway mutation also run PostgreSQL
coverage. Required program scenarios include rule replacement during a claim,
worker crash before and after each durable state transition, duplicate action
insertion races, lease expiry and stale claim epochs, concurrent tag update,
concurrent object publication, concurrent exact version delete, disabled rule,
shared CID, encrypted objects, and a missing bucket or configuration.

The final evidence phase runs signed S3 requests and live AWS CLI or SDK tests
only in the implementation environment. It records commands, client versions,
gateway revision, expected XML and headers, and redacted outcomes. No design
task runs Docker or a live service.

## Non-goals

- CORS configuration.
- Object Lock, retention, legal holds, MFA delete, or replication status.
- Glacier or archive transitions, restore APIs, and any class other than a real
  `STANDARD` to `STANDARD_IA` transition.
- Metadata-only, Pinata-backed, or Filebase-backed cold storage classes.
- Direct Kubo `pin_rm`, garbage collection, or a general content-reclamation
  feature before references are proven to be zero.
- Changing S3 authentication, CID-as-ETag, encryption formats, or public
  version identifiers.

## Official Sources

- [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)
- [GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)
- [DeleteBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html)
- [Lifecycle configuration elements and filters](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)
- [Lifecycle expiration considerations and conflicts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)

## Git and Review Boundary

This document approves architecture only. Each phase needs its own approved
implementation plan with an explicit changed-path boundary. A future change
must preserve the invariants above, stop for a design revision if the locked
s3s 0.14 API or client evidence contradicts this contract, and must not write
Git history without explicit user authorization.

*Author's note: Written for the engineer sequencing lifecycle work, so each shipped capability says exactly what the gateway can safely do.*
