# Lifecycle Expiration Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved lifecycle-expiration subset with atomic S3 configuration APIs, database-clock evaluation, durable fenced actions, version-aware guarded deletion, production workers, and SQLite/PostgreSQL/multi-gateway/live-client evidence without claiming transition or multipart-abort support.

**Architecture:** Persist one canonical, tombstoned, monotonically revisioned configuration per bucket and scan immutable version identities through bounded database-leased pages. A separate action outbox claims work with epochs, rereads the current rule/target/filter/tags/time, and delegates every mutation to the existing ownership admission and version-aware publication boundary; lifecycle code never owns content and never calls Kubo `pin_rm`. Split canonical modeling, validation, filter matching, database clock, configuration storage, scanning, action claims, evaluation, mutation, and process orchestration into one-responsibility files.

**Tech Stack:** Rust 2024 (MSRV 1.92), locked s3s 0.14.0, SeaORM/SeaORM Migration 1.1.20, SQLite, PostgreSQL 17, Tokio 1.52, Axum 0.8, chrono 0.4, serde/serde_json, SHA-256, UUID v4, base64 0.22, quick-xml 0.41, wiremock 0.6, PowerShell 7, Docker Compose v2.23.1+, AWS CLI v2 container, and Kubo.

**Spec:** `docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md` (authoritative approved SHA-256 `0c11f4df0b4f6e81e9834fde45368df5b330e1229dc2cdff4e8d8ab573f35742`). Supporting program context only: `docs/superpowers/specs/2026-08-26-lifecycle-program-design.md` (SHA-256 `fdcfbb22447ea7c7bfae9c549b2722664a2e8df859076e7fd3c2d20b3b0e4574`).

**Global Constraints:**
- This project delivers a complete, durable S3 Bucket Lifecycle configuration surface for expiration actions.
- It supports current `Expiration`, `NoncurrentVersionExpiration`, and `ExpiredObjectDeleteMarker`.
- The project deliberately does not claim full lifecycle support.
- PUT rejects `Transition`, `NoncurrentVersionTransition`, and `AbortIncompleteMultipartUpload` with `InvalidRequest`, and a rejected request persists no portion of its XML.
- Lifecycle configuration has a dedicated control-plane table and lifecycle work has a dedicated durable-action table.
- The worker uses database UTC time for every persisted due-time and eligibility comparison.
- `pin_jobs` remains responsible only for provider `submit`, `poll`, `unpin`, and `reconcile` work under pin-lease fences.
- An immutable `object_versions` row identity and sequence are required to keep an action from touching the successor.
- Lifecycle deletion changes metadata and uses existing guarded lease handling; it does not physically unpin content.
- No handwritten route or XML serializer bypasses s3s SigV4 routing and DTO XML serialization.
- Implementation must compile against the locked s3s 0.14 DTO field names before code is accepted.
- Each bucket has zero or one active configuration, with one to 1,000 rules.
- A rule ID is optional. When present it is at most 255 characters and unique within the configuration; rules without IDs use their canonical ordinal only as an internal revision-scoped identity.
- Status is case-sensitive and exactly `Enabled` or `Disabled`.
- The implementation must reject duplicate present IDs, more than 1,000 rules, unknown status, and duplicate or contradictory typed fields with `InvalidRequest`; s3s rejects malformed XML as `MalformedXML` and ignores unknown direct root children before the handler.
- Each rule contains at least one supported action.
- It may use legacy top-level `Prefix`, or a modern `Filter`, but not both.
- If legacy `Prefix` is absent, the modern `Filter` element is required; an explicitly empty `Filter` means all objects.
- Tag matches require exact key and value.
- A key-only tag means an empty value, matching the repository's stored tag model.
- A prefix match is byte-for-byte.
- `ObjectSizeGreaterThan` and `ObjectSizeLessThan` are nonnegative bytes, exclusive, and, when both exist, lower is strictly less than upper.
- A filter checks an object's publication tags, not provider-policy tags or marker data.
- An `Expiration` may specify exactly one of `Date`, `Days`, or `ExpiredObjectDeleteMarker=true`.
- `Date` is a valid ISO 8601 UTC midnight.
- `Days` is a positive whole number.
- `ExpiredObjectDeleteMarker=false` is rejected because it describes no action.
- An expired-object-delete-marker action cannot coexist in the same `Expiration` with `Date` or `Days`, and it cannot use a tag-based filter because a delete marker has no object tags.
- A `NoncurrentVersionExpiration` requires positive whole-number `NoncurrentDays`.
- `NewerNoncurrentVersions` is an optional additional threshold from 1 through 100 and requires an explicit modern `Filter`, as required by AWS.
- When supplied, both the age and newer-version thresholds must be exceeded.
- These numbers apply to public retained content versions and delete markers, ordered by immutable sequence, not to hidden legacy object rows.
- Canonical JSON orders object keys and rule fields deterministically, preserves rule order, represents absent optional values explicitly, and is the sole value stored and re-serialized by GET.
- A replacement resets the cursor under the new revision.
- A delete increments the revision, clears `canonical_json` and scan state, and retains the tombstone so a later PUT advances again; all old-revision actions cancel during revalidation.
- Future publications set both the version `created_at` and `lifecycle_age_started_at` from the same database UTC clock.
- A row that is latest has a NULL noncurrent timestamp.
- The atomic publication transaction writes the database's UTC time to the previously latest row precisely when it demotes that row.
- A promoted row becomes latest and has `became_noncurrent_at = NULL`.
- A legacy non-latest `objects` row without a public `object_versions` row is ambiguous: it remains invisible, is never made public, and is never lifecycle scheduled.
- The down migration is allowed only if no active or tombstoned lifecycle configuration, lifecycle action, non-null `became_noncurrent_at`, or lifecycle age that differs from its original version timestamp exists.
- The evaluator runs from a lifecycle worker registered in `main.rs` with the root cancellation token.
- It is a production worker, not an HTTP request side effect.
- Pages are bounded by a configuration setting with a safe default; one large bucket cannot monopolize a process or a database lock.
- Multiple gateways can race, but only the winning scan lease owner writes the cursor for that epoch.
- An expired lease is reclaimable.
- For `Days` and `NoncurrentDays`, the due boundary is the first UTC midnight after the stated number of full days from the database-recorded creation or `became_noncurrent_at` instant.
- For `Date`, the supplied UTC midnight is due.
- Past-due existing targets are eligible on the next scan.
- Disabled rules produce no new action.
- The worker does not use its host clock to decide eligibility.
- It considers only public `object_versions` records and their associated `objects` content.
- It excludes ambiguous legacy rows.
- The unique idempotency key and a database unique constraint make scan replay harmless.
- An action worker first claims a due action in one transaction, increasing its claim epoch and setting its lease.
- Tags are reread at execution time, not copied from the scan.
- A failed revalidation marks the action `cancelled`.
- An action whose desired state was already reached by a competing exact delete is `succeeded` idempotently.
- A worker whose claim epoch is stale cannot update the record after a new owner claims it.
- No case invokes Kubo `pin_rm`.
- Date/Days sole-marker cleanup and explicit `ExpiredObjectDeleteMarker=true` both use the exact marker-deletion action.
- The explicit form is eligible as soon as the marker is the sole version; the timed form uses the configured marker age boundary.
- `NoncurrentVersionExpiration` is an exact, permanent deletion of that version-row identity, not a simple delete and never a new marker.
- Marker targets have zero size, no object tags, no object ID, and no lease lifecycle.
- PostgreSQL uses row locks and database uniqueness; SQLite uses its single-writer transaction with bounded retry only for documented busy, serialization, or unique-contention cases.
- No read-then-write sequence selects a target outside the transaction fence.
- It never loops indefinitely.
- Public API errors use normal S3 XML and request identifiers.
- Worker faults are not exposed through an S3 request after PUT has succeeded.
- A completed phase may add an accurate README statement that the expiration subset is supported after all listed evidence passes.
- `ROADMAP.md:77` remains unchecked because transition and the remaining lifecycle program phases are not complete.
- `Transition`, `NoncurrentVersionTransition`, and every storage class are non-goals.
- `AbortIncompleteMultipartUpload`, which is the next program project, is a non-goal.
- CORS, Object Lock, legal holds, retention, MFA Delete, replication, IAM, and bucket-policy lifecycle authorization are non-goals.
- Kubo `pin_rm`, garbage collection, general CID reclamation, and changing the existing ETag-as-CID and encryption behavior are non-goals.
- It must not stage, commit, push, tag, or otherwise write Git history without explicit user authorization.

---

## Execution Protocol

- Historical implementation followed Tasks 1-13 through failed final normal #28, bounded post-#28 diagnostics/corrections, and the reviewed post-#28 final normal validation PASS. AWS-only #27's first host process was killed at 30 minutes before NVE; exact owned project/image/root were manually verified and removed. Its reviewed 60-minute rerun passed full control/current/NVE content+marker/timed/EODM and residual-zero cleanup. Historical #28 passed PostgreSQL versioning/lifecycle and E2E, then failed at full `multi_gateway`; cleanup was exact and zero-residual.
- Tasks 1-9 keep each lower-level behavior change on a RED then minimum-GREEN cycle. A RED is valid only when the named assertion fails for the missing behavior, not because the test does not compile for an unrelated reason. Task 10 is an acceptance-only integrated harness/matrix task: every newly added acceptance target is expected GREEN against Tasks 1-9, and any failure is an integration gap repaired and rerun inside Task 10.
- Do not stage or commit after individual tasks. Oracle/Reviewer and one integrated commit remain gated on a complete final normal-run PASS, promoted evidence/docs, and Task 14 current-identity receipts. Never push or tag.
- Keep `Cargo.toml`, `Cargo.lock`, package `0.1.0`, Rust 2024, MSRV 1.92, and s3s 0.14.0 unchanged. Use only already locked crates.
- Use database time for lifecycle age, due boundaries, scan/action leases, claims, retries, and terminal timestamps. Process time is allowed only for polling, timeout, cancellation, and bounded shutdown.
- Preserve and test the locked s3s XML compatibility boundary: malformed XML is `MalformedXML`, and unknown direct root children are ignored before the typed handler. Do not add a handwritten lifecycle route, patch the dependency, or claim rejection of root children discarded by s3s.
- A fresh full multi-gateway diagnostic did not reproduce #28. Exact stability exposed and corrected a test observer streaming flaw; deterministic action/epoch admission, crash reclaim, user priority, and bounded stale retry closed subsequent blockers. Before the fresh post-fix live run, the PostgreSQL engine-clock test received a test-only ±1-second tolerance and passed `3/3`; on 2026-08-31 the candidate passed a fresh complete final normal validation. Reviewer follow-up then found the max-attempt crash corner: an expired `claimed` row at the limit must be reclaimed once so attempt max+1 can replace/clear its orphan, while an exhausted ordinary `pending` row must fail-safe unclaimed. Pre-admission and retry-exhaustion terminal paths now lock claim then bucket and clear only a deterministic same-action token at epoch ≤ current. Claim tests `8`, action execution tests `14`, targeted regressions, full library `880/880`, integration `143/143`, clippy/fmt/diff pass. Main-path final live remains PASS; changed terminal paths are targeted. Evidence/README remain exact PASS, ROADMAP lifecycle remains unchecked, and Task 14 current re-review is open.
- Do not add transition, noncurrent transition, multipart abort, storage class, residency, dual-Kubo, saga, CORS, Object Lock, IAM, replication, reclamation, or GC code.

## Locked s3s 0.14.0 Contract and XML Compatibility Boundary

The repository locks `s3s 0.14.0` without its `minio` feature. Use the non-MinIO generated DTOs exactly:

```rust
async fn delete_bucket_lifecycle(
    &self,
    req: S3Request<DeleteBucketLifecycleInput>,
) -> S3Result<S3Response<DeleteBucketLifecycleOutput>>;

async fn get_bucket_lifecycle_configuration(
    &self,
    req: S3Request<GetBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>>;

async fn put_bucket_lifecycle_configuration(
    &self,
    req: S3Request<PutBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>>;
```

- `DeleteBucketLifecycleInput { bucket: String, expected_bucket_owner: Option<String> }`; `DeleteBucketLifecycleOutput {}`.
- `GetBucketLifecycleConfigurationInput { bucket, expected_bucket_owner }`; `GetBucketLifecycleConfigurationOutput { rules: Option<Vec<LifecycleRule>>, transition_default_minimum_object_size: Option<TransitionDefaultMinimumObjectSize> }`.
- `PutBucketLifecycleConfigurationInput { bucket, checksum_algorithm, expected_bucket_owner, lifecycle_configuration: Option<BucketLifecycleConfiguration>, transition_default_minimum_object_size }`; output contains only `transition_default_minimum_object_size`.
- `BucketLifecycleConfiguration { rules: Vec<LifecycleRule> }`.
- `LifecycleRule { abort_incomplete_multipart_upload, expiration, filter, id, noncurrent_version_expiration, noncurrent_version_transitions, prefix, status, transitions }`.
- `LifecycleRuleFilter { and, object_size_greater_than: Option<i64>, object_size_less_than: Option<i64>, prefix, tag }` and `LifecycleRuleAndOperator { object_size_greater_than, object_size_less_than, prefix, tags: Option<Vec<Tag>> }`.
- `LifecycleExpiration { date: Option<Timestamp>, days: Option<i32>, expired_object_delete_marker: Option<bool> }`.
- `NoncurrentVersionExpiration { newer_noncurrent_versions: Option<i32>, noncurrent_days: Option<i32> }`.
- `Tag { key: Option<String>, value: Option<String> }`. No lifecycle DTO field is boxed.
- s3s fixes successful HTTP statuses at DELETE 204, GET 200, and PUT 200 and owns XML response serialization.

The authoritative design accepts the installed framework-native boundary: s3s
0.14 converts malformed or structurally invalid nested XML to `MalformedXML`
before the `S3` trait method and skips unknown direct children of the root
`LifecycleConfiguration`. Task 3 locks that behavior with signed tests. The
gateway validates every supported typed field and unsupported action that
reaches the handler, but it does not add a handwritten authenticated raw-body
route or claim strict rejection of XML that s3s has already discarded.

## File Map and Exact Changed-Path Allowlist

The final boundary check rejects every changed or untracked path outside this list.

**Create:**
- `docs/superpowers/plans/2026-08-26-lifecycle-expiration.md` — this plan.
- `src/store/entities/bucket_lifecycle_config.rs` — one SeaORM configuration/tombstone/scan-lease row.
- `src/store/entities/lifecycle_action.rs` — one SeaORM durable-action row.
- `src/store/migrations/m20260826_000001_lifecycle_expiration.rs` — tenth migration, backfill, injection, and fail-closed down.
- `src/store/database_clock.rs` — SQLite/PostgreSQL `database_now` and clock predicates.
- `src/store/lifecycle_config.rs` — atomic revisioned PUT/GET/DELETE and scan lease storage.
- `src/store/lifecycle_scan.rs` — stable cursor codec and bounded immutable-version candidate pages.
- `src/store/lifecycle_action.rs` — idempotent insert, claim, retry, fenced terminal updates.
- `src/lifecycle/mod.rs` — lifecycle exports only.
- `src/lifecycle/model.rs` — canonical rule/filter/action and shared target/result types.
- `src/lifecycle/config.rs` — s3s DTO validation, canonical JSON, and DTO projection.
- `src/lifecycle/filter.rs` — pure prefix/tag/size matching.
- `src/lifecycle/evaluator.rs` — UTC boundary calculation, precedence, and scheduling.
- `src/lifecycle/actions.rs` — claim revalidation, action/epoch-owned admission, crash reclaim, guarded execution, classification.
- `src/lifecycle/worker.rs` — production evaluator/action loops, bounded shutdown handle, and a doc-hidden deterministic after-claim integration-test seam.
- `src/s3/ops/lifecycle.rs` — three s3s operation handlers and expected-owner enforcement.
- `tests/support/lifecycle.rs` — deterministic lifecycle harness and worker controls.
- `tests/postgres_lifecycle.rs` — PostgreSQL migration/clock/rollback and two-worker abort/reclaim fencing, plus Task 13's test-only retained-immutable-object assertion correction; production lifecycle semantics remain unchanged.
- `tests/compose.lifecycle-expiration-validation.yml` — isolated two-gateway PostgreSQL/Kubo/AWS validation topology.
- `scripts/lifecycle-expiration-smoke.ps1` — opt-in, no-pull, owned-resource AWS evidence runner with exact-count receipts, existing full-target diagnostic mode, and a dedicated mutually-exclusive exact lifecycle-race diagnostic mode.
- `docs/lifecycle-expiration-evidence-2026-08-26.log` — sanitized evidence, initially `NOT RUN`.

**Modify:**
- `src/store/entities/object_version.rs`, `src/store/entities/mod.rs` — lifecycle timestamps and entity registration.
- `src/store/migrations/m20260825_000001_object_versioning.rs` — Task 1 test-only task-local injection plumbing; migration-9 production SQL, transaction, and behavior remain byte-for-byte unchanged.
- `src/store/migrations/mod.rs`, `src/store/mod.rs` — migration/store module registration and schema inventory.
- `src/store/object_version.rs` — Task 1 ActiveModel compatibility, then Task 4 database-clock demotion/promotion timestamps and immutable row identity.
- `src/store/bucket.rs` — Task 1-only compatibility updates for object-version ActiveModel fixtures/initializers; no bucket or lifecycle semantic change.
- `src/store/pinning/publication.rs`, `src/store/pinning/publication/tests.rs` — database-time publication and privileged guarded lifecycle mutation.
- `src/store/import/ownership.rs` — bucket-locked deterministic lifecycle action/epoch tokens, same-claim resume, reclaim takeover, and non-superseding exact/prefix ownership checks.
- `src/pinning/worker.rs` — mechanical clippy-only conversion of one predeclared `match` assignment into a match expression; no pinning behavior changes.
- `src/s3/ops/versioning.rs` — mechanical clippy-only removal of a redundant test-module wildcard DTO import; no production or test behavior changes.
- `src/import/model.rs` — `SupersedeReason::LifecycleExpiration` admission reason.
- `src/lib.rs` — export `lifecycle`.
- `src/config.rs`, `config.example.toml` — bounded lifecycle worker settings.
- `src/main.rs` — start and drain the lifecycle worker with the root token.
- `src/error.rs` — `InvalidRequest`, `NoSuchLifecycleConfiguration`, and redacted lifecycle failure mappings.
- `src/s3/handler.rs`, `src/s3/ops/mod.rs`, `src/s3/ops/bucket.rs` — exact trait delegations, module registration, and storage of authenticated bucket owner on new buckets.
- `src/s3/ops/tagging.rs` — Task 13 test-only hidden-null fixture repair, panic-safe gate installation, and bounded gate-versus-task completion; production tagging semantics remain unchanged.
- `src/s3/ops/object.rs` — Task 13 test-only direct fixture installs its matching hidden unversioned object-version row; production object semantics remain unchanged.
- `src/s3/ops/multipart.rs` — Task 13 test-only direct fixture installs its matching hidden unversioned object-version row; production multipart semantics remain unchanged.
- `tests/integration.rs` — signed SQLite API, action, encryption, tag, lease, shared-CID, and no-`pin_rm` matrix.
- `tests/postgres_versioning.rs` — Task 13 test-only migration-9-focused PostgreSQL inventory/fixture compatibility with migration 10; production migrations and semantics remain unchanged.
- `tests/e2e.rs` — live gateway lifecycle API/action checks used by the isolated runner.
- `tests/multi_gateway.rs` — signed cross-replica lifecycle-configuration visibility and publication/action race, fixed safe stage receipts, and conditional bounded public-API cleanup convergence; it never claims process or database control.
- `tests/multi-gateway.Tests.ps1` — static contract separating the PostgreSQL in-process crash proof from endpoint-only scenarios and locking safe-stage/bounded public-API cleanup-convergence source shape.
- `tests/cluster.Tests.ps1` — Task 13 refreshes exactly the two protected hash values for the test-only object/multipart fixture edits; parser/static assertions remain unchanged.
- `tests/client-smoke.Tests.ps1` — Docker-free AST/source/evidence/docs contract, including mutually-exclusive runner modes and accepted/rejected exact lifecycle-race pass/failure/stage fixtures.
- `tests/support/mod.rs` — register the focused lifecycle integration harness.
- `README.md` — supported expiration subset only after all evidence passes.

**Approved inputs, never edit:**
- `docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md`.
- `docs/superpowers/specs/2026-08-26-lifecycle-program-design.md`.

**Protected and expected unchanged:**
- `ROADMAP.md` including unchecked line 77; `Cargo.toml`; `Cargo.lock`; `.github/workflows/release-validation.yml`; all production Compose files; object-versioning evidence; Kubo entrypoints; encryption format code; provider clients; and pin-job schema.

**Ignored verification-only temporary:**
- `.debug-journal.md` contains Task 13 diagnosis/correction/failure context and remains ignored verification-only state throughout the evidence-led rounds. It is removed only after a complete final normal-run PASS opens Task 14; it is never a product artifact, evidence receipt, allowlist/manifest entry, staged path, or commit path.

## Shared Interfaces

Use these names and shapes consistently. A compile-forced change requires one plan revision updating every producer and consumer before implementation resumes.

```rust
// src/lifecycle/model.rs
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalLifecycleConfiguration {
    pub schema_version: u8,
    pub rules: Vec<CanonicalLifecycleRule>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalLifecycleRule {
    pub id: Option<String>,
    pub status: LifecycleRuleStatus,
    pub selector: CanonicalRuleSelector,
    pub expiration: Option<CurrentExpiration>,
    pub noncurrent_version_expiration: Option<NoncurrentExpiration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRuleStatus { Enabled, Disabled }

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalRuleSelector {
    LegacyPrefix { prefix: String },
    Modern { filter: CanonicalFilter },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CanonicalFilter {
    All,
    Prefix { prefix: String },
    Tag { tag: CanonicalTag },
    ObjectSizeGreaterThan { bytes: i64 },
    ObjectSizeLessThan { bytes: i64 },
    And {
        prefix: Option<String>,
        tags: Vec<CanonicalTag>,
        object_size_greater_than: Option<i64>,
        object_size_less_than: Option<i64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct CanonicalTag { pub key: String, pub value: String }

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CurrentExpiration {
    Date { utc_midnight: chrono::DateTime<chrono::Utc> },
    Days { days: u32 },
    ExpiredObjectDeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NoncurrentExpiration {
    pub noncurrent_days: u32,
    pub newer_noncurrent_versions: Option<u16>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuleIdentity { Id(String), Ordinal(u16) }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleActionKind { ExpireCurrent, ExpireNoncurrent, DeleteExpiredMarker }

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionTargetIdentity {
    pub bucket: String,
    pub key: String,
    pub version_row_id: String,
    pub public_version_id: crate::store::object_version::PublicVersionId,
    pub kind: crate::store::object_version::VersionKind,
    pub object_id: Option<String>,
    pub sequence: i64,
}

#[derive(Clone, Debug)]
pub struct LifecycleCandidate {
    pub target: VersionTargetIdentity,
    pub is_latest: bool,
    pub size: i64,
    pub lifecycle_age_started_at: chrono::DateTime<chrono::Utc>,
    pub became_noncurrent_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug)]
pub struct LifecycleCandidatePage {
    pub candidates: Vec<LifecycleCandidate>,
    pub next_cursor: Option<LifecycleScanCursor>,
    pub cycle_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LifecycleScanSource { Current, Noncurrent }

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LifecycleScanCursor {
    pub source: LifecycleScanSource,
    pub bucket: String,
    pub key: String,
    pub sequence: i64,
    pub version_row_id: String,
}

#[derive(Clone, Debug)]
pub struct ClaimedLifecycleScan {
    pub bucket: String,
    pub config_revision: i64,
    pub canonical_json: String,
    pub cursor: Option<LifecycleScanCursor>,
    pub lease_epoch: i64,
    pub database_now: chrono::DateTime<chrono::Utc>,
    pub lease_until: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct NewLifecycleAction {
    pub idempotency_key: String,
    pub bucket: String,
    pub config_revision: i64,
    pub rule_identity: RuleIdentity,
    pub action_kind: LifecycleActionKind,
    pub target: VersionTargetIdentity,
    pub due_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
pub struct ClaimedLifecycleAction {
    pub action: crate::store::entities::lifecycle_action::Model,
    pub claim_epoch: i64,
    pub worker_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedLifecycleExecutionResult {
    Applied(crate::store::object_version::DeleteVersionResult),
    AlreadySatisfied,
    Stale,
}
```

Exact shared function signatures:

```rust
pub async fn database_now<C: sea_orm::ConnectionTrait>(
    db: &C,
) -> crate::error::AppResult<chrono::DateTime<chrono::Utc>>;

pub fn validate_and_canonicalize(
    input: s3s::dto::BucketLifecycleConfiguration,
) -> crate::error::AppResult<CanonicalLifecycleConfiguration>;
pub fn canonical_json(config: &CanonicalLifecycleConfiguration) -> crate::error::AppResult<String>;
pub fn from_canonical_json(json: &str) -> crate::error::AppResult<CanonicalLifecycleConfiguration>;
pub fn to_s3_rules(config: &CanonicalLifecycleConfiguration) -> crate::error::AppResult<Vec<s3s::dto::LifecycleRule>>;
pub fn matches_filter(filter: &CanonicalRuleSelector, key: &str, size: i64, tags: &[crate::pinning::tags::ObjectTag]) -> bool;
pub fn next_utc_midnight_after_full_days(start: chrono::DateTime<chrono::Utc>, days: u32) -> crate::error::AppResult<chrono::DateTime<chrono::Utc>>;

pub async fn put_configuration(db: &sea_orm::DatabaseConnection, bucket: &str, canonical_json: &str) -> crate::error::AppResult<i64>;
pub async fn get_configuration<C: sea_orm::ConnectionTrait>(db: &C, bucket: &str) -> crate::error::AppResult<crate::store::entities::bucket_lifecycle_config::Model>;
pub async fn delete_configuration(db: &sea_orm::DatabaseConnection, bucket: &str) -> crate::error::AppResult<i64>;
pub async fn claim_next_scan(db: &sea_orm::DatabaseConnection, lease_for: chrono::Duration) -> crate::error::AppResult<Option<ClaimedLifecycleScan>>;
pub async fn finish_scan_page(db: &sea_orm::DatabaseConnection, claim: &ClaimedLifecycleScan, next_cursor: Option<&LifecycleScanCursor>, cycle_complete: bool) -> crate::error::AppResult<bool>;
pub async fn scan_candidate_page<C: sea_orm::ConnectionTrait>(db: &C, bucket: &str, cursor: Option<&LifecycleScanCursor>, limit: u64) -> crate::error::AppResult<LifecycleCandidatePage>;

pub async fn insert_idempotent<C: sea_orm::ConnectionTrait>(db: &C, action: NewLifecycleAction, now: chrono::DateTime<chrono::Utc>) -> crate::error::AppResult<bool>;
pub async fn claim_due(db: &sea_orm::DatabaseConnection, worker_id: &str, lease_for: chrono::Duration, limit: u64) -> crate::error::AppResult<Vec<ClaimedLifecycleAction>>;
pub async fn mark_succeeded<C: sea_orm::ConnectionTrait>(db: &C, claim: &ClaimedLifecycleAction, now: chrono::DateTime<chrono::Utc>) -> crate::error::AppResult<bool>;
pub async fn mark_cancelled<C: sea_orm::ConnectionTrait>(db: &C, claim: &ClaimedLifecycleAction, now: chrono::DateTime<chrono::Utc>, failure_class: &str) -> crate::error::AppResult<bool>;
pub async fn schedule_retry<C: sea_orm::ConnectionTrait>(db: &C, claim: &ClaimedLifecycleAction, now: chrono::DateTime<chrono::Utc>, next_attempt_at: chrono::DateTime<chrono::Utc>, failure_class: &str) -> crate::error::AppResult<bool>;
pub async fn mark_failed_safe<C: sea_orm::ConnectionTrait>(db: &C, claim: &ClaimedLifecycleAction, now: chrono::DateTime<chrono::Utc>, failure_class: &str) -> crate::error::AppResult<bool>;
pub async fn lock_claim_for_execution<C: sea_orm::ConnectionTrait>(db: &C, claim: &ClaimedLifecycleAction) -> crate::error::AppResult<Option<crate::store::entities::lifecycle_action::Model>>;

pub async fn try_admit_lifecycle_mutation<C: sea_orm::ConnectionTrait + sea_orm::TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::AppResult<Option<crate::store::import::ownership::StandardMutationGuard>>;

pub async fn clear_lifecycle_mutation_if_owned<C: sea_orm::ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::AppResult<bool>;

pub(crate) async fn execute_lifecycle_delete_guarded(
    txn: &sea_orm::DatabaseTransaction,
    target: &VersionTargetIdentity,
    action_kind: LifecycleActionKind,
    guard: &crate::store::import::ownership::StandardMutationGuard,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::AppResult<GuardedLifecycleExecutionResult>;
```

### Task 1: Add the tenth migration, entities, database clock, and fail-closed rollback

**Files:**
- Create: `src/store/entities/bucket_lifecycle_config.rs`
- Create: `src/store/entities/lifecycle_action.rs`
- Create: `src/store/migrations/m20260826_000001_lifecycle_expiration.rs`
- Create: `src/store/database_clock.rs`
- Create: `tests/postgres_lifecycle.rs`
- Modify: `src/store/entities/object_version.rs`
- Modify: `src/store/entities/mod.rs`
- Modify: `src/store/migrations/m20260825_000001_object_versioning.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/object_version.rs`
- Modify: `src/store/bucket.rs`
- Test: migration file and the initial migration/clock cases in `tests/postgres_lifecycle.rs`

**Interfaces:**
- Consumes: ninth migration `m20260825_000001_object_versioning`, both migrations' test-only failure/count-mismatch injection hooks, PostgreSQL advisory migration transaction, SQLite busy timeout, `object_versions.created_at`, and immutable sequence order.
- Produces: two entities, `lifecycle_age_started_at`, `became_noncurrent_at`, compile-compatible existing ActiveModel literals, task-local scoped injection for migrations 9/10, tenth migration registration, `database_now`, and all required indexes/checks/backfill/down guards.

- [ ] **Step 1: Retain the completed schema REDs and add causal migration-injection isolation REDs**

Add tests named `lifecycle_migration_creates_config_action_tables_indexes_and_checks`, `lifecycle_migration_backfills_successor_noncurrent_time`, `lifecycle_migration_keeps_latest_null_and_legacy_nonlatest_hidden`, `lifecycle_migration_count_mismatch_rolls_back`, `lifecycle_migration_insert_failure_rolls_back`, `lifecycle_down_allows_only_pristine_backfill`, and `lifecycle_down_refuses_each_durable_state`. Assert `database_now` falls between two process observations but is returned by the engine and assert PostgreSQL SQL contains `clock_timestamp()` while SQLite lease predicates contain `julianday('now')`.

Also add `object_versioning_injection_is_task_local` in migration 9 and `lifecycle_injection_is_task_local` in migration 10. For the RED revision, wrap each existing global setter/reset guard behind the same async `with_test_injection(mode, future)` signature used by the final code, so the tests compile while retaining the confirmed leaking behavior. Each regression creates separate SQLite databases, enters an injected failure scope in task A, uses a Barrier plus Notify to hold A inside that scope, runs an ordinary migration on database B in another Tokio task, then releases A. Under the process-global `AtomicU8` design the ordinary migration receives the exact injected error and the regression fails deterministically; under task-local scope the ordinary migration succeeds while only A receives the expected injected error.

```powershell
cargo test --lib store::migrations::m20260825_000001_object_versioning::tests::object_versioning_injection_is_task_local -- --exact --nocapture
$redMigration9Isolation = $LASTEXITCODE
cargo test --lib store::migrations::m20260826_000001_lifecycle_expiration::tests::lifecycle_injection_is_task_local -- --exact --nocapture
$redMigration10Isolation = $LASTEXITCODE
if ($redMigration9Isolation -eq 0 -or $redMigration10Isolation -eq 0) {
    throw "Migration-injection isolation RED was not causal"
}
```

The original schema/backfill/clock/down-refusal RED evidence remains part of Task 1's completed base work. For this bounded correction, both exact isolation regressions fail under the confirmed process-global design because an ordinary concurrent migration observes another task's injected mode; a changing unrelated neighbor is not needed to reproduce the defect.

- [ ] **Step 2: Define the exact SQLite/PostgreSQL schema and checks**

Generate backend timestamp types exactly as migration 9 does and execute these logical statements inside an explicit SQLite/PostgreSQL transaction; PostgreSQL remains additionally protected by `run_postgres_migrations`' outer advisory-lock transaction:

```sql
ALTER TABLE object_versions ADD COLUMN lifecycle_age_started_at TIMESTAMPTZ;
ALTER TABLE object_versions ADD COLUMN became_noncurrent_at TIMESTAMPTZ;

CREATE TABLE bucket_lifecycle_configs (
    bucket TEXT PRIMARY KEY NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    canonical_json TEXT CHECK (canonical_json IS NULL OR json_valid(canonical_json)),
    revision BIGINT NOT NULL CHECK (revision > 0),
    scan_cursor TEXT,
    scan_lease_epoch BIGINT NOT NULL DEFAULT 0 CHECK (scan_lease_epoch >= 0),
    scan_lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_scanned_at TIMESTAMPTZ
);

CREATE TABLE lifecycle_actions (
    id TEXT PRIMARY KEY NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    config_revision BIGINT NOT NULL CHECK (config_revision > 0),
    rule_id TEXT NOT NULL,
    action_kind TEXT NOT NULL CHECK (action_kind IN ('expire_current','expire_noncurrent','delete_expired_marker')),
    target_version_row_id TEXT NOT NULL,
    target_public_version_id TEXT NOT NULL,
    target_object_id TEXT,
    target_sequence BIGINT NOT NULL CHECK (target_sequence >= 0),
    due_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','claimed','succeeded','cancelled','failed_safe')),
    attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    claim_epoch BIGINT NOT NULL DEFAULT 0 CHECK (claim_epoch >= 0),
    lease_until TIMESTAMPTZ,
    claimed_by TEXT,
    failure_class TEXT,
    last_error_redacted TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    CONSTRAINT ck_lifecycle_action_claim CHECK (
        (state = 'claimed' AND lease_until IS NOT NULL AND claimed_by IS NOT NULL) OR
        (state <> 'claimed' AND lease_until IS NULL AND claimed_by IS NULL)
    ),
    CONSTRAINT ck_lifecycle_action_terminal CHECK (
        (state IN ('succeeded','cancelled','failed_safe') AND finished_at IS NOT NULL) OR
        (state IN ('pending','claimed') AND finished_at IS NULL)
    )
);

CREATE INDEX idx_lifecycle_actions_due ON lifecycle_actions(state, next_attempt_at, due_at, id);
CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id);
CREATE INDEX idx_lifecycle_actions_bucket_revision ON lifecycle_actions(bucket, config_revision, id);
CREATE INDEX idx_lifecycle_actions_target ON lifecycle_actions(bucket, object_key, target_version_row_id);
```

Use `TIMESTAMP` instead of `TIMESTAMPTZ` on SQLite. On PostgreSQL replace the SQLite JSON expression with `canonical_json TEXT CHECK (canonical_json IS NULL OR pg_input_is_valid(canonical_json, 'jsonb'))`; retain TEXT rather than JSONB so the exact canonical bytes and field order survive storage. Tests lock migration-8 safety style: valid canonical JSON is accepted, invalid text is rejected with a sanitized migration/application error, and no branch calls `pg_input_error_info`. Add PostgreSQL named constraints after `ALTER TABLE` where SQLite requires inline checks.

Define SeaORM models with these exact persisted fields; `object_version::Model.lifecycle_age_started_at` is non-optional after migration and only `became_noncurrent_at` is nullable:

```rust
// src/store/entities/bucket_lifecycle_config.rs
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "bucket_lifecycle_configs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bucket: String,
    pub canonical_json: Option<String>,
    pub revision: i64,
    pub scan_cursor: Option<String>,
    pub scan_lease_epoch: i64,
    pub scan_lease_until: Option<DateTimeUtc>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub last_scanned_at: Option<DateTimeUtc>,
}
```

```rust
// src/store/entities/lifecycle_action.rs
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "lifecycle_actions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub idempotency_key: String,
    pub bucket: String,
    pub object_key: String,
    pub config_revision: i64,
    pub rule_id: String,
    pub action_kind: String,
    pub target_version_row_id: String,
    pub target_public_version_id: String,
    pub target_object_id: Option<String>,
    pub target_sequence: i64,
    pub due_at: DateTimeUtc,
    pub state: String,
    pub attempts: i64,
    pub next_attempt_at: DateTimeUtc,
    pub claim_epoch: i64,
    pub lease_until: Option<DateTimeUtc>,
    pub claimed_by: Option<String>,
    pub failure_class: Option<String>,
    pub last_error_redacted: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub finished_at: Option<DateTimeUtc>,
}
```

Add these exact fields to `src/store/entities/object_version.rs::Model` immediately after `is_latest` and before `created_at`:

```rust
pub lifecycle_age_started_at: DateTimeUtc,
pub became_noncurrent_at: Option<DateTimeUtc>,
```

Update every existing `object_version::ActiveModel` literal in `src/store/object_version.rs` and `src/store/bucket.rs` only as required by the new non-null field:

```rust
lifecycle_age_started_at: Set(now),
became_noncurrent_at: Set(None),
```

Use the same existing `now` value already assigned to each literal's `created_at`; do not introduce a new clock read, alter publication/demotion/promotion behavior, or change bucket semantics in Task 1. This is a schema-compilation compatibility update only: `src/store/object_version.rs::insert_version_row` and the three `src/store/bucket.rs` object-version fixtures gain the two required fields. Task 4 later obtains database time inside the publication transaction and switches authoritative publication, demotion, and promotion timestamps there.

- [ ] **Step 3: Implement exact successor backfill, count verification, and injected rollback**

Backfill inside the migration transaction with this correlation; a successor is the next higher public indexed sequence, regardless of content/marker kind:

```sql
UPDATE object_versions AS current
SET lifecycle_age_started_at = current.created_at,
    became_noncurrent_at = (
        SELECT successor.created_at
        FROM object_versions AS successor
        WHERE successor.bucket = current.bucket
          AND successor.key = current.key
          AND successor.sequence > current.sequence
        ORDER BY successor.sequence ASC, successor.id ASC
        LIMIT 1
    );
```

Compare selected row count with non-null `lifecycle_age_started_at`, and compare `EXISTS(next higher sequence)` count with non-null `became_noncurrent_at`. Keep statement-failure and count-mismatch injection test-only, but replace migration 9's `BACKFILL_INSERT_INJECTION`/mutex/reset guard and migration 10's `LIFECYCLE_INJECTION`/mutex/reset helper with one Tokio task-local mode per migration:

```rust
#[cfg(test)]
tokio::task_local! {
    static TEST_INJECTION_MODE: u8;
}

#[cfg(test)]
async fn with_test_injection<F, T>(mode: u8, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    TEST_INJECTION_MODE.scope(mode, future).await
}

fn test_injection_mode() -> u8 {
    #[cfg(test)]
    {
        return TEST_INJECTION_MODE.try_with(|mode| *mode).unwrap_or(0);
    }
    #[cfg(not(test))]
    {
        0
    }
}
```

Use migration-specific task-local/static/helper names so the files do not share a mode. Existing injected tests call `with_test_injection(mode, migrate(...))`; ordinary migrations receive mode `0` even while another task's scope is active. Remove the global `AtomicU8`, focused mutexes, setters, and reset guards. Preserve migration-9 and migration-10 production SQL, statement order, transactions, exact injected error strings, and non-test behavior byte-for-byte except the `cfg(test)` mode accessor plumbing. Rollback must still remove both new tables/columns and leave migration 9 data and marker intact.

After verification, PostgreSQL executes `ALTER TABLE object_versions ALTER COLUMN lifecycle_age_started_at SET NOT NULL`. SQLite rebuilds `object_versions` in the same transaction into a table with the migration-9 columns/checks plus `lifecycle_age_started_at TIMESTAMP NOT NULL` and nullable `became_noncurrent_at`, copies every row, drops the old table, renames the replacement, and recreates all seven migration-9 indexes verbatim. The rebuild count must equal the pre-rebuild count; injected copy/count failures roll back the entire tenth migration.

- [ ] **Step 4: Implement `database_now` and execution-time predicates**

Use exact engine expressions and reject unsupported engines:

```rust
pub async fn database_now<C: ConnectionTrait>(db: &C) -> AppResult<DateTime<Utc>> {
    let backend = db.get_database_backend();
    let sql = match backend {
        DatabaseBackend::Postgres => "SELECT clock_timestamp() AS now",
        DatabaseBackend::Sqlite => "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now') AS now",
        _ => return Err(AppError::Internal("lifecycle requires SQLite or PostgreSQL".to_owned())),
    };
    let row = db.query_one(Statement::from_string(backend, sql)).await?
        .ok_or_else(|| AppError::Internal("database clock returned no row".to_owned()))?;
    row.try_get("", "now")
        .map_err(|_| AppError::Internal("database clock result is invalid".to_owned()))
}
```

Every scan/action claim query embeds `clock_timestamp()` on PostgreSQL and `julianday(lease_column) <= julianday('now')` on SQLite; never use `CURRENT_TIMESTAMP` for a claim fence. Keep these expressions private to the store modules so no caller can supply an unchecked column expression.

- [ ] **Step 5: Implement fail-closed down and entity registration**

Refuse down when any row exists in either lifecycle table, any `became_noncurrent_at` is non-null, or any lifecycle age differs from `created_at`; equality must be engine-normalized (`IS DISTINCT FROM` on PostgreSQL, `julianday(...) <> julianday(...)` on SQLite). On pristine data, drop action indexes/table, config table, then the two columns. Register migration tenth and update the schema inventory from sixteen to eighteen tables.

- [ ] **Step 6: Make Task 1 GREEN under focused and repeated standard-parallel execution**

```powershell
cargo test --lib lifecycle_migration -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle migration tests failed" }
cargo test --lib database_clock -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Database clock tests failed" }
cargo test --lib store::migrations::m20260825_000001_object_versioning::tests::object_versioning_injection_is_task_local -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration 9 task-local injection regression failed" }
cargo test --lib store::migrations::m20260826_000001_lifecycle_expiration::tests::lifecycle_injection_is_task_local -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration 10 task-local injection regression failed" }
cargo test --lib store::migrations::m20260825_000001_object_versioning::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration 9 focused scope failed" }
cargo test --lib store::migrations::m20260826_000001_lifecycle_expiration::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration 10 focused scope failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "First standard-parallel library suite failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Second standard-parallel library suite failed" }
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL lifecycle tests did not compile" }
```

Expected: both exact isolation regressions pass, both migration scopes pass, both unmodified standard-parallel `cargo test --lib` runs pass without injected errors leaking to changing neighbor tests, the PostgreSQL test binary compiles, and no PostgreSQL service is contacted. Do not serialize the suite with `--test-threads=1`; the repeated standard-parallel runs are completion evidence for this correction.

### Task 2: Build the canonical rule model, strict DTO validation, JSON round trip, and filter evaluator

**Files:**
- Create: `src/lifecycle/model.rs`
- Create: `src/lifecycle/config.rs`
- Create: `src/lifecycle/filter.rs`
- Create: `src/lifecycle/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/error.rs`

**Interfaces:**
- Consumes: locked s3s lifecycle DTO fields, `ObjectTag`, chrono, serde JSON, and `AppError` redaction.
- Produces: every canonical type and pure function in Shared Interfaces; `AppError::InvalidLifecycleConfiguration` and `AppError::NoSuchLifecycleConfiguration`.

- [ ] **Step 1: Write causal RED validation and round-trip tables**

Add table tests covering 0/1/1,000/1,001 rules; 255/256-character IDs; duplicate IDs; exact statuses; no action; legacy prefix versus modern filter; empty/prefix/tag/size/And filters; duplicate And tag keys; exclusive size boundaries; Date/Days/EODM combinations; `false`; NVE positive days; newer count 1/100 and 0/101; newer count without modern filter; every forbidden action; marker tag-filter rejection; stable JSON; and DTO→canonical→JSON→canonical→DTO equality.

```powershell
cargo test --lib lifecycle::config::tests -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Canonical lifecycle RED unexpectedly passed" }
```

Expected: compile or assertions fail because lifecycle canonicalization is absent.

- [ ] **Step 2: Implement canonical DTO conversion with exact timestamp handling**

Convert s3s `Timestamp` without adding the `time` crate directly:

```rust
fn timestamp_to_utc(value: &Timestamp) -> AppResult<DateTime<Utc>> {
    let mut bytes = Vec::new();
    value.format(TimestampFormat::DateTime, &mut bytes)
        .map_err(|_| invalid("expiration date is invalid"))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| invalid("expiration date is invalid"))?;
    let parsed = DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| invalid("expiration date is invalid"))?;
    if Timestamp::from(std::time::SystemTime::from(parsed)) != value.clone() {
        return Err(invalid("expiration date must use UTC"));
    }
    Ok(parsed)
}

fn utc_to_timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp::from(std::time::SystemTime::from(value))
}
```

Require hour/minute/second/nanosecond all zero for Date. Store `schema_version = 1`; preserve rule order; sort And tags by key; serialize every optional field (do not use `skip_serializing_if`); encode absent rule IDs as `None`, and derive `RuleIdentity::Ordinal(index as u16)` only when scheduling.

- [ ] **Step 3: Implement exhaustive whole-configuration validation**

Count populated filter singleton fields; require 0 only for explicit empty `Filter`, 1 otherwise. Reject any non-empty `transitions`, `noncurrent_version_transitions`, or present `abort_incomplete_multipart_upload` before serialization. Reject present `transition_default_minimum_object_size` in the S3 operation in Task 3 because no transition semantics exist. Use one stable constructor:

```rust
fn invalid(message: &'static str) -> AppError {
    AppError::InvalidLifecycleConfiguration(message.to_owned())
}
```

Map this variant to `S3ErrorCode::InvalidRequest`, HTTP 400, and a stable safe message; map `NoSuchLifecycleConfiguration` to custom code `NoSuchLifecycleConfiguration`, HTTP 404. Backend details remain redacted.

- [ ] **Step 4: Implement exact filter semantics as a pure function**

Use byte-exact Rust string prefix, exact tag key/value, marker inputs `size = 0` and empty tags, and exclusive bounds:

```rust
fn bounds_match(size: i64, greater: Option<i64>, less: Option<i64>) -> bool {
    greater.is_none_or(|bound| size > bound) && less.is_none_or(|bound| size < bound)
}

fn tag_matches(expected: &CanonicalTag, tags: &[ObjectTag]) -> bool {
    tags.iter().any(|actual| actual.key == expected.key && actual.value == expected.value)
}
```

`LegacyPrefix` and modern Prefix share prefix matching; `All` matches; `And` requires every present predicate. A key-only DTO tag canonicalizes to an empty value; an absent key is invalid.

- [ ] **Step 5: Prove deterministic canonical bytes and GET projection**

Assert two semantically equal And tag orders produce the same exact JSON bytes, while rule order remains different. Assert JSON object keys and rule fields are deterministic, all absent fields appear as JSON `null`, and `to_s3_rules` reconstructs legacy Prefix versus explicit empty Filter without inventing transitions or the transition-minimum header.

- [ ] **Step 6: Make Task 2 GREEN and scan for unsupported action leakage**

```powershell
cargo test --lib lifecycle::config::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle canonical configuration tests failed" }
cargo test --lib lifecycle::filter::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle filter tests failed" }
rg -n "Transition|NoncurrentVersionTransition|AbortIncompleteMultipartUpload|storage_class" src/lifecycle
if ($LASTEXITCODE -gt 1) { throw "Unsupported lifecycle action scan failed" }
```

Expected: all canonical/filter tests pass; matches refer only to rejection tests/branches and never to a persisted or executable future action.

### Task 3: Add atomic lifecycle configuration storage and the exact s3s API

**Files:**
- Create: `src/store/lifecycle_config.rs`
- Create: `src/s3/ops/lifecycle.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/s3/handler.rs`
- Modify: `src/s3/ops/mod.rs`
- Modify: `src/s3/ops/bucket.rs`
- Modify: `src/error.rs`
- Test: `src/s3/ops/lifecycle.rs`, `tests/integration.rs`

**Interfaces:**
- Consumes: Task 1 tables/clock, Task 2 canonical functions/errors, authenticated `S3Request.credentials`, bucket owner field, and exact s3s DTOs.
- Produces: atomic revisioned PUT/GET/DELETE, tombstones, expected-owner checks, and exact three handler delegations.

- [ ] **Step 1: Write the signed s3s XML compatibility characterization RED**

Add signed tests `lifecycle_malformed_xml_uses_framework_error_without_write`, `lifecycle_nested_unknown_xml_uses_framework_error_without_write`, and `lifecycle_root_unknown_element_is_ignored_before_typed_handler`. The first two start from a valid saved revision, expect `MalformedXML`, and prove canonical JSON/revision unchanged. The root-unknown test sends a valid supported rule plus one unknown direct root child, expects the supported rule to be accepted, and proves GET contains only the canonical supported model. Send raw XML through the actual `S3ServiceBuilder`, not a direct handler call.

```powershell
cargo test --test integration lifecycle_root_unknown_element_is_ignored_before_typed_handler -- --exact --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle XML characterization RED unexpectedly passed" }
```

Expected: the native lifecycle API is not implemented yet, so the signed characterization test fails for the missing route behavior. The final GREEN assertion must match the framework-native boundary exactly; do not write a custom lifecycle route or serializer.

- [ ] **Step 2: Write causal RED direct-handler storage/API tests**

Add tests for first PUT revision 1, replacement revision 2 with cursor reset, GET canonical DTO projection, absent GET 404, DELETE 204/tombstone revision increment, repeated DELETE increment, later PUT monotonic increment, rejected PUT preserving prior JSON/revision, missing bucket, omitted owner, matching owner, and mismatch 403.

```powershell
cargo test --lib s3::ops::lifecycle::tests -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle API RED unexpectedly passed" }
```

Expected: methods/store module are absent.

- [ ] **Step 3: Implement atomic configuration transactions and monotonic tombstones**

For PUT and DELETE, lock the bucket first (`FOR UPDATE` on PostgreSQL; no-op bucket update on SQLite), obtain `database_now`, lock/select the existing config, compute `revision = previous + 1` with overflow rejection, and upsert. PUT writes complete canonical JSON and clears cursor/scan lease/last scan. DELETE writes `canonical_json = NULL`, clears scan state, preserves `created_at`, and updates `updated_at`. GET returns only non-null configuration; a tombstone is `NoSuchLifecycleConfiguration`.

- [ ] **Step 4: Implement exact owner and S3 operation boundaries**

When creating a new bucket, store `req.credentials.as_ref().map(|value| value.access_key.clone())`; existing nullable owners remain unchanged. For lifecycle operations load the bucket first, then enforce:

```rust
fn verify_expected_owner(bucket_owner: Option<&str>, expected: Option<&str>) -> AppResult<()> {
    match expected {
        None => Ok(()),
        Some(value) if bucket_owner == Some(value) => Ok(()),
        Some(_) => Err(AppError::AccessDenied("expected bucket owner mismatch".to_owned())),
    }
}
```

PUT requires `lifecycle_configuration: Some`, rejects `transition_default_minimum_object_size`, validates before opening the write transaction, then returns `PutBucketLifecycleConfigurationOutput { transition_default_minimum_object_size: None }`. GET returns `rules: Some(...)`; DELETE returns the empty output. Add the three exact delegations to `S3Impl`.

- [ ] **Step 5: Make direct API tests GREEN and prove replacement atomicity under injected failure**

Inject a test-only failure immediately before the upsert/delete write and assert prior JSON/revision/cursor remain unchanged. Also race two PUTs and assert revisions are distinct consecutive values and GET returns exactly one complete canonical document.

- [ ] **Step 6: Run Task 3 API and signed success/error tests**

```powershell
cargo test --lib s3::ops::lifecycle::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle operation tests failed" }
cargo test --test integration lifecycle_ -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Signed lifecycle configuration tests failed" }
```

Expected: PUT/GET/DELETE route through s3s, use normal XML/request IDs, preserve atomic revisions, and honor the documented framework-native XML contract.

### Task 4: Make publication and promotion maintain database-clock lifecycle age and expose stable scan candidates

**Files:**
- Create: `src/store/lifecycle_scan.rs`
- Modify: `src/store/entities/object_version.rs`
- Modify: `src/store/object_version.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Test: `src/store/lifecycle_scan.rs`, publication tests

**Interfaces:**
- Consumes: Task 1 columns/clock, existing publication transaction, object tags, version sequence/current projection.
- Produces: exact lifecycle timestamps, `VersionTargetIdentity`, cursor codec, and bounded current/noncurrent pages.

- [ ] **Step 1: Write causal RED publication-time, demotion, promotion, and cursor tests**

Test that publication reads database time inside its transaction; inserted row has `created_at == lifecycle_age_started_at`; displaced latest gets `became_noncurrent_at == publication_time`; new latest is null; promotion clears noncurrent time; hidden legacy non-latest rows never appear; cursor round-trips and rejects malformed/base64/wrong-bucket values; pages traverse Current then Noncurrent without duplicate or loop.

```powershell
cargo test --lib lifecycle_timestamp -- --nocapture
$redTime = $LASTEXITCODE
cargo test --lib lifecycle_scan -- --nocapture
$redScan = $LASTEXITCODE
if ($redTime -eq 0 -or $redScan -eq 0) { throw "Lifecycle publication/scan RED was not causal" }
```

- [ ] **Step 2: Move authoritative publication timestamps inside the transaction**

Call `database_now(db).await?` after bucket/version frontier locks and use the same value for immutable object `created_at`, version `created_at`, `lifecycle_age_started_at`, demotion `became_noncurrent_at`, lease/tag lifecycle writes, and guard completion. Remove lifecycle-relevant `Utc::now()` calls from `publish_in_transaction`; imported historical caller time does not override lifecycle age.

- [ ] **Step 3: Update version primitives with exact identity and timestamp invariants**

Add `id`, `lifecycle_age_started_at`, and `became_noncurrent_at` to `ResolvedVersion`. Change demotion to set `is_latest = false`, `became_noncurrent_at = now`, and `updated_at = now` in one conditional update. Insert sets lifecycle age and null noncurrent time. Promotion sets `is_latest = true`, `became_noncurrent_at = NULL`, and `updated_at = database now`. Keep exact/public version semantics unchanged.

- [ ] **Step 4: Implement stable cursor encoding and bounded source scans**

Serialize `LifecycleScanCursor` to compact JSON then `base64::engine::general_purpose::URL_SAFE_NO_PAD`; decode with schema/field validation. Query Current rows first (`is_latest = TRUE`) and Noncurrent rows second (`is_latest = FALSE`), each ordered `key ASC, sequence ASC, id ASC`; cursor comparisons use the full tuple. Fill one `LifecycleCandidatePage` up to the configured limit, move from Current to Noncurrent without widening either query, return the last consumed identity as `next_cursor`, and set `cycle_complete` only after Noncurrent is exhausted. Include latest unversioned/null rows but exclude any `objects` row without an `object_versions` identity. Content joins immutable object size/ID; markers use size 0 and no object.

- [ ] **Step 5: Prove publication-path and scan-race behavior**

Cover PutObject, CopyObject, multipart completion, direct ZIP, import, and import ZIP through their common publication primitive. During a paged scan, publish rows before and after the cursor; require no duplicate/infinite loop and require a fresh cycle to discover rows inserted before the old cursor.

- [ ] **Step 6: Make Task 4 GREEN**

```powershell
cargo test --lib lifecycle_timestamp -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle timestamp tests failed" }
cargo test --lib lifecycle_scan -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle scan tests failed" }
cargo test --lib store::pinning::publication::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Publication regressions failed" }
```

### Task 5: Implement scan leases and durable idempotent action claims

**Files:**
- Create: `src/store/lifecycle_action.rs`
- Modify: `src/store/lifecycle_config.rs`
- Test: both store modules and `tests/postgres_lifecycle.rs`

**Interfaces:**
- Consumes: Task 1 tables/clock, Task 4 cursor/targets, config revisions.
- Produces: `claim_next_scan`, `finish_scan_page`, action idempotency, claim epochs, lease recovery, retry and terminal fences.

- [ ] **Step 1: Write causal RED lease, replay, claim, and stale-epoch tests**

Cover one scan winner, expired takeover, stale cursor completion rejection, replacement invalidation, idempotent duplicate action insert, stable due ordering, expired action reclaim, attempts/epoch increments, stale epoch success/retry rejection, max-attempt terminal state, and SQLite unique/busy contention.

```powershell
cargo test --lib lifecycle_claim -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle claim RED unexpectedly passed" }
```

- [ ] **Step 2: Implement scan lease claim and fenced page completion**

In one transaction select the first active config whose lease is null/expired, ordered by portable leading key `CASE WHEN last_scanned_at IS NULL THEN 0 ELSE 1 END`, then `last_scanned_at`, `updated_at`, and `bucket`; lock it on PostgreSQL, increment epoch, and set `scan_lease_until = database_now + lease_for`. Completion updates only `bucket + revision + scan_lease_epoch`; it stores encoded next cursor, sets `last_scanned_at = now`, and clears lease. At cycle completion set cursor null so the next claim restarts Current.

- [ ] **Step 3: Define deterministic action and rule identities**

Persist rule identities as `id:<ID>` or `ordinal:<zero-based-u16>`. Build idempotency bytes from canonical JSON of `(bucket, revision, rule identity, action kind, target row ID, target object ID, sequence, due_at RFC3339)` and store lowercase SHA-256 hex. `insert_idempotent` generates the durable row ID with `uuid::Uuid::new_v4().to_string()` only after validation, then inserts with conflict-do-nothing scoped to `idempotency_key`; no other constraint failure is swallowed. Validate marker targets have no object ID and content targets do.

- [ ] **Step 4: Implement database-clock claim/reclaim with epoch fencing**

Claim rows where `(state = 'pending' AND next_attempt_at <= db_now AND due_at <= db_now) OR (state = 'claimed' AND lease_until <= db_now)`, ordered by `(due_at, id)`. PostgreSQL uses `FOR UPDATE SKIP LOCKED`; SQLite starts a writer transaction and conditional updates each candidate. Set state claimed, increment attempts and claim epoch, set claimed_by/lease. Return the exact post-update model.

- [ ] **Step 5: Implement fenced terminal and retry updates**

Every update filters `id`, `state='claimed'`, `claimed_by`, and `claim_epoch`. Success/cancel/failed-safe clear claim fields and set database `finished_at`; retry returns to pending with database-derived `next_attempt_at`, clears claim fields, and stores only allowlisted failure class plus one fixed redacted message. No method accepts raw backend text.

- [ ] **Step 6: Make Task 5 GREEN and compile PostgreSQL race coverage**

```powershell
cargo test --lib lifecycle_claim -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle claim tests failed" }
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL claim tests did not compile" }
```

### Task 6: Build the database-clock evaluator, UTC boundaries, precedence, and scheduling

**Files:**
- Create: `src/lifecycle/evaluator.rs`
- Modify: `src/lifecycle/model.rs`
- Modify: `src/store/lifecycle_action.rs`
- Test: evaluator module

**Interfaces:**
- Consumes: canonical rules/filter, claimed scan/page, candidates, current tags, newer-noncurrent counts, idempotent insertion.
- Produces: due-boundary calculation and one winning durable action per eligible target/boundary.

- [ ] **Step 1: Write causal RED eligibility and precedence tables**

Use fixed database instants around UTC midnight; test positive Days, Date, NVE age, EODM immediate, past-due existing data, disabled rules, prefix/tag/size, marker no-tags/zero-size, timed marker sole/history cases, NVE content/marker, newer count absent/1/100, exactly N versus more than N, and earliest/permanent-delete precedence.

```powershell
cargo test --lib lifecycle::evaluator::tests -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle evaluator RED unexpectedly passed" }
```

- [ ] **Step 2: Implement strict next-midnight calculation**

Compute `aged = start.checked_add_days(chrono::Days::new(days.into()))`, then the first midnight strictly after `aged`: if `aged` is exactly midnight, advance one date; otherwise use the next date. Build UTC with `and_hms_opt(0,0,0)` and checked conversion. Date actions use their stored UTC midnight unchanged.

- [ ] **Step 3: Evaluate current expiration's three target forms**

For current content emit `ExpireCurrent` at Date/Days due. For a current marker, Date/Days emits `DeleteExpiredMarker` only when the candidate is the sole public row and its marker age is due; EODM emits the same exact marker action immediately only when sole and uses the marker's immutable `lifecycle_age_started_at` as its stable already-past `due_at` rather than scan time. Do not schedule a current marker with retained content or markers.

- [ ] **Step 4: Evaluate noncurrent exact expiration and newer threshold**

Require non-null `became_noncurrent_at`, `is_latest = false`, age due, and filter match. Count newer rows with the same bucket/key, `is_latest = false`, and `sequence > target.sequence`; if N exists require `count > N`. Include content and delete markers in the count and as targets; use empty tags/size zero for markers.

- [ ] **Step 5: Choose winner and insert idempotently**

Collect candidates across all enabled rules, rank exact permanent noncurrent deletion before current marker creation/current expiration, then earlier `due_at`, then rule identity. Insert only the winner for an identical target/due boundary; replay returns `false` without changing the existing action.

- [ ] **Step 6: Make Task 6 GREEN**

```powershell
cargo test --lib lifecycle::evaluator::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle evaluator tests failed" }
cargo test --lib lifecycle_scan -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Evaluator scan integration failed" }
```

### Task 7: Add privileged guarded current-expiration mutation for all versioning states

**Files:**
- Create: `src/lifecycle/actions.rs`
- Modify: `src/import/model.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Modify: `src/store/object_version.rs`

**Interfaces:**
- Consumes: caller-owned `DatabaseTransaction`, exact target identity, pre-admitted `StandardMutationGuard`, caller-supplied database now, action kind, versioning state, and existing guarded simple/exact deletion internals; it does not consume `ClaimedLifecycleAction`.
- Produces: `SupersedeReason::LifecycleExpiration` and low-level caller-transaction-owned `execute_lifecycle_delete_guarded`, which performs only ownership/version mutation and returns `GuardedLifecycleExecutionResult` without reading or writing `lifecycle_actions`.

- [ ] **Step 1: Write causal RED unversioned/enabled/suspended current-expiration tests**

Assert unversioned permanently removes current/null projection; Enabled creates one opaque marker and demotes content with exact noncurrent time; Suspended replaces null content with null marker and ends only displaced ownership. Assert stale row ID/object ID/sequence/current state and a target missing before the final fence return `Stale`; only an exact-delete primitive that reports the desired deletion was concurrently completed after successful target fencing returns `AlreadySatisfied`; no path invokes Kubo. Snapshot `lifecycle_actions` before each direct primitive call and require it byte-for-byte unchanged for `Applied`, `AlreadySatisfied`, `Stale`, and error results.

```powershell
cargo test --lib lifecycle_current_expiration -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Current expiration RED unexpectedly passed" }
```

- [ ] **Step 2: Add lifecycle ownership admission reason and non-superseding admission**

Add `LifecycleExpiration` to `SupersedeReason` with exact string `lifecycle_expiration`. Add `try_admit_lifecycle_mutation` in `src/store/import/ownership.rs`: validate nonempty colon-free `action_id` and positive `claim_epoch`, derive exact token `lifecycle:<action-id>:<claim-epoch>`, and work under the bucket lock. Same action/same epoch resumes the exact token. A higher epoch atomically replaces only an older token for that same action. Exact import/user tokens, another lifecycle action, same-action newer epoch, import prefix ownership, and overlapping standard prefix mutations return `None` without changing ownership. A conflict-free transaction installs and returns the lifecycle guard. `actions.rs` passes the claimed action ID/epoch and maps `None` to `LifecycleAdmissionResult::Temporary`. Ordinary user/import mutation admission remains authoritative and may supersede a lifecycle guard; that stale lifecycle action is bounded-retried without clearing the newer guard.

- [ ] **Step 3: Implement target lock and immutable identity comparison inside the mutation transaction**

The caller supplies an already-open transaction, pre-admitted guard, and database `now`. The primitive never opens a transaction, calls `lock_claim_for_execution`, or obtains/updates a claim epoch. Inside the supplied transaction it locks bucket ownership, verifies the standard mutation guard, locks versioning state, locks the exact `target_version_row_id`, and compares bucket/key/public ID/kind/object ID/sequence/is_latest. A target missing before successful fencing, any immutable field mismatch, a current target demoted behind either a marker or content successor, and a noncurrent promotion return `Stale`. `AlreadySatisfied` is reserved for the exact-delete primitive reporting that the desired state was concurrently reached after this invocation successfully fenced the intended target; it is never inferred from a missing replacement-prone row.

- [ ] **Step 4: Reuse existing current delete semantics without nesting transactions**

Call existing internal `delete_unversioned_current_in_transaction`, Enabled marker installation, or `delete_suspended_current_in_transaction` under the same caller-owned transaction. On `Applied` or `AlreadySatisfied`, finish the standard mutation guard and return the result. On `Stale`, return without touching lifecycle action state so the Task 8 caller can complete the guard and cancel atomically. Never call `mark_succeeded`, `mark_cancelled`, or any other lifecycle-action store function here, and do not call the public transaction-opening `delete_version_with_leases_guarded` from inside this transaction.

- [ ] **Step 5: Prove tags, leases, shared CID, and encryption ownership remain authoritative**

Test plain, SSE-S3, SSE-C, tagged, leased, and two-object shared-CID current targets. Assert only the exact object's tags/leases are ended according to existing behavior, retained versions preserve immutable encryption metadata, the other shared-CID owner remains readable, and wiremock receives zero `/api/v0/pin/rm` calls.

- [ ] **Step 6: Make Task 7 GREEN**

```powershell
cargo test --lib lifecycle_current_expiration -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Current expiration tests failed" }
cargo test --lib store::pinning::publication::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Guarded publication regressions failed" }
```

### Task 8: Implement full action revalidation, exact noncurrent/marker deletion, retries, and idempotent terminal outcomes

**Files:**
- Modify: `src/lifecycle/actions.rs`
- Modify: `src/store/import/ownership.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/lifecycle_action.rs`
- Test: action and publication modules

**Interfaces:**
- Consumes: `ClaimedLifecycleAction`, `lock_claim_for_execution`, current canonical revision, target/tags/filter, conflict evaluator, `try_admit_lifecycle_mutation`, and Task 7's action-store-free `execute_lifecycle_delete_guarded` result.
- Produces: deterministic action/epoch-owned lifecycle admission with crash reclaim and non-superseding conflict handling, claim-fenced revalidated terminal success/cancel in the same final transaction as ownership/version mutation, bounded retry including a user-superseded lifecycle guard, failed-safe, exact noncurrent content/marker deletion, and sole-marker cleanup.

- [ ] **Step 1: Write causal RED revalidation and outcome matrix**

Cover config replace/delete, missing/disabled rule, action removed, prefix/size/tag change, execution-time tag reread, current replacement, noncurrent promotion, target exact-delete, object/sequence mismatch, no-longer-sole marker, changed newer count, conflicting winner, already reached state, database contention, stale epoch, retry exhaustion, and redacted diagnostics.

```powershell
cargo test --lib lifecycle_action_execution -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle action execution RED unexpectedly passed" }
```

- [ ] **Step 2: Revalidate every policy and target field immediately before mutation**

After ownership admission, Task 8 opens one final transaction and calls `lock_claim_for_execution` before any policy read; Task 7's primitive never owns this responsibility. While holding the claim and bucket ownership/guard fences, obtain database `now`, require active canonical config revision, exact rule identity, Enabled status, same action, same target identity, expected current/noncurrent state, database `now >= due_at`, current filter over reread tags/size, sole-marker condition where applicable, NVE newer count, and recomputed conflict winner. Any policy/identity mismatch completes the standard mutation guard and marks the action cancelled in this same transaction.

- [ ] **Step 3: Implement exact permanent content/marker deletion**

For `ExpireNoncurrent`, use exact version-row identity and existing `remove_and_promote`/lease/tag ownership handling; never create a marker. Content ends only its exact object ownership. Marker deletion removes only the exact index row and performs no object, tag, lease, Kubo, SSE, or CID operation. For `DeleteExpiredMarker`, require current and sole at execution, then use the same exact marker deletion.

- [ ] **Step 4: Implement result and failure classification**

After full revalidation, call `execute_lifecycle_delete_guarded` with the same final transaction, target, action kind, guard, and database `now`. Map `Applied` and `AlreadySatisfied` to `mark_succeeded` with the locked claim/epoch before committing. Map primitive `Stale`, a target missing during pre-mutation revalidation, revision/rule/filter/identity/state/conflict changes, a promoted NVE target, and a no-longer-current/no-longer-sole marker to standard-guard completion plus `mark_cancelled` in that same transaction. Assert an injected terminal-store failure rolls back both ownership/version mutation and lifecycle-action state. Map documented SQLite busy/PostgreSQL serialization/temporary admission errors to pending retry using database now; compute `attempt_index = u32::try_from((attempts - 1).max(0)).unwrap_or(62).min(62)` and `delay_secs = max_backoff_secs.min(base_backoff_secs.saturating_mul(1_u64.checked_shl(attempt_index).unwrap_or(u64::MAX)))`. At `attempts >= max_attempts` use `failed_safe`. Store only `database_contention`, `admission_temporarily_unavailable`, `internal_dependency`, or a stable cancellation class.

- [ ] **Step 5: Prove stale workers cannot complete and logs are redacted**

Pause after claim, expire/reclaim under a second worker, then release the first; only the new epoch may write terminal state. Capture logs and require action ID, bucket, key, public version ID, revision, and failure class while excluding SQL, object UUID, raw backend/Kubo/provider response, credentials, SSE-C key/MD5, wrapped key, and body.

- [ ] **Step 6: Make Task 8 GREEN**

```powershell
cargo test --lib lifecycle_action_execution -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle action execution tests failed" }
cargo test --lib lifecycle_current_expiration -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle guarded mutation regressions failed" }
```

### Task 9: Wire bounded lifecycle configuration and the production worker into startup/shutdown

**Files:**
- Create: `src/lifecycle/worker.rs`
- Modify: `src/config.rs`
- Modify: `config.example.toml`
- Modify: `src/main.rs`
- Modify: `src/lifecycle/mod.rs`
- Test: config, worker, and main modules

**Interfaces:**
- Consumes: evaluator/action stores, `Store`, root child token, Task 8 executor.
- Produces: validated settings, evaluator/action loops, `LifecycleWorkerHandle`, production start, cancellation, drain.

- [ ] **Step 1: Write causal RED defaults, validation, startup, crash, and cancellation tests**

Assert defaults `poll_interval_ms=1000`, `scan_page_size=500`, `scan_lease_secs=30`, `action_lease_secs=30`, `worker_concurrency=4`, `max_attempts=8`, `base_backoff_secs=1`, `max_backoff_secs=60`; reject zero/overflow/inverted backoff. Test stop-before-new-claim, in-flight drain, forced stop leaves reclaimable lease, and worker is not started by an HTTP request.

```powershell
cargo test --lib lifecycle_worker -- --nocapture
$redWorker = $LASTEXITCODE
cargo test --bin ipfs-s3-gateway lifecycle -- --nocapture
$redMain = $LASTEXITCODE
if ($redWorker -eq 0 -or $redMain -eq 0) { throw "Lifecycle worker RED was not causal" }
```

- [ ] **Step 2: Add exact raw and validated settings**

Add `[lifecycle]` to config/default/env loading and `config.example.toml`. Environment overrides are `IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS`, `...SCAN_PAGE_SIZE`, `...SCAN_LEASE_SECS`, `...ACTION_LEASE_SECS`, `...WORKER_CONCURRENCY`, `...MAX_ATTEMPTS`, `...BASE_BACKOFF_SECS`, and `...MAX_BACKOFF_SECS`. Use checked chrono conversions; require page/concurrency/attempts positive and base <= max.

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct LifecycleWorkerConfig {
    pub poll_interval_ms: u64,
    pub scan_page_size: u64,
    pub scan_lease_secs: u64,
    pub action_lease_secs: u64,
    pub worker_concurrency: usize,
    pub max_attempts: u64,
    pub base_backoff_secs: u64,
    pub max_backoff_secs: u64,
}

#[derive(Clone, Debug)]
pub struct ValidatedLifecycleConfig {
    pub poll_interval: std::time::Duration,
    pub scan_page_size: u64,
    pub scan_lease: chrono::Duration,
    pub action_lease: chrono::Duration,
    pub worker_concurrency: usize,
    pub max_attempts: i64,
    pub base_backoff_secs: u64,
    pub max_backoff_secs: u64,
}
```

Implement `Default` with the locked values from Step 1 and `TryFrom<LifecycleWorkerConfig>` with `u64 -> i64` checked conversions before constructing chrono durations.

- [ ] **Step 3: Implement evaluator and action loops with database time**

Each delayed tick first observes cancellation. Evaluator claims one config scan, reads at most page size, schedules actions, and fences cursor completion. Action loop claims at most available concurrency and spawns JoinSet tasks. Every claim/due/retry timestamp comes from store database-clock calls; `tokio::time` is only polling and shutdown timeout.

- [ ] **Step 4: Implement handle and bounded drain**

```rust
pub struct LifecycleWorkerHandle {
    cancellation: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

pub fn start_worker(store: Store, config: ValidatedLifecycleConfig, parent: CancellationToken) -> LifecycleWorkerHandle;

impl LifecycleWorkerHandle {
    pub async fn shutdown(self, grace: Duration) {
        self.cancellation.cancel();
        let mut join = self.join;
        if tokio::time::timeout(grace, &mut join).await.is_err() {
            join.abort();
            let _ = join.await;
        }
    }
}
```

After cancellation stop scans/claims, then drain active action tasks; forced abort leaves durable claimed rows for lease expiry.

- [ ] **Step 5: Start and join lifecycle with existing workers**

Validate lifecycle config before binding. Start it with `shutdown.child_token()` beside pinning/import. Add its shutdown future to the existing 30-second `tokio::join!`. A worker failure is logged and must not turn a completed PUT into an HTTP failure.

- [ ] **Step 6: Make Task 9 GREEN**

```powershell
cargo test --lib lifecycle_worker -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle worker tests failed" }
cargo test --bin ipfs-s3-gateway lifecycle -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle main wiring tests failed" }
cargo test --lib config::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle config tests failed" }
```

### Task 10: Add the integrated signed SQLite acceptance harness and matrix

**Files:**
- Create: `tests/support/lifecycle.rs`
- Modify: `tests/integration.rs`
- Modify: `tests/e2e.rs`
- Modify: `tests/support/mod.rs`
- Modify only if an acceptance gap is exposed: `src/s3/ops/lifecycle.rs`, `src/lifecycle/evaluator.rs`, `src/lifecycle/actions.rs`, `src/lifecycle/worker.rs`, `src/store/lifecycle_config.rs`, `src/store/lifecycle_scan.rs`, `src/store/lifecycle_action.rs`, `src/store/pinning/publication.rs`
- Test: same test files

**Interfaces:**
- Consumes: the complete Tasks 1-9 implementation, signed request helpers, mock Kubo, SQLite, and deterministic lifecycle worker controls.
- Produces: acceptance-only signed proof of API/current/noncurrent/marker semantics, revalidation, encryption, tags, leases, shared CID, and no pin removal; any discovered cross-layer integration repair remains inside this task and its existing allowlist.

- [ ] **Step 1: Add the deterministic lifecycle harness and signed configuration acceptance cases**

Create a unique SQLite database, bucket owner, in-process S3 service, wiremock Kubo, `CancellationToken`, and helper methods `set_database_times`, `run_one_scan_page`, `claim_one_action`, `execute_claim`, `restart_worker`, and `stop_after_claim`; register the harness in `tests/support/mod.rs`. Add raw SigV4 `?lifecycle` cases for 200/200/204, canonical GET XML, absent 404, expected owner, missing bucket, full replacement, framework-native XML behavior, unsupported actions, typed invalid combinations, and prior revision unchanged on rejection. Use Notify/Barrier/timeouts; never use sleep as a correctness oracle.

- [ ] **Step 2: Run the signed configuration acceptance target expecting GREEN**

```powershell
cargo test --test integration lifecycle_signed_configuration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Task 10 integration gap in signed lifecycle configuration; keep Task 10 open, repair the owning Tasks 1-9 layer, and rerun" }
```

Expected: GREEN because Tasks 1-9 already implement the behavior. A failure keeps the acceptance task open; trace it to the owning layer, make the smallest allowlisted integration correction in this task, and rerun this exact target before continuing.

- [ ] **Step 3: Add and run current-expiration acceptance cases expecting GREEN**

Add unversioned permanent removal, Enabled marker creation with retained content, Suspended null replacement, current marker with history no-op, timed sole-marker exact cleanup, and immediate EODM sole-marker cleanup. Assert ordinary listings/current reads and version listing reflect the exact expected state, then run:

```powershell
cargo test --test integration lifecycle_expiration_current -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Task 10 integration gap in current-expiration acceptance; repair and rerun inside Task 10" }
```

- [ ] **Step 4: Add and run noncurrent acceptance cases expecting GREEN**

Create ordered public content versions and markers; prove `NoncurrentDays` required, age boundary strict, optional Newer requires modern filter, exactly N newer does not run, more than N plus age does, and exact deletion never creates a marker or touches the successor.

```powershell
cargo test --test integration lifecycle_expiration_noncurrent -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Task 10 integration gap in noncurrent-expiration acceptance; repair and rerun inside Task 10" }
```

- [ ] **Step 5: Add and run revalidation/security/storage acceptance cases expecting GREEN**

Replace/delete config after claim; disable a rule; mutate tags; replace current; promote; exact-delete; race publication/delete; test shared CID, SSE-S3, SSE-C, tags, leases, retry, and process stop. Assert execution rereads tags and mock Kubo has no pin-rm request.

```powershell
cargo test --test integration lifecycle_expiration_invariants -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Task 10 integration gap in lifecycle invariants; repair and rerun inside Task 10" }
```

- [ ] **Step 6: Run the complete acceptance matrix GREEN**

```powershell
cargo test --test integration lifecycle_ -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Complete signed SQLite lifecycle acceptance matrix failed" }
cargo test --test e2e --no-run
if ($LASTEXITCODE -ne 0) { throw "Lifecycle E2E tests did not compile" }
```

Expected: every acceptance target is GREEN. Any failure keeps Task 10 active until the smallest cross-layer integration correction is made and this complete matrix passes.

### Task 11: Prove PostgreSQL worker-crash fencing and signed multi-gateway races

**Files:**
- Modify: `tests/postgres_lifecycle.rs`
- Modify: `src/lifecycle/worker.rs`
- Modify: `tests/multi_gateway.rs`
- Modify: `tests/multi-gateway.Tests.ps1`
- Test: same files

**Interfaces:**
- Consumes: PostgreSQL owned-schema/independent-connection pattern, `LifecycleWorkerHandle`, `ClaimedLifecycleAction`, fenced terminal CAS functions, database clock, and the existing endpoint-only multi-gateway environment.
- Produces: deterministic worker IDs and doc-hidden after-claim/abort controls; an executable PostgreSQL A-crash/B-reclaim/stale-A-CAS proof; signed cross-replica configuration visibility; and a publication/action race with one terminal S3-visible outcome.

- [ ] **Step 1: Write the causal RED source contract for separated PostgreSQL and endpoint-only proofs**

Extend `tests/multi-gateway.Tests.ps1` first. Require `postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch`, `LifecycleWorkerTestControl`, `LifecycleAfterClaimGate`, `start_worker_for_test`, `abort_for_test`, worker IDs `worker-a`/`worker-b`, two independent PostgreSQL connections, DB-clock lease polling, saved stale-A `mark_succeeded`, Barriers/Notify, and 15-second bounds. Require exactly `multi_gateway_lifecycle_configuration_visible_across_replicas` and `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome` in `tests/multi_gateway.rs`; reject any Docker/service/process-stop or direct-database claim in that endpoint-only file.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "PostgreSQL crash-proof and endpoint-only source-contract RED unexpectedly passed" }
```

Expected: the static contract fails on the missing deterministic worker seam and test declarations without contacting Docker or PostgreSQL.

- [ ] **Step 2: Add PostgreSQL parity tests and the exact deterministic worker seam**

Create the migration/clock/claim tests with `lifecycle_<32 lowercase hex>` owned schemas, one-connection fallback cleanup, independent connections, Barriers, and 15-second deadlock bounds. Cover schema/checks/indexes, successor backfill, latest nullness, ambiguous legacy exclusion, injected rollback, down refusal, database-clock due, scan takeover, duplicate insert, and action claims.

Add this doc-hidden integration-test seam to `src/lifecycle/worker.rs`; normal `start_worker` continues to generate its UUID worker ID and passes no gate:

```rust
#[doc(hidden)]
#[derive(Clone)]
pub struct LifecycleWorkerTestControl {
    pub worker_id: String,
    pub after_claim: Option<std::sync::Arc<LifecycleAfterClaimGate>>,
}

#[doc(hidden)]
pub struct LifecycleAfterClaimGate {
    expected_worker_id: String,
    claimed: tokio::sync::Mutex<Option<ClaimedLifecycleAction>>,
    arrived: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl LifecycleAfterClaimGate {
    pub fn new(expected_worker_id: impl Into<String>) -> std::sync::Arc<Self>;
    pub async fn wait_claim(&self) -> ClaimedLifecycleAction;
    pub fn release(&self);
}

#[doc(hidden)]
pub fn start_worker_for_test(
    store: Store,
    config: ValidatedLifecycleConfig,
    parent: CancellationToken,
    control: LifecycleWorkerTestControl,
) -> LifecycleWorkerHandle;

impl LifecycleWorkerHandle {
    #[doc(hidden)]
    pub fn abort_for_test(self) -> tokio::task::JoinHandle<()>;
}
```

The gate stores a clone of the matching post-action-claim `ClaimedLifecycleAction`, notifies the test, and blocks before execution until released. `wait_claim` loops over the mutex/Notify state so an early notification cannot be lost. `abort_for_test` calls `JoinHandle::abort` without cancellation, terminal update, or lease release and returns that handle so the test can require a cancelled join result. These controls are not accepted from configuration or HTTP and production startup never invokes them.

- [ ] **Step 3: Execute the PostgreSQL abort-after-claim, lease takeover, and stale-CAS proof**

In `postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch`, create two independent connections with the same fresh owned schema and wrap each in its own `Store`. Seed one active canonical rule, one exact due target, and one pending action using database time. Start worker A as `worker-a` with a one-second action lease and gate A; wait under 15 seconds for both the gate and an exact DB row `(state='claimed', claimed_by='worker-a', claim_epoch=E)`, then save A's claim and call `abort_for_test`, requiring its JoinHandle to finish cancelled without changing the row.

Poll `database_now` under a bounded timeout until it passes the row's persisted `lease_until`; do not use process sleep as the correctness oracle. Start worker B as `worker-b` with gate B, require the DB row becomes `(state='claimed', claimed_by='worker-b', claim_epoch=E+1)`, release B, and require one terminal `succeeded` row plus the expected exact object/version mutation. Finally call `mark_succeeded` with saved claim A and current database time, require `false`/zero affected rows, and prove the terminal row and target projection are unchanged. Drop the owned schema through the normal guard and its independent-connection fallback even after timeout, panic, or aborted worker.

- [ ] **Step 4: Add only endpoint-executable signed multi-gateway scenarios**

In `multi_gateway_lifecycle_configuration_visible_across_replicas`, PUT signed configuration through gateway A, GET the canonical rules through gateway B and the load balancer, replace through B, and DELETE through A. In `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`, race a signed successor publication from one replica against an already-due lifecycle action driven by the other replica; use bounded signed GET/ListObjectVersions polling to require exactly one allowed terminal S3-visible state and prove the successor is never deleted. Use only the existing four endpoint variables, signed HTTP, Barriers, and timeouts; do not access the database, worker handles, Docker, or service/process controls from `tests/multi_gateway.rs`.

- [ ] **Step 5: Lock the proof separation in the Docker-free static contract**

Require the exact PostgreSQL worker IDs, gate/control/abort symbols, DB claim tuples, `E + 1`, database-time deadline comparison, stale-A CAS zero result, cleanup fallback, and timeout bounds. Separately require the two signed multi-gateway names, existing endpoint names once, SigV4 helpers, `tokio::join!`, bounded polling, and successor-preservation assertions; reject process-kill/stop, Docker, SQL, `DatabaseConnection`, and worker-control symbols in `tests/multi_gateway.rs`. Retain every existing topology/workflow assertion.

- [ ] **Step 6: Run non-live compile/static gates**

```powershell
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL lifecycle tests did not compile" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway lifecycle tests did not compile" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL/multi-gateway proof-separation contract failed" }
```

### Task 12: Add an isolated, no-pull AWS evidence topology, runner, static contract, and honest initial receipt

**Files:**
- Create: `tests/compose.lifecycle-expiration-validation.yml`
- Create: `scripts/lifecycle-expiration-smoke.ps1`
- Create: `docs/lifecycle-expiration-evidence-2026-08-26.log`
- Modify: `tests/client-smoke.Tests.ps1`
- Verify unchanged: production Compose files, workflow, ROADMAP

**Interfaces:**
- Consumes: local existing images/tools, source tree, PostgreSQL/E2E/multi-gateway tests, signed `lifecycle_expiration_invariants`, safe optional-list parsing, exact AWS lifecycle state assertions, and PowerShell AST.
- Produces: opt-in live runner with five bounded Rust commands in the locked order, signed Kubo `/api/v0/pin/rm` zero-request proof, exact owned cleanup, sanitized `NOT RUN`/PASSED evidence, and a Docker-free safety contract.

- [ ] **Step 1: Write causal RED PowerShell AST/source/topology assertions**

Require `[switch]$Run`; exact default `NOT RUN`; anchored run/project/image/bucket grammar; direct-child temp root; `FileMode.CreateNew` receipt; project-label absence; exactly five loopback ports (PostgreSQL, Kubo RPC, gateway A, gateway B, load balancer); Compose >=2.23.1 via `--short`; exact local image inspection; `--pull=false`, `--network none`, `--pull never`, `--no-build`; no install/pull; stage allowlist; exactly five bounded Rust commands in order `postgres_versioning` → `postgres_lifecycle` → `e2e` → `multi_gateway` → signed `integration lifecycle_expiration_invariants`; safe optional-list parsing; exact AWS state assertions; safe evidence charset; partial ownership flags; exact cleanup; residual zero checks; and README/ROADMAP gating. The static contract rejects any SQL or evidence assertion based on `pin_jobs.operation='pin_rm'`; that column/value is not a lifecycle pin-removal proof.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Lifecycle evidence static RED unexpectedly passed" }
```

- [ ] **Step 2: Define the isolated validation topology**

Create this exact project-scoped topology with no `container_name`. It exposes only five dynamically owned loopback ports and reuses the existing streaming Nginx config without modifying production files:

```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: ipfs3
      POSTGRES_USER: ipfs3
      POSTGRES_PASSWORD: ipfs3
    ports:
      - "127.0.0.1:${IPFS_S3_LIFECYCLE_POSTGRES_PORT:?required}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]
      interval: 1s
      timeout: 5s
      retries: 30

  kubo:
    image: ghcr.io/hugefiver/ipfs3-kubo:latest
    environment:
      IPFS_PATH: /data/ipfs
    ports:
      - "127.0.0.1:${IPFS_S3_LIFECYCLE_KUBO_PORT:?required}:5001"
    volumes:
      - kubo_data:/data/ipfs
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 15s

  gateway-a:
    image: "${IPFS_S3_LIFECYCLE_IMAGE:?required}"
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_KUBO_RPC_URL: http://kubo:5001
      IPFS_S3_DATABASE_URL: postgres://ipfs3:ipfs3@postgres:5432/ipfs3
      IPFS_S3_ACCESS_KEY_ID: test
      IPFS_S3_SECRET_ACCESS_KEY: test
      IPFS_S3_MASTER_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS: "100"
      IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS: "3"
      IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS: "3"
      RUST_LOG: info
    ports:
      - "127.0.0.1:${IPFS_S3_LIFECYCLE_GATEWAY_A_PORT:?required}:9000"
    depends_on:
      postgres:
        condition: service_healthy
      kubo:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 2s
      timeout: 3s
      retries: 20
      start_period: 10s

  gateway-b:
    image: "${IPFS_S3_LIFECYCLE_IMAGE:?required}"
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_KUBO_RPC_URL: http://kubo:5001
      IPFS_S3_DATABASE_URL: postgres://ipfs3:ipfs3@postgres:5432/ipfs3
      IPFS_S3_ACCESS_KEY_ID: test
      IPFS_S3_SECRET_ACCESS_KEY: test
      IPFS_S3_MASTER_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
      IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS: "100"
      IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS: "3"
      IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS: "3"
      RUST_LOG: info
    ports:
      - "127.0.0.1:${IPFS_S3_LIFECYCLE_GATEWAY_B_PORT:?required}:9000"
    depends_on:
      postgres:
        condition: service_healthy
      kubo:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 2s
      timeout: 3s
      retries: 20
      start_period: 10s

  load-balancer:
    image: nginx:1.28.0-alpine
    ports:
      - "127.0.0.1:${IPFS_S3_LIFECYCLE_LOAD_BALANCER_PORT:?required}:9000"
    volumes:
      - ../deploy/nginx/multi-gateway.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      gateway-a:
        condition: service_healthy
      gateway-b:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O - http://127.0.0.1:9000/ready | grep -qx READY"]
      interval: 2s
      timeout: 3s
      retries: 20
      start_period: 5s

volumes:
  postgres_data:
  kubo_data:
```

The AWS CLI v2 image is an inspected local prerequisite and runs transiently with `--rm` on the owned Compose network; it is not a sixth long-lived service.

- [ ] **Step 3: Implement fail-closed preflight, ownership, offline build, and cleanup**

Default invocation prints exactly `[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested` and exits zero. `-Run` verifies tools/images without installation or pull, claims unique temp/project/image/bucket/ports, vendors from local cache, builds offline, runs `compose config --quiet`, starts with no pull/build, and marks ownership immediately after observation. Cleanup removes only the exact project/image/direct-child temp root, restores environment, and independently proves zero residual containers/networks/volumes/image.

- [ ] **Step 4: Implement exact live AWS and database-clock action sequence**

Run the complete existing `postgres_versioning`, new `postgres_lifecycle`, complete `e2e`, complete `multi_gateway`, then the focused signed `integration lifecycle_expiration_invariants` group against owned endpoints, in exactly that order. Through AWS CLI path-style SigV4: PUT/GET/replace/DELETE config; assert absent GET code; reject unsupported action without revision change; use safe optional-list parsing before dereference; and assert exact canonical/control-plane and object-version states for unversioned, Enabled, and Suspended current expiration, NVE content and marker, timed sole marker, and EODM. Use read-only/targeted owned-schema SQL only to move lifecycle timestamps into the past; never change process clock. Require bounded polling, exact PostgreSQL A-abort/B-reclaim/stale-CAS receipts, one terminal cross-replica S3-visible race outcome, and successor preservation. The fifth signed integration group is the sole Kubo `/api/v0/pin/rm` zero-request proof; do not query or claim `pin_jobs.operation='pin_rm'` because pin jobs do not use that operation.

The runner executes these exact Rust commands only after the owned topology is healthy and the corresponding endpoint variables are set:

```powershell
cargo test --test postgres_versioning -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Owned PostgreSQL versioning regression failed" }
cargo test --test postgres_lifecycle -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Owned PostgreSQL lifecycle tests failed" }
cargo test --test e2e -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Owned lifecycle E2E regression failed" }
cargo test --test multi_gateway -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Owned multi-gateway lifecycle regression failed" }
cargo test --test integration lifecycle_expiration_invariants -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Signed lifecycle pin-rm invariant group failed" }
```

- [ ] **Step 5: Create honest evidence and make the static/no-run contract GREEN**

Initial evidence contains date, both approved spec hashes, package/version, the exact five ordered Rust command categories (`postgres_versioning`, `postgres_lifecycle`, `e2e`, `multi_gateway`, signed `integration lifecycle_expiration_invariants`), and exact line `LIFECYCLE EXPIRATION REAL CLIENT: NOT RUN`. It identifies the fifth signed group as the Kubo `/api/v0/pin/rm` zero-request proof, contains no SQL/`pin_jobs` substitute and no PASS claim, and records that no live run has occurred. Parse runner AST and its no-run output:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle evidence static contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors" }
$output = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0) { throw "Lifecycle runner no-run path failed" }
if (($output -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Lifecycle runner no-run receipt changed"
}
```

- [ ] **Step 6: Check Task 12 whitespace without running Docker/live work**

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tracked lifecycle runner diff check failed" }
foreach ($path in @(
    "scripts/lifecycle-expiration-smoke.ps1",
    "tests/compose.lifecycle-expiration-validation.yml",
    "docs/lifecycle-expiration-evidence-2026-08-26.log",
    "docs/superpowers/plans/2026-08-26-lifecycle-expiration.md"
)) {
    git -c core.autocrlf=false diff --no-index --check -- NUL $path
    if ($LASTEXITCODE -gt 1) { throw "Untracked lifecycle artifact whitespace check failed: $path" }
}
```

### Task 13: Run the complete evidence matrix and update only the accurate README subset

**Status:** **REVIEWER FOLLOW-UP MAX-ATTEMPT ORPHAN CLEANUP FIXED / CLAIM8 ACTION14 LIB880 INTEGRATION143 GREEN / MAIN-PATH FINAL LIVE STILL PASS / TASK 14 CURRENT RE-REVIEW OPEN.** Historical #28 remains accurately failed; evidence uses no false attempt number. Current plan-review receipt remains required before Task 14 re-review.

**Files:**
- Modify test-only: `src/s3/ops/tagging.rs`
- Modify test-only: `src/s3/ops/object.rs`
- Modify test-only: `src/s3/ops/multipart.rs`
- Modify test-only: `tests/postgres_lifecycle.rs`
- Modify test-only: `tests/postgres_versioning.rs`
- Modify test-only race observer/body bounds/order and cleanup regression: `tests/multi_gateway.rs`
- Modify production ownership admission: `src/store/import/ownership.rs`
- Modify production lifecycle admission/retry tests: `src/lifecycle/actions.rs`
- Modify mechanically for clippy only: `src/pinning/worker.rs`
- Modify mechanically for clippy only: `src/s3/ops/versioning.rs`
- Modify runner-only: `scripts/lifecycle-expiration-smoke.ps1`
- Modify runner static contract only: `tests/client-smoke.Tests.ps1`
- Modify multi-gateway static contract only: `tests/multi-gateway.Tests.ps1`
- Modify protected hashes only: `tests/cluster.Tests.ps1`
- Modify only after a complete final normal-run PASS: `docs/lifecycle-expiration-evidence-2026-08-26.log`
- Modify only after a complete final normal-run PASS: `README.md`
- Verify unchanged: `ROADMAP.md:77`, both specs, all protected paths
- Retain ignored through evidence-led rounds; remove only in Task 14 after complete final PASS: `.debug-journal.md`

**Interfaces:**
- Consumes: Tasks 1-12 current tree, opt-in owned runner with the locked five-command order ending in signed `integration lifecycle_expiration_invariants`, and post-#28 multi-gateway receipts.
- Produces when complete: retained history; deterministic admission/crash reclaim/user priority/stale retry; max-attempt claimed-once reclaim versus exhausted-pending fail-safe; claim-then-bucket owned-token cleanup for terminal paths; clock test `3/3`; claim `8`, actions `14`, library `880/880`, integration `143/143`; unchanged main-path final live PASS; exact evidence/README with ROADMAP unchecked; open Task 14 re-review; and no push/tag claim.

- [ ] **Step 1: Run format, build, focused, library, and signed integration gates**

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Formatting failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library tests failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Signed integration tests failed" }
```

The full prelive integration suite includes `lifecycle_expiration_invariants`; its signed wiremock assertion is the valid Kubo `/api/v0/pin/rm` zero-request proof. No SQL or `pin_jobs.operation` assertion may substitute for that test.

**Task 13 Step 1 full-lib blocker and retained correction record:** The first prelive `cargo test --lib` exceeded 30 minutes; the live runner was not invoked, lifecycle evidence remained `NOT RUN`, and README/ROADMAP were unchanged. A parent standard-parallel rerun completed every other test but left `keep_snapshot_race_rolls_back_tags_when_expired_lease_is_reactivated` and `tagging_revalidates_version_and_lease_under_lock` hung beyond 600 seconds; the exact isolated keep-snapshot test also hung beyond 300 seconds. The confirmed cause was test fixtures inserting `objects` after migrations without matching hidden unversioned `object_versions` rows, so version-aware tagging safely returned `NoSuchKey` before reaching the policy-evaluated gate while the test waited indefinitely.

The retained correction is test-only: `src/s3/ops/tagging.rs` installs matching hidden-null versions in direct fixtures, installs the policy gate through panic-safe RAII, and races bounded gate arrival against tagging-task completion so an early error fails immediately instead of hanging; `src/s3/ops/object.rs` and `src/s3/ops/multipart.rs` install matching hidden-null versions in their direct fixtures. `tests/cluster.Tests.ps1` changes only the two protected hashes for the object/multipart test-only edits. Do not serialize the suite and do not add sleep as a fix or correctness oracle; the bounded gate-versus-task race is test robustness only. Both exact race tests passed, the tagging scope passed `24/24`, the final standard-parallel library suite passed `875/875`, and fmt/diff/LSP were clean.

The subsequent full serial integration run executed 143 tests with `131` passing and `12` failing; every lifecycle-specific integration test passed. The failures were older SSE-C, ranged-HEAD, and blocked-import mutation paths returning 404 or timing out before their intended Kubo block. The same fixture root cause applied in `tests/integration.rs`: direct `seed_latest`, `seed_sse_c_object`, and the standalone ranged-HEAD fixture inserted objects after migrations without a matching hidden unversioned `object_versions` row. The retained test-only correction adds one `install_unversioned_content_version` helper and calls it at exactly those three fixture sites; no product semantics or test assertions change. Representative evidence is `test_head_range_changes_only_content_length_and_never_calls_kubo` changing from RED 404 to GREEN 200, SSE-C GET GREEN, and `legacy_sse_c_copy_admits_destination_before_cat` GREEN. The final full serial integration suite passed `143/143`, with fmt/diff/LSP clean.

These records closed the full-lib and serial-integration blockers before the first live attempt. At that boundary, live invocation count was `0`, lifecycle evidence remained `NOT RUN`, README/ROADMAP were unchanged, and Task 13's Docker-free static contracts plus owned live invocation were still pending.

- [ ] **Step 2: Run every Docker-free deployment and evidence static contract**

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release static validation failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL baseline static validation failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway static validation failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster static validation failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client/evidence static validation failed" }
```

All remaining static scripts and `git diff --check` passed on the first-live candidate.

**Task 13 live invocation #1 and PostgreSQL test-only correction record:** Invocation #1 started an owned topology and failed at stage `postgres` on the first locked command, `postgres_versioning`; `postgres_lifecycle`, `e2e`, `multi_gateway`, signed `integration lifecycle_expiration_invariants`, and every AWS boundary were not reached. Runner cleanup passed for Compose down, image removal, temporary-root removal, and environment restoration; independent container/network/volume/image residual counts were all zero and `cleanup-errors=0`. Evidence stayed `NOT RUN`, README/ROADMAP remained unchanged, and no second invocation has run.

A fresh one-service PostgreSQL reproduction executed 16 tests with `14` passing and two failures: `postgres_object_versioning_migration_adds_status_table_indexes_and_checks` and `postgres_object_versioning_down_refuses_public_state_versions_and_markers`. The migration-9-focused inventory incorrectly included migration-10-owned `lifecycle_age_started_at` and `became_noncurrent_at`; direct invalid-marker/public-version/public-marker fixtures also omitted migration 10's non-null `lifecycle_age_started_at`, so they failed before the intended down-refusal assertion. The retained `tests/postgres_versioning.rs` correction is test-only: exclude exactly those two lifecycle columns from the migration-9 inventory and add `lifecycle_age_started_at = CURRENT_TIMESTAMP` to the invalid-marker, public-version, and public-marker inserts. It weakens no production schema, migration, or assertion. Both focused tests passed `1/1`, the fresh full PostgreSQL suite passed `16/16` with no skips, owned-schema cleanup was zero, environment restoration passed, and fmt/diff/LSP were clean.

Before invocation #2, the complete current-candidate prelive matrix passed: fmt/build, standard library `875/875`, full serial integration `143/143`, all five static scripts, and diff. Invocation #2 then failed at the second locked Rust command, `postgres_lifecycle`; `postgres_versioning` passed, while `e2e`, `multi_gateway`, signed `integration lifecycle_expiration_invariants`, and every AWS boundary were not reached. Cleanup again passed for Compose down, image/temp/environment cleanup, all independent residual-zero checks, and `cleanup-errors=0`. Evidence remained `NOT RUN` and README/ROADMAP remained unchanged.

A fresh PostgreSQL lifecycle target ran four tests with three passing; only `postgres_lifecycle_worker_abort_reclaims_and_fences_stale_epoch` failed. Its old terminal assertion observed the required succeeded `expire_current` action with no failure, the exact public version absent, and the immutable object row retained as `is_latest=false`, but incorrectly required physical object-row deletion. Correct unversioned expiration semantics remove the current projection and public version index while allowing the immutable noncurrent object row to remain, preserving the no-Kubo-pin-removal boundary. The retained `tests/postgres_lifecycle.rs` correction is test-only: accept the immutable object row as either absent or present with `is_latest=false`, while retaining every hard-loss assertion for terminal action state/kind/failure, exact version removal, current/public projection removal, and no pin removal. No production code or semantics change.

The corrected exact PostgreSQL test passed `1/1`, the fresh full lifecycle PostgreSQL target passed `4/4`, owned cleanup was zero, environment restoration passed, and fmt/diff/LSP were clean. One unrelated database-clock assertion failed transiently once; it received no code/test change, and an unchanged control rerun passed the complete target `4/4`, so it is not classified as a product fix or blocker correction.

#3's complete current-candidate prelive matrix passed fmt/build, standard library `875/875`, full serial integration `143/143`, all five static scripts, and diff. In the owned live run, `postgres_versioning` passed, `postgres_lifecycle` passed its hard-loss contract, `e2e` passed, and `multi_gateway` passed its terminal/successor contract. The fifth exact command, `cargo test --test integration lifecycle_expiration_invariants -- --nocapture --test-threads=1`, executed one matching test and its Rust process/test result was GREEN, but runner helper `Assert-RustSuiteExecuted` rejected Rust's singular line `running 1 test` because its source regex required plural `tests`. AWS scenarios and the final pin-rm receipt were not reached or emitted, so the runner correctly ended FAILED. Cleanup passed for Compose down, containers/networks/volumes/image/temp/environment residual zero, and `cleanup-errors=0`; evidence remained `NOT RUN` and README/ROADMAP remained unchanged.

The retained runner/static correction is test-only: anchor the positive count contract exactly as `running [1-9][0-9]* tests?` and independently require `test result: ok.`. Pure static fixtures accept singular `running 1 test` and plural `running 2 tests`, and reject zero and malformed count lines. The exact five-command sequence remains unchanged. `tests/client-smoke.Tests.ps1`, PowerShell parser validation, the runner no-run path, and diff are GREEN; no product behavior changes.

Invocation #4's full current-candidate prelive matrix passed fmt/build, standard library `875/875`, full serial integration `143/143`, all five static scripts, and diff. Its owned live run stopped while validating the first `postgres_versioning` receipt even though that target itself was already proven fresh-direct `16/16`; no product or test input failed. `postgres_lifecycle`, `e2e`, `multi_gateway`, signed `integration lifecycle_expiration_invariants`, every AWS scenario, and the pin-rm receipt were not reached or emitted. Cleanup passed with Compose down and container/network/volume/image/temp/environment residual zero plus `cleanup-errors=0`; evidence remained `NOT RUN` and README/ROADMAP remained unchanged. Under invocation #4's then-active rule, this failure stopped promotion with no automatic #5.

The confirmed runner-only cause was the second half of `Assert-RustSuiteExecuted`: after accepting singular/plural count lines, it still required the entire line to equal bare `test result: ok.`, while Rust emits a full summary such as `test result: ok. 16 passed; 0 failed; ...`. The final test-only parser requires exactly one match for `(?m)^running (?<running>[1-9][0-9]*) tests?\r?$` and exactly one match for `(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<duration>[0-9]+(?:\.[0-9]+)?)s\r?$`; it parses `duration` and accepts only `0..=3600` seconds, and requires `running == passed`. Pure fixtures accept singular `1/1` and plural `16/16`, while bare, count-mismatch, zero, failure, negative, overlong-duration, duplicate, and substring-only receipts are rejected. The five Rust commands and their order remain unchanged. Static fixtures, PowerShell parser validation, runner no-run, and diff are GREEN; no product or product-test input changed.

- [ ] **Step 3: Record final invocation #5 and close the live-attempt boundary**

A fresh agent reran the complete current prelive matrix before the final attempt: fmt/build passed, the standard library suite passed `875/875`, the full serial integration suite passed `143/143`, all five Docker-free static scripts passed, and diff passed. Final authorized owned invocation #5 then passed `postgres_versioning`, passed `postgres_lifecycle`, and passed `e2e`, but failed in the `multi_gateway` Rust suite. Signed `integration lifecycle_expiration_invariants`, every AWS lifecycle scenario, and the final signed Kubo pin-rm-zero receipt were not run or emitted.

Runner cleanup completed successfully: Compose down passed; independent container/network/volume/image residual counts were all zero; the temporary root was removed; the environment was restored; and `cleanup-errors=0`. The final runner result was `FAILED reason=execution-failed`. At that historical boundary, no invocation #6 was authorized or had run. Evidence remained `NOT RUN`, README/ROADMAP remained unchanged, no Git write occurred, and `.debug-journal.md` was retained because the multi-gateway blocker was unresolved. The later user instruction `继续完成` is the sole authority reopening the bounded path below; none of the historical attempts is relabelled or discarded.

- [ ] **Step 4: Retain the safe diagnostic surface and record completed owned diagnostic attempt #6**

First make `tests/client-smoke.Tests.ps1` RED by requiring a mutually exclusive `-DiagnoseMultiGateway` switch, an exact multi-gateway-only command branch, and pure accepted/rejected failure-receipt fixtures. The fixtures accept only one bounded failed-suite receipt with anchored test names matching `[A-Za-z0-9_:]+`; they reject unsafe names, zero/duplicate/mismatched counts, a success summary, missing failed names, unbounded duration, substring matches, arbitrary stderr emission, calls to `Invoke-LifecycleRustSuites`/`Invoke-LifecycleAwsEvidence`, and any command other than `cargo test --test multi_gateway -- --nocapture --test-threads=1`. Run the static contract and require the intended RED before changing the runner.

Implement the diagnostic mode in `scripts/lifecycle-expiration-smoke.ps1` without changing default no-run or normal `-Run` behavior. `-Run` and `-DiagnoseMultiGateway` are mutually exclusive. Both live modes reuse the same preflight, fresh `RunId`, unique Compose project/image/direct-child root/bucket, five-service topology, no-pull/offline-build, exact environment save/restore, logs-before-cleanup, and residual container/network/volume/image/root checks. No project/image/root/bucket from attempts #1-#5 may be reused; every loopback binding must be preflight-free and exclusively held by the fresh #6 topology. After topology health and metadata, diagnostic mode calls only `Invoke-LifecycleMultiGatewayDiagnostic`; it never calls the normal five-suite function or AWS evidence function.

```powershell
[CmdletBinding()]
param(
    [switch]$Run,
    [switch]$DiagnoseMultiGateway
)

if ($Run -and $DiagnoseMultiGateway) {
    throw "Run and DiagnoseMultiGateway are mutually exclusive"
}
if (-not $Run -and -not $DiagnoseMultiGateway) {
    Write-Host "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"
    exit 0
}

function Get-MultiGatewayFailureReceipt {
    param([Parameter(Mandatory)][object]$Result)

    $lines = @($Result.StdOut) + @($Result.StdErr)
    $output = $lines -join "`n"
    $runningMatches = [regex]::Matches($output, '(?m)^running (?<running>[1-9][0-9]*) tests?$')
    $summaryMatches = [regex]::Matches($output, '(?m)^test result: FAILED\. (?<passed>[0-9]+) passed; (?<failed>[1-9][0-9]*) failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$')
    $nameMatches = [regex]::Matches($output, '(?m)^test (?<name>[A-Za-z0-9_:]+) \.\.\. FAILED$')
    if ($runningMatches.Count -ne 1 -or $summaryMatches.Count -ne 1) {
        throw "Multi-gateway diagnostic receipt shape is invalid"
    }

    [long]$running = 0
    [long]$passed = 0
    [long]$failed = 0
    [long]$ignored = 0
    [long]$measured = 0
    [long]$filtered = 0
    [decimal]$seconds = 0
    $parsed =
        [Int64]::TryParse($runningMatches[0].Groups['running'].Value, [ref]$running) -and
        [Int64]::TryParse($summaryMatches[0].Groups['passed'].Value, [ref]$passed) -and
        [Int64]::TryParse($summaryMatches[0].Groups['failed'].Value, [ref]$failed) -and
        [Int64]::TryParse($summaryMatches[0].Groups['ignored'].Value, [ref]$ignored) -and
        [Int64]::TryParse($summaryMatches[0].Groups['measured'].Value, [ref]$measured) -and
        [Int64]::TryParse($summaryMatches[0].Groups['filtered'].Value, [ref]$filtered) -and
        [decimal]::TryParse($summaryMatches[0].Groups['seconds'].Value, [Globalization.NumberStyles]::AllowDecimalPoint, [Globalization.CultureInfo]::InvariantCulture, [ref]$seconds)
    $names = @($nameMatches | ForEach-Object { $_.Groups['name'].Value })
    if (-not $parsed -or $seconds -lt 0 -or $seconds -gt 3600 -or
        $running -ne ($passed + $failed + $ignored + $measured) -or
        $names.Count -ne $failed -or @($names | Sort-Object -Unique).Count -ne $names.Count) {
        throw "Multi-gateway diagnostic receipt counts are invalid"
    }

    $categoryRules = [ordered]@{
        timeout = '(?i)\b(timed out|deadline has elapsed)\b'
        assertion = '(?i)\b(assertion|panicked at)\b'
        'http-status' = '(?i)\b(http|status code)\b'
        connection = '(?i)\b(connection|connect|refused)\b'
        database = '(?i)\b(database|postgres|sqlx)\b'
    }
    $category = 'process-exit'
    :lineScan foreach ($line in $lines) {
        foreach ($rule in $categoryRules.GetEnumerator()) {
            if ($line -match $rule.Value) {
                $category = $rule.Key
                break lineScan
            }
        }
    }
    return [pscustomobject]@{
        Running = $running
        Passed = $passed
        Failed = $failed
        Ignored = $ignored
        Measured = $measured
        Filtered = $filtered
        FailedNames = $names
        FirstErrorCategory = $category
    }
}

function Invoke-LifecycleMultiGatewayDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)

    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "multi-gateway"
    Write-LifecycleEvidence -Category "command" -Value "cargo test --test multi_gateway -- --nocapture --test-threads=1"
    $result = Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @("test", "--test", "multi_gateway", "--", "--nocapture", "--test-threads=1") `
        -Label "Owned multi-gateway diagnostic" `
        -Timeout $RustTestTimeout `
        -AllowedExitCodes @(0, 101) `
        -WorkingDirectory $RepoRoot
    if ($result.ExitCode -eq 0) {
        Assert-RustSuiteExecuted -Result $result -Name "Owned multi-gateway diagnostic"
        $State.DiagnosticOutcome = "not-reproduced"
        Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway=not-reproduced"
        return
    }

    $receipt = Get-MultiGatewayFailureReceipt -Result $result
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-running=$($receipt.Running)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-passed=$($receipt.Passed)"
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-failed=$($receipt.Failed)"
    foreach ($name in $receipt.FailedNames) {
        Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-failed-test=$name"
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "multi-gateway-first-error-category=$($receipt.FirstErrorCategory)"
    $State.DiagnosticOutcome = "captured"
    throw "Owned multi-gateway diagnostic captured a safe failure receipt"
}
```

Add `DiagnosticOutcome` to runner state. In `Invoke-LifecycleMain`, branch to `Invoke-LifecycleMultiGatewayDiagnostic` immediately after metadata when `-DiagnoseMultiGateway` is set; otherwise retain `Invoke-LifecycleRustSuites` followed by AWS. For a captured failure, emit only `FAILED reason=multi-gateway-diagnostic-captured`; for a clean suite, emit only `DIAGNOSTIC outcome=not-reproduced`. Never print or persist arbitrary stdout/stderr, panic text, IDs, bucket/key/body, SQL, URLs, secrets, or raw Compose logs. The safe receipt is emitted before the generic throw; existing sanitized topology diagnostics run before exact cleanup.

The required pre-#6 sequence was: make the static fixtures GREEN, parse the runner, exercise its no-run path, and pass diff before invoking the diagnostic switch exactly once. The following block is the retained command shape for audit, not authorization to rerun it. Attempt #6 is consumed whether the suite failure is safely captured, the suite does not reproduce, the parser rejects output, or setup/cleanup fails.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Diagnostic runner static contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Diagnostic runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Diagnostic runner no-run contract failed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Diagnostic candidate whitespace check failed" }
$diagnosticOutput = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" -DiagnoseMultiGateway 2>&1)
$diagnosticExit = $LASTEXITCODE
if ($diagnosticExit -notin @(0, 1)) { throw "Diagnostic attempt #6 returned an invalid exit code" }
```

Accept #6 only with one terminal diagnostic result plus logs-first cleanup receipts proving Compose down and residual container/network/volume/image/root/environment zero with `cleanup-errors=0`. If the result is `not-reproduced`, receipt parsing fails, cleanup is not exact, or the safe receipt does not identify at least one anchored failed test name with consistent running/passed/failed counts and one fixed category, stop with evidence/README/ROADMAP unchanged; do not correct code and do not run #7.

**Observed diagnostic #6 safe receipt:** `running=11`, `passed=10`, `failed=1`, failed test `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`, first category `assertion`, terminal result `multi-gateway-diagnostic-captured`. Logs-first cleanup passed, Compose down passed, residual container/network/volume/image/root/environment counts were all zero, and `cleanup-errors=0`. No raw process output, IDs, bucket/key/body, SQL, URLs, or secrets are retained in this plan. Attempt #6 is consumed and must not be rerun.

**Confirmed cause:** `tests/multi_gateway.rs::multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome` explicitly accepts terminal state `(predecessor_visible=false, successor_visible=true)`, meaning an Enabled-bucket lifecycle `ExpireCurrent` won before successor publication. Correct product behavior necessarily created an opaque delete marker. The pre-correction cleanup at lines approximately 906-913 deleted only the predecessor and successor version IDs, so that marker remained; the following bucket deletion at lines approximately 914-920 then deterministically received `BucketNotEmpty` whenever the accepted race branch occurred. This was test cleanup only. Lifecycle expiration, publication fencing, versioning, and bucket non-empty enforcement are correct and unchanged.

- [ ] **Step 5: Record the exact test-only correction and complete nonlive GREEN receipts**

Only existing manifest path `tests/multi_gateway.rs` changed. Pure test `multi_gateway_lifecycle_cleanup_includes_delete_markers` uses terminal ListObjectVersions XML with successor and predecessor `<Version>` entries, a `<DeleteMarker>`, empty and duplicate IDs, and a `<NextVersionIdMarker>` pagination token. It requires the three nonempty public entry IDs exactly once in source order and excludes the pagination token.

```rust
#[test]
fn multi_gateway_lifecycle_cleanup_includes_delete_markers() {
    let terminal_versions = r#"
        <ListVersionsResult>
            <Version><VersionId>successor</VersionId></Version>
            <DeleteMarker><VersionId>marker</VersionId></DeleteMarker>
            <Version><VersionId>predecessor</VersionId></Version>
            <Version><VersionId></VersionId></Version>
            <DeleteMarker><VersionId>marker</VersionId></DeleteMarker>
            <NextVersionIdMarker>continuation</NextVersionIdMarker>
        </ListVersionsResult>
    "#;

    assert_eq!(
        version_ids_for_cleanup(terminal_versions),
        ["successor", "marker", "predecessor"]
    );
}
```

The exact causal RED command was:

```powershell
cargo test --test multi_gateway multi_gateway_lifecycle_cleanup_includes_delete_markers -- --exact --nocapture --test-threads=1
```

Observed RED: the helper returned only `successor, predecessor`; the assertion failed because the required marker ID was omitted instead of returning all three IDs `successor, marker, predecessor`. This was the causal cleanup defect, not a skip, compile failure, timeout, environment lookup, network access, or live-endpoint failure.

The corrected test-local `version_ids_for_cleanup(versions: &str) -> Vec<String>` walks exact `<Version>` and `<DeleteMarker>` sections in source order. Within each section it extracts the exact `<VersionId>` value, trims it, rejects empty values, and appends it only if it has not already appeared. Because it enters only those two public-entry sections, `<NextVersionIdMarker>` is excluded. It does not reorder, log, or persist IDs.

```rust
fn version_ids_for_cleanup(versions: &str) -> Vec<String> {
    let mut remaining = versions;
    let mut version_ids = Vec::new();
    loop {
        let next_element = match (
            remaining.find("<Version>"),
            remaining.find("<DeleteMarker>"),
        ) {
            (Some(version), Some(delete_marker)) if version < delete_marker => {
                (version, "<Version>", "</Version>")
            }
            (Some(_), Some(delete_marker)) => (delete_marker, "<DeleteMarker>", "</DeleteMarker>"),
            (Some(version), None) => (version, "<Version>", "</Version>"),
            (None, Some(delete_marker)) => (delete_marker, "<DeleteMarker>", "</DeleteMarker>"),
            (None, None) => break,
        };
        let (element_start, open, close) = next_element;
        let after_open = &remaining[element_start + open.len()..];
        let Some(element_end) = after_open.find(close) else {
            break;
        };
        let element = &after_open[..element_end];
        if let Some(version_id_start) = element.find("<VersionId>") {
            let after_version_id_open = &element[version_id_start + "<VersionId>".len()..];
            if let Some(version_id_end) = after_version_id_open.find("</VersionId>") {
                let version_id = after_version_id_open[..version_id_end].trim();
                if !version_id.is_empty()
                    && !version_ids.iter().any(|existing| existing == version_id)
                {
                    version_ids.push(version_id.to_owned());
                }
            }
        }
        remaining = &after_open[element_end + close.len()..];
    }
    version_ids
}
```

The race cleanup now derives every deletion target from the already stable `terminal_versions` XML. It deletes every listed public object version and lifecycle-created marker before the unchanged DeleteBucket assertion; `204 | 404` remains the accepted idempotent per-version cleanup status.

```rust
for version_id in version_ids_for_cleanup(&terminal_versions) {
    let response =
        signed_delete_object_version(&endpoint_a, &bucket_name, key, &version_id).await;
    assert!(
        matches!(response.status().as_u16(), 204 | 404),
        "signed cleanup of a raced version must not fail"
    );
}
```

The exact causal command then passed GREEN `1/1` without reading any live endpoint. The `multi_gateway` target compiled with `--no-run`; `cargo fmt --check`, `git diff --check`, and LSP diagnostics were GREEN. Product lifecycle/publication/versioning behavior and the two allowed terminal race outcomes remained unchanged.

```powershell
cargo test --test multi_gateway multi_gateway_lifecycle_cleanup_includes_delete_markers -- --exact --nocapture --test-threads=1
```

No production code, race assertion, suite scheduling, correctness timing, diagnostics, specs, evidence, README, or ROADMAP changed during this correction. No Docker or live command ran.

The complete current nonlive matrix then ran exactly once on the corrected candidate using this locked command set:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Correction fmt failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Correction locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Correction library suite failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Correction serial integration suite failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Correction multi-gateway target did not compile" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Correction release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Correction PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Correction multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Correction Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Correction client static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Correction whitespace check failed" }
```

Observed receipts: format PASS; locked build PASS; library `875/875`; serial integration `143/143`; `multi_gateway` target compile PASS; release, PostgreSQL, multi-gateway, Cluster, and client/evidence static contracts all PASS; final diff PASS. The exact manifest remains 53 paths. No Docker or live command ran during the correction or nonlive matrix.

At the post-correction boundary, the recorded #6 receipt/cause, exact `multi_gateway_lifecycle_cleanup_includes_delete_markers` RED→GREEN, complete nonlive GREEN matrix, and unchanged 53-path manifest authorized exactly one #7. That authorization was consumed by the failed attempt recorded in Step 6. The later delegated continuation has its own narrower authority in Steps 7-10 and does not reuse the #7 authorization.

- [ ] **Step 6: Record consumed failed attempt #7 before the delegated bounded continuation**

**Historical #7 gate:** **FAILED / CONSUMED.** Attempt #7 used the sole authorization from Step 5 on a fresh owned runner. The following command is retained only as the exact historical invocation; it must not be rerun.

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -Run
```

**Observed #7 execution receipt:** preflight, Compose configuration, offline build, Compose up, health checks, network checks, and metadata capture all passed on the fresh owned topology. The locked Rust sequence then passed `postgres_versioning`; passed `postgres_lifecycle`, including the hard-loss worker-A abort, worker-B reclaim, and stale-CAS boundary; and passed `e2e`. The normal `cargo test --test multi_gateway -- --nocapture --test-threads=1` command then failed at stage `multi-gateway`. Signed `integration lifecycle_expiration_invariants` and every AWS lifecycle scenario were not run.

**Observed #7 cleanup receipt:** bounded diagnostics were attempted; Compose down passed; project-ownership retry status was `not-needed`; exact image removal passed; independent residual container/network/volume/image counts were all zero; temporary-root removal and environment restoration passed; and `cleanup-errors=0`. The terminal runner result was exactly `FAILED reason=execution-failed`. No Git write occurred.

The previous test-only delete-marker cleanup correction therefore did not close the live failure. This receipt does not identify a new cause, and this plan must not invent or infer one. Attempt #7 is consumed. At that historical boundary local evidence stayed `NOT RUN`, exact hosted boundary stayed `HOSTED lifecycle-expiration: NOT RUN`, README and ROADMAP remained unchanged, `.debug-journal.md` was retained, and Task 14 stayed closed. The user's later `继续完成` plus full-session delegation authorized only the stage instrumentation and diagnostic #8 recorded below; #8's non-qualifying outcome consumed and closed that continuation.

**Unconfirmed source hypothesis:** `wait_for_lifecycle_race_terminal` returns after three stable S3-visible polls but does not prove the background lifecycle action is terminal. Cleanup then consumes only one ListObjectVersions XML snapshot. A later action could create or promote a delete marker while exact deletes run, make an exact delete return 404, and leave DeleteBucket observing `BucketNotEmpty`. This is plausible source inspection only, not a confirmed #7 cause; Step 8 exists to localize the last safe test stage before any correction is allowed.

- [ ] **Step 7: Add a fixed safe lifecycle-race stage receipt and prove its Docker-free contract RED→GREEN**

Modify only `tests/multi_gateway.rs`, `scripts/lifecycle-expiration-smoke.ps1`, `tests/client-smoke.Tests.ps1`, and `tests/multi-gateway.Tests.ps1`, all already present in the 53-path manifest. First extend the two PowerShell static contracts so they require the exact fixed test name, four allowlisted stages, anchored parser grammar, safe last-stage emission, and source-order stage calls. Run both contracts against the current source and require RED specifically because the fixed stage receipt is absent; an AST error or unrelated contract failure is not this RED.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$clientStageRed = $LASTEXITCODE
if ($clientStageRed -ne 1) { throw "Client static stage-receipt RED returned an unexpected exit code" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$multiStageRed = $LASTEXITCODE
if ($multiStageRed -ne 1) { throw "Multi-gateway static stage-receipt RED returned an unexpected exit code" }
```

Add the following test-only type and emitter to `tests/multi_gateway.rs`. These are the only emitted values: the fixed test name and one fixed enum string. They contain no endpoint, bucket, key, version ID, XML, body, error, URL, secret, or raw response.

```rust
const LIFECYCLE_RACE_TEST_NAME: &str =
    "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleRaceStage {
    TerminalStateEvaluation,
    SuccessorRead,
    VersionCleanup,
    BucketDelete,
}

impl LifecycleRaceStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TerminalStateEvaluation => "terminal-state-evaluation",
            Self::SuccessorRead => "successor-read",
            Self::VersionCleanup => "version-cleanup",
            Self::BucketDelete => "bucket-delete",
        }
    }
}

fn record_lifecycle_race_stage(stage: LifecycleRaceStage) {
    eprintln!(
        "[LIFECYCLE-RACE-STAGE] test={LIFECYCLE_RACE_TEST_NAME} stage={}",
        stage.as_str()
    );
}
```

Call `record_lifecycle_race_stage` at these exact boundaries in the named race test: immediately after `wait_for_lifecycle_race_terminal` and before terminal-state evaluation; immediately before the terminal successor GET; immediately before public-version cleanup; and immediately before DeleteBucket. Do not emit a stage from any other test or from production code.

Extend `Get-MultiGatewayFailureReceipt` with an anchored allowlisted parser. A broad prefix match must have the same count as the strict match, so an unknown stage, suffix, payload, or malformed line is rejected. `LastStage` is `not-reached` only when no stage line exists; when the named race test is among `FailedNames`, at least one strict stage is mandatory. Repeated `version-cleanup`/`bucket-delete` lines remain valid for the later bounded convergence loop; the last strict match is the receipt.

```powershell
$stagePrefixMatches = [regex]::Matches(
    $output,
    '(?m)^\[LIFECYCLE-RACE-STAGE\].*\r?$'
)
$stageMatches = [regex]::Matches(
    $output,
    '(?m)^\[LIFECYCLE-RACE-STAGE\] test=multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome stage=(?<stage>terminal-state-evaluation|successor-read|version-cleanup|bucket-delete)\r?$'
)
if ($stagePrefixMatches.Count -ne $stageMatches.Count) {
    throw "Multi-gateway diagnostic stage receipt is invalid"
}
$lastStage = if ($stageMatches.Count -eq 0) {
    "not-reached"
} else {
    $stageMatches[$stageMatches.Count - 1].Groups['stage'].Value
}
$raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
if ($names -contains $raceTestName -and $lastStage -ceq "not-reached") {
    throw "Named lifecycle race failure omitted its safe stage receipt"
}
```

Add `LastStage = $lastStage` to the returned object. `Invoke-LifecycleMultiGatewayDiagnostic` emits only fixed safe line `multi-gateway-last-stage=$($receipt.LastStage)` after the existing count/name/category lines. Extend `tests/client-smoke.Tests.ps1` with pure accepted fixtures ending at each of the four stages and with repeated cleanup/delete stages; reject unknown/missing/malformed/payload stages and a named race failure with `not-reached`. Extend `tests/multi-gateway.Tests.ps1` to require exactly one fixed emitter, all four enum strings, the four boundary calls in source order, and no dynamic formatting input other than `stage.as_str()`.

Use this pure receipt-fixture shape after `Invoke-Expression $multiGatewayParserSource`; the existing `Test-MultiGatewayFailureReceiptRejected` helper remains the rejection oracle:

```powershell
$raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
$safeStages = @(
    "terminal-state-evaluation",
    "successor-read",
    "version-cleanup",
    "bucket-delete"
)
foreach ($stage in $safeStages) {
    $receipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 1 test",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=$stage",
            "test $raceTestName ... FAILED",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
        )
        StdErr = @("fixture assertion")
    })
    Test-MultiGatewayDiagnosticContract (
        $receipt.LastStage -ceq $stage
    ) "Multi-gateway receipt parser rejected safe stage $stage"
}
$repeatedReceipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
    StdOut = @(
        "running 1 test",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=terminal-state-evaluation",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-read",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
        "test $raceTestName ... FAILED",
        "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s"
    )
    StdErr = @("fixture assertion")
})
Test-MultiGatewayDiagnosticContract (
    $repeatedReceipt.LastStage -ceq "bucket-delete"
) "Multi-gateway receipt parser did not retain the last bounded stage"
foreach ($rejected in @(
    @("running 1 test", "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=unknown", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
    @("running 1 test", "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete payload=raw", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
    @("running 1 test", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s")
)) {
    Test-MultiGatewayDiagnosticContract (
        Test-MultiGatewayFailureReceiptRejected -Lines $rejected
    ) "Multi-gateway receipt parser accepted an unsafe stage fixture"
}
```

Run the focused compile, both static contracts, runner AST/no-run path, and whitespace check. These are Docker-free; do not invoke either live switch.

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Stage receipt formatting failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Stage-instrumented multi-gateway target did not compile" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Safe stage receipt client contract failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Safe stage receipt multi-gateway contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Stage-instrumented runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Stage-instrumented runner no-run contract failed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Stage receipt candidate whitespace check failed" }
```

- [ ] **Step 8: Record consumed non-qualifying diagnostic #8 and close the reopened branch**

Attempt #8 used the existing fresh owned five-service topology and `-DiagnoseMultiGateway` mode. The following command is retained only as its historical invocation and must not be rerun.

```powershell
$diagnosticOutput = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" -DiagnoseMultiGateway 2>&1)
$diagnosticExit = $LASTEXITCODE
if ($diagnosticExit -notin @(0, 1)) { throw "Diagnostic attempt #8 returned an invalid exit code" }
```

**Observed #8 execution receipt:** preflight, configuration, offline build, Compose up, health checks, network checks, and metadata capture passed. The runner reached the normal multi-gateway diagnostic command and then emitted only generic `failed-stage=multi-gateway`; it emitted no safe running, passed, failed, test-name, category, or `LastStage` receipt. Therefore safe parser/command qualification failed. No cause is inferred from the missing receipt.

**Observed #8 cleanup receipt:** diagnostics were attempted; project-ownership retry was `not-needed`; Compose down passed; image probe was `not-needed`; exact image removal passed; independent residual container/network/volume/image counts were all zero; temporary-root removal and environment restoration passed; and `cleanup-errors=0`. Final result was exactly `FAILED reason=execution-failed`.

Attempt #8 is consumed. At the pre-delegation boundary its missing mandatory safe receipt authorized no correction or later attempt. Evidence remained `NOT RUN`; exact hosted boundary remained `HOSTED lifecycle-expiration: NOT RUN`; README and ROADMAP remained unchanged; `.debug-journal.md` was retained; no review/commit/push/tag was authorized; Task 13 was blocked; and Task 14 was closed.

The autonomous-completion delegation historically authorized all live rounds through the fresh deterministic-token final normal validation; all are consumed and none may be rerun. Step 101 records the reviewer follow-up terminal/max-attempt correction and targeted/nonlive proof; it authorizes no live rerun because the main path remains covered by the fresh PASS and changed terminal paths have deterministic targeted coverage. After a current plan-critic receipt, Task 14 alone may perform current-identity verification and canonical Oracle/Reviewer re-review before the existing single integrated commit boundary. Push and tag remain forbidden.

- [ ] **Step 9: Add a mutually-exclusive exact lifecycle-race diagnostic mode and prove it Docker-free**

Modify only existing manifest paths `scripts/lifecycle-expiration-smoke.ps1` and `tests/client-smoke.Tests.ps1`; retain the existing stage instrumentation in `tests/multi_gateway.rs` and its `tests/multi-gateway.Tests.ps1` contract unchanged. Add third top-level switch `[switch]$DiagnoseLifecycleRaceExact`. Exactly zero or one of `Run`, `DiagnoseMultiGateway`, and `DiagnoseLifecycleRaceExact` may be present: zero preserves the exact `NOT RUN` receipt, more than one fails before tool/image/topology access.

```powershell
$selectedModeCount = @(
    $Run.IsPresent,
    $DiagnoseMultiGateway.IsPresent,
    $DiagnoseLifecycleRaceExact.IsPresent
).Where({ $_ }).Count
if ($selectedModeCount -gt 1) {
    throw "Lifecycle runner modes are mutually exclusive"
}
if ($selectedModeCount -eq 0) {
    "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"
    return
}
```

First extend `tests/client-smoke.Tests.ps1` to require the third switch, three-way mutual exclusion, exact parser/function signatures, exact cargo argument order, no normal suite/AWS call, fixed receipts, and accepted/rejected pass/failure fixtures. Run it against the current two-switch runner and require exit `1` only because this bounded mode is absent.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$exactModeRed = $LASTEXITCODE
if ($exactModeRed -ne 1) { throw "Exact lifecycle-race diagnostic static RED returned an unexpected exit code" }
```

Use exact function boundaries `Get-LifecycleRaceExactReceipt([object]$Result, [ValidateSet(0, 101)][int]$ExitCode) -> [pscustomobject]` and `Invoke-LifecycleRaceExactDiagnostic([hashtable]$State) -> void`; each parameter is mandatory and neither function accepts pipeline input or additional arguments.

`Get-LifecycleRaceExactReceipt` combines captured stdout/stderr in memory but never emits them. For exit `101`, call existing `Get-MultiGatewayFailureReceipt` and accept only `Running=1`, `Passed=0`, `Failed=1`, `Ignored=0`, `Measured=0`, exact sole failed name `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`, one existing fixed category, and `LastStage` in the existing four-value enum. For exit `0`, require exactly one anchored `running 1 test`, exactly one anchored success summary with `1 passed; 0 failed`, no failed-name line, no unknown stage-prefix line, and final strict stage `bucket-delete`; return fixed category/name values `none`. Every mismatch throws a fixed message.

The success and failure branches return exactly these shapes after validation:

```powershell
$passReceipt = [pscustomobject]@{
    Outcome = "passed"
    Running = 1
    Passed = 1
    Failed = 0
    FailedName = "none"
    FirstErrorCategory = "none"
    LastStage = "bucket-delete"
}
$failureReceipt = [pscustomobject]@{
    Outcome = "failed"
    Running = 1
    Passed = 0
    Failed = 1
    FailedName = $raceTestName
    FirstErrorCategory = $baseReceipt.FirstErrorCategory
    LastStage = $baseReceipt.LastStage
}
```

`Invoke-LifecycleRaceExactDiagnostic` must use the same owned endpoint environment and `multi-gateway` stage but invoke exactly:

`cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1`

```powershell
$result = Invoke-NativeCommand `
    -FilePath "cargo" `
    -ArgumentList @(
        "test",
        "--test",
        "multi_gateway",
        "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
        "--",
        "--exact",
        "--nocapture",
        "--test-threads=1"
    ) `
    -Label "Owned exact lifecycle-race diagnostic" `
    -Timeout $RustTestTimeout `
    -AllowedExitCodes @(0, 101) `
    -WorkingDirectory $RepoRoot
$receipt = Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode
```

Emit only `exact-race-running`, `exact-race-passed`, `exact-race-failed`, `exact-race-failed-test`, `exact-race-first-error-category`, and `exact-race-last-stage` through `Write-LifecycleEvidence`. Set `DiagnosticOutcome` to fixed `exact-race-passed` or `exact-race-failed`; a captured failure terminates with fixed reason `exact-race-diagnostic-captured`. The exact branch occurs after metadata and is mutually exclusive with existing full diagnostic, normal five Rust suites, and AWS evidence.

Static fixtures must accept one exact PASS ending at `bucket-delete` and one exact FAIL for each existing stage/category, then reject running counts other than one, wrong/multiple/absent failed names, pass without `bucket-delete`, unknown/payload stages, inconsistent summaries, raw-output calls, and any additional native command. Run Docker-free GREEN only:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Exact diagnostic formatting failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Exact diagnostic multi-gateway target did not compile" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Exact lifecycle-race diagnostic static contract failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Existing lifecycle-race stage static contract regressed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Exact diagnostic runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Exact diagnostic runner no-run contract failed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Exact diagnostic candidate whitespace check failed" }
```

- [ ] **Step 10: Record consumed exact-test diagnostic #9 with no qualifying safe receipt**

Attempt #9 used the dedicated exact-test mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun.

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #9 execution receipt:** preflight, configuration, offline build, Compose up, health checks, network checks, and metadata capture passed. The exact named command was invoked. No `exact-race-running`, passed, failed, failed-name, category, or `LastStage` safe receipt was emitted; only generic `failed-stage=multi-gateway` was available. Final result was exactly `FAILED reason=execution-failed`. No test outcome, stage, or cause is inferred.

**Observed #9 cleanup receipt:** diagnostics were attempted; project-ownership retry was `not-needed`; Compose down passed; image removal passed; temporary-root removal and environment restoration passed; independent residual container/network/volume/image counts were all zero; and `cleanup-errors=0`.

Attempt #9 is consumed. It selected neither existing Branch A nor Branch B. Its then-reviewed continuation authorized the Docker-free hardening in Step 11 and exact-test #10 in Step 12; that later authorization is also consumed by the receipt recorded below. Evidence/README/ROADMAP remained unchanged.

- [ ] **Step 11: Harden exact-marker parsing and command classification Docker-free**

Modify only existing manifest paths `scripts/lifecycle-expiration-smoke.ps1` and `tests/client-smoke.Tests.ps1`. Keep `[switch]$DiagnoseLifecycleRaceExact`, the exact named cargo command, and `tests/multi_gateway.rs` stage emitter unchanged. Do not add a live command, normal-suite call, AWS call, raw-output emission, source identifier, endpoint, XML/body, or arbitrary error text.

First extend pure `tests/client-smoke.Tests.ps1` fixtures to require prefixed/interleaved fixed markers and the fixed command/parser categories below. Run against the current anchored parser and require exit `1` because prefixed valid markers are rejected; an AST error or unrelated contract failure is not this RED.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$parserHardeningRed = $LASTEXITCODE
if ($parserHardeningRed -ne 1) { throw "Exact diagnostic parser-hardening RED returned an unexpected exit code" }
```

Replace line-anchored stage matching with two layers. The exact token may occur after a whitespace-delimited harness prefix anywhere in a line. The broad match starts at every marker prefix and retains the remainder of that line solely for validation. Every broad value must match the exact fixed test/stage token followed only by horizontal whitespace; therefore unknown stages, connected suffixes, and `payload=...` suffixes are rejected. Reduce only strict matches to the last allowlisted enum.

```powershell
$raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
$stageEnum = "terminal-state-evaluation|successor-read|version-cleanup|bucket-delete"
$broadStageMatches = [regex]::Matches(
    $output,
    '(?m)\[LIFECYCLE-RACE-STAGE\][^\r\n]*'
)
$strictStageMatches = [regex]::Matches(
    $output,
    "(?m)(?<!\S)\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=(?<stage>$stageEnum)(?=[ \t]*$)"
)
$stageRejected = $false
foreach ($broad in $broadStageMatches) {
    if ($broad.Value -cnotmatch "^\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=($stageEnum)[ \t]*$") {
        $stageRejected = $true
        break
    }
}
$lastStage = if ($strictStageMatches.Count -eq 0) {
    "not-reached"
} else {
    $strictStageMatches[$strictStageMatches.Count - 1].Groups['stage'].Value
}
```

Continue parsing anchored running/summary lines when present. Exactly one running plus exactly one matching success/failure summary must satisfy the existing arithmetic and exact-test counts. If both are absent but an allowlisted stage survives, set fixed `CountShape=unavailable` rather than discarding the safe stage. If only one exists, duplicates exist, or counts disagree, set `ParserFailureCategory=output-shape-rejected`. Stage outcomes use `none`, `stage-missing`, or `stage-rejected`; they never include matched text.

The runner catches only fixed `Invoke-NativeCommand` outcomes and emits no exception message:

```text
CommandOutcome = allowed-exit-0 | allowed-exit-101 | timeout | nonallowed-exit | command-error
ParserFailureCategory = none | output-shape-rejected | stage-missing | stage-rejected
CountShape = available | unavailable | rejected
```

Map exact fixed exception messages for label `Owned exact lifecycle-race diagnostic`: timeout → `timeout`, disallowed process exit → `nonallowed-exit`, every other exception → `command-error`. Allowed exits remain `allowed-exit-0` and `allowed-exit-101`. Because the cargo filter fixes test identity, an allowed exit `101` plus a valid allowlisted stage may safely report fixed failed name even when `CountShape=unavailable`; available counts must still be exactly `running=1/passed=0/failed=1`. Allowed exit `0` requires available exact pass counts when present and last stage `bucket-delete`. Any rejected stage never qualifies.

Emit only existing exact-race fields plus fixed `exact-race-command-outcome`, `exact-race-parser-failure-category`, and `exact-race-count-shape`. Static fixtures must include:

- valid marker at line start;
- `test-harness-prefix: <valid-marker>` and a second unrelated safe prefix before a valid marker;
- multiple valid markers reduced to the last enum;
- valid stage with no running/summary, yielding `CountShape=unavailable`;
- exact available PASS and FAIL counts;
- unknown stage, connected suffix, and whitespace `payload=raw` suffix, all `stage-rejected`;
- duplicate/partial/inconsistent running summaries as `output-shape-rejected`;
- timeout, nonallowed exit, and generic command error mapped to fixed `CommandOutcome` values without raw text.

Run Docker-free GREEN. A separate Rust emitter test is unnecessary unless static fixtures reveal the fixed emitter token itself differs; if needed, add only a pure exact-string test in already-manifested `tests/multi_gateway.rs` and keep the live command filter unchanged.

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Parser hardening formatting failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Parser-hardened multi-gateway target did not compile" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Parser-hardening static fixtures failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle-race emitter static contract regressed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Parser-hardened runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Parser-hardened runner no-run contract failed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Parser-hardening whitespace check failed" }
```

- [ ] **Step 12: Record consumed exact-test diagnostic #10 with stage missing**

Attempt #10 used the hardened exact-test mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun.

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #10 execution receipt:** all topology and metadata gates passed and the exact named command was invoked. `CommandOutcome=allowed-exit-101`, `CountShape=available`, `running=1`, `passed=0`, `failed=1`, failed name exactly `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`, first category `assertion`, `LastStage=not-reached`, and `ParserFailureCategory=stage-missing`. Final result was exactly `FAILED reason=exact-race-diagnostic-captured`. No source region or product cause is inferred from a missing stage.

**Observed #10 cleanup receipt:** cleanup diagnostics were attempted; project-ownership retry was `not-needed`; Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were all zero; and `cleanup-errors=0`.

Attempt #10 is consumed. It selected neither Branch A nor Branch B. Its then-reviewed continuation authorized the Docker-free fixed-stage expansion in Step 13 and exact-test #11 in Step 14; that authorization is also consumed by the receipt recorded below. Evidence/README/ROADMAP remained unchanged, and #10 ran no normal/AWS stage.

- [ ] **Step 13: Expand fixed test-only stages to localize pre-terminal failure Docker-free**

Modify only existing manifest paths `tests/multi_gateway.rs`, `tests/multi-gateway.Tests.ps1`, `scripts/lifecycle-expiration-smoke.ps1`, and `tests/client-smoke.Tests.ps1`. Add no dynamic values and change no request, assertion, synchronization, cleanup, product, runner topology, or command behavior. Extend only the test enum, fixed emitter calls, parser allowlist, and static fixtures.

First extend both PowerShell static contracts to require all new enum values, exact string mappings, exact source boundaries, expanded parser allowlist, accepted prefixed fixtures for every stage, and unknown/payload rejection. Run them before Rust/runner changes and require exit `1` only because the nine new stages are absent.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$stageExpansionRustRed = $LASTEXITCODE
if ($stageExpansionRustRed -ne 1) { throw "Pre-terminal stage Rust static RED returned an unexpected exit code" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$stageExpansionParserRed = $LASTEXITCODE
if ($stageExpansionParserRed -ne 1) { throw "Pre-terminal stage parser static RED returned an unexpected exit code" }
```

Replace `LifecycleRaceStage` and its `as_str()` match with this exact fixed thirteen-stage definition:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleRaceStage {
    BucketCreated,
    VersioningEnabled,
    LifecycleConfigured,
    PredecessorCreated,
    RaceStarted,
    SuccessorResponse,
    SuccessorObserved,
    LifecycleConfigDeleted,
    TerminalWaitEntered,
    TerminalStateEvaluation,
    SuccessorRead,
    VersionCleanup,
    BucketDelete,
}

impl LifecycleRaceStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BucketCreated => "bucket-created",
            Self::VersioningEnabled => "versioning-enabled",
            Self::LifecycleConfigured => "lifecycle-configured",
            Self::PredecessorCreated => "predecessor-created",
            Self::RaceStarted => "race-started",
            Self::SuccessorResponse => "successor-response",
            Self::SuccessorObserved => "successor-observed",
            Self::LifecycleConfigDeleted => "lifecycle-config-deleted",
            Self::TerminalWaitEntered => "terminal-wait-entered",
            Self::TerminalStateEvaluation => "terminal-state-evaluation",
            Self::SuccessorRead => "successor-read",
            Self::VersionCleanup => "version-cleanup",
            Self::BucketDelete => "bucket-delete",
        }
    }
}
```

Insert calls at these exact test-only boundaries, always immediately after a successful boundary except `RaceStarted` and `TerminalWaitEntered`, which are immediately before the bounded operation they localize:

```rust
let (bucket_name, _bucket_a) = create_bucket_at(&endpoint_a, "lifecycle-race").await;
record_lifecycle_race_stage(LifecycleRaceStage::BucketCreated);

// Immediately after the existing successful versioning assertion.
record_lifecycle_race_stage(LifecycleRaceStage::VersioningEnabled);

// Immediately after the existing successful lifecycle PUT assertion.
record_lifecycle_race_stage(LifecycleRaceStage::LifecycleConfigured);

let predecessor_version = version_id(&predecessor, "predecessor PUT");
record_lifecycle_race_stage(LifecycleRaceStage::PredecessorCreated);

// Immediately before tokio::join!(publish, observe).
record_lifecycle_race_stage(LifecycleRaceStage::RaceStarted);
let (successor, first_observed_versions) = tokio::join!(publish, observe);
record_lifecycle_race_stage(LifecycleRaceStage::SuccessorResponse);

// Immediately after the existing first_observed_versions successor assertion.
record_lifecycle_race_stage(LifecycleRaceStage::SuccessorObserved);

// Immediately after the existing lifecycle DELETE 204 assertion.
record_lifecycle_race_stage(LifecycleRaceStage::LifecycleConfigDeleted);

// Immediately before wait_for_lifecycle_race_terminal(...).await.
record_lifecycle_race_stage(LifecycleRaceStage::TerminalWaitEntered);
```

Retain existing calls for `TerminalStateEvaluation`, `SuccessorRead`, `VersionCleanup`, and `BucketDelete`. Update `$stageEnum` and every strict/broad validator to this exact source-ordered union:

```powershell
$stageEnum = "bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete"
```

`tests/multi-gateway.Tests.ps1` must extract the named race-test region and prove all thirteen calls occur once and in order. `tests/client-smoke.Tests.ps1` must accept line-start and safely prefixed fixtures for every new stage, reduce repeated valid stages to the last enum, and continue rejecting unknown stages, connected suffixes, and whitespace payload suffixes without raw output.

Run only Docker-free GREEN:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Expanded lifecycle stages formatting failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Expanded lifecycle-stage target did not compile" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded lifecycle-stage source contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded lifecycle-stage parser fixtures failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Expanded lifecycle-stage runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File "scripts/lifecycle-expiration-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") {
    throw "Expanded lifecycle-stage runner no-run contract failed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Expanded lifecycle-stage whitespace check failed" }
```

- [ ] **Step 14: Record consumed exact-test diagnostic #11 and its historical all-object confound**

Attempt #11 used the expanded-stage exact-test mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun.

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #11 execution receipt:** all topology and metadata gates passed and the exact named test ran. `CommandOutcome=allowed-exit-101`, `CountShape=available`, `running=1`, `passed=0`, `failed=1`, exact failed name `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`, first category `assertion`, `ParserFailureCategory=none`, and `LastStage=race-started`. Final result was exactly `FAILED reason=exact-race-diagnostic-captured`.

**Observed #11 cleanup receipt:** cleanup diagnostics were attempted; project-ownership retry was `not-needed`; Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were all zero; and `cleanup-errors=0`.

Attempt #11 is consumed. Its valid `race-started` receipt localizes the failure to joined successor publication/observation before `SuccessorResponse`. Source inspection identified a test-only confound, not a confirmed cause: the past-Date `<Filter/>` rule legally matched both the predecessor and newly published untagged successor. The fixed Tag correction removed only that confound without changing product code, worker behavior, database paths, synchronization, or lifecycle semantics. Consumed #12 later reproduced the same `race-started` boundary, so this plan must not claim that Tag isolation solved the observed failure.

The original stage map is retained for history:

| Last safe stage | Localized source region before the next stage |
|---|---|
| `bucket-created` | signed bucket-versioning PUT and its 200 assertion |
| `versioning-enabled` | signed lifecycle-configuration PUT and its 200 assertion |
| `lifecycle-configured` | predecessor PUT, response assertion, and version-ID extraction |
| `predecessor-created` | barrier/future construction before race start |
| `race-started` | joined successor publication and successor-observation polling |
| `successor-response` | successor response assertion and version-ID extraction |
| `successor-observed` | lifecycle-configuration DELETE and its 204 assertion |
| `lifecycle-config-deleted` | transition into terminal-wait invocation |
| `terminal-wait-entered` | `wait_for_lifecycle_race_terminal` polling/convergence |
| `terminal-state-evaluation` | allowed terminal-state match and assertion |
| `successor-read` | terminal successor GET/status/body assertions |
| `version-cleanup` or `bucket-delete` | existing public-S3 cleanup Branch A (Step 15) |

The actual #11 `race-started` outcome selected neither historical Branch A nor historical Branch B. Step 17 records the subsequently completed test-only Tag-isolation correction/nonlive proof, while Step 18 records that #12 remained at `race-started`. Evidence/README/ROADMAP remain unchanged, and neither exact diagnostic ran a normal/AWS stage.

- [ ] **Step 15: Historical cleanup Branch A — #11 did not select it; do not execute**

The actual #11 stage is `race-started`, not `version-cleanup`/`bucket-delete`. The convergence design below is retained only as historical unselected plan content. None of its RED/GREEN commands, helper code, static edits, or nonlive matrix is authorized or claimed.

First add pure test `multi_gateway_lifecycle_cleanup_converges_after_late_marker` and the minimal compiling classifier scaffold that handles existing success paths but treats `BucketNotEmpty` as unexpected. The test requires the sequence `RetryBucketNotEmpty` then `Complete`, accepts exact-delete `204` and `404/NoSuchVersion`, and rejects `404/NoSuchBucket`, unrelated 409, and 5xx. Run the exact test and require an assertion RED where `409/BucketNotEmpty` is `Err` instead of `RetryBucketNotEmpty`; no other failure is valid. Also extend `tests/multi-gateway.Tests.ps1` first and require its RED because the bounded re-list loop is absent.

Use the enums from the final contract below with this temporary RED classifier body, then replace only this body during GREEN:

```rust
fn classify_lifecycle_race_cleanup_response(
    operation: LifecycleCleanupOperation,
    status: u16,
    error_code: Option<&str>,
) -> Result<LifecycleCleanupDisposition, &'static str> {
    use LifecycleCleanupDisposition::{Complete, Continue};
    use LifecycleCleanupOperation::{BucketDelete, ExactVersionDelete};

    match (operation, status, error_code) {
        (ExactVersionDelete, 204, _) => Ok(Continue),
        (ExactVersionDelete, 404, Some("NoSuchVersion")) => Ok(Continue),
        (BucketDelete, 204, _) => Ok(Complete),
        _ => Err("unexpected lifecycle race cleanup response"),
    }
}
```

```powershell
cargo test --test multi_gateway multi_gateway_lifecycle_cleanup_converges_after_late_marker -- --exact --nocapture --test-threads=1
$cleanupPureRed = $LASTEXITCODE
if ($cleanupPureRed -ne 101) { throw "Cleanup convergence pure RED returned an unexpected exit code" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$cleanupStaticRed = $LASTEXITCODE
if ($cleanupStaticRed -ne 1) { throw "Cleanup convergence static RED returned an unexpected exit code" }
```

Use these exact test-local types and classifier contracts:

```rust
const LIFECYCLE_RACE_CLEANUP_MAX_ROUNDS: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleCleanupOperation {
    ExactVersionDelete,
    BucketDelete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleCleanupDisposition {
    Continue,
    Complete,
    RetryBucketNotEmpty,
}

fn classify_lifecycle_race_cleanup_response(
    operation: LifecycleCleanupOperation,
    status: u16,
    error_code: Option<&str>,
) -> Result<LifecycleCleanupDisposition, &'static str> {
    use LifecycleCleanupDisposition::{Complete, Continue, RetryBucketNotEmpty};
    use LifecycleCleanupOperation::{BucketDelete, ExactVersionDelete};

    match (operation, status, error_code) {
        (ExactVersionDelete, 204, _) => Ok(Continue),
        (ExactVersionDelete, 404, Some("NoSuchVersion")) => Ok(Continue),
        (BucketDelete, 204, _) => Ok(Complete),
        (BucketDelete, 409, Some("BucketNotEmpty")) => Ok(RetryBucketNotEmpty),
        _ => Err("unexpected lifecycle race cleanup response"),
    }
}
```

Add a test-local `s3_error_code(xml: &str) -> Option<&str>` that returns only nonempty text between the first exact `<Code>` and `</Code>` pair and never logs the XML. The pure test must include the bounded late-marker decision sequence and every allowlist/rejection case:

```rust
fn s3_error_code(xml: &str) -> Option<&str> {
    const OPEN: &str = "<Code>";
    const CLOSE: &str = "</Code>";
    let start = xml.find(OPEN)? + OPEN.len();
    let tail = &xml[start..];
    let end = tail.find(CLOSE)?;
    let code = tail[..end].trim();
    (!code.is_empty()).then_some(code)
}
```

```rust
#[test]
fn multi_gateway_lifecycle_cleanup_converges_after_late_marker() {
    use LifecycleCleanupDisposition::{Complete, Continue, RetryBucketNotEmpty};
    use LifecycleCleanupOperation::{BucketDelete, ExactVersionDelete};

    assert_eq!(
        [
            classify_lifecycle_race_cleanup_response(
                BucketDelete,
                409,
                Some("BucketNotEmpty"),
            ),
            classify_lifecycle_race_cleanup_response(BucketDelete, 204, None),
        ],
        [Ok(RetryBucketNotEmpty), Ok(Complete)]
    );
    assert_eq!(
        classify_lifecycle_race_cleanup_response(ExactVersionDelete, 204, None),
        Ok(Continue)
    );
    assert_eq!(
        classify_lifecycle_race_cleanup_response(
            ExactVersionDelete,
            404,
            Some("NoSuchVersion"),
        ),
        Ok(Continue)
    );
    assert!(
        classify_lifecycle_race_cleanup_response(
            ExactVersionDelete,
            404,
            Some("NoSuchBucket"),
        )
        .is_err()
    );
    assert!(
        classify_lifecycle_race_cleanup_response(BucketDelete, 409, Some("OperationAborted"))
            .is_err()
    );
    assert!(
        classify_lifecycle_race_cleanup_response(BucketDelete, 500, Some("InternalError"))
            .is_err()
    );
    assert_eq!(
        s3_error_code("<Error><Code>BucketNotEmpty</Code></Error>"),
        Some("BucketNotEmpty")
    );
}
```

Place the types, error-code parser, and pure test before the helper, then place `async fn converge_lifecycle_race_cleanup(endpoint: &str, bucket: &str, key: &str)` immediately before the named race test. Implement exactly eight or fewer rounds. Every round records `VersionCleanup`, performs a fresh signed ListObjectVersions, requires 200, parses that round's XML with existing `version_ids_for_cleanup`, and exact-deletes every returned Version/DeleteMarker ID. Classify each exact delete using its status and, only for non-204, its bounded XML error code; continue only for 204 or 404/`NoSuchVersion`. Then record `BucketDelete` and attempt signed DeleteBucket. Return only on 204. Retry another fresh list round only for 409/`BucketNotEmpty` and only while a round remains; use `tokio::task::yield_now().await`, never a correctness sleep. Fail with fixed messages for exhaustion or every unallowlisted response.

```rust
async fn converge_lifecycle_race_cleanup(endpoint: &str, bucket: &str, key: &str) {
    use LifecycleCleanupDisposition::{Complete, Continue, RetryBucketNotEmpty};
    use LifecycleCleanupOperation::{BucketDelete, ExactVersionDelete};

    for round in 0..LIFECYCLE_RACE_CLEANUP_MAX_ROUNDS {
        record_lifecycle_race_stage(LifecycleRaceStage::VersionCleanup);
        let list = signed_list_object_versions(endpoint, bucket, key).await;
        if list.status().as_u16() != 200 {
            panic!("lifecycle race cleanup list returned an unexpected response");
        }
        let versions = list
            .text()
            .await
            .expect("read lifecycle race cleanup version XML");

        for version_id in version_ids_for_cleanup(&versions) {
            let response =
                signed_delete_object_version(endpoint, bucket, key, &version_id).await;
            let status = response.status().as_u16();
            let error_code = if status == 204 {
                None
            } else {
                let body = response
                    .text()
                    .await
                    .expect("read lifecycle race exact-delete error XML");
                s3_error_code(&body).map(str::to_owned)
            };
            if classify_lifecycle_race_cleanup_response(
                ExactVersionDelete,
                status,
                error_code.as_deref(),
            ) != Ok(Continue)
            {
                panic!("lifecycle race exact-version cleanup returned an unexpected response");
            }
        }

        record_lifecycle_race_stage(LifecycleRaceStage::BucketDelete);
        let response = signed_delete_bucket(endpoint, bucket).await;
        let status = response.status().as_u16();
        let error_code = if status == 204 {
            None
        } else {
            let body = response
                .text()
                .await
                .expect("read lifecycle race bucket-delete error XML");
            s3_error_code(&body).map(str::to_owned)
        };
        match classify_lifecycle_race_cleanup_response(
            BucketDelete,
            status,
            error_code.as_deref(),
        ) {
            Ok(Complete) => return,
            Ok(RetryBucketNotEmpty)
                if round + 1 < LIFECYCLE_RACE_CLEANUP_MAX_ROUNDS =>
            {
                tokio::task::yield_now().await;
            }
            Ok(RetryBucketNotEmpty) => {
                panic!("lifecycle race cleanup did not converge within its bounded rounds");
            }
            Ok(Continue) | Err(_) => {
                panic!("lifecycle race bucket cleanup returned an unexpected response");
            }
        }
    }

    panic!("lifecycle race cleanup exhausted its bounded rounds");
}
```

Replace the one-snapshot cleanup loop and separate DeleteBucket assertion with exactly one call:

```rust
converge_lifecycle_race_cleanup(&endpoint_a, &bucket_name, key).await;
```

`tests/multi-gateway.Tests.ps1` must replace its one-shot cleanup stage-call shape with the convergence shape while retaining terminal-state/successor stage checks. Lock the maximum `8`, helper signature, fresh signed list inside the bounded loop, `version_ids_for_cleanup`, exact version DELETE, bucket DELETE, both exact error codes, fixed classifier, and absence of DB/worker/process/Docker/Kubo/sleep access. This static contract proves each retry re-lists public state rather than replaying one XML snapshot.

Extract exactly the helper-to-race-test source interval and enforce it with the existing static helpers:

```powershell
$cleanupStart = $RustLive.IndexOf(
    "async fn converge_lifecycle_race_cleanup(endpoint: &str, bucket: &str, key: &str)",
    [StringComparison]::Ordinal
)
$raceTestStart = $RustLive.IndexOf(
    "async fn multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome()",
    [StringComparison]::Ordinal
)
Assert-True (
    ([regex]::Matches(
        $RustLive,
        '(?m)^const LIFECYCLE_RACE_CLEANUP_MAX_ROUNDS: usize = 8;$'
    )).Count -eq 1
) "Lifecycle race cleanup must declare exactly one eight-round bound before the helper"
Assert-True (
    $cleanupStart -ge 0 -and $raceTestStart -gt $cleanupStart
) "Lifecycle race cleanup helper must immediately precede the race-test region"
$cleanupSource = $RustLive.Substring($cleanupStart, $raceTestStart - $cleanupStart)
foreach ($fragment in @(
    "for round in 0..LIFECYCLE_RACE_CLEANUP_MAX_ROUNDS",
    "signed_list_object_versions(endpoint, bucket, key).await",
    "version_ids_for_cleanup(&versions)",
    "signed_delete_object_version(endpoint, bucket, key, &version_id).await",
    "signed_delete_bucket(endpoint, bucket).await",
    'Some("NoSuchVersion")',
    'Some("BucketNotEmpty")',
    "tokio::task::yield_now().await"
)) {
    Assert-Contains $cleanupSource $fragment "Lifecycle race cleanup convergence is incomplete: $fragment"
}
Assert-Matches $cleanupSource '(?s)signed_list_object_versions.*?version_ids_for_cleanup.*?signed_delete_object_version.*?signed_delete_bucket' "Every cleanup round must list, exact-delete, then attempt bucket deletion"
Assert-NotMatches $cleanupSource '(?i)DatabaseConnection|\b(?:SELECT|INSERT|UPDATE|CREATE|DROP)\b|LifecycleWorker|Command::new|\bdocker\b|\bkubo\b|tokio::time::sleep|std::thread::sleep' "Lifecycle race cleanup must remain public-API-only and sleep-free"
```

Run the exact pure GREEN, target compile, both focused statics, LSP on `tests/multi_gateway.rs`, and then the complete nonlive matrix exactly once. No Docker/live command is allowed in this step.

```powershell
cargo test --test multi_gateway multi_gateway_lifecycle_cleanup_converges_after_late_marker -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Cleanup convergence pure regression failed" }
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Convergence candidate formatting failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Convergence candidate locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Convergence candidate library suite failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Convergence candidate serial integration failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Convergence multi-gateway target did not compile" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Convergence release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Convergence PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Convergence multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Convergence Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Convergence client static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Convergence candidate whitespace check failed" }
```

No receipt may be recorded from this unselected branch.

- [ ] **Step 16: Historical exact-PASS Branch B — #11 did not select it; do not execute**

The actual #11 outcome is `allowed-exit-101` at `race-started`, not exact PASS. The ordered-triage design below is retained only as historical unselected plan content; it authorizes no runner switch, source edit, or live attempt.

Before any next live command, update this plan with #11's safe receipt and add a fourth mutually-exclusive runner switch `DiagnoseMultiGatewayOrdered` plus Docker-free parser/static RED→GREEN. The new revision must receive a fresh plan-critic receipt. Only that reviewed revision may authorize the next monotonically numbered attempt, expected to be #12.

The ordered triage uses one fresh owned topology, no AWS, and these exact tests in order, one exact cargo process at a time so external topology state is retained while test-process interleaving is removed:

```text
multi_gateway_lifecycle_cleanup_includes_delete_markers
multi_gateway_cross_replica_contract
load_balancer_surviving_replica_crud
multi_gateway_lifecycle_configuration_visible_across_replicas
multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome
```

For each entry invoke `cargo test --test multi_gateway <fixed-name> -- --exact --nocapture --test-threads=1`. A dedicated parser accepts only `running=1`; either exact PASS `1/0` or exact FAIL `0/1` with the currently selected fixed name; one existing fixed category; and stage `not-applicable` for the first four tests or one fixed lifecycle stage for the race test. Emit only fixed index/name/outcome/category/stage fields. Stop on the first failure. If all five exact commands pass, emit only `ordered-triage=all-exact-tests-passed`. Never emit raw output, IDs, endpoints, XML, bodies, or errors.

No receipt may be recorded from this unselected branch.

- [ ] **Step 17: Record completed tagged-predecessor test correction and full nonlive GREEN**

Only existing manifest paths `tests/multi_gateway.rs` and `tests/multi-gateway.Tests.ps1` changed. Production source, lifecycle evaluator/worker, database, runner, topology, synchronization, waits, assertions, and cleanup remained unchanged.

The scope-characterization static RED extracted the named race-test/configuration source and proved the old past-Date `<Filter/>` rule matched both untagged predecessor and successor. It failed before correction with the fixed all-object-scope assertion. No pure Rust RED was claimed, and this RED did not establish the cause of the `race-started` failure.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$tagIsolationStaticRed = $LASTEXITCODE
if ($tagIsolationStaticRed -ne 1) { throw "Lifecycle race tag-isolation static RED returned an unexpected exit code" }
```

The completed fixed tagged PUT helper differs from `signed_put_object` only by the signed literal `x-amz-tagging: lifecycle-race=expire` header and fixed diagnostic label; it accepts no dynamic tag input:

```rust
async fn signed_put_object_with_tagging(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static("lifecycle-race=expire"),
    );
    http_call(
        "signed PUT tagged object",
        send_sigv4(
            reqwest::Method::PUT,
            endpoint,
            bucket,
            key,
            &[],
            body,
            headers,
            "test",
        ),
    )
    .await
}
```

The completed dedicated configuration helper preserves the caller-supplied fixed rule ID/past Date action while replacing only `<Filter/>` with the fixed Tag filter:

```rust
fn lifecycle_race_configuration_xml(rule_id: &str, expiration: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>lifecycle-race</Key><Value>expire</Value></Tag></Filter>\
         {expiration}</Rule></LifecycleConfiguration>"
    )
}
```

In the named race test only, the generic configuration call became `lifecycle_race_configuration_xml("due-publication-race", "<Expiration><Date>2000-01-01T00:00:00Z</Date></Expiration>")`; predecessor publication became `signed_put_object_with_tagging`; successor publication remains plain `signed_put_object`. `tests/multi-gateway.Tests.ps1` locks exact Tag XML/header literals, tagged predecessor call, untagged successor call, absence of `<Filter/>` from the dedicated helper/race region, all thirteen stage calls, and endpoint-only/no-DB/process-control boundaries.

The scope-characterization source static then passed GREEN; the `multi_gateway` target compiled and LSP diagnostics for `tests/multi_gateway.rs` were clean. The complete nonlive matrix ran once without Docker/live:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation formatting failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation library suite failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation serial integration failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation multi-gateway target did not compile" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation client static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tag-isolation whitespace check failed" }
```

Observed receipts: scope-characterization static RED then GREEN; target compile PASS; LSP clean; format PASS; locked build PASS; library `875/875`; serial integration `143/143`; multi-gateway target compile PASS; all five static contracts PASS (release, PostgreSQL, multi-gateway, Cluster, and client); final diff-check PASS. This correction removed the all-object confound only; #12 later reproduced the same `race-started` boundary, so no causal resolution is claimed.

- [ ] **Step 18: Record consumed exact-test diagnostic #12 without claiming causal closure**

Attempt #12 used the tagged candidate and unchanged exact-test mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #12 execution receipt:** topology and metadata passed; exact command `cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1` ran; `CommandOutcome=allowed-exit-101`; `CountShape=available`; `running=1`, `passed=0`, `failed=1`; exact failed name `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`; first category `assertion`; `ParserFailureCategory=none`; and `LastStage=race-started`. Final result was exactly `FAILED reason=exact-race-diagnostic-captured`.

**Observed #12 cleanup receipt:** cleanup diagnostics passed; project-ownership retry was `not-needed`; Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were exactly zero; and `cleanup-errors=0`.

Attempt #12 is consumed. The predecessor-only Tag filter removed the legal all-object match but did not change the `race-started` boundary, so it is not the cause resolution. Preserve product/test behavior until #13 returns a qualifying last-stage receipt; this step authorizes no code change or further live execution by itself.

- [ ] **Step 19: Write Docker-free RED contracts for seven fixed concurrent-join stages**

Modify only `tests/multi-gateway.Tests.ps1` and `tests/client-smoke.Tests.ps1` first. Require the existing thirteen stages plus these exact fixed no-data stages, in this enum/allowlist order between `race-started` and `successor-response`:

```powershell
$expectedConcurrentStages = @(
    "successor-request-dispatched",
    "successor-request-complete",
    "observer-loop-entered",
    "observer-get-response",
    "observer-list-response",
    "observer-list-status-ok",
    "observer-successor-visible"
)
$expectedStageEnum = "bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-request-dispatched|successor-request-complete|observer-loop-entered|observer-get-response|observer-list-response|observer-list-status-ok|observer-successor-visible|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete"
```

In `tests/multi-gateway.Tests.ps1`, extract the exact `let publish = async { ... };` region from the named race test and the exact `wait_for_signed_successor` function. Require `SuccessorRequestDispatched` immediately before the successor `signed_put_object` await and `SuccessorRequestComplete` immediately after assigning its response, with the response returned unchanged. Require `ObserverLoopEntered` before `loop`; then, within each observer iteration, signed GET await → `ObserverGetResponse` → signed ListObjectVersions await → `ObserverListResponse` → status-200 assertion → `ObserverListStatusOk`; require `ObserverSuccessorVisible` immediately before returning matching versions. Require each emitter call exactly once in source, while permitting runtime repetition of observer-loop iteration stages. Preserve `RaceStarted` before both futures and preserve existing `SuccessorResponse` only after `tokio::join!` returns.

```powershell
$raceStart = $RustLive.IndexOf("async fn multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome()", [StringComparison]::Ordinal)
$raceEnd = $RustLive.IndexOf("`n}", $raceStart, [StringComparison]::Ordinal)
$raceSource = $RustLive.Substring($raceStart, $raceEnd - $raceStart)
$publishStart = $raceSource.IndexOf("let publish = async {", [StringComparison]::Ordinal)
$observeStart = $raceSource.IndexOf("let observe = async {", [StringComparison]::Ordinal)
$joinIndex = $raceSource.IndexOf("let (successor, first_observed_versions) = tokio::join!(publish, observe);", [StringComparison]::Ordinal)
$raceStartedIndex = $raceSource.IndexOf("record_lifecycle_race_stage(LifecycleRaceStage::RaceStarted);", [StringComparison]::Ordinal)
$successorResponseIndex = $raceSource.IndexOf("record_lifecycle_race_stage(LifecycleRaceStage::SuccessorResponse);", [StringComparison]::Ordinal)
$publishSource = $raceSource.Substring($publishStart, $observeStart - $publishStart)
$dispatchIndex = $publishSource.IndexOf("record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestDispatched);", [StringComparison]::Ordinal)
$requestIndex = $publishSource.IndexOf("let response = signed_put_object(&endpoint_b, &bucket_name, key, successor_body.clone()).await;", [StringComparison]::Ordinal)
$completeIndex = $publishSource.IndexOf("record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestComplete);", [StringComparison]::Ordinal)
$returnIndex = $publishSource.IndexOf("`n        response`n", [StringComparison]::Ordinal)
Assert-True (
    $raceStartedIndex -ge 0 -and $publishStart -gt $raceStartedIndex -and
    $dispatchIndex -ge 0 -and $requestIndex -gt $dispatchIndex -and
    $completeIndex -gt $requestIndex -and $returnIndex -gt $completeIndex -and
    $joinIndex -gt $observeStart -and $successorResponseIndex -gt $joinIndex
) "Lifecycle race publish/join stage ordering is incomplete"

$observerStart = $RustLive.IndexOf("async fn wait_for_signed_successor(", [StringComparison]::Ordinal)
$observerEnd = $RustLive.IndexOf("async fn wait_for_lifecycle_race_terminal(", $observerStart, [StringComparison]::Ordinal)
$observerSource = $RustLive.Substring($observerStart, $observerEnd - $observerStart)
$observerOrder = @(
    "record_lifecycle_race_stage(LifecycleRaceStage::ObserverLoopEntered);",
    "loop {",
    "let response = signed_get_object(get_endpoint, bucket, key).await;",
    "record_lifecycle_race_stage(LifecycleRaceStage::ObserverGetResponse);",
    "let list = signed_list_object_versions(list_endpoint, bucket, key).await;",
    "record_lifecycle_race_stage(LifecycleRaceStage::ObserverListResponse);",
    "assert_eq!(",
    "record_lifecycle_race_stage(LifecycleRaceStage::ObserverListStatusOk);",
    "record_lifecycle_race_stage(LifecycleRaceStage::ObserverSuccessorVisible);",
    "return versions;"
)
$previousIndex = -1
foreach ($fragment in $observerOrder) {
    $nextIndex = $observerSource.IndexOf($fragment, $previousIndex + 1, [StringComparison]::Ordinal)
    Assert-True ($nextIndex -gt $previousIndex) "Observer stage order is incomplete: $fragment"
    $previousIndex = $nextIndex
}
foreach ($variant in @(
    "SuccessorRequestDispatched", "SuccessorRequestComplete", "ObserverLoopEntered",
    "ObserverGetResponse", "ObserverListResponse", "ObserverListStatusOk", "ObserverSuccessorVisible"
)) {
    $call = "record_lifecycle_race_stage(LifecycleRaceStage::$variant);"
    Assert-True (([regex]::Matches($RustLive, [regex]::Escape($call))).Count -eq 1) "Expected one fixed source emitter for $variant"
}
```

In `tests/client-smoke.Tests.ps1`, require both runner parsers to contain exactly `$expectedStageEnum`; accept every fixed stage in pure fixtures; reject unknown stages, payload suffixes, alternate test names, and dynamic values. Add this allowed interleaving fixture and require its last stage to be `observer-successor-visible` without interpreting response data:

```powershell
$interleavedStageLines = @(
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=race-started",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-loop-entered",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-request-dispatched",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-get-response",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-request-complete",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-list-response",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-list-status-ok",
    "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-successor-visible"
)
$interleavedReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @("running 1 test") + $interleavedStageLines + @(
        "test $raceTestName ... FAILED",
        "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
    )
    StdErr = @("fixture assertion")
}) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    $interleavedReceipt.ParserFailureCategory -ceq "none" -and
    $interleavedReceipt.LastStage -ceq "observer-successor-visible"
) "Exact lifecycle-race parser rejected a fixed safe concurrent interleaving"
```

Run the two source/static contracts before changing Rust or runner source. Each must exit `1` because the new fixed stages/allowlists are absent; any parse error or unrelated failure is not a valid RED.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$sourceStageRed = $LASTEXITCODE
if ($sourceStageRed -ne 1) { throw "Concurrent-stage source RED returned an unexpected exit code" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$parserStageRed = $LASTEXITCODE
if ($parserStageRed -ne 1) { throw "Concurrent-stage parser RED returned an unexpected exit code" }
```

- [ ] **Step 20: Add the minimal fixed no-data stage emitters and parser allowlists**

Modify only `tests/multi_gateway.rs` and `scripts/lifecycle-expiration-smoke.ps1`. Extend the test-only enum and its total fixed mapping exactly as follows; the emitter format remains unchanged and carries no status, body, endpoint, object key, version, bucket, error, or timing data:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleRaceStage {
    BucketCreated,
    VersioningEnabled,
    LifecycleConfigured,
    PredecessorCreated,
    RaceStarted,
    SuccessorRequestDispatched,
    SuccessorRequestComplete,
    ObserverLoopEntered,
    ObserverGetResponse,
    ObserverListResponse,
    ObserverListStatusOk,
    ObserverSuccessorVisible,
    SuccessorResponse,
    SuccessorObserved,
    LifecycleConfigDeleted,
    TerminalWaitEntered,
    TerminalStateEvaluation,
    SuccessorRead,
    VersionCleanup,
    BucketDelete,
}

impl LifecycleRaceStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BucketCreated => "bucket-created",
            Self::VersioningEnabled => "versioning-enabled",
            Self::LifecycleConfigured => "lifecycle-configured",
            Self::PredecessorCreated => "predecessor-created",
            Self::RaceStarted => "race-started",
            Self::SuccessorRequestDispatched => "successor-request-dispatched",
            Self::SuccessorRequestComplete => "successor-request-complete",
            Self::ObserverLoopEntered => "observer-loop-entered",
            Self::ObserverGetResponse => "observer-get-response",
            Self::ObserverListResponse => "observer-list-response",
            Self::ObserverListStatusOk => "observer-list-status-ok",
            Self::ObserverSuccessorVisible => "observer-successor-visible",
            Self::SuccessorResponse => "successor-response",
            Self::SuccessorObserved => "successor-observed",
            Self::LifecycleConfigDeleted => "lifecycle-config-deleted",
            Self::TerminalWaitEntered => "terminal-wait-entered",
            Self::TerminalStateEvaluation => "terminal-state-evaluation",
            Self::SuccessorRead => "successor-read",
            Self::VersionCleanup => "version-cleanup",
            Self::BucketDelete => "bucket-delete",
        }
    }
}
```

Instrument the existing observer helper exactly at response boundaries. `ObserverLoopEntered` is emitted once; the other observer stages may repeat once per bounded loop iteration. `ObserverSuccessorVisible` is emitted only after status/body equality succeeds and immediately before returning:

```rust
async fn wait_for_signed_successor(
    get_endpoint: &str,
    list_endpoint: &str,
    bucket: &str,
    key: &str,
    successor_body: &[u8],
) -> String {
    tokio::time::timeout(HTTP_TIMEOUT, async {
        record_lifecycle_race_stage(LifecycleRaceStage::ObserverLoopEntered);
        loop {
            let response = signed_get_object(get_endpoint, bucket, key).await;
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverGetResponse);
            let list = signed_list_object_versions(list_endpoint, bucket, key).await;
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverListResponse);
            assert_eq!(
                list.status().as_u16(),
                200,
                "signed ListObjectVersions status"
            );
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverListStatusOk);
            let versions = list
                .text()
                .await
                .expect("read signed ListObjectVersions XML");
            if response.status().as_u16() == 200
                && response
                    .bytes()
                    .await
                    .expect("read signed successor GET")
                    .as_ref()
                    == successor_body
            {
                record_lifecycle_race_stage(LifecycleRaceStage::ObserverSuccessorVisible);
                return versions;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("signed successor must become visible within 15 seconds")
}
```

Change only the publish future inside the named test. `SuccessorRequestComplete` is emitted immediately after the response future resolves, but the emitter contains no response status or other dynamic data. Preserve the observer future and the existing post-join `SuccessorResponse` stage:

```rust
let publish = async {
    publish_barrier.wait().await;
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestDispatched);
    let response = signed_put_object(&endpoint_b, &bucket_name, key, successor_body.clone()).await;
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestComplete);
    response
};
let observe = async {
    observe_barrier.wait().await;
    wait_for_signed_successor(
        &load_balancer,
        &endpoint_a,
        &bucket_name,
        key,
        &successor_body,
    )
    .await
};
let (successor, first_observed_versions) = tokio::join!(publish, observe);
record_lifecycle_race_stage(LifecycleRaceStage::SuccessorResponse);
```

In both `Get-MultiGatewayFailureReceipt` and `Get-LifecycleRaceExactReceipt`, replace only the old stage alternation with this exact fixed string and preserve all broad-prefix rejection, anchored full-line matching, count/name/category hardening, and last-match behavior:

```powershell
$stageEnum = "bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-request-dispatched|successor-request-complete|observer-loop-entered|observer-get-response|observer-list-response|observer-list-status-ok|observer-successor-visible|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete"
$stagePrefixMatches = [regex]::Matches($output, '(?m)^\[LIFECYCLE-RACE-STAGE\].*\r?$')
$stageMatches = [regex]::Matches(
    $output,
    "(?m)^\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=(?<stage>$stageEnum)\r?$"
)
$broadStageMatches = [regex]::Matches($output, '(?m)\[LIFECYCLE-RACE-STAGE\][^\r\n]*')
$strictStageMatches = [regex]::Matches(
    $output,
    "(?m)(?<!\S)\[LIFECYCLE-RACE-STAGE\] test=$raceTestName stage=(?<stage>$stageEnum)(?=[ \t]*$)"
)
```

`Get-MultiGatewayFailureReceipt` uses `$stagePrefixMatches`/`$stageMatches`; `Get-LifecycleRaceExactReceipt` uses `$broadStageMatches`/`$strictStageMatches`. Do not merge their different prefix/interleaving contracts, and keep each function's existing rejected-shape checks unchanged.

- [ ] **Step 21: Prove the expanded stage surface with Docker-free GREEN gates**

Run the two focused static contracts first, parse every PowerShell fence/runner, prove default no-run remains exact, compile the multi-gateway target, and require clean LSP diagnostics for `tests/multi_gateway.rs`. Then run the complete nonlive matrix once on the unchanged candidate. Do not start Docker or invoke any live runner switch in this step.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage source contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage parser contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Expanded concurrent-stage runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Expanded concurrent-stage no-run contract failed" }
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage formatting failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage library suite failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage serial integration failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage multi-gateway target did not compile" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage client static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Expanded concurrent-stage whitespace check failed" }
```

- [ ] **Step 22: Record consumed exact-test diagnostic #13 and keep the cause unresolved**

Attempt #13 used the expanded fixed-stage candidate and unchanged exact-test mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #13 execution receipt:** topology and metadata passed; exact command `cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1` ran; `CommandOutcome=allowed-exit-101`; `CountShape=available`; `running=1`, `passed=0`, `failed=1`; exact failed name `multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome`; first category `assertion`; `ParserFailureCategory=none`; and `LastStage=observer-get-response`. Final result was exactly `FAILED reason=exact-race-diagnostic-captured`.

**Observed #13 cleanup receipt:** cleanup diagnostics passed; project-ownership retry was `not-needed`; Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were exactly zero; and `cleanup-errors=0`.

Attempt #13 is consumed. Tag isolation remains in the test, but neither it nor `observer-get-response` establishes a cause. Preserve the test, product, worker, database, topology, waits, and cleanup behavior until attempt #14 returns a qualifying receipt.

- [ ] **Step 23: Write parser-only RED fixtures for five fixed seen-booleans and strict qualification**

Modify only `tests/client-smoke.Tests.ps1` first. Require `Get-LifecycleRaceExactReceipt` to expose exactly these five Boolean properties, derived only from `$strictStageMatches`: `SuccessorRequestDispatchedSeen`, `SuccessorRequestCompleteSeen`, `ObserverGetResponseSeen`, `ObserverListResponseSeen`, and `ObserverSuccessorVisibleSeen`. Require `Invoke-LifecycleRaceExactDiagnostic` to emit exactly these fixed keys with lowercase `true` or `false` values:

```powershell
$expectedSeenEvidence = @(
    "exact-race-successor-request-dispatched-seen=",
    "exact-race-successor-request-complete-seen=",
    "exact-race-observer-get-response-seen=",
    "exact-race-observer-list-response-seen=",
    "exact-race-observer-successor-visible-seen="
)
foreach ($fragment in $expectedSeenEvidence) {
    Test-MultiGatewayDiagnosticContract (
        ([regex]::Matches($exactDiagnosticSource, [regex]::Escape($fragment))).Count -eq 1
    ) "Exact lifecycle-race diagnostic must emit one fixed seen field: $fragment"
}
foreach ($lowercaseConversion in @(
    '$successorRequestDispatchedSeen = if ($receipt.SuccessorRequestDispatchedSeen) { "true" } else { "false" }',
    '$successorRequestCompleteSeen = if ($receipt.SuccessorRequestCompleteSeen) { "true" } else { "false" }',
    '$observerGetResponseSeen = if ($receipt.ObserverGetResponseSeen) { "true" } else { "false" }',
    '$observerListResponseSeen = if ($receipt.ObserverListResponseSeen) { "true" } else { "false" }',
    '$observerSuccessorVisibleSeen = if ($receipt.ObserverSuccessorVisibleSeen) { "true" } else { "false" }'
)) {
    Test-MultiGatewayDiagnosticContract (
        $exactDiagnosticSource.Contains($lowercaseConversion, [StringComparison]::Ordinal)
    ) "Exact lifecycle-race seen field is not a lowercase fixed Boolean"
}
foreach ($forbiddenSeenField in @(
    "exact-race-stage-trace=",
    "exact-race-stage-count=",
    "exact-race-raw="
)) {
    Test-MultiGatewayDiagnosticContract (
        -not $exactDiagnosticSource.Contains($forbiddenSeenField, [StringComparison]::Ordinal)
    ) "Exact lifecycle-race diagnostic emitted forbidden dynamic stage evidence: $forbiddenSeenField"
}
```

Add pure presence/absence fixtures using only existing strict allowlisted stage lines. The first case proves all false; the next five prove progressive presence. No fixture carries a dynamic trace, count, identity, endpoint, status, body, error, or timing value beyond the existing fixed Rust summary grammar:

```powershell
function New-ExactRaceBooleanFixture {
    param([Parameter(Mandatory)][string[]]$Stages)
    $stageLines = @($Stages | ForEach-Object {
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=$_"
    })
    return [pscustomobject]@{
        StdOut = @("running 1 test") + $stageLines + @(
            "test $raceTestName ... FAILED",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
        )
        StdErr = @("fixture assertion")
    }
}

$seenCases = @(
    [pscustomobject]@{ Stages = @("observer-loop-entered"); Dispatch = $false; Request = $false; Get = $false; List = $false; Visible = $false },
    [pscustomobject]@{ Stages = @("successor-request-dispatched"); Dispatch = $true; Request = $false; Get = $false; List = $false; Visible = $false },
    [pscustomobject]@{ Stages = @("successor-request-dispatched", "successor-request-complete"); Dispatch = $true; Request = $true; Get = $false; List = $false; Visible = $false },
    [pscustomobject]@{ Stages = @("successor-request-dispatched", "successor-request-complete", "observer-get-response"); Dispatch = $true; Request = $true; Get = $true; List = $false; Visible = $false },
    [pscustomobject]@{ Stages = @("successor-request-dispatched", "successor-request-complete", "observer-get-response", "observer-list-response"); Dispatch = $true; Request = $true; Get = $true; List = $true; Visible = $false },
    [pscustomobject]@{ Stages = @("successor-request-dispatched", "successor-request-complete", "observer-get-response", "observer-list-response", "observer-successor-visible"); Dispatch = $true; Request = $true; Get = $true; List = $true; Visible = $true }
)
foreach ($case in $seenCases) {
    $receipt = Get-LifecycleRaceExactReceipt -Result (New-ExactRaceBooleanFixture -Stages $case.Stages) -ExitCode 101
    Test-MultiGatewayDiagnosticContract (
        $receipt.ParserFailureCategory -ceq "none" -and
        $receipt.SuccessorRequestDispatchedSeen -eq $case.Dispatch -and
        $receipt.SuccessorRequestCompleteSeen -eq $case.Request -and
        $receipt.ObserverGetResponseSeen -eq $case.Get -and
        $receipt.ObserverListResponseSeen -eq $case.List -and
        $receipt.ObserverSuccessorVisibleSeen -eq $case.Visible
    ) "Exact lifecycle-race seen-booleans did not match strict stage presence"
}
```

Retain unknown/payload rejection and add an exact unknown-stage fixture. It must return `ParserFailureCategory=stage-rejected` and all five booleans false because no unknown line may enter `$strictStageMatches`:

```powershell
$unknownSeenReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @(
        "running 1 test",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-unknown",
        "test $raceTestName ... FAILED",
        "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
    )
    StdErr = @("fixture assertion")
}) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    $unknownSeenReceipt.ParserFailureCategory -ceq "stage-rejected" -and
    -not $unknownSeenReceipt.SuccessorRequestDispatchedSeen -and
    -not $unknownSeenReceipt.SuccessorRequestCompleteSeen -and
    -not $unknownSeenReceipt.ObserverGetResponseSeen -and
    -not $unknownSeenReceipt.ObserverListResponseSeen -and
    -not $unknownSeenReceipt.ObserverSuccessorVisibleSeen
) "Unknown lifecycle-race stages must not set seen-booleans"
```

Add a pure fail-closed qualification fixture. It may return true only for allowed exit `0` with exact `1/1/0` and success-name semantics, or allowed exit `101` with exact `1/0/1` and the exact failed name; both require parser `none`, count shape `available`, a fixed allowlisted `LastStage`, and exact cleanup. The parser changes in Step 24 make parser `none` on available counts contingent on exactly one matching `test <exact-name> ... ok|FAILED` line and zero opposite-status name lines.

```powershell
function Test-ExactRaceLocalizationQualified {
    param(
        [Parameter(Mandatory)][object]$Receipt,
        [Parameter(Mandatory)][bool]$CleanupExact
    )
    $fixedStages = @(
        "bucket-created", "versioning-enabled", "lifecycle-configured", "predecessor-created",
        "race-started", "successor-request-dispatched", "successor-request-complete",
        "observer-loop-entered", "observer-get-response", "observer-list-response",
        "observer-list-status-ok", "observer-successor-visible", "successor-response",
        "successor-observed", "lifecycle-config-deleted", "terminal-wait-entered",
        "terminal-state-evaluation", "successor-read", "version-cleanup", "bucket-delete"
    )
    $success =
        $Receipt.CommandOutcome -ceq "allowed-exit-0" -and
        $Receipt.Running -eq 1 -and $Receipt.Passed -eq 1 -and $Receipt.Failed -eq 0 -and
        $Receipt.FailedName -ceq "none"
    $failure =
        $Receipt.CommandOutcome -ceq "allowed-exit-101" -and
        $Receipt.Running -eq 1 -and $Receipt.Passed -eq 0 -and $Receipt.Failed -eq 1 -and
        $Receipt.FailedName -ceq $raceTestName
    return (
        $CleanupExact -and
        $Receipt.ParserFailureCategory -ceq "none" -and
        $Receipt.CountShape -ceq "available" -and
        $fixedStages -ccontains $Receipt.LastStage -and
        ($success -or $failure)
    )
}

$qualifiedFailure = Get-LifecycleRaceExactReceipt -Result (
    New-ExactRaceBooleanFixture -Stages @("observer-get-response")
) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    Test-ExactRaceLocalizationQualified -Receipt $qualifiedFailure -CleanupExact $true
) "Exact failed receipt did not qualify"
Test-MultiGatewayDiagnosticContract (
    -not (Test-ExactRaceLocalizationQualified -Receipt $qualifiedFailure -CleanupExact $false)
) "Receipt qualified without exact cleanup"

$qualifiedSuccess = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @(
        "running 1 test",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
        "test $raceTestName ... ok",
        "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out; finished in 0.01s"
    )
    StdErr = @()
}) -ExitCode 0
Test-MultiGatewayDiagnosticContract (
    Test-ExactRaceLocalizationQualified -Receipt $qualifiedSuccess -CleanupExact $true
) "Exact successful receipt did not qualify"

$unavailableReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @("[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-get-response")
    StdErr = @()
}) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    -not (Test-ExactRaceLocalizationQualified -Receipt $unavailableReceipt -CleanupExact $true)
) "Unavailable counts qualified for localization"

$wrongNameReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @(
        "running 1 test",
        "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-get-response",
        "test other_exact_test ... FAILED",
        "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
    )
    StdErr = @("fixture assertion")
}) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    $wrongNameReceipt.ParserFailureCategory -ceq "output-shape-rejected" -and
    -not (Test-ExactRaceLocalizationQualified -Receipt $wrongNameReceipt -CleanupExact $true)
) "Wrong exact-test name qualified for localization"

$missingStageReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
    StdOut = @(
        "running 1 test",
        "test $raceTestName ... FAILED",
        "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
    )
    StdErr = @("fixture assertion")
}) -ExitCode 101
Test-MultiGatewayDiagnosticContract (
    $missingStageReceipt.ParserFailureCategory -ceq "stage-missing" -and
    -not (Test-ExactRaceLocalizationQualified -Receipt $missingStageReceipt -CleanupExact $true)
) "Missing valid stage qualified for localization"
```

Run the static contract before changing the runner and require exit `1` only because the five properties/emissions and available-count exact-name checks are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$seenBooleanRed = $LASTEXITCODE
if ($seenBooleanRed -ne 1) { throw "Lifecycle seen-boolean parser RED returned an unexpected exit code" }
```

- [ ] **Step 24: Add five strict-match booleans, exact-name validation, and fixed evidence fields**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. In `Get-LifecycleRaceExactReceipt`, derive the booleans from the already anchored, allowlisted `$strictStageMatches`; do not inspect `$broadStageMatches`, raw output, line counts, order, payloads, or dynamic values:

```powershell
$strictStages = @($strictStageMatches | ForEach-Object { $_.Groups['stage'].Value })
$successorRequestDispatchedSeen = $strictStages -ccontains "successor-request-dispatched"
$successorRequestCompleteSeen = $strictStages -ccontains "successor-request-complete"
$observerGetResponseSeen = $strictStages -ccontains "observer-get-response"
$observerListResponseSeen = $strictStages -ccontains "observer-list-response"
$observerSuccessorVisibleSeen = $strictStages -ccontains "observer-successor-visible"
```

For available counts only, make parser `none` contingent on exact test-name/status semantics. Unavailable counts remain parseable for historical safe receipts but can never pass Step 27's localization gate:

```powershell
$successNameMatches = [regex]::Matches($output, '(?m)^test (?<name>[A-Za-z0-9_:]+) \.\.\. ok$')
$failureNameMatches = [regex]::Matches($output, '(?m)^test (?<name>[A-Za-z0-9_:]+) \.\.\. FAILED$')
$nameSemanticsValid = if ($countShape -cne "available") {
    $true
} elseif ($ExitCode -eq 0) {
    $successNameMatches.Count -eq 1 -and
        $successNameMatches[0].Groups['name'].Value -ceq $raceTestName -and
        $failureNameMatches.Count -eq 0
} else {
    $failureNameMatches.Count -eq 1 -and
        $failureNameMatches[0].Groups['name'].Value -ceq $raceTestName -and
        $successNameMatches.Count -eq 0
}

$parserFailureCategory = if ($stageRejected) {
    "stage-rejected"
} elseif ($lastStage -ceq "not-reached") {
    "stage-missing"
} elseif ($countShape -ceq "rejected" -or -not $nameSemanticsValid) {
    "output-shape-rejected"
} else {
    "none"
}
```

Add the exact properties to both the normal parser return and the command-error fallback receipt so every safe path has a total fixed schema:

```powershell
SuccessorRequestDispatchedSeen = $successorRequestDispatchedSeen
SuccessorRequestCompleteSeen = $successorRequestCompleteSeen
ObserverGetResponseSeen = $observerGetResponseSeen
ObserverListResponseSeen = $observerListResponseSeen
ObserverSuccessorVisibleSeen = $observerSuccessorVisibleSeen
```

```powershell
SuccessorRequestDispatchedSeen = $false
SuccessorRequestCompleteSeen = $false
ObserverGetResponseSeen = $false
ObserverListResponseSeen = $false
ObserverSuccessorVisibleSeen = $false
```

In `Invoke-LifecycleRaceExactDiagnostic`, convert each Boolean to a lowercase fixed token and emit exactly one line per key. Existing safe command/count/name/category/parser/last-stage fields remain unchanged; add no trace, stage count, raw line, sequence, status, body, identity, endpoint, error, or timing output:

```powershell
$successorRequestDispatchedSeen = if ($receipt.SuccessorRequestDispatchedSeen) { "true" } else { "false" }
$successorRequestCompleteSeen = if ($receipt.SuccessorRequestCompleteSeen) { "true" } else { "false" }
$observerGetResponseSeen = if ($receipt.ObserverGetResponseSeen) { "true" } else { "false" }
$observerListResponseSeen = if ($receipt.ObserverListResponseSeen) { "true" } else { "false" }
$observerSuccessorVisibleSeen = if ($receipt.ObserverSuccessorVisibleSeen) { "true" } else { "false" }
Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-dispatched-seen=$successorRequestDispatchedSeen"
Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-successor-request-complete-seen=$successorRequestCompleteSeen"
Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-get-response-seen=$observerGetResponseSeen"
Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-list-response-seen=$observerListResponseSeen"
Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-observer-successor-visible-seen=$observerSuccessorVisibleSeen"
```

- [ ] **Step 25: Prove the parser-only addition with Docker-free GREEN gates**

Run the client/static contract, parse the runner, preserve exact default no-run behavior, run all five Docker-free static contracts, and check whitespace. Do not modify or compile Rust, start Docker, invoke a live switch, or change evidence/docs in this step.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean parser/static contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle seen-boolean runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle seen-boolean no-run contract failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean client static rerun failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Lifecycle seen-boolean whitespace check failed" }
```

- [ ] **Step 26: Record consumed exact diagnostic #14 as one insufficient PASS sample**

Attempt #14 used the parser-boolean exact mode on a fresh owned no-pull topology. The following command is retained only as its historical invocation and must not be rerun:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

**Observed #14 execution receipt:** topology and metadata passed; `CommandOutcome=allowed-exit-0`; `ParserFailureCategory=none`; `CountShape=available`; `running=1`, `passed=1`, `failed=0`; `FailedName=none`; first category `none`; `LastStage=bucket-delete`; and all five fixed fields were true: `SuccessorRequestDispatchedSeen`, `SuccessorRequestCompleteSeen`, `ObserverGetResponseSeen`, `ObserverListResponseSeen`, and `ObserverSuccessorVisibleSeen`. Final result was exactly `[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-passed`.

**Observed #14 cleanup receipt:** diagnostics were `not-required`; Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were exactly zero; `cleanup-errors=0`; and cleanup was complete.

Attempt #14 is consumed. Preserve that #12 and #13 both failed after the predecessor-only Tag-filter correction, while #14 passed once after parser-only receipt hardening. That single pass is not causal closure, stability proof, or a product-change claim. No prior diagnostic may run again.

- [ ] **Step 27: Write causal RED contracts for a dedicated race timeout and bounded stability mode**

Observed timing basis: after Tag isolation, #13 emitted `observer-get-response` but no `observer-list-response`; both race convergence helpers still shared the global 15-second outer `HTTP_TIMEOUT`. Under Docker/PostgreSQL load this is consistent with outer convergence deadline cancellation while a list request is pending, but it is not a product root-cause claim. The bounded test-only stabilization below tests that timing hypothesis without changing per-request or production behavior.

Modify only `tests/multi-gateway.Tests.ps1` and `tests/client-smoke.Tests.ps1` first. The Rust source contract must fail because no dedicated timeout exists. Require exactly one declaration `const LIFECYCLE_RACE_TIMEOUT: Duration = Duration::from_secs(60);`, exactly two call-site uses `tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {`, one inside `wait_for_signed_successor` and one inside `wait_for_lifecycle_race_terminal`, and exactly three total identifier occurrences including the declaration. Require both outer expectation messages to say `within 60 seconds`.

Lock the unchanged per-request/global behavior separately: `const HTTP_TIMEOUT: Duration = Duration::from_secs(15);` remains exactly once; `http_call` still uses `tokio::time::timeout(HTTP_TIMEOUT, future)`; the Kubo client still uses `.timeout(HTTP_TIMEOUT)`; neither race convergence helper contains `timeout(HTTP_TIMEOUT`; no production source or other timeout constant changes.

```powershell
$raceTimeoutDeclaration = 'const LIFECYCLE_RACE_TIMEOUT: Duration = Duration::from_secs(60);'
Assert-True (([regex]::Matches($RustLive, [regex]::Escape($raceTimeoutDeclaration))).Count -eq 1) "Lifecycle race timeout declaration must be exact"
Assert-True (([regex]::Matches($RustLive, [regex]::Escape('tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {'))).Count -eq 2) "Lifecycle race timeout must have exactly two outer uses"
Assert-True (([regex]::Matches($RustLive, '\bLIFECYCLE_RACE_TIMEOUT\b')).Count -eq 3) "Lifecycle race timeout leaked beyond declaration plus two uses"
Assert-True (([regex]::Matches($RustLive, [regex]::Escape('const HTTP_TIMEOUT: Duration = Duration::from_secs(15);'))).Count -eq 1) "Per-request HTTP timeout changed"

$successorWaitStart = $RustLive.IndexOf('async fn wait_for_signed_successor(', [StringComparison]::Ordinal)
$terminalWaitStart = $RustLive.IndexOf('async fn wait_for_lifecycle_race_terminal(', $successorWaitStart, [StringComparison]::Ordinal)
$kuboCatStart = $RustLive.IndexOf('async fn kubo_cat(', $terminalWaitStart, [StringComparison]::Ordinal)
$successorWaitSource = $RustLive.Substring($successorWaitStart, $terminalWaitStart - $successorWaitStart)
$terminalWaitSource = $RustLive.Substring($terminalWaitStart, $kuboCatStart - $terminalWaitStart)
foreach ($waitSource in @($successorWaitSource, $terminalWaitSource)) {
    Assert-True (([regex]::Matches($waitSource, [regex]::Escape('tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {'))).Count -eq 1) "Each lifecycle race wait must use the dedicated bound once"
    Assert-True (-not $waitSource.Contains('timeout(HTTP_TIMEOUT', [StringComparison]::Ordinal)) "Lifecycle race outer wait still uses per-request HTTP timeout"
}
Assert-Contains $successorWaitSource 'signed successor must become visible within 60 seconds' "Successor convergence message did not adopt the dedicated bound"
Assert-Contains $terminalWaitSource 'lifecycle race must reach a stable S3-visible state within 60 seconds' "Terminal convergence message did not adopt the dedicated bound"
Assert-Contains $RustLive 'tokio::time::timeout(HTTP_TIMEOUT, future)' "Per-request HTTP timeout wrapper changed"
Assert-Contains $RustLive '.timeout(HTTP_TIMEOUT)' "Kubo per-request timeout changed"
```

In `tests/client-smoke.Tests.ps1`, require a fourth mutually exclusive switch `DiagnoseLifecycleRaceStability`, one branch after metadata that calls only `Invoke-LifecycleRaceStabilityDiagnostic`, a literal sequential `foreach ($iteration in 1..5)`, five bounded invocations of the existing exact cargo argument list at runtime, and one fixed success receipt per iteration. Reject any call to `Invoke-LifecycleRustSuites`, `Invoke-LifecycleAwsEvidence`, raw stdout/stderr emission, AWS command, database query, dynamic bucket/version/body output, parallel dispatch, or a loop bound other than `1..5`. Require final fixed outcome `exact-race-stability-passed` and no change to normal/no-run/cleanup behavior.

Run both contracts before source/runner changes. Each must exit `1`; the multi-gateway RED is valid only for the absent dedicated timeout, and the client RED only for the absent stability mode.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$raceTimeoutRed = $LASTEXITCODE
if ($raceTimeoutRed -ne 1) { throw "Dedicated lifecycle race timeout RED returned an unexpected exit code" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$stabilityModeRed = $LASTEXITCODE
if ($stabilityModeRed -ne 1) { throw "Lifecycle race stability-mode RED returned an unexpected exit code" }
```

- [ ] **Step 28: Implement only the dedicated test timeout and sequential stability mode**

Modify only test/runner/static paths: `tests/multi_gateway.rs`, `scripts/lifecycle-expiration-smoke.ps1`, `tests/multi-gateway.Tests.ps1`, and `tests/client-smoke.Tests.ps1`. In Rust, add the dedicated constant beside the unchanged timeout constants, replace only the two outer convergence bounds, and update only their fixed expectation messages:

```rust
const S3_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const LIFECYCLE_RACE_TIMEOUT: Duration = Duration::from_secs(60);
const IMPORT_TIMEOUT: Duration = Duration::from_secs(30);
```

```diff
-    tokio::time::timeout(HTTP_TIMEOUT, async {
+    tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {
-    .expect("signed successor must become visible within 15 seconds")
+    .expect("signed successor must become visible within 60 seconds")

-    tokio::time::timeout(HTTP_TIMEOUT, async {
+    tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {
-    .expect("lifecycle race must reach a stable S3-visible state within 15 seconds")
+    .expect("lifecycle race must reach a stable S3-visible state within 60 seconds")
```

The diff changes exactly four lines in the two existing functions; function signatures, loop bodies, poll intervals, assertions, and return behavior remain byte-for-byte unchanged. No production timeout, request timeout, lifecycle behavior, worker, database, or topology changes.

Add the fourth runner switch and include it in the existing exact mutual-exclusion count:

```powershell
[CmdletBinding()]
param(
    [switch]$Run,
    [switch]$DiagnoseMultiGateway,
    [switch]$DiagnoseLifecycleRaceExact,
    [switch]$DiagnoseLifecycleRaceStability
)

$selectedModeCount = @(
    $Run.IsPresent,
    $DiagnoseMultiGateway.IsPresent,
    $DiagnoseLifecycleRaceExact.IsPresent,
    $DiagnoseLifecycleRaceStability.IsPresent
).Where({ $_ }).Count
```

Add one sequential function. Each cargo process runs the existing exact named test, whose `unique_bucket("lifecycle-race")` incorporates process ID, nanoseconds, and process-local counter; its successful final `BucketDelete` stage proves per-iteration public-S3 cleanup. Capture output only in memory through the existing strict parser and emit no raw line:

```powershell
function Invoke-LifecycleRaceStabilityDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)

    Set-LifecycleEndpointEnvironment
    Set-LifecycleStage -State $State -Stage "multi-gateway"
    foreach ($iteration in 1..5) {
        $receipt = $null
        try {
            $result = Invoke-NativeCommand `
                -FilePath "cargo" `
                -ArgumentList @(
                    "test", "--test", "multi_gateway",
                    "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
                    "--", "--exact", "--nocapture", "--test-threads=1"
                ) `
                -Label "Owned lifecycle-race stability process" `
                -Timeout $RustTestTimeout `
                -AllowedExitCodes @(0, 101) `
                -WorkingDirectory $RepoRoot
            $receipt = Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode
        } catch {
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-stability-iteration=$iteration result=failed"
            $State.DiagnosticOutcome = "exact-race-stability-failed"
            throw "Owned lifecycle-race stability process failed safely"
        }

        $passed =
            $receipt.Outcome -ceq "passed" -and
            $receipt.CommandOutcome -ceq "allowed-exit-0" -and
            $receipt.ParserFailureCategory -ceq "none" -and
            $receipt.CountShape -ceq "available" -and
            $receipt.Running -eq 1 -and
            $receipt.Passed -eq 1 -and
            $receipt.Failed -eq 0 -and
            $receipt.FailedName -ceq "none" -and
            $receipt.FirstErrorCategory -ceq "none" -and
            $receipt.LastStage -ceq "bucket-delete" -and
            $receipt.SuccessorRequestDispatchedSeen -and
            $receipt.SuccessorRequestCompleteSeen -and
            $receipt.ObserverGetResponseSeen -and
            $receipt.ObserverListResponseSeen -and
            $receipt.ObserverSuccessorVisibleSeen
        if (-not $passed) {
            Write-LifecycleEvidence -Category "diagnostic" -Value "exact-race-stability-iteration=$iteration result=failed"
            $State.DiagnosticOutcome = "exact-race-stability-failed"
            throw "Owned lifecycle-race stability receipt was not an exact pass"
        }

        Write-LifecycleEvidence -Category "diagnostic" -Value (
            "exact-race-stability-iteration=$iteration command-outcome=allowed-exit-0 count-shape=available running=1 passed=1 failed=0 failed-name=none error-category=none parser=none last-stage=bucket-delete successor-request-dispatched-seen=true successor-request-complete-seen=true observer-get-response-seen=true observer-list-response-seen=true observer-successor-visible-seen=true result=passed"
        )
    }
    $State.DiagnosticOutcome = "exact-race-stability-passed"
}
```

Branch after metadata and before every existing mode. Stability invokes only that function:

```powershell
if ($DiagnoseLifecycleRaceStability) {
    Invoke-LifecycleRaceStabilityDiagnostic -State $State
} elseif ($DiagnoseLifecycleRaceExact) {
    Invoke-LifecycleRaceExactDiagnostic -State $State
} elseif ($DiagnoseMultiGateway) {
    Invoke-LifecycleMultiGatewayDiagnostic -State $State
} else {
    Invoke-LifecycleRustSuites -State $State
    Set-LifecycleStage -State $State -Stage "aws"
    Invoke-LifecycleAwsEvidence -State $State -Network $network
}
```

Add fixed terminal handling without changing cleanup:

```powershell
if ($state.DiagnosticOutcome -eq "exact-race-stability-failed") {
    Write-Host "[RESULT] lifecycle-expiration=FAILED reason=exact-race-stability-captured"
    exit 1
}
if ($workSucceeded -and $state.DiagnosticOutcome -eq "exact-race-stability-passed") {
    Write-LifecycleEvidence -Category "cleanup" -Value "cleanup=complete"
    Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-stability-passed"
    exit 0
}
```

- [ ] **Step 29: Prove the stabilization change Docker-free before live execution**

Run both corrected static contracts, format/compile the multi-gateway target, parse the runner, preserve exact default no-run behavior, and require clean LSP diagnostics for `tests/multi_gateway.rs`. Do not run Docker, the exact test, normal suites, AWS, evidence, or documentation changes.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Dedicated lifecycle race timeout source contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle race stability-mode static contract failed" }
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Lifecycle race stabilization formatting failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Lifecycle race stabilization target did not compile" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle race stability runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle race stability no-run contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Lifecycle race stabilization whitespace check failed" }
```

LSP must report zero errors/warnings for `tests/multi_gateway.rs`. The final source receipt is: `HTTP_TIMEOUT=15s` unchanged; `LIFECYCLE_RACE_TIMEOUT=60s`; exactly two dedicated outer uses; no production/global timeout edit; stability runner sequential `1..5`; normal/AWS branches unchanged.

- [ ] **Step 30: Record consumed stability #15 iteration-1 failure and exact cleanup**

Attempt #15 used one fresh owned topology and `-DiagnoseLifecycleRaceStability`. Topology and metadata passed. The first exact cargo process started and failed; the runner emitted only `exact-race-stability-iteration=1 result=failed`. It emitted no command-outcome, parser category, count shape/counts, failed name/category, last stage, or five-boolean fields, so the failure is not source-localizable. Iterations 2-5 did not run.

Diagnostics were attempted. Compose down, exact image removal, temporary-root removal, and environment restoration passed; independent residual container/network/volume/image counts were zero; `cleanup-errors=0`; and the final result was exactly `[RESULT] lifecycle-expiration=FAILED reason=exact-race-stability-captured`. Attempt #15 is consumed. The dedicated 60-second bound did not prove stability, and this receipt establishes no cause. Evidence/README/ROADMAP remained unchanged and Task 14 stayed closed.

- [ ] **Step 31: Write Docker-free RED contracts for complete failed-iteration receipts**

Modify only `tests/client-smoke.Tests.ps1` first. Require one helper `Write-LifecycleRaceStabilityReceipt` that emits, for iteration `1..5`, the exact bounded fields already used by the exact parser: command outcome; parser failure category; count shape; running/passed/failed; failed name; first error category; last stage; and all five seen-booleans. Require the helper call after either parsed or fallback receipt creation and before `result=failed` or `result=passed`. Reject direct `StdOut`/`StdErr`, raw line arrays, exception text, IDs, endpoints, body/status, stage traces/counts, or any field outside the fixed parser schema.

Add fixed source fixtures for both failure paths: a parsed `allowed-exit-101` receipt and a catch fallback `command-error/output-shape-rejected/unavailable/-1/-1/-1/none/process-exit/not-reached/five-false` receipt. Both must emit the complete fixed field line before `exact-race-stability-iteration=$iteration result=failed`; neither may emit raw output. The existing exact diagnostic output remains unchanged. Stability success keeps the same fixed field values, emitted by the shared writer before a separate fixed `result=passed` marker.

```powershell
$requiredFailureFields = @(
    "command-outcome=", "parser=", "count-shape=", "running=", "passed=", "failed=",
    "failed-name=", "error-category=", "last-stage=",
    "successor-request-dispatched-seen=", "successor-request-complete-seen=",
    "observer-get-response-seen=", "observer-list-response-seen=",
    "observer-successor-visible-seen="
)
foreach ($field in $requiredFailureFields) {
    Test-MultiGatewayDiagnosticContract $stabilitySource.Contains($field, [StringComparison]::Ordinal) "Stability failure receipt omitted fixed field: $field"
}
$receiptCallIndex = $stabilitySource.IndexOf('Write-LifecycleRaceStabilityReceipt -Iteration $iteration -Receipt $receipt', [StringComparison]::Ordinal)
$failureIndex = $stabilitySource.IndexOf('exact-race-stability-iteration=$iteration result=failed', [StringComparison]::Ordinal)
Test-MultiGatewayDiagnosticContract ($receiptCallIndex -ge 0 -and $failureIndex -gt $receiptCallIndex) "Stability failure marker preceded its safe receipt"
foreach ($forbidden in @('StdOut', 'StdErr', '$_.Exception.ToString()', 'Write-Host $_', 'stage-trace=', 'stage-count=', 'raw=')) {
    Test-MultiGatewayDiagnosticContract (-not $stabilitySource.Contains($forbidden, [StringComparison]::Ordinal)) "Stability mode exposed forbidden raw failure data: $forbidden"
}
```

Run the static contract before changing the runner. It must exit `1` only because failed iterations currently emit the fixed failure marker without the complete safe receipt:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$stabilityReceiptRed = $LASTEXITCODE
if ($stabilityReceiptRed -ne 1) { throw "Stability failed-receipt RED returned an unexpected exit code" }
```

- [ ] **Step 32: Emit the same fixed receipt schema before every stability result**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Add a fixed-schema writer; it emits one sanitized line and no raw process data:

```powershell
function Write-LifecycleRaceStabilityReceipt {
    param(
        [Parameter(Mandatory)][ValidateRange(1, 5)][int]$Iteration,
        [Parameter(Mandatory)][object]$Receipt
    )
    $dispatchSeen = $Receipt.SuccessorRequestDispatchedSeen.ToString().ToLowerInvariant()
    $requestCompleteSeen = $Receipt.SuccessorRequestCompleteSeen.ToString().ToLowerInvariant()
    $observerGetSeen = $Receipt.ObserverGetResponseSeen.ToString().ToLowerInvariant()
    $observerListSeen = $Receipt.ObserverListResponseSeen.ToString().ToLowerInvariant()
    $observerVisibleSeen = $Receipt.ObserverSuccessorVisibleSeen.ToString().ToLowerInvariant()
    Write-LifecycleEvidence -Category "diagnostic" -Value (
        "exact-race-stability-iteration=$Iteration command-outcome=$($Receipt.CommandOutcome) parser=$($Receipt.ParserFailureCategory) count-shape=$($Receipt.CountShape) running=$($Receipt.Running) passed=$($Receipt.Passed) failed=$($Receipt.Failed) failed-name=$($Receipt.FailedName) error-category=$($Receipt.FirstErrorCategory) last-stage=$($Receipt.LastStage) successor-request-dispatched-seen=$dispatchSeen successor-request-complete-seen=$requestCompleteSeen observer-get-response-seen=$observerGetSeen observer-list-response-seen=$observerListSeen observer-successor-visible-seen=$observerVisibleSeen"
    )
}
```

Refactor the loop so catch creates the same fixed fallback schema, then every path calls the writer exactly once before pass/fail selection:

```powershell
try {
    $result = Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @(
            "test", "--test", "multi_gateway",
            "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome",
            "--", "--exact", "--nocapture", "--test-threads=1"
        ) `
        -Label "Owned lifecycle-race stability process" `
        -Timeout $RustTestTimeout `
        -AllowedExitCodes @(0, 101) `
        -WorkingDirectory $RepoRoot
    $receipt = Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode
} catch {
    $fixedCommandOutcome = if ($_.Exception.Message -ceq "Owned lifecycle-race stability process timed out") {
        "timeout"
    } elseif ($_.Exception.Message -ceq "Owned lifecycle-race stability process failed") {
        "nonallowed-exit"
    } else {
        "command-error"
    }
    $receipt = [pscustomobject]@{
        Outcome = "unqualified"
        CommandOutcome = $fixedCommandOutcome
        ParserFailureCategory = "output-shape-rejected"
        CountShape = "unavailable"
        Running = -1
        Passed = -1
        Failed = -1
        FailedName = "none"
        FirstErrorCategory = "process-exit"
        LastStage = "not-reached"
        SuccessorRequestDispatchedSeen = $false
        SuccessorRequestCompleteSeen = $false
        ObserverGetResponseSeen = $false
        ObserverListResponseSeen = $false
        ObserverSuccessorVisibleSeen = $false
    }
}
Write-LifecycleRaceStabilityReceipt -Iteration $iteration -Receipt $receipt
```

Keep the existing exact-pass predicate. On failure, emit only the already-fixed `exact-race-stability-iteration=$iteration result=failed`, set `exact-race-stability-failed`, and throw the fixed message; on success emit only `result=passed`. Never interpolate exceptions or process output.

- [ ] **Step 33: Prove failed-receipt hardening Docker-free**

Run the corrected static contract, parse/no-run the runner, preserve all existing source contracts, and check whitespace. Do not run Rust, Docker, stability, exact, normal, AWS, evidence, or docs.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Stability failed-receipt static contract failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle race timeout/source contract regressed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Stability failed-receipt runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Stability failed-receipt no-run contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Stability failed-receipt whitespace check failed" }
```

- [ ] **Step 34: After current plan-critic and Docker-free GREEN, run exactly one exact #16**

This is the sole live round authorized by this revision. Start one fresh owned topology under existing no-pull/offline-build/no-install, ownership, loopback, logs-first, cleanup, restoration, and residual-zero guards. Use the current test-only 60-second convergence bound and the existing one-process exact mode, not the stability loop:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceExact
```

Attempt #16 is consumed on setup. Require the complete fixed exact receipt: command outcome; parser category; count shape/counts; failed name/category; last stage; all five booleans; topology/metadata; diagnostics disposition; down/image/temp/environment; residual container/network/volume/image/root zero; `cleanup-errors=0`; cleanup complete when applicable; and one exact final result. Do not run stability, normal Rust suites, or AWS.

- [ ] **Step 35: Record exact #16's full PASS without claiming closure**

Attempt #16 used one fresh owned topology and existing `-DiagnoseLifecycleRaceExact` under the test-only 60-second bound. Its complete fixed receipt was: `command-outcome=allowed-exit-0`; parser failure category `none`; count shape `available`; `running=1`, `passed=1`, `failed=0`; failed name `none`; first error category `none`; last stage `bucket-delete`; and all five booleans true for successor request dispatched, successor request complete, observer GET response, observer LIST response, and observer successor visible. Owned cleanup completed with exact zero residuals and `cleanup-errors=0`; the diagnostic outcome was `exact-race-passed`.

Attempt #16 is consumed and may not be rerun. Preserve #12/#13 as failures and #15 as an iteration-1 stability failure that exposed no command/parser/count/name/category/stage/boolean detail. One exact PASS after those failures is not causal or stability closure. Evidence/README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, Task 14 remains closed, and no Git write, push, or tag is authorized.

- [ ] **Step 36: After current plan-critic, run exactly one five-process stability #17**

This is the sole live round authorized by this revision. Start one fresh owned five-service topology under all existing no-pull/offline-build/no-install, ownership, loopback, logs-first, cleanup, restoration, and residual-zero guards. Use the current test-only 60-second convergence bound and invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleRaceStability
```

Attempt #17 is consumed on setup. The topology must run exactly five sequential exact cargo processes, each with its own unique bucket and public-S3 cleanup. Before each fixed `result=passed` or `result=failed` marker, the hardened runner must emit the complete fixed receipt fields: command outcome; parser failure category; count shape and running/passed/failed counts; failed name; first error category; last stage; and all five booleans. It must never emit raw process output, IDs, endpoints, bodies/statuses, stage traces/counts, timing, or exception text.

Each PASS must be exactly `allowed-exit-0`, parser `none`, count shape `available`, `1/1/0`, failed name/category `none`, last stage `bucket-delete`, all five booleans true, and `result=passed`. Stability PASS requires five such receipts for iterations 1-5, final diagnostic outcome `exact-race-stability-passed`, exact cleanup, residual container/network/volume/image/root zero, `cleanup-errors=0`, and cleanup complete. No normal Rust suite or AWS command may run.

At the first stability-process timeout, nonallowed exit, parsed `101`, rejected/unavailable or non-`1/1/0` count, failed name/category, incomplete stage/boolean set, or cargo error, emit the complete fixed failure receipt before the fixed failure marker, stop immediately, and do not run later iterations. Branch only from that fixed iteration receipt in a new reviewed plan; do not use or request raw output, infer a cause from missing fields, or inherit a rerun. A preflight/topology or cleanup failure also stops with the runner's existing fixed stage/cleanup receipt and must not fabricate process fields for an iteration that did not run.

- [ ] **Step 37: Only after #17 reaches exact 5/5, run the complete nonlive matrix**

Do not run this step after any #17 iteration or cleanup failure. On the unchanged 5/5 candidate, require formatting, locked build, library `875/875`, serial integration `143/143`, multi-gateway compile, runner AST/no-run, all five static contracts, and whitespace GREEN:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Post-stability formatting failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Post-stability locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Post-stability library suite failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Post-stability serial integration failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Post-stability multi-gateway target did not compile" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Post-stability runner has PowerShell parse errors" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Post-stability no-run contract failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-stability release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-stability PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-stability multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-stability Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-stability client static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Post-stability whitespace check failed" }
```

- [ ] **Step 38: Record #17 exact 5/5 and post-stability nonlive PASS**

Attempt #17 used one fresh owned five-service topology. Each of iterations 1, 2, 3, 4, and 5 emitted the complete exact PASS receipt: command outcome `allowed-exit-0`; parser failure category `none`; count shape `available`; `running=1`, `passed=1`, `failed=0`; failed name `none`; first error category `none`; last stage `bucket-delete`; successor request dispatched, successor request complete, observer GET response, observer LIST response, and observer successor visible all `true`; fixed result `passed`. The final result was `[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-stability-passed`.

Diagnostics were `not-required`. Compose down, exact image removal, temporary-root removal, and environment restoration passed. Independent residual container/network/volume/image counts were zero, `cleanup-errors=0`, and cleanup was complete. Attempt #17 is consumed and may not be rerun.

On the unchanged #17 candidate, the complete post-stability nonlive matrix passed: `cargo fmt --check`; `cargo build --locked`; library `875/875`; serial integration `143/143`; multi-gateway target compile; release, PostgreSQL, multi-gateway, Cluster, and client/evidence static contracts; and `git diff --check`. No normal runner or AWS evidence ran in #17 or its post-stability matrix. The prior #12/#13 and #15 failures remain preserved rather than relabelled.

- [ ] **Step 39: After current plan-critic, run exactly one fresh owned final normal #18**

This is the sole live round authorized by this revision. Use one fresh owned five-service topology and all existing no-pull/offline-build/no-install, local-image inspection, unique project/image/root/bucket/ports, loopback-only, logs-first, exact environment restoration, and residual-zero guards. Invoke only normal mode, with no `-DiagnoseMultiGateway`, `-DiagnoseLifecycleRaceExact`, or `-DiagnoseLifecycleRaceStability` switch:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -Run
```

Attempt #18 is consumed on setup. It must run the five bounded Rust commands exactly once and in this order: full `postgres_versioning`; full `postgres_lifecycle`; full `e2e`; full `multi_gateway`; focused signed `integration lifecycle_expiration_invariants`. Then it must run the complete path-style SigV4 AWS lifecycle evidence: Put/Get/replace/Delete configuration, absent configuration, unsupported transition rejection without revision change, current expiration in unversioned/Enabled/Suspended buckets, noncurrent content and marker expiration, timed sole-marker cleanup, and EODM cleanup. The signed integration group remains the only valid Kubo `/api/v0/pin/rm` zero-request proof.

Accept #18 only when the sanitized receipt includes `postgres-versioning=passed`, `postgres-lifecycle=A-abort-B-reclaim-stale-CAS`, `e2e=passed`, `cross-replica=one-terminal-successor-retained`, `pin-rm-request=zero-signed-integration`, `lifecycle-put-get-replace-delete=passed`, `unsupported-revision-unchanged=passed`, `current-days-unversioned=passed`, `current-days-enabled=passed`, `current-days-suspended=passed`, `nve-content=passed`, `nve-marker=passed`, `sole-marker-timed=passed`, `sole-marker-eodm=passed`, and `lifecycle-actions=passed`. It must also include diagnostics `not-required`; Compose down, image removal, temporary-root removal, and environment restoration PASS; residual container/network/volume/image zero; `cleanup-errors=0`; `cleanup=complete`; and exactly `[RESULT] lifecycle-expiration=PASSED`.

Any command, AWS assertion, parser, topology, or cleanup failure leaves `docs/lifecycle-expiration-evidence-2026-08-26.log` at `NOT RUN`, leaves README and ROADMAP unchanged, retains `.debug-journal.md`, keeps Task 14 closed, and stops for a new reviewed plan. No retry, diagnostic round, correction, evidence promotion, commit, push, or tag is inherited.

- [ ] **Step 40: Record consumed final normal #18 and keep promotion closed**

Attempt #18 used one fresh owned five-service topology. Topology and metadata passed. Full `postgres_versioning`, `postgres_lifecycle` hard-loss/reclaim fencing, `e2e`, full `multi_gateway`, and signed `integration lifecycle_expiration_invariants` all passed; the signed group had already emitted the valid Kubo `/api/v0/pin/rm` zero-request receipt.

The runner entered AWS and then ended with generic `execution-failed` before emitting any control-plane, current-version, noncurrent-version, timed-marker, or EODM scenario receipt. The existing receipt therefore proves neither which AWS scenario started nor which helper failed, and this plan infers no cause.

Failure diagnostics, Compose down, exact image removal, temporary-root removal, and environment restoration passed. Independent residual container/network/volume/image counts were zero and `cleanup-errors=0`. Attempt #18 is consumed and may not be rerun. `docs/lifecycle-expiration-evidence-2026-08-26.log` remains exactly `NOT RUN`; README and ROADMAP remain unchanged; `.debug-journal.md` stays retained; Task 14 remains closed; and no Git write, push, or tag is authorized.

- [ ] **Step 41: Write Docker-free RED contracts for an AWS-only diagnostic and fixed substage order**

Modify only `tests/client-smoke.Tests.ps1` first. Require a fifth top-level switch named `DiagnoseLifecycleAws`; exactly zero or one of `Run`, `DiagnoseMultiGateway`, `DiagnoseLifecycleRaceExact`, `DiagnoseLifecycleRaceStability`, and `DiagnoseLifecycleAws` may be selected. Zero must retain the exact `NOT RUN` output. The AWS branch must occur after metadata, call only `Invoke-LifecycleAwsEvidence`, and contain no `Invoke-LifecycleRustSuites` or other diagnostic invocation.

Require `Invoke-LifecycleAwsEvidence` to emit this exact fixed order, with each base line immediately before its existing function call and each `passed` line immediately after return:

```powershell
$expectedAwsSubstages = @(
    "control-plane", "control-plane-passed",
    "current-unversioned", "current-unversioned-passed",
    "current-enabled", "current-enabled-passed",
    "current-suspended", "current-suspended-passed",
    "noncurrent-content", "noncurrent-content-passed",
    "noncurrent-marker", "noncurrent-marker-passed",
    "sole-marker-timed", "sole-marker-timed-passed",
    "sole-marker-eodm", "sole-marker-eodm-passed"
)
$awsEvidenceSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleAwsEvidence"
$actualAwsSubstages = @(
    [regex]::Matches(
        $awsEvidenceSource,
        'Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=(?<stage>[a-z-]+)"'
    ) | ForEach-Object { $_.Groups['stage'].Value }
)
Assert-True (($actualAwsSubstages -join ',') -ceq ($expectedAwsSubstages -join ',')) "Lifecycle AWS fixed substage order changed"
$orderedAwsFragments = @(
    'aws-substage=control-plane', 'Invoke-LifecycleControlPlaneEvidence -State $State -Network $Network -Endpoint $endpoint', 'aws-substage=control-plane-passed',
    'aws-substage=current-unversioned', 'Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "unversioned"', 'aws-substage=current-unversioned-passed',
    'aws-substage=current-enabled', 'Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "enabled"', 'aws-substage=current-enabled-passed',
    'aws-substage=current-suspended', 'Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "suspended"', 'aws-substage=current-suspended-passed',
    'aws-substage=noncurrent-content', 'Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "content"', 'aws-substage=noncurrent-content-passed',
    'aws-substage=noncurrent-marker', 'Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "marker"', 'aws-substage=noncurrent-marker-passed',
    'aws-substage=sole-marker-timed', 'Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "timed"', 'aws-substage=sole-marker-timed-passed',
    'aws-substage=sole-marker-eodm', 'Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "eodm"', 'aws-substage=sole-marker-eodm-passed'
)
$awsFragmentCursor = 0
foreach ($fragment in $orderedAwsFragments) {
    $nextIndex = $awsEvidenceSource.IndexOf($fragment, $awsFragmentCursor, [StringComparison]::Ordinal)
    Assert-True ($nextIndex -ge $awsFragmentCursor) "Lifecycle AWS call/substage order changed at: $fragment"
    $awsFragmentCursor = $nextIndex + $fragment.Length
}
foreach ($forbidden in @('bucket=', 'key=', 'id=', 'version=', 'cid=', 'xml=', 'body=', 'status=', 'error=', 'raw=', 'StdOut', 'StdErr', 'Exception.Message')) {
    Assert-NotContains $awsEvidenceSource $forbidden "Lifecycle AWS substage instrumentation exposed dynamic data: $forbidden"
}
```

The source/AST contract must additionally require success state `aws-diagnostic-passed`, fixed success result `aws-passed`, generic fixed failure reason `execution-failed`, and exact no-Rust isolation. It must reject duplicate/missing/out-of-order substages, a dynamic/interpolated substage, multiple selected switches, any diagnostic switch combined with `Run`, and any AWS diagnostic branch that calls the five Rust suites. Run the new contract before modifying the runner; it must fail only because `DiagnoseLifecycleAws` and fixed substages do not yet exist:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$awsDiagnosticRed = $LASTEXITCODE
if ($awsDiagnosticRed -ne 1) { throw "AWS diagnostic static RED returned an unexpected exit code" }
```

- [ ] **Step 42: Add the mutually-exclusive AWS mode and fixed no-data substages**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Extend the top-level mode guard exactly:

```powershell
[CmdletBinding()]
param(
    [switch]$Run,
    [switch]$DiagnoseMultiGateway,
    [switch]$DiagnoseLifecycleRaceExact,
    [switch]$DiagnoseLifecycleRaceStability,
    [switch]$DiagnoseLifecycleAws
)

$requestedModeCount = @(
    $Run.IsPresent,
    $DiagnoseMultiGateway.IsPresent,
    $DiagnoseLifecycleRaceExact.IsPresent,
    $DiagnoseLifecycleRaceStability.IsPresent,
    $DiagnoseLifecycleAws.IsPresent
).Where({ $_ }).Count
if ($requestedModeCount -gt 1) {
    throw "Run and lifecycle diagnostic switches are mutually exclusive"
}
if ($requestedModeCount -eq 0) {
    Write-Host "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"
    exit 0
}
```

Instrument the existing AWS function with only fixed lines around the unchanged calls:

```powershell
function Invoke-LifecycleAwsEvidence {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network)
    $endpoint = "http://load-balancer:9000"
    Write-LifecycleEvidence -Category "command" -Value "aws-cli-path-style-sigv4-lifecycle-actions"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=control-plane"
    Invoke-LifecycleControlPlaneEvidence -State $State -Network $Network -Endpoint $endpoint
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=control-plane-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-unversioned"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "unversioned"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-unversioned-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-enabled"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "enabled"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-enabled-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-suspended"
    Invoke-LifecycleCurrentDaysScenario -State $State -Network $Network -Endpoint $endpoint -Versioning "suspended"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=current-suspended-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-content"
    Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "content"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-content-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-marker"
    Invoke-LifecycleNoncurrentScenario -State $State -Network $Network -Endpoint $endpoint -Target "marker"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=noncurrent-marker-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-timed"
    Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "timed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-timed-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-eodm"
    Invoke-LifecycleMarkerScenario -State $State -Network $Network -Endpoint $endpoint -Mode "eodm"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-substage=sole-marker-eodm-passed"
    Write-LifecycleEvidence -Category "result" -Value "lifecycle-actions=passed"
}
```

After metadata, add one branch that never runs Rust or another diagnostic:

```powershell
if ($DiagnoseLifecycleAws) {
    Set-LifecycleStage -State $State -Stage "aws"
    Invoke-LifecycleAwsEvidence -State $State -Network $network
    $State.DiagnosticOutcome = "aws-diagnostic-passed"
} elseif ($DiagnoseLifecycleRaceStability) {
    Invoke-LifecycleRaceStabilityDiagnostic -State $State
} elseif ($DiagnoseLifecycleRaceExact) {
    Invoke-LifecycleRaceExactDiagnostic -State $State
} elseif ($DiagnoseMultiGateway) {
    Invoke-LifecycleMultiGatewayDiagnostic -State $State
} else {
    Invoke-LifecycleRustSuites -State $State
    Set-LifecycleStage -State $State -Stage "aws"
    Invoke-LifecycleAwsEvidence -State $State -Network $network
}
```

Extend the existing success chain with the fixed AWS diagnostic success result; failures retain the generic fixed `execution-failed` result after emitting their last safe stage and exact cleanup:

```powershell
if ($workSucceeded) {
    Write-LifecycleEvidence -Category "cleanup" -Value "cleanup=complete"
    if ($state.DiagnosticOutcome -eq "aws-diagnostic-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=aws-passed"
    } elseif ($state.DiagnosticOutcome -eq "exact-race-stability-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-stability-passed"
    } elseif ($state.DiagnosticOutcome -eq "exact-race-passed") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=exact-race-passed"
    } elseif ($state.DiagnosticOutcome -eq "not-reproduced") {
        Write-Host "[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=not-reproduced"
    } else {
        Write-Host "[RESULT] lifecycle-expiration=PASSED"
    }
    exit 0
}
```

Do not print exception text, AWS output, identifiers, XML, response bodies, or dynamic substage names. The normal `-Run`, existing diagnostic branches, AWS helper semantics, and cleanup remain otherwise unchanged.

- [ ] **Step 43: Prove AWS diagnostic isolation and ordering Docker-free**

Run all source/AST/no-run/mutual-exclusion/order contracts without Rust, Docker, AWS, evidence, README, or ROADMAP changes:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic client/static contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after AWS instrumentation" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "AWS diagnostic whitespace check failed" }
```

- [ ] **Step 44: After current plan-critic, run exactly one fresh owned AWS-only #19**

This is the sole live round authorized by this revision. Start one fresh owned five-service topology under the existing no-pull/offline-build/no-install, local-image inspection, unique ownership, loopback, logs-first, cleanup, environment-restoration, and residual-zero guards. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #19 is consumed on setup. It must pass preflight/topology/metadata and then branch directly to `Invoke-LifecycleAwsEvidence`. It must emit no `cargo test` command receipt, invoke no five-suite function, and select none of `Run`, `DiagnoseMultiGateway`, `DiagnoseLifecycleRaceExact`, or `DiagnoseLifecycleRaceStability`.

On success, require all sixteen fixed substage lines in exact order, all existing AWS scenario assertions, `lifecycle-actions=passed`, diagnostics `not-required`, down/image/temp/environment PASS, residual container/network/volume/image zero, `cleanup-errors=0`, cleanup complete, and `[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=aws-passed`. On AWS failure, require the last base substage line and absence of its matching `passed` line, fixed `[RESULT] lifecycle-expiration=FAILED reason=execution-failed`, diagnostics attempted, and the same exact cleanup receipt. No bucket, key, version, CID, XML, body, status, error, raw native output, or exception text may be retained.

- [ ] **Step 45: Record #19's fixed receipt and stop for a causal-fix review**

Whether #19 passes or fails, record only its fixed substage sequence, terminal diagnostic result, and exact cleanup in a new complete plan revision. A failure localizes the boundary to the last base/not-passed fixed substage; the next plan must inspect only that helper, establish a causal RED, apply the minimum correction, run the complete nonlive matrix, and obtain a fresh plan-critic receipt before any new final normal. If #19 passes all AWS substages, record that fact without inventing a cause and require a new reviewed final-normal decision.

This revision authorizes no causal code/test correction, no normal runner, no other diagnostic, no evidence/README/ROADMAP promotion, no Task 14, no subsequent attempt number, and no Git write. #19 has no inherited retry. Evidence remains `NOT RUN`; README/ROADMAP remain unchanged; `.debug-journal.md` stays retained; Task 14 stays closed; no pull/install/push/tag is authorized.

- [ ] **Step 46: Record consumed AWS-only #19 at the outer control-plane boundary**

Attempt #19 used one fresh owned topology. Topology and metadata passed. The AWS-only branch emitted the fixed outer control-plane stage and then failed before `control-plane-passed`; it emitted no current, noncurrent, or marker stage. The final result was generic `[RESULT] lifecycle-expiration=FAILED reason=execution-failed`.

Failure diagnostics, Compose down, exact image removal, temporary-root removal, and environment restoration passed. Independent residual container/network/volume/image counts were zero and `cleanup-errors=0`. Attempt #19 is consumed and may not be rerun. This receipt localizes only to `Invoke-LifecycleControlPlaneEvidence`; it does not identify an operation or prove a product cause. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 47: Write Docker-free RED contracts for fixed control-plane substage order**

Modify only `tests/client-smoke.Tests.ps1` first. Extract `Invoke-LifecycleControlPlaneEvidence` and require exactly these twenty-two fixed diagnostic values in order:

```powershell
$expectedControlPlaneSubstages = @(
    "files-written", "files-written-passed",
    "bucket-created", "bucket-created-passed",
    "initial-put", "initial-put-passed",
    "initial-get-shape", "initial-get-shape-passed",
    "initial-revision", "initial-revision-passed",
    "unsupported-rejected", "unsupported-rejected-passed",
    "revision-unchanged", "revision-unchanged-passed",
    "replacement-put", "replacement-put-passed",
    "replacement-get-shape", "replacement-get-shape-passed",
    "lifecycle-deleted", "lifecycle-deleted-passed",
    "absent-get-verified", "absent-get-verified-passed"
)
$controlPlaneSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleControlPlaneEvidence"
$actualControlPlaneSubstages = @(
    [regex]::Matches(
        $controlPlaneSource,
        'Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=(?<stage>[a-z-]+)"'
    ) | ForEach-Object { $_.Groups['stage'].Value }
)
Assert-True (($actualControlPlaneSubstages -join ',') -ceq ($expectedControlPlaneSubstages -join ',')) "Lifecycle control-plane fixed substage order changed"
foreach ($stage in $actualControlPlaneSubstages) {
    Assert-True ($stage -match '^[a-z]+(?:-[a-z]+)*$') "Lifecycle control-plane substage contains a dynamic value"
}
```

Require every base stage immediately before its existing operation/assertion group and its `-passed` line only after the group returns. `files-written` wraps all three unchanged `Write-LifecycleConfiguration` calls; `initial-get-shape` and `replacement-get-shape` each wrap both GET and shape assertion; `revision-unchanged` wraps the second revision read/comparison. Reject any duplicate, missing, reordered, interpolated, non-allowlisted, bucket/key/ID/XML/status/body/error/raw value, or a substage emitted by a semantic helper. Preserve the outer AWS stages and their exact order.

Run the contract before changing the runner. It must exit `1` only because the inner fixed substages are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$controlPlaneSubstageRed = $LASTEXITCODE
if ($controlPlaneSubstageRed -ne 1) { throw "Control-plane substage static RED returned an unexpected exit code" }
```

- [ ] **Step 48: Add fixed no-data substages around the unchanged control-plane operations**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Keep all existing calls, arguments, assertions, and order; add only these fixed emissions:

```powershell
function Invoke-LifecycleControlPlaneEvidence {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$Endpoint)
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-control") -Prefix "ipfs3-lc"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=files-written"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "current-days"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "noncurrent"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "unsupported-transition"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=files-written-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=bucket-created"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=bucket-created-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-put"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/current-days.xml") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-put-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-get-shape"
    $initialConfiguration = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket)
    Assert-LifecycleConfigurationShape -Document $initialConfiguration -ExpectedKind "current-days"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-get-shape-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-revision"
    $initialRevision = Get-LifecycleRevision -State $State -Bucket $bucket
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=initial-revision-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=unsupported-rejected"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/unsupported-transition.xml") -ExpectedCode "InvalidRequest"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=unsupported-rejected-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=revision-unchanged"
    if ((Get-LifecycleRevision -State $State -Bucket $bucket) -cne $initialRevision) { throw "Unsupported lifecycle action changed the revision" }
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=revision-unchanged-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-put"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/noncurrent.xml") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-put-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-get-shape"
    $replacementConfiguration = Invoke-AwsJson -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket)
    Assert-LifecycleConfigurationShape -Document $replacementConfiguration -ExpectedKind "noncurrent"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=replacement-get-shape-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=lifecycle-deleted"
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-bucket-lifecycle", "--bucket", $bucket) | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=lifecycle-deleted-passed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=absent-get-verified"
    Assert-AwsExpectedFailure -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "get-bucket-lifecycle-configuration", "--bucket", $bucket) -ExpectedCode "NoSuchLifecycleConfiguration"
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-control-plane-substage=absent-get-verified-passed"
    Write-LifecycleEvidence -Category "assertion" -Value "lifecycle-put-get-replace-delete=passed"
    Write-LifecycleEvidence -Category "assertion" -Value "unsupported-revision-unchanged=passed"
}
```

Do not modify production Rust, S3 semantics, AWS command arguments, XML writers, shape checks, revision logic, timeout values, outer AWS stages, cleanup, or result mapping. No emitted substage may interpolate data.

- [ ] **Step 49: Prove inner control-plane instrumentation Docker-free**

Run the exact order/static contract, parse the runner, preserve no-run and all existing mode contracts, run every static suite, and check whitespace. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Control-plane substage static contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after control-plane instrumentation" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Control-plane release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Control-plane PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Control-plane multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Control-plane Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Control-plane instrumentation whitespace check failed" }
```

- [ ] **Step 50: After current plan-critic, run exactly one fresh owned AWS-only #20**

This is the sole live round authorized by this revision. Reuse the existing inspected local images, no-pull/offline-build/no-install controls, fresh project/image/root/bucket/ports, loopback topology, logs-first diagnostics, exact cleanup, environment restoration, and residual-zero checks. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #20 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. Require outer `aws-substage=control-plane`, then the fixed inner sequence. On an inner failure, require the last base `aws-control-plane-substage=<name>` and absence of its matching `<name>-passed`, generic `[RESULT] lifecycle-expiration=FAILED reason=execution-failed`, diagnostics attempted, and exact down/image/temp/environment plus residual container/network/volume/image zero and `cleanup-errors=0`. On full control-plane success, require all twenty-two inner lines and outer `control-plane-passed`; the AWS-only run may continue through the unchanged later scenarios and can succeed only with all outer passes, all AWS assertions, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`.

No bucket/key/ID/version/CID/XML/body/status/error/raw output or exception text may be retained. Do not infer a product cause from the base stage alone.

- [ ] **Step 51: Record #20's fixed receipt and stop before any product fix**

Record only #20's ordered outer/inner fixed stages, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. If it fails, branch only from the last inner base stage lacking its `-passed` partner; inspect that exact operation/assertion group and require a causal RED before proposing any minimum fix. If all inner control-plane stages pass but a later outer scenario fails, branch only from that later outer base/not-passed pair. If #20 passes, record it without inventing a cause.

This revision authorizes no production/test semantic fix before #20's receipt, no normal runner, no other diagnostic, no evidence/README/ROADMAP promotion, no Task 14, no subsequent attempt, and no Git write. #20 has no inherited retry. Cleanup/no-pull/no-install/no-push/no-tag boundaries remain unchanged.

- [ ] **Step 52: Record consumed #20 and the source-confirmed runner-only JSON/XML mismatch**

Attempt #20 used one fresh owned topology. Topology and metadata passed. It reached outer control-plane and emitted `aws-substage=control-plane`, reached inner `files-written`, emitted `bucket-created` and `bucket-created-passed`, then emitted `initial-put` and failed before `initial-put-passed`. It emitted no later control-plane or outer AWS substage. Diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #20 is consumed and may not be rerun.

Source inspection confirms a runner-only cause: AWS CLI `s3api put-bucket-lifecycle-configuration --lifecycle-configuration file://...` parses its file argument as the CLI input JSON structure, while `Write-LifecycleConfiguration` currently writes REST XML into `.xml` files. The signed REST/XML product tests and signed pin-rm invariant already pass, so this receipt does not justify any Rust, s3s, route, serializer, handler, store, worker, or lifecycle semantic change. Evidence remains `NOT RUN`; README/ROADMAP remain unchanged; Task 14 stays closed.

- [ ] **Step 53: Write a Docker-free RED contract for strict AWS CLI lifecycle JSON**

Modify only `tests/client-smoke.Tests.ps1` first. Extract and execute the existing `Assert-CanonicalChildPath` and `Write-LifecycleConfiguration` function definitions in an exact owned OS-temp child. For each of the five kinds, require UTF-8 without BOM, `.json`, exact compact JSON bytes, successful `ConvertFrom-Json`, one top-level `Rules` property, one rule, empty `Filter`, `Status=Enabled`, and only the expected action shape:

```powershell
$expectedLifecycleConfigurationJson = [ordered]@{
    "current-days" = '{"Rules":[{"ID":"current-days","Filter":{},"Status":"Enabled","Expiration":{"Days":1}}]}'
    "noncurrent" = '{"Rules":[{"ID":"noncurrent-days","Filter":{},"Status":"Enabled","NoncurrentVersionExpiration":{"NoncurrentDays":1}}]}'
    "timed-marker" = '{"Rules":[{"ID":"timed-marker","Filter":{},"Status":"Enabled","Expiration":{"Days":1}}]}'
    "eodm" = '{"Rules":[{"ID":"eodm","Filter":{},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true}}]}'
    "unsupported-transition" = '{"Rules":[{"ID":"unsupported","Filter":{},"Status":"Enabled","Transitions":[{"Days":1,"StorageClass":"GLACIER"}]}]}'
}
$canonicalPathFunction = Get-LifecycleRunnerFunctionSource "Assert-CanonicalChildPath"
$configurationWriterFunction = Get-LifecycleRunnerFunctionSource "Write-LifecycleConfiguration"
. ([scriptblock]::Create($canonicalPathFunction))
. ([scriptblock]::Create($configurationWriterFunction))
$tempParent = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$fixtureRoot = Join-Path $tempParent ("ipfs3-lifecycle-json-contract-" + [Guid]::NewGuid().ToString("N"))
$null = [IO.Directory]::CreateDirectory($fixtureRoot)
try {
    foreach ($entry in $expectedLifecycleConfigurationJson.GetEnumerator()) {
        $path = Write-LifecycleConfiguration -RunRoot $fixtureRoot -Kind $entry.Key
        Assert-True ([IO.Path]::GetExtension($path) -ceq ".json") "Lifecycle configuration extension must be .json"
        $bytes = [IO.File]::ReadAllBytes($path)
        Assert-True ($bytes.Count -lt 3 -or -not ($bytes[0] -eq 0xef -and $bytes[1] -eq 0xbb -and $bytes[2] -eq 0xbf)) "Lifecycle JSON must be UTF-8 without BOM"
        $text = [Text.UTF8Encoding]::new($false, $true).GetString($bytes)
        Assert-True ($text -ceq $entry.Value) "Lifecycle JSON bytes differed for $($entry.Key)"
        $document = $text | ConvertFrom-Json -NoEnumerate
        Assert-True ($document -is [pscustomobject]) "Lifecycle JSON root must be one object"
        Assert-True (@($document.PSObject.Properties).Count -eq 1 -and $document.PSObject.Properties.Name -ceq "Rules") "Lifecycle JSON root must contain only Rules"
        Assert-True (@($document.Rules).Count -eq 1) "Lifecycle JSON must contain exactly one rule"
        Assert-True (@($document.Rules[0].Filter.PSObject.Properties).Count -eq 0) "Lifecycle JSON Filter must be empty"
        Assert-True ([string]$document.Rules[0].Status -ceq "Enabled") "Lifecycle JSON status must be Enabled"
    }
} finally {
    $resolvedFixtureRoot = [IO.Path]::GetFullPath($fixtureRoot)
    $resolvedTempParent = [IO.Path]::GetFullPath($tempParent).TrimEnd([IO.Path]::DirectorySeparatorChar)
    if ([IO.Directory]::GetParent($resolvedFixtureRoot).FullName.TrimEnd([IO.Path]::DirectorySeparatorChar) -cne $resolvedTempParent) {
        throw "Lifecycle JSON fixture root escaped the OS temp parent"
    }
    if (Test-Path -LiteralPath $resolvedFixtureRoot) { Remove-Item -LiteralPath $resolvedFixtureRoot -Recurse -Force -ErrorAction Stop }
}
```

Also require `Write-LifecycleConfiguration` to use `ConvertTo-Json -Depth 8 -Compress` and `[Text.UTF8Encoding]::new($false)`, require every lifecycle `file:///work/` reference to end in `.json`, and reject `.xml`, `<LifecycleConfiguration`, XML fragments, or `file:///work/*.xml` anywhere in the lifecycle runner. Run the contract before changing the runner; it must exit `1` only because the current writer and references are XML:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$lifecycleJsonRed = $LASTEXITCODE
if ($lifecycleJsonRed -ne 1) { throw "Lifecycle CLI JSON static RED returned an unexpected exit code" }
```

- [ ] **Step 54: Replace only the runner fixture writer and file references with strict JSON**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Replace `Write-LifecycleConfiguration` with this exact runner-only implementation:

```powershell
function Write-LifecycleConfiguration {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][ValidateSet("current-days", "noncurrent", "timed-marker", "eodm", "unsupported-transition")][string]$Kind
    )
    $path = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "$Kind.json")
    $document = switch ($Kind) {
        "current-days" {
            [ordered]@{ Rules = @([ordered]@{ ID = "current-days"; Filter = [ordered]@{}; Status = "Enabled"; Expiration = [ordered]@{ Days = 1 } }) }
        }
        "noncurrent" {
            [ordered]@{ Rules = @([ordered]@{ ID = "noncurrent-days"; Filter = [ordered]@{}; Status = "Enabled"; NoncurrentVersionExpiration = [ordered]@{ NoncurrentDays = 1 } }) }
        }
        "timed-marker" {
            [ordered]@{ Rules = @([ordered]@{ ID = "timed-marker"; Filter = [ordered]@{}; Status = "Enabled"; Expiration = [ordered]@{ Days = 1 } }) }
        }
        "eodm" {
            [ordered]@{ Rules = @([ordered]@{ ID = "eodm"; Filter = [ordered]@{}; Status = "Enabled"; Expiration = [ordered]@{ ExpiredObjectDeleteMarker = $true } }) }
        }
        "unsupported-transition" {
            [ordered]@{ Rules = @([ordered]@{ ID = "unsupported"; Filter = [ordered]@{}; Status = "Enabled"; Transitions = @([ordered]@{ Days = 1; StorageClass = "GLACIER" }) }) }
        }
    }
    $json = $document | ConvertTo-Json -Depth 8 -Compress
    [IO.File]::WriteAllText($path, $json, [Text.UTF8Encoding]::new($false))
    return $path
}
```

Replace every lifecycle CLI file reference exactly:

```text
"file:///work/current-days.xml"       -> "file:///work/current-days.json"
"file:///work/noncurrent.xml"         -> "file:///work/noncurrent.json"
"file:///work/$kind.xml"              -> "file:///work/$kind.json"
"file:///work/unsupported-transition.xml" -> "file:///work/unsupported-transition.json"
```

Apply the replacements at every current, noncurrent, marker, and control-plane call site. Do not change AWS commands, expected error codes, scenario logic, REST/s3s product tests, Rust, XML protocol support, control-plane stages, worker timing, cleanup, or result mapping.

- [ ] **Step 55: Prove strict JSON fixtures and all runner contracts Docker-free**

Run the pure JSON byte/parse/shape fixtures, PowerShell parser, no-run contract, all five static suites, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle CLI JSON contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after JSON correction" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after JSON correction" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle JSON release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle JSON PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle JSON multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle JSON Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Lifecycle JSON whitespace check failed" }
```

- [ ] **Step 56: After current plan-critic, run exactly one fresh owned AWS-only #21**

This is the sole live round authorized by this revision. Reuse the existing no-pull/offline-build/no-install, inspected images, fresh ownership, loopback topology, logs-first diagnostics, exact cleanup, environment restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #21 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. Require all eleven inner control-plane base/pass pairs, outer `control-plane-passed`, and all eight outer base/pass pairs in exact order: control-plane, current-unversioned, current-enabled, current-suspended, noncurrent-content, noncurrent-marker, sole-marker-timed, sole-marker-eodm. Full AWS-only PASS additionally requires all existing scenario assertions, diagnostics `not-required`, exact down/image/temp/environment, residual container/network/volume/image zero, `cleanup-errors=0`, cleanup complete, and `[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=aws-passed`.

On failure, require the last fixed base without its `-passed` pair, generic `execution-failed`, diagnostics attempted, and exact cleanup. No IDs, paths, JSON bodies, AWS response/error text, raw native output, or exception text may be retained.

- [ ] **Step 57: Gate any new final normal on #21's complete eight-substage PASS**

Record #21's ordered fixed receipt and cleanup in a new complete plan revision. Only all eight outer base/pass pairs, all eleven inner control-plane pairs, every AWS assertion, diagnostic success, and exact cleanup may proceed to a newly reviewed final normal. That next plan must run the complete nonlive matrix and obtain a fresh current-revision plan-critic receipt before authorizing the new final normal.

If #21 fails, branch only from its last fixed base without `-passed`; no product change follows unless the receipt contradicts the source-confirmed runner-only mismatch and a new causal RED proves a separate defect. This revision authorizes no final normal, evidence/README/ROADMAP promotion, Task 14, subsequent attempt, or Git write. #21 has no inherited retry; cleanup/no-pull/no-install/no-push/no-tag boundaries remain unchanged.

- [ ] **Step 58: Record consumed #21 at the initial-revision boundary**

Attempt #21 used one fresh owned topology. Topology and metadata passed, and the strict `.json` lifecycle files were accepted. Outer control-plane and inner files-written, bucket-created, initial-put, and initial-get-shape each reached their matching `-passed` receipt. The runner then emitted `aws-control-plane-substage=initial-revision` and failed before `initial-revision-passed`; it emitted no later control-plane or outer AWS substage.

Failure diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #21 is consumed and may not be rerun. This receipt localizes only to `Get-LifecycleRevision` or its fixed shape check; it does not reveal row count/value and proves no product cause. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 59: Write Docker-free RED fixtures for inline revision row-count and shape receipts**

Modify only `tests/client-smoke.Tests.ps1` first. Extract `Get-LifecycleRevision` and require the row-count and shape expressions to remain inline after `$rows = @(Invoke-LifecycleSql ...)`; no revision receipt helper exists or may be introduced. Execute these exact expression fixtures without database, Docker, bucket, SQL, or raw output:

```powershell
$revisionReceiptFixtures = @(
    [pscustomobject]@{ Rows = [string[]]@(); RowCount = "0"; ShapeValid = $false },
    [pscustomobject]@{ Rows = [string[]]@("1"); RowCount = "1"; ShapeValid = $true },
    [pscustomobject]@{ Rows = [string[]]@("42"); RowCount = "1"; ShapeValid = $true },
    [pscustomobject]@{ Rows = [string[]]@(" 7 "); RowCount = "1"; ShapeValid = $true },
    [pscustomobject]@{ Rows = [string[]]@(""); RowCount = "1"; ShapeValid = $false },
    [pscustomobject]@{ Rows = [string[]]@("0"); RowCount = "1"; ShapeValid = $false },
    [pscustomobject]@{ Rows = [string[]]@("01"); RowCount = "1"; ShapeValid = $false },
    [pscustomobject]@{ Rows = [string[]]@("invalid"); RowCount = "1"; ShapeValid = $false },
    [pscustomobject]@{ Rows = [string[]]@("1", "2"); RowCount = "many"; ShapeValid = $false }
)
foreach ($fixture in $revisionReceiptFixtures) {
    $rows = @($fixture.Rows)
    $rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }
    $shapeValid = $rows.Count -eq 1 -and -not [string]::IsNullOrWhiteSpace($rows[0]) -and $rows[0].Trim() -cmatch '^[1-9][0-9]*$'
    Assert-True ($rowCountReceipt -ceq $fixture.RowCount) "Revision row-count fixture differed"
    Assert-True ($shapeValid -eq $fixture.ShapeValid) "Revision shape fixture differed"
}
```

Extract `Get-LifecycleRevision` and require exactly these two fixed evidence forms before the existing fixed validation error:

```powershell
$revisionSource = Get-LifecycleRunnerFunctionSource "Get-LifecycleRevision"
Assert-Contains $revisionSource '$rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }' "Inline revision row-count expression is missing"
Assert-Contains $revisionSource '$shapeValid = $rows.Count -eq 1 -and $rows[0].Trim() -cmatch ''^[1-9][0-9]*$''' "Inline revision shape expression is missing"
Assert-Contains $revisionSource 'Write-LifecycleEvidence -Category "diagnostic" -Value "revision-row-count=$rowCountReceipt"' "Revision row-count receipt is missing"
Assert-Contains $revisionSource 'Write-LifecycleEvidence -Category "diagnostic" -Value "revision-shape-valid=$($shapeValid.ToString().ToLowerInvariant())"' "Revision shape receipt is missing"
Assert-Contains $revisionSource 'throw "Lifecycle revision receipt is invalid"' "Revision validation error changed"
foreach ($forbidden in @('revision-value=', 'Write-LifecycleEvidence -Category "diagnostic" -Value "bucket=', 'Write-LifecycleEvidence -Category "diagnostic" -Value "sql=', 'StdOut', 'StdErr', 'Exception.Message')) {
    Assert-NotContains $revisionSource $forbidden "Inline revision diagnostics exposed a forbidden form: $forbidden"
}
```

The static contract must also prove the inline row-count is only `0`, `1`, or `many`, shape validity is boolean, no write interpolates `$rows`, `$Bucket`, SQL, or the revision value, and no helper declaration/call exists. Run RED before changing the runner; it must exit `1` only because the inline safe receipts are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$revisionReceiptRed = $LASTEXITCODE
if ($revisionReceiptRed -ne 1) { throw "Inline revision diagnostics static RED returned an unexpected exit code" }
```

- [ ] **Step 60: Emit only fixed inline row-count and shape receipts before existing validation**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Replace only `Get-LifecycleRevision`; keep all diagnostics inline and introduce no helper:

```powershell
function Get-LifecycleRevision {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Bucket)
    $rows = @(Invoke-LifecycleSql -State $State -Statement "revision" -Bucket $Bucket -Key "lifecycle-control.txt")
    $rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }
    $shapeValid = $rows.Count -eq 1 -and $rows[0].Trim() -cmatch '^[1-9][0-9]*$'
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-row-count=$rowCountReceipt"
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-shape-valid=$($shapeValid.ToString().ToLowerInvariant())"
    if (-not $shapeValid) { throw "Lifecycle revision receipt is invalid" }
    return $rows[0].Trim()
}
```

Do not emit the revision, bucket, SQL, row text, length, hash, database output, or error detail. Do not change the query, accepted positive-decimal shape, returned revision, control-plane semantics, JSON fixtures, AWS commands, cleanup, timeout, product code, or result mapping.

- [ ] **Step 61: Prove revision receipts and all runner contracts Docker-free**

Run pure fixtures, all static contracts, parser/no-run, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Inline revision diagnostics contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after revision receipts" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after revision receipts" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision receipt release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision receipt PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision receipt multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision receipt Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Revision receipt whitespace check failed" }
```

- [ ] **Step 62: After current plan-critic, run exactly one fresh owned AWS-only #22**

This is the sole live round authorized by this revision. Reuse all existing no-pull/offline-build/no-install, inspected-image, fresh-ownership, loopback, logs-first, exact-cleanup, environment-restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #22 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. At each `Get-LifecycleRevision` call, require `revision-row-count=0|1|many` followed by `revision-shape-valid=true|false` and no revision value. At initial-revision failure, branch only from that pair plus absence of `initial-revision-passed`; on success require `revision-row-count=1`, `revision-shape-valid=true`, and `initial-revision-passed` before continuing. Apply the same receipt contract to the later revision-unchanged read.

Any failure requires generic `execution-failed`, diagnostics attempted, exact down/image/temp/environment, residual container/network/volume/image zero, and `cleanup-errors=0`. Full AWS-only PASS still requires all eleven inner and all eight outer base/pass pairs, all AWS assertions, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`. No value/bucket/SQL/path/JSON/error/raw output or exception text may be retained.

- [ ] **Step 63: Record #22 and branch only from its fixed revision receipt**

Record #22's ordered outer/inner stages, each fixed revision row-count/shape pair, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. If initial revision fails, branch only on `0|1|many` plus `true|false`; do not infer or retrieve the revision value. If it passes and a later stage fails, branch only from that later fixed receipt. If all eight outer stages pass, record full AWS-only PASS and require a newly reviewed final-normal plan with the complete nonlive matrix.

This revision authorizes no product/test semantic correction after #22, no final normal, no evidence/README/ROADMAP promotion, no Task 14, no subsequent attempt, and no Git write. #22 has no inherited retry. All cleanup/no-pull/no-install/no-push/no-tag gates remain unchanged.

- [ ] **Step 64: Record consumed #22 and the bounded scalarization hypothesis**

AWS-only #22 accepted the strict JSON lifecycle files and passed initial PUT plus initial GET shape. It then entered initial-revision and failed before emitting either `revision-row-count` or `revision-shape-valid`, and before `initial-revision-passed`. No later AWS substage ran. Failure diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #22 is consumed and may not be rerun.

Source inspection identified one bounded runner-only hypothesis: direct assignment of a returned single string can scalarize it, making `$rows[0]` a `Char`. The test-only probe/correction is exactly `$rows = @(Invoke-LifecycleSql ...)`; #23 later reproduces before return and therefore refutes this hypothesis as the live cause. The SQL statement, database result, accepted positive-decimal shape, revision comparison, product code, and signed REST behavior remain unchanged. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 65: Write a Docker-free RED contract for the one-line array boundary**

Modify only `tests/client-smoke.Tests.ps1` first. Require the exact array-wrapped assignment in `Get-LifecycleRevision`, exactly once, while preserving the exact `Invoke-LifecycleSql` arguments:

```powershell
$revisionSource = Get-LifecycleRunnerFunctionSource "Get-LifecycleRevision"
$expectedArrayAssignment = '$rows = @(Invoke-LifecycleSql -State $State -Statement "revision" -Bucket $Bucket -Key "lifecycle-control.txt")'
Assert-True (([regex]::Matches($revisionSource, [regex]::Escape($expectedArrayAssignment))).Count -eq 1) "Get-LifecycleRevision must array-wrap exactly one SQL result"
Assert-True (([regex]::Matches($revisionSource, 'Invoke-LifecycleSql')).Count -eq 1) "Get-LifecycleRevision SQL call count changed"
```

Add a pure fixture that proves one emitted string remains a one-element array of `String` and still passes the same inline row-count/shape expressions:

```powershell
function Invoke-SingleRevisionLineFixture { return "7" }
$singleRows = @(Invoke-SingleRevisionLineFixture)
Assert-True ($singleRows -is [array]) "Single revision output must remain an array"
Assert-True ($singleRows.Count -eq 1) "Single revision output array must contain one row"
Assert-True ($singleRows[0] -is [string]) "Single revision row must remain String rather than Char"
Assert-True ($singleRows[0].Trim() -ceq "7") "Single revision row trim changed"
$singleRowCountReceipt = if ($singleRows.Count -eq 0) { "0" } elseif ($singleRows.Count -eq 1) { "1" } else { "many" }
$singleShapeValid = $singleRows.Count -eq 1 -and $singleRows[0].Trim() -cmatch '^[1-9][0-9]*$'
Assert-True ($singleRowCountReceipt -ceq "1") "Single revision row-count receipt changed"
Assert-True $singleShapeValid "Single revision shape must remain valid"
```

Retain the existing zero/one/many and true/false fixtures. Reject any SQL text/argument, receipt, return, regex, bucket, timeout, or product change. Run RED before changing the runner; it must exit `1` only because the direct assignment is not array wrapped:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$revisionArrayRed = $LASTEXITCODE
if ($revisionArrayRed -ne 1) { throw "Revision array-boundary static RED returned an unexpected exit code" }
```

- [ ] **Step 66: Array-wrap exactly the existing lifecycle SQL call**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Keep the inline row-count/shape logic and both fixed diagnostic lines unchanged; replace only the assignment in this exact function:

```powershell
function Get-LifecycleRevision {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Bucket)
    $rows = @(Invoke-LifecycleSql -State $State -Statement "revision" -Bucket $Bucket -Key "lifecycle-control.txt")
    $rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }
    $shapeValid = $rows.Count -eq 1 -and $rows[0].Trim() -cmatch '^[1-9][0-9]*$'
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-row-count=$rowCountReceipt"
    Write-LifecycleEvidence -Category "diagnostic" -Value "revision-shape-valid=$($shapeValid.ToString().ToLowerInvariant())"
    if (-not $shapeValid) { throw "Lifecycle revision receipt is invalid" }
    return $rows[0].Trim()
}
```

Do not alter `Invoke-LifecycleSql`, its SQL, inline receipt formats, validation regex, returned revision, callers, AWS scenarios, JSON fixtures, product Rust, cleanup, or result mapping. Do not introduce a revision receipt helper.

- [ ] **Step 67: Prove the scalarization correction and all runner contracts Docker-free**

Run the scalarization fixture, complete static suite, parser/no-run, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision array-boundary contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after array wrapping" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after array wrapping" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision array release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision array PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision array multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Revision array Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Revision array whitespace check failed" }
```

- [ ] **Step 68: After current plan-critic, run exactly one fresh owned AWS-only #23**

This is the sole live round authorized by this revision. Reuse all existing no-pull/offline-build/no-install, inspected-image, fresh-ownership, loopback, logs-first, exact-cleanup, environment-restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #23 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. At initial-revision require `revision-row-count=1`, `revision-shape-valid=true`, and `initial-revision-passed` in that order, with no revision value. Require the same fixed pair at revision-unchanged before `revision-unchanged-passed`. Any failure branches only from the last fixed receipt and requires generic `execution-failed`, diagnostics attempted, exact down/image/temp/environment, residual container/network/volume/image zero, and `cleanup-errors=0`.

Full AWS-only PASS still requires all eleven inner and all eight outer base/pass pairs, all AWS assertions, diagnostics `not-required`, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`. No revision/bucket/SQL/path/JSON/error/raw output or exception text may be retained.

- [ ] **Step 69: Record #23 and stop for a newly reviewed final-normal decision**

Record #23's fixed revision pairs, ordered control-plane/outer stages, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. If a revision pair is not `1/true`, branch only from that pair and do not retrieve the revision value. If revision succeeds and a later stage fails, branch only from that later fixed receipt. Only complete AWS-only PASS may enter a newly reviewed plan that reruns the complete nonlive matrix before authorizing final normal.

This revision authorizes no further correction, final normal, evidence/README/ROADMAP promotion, Task 14, subsequent attempt, or Git write. #23 has no inherited retry. All cleanup/no-pull/no-install/no-push/no-tag gates remain unchanged.

- [ ] **Step 70: Record consumed #23 and the pre-return SQL command boundary**

AWS-only #23 reproduced the same control-plane sequence through `initial-get-shape-passed` and then emitted `initial-revision`. It emitted neither `revision-row-count` nor `revision-shape-valid`, despite the array-wrapped assignment being present, and emitted no later stage. Failure diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #23 is consumed and may not be rerun.

This recurrence proves `Invoke-LifecycleSql` throws before returning rows to `Get-LifecycleRevision`; the scalarization correction does not explain this live failure. It does not identify the psql error class or justify changing SQL. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 71: Write Docker-free RED fixtures for safe lifecycle SQL outcome classification**

Modify only `tests/client-smoke.Tests.ps1` first. Lock `Invoke-Compose` to an optional `[int[]]$AllowedExitCodes = @(0)` parameter passed unchanged to `Invoke-Docker`, and require `Invoke-LifecycleSql` to supply `-AllowedExitCodes @(0, 1)` exactly once without changing its arguments or query switch.

Add a pure `Get-LifecycleSqlErrorCategory` contract with these exact fixtures:

```powershell
$sqlCategoryFunction = Get-LifecycleRunnerFunctionSource "Get-LifecycleSqlErrorCategory"
. ([scriptblock]::Create($sqlCategoryFunction))
$sqlCategoryFixtures = @(
    [pscustomobject]@{ Lines = [string[]]@('ERROR: syntax error at or near ":"'); Category = "variable-substitution" },
    [pscustomobject]@{ Lines = [string[]]@('psql: error: undefined variable "bucket"'); Category = "variable-substitution" },
    [pscustomobject]@{ Lines = [string[]]@('ERROR: relation "bucket_lifecycle_configs" does not exist'); Category = "missing-relation" },
    [pscustomobject]@{ Lines = [string[]]@('psql: error: connection to server at "postgres" failed: Connection refused'); Category = "connection" },
    [pscustomobject]@{ Lines = [string[]]@('server closed the connection unexpectedly'); Category = "connection" },
    [pscustomobject]@{ Lines = [string[]]@('ERROR: permission denied'); Category = "other" },
    [pscustomobject]@{ Lines = [string[]]@(); Category = "other" }
)
foreach ($fixture in $sqlCategoryFixtures) {
    $actualCategory = Get-LifecycleSqlErrorCategory -StdErr $fixture.Lines
    Assert-True ($actualCategory -ceq $fixture.Category) "Lifecycle SQL safe category fixture differed"
}
```

Require `Invoke-LifecycleSql` to emit only these forms: `lifecycle-sql-outcome=failed`, `lifecycle-sql-error-category=variable-substitution|missing-relation|connection|other`, or `lifecycle-sql-outcome=passed`. Reject interpolation of stderr, stdout, query, SQL, bucket, key, path, error message, exit text, or exception. Require failed outcome then category before the fixed throw, and passed outcome before returning rows.

Run RED before changing the runner; it must exit `1` only because safe SQL classification and the `@(0,1)` use are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$sqlCategoryRed = $LASTEXITCODE
if ($sqlCategoryRed -ne 1) { throw "Lifecycle SQL category static RED returned an unexpected exit code" }
```

- [ ] **Step 72: Extend the Compose boundary and emit only fixed lifecycle SQL receipts**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Lock `Invoke-Compose` to this optional parameter and pass-through behavior:

```powershell
function Invoke-Compose {
    param(
        [Parameter(Mandatory)][string]$Project,
        [Parameter(Mandatory)][string[]]$Arguments,
        [string]$Label = "Compose operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker -Arguments (@("compose", "--project-name", $Project, "--file", $ComposeFile) + $Arguments) -Label $Label -Timeout $Timeout -AllowedExitCodes $AllowedExitCodes
}
```

Add the pure classifier:

```powershell
function Get-LifecycleSqlErrorCategory {
    param([AllowEmptyCollection()][AllowNull()][string[]]$StdErr)
    $text = @($StdErr) -join "`n"
    if ($text -match "(?i)(syntax error at or near\s+[`"']?:|undefined variable|unrecognized value .* variable)") {
        return "variable-substitution"
    }
    if ($text -match '(?i)relation .+ does not exist') {
        return "missing-relation"
    }
    if ($text -match '(?i)(connection to server .* failed|could not connect to server|server closed the connection unexpectedly|database system is starting up)') {
        return "connection"
    }
    return "other"
}
```

Keep the query switch and all psql arguments byte-for-byte; change only the command acceptance and outcome handling:

```powershell
$result = Invoke-Compose -Project $State.Project -Arguments @(
    "exec", "-T", "postgres", "psql", "-X", "-U", "ipfs3", "-d", "ipfs3", "-v", "ON_ERROR_STOP=1", "-A", "-t",
    "-v", "bucket=$Bucket", "-v", "key=$Key", "-c", $query
) -Label "targeted lifecycle database assertion" -Timeout ([TimeSpan]::FromSeconds(30)) -AllowedExitCodes @(0, 1)
if ($result.ExitCode -ne 0) {
    $category = Get-LifecycleSqlErrorCategory -StdErr $result.StdErr
    Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=failed"
    Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-error-category=$category"
    throw "Targeted lifecycle database assertion failed"
}
Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=passed"
return @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
```

Never emit or persist stderr, stdout, query, SQL, bucket/key, revision, path, process output, or exception text. Do not alter any SQL string, psql argument, accepted exit beyond `0/1`, caller, product code, lifecycle semantics, timeout, cleanup, or result mapping.

- [ ] **Step 73: Prove SQL categories and all runner contracts Docker-free**

Run classifier fixtures, source contracts, parser/no-run, all five static suites, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle SQL category contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after SQL classification" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after SQL classification" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "SQL category release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "SQL category PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "SQL category multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "SQL category Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "SQL category whitespace check failed" }
```

- [ ] **Step 74: After current plan-critic, run exactly one fresh owned AWS-only #24**

This is the sole live round authorized by this revision. Reuse all existing no-pull/offline-build/no-install, inspected-image, fresh-ownership, loopback, logs-first, exact-cleanup, environment-restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #24 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. At every lifecycle SQL call, require exactly one outcome. Exit 0 emits `lifecycle-sql-outcome=passed` before any returned-row receipt and no category. Exit 1 emits `lifecycle-sql-outcome=failed`, exactly one category from `variable-substitution|missing-relation|connection|other`, no row-count/shape receipt for that call, and the generic failure path.

Branch only from the fixed category. Require diagnostics attempted and exact down/image/temp/environment plus residual container/network/volume/image zero and `cleanup-errors=0` on failure. Full AWS-only PASS still requires SQL passed outcomes, both revision `1/true` pairs, all eleven inner and eight outer stage pairs, all AWS assertions, diagnostics `not-required`, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`. No stderr/stdout/SQL/bucket/key/revision/path/error/raw output may be retained.

- [ ] **Step 75: Record #24 and stop before any SQL or product correction**

Record #24's SQL outcome/category receipts, revision receipts if reached, ordered AWS stages, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. On SQL failure, branch only from `variable-substitution`, `missing-relation`, `connection`, or `other`; do not inspect or disclose raw stderr in the plan. A later correction requires a new causal RED and reviewed plan. On complete AWS-only PASS, require a new plan with complete nonlive and fresh plan-critic before final normal.

This revision authorizes no SQL/product/test semantic correction after #24, no final normal, no evidence/README/ROADMAP promotion, no Task 14, no subsequent attempt, and no Git write. #24 has no inherited retry. All cleanup/no-pull/no-install/no-push/no-tag gates remain unchanged.

- [ ] **Step 76: Record consumed #24 and select the variable-substitution runner branch**

AWS-only #24 reached initial revision and emitted exactly `lifecycle-sql-outcome=failed` followed by `lifecycle-sql-error-category=variable-substitution`. It emitted no row-count/shape receipt or later AWS stage. Failure diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #24 is consumed and may not be rerun.

Source inspection confirms a runner-only psql invocation defect: the targeted query is passed through psql `-c`, where the expected `:'bucket'` and `:'key'` substitution does not occur in this invocation shape. The safe branch permits changing only the runner's validated query construction and psql argv. It does not authorize migration/store/product SQL or arbitrary query input. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 77: Write Docker-free RED fixtures for validator-first literal runner SQL**

Modify only `tests/client-smoke.Tests.ps1` first. Extract a pure `Assert-LifecycleSqlIdentifiers` and require the bucket grammar already used by generated lifecycle buckets: length at most 63 and `^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$`. Require key length at most 256 and `^[A-Za-z0-9](?:[A-Za-z0-9./-]{0,254}[A-Za-z0-9])?$`, explicitly rejecting quotes/backslash/whitespace/colon. Execute these exact fixtures without SQL or Docker:

```powershell
$identifierValidatorSource = Get-LifecycleRunnerFunctionSource "Assert-LifecycleSqlIdentifiers"
. ([scriptblock]::Create($identifierValidatorSource))
$validSqlIdentifiers = @(
    [pscustomobject]@{ Bucket = "ipfs3-lc-a1"; Key = "lifecycle-control.txt" },
    [pscustomobject]@{ Bucket = "a1"; Key = "current-enabled.txt" },
    [pscustomobject]@{ Bucket = "bucket-123"; Key = "folder/item-1.txt" },
    [pscustomobject]@{ Bucket = "bucket-abc"; Key = "A1/b.C-9" }
)
foreach ($fixture in $validSqlIdentifiers) {
    Assert-LifecycleSqlIdentifiers -Bucket $fixture.Bucket -Key $fixture.Key
}
$invalidBuckets = @("", "A-bucket", "-bucket", "bucket-", "bucket_name", "bucket name", "bucket:name", "bucket'name", 'bucket"name', "bucket\name", ("a" * 64))
foreach ($bucket in $invalidBuckets) {
    $rejected = $false
    try { Assert-LifecycleSqlIdentifiers -Bucket $bucket -Key "safe.txt" } catch { $rejected = $true }
    Assert-True $rejected "Unsafe lifecycle SQL bucket fixture was accepted"
}
$invalidKeys = @("", ".leading", "trailing.", "/leading", "trailing/", "-leading", "trailing-", "bad key", "bad:key", "bad'key", 'bad"key', "bad\key", ("a" * 257))
foreach ($key in $invalidKeys) {
    $rejected = $false
    try { Assert-LifecycleSqlIdentifiers -Bucket "safe-bucket" -Key $key } catch { $rejected = $true }
    Assert-True $rejected "Unsafe lifecycle SQL key fixture was accepted"
}
```

Extract `Invoke-LifecycleSql` and require validator invocation to occur before `$query = switch`. Lock exactly five allowlisted switch arms, single-quoted `$Bucket`/`$Key` literals, and exact psql argv with only `-v ON_ERROR_STOP=1`; reject `:'bucket'`, `:'key'`, `-v bucket=`, `-v key=`, quote/backslash acceptance, arbitrary statement/query parameters, concatenated caller SQL, or validator invocation after query construction.

Run RED before changing the runner; it must exit `1` only because the strict validator and literal query contract are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$literalSqlRed = $LASTEXITCODE
if ($literalSqlRed -ne 1) { throw "Lifecycle literal-SQL static RED returned an unexpected exit code" }
```

- [ ] **Step 78: Validate identifiers before constructing the allowlisted literal queries**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Add this fixed validator:

```powershell
function Assert-LifecycleSqlIdentifiers {
    param(
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$Key
    )
    if ($Bucket.Length -gt 63 -or $Bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid lifecycle SQL bucket identifier"
    }
    if ($Key.Length -gt 256 -or $Key -cnotmatch '^[A-Za-z0-9](?:[A-Za-z0-9./-]{0,254}[A-Za-z0-9])?$') {
        throw "Invalid lifecycle SQL key identifier"
    }
}
```

Replace only `Invoke-LifecycleSql` with this validator-first, allowlisted implementation:

```powershell
function Invoke-LifecycleSql {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("current-age", "noncurrent-age", "marker-age", "sole-marker-age", "revision")][string]$Statement,
        [Parameter(Mandatory)][string]$Bucket,
        [Parameter(Mandatory)][string]$Key
    )
    Assert-LifecycleSqlIdentifiers -Bucket $Bucket -Key $Key
    $query = switch ($Statement) {
        "current-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest" }
        "noncurrent-age" { "UPDATE object_versions SET became_noncurrent_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND NOT is_latest" }
        "marker-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest AND kind = 'delete_marker'" }
        "sole-marker-age" { "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - interval '2 days' WHERE bucket = '$Bucket' AND key = '$Key' AND is_latest AND kind = 'delete_marker'" }
        "revision" { "SELECT revision::text FROM bucket_lifecycle_configs WHERE bucket = '$Bucket'" }
    }
    $result = Invoke-Compose -Project $State.Project -Arguments @(
        "exec", "-T", "postgres", "psql", "-X", "-U", "ipfs3", "-d", "ipfs3", "-v", "ON_ERROR_STOP=1", "-A", "-t", "-c", $query
    ) -Label "targeted lifecycle database assertion" -Timeout ([TimeSpan]::FromSeconds(30)) -AllowedExitCodes @(0, 1)
    if ($result.ExitCode -ne 0) {
        $category = Get-LifecycleSqlErrorCategory -StdErr $result.StdErr
        Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=failed"
        Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-error-category=$category"
        throw "Targeted lifecycle database assertion failed"
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "lifecycle-sql-outcome=passed"
    return @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}
```

Validation must occur before any query string exists. Since accepted characters exclude single quote, double quote, backslash, whitespace, and colon, the single-quoted literals cannot terminate or escape; allowed hyphens and slashes remain inert inside the unterminated literal boundary. Do not add a raw SQL parameter, generic executor, alternate statement, escaping function, product/store query, or new accepted character.

- [ ] **Step 79: Prove validator order, exact argv, unsafe rejection, and all runner contracts Docker-free**

Run pure validator fixtures, query/argv source contracts, safe category fixtures, parser/no-run, all five static suites, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle literal-SQL contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after literal SQL correction" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after literal SQL correction" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Literal SQL release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Literal SQL PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Literal SQL multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Literal SQL Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Literal SQL whitespace check failed" }
```

- [ ] **Step 80: After current plan-critic, run exactly one fresh owned AWS-only #25**

This is the sole live round authorized by this revision. Reuse all existing no-pull/offline-build/no-install, inspected-image, fresh-ownership, loopback, logs-first, exact-cleanup, environment-restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #25 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. Initial revision must emit `lifecycle-sql-outcome=passed`, then `revision-row-count=1`, `revision-shape-valid=true`, and `initial-revision-passed`; it must emit no SQL error category. Require the same safe sequence at revision-unchanged. Every later targeted age update must emit only `lifecycle-sql-outcome=passed`.

Any failure branches only from the last fixed SQL/category, revision, or AWS stage receipt and requires generic `execution-failed`, diagnostics attempted, exact down/image/temp/environment, residual container/network/volume/image zero, and `cleanup-errors=0`. Full AWS-only PASS requires all eleven inner/eight outer pairs, all AWS assertions, all SQL passed outcomes, diagnostics `not-required`, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`. No raw SQL value, identifier, stderr/stdout, path, JSON, response, or error text may be retained.

- [ ] **Step 81: Record #25 and stop for a newly reviewed final-normal decision**

Record #25's fixed SQL/revision/AWS receipts, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. If a fixed SQL category recurs, branch only from that category. If SQL/revision succeeds and a later stage fails, branch only from that fixed later receipt. Only complete AWS-only PASS may enter a new plan that reruns the complete nonlive matrix and obtains a fresh plan-critic receipt before final normal.

This revision authorizes no further SQL/product/test semantic correction, final normal, evidence/README/ROADMAP promotion, Task 14, subsequent attempt, or Git write. #25 has no inherited retry. All cleanup/no-pull/no-install/no-push/no-tag gates remain unchanged.

- [ ] **Step 82: Record consumed #25 at noncurrent-marker after the age update**

AWS-only #25 passed every control-plane substage, including both revision reads with `revision-row-count=1` and `revision-shape-valid=true`. Current unversioned, Enabled, and Suspended passed. Noncurrent-content passed. The run then emitted outer `aws-substage=noncurrent-marker`, emitted `lifecycle-sql-outcome=passed` for its noncurrent-age update, and failed before `aws-substage=noncurrent-marker-passed`. It emitted no later outer stage.

Failure diagnostics and exact cleanup passed with residual container/network/volume/image zero and `cleanup-errors=0`. Attempt #25 is consumed and may not be rerun. This receipt proves setup reached the age update but does not distinguish wait convergence from final assertion, so no semantic fix is authorized. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` stays retained, and Task 14 remains closed.

- [ ] **Step 83: Write Docker-free RED contracts for exact NVE substage order**

Modify only `tests/client-smoke.Tests.ps1` first. Extract `Invoke-LifecycleNoncurrentScenario` and require exactly four fixed emissions using only the ValidateSet `$Target` token:

```powershell
$nveSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleNoncurrentScenario"
$expectedNveSubstages = @(
    'aws-nve-$Target-substage=setup-complete',
    'aws-nve-$Target-substage=age-applied',
    'aws-nve-$Target-substage=wait-complete',
    'aws-nve-$Target-substage=final-assert-passed'
)
$actualNveSubstages = @(
    [regex]::Matches(
        $nveSource,
        'Write-LifecycleEvidence -Category "diagnostic" -Value "(?<value>aws-nve-\$Target-substage=[a-z-]+)"'
    ) | ForEach-Object { $_.Groups['value'].Value }
)
Assert-True (($actualNveSubstages -join ',') -ceq ($expectedNveSubstages -join ',')) "Lifecycle NVE fixed substage order changed"
Assert-Contains $nveSource '[ValidateSet("content", "marker")]' "Lifecycle NVE target allowlist changed"
```

Require exact source ordering: second successor PUT, setup-complete, `Invoke-LifecycleSql`, age-applied, `Wait-LifecycleCondition`, wait-complete, the final `Assert-LifecycleNoncurrentState`, final-assert-passed, existing `nve-$Target=passed`. Reject any duplicate, missing, reordered, target other than the ValidateSet token, bucket/key/version/ID/CID/SQL/status/body/error/raw value, output interpolation beyond `$Target`, or change to the wait/final assertion bodies.

Run RED before changing the runner; it must exit `1` only because the four inner NVE receipts are absent:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
$nveSubstageRed = $LASTEXITCODE
if ($nveSubstageRed -ne 1) { throw "Lifecycle NVE substage static RED returned an unexpected exit code" }
```

- [ ] **Step 84: Add fixed NVE milestones around unchanged setup, age, wait, and assertion**

Modify only `scripts/lifecycle-expiration-smoke.ps1`. Replace only `Invoke-LifecycleNoncurrentScenario` with the same operations plus these four emissions:

```powershell
function Invoke-LifecycleNoncurrentScenario {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network, [Parameter(Mandatory)][string]$Endpoint, [Parameter(Mandatory)][ValidateSet("content", "marker")][string]$Target)
    $bucket = New-LifecycleBucketName -RunId ("$($State.RunId)-nve-$Target") -Prefix "ipfs3-lc"
    $key = "nve-$Target.txt"
    $null = Write-LifecycleConfiguration -RunRoot $State.RunRoot -Kind "noncurrent"
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "nve-first.txt"), "nve-first", [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText((Join-Path $State.RunRoot "nve-second.txt"), "nve-second", [Text.UTF8Encoding]::new($false))
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "create-bucket", "--bucket", $bucket) | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-versioning", "--bucket", $bucket, "--versioning-configuration", "Status=Enabled") | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-bucket-lifecycle-configuration", "--bucket", $bucket, "--lifecycle-configuration", "file:///work/noncurrent.json") | Out-Null
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/nve-first.txt") | Out-Null
    if ($Target -eq "marker") {
        Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "delete-object", "--bucket", $bucket, "--key", $key) | Out-Null
    }
    Invoke-Aws -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Arguments @("s3api", "put-object", "--bucket", $bucket, "--key", $key, "--body", "/work/nve-second.txt") | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=setup-complete"
    Invoke-LifecycleSql -State $State -Statement "noncurrent-age" -Bucket $bucket -Key $key | Out-Null
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=age-applied"
    Wait-LifecycleCondition -Name "noncurrent expiration" -Condition {
        try {
            Assert-LifecycleNoncurrentState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Key $key -Target $Target
            return $true
        } catch { return $false }
    }
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=wait-complete"
    Assert-LifecycleNoncurrentState -Network $Network -RunRoot $State.RunRoot -Endpoint $Endpoint -Bucket $bucket -Key $key -Target $Target
    Write-LifecycleEvidence -Category "diagnostic" -Value "aws-nve-$Target-substage=final-assert-passed"
    Write-LifecycleEvidence -Category "assertion" -Value "nve-$Target=passed"
}
```

Do not change setup order, object/versioning operations, lifecycle configuration, SQL statement, wait count/interval, assertion logic, target allowlist, product code, cleanup, or result mapping. `$Target` is the only permitted interpolation in the four fixed receipts.

- [ ] **Step 85: Prove NVE source order and all runner contracts Docker-free**

Run NVE order/static contracts, parser/no-run, all five static suites, and diff-check. Do not run Rust, Docker, AWS, evidence, README, or ROADMAP work:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Lifecycle NVE substage contract failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/lifecycle-expiration-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Lifecycle runner has PowerShell parse errors after NVE substages" }
$noRun = @(& pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($noRun -join "`n") -cne "[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested") { throw "Lifecycle runner no-run contract changed after NVE substages" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "NVE substage release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "NVE substage PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "NVE substage multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "NVE substage Cluster static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "NVE substage whitespace check failed" }
```

- [ ] **Step 86: After current plan-critic, run exactly one fresh owned AWS-only #26**

This is the sole live round authorized by this revision. Reuse all existing no-pull/offline-build/no-install, inspected-image, fresh-ownership, loopback, logs-first, exact-cleanup, environment-restoration, and residual-zero controls. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #26 is consumed on setup. It must run no Rust suite, normal mode, or other diagnostic. For both content and marker, require the ordered fixed NVE milestones as reached. A failure branches only from the last emitted `aws-nve-content-substage=...` or `aws-nve-marker-substage=...` and the absence of its next milestone/outer pass. Preserve all SQL outcome and outer-stage receipts.

Any failure requires generic `execution-failed`, diagnostics attempted, exact down/image/temp/environment, residual container/network/volume/image zero, and `cleanup-errors=0`. Full AWS-only PASS requires all four content and marker milestones, both `nve-<target>=passed`, all eleven control-plane/eight outer pairs, all AWS assertions, SQL passed outcomes, diagnostics `not-required`, exact cleanup, and `DIAGNOSTIC outcome=aws-passed`. No object identity, bucket/key/version/CID/SQL/status/body/error/raw output may be retained.

- [ ] **Step 87: Record #26 and branch only from the last fixed NVE milestone**

Record #26's NVE milestones, SQL/outer receipts, terminal result, diagnostics disposition, and exact cleanup in a new complete plan revision. If marker or content fails, branch only from its last fixed milestone and do not change semantics before a causal RED. If all AWS stages pass, require a new plan with complete nonlive and fresh plan-critic before final normal.

This revision authorizes no product/test semantic fix, final normal, evidence/README/ROADMAP promotion, Task 14, subsequent attempt, or Git write. #26 has no inherited retry. All cleanup/no-pull/no-install/no-push/no-tag gates remain unchanged.

- [ ] **Step 88: Freeze #26 and the completed same-key stale-guard retry correction**

AWS-only #26 passed every control-plane substage, current unversioned/Enabled/Suspended, and NVE content. NVE marker emitted `setup-complete` and `age-applied`, then emitted no `wait-complete` before the 30-minute host timeout. Runner finally/down removed every owned container and project network; independent owned residuals were zero. The unrelated `del2d` cargo remained untouched. Attempt #26 is consumed.

The confirmed product cause is same-key lifecycle action concurrency: the evaluator schedules both noncurrent content and marker targets, workers claim them concurrently, and each admits a standard mutation. The later admission supersedes the earlier guard; the earlier final transaction receives `AppError::StaleContentMutation`. Before correction that action terminal-cancelled while its durable idempotency row prevented the target from being scheduled again, so one eligible target remained forever.

Freeze the bounded correction already present in `src/lifecycle/actions.rs`:

```rust
fn is_temporary_execution_error(error: &AppError) -> bool {
    matches!(error, AppError::StaleContentMutation)
        || matches!(error, AppError::Database(message) if is_temporary_database_contention(message))
}
```

`execute_claimed_lifecycle_action` therefore routes final-transaction guard staleness through `settle_post_admission_failure(..., retry = true)`. Settlement attempts to complete the old guard, treats `StaleContentMutation` as proof that it must not clear the newer guard, then calls `schedule_retry` only when `lock_claim_for_execution` still returns the current claim and `attempts < max_attempts`; exhaustion remains `mark_failed_safe`. Existing target-changed/config-changed, absent-target, and stale-claim cancellation paths remain unchanged.

The deterministic regression `lifecycle_action_execution_retries_a_guard_superseded_by_another_same_key_action` admits two same-key guards, settles the superseded one, asserts the action returns to `pending` with no claim/lease and a future retry time, asserts the newer mutation ID remains installed, then settles that newer guard. Focused `lifecycle::actions` scope passed `14/14`; `src/lifecycle/actions.rs` LSP diagnostics and `cargo fmt --check` passed. No broader runtime claim is inferred from focused proof.

- [ ] **Step 89: After current plan-critic, run one complete nonlive matrix on the fixed candidate**

Run exactly this nonlive sequence once. No Docker, AWS, evidence, README, ROADMAP, staging, or Git write is allowed in this step:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry fmt failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry library matrix failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry signed integration matrix failed" }
cargo test --test postgres_versioning --no-run
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry PostgreSQL versioning compile failed" }
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry PostgreSQL lifecycle compile failed" }
cargo test --test e2e --no-run
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry E2E compile failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry multi-gateway compile failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry lifecycle static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Post-guard-retry diff check failed" }
```

Require every command to pass on the same candidate. Record exact Rust test summaries rather than assuming the historical totals. Any failure stops before #27 with evidence `NOT RUN`, README/ROADMAP unchanged, Task 14 closed, and no inherited rerun.

- [ ] **Step 90: Run exactly one fresh owned AWS-only #27 after complete nonlive GREEN**

Reuse the inspected local images, no-pull/offline-build/no-install preflight, a fresh owned five-service project, loopback ports, direct-child temp root, logs-first finally/down, exact image ownership, environment restoration, and independent residual-zero checks. Invoke only:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -DiagnoseLifecycleAws
```

Attempt #27 is consumed on setup and runs no Rust suite or other diagnostic. Require all control-plane/current/content receipts, then require NVE marker `setup-complete` → `age-applied` → `wait-complete` → `final-assert-passed` → outer `noncurrent-marker-passed`, followed by timed sole-marker and EODM outer PASS. Require `lifecycle-actions=passed`, diagnostics `not-required`, exact down/image/temp/environment cleanup, residual container/network/volume/image zero, `cleanup-errors=0`, and terminal `DIAGNOSTIC outcome=aws-passed`.

If any receipt, assertion, topology, timeout, or cleanup gate fails, stop. Preserve evidence `NOT RUN`, README/ROADMAP unchanged, `.debug-journal.md` retained, Task 14 closed, and no final normal authorization. #27 has no inherited retry.

- [ ] **Step 91: Only after complete #27 PASS, run one fresh final normal full-live #28**

Use a new run ID/project/image/temp root/ports and all unchanged ownership/no-pull/no-install/exact-cleanup controls. Invoke normal mode exactly once:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -Run
```

Attempt #28 is consumed on setup. Require the five bounded Rust commands in locked order—`postgres_versioning`, `postgres_lifecycle`, `e2e`, full `multi_gateway`, and signed `integration lifecycle_expiration_invariants`—with positive executed counts, zero failed tests, and bounded durations. Then require every AWS control/current/NVE/marker receipt from #27, the signed Kubo `/api/v0/pin/rm` zero-request receipt, `lifecycle-actions=passed`, diagnostics `not-required`, exact owned cleanup, all independent residuals zero, `cleanup-errors=0`, and terminal `[RESULT] lifecycle-expiration=PASSED`.

A final-normal failure stops without promotion or retry: evidence stays `NOT RUN`, README/ROADMAP remain unchanged, `.debug-journal.md` remains retained, Task 14 remains closed, and no commit/push/tag is authorized. Never remove or inspect unrelated Docker resources.

- [ ] **Step 92: Record the skipped #28 promotion gate honestly**

Historical final normal #28 failed in `multi_gateway`, so this gate did not run: evidence remained `NOT RUN`, README/ROADMAP remained unchanged, `.debug-journal.md` stayed retained, and Task 14 remained closed. No #28 PASS or #28 evidence promotion is claimed anywhere. The later post-#28 reviewed final normal validation and its exact promotion are recorded only in Steps 97-98.

- [ ] **Step 93: Record the bounded #27 host rerun and full AWS-only PASS**

The first AWS-only #27 host invocation was killed by its 30-minute host bound before NVE began. Because host termination preempted the runner's normal terminal receipt, the exact run/project/image/root identities were independently checked, and only those owned resources were manually removed. Their absence was verified; no broad Docker cleanup or unrelated-resource operation occurred.

Under the explicit continuation authority for the same diagnostic, a fresh owned rerun used a 60-minute host bound and passed every control-plane substage, current unversioned/Enabled/Suspended, NVE content and marker including all four fixed inner milestones, timed sole-marker, EODM, SQL outcomes, lifecycle assertions, diagnostics `not-required`, down/image/temp/environment cleanup, independent container/network/volume/image residual zero, and `cleanup-errors=0`. This successful rerun is the accepted #27 receipt. Both #27 host invocations are consumed and may not be repeated.

- [ ] **Step 94: Record final normal #28 and the test-observer localization sequence**

Final normal #28 passed `postgres_versioning`, `postgres_lifecycle`, and `e2e`, then failed in full `multi_gateway`; the later signed integration and AWS phases did not run. Diagnostics and exact owned cleanup passed with independent residual zero and `cleanup-errors=0`. Evidence remained `NOT RUN`, README/ROADMAP were unchanged, `.debug-journal.md` remained retained, and #28 is consumed.

A fresh full-`multi_gateway` diagnostic did not reproduce the failure. Exact stability then exposed a test observer streaming defect: the observer issued ListObjectVersions before consuming the preceding GET response body, and response-body futures lacked the per-request `http_call` timeout. The test-only correction in `tests/multi_gateway.rs`:

1. consumes the bounded GET body before issuing ListObjectVersions;
2. wraps observer List and terminal GET/List body reads in `http_call`;
3. starts observation only after successor PUT returns and its `200` response/version identity are validated, because the independently running lifecycle worker—not observer concurrency—is the publication/action racer.

The terminal outcome set stays exactly `(predecessor_visible=false, successor_visible=true)` or `(true, true)`; successor visibility/readability remains mandatory. No sleep becomes a correctness oracle, and the 60-second convergence bounds remain. These test-only changes alone remained unstable at `successor-response`, selecting the product admission investigation below rather than weakening the assertion.

- [ ] **Step 95: Freeze non-superseding lifecycle admission, stale retry, clippy cleanup, and 5/5 stability**

The confirmed product cause was lifecycle calling ordinary `admit_content_mutation`, which is intentionally allowed to supersede prior ownership and therefore could supersede an in-flight user PUT. Freeze this exact shared interface in `src/store/import/ownership.rs`:

```rust
pub async fn try_admit_lifecycle_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<Option<StandardMutationGuard>>;
```

Its caller-owned retry loop derives deterministic token `lifecycle:<action-id>:<claim-epoch>`, opens a transaction, takes `lock_bucket_for_ownership`, and locks the exact destination. Same action/same epoch reconstructs and returns the existing guard. A higher epoch may atomically replace only an older positive epoch for the same action. Another lifecycle action, a newer same-action epoch, exact user/import ownership, import prefix ownership, or overlapping standard prefix mutation returns `Ok(None)` without changing any token. Only a conflict-free destination calls `upsert_standard_mutation(...).map(Some)`. `admit_lifecycle_expiration` passes the claimed action ID/epoch and maps `Ok(None)` to `LifecycleAdmissionResult::Temporary`, using existing database-time bounded retry. Ordinary user/import admission remains unchanged and may supersede a lifecycle token. A final stale lifecycle guard is retried within its current claim/attempt budget; settling that stale guard accepts `StaleContentMutation` without clearing the newer user guard. Target/config/identity cancellation remains terminal.

Require these deterministic regressions to remain in `src/lifecycle/actions.rs`:

- `lifecycle_admission_never_supersedes_an_inflight_user_mutation` installs a user PUT guard, receives lifecycle `Temporary`, and proves the exact user mutation ID is unchanged.
- `lifecycle_action_execution_retries_a_guard_superseded_by_another_same_key_action` proves the lifecycle action returns pending with a future retry and the newer user guard remains installed.

The complete lifecycle action scope passed `14/14`; full clippy passed. Record both mechanical clippy-only changes: `src/pinning/worker.rs::reconcile_without_desired` computes `outcome_state` directly from the existing `match completion` expression with identical branch calls/string values, and the `src/s3/ops/versioning.rs` test module removes only redundant `use s3s::dto::*` because `super::*` already supplies those names. Neither change alters runtime or test semantics.

After the ownership fix and observer correction, five sequential exact race stability processes passed `5/5`. Every process emitted complete successor request/response, observer GET/List/status/visibility, terminal evaluation, cleanup, and bucket-delete stages; the stability runner's owned topology/image/root cleanup and independent residual checks were zero with `cleanup-errors=0`. This closes the focused race blocker but does not replace complete nonlive or final normal evidence.

- [ ] **Step 96: Record the Task 7 test-helper correction and complete nonlive PASS**

The first post-fix nonlive pass exposed one test-only Task 7 helper defect in `src/store/pinning/publication/tests.rs::execute_lifecycle_current_expiration`: after direct admission, a `GuardedLifecycleExecutionResult::Stale` was committed without completing that helper-owned guard. The helper now calls `complete_standard_mutation_in_transaction(&txn, &guard, now)` on `Stale` before committing; production code and assertions are unchanged. The complete rerun used this exact matrix:

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Final-candidate fmt failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Final-candidate locked build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Final-candidate library matrix failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate signed integration matrix failed" }
cargo test --test postgres_versioning --no-run
if ($LASTEXITCODE -ne 0) { throw "Final-candidate PostgreSQL versioning compile failed" }
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "Final-candidate PostgreSQL lifecycle compile failed" }
cargo test --test e2e --no-run
if ($LASTEXITCODE -ne 0) { throw "Final-candidate E2E compile failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Final-candidate multi-gateway compile failed" }
cargo clippy --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Final-candidate clippy failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final-candidate lifecycle static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Final-candidate diff check failed" }
```

Every command passed on one unchanged candidate. The full library receipt was `877/877`; signed serial integration was `143/143`; all compile, clippy, static, and diff gates passed. No Docker/live work occurred during this correction/rerun.

- [ ] **Step 97: Record the reviewed post-#28 final normal live PASS**

After Step 96 GREEN, one fresh final normal used inspected local images, no pull, offline build, no install, a unique project/image/direct-child root/ports, logs-first finally/down, exact ownership checks, environment restoration, and independent residual-zero verification. It invoked only normal mode:

```powershell
pwsh -NoProfile -File scripts/lifecycle-expiration-smoke.ps1 -Run
```

The run passed all five Rust commands in locked order: `postgres_versioning`, `postgres_lifecycle` hard-loss/reclaim, `e2e`, full `multi_gateway`, and signed `integration lifecycle_expiration_invariants` with zero Kubo `/api/v0/pin/rm` requests. AWS control plane, current expiration in unversioned/Enabled/Suspended, NVE content and marker, timed sole-marker, and EODM all passed. Diagnostics were `not-required`; exact down/image/temp/environment cleanup and independent container/network/volume/image residual-zero checks passed with `cleanup-errors=0`; terminal result was `[RESULT] lifecycle-expiration=PASSED`.

This run is the reviewed post-#28 final normal validation and is consumed. Historical attempt #28 remains failed; no new numeric attempt label is invented. Evidence therefore records `Final normal lifecycle validation: PASSED`, not a false attempt number. No unrelated Docker resource was inspected, stopped, or removed.

- [ ] **Step 98: Record exact evidence/docs promotion and open Task 14**

Step 92's #28 promotion condition was false and remains permanently skipped. After the post-#28 PASS, `docs/lifecycle-expiration-evidence-2026-08-26.log` contains exactly the current sanitized fields:

```text
LIFECYCLE EXPIRATION REAL CLIENT: PASSED
Completed: 2026-08-31
Expiration spec SHA-256: 0c11f4df0b4f6e81e9834fde45368df5b330e1229dc2cdff4e8d8ab573f35742
Lifecycle program spec SHA-256: fdcfbb22447ea7c7bfae9c549b2722664a2e8df859076e7fd3c2d20b3b0e4574
Package: 0.1.0
Candidate base HEAD: f42515a
Focused lifecycle actions regression: PASSED 14/14
Lifecycle admission priority regressions: PASSED 2/2
Complete nonlive matrix: PASSED 880 library / 143 integration
AWS-only lifecycle diagnostic: PASSED
Final normal lifecycle validation: PASSED
postgres_versioning: PASSED
postgres_lifecycle hard-loss and reclaim: PASSED
e2e: PASSED
multi_gateway: PASSED
signed integration lifecycle_expiration_invariants: PASSED
AWS lifecycle control plane: PASSED
AWS current expiration unversioned enabled suspended: PASSED
AWS noncurrent content and marker expiration: PASSED
AWS timed sole-marker and EODM cleanup: PASSED
Kubo /api/v0/pin/rm requests: ZERO
Owned cleanup and independent residual checks: PASSED
HOSTED lifecycle-expiration: NOT RUN
```

README now contains this exact section after Object versioning; the prior Non-goals line removes only `Lifecycle, ` and retains CORS, MFA Delete, Object Lock, pin reclamation, and replication:

```markdown
## Lifecycle expiration

The [approved expiration design](docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md)
and [sanitized LOCAL evidence](docs/lifecycle-expiration-evidence-2026-08-26.log)
describe the implemented subset. `PutBucketLifecycleConfiguration`,
`GetBucketLifecycleConfiguration`, and `DeleteBucketLifecycle` support strict,
atomic replacement of expiration rules with expected-owner enforcement.

Supported actions are current-version `Expiration` by date or days,
`NoncurrentVersionExpiration` for content and delete markers, and
`ExpiredObjectDeleteMarker`. Eligibility uses database UTC and UTC-midnight
semantics. Durable scan/action leases, claim epochs, final policy
revalidation, and version-aware ownership guards make execution safe across
multiple gateway replicas. Lifecycle deletion retains Kubo pins and never
calls `pin/rm`.

`Transition`, `NoncurrentVersionTransition`, and
`AbortIncompleteMultipartUpload` are not supported; a configuration
containing any unsupported action is rejected as a whole.
```

Only matching evidence/README assertions changed in `tests/client-smoke.Tests.ps1`. Exact hosted `NOT RUN` and exact unchecked `ROADMAP.md:77` remain. These post-document gates all passed:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final promoted lifecycle docs contract failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final promoted release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final promoted PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final promoted multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final promoted Cluster static failed" }
$roadmapLine = (Get-Content -LiteralPath "ROADMAP.md")[76]
if ($roadmapLine -cne "- [ ] Lifecycle rules (expiration, transition)") { throw "ROADMAP lifecycle phase boundary changed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Final promoted docs diff check failed" }
```

Task 14's entry gate is now open. Canonical current-identity Oracle and Reviewer acceptance still precede the single integrated commit boundary. No per-task commit, push, or tag is authorized.

- [ ] **Step 99: Record the final reviewer orphaned-admission blocker and deterministic reclaim fix**

**Reviewer finding:** the prior lifecycle-only admission used a fresh random mutation token. A worker crash after admission but before final execution/settlement left that token installed. After action-lease expiry, a reclaimed claim generated another token, saw the orphan as an exact standard mutation, returned `Temporary`, and could never resume or safely replace its own prior admission. This was a concrete durable-progress blocker despite the previously passing live matrix.

Freeze the corrected shared interface and ownership semantics:

```rust
pub async fn try_admit_lifecycle_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<Option<StandardMutationGuard>>;
```

Reject empty/colon-containing action IDs and nonpositive epochs. Under `lock_bucket_for_ownership`, derive exact token `lifecycle:<action-id>:<claim-epoch>` and apply this fail-closed matrix atomically:

1. Same action and same epoch returns a reconstructed guard for the existing token without changing its generation or ownership.
2. A higher epoch replaces only an older positive-epoch token for that exact action.
3. A different lifecycle action, a newer epoch for the same action, user standard token, exact import owner, import prefix owner, or overlapping standard prefix mutation returns `Ok(None)`/`LifecycleAdmissionResult::Temporary` without changing any token.
4. A conflict-free destination installs the deterministic lifecycle token.
5. Ordinary user mutation may still supersede lifecycle. The stale lifecycle action uses bounded retry, and stale settlement never clears the newer guard.

`execute_claimed_lifecycle_action` passes `claim.action.id` and `claim.claim_epoch` through `admit_lifecycle_expiration`. The deterministic regression `lifecycle_action_reclaims_an_orphaned_post_admission_token_with_a_new_epoch` admits epoch N, simulates a crash by leaving its token installed, expires the lease, reclaims the action at claim epoch N+1, executes it, asserts terminal `succeeded`, asserts the target version is deleted, and asserts the ownership guard is cleared. `lifecycle_admission_never_supersedes_an_inflight_user_mutation` and `lifecycle_action_execution_retries_a_guard_superseded_by_another_same_key_action` remain GREEN, preserving user priority and the newer guard.

Post-fix focused receipts are full library `878/878`, signed serial integration `143/143`, `cargo fmt --check`, full clippy, and `git diff --check` PASS. Evidence changes only its exact nonlive line to `Complete nonlive matrix: PASSED 878 library / 143 integration`; hosted remains `NOT RUN`, and ROADMAP lifecycle remains unchecked. This correction invalidated all prior review receipts. The subsequent fresh live validation is recorded separately in Step 100.

- [ ] **Step 100: Record PostgreSQL clock-test precision and fresh deterministic-token final normal PASS**

Before the fresh live run, `tests/postgres_lifecycle.rs::postgres_lifecycle_migration_and_database_clock_are_engine_owned` was corrected test-only: compare PostgreSQL `clock_timestamp()` against host `before`/`after` with an explicit ±1-second tolerance:

```rust
assert!(
    now >= before - chrono::Duration::seconds(1)
        && now <= after + chrono::Duration::seconds(1),
    "PostgreSQL clock_timestamp() must be current"
);
```

This accommodates host/database timestamp precision and scheduling boundaries only; it does not alter `database_now`, SQL, production time semantics, or lifecycle eligibility. The complete `postgres_lifecycle` target passed `3/3`, including repeated fresh fixtures.

On 2026-08-31, the deterministic action/epoch-token candidate received one fresh complete final normal live PASS. In locked order it passed `postgres_versioning`; `postgres_lifecycle` hard-loss/reclaim; `e2e`; full `multi_gateway`; and signed `lifecycle_expiration_invariants` with zero Kubo `/api/v0/pin/rm` requests. AWS lifecycle control plane, current expiration in unversioned/Enabled/Suspended, NVE content and marker, timed sole-marker, and EODM all passed. Exact owned down/image/temp/environment cleanup passed; independent container/network/volume/image residuals were zero; `cleanup-errors=0`; terminal result was `[RESULT] lifecycle-expiration=PASSED`.

Current nonlive receipts are library `880/880`, integration `143/143`, full clippy, fmt, all five static contracts, and diff GREEN. `docs/lifecycle-expiration-evidence-2026-08-26.log` and README remain byte-for-byte equal to Step 98's exact PASS blocks apart from the verified nonlive count refresh; evidence needs no new attempt number or field. Hosted remains `NOT RUN`; `Transition`, `NoncurrentVersionTransition`, and `AbortIncompleteMultipartUpload` remain unsupported; ROADMAP lifecycle remains unchecked. Task 14 current review is open. This step authorizes no further live execution.

- [ ] **Step 101: Record reviewer follow-up max-attempt reclaim and owned-token terminal cleanup**

**Reviewer finding:** deterministic admission solved ordinary lease reclaim, but a crash after final allowed admission at the maximum configured attempt left a claimed row and its token. Treating every exhausted row as immediately `failed_safe` could not safely clear that orphan because no current claim fenced the terminal transaction.

Freeze the corrected claim distinction in `src/store/lifecycle_action.rs::claim_due_in_transaction`:

```rust
if candidate.attempts >= max_attempts {
    let one_recovery_claim =
        candidate.state == STATE_CLAIMED && candidate.attempts == max_attempts;
    if !one_recovery_claim {
        fail_safe_exhausted(db, &candidate, now).await?;
        continue;
    }
}
if let Some(claim) = claim_candidate(db, candidate, worker_id, now, lease_until).await? {
    claimed.push(claim);
}
```

An expired `claimed` row whose attempts equal the validated worker-specific `max_attempts` is reclaimed once as attempt `max_attempts + 1`, gaining a new claim epoch so execution can atomically replace and settle the older same-action admission token. The production worker threads `ValidatedLifecycleConfig.max_attempts` through `claim_due_with_max_attempts`; the default `claim_due` compatibility wrapper retains the repository maximum of eight only for callers without a worker configuration. An ordinary exhausted `pending` row still transitions directly to `failed_safe`, retains attempts at the configured maximum, and receives no worker claim. A claimed row already at `max_attempts + 1` is terminalized without another claim.

Freeze terminal cleanup through this shared helper:

```rust
pub async fn clear_lifecycle_mutation_if_owned<C: ConnectionTrait>(
    txn: &C,
    bucket_name: &str,
    key: &str,
    action_id: &str,
    claim_epoch: i64,
    now: DateTime<Utc>,
) -> AppResult<bool>;
```

Both pre-admission `fail_safe` and the exhausted branch of `retry_or_fail_safe` now run one transaction in strict lock order: `lock_claim_for_execution` first, then `lock_bucket_for_ownership`, then `clear_lifecycle_mutation_if_owned`, then `mark_failed_safe`. Cleanup accepts only a non-prefix token parsing as `lifecycle:<same-action-id>:<epoch>` with positive epoch ≤ the current claim epoch and clears it with an exact-token compare-and-set. It never clears a user token, another action's token, import ownership, a prefix token, or a newer same-action epoch. A stale claim performs no cleanup or terminal write.

Regression `lifecycle_action_reclaims_an_orphaned_post_admission_token_with_a_new_epoch` now seeds attempts at the configured maximum after admission, expires the claim, verifies reclaim at max+1/epoch+1, executes successfully, deletes the target, and clears the guard. `lifecycle_terminal_failure_clears_only_its_owned_admission_token` proves owned-token terminal failure clears its guard while a newer user guard survives unchanged. `lifecycle_claim_max_attempts_fails_safe_without_another_claim` uses non-default cap two and proves ordinary pending exhaustion is unclaimed `failed_safe`. `lifecycle_claim_final_recovery_crash_clears_only_its_owned_token_without_reclaiming` proves a crashed max+1 claimant is terminalized without max+2 while preserving user, other-action, and newer-epoch tokens. Fresh PostgreSQL 17 runtime evidence passed the corresponding bounded-recovery/token-preservation test `1/1` and the complete `postgres_lifecycle` target `5/5`, with logs-first cleanup and zero project residuals.

Receipts: lifecycle claim scope `8/8`, lifecycle action execution scope `14/14`, all named targeted regressions, full library `880/880`, signed serial integration `143/143`, full clippy, fmt, and diff GREEN. The changed paths are terminal/reclaim logic only; the fresh main-path final normal live receipt from Step 100 remains PASS and is not rerun. Evidence changes only its exact count line to `Complete nonlive matrix: PASSED 880 library / 143 integration`; README remains byte-for-byte exact, hosted remains `NOT RUN`, and ROADMAP lifecycle remains unchecked. Task 14 current re-review is open and all earlier review receipts remain stale.

### Task 14: Conditional phase-A final verification, review, and commit boundary

**Status / entry gate:** **OPEN — STEP 101 REVIEWER FOLLOW-UP FIX COMPLETE; AWAITING CURRENT PLAN-CRITIC RECEIPT BEFORE TASK 14 CURRENT RE-REVIEW.** Main-path final live remains PASS; terminal/reclaim regressions and nonlive are current; exact evidence/README, unchecked ROADMAP lifecycle item, and exact 56-path manifest are present. Earlier Oracle/Reviewer receipts remain stale.

**Files:**
- Verify after full completion only: every allowlisted path and every protected path
- Remove ignored `.debug-journal.md` only after the final PASS/post-document gate opens this task
- Use full-session Git-write delegation only after both current-identity review receipts pass; never push or tag

**Interfaces:**
- Consumes: fresh passing main-path live candidate plus current max-attempt claimed reclaim, exhausted-pending fail-safe, deterministic owned-token terminal cleanup, crash reclaim, user/newer-token preservation, test-only PostgreSQL ±1-second clock assertion, approved hashes, exact local evidence with hosted `NOT RUN`, and orchestrator-owned current re-review.
- Produces: current-identity verification receipts, `receipt=waiting for receipt` until reviews return, at most one delegated integrated commit, and no hosted/push/tag claim.

The final PASS/post-document entry gate is open. After the current plan-critic receipt, execute the six steps below in order on the unchanged final candidate.

- [ ] **Step 1: Run LSP diagnostics and symbol consistency checks after the final gate opens**

Run `lsp_diagnostics` on every changed `.rs` file and require zero errors/warnings, explicitly including `src/lifecycle/actions.rs`, `src/store/lifecycle_action.rs`, `src/store/import/ownership.rs`, `src/pinning/worker.rs`, `src/s3/ops/versioning.rs`, `src/s3/ops/tagging.rs`, `src/s3/ops/object.rs`, `src/s3/ops/multipart.rs`, `tests/postgres_versioning.rs`, `tests/postgres_lifecycle.rs`, and `tests/multi_gateway.rs`. Require lifecycle admission tokens to be exactly `lifecycle:<action-id>:<claim-epoch>`; same epoch resumes, a higher epoch replaces only an older same-action token, and all other lifecycle/user/import/exact/prefix conflicts return Temporary without mutation. Require only an expired `claimed` max-attempt row to receive max+1 reclaim while exhausted `pending` fails safe without claim. Require pre-admission/max-attempt terminal paths to lock claim then bucket and clear only the same action's deterministic token at epoch ≤ current, preserving every user/other-action/newer/prefix/import token. Require user/import admission precedence, stale newer-guard preservation, target/config/absent cancellation, and pinning behavior to remain unchanged. Require `src/s3/ops/versioning.rs` to remove only its redundant test-module wildcard DTO import. Require the multi-gateway test delta to preserve the two terminal outcomes. Require `tests/cluster.Tests.ps1` to differ only in the approved protected hashes. Use `lsp_find_references` for `CanonicalLifecycleConfiguration`, `database_now`, `ClaimedLifecycleScan`, `ClaimedLifecycleAction`, `VersionTargetIdentity`, `GuardedLifecycleExecutionResult`, `try_admit_lifecycle_mutation`, and `clear_lifecycle_mutation_if_owned`; all producers/consumers must match Shared Interfaces. Search production lifecycle code and require no `Utc::now`, `pin_rm`, `pin::rm`, transition execution, raw backend diagnostic, or direct object deletion outside the guarded boundary.

- [ ] **Step 2: Run final full regression once on the unchanged final candidate**

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Final fmt failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "Final build failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Final library tests failed" }
cargo test --test integration -- --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Final integration tests failed" }
cargo test --test postgres_versioning --no-run
if ($LASTEXITCODE -ne 0) { throw "Existing PostgreSQL versioning compile regressed" }
cargo test --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "Lifecycle PostgreSQL compile failed" }
cargo test --test e2e --no-run
if ($LASTEXITCODE -ne 0) { throw "E2E compile failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway compile failed" }
cargo clippy --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Final clippy failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final release static failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final PostgreSQL static failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final multi-gateway static failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final Cluster static failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Final lifecycle evidence/static failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Final diff check failed" }
```

- [ ] **Step 3: Verify approved source identities and parse every PowerShell block/script**

```powershell
$expected = [ordered]@{
    "docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md" = "0c11f4df0b4f6e81e9834fde45368df5b330e1229dc2cdff4e8d8ab573f35742"
    "docs/superpowers/specs/2026-08-26-lifecycle-program-design.md" = "fdcfbb22447ea7c7bfae9c549b2722664a2e8df859076e7fd3c2d20b3b0e4574"
}
foreach ($entry in $expected.GetEnumerator()) {
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $entry.Key).Hash.ToLowerInvariant()
    if ($actual -cne $entry.Value) { throw "Approved spec identity changed: $($entry.Key)" }
}
foreach ($path in @("scripts/lifecycle-expiration-smoke.ps1", "tests/client-smoke.Tests.ps1", "tests/multi-gateway.Tests.ps1")) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors.Count -ne 0) { throw "PowerShell parse failed: $path" }
}
```

- [ ] **Step 4: Enforce the exact final changed-path manifest and unstaged state**

```powershell
$allowed = @(
    "README.md",
    "config.example.toml",
    "docs/lifecycle-expiration-evidence-2026-08-26.log",
    "docs/superpowers/plans/2026-08-26-lifecycle-expiration.md",
    "docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md",
    "docs/superpowers/specs/2026-08-26-lifecycle-program-design.md",
    "scripts/lifecycle-expiration-smoke.ps1",
    "src/config.rs",
    "src/error.rs",
    "src/import/model.rs",
    "src/lib.rs",
    "src/lifecycle/actions.rs",
    "src/lifecycle/config.rs",
    "src/lifecycle/evaluator.rs",
    "src/lifecycle/filter.rs",
    "src/lifecycle/mod.rs",
    "src/lifecycle/model.rs",
    "src/lifecycle/worker.rs",
    "src/main.rs",
    "src/pinning/worker.rs",
    "src/s3/handler.rs",
    "src/s3/ops/bucket.rs",
    "src/s3/ops/lifecycle.rs",
    "src/s3/ops/multipart.rs",
    "src/s3/ops/mod.rs",
    "src/s3/ops/object.rs",
    "src/s3/ops/tagging.rs",
    "src/s3/ops/versioning.rs",
    "src/store/bucket.rs",
    "src/store/database_clock.rs",
    "src/store/entities/bucket_lifecycle_config.rs",
    "src/store/entities/lifecycle_action.rs",
    "src/store/entities/mod.rs",
    "src/store/entities/object_version.rs",
    "src/store/lifecycle_action.rs",
    "src/store/lifecycle_config.rs",
    "src/store/lifecycle_scan.rs",
    "src/store/import/ownership.rs",
    "src/store/migrations/m20260825_000001_object_versioning.rs",
    "src/store/migrations/m20260826_000001_lifecycle_expiration.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "src/store/object_version.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs",
    "tests/client-smoke.Tests.ps1",
    "tests/cluster.Tests.ps1",
    "tests/compose.lifecycle-expiration-validation.yml",
    "tests/e2e.rs",
    "tests/integration.rs",
    "tests/multi-gateway.Tests.ps1",
    "tests/multi_gateway.rs",
    "tests/postgres_lifecycle.rs",
    "tests/postgres_versioning.rs",
    "tests/support/lifecycle.rs",
    "tests/support/mod.rs"
)
if ($allowed.Count -ne 56) { throw "Lifecycle changed-path manifest count changed" }
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate tracked changes" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate untracked changes" }
$actual = @($tracked + $untracked | Where-Object { $_ } | Sort-Object -Unique)
$unexpected = @($actual | Where-Object { $_ -notin $allowed })
$missing = @($allowed | Where-Object { $_ -notin $actual })
if ($unexpected.Count -ne 0) { throw "Unexpected changed paths: $($unexpected -join ', ')" }
if ($missing.Count -ne 0) { throw "Required changed paths absent: $($missing -join ', ')" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Staged changes exist before final review" }
if ($allowed -contains ".debug-journal.md") { throw ".debug-journal.md must never enter the changed-path manifest" }
git ls-files --error-unmatch -- ".debug-journal.md" *> $null
if ($LASTEXITCODE -eq 0) { throw ".debug-journal.md must never be tracked" }
if (Test-Path -LiteralPath ".debug-journal.md") {
    git check-ignore --quiet -- ".debug-journal.md"
    if ($LASTEXITCODE -ne 0) { throw ".debug-journal.md exists but is not ignored" }
    Remove-Item -LiteralPath ".debug-journal.md" -Force -ErrorAction Stop
}
if (Test-Path -LiteralPath ".debug-journal.md") { throw ".debug-journal.md survived Task 14 cleanup" }
```

- [ ] **Step 5: Run whitespace/secret/boundary scans, freeze hashes, and obtain canonical reviews**

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tracked diff whitespace check failed" }
foreach ($path in @(git ls-files --others --exclude-standard)) {
    git -c core.autocrlf=false diff --no-index --check -- NUL $path
    if ($LASTEXITCODE -gt 1) { throw "Untracked whitespace check failed: $path" }
}
$forbidden = @("T" + "BD", "T" + "ODO", "implement" + " later", "fill in" + " details", "similar to" + " Task")
$planText = [IO.File]::ReadAllText("docs/superpowers/plans/2026-08-26-lifecycle-expiration.md")
foreach ($term in $forbidden) {
    if ($planText.Contains($term, [StringComparison]::OrdinalIgnoreCase)) { throw "Plan red-flag term found: $term" }
}
$unsafeMatches = @(rg -n "pin_rm|pin::rm|Utc::now|secret_key|wrapped_key" src/lifecycle src/store/lifecycle_action.rs src/store/lifecycle_config.rs src/store/lifecycle_scan.rs)
if ($LASTEXITCODE -gt 1) { throw "Lifecycle safety scan failed" }
if ($unsafeMatches.Count -ne 0) { throw "Forbidden lifecycle production match: $($unsafeMatches -join '; ')" }
$futureExecutionMatches = @(rg -n "Transition|NoncurrentVersionTransition|AbortIncompleteMultipartUpload|storage_class" src/lifecycle/actions.rs src/lifecycle/evaluator.rs src/lifecycle/model.rs src/lifecycle/worker.rs src/store/lifecycle_action.rs)
if ($LASTEXITCODE -gt 1) { throw "Future-action execution scan failed" }
if ($futureExecutionMatches.Count -ne 0) { throw "Future lifecycle action leaked into execution: $($futureExecutionMatches -join '; ')" }
```

Record HEAD, status, SHA-256 of both specs/program/plan/evidence/Cargo files/every changed path, all command results, the complete final normal-run receipt, and exact hosted boundary `HOSTED lifecycle-expiration: NOT RUN`. The orchestrator then dispatches the configured canonical Oracle acceptance review and canonical Reviewer code review against that exact identity. Both must return passing current-revision receipts; any edit invalidates both and requires affected gates plus both reviews again. Until then report `receipt=waiting for receipt`.

- [ ] **Step 6: Use full-session delegation for one integrated commit after current reviews**

After both current-identity review receipts pass, stage exactly the 56-path manifest under the user's full-session delegation, verify the staged diff, and create one commit:

```powershell
git add -- README.md config.example.toml docs/lifecycle-expiration-evidence-2026-08-26.log docs/superpowers/plans/2026-08-26-lifecycle-expiration.md docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md docs/superpowers/specs/2026-08-26-lifecycle-program-design.md scripts/lifecycle-expiration-smoke.ps1 src/config.rs src/error.rs src/import/model.rs src/lib.rs src/lifecycle/actions.rs src/lifecycle/config.rs src/lifecycle/evaluator.rs src/lifecycle/filter.rs src/lifecycle/mod.rs src/lifecycle/model.rs src/lifecycle/worker.rs src/main.rs src/pinning/worker.rs src/s3/handler.rs src/s3/ops/bucket.rs src/s3/ops/lifecycle.rs src/s3/ops/mod.rs src/s3/ops/multipart.rs src/s3/ops/object.rs src/s3/ops/tagging.rs src/s3/ops/versioning.rs src/store/bucket.rs src/store/database_clock.rs src/store/entities/bucket_lifecycle_config.rs src/store/entities/lifecycle_action.rs src/store/entities/mod.rs src/store/entities/object_version.rs src/store/import/ownership.rs src/store/lifecycle_action.rs src/store/lifecycle_config.rs src/store/lifecycle_scan.rs src/store/migrations/m20260825_000001_object_versioning.rs src/store/migrations/m20260826_000001_lifecycle_expiration.rs src/store/migrations/mod.rs src/store/mod.rs src/store/object_version.rs src/store/pinning/publication.rs src/store/pinning/publication/tests.rs tests/client-smoke.Tests.ps1 tests/cluster.Tests.ps1 tests/compose.lifecycle-expiration-validation.yml tests/e2e.rs tests/integration.rs tests/multi-gateway.Tests.ps1 tests/multi_gateway.rs tests/postgres_lifecycle.rs tests/postgres_versioning.rs tests/support/lifecycle.rs tests/support/mod.rs
if ($LASTEXITCODE -ne 0) { throw "Staging reviewed lifecycle paths failed" }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged lifecycle whitespace check failed" }
git commit -m "feat: add lifecycle expiration" -m "Add canonical lifecycle configuration, database-clock evaluation, guarded expiration workers, and verified SQLite/PostgreSQL client evidence."
if ($LASTEXITCODE -ne 0) { throw "Lifecycle integrated commit failed" }
```

Do not amend, push, or tag. If either review receipt is missing or stale, leave every path unstaged and report the exact commit manifest and proposed message. The final handoff keeps `HOSTED lifecycle-expiration: NOT RUN`; local evidence never authorizes a hosted, push, tag, merge, or release claim.

## Verification Waves

1. Tasks 1-2: migration/clock/canonical/filter unit evidence, including task-local migration-9/10 injection isolation and two consecutive standard-parallel library-suite passes.
2. Task 3: framework-native s3s XML characterization plus atomic API evidence.
3. Tasks 4-6: publication timestamps, stable scans, action claims, eligibility/scheduling.
4. Tasks 7-9: guarded mutations, revalidation/retry, production worker lifecycle.
5. Task 10: acceptance-only signed SQLite cross-feature harness/matrix, with any integration gap repaired before GREEN.
6. Task 11: deterministic in-process PostgreSQL worker abort/reclaim fencing plus endpoint-only signed multi-gateway visibility/race evidence; runtime executes inside Task 13's owned environment.
7. Task 12: Docker-free runner/static/no-run evidence locking five ordered Rust commands and the signed Kubo pin-rm-zero invariant group.
8. Task 13: records all prior history, deterministic admission/crash reclaim, fresh main-path live PASS, and exact promotion. Step 101 records reviewer follow-up max-attempt claimed-once reclaim, exhausted-pending unclaimed fail-safe, claim-then-bucket deterministic owned-token terminal cleanup, claim `8`, actions `14`, library `880/880`, integration `143/143`, and unchanged live/README with exact refreshed evidence and unchecked ROADMAP.
9. Task 14: current re-review entry is open after Step 101. After the current plan-critic receipt it reruns final LSP/regression/statics/56-path-manifest/identity/Oracle/Reviewer and may use the delegated one integrated commit. Push/tag are never authorized.

## Requirement-to-Task Coverage and Self-Review Record

- Dedicated entity/migration, SQLite/PostgreSQL types/checks/indexes, migration-8/9 transaction safety, successor backfill/count verification/down refusal, task-local migration-9/10 injected-failure isolation, and repeated standard-parallel evidence: Task 1.
- Canonical rule/filter/action model, strict validation, future-action full PUT rejection, canonical JSON/GET projection: Task 2.
- Put/Get/Delete, tombstoned revision, replacement atomicity, absent 404, DELETE 204, expected owner, exact s3s fields: Task 3.
- Database-clock publication age, demotion/promotion timestamps, stable cursor/source/target identity, ambiguous legacy exclusion: Task 4.
- Scan/action leases, idempotency, claim epochs, replay, stale completion, bounded retry state: Task 5.
- UTC midnight, current/timed marker/EODM/NVE eligibility, Newer optional-and-both-exceeded, precedence: Task 6.
- Low-level caller-transaction-owned unversioned/Enabled/Suspended ownership/version mutation, action-store isolation, and no-pin-rm: Task 7.
- Non-superseding lifecycle admission under the bucket lock, user/import precedence, claim locking, full execution revalidation, exact content/marker deletion, same-transaction success/cancel, sole marker, stale-guard retry, idempotent success/failure safety/redaction: Task 8.
- Production startup/shutdown/cancellation/drain and database-time worker operation: Task 9.
- Acceptance-only signed API/SQLite action/config replacement, shared CID/SSE/tags/leases/races, with unexpected failures fixed in-task: Task 10.
- PostgreSQL in-process worker-A abort, worker-B epoch takeover/completion, stale-A CAS rejection, and endpoint-only signed cross-replica visibility/publication race: Task 11.
- Isolated local AWS runner, exact five-command order, safe optional-list/exact lifecycle-state assertions, signed Kubo `/api/v0/pin/rm` zero-request proof, no SQL pin-jobs substitute, no pull/install, owned cleanup, and honest initial `NOT RUN` evidence: Task 12.
- Prior history and fresh main-path live PASS; expired `claimed` max-attempt row reclaimed once at max+1 while exhausted ordinary `pending` fails safe without claim; pre-admission `fail_safe` and exhausted `retry_or_fail_safe` lock claim then bucket and clear only deterministic same-action epoch ≤ current; final-attempt crash reclaim succeeds and clears; terminal failure clears owned token but preserves newer user guard; claim `8/8`, action execution `14/14`, targeted regressions, library `880/880`, integration `143/143`, clippy/fmt/diff GREEN; exact count-refreshed evidence, unchanged README/live, hosted `NOT RUN`, ROADMAP unchecked, and Task 14 current re-review open: Task 13.
- Exact manifest, LSP/full regression, canonical Oracle+Reviewer, full-session one-final-commit delegation, hosted `NOT RUN`, and no push/tag remain Task 14 scope; its entry gate opens only after complete final normal-run PASS and Task 13 post-document gates.
- Future lifecycle phases remain excluded throughout.

## Superseded Pre-Reviewer-Fix Final Self-Review Record

Every authoritative expiration-spec requirement still maps to a task; shared interfaces and approved spec identities remain consistent. The plan has 14 tasks and 176 steps. The exact manifest has 56 unique paths and matches every current tracked/untracked product, test, static, README, evidence, spec, and plan path, including `src/store/import/ownership.rs`, `src/pinning/worker.rs`, and `src/s3/ops/versioning.rs`. Steps 59-67 retain accurate inline lifecycle revision diagnostics with no nonexistent helper. The post-fix nonlive record includes the test-only Task 7 helper guard settlement and full `877/877` library plus `143/143` integration PASS. Historical #28 remains failed; the successful run is honestly named the reviewed post-#28 final normal validation, and the exact on-disk evidence contains no invented attempt number. Final live records every required Rust target, zero pin-rm, all control/current/NVE/timed/EODM AWS surfaces, terminal PASSED, exact cleanup, and independent residual zero. README preserves only the supported expiration subset; `Transition`, `NoncurrentVersionTransition`, and `AbortIncompleteMultipartUpload` remain unsupported, hosted remains `NOT RUN`, and ROADMAP line 77 remains unchecked. All five static contracts and diff pass. This record was superseded by the reviewer blocker and Step 99 correction below.

## Current Post-Reviewer-Fix Self-Review Record

Every authoritative expiration-spec requirement still maps to a task; shared interfaces and approved spec identities remain consistent. The plan has 14 tasks and 179 steps. The exact manifest remains 56 unique paths and matches every actual changed/untracked path. Deterministic admission semantics remain locked. Reviewer follow-up is explicit: only expired `claimed` work at the maximum is reclaimed at max+1; ordinary exhausted `pending` fails safe unclaimed. `fail_safe` and exhausted `retry_or_fail_safe` use claim-then-bucket lock order and `clear_lifecycle_mutation_if_owned`, whose exact action/epoch≤current compare-and-set preserves user, import, prefix, other-action, and newer tokens. The final-attempt crash and terminal-owned/newer-user regressions plus claim `8/8` and action `14/14` cover changed terminal paths. Full library is `880/880`, integration is `143/143`, and clippy/fmt/diff are GREEN. The fresh 2026-08-31 main-path final normal remains PASS and is not rerun; Step 98 evidence is byte-exact with `880 library / 143 integration`, README remains exact, hosted remains `NOT RUN`, and ROADMAP line 77 remains unchecked. Future Transition/NCT/Abort phases remain excluded. Task 14 current re-review is open, but prior Oracle/Reviewer receipts are stale; current plan-critic, Task 14 verification, fresh reviews, staging, and integrated commit remain pending. No implementation, test, Docker/live, review, subagent, or Git-write action occurs during this plan revision. Current execution status is **task-14-current-re-review-open-awaiting-current-plan-critic** and plan-review receipt status is **waiting for receipt**.

## Retained Pre-Final Self-Review Record

Self-review result: every authoritative expiration-spec requirement still maps to a task and all attempt history through #28 plus bounded post-#28 diagnostics remains intact; shared interfaces, approved specs, lifecycle scope, 14-task/176-step shape, and 56-path manifest are consistent. Critic blocker coverage remains correct: Steps 59-67 use inline `$rows = @(Invoke-LifecycleSql ...)`, inline `0|1|many` row count and positive-decimal shape diagnostics, with no revision receipt helper. #27 records its first 30-minute host kill, exact manually verified owned cleanup, and fresh 60-minute full AWS PASS. #28 records PG versioning/lifecycle/E2E PASS, multi-gateway failure, skipped later phases, and exact cleanup. The non-reproducing full diagnostic, observer streaming/body-timeout/order correction, remaining successor-response instability, and product root cause are separated without weakening the two terminal outcomes. The shared `try_admit_lifecycle_mutation` signature and bucket-locked exact/import-prefix/standard-prefix conflict semantics are explicit; lifecycle conflicts return Temporary without changing tokens, user mutation may supersede lifecycle, and stale lifecycle guards retry without clearing newer guards. Named user-precedence and stale-retry regressions passed in action scope `14/14`; clippy passed; `src/pinning/worker.rs` has only the mechanical match-expression assignment and `src/s3/ops/versioning.rs` only removes a redundant test wildcard import; exact race stability passed `5/5` with complete stages and zero cleanup residuals. `src/store/import/ownership.rs`, `src/pinning/worker.rs`, and `src/s3/ops/versioning.rs` are present in every final manifest/staging/review boundary, raising the original 53 paths to 56. After current review, Step 96 runs one exact complete nonlive matrix, Step 97 permits one final normal live invocation, and Step 98 promotes evidence/README and opens Task 14 only on PASS while hosted remains `NOT RUN` and ROADMAP line 77 stays unchecked. Any failure has no inherited retry or promotion. No implementation, test, Docker/live, review, subagent, or Git-write action occurs during this plan revision. Current execution status is **awaiting-current-plan-critic-for-complete-nonlive-and-one-final-normal** and plan-review receipt status is **waiting for receipt**.
