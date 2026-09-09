# Lifecycle Abort Incomplete Multipart Upload Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver lifecycle program Phase B as one atomic feature: canonical `AbortIncompleteMultipartUpload` configuration, database-clock MPU scanning, durable claim-fenced abort execution, a shared bucket-locked exact-abort primitive, and SQLite/PostgreSQL 17/multi-gateway/AWS evidence without claiming the unfinished Lifecycle roadmap item.

**Architecture:** Extend the existing lifecycle action outbox with a checked `version`/`multipart_upload` target discriminator while preserving every Phase A version action identity byte-for-byte. The one existing lifecycle worker scans `Current -> Noncurrent -> Multipart`, schedules immutable upload targets, and executes MPU actions in a claim-first, bucket-locked transaction that never takes the standard content-mutation token; explicit abort, UploadPart's final write, and CompleteMultipartUpload use the same bucket-serialization boundary. Local signed tests, fresh PostgreSQL 17 contention tests, and one owned no-pull live parity run gate evidence and the README subset.

**Tech Stack:** Rust 2024 (MSRV 1.92), locked `s3s` 0.14.0, SeaORM/SeaORM Migration 1.1.20, SQLite, PostgreSQL 17, Tokio, chrono, serde/serde_json, SHA-256, Axum/s3s SigV4 routing, wiremock Kubo controls, PowerShell 7, Docker Compose v2, and a cached AWS CLI v2 container.

**Spec:** `docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md` (approved SHA-256 `ebe715085739f0cfc4be7e941b42778e1b850522bcc3e0a7fdcb72ba56f42fe4`) on baseline `91e164d9c91b325f9df059cf90bdf5024f675922`. The locked `s3s` 0.14.0 DTO represents `DaysAfterInitiation` as `i32`, so the public range is `1..=i32::MAX`. The canonical Rust field remains `u32`; canonical semantic validation rejects larger values so PUT, stored JSON, GET projection, scan, and execution share one representable contract.

**Global Constraints:**
- Implement this as one atomic Phase B feature; do not stage or commit after individual tasks.
- `ROADMAP.md:77` remains exactly `- [ ] Lifecycle rules (expiration, transition)` because transition and later lifecycle phases remain unfinished.
- Reuse the existing generic lifecycle scan/action worker and its claim, lease, retry, recovery, and shutdown loops. Do not add an MPU worker, startup call, queue, or worker configuration.
- Existing Phase A version action IDs, idempotency keys, target values, states, claims, due times, errors, and audit history remain byte-identical through migration and Rust refactoring.
- The public abort day range is positive `1..=i32::MAX` under the locked s3s DTO boundary; `0`, negatives, typed values outside that range, and semantically tampered stored JSON are rejected fail-closed.
- Canonical lifecycle JSON keeps schema version `1`; the new optional action uses a serde default and old Phase A JSON needs no rewrite.
- A rule still requires at least one supported action. Mixed abort plus supported expiration retains every action; unsupported transition actions reject the complete document without changing configuration or revision.
- Abort rules allow only empty modern `Filter`, one modern `Prefix`, or legacy top-level `Prefix`. `Tag`, `And`, and either object-size selector reject the complete document only when that rule contains abort; existing expiration-only selectors retain Phase A semantics.
- `create_upload` obtains lifecycle-relevant `created_at` from `database_now`; UploadPart, replacement parts, and retries never reset initiation time.
- Persisted due times and eligibility use database UTC and `next_utc_midnight_after_full_days`; process time is limited to sleeps, cancellation, polling, and timeouts.
- Scan source order is exactly `Current`, `Noncurrent`, `Multipart`; Phase A version ordering and old cursor decoding remain unchanged.
- Multipart order is exactly `(key ASC, created_at ASC, upload_id ASC)` with a strict tuple-after predicate and exact source-specific cursor field validation.
- Lifecycle MPU execution locks/fences the action first, then reads database time, then takes the bucket lock, revalidates configuration and immutable upload identity, performs the exact abort, and claim-fences the terminal write in the same transaction.
- Lifecycle MPU execution never acquires, verifies, clears, replaces, or waits on a standard content-mutation admission token and therefore does not block PUT or CopyObject.
- Explicit abort, lifecycle abort, UploadPart final persistence, CompleteMultipartUpload publication, and bucket deletion serialize through the existing bucket ownership lock.
- Explicit AbortMultipartUpload returns `NoSuchUpload` for an absent upload or bucket/key mismatch; lifecycle observation of an absent exact target is `AlreadySatisfied` and terminal success.
- Abort-wins completion returns `NoSuchUpload` and publishes no object version. Completion-wins lifecycle abort records `AlreadySatisfied` success.
- Abort-wins UploadPart may retain its already-created CID but its final database write returns `NoSuchUpload` and cannot recreate an upload or part row.
- No path calls Kubo `pin_rm`, a pin provider unpin API, or any physical CID reclamation operation.
- Do not add `x-amz-abort-date`, `x-amz-abort-rule-id`, `ListMultipartUploads`, transition, noncurrent transition, storage residency, garbage collection, or pin reclamation.
- Preserve SigV4, SSE-S3, SSE-C, CID-as-ETag, object metadata, CORS behavior, transition/residency code, default deployment profiles, production Compose files, and release workflow behavior.
- Keep `Cargo.toml`, `Cargo.lock`, package version, locked dependencies, Rust edition, and MSRV unchanged. Do not install software, download dependencies, or pull images.
- Live evidence is local, uniquely owned, no-pull, bounded, sanitized, and run exactly once through the Phase B runner after every non-live gate passes.
- `README.md` changes only after that complete live run passes; a failure or incomplete cleanup leaves README unchanged and evidence non-PASS.
- `.debug-journal.md` is permitted only if runtime debugging actually occurs; it must be ignored, temporary, absent from the manifest, and never staged or committed.
- Final acceptance is identity-bound: any edit after Oracle or Reviewer review invalidates both receipts and requires affected verification plus both reviews again.
- This conversation already contains explicit per-task commit authorization and full-session delegation for exactly one final semantic commit after current-identity Oracle and Reviewer PASS receipts. Immediately before any Git write, the orchestrator must verify that authorization and delegation are still present and not revoked in the active conversation; if either is absent, stop after review and report the proposed exact manifest and commit message. Do not amend, push, or tag.

---

## 0. Extrinsic Constraints and Baseline Gate

The extrinsic pass found no paid-service, hosted-environment, accessibility, privacy, compliance, capacity, or new-stack requirement. Safe defaults are existing locked crates, local file-backed SQLite for deterministic signed races, a fresh uniquely owned PostgreSQL 17 schema/container for concurrency, cached images only, path-style S3 requests, and no network pull. Repository metadata resolves `s3s 0.14.0`, whose generated source defines `AbortIncompleteMultipartUpload.days_after_initiation: Option<DaysAfterInitiation>` and `DaysAfterInitiation = i32`; therefore the implementation uses that locked typed boundary instead of inventing a custom XML parser or changing dependencies.

Before product work, verify the exact baseline and approved source identity. The only expected pre-implementation untracked inputs are the approved spec and this plan; the branch being ahead of its remote is irrelevant and must not be changed.

```powershell
$expectedHead = '2a17f8ada4dffb5935bd8de996b9ce8af89dba2e'
$actualHead = (git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $actualHead -cne $expectedHead) { throw "Wrong baseline HEAD: $actualHead" }
$specPath = 'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md'
$specHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $specPath).Hash.ToLowerInvariant()
if ($specHash -cne 'ebe715085739f0cfc4be7e941b42778e1b850522bcc3e0a7fdcb72ba56f42fe4') {
    throw "Approved spec identity changed"
}
$changed = @((git diff --name-only) + (git ls-files --others --exclude-standard) | Where-Object { $_ } | Sort-Object -Unique)
$allowedInputs = @(
    'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md',
    'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md'
)
$unexpected = @($changed | Where-Object { $_ -notin $allowedInputs })
if ($unexpected.Count -ne 0) { throw "Unexpected baseline changes: $($unexpected -join ', ')" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Baseline index is not empty" }
```

## 1. Locked Framework Contract and Deliberate Non-Goals

Use the installed, non-MinIO `s3s` 0.14 DTOs; do not add a raw XML route or serializer:

```rust
pub struct AbortIncompleteMultipartUpload {
    pub days_after_initiation: Option<i32>,
}

pub struct LifecycleRule {
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
    // Existing expiration/filter/id/status/transition fields remain unchanged.
}
```

The existing `put_bucket_lifecycle_configuration`, `get_bucket_lifecycle_configuration`, and `delete_bucket_lifecycle` handlers in `src/s3/ops/lifecycle.rs` remain the authenticated API boundary. `src/s3/ops/lifecycle.rs`, `src/s3/handler.rs`, `src/error.rs`, `Cargo.toml`, and `Cargo.lock` are verification-only paths unless a compile error contradicts the locked contract; such a contradiction stops execution and returns to plan revision rather than widening scope.

Public `AbortMultipartUpload` deliberately differs from AWS directory-bucket idempotency text in the generated DTO documentation: this gateway preserves its approved general-bucket behavior and returns `NoSuchUpload` when the upload is absent. Do not implement `if_match_initiated_time`, abort response headers, or MPU listing in this phase.

## 2. Shared Interfaces

Use these final names and signatures consistently. A compile-forced name or type change requires updating this plan's producers, consumers, tests, and manifest before implementation continues.

```rust
// src/lifecycle/model.rs
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AbortIncompleteMultipartUploadAction {
    pub days_after_initiation: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipartUploadTargetIdentity {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub initiated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LifecycleTargetIdentity {
    Version(VersionTargetIdentity),
    MultipartUpload(MultipartUploadTargetIdentity),
}

#[derive(Clone, Debug)]
pub struct VersionLifecycleCandidate {
    pub target: VersionTargetIdentity,
    pub is_latest: bool,
    pub size: i64,
    pub lifecycle_age_started_at: chrono::DateTime<chrono::Utc>,
    pub became_noncurrent_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug)]
pub enum LifecycleCandidate {
    Version(VersionLifecycleCandidate),
    MultipartUpload(MultipartUploadTargetIdentity),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleActionKind {
    ExpireCurrent,
    ExpireNoncurrent,
    DeleteExpiredMarker,
    AbortIncompleteMultipartUpload,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LifecycleScanSource {
    Current,
    Noncurrent,
    Multipart,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LifecycleScanCursor {
    pub source: LifecycleScanSource,
    pub bucket: String,
    pub key: String,
    pub sequence: Option<i64>,
    pub version_row_id: Option<String>,
    pub multipart_created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub multipart_upload_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct NewLifecycleAction {
    pub idempotency_key: String,
    pub bucket: String,
    pub config_revision: i64,
    pub rule_identity: RuleIdentity,
    pub action_kind: LifecycleActionKind,
    pub target: LifecycleTargetIdentity,
    pub due_at: chrono::DateTime<chrono::Utc>,
}
```

`CanonicalLifecycleRule` gains this serde-defaulted field without changing `schema_version`:

```rust
#[serde(default)]
pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUploadAction>,
```

```rust
// src/store/multipart.rs
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortExactIncompleteUploadResult {
    Applied,
    AlreadySatisfied,
    Stale,
}

pub async fn abort_exact_incomplete_upload_in_transaction<C: sea_orm::ConnectionTrait>(
    txn: &C,
    target: &crate::lifecycle::model::MultipartUploadTargetIdentity,
) -> crate::error::AppResult<AbortExactIncompleteUploadResult>;

pub async fn upsert_part_for_active_upload(
    db: &sea_orm::DatabaseConnection,
    target: &crate::lifecycle::model::MultipartUploadTargetIdentity,
    part_number: i32,
    cid: &str,
    size: i64,
    etag: &str,
) -> crate::error::AppResult<()>;
```

The exact-abort primitive assumes its caller already owns the bucket lock. It performs one conditional delete on all four immutable fields and distinguishes missing upload ID from an existing mismatched row:

```rust
let deleted = multipart_upload::Entity::delete_many()
    .filter(multipart_upload::Column::UploadId.eq(&target.upload_id))
    .filter(multipart_upload::Column::Bucket.eq(&target.bucket))
    .filter(multipart_upload::Column::Key.eq(&target.key))
    .filter(multipart_upload::Column::CreatedAt.eq(target.initiated_at))
    .exec(txn)
    .await?;
if deleted.rows_affected == 1 {
    return Ok(AbortExactIncompleteUploadResult::Applied);
}
let upload_id_exists = multipart_upload::Entity::find_by_id(&target.upload_id)
    .one(txn)
    .await?
    .is_some();
Ok(if upload_id_exists {
    AbortExactIncompleteUploadResult::Stale
} else {
    AbortExactIncompleteUploadResult::AlreadySatisfied
})
```

Completed-publication APIs consume the full immutable target, not a bare upload ID:

```rust
pub async fn publish_standard_completed_upload(
    db: &sea_orm::DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: PublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError>;

pub async fn publish_standard_completed_zip(
    db: &sea_orm::DatabaseConnection,
    upload_target: &MultipartUploadTargetIdentity,
    request: ZipPublicationRequest,
    guard: StandardMutationGuard,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError>;
```

The final action conversion rejects every impossible nullable combination after a database read:

```rust
fn target_from_action(
    action: &crate::store::entities::lifecycle_action::Model,
) -> crate::error::AppResult<LifecycleTargetIdentity>;

fn action_kind_from_db(value: &str) -> crate::error::AppResult<LifecycleActionKind>;

fn persisted_action_kind(kind: LifecycleActionKind) -> &'static str;
```

## 3. File Responsibility Map and Final Manifest

The initial implementation boundary proposed 29 paths after a complete local live PASS. Any path outside that list required the recorded Task 10 revision below before final review.

**Task 10 manifest revision (2026-09-10, orchestrator ruling):** the shipped evidence layout supersedes two planned paths with functionally equivalent ones, and admits two in-flight rulings plus evidence receipts. The final committed manifest is exactly 35 paths:

- `scripts/lifecycle-abort-multipart-smoke.ps1` → superseded by `tests/run-postgres-lifecycle-validation.ps1` (Task 8 runner: no-pull, offline compile+test chain, sanitized evidence) plus `tests/postgres-lifecycle.Tests.ps1` (its static safety contract).
- `docs/lifecycle-abort-multipart-evidence-2026-09-01.log` → superseded by `tests/results/postgres-lifecycle-validation/NOT-RUN.md` (initial honest NOT RUN) + `RUN-2026-09-09.md` (sole owned local PASS receipt) + `AWS-CLI-PARITY-2026-09-10.md` (spec §9 AWS CLI parity receipt, executed 2026-09-10 by the orchestrator). Raw timestamped runner outputs stay untracked via `.gitignore` (machine-local paths, large compiler JSON); only sanitized receipts are committed. `.gitignore` is therefore an additional manifest path.
- `tests/client-smoke.Tests.ps1` remains in the manifest: Task 10's full static matrix exposed its stale README assertion that abort was unsupported, so the contract now pins the promoted Phase B capability and non-goals. The dedicated `tests/postgres-lifecycle.Tests.ps1` separately owns runner safety.
- Admitted in-flight rulings: `src/s3/ops/lifecycle.rs` (Task 2: legacy rejection test now asserts the new legal abort contract) and `src/store/lifecycle_config.rs` (Task 4: scan-cursor constructor polymorphic adaptation — necessary production responsibility).
- Task 9's planned single `-Run` compose/AWS parity invocation was executed as: the orchestrator-run PG17.11 compose-stack validation (RUN-2026-09-09.md, 9+13 tests PASS) plus the 2026-09-10 AWS CLI parity run (AWS-CLI-PARITY-2026-09-10.md). Both are recorded receipts; neither rewrote the other's boundary.

| Path | Responsibility |
|---|---|
| `README.md` | Promote only the verified Phase B lifecycle subset after live PASS; keep ROADMAP incomplete. |
| `docs/lifecycle-abort-multipart-evidence-2026-09-01.log` | Initial sanitized NOT RUN receipt, then the sole owned local PASS receipt. |
| `docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md` | This executable plan. |
| `docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md` | Approved immutable design input included in the final commit manifest. |
| `scripts/lifecycle-abort-multipart-smoke.ps1` | Opt-in no-pull, one-run, owned PostgreSQL/Kubo/two-gateway/AWS parity runner. |
| `src/lifecycle/actions.rs` | Polymorphic action decoding, claim-first MPU branch, exact revalidation, retry/terminal atomicity. |
| `src/lifecycle/config.rs` | Typed DTO conversion, canonical validation/projection, selector restriction, old-JSON validation. |
| `src/lifecycle/evaluator.rs` | Version behavior preservation plus abort due-time/rule proposal scheduling. |
| `src/lifecycle/model.rs` | Canonical abort model, target/candidate/action discriminators, cursor fields. |
| `src/lifecycle/worker.rs` | Mechanical test fixture updates and proof that the one generic worker handles both target kinds. |
| `src/main.rs` | Mechanical test fixture update and exact-one-worker regression only; production startup remains unchanged. |
| `src/s3/ops/multipart.rs` | Explicit abort transaction, exact target propagation, UploadPart final-write guard. |
| `src/store/entities/lifecycle_action.rs` | Nullable version columns, target discriminator, MPU target fields. |
| `src/store/lifecycle_action.rs` | Byte-stable version keys, canonical MPU keys, polymorphic insert/claim recovery safety. |
| `src/store/lifecycle_scan.rs` | Backward-compatible cursor codec and Current/Noncurrent/Multipart page traversal. |
| `src/store/migrations/m20260831_000001_bucket_cors.rs` | Update only the migration-registration regression so CORS remains exactly migration 11 and abort becomes migration 12; preserve CORS DDL/behavior. |
| `src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs` | Twelfth migration, SQLite rebuild, PostgreSQL ALTER, failure injection, safe down. |
| `src/store/migrations/mod.rs` | Migration module registration. |
| `src/store/mod.rs` | Twelfth-and-last migrator registration and ordering test. |
| `src/store/multipart.rs` | Database-clock creation, exact abort primitive, bucket-locked active-upload part persistence. |
| `src/store/pinning/publication.rs` | Full upload target propagation and completion-winner/abort-winner mapping. |
| `src/store/pinning/publication/tests.rs` | Completion rollback, exact target, and no-publication race regressions. |
| `tests/client-smoke.Tests.ps1` | Docker-free runner/evidence/README/ROADMAP/static safety contract. |
| `tests/cluster.Tests.ps1` | Refresh only the protected hash for the intentionally changed multipart operation file. |
| `tests/integration.rs` | Signed SQLite API, execution, exact race seams, and Kubo no-`pin/rm` matrix. |
| `tests/multi-gateway.Tests.ps1` | Lock the new bounded PostgreSQL clock-control and cross-replica MPU race source shape. |
| `tests/multi_gateway.rs` | Signed two-gateway Complete/lifecycle-abort contention and terminal-state proof. |
| `tests/postgres_lifecycle.rs` | Fresh PG17 migration/down, multiworker, multiconnection, crash/retry/race proof. |
| `tests/support/decompress.rs` | Generalize signed multipart helpers to `S3TestEndpoint` and reuse deterministic Kubo block controls. |

**Runtime inputs verified unchanged:** `tests/compose.lifecycle-expiration-validation.yml` is reused with unique project/image/ports because it already provides PostgreSQL 17, Kubo, two gateways, and the one generic lifecycle worker per gateway. Also verify unchanged: `ROADMAP.md`, `Cargo.toml`, `Cargo.lock`, `config.example.toml`, `src/config.rs`, `src/main.rs` production startup, `src/s3/ops/lifecycle.rs`, all CORS production code and all of `m20260831_000001_bucket_cors.rs` except its exact migration-registration assertion, transition/residency code, `docker-compose*.yml`, `.github/workflows/release-validation.yml`, and Kubo/provider code.

## Execution Protocol

- Execute Tasks 1-9 continuously on one worktree and do not stage or commit between tasks.
- For every behavior change, add the named test first, run the exact RED command, and accept RED only when the named missing behavior fails. Compilation failure is valid only when the test intentionally introduces a final shared type or function that does not exist yet.
- Implement the smallest final-shaped code for GREEN; do not add temporary tables, sentinel version identities, delimiter-based action keys, a second queue, or a second worker.
- Task 6 is signed SQLite acceptance: failures expose an integration gap and are fixed within that task without weakening the assertion.
- Task 8 creates and validates the no-run path only. It must not start Docker, a gateway, PostgreSQL, Kubo, or AWS CLI.
- Task 9 is the sole live execution and documentation promotion gate. Invoke `-Run` once; a failure stops promotion and returns the fixed safe failure receipt.
- Task 10 owns the final current-identity reviews and the single authorized commit. No push or tag is permitted.

### Task 1: Polymorphic lifecycle-action migration, entity, and byte-stable version identity

**Files:**
- Create: `src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs`
- Modify: `src/store/migrations/m20260831_000001_bucket_cors.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/entities/lifecycle_action.rs`
- Modify: `src/store/lifecycle_action.rs`
- Test: migration module tests and `src/store/lifecycle_action.rs` unit tests

**Interfaces:**
- Consumes: Phase A `lifecycle_actions`, migration `m20260831_000001_bucket_cors`, the current `CanonicalActionIdempotency` field order, SQLite transaction/rebuild conventions, PostgreSQL transactional DDL, and the existing claim/state checks.
- Produces: twelfth migration, exact target-shape checks, nullable entity fields, one MPU index, safe down migration, and a frozen Phase A key fixture.

**Recommended executor:** `deep`

- [ ] **Step 1: Add causal RED tests for migration shape, rollback, down safety, and identity preservation**

Add exact tests named `lifecycle_abort_migration_rebuilds_sqlite_without_changing_version_actions`, `lifecycle_abort_migration_rejects_hybrid_targets_and_kind_mismatches`, `lifecycle_abort_migration_failure_before_swap_preserves_old_table_rows_and_indexes`, `lifecycle_abort_down_refuses_multipart_or_abort_state_and_restores_phase_a_shape`, and `phase_a_version_idempotency_bytes_are_frozen`. In `m20260831_000001_bucket_cors.rs`, rename the current `bucket_cors_migration_is_eleventh_last_and_preserves_pre_cors_bucket` test to `bucket_cors_migration_remains_eleventh_before_lifecycle_abort_and_preserves_pre_cors_bucket`, retain all of its CORS schema/data assertions, and replace only its registration preamble with this RED assertion:

```rust
let migrations = crate::store::migrator::Migrator::migrations();
assert_eq!(migrations.len(), 12);
assert_eq!(migrations[10].name(), "m20260831_000001_bucket_cors");
assert_eq!(
    migrations[11].name(),
    "m20260901_000001_lifecycle_abort_multipart"
);
```

Seed one Phase A row with non-default claim/audit values and assert all old columns, ID, and key are identical after up. Assert SQLite retains the four old indexes and adds exactly `idx_lifecycle_actions_multipart_target`. Insert invalid version/MPU hybrids and kind/target mismatches and require database rejection. Inject a failure after copy but before drop/rename and assert the old table, row, checks, indexes, and migration marker survive unchanged. Down must reject either an MPU target or abort kind; after removing such state, it must restore the Phase A non-null columns/checks/indexes.

```powershell
cargo test --locked --offline --lib lifecycle_abort_migration_ -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Lifecycle abort migration RED unexpectedly passed" }
cargo test --locked --offline --lib store::migrations::m20260831_000001_bucket_cors::tests::bucket_cors_migration_remains_eleventh_before_lifecycle_abort_and_preserves_pre_cors_bucket -- --exact --nocapture
if ($LASTEXITCODE -eq 0) { throw "CORS migration-position RED unexpectedly passed" }
cargo test --locked --offline --lib phase_a_version_idempotency_bytes_are_frozen -- --exact --nocapture
if ($LASTEXITCODE -eq 0) { throw "Version idempotency freeze RED unexpectedly passed" }
```

- [ ] **Step 2: Implement backend-specific up/down SQL inside one transaction**

The final table shape is:

```sql
target_type TEXT NOT NULL,
target_version_row_id TEXT,
target_public_version_id TEXT,
target_object_id TEXT,
target_sequence BIGINT,
target_upload_id TEXT,
target_upload_created_at TIMESTAMPTZ,
CONSTRAINT ck_lifecycle_actions_action_kind CHECK (
  action_kind IN ('expire_current','expire_noncurrent','delete_expired_marker','abort_incomplete_multipart_upload')
),
CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (
  target_sequence IS NULL OR target_sequence >= 0
),
CONSTRAINT ck_lifecycle_actions_target_shape CHECK (
  (target_type = 'version'
   AND target_version_row_id IS NOT NULL
   AND target_public_version_id IS NOT NULL
   AND target_sequence IS NOT NULL
   AND target_upload_id IS NULL
   AND target_upload_created_at IS NULL)
  OR
  (target_type = 'multipart_upload'
   AND target_version_row_id IS NULL
   AND target_public_version_id IS NULL
   AND target_object_id IS NULL
   AND target_sequence IS NULL
   AND target_upload_id IS NOT NULL
   AND target_upload_created_at IS NOT NULL)
),
CONSTRAINT ck_lifecycle_actions_kind_target CHECK (
  (action_kind IN ('expire_current','expire_noncurrent','delete_expired_marker') AND target_type = 'version')
  OR
  (action_kind = 'abort_incomplete_multipart_upload' AND target_type = 'multipart_upload')
)
```

Use `TIMESTAMP` on SQLite. SQLite creates `lifecycle_actions_abort_rebuild`, copies every old column exactly while setting only `target_type = 'version'` and MPU columns to NULL, verifies counts, injects the task-local failure before swap, drops/renames, and recreates all five indexes. PostgreSQL adds columns, backfills `target_type`, sets it non-null, drops NOT NULL from three version fields plus sequence, replaces named checks, and creates the MPU index transactionally. Neither branch touches configuration, uploads, parts, versions, objects, CIDs, pins, provider state, or Kubo.

Down first rejects this defensive predicate:

```sql
target_type <> 'version'
OR action_kind = 'abort_incomplete_multipart_upload'
OR target_upload_id IS NOT NULL
OR target_upload_created_at IS NOT NULL
```

SQLite then rebuilds the exact Phase A table and four indexes. PostgreSQL down is one transaction and uses this dependency-safe order after the refusal predicate passes:

```sql
DROP INDEX IF EXISTS idx_lifecycle_actions_multipart_target;
ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_shape;
ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_kind_target;
ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind;
ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_target_sequence;
ALTER TABLE lifecycle_actions ALTER COLUMN target_version_row_id SET NOT NULL;
ALTER TABLE lifecycle_actions ALTER COLUMN target_public_version_id SET NOT NULL;
ALTER TABLE lifecycle_actions ALTER COLUMN target_sequence SET NOT NULL;
```

Every `DROP CONSTRAINT` above is deliberately issued without `CASCADE`. Before dropping columns, execute this query and fail down unless it returns zero rows, proving no remaining constraint references a column about to be removed:

```sql
SELECT conname
FROM pg_constraint
WHERE conrelid = 'lifecycle_actions'::regclass
  AND (
    pg_get_constraintdef(oid) LIKE '%target_type%'
    OR pg_get_constraintdef(oid) LIKE '%target_upload_id%'
    OR pg_get_constraintdef(oid) LIKE '%target_upload_created_at%'
  );
```

Only then execute:

```sql
ALTER TABLE lifecycle_actions DROP COLUMN target_type;
ALTER TABLE lifecycle_actions DROP COLUMN target_upload_id;
ALTER TABLE lifecycle_actions DROP COLUMN target_upload_created_at;
ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_action_kind
  CHECK (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker'));
ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_target_sequence
  CHECK (target_sequence >= 0);
```

The down test inventories the restored PostgreSQL table rather than accepting successful DDL alone. Require `target_version_row_id`, `target_public_version_id`, and `target_sequence` to be NOT NULL; `target_object_id` remains nullable; all three new target columns are absent. Compare the complete check-constraint name/predicate inventory against these exact Phase A definitions (the six unaffected checks remain present; the two replaced checks are recreated above):

```sql
CONSTRAINT ck_lifecycle_actions_config_revision CHECK (config_revision > 0),
CONSTRAINT ck_lifecycle_actions_action_kind CHECK (
  action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker')
),
CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (target_sequence >= 0),
CONSTRAINT ck_lifecycle_actions_state CHECK (
  state IN ('pending', 'claimed', 'succeeded', 'cancelled', 'failed_safe')
),
CONSTRAINT ck_lifecycle_actions_attempts CHECK (attempts >= 0),
CONSTRAINT ck_lifecycle_actions_claim_epoch CHECK (claim_epoch >= 0),
CONSTRAINT ck_lifecycle_action_claim CHECK (
  (state = 'claimed' AND lease_until IS NOT NULL AND claimed_by IS NOT NULL)
  OR (state <> 'claimed' AND lease_until IS NULL AND claimed_by IS NULL)
),
CONSTRAINT ck_lifecycle_action_terminal CHECK (
  (state IN ('succeeded', 'cancelled', 'failed_safe') AND finished_at IS NOT NULL)
  OR (state IN ('pending', 'claimed') AND finished_at IS NULL)
)
```

Require exactly the four Phase A indexes and definitions: `idx_lifecycle_actions_due` on `(state, next_attempt_at, due_at, id)`, `idx_lifecycle_actions_reclaim` on `(state, lease_until, id)`, `idx_lifecycle_actions_bucket_revision` on `(bucket, config_revision, id)`, and `idx_lifecycle_actions_target` on `(bucket, object_key, target_version_row_id)`; assert `idx_lifecycle_actions_multipart_target`, `ck_lifecycle_actions_target_shape`, and `ck_lifecycle_actions_kind_target` are absent. Use a task-local injection so parallel tests cannot leak failure mode.

- [ ] **Step 3: Update the entity and register the migration exactly twelfth and last**

The entity fields become:

```rust
pub target_type: String,
pub target_version_row_id: Option<String>,
pub target_public_version_id: Option<String>,
pub target_object_id: Option<String>,
pub target_sequence: Option<i64>,
pub target_upload_id: Option<String>,
pub target_upload_created_at: Option<DateTimeUtc>,
```

Register `m20260901_000001_lifecycle_abort_multipart` after CORS in both migration module lists. Rename the store-order test to `lifecycle_abort_multipart_migration_is_registered_twelfth_and_last` and assert all 12 exact migration names in order.

- [ ] **Step 4: Freeze Phase A serialization while teaching inserts the version database shape**

Retain a dedicated serialization struct with the exact current field names and order:

```rust
#[derive(serde::Serialize)]
struct CanonicalVersionActionIdempotency<'a> {
    bucket: &'a str,
    config_revision: i64,
    rule_identity: &'a str,
    action_kind: &'a str,
    target_version_row_id: &'a str,
    target_public_version_id: &'a str,
    target_object_id: Option<&'a str>,
    target_sequence: i64,
    due_at: String,
}
```

For the fixed fixture, assert the exact canonical JSON and SHA-256:

```text
{"bucket":"bucket","config_revision":7,"rule_identity":"id:expire","action_kind":"expire_current","target_version_row_id":"version-row","target_public_version_id":"00000000-0000-4000-8000-000000000001","target_object_id":"object-id","target_sequence":1,"due_at":"2026-09-01T00:00:00.000000000Z"}
d9c49eddf8319d6726c99c8162ff9b72150f6d57f23ad121be9a263647f55578
```

Existing version inserts set `target_type = "version"`, wrap the three formerly required values in `Some`, and set both MPU fields to `None`. Do not add `target_type` to version canonical bytes.

- [ ] **Step 5: Run Task 1 GREEN and migration whitespace checks**

```powershell
cargo test --locked --offline --lib lifecycle_abort_migration_ -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle abort migration tests failed" }
cargo test --locked --offline --lib phase_a_version_idempotency_bytes_are_frozen -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "Version idempotency freeze failed" }
cargo test --locked --offline --lib store::migrations::m20260831_000001_bucket_cors::tests::bucket_cors_migration_remains_eleventh_before_lifecycle_abort_and_preserves_pre_cors_bucket -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "CORS migration-position regression failed" }
cargo test --locked --offline --lib store::tests::lifecycle_abort_multipart_migration_is_registered_twelfth_and_last -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration order test failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 1 whitespace check failed" }
```

### Task 2: Canonical abort model, positive-day DTO conversion, selector restriction, and stored revalidation

**Files:**
- Modify: `src/lifecycle/model.rs`
- Modify: `src/lifecycle/config.rs`
- Test: inline tests in `src/lifecycle/config.rs`

**Interfaces:**
- Consumes: locked `s3s::dto::AbortIncompleteMultipartUpload { days_after_initiation: Option<i32> }`, schema version 1, current canonical rule validation, selector model, and Phase A whole-document rejection.
- Produces: `AbortIncompleteMultipartUploadAction`, exact DTO/canonical conversion, old JSON compatibility, abort-safe selector validation, and deterministic GET projection.

**Recommended executor:** `complex`

- [ ] **Step 1: Write RED tests for range, action presence, selectors, mixed rules, and old JSON**

Use actual DTO-shaped JSON in tests:

```rust
fn abort_rule(days: i32, selector: serde_json::Value) -> s3s::dto::LifecycleRule {
    let mut value = serde_json::json!({
        "status": "Enabled",
        "abort_incomplete_multipart_upload": { "days_after_initiation": days }
    });
    for (key, selected) in selector.as_object().unwrap() {
        value[key] = selected.clone();
    }
    serde_json::from_value(value).unwrap()
}
```

Add exact tests `abort_days_are_positive_and_s3s_representable`, `abort_is_a_supported_action_and_mixed_actions_round_trip`, `abort_accepts_only_all_or_prefix_selectors`, `abort_rejection_is_rule_local_but_document_atomic`, `phase_a_canonical_json_decodes_with_absent_abort`, and `stored_abort_json_is_semantically_revalidated`. Cases: `1` and `i32::MAX` pass; `0` and negatives fail; stored `2147483648` fails at the locked DTO boundary; absent `days_after_initiation` fails; abort-only passes; no-action still fails; disabled abort stores/GETs but does not schedule; legacy prefix, empty modern filter, and modern prefix pass; Tag, And (including prefix-only And), greater-than, and less-than fail when abort is present. A valid mixed abort+expiration rule preserves both in canonical JSON and GET DTO. Transition fields still reject the entire document.

```powershell
cargo test --locked --offline --lib lifecycle::config::tests::abort_ -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Canonical abort RED unexpectedly passed" }
cargo test --locked --offline --lib lifecycle::config::tests::phase_a_canonical_json_decodes_with_absent_abort -- --exact --nocapture
if ($LASTEXITCODE -eq 0) { throw "Old canonical JSON RED unexpectedly passed" }
```

- [ ] **Step 2: Add the serde-defaulted action and exact semantic validation**

Canonicalization maps a present DTO only when `days_after_initiation` exists and is positive. Convert with `u32::try_from(value)` after the positive check. Canonical validation requires `days <= i32::MAX as u32`, and rule action presence becomes:

```rust
if rule.expiration.is_none()
    && rule.noncurrent_version_expiration.is_none()
    && rule.abort_incomplete_multipart_upload.is_none()
{
    return Err(invalid("lifecycle rule requires a supported action"));
}
```

When abort is present, accept only:

```rust
matches!(
    &rule.selector,
    CanonicalRuleSelector::LegacyPrefix { .. }
        | CanonicalRuleSelector::Modern { filter: CanonicalFilter::All }
        | CanonicalRuleSelector::Modern { filter: CanonicalFilter::Prefix { .. } }
)
```

Keep transition rejection in `reject_future_actions`; remove only the abort rejection. Every `from_canonical_json`, `canonical_json`, and `to_s3_rules` call continues through semantic validation.

- [ ] **Step 3: Project the exact s3s DTO and retain deterministic schema-1 JSON**

Projection is explicit and checked:

```rust
abort_incomplete_multipart_upload: rule
    .abort_incomplete_multipart_upload
    .as_ref()
    .map(|abort| -> AppResult<s3s::dto::AbortIncompleteMultipartUpload> {
        Ok(s3s::dto::AbortIncompleteMultipartUpload {
            days_after_initiation: Some(i32::try_from(abort.days_after_initiation)
                .map_err(|_| invalid("abort days after initiation are out of range"))?),
        })
    })
    .transpose()?,
```

Assert new JSON writes the optional field deterministically, old Phase A JSON without that key decodes to `None`, and no existing rule/tag ordering changes.

- [ ] **Step 4: Run Task 2 GREEN and the full canonical regression scope**

```powershell
cargo test --locked --offline --lib lifecycle::config::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle canonical configuration tests failed" }
cargo test --locked --offline --lib lifecycle::filter::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Existing lifecycle filter tests regressed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 2 whitespace check failed" }
```

### Task 3: Database-clock MPU creation and the shared bucket-locked exact-abort boundary

**Files:**
- Modify: `src/lifecycle/model.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Modify: `tests/support/decompress.rs`
- Modify: `tests/cluster.Tests.ps1` only after the Rust file reaches GREEN
- Test: inline multipart/publication tests

**Interfaces:**
- Consumes: `database_now`, `lock_bucket_for_ownership`, existing multipart FK cascade, standard completion guard, publication transaction/retry code, and `KuboBlockTarget::{Add,Cat,PinAdd}`.
- Produces: `MultipartUploadTargetIdentity`, `AbortExactIncompleteUploadResult`, exact abort primitive, active-upload part upsert, full target completion propagation, and explicit `NoSuchUpload` mapping.

**Recommended executor:** `deep`

- [ ] **Step 1: Write REDs for database time, exact abort outcomes, explicit semantics, and final-write races**

Add tests named `create_upload_uses_database_clock_and_parts_do_not_reset_it`, `abort_exact_upload_applies_cascades_and_distinguishes_missing_from_stale`, `explicit_abort_is_bucket_locked_and_maps_missing_or_mismatch_to_no_such_upload`, `upload_part_after_abort_returns_no_such_upload_without_resurrection`, `completed_publication_after_abort_rolls_back_without_object_version`, and `completed_publication_winner_removes_the_exact_upload`. Use two file-backed SQLite connections where lock order matters. Assert parts cascade, `created_at` stays exact after part replacement, missing explicit abort is `NoSuchUpload`, and no test accepts a generic database error for the UploadPart loser.

```powershell
cargo test --locked --offline --lib create_upload_uses_database_clock_and_parts_do_not_reset_it -- --exact --nocapture
$redClock = $LASTEXITCODE
cargo test --locked --offline --lib abort_exact_upload_applies_cascades_and_distinguishes_missing_from_stale -- --exact --nocapture
$redAbort = $LASTEXITCODE
cargo test --locked --offline --lib upload_part_after_abort_returns_no_such_upload_without_resurrection -- --exact --nocapture
$redPart = $LASTEXITCODE
if ($redClock -eq 0 -or $redAbort -eq 0 -or $redPart -eq 0) { throw "Multipart boundary RED was not causal" }
```

- [ ] **Step 2: Move initiation to database time and implement the exact primitive**

In `create_upload`, call `database_now(db).await?` immediately before constructing the ActiveModel and use that value only for `multipart_upload.created_at`. Part `uploaded_at` may remain an operational timestamp; it never changes upload initiation.

Implement the Shared Interfaces primitive exactly. Do not call it without the bucket lock. Its conditional delete is one statement; the follow-up existence read only classifies zero rows. FK cascade removes parts. It never touches objects, versions, pins, leases, jobs, usage, or Kubo.

- [ ] **Step 3: Make UploadPart final persistence bucket-locked and non-resurrecting**

Build the exact target from the upload row read before Kubo. After add/pin, call `upsert_part_for_active_upload`. That function begins a transaction, acquires `lock_bucket_for_ownership(txn, &target.bucket)`, rereads upload ID under the lock (exclusive row lock on PostgreSQL), compares bucket/key/created_at, then performs the existing upsert. Missing or mismatch returns `AppError::NoSuchUpload(target.upload_id.clone())`. Therefore an abort winner cannot be followed by a part insert, while an UploadPart winner can commit and then be cascade-deleted by abort.

- [ ] **Step 4: Route explicit abort and completed publication through the same target**

Explicit abort begins a transaction using the request bucket, locks that bucket, loads the upload, maps absent or bucket/key mismatch to `NoSuchUpload`, constructs the full target, and calls the primitive. Only `Applied` returns 204; `AlreadySatisfied` or `Stale` maps to `NoSuchUpload`.

Add `upload_target: MultipartUploadTargetIdentity` to `CompletedMultipartArchive` and propagate `&upload_target` through `CompletedUploadFinalizerStore`, standard/nonstandard object and ZIP completion APIs, retry helpers, and `publish_in_transaction`. Publication already locks the bucket before writing. At the existing upload-removal point:

```rust
match crate::store::multipart::abort_exact_incomplete_upload_in_transaction(db, upload_target).await? {
    AbortExactIncompleteUploadResult::Applied => {}
    AbortExactIncompleteUploadResult::AlreadySatisfied | AbortExactIncompleteUploadResult::Stale => {
        return Err(AppError::NoSuchUpload(upload_target.upload_id.clone()));
    }
}
```

The error rolls back object/version/tags/leases/jobs and leaves an already-created root CID retained. Completion-wins commits object publication and upload removal atomically.

- [ ] **Step 5: Generalize signed helpers and update only the required protected hash**

Change `create_multipart`, `send_upload_part_with_headers`, `upload_part`, `complete_multipart`, and `abort_multipart` test helpers to accept `&(impl S3TestEndpoint + ?Sized)` wherever they use only endpoint/bucket. Keep deterministic Kubo block control unchanged.

After Rust GREEN, compute the new exact `src/s3/ops/multipart.rs` SHA-256 and replace only its value in `$protectedHashes` in `tests/cluster.Tests.ps1`. No other protected hash or cluster assertion changes.

```powershell
$multipartHash = (Get-FileHash -Algorithm SHA256 -LiteralPath 'src/s3/ops/multipart.rs').Hash.ToLowerInvariant()
if ($multipartHash.Length -ne 64) { throw "Multipart source hash is invalid" }
pwsh -NoLogo -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster protected-source contract failed" }
```

- [ ] **Step 6: Run Task 3 GREEN**

```powershell
cargo test --locked --offline --lib store::multipart::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Multipart store tests failed" }
cargo test --locked --offline --lib s3::ops::multipart::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Multipart operation tests failed" }
cargo test --locked --offline --lib store::pinning::publication::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Completed publication tests failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 3 whitespace check failed" }
```

### Task 4: Polymorphic candidates, cursor traversal, evaluator, and durable action persistence

**Files:**
- Modify: `src/lifecycle/model.rs`
- Modify: `src/store/lifecycle_scan.rs`
- Modify: `src/lifecycle/evaluator.rs`
- Modify: `src/store/lifecycle_action.rs`
- Modify: `src/lifecycle/actions.rs` for final-shaped decoding/exhaustive compile only
- Modify: `src/lifecycle/worker.rs` for final-shaped test fixtures only
- Modify: `src/main.rs` for final-shaped test fixtures only
- Modify: `tests/postgres_lifecycle.rs` for final-shaped fixtures only
- Test: scanner/evaluator/action-store unit tests

**Interfaces:**
- Consumes: Tasks 1-3 schema/config/target/primitive, Phase A cursor JSON, scan lease transaction, `next_utc_midnight_after_full_days`, rule identity, and existing unique action key.
- Produces: final target/candidate/action discriminators, source-specific cursor codec, three-source pages, abort proposals, canonical MPU keys, and checked persisted shapes.

**Recommended executor:** `deep`

- [ ] **Step 1: Write RED cursor/page tests covering old JSON and all race positions**

Add exact tests `lifecycle_scan_cursor_decodes_phase_a_and_rejects_mixed_multipart_shapes`, `lifecycle_scan_pages_current_noncurrent_multipart_without_duplicates_or_loops`, `multipart_scan_uses_strict_key_created_upload_tuple_order`, and `multipart_cursor_race_defers_before_cursor_insert_until_fresh_cycle`. The old Phase A payload remains valid:

```json
{"source":"Current","bucket":"bucket","key":"key","sequence":1,"version_row_id":"row"}
```

Version sources require `sequence` and `version_row_id` and forbid both multipart fields. Multipart requires `multipart_created_at` and `multipart_upload_id` and forbids both version fields. All sources require matching nonempty bucket/key. Assert exact sequence Current -> Noncurrent -> Multipart with page size 1, bounded page transitions, no duplicate/loop, active uploads only, after-cursor visibility in the same cycle, and before-cursor visibility only on a fresh cycle.

```powershell
cargo test --locked --offline --lib store::lifecycle_scan::tests::lifecycle_scan_cursor_decodes_phase_a_and_rejects_mixed_multipart_shapes -- --exact --nocapture
$redCursor = $LASTEXITCODE
cargo test --locked --offline --lib store::lifecycle_scan::tests::lifecycle_scan_pages_current_noncurrent_multipart_without_duplicates_or_loops -- --exact --nocapture
$redPage = $LASTEXITCODE
if ($redCursor -eq 0 -or $redPage -eq 0) { throw "Polymorphic scan RED was not causal" }
```

- [ ] **Step 2: Implement backward-compatible stored cursor and three-source traversal**

Use one `StoredCursor` whose six position fields are optional with `#[serde(default, skip_serializing_if = "Option::is_none")]`; do not reuse `sequence` for timestamps or version row ID for upload ID. Preserve Phase A tuple logic exactly for version sources. Multipart query uses `multipart_upload::Entity`, bucket filter, strict tuple-after condition, and ascending key/created_at/upload_id order. Generalize page traversal to fill remaining capacity from each subsequent source and set `cycle_complete` only after Multipart is exhausted.

- [ ] **Step 3: Write RED evaluator/action tests for due boundaries, disabled/mixed rules, and stable keys**

Add exact tests `abort_evaluator_uses_initiation_midnight_prefix_and_database_now`, `abort_evaluator_chooses_earliest_due_then_stable_rule_identity`, `abort_scan_replay_inserts_one_polymorphic_action`, `multipart_action_key_is_canonical_and_version_key_is_unchanged`, and `polymorphic_action_insert_rejects_shape_kind_or_supplied_key_mismatch`. Assert no tag/size/version count query is needed for MPU candidates, disabled rules yield no proposal, prefix is byte-exact, due time uses the shared helper, and process time is absent.

The fixed MPU canonical bytes and key are:

```text
{"bucket":"bucket","config_revision":7,"rule_identity":"id:abort","action_kind":"abort_incomplete_multipart_upload","target_upload_id":"upload-1","target_upload_created_at":"2026-09-01T00:00:00.000000000Z","due_at":"2026-09-03T00:00:00.000000000Z"}
b0cc004f498978dab985d957699c30782cc76a2f6a0e2d4fe769fa0a543cd8c5
```

```powershell
cargo test --locked --offline --lib abort_evaluator_ -- --nocapture
$redEvaluator = $LASTEXITCODE
cargo test --locked --offline --lib multipart_action_key_is_canonical_and_version_key_is_unchanged -- --exact --nocapture
$redKey = $LASTEXITCODE
if ($redEvaluator -eq 0 -or $redKey -eq 0) { throw "Evaluator/action RED was not causal" }
```

- [ ] **Step 4: Implement discriminated evaluation and action persistence**

Version candidate matching, tag reads, counts, precedence, and canonical bytes remain unchanged behind the `Version` variant. MPU evaluation considers enabled abort actions only and permits only all/prefix selectors already validated by Task 2. Build one proposal per matching rule that is due, then choose earliest `due_at` and stable rule identity; one scan of one immutable target inserts at most one action and replay conflicts on the unique key.

Use a separate canonical MPU serialization struct with the exact field order frozen above. `insert_idempotent` fills exactly one target shape. `target_from_action` validates discriminator, action kind, nullability, nonempty bucket/key/upload ID, positive revision, and initiation timestamp. Extend `same_action_definition`, `action_matches_expected`, `persisted_action_kind`, and diagnostics to compare/log only public target context; never unwrap optional version fields for an MPU action.

For compilation before Task 5's behavior RED, the MPU dispatch arm fails closed through the existing redacted `internal_dependency` terminal path and performs no target mutation or token operation. Task 5 replaces that arm immediately; do not run or deploy the intermediate tree.

- [ ] **Step 5: Run Task 4 GREEN and prove no Phase A ordering/key regression**

```powershell
cargo test --locked --offline --lib store::lifecycle_scan::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle scan tests failed" }
cargo test --locked --offline --lib lifecycle::evaluator::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle evaluator tests failed" }
cargo test --locked --offline --lib store::lifecycle_action::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lifecycle action store tests failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 4 whitespace check failed" }
```

### Task 5: Claim-fenced MPU worker branch with full revalidation, retry, and terminal atomicity

**Files:**
- Modify: `src/lifecycle/actions.rs`
- Modify: `src/store/lifecycle_action.rs`
- Modify: `src/lifecycle/worker.rs`
- Modify: `src/main.rs` test fixtures to wrap the existing version target in `LifecycleTargetIdentity::Version`; production startup remains byte-for-byte unchanged
- Test: action/worker unit tests

**Interfaces:**
- Consumes: final polymorphic action model, `lock_claim_for_execution`, database clock, configuration row lock, Task 3 abort primitive, existing retry classifier, and generic worker loop.
- Produces: claim-first MPU execution without standard token, exact cancellation/success semantics, rollback-safe terminal writes, and generic worker crash recovery.

**Recommended executor:** `deep`

- [ ] **Step 1: Write REDs for exact transaction order and no standard token**

Add exact tests `multipart_execution_locks_claim_before_policy_and_never_takes_standard_token`, `multipart_execution_applies_and_claim_fences_terminal_success`, `multipart_execution_missing_target_is_success_but_stale_identity_is_cancelled`, and `multipart_execution_revalidates_revision_rule_status_selector_due_and_target`. Use test hooks/counters that fail if configuration, bucket, upload, or standard mutation state is read before a stale claim is rejected. Seed a foreign standard mutation token on the same bucket/key and assert lifecycle abort succeeds without changing it.

```powershell
cargo test --locked --offline --lib lifecycle::actions::tests::multipart_execution_ -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "MPU lifecycle execution RED unexpectedly passed" }
```

- [ ] **Step 2: Make claim locking truly precede the database clock read**

Refactor `lock_claim_for_execution` so it first selects/locks the exact `(id, state='claimed', claimed_by, claim_epoch)` row, then reads `database_now`, then rejects an absent/expired lease. This preserves the public signature and stale-epoch behavior while satisfying the approved action-first order for both target types. Add a regression that a stale claim cannot read policy or mutate target state.

- [ ] **Step 3: Implement the MPU final transaction in the exact approved order**

Dispatch by `LifecycleTargetIdentity`; keep the existing admitted version branch unchanged. MPU execution starts no admission call and runs one transaction:

```rust
let Some(locked_action) = lock_claim_for_execution(txn, &claim).await? else {
    return Ok(());
};
let now = database_now(txn).await?;
if !same_action_definition(&locked_action, &claim.action) {
    return cancel_without_guard_in_transaction(txn, &claim, now, FAILURE_CANCELLED_STALE).await;
}
lock_bucket_for_ownership(txn, &target.bucket).await?;
// Lock active config; require same revision, enabled named rule, abort action,
// all/prefix selector, exact target, recomputed due_at, and now >= due_at.
// Then call abort_exact_incomplete_upload_in_transaction and claim-fence
// succeeded/cancelled in this same transaction.
```

Resolve `rule_id` as exact `id:<value>` or revision-scoped `ordinal:<n>`. Decode canonical JSON through semantic validation. Missing valid target after policy revalidation maps `AlreadySatisfied` and `mark_succeeded`. Present upload with different bucket/key/initiation, rule/config/revision/status/selector mismatch, recomputed due mismatch, or not-yet-due maps `mark_cancelled(..., FAILURE_CANCELLED_STALE)`. `Applied` and primitive `AlreadySatisfied` are success; primitive `Stale` is cancellation. All terminal writes require the current claim epoch.

- [ ] **Step 4: Make retry/fail-safe cleanup target-aware**

Existing bounded transient classification, attempt caps, lease reclaim, and redacted errors apply to both targets. In `fail_safe_exhausted`, `retry_or_fail_safe`, and malformed-action settlement, call `clear_lifecycle_mutation_if_owned` only when `target_type == "version"`; MPU work never owns such a token. Add tests `multipart_terminal_failure_never_clears_foreign_standard_token`, `multipart_temporary_failure_retries_then_succeeds`, `multipart_terminal_write_failure_rolls_back_abort`, `multipart_stale_epoch_cannot_abort_or_terminalize`, and `multipart_final_recovery_claim_is_bounded`.

The terminal-write failure test uses the existing `fail_next_succeeded` hook: deletion and success update share one transaction, so injected failure leaves the upload and parts present; a later retry applies once and succeeds.

- [ ] **Step 5: Prove one generic worker and no startup/config duplication**

Extend worker tests so the existing `start_worker` claims an MPU action, survives abort-after-claim, reclaims an expired lease, and settles it. In `src/main.rs` retain exactly one production `start_worker(` invocation and unchanged lifecycle config validation/shutdown. Add source/static assertions later in Tasks 7-8 for zero identifiers matching a second abort worker, queue, poll interval, or concurrency setting.

- [ ] **Step 6: Run Task 5 GREEN**

```powershell
cargo test --locked --offline --lib lifecycle::actions::tests::multipart_ -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "MPU action execution tests failed" }
cargo test --locked --offline --lib lifecycle::worker::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Generic lifecycle worker tests failed" }
cargo test --locked --offline --lib store::lifecycle_action::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Action claim/retry tests failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 5 whitespace check failed" }
```

### Task 6: Signed SQLite API, exact Complete/UploadPart race seams, and no-pin-removal proof

**Files:**
- Modify: `tests/integration.rs`
- Modify: `tests/support/decompress.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Tasks 1-5 production behavior, `send_sigv4`, generic multipart helpers, file-backed SQLite harnesses, production worker, Kubo block controls, and Kubo request log.
- Produces: signed API/race acceptance with both Complete winners, both UploadPart winners, explicit/lifecycle absence distinction, and direct `/api/v0/pin/rm` zero-request evidence.

**Recommended executor:** `deep`

- [ ] **Step 1: Add the signed management and absence-semantics RED**

Add `lifecycle_abort_multipart_signed_api_and_absence_semantics`. Through signed S3 requests, PUT and GET an abort-only prefix rule, verify exact XML round-trip, create/upload a part, age only `multipart_uploads.created_at` through the test database, scan/claim/execute with the production worker, and assert upload/parts absent with no object version. Replace with invalid zero/Tag/And/size rules and assert `InvalidRequest` plus unchanged canonical JSON and revision. Retry explicit abort on an absent upload and wrong key and require `NoSuchUpload`; separately execute a durable action after explicit abort and require terminal `succeeded` (`AlreadySatisfied`). Assert disabled rule schedules no action.

```powershell
cargo test --locked --offline --test integration lifecycle_abort_multipart_signed_api_and_absence_semantics -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -eq 0) { throw "Signed lifecycle abort API RED unexpectedly passed" }
```

- [ ] **Step 2: Add the exact UploadPart-vs-abort seam with both winners**

Add `lifecycle_abort_multipart_upload_part_race_cannot_resurrect` with two subcases:

1. **Abort wins:** block the signed UploadPart at Kubo `PinAdd` after CID creation but before final database persistence; execute the due lifecycle action; release Kubo; require UploadPart `NoSuchUpload`, no upload/part row, retained pin-add observation, no object version, and no `pin/rm`.
2. **UploadPart wins:** let signed UploadPart persist first, then execute lifecycle abort; require UploadPart 200, lifecycle terminal success, and cascading removal of upload/part without `pin/rm`.

Do not use sleeps as the winner oracle; use `KuboBlockControl::wait_until_blocked` and bounded task completion.

- [ ] **Step 3: Add the exact Complete-vs-abort seam with both winners**

Add `lifecycle_abort_multipart_complete_race_has_both_winners`:

1. **Abort wins:** block Complete's root `Add` after it has read the upload/parts but before final publication, execute lifecycle abort to success, release the add, require Complete `NoSuchUpload`, no current object/version/tags/leases/jobs, and acceptance of retained part/root CIDs without `pin/rm`.
2. **Completion wins:** stop the lifecycle worker immediately after claim with the existing claim gate, complete and publish the object, release/reclaim the lifecycle action, require Complete 200 and readable object/version, lifecycle terminal `succeeded` via `AlreadySatisfied`, and no `pin/rm`.

Both cases assert one terminal action row and no accidental mixed state. Include ordinary and ZIP completion target propagation in lower-level Task 3 tests; the signed race uses ordinary multipart to keep the causal seam focused.

- [ ] **Step 4: Add signed revalidation/retry/content-token invariants**

Add `lifecycle_abort_multipart_revalidation_retry_and_no_pin_rm`. Cover configuration replacement/delete, missing/disabled rule, changed prefix, not-yet-due initiation, stale upload identity, stale claim epoch, transient SQLite contention, terminal-write rollback/retry, and a foreign standard content token. Cancellation leaves upload/parts unchanged; transient retry eventually applies once; foreign token is byte-identical; PUT/Copy admission is not blocked by the pending MPU action.

- [ ] **Step 5: Run the focused group twice, then full signed integration once**

```powershell
foreach ($iteration in 1..2) {
    cargo test --locked --offline --test integration lifecycle_abort_multipart_ -- --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Signed lifecycle abort group failed on iteration $iteration" }
}
cargo test --locked --offline --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Full signed integration regression failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 6 whitespace check failed" }
```

### Task 7: Fresh PostgreSQL 17 migration/multiworker/crash proof and signed multigateway contention

**Files:**
- Modify: `tests/postgres_lifecycle.rs`
- Modify: `tests/multi_gateway.rs`
- Modify: `tests/multi-gateway.Tests.ps1`
- Test: both Rust targets plus the static contract

**Interfaces:**
- Consumes: owned-schema PG fixture, independent connections, generic production worker/test claim gate, existing multi-gateway endpoints, signed multipart helpers, and test-only database initiation-time control.
- Produces: fresh PG17 up/down/check/index evidence, multiworker/claim/crash/retry fencing, serialization contention, and one cross-replica signed Complete/lifecycle-abort terminal outcome.

**Recommended executor:** `deep`

- [ ] **Step 1: Add PostgreSQL migration and down-safety REDs**

Extend `PreLifecycleMigrator` through `m20260831_000001_bucket_cors`, then add exact tests `postgres_lifecycle_abort_migration_preserves_version_identity_and_checks_shapes` and `postgres_lifecycle_abort_down_refuses_abort_state_and_restores_phase_a`. On a fresh owned schema assert `target_upload_created_at` is `timestamp with time zone`, old action values/key are exact, all named checks reject hybrids, both target indexes exist, and rollback/down semantics match Task 1.

```powershell
cargo test --locked --offline --test postgres_lifecycle postgres_lifecycle_abort_migration_ -- --nocapture --test-threads=1
if ($LASTEXITCODE -eq 0) { throw "PostgreSQL migration RED unexpectedly passed" }
```

An unset `IPFS_S3_TEST_POSTGRES_URL` skip is not GREEN. Task 8 compiles these tests; Task 9 runs them against owned `postgres:17` and parses a positive test count.

- [ ] **Step 2: Add PG multiworker, crash, configuration, and retry races**

Add `postgres_lifecycle_abort_multiworker_claim_crash_retry_is_fenced` and `postgres_lifecycle_abort_configuration_and_bucket_lock_races_are_atomic`. Use at least two independent DB connections and two instances of the existing generic worker. Assert concurrent scans create one MPU idempotency row; `FOR UPDATE SKIP LOCKED` gives one current claim; worker A stops after claim, lease expires by database time, worker B reclaims at a higher epoch and succeeds, stale A cannot mutate/terminalize, and retry caps remain bounded. Race configuration replacement/rule disablement, explicit abort, bucket delete, Complete publication, and UploadPart final persistence under the same bucket lock. Inject SQLSTATE `40001`/`40P01`-classified failures at the existing test seam and require bounded retry or terminal safe failure with redacted diagnostics.

- [ ] **Step 3: Add one signed two-gateway MPU race and exact test-only clock control**

Add `multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome`. Create bucket/MPU/part through gateway A, put abort configuration through gateway B, and use an owned `IPFS_S3_MULTI_GATEWAY_DATABASE_URL` connection only to set that exact upload's `created_at = clock_timestamp() - INTERVAL '3 days'`. Race Complete through A against the existing workers through both gateways. Accept exactly two coherent outcomes:

- Complete 200, object readable, upload absent, and the one action terminal `succeeded` as already satisfied.
- Complete `NoSuchUpload`, object/version absent, upload absent, and the one action terminal `succeeded` as applied.

Reject both-published-and-upload-present, missing terminal action, cancellation, failed-safe, duplicate actions, or any `pin/rm` request. Emit only fixed safe stage labels and clean the configuration/object/version/bucket through signed APIs.

- [ ] **Step 4: Update the multi-gateway static contract without weakening existing races**

Change `tests/multi-gateway.Tests.ps1` to expect five Tokio tests and the new exact test name/stages. Replace the blanket database prohibition with a narrow allowance for one helper named `age_exact_multipart_upload_for_lifecycle`; require parameterized upload ID, exact owned environment variable, one bounded connection, the single `UPDATE multipart_uploads ... WHERE upload_id = $1` test-clock statement, and no other DDL/DML. Continue forbidding Docker/process control and worker test controls in endpoint tests. Assert `src/main.rs` has one lifecycle `start_worker` call and no abort-specific worker/config identifiers.

```powershell
pwsh -NoLogo -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway static lifecycle abort contract failed" }
```

- [ ] **Step 5: Compile PG/live targets offline; defer runtime to the owned runner**

```powershell
cargo test --locked --offline --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL lifecycle target did not compile" }
cargo test --locked --offline --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway target did not compile" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 7 whitespace check failed" }
```

### Task 8: Isolated no-pull runner, static safety contract, and honest initial NOT RUN evidence

**Files:**
- Create: `scripts/lifecycle-abort-multipart-smoke.ps1`
- Create: `docs/lifecycle-abort-multipart-evidence-2026-09-01.log`
- Modify: `tests/client-smoke.Tests.ps1`
- Verify unchanged: `tests/compose.lifecycle-expiration-validation.yml`, production Compose files, README, ROADMAP
- Test: PowerShell parser/static/no-run contract only

**Interfaces:**
- Consumes: cached local images/tools, existing isolated lifecycle Compose topology, Tasks 1-7 test targets, path-style AWS CLI, and safe runner patterns already enforced for lifecycle expiration/CORS.
- Produces: one opt-in `-Run` path, fixed no-run/failure/PASS grammar, exact ownership cleanup, initial NOT RUN evidence, and promotion gating.

**Recommended executor:** `deep`

- [ ] **Step 1: Add a causal static-contract RED before creating runner/evidence**

Add a dedicated Phase B section in `tests/client-smoke.Tests.ps1`. It must parse the runner AST and require: one `[switch]$Run`; no execution with no switch; exact baseline/spec/plan checks; unique lowercase hex run ID; anchored project/image/bucket names; direct-child temp root plus `CreateNew` ownership receipt; saved/restored environment; loopback port ownership; cached image inspection; Cargo `--locked --offline`; Docker build `--pull=false --network none`; Compose `--pull never --no-build`; AWS container `--pull=never`; bounded native commands; fixed safe stage allowlist; logs captured only under owned temp; cleanup limited to owned project/image/root; independent zero-residual containers/networks/volumes/image proof; no install/pull/push/tag; no raw secrets/URLs/bucket/key/upload ID/SQL/log/body/header output in evidence; and README promotion forbidden inside the runner.

```powershell
pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Phase B runner static RED unexpectedly passed" }
```

- [ ] **Step 2: Implement a fail-closed no-run path and owned preflight**

Default invocation prints exactly one line and returns before tool/image/port/network/filesystem preflight:

```text
[RESULT] lifecycle-abort-multipart=NOT RUN reason=execution-not-requested
```

`-Run` verifies HEAD/spec/plan identities, clean index, manifest subset, PowerShell/Compose/Docker/Cargo availability, Compose version, and exact cached image IDs without pulling. It reuses `tests/compose.lifecycle-expiration-validation.yml` under a unique project and supplies that file's required environment variables with unique loopback ports/image. Vendor only from the local Cargo cache, build the unique gateway image with no network/pull, and start with `--pull never --no-build`.

- [ ] **Step 3: Lock one full-run order and exact AWS parity**

The runner order is fixed:

1. `cargo fmt --check`.
2. Full library tests.
3. Full signed integration tests.
4. All PowerShell static suites named in Task 10.
5. Inspect cached images, claim resources, offline build, Compose config, start PostgreSQL/Kubo/two gateways/load balancer, assert PostgreSQL 17 and health.
6. Run full `postgres_lifecycle` with owned `IPFS_S3_TEST_POSTGRES_URL`.
7. Run full `multi_gateway` with all existing endpoint variables plus owned database URL.
8. Run AWS CLI lifecycle/MPU parity once.
9. Capture versions/revision/counts/fixed outcomes, tear down logs-first, restore environment, prove zero residual resources, then and only then write PASS evidence.

AWS parity uses `s3api` path-style calls: create bucket; create MPU under `stale/`; upload one part; write an abort-only Days=1 prefix lifecycle JSON file in the owned temp root; PUT/GET and structurally assert it; use parameterized owned PostgreSQL control to age only the exact upload; poll `list-parts` until parsed error code is `NoSuchUpload`; assert no object was published; create/explicitly abort a second MPU and require retry `NoSuchUpload`; reject zero and Tag abort configurations without revision change; delete lifecycle configuration and bucket. Do not call or claim `ListMultipartUploads`, and do not expect `x-amz-abort-date` or `x-amz-abort-rule-id`.

- [ ] **Step 4: Create the exact initial sanitized evidence**

Write LF-only UTF-8 without BOM and fixed safe text:

```text
LIFECYCLE ABORT INCOMPLETE MULTIPART LOCAL EVIDENCE
Date: 2026-09-01
Baseline: 2a17f8ada4dffb5935bd8de996b9ce8af89dba2e
Spec SHA256: ebe715085739f0cfc4be7e941b42778e1b850522bcc3e0a7fdcb72ba56f42fe4
Public days range: 1..=i32::MAX (locked s3s DTO boundary)
Runner: scripts/lifecycle-abort-multipart-smoke.ps1
Compose input: tests/compose.lifecycle-expiration-validation.yml (reused unchanged)
PostgreSQL 17 migration/multiworker/crash/race: NOT RUN
Signed SQLite API/Complete/UploadPart/no-pin-rm: NOT RUN
Two-gateway lifecycle MPU race: NOT RUN
AWS CLI lifecycle MPU parity: NOT RUN
Cleanup residual check: NOT RUN
LOCAL lifecycle-abort-multipart: NOT RUN
HOSTED lifecycle-abort-multipart: NOT RUN
README promotion: NOT RUN
ROADMAP Lifecycle: UNCHECKED
```

No PASS word appears as a result claim in this initial receipt.

- [ ] **Step 5: Parse every script and prove no-run/static GREEN without live work**

```powershell
$scripts = @(
    'scripts/lifecycle-abort-multipart-smoke.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/multi-gateway.Tests.ps1',
    'tests/cluster.Tests.ps1'
)
foreach ($path in $scripts) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors.Count -ne 0) { throw "PowerShell parse failed: $path" }
}
pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client/evidence static contract failed" }
$output = @(& pwsh -NoLogo -NoProfile -File scripts/lifecycle-abort-multipart-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0) { throw "Phase B no-run path failed" }
if (($output -join "`n") -cne '[RESULT] lifecycle-abort-multipart=NOT RUN reason=execution-not-requested') {
    throw "Phase B no-run receipt changed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 8 whitespace check failed" }
```

### Task 9: One owned live parity run, evidence promotion, and README subset only after PASS

**Files:**
- Modify after PASS only: `docs/lifecycle-abort-multipart-evidence-2026-09-01.log`
- Modify after PASS only: `README.md`
- Verify unchanged: every Rust/runtime input after the runtime identity is frozen; `ROADMAP.md`
- Test: the one full runner invocation plus post-promotion static/no-run checks

**Interfaces:**
- Consumes: Tasks 1-8 unchanged runtime candidate, initial NOT RUN evidence, cached images, exact owned runner, and final local runtime identity.
- Produces: one complete local PASS receipt, hosted NOT RUN boundary, accurate README subset, and unchanged ROADMAP.

**Recommended executor:** `deep`

- [ ] **Step 1: Freeze the pre-live runtime identity and rerun every non-live gate**

Compute SHA-256 for every current manifest path except conditional README/PASS evidence content, plus HEAD, Cargo files, reused Compose, and cached image IDs. Save only the canonical identity under the owned temp root; do not write raw dynamic values to evidence. Rerun the Task 10 non-live command matrix before authorizing the single invocation. If any input changes afterward, do not run live until the identity is recomputed and non-live gates pass again.

- [ ] **Step 2: Invoke the full runner exactly once**

```powershell
pwsh -NoLogo -NoProfile -File scripts/lifecycle-abort-multipart-smoke.ps1 -Run
if ($LASTEXITCODE -ne 0) { throw "Owned lifecycle abort multipart parity failed; README promotion is forbidden" }
```

Require exact terminal `[RESULT] lifecycle-abort-multipart=PASSED`, every fixed stage PASS, PostgreSQL major 17, positive parsed Rust test counts, AWS structural assertions, and cleanup residual zero. A timeout, partial output, failed cleanup, or diagnostic-only run is not PASS and cannot be retried silently.

- [ ] **Step 3: Promote the sanitized evidence only from the complete PASS**

Replace the initial NOT RUN local stage lines with fixed PASS outcomes and record: date; baseline; spec hash; plan hash; runtime identity; sanitized Docker/Compose/AWS/Cargo client versions; positive test counts; PostgreSQL 17; migration/up/down/shape PASS; SQLite signed API/races/no-pin-rm PASS; PG multiworker/crash/retry PASS; two-gateway terminal race PASS; AWS management/abort parity PASS; cleanup residual zero; `LOCAL lifecycle-abort-multipart: PASSED`; and `HOSTED lifecycle-abort-multipart: NOT RUN`. Never record resource names, ports, URLs, upload IDs, CIDs, credentials, SQL output, request/response bodies, raw logs, or internal IDs.

- [ ] **Step 4: Update only the accurate README lifecycle subset**

Keep the existing `## Lifecycle expiration` heading. Add links to the Phase B design and sanitized evidence. State that `AbortIncompleteMultipartUpload` supports all-object, modern prefix, and legacy prefix rules; initiation/due time uses database UTC; execution is durable and bucket-locked across gateways; explicit absent abort remains `NoSuchUpload`; lifecycle absent is idempotent success; and CIDs remain retained with no `pin/rm`. Remove abort from the unsupported sentence, leaving `Transition` and `NoncurrentVersionTransition` unsupported. Explicitly state that abort response headers and `ListMultipartUploads` are not implemented. Do not claim full lifecycle support and do not edit ROADMAP.

- [ ] **Step 5: Rerun post-promotion static/no-run checks and prove protected files unchanged**

```powershell
pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Promoted evidence/README static contract failed" }
$output = @(& pwsh -NoLogo -NoProfile -File scripts/lifecycle-abort-multipart-smoke.ps1 2>&1)
if ($LASTEXITCODE -ne 0 -or ($output -join "`n") -cne '[RESULT] lifecycle-abort-multipart=NOT RUN reason=execution-not-requested') {
    throw "Post-promotion no-run path changed"
}
$roadmapLine = (Get-Content -LiteralPath 'ROADMAP.md')[76]
if ($roadmapLine -cne '- [ ] Lifecycle rules (expiration, transition)') { throw "ROADMAP Lifecycle item changed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 9 whitespace check failed" }
```

### Task 10: Final regression, LSP, manifest, identity-bound reviews, and one authorization-rechecked commit

**Files:**
- Verify: all 35 revised manifest paths and every protected path
- Remove only if it exists after runtime debugging: ignored `.debug-journal.md`
- Stage/commit only after current Oracle and Reviewer PASS receipts

**Interfaces:**
- Consumes: complete Task 9 PASS candidate, approved spec/hash, exact manifest, hosted NOT RUN boundary, current review receipts, and the conversation's explicit per-task commit authorization plus full-session delegation, rechecked immediately before a Git write.
- Produces: final verification evidence, canonical review identity, `waiting for receipt` until both reviews pass, one semantic commit only if authorization is still present, and no push/tag; otherwise a reviewed proposed manifest/message with no Git write.

**Recommended executor:** `deep`

- [ ] **Step 1: Run LSP diagnostics and symbol/reference consistency**

Run `lsp_diagnostics` on every changed `.rs` file and require zero errors/warnings. Use LSP references for `LifecycleTargetIdentity`, `MultipartUploadTargetIdentity`, `LifecycleCandidate`, `LifecycleScanCursor`, `NewLifecycleAction`, `abort_exact_incomplete_upload_in_transaction`, `upsert_part_for_active_upload`, `publish_standard_completed_upload`, `lock_claim_for_execution`, and `execute_claimed_lifecycle_action`. Confirm all signatures match Shared Interfaces, every match is exhaustive, and no consumer treats nullable MPU/version fields as the other target shape.

Search production lifecycle/MPU changes and require: one generic lifecycle worker startup; no abort worker/queue/config; no MPU branch call to standard admission/verification/clear; no `Utc::now` for upload initiation/due/eligibility; no `pin_rm`/unpin; no transition/residency execution; no raw backend/secret/body logging; and exact lock order claim -> database now -> bucket -> policy/target -> mutation -> terminal.

- [ ] **Step 2: Run the complete non-live regression matrix once on the unchanged promoted candidate**

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "Final formatting failed" }
cargo build --locked --offline
if ($LASTEXITCODE -ne 0) { throw "Final locked offline build failed" }
cargo test --locked --offline --lib
if ($LASTEXITCODE -ne 0) { throw "Final library tests failed" }
cargo test --locked --offline --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Final signed integration tests failed" }
cargo test --locked --offline --test postgres_lifecycle --no-run
if ($LASTEXITCODE -ne 0) { throw "Final PostgreSQL lifecycle compile failed" }
cargo test --locked --offline --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Final multi-gateway compile failed" }
cargo clippy --locked --offline --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Final clippy failed" }
pwsh -NoLogo -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release static validation failed" }
pwsh -NoLogo -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL static validation failed" }
pwsh -NoLogo -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway static validation failed" }
pwsh -NoLogo -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster static validation failed" }
pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client/evidence static validation failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Final diff whitespace check failed" }
```

- [ ] **Step 3: Verify spec identity, parse every PowerShell file and every PowerShell fence**

```powershell
$specPath = 'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md'
$specHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $specPath).Hash.ToLowerInvariant()
if ($specHash -cne 'ebe715085739f0cfc4be7e941b42778e1b850522bcc3e0a7fdcb72ba56f42fe4') { throw "Spec hash changed" }
$scriptPaths = @(
    'tests/run-postgres-lifecycle-validation.ps1',
    'tests/postgres-lifecycle.Tests.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/cluster.Tests.ps1',
    'tests/multi-gateway.Tests.ps1'
)
foreach ($path in $scriptPaths) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors.Count -ne 0) { throw "PowerShell file parse failed: $path" }
}
$planPath = 'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md'
$planText = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $planPath))
$fences = [regex]::Matches($planText, '(?ms)^```powershell\r?\n(.*?)^```')
if ($fences.Count -eq 0) { throw "Plan contains no PowerShell fences" }
for ($index = 0; $index -lt $fences.Count; $index++) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseInput($fences[$index].Groups[1].Value, [ref]$tokens, [ref]$errors) | Out-Null
    if ($errors.Count -ne 0) { throw "PowerShell fence parse failed at index $index" }
}
```

- [ ] **Step 4: Enforce the exact 35-path revised manifest, protected files, and empty index**

```powershell
$manifest = @(
    '.gitignore',
    'README.md',
    'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md',
    'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md',
    'src/lifecycle/actions.rs',
    'src/lifecycle/config.rs',
    'src/lifecycle/evaluator.rs',
    'src/lifecycle/model.rs',
    'src/lifecycle/worker.rs',
    'src/main.rs',
    'src/s3/ops/lifecycle.rs',
    'src/s3/ops/multipart.rs',
    'src/store/entities/lifecycle_action.rs',
    'src/store/lifecycle_action.rs',
    'src/store/lifecycle_config.rs',
    'src/store/lifecycle_scan.rs',
    'src/store/migrations/m20260831_000001_bucket_cors.rs',
    'src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs',
    'src/store/migrations/mod.rs',
    'src/store/mod.rs',
    'src/store/multipart.rs',
    'src/store/pinning/publication.rs',
    'src/store/pinning/publication/tests.rs',
    'tests/cluster.Tests.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/integration.rs',
    'tests/multi-gateway.Tests.ps1',
    'tests/multi_gateway.rs',
    'tests/postgres-lifecycle.Tests.ps1',
    'tests/postgres_lifecycle.rs',
    'tests/results/postgres-lifecycle-validation/AWS-CLI-PARITY-2026-09-10.md',
    'tests/results/postgres-lifecycle-validation/NOT-RUN.md',
    'tests/results/postgres-lifecycle-validation/RUN-2026-09-09.md',
    'tests/run-postgres-lifecycle-validation.ps1',
    'tests/support/decompress.rs'
) | Sort-Object -Unique
if ($manifest.Count -ne 35) { throw "Final manifest count changed" }
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate tracked changes" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate untracked changes" }
$actual = @($tracked + $untracked | Where-Object { $_ } | Sort-Object -Unique)
$unexpected = @($actual | Where-Object { $_ -notin $manifest })
$missing = @($manifest | Where-Object { $_ -notin $actual })
if ($unexpected.Count -ne 0) { throw "Unexpected changed paths: $($unexpected -join ', ')" }
if ($missing.Count -ne 0) { throw "Required changed paths absent: $($missing -join ', ')" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Index must remain empty before current reviews" }
if ((Get-Content -LiteralPath 'ROADMAP.md')[76] -cne '- [ ] Lifecycle rules (expiration, transition)') { throw "ROADMAP Lifecycle changed" }
foreach ($protected in @('Cargo.toml','Cargo.lock','config.example.toml','tests/compose.lifecycle-expiration-validation.yml')) {
    if ($protected -in $actual) { throw "Protected path changed: $protected" }
}
git ls-files --error-unmatch -- '.debug-journal.md' *> $null
if ($LASTEXITCODE -eq 0) { throw ".debug-journal.md must never be tracked" }
if (Test-Path -LiteralPath '.debug-journal.md') {
    git check-ignore --quiet -- '.debug-journal.md'
    if ($LASTEXITCODE -ne 0) { throw ".debug-journal.md exists but is not ignored" }
    Remove-Item -LiteralPath '.debug-journal.md' -Force -ErrorAction Stop
}
```

- [ ] **Step 5: Run red-flag, boundary, secret, and untracked whitespace scans**

```powershell
$planPath = 'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md'
$planText = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $planPath))
$redFlags = @('T' + 'BD', 'T' + 'ODO', 'implement' + ' later', 'fill in' + ' details', 'similar to' + ' Task')
foreach ($term in $redFlags) {
    if ($planText.Contains($term, [StringComparison]::OrdinalIgnoreCase)) { throw "Plan red-flag marker found: $term" }
}
$forbiddenWorker = @(rg -n 'abort.*worker|worker.*abort|multipart.*worker|worker.*multipart' src/main.rs src/config.rs src/lifecycle/worker.rs)
if ($LASTEXITCODE -gt 1) { throw "Worker duplication scan failed" }
if ($forbiddenWorker.Count -ne 0) { throw "Abort-specific worker surface found: $($forbiddenWorker -join '; ')" }
$forbiddenMutation = @(rg -n 'try_admit_lifecycle_mutation|verify_standard_mutation_guard|clear_lifecycle_mutation_if_owned' src/lifecycle/actions.rs)
if ($LASTEXITCODE -gt 1) { throw "Mutation-token scan failed" }
foreach ($line in $forbiddenMutation) {
    if ($line -match 'multipart|Multipart|AbortIncomplete') { throw "MPU branch uses standard mutation token: $line" }
}
$forbiddenScope = @(rg -n 'ListMultipartUploads|x-amz-abort-date|x-amz-abort-rule-id|Transition|NoncurrentVersionTransition|pin_rm|pin::rm' src/lifecycle src/store/lifecycle_action.rs src/store/lifecycle_scan.rs src/store/multipart.rs src/s3/ops/multipart.rs)
if ($LASTEXITCODE -gt 1) { throw "Scope/safety scan failed" }
foreach ($line in $forbiddenScope) {
    if ($line -notmatch '#\[cfg\(test\)\]|not supported|no pin|assert|test') { throw "Forbidden production scope match: $line" }
}
foreach ($path in @(git ls-files --others --exclude-standard)) {
    git -c core.autocrlf=false diff --no-index --check -- NUL $path
    if ($LASTEXITCODE -gt 1) { throw "Untracked whitespace check failed: $path" }
}
```

- [ ] **Step 6: Freeze the canonical review identity and obtain current Oracle plus Reviewer receipts**

```powershell
$manifest = @(
    '.gitignore',
    'README.md',
    'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md',
    'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md',
    'src/lifecycle/actions.rs',
    'src/lifecycle/config.rs',
    'src/lifecycle/evaluator.rs',
    'src/lifecycle/model.rs',
    'src/lifecycle/worker.rs',
    'src/main.rs',
    'src/s3/ops/lifecycle.rs',
    'src/s3/ops/multipart.rs',
    'src/store/entities/lifecycle_action.rs',
    'src/store/lifecycle_action.rs',
    'src/store/lifecycle_config.rs',
    'src/store/lifecycle_scan.rs',
    'src/store/migrations/m20260831_000001_bucket_cors.rs',
    'src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs',
    'src/store/migrations/mod.rs',
    'src/store/mod.rs',
    'src/store/multipart.rs',
    'src/store/pinning/publication.rs',
    'src/store/pinning/publication/tests.rs',
    'tests/cluster.Tests.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/integration.rs',
    'tests/multi-gateway.Tests.ps1',
    'tests/multi_gateway.rs',
    'tests/postgres-lifecycle.Tests.ps1',
    'tests/postgres_lifecycle.rs',
    'tests/results/postgres-lifecycle-validation/AWS-CLI-PARITY-2026-09-10.md',
    'tests/results/postgres-lifecycle-validation/NOT-RUN.md',
    'tests/results/postgres-lifecycle-validation/RUN-2026-09-09.md',
    'tests/run-postgres-lifecycle-validation.ps1',
    'tests/support/decompress.rs'
) | Sort-Object -Unique
if ($manifest.Count -ne 35) { throw "Final manifest count changed" }
$base = (git rev-parse HEAD).Trim()
if ($base -cne '2a17f8ada4dffb5935bd8de996b9ce8af89dba2e') { throw "Review base changed" }
$tracked = @(git diff --name-only HEAD --)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate tracked changes" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate untracked changes" }
$actual = @($tracked + $untracked | Where-Object { $_ } | Sort-Object -Unique)
if (Compare-Object $manifest $actual) { throw "Final manifest differs from the exact 35-path manifest" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Index must remain empty before current reviews" }

$script = @'
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { lstatSync, readFileSync, readlinkSync } from 'node:fs';

function runGit(...args) {
  return execFileSync('git', args, { encoding: 'buffer', maxBuffer: 1024 ** 3 });
}
const hash = createHash('sha256');
function record(tag, bytes) {
  const body = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
  hash.update(Buffer.from(tag, 'ascii'));
  hash.update(Buffer.from([0]));
  hash.update(Buffer.from(String(body.length), 'ascii'));
  hash.update(Buffer.from([0]));
  hash.update(body);
  hash.update(Buffer.from([0]));
}
function nulFields(bytes) {
  const entries = [];
  let start = 0;
  for (let end = 0; end < bytes.length; end++) {
    if (bytes[end] !== 0) continue;
    if (end === start) throw new Error('Empty NUL field');
    entries.push(bytes.subarray(start, end));
    start = end + 1;
  }
  if (start !== bytes.length) throw new Error('NUL field list is not terminated');
  return entries;
}
record('ocmm-review-artifact-v1', '');
record('head', runGit('rev-parse', 'HEAD'));
record('tracked-diff', runGit('diff', '--binary', '--no-ext-diff', 'HEAD', '--'));
for (const pathBytes of nulFields(runGit('ls-files', '--others', '--exclude-standard', '-z')).sort(Buffer.compare)) {
  const path = pathBytes.toString('utf8');
  const stat = lstatSync(path);
  record('untracked-path', pathBytes);
  if (stat.isSymbolicLink()) {
    record('untracked-type', 'symlink');
    record('untracked-bytes', readlinkSync(path, { encoding: 'buffer' }));
  } else if (stat.isFile()) {
    record('untracked-type', 'file');
    record('untracked-bytes', readFileSync(path));
  } else {
    throw new Error(`Unsupported untracked entry type: ${path}`);
  }
}
process.stdout.write(`sha256:${hash.digest('hex')}\n`);
'@
$output = @(node --input-type=module -e $script)
if ($LASTEXITCODE -ne 0) { throw "Canonical review identity failed" }
$reviewIdentity = ($output -join '').Trim()
if ($reviewIdentity -notmatch '^sha256:[0-9a-f]{64}$') { throw "Review identity is invalid: $reviewIdentity" }
$reviewIdentity
```

The orchestrator sends the exact base, sorted 35-path revised manifest, canonical binary working diff, byte-sorted nonignored untracked manifest with entry types and bytes, review identity, approved spec with its locked `s3s` DTO boundary, and verification receipts to the canonical Oracle acceptance review and canonical Reviewer code review. Both must return PASS for that same identity. A timeout, partial response, old identity, or missing verdict is not a receipt. Any edit invalidates both; rerun affected checks, compute a new identity, and obtain both reviews again. Until both current receipts exist, report `waiting for receipt` and leave the index empty.

- [ ] **Step 7: Recheck conversation authorization, then create the one semantic commit only after both current receipts pass**

The conversation contains full-session execution delegation, but the Commit Guard requires a fresh explicit Git-write authorization after the current reviews. Do not execute `git add` or `git commit` until the user approves the reviewed 35-path manifest and message. If authorization is unavailable or revoked, stop after review and report the proposed manifest and message below.

```powershell
$manifest = @(
    '.gitignore',
    'README.md',
    'docs/superpowers/plans/2026-09-01-lifecycle-abort-multipart.md',
    'docs/superpowers/specs/2026-09-01-lifecycle-abort-multipart-design.md',
    'src/lifecycle/actions.rs',
    'src/lifecycle/config.rs',
    'src/lifecycle/evaluator.rs',
    'src/lifecycle/model.rs',
    'src/lifecycle/worker.rs',
    'src/main.rs',
    'src/s3/ops/lifecycle.rs',
    'src/s3/ops/multipart.rs',
    'src/store/entities/lifecycle_action.rs',
    'src/store/lifecycle_action.rs',
    'src/store/lifecycle_config.rs',
    'src/store/lifecycle_scan.rs',
    'src/store/migrations/m20260831_000001_bucket_cors.rs',
    'src/store/migrations/m20260901_000001_lifecycle_abort_multipart.rs',
    'src/store/migrations/mod.rs',
    'src/store/mod.rs',
    'src/store/multipart.rs',
    'src/store/pinning/publication.rs',
    'src/store/pinning/publication/tests.rs',
    'tests/cluster.Tests.ps1',
    'tests/client-smoke.Tests.ps1',
    'tests/integration.rs',
    'tests/multi-gateway.Tests.ps1',
    'tests/multi_gateway.rs',
    'tests/postgres-lifecycle.Tests.ps1',
    'tests/postgres_lifecycle.rs',
    'tests/results/postgres-lifecycle-validation/AWS-CLI-PARITY-2026-09-10.md',
    'tests/results/postgres-lifecycle-validation/NOT-RUN.md',
    'tests/results/postgres-lifecycle-validation/RUN-2026-09-09.md',
    'tests/run-postgres-lifecycle-validation.ps1',
    'tests/support/decompress.rs'
)
git add -- $manifest
if ($LASTEXITCODE -ne 0) { throw "Staging the reviewed manifest failed" }
$staged = @(git diff --cached --name-only | Sort-Object -Unique)
if (Compare-Object ($manifest | Sort-Object -Unique) $staged) { throw "Staged manifest differs from reviewed manifest" }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged whitespace check failed" }
git commit -m 'feat: add lifecycle multipart abort' -m 'Add durable AbortIncompleteMultipartUpload scheduling, bucket-locked MPU execution, and verified SQLite, PostgreSQL, and local AWS parity.'
if ($LASTEXITCODE -ne 0) { throw "Authorized lifecycle multipart abort commit failed" }
```

Do not amend, push, or tag. If either review is stale or missing, or if the immediately preceding authorization check fails, do not stage or commit; report the proposed message and exact manifest instead.

## Verification Waves

1. Task 1: schema/entity/version-key migration contract on SQLite plus PostgreSQL SQL shape.
2. Task 2: canonical/DTO/stored semantic contract at the approved locked-DTO `i32::MAX` boundary.
3. Task 3: database-clock initiation and one bucket-locked abort boundary for explicit abort, part persistence, and completion.
4. Task 4: backward-compatible cursor, Current/Noncurrent/Multipart pages, evaluator, and polymorphic persistence.
5. Task 5: claim-first no-token execution, revalidation, retries, crash recovery, and one generic worker.
6. Task 6: signed SQLite API plus deterministic UploadPart/Complete race winners and direct no-`pin/rm` proof.
7. Task 7: fresh PG17 migration/multiworker/crash/contention and signed two-gateway race source.
8. Task 8: no-live runner/static/initial NOT RUN contract.
9. Task 9: exactly one owned no-pull live parity, sanitized evidence, and conditional README promotion.
10. Task 10: LSP/full regression/PowerShell parsing/35-path revised manifest/current identity/Oracle+Reviewer/authorization recheck/one conditional commit.

## Requirement-to-Task Self-Review Matrix

| Requirement | Coverage |
|---|---|
| Polymorphic schema, SQLite rebuild rollback, PostgreSQL transactional ALTER/down, exact checks/indexes | Task 1, Task 7 |
| Byte-stable Phase A action identities and canonical MPU key | Task 1 fixed version fixture; Task 4 fixed MPU fixture |
| Canonical schema 1, positive representable days, DTO/GET, old JSON, mixed actions, selector restriction | Task 2; signed atomic replacement in Task 6 |
| Database-clock initiation and no UploadPart reset | Task 3 |
| Current -> Noncurrent -> Multipart, old cursor compatibility, strict multipart tuple/races | Task 4 |
| Abort evaluator, due boundary, prefix/all, disabled rules, idempotent action insertion | Task 4 |
| Shared exact abort primitive and explicit missing/mismatch `NoSuchUpload` | Task 3, signed Task 6 |
| Complete-vs-abort both winners and no accidental publication | Task 3 rollback tests, deterministic signed Task 6, PG/multigateway Task 7 |
| UploadPart-vs-abort both winners, no resurrection, retained CID acceptance | Task 3 primitive, deterministic signed Task 6, PG Task 7 |
| Claim-first action, no standard token, complete revalidation, retry/terminal atomicity | Task 5, signed Task 6, PG Task 7 |
| Missing lifecycle target succeeds while explicit missing remains an S3 error | Task 5 and signed Task 6 |
| One generic worker, no duplication | Task 5 plus static Tasks 7-8 and final Task 10 |
| SQLite signed API/races/no `pin_rm` | Task 6 |
| Fresh PG17 multiworker/multigateway/crash/races | Task 7, executed by Task 9 runner |
| Isolated no-pull runner and honest initial NOT RUN | Task 8 |
| One owned live parity and README promotion only after PASS | Task 9 |
| Abort headers and ListMultipartUploads excluded; transition/residency/CORS/default profiles/Cargo/ROADMAP protected | Global Constraints, Tasks 8-10 |
| `.debug-journal.md` ignored/temp only and never committed | Global Constraints and Task 10 manifest cleanup |
| Full regression/LSP/manifest/spec+plan identity/Oracle+Reviewer/one commit/no push/tag | Task 10 |

## Planner Self-Review Record

- Spec coverage: every approved section is mapped above, including the directly approved `1..=i32::MAX` public range imposed by the locked `s3s` DTO and carried consistently through canonical tests, evidence, and review identity.
- Red-flag scan: no deferred implementation markers or unspecified edge-case steps remain; each task names exact tests, final interfaces, RED/GREEN commands, outcomes, and ownership.
- Type consistency: `MultipartUploadTargetIdentity.initiated_at` maps to `lifecycle_actions.target_upload_created_at`; cursor uses `multipart_created_at`; action persistence uses optional version/MPU columns; completion and abort consume the same full target.
- Race consistency: Complete and explicit/lifecycle abort share the bucket lock; UploadPart locks only its final database persistence after Kubo; abort winner never publishes/resurrects; losing CIDs remain retained; no path unpins.
- Scope consistency: the original 29-path proposal is superseded by the recorded Task 10 ruling; the final manifest is exactly 35 paths, includes both spec and plan, the necessary Task 2/4 integration paths, the promoted README static contract, dedicated runner/static receipts, and `.gitignore` evidence hygiene; it excludes ROADMAP/Cargo/default Compose/config/CORS behavior/transition/residency, and admits README only after local PG17, dual-gateway, and AWS CLI parity PASS.
- Execution status: planning only; no product/test/runtime path was edited, no Docker/live command was run, no file was staged, and no Git write was performed.
- Plan-review receipt status: `waiting for receipt`.

## Task Execution Record (appended by the orchestrator)

- **Baseline ruling (2026-09-09):** the three baseline gates (Task 0 assertion, Task 8 template, Task 10 review base) moved from `91e164d` to `2a17f8a` (the separately authorized RUST_LOG fix commit); the spec hash `ebe71508…42fe4` is unchanged.
- **Tasks 1-7:** executed by fresh implementation subagents with orchestrator verification per task. All named RED→GREEN cycles, module suites, full `--lib` runs (final count 981 passed), and clippy `-D warnings` passed. In-flight rulings recorded above were applied at their owning tasks.
- **Task 8:** delivered as `tests/run-postgres-lifecycle-validation.ps1` + `tests/postgres-lifecycle.Tests.ps1` + initial `NOT-RUN.md`; all static suites PASS. `-SkipTeardown` is runner-level only (Rust fixtures own schema cleanup).
- **Task 9:** the orchestrator stood up the multi-gateway validation compose stack (rebuilt from the current tree after discovering the cached `ghcr.io/hugefiver/ipfs3:latest` was stale), repaired three runner/test-side defects (cargo shim path resolution, `-MultiGatewayDatabaseUrl` default database, pending-gate accepting persisted-or-succeeded rows), and ran the final validation on PostgreSQL 17.11: `postgres_lifecycle` 9 passed / 0 failed, `multi_gateway` 13 passed / 0 failed, including `multi_gateway_lifecycle_abort_multipart_race_has_one_terminal_outcome`. Receipts: `RUN-2026-09-09.md` (with the four-attempt failure history) and the timestamped artifacts under `tests/results/postgres-lifecycle-validation/` (untracked). The zero-`pin/rm` proof for the multi-gateway topology is intentionally covered by the wiremock request-log assertion in `tests/integration.rs` (external Kubo provides no request log) — orchestrator ruling.
- **Task 9 supplement (2026-09-10):** the spec §9 AWS CLI evidence gate was executed by the orchestrator: local gateway build + local `ipfs/kubo:v0.43.0` container + `amazon/aws-cli:latest` (all `--pull=never`). Full sequence passed: bucket create, lifecycle PUT/GET round-trip (prefix rule, `DaysAfterInitiation=1`), MPU create + part upload, initiation aged via direct SQL, production worker executed the durable abort (`succeeded`, upload row removed), post-abort `list-parts`/`abort-multipart-upload` both `NoSuchUpload`, lifecycle delete, bucket delete. Methodology note: a first aging attempt used SQLite `datetime()` (space-separated) text that mismatches the sea-orm `DateTimeUtc` binding format (`+00:00` suffix); the exact four-field DELETE correctly classified it `Stale` and the action terminalized `cancelled/cancelled_stale` — test-tooling error, not a product defect; the fail-closed behavior is spec-conformant. Receipt: `AWS-CLI-PARITY-2026-09-10.md`. Environment fully torn down after the run.
- **Task 10:** final verification and identity-bound reviews use the 35-path manifest revision recorded in section 3; the commit remains gated on fresh explicit user authorization.
