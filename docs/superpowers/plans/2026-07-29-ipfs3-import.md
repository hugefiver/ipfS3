# IPFS3 Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable, SigV4-authenticated `ipfs3-import` S3 extension that imports a CID or an allowlisted HTTPS URL, reports truthful progress, optionally decompresses ZIP content, and cannot overwrite a newer same-path operation.

**Architecture:** Register one composite `GatewayRoute` that delegates exact `ipfs3-import` requests to a focused import route and preserves the existing decompression/standard S3 paths. A separate import coordinator, worker, store, URL downloader, and Kubo progress APIs execute jobs; exact-key generations, prefix claims, and transaction-local publication guards prevent stale publication.

**Tech Stack:** Rust 2024 (MSRV 1.92), Tokio, Axum, s3s 0.14, SeaORM with SQLite/PostgreSQL, reqwest 0.13/rustls, Kubo RPC, quick-xml, wiremock.

**Authoritative spec:** `docs/superpowers/specs/2026-07-29-ipfs3-import-design.md`

**Git constraint:** The user explicitly prohibited commits. Do not run `git add`, `git commit`, `git push`, `git tag`, or any other Git write command. Each task ends with a verification checkpoint instead of a commit.

---

## File responsibility map

**Create:**

- `src/import/mod.rs` — coordinator exports and validated import configuration.
- `src/import/model.rs` — job state, phase, source, progress, failure, and result DTOs.
- `src/import/error.rs` — typed synchronous API, retryable/terminal execution, and stale-ownership errors.
- `src/import/downloader.rs` — strict HTTPS origin/DNS authorization and bounded streaming GET.
- `src/import/pipeline.rs` — CID/URL stage orchestration and progress persistence.
- `src/import/worker.rs` — durable claim, lease, retry, cancellation, cleanup, and shutdown.
- `src/import/decompress.rs` — combined import/decompression orchestration and output claims.
- `src/import/response.rs` — accepted/status/result XML rendering.
- `src/store/import/mod.rs` — import-store module exports.
- `src/store/import/jobs.rs` — submit, idempotency, claim, progress, retry, terminal state.
- `src/store/import/ownership.rs` — exact generations, prefix claims, supersession, publication guard.
- `src/store/import/results.rs` — ordered result rows and cursor pages.
- `src/store/entities/import_job.rs`
- `src/store/entities/import_destination.rs`
- `src/store/entities/import_prefix_claim.rs`
- `src/store/entities/import_job_target.rs`
- `src/store/entities/import_job_result.rs`
- `src/store/migrations/m20260729_000001_ipfs3_import.rs`
- `src/kubo/routing.rs` — streamed `/routing/findprovs` events.
- `src/s3/route/gateway.rs` — sole composite `S3Route` registered with s3s.
- `src/s3/route/import_object.rs` — import route matching, auth, parsing, and response.
- `tests/support/import.rs` — signed real-TCP import and source/Kubo harnesses.
- `tests/postgres_import.rs` — live PostgreSQL ownership-lock concurrency tests.
- `tests/compose.postgres-import.yml` — disposable PostgreSQL service for those tests.

**Modify narrowly:**

- `Cargo.toml`, `Cargo.lock`, `src/lib.rs`, `src/config.rs`, `config.example.toml` — direct CID/URL dependencies and module/config registration.
- `src/error.rs`, `src/main.rs` — S3 error mapping, coordinator construction, and worker lifecycle.
- `src/store/mod.rs`, `src/store/entities/mod.rs`, `src/store/migrations/mod.rs` — registrations/tests.
- `src/store/pinning/publication.rs` — optional import guard inside publication transaction.
- `src/kubo/{mod.rs,add.rs,pin.rs,cat.rs}` — progress APIs and logical-size inspection.
- `src/s3/route/{mod.rs,decompress_zip.rs}` — composite registration and reusable ZIP boundary.
- `src/s3/ops/{object.rs,multipart.rs,bucket.rs}` — mutation admission at exact destinations.
- `src/zip/extract.rs` — optional entry/progress observer while preserving existing wrappers.
- `tests/support/mod.rs`, `tests/integration.rs` — import scenarios and standard regressions.

## Binary acceptance scenarios

1. **CID happy path:** signed POST returns 202/job ID; status advances through provider discovery and local pin; final object has the submitted CID and correct logical size.
2. **URL edge path:** allowlisted HTTPS with unknown `Content-Length` streams successfully, never reports a percentage, and fails before publication when bytes exceed the configured limit.
3. **Stale-worker race:** a blocked import is superseded by PUT/COPY/DELETE/multipart completion or a newer import; after unblocking, the old worker publishes no object, tag, result, lease, or provider job.
4. **Combined ZIP:** import plus decompression exposes no new archive/entries while running, then publishes archive and successful entries in one transaction and returns paginated failures/results.
5. **Adjacent regression:** standard requests without `ipfs3-import` retain current PUT/GET/HEAD/List/Tagging/Multipart/SSE/SigV4 behavior and trigger no import/Kubo-routing/downloader calls.

---

### Task 1: Import configuration and domain model

**Files:**
- Create: `src/import/mod.rs`
- Create: `src/import/model.rs`
- Create: `src/import/error.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `src/lib.rs`
- Modify: `src/config.rs`
- Modify: `src/error.rs`
- Modify: `config.example.toml`

- [ ] **Step 1: Write failing configuration and model tests**

Add tests in `src/config.rs` named:

```rust
#[test]
fn import_defaults_preserve_existing_config_files() {
    let config = ImportConfig::default();
    assert!(config.enabled);
    assert!(config.allowed_https_origins.is_empty());
    assert_eq!(config.worker_concurrency, 4);
    assert_eq!(config.poll_interval_ms, 500);
    assert_eq!(config.lease_duration_secs, 60);
    assert_eq!(config.progress_flush_interval_ms, 1_000);
    assert_eq!(config.connect_timeout_secs, 10);
    assert_eq!(config.idle_timeout_secs, 120);
    assert_eq!(config.job_timeout_secs, 86_400);
    assert_eq!(config.max_download_bytes, 5_368_709_120);
    assert_eq!(config.max_attempts, 5);
    assert_eq!(config.terminal_retention_secs, 604_800);
    assert_eq!(config.max_provider_records, 20);
}

#[test]
fn import_origins_require_normalized_https_origins() {
    for origin in [
        "http://example.com",
        "https://user@example.com",
        "https://example.com/path",
        "https://example.com?query=1",
        "https://127.0.0.1",
    ] {
        let config = ImportConfig {
            allowed_https_origins: vec![origin.to_owned()],
            ..ImportConfig::default()
        };
        assert!(config.validate().is_err(), "accepted {origin}");
    }
}

#[test]
fn empty_import_origin_list_keeps_cid_import_enabled() {
    let validated = ImportConfig::default().validate().unwrap();
    assert!(validated.raw.enabled);
    assert!(validated.allowed_origins.is_empty());
}

#[test]
fn import_numeric_bounds_are_fail_fast() {
    for count in [0, 21] {
        let config = ImportConfig {
            max_provider_records: count,
            ..ImportConfig::default()
        };
        assert!(config.validate().is_err());
    }
    let config = ImportConfig {
        idle_timeout_secs: 0,
        ..ImportConfig::default()
    };
    assert!(config.validate().is_err());
}
```

Add model transition tests in `src/import/model.rs` asserting the exact state/phase strings from the spec and rejecting impossible terminal-to-running transitions.
Add error tests in `src/error.rs` that map malformed import, denied source, unknown job, idempotency conflict, disabled feature, and stale ownership to the exact synchronous S3/error behavior from the spec without exposing source/backend details.

- [ ] **Step 2: Run the focused tests and capture RED**

Run:

```powershell
cargo test --lib import_defaults_preserve_existing_config_files -- --nocapture
cargo test --lib import_model -- --nocapture
cargo test --lib error::tests::import_errors_are_stable_and_redacted -- --nocapture
```

Expected: compilation fails because `ImportConfig`, `ImportState`, and `ImportPhase` do not exist.

- [ ] **Step 3: Add exact configuration and model types**

Implement and export these interfaces:

```rust
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImportConfig {
    pub enabled: bool,
    pub allowed_https_origins: Vec<String>,
    pub worker_concurrency: usize,
    pub poll_interval_ms: u64,
    pub lease_duration_secs: u64,
    pub progress_flush_interval_ms: u64,
    pub connect_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub job_timeout_secs: u64,
    pub max_download_bytes: u64,
    pub max_attempts: u32,
    pub terminal_retention_secs: u64,
    pub max_provider_records: usize,
}

#[derive(Clone, Debug)]
pub struct ValidatedImportConfig {
    pub raw: ImportConfig,
    pub allowed_origins: std::collections::HashSet<url::Origin>,
}

impl ImportConfig {
    pub fn validate(&self) -> anyhow::Result<ValidatedImportConfig>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportState { Queued, Running, Completed, Failed, Superseded }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportPhase {
    Queued, DiscoveringProviders, PinningLocal, Downloading,
    AddingToIpfs, Inspecting, Decompressing, Publishing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImportSource { Cid(String), Url(url::Url) }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupersedeReason {
    NewImport, PutObject, CopyObject, DeleteObject,
    CompleteMultipartUpload, DecompressZip, DeleteBucket,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportFailureCode {
    SourceUnreachable, SourceHttpError, SourceRedirected, SourceTooLarge,
    SourceStalled, CidNotFound, CidNotFile, KuboAddFailed, KuboPinFailed,
    InvalidArchive, DecompressionLimitExceeded, PublicationFailed,
    JobDeadlineExceeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportFailure {
    pub code: ImportFailureCode,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportClaim {
    pub job_id: String,
    pub worker_id: String,
    pub attempt: u32,
    pub claim_epoch: i64,
    pub locked_until: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImportProgress {
    pub providers_observed: u32,
    pub pin_nodes_processed: u64,
    pub pin_bytes_processed: u64,
    pub downloaded_bytes: u64,
    pub download_total: Option<u64>,
    pub ipfs_add_bytes: u64,
    pub logical_size: Option<u64>,
    pub entries_processed: u64,
    pub entries_succeeded: u64,
    pub entries_failed: u64,
    pub decompressed_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportExecutionError {
    #[error("retryable import failure")]
    Retryable(ImportFailure),
    #[error("terminal import failure")]
    Terminal(ImportFailure),
    #[error("import ownership was superseded")]
    Superseded,
    #[error("import execution was interrupted by process shutdown")]
    Interrupted,
}
```

Add direct dependencies `cid = "0.11"`, `url = "2"`, and `rustls = "0.23"`, then update the lockfile through the normal Cargo test/build command rather than by hand. `rustls::Error` is used only to distinguish terminal certificate/hostname validation from retryable TLS transport interruption in the downloader error source chain. Add `pub imports: ImportConfig` to `Config`, defaults exactly matching the spec, and the documented `[imports]` block to `config.example.toml`. Validation must normalize omitted HTTPS port and port 443 identically, reject IP literals, and enforce `max_provider_records` in `1..=20`.

Add explicit `AppError` variants for invalid import request, denied URL, unknown job, idempotency conflict, disabled import, and stale import ownership. Only the first five map to custom-route S3 errors; stale ownership is internal and converts to `ImportExecutionError::Superseded` in the worker.

- [ ] **Step 4: Run GREEN and formatting**

```powershell
cargo test --lib import_defaults_preserve_existing_config_files -- --nocapture
cargo test --lib import_origins_require_normalized_https_origins -- --nocapture
cargo test --lib import_model -- --nocapture
cargo test --lib error::tests::import_errors_are_stable_and_redacted -- --nocapture
cargo fmt -- --check
```

Expected: all focused tests pass; formatting exits 0.

- [ ] **Step 5: Checkpoint**

Record the changed config/model files and test output. Do not stage or commit them.

---

### Task 2: Import schema, entities, and migration constraints

**Files:**
- Create: `src/store/migrations/m20260729_000001_ipfs3_import.rs`
- Create: `src/store/entities/import_job.rs`
- Create: `src/store/entities/import_destination.rs`
- Create: `src/store/entities/import_prefix_claim.rs`
- Create: `src/store/entities/import_job_target.rs`
- Create: `src/store/entities/import_job_result.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/entities/mod.rs`
- Modify: `src/store/mod.rs`

- [ ] **Step 1: Write failing SQLite/PostgreSQL schema tests**

Extend migration tests to require exactly these tables and constraints:

```text
import_jobs
import_destinations
import_prefix_claims
import_job_targets
import_job_results
```

Tests must reject invalid `source_type`, `state`, `phase`, negative counters or claim epochs, duplicate `(bucket,key,client_token)`, duplicate target keys per job, and result sequence reuse. Test cascade cleanup of targets/results/claims when a terminal job is removed, while destination generations remain.

- [ ] **Step 2: Run schema tests and capture RED**

```powershell
cargo test --lib store::tests::migrations_create_import_tables -- --nocapture
cargo test --lib store::entities::tests::import_constraints -- --nocapture
```

Expected: failure because the migration and entity modules are absent.

- [ ] **Step 3: Implement the migration and entity models**

Use SeaORM `DeriveEntityModel` and the repository's migration style. The migration must create:

```text
import_jobs:
  id PK, bucket, key, source_type, source_value, request_fingerprint,
  client_token NULL, object_content_type NULL, metadata_json, tags_json,
  decompress_prefix NULL, state, phase, attempts, next_attempt_at,
  locked_by NULL, locked_until NULL, claim_epoch BIGINT NOT NULL DEFAULT 0,
  all progress counters,
  download_total NULL, logical_size NULL, final_cid NULL,
  failure_code NULL, failure_message NULL, created_at, updated_at, completed_at NULL

import_destinations:
  (bucket,key) PK, generation BIGINT >= 1, owner_job_id NULL, updated_at

import_prefix_claims:
  job_id, bucket, prefix, claim_order; PK(job_id,bucket,prefix)

import_job_targets:
  job_id, bucket, key, expected_generation, kind; PK(job_id,bucket,key)

import_job_results:
  job_id, sequence, key, cid NULL, size NULL,
  error_code NULL, error_message NULL; PK(job_id,sequence)
```

Create indexes for due-job claims, terminal cleanup, `(bucket,key)` ownership checks, prefix claims by bucket, and result pagination. Register migration/entity modules and update table-name assertions.

- [ ] **Step 4: Run GREEN on both backend-specific test paths**

```powershell
cargo test --lib store::tests::migrations_create_import_tables -- --nocapture
cargo test --lib store::entities::tests::import_constraints -- --nocapture
```

Expected: all schema/entity tests pass for SQLite; PostgreSQL-gated SQL-shape tests pass without weakening constraints.

- [ ] **Step 5: Checkpoint**

Record migration/entity files and exact passing test names. Do not stage or commit.

---

### Task 3: Durable job primitives, claim epochs, progress, retries, and results

**Files:**
- Create: `src/store/import/mod.rs`
- Create: `src/store/import/jobs.rs`
- Create: `src/store/import/results.rs`
- Modify: `src/store/mod.rs`

- [ ] **Step 1: Write failing store tests**

Cover: fair due-job claim, atomic claim-epoch increment on every initial/reclaimed lease, stale-epoch rejection for renewal/phase/progress/retry, lease renewal/expiry, progress monotonicity within one attempt, retry counter/reset of URL bytes, path-bound lookup, result cursor pages, and retention deletion. Atomic submission/idempotency and ownership-releasing terminal transitions are deferred to Task 4, where ownership primitives exist.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib store::import::jobs::tests -- --nocapture
cargo test --lib store::import::results::tests -- --nocapture
```

Expected: compile failure because `store::import` does not exist.

- [ ] **Step 3: Implement explicit store interfaces**

```rust
pub struct NewImportJob {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub source: ImportSource,
    pub request_fingerprint: String,
    pub client_token: Option<String>,
    pub object_content_type: Option<String>,
    pub metadata: std::collections::HashMap<String, String>,
    pub tags: Vec<crate::pinning::tags::ObjectTag>,
    pub decompress_prefix: Option<String>,
}

pub enum SubmitImportOutcome { Created(import_job::Model), Replayed(import_job::Model) }

pub(crate) async fn find_idempotent<C: ConnectionTrait>(
    txn: &C, bucket: &str, key: &str, client_token: &str,
) -> AppResult<Option<import_job::Model>>;

pub(crate) async fn insert_queued<C: ConnectionTrait>(
    txn: &C, request: NewImportJob, now: DateTime<Utc>,
) -> AppResult<import_job::Model>;

pub struct ClaimedImportJob {
    pub job: import_job::Model,
    pub claim: ImportClaim,
}

pub async fn claim_due(
    db: &DatabaseConnection,
    worker_id: &str,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u64,
) -> AppResult<Vec<ClaimedImportJob>>;

pub async fn renew_claim<C: ConnectionTrait>(
    db: &C, job_id: &str, worker_id: &str, claim_epoch: i64,
    now: DateTime<Utc>, lease_until: DateTime<Utc>,
) -> AppResult<bool>;
pub async fn update_phase<C: ConnectionTrait>(
    db: &C, job_id: &str, worker_id: &str, claim_epoch: i64,
    state: ImportState, phase: ImportPhase, now: DateTime<Utc>,
) -> AppResult<()>;
pub async fn update_progress<C: ConnectionTrait>(
    db: &C, job_id: &str, worker_id: &str, claim_epoch: i64, attempt: u32,
    progress: &ImportProgress, now: DateTime<Utc>,
) -> AppResult<()>;
pub async fn retry<C: ConnectionTrait>(
    db: &C, job_id: &str, worker_id: &str, claim_epoch: i64, attempt: u32,
    next_attempt_at: DateTime<Utc>, failure: &ImportFailure,
    now: DateTime<Utc>,
) -> AppResult<()>;
pub async fn get_for_path<C: ConnectionTrait>(
    db: &C, job_id: &str, bucket: &str, key: &str,
) -> AppResult<Option<import_job::Model>>;

pub struct ResultPage {
    pub rows: Vec<import_job_result::Model>,
    pub next_sequence: Option<i64>,
}

pub async fn page<C: ConnectionTrait>(
    db: &C, job_id: &str, after: Option<i64>, limit: u64,
) -> AppResult<ResultPage>;
```

`claim_due` increments `claim_epoch` in the same conditional update that sets `locked_by/locked_until`, then returns the exact epoch in `ImportClaim`. Every worker-authored update, including retry/fail/publication, matches `(job_id, worker_id, claim_epoch)` and requires an unexpired lease. Status path mismatch returns `None`. Progress writes never move counters backward within one attempt.

- [ ] **Step 4: Run GREEN**

```powershell
cargo test --lib store::import::jobs::tests -- --nocapture
cargo test --lib store::import::results::tests -- --nocapture
```

Expected: all job/result persistence tests pass.

- [ ] **Step 5: Checkpoint**

Record store APIs and test evidence. Do not stage or commit.

---

### Task 4: Destination generations, prefix claims, and publication guard

**Files:**
- Create: `src/store/import/ownership.rs`
- Create: `tests/postgres_import.rs`
- Create: `tests/compose.postgres-import.yml`
- Modify: `src/store/import/jobs.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`

- [ ] **Step 1: Write failing race and rollback tests**

Create deterministic tests for atomic new submission, same-token replay, mismatched-token fingerprint, no-token supersession, exact generation monotonicity, overlapping prefixes, empty-prefix whole-bucket matching, output-target claims, failed/superseded ownership release, one lost target rolling back every ZIP object/result, and a stale worker blocked immediately before publication. Add the critical lease race: attempt 1 expires, attempt 2 reclaims the same job with a higher claim epoch, and attempt 1 cannot renew, claim an extracted target, fail, update, or publish even though destination generations still match. With barriers rather than sleeps, race submit against exact admission, prefix admission, extracted-target claim, and bucket deletion using two connections to a file-backed SQLite database and to live PostgreSQL.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib store::import::ownership::tests -- --nocapture
cargo test --lib store::pinning::publication::tests::stale_import_guard_rolls_back_everything -- --nocapture
```

Expected: compile failure because ownership and guarded publication APIs are absent.

- [ ] **Step 3: Implement ownership interfaces**

```rust
#[derive(Clone, Debug)]
pub struct ExpectedImportTarget {
    pub bucket: String,
    pub key: String,
    pub generation: i64,
}

#[derive(Clone, Debug)]
pub struct ImportPublicationGuard {
    pub job_id: String,
    pub worker_id: String,
    pub claim_epoch: i64,
    pub targets: Vec<ExpectedImportTarget>,
}

pub async fn lock_bucket_for_ownership<C: ConnectionTrait>(
    txn: &C, bucket: &str,
) -> AppResult<()>;

pub async fn submit(
    db: &DatabaseConnection,
    request: NewImportJob,
    now: DateTime<Utc>,
) -> AppResult<SubmitImportOutcome>;

pub async fn claim_primary_destination<C: ConnectionTrait>(
    txn: &C, job_id: &str, bucket: &str, key: &str, now: DateTime<Utc>,
) -> AppResult<i64>;

pub async fn install_prefix_claim<C: ConnectionTrait>(
    txn: &C, job_id: &str, bucket: &str, prefix: &str, now: DateTime<Utc>,
) -> AppResult<()>;

pub async fn claim_extracted_target(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<i64>;

pub async fn admit_content_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C, bucket: &str, key: &str, excluding_job: Option<&str>, now: DateTime<Utc>,
) -> AppResult<()>;

pub async fn admit_prefix_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C, bucket: &str, prefix: &str, now: DateTime<Utc>,
) -> AppResult<()>;

pub async fn admit_content_and_prefix_mutation<C: ConnectionTrait + TransactionTrait>(
    db: &C, bucket: &str, key: &str, prefix: &str, now: DateTime<Utc>,
) -> AppResult<()>;

pub async fn supersede_bucket<C: ConnectionTrait>(
    txn: &C, bucket: &str, now: DateTime<Utc>,
) -> AppResult<u64>;

pub async fn fail_claimed(
    db: &DatabaseConnection,
    claim: &ImportClaim,
    bucket: &str,
    failure: &ImportFailure,
    now: DateTime<Utc>,
) -> AppResult<()>;

pub async fn supersede_job_in_transaction<C: ConnectionTrait>(
    txn: &C, job_id: &str, reason: SupersedeReason, now: DateTime<Utc>,
) -> AppResult<()>;
```

Every ownership-changing transaction calls `lock_bucket_for_ownership` as its first database operation after `begin`: PostgreSQL uses `bucket::Entity::find_by_id(bucket).lock_exclusive().one(txn)`; SQLite executes a no-op `UPDATE buckets SET created_at = created_at WHERE name = ?` to acquire write intent. Both paths return `NoSuchBucket` when no row is locked. After the bucket lock, acquire job/destination rows in canonical `(bucket,key)` order. This rule applies to submit, exact/prefix admission, extracted-target claim, failed/superseded/completed release, guarded publication, and bucket deletion. It prevents prefix/exact phantom races and serializes submit against successful bucket deletion without holding locks during network work.

`submit` owns one transaction under that lock: resolve idempotent replay and compare the SHA-256 request fingerprint exactly; insert the queued job; increment/own the primary destination; insert the archive target; install an optional prefix claim; supersede conflicting exact/prefix owners; then commit. No public route can insert an unowned job. `claim_extracted_target` opens its own transaction, locks the bucket first, then verifies `(job_id, worker_id, claim_epoch, running, locked_until > now)` before incrementing a destination generation or inserting a target. Use canonical lock ordering and retry the same narrow SQLite/PostgreSQL transaction conflicts already handled by publication code.

Create `tests/compose.postgres-import.yml` with a pinned `postgres:17` service, fixed test database/user/password, host port 55432, and `pg_isready` healthcheck. `tests/postgres_import.rs` requires `IPFS_S3_TEST_POSTGRES_URL`, runs migrations, opens independent connections, and uses barriers/channels to prove the bucket first-lock rule for submit versus exact admission, overlapping prefix admission, extracted-target claim, and bucket deletion. It must assert final generations/owners/job states, not only absence of deadlock.

Every terminal transition releases ownership transactionally. `supersede_job_in_transaction` is called only after its caller holds the bucket lock; it marks the job terminal, clears exact destinations only where `owner_job_id` still matches, and deletes its prefix claims/targets in the caller transaction. `fail_claimed` opens a transaction, locks the supplied bucket first, validates the job belongs to that bucket, fences `(worker_id, claim_epoch, unexpired locked_until)`, then marks failed and releases claims. Standard mutation admission uses the same supersession helper. Successful completion releases ownership inside guarded publication after the same first bucket lock. Retry retains ownership.

- [ ] **Step 4: Add import-specific publication wrappers without changing standard callers**

Keep `publish_object`, `publish_completed_upload`, `publish_zip`, and `publish_completed_zip` signatures stable. Add:

```rust
pub async fn publish_import_object(
    db: &DatabaseConnection,
    request: PublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult>;

pub async fn publish_import_zip(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    guard: ImportPublicationGuard,
    result_rows: Vec<import_job_result::ActiveModel>,
    now: DateTime<Utc>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult>;
```

Thread `Option<&ImportPublicationGuard>` into `publication_attempt` and `publish_in_transaction`. Inside `publish_in_transaction`, lock the archive bucket first, then every destination and the job; verify owner/generation/bucket plus `locked_by == worker_id`, exact `claim_epoch`, running state, and `locked_until > now`; then write result rows, complete the job, and release claims in the same transaction. Guard mismatch returns typed stale import ownership and rolls back; a stale worker must not mark the reclaimed job superseded afterward.

- [ ] **Step 5: Run GREEN and concurrency repetition**

```powershell
cargo test --lib store::import::ownership::tests -- --nocapture
cargo test --lib store::pinning::publication::tests::stale_import_guard_rolls_back_everything -- --nocapture
cargo test --lib store::pinning::publication::tests::expired_attempt_cannot_publish_after_reclaim -- --nocapture
1..20 | ForEach-Object { cargo test --lib store::pinning::publication::tests::stale_import_guard_rolls_back_everything --quiet; if ($LASTEXITCODE -ne 0) { throw "race iteration $_ failed" } }
$compose = "tests/compose.postgres-import.yml"
docker compose -f $compose up -d --wait
try {
    $env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:ipfs3@127.0.0.1:55432/ipfs3_import_test"
    cargo test --test postgres_import -- --nocapture
    if ($LASTEXITCODE -ne 0) { throw "PostgreSQL import ownership tests failed" }
} finally {
    Remove-Item Env:IPFS_S3_TEST_POSTGRES_URL -ErrorAction SilentlyContinue
    docker compose -f $compose down -v
}
```

Expected: all tests pass for every iteration on file-backed SQLite and disposable PostgreSQL with no partial rows; attempt 1 cannot alter or publish the job after attempt 2 increments `claim_epoch`; the PostgreSQL container and volume are removed in `finally`.

- [ ] **Step 6: Checkpoint**

Record transaction/race evidence. Do not stage or commit.

---

### Task 5: Supersession admission for standard S3 content mutations

**Files:**
- Modify: `src/s3/ops/object.rs`
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/s3/ops/bucket.rs`
- Modify: `src/store/bucket.rs`
- Modify: `src/store/import/ownership.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/entities/import_destination.rs`
- Create: additive standard-mutation-fence migration
- Modify: `src/s3/route/decompress_zip.rs`
- Test: existing unit modules, `tests/integration.rs`, and `tests/postgres_import.rs`

- [ ] **Step 1: Write failing admission tests**

For each operation, seed a running job/claim, invoke the standard operation, and assert the job becomes `superseded` before backend content work: PutObject, CopyObject destination, DeleteObject including absent key, every DeleteObjects key, CompleteMultipartUpload, direct decompression archive/prefix, and DeleteBucket. Assert multipart create/upload/abort, reads, lists, and tagging do not supersede. Add deterministic final-fence tests for every exact standard operation plus direct and multipart ZIP prefix publication. In the critical causal ordering, block an older standard ZIP operation after admission, fully publish a newer overlapping import first, then release the old operation and require `OperationAborted`, the newer body/latest row, and no stale object/result/lease/provider-job side effects. Include literal `%`, `_`, and case-sensitive prefixes plus unrelated-key nonconflict.

- [ ] **Step 2: Run RED**

```powershell
cargo test --test integration standard_content_mutations_supersede_import -- --nocapture
cargo test --test integration non_content_operations_do_not_supersede_import -- --nocapture
```

Expected: active imports remain running because admission calls are not wired.

- [ ] **Step 3: Insert mutation admission at exact boundaries**

Call `admit_content_mutation` after destination validation and before Kubo/network/body work in:

```text
src/s3/ops/object.rs::put_object
src/s3/ops/object.rs::copy_object              destination only
src/s3/ops/object.rs::delete_object
src/s3/ops/object.rs::delete_objects           every requested key
src/s3/ops/multipart.rs::complete_multipart_upload_inner
```

Each admission helper opens one short transaction, invokes `lock_bucket_for_ownership` first, then locks destinations/jobs canonically, increments generations, supersedes matching exact/prefix owners, installs a durable UUID mutation token, and returns a `StandardMutationGuard` before the operation starts network/body work. `DeleteObjects` batches all requested keys for one bucket into one admission transaction rather than opening one transaction per key. Later standard/import exact or prefix admission invalidates all older overlapping active tokens using literal, bounded, canonical queries; unrelated keys remain independent.

For direct single-PUT decompression, parse/normalize the prefix first, then call `admit_content_and_prefix_mutation` so the archive exact key and output prefix are admitted in one transaction before adding the archive. Refactor multipart completion preflight so `complete_multipart_upload_inner` loads and validates the upload, bucket/key, submitted parts, and stored `decompress_zip_target` before any Kubo concatenation or pin. If a decompression prefix exists, call `admit_content_and_prefix_mutation`; otherwise call `admit_content_mutation`. Flow the returned guard through `CompletedMultipartArchive` and finalizer reconciliation. Only then begin Kubo work. `DecompressZipRoute::call_complete` must not perform a later independent prefix admission. Guarded standard publication/delete locks the bucket first, validates and clears its token in the same transaction as every object/tag/lease/target/provider-job/upload write or delete, and maps stale guards to stable `OperationAborted`. Refactor bucket deletion to begin one transaction, call `lock_bucket_for_ownership` first, perform the existing non-empty check, call `supersede_bucket`, and delete the bucket before commit. A failed `BucketNotEmpty` deletion rolls back supersession.

- [ ] **Step 4: Run GREEN plus standard adjacent tests**

```powershell
cargo test --test integration standard_content_mutations_supersede_import -- --nocapture
cargo test --test integration non_content_operations_do_not_supersede_import -- --nocapture
cargo test --test integration multipart_decompress_admits_prefix_before_kubo -- --nocapture
cargo test --test integration newer_import_submitted_during_blocked_complete_wins -- --nocapture
cargo test --test integration newer_import_completed_during_blocked_direct_decompress_wins -- --nocapture
cargo test --test integration test_standard_put_sse_s3_still_succeeds -- --nocapture
cargo test --test integration test_standard_put_sse_c_still_succeeds -- --nocapture
cargo test --test integration test_standard_multipart_signed_still_succeeds -- --nocapture
```

Expected: supersession tests pass; in both blocked-standard races the newer import completes before release and the old publication fails without side effects; standard response contracts remain green. Run the corresponding live PostgreSQL mutation-fence test with `IPFS_S3_TEST_POSTGRES_URL` set.

- [ ] **Step 5: Checkpoint**

Record every integration point and regression output. Do not stage or commit.

---

### Task 6: Streamed Kubo provider, add, pin, and inspection progress

**Files:**
- Create: `src/kubo/routing.rs`
- Modify: `src/kubo/mod.rs`
- Modify: `src/kubo/add.rs`
- Modify: `src/kubo/pin.rs`
- Modify: `src/kubo/cat.rs`

- [ ] **Step 1: Write failing wiremock tests**

Cover fragmented newline-delimited JSON, multiple provider events for one peer, malformed frames, redacted non-2xx errors, no-progress idle timeout, cancellation, add progress before final CID, pin progress plus final Pins, directory rejection, and logical-size extraction from `cat` response headers with streaming-count fallback. Add body-source error identity tests proving a `DownloadError::TooLarge`, `Stalled`, or `Canceled` emitted after several uploaded chunks returns as the exact `StreamAddError::Source` variant rather than a generic reqwest/Kubo error.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib kubo::routing::tests -- --nocapture
cargo test --lib kubo::add::tests::stream_add_reports_progress -- --nocapture
cargo test --lib kubo::pin::tests::pin_add_reports_progress -- --nocapture
```

Expected: missing routing/progress APIs.

- [ ] **Step 3: Implement progress event interfaces**

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KuboProgress {
    ProviderObserved { peer_id: String },
    AddBytes { bytes: u64 },
    PinProgress { nodes: u64, bytes: u64 },
}

pub type ProgressSender = tokio::sync::mpsc::Sender<KuboProgress>;

pub async fn find_providers(
    kubo: &KuboClient,
    cid: &str,
    max_providers: usize,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> AppResult<u32>;

pub struct AddResult { pub cid: String, pub kubo_size: Option<u64> }

#[derive(Debug, thiserror::Error)]
pub enum StreamAddError<E: std::error::Error + Send + Sync + 'static> {
    #[error("source stream failed")]
    Source(#[source] E),
    #[error("Kubo add failed")]
    Kubo(#[source] AppError),
    #[error("Kubo add was canceled")]
    Canceled,
}

pub async fn stream_add_with_progress<S, E>(
    kubo: &KuboClient,
    stream: S,
    cid_version: u8,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> Result<AddResult, StreamAddError<E>>
where S: Stream<Item = Result<Bytes, E>> + Send + 'static,
      E: std::error::Error + Send + Sync + 'static;

pub async fn pin_add_with_progress(
    kubo: &KuboClient,
    cid: &str,
    progress: ProgressSender,
    cancel: CancellationToken,
) -> AppResult<()>;

pub async fn inspect_file(
    kubo: &KuboClient, cid: &str, cancel: CancellationToken,
) -> AppResult<u64>;
```

Parse response bodies incrementally with a bounded line buffer and inter-frame idle timeout. Deduplicate provider peer IDs. To preserve source error identity, do not pass the caller's fallible stream directly to `reqwest::Body::wrap_stream`: pump it into a bounded Tokio duplex/channel in a separately awaited producer future, upload only the infallible/`io::Error` receiver side, and `select!` producer, HTTP upload/response, and cancellation. A producer error cancels the HTTP request and returns `StreamAddError::Source(error)` exactly. Preserve the existing public generic bounds of `stream_add`: map its `E: Into<Box<dyn Error + Send + Sync>>` into a private boxed error newtype that implements `Error`, call the typed primitive, and redact `StreamAddError::Source` back into the existing `AppError` contract. Preserve `pin_add` as the same compatibility wrapper so ordinary S3 call sites do not change behavior.

- [ ] **Step 4: Run GREEN and existing Kubo regressions**

```powershell
cargo test --lib kubo::routing::tests -- --nocapture
cargo test --lib kubo::add::tests -- --nocapture
cargo test --lib kubo::pin::tests -- --nocapture
cargo test --lib kubo::cat::tests -- --nocapture
```

Expected: all new and existing Kubo tests pass; backend error bodies remain redacted.

- [ ] **Step 5: Checkpoint**

Record progress-frame and idle/cancellation evidence. Do not stage or commit.

---

### Task 7: Strict HTTPS downloader and SSRF controls

**Files:**
- Create: `src/import/downloader.rs`
- Modify: `src/import/mod.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

- [ ] **Step 1: Write failing URL-policy and streaming tests**

Tests must cover normalized exact origins, explicit/default port equivalence, rejected HTTP/IP/userinfo/fragment, public and forbidden DNS answers, IPv4-mapped IPv6, hostname re-resolution on retry, TLS hostname preservation, terminal invalid-certificate/hostname errors, retryable TLS connection reset/timeout, disabled redirects, no forwarded credentials/cookies, known and unknown length, early Content-Length rejection, streaming cutoff, connection timeout, inter-chunk idle timeout, cancellation, and URL-free diagnostics.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib import::downloader::tests -- --nocapture
```

Expected: compile failure because `SourceDownloader` and URL policy types do not exist.

- [ ] **Step 3: Implement resolver-bound download interfaces**

```rust
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("source is not allowed")]
    NotAllowed,
    #[error("source name resolution failed")]
    Dns,
    #[error("source connection failed")]
    Connect,
    #[error("source TLS certificate validation failed")]
    TlsCertificate,
    #[error("source TLS transport failed")]
    TlsTransport,
    #[error("source redirected")]
    Redirect,
    #[error("source returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("source exceeded byte limit")]
    TooLarge,
    #[error("source stalled")]
    Stalled,
    #[error("source transfer was canceled")]
    Canceled,
    #[error("source response was invalid")]
    InvalidResponse,
}

impl DownloadError {
    pub fn into_import_failure(self) -> ImportFailure;
}

#[derive(Clone, Copy, Debug)]
pub struct DownloadLimits {
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_bytes: u64,
}

#[async_trait::async_trait]
pub trait ImportResolver: Send + Sync {
    async fn resolve(
        &self, host: &str, port: u16,
    ) -> Result<Vec<std::net::SocketAddr>, DownloadError>;
}

pub trait AddressPolicy: Send + Sync {
    fn validate(
        &self, addresses: &[std::net::SocketAddr],
    ) -> Result<(), DownloadError>;
}

#[derive(Clone, Debug)]
pub struct AuthorizedSource {
    url: url::Url,
    server_name: String,
    addresses: Vec<std::net::SocketAddr>,
}

#[async_trait::async_trait]
pub trait ImportHttpTransport: Send + Sync {
    async fn open(
        &self,
        source: AuthorizedSource,
        limits: DownloadLimits,
        progress: tokio::sync::watch::Sender<u64>,
        cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError>;
}

pub struct ReqwestImportHttpTransport {
    extra_root_certificates: Vec<reqwest::Certificate>,
}

impl ReqwestImportHttpTransport {
    pub fn new(
        limits: DownloadLimits,
        extra_root_certificates: Vec<reqwest::Certificate>,
    ) -> Self;
}

pub struct StrictPublicAddressPolicy;

impl AddressPolicy for StrictPublicAddressPolicy {
    fn validate(
        &self, addresses: &[std::net::SocketAddr],
    ) -> Result<(), DownloadError>;
}

pub struct SourceDownloader {
    config: Arc<ValidatedImportConfig>,
    resolver: Arc<dyn ImportResolver>,
    address_policy: Arc<dyn AddressPolicy>,
    transport: Arc<dyn ImportHttpTransport>,
}

pub struct DownloadStream {
    pub body: Pin<Box<dyn Stream<Item = Result<Bytes, DownloadError>> + Send>>,
    pub total: Option<u64>,
    pub content_type: Option<String>,
}

impl SourceDownloader {
    pub fn production(config: Arc<ValidatedImportConfig>) -> Self;
    #[doc(hidden)]
    pub fn with_components(
        config: Arc<ValidatedImportConfig>,
        resolver: Arc<dyn ImportResolver>,
        address_policy: Arc<dyn AddressPolicy>,
        transport: Arc<dyn ImportHttpTransport>,
    ) -> Self;
    pub async fn authorize(
        &self, source: &url::Url,
    ) -> Result<AuthorizedSource, DownloadError>;
    pub async fn open(
        &self,
        source: &url::Url,
        progress: tokio::sync::watch::Sender<u64>,
        cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError>;
}
```

`authorize` performs the same origin and DNS/address checks used by `open`; the route calls it before creating a URL job, and `open` repeats it on every worker attempt. Resolve once per attempt, reject the complete forbidden-address set before connecting, and pass only `AuthorizedSource` plus validated `DownloadLimits` to the transport. The production reqwest transport uses host-resolution override so TLS host/SNI remains the allowed hostname and no unchecked second DNS lookup occurs. Build it with `.no_proxy()`, redirect policy `none`, no cookie store, configured connect timeout, and rustls certificate verification. Apply the idle timeout and byte limit to every body frame so `DownloadError` survives through the stream; return `TooLarge` immediately after the configured maximum is crossed. Walk the reqwest error source chain: `rustls::Error::InvalidCertificate` and hostname/certificate validation become terminal `TlsCertificate`; connection reset, EOF, timeout, and transport interruption during handshake become retryable `TlsTransport`. Except for `Canceled`, which Task 8 resolves from shutdown/ownership context, `DownloadError::into_import_failure` is the sole retryability mapping: DNS/connect/TLS transport/stall/5xx are retryable, while certificate/policy/redirect/4xx/oversize/invalid response are terminal.

Add `tokio-rustls = "0.26"` and `rcgen = "0.14"` as dev dependencies. Downloader unit tests run a real loopback TLS server with a generated test CA and original hostname SNI. They inject an `AddressPolicy` that permits loopback only for that test while using the production reqwest transport with the test CA. Separate strict-policy tests prove production authorization rejects loopback before transport invocation. Integration tests may inject the same components; no import configuration option can select a permissive policy or custom CA.

- [ ] **Step 4: Run GREEN and security-focused repetitions**

```powershell
cargo test --lib import::downloader::tests -- --nocapture
1..10 | ForEach-Object { cargo test --lib import::downloader::tests::dns_rebinding_cannot_escape_validated_addresses --quiet; if ($LASTEXITCODE -ne 0) { throw "DNS test iteration $_ failed" } }
```

Expected: all policy/stream tests pass and captured errors/log fields contain no source URL.

- [ ] **Step 5: Checkpoint**

Record the forbidden-address matrix, TLS fixture, and output. Do not stage or commit.

---

### Task 8: Import coordinator, durable worker, and CID/URL pipelines

**Files:**
- Create: `src/import/pipeline.rs`
- Create: `src/import/worker.rs`
- Modify: `src/import/mod.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Write failing worker lifecycle tests**

Test bounded concurrency, fair claims, lease renewal, expired-lease restart, progress flush coalescing, graceful shutdown producing `Interrupted` without consuming an attempt or changing terminal state, URL retry resetting download counters, exact mid-upload TooLarge/Stalled classification, retryable TLS transport versus terminal certificate failure, max attempts, overall deadline, ownership-loss cancellation, and stale publication fallback.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib import::worker::tests -- --nocapture
cargo test --lib import::pipeline::tests -- --nocapture
```

Expected: compile failure because coordinator/worker/pipeline types are absent.

- [ ] **Step 3: Implement coordinator and worker contracts**

```rust
pub struct ImportCoordinator {
    config: Arc<ValidatedImportConfig>,
    downloader: Arc<SourceDownloader>,
}

impl ImportCoordinator {
    pub fn new(
        config: ValidatedImportConfig,
        downloader: SourceDownloader,
    ) -> Arc<Self>;
    pub fn enabled(&self) -> bool;
    pub async fn authorize_url_for_submission(
        &self, source: &url::Url,
    ) -> Result<(), AppError>;
    pub fn start(
        self: &Arc<Self>,
        state: Arc<AppState>,
        shutdown: CancellationToken,
    ) -> ImportWorkerHandle;
}

pub struct ImportWorkerHandle {
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub struct JobCancellation {
    pub shutdown: CancellationToken,
    pub ownership_lost: CancellationToken,
}

impl ImportWorkerHandle {
    pub async fn shutdown(self, grace: Duration);
}

pub async fn execute_job(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    job: import_job::Model,
    claim: ImportClaim,
    cancellation: JobCancellation,
) -> Result<ImportArtifact, ImportExecutionError>;

pub struct ImportArtifact {
    pub cid: String,
    pub logical_size: u64,
    pub object_content_type: Option<String>,
}
```

CID execution uses `state.kubo` for provider discovery, recursive local pin, and file inspection. URL execution consumes the typed `DownloadStream`, maps non-cancellation errors only through `DownloadError::into_import_failure`, streams into Kubo add, persists separate download/add counters, then recursively pins. Direct publication evaluates `state.pinning.policy()` with the captured tags and `is_decompress_zip = false`, builds `PublicationRequest`, and calls `publish_import_object` with the current `ImportClaim` fence, `Utc::now()`, and `state.pinning.provider_limits()`. Combined publication passes the same `Arc<AppState>` to Task 9 and evaluates policy with `is_decompress_zip = true`. On `DownloadError::Canceled` or `StreamAddError::Canceled`, inspect `JobCancellation`: process shutdown returns `ImportExecutionError::Interrupted` and performs no retry/failure/attempt update so lease recovery can reclaim; ownership loss returns `Superseded` and performs no stale-worker mutation. Convert typed stale ownership to `ImportExecutionError::Superseded`; a stale/expired claim exits without retrying, failing, or superseding the reclaimed job. Convert remaining Kubo/ZIP failures to retryable or terminal `ImportFailure` at one explicit boundary rather than erasing them through generic `AppResult`. Use one progress-consumer task per job to coalesce channel/watch updates at `progress_flush_interval_ms`; phase changes bypass coalescing and every write carries `claim_epoch`. A failed lease renewal or fenced progress write cancels `ownership_lost`, ending active network work promptly without requiring standard S3 handlers to own the coordinator.

- [ ] **Step 4: Integrate lifecycle without changing pinning-worker behavior**

Do not add `ImportCoordinator` to `AppState`, avoiding a construction cycle and changes to every test literal. In `main.rs`, after `AppState::new`, validate `cfg.imports`, construct `SourceDownloader::production`, then construct `Arc<ImportCoordinator>`. Pass `state.clone()` and `imports.clone()` to `GatewayRoute::new`; start the import worker with `imports.start(state.clone(), shutdown.child_token())`. Start import and pinning workers from sibling child tokens, cancel both during shutdown, and await each with the existing 30-second bound. Feature-disabled configuration still builds the coordinator but does not claim jobs. `ImportObjectRoute::submit` calls `coordinator.enabled()` before parsing source work and calls `authorize_url_for_submission` before opening the atomic submission transaction.

- [ ] **Step 5: Run GREEN**

```powershell
cargo test --lib import::worker::tests -- --nocapture
cargo test --lib import::pipeline::tests -- --nocapture
cargo test --lib import::error::tests -- --nocapture
```

Expected: all lifecycle/pipeline/error tests pass; shutdown leaves jobs reclaimable and every publication has access to policy/provider limits.

- [ ] **Step 6: Checkpoint**

Record restart, retry, and shutdown evidence. Do not stage or commit.

---

### Task 9: Reusable ZIP observation and combined atomic import

**Files:**
- Create: `src/import/decompress.rs`
- Modify: `src/zip/extract.rs`
- Modify: `src/s3/route/decompress_zip.rs`
- Modify: `src/store/import/results.rs`

- [ ] **Step 1: Write failing observer and combined-job tests**

Test entry discovery before entry add, prefix claim cancellation, exact target generation capture, progress counters, archive-key collision, entry-level failures, fatal parser/limit failures, one superseded output rolling back the whole combined publication, and unchanged direct decompression output.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib zip::extract::tests::observer_receives_sanitized_key_before_add -- --nocapture
cargo test --lib import::decompress::tests -- --nocapture
```

Expected: missing observer/orchestrator APIs.

- [ ] **Step 3: Add an optional extraction observer while preserving wrappers**

```rust
pub enum ObservedExtractionError<E> {
    Archive(S3Error),
    Observer(E),
}

#[async_trait::async_trait]
pub trait ExtractionObserver: Send {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error>;
    async fn entry_finished(&mut self, entry: &ExtractedEntry) -> Result<(), Self::Error>;
    async fn entry_failed(
        &mut self, key: &str, error: &ExtractFailure,
    ) -> Result<(), Self::Error>;
    async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error>;
}

pub async fn extract_zip_stream_observed<S, E, O>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
    max_decompressed_bytes: u64,
    observer: &mut O,
) -> Result<ExtractOutcome, ObservedExtractionError<O::Error>>
where S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
      E: std::error::Error + Send + Sync + 'static,
      O: ExtractionObserver;
```

Keep `extract_zip_stream` and `extract_zip_stream_with_limit` as no-op-observer wrappers using `Infallible` and map only `ObservedExtractionError::Archive` back to the existing `S3Result`; their public signatures remain unchanged. The import observer uses `type Error = ImportExecutionError`, claims each sanitized key transactionally before its Kubo add, and persists only bounded progress counters. `ObservedExtractionError::Observer(ImportExecutionError::Superseded)` reaches `decompress_import` unchanged, so it cannot become a generic S3/backend failure. The extractor's existing bounded outcome holds successful/failure entries until final publication converts them to result rows; non-terminal status never exposes partial result rows.

- [ ] **Step 4: Implement combined pipeline completion**

```rust
pub async fn decompress_import(
    state: &Arc<AppState>,
    job: &import_job::Model,
    artifact: ImportArtifact,
    claim: &ImportClaim,
    cancel: CancellationToken,
) -> Result<PublicationResult, ImportExecutionError>;
```

Read the locally pinned archive through `stream_cat`, enforce existing ZIP limits, build `ZipPublicationRequest`, construct an `ImportPublicationGuard` from the current claim epoch plus archive/successful entries, and call `publish_import_zip`. Map `ObservedExtractionError::Archive` once into a retryable/terminal `ImportFailure` according to Kubo-stream provenance versus deterministic ZIP errors; propagate `ObservedExtractionError::Observer` unchanged. Do not publish the archive separately. Fatal errors and ownership loss leave previous objects unchanged; successful and failed entry rows become status pages only in the final transaction.

- [ ] **Step 5: Run GREEN and direct decompression regressions**

```powershell
cargo test --lib zip::extract::tests -- --nocapture
cargo test --lib import::decompress::tests -- --nocapture
cargo test --lib s3::route::decompress_zip::tests -- --nocapture
```

Expected: combined tests pass and existing direct/multipart decompression tests remain green.

- [ ] **Step 6: Checkpoint**

Record atomicity and unchanged-decompression evidence. Do not stage or commit.

---

### Task 10: Import XML API and composite S3 route

**Files:**
- Create: `src/import/response.rs`
- Create: `src/s3/route/import_object.rs`
- Create: `src/s3/route/gateway.rs`
- Modify: `src/s3/route/mod.rs`
- Modify: `src/main.rs`
- Modify: `tests/support/decompress.rs`

- [ ] **Step 1: Write failing parser, response, route, and auth tests**

Cover strict 64 KiB XML, one-of CID/URL, unknown/duplicate/DTD/entity rejection, empty POST query value, non-empty GET job ID, duplicate query keys, invalid upload query combinations, SSE rejection, metadata/tag capture, 202/Location/job header, path-bound status lookup, pagination limits/tokens, disabled feature, wrong credentials before DB work, route priority with both import/decompress keys, and standard fallback.

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib s3::route::import_object::tests -- --nocapture
cargo test --lib s3::route::gateway::tests -- --nocapture
cargo test --lib import::response::tests -- --nocapture
```

Expected: missing route/response modules.

- [ ] **Step 3: Implement request and response contracts**

```rust
pub struct ImportObjectRoute {
    state: Arc<AppState>,
    coordinator: Arc<ImportCoordinator>,
}

impl ImportObjectRoute {
    pub fn new(
        state: Arc<AppState>,
        coordinator: Arc<ImportCoordinator>,
    ) -> Self;
    async fn submit(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>>;
    async fn status(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>>;
}

pub struct GatewayRoute {
    imports: ImportObjectRoute,
    decompress: DecompressZipRoute,
}

impl GatewayRoute {
    pub fn new(
        state: Arc<AppState>,
        coordinator: Arc<ImportCoordinator>,
    ) -> Self;
}
```

Implement `S3Route` for both. `GatewayRoute` dispatches import POST/GET first, existing decompression predicates second, and otherwise returns false. Every matched `call` executes `check_access` exactly once before reading job/source data. Render custom XML with quick-xml escaping; never interpolate raw source URLs into XML/errors/logs.

- [ ] **Step 4: Register the sole composite route**

Replace production and test-harness `set_route(DecompressZipRoute::new(state.clone()))` with `set_route(GatewayRoute::new(state.clone(), coordinator.clone()))`. Use `S3Response::with_status(Body::from(xml), StatusCode::ACCEPTED)` for POST, then attach explicit XML content type, `Location`, and `x-ipfs3-import-job-id` headers.

- [ ] **Step 5: Run GREEN and exact fallback checks**

```powershell
cargo test --lib s3::route::import_object::tests -- --nocapture
cargo test --lib s3::route::gateway::tests -- --nocapture
cargo test --lib import::response::tests -- --nocapture
```

Expected: all API/route/auth tests pass; an unrelated query never matches the custom route.

- [ ] **Step 6: Checkpoint**

Record XML snapshots, route predicates, and auth-order evidence. Do not stage or commit.

---

### Task 11: Real-TCP import integration and concurrency acceptance

**Files:**
- Create: `tests/support/import.rs`
- Modify: `tests/support/mod.rs`
- Modify: `tests/integration.rs`

- [ ] **Step 1: Build the signed import harness and write failing scenarios**

The harness must provide:

```rust
pub struct ImportHarness {
    pub endpoint: String,
    pub state: Arc<AppState>,
    pub coordinator: Arc<ImportCoordinator>,
    pub kubo: MockServer,
    pub source: TestHttpsSource,
    pub worker: ImportWorkerHandle,
}

pub async fn post_import(
    harness: &ImportHarness,
    bucket: &str,
    key: &str,
    query: &str,
    xml: &str,
    client_token: Option<&str>,
) -> http::Response<Vec<u8>>;

pub async fn get_import_status(
    harness: &ImportHarness,
    bucket: &str,
    key: &str,
    job_id: &str,
    max_results: Option<u64>,
    continuation_token: Option<&str>,
) -> http::Response<Vec<u8>>;
```

Use real TCP and `tests/support/sigv4.rs::send_sigv4`. The HTTPS source fixture presents a generated certificate trusted only by `ReqwestImportHttpTransport::new(test_download_limits, vec![test_ca])`. Construct the harness coordinator with `SourceDownloader::with_components`, a resolver returning the TLS listener, and a test-only `AddressPolicy` that permits that loopback address; the actual production reqwest transport still performs HTTPS, certificate validation, hostname/SNI validation, redirect rejection, and pinned-address connection. Separate strict-policy integration cases use `StrictPublicAddressPolicy`, assert loopback/forbidden answers fail before transport invocation, and never install the permissive policy in production configuration.

- [ ] **Step 2: Add scenario tests and capture RED**

Add named tests:

```text
test_import_cid_reports_providers_pins_and_publishes
test_import_url_streams_unknown_length_and_enforces_limit
test_import_overwrite_keeps_previous_object_visible_until_publish
test_new_key_is_no_such_key_until_import_publish
test_new_import_supersedes_blocked_old_worker
test_put_copy_delete_and_complete_supersede_blocked_import
test_combined_import_zip_publishes_once_and_pages_results
test_combined_import_zip_fatal_error_preserves_previous_objects
test_import_redirect_and_forbidden_dns_fail_without_fetch_escape
test_import_worker_restart_reclaims_without_double_publication
```

Run one before implementation wiring is complete:

```powershell
cargo test --test integration test_import_cid_reports_providers_pins_and_publishes -- --nocapture
```

Expected: FAIL until route/worker harness integration is complete.

- [ ] **Step 3: Complete only the fixture/wiring required by the tests**

Add deterministic blockpoints before pin completion and publication; use `Notify`/channels instead of sleeps. Capture Kubo call counts and DB row counts so stale-worker assertions prove absence of object/result/lease/provider-job writes, not merely a terminal status string.

- [ ] **Step 4: Run all import integration scenarios**

```powershell
cargo test --test integration test_import_ -- --nocapture
```

Expected: every named import scenario passes with no ignored/filtered accidental omissions.

- [ ] **Step 5: Repeat the stale-worker race**

```powershell
1..20 | ForEach-Object { cargo test --test integration test_new_import_supersedes_blocked_old_worker --quiet; if ($LASTEXITCODE -ne 0) { throw "import race iteration $_ failed" } }
1..20 | ForEach-Object { cargo test --test integration test_put_copy_delete_and_complete_supersede_blocked_import --quiet; if ($LASTEXITCODE -ne 0) { throw "S3 race iteration $_ failed" } }
```

Expected: 40/40 runs pass with zero late publication.

- [ ] **Step 6: Checkpoint**

Record scenario output and DB/Kubo evidence. Do not stage or commit.

---

### Task 12: Full compatibility, diagnostics, and real-surface verification

**Files:**
- Modify only if verification exposes a spec violation; keep fixes within files already listed
- Verify: `docs/superpowers/specs/2026-07-29-ipfs3-import-design.md`
- Verify: `config.example.toml`

- [ ] **Step 1: Run standard S3 regression tests**

```powershell
cargo test --test integration test_create_and_put_and_get_plain_object -- --nocapture
cargo test --test integration test_client_compat_head_nested_key_signed_on_localhost -- --nocapture
cargo test --test integration test_client_compat_get_bucket_location_is_standard_us_east_1 -- --nocapture
cargo test --test integration test_client_compat_list_v1_delimiter_marker_pages_without_replay -- --nocapture
cargo test --test integration test_standard_put_sse_s3_still_succeeds -- --nocapture
cargo test --test integration test_standard_put_sse_c_still_succeeds -- --nocapture
cargo test --test integration test_standard_multipart_signed_still_succeeds -- --nocapture
```

Expected: all existing tests pass unchanged and import mock counters remain zero for nonmatching requests.

- [ ] **Step 2: Run static and complete automated gates**

```powershell
cargo fmt -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib
cargo test --test integration
```

Expected: every command exits 0; no test is newly ignored and no `.only`/equivalent filtering exists.

- [ ] **Step 3: Run diagnostics on every changed Rust file**

Use `lsp_diagnostics` for all modified/created `.rs` paths. Expected: zero new errors or warnings.

- [ ] **Step 4: Exercise the signed real surface**

Start the existing test gateway/Kubo stack or dedicated import acceptance harness. Send a SigV4-signed raw POST with CID XML, poll signed GET until completed, and fetch the object through the ordinary S3 client. Repeat with an allowlisted HTTPS URL and combined `decompress-zip`; capture request/response XML and object/result assertions in a temporary evidence log outside the repository or under an existing ignored evidence location. Stop every server and worker afterward.

Binary pass conditions:

```text
POST status = 202 and returns one job ID
GET status progresses without source URL disclosure
ordinary GET returns old object or NoSuchKey while running
terminal completed object CID/size match the job result
combined result pages cover every success/failure exactly once
all spawned listeners/workers are shut down
```

- [ ] **Step 5: Inspect scope and repository state**

```powershell
git status --short
git diff --check
git diff --stat
```

Expected: only spec-approved implementation, tests, config example, spec, and plan files are present; `git diff --check` is silent. These are read-only Git commands.

- [ ] **Step 6: Final checkpoint without commit**

Report changed files grouped by the 12 tasks, RED/GREEN evidence, full-suite results, real-surface artifact path, and any residual operational risk. Do not stage or commit.

---

## Dependency waves

```text
Wave 1: Task 1 (config/model)
Wave 2: Task 2 (schema/entities)
Wave 3: Task 3 (job store) + Task 6 (Kubo progress) + Task 7 (downloader)
Wave 4: Task 4 (ownership/publication guard)
Wave 5: Task 5 (standard mutation integration) + Task 8 (worker/pipelines)
Wave 6: Task 9 (combined decompression)
Wave 7: Task 10 (route/API)
Wave 8: Task 11 (real-TCP integration)
Wave 9: Task 12 (full acceptance)
```

Tasks in the same wave may use separate implementation agents only when they do not edit the same files. Task 4 must finish before Tasks 5, 8, or 9 can claim publication safety. Task 10 must finish before Task 11.

## Plan self-review

### Spec coverage

- API, SigV4, XML, idempotency, 202, status pagination: Tasks 1, 4, 10, 11.
- Exact composite routing and standard fallback: Tasks 10, 12.
- CID providers/pin/inspection and truthful counters: Tasks 6, 8, 11.
- URL allowlist, DNS/SSRF, redirects, limits, streaming: Tasks 1, 7, 8, 11.
- Durable jobs, retries, restart, retention: Tasks 2, 3, 8, 11.
- Exact generations, prefixes, extracted targets, stale fencing: Tasks 2-5, 9, 11.
- Combined decompression and atomic result publication: Tasks 4, 9, 11.
- Standard S3 non-regression and existing SSE behavior: Tasks 5, 10, 12.
- No import SSE, no partial visibility, no automatic pin removal: Tasks 8-12 and acceptance assertions.

### Placeholder scan

The plan contains no placeholder markers, deferred implementation steps, empty test bodies, or abbreviated function signatures. Every code-changing task names concrete files, interfaces, tests, commands, and binary expected outcomes.

### Type consistency

- `ImportConfig` validates to `ValidatedImportConfig`, consumed by `ImportCoordinator` and `SourceDownloader`.
- `ImportProgress` fields match persisted columns and status XML.
- `ImportPublicationGuard` contains the exact worker/claim epoch and `ExpectedImportTarget` values produced by ownership claims; publication verifies both lease and destination fences in one transaction.
- `ImportArtifact` is produced by CID/URL pipeline and consumed by optional decompression or direct guarded publication.
- `DownloadError` remains typed through resolver, address policy, transport, and response-body streams before one explicit conversion to `ImportFailure`.
- `ObservedExtractionError<ImportExecutionError>` preserves ownership supersession separately from ZIP/backend failures.
- Existing standard `stream_add`, `pin_add`, and publication API signatures remain stable through wrappers; only import-specific consumers use progress/guard variants.

### No-commit compliance

Every task ends in a checkpoint. No Git write command is included. The only Git commands in this plan are final read-only inspection commands.
