# Multi-Provider Pinning Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add durable asynchronous Filebase/Pinata remote pinning, standard S3 object-tag control, ordered automatic/manual lease policies, multi-provider coordination, soft quota eviction, and crash-safe lifecycle processing without changing local Kubo pin safety.

**Architecture:** Keep the synchronous axum → s3s → `S3Impl` path responsible only for validation, Kubo publication, and one atomic SeaORM transaction that stores the object, tags, leases, targets, quota reservations, and outbox jobs. A pure policy evaluator produces normalized lease intents, while an in-process `PinningCoordinator` owns provider clients and a cancellation-aware persistent worker that performs every remote HTTP call after commit. Split provider, policy, worker, quota, publication, and tag responsibilities into focused modules; do not add strategy or provider logic to the already-large `src/s3/ops/object.rs` and `src/s3/ops/multipart.rs` files.

**Tech Stack:** Rust 2024 (MSRV 1.92), axum 0.8, s3s 0.14, tokio/tokio-util, reqwest 0.13, SeaORM/SeaORM Migration 1.x, SQLite and PostgreSQL builders, serde/serde_json/toml, chrono, rand, wiremock, rust-s3, SigV4 real-TCP tests.

---

## Global Constraints and Invariants

- The approved contract is `docs/superpowers/specs/2026-07-21-multi-provider-pinning-service-design.md`; do not edit it.
- Remote provider HTTP is never called in an S3 request transaction or before an S3 publication response.
- Do not introduce Redis, Kafka, an external queue, a provider upload API, or a separate worker service. `pin_jobs` is the durable outbox.
- Do not use provider-native TTL. Lease expiry, cancellation, overwrite, deletion, and eviction are gateway-owned database state transitions.
- Do not physically delete S3 object rows/content for remote lifecycle events. Do not call Kubo `pin_rm` from remote lease, target, quota, expiry, renewal, or worker code.
- `UploadPart` remains locally added/pinned only and never creates a remote target or `submit` job.
- Repeated CIDs reserve `max_bytes`/`max_pins` once per provider. Usage is released only after remote unpin succeeds or returns NotFound.
- Lease generations guard target-scoped submit/poll work; a separate monotonically increasing `(provider,cid)` remote epoch guards remote-scoped reconcile/unpin work. A completed DELETE never releases quota if the epoch or desired-reference set changed while the request was in flight.
- `remote_pins`, not an arbitrary lease target, owns provider request ID/status. Every remote status observation is transactionally projected to all current desired targets sharing `(provider,cid)`; the deterministic canonical worker owner is the lowest `(created_at,target_id)` current target.
- A queued/pinning Poll reuses and reschedules one stable Poll row in place at a bounded interval. It does not insert a duplicate stable ID and is not completed until pinned, failed, stale/no-longer-needed, or explicit coordination takeover.
- A manual lease may reactivate from `expired` only during the narrow pre-release race: the same latest owner and preserved original targets must still have at least one capacity-holding/request-bearing/recoverable-Unpin remote. Confirmed absence, cancellation, and eviction never create a new post-expiry lease or reconstructed decompressed target set.
- Provider-reported `failed` state has one durable retry budget per `(provider,cid)`, not per target. Duplicate failure observations are idempotent, retries stop after eight failed requests, and only a genuinely new target or generation-advancing manual extension/reactivation resets the exhausted budget.
- A Submit that may have reached a provider is never treated as absent from request-ID state alone. Reclaimed/ambiguous Submit work must query by stable CID+metadata before any second POST, and quota remains reserved until that ambiguity resolves and any adopted request is unpinned.
- Provider tokens are loaded only from named environment variables, are never persisted, and are redacted from `Debug`, errors, request logging, and tracing fields.
- Ordinary S3 tags round-trip unchanged. Reserved `ipfs-s3:*` tags use normal replacement semantics and additionally control only the manual lease.
- Automatic and manual leases are independent. Cancelling manual tags cannot shorten, cancel, or replace an automatic lease.
- Work is limited to this approved v0.4 feature. Do not add master-key rotation, object versioning, lifecycle rules, IPFS Cluster, multi-node behavior, or any v0.5+ feature.
- Every task follows RED → minimal implementation → GREEN. Do not install software or pull images.
- Git writes are guarded. A commit boundary is advisory; only the orchestrator, and only after explicit user authorization in the active conversation, may run `git add`/`git commit`. Implementation workers must not run Git write commands.

## File Structure

### Create

- `src/pinning/config.rs` — validate raw pinning configuration, resolve token environment references into redacted runtime secrets, normalize durations, and construct provider definitions.
- `src/pinning/provider.rs` — define the provider trait, normalized remote states, request/response DTOs, and classified/redacted provider errors.
- `src/pinning/psa.rs` — implement shared PSA `/pins` HTTP serialization, bearer authentication, status normalization, timeout/conflict classification, and list-based reconciliation queries.
- `src/pinning/pinata.rs` — construct the Pinata PSA client with its default base URL and provider defaults.
- `src/pinning/filebase.rs` — construct the Filebase PSA client with its default base URL and provider defaults.
- `src/pinning/tags.rs` — validate S3 tag limits, decode/encode `x-amz-tagging`, and normalize the four reserved control tags.
- `src/pinning/policy.rs` — pure ordered first-match evaluator producing independent automatic/manual lease intents without database/network I/O.
- `src/pinning/coordinator.rs` — provide the compile-ready policy/provider/limit/settings foundation in Task 6, then add priority/health runtime, one/all coordination, rate bounds, and worker startup in Tasks 12–13.
- `src/pinning/worker.rs` — claim persistent jobs, execute submit/find/get/unpin/reconcile, retry with backoff, poll status, recover locks, and honor cancellation.
- `src/pinning/quota.rs` — hold pure quota decisions and oldest-pin candidate ordering used by the transactional store layer.
- `src/store/migrations/m20260721_000001_multi_provider_pinning.rs` — reversible six-table/object-tag migration plus multipart tag storage, indexes, constraints, SQLite rollback tests, and PostgreSQL SQL-builder tests.
- `src/store/entities/object_tag.rs` — SeaORM mapping for immutable-object tags.
- `src/store/entities/pin_lease.rs` — SeaORM mapping for automatic/manual leases and generations.
- `src/store/entities/pin_lease_target.rs` — SeaORM mapping for lease/CID/provider targets.
- `src/store/entities/remote_pin.rs` — SeaORM mapping for unique provider/CID remote state, request identity, monotonic remote epoch, and durable failed-request retry budget.
- `src/store/entities/pin_job.rs` — SeaORM mapping for durable target/remote-scoped outbox jobs, lease/remote generation guards, Submit recovery phase, and recoverable locks.
- `src/store/entities/pin_provider_usage.rs` — SeaORM mapping for local reservations and advisory provider observations.
- `src/store/pinning/mod.rs` — export focused pinning persistence modules and shared transaction input/output types.
- `src/store/pinning/tags.rs` — replace/list object tags and persist normalized multipart-upload tags.
- `src/store/pinning/leases.rs` — create, narrowly reactivate/renew, cancel, expire, evict, generation-check, and atomically project shared remote status across leases/targets.
- `src/store/pinning/jobs.rs` — construct scope-safe target/remote jobs, derive stable IDs, persist Submit recovery phases, enqueue/ensure/reactivate canonical work, atomically distinguish fresh/reclaimed claims, reschedule Poll/Reconcile work, retry, complete, and unlock jobs.
- `src/store/pinning/quota.rs` — reserve unique provider/CID usage, choose eviction candidates, retain capacity through Submit ambiguity/failed retries, release confirmed capacity, and wake waiters.
- `src/store/pinning/publication.rs` — atomically publish/replace/delete normal, multipart, copy, ordinary ZIP, and multipart ZIP objects with tags, leases, targets, usage, jobs, and upload/part finalization.
- `src/s3/ops/tagging.rs` — implement GetObjectTagging, PutObjectTagging, and DeleteObjectTagging against the transactional tag/lease store API.
- `tests/support/pinning.rs` — configurable real-TCP gateway/provider harness, scripted PSA responses, worker clock controls, and remote/Kubo request assertions.

### Modify

- `Cargo.toml` — in Task 12, enable `tokio-util`'s `rt` feature before `CancellationToken` is compiled and Tokio's existing `test-util` feature for paused-clock Poll/retry tests; add no new crate.
- `src/config.rs` — replace the single `pinning.provider` string with raw provider/policy/worker configuration arrays while keeping empty pinning as the no-op default.
- `src/error.rs` — map normalized tag/policy request failures to S3 `InvalidArgument` without exposing provider diagnostics.
- `src/pinning/mod.rs` and `src/pinning/noop.rs` — export the new modules and replace the old CID-only service with the normalized no-op provider contract.
- `src/store/migrations/mod.rs`, `src/store/entities/mod.rs`, and `src/store/mod.rs` — register the migration/entities/pinning store tree and make `Store` cheaply cloneable.
- `src/store/object.rs` — expose transaction-local latest-row replacement/deletion primitives that return the prior object ID; keep object persistence free of policy logic.
- `src/store/multipart.rs` and `src/store/entities/multipart_upload.rs` — persist create-time normalized tags and include publication state in the Complete transaction.
- `src/state.rs` — validate pinning configuration, build the coordinator/provider registry, and expose it through `AppState`.
- `src/s3/ops/mod.rs`, `src/s3/handler.rs` — register standard object-tag operations.
- `src/s3/ops/object.rs` — preflight tags/policy and pass publication/delete inputs to the store facade for Put/Copy/Delete/DeleteObjects only.
- `src/s3/ops/multipart.rs` — persist Create tags, keep UploadPart remote-free, and pass Complete publication inputs to the store facade.
- `src/s3/route/decompress_zip.rs` — stage extraction results, then atomically publish archive/successful entries and archive-owned decompressed targets.
- `src/main.rs` — start the persistent worker after migrations/configuration, share a cancellation token with HTTP shutdown, and wait for bounded worker drain.
- `tests/support/mod.rs`, `tests/support/decompress.rs`, and `tests/integration.rs` — compose the pinning harness with existing signed S3/Kubo coverage and add the full real-surface matrix.
- `README.md`, `config.example.toml`, `config.docker.toml`, `.env.example`, `docker-compose.yml`, and `ROADMAP.md` — document asynchronous semantics, provider env references, quotas/eviction, shutdown, and v0.4 completion only after all gates pass.

### Read-Only During Implementation

- `docs/superpowers/specs/2026-07-21-multi-provider-pinning-service-design.md` — authoritative product behavior.
- `src/kubo/pin.rs` — local Kubo pin behavior must not be changed by this feature.
- `Cargo.lock` — use already-resolved crates; no dependency installation.

---

### Task 1: Validated Configuration, Durations, and Redacted Secrets

**Files:**
- Modify: `src/config.rs`
- Create: `src/pinning/config.rs`
- Modify: `src/pinning/mod.rs`

**Interfaces:**
- Produce `PinningConfig`, `ProviderConfig`, `PolicyConfig`, `ProviderKind`, `PolicyTrigger`, `ProviderMode`, `LeaseDuration`, `ValidatedPinningConfig`, `ValidatedProvider`, and `ValidatedPolicy`.
- Produce `ValidatedPinningConfig::from_raw<F>(raw: &PinningConfig, get_env: F) -> anyhow::Result<Self> where F: Fn(&str) -> Option<String>`.

- [ ] **Step 1: Write RED tests for defaults, duration grammar, all startup failures, and token redaction**

Add table-driven tests in `src/pinning/config.rs` that parse the approved TOML and assert exact failures:

```rust
#[test]
fn duration_accepts_positive_integer_and_s_m_h_d_only() {
    for (raw, seconds) in [("1s", 1), ("5m", 300), ("2h", 7200), ("30d", 2_592_000)] {
        assert_eq!(LeaseDuration::parse(raw).unwrap().as_seconds(), seconds);
    }
    for raw in ["", "0s", "-1s", "1.5h", "1w", " 1h", "1h "] {
        assert!(LeaseDuration::parse(raw).is_err(), "{raw}");
    }
}

#[test]
fn validation_rejects_every_fail_fast_configuration_class() {
    let cases = [
        (fixture_duplicate_names(), "duplicate pinning provider name"),
        (fixture_unknown_kind(), "unknown pinning provider kind"),
        (fixture_missing_token_env(), "missing provider token environment variable"),
        (fixture_zero_bytes(), "max_bytes must be positive"),
        (fixture_zero_pins(), "max_pins must be positive"),
        (fixture_invalid_duration(), "invalid duration"),
        (fixture_empty_provider_set(), "must reference at least one enabled provider"),
        (fixture_unknown_provider(), "unknown provider reference"),
        (fixture_default_exceeds_max(), "default_duration exceeds max_duration"),
    ];
    for (raw, message) in cases {
        let error = ValidatedPinningConfig::from_raw(&raw, |name| {
            (name == "PINATA_JWT").then(|| "secret-token".to_owned())
        })
        .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert!(!format!("{error:?}").contains("secret-token"));
    }
}

#[test]
fn empty_pinning_configuration_is_a_valid_noop_default() {
    let validated = ValidatedPinningConfig::from_raw(&PinningConfig::default(), |_| None).unwrap();
    assert!(validated.providers.is_empty());
    assert!(validated.policies.is_empty());
}
```

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::config::tests -- --nocapture
```

Expected: compile failure because the new configuration types and validator are absent.

- [ ] **Step 3: Implement the raw and validated models with exact defaults**

Replace the old `PinningConfig { provider: String }` with these serializable shapes and put runtime-only types in `src/pinning/config.rs`:

```rust
#[derive(Debug, Deserialize, Clone)]
pub struct PinningConfig {
    #[serde(default = "default_worker_interval")]
    pub worker_interval: String,
    #[serde(default = "default_worker_concurrency")]
    pub worker_concurrency: usize,
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    #[serde(default)]
    pub policies: Vec<PolicyConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub name: String,
    pub kind: String,
    pub token_env: Option<String>,
    pub endpoint: Option<String>,
    #[serde(default = "enabled")]
    pub enabled: bool,
    pub priority: u32,
    pub max_bytes: u64,
    pub max_pins: u64,
    pub requests_per_second: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PolicyConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    pub trigger: String,
    pub provider_mode: String,
    pub providers: Vec<String>,
    pub default_duration: String,
    pub max_duration: String,
    #[serde(default)]
    pub allow_decompressed: bool,
}

fn default_worker_interval() -> String { "5s".to_owned() }
fn default_worker_concurrency() -> usize { 4 }
fn enabled() -> bool { true }

impl Default for PinningConfig {
    fn default() -> Self {
        Self {
            worker_interval: default_worker_interval(),
            worker_concurrency: default_worker_concurrency(),
            providers: Vec::new(),
            policies: Vec::new(),
        }
    }
}
```

Use non-zero checked multiplication in `LeaseDuration::parse`, reject whitespace/sign/decimal/unknown units, require `worker_interval > 0`, `worker_concurrency > 0`, `requests_per_second > 0` when present, exact bucket or `*`, and non-empty provider names. Preserve policy order and assign identity `policy:<index>:<sha256>` where the digest is computed from the canonical bucket/prefix/trigger/mode/provider-list/default/max/decompressed tuple using the already-present `sha2` crate; reordering or changing a rule therefore makes an older lease policy identity explicitly unknown rather than silently binding it to another rule. Sort each validated policy's providers by `(priority, name)` while leaving rules themselves ordered.

Store a token in a wrapper whose output is always redacted:

```rust
#[derive(Clone)]
pub struct SecretToken(String);

impl SecretToken {
    pub(crate) fn expose(&self) -> &str { &self.0 }
}

impl std::fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretToken([REDACTED])")
    }
}
```

Normalize string discriminants and provider limits into these runtime types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind { Pinata, Filebase, Noop }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyTrigger { Always, Request }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMode { One, All }

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseDuration(u64);

impl LeaseDuration {
    pub fn parse(raw: &str) -> anyhow::Result<Self>;
    pub fn as_seconds(self) -> u64 { self.0 }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderLimits {
    pub priority: u32,
    pub max_bytes: i64,
    pub max_pins: i64,
    pub enabled: bool,
}

pub type ProviderLimitMap = std::collections::BTreeMap<String, ProviderLimits>;
```

Require `token_env` and a non-empty environment value for `pinata`/`filebase`; reject `token_env` on `noop`. Accept exactly `pinata`, `filebase`, and `noop`. Reject quotas or durations that exceed `i64::MAX` before database/chrono conversion. Validate duplicate names before resolving any token. Never include the env value in an `anyhow` context.

- [ ] **Step 4: Run GREEN and the existing config regression suite**

Run:

```powershell
cargo test --lib pinning::config::tests -- --nocapture
cargo test --lib config::tests -- --nocapture
```

Expected: PASS; valid Filebase/Pinata/Noop definitions normalize, every approved startup error fails deterministically, and no formatted value contains a token.

- [ ] **Step 5: Record the review boundary**

Review only `src/config.rs`, `src/pinning/config.rs`, and `src/pinning/mod.rs`. If the user explicitly authorizes a commit, the orchestrator may commit these files as `feat: validate pinning configuration`; otherwise record Task 1 complete without a Git write.

---

### Task 2: PSA Provider Contract and Filebase/Pinata/Noop Clients

**Files:**
- Create: `src/pinning/provider.rs`
- Create: `src/pinning/psa.rs`
- Create: `src/pinning/pinata.rs`
- Create: `src/pinning/filebase.rs`
- Modify: `src/pinning/noop.rs`
- Modify: `src/pinning/mod.rs`

**Interfaces:**
- Produce the approved `PinningProvider` methods and normalized `RemotePinStatus::{Queued, Pinning, Pinned, Failed}`.
- Preserve PSA `requestid`; classify authentication, not-found, conflict/ambiguous submit, rate limit with `Retry-After`, quota, transient, terminal, and protocol failures.

- [ ] **Step 1: Write RED contract tests for both base paths, auth, serialization, statuses, and errors**

Use one table for Pinata (`/psa/pins`) and Filebase (`/v1/ipfs/pins`). Each case mounts wiremock and asserts the request body contains stable metadata but no token in diagnostics:

```rust
#[tokio::test]
async fn pinata_and_filebase_obey_the_same_psa_contract() {
    for (kind, prefix) in [(ProviderKind::Pinata, "/psa"), (ProviderKind::Filebase, "/v1/ipfs")] {
        let server = wiremock::MockServer::start().await;
        mount_psa_contract(&server, prefix, "request-7").await;
        let provider = provider_for_test(kind, &server, "provider-token");
        let submitted = provider.submit(SubmitPin {
            cid: "bafy-target".to_owned(),
            name: "bucket/key".to_owned(),
            metadata: std::collections::BTreeMap::from([
                ("gateway_job_id".to_owned(), "job-7".to_owned()),
                ("gateway_lease_id".to_owned(), "lease-7".to_owned()),
            ]),
        }).await.unwrap();
        assert_eq!(submitted.request_id, "request-7");
        assert_eq!(submitted.status, RemotePinStatus::Queued);
        assert_eq!(provider.get("request-7").await.unwrap().status, RemotePinStatus::Pinned);
        assert_eq!(provider.find(FindPin::for_job("bafy-target", "job-7")).await.unwrap().len(), 1);
        provider.unpin("request-7").await.unwrap();
    }
}

#[tokio::test]
async fn provider_errors_are_classified_without_secret_material() {
    for (status, class) in [
        (401, ProviderErrorClass::Authentication),
        (403, ProviderErrorClass::Authentication),
        (409, ProviderErrorClass::Ambiguous),
        (429, ProviderErrorClass::RateLimited),
        (507, ProviderErrorClass::Quota),
        (500, ProviderErrorClass::Transient),
    ] {
        let error = submit_error(status, Some("17"), "provider-token").await;
        assert_eq!(error.class, class);
        if status == 429 { assert_eq!(error.retry_after, Some(std::time::Duration::from_secs(17))); }
        assert!(!format!("{error:?}").contains("provider-token"));
    }
}
```

Add separate malformed JSON, unknown status, delayed timeout, unpin 404, exact bearer header, GET/list/DELETE path, and no-requestid fabrication tests.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::psa::tests -- --nocapture
cargo test --lib pinning::noop::tests -- --nocapture
```

Expected: compile failure because the normalized contract and PSA client do not exist.

- [ ] **Step 3: Define the provider types and error contract**

Implement exact public types in `provider.rs`:

```rust
#[async_trait::async_trait]
pub trait PinningProvider: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError>;
    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError>;
    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError>;
    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitPin {
    pub cid: String,
    pub name: String,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindPin {
    pub cid: String,
    pub metadata: std::collections::BTreeMap<String, String>,
}

impl FindPin {
    pub fn for_job(cid: &str, job_id: &str) -> Self {
        Self {
            cid: cid.to_owned(),
            metadata: std::collections::BTreeMap::from([
                ("gateway_job_id".to_owned(), job_id.to_owned()),
            ]),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePinStatus { Queued, Pinning, Pinned, Failed }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePin {
    pub request_id: String,
    pub cid: String,
    pub status: RemotePinStatus,
    pub raw_status: String,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorClass {
    Authentication,
    NotFound,
    Ambiguous,
    RateLimited,
    Quota,
    Transient,
    Terminal,
    Protocol,
}

#[derive(Debug, thiserror::Error)]
#[error("provider request failed: class={class:?}, message={message}")]
pub struct ProviderError {
    pub class: ProviderErrorClass,
    pub message: String,
    pub retry_after: Option<std::time::Duration>,
}
```

- [ ] **Step 4: Implement shared PSA wire behavior and provider constructors**

`PsaClient::submit` sends `POST {base}/pins` with this shape, and response decoding always requires a non-empty `requestid`:

```rust
#[derive(serde::Serialize)]
struct PsaPin<'a> {
    cid: &'a str,
    name: &'a str,
    origins: Vec<String>,
    meta: &'a std::collections::BTreeMap<String, String>,
}

#[derive(serde::Deserialize)]
struct PsaStatus {
    requestid: String,
    status: String,
    pin: PsaResponsePin,
    #[serde(default)]
    info: std::collections::BTreeMap<String, String>,
}
```

Send `PsaPin` itself as the POST body; do not wrap it in another `pin` field. Use `GET {base}/pins/{requestid}`, `DELETE {base}/pins/{requestid}`, and `GET {base}/pins` with `cid=<cid>` plus `meta=<compact JSON object>`; for job reconciliation the decoded metadata parameter is exactly `{"gateway_job_id":"job-7"}`. Build the query with reqwest pairs so brackets/quotes are percent-encoded once. Decode list responses as `{ count, results }`. Normalize only `queued`, `pinning`, `pinned`, and `failed`; an unknown value is `Protocol`. Treat unpin 404 as `Ok(())`. Parse delta-seconds `Retry-After`; keep the error transient/rate-limited if the header is malformed. Classify HTTP 507 as `Quota`, 401/403 as `Authentication`, 409 submit as `Ambiguous`, 429 as `RateLimited`, 5xx as `Transient`, other 4xx as `Terminal`, and malformed success bodies as `Protocol`. A timeout during submit is `Ambiguous`; other transport failures are `Transient`. Never include response/request bodies in errors; retain a bounded, token-free status/reason string.

Construct wrappers with exact defaults:

```rust
pub const PINATA_BASE_URL: &str = "https://api.pinata.cloud/psa";
pub const FILEBASE_BASE_URL: &str = "https://api.filebase.io/v1/ipfs";

pub fn build_pinata(name: String, token: SecretToken, endpoint: Option<String>) -> PsaClient {
    PsaClient::new(name, endpoint.unwrap_or_else(|| PINATA_BASE_URL.to_owned()), token)
}

pub fn build_filebase(name: String, token: SecretToken, endpoint: Option<String>) -> PsaClient {
    PsaClient::new(name, endpoint.unwrap_or_else(|| FILEBASE_BASE_URL.to_owned()), token)
}
```

Replace `NoopPinningService` with `NoopProvider`; submit returns `request_id = format!("noop:{}", request.cid)`, status `Pinned`, get/find return deterministic pinned records, and unpin succeeds without network access.

- [ ] **Step 5: Run GREEN**

Run:

```powershell
cargo test --lib pinning::psa::tests -- --nocapture
cargo test --lib pinning::pinata::tests -- --nocapture
cargo test --lib pinning::filebase::tests -- --nocapture
cargo test --lib pinning::noop::tests -- --nocapture
```

Expected: PASS; both provider base paths, bearer auth, submit/get/find/delete, request ID retention, status normalization, 401/403/404/409/429/5xx/malformed/timeout behavior, and token redaction are observable.

- [ ] **Step 6: Record the review boundary**

Review the six pinning provider files. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: add PSA pinning providers`; otherwise record Task 2 complete without a Git write.

---

### Task 3: Reversible Persistence Schema and SeaORM Entities

**Files:**
- Create: `src/store/migrations/m20260721_000001_multi_provider_pinning.rs`
- Create: `src/store/entities/object_tag.rs`
- Create: `src/store/entities/pin_lease.rs`
- Create: `src/store/entities/pin_lease_target.rs`
- Create: `src/store/entities/remote_pin.rs`
- Create: `src/store/entities/pin_job.rs`
- Create: `src/store/entities/pin_provider_usage.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/entities/mod.rs`
- Modify: `src/store/entities/multipart_upload.rs`
- Modify: `src/store/mod.rs`

**Interfaces:**
- Add `object_tags`, `pin_leases`, `pin_lease_targets`, `remote_pins`, `pin_jobs`, `pin_provider_usage`, and `multipart_uploads.tags_json`.
- Map every timestamp as `DateTimeUtc`, byte/count/lease-generation/remote-epoch values as checked `i64`, IDs/CIDs/provider names/state discriminants as `String`, and nullable diagnostics/request IDs as `Option<String>`.
- Persist `remote_pins.epoch` separately from `pin_jobs.expected_generation`; `pin_jobs.expected_remote_epoch` is the exclusive guard for remote-scoped Unpin/Reconcile jobs.
- Persist failed-request retry ownership/idempotency on `remote_pins.failure_attempts`/`next_retry_at`/`last_failed_request_id`, and Submit uncertainty on `pin_jobs.submit_phase`; neither may be inferred from attempt counters or error text.

- [ ] **Step 1: Write migration RED tests before registration**

Seed a bucket, object, multipart upload, and part under the first three migrations. Run the new migration up/down and assert all seeded values and the multipart cascade survive. Add PostgreSQL builder assertions for every table/index plus the multipart alter:

```rust
#[tokio::test]
async fn sqlite_up_down_preserves_existing_object_upload_part_and_foreign_keys() {
    let db = seeded_pre_pinning_database().await;
    let before = legacy_snapshot(&db).await;
    let manager = sea_orm_migration::SchemaManager::new(&db);
    Migration.up(&manager).await.unwrap();
    assert_eq!(pinning_table_names(&db).await, vec![
        "object_tags", "pin_jobs", "pin_lease_targets", "pin_leases",
        "pin_provider_usage", "remote_pins",
    ]);
    assert_eq!(multipart_tags_json(&db, "upload-1").await, "[]");
    Migration.down(&manager).await.unwrap();
    assert_eq!(legacy_snapshot(&db).await, before);
    assert_multipart_part_cascade(&db, "upload-1").await;
}

#[test]
fn postgres_builders_cover_all_tables_constraints_indexes_and_rollback() {
    let up = up_statements().into_iter()
        .map(|statement| statement.to_string(PostgresQueryBuilder))
        .collect::<Vec<_>>();
    let down = down_statements().into_iter()
        .map(|statement| statement.to_string(PostgresQueryBuilder))
        .collect::<Vec<_>>();
    for table in ["object_tags", "pin_leases", "pin_lease_targets", "remote_pins", "pin_jobs", "pin_provider_usage"] {
        assert!(up.iter().any(|sql| sql.contains(&format!("\"{table}\""))), "{table}");
        assert!(down.iter().any(|sql| sql.contains(&format!("\"{table}\""))), "{table}");
    }
    assert!(up.iter().any(|sql| sql.contains("UNIQUE") && sql.contains("object_id") && sql.contains("key")));
    assert!(up.iter().any(|sql| sql.contains("provider") && sql.contains("cid")));
    assert!(up.iter().any(|sql| sql.contains("next_attempt_at")));
    assert!(up.iter().any(|sql| sql.contains("expected_remote_epoch")));
    assert!(up.iter().any(|sql| sql.contains("failure_attempts") && sql.contains("next_retry_at") && sql.contains("last_failed_request_id")));
    assert!(up.iter().any(|sql| sql.contains("submit_phase")));
    assert!(up.iter().any(|sql| sql.contains("CHECK") && sql.contains("operation") && sql.contains("expected_generation")));
    assert!(up.iter().any(|sql| sql.contains("CHECK") && sql.contains("epoch") && sql.contains(">= 1")));
    assert!(up.iter().any(|sql| sql.contains("CHECK") && sql.contains("failure_attempts") && sql.contains(">= 0")));
}
```

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib store::migrations::m20260721_000001_multi_provider_pinning -- --nocapture
```

Expected: compile failure because the migration, builders, and helpers do not exist.

- [ ] **Step 3: Implement exact tables, constraints, indexes, and reverse order rollback**

Build backend-neutral SeaQuery statements with these exact columns and constraints:

```text
object_tags: object_id FK objects(id) ON DELETE CASCADE, key TEXT, value TEXT,
             PRIMARY KEY(object_id, key)
pin_leases: id PK, owner_object_id FK objects(id), source, policy_id, provider_mode,
            content_mode, created_at, last_touched_at, expires_at, generation BIGINT,
            state, UNIQUE(owner_object_id, source)
pin_lease_targets: id PK, lease_id FK pin_leases(id) ON DELETE CASCADE, cid,
                   logical_size BIGINT, provider, state, created_at, last_touched_at,
                   UNIQUE(lease_id, cid, provider)
remote_pins: provider, cid, request_id NULL, cid_size BIGINT, status, epoch BIGINT NOT NULL,
              failure_attempts INTEGER NOT NULL DEFAULT 0, next_retry_at NULL,
              last_failed_request_id NULL,
              last_touched_at, last_error_class NULL, last_error_text NULL,
              PRIMARY KEY(provider, cid)
pin_jobs: id PK, operation, provider, cid, lease_id NULL, target_id NULL,
           expected_generation NULL, expected_remote_epoch NULL, state, attempts INTEGER, next_attempt_at,
           locked_until NULL, submit_phase NULL, last_error NULL, created_at, updated_at
pin_provider_usage: provider PK, reserved_bytes BIGINT, reserved_pins BIGINT,
                    observed_bytes NULL, observed_pins NULL, observed_at NULL
multipart_uploads: add tags_json TEXT NOT NULL DEFAULT '[]'
```

Allow lease states `active`, `expired`, `cancelled`, and `evicted`; target states `waiting`, `submitted`, `pinned`, `degraded`, `quota_waiting`, `quota_blocked`, `evicted`, and `released`; remote states `reserved`, `queued`, `pinning`, `pinned`, `failed`, and `absent`; job states `pending`, `running`, and `done`; and Submit phases `ready`, `calling`, `recovering`, and `recovery_backoff`. Add CHECK constraints for these discriminants, `CHECK (epoch >= 1)`, and `CHECK (failure_attempts >= 0)` to `remote_pins`. `next_retry_at` is nullable because pinned and exhausted remotes have no due retry. Add these backend-compatible `pin_jobs` CHECK expressions through SeaQuery so both SQLite execution and PostgreSQL rendering enforce one unambiguous scope and recovery owner:

```sql
CHECK (
  (operation IN ('submit', 'poll')
   AND lease_id IS NOT NULL AND target_id IS NOT NULL
   AND expected_generation IS NOT NULL AND expected_remote_epoch IS NULL)
  OR
  (operation IN ('unpin', 'reconcile')
   AND lease_id IS NULL AND target_id IS NULL
   AND expected_generation IS NULL AND expected_remote_epoch IS NOT NULL)
)

CHECK (
  (operation = 'submit' AND submit_phase IN ('ready', 'calling', 'recovering', 'recovery_backoff'))
  OR
  (operation <> 'submit' AND submit_phase IS NULL)
)
```

Add indexes on `(pin_jobs.state, pin_jobs.next_attempt_at, pin_jobs.locked_until)`, `(remote_pins.provider, remote_pins.last_touched_at)`, `(pin_lease_targets.provider, pin_lease_targets.cid, pin_lease_targets.state)`, and `(pin_leases.state, pin_leases.expires_at)`. Down drops indexes/tables in child-first order, then drops `tags_json` without rebuilding `multipart_uploads`.

Register the migration after `SseCKeyFingerprintMigration` and export all six entity modules. Map `remote_pins` with a composite primary key and `object_tags` with its composite primary key. Keep state/source/mode values as strings so SQLite and PostgreSQL share one schema contract.

- [ ] **Step 4: Add entity round-trip, recovery-field, and final-schema tests**

Insert one row for every entity, reload it, and assert exact request ID, lease generation, remote epoch, `failure_attempts`, `next_retry_at`, `last_failed_request_id`, expected remote epoch, Submit phase, state, logical size, reserved usage, and UTC timestamps. Attempt all four invalid nullable-field combinations (target op with missing target generation, target op with remote epoch, remote op with target IDs, remote op without remote epoch), a Submit without a phase, a non-Submit with a phase, a negative failure count, plus invalid lease/target/remote/job/phase values, and assert the SQLite CHECK rejects them. Verify defaults are `failure_attempts=0`, `next_retry_at=NULL`, `last_failed_request_id=NULL`, and new Submit jobs begin in `ready`. Extend `src/store/mod.rs::test_migration_runs` to require all ten existing/new tables and `multipart_uploads.tags_json`.

Run:

```powershell
cargo test --lib store::entities -- --nocapture
cargo test --lib store::tests -- --nocapture
```

Expected: PASS; all entity models round-trip and the final registered schema contains every required table/column.

- [ ] **Step 5: Run migration GREEN**

Run:

```powershell
cargo test --lib store::migrations::m20260721_000001_multi_provider_pinning -- --nocapture
```

Expected: PASS; SQLite up/down retains pre-existing object/multipart data and cascade behavior, while PostgreSQL builders emit every create/index/drop/alter statement.

- [ ] **Step 6: Record the review boundary**

Review only migration/entity registration changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: add durable pinning schema`; otherwise record Task 3 complete without a Git write.

---

### Task 4: S3 Tag Codec, Reserved Controls, and Tag Persistence

**Files:**
- Create: `src/pinning/tags.rs`
- Create: `src/store/pinning/mod.rs`
- Create: `src/store/pinning/tags.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/store/entities/multipart_upload.rs`

**Interfaces:**
- Produce `ObjectTag { key, value }`, `PinControl`, `ContentMode::{Object, Decompressed}`, `parse_tagging_header`, `validate_tag_set`, `encode_tagging_header`, and transactional tag replacement/list APIs.
- Persist normalized multipart tags as JSON and immutable-object tags as rows.

- [ ] **Step 1: Write RED tests for ordinary tag fidelity, S3 limits, form decoding, and every reserved tag combination**

```rust
#[test]
fn header_codec_round_trips_form_encoding_and_preserves_user_values() {
    let tags = parse_tagging_header("team=R%26D&space=hello+world&empty=").unwrap();
    assert_eq!(tags, vec![
        ObjectTag::new("team", "R&D"),
        ObjectTag::new("space", "hello world"),
        ObjectTag::new("empty", ""),
    ]);
    assert_eq!(parse_tagging_header(&encode_tagging_header(&tags)).unwrap(), tags);
}

#[test]
fn reserved_controls_normalize_initial_request_and_reject_invalid_combinations() {
    let tags = reserved(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "30d"), ("ipfs-s3:content", "decompressed")]);
    assert_eq!(PinControl::from_tags(&tags).unwrap(), PinControl::Request {
        duration: Some(LeaseDuration::parse("30d").unwrap()),
        content: ContentMode::Decompressed,
    });
    for tags in [
        reserved(&[("ipfs-s3:pin", "yes")]),
        reserved(&[("ipfs-s3:duration", "30d")]),
        reserved(&[("ipfs-s3:pin", "true"), ("ipfs-s3:retain-until", "not-time")]),
        reserved(&[("ipfs-s3:content", "recursive")]),
    ] {
        assert!(PinControl::from_tags(&tags).is_err());
    }
}
```

Also assert at most 10 tags, unique keys, non-empty key, key ≤128 UTF-8 characters, value ≤256 UTF-8 characters, and only the four exact `ipfs-s3:` keys are accepted in that namespace.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::tags::tests -- --nocapture
cargo test --lib store::pinning::tags::tests -- --nocapture
```

Expected: compile failure because the codec/control and store modules are absent.

- [ ] **Step 3: Implement concrete tag/control types and replacement semantics**

Use these control variants:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectTag {
    pub key: String,
    pub value: String,
}

impl ObjectTag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: value.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinControl {
    Absent,
    Cancel,
    Request { duration: Option<LeaseDuration>, content: ContentMode },
    Renew { retain_until: chrono::DateTime<chrono::Utc> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentMode { Object, Decompressed }
```

Parse `ipfs-s3:pin=false` only as `Cancel`; reject duration/content/retain-until beside false. Parse pin=true + `retain-until` as `Renew`, and reject simultaneous duration. Parse RFC3339 and require a UTC-normalized instant. Initial-vs-renew legality remains a policy-layer decision in Task 5. Encode form pairs with RFC3986 percent encoding and `&`/`=` separators; decode `+` as space exactly once.

Implement store signatures:

```rust
pub async fn replace_object_tags<C: sea_orm::ConnectionTrait>(
    db: &C,
    object_id: &str,
    tags: &[ObjectTag],
) -> Result<(), sea_orm::DbErr>;

pub async fn list_object_tags<C: sea_orm::ConnectionTrait>(
    db: &C,
    object_id: &str,
) -> Result<Vec<ObjectTag>, sea_orm::DbErr>;

pub fn tags_to_json(tags: &[ObjectTag]) -> Result<serde_json::Value, serde_json::Error>;
pub fn tags_from_json(value: &serde_json::Value) -> Result<Vec<ObjectTag>, serde_json::Error>;
```

Delete all existing rows for the immutable object ID and insert the validated replacement in one caller-owned transaction. Sort reads by key for deterministic output; values are not rewritten. Add `tags_json: Json` to `multipart_upload::Model`, accept `tags: &[ObjectTag]` in `create_upload`, and round-trip through `tags_to_json`/`tags_from_json`.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
cargo test --lib pinning::tags::tests -- --nocapture
cargo test --lib store::pinning::tags::tests -- --nocapture
cargo test --lib store::multipart::tests -- --nocapture
```

Expected: PASS; normal tags survive header/JSON/database round trips and all malformed/duplicate/over-limit/reserved combinations fail deterministically.

- [ ] **Step 5: Record the review boundary**

Review tag codec/storage and multipart model changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: persist S3 object tags`; otherwise record Task 4 complete without a Git write.

---

### Task 5: Pure Ordered Policy Evaluation and Lease Composition

**Files:**
- Create: `src/pinning/policy.rs`
- Modify: `src/pinning/mod.rs`
- Modify: `src/error.rs`

**Interfaces:**
- Produce `PublicationContext`, `PublicationPolicy`, `LeaseIntent`, `LeaseSource::{Automatic, Manual}`, and `PinPolicyEvaluator::evaluate_publication`/`evaluate_tag_replacement`.
- The evaluator receives only validated config, operation context, tags, and time; it performs no database, Kubo, or HTTP I/O.

- [ ] **Step 1: Write RED table tests for every policy and renewal rule**

```rust
#[test]
fn ordered_rules_use_first_exact_bucket_and_literal_prefix_match() {
    let evaluator = evaluator_with_rules([
        rule("*", "images/", "request", "one", &["pinata"], "1d", "30d", false),
        rule("photos", "images/raw/", "always", "all", &["pinata", "filebase"], "7d", "90d", true),
    ]);
    let plan = evaluator.evaluate_publication(context("photos", "images/raw/a.nef", no_tags(), false)).unwrap();
    assert!(plan.leases.is_empty(), "the first wildcard/literal-prefix rule is authoritative and requires a request");
}

#[test]
fn always_and_manual_create_independent_lease_intents() {
    let evaluator = evaluator_always_all();
    let plan = evaluator.evaluate_publication(context(
        "bucket", "key", reserved(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "10d")]), false,
    )).unwrap();
    assert_eq!(plan.leases.iter().map(|lease| lease.source).collect::<Vec<_>>(), vec![LeaseSource::Automatic, LeaseSource::Manual]);
    assert_eq!(plan.leases[0].duration, LeaseDuration::parse("30d").unwrap());
    assert_eq!(plan.leases[1].duration, LeaseDuration::parse("10d").unwrap());
}
```

Add cases for no-match/no lease, request requires manual tags, default/max boundary, initial retain-until rejection, active renewal equal timestamp idempotency, active renewal extension, expired renewal with a future retain-until producing `Renew`, expired renewal at/before `now` rejection, cancelled/evicted renewal rejection, renewal shortening rejection, renewal beyond max rejection, unknown captured policy identity rejection, cancellation preserving automatic, one/all provider order, decompressed disallowed, decompressed without ZIP, and decompressed target selection. The pure evaluator recognizes the narrow expired-renewal intent but does not decide whether remote capacity is still held; Task 6's locked store transaction owns that decision.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::policy::tests -- --nocapture
```

Expected: compile failure because the evaluator and normalized lease intents are absent.

- [ ] **Step 3: Implement publication evaluation with explicit boundaries**

Use exact shapes:

```rust
#[derive(Debug, Clone)]
pub struct PublicationContext<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub tags: &'a [ObjectTag],
    pub is_decompress_zip: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseIntent {
    pub source: LeaseSource,
    pub policy_id: String,
    pub provider_mode: ProviderMode,
    pub providers: Vec<String>,
    pub content_mode: ContentMode,
    pub duration: LeaseDuration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationPolicy {
    pub tags: Vec<ObjectTag>,
    pub leases: Vec<LeaseIntent>,
}

impl PinPolicyEvaluator {
    pub fn evaluate_publication(
        &self,
        context: PublicationContext<'_>,
    ) -> Result<PublicationPolicy, PolicyError>;
}
```

Select the first rule where `(bucket == "*" || bucket == context.bucket) && context.key.starts_with(prefix)`. With no match, reject any reserved pin request and otherwise return no leases. `always` adds one automatic object-content lease at default duration. `request` adds no automatic lease. A valid pin=true adds one manual lease, defaulting omitted duration to the rule default and rejecting a duration above max. Initial retain-until is always `InvalidArgument`. Decompressed is manual-only, requires `is_decompress_zip` and `allow_decompressed`, and never changes the automatic lease's object content. The evaluator returns relative durations only; `store::pinning::publication` captures `created_at` immediately inside the publication transaction and derives `expires_at = created_at + duration`, so Kubo upload time is never counted against the lease.

Implement tag replacement evaluation separately:

```rust
pub enum ManualLeaseMutation {
    Keep,
    Renew { retain_until: chrono::DateTime<chrono::Utc> },
    Cancel,
}

#[derive(Debug, Clone)]
pub struct ExistingManualLease {
    pub id: String,
    pub policy_id: String,
    pub content_mode: ContentMode,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub generation: i64,
    pub state: ExistingManualLeaseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingManualLeaseState { Active, Expired, Cancelled, Evicted }

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("invalid pinning request: {0}")]
    InvalidRequest(String),
}

pub fn evaluate_tag_replacement(
    &self,
    existing_manual: Option<&ExistingManualLease>,
    replacement: &[ObjectTag],
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ManualLeaseMutation, PolicyError>;
```

Omitted pin tag, pin=false, and DeleteObjectTagging map to `Cancel` only for an active manual lease; an already expired/cancelled/evicted lease remains in that state. Pin=true cannot create a missing manual lease. Active renewal requires retain-until, permits equality as `Keep`, and rejects shortening. Expired renewal is recognized only when retain-until is strictly later than both the old expiry and `now`; cancelled/evicted renewal is rejected. Both active and expired paths reject a value later than `now + captured_policy.max_duration`, cannot convert object/decompressed content mode or providers, and return `Renew` for Task 6's transaction. Convert `PolicyError`/tag-control errors into `AppError::InvalidPinningRequest(String)` and map that variant to S3 `InvalidArgument`; database/provider failures retain their existing internal/durable paths.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
cargo test --lib pinning::policy::tests -- --nocapture
```

Expected: PASS for ordered first-match, independent lease composition, duration/default/max, renewal/cancellation, one/all, and object/decompressed cases.

- [ ] **Step 5: Record the review boundary**

Review only the pure evaluator. If the user explicitly authorizes a commit, the orchestrator may commit it as `feat: evaluate remote pin policies`; otherwise record Task 5 complete without a Git write.

---

### Task 6: Durable Jobs, Lease Generations, and Atomic Quota Reservations

**Files:**
- Create: `src/pinning/coordinator.rs`
- Create: `src/store/pinning/jobs.rs`
- Create: `src/store/pinning/leases.rs`
- Create: `src/store/pinning/quota.rs`
- Create: `src/pinning/quota.rs`
- Modify: `src/pinning/mod.rs`
- Modify: `src/store/pinning/mod.rs`

**Interfaces:**
- Produce job enqueue/claim/complete/retry APIs, lease renew/cancel/expire/evict APIs, and unique provider/CID reservation/release APIs.
- Create a compile-ready `PinningCoordinator` foundation now—before AppState/S3 integration—with the policy, provider registry, limits, worker settings, `policy()`, `provider_limits()`, and `disabled_for_test()` APIs. Task 12 extends this file; it does not create it.
- Make Submit/Poll target-scoped and Unpin/Reconcile remote-pin-scoped through typed constructors; callers cannot create a job with a semantically invalid nullable-field combination.
- Produce one in-place successful-Poll reschedule API, safe ensure/reactivate APIs for canonical Submit/Poll ownership, and `apply_remote_status` to project one remote observation to every current desired shared-CID target atomically.
- Distinguish fresh from reclaimed claims, persist every Submit's provider-call/recovery phase, and expose a no-request ambiguity guard that prevents quota release while a POST may exist remotely.
- Define the limited expired-manual-lease reactivation transaction and one bounded, idempotent failed-request retry budget owned by each shared `remote_pins` row.
- Every mutating function accepts a caller-owned `ConnectionTrait`; transaction ownership stays at publication/worker boundaries.

- [ ] **Step 1: Write RED store tests for shared-CID accounting, claims, crash locks, generations, and oldest ordering**

```rust
#[tokio::test]
async fn concurrent_shared_cid_reserves_provider_usage_once() {
    let db = file_backed_database_with_pool(4).await;
    seed_usage(&db, "pinata", 1_000, 10).await;
    let first = reserve_unique(&db, reservation("pinata", "bafy-shared", 100)).await;
    let second = reserve_unique(&db, reservation("pinata", "bafy-shared", 100)).await;
    let (first, second) = tokio::join!(first, second);
    assert!(first.unwrap().is_reserved_or_reused());
    assert!(second.unwrap().is_reserved_or_reused());
    assert_eq!(usage(&db, "pinata").await, (100, 1));
    assert_eq!(remote_pin_count(&db, "pinata", "bafy-shared").await, 1);
}

#[tokio::test]
async fn expired_job_lock_is_reclaimable_but_live_lock_is_not() {
    let db = setup().await;
    let now = fixed_time();
    enqueue_job(&db, submit_job("pinata", "bafy-1", "lease-1", "target-1", 1, now)).await.unwrap();
    let fresh = claim_due_jobs(&db, now, chrono::Duration::seconds(30), 1).await.unwrap().pop().unwrap();
    assert!(!fresh.reclaimed);
    assert_eq!(fresh.model.submit_phase.as_deref(), Some("ready"));
    assert!(claim_due_jobs(&db, now + chrono::Duration::seconds(29), chrono::Duration::seconds(30), 1).await.unwrap().is_empty());
    let reclaimed = claim_due_jobs(&db, now + chrono::Duration::seconds(31), chrono::Duration::seconds(30), 1).await.unwrap().pop().unwrap();
    assert!(reclaimed.reclaimed);
    assert_eq!(reclaimed.model.submit_phase.as_deref(), Some("recovering"));
}
```

Add tests proving: stable job IDs deduplicate inserts; two claimers never own one live lock; equal renewal is no-op; extending renewal increments generation and touches targets/remote pins; stale generation cannot release; expiry/cancel marks only manual/selected lease; newest shared-target touch protects a CID from oldest selection; oversize returns `QuotaBlocked` without eviction; release does not decrement until confirmed.

Add coordinator foundation and remote-epoch tests:

```rust
#[test]
fn foundation_exposes_policy_limits_registry_settings_and_disabled_fixture() {
    let coordinator = PinningCoordinator::build(validated_noop_fixture()).unwrap();
    assert_eq!(coordinator.policy().evaluate_publication(publication_context()).unwrap().leases.len(), 1);
    assert_eq!(coordinator.provider_limits()["noop"].max_pins, 10);
    assert!(coordinator.provider("noop").is_some());
    assert_eq!(coordinator.settings().max_attempts, 8);
    assert_eq!(coordinator.settings().poll_interval, std::time::Duration::from_secs(5));
    assert!(PinningCoordinator::disabled_for_test().provider_limits().is_empty());
}

#[tokio::test]
async fn desired_reference_changes_bump_remote_epoch_and_block_stale_release() {
    let db = setup().await;
    seed_remote_pin(&db, "pinata", "bafy-shared", 7, 100).await;
    let delete_epoch = begin_remote_unpin(&db, "pinata", "bafy-shared").await.unwrap();
    assert_eq!(delete_epoch, 8);
    add_shared_active_target(&db, "lease-2", "target-2", "pinata", "bafy-shared").await;
    assert_eq!(remote_epoch(&db, "pinata", "bafy-shared").await, 9);
    let outcome = complete_remote_delete(&db, "pinata", "bafy-shared", delete_epoch, fixed_time()).await.unwrap();
    assert!(matches!(outcome, RemoteDeleteCompletion::Compensated { .. }));
    assert_eq!(usage(&db, "pinata").await, (100, 1));
}

#[tokio::test]
async fn successful_poll_reschedules_same_claimed_row_without_incrementing_attempts() {
    let db = setup().await;
    let now = fixed_time();
    let NewPinJob::Target(job) = poll_job("pinata", "bafy-1", "lease-1", "target-1", 3, "request-1", now) else {
        panic!("poll_job must construct target-scoped work");
    };
    let poll_id = job.id.clone();
    enqueue_job(&db, NewPinJob::Target(job)).await.unwrap();
    set_job_attempts(&db, &poll_id, 2).await;
    let claimed = claim_due_jobs(&db, now, chrono::Duration::seconds(30), 1).await.unwrap().pop().unwrap();
    let next = reschedule_poll_job(
        &db,
        &claimed.model.id,
        claimed.model.locked_until.unwrap(),
        now,
        std::time::Duration::from_secs(5),
    ).await.unwrap();
    let row = job_by_id(&db, &claimed.model.id).await;
    assert_eq!(next, now + chrono::Duration::seconds(5));
    assert_eq!((row.state.as_str(), row.attempts, row.locked_until), ("pending", 2, None));
    assert_eq!(pin_job_count(&db).await, 1);
}

#[tokio::test]
async fn one_remote_status_projects_to_every_current_shared_target() {
    let db = setup_with_shared_targets([("automatic", "one"), ("manual", "one"), ("copy", "all")]).await;
    let result = apply_remote_status(&db, RemoteStatusUpdate {
        provider: "pinata",
        cid: "bafy-shared",
        request_id: "request-1",
        origin: RemoteStatusOrigin::Adopt,
        status: RemotePinStatus::Pinned,
        error_class: None,
        error_text: None,
        now: fixed_time(),
    }).await.unwrap();
    let RemoteStatusApplyResult::Applied { affected, .. } = result else { panic!("expected applied status") };
    assert_eq!(current_target_states(&db, "pinata", "bafy-shared").await, vec!["pinned", "pinned", "pinned"]);
    assert_eq!(affected.len(), 3);
    assert!(affected.iter().all(|lease| lease.available));
}

#[tokio::test]
async fn expired_manual_lease_reactivates_only_preserved_capacity_holding_targets() {
    let db = setup_expired_decompressed_manual_lease([
        ("target-a", "pinata", "bafy-a", "pinned"),
        ("target-b", "pinata", "bafy-b", "absent"),
    ]).await;
    let original_ids = target_ids(&db, "lease-manual").await;
    let old_generation = lease_generation(&db, "lease-manual").await;
    let old_epoch = remote_epoch(&db, "pinata", "bafy-a").await;
    let result = renew_manual_lease(
        &db, "object-latest", "lease-manual", time(30), time(10),
    ).await.unwrap();
    assert!(matches!(result, ManualLeaseRenewalOutcome::Reactivated { .. }));
    assert_eq!(lease_state(&db, "lease-manual").await, "active");
    assert_eq!(lease_generation(&db, "lease-manual").await, old_generation + 1);
    assert_eq!(remote_epoch(&db, "pinata", "bafy-a").await, old_epoch + 1);
    assert_eq!(target_ids(&db, "lease-manual").await, original_ids, "no decompressed target is reconstructed");
    assert_eq!(target_state(&db, "target-a").await, "pinned");
    assert_eq!(target_state(&db, "target-b").await, "released");
}

#[tokio::test]
async fn expired_manual_lease_after_confirmed_release_is_not_recreated() {
    let db = setup_fully_released_expired_decompressed_lease().await;
    let before = target_snapshot(&db, "lease-manual").await;
    let error = renew_manual_lease(
        &db, "object-latest", "lease-manual", time(30), time(10),
    ).await.unwrap_err();
    assert!(matches!(error, RenewManualLeaseError::NoRecoverableReservation));
    assert_eq!(target_snapshot(&db, "lease-manual").await, before);
    assert_eq!(lease_state(&db, "lease-manual").await, "expired");
}

#[tokio::test]
async fn duplicate_failed_observation_counts_once_and_schedules_one_shared_retry() {
    let db = setup_with_shared_targets([("one-a", "one"), ("all-b", "all"), ("all-c", "all")]).await;
    let update = failed_update("pinata", "bafy-shared", "request-1", time(10));
    let first = apply_remote_status(&db, update.clone()).await.unwrap();
    let second = apply_remote_status(&db, update).await.unwrap();
    let first_progress = failure_progress(&first);
    let second_progress = failure_progress(&second);
    assert_eq!(first_progress.attempts, 1);
    assert!(first_progress.newly_counted);
    assert_eq!(second_progress.attempts, 1);
    assert!(!second_progress.newly_counted);
    ensure_failed_remote_retry(&db, "pinata", "bafy-shared", time(10)).await.unwrap();
    assert_eq!(remote_failure_state(&db, "pinata", "bafy-shared").await, (1, Some(time(11))));
    assert_eq!(pending_remote_reconcile_count(&db, "pinata", "bafy-shared").await, 1);
}
```

Also test that new target, active/expired renewal, cancellation, expiry, eviction, and failover each increment the affected remote epoch once per `(provider,cid)` transaction; stale pre-call remote guards skip; an epoch-changed/no-current-ref DELETE completion retains usage and enqueues current Reconcile; target generation guards reject stale Submit/Poll; and the four typed job constructors populate only the allowed scope fields/phases. Add tests that a successful Poll can reschedule only a claimed `running` Poll; retry increments attempts but successful queued/pinning reschedule does not; `ensure_or_reactivate_poll_job` leaves a live running row untouched and reactivates a done row; `ensure_or_reactivate_reconcile_job` keeps one stable current-epoch row and honors the requested durable due time; queued/pinning/pinned/failed projection updates all active shared targets; `ExistingRequest` with null/different request returns `StaleRequest`, while `Adopt` fills only null; an old Poll response after failed-request cleanup cannot re-adopt the deleted ID; `last_failed_request_id` makes duplicate failure counting idempotent across projection; Pinned resets failure count/due/last identity; eight distinct failed request cycles stop with `failure_attempts=8`, `next_retry_at=NULL`, and no due Reconcile; and terminal/quota/released targets are excluded from active projection. Assert renewal rejects a non-latest owner and cancelled/evicted lease, and that expired reactivation preserves the original target row IDs/content mode/providers, restores only capacity-holding remotes, retains usage, and never reacquires an absent remote.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib store::pinning::jobs::tests -- --nocapture
cargo test --lib store::pinning::leases::tests -- --nocapture
cargo test --lib store::pinning::quota::tests -- --nocapture
cargo test --lib pinning::coordinator::tests::foundation_ -- --nocapture
```

Expected: compile failure because the focused store modules and coordinator foundation are absent.

- [ ] **Step 3: Implement the compile-ready coordinator foundation**

Create `src/pinning/coordinator.rs` without using `CancellationToken` yet:

```rust
pub struct WorkerSettings {
    pub interval: std::time::Duration,
    pub poll_interval: std::time::Duration,
    pub worker_concurrency: usize,
    pub claim_limit: u64,
    pub lock_for: chrono::Duration,
    pub base_backoff: std::time::Duration,
    pub max_backoff: std::time::Duration,
    pub max_attempts: u32,
    pub shutdown_grace: std::time::Duration,
}

pub struct PinningCoordinator {
    policy: PinPolicyEvaluator,
    providers: std::collections::HashMap<String, std::sync::Arc<dyn PinningProvider>>,
    limits: ProviderLimitMap,
    settings: WorkerSettings,
}

impl PinningCoordinator {
    pub fn build(config: ValidatedPinningConfig) -> anyhow::Result<std::sync::Arc<Self>>;
    pub fn disabled_for_test() -> std::sync::Arc<Self>;
    pub fn policy(&self) -> &PinPolicyEvaluator;
    pub fn provider_limits(&self) -> &ProviderLimitMap;
    pub fn provider(&self, name: &str) -> Option<std::sync::Arc<dyn PinningProvider>>;
    pub fn settings(&self) -> &WorkerSettings;
}
```

`build` constructs Filebase/Pinata/Noop providers through Task 2, creates the Task 5 evaluator, copies checked limits, and uses the validated worker interval for claim scans. Define `pub const POLL_INTERVAL: Duration = Duration::from_secs(5)` in `jobs.rs` and set `WorkerSettings.poll_interval = POLL_INTERVAL`; the store can therefore schedule the first canonical Poll without depending on coordinator/provider objects, while the 1–30 second guard prevents an invalid interval. Use claim limit `worker_concurrency * 2`, 30-second lock, 1-second base/5-minute maximum backoff, 8 attempts, and 30-second shutdown grace. `disabled_for_test` returns an empty registry/limits/evaluator with the same safe settings. Export the module from `src/pinning/mod.rs`. Task 6's GREEN commands must compile this API before Task 9 references it.

- [ ] **Step 4: Implement scope-safe stable jobs and backend-safe atomic claims**

Use these types and state transitions:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PinJobOperation { Submit, Poll, Unpin, Reconcile }

#[derive(Debug, Clone)]
pub enum NewPinJob {
    Target(TargetPinJob),
    Remote(RemotePinJob),
}

#[derive(Debug, Clone)]
pub struct TargetPinJob {
    pub id: String,
    pub operation: TargetJobOperation,
    pub provider: String,
    pub cid: String,
    pub lease_id: String,
    pub target_id: String,
    pub expected_generation: i64,
    pub next_attempt_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct RemotePinJob {
    pub id: String,
    pub operation: RemoteJobOperation,
    pub provider: String,
    pub cid: String,
    pub expected_remote_epoch: i64,
    pub next_attempt_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetJobOperation { Submit, Poll }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteJobOperation { Unpin, Reconcile }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitPhase { Ready, Calling, Recovering, RecoveryBackoff }

#[derive(Debug, Clone)]
pub struct ClaimedPinJob {
    pub model: pin_job::Model,
    pub reclaimed: bool,
}

pub const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

pub fn submit_job(provider: &str, cid: &str, lease_id: &str, target_id: &str, generation: i64, at: DateTimeUtc) -> NewPinJob;
pub fn poll_job(provider: &str, cid: &str, lease_id: &str, target_id: &str, generation: i64, request_id: &str, at: DateTimeUtc) -> NewPinJob;
pub fn unpin_job(provider: &str, cid: &str, remote_epoch: i64, at: DateTimeUtc) -> NewPinJob;
pub fn reconcile_job(provider: &str, cid: &str, remote_epoch: i64, at: DateTimeUtc) -> NewPinJob;
pub async fn ensure_or_reactivate_submit_job<C: ConnectionTrait>(db: &C, job: TargetPinJob, now: DateTimeUtc) -> AppResult<EnsureJobOutcome>;
pub async fn ensure_or_reactivate_poll_job<C: ConnectionTrait>(db: &C, job: TargetPinJob, now: DateTimeUtc) -> AppResult<EnsureJobOutcome>;
pub async fn ensure_or_reactivate_reconcile_job<C: ConnectionTrait>(db: &C, job: RemotePinJob, now: DateTimeUtc) -> AppResult<EnsureJobOutcome>;
pub async fn reschedule_poll_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    now: DateTimeUtc,
    requested_interval: std::time::Duration,
) -> AppResult<DateTimeUtc>;
pub async fn reschedule_reconcile_job<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    expected_locked_until: DateTimeUtc,
    next_attempt_at: DateTimeUtc,
) -> AppResult<()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnsureJobOutcome { Inserted, Pending, Running, Reactivated }

pub async fn claim_due_jobs<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    now: DateTimeUtc,
    lock_for: chrono::Duration,
    limit: u64,
) -> AppResult<Vec<ClaimedPinJob>>;

pub enum SubmitCallDecision { ReadyToCall, NoLongerDesired }
pub enum SubmitRecoveryDecision {
    RetryScheduled { next_attempt_at: DateTimeUtc },
    NoLongerDesired { reconcile_job_id: String },
}

pub async fn prepare_submit_call<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
) -> AppResult<SubmitCallDecision>;

pub async fn record_submit_recovery_no_match<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    backoff: std::time::Duration,
) -> AppResult<SubmitRecoveryDecision>;

pub async fn retry_submit_recovery<C: ConnectionTrait>(
    db: &C,
    claimed: &ClaimedPinJob,
    now: DateTimeUtc,
    backoff: std::time::Duration,
    redacted_error: &str,
) -> AppResult<()>;

pub enum NoRequestSubmitAmbiguity {
    Clear { cancelled_never_started: Vec<String> },
    Wait { next_check_at: DateTimeUtc },
}

pub async fn resolve_no_request_submit_ambiguity<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<NoRequestSubmitAmbiguity>;
```

Persist operation discriminants exactly as lowercase `submit`, `poll`, `unpin`, and `reconcile` to match Task 3's CHECK. Construct stable IDs exactly as `submit:<provider>:<cid>:<target_id>:g<generation>`, `poll:<provider>:<cid>:<target_id>:g<generation>:<sha256(request_id)>`, `unpin:<provider>:<cid>:e<epoch>`, and `reconcile:<provider>:<cid>:e<epoch>`. Mapping `Target` to the entity sets lease/target/expected_generation and clears expected_remote_epoch; mapping `Remote` does the inverse. A new Submit has `submit_phase='ready'`; every non-Submit has `submit_phase=NULL`. No public raw-field constructor is exposed.

Normal enqueue uses `ON CONFLICT(id) DO NOTHING`. `ensure_or_reactivate_submit_job` is valid only after the same transaction verifies `remote_pins.request_id IS NULL`, status is `reserved`, and the supplied target is the canonical lowest `(created_at,id)` current desired target; it schedules the supplied job at `now`. Insert/reactivation sets `submit_phase='ready'`, attempts zero, and clears lock/error. It never rewrites a live pending/running Submit, because that row may own provider ambiguity. `ensure_or_reactivate_poll_job` similarly requires the remote request ID/status to be current queued/pinning and the supplied target canonical, and requires `job.next_attempt_at` to fall between `now + 1s` and `now + 30s`. `ensure_or_reactivate_reconcile_job` requires a current remote epoch, inserts absent work, keeps live pending/running work, or reactivates a done row at the requested durable time; for an existing pending row it keeps the earlier due time. These APIs cover initial shared attachment, stale-owner handoff, failed-request retry, Reconcile, and DELETE compensation without creating duplicate POSTs.

Claim in a transaction by selecting due unlocked `pending` rows or `running` rows whose `locked_until < now`, then conditionally setting `state='running'` and a new lock; return only rows whose update affected one row. A pending claim returns `ClaimedPinJob { reclaimed: false }` and preserves its Submit phase. Taking an expired running row returns `reclaimed: true`; for Submit it atomically changes `calling`/any running phase to `recovering` before returning, while other operations retain `submit_phase=NULL`. Therefore cancellation/restart cannot lose the query-before-retry requirement.

`prepare_submit_call` accepts only the current claimed lock, rechecks target generation/desire and canonical remote ownership, and commits `submit_phase='calling'` before HTTP; `ReadyToCall` is the only state from which worker code may POST. A stale target returns `NoLongerDesired`, completes safely, and ensures current Reconcile. `record_submit_recovery_no_match` is called only after a successful zero-result stable-metadata find: if a desired target remains, it persists `submit_phase='recovery_backoff'`, returns the row to pending at `now + base_backoff`, preserves attempts, and clears the lock; if none remains, it marks the Submit done/safe and ensures current Reconcile. It never POSTs in the same execution. A multiple-result/protocol/transient find uses `retry_submit_recovery`, which keeps `submit_phase='recovering'` and returns pending. Attempts increment up to `settings.max_attempts`; after that they remain capped and the same stable recovery lookup is scheduled at `max_backoff` rather than becoming `done`/safe. This ambiguity-only safety probe may continue at capped non-rapid cadence because absence cannot be proven from an unavailable/malformed list response; it never POSTs or releases quota. A later fresh claim of `recovery_backoff` must recheck generation and call `prepare_submit_call` before POST; if it crashes after `calling` but before request-ID persistence, reclamation requires find again.

`resolve_no_request_submit_ambiguity` is used only by current-epoch Reconcile when there are no desired refs and `remote_pins.request_id IS NULL`. It may mark pending `ready` or `recovery_backoff` Submit rows done because no POST is unresolved; any running Submit or pending `calling`/`recovering` row returns `Wait` at the earliest lock expiry/next attempt. It does not release usage. `reschedule_reconcile_job` conditionally returns the currently claimed stable Reconcile to pending at that time without incrementing attempts. Only Task 14 may release after this API returns `Clear` and rechecks the same epoch/no-ref/no-request predicates.

`reschedule_poll_job` accepts only a `running` Poll whose persisted `locked_until` equals `expected_locked_until` from the claimed model, clamps `requested_interval` to 1–30 seconds, preserves the stable ID and attempts, sets `state='pending'`, sets `next_attempt_at = now + clamped_interval`, and clears lock/error. A zero-row conditional update is a stale claim and cannot overwrite a reclaimed worker. The function is used only after a successful queued/pinning provider observation; it never inserts a row and never increments attempts. `retry_job` handles non-Submit-recovery errors separately by incrementing attempts, storing redacted error text, setting bounded backoff, returning to pending, and clearing lock. `complete_job` marks done and clears lock; any Submit completion with no persisted remote request normalizes its phase to `ready` so no done row falsely carries ambiguity. This works on SQLite and PostgreSQL and prevents duplicate live ownership without backend-specific `SKIP LOCKED`.

- [ ] **Step 5: Implement lease generations, remote epochs, delete completion, and unique quota reservations**

Define results:

```rust
pub enum ReservationOutcome { Reused, Reserved, QuotaWaiting { evict: Vec<(String, String)> }, QuotaBlocked }
pub enum GenerationDecision { Current, Stale, NoLongerNeeded }
pub enum RemoteDeleteCompletion {
    Released,
    Compensated { submit_job_id: String },
    ReconcileRequired { reconcile_job_id: String },
}

#[derive(Debug, Clone)]
pub struct RemoteStatusUpdate<'a> {
    pub provider: &'a str,
    pub cid: &'a str,
    pub request_id: &'a str,
    pub origin: RemoteStatusOrigin,
    pub status: RemotePinStatus,
    pub error_class: Option<&'a str>,
    pub error_text: Option<&'a str>,
    pub now: DateTimeUtc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteStatusOrigin { Adopt, ExistingRequest }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffectedLeaseOutcome {
    pub lease_id: String,
    pub target_id: String,
    pub provider_mode: ProviderMode,
    pub available: bool,
}

pub enum RemoteStatusApplyResult {
    Applied {
        affected: Vec<AffectedLeaseOutcome>,
        failure: Option<RemoteFailureProgress>,
    },
    StaleRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteFailureProgress {
    pub attempts: i32,
    pub next_retry_at: Option<DateTimeUtc>,
    pub newly_counted: bool,
    pub exhausted: bool,
}

pub enum TargetProjection {
    Pinned,
    Submitted { poll_job_id: String },
    Waiting { submit_job_id: String },
    Degraded { reconcile_job_id: String },
    QuotaWaiting,
    QuotaBlocked,
}

pub async fn project_target_from_remote<C: ConnectionTrait>(
    db: &C,
    target_id: &str,
    now: DateTimeUtc,
) -> AppResult<TargetProjection>;

pub async fn apply_remote_status<C: ConnectionTrait>(
    db: &C,
    update: RemoteStatusUpdate<'_>,
) -> AppResult<RemoteStatusApplyResult>;

pub async fn complete_remote_delete<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<RemoteDeleteCompletion>;

pub enum NoRequestRemoteCompletion {
    Released,
    Wait { next_check_at: DateTimeUtc },
    Stale,
}

pub async fn complete_no_request_remote_absence<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    now: DateTimeUtc,
) -> AppResult<NoRequestRemoteCompletion>;

pub const MAX_FAILED_REQUEST_ATTEMPTS: i32 = 8;
pub const FAILED_REQUEST_BASE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
pub const FAILED_REQUEST_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(300);

pub enum FailedRemoteRetryDecision {
    Scheduled { reconcile_job_id: String, at: DateTimeUtc },
    NotNeeded,
    Exhausted,
}

pub enum FailedRemoteResubmitDecision {
    Prepared { new_epoch: i64, submit_job_id: String },
    Stale,
    NoAllModeTarget,
    Exhausted,
}

pub async fn ensure_failed_remote_retry<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<FailedRemoteRetryDecision>;

pub async fn reset_failed_remote_retry_on_user_touch<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    now: DateTimeUtc,
) -> AppResult<FailedRemoteRetryDecision>;

pub async fn prepare_failed_remote_resubmit<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    cid: &str,
    expected_remote_epoch: i64,
    expected_request_id: &str,
    now: DateTimeUtc,
) -> AppResult<FailedRemoteResubmitDecision>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualLeaseRenewalOutcome {
    Kept { generation: i64 },
    Extended { generation: i64 },
    Reactivated { generation: i64, restored_target_ids: Vec<String> },
}

#[derive(Debug, thiserror::Error)]
pub enum RenewManualLeaseError {
    #[error("manual lease is not owned by the latest object")]
    NotLatestOwner,
    #[error("manual lease state cannot be renewed")]
    InvalidState,
    #[error("expired manual lease has no recoverable remote reservation")]
    NoRecoverableReservation,
    #[error(transparent)]
    Database(#[from] sea_orm::DbErr),
}

pub async fn renew_manual_lease<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
    lease_id: &str,
    retain_until: DateTimeUtc,
    now: DateTimeUtc,
) -> Result<ManualLeaseRenewalOutcome, RenewManualLeaseError>;
```

`reserve_unique` first looks up `(provider,cid)`. A new remote pin is inserted with epoch 1, `failure_attempts=0`, and no due retry. Reuse of a capacity-holding `reserved`/`queued`/`pinning`/`pinned`/`failed` row adds a desired active target, increments epoch transactionally, updates `last_touched_at` to the maximum active-target touch, and does not change usage. A genuinely new target is an explicit user touch: if the reused row is `failed`, call `reset_failed_remote_retry_on_user_touch` in that same caller-owned transaction after target insertion; ordinary worker projection never resets the budget. An existing `absent` row is not counted reuse: atomically reacquire quota, increment bytes/count once, set `reserved`, clear stale errors/request/failure budget, and bump epoch before target projection. For a new CID, checked-convert size/count to `i64`, atomically compare current usage to provider limits, insert `remote_pins(status='reserved', epoch=1)`, and increment bytes/count once. If `cid_size > max_bytes`, return `QuotaBlocked` and create no reservation. Otherwise return ordered eviction candidates and leave the new target `quota_waiting` until confirmed capacity is released.

After inserting any target, call `project_target_from_remote` in the same transaction. Current active desired targets are those whose parent lease is `active` and whose target state is `waiting`, `submitted`, `pinned`, or `degraded`; quota-waiting/blocked, evicted, released, and terminal-lease targets are excluded. Projection is exact:

- remote `pinned` → set the new target `pinned`, derive its lease availability immediately, enqueue no remote job;
- remote `queued`/`pinning` with request ID → set target `submitted`, choose canonical lowest `(created_at,id)` active target, and `ensure_or_reactivate_poll_job` for that target/current lease generation/request ID at `now + POLL_INTERVAL`;
- remote `reserved` with no request ID → set target `waiting`, choose the same canonical target, and `ensure_or_reactivate_submit_job` exactly once;
- remote `failed` → set target `degraded` and ensure one current-epoch Reconcile so Task 12 projects/coordinates every affected lease; a genuinely new target or generation-advancing manual extension/reactivation first resets the shared failure budget, while equal renewal/passive projection preserves it;
- remote `absent` → atomically reactivate/reserve it as `reserved` under quota rules, then follow the waiting/canonical Submit branch;
- target `quota_waiting`/`quota_blocked` remains in that state and creates no Submit/Poll until Task 14 grants a reservation.

`apply_remote_status` first enforces request ownership. `ExistingRequest` (Poll or persisted Reconcile projection) requires the stored request ID to equal the observation exactly; null/different is `StaleRequest`. `Adopt` (Submit response or stable-metadata find) may fill a null request ID but may not replace a different non-null ID. This prevents an in-flight old Poll from re-adopting a failed request after failed-request DELETE cleared it. In one transaction the function persists the accepted request ID, remote status, redacted error fields, and touch time, then projects to every active desired target for `(provider,cid)`: queued/pinning → `submitted`; pinned → `pinned`; failed → `degraded`. Return one `AffectedLeaseOutcome` per affected lease/target with `available = any(current target for that lease is pinned)`.

Failure accounting belongs to that same transaction. A `failed` observation increments `failure_attempts` exactly once unless `last_failed_request_id == request_id`; the duplicate case returns `newly_counted=false` and preserves count/due time even if stale delivery followed an intervening projection. A newly counted failure stores `last_failed_request_id=request_id`. For attempts 1–7, set `next_retry_at = now + min(1s * 2^(attempts-1), 300s)` with checked arithmetic; attempt 8 sets `next_retry_at=NULL` and `exhausted=true`. Queued/pinning preserves the accumulated count/last failed identity but clears stale `next_retry_at`; Pinned resets count to zero and clears both due time and last failed identity. Provider HTTP remains outside this transaction. After processing every returned one-mode outcome, the worker calls `ensure_failed_remote_retry`: it schedules/reactivates exactly one stable current-epoch Reconcile at `next_retry_at` only if the row is still failed, attempts are below eight, and at least one current all-mode desired target remains. No per-target retry row is created.

When that Reconcile becomes due, Task 12 DELETEs/forgets the persisted failed request outside the transaction. After success/NotFound, `prepare_failed_remote_resubmit` locks and rechecks request ID, epoch, status, due time, attempts, and an all-mode desired target. If current, it clears request/error/due time, sets status `reserved`, preserves `failure_attempts`, `last_failed_request_id`, and usage, increments epoch once, marks all Poll jobs tied to the forgotten request done, chooses the canonical current target/generation, and ensures one `ready` Submit. A stale old Poll therefore returns `StaleRequest` and cannot re-adopt the deleted request. A stale DELETE response cannot mutate the row. On the next distinct failed request, the count/backoff advances; at eight, no due job is retained, so the worker cannot spin. `reset_failed_remote_retry_on_user_touch` is the only automatic reset API and is called only by a genuinely new target publication or generation-advancing manual extension/reactivation; equal/idempotent renewal does not call it. The API sets count to zero, retains the current `last_failed_request_id` so replay stays idempotent, and schedules current failed all-mode state from attempt-one timing. Administrative tooling may call the same explicitly later, but no worker loop calls it.

`renew_manual_lease` locks/reloads the manual lease, its owner, all preserved original targets, their remote rows, and relevant Unpin jobs. It first requires `owner_object_id` to match the lease owner and that immutable object to remain latest. For `active`, equality is `Kept`; a valid extension increments generation once. For `expired`, reactivation is permitted only if at least one original `(provider,cid)` still has a non-null request ID, a pending/running recoverable Unpin, or a capacity-holding `reserved`/`queued`/`pinning`/`pinned`/`failed` row whose usage has not been released. If every original remote is `absent`/released, return `NoRecoverableReservation` without modifying tags, lease, targets, jobs, epochs, or usage. `cancelled` and `evicted` return `InvalidState`.

An allowed expired reactivation never inserts a lease/target and never reconstructs decompressed entries. In one transaction it sets the same lease `active`, increments its generation once, writes the later expiry/touch, and restores only original targets whose remotes still satisfy the capacity-holding/request/Unpin predicate: pinned→pinned, queued/pinning→submitted, reserved→waiting, failed→degraded. Already absent/released original targets remain released. For each distinct restored `(provider,cid)`, increment the remote epoch once, retain quota, refresh touch, reset exhausted failure budget as this explicit renewal touch, project canonical Submit/Poll or failed Reconcile, and ensure a current-epoch Reconcile to invalidate/compensate old Unpin work. A DELETE already past its pre-call check may finish; `complete_remote_delete` then observes the newer epoch/current generation and compensates without release. Active extension performs the same generation/epoch/touch/current-work refresh across its current original targets. `cancel_lease`, `expire_due_leases`, `evict_provider_cid`, new shared-target publication, and later failover likewise increment each affected remote epoch once per transaction, recalculate derived touch, and enqueue `reconcile_job`/`unpin_job` with that exact epoch; they never decrement usage. Expiry preserves target rows/identity so only this bounded race can reactivate them.

Before a target job, `check_target_job_generation` reloads both lease and target and returns stale/no-longer-needed unless IDs, desired state, and lease generation match. Before a remote job, `check_remote_job_epoch` reloads `(provider,cid)` and compares only `expected_remote_epoch`; Unpin additionally requires zero active desired targets. A stale remote job completes without provider mutation while the current-epoch reconcile job remains or is inserted idempotently.

`complete_no_request_remote_absence` is the Task 6 release primitive for current-epoch Reconcile with zero desired refs and null request ID. In one transaction it calls `resolve_no_request_submit_ambiguity`; safe pending `ready`/`recovery_backoff` Submit rows become done, while any running or pending `calling`/`recovering` work returns `Wait` without status/usage mutation. After `Clear`, it rechecks epoch/no refs/null request/no unresolved Submit, marks remote absent, clears error/failure fields, and decrements usage exactly once. Task 12 can therefore compile and execute safe release; Task 14 later couples its `Released` result to waiter wake/project under provider limits.

`complete_remote_delete` runs only after provider DELETE success/NotFound and rechecks remote row, epoch, and active desired targets in one transaction. If epoch still matches and no active refs exist, it marks absent, clears request ID/error, resets failure count/due time/last failed identity, marks old targets released, and decrements bytes/count exactly once. Its `Released` result lets Task 14 wake waiters with runtime provider limits after commit. Otherwise it never decrements usage and always clears the now-invalid request ID. With active refs, set status `reserved`, increment epoch only if refs changed without a prior bump, select the canonical lowest `(created_at,id)` active target, and call `ensure_or_reactivate_submit_job` with its current lease generation; this reactivates even a same-ID Submit that finished before DELETE returned and returns `Compensated`. If epoch changed but no refs remain, keep status/reservation, enqueue `reconcile_job` at the current epoch, and return `ReconcileRequired`; that current remote reconcile may later prove absence/release without inventing a target. This is the durable compensation path for renewal/new-reference during an in-flight DELETE.

- [ ] **Step 6: Run GREEN including dependency and concurrency tests**

Run:

```powershell
cargo test --lib store::pinning::jobs::tests -- --nocapture
cargo test --lib store::pinning::leases::tests -- --nocapture
cargo test --lib store::pinning::quota::tests -- --nocapture
cargo test --lib pinning::coordinator::tests::foundation_ -- --nocapture
cargo check --lib
```

Expected: PASS; the Task 6 library compiles with coordinator available, claims are exclusive and report fresh/reclaimed ownership, reclaimed Submit persists `recovering`, successful Poll/Reconcile reschedule keeps one stable row/attempt count, job scopes/phases satisfy schema constraints, one remote observation projects to every active shared target, canonical job ensure/reactivation does not duplicate Submit/Poll/Reconcile, duplicate failed observations count once and eight distinct failed requests stop durably, shared reservations count once, active/limited-expired renewals obey latest-owner/original-target rules, lease/remote guards reject stale work, renew-during-delete produces compensation without quota release, unresolved Submit ambiguity blocks absence, ordering uses newest active touch, and capacity changes only after a current-epoch confirmed absence.

- [ ] **Step 7: Record the review boundary**

Review coordinator foundation and job/lease/quota store logic together. If the user explicitly authorizes a commit, the orchestrator may commit it as `feat: add durable pin lifecycle foundation`; otherwise record Task 6 complete without a Git write.

---

### Task 7: Atomic Object, Tag, Lease, Target, Usage, and Outbox Publication

**Files:**
- Create: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/mod.rs`
- Modify: `src/store/object.rs`
- Modify: `src/store/multipart.rs`

**Interfaces:**
- Produce `PublicationObject`, `PublicationRequest`, `ZipPublicationRequest`, `publish_object`, `publish_completed_upload`, `publish_zip`, `publish_completed_zip`, `reconcile_completed_publication`, and `delete_latest_with_leases`.
- Overwrite/delete closes prior latest-object leases in the same transaction; no provider call occurs in these functions.

- [ ] **Step 1: Write RED transaction tests for commit/rollback, overwrite/delete, and provider-call isolation**

```rust
#[tokio::test]
async fn publication_atomically_commits_object_tags_two_leases_targets_usage_and_jobs() {
    let db = setup().await;
    let request = publication_with_automatic_and_manual("bafy-one");
    let result = publish_object(&db, request, &limits()).await.unwrap();
    assert_eq!(latest(&db, "bucket", "key").await.id, result.object_id);
    assert_eq!(tags(&db, &result.object_id).await.len(), 3);
    assert_eq!(active_leases(&db, &result.object_id).await.len(), 2);
    assert_eq!(targets(&db, &result.object_id).await.len(), 3);
    assert_eq!(usage(&db, "pinata").await, (7, 1));
    assert_eq!(usage(&db, "filebase").await, (7, 1));
    assert_eq!(pending_operations(&db).await, vec!["submit", "submit"]);
}

#[tokio::test]
async fn forced_outbox_failure_rolls_back_object_tags_leases_and_usage() {
    let db = setup_with_failing_pin_job_insert().await;
    let error = publish_object(&db, publication_with_manual("bafy-new"), &limits()).await.unwrap_err();
    assert!(matches!(error, AppError::Database(_)));
    assert!(latest_optional(&db, "bucket", "key").await.is_none());
    assert_eq!(all_pinning_row_counts(&db).await, [0, 0, 0, 0, 0, 0]);
}

#[tokio::test]
async fn multipart_zip_failure_preserves_upload_parts_and_rolls_back_every_publication_row() {
    for failure in [FailurePoint::ArchiveInsert, FailurePoint::EntryInsert, FailurePoint::LeaseInsert, FailurePoint::JobInsert] {
        let db = setup_with_prior_latest_tagged_upload_parts_and_failure(failure).await;
        let request = multipart_zip_publication("completion-attempt-1", ["bafy-entry-a", "bafy-entry-b"]);
        let error = publish_completed_zip(&db, "upload-1", request, &limits()).await.unwrap_err();
        assert!(matches!(error, CommitCompletedUploadError::RolledBack { .. }), "{failure:?}");
        assert!(get_upload(&db, "upload-1").await.is_ok(), "{failure:?}");
        assert_eq!(list_parts(&db, "upload-1").await.unwrap().len(), 2, "{failure:?}");
        assert_eq!(latest(&db, "bucket", "archive.zip").await.cid, "bafy-prior", "{failure:?}");
        assert_eq!(new_object_tag_lease_target_remote_usage_job_counts(&db).await, [0, 0, 0, 0, 0, 0, 0], "{failure:?}");
    }
}

#[tokio::test]
async fn multipart_zip_success_publishes_every_row_and_removes_upload_parts_atomically() {
    let db = setup_with_tagged_upload_parts().await;
    let request = multipart_zip_publication("completion-attempt-2", ["bafy-entry-a", "bafy-entry-b"]);
    publish_completed_zip(&db, "upload-2", request, &limits()).await.unwrap();
    assert!(get_upload(&db, "upload-2").await.is_err());
    assert!(list_parts(&db, "upload-2").await.unwrap().is_empty());
    assert_eq!(published_archive_and_entry_cids(&db).await, vec!["bafy-archive", "bafy-entry-a", "bafy-entry-b"]);
    assert_eq!(pending_operations(&db).await, vec!["submit", "submit"]);
}
```

Add tests for repeated CID reuse (usage unchanged but remote epoch increments for the new desired target), one creates only the first reserved provider target, all creates every provider target, every Submit job carries lease/target/expected_generation and `submit_phase='ready'` with no remote epoch, overwrite/delete increments affected remote epochs and creates only remote-scoped Reconcile/Unpin jobs, ZIP manual decompressed targets only successful entries, and both multipart Complete functions delete upload/parts in their publication transaction. Add automatic+manual+Copy shared-CID tests proving initial reserved state creates one canonical Submit for all waiting targets, a pre-pinned remote makes a later target immediately pinned with no job, and queued/pinning reuse creates/retains exactly one canonical Poll. Seed an exhausted failed shared remote, publish a genuinely new all-mode target, and assert publication retains unique usage, resets `failure_attempts` to zero through `reset_failed_remote_retry_on_user_touch`, leaves all targets degraded, and ensures exactly one current-epoch Reconcile; ordinary tag replacement, equal renewal, and passive worker projection must not reset it.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib store::pinning::publication::tests -- --nocapture
```

Expected: compile failure because the publication facade does not exist.

- [ ] **Step 3: Define transaction inputs that keep policy out of S3 ops**

```rust
#[derive(Debug, Clone)]
pub struct PublicationObject {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    pub logical_size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub sse_c_key_fingerprint: Option<String>,
    pub multipart: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct PublicationRequest {
    pub object: PublicationObject,
    pub tags: Vec<ObjectTag>,
    pub policy: PublicationPolicy,
    pub object_target: PinTargetSpec,
}

#[derive(Debug, Clone)]
pub struct ZipPublicationRequest {
    pub archive: PublicationRequest,
    pub entries: Vec<PublicationObject>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationResult { pub object_id: String }

pub async fn publish_object(
    db: &sea_orm::DatabaseConnection,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult>;

pub async fn publish_completed_upload(
    db: &sea_orm::DatabaseConnection,
    upload_id: &str,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError>;

pub async fn publish_zip(
    db: &sea_orm::DatabaseConnection,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult>;

pub async fn publish_completed_zip(
    db: &sea_orm::DatabaseConnection,
    upload_id: &str,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError>;

pub async fn reconcile_completed_publication(
    db: &sea_orm::DatabaseConnection,
    upload_id: &str,
    expected_archive: &PublicationObject,
) -> ReconciledCommitOutcome;

pub async fn delete_latest_with_leases(
    db: &sea_orm::DatabaseConnection,
    bucket: &str,
    key: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> AppResult<bool>;
```

`PinTargetSpec` contains CID and logical S3 size only. For an automatic lease or manual object-content lease, target the published object. For a manual decompressed lease, target every successful `entries` object and never the archive. Generated entries receive no recursive policy evaluation for this operation.

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinTargetSpec {
    pub cid: String,
    pub logical_size: i64,
}

#[allow(clippy::too_many_arguments)]
impl PublicationObject {
    pub fn from_put(
        id: String,
        bucket: &str,
        key: &str,
        cid: String,
        logical_size: i64,
        content_type: Option<String>,
        metadata: Option<serde_json::Value>,
        encrypted: bool,
        key_wrap: Option<String>,
        sse_c_key_fingerprint: Option<String>,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Self;
}
```

- [ ] **Step 4: Implement one transaction for each publication/delete shape**

Make `write_latest_in_transaction` return `Option<String>` for the previous latest object ID after marking it non-latest. In the same transaction:

1. End prior-object active leases, increment lease generations and each affected `(provider,cid)` remote epoch once, release their targets logically, and enqueue `reconcile_job`/`unpin_job` with expected_remote_epoch and no lease/target fields.
2. Insert the new immutable object and replacement `object_tags`.
3. Capture `created_at = Utc::now()` inside the transaction, then insert separate automatic/manual leases using deterministic IDs from the object ID/source and `expires_at = created_at + intent.duration`.
4. For `all`, process every configured provider. For `one`, process providers in validated priority order until one is reserved/reused; if none can reserve, persist the first candidate as `quota_waiting`/`quota_blocked` without fabricating success.
5. Reuse or reserve `(provider,cid)` atomically; insert a new remote row at epoch 1 or bump an existing row once for the newly desired target; insert each target; if this genuinely new desired target attaches to an existing failed remote, call `reset_failed_remote_retry_on_user_touch` once per distinct remote; then call `project_target_from_remote` so pinned/queued/reserved/failed shared state is reflected immediately and only deterministic canonical Submit/Poll/Reconcile work is ensured.
6. Commit before returning.

`publish_zip` is only for ordinary/custom Put ZIP and writes archive plus successful extracted-entry object rows before archive-owned decompressed targets. `publish_completed_upload` does the non-ZIP root publication; `publish_completed_zip` performs the same ZIP publication and additionally requires/deletes exactly one matching multipart upload row, whose FK cascade removes parts, inside that one transaction. Preserve the existing maximum three retries for transaction-body unique/constraint conflicts. The completed function's reconciliation identity is exactly `request.object.id` or `request.archive.object.id`; the S3 finalizer layer in Tasks 10–11 verifies that ID against `CompletedMultipartArchive.completion_attempt_id` before calling the store. Map a transaction-body error to `CommitCompletedUploadError::RolledBack` and a commit/connection uncertainty to `OutcomeUnknown`, preserving that identity. `reconcile_completed_publication` checks the exact expected archive object/latest fields plus upload absence; because all archive/entry/tag/lease/target/usage/job writes and upload deletion share one transaction, exact attempt present + upload absent is `Committed`, attempt absent is `NotCommitted`, and any mixed/query state is `Unknown`. Callers must reconcile OutcomeUnknown and never guess or issue a second publication. `delete_latest_with_leases` marks latest false and ends its leases in one transaction; it never deletes object bytes/rows and never references Kubo.

- [ ] **Step 5: Run GREEN and inspect SQL boundaries**

Run:

```powershell
cargo test --lib store::pinning::publication::tests -- --nocapture
rg -n "reqwest|PinningProvider|\.submit\(|\.unpin\(" src/store
```

Expected: tests PASS; ordinary and completed ZIP transactions have distinct typed entry points, multipart ZIP rollback/success includes upload/parts and every publication row, shared-CID targets inherit remote state with one canonical job, and the search returns no provider/network calls under `src/store`.

- [ ] **Step 6: Record the review boundary**

Review publication/object/multipart store changes as one atomicity boundary. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: publish pin outbox atomically`; otherwise record Task 7 complete without a Git write.

---

### Task 8: Standard Get/Put/Delete Object Tagging Control Plane

**Files:**
- Modify: `src/state.rs`
- Create: `src/s3/ops/tagging.rs`
- Modify: `src/s3/ops/mod.rs`
- Modify: `src/s3/handler.rs`
- Modify: `src/store/pinning/leases.rs`
- Modify: `src/store/pinning/tags.rs`
- Modify: direct `AppState` test fixtures in `src/s3/ops/bucket.rs`, `src/s3/ops/object.rs`, `src/s3/ops/multipart.rs`, `src/s3/route/decompress_zip.rs`, and `src/zip/extract.rs`
- Modify: `tests/support/decompress.rs` direct `AppState` fixture

**Interfaces:**
- Implement exact s3s trait methods for `GetObjectTaggingInput`, `PutObjectTaggingInput`, and `DeleteObjectTaggingInput`.
- Tag replacement and manual lease renew/cancel commit together; automatic leases remain untouched.

- [ ] **Step 1: Write RED operation tests for round-trip, renew, idempotency, shortening, omission, and deletion**

```rust
#[tokio::test]
async fn put_get_and_delete_tagging_use_replacement_semantics_and_only_manual_control() {
    let state = state_with_automatic_and_manual_object().await;
    put_object_tagging(&state, tagging_request(vec![
        tag("team", "storage"),
        tag("ipfs-s3:pin", "true"),
        tag("ipfs-s3:retain-until", "2026-08-21T00:00:00Z"),
    ])).await.unwrap();
    let output = get_object_tagging(&state, get_tagging_request()).await.unwrap().output;
    assert_eq!(output.tag_set, vec![
        tag("ipfs-s3:pin", "true"),
        tag("ipfs-s3:retain-until", "2026-08-21T00:00:00Z"),
        tag("team", "storage"),
    ]);
    delete_object_tagging(&state, delete_tagging_request()).await.unwrap();
    assert!(stored_tags(&state).await.is_empty());
    assert_eq!(lease_state(&state, LeaseSource::Manual).await, "cancelled");
    assert_eq!(lease_state(&state, LeaseSource::Automatic).await, "active");
}
```

Add a same-timestamp test asserting lease generation/remote epoch/job counts do not change, an extension test asserting lease generation and every affected remote epoch increment once, an earlier timestamp `InvalidArgument` test asserting tags/lease/epochs unchanged, pin=true without an existing manual lease rejection, normal/decompressed content-mode mutation rejection, omission-of-pin cancellation, and nonexistent key `NoSuchKey`.

Add these expiry-race RED cases at the S3 operation boundary:

```rust
#[tokio::test]
async fn put_tagging_reactivates_expired_manual_lease_only_while_original_remote_is_held() {
    let state = state_with_expired_manual_and_blocked_unpin("object-latest", "bafy-entry").await;
    let before = pinning_identity_snapshot(&state).await;
    put_object_tagging(&state, retain_until_request("2026-08-21T00:00:00Z")).await.unwrap();
    let after = pinning_identity_snapshot(&state).await;
    assert_eq!(after.lease_state, "active");
    assert_eq!(after.lease_generation, before.lease_generation + 1);
    assert_eq!(after.remote_epoch, before.remote_epoch + 1);
    assert_eq!(after.lease_id, before.lease_id);
    assert_eq!(after.target_ids, before.target_ids);
    assert_eq!(after.usage, before.usage);
}

#[tokio::test]
async fn put_tagging_after_confirmed_release_rejects_and_does_not_recreate_decompressed_targets() {
    let state = state_with_released_expired_decompressed_lease().await;
    let before = pinning_identity_snapshot(&state).await;
    let error = put_object_tagging(&state, retain_until_request("2026-08-21T00:00:00Z")).await.unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidArgument");
    assert_eq!(pinning_identity_snapshot(&state).await, before);
    assert_eq!(manual_target_cids(&state).await, vec!["bafy-original-a", "bafy-original-b"]);
}
```

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib s3::ops::tagging::tests -- --nocapture
```

Expected: compile failure because the operations and handler methods are absent.

- [ ] **Step 3: Implement exact handler and transaction flow**

Attach the already-compiled Task 6 foundation before the tagging handler uses policy state. In `AppState::new`, validate `cfg.pinning` with production environment lookup and call `PinningCoordinator::build(validated)`. Add `pub pinning: Arc<PinningCoordinator>` to `AppState`. Use `PinningCoordinator::disabled_for_test()` in every unrelated direct fixture listed above; configured tagging tests use `PinningCoordinator::build`. Task 15 later extracts `new_with_env` without changing this behavior.

Add methods to `impl S3 for S3Impl`:

```rust
async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    super::ops::tagging::get_object_tagging(&self.state, req).await
}

async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    super::ops::tagging::put_object_tagging(&self.state, req).await
}

async fn delete_object_tagging(&self, req: S3Request<DeleteObjectTaggingInput>) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    super::ops::tagging::delete_object_tagging(&self.state, req).await
}
```

Convert between s3s `Tag { key, value }` and `ObjectTag`. Validate the complete replacement before opening a transaction. Reload latest object and existing manual lease, including `ExistingManualLeaseState`, call `state.pinning.policy().evaluate_tag_replacement`, then in one transaction replace ordinary/reserved rows and apply `Keep`, `Renew`, or `Cancel`. Call Task 6's `renew_manual_lease` inside that same transaction before tag replacement is committed; map `NotLatestOwner`, `InvalidState`, and `NoRecoverableReservation` to S3 `InvalidArgument`, and let database errors retain the existing internal mapping. Any rejection rolls back the replacement tags.

Active renewal updates one lease generation and all current original targets. Expired renewal follows only Task 6's limited reactivation: same latest owner, same lease/content mode/provider/target row IDs, at least one original capacity-holding/request-bearing/recoverable-Unpin remote, no absent-remote reacquisition, no decompressed target reconstruction, one generation/remote-epoch advance, retained quota, and current typed Reconcile/Submit/Poll. A blocked old DELETE may finish and then compensate. Cancellation performs the same epoch bump before creating remote-scoped work. Cancellation/omission/DeleteObjectTagging never reactivates an expired lease and never modifies an automatic lease. Return default Put/Delete outputs and a deterministic key-sorted tag set for Get.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
cargo test --lib s3::ops::tagging::tests -- --nocapture
cargo test --lib store::pinning::leases::tests -- --nocapture
cargo check --all-targets
```

Expected: PASS; standard operations persist normal tags, active renewal is monotonic/idempotent, the limited expired renewal preserves owner/targets/quota through an in-flight DELETE, confirmed release rejects without recreating decompressed targets or tags, cancellation follows S3 replacement semantics, and automatic leases survive.

- [ ] **Step 5: Record the review boundary**

Review tagging handler/store changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: implement S3 object tagging`; otherwise record Task 8 complete without a Git write.

---

### Task 9: PutObject, CopyObject, Overwrite, DeleteObject, and DeleteObjects Publication Hooks

**Files:**
- Modify: `src/s3/ops/object.rs`
- Modify: `src/store/pinning/publication.rs`

**Interfaces:**
- Put and Copy preflight `x-amz-tagging`, evaluate policy before Kubo mutation, then use `publish_object` after local add/pin.
- Delete and each successful DeleteObjects item use `delete_latest_with_leases`; local Kubo pins remain untouched.

- [ ] **Step 1: Write RED object-operation tests for automatic/manual publication and remote isolation**

```rust
#[tokio::test]
async fn put_commits_outbox_without_calling_provider_in_the_response_path() {
    let (state, provider) = state_with_rule_and_recording_provider("always", "all").await;
    let response = put_object(&state, put_request("bucket", "key", b"body", HeaderMap::new())).await.unwrap();
    assert_eq!(response.output.e_tag.unwrap().value(), "bafy-put");
    assert!(provider.requests().await.is_empty());
    assert_eq!(active_lease_sources(&state, "bucket", "key").await, vec!["automatic"]);
    assert_eq!(pending_operations(state.store.db()).await, vec!["submit", "submit"]);
}

#[tokio::test]
async fn overwrite_ends_old_leases_and_creates_new_state_in_one_commit() {
    let state = state_with_manual_rule().await;
    publish_seed(&state, "old-id", "bafy-old").await;
    put_object(&state, put_with_tagging("ipfs-s3%3Apin=true&ipfs-s3%3Aduration=7d")).await.unwrap();
    assert_eq!(owner_lease_states(&state, "old-id").await, vec!["cancelled"]);
    assert_eq!(latest_cid(&state).await, "bafy-new");
    assert_eq!(active_lease_sources(&state, "bucket", "key").await, vec!["manual"]);
}
```

Add tests that invalid tag/control/policy fails before `/api/v0/add`; a request rule without tags creates no lease; automatic plus manual creates two leases; Copy defaults to source tags, `x-amz-tagging` replacement is honored, and shared CID usage counts once; Copy that reuses an already pinned `(provider,cid)` creates a pinned target/available lease immediately with no Submit/Poll; DeleteObject/DeleteObjects close only successfully removed latest-object leases; DeleteObjects preserves per-item errors/order; no remote provider request occurs synchronously.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib s3::ops::object::tests::pinning_ -- --nocapture
```

Expected: new tests fail because object operations still call the legacy store methods and ignore tags/policies.

- [ ] **Step 3: Consume the existing coordinator foundation without starting the worker**

Task 6 created/exported `src/pinning/coordinator.rs`, and Task 8 already attached it to AppState and updated all direct fixtures. Task 9 uses this existing field; it must not redefine coordinator or AppState types:

```rust
let policy = state.pinning.policy().evaluate_publication(context)?;
let limits = state.pinning.provider_limits();
```

Use the already-compiled `PinningCoordinator::policy() -> &PinPolicyEvaluator` and `provider_limits() -> &ProviderLimitMap`; Task 9 must not redefine or recreate coordinator types. Object ops consume these read-only values but never call a provider method.

- [ ] **Step 4: Refactor Put/Copy/Delete to the publication facade**

At the beginning of Put, before extracting/streaming the body, parse `req.headers["x-amz-tagging"]`, validate controls, and evaluate with `is_decompress_zip=false`. Keep encryption/Kubo add/local `pin_add` unchanged. Replace only the final object `upsert` with:

```rust
let publication = PublicationRequest {
    object: PublicationObject::from_put(
        object_id, bucket, key, cid.clone(), size, content_type,
        metadata, encrypted, key_wrap, sse_c_key_fingerprint, object_created_at,
    ),
    tags: policy.tags.clone(),
    policy,
    object_target: PinTargetSpec { cid: cid.clone(), logical_size: size },
};
crate::store::pinning::publication::publish_object(
    db,
    publication,
    state.pinning.provider_limits(),
).await?;
```

Set `let object_created_at = chrono::Utc::now();` immediately before constructing the request. This is the immutable S3 row timestamp; Task 7 independently captures the lease start inside the database transaction.

For Copy, implement standard directive behavior exactly: omitted or `COPY` loads source object tags and rejects a non-empty `x-amz-tagging` header; `REPLACE` requires and parses `x-amz-tagging` (an empty value means an empty tag set); any other directive is `InvalidArgument`. Evaluate against destination bucket/key and publish the new immutable object with the shared CID.

Replace DeleteObject's `delete_latest` and each DeleteObjects item's `delete_latest_if_present` with transaction functions that clear latest and close owner leases/outbox together. Preserve idempotent multi-delete output. Do not add a Kubo `pin_rm`; do not add a provider call.

- [ ] **Step 5: Run GREEN and isolation regressions**

Run:

```powershell
cargo test --lib s3::ops::object::tests::pinning_ -- --nocapture
cargo test --lib s3::ops::object::tests::delete_objects_ -- --nocapture
cargo check --lib
cargo check --all-targets
rg -n "PinningProvider|\.submit\(|\.find\(|\.unpin\(" src/s3 src/store
```

Expected: tests PASS; the search finds no provider calls in S3/store modules, and existing Kubo/local pin behavior remains covered.

- [ ] **Step 6: Record the review boundary**

Review AppState/object publication hooks. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: enqueue pinning on object publication`; otherwise record Task 9 complete without a Git write.

---

### Task 10: Multipart Tag Persistence and Complete-Only Remote Publication

**Files:**
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/s3/route/decompress_zip.rs` at direct `create_upload` test call sites

**Interfaces:**
- CreateMultipartUpload validates/persists `x-amz-tagging`; non-ZIP Complete evaluates and calls `publish_completed_upload` to atomically publish root/tags/leases/outbox while deleting the upload.
- UploadPart and AbortMultipartUpload never create remote targets/jobs and never remotely unpin parts.

- [ ] **Step 1: Write RED multipart tests for tag propagation, Complete atomicity, and UploadPart exclusion**

```rust
#[tokio::test]
async fn create_persists_tags_but_upload_part_never_creates_remote_state() {
    let state = multipart_state().await;
    let upload_id = create_with_tagging(&state, "team=storage&ipfs-s3%3Apin=true").await;
    assert_eq!(upload_tags(&state, &upload_id).await, vec![
        ObjectTag::new("ipfs-s3:pin", "true"),
        ObjectTag::new("team", "storage"),
    ]);
    upload_part(&state, &upload_id, 1, b"part").await.unwrap();
    assert_eq!(pinning_row_counts(state.store.db()).await, PinningRowCounts::zero());
}

#[tokio::test]
async fn complete_commits_root_tags_lease_outbox_and_upload_delete_together() {
    let state = multipart_state().await;
    let upload_id = seeded_tagged_upload(&state).await;
    complete_multipart_upload(&state, complete_request(&upload_id)).await.unwrap();
    let object = latest_object(&state).await;
    assert_eq!(list_object_tags(state.store.db(), &object.id).await.unwrap().len(), 2);
    assert_eq!(active_leases(state.store.db(), &object.id).await.len(), 1);
    assert!(crate::store::multipart::get_upload(state.store.db(), &upload_id).await.is_err());
    assert_eq!(pending_operations(state.store.db()).await, vec!["submit"]);
}
```

Add rollback tests for forced lease/job insertion failure preserving upload/parts and prior latest object, Complete overwrite ending old leases, request/default duration starting at Complete commit, all-mode fan-out, Create invalid controls before DB/Kubo, and Abort leaving no remote jobs. Preserve existing finalizer tests for `RolledBack`, `OutcomeUnknown→Committed`, `OutcomeUnknown→NotCommitted`, and `OutcomeUnknown→Unknown`, now using the full `PublicationRequest` and `completion_attempt_id` rather than an object-only write.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib s3::ops::multipart::tests::pinning_ -- --nocapture
cargo test --lib store::multipart::tests::pinning_ -- --nocapture
```

Expected: tests fail because Create ignores tags and Complete uses the old object/upload-only transaction.

- [ ] **Step 3: Persist normalized Create tags and carry them through completion**

Parse and validate `x-amz-tagging` at Create before writing the upload. Evaluate structural legality immediately: manual pin=true requires a matching rule; decompressed requires the existing `decompress-zip` query and `allow_decompressed`; initial retain-until and over-max duration fail. Store normalized tags in `tags_json`, but calculate lease `created_at`/`expires_at` from Complete's commit time.

Extend `CompletedMultipartArchive`:

```rust
pub struct CompletedMultipartArchive {
    pub tags: Vec<ObjectTag>,
    pub publication_policy: PublicationPolicy,
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub encryption_object_id: String,
    pub completion_attempt_id: String,
    pub root_cid: String,
    pub total_size: i64,
    pub content_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub sse_c_key_fingerprint: Option<String>,
    pub decompress_zip_target: Option<String>,
    pub decompress_zip_result: bool,
    pub server_side_encryption: Option<ServerSideEncryption>,
}
```

Update every direct `create_upload` call to pass an explicit tag slice; plain tests use `&[]`.

- [ ] **Step 4: Route Complete through the publication transaction and protect UploadPart**

For a non-decompressed Complete, `finalize_completed_multipart_archive` builds `PublicationRequest` with `object.id = completed.completion_attempt_id` and calls `publish_completed_upload(db, upload_id, request, limits)`. Adapt the existing `CompletedUploadFinalizerStore` seam to `commit_object(upload_id, request, limits)` plus `reconcile(upload_id, expected_archive)`. Preserve the existing decision table exactly: `Ok` succeeds; `RolledBack` returns its source; `OutcomeUnknown` calls `reconcile_completed_publication`; `Committed` succeeds, `NotCommitted` returns the original commit error, and `Unknown` returns `InternalError` containing both bounded diagnostics. Never repeat Complete publication after unknown outcome. Keep root Kubo add/local pin before that transaction. Remove the legacy separate object-write/upload-delete commit path after parity tests pass. Task 11 adds a typed `commit_zip` branch to this same finalizer seam.

```rust
#[async_trait::async_trait]
pub(crate) trait CompletedUploadFinalizerStore: Send + Sync {
    async fn commit_object(
        &self,
        upload_id: &str,
        request: PublicationRequest,
        limits: &ProviderLimitMap,
    ) -> Result<PublicationResult, CommitCompletedUploadError>;

    async fn reconcile(
        &self,
        upload_id: &str,
        expected_archive: &PublicationObject,
    ) -> ReconciledCommitOutcome;
}
```

Add an explicit regression assertion after UploadPart and Abort:

```rust
assert_eq!(
    crate::store::entities::pin_job::Entity::find().count(state.store.db()).await.unwrap(),
    0,
    "UploadPart and Abort must not enqueue remote work",
);
```

- [ ] **Step 5: Run GREEN**

Run:

```powershell
cargo test --lib s3::ops::multipart::tests::pinning_ -- --nocapture
cargo test --lib store::multipart::tests -- --nocapture
cargo test --lib s3::ops::multipart::tests::upload_part_ -- --nocapture
```

Expected: PASS; tags survive Create→Complete, publication is atomic, and UploadPart/Abort produce no remote pin state.

- [ ] **Step 6: Record the review boundary**

Review multipart/store changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: publish multipart pin leases`; otherwise record Task 10 complete without a Git write.

---

### Task 11: ZIP Decompressed Targets and Archive-Owned Renewal

**Files:**
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/s3/route/decompress_zip.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/zip/extract.rs` tests only if additional deterministic partial fixtures are required

**Interfaces:**
- A manual decompressed lease is owned by the archive object and targets only successfully published entry CIDs.
- Global rejection creates no manual lease; failed entries create no target; generated entries are not recursively evaluated.
- Ordinary/custom Put ZIP calls `publish_zip(db, request, limits)`; multipart Complete ZIP calls `publish_completed_zip(db, upload_id, request, limits)` through the existing completion-attempt finalizer/reconciliation flow.

- [ ] **Step 1: Write RED ZIP tests for success, partial success, global rejection, ownership, and non-recursion**

```rust
#[tokio::test]
async fn decompressed_manual_lease_targets_successful_entries_not_archive() {
    let state = zip_state_with_request_rule().await;
    let outcome = publish_zip_fixture(&state, tagged_decompressed_request(), partial_zip()).await.unwrap();
    assert_eq!(outcome.entries.iter().map(|entry| entry.cid.as_str()).collect::<Vec<_>>(), vec!["bafy-entry-ok"]);
    let archive = latest_by_key(&state, "archive.zip").await;
    let lease = manual_lease(&state, &archive.id).await;
    assert_eq!(target_cids(&state, &lease.id).await, vec!["bafy-entry-ok"]);
    assert!(!target_cids(&state, &lease.id).await.contains(&archive.cid));
    assert!(leases_for_key(&state, "prefix/entry.txt").await.is_empty());
}

#[tokio::test]
async fn archive_renewal_touches_all_entry_targets_and_entry_delete_does_not_cancel_owner() {
    let state = state_with_published_decompressed_archive().await;
    delete_entry(&state, "prefix/a.txt").await;
    renew_archive(&state, "2026-09-01T00:00:00Z").await;
    assert_eq!(active_target_cids_for_archive(&state).await, vec!["bafy-a", "bafy-b"]);
}

#[tokio::test]
async fn multipart_zip_failure_rolls_back_entries_and_keeps_upload_parts() {
    let state = multipart_zip_state_with_forced_outbox_failure().await;
    let completed = completed_archive("upload-1", "completion-attempt-1");
    let request = zip_request(&completed, staged_entries());
    let error = finalize_completed_multipart_zip(&state, &completed, request).await.unwrap_err();
    assert_eq!(error.code().as_str(), "InternalError");
    assert!(upload_exists(&state, "upload-1").await);
    assert_eq!(part_count(&state, "upload-1").await, 2);
    assert_eq!(new_archive_entry_and_pinning_row_count(&state).await, 0);
}

#[tokio::test]
async fn test_pinning_multipart_zip_outcome_unknown_reconciles_exact_attempt() {
    let completed = completed_archive("upload-1", "completion-attempt-1");
    let request = zip_request(&completed, staged_entries());
    let committed = FakeFinalizerStore::outcome_unknown_then(ReconciledCommitOutcome::Committed);
    assert!(finalize_completed_multipart_zip_with_store(&completed, request.clone(), &limits(), &committed).await.is_ok());
    let not_committed = FakeFinalizerStore::outcome_unknown_then(ReconciledCommitOutcome::NotCommitted);
    assert!(finalize_completed_multipart_zip_with_store(&completed, request.clone(), &limits(), &not_committed).await.is_err());
    let unknown = FakeFinalizerStore::outcome_unknown_then(ReconciledCommitOutcome::Unknown(
        AppError::Internal("reconciliation unavailable".to_owned()),
    ));
    assert!(finalize_completed_multipart_zip_with_store(&completed, request, &limits(), &unknown).await.is_err());
    assert_eq!(committed.reconciled_attempt_ids(), vec!["completion-attempt-1"]);
    assert_eq!(not_committed.reconciled_attempt_ids(), vec!["completion-attempt-1"]);
    assert_eq!(unknown.reconciled_attempt_ids(), vec!["completion-attempt-1"]);
}
```

Add cases for archive plus automatic object lease (automatic targets archive while manual targets entries), one/all provider modes, failed extraction entry omitted, archive-key collision/global ZIP rejection creating no object/lease/outbox changes, cancellation/delete/overwrite archive ending all entry targets, and later independent S3 Put on an entry creating its own separate lease. Expire an archive-owned decompressed lease, confirm all entry remotes absent/released, then attempt retain-until renewal and assert `InvalidArgument`, unchanged expired lease/target row IDs, and zero newly extracted/reconstructed targets; the route must never reopen or re-extract ZIP content during tagging. Add multipart ZIP success asserting archive/entries/tags/leases/targets/usage/jobs commit and upload/parts disappear together. Add fake-finalizer cases for `OutcomeUnknown→Committed` (return S3 success), `OutcomeUnknown→NotCommitted` (return original error with upload/parts retained), and `OutcomeUnknown→Unknown` (return InternalError and do not guess/retry), all keyed by the exact `completion_attempt_id`.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib s3::route::decompress_zip::tests::pinning_ -- --nocapture
```

Expected: tests fail because the route publishes archive/entries separately and cannot atomically bind archive-owned targets.

- [ ] **Step 3: Stage extraction, then choose the typed ordinary or multipart ZIP transaction**

For both custom Put and multipart Complete routes, perform the common pre-transaction work:

1. Parse/validate archive tags and policy before archive Kubo add.
2. Stage archive and run streaming extraction exactly as today.
3. Preserve `ExtractOutcome.failures`; keep only successfully added/pinned entries in `ExtractOutcome.entries`.
4. Perform archive-key collision/global checks before database mutation.
5. Build `ZipPublicationRequest { archive, entries }` with archive object ID equal to the completion attempt for multipart.

Then branch exactly once:

```rust
pub async fn finalize_completed_multipart_zip(
    state: &Arc<AppState>,
    completed: &CompletedMultipartArchive,
    request: ZipPublicationRequest,
) -> S3Result<PublicationResult>;

async fn finalize_completed_multipart_zip_with_store<S: CompletedUploadFinalizerStore + ?Sized>(
    completed: &CompletedMultipartArchive,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
    store: &S,
) -> S3Result<PublicationResult>;

// Ordinary/custom Put ZIP:
publish_zip(state.store.db(), request, state.pinning.provider_limits()).await?;

// Multipart Complete ZIP:
finalize_completed_multipart_zip(state, &completed, request).await?;
```

Extend Task 10's finalizer seam with:

```rust
async fn commit_zip(
    &self,
    upload_id: &str,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> Result<PublicationResult, CommitCompletedUploadError>;
```

`DatabaseCompletedUploadFinalizer::commit_zip` calls `publish_completed_zip`. `finalize_completed_multipart_zip` verifies `request.archive.object.id == completed.completion_attempt_id`, calls `commit_zip`, and applies the same RolledBack/OutcomeUnknown reconciliation decision table as Task 10 against `request.archive.object`. Only after committed/reconciled success does the route build response XML from the committed successful-entry list plus extraction failures. It never calls ordinary `publish_zip` for multipart and never separately finalizes/deletes the upload.

Both transaction variants write archive and entry object rows, give tags only to the archive, create no policy state for generated entries, target manual decompressed leases to committed entry CIDs, and target any automatic lease to the archive CID. The multipart variant additionally deletes upload/parts in that same transaction. A database failure rolls back the entire DB publication/outbox/upload deletion while retaining staged conservative local Kubo pins.

- [ ] **Step 4: Run GREEN and local-pin regressions**

Run:

```powershell
cargo test --lib s3::route::decompress_zip::tests::pinning_ -- --nocapture
cargo test --lib test_pinning_multipart_zip_outcome_unknown_reconciles_exact_attempt -- --nocapture
cargo test --lib zip::extract::tests -- --nocapture
cargo test --lib s3::route::decompress_zip::tests -- --nocapture
```

Expected: PASS; only committed successful entries become targets, ordinary and multipart ZIP use their distinct typed transaction paths, multipart rollback/success and completion-attempt reconciliation are atomic/deterministic, archive ownership controls renewal/end, and all existing streaming/global-reject/local-pin tests remain green.

- [ ] **Step 5: Record the review boundary**

Review ZIP route/publication changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: pin decompressed ZIP entries`; otherwise record Task 11 complete without a Git write.

---

### Task 12: Persistent Worker Submit, Reconciliation, Polling, Retry, and Crash Recovery

**Files:**
- Modify: `Cargo.toml`
- Create: `src/pinning/worker.rs`
- Modify: `src/pinning/coordinator.rs`
- Modify: `src/pinning/mod.rs`
- Modify: `src/store/pinning/jobs.rs`
- Modify: `src/store/pinning/leases.rs`

**Interfaces:**
- Extend Task 6's existing `PinningCoordinator` with provider runtime/rate state, `PinningCoordinator::start`, and `PinningWorkerHandle::shutdown`; do not recreate the coordinator foundation or its accessors.
- Enable `tokio-util` `rt` before compiling the first `CancellationToken` use and Tokio `test-util` before compiling paused-clock tests.
- Dispatch target-scoped Submit/Poll through lease-generation guards and remote-scoped Unpin/Reconcile through remote-epoch/desired-reference guards.
- Persist request IDs and normalized status; stable submit metadata is `gateway_job_id`, `gateway_lease_id`, and `gateway_target_id`.
- Execute reclaimed/`recovering` Submit via find-before-generation-check/POST, and execute all-mode failed-request cleanup/resubmission through one remote-scoped durable retry owner.

- [ ] **Step 1: Write RED deterministic worker tests for submit→poll→pinned, ambiguous adoption, retries, and locks**

```rust
#[tokio::test(start_paused = true)]
async fn submit_then_poll_reaches_the_availability_success_line() {
    let fixture = worker_fixture(script_provider([
        Script::Submit(remote("request-1", RemotePinStatus::Queued)),
        Script::Get("request-1", remote("request-1", RemotePinStatus::Pinning)),
        Script::Get("request-1", remote("request-1", RemotePinStatus::Pinned)),
    ])).await;
    fixture.enqueue_submit("lease-1", "target-1", 1).await;
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().submit_count(), fixture.provider().get_count()), (1, 0));
    let poll_id = fixture.only_poll_job().await.id;
    assert_eq!(fixture.only_poll_job().await.state, "pending");
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().submit_count(), fixture.provider().get_count()), (1, 1));
    assert_eq!(fixture.job(&poll_id).await.state, "pending");
    assert_eq!(fixture.pin_job_count_by_operation("poll").await, 1);
    tokio::time::advance(std::time::Duration::from_secs(5)).await;
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().submit_count(), fixture.provider().get_count()), (1, 2));
    assert_eq!(fixture.job(&poll_id).await.state, "done");
    assert_eq!(fixture.job(&poll_id).await.attempts, 0);
    assert_eq!(fixture.remote_pin().await.request_id.as_deref(), Some("request-1"));
    assert_eq!(fixture.remote_pin().await.status, "pinned");
    assert_eq!(fixture.target().await.state, "pinned");
    assert!(fixture.lease_has_available_provider().await);
}

#[tokio::test(start_paused = true)]
async fn ambiguous_submit_finds_and_adopts_existing_requestid_before_retry() {
    let fixture = worker_fixture(script_provider([
        Script::SubmitError(ProviderErrorClass::Ambiguous),
        Script::Find(vec![remote("adopted-7", RemotePinStatus::Pinning)]),
    ])).await;
    fixture.enqueue_submit("lease-7", "target-7", 4).await;
    fixture.run_one().await;
    assert_eq!(fixture.remote_pin().await.request_id.as_deref(), Some("adopted-7"));
    assert_eq!(fixture.provider().submit_count(), 1);
}

#[tokio::test]
async fn accepted_post_crash_is_reclaimed_found_and_never_posted_twice() {
    let fixture = worker_fixture(script_provider([
        Script::Find(vec![remote("accepted-before-crash", RemotePinStatus::Queued)]),
    ])).await;
    fixture.seed_provider_pin_from_crashed_post("accepted-before-crash").await;
    fixture.seed_expired_running_submit("lease-1", "target-1", 1, "calling").await;
    fixture.run_one_due().await;
    assert_eq!(fixture.provider().find_count(), 1);
    assert_eq!(fixture.provider().submit_count(), 1, "the only POST is the pre-crash request");
    assert_eq!(fixture.remote_pin().await.request_id.as_deref(), Some("accepted-before-crash"));
    assert_eq!(fixture.only_poll_job().await.state, "pending");
}

#[tokio::test(start_paused = true)]
async fn reclaimed_submit_find_none_with_no_desired_ref_never_posts_and_releases_only_after_reconcile() {
    let fixture = worker_fixture(script_provider([Script::Find(vec![])])).await;
    fixture.seed_expired_running_submit("lease-1", "target-1", 1, "calling").await;
    fixture.cancel_only_target().await;
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().find_count(), fixture.provider().submit_count()), (1, 0));
    assert_eq!(fixture.usage("pinata").await, (100, 1));
    assert_eq!(fixture.submit_job().await.state, "done");
    fixture.run_current_reconcile().await;
    assert_eq!(fixture.remote_pin().await.status, "absent");
    assert_eq!(fixture.usage("pinata").await, (0, 0));
}

#[tokio::test]
async fn target_cancel_during_blocked_submit_keeps_usage_until_ambiguity_converges() {
    let fixture = worker_fixture(blocked_submit_provider()).await;
    fixture.start_submit_and_wait_until_post_is_blocked().await;
    assert_eq!(fixture.submit_job().await.submit_phase.as_deref(), Some("calling"));
    fixture.cancel_only_target_and_run_reconcile().await;
    assert_eq!(fixture.usage("pinata").await, (100, 1));
    assert_eq!(fixture.remote_pin().await.status, "reserved");
    assert!(fixture.current_reconcile_is_rescheduled().await);
    fixture.crash_worker_without_unlocking().await;
    fixture.release_provider_as_accepted("request-after-cancel").await;
    fixture.advance_past_lock_and_reclaim().await;
    assert_eq!(fixture.provider().find_count(), 1);
    assert_eq!(fixture.provider().submit_count(), 1);
    assert!(fixture.has_current_unpin().await);
    fixture.run_unpin_success().await;
    assert_eq!(fixture.usage("pinata").await, (0, 0));
}

#[tokio::test(start_paused = true)]
async fn failed_all_remote_forgets_old_request_then_submits_once_and_resets_when_pinned() {
    let fixture = worker_fixture(script_provider([
        Script::UnpinOk("failed-request-1"),
        Script::Submit(remote("request-2", RemotePinStatus::Pinned)),
    ])).await;
    fixture.seed_due_failed_all_remote("failed-request-1", 1, 7, 100).await;
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().unpin_count(), fixture.provider().submit_count()), (1, 0));
    assert_eq!(fixture.remote_pin().await.epoch, 8);
    assert_eq!(fixture.usage("pinata").await, (100, 1));
    fixture.run_one_due().await;
    assert_eq!((fixture.provider().unpin_count(), fixture.provider().submit_count()), (1, 1));
    assert_eq!(fixture.remote_failure_state().await, (0, None));
    assert_eq!(fixture.remote_pin().await.status, "pinned");
}
```

Add tests for conflict→find none→persisted `recovery_backoff`→bounded delayed POST, reclaimed/ambiguous find returning multiple→protocol retry that remains `recovering`, auth terminal degradation, 429 `Retry-After`, 5xx/transport backoff, malformed protocol recovery without request ID, failed normalized status, restart reclaim after expired lock, duplicate poll idempotency, duplicate unpin idempotency, and redacted logs/errors. Assert reclaimed Submit performs find before a stale generation/target early exit; a matching adopted request with no desired refs creates Unpin rather than POST; zero matches with a still-desired target persists backoff and only a later fresh claim may POST; cancellation/restart between lookup/call phases cannot bypass find; and eight consecutive failed recovery finds cap attempts/backoff but remain `pending/recovering`, keep usage, reschedule Reconcile, and issue zero POST until one conclusive zero/one-match lookup resolves ambiguity. Add a blocked old Poll response after failed-request DELETE/epoch bump and assert `ExistingRequest` returns stale, cannot restore the forgotten request ID, and cannot increment `failure_attempts`. Add shared-CID tests where automatic+manual+Copy targets share one reserved remote: assert exactly one POST, queued/pinning projects all targets to submitted, final pinned projects all to pinned, and a later target is immediately pinned with no POST/GET. Add terminal-failure projection across multiple one/all leases and a stale/cancelled Poll-owner test asserting current-epoch Reconcile selects the next canonical target, ensures a new current-generation Poll, and does not issue another POST.

Add the remote-scope race tests with a provider DELETE barrier:

```rust
#[tokio::test]
async fn stale_remote_epoch_skips_unpin_before_provider_call() {
    let fixture = worker_fixture(script_provider([])).await;
    fixture.seed_remote_unpin_job("pinata", "bafy-race", 4).await;
    fixture.bump_remote_epoch_with_new_target("pinata", "bafy-race", 5).await;
    fixture.run_one().await;
    assert_eq!(fixture.provider().unpin_count(), 0);
    assert!(fixture.has_reconcile("pinata", "bafy-race", 5).await);
}

#[tokio::test]
async fn expired_manual_reactivation_during_blocked_delete_compensates_to_pinned() {
    let fixture = worker_fixture(blocked_delete_then_pinned_provider()).await;
    fixture.seed_due_active_manual_lease("object-latest", "lease-manual", "target-1", "pinata", "bafy-race", 8, 100).await;
    fixture.run_expiry_scan().await;
    assert_eq!(fixture.lease_state("lease-manual").await, "expired");
    let expired_generation = fixture.lease_generation("lease-manual").await;
    let delete_epoch = fixture.remote_pin().await.epoch;
    fixture.start_one_and_wait_for_delete().await;
    fixture.reactivate_expired_manual("object-latest", "lease-manual", time(30)).await.unwrap();
    assert_eq!(fixture.lease_state("lease-manual").await, "active");
    assert_eq!(fixture.lease_generation("lease-manual").await, expired_generation + 1);
    assert_eq!(fixture.remote_pin().await.epoch, delete_epoch + 1);
    fixture.release_delete_with_success().await;
    fixture.run_compensation_until_pinned().await;
    assert_eq!(fixture.usage("pinata").await, (100, 1));
    assert_eq!(fixture.remote_pin().await.status, "pinned");
    assert_eq!(fixture.provider().submit_count(), 1);
}
```

Repeat the blocked-DELETE test with a newly published shared target and with provider NotFound; both must preserve usage and enqueue one current lease-generation Submit. Add the exact expiry path: `expire_due_leases` marks the manual lease expired and enqueues DELETE; block that DELETE; execute PutObjectTagging retain-until against the same latest owner; assert lease generation and each eligible remote epoch advance once, then return DELETE success and run the compensation Submit/Poll to pinned with unchanged usage. A control case with unchanged epoch/no refs must release exactly once. Also drive eight distinct failed request IDs through failed→due Reconcile→DELETE/NotFound→epoch bump→Submit→failed; assert attempt eight leaves every all target degraded, `next_retry_at=NULL`, and no pending/running due Reconcile.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::worker::tests -- --nocapture
```

Expected: compile failure because the worker/runtime extension is absent, `tokio-util::sync::CancellationToken` is not enabled yet, and paused-clock `advance` lacks Tokio `test-util`; the Task 6 coordinator foundation itself already compiles.

- [ ] **Step 3: Enable CancellationToken and extend the existing coordinator with runtime state**

Change the existing dependency before adding any `CancellationToken` import:

```toml
tokio = { version = "1", features = ["full", "test-util"] }
tokio-util = { version = "0.7", features = ["io", "compat", "rt"] }
```

Keep Task 6's `PinningCoordinator` fields/accessors and add `provider_runtime`; do not redeclare its policy, providers, limits, settings, `build`, or `disabled_for_test` APIs:

```rust
// Add this field to the existing Task 6 PinningCoordinator definition.
provider_runtime: std::collections::HashMap<String, ProviderRuntime>,

pub struct ProviderRuntime {
    pub priority: u32,
    pub concurrency: Arc<tokio::sync::Semaphore>,
    pub min_request_interval: std::time::Duration,
    pub health: Arc<tokio::sync::RwLock<ProviderHealth>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealth { Healthy, Degraded, Terminal }

pub struct PinningWorkerHandle {
    cancellation: tokio_util::sync::CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl PinningCoordinator {
    pub fn start(
        self: &Arc<Self>,
        store: Store,
        parent: tokio_util::sync::CancellationToken,
    ) -> PinningWorkerHandle;
}

impl PinningWorkerHandle {
    pub async fn shutdown(self, grace: std::time::Duration);
}
```

Extend Task 6's `build` body to create one semaphore/rate gate per existing provider and keep config priority. Default external-provider rate is one request/second unless positive `requests_per_second` overrides it; Noop has no sleep. Reuse Task 6's `WorkerSettings` unchanged. `start(self: &Arc<Self>, store: Store, parent: CancellationToken) -> PinningWorkerHandle` spawns one claim loop; each claimed job acquires both global and provider permits before HTTP.

- [ ] **Step 4: Implement exact job execution and retry rules**

Branch by the claimed model's persisted operation scope; preserve `reclaimed` for Submit recovery and do not unconditionally reload lease/target for remote jobs:

```rust
let operation = claimed.model.operation.clone();
match operation.as_str() {
    "submit" | "poll" => execute_target_job(&store, &coordinator, claimed).await,
    "unpin" | "reconcile" => execute_remote_job(&store, &coordinator, claimed.model).await,
    operation => Err(AppError::Internal(format!("unknown pin job operation: {operation}"))),
}
```

`execute_target_job` requires non-null lease_id/target_id/expected_generation and null expected_remote_epoch. Poll follows the ordinary generation guard, but Submit ordering is phase-sensitive:

1. If `claimed.reclaimed` is true or persisted phase is `recovering`, call `provider.find(FindPin::for_job(cid, stable_job_id))` **before** loading/early-exiting on target generation. Exactly one matching CID+metadata result is adopted through `apply_remote_status`; multiple matches are protocol retry through `retry_submit_recovery`; transient list error remains `recovering`. No POST is possible in these branches.
2. A zero-result recovery calls `record_submit_recovery_no_match`. With no desired target it completes Submit, ensures current Reconcile, and never POSTs. With a desired target it persists `recovery_backoff` and returns pending for at least `base_backoff`; it still never POSTs in that execution.
3. A fresh `ready`/due `recovery_backoff` Submit reloads lease/target and calls `prepare_submit_call`, which atomically verifies current generation, desired state, canonical ownership, and the claim lock before setting `calling`. `NoLongerDesired` completes without provider mutation and ensures Reconcile. Only `ReadyToCall` may build and POST this request:

```rust
let job = &claimed.model;
let metadata = BTreeMap::from([
    ("gateway_job_id".to_owned(), job.id.clone()),
    ("gateway_lease_id".to_owned(), lease.id.clone()),
    ("gateway_target_id".to_owned(), target.id.clone()),
]);
let request = SubmitPin {
    cid: job.cid.clone(),
    name: format!("{}/{}", owner.bucket, owner.key),
    metadata,
};
```

The stable metadata is written from the claimed job before `prepare_submit_call`; it never changes across restart. On Submit/find success, open one DB transaction and call `apply_remote_status` with `RemoteStatusOrigin::Adopt`; this persists the remote-owned request/status and projects every active shared target. A `StaleRequest` response cannot overwrite a newer remote request: complete this Submit and enqueue current-epoch Reconcile. For queued/pinning `Applied`, select the canonical lowest `(created_at,target_id)` active target, call `ensure_or_reactivate_poll_job` with that target's current lease generation/request ID and `next_attempt_at = now + poll_interval`, then complete the Submit. For pinned, complete Submit after projection. For failed, complete Submit after projection, pass every returned one-mode lease to Task 13, then call `ensure_failed_remote_retry` once for the shared remote after one-mode desired-set changes.

An ambiguous/409/timeout after `calling` persists phase `recovering` before releasing the claim, then performs or schedules `find(FindPin::for_job(cid, job_id))`; exactly one matching result uses the same adoption path, zero uses recovery backoff, and multiple is protocol retry. If the process dies after provider acceptance but before request-ID persistence, the row remains running/`calling`; expired-lock claim atomically changes it to `recovering`, find adopts it, and POST count remains one. A matching result found after all desired refs ended is still persisted first, then current-epoch Unpin is enqueued; quota is not released from a stale target check. Never issue a second POST before a durable zero-result lookup and subsequent backoff have completed.

For Poll, require persisted request ID after the target generation guard and call get outside the transaction. Apply the observation with `RemoteStatusOrigin::ExistingRequest` and mutate ownership in one DB transaction. `StaleRequest` completes the old Poll and enqueues current-epoch Reconcile without applying its response. On queued/pinning `Applied`, reload the canonical active target: if it is still this job's target/current generation, call `reschedule_poll_job(claimed.model.id, claimed.model.locked_until.unwrap(), now, settings.poll_interval)`; if ownership changed while GET was in flight, complete the old Poll and call `ensure_or_reactivate_poll_job` for the new canonical target/request at the bounded interval. Normal continuation therefore never inserts another row, while stale-owner handoff is the explicit coordination takeover allowed to complete the old row. Pinned `Applied` completes that Poll after projection and resets shared failure budget. Failed `Applied` completes Poll, coordinates each returned one-mode lease, then calls `ensure_failed_remote_retry` once for remaining all-mode refs; duplicate same-request failed GETs neither increment the remote count nor create another Reconcile. Stale/no-longer-needed before the call completes it and lets current Reconcile ensure ownership. Retryable provider errors call `retry_job` and increment attempts instead of successful-Poll reschedule.

`execute_remote_job` requires null lease_id/target_id/expected_generation and non-null expected_remote_epoch, then reloads only remote pin plus active desired targets. A stale epoch completes without HTTP and inserts/retains current-epoch Reconcile. Reconcile converges from current state:

- desired refs + no request + `reserved` selects the canonical current target and calls `ensure_or_reactivate_submit_job`;
- desired refs + queued/pinning request selects the canonical target and calls `ensure_or_reactivate_poll_job`, retaining pending/live-running work or reactivating done work;
- pinned calls `apply_remote_status` with `ExistingRequest` and the persisted request/status to project every current target pinned without a job;
- failed first applies the persisted observation with `ExistingRequest` idempotently and coordinates every returned one-mode lease. It then reloads the shared remote/current desired set. With no all-mode desired target, normal reconcile/unpin convergence owns any obsolete request. With an all-mode target and attempts < 8, it reschedules the same stable Reconcile to `next_retry_at` when not yet due. Once due, it calls `provider.unpin(failed_request_id)` outside any DB transaction to forget the failed PSA request; success/NotFound calls `prepare_failed_remote_resubmit`, which rechecks epoch/request/status/count/all target, keeps usage, clears the old identity/error, sets reserved, bumps epoch once, completes old-request Poll rows, and ensures one canonical current-generation `ready` Submit. A transient/rate-limited DELETE retries the same Reconcile; stale completion cannot clear newer state. At attempts 8, it completes with all remaining targets degraded and no due loop;
- no desired refs + request ID enqueues/executes current-epoch Unpin;
- no desired refs + no request calls Task 6's `complete_no_request_remote_absence`. `Wait` uses `reschedule_reconcile_job` at the returned lock-expiry/next-attempt time and retains status/usage; `Released` completes the Reconcile after guarded absence/decrement (Task 14 later wakes waiters); `Stale` ensures the current-epoch Reconcile. A never-started pending `ready` or post-zero-find `recovery_backoff` Submit may be cancelled; running/reclaimed/`calling`/`recovering` work cannot.

If a Submit/Poll response finds no active desired target after HTTP, persist/adopt the remote request and enqueue current-epoch Reconcile/Unpin rather than leaking it. If the old Submit/Poll target became stale, Reconcile hands ownership to the next canonical current target using its current lease generation; an existing remote request always chooses Poll, never another POST. Failed-request DELETE, provider find/get/submit, and ordinary Unpin all occur outside transactions; only their guarded durable transitions occur inside.

For Unpin, perform the pre-call epoch/no-active-ref check immediately before `provider.unpin(request_id)`. After success or NotFound, call Task 6's `complete_remote_delete` transaction. `Released` decrements usage once; `Compensated` means DELETE is known complete and active desired refs exist, so usage stays reserved, request ID remains cleared, remote status is `reserved`, and `ensure_or_reactivate_submit_job` makes the canonical current target-generation Submit pending even if that stable job ID had already completed before DELETE returned. `ReconcileRequired` keeps reservation and current remote reconcile when the epoch changed but no refs remain. Never route remote Unpin/Reconcile through a lease-generation comparison.

For transport/job retry delay use `Retry-After` when present; otherwise checked exponential backoff `min(base * 2^attempts, max)` plus jitter uniformly bounded to 0–25% of the capped delay. This job-attempt counter is separate from `remote_pins.failure_attempts`, whose deterministic durable failed-request backoff is defined in Task 6. Authentication marks provider health terminal/degraded and stops rapid retries. Quota enters Task 14's eviction path. Ordinary protocol/transient/rate-limit retries are bounded by settings; exhaustion enters coordination rather than deleting S3 data. Submit recovery find is the safety exception: after attempts cap, it keeps the one stable `recovering` row at max backoff until a zero/one-match answer resolves ambiguity, with no POST/quota release. No exhausted or ambiguity path leaves an immediately due row.

Emit one structured tracing event per transition with `provider`, `cid`, `object_id`, `lease_id`, `target_id`, `job_id`, old/new state, and remote request ID when known. Do not log authorization headers, `SecretToken`, request bodies, or raw provider bodies.

- [ ] **Step 5: Implement cancellation-aware claim/drain behavior**

The loop uses `tokio::select!` over cancellation and worker interval. Before each claim batch, call `expire_due_leases(now)`; expiry preserves original target rows, marks the lease/targets terminal, increments lease generation/remote epochs, then enqueues epoch-bound Reconcile/Unpin jobs. Wake quota waiters whose provider has confirmed headroom. A later allowed PutObjectTagging renewal runs Task 6's same-owner/original-target reactivation and makes pre-call Unpin stale or post-call DELETE compensating. Once cancelled, stop expiry scans and claiming immediately, wait up to `shutdown_grace` for spawned in-flight tasks, abort only local task handles after the grace, and leave unfinished DB locks/phases to expire naturally. In particular, a blocked Submit remains `running/calling`; do not clear its phase or lock, so the next claimant sets `recovering` and finds before POST.

- [ ] **Step 6: Run GREEN**

Run:

```powershell
cargo test --lib pinning::worker::tests -- --nocapture
cargo check --lib
```

Expected: PASS; `rt` compiles before `CancellationToken` and `test-util` enables deterministic paused-clock advancement; queued→pinning→pinned uses exactly one POST/two interval-separated GETs/one stable Poll row; shared status projects all targets; stale owner hands Poll to a current target without duplicate POST; reclaimed/ambiguous Submit always finds before stale-target exit or another POST; accepted-before-crash is adopted with zero second POST; no-match/no-ref resolves without POST and releases only after guarded Reconcile; running Submit ambiguity retains quota; failed all-mode requests use one remote backoff/Reconcile, forget old request, bump epoch, and resubmit until pinned or eight-cycle stop; target vs remote dispatch, stale pre-call skip, expiry-renewal blocked-DELETE compensation, stable request ID adoption, lock recovery, and graceful cancellation are deterministic.

- [ ] **Step 7: Record the review boundary**

Review coordinator/worker/job transitions. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: run persistent pinning worker`; otherwise record Task 12 complete without a Git write.

---

### Task 13: One-Mode Sticky Failover and All-Mode Independent Convergence

**Files:**
- Modify: `src/pinning/coordinator.rs`
- Modify: `src/pinning/worker.rs`
- Modify: `src/store/pinning/leases.rs`
- Modify: `src/store/pinning/publication.rs`

**Interfaces:**
- One mode persists one sticky assignment, retries transient errors in place, fails over only on terminal/exhausted/evicted state, and reconciles temporary duplicate pins.
- All mode dispatches all referenced providers independently; one pinned target is the availability success line while remaining targets can stay degraded/retrying.

- [ ] **Step 1: Write RED coordination tests**

```rust
#[tokio::test(start_paused = true)]
async fn one_mode_is_sticky_for_transient_failure_then_fails_over_terminally() {
    let fixture = one_mode_fixture(["pinata", "filebase"]).await;
    fixture.provider("pinata").script_transient_then_terminal();
    fixture.provider("filebase").script_pinned("filebase-request");
    fixture.run_transient_attempt().await;
    assert_eq!(fixture.target_providers().await, vec!["pinata"]);
    fixture.exhaust_primary_and_run_failover().await;
    assert_eq!(fixture.target_providers().await, vec!["filebase", "pinata"]);
    assert_eq!(fixture.active_assignment().await, "filebase");
}

#[tokio::test(start_paused = true)]
async fn all_mode_keeps_partial_success_and_continues_degraded_provider() {
    let fixture = all_mode_fixture(["pinata", "filebase"]).await;
    fixture.provider("pinata").script_pinned("p-1");
    fixture.provider("filebase").script_transient_forever();
    fixture.run_due().await;
    assert!(fixture.lease_has_available_provider().await);
    assert_eq!(fixture.target_state("pinata").await, "pinned");
    assert_eq!(fixture.target_state("filebase").await, "degraded");
    assert!(fixture.has_pending_retry("filebase").await);
}

#[tokio::test]
async fn shared_terminal_remote_failure_coordinates_every_affected_lease_independently() {
    let fixture = shared_failure_fixture([("one-a", "one"), ("one-b", "one"), ("all-c", "all")]).await;
    fixture.apply_terminal_remote_failure("pinata", "bafy-shared", "request-1").await;
    assert_eq!(fixture.target_states_for_cid("bafy-shared").await, vec!["degraded", "degraded", "degraded"]);
    fixture.coordinate_returned_outcomes().await;
    assert_eq!(fixture.failover_count("one-a").await, 1);
    assert_eq!(fixture.failover_count("one-b").await, 1);
    assert_eq!(fixture.failover_count("all-c").await, 0);
    assert!(fixture.has_bounded_all_retry("all-c").await);
}

#[tokio::test(start_paused = true)]
async fn shared_one_and_all_failure_fails_over_one_but_retries_remote_once_for_all() {
    let fixture = shared_failure_fixture([("one-a", "one"), ("all-b", "all"), ("all-c", "all")]).await;
    fixture.observe_failed("pinata", "bafy-shared", "failed-request-1").await;
    fixture.coordinate_returned_outcomes().await;
    assert_eq!(fixture.failover_count("one-a").await, 1);
    assert_eq!(fixture.target_state("all-b", "pinata").await, "degraded");
    assert_eq!(fixture.target_state("all-c", "pinata").await, "degraded");
    assert_eq!(fixture.remote_failure_attempts("pinata", "bafy-shared").await, 1);
    assert_eq!(fixture.remote_reconcile_count("pinata", "bafy-shared").await, 1);
    fixture.advance_to_failed_retry().await;
    fixture.run_failed_request_delete_then_submit_pinned().await;
    assert_eq!(fixture.remote_failure_attempts("pinata", "bafy-shared").await, 0);
    assert!(fixture.lease_available("all-b").await);
    assert!(fixture.lease_available("all-c").await);
}
```

Add priority order/disabled/degraded skip tests, ambiguous old request plus replacement temporary duplicate, replacement-pinned then old-provider remote-scoped unpin, old-provider NotFound convergence, all-mode quota eviction preserving other targets, and one-mode eviction failover. Assert every failover desired-set change increments the old/new `(provider,cid)` epochs exactly once and all resulting Submit versus Unpin/Reconcile rows satisfy their target/remote scope fields. Add shared pinned projection tests proving every automatic/manual/Copy lease becomes available, and stale-owner handoff tests proving another active target owns Poll while lease availability remains derived from all projected targets. Drive eight distinct failed request IDs for one provider/CID shared by two all leases: attempts 1–7 each create one remote Reconcile and one replacement Submit after DELETE/NotFound; attempt 8 leaves both degraded with `next_retry_at=NULL`, no due row, and unchanged usage. Reapply each failed response twice to prove idempotent counting. Then publish one genuinely new target and separately perform a generation-advancing manual extension/reactivation to prove only `reset_failed_remote_retry_on_user_touch` restarts the budget; an equal renewal or ordinary worker scan does not.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::coordinator::tests -- --nocapture
```

Expected: tests fail because worker failure handling does not yet create/finalize replacement targets.

- [ ] **Step 3: Implement persisted one/all transitions**

Add transaction functions:

```rust
pub async fn fail_one_target<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    failed_target_id: &str,
    ordered_providers: &[String],
    limits: &ProviderLimitMap,
    now: DateTimeUtc,
) -> AppResult<Option<pin_lease_target::Model>>;

pub async fn converge_one_after_replacement<C: ConnectionTrait>(
    db: &C,
    lease_id: &str,
    winning_target_id: &str,
    now: DateTimeUtc,
) -> AppResult<Vec<NewPinJob>>;
```

For one, consume `apply_remote_status`'s complete `AffectedLeaseOutcome` list rather than coordinating only the job-owning target. For each outcome with `provider_mode == One` and `available == false`, call `fail_one_target` separately using that lease/target; retain failed/ambiguous records, choose the next higher priority enabled/healthy provider that can reserve, persist the replacement before target-scoped Submit, increment affected old/new remote epochs, and never move back during ordinary retries. Once replacement is pinned, remove the old desired target, bump its remote epoch, and enqueue remote-scoped Reconcile/Unpin for every unnecessary older provider request. If older status is ambiguous, Reconcile queries before deleting. Brief double pinning is accepted; final convergence leaves one required provider target per affected one-mode lease. One-mode handling never increments or duplicates the shared failure counter; it only changes that lease's desired set.

For all, never call one-mode failover and never cancel sibling targets when one shared remote fails or pins. Keep each projected failed target degraded and call `ensure_failed_remote_retry` once after all one-mode outcomes are processed. The retry owner is `remote_pins(provider,cid)`: one count, one `next_retry_at`, one current-epoch Reconcile, one failed-request DELETE, and one canonical replacement Submit regardless of target/lease count. Distinct failed requests advance the count; a duplicate same-request observation does not. Pinned resets it. At eight failures, leave targets degraded and complete any Reconcile instead of immediately requeueing; only a new publication target, generation-advancing active extension/expired reactivation, or explicit administrative call to `reset_failed_remote_retry_on_user_touch` starts a new bounded budget. Equal renewal is not a reset. Derive each lease's availability as `any(current desired target.state == "pinned")`, not all-target success; `apply_remote_status(Pinned)` makes every lease sharing that provider/CID immediately observe the correct availability.

- [ ] **Step 4: Run GREEN**

Run:

```powershell
cargo test --lib pinning::coordinator::tests -- --nocapture
cargo test --lib pinning::worker::tests::coordination_ -- --nocapture
```

Expected: PASS; one is priority-sticky with bounded failover/duplicate convergence, every shared affected one lease coordinates independently, all fans out with partial success and one shared failed-request retry owner, duplicate failures count once, eight cycles stop without a due loop, explicit user touch can reset the budget, and lease availability follows projected shared remote state rather than job ownership.

- [ ] **Step 5: Record the review boundary**

Review one/all transitions. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: coordinate pinning providers`; otherwise record Task 13 complete without a Git write.

---

### Task 14: Soft Quotas, Oversize Blocking, Oldest Eviction, and Confirmed Release

**Files:**
- Modify: `src/pinning/quota.rs`
- Modify: `src/pinning/worker.rs`
- Modify: `src/store/pinning/quota.rs`
- Modify: `src/store/pinning/leases.rs`

**Interfaces:**
- Enforce per-provider unique CID logical bytes and pin count, select oldest by derived active-target touch, and release only after successful unpin/NotFound.
- Explicit provider quota errors enter the same bounded eviction path; provider-observed usage is advisory only.
- Treat failed-request replacement and every running/recovering Submit as capacity-holding; no-request Reconcile may release only after durable Submit ambiguity is clear.
- Wake FIFO waiters with `wake_provider_waiters` only after a guarded release result, then project each newly reserved target through shared remote ownership.

- [ ] **Step 1: Write RED quota/eviction tests including shared references and races**

```rust
#[tokio::test]
async fn oversize_target_is_quota_blocked_without_evicting_unrelated_pins() {
    let fixture = quota_fixture(100, 10).await;
    fixture.seed_pinned("old", 50, time(1)).await;
    let outcome = fixture.reserve("oversize", 101, time(2)).await;
    assert_eq!(outcome, ReservationOutcome::QuotaBlocked);
    assert_eq!(fixture.target_state("oversize").await, "quota_blocked");
    assert!(fixture.pending_unpins().await.is_empty());
    assert_eq!(fixture.usage().await, (50, 1));
}

#[tokio::test]
async fn oldest_unique_cid_is_evicted_and_usage_releases_after_not_found_only() {
    let fixture = quota_fixture(100, 2).await;
    fixture.seed_pinned("old", 40, time(1)).await;
    fixture.seed_pinned("new", 40, time(3)).await;
    fixture.reserve("incoming", 40, time(4)).await.unwrap_waiting();
    assert_eq!(fixture.pending_unpins().await, vec!["old"]);
    assert_eq!(fixture.usage().await, (80, 2));
    fixture.complete_unpin_not_found("old").await;
    assert_eq!(fixture.usage().await, (80, 2));
    assert_eq!(fixture.target_state("incoming").await, "waiting");
}

#[tokio::test]
async fn no_request_reconcile_retains_usage_while_submit_can_have_reached_provider() {
    let fixture = quota_fixture(100, 1).await;
    fixture.seed_remote_with_running_submit("bafy-race", 100, "calling").await;
    fixture.cancel_only_target().await;
    fixture.run_current_reconcile().await;
    assert_eq!(fixture.usage().await, (100, 1));
    assert_ne!(fixture.remote_state("bafy-race").await, "absent");
    assert!(fixture.current_reconcile_is_pending_after_lock_expiry().await);
}

#[tokio::test]
async fn failed_request_cleanup_retains_usage_across_delete_and_replacement_submit() {
    let fixture = quota_fixture(100, 1).await;
    fixture.seed_due_failed_all_remote("bafy-failed", 100, "failed-request", 3).await;
    fixture.complete_failed_request_delete_not_found().await;
    assert_eq!(fixture.usage().await, (100, 1));
    assert_eq!(fixture.remote_state("bafy-failed").await, "reserved");
    assert_eq!(fixture.remote_failure_attempts("bafy-failed").await, 3);
    assert_eq!(fixture.canonical_submit_count("bafy-failed").await, 1);
}
```

In the final assertion, usage remains `(80,2)` because old `(40,1)` is released and incoming `(40,1)` is atomically reserved/woken. Add tests for unpin transient keeping usage and waiter blocked, unpin success release, newest shared-reference touch ordering, active and limited-expired renewal refresh, all leases evictable, all-mode sibling survival, one-mode failover, repeated CID no double count, concurrent release/wake no overcommit, and provider quota response triggering bounded eviction. Add a blocked-DELETE quota test: capture epoch 12, expire then reactivate the same lease or add a shared target to epoch 13 while DELETE waits, return success and NotFound in separate cases, then assert usage is unchanged, no waiter wakes, request ID is cleared/status reserved, and a current canonical target Submit exists. Add tests that reusing an already pinned CID consumes no additional bytes/pins, immediately projects the new target pinned, refreshes oldest ordering, and creates no job; a failed/degraded remote and failed-request DELETE/replacement Submit continue consuming one reservation; and a woken waiter passes through `project_target_from_remote` before worker dispatch. Cover no-request states explicitly: pending `ready` and `recovery_backoff` Submit can be atomically cancelled then release; running Submit and pending/running `calling`/`recovering` return `Wait`; adopted request must be unpinned before release; a zero-result recovery with no desired target completes Submit, then current Reconcile releases exactly once.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib pinning::quota::tests -- --nocapture
cargo test --lib store::pinning::quota::tests::eviction_ -- --nocapture
```

Expected: tests fail because worker unpin completion is not yet connected to reservation wake-up and provider quota classification.

- [ ] **Step 3: Implement deterministic eviction selection and transactional marking**

Select `remote_pins` for one provider where state may consume capacity, ordered by `(last_touched_at ASC, cid ASC)`. For every selected `(provider,cid)`, set all matching active target states to `evicted`, preserve their parent lease unless all semantics require derived degradation, increment remote epoch once, and enqueue one `unpin_job`/`reconcile_job` carrying that epoch and no lease/target generation. Stop once projected released bytes/count would satisfy the waiting reservation. Never evict when the incoming CID alone exceeds `max_bytes`.

Compute `remote_pins.last_touched_at` as the maximum `created_at`/renew touch among all active target references. A new shared lease—including one immediately projected pinned—and a renewal both refresh it without incrementing usage. Failed/degraded remote state still consumes its unique reservation. Every lease source remains eligible for eviction; the timestamp changes order but does not grant immunity.

- [ ] **Step 4: Couple worker unpin outcomes to release/wake without unsafe early decrement**

Use Task 6's already-compiled `complete_remote_delete` and `complete_no_request_remote_absence` primitives; Task 14 adds only provider-limit-aware waiter wake/project behavior after their `Released` result.

```rust
pub async fn wake_provider_waiters<C: ConnectionTrait>(
    db: &C,
    provider: &str,
    limits: &ProviderLimits,
    now: DateTimeUtc,
) -> AppResult<Vec<String>>; // target IDs newly moved from quota_waiting
```

Before unpin, compare `pin_jobs.expected_remote_epoch` to `remote_pins.epoch` and reload active desired targets for `(provider,cid)`. A stale epoch or non-empty desired refs completes without provider call and retains/enqueues current Reconcile. Otherwise call unpin by persisted request ID. On success or NotFound, call `complete_remote_delete(expected_remote_epoch)` rather than an unconditional release. Only `Released` decrements once/marks old targets released/resets failure fields; after that commit, call `wake_provider_waiters` in a transaction that reserves waiting targets in FIFO `(created_at,id)` order without exceeding limits and calls `project_target_from_remote` for each newly reserved target so shared state/job ownership remains canonical. A crash between release and wake is recovered by the next periodic waiter scan, so capacity is never overcommitted. `Compensated` keeps usage/reservation intact, wakes nobody, clears the deleted request identity, marks submission-needed, and ensures/reactivates the canonical current target Submit even when its stable ID was already done. `ReconcileRequired` also keeps usage and wakes nobody when no target currently exists. On transient/auth/protocol failure, retain usage and retry/degrade according to Task 12.

For no desired refs and no request ID, do not infer absence. Call `complete_no_request_remote_absence` from the current Reconcile transaction; it invokes `resolve_no_request_submit_ambiguity`, atomically finishes only safe pending `ready`/`recovery_backoff` Submit rows, and returns `Wait` for every running/reclaimed/`calling`/`recovering` row that could have reached the provider. On `Wait`, keep usage and reschedule the same stable Reconcile after the ambiguity's lock/due time. On `Clear`, the function rechecks remote epoch, zero refs, null request, and no unresolved Submit in that same transaction, then marks absent, clears error/failure count/due time/last failed identity, and decrements usage exactly once before returning `Released`; the worker then wakes/projects waiters under the configured provider limits. A reclaimed find that adopts a request changes this path to ordinary Unpin; a zero-match recovery marks the Submit safe before Reconcile may release. Graceful shutdown never shortcuts this guard.

The failed-request retry DELETE is not a quota release operation because all-mode desired refs still exist. `prepare_failed_remote_resubmit` retains usage while clearing the failed request, moving to reserved, bumping epoch, and ensuring Submit. Only eventual lease end/eviction plus confirmed absence may decrement capacity.

Map `ProviderErrorClass::Quota` to the same bounded eviction selection once per retry window. For an all-mode evicted target, schedule reconciliation no earlier than one worker interval after confirmed release and reserve only when current headroom is sufficient without evicting the CID that just won capacity; this prevents an immediate eviction loop. Store optional observed usage/count/time independently and never overwrite local reserved usage with observations.

- [ ] **Step 5: Run GREEN including concurrency**

Run:

```powershell
cargo test --lib pinning::quota::tests -- --nocapture
cargo test --lib store::pinning::quota::tests -- --nocapture
cargo test --lib pinning::worker::tests::quota_ -- --nocapture
```

Expected: PASS; oversize is blocked, shared pinned reuse is immediate/job-free/single-counted, failed remotes and failed-request replacement retain reservation, oldest ordering is deterministic/protected by newest shared touch, release requires a matching remote epoch plus zero refs and no unresolved Submit, running/recovered Submit ambiguity cannot wake a waiter, expired-renew/new-reference during blocked DELETE compensates without quota release, and safe zero-match/never-started paths release once before waiters wake/project without overcommit.

- [ ] **Step 6: Record the review boundary**

Review quota/store/worker changes. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: enforce remote pin quotas`; otherwise record Task 14 complete without a Git write.

---

### Task 15: Application Startup, Shared Cancellation, and Bounded Shutdown

**Files:**
- Modify: `src/state.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/main.rs`

**Interfaces:**
- `AppState::new` completes config validation, token resolution, database connection/migrations, and coordinator construction before serving.
- HTTP and worker share a cancellation token; shutdown stops claims, drains bounded in-flight calls, and leaves recoverable locks.

- [ ] **Step 1: Write RED lifecycle tests for fail-fast startup and graceful worker shutdown**

```rust
#[tokio::test]
async fn invalid_provider_configuration_fails_before_listener_or_worker() {
    let mut config = Config::default_for_test();
    config.pinning = missing_token_config();
    let error = AppState::new_with_env(&config, |_| None).await.unwrap_err();
    assert!(error.to_string().contains("missing provider token environment variable"));
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_claiming_and_unfinished_lock_recovers_after_expiry() {
    let fixture = lifecycle_fixture_with_blocked_submit().await;
    let handle = fixture.start_worker();
    fixture.wait_until_submit_phase("calling").await;
    handle.shutdown(std::time::Duration::from_secs(5)).await;
    assert_eq!(fixture.provider_call_count(), 1);
    assert_eq!(fixture.submit_job().await.state, "running");
    assert_eq!(fixture.submit_job().await.submit_phase.as_deref(), Some("calling"));
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    let reclaimed = fixture.claim_expired_lock().await.pop().unwrap();
    assert!(reclaimed.reclaimed);
    assert_eq!(reclaimed.model.submit_phase.as_deref(), Some("recovering"));
}
```

Complete the lifecycle test by scripting find to return the request accepted before shutdown, then run the reclaimed claim and assert one total POST, one find, adopted request ID, and normal Poll/Unpin convergence according to whether the target remains desired. Add a cancellation variant where the target ends while Submit is blocked: current Reconcile retains usage until lock expiry/find adoption and confirmed Unpin; shutdown itself never clears lock, phase, remote row, or usage.

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --lib state::tests::pinning_ -- --nocapture
cargo test --lib pinning::worker::tests::shutdown_ -- --nocapture
```

Expected: tests fail because the injectable `new_with_env` unit seam and main/server lifecycle integration are absent; Task 8's production coordinator construction and Task 12's worker handle already compile.

- [ ] **Step 3: Construct coordinator-backed application state after validation/migrations**

Task 12 already enabled `tokio-util` `rt` and compiled `CancellationToken`; Task 15 does not change dependency features. Derive `Clone` for `Store` because `DatabaseConnection` is cloneable. Add an internal `AppState::new_with_env` seam used only by library unit tests; production `new` passes `std::env::var(name).ok()`. Validate and resolve tokens before returning state, call the existing `PinningCoordinator::build`, and never retain raw `ProviderConfig` tokens.

Add `#[cfg(test)] pub(crate) fn Config::default_for_test() -> Self` as a direct wrapper around the existing private default builder, and define:

```rust
pub(crate) async fn new_with_env<F>(cfg: &Config, get_env: F) -> anyhow::Result<Arc<Self>>
where
    F: Fn(&str) -> Option<String>,
```

Production `AppState::new` delegates to this function with `|name| std::env::var(name).ok()`.

- [ ] **Step 4: Wire one cancellation source through server and worker**

Use this production sequence:

```rust
let state = AppState::new(&cfg).await?;
let shutdown = tokio_util::sync::CancellationToken::new();
let worker = state.pinning.start(state.store.clone(), shutdown.child_token());
let signal_token = shutdown.clone();
let server = axum::serve(listener, app).with_graceful_shutdown(async move {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install shutdown signal");
    }
    signal_token.cancel();
});
let server_result = server.await;
shutdown.cancel();
worker.shutdown(std::time::Duration::from_secs(30)).await;
server_result?;
```

Migrations/config/provider construction complete before listener bind. Empty providers/policies start a harmless worker that claims no provider jobs. Direct AppState fixtures were already made compile-ready with `PinningCoordinator::disabled_for_test()` in Task 9; this task changes only lifecycle-specific fixture behavior. Bounded shutdown deliberately leaves a blocked Submit `running/calling`; the next expired-lock claim returns `reclaimed=true`, persists `recovering`, and performs find before target/generation exit or POST.

- [ ] **Step 5: Run GREEN and binary compilation**

Run:

```powershell
cargo test --lib state::tests::pinning_ -- --nocapture
cargo test --lib pinning::worker::tests::shutdown_ -- --nocapture
cargo check --bin ipfs-s3-gateway
```

Expected: PASS; invalid config fails before serve, worker and HTTP share cancellation, blocked Submit locks/phases survive shutdown, reclaimed work finds/adopts with no second POST and cannot release quota early, and the Rust 2024 binary compiles on MSRV 1.92.

- [ ] **Step 6: Record the review boundary**

Review startup/shutdown and fixture-only updates. If the user explicitly authorizes a commit, the orchestrator may commit them as `feat: manage pinning worker lifecycle`; otherwise record Task 15 complete without a Git write.

---

### Task 16: Real-TCP Pinning Harness and Signed S3 Tag Helpers

**Files:**
- Create: `tests/support/pinning.rs`
- Modify: `tests/support/mod.rs`
- Modify: `tests/support/decompress.rs`
- Modify: `tests/integration.rs` helper section

**Interfaces:**
- Extend the existing real axum+s3s+SQLite+mock-Kubo harness with two configurable PSA wiremock servers and an actual worker.
- Provide signed raw helpers for Get/Put/DeleteObjectTagging and `x-amz-tagging` on Put/Copy/CreateMultipartUpload.
- Provide deterministic barriers/restart hooks for blocked Submit/DELETE, expired-lock reclaim, paused expiry, and direct durable-state assertions without bypassing S3/provider TCP surfaces.

- [ ] **Step 1: Write a RED harness contract test**

```rust
#[tokio::test]
async fn pinning_harness_runs_signed_s3_and_async_psa_over_real_tcp() {
    let harness = start_pinning_harness(PinningHarnessConfig::request_one()).await;
    let put = signed_put_with_tagging(&harness, "key", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    assert!(harness.pinata_requests().await.is_empty(), "provider is not on the S3 response path");
    harness.run_worker_until_idle().await;
    assert_eq!(harness.pinata_requests().await.iter().map(|request| request.path.as_str()).collect::<Vec<_>>(), vec!["/psa/pins"]);
}
```

- [ ] **Step 2: Run RED**

Run:

```powershell
cargo test --test integration pinning_harness_runs_signed_s3_and_async_psa_over_real_tcp -- --nocapture
```

Expected: compile failure because the combined harness and signed tag helpers do not exist.

- [ ] **Step 3: Implement the harness with explicit clocks and cleanup ownership**

Define:

```rust
pub struct PinningHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: Arc<AppState>,
    pub kubo: wiremock::MockServer,
    pub pinata: wiremock::MockServer,
    pub filebase: wiremock::MockServer,
    pub worker: Option<PinningWorkerHandle>,
}

pub struct PinningHarnessConfig {
    pub providers: Vec<TestProviderConfig>,
    pub policies: Vec<PolicyConfig>,
    pub kubo_script: KuboScript,
    pub pinata_script: Vec<PsaReply>,
    pub filebase_script: Vec<PsaReply>,
}
```

The integration crate cannot call the `pub(crate)` library-unit seam `AppState::new_with_env`. Follow `tests/support/decompress.rs`'s public direct-state pattern exactly: start Kubo/provider wiremock, build raw pinning config with endpoint overrides/token env names, call public validation/coordinator APIs, connect/migrate SQLite, create the bucket, then construct public `AppState` including `pinning`:

```rust
let raw_pinning = PinningConfig {
    worker_interval: "5s".to_owned(),
    worker_concurrency: 4,
    providers: config.providers.into_iter().map(TestProviderConfig::into_provider_config).collect(),
    policies: config.policies,
};
let validated = ValidatedPinningConfig::from_raw(&raw_pinning, |name| match name {
    "TEST_PINATA_TOKEN" => Some("pinata-test-token".to_owned()),
    "TEST_FILEBASE_TOKEN" => Some("filebase-test-token".to_owned()),
    _ => None,
}).expect("validate test pinning config");
let pinning = PinningCoordinator::build(validated).expect("build test pinning coordinator");
let db = sea_orm::Database::connect("sqlite::memory:").await.expect("in-memory SQLite database");
ipfs_s3_gateway::store::run_migrations(&db).await.expect("run test migrations");
ipfs_s3_gateway::store::bucket::create(&db, &bucket, None).await.expect("create test bucket");
let state = Arc::new(ipfs_s3_gateway::state::AppState {
    kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
    store: ipfs_s3_gateway::store::Store::new(db),
    credentials: HashMap::from([("test".to_owned(), s3s::auth::SecretKey::from("test"))]),
    master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).expect("zero test master key"),
    pinning,
});
```

Start the production S3 service and actual worker from this state. Capture method/path/query/headers/body for PSA and assert authorization in a dedicated helper that never returns token text. Add `block_next_submit`, `block_next_delete`, `wait_for_provider_request`, `stop_worker_without_unlocking`, `advance_past_job_lock`, `restart_worker`, and `run_current_reconcile` test controls; they coordinate channels/clock and inspect the same SQLite store but never invoke provider/S3 operation functions directly. `Drop` cannot await, so every test calls `harness.shutdown().await`; this cancels worker/server tasks and asserts no unexpected scripted request remains.

Add signed helpers using existing `send_sigv4`: object tag operations use `?tagging`, PutObjectTagging serializes standard `<Tagging><TagSet><Tag><Key>…`, and DeleteObjectTagging sends DELETE with empty body. Keep all requests over the bound TCP endpoint.

- [ ] **Step 4: Run GREEN and existing harness regressions**

Run:

```powershell
cargo test --test integration pinning_harness_runs_signed_s3_and_async_psa_over_real_tcp -- --nocapture
cargo test --test integration test_harness_ -- --nocapture
```

Expected: PASS; signed S3 returns before provider traffic, the actual worker performs PSA over TCP, and existing Kubo/decompress harness behavior remains intact.

- [ ] **Step 5: Record the review boundary**

Review test support only. If the user explicitly authorizes a commit, the orchestrator may commit it as `test: add pinning service harness`; otherwise record Task 16 complete without a Git write.

---

### Task 17: Signed Real-Surface Acceptance Matrix

**Files:**
- Modify: `tests/integration.rs`
- Modify: `tests/support/pinning.rs`

**Interfaces:**
- Exercise the feature through signed S3 requests, real axum TCP, real reqwest provider clients, SQLite, mock Kubo, and wiremock PSA endpoints.
- No test requires live Filebase/Pinata credentials.

- [ ] **Step 1: Add publication and tag control-plane acceptance scenarios**

Add these named tests with exact end-state assertions:

```rust
#[tokio::test]
async fn test_pinning_automatic_put_is_async_and_eventually_pinned() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::automatic_all()).await;
    let response = signed_put_with_tagging(&harness, "happy.txt", b"happy".to_vec(), "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(harness.provider_requests().await.is_empty());
    assert_eq!(harness.active_lease_sources("happy.txt").await, vec!["automatic"]);
    harness.run_worker_until_idle().await;
    assert_eq!(harness.target_states("happy.txt").await, vec![
        ("filebase-primary".to_owned(), "pinned".to_owned()),
        ("pinata-primary".to_owned(), "pinned".to_owned()),
    ]);
    assert_eq!(signed_get(&harness, "happy.txt").await.bytes().await.unwrap().as_ref(), b"happy");
    harness.shutdown().await;
}
```

Add `test_pinning_manual_header_put_and_standard_tag_round_trip`, `test_pinning_copy_copies_or_replaces_tags_and_reuses_cid_usage`, `test_pinning_multipart_create_tags_apply_only_to_completed_root`, `test_pinning_put_tagging_renews_idempotently_and_delete_tagging_cancels_manual_only`, `test_pinning_put_tagging_rejects_expired_lease_after_confirmed_release`, and `test_pinning_overwrite_delete_and_delete_objects_end_only_owned_remote_leases`. For each, assert S3 status/XML, signed GET readability, exact sorted tags, owner object ID, lease source/state/generation, target provider/CID/state, pending job operation, exact PSA path/auth/request ID, unique usage values, and zero Kubo `/api/v0/pin/rm` requests caused by remote lifecycle. The confirmed-release case uses an archive-owned decompressed lease and asserts `InvalidArgument`, unchanged expired lease/original target IDs, and no reconstructed/extracted target.

- [ ] **Step 2: Add worker/coordination/quota/race acceptance scenarios**

| Test | Script and observable end state |
|---|---|
| `test_pinning_worker_recovers_expired_claim_and_adopts_ambiguous_submit` | Let real PSA accept one POST, stop worker before request-ID persistence, expire the `running/calling` lock, restart, and return one stable CID+metadata list match; assert reclaimed=true/`recovering`, find precedes target check, exactly one total POST, adopted request ID, and pinned target. |
| `test_pinning_reclaimed_submit_with_cancelled_target_adopts_then_unpins` | Accept POST then crash; cancel the only target before reclaim; assert find/adopt with zero second POST, usage retained, current Unpin issued, and release only after DELETE success/NotFound. |
| `test_pinning_recovery_find_none_with_no_desired_target_never_posts` | Signed Put creates Submit; let its first POST become ambiguous, stop before DB persistence, cancel the target, then return zero from stable list lookup on reclaim; assert find occurs before stale-generation exit, no second POST, Submit becomes safe/done, and Reconcile marks absent/releases exactly once. |
| `test_pinning_running_submit_blocks_no_request_quota_release` | Signed Put starts a real POST blocked after `calling`; cancel target, run Reconcile and shutdown; assert usage/remote row retained and Reconcile rescheduled while lock is live, restart reclaim finds/adopts, then Unpin releases. |
| `test_pinning_poll_reschedules_one_stable_job_until_pinned` | Script POST→queued, GET→pinning, GET→pinned; advance two poll intervals and assert exactly one POST, two GETs, one unchanged Poll ID, attempts zero, pending after first GET, done after second. |
| `test_pinning_shared_remote_projects_automatic_manual_and_copy_targets` | Publish automatic+manual targets then Copy/shared target for one CID; assert one POST, all submitted then pinned, all leases available, unique usage counted once, and a later target immediately pinned with no job. |
| `test_pinning_shared_terminal_failure_coordinates_each_lease` | Share one remote across two one-mode and one all-mode lease; return failed/terminal; assert all targets degraded, each one lease independently fails over once, and all lease keeps one bounded remote retry. |
| `test_pinning_failed_all_remote_forgets_and_resubmits_once` | Return PSA `failed` for one provider/CID shared by one+two all leases; assert failure count increments once, one leases fail over independently, exactly one all-mode Reconcile at durable backoff DELETEs/forgets old request, epoch advances once, usage stays reserved, one canonical Submit pins both all leases. |
| `test_pinning_failed_remote_eight_cycles_stop_without_spin` | Drive eight distinct failed request IDs, re-observing each twice; assert counts 1…8 once each, cycles 1–7 each have one DELETE/one replacement Submit, cycle 8 leaves degraded targets, `next_retry_at=NULL`, and no due job until a new target or generation-advancing manual extension/reactivation resets it; equal renewal does not. |
| `test_pinning_stale_poll_owner_hands_off_without_duplicate_post` | Cancel the target named by a queued remote's Poll; run current-epoch Reconcile; assert next `(created_at,target_id)` target owns a current-generation Poll and POST count remains one. |
| `test_pinning_stale_remote_epoch_skips_delete` | Seed Unpin at epoch 4, add a shared desired target to epoch 5 before claim; assert zero provider DELETE, unchanged usage, and current-epoch Reconcile. |
| `test_pinning_one_mode_sticky_priority_failover_converges_duplicate` | Script transient then terminal primary, pinned secondary, and successful old-primary delete; assert primary remains selected through transient, secondary wins after terminal, and one required provider remains. |
| `test_pinning_all_mode_partial_success_keeps_retrying_degraded_provider` | Script Pinata pinned and Filebase 5xx; assert availability true, Pinata pinned, Filebase degraded, and a future retry remains. |
| `test_pinning_quota_evicts_oldest_unique_cid_after_confirmed_unpin` | Fill both limits, renew newer CID, publish incoming CID; assert oldest DELETE occurs, usage is unchanged before DELETE response, then incoming reservation wakes. |
| `test_pinning_shared_cid_counts_once_and_blocks_unsafe_unpin` | Publish two keys with one CID, cancel one; assert `(bytes,pins)` counted once and no DELETE until the second target ends. |
| `test_pinning_renewal_generation_wins_against_inflight_expiry_unpin` | Let expiry scan mark the manual lease expired, pause provider DELETE after claim, send signed PutObjectTagging retain-until for the same latest owner, assert same lease/original target IDs plus generation/epoch advance and retained usage, release DELETE, then assert compensation Submit/Poll restores pinned and S3 GET succeeds. |
| `test_pinning_new_shared_target_during_delete_compensates_without_release` | Pause DELETE, publish another key sharing the CID, return NotFound; assert epoch advances, unique usage remains reserved once, no quota waiter wakes, and current target Submit is pending. |
| `test_pinning_expiry_removes_remote_pin_but_preserves_s3_and_kubo_pin` | Advance beyond expiry, run worker; assert remote DELETE/lease expired, S3 GET success, and no Kubo pin removal. |

- [ ] **Step 3: Add ZIP acceptance scenarios**

Add integration tests `test_pinning_zip_decompressed_pins_entries_not_archive`, `test_pinning_zip_partial_success_targets_only_published_entries`, `test_pinning_zip_archive_renewal_updates_all_entry_targets`, `test_pinning_zip_global_reject_creates_no_manual_lease`, `test_pinning_multipart_zip_complete_commits_entries_and_upload_delete_atomically`, and `test_pinning_multipart_zip_outbox_failure_preserves_upload_and_parts`. Keep the Task 11 library fake-store test named `test_pinning_multipart_zip_outcome_unknown_reconciles_exact_attempt`; it is executed by the final `cargo test --lib` gate rather than pretending to be a real-TCP case. Assert exact CID sets from PSA bodies, archive owner ID on the lease, no archive CID in the manual target set, no generated-entry recursive lease, failed-entry omission, renewal generation/expiry equality across all entry targets, global-reject zero pinning rows, ordinary archive tags round-trip, success removal of upload/parts, rollback preservation with zero publication rows, and Committed/NotCommitted/Unknown decisions keyed by `completion_attempt_id`.

- [ ] **Step 4: Run the focused real-surface matrix**

Run:

```powershell
cargo test --test integration test_pinning_ -- --nocapture
```

Expected: PASS if Tasks 1–16 are complete; every scenario traverses signed S3 and real TCP rather than calling an op/store function directly, including expiry→tag renewal→blocked DELETE compensation, failed-request replacement/stop, and Submit crash/reclaim/absence guards.

- [ ] **Step 5: Run the complete standard integration regression**

Run:

```powershell
cargo test --test integration
```

Expected: PASS; happy path, recurring Poll, shared-state projection/owner handoff, tag control, Copy/Multipart/ZIP atomic completion, limited expired renewal, failed-request bounded retry, Submit recovery/no-early-release races, one/all, quota/shared CID, async failure isolation, and all prior signed integration tests are green.

- [ ] **Step 6: Record the review boundary**

Review the real-surface matrix and any scenario-driven focused fixes. If the user explicitly authorizes a commit, the orchestrator may commit them as `test: cover multi-provider pinning`; otherwise record Task 17 complete without a Git write.

---

### Task 18: Documentation, Configuration Examples, Release State, and Full Gates

**Files:**
- Modify: `README.md`
- Modify: `config.example.toml`
- Modify: `config.docker.toml`
- Modify: `.env.example`
- Modify: `docker-compose.yml`
- Modify: `ROADMAP.md`

**Interfaces:**
- Document only behavior proven by Tasks 1–17 and expose tokens through environment-variable references.
- Mark v0.4 complete only after every final gate passes.

- [ ] **Step 1: Add documentation/config assertions before editing docs**

Run these searches and record their current absence/stale legacy shape:

```powershell
rg -n "worker_interval|token_env|allow_decompressed|max_bytes|max_pins" README.md config.example.toml config.docker.toml .env.example docker-compose.yml
rg -n 'provider = "noop"|v0.4 — Pinning Service' config.example.toml config.docker.toml ROADMAP.md
```

Expected: the first search lacks the complete configuration contract and the second finds the old single-provider examples/uncompleted v0.4 list.

- [ ] **Step 2: Replace examples with the validated schema and env references**

Use this exact shape in `config.example.toml` with explanatory comments and non-secret env names:

```toml
[pinning]
worker_interval = "5s"
worker_concurrency = 4

[[pinning.providers]]
name = "pinata-primary"
kind = "pinata"
token_env = "PINATA_JWT"
priority = 10
max_bytes = 107374182400
max_pins = 10000

[[pinning.providers]]
name = "filebase-primary"
kind = "filebase"
token_env = "FILEBASE_PINNING_TOKEN"
priority = 20
max_bytes = 107374182400
max_pins = 10000

[[pinning.policies]]
bucket = "*"
prefix = ""
trigger = "always"
provider_mode = "all"
providers = ["pinata-primary", "filebase-primary"]
default_duration = "30d"
max_duration = "365d"
allow_decompressed = true
```

Keep `config.docker.toml` safe by default with empty `providers`/`policies` or a commented provider example. Add blank `PINATA_JWT=` and `FILEBASE_PINNING_TOKEN=` entries to `.env.example`; pass them through `docker-compose.yml` without literal values. State that endpoint override is intended for tests/private PSA-compatible services.

- [ ] **Step 3: Document semantics and control tags without credential exposure**

README must state: remote pinning is asynchronous; at least one pinned provider is the availability success line; all leases are evictable; local quotas use unique CID logical S3 bytes/count and are soft controls, not billing truth; expiry/eviction/unpin never removes S3 metadata/content or local Kubo pins; UploadPart is never remotely pinned; provider tokens come from env references; rules are ordered first-match with exact bucket/`*` and literal prefix; one is sticky priority/failover and all fans out. Document that retain-until may reactivate the same expired manual lease only while an original remote reservation/unpin race is unresolved, never after confirmed release and never by reconstructing decompressed targets. State that provider `failed` requests retry per unique provider/CID with bounded backoff/eight-attempt stop, while crash-recovered Submit queries before another POST and holds quota until ambiguity is resolved.

Document standard examples for `x-amz-tagging`, `put-object-tagging`, `get-object-tagging`, and `delete-object-tagging`, including `ipfs-s3:pin`, `duration`, `retain-until`, and `content=decompressed`. Use PowerShell environment syntax in Windows examples.

- [ ] **Step 4: Run full automated gates before changing ROADMAP status**

Run exactly:

```powershell
cargo test --lib
cargo test --test integration
cargo test --test e2e --no-run
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Expected: every command exits 0; no live provider credentials are required or printed.

- [ ] **Step 5: Mark v0.4 complete only after the gates are green**

Update `ROADMAP.md` Current from v0.3 to v0.4 and check the delivered Filebase/Pinata PSA clients, ordered automatic/manual policies, standard tag control, durable worker, one/all coordination, duration/renewal/expiry, decompressed targets, and local quota eviction. Preserve all v0.5+ items unchecked.

- [ ] **Step 6: Re-run documentation and diff checks**

Run:

```powershell
rg -n "asynchronous|soft|evict|PINATA_JWT|FILEBASE_PINNING_TOKEN|retain-until|decompressed|UploadPart" README.md config.example.toml .env.example ROADMAP.md
git diff --check
```

Expected: required statements/env references are present and diff whitespace is clean.

- [ ] **Step 7: Record the final review/commit boundary**

Review docs/config/release state and the complete implementation diff. If the user explicitly authorizes a commit, the orchestrator may create the final semantic commit(s) from the reviewed task boundaries; otherwise leave all changes unstaged and report the implementation ready for user-controlled versioning.

---

## Final Acceptance Scenarios

Run the focused real-surface suite first:

```powershell
cargo test --test integration test_pinning_ -- --nocapture
```

The named scenarios must prove:

1. **Happy path and recurring Poll:** signed Put commits object/tags/lease/outbox, responds before remote HTTP, worker performs one Submit and repeatedly reschedules one stable Poll row at bounded intervals through queued→pinning→pinned, and S3 GET remains readable.
2. **Quota eviction and ambiguity guard:** a new unique CID selects the oldest `last_touched_at`, waits through remote unpin, and releases capacity only on success/NotFound or a current-epoch no-request Reconcile that proves no unresolved Submit. Running/recovering Submit and failed-request replacement retain capacity; safe release then reserves/wakes without exceeding bytes/count.
3. **Renew/expiry/shared-CID race:** expiry preserves original target identity; signed retain-until may reactivate the same expired manual lease only for the same latest owner while at least one original remote reservation/request/recoverable Unpin remains. Generation/epoch advance once, quota remains, blocked DELETE compensation restores pinned, and confirmed release rejects without recreating decompressed targets. Cancelled/evicted leases remain terminal.
4. **One failover:** priority assignment stays sticky across transient failures, terminal/exhausted failure selects the next provider, and temporary duplicate remote pins converge to one required provider.
5. **All partial success and failed-request retry:** every provider is dispatched; one pinned target satisfies availability. A failed provider/CID shared by many targets increments one durable failure count per distinct request, schedules one remote Reconcile, forgets the failed request before one canonical Submit, resets on pinned, and after eight cycles remains degraded with no due spin; shared one-mode leases fail over independently.
6. **ZIP decompressed and multipart finalization:** archive-owned manual lease targets only committed successful entry CIDs, not archive/failed entries; archive renewal/end updates all targets together; multipart ZIP commits archive/entries/pinning rows and upload/part deletion in one transaction, with exact completion-attempt reconciliation for unknown outcomes.
7. **Tag control plane:** standard Put/Get/DeleteObjectTagging and publication `x-amz-tagging` preserve ordinary tags, renew monotonically/idempotently, and cancel only manual leases.
8. **Kubo pin regression:** expiry, cancel, overwrite, delete, provider failure, quota eviction, multipart ZIP rollback, and remote unpin produce no local Kubo `pin/rm`; remote failure never makes committed S3 data unreadable.
9. **Shared remote projection:** automatic/manual/Copy targets for one provider/CID cause one canonical POST/Poll stream, all current targets receive each remote status atomically, later targets inherit pinned immediately, terminal failure coordinates every affected lease, and stale ownership hands off without duplicate POST.
10. **Submit crash recovery:** an accepted POST followed by process death leaves `running/calling`; reclaim persists `recovering`, performs stable CID+metadata find before target-generation exit, adopts with zero second POST, and if no desired refs unpins before releasing. Zero find plus no desired refs never POSTs and releases only after Reconcile; graceful cancellation preserves the recovery lock/phase.

Then run the complete acceptance gates:

```powershell
cargo test --lib
cargo test --test integration
cargo test --test e2e --no-run
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Expected: all commands exit 0 on Rust 2024/MSRV 1.92; automated acceptance uses local wiremock endpoints only, does not install software or pull images, and does not require/log real Filebase or Pinata credentials.

## Specification Traceability

- **Provider/config contract:** Tasks 1–2 define validated Filebase/Pinata/Noop construction, endpoint overrides, env-only redacted tokens, PSA request IDs/status/find/unpin, and classified idempotent recovery; Tasks 12 and 15 connect them to runtime lifecycle.
- **Persistence contract:** Tasks 3–7 add and exercise all six required tables plus multipart tag persistence, reversible SQLite migration, PostgreSQL builders/CHECK constraints, target lease generations, remote epochs, durable failed-request count/due time, scope-safe jobs, persisted Submit recovery phase, fresh/reclaimed claim identity, in-place recurring Poll/Reconcile state, shared remote projection, concurrency-safe claims/reservations, atomic ordinary/multipart/ZIP publication, overwrite, and delete.
- **S3 tag control plane:** Tasks 4–5 and 8–11 cover ordinary tag fidelity, all four reserved tags, Put/Copy/Create `x-amz-tagging`, Get/Put/DeleteObjectTagging, default/max duration, active monotonic renewal, same-owner/original-target limited expired reactivation, confirmed-release rejection without decompressed reconstruction, cancellation, and archive-owned decompressed renewal.
- **Publication scope/local safety:** Tasks 7 and 9–11 cover Put, Copy, non-ZIP Complete, ordinary ZIP, typed multipart ZIP Complete, atomic upload/part deletion, completion-attempt outcome reconciliation, successful ZIP entries, overwrite, DeleteObject/DeleteObjects, UploadPart exclusion, no recursive generated-entry policy, and no remote-lifecycle Kubo unpin.
- **Coordination/worker:** Task 6 creates the compile-ready coordinator foundation and exact renewal/failure/recovery store APIs before Task 9; Tasks 12–15 add `tokio-util` runtime support, target-vs-remote dispatch, durable fresh/reclaimed claims, `ready/calling/recovering/recovery_backoff` Submit transitions, stable find-before-retry adoption, recurring in-place Poll/Reconcile, canonical shared owner handoff, atomic all-target projection, request ID adoption, epoch/generation races, expiry-renewal compensation, one shared failed-request retry owner with eight-cycle stop, per-lease one failover/duplicate convergence, all fan-out/partial success, startup, and bounded shutdown that preserves locks.
- **Quota contract:** Tasks 6, 12, and 14 cover unique provider/CID bytes/count, pinned shared reuse without extra reservation/job, failed-remote/replacement-Submit retained capacity, oversize blocking, derived newest touch, oldest eviction, all leases evictable, current-epoch/no-ref/no-unresolved-Submit confirmed release, blocked-DELETE compensation without release, safe zero-find release, projected waiter wake-up, shared references, one failover, all survival, and advisory observations.
- **Acceptance/release:** Task 16 builds external integration state only through public validation/coordinator APIs and direct AppState construction; Tasks 16–18 provide real-TCP signed S3/provider tests for recurring Poll, shared projection/handoff, multipart ZIP atomicity/reconciliation, limited expired renewal, failed retry/stop, accepted-POST crash recovery, no-early-quota-release guards, and all required happy/race/failover/quota/ZIP/tag/Kubo scenarios, plus PowerShell-safe commands, documentation/config/Docker guidance, ROADMAP update, and all final gates.

## Optional Live Smoke Boundary

Operator-supplied credentials may be used only in a separately authorized manual smoke run after automated acceptance. The command must reference `PINATA_JWT`/`FILEBASE_PINNING_TOKEN` through environment variables, must not echo them, and is not an automated release requirement.
