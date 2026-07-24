# Multi-provider Pinning Service Design

**Status:** Approved for implementation planning
**Date:** 2026-07-21
**Scope:** Filebase and Pinata providers, composable automatic/manual pin policies, S3-compatible renewal, multi-provider coordination, local quota management, and duration-based lifecycle management

## Goal

Add durable remote IPFS pinning without changing the existing local Kubo pin safety model or making S3 writes depend on remote provider availability.

The gateway must support:

- Filebase and Pinata through their IPFS Pinning Service API endpoints.
- Ordered configuration rules that select eligible object publications.
- Automatic pinning or explicit per-request opt-in.
- One-provider or all-provider coordination.
- Duration-based leases and S3-compatible renewal.
- A decompression mode that remotely pins only successfully published extracted entries.
- Per-provider local soft limits for unique CID bytes and pin count.
- Automatic eviction of the oldest remote pins when capacity is needed.
- Durable asynchronous submission, polling, retry, unpin, and crash recovery.

## Non-goals

- No deletion or unpinning of local Kubo content as part of remote lifecycle management.
- No claim that local quota state exactly matches a provider's billing system in real time.
- No dependency on Redis, Kafka, or a separate worker service.
- No generic policy DSL.
- No provider-specific upload API; both initial providers pin existing CIDs through PSA.
- No guarantee that a lease survives quota eviction. A configured duration is the maximum retention period, not an absolute retention promise.
- No live Filebase or Pinata request in automated tests without credentials supplied by the operator.

## Confirmed decisions

1. Remote operations are asynchronous. An S3 publication succeeds after its database transaction, without waiting for a provider.
2. At least one provider reaching `pinned` is the availability success line. In `all` mode, remaining providers continue until they converge or remain degraded.
3. Quotas are local soft limits measured per provider by unique CID bytes and unique remote pin count.
4. Lease time starts when the S3 object publication commits.
5. Expiry, cancellation, or eviction affects only remote provider pins.
6. All leases are evictable when quota pressure requires capacity.
7. `one` mode uses provider priority, sticky assignment, and failover. A transition may briefly leave the CID pinned on two providers.
8. Manual policy and renewal use standard S3 object tags.
9. Configuration and manual requests create independent logical leases that can coexist for one object.
10. Durable database outbox jobs and an in-process worker own all remote HTTP calls.

## Architecture

```text
PutObject / CopyObject / CompleteMultipartUpload / PutObjectTagging
                           |
                           v
                 PinPolicyEvaluator
                           |
                 EffectivePinPolicy
                           |
             object publication transaction
          object + tags + leases + targets + jobs
                           |
                           v
                  PinningCoordinator
                  /               \
          PinataProvider      FilebaseProvider
             PSA /psa       PSA /v1/ipfs
```

`AppState` owns a `PinningCoordinator`. The coordinator owns the configured providers and the persistent worker. The existing Kubo `pin_add` and conservative local unpin behavior remain separate and unchanged.

The policy evaluator is pure: it receives the object operation, bucket, key, size, matching configuration rule, request tags, and decompression result, and returns normalized leases and targets. It performs no database or network I/O.

## Provider abstraction

Replace the current two-method abstraction with an asynchronous remote-pin contract that preserves PSA request identity:

```rust
trait PinningProvider: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn submit(&self, request: SubmitPin) -> Result<RemotePin, ProviderError>;
    async fn get(&self, request_id: &str) -> Result<RemotePin, ProviderError>;
    async fn find(&self, query: FindPin) -> Result<Vec<RemotePin>, ProviderError>;
    async fn unpin(&self, request_id: &str) -> Result<(), ProviderError>;
}
```

`RemotePin` normalizes PSA states to `Queued`, `Pinning`, `Pinned`, and `Failed`. Raw provider response values may be retained for diagnostics but do not leak into policy logic.

Both providers share a standards-oriented PSA HTTP client and have provider-specific base URLs, rate limits, authentication validation, and error mapping:

- Pinata: `https://api.pinata.cloud/psa`
- Filebase: `https://api.filebase.io/v1/ipfs`

Both use `Authorization: Bearer <token>`. Tokens are read from named environment variables at startup and are never serialized into debug output or logs.

Each submit request includes stable gateway metadata containing the outbox operation ID. If a POST times out or returns an ambiguous conflict, the worker searches by CID and stable metadata before retrying and adopts the existing `requestid` when found.

## Configuration model

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

Provider names must be unique. A provider endpoint can be overridden for tests and compatible private deployments. Built-in endpoints remain the default.

Policy rules are ordered. The first rule whose bucket and key prefix match is authoritative:

- `bucket` is either `"*"` or one exact bucket name.
- `prefix` is a literal key prefix.
- No match means no remote pin policy.

`trigger` is `always` or `request`. `provider_mode` is `one` or `all`. A policy must reference at least one enabled provider. Durations use a positive integer followed by `s`, `m`, `h`, or `d`.

Startup fails before serving requests when configuration contains a duplicate name, unknown provider type, missing token environment variable, zero/invalid quota, invalid duration, empty provider set, unknown provider reference, or `default_duration > max_duration`.

## S3 object-tag control plane

The gateway implements normal persistence and retrieval for S3 object tags rather than recognizing only private headers. The implementation includes:

- `x-amz-tagging` on PutObject, CopyObject, and CreateMultipartUpload.
- PutObjectTagging.
- GetObjectTagging.
- DeleteObjectTagging.

Reserved control tags use the `ipfs-s3:` prefix:

| Tag | Meaning |
|---|---|
| `ipfs-s3:pin` | `true` requests a manual lease; `false` cancels it. |
| `ipfs-s3:duration` | Initial relative duration such as `30d`. |
| `ipfs-s3:retain-until` | Absolute RFC3339 UTC timestamp used for idempotent renewal. |
| `ipfs-s3:content` | `object` or `decompressed`. |

Initial requests use `duration`; omitting it uses the matching rule's `default_duration`, while supplying `retain-until` on an initial publication is invalid. Renewal uses `retain-until`. A renewal with the same timestamp is idempotent. A timestamp earlier than the current manual lease expiry is rejected as `InvalidArgument`; cancellation uses `ipfs-s3:pin=false`, omission of the reserved pin tag in a replacing PutObjectTagging request, or DeleteObjectTagging.

Manual duration must not exceed the matching rule's `max_duration`. `decompressed` is valid only when the publication uses the existing `?decompress-zip` flow and the matching policy sets `allow_decompressed = true`.

PutObjectTagging renews or cancels an existing manual lease. It does not create a decompressed lease for an old object that lacks the original archive-to-entry target set. A normal object may receive its initial manual lease only during publication; this keeps initial policy validation and object publication atomic.

Ordinary user tags are stored and returned unchanged, subject to S3 tag validation and limits. Reserved tags participate in the same tag replacement semantics but additionally drive the manual lease.

## Policy composition

Automatic and manual requests are separate lease sources:

- A matching `always` rule creates an automatic lease with `default_duration`.
- A valid `ipfs-s3:pin=true` request creates a manual lease with its requested duration.
- A `request` rule creates no automatic lease and requires the manual tags.
- Removing or cancelling manual tags ends only the manual lease.
- An automatic lease cannot be shortened or disabled through object tags.
- The shared remote pin remains while any active target still references it.

The matching configuration rule controls the provider set and `one`/`all` mode. Request tags do not name credentials or arbitrary providers.

## Publication scope

Policy evaluation applies when a new latest object is published by:

- PutObject.
- CopyObject.
- CompleteMultipartUpload.
- The existing ZIP decompression publication path.

UploadPart does not create a remote pin lease. Multipart request tags are persisted at CreateMultipartUpload and evaluated only after Complete publishes the root object.

Overwriting a key ends leases owned by the prior latest object and creates leases for the new object in the same database transaction. A repeated CID shares the existing provider pin and does not consume quota twice.

DeleteObject and each successful DeleteObjects item end leases owned by the removed latest object in the same database transaction that clears `is_latest`. DeleteBucket can proceed only for an empty bucket, so it has no live object leases to end. These operations affect remote leases only; they retain the existing rule that no local Kubo pin is removed.

## Decompressed content mode

For a publication carrying `ipfs-s3:content=decompressed`:

- The archive CID is not remotely pinned by that manual lease.
- Every successfully published extracted entry CID becomes a target of the archive-owned lease.
- Failed entries do not create targets.
- A global decompression rejection creates no manual lease.
- Renewing the archive object's manual lease renews all its entry targets together.
- Deleting an extracted S3 entry does not by itself end the archive-owned lease; deleting or replacing the archive, cancelling its tags, expiry, or quota eviction does.

No recursive policy evaluation is performed on generated entries for the same upload operation. A separate later S3 operation on an entry can create its own independent lease.

## Persistence model

Add the following durable entities through a reversible migration:

### `object_tags`

- Immutable object ID foreign key.
- Tag key and value.
- Unique `(object_id, key)`.

### `pin_leases`

- Lease ID.
- Owner object ID.
- Source: `automatic` or `manual`.
- Policy identity captured at creation.
- Provider mode and content mode.
- `created_at`, `last_touched_at`, and `expires_at`.
- Monotonic generation used to reject stale jobs.
- State: `active`, `cancelled`, `expired`, or `evicted`.

### `pin_lease_targets`

- Lease ID.
- CID and S3 logical object size used for local quota accounting. This deliberately excludes UnixFS and provider-side overhead, so it remains a soft control rather than a billing measurement.
- Provider assignment.
- State: `waiting`, `submitted`, `pinned`, `degraded`, `quota_waiting`, `evicted`, or `released`.
- Unique lease/CID/provider identity.

### `remote_pins`

- Unique `(provider, cid)`.
- Provider `requestid` when known.
- CID size.
- Normalized remote status.
- `last_touched_at`, derived as the newest create/renew time of active targets.
- Last error class and redacted diagnostic text.

### `pin_jobs`

- Stable operation ID.
- Operation: `submit`, `poll`, `unpin`, or `reconcile`.
- Provider and CID.
- Expected lease generation when applicable.
- State, attempts, `next_attempt_at`, `locked_until`, and redacted last error.

### `pin_provider_usage`

- Provider name.
- Reserved unique CID bytes.
- Reserved unique CID count.
- Last optional provider-observed usage snapshot and observation time.

The first target for `(provider, cid)` atomically creates `remote_pins` and reserves capacity. Additional leases reuse it without increasing usage. Capacity is released only after unpin succeeds or the provider confirms NotFound.

All publication-related object, tag, lease, target, usage reservation, and outbox changes occur in the same database transaction. No provider HTTP request occurs inside a database transaction.

## Provider coordination

### `one` mode

1. Consider providers in ascending configured priority.
2. Skip providers that are disabled, terminally misconfigured, or cannot reserve capacity.
3. Persist the selected provider as a sticky assignment.
4. Retry transient failures on that provider.
5. On terminal pin failure or exhausted retry policy, create a target on the next provider.
6. A replacement may be submitted before an ambiguous prior request is fully reconciled.
7. After the replacement reaches `pinned`, reconcile and unpin an unnecessary older provider request.

### `all` mode

Create a target for every referenced provider. One successful target satisfies the availability success line, while incomplete targets remain degraded and continue retrying. Quota eviction can remove one provider target without cancelling the lease's remaining targets.

## Local quota and oldest-pin eviction

Each provider has a configured `max_bytes` and `max_pins`. Accounting uses unique `(provider, cid)` records in states that may consume remote capacity.

When a new unique CID cannot reserve capacity:

1. If the CID alone exceeds `max_bytes`, mark its target `quota_blocked`; do not evict unrelated pins.
2. Otherwise select remote pins on that provider by ascending `last_touched_at`.
3. The timestamp is the maximum create/renew time of all active targets, so a recently created shared reference protects the CID from appearing old.
4. Mark all selected provider/CID targets evicted and enqueue unpin until enough capacity can be released.
5. Wait for unpin success or NotFound before decrementing reserved usage.
6. Wake capacity-waiting targets after release.

All leases are eligible for eviction. Renewal refreshes `last_touched_at` and therefore reduces, but does not eliminate, eviction risk. In `one` mode an evicted target may fail over to another provider with capacity. In `all` mode the lease remains active on surviving providers and the evicted provider target remains absent until a later reconciliation can reserve capacity without immediately causing another eviction loop.

An explicit provider quota response triggers the same bounded eviction process. Provider usage endpoints, when available, are advisory observations only and never replace local atomic reservations.

## Worker lifecycle and concurrency

The gateway starts an in-process persistent worker after configuration and migrations succeed. The HTTP server and worker share a cancellation token and use graceful shutdown.

Workers atomically claim due jobs with `locked_until`. Expired locks are recoverable after a crash. Each provider has bounded concurrency and a provider-appropriate request rate. Jobs use exponential backoff with jitter and honor `Retry-After` when present.

Before acting, a job reloads the current lease and target state:

- A stale generation cannot unpin a renewed lease.
- Unpin is skipped when another active target still needs the provider/CID pin.
- If unpin was already in flight when a renewal committed, successful removal is followed by a new submit job.
- Duplicate poll and unpin execution is idempotent.

The worker stops claiming jobs during shutdown, allows in-flight calls a bounded grace period, then leaves locks to expire for recovery by the next process.

## Error semantics

### S3 request errors

Invalid control tags, malformed duration/timestamp, duration beyond the policy maximum, unsupported decompressed mode, an unknown policy state, or renewal that shortens a lease returns `InvalidArgument` before Kubo or database mutation.

Database failure while publishing the object and its policy state fails the S3 operation. The object publication and pin outbox cannot commit independently.

Remote provider failures never roll back an already committed S3 object. They update durable target state and structured logs.

### Provider errors

- 401/403: terminal authentication/configuration failure; stop rapid retries and mark provider degraded.
- 404 during unpin: idempotent success.
- 409 or ambiguous submit timeout: query by stable metadata/CID before retrying.
- 429: rate limited; honor `Retry-After` when supplied.
- 5xx and transport failures: transient retry with bounded exponential backoff and jitter.
- PSA terminal `failed`: record the provider reason and follow one/all failover semantics.
- Malformed provider response: protocol error with bounded retry, never fabricate a `requestid`.

Logs include provider name, CID, object/lease/job IDs, state transition, and remote request ID when available. They exclude bearer tokens and request bodies containing credentials.

## Testing strategy

### Policy unit tests

Table-driven tests cover:

- Ordered first-match rules.
- `always` and `request` triggers.
- Automatic/manual lease composition.
- `one` and `all` provider targets.
- Duration boundaries and RFC3339 renewal idempotency.
- Cancellation without weakening automatic leases.
- Object and decompressed target selection.

### Provider contract tests

Wiremock tests separately verify Pinata and Filebase:

- Exact base paths and bearer authentication.
- PSA submit/get/list/delete serialization.
- `requestid` persistence.
- Status normalization.
- 401/403, 404, 409, 429, 5xx, malformed body, and timeout handling.
- Ambiguous submit reconciliation without duplicate pin creation.
- Token redaction.

### Store and worker tests

- Reversible SQLite migration retaining existing object/multipart data.
- PostgreSQL query-builder coverage for the migration.
- Atomic object/tag/lease/outbox publication.
- Concurrent shared-CID reservation counting once.
- Restart recovery from expired job locks.
- Generation protection for renew-versus-unpin races.
- Capacity release only after confirmed unpin.
- Oldest-pin ordering with shared references and renewal.
- `one` failover and `all` degraded convergence.

### Real-TCP integration tests

Signed S3 requests exercise:

- Automatic PutObject pin policy.
- Manual `x-amz-tagging` opt-in.
- Create/Complete multipart tag propagation.
- GetObjectTagging, PutObjectTagging renewal, and DeleteObjectTagging cancellation.
- CopyObject publication and tag behavior.
- ZIP `decompressed` mode pinning entries but not archive.
- Partial ZIP success targeting only published entries.
- TTL expiry.
- Quota pressure evicting the oldest unique CID.
- Shared CIDs preventing double accounting and unsafe unpin.
- Provider priority, sticky assignment, failover, and all-provider fan-out.
- Remote provider failure leaving S3 data readable and locally pinned.

### Final gates

```powershell
cargo test --lib
cargo test --test integration
cargo test --test e2e --no-run
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Optional live smoke commands may be documented for operator-supplied Filebase and Pinata credentials. Automated acceptance must not require or log real credentials.

## Documentation and release state

Update `README.md`, `config.example.toml`, `.env.example`, Docker configuration guidance, and `ROADMAP.md` only after implementation and acceptance gates pass. Documentation must state:

- Remote pinning is asynchronous.
- At least one provider is the availability success line.
- All leases may be evicted under quota pressure.
- Local quota is a soft control, not provider billing truth.
- Remote expiry never deletes S3 metadata or unpins local Kubo content.
- Provider tokens must be supplied through environment variables.

## Invariants

1. An object publication and its initial pin policy state commit atomically.
2. Provider HTTP calls never occur inside a database transaction.
3. A repeated CID consumes provider quota once.
4. A CID is not remotely unpinned while another active target on that provider requires it.
5. Remote lifecycle actions never remove a local Kubo pin.
6. A stale worker job cannot undo a newer renewal.
7. Provider tokens are never persisted or logged.
8. Decompressed mode pins only successfully published extracted entry CIDs for that lease.
9. `one` assignments are sticky until failure or eviction; `all` keeps independent provider targets.
10. Capacity is released only after remote deletion is confirmed or the pin is already absent.
