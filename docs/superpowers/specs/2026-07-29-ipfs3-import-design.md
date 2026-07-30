# IPFS3 Import Design

**Date:** 2026-07-29
**Status:** Approved design
**Scope:** Durable asynchronous import of an IPFS CID or an allowlisted HTTPS URL into an S3 object key, with optional ZIP decompression and queryable progress.

## 1. Goals

1. Add a SigV4-authenticated custom S3 action named `ipfs3-import`.
2. Accept exactly one source per request: an IPFS CID or an allowlisted HTTPS URL.
3. Persist every accepted job so work survives process restarts.
4. Expose truthful provider discovery, local pin, download, IPFS add, decompression, and publication progress.
5. Allow `ipfs3-import` and `decompress-zip` on the same submission.
6. Preserve normal S3 behavior and visibility for every request that does not exactly match the custom action.
7. Ensure a superseded worker can never publish stale content, even if its network operation ignores cooperative cancellation temporarily.
8. Make every later content replacement or deletion of the same destination take precedence over an active import.

## 2. Non-goals

- This is not an AWS-standard S3 operation and will not be added to the `s3s::S3` trait.
- This release does not add a separate Axum REST API.
- This release does not support `http://`, redirects, private-network URL sources, authenticated header forwarding, or URL upload resume.
- This release does not support SSE-S3 or SSE-C headers on import submissions. Existing standard S3 encryption behavior remains unchanged.
- Provider discovery does not estimate replica count or guarantee remote durability.
- Jobs do not expose partially imported bytes through ordinary S3 `GET`, `HEAD`, or `ListObjects` operations.
- This design does not introduce automatic Kubo `pin/rm`; it retains the repository's existing shared-CID safety policy.

## 3. Compatibility invariants

The following are non-negotiable:

1. `GatewayRoute::is_match` claims only the exact method/query combinations defined in this document.
2. Requests without a decoded `ipfs3-import` query key continue through the existing `DecompressZipRoute` or standard `S3Impl` path unchanged.
3. Every custom route call invokes `S3Route::check_access` before reading or mutating job data.
4. While an import runs, ordinary reads return the previous committed object. If no previous object exists, they return normal `NoSuchKey`.
5. Ordinary listings contain only committed objects and never contain a job placeholder.
6. Object content becomes visible only through the existing atomic publication transaction.
7. Existing PUT, GET, HEAD, COPY, DELETE, tagging, listing, multipart, SSE, and authentication responses retain their existing wire contracts.

## 4. Public S3 extension contract

### 4.1 Submit a job

```http
POST /{bucket}/{key}?ipfs3-import HTTP/1.1
Authorization: AWS4-HMAC-SHA256 Credential=TESTACCESS/20260729/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000
Content-Type: application/xml
x-ipfs3-client-token: optional-client-token

<IPFS3ImportRequest>
  <CID>bafybeigdyrzt5sfp7udm7hu76uh7y26nf3wok5z65r4m2a5qhnwdd4mwei</CID>
</IPFS3ImportRequest>
```

URL form:

```xml
<IPFS3ImportRequest>
  <URL>https://downloads.example.com/path/archive.zip?signature=test-signature</URL>
</IPFS3ImportRequest>
```

Request rules:

- The decoded `ipfs3-import` query key must occur exactly once and have an empty value.
- The XML body is capped at 64 KiB.
- Exactly one of `CID` and `URL` must be present and non-empty.
- Unknown XML elements, duplicate elements, mixed text, DTDs, and entities are rejected.
- A CID is parsed canonically before the job is created.
- A URL is normalized and authorized before the job is created.
- `Content-Type` must describe the XML submission body. Optional `x-ipfs3-object-content-type`, `x-amz-tagging`, and `x-amz-meta-*` are captured for final object publication.
- If `x-ipfs3-object-content-type` is absent, a URL import uses a valid source response `Content-Type`; a CID import publishes no content type.
- Any SSE-S3 or SSE-C request header returns `InvalidArgument` before a job is created.
- `uploadId`, `uploads`, or a non-empty `ipfs3-import` value on POST is an invalid combination.

The response is:

```http
HTTP/1.1 202 Accepted
Content-Type: application/xml
Location: /bucket/key?ipfs3-import=7f9f0d4b-4ad1-4b54-9a86-14fd992b03e1
x-ipfs3-import-job-id: 7f9f0d4b-4ad1-4b54-9a86-14fd992b03e1

<IPFS3ImportAccepted>
  <JobId>7f9f0d4b-4ad1-4b54-9a86-14fd992b03e1</JobId>
  <State>queued</State>
  <Phase>queued</Phase>
</IPFS3ImportAccepted>
```

`s3s 0.14` supports the status override through `S3Response::with_status` or its public `status` field.

### 4.2 Optional idempotency

`x-ipfs3-client-token` is optional, 1-128 visible ASCII characters, and scoped to `(bucket, key)`.

- Repeating the same canonical request with the same token returns the original job and does not supersede it.
- Reusing the token with a different source, decompression prefix, tags, object content type, or metadata returns `IdempotentParameterMismatch`.
- A request without a token always creates a new job and supersedes the previous active owner.
- The database stores a SHA-256 fingerprint of the canonical request for comparison.

### 4.3 Combine import and decompression

```http
POST /bucket/archive.zip?ipfs3-import&decompress-zip=prefix/ HTTP/1.1
```

Rules:

- `decompress-zip` may occur once and uses the existing target-prefix normalization and sanitization rules.
- `decompress-zip-result` is not accepted on import submission because the result is obtained from the status operation.
- `ImportObjectRoute` owns a request containing both query keys; it does not forward the request to `DecompressZipRoute`.
- Existing direct and multipart `decompress-zip` requests remain unchanged.

### 4.4 Query status and results

```http
GET /{bucket}/{key}?ipfs3-import={job-id} HTTP/1.1
```

Optional result pagination:

```http
GET /{bucket}/{key}?ipfs3-import={job-id}&max-results=100&continuation-token=MTAw HTTP/1.1
```

Rules:

- The job ID is a UUID generated by the server.
- The requested bucket/key must match the job's destination. A mismatch returns `NoSuchImportJob`, preventing job-ID probing.
- `max-results` defaults to 100 and is capped at 1000.
- The continuation token is an opaque encoding of the job/result cursor and is valid only for that job.
- Status GET is read-only and never changes task ownership or retry timing.
- Terminal and non-terminal job states return HTTP 200. Runtime failure is represented in the XML state, not as an HTTP error.

Representative response:

```xml
<IPFS3ImportStatus>
  <JobId>7f9f0d4b-4ad1-4b54-9a86-14fd992b03e1</JobId>
  <State>running</State>
  <Phase>pinning_local</Phase>
  <Attempt>1</Attempt>
  <Progress>
    <ProvidersObserved>3</ProvidersObserved>
    <PinNodesProcessed>124</PinNodesProcessed>
    <PinBytesProcessed>9437184</PinBytesProcessed>
  </Progress>
  <CreatedAt>2026-07-29T00:00:00Z</CreatedAt>
  <UpdatedAt>2026-07-29T00:00:03Z</UpdatedAt>
</IPFS3ImportStatus>
```

The response never includes the source URL. A completed decompression response additionally contains the archive CID/size, aggregate entry counters, one result page, and an optional next continuation token.

## 5. Routing architecture

`s3s 0.14` has one custom route slot. The service therefore registers one composite route:

```text
S3ServiceBuilder
└── GatewayRoute
    ├── ImportObjectRoute
    └── DecompressZipRoute
```

Routing order:

1. `POST` with decoded `ipfs3-import` present: `ImportObjectRoute::submit`.
2. `GET` with decoded `ipfs3-import` present: `ImportObjectRoute::status`.
3. Existing decompression predicates: `DecompressZipRoute`.
4. Otherwise `GatewayRoute::is_match` returns false, allowing normal s3s dispatch.

Import matching is intentionally based on the decoded key, using `src/s3/query.rs`, so matching and SigV4 canonical-query behavior remain consistent. Duplicate or malformed values are rejected inside `call`; `is_match` only decides ownership.

Implementation boundaries:

```text
src/s3/route/gateway.rs          composite custom-route dispatcher
src/s3/route/import_object.rs    auth, request parsing, response building
src/import/mod.rs                coordinator and public types
src/import/worker.rs             durable claims, retries, cancellation
src/import/downloader.rs         HTTPS allowlist and bounded streaming
src/import/pipeline.rs           CID/URL/decompression orchestration
src/import/response.rs           XML status and result pages
src/store/import/                job, ownership, progress, and result CRUD
src/kubo/routing.rs              streamed provider discovery
src/kubo/add.rs                  reusable streamed add progress
src/kubo/pin.rs                  reusable streamed local-pin progress
```

ZIP extraction orchestration must be exposed from a focused reusable module. Import behavior must not be added directly to the already oversized `decompress_zip.rs`.

## 6. Configuration

Configuration is added under `[imports]` with serde defaults so existing configuration files continue to load:

```toml
[imports]
enabled = true
allowed_https_origins = ["https://downloads.example.com"]
worker_concurrency = 4
poll_interval_ms = 500
lease_duration_secs = 60
progress_flush_interval_ms = 1000
connect_timeout_secs = 10
idle_timeout_secs = 120
job_timeout_secs = 86400
max_download_bytes = 5368709120
max_attempts = 5
terminal_retention_secs = 604800
max_provider_records = 20
```

Validation rules:

- Every allowed origin must be an absolute HTTPS origin with no path, query, fragment, or userinfo.
- Origin hostnames are normalized before duplicate detection.
- IP-literal origins are rejected in this release.
- Ports are normalized so omitted HTTPS port and explicit port 443 are equivalent.
- Numeric limits and durations must be non-zero and fit their implementation types.
- `max_provider_records` must be in `1..=20` and defaults to 20.
- An empty allowlist disables URL sources but does not disable CID imports.
- `enabled = false` makes exact import requests return `NotImplemented` without affecting other S3 operations.

No environment-variable encoding for the origin list is added in this release. Existing config-file-plus-env precedence remains unchanged for existing fields.

## 7. Persistent data model

Import persistence is separate from provider-oriented `pin_jobs`.

### 7.1 `import_jobs`

Principal fields:

- `id`
- `bucket`, `key`
- `source_type`: `cid | url`
- `source_value`: canonical CID or normalized source URL
- `request_fingerprint`, nullable `client_token`
- captured content type, metadata, and tags
- nullable decompression prefix
- `state`, `phase`, `attempts`, `next_attempt_at`
- `locked_by`, `locked_until`, and monotonically increasing `claim_epoch`
- progress counters and nullable known totals
- nullable final CID and logical size
- nullable stable failure code and redacted failure message
- result counters
- `created_at`, `updated_at`, `completed_at`

The source URL is required for restart-safe execution and is sensitive database data. It is never returned by the API, placed in tracing fields, or embedded in error messages.

### 7.2 `import_destinations`

Primary key: `(bucket, key)`.

Fields:

- monotonically increasing `generation`
- nullable `owner_job_id`
- `updated_at`

This row is the exact-key publication fence. Generations are never decremented or reused.

### 7.3 `import_prefix_claims`

Fields:

- `job_id`, `bucket`, normalized `prefix`
- claim generation/creation order

A combined import claims its normalized decompression prefix at submission time. This is necessary because output keys are not known until ZIP parsing. A later content mutation whose destination key falls under the prefix supersedes the job before that output entry is discovered. An empty prefix intentionally claims the whole bucket for conflict detection.

Overlapping active prefix claims in the same bucket supersede the older job. A new exact destination inside an active prefix also supersedes the prefix-owning job, except when the operation belongs to that same job.

### 7.4 `import_job_targets`

Fields:

- `job_id`, `bucket`, `key`
- `expected_generation`
- `kind`: `archive | extracted`

The archive target is recorded at submission. Extracted targets are recorded as sanitized entries are discovered. Prefix claims provide early conflict detection; exact targets provide final transactional publication fencing.

### 7.5 `import_job_results`

Ordered rows store successful and failed extraction entries:

- job ID and monotonic result sequence
- key
- nullable CID and size
- nullable stable error code and redacted message

Rows support cursor pagination without assembling an unbounded XML document. Terminal jobs and result rows are retained for `terminal_retention_secs`; active jobs are never removed by retention cleanup.

SQLite and PostgreSQL migrations must enforce state/phase/source checks, uniqueness of active ownership, idempotency token scope, and referential cleanup.

## 8. Job state model

Top-level states:

```text
queued | running | completed | failed | superseded
```

Phases:

```text
queued
discovering_providers
pinning_local
downloading
adding_to_ipfs
inspecting
decompressing
publishing
```

`state` describes scheduling/terminal outcome. `phase` describes current work. `completed`, `failed`, and `superseded` are terminal.

### 8.1 CID pipeline

```text
queued
→ discovering_providers
→ pinning_local
→ inspecting
→ optional decompressing
→ publishing
→ completed
```

Progress fields:

- `providers_observed`
- `pin_nodes_processed`
- `pin_bytes_processed`
- nullable `logical_size`

`/routing/findprovs` records peers observed advertising the CID at lookup time. It does not count durable copies, complete DAG replicas, or guaranteed reachable providers. A zero result does not by itself terminate the job; `pin/add` remains the authoritative retrieval attempt.

`pin/add?recursive=true&progress=true` provides processed node and byte counters but no trustworthy total. The API therefore exposes counters, never a percentage.

After pinning, inspection must reject directories and sources that cannot produce one S3 byte stream. Logical UnixFS size is taken from a trustworthy Kubo response length when available; otherwise the gateway streams and counts `cat` output.

### 8.2 URL pipeline

```text
queued
→ downloading
→ adding_to_ipfs
→ pinning_local
→ optional decompressing
→ publishing
→ completed
```

Progress fields:

- `downloaded_bytes`
- nullable `download_total`
- `ipfs_add_bytes`
- local pin counters

The HTTPS response body streams directly into Kubo add and is never fully buffered. Download and upload can overlap. While source bytes are arriving, the phase is `downloading`; both download and IPFS counters may advance. After source EOF, the phase becomes `adding_to_ipfs` while Kubo finishes UnixFS construction and returns the final CID.

`download_total` is populated only from a valid, policy-compliant `Content-Length`. Unknown-length responses never show a percentage. The byte limit is enforced independently of the header.

Kubo add uses `pin=false`; after the final CID is known, the worker performs the same explicit local recursive pin step used by ordinary object publication.

### 8.3 Decompression pipeline

When requested, status adds:

- `entries_processed`
- `entries_succeeded`
- `entries_failed`
- `decompressed_bytes`

The imported archive is read from local Kubo and passed to the existing bounded streaming extractor. Existing path sanitization, archive size, entry count, compression method, and collision rules remain authoritative.

Entry-level failures are recorded and successful entries remain eligible for publication, matching existing `decompress-zip` behavior. A fatal parser/limit error, archive-key collision, cancellation, or loss of any target ownership prevents all publication by this job.

## 9. Worker lifecycle, retries, and cancellation

`ImportCoordinator` owns configuration and starts a durable worker from `main.rs` using a child of the process cancellation token. Shutdown follows the existing worker pattern and has a bounded grace period.

Worker behavior:

1. Fairly claim due jobs with a database lease.
2. Increment `claim_epoch` in the same conditional update that sets `running`, `locked_by`, and `locked_until`.
3. Refresh the lease while making progress.
4. Persist phase transitions immediately.
5. Coalesce progress writes to at most once per configured interval.
6. Fence every worker-authored progress, retry, failure, and publication update by job ID, worker ID, claim epoch, and unexpired lease.
7. Check cancellation between network frames and pipeline stages.
8. Abort owned HTTP/Kubo requests when superseded or shutting down.
9. Before publication, rely on the transactional generation and claim-epoch guard rather than cancellation state alone.

Expired leases are reclaimable after restart. A URL retry starts from byte zero; no resumability is advertised. Existing Kubo blocks from an interrupted content-addressed add may reduce backend work, but the gateway does not depend on that optimization.

Retryable conditions include transient DNS/connect/TLS transport errors, idle timeouts, HTTP 5xx, and transient Kubo RPC errors. Non-retryable conditions include authorization failure, URL policy violations, redirects, HTTP 4xx, size limits, invalid CID, unsupported source type, invalid ZIP, decompression limits, and deterministic publication conflicts.

Retries use bounded exponential backoff with jitter and stop at `max_attempts` or the overall job deadline. Graceful process shutdown does not consume an attempt or mark a job failed; lease recovery resumes it later.

## 10. URL authorization and SSRF defense

URL validation is performed before job creation and again before every attempt.

1. Parse with the `url` crate and canonicalize scheme, IDNA hostname, and port.
2. Require `https`, a DNS hostname, and exact origin membership.
3. Reject userinfo and fragments. Query strings are allowed for signed download URLs.
4. Configure the HTTP client with redirects disabled. Every 3xx response is terminal.
5. Resolve the hostname through the configured resolver.
6. Reject every loopback, RFC1918/private, link-local, multicast, unspecified, documentation/reserved, IPv4-mapped forbidden IPv6, and known cloud metadata address.
7. Connect only to validated resolved addresses while retaining the original host for TLS certificate validation and SNI.
8. Re-resolve and revalidate on each retry. Never perform a separate unchecked resolver lookup during connection.
9. Send a fresh GET with no client-supplied Authorization, Cookie, proxy credential, or arbitrary forwarded header; the import client does not honor system proxy settings.
10. Require a successful 2xx response and enforce connection, inter-chunk idle, total-job, and byte limits.

Logs identify jobs by job ID, bucket/key, source type, phase, and redacted error class. They never include the full URL or query string.

## 11. Destination ownership and stale-write prevention

Cooperative cancellation is an optimization. Publication authorization comes only from database ownership.

### 11.1 Import submission

The submission transaction:

1. Locks and verifies the bucket still exists.
2. Resolves idempotent replay before changing ownership.
3. Inserts the queued job within the still-uncommitted transaction.
4. Locks/upserts the primary destination row, increments its generation, records the archive target, and assigns the new job as owner.
5. Marks the previous exact owner and any active prefix-owning job containing the primary destination `superseded`, releasing their claims transactionally.
6. If decompression is requested, installs the normalized prefix claim and supersedes older overlapping claims and active exact-key jobs inside that prefix.
7. Commits job creation, ownership, targets, prefix claims, and supersession together; no unowned accepted job is externally visible.

### 11.2 Standard content mutations

At operation admission, the following destination mutations increment the exact-key generation, clear active import ownership, supersede matching exact/prefix jobs, and install a durable standard-mutation token on the destination row:

- `PutObject`
- destination side of `CopyObject`
- `DeleteObject` and each `DeleteObjects` key
- `CompleteMultipartUpload`
- direct `decompress-zip` publication targets
- a newer `ipfs3-import`

Admission returns a `StandardMutationGuard` containing the token, exact destination generation, and, for direct or multipart ZIP decompression, the normalized output prefix. Later exact admission invalidates an older token for the same key and any older active prefix token containing that key. Later prefix admission invalidates older active exact tokens under the prefix and overlapping prefix tokens. Import submission and extracted-target admission perform the same invalidation. Comparisons are literal and case-sensitive, so `%` and `_` are data rather than SQL wildcards. Active-token scans are keyset-paginated in canonical key order and never scan ownerless destination history.

Admission order determines the winner even if the later operation subsequently fails. Multipart creation, part upload, abort, reads, listing, and tagging do not replace object content and do not supersede an import or invalidate a standard mutation token. A standard mutation that fails after admission still leaves the import superseded; the previously committed object remains visible until a later successful publication.

Every ownership-changing transaction locks the bucket row first, then job/destination rows in canonical key order. PostgreSQL uses a row lock; SQLite acquires write intent through the same bucket row. Submission, exact/prefix admission, extracted-target claims, terminal claim release, guarded publication, and bucket deletion all follow this order so concurrent prefix/exact operations cannot miss uncommitted ownership changes.

A direct `decompress-zip` request knows its normalized target prefix at admission. It supersedes active import prefix claims that overlap that output scope and active exact-key import destinations under that prefix, as well as any import owning the archive key. This remains true even when the archive later contains no entry for one of those potential keys; operation admission, not eventual ZIP membership, establishes precedence.

`DeleteBucket` atomically supersedes all jobs/claims in the bucket before successful deletion. An import submission and bucket deletion serialize on bucket state so no accepted job can publish into a deleted bucket.

### 11.3 Combined decompression ownership

The prefix claim protects not-yet-discovered output keys. As each entry is discovered, the worker locks the bucket, verifies its exact worker ID, claim epoch, running state, and unexpired lease, then claims the exact sanitized key and records the expected generation in one transaction. An expired/reclaimed worker cannot mutate destinations. If another operation supersedes any archive or output target, the entire combined job becomes `superseded` and publishes nothing.

### 11.4 Final publication

Import publication uses its `ImportPublicationGuard`. Guarded standard object, ZIP, completed-multipart, and delete entry points require the `StandardMutationGuard` returned by admission. Their final transaction locks the bucket first and verifies the standard token, exact generation, absent import owner, archive key, and normalized prefix before any object/delete side effect. The same transaction writes or deletes object rows, tags, leases, targets, provider reservations, remote pin jobs, multipart-upload state, and then clears the standard token. A preflight-only check is never sufficient. A stale standard guard aborts without changing publication, lease, result, or provider-job state and maps to stable S3 `OperationAborted` with HTTP 409.

Inside the import publication transaction, the import guard:

1. Locks the archive bucket row.
2. Locks every expected destination row in canonical key order.
3. Locks the job and verifies the expected worker ID, claim epoch, running state, and unexpired lease.
4. Verifies `owner_job_id` and generation for every target.
5. Verifies the job is still the active non-terminal owner.
6. Verifies the bucket still exists.
7. Publishes objects and result rows.
8. Marks the job completed and releases exact/prefix ownership.

Any mismatch aborts the transaction. An ownership mismatch caused by a newer operation has already superseded the job in that operation's transaction. A worker/claim-epoch/lease mismatch is a stale worker and must exit without changing the reclaimed job. Import ownership loss remains `StaleImportOwnership` and becomes `Superseded` in worker semantics. A pre-transaction read is never treated as sufficient authorization.

## 12. Atomic visibility and decompression result semantics

An import without decompression publishes one object atomically after local pin completion and size validation.

A combined import performs no S3 publication until extraction has reached a valid final outcome. The final transaction publishes:

- the imported archive;
- every successfully extracted entry;
- the archive and entry tags/metadata required by existing decompression semantics;
- durable remote pinning leases/jobs selected by existing policy;
- paginated import result rows;
- the completed job state.

A fatal extraction error publishes none of these objects. The previous committed archive and entry objects remain unchanged. Entry-level extraction failures are included in the terminal result while successful entries are published together.

This contract applies only to the combined async job. Existing direct `decompress-zip` behavior remains wire-compatible.

## 13. Error contract

Synchronous custom-route errors use normal S3 XML formatting:

| Condition | Error code | HTTP |
|---|---|---:|
| Malformed/oversized XML, duplicate source, invalid CID/query combination | `InvalidArgument` | 400 |
| SSE header on import | `InvalidArgument` | 400 |
| Missing bucket | `NoSuchBucket` | 404 |
| URL origin or resolved address not allowed | `AccessDenied` | 403 |
| Reused idempotency token with different request | `IdempotentParameterMismatch` | 409 |
| Unknown job or path/job mismatch | `NoSuchImportJob` | 404 |
| Import feature disabled | `NotImplemented` | 501 |

Asynchronous failures are returned by status GET with stable values such as:

```text
source_unreachable
source_http_error
source_redirected
source_too_large
source_stalled
cid_not_found
cid_not_file
kubo_add_failed
kubo_pin_failed
invalid_archive
decompression_limit_exceeded
publication_failed
job_deadline_exceeded
```

Messages are bounded and redacted. Kubo/database internals, source URLs, resolved forbidden addresses, credentials, and raw provider responses are not returned.

## 14. Retention and cleanup

- Terminal jobs and result rows are retained for seven days by default.
- Cleanup deletes only terminal jobs older than the configured retention period.
- Active destination and prefix ownership is released at terminal transition; generations remain monotonic.
- Status for a removed job returns `NoSuchImportJob`.
- Kubo local pins are not automatically removed on failure or supersession because the same CID may already be referenced by another key and the current gateway has no local-pin reference count.
- URL add uses `pin=false`, limiting abandoned data before the explicit pin stage; Kubo GC policy remains an operator concern.

## 15. Testing strategy

### 15.1 Unit tests

- Exact route predicates, duplicate query behavior, route priority, and fallback.
- Strict XML parsing and 64 KiB limit.
- CID canonicalization.
- Origin normalization, allowlist matching, IP classification, redirect rejection, and URL redaction.
- State/phase transitions and progress coalescing.
- Kubo `findprovs`, add, and pin newline/event streaming, malformed frames, idle bounds, and cancellation.
- Result pagination and continuation-token validation.
- Config defaults and fail-fast validation.

### 15.2 Store and worker tests

- SQLite and PostgreSQL migration constraints.
- Atomic submission, idempotent replay, and token mismatch.
- Lease claim, expiry, restart recovery, retry timing, and retention.
- Exact-key and prefix ownership, overlapping prefixes, and monotonic generations.
- A canceled worker cannot publish after a newer import.
- A canceled worker cannot publish after PUT, COPY, DELETE, multipart completion, direct decompression, or bucket deletion.
- Loss of one combined-job output target prevents every publication by that job.
- Publication guard checks and object publication occur in one transaction.
- A completed newer import prevents every older PUT, COPY destination, DELETE, DeleteObjects key, and non-ZIP multipart completion from publishing or deleting.
- A completed newer exact import prevents an older overlapping direct or multipart ZIP publication, while unrelated exact keys do not conflict.
- A later overlapping prefix admission prevents an older exact or prefix standard publication even if the later operation subsequently fails.

### 15.3 Integration tests

Use the existing real-TCP SigV4 harness, mock Kubo, and mock HTTPS source:

1. CID happy path reports provider and pin progress, then publishes the original CID and logical size.
2. URL happy path reports download/add progress, pins, and publishes.
3. Unknown `Content-Length` never produces a percentage and still enforces the byte limit.
4. Existing object remains readable throughout an overwrite import; a new key returns `NoSuchKey` until completion.
5. Combined import/decompress remains invisible until one atomic publication and returns paginated results.
6. Fatal ZIP failure preserves all previous objects.
7. A standard destination mutation supersedes a running job without changing that standard operation's response.
8. With an older standard multipart or direct ZIP operation blocked after admission, a newer overlapping import completes first; releasing the older operation returns `OperationAborted` and creates no stale object, result, lease, or provider-job state.
9. Wrong credentials fail before job lookup or mutation.
10. URL redirect, forbidden DNS answer, rebinding attempt, timeout, and over-limit stream fail safely.
10. Worker restart reclaims jobs without allowing two owners to publish.

### 15.4 Standard S3 regression suite

Run and preserve the existing standard tests for:

- PUT/GET/HEAD/DELETE/COPY;
- ListObjects v1/v2 and nested keys;
- tagging;
- multipart create/upload/complete/abort;
- direct and multipart decompression;
- SSE-S3 and SSE-C standard PUT;
- SigV4 success and wrong-credential rejection.

Add explicit assertions that requests with unrelated query parameters do not match `GatewayRoute` and cause no import, Kubo routing, or downloader activity.

### 15.5 Real-surface acceptance

- Send signed raw HTTP POST and GET requests against the running gateway.
- Capture accepted/status XML for CID, URL, and combined decompression jobs.
- While a worker is blocked, issue ordinary S3 reads and a same-key standard write, then prove the old worker cannot publish.
- While an older standard ZIP operation is blocked after admission, fully publish a newer overlapping import first, then prove the older operation fails with `OperationAborted` and the import remains the only visible/latest publication state.
- Run `cargo test --lib`, `cargo test --test integration`, formatting, lint, and diagnostics after implementation.

## 16. Implementation constraints

- Preserve streaming; never collect a source object or archive into memory.
- Bound every XML body, result page, network stream, timeout, retry count, and stored error.
- Keep progress events truthful and source-specific.
- Keep import persistence separate from remote-provider `pin_jobs`.
- Avoid adding import responsibilities to oversized existing modules.
- Do not weaken current S3 validation, authentication, publication policy, encryption, or redaction behavior.
- No implementation may treat `providers_observed` as a replica count.

## 17. Implementation sequence

1. Add import configuration and persistence migrations/entities.
2. Add destination/prefix ownership primitives and publication guards.
3. Integrate supersession with standard content mutations.
4. Add streamed Kubo routing/add/pin progress APIs without changing existing call semantics.
5. Add the strict HTTPS downloader.
6. Add coordinator, worker, retry, progress, and retention behavior.
7. Extract reusable ZIP orchestration and add combined-job publication.
8. Add `ImportObjectRoute`, XML responses, and `GatewayRoute`.
9. Add focused unit/store/integration tests and run the full compatibility suite.

## 18. Source references

- AWS `PutObject`: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
- AWS `CopyObject`: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>
- Kubo RPC API: <https://docs.ipfs.tech/reference/kubo/rpc/>
- s3s 0.14 `S3ServiceBuilder`: <https://docs.rs/s3s/0.14.0/s3s/service/struct.S3ServiceBuilder.html>
- s3s 0.14 `S3Response`: <https://docs.rs/s3s/0.14.0/s3s/struct.S3Response.html>
