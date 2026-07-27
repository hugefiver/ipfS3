# Post-Release Hardening Plan — Review Revision

> **Revision status:** This is a review-fix revision of the 2026-07-23 plan. It describes the implemented hardening shape and its current evidence; it does not reconstruct or claim historical RED results that were not recorded. The Kubo body-redaction tests did have a recorded RED→GREEN transition.

## Goal

Close the post-release Kubo transport, ZIP decompression-budget, and pinning-worker observability findings while preserving the existing conservative local-pin, transaction, and SSE-C multipart contracts.

## Scope and invariants

- Start from `HEAD 3232b85` and retain the reviewed, uncommitted hardening work already in the tree.
- Do not run a Git write command. A separate user authorization is required for any commit.
- Never add a `pin_rm` production call outside `src/kubo/pin.rs`.
- Never make provider or Kubo HTTP calls inside a database transaction.
- Do not log provider tokens or response bodies, SSE-C material, master keys, or Kubo error bodies.
- Do not change legacy SSE-C multipart claim ordering or `DeleteBucket` behavior.
- Do not install software, use the network, or start Docker.

## File structure

| File | Responsibility in this revision |
|---|---|
| `src/error.rs` | Safe, typed `AppError::KuboRpc` boundary and fixed S3-visible backend message |
| `src/kubo/client.rs` | Separate control, upload, and download `reqwest::Client`s plus timeout evidence |
| `src/kubo/add.rs` | Streaming add, safe non-2xx handling, add redaction tests |
| `src/kubo/cat.rs` | Streaming cat, safe non-2xx handling, safe body-stream errors |
| `src/kubo/pin.rs` | Control-plane pin calls and non-2xx redaction tests |
| `src/crypto/chunker.rs` | Preserve Kubo body-stream provenance through encrypted stream decryption |
| `src/s3/ops/object.rs` | Typed Kubo propagation at the S3 boundary and PutObject safety evidence |
| `src/s3/ops/multipart.rs` | Typed Kubo propagation without changing SSE-C claim ordering |
| `src/s3/route/decompress_zip.rs` | Typed Kubo propagation and route-level ZIP budget evidence |
| `src/zip/extract.rs` | Archive-wide decompressed-byte budget and early-upload-failure drain behavior |
| `src/pinning/worker.rs` | Job identity correlation for join failures and execution failures |
| `src/store/pinning/leases.rs` | Parallel-safe fixture identities for the lifecycle-order release test |

## Task 1 — Kubo clients and safe error boundary

### Client interface

`KuboClient` owns three clients:

| Accessor | Use | Timeout policy |
|---|---|---|
| `http()` | Short control-plane operations, including `pin_add` and `pin_rm` | 30-second connect timeout and 300-second whole-request timeout |
| `upload_http()` | `/api/v0/add` request bodies | 30-second connect timeout only; no whole-request or read timeout |
| `download_http()` | `/api/v0/cat` response bodies | 30-second connect timeout and a 120-second inter-chunk read timeout; no whole-request timeout |

Uploads cannot use reqwest's read timeout: Kubo may consume a large request before emitting any response bytes. Downloads use the read timeout only as an inter-chunk liveness bound. The download test uses a local chunked TCP response that sends headers and `first` immediately, then stalls the next chunk beyond an injected 50 ms timeout; it proves that the first chunk is readable and the subsequent read fails by timeout. The upload slow-response and connection-setup regressions remain covered.

### Error contract

Kubo add, cat, and pin APIs return `AppResult` with `AppError::KuboRpc { status, detail }`. Its `Display` is always `kubo rpc failure`; it does not render `detail`. `From<AppError> for S3Error` renders every Kubo RPC failure as:

```
InternalError: internal storage backend error
```

All initial Kubo calls in object, multipart, and decompress-zip production paths propagate the typed error to this conversion boundary. Deferred successful cat body-stream failures are replaced with the same fixed literal before entering an S3 response stream.

On non-2xx, add/cat/pin log only the operation, status, and (where useful) CID. They return the status-bearing typed error without calling `resp.text()`, reading, storing, or logging the Kubo response body. Successful add responses alone are read to parse Kubo's JSON result.

### Evidence

The Kubo redaction tests were written before the boundary change. Their RED run was:

```powershell
cargo test --lib kubo:: -- --nocapture
```

It ran 12 tests with four failures because captured tracing contained the marker `kubo-body-marker-do-not-leak` from add, cat, pin-add, and pin-rm non-2xx responses. After the safe boundary implementation, the same command passed all 12 tests. The tests assert that the marker is absent from both returned errors and tracing captures. The PutObject pin-add failure test additionally asserts the S3 message is exactly `internal storage backend error` and contains neither its Kubo marker nor an internal URL. The later stream-provenance addendum brings the current `kubo::` targeted suite to 13 passing tests.

## Task 2 — Archive-wide ZIP budget

`extract_zip_stream_with_limit` maintains a single remaining decompressed-byte budget for an entire archive; the production wrapper uses `MAX_DECOMPRESSED_ARCHIVE_BYTES` (8 GiB). Every decompressed entry byte is charged, including bytes drained after an entry upload fails. Crossing the budget returns the global `InvalidParameterValue` rejection (HTTP 400), rather than an entry-local failure.

The entry upload races bounded copying with the Kubo add future. An early add failure shuts down the duplex writer, reports an upload failure, and allows the caller to drain the remaining entry against the same total budget. A copy budget failure also shuts down the writer so the add future cannot deadlock. Extraction rejects globally before route publication; existing local staged pins are retained and no `pin_rm` is introduced.

The route's private limit-taking helper lets the S3 handler test exercise the production PUT path with a small deterministic limit while keeping the public production API unchanged.

### Evidence

- `extraction_stops_when_the_archive_exceeds_the_decompressed_budget` uses two 5-byte entries with a 7-byte limit. Each entry independently fits, but the archive does not. It checks `InvalidParameterValue`/HTTP 400, one `/add`, one `/pin/add`, no `/pin/rm`, and no object publication.
- `early_entry_upload_failure_drains_into_the_global_budget` uses a local TCP Kubo fixture that returns the second add failure before consuming its complete request body. The failed entry itself fits, but draining the remainder crosses the archive budget. It checks global rejection, no hang, no publication, and no unpin.
- `put_budget_rejection_is_a_global_400_without_publication` performs the same cumulative condition through the route PUT helper and checks the S3 error boundary, exact Kubo calls, and no publication.

Recorded targeted GREEN commands:

```powershell
cargo test --lib zip::extract:: -- --nocapture                 # 16 passed
cargo test --lib s3::route::decompress_zip:: -- --nocapture   # 50 passed
```

## Task 3 — Worker failure identity

The pinning worker records a `JobLogContext` keyed by the `JoinSet` task ID when it spawns a job and removes it on every successful/error join path: the normal select join, tick-time `try_join`, and shutdown drain. Both task join failures and in-task `pinning job execution failed` logs carry the exact `job_id`, `provider`, `cid`, `lease_id`, and `target_id` fields.

The evidence uses actual worker execution, isolated tracing subprocesses, and deterministic notification barriers rather than sleeps or direct log-helper tests:

- shutdown drain panic logs all five fields for its submit job;
- a successful task is joined before a later tick-time panic, proving stale task context cannot be reused;
- a deterministic post-status store failure reaches `pinning job execution failed` and logs all five fields;
- assertions also exclude provider body and token-like fixtures.

Recorded targeted GREEN command:

```powershell
cargo test --lib pinning::worker:: -- --nocapture              # 79 passed
```

## Task 4 — Documentation and final gates

The companion architecture/readme/spec documentation updates remain part of the working tree. This plan is synchronized to the three-client Kubo implementation and review evidence above. The release expectation after this revision is **527 library tests passed** (not the obsolete 515 estimate).

During the final library gate, `publication_prelocks_union_of_leases_then_targets_then_remotes` exposed a test-only parallel collision: its process-global lifecycle recorder matched another test's generic `filebase`/`bafy-a` fixture pair. The test now uses names unique to that recorder assertion; its production behavior and lock-order expectation are unchanged.

## Review addendum — Kubo stream provenance and worker join cleanup

Successful Kubo `cat` responses can still fail after their headers and first body bytes. The body stream will carry a private, downcastable Kubo-backend marker as the source of a fixed-message `io::Error`; it contains no URL, response body, or transport detail. `AppError` exposes source-chain detection so consumers can preserve this provenance without string matching.

ZIP extraction treats that marker as a global `AppError::KuboRpc` at every archive-reading boundary: next-entry parsing, local-header observation, directory draining, entry copying, and entry completion. It therefore becomes the fixed S3 `InternalError` instead of a malformed-archive 400 or an `EntryReadFailed` partial result. Ordinary ZIP and input read failures retain their present 400 and entry-failure behavior.

`decrypt_chunk_stream` recognizes the marker through its generic input error source chain and yields `AppError::KuboRpc`; ordinary input stream errors remain `AppError::Internal`. Buffered encrypted object, legacy SSE-C, and multipart authentication paths propagate that typed Kubo error through `From<AppError> for S3Error`. Streaming responses remain constrained to safe fixed `io::Error` messages. Pinata's Kubo-read mapping remains a generic safe transient provider error.

The worker will route main-loop join, tick-time join, and shutdown-drain results through one context-removal/logging helper. A unit test uses a real `JoinSet` success ID/result and asserts its matching map entry is removed. The execution-error trace fixture supplies a `RemotePin.failure_reason` containing bearer/body/token markers and verifies the actual worker path never logs them.

The discriminating tests use a local TCP Kubo fixture that sends a 200 response and a first chunk, then stalls past an injected download idle timeout. Notification/oneshot signals establish the first-chunk boundary; outer timeouts only prevent hangs. GREEN evidence is: `kubo::` 13 passed, `zip::extract::` 16 passed, `s3::ops::object::` 39 passed, `s3::ops::multipart::` 38 passed, `crypto::chunker::` 2 passed, and `pinning::worker::` 79 passed. The final library count is 527 after the release gate.

Recorded RED evidence for this addendum: the new tests first failed to compile because neither `has_kubo_stream_provenance` nor the shared `handle_join_result` existed. After the initial marker implementation, the raw TCP cat test still failed because `std::io::Error::get_ref()` was not traversed; its safe text was present but provenance was not detectable. The final source-chain traversal and shared handler made those tests GREEN. These are current review-fix RED results, not reconstructed historical evidence.

Run the final gates after all source and documentation changes:

```powershell
cargo test --lib
cargo test --test integration
cargo test --test e2e --no-run
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
rg "pin_rm\(" src
```

The last command may match only `src/kubo/pin.rs` (the implementation and its tests). Run language-server diagnostics for every changed Rust file and inspect `git status --short`/`git diff --stat` read-only. Do not commit as part of this plan.

## Acceptance criteria

1. Kubo non-2xx response bodies are fully discarded: not read and not logged; Kubo details never reach ordinary logs or S3 clients.
2. S3-visible Kubo failures are consistently `internal storage backend error`, including successful-response streaming failures.
3. ZIP budget enforcement is archive-wide, includes post-failure drains, cannot hang on early upload failure, publishes nothing on global rejection, and never unpins staged local CIDs.
4. Worker join and execution failure logs carry the five exact job identity fields without leaking provider secrets or bodies.
5. The download idle test proves a real post-first-chunk stall, while upload and connection behavior remain protected.
6. Targeted suites, all release gates, diagnostics, and the pin-removal call-site audit pass without Git writes.
