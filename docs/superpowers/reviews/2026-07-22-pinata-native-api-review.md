# Pinata Native API Review Report

**Verdict:** `REVIEW CHANGES REQUESTED`
**Date:** 2026-07-22
**Scope:** Uncommitted worktree changes on `master` (16 files, 2372 insertions / 31 deletions) on top of `1caff53 feat: add multi-provider pinning service`
**Reviewer:** Independent read-only reviewer session `ses_061ed2446ffeHL665VgcmfiwDF`, findings independently re-verified by the orchestrator

---

## 1. Change Summary

The change converts the Pinata provider from a pure IPFS Pinning Service API (PSA) client into a native Pinata API implementation with two orthogonal configuration dimensions:

- `api = "v3" | "legacy"` — Pinata V3 Files API versus the legacy `/pinning` + `/data` API.
- `strategy = "cid" | "upload"` — pin an existing CID versus stream object bytes from Kubo into Pinata for a fresh add.

Supporting changes:

- New `upload_endpoint` provider setting.
- `PinningCoordinator::build_with_kubo` injects a `KuboClient` so the upload strategy can read object bytes back.
- `AppState` passes the real Kubo client into the coordinator.
- `psa.rs` no longer covers Pinata; it now serves Filebase only.
- Request IDs use a self-describing `prefix + base64(provider_id:cid:metadata)` encoding so routing stays stable across all four `api × strategy` combinations while remaining compatible with bare legacy PSA IDs.

| File | Role |
|---|---|
| `src/pinning/pinata.rs` | Native Pinata client (+2191 lines) |
| `src/pinning/{config,coordinator,policy,psa,worker}.rs` | Config surface, Kubo injection, PSA narrowing |
| `src/config.rs` | `ProviderConfig` gains `api`, `strategy`, `upload_endpoint` |
| `src/state.rs` | Real Kubo client into coordinator |
| `src/s3/ops/{multipart,object,tagging}.rs`, `src/s3/route/decompress_zip.rs` | Coordinator construction call sites |
| `tests/support/pinning.rs` | Harness updates |
| `.env.example`, `README.md`, `config.example.toml` | Documentation and configuration examples |

---

## 2. Verification Evidence

All commands were actually executed by the reviewer:

| Command | Result |
|---|---|
| `cargo test --lib pinning::pinata` | 14 passed |
| `cargo test --lib` | 491 passed |
| `cargo test --test integration` | 106 passed |
| `cargo check --all-targets` | pass (1m37s) |
| `cargo fmt --all --check` | pass |
| `git diff --check` | pass |

Green gates do not clear the findings below, because the highest-risk path (upload strategy CID parity and its crash recovery) has no test coverage at all.

---

## 3. Blocker

### B1 `[product]` Upload strategy CID mismatch leaks remote pins and re-uploads forever

**Location:** `src/pinning/pinata.rs:510-513` (V3), `src/pinning/pinata.rs:629-631` (legacy, via `remote_from_legacy_pin_response`)

The upload strategy streams bytes from Kubo `cat` into Pinata for a fresh add and then requires the returned CID to equal the requested CID. That silently assumes Pinata's chunking and DAG parameters match the local `add?cid-version=1` call at `src/kubo/add.rs:40`. No test or documentation establishes that parity.

When the CIDs differ, the failure cascade is:

1. The upload has already succeeded on Pinata and consumes quota/billing.
2. The code returns `protocol_error("provider response failed pin correlation")`.
3. The Protocol class routes into `recover_submit` (`src/pinning/worker.rs:657-663`).
4. Recovery calls `find`, which queries by the **original** CID (`src/pinning/pinata.rs:900`, `list_files(Some(&query.cid))`). The uploaded artifact has a different CID, so the query never matches.
5. `record_submit_recovery_no_match` fires, submit is retried, and the **entire object is uploaded again**.

Each retry leaves an orphan pin on Pinata that the gateway has no record of and will never unpin, growing without bound until quota is exhausted. This breaks the durable request-ID/traceability contract and crosses the data/pin-loss line.

Secondary consequence: if an orphan happens to match the CID and two copies exist, `find` returns two results and the `_ =>` arm at `src/pinning/worker.rs:786-789` leaves the job permanently degraded and stuck.

**Suggested direction:** on successful upload with a mismatched CID, best-effort `DELETE` the just-returned file id before erroring; classify the error as `Terminal` (a configuration defect where retry is pointless) rather than `Protocol`; and add either a metadata-based orphan-reclamation query or a test that proves CID parity.

---

## 4. Important

### I1 `[product]` 30-second total timeout makes the upload strategy fail systematically for large objects

**Location:** `src/pinning/pinata.rs:20`, `src/pinning/pinata.rs:80-83`

`DEFAULT_TIMEOUT = 30s` is a reqwest **whole-request** deadline, and the same `Client` is used for streaming uploads. `config.example.toml` advertises `max_bytes = 100 GiB`. Any object that cannot finish uploading within 30 seconds times out, becomes `Ambiguous`, produces an empty `find`, retries, and times out again — a permanent loop that never succeeds and burns upstream bandwidth each round. The Kubo-side client at `src/kubo/client.rs:13` uses 300s, which is also insufficient for large objects.

**Suggested direction:** use a separate client for the upload path with no total timeout (connect timeout only), and document the upload strategy's size boundary.

### I2 `[product]` `validate_provider_field` drops the exact `.` / `..` rejection — a regression against the established baseline

**Location:** `src/pinning/pinata.rs:775-788`, compare `src/pinning/psa.rs:244-256`

The PSA baseline explicitly rejects exact `"."` and `".."` (`src/pinning/psa.rs:246`, `matches!(request_id, "." | "..")`). The Pinata validator only checks trim-empty, token substrings, bearer prefixes, whitespace, and control characters. A provider-returned `id = ".."` passes validation and is encoded into the request ID; later `file_url` / `pin_by_cid_request_url` build paths with `push`/`extend` (the `url` crate does not special-case dot segments), so a DELETE targets `/v3/files/public/..`, which an intermediary or the server may normalize to `/v3/files`. The impact is bounded to the same provider host, but this weakens an already-established validation baseline for no reason.

**Suggested direction:** add `matches!(value, "." | "..")` rejection to `validate_provider_field`.

### I3 `[evidence]` Upload strategy crash recovery has zero tests

The core of the crash-recovery contract is that after an `Ambiguous` submit, `find(cid, gateway_job_id)` must locate the already-successful upload. That requires the V3 multipart `keyvalues` field (`src/pinning/pinata.rs:494-500`) and the legacy `pinataMetadata` field (`src/pinning/pinata.rs:519-528`) to actually persist in a form queryable via `/files/public?cid=...` / `/data/pinList` `keyvalues`.

The existing tests (`v3_upload_strategy_streams_kubo_content_to_the_upload_api`, `legacy_upload_strategy_...`) only assert the submit URL path and the return value. They neither validate the multipart body (whether `keyvalues`, `network`, `name` are emitted and correctly shaped) nor cover any end-to-end "timeout → find → adopt" recovery. This is the highest-risk path in the change, and mocks currently hide the real semantics.

**Suggested direction:** assert the multipart body's `keyvalues` JSON and auth header; add one "submit Ambiguous → find matches a file carrying the same keyvalues → adopt" recovery test each for v3 and legacy.

### I4 `[product]` `/psa` endpoint override silently bypasses `api` / `strategy`

**Location:** `src/pinning/pinata.rs:109-111`, `src/pinning/pinata.rs:828-830`

When the endpoint ends in `/psa`, the whole client degrades to a plain `PsaClient`, and explicit `api = "legacy"` or `strategy = "upload"` settings are **silently ignored**. An operator upgrading from an older configuration who sets `strategy = "upload"` will believe the free-tier path is active while requests still go out as PSA pin-by-CID and fail. Config validation at `src/config.rs:290-296` does not reject this contradictory combination.

**Suggested direction:** reject a `/psa` endpoint combined with non-default `api`/`strategy` during `ValidatedPinningConfig` validation, or at minimum fail at build time.

---

## 5. Minor

| ID | Finding |
|---|---|
| M1 `[product]` | `upload_endpoint` is silently ignored under `api = "legacy"` or `strategy = "cid"` (`src/pinning/pinata.rs:227-233`); only `submit_v3_upload` reads it. Validation does not reject the invalid combination, which conflicts with the README's "Pinata V3 upload endpoint overrides" wording. |
| M2 `[evidence]` | `402 \| 507 => Quota` (`src/pinning/pinata.rs:310`) diverges from the PSA baseline (507 only; other 4xx → `Terminal`). Mapping 402 to `Quota` is reasonable for Pinata free-tier limits, but it is an untested, undocumented behavior change. |
| M3 `[product]` | `list_legacy_pins` (`src/pinning/pinata.rs:412-422`) fetches a single `pageLimit=100` page. With more than 100 pins on one CID (very unlikely), `find`/`get` can miss matches. This is inconsistent with `list_legacy_jobs`, which paginates up to 10 pages and errors as `Protocol` when incomplete. |
| M4 `[evidence]` | `PinataApi::parse` / `strategy` parsing in `src/pinning/config.rs` is case-sensitive; `"V3"` is rejected. The error message is clear, so this is acceptable, but `to_ascii_lowercase` or a documentation note would help. |
| M5 `[evidence]` | Filebase regression coverage confirmed still sufficient. After `src/pinning/psa.rs:563-566` narrowed the contract tests to Filebase only, the full `mount_contract` assertion set (token leakage, request-ID validation, classification matrix) still runs against Filebase, and `filebase.rs` is unchanged. Noop/Filebase behavior is unaffected by the new fields because validation forbids them from carrying Pinata settings (`src/pinning/config.rs:294-300`, tested). No action required. |

---

## 6. Confirmed Strengths

- Tokens reach only the `Authorization` header via `bearer_auth`; they never appear in URLs, logs, or error text.
- All three base URLs pass through `parse_base_url` (scheme/host validation, no query, no fragment).
- The 1 MiB checked-add response-body cap is preserved (`src/pinning/pinata.rs:336-368`).
- Submit timeouts classify as `Ambiguous` (`src/pinning/pinata.rs:800-818`), preserving crash-recovery semantics.
- Unpin treats 404 as idempotent success (`src/pinning/pinata.rs:972`, `:984`).
- Kubo read-back uses streaming `wrap_stream` (`src/pinning/pinata.rs:540-557`); object bytes never buffer in memory.
- No new `pin_rm` calls in production code; the conservative local-pin policy holds.
- No provider HTTP occurs inside a database transaction.
- The `.env.example` comment stating `PINATA_API_KEY` / `PINATA_SECRET` are unused is accurate; a repo-wide grep matches only `.env.example`.
- The self-describing request ID encoding gives stable routing across all four `api × strategy` combinations while remaining backward compatible with bare PSA IDs.

---

## 7. Conclusion

All four `api × strategy` combinations implement submit/get/find/unpin, error classification and response-size caps inherit the established baseline, and request-ID routing is well designed. Those parts are solid.

However, the upload strategy carries one fundamental untested risk (B1: pin leakage plus unbounded re-upload when the CID-parity assumption fails) and one defect that makes it unusable for large objects (I1: the 30-second total timeout), compounded by a security-validation regression (I2) and zero coverage of the critical recovery path (I3). The change does not meet the delivery bar.

**Recommended fix order:** B1 → I1 → I2 → I3 → I4, then the Minor items as capacity allows.

---

## 8. Report Constraints

- This review and report are read-only with respect to Git; no `add`, `commit`, `checkout`, `reset`, `stash`, `push`, `tag`, or `branch` command was executed.
- All changes under review remain uncommitted in the `master` worktree.
- No live provider credentials, network calls to Pinata/Filebase, or software installation were involved.
