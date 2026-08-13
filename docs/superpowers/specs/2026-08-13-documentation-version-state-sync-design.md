# Documentation and Version State Sync Design

**Date:** 2026-08-13
**Status:** Approved design
**Scope:** Synchronize public documentation and roadmap state with the durable import implementation and current client evidence without inventing a release version.

## Problem

The repository has shipped durable `ipfs3-import` implementation and verification work, but the public README does not describe the feature. The roadmap still reports the AWS CLI smoke as unexecuted and contains conflicting encrypted Range milestone text. At the same time, the package remains `0.1.0`, there are no Git tags, and no release decision assigns durable import to a numbered milestone.

Historical implementation-plan checkboxes are execution records, not a retrospective feature inventory. Marking them complete now would claim command and checkpoint evidence that was not recorded when the plan ran.

## Goals

1. Document the implemented SigV4 `ipfs3-import` submission and status-query contract.
2. Explain CID and allowlisted HTTPS sources, optional idempotency, durable job recovery, optional ZIP decompression, and stale-publication fencing without overstating URL resume or encryption support.
3. Update the architecture description to include the composite gateway route and import coordinator/worker.
4. Make the roadmap agree with the accepted 2026-08-13 AWS CLI evidence.
5. Record durable import as delivered while leaving its release assignment explicitly pending.
6. Make the encrypted Range roadmap reference internally consistent at v0.8.

## Non-goals

- Do not change `Cargo.toml` or `Cargo.lock` from package version `0.1.0`.
- Do not create a tag, release, changelog entry, release note, or publication claim.
- Do not assign durable import to v0.4, v0.5, or another numbered release.
- Do not mark historical steps in `docs/superpowers/plans/2026-07-29-ipfs3-import.md` complete.
- Do not change production code, configuration defaults, tests, workflows, Compose files, client evidence, or v0.5+ implementation status.

## Approaches Considered

### 1. Factual state synchronization without a version bump — selected

Update README and ROADMAP statements that can be proven from current code and accepted evidence. Keep package metadata and historical plans unchanged. This removes user-facing drift without manufacturing release history.

### 2. Promote the package to a roadmap version

Changing `0.1.0` to `0.4.x` or `0.5.x` would imply a release policy and milestone assignment unsupported by tags, release commits, or user direction. This is rejected.

### 3. Retrospectively complete the import implementation plan

Checking every historical task would conflate current implementation state with contemporaneous RED/GREEN, live PostgreSQL, and checkpoint evidence. This is rejected; the roadmap and README are the current-state surfaces.

## README Design

Add durable import to the feature list and a focused usage section after the standard PutObject IPFS response headers.

The section will show:

- a signed S3-extension request shape using `POST /{bucket}/{key}?ipfs3-import` with an XML body containing exactly one `CID` or `URL`;
- the `202 Accepted`, `Location`, and `x-ipfs3-import-job-id` response contract;
- status lookup through `GET /{bucket}/{key}?ipfs3-import={job-id}`;
- optional `x-ipfs3-client-token` idempotency and `decompress-zip=<prefix>` composition;
- durable lease-based recovery as job retry/reclaim, explicitly not byte-range URL resume;
- visibility and stale-publication guarantees;
- URL restrictions: exact configured HTTPS origins, public DNS addresses, no redirects, private-network sources, forwarded authentication, or HTTP;
- import encryption restriction: SSE-S3 and SSE-C submission headers are rejected.

The configuration section will point to `[imports]` in `config.example.toml` and explain that CID imports are enabled by default while URL imports require `allowed_https_origins`.

The architecture diagram and `AppState` paragraph will represent `GatewayRoute`, `ImportObjectRoute`, `DecompressZipRoute`, and the separate import coordinator/worker without claiming the coordinator is stored in `AppState`.

## ROADMAP Design

1. Replace the stale AWS CLI v0.2 row with the accepted 2026-08-13 execution result and evidence path.
2. Add an unnumbered section immediately after current v0.4 named `Delivered — Release Assignment Pending`.
3. List only implemented durable import capabilities: CID/allowlisted HTTPS submission, persisted status/progress and recovery, idempotent replay, optional atomic ZIP publication, and stale-worker/content-mutation fencing.
4. State that package version remains `0.1.0` and no numbered release assignment is implied.
5. Keep every v0.5+ checkbox unchanged.
6. Change the v0.8 encrypted Range item from the contradictory “v0.9 optimization” wording to a plain v0.8 item, and make README use the same milestone.

## Data and Error Flow Documentation

The documented flow is:

```text
signed POST
  -> GatewayRoute / ImportObjectRoute
  -> validate source and persist queued job
  -> 202 + job ID
  -> durable worker performs CID pin or HTTPS download/add
  -> optional ZIP extraction
  -> ownership-fenced atomic publication
  -> signed GET returns progress or terminal result
```

Submission validation errors remain synchronous S3 errors. Runtime failures are represented by terminal job state in status XML. Ordinary S3 reads and listings expose only committed objects.

## Verification

- Assert README contains the exact submit/status query shapes, response headers, idempotency header, recovery boundary, URL restrictions, encryption restriction, and `[imports]` reference.
- Assert ROADMAP references the 2026-08-13 AWS evidence, contains the unversioned delivered section, keeps all four v0.5 items unchecked, and has no stale AWS `SKIPPED` statement.
- Assert README and ROADMAP both place chunk-level encrypted Range in v0.8.
- Assert `Cargo.toml`, `Cargo.lock`, and the historical import plan are byte-for-byte unchanged.
- Run `git diff --check` and verify the changed-path allowlist.

## Git Boundary

This task may be committed after final review under the user's standing instruction to commit each completed roadmap task. It does not authorize push, tag, release creation, or any other publication action.
