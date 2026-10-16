# Import documentation requirements

Public documentation describes the [durable import contract](2026-07-29-ipfs3-import-design.md), not implementation checkboxes or an inferred release assignment. A documentation update doesn't create a release or change package metadata.

## Reader-facing contract

The import guide covers signed `POST /{bucket}/{key}?ipfs3-import` with exactly one `CID` or `URL`, `202 Accepted`, `Location` and `x-ipfs3-import-job-id`, and signed status lookup through `GET /{bucket}/{key}?ipfs3-import={job-id}`. It explains `x-ipfs3-client-token` idempotency and optional `decompress-zip=<prefix>` composition.

Recovery is durable job retry/reclaim, not byte-range URL resume. Ordinary reads and listings expose only committed objects. Submission validation produces synchronous S3 errors; runtime failures appear in terminal status XML. Stale workers can't publish over a newer owner.

URL sources require exact configured HTTPS origins and public DNS addresses. Redirects, private-network sources, forwarded authentication and HTTP aren't supported. CID imports are enabled by default; URL imports require `[imports].allowed_https_origins`. SSE-S3 and SSE-C submission headers are rejected.

The architecture description distinguishes `GatewayRoute`, `ImportObjectRoute`, `DecompressZipRoute` and the separate import coordinator/worker. It doesn't claim the coordinator is held in `AppState`.

## Evidence and consistency

Client compatibility statements cite actual dated receipts. Local execution, hosted execution and unexecuted checks remain distinct. A roadmap completion or numbered release claim needs its own supporting evidence; documentation edits alone don't establish either.

README and ROADMAP use the same milestone for chunk-level encrypted Range support. Review examples against the public query/header contract and configuration schema rather than freezing prose or whole-file bytes.
