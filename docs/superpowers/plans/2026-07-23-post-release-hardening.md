# Kubo transport, ZIP budgets and worker diagnostics

## Required behavior

Kubo transport failures must remain distinguishable inside the gateway without exposing backend bodies, credentials or internal URLs to S3 clients. ZIP extraction must enforce an archive-wide decompressed-byte budget, including bytes drained after an entry upload fails. Pinning worker failures must retain the identity of the actual job.

These requirements preserve CID-as-ETag, conservative local pin retention, transaction boundaries and SSE-C multipart claim ordering. Kubo and provider network calls don't run inside database transactions.

## Transport and error boundary

`KuboClient` separates short control operations, streaming uploads and streaming downloads:

| Client | Timeout policy |
|---|---|
| Control | 30-second connect timeout; 300-second whole-request timeout |
| Upload | 30-second connect timeout; no whole-request or read timeout |
| Download | 30-second connect timeout; 120-second inter-chunk read timeout; no whole-request timeout |

An upload can finish sending long before Kubo returns response headers. A download idle limit applies between chunks, not to the complete object duration.

Non-2xx add, cat and pin responses discard backend bodies without reading or logging them. `AppError::KuboRpc` retains typed internal provenance; its display is fixed and the S3 conversion returns `InternalError` with `internal storage backend error`. Successful add responses are parsed only for their result.

Late cat failures retain private, downcastable backend provenance through the error source chain. ZIP readers and crypto chunkers preserve that classification without matching error text. ZIP parsing, directory drains, entry copies and completion must not misclassify a backend outage as a malformed archive or an entry-local partial failure. Errors after an HTTP response begins remain safe stream failures.

## ZIP budget and publication

The archive-wide decompressed-byte default is 8 GiB. Every decompressed byte counts, including the remainder drained after an early entry-upload failure. Crossing the budget rejects the archive globally with `InvalidParameterValue` and HTTP 400 before publication. A failed add or copy closes its duplex writer so the other side can't hang. Staged local pins remain retained; rejection never calls `pin_rm`.

## Worker correlation

The worker associates each `JoinSet` task ID with `JobLogContext`. Main-loop joins, tick-time joins and shutdown drains remove that context on every outcome. Join and execution failures identify the actual `job_id`, `provider`, `cid`, `lease_id` and `target_id`, without provider response bodies, tokens or encryption material. Completed task context must never be reused for a later failure.

## Acceptance

- Real local HTTP fixtures prove slow successful uploads, connection failure, a download that stalls after its first chunk, and safe late-stream error propagation.
- ZIP fixtures prove cumulative budget rejection when individual entries fit, post-failure draining, bounded early-failure completion, no database publication and no unpin.
- Actual worker execution proves panic and store-failure correlation across normal, tick and shutdown joins, context removal after success, and secret redaction.
- Parallel fixtures use distinct identities without weakening lock-order or canonical-target assertions. Passing an isolated test doesn't establish parallel-suite stability.

Current repository-wide verification instructions are in [Testing](../../testing.md). Commands and acceptance requirements aren't execution receipts.
