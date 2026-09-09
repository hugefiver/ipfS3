# AWS CLI lifecycle-abort-multipart parity — 2026-09-10

- **Status**: PASS (single local run, orchestrator-executed)
- **Client**: AWS CLI v2 via local `amazon/aws-cli:latest` image (`docker run --pull=never`), dedicated test credentials, region `us-east-1`, path-style endpoint
- **Gateway**: local debug build from the current working tree (contains the lifecycle-abort-multipart changes), SQLite file database, `lifecycle.poll_interval_ms = 500`
- **Kubo**: local `ipfs/kubo:v0.43.0` container, torn down after the run
- **Scope**: spec §9 "AWS CLI lifecycle configuration evidence" — this closes the README capability-claim gate that the PG17 runner run (RUN-2026-09-09) left uncovered

## Verified sequence

1. `create-bucket` — OK
2. `put-bucket-lifecycle-configuration` (JSON: one rule, prefix `logs/`, `AbortIncompleteMultipartUpload.DaysAfterInitiation = 1`) — OK; canonical JSON persisted at revision 2 with the expected modern-prefix selector
3. `get-bucket-lifecycle-configuration` — round-trips the rule verbatim (ID, prefix filter, status, days)
4. `create-multipart-upload` (`logs/parity-object.bin`) + `upload-part` (part 1, 2 MiB) — OK against real Kubo
5. Upload initiation time aged to −3 days via direct SQL, then the production lifecycle worker scanned, persisted the durable abort action, and executed it: `multipart_uploads` row removed, `lifecycle_actions` terminal state `succeeded`
6. `list-parts` after lifecycle abort — `NoSuchUpload` (no resurrection)
7. `abort-multipart-upload` after lifecycle abort — `NoSuchUpload` (client retry stays distinguishable from the durable action)
8. `delete-bucket-lifecycle` — OK
9. `delete-bucket` — OK (bucket empty after abort)

## Methodology note

A first aging attempt used SQLite `datetime('now','-3 days')` (space-separated), which does not match the sea-orm `DateTimeUtc` binding format (`...+00:00`); the exact four-field conditional DELETE correctly classified it as `Stale` and the action terminalized `cancelled/cancelled_stale`. This was test-tooling error, not a product defect; production rows are written by sea-orm on both sides and are format-consistent. After re-aging with the binding-consistent format (and replaying the scan), the full lifecycle executed `succeeded`. The fail-closed behavior on mismatched initiation identity is itself spec-conformant.

## Boundary

Local environment only; no hosted execution. Evidence retained in temp run directory (not committed): raw CLI transcript. This file is the sanitized receipt.
