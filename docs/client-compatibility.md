# S3 Client Compatibility

## Endpoint and authentication contract

- Path-style endpoint, SigV4 service `s3`, region `us-east-1`.
- Credentials in development examples are `test` / `test`.
- Host path: `http://127.0.0.1:9000` (used by both Mc and AWS same-client dual-endpoint HEAD/stat verification).
- Compose-network path: `http://gateway:9000` (the only endpoint exercised by rclone).
- ETag is the IPFS CID, not an MD5 digest. Options that require an MD5 ETag are unsupported.

## Recommended rclone configuration

```ini
[ipfs-s3]
type = s3
provider = Other
endpoint = http://gateway:9000
access_key_id = test
secret_access_key = test
region = us-east-1
force_path_style = true
list_version = 2
use_server_modtime = true
```

`list_version = 2` is the recommended rclone path, but the gateway also implements ListObjects v1 for SDK and legacy-client compatibility. `use_server_modtime = true` uses S3 `LastModified` rather than treating the CID ETag as MD5 metadata.

## Result definitions

- `PASSED`: the listed commands and assertions actually executed successfully.
- `FAILED`: prerequisites were available and execution started, but a command or assertion failed.
- `SKIPPED`: execution did not run because an image/tool/authorization was absent.

Rust integration results and Docker-client results are separate evidence. A compiled script or passing Rust suite does not make a client row `PASSED`.

An actually executed client with `FAILED` blocks its ROADMAP checkbox and the final v0.2 commit. A `SKIPPED` row may support checking the ROADMAP item only as “smoke artifact implemented but not executed,” and the row must remain `SKIPPED` with its authorization/image reason.

## Compatibility matrix — evidence snapshot 2026-08-13

The final transcript SHA-256 is `5a914a22fc136ba64ce0c204c629af6fa4bfe811b06b51a401c12c713617bfce`. It is the portable, deterministic derivation of the retained one-run output; its provenance receipt records the raw source SHA-256 `2ee5df03e308dfb4bb4a96b6da86a19ef2fcac9639ef4633b5353766ba791d3c`.

| Client | Version | Transport | Endpoint path | Auth/region | Operations | Recommended options | Result | Evidence date | Evidence command or log | Known limitation |
|---|---|---|---|---|---|---|---|---|---|---|
| rclone | 1.74.4; image `sha256:c61954aaa32328a5486715dd063a81c7879f5195ad3505cd362deddd509dc4a1` | Docker | Compose network: `http://gateway:9000` | SigV4, us-east-1 | mkdir, copy, ls, cat, deletefile, rmdir | `list_version = 2`, `use_server_modtime = true` | PASSED | 2026-08-13 | `docs/client-smoke-evidence-2026-08-13.log`: `[RESULT] client=Rclone status=PASSED dual_head=NOT_RUN` | Compose-network-only; no localhost probe, nested signed HEAD, or cross-client verifier is claimed. CID ETag is not MD5. |
| MinIO mc | RELEASE.2025-08-13T08-35-41Z; image `sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` | Docker | localhost + Compose network | S3v4, us-east-1 | temporary alias config, alias list, mb, cp, ls, cat, stat, rm, rb | path-style alias | PASSED | 2026-08-13 | `docs/client-smoke-evidence-2026-08-13.log`: `[RESULT] client=Mc status=PASSED dual_head=PASSED` and `[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED` | This mc release rejects the short `test` secret in `alias set`; the smoke writes the equivalent test-only temporary alias config before executing signed operations. |
| AWS CLI | 2.36.9; image `sha256:00dee8ccaf669721f8163944ea9e6ac13183851b23d5b7a9868a608bc932cfdb` | Docker | localhost + Compose network | SigV4, us-east-1 | mb, cp, ls, get-bucket-location, list-objects v1, head-object, delete-objects, rm, rb | path-style endpoint URL | PASSED | 2026-08-13 | `docs/client-smoke-evidence-2026-08-13.log`: `[RESULT] client=Aws status=PASSED dual_head=PASSED` and `[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED` | CID ETag is not MD5. |
| Rust integration harness | reqwest SigV4 helper + rust-s3 0.37.2 | real TCP to in-process axum+s3s | localhost listener | SigV4, us-east-1 | reqwest SigV4 GetBucketLocation/restXml, ListObjects v1/v2 URL-encoding wire projection and raw pagination, DeleteObjects, nested HEAD; rust-s3 v2 list and object/SSE/multipart/ZIP regressions | path style for rust-s3 coverage | PASSED | 2026-07-19 | `cargo test --test integration` | Wiremock Kubo; rust-s3 0.37.2 `location()` encodes `?location` as an object path and is not Task 4 protocol evidence. |

## Re-running and updating evidence

Run the two dependency-free static tests before any Docker action. Refuse the run if either fixed container name exists in any state, then preflight a new unique lowercase `COMPOSE_PROJECT_NAME` for containers, networks, and volumes carrying that exact project label. Inspect all six required local images without pulling or installing anything.

An external ownership envelope, not `scripts/client-smoke.ps1`, owns diagnostics and cleanup. It records whether `COMPOSE_PROJECT_NAME` existed and its exact value, sets one unique value for every Compose operation, and restores the original presence and value in `finally`. Capture the runner and envelope output with `[IO.StreamWriter]` and `[Text.UTF8Encoding]::new($false)`; do not use `>` or `*>`, whose encoding is implicit.

Accept the transcript only after exact parsing finds one ordered `PASSED` result for Rclone, Mc, and Aws, with `dual_head=NOT_RUN` for Rclone and same-client `dual_head=PASSED` evidence for Mc and Aws. After an owned run, collect non-coloured project logs, then run `docker compose --project-name <claimed-project> -f docker-compose.yml down --volumes --remove-orphans` and verify that the claimed project's containers, networks, and volumes, plus the fixed containers, are absent.

Any `FAILED`, `SKIPPED`, nonzero exit, diagnostics or cleanup failure, malformed receipt, failed restoration, or residual resource blocks acceptance. Retain a failed transcript unchanged and do not update the matrix from it. Do not install a host AWS CLI or mc, pull images, or translate an unexecuted client into compatibility success.
