# S3 Client Compatibility

## Endpoint and authentication contract

- Path-style endpoint, SigV4 service `s3`, region `us-east-1`.
- Credentials in development examples are `test` / `test`.
- Host path: `http://127.0.0.1:9000` (used by the Mc same-client dual-endpoint stat).
- Compose-network path: `http://gateway:9000` (the only endpoint exercised by Rclone).
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

## Compatibility matrix — evidence snapshot 2026-07-19

| Client | Version | Transport | Endpoint path | Auth/region | Operations | Recommended options | Result | Evidence date | Evidence command or log | Known limitation |
|---|---|---|---|---|---|---|---|---|---|---|
| rclone | 1.74.4; image `sha256:c61954aaa32328a5486715dd063a81c7879f5195ad3505cd362deddd509dc4a1` | Docker | Compose network: `http://gateway:9000` | SigV4, us-east-1 | mkdir, copy, ls, cat, deletefile, rmdir | `list_version = 2`, `use_server_modtime = true` | PASSED | 2026-07-19 | `docs/client-smoke-evidence-2026-07-19.log`: `[RESULT] client=Rclone status=PASSED dual_head=NOT_RUN` | No localhost probe, nested signed HEAD, or cross-client verifier is claimed; CID ETag is not MD5. |
| MinIO mc | RELEASE.2025-08-13T08-35-41Z; image `sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` | Docker | localhost + Compose network | S3v4, us-east-1 | temporary alias config, alias list, mb, cp, ls, cat, stat, rm, rb | path-style alias | PASSED | 2026-07-19 | `docs/client-smoke-evidence-2026-07-19.log`: `[RESULT] client=Mc status=PASSED dual_head=PASSED` and `[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED` | This mc release rejects the short `test` secret in `alias set`; the smoke writes the equivalent test-only temporary alias config before executing signed operations. |
| AWS CLI | `amazon/aws-cli:latest` absent | Docker | localhost + Compose network | SigV4, us-east-1 | mb, cp, ls, get-bucket-location, list-objects v1, head-object, delete-objects, rm, rb | path-style endpoint URL | SKIPPED | 2026-07-19 | `docs/client-smoke-evidence-2026-07-19.log`: `[RESULT] client=Aws status=SKIPPED dual_head=NOT_RUN detail=local image missing: amazon/aws-cli:latest` | Exact local image is absent; the script reports the required pull command but never executes it. |
| Rust integration harness | reqwest SigV4 helper + rust-s3 0.37.2 | real TCP to in-process axum+s3s | localhost listener | SigV4, us-east-1 | reqwest SigV4 GetBucketLocation/restXml, ListObjects v1/v2 URL-encoding wire projection and raw pagination, DeleteObjects, nested HEAD; rust-s3 v2 list and object/SSE/multipart/ZIP regressions | path style for rust-s3 coverage | PASSED | 2026-07-19 | `cargo test --test integration` | Wiremock Kubo; rust-s3 0.37.2 `location()` encodes `?location` as an object path and is not Task 4 protocol evidence. |

## Re-running and updating evidence

1. Run preflight without side effects: `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All`.
2. After explicit build/start authorization, capture the real runner's outer stdout/stderr unchanged as UTF-8: `$evidenceLog = "docs/client-smoke-evidence-2026-07-19.log"; & pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run *> $evidenceLog; $smokeExit = $LASTEXITCODE; if ($smokeExit -ne 0) { throw "smoke exited $smokeExit" }`.
3. Before changing ROADMAP or committing, update every actually executed client row from that tracked transcript, including the exact `PASSED` or `FAILED` result; do not retain a stale `SKIPPED` row for a client that ran. The expected current receipt is exactly Rclone `PASSED/NOT_RUN`, Mc `PASSED/PASSED`, Aws `SKIPPED/NOT_RUN`, with only `client=Mc verifier=Mc` runtime EVIDENCE.
4. Stop on any `FAILED`; leave that client's ROADMAP checkbox open and do not create the final commit.
5. Keep volumes unless cleanup is explicitly selected with `-CleanupVolumes`.

Do not install a host AWS CLI or mc for this matrix, do not pull images without approval, and do not translate `SKIPPED` into compatibility success.

When the standard final-stage image is unavailable but the local gateway and `rust:latest` images exist, the runner can still rebuild the current gateway source without network access: it vendors dependencies from the existing host Cargo cache with `--offline`, builds with Docker `--network none --pull=false`, and starts Compose with `--pull never --no-build`. A missing local dependency remains a real failure; the runner never downloads it.
