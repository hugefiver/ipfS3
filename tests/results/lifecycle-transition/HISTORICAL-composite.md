# Lifecycle transition F1 — historical composite receipt (superseded)

Not authoritative: review identified unproven CAR-size, process read-surface,
input identity and cleanup-status claims. Retained as historical evidence only.

UTC execution date: 2026-09-12 (local environment date 2026-09-13).

**Requested F1 matrix: PASS, assembled from the successful executions below.** This is a composite receipt, not a claim that a single invocation returned full PASS. Intermediate failures and partial-run exit codes remain intact in their own `summary.json`; no failed or skipped case is counted as success. After repairing test fixtures, only affected/remaining gates were rerun. F2 is intentionally untouched: no README, public config examples, deployment Compose or ROADMAP changes.

## Reproduction and execution semantics

```powershell
# Complete isolated run; only this mode can return 0 for full PASS.
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1

# The actual successful slices used for this receipt (see source receipts below).
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase remaining
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase regression -ResumeFromTest postgres_versioning
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase regression -ResumeFromTest multi_gateway
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase live
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase stress
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1 -Phase quality
```

Each step's exact arguments, native exit code, output and test counts are in the respective run directory. `FAIL` returns 1 (failed native commands retain their exact code in the step); a successful explicit partial phase returns **2**, never 0. An unknown resume target fails. The full runner checks every current live test name plus the required PG/regression/quality steps. Docker image acquisition and Rust installation are disabled. It creates only unique project-labelled resources and random local ports; it does not reuse a user's database. Gateway executables are copies of the freshly built production binary, not mocked HTTP handlers. The test-only TCP balancer preserves signed bytes/Host.

Standalone safety was also actually exercised:

- `cargo test --locked --offline --test lifecycle_transition`: exit 0, **0 passed / 9 ignored**. This is explicitly NOT RUN, not live success.
- With `IPFS_S3_TRANSITION_A_ENDPOINT` unset, `cargo test --locked --offline --test lifecycle_transition real_lifecycle_transition_current_plain_encrypted_copy_and_reporting_matrix -- --ignored --exact`: expected exit **101**, **0 passed / 1 failed / 8 filtered**, with `NOT RUN: IPFS_S3_TRANSITION_A_ENDPOINT is required`. Missing endpoints cannot masquerade as a pass.

## Successful gates and exact counts

`P/F/I` means passed/failed/ignored. Counts include module tests compiled into those integration binaries; they are execution counts, not a claim of globally unique names.

| Gate | P/F/I | Source run |
|---|---:|---|
| `postgres_residency` | 2/0/0 | `20260912T231722Z-b8c499da` |
| `postgres_residency_concurrency` | 6/0/0 | same |
| `postgres_lifecycle_transition` | 2/0/0 | same |
| `postgres_transition_saga` | 1/0/0 | same |
| `postgres_lifecycle` | 9/0/0 | same |
| `postgres_versioning` | 31/0/0 | `20260912T232251Z-be9bb993` |
| `postgres_cors` | 4/0/0 | same |
| `residency_publication` explicit PG case | 1/0/0, 5 filtered | same |
| `lib` | 1118/0/0 | same |
| `integration` | 164/0/0 | same |
| `residency` | 10/0/0 | same |
| `lifecycle_transition_schema` | 4/0/0 | same |
| `cors` | 8/0/0 | same |
| `residency_publication` default cases | 5/0/1 | same; ignored PG case separately executed above |
| `multi_gateway` | 28/0/0 | `20260912T232901Z-511d6197` |
| Real `lifecycle_transition` | 9/0/0 across nine exact invocations; each has 8 filtered | `20260912T233416Z-5953b938` |
| OS-process recovery parent | 1/0/0 | same; seven successful child harness exits are logged separately, not added to parent count; four intentional kill exits |
| Real 256-MiB stress rerun | 1/0/0, 8 filtered | `20260912T234023Z-e7f2b2f2` |
| fmt, all-target clippy `-D warnings`, Rust 1.92 all-target check, diff-check | all exit 0 | `20260912T234615Z-859dafa8` |
| PowerShell runner contracts | exit 0, 1/0/0 | same; rerun after final receipt bookkeeping changes |

Fresh/upgrade SQLite and PostgreSQL migrations, pre-transition legacy snapshots and schema constraints are exercised by the schema/residency/PG suites. The live gateway databases are fresh PG17 databases; isolated legacy-schema fixtures exercise upgrades. All explicitly required PostgreSQL suites received real endpoints: no “skipping” output was accepted.

## Runtime and topology

- PostgreSQL **17.11**, Kubo **0.43.0-e9914bb**, repo schema 18.
- AWS CLI **2.36.34**, `s3s` **0.14.0**, Rust SDK `rust-s3` **0.37.2**.
- Cargo **1.98.1**; MSRV check uses the already-installed **1.92.0-x86_64-pc-windows-gnu** toolchain with `--locked --offline --all-targets` and auto-install disabled. No software installed.
- Cached images only: `postgres:17`, `ipfs/kubo:v0.43.0`, `amazon/aws-cli:latest`; versions were read from the actual executables, not inferred from tags.
- Final live hot peer: `12D3KooWLngHSb4JAmjbmf6jN93JuQvrGZU8rUiTvD217kmLMyEH`.
- Final live cold peer: `12D3KooWKqM5hW4r5cAw5DyYLRwQojtUDKwgukAYQPBs8NF2XAgN`.
- Independent Docker named volumes `…_hot_data` and `…_cold_data` were inspected and asserted unequal. The full names and public peer IDs are retained in the topology receipt; database object/action/worker IDs and credentials are redacted.
- Cold runs **`daemon --offline`** throughout; it cannot fetch missing blocks from hot or any swarm peer. Hot uses **`--routing=none`** so real local CID import provider discovery terminates deterministically without public DHT routing. Distinct IDs alone were not treated as volume isolation proof.
- Two concurrent native production gateway processes share PG and the bound tiers. Process-recovery scenarios launch further independently scheduled native worker processes using the unchanged production worker. The proxy and PG trigger/lock controls exist only in test code/schema.
- AWS CLI container actually issued a signed `s3api list-buckets`; the detailed lifecycle/XML/object matrix uses real SigV4 HTTP and SDK requests through axum+s3s.

## What the live assertions proved

- Signed lifecycle XML PUT/GET round-trip and current/noncurrent STANDARD→STANDARD_IA; GET, HEAD, List v1/v2, ListVersions, public version IDs and explicit null versions.
- Plain/SSE-S3/SSE-C full reads, Range response bytes and headers, wrong SSE-C key rejection, metadata and tagging. Stored ciphertext bytes match before/after migration; CID, ETag and selected public version remain unchanged. Cold→hot COPY produces a new STANDARD object. Direct IA writes are rejected.
- The nondefault imported CID is CIDv1 with 64-KiB chunks/raw leaves, submitted and status-polled through the real signed import route. Cold retains the same root/layout. Empty, multi-block, cross-256-KiB Range and multipart-completed root cases verify bytes, pin and full-local-DAG facts.
- Real hot container stop: IA GET succeeds from cold. Real cold container stop, with hot restarted and holding the bytes: IA GET fails without fallback. The backend-stop fixture is then cleaned through the signed API.
- Process kill checkpoints: durable `prepare` after external import, `copy` during local verification, `verify` before publication transaction, and committed `publish` before cleanup. Every killed child is reaped; restart advances epochs **1→2→3→4→5**. Cleanup finishes without reversing IA or changing immutable content.
- An old still-alive worker is released after takeover: its epoch is fenced and cannot overwrite the new owner. Deleting policy while held at verification cancels stale publication.
- The copy I/O response is held **31 seconds** with a **2-second lease**; the real heartbeat advances the deadline. Failed copy recovers after restart. Eight actual failing attempts exhaust safely with no publication and STANDARD retained. Shutdown is requested through the test control table and the actual worker handle drains before child exit.
- The process proxy audits **zero `/api/v0/pin/rm` requests**. No runner operation invokes unpin, GC or block deletion. Shared-CID/lease/ownership/publication regressions remain covered by PG and signed integration suites.
- Additional actual **256-MiB** object stress: cold full DAG and byte fidelity succeed. OS peak working sets of the two production gateways are **34,435,072** and **35,708,928 bytes**, both below the enforced **192-MiB per-process budget**, while the object exceeds that budget. This measures gateway resident memory only, not Rust test-client or Kubo memory. The separate 31-second hold proves renewal; it is not mislabeled as a 31-second continuously flowing upload.

## Failures repaired without production changes

1. PowerShell's browser-like default User-Agent caused real Kubo RPC 403; the validator now sends `ipfs3-f1-validator`. Container readiness and HTTP success were distinguished.
2. Windows locks running executables: native gateways now run unique copied images so Cargo can relink the normal binary target.
3. Process fixture converted terminal NULL lease to PostgreSQL `-infinity`, which overflowed Chrono. It now uses `Option<DateTime<Utc>>`; panic cleanup and redacted recovery events were strengthened.
4. Signed import fixture expected `pending`; the established public accepted state is `queued`. Its assertion was corrected to the actual existing protocol.
5. Offline hot prevented the import route's existing provider-discovery phase. Public-DHT hot was then nondeterministic (>30 seconds). Final test topology uses online/null-routing hot and offline cold; no timeout was increased and cold isolation was not weakened.
6. Historical versioning-down test tried to drop its table beneath new residency foreign keys. It now creates the pre-versioning fixture and applies only the migration being reversed. Current-schema downgrade guarantees and production migration SQL are unchanged.
7. New fixture formatting was corrected to match rustfmt. No failing regression was deleted or bypassed.

The pre-existing C/D/E1/E2/E3 implementation was preserved. No product bug requiring production changes was discovered in this execution.

## Files delivered

- New `tests/lifecycle_transition.rs`, `tests/lifecycle_transition_process.rs`, `tests/lifecycle_transition_balancer.rs`.
- New `tests/support/lifecycle_transition_real.rs`, `lifecycle_transition_real_http.rs`, `lifecycle_transition_real_sigv4.rs`, `lifecycle_transition_process.rs`.
- New `tests/run-lifecycle-transition-validation.ps1`, `tests/lifecycle-transition.Tests.ps1`, `tests/compose.lifecycle-transition-validation.yml`.
- Modified only the historical successful-down fixture in `tests/postgres_versioning.rs`; pre-existing changes in that file are preserved.
- This receipt and the run-specific redacted evidence under `tests/results/lifecycle-transition/`.

## Cleanup and residual boundaries

Every topology run captures container logs before `down --volumes`; gateway pipes are drained into redacted evidence and owned process trees are stopped. The uniquely copied executable is removed. Each run confirms its project-labelled container, volume and network inventories are empty. A final name-scoped Docker inventory and process inventory also found no F1-owned resources remaining. Existing resources were never deleted. Environment overrides are restored in `finally`.

Earlier receipts were re-redacted with the final redactor (including ANSI-encoded logs and fixed test-credential arguments); statuses/counts were not rewritten. The one-time sanitization script was removed. There are no debug hooks or transient debug scripts left.

Residual scope: this is the requested PG17/Kubo43/Windows-native F1 environment, not a cross-platform certification or a Kubo/client memory benchmark. It does not add archive/restore, remote cold providers, direct IA writes, reverse lifecycle or physical hot-space reclamation. The existing dependency `proc-macro-error2 v2.0.1` emits a future-incompatibility notice; current clippy and MSRV checks pass. No Git writes, image pulls, software installation, or F2/public documentation changes were performed.
