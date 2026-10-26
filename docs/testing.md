# Testing

## Default checks

The repository-wide command compiles every target. The multi-service topology tests are explicitly ignored by default, but the PostgreSQL targets still require a dedicated test database (including the startup and JSON compatibility tests in `postgres_import`). Configure `IPFS_S3_TEST_POSTGRES_URL` before running this command; it is not a database-free local check:

```powershell
cargo test --all-targets
```

For a narrower local iteration, run the checks that match the area being changed:

```powershell
cargo test --lib
cargo test --test integration
cargo test --test http_admission
cargo test --test object_read_snapshot
cargo test --test bucket_config_owner_atomicity
```

The reusable tests under `tests/support/` are registered only by the `integration` target. Other integration targets compile only the helpers they use, so the same support tests are not executed four times. The external tests in `e2e`, `multi_gateway`, and `cluster` are marked `ignored` with their required topology; their pure unit tests remain part of the default run.

The PowerShell behavior checks are individually runnable:

```powershell
Get-ChildItem tests/*.Tests.ps1 | ForEach-Object { pwsh -NoProfile -File $_.FullName }
```

These checks exercise runner no-op modes, native argument isolation, bounded process-tree termination, concurrent temporary-root ownership, owned-only cleanup, diagnostic redaction, cleanup inventory failure handling, input identity, and the private-swarm entrypoint. They do not substitute source-string assertions for the Rust and deployment behavior tests.

The native runner isolation fixture is also directly runnable and is part of CI on PowerShell-capable Ubuntu runners:

```powershell
pwsh -NoProfile -File tests/native-runner.Tests.ps1
```

## Stage 3 pin-control checks

Run these focused tests against the new optional-control and read-only CLI
surfaces. Check the test counts and failures, not just the exit code:

```powershell
cargo test --lib pinning::
cargo test --lib s3::
cargo test --lib import::
cargo test --test integration
cargo test --test cors
cargo test --test multipart_pin_decision
cargo test --test diagnostics
cargo test --lib store::migrations::stage3_tests
cargo test --test stage3_restart
cargo test --test stage3_route_fence
cargo test --test import_legacy_replay
cargo test --lib store::pinning::publication
```

These exercise captured decisions, warn/strict responses, multipart reuse,
tag/copy behavior, import paths, CORS and local diagnostic read safety. The
diagnostic commands themselves can be run as `cargo run -- --pinning-doctor`,
`cargo run -- --config-explain`, and
`cargo run -- --pinning-policy-explain my-bucket file.txt`; status requires an
existing configured DB and must not create one. A local mock or SQLite pass
doesn't establish PostgreSQL migration/concurrency, real SDK compatibility,
remote account permissions or remote pin success. Record those separately when
their environment and authorization are available; don't count them as green
based on the commands above.

For isolated PostgreSQL 17 coverage, run `postgres_pinning_stage3` and
`postgres_stage3_route_fence` against the configured test database; these are
distinct from SQLite checks. Recorded focused results are 5 schema tests and
2 route-interleaving tests passing. The new Noop path has not been rerun on PG.

```powershell
cargo test --test postgres_pinning_stage3 -- --include-ignored --test-threads=1
cargo test --lib store::migrations::stage3_tests::pg17_stage2_upgrade_retains_raw_tags_without_backfilling_decisions -- --ignored --exact
cargo test --test postgres_stage3_route_fence -- --include-ignored --test-threads=1
```

A real two-process SigV4 MPU completion test passed 1/1 with the same config;
changing both credential and endpoint revisions was rejected before side effects.
The final Stage 3 selected-target run passed: 1443 test executions, 1 ignored,
including 1250 library and 165 integration tests. This is not a real-provider
account validation. The intermittent Stage 2 PG historical-route error still
has no confirmed root cause.

Test cleanup may remove static source, hash, documentation-checkbox, and historical TDD-shape assertions. It must retain executable regressions for command argument boundaries, process ownership, path confinement, cleanup scope, redaction, concurrency, and other observable security behavior.

## Stage 4 ZIP verification

Stage 4 passed its selected integration run (1852 executions, 4 ignored),
followed by focused regressions closing initial-root cancellation, MPU
root-recovery capture, and source-residency lock-order findings. The affected
targets passed 198 executions with 1 ignored; counts include repeated support
self-tests, not 198 independent new scenarios. The isolated acceptance evidence
includes 15 distinct PostgreSQL cases, 4 real-Kubo directory tests and the
explicit R23 load test. Real Filebase/Pinata account writes remain NOT RUN.
Don't count ignored tests as passing. The ZIP checks must cover legacy archive ETag/VersionId and
result compatibility; default-on and signed-tag root selection; final relative
paths and exact committed versions; disabled, empty, partial and failed roots;
separate batch/root ownership and GET status; per-output private denial; and
authenticated v2 controls, token replay, source=false, server-measured URL input,
and zero-output MPU completion/replay. In particular, source=false zero-output Complete must
persist a queryable failed batch and return the same 4xx on the first call and
replay, not a successful empty result. Run against the completed implementation,
not an unfinished worker.

The four real-Kubo directory tests in `zip_directory` are explicitly ignored.
They need isolated Kubo RPC endpoints via both
`IPFS3_DIRECTORY_TEST_KUBO_URL` and
`IPFS3_DIRECTORY_LARGE_TEST_KUBO_URL`, plus explicit `--ignored`. The 10,000-path
HAMT test is expensive. Set the environment variables to authorized disposable
nodes before running; no real node is contacted by the default test command.

```powershell
cargo test --test zip_directory real_kubo_ -- --ignored --nocapture --test-threads=1
```

R23's 65/30-entry multi-provider real-worker test is also ignored and must be
selected by its exact name. It doesn't run with default `cargo test`:

```powershell
cargo test --test zip_v2_pinning_load signed_zip_batches_share_quota_but_not_logical_leases_and_worker_remains_fair -- --ignored --exact --nocapture --test-threads=1
```

The PostgreSQL ZIP batch and v2 execution targets require a dedicated,
authorized test database through `IPFS_S3_TEST_POSTGRES_URL`. Don't use a
production database. These commands only select tests; they don't establish a
PASS until run against the finished tree and their results are inspected.

```powershell
cargo test --test postgres_zip_batch_store -- --ignored --test-threads=1
cargo test --test zip_v2_execution -- --ignored --test-threads=1
cargo test --test postgres_zip_v2_publication -- --ignored --test-threads=1
cargo test --test postgres_zip_v2_residency_lock_order -- --ignored --test-threads=1
cargo test --test zip_v2_mpu_complete -- --ignored --test-threads=1
cargo test --test import_zip_v2_acceptance -- --ignored --test-threads=1
```

## Stage 9 R25 ZIP verification

**Status: local isolated R25 acceptance passed**, committed as `55e06fa`.
Coverage preserves v1 ETag/CID, VersionId, legacy result bytes and v2
`ZipV2Result`, exact successful output owners, manual archive-owned cancellation,
non-ZIP behavior, frozen configuration, recovery provenance, complete input proof
and quota fences.

| R25 acceptance check | Actual result |
|---|---|
| `fmt --all -- --check` | exit 0 |
| all-targets `check` and Clippy `-D warnings` | both exit 0 |
| complete default-concurrency lib suite | 1494 passed, 0 failed, 1 ignored |
| complete integration suite | 165 passed, 0 failed |
| Stage2 migration target | SQLite 2 passed; PG 2 ignored in this command |
| 18 complete affected protocol/store/lifecycle/migration binaries | all 0 failed, 0 filtered; one original PG case ignored |
| current-source isolated PostgreSQL 17.11 | five exact cases passed, 0 failed |

The affected binaries include real production SigV4 HTTP (16), replay (13),
unchanged literal wire goldens (5), direct capture/route fences (11), control
review (16), recovery (49), complete MPU and R26 store/CLI/surface targets. Their
shared support tests are not accumulated as unique scenarios. Compile-only and
ignored cases are not counted as runtime passes.

Unreproduced file-close and timing symptoms aren't claimed fixed by passing
current suites. Stage 8 remains incomplete; real paid-provider accounts and
production GC remain **NOT RUN**. Local Noop/mock durable jobs don't prove physical
pin completion. Stage 7 acceptance is recorded separately below.

The current local targets are `tests/stage9_zip_*.rs` (support directories aren't
test targets). With Rust 1.92 or later and cached dependencies, use:

```powershell
cargo test --locked --offline --test 'stage9_zip_*' -- --test-threads=1
cargo test --locked --offline --lib zip::options:: -- --test-threads=1
```

The glob includes capture/config, direct capture, HTTP, replay, wire, store,
lifecycle and the four import targets, plus the ignored PostgreSQL target. An
ignored PostgreSQL case isn't a PASS. Check individual results; a command listed
here isn't evidence that all its targets passed.

The current-source native PG run executes four existing
`legacy_pinning_migration_postgres` cases (NOWAIT/phantom and receipt failure,
historical identity/new lifetime, damaged receipt catalog, committed route
drift), plus
`stage9_zip_postgres::r25_pg_tail_rollback_then_exact_shared_cid_publication`.
All five ran with their original 8-second SQL deadline and passed on isolated
PostgreSQL 17.11. The tail case proves zero publication
and quota writes after the injected fault, then same-capture retry with three
versions, two leases and shared-CID usage of 5 bytes/1 pin.

Non-NULL ZIP capture pointer/identity closure and seeded-history no-fill/down
tests have SQLite native evidence and use the shared Rust TEXT/JSON scanner;
these five PG cases do **not** prove those individual fixtures on PostgreSQL.
No independent PG MPU/root-recovery or physical-provider assertion is implied.

Run that case only with an explicitly authorized disposable PostgreSQL 17 test
database already configured through `IPFS_S3_TEST_POSTGRES_URL`:

```powershell
cargo test --locked --offline --test stage9_zip_postgres r25_pg_tail_rollback_then_exact_shared_cid_publication -- --ignored --exact --nocapture --test-threads=1
```

The case owns a fresh UUID schema and removes only that exact schema.
Don't substitute a production database, a paid account, unpin/GC operations or
an unowned test instance for this fixture.

## Stage 5 RPC verification

**Status: Stage 5 implementation and isolated-backend acceptance passed.**
The provider requirements in
[`2026-09-20-pinning-zip-unixfs.md`](superpowers/plans/2026-09-20-pinning-zip-unixfs.md)
define the contract. The commands below are entry points, not results.
Don't count an ignored test, compilation-only run or mock response as live
provider evidence. Real Filebase/Pinata account tests remain **NOT RUN**.

The implemented configuration uses `[[pinning.providers]]` plus
`[[pinning_rpc.providers]]` joined by `config_name`, with explicit
`[[pinning_identity.providers]]`. No extra registry is needed. For exact examples
and safety constraints, see [`config.example.toml`](../config.example.toml) and
the [RPC usage section](../README.md#scoped-ipfs-rpc-providers-stage-5).

```powershell
cargo test --test rpc_provider_config
cargo test --test rpc_submission_ledger
cargo test --test rpc_publication_cids
cargo test --locked --offline --test rpc_recovery_availability
cargo test --test rpc_diagnostics
cargo test --locked --offline --test rpc_stage5_http
cargo test --lib pinning::ipfs_rpc::
cargo test --lib pinning::
cargo test --lib kubo::
cargo test --test integration
cargo test --test lifecycle_transition_car_proxy
```

The focused targets have distinct evidence boundaries:

- `rpc_provider_config`: actual config parsing/coordinator construction, Kubo
  cid/upload/car and auth matrix, old Filebase PSA compatibility, rejected
  Filebase strategies/auth, URL/private-network/TLS restrictions, identities and
  source/target credential isolation. Its Filebase HTTP requests use mocks.
- `rpc_submission_ledger`: typed effects/resource evidence, retained quota debt,
  unknown/mismatch roots, stale claims/epochs, atomic observation persistence and
  unsafe downgrade refusal. Default coverage uses SQLite. Its PostgreSQL case
  is ignored and requires an explicitly selected disposable test database.
- `rpc_recovery_availability`: healthy in-flight allocation waits, late-receipt
  recovery under a new claim, coherent admission snapshots and negative lifetime
  fences. Its PostgreSQL opt-in also checks native timestamp precision without
  weakening the exact claim predicate.
- `rpc_publication_cids`: scoped canonical RPC targets, equivalent CIDv0/v1
  reservations, shared logical references, historical debt and route fencing,
  including legacy/v2 ZIP publication. Original object/version CIDs and S3
  ETags must remain unchanged; canonical target planning isn't object rewriting.
- `rpc_diagnostics`: real local CLI subprocess output and credential/username/URL
  redaction, configured route capabilities and denied unsupported combinations.
  These config-only commands don't connect to a database or probe a remote
  provider; configured support isn't account permission or a verification receipt.
- `rpc_stage5_http`: real SigV4 requests through registered `AppState`, standard
  publication and the actual pinning worker, with loopback HTTP source/target
  fixtures and the submission ledger. Its two tests cover shared CID allocation,
  stored SSE-C ciphertext, MPU and ZIP entries, unchanged public object identity,
  and evidence surviving terminal-job deletion. This is not a live provider
  account or production `main` test.

For PostgreSQL ledger coverage, provision a dedicated authorized test database
through `IPFS_S3_TEST_POSTGRES_URL`, then run the exact ignored case. Never use a
production database:

```powershell
cargo test --test rpc_submission_ledger postgres_rpc_submission_evidence_is_real_not_silently_skipped -- --ignored --exact --nocapture --test-threads=1
```

For the separate recovery PostgreSQL case, select a newly authorized loopback
database through `IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL`:

```powershell
cargo test --locked --offline --test rpc_recovery_availability postgres_actual_worker_receipt_lock_order_and_recoverable_waiters -- --ignored --exact --nocapture
```

The real leaf RPC runner needs Docker, PowerShell 7, cached
`ipfs/kubo:v0.43.0`, and already-available offline Cargo dependencies. It doesn't
pull images or install software. It creates two fresh repositories on a Docker
internal network, uses offline daemons with external discovery disabled, and
exposes only random loopback RPC ports. The output parent must already exist;
logs/summary are kept there, and cleanup is limited to this run's labeled
containers, volumes and network.

```powershell
pwsh -NoProfile -File tests/run-ipfs-rpc-real.ps1
```

The runner selects the ignored `rpc_provider_real_transports_and_local_dags`
case in `ipfs_rpc_real`. Its scope is
**leaf provider transport, exact DAG and stored bytes**, not production `main`,
configuration/worker integration or signed S3 SDK acceptance. It checks empty,
small/raw and multiblock files, an arbitrary-chunker upload mismatch followed by
CAR, gateway-crypto ciphertext, complete directory CAR, and a preexisting shared
pin. CID mode uses explicit unpinned CAR preseed, not public swarm retrieval.
Recursive target pins, local DAG completeness and target reads after stopping
the source are the real-node boundary. This leaf test doesn't prove actual S3
MPU/ZIP request handling; gateway-crypto ciphertext isn't an SSE-S3/SSE-C SDK test.

Basic/Bearer requests to the anonymous Kubo nodes exercise header transport only.
They **don't prove authentication enforcement**, a TLS/auth reverse proxy, or
Filebase account access. Filebase add, strict CID, account pin and public gateway
bytes require a separately authorized real-account run and remain **NOT RUN**.
The implementation uses only Filebase's supported upload parameters and doesn't
follow add with `pin/add`; mock coverage can't establish the account's plan or
paid capabilities. No Filebase CAR acceptance is claimed.

The isolated two-node matrix passed: all eight roots remained readable offline
after source shutdown. The separate registered-worker/SigV4 target passed both
tests. Affected targets passed 28 executions, with two PostgreSQL cases ignored
by default; recovery PostgreSQL was separately executed on 17.11 and passed,
including the single-statement snapshot and exact SQL timestamp fence. These
results don't establish account authorization or continuous availability.

Across these checks, HTTP 200 plus Hash must not hide EOF/trailer or stream
failures. Strict comparison includes codec and multihash, not textual equality
alone. Unknown effects and mismatched resources must remain persisted and block
automatic re-POST/deletion; matching CID or recursive pin doesn't imply
`ApplicationCreated`. Stop/drain old writers and workers before the RPC ledger
migration and retain the evidence afterward: a refused down migration must not
be bypassed by dropping tables. Inspect run summaries, failures and cleanup
receipts before recording a result; commands alone are not acceptance evidence.

## Stage 6 Cluster acceptance

The Cluster provider is separate from the main Kubo. Proxy transport and native
REST transport have distinct wire contracts; Proxy `dag/import` forwarding to a
paired Kubo is not Cluster CAR acceptance. Run the focused default targets:

```powershell
cargo test --locked --offline --test cluster_provider_config --test cluster_diagnostics --test cluster_provider_contract
cargo test --locked --offline --test cluster_network_policy --test cluster_replica_verifier --test stage6_cluster_worker
cargo test --locked --offline --test cluster_documented_config --test stage6_cluster_auth
```

Inspect the actual counts and assertions. Mock results do not establish physical
N/N replication. Fresh allocated peer observations must satisfy the lease's
captured confirmation contract before policy one/all aggregation. Queued, remote,
missing, failed and stale peers do not count. Old evidence remains audit history,
not a new live observation; matching CID does not establish deletion ownership.

The isolated provider-phase runner uses cached Kubo 0.43.0 and Cluster 1.1.6,
an independent third source, two allocated target Kubo nodes, and an internal
network. It must not pull images, reuse production data, or delete unknown
resources. Read its current parameters and prerequisites before execution:

```powershell
cargo test --locked --offline --test cluster_provider_real --no-run
pwsh -NoProfile -File tests/run-cluster-pinning-real.ps1 -ProviderPhase
```

**Local isolated acceptance passed.** Verifier coverage established 1/2, 2/2
and loss of a fresh replica in its bounded topology. The separate provider
transport target passed. Its wire audit recorded 26 POSTs: each transport used
six adds and seven CID submissions, with zero `dag/import` or DELETE calls.
Proxy's `Name/Hash/Bytes/Size` and REST's `name/cid/bytes/size/allocations` were
captured separately. Both target nodes passed complete offline DAG and stored-byte
checks after the independent source stopped, including ciphertext. Fresh 2/2
verification passed. This bounded same-host run is
not an account-authentication, billing or multi-host availability result.

PostgreSQL worker/replica-contract coverage is separately opt-in through
`IPFS_S3_STAGE6_TEST_DATABASE_URL`. Use only an authorized isolated test database;
an explicitly selected test without its environment must fail, not silently skip.
Neither SQLite nor an ignored PostgreSQL test is PostgreSQL fence evidence.

Isolated PostgreSQL 17.11 validation passed
`stage6_cluster_postgres_registered_claim_and_replica_contract` (1/1), using
actual AppState route registration, signed PUT/tagging/delete and the registered
worker. Native SQL accepted the original nanosecond caller token despite PG's
microsecond readback, rejected the previous token, and rolled back the claim
exactly. Both history tables rejected UPDATE/DELETE, downgrade was refused, and
deletion retained Unknown ownership and the reservation. Its provider mock is
not physical replica evidence.

`stage6_cluster_auth` passed 3/3: REST 401 at each verifier read remains
Authentication; a registered worker becomes Terminal and later due Polls issue
no HTTP while preserving Unknown history and quota. This target calls the S3
handler directly; signed HTTP evidence comes from the separate PG target.
The final isolated PostgreSQL 17.11 validation passed all five exact tests: the basic
contract above, late Poll/new-lifetime fencing, owner handoff with a fresh
successor, typed receipt/projection rollback and recovery, and actual lease-first
writer locking. The last case used `pg_blocking_pids` plus successful downstream
`FOR UPDATE NOWAIT` acquisitions, not a sleep-based ordering assumption.
An `unexpected EOF ... open transaction`
diagnostic after the injected rollback remains unassigned; rollback/recovery
assertions passed, and it is not reported as a fixed defect.

Stage 6 quality checks passed: Rust 1.92 `fmt --all -- --check`,
`check --locked --offline --all-targets`, and strict all-targets Clippy with
`-D warnings`; library tests **1438 passed / 0 failed / 1 ignored** and the
integration target **165 passed / 0 failed**. The eight focused targets above
recorded 171 target PASS results, including reused embedded leaf tests, and
7 ignored tests; ignored is not PASS. Five of those ignored PG cases were then
executed explicitly as described above. Real Filebase/Pinata account writes and
account authentication remain NOT RUN.

## R26 legacy namespace migration acceptance

The default formal CLI mode is read-only inventory. Apply/rollback require
explicit declarations, stop/drain assertions, fresh native fences and durable
DB receipts; no migration command may start AppState/provider workers, contact
remote storage, release retained usage, infer historical ownership, or repair
schema automatically. The separate registered Stage 7 management CLI is not
this R26 legacy namespace migration CLI.

```powershell
cargo test --locked --offline --test legacy_pinning_migration_store
cargo test --locked --offline --test legacy_pinning_migration_cli
cargo test --locked --offline --test legacy_pinning_migration_surface
cargo test --locked --offline --test legacy_pinning_migration_postgres -- --ignored --nocapture --test-threads=1
```

The PG target requires an explicitly run-owned disposable database and
`IPFS_S3_R26_PG_OWNED=legacy-r26-fixture-only`; do not supply a production URL.
Each case creates an isolated schema. All four R26 PG cases and both Stage 2 PG
cases passed on isolated PostgreSQL 17.11. Coverage includes native NOWAIT/phantom fences,
receipt-write fault and atomic rollback, route lock/committed drift,
contradictory historical account identity, the old-resource/new-allocation
boundary and damaged registered receipt catalogs.

The management-catalog lib cases in `snapshot_management_postgres_tests` use the
same explicit fixture marker and PostgreSQL URL. They create fresh `stage7_r26_`
schemas and retain them, including on failure, for operator inspection.

**Local isolated acceptance passed.** The formal
CLI target passed 7/7, including cross-process receipt replay/rollback,
97-byte/3-pin conservation, fresh read-only rollback preview, missing/corrupt
schema refusal, and undrained-apply/oversized-input guards. Focused review accepted
the historical-identity and new-lifetime product fixes. The signed HTTP recovery target passed
3/3 selected fixture cases (22 reused support tests were filtered): authenticated
PUT, Copy, CompleteMPU and legacy ZIP were refused before migration and succeeded
after the formal CLI apply/replay and HTTP restart. The remote fixture combines
accurate usage with a remote and ledger; it is not an independent remote-guard
branch proof. This checks publication, leases, capacity and mock Kubo readback,
not completed remote pinning or every legacy wire byte. The final isolated
export passed Rust 1.92 fmt check and strict all-targets Clippy, full lib
1442 passed/0 failed/1 ignored under default parallelism, integration 165,
store 33, CLI 7 and Stage2 SQLite 2. The two Stage2 PG cases ignored in the
ordinary command were actually exercised in the final PG run above.
No production migration, remote account operation, unpin, or GC has been performed.

## Stage 7 local management checks

**Status: local runtime validation passed.** The complete ten-apply workflow
includes real receipt-scoped workers, shared CIDs, retirement, same-file database
close/reopen, original-document replay and export, with private historical cells
preserved. The cumulative decode and full-registry SQL limits remain enabled.

Final checks on Rust 1.92: default-feature lib **1646 passed, 0 failed,
6 ignored**; integration **165 passed, 0 failed**; all-targets strict Clippy and
`cargo fmt --all -- --check` passed. Ignored tests are not counted as passing.
The four isolated PostgreSQL 17.11 targets for formal apply (including Renew and
cross-connection replay), managed Submit, retirement concurrency and root-release
migration were executed separately and passed. These are local/mock-provider and
isolated-database results, not production migration or external-account evidence.

With Rust 1.92 or later and cached dependencies, these commands select the local
targets. Commands alone aren't recorded passes:

```powershell
cargo test --locked --offline --test management_plan_wire --test management_db_snapshot
cargo test --locked --offline --test 'stage7_management_*'
cargo test --locked --offline --test stage7_retirement_barrier --test stage7_retirement_barrier_consumers --test stage7_retirement_barrier_http
```

The management targets check schema/native snapshots, confirmation, file and
directory evidence, CLI, action replay/chains/lifecycle/migration/provider points
and retirement. Acceptance must inspect executed results for original
confirmation/expiry, exact object/version/CID and route binding, ciphertext and
directory block-byte proof, atomic receipts, scoped execution and canonical
retirement fencing. Compilation and mock/local evidence aren't physical-provider
completion or storage reclamation evidence.

`stage7_retirement_barrier_postgres` is separately ignored and requires a
explicitly authorized isolated `stage7_` schema through
`IPFS_S3_STAGE7_RETIREMENT_DATABASE_URL`. It must not be pointed at production or
an occupied fixture. Once that environment is explicitly provided, select it with:

```powershell
cargo test --locked --offline --test stage7_retirement_barrier_postgres -- --ignored --nocapture --test-threads=1
```

`stage7_management_apply_postgres` is separately ignored and requires a
explicitly selected, empty, exclusively owned `stage7_` schema through
`IPFS_S3_STAGE7_APPLY_DATABASE_URL`. Use an authorized disposable PostgreSQL
17.11 instance; don't use a
production database or occupied fixture. Once that environment is explicitly
provided, select the target with:

```powershell
cargo test --locked --offline --test stage7_management_apply_postgres -- --ignored --nocapture --test-threads=1
```

Formal apply, native microsecond timestamp precision, NOWAIT/phantom fences and
receipt-write fault handling require actual execution of the ignored apply
target. Target existence isn't evidence that these checks passed.

Ignored PostgreSQL cases and command examples aren't passing evidence. Runtime
and native PostgreSQL acceptance require executed results against the finished
source. External account operations, production operations, unpin and GC aren't
authorized by these checks.

## Stage 7.5 management scale acceptance

Stage 7 results do not establish large-registry management usability. Stage 7.5
requires both bounded local-operation measurements and complete global inventory,
retirement, history recovery and export checks. Use isolated databases and
loopback provider fixtures; these tests do not authorize production upgrades or
external provider writes.

The commands below are an index by affected behavior, not unconditional gates for
every edit or release. Reuse passing evidence when later changes don't affect the
covered behavior or interfaces. Full Stage 7.5 acceptance still requires all
conditions in the [Stage 7.5 plan](superpowers/plans/2026-09-20-pinning-zip-unixfs.md#必要验收).

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_storage
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_storage_guards
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --lib conversion_read_capability_is_exact_and_does_not_escape_its_task
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --lib final_attestation_fence_rolls_back_mutation_or_cancellation
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage7_management_evidence --test stage7_management_directory_evidence
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_scale -- --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_retirement_scale -- --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_workflow --test stage75_history_conversion
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_consumers
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_transaction_lifetime --test stage75_transaction_postgres
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_wire --test stage75_management_upgrade_cli
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_initialization
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_history_export
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_checkpoint_cli
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_cli_scale real_100k_two_provider_cli_output_and_original_confirmation_recovery -- --exact --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_checkpoint_integrity
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_retirement_status
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_locator_meet
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_pixivbot
```

The scale target compares the same selected Create, Renew and Retry planning
scope at 1,000, 10,000 and 100,000 unrelated current objects, with current versions
and additional noncurrent history. It also measures ordinary publication and
ordinary worker completion with an applied management receipt present. Run this target serially: its allocation and
process-memory measurements are process-wide. Fixture construction is outside
the measured operation. Planning labels fresh single-connection and warm repeat
measurements separately; neither claims an evicted OS or PostgreSQL server cache.
Report SQL count and elapsed time, query plans, cumulative decode work, storage
returned rows/cells, raw/copied/output bytes, reservation peak, Rust peak/total
allocation and sampled process RSS separately. Storage meters count work charged
by the scoped reader, not the database engine's total scanned rows. Rust
allocation counters exclude native driver memory; RSS includes driver caches and
is sampled, not an exact allocation trace. SQLite query-plan output is not an
actual scanned-row counter. These measurements alone do not prove global-task
memory bounds, lock duration, all worker recovery/cancellation cases or all action branches.

The retirement scale target uses 100,000 current objects with leases and targets
in the selected domain, not just unrelated background objects. It checks disk
scan close/resume, cancellation and restart, a competing writer between pages,
complete responsibility totals, and private streaming export. Keep its memory
measurements serial as well. The workflow and history targets separately cover
the action chain, shared-CID allocation, frozen review recovery and original
committed facts; scale measurements do not replace those contracts.

Checkpoint CLI and integrity targets cover process exit/resume, source drift,
confirmation staging/publication, preservation of existing operator files,
corrupted originals or membership/totals, cumulative budgets and read-only recovery
after publication without output. Retirement status compares current responsibility
with frozen replay. Locator completeness checks fail-closed behavior for a missing
shared-job locator before and after owner cancellation; equal allocated authorities
don't prove the differing-authority case. `stage75_retirement_branches_scale`
separately covers large `RetireWith` selections and over-budget rollback.

The CLI scale target exercises a fresh 100,000-object task through proposal,
confirmation publication, complete original-inventory traversal, and two real CLI
recovery exports to distinct paths. Each output passes independent integrity checks;
a bounded streaming comparison checks all semantic content, including public cell
payloads, while allowing different JSON encoding and chunk boundaries. Original
confirmation times, ACKs and bindings remain unchanged, and recovery performs no
additional publication or remote calls. Persisted task work remains cumulative and
within the task's saved caps. Use the effective limits documented in the
[management guide](../src/pinning/management/README.md); a retained archive probe
without the original checkpoint cannot replace this end-to-end acceptance.

Use only the locally cached `postgres:17.11` image with `--pull=never`, loopback
binding and an explicitly authorized task-owned instance. Don't pull images, touch
existing instances or production databases, or delete databases/volumes. Stop only
the task-owned instance and report retained resources.

Each ignored entrypoint below requires a fresh, empty, exclusively owned `stage7_`
schema, except the initialized storage-fixture checks described below. Never reuse
another test's occupied schema or clear it to make a rerun pass.

| Target | Environment variable |
|---|---|
| `stage75_management_scale` | `IPFS_S3_STAGE7_APPLY_DATABASE_URL` |
| `stage75_storage_postgres` | `IPFS_S3_STAGE75_STORAGE_DATABASE_URL` |
| `stage75_storage_constraints_postgres` (initialized storage fixture) | `IPFS_S3_STAGE75_STORAGE_DATABASE_URL` |
| `stage75_history_conversion` | `IPFS_S3_STAGE75_HISTORY_DATABASE_URL` |
| `stage75_initialization` (each entrypoint needs its own fresh schema) | `IPFS_S3_STAGE75_HISTORY_DATABASE_URL` |
| `stage75_history_export_postgres` | `IPFS_S3_STAGE75_HISTORY_DATABASE_URL` |
| `stage75_recorded_status_postgres` | `IPFS_S3_STAGE75_RECORDED_STATUS_DATABASE_URL` |
| `stage75_confirmation_postgres` | `IPFS_S3_STAGE75_CONFIRMATION_DATABASE_URL` |
| `stage75_locator_meet` | `IPFS_S3_STAGE7_APPLY_DATABASE_URL` |
| `stage75_transaction_postgres` | `IPFS_S3_STAGE75_TRANSACTION_DATABASE_URL` |
| `stage75_transaction_lifetime` | `IPFS_S3_STAGE75_TRANSACTION_LIFETIME_DATABASE_URL` |
| `stage75_consumers_completion_postgres` | `IPFS_S3_STAGE7_APPLY_DATABASE_URL` |

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_management_scale -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_storage_postgres postgres_scoped_storage_fences_chunks_and_revision -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_storage_postgres postgres_immutable_guards_are_validated -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_storage_constraints_postgres -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_history_conversion postgres_nonempty_upgrade -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_initialization postgres_concurrent_appstate_initializes_once_and_refuses_backfill -- --exact --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_initialization postgres_appstate_does_not_initialize_partial_schema_without_migration_metadata -- --exact --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_history_export_postgres -- --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_recorded_status_postgres postgres_converted_status_uses_a_read_only_repeatable_snapshot_and_keeps_replay_original -- --exact --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_recorded_status_postgres postgres_historical_provider_status_rechecks_current_responsibility_without_rewriting_replay -- --exact --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_confirmation_postgres -- --ignored --nocapture
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_locator_meet -- --ignored
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_transaction_postgres -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_transaction_lifetime -- --ignored --nocapture --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_consumers_completion_postgres -- --ignored --nocapture --test-threads=1
```

The PostgreSQL storage checks cover empty-membership locks, revision drift,
rollback, exact private UTF-8 streaming, immutable chunks and write cancellation.
Immutable-guard and constraint checks require the initialized storage fixture and
the same storage URL; run them serially after the scoped-storage entrypoint.
Missing or weakened protections must reject authority, and sealed archives must
survive refused TRUNCATE operations. Conversion checks cover runtime refusal during
backfill, task/transaction/database isolation even after failure, and AppState
refusal before explicit nonempty upgrade. Recovery must preserve private originals
and NULL cells without rewriting source rows. Formal apply, Submit, retirement
concurrency and root-release checks remain required.

The initialization target checks the real AppState startup path for concurrent
empty-database initialization and zero-write refusal of partial or backfill schemas.
The history-export targets exercise complete private history recovery without
target-database writes, including unreferenced archive segments. The PostgreSQL
case also checks transactional collection revisions, committed drift refusal and
schema rejection when archive revision maintenance is missing.
The recorded-status PostgreSQL target checks converted object and historical
provider history against current database facts in a read-only consistent snapshot
while preserving the original committed replay result. The historical provider
case also checks unresolved resource identity, cancellation and missing
original-authority locators.

`stage75_consumers_authority_postgres` instead uses an already initialized,
parent-owned fixture through `IPFS_S3_STAGE7_APPLY_DATABASE_URL`, with a formally
registered route and clean source/locator seals. Do not use a fixture left dirty
by an intentional phantom or corruption test. It checks read-only authority
discovery and NOWAIT refusal under a maintenance-compatible lock, followed by
successful acquisition after rollback, without business writes.

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage75_consumers_authority_postgres -- --ignored --nocapture --test-threads=1
```

Pixivbot compatibility uses signed v1 ZIP PUT and MPU Create with
`decompress-zip=<prefix>/` and
`ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1d&ipfs-s3%3Acontent=decompressed` tagging.
Neither `x-ipfs3-zip-contract: v2` nor `x-ipfs3-zip-expected-sha256` is sent.
Acceptance checks XML `Entry.ETag` CIDs, decompressed-object pin intent and actual
loopback worker calls/completion, including nonempty management, retirement and
legacy history. Management wire v3 and ZIP v2 are separate protocols.

Commands and ignored entrypoints are not passing evidence. Stage 7.5 acceptance
also requires the complete nine-action/ten-apply workflow, original-plan replay,
same-file restart, private history and large export, all RetireWith branches,
SQLite/PostgreSQL failure and concurrency fences, full lib/integration with default
concurrency, fmt and strict all-targets Clippy against the integrated source.

## Opt-in multi-process capacity measurements

`pinning_multi_process_benchmark` provides bounded, isolated SQLite and loopback
provider scenarios. Its default test run checks workload construction and metrics:

```powershell
cargo test --locked --offline --test pinning_multi_process_benchmark
```

These checks passed **14 tests**, with **4 runtime entrypoints ignored**. They do
not establish benchmark completion or Stage 8 acceptance. Cross-process Submit
convergence, production-workload baselines and fault comparisons remain unverified.

To select the bounded runtime scenario explicitly:

```powershell
$env:IPFS_S3_PINNING_BENCHMARK = "offline-sqlite"
$env:IPFS_S3_PINNING_BENCHMARK_SECONDS = "60"
cargo test --locked --offline --test pinning_multi_process_benchmark pinning_multi_process_offline_benchmark -- --ignored --exact --nocapture --test-threads=1
```

The internal child entrypoint is started only by the harness. Do not use
`--include-ignored` to run all entrypoints together. The harness does not authorize
production traffic, physical GC or external account operations; its measurements
are not proof of a cross-process global limiter or physical storage reclamation.

## Environment-backed and deep checks

Run the relevant command when its external environment is available:

```powershell
pwsh -NoProfile -File tests/run-postgres-lifecycle-validation.ps1 -PostgresUrl $env:IPFS_S3_TEST_POSTGRES_URL
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1
cargo test --test postgres_versioning -- --nocapture --test-threads=1
cargo test --test e2e -- --include-ignored --nocapture --test-threads=1
cargo test --test multi_gateway -- --include-ignored --nocapture --test-threads=1
cargo test --test cluster -- --include-ignored --nocapture --test-threads=1
```

Use `--include-ignored` for a complete target so its unit and external tests both run. For one named external test, use `--ignored --exact`, for example:

```powershell
cargo test --test multi_gateway multi_gateway_cross_replica_contract -- --ignored --exact --nocapture --test-threads=1
cargo test --test cluster cluster_topology_converges -- --ignored --exact --nocapture --test-threads=1
```

These commands select the tests unconditionally: if the required topology is missing or unhealthy, the runner must fail rather than silently skip. Tests marked `ignored` are not counted as passing unless explicitly run in their required environment. Do not ignore the streaming, versioning, or security regression tests merely to shorten the default suite.
