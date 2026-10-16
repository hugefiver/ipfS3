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
cargo test --test stage3_migrations
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

**Status: local isolated R25 acceptance passed.** The final revision3 source is
the `dc519271` archive plus 135 exact overlay paths, independently built with
installed Windows GNU Rust 1.92.0, locked/offline, standard profiles and default
test concurrency. Earlier revision2 failures, cancelled builds and QA-wrapper
errors are retained as historical evidence, not relabelled as successful runs.

| Current revision3 check | Actual result |
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
ignored cases are not counted as runtime passes. Other previously verified
revision2 protocol targets are reused only where substantive dependencies are
unchanged; they are not reported as new revision3 executions.

Evidence lives under
`target/stage9-r25-integrate-20261008-dc51927-a718e92c/`:
`qa-r3-6b9e/report.md`, `r3protocol-6b9e21d4/report.md`, and the PG report below.
All lanes verified the 135 receipts and unchanged source identities. Coverage
preserves v1 ETag/CID, VersionId, legacy result bytes and v2 `ZipV2Result`, exact
successful output owners, manual archive-owned cancellation, non-ZIP behavior,
frozen configuration, recovery provenance, complete input proof and quota fences.

The Import heartbeat/publication deadlock has an observed same-deadline
failure/fix/revert proof. Historical Windows Code 32 and other unreproduced
timing symptoms are not claimed fixed by passing current suites. Stage7/8,
real paid-provider accounts, physical remote pins and production GC remain
**NOT RUN**. Local Noop/mock durable jobs do not prove physical pin completion.

The current local targets are `tests/stage9_zip_*.rs` (support directories aren't
test targets). With Rust 1.92 and already cached dependencies, use these bounded
PowerShell commands with the already installed Windows GNU toolchain, without
installing dependencies or adding persistent environment settings:

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test 'stage9_zip_*' -- --test-threads=1
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --lib zip::options:: -- --test-threads=1
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
All five ran with their original 8-second SQL deadline and passed on a fresh
loopback/tmpfs PostgreSQL 17.11 instance. The tail case proves zero publication
and quota writes after the injected fault, then same-capture retry with three
versions, two leases and shared-CID usage of 5 bytes/1 pin.

The separately authorized official `postgres:17.11` image pull fixed the missing
local cache; subsequent runs used the fixed image with `--pull=never`. Raw native
logs, server version, ten schema create/drop pairs, current nullable ALTERs,
case results and logs-first exact resource cleanup are recorded in
`qa-pg-rev3-f9768f2e-c2f0-4d29-af86-d8d77f77b63d/evidence.md` under the same parent
root. Schemas/other clients were zero before stopping the owned container;
owned run-label containers/volumes/networks were zero afterward.

Non-NULL ZIP capture pointer/identity closure and seeded-history no-fill/down
tests have SQLite native evidence and use the shared Rust TEXT/JSON scanner;
these five PG cases do **not** prove those individual fixtures on PostgreSQL.
No independent PG MPU/root-recovery or physical-provider assertion is implied.

Run that case only with an explicitly authorized disposable PostgreSQL 17 test
database already configured through `IPFS_S3_TEST_POSTGRES_URL`:

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage9_zip_postgres r25_pg_tail_rollback_then_exact_shared_cid_publication -- --ignored --exact --nocapture --test-threads=1
```

The case owns a fresh UUID schema and removes only that exact schema. Keep the
owned-run marker and evidence path with the result; `a718e92c` identifies the
recorded source/root run, not permission to clean up another run's resources.
Don't substitute a production database, a paid account, unpin/GC operations or
an unowned test instance for this fixture.

## Stage 5 RPC verification

**Status: Stage 5 implementation and isolated-backend acceptance passed.**
The approved Stage 5 section of
[`2026-09-20-pinning-zip-unixfs.md`](superpowers/plans/2026-09-20-pinning-zip-unixfs.md)
is the acceptance authority. The commands below are entry points, not results.
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

The isolated two-node runner completed with exit 0 in run
`24c5b32e5a224c1cb57be7784c564cd2`: the real matrix passed once, all eight roots
remained readable offline after source shutdown, and exact owned-resource cleanup
passed. The separate registered-worker/SigV4 target passed both tests.

The concurrent admission and late-receipt findings were fixed and accepted in
focused review. Latest affected targets passed 28 executions, with two PostgreSQL
cases ignored by default; recovery PostgreSQL was separately executed on 17.11
and passed, including the new single-statement snapshot and exact SQL timestamp
fence. The prior nanosecond-versus-microsecond failure was reproduced by restoring
the Rust comparison, not addressed with tolerance or longer waits. Latest pinning
library coverage passed 460 tests and integration passed 165; fmt, strict
all-targets Clippy, Rust 1.92 check and diff-check passed. The earlier complete
library run passed 1400 tests with one ignored before these focused corrections;
it is not presented as a post-correction full-library run. Disposable databases
and exact owned container inventories were cleaned. These are test execution
counts and bounded evidence, not account authorization or continuous availability.

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

The earlier verifier-only run `58672868d8ee43baaddd5aa3960881b5` established
1/2, 2/2 and loss of a fresh replica in its bounded topology. It does not establish
provider CAR transport, registered-worker persistence or continuous availability.
The initial provider-phase run `278435a7f9744af48d3d56a5a53eafd9` stopped at a
compile failure; its real transmission assertions were NOT RUN. Later acceptance
must record its own run, offline target DAG/byte checks and owned cleanup evidence.
Real Filebase/Pinata account writes and account authentication remain NOT RUN.

The later provider-phase run `a5d0e44ad9a44c3e8e7c8deee1d48a4d` passed its
isolated transport target. Its wire audit recorded 26 POSTs: each transport used
six adds and seven CID submissions, with zero `dag/import` or DELETE calls.
Proxy's `Name/Hash/Bytes/Size` and REST's `name/cid/bytes/size/allocations` were
captured separately. Both target nodes passed complete offline DAG and stored-byte
checks after the independent source stopped, including ciphertext. Fresh 2/2
verification and owned logs-first cleanup passed. This bounded same-host run is
not an account-authentication, billing or multi-host availability result.

PostgreSQL worker/replica-contract coverage is separately opt-in through
`IPFS_S3_STAGE6_TEST_DATABASE_URL`. Use only an authorized isolated test database;
an explicitly selected test without its environment must fail, not silently skip.
Neither SQLite nor an ignored PostgreSQL test is PostgreSQL fence evidence.

The isolated PostgreSQL 17.11 run `4caf2cc3831f456bb92f6dca15b2188d` passed
`stage6_cluster_postgres_registered_claim_and_replica_contract` (1/1). It used
actual AppState route registration, signed PUT/tagging/delete and the registered
worker. Native SQL accepted the original nanosecond caller token despite PG's
microsecond readback, rejected the previous token, and rolled back the claim
exactly. Both history tables rejected UPDATE/DELETE, downgrade was refused, and
deletion retained Unknown ownership and the reservation. Its provider mock is
not physical replica evidence. The owned container was logs-first removed; no
owned containers, volumes or networks remained.

`stage6_cluster_auth` passed 3/3: REST 401 at each verifier read remains
Authentication; a registered worker becomes Terminal and later due Polls issue
no HTTP while preserving Unknown history and quota. This target calls the S3
handler directly; signed HTTP evidence comes from the separate PG target.
The final updated-core PostgreSQL run `1a2319d11ef24384b4537865d56660cb`
passed all five exact tests, each in a distinct fresh PG17.11 database: the basic
contract above, late Poll/new-lifetime fencing, owner handoff with a fresh
successor, typed receipt/projection rollback and recovery, and actual lease-first
writer locking. The last case used `pg_blocking_pids` plus successful downstream
`FOR UPDATE NOWAIT` acquisitions, not a sleep-based ordering assumption. Logs and
SQL were captured before exact owned cleanup; remaining clients and owned
containers/volumes/networks were zero. An `unexpected EOF ... open transaction`
diagnostic after the injected rollback remains unassigned; rollback/recovery
assertions passed, and it is not reported as a fixed defect.

Final stable-tree checks passed: Rust 1.92 GNU `fmt --all -- --check`,
`check --locked --offline --all-targets`, and strict all-targets Clippy with
`-D warnings`; library tests **1438 passed / 0 failed / 1 ignored** and the
integration target **165 passed / 0 failed**. The eight focused targets above
recorded 171 target PASS results, including reused embedded leaf tests, and
7 ignored tests; ignored is not PASS. Five of those ignored PG cases were then
executed explicitly as described above. Quality runs used a run-owned writable
project-target TEMP/TMP/TMPDIR because this machine's system TEMP rejected file
creation; no global permissions, dependency versions or lint allowances changed.
Focused review closed the min/RF, retired-candidate and authentication defects
and the remaining PG/quality evidence gaps. **Stage 6 local isolated acceptance
passed**, within the topology and real-account limitations stated above.

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
Each case creates an isolated schema. Final run on 2026-10-08,
`b3a3aabdd2fb4ce88f7b4657126601e8`, used the R26-only export over committed
Stage6 `1515f89`, including the final concurrency fixes. All four R26 PG cases
and both Stage2 PG cases passed. Coverage includes native NOWAIT/phantom fences,
receipt-write fault and atomic rollback, route lock/committed drift,
contradictory historical account identity, the old-resource/new-allocation
boundary and damaged registered receipt catalogs. PostgreSQL 17.11 was cached,
pull disabled, data in tmpfs and access loopback-only. Schemas and client
sessions were zero before logs-first ID/name/label-verified container cleanup;
owned containers/volumes/networks were zero afterwards. Evidence and an explicitly
abbreviated diagnostic log excerpt remain at
`target/stage9-r26-pg-b3a3aabdd2fb4ce88f7b4657126601e8/`.
Earlier runs `64a9c2878fb5427ab0a013e0197a6b88` and
`d264f0d36b2d4df19d1773cf62708896` are retained as intermediate evidence only.

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
The empty-receipt hot path and duplicate failed-observation CAS regressions
were fixed with controlled and deterministic reproductions; no internal
timeouts, retries, worker counts, default parallelism or original assertions
were weakened. Implementation review accepted the final fixes and evidence.
Future Stage7/8 files were excluded from this export and do not prove their own
delivery. No production migration, remote
account operation, unpin, or GC has been performed.

## Stage 7 local management checks

**Status: local runtime validation passed.** The complete ten-apply workflow
includes real receipt-scoped workers, shared CIDs, retirement, same-file database
close/reopen, original-document replay and export, with private historical cells
preserved. The cumulative decode and full-registry SQL limits remain enabled.

Final checks on Rust 1.92 GNU: default-feature lib **1646 passed, 0 failed,
6 ignored**; integration **165 passed, 0 failed**; all-targets strict Clippy and
`cargo fmt --all --check` passed. Ignored tests are not counted as passing.
The four isolated PostgreSQL 17.11 targets for formal apply (including Renew and
cross-connection replay), managed Submit, retirement concurrency and root-release
migration were executed separately and passed. These are local/mock-provider and
isolated-database results, not production migration or external-account evidence.

With the installed Windows GNU Rust toolchain and cached dependencies, these
PowerShell commands select the actual local targets. They aren't recorded passes:

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test management_plan_wire --test management_db_snapshot
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test 'stage7_management_*'
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage7_retirement_barrier --test stage7_retirement_barrier_consumers --test stage7_retirement_barrier_http
```

The management targets check schema/native snapshots, confirmation, file and
directory evidence, CLI, action replay/chains/lifecycle/migration/provider points
and retirement. Acceptance must inspect executed results for original
confirmation/expiry, exact object/version/CID and route binding, ciphertext and
directory block-byte proof, atomic receipts, scoped execution and canonical
retirement fencing. Compilation and mock/local evidence aren't physical-provider
completion or storage reclamation evidence.

`stage7_retirement_barrier_postgres` is separately ignored and requires a
parent-registered, authorized isolated `stage7_` schema through
`IPFS_S3_STAGE7_RETIREMENT_DATABASE_URL`. It must not be pointed at production or
an occupied fixture. Once that environment is explicitly provided, select it with:

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage7_retirement_barrier_postgres -- --ignored --nocapture --test-threads=1
```

`stage7_management_apply_postgres` is separately ignored and requires a
parent-selected, empty, exclusively owned `stage7_` schema through
`IPFS_S3_STAGE7_APPLY_DATABASE_URL`. Use an authorized disposable PostgreSQL
17.11 instance with its image already cached; don't pull an image or use a
production database or occupied fixture. Once that environment is explicitly
provided, select the target with:

```powershell
cargo +1.92.0-x86_64-pc-windows-gnu test --locked --offline --test stage7_management_apply_postgres -- --ignored --nocapture --test-threads=1
```

Formal apply, native microsecond timestamp precision, NOWAIT/phantom fences and
receipt-write fault handling require actual execution of the ignored apply
target. Target existence isn't evidence that these checks passed.

Ignored PostgreSQL cases and command examples aren't passing evidence. Runtime
and native PostgreSQL acceptance require executed results against the finished
source. External account operations, production operations, unpin and GC aren't
authorized by these checks.

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
