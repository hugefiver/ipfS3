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
v2 signed headers, token replay, source=false, URL digest, and zero-output MPU
completion/replay. In particular, source=false zero-output Complete must
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
