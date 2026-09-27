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
The final Stage 3 `--lib` and `--test integration` combined run is still pending;
don't report its counts as passed until that run finishes. The intermittent
Stage 2 PG historical-route error still has no confirmed root cause.

Test cleanup may remove static source, hash, documentation-checkbox, and historical TDD-shape assertions. It must retain executable regressions for command argument boundaries, process ownership, path confinement, cleanup scope, redaction, concurrency, and other observable security behavior.

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
