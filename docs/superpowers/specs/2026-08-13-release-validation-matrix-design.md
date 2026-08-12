# Release Validation Matrix Design

**Date:** 2026-08-13
**Status:** Approved design
**Scope:** Add secret-free, blocking automation for the live PostgreSQL, Docker Compose end-to-end, and client-smoke infrastructure surfaces that the existing CI does not execute.

## Problem

The existing `CI` workflow checks, lints, formats, and runs the library and
in-process integration suites. It compiles all test targets, but it does not
execute `tests/postgres_import.rs` or `tests/e2e.rs`.

This creates two release-evidence gaps:

1. `postgres_import` exits successfully without exercising PostgreSQL when
   `IPFS_S3_TEST_POSTGRES_URL` is absent.
2. `e2e` requires a running Kubo and gateway stack, so it is never exercised by
   the existing workflow.

The repository also has a PowerShell client-smoke runner and infrastructure
tests. Real rclone and mc evidence is currently manual, and AWS CLI is recorded
as skipped. Moving all three real clients into blocking CI now would require a
separate cross-platform rewrite because the runner intentionally assumes local
images and contains Docker Desktop path handling.

## Goals

1. Run all four live PostgreSQL import/concurrency tests against PostgreSQL 17.
2. Build and start the repository's Kubo and gateway Compose services, wait for
   health, and run the real `e2e` target serially.
3. Run the client-smoke infrastructure contract on Ubuntu without claiming that
   any real external client passed.
4. Make every selected surface blocking on pull requests and `master` pushes,
   while retaining a manual dispatch entry point.
5. Bound execution time, capture Compose diagnostics on failure, and clean up
   test resources on every completed run.
6. Ensure local verification owns only uniquely named Compose projects, refuses
   pre-existing fixed gateway/Kubo container names, and cannot inherit endpoint
   or port overrides from the operator's environment.
7. Keep the four live PostgreSQL tests isolated when they share one disposable
   database by terminally superseding only active jobs intentionally left queued
   by an earlier test before that test releases the serial guard.

## Non-goals

- Do not perform a release, publish images, create tags, or change package
  versions.
- Do not implement PostgreSQL production deployment, multiple gateways, IPFS
  Cluster, or private swarm networking.
- Do not connect to Pinata, Filebase, Cloudflare, or any other credentialed
  provider.
- Do not report AWS CLI, mc, or rclone as passing. Their real smoke execution is
  the next independent roadmap task.
- Do not replace or duplicate the existing check, Clippy, unit, integration, or
  formatting jobs.
- Do not change production Rust behavior, database schema, Compose topology, or
  client-smoke runner behavior.
- Do not change PostgreSQL ownership assertions or global `claim_due` behavior;
  test isolation must operate only on buckets created by the current test.

## Architecture

Create `.github/workflows/release-validation.yml` with `pull_request`, `push` to
`master`, and `workflow_dispatch` triggers. A workflow-level concurrency group
uses the workflow name and ref and cancels an older run for the same ref.
Permissions are read-only and each job has an explicit timeout.

The workflow has three independent blocking jobs:

### PostgreSQL import

Use a GitHub Actions PostgreSQL 17 service container with the same database,
user, and password as `tests/compose.postgres-import.yml`. Publish port 5432,
require `pg_isready` health checks, and set
`IPFS_S3_TEST_POSTGRES_URL=postgres://ipfs3:ipfs3@127.0.0.1:5432/ipfs3_import_test`
at job scope. Install Rust 1.92, restore the Cargo cache, and run:

```text
cargo test --test postgres_import -- --nocapture --test-threads=1
```

The job-scoped URL ensures the target cannot take its environment-missing skip
path in this workflow. Serial execution preserves the target's live-database
isolation assumptions.

The live target contains race fixtures that intentionally leave selected import
jobs queued after asserting their intermediate state. Those jobs are globally
eligible for `claim_due`, so a later serial test can otherwise claim an earlier
test's job. The affected tests therefore perform test-only teardown after all
behavioral assertions: begin a transaction, lock each current-test bucket with
`lock_bucket_for_ownership`, call `supersede_bucket`, and commit. This preserves
the asserted race outcomes while making the shared disposable database a valid
suite-level fixture. Production claim ordering and ownership behavior remain
unchanged.

### Docker Compose E2E

Check out the repository, install Rust 1.92, restore the Cargo cache, and prove
that the runner exposes Docker Compose. Start only `kubo` and `gateway` from the
explicit base file:

```text
docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway
```

Selecting the two services excludes `cloudflared` and therefore requires no
tunnel token. Using `-f docker-compose.yml` prevents the developer override from
mounting an unrelated local `config.toml`. After health succeeds, run:

```text
cargo test --test e2e -- --nocapture --test-threads=1
```

An `always()` diagnostics step prints non-coloured Compose logs. A second
`always()` step removes containers, networks, orphaned resources, and volumes.
Both diagnostic steps are bounded by the job timeout and are marked
`continue-on-error` so cleanup failures cannot hide the primary test result.

### Local live-verification ownership

Local PostgreSQL and E2E verification use unique explicit Compose project names
for every run and pass that project name to every `up`, `logs`, and `down`
command. Cleanup is therefore scoped to resources created by that verification
attempt.

The base Compose file assigns fixed `container_name` values to Kubo and the
gateway, which project naming cannot isolate. Before E2E starts, verification
must inspect all Docker containers, including stopped containers, and refuse to
run if either `ipfs-s3-kubo` or `ipfs-s3-gateway` already exists. It must not
clean up after this refusal. Once that preflight passes, a failed or partial
start is owned by the verification project and may be cleaned up.

Local commands save and restore all relevant environment variables in `finally`
blocks. PostgreSQL forces `IPFS3_IMPORT_POSTGRES_PORT=55432` and the matching
`IPFS_S3_TEST_POSTGRES_URL`. E2E forces
`IPFS_S3_E2E_ENDPOINT=http://127.0.0.1:9000` and
`IPFS_S3_E2E_KUBO_URL=http://127.0.0.1:5001`. This prevents an operator's shell
from redirecting the test to an unrelated service and producing false evidence.

### Client-smoke infrastructure

Run a new static workflow contract test followed by the existing client-smoke
infrastructure test:

```text
pwsh -NoProfile -File tests/release-validation.Tests.ps1
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

The first script parses the workflow as text and fails unless the required
triggers, PostgreSQL URL and target, explicit Compose file and services, serial
E2E target, diagnostics, cleanup, and timeouts remain present. It also rejects
real client execution and `cloudflared` in this workflow. The existing script
continues to test command argument boundaries, timeout tree cleanup, temporary
path ownership, offline-build invariants, and requested-unavailable exit
classification without launching real clients.

## Data and Failure Flow

Each job is independent and produces one binary result:

1. GitHub prepares a fresh Ubuntu runner.
2. The job starts only its declared local service dependencies.
3. The target test either exercises that dependency and exits zero or fails the
   job.
4. The Compose job prints service logs regardless of test outcome and then tears
   down its owned stack.
5. GitHub destroys the runner and service containers.

No job accepts missing credentials as a skip because no credentialed provider is
part of the matrix. The PostgreSQL URL is mandatory workflow configuration, E2E
has no skip gate, and the infrastructure scripts throw on violated contracts.

## Verification

The implementation is accepted only when:

1. `tests/release-validation.Tests.ps1` first fails because the workflow is
   absent, then passes after the workflow is written.
2. `tests/client-smoke.Tests.ps1` passes locally.
3. The workflow contract proves that PostgreSQL cannot use its missing-URL skip
   path and that E2E starts only Kubo and gateway from the base Compose file.
4. `docker compose -f docker-compose.yml config` succeeds locally.
5. `cargo test --lib`, `cargo test --test integration`, formatting, and Clippy
   remain green.
6. YAML whitespace and the complete Git diff pass repository checks.
7. Local verification audits prove unique project names, fixed-container
   preflight, forced PostgreSQL port/URL, forced E2E endpoints, and restoration
   of every overridden environment variable.
8. The four PostgreSQL tests pass in one fresh serial target invocation, and a
   second fresh-fixture invocation also passes all four. Each invocation proves
   that no earlier test in that target leaves claimable work for a later test;
   using a fresh fixture avoids conflating this isolation check with unrelated
   fixed job IDs that are intentionally retained as terminal history.

Local execution of the full live matrix is required when available. If a local
Docker daemon or required image/build network is unavailable, that is reported
as unverified evidence rather than converted into a passing result; the workflow
definition and its contract tests must still pass.

## Repository Boundary

This task may create the workflow, its PowerShell contract test, this design,
and its implementation plan, and may add test-only active-job teardown to
`tests/postgres_import.rs`. It may not modify production source, PostgreSQL
behavioral assertions, Compose files, client-smoke behavior, version metadata,
or compatibility claims.
