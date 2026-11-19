# PostgreSQL Production Baseline Design

**Date:** 2026-08-13

**Status:** Approved design

**Scope:** Add an opt-in PostgreSQL Docker Compose deployment baseline, database-backed readiness, an in-image readiness probe, and shared local-live and hosted-CI validation. The existing SQLite development Compose stack remains unchanged.

## Background

The repository's default `docker-compose.yml` and `config.docker.toml` are a
development stack: they use a file-backed SQLite database, mount that config,
publish Kubo ports, include Cloudflared, and retain gateway data. That is useful
for local work, but it is not a production deployment contract.

The gateway supports PostgreSQL through SeaORM. `AppState::new` connects
to the configured database and opens or initializes its schema before binding the listener.
`Store::db()` exposes the `DatabaseConnection`, and SeaORM 1.1.14 provides
`DatabaseConnection::ping`. The `/health` handler always returns `200 OK`.
The runtime image intentionally contains neither `curl` nor `wget`, so a Compose
healthcheck cannot depend on either tool. PostgreSQL store tests and SQLite
Compose E2E don't by themselves prove the production PostgreSQL topology.

## Goals

1. Provide `docker-compose.postgres.yml` as a production-oriented, single-node
   deployment: one PostgreSQL 17 instance, one Kubo instance, and one gateway.
2. Keep `docker-compose.yml` and `config.docker.toml` behavior unchanged.
3. Require the database password, S3 access key, S3 secret key, and master key
   during Compose interpolation. A missing value must make `docker compose
   config` fail before any container starts.
4. Distinguish process liveness from database readiness without exposing database
   errors.
5. Verify the deployment locally with live Docker in one unique disposable
   Compose project, then run the same validation as a blocking hosted CI
   regression using only disposable resources.
6. Production support claims require live topology evidence. Local and
   GitHub-hosted results are reported separately; the blocking hosted job must
   pass before merge or release.

## Non-goals

- Multiple gateway instances, migration leader election, PostgreSQL high
  availability, backup, TLS, or connection-pool tuning.
- IPFS Cluster, private swarm configuration, Kubo readiness checks beyond its
  existing service healthcheck, or cloud resources.
- Secret-file (`_FILE`) support, a secret manager, key rotation, or changing
  existing encryption semantics.
- Publishing PostgreSQL or Kubo service ports from the production Compose file.
- Cloudflared, `container_name`, `config.docker.toml` bind mounts, or a
  `gateway_data` volume in the production Compose file.

## Deployment structure

Create `docker-compose.postgres.yml` beside the default Compose file. It owns a
single PostgreSQL service, Kubo service, gateway service, named PostgreSQL data
volume, and named IPFS data volume. It starts with environment-only gateway
configuration and is selected explicitly with `-f docker-compose.postgres.yml`.

This keeps SQLite quick-start behavior stable, gives production operators a
small topology to inspect, and prevents local Cloudflared or development config
from becoming an accidental production dependency.

## Architecture and topology

`docker-compose.postgres.yml` defines exactly these services:

```text
S3 client -> host bind -> gateway:9000 -> postgres:5432
                                    -> kubo:5001
```

The production file declares no `ports` entry for PostgreSQL or Kubo. They are
reachable only by service name on the Compose network. The gateway is the only
published service. Its host mapping must use an explicit host bind and defaults
to loopback in the documented invocation, for example
`127.0.0.1:9000:9000`. The production file must never publish `0.0.0.0:9000`.
An operator who needs a reverse proxy binds a specific host interface and puts
TLS termination outside this baseline.

The file contains no `container_name`, so the Compose project name namespaces
containers, networks, and volumes. It contains no Cloudflared service, no
development config mount, and no gateway data volume. Kubo keeps its named IPFS
volume and PostgreSQL keeps its named database volume. The gateway is stateless
apart from those backing services.

PostgreSQL uses `postgres:17`, a database named `ipfs3`, and a non-superuser
application account named `ipfs3`. Kubo uses the existing image/build and its
existing `ipfs id` healthcheck. Gateway waits for PostgreSQL and Kubo health
before starting. It also has its own readiness healthcheck, described below.

## Configuration and secret contract

The production Compose file sets gateway configuration through environment
variables only:

| Variable | Required | Contract |
| --- | --- | --- |
| `POSTGRES_PASSWORD` | Yes | PostgreSQL application-account password. It must contain only URL-safe unreserved characters, `[A-Za-z0-9._~-]`, because it is interpolated into the database URL. |
| `IPFS_S3_ACCESS_KEY_ID` | Yes | S3 access key presented to gateway authentication. |
| `IPFS_S3_SECRET_ACCESS_KEY` | Yes | S3 secret key presented to gateway authentication. |
| `IPFS_S3_MASTER_KEY` | Yes | Exactly 64 hexadecimal characters representing the 32-byte master key. |
| `IPFS_S3_GATEWAY_BIND` | Yes | Explicit non-wildcard host bind for the published gateway port. The documented production value is `127.0.0.1`. |
| `IPFS_S3_GATEWAY_PORT` | Yes | Host TCP port for the gateway, decimal `1` to `65535`. |
| `PINATA_JWT` | No | Used only when a configured Pinata provider refers to this environment variable. |
| `FILEBASE_PINNING_TOKEN` | No | Used only when a configured Filebase provider refers to this environment variable. |

Every required secret interpolation uses Compose's required form, for example
`${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}` and
`${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}`. The gateway receives
`IPFS_S3_DATABASE_URL=postgres://ipfs3:${POSTGRES_PASSWORD}@postgres:5432/ipfs3`,
`IPFS_S3_KUBO_RPC_URL=http://kubo:5001`, and `IPFS_S3_BIND=0.0.0.0:9000`.
The final bind is internal to the container; host exposure remains controlled by
the explicit `ports` host address.

Provider tokens are omitted unless an enabled provider configuration references
them. No literal provider token appears in YAML. Compose environment variables
are visible to people and processes allowed to inspect the container or its
Compose configuration. This baseline reduces accidental publication but does not
remove that residual secret-visibility risk. Operators must restrict Docker and
host access accordingly.

The implementation adds a static PowerShell contract test. It reads the
production Compose YAML as text and rejects default values for required secrets,
`0.0.0.0` gateway publication, published PostgreSQL or Kubo ports,
`container_name`, Cloudflared, a development config mount, and `gateway_data`.
It also requires the PostgreSQL 17 image, exact service set, mandatory-variable
syntax, loopback-only disposable validation mappings, and the readiness probe
healthcheck. The test does not start Docker.

## Liveness, readiness, and probe behavior

`/health` remains an unconditional liveness endpoint. It always returns
`200 OK` with body `OK` once the HTTP server is running. It does not ping
PostgreSQL or Kubo.

The gateway adds `/ready`. The route holds the initialized `AppState`, calls
`state.store.db().ping()`, and wraps the future in a two-second timeout. A
successful ping returns `200 OK` with body `READY`. A database error or timeout
returns `503 Service Unavailable` with body `NOT READY`. The HTTP response,
including headers and body, contains no driver, database, hostname, credential,
or timeout detail. A structured server-side warning records only the failure
class, `error` or `timeout`, without a connection URL.

`AppState::new` opens the configured schema before the listener binds. Readiness is a later, independent database check for a running
process whose database becomes unavailable. It intentionally does not test
Kubo, remote pinning providers, workers, S3 credentials, or an individual S3
operation.

The gateway binary accepts `--ready-probe` as its sole operational probe mode.
It performs a bounded HTTP GET to `http://127.0.0.1:9000/ready`, with a
two-second total request deadline, and exits zero only for status `200` and body
`READY`; it exits nonzero for connection failure, timeout, any other status, or
any other body. It starts no listener, workers, migrations, or normal gateway
process in this mode. The production gateway healthcheck invokes that binary
mode directly. No package is added to the runtime image.

## Migration ownership

The gateway connects before binding port 9000. It initializes an empty schema
and opens a current one. For any supported older schema, stop the gateway and
run the [explicit database upgrade](../../database-storage.md#upgrade-an-existing-database)
before starting it. The Compose file doesn't run an upgrade command.

Persistent database data is retained by the named PostgreSQL volume. Production
operator documentation must never recommend `docker compose down --volumes`,
because it deletes that volume. `down --volumes` is permitted only in disposable
CI cleanup, where the project, volume, and data were created solely for that
job.

## PostgreSQL JSON columns

New PostgreSQL schemas define `objects.metadata`, `multipart_uploads.metadata`,
and `multipart_uploads.tags_json` as `JSONB`. Metadata is nullable; multipart
tags are required and default to an empty array. SQLite uses `TEXT` for these
columns. PostgreSQL store coverage includes multipart metadata and tags round
trips and multipart-part writes.

## Data and failure flow

1. Compose interpolates required variables. A missing required value stops
   `docker compose config` before resources are created.
2. PostgreSQL and Kubo start on the internal network and become healthy.
3. Gateway connects to PostgreSQL, opens or initializes the schema, binds
   internally on port 9000, then starts workers.
4. The gateway probe requests `/ready`; only a successful database ping marks
   the service healthy.
5. S3 requests flow from the explicit host binding through the gateway to
   PostgreSQL metadata and Kubo content storage.
6. If PostgreSQL later stops or stops accepting connections, `/health` remains
   `200 OK`; `/ready` returns `503 NOT READY` within the two-second handler
   deadline. Existing S3 requests follow their established database error path.
7. If Kubo fails after startup, `/ready` can still be ready because it represents
   database readiness only. Kubo failures remain visible through the existing
   S3/Kubo operation errors and Kubo service health.

## Error handling and security

Startup errors from invalid configuration, secret parsing, database connection,
or migrations fail the gateway process before it listens. Compose health and
dependency conditions make that failure visible without returning a falsely
ready HTTP service.

The readiness handler treats a ping error and a timeout identically to callers:
`503 NOT READY`. It never serializes `DbErr`, a URL, or a credential. The binary
probe similarly exposes no error text to Compose, only its exit code. Logs retain
enough classified information for an operator to distinguish a timeout from a
database error, but not the database URL or password.

The deployment baseline is not a complete secret-management solution. Values
in environment variables may be exposed through container inspection, process
environment access, crash diagnostics, or insecure shell history. It does not
solve that risk. It requires least-privilege host and Docker access, avoids
printing the composed configuration in production logs, and defers secret
manager integration to a separate design.

## CI and live validation

The complete local validation sequence uses disposable resources. Every Compose command
uses one unique, disposable Compose project name, the committed disposable
validation override, the same disposable URL-safe credentials and test-only
S3/master-key environment values, the same fixed loopback validation ports, and
the same PostgreSQL-stop failure scenario as the
`postgres-production-deployment` workflow job. All blocking commands must pass
against real local Docker containers. This is local live evidence, not
GitHub-hosted evidence, and it must never be reported as a hosted workflow pass.

The PowerShell workflow checks verify that the committed workflow retains the
required blocking job and validation sequence, but it does not replace the live
Docker run.

`release-validation.yml` gains a fourth independent, blocking job named
`postgres-production-deployment`. The job uses a unique Compose project name
formed from the GitHub run ID and attempt. It supplies disposable, URL-safe
PostgreSQL credentials and test-only S3/master-key values through job-scoped
environment variables. It has an explicit timeout and runs on Ubuntu.

The job combines `docker-compose.postgres.yml` with a committed disposable
validation override. The base production file publishes neither PostgreSQL nor
Kubo. The override publishes PostgreSQL `5432`, Kubo RPC `5001`, and gateway
`9000` only to `127.0.0.1` on fixed job-local validation ports. It is never
referenced by production instructions. E2E receives those loopback endpoints
through `IPFS_S3_E2E_ENDPOINT` and `IPFS_S3_E2E_KUBO_URL`.

The local validation sequence and the hosted job perform these blocking checks in
the same order:

1. Run Compose config with each required secret removed and require failure.
2. Run Compose config with the job's full environment and require success.
3. Build and start the three-service stack with `--wait` under the unique
   project name.
4. Request `/health` and `/ready` through the loopback gateway mapping and
    require `200 OK` bodies `OK` and `READY`.
5. Query PostgreSQL inside its container for the current schema marker in
   `seaql_migrations` and verify the three JSONB column types before running E2E.
6. Run the existing serial `cargo test --test e2e -- --nocapture
    --test-threads=1` target against the validation loopback endpoints. It must
    pass every selected case, including the 6 MiB multipart `UploadPart` case.
7. Stop only the validation project's PostgreSQL service. Require `/health` to
   remain `200 OK`, then poll `/ready` for no more than ten seconds until it
   returns `503 NOT READY`.

An `always()` diagnostics step prints non-coloured logs for the project. A
following `always()` cleanup step runs `docker compose ... down --volumes
--remove-orphans` with the same project name, then verifies that no containers,
networks, or volumes with that Compose project label remain. Cleanup and the
zero-residual assertion belong only to this disposable CI stack. Failure of
diagnostics or cleanup must not hide the primary command's result.

The PowerShell checks run in the release-validation infrastructure job before hosted live
deployment validation. It checks the job is blocking, has the unique project
name, includes all checks above in order, uses the disposable override, prints
logs and cleans up on `always()`, and does not weaken the default SQLite Compose
E2E job.

The `postgres-production-deployment` job is a blocking regression for pushes
and pull requests. A GitHub-hosted pass is required before merge or release.
Local live results don't establish a hosted pass.

## Component responsibilities

| Path | Responsibility |
| --- | --- |
| `docker-compose.postgres.yml` | Production PostgreSQL, Kubo, and gateway topology and environment contract. |
| `tests/compose.postgres-production-validation.yml` | Disposable CI-only loopback port publication. |
| `src/main.rs` | `/ready` route, readiness response, and `--ready-probe` process mode. |
| `src/store/mod.rs` | Expose `Store::db()` for readiness. |
| `Dockerfile` | Healthcheck-compatible binary packaging only if required, with no HTTP probe package installed. |
| `.github/workflows/release-validation.yml` | Independent blocking production deployment job. |
| `tests/postgres-production-baseline.Tests.ps1` | Static Compose and workflow contract checks. |
| `src/store/migrations/baseline.rs` | Install the final SQLite or PostgreSQL layout. |
| `tests/postgres_import.rs` | PostgreSQL JSONB and multipart compatibility coverage. |
| `tests/release-validation.Tests.ps1` | Static latest-migration expectation used by the production validation workflow. |
| `README.md` | Production invocation and secret-handling guidance, only after local live Docker evidence and the static workflow contract test pass. |
| `ROADMAP.md` | Check only `PostgreSQL production deployment`, only after local live Docker evidence and the static workflow contract test pass. |

## Acceptance criteria

The implementation is accepted when all of the following are true:

1. Only the explicit PostgreSQL Compose file selects PostgreSQL. The default
   Compose file still uses SQLite and retains its current development behavior.
2. Production Compose contains one PostgreSQL 17, one Kubo, and one gateway;
   it has no Cloudflared, fixed container names, development config mount, or
   gateway data volume.
3. PostgreSQL and Kubo have no host port publication. Gateway host publication
   requires an explicit non-wildcard bind, with `127.0.0.1` documented as the
   standard value.
4. Missing database password, S3 access key, S3 secret key, or master key makes
   `docker compose config` fail. Provider tokens remain optional.
5. A running gateway returns `200 OK`/`OK` from `/health`; a successful database
   ping returns `200 OK`/`READY` from `/ready`; database error or timeout returns
   `503`/`NOT READY` without error detail.
6. `--ready-probe` performs only the bounded local readiness request and serves
   as the gateway container healthcheck without adding curl or wget to the image.
7. Gateway startup initializes an empty schema before binding. Live validation
   checks the current ledger marker and the three PostgreSQL `jsonb` columns.
8. The initial local live validation sequence and the identical new CI job prove
   missing-secret configuration failure, successful stack readiness, both healthy
   endpoints, current migrations, existing E2E success, correct behavior after
   PostgreSQL stops, diagnostics, scoped cleanup, and no residual project
   resources.
9. Static PowerShell checks enforce the topology, secret, healthcheck, and CI
   contracts without starting Docker.
10. README and ROADMAP change only after the local live Docker validation has
    passed in one unique disposable Compose project and the static PowerShell
    workflow contract test has passed. The roadmap change checks PostgreSQL
    production deployment only; it does not check multiple gateway instances,
    IPFS Cluster, or private swarm.
11. `postgres-production-deployment` is a blocking regression on pushes and pull
    requests. Its GitHub-hosted pass is required before merge or release;
    reports distinguish local live evidence from hosted evidence.
12. PostgreSQL store tests verify the three `jsonb` columns, multipart metadata
    and tags round trips, and successful part upsert.
13. The complete validation sequence passes from a unique disposable Compose
    project, including E2E, PostgreSQL-stop readiness behavior, diagnostics and
    scoped cleanup. Partial results don't satisfy deployment acceptance.
