# PostgreSQL Production Baseline Design

**Date:** 2026-08-13

**Status:** Approved design

**Scope:** Add an opt-in PostgreSQL Docker Compose deployment baseline, database-backed readiness, an in-image readiness probe, and shared local-live and hosted-CI validation. The existing SQLite development Compose stack remains unchanged.

## Runtime-driven revision, 2026-08-24

This approved design is revised after Task 4 live evidence exposed a PostgreSQL
JSON model and schema incompatibility inside the same v0.5-A boundary. The
original design did not claim to anticipate this problem. This revision records
the observed evidence, the migration candidate, and the proof required before
that candidate can be called the root cause.

The production PostgreSQL 17 Compose stack passed health, readiness, migration,
and application-role checks. The full end-to-end target passed 10 of 11 tests.
Its only failure was `test_10_multipart_upload`: a 6 MiB `UploadPart` returned
`500 internal database error`. A fresh, isolated Compose project with a unique
name reproduced the exact test in a stable 1 of 1 failure.

In the gateway log, a successful `SELECT multipart_uploads ...` returned one
row and was followed immediately by the 500 response. PostgreSQL verbose logs
contained no `ERROR`, `STATEMENT`, or SQLSTATE, and no `multipart_parts` INSERT
was issued. That rules out an upsert statement failure, part-width issue, and
test-order leakage for this observation.

The inspected live schema was fully current through
`m20260730_000001_standard_mutation_fence`; the role ownership and privileges,
multipart primary key and foreign key, `INTEGER` and `BIGINT` widths, and
`TIMESTAMPTZ` columns were correct. The failed upload row existed with zero part
rows. Its selected values were `metadata = NULL` and `tags_json = []`, but both
PostgreSQL columns are `TEXT`. The entity models decode
`multipart_upload::Model.metadata` as `Option<Json>` and
`multipart_upload::Model.tags_json` as `Json`; `object::Model.metadata` also
decodes `TEXT` as `Option<Json>`.

These facts make a PostgreSQL JSON decode mismatch the leading candidate, not a
confirmed root cause. Confirmation requires a direct PostgreSQL store test that
shows the unredacted decode error in the pre-migration state, followed by the
same test passing when the new migration is applied. The S3 handler's redacted
error is not sufficient evidence on its own.

## Background

The repository's default `docker-compose.yml` and `config.docker.toml` are a
development stack: they use a file-backed SQLite database, mount that config,
publish Kubo ports, include Cloudflared, and retain gateway data. That is useful
for local work, but it is not a production deployment contract.

The gateway already supports PostgreSQL through SeaORM. `AppState::new` connects
to the configured database and runs all migrations before binding the listener.
`Store::db()` exposes the `DatabaseConnection`, and SeaORM 1.1.14 provides
`DatabaseConnection::ping`. The current `/health` handler always returns `200 OK`.
The runtime image intentionally contains neither `curl` nor `wget`, so a Compose
healthcheck cannot depend on either tool. Existing release validation runs live
PostgreSQL store tests and SQLite-based Compose E2E, but does not prove the
production PostgreSQL topology.

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
6. Update README production instructions and mark only the PostgreSQL production
   deployment roadmap item after the local live evidence and the static workflow
   contract test have passed. A GitHub-hosted job pass is required before merge or
   release, not before the initial implementation commit.

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

## Approaches considered

### Recommended: separate PostgreSQL Compose file

Create `docker-compose.postgres.yml` beside the default Compose file. It owns a
single PostgreSQL service, Kubo service, gateway service, named PostgreSQL data
volume, and named IPFS data volume. It starts with environment-only gateway
configuration and is selected explicitly with `-f docker-compose.postgres.yml`.

This keeps SQLite quick-start behavior stable, gives production operators a
small topology to inspect, and prevents local Cloudflared or development config
from becoming an accidental production dependency.

### Replace the default Compose file

Make PostgreSQL the default and move SQLite into a development override. This
would reduce the number of Compose files, but it breaks the established local
workflow, changes current E2E assumptions, and makes a production deployment
decision affect every contributor.

### One parameterized Compose file with profiles

Use profiles and conditional environment variables to select SQLite or
PostgreSQL. This keeps one YAML file, but it mixes incompatible storage,
secrets, port, and volume contracts. A profile mistake can silently select the
wrong backend. Separate files make the operational choice visible.

### Runtime revision: PostgreSQL JSON compatibility options

#### Recommended: a forward PostgreSQL-only compatibility migration

Add `m20260813_000001_postgres_json_columns`. Its PostgreSQL `up` path converts
`objects.metadata`, `multipart_uploads.metadata`, and
`multipart_uploads.tags_json` from `TEXT` to `JSONB`. It keeps the SQLite path
as a no-op. This matches the existing entity semantics, upgrades existing
databases instead of only fresh ones, and repairs the same latent metadata
mapping in the object path. The cost is one new latest migration, a PostgreSQL
table lock during type changes, and a fail-closed migration for any legacy text
that is not valid JSON. This is the selected approach.

#### Change entity fields to `String` and parse at every store boundary

This avoids a schema migration, but it spreads parsing and serialization across
the object and multipart stores. It can alter comparison and publication paths,
and a missed boundary would retain the failure. It is not selected.

#### Special-case only `multipart_uploads.tags_json`

This is smaller, but leaves both `metadata` columns with the same model-to-column
mismatch. It would treat the observed multipart path without correcting the
documented latent object path. It is not selected.

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

`AppState::new` continues to connect and migrate before the listener binds.
Consequently a process cannot report either endpoint until startup migration has
succeeded. Readiness is a later, independent database check for a running
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

The single gateway owns automatic migration execution. On every gateway start,
`AppState::new` connects to PostgreSQL and invokes the existing
`store::run_migrations` before it binds port 9000. PostgreSQL has no separate
migration container, init SQL, or manual migration command in this baseline.
For the one-gateway topology, this provides one migration writer and avoids an
extra lifecycle surface. It is deliberately not a design for simultaneous
gateway startup; migration leader election is deferred with multi-gateway work.

Persistent database data is retained by the named PostgreSQL volume. Production
operator documentation must never recommend `docker compose down --volumes`,
because it deletes that volume. `down --volumes` is permitted only in disposable
CI cleanup, where the project, volume, and data were created solely for that
job.

## PostgreSQL JSON compatibility migration

`m20260813_000001_postgres_json_columns` is a forward compatibility migration,
registered in `src/store/migrations/mod.rs` and appended to the `Migrator` in
the existing registration order. It becomes the latest required marker:
`m20260813_000001_postgres_json_columns`. Every production-validation query,
workflow and static-contract expectation, and the next authorised revision of
the implementation plan must use that marker. This revision changes only the
spec, so it does not edit the plan or mark any plan checkbox.

The PostgreSQL `up` path must validate every non-null source value before any
type conversion. It must reject invalid JSON with a generic migration failure
that identifies only the table and column, never a stored JSON value. PostgreSQL
17 can perform that check with a validity predicate such as
`pg_input_is_valid(value, 'jsonb')`; an equivalent check is acceptable only if
it has the same no-content, fail-closed behavior. Invalid legacy text stops
gateway startup and leaves the migration unapplied. The migration must not
replace invalid values, turn them into `NULL`, substitute an empty object or
array, or otherwise coerce or discard data.

After validation, the migration uses PostgreSQL-only `ALTER TABLE ... ALTER
COLUMN ... TYPE JSONB USING ...` statements. The nullable metadata columns use
the equivalent of `CASE WHEN metadata IS NULL THEN NULL ELSE metadata::jsonb
END`. `tags_json` uses a safe equivalent that preserves existing valid JSON and
its logically empty-array default. It remains `NOT NULL`, and its PostgreSQL
default is explicitly ` '[]'::jsonb`. The migration preserves each column's
nullability and default semantics; it does not change keys, foreign keys,
timestamps, privileges, or unrelated columns.

The PostgreSQL `down` path converts the same three columns back to `TEXT` using
the equivalent of `JSONB::text`, preserves `metadata` nulls, restores the
logical empty-array default as text for `tags_json`, and retains its `NOT NULL`
constraint. SQLite performs no schema change in either direction. The default
SQLite Compose stack, its schema diff, and its runtime behavior remain
unchanged.

Migration execution is transactional where the backend permits it. PostgreSQL
operators must account for the `ALTER TABLE` lock before deploying. A failed
validation or conversion is an explicit failed deployment, not a partial
success. Rollback is limited to the normal migration `down` operation after a
successful `up`; it serializes JSONB canonically as text and does not promise to
restore formatting or key order from pre-migration JSON text.

## TDD proof and regression coverage

Use the existing PostgreSQL-only `tests/postgres_import.rs` target rather than
adding a second PostgreSQL harness. It already serializes shared PostgreSQL
migrations and has four tests, so the final target must contain five tests.

Before adding the new migration, write one direct-store compatibility test and
run it against a fresh PostgreSQL database migrated only through
`m20260730_000001_standard_mutation_fence`. It creates a multipart upload with
the representative nullable metadata and empty tags values, then reads that
upload through the store. The expected RED result is the unredacted SeaORM/
PostgreSQL decode error at the direct store boundary. It must not assert through
the S3 handler, whose error mapping intentionally hides database detail.

After adding and registering the migration, the same isolated setup applies the
new migration and becomes GREEN. The lasting fifth test must assert all of the
following:

1. PostgreSQL reports `jsonb` for `objects.metadata`,
   `multipart_uploads.metadata`, and `multipart_uploads.tags_json`.
2. Multipart upload creation and retrieval round-trip nullable metadata and
   JSON tags through the store.
3. Multipart-part upsert succeeds after that read and creates the expected part
   row.

The final test may construct the old schema from the known pre-revision
migration chain before applying the new migration, so it preserves the direct
RED-to-GREEN proof after fresh full migrations naturally start at the new
latest marker. It must use a unique fixture namespace and retain the target's
existing serialization. The direct error output may identify a decoder or
database type mismatch but must not print stored JSON.

## Data and failure flow

1. Compose interpolates required variables. A missing required value stops
   `docker compose config` before resources are created.
2. PostgreSQL and Kubo start on the internal network and become healthy.
3. Gateway connects to PostgreSQL, applies migrations, initializes state, binds
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

Before updating README or ROADMAP for the initial implementation, the implementer
must run the complete validation sequence below locally. Every Compose command
uses one unique, disposable Compose project name, the committed disposable
validation override, the same disposable URL-safe credentials and test-only
S3/master-key environment values, the same fixed loopback validation ports, and
the same PostgreSQL-stop failure scenario as the
`postgres-production-deployment` workflow job. All blocking commands must pass
against real local Docker containers. This is local live evidence, not
GitHub-hosted evidence, and it must never be reported as a hosted workflow pass.

The static PowerShell workflow contract test must also pass locally before those
documentation updates. It verifies that the committed workflow retains the
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
5. Query PostgreSQL inside its container and require the latest registered
    migration, `m20260813_000001_postgres_json_columns`, in
    `seaql_migrations`. Also require `jsonb` for the three compatibility
    columns before running E2E.
6. Run the existing serial `cargo test --test e2e -- --nocapture
    --test-threads=1` target against the validation loopback endpoints. It must
    pass all 11 tests, including the 6 MiB multipart `UploadPart` case that
    previously returned 500.
7. Stop only the validation project's PostgreSQL service. Require `/health` to
   remain `200 OK`, then poll `/ready` for no more than ten seconds until it
   returns `503 NOT READY`.

An `always()` diagnostics step prints non-coloured logs for the project. A
following `always()` cleanup step runs `docker compose ... down --volumes
--remove-orphans` with the same project name, then verifies that no containers,
networks, or volumes with that Compose project label remain. Cleanup and the
zero-residual assertion belong only to this disposable CI stack. Failure of
diagnostics or cleanup must not hide the primary command's result.

The static PowerShell contract test runs locally before README or ROADMAP changes
and in the existing release-validation infrastructure job before hosted live
deployment validation. It checks the job is blocking, has the unique project
name, includes all checks above in order, uses the disposable override, prints
logs and cleans up on `always()`, and does not weaken the default SQLite Compose
E2E job. Its migration marker and schema assertions must change to
`m20260813_000001_postgres_json_columns` and `jsonb` for the three columns.

The new workflow job is a blocking regression for future pushes and pull
requests. Its actual GitHub-hosted pass is required before merge or release, but
cannot be an initial-commit prerequisite because the workflow does not exist on
GitHub until that change is submitted. The initial implementation report must
state the separate local live-evidence result and explicitly state that the
hosted `postgres-production-deployment` job has not yet run.

## File boundaries

Implementation may change only these boundaries, plus focused unit or contract
tests that directly cover them:

| Path | Responsibility |
| --- | --- |
| `docker-compose.postgres.yml` | Production PostgreSQL, Kubo, and gateway topology and environment contract. |
| `tests/compose.postgres-production-validation.yml` | Disposable CI-only loopback port publication. |
| `src/main.rs` | `/ready` route, readiness response, and `--ready-probe` process mode. |
| `src/store/mod.rs` | Keep `Store::db()` as the readiness dependency and append the new migration to the `Migrator` registration order. |
| `Dockerfile` | Healthcheck-compatible binary packaging only if required, with no HTTP probe package installed. |
| `.github/workflows/release-validation.yml` | Independent blocking production deployment job. |
| `tests/postgres-production-baseline.Tests.ps1` | Static Compose and workflow contract checks. |
| `src/store/migrations/m20260813_000001_postgres_json_columns.rs` | PostgreSQL-only, fail-closed JSON compatibility migration and reversible down path. |
| `src/store/migrations/mod.rs` | Declare the new migration module. |
| `tests/postgres_import.rs` | Direct PostgreSQL store RED-to-GREEN proof and final JSONB multipart compatibility regression, increasing the target from four tests to five. |
| `tests/release-validation.Tests.ps1` | Static latest-migration expectation used by the production validation workflow. |
| `README.md` | Production invocation and secret-handling guidance, only after local live Docker evidence and the static workflow contract test pass. |
| `ROADMAP.md` | Check only `PostgreSQL production deployment`, only after local live Docker evidence and the static workflow contract test pass. |

The work does not modify `docker-compose.yml`, `config.docker.toml`, database
migration files other than the named compatibility migration and its required
module and Migrator registration, credential semantics, Kubo behavior,
Cloudflared behavior, or the roadmap entries for multiple gateways, Cluster,
and private swarm. It does not modify `Dockerfile`, `tests/e2e.rs`, default
Compose behavior, or entity definitions as a broad `Json`-to-`String` rewrite.
Existing Task 1 to Task 3 boundaries remain intact. Any future plan revision
updates its marker expectation only; it must not broaden this scope or mark a
checkbox before its own work is performed.

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
7. Gateway startup still completes migrations before binding. The initial local
    live validation sequence and each subsequent hosted job run prove
    `m20260813_000001_postgres_json_columns` is applied and the three specified
    PostgreSQL columns are `jsonb`.
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
11. `postgres-production-deployment` is a blocking regression on future pushes
    and pull requests. Its actual GitHub-hosted pass is required before merge or
    release, not before the initial implementation commit. The initial report
    distinguishes local live evidence from hosted evidence and states that the
    hosted job has not yet run.
12. The compatibility migration validates all non-null legacy JSON text before
    conversion. Invalid text fails migration and gateway startup without logging
    or replacing the stored JSON value.
13. On PostgreSQL, `objects.metadata`, `multipart_uploads.metadata`, and
    `multipart_uploads.tags_json` are `JSONB`; nullable metadata remains
    nullable, and `tags_json` remains `NOT NULL` with a logically empty-array
    default. SQLite has no change in either schema or behavior.
14. The direct PostgreSQL store test first demonstrates an unredacted pre-
    migration decode failure, then passes after the migration. The final
    `postgres_import` target contains its four existing tests plus this one
    compatibility test, which verifies the three `jsonb` types, multipart
    create/get metadata and tags round-trip, and successful part upsert.
15. The migration's PostgreSQL `down` path returns the three columns to `TEXT`
    with the original nullability and logical defaults. It is not a guarantee of
    preserving JSON whitespace or object key order. SQLite `up` and `down` are
    no-ops.
16. The complete validation sequence is rerun from a clean, unique disposable
    Compose project and passes all 11 E2E tests, PostgreSQL stop and readiness
    behavior, diagnostics, and scoped cleanup. The previous 10 of 11 result
    cannot gate README or ROADMAP changes.

The `postgres-import` target, its four existing tests plus the new fifth
compatibility test, and relevant unit/regression targets must pass after the
migration is implemented. The prior 10 of 11 local E2E result is diagnostic
evidence only. README and ROADMAP remain unchanged until the complete
from-scratch local sequence, including the direct store proof, PostgreSQL test
target, full 11 of 11 E2E, PostgreSQL-stop readiness check, diagnostics, and
scoped cleanup, passes again. The hosted blocking job is still initially not
run, and must not be reported as passing until GitHub executes it.

## Git boundary

This design authorizes no version-control write. Implementation should stage no
unrelated files and must not create a commit, tag, or push without the user's
explicit permission. The implementation change set is limited to the file
boundaries above and direct tests. No test or deployment command is run as part
of writing this design. This runtime-driven revision modifies only this spec; it
does not modify the implementation plan, change plan checkboxes, or perform a
Git write.
