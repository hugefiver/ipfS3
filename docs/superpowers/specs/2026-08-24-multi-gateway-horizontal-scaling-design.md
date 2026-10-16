# Multi-Gateway Horizontal Scaling Design

**Date:** 2026-08-24

**Status:** Approved design

**Scope:** v0.5-B adds one deliberately bounded horizontal-scaling deployment: two HTTP gateway replicas behind one Nginx entry point, sharing one PostgreSQL 17 instance and one Kubo instance.

## Background and evidence

The current `docker-compose.postgres.yml` is a single-node baseline with one
gateway, one PostgreSQL 17 service, and one Kubo service. PostgreSQL and Kubo
have no published host ports, gateway readiness is `/ready`, and required
database, S3, and master-key inputs have no fallback. The default
`docker-compose.yml` remains the SQLite development topology.

`AppState::new` connects to the configured database and calls
`store::run_migrations` before the listener binds. Concurrent PostgreSQL
startup serializes migration execution with a transaction-scoped advisory lock.

Every normal gateway process starts both the existing pinning worker and the
existing durable import worker after binding. Import jobs are already durable:
they have UUID worker identities, `claim_epoch`, database-clock lease fencing,
and conditional ownership updates. Pinning work already uses exact conditional
tokens such as `locked_until`. These properties are the basis for this limited
shared-database topology; they do not turn provider scheduling into a
cluster-wide service.

The serial `tests/e2e.rs` target can point at a
gateway and Kubo endpoint using environment variables. The production baseline
already has a disposable, loopback-only validation override and a blocking
release-validation job with owned-project cleanup.

The Nginx image selected for the new load balancer is Docker Hub's active exact
tag `nginx:1.28.0-alpine`. The observed OCI index digest is
`sha256:30f1c0d78e0ad60901648be663a710bdadf19e4c10ac6782c235200619158284`.
The Compose file may use that exact tag. The digest is recorded as selection
evidence, not as a promise about a platform-specific child manifest digest.

### PostgreSQL lease SQL

`src/store/import/lease_clock.rs` compares the bound proposed lease timestamp
with `clock_timestamp()` through a structural SeaQuery expression. PostgreSQL
SQL must retain one bound value and use a numbered bind, not a literal `?`.
SQLite and MySQL retain their backend-specific lease-clock expressions.
Acceptance includes generated-SQL validation and a real completed cross-replica
CID import whose worker renews its lease.

## Supported declaration

This release supports exactly **two HTTP gateway replicas, through one Nginx
entry point, sharing one PostgreSQL 17 instance and one Kubo instance**. It
does not claim high availability for PostgreSQL, Kubo, the load balancer, or
the host. It is not a general multi-host deployment.

After the required local live evidence is recorded, README may describe this
single-entry deployment and ROADMAP may check only `Multiple gateway instances
(horizontal scaling)`. README and ROADMAP must not claim PostgreSQL, Kubo, or
load-balancer HA.

## Goals

1. Add an opt-in Compose topology that sends all client traffic through one
   Nginx endpoint and runs two identical gateway replicas against the same
   PostgreSQL and Kubo services.
2. Serialize PostgreSQL automatic migrations at concurrent gateway startup,
   without changing SQLite or MySQL migration behavior.
3. Preserve SigV4 request validity, upload and download streaming, object
   consistency, multipart completion, durable import recovery, and the current
   pinning safety fences across either gateway.
4. Prove the topology with static contracts, a real two-gateway target, the
   existing 11-test E2E target through the load balancer, and a blocking CI
   surface.
5. Keep the default SQLite Compose stack and the existing single-gateway
   PostgreSQL baseline behavior unchanged.
6. Preserve durable import lease renewal on PostgreSQL by requiring backend-valid
   numbered binds before claiming cross-replica import support.

## Non-goals

- IPFS Cluster or a private swarm.
- Master-key rotation.
- PostgreSQL HA, backup, or connection-pool tuning.
- TLS, Kubernetes, cloud resources, or load-balancer HA.
- A remote-provider distributed limiter, remote-provider cluster-wide RPS,
  concurrency, health, or fairness guarantees.
- Worker roles or a separate import or pinning worker deployment.
- Remote pinning providers in this multi-gateway Compose file.

Cross-host pinning clock skew is outside the supported topology. A remote
provider must not be represented as a cluster-coordinated resource merely
because two local gateway processes can claim durable work.

## Approaches considered

### Recommended: two identical gateways behind Nginx, with a PostgreSQL transaction lock

Create an independent Compose file with two gateway services, one Nginx
service, one PostgreSQL service, and one Kubo service. Let each gateway retain
its normal startup and worker lifecycle, but serialize its PostgreSQL migration
call with a transaction-scoped advisory lock. Nginx preserves the signed Host
header and proxies streaming bodies to either replica.

This keeps the proven application runtime intact, limits coordination to the
one startup race that is not already fenced, and gives operators one explicit
client address. It is selected.

### A dedicated migration service and worker roles

Create a one-shot migration container and split import and pinning workers from
HTTP gateways. This would reduce duplicate polling, but introduces leader
lifecycles, rollout ordering, worker authentication, and a second operational
topology. It is unnecessary for two replicas sharing one local Compose network.

### A generalized clustered deployment

Add IPFS Cluster, PostgreSQL replication, distributed provider quotas, and a
highly available load balancer. Those components solve different availability
problems and would make the support statement materially broader. They are not
part of v0.5-B.

## Architecture and components

```text
S3 client
   |
   v
explicit host bind -> load-balancer:9000 (Nginx)
                       |             |
                       v             v
                  gateway-a:9000 gateway-b:9000
                       |             |
                       +------ shared PostgreSQL 17:5432
                       |
                       +------ shared Kubo RPC:5001
```

`docker-compose.multi-gateway.yml` is independent from both existing Compose
files. Its `services` mapping contains exactly `postgres`, `kubo`,
`gateway-a`, `gateway-b`, and `load-balancer`. It contains no `container_name`,
Cloudflared service, gateway-local volume, remote-provider configuration, or
provider token. PostgreSQL retains its named database volume and Kubo retains
its named IPFS volume. Neither gateway receives a persistent local volume.

PostgreSQL, Kubo, `gateway-a`, and `gateway-b` have no `ports` publication in
the production file. Only `load-balancer` publishes port 9000, with required
`IPFS_S3_LOAD_BALANCER_BIND` and `IPFS_S3_LOAD_BALANCER_PORT` interpolation.
The bind must be an explicit non-wildcard address. Production instructions use
`127.0.0.1` unless an operator deliberately selects another specific interface.
No value falls back to a default.

The four required secret inputs are identical to the single-PostgreSQL
baseline: `POSTGRES_PASSWORD`, `IPFS_S3_ACCESS_KEY_ID`,
`IPFS_S3_SECRET_ACCESS_KEY`, and `IPFS_S3_MASTER_KEY`. Required Compose syntax
must reject absence before a container starts. Both gateway services receive the
same database URL, Kubo RPC URL, access-key ID, secret access key, and master
key. Different credentials or master keys are unsupported because they would
make authentication or encrypted-object handling replica-dependent.

Both gateways use their normal `AppState` initialization and start both normal
workers. There is no worker role switch. Import's durable UUID identity,
`claim_epoch`, and database-clock fencing remain the ownership mechanism. The
pinning worker retains its exact `locked_until` conditional-token checks. The
Compose topology configures no remote providers, so it makes no assertion about
provider RPS, concurrency, health, or fairness across replicas.

### Nginx boundary

Add `deploy/nginx/multi-gateway.conf`. It defines an upstream containing
`gateway-a:9000` and `gateway-b:9000`, and is used by the exact image
`nginx:1.28.0-alpine`.

For S3 routes Nginx must use HTTP/1.1, set `Host` to `$http_host` without
normalization, and set the normal forwarding headers without overwriting the
signed Host value. `proxy_request_buffering off`, `proxy_buffering off`, and
`client_max_body_size 0` are required so request and response bodies retain the
gateway's streaming behavior and there is no Nginx body-size cap. The proxy
configuration enables failover on connection error, timeout, and upstream HTTP
502, 503, or 504, with at most the two configured upstream attempts.

`/health` and `/ready` at the load balancer both proxy the selected gateway's
`/ready` endpoint. Thus load-balancer health and readiness have exactly the
gateway readiness meaning. Neither endpoint queries PostgreSQL, Kubo, a
provider, or a second private health endpoint directly. Nginx open-source
passive upstream failure handling is not an active health-check or HA
guarantee.

The Nginx log format may record request method, path, status, duration, and
upstream status. It must not include `Authorization`, any request header dump,
or request-body data. Nginx does not terminate TLS in this scope. Operators who
need TLS place it at a separately designed boundary and must preserve the
client's signed Host behavior.

## PostgreSQL migration coordination

`store::run_migrations` keeps the existing direct `Migrator::up(db, None)` path
for SQLite and MySQL. For PostgreSQL only, it performs this exact sequence:

1. Begin one outer database transaction.
2. Execute `SET LOCAL lock_timeout = '60s'` in that transaction.
3. Log the migration-lock waiting category, then execute
   `SELECT pg_advisory_xact_lock(1229997651, 1395879239)` in the same outer
   transaction. These two signed integers are the stable product key.
4. On acquisition, log the acquired category and call SeaORM 1.1.20
   `Migrator::up(&txn, None)` before ending the outer transaction. SeaORM's
   PostgreSQL migration path creates its nested transaction or savepoint inside
   this outer transaction.
5. Commit the outer transaction. A rollback, lock timeout, query error,
   migration error, or commit error returns an error and makes gateway startup
   fail before it binds the HTTP listener.

The timeout bounds migration-lock waiting at 60 seconds. Startup logs may say
only `waiting`, `acquired`, or a failure category such as setup, timeout,
migration, or commit. They must not log the database URL, DSN, password, or an
unredacted database error that can contain either.

The advisory lock coordinates automatic migrations from this application only.
It is not a replacement for an independent migration service, migration policy
outside this binary, or PostgreSQL HA. A deployment that runs a separate schema
writer must coordinate that writer explicitly.

## Data and error flow

1. Compose validates all required secrets and the explicit load-balancer bind
   and port. Missing input stops `docker compose config`.
2. PostgreSQL and Kubo become healthy. Both gateways start concurrently against
   an empty or existing PostgreSQL database.
3. Each gateway runs startup migration. PostgreSQL admits one outer transaction
   to the advisory lock; the other waits for no more than 60 seconds. The winner
   migrates and commits, then the follower observes the completed migration
   state, commits, and both instances bind and become ready.
4. A client sends a SigV4 request to the load balancer. Nginx retains the exact
   client Host header and streams the request to one gateway. The gateway uses
   PostgreSQL for metadata and Kubo for content as it does in the single-node
   baseline.
5. Either process can persist or resume durable import work. A stale import
   worker cannot publish after its claim epoch or database-clock lease fence
   stops matching. Pinning work follows its existing conditional state tokens.
6. If one gateway becomes unavailable, Nginx can retry the other configured
   upstream for the configured connection, timeout, 502, 503, or 504 cases.
   A request that already reached an application side effect still follows the
   existing S3 operation semantics; Nginx retry is not an exactly-once API.
7. If PostgreSQL or Kubo fails, the shared single point fails both replicas'
   dependent operations. Gateway `/ready` still has its documented
   database-only meaning. This release does not mask that failure with HA.

## Security and operational boundaries

Secrets remain Compose environment values and can be visible to principals who
can inspect the host, Docker daemon, process environment, or composed service.
Use least-privilege host and Docker access, do not publish generated Compose
configuration into logs, and keep production LB exposure on a deliberate
non-wildcard interface. This design does not add a secret manager or master-key
rotation procedure.

There is no session stickiness. State is shared through PostgreSQL and Kubo, so
a valid request may reach either replica. Multipart and import flows are
explicitly tested across replicas. One PostgreSQL instance, one Kubo instance,
one Nginx process, and their host remain single points of failure.

The production Compose file must never recommend direct gateway, PostgreSQL, or
Kubo host ports. Direct ports exist only in the disposable validation override
for test observation. They bind only to loopback and are not an operator API.

## Acceptance

Static tests don't start Docker. Live tests use a unique disposable Compose
project and real containers.

### Static contracts

Add `tests/multi-gateway.Tests.ps1`. It reads the Compose, Nginx, workflow, and
test source as text and rejects a widened topology. It requires the exact
five-service Compose set, exact Nginx image tag, observed index-digest evidence
in this design, required no-fallback secret syntax, equal gateway secret and
backend settings, no Cloudflared, no gateway volume, no provider settings, no
published production ports except the explicit non-wildcard load-balancer
mapping, and no direct production mapping for PostgreSQL, Kubo, or either
gateway.

It also requires the Nginx upstream names, Host preservation, HTTP/1.1,
unbuffered request and response handling, unlimited body size, the five
connection and response failover classes, two-attempt bound, proxied readiness,
and a log format that excludes Authorization and request-body data. It locks the
PostgreSQL-only migration sequence, the exact advisory-lock key pair, the
60-second local lock timeout, outer transaction use, fail-closed startup, and
the unchanged SQLite and MySQL path.

The unit and source contracts also lock PostgreSQL import renewal SQL: the
proposed lease timestamp is a bound value in a structural comparison with
`clock_timestamp()`, PostgreSQL SQL contains a numbered bind and no literal
question mark, and the existing SQLite lease normalization remains green.

The static contract requires `tests/compose.multi-gateway-validation.yml` to
publish only these loopback mappings:

| Service | Host port | Container port | Use |
| --- | ---: | ---: | --- |
| `gateway-a` | 59001 | 9000 | Direct replica verification only. |
| `gateway-b` | 59002 | 9000 | Direct replica verification only. |
| `load-balancer` | 59000 | 9000 | Client and existing E2E entry point. |
| `kubo` | 55002 | 5001 | CID import fixture and content observation. |
| `postgres` | 55434 | 5432 | Migration-marker query only. |

The test must reject wildcard host binds, changed ports, production references
to the override, and a validation override that adds any service. It also locks
the exact live scenarios below and the required cleanup order.

### Live target and workflow parity

Add `tests/multi_gateway.rs`. It uses the loopback direct A and B endpoints,
the load-balancer endpoint, Kubo endpoint, PostgreSQL endpoint, existing
test-only credentials, bounded request timeouts, and unique bucket/key/job
names. The corresponding local workflow and hosted job must perform all of the
following in order:

1. Save required environment values, remove each required secret in turn,
   require `docker compose config` to fail, and restore exact presence and value
   in a `finally` path. With all values restored, require `config --quiet` to
   pass.
2. Preflight a unique Compose project and the five fixed loopback ports. Refuse
   to run if resources bearing that project label already exist.
3. Start PostgreSQL and Kubo, then start `gateway-a` and `gateway-b`
   concurrently against an empty PostgreSQL database. Require both direct
   `/ready` endpoints to return `200 READY`.
4. Query `seaql_migrations` through the loopback PostgreSQL mapping. Require
   every registered migration marker exactly once, including:
   `m20250701_000001_init`, `m20260707_000001_decompress_zip`,
   `m20260720_000001_sse_c_key_fingerprint`,
   `m20260721_000001_multi_provider_pinning`,
   `m20260729_000001_ipfs3_import`,
   `m20260729_000002_postgres_utc_timestamps`,
   `m20260730_000001_standard_mutation_fence`, and
   `m20260813_000001_postgres_json_columns`.
5. Through direct endpoints, create a bucket and object on A, then read, list,
   head, and delete that object on B.
6. Start multipart upload and upload one part through A, upload a later part
   and complete through B, then retrieve and verify the completed object.
7. Race A and B writing different complete payloads to the same key. The final
   object must equal one whole submitted payload, never a mixture. Direct A and
   B reads must return identical body, CID, and ETag for that final state.
8. Run the existing serial `cargo test --test e2e -- --nocapture
   --test-threads=1` target with its gateway endpoint set to loopback port 59000
   and its Kubo endpoint set to port 55002. Every selected E2E case must pass through
   the load balancer.
9. Submit a CID import through A, observe its persisted status through B, and
   require the job to reach its completed result through B.
10. Capture project logs before stopping any service. Stop only gateway A,
    require gateway B's direct readiness to remain `200 READY`, then require
    load-balancer readiness to recover through B within 30 seconds. After that,
    perform a new load-balancer bucket create, write, read, list, and delete
    sequence successfully through B's surviving path.
11. On every attempted live run, emit non-coloured project logs before teardown.
    Tear down only the owned unique project with volumes and orphans removed,
    then query Docker labels and require zero residual containers, networks, and
    volumes for that project.

The hosted release-validation workflow gains an independent blocking job named
`multi-gateway-deployment`, or an equally independent blocking surface with the
same contract. It runs on Ubuntu, uses a unique project name based on GitHub run
ID and attempt, disables implicit `.env` loading, and uses the loopback override
only for disposable verification. `tests/multi-gateway.Tests.ps1` runs in the
existing infrastructure job. Existing PostgreSQL single-baseline and SQLite E2E
job meaning must not change.

Support claims require the full local workflow-parity sequence, including
static contracts, live cases, diagnostics and zero-residual cleanup. Hosted
execution is reported separately: a job not executed is `NOT RUN`, and an
actual hosted pass is required before merge or release.

## Documentation contract

After local workflow parity passes, README documents one Nginx entry point, no
stickiness, identical shared credentials and master key, shared PostgreSQL and
Kubo state, the absence of remote providers in this topology, direct-port
restrictions, and the remaining single points of failure. ROADMAP changes only
the current unchecked horizontal-scaling line. It does not check IPFS Cluster
or private swarm.

## Component responsibilities

| Path | Responsibility |
| --- | --- |
| `docker-compose.multi-gateway.yml` | Opt-in five-service production topology and required environment contract. |
| `deploy/nginx/multi-gateway.conf` | Nginx upstream, streaming proxy, failover, readiness proxy, and safe logging. |
| `tests/compose.multi-gateway-validation.yml` | Disposable loopback-only direct and observation ports. |
| `src/store/mod.rs` | PostgreSQL transaction-scoped migration coordination while preserving SQLite and MySQL behavior. |
| `src/store/import/lease_clock.rs` | PostgreSQL-safe numbered bind generation for import lease renewal, with SQLite and MySQL expressions unchanged. |
| `tests/postgres_import.rs` or focused store tests | PostgreSQL migration-lock success, timeout, and no-secret-log regressions. |
| `tests/multi_gateway.rs` | Real two-replica consistency, import, failover, and migration-marker checks. |
| `tests/multi-gateway.Tests.ps1` | Static Compose, Nginx, migration, test, and workflow contract. |
| `.github/workflows/release-validation.yml` | Independent blocking multi-gateway live-validation job. |
| `tests/release-validation.Tests.ps1` | Exact workflow-job and blocking-contract expectation updates. |
| `README.md` | Supported deployment description, only after local parity passes. |
| `ROADMAP.md` | One horizontal-scaling checkbox, only after local parity passes. |

Do not modify `docker-compose.yml`, `docker-compose.postgres.yml`, existing
SQLite E2E semantics, existing single-PostgreSQL validation semantics, provider
implementations, import or pinning ownership semantics, Kubernetes or cloud
files. The default and single-gateway profiles retain their separate behavior.

## Acceptance criteria

The implementation is accepted only when the supported declaration is true in
the committed artifacts and the full static and live sequence above passes. In
particular, it must prove concurrent empty-database startup, one occurrence of
each registered migration marker, cross-replica CRUD and multipart behavior,
whole-payload concurrent-write convergence, the complete E2E suite through
Nginx, cross-replica import observation, bounded one-replica failover, safe
diagnostics, exact environment restoration, and zero residual disposable
resources. The result report separates local evidence from hosted evidence and
does not claim HA or a capability outside this document's support statement.
Cross-replica import acceptance requires backend-valid PostgreSQL renewal SQL
and a real completed CID import; a queued or running timeout isn't acceptance.
