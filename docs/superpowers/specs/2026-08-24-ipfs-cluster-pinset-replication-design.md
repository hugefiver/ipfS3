# IPFS Cluster Pinset Replication Design

**Status:** Approved design
**Date:** 2026-08-24
**Roadmap scope:** v0.5 — `IPFS Cluster for pinset replication`

## Summary

Add an opt-in deployment profile in which one gateway reaches IPFS through the
Kubo-compatible proxy of a two-peer IPFS Cluster. Each full Cluster peer owns a
separate Kubo daemon and persistent identity. The Cluster pinset is configured
for two allocations, so a gateway-created CID is eventually pinned by both Kubo
peers.

This profile proves pinset replication. It does not combine the existing
multi-gateway profile with independent Kubo peers, and it does not claim high
availability. Immediate reads are guaranteed only through the gateway's local
Cluster/Kubo path. A second Kubo is a supported read source only after Cluster
reports two allocations and both peer tracker states as physically `pinned`.

S3 deletion keeps the existing retention policy: metadata is removed, but no
Cluster unpin is requested. Replicated content therefore remains pinned.

## Evidence and Decision Context

### Existing repository behavior

- `AppState` contains one `KuboClient`, and all reads use that client's `cat`
  path.
- Kubo `add` is sent with `pin=false`; object, multipart, ZIP, and import flows
  subsequently issue `pin/add` before database publication.
- Production code has no `pin/rm` caller. S3 deletion and multipart abort remove
  database state without releasing local Kubo pins.
- The remote pinning provider lifecycle may eventually call `unpin`, and it does
  not include multipart parts. It therefore cannot safely represent the same
  physical Cluster pinset without a new retained-reference lifecycle.
- The existing multi-gateway profile deliberately shares one Kubo. It proves
  gateway horizontal scaling, not Kubo replication.

### Versioned external contract

- Use two full `ipfs-cluster-service` peers from IPFS Cluster v1.1.6, released
  2026-05-11:
  <https://github.com/ipfs-cluster/ipfs-cluster/releases/tag/v1.1.6>.
- Use the versioned image
  `ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd`.
- Use CRDT consensus. CRDT is the recommended eventual-consistency mode; Raft
  is unsuitable for a two-peer baseline because losing one peer loses its
  majority:
  <https://ipfscluster.io/documentation/guides/consensus/>.
- Every Cluster peer is paired with a separate Kubo daemon through
  `ipfs_connector.ipfshttp.node_multiaddress`:
  <https://ipfscluster.io/documentation/deployment/architecture/>.
- Cluster membership uses one shared 32-byte hex secret and separate persistent
  identities:
  <https://ipfscluster.io/documentation/deployment/setup/> and
  <https://ipfscluster.io/documentation/deployment/bootstrap/>.
- The Kubo-compatible proxy intercepts Kubo add and pin operations and forwards
  ordinary requests such as `cat`:
  <https://ipfscluster.io/documentation/reference/proxy/>.
- `/health` is an unauthenticated `204` endpoint. Allocations and peer tracker
  status are separate from mutation acceptance:
  <https://ipfscluster.io/documentation/reference/api/>.
- `replication_factor_min` is the minimum healthy allocation count and
  `replication_factor_max` is the target. The acceptance topology uses `2/2`,
  intentionally trading degraded write availability for deterministic evidence:
  <https://ipfscluster.io/documentation/guides/pinning/>.

There is no official Kubo/Cluster compatibility matrix. The selected Kubo
v0.43.0 and the repository's exact add, pin, and cat request shapes must pass a
real compatibility test before this profile is documented as supported.

### Runtime revision: Cluster version representation

The first live topology gate observed the official v1.1.6 image reporting its
version as `1.1.6+git<commit>`. This is SemVer build metadata, not a different
release. Version validation therefore requires the exact `1.1.6` core and accepts
only an optional nonempty, syntactically valid `+build.metadata` suffix. Other
patch versions and all pre-release forms remain terminal contract errors. Logs
and test output report only the normalized core, never the commit metadata.

### Runtime revision: proxy Kubo endpoint

The first post-version-fix proxy compatibility run proved that
`CLUSTER_IPFSHTTP_NODEMULTIADDRESS` configures the Cluster IPFS connector but not
the Kubo-compatible proxy's forwarding target. The v1.1.6 proxy has a separate
`api.ipfsproxy.node_multiaddress` setting whose default is
`/ip4/127.0.0.1/tcp/5001`; inside a Cluster container that address has no Kubo.
Each peer must therefore set `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` to the same
paired Kubo DNS endpoint as its connector. Production keeps the proxy internal;
the validation-only Cluster A proxy remains loopback-published for the direct
wire-compatibility test.

## Considered Approaches

### 1. Separate Cluster-proxy deployment — selected

Point the gateway's existing Kubo RPC URL at Cluster peer A's Kubo-compatible
proxy. The proxy becomes the pin authority for this topology, while the gateway
keeps its existing add, pin, and cat code. Both multipart parts and completed
objects pass through the same proxy, avoiding a second application lifecycle.

This is the smallest design that preserves current application interfaces and
the intentional no-unpin retention policy.

### 2. Implement Cluster as a `PinningProvider` — rejected

The durable provider worker could submit and poll Cluster pins, but the existing
provider lifecycle can physically unpin after the last provider lease expires.
That is unsafe when the same Kubo pin is also treated as a retained local pin.
The provider path also excludes multipart parts. Correcting both issues requires
a new retained-provider lifecycle and broader multipart semantics.

### 3. Add an application replication barrier and read fallback — rejected

Replacing publication with synchronous two-peer completion, routing reads across
Kubos, and handling partial replication would create a distributed storage
subsystem. It would also make `2/2` placement part of S3 write availability. That
work is outside this roadmap item.

## Supported Deployment Contract

The supported topology contains exactly these services:

1. `postgres` — PostgreSQL 17 metadata database.
2. `kubo-a` — Kubo v0.43.0 with its own repository volume.
3. `kubo-b` — Kubo v0.43.0 with a different repository volume.
4. `cluster-a` — full IPFS Cluster v1.1.6 CRDT peer attached to `kubo-a`.
5. `cluster-b` — full IPFS Cluster v1.1.6 CRDT peer attached to `kubo-b`.
6. `gateway` — one normal gateway attached to PostgreSQL and to
   `http://cluster-a:9095` as its Kubo RPC URL.

The profile is same-host Docker Compose. The Cluster peers may use the official
Docker-network mDNS discovery behavior, with separate identities persisted in
their own volumes. Operators and validation must wait for exactly two visible
Cluster peers before accepting writes. This is not a deterministic multi-host
bootstrap claim.

Both Cluster peers use CRDT with replication minimum and maximum set to two.
Each peer configures both its IPFS connector and its Kubo-compatible proxy to
the same paired Kubo DNS endpoint; neither may use the container-local default
`127.0.0.1:5001`.
Kubo garbage collection remains disabled. The production profile does not
publish PostgreSQL, Kubo API, Cluster REST, Cluster proxy, or Cluster swarm
ports. Only the gateway S3 port is published, through a required explicit
non-wildcard host bind and required port.

## Files and Responsibilities

### New files

- `docker-compose.cluster.yml` — production six-service Cluster topology.
- `ipfs/cluster.Dockerfile` — Kubo image for this profile, based on exact
  `ipfs/kubo:v0.43.0` and reusing the repository Kubo entrypoint without changing
  existing image contracts.
- `tests/compose.cluster-validation.yml` — disposable loopback-only port
  publication for live validation.
- `tests/cluster.Tests.ps1` — dependency-free static deployment and workflow
  contract.
- `tests/cluster.rs` — real S3, Cluster REST, and Kubo replication checks.
- `tests/support/cluster.rs` only if the live target would otherwise mix HTTP
  parsing, polling, and scenario logic in one oversized file.
- `docs/superpowers/plans/2026-08-24-ipfs-cluster-pinset-replication.md` — the
  reviewed implementation plan.

### Modified files

- `.github/workflows/release-validation.yml` — add one independent blocking
  Cluster deployment job.
- `tests/release-validation.Tests.ps1` — lock the new job and updated static
  command order.
- `tests/postgres-production-baseline.Tests.ps1` and
  `tests/multi-gateway.Tests.ps1` — update only shared workflow job-count and
  infrastructure-command contracts while preserving their existing job bodies.
- `README.md` — document the opt-in topology and its limits after live evidence.
- `ROADMAP.md` — check only `IPFS Cluster for pinset replication` after all
  local gates pass.

No application source change is planned. If the real proxy rejects or changes
the repository's current Kubo request/response shapes, implementation stops and
this design is revised before any application compatibility code is added.

## Configuration and Secret Handling

`docker-compose.cluster.yml` requires all of the following with `${VAR:?message}`
interpolation and no tracked fallback:

- `POSTGRES_PASSWORD` — URL-safe unreserved password.
- `IPFS_S3_ACCESS_KEY_ID`.
- `IPFS_S3_SECRET_ACCESS_KEY`.
- `IPFS_S3_MASTER_KEY` — nonzero 64-hex-character key, identical for the life of
  stored encrypted objects.
- `IPFS_S3_CLUSTER_SECRET` — 64 hex characters representing the Cluster
  membership secret.
- `IPFS_S3_GATEWAY_BIND` — explicit non-wildcard host bind.
- `IPFS_S3_GATEWAY_PORT`.

The Cluster secret is injected as `CLUSTER_SECRET` into both Cluster peers. Each
peer generates and persists a distinct identity in a separate volume. No
identity, peerstore, or secret is committed.

The REST and proxy endpoints are unauthenticated only inside the unexposed
Compose network. The validation override may publish REST and Kubo APIs on
loopback for disposable testing. Any non-loopback REST/proxy exposure requires
TLS and authentication, but implementing that exposure is outside this design.
`CLUSTER_SECRET` protects Cluster membership; it does not create a private Kubo
swarm.

## Data Flow

### Write and replication

1. The client submits an S3 write to `gateway`.
2. The gateway sends its existing Kubo `add` request with `pin=false` to
   `cluster-a:9095`.
3. The gateway sends its existing `pin/add` request to the same proxy.
4. Cluster commits the CID to the CRDT pinset with two target allocations and
   drives physical pinning on `kubo-a` and `kubo-b`.
5. The gateway publishes S3 metadata through the existing PostgreSQL transaction
   only after its current Kubo calls succeed.
6. An immediate S3 GET uses the same proxy and is forwarded to `kubo-a`.
7. Cross-Kubo availability is accepted only after Cluster reports exactly two
   distinct allocations and both peer tracker states as `pinned`.

A successful proxy or REST mutation response is not replication evidence. The
allocation and physical tracker checks are mandatory.

### Read

Normal S3 reads use Cluster A's proxy, which forwards `cat` to `kubo-a`. The
profile has no application read fallback. `kubo-b` is read directly only in
validation after physical replication is proven.

### Delete

S3 delete removes or supersedes database metadata using existing behavior. It
does not send Cluster `DELETE /pins/{cid}` or Kubo `pin/rm`. A subsequent S3 HEAD
returns not found, while the Cluster allocation and Kubo-B content remain. This
is retained pinning, not lifecycle reclamation.

## Failure Semantics

- Before two Cluster peers converge, a `2/2` pin cannot satisfy the deployment
  contract. The operation must fail rather than silently publish an
  under-replicated object.
- Cluster mutation acceptance without two physical `pinned` states is treated as
  pending, never as successful replication.
- When `cluster-b` or `kubo-b` is stopped, the design expects the topology to stop
  demonstrating two healthy physical pins. It does not require a specific
  undocumented tracker status string.
- Existing pinned content remains readable through the surviving local path, but
  the profile makes no degraded-write or HA guarantee.
- Restarting peer B with its original Cluster and Kubo volumes must restore the
  two-peer `pinned` state without re-uploading the S3 object.
- PostgreSQL, gateway, Cluster A, Kubo A, and the Docker host remain single
  failure points for their respective responsibilities.
- Logs must classify failures without emitting PostgreSQL DSNs, S3 credentials,
  the master key, the Cluster secret, authorization headers, request bodies, or
  generated peer identities.

## Validation Design

### Static contract

`tests/cluster.Tests.ps1` locks:

- exact six-service topology and separate persistent volumes;
- exact Cluster v1.1.6 image tag/digest and Kubo v0.43.0 base;
- two full CRDT peers, two distinct Kubo endpoints, and `2/2` replication;
- exact paired-Kubo targets for both the Cluster connector and proxy forwarding;
- required interpolation and 64-hex Cluster-secret validation;
- only the gateway production port is published;
- absence of `container_name`, cloudflared, remote pinning providers, shared Kubo
  data, tracked identities, and development secret defaults;
- gateway Kubo URL points to Cluster A's proxy;
- validation ports are loopback-only and non-conflicting;
- existing add, pin, and cat source interfaces remain unchanged;
- release workflow isolation, ownership, cleanup, and static-command ordering;
- existing default, PostgreSQL, and multi-gateway Compose files remain unchanged.

### Disposable validation ports

`tests/compose.cluster-validation.yml` publishes only loopback ports:

- gateway: `59100`;
- PostgreSQL: `55435`;
- Kubo A/B APIs: `55100` and `55101`;
- Cluster A/B REST APIs: `59101` and `59102`.

Cluster proxy port `9095` remains internal unless the implementation plan proves
a direct proxy probe is necessary and assigns a separate loopback-only test port.

### Real Rust scenarios

All HTTP operations are bounded and parse the actual v1.1.6 response shapes.
`tests/cluster.rs` verifies:

1. Both Cluster `/health` endpoints return exactly `204`.
2. Both REST APIs converge on exactly two distinct Cluster peers and report the
   exact `1.1.6` release core, allowing only valid SemVer build metadata.
3. S3 PUT through the gateway succeeds and immediate gateway GET returns exact
   bytes.
4. The CID reaches exactly two allocations and both peer tracker states become
   `pinned` within a bounded interval.
5. Direct Kubo A and Kubo B `cat` each return the exact original bytes.
6. S3 DELETE makes S3 HEAD return not found while the Cluster allocation and
   Kubo-B content remain.
7. Stopping `cluster-b` and `kubo-b` removes the evidence of two healthy physical
   pins without claiming service HA.
8. Restarting both services with the same volumes restores two physical
   `pinned` states and Kubo-B `cat` without re-upload.

The first live run also proves that the existing Kubo add NDJSON, `pin/add`
response, and forwarded `cat` response are compatible with the Cluster proxy.

### Local workflow-parity gate

The local validation run:

1. Sets `COMPOSE_DISABLE_ENV_FILE=1` and never reads or modifies `.env`.
2. Uses a unique Compose project and verifies project-labeled containers,
   networks, and volumes are absent before ownership.
3. Verifies all validation ports are free.
4. Confirms each required secret/interpolation fails closed when absent, then
   confirms full configuration succeeds.
5. Builds and starts exactly the six services, waits for health and two-peer
   convergence, and runs the Rust scenarios.
6. Captures Cluster, Kubo, PostgreSQL, and gateway logs before cleanup.
7. Uses `down --volumes --remove-orphans` only for the owned disposable project.
8. Requires cleanup command success, zero residual project resources, and exact
   restoration of every process environment variable.

Production documentation must not recommend `down --volumes`.

## CI and Documentation Gates

Add an independent blocking `cluster-pinset-replication` job to
`.github/workflows/release-validation.yml`. It mirrors the local project,
environment, ports, commands, health checks, failure/restart scenario, logging,
and cleanup. Existing release jobs remain behaviorally unchanged except for the
shared job count and infrastructure static-command list.

On the initial local implementation, the hosted job is reported honestly as
`NOT RUN`. A hosted PASS is required after push or pull request and before merge
or release, but this session has no push authorization.

README and ROADMAP updates occur only after the local workflow-parity run, static
contracts, and full regressions pass. README states the exact supported topology,
eventual replication boundary, retained pins, `2/2` availability cost, internal
API requirement, and all single points of failure. ROADMAP checks only
`IPFS Cluster for pinset replication`; `Private IPFS swarm` remains unchecked.

## Regression Gates

Required final checks include:

- Cluster, release-validation, PostgreSQL production, multi-gateway, and
  client-smoke PowerShell contracts;
- the Cluster Rust target and existing PostgreSQL/multi-gateway targets;
- `cargo test --lib`;
- `cargo test --test integration`;
- `cargo check --all-targets`;
- `cargo fmt --all -- --check`;
- `cargo clippy --all-targets -- -D warnings`;
- LSP diagnostics on changed Rust files;
- `git diff --check` and exact changed-path audit.

## Non-goals

- A `ClusterProvider` in `src/pinning/`.
- Cluster-driven unpin, reference reclamation, or garbage collection.
- Synchronous replication before S3 publication.
- Application read fallback across Kubo peers.
- Combining this profile with the two-gateway load-balanced profile.
- Private Kubo swarm configuration.
- PostgreSQL, Kubo, Cluster, gateway, or host high availability.
- Degraded writes with one Cluster peer unavailable.
- TLS or REST/proxy authentication implementation.
- General multi-host bootstrap or orchestration.
- Distributed remote-provider rate limiting.
- Kubernetes, cloud resources, key rotation, or unrelated refactoring.

## Acceptance Criteria

The roadmap item is complete only when all of the following are true:

1. The independent six-service Compose profile satisfies the static contract and
   leaves all existing deployment profiles unchanged.
2. Required credentials and Cluster membership secret have no tracked fallback.
3. Two full CRDT peers with separate identities and separate Kubo repositories
   converge in the disposable environment and report the exact `1.1.6` release
   core with no pre-release version.
4. The existing gateway Kubo request shapes work through the Cluster proxy
   without application source changes.
5. A gateway-created CID has exactly two allocations and both peers report
   physical `pinned` state.
6. Kubo A and Kubo B serve identical content after replication.
7. S3 deletion removes metadata but preserves the Cluster pin and Kubo-B content.
8. Peer-B stop/restart demonstrates loss and recovery of the two-peer physical
   pin state using persistent volumes.
9. Local cleanup has zero residual resources and restores all environment state.
10. Static contracts and full Rust quality gates pass.
11. README describes only the evidence-backed bounded profile, and ROADMAP changes
    only the Cluster item.
12. The current working-tree artifact receives the required identity-bound final
    review receipts before the authorized commit, unless the user explicitly
    overrides a disclosed review-system blocker. No push or tag occurs.

## Git Boundary

The implementation may change only the files listed in **Files and
Responsibilities**, plus this specification. Any proxy incompatibility or need
for application source changes stops execution and requires a design revision.
Implementation subagents do not stage, commit, push, or tag. The orchestrator may
commit the integrated task only under the user's existing per-task commit
authorization after the applicable acceptance gate; push and tag are not
authorized.
