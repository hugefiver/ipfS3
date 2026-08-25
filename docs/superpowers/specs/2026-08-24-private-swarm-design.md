# Private IPFS Swarm Design

**Status:** Approved design
**Date:** 2026-08-24
**Roadmap scope:** v0.5-D, private IPFS swarm for the Cluster profile

## Summary

Extend the existing same-host IPFS Cluster Compose profile so its two Kubo
daemons form one private libp2p swarm. The supported deployment is exactly one
gateway, PostgreSQL 17, two Kubo v0.43.0 daemons, two IPFS Cluster v1.1.6 peers,
and one one-shot bootstrap service. Cluster remains CRDT with replication
factor `2/2` and remains eventually consistent.

This work makes Kubo peer admission depend on a shared swarm pre-shared key
(PSK). It does not add high availability, block container egress, or turn the
Cluster membership secret into a Kubo private-network key. A successful
Cluster pin mutation remains insufficient evidence of replication. The
existing allocation and physical `pinned` checks remain required.

## Context and Decision

`CLUSTER_SECRET` authenticates IPFS Cluster membership only. It is not read by
Kubo and does not enable libp2p private-network mode. Kubo private networking
requires the repository-local `swarm.key` PSK file plus
`LIBP2P_FORCE_PNET=1`. Both mechanisms are needed in this profile for their
separate purposes.

### Considered approaches

1. **Extend the existing Cluster profile, selected.** Add a Cluster-specific
   Kubo entrypoint, a required Compose secret, and a bootstrap service to
   `docker-compose.cluster.yml`. This preserves the established one-gateway,
   two-Kubo Cluster contract and confines the new behavior to its opt-in
   profile.
2. **Copy the profile, rejected.** A second near-identical Compose topology
   would duplicate the existing pinset, proxy, validation, and workflow
   contracts. The copies would drift and would make it unclear which topology
   v0.5 supports.
3. **Modify the shared Kubo entrypoint, rejected.** The default, single-PG,
   and multi-gateway profiles must retain their present startup behavior. A
   private-swarm requirement belongs only to the Cluster image.

The selected approach is deliberately bounded: it changes Cluster-profile
infrastructure, validation, and documentation without changing Cargo files,
Rust application behavior, or the shared Kubo entrypoint.

## Supported Deployment Contract

The production profile is **same-host Docker Compose only** and contains these
seven services:

1. `postgres`, PostgreSQL 17 for gateway metadata.
2. `kubo-a`, Kubo v0.43.0 with its own persistent repository volume.
3. `kubo-b`, Kubo v0.43.0 with a different persistent repository volume.
4. `swarm-bootstrap`, a one-shot internal service.
5. `cluster-a`, IPFS Cluster v1.1.6 CRDT peer paired with `kubo-a`.
6. `cluster-b`, IPFS Cluster v1.1.6 CRDT peer paired with `kubo-b`.
7. `gateway`, one unchanged gateway using Cluster A's internal Kubo-compatible
   proxy.

`swarm-bootstrap` starts only after `kubo-a` and `kubo-b` are healthy. Both
Cluster services require `swarm-bootstrap` to complete successfully, as well
as retaining their paired-Kubo readiness dependency. The gateway continues to
depend on PostgreSQL and Cluster A health. This order ensures that Cluster
membership begins only after the Kubo swarm has its explicit persistent peer
relationship.

The profile retains CRDT and `replication_factor_min=2` and
`replication_factor_max=2`. Two allocations are an eventual target, not a
synchronous S3 publication barrier. A stopped peer can therefore remove the
evidence required by this profile, and this design makes no degraded-write or
high-availability promise.

Production publishes only the existing gateway S3 port, with its fixed
loopback host-bind guard. It must not publish Kubo TCP 4001, Kubo API 5001,
Kubo Gateway 8080, Cluster REST 9094, Cluster proxy 9095, PostgreSQL, or any
other Cluster or libp2p port. Cluster gateway binding remains the existing
fixed loopback behavior.

## Compose and Image Changes

`docker-compose.cluster.yml` adds a top-level `swarm_key` secret whose `file`
is required through `${IPFS_S3_SWARM_KEY_FILE:?IPFS_S3_SWARM_KEY_FILE is required}`.
`kubo-a` and `kubo-b` each mount that same Compose secret at
`/run/secrets/swarm_key` and set:

```text
IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key
LIBP2P_FORCE_PNET=1
```

There is no raw swarm-key environment variable, tracked fallback, or secret
default. Compose secrets take precedence over any raw environment mechanism.

`ipfs/cluster.Dockerfile` remains based on `ipfs/kubo:v0.43.0`, but copies and
executes `ipfs/private-swarm-entrypoint.sh`. It no longer selects
`ipfs/entrypoint.sh`. The shared `ipfs/entrypoint.sh` remains byte-for-byte
unchanged for this work.

### Cluster-only private entrypoint

`ipfs/private-swarm-entrypoint.sh` is a fail-closed shell wrapper for the
Cluster image only. It must not emit the key, its contents, a digest, or a
command line containing the key. Its required sequence is:

1. Require a readable `IPFS_SWARM_KEY_FILE` and require
   `LIBP2P_FORCE_PNET` to be exactly `1`.
2. Read and validate the source as strict UTF-8, LF-only, no-BOM text with
   exactly three lines. The lines must be, in order, the literal
   `/key/swarm/psk/1.0.0/`, the literal `/base16/`, and exactly 64 lowercase
   hexadecimal characters. Extra bytes, CRLF, a missing final LF, an empty
   line, uppercase hex, or any other formatting is terminal.
3. Initialize and apply the Cluster-specific Kubo configuration when the
   repository is new. Configuration errors are terminal, unlike the shared
   entrypoint's permissive compatibility behavior.
4. On every start, after initialization and configuration, install the
   validated source at `$IPFS_PATH/swarm.key` with mode `0400` before starting
   Kubo. Replacing the file each start prevents a persisted volume from
   retaining a stale key.
5. Enforce `Bootstrap=[]`, `Routing.Type=none`, `Discovery.MDNS.Enabled=false`,
   `AutoConf.Enabled=false`, `Swarm.AddrFilters=[]`, and an `Addresses.Swarm`
   list containing only `/ip4/0.0.0.0/tcp/4001`. The `server` init profile
   installs RFC1918 dial filters, including Docker bridge ranges, so the
   Cluster-only wrapper must clear those filters on every start. The PSK and
   internal-only Swarm port remain the membership boundary. Disabling AutoConf is mandatory because Kubo
   v0.43 refuses to use its default mainnet AutoConf URL when a private-network
   key is present. Preserve the existing internal API and Gateway behavior
   required by the Cluster profile. Start the daemon with garbage collection
   disabled.

The wrapper must fail before daemon readiness on any validation, installation,
or configuration failure. It returns nonzero with exactly the fixed, redacted
message `private swarm startup rejected`, rather than exposing which key check
failed. It must not silently start a public swarm daemon. Kubo's
repository-key file, shell temporary files, and copied secret must never be
logged.

## Bootstrap and Peer Data Flow

`swarm-bootstrap` is a one-shot, internal-only service. Once both Kubo health
checks pass, it uses their internal Kubo APIs or `ipfs` CLI endpoints to obtain
each peer ID. It then adds the other Kubo's TCP 4001 multiaddress as a
persistent Peering entry on each daemon and connects both directions.

The service must wait with bounded retries for the required private peer
relationship. Kubo v0.43 `swarm peers` has no quiet option and emits a full
peer multiaddress. The bootstrap therefore succeeds only when each daemon
reports exactly one nonempty peer line ending in `/p2p/<the-other-ID>` and both
persistent Peering entries exist. Its success state is
represented only by its Compose completion code. It exposes no ports, writes no
tracked state, and emits no peer IDs, multiaddresses, secret material, or
request bodies in normal logs.

Any read, ID discovery, Peering write, connect, or verification failure prints
exactly `private swarm bootstrap failed` and exits nonzero. There is no fallback to
mDNS, public bootstrap peers, DNS discovery, a random peer, or a partially
configured Cluster startup.

At runtime the data path is unchanged after membership is established:

```text
S3 client -> gateway -> Cluster A proxy -> Kubo A
                                   |
                                   +-> Cluster CRDT pinset -> Kubo A and Kubo B
```

The private PSK gates the Kubo A/B libp2p connection. Cluster still drives
pinset replication after the gateway's existing add and pin operations. Normal
S3 reads still use Cluster A's proxy and Kubo A. There is no application read
fallback to Kubo B. S3 deletion still removes metadata without Cluster unpin or
Kubo `pin/rm`, so replicated content remains retained under the existing
policy.

## Failure and Security Semantics

| Condition | Required result |
|---|---|
| Missing secret file or `LIBP2P_FORCE_PNET` not `1` | Private wrapper exits nonzero before daemon readiness with a fixed redacted error. |
| Invalid PSK structure or file encoding | Private wrapper exits nonzero; no daemon starts. |
| AutoConf remains enabled with its default mainnet URL | Private wrapper configuration is incomplete and Kubo must not become ready. The tracked wrapper explicitly sets `AutoConf.Enabled=false`. |
| `server`-profile RFC1918 address filters remain active | Docker bridge dials are rejected before pnet negotiation. The tracked wrapper explicitly sets `Swarm.AddrFilters=[]` for this internal-only profile. |
| Kubo A/B use different PSKs | They cannot establish the private libp2p connection; bootstrap fails and Cluster peers do not start. |
| Bootstrap cannot create or verify both persistent peerings | Bootstrap exits nonzero with a fixed redacted error; Cluster peers do not start. |
| A or B restarts with its existing volume | The wrapper reinstalls the supplied key and bootstrap recreates or verifies the two persistent peerings, restoring the unique A/B relationship. |
| Cluster accepts a pin before physical replication converges | Treat it as pending. Existing allocation and tracker-state checks still decide replication success. |

Logs and diagnostic capture must redact PSK-format material and standalone
64-character lowercase hexadecimal secret values. The redactor must be scoped
so ordinary CID output remains intact. It must not broadly mask every
base32/base58-looking token or change application-visible CID responses.

The PSK protects swarm membership only. It does not isolate containers from
the internet, prevent outbound container traffic, encrypt REST or proxy
traffic, authenticate a published API, or provide host, database, Kubo, Cluster,
gateway, or load-balancer availability.

## Validation Topology and Runtime Evidence

`tests/compose.cluster-validation.yml` remains an override, not a production
deployment. It retains the existing loopback-only validation mappings and adds
only a validation-only `swarm_key_wrong` Compose secret and `kubo-c` service
with that different disposable swarm key and loopback Kubo API port `55102`.
Kubo C is not a Cluster member, has no
production counterpart, is not exposed beyond loopback, and is used solely to
prove rejection of a wrong PSK. Base validation ports remain unchanged: gateway
`59100`, PostgreSQL `55435`, Kubo A/B APIs `55100` and `55101`, and Cluster A/B
REST APIs `59101` and `59102`.

The Rust runtime target in `tests/cluster.rs`, with HTTP and polling helpers
split into `tests/support/cluster.rs` only if needed to preserve file
boundaries, must prove the protocol-visible assertions below. The outer
PowerShell/Compose validation owns container lifecycle, filesystem, and process
exit assertions that cannot be observed through Kubo HTTP APIs.

The Rust target must prove:

1. Kubo A and B each report `Bootstrap=[]`, routing type `none`, mDNS
   disabled, AutoConf disabled, and an empty `Swarm.AddrFilters` list.
2. Kubo A's swarm-peer set is exactly B and Kubo B's set is exactly A. Both
   directions have the expected persistent Peering entry.
3. Kubo C, given the wrong disposable key and `LIBP2P_FORCE_PNET=1`, cannot
   connect to A or B. The three
   peer sets remain A={B}, B={A}, and C={}. A wrong-key error must not reveal
   the source key, the wrong key, or their digests.
4. Restarting A and B with their original volumes restores both persistent
   peerings and the exact unique A/B peer relation without generating a new key.
5. The already-required Cluster topology, proxy, replication,
   delete-retention, outage, and recovery scenarios all pass afterward.

The outer validation must prove, without rendering secret material:

1. `swarm.key` is mode `0400` on each Kubo volume, and the two key-content
   digests compare equal only in memory. Neither key nor digest may be emitted.
2. A wrapper invocation with the key absent and
   `LIBP2P_FORCE_PNET=1` exits nonzero and never reaches daemon ready state.
3. The generated main and wrong-key files satisfy the strict file contract
   before Compose receives them, and both are deleted during owned cleanup.

Validation must retain bounded timeouts, loopback-only endpoint checks, and
existing real evidence that the Cluster proxy accepts the gateway's add, pin,
and cat request shapes. Tests must distinguish a connected swarm peer from a
Cluster peer and from a successful pin mutation.

## Secret Lifecycle and Workflow

The local workflow and the existing blocking Cluster CI job generate two
disposable random swarm-key files: the production A/B key and Kubo C's wrong
key. Generation requirements are strict: UTF-8 without BOM, LF line endings,
exactly the validated three-line format, and atomic `CreateNew` creation. The
workflow never displays, serializes, uploads, commits, or reuses either file.

The workflow must set `COMPOSE_DISABLE_ENV_FILE=1`, preserve existing process
environment restoration, and prefer the Compose secret-file interface. It must
validate the missing-key failure before the full startup. It owns a unique
Compose project, checks all validation ports before use, and removes only that
project's containers, networks, volumes, generated secret files, and temporary
artifacts. Cleanup is fail-closed: cleanup-command failure, residual owned
resources, a generated-file deletion failure, or an environment-restoration
mismatch fails the job.

`.github/workflows/release-validation.yml` extends the existing blocking
Cluster job. It must not create a second private-swarm job. Static contracts
lock the causal sequence: secret generation and wrapper absence test, Compose
config, Kubo A/B and C startup, successful bootstrap before Cluster start,
outer filesystem assertions, Rust private-swarm assertions, then the existing
topology and replication scenarios. They
also lock redaction, loopback-only validation exposure, cleanup, and the
unchanged default, single-PG, multi-gateway, and shared-entrypoint boundaries.

README and ROADMAP are edited only after all local static, runtime, and
regression gates pass. README describes the bounded same-host support and its
limits. ROADMAP checks only the private-swarm item. The initial hosted result
must be reported as `NOT RUN`; it is not inferred from local success.

## Expected File Boundary

Implementation may modify only the following paths, plus a future implementation
plan and this specification:

- `.github/workflows/release-validation.yml`
- `README.md`
- `ROADMAP.md`
- `docker-compose.cluster.yml`
- `ipfs/cluster.Dockerfile`
- `ipfs/private-swarm-entrypoint.sh` (new)
- `tests/compose.cluster-validation.yml`
- `tests/cluster.Tests.ps1`
- `tests/cluster.rs`
- `tests/support/cluster.rs`
- shared static tests when necessary to lock the workflow contract

Protected paths include all Cargo and application sources, the default Compose
profile, the single-PG profile, the multi-gateway profile, and
`ipfs/entrypoint.sh`. Any need to change a protected path, weaken a fail-closed
condition, or expose a production internal endpoint stops implementation for a
design revision.

## Non-goals

- Container egress isolation.
- General multi-host bootstrap.
- Online PSK rotation, dual-key operation, or member revocation.
- Kubo, Cluster, load-balancer, PostgreSQL, gateway, or host high availability.
- The combined two-gateway, independent-Kubo topology.
- Private REST exposure or TLS for REST/proxy endpoints.
- Application read fallback across Kubo peers.
- Pin reclamation, Cluster unpin, or garbage collection lifecycle changes.

PSK rotation requires coordinated downtime across the participating Kubo peers.
This project does not automate that process.

## Acceptance Criteria

The v0.5-D item is complete only when:

1. The Cluster profile has exactly the supported seven-service same-host
   topology, with one gateway, PostgreSQL 17, two Kubo v0.43.0 daemons, two
   IPFS Cluster v1.1.6 peers, and successful one-shot bootstrap.
2. Production uses the required `swarm_key` Compose secret file for both Kubos,
   never a tracked or raw-environment PSK, and publishes no Kubo, Cluster, or
   PostgreSQL endpoint.
3. The Cluster-only wrapper validates the exact file contract, configures the
   closed Kubo discovery surface including `AutoConf.Enabled=false` and the
   required empty dial-filter list, installs a mode-`0400` key every start, and
   fails closed without secret disclosure.
4. Bootstrap establishes and verifies the exact persistent A/B peer relation
   before either Cluster peer starts.
5. The validation-only wrong-key Kubo C proves PSK mismatch rejection without
   changing the production surface.
6. All required private-swarm assertions pass, including restart recovery, and
   all pre-existing Cluster topology, proxy, replication, delete-retention,
   outage, and recovery scenarios remain green.
7. The existing blocking Cluster job enforces the ordered contract, generated
   secrets and cleanup are fail-closed, and static tests prevent profile drift.
8. README and ROADMAP change only after the complete local evidence set passes;
   the first hosted result is honestly `NOT RUN`.
9. `git diff --check` passes and the final changed-path audit contains only the
   approved implementation boundary.

## Official Sources

- Kubo private networks and `swarm.key` format:
  <https://docs.ipfs.tech/how-to/private-networks/>
- Kubo configuration reference, including Bootstrap, Routing, Discovery, and
  Addresses: <https://docs.ipfs.tech/reference/kubo/config/>
- Kubo v0.43 AutoConf configuration and private-network startup validation:
  <https://github.com/ipfs/kubo/blob/v0.43.0/docs/config.md#autoconf>
- Kubo v0.43 `server` profile address filters:
  <https://github.com/ipfs/kubo/blob/v0.43.0/config/profile.go>
- Kubo v0.43 swarm peer and peering CLI encoders:
  <https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/swarm.go>
- Kubo peering configuration: <https://docs.ipfs.tech/how-to/peering/>
- Kubo v0.43.0 release: <https://github.com/ipfs/kubo/releases/tag/v0.43.0>
- IPFS Cluster v1.1.6 release:
  <https://github.com/ipfs-cluster/ipfs-cluster/releases/tag/v1.1.6>
- IPFS Cluster CRDT consensus:
  <https://ipfscluster.io/documentation/guides/consensus/>
- IPFS Cluster architecture and paired Kubo nodes:
  <https://ipfscluster.io/documentation/deployment/architecture/>
- IPFS Cluster replication factors and pinning:
  <https://ipfscluster.io/documentation/guides/pinning/>

## Git Boundary

This specification authorizes no implementation itself. A future implementation
must stay within **Expected File Boundary**, must not stage, commit, push, or
tag without separate explicit authorization, and must stop for a design revision
if runtime evidence contradicts this contract.
