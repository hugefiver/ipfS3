# Cluster provider and REST replica verifier — Stage 6

`pinning/mod.rs` registers `registered.rs`, which exposes the transport leaf,
formal configuration and shared `PinningProvider` adapter. The verifier itself
performs only GET requests. Independent contract targets include `mod.rs` with
`#[path]`; `cluster_provider_config` tests formal configuration/registration.
Worker/DB integration and real backend acceptance have separate evidence gates.

Administrator configuration uses `kind = "cluster"`, explicit `strategy =
"cid"|"upload"|"car"`, and `[pinning.providers.cluster]` with `protocol`,
`rest_endpoint`, `min_replicas` and explicit private-network authorization.
Proxy additionally requires `proxy_endpoint`. Credentials are independent
`proxy_auth`, `rest_auth` and `source` auth tables (none/Bearer/Basic with env
references); source has no URL option and binds only to the actual main Kubo.
Explicit `pinning_identity` captures backend/scope, write secret reference and
administrator endpoint/credential revisions, including verifier/source changes.
There is no second registry, main Kubo replacement or RF mutation.

Use `ValidatedProvider::route_snapshot()` to capture confirmation; identity-only
snapshots omit it for old JSON compatibility. Route snapshots and historical
resource observations default/skip new optional fields. Historical replica facts
do not deserialize into a live `ReplicaObservation`. Retired/disabled routes are
not new allocation candidates; they remain registered for historical reads.
Incompatible original protocol/strategy/revisions/confirmation fail closed.
Unknown ownership never grants adapter unpin permission. Multiple declared
addresses of one pinset share one allocation; the deterministic first route is
selected, with no automatic address or strategy fallback.

## Integration API

```rust,ignore
ClusterPinset::new(backend: &str, scope: &str)
    -> Result<ClusterPinset, VerificationFailure>

ClusterRestProfile::new(
    pinset: ClusterPinset,
    rest_endpoint: Option<&str>,
    auth: Option<ClusterAuth>,
    min_replicas: usize,
    limits: RestLimits,
) -> Result<ClusterRestProfile, VerificationFailure>

ClusterRestProfile::resource_key(&self, cid: &str)
    -> Result<ClusterResourceKey, VerificationFailure>

ClusterReplicaVerifier::new(profile: ClusterRestProfile)
    -> Result<ClusterReplicaVerifier, VerificationFailure>
ClusterReplicaVerifier::observe(&self, cid: &str) -> ReplicaObservation

ReplicaObservation::outcome(&self) -> ReplicaOutcome
ReplicaObservation::evidence(&self) -> Option<&ReplicaEvidence>
ReplicaObservation::failure(&self) -> Option<VerificationFailure>
```

- `ReplicaOutcome` is `Complete | Insufficient | Unknown`. Only `Complete` from
  **the current observation** satisfies this verifier's `min_replicas` condition.
  It is not a gateway-read/content-integrity/physical-disaster-recovery receipt.
- `ReplicaEvidence` contains `resource`, `allocation`, `required_replicas`,
  `pinned_peers`, `peer_states`, `started_at`, `global_received_at`, `finished_at`.
  `AllocationBinding` contains canonical/deduplicated `peers`, `created_at`, and
  the observed `replication_factor_min/max`. The verifier does not modify those
  Cluster settings. Counts are not substitute proofs; historical evidence stays
  readable when `outcome()` has become `Unknown`.
- Resource identity is `(administrator-declared backend, scope, canonical CID)`.
  Neither address, credential nor config display name creates a replica. Parent
  policy aggregation must deduplicate this key, not sum counts from multiple REST
  addresses of one pinset. Two profiles pointing at one pinset must receive the
  **same** backend/scope. The REST API does not establish a physical failure domain.
- Reuse one verifier per logical route/pinset so a new observation of the same CID
  invalidates its previous proof **before** network IO, even on query failure or
  concurrent late replies. Different CIDs do not invalidate one another. Clones of
  an observation share invalidation and expire on the same monotonic clock.
  Supersession does not extend across separately constructed verifiers: parent
  route/config revisions must fence old observations. Never persist/reload a
  receipt as live proof; only its historical evidence is suitable for the ledger.
- DTOs are intentionally private (`PinAllocation`, `GlobalPinInfo`, `PeerPinInfo`,
  `Version`). There is no public function accepting a previous GET or externally
  supplied timestamp to mint current proof. Missing required JSON fields,
  conflicting duplicate peer-map keys and unsupported allocated statuses fail
  closed. Optional official fields unrelated to proof are ignored.

## Read protocol and freshness

Every call is bounded by a single total deadline and performs:

1. `GET /version`: exact `1.1.6` or `1.1.6+<valid build metadata>` only.
2. `GET /allocations/{cid}`: bind the canonical CID, canonical allocated peer set,
   creation timestamp and replication factors.
3. `GET /pins/{cid}?local=false`: require the same CID, allocations and `created`.
4. `GET /allocations/{cid}` again: require the original binding unchanged.

Any binding disagreement returns `Unknown/AllocationChanged`, with no partial
proof. A subsequent call repeats **all** reads; no GET results or counts are
cached. Only allocated, distinct peers whose status is exactly `pinned`, error
string is empty and timestamp is not before the bound creation or in the future
count. Remote/sharded/missing/errors/queued/pinning/unpinned/stale do not count.
Unallocated peers never count. Cluster peer IDs are normalized from base58
multihash/CIDv1 libp2p-key forms; textual aliases cannot inflate replicas. Empty
allocations, including Cluster's `replication_factor=-1` all-peers mode, do not
implicitly allocate every `peer_map` entry and therefore cannot satisfy a positive
minimum. That mode needs an explicit, separate membership contract before support.

In v1.1.6 stateless tracking, a legitimate long-lived pinned peer's `timestamp`
may be the pin's creation time. It is **not** interpreted as its last health check.
Local observation age is measured monotonically from the start of this request
window. Future creation timestamps fail closed; future peer timestamps do not
count. Clock skew can cause conservative false negatives, never implicit success.

This is a bounded REST snapshot, not a linearizable transaction: v1.1.6 does not
expose a monotonic allocation revision/read transaction, so an unreported ABA
between reads or a dishonest/stale reverse proxy cannot be disproved. HTTP reads
request `Cache-Control: no-cache, no-store`. The parent must trust the fixed origin
and require fresh observation at its completion gate, not claim continuous N/N
availability. `local=true` is never sent and cannot be supplied via base URL query.

## Transport/security contract

- Missing/blank REST endpoint returns `MissingRestConfiguration`: no Proxy `pin/ls`
  fallback, no REST discovery and no complete-provider registration here.
- `ClusterAuth` supports Bearer and Basic; Debug is redacted. Authentication headers
  are marked sensitive and attached only to requests built from the fixed REST
  origin. Endpoints cannot contain userinfo, query or fragment. Administrators may
  explicitly choose HTTP; TLS certificate/hostname verification is never disabled.
- Redirects are rejected, including same-origin redirects. Error bodies, Location,
  raw transport/parser errors, endpoint and auth are not exposed through outcomes.
  `VerificationFailure` has only `operation`, `code`, optional numeric `http_status`.
  In particular, 403 is `UnknownForbidden` and 404 is `Unknown/NotFound`, not an
  authoritative empty pinset or inferred account-plan/permission diagnosis.
- Default budgets: connect 3 s, whole observation 10 s, response/head/body idle 3 s,
  each successful body 256 KiB, live observation age 30 s. Values must be positive;
  timeout/age ceiling is 300 s and body ceiling is 4 MiB. Bodies are read by bounded
  chunks and require clean EOF before JSON is accepted. Headers/body stalls,
  truncated responses, excessive announced or actual bytes return Unknown.

## Fixed source evidence and verification

Context7 was queried first; it had no matching Cluster documentation. Protocol
evidence was read via GitHub API from the official **v1.1.6** source, not master:

- `api/types.go` L349–360: global CID/allocations/created/peer_map; L409–418:
  per-peer status/timestamp/error; L755–757 and L992–1015: allocation factors and
  consensus pin timestamp.
- `api/rest/restapi.go` L538–550: allocation PinGet; L713–740: `local=true` calls
  StatusLocal and still wraps it in a global-shaped response; otherwise Status.
- `pintracker/stateless/stateless.go` L475–515 and L529–553: timestamps may be pin
  creation timestamps; a fresh Status asks IPFSConnector.PinLsCid.

Source URLs (all use `ref=v1.1.6`):

- https://api.github.com/repos/ipfs-cluster/ipfs-cluster/contents/api/types.go?ref=v1.1.6
- https://api.github.com/repos/ipfs-cluster/ipfs-cluster/contents/api/rest/restapi.go?ref=v1.1.6
- https://api.github.com/repos/ipfs-cluster/ipfs-cluster/contents/pintracker/stateless/stateless.go?ref=v1.1.6

Run the isolated leaf contracts without installing/fetching software:

```powershell
cargo test --offline --test cluster_replica_verifier
cargo clippy --offline --test cluster_replica_verifier -- -D warnings
rustfmt --edition 2024 --check src/pinning/cluster/mod.rs tests/cluster_replica_verifier.rs
```

Leaf verification result: **28 passed, 0 failed, 0 ignored**; the scoped Clippy
command with `-D warnings` and the scoped rustfmt check both passed. A pre-existing
dependency future-compatibility notice for `proc-macro-error2` is not a leaf lint.

Tests exercise 1/2 versus 2/2, remote/unallocated/missing/error states, duplicate and
aliased peers, CID mismatch/aliases, allocation/creation/factor changes, old peer
revisions, expiry/supersession, global-vs-local scope, 403 and other HTTP failures,
redirect/auth isolation, malformed/oversized/truncated responses, total/idle
deadlines and actual untrusted TLS handshake rejection. These are mock/loopback
HTTP/TLS contracts, **not real Cluster N/N replica verification**. No containers,
provider accounts, uploads, unpins, CAR, management or public HTTP API are involved.
