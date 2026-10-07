# Independent Cluster transport leaf (v1.1.6)

This leaf does not implement or register the shared `PinningProvider` trait. The
parent Stage6 integration owns configuration/route revisions, ledger, worker,
quota, fences, retries and final stage acceptance. The standalone contract target
wires the transport leaf through `#[path]`; the separate provider config target
uses the registered adapter. Neither substitutes for the real backend runner.

## API and protocols

- `ClusterProvider::new(ClusterProviderOptions)` requires an explicit backend/scope
  pinset and REST verifier endpoint. `Proxy` additionally requires a separately
  authenticated Proxy endpoint; `Rest` rejects Proxy options.
- `submit_observed(SubmitPin)` / `submit_with_cancel(SubmitPin, &CancellationToken)`
  return `ClusterWriteObservation`, **not** shared `SubmitObservation`.
- `observe(cid)` returns the existing fresh `ReplicaObservation`.
- `resource_key(cid)` and each footprint's `ledger_key()` preserve CID codec and
  complete multihash, canonicalizing v0/v1 spelling. Endpoint aliases with the
  same backend/scope/CID map to one `ClusterPin` resource, not two backups.
- `unpin_authorized_ref(&ClusterRemoteRef, &CancellationToken)` is an explicit
  exact-pinset/protocol/strategy/CID primitive. It does not authorize ownership,
  perform reconciliation or run automatically in any submit/failure path.

Each job selects **one** write endpoint; no fallback, alternate re-POST, hidden
Kubo `dag/import`, or error-triggered unpin is performed:

| Strategy | Proxy | Native REST |
| --- | --- | --- |
| CID | `POST /api/v0/pin/add?arg=…&type=recursive` | `POST /pins/{cid}` |
| Upload | `POST /api/v0/add?format=unixfs` | `POST /add?format=unixfs` |
| CAR | `POST /api/v0/add?format=car` | `POST /add?format=car` |

The add request has one opaque multipart file, `no-pin=false`, no wrapping or
sharding, and explicitly selected `stream-channels=true|false`. It does not send
object names, metadata, tags or replication-min/max. `min_replicas` is solely the
REST verification threshold and never changes deployment RF. CID REST writes
do not pretend that the `/pins/{cid}` handler accepts PinOptions.

## Transfer and receipt invariants

Stored source RPC endpoints/auth are separate from Proxy and REST. Upload first
checks `files/stat` against the requested CID and rejects directories before
`cat`; directories require explicit CAR. Neither path accesses S3 plaintext or
decryption keys. Upload uses SHA-256 raw/dag-pb UnixFS import. Its default version
follows the requested CID spelling (v0/non-raw or v1/raw leaves), balanced layout,
size-262144. `ClusterUnixfsOptions` can select explicit version/raw leaves/chunk
size/trickle parameters. Other DAG construction parameters may yield a mismatch;
the actual returned resource remains recorded and there is no CAR retry.

CAR uses stored `dag/export` and validates a complete single-root CARv1 with
matching codec/multihash, identity/SHA-256/SHA-512 block hashes, raw/dag-pb/dag-cbor
links, exact closure, no duplicate/extraneous/missing blocks, and no truncated
suffix. Unsupported hashes/codecs fail closed. It buffers one bounded block and
a bounded link graph, not a whole object. The byte limit applies to both upload
and CAR; graph/block budgets additionally constrain CAR. Multipart demand pulls
source bytes, without a background producer or unbounded queue.

Source HTTP EOF (including trailers), CAR EOF and the entire destination
multipart EOF must all occur. Header and trailer `X-Stream-Error`, partial bodies,
early destination responses and source failures cannot establish acceptance.
`TE: trailers` is explicitly negotiated. Transfers use progress-based idle
timeouts rather than an absolute object-size-dependent timeout; control-plane
writes and source inspection have bounded total timeouts. Cancellation drops
HTTP body futures and returns the dispatched footprint.

Add response parsing uses independent official v1.1.6 DTOs: Proxy's
`Name/Hash/Bytes/Size` (`Size` is a string; zero Bytes/Size and undefined Hash are
omitted), versus native REST's `name/cid/bytes/size/allocations` (`size` is numeric;
undefined `cid` is null). There is no cross-protocol alias or fixture translation.
Both support streaming JSON with `X-Chunked-Output: 1`, or a bounded JSON array for
false mode. A receipt is accepted only after terminal framing and EOF checks.
Allocation hints are historical metadata, never quorum. Proxy `dag/import` is
Kubo passthrough and is never used to establish Cluster registration.

## Parent handoff (must remain lossless)

Persist **all** `resources`, not just `result`'s requested receipt. Each includes
canonical pinset/CID, protocol, strategy, historical timestamp, reported allocation
hints, and `DispatchIntent` / `Reported` / `PinAccepted` / `UnpinAccepted` status.
Every ownership value remains `Unknown`. The observation also preserves known
write HTTP status and typed/redacted failure code. Post-dispatch 403, timeout,
trailer failures and CID mismatch remain `Unknown` effects with requested and
any parsed unexpected resources intact. Malformed receipts do not roll back
already reported CIDs.

An accepted receipt is **not** N/N completion. `replication_outcome()` only uses
the resource-bound, dynamically fresh existing REST proof. The verifier checks
v1.1.6, fresh stable allocations, `GET /pins/{cid}?local=false`, peer deduplication
and quorum; expired/superseded proofs become Unknown while historical evidence
is retained. Independent provider instances do not share proof invalidation;
parent must deduplicate their pinset identities and refresh before ledger
decisions. 404/403/empty allocations never authorize absence or cleanup.

Do not map Cluster quorum into shared `RecursiveVerified`. Parent must add a
normal typed Cluster adapter/proof channel, preserve route/credential revisions,
record in-flight intent **before** awaiting writes, and interpret cancellation
after accepted writes (receipt retained, proof may be absent). Dropping the entire
future cannot return observations, so ledger recovery remains necessary.

Deletion requires parent ownership/reference/in-flight/fence authorization and
the exact historical route. The leaf checks typed reference binding but cannot
prove those ledger conditions. Nothing invokes this primitive automatically.

## Network boundary

Source, target write and the REST verifier now use one internal `cluster/network`
endpoint validator and finalized client factory. Every client disables ambient
environment/system proxies, redirects and HTTP retries. TLS certificate and
hostname validation stay enabled; the original hostname is retained for Host/SNI.

`ClusterProviderOptions::allow_private_network=false` maps to
`ClusterNetworkPolicy::PublicOnly` for **all** profiles. HTTPS public IP literals
and valid DNS hostnames are accepted without constructor DNS I/O. Each connection
resolves through the policy resolver within the connect deadline, rejects empty,
oversized (>64 addresses), mixed/public-private or special-address answers, and
hands that same approved address iterator to the connector. There is no preflight
followed by an unconstrained second lookup. DNS-supplied ports cannot override the
original URL port. HTTP/1 with no idle connection pool avoids later requests
skipping fresh resolution by reusing an old connection.

Private/internal endpoints or cleartext HTTP require explicit
`allow_private_network=true` / `ClusterNetworkPolicy::AllowPrivate`; this does
**not** permit userinfo (including empty userinfo), query/fragment, redirects,
ambient proxies or insecure TLS. Control/observation total deadlines, progress
idle deadlines and existing response/CAR byte budgets are unchanged. DNS failure,
policy denial or cancellation cannot establish absence/quorum or revive a
superseded proof. Tokio bounds the DNS await; OS resolver internals are not a
separately managed DNS service.

For the parent production factory:

```rust,ignore
let policy = ClusterNetworkPolicy::from_allow_private_network(allow_private);
let profile = ClusterRestProfile::new_with_network_policy(
    pinset, Some(rest_endpoint), rest_auth, min_replicas, limits, policy,
)?;
let verifier = ClusterReplicaVerifier::new(profile)?;
```

The existing `ClusterRestProfile::new(pinset, endpoint, auth, min_replicas, limits)`
signature remains available, but defaults to `PublicOnly`; callers that used
implicit HTTP/internal trust must migrate to the explicit policy constructor.
The provider already performs that mapping and cannot give its verifier another
policy. No additional registry/configuration surface was introduced.

## Independent evidence only

```powershell
cargo test --offline --test cluster_provider_contract
cargo test --offline --test cluster_network_policy -- --skip cluster::
cargo check --offline --test cluster_provider_contract --test cluster_network_policy
cargo clippy --offline --test cluster_provider_contract --test cluster_network_policy -- -D warnings
```

Run rustfmt only on the affected Cluster files and the independent targets, with
`skip_children=true`; never format shared Stage5 files. The network target's
ambient-proxy contract launches its own executable with an explicit child opt-in
and child-only environment overrides; no process-global env mutation/race. Child
exit/timeout is bounded and cleanup is owned by the process/mock/task handles.
These contracts use real loopback HTTP/wiremock. They are **not** proof of real
Cluster interoperability or completed Stage6 coordinator/worker integration.
Controlled DNS and an owned test TLS root verify binding/SNI/hostname rejection;
real public DNS routing and deployed Cluster authentication enforcement were not
tested. No OS trust/proxy state, external account or real Cluster was changed.

Protocol source: official tagged
[`api/add.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/add.go),
[`api/ipfsproxy/ipfsproxy.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/ipfsproxy/ipfsproxy.go),
[`api/rest/restapi.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/rest/restapi.go),
[`adder/adderutils/adderutils.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/adder/adderutils/adderutils.go),
[`adder/adder.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/adder/adder.go),
[`api/types.go`](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/types.go).
