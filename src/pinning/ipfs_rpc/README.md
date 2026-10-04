# RPC provider leaf integration contract

This module is registered as `pinning::ipfs_rpc`. Stage 5 integrates it with
provider configuration and the durable worker's typed submission ledger; it
does not replace the primary Kubo client or rewrite S3 object identities.

## Public entry points

- `IpfsRpcProvider::new(name, trusted_endpoint, source_kubo, profile, strategy, auth)`
  returns `Result<IpfsRpcProvider, ProviderError>`.
- Profiles: `RpcProfile::{Kubo, Filebase}`. Strategies:
  `RpcStrategy::{Cid, Upload, Car}`. Filebase accepts only Upload with Bearer auth.
- `RpcAuth::Bearer(String)` / `RpcAuth::Basic { username, password }` have redacted
  Debug. Kubo also accepts no auth. No custom TLS/CA/mTLS bypass is implemented;
  parent configuration must reject unsupported options, not silently ignore them.
- `RpcTimeouts` and `new_with_timeouts` allow explicit bounds/test injection.
- `submit_observed(&self, request: SubmitPin) -> RpcSubmitObservation` is the
  inherent integration seam. The trait's `submit_observed` converts and preserves
  the full observation; the durable worker uses it. The compatibility `submit`
  method returns only `result` and must not be used to capture write evidence.
- `PinningProvider::invocation_route()` uses `"kubo"` or `"filebase-rpc"` as
  its profile, with the configured `"cid"`, `"upload"` or `"car"` strategy.
  Historical read/delete methods accept the supported historical RPC strategy,
  independently of the current strategy. The parent must select the exact stored
  endpoint/credential/scope revision before calling them.
- `observe` returns `QueryObservation`; `observe_pin` preserves
  Recursive/Direct/Indirect pin-list evidence but is **not** a residency proof.

Request IDs are `rpc-pin:<kubo|filebase>:<canonical CIDv1>`. RemotePin CIDs also
use canonical CIDv1, comparing **codec + multihash**, not the CID version or
multibase spelling. Parent matching/resource-key code must use that identity
comparison rather than lexical `remote.cid == original_cid`. S3 ETags/stored
object CIDs must not be rewritten.

## Parent ledger safeguards

`remote_ref(pin, historical_route)` always returns ownership Unknown, even when
the route says Managed. A matching preexisting recursive pin, an idempotent
pin/add, or a completed add/import is not application-created ownership proof.
There is no automatic cleanup. `unpin` is an exact-CID primitive for **already
authorized** outer-ledger calls only; it does not authorize itself.

Submit results and observed resources are independent structured fields, NOT
encoded in ProviderError.message. The previous JSON-message mismatch carrier is
removed. Errors contain finite safe messages and no requested/observed CID.

```rust
pub struct RpcSubmitObservation {
    pub result: Result<RemotePin, ProviderError>,
    pub resources: Vec<RpcObservedResource>,
    pub effect: RpcSubmitEffect,
}
pub struct RpcObservedResource {
    pub resource_type: RemoteResourceType, // always RpcPin
    pub cid: String,                      // canonical CIDv1
    pub request_id: String,               // stable typed ID
    pub status: RpcResourceStatus,
    pub ownership: Ownership,             // always Unknown
}
```

`RpcResourceStatus` is Reported (valid streamed root record, but clean response
completion not established; NO storage/pin claim), Stored (Kubo add pin=false),
PinAccepted (clean RPC root pin
acknowledgment, NOT Pinned/local verification), PinError (CAR Root.PinErrorMsg,
still no assertion that a preexisting pin is absent), or RecursiveVerified
(independent recursive proof; additionally local/offline/stable node for Kubo).
Only an exactly matching, single-root successful submission can reach the last
state. Multi-root and mismatch results fail without deleting any CID.

`RpcSubmitEffect::{NotSubmitted, Observed, Unknown}` describes write evidence,
independently of result failure. Observed means clean HTTP 200 EOF/trailer-checked
roots were recorded; it does not prove application creation or external-resource
absence. Unknown may have an empty list (unknown I/O with no clean root), or retain
known prior roots (e.g. clean add followed by pin/add I/O failure). A valid streamed
root followed by framing/trailer/I/O failure is retained only as Reported, with
effect Unknown; it never becomes a successful pin. Earlier stronger evidence is
not downgraded by a later partial response. Read-only final
verification failure preserves the previously observed root acknowledgments.
Resource vectors are canonical/deduplicated and bounded by
`MAX_OBSERVED_RESOURCES = 16`; incomplete/overflow observations remain Unknown.
Do not derive resource ownership or automatic retry/cleanup permission from this
enum. Persist both the failed result and all structured resources in the parent
ledger, with the exact historical route snapshot.

Malformed/truncated/trailer-failed responses and postdispatch write HTTP errors
retain unknown effects. Only local preflight or preconnection failures prove
NotSubmitted. A later failure cannot erase an earlier acknowledged write.

## Transport guarantees

Both source and target clients disable redirects; only target clients carry
target auth. Endpoints reject URL userinfo, query strings, fragments and non-HTTP
schemes. Endpoint trust/private-network authorization remains an administrator
registration decision, never an S3-tag input.

Upload reads the source's **stored** UnixFS bytes, without S3 decryption. A source
files/stat check rejects directories before cat. Kubo add uses fixed import
parameters and pin=false, checks the returned root, then explicitly recursively
pins. Filebase add uses only cid-version/wrap-with-directory and relies on its
official add-and-pin behavior; it never follows add with pin/add. Filebase pin/ls
uses only arg/stream/names, and pin/rm uses only arg.

CAR uses a full network-capable dag/export, not offline/local-only export. The
successful CAR submission requires exactly one successful Root, then typed final Stats,
then complete HTTP EOF with no stream error header/trailer. Kubo completion also
requires recursive pin/ls, offline files/stat with Local/WithLocality, another
recursive pin check, and stable node identity. Filebase CAR remains rejected.

Payloads stream with bounded buffering; control responses are capped at 64 KiB.
The upload watchdog has no whole-transfer deadline: consumed payload and strictly
increasing progress reset the idle bound; repeated/non-progress records do not.
Source and multipart upload must both reach EOF. Response trailers and declared
Content-Length are checked, not discarded via bytes_stream.

## Verification status and parent commands

Tests are registered under this module. Stage 5 implementation and isolated
backend acceptance passed; the current scoped results and unverified account
boundaries are recorded in `docs/testing.md`. Formatting/parser checks alone
do not prove mock or real-node success. Relevant targeted commands are:

```powershell
cargo test --lib pinning::ipfs_rpc::
cargo test --lib pinning::
cargo test --lib kubo::
cargo test --test integration
cargo test --test lifecycle_transition_car_proxy
```

The isolated dual-Kubo 0.43.0 runner passed in run
`24c5b32e5a224c1cb57be7784c564cd2`: zero-byte/raw/multiblock upload, explicitly
unpinned CID preseed, nondefault-chunker mismatch, stored ciphertext, directory
CAR and shared preexisting pins. After stopping the source, all eight roots
passed target-local/offline block and byte comparisons. All observed ownership
remained Unknown; no pin/rm or GC was executed. The runner cleaned up its two
nodes, volumes, internal network and relay processes.

This is leaf-provider/DAG/stored-byte evidence, not worker/config/main or S3 SSE
SDK acceptance. Basic/Bearer against anonymous nodes only proves header
transport, not authentication enforcement. Real Filebase/Pinata account writes
remain NOT RUN. Mock CAR strings are protocol fixtures, not real complete CAR
archives, and must not be reported as real DAG/account evidence.
