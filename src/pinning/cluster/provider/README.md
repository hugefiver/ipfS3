# IPFS Cluster v1.1.6 write transport

| Operation | Cluster Proxy | Native REST |
| --- | --- | --- |
| Submit CID | `POST /api/v0/pin/add?arg=…&type=recursive` | `POST /pins/{cid}` |
| Upload stored bytes | `POST /api/v0/add?format=unixfs` | `POST /add?format=unixfs` |
| Upload CAR | `POST /api/v0/add?format=car` | `POST /add?format=car` |
| Remove | `POST /api/v0/pin/rm?arg=…` | `DELETE /pins/{cid}` |

Each operation uses one configured target without fallback or automatic retry. CID submission and removal accept HTTP 200 with a matching `Pins` (Proxy) or `cid` (REST). Native REST removal also accepts a bodyless 202 or 204. The submission receipt retains the acknowledged CID version for use as the deletion handle; root comparison treats CIDv0 and CIDv1 with the same codec and multihash as equivalent. Completion is a protocol acknowledgment. No version, allocation, status, or garbage collection query follows it.

The `add` request uses one opaque multipart file, `no-pin=false`, and no wrapping or sharding. `stream-channels=true` requires newline-delimited JSON with `X-Chunked-Output: 1`; `false` uses a bounded JSON array. Proxy uses `Name/Hash/Bytes/Size` with string `Size`; REST uses `name/cid/bytes/size/allocations` with numeric `size`. Exactly one matching root is required. The response and transfer must complete without any stream errors. The request sends no object name, tags, or replication settings.

Byte upload checks the source `files/stat` before `cat`; directories require CAR. Default UnixFS settings follow the requested CID version, use raw leaves for CIDv1, balanced layout, and size-262144 chunking. An incompatible import produces a CID mismatch rather than an alternate submission.

CAR reads from `dag/export` and validates a complete single-root CARv1 with matching codec and multihash, supported block hashes and links, exact closure, and no truncated suffix. Multipart streaming is demand-driven. An early response cannot replace source or multipart EOF. Upload and CAR use progress-based idle deadlines; CID writes, removals, and source inspection also have control deadlines.

See the tagged [REST routes](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/rest/restapi.go), [Proxy routes](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/api/ipfsproxy/ipfsproxy.go), and [add handler](https://github.com/ipfs-cluster/ipfs-cluster/blob/v1.1.6/adder/adderutils/adderutils.go) for wire details.
