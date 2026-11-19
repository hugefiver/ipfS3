# IPFS RPC pinning provider

`IpfsRpcProvider` implements `PinningProvider` for Kubo and Filebase RPC. Kubo
supports CID, stored-byte upload and CAR. Filebase supports upload with a
bucket-scoped bearer token. `RpcProviderOptions` configures profile, strategy,
authentication, endpoint network policy and timeouts.

- Kubo CID submits one recursive `pin/add` and requires an exact root ACK.
- Upload streams the source UnixFS file's stored bytes, including ciphertext,
  and verifies the terminal `add` root. Source `files/stat` rejects directories
  before `cat`; use CAR for directories. Kubo sends `pin=true` with fixed import
  parameters; Filebase pins as part of its `add` and accepts only `cid-version`
  and `wrap-with-directory` on this path.
- CAR streams a complete, network-capable `dag/export` to `dag/import` with
  `pin-roots=true`. The ACK requires one matching root with no pin error and
  the terminal stats record.

A successful submit returns `RemotePinStatus::Pinned` from the provider's ACK.
It does not establish physical residency or resource ownership. Streamed ACKs
require source and multipart EOF, a complete response, and no stream error
header or trailer. Payloads are streamed with bounded buffering and idle
limits; control responses are limited to 64 KiB. RPC clients do not follow
redirects or retry requests. Target credentials never reach the source client.

Request IDs are `rpc-pin:<kubo|filebase>:<acknowledged CID>` and preserve the
acknowledged CID version. Submit compares codec and multihash and returns a
canonical CIDv1 in `RemotePin.cid`. `unpin` sends `pin/rm` for the exact CID
from the authorized handle and requires a matching root, including CID version. The adapter does not list pins, check local
residency, or run garbage collection.
