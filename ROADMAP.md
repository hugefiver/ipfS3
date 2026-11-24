# Roadmap

## Implemented

### S3 client operations

- Bucket creation, deletion, HEAD, listing and location (`us-east-1`).
- Object PUT, GET, HEAD, COPY, single and batch DELETE, ListObjects v1/v2 and per-version tags. The object ETag is its IPFS CID; standard PUT also returns the CID and its `ipfs://` URI in response headers.
- SigV4 with configured access keys and [locally issued temporary sessions](docs/local-sessions.md), including presigned access. [Local user and bucket policies](docs/local-policies.md) constrain operations, prefixes, versions and supported tag conditions.
- Multipart upload initiation, part upload and listing, completion and abort. Encrypted parts must use the same SSE-C key as the upload.
- Bucket versioning (enable/suspend), `ListObjectVersions`, delete markers and exact-version operations.
- Path-style bucket CORS. CORS and lifecycle configuration APIs honor `ExpectedBucketOwner` when supplied.
- Optional [per-caller limits](README.md#per-caller-rate-limits) on S3 request rate and aggregate proxy download bandwidth. Redirected downloads count as requests only.

### Storage and background work

- Streaming plaintext, SSE-S3 and SSE-C object I/O. Encrypted Range reads fetch and authenticate the covering ciphertext frames; legacy SSE-C key enrollment still verifies the full object once. Each [AES-GCM stream](src/crypto/chunker.rs) reuses its cipher; encryption generates a fresh nonce per chunk.
- Lifecycle expiration for current and noncurrent versions and eligible delete markers, incomplete multipart abort, and current/noncurrent transitions from `STANDARD` to `STANDARD_IA` on a configured independent cold Kubo. Rules support [prefix, tag and size filters](src/lifecycle/filter.rs) where the action allows them.
- Asynchronous remote backups with Pinata, Filebase, independent Kubo RPC and IPFS Cluster. Signed S3 requests manage expiring per-file/version references and query status; references share one remote resource per provider allocation domain and canonical CID. Submit and cleanup finish on the provider's acknowledgment. See [remote pin operations](docs/pinning-recovery.md).
- Durable, signed `ipfs3-import` from a CID or allowlisted HTTPS URL, with persisted progress, lease recovery, optional idempotency and ownership-fenced publication. `202 Accepted` acknowledges the job, before object publication or remote pin submission.
- Streaming ZIP extraction for PUT, multipart completion and import, with durable batch results, optional source publication in v2, UnixFS directory roots and opt-in automatic pinning for published outputs.
- SQLite and PostgreSQL metadata storage; a two-gateway profile with a shared PostgreSQL and Kubo backend and [Nginx upstream keepalive](deploy/nginx/multi-gateway.conf); a separate same-host IPFS Cluster replication profile with a private Kubo swarm.

- An optional Prometheus listener exposes S3 request and background pin timing and outcome metrics.

## Planned

### S3 and client extensions

- S3 Select and tag-based search. Search can be eventually consistent and is separate from authorization.
- Static website hosting via IPFS Gateway and DNSLink.
- Client CID preflight on `PUT` and `decompress-zip`: allow clients to supply candidate CIDs on `PUT` and reuse content directly when already retrievable on the primary Kubo or IPFS network; for `decompress-zip`, allow preflighting the full file manifest so clients upload only files whose CIDs are missing, then either skip storing the archive ZIP or reconstruct it on the backend.

### Storage and lifecycle

- Per-bucket primary Kubo GC/unpin policies to unpin CIDs from the primary Kubo node once their lifecycle has ended and no active database references remain.
- Versioned UnixFS directory root (`rootfs`) pin options allowing clients to choose between retaining only the latest version or keeping multiple versions, defaulting to latest-only (regenerating a root CID for the same directory updates the existing database record in place and unpins the previous root).
