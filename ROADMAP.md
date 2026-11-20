# Roadmap

## Implemented

### S3 client operations

- Bucket creation, deletion, HEAD, listing and location (`us-east-1`).
- Object PUT, GET, HEAD, COPY, single and batch DELETE, ListObjects v1/v2 and per-version tags. The object ETag is its IPFS CID; standard PUT also returns the CID and its `ipfs://` URI in response headers.
- SigV4 with configured access keys and [locally issued temporary sessions](docs/local-sessions.md), including presigned access. [Local user and bucket policies](docs/local-policies.md) constrain operations, prefixes, versions and supported tag conditions.
- Multipart upload initiation, part upload and listing, completion and abort. Encrypted parts must use the same SSE-C key as the upload.
- Bucket versioning (enable/suspend), `ListObjectVersions`, delete markers and exact-version operations.
- Path-style bucket CORS. CORS and lifecycle configuration APIs honor `ExpectedBucketOwner` when supplied.

### Storage and background work

- Streaming plaintext, SSE-S3 and SSE-C object I/O. Encrypted Range reads fetch and authenticate the covering ciphertext frames; legacy SSE-C key enrollment still verifies the full object once. Each [AES-GCM stream](src/crypto/chunker.rs) reuses its cipher; encryption generates a fresh nonce per chunk.
- Lifecycle expiration for current and noncurrent versions and eligible delete markers, incomplete multipart abort, and current/noncurrent transitions from `STANDARD` to `STANDARD_IA` on a configured independent cold Kubo. Rules support [prefix, tag and size filters](src/lifecycle/filter.rs) where the action allows them.
- Asynchronous remote backups with Pinata, Filebase, independent Kubo RPC and IPFS Cluster. Signed S3 requests manage expiring per-file/version references and query status; references share one remote resource per provider allocation domain and canonical CID. Submit and cleanup finish on the provider's acknowledgment. See [remote pin operations](docs/pinning-recovery.md).
- Durable, signed `ipfs3-import` from a CID or allowlisted HTTPS URL, with persisted progress, lease recovery, optional idempotency and ownership-fenced publication. `202 Accepted` acknowledges the job, before object publication or remote pin submission.
- Streaming ZIP extraction for PUT, multipart completion and import, with durable batch results, optional source publication in v2, UnixFS directory roots and opt-in automatic pinning for published outputs.
- SQLite and PostgreSQL metadata storage; a two-gateway profile with a shared PostgreSQL and Kubo backend and [Nginx upstream keepalive](deploy/nginx/multi-gateway.conf); a separate same-host IPFS Cluster replication profile with a private Kubo swarm.

- An optional Prometheus listener exposes S3 request and background pin timing and outcome metrics.

## Planned

### Identity and request controls

- Gateway request rate limits. Current dispatch limits apply to background pin providers, not S3 clients.
- Audit logging for S3 requests and administrative changes.

### S3 and client extensions

- S3 Select and tag-based search. Search can be eventually consistent and is separate from authorization.
- Object event notifications via webhook for committed PUT and DELETE operations. Delivery work is tracked independently of request completion and settled by the destination's acknowledgment.
- Static website hosting via IPFS Gateway and DNSLink.
- Native rclone backend for gateway extensions, separate from rclone's existing S3 backend.
