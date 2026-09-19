# Roadmap

## v0.1 — MVP

- [x] S3 CRUD: PutObject, GetObject, HeadObject, DeleteObject, CopyObject, ListObjectsV2
- [x] Bucket operations: CreateBucket, DeleteBucket, HeadBucket, ListBuckets
- [x] SigV4 authentication (via s3s)
- [x] SSE-S3 encryption (AES-256-GCM, gateway-managed key)
- [x] SSE-C encryption (customer-provided key)
- [x] Multipart Upload (Create, UploadPart, Complete, Abort, ListParts)
- [x] Streaming (no full-body buffering)
- [x] Docker Compose deployment
- [x] Cloudflare Tunnel support
- [x] Kubo config optimization (bootstrap, GC, datastore, CORS)

## v0.2 — Client Compatibility

Goal: make the gateway work out-of-the-box with common S3 clients, not only
with the AWS CLI happy path. Compatibility is validated by running clients in
Docker against the local docker-compose stack.

- [x] Fix docker-compose SQLite file database startup (`sqlite:///data/ipfs-s3.db`)
- [x] GetBucketLocation (`us-east-1`) for MinIO `mc` and SDK preflight checks
- [x] ListObjects v1 compatibility by reusing the ListObjectsV2 listing logic
- [x] DeleteObjects (batch delete) for clients that remove multiple keys at once
- [x] rclone smoke test PASSED: mkdir, copy, ls, cat, deletefile, rmdir through the Compose-network endpoint (`dual_head=NOT_RUN`; see `docs/client-smoke-evidence-2026-07-19.log`)
- [x] MinIO `mc` smoke test PASSED: temporary alias config, alias list, mb, cp, ls, cat, stat through both endpoints, rm, rb (`dual_head=PASSED`; see `docs/client-smoke-evidence-2026-07-19.log`)
- [x] AWS CLI smoke test PASSED: mb, cp, ls, get-bucket-location, ListObjects v1, same-client dual-endpoint HeadObject, DeleteObjects, rm, and rb (`dual_head=PASSED`; see `docs/client-smoke-evidence-2026-08-13.log`)
- [x] Document recommended rclone options when exact S3 behavior differs (`list_version=2`, `use_server_modtime`)
- [x] Verify HeadObject signatures for nested keys through direct docker networking and localhost (MinIO `mc` same-client dual endpoint `stat` PASSED with `client=Mc verifier=Mc` EVIDENCE; see `docs/client-smoke-evidence-2026-07-19.log`)
- [x] Track client compatibility matrix in docs

## v0.3 — Hardening

- [x] Presigned URL (GET/PUT)
- [x] Bucket name validation
- [x] HeadObject Range support
- [x] SSE-C key consistency validation for multipart
- [x] Integration tests for encryption, multipart, SSE-C, Range
- [x] PutObject response custom headers (x-amz-meta-ipfs-cid, x-amz-meta-ipfs-url)

## Current: v0.4 — Pinning Service

- [x] Filebase and Pinata PSA clients
- [x] Ordered automatic and manual pinning policies
- [x] Standard S3 object-tag control
- [x] Durable asynchronous worker and crash recovery
- [x] Sticky `one` failover and `all` provider coordination
- [x] Lease duration, renewal, expiry, and remote unpin
- [x] Decompressed ZIP entry targets
- [x] Unique-CID local soft quota and eviction

## Delivered — Release Assignment Pending

Package version remains `0.1.0`. This unnumbered section records delivered
functionality without assigning durable import to v0.4, v0.5, or any other
numbered release.

- [x] Durable SigV4 `ipfs3-import` submission from a CID or allowlisted HTTPS URL
- [x] Persisted import status, progress, lease-based retries, and crash recovery
- [x] Idempotent replay through the optional `x-ipfs3-client-token` header
- [x] Optional ZIP extraction with ownership-fenced atomic publication
- [x] Stale-worker and overlapping content-mutation fencing

## v0.5 — Multi-node

- [x] PostgreSQL production deployment
- [x] Multiple gateway instances (horizontal scaling)
- [x] IPFS Cluster for pinset replication
- [x] Private swarm (swarm.key) for node-to-node communication

## v0.6 — Versioning & Lifecycle

- [x] Object versioning (enable/suspend on bucket)
- [x] ListObjectVersions
- [x] DeleteMarker support
- [x] Lifecycle rules (expiration, transition)
- [x] Bucket CORS configuration

## v0.7 — IAM & Security

- [ ] IAM users and policies
- [ ] STS temporary credentials
- [ ] Per-bucket access control
- [ ] Request rate limiting
- [ ] Audit logging

## v0.8 — Performance

- [ ] Chunk-level encrypted Range
- [ ] Pebble datastore backend for Kubo
- [ ] Connection pooling tuning
- [ ] Metrics and Prometheus exporter
- [ ] AES cipher reuse (avoid per-chunk key schedule)

## v0.9 — Ecosystem

- [ ] S3 Select
- [ ] Advanced tag-based search and policy conditions
- [ ] Event notifications (webhook on Put/Delete)
- [ ] Static website hosting (via IPFS Gateway + DNSLink)
- [ ] rclone backend plugin
