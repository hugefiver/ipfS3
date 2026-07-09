# Roadmap

## Current: v0.1 — MVP

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

- [ ] Fix docker-compose SQLite file database startup (`sqlite:///data/ipfs-s3.db`)
- [ ] GetBucketLocation (`us-east-1`) for MinIO `mc` and SDK preflight checks
- [ ] ListObjects v1 compatibility by reusing the ListObjectsV2 listing logic
- [ ] DeleteObjects (batch delete) for clients that remove multiple keys at once
- [ ] rclone smoke test: mkdir, copy, ls, cat, deletefile, rmdir with default config where possible
- [ ] MinIO `mc` smoke test: alias, mb, cp, ls, cat, stat, rm, rb
- [ ] AWS CLI smoke test kept as baseline regression coverage
- [ ] Document recommended rclone options when exact S3 behavior differs (`list_version=2`, `use_server_modtime`)
- [ ] Verify HeadObject signatures for nested keys through direct docker networking and localhost
- [ ] Track client compatibility matrix in docs

## v0.3 — Hardening

- [ ] Presigned URL (GET/PUT)
- [ ] Bucket name validation
- [ ] HeadObject Range support
- [ ] SSE-C key consistency validation for multipart
- [ ] Integration tests for encryption, multipart, SSE-C, Range
- [ ] PutObject response custom headers (x-amz-meta-ipfs-cid, x-amz-meta-ipfs-url)

## v0.4 — Pinning Service

- [ ] Pinata integration (PinningService trait implementation)
- [ ] Automatic pin on PutObject/CompleteMultipartUpload
- [ ] Unpin on DeleteObject
- [ ] Configurable pinning provider

## v0.5 — Multi-node

- [ ] PostgreSQL production deployment
- [ ] Multiple gateway instances (horizontal scaling)
- [ ] IPFS Cluster for pinset replication
- [ ] Private swarm (swarm.key) for node-to-node communication

## v0.6 — Versioning & Lifecycle

- [ ] Object versioning (enable/suspend on bucket)
- [ ] ListObjectVersions
- [ ] DeleteMarker support
- [ ] Lifecycle rules (expiration, transition)
- [ ] Bucket CORS configuration

## v0.7 — IAM & Security

- [ ] IAM users and policies
- [ ] STS temporary credentials
- [ ] Per-bucket access control
- [ ] Request rate limiting
- [ ] Audit logging

## v0.8 — Performance

- [ ] Chunk-level encrypted Range (v0.9 optimization)
- [ ] Pebble datastore backend for Kubo
- [ ] Connection pooling tuning
- [ ] Metrics and Prometheus exporter
- [ ] AES cipher reuse (avoid per-chunk key schedule)

## v0.9 — Ecosystem

- [ ] S3 Select
- [ ] Object Tagging
- [ ] Event notifications (webhook on Put/Delete)
- [ ] Static website hosting (via IPFS Gateway + DNSLink)
- [ ] rclone backend plugin
