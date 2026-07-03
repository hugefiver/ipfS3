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

## v0.2 — Hardening

- [ ] Presigned URL (GET/PUT)
- [ ] DeleteObjects (batch delete)
- [ ] GetBucketLocation
- [ ] Bucket name validation
- [ ] HeadObject Range support
- [ ] SSE-C key consistency validation for multipart
- [ ] Integration tests for encryption, multipart, SSE-C, Range
- [ ] PutObject response custom headers (x-amz-meta-ipfs-cid, x-amz-meta-ipfs-url)

## v0.3 — Pinning Service

- [ ] Pinata integration (PinningService trait implementation)
- [ ] Automatic pin on PutObject/CompleteMultipartUpload
- [ ] Unpin on DeleteObject
- [ ] Configurable pinning provider

## v0.4 — Multi-node

- [ ] PostgreSQL production deployment
- [ ] Multiple gateway instances (horizontal scaling)
- [ ] IPFS Cluster for pinset replication
- [ ] Private swarm (swarm.key) for node-to-node communication

## v0.5 — Versioning & Lifecycle

- [ ] Object versioning (enable/suspend on bucket)
- [ ] ListObjectVersions
- [ ] DeleteMarker support
- [ ] Lifecycle rules (expiration, transition)
- [ ] Bucket CORS configuration

## v0.6 — IAM & Security

- [ ] IAM users and policies
- [ ] STS temporary credentials
- [ ] Per-bucket access control
- [ ] Request rate limiting
- [ ] Audit logging

## v0.7 — Performance

- [ ] Chunk-level encrypted Range (v0.9 optimization)
- [ ] Pebble datastore backend for Kubo
- [ ] Connection pooling tuning
- [ ] Metrics and Prometheus exporter
- [ ] AES cipher reuse (avoid per-chunk key schedule)

## v0.8 — Ecosystem

- [ ] S3 Select
- [ ] Object Tagging
- [ ] Event notifications (webhook on Put/Delete)
- [ ] Static website hosting (via IPFS Gateway + DNSLink)
- [ ] rclone backend plugin
