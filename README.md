# IPFS S3 Gateway

An S3-compatible gateway backed by IPFS (Kubo). Translates S3 API calls into Kubo RPC operations, with optional per-object AES-256-GCM encryption.

[![CI](https://github.com/hugefiver/ipfS3/actions/workflows/ci.yml/badge.svg)](https://github.com/hugefiver/ipfS3/actions/workflows/ci.yml)
[![License: AAAPL](https://img.shields.io/badge/License-AAAPL-blue.svg)](LICENSE.md)

## Features

- **S3-compatible API** — PutObject, GetObject, HeadObject, DeleteObject, CopyObject, ListObjectsV2, ListBuckets, CreateBucket, DeleteBucket, HeadBucket
- **Multipart Upload** — CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts
- **SigV4 Authentication** — AWS Signature Version 4 via [s3s](https://github.com/s3s-project/s3s)
- **Per-object Encryption** — SSE-S3 (gateway-managed key) and SSE-C (customer-provided key) with AES-256-GCM
- **Content-addressed Storage** — ETag = IPFS CID; plain objects accessible via any public IPFS gateway (`https://ipfs.io/ipfs/<CID>`)
- **Streaming** — Never buffers entire request/response bodies; true end-to-end streaming
- **Dual Backend** — SQLite (dev) or PostgreSQL (prod) via sea-orm, single migration

## Quick Start

### Docker Compose

```bash
# Clone and configure
git clone https://github.com/hugefiver/ipfS3.git
cd ipfS3

# Set credentials
cp .env.example .env
# Edit .env with your access key, secret key, master key, and Cloudflare tunnel token

# Start
docker compose up -d --build
```

### Use with aws cli

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://my-bucket/file.txt -
```

### Access via IPFS Gateway

Plain (unencrypted) objects can be accessed directly through any public IPFS gateway:

```bash
# Get the CID from the ETag header
aws --endpoint-url http://localhost:9000 s3api head-object --bucket my-bucket --key file.txt
# ETag: "bafybei..."

# Access via public gateway
curl https://ipfs.io/ipfs/bafybei...
```

## Configuration

Configuration is loaded from `config.toml` (or path specified by `IPFS_S3_CONFIG`), then overridden by environment variables.

| Env Var                     | Description                                 | Default                 |
| --------------------------- | ------------------------------------------- | ----------------------- |
| `IPFS_S3_CONFIG`            | Path to config file                         | `config.toml`           |
| `IPFS_S3_BIND`              | Bind address                                | `0.0.0.0:9000`          |
| `IPFS_S3_KUBO_RPC_URL`      | Kubo RPC URL                                | `http://127.0.0.1:5001` |
| `IPFS_S3_DATABASE_URL`      | Database URL                                | `sqlite::memory:`       |
| `IPFS_S3_ACCESS_KEY_ID`     | S3 access key                               | `test`                  |
| `IPFS_S3_SECRET_ACCESS_KEY` | S3 secret key                               | `test`                  |
| `IPFS_S3_MASTER_KEY`        | Hex-encoded 32-byte master key (for SSE-S3) | `00...00` (dev only)    |

See [`config.example.toml`](config.example.toml) for the full schema.

## Architecture

```
aws cli / sdk
    │  (SigV4)
    ▼
axum (HTTP :9000) ── /health ──► health_check
    │
    ▼ (fallback_service)
s3s (SigV4 verify + S3 route + DTO)
    │
    ▼
S3Impl (impl S3 trait) ── holds Arc<AppState>
    │
    ├── ops/bucket.rs     → store/bucket.rs   (sea-orm)
    ├── ops/object.rs     → store/object.rs   + kubo/add,cat,pin + crypto
    └── ops/multipart.rs  → store/multipart.rs + kubo + crypto
```

**AppState** holds: `KuboClient` (reqwest), `Store` (sea-orm DatabaseConnection), `credentials` (HashMap), `master_key` (MasterKey).

## Key Design Decisions

1. **ETag = CID.** Object ETag is its IPFS CID string, not MD5. Plain objects are accessible via `ipfs cat <cid>`.
2. **Default plain.** No encryption headers = plaintext storage. `x-amz-server-side-encryption: AES256` triggers SSE-S3.
3. **Metadata in DB, content in IPFS.** S3-strong-consistency via DB ACID + IPFS content addressing.
4. **No pin::rm on delete.** Kubo's pin API has no reference counting. GC is disabled.
5. **Multipart Complete = overall add.** Parts are concatenated and re-added as a single UnixFS file.
6. **Encrypted Range = full decrypt + slice.** MVP decrypts entire object then slices. v0.9 will optimize to chunk-level Range.

## Development

```bash
# Check
cargo check

# Run tests
cargo test --lib --test integration

# Run (requires Kubo at localhost:5001)
cargo run
```

See [`AGENTS.md`](AGENTS.md) for detailed architecture and conventions.

## Tech Stack

- **Rust** (edition 2024, MSRV 1.92)
- **axum** — HTTP server
- **s3s** — S3 protocol (SigV4, routing, DTO)
- **sea-orm** — ORM (SQLite / PostgreSQL)
- **reqwest** — Kubo RPC client
- **aes-gcm** — AES-256-GCM encryption
- **Docker Compose** — dev/prod deployment

## License

Copyright (c) 2026 hugefiver. Licensed under the [AAAPL](LICENSE) (Anti American AI Public License).
