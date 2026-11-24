# ipfS3

An S3-compatible object storage gateway backed by IPFS (Kubo). Upload and manage files with standard S3 tools (`aws cli`, SDKs, rclone), and read plaintext objects directly over IPFS by CID.

[![CI](https://github.com/hugefiver/ipfS3/actions/workflows/ci.yml/badge.svg)](https://github.com/hugefiver/ipfS3/actions/workflows/ci.yml)
[![License: AAAPL](https://img.shields.io/badge/License-AAAPL-blue.svg)](LICENSE.md)

## Features

- **S3 API** — Buckets, objects, batch delete, `ListObjects` v1/v2, multipart upload, versioning, tagging, bucket CORS, and SigV4 (including presigned GET/PUT).
- **IPFS-native storage** — Files live in Kubo and metadata lives in SQLite or PostgreSQL. Every object's S3 `ETag` is its IPFS CID, so plaintext files work directly with `ipfs cat <cid>` or any IPFS HTTP gateway.
- **Optional encryption** — Plaintext by default; supports streaming AES-256-GCM encryption via SSE-S3 (`AES256`) and SSE-C, including encrypted byte-range reads.
- **Access control & limits** — Bucket ACLs (`private`, `authenticated-read`, `public-read`, `public-read-write`), local user/bucket/anonymous policies, offline temporary session credentials, bucket storage quotas, and per-caller rate limits.
- **IPFS gateway redirects** — Optionally answer `GetObject` with `307 Temporary Redirect` to an IPFS HTTP gateway, matched by bucket, prefix, filename glob, user, or anonymous caller. Clients can override with `X-IPFS3-Download-Gateway: prefer | require | proxy`.
- **Remote pinning** — Async backup pinning to Pinata, Filebase, remote Kubo RPC, or IPFS Cluster with per-policy or per-tag TTLs (`ipfs-s3:pin`, `ipfs-s3:duration`, `ipfs-s3:retain-until`).
- **Lifecycle & tiering** — Expiration, incomplete multipart cleanup, and `STANDARD` → `STANDARD_IA` transitions to an optional cold Kubo node.
- **Imports, ZIP extraction & directory CIDs** — Async CID or allowlisted HTTPS imports, server-side ZIP extraction with optional UnixFS directory roots, and on-demand UnixFS directory CIDs from `ListObjectsV2`.
- **Static Pastebin** — Includes a zero-backend static web UI in [`pastebin/`](pastebin/README.md) for publishing text, files, encrypted shares (client-side E2EE), and ZIP directories.

## Quick Start

### Docker Compose (local / SQLite)

```bash
git clone https://github.com/hugefiver/ipfS3.git
cd ipfS3
cp .env.example .env
# Edit .env with your S3 keys and 64-hex master key
docker compose up -d --build
```

For production deployments, use [`docker-compose.postgres.yml`](docker-compose.postgres.yml) (single gateway + PostgreSQL 18 + Kubo) or [`docker-compose.multi-gateway.yml`](docker-compose.multi-gateway.yml) (two gateways behind Nginx sharing PostgreSQL + Kubo).

### Use with AWS CLI

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp hello.txt s3://my-bucket/hello.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket/
```

### Read via IPFS

The object's `ETag` is its CID:

```bash
curl -I http://localhost:9000/my-bucket/hello.txt
# ETag: "bafybei..."

curl https://ipfs.io/ipfs/bafybei...
```

## Configuration

Settings load from `config.toml` (or `IPFS_S3_CONFIG`) and can be overridden with environment variables. See [`config.example.toml`](config.example.toml) for the full schema.

| Environment Variable | Purpose | Default |
| --- | --- | --- |
| `IPFS_S3_CONFIG` | Path to TOML config file | `config.toml` |
| `IPFS_S3_BIND` | S3 HTTP listen address | `0.0.0.0:9000` |
| `IPFS_S3_KUBO_RPC_URL` | Primary Kubo RPC URL | `http://127.0.0.1:5001` |
| `IPFS_S3_DATABASE_URL` | SQLite or PostgreSQL URL | `sqlite::memory:` |
| `IPFS_S3_ACCESS_KEY_ID` | Static S3 access key | `test` |
| `IPFS_S3_SECRET_ACCESS_KEY` | Static S3 secret key | `test` |
| `IPFS_S3_MASTER_KEY` | 64-char hex key for SSE-S3 & SSE-C fingerprints | all zeros (dev only) |
| `IPFS_S3_METRICS_BIND` | Optional Prometheus `/metrics` address | disabled |
| `IPFS_S3_TRUST_PROXY_HEADERS` | Trust reverse-proxy client IP headers for rate limits | `false` |

Health endpoints on the S3 port:
- `GET /health` — process liveness (`200 OK`)
- `GET /ready` — database readiness (`200 READY` / `503 NOT READY`)

CLI helpers: `--config-explain` prints the effective non-secret config; `--ready-probe` checks a running gateway's readiness.

## Key Design Trade-offs

- **ETag = CID** — `ETag` is the IPFS CID rather than MD5. Clients that strictly check MD5 ETags may need that check disabled (see [client compatibility](docs/client-compatibility.md)).
- **Metadata in DB, bytes in Kubo** — Strong S3 consistency comes from SQLite/PostgreSQL transactions while Kubo stores content-addressed blocks.
- **S3 delete keeps Kubo pins** — Because multiple keys or versions can share a CID, deleting an S3 object does not unpin it from the primary Kubo node yet (planned in [`ROADMAP.md`](ROADMAP.md)).
- **Fixed master key** — Keep `IPFS_S3_MASTER_KEY` unchanged for the lifetime of any encrypted objects; key rotation is not implemented.
- **Multipart completion** — Parts are concatenated and re-added as one UnixFS file on completion so the final object gets a single root CID.

## Documentation

- [Configuration reference](config.example.toml) — full annotated TOML config (redirects, rate limits, pinning, lifecycle, ZIP, imports)
- [Local policies & bucket ACLs](docs/local-policies.md) — bucket ACLs, prefix/anonymous rules, tag conditions
- [Temporary sessions](docs/local-sessions.md) — offline STS-style session credentials
- [Bucket storage quotas](docs/bucket-quota.md) — per-bucket logical byte limits
- [Database storage & upgrades](docs/database-storage.md) — SQLite/PostgreSQL setup and schema migrations
- [Directory CID](docs/directory-cid.md) — generating a UnixFS directory CID for an S3 prefix
- [Remote pinning recovery](docs/pinning-recovery.md) — inspecting and recovering remote pin tasks
- [Client compatibility](docs/client-compatibility.md) — notes for AWS CLI, SDKs, and S3 clients
- [Static Pastebin](pastebin/README.md) — browser upload/share frontend and Cloudflare Pages deployment
- [Roadmap](ROADMAP.md) — planned features

## Development

Requires Rust 1.92+ (edition 2024).

```bash
cargo check
cargo test --lib --test integration
cargo run   # requires Kubo at http://127.0.0.1:5001
```

See [testing guidance](docs/testing.md) for PostgreSQL, Docker, and end-to-end suites.

## License

Copyright (c) 2026 hugefiver. Licensed under the [AAAPL](LICENSE.md) (Anti American AI Public License).
