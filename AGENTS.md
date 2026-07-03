# IPFS S3 Gateway — Agent Guide

## Overview

`ipfs-s3-gateway` is an S3-compatible gateway backed by IPFS (Kubo). It translates S3 API calls into Kubo RPC operations, with optional per-object AES-256-GCM encryption.

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

**AppState** holds: `KuboClient` (reqwest), `Store` (sea-orm DatabaseConnection), `credentials` (HashMap<access_key, SecretKey>), `master_key` (MasterKey).

## Module Map

| Module | Responsibility |
|--------|---------------|
| `main.rs` | Entry point: config, state, server |
| `config.rs` | Toml + env config loading |
| `state.rs` | AppState struct + init |
| `error.rs` | AppError → S3Error mapping |
| `auth.rs` | S3Auth impl (credential lookup) |
| `kubo/` | Kubo RPC client (add/cat/pin) |
| `store/` | sea-orm entities + CRUD |
| `crypto/` | AES-256-GCM + key wrap + chunker |
| `pinning/` | PinningService trait + Noop |
| `s3/handler.rs` | S3Impl: impl S3, delegates to ops |
| `s3/ops/` | Per-operation implementations |

## Key Design Decisions

1. **ETag = CID.** The S3 ETag for each object is its IPFS CID string (not MD5). This is a deliberate trade-off: enables `ipfs cat <cid>` for plain objects, but deviates from S3 standard. Clients with strict ETag-MD5 validation may need configuration.
2. **Default plain.** No encryption headers = plaintext storage. `x-amz-server-side-encryption: AES256` triggers SSE-S3 (gateway-managed key). SSE-C via customer key headers.
3. **Metadata in DB, content in IPFS.** All object metadata (key→CID mapping, size, encryption state) lives in sea-orm. Content lives in Kubo. This provides S3-strong-consistency via DB ACID + IPFS content addressing.
4. **No pin::rm on delete.** Kubo's pin API has no reference counting. Deleting an object only removes the DB record; the CID may still be referenced by other keys (e.g. via CopyObject). GC is disabled in dev.
5. **Multipart Complete = overall add.** Parts are individually added (each gets a part-CID). On Complete, all parts are concatenated and re-added as a single UnixFS file, producing a new root CID. This avoids manual dag-pb construction.
6. **Encrypted Range = full decrypt + slice.** MVP decrypts the entire object then slices to the requested range. v0.9 will optimize to chunk-level Range.

## Conventions

- **Rust edition 2024, MSRV 1.92.**
- **TDD:** write failing test first, then minimal implementation.
- **Streaming:** never `.collect()` an entire request body. Use `wrap_stream` / `from_stream` patterns.
- **Errors:** return `AppResult<T>` from store/kubo/crypto layers. Convert to `S3Error` at the S3 handler boundary via `Into`.
- **Commits:** semantic style (`feat:`, `fix:`, `chore:`, `test:`, `docs:`).
- **File boundaries:** one responsibility per file. If a file exceeds ~250 LOC, consider splitting.

## Testing

### Unit tests

```powershell
cargo test --lib
```

Tests live in each module's `#[cfg(test)] mod tests`. Kubo is mocked with `wiremock`. DB uses in-memory SQLite.

### Integration tests

```powershell
cargo test --test integration
```

Full axum + s3s + SQLite + mock Kubo. Uses `rust-s3` crate as the S3 client.

### End-to-end (docker compose)

```powershell
docker compose up -d --build
```

Then use `aws cli`:

```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"

aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/file.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/file.txt -
```

Verify plain object is accessible via IPFS:

```powershell
curl -X POST "http://localhost:5001/api/v0/cat?arg=<CID>"
```

Cleanup:

```powershell
docker compose down -v
```

## Configuration

Config is loaded from `config.toml` (if present) then overridden by environment variables. See `config.example.toml` for the full schema.

Key env vars:
- `IPFS_S3_BIND` — bind address (default `0.0.0.0:9000`)
- `IPFS_S3_KUBO_RPC_URL` — Kubo RPC URL (default `http://127.0.0.1:5001`)
- `IPFS_S3_DATABASE_URL` — sea-orm database URL (SQLite or Postgres)
- `IPFS_S3_ACCESS_KEY_ID` / `IPFS_S3_SECRET_ACCESS_KEY` — S3 credentials
- `IPFS_S3_MASTER_KEY` — hex-encoded 32-byte master key for SSE-S3

## Spec

See `docs/superpowers/specs/2026-07-02-ipfs-s3-gateway-design.md` for the full design document.
