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
- **Streaming** — Request and response bodies stream end to end; the documented exception is a Range read of an encrypted object, which decrypts the full object before slicing; chunk-level encrypted Range reads are planned for v0.8
- **Dual Backend** — SQLite (dev) or PostgreSQL (prod) via sea-orm, with sequential schema migrations
- **Remote Pinning** — Asynchronous Pinata/Filebase PSA pinning with ordered policies, durable work, leases, and local soft quotas
- **Durable Import** — SigV4-authenticated CID or allowlisted HTTPS import with persisted progress, lease-based recovery, optional idempotency, optional ZIP extraction, and stale-publication fencing

## Quick Start

### Docker Compose

```bash
# Clone and configure
git clone https://github.com/hugefiver/ipfS3.git
cd ipfS3

# Set credentials
cp .env.example .env
# Edit .env with your access key, secret key, master key, optional remote
# provider tokens, and Cloudflare tunnel token

# Start
docker compose up -d --build
```

### Use with aws cli

```powershell
$env:AWS_ACCESS_KEY_ID = "your-access-key"
$env:AWS_SECRET_ACCESS_KEY = "your-secret-key"
$env:AWS_DEFAULT_REGION = "us-east-1"

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

### PutObject IPFS response headers

After a standard `PutObject` successfully completes Kubo `add`, `pin`, and
database publication, its response includes:

- `ETag: "<CID>"`
- `x-amz-meta-ipfs-cid: <CID>`
- `x-amz-meta-ipfs-url: ipfs://<CID>`

The `ipfs://` value is an IPFS URI, not a public HTTP gateway URL. These
headers are returned for plain, SSE-S3, and SSE-C uploads. For encrypted
objects, the CID identifies the ciphertext stored in IPFS, not the plaintext.

## Durable `ipfs3-import`

`ipfs3-import` is a SigV4-authenticated S3 extension. Submit a job with
`POST /{bucket}/{key}?ipfs3-import`, `Content-Type: application/xml`, and an XML
document containing exactly one source: `CID` or `URL`; query persisted progress
or a terminal result with `GET /{bucket}/{key}?ipfs3-import={job-id}`. All import
submission and status requests require the same valid SigV4 authentication as
ordinary S3 requests; the abbreviated signing values below show the HTTP shape.

```http
POST /my-bucket/imported.bin?ipfs3-import HTTP/1.1
Host: localhost:9000
Authorization: AWS4-HMAC-SHA256 Credential=ACCESS_KEY/20260813/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=SIGNATURE
x-amz-date: 20260813T000000Z
x-amz-content-sha256: SHA256_OF_XML_BODY
Content-Type: application/xml
x-ipfs3-client-token: deployment-42

<?xml version="1.0" encoding="UTF-8"?>
<IPFS3ImportRequest>
  <CID>bafkreicfodt3gdlunhj7ojhh5roa2gm554sufkc7awvmi3d4jjkrium7zm</CID>
</IPFS3ImportRequest>
```

For an HTTPS source, replace the `CID` element rather than adding a second
source:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<IPFS3ImportRequest>
  <URL>https://downloads.example.com/object.bin</URL>
</IPFS3ImportRequest>
```

An accepted request returns the persisted job identifier in the response body
and both status-discovery headers:

```http
HTTP/1.1 202 Accepted
Content-Type: application/xml
Location: /my-bucket/imported.bin?ipfs3-import=7c8b6c8f-2898-4dc7-bab4-71b13cb472b8
x-ipfs3-import-job-id: 7c8b6c8f-2898-4dc7-bab4-71b13cb472b8

<?xml version="1.0" encoding="UTF-8"?>
<IPFS3ImportAccepted><JobId>7c8b6c8f-2898-4dc7-bab4-71b13cb472b8</JobId><State>queued</State><Phase>queued</Phase></IPFS3ImportAccepted>
```

Query that path with a signed GET to read persisted progress or the terminal
result:

```http
GET /my-bucket/imported.bin?ipfs3-import=7c8b6c8f-2898-4dc7-bab4-71b13cb472b8 HTTP/1.1
Host: localhost:9000
Authorization: AWS4-HMAC-SHA256 Credential=ACCESS_KEY/20260813/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=SIGNATURE
x-amz-date: 20260813T000100Z
x-amz-content-sha256: SHA256_OF_EMPTY_BODY
```

The optional `x-ipfs3-client-token` header makes an identical replay return the
same job; reusing a token with different source, metadata, tags, content type,
or decompression prefix is rejected. Add `decompress-zip=<prefix>` to the
submission query to import a ZIP and publish the archive plus successful
entries together, for example
`POST /my-bucket/archive.zip?ipfs3-import&decompress-zip=expanded%2F`.

Jobs, attempts, and progress are persisted. If a process or worker stops, a
lease can expire so another worker reclaims and retries the job. This recovery
is job-level retry/reclaim, not byte-range URL resume; a retried URL attempt may
download the source again from byte zero. Submission validation failures are
returned synchronously as S3 errors, while execution failures appear as a
terminal state in the status XML.

Ordinary S3 reads and listings expose only committed objects. Publication is
ownership-fenced and atomic, so a stale worker cannot overwrite a newer import
or an overlapping S3 content mutation. Combined ZIP output is also hidden
until its fenced publication transaction commits.

URL imports accept only exact origins listed in `allowed_https_origins`. The
source must use HTTPS and resolve exclusively to public addresses. Redirects,
HTTP, private-network addresses, loopback/link-local sources, and forwarded
authentication are rejected; the gateway does not forward the incoming S3
`Authorization` header, cookies, or client credentials to the source. The
import extension does not support SSE-S3 or SSE-C: their submission headers are
rejected.

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
| `IPFS_S3_MASTER_KEY`        | Hex-encoded 32-byte master key (for SSE-S3 and SSE-C fingerprints) | `00...00` (dev only)    |
| `PINATA_JWT`                | Pinata token referenced by `token_env`      | unset                   |
| `FILEBASE_PINNING_TOKEN`    | Filebase token referenced by `token_env`    | unset                   |

See [`config.example.toml`](config.example.toml) for the full schema. Durable
import settings are under its `[imports]` table. CID imports are enabled by
default when `enabled = true`; URL imports remain unavailable until
`allowed_https_origins` contains each exact HTTPS origin that may be used.

`IPFS_S3_MASTER_KEY` must remain unchanged for the full lifetime of every SSE-C
object and multipart upload. The gateway uses it to verify the persisted
SSE-C key fingerprint during PutObject, UploadPart, CompleteMultipartUpload,
GetObject, HeadObject, and CopyObject. v0.4 has no master-key rotation
procedure. Changing this value can make an existing SSE-C object inaccessible.

## Remote Pinning

Remote pinning is asynchronous: when a policy or manual request selects it, a
successful S3 write publishes the object and its pinning intent durably, but
does not wait for a remote provider. At least one configured provider reaching
`pinned` is the availability success line. Other provider work may still be
pending or degraded. Remote lease durations are retention requests and control
signals, not guaranteed provider TTLs.

Every lease, including a manual lease, is eligible for quota eviction. Local
quota admission counts each unique CID by its logical S3 byte size and one pin;
shared leases do not multiply that local charge. This is a soft local control,
not provider billing or usage truth. Expiry, eviction, an explicit unpin, and
remote provider release never delete S3 metadata or IPFS content and never
remove the gateway's local Kubo pins. `UploadPart` is never remotely pinned.
Objects published by `PutObject`, `CopyObject`, or `CompleteMultipartUpload`
are evaluated against the configured destination policy; evaluation can
legitimately produce no remote work when no rule or manual request matches.

Provider credentials are never literal config values. Each provider's
`token_env` names an environment variable such as `PINATA_JWT` or
`FILEBASE_PINNING_TOKEN`. Pinata and Filebase use their built-in public
endpoints by default. Pinata also supports `api = "v3" | "legacy"` and
`strategy = "cid" | "upload"`. The CID strategy asks Pinata to pin the gateway's
existing Kubo CID; the upload strategy streams the object back from local Kubo
and uploads it to Pinata, which can support free-plan accounts that reject
pin-by-CID. Upload requests are not bound by the 30s request timeout that
applies to every other provider call, so large objects are limited by the
provider and by `max_bytes` rather than by a gateway deadline; only the connect
phase is bounded. The upload strategy also requires Pinata to return the same
CID the gateway computed, otherwise the submit fails permanently. An `endpoint`
override is only for tests or private compatible services; Pinata V3 upload
endpoint overrides use `upload_endpoint`.

### Policies and coordination

Policies use ordered first-match evaluation. A rule matches an exact bucket or
`*`, followed by a literal key-prefix match. An `always` rule creates automatic
pinning intent; a `request` rule permits manual tag control. In `one` mode,
providers are considered by priority and the chosen provider is sticky, with
failover rather than continuous rebalancing. In `all` mode, work fans out to
every provider named by the rule.

The durable worker coordinates retries per unique provider/CID request, not
once per lease. Failed requests use bounded backoff and stop after eight failed
attempts. When a crash-recovered `Submit` job is ambiguous, the worker performs
a provider `find` before another `POST`; the local quota reservation remains
held while that ambiguity is unresolved.

### Standard S3 tag control

The reserved controls are `ipfs-s3:pin`, `ipfs-s3:duration`,
`ipfs-s3:retain-until`, and `ipfs-s3:content`. The following PowerShell example
requests a 30-day manual lease through the standard PutObject
`x-amz-tagging` header (AWS CLI percent-encodes the colon in query-string tag
syntax):

```powershell
$env:AWS_ACCESS_KEY_ID = "your-access-key"
$env:AWS_SECRET_ACCESS_KEY = "your-secret-key"
$env:AWS_DEFAULT_REGION = "us-east-1"

aws --endpoint-url http://localhost:9000 s3api put-object `
  --bucket my-bucket `
  --key file.txt `
  --body .\file.txt `
  --tagging "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=30d"
```

Object-tagging APIs can inspect, renew, cancel, or clear that manual intent:
`ipfs-s3:pin=false` cancels an active manual lease and cannot be combined with
the duration, retain-until, or content controls.

```powershell
# Extend an existing manual lease; retain-until must be RFC 3339 and cannot shorten it.
aws --endpoint-url http://localhost:9000 s3api put-object-tagging `
  --bucket my-bucket `
  --key file.txt `
  --tagging 'TagSet=[{Key=ipfs-s3:pin,Value=true},{Key=ipfs-s3:retain-until,Value=2030-01-01T00:00:00Z}]'

aws --endpoint-url http://localhost:9000 s3api get-object-tagging `
  --bucket my-bucket `
  --key file.txt

# Removes all object tags and cancels active manual pinning intent.
aws --endpoint-url http://localhost:9000 s3api delete-object-tagging `
  --bucket my-bucket `
  --key file.txt
```

`ipfs-s3:content=decompressed` applies only when the same PutObject (or the
multipart upload completed as that object) uses the gateway's signed
`?decompress-zip=<prefix>` extension. Its PutObject `x-amz-tagging` header has
this form:

```text
ipfs-s3%3Apin=true&ipfs-s3%3Aduration=30d&ipfs-s3%3Acontent=decompressed
```

An expired manual lease may be restored with `ipfs-s3:retain-until` only when
the original remote reservation still exists because the unpin/release race
has not been confirmed. Once release is confirmed, renewal is rejected.
Renewal never reconstructs decompressed targets, and cancelled or evicted
leases cannot be revived.

## Architecture

```text
aws cli / sdk
    │  (SigV4)
    ▼
axum (HTTP :9000) ── /health ──► health_check
    │  (fallback_service)
    ▼
s3s (SigV4 verify + standard S3 dispatch + custom S3Route)
    │
    ├── S3Impl (impl S3 trait) ── holds Arc<AppState>
    │   ├── ops/bucket.rs     → store/bucket.rs   (sea-orm)
    │   ├── ops/object.rs     → store/object.rs   + kubo/add,cat,pin + crypto
    │   ├── ops/multipart.rs  → store/multipart.rs + kubo + crypto
    │   └── pinning/          → PSA clients + durable jobs + policy/lease coordination
    │
    └── GatewayRoute (composite custom route)
        ├── ImportObjectRoute → persisted import jobs and status XML
        └── DecompressZipRoute → signed direct ZIP extraction

ImportCoordinator (constructed separately from AppState)
    └── durable import worker → store/import + Kubo + optional ZIP publication
```

The durable import flow is:

```text
signed POST
  → GatewayRoute / ImportObjectRoute
  → validate source and persist queued job
  → 202 Accepted + job ID
  → durable worker performs CID pin or HTTPS download/add
  → optional ZIP extraction
  → ownership-fenced atomic publication
  → signed GET returns progress or terminal result
```

**AppState** holds `KuboClient` (reqwest), `Store` (sea-orm
`DatabaseConnection`), `credentials` (`HashMap`), `master_key` (`MasterKey`),
and `pinning` (`Arc<PinningCoordinator>`). `ImportCoordinator` is constructed separately from `AppState`, passed to `GatewayRoute`, and starts its durable worker beside the pinning worker.

## Key Design Decisions

1. **ETag = CID.** Object ETag is its IPFS CID string, not MD5. Plain objects are accessible via `ipfs cat <cid>`.
2. **Default plain.** No encryption headers = plaintext storage. `x-amz-server-side-encryption: AES256` triggers SSE-S3.
3. **Metadata in DB, content in IPFS.** S3-strong-consistency via DB ACID + IPFS content addressing.
4. **No pin::rm on delete.** Kubo's pin API has no reference counting. GC is disabled.
5. **Multipart Complete = overall add.** Parts are concatenated and re-added as a single UnixFS file.
6. **Encrypted Range = full decrypt + slice.** The gateway currently decrypts the entire encrypted object and then slices the response; chunk-level encrypted Range reads are planned for v0.8.

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
