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
- **Object Versioning** — Unversioned, Enabled, and Suspended bucket states with S3-style version IDs, delete markers, and `ListObjectVersions`
- **Lifecycle Rules** — Expiration, incomplete multipart abort, and current/noncurrent `STANDARD -> STANDARD_IA` transitions to an optional independent cold Kubo
- **Streaming** — Request and response bodies stream end to end; the documented exception is a Range read of an encrypted object, which decrypts the full object before slicing; chunk-level encrypted Range reads are planned for v0.8
- **Dual Backend** — SQLite (dev) or PostgreSQL (prod) via sea-orm, with sequential schema migrations
- **Remote Pinning** — Asynchronous Pinata/Filebase PSA pinning with ordered policies, durable work, leases, and local soft quotas
- **Durable Import** — SigV4-authenticated CID or allowlisted HTTPS import with persisted progress, lease-based recovery, optional idempotency, optional ZIP extraction, and stale-publication fencing

## Hardening boundaries

- **Mutation-lease upgrades:** Standard content mutations use a database-clock
  lease of 120 seconds and long requests renew it every 30 seconds. Before the
  migration that introduces this lease, stop and drain every old writer. Do not
  run old and new writers against the upgraded database together: an old writer
  bypasses the fence. Failed renewal or a crashed process is recovered after
  lease expiry, not by an immediate handoff.
- **Unsupported write shapes:** `POST` browser `multipart/form-data` is rejected
  before the body reaches s3s, including requests with media-type parameters.
  `If-Match` and `If-None-Match` on `PutObject` or multipart completion are
  explicitly rejected. They do not provide compare-and-swap writes.
- **ZIP extraction:** Direct decompression and ZIP import share a limit of
  10,000 local entries and 64 MiB of conservative metadata reservations per
  archive, independent of the 8 GiB decompressed-byte limit. Final
  prefix-plus-entry keys are limited to 1024 UTF-8 bytes. CRC32 and size checks,
  including Deflate data descriptors, must pass before an entry succeeds. See
  the [ZIP safety boundary](src/zip/README.md) for accounting, compatibility,
  and retained-staging details.
- **Shutdown and failover:** On Unix, `SIGTERM` and `SIGINT` begin one shared
  30-second graceful drain for HTTP and workers; Compose gives the gateway 40
  seconds. The multi-gateway Nginx configuration retries only `GET` and `HEAD`.
  A write-side upstream failure fails the request and is never replayed.

**Implementation status (2026-09-19):** The hardening code is present. Final
suite verification is still running, so this document does not claim a final
PASS result or treat historical F1 evidence as evidence for this source state.

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

### PostgreSQL production baseline

`docker-compose.postgres.yml` is an explicit single-node production baseline:
one PostgreSQL 17 service, one Kubo service, and one gateway. The default
`docker-compose.yml` remains the SQLite development stack. PostgreSQL and Kubo
publish no host ports; only the gateway is published, and its host bind must be
an explicit non-wildcard address.

The PostgreSQL password is interpolated into a URL and must contain only
`A-Z`, `a-z`, `0-9`, `.`, `_`, `~`, or `-`. Keep the 64-hex-character master
key unchanged for the lifetime of encrypted data. Compose environment values
can be inspected by principals with Docker or host access, so restrict that
access and do not print `docker compose config` output into logs.

```powershell
$env:POSTGRES_PASSWORD = "replace-with-url-safe-password"
$env:IPFS_S3_ACCESS_KEY_ID = "replace-with-access-key"
$env:IPFS_S3_SECRET_ACCESS_KEY = "replace-with-secret-key"
$masterKey = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($masterKey)
$env:IPFS_S3_MASTER_KEY = [Convert]::ToHexString($masterKey).ToLowerInvariant()
$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
$env:IPFS_S3_GATEWAY_PORT = "9000"

docker compose -f docker-compose.postgres.yml config --quiet
docker compose -f docker-compose.postgres.yml up --detach --build --wait --wait-timeout 300
```

`GET /health` is process liveness and returns `200 OK` with `OK` while the HTTP
server runs. `GET /ready` is database readiness and returns `200 OK` with
`READY` only when PostgreSQL responds; it returns `503 Service Unavailable`
with `NOT READY` after a database error or two-second timeout. Readiness does
not test Kubo or remote pinning providers.

Stop the production stack without deleting its named data volumes:

```powershell
docker compose -f docker-compose.postgres.yml down --remove-orphans
```

Never add `--volumes` to the production shutdown command: it deletes the
PostgreSQL and IPFS named volumes. This single-gateway baseline does not provide
PostgreSQL high availability, backups, TLS, IPFS Cluster, a private swarm, a
secret manager, or key rotation. For the bounded two-gateway topology, use the
separate deployment below.

### Two-gateway horizontal scaling

`docker-compose.multi-gateway.yml` runs exactly two normal gateway replicas
behind one Nginx entry point. Both replicas share one PostgreSQL 17 service, one
Kubo service, identical S3 credentials, and the same master key. There is no
session stickiness: PostgreSQL provides shared metadata and Kubo provides shared
content. PostgreSQL startup migrations are serialized with a transaction-scoped
advisory lock before either gateway binds its listener.

Set the four required secret values as in the PostgreSQL baseline, then provide
an explicit non-wildcard load-balancer bind and port:

```powershell
$env:POSTGRES_PASSWORD = "replace-with-url-safe-password"
$env:IPFS_S3_ACCESS_KEY_ID = "replace-with-access-key"
$env:IPFS_S3_SECRET_ACCESS_KEY = "replace-with-secret-key"
$env:IPFS_S3_MASTER_KEY = "replace-with-one-stable-64-hex-character-key"
$env:IPFS_S3_LOAD_BALANCER_BIND = "127.0.0.1"
$env:IPFS_S3_LOAD_BALANCER_PORT = "9000"

docker compose -f docker-compose.multi-gateway.yml config --quiet
docker compose -f docker-compose.multi-gateway.yml up --detach --build --wait --wait-timeout 300
```

Only Nginx is published by the production file. PostgreSQL, Kubo, and the two
gateway replicas have no host ports. Nginx preserves the signed Host header,
streams request and response bodies without buffering, and passively retries
the other replica for bounded connection/timeout/502/503/504 failures. Its
`/health` and `/ready` routes both proxy a selected gateway's database-only
`/ready`; they are not active health checks for every component.

Both replicas run the existing durable import and pinning workers. This topology
configures no remote pinning providers and does not provide cluster-wide remote
provider rate limits, concurrency, health, or fairness. Direct replica ports in
`tests/compose.multi-gateway-validation.yml` are disposable loopback-only test
surfaces, not operator endpoints.

Stop the stack without deleting PostgreSQL or Kubo data:

```powershell
docker compose -f docker-compose.multi-gateway.yml down --remove-orphans
```

This is gateway-layer horizontal scaling, not full-stack high availability. The
single PostgreSQL instance, Kubo instance, Nginx process, and host remain single
points of failure. It does not add PostgreSQL HA, Kubo replication, load-balancer
HA, IPFS Cluster, a private swarm, TLS, distributed provider coordination, or
master-key rotation.

### IPFS Cluster pinset replication

`docker-compose.cluster.yml` is a separate one-gateway profile, not combined
with horizontal scaling. It starts exactly seven roles: PostgreSQL 17; Kubo A
and Kubo B at v0.43.0 with separate repositories; a one-shot swarm-bootstrap
service; Cluster A and Cluster B in full CRDT mode at v1.1.6 with separate
identities; and the gateway through the Cluster A proxy. Each peer's connector
and proxy forwarder target its paired Kubo DNS endpoint. They never use a
container-local Kubo loopback address or a different peer's Kubo endpoint.
Replication is min=max 2. This profile is same-host only and makes no claim
beyond one Docker host.

Kubo A and Kubo B use a shared Kubo `swarm.key` PSK that is separate from
`IPFS_S3_CLUSTER_SECRET`. The one-shot swarm-bootstrap service ensures that
Kubo A and Kubo B each retain one persistent Peering entry for the other as their
sole peer. AutoConf disabled, public bootstrap is empty, routing is `none`, and
mDNS is disabled. The server-profile RFC1918 `Swarm.AddrFilters` are cleared
solely for the PSK-gated internal Docker bridge.

The PSK is limited to PSK membership and libp2p connection protection for Kubo
node-to-node traffic. It does not control container egress, does not encrypt or
authenticate REST, and does not provide high availability, multi-host discovery,
online rotation, or member revocation. Rotation requires coordinated downtime
and is not automated.

Set fresh values in the current PowerShell session. The password, access key,
and secret below are cryptographically random URL-safe values. The 32-byte
master key and 32-byte Cluster secret are rendered as lowercase hex. Generate
the shared swarm key from 32 random bytes before the remaining variables. Its
file has exactly three LF-terminated lines: `/key/swarm/psk/1.0.0/`, `/base16/`,
and the lowercase 64-hex-character key. The generator writes UTF-8 without a
BOM through `CreateNew`, sets only the exact path in
`$env:IPFS_S3_SWARM_KEY_FILE`, and does not print the key or a digest. Use a
persistent operator path outside the repository, protect the directory and file
with host ACLs. Never commit the swarm-key file. The bind and port are explicit,
and Compose ignores any `.env` file.

This bounded same-host profile publishes the gateway only on fixed host loopback
`127.0.0.1`. `IPFS_S3_GATEWAY_BIND=127.0.0.1` is a required acknowledgement;
other values are rejected. Direct non-loopback publication is unsupported.
External clients require a separately secured TLS/auth reverse proxy, which is
out of scope and not shipped by this profile.

```powershell
$swarmKeyDirectory = Join-Path $HOME ".ipfs3/secrets"
$null = [IO.Directory]::CreateDirectory($swarmKeyDirectory)
$swarmKeyPath = Join-Path $swarmKeyDirectory "swarm.key"
$swarmKeyBytes = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($swarmKeyBytes)
$swarmKeyHex = [Convert]::ToHexString($swarmKeyBytes).ToLowerInvariant()
$swarmKeyText = "/key/swarm/psk/1.0.0/`n/base16/`n$swarmKeyHex`n"
$swarmKeyPayload = [Text.UTF8Encoding]::new($false).GetBytes($swarmKeyText)
$swarmKeyStream = [IO.File]::Open(
    $swarmKeyPath,
    [IO.FileMode]::CreateNew,
    [IO.FileAccess]::Write,
    [IO.FileShare]::None
)
try {
    $swarmKeyStream.Write($swarmKeyPayload, 0, $swarmKeyPayload.Length)
    $swarmKeyStream.Flush($true)
} finally {
    $swarmKeyStream.Dispose()
}
$env:IPFS_S3_SWARM_KEY_FILE = $swarmKeyPath

function New-UrlSafeSecret {
    param([int]$ByteCount = 32)

    $bytes = [byte[]]::new($ByteCount)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    [Convert]::ToBase64String($bytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
}

$env:POSTGRES_PASSWORD = New-UrlSafeSecret
$env:IPFS_S3_ACCESS_KEY_ID = New-UrlSafeSecret
$env:IPFS_S3_SECRET_ACCESS_KEY = New-UrlSafeSecret
$masterKey = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($masterKey)
$env:IPFS_S3_MASTER_KEY = [Convert]::ToHexString($masterKey).ToLowerInvariant()
$clusterSecret = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($clusterSecret)
$env:IPFS_S3_CLUSTER_SECRET = [Convert]::ToHexString($clusterSecret).ToLowerInvariant()
$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
$env:IPFS_S3_GATEWAY_PORT = "9000"
$env:COMPOSE_DISABLE_ENV_FILE = "1"

docker compose -f docker-compose.cluster.yml config --quiet
if ($LASTEXITCODE -ne 0) { throw "Cluster Compose configuration failed" }

docker compose -f docker-compose.cluster.yml up --detach --build --wait --wait-timeout 300
if ($LASTEXITCODE -ne 0) { throw "Cluster profile did not become healthy" }
```

These production commands use the base `docker-compose.cluster.yml` and its
required environment paths. Do not add the validation override to production.

Store these generated secrets before the first write and restore them unchanged
for every later start. Changing the master key breaks encrypted objects;
changing the Cluster secret breaks Cluster membership. Keep the same swarm-key
file path and content for both Kubo peers.

Local service health is insufficient. The shipped validator first runs an
identity-suppressed no-write exact-two-peer gate, which reports only
count/normalized v1.1.6, then proves complete production
`add(pin=false)` -> `pin/add` -> `cat` compatibility before replication. It
does not print identities.

A PUT followed by an immediate GET proves the local A path only. Replication
eventually reaches exact 2/2 allocations and is accepted only when both physical
tracker states are `pinned`. Kubo B reads only afterward, and the gateway has no
fallback. `S3 DELETE` removes metadata and produces `HEAD 404`, but intentionally
does not unpin, so the allocation and Kubo B bytes remain.

Peer-B stop/restart evidence proves loss and recovery of the two-pin state with
the existing volumes. It does not demonstrate high availability, and 2/2 does
not guarantee writes while degraded. PostgreSQL, gateway, Cluster A, Kubo A, and
the Docker host are single points. This profile does not provide PostgreSQL,
Kubo, Cluster, gateway, or host high availability.

Production PostgreSQL, Kubo, Cluster REST, Cluster proxy, and swarm endpoints
are internal. The validation alone exposes the Cluster A proxy at loopback
`59103`; any non-loopback access needs TLS and authentication, and TLS and
authentication are not implemented. Validation-only Kubo C on loopback `55102`
uses a wrong key to prove it cannot join the private swarm. Hosted job: `NOT RUN`.

For production shutdown, use only the following command and check its exit
status:

```powershell
docker compose -f docker-compose.cluster.yml down --remove-orphans
if ($LASTEXITCODE -ne 0) { throw "Cluster shutdown failed" }
```

Never use the `--volumes` flag: the five named volumes are durable.

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

## Object versioning

The [approved design](docs/superpowers/specs/2026-08-25-object-versioning-design.md)
and [sanitized LOCAL evidence](docs/object-versioning-evidence-2026-08-25.log)
describe the implemented scope. **Unversioned** buckets overwrite the current
object; **Enabled** assigns opaque VersionIds to each write and can thereafter
only be **Suspended**; **Suspended** overwrites the literal `null` version while
retaining prior opaque versions.

Deletes in versioned states create a delete marker. A current read of a marker
returns `404` (`NoSuchKey`); `HeadObject` or `GetObject` targeting that marker
explicitly returns `405`. Deleting the exact marker restores the previous
version. `GetObject`, `HeadObject`, `CopyObject`, object tagging, and
`DeleteObject` support current or exact-version requests.

`ListObjectVersions` returns versions and delete markers in combined order; its
key-marker and version-id-marker pagination continue that same order.
`PutObject`, `CopyObject`, completed multipart uploads, `ipfs3-import`, and ZIP
extraction all publish version-aware objects. Each version retains `ETag = CID`
and its encryption metadata. Deleting a version removes only public metadata:
gateway Kubo pins are retained and `pin/rm` is not called. Bucket deletion
requires exact removal of every public version and delete marker.

Non-goals: MFA Delete, Object Lock, pin reclamation, and replication.

## Lifecycle rules

The [approved expiration design](docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md)
and [sanitized LOCAL evidence](docs/lifecycle-expiration-evidence-2026-08-26.log)
describe expiration behavior. `PutBucketLifecycleConfiguration`,
`GetBucketLifecycleConfiguration`, and `DeleteBucketLifecycle` support strict,
atomic replacement of lifecycle rules with expected-owner enforcement.

Supported actions are current-version `Expiration` by date or days,
`NoncurrentVersionExpiration` for content and delete markers, and
`ExpiredObjectDeleteMarker`. Eligibility uses database UTC and UTC-midnight
semantics. Durable scan/action leases, claim epochs, final policy
revalidation, and version-aware ownership guards make execution safe across
multiple gateway replicas. Lifecycle deletion retains Kubo pins and never
calls `pin/rm`.

`AbortIncompleteMultipartUpload` is supported with all-objects or prefix selectors.
`DaysAfterInitiation=N` becomes due at the next UTC midnight after N full days
from initiation, using the database UTC clock; uploading parts does not reset it.
Lifecycle aborts are durable, bucket-locked actions safe across gateway
replicas; an explicit abort of an absent upload still returns `NoSuchUpload`,
while a lifecycle action observing the same absence succeeds idempotently.
Aborted CID pins are retained; no lifecycle path calls `pin/rm`.
Abort response headers (`x-amz-abort-date`, `x-amz-abort-rule-id`) and
`ListMultipartUploads` are not implemented.
PG17 runner (requires two gateways, a load balancer, and Kubo endpoints):
`pwsh tests/run-postgres-lifecycle-validation.ps1 -PostgresUrl <url>`
See [validation evidence](tests/results/postgres-lifecycle-validation/) for run status.

### Transition to independent cold storage

Current-version `Transition` (Date or Days) and `NoncurrentVersionTransition`
(NoncurrentDays, with the supported newer-version retention filter) move content
from `STANDARD -> STANDARD_IA`. Delete markers and incomplete multipart uploads
are not transition targets. This is a real transfer to an **independent cold
Kubo node**, not a metadata-only class label, an IPFS Cluster replica, or a
Pinata/Filebase provider pin. The original DAG is streamed to cold, its original
CID and complete local pinned content are verified, and only then is that
immutable version's cold residency/class published atomically.

CID, ETag, public version ID, object bytes, metadata, tags, and encryption remain
unchanged, including SSE-S3 key wrapping and SSE-C key identity. GET/HEAD, Range,
object listings and version listings use the selected version's residency;
different versions sharing a CID can have different classes. Copying an IA
source creates a new `STANDARD` destination; it does not reverse the source's
transition.

- **Default is hot-only.** Without `[cold_kubo]` / `IPFS_S3_COLD_KUBO_RPC_URL`,
  new and legacy objects remain `STANDARD`; new lifecycle configurations
  containing either transition action are rejected atomically with
  `InvalidRequest`. Expiration/abort-only rules continue to work. Existing
  objects are backfilled as `STANDARD`, with hot-local verification before they
  can transition; backfill does not change CID, version identity or encryption.
- **Temporary cold outage is not a configuration syntax error.** With cold
  configured, valid transition rules can still be stored. Execution retries
  transient failures with bounded attempts and fails safe without publishing IA
  if copy/verification cannot complete. An already-published IA read fails when
  cold is unavailable; it **does not fall back to hot**, even if hot still has
  the bytes. `/health` is liveness and `/ready` is database readiness, not proof
  that either Kubo tier is usable.
- **Disabling/deleting rules cancels only unpublished actions.** Published IA
  versions stay IA; recovery can finish their idempotent logical cleanup.
  Keep cold configured and its repository intact for those versions.
- **No physical reclamation promise.** Transition cleanup releases only its
  logical references. No lifecycle path calls `pin_rm` (`pin/rm`), runs GC or
  deletes blocks. Shared-CID owners and pinning leases remain protected; hot
  disk usage need not shrink, and extra cold copies may remain after failures.
- Archive/restore tiers, provider pseudo-tiers, direct IA writes (PUT/COPY/
  multipart initiation), and reverse lifecycle transitions are unsupported.
  Unsupported actions/classes reject the entire configuration, not just one
  rule. This is not AWS archival storage or its billing/minimum-size policy.

**Upgrade all gateway and worker instances together before enabling transitions.**
Every instance sharing the database must support the new residency/action schema
and tier-aware reader/writer, with identical hot/cold bindings to the same two
node identities and separate repositories/volumes. Do not mix old workers or
readers with the upgraded database. Keep the master key stable. Disabling rules
is not a schema rollback: do not blindly downgrade binaries or remove residency
tables/cold storage after IA publication. A separately designed reverse migration
would be required; this release does not provide one.

See the [lifecycle program design](docs/superpowers/specs/2026-08-26-lifecycle-program-design.md),
[transition implementation plan](docs/superpowers/plans/2026-09-12-lifecycle-transition.md),
and [F1 CURRENT evidence](tests/results/lifecycle-transition/CURRENT.md).
The linked CURRENT receipt identifies the sole authoritative full F1 run (exit
0); the deployment smoke below is not a replacement for that full validation matrix.

F2 deployment smoke (2026-09-13, current-source offline image
`ipfs3-f2-current:20260913-b631aa`, image ID prefix `4233de1f7f3f`) verified a
signed Days=1 transition using one precisely aged disposable SQLite version,
DB cold/IA publication, unchanged CID/ETag and 67 bytes, IA GET/HEAD/listing,
and complete pinned cold-local content with hot stopped; its project data and
unique image were removed after logs-first cleanup.

### Opt-in dual-Kubo development deployment

The default `docker compose up` and `config.docker.toml` remain single-hot;
`config.example.toml` shows the optional, commented-out `[cold_kubo] rpc_url`.
Use the explicit overlay with Docker Compose **2.24.4+**:

```powershell
docker compose -p ipfs3-lifecycle -f docker-compose.yml -f docker-compose.lifecycle.yml config --quiet
docker compose -p ipfs3-lifecycle -f docker-compose.yml -f docker-compose.lifecycle.yml up --detach --build --wait --wait-timeout 180 kubo cold gateway
curl.exe --fail http://127.0.0.1:9000/ready
```

Build the gateway from the upgraded source; an old cached `latest` image is not
evidence of transition support. For an already-built, verified gateway and cached
Kubo images, replace `--build` with `--no-build --pull never`. The overlay pins
both Kubo nodes to the F1-validated `ipfs/kubo:v0.43.0`, with separate project-scoped
`ipfs_data` (hot) and `cold_ipfs_data` (cold) named volumes. Cold runs offline as
in F1: CAR import over private RPC provides its data, not swarm retrieval from hot.
Neither Kubo publishes host ports in this overlay; the gateway binds only to
`127.0.0.1:${IPFS_S3_LIFECYCLE_PORT:-9000}`. The gateway uses
`IPFS_S3_COLD_KUBO_RPC_URL=http://cold:5001`, and waits for both nodes at startup.
The explicit service list does not start the optional Cloudflare tunnel.

This is a same-host SQLite development topology, not a production HA, Cluster,
or provider setup. It does not modify the separate PostgreSQL/multi-gateway
deployment files. Multi-gateway operators must configure **every gateway and
worker with the same hot/cold tier bindings**, never one cold node per replica.
Keep RPC private and protect Docker/host access; Compose configuration output
may include secrets. Do not reuse a hot repository or volume as cold.

Inspect logs before shutdown, and retain data volumes:

```powershell
docker compose -p ipfs3-lifecycle -f docker-compose.yml -f docker-compose.lifecycle.yml logs --no-color
docker compose -p ipfs3-lifecycle -f docker-compose.yml -f docker-compose.lifecycle.yml down --remove-orphans
```

Only for a disposable smoke project with no retained data, add `--volumes` to
`down` and verify that no containers, networks or volumes with that project's
`com.docker.compose.project` label remain. Never use that cleanup on a persistent
deployment; removing cold data makes published IA versions unreadable.

## Bucket CORS

The [approved Bucket CORS design](docs/superpowers/specs/2026-08-31-bucket-cors-design.md)
and [sanitized LOCAL evidence](docs/bucket-cors-evidence-2026-08-31.log)
cover path-style Bucket CORS. The gateway provides native signed management CRUD
through `PutBucketCors`, `GetBucketCors`, and `DeleteBucketCors`, validating a
configuration body with MD5 or the AWS CLI default `CRC64NVME`.

Both unsigned preflight requests and signed actual responses, including S3
errors, receive the matching CORS headers. The policy also covers custom import
and decompress routes. Rules keep their submitted order, so the first matching
rule wins. `/health` and `/ready` are excluded. Non-goals: virtual-hosted-style
routing, IAM or bucket-policy evaluation, directory buckets, TLS or edge
behavior, global CORS, and Kubo CORS.

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
axum (HTTP :9000) ── /health ──► unconditional liveness
                  └─ /ready  ──► bounded database ping
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
See [testing guidance](docs/testing.md) for default and environment-backed
checks, and the [dependency audit](docs/dependency-audit-2026-09-19.md) for the
2026-09-19 advisory scope and remaining feature-boundary findings.

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
