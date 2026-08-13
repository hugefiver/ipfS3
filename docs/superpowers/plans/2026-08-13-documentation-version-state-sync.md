# Documentation and Version State Sync Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Synchronize `README.md` and `ROADMAP.md` with the delivered durable `ipfs3-import` behavior, accepted 2026-08-13 AWS CLI evidence, current package version, and v0.8 encrypted Range assignment without changing implementation or release state.

**Architecture:** This is a documentation-only synchronization: `README.md` becomes the durable import usage, configuration, security, recovery, and architecture contract, while `ROADMAP.md` records accepted evidence and delivered-but-unassigned work. Inline PowerShell assertions provide RED/GREEN documentation checks, followed by one integrated read-only audit that protects package metadata, the historical import plan, the changed-path boundary, and Markdown quality.

**Tech Stack:** Markdown, PowerShell 7, read-only Git inspection, and Cargo metadata.

**Global Constraints:**
- Modify only `README.md` and `ROADMAP.md` during implementation; this plan is the only file created by planning.
- Keep `Cargo.toml` and `Cargo.lock` at package version `0.1.0`; do not bump any version.
- Do not create a tag, release, changelog entry, release note, or publication claim.
- Do not assign durable import to v0.4, v0.5, or any other numbered release.
- Do not mark any historical checkbox in `docs/superpowers/plans/2026-07-29-ipfs3-import.md` complete.
- Do not change production code, configuration defaults, tests, workflows, Compose files, client evidence, or any v0.5+ checkbox state.
- Use Windows PowerShell syntax for every executable command.
- Do not run Docker, Compose, AWS CLI, MinIO `mc`, rclone, Kubo, the gateway, or any other live client/service.
- Do not run `git add`, `git commit`, `git push`, `git tag`, or any other Git write command.

**Authoritative spec:** `docs/superpowers/specs/2026-08-13-documentation-version-state-sync-design.md`

---

## File Responsibility Map

**Create:**

- `docs/superpowers/plans/2026-08-13-documentation-version-state-sync.md` — executable documentation synchronization plan, exact replacement copy, and verification gates.

**Modify:**

- `README.md` — durable import feature and API contract, configuration/security/recovery boundaries, composite route architecture, and v0.8 encrypted Range wording.
- `ROADMAP.md` — accepted AWS CLI result, unnumbered delivered/import section, and v0.8 encrypted Range wording.

**Verify unchanged:**

- `Cargo.toml` — package version must remain `0.1.0` byte-for-byte relative to `HEAD`.
- `Cargo.lock` — root package version and full file must remain unchanged relative to `HEAD`.
- `docs/superpowers/plans/2026-07-29-ipfs3-import.md` — historical execution checkboxes and full file must remain unchanged relative to `HEAD`.
- `docs/superpowers/specs/2026-08-13-documentation-version-state-sync-design.md` — approved specification is a review artifact, not an implementation edit target.

## Execution and Verification Waves

1. **Wave 1 — README contract:** Task 1 updates and independently validates `README.md`.
2. **Wave 2 — ROADMAP state:** Task 2 updates and independently validates `ROADMAP.md`.
3. **Wave 3 — Integrated acceptance:** Task 3 verifies cross-document consistency, local links, package/version state, byte-for-byte protected files, the exact changed-path set, and clean Markdown diff output.

---

### Task 1: Document the durable import contract and architecture

**Files:**
- Modify: `README.md:8-235`
- Verify: `config.example.toml:20-34`
- Verify: `src/s3/route/gateway.rs:13-57`
- Verify: `src/s3/route/import_object.rs:164-205`
- Verify: `src/state.rs:12-18`
- Verify: `src/main.rs:36-83`

**Interfaces:**
- Consumes: the approved README requirements, the implemented `POST /{bucket}/{key}?ipfs3-import` and `GET /{bucket}/{key}?ipfs3-import={job-id}` routes, the `Location`/`x-ipfs3-import-job-id` response contract, `[imports]` defaults, `GatewayRoute`, `ImportObjectRoute`, `DecompressZipRoute`, `ImportCoordinator`, and `AppState` boundaries.
- Produces: a self-contained README contract for durable import submission/status, source policy, recovery and publication guarantees, optional ZIP composition, configuration, architecture, and v0.8 encrypted Range placement.

- [ ] **Step 1: Run the focused README assertion and capture RED**

Run from the repository root:

```powershell
$readme = [IO.File]::ReadAllText((Join-Path (Get-Location) "README.md"))
$required = @(
    '## Durable `ipfs3-import`',
    'POST /{bucket}/{key}?ipfs3-import',
    'GET /{bucket}/{key}?ipfs3-import={job-id}',
    'x-ipfs3-client-token',
    'GatewayRoute',
    '`ImportCoordinator` is constructed separately from `AppState`',
    'chunk-level encrypted Range reads are planned for v0.8'
)
$missing = @($required | Where-Object { -not $readme.Contains($_) })
if ($missing.Count -ne 0) {
    throw "README contract is incomplete: $($missing -join '; ')"
}
```

Expected: the command fails with `README contract is incomplete` because the durable import section and current architecture/version wording are absent.

- [ ] **Step 2: Replace the complete Features section**

Replace the existing `## Features` section, from its heading through the final feature bullet, with exactly:

```markdown
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
```

- [ ] **Step 3: Insert the complete durable import section**

Insert the following section immediately after the `PutObject IPFS response headers` section and immediately before `## Configuration`:

````markdown
## Durable `ipfs3-import`

`ipfs3-import` is a SigV4-authenticated S3 extension. Submit a job with
`POST /{bucket}/{key}?ipfs3-import`, `Content-Type: application/xml`, and an XML
document containing exactly one source: `CID` or `URL`. All import submission
and status requests require the same valid SigV4 authentication as ordinary S3
requests; the abbreviated signing values below show the HTTP shape.

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
````

- [ ] **Step 4: Replace the Configuration lead-in with the exact import configuration boundary**

Keep the environment-variable table unchanged. Replace the paragraph beginning
`See [` and ending before the `IPFS_S3_MASTER_KEY` warning with exactly:

```markdown
See [`config.example.toml`](config.example.toml) for the full schema. Durable
import settings are under its `[imports]` table. CID imports are enabled by
default when `enabled = true`; URL imports remain unavailable until
`allowed_https_origins` contains each exact HTTPS origin that may be used.
```

- [ ] **Step 5: Replace the complete Architecture section and encrypted Range decision**

Replace the existing `## Architecture` diagram and `AppState` paragraph with
exactly:

````markdown
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
and `pinning` (`Arc<PinningCoordinator>`). `ImportCoordinator` is constructed
separately from `AppState`, passed to `GatewayRoute`, and starts its durable
worker beside the pinning worker.
````

In `## Key Design Decisions`, replace item 6 with exactly:

```markdown
6. **Encrypted Range = full decrypt + slice.** The gateway currently decrypts the entire encrypted object and then slices the response; chunk-level encrypted Range reads are planned for v0.8.
```

- [ ] **Step 6: Run the focused README assertion and capture GREEN**

Run:

```powershell
$readme = [IO.File]::ReadAllText((Join-Path (Get-Location) "README.md"))
$required = @(
    '## Durable `ipfs3-import`',
    'POST /{bucket}/{key}?ipfs3-import',
    'GET /{bucket}/{key}?ipfs3-import={job-id}',
    'document containing exactly one source: `CID` or `URL`',
    'HTTP/1.1 202 Accepted',
    'Location: /my-bucket/imported.bin?ipfs3-import=',
    'x-ipfs3-import-job-id:',
    'x-ipfs3-client-token',
    'decompress-zip=<prefix>',
    'job-level retry/reclaim, not byte-range URL resume',
    'exact origins listed in `allowed_https_origins`',
    'resolve exclusively to public addresses',
    'Redirects,',
    'and forwarded',
    'authentication are rejected',
    'does not support SSE-S3 or SSE-C',
    '[`config.example.toml`](config.example.toml)',
    '`[imports]` table',
    'CID imports are enabled by',
    'GatewayRoute (composite custom route)',
    'ImportObjectRoute',
    'DecompressZipRoute',
    '`ImportCoordinator` is constructed separately from `AppState`',
    'ownership-fenced atomic publication',
    'chunk-level encrypted Range reads are planned for v0.8'
)
$missing = @($required | Where-Object { -not $readme.Contains($_) })
if ($missing.Count -ne 0) {
    throw "README contract is incomplete: $($missing -join '; ')"
}
$putHeaders = $readme.IndexOf('### PutObject IPFS response headers', [StringComparison]::Ordinal)
$importSection = $readme.IndexOf('## Durable `ipfs3-import`', [StringComparison]::Ordinal)
$configuration = $readme.IndexOf('## Configuration', [StringComparison]::Ordinal)
if ($putHeaders -lt 0 -or $importSection -le $putHeaders -or $configuration -le $importSection) {
    throw 'Durable import section must be between PutObject response headers and Configuration'
}
if ($readme.Contains('v0.9 will optimize to chunk-level Range')) {
    throw 'README still contains the stale v0.9 encrypted Range assignment'
}
if ($readme -match '(?s)\*\*AppState\*\* holds[^.]*ImportCoordinator') {
    throw 'README incorrectly claims ImportCoordinator is held in AppState'
}
```

Expected: exit code 0 with no output. The exact submit/status shapes, response
headers, idempotency and ZIP controls, recovery boundary, URL/security policy,
encryption restriction, configuration link, route topology, separate
coordinator, and v0.8 Range wording are present in the required order.

- [ ] **Step 7: Record the Task 1 checkpoint without a Git write**

Run:

```powershell
git diff -- README.md
```

Expected: only the exact README replacements from Steps 2-5 appear. Do not
stage or commit the file.

---

### Task 2: Synchronize roadmap evidence and delivered version state

**Files:**
- Modify: `ROADMAP.md:16-82`
- Verify: `docs/client-smoke-evidence-2026-08-13.log:637-725`
- Verify: `docs/client-compatibility.md:38-46`
- Verify: `Cargo.toml:1-4`

**Interfaces:**
- Consumes: accepted AWS CLI `PASSED`/same-client `dual_head=PASSED` evidence dated 2026-08-13, durable import capabilities proven by the current repository, package version `0.1.0`, and the approved rule that release assignment remains pending.
- Produces: a ROADMAP with truthful AWS evidence, one unnumbered delivered/import section after v0.4, unchanged v0.5+ checkbox states, and one plain v0.8 encrypted Range item.

- [ ] **Step 1: Run the focused ROADMAP assertion and capture RED**

Run:

```powershell
$roadmap = [IO.File]::ReadAllText((Join-Path (Get-Location) "ROADMAP.md"))
$required = @(
    'AWS CLI smoke test PASSED:',
    'docs/client-smoke-evidence-2026-08-13.log',
    '## Delivered — Release Assignment Pending',
    'Package version remains `0.1.0`',
    '- [ ] Chunk-level encrypted Range'
)
$missing = @($required | Where-Object { -not $roadmap.Contains($_) })
if ($missing.Count -ne 0) {
    throw "ROADMAP state is stale: $($missing -join '; ')"
}
```

Expected: the command fails with `ROADMAP state is stale` because the AWS row,
delivered section, and Range wording have not yet been synchronized.

- [ ] **Step 2: Replace the stale AWS CLI row with accepted evidence wording**

Under `## v0.2 — Client Compatibility`, replace only the existing AWS CLI
checkbox line with exactly:

```markdown
- [x] AWS CLI smoke test PASSED: mb, cp, ls, get-bucket-location, ListObjects v1, same-client dual-endpoint HeadObject, DeleteObjects, rm, and rb (`dual_head=PASSED`; see `docs/client-smoke-evidence-2026-08-13.log`)
```

- [ ] **Step 3: Insert the complete unnumbered delivered section**

Insert this section immediately after the final v0.4 checkbox and immediately
before `## v0.5 — Multi-node`:

```markdown
## Delivered — Release Assignment Pending

Package version remains `0.1.0`. This unnumbered section records delivered
functionality without assigning durable import to v0.4, v0.5, or any other
numbered release.

- [x] Durable SigV4 `ipfs3-import` submission from a CID or allowlisted HTTPS URL
- [x] Persisted import status, progress, lease-based retries, and crash recovery
- [x] Idempotent replay through the optional `x-ipfs3-client-token` header
- [x] Optional ZIP extraction with ownership-fenced atomic publication
- [x] Stale-worker and overlapping content-mutation fencing
```

- [ ] **Step 4: Replace only the v0.8 encrypted Range wording**

Under `## v0.8 — Performance`, replace the current Range item with exactly:

```markdown
- [ ] Chunk-level encrypted Range
```

Do not alter the checkbox marker or any other v0.5+ item.

- [ ] **Step 5: Run the focused ROADMAP assertion and capture GREEN**

Run:

```powershell
$roadmapPath = Join-Path (Get-Location) "ROADMAP.md"
$roadmap = [IO.File]::ReadAllText($roadmapPath)
$exactAws = '- [x] AWS CLI smoke test PASSED: mb, cp, ls, get-bucket-location, ListObjects v1, same-client dual-endpoint HeadObject, DeleteObjects, rm, and rb (`dual_head=PASSED`; see `docs/client-smoke-evidence-2026-08-13.log`)'
$required = @(
    $exactAws,
    '## Delivered — Release Assignment Pending',
    'Package version remains `0.1.0`.',
    'without assigning durable import to v0.4, v0.5, or any other',
    '- [x] Durable SigV4 `ipfs3-import` submission from a CID or allowlisted HTTPS URL',
    '- [x] Persisted import status, progress, lease-based retries, and crash recovery',
    '- [x] Idempotent replay through the optional `x-ipfs3-client-token` header',
    '- [x] Optional ZIP extraction with ownership-fenced atomic publication',
    '- [x] Stale-worker and overlapping content-mutation fencing',
    '- [ ] Chunk-level encrypted Range'
)
$missing = @($required | Where-Object { -not $roadmap.Contains($_) })
if ($missing.Count -ne 0) {
    throw "ROADMAP state is incomplete: $($missing -join '; ')"
}
if ($roadmap.Contains('AWS CLI smoke artifact implemented but not executed') -or
    $roadmap.Contains('`SKIPPED`: local image absent') -or
    $roadmap.Contains('Chunk-level encrypted Range (v0.9 optimization)')) {
    throw 'ROADMAP retains stale AWS or encrypted Range wording'
}
$currentV04 = $roadmap.IndexOf('## Current: v0.4 — Pinning Service', [StringComparison]::Ordinal)
$delivered = $roadmap.IndexOf('## Delivered — Release Assignment Pending', [StringComparison]::Ordinal)
$v05 = $roadmap.IndexOf('## v0.5 — Multi-node', [StringComparison]::Ordinal)
if ($currentV04 -lt 0 -or $delivered -le $currentV04 -or $v05 -le $delivered) {
    throw 'Delivered section must appear immediately between current v0.4 and v0.5'
}
$betweenDeliveredAndV05 = $roadmap.Substring(
    $delivered + '## Delivered — Release Assignment Pending'.Length,
    $v05 - ($delivered + '## Delivered — Release Assignment Pending'.Length)
)
if ($betweenDeliveredAndV05 -match '(?m)^## ') {
    throw 'Another level-two section appears between Delivered and v0.5'
}
$v05Required = @(
    '- [ ] PostgreSQL production deployment',
    '- [ ] Multiple gateway instances (horizontal scaling)',
    '- [ ] IPFS Cluster for pinset replication',
    '- [ ] Private swarm (swarm.key) for node-to-node communication'
)
$missingV05 = @($v05Required | Where-Object { -not $roadmap.Contains($_) })
if ($missingV05.Count -ne 0) {
    throw "One or more v0.5 items changed: $($missingV05 -join '; ')"
}
$originalConsoleOutputEncoding = [Console]::OutputEncoding
try {
    [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
    $headRoadmap = (git show HEAD:ROADMAP.md) -join "`n"
    if ($LASTEXITCODE -ne 0) { throw 'Unable to read HEAD:ROADMAP.md' }
} finally {
    [Console]::OutputEncoding = $originalConsoleOutputEncoding
}
$headV05 = $headRoadmap.IndexOf('## v0.5 — Multi-node', [StringComparison]::Ordinal)
if ($headV05 -lt 0) { throw 'HEAD ROADMAP has no v0.5 section' }
$headTail = $headRoadmap.Substring($headV05)
$workTail = $roadmap.Substring($v05)
$headCheckboxes = @([regex]::Matches($headTail, '(?m)^- \[[ xX]\].*$') | ForEach-Object { $_.Value })
$workCheckboxes = @([regex]::Matches($workTail, '(?m)^- \[[ xX]\].*$') | ForEach-Object { $_.Value })
$expectedWorkCheckboxes = @($headCheckboxes | ForEach-Object {
    if ($_ -eq '- [ ] Chunk-level encrypted Range (v0.9 optimization)') {
        '- [ ] Chunk-level encrypted Range'
    } else {
        $_
    }
})
if (($expectedWorkCheckboxes -join "`n") -cne ($workCheckboxes -join "`n")) {
    throw 'A v0.5+ checkbox changed beyond the approved v0.8 Range text replacement'
}
```

Expected: exit code 0 with no output. The AWS line cites accepted 2026-08-13
evidence, the delivered section is unnumbered and correctly positioned, all
four v0.5 items and every later checkbox retain their prior state, and v0.8 no
longer refers to v0.9.

- [ ] **Step 6: Record the Task 2 checkpoint without a Git write**

Run:

```powershell
git diff -- ROADMAP.md
```

Expected: exactly one v0.2 line replacement, one delivered section insertion,
and one v0.8 line replacement. Do not stage or commit the file.

---

### Task 3: Run integrated documentation, version, and scope acceptance

**Files:**
- Verify: `README.md`
- Verify: `ROADMAP.md`
- Verify unchanged: `Cargo.toml`
- Verify unchanged: `Cargo.lock`
- Verify unchanged: `docs/superpowers/plans/2026-07-29-ipfs3-import.md`
- Verify: `docs/superpowers/specs/2026-08-13-documentation-version-state-sync-design.md`
- Verify: `docs/superpowers/plans/2026-08-13-documentation-version-state-sync.md`

**Interfaces:**
- Consumes: the independently green README and ROADMAP deliverables from Tasks 1-2 and the protected `HEAD` baselines.
- Produces: executable evidence that text/link contracts agree, package state remains `0.1.0`, protected files are byte-for-byte unchanged, only the four approved review/change paths differ from `HEAD`, and the final Markdown diff is whitespace-clean.

- [ ] **Step 1: Verify the protected files are byte-for-byte unchanged from `HEAD`**

Run:

```powershell
git diff --exit-code HEAD -- Cargo.toml Cargo.lock docs/superpowers/plans/2026-07-29-ipfs3-import.md
```

Expected: exit code 0 and no output. This is the byte-for-byte baseline gate for
package metadata, the lockfile, and the historical import implementation plan.

- [ ] **Step 2: Audit Cargo metadata and both root package version declarations**

Run:

```powershell
$metadataJson = cargo metadata --locked --no-deps --format-version 1
if ($LASTEXITCODE -ne 0) { throw 'cargo metadata --locked failed' }
$metadata = $metadataJson | ConvertFrom-Json
$packages = @($metadata.packages | Where-Object { $_.name -eq 'ipfs-s3-gateway' })
if ($packages.Count -ne 1) { throw "Expected one ipfs-s3-gateway package, found $($packages.Count)" }
if ($packages[0].version -ne '0.1.0') { throw "Cargo metadata version changed to $($packages[0].version)" }

$manifest = [IO.File]::ReadAllText((Join-Path (Get-Location) 'Cargo.toml'))
if ($manifest -notmatch '(?m)^name = "ipfs-s3-gateway"\r?$' -or
    $manifest -notmatch '(?m)^version = "0\.1\.0"\r?$') {
    throw 'Cargo.toml root package identity/version is not ipfs-s3-gateway 0.1.0'
}

$lock = [IO.File]::ReadAllText((Join-Path (Get-Location) 'Cargo.lock'))
$rootLock = [regex]::Matches($lock, '(?m)^\[\[package\]\]\r?\nname = "ipfs-s3-gateway"\r?\nversion = "(?<version>[^"]+)"\r?$')
if ($rootLock.Count -ne 1 -or $rootLock[0].Groups['version'].Value -ne '0.1.0') {
    throw 'Cargo.lock root package version is not exactly 0.1.0'
}
```

Expected: exit code 0 with no output; Cargo resolves from the existing lockfile
and reports exactly one `ipfs-s3-gateway` package at `0.1.0`, matching both
manifest and lockfile declarations.

- [ ] **Step 3: Verify cross-document text, local links, evidence, and milestone consistency**

Run:

```powershell
$readmePath = Join-Path (Get-Location) 'README.md'
$roadmapPath = Join-Path (Get-Location) 'ROADMAP.md'
$readme = [IO.File]::ReadAllText($readmePath)
$roadmap = [IO.File]::ReadAllText($roadmapPath)

$readmeRequired = @(
    'POST /{bucket}/{key}?ipfs3-import',
    'GET /{bucket}/{key}?ipfs3-import={job-id}',
    'HTTP/1.1 202 Accepted',
    'Location:',
    'x-ipfs3-import-job-id:',
    'x-ipfs3-client-token',
    'decompress-zip=<prefix>',
    'job-level retry/reclaim, not byte-range URL resume',
    'exact origins listed in `allowed_https_origins`',
    'resolve exclusively to public addresses',
    'does not support SSE-S3 or SSE-C',
    '`[imports]` table',
    'GatewayRoute (composite custom route)',
    'ImportObjectRoute',
    'DecompressZipRoute',
    '`ImportCoordinator` is constructed separately from `AppState`',
    'ownership-fenced atomic publication',
    'chunk-level encrypted Range reads are planned for v0.8'
)
$readmeMissing = @($readmeRequired | Where-Object { -not $readme.Contains($_) })
if ($readmeMissing.Count -ne 0) {
    throw "README acceptance text missing: $($readmeMissing -join '; ')"
}

$roadmapRequired = @(
    'AWS CLI smoke test PASSED:',
    'docs/client-smoke-evidence-2026-08-13.log',
    '## Delivered — Release Assignment Pending',
    'Package version remains `0.1.0`.',
    'without assigning durable import to v0.4, v0.5, or any other',
    '- [x] Durable SigV4 `ipfs3-import` submission from a CID or allowlisted HTTPS URL',
    '- [x] Persisted import status, progress, lease-based retries, and crash recovery',
    '- [x] Idempotent replay through the optional `x-ipfs3-client-token` header',
    '- [x] Optional ZIP extraction with ownership-fenced atomic publication',
    '- [x] Stale-worker and overlapping content-mutation fencing',
    '- [ ] Chunk-level encrypted Range'
)
$roadmapMissing = @($roadmapRequired | Where-Object { -not $roadmap.Contains($_) })
if ($roadmapMissing.Count -ne 0) {
    throw "ROADMAP acceptance text missing: $($roadmapMissing -join '; ')"
}
if ($roadmap -match 'AWS CLI smoke artifact implemented but not executed|`SKIPPED`: local image absent|Chunk-level encrypted Range \(v0\.9 optimization\)') {
    throw 'ROADMAP retains stale AWS SKIPPED or v0.9 Range text'
}
if ($readme -notmatch 'chunk-level encrypted Range reads are planned for v0\.8' -or
    $roadmap -notmatch '(?m)^- \[ \] Chunk-level encrypted Range\r?$') {
    throw 'README and ROADMAP do not both assign chunk-level encrypted Range to v0.8'
}

$configLink = '[`config.example.toml`](config.example.toml)'
if (-not $readme.Contains($configLink)) { throw "README is missing $configLink" }
$configTarget = Join-Path (Get-Location) 'config.example.toml'
if (-not (Test-Path -LiteralPath $configTarget -PathType Leaf)) {
    throw 'README config.example.toml link target does not exist'
}
$config = [IO.File]::ReadAllText($configTarget)
if ($config -notmatch '(?m)^\[imports\]\r?$' -or
    $config -notmatch '(?m)^enabled = true\r?$' -or
    $config -notmatch '(?m)^allowed_https_origins = \[\]\r?$') {
    throw 'README import defaults do not match config.example.toml'
}

$evidencePath = Join-Path (Get-Location) 'docs/client-smoke-evidence-2026-08-13.log'
if (-not (Test-Path -LiteralPath $evidencePath -PathType Leaf)) {
    throw 'ROADMAP AWS evidence link target does not exist'
}
$evidence = [IO.File]::ReadAllText($evidencePath)
if (-not $evidence.Contains('[RESULT] client=Aws status=PASSED dual_head=PASSED') -or
    -not $evidence.Contains('[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED')) {
    throw 'ROADMAP AWS claim is not backed by accepted result and same-client evidence'
}
```

Expected: exit code 0 with no output. Every required public contract is present,
the `[imports]` and evidence links resolve to repository files whose content
supports the statements, no stale AWS/Range wording remains, and both public
documents agree on v0.8.

- [ ] **Step 4: Enforce the exact changed-path allowlist**

Run:

```powershell
$expectedPaths = @(
    'README.md',
    'ROADMAP.md',
    'docs/superpowers/plans/2026-08-13-documentation-version-state-sync.md',
    'docs/superpowers/specs/2026-08-13-documentation-version-state-sync-design.md'
) | Sort-Object
$statusLines = @(git -c core.quotepath=false status --short)
if ($LASTEXITCODE -ne 0) { throw 'git status --short failed' }
$actualPaths = @($statusLines | ForEach-Object {
    if ($_ -match ' -> ') { throw "Renamed paths are outside this task: $_" }
    if ($_.Length -lt 4) { throw "Unexpected porcelain status line: $_" }
    $_.Substring(3).Replace('\', '/')
}) | Sort-Object
$pathDelta = @(Compare-Object -ReferenceObject $expectedPaths -DifferenceObject $actualPaths)
if ($pathDelta.Count -ne 0) {
    throw "Changed paths differ from the exact allowlist:`n$($pathDelta | Out-String)"
}
```

Expected: exit code 0 with no output. The only paths differing from `HEAD` are
the two intended public documents, the approved design spec already created by
the parent workflow, and this implementation plan. No source, config, test,
workflow, client evidence, Cargo, or historical-plan file appears.

- [ ] **Step 5: Run whitespace validation and inspect the complete documentation diff**

Run:

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw 'git diff --check reported whitespace errors' }
git diff -- README.md ROADMAP.md
if ($LASTEXITCODE -ne 0) { throw 'Unable to render final README/ROADMAP diff' }
```

Expected: `git diff --check` emits no output. The rendered diff contains only
Task 1 and Task 2 copy: durable import README documentation/architecture,
accepted AWS evidence, the unnumbered delivered section, and v0.8 Range
alignment.

- [ ] **Step 6: Stop at the review boundary**

Report the passing commands and the four-path allowlist. Do not run a live
client, stage files, commit, push, tag, publish, or modify any additional file.

---

## Requirement-to-Task Traceability

| Requirement | Plan coverage |
|---|---|
| Exact submit/status query shapes and accepted response headers | Task 1 Steps 3 and 6; Task 3 Step 3 |
| CID or URL source, idempotency, optional ZIP | Task 1 Step 3; Task 3 Step 3 |
| Job retry/reclaim but no byte-range URL resume | Task 1 Step 3; Task 3 Step 3 |
| Exact HTTPS-origin/public-address/no-redirect/no-forwarded-auth policy | Task 1 Step 3; Task 3 Step 3 |
| SSE-S3 and SSE-C import rejection | Task 1 Step 3; Task 3 Step 3 |
| `[imports]`, CID default, URL allowlist requirement | Task 1 Step 4; Task 3 Step 3 |
| `GatewayRoute`/`ImportObjectRoute`/`DecompressZipRoute` and separate coordinator/worker | Task 1 Step 5; Task 3 Step 3 |
| Accepted 2026-08-13 AWS CLI result | Task 2 Step 2; Task 3 Step 3 |
| Delivered import with no numbered release assignment | Task 2 Step 3; Task 3 Step 3 |
| Preserve all v0.5+ checkbox states | Task 2 Steps 4-5 |
| README/ROADMAP encrypted Range agreement at v0.8 | Task 1 Steps 2 and 5; Task 2 Step 4; Task 3 Step 3 |
| Package remains `0.1.0` | Task 3 Steps 1-2 |
| Historical import plan unchanged byte-for-byte | Task 3 Step 1 |
| Exact changed-path allowlist and whitespace-clean diff | Task 3 Steps 4-5 |
| No Git writes or live clients | Global Constraints; Task 3 Step 6 |

## Final Review Boundary

Review only these four paths together:

```text
README.md
ROADMAP.md
docs/superpowers/plans/2026-08-13-documentation-version-state-sync.md
docs/superpowers/specs/2026-08-13-documentation-version-state-sync-design.md
```

The implementation has two independently acceptable documentation units
(README contract and ROADMAP state) followed by one integrated acceptance
wave. Any future commit decision belongs to the parent/user review workflow;
this plan authorizes no Git write.
