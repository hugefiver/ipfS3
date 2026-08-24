# Multi-Gateway Horizontal Scaling Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the bounded v0.5-B topology of exactly two HTTP gateway replicas behind one Nginx entry point, sharing one PostgreSQL 17 instance and one Kubo instance, with serialized PostgreSQL startup migrations and evidence-backed cross-replica behavior without claiming full-stack HA.

**Architecture:** Add an independent five-service Compose topology and an Nginx streaming/SigV4 boundary; both normal gateway processes keep their existing HTTP, import-worker, and pinning-worker lifecycles and share the same PostgreSQL, Kubo, credentials, and master key. `store::run_migrations` branches only for PostgreSQL, where one outer transaction sets a 60-second local lock timeout, takes the stable transaction advisory lock, runs SeaORM 1.1.20 migrations through the transaction/savepoint path, and commits fail-closed; SQLite and every other backend retain the direct `Migrator::up` path. A runtime-discovered PostgreSQL import-renewal defect is corrected by replacing a literal custom-expression question mark with a structurally bound value expression. Static PowerShell contracts, a real two-replica Rust target, an independent blocking release job, and one owned local workflow-parity run gate documentation and the single ROADMAP checkbox.

**Tech Stack:** Rust 2024 (MSRV 1.92), Tokio 1, SeaORM/sea-orm-migration 1.1.20, rust-s3 0.37, reqwest 0.13, SigV4 test support, PostgreSQL 17, Kubo, Docker Compose v2.23.1+, Nginx `1.28.0-alpine`, PowerShell 7, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md`

**Global Constraints:**
- This release supports exactly **two HTTP gateway replicas, through one Nginx entry point, sharing one PostgreSQL 17 instance and one Kubo instance**.
- It does not claim high availability for PostgreSQL, Kubo, the load balancer, the host, or a general multi-host deployment.
- `docker-compose.multi-gateway.yml` is independent from `docker-compose.yml` and `docker-compose.postgres.yml`; neither existing Compose file may be modified.
- The production `services` mapping contains exactly `postgres`, `kubo`, `gateway-a`, `gateway-b`, and `load-balancer`.
- The production topology contains no `container_name`, Cloudflared service, gateway-local volume, remote-provider configuration, provider token, or direct PostgreSQL/Kubo/gateway host publication.
- Only `load-balancer` publishes production port 9000, through required no-default `IPFS_S3_LOAD_BALANCER_BIND` and `IPFS_S3_LOAD_BALANCER_PORT`; the bind is explicit and non-wildcard.
- `POSTGRES_PASSWORD`, `IPFS_S3_ACCESS_KEY_ID`, `IPFS_S3_SECRET_ACCESS_KEY`, and `IPFS_S3_MASTER_KEY` use required Compose interpolation with no fallback.
- Both gateway services receive byte-identical database URL, Kubo URL, access key, secret key, master key, bind, logging, dependency, healthcheck, and worker-effective configuration.
- Both gateways continue to start the existing pinning worker and durable import worker; do not add worker roles or change import `claim_epoch`/database-clock fencing or pinning `locked_until` semantics.
- The topology configures no remote providers and documentation states that it provides no cluster-level provider RPS, concurrency, health, or fairness guarantee.
- Do not implement IPFS Cluster, a private swarm, master-key rotation, PostgreSQL HA/backup/pool tuning, a distributed provider limiter, worker-role separation, TLS, Kubernetes, cloud resources, or load-balancer HA.
- Nginx uses HTTP/1.1, preserves the signed Host as `$http_host`, disables request and response buffering, sets unlimited body size, retries only the bounded configured upstream set for `error`, `timeout`, `http_502`, `http_503`, and `http_504`, and never logs Authorization or request bodies.
- Load-balancer `/health` and `/ready` both proxy a selected gateway `/ready`; this remains database-only readiness and is not active upstream health checking.
- PostgreSQL migration coordination uses the exact stable key `(1229997651, 1395879239)`, exact `SET LOCAL lock_timeout = '60s'`, one outer transaction, sanitized waiting/acquired/failure categories, nested SeaORM migration/savepoint behavior, and fail-closed commit.
- SQLite and every non-PostgreSQL backend retain the current direct `Migrator::up(db, None)` path.
- PostgreSQL import lease renewal must generate a numbered bind through a structural SeaQuery value comparison with `clock_timestamp()`; it must not emit a literal `?`, while SQLite and MySQL lease-clock expressions remain unchanged.
- Static tests never start Docker; live tests use a unique disposable Compose project and fixed loopback-only observation ports `59000`, `59001`, `59002`, `55002`, and `55434`.
- Validation sets exact `COMPOSE_DISABLE_ENV_FILE="1"`, never uses `--env-file`, never reads or outputs ignored `.env` or secrets, and restores exact environment-variable presence and case-sensitive value (`Remove-Item Env:` for prior absence).
- Every Docker/Git native query checks `$LASTEXITCODE` immediately; live preflight checks all-state project-labelled containers, networks, and volumes plus every fixed port before ownership.
- Cleanup runs only for an owned, attempted disposable project; non-coloured logs are emitted before cleanup; disposable cleanup may use volumes; zero labelled residual containers, networks, and volumes is mandatory. Diagnostics, cleanup, and each residual-query failure are collected without throwing until an outer `finally` has attempted and verified exact restoration of all 15 process-environment entries.
- Production documentation never recommends `down --volumes`; only disposable validation cleanup may remove volumes.
- README and ROADMAP may change only after every required LOCAL static/live/regression result passes; the first hosted result is exactly `HOSTED multi-gateway-deployment: NOT RUN`.
- ROADMAP checks only `Multiple gateway instances (horizontal scaling)`; `IPFS Cluster` and `Private swarm` remain unchecked.
- Existing `postgres-import`, `postgres-production-deployment`, SQLite `e2e`, and `client-smoke-infrastructure` job meanings remain unchanged; the new `multi-gateway-deployment` job is independent and blocking.
- Do not install software or add Cargo, PowerShell-module, YAML-parser, action-linter, Nginx-module, or system-package dependencies.
- Use PowerShell syntax for every command and GitHub Actions multiline script; do not use `&&`, Bash environment assignment, heredocs, `/dev/null`, or Bash-only redirection.
- The runtime-revised approved spec SHA-256 is `afce0b5ccdadd1839d8f50468f6c82c77a691c3553890b5bf58247512b95b7d4`; implementation agents must not modify the spec.
- Implementation agents must not modify this plan except orchestrator-owned checkbox progress that does not change task semantics.
- Implementation subagents must not stage, commit, push, tag, amend, or perform another Git write. After the exact identity receives both Oracle and Reviewer approval, the orchestrator may use the user's existing authorization for one focused commit only; no push or tag is authorized.

---

## File Map

### Create

- `docker-compose.multi-gateway.yml` — independent exact five-service production topology, shared configuration, named PostgreSQL/Kubo volumes, and sole Nginx publication.
- `deploy/nginx/multi-gateway.conf` — two-member upstream, SigV4-safe Host forwarding, streaming, bounded passive failover, readiness aliases, and non-sensitive access logging.
- `tests/compose.multi-gateway-validation.yml` — disposable loopback-only direct/observation mappings on the five fixed ports.
- `tests/multi-gateway.Tests.ps1` — structural static contract over Compose, canonical plus nested-scope Nginx blocks, independently extracted migration dispatcher/helper functions, Rust live scenarios, workflow order/safety, and protected boundaries.
- `tests/multi_gateway.rs` — real direct A/B cross-replica CRUD, multipart, same-key convergence, CID import observation, and post-failover load-balancer CRUD.
- `docs/superpowers/plans/2026-08-24-multi-gateway-horizontal-scaling.md` — this reviewed implementation plan and final-manifest artifact.

### Modify

- `src/store/mod.rs` — PostgreSQL-only transaction advisory migration lock and focused existing SQLite/non-PostgreSQL regressions.
- `src/store/import/lease_clock.rs` — PostgreSQL-safe numbered bind generation for proposed import lease renewal, with builder RED→GREEN coverage and unchanged SQLite/MySQL branches.
- `tests/postgres_import.rs` — serialized live PostgreSQL lock wait/acquisition, exact-marker, 60-second timeout, rollback/fail-closed, and redaction tests using fresh unique schemas and connections.
- `.github/workflows/release-validation.yml` — independent blocking `multi-gateway-deployment` job plus one blocking static-contract command in the existing infrastructure job.
- `tests/release-validation.Tests.ps1` — change the root contract from four to five independent blocking jobs and lock the new job without weakening existing assertions.
- `tests/postgres-production-baseline.Tests.ps1` — only the unavoidable workflow-count/client-static-command expectations (four→five jobs, three→four static commands); all single-PostgreSQL Compose and job assertions remain unchanged.
- `README.md` — evidence-gated bounded deployment/operator contract and non-destructive shutdown.
- `ROADMAP.md` — only line 68, `Multiple gateway instances (horizontal scaling)`, changes from unchecked to checked.

### Protected/Verify Unchanged

- `docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md`
- `docker-compose.yml`, `docker-compose.postgres.yml`, `tests/compose.postgres-production-validation.yml`
- `Cargo.toml`, `Cargo.lock`, `config.example.toml`, `config.docker.toml`, `Dockerfile`
- `src/state.rs`, `src/main.rs`, provider implementations, import/pinning ownership modules other than the focused lease-clock helper, every migration file, and existing `tests/e2e.rs` semantics
- `.github/workflows/ci.yml`, Kubernetes/cloud files, unrelated plans/specs, and ignored `.env`

---

### Task 1: Add the independent topology, Nginx boundary, override, and structural static contract

**Files:**
- Create: `docker-compose.multi-gateway.yml`
- Create: `deploy/nginx/multi-gateway.conf`
- Create: `tests/compose.multi-gateway-validation.yml`
- Create/Test: `tests/multi-gateway.Tests.ps1`
- Verify unchanged: `docker-compose.yml`, `docker-compose.postgres.yml`, `tests/compose.postgres-production-validation.yml`

**Interfaces:**
- Consumes: existing gateway image/healthcheck contract, PostgreSQL inline `postgres_init`, Kubo healthcheck/volume layout, required secret names, and Nginx official proxy directives.
- Produces: Compose services `postgres`, `kubo`, `gateway-a`, `gateway-b`, `load-balancer`; upstream `ipfs3_gateways`; production endpoint `${IPFS_S3_LOAD_BALANCER_BIND}:${IPFS_S3_LOAD_BALANCER_PORT}:9000`; validation endpoints A `59001`, B `59002`, LB `59000`, Kubo `55002`, PostgreSQL `55434`; command `pwsh -NoProfile -File tests/multi-gateway.Tests.ps1`.

- [ ] **Step 1: Write the missing-file and protected-baseline RED contract**

Create `tests/multi-gateway.Tests.ps1` with strict mode, normalized reads, the standard assertion helpers, the indentation-aware `Get-YamlBlock`, and one comment/string-aware nested-brace extractor shared by the Nginx and Rust source contracts. Add these first assertions before creating the deployment files:

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposePath = Join-Path $RepoRoot "docker-compose.multi-gateway.yml"
$NginxPath = Join-Path $RepoRoot "deploy/nginx/multi-gateway.conf"
$OverridePath = Join-Path $RepoRoot "tests/compose.multi-gateway-validation.yml"
$StorePath = Join-Path $RepoRoot "src/store/mod.rs"
$StatePath = Join-Path $RepoRoot "src/state.rs"
$MainPath = Join-Path $RepoRoot "src/main.rs"
$RustLivePath = Join-Path $RepoRoot "tests/multi_gateway.rs"
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
$SpecPath = Join-Path $RepoRoot "docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md"

function Read-NormalizedText {
    param([Parameter(Mandatory)][string]$Path)
    if (-not [IO.File]::Exists($Path)) { throw "Required file is missing: $Path" }
    [IO.File]::ReadAllText($Path).Replace("`r`n", "`n").Replace("`r", "`n")
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw $Message }
}

function Assert-Contains {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string]$Fragment, [Parameter(Mandatory)][string]$Message)
    Assert-True $Text.Contains($Fragment, [StringComparison]::Ordinal) $Message
}

function Assert-NotContains {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string]$Fragment, [Parameter(Mandatory)][string]$Message)
    Assert-True (-not $Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-Matches {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string]$Pattern, [Parameter(Mandatory)][string]$Message)
    Assert-True ([regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-NotMatches {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string]$Pattern, [Parameter(Mandatory)][string]$Message)
    Assert-True (-not [regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-InOrder {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string[]]$Fragments, [Parameter(Mandatory)][string]$Message)
    $cursor = 0
    foreach ($fragment in $Fragments) {
        $index = $Text.IndexOf($fragment, $cursor, [StringComparison]::Ordinal)
        if ($index -lt 0) { throw "$Message Missing or out of order: $fragment" }
        $cursor = $index + $fragment.Length
    }
}

function Get-YamlBlock {
    param([Parameter(Mandatory)][string]$Text, [Parameter(Mandatory)][string]$Key, [Parameter(Mandatory)][int]$Indent)
    $lines = @($Text -split "`n")
    $header = (" " * $Indent) + $Key + ":"
    $indexes = @(for ($index = 0; $index -lt $lines.Count; $index++) { if ($lines[$index].TrimEnd() -ceq $header) { $index } })
    if ($indexes.Count -ne 1) { throw "Expected one YAML key '$header', found $($indexes.Count)" }
    $start = $indexes[0]
    $end = $lines.Count
    for ($index = $start + 1; $index -lt $lines.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace($lines[$index])) { continue }
        if ([regex]::Match($lines[$index], '^( *)').Groups[1].Length -le $Indent) { $end = $index; break }
    }
    if ($end -le $start + 1) { return "" }
    $lines[($start + 1)..($end - 1)] -join "`n"
}

function Get-BracedBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$HeaderPattern,
        [Parameter(Mandatory)][string]$Label
    )
    $matches = [regex]::Matches($Text, $HeaderPattern, [Text.RegularExpressions.RegexOptions]::Multiline)
    if ($matches.Count -ne 1) { throw "Expected exactly one $Label header, found $($matches.Count)" }
    $start = $matches[0].Index
    $open = $Text.IndexOf('{', $start)
    if ($open -lt 0 -or $open -ge ($start + $matches[0].Length)) { throw "$Label header has no opening brace" }

    $depth = 0
    $quote = [char]0
    $escaped = $false
    $lineComment = $false
    $blockComment = $false
    for ($index = $open; $index -lt $Text.Length; $index++) {
        $character = $Text[$index]
        $next = if ($index + 1 -lt $Text.Length) { $Text[$index + 1] } else { [char]0 }
        if ($lineComment) {
            if ($character -eq "`n") { $lineComment = $false }
            continue
        }
        if ($blockComment) {
            if ($character -eq '*' -and $next -eq '/') { $blockComment = $false; $index++ }
            continue
        }
        if ($quote -ne [char]0) {
            if ($escaped) { $escaped = $false; continue }
            if ($character -eq '\') { $escaped = $true; continue }
            if ($character -eq $quote) { $quote = [char]0 }
            continue
        }
        if ($character -eq '#') { $lineComment = $true; continue }
        if ($character -eq '/' -and $next -eq '/') { $lineComment = $true; $index++; continue }
        if ($character -eq '/' -and $next -eq '*') { $blockComment = $true; $index++; continue }
        if ($character -eq '"' -or $character -eq "'") { $quote = $character; continue }
        if ($character -eq '{') { $depth++; continue }
        if ($character -eq '}') {
            $depth--
            if ($depth -eq 0) {
                return [pscustomobject]@{
                    Text = $Text.Substring($start, $index - $start + 1)
                    Body = $Text.Substring($open + 1, $index - $open - 1)
                }
            }
            if ($depth -lt 0) { throw "$Label has an unmatched closing brace" }
        }
    }
    throw "$Label has no matching closing brace"
}

function Assert-DirectiveCount {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Directive,
        [Parameter(Mandatory)][int]$Count,
        [Parameter(Mandatory)][string]$Label
    )
    $pattern = '(?m)^\s*' + [regex]::Escape($Directive) + '\s*$'
    $actual = [regex]::Matches($Text, $pattern).Count
    Assert-True ($actual -eq $Count) "$Label expected $Count exact '$Directive' directives, found $actual"
}

$defaultSha = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $RepoRoot "docker-compose.yml")).Hash.ToLowerInvariant()
$singlePgSha = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $RepoRoot "docker-compose.postgres.yml")).Hash.ToLowerInvariant()
Assert-True ($defaultSha -ceq "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c") "Default SQLite Compose changed"
Assert-True ($singlePgSha -ceq "6a39a43a48beda2cbd79a0ae8ca403a3ead2a67766ed34c214de78fa1a6a1782") "Single-PostgreSQL Compose changed"

$Compose = Read-NormalizedText $ComposePath
$Nginx = Read-NormalizedText $NginxPath
$Override = Read-NormalizedText $OverridePath
```

Run:

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Expected the absent multi-gateway deployment contract to be RED" }
```

Expected: non-zero with `Required file is missing:` followed by the repository-local `docker-compose.multi-gateway.yml` path; neither protected Compose file changes.

- [ ] **Step 2: Create the exact five-service production Compose file**

Create `docker-compose.multi-gateway.yml` with this complete service/volume/config shape. Keep the two gateway blocks textually identical except service name:

```yaml
name: ipfs3-multi-gateway

services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    configs:
      - source: postgres_init
        target: /docker-entrypoint-initdb.d/10-ipfs3.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ipfs3 -d ipfs3"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 10s
    restart: unless-stopped

  kubo:
    build: ./ipfs
    image: ghcr.io/hugefiver/ipfs3-kubo:latest
    volumes:
      - ipfs_data:/data/ipfs
    environment:
      IPFS_PATH: /data/ipfs
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 15s
    restart: unless-stopped

  gateway-a:
    build: .
    image: ghcr.io/hugefiver/ipfs3:latest
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_KUBO_RPC_URL: http://kubo:5001
      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"
      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"
      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"
      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"
      RUST_LOG: info
    depends_on:
      postgres:
        condition: service_healthy
      kubo:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s
    restart: unless-stopped

  gateway-b:
    build: .
    image: ghcr.io/hugefiver/ipfs3:latest
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_KUBO_RPC_URL: http://kubo:5001
      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"
      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"
      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"
      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"
      RUST_LOG: info
    depends_on:
      postgres:
        condition: service_healthy
      kubo:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s
    restart: unless-stopped

  load-balancer:
    image: nginx:1.28.0-alpine
    ports:
      - "${IPFS_S3_LOAD_BALANCER_BIND:?IPFS_S3_LOAD_BALANCER_BIND is required}:${IPFS_S3_LOAD_BALANCER_PORT:?IPFS_S3_LOAD_BALANCER_PORT is required}:9000"
    volumes:
      - ./deploy/nginx/multi-gateway.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      gateway-a:
        condition: service_healthy
      gateway-b:
        condition: service_healthy
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O - http://127.0.0.1:9000/ready | grep -qx READY"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 5s
    restart: unless-stopped

volumes:
  postgres_data:
  ipfs_data:

configs:
  postgres_init:
    content: |
      \set app_password '${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'
      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec
      CREATE DATABASE ipfs3 OWNER ipfs3;
```

The gateway blocks are deliberately duplicated so the dependency-free static contract can compare their complete text byte-for-byte and reject replica-specific drift, including either gateway gaining `ports` or `volumes`.

- [ ] **Step 3: Create the SigV4-safe Nginx configuration**

Create `deploy/nginx/multi-gateway.conf`:

```nginx
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    log_format gateway '$request_method $uri status=$status request_time=$request_time upstream_status=$upstream_status';
    access_log /var/log/nginx/access.log gateway;
    error_log /var/log/nginx/error.log warn;

    upstream ipfs3_gateways {
        server gateway-a:9000 max_fails=1 fail_timeout=10s;
        server gateway-b:9000 max_fails=1 fail_timeout=10s;
        keepalive 32;
    }

    server {
        listen 9000;
        server_name _;
        client_max_body_size 0;

        location = /health {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location = /ready {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location / {
            proxy_pass http://ipfs3_gateways;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_request_buffering off;
            proxy_buffering off;
            proxy_connect_timeout 2s;
            proxy_send_timeout 120s;
            proxy_read_timeout 120s;
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }
    }
}
```

`non_idempotent` permits bounded retry of PUT/POST after the configured failure classes; it does not claim exactly-once behavior. `$uri` excludes Authorization and body data; do not change it to a header dump or body variable.

- [ ] **Step 4: Create the loopback-only validation override**

Create `tests/compose.multi-gateway-validation.yml`:

```yaml
services:
  postgres:
    ports:
      - "127.0.0.1:55434:5432"

  kubo:
    ports:
      - "127.0.0.1:55002:5001"

  gateway-a:
    ports:
      - "127.0.0.1:59001:9000"

  gateway-b:
    ports:
      - "127.0.0.1:59002:9000"

  load-balancer:
    ports:
      - "127.0.0.1:59000:9000"
```

The hosted/local environment must set the production LB mapping to the same `127.0.0.1:59000:9000` uniqueness key so Compose does not append a second LB publication.

- [ ] **Step 5: Complete structural assertions for topology, ports, equality, canonical Nginx scopes, and protected files**

Append assertions that derive exact service/volume sets and compare gateway contracts. For Nginx, require byte-for-byte canonical content first, then independently extract and validate nested `http`, `upstream ipfs3_gateways`, `server`, root S3 location, exact `/health`, and exact `/ready` blocks; this makes comments, extra blocks, conflicting directives, and out-of-scope matches fail rather than satisfy the contract:

```powershell
$services = Get-YamlBlock $Compose "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):(?:\s+&[A-Za-z0-9_-]+)?\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedServices = @("postgres", "kubo", "gateway-a", "gateway-b", "load-balancer")
Assert-True ($serviceNames.Count -eq 5) "Production topology must define exactly five services"
Assert-True ((($serviceNames | Sort-Object) -join "`n") -ceq (($expectedServices | Sort-Object) -join "`n")) "Production service set changed"

$gatewayA = Get-YamlBlock $services "gateway-a" 2
$gatewayB = Get-YamlBlock $services "gateway-b" 2
$loadBalancer = Get-YamlBlock $services "load-balancer" 2
foreach ($gateway in @($gatewayA, $gatewayB)) {
    Assert-NotMatches $gateway '(?m)^    ports:\s*$' "Production gateways must not publish ports"
    Assert-NotMatches $gateway '(?m)^    volumes:\s*$' "Production gateways must not mount persistent volumes"
}
Assert-True ($gatewayA -ceq $gatewayB) "gateway-a and gateway-b configuration blocks must be byte-identical"
foreach ($backendName in @("postgres", "kubo")) {
    Assert-NotMatches (Get-YamlBlock $services $backendName 2) '(?m)^    ports:\s*$' "Production backend must not publish ports: $backendName"
}
foreach ($required in @("POSTGRES_PASSWORD", "IPFS_S3_ACCESS_KEY_ID", "IPFS_S3_SECRET_ACCESS_KEY", "IPFS_S3_MASTER_KEY", "IPFS_S3_LOAD_BALANCER_BIND", "IPFS_S3_LOAD_BALANCER_PORT")) {
    Assert-Contains $Compose ('${' + $required + ':?') "Required no-default interpolation is missing: $required"
    Assert-NotContains $Compose ('${' + $required + ':-') "Required input has a default: $required"
}
foreach ($forbidden in @("container_name:", "cloudflared", "gateway_data", "PINATA_JWT", "FILEBASE_PINNING_TOKEN", "pinata", "filebase", "remote")) {
    Assert-NotContains $Compose $forbidden "Forbidden multi-gateway production fragment: $forbidden"
}
Assert-Contains $loadBalancer "    image: nginx:1.28.0-alpine" "Nginx exact image tag changed"
Assert-Contains (Read-NormalizedText $SpecPath) "sha256:30f1c0d78e0ad60901648be663a710bdadf19e4c10ac6782c235200619158284" "Nginx selection digest evidence is missing from the approved spec"

$validationServices = Get-YamlBlock $Override "services" 0
$validationNames = @([regex]::Matches($validationServices, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ((($validationNames | Sort-Object) -join "`n") -ceq (($expectedServices | Sort-Object) -join "`n")) "Validation override must mention exactly the five production services"
foreach ($mapping in @(
    '127.0.0.1:55434:5432',
    '127.0.0.1:55002:5001',
    '127.0.0.1:59001:9000',
    '127.0.0.1:59002:9000',
    '127.0.0.1:59000:9000'
)) {
    Assert-True (([regex]::Matches($Override, [regex]::Escape($mapping))).Count -eq 1) "Validation mapping must occur exactly once: $mapping"
}
Assert-NotContains $Override "0.0.0.0" "Validation ports must be loopback-only"
Assert-NotMatches $Override '(?m)^volumes:' "Validation override must not declare volumes"
Assert-NotContains $Compose "tests/compose.multi-gateway-validation.yml" "Production Compose must not reference its disposable validation override"

$ExpectedNginx = @'
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    log_format gateway '$request_method $uri status=$status request_time=$request_time upstream_status=$upstream_status';
    access_log /var/log/nginx/access.log gateway;
    error_log /var/log/nginx/error.log warn;

    upstream ipfs3_gateways {
        server gateway-a:9000 max_fails=1 fail_timeout=10s;
        server gateway-b:9000 max_fails=1 fail_timeout=10s;
        keepalive 32;
    }

    server {
        listen 9000;
        server_name _;
        client_max_body_size 0;

        location = /health {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location = /ready {
            proxy_pass http://ipfs3_gateways/ready;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }

        location / {
            proxy_pass http://ipfs3_gateways;
            proxy_http_version 1.1;
            proxy_set_header Host $http_host;
            proxy_set_header Connection "";
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_request_buffering off;
            proxy_buffering off;
            proxy_connect_timeout 2s;
            proxy_send_timeout 120s;
            proxy_read_timeout 120s;
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
        }
    }
}
'@
$ExpectedNginx = $ExpectedNginx.Replace("`r`n", "`n").Replace("`r", "`n")
Assert-True ($Nginx.TrimEnd("`n") -ceq $ExpectedNginx.TrimEnd("`n")) "Nginx file differs from the reviewed canonical configuration; comments and extra directives are forbidden"

$httpBlock = Get-BracedBlock $Nginx '(?m)^http\s*\{' 'http block'
$upstreamBlock = Get-BracedBlock $httpBlock.Body '(?m)^\s*upstream\s+ipfs3_gateways\s*\{' 'ipfs3_gateways upstream block'
$serverBlock = Get-BracedBlock $httpBlock.Body '(?m)^\s*server\s*\{' 'Nginx server block'
$healthBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+=\s+/health\s*\{' 'exact /health location'
$readyBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+=\s+/ready\s*\{' 'exact /ready location'
$rootBlock = Get-BracedBlock $serverBlock.Body '(?m)^\s*location\s+/\s*\{' 'root S3 location'

Assert-True (([regex]::Matches($httpBlock.Body, '(?m)^\s*upstream\s+[^\s{]+\s*\{')).Count -eq 1) "Nginx http block must contain exactly one upstream"
Assert-True (([regex]::Matches($httpBlock.Body, '(?m)^\s*server\s*\{')).Count -eq 1) "Nginx http block must contain exactly one server block"
Assert-True (([regex]::Matches($serverBlock.Body, '(?m)^\s*location\s+(?:=\s+)?/[^\s{]*\s*\{')).Count -eq 3) "Nginx server must contain only /, exact /health, and exact /ready locations"

Assert-DirectiveCount $upstreamBlock.Body 'server gateway-a:9000 max_fails=1 fail_timeout=10s;' 1 'upstream gateway-a'
Assert-DirectiveCount $upstreamBlock.Body 'server gateway-b:9000 max_fails=1 fail_timeout=10s;' 1 'upstream gateway-b'
Assert-DirectiveCount $upstreamBlock.Body 'keepalive 32;' 1 'upstream keepalive'

foreach ($probe in @($healthBlock, $readyBlock)) {
    Assert-DirectiveCount $probe.Body 'proxy_pass http://ipfs3_gateways/ready;' 1 'readiness proxy target'
    Assert-DirectiveCount $probe.Body 'proxy_http_version 1.1;' 1 'readiness HTTP version'
    Assert-DirectiveCount $probe.Body 'proxy_set_header Host $http_host;' 1 'readiness signed Host'
    Assert-DirectiveCount $probe.Body 'proxy_set_header Connection "";' 1 'readiness connection header'
    Assert-DirectiveCount $probe.Body 'proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;' 1 'readiness failover classes'
    Assert-DirectiveCount $probe.Body 'proxy_next_upstream_tries 2;' 1 'readiness bounded attempts'
    Assert-True (([regex]::Matches($probe.Body, '(?m)^\s*proxy_set_header\s+Host\s+')).Count -eq 1) "Readiness location has a conflicting Host directive"
}

Assert-DirectiveCount $serverBlock.Body 'client_max_body_size 0;' 1 'server unlimited body size'
Assert-DirectiveCount $rootBlock.Body 'proxy_pass http://ipfs3_gateways;' 1 'root upstream target'
Assert-DirectiveCount $rootBlock.Body 'proxy_http_version 1.1;' 1 'root HTTP version'
Assert-DirectiveCount $rootBlock.Body 'proxy_set_header Host $http_host;' 1 'root signed Host'
Assert-DirectiveCount $rootBlock.Body 'proxy_request_buffering off;' 1 'root request streaming'
Assert-DirectiveCount $rootBlock.Body 'proxy_buffering off;' 1 'root response streaming'
Assert-DirectiveCount $rootBlock.Body 'proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;' 1 'root failover classes'
Assert-DirectiveCount $rootBlock.Body 'proxy_next_upstream_tries 2;' 1 'root bounded attempts'
Assert-True (([regex]::Matches($rootBlock.Body, '(?m)^\s*proxy_set_header\s+Host\s+')).Count -eq 1) "Root location has a conflicting Host directive"
Assert-NotMatches $serverBlock.Body '(?m)^\s*proxy_(?:request_)?buffering\s+on;' "Nginx proxy buffering must never be enabled"
Assert-NotMatches $serverBlock.Body '(?m)^\s*proxy_next_upstream_tries\s+(?:0|[3-9][0-9]*);' "Nginx upstream retries must remain bounded at two"
Assert-True (([regex]::Matches($serverBlock.Body, '(?m)^\s*proxy_pass\s+')).Count -eq 3) "Only the three reviewed locations may proxy requests"

$logLine = @($httpBlock.Body -split "`n" | Where-Object { $_ -match '^\s*log_format gateway ' })
Assert-True ($logLine.Count -eq 1) "Expected one safe Nginx log format in the http block"
Assert-NotMatches $logLine[0] '(?i)authorization|request_body|http_' "Nginx access log must not contain headers or body variables"

Write-Host "multi-gateway static contract tests: PASSED"
```

- [ ] **Step 6: Run Task 1 GREEN and no-side-effect boundary checks**

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway deployment static contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Whitespace validation failed" }
git diff --name-only -- docker-compose.yml docker-compose.postgres.yml tests/compose.postgres-production-validation.yml
if ($LASTEXITCODE -ne 0) { throw "Protected Compose diff query failed" }
```

Expected: static contract prints PASS; `git diff --check` exits zero; protected Compose diff output is empty. Do not run Docker in this task.

- [ ] **Step 7: Completion checkpoint (no Git write)**

Record `Task 1 LOCAL STATIC: PASS`, canonical Nginx equality, six extracted-scope receipts (`http`, upstream, server, root, health, ready), exact created paths, and protected-file hashes. Do not stage or commit.

---

### Task 2: Serialize PostgreSQL migrations with a transaction advisory lock

**Files:**
- Modify: `src/store/mod.rs:9-83,85-275`
- Modify/Test: `src/store/import/lease_clock.rs`
- Modify/Test: `tests/postgres_import.rs`
- Modify/Test: `tests/multi-gateway.Tests.ps1`
- Verify unchanged: `src/state.rs:27-36`, every `src/store/migrations/*.rs`, `Cargo.toml`, `Cargo.lock`

**Interfaces:**
- Consumes: `DatabaseConnection`, `ConnectionTrait::get_database_backend`, `TransactionTrait::begin`, `DatabaseTransaction::execute_unprepared/commit`, `MigratorTrait::up`, current eight migration names, `IPFS_S3_TEST_POSTGRES_URL`, the existing serialized unique-schema PostgreSQL test pattern, and SeaQuery `Expr::value(...).gt(Expr::cust(...))` bind generation.
- Produces: constants `POSTGRES_MIGRATION_LOCK_KEY_1: i32 = 1229997651`, `POSTGRES_MIGRATION_LOCK_KEY_2: i32 = 1395879239`; private `async fn run_postgres_migrations(db: &DatabaseConnection) -> Result<(), DbErr>`; unchanged public `pub async fn run_migrations(db: &DatabaseConnection) -> Result<(), DbErr>`; sanitized log fields `migration_lock="waiting"|"acquired"|"failure"` and safe `category`; PostgreSQL renewal SQL with a numbered bind and no literal question mark.

- [ ] **Step 1: Add deterministic fresh-schema PostgreSQL RED tests**

In `tests/postgres_import.rs`, reuse `POSTGRES_TEST_SERIAL`, `connect_single`, UUID-simple schema names, `SET search_path`, `backend_pid`, and `pg_blocking_pids` style polling. Add:

```rust
const MIGRATION_LOCK_KEY_1: i32 = 1_229_997_651;
const MIGRATION_LOCK_KEY_2: i32 = 1_395_879_239;
const EXPECTED_MIGRATIONS: [&str; 8] = [
    "m20250701_000001_init",
    "m20260707_000001_decompress_zip",
    "m20260720_000001_sse_c_key_fingerprint",
    "m20260721_000001_multi_provider_pinning",
    "m20260729_000001_ipfs3_import",
    "m20260729_000002_postgres_utc_timestamps",
    "m20260730_000001_standard_mutation_fence",
    "m20260813_000001_postgres_json_columns",
];

async fn set_search_path(db: &DatabaseConnection, schema: &str) {
    assert!(schema.starts_with("migration_"));
    db.execute_unprepared(&format!("SET search_path TO {schema}"))
        .await
        .unwrap();
}

async fn wait_for_two_migration_lock_waiters(observer: &DatabaseConnection) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let row = observer
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    format!(
                        "SELECT COUNT(*)::bigint AS count FROM pg_locks \
                         WHERE locktype = 'advisory' \
                           AND classid = {MIGRATION_LOCK_KEY_1}::oid \
                           AND objid = {MIGRATION_LOCK_KEY_2}::oid \
                           AND NOT granted"
                    ),
                ))
                .await
                .unwrap()
                .unwrap();
            if row.try_get::<i64>("", "count").unwrap() == 2 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both migration connections must wait on the stable advisory key");
}

#[tokio::test]
async fn postgres_concurrent_startup_uses_transaction_advisory_lock_once() {
    let Ok(url) = std::env::var("IPFS_S3_TEST_POSTGRES_URL") else {
        panic!("IPFS_S3_TEST_POSTGRES_URL is required for the migration-lock test");
    };
    let _serial = POSTGRES_TEST_SERIAL.lock().await;
    let schema = format!("migration_{}", uuid::Uuid::new_v4().simple());
    let admin = connect_single(&url).await;
    admin.execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let gate = connect_single(&url).await;
    let first = connect_single(&url).await;
    let second = connect_single(&url).await;
    let observer = connect_single(&url).await;
    set_search_path(&first, &schema).await;
    set_search_path(&second, &schema).await;
    set_search_path(&observer, &schema).await;
    gate.execute_unprepared(&format!(
        "SELECT pg_advisory_lock({MIGRATION_LOCK_KEY_1}, {MIGRATION_LOCK_KEY_2})"
    ))
    .await
    .unwrap();

    let first_task = tokio::spawn(async move { store::run_migrations(&first).await });
    let second_task = tokio::spawn(async move { store::run_migrations(&second).await });
    wait_for_two_migration_lock_waiters(&observer).await;
    gate.execute_unprepared(&format!(
        "SELECT pg_advisory_unlock({MIGRATION_LOCK_KEY_1}, {MIGRATION_LOCK_KEY_2})"
    ))
    .await
    .unwrap();
    first_task.await.unwrap().unwrap();
    second_task.await.unwrap().unwrap();

    let rows = observer
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT version, COUNT(*)::bigint AS count FROM seaql_migrations GROUP BY version ORDER BY version",
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), EXPECTED_MIGRATIONS.len());
    for (row, expected) in rows.iter().zip(EXPECTED_MIGRATIONS) {
        assert_eq!(row.try_get::<String>("", "version").unwrap(), expected);
        assert_eq!(row.try_get::<i64>("", "count").unwrap(), 1);
    }

    admin.execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
```

Add a second serialized test that holds the same session advisory lock, invokes `store::run_migrations` on a fresh-schema connection under a 70-second Tokio deadline, requires the production 60-second lock timeout to return `DbErr::Custom("PostgreSQL migration timeout failed")`, asserts no `buckets` or `seaql_migrations` table was published, releases the lock, and checks captured logs contain only `migration_lock="waiting"`, `migration_lock="failure"`, `category="timeout"`; the logs and returned error must not contain the test DSN, password, `canceling statement`, or raw driver text. Use the existing `CapturedWriter`/`tracing::dispatcher::with_default` pattern from `src/main.rs` in a dedicated current-thread runtime so the subscriber remains active while the future is polled.

- [ ] **Step 2: Run the exact-key concurrency test RED against a disposable PostgreSQL 17 fixture**

Use a unique project and only the Task 1 PostgreSQL service. Before `up`, set all required interpolation variables, disable `.env`, refuse existing labelled resources and occupied port `55434`, mark ownership, and mark attempted immediately before `up`. Always emit PostgreSQL logs before owned cleanup and restore every process environment variable to its exact prior presence/value.

Run the focused test after the fixture is healthy:

```powershell
$env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:$($env:POSTGRES_PASSWORD)@127.0.0.1:55434/ipfs3"
cargo test --test postgres_import postgres_concurrent_startup_uses_transaction_advisory_lock_once -- --exact --nocapture --test-threads=1
$redExit = $LASTEXITCODE
if ($redExit -eq 0) { throw "Expected migration-lock test to fail before PostgreSQL lock implementation" }
```

Expected: RED because the observer cannot see both connections waiting on `(1229997651,1395879239)` (or the unlocked concurrent migration fails); never accept an unset-variable skip as RED.

- [ ] **Step 3: Implement the minimal PostgreSQL-only transaction sequence**

Replace the current direct-only body in `src/store/mod.rs` with this boundary; import `ConnectionTrait`, `DatabaseBackend`, `DbErr`, and `TransactionTrait`:

```rust
pub const POSTGRES_MIGRATION_LOCK_KEY_1: i32 = 1_229_997_651;
pub const POSTGRES_MIGRATION_LOCK_KEY_2: i32 = 1_395_879_239;

fn postgres_migration_failure(category: &'static str) -> sea_orm::DbErr {
    tracing::error!(migration_lock = "failure", category);
    sea_orm::DbErr::Custom(format!("PostgreSQL migration {category} failed"))
}

async fn run_postgres_migrations(
    db: &sea_orm::DatabaseConnection,
) -> Result<(), sea_orm::DbErr> {
    use sea_orm::{ConnectionTrait, TransactionTrait};
    use sea_orm_migration::MigratorTrait;

    let txn = db
        .begin()
        .await
        .map_err(|_| postgres_migration_failure("setup"))?;
    txn
        .execute_unprepared("SET LOCAL lock_timeout = '60s'")
        .await
        .map_err(|_| postgres_migration_failure("setup"))?;
    tracing::info!(migration_lock = "waiting");
    txn
        .execute_unprepared(
            "SELECT pg_advisory_xact_lock(1229997651, 1395879239)",
        )
        .await
        .map_err(|_| postgres_migration_failure("timeout"))?;
    tracing::info!(migration_lock = "acquired");
    migrator::Migrator::up(&txn, None)
        .await
        .map_err(|_| postgres_migration_failure("migration"))?;
    txn
        .commit()
        .await
        .map_err(|_| postgres_migration_failure("commit"))
}

pub async fn run_migrations(
    db: &sea_orm::DatabaseConnection,
) -> Result<(), sea_orm::DbErr> {
    use sea_orm::ConnectionTrait;
    use sea_orm_migration::MigratorTrait;

    if db.get_database_backend() != sea_orm::DatabaseBackend::Postgres {
        return migrator::Migrator::up(db, None).await;
    }
    run_postgres_migrations(db).await
}
```

Do not log the discarded error value. The outer transaction owns the advisory lock; SeaORM 1.1.20 receives `&DatabaseTransaction` and uses its PostgreSQL nested savepoint path.

- [ ] **Step 4: Lock non-PostgreSQL behavior and source ordering**

Keep `test_migration_runs` unchanged as the executable SQLite regression. Do not add MySQL features or a MySQL service. Use `Get-BracedBlock` to inspect the public dispatcher and private PostgreSQL helper independently, so source declaration order cannot produce a false failure and a transaction cannot leak into the non-PostgreSQL path:

```powershell
$Store = Read-NormalizedText $StorePath
$State = Read-NormalizedText $StatePath
$Main = Read-NormalizedText $MainPath
$publicMigrations = Get-BracedBlock $Store '(?s)(?m)^pub async fn run_migrations\(\s*db: &sea_orm::DatabaseConnection,\s*\) -> Result<\(\), sea_orm::DbErr> \{' 'public run_migrations function'
$postgresMigrations = Get-BracedBlock $Store '(?s)(?m)^async fn run_postgres_migrations\(\s*db: &sea_orm::DatabaseConnection,\s*\) -> Result<\(\), sea_orm::DbErr> \{' 'private run_postgres_migrations helper'

Assert-InOrder $publicMigrations.Body @(
    'if db.get_database_backend() != sea_orm::DatabaseBackend::Postgres',
    'return migrator::Migrator::up(db, None).await;',
    'run_postgres_migrations(db).await'
) "Public migration dispatcher must preserve the direct non-PostgreSQL path and delegate PostgreSQL"
Assert-True (([regex]::Matches($publicMigrations.Body, [regex]::Escape('run_postgres_migrations(db).await'))).Count -eq 1) "Public dispatcher must invoke the PostgreSQL helper exactly once"
Assert-True (([regex]::Matches($publicMigrations.Body, [regex]::Escape('migrator::Migrator::up(db, None).await'))).Count -eq 1) "Public dispatcher must retain exactly one direct non-PostgreSQL Migrator::up call"
Assert-NotContains $publicMigrations.Body '.begin()' "Public migration dispatcher must not begin a transaction for non-PostgreSQL backends"
Assert-NotMatches $publicMigrations.Body '(?i)pg_(?:try_)?advisory' "Public migration dispatcher must not contain advisory-lock SQL"

Assert-InOrder $postgresMigrations.Body @(
    '.begin()',
    '"SET LOCAL lock_timeout = ''60s''"',
    'migration_lock = "waiting"',
    '"SELECT pg_advisory_xact_lock(1229997651, 1395879239)"',
    'migration_lock = "acquired"',
    'migrator::Migrator::up(&txn, None)',
    '.commit()'
) "Private PostgreSQL migration helper sequence changed"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('.begin()'))).Count -eq 1) "PostgreSQL helper must begin exactly one outer transaction"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape("SET LOCAL lock_timeout = '60s'"))).Count -eq 1) "PostgreSQL helper must set exactly one 60-second local lock timeout"
Assert-True (([regex]::Matches($postgresMigrations.Body, '(?i)lock_timeout')).Count -eq 1) "PostgreSQL helper must not add a second or conflicting lock timeout"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('SELECT pg_advisory_xact_lock(1229997651, 1395879239)'))).Count -eq 1) "PostgreSQL helper must take the stable transaction advisory lock exactly once"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('migrator::Migrator::up(&txn, None)'))).Count -eq 1) "PostgreSQL helper must run migrations exactly once through the outer transaction"
Assert-True (([regex]::Matches($postgresMigrations.Body, [regex]::Escape('.commit()'))).Count -eq 1) "PostgreSQL helper must commit exactly once"
Assert-NotMatches $postgresMigrations.Body '(?i)\bpg_(?:try_)?advisory_(?!xact_)lock\s*\(' "Production migration helper must not take a session advisory lock"
Assert-NotMatches $postgresMigrations.Body '(?i)lock_timeout\s*=\s*''(?:0|0ms|0s)''' "Production migration helper must not configure unbounded lock waiting"
Assert-NotMatches $Store 'tracing::(?:error|warn|info)!\([^\r\n]*(?:error|database_url|dsn|password)' "Migration logs must not render private errors or connection data"
Assert-InOrder $State @(
    'let db = crate::store::connect_database(&cfg.storage.database_url).await?;',
    'crate::store::run_migrations(&db).await?;',
    'let store = Store::new(db);'
) "AppState must fail before construction when migrations fail"
Assert-InOrder $Main @(
    'let state = AppState::new(&cfg).await?;',
    'let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;'
) "Gateway must run migrations before binding its listener"
```

- [ ] **Step 5: Run focused GREEN, real timeout, SQLite, and static tests**

With the same owned PostgreSQL fixture and mandatory URL:

```powershell
cargo test --test postgres_import postgres_concurrent_startup_uses_transaction_advisory_lock_once -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Concurrent migration lock test failed" }
cargo test --test postgres_import postgres_migration_lock_timeout_is_fail_closed_and_redacted -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Migration timeout/redaction test failed" }
cargo test --lib store::tests::test_migration_runs -- --exact --nocapture
if ($LASTEXITCODE -ne 0) { throw "SQLite migration regression failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Migration static contract failed" }
```

Expected: both PostgreSQL tests PASS; the timeout test takes approximately 60 seconds and leaves no migration tables in its fresh schema; SQLite PASS; static PASS. Emit fixture logs, clean only the owned attempted project with volumes, prove zero project-labelled residual resources, and restore exact environment state.

- [ ] **Step 6: Lock the runtime-discovered PostgreSQL import-renewal bind defect RED→GREEN**

The first real five-service run reached CID import but timed out. PostgreSQL
recorded this exact causal statement fragment:

```text
UPDATE "import_jobs" ... AND (? > clock_timestamp())
ERROR: syntax error at or near ">"
```

In `src/store/import/lease_clock.rs`, add the focused builder test
`postgres_future_lease_uses_numbered_bind_instead_of_literal_question_mark`.
Build a PostgreSQL select using `lease_end_is_future`, then require one bound
value, `$1 > (clock_timestamp())`, and no `?`. Run it before the fix:

```powershell
cargo test --lib postgres_future_lease_uses_numbered_bind_instead_of_literal_question_mark -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Expected the PostgreSQL renewal placeholder regression to be RED" }
```

Expected RED includes `SELECT "id" FROM "import_jobs" WHERE ? >
clock_timestamp()`. Replace only the PostgreSQL branch:

```rust
DatabaseBackend::Postgres => {
    Expr::value(lease_until).gt(Expr::cust("clock_timestamp()"))
}
```

Import `ExprTrait`; keep the SQLite and MySQL branches byte-for-byte unchanged.
Then run:

```powershell
cargo test --lib lease_clock::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Lease-clock builder regressions failed" }
```

Expected GREEN: all three lease-clock tests pass. The real five-service CID
import in Task 5 is the required surface-level toggle proof; compile-only or SQL
string evidence does not close the runtime failure.

- [ ] **Step 7: Completion checkpoint (no Git write)**

Record `Task 2 PG LOCK: PASS`, separately extracted public-dispatcher/private-helper static receipts, rejection of production session advisory locks and unbounded timeout, exact eight `version:1` rows, sanitized log categories, timeout elapsed time, SQLite PASS, lease-clock builder RED→GREEN, and fixture cleanup receipt. Do not stage or commit.

---

### Task 3: Add the real two-replica Rust acceptance target

**Files:**
- Create/Test: `tests/multi_gateway.rs`
- Reuse unchanged: `tests/support/mod.rs`, `tests/support/sigv4.rs`
- Modify/Test: `tests/multi-gateway.Tests.ps1`
- Verify unchanged: `Cargo.toml`, `Cargo.lock`, `tests/e2e.rs`, `src/s3/route/import_object.rs`

**Interfaces:**
- Consumes: `support::sigv4::send_sigv4(method, endpoint, bucket, key, query, body, headers, "test")`, rust-s3 `Bucket::create_with_path_style`, `Bucket::new(name, Region::Custom { region, endpoint }, credentials).with_path_style()`, `put_object`, `get_object`, `head_object`, `list`, `delete_object`, `initiate_multipart_upload`, `put_multipart_chunk`, `complete_multipart_upload`, import POST `202` header `x-ipfs3-import-job-id`, and status XML elements `State=completed` plus `Artifact/CID`.
- Produces: environment contract `IPFS_S3_MULTI_GATEWAY_A_ENDPOINT`, `IPFS_S3_MULTI_GATEWAY_B_ENDPOINT`, `IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT`, `IPFS_S3_MULTI_GATEWAY_KUBO_URL`; tests `multi_gateway_cross_replica_contract` and `load_balancer_surviving_replica_crud`.

- [ ] **Step 1: Create a compile RED for the absent integration target**

```powershell
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -eq 0) { throw "Expected absent multi_gateway target to be RED" }
```

Expected: no test target named `multi_gateway`.

- [ ] **Step 2: Add endpoint, timeout, credentials, bucket, and HTTP helper seams**

Create `tests/multi_gateway.rs` beginning with `mod support;`, imports from existing dependencies only, constants `S3_TIMEOUT=30s`, `HTTP_TIMEOUT=15s`, `IMPORT_TIMEOUT=30s`, and these exact helper signatures:

```rust
mod support;

use http::{HeaderMap, HeaderValue, StatusCode};
use s3::{bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, region::Region};
use std::{future::Future, sync::atomic::{AtomicU64, Ordering}, time::{Duration, SystemTime, UNIX_EPOCH}};
use support::{decompress::S3TestEndpoint, sigv4::send_sigv4};

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const IMPORT_TIMEOUT: Duration = Duration::from_secs(30);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct LiveEndpoint {
    endpoint: String,
    bucket: String,
}

impl S3TestEndpoint for LiveEndpoint {
    fn endpoint(&self) -> &str { &self.endpoint }
    fn bucket(&self) -> &str { &self.bucket }
}

fn endpoint_from_env(name: &str) -> String {
    let endpoint = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    let endpoint = endpoint.trim_end_matches('/').to_owned();
    assert!(endpoint.starts_with("http://") || endpoint.starts_with("https://"));
    assert!(!endpoint.to_ascii_lowercase().contains("localhost"));
    endpoint
}

fn test_credentials() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None).unwrap()
}

fn bucket_at(endpoint: &str, name: &str) -> Box<Bucket> {
    Bucket::new(
        name,
        Region::Custom { region: "us-east-1".to_owned(), endpoint: endpoint.to_owned() },
        test_credentials(),
    ).unwrap().with_path_style()
}

async fn s3_call<T, E, F>(label: &str, future: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    tokio::time::timeout(S3_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("S3 operation timed out: {label}"))
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("mg-{scenario}-{}-{nanos:x}-{counter:x}", std::process::id());
    assert!(name.len() <= 63);
    name
}

fn etag(headers: &std::collections::HashMap<String, String>) -> String {
    headers.get("etag").or_else(|| headers.get("ETag"))
        .cloned().unwrap_or_default().trim_matches('"').to_owned()
}
```

Do not add a new shared helper API; this live target owns its endpoint-specific wrappers and reuses only the already-existing public test support.

- [ ] **Step 3: Implement direct A-write/B-read-head-list-delete and cross-phase multipart**

In `multi_gateway_cross_replica_contract`, create one unique bucket through A, construct A and B clients for that name, then assert in order:

1. A `put_object("crud.txt", b"written-through-a")` returns 200 and a non-empty CID ETag.
2. B `get_object` returns the exact body; B `head_object` returns 200, matching ETag and content length; B `list("", None)` contains `crud.txt`.
3. B `delete_object` returns 204; A GET returns 404/NoSuchKey.
4. A initiates `multipart.bin`; A uploads part 1 with exactly 5 MiB; B uploads part 2 with `b"part-two-through-b"`; B completes with both returned parts in ascending order; A retrieves the exact concatenation.

Use the existing e2e API calls exactly; do not hand-build multipart XML or invent a client method.

- [ ] **Step 4: Implement concurrent whole-payload convergence and CID identity**

Use `tokio::join!` around two existing `send_sigv4` PUT calls to the same key, one through A and one through B, with distinct complete bodies. Require each response to be 200 or 409, at least one 200, and no 5xx. Then GET and HEAD through both direct endpoints; require identical final bodies, matching direct ETags, and final body equal to exactly one submitted payload. POST Kubo `/api/v0/cat?arg=<final-etag>` using the configured `IPFS_S3_MULTI_GATEWAY_KUBO_URL` and require the same whole body. A mixed body or differing CID/ETag fails.

- [ ] **Step 5: Implement CID import submit-through-A/status-through-B**

Submit this XML through A with signed POST query `[('ipfs3-import', '')]` and `Content-Type: application/xml`:

```xml
<IPFS3ImportRequest><CID>{final_cid}</CID></IPFS3ImportRequest>
```

Require `202`, extract one non-empty `x-ipfs3-import-job-id`, then poll a signed GET through B with query `[('ipfs3-import', job_id)]` every 250ms under `IMPORT_TIMEOUT`. Require a persisted status body containing the same JobId, `<State>completed</State>`, and `<CID>{final_cid}</CID>`; any terminal `<State>failed</State>` fails immediately. Finally GET the imported key through B and require the converged body.

- [ ] **Step 6: Add the post-failover load-balancer CRUD test**

`load_balancer_surviving_replica_crud` uses only `IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT`: create a new unique bucket, PUT `after-failover.txt`, GET exact bytes, HEAD matching CID/length, LIST containing the key, DELETE the key, then delete the bucket. This test is invoked only after Task 5 stops gateway A; it must not use AWS CLI, mc, rclone, or a direct gateway endpoint.

- [ ] **Step 7: Extend static scenario locking and compile GREEN**

Append structural checks for the two exact test names, four endpoint names, `mod support`, `send_sigv4`, `tokio::join!`, multipart methods, `x-ipfs3-import-job-id`, completed status, and failover CRUD method calls. Assert the source contains no `Command::new`, `aws`, `mc`, or `rclone` execution.

```powershell
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "multi_gateway integration target did not compile" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Live-scenario static contract failed" }
```

Expected: compile succeeds using existing dependencies; static PASS. Do not claim runtime PASS until Task 5.

- [ ] **Step 8: Completion checkpoint (no Git write)**

Record `Task 3 COMPILE: PASS; LIVE: NOT RUN`. Do not stage or commit.

---

### Task 4: Add the independent blocking release job and preserve existing job meanings

**Files:**
- Modify/Test: `.github/workflows/release-validation.yml`
- Modify/Test: `tests/release-validation.Tests.ps1`
- Modify/Test: `tests/postgres-production-baseline.Tests.ps1` (workflow expectations only)
- Modify/Test: `tests/multi-gateway.Tests.ps1`
- Verify unchanged: `.github/workflows/ci.yml`, existing four job blocks except the added static command in `client-smoke-infrastructure`

**Interfaces:**
- Consumes: Task 1 Compose/Nginx/override, Task 3 test target, GitHub run identity, existing Docker Compose version parser, owned/attempted marker pattern, and exact environment restoration pattern.
- Produces: independent blocking job `multi-gateway-deployment`; unique project `ipfs3-mg-${{ github.run_id }}-${{ github.run_attempt }}`; static infrastructure command `pwsh -NoProfile -File tests/multi-gateway.Tests.ps1`; hosted evidence surface that is initially reported `NOT RUN`.

- [ ] **Step 1: Extend all static workflow contracts first and capture RED**

Update root job expectations from four to five while preserving exact assertions for all existing jobs. In both existing PowerShell tests, add only `multi-gateway-deployment` to the required set and add the fourth blocking client command after the PostgreSQL baseline contract and before `client-smoke.Tests.ps1`:

```yaml
      - name: Test multi-gateway deployment contract
        run: pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
```

In `tests/release-validation.Tests.ps1` and `tests/multi-gateway.Tests.ps1`, extract the new job block and require: Ubuntu, 60-minute timeout, no root `needs`, no root `continue-on-error`, `COMPOSE_DISABLE_ENV_FILE: "1"`, unique project identity, all fixed endpoints/ports, exact step order, Rust setup, direct suite, E2E 11/11 command, pre-failover logs, gateway-a-only stop, bounded readiness loop, post-failover filtered test, diagnostics before cleanup, and zero residual queries.

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
$releaseRed = $LASTEXITCODE
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
$baselineRed = $LASTEXITCODE
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$multiRed = $LASTEXITCODE
if ($releaseRed -eq 0 -or $baselineRed -eq 0 -or $multiRed -eq 0) { throw "All workflow contracts must be RED before the fifth job exists" }
```

Expected: failures identify the absent fifth job/new static command, not a weakened existing assertion.

- [ ] **Step 2: Add exact job environment and setup**

Insert a peer job (no `needs`) with this job-scope contract:

```yaml
  multi-gateway-deployment:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      COMPOSE_DISABLE_ENV_FILE: "1"
      COMPOSE_PROJECT_NAME: ipfs3-mg-${{ github.run_id }}-${{ github.run_attempt }}
      POSTGRES_PASSWORD: mg-${{ github.run_id }}-${{ github.run_attempt }}
      IPFS_S3_ACCESS_KEY_ID: test
      IPFS_S3_SECRET_ACCESS_KEY: test
      IPFS_S3_MASTER_KEY: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      IPFS_S3_LOAD_BALANCER_BIND: 127.0.0.1
      IPFS_S3_LOAD_BALANCER_PORT: 59000
      IPFS_S3_MULTI_GATEWAY_A_ENDPOINT: http://127.0.0.1:59001
      IPFS_S3_MULTI_GATEWAY_B_ENDPOINT: http://127.0.0.1:59002
      IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT: http://127.0.0.1:59000
      IPFS_S3_MULTI_GATEWAY_KUBO_URL: http://127.0.0.1:55002
      IPFS_S3_E2E_ENDPOINT: http://127.0.0.1:59000
      IPFS_S3_E2E_KUBO_URL: http://127.0.0.1:55002
      IPFS_S3_TEST_POSTGRES_URL: postgres://ipfs3:mg-${{ github.run_id }}-${{ github.run_attempt }}@127.0.0.1:55434/ipfs3
    steps:
      - uses: actions/checkout@v7

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@v1
        with:
          toolchain: "1.92"

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2
```

- [ ] **Step 3: Add fail-closed environment, project, and port preflights**

Add `shell: pwsh` steps that:

1. Parse `docker compose version --short` with the existing strict numeric-core parser and require `>=2.23.1`.
2. Build `$compose = @("--project-name", $env:COMPOSE_PROJECT_NAME, "-f", "docker-compose.multi-gateway.yml", "-f", "tests/compose.multi-gateway-validation.yml")`.
3. Snapshot the four required secret values, remove each with `Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop`, require `docker compose @compose config --quiet` non-zero, and restore exact presence/value in each `finally`; require full config success after restoration.
4. Validate URL-safe PostgreSQL password, 64-hex master key, exact non-wildcard LB bind, and port range.
5. Query all-state containers, networks, and volumes by exact project label with immediate exit checks; refuse non-empty results.
6. Probe ports `55434,55002,59000,59001,59002` with loopback `TcpListener`; only then append `MULTI_GATEWAY_OWNED=true` to `$env:GITHUB_ENV`.

Do not print composed configuration or secret values.

- [ ] **Step 4: Add startup, readiness, marker, direct, E2E, and pre-failover steps**

Immediately before startup append `MULTI_GATEWAY_ATTEMPTED=true`; run exactly:

```yaml
      - name: Build and start the multi-gateway topology
        shell: pwsh
        run: |
          "MULTI_GATEWAY_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.multi-gateway.yml -f tests/compose.multi-gateway-validation.yml up --detach --build --wait --wait-timeout 300 postgres kubo gateway-a gateway-b load-balancer
          if ($LASTEXITCODE -ne 0) { throw "Multi-gateway topology did not become healthy" }

      - name: Run direct cross-replica acceptance
        run: cargo test --test multi_gateway multi_gateway_cross_replica_contract -- --exact --nocapture --test-threads=1

      - name: Run existing E2E through the load balancer
        run: cargo test --test e2e -- --nocapture --test-threads=1

      - name: Capture pre-failover diagnostics
        shell: pwsh
        run: |
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.multi-gateway.yml -f tests/compose.multi-gateway-validation.yml logs --no-color postgres kubo gateway-a gateway-b load-balancer
          if ($LASTEXITCODE -ne 0) { throw "Pre-failover diagnostics failed" }
```

Between startup and direct acceptance, add a PowerShell step requiring A and B direct `/ready` to return exact `200 READY`, LB `/health` and `/ready` to return exact `200 READY`, then query `seaql_migrations` through `docker compose exec -T postgres psql` and compare exactly the eight approved `version:1` rows in sorted order.

- [ ] **Step 5: Add bounded one-replica failover and new CRUD**

Stop only gateway A, check the Docker exit immediately, require B direct `200 READY`, and poll LB `/ready` every 500ms for no more than 30 seconds. Then run:

```yaml
      - name: Run new CRUD through the surviving load-balanced path
        run: cargo test --test multi_gateway load_balancer_surviving_replica_crud -- --exact --nocapture --test-threads=1
```

Do not restart A and do not describe this as active health checking or HA.

- [ ] **Step 6: Add always-on diagnostics and owned cleanup**

The diagnostics step runs on `always()` only when attempted and logs all five services with `--no-color`. The later cleanup step runs on `always()` only when both owned and attempted, executes project-scoped disposable `down --volumes --remove-orphans`, then independently queries exact project-labelled containers, networks, and volumes with immediate exit capture and requires all three sets empty. Diagnostics must precede cleanup in YAML; the workflow contract must reject cleanup code that throws before all three residual query exits have been captured.

- [ ] **Step 7: Parse every workflow PowerShell block and run workflow contracts GREEN**

In `tests/multi-gateway.Tests.ps1`, extract each `shell: pwsh` + `run: |` body from the new job, remove its YAML indentation, and pass it to `[System.Management.Automation.Language.Parser]::ParseInput`; throw all parser messages. Also reject Bash chaining/export/null-device syntax, any `SetEnvironmentVariable` call whose value argument is `$null`, broad Docker prune, cleanup before diagnostics, missing per-query `$LASTEXITCODE` captures, or an environment probe whose exact restoration is not in `finally`. Read `tests/e2e.rs`, count exactly eleven `#[tokio::test]` attributes, and lock the exact serial E2E command so `11/11` cannot silently become a smaller target.

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release workflow contract failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Single-PostgreSQL baseline contract regressed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway workflow contract failed" }
```

Expected: all PASS; existing single-PG and SQLite job assertions remain byte-for-byte equivalent except root job/static-command counts.

- [ ] **Step 8: Completion checkpoint (no Git write)**

Record `Task 4 WORKFLOW STATIC: PASS` and `HOSTED multi-gateway-deployment: NOT RUN`. Do not stage, commit, push, or tag.

---

### Task 5: Execute one fail-closed local workflow-parity live gate

**Files:**
- Test only: all Task 1-4 implementation surfaces
- No file edits until every result and cleanup check below passes

**Interfaces:**
- Consumes: exact hosted job commands and environment names, fixed loopback ports, disposable project labels, both Rust live tests, existing E2E target, full PostgreSQL regression target, and Docker Compose ownership markers.
- Produces: one LOCAL evidence matrix covering env isolation, empty-PG concurrent startup, eight markers, direct A/B suite, E2E 11/11 through LB, import A→B, gateway-A stop and ≤30s recovery, new LB CRUD, diagnostics-before-cleanup, zero residual resources, unconditional exact 15/15 environment restoration, and a single post-restoration aggregate error outcome.

- [ ] **Step 1: Run no-Docker static/compile preflight and require a clean index**

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway static preflight failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release static preflight failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL baseline static preflight failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway test compile failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not have staged changes" }
```

- [ ] **Step 2: Snapshot exact environment state and claim a unique local identity**

Run the live wave from repository root in one PowerShell process. The outer `finally` restores every name below to prior presence/value; prior absence is restored with `Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop` and verified as absent.

```powershell
$ErrorActionPreference = "Stop"
$environmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "COMPOSE_PROJECT_NAME",
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_LOAD_BALANCER_BIND",
    "IPFS_S3_LOAD_BALANCER_PORT",
    "IPFS_S3_MULTI_GATEWAY_A_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_B_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_KUBO_URL",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL",
    "IPFS_S3_TEST_POSTGRES_URL"
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [pscustomobject]@{
        Exists = Test-Path -LiteralPath "Env:$name"
        Value = [Environment]::GetEnvironmentVariable($name, "Process")
    }
}
$owned = $false
$attempted = $false
$project = "ipfs3-mg-local-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$compose = @(
    "--project-name", $project,
    "-f", "docker-compose.multi-gateway.yml",
    "-f", "tests/compose.multi-gateway-validation.yml"
)
```

- [ ] **Step 3: Set isolated values, verify missing-secret rejection, and preflight resources**

Define the first primary-phase function below. It sets exact local values, validates each of the four secret-removal probes with an inner `finally`, then requires complete config. It queries all-state project resources and all five ports before setting script-scope ownership; Task 5 Step 7 invokes this function inside the primary `try`.

```powershell
function Initialize-MultiGatewayValidation {
$env:COMPOSE_DISABLE_ENV_FILE = "1"
$env:COMPOSE_PROJECT_NAME = $project
$env:POSTGRES_PASSWORD = "mg-local-$PID"
$env:IPFS_S3_ACCESS_KEY_ID = "test"
$env:IPFS_S3_SECRET_ACCESS_KEY = "test"
$env:IPFS_S3_MASTER_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
$env:IPFS_S3_LOAD_BALANCER_BIND = "127.0.0.1"
$env:IPFS_S3_LOAD_BALANCER_PORT = "59000"
$env:IPFS_S3_MULTI_GATEWAY_A_ENDPOINT = "http://127.0.0.1:59001"
$env:IPFS_S3_MULTI_GATEWAY_B_ENDPOINT = "http://127.0.0.1:59002"
$env:IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT = "http://127.0.0.1:59000"
$env:IPFS_S3_MULTI_GATEWAY_KUBO_URL = "http://127.0.0.1:55002"
$env:IPFS_S3_E2E_ENDPOINT = "http://127.0.0.1:59000"
$env:IPFS_S3_E2E_KUBO_URL = "http://127.0.0.1:55002"
$env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:$($env:POSTGRES_PASSWORD)@127.0.0.1:55434/ipfs3"

foreach ($name in @("POSTGRES_PASSWORD", "IPFS_S3_ACCESS_KEY_ID", "IPFS_S3_SECRET_ACCESS_KEY", "IPFS_S3_MASTER_KEY")) {
    $value = [Environment]::GetEnvironmentVariable($name, "Process")
    try {
        Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
        docker compose @compose config --quiet
        $missingExit = $LASTEXITCODE
    } finally {
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
        if (-not (Test-Path -LiteralPath "Env:$name")) { throw "Required variable restoration lost presence: $name" }
        if ([Environment]::GetEnvironmentVariable($name, "Process") -cne $value) { throw "Required variable restoration changed value: $name" }
    }
    if ($missingExit -eq 0) { throw "Compose accepted missing required variable: $name" }
}
docker compose @compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "Complete multi-gateway Compose config failed" }

$containerRaw = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")
if ($LASTEXITCODE -ne 0) { throw "Container ownership preflight failed" }
$networkRaw = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project")
if ($LASTEXITCODE -ne 0) { throw "Network ownership preflight failed" }
$volumeRaw = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project")
if ($LASTEXITCODE -ne 0) { throw "Volume ownership preflight failed" }
$ownedResources = @($containerRaw + $networkRaw + $volumeRaw | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($ownedResources.Count -ne 0) { throw "Unique project already owns resources: $project" }
foreach ($port in @(55434, 55002, 59000, 59001, 59002)) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
    try { $listener.Start() } catch { throw "Fixed validation port is occupied: $port" } finally { $listener.Stop() }
}
$script:owned = $true
}
```

- [ ] **Step 4: Start all five services from an empty database and prove readiness/markers**

Define the startup function. It sets script-scope attempted state immediately before the single `up` command so Compose starts A and B concurrently once PostgreSQL and Kubo are healthy:

```powershell
function Start-MultiGatewayTopology {
$script:attempted = $true
docker compose @compose up --detach --build --wait --wait-timeout 300 postgres kubo gateway-a gateway-b load-balancer
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway topology did not become healthy" }
foreach ($endpoint in @("http://127.0.0.1:59001/ready", "http://127.0.0.1:59002/ready", "http://127.0.0.1:59000/health", "http://127.0.0.1:59000/ready")) {
    $response = Invoke-WebRequest -Uri $endpoint -TimeoutSec 5
    if ($response.StatusCode -ne 200 -or $response.Content -cne "READY") { throw "Unexpected readiness response: $endpoint" }
}
$migrationRaw = @(docker compose @compose exec -T postgres psql -U postgres -d ipfs3 -tA -c "SELECT version || ':' || COUNT(*)::text FROM seaql_migrations GROUP BY version ORDER BY version;")
if ($LASTEXITCODE -ne 0) { throw "Migration marker query failed" }
$actualMigrations = @($migrationRaw | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne "" })
$expectedMigrations = @(
    "m20250701_000001_init:1",
    "m20260707_000001_decompress_zip:1",
    "m20260720_000001_sse_c_key_fingerprint:1",
    "m20260721_000001_multi_provider_pinning:1",
    "m20260729_000001_ipfs3_import:1",
    "m20260729_000002_postgres_utc_timestamps:1",
    "m20260730_000001_standard_mutation_fence:1",
    "m20260813_000001_postgres_json_columns:1"
)
if ($actualMigrations.Count -ne 8) { throw "Expected exactly eight migration markers: $($actualMigrations -join ', ')" }
for ($index = 0; $index -lt 8; $index++) {
    if ($actualMigrations[$index] -cne $expectedMigrations[$index]) { throw "Unexpected migration marker at $index" }
}
}
```

- [ ] **Step 5: Run direct cross-replica, E2E 11/11, import, and PostgreSQL regressions**

```powershell
function Invoke-MultiGatewayRegression {
cargo test --test multi_gateway multi_gateway_cross_replica_contract -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Direct cross-replica suite failed" }
cargo test --test e2e -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Existing E2E through load balancer failed" }
cargo test --test postgres_import -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Full PostgreSQL regression target failed" }
docker compose @compose logs --no-color postgres kubo gateway-a gateway-b load-balancer
if ($LASTEXITCODE -ne 0) { throw "Pre-failover diagnostics failed" }
}
```

Expected: direct suite proves CRUD, multipart, whole-payload CID/ETag/body convergence, and import A→B completed; existing E2E reports all 11 tests PASS through port 59000; every PostgreSQL test (including the two migration-lock tests) runs rather than skips; logs are captured before any stop.

- [ ] **Step 6: Stop A, recover through B within 30 seconds, and run new LB CRUD**

```powershell
function Invoke-MultiGatewayFailover {
docker compose @compose stop gateway-a
if ($LASTEXITCODE -ne 0) { throw "Stopping gateway-a failed" }
$gatewayB = Invoke-WebRequest -Uri "http://127.0.0.1:59002/ready" -TimeoutSec 5
if ($gatewayB.StatusCode -ne 200 -or $gatewayB.Content -cne "READY") { throw "gateway-b readiness did not survive" }
$deadline = [DateTime]::UtcNow.AddSeconds(30)
$recovered = $false
do {
    try {
        $ready = Invoke-WebRequest -Uri "http://127.0.0.1:59000/ready" -TimeoutSec 3 -SkipHttpErrorCheck
        if ($ready.StatusCode -eq 200 -and $ready.Content -ceq "READY") { $recovered = $true; break }
    } catch {
        Write-Host "Load-balancer readiness has not recovered through gateway-b"
    }
    Start-Sleep -Milliseconds 500
} while ([DateTime]::UtcNow -lt $deadline)
if (-not $recovered) { throw "Load balancer did not recover through gateway-b within 30 seconds" }
cargo test --test multi_gateway load_balancer_surviving_replica_crud -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Post-failover load-balancer CRUD failed" }
}
```

- [ ] **Step 7: Aggregate primary/cleanup failures while making 15-variable restoration unskippable**

Invoke the four exact functions from Steps 3-6 inside the inner primary `try`. Its `catch` records rather than rethrows the primary error. The inner `finally` always attempts diagnostics, owned cleanup, and all three residual queries, recording every native exit immediately without throwing. The outer `finally` then restores and verifies all 15 environment entries one by one regardless of any Docker failure; each restoration has its own `try/catch`, so one failed variable cannot skip the remaining names. Only after restoration completes does the script aggregate primary, cleanup, residual, and restoration errors and throw once.

```powershell
$primaryErrors = [Collections.Generic.List[string]]::new()
$cleanupErrors = [Collections.Generic.List[string]]::new()
$restoreErrors = [Collections.Generic.List[string]]::new()
$restoredCount = 0
try {
    try {
        Initialize-MultiGatewayValidation
        Start-MultiGatewayTopology
        Invoke-MultiGatewayRegression
        Invoke-MultiGatewayFailover
    } catch {
        $primaryErrors.Add($_.Exception.Message)
    } finally {
        if ($attempted) {
            $diagnosticExit = -1
            try {
                docker compose @compose logs --no-color postgres kubo gateway-a gateway-b load-balancer
                $diagnosticExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Final diagnostics could not execute: $($_.Exception.Message)")
            }
            if ($diagnosticExit -ne 0) { $cleanupErrors.Add("Final diagnostics exited $diagnosticExit") }
        }
        if ($owned -and $attempted) {
            $downExit = -1
            try {
                docker compose @compose down --volumes --remove-orphans
                $downExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Disposable project cleanup could not execute: $($_.Exception.Message)")
            }
            if ($downExit -ne 0) { $cleanupErrors.Add("Disposable project cleanup exited $downExit") }

            $containerRaw = @()
            $containerExit = -1
            try {
                $containerRaw = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project")
                $containerExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual container query could not execute: $($_.Exception.Message)")
            }
            if ($containerExit -ne 0) { $cleanupErrors.Add("Residual container query exited $containerExit") }

            $networkRaw = @()
            $networkExit = -1
            try {
                $networkRaw = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project")
                $networkExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual network query could not execute: $($_.Exception.Message)")
            }
            if ($networkExit -ne 0) { $cleanupErrors.Add("Residual network query exited $networkExit") }

            $volumeRaw = @()
            $volumeExit = -1
            try {
                $volumeRaw = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project")
                $volumeExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual volume query could not execute: $($_.Exception.Message)")
            }
            if ($volumeExit -ne 0) { $cleanupErrors.Add("Residual volume query exited $volumeExit") }

            $residual = @($containerRaw + $networkRaw + $volumeRaw | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            if ($residual.Count -ne 0) { $cleanupErrors.Add("Residual project resources remain: $($residual -join ', ')") }
        }
    }
} finally {
    foreach ($name in $environmentNames) {
        try {
            $saved = $savedEnvironment[$name]
            if ($saved.Exists) {
                [Environment]::SetEnvironmentVariable($name, $saved.Value, "Process")
                if (-not (Test-Path -LiteralPath "Env:$name")) { throw "restoration lost prior presence" }
                $restoredValue = [Environment]::GetEnvironmentVariable($name, "Process")
                if ($restoredValue -cne $saved.Value) { throw "restoration changed the case-sensitive prior value" }
            } else {
                if (Test-Path -LiteralPath "Env:$name") {
                    Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
                }
                if (Test-Path -LiteralPath "Env:$name") { throw "restoration retained a prior-absent provider entry" }
                if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "restoration retained a prior-absent process value" }
            }
            $restoredCount++
        } catch {
            $restoreErrors.Add("${name}: $($_.Exception.Message)")
        }
    }
    if ($restoredCount -ne $environmentNames.Count) {
        $restoreErrors.Add("Environment restoration verified $restoredCount/$($environmentNames.Count), expected 15/15")
    }
}

$allErrors = [Collections.Generic.List[string]]::new()
foreach ($entry in $primaryErrors) { $allErrors.Add("primary: $entry") }
foreach ($entry in $cleanupErrors) { $allErrors.Add("cleanup: $entry") }
foreach ($entry in $restoreErrors) { $allErrors.Add("restore: $entry") }
if ($allErrors.Count -ne 0) { throw ($allErrors -join "`n") }
Write-Host "multi-gateway local workflow parity: PASSED environment_restore=15/15 residual=0"
```

This ordering is mandatory: no diagnostic, `down`, or residual-query error may bypass the outer restoration `finally`; no restoration error may stop attempts for later variables; and no cleanup uncertainty may be reported as zero-residual success.

- [ ] **Step 8: Record the exact LOCAL matrix and hosted boundary**

Record each line separately: env isolation 4/4; project/ports preflight; five services healthy; A/B direct ready; LB health/ready; migrations 8/8 each once; direct CRUD; multipart A→B; concurrent whole-payload convergence; import A→B completed; E2E 11/11 via LB; PostgreSQL target all PASS; pre-failover logs; B ready after A stop; LB recovered in ≤30s; new LB CRUD; final logs; zero containers/networks/volumes; environment restore 15/15; aggregate error count zero. Record `HOSTED multi-gateway-deployment: NOT RUN`. Any missing LOCAL PASS, cleanup uncertainty, or restoration count below 15 blocks Task 6.

---

### Task 6: Update README and exactly one ROADMAP checkbox after LOCAL parity passes

**Files:**
- Modify: `README.md`
- Modify: `ROADMAP.md:65-70`
- Test: `tests/multi-gateway.Tests.ps1`

**Interfaces:**
- Consumes: Task 5 complete LOCAL PASS matrix and zero-residual receipt.
- Produces: one bounded operator section and only `- [x] Multiple gateway instances (horizontal scaling)`; no HA, Cluster, private swarm, provider coordination, or hosted-PASS claim.

- [ ] **Step 1: Add documentation RED assertions before editing docs**

Extend the static contract to require the supported declaration, exact Compose filename, Nginx endpoint, identical credentials/master key, no stickiness, shared PostgreSQL/Kubo, remote providers absent/no cluster-level limits, direct ports test-only, all remaining single points of failure, and non-destructive production shutdown. Require line 68 checked and lines 69-70 unchanged/unchecked. Require README's multi-gateway production section not to contain `down --volumes`.

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Expected docs contract to be RED before evidence-gated edits" }
```

- [ ] **Step 2: Add the bounded README operator section**

After the single-PostgreSQL baseline section, document exactly two gateways behind one Nginx entry point, one shared PostgreSQL 17 and Kubo, no stickiness, identical credentials/master key, both existing workers in both processes, no remote providers, no direct production ports except LB, and the host/PostgreSQL/Kubo/Nginx single points of failure. Provide PowerShell setup using the six required variables and:

```powershell
docker compose -f docker-compose.multi-gateway.yml config --quiet
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway Compose configuration is invalid" }
docker compose -f docker-compose.multi-gateway.yml up --detach --build --wait --wait-timeout 300
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway topology did not become healthy" }
```

Production shutdown is only:

```powershell
docker compose -f docker-compose.multi-gateway.yml down --remove-orphans
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway production shutdown failed" }
```

State explicitly that operators must not add `--volumes` to that production shutdown.

- [ ] **Step 3: Check only the horizontal-scaling roadmap item**

Change exactly:

```markdown
- [ ] Multiple gateway instances (horizontal scaling)
```

to:

```markdown
- [x] Multiple gateway instances (horizontal scaling)
```

Leave `IPFS Cluster for pinset replication` and `Private swarm (swarm.key) for node-to-node communication` unchecked.

- [ ] **Step 4: Run documentation GREEN and scope diff**

```powershell
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Evidence-gated documentation contract failed" }
git diff --unified=0 -- README.md ROADMAP.md
if ($LASTEXITCODE -ne 0) { throw "Documentation diff query failed" }
```

Expected: static PASS; README makes only the bounded declaration; ROADMAP has one changed checkbox; no hosted PASS or HA claim; no production `down --volumes`.

- [ ] **Step 5: Completion checkpoint (no Git write)**

Record `Task 6 DOCS: PASS (gated by Task 5 LOCAL ALL PASS)` and `HOSTED multi-gateway-deployment: NOT RUN`. Do not stage or commit.

---

### Task 7: Run complete verification, freeze the exact identity, and hand off Oracle/Reviewer approval and commit

**Files:**
- Verify all exact final-manifest paths listed below
- No implementation edits after identity generation unless verification/review is restarted

**Interfaces:**
- Consumes: Tasks 1-6 receipts, Task 5 retained local matrix, exact protected spec hash, Git HEAD `0fd19f006cdcdcf6acefca546e1a5a0c27b5c53c` unless the orchestrator records an authorized base change, LSP diagnostics, Oracle and Reviewer profiles owned by the orchestrator.
- Produces: complete static/Rust/quality/boundary receipts; one HEAD + sorted SHA-256 manifest identity; identity-bound Oracle approval; identity-bound Reviewer approval; orchestrator-only semantic commit handoff; status `waiting for receipt` until formal plan review occurs.

- [ ] **Step 1: Run all static contracts and PowerShell AST checks**

```powershell
$scripts = @(
    "tests/multi-gateway.Tests.ps1",
    "tests/release-validation.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1"
)
foreach ($script in $scripts) {
    $tokens = $null
    $errors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile(
        (Resolve-Path -LiteralPath $script),
        [ref]$tokens,
        [ref]$errors
    )
    if ($errors.Count -ne 0) { throw "$script parse errors: $($errors.Message -join '; ')" }
    pwsh -NoProfile -File $script
    if ($LASTEXITCODE -ne 0) { throw "Static contract failed: $script" }
}
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Existing client-smoke infrastructure contract failed" }
```

Expected: every script AST parses and all four static commands PASS; workflow-embedded PowerShell AST is checked by the multi-gateway static test.

- [ ] **Step 2: Run complete non-Docker Rust and quality gates**

Task 5 already provides the only required live Docker and PostgreSQL receipts. With no relevant edits after that run:

```powershell
cargo test --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "Binary tests failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library tests failed" }
cargo test --test integration
if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway target compile failed" }
cargo check --all-targets
if ($LASTEXITCODE -ne 0) { throw "cargo check failed" }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw "cargo fmt check failed" }
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Clippy failed" }
```

Expected: all exit zero. If any source/workflow/live input changes, rerun the affected Task 5 live evidence before docs/review.

- [ ] **Step 3: Require clean LSP diagnostics on every changed Rust file**

Use `lsp_diagnostics` on `src/store/mod.rs`, `src/store/import/lease_clock.rs`, `tests/postgres_import.rs`, and `tests/multi_gateway.rs` with severity `all`. Expected: zero errors and zero warnings. Do not substitute a compile claim for this LSP receipt.

- [ ] **Step 4: Verify protected hashes, whitespace, exact path boundary, and clean index**

```powershell
$specSha = (Get-FileHash -Algorithm SHA256 -LiteralPath "docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md").Hash.ToLowerInvariant()
if ($specSha -cne "afce0b5ccdadd1839d8f50468f6c82c77a691c3553890b5bf58247512b95b7d4") { throw "Approved runtime-revised spec changed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "git diff --check failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not stage files" }
$allowed = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "deploy/nginx/multi-gateway.conf",
    "docker-compose.multi-gateway.yml",
    "docs/superpowers/plans/2026-08-24-multi-gateway-horizontal-scaling.md",
    "docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md",
    "src/store/import/lease_clock.rs",
    "src/store/mod.rs",
    "tests/compose.multi-gateway-validation.yml",
    "tests/multi-gateway.Tests.ps1",
    "tests/multi_gateway.rs",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/postgres_import.rs",
    "tests/release-validation.Tests.ps1"
)
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Tracked path query failed" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Untracked path query failed" }
$changed = @($tracked + $untracked | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
$unexpected = @($changed | Where-Object { $allowed -cnotcontains $_ })
if ($unexpected.Count -ne 0) { throw "Out-of-scope changed paths: $($unexpected -join ', ')" }
foreach ($required in $allowed) {
    if ($changed -cnotcontains $required) { throw "Final manifest path is absent from the change set: $required" }
}
git status --short
if ($LASTEXITCODE -ne 0) { throw "Git status query failed" }
```

Expected: exact protected spec hash; whitespace clean; no staged paths; all and only the 15 manifest paths changed/untracked. The default and single-PG Compose files, existing E2E, dependencies, state/main, and ownership semantics remain outside the diff.

- [ ] **Step 5: Freeze the identity-bound exact manifest**

```powershell
$head = (git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw "HEAD query failed" }
$manifest = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "deploy/nginx/multi-gateway.conf",
    "docker-compose.multi-gateway.yml",
    "docs/superpowers/plans/2026-08-24-multi-gateway-horizontal-scaling.md",
    "docs/superpowers/specs/2026-08-24-multi-gateway-horizontal-scaling-design.md",
    "src/store/import/lease_clock.rs",
    "src/store/mod.rs",
    "tests/compose.multi-gateway-validation.yml",
    "tests/multi-gateway.Tests.ps1",
    "tests/multi_gateway.rs",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/postgres_import.rs",
    "tests/release-validation.Tests.ps1"
)
$rows = @($manifest | Sort-Object | ForEach-Object {
    if (-not (Test-Path -LiteralPath $_ -PathType Leaf)) { throw "Manifest file missing: $_" }
    $sha = (Get-FileHash -Algorithm SHA256 -LiteralPath $_).Hash.ToLowerInvariant()
    "$sha  $_"
})
$identityText = "HEAD $head`n" + ($rows -join "`n") + "`n"
$identityBytes = [Text.Encoding]::UTF8.GetBytes($identityText)
$identitySha = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($identityBytes)).ToLowerInvariant()
Write-Host "REVIEW_IDENTITY_SHA256 $identitySha"
Write-Host $identityText
```

Any content or HEAD change invalidates both approvals and requires affected verification plus a new identity.

- [ ] **Step 6: Hand the same identity and evidence to Oracle and Reviewer**

The orchestrator, not an implementation subagent, dispatches one Oracle and one Reviewer. Each prompt includes: exact `REVIEW_IDENTITY_SHA256`, HEAD, all 15 SHA-256 rows, approved spec SHA, complete Task 5 LOCAL matrix, `HOSTED multi-gateway-deployment: NOT RUN`, all static/Rust/quality/LSP/boundary receipts, and the explicit non-goals. Each reviewer must state that its verdict applies to that exact identity. A timeout, partial response, review of a different identity, or approval before the final edit is not a receipt.

- [ ] **Step 7: Recheck identity, then orchestrator-only commit after both approvals**

After both identity-bound approvals, recompute Step 5 and require the same identity. Implementation subagents still perform no Git write. The user has authorized the orchestrator to stage exactly the 15 manifest paths and create one semantic commit only after the approvals; the intended message is `feat: add multi-gateway horizontal scaling` with a brief body describing the two-gateway Nginx/PostgreSQL/Kubo topology, migration lock, and PostgreSQL import-lease compatibility correction. Before committing, the orchestrator inspects `git status`, `git diff`, `git log --oneline -10`, the staged path set, and staged diff; it must not push, tag, amend, or include another path.

- [ ] **Step 8: Return final evidence without overstating hosted status**

Return task/step completion counts, exact manifest, spec/plan SHA-256, implementation review identity, both approval receipts, local matrix, static/Rust/LSP/diff results, pre/post-commit Git status boundary, and commit hash if the orchestrator committed. Keep `HOSTED multi-gateway-deployment: NOT RUN`; a later real hosted PASS after an authorized push/PR is required before merge/release and is outside this initial commit.

---

## Verification Waves and Acceptance Boundary

1. **Wave 1 — deployment static TDD:** missing files RED; exact five-service Compose, canonical Nginx equality plus independently extracted nested `http`/upstream/server/root/health/ready scopes, loopback override, and protected Compose hashes GREEN.
2. **Wave 2 — PostgreSQL startup and lease TDD:** two fresh-schema connections deterministically fail to wait before implementation; independently extracted public dispatcher proves non-PG direct return/no transaction, the private helper proves the exact bounded transaction-lock sequence and rejects session locks, and sanitized logs, eight markers once, real 60-second timeout rollback, and SQLite regression are GREEN. The live-discovered import-renewal literal-`?` builder test is RED before the focused structural value expression and GREEN with a numbered PostgreSQL bind.
3. **Wave 3 — live-target TDD:** absent target compile RED; existing rust-s3/SigV4 helper-based direct A/B, multipart, convergence, import, and post-failover tests compile GREEN without dependencies.
4. **Wave 4 — workflow TDD:** fifth-job/static-command contracts RED; independent blocking job, env isolation, ownership, ordering, logs-first cleanup, per-query exit capture, restoration-in-finally static guards, and embedded PowerShell AST GREEN while existing job meanings remain unchanged.
5. **Wave 5 — one from-scratch local live gate:** exact four-secret rejection, empty-PG concurrent A/B startup, markers 8/8 once, direct suite including the PostgreSQL lease-renewal surface toggle, E2E 11/11 via LB, full PG regression, import A→B, A stop/B readiness/LB ≤30s/new CRUD, diagnostics, cleanup, all residual queries, unconditional environment 15/15 restoration, then one aggregate zero-error decision.
6. **Wave 6 — evidence-gated docs:** README bounded declaration and only ROADMAP horizontal-scaling checkbox change after Wave 5; hosted remains NOT RUN.
7. **Wave 7 — final acceptance:** static, bin/lib/integration, compile, check, fmt, Clippy, diff, LSP, protected hashes, exact 15-path boundary, immutable identity, identity-bound Oracle+Reviewer, then orchestrator-only authorized commit.

Initial implementation acceptance requires every LOCAL line above to PASS. Docker unavailability, a skipped PostgreSQL test, inability to bind a fixed port, cleanup uncertainty, any primary/cleanup/restore aggregate error, environment restoration below 15/15, or absent LSP/review evidence is `UNVERIFIED` and blocks docs/review/commit. The hosted job's first status is never inferred from local evidence.

## Requirement-to-Task Coverage

| Requirement | Coverage |
| --- | --- |
| Exact PostgreSQL advisory key, 60s local timeout, outer transaction, function-scoped static proof, no production session lock/unbounded wait, sanitized logs, fail-closed, non-PG unchanged | Task 2 |
| PostgreSQL import renewal uses a numbered structural bind, literal question-mark RED is locked, SQLite/MySQL expressions stay unchanged, and real CID import completes | Task 2 Step 6; runtime Task 5 |
| Exact independent five-service production topology and loopback override | Task 1 |
| Canonical and nested-scope Nginx Host/HTTP1.1/streaming/unlimited body/passive failover/readiness/safe logs, with no comment/extra/conflicting proxy configuration | Task 1 |
| Direct cross-replica CRUD, multipart, same-key whole-payload convergence, CID import | Task 3; runtime Task 5 |
| Independent blocking hosted job, `.env` isolation, owned/attempted cleanup, existing jobs unchanged | Task 4 |
| Empty-PG startup, markers 8/8, direct suite, E2E 11/11, A-stop recovery/new CRUD, PG regressions, cleanup error collection, and unskippable 15/15 env restoration | Task 5 |
| README/ROADMAP only after all LOCAL PASS; no HA; hosted NOT RUN | Task 6 |
| Full quality/LSP/diff, exact spec+plan manifest, Oracle+Reviewer identity, orchestrator-only commit | Task 7 |
| Cluster/private swarm/key rotation/PG HA/provider limiter/worker role/pool/K8s/cloud/LB HA excluded | Global Constraints; Tasks 1, 6, 7 |

## Risks and Assumptions

- SeaORM/sea-orm-migration 1.1.20 accepts `&DatabaseTransaction` through its schema-manager connection conversion; PostgreSQL nested migration transactions use savepoints. The live timeout/rollback and exact-marker tests are the executable proof, not the documentation claim alone.
- The real lock-timeout test intentionally takes about 60 seconds because production timeout is fixed and no test-only runtime configuration is introduced.
- Compose merges `ports` by a uniqueness key. Hosted/local validation forces the base LB interpolation to `127.0.0.1:59000:9000`, matching the override exactly; the static and rendered `config --quiet` contracts prevent a second LB publication.
- Nginx open-source passive failure handling is not an active health checker. `max_fails=1`, `fail_timeout=10s`, `proxy_next_upstream_tries 2`, and the ≤30-second real recovery test establish only the bounded supported behavior.
- Nginx static validation intentionally combines exact canonical-file equality with comment/string-aware nested-brace extraction. Any comment, extra upstream/server/location, conflicting Host, buffering-on directive, or proxy directive outside the six reviewed scopes fails before live validation.
- A concurrent same-key writer may receive the existing fenced 409 conflict. Acceptance requires no 5xx, at least one successful complete payload, and final A/B/Kubo convergence to one whole submitted payload.
- Both normal workers run in each gateway by design. Durable import ownership and pinning tokens remain the only worker coordination; no cluster-level provider scheduling promise is introduced.
- The first five-service run exposed a SeaQuery custom-expression placeholder that produced literal PostgreSQL `?` syntax. Builder RED→GREEN and the repeated real CID import are both mandatory; neither proof substitutes for the other.
- Fixed ports can be occupied by unrelated local processes. Preflight refuses without killing processes or cleaning resources it does not own.
- The local live gate records primary and Docker cleanup errors instead of throwing from cleanup, then uses an outer `finally` to attempt all 15 exact environment restorations. A `down`/query failure can therefore neither skip restoration nor be mistaken for a zero-residual receipt.
- The runtime-revised approved spec and this plan are untracked at planning time and are intentionally included in the final 15-path identity/commit. Implementation agents may not alter their semantics.
- Context7 documentation lookup was unavailable due quota during planning; repository evidence and official SeaORM/NGINX/Docker documentation contracts were cross-checked without adding dependencies. Runtime/static proofs remain authoritative.

## Plan Self-Review

- Spec coverage: the original design plus the runtime-driven PostgreSQL lease revision map to Tasks 1-7 and the coverage table; no unsupported subsystem was added.
- Placeholder scan: no unresolved marker or abbreviated implementation instruction is present; every task has exact files, interfaces, RED/GREEN evidence, and completion boundary.
- Type/name consistency: migration keys, environment names, service names, endpoints, ports, test names, job name, marker list, manifest, and review identity are consistent across tasks.
- Runtime-revision consistency: `src/store/import/lease_clock.rs`, the numbered-bind unit contract, Task 5 CID import toggle, LSP list, and 15-path manifest are aligned.
- Static-scope consistency: Nginx assertions operate on exact extracted blocks, and migration assertions operate separately on the public dispatcher and private PostgreSQL helper; declaration order cannot invalidate GREEN or hide non-PG transaction use.
- Safety: every live Docker path is unique-project, all-state/port preflighted, owned+attempted guarded, logs-first, all cleanup/query errors collected, outer-finally exact-restoring 15/15, and residual-checked; implementation workers perform no Git write.
- Documentation truthfulness: README/ROADMAP are local-evidence gated; hosted remains `NOT RUN`; no full-stack HA, provider-cluster, Cluster, swarm, rotation, or cloud claim exists.

## Plan Review Status

- Receipt: `waiting for receipt`
- Review owner: orchestrator
- Any edit to this plan invalidates a later receipt and requires review of the complete current revision.
