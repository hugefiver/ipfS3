# IPFS Cluster Pinset Replication Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved v0.5-C same-host six-service IPFS Cluster profile and prove, without application-source changes, that the gateway's existing Kubo add/pin/cat protocol produces a retained CID with exactly two Cluster allocations and two physical pins that survives S3 deletion and peer-B stop/restart.

**Architecture:** Add an opt-in same-host Compose topology containing PostgreSQL, two exact Kubo v0.43.0 daemons, two full IPFS Cluster v1.1.6 CRDT peers, and one unchanged gateway whose sole Kubo RPC URL is Cluster A's internal proxy. Each Cluster peer's connector and proxy forwarder target the same paired Kubo DNS endpoint, while the gateway's only production host publication is intrinsically fixed to loopback and a pre-binary container guard verifies the required loopback acknowledgement in depth. A dependency-free PowerShell contract freezes those boundaries; split Rust live validation owns protocol/replication evidence while outer PowerShell alone owns Docker lifecycle, diagnostics, cleanup, and environment restoration.

**Tech Stack:** Rust 2024 (MSRV 1.92), Tokio 1, reqwest 0.13, serde/serde_json, anyhow, rust-s3 0.37, IPFS Cluster v1.1.6, Kubo v0.43.0, PostgreSQL 17, Docker Compose v2.23.1+, PowerShell 7, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-24-ipfs-cluster-pinset-replication-design.md` (runtime-revised approved SHA256 `56d0d5518ff05f59cacec7f3740a6607c6505e7613086a89545650dfde9202eb`)

**Global Constraints:**
- The supported topology is same-host Docker Compose with exactly `postgres`, `kubo-a`, `kubo-b`, `cluster-a`, `cluster-b`, and `gateway`; it is not a multi-host or high-availability claim.
- Cluster uses `ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd`; both Kubo images derive from exact `ipfs/kubo:v0.43.0` through the new profile-specific Dockerfile.
- Cluster `/peers.version` must have exact release core `1.1.6` and may carry only optional valid SemVer build metadata (`1.1.6+identifier[.identifier...]`). Pre-release/other-core/malformed suffixes are terminal; all runtime evidence normalizes to `version=1.1.6` and never emits metadata.
- Both full Cluster peers use CRDT, `replication_factor_min=2`, `replication_factor_max=2`, distinct Kubo connectors, one shared 32-byte hex membership secret, and separate persistent Cluster identities; Kubo repositories are also separate persistent volumes. On each peer, `CLUSTER_IPFSHTTP_NODEMULTIADDRESS` and `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` must both equal that peer's `/dns4/kubo-{a|b}/tcp/5001` target; container-local `/ip4/127.0.0.1/tcp/5001` and cross-wiring are forbidden.
- The gateway keeps one Kubo client and points only to `http://cluster-a:9095`; the exact existing add with `pin=false`, pin/add, and forwarded cat interfaces must work unchanged.
- The runtime proxy correction adds only the two peer-local `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` environment entries. It adds no application change, file, service, volume, port, fallback, or alternate proxy path and preserves both connector and listen settings.
- Only failure of the direct validation-only Cluster A proxy test through public production `KuboClient` + `stream_add`→`pin_add`→`stream_cat` is a fail-fast proxy design blocker. The test must not hand-build multipart bodies, URLs, or response parsers; stop implementation, preserve failure/log evidence, and revise the design rather than relabel S3/PostgreSQL failures or add fallback/application compatibility code.
- No application source, `Cargo.toml`, or `Cargo.lock` change is allowed. Any apparent need to change one stops execution and requires a revised approved spec and plan.
- Production publishes only fixed loopback `127.0.0.1:${IPFS_S3_GATEWAY_PORT}:9000`; no arbitrary host bind drives the mapping. Required `IPFS_S3_GATEWAY_BIND` is an operator/CI acknowledgement that must equal case-sensitive exact `127.0.0.1`, is passed to the gateway container as `IPFS_S3_PUBLISHED_BIND`, and is checked by a no-output `/bin/sh -ec` guard before `exec /app/ipfs-s3-gateway`. The fixed mapping—not the later container guard—prevents wildcard publication.
- Validation publishes only loopback gateway `59100`, PostgreSQL `55435`, Kubo A/B `55100`/`55101`, Cluster REST A/B `59101`/`59102`, and Cluster A proxy `59103`; Cluster B proxy and every production proxy remain internal.
- `POSTGRES_PASSWORD`, `IPFS_S3_ACCESS_KEY_ID`, `IPFS_S3_SECRET_ACCESS_KEY`, `IPFS_S3_MASTER_KEY`, `IPFS_S3_CLUSTER_SECRET`, `IPFS_S3_GATEWAY_BIND`, and `IPFS_S3_GATEWAY_PORT` use `${VAR:?message}` with no tracked fallback. The bind acknowledgement is exactly `127.0.0.1`; the master key is nonzero 64-hex and the Cluster secret is exactly 64-hex.
- Direct non-loopback publication by this profile is unsupported. External access requires a separately secured TLS/auth reverse proxy that connects to the fixed loopback endpoint; designing or adding that subsystem is outside scope.
- No identity, peerstore, secret, `.env` content, authorization header, request body, or generated peer identity is tracked or emitted. Rust errors expose only counts/categories; every Compose diagnostic is captured, sanitized in memory, and only then emitted. `COMPOSE_DISABLE_ENV_FILE="1"` is mandatory for validation; no step reads, changes, deletes, or outputs `.env`.
- Same-host Compose mDNS is bounded behavior only. A separate no-write topology gate must prove that both REST views agree on exactly two distinct peer IDs and no third peer before proxy compatibility; IDs are compared only in memory and are never emitted, tracked, or printed.
- Successful mutation acceptance is not replication evidence. Acceptance requires exactly two allocations and tracker status `pinned` for both allocated peers within a bounded interval.
- S3 deletion removes metadata only. No Cluster unpin or production Kubo `pin/rm` is added; allocation and Kubo-B bytes must remain.
- Peer-B stop/restart is orchestrated outside Rust. The stopped state must cease to demonstrate two healthy physical pins without asserting an undocumented status string; restart reuses the same volumes and recovers without upload.
- Static tests are dependency-free and never start Docker. Live work uses a unique project, all-state project-label and fixed-port preflight, independent project/receipt ownership flags, logs before stop and cleanup, owned `down --volumes --remove-orphans`, sanitized diagnostics, independent residual-query exits, and zero residual resources. A pre-existing receipt is BLOCKED and untouched; cleanup may remove only a receipt claimed with fail-closed CreateNew after project ownership.
- Preserve exact prior presence and case-sensitive value for every touched process environment variable. Prior absence is restored with `Remove-Item -LiteralPath "Env:$name"`, never a null assignment.
- Do not install or explicitly pull anything. A direct declared Compose build/up may resolve images normally; missing runtime/image/network evidence is `UNVERIFIED`, never PASS.
- Existing default SQLite, single-PostgreSQL, and multi-gateway deployment profiles and all five current release-job commands/semantics remain unchanged except the shared job count and static-command list.
- README/ROADMAP edits are blocked until the complete local static/live/regression matrix passes. Initial hosted status is exactly `HOSTED cluster-pinset-replication: NOT RUN`; only the Cluster ROADMAP checkbox changes and Private swarm remains unchecked.
- Use PowerShell syntax for every command and new workflow run block. Do not use Bash assignment, `export`, `source`, `&&`, `/dev/null`, heredocs, or broad Docker cleanup.
- A fresh implementation subagent owns each task. No task-level stage or commit is allowed; one integrated commit is permitted only after the current exact working-tree identity has both Oracle and Reviewer receipts, unless the user explicitly overrides a disclosed review-system blocker. No push or tag is authorized.

---

## Exact Release Workflow Reference

This reference is placed before the task list so Task 3 can cite one canonical set of complete workflow/static-contract blocks without duplicating security-sensitive PowerShell. Task 3 remains dependency-ordered after Tasks 1-2 and has its own checkbox actions.

**Files:**
- Modify/Test: `.github/workflows/release-validation.yml`
- Modify/Test: `tests/release-validation.Tests.ps1`
- Modify/Test: `tests/postgres-production-baseline.Tests.ps1` (shared count/order only)
- Modify/Test: `tests/multi-gateway.Tests.ps1` (shared count/order only)
- Modify/Test: `tests/cluster.Tests.ps1`
- Verify unchanged: exact command/step semantics of `postgres-import`, `postgres-production-deployment`, `multi-gateway-deployment`, `e2e`, and existing client-smoke command

**Interfaces:**
- Consumes: Task 1 Compose pair with paired Cluster targets, fixed loopback gateway mapping, and exact bind-acknowledgement guard; Task 2 five Tokio live tests plus one plain version unit; strict Compose `>=2.23.1` parser; GitHub run identity; exact project/port/receipt ownership, environment restoration, diagnostics, and residual-query patterns.
- Produces: independent blocking job `cluster-pinset-replication`; unique project; exact case-sensitive `IPFS_S3_GATEWAY_BIND=127.0.0.1` workflow validation; exact test endpoints and receipt; topology GREEN then proxy GREEN before replication; static order release → PG → multi → cluster → client; initial hosted `NOT RUN`.

#### Reference 1: Shared static expectations and causal RED

In `tests/release-validation.Tests.ps1`, change the exact root job count from five to six, require `cluster-pinset-replication`, extract its block, and change client infrastructure from four to five commands. In `tests/postgres-production-baseline.Tests.ps1` and `tests/multi-gateway.Tests.ps1`, change only those same shared count/name/command-list assertions. The exact new command is inserted after multi-gateway and before client-smoke:

```yaml
      - name: Test Cluster pinset replication contract
        run: pwsh -NoProfile -File tests/cluster.Tests.ps1
```

Extend `tests/cluster.Tests.ps1` to expect the absent job and command, then run all four contracts:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
$releaseRed = $LASTEXITCODE
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
$postgresRed = $LASTEXITCODE
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
$multiRed = $LASTEXITCODE
pwsh -NoProfile -File tests/cluster.Tests.ps1
$clusterRed = $LASTEXITCODE
if ($releaseRed -eq 0 -or $postgresRed -eq 0 -or $multiRed -eq 0 -or $clusterRed -eq 0) {
    throw "All shared workflow contracts must be RED before the sixth job and fifth static command exist"
}
```

Expected: failures point to the absent sixth job/new static command. Do not weaken or delete an existing job-body assertion to obtain RED.

#### Reference 2: Exact job identity, environment, and Rust setup

Insert the new peer job without `needs` or `continue-on-error`:

```yaml
  cluster-pinset-replication:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    env:
      COMPOSE_DISABLE_ENV_FILE: "1"
      COMPOSE_PROJECT_NAME: ipfs3-cl-${{ github.run_id }}-${{ github.run_attempt }}
      POSTGRES_PASSWORD: cl-${{ github.run_id }}-${{ github.run_attempt }}
      IPFS_S3_ACCESS_KEY_ID: test
      IPFS_S3_SECRET_ACCESS_KEY: test
      IPFS_S3_MASTER_KEY: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
      IPFS_S3_CLUSTER_SECRET: abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
      IPFS_S3_GATEWAY_BIND: 127.0.0.1
      IPFS_S3_GATEWAY_PORT: 59100
      IPFS_S3_CLUSTER_GATEWAY_ENDPOINT: http://127.0.0.1:59100
      IPFS_S3_CLUSTER_A_REST_URL: http://127.0.0.1:59101
      IPFS_S3_CLUSTER_B_REST_URL: http://127.0.0.1:59102
      IPFS_S3_CLUSTER_A_PROXY_URL: http://127.0.0.1:59103
      IPFS_S3_CLUSTER_KUBO_A_URL: http://127.0.0.1:55100
      IPFS_S3_CLUSTER_KUBO_B_URL: http://127.0.0.1:55101
      IPFS_S3_CLUSTER_STATE_PATH: ${{ runner.temp }}/ipfs3-cluster-${{ github.run_id }}-${{ github.run_attempt }}.json
    steps:
      - uses: actions/checkout@v7

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@v1
        with:
          toolchain: "1.92"

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2
```

The endpoint credentials match Task 2's fixed `test` credentials. The state path is runner-temp, unique per attempt, and is never under the repository. The workflow must claim it with CreateNew before any Rust test may consume/update it.

#### Reference 3: Strict Compose and seven-variable fail-closed configuration checks

Add explicit `shell: pwsh` literal blocks. The Compose prerequisite is exact:

```powershell
docker compose version
if ($LASTEXITCODE -ne 0) { throw "Docker Compose v2 is unavailable" }
$composeVersionText = (docker compose version --short).Trim()
$composeVersionMatch = [regex]::Match($composeVersionText, '^v?(?<core>\d+\.\d+\.\d+)(?:[-+][0-9A-Za-z.-]+)?$')
$composeVersion = $null
if (-not $composeVersionMatch.Success -or
    -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or
    $composeVersion -lt [Version]"2.23.1") {
    throw "Docker Compose 2.23.1 or newer is required for inline configs.content"
}
```

The following block is the environment/configuration step. It never renders Compose config or a secret value:

```powershell
$compose = @(
    "--project-name", $env:COMPOSE_PROJECT_NAME,
    "-f", "docker-compose.cluster.yml",
    "-f", "tests/compose.cluster-validation.yml"
)
$requiredNames = @(
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_CLUSTER_SECRET",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT"
)
$savedRequiredValues = @{}
foreach ($requiredName in $requiredNames) {
    if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required job variable is absent: $requiredName" }
    $savedRequiredValues[$requiredName] = [Environment]::GetEnvironmentVariable($requiredName, "Process")
}
if ($env:POSTGRES_PASSWORD -notmatch '^[A-Za-z0-9._~-]+$') { throw "POSTGRES_PASSWORD must be URL-safe unreserved text" }
if ([string]::IsNullOrWhiteSpace($env:IPFS_S3_ACCESS_KEY_ID)) { throw "S3 access key must be nonempty" }
if ([string]::IsNullOrWhiteSpace($env:IPFS_S3_SECRET_ACCESS_KEY)) { throw "S3 secret key must be nonempty" }
if ($env:IPFS_S3_MASTER_KEY -notmatch '^[0-9A-Fa-f]{64}$' -or $env:IPFS_S3_MASTER_KEY -match '^0{64}$') { throw "Master key must be nonzero 64-hex" }
if ($env:IPFS_S3_CLUSTER_SECRET -notmatch '^[0-9A-Fa-f]{64}$') { throw "Cluster secret must be exactly 64-hex" }
if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Gateway bind acknowledgement must equal exact loopback 127.0.0.1" }
$gatewayPort = 0
if (-not [int]::TryParse($env:IPFS_S3_GATEWAY_PORT, [ref]$gatewayPort) -or $gatewayPort -ne 59100) { throw "Validation gateway port must be exactly 59100" }
foreach ($name in $requiredNames) {
    try {
        Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
        if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal retained presence: $name" }
        docker compose @compose config --quiet
        $missingExit = $LASTEXITCODE
    } finally {
        foreach ($requiredName in $savedRequiredValues.Keys) {
            [Environment]::SetEnvironmentVariable($requiredName, $savedRequiredValues[$requiredName], "Process")
            if (-not (Test-Path -LiteralPath "Env:$requiredName")) { throw "Required variable restoration lost presence: $requiredName" }
            if ([Environment]::GetEnvironmentVariable($requiredName, "Process") -cne $savedRequiredValues[$requiredName]) {
                throw "Required variable restoration changed value: $requiredName"
            }
        }
    }
    if ($missingExit -eq 0) { throw "Compose accepted missing required variable: $name" }
}
docker compose @compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "Complete Cluster Compose configuration failed" }
```

#### Reference 4: Independent project/port/receipt ownership and exact six-service startup

The ownership step independently captures each Docker query exit before inspecting results, probes exactly `55435,55100,55101,59100,59101,59102,59103`, and appends `CLUSTER_PINSET_OWNED=true` only after those checks pass. It then treats a pre-existing state path as BLOCKED, claims a new empty receipt with `FileMode.CreateNew`, and appends `CLUSTER_STATE_RECEIPT_OWNED=true` only after CreateNew succeeds. A race that creates the path between `Test-Path` and `CreateNew` fails closed and never grants receipt ownership. Immediately before `up`, append `CLUSTER_PINSET_ATTEMPTED=true` and run:

```powershell
$project = $env:COMPOSE_PROJECT_NAME
$containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$containerExit = $LASTEXITCODE
$networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$networkExit = $LASTEXITCODE
$volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$volumeExit = $LASTEXITCODE
if ($containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0) { throw "Project ownership preflight query failed" }
if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "Unique project already owns resources: $project" }
foreach ($port in @(55435, 55100, 55101, 59100, 59101, 59102, 59103)) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
    try { $listener.Start() } catch { throw "Fixed validation port is occupied: $port" } finally { $listener.Stop() }
}
if (Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH) { throw "Cluster state receipt already exists" }
"CLUSTER_PINSET_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
$stateReceiptOwned = $false
$receipt = $null
try {
    $receipt = [IO.File]::Open(
        $env:IPFS_S3_CLUSTER_STATE_PATH,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    $receipt.Dispose()
    $stateReceiptOwned = $true
} finally {
    if ($null -ne $receipt) { $receipt.Dispose() }
}
if (-not $stateReceiptOwned) { throw "State receipt claim did not complete" }
"CLUSTER_STATE_RECEIPT_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

```powershell
"CLUSTER_PINSET_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b cluster-a cluster-b gateway
if ($LASTEXITCODE -ne 0) { throw "Cluster topology did not become healthy" }
```

No `pull` step is added. Compose may resolve only the images declared by the topology through normal `up --build` behavior.

#### Canonical sanitized diagnostics block

Every workflow diagnostics step and the local parity process uses this exact redactor. Workflow steps prepend both functions because PowerShell functions do not persist between GitHub Actions steps. The native output is captured with stderr and `$LASTEXITCODE` is saved immediately; only transformed lines are emitted. The broad `12D3Koo...` matcher is safe because that prefix identifies Ed25519 peer IDs. CIDv0-shaped `Qm...` text is replaced only on a peer/identity-labeled line or inside `/p2p/`, so ordinary content CIDs remain visible.

```powershell
function Protect-ClusterDiagnosticLine {
    param([AllowEmptyString()][string]$Line)
    $safe = $Line
    $safe = [regex]::Replace(
        $safe,
        '(?i)((?:"?)(?:PeerID|peer_id|peer-id)(?:"?)\s*[:=]\s*)(?:"[^"]*"|''[^'']*''|[^\s,;]+)',
        '$1[REDACTED_PEER_ID]'
    )
    $safe = [regex]::Replace(
        $safe,
        '(?i)(/p2p/)[1-9A-HJ-NP-Za-km-z]+',
        '$1[REDACTED_PEER_ID]'
    )
    $safe = [regex]::Replace(
        $safe,
        '(?<![1-9A-HJ-NP-Za-km-z])12D3Koo[1-9A-HJ-NP-Za-km-z]{20,}(?![1-9A-HJ-NP-Za-km-z])',
        '[REDACTED_PEER_ID]'
    )
    if ($safe -match '(?i)\b(?:peer(?:[_-]?id)?|peerid|identity|peerstore)\b') {
        $safe = [regex]::Replace(
            $safe,
            '(?<![1-9A-HJ-NP-Za-km-z])Qm[1-9A-HJ-NP-Za-km-z]{44}(?![1-9A-HJ-NP-Za-km-z])',
            '[REDACTED_PEER_ID]'
        )
    }
    $safe
}

function Write-SanitizedClusterDiagnostics {
    param(
        [Parameter(Mandatory)][string[]]$ComposeArgs,
        [Parameter(Mandatory)][string]$FailureMessage
    )
    $rawDiagnosticLines = @(
        docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway 2>&1
    )
    $diagnosticExit = $LASTEXITCODE
    foreach ($rawLine in $rawDiagnosticLines) {
        [Console]::Out.WriteLine((Protect-ClusterDiagnosticLine -Line "$rawLine"))
    }
    if ($diagnosticExit -ne 0) { throw "$FailureMessage (exit=$diagnosticExit)" }
}
```

Static RED fixtures must feed each required form (`PeerID:`, `peer_id=`, `peer-id=`, `/p2p/12D3Koo...`, bare `12D3Koo...`, peer-labeled bare `Qm...`) and assert no fixture identity survives, while a non-peer content CID line remains byte-identical.

#### Reference 5: Fail-fast compatibility, replication, stop/outage, restart/recovery sequence

Add the following exact named order. Every cargo command is an explicit `shell: pwsh` literal block with an immediate exit check. `cluster_topology_converges` is the first no-write runtime gate and owns topology/convergence classification. Only after it passes may `cluster_proxy_compatibility` use `IPFS_S3_CLUSTER_A_PROXY_URL` and the production public Kubo client/functions under an outer timeout to exercise add(`pin=false`) → pin/add → cat and use `PROXY_COMPATIBILITY_BLOCKER`. Record the confirmed pre-fix RED as `add=200 pin=200 cat=502` with proxy forwarding to container-local `127.0.0.1:5001`; only a complete rerun may set the proxy GREEN marker, and S3/PostgreSQL work remains blocked until that marker exists.

```yaml
      - name: Prove Cluster release-version representation contract
        shell: pwsh
        run: |
          cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact
          if ($LASTEXITCODE -ne 0) { throw "Cluster release-version unit contract failed" }

      - name: Prove exact two-peer topology without writes
        shell: pwsh
        run: |
          cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "TOPOLOGY_CONVERGENCE_BLOCKER: exact 1.1.6 release-core two-peer topology did not converge" }
          "CLUSTER_TOPOLOGY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV

      - name: Prove direct Kubo wire compatibility against Cluster A proxy
        shell: pwsh
        run: |
          if ($env:CLUSTER_TOPOLOGY_GREEN -ne "true") { throw "Topology GREEN receipt is required before compatibility" }
          cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "PROXY_COMPATIBILITY_BLOCKER: stop and revise the approved design; do not add app fallback code" }
          "CLUSTER_PROXY_COMPATIBILITY_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV

      - name: Prove replication and retained deletion
        shell: pwsh
        run: |
          if ($env:CLUSTER_PROXY_COMPATIBILITY_GREEN -ne "true") { throw "Add-pin-cat proxy GREEN receipt is required before replication" }
          if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before replication" }
          cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "Cluster replication and retention contract failed" }

      - name: Capture diagnostics before peer B stop
        shell: pwsh
        run: |
          $compose = @("--project-name", $env:COMPOSE_PROJECT_NAME, "-f", "docker-compose.cluster.yml", "-f", "tests/compose.cluster-validation.yml")
          Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Pre-stop Cluster diagnostics failed"

      - name: Stop Cluster and Kubo peer B
        shell: pwsh
        run: |
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml stop cluster-b kubo-b
          if ($LASTEXITCODE -ne 0) { throw "Peer B stop failed" }

      - name: Prove stopped peer loses two-pin evidence
        shell: pwsh
        run: |
          if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before outage validation" }
          cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "Peer B outage contract failed" }

      - name: Capture stopped-peer diagnostics before restart
        shell: pwsh
        run: |
          $compose = @("--project-name", $env:COMPOSE_PROJECT_NAME, "-f", "docker-compose.cluster.yml", "-f", "tests/compose.cluster-validation.yml")
          Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Stopped-peer diagnostics failed"

      - name: Restart Cluster and Kubo peer B with existing volumes
        shell: pwsh
        run: |
          docker compose --project-name $env:COMPOSE_PROJECT_NAME -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml start kubo-b cluster-b
          if ($LASTEXITCODE -ne 0) { throw "Peer B restart failed" }

      - name: Prove same-volume peer B recovery
        shell: pwsh
        run: |
          if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before recovery validation" }
          cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "Peer B same-volume recovery contract failed" }
```

Prepend the canonical redactor/functions to both diagnostics run bodies shown above. The topology gate performs no mutation and emits only `peers=2 version=1.1.6`; terminal topology errors are never relabeled as proxy incompatibility. The direct proxy test reuses production add/pin/cat code, has no hand-built protocol or S3 bucket/object/database operation, emits no CID/body, and leaves its pin for disposable-volume teardown. If topology fails, compatibility and every later live scenario are skipped. If add, pin, or cat fails or reaches its outer deadline, no proxy GREEN marker is written, `PROXY_COMPATIBILITY_BLOCKER` remains valid, and replication/outage/recovery stay skipped. The confirmed `add=200 pin=200 cat=502` run is RED, not partial compatibility; only a complete add→pin→cat PASS writes `CLUSTER_PROXY_COMPATIBILITY_GREEN=true` and permits replication.

#### Reference 6: Always-on logs-first owned cleanup and independent residual exits

Add final diagnostics with `if: ${{ always() && env.CLUSTER_PINSET_ATTEMPTED == 'true' }}`. Prepend the canonical redactor/functions, then call them exactly as below so native output is captured before sanitized emission and a nonzero log exit still fails the step:

```powershell
$compose = @("--project-name", $env:COMPOSE_PROJECT_NAME, "-f", "docker-compose.cluster.yml", "-f", "tests/compose.cluster-validation.yml")
Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Final Cluster diagnostics failed"
```

Place cleanup after diagnostics with `if: ${{ always() }}`. Docker cleanup is gated by project OWNED+ATTEMPTED; receipt cleanup has its own `CLUSTER_STATE_RECEIPT_OWNED` gate and therefore still runs if CreateNew succeeded but startup never began. Cleanup uses exactly the project and Compose pair, captures `down`, container, network, volume, and owned state-file cleanup outcomes before throwing once:

```powershell
if (($env:CLUSTER_PINSET_OWNED -ne "true" -or $env:CLUSTER_PINSET_ATTEMPTED -ne "true") -and
    $env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") {
    Write-Host "No owned Cluster topology or receipt requires cleanup"
    exit 0
}
$project = $env:COMPOSE_PROJECT_NAME
$errors = [Collections.Generic.List[string]]::new()
if ($env:CLUSTER_PINSET_OWNED -eq "true" -and $env:CLUSTER_PINSET_ATTEMPTED -eq "true") {
    docker compose --project-name $project -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml down --volumes --remove-orphans
    $downExit = $LASTEXITCODE
    if ($downExit -ne 0) { $errors.Add("down exited $downExit") }
    $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $containerExit = $LASTEXITCODE
    $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $networkExit = $LASTEXITCODE
    $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $volumeExit = $LASTEXITCODE
    if ($containerExit -ne 0) { $errors.Add("container residual query exited $containerExit") }
    if ($networkExit -ne 0) { $errors.Add("network residual query exited $networkExit") }
    if ($volumeExit -ne 0) { $errors.Add("volume residual query exited $volumeExit") }
    if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { $errors.Add("project-labelled residual resources remain") }
}
if ($env:CLUSTER_STATE_RECEIPT_OWNED -eq "true") {
    try {
        if (-not (Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -PathType Leaf)) {
            throw "owned state receipt is missing before cleanup"
        }
        Remove-Item -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH -ErrorAction Stop
        if (Test-Path -LiteralPath $env:IPFS_S3_CLUSTER_STATE_PATH) { throw "state receipt remains" }
    } catch {
        $errors.Add("owned state receipt cleanup failed: $($_.Exception.Message)")
    }
}
if ($errors.Count -ne 0) { throw ($errors -join "; ") }
```

No cleanup branch tests or removes `$env:IPFS_S3_CLUSTER_STATE_PATH` without the receipt-owned marker. Thus a pre-existing or raced-in unowned path remains untouched and the job is BLOCKED rather than “cleaned.”

#### Reference 7: Complete new-job and embedded-PowerShell static contract

In both `tests/release-validation.Tests.ps1` and `tests/cluster.Tests.ps1`, lock: six exact independent/blocking jobs; 60-minute timeout; job bind acknowledgement exactly `127.0.0.1` and case-sensitive `-cne` rejection of every mismatch; exact environment lines including proxy URL; seven missing-variable probes; strict `2.23.1` parser; seven fixed ports; Task 1's fixed mapping/guard/no-output contract; project OWNED before receipt CreateNew, receipt-owned after CreateNew, ATTEMPTED before `up`; exact six-service `up`; exactly six Rust commands in causal order; topology GREEN before compatibility and proxy GREEN before replication; sanitized logs before stop/restart/cleanup; same-volume restart; project-scoped down; independently captured Docker exits and receipt cleanup; and absence of arbitrary/non-loopback host mapping, `${IPFS_S3_GATEWAY_BIND}` in the ports block, `.env`, explicit pulls, native clients, broad prune, null environment removal, app-source steps, identity-bearing output/state, or unsafe shell syntax.

For every diagnostics body, require `$rawDiagnosticLines` capture, immediate `$diagnosticExit = $LASTEXITCODE`, both canonical function definitions, sanitized-only console emission, and a post-emission nonzero-exit throw. Separately invoke `Protect-ClusterDiagnosticLine` with every fixture listed under the canonical block and require all peer forms redacted while the content-CID fixture is unchanged. Reject a command statement whose pipeline begins `docker compose ... logs`, any direct `docker compose ... logs` not assigned to `$rawDiagnosticLines`, `ipfs-cluster-ctl peers ls`, raw response-body formatting, `peer_ids` in `RecoveryState`, and `unwrap_or_default()` in topology/status polling.

Extract every `shell: pwsh` + `run: |` body from the Cluster job, remove ten spaces of YAML indentation, parse via `[System.Management.Automation.Language.Parser]::ParseInput`, and reject `export`, `source`, `&&`, and `/dev/null`. Assert the exact number of extracted blocks derived from the final job rather than silently skipping a block.

#### Reference 8: Five static commands in production order

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release workflow contract failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Single-PostgreSQL shared contract regressed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway shared contract regressed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster workflow contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client-smoke infrastructure contract regressed" }
```

Expected: five PASS lines in release → PG → multi → cluster → client order. Existing job commands and deployment-profile hashes remain unchanged.

#### Reference 9: Task 3 receipt boundary

Record `Task 3 WORKFLOW STATIC: PASS`, `jobs=6 independent/blocking`, `client static commands=5 exact order`, all embedded PowerShell blocks parsed, and `HOSTED cluster-pinset-replication: NOT RUN`. Do not stage, commit, push, or tag.

---

## File Map

### Create

- `docker-compose.cluster.yml` — exact six-service production profile, five separate data volumes, peer-local connector/proxy forwarding, sole fixed-loopback gateway publication, exact bind-acknowledgement pre-binary guard, internal Cluster/Kubo surfaces, and inline PostgreSQL initializer.
- `ipfs/cluster.Dockerfile` — profile-specific Kubo image pinned to v0.43.0 while reusing the unchanged repository entrypoint.
- `tests/compose.cluster-validation.yml` — seven exact loopback-only validation mappings, including Cluster A proxy `59103:9095`; no Cluster B proxy publication or volumes.
- `tests/cluster.Tests.ps1` — dependency-free topology including paired Cluster targets plus fixed loopback publication, exact bind acknowledgement/guard/order/no-output contract, protected hashes, Rust/workflow/docs/PowerShell-AST, and boundary contract.
- `tests/cluster.rs` — no-write topology, direct Cluster A compatibility through the production public Kubo add/pin/cat functions under an outer timeout, separate S3 replication/retention, outage, and restart/recovery scenarios; it never executes Docker or prints identities/CIDs.
- `tests/support/cluster.rs` — focused v1.1.6 REST types, strict release-core/optional-build-metadata helper plus pure unit contract, NDJSON parsing, bounded HTTP/polling, Kubo cat helper, and recovery-state receipt. This split is required because protocol parsing/polling and scenario orchestration are independently reviewable responsibilities and would otherwise push `tests/cluster.rs` well beyond the repository's focused-file guideline.
- `docs/superpowers/plans/2026-08-24-ipfs-cluster-pinset-replication.md` — this execution plan and an exact final-manifest member.

### Modify

- `.github/workflows/release-validation.yml` — add independent blocking `cluster-pinset-replication` and insert its static command between multi-gateway and client-smoke.
- `tests/release-validation.Tests.ps1` — six-job contract, exact Cluster job contract, and five-command infrastructure order while preserving every existing job assertion.
- `tests/postgres-production-baseline.Tests.ps1` — only shared five-to-six job and four-to-five infrastructure-command expectations.
- `tests/multi-gateway.Tests.ps1` — only shared five-to-six job and four-to-five infrastructure-command expectations; existing multi-gateway job/body assertions remain unchanged.
- `README.md` — evidence-gated same-host operator contract, fixed loopback publication, external TLS/auth reverse-proxy boundary, paired Cluster forwarding, validation gates, retained pins, and non-destructive shutdown.
- `ROADMAP.md` — only `IPFS Cluster for pinset replication` changes from unchecked to checked.

### Protected / Verify Unchanged

- `docs/superpowers/specs/2026-08-24-ipfs-cluster-pinset-replication-design.md`
- `docker-compose.yml`, `docker-compose.postgres.yml`, `docker-compose.multi-gateway.yml`, and their existing validation overrides
- `ipfs/Dockerfile`, `ipfs/entrypoint.sh`, root `Dockerfile`, `config.docker.toml`, `Cargo.toml`, and `Cargo.lock`
- `src/kubo/add.rs`, `src/kubo/pin.rs`, `src/kubo/cat.rs`, `src/state.rs`, `src/s3/ops/object.rs`, `src/s3/ops/multipart.rs`, and every other application source
- Existing five workflow job bodies and commands, except the new static command in `client-smoke-infrastructure`
- `.env`, `.env.example`, provider configuration, private-swarm configuration, Kubernetes/cloud files, and unrelated specs/plans

## Execution Protocol

- Dispatch one fresh implementation subagent for each Task 1-6; never reuse a worker across tasks.
- After every returned task, the orchestrator checks the task's exact files, interfaces, RED/GREEN evidence, protected boundary, and no-staged-files state before dispatching the next worker.
- No worker stages or commits. Checkbox tracking may be updated only by the orchestrator without changing task semantics.
- Execute tasks continuously in dependency order. Task 5 is hard-blocked on every Task 4 LOCAL receipt; Task 6 identity/review is hard-blocked on all earlier receipts.
- One integrated commit is the only Git write authorized by this plan and occurs only at Task 6 Step 8 under its receipt rules.

## Final-Review Security Correction

- `reviewer_identity=ca26` is a rejection: the prior late container guard could not prevent transient wildcard publication because Docker creates host bindings first.
- The correction fixes the Compose host address to `127.0.0.1`; exact bind acknowledgement and exit-64 pre-binary guard are defense-in-depth only. No direct non-loopback mode or reverse-proxy subsystem is added.
- Required evidence is publication static RED→GREEN, separate gateway-image build PASS whose output is excluded from bind assertions, isolated guard execution exit64/output-safe/no-service-port/residual-zero, unchanged valid full parity, exact 14-path identity, and fresh Oracle+Reviewer approvals.

---

### Task 1: Lock and add the six-service production and validation topology

**Files:**
- Create/Test: `tests/cluster.Tests.ps1`
- Create: `ipfs/cluster.Dockerfile`
- Create: `docker-compose.cluster.yml`
- Create: `tests/compose.cluster-validation.yml`
- Verify unchanged: every Protected / Verify Unchanged path listed above

**Interfaces:**
- Consumes: exact PostgreSQL initializer and gateway health contract from `docker-compose.postgres.yml`; unchanged image/application sources; Compose required interpolation; final-review ruling that only fixed loopback host publication can prevent transient wildcard exposure before container startup; exact bind acknowledgement `127.0.0.1`; Cluster v1.1.6 connector/proxy env names; confirmed direct-proxy RED `add=200 pin/add=200 cat=502` caused by the proxy default `/ip4/127.0.0.1/tcp/5001`.
- Produces: exact services `postgres`, `kubo-a`, `kubo-b`, `cluster-a`, `cluster-b`, `gateway`; volumes `postgres_data`, `kubo_a_data`, `kubo_b_data`, `cluster_a_data`, `cluster_b_data`; paired Cluster targets; sole production mapping `127.0.0.1:${IPFS_S3_GATEWAY_PORT}:9000`; required `IPFS_S3_PUBLISHED_BIND` derived from `IPFS_S3_GATEWAY_BIND`; exact no-output exit-64 pre-binary guard; seven unchanged loopback validation mappings; command `pwsh -NoProfile -File tests/cluster.Tests.ps1`.

**Runtime-revision resume rule:** The original Task 1 baseline files and prior static assertions already exist in the current working tree. The resumed Task 1 worker appends the Step 6 proxy-node assertions, executes Step 7 RED and Step 8's two-line correction, then applies the final-review publication RED/correction in Steps 9-10 before Step 11 GREEN and Step 12 receipt. Do not rewrite or regenerate Steps 1-5. Their blocks remain the complete clean-replay/final-topology reference.

**Final-review security resume rule:** Reviewer identity `ca26` rejected the prior late-guard design because Docker creates the host publication before a container entrypoint can run. After the proxy-node correction is GREEN, append the publication assertions and capture their focused RED against the current arbitrary-bind mapping/late guard; then apply only the fixed mapping plus acknowledgement guard shown below. No new file, script, service, port, reverse proxy, or application change is permitted.

- [ ] **Step 1: Create the missing-file/protected-baseline RED contract**

Create `tests/cluster.Tests.ps1` with this foundation. The hard-coded hashes are the approved `7572570cee529879d57741b4f4ed1b6df2f79dbe` baseline and make an application/profile edit fail before Docker is available.

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposePath = Join-Path $RepoRoot "docker-compose.cluster.yml"
$KuboDockerfilePath = Join-Path $RepoRoot "ipfs/cluster.Dockerfile"
$OverridePath = Join-Path $RepoRoot "tests/compose.cluster-validation.yml"
$RustPath = Join-Path $RepoRoot "tests/cluster.rs"
$RustSupportPath = Join-Path $RepoRoot "tests/support/cluster.rs"
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
$ReadmePath = Join-Path $RepoRoot "README.md"
$RoadmapPath = Join-Path $RepoRoot "ROADMAP.md"

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

$protectedHashes = [ordered]@{
    "docker-compose.yml" = "4e0df23fcfbdd254933bcda0d18b17a215325052cb6d2a7cc70c213b8e44a48c"
    "docker-compose.postgres.yml" = "6a39a43a48beda2cbd79a0ae8ca403a3ead2a67766ed34c214de78fa1a6a1782"
    "docker-compose.multi-gateway.yml" = "0397160b0abf7e5597b1fbfd4395ad0cc40a1112603eeeb90ea0d75422bf51bd"
    "Dockerfile" = "34aa8b4b5b880ec474d14dabe625fce5fecb45c4e906eb3489ad335b7fb6837c"
    "config.docker.toml" = "506596ff7da7c684f5ab9b860a49784f676372f31fd9dbf08fef401438714183"
    "Cargo.toml" = "86f5c654c8b54e57d4b324da0faf75df8f0353c4fa2df208da924dddc49f8764"
    "Cargo.lock" = "b579f0d8669ee6c05c9c09cc71f692c2c4dd667772f59fcbbf0f5ac57452d5c1"
    "ipfs/Dockerfile" = "ba4e543d3004ea5b2db18866f189afd5b2724f35b4e2ed64a6a37573748304f0"
    "ipfs/entrypoint.sh" = "6fb8407d5dcdc206aa4bf50d0e451c36eb28ad133896702271a5cb0a8d17a6ad"
    "src/kubo/add.rs" = "d4698d5e01df5dcf5a32e5737323890960d043110d858f05c8e14938b5b1b751"
    "src/kubo/pin.rs" = "6047757369227cf6fa4c4bbc7debe1cbabe5a9aa1a0bba1498b2c834c7a22216"
    "src/kubo/cat.rs" = "e970271249f4e10efb61f0720a11ee0cbca18b1aad7d5a0f2629bb432c0c90bf"
    "src/state.rs" = "bfbe43c3d9acb9a0b4b3cb901e6cd6c0af8c7e4c728d3149f19c0bb77f938541"
    "src/s3/ops/object.rs" = "3cef9c6b169a717720b721399ea35d28e405943529a2fe7b680c16a01624296e"
    "src/s3/ops/multipart.rs" = "3e08ab1dca7ded499193bee88e9c0014fe34a744d907259ac60573e57bcda4cf"
}
foreach ($entry in $protectedHashes.GetEnumerator()) {
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath (Join-Path $RepoRoot $entry.Key)).Hash.ToLowerInvariant()
    Assert-True ($actual -ceq $entry.Value) "Protected baseline changed: $($entry.Key)"
}

$Compose = Read-NormalizedText $ComposePath
$KuboDockerfile = Read-NormalizedText $KuboDockerfilePath
$Override = Read-NormalizedText $OverridePath
```

- [ ] **Step 2: Run the static contract RED before creating deployment files**

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Expected absent Cluster deployment files to keep the contract RED" }
```

Expected: nonzero with `Required file is missing:` for `docker-compose.cluster.yml`; every protected hash still matches.

- [ ] **Step 3: Create the exact profile-specific Kubo Dockerfile**

Create `ipfs/cluster.Dockerfile` exactly:

```dockerfile
FROM ipfs/kubo:v0.43.0

COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
```

Do not edit `ipfs/entrypoint.sh` or `ipfs/Dockerfile`. The first real proxy test, not an assumed compatibility matrix, decides whether this exact image can support the design.

- [ ] **Step 4: Create the exact six-service production Compose topology**

Create `docker-compose.cluster.yml` with this complete content:

```yaml
name: ipfs3-cluster

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

  kubo-a:
    build:
      context: ./ipfs
      dockerfile: cluster.Dockerfile
    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0
    volumes:
      - kubo_a_data:/data/ipfs
    environment:
      IPFS_PATH: /data/ipfs
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 15s
    restart: unless-stopped

  kubo-b:
    build:
      context: ./ipfs
      dockerfile: cluster.Dockerfile
    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0
    volumes:
      - kubo_b_data:/data/ipfs
    environment:
      IPFS_PATH: /data/ipfs
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 15s
    restart: unless-stopped

  cluster-a:
    image: ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd
    environment:
      IPFS_CLUSTER_CONSENSUS: crdt
      CLUSTER_PEERNAME: cluster-a
      CLUSTER_SECRET: "${IPFS_S3_CLUSTER_SECRET:?IPFS_S3_CLUSTER_SECRET is required}"
      CLUSTER_CRDT_TRUSTEDPEERS: "*"
      CLUSTER_REPLICATIONFACTORMIN: "2"
      CLUSTER_REPLICATIONFACTORMAX: "2"
      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001
      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001
      CLUSTER_RESTAPI_HTTPLISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9094
      CLUSTER_IPFSPROXY_LISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9095
    volumes:
      - cluster_a_data:/data/ipfs-cluster
    depends_on:
      kubo-a:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "ipfs-cluster-ctl", "id"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 15s
    restart: unless-stopped

  cluster-b:
    image: ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd
    environment:
      IPFS_CLUSTER_CONSENSUS: crdt
      CLUSTER_PEERNAME: cluster-b
      CLUSTER_SECRET: "${IPFS_S3_CLUSTER_SECRET:?IPFS_S3_CLUSTER_SECRET is required}"
      CLUSTER_CRDT_TRUSTEDPEERS: "*"
      CLUSTER_REPLICATIONFACTORMIN: "2"
      CLUSTER_REPLICATIONFACTORMAX: "2"
      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001
      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001
      CLUSTER_RESTAPI_HTTPLISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9094
      CLUSTER_IPFSPROXY_LISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9095
    volumes:
      - cluster_b_data:/data/ipfs-cluster
    depends_on:
      kubo-b:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "ipfs-cluster-ctl", "id"]
      interval: 5s
      timeout: 5s
      retries: 30
      start_period: 15s
    restart: unless-stopped

  gateway:
    build: .
    image: ghcr.io/hugefiver/ipfs3:latest
    ports:
      - "127.0.0.1:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_PUBLISHED_BIND: "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}"
      IPFS_S3_KUBO_RPC_URL: http://cluster-a:9095
      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"
      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"
      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"
      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"
      RUST_LOG: info
    entrypoint:
      - /bin/sh
      - -ec
      - |
        case "$${IPFS_S3_PUBLISHED_BIND}" in
          "127.0.0.1") ;;
          *) exit 64 ;;
        esac
        exec /app/ipfs-s3-gateway
    depends_on:
      postgres:
        condition: service_healthy
      cluster-a:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "/app/ipfs-s3-gateway", "--ready-probe"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 10s
    restart: unless-stopped

volumes:
  postgres_data:
  kubo_a_data:
  kubo_b_data:
  cluster_a_data:
  cluster_b_data:

configs:
  postgres_init:
    content: |
      \set app_password '${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}'
      CREATE ROLE ipfs3 LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
      SELECT format('ALTER ROLE ipfs3 PASSWORD %L', :'app_password') \gexec
      CREATE DATABASE ipfs3 OWNER ipfs3;
```

`cluster-a` and `cluster-b` are deliberately duplicated rather than YAML-merged so the static test can compare all settings after normalizing only peer name, Kubo DNS name, and volume name. Their local healthchecks are not a two-peer gate; Task 2 performs that gate before writes.

The gateway mapping is intrinsically loopback before container creation. `IPFS_S3_GATEWAY_BIND` no longer controls the host address; it remains required only as an explicit acknowledgement passed into `IPFS_S3_PUBLISHED_BIND`. The shell guard emits no value and returns 64 on every non-exact value before executing the binary, but it is defense-in-depth rather than the host-publication security boundary.

- [ ] **Step 5: Create the exact loopback-only validation override**

Create `tests/compose.cluster-validation.yml` exactly:

```yaml
services:
  postgres:
    ports:
      - "127.0.0.1:55435:5432"

  kubo-a:
    ports:
      - "127.0.0.1:55100:5001"

  kubo-b:
    ports:
      - "127.0.0.1:55101:5001"

  cluster-a:
    ports:
      - "127.0.0.1:59101:9094"
      - "127.0.0.1:59103:9095"

  cluster-b:
    ports:
      - "127.0.0.1:59102:9094"

  gateway:
    ports:
      - "127.0.0.1:59100:9000"
```

Publish only Cluster A proxy `9095` as loopback `127.0.0.1:59103` in this validation override. Do not publish Cluster B proxy, Kubo swarm/gateway, or any non-loopback address. Production Compose still publishes only the gateway; hosted/local validation sets the base gateway publication to the same `127.0.0.1:59100:9000` uniqueness key.

- [ ] **Step 6: Complete the topology, secret, forbidden-surface, and source-interface assertions**

Append exact set and fragment checks to `tests/cluster.Tests.ps1`. IPFS Cluster v1.1.6 `api/ipfsproxy/config.go` uses env prefix `cluster_ipfsproxy` and JSON field `node_multiaddress`, yielding exact env name `CLUSTER_IPFSPROXY_NODEMULTIADDRESS`; do not infer it from the connector prefix. The source checks freeze both protocol shape and add-before-pin order; the hashes prohibit any disguised application edit.

```powershell
$ExpectedKuboDockerfile = @'
FROM ipfs/kubo:v0.43.0

COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
'@
$ExpectedKuboDockerfile = $ExpectedKuboDockerfile.Replace("`r`n", "`n").TrimEnd("`n")
Assert-True ($KuboDockerfile.TrimEnd("`n") -ceq $ExpectedKuboDockerfile) "Cluster Kubo Dockerfile changed"

$services = Get-YamlBlock $Compose "services" 0
$serviceNames = @([regex]::Matches($services, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedServices = @("postgres", "kubo-a", "kubo-b", "cluster-a", "cluster-b", "gateway")
Assert-True ($serviceNames.Count -eq 6) "Cluster topology must define exactly six services"
Assert-True ((($serviceNames | Sort-Object) -join "`n") -ceq (($expectedServices | Sort-Object) -join "`n")) "Cluster service set changed"

$volumes = Get-YamlBlock $Compose "volumes" 0
$volumeNames = @([regex]::Matches($volumes, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedVolumes = @("postgres_data", "kubo_a_data", "kubo_b_data", "cluster_a_data", "cluster_b_data")
Assert-True ($volumeNames.Count -eq 5) "Cluster topology must declare exactly five volumes"
Assert-True ((($volumeNames | Sort-Object) -join "`n") -ceq (($expectedVolumes | Sort-Object) -join "`n")) "Cluster volume set changed"

$postgres = Get-YamlBlock $services "postgres" 2
$kuboA = Get-YamlBlock $services "kubo-a" 2
$kuboB = Get-YamlBlock $services "kubo-b" 2
$clusterA = Get-YamlBlock $services "cluster-a" 2
$clusterB = Get-YamlBlock $services "cluster-b" 2
$gateway = Get-YamlBlock $services "gateway" 2
Assert-Contains $postgres "    image: postgres:17" "PostgreSQL tag changed"
foreach ($kubo in @($kuboA, $kuboB)) {
    Assert-Contains $kubo "      dockerfile: cluster.Dockerfile" "Kubo must use the profile-specific Dockerfile"
    Assert-Contains $kubo "    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0" "Kubo profile image tag changed"
    Assert-NotMatches $kubo '(?m)^    ports:\s*$' "Production Kubo must not publish ports"
}
Assert-Contains $kuboA "      - kubo_a_data:/data/ipfs" "Kubo A volume changed"
Assert-Contains $kuboB "      - kubo_b_data:/data/ipfs" "Kubo B volume changed"

$clusterImage = "    image: ipfs/ipfs-cluster:v1.1.6@sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd"
foreach ($cluster in @($clusterA, $clusterB)) {
    Assert-Contains $cluster $clusterImage "Cluster image tag/digest changed"
    Assert-Contains $cluster "      IPFS_CLUSTER_CONSENSUS: crdt" "Cluster consensus must be CRDT"
    Assert-Contains $cluster '      CLUSTER_SECRET: "${IPFS_S3_CLUSTER_SECRET:?IPFS_S3_CLUSTER_SECRET is required}"' "Cluster secret interpolation changed"
    Assert-Contains $cluster '      CLUSTER_CRDT_TRUSTEDPEERS: "*"' "CRDT trusted-peer contract changed"
    Assert-Contains $cluster '      CLUSTER_REPLICATIONFACTORMIN: "2"' "Cluster replication minimum changed"
    Assert-Contains $cluster '      CLUSTER_REPLICATIONFACTORMAX: "2"' "Cluster replication maximum changed"
    Assert-Contains $cluster "      CLUSTER_RESTAPI_HTTPLISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9094" "Cluster REST internal listener changed"
    Assert-Contains $cluster "      CLUSTER_IPFSPROXY_LISTENMULTIADDRESS: /ip4/0.0.0.0/tcp/9095" "Cluster proxy internal listener changed"
    Assert-NotMatches $cluster '(?m)^    ports:\s*$' "Production Cluster must not publish REST/proxy ports"
}
Assert-Contains $clusterA "      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001" "Cluster A Kubo connector changed"
Assert-Contains $clusterB "      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001" "Cluster B Kubo connector changed"
$peerTargetContracts = @(
    @{ Name = "Cluster A"; Block = $clusterA; Expected = "/dns4/kubo-a/tcp/5001"; ForbiddenPeer = "kubo-b" },
    @{ Name = "Cluster B"; Block = $clusterB; Expected = "/dns4/kubo-b/tcp/5001"; ForbiddenPeer = "kubo-a" }
)
foreach ($peer in $peerTargetContracts) {
    $connectorMatches = [regex]::Matches($peer.Block, '(?m)^      CLUSTER_IPFSHTTP_NODEMULTIADDRESS: (\S+)$')
    $proxyMatches = [regex]::Matches($peer.Block, '(?m)^      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: (\S+)$')
    Assert-True ($connectorMatches.Count -eq 1) "$($peer.Name) must define exactly one connector node target"
    Assert-True ($proxyMatches.Count -eq 1) "$($peer.Name) must define exactly one proxy node target"
    $connectorTarget = $connectorMatches[0].Groups[1].Value
    $proxyTarget = $proxyMatches[0].Groups[1].Value
    Assert-True ($connectorTarget -ceq $peer.Expected) "$($peer.Name) connector must target its paired Kubo"
    Assert-True ($proxyTarget -ceq $peer.Expected) "$($peer.Name) proxy must target its paired Kubo"
    Assert-True ($connectorTarget -ceq $proxyTarget) "$($peer.Name) connector/proxy targets must match"
    Assert-NotContains $peer.Block "/dns4/$($peer.ForbiddenPeer)/tcp/5001" "$($peer.Name) must not cross-wire to the other Kubo"
    Assert-NotContains $peer.Block "/ip4/127.0.0.1/tcp/5001" "$($peer.Name) must not use the container-local Kubo default"
}
Assert-Contains $clusterA "      - cluster_a_data:/data/ipfs-cluster" "Cluster A identity volume changed"
Assert-Contains $clusterB "      - cluster_b_data:/data/ipfs-cluster" "Cluster B identity volume changed"

Assert-Contains $gateway "      IPFS_S3_KUBO_RPC_URL: http://cluster-a:9095" "Gateway must use Cluster A proxy"
Assert-Contains $gateway '      IPFS_S3_PUBLISHED_BIND: "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}"' "Gateway bind acknowledgement wiring changed"
$gatewayPorts = Get-YamlBlock $gateway "ports" 4
$gatewayMappings = @([regex]::Matches($gatewayPorts, '(?m)^      - "([^"]+)"\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($gatewayMappings.Count -eq 1) "Gateway must define exactly one production host mapping"
Assert-True ($gatewayMappings[0] -ceq '127.0.0.1:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000') "Gateway production mapping must be fixed loopback"
Assert-NotContains $gatewayPorts '${IPFS_S3_GATEWAY_BIND' "Bind acknowledgement must not drive host publication"
Assert-NotMatches $gatewayPorts '(?m)^\s*-\s+"?(0\.0\.0\.0|\[::\]|::):' "Wildcard gateway host publication is forbidden"
$gatewayEntrypoint = (Get-YamlBlock $gateway "entrypoint" 4).TrimEnd("`n")
$expectedGatewayEntrypoint = @'
      - /bin/sh
      - -ec
      - |
        case "$${IPFS_S3_PUBLISHED_BIND}" in
          "127.0.0.1") ;;
          *) exit 64 ;;
        esac
        exec /app/ipfs-s3-gateway
'@
$expectedGatewayEntrypoint = $expectedGatewayEntrypoint.Replace("`r`n", "`n").TrimEnd("`n")
Assert-True ($gatewayEntrypoint -ceq $expectedGatewayEntrypoint) "Gateway bind acknowledgement guard changed"
Assert-True (([regex]::Matches($gateway, 'IPFS_S3_PUBLISHED_BIND')).Count -eq 2) "Published-bind acknowledgement must appear only in environment and guard"
Assert-NotMatches $gatewayEntrypoint '(?im)\b(echo|printf|printenv|env|set)\b' "Gateway bind guard must not emit values"
$guardCaseIndex = $gatewayEntrypoint.IndexOf('case "$${IPFS_S3_PUBLISHED_BIND}" in', [StringComparison]::Ordinal)
$guardRejectIndex = $gatewayEntrypoint.IndexOf('*) exit 64 ;;', [StringComparison]::Ordinal)
$guardExecIndex = $gatewayEntrypoint.IndexOf('exec /app/ipfs-s3-gateway', [StringComparison]::Ordinal)
Assert-True ($guardCaseIndex -ge 0 -and $guardRejectIndex -gt $guardCaseIndex -and $guardExecIndex -gt $guardRejectIndex) "Gateway must reject a mismatched acknowledgement before binary exec"
$publishedServices = @($serviceNames | Where-Object { (Get-YamlBlock $services $_ 2) -match '(?m)^    ports:\s*$' })
Assert-True ($publishedServices.Count -eq 1 -and $publishedServices[0] -ceq "gateway") "Only gateway may publish a production port"

foreach ($required in @("POSTGRES_PASSWORD", "IPFS_S3_ACCESS_KEY_ID", "IPFS_S3_SECRET_ACCESS_KEY", "IPFS_S3_MASTER_KEY", "IPFS_S3_CLUSTER_SECRET", "IPFS_S3_GATEWAY_BIND", "IPFS_S3_GATEWAY_PORT")) {
    Assert-Contains $Compose ('${' + $required + ':?') "Required interpolation missing: $required"
    Assert-NotContains $Compose ('${' + $required + ':-') "Required interpolation gained a default: $required"
    Assert-NotContains $Compose ('${' + $required + '-default') "Required interpolation gained an alternate default: $required"
}
foreach ($forbidden in @("container_name:", "cloudflared", "PINATA_JWT", "FILEBASE_PINNING_TOKEN", "CLOUDFLARE_TUNNEL_TOKEN", "swarm.key", "CLUSTER_ID", "CLUSTER_PRIVATEKEY", "identity.json", "service.json", "gateway_data", "remote")) {
    Assert-NotContains $Compose $forbidden "Forbidden Cluster production fragment: $forbidden"
}

$validationServices = Get-YamlBlock $Override "services" 0
$actualMappings = @([regex]::Matches($Override, '(?m)^\s*-\s+"([^"]+)"\s*$') | ForEach-Object { $_.Groups[1].Value })
$expectedMappings = @("127.0.0.1:55435:5432", "127.0.0.1:55100:5001", "127.0.0.1:55101:5001", "127.0.0.1:59101:9094", "127.0.0.1:59102:9094", "127.0.0.1:59103:9095", "127.0.0.1:59100:9000")
Assert-True ($actualMappings.Count -eq 7) "Validation override must publish exactly seven ports"
Assert-True ((($actualMappings | Sort-Object) -join "`n") -ceq (($expectedMappings | Sort-Object) -join "`n")) "Validation mappings changed"
Assert-NotContains $Override "0.0.0.0" "Validation ports must be loopback-only"
$validationClusterA = Get-YamlBlock $validationServices "cluster-a" 2
$validationClusterB = Get-YamlBlock $validationServices "cluster-b" 2
Assert-Contains $validationClusterA '      - "127.0.0.1:59103:9095"' "Validation must expose Cluster A proxy on exact loopback port 59103"
Assert-NotContains $validationClusterB ":9095" "Validation must not expose Cluster B proxy"
Assert-NotMatches $Override '(?m)^volumes:' "Validation override must not declare volumes"

$AddSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/add.rs")
$PinSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/pin.rs")
$CatSource = Read-NormalizedText (Join-Path $RepoRoot "src/kubo/cat.rs")
$ObjectSource = Read-NormalizedText (Join-Path $RepoRoot "src/s3/ops/object.rs")
Assert-Contains $AddSource 'pub async fn stream_add_with_progress<S, E>(' "Kubo add interface changed"
Assert-Contains $AddSource 'api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false&progress=true' "Kubo add query changed"
Assert-Contains $AddSource 'let body = ReqwestBody::wrap_stream(ReaderStream::new(reader));' "Kubo add stream wrapping changed"
Assert-Contains $AddSource 'let part = multipart::Part::stream(body)' "Kubo add streaming multipart changed"
Assert-Contains $AddSource '.file_name("object")' "Kubo add multipart filename changed"
Assert-Contains $AddSource '.mime_str("application/octet-stream")' "Kubo add MIME changed"
Assert-Contains $AddSource 'let form = multipart::Form::new().part("file", part);' "Kubo multipart field changed"
Assert-Contains $AddSource 'let mut lines = NdjsonBuffer::new();' "Kubo add NDJSON parser changed"
Assert-Contains $AddSource 'let mut final_record_was_root = false;' "Kubo add final-root tracking changed"
Assert-Contains $AddSource 'pub async fn stream_add<S, E>(kubo: &KuboClient, stream: S, cid_version: u8) -> AppResult<String>' "Simple add signature changed"
Assert-Contains $PinSource 'pub async fn pin_add(kubo: &KuboClient, cid: &str) -> AppResult<()>' "Kubo pin signature changed"
Assert-Contains $PinSource 'api/v0/pin/add?arg={cid}' "Kubo pin/add query changed"
Assert-Contains $PinSource 'api/v0/pin/add?arg={canonical_cid}&recursive=true&progress=true' "Progress pin query changed"
Assert-Contains $PinSource '#[serde(rename = "Pins")]' "Kubo pin response shape changed"
Assert-Contains $CatSource 'pub async fn stream_cat(' "Kubo cat signature changed"
Assert-Contains $CatSource 'format!("{}/api/v0/cat?arg={cid}", kubo.base_url())' "Kubo cat query changed"
Assert-InOrder $ObjectSource @(
    'let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1).await?;',
    'crate::kubo::pin::pin_add(&state.kubo, &cid).await?;',
    'Ok(StoredObject {'
) "Plain object must remain add then pin then publication input"
Write-Host "cluster static contract tests: PASSED"
```

- [ ] **Step 7: Run the new proxy-node contract RED against the current Compose**

Extend `tests/cluster.Tests.ps1` with the exact checks from Step 6 **before** adding either new Compose environment line. Run:

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
$proxyNodeRed = $LASTEXITCODE
if ($proxyNodeRed -eq 0) { throw "Expected current Compose to fail the missing Cluster proxy-node target contract" }
```

Expected causal RED: both existing connector targets remain valid, but the first missing `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` fails with `must define exactly one proxy node target`. Record the already confirmed runtime evidence separately as `DIRECT PROXY RED: topology=GREEN add=200 pin=200 cat=502; proxy attempted container-local 127.0.0.1:5001; connector pin succeeded`. Do not rerun Docker and do not classify this as an application-protocol failure.

- [ ] **Step 8: Add the two exact paired proxy forwarding targets**

Only after Step 7 is RED, add the following lines immediately after each peer's unchanged `CLUSTER_IPFSHTTP_NODEMULTIADDRESS`; these are the final lines already reflected in Step 4's complete topology block:

```yaml
# cluster-a
      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-a/tcp/5001

# cluster-b
      CLUSTER_IPFSPROXY_NODEMULTIADDRESS: /dns4/kubo-b/tcp/5001
```

Do not change either connector target, either REST/proxy listen setting, service/volume/port count, gateway URL, Cluster image, or validation override. This changes only proxy forwarding from the invalid container-local default to each peer's already paired Kubo DNS endpoint.

- [ ] **Step 9: Extend the publication contract and capture the final-review security RED**

Append the fixed-mapping, acknowledgement environment, exact entrypoint, no-output, exit-64, and guard-before-exec assertions shown in Step 6 before changing the gateway Compose block. Run:

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
$publicationRed = $LASTEXITCODE
if ($publicationRed -eq 0) { throw "Expected the arbitrary-bind/late-guard publication contract to be RED" }
```

Expected causal RED: the current mapping still contains `${IPFS_S3_GATEWAY_BIND}` or otherwise lacks the exact intrinsic loopback mapping/guard contract. Record `FINAL REVIEW SECURITY RED: reviewer_identity=ca26; late container guard cannot prevent transient wildcard host publication`. Do not run Docker: this RED is dependency-free and must precede the Compose correction.

- [ ] **Step 10: Apply only the intrinsic loopback mapping and defence-in-depth guard**

Replace the gateway publication/environment/entrypoint portion with the exact final block from Step 4:

```yaml
    ports:
      - "127.0.0.1:${IPFS_S3_GATEWAY_PORT:?IPFS_S3_GATEWAY_PORT is required}:9000"
    environment:
      IPFS_S3_BIND: 0.0.0.0:9000
      IPFS_S3_PUBLISHED_BIND: "${IPFS_S3_GATEWAY_BIND:?IPFS_S3_GATEWAY_BIND is required}"
      IPFS_S3_KUBO_RPC_URL: http://cluster-a:9095
      IPFS_S3_DATABASE_URL: "postgres://ipfs3:${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}@postgres:5432/ipfs3"
      IPFS_S3_ACCESS_KEY_ID: "${IPFS_S3_ACCESS_KEY_ID:?IPFS_S3_ACCESS_KEY_ID is required}"
      IPFS_S3_SECRET_ACCESS_KEY: "${IPFS_S3_SECRET_ACCESS_KEY:?IPFS_S3_SECRET_ACCESS_KEY is required}"
      IPFS_S3_MASTER_KEY: "${IPFS_S3_MASTER_KEY:?IPFS_S3_MASTER_KEY is required}"
      RUST_LOG: info
    entrypoint:
      - /bin/sh
      - -ec
      - |
        case "$${IPFS_S3_PUBLISHED_BIND}" in
          "127.0.0.1") ;;
          *) exit 64 ;;
        esac
        exec /app/ipfs-s3-gateway
```

The mapping must not contain `${IPFS_S3_GATEWAY_BIND}`. The guard must not echo, format, or otherwise emit the acknowledgement; it must reject all non-exact values before binary execution. Preserve every other gateway field and the exact six-service/five-volume/seven-validation-port topology.

- [ ] **Step 11: Parse the static test and run Task 1 GREEN without Docker**

```powershell
$tokens = $null
$errors = $null
$null = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path -LiteralPath "tests/cluster.Tests.ps1"),
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -ne 0) { throw ($errors.Message -join "; ") }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster topology static contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Whitespace validation failed" }
```

Expected: parser success and `cluster static contract tests: PASSED` after adding that final success line; no Docker command executes.

- [ ] **Step 12: Record Task 1 receipt without a Git write**

Record `Task 1 PROXY-NODE STATIC RED→GREEN` and `FINAL REVIEW PUBLICATION STATIC RED→GREEN`; reviewer identity `ca26` rejected; exact fixed production mapping; exact bind acknowledgement/environment; exit-64/no-output/guard-before-exec contract; no arbitrary non-loopback mapping; connector/proxy pairs; unchanged six services, five volumes, seven validation mappings, and 15 protected hashes; `DOCKER/LIVE: NOT RUN`. Retain the confirmed proxy runtime RED pending Task 4 GREEN. Do not stage or commit.

---

### Task 2: Add actual v1.1.6 Cluster response types, bounded polling, and live Rust scenarios

**Files:**
- Create/Test: `tests/support/cluster.rs`
- Create/Test: `tests/cluster.rs`
- Modify/Test: `tests/cluster.Tests.ps1`
- Reuse unchanged: `tests/support/mod.rs`, `tests/support/sigv4.rs`, `Cargo.toml`, `Cargo.lock`

**Interfaces:**
- Consumes: Task 1's exact paired connector/proxy node targets and localhost/cross-wire rejection; Cluster v1.1.6 `GET /health -> 204`, NDJSON `GET /peers`, JSON `GET /allocations/{cid}`, JSON or non-success `GET /pins/{cid}`, tracker status `pinned`; public production `ipfs_s3_gateway::kubo::{KuboClient, add::stream_add, pin::pin_add, cat::stream_cat}` whose protected source owns the multipart/query/NDJSON/cat wire details; rust-s3 path-style APIs only for the separate replication scenario; `support::sigv4::send_sigv4`; environment endpoints from Task 3.
- Produces: `ClusterClient::new(endpoint: &str) -> anyhow::Result<Self>`; `health_probe`, `peers_probe`, `allocation_probe`, `pin_status_probe`; `is_release_1_1_6_version(version: &str) -> bool`; sanitized `ProbeError::{Transient,Terminal}`; `wait_for_shared_two_peer_view`, `peer_set_digest`, `wait_for_two_pinned`, `wait_until_not_fully_pinned`, `kubo_cat`; direct production-surface calls through `KuboClient::new_with_timeouts`, `kubo::add::stream_add`, `kubo::pin::pin_add`, and `kubo::cat::stream_cat`; `RecoveryState::{write_claimed,read}`; exactly five Tokio live tests `cluster_topology_converges`, `cluster_proxy_compatibility`, `cluster_replication_and_retention`, `cluster_peer_b_outage_contract`, `cluster_peer_b_restart_recovery`; and one plain deterministic unit test `cluster_support::release_version_validator_accepts_exact_release_and_build_metadata`.

- [ ] **Step 1: Capture compile RED for both absent Rust files**

```powershell
cargo test --test cluster --no-run
if ($LASTEXITCODE -eq 0) { throw "Expected absent cluster target to be RED" }
```

Expected: Cargo reports no test target named `cluster`; no dependency file changes.

- [ ] **Step 2: Create versioned response types and bounded client construction**

Create `tests/support/cluster.rs` with these public shapes and constants. Keep serde field names identical to v1.1.6; `/peers` is NDJSON, not an array.

```rust
use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    fmt,
    io::Write,
    path::Path,
    time::Duration,
};

const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterPeer {
    pub id: String,
    #[serde(default)]
    pub cluster_peers: Vec<String>,
    pub version: String,
    pub peername: String,
    #[serde(default)]
    pub error: String,
    pub ipfs: ClusterIpfsIdentity,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterIpfsIdentity {
    pub id: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PinAllocation {
    pub cid: String,
    pub replication_factor_min: i32,
    pub replication_factor_max: i32,
    #[serde(default)]
    pub allocations: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GlobalPinInfo {
    pub cid: String,
    #[serde(default)]
    pub allocations: Vec<String>,
    #[serde(default)]
    pub peer_map: HashMap<String, PeerPinInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerPinInfo {
    pub status: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Debug)]
pub enum PinStatusObservation {
    Pending,
    Available(GlobalPinInfo),
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeError {
    Transient(&'static str),
    Terminal(&'static str),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transient(category) => write!(f, "transient Cluster probe: {category}"),
            Self::Terminal(category) => write!(f, "terminal Cluster contract error: {category}"),
        }
    }
}

impl std::error::Error for ProbeError {}

type ProbeResult<T> = std::result::Result<T, ProbeError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEvidence {
    pub allocations: Vec<String>,
    pub pinned_peers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryState {
    pub schema: String,
    pub source: String,
    pub cid: String,
    pub body: Vec<u8>,
    pub peer_set_sha256: String,
}

pub struct ClusterClient {
    base_url: String,
    http: Client,
}

impl ClusterClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        let endpoint = endpoint.trim_end_matches('/');
        ensure!(endpoint.starts_with("http://127.0.0.1:"), "Cluster validation REST must be loopback HTTP");
        Ok(Self {
            base_url: endpoint.to_owned(),
            http: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(CALL_TIMEOUT)
                .build()
                .context("build bounded Cluster client")?,
        })
    }
}
```

- [ ] **Step 3: Implement exact HTTP/NDJSON probes with transient/terminal classification**

Add these methods. Every connect/request/body operation is covered by the client timeout plus an outer scenario deadline. Only request-send or response-body transport failures are `Transient`; malformed JSON/NDJSON, wrong health status/body, and other response-contract errors are `Terminal`. Neither raw response bodies nor peer IDs appear in errors.

```rust
impl ClusterClient {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn response(&self, path: &str) -> ProbeResult<reqwest::Response> {
        self.http.get(self.url(path)).send().await
            .map_err(|_| ProbeError::Transient("request_transport"))
    }

    async fn response_bytes(response: reqwest::Response) -> ProbeResult<bytes::Bytes> {
        response.bytes().await
            .map_err(|_| ProbeError::Transient("response_body_transport"))
    }

    fn decode_json<T: DeserializeOwned>(bytes: &[u8], category: &'static str) -> ProbeResult<T> {
        serde_json::from_slice(bytes).map_err(|_| ProbeError::Terminal(category))
    }

    pub async fn health_probe(&self) -> ProbeResult<()> {
        let response = self.response("/health").await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Err(ProbeError::Terminal("health_status_not_204"));
        }
        if !Self::response_bytes(response).await?.is_empty() {
            return Err(ProbeError::Terminal("health_204_body_not_empty"));
        }
        Ok(())
    }

    pub async fn peers_probe(&self) -> ProbeResult<Vec<ClusterPeer>> {
        let response = self.response("/peers").await?;
        if response.status() == StatusCode::NO_CONTENT { return Ok(Vec::new()); }
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("peers_status_not_success"));
        }
        let bytes = Self::response_bytes(response).await?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| ProbeError::Terminal("peers_body_not_utf8"))?;
        text.lines().filter(|line| !line.trim().is_empty())
            .map(|line| Self::decode_json(line.as_bytes(), "peers_malformed_ndjson"))
            .collect()
    }

    pub async fn allocation_probe(&self, cid: &str) -> ProbeResult<Option<PinAllocation>> {
        let response = self.response(&format!("/allocations/{cid}")).await?;
        if response.status() == StatusCode::NOT_FOUND { return Ok(None); }
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("allocation_status_not_success"));
        }
        let bytes = Self::response_bytes(response).await?;
        Self::decode_json(&bytes, "allocation_malformed_json").map(Some)
    }

    pub async fn pin_status_probe(&self, cid: &str) -> ProbeResult<PinStatusObservation> {
        let path = format!("/pins/{cid}");
        let response = self.response(&path).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(PinStatusObservation::Pending);
        }
        if !response.status().is_success() {
            return Ok(PinStatusObservation::Rejected);
        }
        let bytes = Self::response_bytes(response).await?;
        Ok(PinStatusObservation::Available(Self::decode_json(
            &bytes,
            "pin_status_malformed_json",
        )?))
    }
}
```

- [ ] **Step 4: Implement exact-two-peer and physical-pin polling without error suppression**

Record the existing causal live RED before editing: the official v1.1.6 image returned build-bearing `/peers.version`, the topology test failed with sanitized category `peer_version_not_exact_1_1_6`, and no proxy/S3 stage ran. This RED is evidence of an overly strict representation contract, not a different release or proxy failure.

Add the following helpers and use the same decision table in both pin waiters: retry only `ProbeError::Transient`, zero/one-peer `PeerView::Pending`, absent allocation, and documented pinning transitions; return every `ProbeError::Terminal` immediately. Wrong release core, pre-release or malformed build metadata, malformed payload, duplicate/third/unknown peer, contradictory full REST view, wrong replication factor/CID, unknown tracker state, and tracker error are terminal sanitized categories. Never format a raw version, build metadata, response body, or ID.

```rust
#[derive(Debug)]
enum PeerView {
    Pending(usize),
    Ready(Vec<String>),
}

fn sorted_set(values: &[String]) -> Vec<String> {
    let mut unique = values.iter().cloned().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
    unique.sort();
    unique
}

fn is_release_1_1_6_version(version: &str) -> bool {
    let Some(suffix) = version.strip_prefix("1.1.6") else { return false };
    if suffix.is_empty() { return true; }
    let Some(metadata) = suffix.strip_prefix('+') else { return false };
    !metadata.is_empty() && metadata.split('.').all(|identifier| {
        !identifier.is_empty() && identifier.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-'
        })
    })
}

#[test]
fn release_version_validator_accepts_exact_release_and_build_metadata() {
    for accepted in [
        "1.1.6",
        "1.1.6+gitabcdef",
        "1.1.6+git-ABC.42",
        "1.1.6+0",
        "1.1.6+a-b.c9",
    ] {
        assert!(is_release_1_1_6_version(accepted));
    }
    for rejected in [
        "1.1.5",
        "1.1.7",
        "1.1.60",
        "1.1.6-rc.1",
        "1.1.6+",
        "1.1.6+.git",
        "1.1.6+git.",
        "1.1.6+git..abcdef",
        "1.1.6+git abcdef",
        "1.1.6+git/abcdef",
        "1.1.6+git_abcdef",
        "1.1.6++gitabcdef",
        "1.1.6gitabcdef",
        "1.1.6.other",
        " 1.1.6",
        "1.1.6 ",
    ] {
        assert!(!is_release_1_1_6_version(rejected));
    }
}

async fn probe_peer_view(client: &ClusterClient) -> ProbeResult<PeerView> {
    client.health_probe().await?;
    let peers = client.peers_probe().await?;
    if peers.len() > 2 { return Err(ProbeError::Terminal("third_peer_record")); }
    let mut ids = Vec::with_capacity(peers.len());
    for peer in &peers {
        if peer.error.is_empty() && !peer.id.is_empty() && !peer.peername.is_empty() &&
            peer.ipfs.error.is_empty() && !peer.ipfs.id.is_empty() {
            // Values are retained only for in-memory equality; no value is formatted.
        } else {
            return Err(ProbeError::Terminal("peer_record_invalid"));
        }
        if !is_release_1_1_6_version(&peer.version) {
            return Err(ProbeError::Terminal("peer_version_not_release_1_1_6"));
        }
        if peer.cluster_peers.len() > 2 {
            return Err(ProbeError::Terminal("third_peer_membership"));
        }
        ids.push(peer.id.clone());
    }
    let unique = sorted_set(&ids);
    if unique.len() != ids.len() { return Err(ProbeError::Terminal("duplicate_peer_id")); }
    if unique.len() < 2 || peers.iter().any(|peer| peer.cluster_peers.len() < 2) {
        return Ok(PeerView::Pending(unique.len()));
    }
    if peers.iter().any(|peer| sorted_set(&peer.cluster_peers) != unique) {
        return Err(ProbeError::Terminal("peer_membership_contradiction"));
    }
    Ok(PeerView::Ready(unique))
}

pub async fn wait_for_shared_two_peer_view(
    a: &ClusterClient,
    b: &ClusterClient,
    timeout: Duration,
) -> ProbeResult<Vec<String>> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let (a_probe, b_probe) = tokio::join!(probe_peer_view(a), probe_peer_view(b));
        match (a_probe, b_probe) {
            (Err(error @ ProbeError::Terminal(_)), _) |
            (_, Err(error @ ProbeError::Terminal(_))) => return Err(error),
            (Ok(PeerView::Ready(a_ids)), Ok(PeerView::Ready(b_ids))) => {
                if a_ids != b_ids {
                    return Err(ProbeError::Terminal("contradictory_complete_rest_views"));
                }
                return Ok(a_ids);
            }
            (Ok(PeerView::Pending(a_count)), Ok(PeerView::Pending(b_count))) => {
                let _sanitized_counts = (a_count, b_count);
            }
            (Err(ProbeError::Transient(_)), _) | (_, Err(ProbeError::Transient(_))) |
            (Ok(PeerView::Pending(_)), Ok(PeerView::Ready(_))) |
            (Ok(PeerView::Ready(_)), Ok(PeerView::Pending(_))) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ProbeError::Terminal("two_peer_convergence_timeout"));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub fn peer_set_digest(values: &[String]) -> ProbeResult<String> {
    let ids = sorted_set(values);
    if ids.len() != 2 || ids.len() != values.len() {
        return Err(ProbeError::Terminal("peer_digest_requires_exact_distinct_two"));
    }
    let mut hash = Sha256::new();
    for id in ids { hash.update(id.as_bytes()); hash.update([0]); }
    Ok(hex::encode(hash.finalize()))
}

pub async fn wait_for_two_pinned(
    client: &ClusterClient,
    cid: &str,
    expected_peers: &[String],
    timeout: Duration,
) -> ProbeResult<ReplicationEvidence> {
    let expected = sorted_set(expected_peers);
    if expected.len() != 2 || expected.len() != expected_peers.len() {
        return Err(ProbeError::Terminal("pin_wait_requires_exact_distinct_two"));
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let allocation = match client.allocation_probe(cid).await {
            Ok(Some(value)) => value,
            Ok(None) | Err(ProbeError::Transient(_)) => {
                if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("two_pin_timeout")); }
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        if allocation.cid != cid || allocation.replication_factor_min != 2 || allocation.replication_factor_max != 2 {
            return Err(ProbeError::Terminal("allocation_contract_mismatch"));
        }
        let allocations = sorted_set(&allocation.allocations);
        if allocations.len() != allocation.allocations.len() || allocations.iter().any(|id| !expected.contains(id)) {
            return Err(ProbeError::Terminal("allocation_peer_set_invalid"));
        }
        let status = match client.pin_status_probe(cid).await {
            Ok(PinStatusObservation::Available(value)) => value,
            Ok(PinStatusObservation::Pending) | Err(ProbeError::Transient(_)) => {
                if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("two_pin_timeout")); }
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
            Ok(PinStatusObservation::Rejected) => return Err(ProbeError::Terminal("pin_status_rejected")),
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        let status_allocations = sorted_set(&status.allocations);
        if status.cid != cid || status_allocations.len() != status.allocations.len() || status_allocations != allocations {
            return Err(ProbeError::Terminal("pin_status_contract_mismatch"));
        }
        if status.peer_map.keys().any(|id| !expected.contains(id)) || status.peer_map.len() > 2 {
            return Err(ProbeError::Terminal("pin_status_peer_set_invalid"));
        }
        let known_states = [
            "pinned", "pinning", "unpinning", "unpinned", "remote",
            "pin_queued", "unpin_queued", "queued", "sharded",
        ];
        if status.peer_map.values().any(|info| !info.error.is_empty() || !known_states.contains(&info.status.as_str())) {
            return Err(ProbeError::Terminal("pin_tracker_contract_error"));
        }
        let pinned = sorted_set(&status.peer_map.iter()
            .filter(|(_, info)| info.status == "pinned")
            .map(|(peer, _)| peer.clone()).collect::<Vec<_>>());
        if allocations == expected && pinned == expected {
            return Ok(ReplicationEvidence { allocations, pinned_peers: pinned });
        }
        if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("two_pin_timeout")); }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn wait_until_not_fully_pinned(
    client: &ClusterClient,
    cid: &str,
    expected_peer_digest: &str,
    timeout: Duration,
) -> ProbeResult<()> {
    if expected_peer_digest.len() != 64 || !expected_peer_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ProbeError::Terminal("outage_wait_requires_valid_peer_digest"));
    }
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client.health_probe().await {
            Ok(()) => {}
            Err(ProbeError::Transient(_)) => {
                if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("outage_observation_timeout")); }
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        }
        let allocation = match client.allocation_probe(cid).await {
            Ok(Some(value)) => value,
            Ok(None) => return Err(ProbeError::Terminal("outage_allocation_missing")),
            Err(ProbeError::Transient(_)) => {
                if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("outage_observation_timeout")); }
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        };
        let expected = sorted_set(&allocation.allocations);
        if expected.len() != 2 || expected.len() != allocation.allocations.len() ||
            peer_set_digest(&expected)? != expected_peer_digest {
            return Err(ProbeError::Terminal("outage_allocation_peer_digest_mismatch"));
        }
        if allocation.cid != cid || allocation.replication_factor_min != 2 || allocation.replication_factor_max != 2 ||
            sorted_set(&allocation.allocations) != expected {
            return Err(ProbeError::Terminal("outage_allocation_contract_mismatch"));
        }
        match client.pin_status_probe(cid).await {
            Ok(PinStatusObservation::Rejected | PinStatusObservation::Pending) => return Ok(()),
            Ok(PinStatusObservation::Available(status)) => {
                let status_allocations = sorted_set(&status.allocations);
                if status.cid != cid || status_allocations.len() != status.allocations.len() || status_allocations != expected ||
                    status.peer_map.keys().any(|id| !expected.contains(id)) || status.peer_map.len() > 2 {
                    return Err(ProbeError::Terminal("outage_pin_status_contract_mismatch"));
                }
                let documented_states = [
                    "cluster_error", "pin_error", "unpin_error", "error", "pinned", "pinning",
                    "unpinning", "unpinned", "remote", "pin_queued", "unpin_queued", "queued",
                    "sharded", "unexpectedly_unpinned",
                ];
                if status.peer_map.values().any(|info| !documented_states.contains(&info.status.as_str())) {
                    return Err(ProbeError::Terminal("outage_unknown_tracker_state"));
                }
                if status.peer_map.values().any(|info| !info.error.is_empty()) { return Ok(()); }
                let pinned = sorted_set(&status.peer_map.iter()
                    .filter(|(_, info)| info.status == "pinned" && info.error.is_empty())
                    .map(|(peer, _)| peer.clone()).collect::<Vec<_>>());
                if pinned != expected { return Ok(()); }
            }
            Err(ProbeError::Transient(_)) => {}
            Err(error @ ProbeError::Terminal(_)) => return Err(error),
        }
        if tokio::time::Instant::now() >= deadline { return Err(ProbeError::Terminal("outage_observation_timeout")); }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
```

- [ ] **Step 5: Add bounded Kubo cat and exact recovery receipt**

Complete `tests/support/cluster.rs` with these helpers. Outer PowerShell must already have claimed the unique absolute temp receipt with CreateNew. Rust cannot create it: `write_claimed` accepts only an existing zero-length regular file, writes the CID, deterministic bytes, and a 64-hex SHA-256 digest of the in-memory two-peer set, and never serializes or prints an identity.

```rust
pub async fn kubo_cat(endpoint: &str, cid: &str) -> Result<Vec<u8>> {
    ensure!(endpoint.starts_with("http://127.0.0.1:"), "Kubo validation API must be loopback HTTP");
    let client = Client::builder().connect_timeout(Duration::from_secs(5)).timeout(CALL_TIMEOUT)
        .build().context("build bounded Kubo client")?;
    let mut url = url::Url::parse(&format!("{}/api/v0/cat", endpoint.trim_end_matches('/')))
        .context("parse Kubo cat URL")?;
    url.query_pairs_mut().append_pair("arg", cid);
    let response = client.post(url).send().await.context("Kubo cat request")?;
    ensure!(response.status().is_success(), "Kubo cat returned {}", response.status());
    Ok(response.bytes().await.context("read Kubo cat bytes")?.to_vec())
}

impl RecoveryState {
    pub fn write_claimed(&self, path: &Path) -> Result<()> {
        ensure!(path.is_absolute(), "recovery state path must be absolute");
        ensure!(self.schema == "ipfs3-cluster-recovery-v1", "unexpected recovery state schema");
        ensure!(self.source == "s3-replication-retention-v1", "unexpected recovery state source");
        ensure!(!self.cid.is_empty() && !self.body.is_empty(), "recovery state CID/body must be nonempty");
        ensure!(self.peer_set_sha256.len() == 64 && self.peer_set_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()), "recovery state peer digest must be 64-hex");
        let bytes = serde_json::to_vec(self).context("serialize recovery state")?;
        let mut file = OpenOptions::new().read(true).write(true).open(path)
            .context("open outer-owned recovery state")?;
        let metadata = file.metadata().context("inspect outer-owned recovery state")?;
        ensure!(metadata.is_file() && metadata.len() == 0, "outer-owned recovery state must be an empty regular file");
        file.write_all(&bytes).context("write recovery state")?;
        file.sync_all().context("sync recovery state")
    }

    pub fn read(path: &Path) -> Result<Self> {
        ensure!(path.is_absolute(), "recovery state path must be absolute");
        let state: Self = serde_json::from_slice(&std::fs::read(path).context("read recovery state")?)
            .context("decode recovery state")?;
        ensure!(state.schema == "ipfs3-cluster-recovery-v1", "unexpected recovery state schema");
        ensure!(state.source == "s3-replication-retention-v1", "unexpected recovery state source");
        ensure!(!state.cid.is_empty() && !state.body.is_empty(), "recovery state CID/body must be nonempty");
        ensure!(state.peer_set_sha256.len() == 64 && state.peer_set_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()), "recovery state peer digest must be 64-hex");
        Ok(state)
    }
}
```

- [ ] **Step 6: Create separate S3 helpers, the no-write topology gate, and the production-surface direct-proxy test**

Create `tests/cluster.rs` with this exact helper surface so `tests/support/mod.rs` stays unchanged:

```rust
#[allow(dead_code)]
mod support;
#[path = "support/cluster.rs"]
mod cluster_support;

use cluster_support::{
    ClusterClient, RecoveryState, kubo_cat, peer_set_digest, wait_for_shared_two_peer_view,
    wait_for_two_pinned, wait_until_not_fully_pinned,
};
use anyhow::{Result as AnyResult, anyhow, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use http::{HeaderMap, Method};
use ipfs_s3_gateway::kubo::{
    KuboClient,
    add::stream_add,
    cat::stream_cat,
    pin::pin_add,
};
use s3::{
    bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials,
    error::S3Error, region::Region,
};
use std::{
    future::Future,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use support::sigv4::send_sigv4;

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const PROXY_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_DOWNLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_SEQUENCE_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn endpoint(name: &str) -> String {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    let value = value.trim_end_matches('/').to_owned();
    assert!(value.starts_with("http://127.0.0.1:"), "{name} must be loopback HTTP");
    value
}

fn state_path() -> PathBuf {
    let path = PathBuf::from(std::env::var("IPFS_S3_CLUSTER_STATE_PATH")
        .expect("IPFS_S3_CLUSTER_STATE_PATH is required"));
    assert!(path.is_absolute(), "Cluster recovery state path must be absolute");
    path
}

fn credentials() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None).unwrap()
}

fn region(endpoint: &str) -> Region {
    Region::Custom { region: "us-east-1".to_owned(), endpoint: endpoint.to_owned() }
}

fn bucket_at(endpoint: &str, name: &str) -> Box<Bucket> {
    Bucket::new(name, region(endpoint), credentials()).unwrap().with_path_style()
}

async fn s3_call<T, F>(label: &str, future: F) -> Result<T, S3Error>
where
    F: Future<Output = Result<T, S3Error>>,
{
    tokio::time::timeout(S3_TIMEOUT, future).await
        .unwrap_or_else(|_| panic!("S3 operation timed out: {label}"))
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("cl-{scenario}-{}-{nanos:x}-{counter:x}", std::process::id());
    assert!(name.len() <= 63, "bucket name exceeds S3 limit");
    name
}

async fn create_bucket(endpoint: &str, scenario: &str) -> (String, Box<Bucket>) {
    let name = unique_bucket(scenario);
    let response = s3_call(
        "create bucket",
        Bucket::create_with_path_style(&name, region(endpoint), credentials(), BucketConfiguration::default()),
    ).await.expect("create bucket must succeed");
    assert_eq!(response.response_code, 200);
    (name.clone(), bucket_at(endpoint, &name))
}

fn etag(headers: &std::collections::HashMap<String, String>) -> String {
    headers.get("etag").or_else(|| headers.get("ETag"))
        .or_else(|| headers.get("e-tag")).cloned().expect("PUT response must include ETag")
        .trim_matches('"').to_owned()
}
```

Add these two Tokio tests after the helpers. The first is an exact no-write gate: it touches only Cluster REST, requires exact `1.1.6` release core through `is_release_1_1_6_version`, accepts only valid optional build metadata, and prints only normalized count/core. The second talks only to loopback Cluster A proxy `IPFS_S3_CLUSTER_A_PROXY_URL` through the repository's public production `KuboClient`/`stream_add`/`pin_add`/`stream_cat` surfaces. It has no S3/PostgreSQL setup or cleanup, wraps the complete sequence in an honest outer timeout because the upload client intentionally has no whole-request deadline, and is the only test whose failure may be labeled proxy compatibility.

```rust
#[tokio::test]
async fn cluster_topology_converges() {
    let cluster_a = ClusterClient::new(&endpoint("IPFS_S3_CLUSTER_A_REST_URL")).unwrap();
    let cluster_b = ClusterClient::new(&endpoint("IPFS_S3_CLUSTER_B_REST_URL")).unwrap();
    let peers = wait_for_shared_two_peer_view(&cluster_a, &cluster_b, CONVERGENCE_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("TOPOLOGY_CONVERGENCE failed: {error}"));
    assert_eq!(peers.len(), 2);
    eprintln!("TOPOLOGY_CONVERGENCE PASS peers=2 version=1.1.6");
}

#[tokio::test]
async fn cluster_proxy_compatibility() {
    let result: AnyResult<()> = match tokio::time::timeout(PROXY_SEQUENCE_TIMEOUT, async {
        let proxy = endpoint("IPFS_S3_CLUSTER_A_PROXY_URL");
        ensure!(proxy == "http://127.0.0.1:59103", "proxy_endpoint_not_exact_validation_loopback");
        let client = KuboClient::new_with_timeouts(
            proxy,
            PROXY_CONTROL_TIMEOUT,
            PROXY_DOWNLOAD_IDLE_TIMEOUT,
        );
        let body = Bytes::from_static(b"cluster-proxy-add-pin-cat-compatibility-v1");
        let split = body.len() / 2;
        let source = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(body.slice(..split)),
            Ok::<Bytes, std::io::Error>(body.slice(split..)),
        ]);
        let cid = stream_add(&client, source, 1).await
            .map_err(|_| anyhow!("production_stream_add_failed"))?;
        pin_add(&client, &cid).await
            .map_err(|_| anyhow!("production_pin_add_failed"))?;
        let cat = stream_cat(&client, &cid, None).await
            .map_err(|_| anyhow!("production_stream_cat_failed"))?;
        tokio::pin!(cat);
        let mut actual = Vec::new();
        while let Some(chunk) = cat.next().await {
            let chunk = chunk.map_err(|_| anyhow!("production_stream_cat_chunk_failed"))?;
            ensure!(
                actual.len().saturating_add(chunk.len()) <= body.len(),
                "production_stream_cat_length_mismatch"
            );
            actual.extend_from_slice(&chunk);
        }
        ensure!(actual.as_slice() == body.as_ref(), "production_stream_cat_bytes_mismatch");
        Ok(())
    }).await {
        Ok(inner) => inner,
        Err(_) => Err(anyhow!("proxy_compatibility_sequence_timeout")),
    };
    result.unwrap_or_else(|error| panic!("PROXY_COMPATIBILITY failed: {error}"));
    eprintln!("PROXY_COMPATIBILITY PASS direct_add_pin_cat=true");
}
```

The direct proxy CID remains a function-local opaque value: do not print it, place it in `RecoveryState`, compare it to the later S3 ETag/CID, or unpin it. Disposable validation volumes provide final cleanup. The later `cluster_replication_and_retention` test—not compatibility—uses the S3 helpers, `Method`, `HeaderMap`, `send_sigv4`, `RecoveryState`, `state_path`, digest, and polling helpers. Do not execute native tools or add a fallback. Preserve the confirmed pre-fix result as RED: add and pin/add returned 200, cat returned 502, and sanitized Cluster A diagnostics showed proxy forwarding to container-local `127.0.0.1:5001` even though the connector pin succeeded. Workflow/local execution must run `cluster_topology_converges` first; only a complete subsequent add→pin→cat PASS through `cluster_proxy_compatibility` is GREEN and may unlock replication.

- [ ] **Step 7: Implement replication, retention, outage, and same-volume recovery tests**

Add three S3/replication tests using these exact environment names: `IPFS_S3_CLUSTER_GATEWAY_ENDPOINT`, `IPFS_S3_CLUSTER_A_REST_URL`, `IPFS_S3_CLUSTER_B_REST_URL`, `IPFS_S3_CLUSTER_KUBO_A_URL`, `IPFS_S3_CLUSTER_KUBO_B_URL`, and `IPFS_S3_CLUSTER_STATE_PATH`. They do not consume `IPFS_S3_CLUSTER_A_PROXY_URL`; that endpoint belongs only to `cluster_proxy_compatibility`.

`cluster_replication_and_retention` performs, in order:

1. Re-probe both REST APIs through `wait_for_shared_two_peer_view`; exact 204 health/version/membership errors remain replication-topology preconditions, never proxy compatibility errors, and the two IDs remain in memory only.
2. Create a unique `cl-repl-...` bucket; PUT `retained.bin` with constant `b"ipfs3-cluster-retained-replication-v1"`; require `200`, nonempty CID ETag, and immediate S3 GET exact bytes.
3. Call `wait_for_two_pinned` against Cluster A and Cluster B independently; both returned `ReplicationEvidence` values must equal exact allocations and pinned peers matching the two discovered IDs.
4. Require direct Kubo A and Kubo B cat to equal the body.
5. S3 DELETE must return `204`; a signed HEAD through `support::sigv4::send_sigv4` must return exactly `404`; delete the now-empty bucket.
6. Re-read Cluster A allocation and require CID, factors `2/2`, and exact two allocations; re-run two-pinned evidence and require Kubo-B cat exact bytes.
7. Compute `peer_set_digest(&peers)` and call `write_claimed` with `RecoveryState { schema: "ipfs3-cluster-recovery-v1", source: "s3-replication-retention-v1", cid, body, peer_set_sha256 }`; require the outer-owned file to exist and be empty. The exact source discriminator prevents the earlier direct-proxy CID from being mistaken for the S3 scenario CID; no identity/CID is emitted.

`cluster_peer_b_outage_contract` reads the owned receipt, calls `wait_until_not_fully_pinned(&cluster_a, &state.cid, &state.peer_set_sha256, Duration::from_secs(90))`, and proves Kubo A still cats exact receipt bytes. The helper requires Cluster A health and the allocation’s exact two-peer digest while accepting only a completed non-success/pending pin response or an incomplete known pinned set as outage evidence. It does not call Cluster B or Kubo B and never emits identities.

`cluster_peer_b_restart_recovery` reads the same receipt, re-converges both REST views, requires `peer_set_digest(&peers) == state.peer_set_sha256`, requires both Cluster A and B to return exact `2/2` physical-pin evidence, and requires Kubo B cat exact bytes. It contains no S3 PUT, so recovery cannot re-upload the object, and reports only counts/categories.

- [ ] **Step 8: Extend the dependency-free static target contract and compile GREEN**

Immediately before the sole final PASS line, insert checks in `tests/cluster.Tests.ps1` for exactly one of each five Tokio live test names and exactly one plain `cluster_support::release_version_validator_accepts_exact_release_and_build_metadata`, all seven test environment names, all helper/type names, exact `ProbeError::{Transient,Terminal}` branching, `is_release_1_1_6_version(&peer.version)` with no direct equality/inequality comparison of `peer.version` to bare `1.1.6`, `GET /peers` NDJSON line parsing, `replication_factor_min/max`, `peer_map`, `status == "pinned"`, 204 health, both Kubo endpoints, signed HEAD 404, `write_claimed`, existing-empty receipt checks, `peer_set_sha256`, schema `ipfs3-cluster-recovery-v1`, and source discriminator `s3-replication-retention-v1`. Require imports of public production `ipfs_s3_gateway::kubo::KuboClient`, `add::stream_add`, `pin::pin_add`, and `cat::stream_cat`.

The version-helper static contract requires exact core prefix `1.1.6`, optional `+`, nonempty dot-separated metadata identifiers, ASCII alphanumeric-or-hyphen bytes only, and normalized topology output literal `version=1.1.6`. It must require every accepted/rejected example shown in Step 4, forbid logging/formatting `peer.version` or metadata, and retain terminal category `peer_version_not_release_1_1_6` for every rejected representation. The plain unit test contains no Tokio attribute/environment access and its fully qualified exact name cannot match any of the five live `--exact` commands.

Implement that count/representation lock with this exact static block after loading `$Rust = Read-NormalizedText $RustPath` and `$RustSupport = Read-NormalizedText $RustSupportPath`:

```powershell
$liveTests = @(
    "cluster_topology_converges",
    "cluster_proxy_compatibility",
    "cluster_replication_and_retention",
    "cluster_peer_b_outage_contract",
    "cluster_peer_b_restart_recovery"
)
Assert-True (([regex]::Matches($Rust, '(?m)^#\[tokio::test\]$')).Count -eq 5) "Cluster target must define exactly five Tokio live tests"
foreach ($testName in $liveTests) {
    $escaped = [regex]::Escape($testName)
    Assert-True (([regex]::Matches($Rust, "(?m)^async fn $escaped\(\) \{")).Count -eq 1) "Expected one Tokio live test: $testName"
}
Assert-True (([regex]::Matches($RustSupport, '(?m)^#\[test\]$')).Count -eq 1) "Cluster support must define exactly one plain unit test"
Assert-True (([regex]::Matches($RustSupport, '(?m)^fn release_version_validator_accepts_exact_release_and_build_metadata\(\) \{$')).Count -eq 1) "Release-version unit test count changed"
Assert-Contains $RustSupport 'fn is_release_1_1_6_version(version: &str) -> bool' "Missing release-version helper"
Assert-Contains $RustSupport 'version.strip_prefix("1.1.6")' "Release helper must lock the exact core prefix"
Assert-Contains $RustSupport 'if suffix.is_empty() { return true; }' "Bare 1.1.6 must remain valid"
Assert-Contains $RustSupport "suffix.strip_prefix('+')" "Optional build metadata must begin with plus"
Assert-Contains $RustSupport '!metadata.is_empty()' "Build metadata must be nonempty"
Assert-Contains $RustSupport "metadata.split('.')" "Build metadata must use dot-separated identifiers"
Assert-Contains $RustSupport '!identifier.is_empty()' "Build identifiers must be nonempty"
Assert-Contains $RustSupport "byte.is_ascii_alphanumeric() || byte == b'-'" "Build identifiers must be ASCII alphanumeric or hyphen"
Assert-Contains $RustSupport 'if !is_release_1_1_6_version(&peer.version)' "Peer probing must use the release-version helper"
Assert-Contains $RustSupport 'ProbeError::Terminal("peer_version_not_release_1_1_6")' "Rejected versions must retain the sanitized category"
Assert-NotMatches $RustSupport 'peer\.version\s*(==|!=)\s*"1\.1\.6"' "Direct peer-version byte equality is forbidden"
Assert-NotMatches $RustSupport '(?s)(format!|panic!|println!|eprintln!)\([^)]*peer\.version' "Raw peer versions must never be emitted"
foreach ($accepted in @('1.1.6', '1.1.6+gitabcdef', '1.1.6+git-ABC.42', '1.1.6+0', '1.1.6+a-b.c9')) {
    Assert-Contains $RustSupport "`"$accepted`"" "Accepted release-version example is missing: $accepted"
}
foreach ($rejected in @('1.1.5', '1.1.7', '1.1.60', '1.1.6-rc.1', '1.1.6+', '1.1.6+.git', '1.1.6+git.', '1.1.6+git..abcdef', '1.1.6+git abcdef', '1.1.6+git/abcdef', '1.1.6+git_abcdef', '1.1.6++gitabcdef', '1.1.6gitabcdef', '1.1.6.other', ' 1.1.6', '1.1.6 ')) {
    Assert-Contains $RustSupport "`"$rejected`"" "Rejected release-version example is missing: $rejected"
}
Assert-Contains $Rust 'TOPOLOGY_CONVERGENCE PASS peers=2 version=1.1.6' "Topology output must contain only the normalized core"
Assert-NotMatches $Rust '(?s)(println!|eprintln!|panic!)\([^)]*(build|metadata|peer\.version)' "Runtime output must not reveal build metadata"
```

Copy the existing comment/string-aware `Get-BracedBlock` helper from `tests/multi-gateway.Tests.ps1` and extract `cluster_proxy_compatibility`. Require that block to use only `IPFS_S3_CLUSTER_A_PROXY_URL`, exact `KuboClient::new_with_timeouts(proxy, PROXY_CONTROL_TIMEOUT, PROXY_DOWNLOAD_IDLE_TIMEOUT)`, a bounded multi-chunk `futures_util::stream::iter` byte source, `stream_add(&client, source, 1)`, `pin_add(&client, &cid)`, `stream_cat(&client, &cid, None)`, bounded incremental exact-byte collection, and an outer `tokio::time::timeout(PROXY_SEQUENCE_TIMEOUT, ...)` enclosing the entire add/pin/cat sequence. Require every production-call/stream error to map to a fixed sanitized category before the final panic. Reject from that block `IPFS_S3_CLUSTER_GATEWAY_ENDPOINT`, `create_bucket`, `s3_call`, `Bucket`, `put_object`, `get_object`, `delete_object`, `state_path`, `RecoveryState`, PostgreSQL text, receipt writes, and tracing-subscriber initialization.

Reject anywhere in `tests/cluster.rs` all hand-built proxy protocol surfaces: `reqwest::Client`, `reqwest::multipart`, `multipart::`, `Part::bytes`, `Part::stream`, `.file_name(`, `ReqwestBody`, `ProxyAddEvent`, `ProxyPinResponse`, `serde_json::from_slice`, `url::Url`, literal `/api/v0/add`, literal `/api/v0/pin/add`, literal `/api/v0/cat`, and stale custom helpers `bounded_proxy_client`, `proxy_add_pin_false`, `proxy_pin_add`, `proxy_cat`, `observe_add_record`, or `read_bounded_proxy_body`. Task 1's protected hashes and source assertions remain the sole lock for production `ReqwestBody::wrap_stream`, `multipart::Part::stream`, filename `object`, MIME `application/octet-stream`, exact add query/NDJSON final-root parser, pin/add, and cat handling. Require topology before compatibility, compatibility before the separately labeled replication command, and only `cluster_replication_and_retention` to call `write_claimed`. Also reject `std::process::Command`, `Command::new`, Docker/AWS/mc/rclone execution, production/Cluster-B proxy publication, `unwrap_or_default()`, catch-all `if let Ok` polling, raw response formatting, serialized `peer_ids`, identity/CID-bearing `panic!`/`assert!`/`eprintln!`, unpin calls, and any source/Cargo hash change.

```powershell
cargo test --test cluster --no-run
if ($LASTEXITCODE -ne 0) { throw "Cluster Rust target did not compile" }
cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact
if ($LASTEXITCODE -ne 0) { throw "Cluster release-version unit contract failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster Rust static contract failed" }
```

Expected: compile, one pure version unit PASS, and static PASS using existing dependencies. No live test is selected by the unit command. Runtime remains the recorded topology RED until the isolated topology gate turns GREEN in Task 4.

- [ ] **Step 9: Record Task 2 receipt without a Git write**

Record `Task 2 COMPILE/STATIC: PASS`, `VERSION UNIT: PASS`, exactly five Tokio live tests in topology → production-surface direct-proxy compatibility → separate S3 replication → outage → recovery order, one plain unit test, both Rust file paths, valid optional build-metadata contract, normalized output, preserved terminal category, protected production add/pin/cat source shapes, no hand-built protocol, 10s control/download bounds plus a 30s outer compatibility bound, explicit transient/terminal polling, identity/CID-free output, S3-only receipt provenance, and 120s convergence. Retain the historical version RED and confirmed topology GREEN. Also retain `DIRECT PROXY: RED add=200 pin=200 cat=502 (container-local proxy target)`; do not run replication or call compatibility GREEN until Task 4 completes add→pin→cat after Task 1's proxy-node static GREEN. Do not stage or commit.

---

### Task 3: Add the sixth independent blocking release job and update shared static contracts

**Files:**
- Modify/Test: `.github/workflows/release-validation.yml`
- Modify/Test: `tests/release-validation.Tests.ps1`
- Modify/Test: `tests/postgres-production-baseline.Tests.ps1`
- Modify/Test: `tests/multi-gateway.Tests.ps1`
- Modify/Test: `tests/cluster.Tests.ps1`

**Interfaces:**
- Consumes: Task 1 production/validation Compose pair with paired Cluster targets, fixed loopback gateway mapping, exact acknowledgement guard, and publication static RED→GREEN; Task 2 five Tokio live tests plus one plain unit; Exact Release Workflow Reference 1-9.
- Produces: independent blocking `cluster-pinset-replication`; six-job workflow; exact `IPFS_S3_GATEWAY_BIND=127.0.0.1` validator; release → PG → multi → cluster → client order; version unit, topology GREEN, proxy GREEN, then S3 replication; sanitized diagnostics; owned cleanup; hosted `NOT RUN`.

- [ ] **Step 1: Apply Reference 1 and run the four-command causal RED**

Make only the exact five-to-six job and four-to-five static-command expectation changes described in Reference 1. Run its complete PowerShell RED block and require all four scripts to fail for the absent job/command.

- [ ] **Step 2: Add the exact job environment and setup from Reference 2**

Insert the complete YAML in Reference 2 verbatim. Confirm the job has no `needs`, no job-level `continue-on-error`, exact 60-minute timeout, Rust 1.92, and runner-temp recovery receipt.

- [ ] **Step 3: Add both fail-closed configuration blocks from Reference 3**

Insert the exact Compose version and seven-variable removal/restoration blocks as explicit `shell: pwsh` steps. Require the bind acknowledgement with case-sensitive `-cne "127.0.0.1"`; do not retain the old finite wildcard-denylist check, render config, use an env file, or output any value.

- [ ] **Step 4: Add ownership and startup from Reference 4**

Insert the complete project/resource/port preflight, CreateNew receipt claim, and exact six-service startup blocks. Project OWNED is written only after resource/port preflight passes; receipt-owned is written only after CreateNew succeeds; ATTEMPTED is written immediately before `up`.

- [ ] **Step 5: Add the ten unit/live/failure/recovery steps from Reference 5**

Insert the exact YAML in the displayed order. The pure unit runs first and cannot select a live test. `cluster_topology_converges` is no-write and preserves normalized release-core GREEN before it writes `CLUSTER_TOPOLOGY_GREEN=true`; compatibility fails closed without that marker. The first write is `cluster_proxy_compatibility` through the production public Kubo surfaces under its outer timeout. Only complete add→pin→cat success writes `CLUSTER_PROXY_COMPATIBILITY_GREEN=true`; replication fails closed without it. Preserve the confirmed 502 run as RED rather than treating add/pin 200 as partial PASS. Topology, proxy, and S3 failures retain their distinct labels, and any failure still reaches later `always()` diagnostics/cleanup.

- [ ] **Step 6: Add diagnostics and cleanup from Reference 6**

Prepend the exact sanitizer to all three diagnostics bodies and place final sanitized six-service logs before cleanup. Require native log exit capture before sanitized emission, failed logs to remain blocking, all Docker residual exits to be collected, Docker cleanup only for project-owned+attempted, and state deletion only for the independent receipt-owned marker.

- [ ] **Step 7: Implement the exact static assertions and AST parsing from Reference 7**

Keep every existing job-body assertion. Derive and assert the final Cluster PowerShell block count, parse each block, and reject all listed unsafe syntax/surfaces.

- [ ] **Step 8: Run the exact five-command GREEN sequence from Reference 8**

Expected: all five scripts print PASS in the required order, all workflow PowerShell parses, existing profile hashes remain fixed, and no Docker command runs from a static script.

- [ ] **Step 9: Record the Reference 9 receipt without a Git write**

Record the exact six jobs, five static commands, and hosted `NOT RUN` boundary. Do not stage, commit, push, or tag.

---

### Task 4: Execute one owned unique-project local workflow-parity live run

**Files:**
- Test only: all Task 1-3 implementation surfaces
- Runtime-only temp artifact: one unique recovery JSON under `[IO.Path]::GetTempPath()`, deleted before success
- No repository edit while this task runs

**Interfaces:**
- Consumes: Task 3 hosted sequence and 16 process environment names; seven validation ports; Task 1 paired Cluster targets plus fixed-loopback/guard static receipt; Task 2's five Tokio live tests plus one unit; prior RED→GREEN evidence; independent ownership, sanitizer, and same-volume restart contracts.
- Produces: one fail-closed LOCAL matrix with a separately checked gateway image build whose output is excluded from bind assertions, followed by isolated `IPFS_S3_GATEWAY_BIND=0.0.0.0` guard execution proving exit64/no value leak/no service ports/zero residuals before the unchanged valid path; topology/proxy/replication/outage/recovery evidence; owned cleanup; environment restore `16/16`; aggregate zero errors.

- [ ] **Step 1: Run no-Docker static/compile preflight and require a clean index**

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster static preflight failed" }
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Release static preflight failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL shared contract preflight failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Multi-gateway shared contract preflight failed" }
cargo test --test cluster --no-run
if ($LASTEXITCODE -ne 0) { throw "Cluster target compile preflight failed" }
cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact
if ($LASTEXITCODE -ne 0) { throw "Cluster release-version unit preflight failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not stage changes" }
```

- [ ] **Step 2: Start one PowerShell process, snapshot all 16 environment entries, and define isolated initialization**

Run Steps 2-5 as one PowerShell script in one process; each displayed block is concatenated in order. Begin with this exact state and initialization function:

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$environmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "COMPOSE_PROJECT_NAME",
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_CLUSTER_SECRET",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT",
    "IPFS_S3_CLUSTER_GATEWAY_ENDPOINT",
    "IPFS_S3_CLUSTER_A_REST_URL",
    "IPFS_S3_CLUSTER_B_REST_URL",
    "IPFS_S3_CLUSTER_A_PROXY_URL",
    "IPFS_S3_CLUSTER_KUBO_A_URL",
    "IPFS_S3_CLUSTER_KUBO_B_URL",
    "IPFS_S3_CLUSTER_STATE_PATH"
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [pscustomobject]@{
        Exists = Test-Path -LiteralPath "Env:$name"
        Value = [Environment]::GetEnvironmentVariable($name, "Process")
    }
}
$project = "ipfs3-cl-local-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$guardProject = "$project-bind-guard"
$statePath = Join-Path ([IO.Path]::GetTempPath()) "$project-recovery.json"
$compose = @(
    "--project-name", $project,
    "-f", "docker-compose.cluster.yml",
    "-f", "tests/compose.cluster-validation.yml"
)
$guardCompose = @(
    "--project-name", $guardProject,
    "-f", "docker-compose.cluster.yml"
)
$owned = $false
$attempted = $false
$stateReceiptOwned = $false
$topologyGreen = $false
$proxyCompatibilityGreen = $false
$bindGuardVerified = $false
$primaryErrors = [Collections.Generic.List[string]]::new()
$cleanupErrors = [Collections.Generic.List[string]]::new()
$restoreErrors = [Collections.Generic.List[string]]::new()
$restoredCount = 0

function Protect-ClusterDiagnosticLine {
    param([AllowEmptyString()][string]$Line)
    $safe = $Line
    $safe = [regex]::Replace($safe, '(?i)((?:"?)(?:PeerID|peer_id|peer-id)(?:"?)\s*[:=]\s*)(?:"[^"]*"|''[^'']*''|[^\s,;]+)', '$1[REDACTED_PEER_ID]')
    $safe = [regex]::Replace($safe, '(?i)(/p2p/)[1-9A-HJ-NP-Za-km-z]+', '$1[REDACTED_PEER_ID]')
    $safe = [regex]::Replace($safe, '(?<![1-9A-HJ-NP-Za-km-z])12D3Koo[1-9A-HJ-NP-Za-km-z]{20,}(?![1-9A-HJ-NP-Za-km-z])', '[REDACTED_PEER_ID]')
    if ($safe -match '(?i)\b(?:peer(?:[_-]?id)?|peerid|identity|peerstore)\b') {
        $safe = [regex]::Replace($safe, '(?<![1-9A-HJ-NP-Za-km-z])Qm[1-9A-HJ-NP-Za-km-z]{44}(?![1-9A-HJ-NP-Za-km-z])', '[REDACTED_PEER_ID]')
    }
    $safe
}

function Write-SanitizedClusterDiagnostics {
    param([Parameter(Mandatory)][string[]]$ComposeArgs, [Parameter(Mandatory)][string]$FailureMessage)
    $rawDiagnosticLines = @(docker compose @ComposeArgs logs --no-color postgres kubo-a kubo-b cluster-a cluster-b gateway 2>&1)
    $diagnosticExit = $LASTEXITCODE
    foreach ($rawLine in $rawDiagnosticLines) {
        [Console]::Out.WriteLine((Protect-ClusterDiagnosticLine -Line "$rawLine"))
    }
    if ($diagnosticExit -ne 0) { throw "$FailureMessage (exit=$diagnosticExit)" }
}

function Test-GatewayBindGuard {
    $guardErrors = [Collections.Generic.List[string]]::new()
    $guardOwned = $false
    $savedBind = [Environment]::GetEnvironmentVariable("IPFS_S3_GATEWAY_BIND", "Process")
    try {
        $guardContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $guardContainerExit = $LASTEXITCODE
        $guardNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $guardNetworkExit = $LASTEXITCODE
        $guardVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        $guardVolumeExit = $LASTEXITCODE
        if ($guardContainerExit -ne 0 -or $guardNetworkExit -ne 0 -or $guardVolumeExit -ne 0) {
            throw "Guard-project ownership preflight query failed"
        }
        if (($guardContainers.Count + $guardNetworks.Count + $guardVolumes.Count) -ne 0) {
            throw "Guard project is not empty"
        }
        $guardOwned = $true
        docker compose @guardCompose build gateway
        $guardBuildExit = $LASTEXITCODE
        if ($guardBuildExit -ne 0) { throw "Guard gateway image build failed" }
        $env:IPFS_S3_GATEWAY_BIND = "0.0.0.0"
        $guardOutput = @(docker compose @guardCompose run --rm --no-deps gateway 2>&1)
        $guardExit = $LASTEXITCODE
        $guardText = $guardOutput -join "`n"
        if ($guardText.Contains("0.0.0.0", [StringComparison]::Ordinal)) {
            $guardErrors.Add("Guard output exposed the rejected acknowledgement")
        }
        foreach ($sensitiveName in @("POSTGRES_PASSWORD", "IPFS_S3_MASTER_KEY", "IPFS_S3_CLUSTER_SECRET")) {
            $sensitiveValue = [Environment]::GetEnvironmentVariable($sensitiveName, "Process")
            if (-not [string]::IsNullOrEmpty($sensitiveValue) -and $guardText.Contains($sensitiveValue, [StringComparison]::Ordinal)) {
                $guardErrors.Add("Guard output exposed a protected value")
            }
        }
        if ($guardExit -ne 64) { $guardErrors.Add("Guard mismatch did not return exit 64") }
    } catch {
        $guardErrors.Add("Guard execution did not complete")
    } finally {
        [Environment]::SetEnvironmentVariable("IPFS_S3_GATEWAY_BIND", $savedBind, "Process")
        if ([Environment]::GetEnvironmentVariable("IPFS_S3_GATEWAY_BIND", "Process") -cne $savedBind) {
            $guardErrors.Add("Guard bind restoration changed the valid acknowledgement")
        }
        if ($guardOwned) {
            $guardDownOutput = @(docker compose @guardCompose down --volumes --remove-orphans 2>&1)
            $guardDownExit = $LASTEXITCODE
            if ($guardDownExit -ne 0) { $guardErrors.Add("Guard-project cleanup failed") }
            $guardContainers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            $guardContainerExit = $LASTEXITCODE
            $guardNetworks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            $guardNetworkExit = $LASTEXITCODE
            $guardVolumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$guardProject" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            $guardVolumeExit = $LASTEXITCODE
            if ($guardContainerExit -ne 0 -or $guardNetworkExit -ne 0 -or $guardVolumeExit -ne 0) {
                $guardErrors.Add("Guard-project residual query failed")
            }
            if (($guardContainers.Count + $guardNetworks.Count + $guardVolumes.Count) -ne 0) {
                $guardErrors.Add("Guard-project residual resources remain")
            }
        }
    }
    if ($guardErrors.Count -ne 0) { throw ($guardErrors -join "; ") }
    $script:bindGuardVerified = $true
    Write-Host "gateway bind guard: PASSED build=pass exit=64 guard_output=safe residual=0"
}

function Initialize-ClusterValidation {
    docker version
    if ($LASTEXITCODE -ne 0) { throw "Docker runtime is unavailable" }
    docker compose version
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose v2 is unavailable" }
    $versionText = (docker compose version --short).Trim()
    $versionMatch = [regex]::Match($versionText, '^v?(?<core>\d+\.\d+\.\d+)(?:[-+][0-9A-Za-z.-]+)?$')
    $version = $null
    if (-not $versionMatch.Success -or
        -not [Version]::TryParse($versionMatch.Groups["core"].Value, [ref]$version) -or
        $version -lt [Version]"2.23.1") {
        throw "Docker Compose 2.23.1 or newer is required"
    }

    $env:COMPOSE_DISABLE_ENV_FILE = "1"
    $env:COMPOSE_PROJECT_NAME = $project
    $env:POSTGRES_PASSWORD = "cl-local-$PID"
    $env:IPFS_S3_ACCESS_KEY_ID = "test"
    $env:IPFS_S3_SECRET_ACCESS_KEY = "test"
    $env:IPFS_S3_MASTER_KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    $env:IPFS_S3_CLUSTER_SECRET = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
    $env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
    if ($env:IPFS_S3_GATEWAY_BIND -cne "127.0.0.1") { throw "Local bind acknowledgement must equal exact loopback" }
    $env:IPFS_S3_GATEWAY_PORT = "59100"
    $env:IPFS_S3_CLUSTER_GATEWAY_ENDPOINT = "http://127.0.0.1:59100"
    $env:IPFS_S3_CLUSTER_A_REST_URL = "http://127.0.0.1:59101"
    $env:IPFS_S3_CLUSTER_B_REST_URL = "http://127.0.0.1:59102"
    $env:IPFS_S3_CLUSTER_A_PROXY_URL = "http://127.0.0.1:59103"
    $env:IPFS_S3_CLUSTER_KUBO_A_URL = "http://127.0.0.1:55100"
    $env:IPFS_S3_CLUSTER_KUBO_B_URL = "http://127.0.0.1:55101"
    $env:IPFS_S3_CLUSTER_STATE_PATH = $statePath

    $requiredNames = @(
        "POSTGRES_PASSWORD",
        "IPFS_S3_ACCESS_KEY_ID",
        "IPFS_S3_SECRET_ACCESS_KEY",
        "IPFS_S3_MASTER_KEY",
        "IPFS_S3_CLUSTER_SECRET",
        "IPFS_S3_GATEWAY_BIND",
        "IPFS_S3_GATEWAY_PORT"
    )
    foreach ($name in $requiredNames) {
        $value = [Environment]::GetEnvironmentVariable($name, "Process")
        try {
            Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
            if (Test-Path -LiteralPath "Env:$name") { throw "Required variable removal retained presence: $name" }
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
    if ($LASTEXITCODE -ne 0) { throw "Complete Cluster Compose config failed" }

    $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $containerExit = $LASTEXITCODE
    $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $networkExit = $LASTEXITCODE
    $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $volumeExit = $LASTEXITCODE
    if ($containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0) { throw "Project preflight query failed" }
    if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "Unique project already owns resources: $project" }
    foreach ($port in @(55435, 55100, 55101, 59100, 59101, 59102, 59103)) {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
        try { $listener.Start() } catch { throw "Fixed validation port is occupied: $port" } finally { $listener.Stop() }
    }
    if (Test-Path -LiteralPath $statePath) { throw "Unique recovery receipt already exists" }
    $script:owned = $true
    $receipt = $null
    try {
        $receipt = [IO.File]::Open(
            $statePath,
            [IO.FileMode]::CreateNew,
            [IO.FileAccess]::Write,
            [IO.FileShare]::None
        )
        $receipt.Dispose()
        $script:stateReceiptOwned = $true
    } finally {
        if ($null -ne $receipt) { $receipt.Dispose() }
    }
    if (-not $script:stateReceiptOwned) { throw "Recovery receipt claim did not complete" }
}
```

`Test-GatewayBindGuard` first runs `docker compose @guardCompose build gateway` with an immediate independent exit check; build output is neither captured nor inspected for bind text because legitimate Dockerfile output may contain `ENV IPFS_S3_BIND=0.0.0.0:9000`. Only after build PASS does it toggle the acknowledgement and run `docker compose @guardCompose run --rm --no-deps gateway`. That guard execution has no service ports; only its stdout/stderr is captured for no-value assertions and exact exit64. Cleanup/down/residual checks run after that capture, and exact loopback is restored before normal full-path `up`.

- [ ] **Step 3: Define startup, no-write topology, direct-proxy compatibility, and separate replication/retention execution**

Append this function in the same process. Do not insert another write before the exact compatibility target.

```powershell
function Start-And-Prove-ClusterReplication {
    $script:attempted = $true
    docker compose @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b cluster-a cluster-b gateway
    if ($LASTEXITCODE -ne 0) { throw "Cluster topology did not become healthy" }

    cargo test --test cluster cluster_topology_converges -- --exact --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw "TOPOLOGY_CONVERGENCE_BLOCKER: exact 1.1.6 release-core two-peer topology did not converge"
    }
    $script:topologyGreen = $true
    if (-not $topologyGreen) { throw "Topology GREEN receipt is required before compatibility" }
    cargo test --test cluster cluster_proxy_compatibility -- --exact --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) {
        throw "PROXY_COMPATIBILITY_BLOCKER: stop and revise design; no application fallback is permitted"
    }
    $script:proxyCompatibilityGreen = $true
    if (-not $proxyCompatibilityGreen) { throw "Add-pin-cat proxy GREEN receipt is required before replication" }
    if (-not $stateReceiptOwned) { throw "Owned state receipt is required before replication" }
    cargo test --test cluster cluster_replication_and_retention -- --exact --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Replication and retained-delete test failed" }

    Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Pre-stop diagnostics failed"
}
```

Expected: the topology test accepts the official build-bearing representation but emits only `peers=2 version=1.1.6`, never metadata, and performs no write. Its zero exit preserves the confirmed topology GREEN. Only then does compatibility use the actual public production add(`pin=false`), pin/add, and cat functions through loopback Cluster A proxy `59103`. The prior `add=200 pin=200 cat=502` run remains RED because Cluster A's proxy forwarded cat to container-local `127.0.0.1:5001`; after Task 1 pairs `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` with `kubo-a`, only complete add→pin→cat exit zero sets `$proxyCompatibilityGreen`. No S3/PostgreSQL command executes before that toggle. Topology failures retain topology classification; compatibility failures retain `PROXY_COMPATIBILITY_BLOCKER`; replication owns the later S3 label.

- [ ] **Step 4: Define stop/outage, stopped-peer diagnostics, restart, and no-upload recovery**

Append this function in the same process:

```powershell
function Stop-And-Recover-ClusterPeerB {
    if (-not $stateReceiptOwned) { throw "Owned state receipt is required before outage/recovery validation" }
    docker compose @compose stop cluster-b kubo-b
    if ($LASTEXITCODE -ne 0) { throw "Peer B stop failed" }
    cargo test --test cluster cluster_peer_b_outage_contract -- --exact --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Peer B outage evidence failed" }

    Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Stopped-peer diagnostics failed"
    docker compose @compose start kubo-b cluster-b
    if ($LASTEXITCODE -ne 0) { throw "Peer B same-volume restart failed" }
    cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Peer B recovery evidence failed" }
}
```

Expected: outage accepts no exact undocumented status string, Kubo A still serves bytes, restart retains the same named Kubo/Cluster volumes, and recovery contains no PUT.

- [ ] **Step 5: Make sanitized logs, owned cleanup, every residual query, owned-state deletion, and 16-variable restoration unskippable**

Append this exact control/cleanup tail. It records errors until all cleanup and restoration attempts finish, then emits one aggregate decision.

```powershell
try {
    try {
        Initialize-ClusterValidation
        Test-GatewayBindGuard
        if (-not $bindGuardVerified) { throw "Gateway bind guard receipt is required before the valid full path" }
        Start-And-Prove-ClusterReplication
        Stop-And-Recover-ClusterPeerB
    } catch {
        $primaryErrors.Add($_.Exception.Message)
    } finally {
        if ($attempted) {
            try {
                Write-SanitizedClusterDiagnostics -ComposeArgs $compose -FailureMessage "Final diagnostics failed"
            } catch {
                $cleanupErrors.Add("Final sanitized diagnostics failed: $($_.Exception.Message)")
            }
        }
        if ($owned -and $attempted) {
            $downExit = -1
            try {
                docker compose @compose down --volumes --remove-orphans
                $downExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Owned disposable cleanup could not execute: $($_.Exception.Message)")
            }
            if ($downExit -ne 0) { $cleanupErrors.Add("Owned disposable cleanup exited $downExit") }

            $containers = @()
            $containerExit = -1
            try {
                $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
                $containerExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual container query could not execute: $($_.Exception.Message)")
            }
            if ($containerExit -ne 0) { $cleanupErrors.Add("Residual container query exited $containerExit") }

            $networks = @()
            $networkExit = -1
            try {
                $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
                $networkExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual network query could not execute: $($_.Exception.Message)")
            }
            if ($networkExit -ne 0) { $cleanupErrors.Add("Residual network query exited $networkExit") }

            $volumes = @()
            $volumeExit = -1
            try {
                $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
                $volumeExit = $LASTEXITCODE
            } catch {
                $cleanupErrors.Add("Residual volume query could not execute: $($_.Exception.Message)")
            }
            if ($volumeExit -ne 0) { $cleanupErrors.Add("Residual volume query exited $volumeExit") }
            if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { $cleanupErrors.Add("Project-labelled residual resources remain") }
        }
        if ($stateReceiptOwned) {
            try {
                if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) { throw "Owned recovery state is missing before cleanup" }
                Remove-Item -LiteralPath $statePath -ErrorAction Stop
                if (Test-Path -LiteralPath $statePath) { throw "Owned recovery state remains" }
            } catch {
                $cleanupErrors.Add("Owned recovery-state cleanup failed: $($_.Exception.Message)")
            }
        }
    }
} finally {
    foreach ($name in $environmentNames) {
        try {
            $saved = $savedEnvironment[$name]
            if ($saved.Exists) {
                [Environment]::SetEnvironmentVariable($name, $saved.Value, "Process")
                if (-not (Test-Path -LiteralPath "Env:$name")) { throw "restoration lost prior presence" }
                if ([Environment]::GetEnvironmentVariable($name, "Process") -cne $saved.Value) { throw "restoration changed prior value" }
            } else {
                if (Test-Path -LiteralPath "Env:$name") { Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop }
                if (Test-Path -LiteralPath "Env:$name") { throw "restoration retained a prior-absent provider entry" }
                if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "restoration retained a prior-absent process value" }
            }
            $restoredCount++
        } catch {
            $restoreErrors.Add("${name}: $($_.Exception.Message)")
        }
    }
    if ($restoredCount -ne $environmentNames.Count) {
        $restoreErrors.Add("Environment restoration verified $restoredCount/$($environmentNames.Count), expected 16/16")
    }
}

$allErrors = [Collections.Generic.List[string]]::new()
foreach ($entry in $primaryErrors) { $allErrors.Add("primary: $entry") }
foreach ($entry in $cleanupErrors) { $allErrors.Add("cleanup: $entry") }
foreach ($entry in $restoreErrors) { $allErrors.Add("restore: $entry") }
if ($allErrors.Count -ne 0) {
    Write-Host "cluster local workflow parity: UNVERIFIED"
    throw ($allErrors -join "`n")
}
Write-Host "cluster local workflow parity: PASSED environment_restore=16/16 residual=0"
```

This is the only allowed PASS path. Separate gateway build failure, bind-guard exit other than64, unsafe guard-execution output, guard-project residue, valid acknowledgement other than exact loopback, Docker/runtime/image/network absence, topology/proxy/S3 failure, cleanup uncertainty, residual resources, or restoration below16/16 is `UNVERIFIED`. Build output is never a bind-value assertion surface.

- [ ] **Step 6: Execute the concatenated script exactly once and retain non-secret evidence**

Run the concatenation of Steps 2-5 from repository root. Retain separate build exit zero, then guard execution exit64/output-safe/residual-zero; do not apply bind-value checks to build output. The negative guard execution remains isolated before the unchanged valid path. Do not install, explicitly pull, touch `.env`, kill processes, prune Docker, or retain raw guard output/config/environment/body/identity data.

- [ ] **Step 7: Record the exact LOCAL matrix and hosted boundary**

Record each line separately without IDs/CIDs/build metadata: final-review security RED `reviewer_identity=ca26`; intrinsic publication static RED→GREEN; fixed production mapping `127.0.0.1:${IPFS_S3_GATEWAY_PORT}:9000`; bind acknowledgement exact `127.0.0.1`; separate guard-image build exit0 with build output excluded from bind assertions; isolated mismatch guard execution exit64/output safe/no service-port publication/residual zero/valid acknowledgement restored; historical equality-based topology RED category `peer_version_not_exact_1_1_6`; release-version unit PASS; confirmed normalized topology GREEN; confirmed direct proxy RED `add=200 pin=200 cat=502` with sanitized category `container-local proxy target` and successful connector pin; proxy-node static RED→GREEN; exact Cluster A connector/proxy pair `/dns4/kubo-a/tcp/5001`; exact Cluster B pair `/dns4/kubo-b/tcp/5001`; localhost/cross-wire rejection; Compose parser `>=2.23.1`; config rejection `7/7`; project resources initially zero; receipt path initially absent; project ownership established; receipt CreateNew ownership established; seven ports free including Cluster A proxy `59103`; six services healthy; isolated no-write topology GREEN with health204, normalized `version=1.1.6`, exact same distinct `2/2`, no third peer; no compatibility before topology GREEN; complete production `stream_add`→`pin_add`→`stream_cat` proxy GREEN under30s; no replication before proxy GREEN; direct proxy CID omitted/absent from receipt; S3 immediate GET; exact two allocations/pins; Kubo A/B bytes; DELETE→HEAD404 with allocation/B bytes retained; sanitized pre-stop/stopped/final logs; peer-B same-volume recovery without upload; down success; main residual zero; owned state absent; environment restore16/16; aggregate errors zero. Record hosted `NOT RUN`. Any missing line blocks documentation/review.

---

### Task 5: Update README and only the Cluster ROADMAP checkbox after LOCAL success

**Files:**
- Modify: `README.md`
- Modify: `ROADMAP.md:65-70`
- Modify/Test: `tests/cluster.Tests.ps1`

**Interfaces:**
- Consumes: every Task 4 LOCAL matrix line, zero-residual receipt, exact environment restoration, and `HOSTED cluster-pinset-replication: NOT RUN`.
- Produces: bounded same-host documentation with fixed loopback gateway publication, exact bind acknowledgement, no direct non-loopback support, external TLS/auth reverse-proxy requirement explicitly out of scope, only the Cluster checkbox, and non-destructive shutdown.

- [ ] **Step 1: Add documentation assertions first and capture RED**

Immediately before the sole final PASS line, extend `tests/cluster.Tests.ps1` to require one README section containing: `docker-compose.cluster.yml`; exact six service roles/versions/`2/2`; separate repositories/identities; paired Cluster connector/proxy targets; production gateway fixed to `127.0.0.1:${IPFS_S3_GATEWAY_PORT}:9000`; commands explicitly setting `IPFS_S3_GATEWAY_BIND=127.0.0.1`; direct non-loopback publication unsupported; external access requires a separately secured TLS/auth reverse proxy and that subsystem is out of scope; topology then add→pin→cat gates; physical pin/delete-retention boundaries; production Cluster surfaces internal; validation-only proxy `59103`; same-host/non-HA/single-point/private-swarm limits; and non-destructive shutdown. Require only the Cluster ROADMAP item checked. Reject README claims that an arbitrary/non-wildcard bind drives publication, direct external publication is supported, or the container guard alone prevents host exposure; also reject destructive shutdown, peer-list/raw-log commands, production Cluster proxy publication, and hosted PASS.

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Expected evidence-gated Cluster documentation contract to be RED before README/ROADMAP edits" }
```

Expected: failure points to missing README declaration and/or unchecked Cluster item, not topology/workflow regression.

- [ ] **Step 2: Add exact production setup and bounded peer-readiness guidance**

Add a `### IPFS Cluster pinset replication` subsection after the multi-gateway section. Explain that it is a separate one-gateway profile, not a composition with horizontal scaling. Include these fail-closed PowerShell commands without printing config or secrets:

```powershell
$env:COMPOSE_DISABLE_ENV_FILE = "1"
$postgresPasswordBytes = [byte[]]::new(24)
[Security.Cryptography.RandomNumberGenerator]::Fill($postgresPasswordBytes)
$env:POSTGRES_PASSWORD = [Convert]::ToHexString($postgresPasswordBytes).ToLowerInvariant()
$accessKeyBytes = [byte[]]::new(16)
[Security.Cryptography.RandomNumberGenerator]::Fill($accessKeyBytes)
$env:IPFS_S3_ACCESS_KEY_ID = [Convert]::ToHexString($accessKeyBytes).ToLowerInvariant()
$secretKeyBytes = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($secretKeyBytes)
$env:IPFS_S3_SECRET_ACCESS_KEY = [Convert]::ToHexString($secretKeyBytes).ToLowerInvariant()
$masterKey = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($masterKey)
$env:IPFS_S3_MASTER_KEY = [Convert]::ToHexString($masterKey).ToLowerInvariant()
$clusterSecret = [byte[]]::new(32)
[Security.Cryptography.RandomNumberGenerator]::Fill($clusterSecret)
$env:IPFS_S3_CLUSTER_SECRET = [Convert]::ToHexString($clusterSecret).ToLowerInvariant()
$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
$env:IPFS_S3_GATEWAY_PORT = "9000"
docker compose -f docker-compose.cluster.yml config --quiet
if ($LASTEXITCODE -ne 0) { throw "Cluster production Compose configuration is invalid" }
docker compose -f docker-compose.cluster.yml up --detach --build --wait --wait-timeout 300
if ($LASTEXITCODE -ne 0) { throw "Cluster production topology did not become locally healthy" }
```

Immediately after the commands, state that `IPFS_S3_GATEWAY_BIND=127.0.0.1` is a required acknowledgement rather than a configurable publication address: the Compose mapping itself is fixed to loopback before container creation, and the container guard is only defense-in-depth.

State immediately after the final command: local service health is insufficient. The shipped validation workflow/target proves the profile contract with a no-write topology gate that compares identities only in memory and reports only exact count/version; production automation must enforce an equivalent identity-suppressed exact-two-peer gate before accepting writes. Direct peer-list commands are intentionally omitted because they expose generated identities. The exact two-peer physical pin check remains the supported acceptance boundary, and Compose-network mDNS is only the evidenced same-host discovery behavior.
Also state that all generated credential/key values must be stored in the operator's secret system before the first write and restored unchanged on every restart; in particular, changing the master key makes existing encrypted objects unreadable and changing the Cluster secret breaks membership.

- [ ] **Step 3: Document replication, delete retention, exposure, and failure limits**

State all of the following without broadening scope:

- Gateway add uses existing `pin=false`, then existing pin/add through Cluster A proxy; normal cat is forwarded to Kubo A.
- A successful PUT/immediate GET is local-path compatibility, not replication completion. A CID is replicated only after exact two allocations and tracker `pinned` for both peers.
- Direct Kubo B reads are supported only after that state. The gateway has no B fallback.
- S3 DELETE removes metadata but intentionally does not unpin; Cluster allocation and replicated bytes remain.
- `2/2` trades degraded write availability for deterministic evidence. No write, HA, or multi-host guarantee applies with a peer unavailable.
- PostgreSQL, gateway, Cluster A, Kubo A, and the Docker host remain single points of failure.
- The gateway host publication is fixed loopback; direct non-loopback gateway exposure is unsupported. External access requires a separately secured TLS/auth reverse proxy to that loopback endpoint, and this profile does not implement that subsystem.
- PostgreSQL, Kubo APIs, Cluster REST/proxy/swarm stay internal in production. Validation alone publishes Cluster A proxy as disposable loopback `127.0.0.1:59103`; Cluster B proxy remains internal. Non-loopback Cluster REST/proxy exposure is unsupported.
- `IPFS_S3_CLUSTER_SECRET` protects Cluster membership; it does not create a private Kubo swarm.

- [ ] **Step 4: Add only non-destructive production shutdown**

Include exactly this production shutdown and explicitly warn never to add `--volumes` because all five named volumes carry PostgreSQL, Kubo repositories, or Cluster identities:

```powershell
docker compose -f docker-compose.cluster.yml down --remove-orphans
if ($LASTEXITCODE -ne 0) { throw "Cluster production shutdown failed" }
```

- [ ] **Step 5: Check only the Cluster roadmap item**

Change exactly:

```markdown
- [ ] IPFS Cluster for pinset replication
```

to:

```markdown
- [x] IPFS Cluster for pinset replication
```

Leave `- [ ] Private swarm (swarm.key) for node-to-node communication` unchanged and unchecked.

- [ ] **Step 6: Run docs GREEN and record truthful evidence**

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster documentation contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Documentation whitespace validation failed" }
```

Expected: static PASS and only the bounded section/one checkbox docs diff. Record `Task 5 DOCS: PASS (gated by Task 4 LOCAL ALL PASS)` and `HOSTED cluster-pinset-replication: NOT RUN`. Do not stage or commit.

---

### Task 6: Run final regressions, freeze the exact identity, obtain receipts, and hand off one commit

**Files:**
- Verify all 14 exact final-manifest paths below
- No edit after identity generation unless affected verification, identity, and both reviews restart

**Interfaces:**
- Consumes: Tasks 1-5 receipts including intrinsic publication static/local guard evidence; final-review rejection identity `ca26`; approved spec SHA `56d0d5518ff05f59cacec7f3740a6607c6505e7613086a89545650dfde9202eb`; base HEAD unless explicitly authorized otherwise; LSP; orchestrator-owned Oracle and Reviewer.
- Produces: complete static/live/quality/boundary receipts; unchanged exact 14-path manifest; new canonical identity after the security correction; one common packet explaining why the late guard was insufficient and how fixed mapping closes it; fresh identity-bound Oracle and Reviewer approvals; one integrated commit boundary; no push/tag.

- [ ] **Step 1: Parse and run all five PowerShell contracts**

```powershell
$scripts = @(
    "tests/release-validation.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/multi-gateway.Tests.ps1",
    "tests/cluster.Tests.ps1",
    "tests/client-smoke.Tests.ps1"
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
```

Expected: all five parse and PASS. `tests/cluster.Tests.ps1` also parses every new workflow PowerShell body and proves protected profiles/source interfaces unchanged.

- [ ] **Step 2: Run complete non-Docker Rust, compile, format, and lint gates**

Task 4 is the required live Cluster target receipt. Since application/Cargo and the existing PostgreSQL/multi-gateway topology files are hash-protected unchanged, their applicable local final gate is target compilation; their existing hosted live jobs remain blocking after a later authorized push/PR.

```powershell
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library tests failed" }
cargo test --test integration
if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
cargo test --test cluster --no-run
if ($LASTEXITCODE -ne 0) { throw "Cluster target compile failed after live evidence" }
$releaseVersionOutput = @(cargo test --color never --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact 2>&1)
$releaseVersionExit = $LASTEXITCODE
$releaseVersionOutput | ForEach-Object { [Console]::Out.WriteLine("$_") }
if ($releaseVersionExit -ne 0) { throw "Cluster release-version unit contract regressed" }
$releaseVersionPass = @($releaseVersionOutput | Where-Object {
    "$_" -match '^test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; \d+ filtered out; finished in .+$'
})
if ($releaseVersionPass.Count -ne 1) { throw "Cluster release-version exact filter must execute exactly one passing test" }
cargo test --test multi_gateway --no-run
if ($LASTEXITCODE -ne 0) { throw "Existing multi-gateway target compile regressed" }
cargo test --test postgres_import --no-run
if ($LASTEXITCODE -ne 0) { throw "Existing PostgreSQL target compile regressed" }
cargo check --all-targets
if ($LASTEXITCODE -ne 0) { throw "cargo check failed" }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw "cargo fmt check failed" }
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Clippy failed" }
```

The Task 6 release-version receipt requires both native exit zero and exactly one summary line reporting `1 passed; 0 failed`; an exact filter that matches zero tests is a failure even if Cargo exits zero. Do not run a live target with absent environment variables and call its skip PASS. If an implementation edit occurs after Task 4, rerun Task 4 rather than relying on compilation.

- [ ] **Step 3: Require clean LSP diagnostics on both changed Rust files**

Run `lsp_diagnostics` with severity `all` on `tests/cluster.rs` and `tests/support/cluster.rs`. Expected: zero errors and zero warnings. No other Rust file may be changed; a compile result does not replace this receipt.

- [ ] **Step 4: Recheck spec/protected hashes and review tracked plus untracked whitespace**

```powershell
$specSha = (Get-FileHash -Algorithm SHA256 -LiteralPath "docs/superpowers/specs/2026-08-24-ipfs-cluster-pinset-replication-design.md").Hash.ToLowerInvariant()
if ($specSha -cne "56d0d5518ff05f59cacec7f3740a6607c6505e7613086a89545650dfde9202eb") { throw "Approved Cluster spec changed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tracked whitespace check failed" }
$untrackedForCheck = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Untracked path query failed" }
foreach ($path in $untrackedForCheck) {
    $whitespaceOutput = @(git -c core.autocrlf=false diff --no-index --check -- NUL $path 2>&1)
    $checkExit = $LASTEXITCODE
    if ($checkExit -gt 1) { throw "Untracked whitespace/error check failed: $path" }
    if ($whitespaceOutput.Count -ne 0) { throw "Untracked whitespace finding: $path" }
}
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not stage files before review" }
```

Then run the protected-hash map from Task 1 again and require all 15 values unchanged. The approved spec SHA is authoritative, and every tracked or untracked file must be whitespace-clean.

- [ ] **Step 5: Enforce the exact 14-path changed manifest**

```powershell
$allowed = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.cluster.yml",
    "docs/superpowers/plans/2026-08-24-ipfs-cluster-pinset-replication.md",
    "docs/superpowers/specs/2026-08-24-ipfs-cluster-pinset-replication-design.md",
    "ipfs/cluster.Dockerfile",
    "tests/cluster.Tests.ps1",
    "tests/cluster.rs",
    "tests/compose.cluster-validation.yml",
    "tests/multi-gateway.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/release-validation.Tests.ps1",
    "tests/support/cluster.rs"
)
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Tracked path query failed" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Untracked path query failed" }
$changed = @($tracked + $untracked | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
$unexpected = @($changed | Where-Object { $allowed -cnotcontains $_ })
$missing = @($allowed | Where-Object { $changed -cnotcontains $_ })
if ($unexpected.Count -ne 0) { throw "Out-of-scope changed paths: $($unexpected -join ', ')" }
if ($missing.Count -ne 0) { throw "Required final-manifest paths absent: $($missing -join ', ')" }
if ($changed.Count -ne 14) { throw "Expected exactly 14 changed paths, found $($changed.Count)" }
git status --short
if ($LASTEXITCODE -ne 0) { throw "Git status query failed" }
```

For every untracked path, review actual content with `git diff --no-index -- NUL <path>` and accept native exit `1` as the expected difference. Plain `git diff` is insufficient because the spec, plan, and new implementation files begin untracked.

- [ ] **Step 6: Freeze the canonical working-tree identity**

```powershell
$head = (git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw "HEAD query failed" }
$manifest = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.cluster.yml",
    "docs/superpowers/plans/2026-08-24-ipfs-cluster-pinset-replication.md",
    "docs/superpowers/specs/2026-08-24-ipfs-cluster-pinset-replication-design.md",
    "ipfs/cluster.Dockerfile",
    "tests/cluster.Tests.ps1",
    "tests/cluster.rs",
    "tests/compose.cluster-validation.yml",
    "tests/multi-gateway.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/release-validation.Tests.ps1",
    "tests/support/cluster.rs"
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

Any content or HEAD change invalidates the identity, all post-change verification, and both review receipts.

- [ ] **Step 7: Send one common packet and require identity-bound Oracle plus Reviewer receipts**

The common review packet contains the new identity/14 SHA rows/unchanged spec SHA; `ca26` rejection correction; intrinsic fixed mapping and defense-in-depth guard proof; publication static RED→GREEN; separate guard-image build exit0 with build output explicitly excluded from bind assertions; guard execution exit64/output-safe/no-service-port/residual-zero; the full Task4 matrix; hosted `NOT RUN`; all static/Rust/quality/boundary evidence; paired Cluster targets; no scope expansion; ownership/cleanup/environment receipts; and intended commit boundary. It contains no raw guard output, secrets, identities, CIDs, bodies, or environment values.

Each valid response must state its role, verdict, and exact new identity, for example `ORACLE_RECEIPT identity=<sha> verdict=APPROVE` and `REVIEWER_RECEIPT identity=<sha> verdict=APPROVE`. Identity `ca26` is a rejection record, never an approval receipt. A timeout, partial response, older identity, pre-correction approval, or generic approval is invalid. If either review system is blocked, disclose and stop unless the user explicitly overrides that blocker.

- [ ] **Step 8: Recompute identity, inspect Git boundaries, and create the one authorized integrated commit**

After both receipts, rerun Step 6 and require the identical SHA. The orchestrator—not an implementation subagent—then uses the user's existing authorization for exactly one commit:

```powershell
git status --short
if ($LASTEXITCODE -ne 0) { throw "Pre-commit status query failed" }
git diff --stat
if ($LASTEXITCODE -ne 0) { throw "Pre-commit diff stat failed" }
git log --oneline -10
if ($LASTEXITCODE -ne 0) { throw "Recent history query failed" }
git add -- $manifest
if ($LASTEXITCODE -ne 0) { throw "Exact manifest staging failed" }
$staged = @(git diff --cached --name-only | Sort-Object)
if ($LASTEXITCODE -ne 0) { throw "Staged path query failed" }
if (($staged -join "`n") -cne (($manifest | Sort-Object) -join "`n")) { throw "Staged paths differ from exact manifest" }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged diff contains whitespace findings" }
git diff --cached --stat
if ($LASTEXITCODE -ne 0) { throw "Staged diff stat failed" }
git commit -m "feat: add IPFS Cluster pinset replication" -m "Add the six-service Cluster topology, bounded replication and peer-recovery validation, release gate, and evidence-backed operator documentation."
if ($LASTEXITCODE -ne 0) { throw "Integrated Cluster commit failed" }
```

Do not amend, push, tag, or include another path. If either receipt is absent or stale, do not stage or commit.

- [ ] **Step 9: Return final evidence without overstating hosted status**

Return task/step/test counts; exact 14-path manifest; spec/plan SHA256; `ca26` rejection correction note; publication/proxy RED→GREEN and isolated guard receipts; new review identity and both fresh approvals; full LOCAL/static/Rust/quality/LSP/diff evidence; pre/post-commit status and commit hash if created. Keep hosted `NOT RUN`; a later authorized hosted PASS is required before merge/release.

---

## Verification Waves and Acceptance Boundary

1. **Wave 1 — topology static TDD:** retain the missing-file and proxy-node RED→GREEN sequence, then require a focused final-review publication RED against the arbitrary-bind/late-guard design. GREEN requires the sole fixed loopback mapping, exact acknowledgement environment, exact no-output exit-64 guard before exec, paired Cluster targets, unchanged six services/five volumes/seven validation ports, and protected hashes without Docker.
2. **Wave 2 — Rust protocol/target TDD:** absent target RED, historical version-representation RED→topology GREEN, and confirmed direct proxy RED `add=200 pin=200 cat=502`; one pure helper unit preserves exact `1.1.6` core with optional valid build metadata. Actual v1.1.6 types, bounded polling, normalized output, public production `KuboClient` plus `stream_add`→`pin_add`→`stream_cat` under an outer bound, separate S3 provenance, and outage/recovery scenarios compile/static GREEN; runtime proxy GREEN remains distinct and mandatory.
3. **Wave 3 — workflow TDD:** six-job/five-command contracts RED; bind acknowledgement must be case-sensitive exact loopback while all seven missing-variable probes remain; ownership, topology/proxy GREEN gates, diagnostics, restart, cleanup, and AST become GREEN without changing existing job semantics.
4. **Wave 4 — one from-scratch LOCAL gate:** after preflight, separately build the gateway and check exit immediately without bind inspection; then toggle bind and capture only no-build guard execution output for exit64/no-value checks, prove zero residuals/restore loopback, and run the unchanged valid parity path.
5. **Wave 5 — evidence-gated docs:** bounded README and exactly one ROADMAP checkbox only after every LOCAL line passes; hosted remains NOT RUN.
6. **Wave 6 — final acceptance:** five static contracts, live receipt plus all Rust/quality/LSP/diff gates, protected hashes, exact 14 paths, immutable identity, common Oracle+Reviewer packet/receipts, then one orchestrator-only authorized commit.

Arbitrary/non-loopback gateway mapping, bind acknowledgement not exact loopback, missing/misordered/outputting guard, negative guard exit other than64, any guard residue, Docker/runtime/image/network absence, missing topology/proxy GREEN, protocol/replication failure, unsafe diagnostics, cleanup/restoration uncertainty, changed application/dependency/file/service/port scope, or stale review receipt is not PASS. The fixed mapping is the publication boundary; guard success alone never proves safe host exposure.

## Requirement-to-Task Coverage

| Requirement | Coverage |
| --- | --- |
| Exact six services/images/volumes/Cluster targets plus sole fixed loopback gateway mapping, required acknowledgement environment, no-output exit-64 guard before exec, unchanged seven validation ports | Task 1 |
| Seven required variables; bind exact loopback; secret/key validation; no arbitrary/wildcard publication, tracked secret/default, provider/private-swarm/profile drift | Tasks 1, 3, 4 |
| Separate no-write topology gate; exact health, exact `1.1.6` release core with optional valid build metadata, two distinct agreeing views/no third peer; normalized output; terminal categories propagate without raw versions/IDs | Tasks 2-4 |
| Existing add `pin=false`, pin/add and cat unchanged; confirmed cat-502 RED traced to proxy localhost default; direct test calls public production functions, duplicates no protocol, excludes S3/PostgreSQL, and must set full add→pin→cat GREEN before replication | Tasks 1-4 |
| Actual v1.1.6 health/peers/allocation/pin shapes, five Tokio live tests plus one deterministic release-version unit, bounded transient-only retries, exact two peer/allocation/pinned evidence | Task 2; live Task 4 |
| S3 PUT/immediate GET, Kubo A/B bytes, S3 DELETE→HEAD404 with retained allocation/B bytes | Task 2; live Task 4 |
| Peer-B stop loses healthy 2-pin evidence; sanitized logs; same-volume restart restores without upload | Tasks 2-4 |
| Independent sixth job, exact-loopback bind validator, strict parser, ownership/cleanup, GREEN gates, diagnostics, five static commands, existing job meanings unchanged | Task 3 |
| Separate guard-image build exit0/output excluded from bind assertions; no-build mismatch execution exit64/output-safe/no-port/residual-zero; valid parity/cleanup/environment16/16 | Task 4 |
| README fixed-loopback/direct-non-loopback-unsupported/TLS-auth reverse-proxy boundary; identity suppression; only Cluster checkbox; hosted NOT RUN | Task 5 |
| Five static contracts, version-unit rerun, Cluster plus existing-target compile applicability, lib/integration/check/fmt/Clippy/LSP/diff, exact 14 paths | Task 6 |
| Canonical identity, common Oracle+Reviewer packet, both receipts before one integrated commit, no push/tag | Task 6 |

## Risks and Assumptions

- There is no official Kubo/Cluster compatibility matrix. The separate no-write topology test must pass first; the subsequent direct loopback `59103` test reuses the public production `stream_add`→`pin_add`→`stream_cat` path and is the sole compatibility authority. Its explicit outer timeout is required because the production upload client intentionally lacks a whole-request deadline. S3 bucket/database behavior is excluded from that verdict, and no plan instruction permits adapting application source around a failure.
- Cluster v1.1.6 `/peers` is NDJSON and empty streams may be 204; `/allocations/{cid}` and `/pins/{cid}` are JSON. The Rust split prevents scenario logic from inventing an array response or treating mutation acceptance as replication.
- The official image represents the same release as `1.1.6+git<commit>`. `is_release_1_1_6_version` accepts only bare `1.1.6` or valid nonempty dot-separated ASCII alphanumeric/hyphen build identifiers; it rejects every pre-release, other core, empty/dotted malformed metadata, whitespace, slash, underscore, and arbitrary suffix. Runtime output remains only normalized `version=1.1.6`.
- IPFS Cluster v1.1.6 separates `ipfs_connector.ipfshttp.node_multiaddress` from `api.ipfsproxy.node_multiaddress`; `api/ipfsproxy/config.go` maps env prefix `cluster_ipfsproxy` plus field `node_multiaddress` to `CLUSTER_IPFSPROXY_NODEMULTIADDRESS`, whose default is container-local `/ip4/127.0.0.1/tcp/5001`. The confirmed add/pin-200 plus cat-502 run proves connector success cannot stand in for proxy forwarding. Both env settings must target the same peer-local Kubo DNS name, and only a complete production-surface add→pin→cat rerun closes this RED.
- Reviewer identity `ca26` correctly rejected reliance on a container entrypoint guard for host-publication safety: Docker creates host port bindings before that guard executes, and Compose interpolation cannot regex-validate an arbitrary IP. This same-host/non-HA profile therefore supports only an intrinsically fixed `127.0.0.1` mapping. The exact acknowledgement guard remains defense-in-depth; external access requires a separately secured TLS/auth reverse proxy outside this plan.
- Docker build output may legitimately echo protected Dockerfile text such as `ENV IPFS_S3_BIND=0.0.0.0:9000`; it is therefore never inspected for bind acknowledgement values. Only the subsequent no-build guard container execution output is eligible for no-value assertions and exact exit64 evidence.
- The unchanged Kubo entrypoint uses repository-specific initialization. Whether its network behavior permits peer B to fetch blocks is intentionally resolved only by real Kubo-B cat plus tracker `pinned`; a failure is not papered over.
- Compose-network mDNS convergence and tracker transitions are timing-sensitive. Individual HTTP calls are 10 seconds and convergence is bounded. Only transport/partial-state categories retry; malformed payloads, wrong version, duplicate/third peers, and contradictory complete views fail immediately without identities.
- The outage test accepts a completed non-success pin-status response or an incomplete physical pinned set while Cluster A health remains 204; it does not bless an arbitrary undocumented status string or accept Cluster A network timeout as evidence.
- `docker compose start kubo-b cluster-b` reuses stopped containers and their named volumes. The recovery Rust test contains no PUT and verifies the receipt CID/body, preventing accidental re-upload from satisfying recovery.
- Validation state JSON has exact source `s3-replication-retention-v1` and contains only that S3 scenario's deterministic bytes/CID plus the 64-hex digest of the in-memory two-peer set. The direct proxy test cannot access or update it, and its opaque CID is discarded without unpin. Outer PowerShell claims the receipt with CreateNew after project ownership; Rust replication only updates the owned empty file; cleanup removes it only under the independent receipt-owned flag. No identity or CID is printed.
- Compose logs may contain generated identities even when commands do not request them. Every diagnostics body captures native output/exit first, applies the same in-memory PeerID/`/p2p/`/12D3Koo/peer-context-Qm redactor, emits only sanitized lines, and still blocks on the saved nonzero exit.
- Existing PostgreSQL and multi-gateway live jobs are unchanged and remain blocking hosted surfaces. Locally, protected hashes plus target compilation are the applicable final regression because the one required Docker run is the Cluster workflow-parity gate; no skipped live test is reported as PASS.
- Fixed ports may be occupied. Validation refuses to kill processes or clean resources it did not first prove absent and claim.
- Production `down --volumes` is forbidden in docs; only the owned disposable validation project may remove volumes.
- The approved spec and this plan begin untracked and are intentionally included in the exact identity and integrated commit.
- The approved spec and every other changed file must remain whitespace-clean under both no-index and staged `git diff --check` validation.

## Plan Self-Review

- Spec coverage: all acceptance criteria, both runtime revisions, final-review security ruling, and Git boundary map to Tasks 1-6. Fixed loopback is a stricter subset of the spec's explicit non-wildcard requirement; the acknowledgement guard adds defense-in-depth without changing files/services/ports/application behavior or adding external-access infrastructure.
- Incomplete-marker scan: no unresolved marker, deferred implementation instruction, abbreviated cross-reference, example credential token, or unnamed interface remains. The Exact Release Workflow Reference contains the full security-sensitive blocks consumed by Task 3.
- Stale-contract scan: both superseded spec SHAs, direct peer-version byte equality, and absence of `CLUSTER_IPFSPROXY_NODEMULTIADDRESS` are rejected; every protected check/review packet uses revised spec SHA `56d0d5518ff05f59cacec7f3740a6607c6505e7613086a89545650dfde9202eb`.
- Publication wording scan: every final mapping/reference uses fixed `127.0.0.1`; `${IPFS_S3_GATEWAY_BIND}` appears only as the required acknowledgement source or in explicitly rejected historical/static examples. No final claim says an arbitrary non-wildcard value can drive host publication, and every external-access statement requires an out-of-scope secured TLS/auth reverse proxy.
- Guard-command scan: the negative path has one separate `build gateway` with immediate exit capture/check and one `run --rm --no-deps gateway` without `--build`; only `$guardOutput` from the latter is inspected for rejected/protected values. Combined `run --build` is absent.
- Naming synchronization: final implementation and verification consistently use helper `is_release_1_1_6_version`, pure test `cluster_support::release_version_validator_accepts_exact_release_and_build_metadata`, and terminal category `peer_version_not_release_1_1_6`. The superseded helper/test names are absent; the superseded terminal category remains only in the two explicitly historical RED evidence statements.
- Type/signature consistency: existing Rust names remain synchronized; paired Cluster targets, fixed mapping, `IPFS_S3_PUBLISHED_BIND`, exact shell guard, workflow/local acknowledgement checks, topology/proxy GREEN markers, ownership flags, unchanged service/volume/port counts, and `16/16` restoration are consistent across tasks.
- Causal TDD: deployment/Rust/workflow/docs absence, historical version equality, focused missing proxy-node static contract, and confirmed cat-502 runtime each produce named RED evidence. The static correction must turn GREEN before runtime, and replication cannot proceed until complete add→pin→cat sets the proxy GREEN toggle.
- Critic-ledger closure: the version helper/unit and sanitized `ProbeError` categories remain unchanged. Task 1 locks the separate v1.1.6 proxy target to each connector's Kubo DNS endpoint and rejects localhost/cross-wiring. Tasks 2-4 preserve the 502 as RED, require full production-surface proxy GREEN before S3, duplicate no protocol, and retain bounded/identity-redacted/owned-cleanup behavior.
- Safety: static tests never run Docker; host mapping is intrinsically loopback. Task4 separately checks image-build exit without inspecting build output, then captures only the no-build/no-service-port guard execution for no-value/exit64 evidence. Unique ownership, capture-before-down, residual checks, bind restoration, main diagnostics/cleanup, and environment16/16 remain fail-closed.
- Review/Git: exact 14 paths remain unchanged. Rejection identity `ca26` and all pre-correction approvals are invalid as receipts; the corrected identity must receive fresh Oracle+Reviewer approvals before the one authorized commit. Push/tag remain forbidden.
- Documentation truth: Task 5 cannot begin before LOCAL ALL PASS, hosted remains `NOT RUN`, only the Cluster checkbox changes, and production shutdown never deletes volumes.

## Plan Review Status

- Receipt: `waiting for receipt`
- Review owner: orchestrator
- Any edit to this plan invalidates a later plan-review receipt and requires review of the complete current revision.
