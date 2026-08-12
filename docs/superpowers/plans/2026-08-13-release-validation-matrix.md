# Release Validation Matrix Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add secret-free, blocking GitHub Actions coverage for the live PostgreSQL import, Docker Compose E2E, and client-smoke infrastructure surfaces that the existing CI does not execute.

**Architecture:** Add one text-parsing PowerShell contract test and one independent GitHub Actions workflow. The workflow keeps PostgreSQL, Compose E2E, and client-smoke infrastructure in three isolated blocking jobs, while the contract test locks down triggers, permissions, timeouts, service configuration, exact commands, diagnostics, cleanup, and non-goal boundaries without a YAML module or `actionlint`. Add test-only teardown for live PostgreSQL race fixtures that intentionally leave queued jobs, preventing a later serial test's global `claim_due` from claiming an earlier test's work.

**Tech Stack:** GitHub Actions YAML, PowerShell 7, Docker Compose v2, PostgreSQL 17 service containers, Rust 1.92, Cargo, existing `actions/checkout@v7`, `dtolnay/rust-toolchain@v1`, and `Swatinem/rust-cache@v2` actions.

**Global Constraints:**
- Modify only `.github/workflows/release-validation.yml`, `tests/release-validation.Tests.ps1`, and test-only fixture teardown in `tests/postgres_import.rs`; this plan and the approved specification are documentation artifacts, not production surfaces.
- Do not modify production Rust, PostgreSQL behavioral assertions, Compose files, `scripts/client-smoke.ps1`, version metadata, README, ROADMAP, or compatibility documentation.
- Do not perform a release, publish or push images, create or push tags, or connect to real AWS, mc, rclone, Cloudflare, Pinata, Filebase, or any other credentialed provider.
- Do not replace or duplicate the existing check, Clippy, library, integration, or formatting jobs in `.github/workflows/ci.yml`.
- The new workflow must trigger on pull requests, pushes to `master`, and `workflow_dispatch`; use read-only permissions and workflow-plus-ref concurrency with cancellation of an older run for the same ref.
- The workflow must contain exactly three independent blocking jobs, and every job must have an explicit timeout.
- The PostgreSQL job must set `IPFS_S3_TEST_POSTGRES_URL` at job scope and run `cargo test --test postgres_import -- --nocapture --test-threads=1` against PostgreSQL 17 so all four tests cannot take their missing-environment skip path.
- The E2E job must explicitly check `docker compose version`, use only `docker-compose.yml`, start only `kubo gateway` with `--detach --build --wait --wait-timeout 300`, run the E2E target serially, and execute non-coloured logs plus volume/orphan cleanup under `always()` with `continue-on-error`.
- The client-smoke infrastructure job must run `tests/release-validation.Tests.ps1` before `tests/client-smoke.Tests.ps1` and must not invoke `scripts/client-smoke.ps1 -Run` or any real external client.
- Use no new PowerShell module, YAML parser, action linter, dependency, or software installation.
- Use PowerShell syntax for every command; do not use Bash `&&`, shell `export`, or `/dev/null` syntax.
- Do not stage, commit, push, tag, or perform any other Git write from implementation subagents.
- If Docker, the daemon, an image pull, or a build network is unavailable locally, preserve the failing command and output as `UNVERIFIED` live evidence; never convert an unexecuted live surface into a pass.
- Every local live wave must use a unique explicit Compose project name for all of its Compose commands, scope cleanup to that project, and restore every environment variable it overrides.
- Local E2E must refuse to start if either fixed container name `ipfs-s3-kubo` or `ipfs-s3-gateway` already exists in any state; that refusal must not run cleanup.
- Local PostgreSQL must force `IPFS3_IMPORT_POSTGRES_PORT=55432` and the matching test URL; local E2E must force both loopback endpoint variables so ambient shell state cannot redirect evidence.
- PostgreSQL teardown must lock and supersede active jobs only for buckets created by the current test, after all behavioral assertions; do not change global `claim_due` selection or consume unrelated queued work.

**Authoritative spec:** `docs/superpowers/specs/2026-08-13-release-validation-matrix-design.md`

## File Map

- Create `.github/workflows/release-validation.yml`: the secret-free release-evidence workflow with three blocking live-surface jobs.
- Create `tests/release-validation.Tests.ps1`: a dependency-free static contract test for the workflow's exact security, trigger, job, command, and cleanup invariants.
- Modify `tests/postgres_import.rs`: add a test-only bucket-scoped teardown helper and invoke it only for race fixtures that intentionally retain queued jobs.
- Verify only `.github/workflows/ci.yml`, `docker-compose.yml`, `config.docker.toml`, `tests/compose.postgres-import.yml`, `tests/e2e.rs`, and `tests/client-smoke.Tests.ps1`; do not edit them.

---

### Task 1: Define the release-workflow contract and capture RED

**Files:**
- Create: `tests/release-validation.Tests.ps1`
- Missing RED dependency: `.github/workflows/release-validation.yml`
- Reference: `tests/client-smoke.Tests.ps1`

**Interfaces:**
- Consumes: repository-relative path resolution through `$PSScriptRoot`, raw workflow text from `.github/workflows/release-validation.yml`, and only built-in PowerShell/.NET APIs.
- Produces: executable command `pwsh -NoProfile -File tests/release-validation.Tests.ps1`; exit zero plus `release-validation workflow contract tests: PASSED` when the complete contract holds, or a precise thrown invariant message otherwise.

- [ ] **Step 1: Create the complete text-based contract test**

Create `tests/release-validation.Tests.ps1` with exactly this content:

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$WorkflowPath = Join-Path $RepoRoot ".github/workflows/release-validation.yml"
if (-not [IO.File]::Exists($WorkflowPath)) {
    throw "Release validation workflow is missing: $WorkflowPath"
}

$Workflow = [IO.File]::ReadAllText($WorkflowPath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")

function Assert-True {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { throw $Message }
}

function Assert-Contains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ($Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Fragment,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not $Text.Contains($Fragment, [StringComparison]::Ordinal)) $Message
}

function Assert-Matches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True ([regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-NotMatches {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Message
    )
    Assert-True (-not [regex]::IsMatch($Text, $Pattern)) $Message
}

function Assert-ExactLine {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Line,
        [Parameter(Mandatory)][string]$Message
    )

    $count = @($Text -split "`n" | Where-Object { $_ -ceq $Line }).Count
    Assert-True ($count -eq 1) "$Message Expected exactly one line, found $count."
}

function Get-YamlBlock {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string]$Key,
        [Parameter(Mandatory)][int]$Indent
    )

    $lines = @($Text -split "`n")
    $header = (" " * $Indent) + $Key + ":"
    $matchingIndexes = @(
        for ($index = 0; $index -lt $lines.Count; $index++) {
            if ($lines[$index].TrimEnd() -ceq $header) { $index }
        }
    )
    if ($matchingIndexes.Count -ne 1) {
        throw "Expected one YAML key '$header', found $($matchingIndexes.Count)"
    }

    $start = $matchingIndexes[0]
    $end = $lines.Count
    for ($index = $start + 1; $index -lt $lines.Count; $index++) {
        if ([string]::IsNullOrWhiteSpace($lines[$index])) { continue }
        $leadingSpaces = [regex]::Match($lines[$index], '^( *)').Groups[1].Length
        if ($leadingSpaces -le $Indent) {
            $end = $index
            break
        }
    }
    if ($end -le $start + 1) { return "" }
    return ($lines[($start + 1)..($end - 1)] -join "`n")
}

function Assert-InOrder {
    param(
        [Parameter(Mandatory)][string]$Text,
        [Parameter(Mandatory)][string[]]$Fragments,
        [Parameter(Mandatory)][string]$Message
    )

    $cursor = 0
    foreach ($fragment in $Fragments) {
        $index = $Text.IndexOf($fragment, $cursor, [StringComparison]::Ordinal)
        if ($index -lt 0) { throw "$Message Missing or out of order: $fragment" }
        $cursor = $index + $fragment.Length
    }
}

function Assert-RustSetup {
    param(
        [Parameter(Mandatory)][string]$JobBlock,
        [Parameter(Mandatory)][string]$JobName
    )

    foreach ($fragment in @(
        "      - uses: actions/checkout@v7",
        "        uses: dtolnay/rust-toolchain@v1",
        '          toolchain: "1.92"',
        "        uses: Swatinem/rust-cache@v2"
    )) {
        Assert-Contains $JobBlock $fragment "$JobName is missing required Rust setup fragment: $fragment"
    }
}

Assert-NotContains $Workflow "`t" "Workflow must use spaces, not tabs"

# Read `on:` as an exact textual key so PowerShell never delegates YAML 1.1 key coercion.
$onBlock = Get-YamlBlock -Text $Workflow -Key "on" -Indent 0
Assert-Matches $onBlock '(?m)^  pull_request:\s*$' "pull_request trigger is missing"
Assert-Matches $onBlock '(?m)^  workflow_dispatch:\s*$' "workflow_dispatch trigger is missing"
$pushBlock = Get-YamlBlock -Text $onBlock -Key "push" -Indent 2
Assert-Matches $pushBlock '(?m)^    branches: \[master\]\s*$' "push trigger must target only master"

$permissionsBlock = Get-YamlBlock -Text $Workflow -Key "permissions" -Indent 0
$permissionLines = @($permissionsBlock -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($permissionLines.Count -eq 1) "Workflow permissions must contain only contents: read"
Assert-True ($permissionLines[0].Trim() -ceq "contents: read") "Workflow permissions must be read-only contents: read"
Assert-NotMatches $Workflow '(?m)^\s*[A-Za-z0-9_-]+:\s*write\s*$' "Workflow must not grant write permission"

$concurrencyBlock = Get-YamlBlock -Text $Workflow -Key "concurrency" -Indent 0
$concurrencyLines = @($concurrencyBlock -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($concurrencyLines.Count -eq 2) "Concurrency must contain only group and cancel-in-progress"
Assert-Contains $concurrencyBlock '  group: ${{ github.workflow }}-${{ github.ref }}' "Concurrency group must combine workflow and ref"
Assert-Contains $concurrencyBlock "  cancel-in-progress: true" "Concurrency must cancel an older run for the same ref"

$jobsBlock = Get-YamlBlock -Text $Workflow -Key "jobs" -Indent 0
$jobNames = @([regex]::Matches($jobsBlock, '(?m)^  ([A-Za-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True ($jobNames.Count -eq 3) "Expected exactly three jobs, found $($jobNames.Count): $($jobNames -join ', ')"
foreach ($expectedJob in @("postgres-import", "e2e", "client-smoke-infrastructure")) {
    Assert-True ($jobNames -ccontains $expectedJob) "Required job is missing: $expectedJob"
}
Assert-NotMatches $jobsBlock '(?m)^    needs:' "Release-validation jobs must be independent"
Assert-NotMatches $jobsBlock '(?m)^    continue-on-error:' "Release-validation jobs must be blocking"

$postgresJob = Get-YamlBlock -Text $jobsBlock -Key "postgres-import" -Indent 2
$e2eJob = Get-YamlBlock -Text $jobsBlock -Key "e2e" -Indent 2
$clientJob = Get-YamlBlock -Text $jobsBlock -Key "client-smoke-infrastructure" -Indent 2

Assert-Contains $postgresJob "    runs-on: ubuntu-latest" "PostgreSQL job must use ubuntu-latest"
Assert-Contains $postgresJob "    timeout-minutes: 30" "PostgreSQL job timeout must be 30 minutes"
Assert-RustSetup -JobBlock $postgresJob -JobName "PostgreSQL job"
$postgresEnv = Get-YamlBlock -Text $postgresJob -Key "env" -Indent 4
$postgresEnvLines = @($postgresEnv -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
Assert-True ($postgresEnvLines.Count -eq 1) "PostgreSQL job env must contain exactly the test URL"
Assert-True ($postgresEnvLines[0].Trim() -ceq "IPFS_S3_TEST_POSTGRES_URL: postgres://ipfs3:ipfs3@127.0.0.1:5432/ipfs3_import_test") "PostgreSQL URL is missing or not job-scoped"
$postgresServices = Get-YamlBlock -Text $postgresJob -Key "services" -Indent 4
$postgresService = Get-YamlBlock -Text $postgresServices -Key "postgres" -Indent 6
foreach ($fragment in @(
    "        image: postgres:17",
    "          POSTGRES_DB: ipfs3_import_test",
    "          POSTGRES_USER: ipfs3",
    "          POSTGRES_PASSWORD: ipfs3",
    "          - 5432:5432",
    '          --health-cmd "pg_isready -U ipfs3 -d ipfs3_import_test"',
    "          --health-interval 1s",
    "          --health-timeout 5s",
    "          --health-retries 30"
)) {
    Assert-Contains $postgresService $fragment "PostgreSQL service contract is missing: $fragment"
}
$postgresRunLines = @($postgresJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($postgresRunLines.Count -eq 1) "PostgreSQL job must contain exactly one run command"
Assert-ExactLine $postgresJob "        run: cargo test --test postgres_import -- --nocapture --test-threads=1" "PostgreSQL job must run the exact serial target."

Assert-Contains $e2eJob "    runs-on: ubuntu-latest" "E2E job must use ubuntu-latest"
Assert-Contains $e2eJob "    timeout-minutes: 60" "E2E job timeout must be 60 minutes"
Assert-RustSetup -JobBlock $e2eJob -JobName "E2E job"
$e2eRunLines = @($e2eJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($e2eRunLines.Count -eq 5) "E2E job must contain exactly five run commands"
foreach ($line in @(
    "        run: docker compose version",
    "        run: docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway",
    "        run: cargo test --test e2e -- --nocapture --test-threads=1",
    "        run: docker compose -f docker-compose.yml logs --no-color kubo gateway",
    "        run: docker compose -f docker-compose.yml down --volumes --remove-orphans"
)) {
    Assert-ExactLine $e2eJob $line "E2E job command is missing or changed."
}
Assert-InOrder -Text $e2eJob -Message "E2E commands must preserve setup/test/diagnostics/cleanup order." -Fragments @(
    "        run: docker compose version",
    "        run: docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway",
    "        run: cargo test --test e2e -- --nocapture --test-threads=1",
    "      - name: Compose diagnostics",
    "        run: docker compose -f docker-compose.yml logs --no-color kubo gateway",
    "      - name: Compose cleanup",
    "        run: docker compose -f docker-compose.yml down --volumes --remove-orphans"
)
Assert-True (([regex]::Matches($e2eJob, '(?m)^        if: \$\{\{ always\(\) \}\}\s*$')).Count -eq 2) "Diagnostics and cleanup must both use always()"
Assert-True (([regex]::Matches($e2eJob, '(?m)^        continue-on-error: true\s*$')).Count -eq 2) "Diagnostics and cleanup must both continue on error"
Assert-True (([regex]::Matches($jobsBlock, '(?m)^        continue-on-error: true\s*$')).Count -eq 2) "Only E2E diagnostics and cleanup may continue on error"

Assert-Contains $clientJob "    runs-on: ubuntu-latest" "Client-smoke infrastructure job must use ubuntu-latest"
Assert-Contains $clientJob "    timeout-minutes: 15" "Client-smoke infrastructure timeout must be 15 minutes"
Assert-Contains $clientJob "      - uses: actions/checkout@v7" "Client-smoke infrastructure job must check out the repository"
$clientRunLines = @($clientJob -split "`n" | Where-Object { $_ -match '^\s+run:' })
Assert-True ($clientRunLines.Count -eq 2) "Client-smoke infrastructure job must contain exactly two run commands"
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1" "Release-validation contract command is missing or changed."
Assert-ExactLine $clientJob "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1" "Client-smoke infrastructure command is missing or changed."
Assert-InOrder -Text $clientJob -Message "Client-smoke contract test must run before the existing infrastructure test." -Fragments @(
    "        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1",
    "        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1"
)

Assert-NotContains $Workflow "cloudflared" "Release validation must not start or reference cloudflared"
Assert-NotContains $Workflow "scripts/client-smoke.ps1" "Release validation must not invoke the real client-smoke runner"
Assert-NotMatches $Workflow '(?im)^\s*run:[^\r\n]*(?:\s|/)(?:aws|mc|rclone)(?:\.exe)?(?:\s|$)' "Release validation must not execute a real AWS, mc, or rclone client"
Assert-NotMatches $clientJob '(?m)(?:^|\s)-Run(?:\s|$)' "Client-smoke infrastructure job must not request real client execution"

Write-Host "release-validation workflow contract tests: PASSED"
```

- [ ] **Step 2: Run the new test while the workflow is absent and preserve RED**

Run:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
```

Expected: exit non-zero with a message beginning `Release validation workflow is missing:` and ending in `.github\workflows\release-validation.yml`. If the workflow already exists from an interrupted implementation, temporarily move only that untracked implementation file aside with the editing tool, run this RED check, and restore it with the editing tool; do not use a filesystem or Git write command.

- [ ] **Step 3: Review the RED contract's scope before adding YAML**

Confirm from the script that it reads only `.github/workflows/release-validation.yml`, has no module import, does not execute Docker/Cargo/real clients, handles `on:` through `Get-YamlBlock -Key "on"`, requires exactly three root-level jobs, and emits a distinct invariant message for every failure. The intentionally failing RED state is the accepted Task 1 deliverable; Task 2 immediately supplies its dependency.

---

### Task 2: Implement the three-job blocking workflow and turn the contract GREEN

**Files:**
- Create: `.github/workflows/release-validation.yml`
- Test: `tests/release-validation.Tests.ps1`
- Verify unchanged: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Task 1's static contract; `tests/postgres_import.rs` environment key `IPFS_S3_TEST_POSTGRES_URL`; the base Compose services `kubo` and `gateway`; E2E loopback defaults and `test`/`test` credentials; existing `tests/client-smoke.Tests.ps1` entry point.
- Produces: GitHub Actions workflow `Release validation` with independent jobs `postgres-import`, `e2e`, and `client-smoke-infrastructure`, all blocking and time-bounded.

- [ ] **Step 1: Create the complete workflow**

Create `.github/workflows/release-validation.yml` with exactly this content:

```yaml
name: Release validation

on:
  pull_request:
  push:
    branches: [master]
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  postgres-import:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      IPFS_S3_TEST_POSTGRES_URL: postgres://ipfs3:ipfs3@127.0.0.1:5432/ipfs3_import_test
    services:
      postgres:
        image: postgres:17
        env:
          POSTGRES_DB: ipfs3_import_test
          POSTGRES_USER: ipfs3
          POSTGRES_PASSWORD: ipfs3
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U ipfs3 -d ipfs3_import_test"
          --health-interval 1s
          --health-timeout 5s
          --health-retries 30
    steps:
      - uses: actions/checkout@v7

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@v1
        with:
          toolchain: "1.92"

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2

      - name: Run live PostgreSQL import tests
        run: cargo test --test postgres_import -- --nocapture --test-threads=1

  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v7

      - name: Install Rust toolchain
        uses: dtolnay/rust-toolchain@v1
        with:
          toolchain: "1.92"

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2

      - name: Verify Docker Compose
        run: docker compose version

      - name: Build and start Kubo and gateway
        run: docker compose -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway

      - name: Run serial end-to-end tests
        run: cargo test --test e2e -- --nocapture --test-threads=1

      - name: Compose diagnostics
        if: ${{ always() }}
        continue-on-error: true
        run: docker compose -f docker-compose.yml logs --no-color kubo gateway

      - name: Compose cleanup
        if: ${{ always() }}
        continue-on-error: true
        run: docker compose -f docker-compose.yml down --volumes --remove-orphans

  client-smoke-infrastructure:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v7

      - name: Test release-validation workflow contract
        run: pwsh -NoProfile -File tests/release-validation.Tests.ps1

      - name: Test client-smoke infrastructure
        run: pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

- [ ] **Step 2: Run the contract test and verify GREEN**

Run:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
```

Expected: exit zero and print `release-validation workflow contract tests: PASSED`. A failure must be fixed in the workflow rather than by weakening or deleting a contract assertion unless the assertion demonstrably contradicts the authoritative spec.

- [ ] **Step 3: Run the existing client-smoke infrastructure test**

Run:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

Expected: exit zero and print `client-smoke infrastructure tests: PASSED`; no real rclone, mc, or AWS smoke client is launched.

- [ ] **Step 4: Prove the new workflow does not duplicate existing CI**

Run this read-only audit:

```powershell
$releaseWorkflow = [IO.File]::ReadAllText(".github/workflows/release-validation.yml")
$forbiddenCommands = @(
    "cargo check --all-targets",
    "cargo clippy --all-targets -- -D warnings",
    "cargo test --lib",
    "cargo test --test integration",
    "cargo fmt --all -- --check"
)
foreach ($command in $forbiddenCommands) {
    if ($releaseWorkflow.Contains($command, [StringComparison]::Ordinal)) {
        throw "Existing CI command was duplicated in release-validation.yml: $command"
    }
}
```

Expected: no exception. The new workflow contains only the three missing live surfaces.

---

### Task 3: Execute static, local-live, regression, and repository-boundary verification waves

**Files:**
- Verify: `.github/workflows/release-validation.yml`
- Verify: `tests/release-validation.Tests.ps1`
- Modify: `tests/postgres_import.rs`
- Verify unchanged: `docker-compose.yml`
- Verify unchanged: `tests/compose.postgres-import.yml`
- Verify unchanged: `tests/e2e.rs`
- Verify unchanged: `tests/client-smoke.Tests.ps1`
- Verify unchanged: all production Rust, version, README, ROADMAP, and compatibility files

**Interfaces:**
- Consumes: Task 2's GREEN workflow contract, local PowerShell/Cargo toolchain, and Docker Compose/daemon only when available.
- Produces: four evidence records—static workflow evidence, live PostgreSQL evidence, live E2E evidence, and complete regression/boundary evidence—with unavailable live surfaces explicitly labelled `UNVERIFIED` rather than passed.

- [ ] **Step 0: Isolate intentionally queued PostgreSQL race fixtures**

Preserve the first live run as RED evidence: on one fresh PostgreSQL 17 database
the four serial tests ran, two passed, and two failed because later
`claim_due(..., limit = 1)` calls claimed queued jobs retained by preceding
tests. Each failing test passes on its own fresh database, while a second fresh
full-suite run reproduces the same 2/4 failure signature.

In `tests/postgres_import.rs`, add this test-only helper beside the existing
bucket ownership helpers:

```rust
async fn supersede_active_test_jobs(
    db: &DatabaseConnection,
    bucket_name: &str,
    now: DateTime<Utc>,
) {
    let bucket_name = bucket_name.to_owned();
    db.transaction(|txn| {
        Box::pin(async move {
            lock_bucket_for_ownership(txn, &bucket_name).await?;
            supersede_bucket(txn, &bucket_name, now).await?;
            Ok::<_, AppError>(())
        })
    })
    .await
    .unwrap();
}
```

After every existing assertion in
`postgres_claim_due_contends_with_prefix_and_bucket_supersession_without_deadlock`,
call it for `prefix_bucket`; this terminally supersedes only the intentionally
queued `prefix_trigger` fixture. After every existing assertion in
`postgres_bucket_first_ownership_serializes_all_task_four_races`, call it for
the three current-test buckets whose final assertions intentionally retain a
queued owner:

```rust
for bucket_name in [
    &bucket_exact_admit,
    &bucket_prefix_admit,
    &bucket_empty_admit,
] {
    supersede_active_test_jobs(&empty_holder, bucket_name, Utc::now()).await;
}
```

Any connection from that same test database is valid; the helper is bucket
scoped. Do not alter existing assertions, fixed race ordering, production code,
or `claim_due`. Format, then rerun the exact full live target against one healthy
fresh fixture. It must execute and pass all four tests. Recreate the fixture and
run the same full target once more; it must again pass all four tests. Each run
proves isolation between tests inside one target invocation, while the fresh
second fixture avoids unrelated collisions with intentionally retained terminal
job IDs.

- [ ] **Step 1: Run Wave 1—the dependency-free static gates**

Run sequentially:

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

Expected: both commands exit zero and print their `PASSED` messages. Stop before live testing if either static gate fails.

- [ ] **Step 2: Validate Docker Compose availability and the base-file model**

Run:

```powershell
docker compose version
if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: Docker Compose v2 is unavailable" }

docker compose -f docker-compose.yml config
if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: base docker-compose.yml did not render" }
```

Expected: both commands exit zero; the rendered model contains `kubo`, `gateway`, and `cloudflared`, while the workflow's explicit service selection remains solely `kubo gateway`. `config` may warn that optional provider/tunnel environment variables are unset, but it must not fail. If Compose itself is unavailable, preserve the command/output and label Compose config plus both live waves `UNVERIFIED`; the two PowerShell contract tests must still remain GREEN.

- [ ] **Step 3: Run Wave 2—the live PostgreSQL target against the repository fixture**

Only after `docker compose version` succeeds, run this PowerShell block from the repository root:

```powershell
$hadPostgresUrl = Test-Path Env:\IPFS_S3_TEST_POSTGRES_URL
$oldPostgresUrl = if ($hadPostgresUrl) { $env:IPFS_S3_TEST_POSTGRES_URL } else { $null }
$hadPostgresPort = Test-Path Env:\IPFS3_IMPORT_POSTGRES_PORT
$oldPostgresPort = if ($hadPostgresPort) { $env:IPFS3_IMPORT_POSTGRES_PORT } else { $null }
$postgresProject = "ipfs3-release-pg-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$postgresAttempted = $false
try {
    $env:IPFS3_IMPORT_POSTGRES_PORT = "55432"
    $env:IPFS_S3_TEST_POSTGRES_URL = "postgres://ipfs3:ipfs3@127.0.0.1:55432/ipfs3_import_test"
    $postgresAttempted = $true
    docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml up --detach --wait --wait-timeout 120 postgres-import
    if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: PostgreSQL 17 fixture did not become healthy" }

    cargo test --test postgres_import -- --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Live PostgreSQL import target failed" }
} finally {
    if ($postgresAttempted) {
        docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml logs --no-color postgres-import
        if ($LASTEXITCODE -ne 0) { Write-Warning "PostgreSQL diagnostic logs failed" }
        docker compose --project-name $postgresProject -f tests/compose.postgres-import.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { Write-Warning "PostgreSQL fixture cleanup failed; record cleanup as failed" }
    }
    if (-not $hadPostgresUrl) {
        Remove-Item Env:\IPFS_S3_TEST_POSTGRES_URL -ErrorAction SilentlyContinue
    } else {
        $env:IPFS_S3_TEST_POSTGRES_URL = $oldPostgresUrl
    }
    if (-not $hadPostgresPort) {
        Remove-Item Env:\IPFS3_IMPORT_POSTGRES_PORT -ErrorAction SilentlyContinue
    } else {
        $env:IPFS3_IMPORT_POSTGRES_PORT = $oldPostgresPort
    }
}
```

Expected: PostgreSQL 17 becomes healthy; the exact serial target runs four tests rather than printing any `IPFS_S3_TEST_POSTGRES_URL is unset` skip message; all four pass; logs and cleanup commands complete. A daemon/port/image-network failure is `UNVERIFIED` environment evidence, while a test assertion failure after a healthy database is a release-matrix failure.

After the first successful local PostgreSQL wave and cleanup, repeat this Step 3
block with a new unique project name and a fresh volume. The second fresh target
must also report four executed tests and four passes; record both results. Do not
reuse the first database because this target intentionally retains terminal job
history with fixed IDs in one race fixture.

- [ ] **Step 4: Run Wave 3—the live base-Compose E2E target without disturbing an existing stack**

First prove that neither fixed Compose container name already exists in any
state. This preflight occurs before ownership is claimed, so refusal performs no
cleanup:

```powershell
$fixedContainers = @(
    docker ps --all --filter "name=^/ipfs-s3-kubo$" --filter "name=^/ipfs-s3-gateway$" --format "{{.Names}}"
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: Docker daemon is unavailable for E2E preflight" }
if ($fixedContainers.Count -ne 0) {
    throw "BLOCKED: fixed E2E container names already exist; no cleanup was attempted: $($fixedContainers -join ', ')"
}
```

When the preflight is clear, run the same start/test/log/down commands used by GitHub Actions:

```powershell
$hadE2eEndpoint = Test-Path Env:\IPFS_S3_E2E_ENDPOINT
$oldE2eEndpoint = if ($hadE2eEndpoint) { $env:IPFS_S3_E2E_ENDPOINT } else { $null }
$hadE2eKuboUrl = Test-Path Env:\IPFS_S3_E2E_KUBO_URL
$oldE2eKuboUrl = if ($hadE2eKuboUrl) { $env:IPFS_S3_E2E_KUBO_URL } else { $null }
$e2eProject = "ipfs3-release-e2e-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))"
$e2eAttempted = $false
try {
    $env:IPFS_S3_E2E_ENDPOINT = "http://127.0.0.1:9000"
    $env:IPFS_S3_E2E_KUBO_URL = "http://127.0.0.1:5001"
    $e2eAttempted = $true
    docker compose --project-name $e2eProject -f docker-compose.yml up --detach --build --wait --wait-timeout 300 kubo gateway
    if ($LASTEXITCODE -ne 0) { throw "UNVERIFIED: Kubo/gateway images did not build or become healthy" }

    cargo test --test e2e -- --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Live E2E target failed" }
} finally {
    if ($e2eAttempted) {
        docker compose --project-name $e2eProject -f docker-compose.yml logs --no-color kubo gateway
        if ($LASTEXITCODE -ne 0) { Write-Warning "E2E diagnostic logs failed" }
        docker compose --project-name $e2eProject -f docker-compose.yml down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { Write-Warning "E2E cleanup failed; record cleanup as failed" }
    }
    if (-not $hadE2eEndpoint) {
        Remove-Item Env:\IPFS_S3_E2E_ENDPOINT -ErrorAction SilentlyContinue
    } else {
        $env:IPFS_S3_E2E_ENDPOINT = $oldE2eEndpoint
    }
    if (-not $hadE2eKuboUrl) {
        Remove-Item Env:\IPFS_S3_E2E_KUBO_URL -ErrorAction SilentlyContinue
    } else {
        $env:IPFS_S3_E2E_KUBO_URL = $oldE2eKuboUrl
    }
}
```

Expected: only `kubo` and `gateway` start from `docker-compose.yml`; health completes within 300 seconds; the serial E2E target passes against `127.0.0.1`; non-coloured logs are captured; containers, networks, orphans, and volumes are removed. A daemon/image/build-network failure is `UNVERIFIED`; a Rust E2E failure after healthy services is a release-matrix failure. Never run cleanup after the preflight reports an existing developer-owned stack.

- [ ] **Step 5: Run Wave 4—the complete existing regression gates**

Run sequentially, preserving each exit code:

```powershell
cargo test --lib
cargo test --test integration
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

Expected: every command exits zero with no failed tests, formatting diff, or Clippy warning. These commands are local regression evidence only; they remain in `.github/workflows/ci.yml` and must not be added to the release-validation workflow.

- [ ] **Step 6: Run whitespace and exact repository-boundary audits**

Run:

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Git diff whitespace check failed" }

$allowedPaths = @(
    ".github/workflows/release-validation.yml",
    "docs/superpowers/plans/2026-08-13-release-validation-matrix.md",
    "docs/superpowers/specs/2026-08-13-release-validation-matrix-design.md",
    "tests/release-validation.Tests.ps1",
    "tests/postgres_import.rs"
)
$changedPaths = @(
    git diff --name-only
    git diff --cached --name-only
    git ls-files --others --exclude-standard
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique

$unexpectedPaths = @($changedPaths | Where-Object { $allowedPaths -cnotcontains $_ })
if ($unexpectedPaths.Count -ne 0) {
    throw "Out-of-scope changed paths: $($unexpectedPaths -join ', ')"
}
foreach ($requiredPath in @(
    ".github/workflows/release-validation.yml",
    "tests/release-validation.Tests.ps1"
)) {
    if ($changedPaths -cnotcontains $requiredPath) {
        throw "Expected implementation path is absent from the diff: $requiredPath"
    }
}

git status --short
```

Expected: `git diff --check` exits zero; the changed-path set is limited to the approved spec, this plan, the new workflow, and the new contract test; both implementation files are present; no file is staged by an implementation subagent.

- [ ] **Step 7: Record the final evidence matrix and checkpoint without Git writes**

Record one line for each surface:

```text
STATIC release-validation contract: PASS | FAIL
STATIC client-smoke infrastructure: PASS | FAIL
COMPOSE base-file config: PASS | FAIL | UNVERIFIED (<exact reason>)
LIVE PostgreSQL 17 / postgres_import (4 tests): PASS | FAIL | UNVERIFIED (<exact reason>)
LIVE Kubo+gateway / e2e: PASS | FAIL | UNVERIFIED (<exact reason>)
REGRESSION lib/integration/fmt/clippy: PASS | FAIL
BOUNDARY diff-check/status: PASS | FAIL
```

Do not call a live surface PASS unless its exact live command ran to completion. Do not stage or commit from any implementation subagent. After all tasks, completion/integration checks, and the final acceptance review are approved, return the evidence and changed paths to the orchestrator; only the orchestrator may create the single release-validation-matrix commit, and only after confirming explicit user authorization at that final checkpoint.

## Verification Waves and Acceptance Boundary

1. **Wave 1—static/TDD:** prove the missing-workflow RED, then make `tests/release-validation.Tests.ps1` and `tests/client-smoke.Tests.ps1` GREEN.
2. **Wave 2—PostgreSQL live:** run the exact target serially with the environment URL set and PostgreSQL 17 healthy; four executed tests are required for PASS.
3. **Wave 3—Compose E2E live:** build/wait only Kubo and gateway from the base file, execute serial E2E, and always collect logs and remove volumes/orphans.
4. **Wave 4—regression/boundary:** run library, integration, fmt, Clippy, `git diff --check`, changed-path allowlist, and status inspection.

Acceptance requires both static tests, all non-Docker regression gates, and the boundary audit to pass. Locally unavailable Docker-dependent evidence remains explicitly `UNVERIFIED` until a capable local environment or the blocking GitHub Actions jobs execute it; it is never inferred from the static contract.

## Risks and Assumptions

- The contract intentionally validates exact text and indentation rather than general YAML equivalence. This reliably handles the literal `on:` key without a dependency, but semantically equivalent workflow refactors must update the contract and receive review.
- GitHub-hosted Ubuntu is assumed to provide PowerShell 7 and Docker Compose v2. The workflow proves Compose availability before starting services; it does not install runner software.
- Local PostgreSQL uses the fixture's default host port `55432`, while the GitHub service publishes fixed port `5432`; either can be unavailable because of a host-port conflict, which must be recorded rather than treated as a pass.
- The base Compose file has fixed container names and persistent volumes. The local E2E preflight must refuse ownership when either fixed name exists in any state, every live wave must use a unique project name, and cleanup must target only that project after ownership is claimed.
- Ambient `IPFS3_IMPORT_POSTGRES_PORT`, `IPFS_S3_TEST_POSTGRES_URL`, `IPFS_S3_E2E_ENDPOINT`, and `IPFS_S3_E2E_KUBO_URL` values can redirect tests. Local live blocks must force the specified values and restore prior presence and content in `finally`.
- Static tests can prove workflow shape but cannot prove GitHub Actions service-container semantics, image availability, or Docker build success. Those remain live evidence supplied by a capable local run and the eventual blocking workflow run.
- `actions/checkout@v7`, `dtolnay/rust-toolchain@v1`, and `Swatinem/rust-cache@v2` follow the repository's existing CI conventions; changing action-version policy is outside this scope.
