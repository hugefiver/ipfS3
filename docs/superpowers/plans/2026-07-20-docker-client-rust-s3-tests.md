# Docker Client Test Runner Safety Remediation Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing Docker client smoke runner collision-safe, forced-offline, client-pull-locked, process-bounded, cleanup-safe, and exit-code-correct, then regenerate fresh Rclone and MinIO `mc` evidence without changing the completed 11/11 Rust e2e suite.

**Architecture:** Keep client workflows in `scripts/client-smoke.ps1`, but route identity creation, native execution, preflight classification, and owned-artifact cleanup through small testable functions, and pull-lock all six client-container entry points. Add one dependency-free PowerShell executable test that imports those functions through the AST and exercises process-tree timeout, concurrent collisions, forced-offline source shape, exact client `--pull=never` placement, cleanup containment, and exit semantics without Docker; only after it passes, rerun both real clients through the one archive-only build path.

**Tech Stack:** PowerShell 7, .NET `System.Diagnostics.Process`/`ProcessStartInfo.ArgumentList`, Docker Compose, local `rust:latest`, `rclone/rclone:1.74.4`, and `minio/mc:latest`; Rust 2024 `tests/e2e.rs` remains unchanged.

**Receipt status:** `waiting for receipt`

**Global Constraints:**
- Start from `master` at `a3158cf17a89dab054a45bd6ac2be0af1254a00a` and the exact dirty-tree baseline recorded below; do not reset or overwrite completed work.
- Do not modify gateway production code, `tests/e2e.rs`, `docker-compose.yml`, Cargo manifests/lockfile, configuration, or integration tests.
- `tests/e2e.rs` must retain Git blob hash `6756f469a5d23f3f6ac5722e3112f548d401e302` and its 11 `#[tokio::test]` cases.
- Do not install software, pull images, stage, commit, push, tag, or perform another Git write.
- Do not run `docker compose down -v`, stop services, delete volumes, or delete the evidence log.
- Every `-Run` build must be archive-only, `--network none`, `--pull=false`, and followed by Compose `--pull never --no-build`; cached standard images must not select another path.
- The Rclone, MinIO `mc`, and AWS wrapper `docker run` entries and their three version probes are the only six client-container entry points; each exact source prefix is `"run", "--rm", "--pull=never"`, with no intervening or split argument.
- Every runner-owned Docker/client/Cargo/tar process must use `Invoke-NativeCommand`; do not use `Start-Job`, a shell command string, or an unkillable timeout wrapper.
- Fixed child-process budgets are 5 minutes for ordinary Docker/client commands, 5 minutes for `cargo vendor`, 5 minutes for `tar.exe`, and 30 minutes for the offline build.
- The only removable artifacts are `vendor`, `vendor-archive-context`, and `Dockerfile.gateway-runtime` under the current invocation's unique `RunRoot`; cleanup runs after `Stop-Transcript` and preserves `client-smoke.log`.
- Do not execute Docker until Task 5. Tasks 1–4 are source editing and non-Docker verification only.

---

## Current Dirty-Tree Baseline

Before Task 1, all of these assertions must pass:

```powershell
$expectedHead = "a3158cf17a89dab054a45bd6ac2be0af1254a00a"
if ((git rev-parse HEAD).Trim() -ne $expectedHead) { throw "Unexpected HEAD" }
if ((git branch --show-current).Trim() -ne "master") { throw "Expected master" }

$expectedStatus = @(
    " M scripts/client-smoke.ps1",
    " M tests/e2e.rs",
    "?? docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md",
    "?? docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md"
) | Sort-Object
$actualStatus = @(git status --short) | Sort-Object
$delta = @(Compare-Object -ReferenceObject $expectedStatus -DifferenceObject $actualStatus)
if ($delta.Count -ne 0) { $delta | Format-Table | Out-String | Write-Host; throw "Unexpected dirty-tree baseline" }

if ((git hash-object tests/e2e.rs).Trim() -ne "6756f469a5d23f3f6ac5722e3112f548d401e302") {
    throw "Completed tests/e2e.rs changed"
}
if ([int](rg -c '#\[tokio::test\]' tests/e2e.rs) -ne 11) { throw "Expected 11 e2e tests" }
if ((git hash-object scripts/client-smoke.ps1).Trim() -ne "0ac8e3c88bde617df60b018c0b4988a810055ac8") {
    throw "Runner input no longer matches the reviewed dirty-tree baseline"
}
```

Expected: all guards exit 0. This plan deliberately begins from the current dirty runner, including its already-proven archive transport; it does not revert to HEAD.

## File Map

### Modify
- `scripts/client-smoke.ps1` — unique RunId/root, bounded native command helper, unconditional offline build path, strict preflight exit semantics, and exact post-transcript cleanup.
- `docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md` — authoritative remediation design; no implementation worker should edit it.
- `docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md` — this execution contract; no implementation worker should edit it.

### Create
- `tests/client-smoke.Tests.ps1` — dependency-free executable PowerShell tests; it never contacts Docker.

### Read and execute unchanged
- `tests/e2e.rs` — completed 11/11 Rust Docker suite; hash guard only.
- `docker-compose.yml` — service topology read by the runner.

## Execution Boundaries

1. Execute Tasks 1–6 in order. Stop on the first failed gate.
2. Task 1 creates RED tests only. Tasks 2–3 modify only `scripts/client-smoke.ps1`. Task 4 is non-Docker GREEN. Task 5 is the only Docker/live-client task. Task 6 packages evidence without a repository artifact.
3. No task stages or commits. No plan step uses `docker pull`, `docker compose down`, or `-CleanupVolumes`.
4. A source or test edit after Task 6 review evidence is assembled invalidates that package; rerun Tasks 4–6.

---

### Task 1: Add executable RED tests for all final-review findings

**Files:**
- Create: `tests/client-smoke.Tests.ps1`
- Read: `scripts/client-smoke.ps1`

**Interfaces:**
- Consumes: the PowerShell AST of `scripts/client-smoke.ps1` and future functions `Convert-NativeTextToLines`, `Get-NativeDiagnostic`, `Invoke-NativeCommand`, `New-SmokeRunId`, `New-SmokeRunRoot`, `New-SmokeBucketName`, `Test-PreflightMustFail`, `Assert-CanonicalChildPath`, and `Remove-OwnedBuildArtifacts`; native helpers are imported in that dependency order and need no runner-global variables.
- Produces: one non-Docker executable test whose final line is `client-smoke infrastructure tests: PASSED`.

- [ ] **Step 1: Create the complete PowerShell test**

Create `tests/client-smoke.Tests.ps1` with exactly this content:

```powershell
$ErrorActionPreference = "Stop"

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$RunnerPath = Join-Path $RepoRoot "scripts/client-smoke.ps1"
$RunnerSource = [IO.File]::ReadAllText($RunnerPath)
$tokens = $null
$parseErrors = $null
$RunnerAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $RunnerPath,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -ne 0) {
    $parseErrors | Format-List | Out-String | Write-Host
    throw "scripts/client-smoke.ps1 has parse errors"
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw $Message }
}

function Get-RunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)
    $matches = @($RunnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one function named $Name, found $($matches.Count)" }
    return $matches[0].Extent.Text
}

$requiredFunctions = @(
    "Convert-NativeTextToLines",
    "Get-NativeDiagnostic",
    "Invoke-NativeCommand",
    "New-SmokeRunId",
    "Assert-CanonicalChildPath",
    "New-SmokeRunRoot",
    "New-SmokeBucketName",
    "Test-PreflightMustFail",
    "Remove-OwnedBuildArtifacts"
)
foreach ($name in $requiredFunctions) {
    Invoke-Expression (Get-RunnerFunctionSource $name)
}

$TestRoot = Join-Path ([IO.Path]::GetTempPath()) ("ipfs-s3-client-smoke-tests-" + [Guid]::NewGuid().ToString("N"))
if (Test-Path -LiteralPath $TestRoot) { throw "Unique test root already exists: $TestRoot" }
$null = New-Item -ItemType Directory -Path $TestRoot
$independentSleeper = $null

try {
    $pwshPath = (Get-Command pwsh -ErrorAction Stop).Source

    # ArgumentList must preserve one literal metacharacter-bearing argument.
    $echoScript = Join-Path $TestRoot "echo-argument.ps1"
    $echoOutput = Join-Path $TestRoot "echo-output.txt"
    $injectionSentinel = Join-Path $TestRoot "injection-sentinel.txt"
    [IO.File]::WriteAllText($echoScript, @'
param([string]$OutputPath, [string]$Value)
[IO.File]::WriteAllText($OutputPath, $Value, [Text.UTF8Encoding]::new($false))
'@, [Text.UTF8Encoding]::new($false))
    $literalArgument = "literal value; write forbidden > `"$injectionSentinel`" & stop"
    $argumentResult = Invoke-NativeCommand `
        -FilePath $pwshPath `
        -ArgumentList @("-NoProfile", "-File", $echoScript, $echoOutput, $literalArgument) `
        -Label "literal argument probe" `
        -Timeout ([TimeSpan]::FromSeconds(10))
    Assert-True ($argumentResult.ExitCode -eq 0) "Literal argument probe failed"
    Assert-True ([IO.File]::ReadAllText($echoOutput) -ceq $literalArgument) "ArgumentList changed the literal argument"
    Assert-True (-not [IO.File]::Exists($injectionSentinel)) "Shell metacharacters were executed"

    # Timeout must kill only the fake parent tree, not an independently launched sleeper.
    $independentStartInfo = [Diagnostics.ProcessStartInfo]::new()
    $independentStartInfo.FileName = $pwshPath
    $independentStartInfo.UseShellExecute = $false
    $null = $independentStartInfo.ArgumentList.Add("-NoProfile")
    $null = $independentStartInfo.ArgumentList.Add("-Command")
    $null = $independentStartInfo.ArgumentList.Add("Start-Sleep -Seconds 600")
    $independentSleeper = [Diagnostics.Process]::Start($independentStartInfo)
    Assert-True ($null -ne $independentSleeper -and -not $independentSleeper.HasExited) "Independent control sleeper did not start"

    $treeScript = Join-Path $TestRoot "native-tree.ps1"
    $parentPidPath = Join-Path $TestRoot "native-parent.pid"
    $childPidPath = Join-Path $TestRoot "native-child.pid"
    [IO.File]::WriteAllText($treeScript, @'
param([string]$ParentPidPath, [string]$ChildPidPath, [string]$PwshPath)
[IO.File]::WriteAllText($ParentPidPath, [string]$PID, [Text.UTF8Encoding]::new($false))
$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $PwshPath
$startInfo.UseShellExecute = $false
$null = $startInfo.ArgumentList.Add("-NoProfile")
$null = $startInfo.ArgumentList.Add("-Command")
$null = $startInfo.ArgumentList.Add("Start-Sleep -Seconds 120")
$child = [Diagnostics.Process]::Start($startInfo)
[IO.File]::WriteAllText($ChildPidPath, [string]$child.Id, [Text.UTF8Encoding]::new($false))
Start-Sleep -Seconds 120
'@, [Text.UTF8Encoding]::new($false))
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $timeoutMessage = $null
    try {
        Invoke-NativeCommand `
            -FilePath $pwshPath `
            -ArgumentList @("-NoProfile", "-File", $treeScript, $parentPidPath, $childPidPath, $pwshPath) `
            -Label "fake native tree" `
            -Timeout ([TimeSpan]::FromSeconds(5)) | Out-Null
    } catch {
        $timeoutMessage = $_.Exception.Message
    } finally {
        $timer.Stop()
    }
    Assert-True ($null -ne $timeoutMessage) "Fake native tree did not time out"
    Assert-True ($timeoutMessage.Contains("fake native tree")) "Timeout error omitted its label"
    Assert-True ($timeoutMessage.Contains("timed out")) "Timeout error omitted timeout classification"
    Assert-True ($timer.Elapsed -lt [TimeSpan]::FromSeconds(15)) "Timeout was not wall-clock bounded"
    Assert-True ([IO.File]::Exists($parentPidPath)) "Fake parent did not publish its PID"
    Assert-True ([IO.File]::Exists($childPidPath)) "Fake parent did not publish its child PID"
    $treePids = @(
        [int][IO.File]::ReadAllText($parentPidPath),
        [int][IO.File]::ReadAllText($childPidPath)
    )
    foreach ($treePid in $treePids) {
        for ($attempt = 0; $attempt -lt 50 -and $null -ne (Get-Process -Id $treePid -ErrorAction SilentlyContinue); $attempt++) {
            Start-Sleep -Milliseconds 100
        }
        Assert-True ($null -eq (Get-Process -Id $treePid -ErrorAction SilentlyContinue)) "Timed-out tree PID survived Kill(true): $treePid"
    }
    $independentSleeper.Refresh()
    Assert-True (-not $independentSleeper.HasExited) "Kill(true) terminated the independent control sleeper"

    # Exactly one concurrent creator may own a requested RunRoot.
    $collisionParent = Join-Path $TestRoot "collision-parent"
    $null = New-Item -ItemType Directory -Path $collisionParent
    $collisionRunId = "20260720t120000000z-$PID-deadbeef"
    $rootFunctionSource = @(
        Get-RunnerFunctionSource "Assert-CanonicalChildPath"
        Get-RunnerFunctionSource "New-SmokeRunRoot"
    ) -join [Environment]::NewLine
    $collisionResults = @(1..8 | ForEach-Object -Parallel {
        Invoke-Expression $using:rootFunctionSource
        try {
            $null = New-SmokeRunRoot -TempRoot $using:collisionParent -RunId $using:collisionRunId
            "CREATED"
        } catch {
            "COLLISION"
        }
    } -ThrottleLimit 8)
    Assert-True (@($collisionResults | Where-Object { $_ -eq "CREATED" }).Count -eq 1) "Concurrent RunRoot creation did not have exactly one owner"
    Assert-True (@($collisionResults | Where-Object { $_ -eq "COLLISION" }).Count -eq 7) "Concurrent RunRoot collision count was not seven"

    # RunId and bucket contracts are one canonical grammar.
    $runIds = @(1..64 | ForEach-Object { New-SmokeRunId })
    Assert-True (($runIds | Sort-Object -Unique).Count -eq 64) "Generated RunIds were not unique"
    foreach ($runId in $runIds) {
        Assert-True ($runId -cmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') "Invalid RunId: $runId"
        foreach ($prefix in @("ipfs-s3-rclone", "ipfs-s3-mc", "ipfs-s3-aws")) {
            $bucket = New-SmokeBucketName -Prefix $prefix -RunId $runId
            Assert-True ($bucket.Length -le 63) "Bucket exceeds 63 characters: $bucket"
            Assert-True ($bucket -cmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') "Bucket is not S3-safe: $bucket"
        }
    }

    # Cleanup must remove only exact owned children and reject an outside root.
    $cleanupParent = Join-Path $TestRoot "cleanup-parent"
    $null = New-Item -ItemType Directory -Path $cleanupParent
    $cleanupRunId = "20260720t120000001z-$PID-cafebabe"
    $cleanupRoot = New-SmokeRunRoot -TempRoot $cleanupParent -RunId $cleanupRunId
    $null = New-Item -ItemType Directory -Path (Join-Path $cleanupRoot "vendor")
    $null = New-Item -ItemType Directory -Path (Join-Path $cleanupRoot "vendor-archive-context")
    [IO.File]::WriteAllText((Join-Path $cleanupRoot "vendor/payload"), "owned")
    [IO.File]::WriteAllText((Join-Path $cleanupRoot "vendor-archive-context/vendor.tar.gz"), "owned")
    [IO.File]::WriteAllText((Join-Path $cleanupRoot "Dockerfile.gateway-runtime"), "owned")
    [IO.File]::WriteAllText((Join-Path $cleanupRoot "client-smoke.log"), "retain")
    [IO.File]::WriteAllText((Join-Path $cleanupRoot "file.txt"), "retain")
    $unrelated = Join-Path $cleanupRoot "unrelated"
    $null = New-Item -ItemType Directory -Path $unrelated
    [IO.File]::WriteAllText((Join-Path $unrelated "sentinel"), "retain")
    Remove-OwnedBuildArtifacts -TempRoot $cleanupParent -RunRoot $cleanupRoot -RunId $cleanupRunId
    foreach ($removed in @("vendor", "vendor-archive-context", "Dockerfile.gateway-runtime")) {
        Assert-True (-not (Test-Path -LiteralPath (Join-Path $cleanupRoot $removed))) "Owned artifact survived: $removed"
    }
    foreach ($retained in @("client-smoke.log", "file.txt", "unrelated/sentinel")) {
        Assert-True (Test-Path -LiteralPath (Join-Path $cleanupRoot $retained)) "Non-owned artifact was deleted: $retained"
    }

    $outsideParent = Join-Path $TestRoot "outside-parent"
    $null = New-Item -ItemType Directory -Path $outsideParent
    $outsideRoot = Join-Path $outsideParent "ipfs-s3-client-smoke-$cleanupRunId"
    $null = New-Item -ItemType Directory -Path $outsideRoot
    $outsideVendor = Join-Path $outsideRoot "vendor"
    $null = New-Item -ItemType Directory -Path $outsideVendor
    [IO.File]::WriteAllText((Join-Path $outsideVendor "sentinel"), "retain")
    $outsideRejected = $false
    try {
        Remove-OwnedBuildArtifacts -TempRoot $cleanupParent -RunRoot $outsideRoot -RunId $cleanupRunId
    } catch {
        $outsideRejected = $true
    }
    Assert-True $outsideRejected "Cleanup accepted a RunRoot outside TempRoot"
    Assert-True ([IO.File]::Exists((Join-Path $outsideVendor "sentinel"))) "Rejected cleanup deleted an outside sentinel"

    # Source/AST invariants force one offline path and one native boundary.
    $offlineSource = Get-RunnerFunctionSource "Invoke-OfflineGatewayBuild"
    foreach ($fragment in @(
        'if (Test-Path -LiteralPath $archiveContext)',
        'throw "archive context already exists:',
        'New-Item -ItemType Directory -Path $archiveContext',
        '"--build-context", "vendor-archive=$archiveContext"',
        '"build", "--pull=false", "--network", "none", "--quiet"',
        '-Timeout $OfflineBuildTimeout'
    )) {
        Assert-True ($offlineSource.Contains($fragment)) "Offline build fragment missing: $fragment"
    }
    Assert-True (-not $offlineSource.Contains('Remove-Item -LiteralPath $archiveContext')) "Archive context is pre-deleted"

    $allClientRunRm = [regex]::Matches($RunnerSource, '"run"\s*,\s*"--rm"')
    $pullLockedClientRuns = [regex]::Matches(
        $RunnerSource,
        [regex]::Escape('"run", "--rm", "--pull=never"')
    )
    Assert-True ($allClientRunRm.Count -eq 6) "Expected exactly six client docker run --rm entry points, found $($allClientRunRm.Count)"
    Assert-True ($pullLockedClientRuns.Count -eq 6) "Expected exactly six immediate --pull=never client prefixes, found $($pullLockedClientRuns.Count)"
    Assert-True (($allClientRunRm.Count - $pullLockedClientRuns.Count) -eq 0) "A client docker run --rm entry is not immediately followed by the single --pull=never argument"

    $mainSource = Get-RunnerFunctionSource "Invoke-SmokeMain"
    foreach ($fragment in @(
        'Invoke-OfflineGatewayBuild',
        '"up", "-d", "--pull", "never", "--no-build", "kubo", "gateway"'
    )) {
        Assert-True ($mainSource.Contains($fragment)) "Forced-offline main fragment missing: $fragment"
    }
    foreach ($fragment in @("StandardBuildImages", "missingStandardBuild", "useOfflineGatewayBuild", '"--build"', "docker pull", '"down", "-v"', "CleanupVolumes")) {
        Assert-True (-not $RunnerSource.Contains($fragment)) "Forbidden runner fragment remains: $fragment"
    }
    Assert-True (-not $RunnerSource.Contains('$Stamp')) "Legacy Stamp remains"
    Assert-True ($RunnerSource.Contains('$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$RunId"')) "Evidence placeholder is not RunId-based"
    Assert-True (-not $RunnerSource.Contains('\d{14}')) "Fixed 14-digit evidence regex remains"

    $directNative = @($RunnerAst.FindAll({
        param($node)
        if ($node -isnot [System.Management.Automation.Language.CommandAst]) { return $false }
        $name = $node.GetCommandName()
        return $name -in @("docker", "cargo", "tar.exe")
    }, $true))
    Assert-True ($directNative.Count -eq 0) "Direct docker/cargo/tar invocation remains: $($directNative.Extent.Text -join '; ')"
    Assert-True (-not $RunnerSource.Contains("Start-Job")) "Start-Job is forbidden"
    foreach ($fragment in @(
        '[TimeSpan]::FromMinutes(5)',
        '[TimeSpan]::FromMinutes(30)',
        '[Diagnostics.ProcessStartInfo]::new()',
        '$startInfo.ArgumentList.Add($argument)',
        'ReadToEndAsync()',
        'WaitForExit($timeoutMilliseconds)',
        'Kill($true)'
    )) {
        Assert-True ($RunnerSource.Contains($fragment)) "Native timeout fragment missing: $fragment"
    }

    # Exit classification is explicit and independently executable.
    Assert-True (-not (Test-PreflightMustFail -RunRequested $false -RunnableClientCount 0 -PrerequisiteUnavailable $true)) "No-Run preflight must exit zero"
    Assert-True (Test-PreflightMustFail -RunRequested $true -RunnableClientCount 0 -PrerequisiteUnavailable $true) "Requested unavailable run must fail"
    Assert-True (Test-PreflightMustFail -RunRequested $true -RunnableClientCount 0 -PrerequisiteUnavailable $false) "Requested all-missing run must fail"
    Assert-True (-not (Test-PreflightMustFail -RunRequested $true -RunnableClientCount 1 -PrerequisiteUnavailable $false)) "Partial All availability must remain runnable"

    # Real entry-point behavior with Docker hidden: no-Run is zero; -Run is nonzero.
    $oldPath = $env:PATH
    $oldTemp = $env:TEMP
    $oldTmp = $env:TMP
    try {
        $env:PATH = ""
        $env:TEMP = $TestRoot
        $env:TMP = $TestRoot
        $dryResult = Invoke-NativeCommand `
            -FilePath $pwshPath `
            -ArgumentList @("-NoProfile", "-File", $RunnerPath, "-Client", "Rclone") `
            -Label "runner dry unavailable preflight" `
            -Timeout ([TimeSpan]::FromSeconds(20))
        Assert-True ($dryResult.ExitCode -eq 0) "No-Run unavailable preflight was nonzero"
        Assert-True ((@($dryResult.StdOut) -join "`n").Contains("[RESULT] client=Rclone status=SKIPPED")) "No-Run unavailable preflight omitted SKIPPED"

        $runResult = Invoke-NativeCommand `
            -FilePath $pwshPath `
            -ArgumentList @("-NoProfile", "-File", $RunnerPath, "-Client", "Rclone", "-Run") `
            -Label "runner requested unavailable preflight" `
            -Timeout ([TimeSpan]::FromSeconds(20)) `
            -AllowedExitCodes @(0, 1)
        Assert-True ($runResult.ExitCode -eq 1) "Requested unavailable preflight did not exit one"
        Assert-True ((@($runResult.StdOut) -join "`n").Contains("[RESULT] client=Rclone status=SKIPPED")) "Requested unavailable preflight omitted SKIPPED"
    } finally {
        $env:PATH = $oldPath
        $env:TEMP = $oldTemp
        $env:TMP = $oldTmp
    }

    Write-Host "client-smoke infrastructure tests: PASSED"
} finally {
    try {
        if ($null -ne $independentSleeper) {
            try {
                $independentSleeper.Refresh()
                if (-not $independentSleeper.HasExited) {
                    $independentSleeper.Kill($true)
                    if (-not $independentSleeper.WaitForExit(10000)) {
                        throw "Independent control sleeper did not terminate during test cleanup"
                    }
                }
            } finally {
                $independentSleeper.Dispose()
            }
        }
    } finally {
        if (Test-Path -LiteralPath $TestRoot) {
            Remove-Item -LiteralPath $TestRoot -Recurse -Force
        }
    }
}
```

- [ ] **Step 2: Run the test and confirm the intended RED**

Run:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

Expected RED: exit 1 with `Expected one function named Convert-NativeTextToLines, found 0`. The test must fail before any Docker call; do not weaken it to accommodate the current runner. After Task 2 adds the three native helpers, their dependency-ordered import makes the first literal-argument child probe executable without dot-sourcing runner top-level code.

---

### Task 2: Implement unique identity, bounded native execution, and exact cleanup

**Files:**
- Modify: `scripts/client-smoke.ps1:1-210,518-526`
- Test: `tests/client-smoke.Tests.ps1`

**Interfaces:**
- Consumes: existing client arguments, temporary root, Docker/Cargo/tar executables, and transcript lifecycle.
- Produces: `New-SmokeRunId()`, `Assert-CanonicalChildPath(ParentPath, ChildPath)`, `New-SmokeRunRoot(TempRoot, RunId)`, `New-SmokeBucketName(Prefix, RunId)`, `Convert-NativeTextToLines(Text)`, `Get-NativeDiagnostic(StdOut, StdErr)`, `Invoke-NativeCommand(FilePath, ArgumentList, Label, Timeout, AllowedExitCodes)`, `Invoke-Docker(Arguments, Timeout, Label)`, and `Remove-OwnedBuildArtifacts(TempRoot, RunRoot, RunId)`.

- [ ] **Step 1: Replace parameter/global identity initialization with the frozen RunId contract**

Remove `CleanupVolumes` from `param`, remove every `$Stamp` assignment/use, and place this block after `$ComposeFile` is assigned and before `$PortablePathReplacements` is created. Keep the existing `Convert-ToDockerDesktopPath` definition before the initialization call because the replacement table uses it.

```powershell
$DockerCommandTimeout = [TimeSpan]::FromMinutes(5)
$CargoVendorTimeout = [TimeSpan]::FromMinutes(5)
$TarTimeout = [TimeSpan]::FromMinutes(5)
$OfflineBuildTimeout = [TimeSpan]::FromMinutes(30)
$TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())

function New-SmokeRunId {
    $timestamp = [DateTime]::UtcNow.ToString(
        "yyyyMMddTHHmmssfffZ",
        [Globalization.CultureInfo]::InvariantCulture
    ).ToLowerInvariant()
    $guidSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 8).ToLowerInvariant()
    $runId = "$timestamp-$PID-$guidSuffix"
    if ($runId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Generated invalid RunId: $runId"
    }
    return $runId
}

function Assert-CanonicalChildPath {
    param(
        [Parameter(Mandatory)][string]$ParentPath,
        [Parameter(Mandatory)][string]$ChildPath
    )
    $canonicalParent = [IO.Path]::GetFullPath($ParentPath).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalChild = [IO.Path]::GetFullPath($ChildPath)
    $prefix = $canonicalParent + [IO.Path]::DirectorySeparatorChar
    if (-not $canonicalChild.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path is outside the owned root: $canonicalChild"
    }
    return $canonicalChild
}

function New-SmokeRunRoot {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    if ($RunId -cnotmatch '^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$') {
        throw "Invalid RunId for RunRoot: $RunId"
    }
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    if (-not (Test-Path -LiteralPath $canonicalTemp -PathType Container)) {
        throw "Temporary root must already exist: $canonicalTemp"
    }
    $runRoot = Assert-CanonicalChildPath `
        -ParentPath $canonicalTemp `
        -ChildPath (Join-Path $canonicalTemp "ipfs-s3-client-smoke-$RunId")
    if (-not [IO.Path]::GetDirectoryName($runRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot must be a direct child of the temporary root: $runRoot"
    }
    if (Test-Path -LiteralPath $runRoot) { throw "RunRoot collision: $runRoot" }
    try {
        $created = New-Item -ItemType Directory -Path $runRoot -ErrorAction Stop
    } catch {
        throw "RunRoot creation failed or collided: $runRoot; $($_.Exception.Message)"
    }
    if (-not $created.FullName.Equals($runRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Created RunRoot differs from requested path: $($created.FullName)"
    }
    return $runRoot
}

function New-SmokeBucketName {
    param(
        [Parameter(Mandatory)][string]$Prefix,
        [Parameter(Mandatory)][string]$RunId
    )
    $bucket = "$Prefix-$RunId"
    if ($bucket.Length -gt 63 -or $bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid S3 bucket name: $bucket"
    }
    return $bucket
}

$RunId = New-SmokeRunId
$RunRoot = New-SmokeRunRoot -TempRoot $TempRoot -RunId $RunId
$LogPath = Join-Path $RunRoot "client-smoke.log"
$FixturePath = Join-Path $RunRoot "file.txt"
$FixtureText = "ipfs-s3-client-smoke-$RunId"
$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$RunId"
$EvidenceRepoRoot = "<repo>"
$EvidenceLogPath = "client-smoke.log"
[IO.File]::WriteAllText($FixturePath, $FixtureText, [Text.UTF8Encoding]::new($false))
```

The `New-Item` call intentionally has no `-Force`. Do not replace it with `Directory.CreateDirectory`, because that API silently adopts an existing directory.

- [ ] **Step 2: Add the single native-process implementation and route Docker through it**

Place these functions after `Convert-NativeOutputItem`; replace the existing `Invoke-Docker` and `Test-LocalImage` definitions with the versions below:

```powershell
function Convert-NativeTextToLines {
    param([AllowNull()][string]$Text)
    if ([string]::IsNullOrEmpty($Text)) { return @() }
    return @($Text -split "\r?\n" | Where-Object { $_.Length -gt 0 })
}

function Get-NativeDiagnostic {
    param([string[]]$StdOut, [string[]]$StdErr)
    $diagnostic = (@($StdErr) + @($StdOut) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join " | "
    if ($diagnostic.Length -gt 2000) { return $diagnostic.Substring($diagnostic.Length - 2000) }
    return $diagnostic
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][TimeSpan]$Timeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    if ($Timeout -le [TimeSpan]::Zero -or $Timeout.TotalMilliseconds -gt [int]::MaxValue) {
        throw "$Label has an invalid timeout: $Timeout"
    }
    if ($AllowedExitCodes.Count -eq 0) { throw "$Label requires at least one allowed exit code" }

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true
    foreach ($argument in $ArgumentList) {
        $null = $startInfo.ArgumentList.Add($argument)
    }

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        try {
            if (-not $process.Start()) { throw "Process.Start returned false" }
        } catch {
            throw "$Label failed to start '$FilePath': $($_.Exception.Message)"
        }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timeoutMilliseconds = [int][Math]::Ceiling($Timeout.TotalMilliseconds)
        if (-not $process.WaitForExit($timeoutMilliseconds)) {
            try {
                $process.Kill($true)
            } catch {
                throw "$Label timed out after $($Timeout.TotalSeconds) seconds and Kill(true) failed: $($_.Exception.Message)"
            }
            if (-not $process.WaitForExit(10000)) {
                throw "$Label timed out after $($Timeout.TotalSeconds) seconds and its process tree did not terminate within 10 seconds"
            }
            $stdout = @(Convert-NativeTextToLines ($stdoutTask.GetAwaiter().GetResult()))
            $stderr = @(Convert-NativeTextToLines ($stderrTask.GetAwaiter().GetResult()))
            $diagnostic = Get-NativeDiagnostic -StdOut $stdout -StdErr $stderr
            $suffix = if ([string]::IsNullOrWhiteSpace($diagnostic)) { "" } else { "; $diagnostic" }
            throw "$Label timed out after $($Timeout.TotalSeconds) seconds; process tree terminated$suffix"
        }
        $stdout = @(Convert-NativeTextToLines ($stdoutTask.GetAwaiter().GetResult()))
        $stderr = @(Convert-NativeTextToLines ($stderrTask.GetAwaiter().GetResult()))
        $exitCode = $process.ExitCode
        if ($exitCode -notin $AllowedExitCodes) {
            $diagnostic = Get-NativeDiagnostic -StdOut $stdout -StdErr $stderr
            $suffix = if ([string]::IsNullOrWhiteSpace($diagnostic)) { "" } else { "; $diagnostic" }
            throw "$Label exited $exitCode$suffix"
        }
        return [pscustomobject]@{
            ExitCode = $exitCode
            StdOut = $stdout
            StdErr = $stderr
        }
    } finally {
        $process.Dispose()
    }
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][string[]]$Arguments,
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [string]$Label = ""
    )
    $displayCommand = Convert-ToPortableEvidence ("docker " + ($Arguments -join " "))
    Write-Host $displayCommand
    if ([string]::IsNullOrWhiteSpace($Label)) { $Label = "docker $($Arguments[0])" }
    $result = Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList $Arguments `
        -Label $Label `
        -Timeout $Timeout
    foreach ($line in @($result.StdOut) + @($result.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    return @($result.StdOut)
}

function Test-LocalImage {
    param([Parameter(Mandatory)][string]$Image)
    $result = Invoke-NativeCommand `
        -FilePath "docker" `
        -ArgumentList @("image", "inspect", $Image, "--format", "{{.Id}}") `
        -Label "inspect local image $Image" `
        -Timeout $DockerCommandTimeout `
        -AllowedExitCodes @(0, 1)
    return $result.ExitCode -eq 0
}
```

- [ ] **Step 3: Replace `Invoke-OfflineGatewayBuild` with fresh-context, bounded archive transport**

Replace that function in full:

```powershell
function Invoke-OfflineGatewayBuild {
    $runtimeImage = "ghcr.io/hugefiver/ipfs3:latest"
    $vendorPath = Join-Path $RunRoot "vendor"
    $archiveContext = Join-Path $RunRoot "vendor-archive-context"
    $vendorArchive = Join-Path $archiveContext "vendor.tar.gz"
    $dockerfile = Join-Path $RunRoot "Dockerfile.gateway-runtime"

    Write-Host (Convert-ToPortableEvidence "cargo vendor --locked --offline $vendorPath")
    $vendorResult = Invoke-NativeCommand `
        -FilePath "cargo" `
        -ArgumentList @("vendor", "--locked", "--offline", $vendorPath) `
        -Label "cargo vendor offline dependency tree" `
        -Timeout $CargoVendorTimeout
    foreach ($line in @($vendorResult.StdOut) + @($vendorResult.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    Write-Host "cargo vendor completed from the local cache"

    if (Test-Path -LiteralPath $archiveContext) {
        throw "archive context already exists: $(Convert-ToPortableEvidence $archiveContext)"
    }
    $null = New-Item -ItemType Directory -Path $archiveContext
    if ($null -eq (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        throw "installed tar.exe is required for offline vendor transport"
    }
    Write-Host (Convert-ToPortableEvidence "tar.exe -czf $vendorArchive -C $vendorPath .")
    $tarResult = Invoke-NativeCommand `
        -FilePath "tar.exe" `
        -ArgumentList @("-czf", $vendorArchive, "-C", $vendorPath, ".") `
        -Label "create offline vendor archive" `
        -Timeout $TarTimeout
    foreach ($line in @($tarResult.StdOut) + @($tarResult.StdErr)) {
        Write-Host (Convert-ToPortableEvidence $line)
    }
    $archiveItems = @(Get-ChildItem -LiteralPath $archiveContext -Force)
    if ($archiveItems.Count -ne 1 -or $archiveItems[0].PSIsContainer -or
        -not $archiveItems[0].FullName.Equals($vendorArchive, [StringComparison]::OrdinalIgnoreCase)) {
        throw "vendor archive context must contain only vendor.tar.gz"
    }
    Write-Host (Convert-ToPortableEvidence "vendor archive completed: $vendorArchive")

    [IO.File]::WriteAllText(
        $dockerfile,
        @"
FROM rust:latest AS builder
WORKDIR /app
COPY --from=vendor-archive vendor.tar.gz /tmp/vendor.tar.gz
RUN mkdir /vendor && tar -xzf /tmp/vendor.tar.gz -C /vendor && rm /tmp/vendor.tar.gz
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo --config 'source.crates-io.replace-with="vendored-sources"' --config 'source.vendored-sources.directory="/vendor"' build --release --locked --offline --bin ipfs-s3-gateway

FROM $runtimeImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker `
        -Arguments @(
            "build", "--pull=false", "--network", "none", "--quiet",
            "--build-context", "vendor-archive=$archiveContext", "--tag", $runtimeImage,
            "--file", $dockerfile, $RepoRoot
        ) `
        -Timeout $OfflineBuildTimeout `
        -Label "offline gateway image build" | Out-Null
}
```

The `&&` tokens are inside the Linux Dockerfile and are not PowerShell syntax.

- [ ] **Step 4: Add exact cleanup and replace the outer transcript/exit block**

Add this function before `Invoke-SmokeMain`:

```powershell
function Remove-OwnedBuildArtifacts {
    param(
        [Parameter(Mandatory)][string]$TempRoot,
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId
    )
    $canonicalTemp = [IO.Path]::GetFullPath($TempRoot).TrimEnd(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar
    )
    $canonicalRunRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $RunRoot
    $expectedRunRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-client-smoke-$RunId"))
    if (-not $canonicalRunRoot.Equals($expectedRunRoot, [StringComparison]::OrdinalIgnoreCase) -or
        -not [IO.Path]::GetDirectoryName($canonicalRunRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Cleanup RunRoot is not the exact invocation root: $canonicalRunRoot"
    }

    $owned = @(
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "vendor"; Kind = "Container" },
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "vendor-archive-context"; Kind = "Container" },
        [pscustomobject]@{ Path = Join-Path $canonicalRunRoot "Dockerfile.gateway-runtime"; Kind = "Leaf" }
    )
    foreach ($artifact in $owned) {
        $canonicalArtifact = Assert-CanonicalChildPath -ParentPath $canonicalRunRoot -ChildPath $artifact.Path
        if (-not $canonicalArtifact.Equals([IO.Path]::GetFullPath($artifact.Path), [StringComparison]::OrdinalIgnoreCase)) {
            throw "Owned artifact canonical path mismatch: $canonicalArtifact"
        }
        if (Test-Path -LiteralPath $canonicalArtifact -PathType Container) {
            if ($artifact.Kind -ne "Container") { throw "Owned artifact type mismatch: $canonicalArtifact" }
            Remove-Item -LiteralPath $canonicalArtifact -Recurse -Force
        } elseif (Test-Path -LiteralPath $canonicalArtifact -PathType Leaf) {
            if ($artifact.Kind -ne "Leaf") { throw "Owned artifact type mismatch: $canonicalArtifact" }
            Remove-Item -LiteralPath $canonicalArtifact -Force
        } elseif (Test-Path -LiteralPath $canonicalArtifact) {
            throw "Owned artifact has an unsupported filesystem type: $canonicalArtifact"
        }
    }
}
```

Replace lines from `Start-Transcript` through the final `exit` with:

```powershell
$exitCode = 1
$transcriptStarted = $false
try {
    Start-Transcript -LiteralPath $LogPath | Out-Null
    $transcriptStarted = $true
    $failed = Invoke-SmokeMain
    $exitCode = if ($failed) { 1 } else { 0 }
} catch {
    Write-Host "Runner failed: $(Convert-ToPortableEvidence $_.Exception.Message)"
    $exitCode = 1
} finally {
    if ($transcriptStarted) {
        try {
            Stop-Transcript | Out-Null
        } catch {
            Write-Host "Stop-Transcript failed: $($_.Exception.Message)"
            $exitCode = 1
        }
    }
    try {
        Remove-OwnedBuildArtifacts -TempRoot $TempRoot -RunRoot $RunRoot -RunId $RunId
    } catch {
        Write-Host "Owned build artifact cleanup failed: $($_.Exception.Message)"
        $exitCode = 1
    }
}
Write-Host "Evidence log: $EvidenceLogPath"
exit $exitCode
```

`Stop-Transcript` is intentionally attempted before cleanup. Do not add `-Force` to `Start-Transcript`; the unique RunRoot guarantees a fresh log path.

---

### Task 3: Force the offline path, pull-lock client runs, and correct requested-run exit semantics

**Files:**
- Modify: `scripts/client-smoke.ps1:67-81,319-526`
- Test: `tests/client-smoke.Tests.ps1`

**Interfaces:**
- Consumes: Task 2 helpers, local image map, selected clients, and existing client smoke functions.
- Produces: one unconditional archive-build path for runnable `-Run`, exactly six client `docker run --rm --pull=never` source entries, bucket-safe names, accurate `SKIPPED`/`FAILED`/`PASSED` lines, and the exit matrix from the design.

- [ ] **Step 1: Remove the standard-build selector, pull-lock all six client runs, and use RunId buckets**

Delete the complete `$StandardBuildImages` array. In `Invoke-Rclone`, `Invoke-Mc`, and `Invoke-Aws`, replace the start of each `$dockerArgs` array with this exact three-argument prefix before its existing `--network` argument:

```powershell
"run", "--rm", "--pull=never", "--network", $Network,
```

Replace the three version probes with these exact calls:

```powershell
Invoke-Docker @("run", "--rm", "--pull=never", $Images.Rclone, "version") | Out-Null
Invoke-Docker @("run", "--rm", "--pull=never", $Images.Mc, "--version") | Out-Null
Invoke-Docker @("run", "--rm", "--pull=never", $Images.Aws, "--version") | Out-Null
```

These wrapper/probe changes are exactly six source entry points. Do not add another client `docker run`, and do not express the pull policy as two arguments. Replace the three bucket assignments with:

```powershell
$bucket = New-SmokeBucketName -Prefix "ipfs-s3-rclone" -RunId $RunId
```

```powershell
$bucket = New-SmokeBucketName -Prefix "ipfs-s3-mc" -RunId $RunId
```

```powershell
$bucket = New-SmokeBucketName -Prefix "ipfs-s3-aws" -RunId $RunId
```

Replace the manual-cleanup diagnostic inside the client catch with:

```powershell
Write-Host "Manual cleanup endpoint: http://127.0.0.1:9000 bucket prefix ipfs-s3-$($name.ToLower())-$RunId"
```

- [ ] **Step 2: Add the pure preflight classifier**

Place this function before `Invoke-SmokeMain`:

```powershell
function Test-PreflightMustFail {
    param(
        [Parameter(Mandatory)][bool]$RunRequested,
        [Parameter(Mandatory)][ValidateRange(0, 3)][int]$RunnableClientCount,
        [Parameter(Mandatory)][bool]$PrerequisiteUnavailable
    )
    if (-not $RunRequested) { return $false }
    return $PrerequisiteUnavailable -or $RunnableClientCount -eq 0
}
```

- [ ] **Step 3: Replace `Invoke-SmokeMain` with the one-path implementation**

Replace the function in full. The client smoke functions themselves remain unchanged except for Task 3 Step 1 pull locks and bucket assignments.

```powershell
function Invoke-SmokeMain {
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker-compose.yml is missing" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker command is missing" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    try {
        $dockerInfo = Invoke-NativeCommand `
            -FilePath "docker" `
            -ArgumentList @("info", "--format", "{{.ServerVersion}}") `
            -Label "docker daemon preflight" `
            -Timeout $DockerCommandTimeout `
            -AllowedExitCodes @(0, 1)
    } catch {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon preflight failed: $($_.Exception.Message)" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }
    if ($dockerInfo.ExitCode -ne 0) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon is unavailable" }
        return (Test-PreflightMustFail -RunRequested ([bool]$Run) -RunnableClientCount 0 -PrerequisiteUnavailable $true)
    }

    $missingClients = @{}
    foreach ($name in $Selected) {
        if (-not (Test-LocalImage $Images[$name])) { $missingClients[$name] = $Images[$name] }
    }
    if (-not $Run) {
        foreach ($name in $Selected) {
            if ($missingClients.ContainsKey($name)) {
                Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
            } else {
                Write-Result $name "SKIPPED" "execution not requested; rerun with -Run"
            }
        }
        return $false
    }

    $runnable = @($Selected | Where-Object { -not $missingClients.ContainsKey($_) })
    foreach ($name in $Selected | Where-Object { $missingClients.ContainsKey($_) }) {
        Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
    }
    if ($runnable.Count -eq 0) {
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount 0 -PrerequisiteUnavailable $false)
    }

    $missingServices = @($ServiceImages | Where-Object { -not (Test-LocalImage $_) })
    if ($missingServices.Count -gt 0) {
        foreach ($name in $runnable) { Write-Result $name "SKIPPED" "local service image missing: $($missingServices -join ', ')" }
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount $runnable.Count -PrerequisiteUnavailable $true)
    }
    if (-not (Test-LocalImage "rust:latest")) {
        foreach ($name in $runnable) { Write-Result $name "SKIPPED" "local build image missing: rust:latest" }
        return (Test-PreflightMustFail -RunRequested $true -RunnableClientCount $runnable.Count -PrerequisiteUnavailable $true)
    }

    try {
        Write-Host "Building the current gateway source from an archive-only offline vendored dependency context."
        Invoke-OfflineGatewayBuild
        Invoke-Docker @(
            "compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway"
        ) | Out-Null
        Wait-GatewayHealthy
        Invoke-Docker @("compose", "-f", $ComposeFile, "ps") | Out-Null
        $network = Get-ComposeNetwork
    } catch {
        $stackError = Convert-ToPortableEvidence $_.Exception.Message
        foreach ($name in $runnable) { Write-Result $name "FAILED" "stack setup failed: $stackError" }
        Write-Host "Stack setup failed before client execution; per-client smoke loop was not entered."
        Write-Host "Compose services and volumes preserved for diagnosis."
        return $true
    }

    $failed = $false
    foreach ($name in $runnable) {
        try {
            $imageId = Get-ImageEvidence $Images[$name]
            Write-Host "client=$name image=$($Images[$name]) image_id=$imageId"
            switch ($name) {
                "Rclone" { Invoke-RcloneSmoke $network }
                "Mc" { Invoke-McSmoke $network }
                "Aws" { Invoke-AwsSmoke $network }
            }
            $dualHead = if ($name -eq "Rclone") { "NOT_RUN" } else { "PASSED" }
            Write-Result -Name $name -Status "PASSED" -Detail "all commands and assertions completed" -DualHead $dualHead
        } catch {
            $failed = $true
            Write-Result $name "FAILED" (Convert-ToPortableEvidence $_.Exception.Message)
            Write-Host "Manual cleanup endpoint: http://127.0.0.1:9000 bucket prefix ipfs-s3-$($name.ToLower())-$RunId"
        }
    }
    Write-Host "Compose services and volumes preserved."
    return $failed
}
```

No ordinary Compose build branch or volume-cleanup branch may remain.

---

### Task 4: Run all non-Docker GREEN gates

**Files:**
- Test: `tests/client-smoke.Tests.ps1`
- Verify: `scripts/client-smoke.ps1`
- Verify unchanged: `tests/e2e.rs`

**Interfaces:**
- Consumes: Tasks 1–3 complete source.
- Produces: executable process/filesystem GREEN, AST/source GREEN, exact dirty-tree guard, and whitespace-clean source/doc/test artifacts without Docker.

- [ ] **Step 1: Parse both PowerShell files**

```powershell
foreach ($path in @("scripts/client-smoke.ps1", "tests/client-smoke.Tests.ps1")) {
    $tokens = $null
    $errors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile(
        (Resolve-Path -LiteralPath $path),
        [ref]$tokens,
        [ref]$errors
    )
    if ($errors.Count -ne 0) { $errors | Format-List | Out-String | Write-Host; throw "Parse failed: $path" }
}
```

Expected: no parse errors.

- [ ] **Step 2: Run the executable PowerShell test**

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

Expected GREEN: exit 0 and final line `client-smoke infrastructure tests: PASSED`. The test itself proves no-Docker preflight by launching child runners with an empty `PATH`; it must not contact a daemon.

- [ ] **Step 3: Run independent source and invariant guards**

```powershell
$forbidden = @(rg -n '\$Stamp|StandardBuildImages|missingStandardBuild|useOfflineGatewayBuild|Start-Job|CleanupVolumes|docker pull|"down", "-v"|&\s+(docker|cargo|tar\.exe)' scripts/client-smoke.ps1)
if ($LASTEXITCODE -eq 0) { $forbidden; throw "Forbidden runner path remains" }
if ($LASTEXITCODE -ne 1) { throw "Forbidden-pattern guard failed with exit $LASTEXITCODE" }

$required = @(
    'ProcessStartInfo]::new',
    'ArgumentList.Add',
    'ReadToEndAsync',
    'WaitForExit',
    'Kill($true)',
    'archive context already exists',
    'vendor-archive=$archiveContext',
    '"--pull", "never", "--no-build"',
    'Remove-OwnedBuildArtifacts',
    'Stop-Transcript'
)
foreach ($fragment in $required) {
    $match = @(rg -n --fixed-strings -- $fragment scripts/client-smoke.ps1)
    if ($LASTEXITCODE -ne 0 -or $match.Count -eq 0) { throw "Required fragment missing: $fragment" }
}

if ((git hash-object tests/e2e.rs).Trim() -ne "6756f469a5d23f3f6ac5722e3112f548d401e302") {
    throw "tests/e2e.rs changed during runner remediation"
}
if ([int](rg -c '#\[tokio::test\]' tests/e2e.rs) -ne 11) { throw "Expected 11 unchanged e2e tests" }
```

Expected: no forbidden pattern; every required safety fragment exists; e2e hash/count remain frozen.

- [ ] **Step 4: Run whitespace and exact-status guards**

```powershell
git diff --check
foreach ($untracked in @(
    "docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md",
    "docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md",
    "tests/client-smoke.Tests.ps1"
)) {
    $output = @(& git diff --no-index --check -- NUL $untracked 2>&1)
    $exit = $LASTEXITCODE
    if ($exit -ne 1 -or $output.Count -ne 0) {
        $output | ForEach-Object { $_ }
        throw "No-index whitespace check failed for $untracked with exit $exit"
    }
}

$expectedStatus = @(
    " M scripts/client-smoke.ps1",
    " M tests/e2e.rs",
    "?? docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md",
    "?? docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md",
    "?? tests/client-smoke.Tests.ps1"
) | Sort-Object
$actualStatus = @(git status --short) | Sort-Object
$delta = @(Compare-Object -ReferenceObject $expectedStatus -DifferenceObject $actualStatus)
if ($delta.Count -ne 0) { $delta | Format-Table | Out-String | Write-Host; throw "Unexpected files after remediation" }
```

Expected: all checks pass. Do not stage or commit.

---

### Task 5: Regenerate fresh real Rclone and MinIO `mc` evidence after the pull lock

**Files:**
- Execute: `scripts/client-smoke.ps1`
- Read unchanged: `docker-compose.yml`
- Retain temporary evidence: `C:\Users\HUGEFI~1\AppData\Local\Temp\opencode\ipfs-s3-client-smoke-*\client-smoke.log`

**Interfaces:**
- Consumes: Task 4 GREEN including the exactly-six pull-lock and offline-build-timeout guards, existing local service/build/client images, current source tree, and no network pulls.
- Produces: two fresh post-fix 45-minute-bounded runner outputs, two archive-only builds, runtime proof that every emitted Rclone/`mc` Docker run is pull-locked, one Rclone PASS, one `mc` PASS plus dual-head evidence, and post-run owned-artifact cleanup proof.

- [ ] **Step 1: Define one bounded runner harness for both live invocations**

Run this once in the verification PowerShell session:

```powershell
$approvedTemp = "C:\Users\HUGEFI~1\AppData\Local\Temp\opencode"
if (-not (Test-Path -LiteralPath $approvedTemp -PathType Container)) { throw "Approved temp root is missing" }
$runnerPath = [IO.Path]::GetFullPath("scripts/client-smoke.ps1")
$tokens = $null
$errors = $null
$runnerAst = [System.Management.Automation.Language.Parser]::ParseFile($runnerPath, [ref]$tokens, [ref]$errors)
if ($errors.Count -ne 0) { throw "Runner parse failed before live verification" }
foreach ($name in @("Convert-NativeTextToLines", "Get-NativeDiagnostic", "Invoke-NativeCommand")) {
    $matches = @($runnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one $name function" }
    Invoke-Expression $matches[0].Extent.Text
}
$pwshPath = (Get-Command pwsh -ErrorAction Stop).Source
$oldTemp = $env:TEMP
$oldTmp = $env:TMP
$env:TEMP = $approvedTemp
$env:TMP = $approvedTemp
```

The harness imports only the reviewed native helper and places each complete runner process under a 45-minute outer timeout.

- [ ] **Step 2: Run and validate Rclone**

```powershell
try {
    $rclone = Invoke-NativeCommand `
        -FilePath $pwshPath `
        -ArgumentList @("-NoProfile", "-File", $runnerPath, "-Client", "Rclone", "-Run") `
        -Label "complete Rclone smoke invocation" `
        -Timeout ([TimeSpan]::FromMinutes(45))
    $rcloneLines = @($rclone.StdOut) + @($rclone.StdErr)
    $rcloneLines | ForEach-Object { $_ }

    $runIdPattern = '[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}'
    $archive = @($rcloneLines | Where-Object { $_ -match "^vendor archive completed: <temp>[\\/]ipfs-s3-client-smoke-($runIdPattern)[\\/]vendor-archive-context[\\/]vendor\.tar\.gz$" })
    if ($archive.Count -ne 1) { throw "Rclone did not prove one RunId-based archive" }
    $rcloneRunId = [regex]::Match($archive[0], $runIdPattern).Value
    $offlineBuild = @($rcloneLines | Where-Object { $_ -match '^docker build --pull=false --network none --quiet --build-context vendor-archive=' })
    $composeNoBuild = @($rcloneLines | Where-Object { $_ -match 'docker compose .* up -d --pull never --no-build kubo gateway$' })
    $result = @($rcloneLines | Where-Object { $_ -match '^\[RESULT\] client=Rclone ' })
    if ($offlineBuild.Count -ne 1 -or $composeNoBuild.Count -ne 1) { throw "Rclone did not prove one forced offline/no-build path" }
    if ($result.Count -ne 1 -or $result[0] -notmatch '^\[RESULT\] client=Rclone status=PASSED dual_head=NOT_RUN detail=all commands and assertions completed evidence=client-smoke\.log$') {
        throw "Unexpected Rclone result: $($result -join ' | ')"
    }
    $rcloneDockerRuns = @($rcloneLines | Where-Object { $_ -match '^docker run ' })
    if ($rcloneDockerRuns.Count -eq 0) { throw "Rclone emitted no docker run evidence" }
    if (@($rcloneDockerRuns | Where-Object { $_ -notmatch '^docker run --rm --pull=never(?:\s|$)' }).Count -ne 0) {
        throw "Rclone emitted a docker run without immediate --pull=never"
    }
    if (@($rcloneLines | Where-Object { $_ -match '--build-context vendor=|docker pull| down -v| up -d --build' }).Count -ne 0) {
        throw "Rclone output contains a forbidden build/pull/down path"
    }
    $rcloneRoot = Join-Path $approvedTemp "ipfs-s3-client-smoke-$rcloneRunId"
    foreach ($removed in @("vendor", "vendor-archive-context", "Dockerfile.gateway-runtime")) {
        if (Test-Path -LiteralPath (Join-Path $rcloneRoot $removed)) { throw "Rclone owned artifact survived: $removed" }
    }
    if (-not [IO.File]::Exists((Join-Path $rcloneRoot "client-smoke.log"))) { throw "Rclone evidence log was deleted" }
} finally {
    $env:TEMP = $oldTemp
    $env:TMP = $oldTmp
}
```

Expected: this mandatory post-fix run exits 0; one forced archive path, only `docker run --rm --pull=never` client commands, exact Rclone PASS, no forbidden path, and only large/generated build artifacts removed from its unique RunRoot.

- [ ] **Step 3: Restore the approved temp variables and run/validate `mc` independently**

```powershell
$oldTemp = $env:TEMP
$oldTmp = $env:TMP
$env:TEMP = $approvedTemp
$env:TMP = $approvedTemp
try {
    $mc = Invoke-NativeCommand `
        -FilePath $pwshPath `
        -ArgumentList @("-NoProfile", "-File", $runnerPath, "-Client", "Mc", "-Run") `
        -Label "complete mc smoke invocation" `
        -Timeout ([TimeSpan]::FromMinutes(45))
    $mcLines = @($mc.StdOut) + @($mc.StdErr)
    $mcLines | ForEach-Object { $_ }

    $runIdPattern = '[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}'
    $archive = @($mcLines | Where-Object { $_ -match "^vendor archive completed: <temp>[\\/]ipfs-s3-client-smoke-($runIdPattern)[\\/]vendor-archive-context[\\/]vendor\.tar\.gz$" })
    if ($archive.Count -ne 1) { throw "mc did not prove one RunId-based archive" }
    $mcRunId = [regex]::Match($archive[0], $runIdPattern).Value
    $offlineBuild = @($mcLines | Where-Object { $_ -match '^docker build --pull=false --network none --quiet --build-context vendor-archive=' })
    $composeNoBuild = @($mcLines | Where-Object { $_ -match 'docker compose .* up -d --pull never --no-build kubo gateway$' })
    $result = @($mcLines | Where-Object { $_ -match '^\[RESULT\] client=Mc ' })
    $evidence = @($mcLines | Where-Object { $_ -match '^\[EVIDENCE\] client=Mc verifier=Mc ' })
    if ($offlineBuild.Count -ne 1 -or $composeNoBuild.Count -ne 1) { throw "mc did not prove one forced offline/no-build path" }
    if ($result.Count -ne 1 -or $result[0] -notmatch '^\[RESULT\] client=Mc status=PASSED dual_head=PASSED detail=all commands and assertions completed evidence=client-smoke\.log$') {
        throw "Unexpected mc result: $($result -join ' | ')"
    }
    if ($evidence.Count -ne 1 -or $evidence[0] -notmatch '^\[EVIDENCE\] client=Mc verifier=Mc dual_head=PASSED key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=(Qm|baf)\S* content_length=\d+$') {
        throw "Unexpected mc evidence: $($evidence -join ' | ')"
    }
    $mcDockerRuns = @($mcLines | Where-Object { $_ -match '^docker run ' })
    if ($mcDockerRuns.Count -eq 0) { throw "mc emitted no docker run evidence" }
    if (@($mcDockerRuns | Where-Object { $_ -notmatch '^docker run --rm --pull=never(?:\s|$)' }).Count -ne 0) {
        throw "mc emitted a docker run without immediate --pull=never"
    }
    if (@($mcLines | Where-Object { $_ -match '--build-context vendor=|docker pull| down -v| up -d --build' }).Count -ne 0) {
        throw "mc output contains a forbidden build/pull/down path"
    }
    $mcRoot = Join-Path $approvedTemp "ipfs-s3-client-smoke-$mcRunId"
    foreach ($removed in @("vendor", "vendor-archive-context", "Dockerfile.gateway-runtime")) {
        if (Test-Path -LiteralPath (Join-Path $mcRoot $removed)) { throw "mc owned artifact survived: $removed" }
    }
    if (-not [IO.File]::Exists((Join-Path $mcRoot "client-smoke.log"))) { throw "mc evidence log was deleted" }
} finally {
    $env:TEMP = $oldTemp
    $env:TMP = $oldTmp
}
```

Expected: this mandatory post-fix run exits 0; one forced archive path, only `docker run --rm --pull=never` client commands, exact `mc` PASS and dual-head EVIDENCE, no forbidden path, and exact owned-artifact cleanup.

- [ ] **Step 4: Re-run the non-Docker test after live execution**

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ((git hash-object tests/e2e.rs).Trim() -ne "6756f469a5d23f3f6ac5722e3112f548d401e302") { throw "e2e file changed" }
```

Expected: PowerShell test remains GREEN and e2e stays byte-for-byte frozen. Do not rerun the already-completed 11/11 Docker e2e suite unless another change modifies `tests/e2e.rs` or gateway behavior.

---

### Task 6: Final quality gates and Oracle/reviewer package

**Files:**
- Read in full: `docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md`
- Read in full: `docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md`
- Review complete diff: `scripts/client-smoke.ps1`
- Read in full: `tests/client-smoke.Tests.ps1`
- Verify unchanged hash: `tests/e2e.rs`

**Interfaces:**
- Consumes: Task 4 static/executable evidence and Task 5 fresh live evidence.
- Produces: one complete, current-revision review package for orchestrator-owned Oracle and reviewer dispatch, exact final status, and receipt state `waiting for receipt` until those reviews complete.

- [ ] **Step 1: Run final no-placeholder, whitespace, and working-tree gates**

```powershell
$documents = @(
    "docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md",
    "docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md"
)
$redFlags = @(
    ("T" + "BD"),
    ("T" + "ODO"),
    ("FIX" + "ME"),
    ("<" + "paste"),
    ("fill" + " in"),
    ("similar" + " to above")
)
foreach ($redFlag in $redFlags) {
    $hits = @(rg -n --fixed-strings -- $redFlag @documents)
    if ($LASTEXITCODE -eq 0) { $hits; throw "Plan/spec red-flag text remains: $redFlag" }
    if ($LASTEXITCODE -ne 1) { throw "Red-flag scan failed with exit $LASTEXITCODE" }
}

git diff --check
foreach ($untracked in $documents + @("tests/client-smoke.Tests.ps1")) {
    $output = @(& git diff --no-index --check -- NUL $untracked 2>&1)
    $exit = $LASTEXITCODE
    if ($exit -ne 1 -or $output.Count -ne 0) { $output; throw "Whitespace check failed: $untracked" }
}
if ((git hash-object tests/e2e.rs).Trim() -ne "6756f469a5d23f3f6ac5722e3112f548d401e302") { throw "e2e file changed" }
$expectedStatus = @(
    " M scripts/client-smoke.ps1",
    " M tests/e2e.rs",
    "?? docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md",
    "?? docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md",
    "?? tests/client-smoke.Tests.ps1"
) | Sort-Object
$actualStatus = @(git status --short) | Sort-Object
$delta = @(Compare-Object -ReferenceObject $expectedStatus -DifferenceObject $actualStatus)
if ($delta.Count -ne 0) { $delta; throw "Final working tree differs from the approved remediation set" }
$actualStatus
git diff --stat
```

Expected: no placeholders or whitespace errors; e2e hash unchanged; status is exactly the two original modified files, two untracked documents, and new untracked PowerShell test.

- [ ] **Step 2: Assemble the exact final review package in the orchestrator response**

Read the two documents and PowerShell test in full. Include the complete `git diff -- scripts/client-smoke.ps1` and `git diff --no-index -- NUL tests/client-smoke.Tests.ps1`. The package must state these evidence rows, each backed by Task 4 or Task 5 output:

1. **Baseline preservation:** HEAD/branch, exact dirty-tree start, `tests/e2e.rs` hash `6756f469a5d23f3f6ac5722e3112f548d401e302`, 11 tests, prior 11/11 result, and no e2e edit during remediation.
2. **Run identity:** canonical RunId regex, 64 generated IDs unique, eight-way collision test exactly 1 create/7 reject, bucket grammar and length pass, no `$Stamp` or fixed `\d{14}` evidence matcher.
3. **Native safety:** dependency-ordered helper import makes the first literal-argument child probe GREEN; the five-second fake-tree timeout completes within 15 seconds including termination grace, reports its label, removes both recorded parent/child PIDs, and leaves the independently launched control sleeper alive until test-finally cleanup; no direct Docker/Cargo/tar AST commands remain and fixed budgets are 5m/5m/5m/30m.
4. **Forced offline:** source guards reject standard-build selection and prove `Invoke-OfflineGatewayBuild` passes `-Timeout $OfflineBuildTimeout`; both fresh runs show one `vendor-archive` build, `--network none`, `--pull=false`, Compose `--pull never --no-build`, and no pull/ordinary build.
5. **Cleanup:** safe-cleanup test removes only the three owned artifacts and rejects outside root; both real RunRoots retain `client-smoke.log` and omit vendor/archive context/generated Dockerfile after exit.
6. **Exit semantics:** executable classifier covers dry skip/zero, requested unavailable/nonzero, all missing/nonzero, partial available/runnable; empty-PATH child entry-point checks confirm dry zero and requested exit 1 with SKIPPED.
7. **Client pull lock:** source test finds exactly six `"run", "--rm", "--pull=never"` entries and zero unlocked `run --rm` entries; every emitted Rclone/`mc` Docker run in both mandatory fresh outputs begins `docker run --rm --pull=never`.
8. **Rclone:** fresh post-fix exact PASS with nested upload/list/download/delete and `dual_head=NOT_RUN`.
9. **MinIO `mc`:** fresh post-fix exact PASS plus one exact same-client dual-head EVIDENCE line with CID and content length.
10. **Preservation:** no down-v, service stop, volume removal, evidence-log deletion, pull, installation, tracked evidence, or Git write.
11. **Quality:** PowerShell parser/test GREEN before and after live runs, source guards GREEN, `git diff --check` GREEN, all no-index whitespace checks GREEN, exact final status.

- [ ] **Step 3: Hand the package to the orchestrator-owned final reviews**

The orchestrator sends the same complete package and current file contents to its final Oracle and reviewer. This planner does not dispatch those agents and does not claim a receipt. If either review requests an edit, make only the requested plan-authorized source/test change, then rerun Tasks 4–6 and obtain reviews for the new revision.

Expected final state for this plan artifact: `Receipt status: waiting for receipt`. A timeout, acknowledgement, partial review, or verdict for an older revision is not a receipt.

---

## Requirement-to-Task Coverage

| Requirement | Plan coverage |
|---|---|
| Frozen UTC-millisecond+PID+8-hex RunId, bucket-safe and <=63 | Task 2 Step 1; Task 1 executable identity tests; Task 5 regexes |
| Non-Force RunRoot creation and collision failure | Task 2 Step 1; Task 1 eight-way concurrent collision test |
| Evidence placeholder/regex no fixed 14-digit stamp | Tasks 1, 2, 4, and 5 |
| Existing archive context fails and is never pre-deleted | Task 2 Step 3; Task 1 source guard |
| Every `-Run` uses archive-only offline build + Compose no-build/no-pull | Task 3 Step 3; Tasks 1 and 4 guards; Task 5 real runs |
| Exactly six client `run --rm --pull=never` entries, no unlocked entry, and fresh runtime proof | Task 1 source test; Task 3 Step 1; Task 5 fresh Rclone/`mc`; Task 6 review row 7 |
| Remove standard-image conditional branch | Task 3 Steps 1 and 3; Tasks 1 and 4 forbidden guards |
| Dependency-ordered native-helper import; ProcessStartInfo.ArgumentList, async drain, bounded wait, Kill(true) only the target tree | Task 2 Step 2; Task 1 literal-argument, recorded parent/child, and independent-sleeper tests |
| Explicit 5m/5m/5m/30m timeouts for all native paths | Task 2 Steps 2–3; Tasks 1 and 4 guards |
| Offline build explicitly receives `$OfflineBuildTimeout` | Task 1 `Invoke-OfflineGatewayBuild` source assertion; Task 2 Step 3; Task 6 review row 4 |
| Exact containment-checked cleanup after Stop-Transcript | Task 2 Step 4; Task 1 filesystem tests; Task 5 real RunRoot checks |
| Preserve evidence, services, and volumes; no down-v | Global constraints; Tasks 1, 3, 5, and 6 |
| Requested unexecutable/all-missing nonzero; dry skip zero; partial All may succeed | Task 3 Steps 2–3; Task 1 classifier and empty-PATH entry-point tests |
| PowerShell executable tests: timeout/tree, collision, offline, cleanup, AST | Task 1; Task 4; Task 5 Step 4 |
| Fresh real Rclone and `mc` archive/RESULT/EVIDENCE proof | Task 5 |
| Completed e2e 11/11 remains unchanged | Baseline; Tasks 4–6 hash/count guards |
| Final Oracle+reviewer package and waiting receipt | Task 6 |
| No Git write | Global constraints and every checkpoint |
