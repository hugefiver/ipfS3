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
        '"build", "--pull=false", "--network", "none", "--quiet"'
    )) {
        Assert-True ($offlineSource.Contains($fragment)) "Offline build fragment missing: $fragment"
    }
    Assert-True ($offlineSource.Contains('-Timeout $OfflineBuildTimeout')) "Offline build does not use the dedicated timeout"
    Assert-True (-not $offlineSource.Contains('Remove-Item -LiteralPath $archiveContext')) "Archive context is pre-deleted"

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
    $runEntrypoints = @([regex]::Matches($RunnerSource, '"run",\s*"--rm"'))
    $lockedRunEntrypoints = @([regex]::Matches($RunnerSource, '"run",\s*"--rm",\s*"--pull=never"'))
    $unlockedRunEntrypoints = @([regex]::Matches($RunnerSource, '"run",\s*"--rm"(?!\s*,\s*"--pull=never")'))
    Assert-True ($runEntrypoints.Count -eq 6) "Expected six Docker run entrypoints, found $($runEntrypoints.Count)"
    Assert-True ($lockedRunEntrypoints.Count -eq 6) "Expected six pull-locked Docker run entrypoints, found $($lockedRunEntrypoints.Count)"
    Assert-True ($unlockedRunEntrypoints.Count -eq 0) "Found unlocked Docker run entrypoints: $($unlockedRunEntrypoints.Value -join '; ')"

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
