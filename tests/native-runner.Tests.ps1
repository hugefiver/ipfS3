#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$runnerPath = Join-Path ([IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))) 'scripts/client-smoke.ps1'
$tokens = $null
$errors = $null
$runnerAst = [Management.Automation.Language.Parser]::ParseFile(
    $runnerPath,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count -ne 0) { throw 'Client smoke runner has syntax errors' }

function Get-RunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)

    $matches = @($runnerAst.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one runner function named $Name" }
    return $matches[0].Extent.Text
}

function Assert-True {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { throw $Message }
}

foreach ($name in @(
    'Convert-NativeTextToLines',
    'Get-NativeDiagnostic',
    'Invoke-NativeCommand',
    'Assert-CanonicalChildPath',
    'New-SmokeRunRoot',
    'Remove-OwnedBuildArtifacts'
)) {
    Invoke-Expression (Get-RunnerFunctionSource $name)
}

$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar
)
$testRoot = Join-Path $tempRoot ('ipfs-s3-native-runner-tests-' + [Guid]::NewGuid().ToString('N'))
if (-not [IO.Path]::GetDirectoryName($testRoot).Equals($tempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Native runner test root is not a direct child of the OS temporary directory'
}
$null = New-Item -ItemType Directory -Path $testRoot -ErrorAction Stop
$independentSleeper = $null

try {
    $pwshPath = (@(Get-Command pwsh -CommandType Application -ErrorAction Stop))[0].Source

    $echoScript = Join-Path $testRoot 'echo-argument.ps1'
    $echoOutput = Join-Path $testRoot 'echo-output.txt'
    $injectionSentinel = Join-Path $testRoot 'injection-sentinel.txt'
    [IO.File]::WriteAllText($echoScript, @'
param([string]$OutputPath, [string]$Value)
[IO.File]::WriteAllText($OutputPath, $Value, [Text.UTF8Encoding]::new($false))
'@, [Text.UTF8Encoding]::new($false))
    $literalArgument = "literal value with spaces; write forbidden > `"$injectionSentinel`" & stop | `$(`"injected`")"
    $argumentResult = Invoke-NativeCommand `
        -FilePath $pwshPath `
        -ArgumentList @('-NoProfile', '-File', $echoScript, $echoOutput, $literalArgument) `
        -Label 'literal argument probe' `
        -Timeout ([TimeSpan]::FromSeconds(10))
    Assert-True ($argumentResult.ExitCode -eq 0) 'Literal argument probe failed'
    Assert-True ([IO.File]::ReadAllText($echoOutput) -ceq $literalArgument) 'ArgumentList changed spaces, quotes, or shell metacharacters'
    Assert-True (-not [IO.File]::Exists($injectionSentinel)) 'Shell metacharacters executed an injection sentinel'

    $independentStartInfo = [Diagnostics.ProcessStartInfo]::new()
    $independentStartInfo.FileName = $pwshPath
    $independentStartInfo.UseShellExecute = $false
    $null = $independentStartInfo.ArgumentList.Add('-NoProfile')
    $null = $independentStartInfo.ArgumentList.Add('-Command')
    $null = $independentStartInfo.ArgumentList.Add('Start-Sleep -Seconds 60')
    $independentSleeper = [Diagnostics.Process]::Start($independentStartInfo)
    Assert-True ($null -ne $independentSleeper -and -not $independentSleeper.HasExited) 'Independent control sleeper did not start'

    $treeScript = Join-Path $testRoot 'native-tree.ps1'
    $parentPidPath = Join-Path $testRoot 'native-parent.pid'
    $childPidPath = Join-Path $testRoot 'native-child.pid'
    [IO.File]::WriteAllText($treeScript, @'
param([string]$ParentPidPath, [string]$ChildPidPath, [string]$PwshPath)
[IO.File]::WriteAllText($ParentPidPath, [string]$PID, [Text.UTF8Encoding]::new($false))
$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $PwshPath
$startInfo.UseShellExecute = $false
$null = $startInfo.ArgumentList.Add('-NoProfile')
$null = $startInfo.ArgumentList.Add('-Command')
$null = $startInfo.ArgumentList.Add('Start-Sleep -Seconds 60')
$child = [Diagnostics.Process]::Start($startInfo)
[IO.File]::WriteAllText($ChildPidPath, [string]$child.Id, [Text.UTF8Encoding]::new($false))
Start-Sleep -Seconds 60
'@, [Text.UTF8Encoding]::new($false))
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $timeoutMessage = $null
    try {
        Invoke-NativeCommand `
            -FilePath $pwshPath `
            -ArgumentList @('-NoProfile', '-File', $treeScript, $parentPidPath, $childPidPath, $pwshPath) `
            -Label 'fake native tree' `
            -Timeout ([TimeSpan]::FromSeconds(5)) | Out-Null
    } catch {
        $timeoutMessage = $_.Exception.Message
    } finally {
        $timer.Stop()
    }
    Assert-True ($null -ne $timeoutMessage -and $timeoutMessage.Contains('fake native tree') -and $timeoutMessage.Contains('timed out')) 'Native tree timeout was not reported'
    Assert-True ($timer.Elapsed -lt [TimeSpan]::FromSeconds(15)) 'Native tree timeout was not wall-clock bounded'
    Assert-True ([IO.File]::Exists($parentPidPath) -and [IO.File]::Exists($childPidPath)) 'Native tree did not publish both PIDs'
    foreach ($treePid in @(
        [int][IO.File]::ReadAllText($parentPidPath),
        [int][IO.File]::ReadAllText($childPidPath)
    )) {
        for ($attempt = 0; $attempt -lt 50 -and $null -ne (Get-Process -Id $treePid -ErrorAction SilentlyContinue); $attempt++) {
            Start-Sleep -Milliseconds 100
        }
        Assert-True ($null -eq (Get-Process -Id $treePid -ErrorAction SilentlyContinue)) "Timed-out tree PID survived Kill(true): $treePid"
    }
    $independentSleeper.Refresh()
    Assert-True (-not $independentSleeper.HasExited) 'Kill(true) terminated the independent control sleeper'

    $collisionParent = Join-Path $testRoot 'collision-parent'
    $null = New-Item -ItemType Directory -Path $collisionParent
    $collisionRunId = "20260720t120000000z-$PID-deadbeef"
    $rootFunctionSource = @(
        ($runnerAst.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Assert-CanonicalChildPath' }, $true))[0].Extent.Text
        ($runnerAst.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'New-SmokeRunRoot' }, $true))[0].Extent.Text
    ) -join [Environment]::NewLine
    $collisionResults = @(1..8 | ForEach-Object -Parallel {
        Invoke-Expression $using:rootFunctionSource
        try {
            $null = New-SmokeRunRoot -TempRoot $using:collisionParent -RunId $using:collisionRunId
            'CREATED'
        } catch {
            'COLLISION'
        }
    } -ThrottleLimit 8)
    Assert-True (@($collisionResults | Where-Object { $_ -ceq 'CREATED' }).Count -eq 1) 'Concurrent RunRoot creation did not have exactly one owner'
    Assert-True (@($collisionResults | Where-Object { $_ -ceq 'COLLISION' }).Count -eq 7) 'Concurrent RunRoot collision count was not seven'

    $cleanupParent = Join-Path $testRoot 'cleanup-parent'
    $null = New-Item -ItemType Directory -Path $cleanupParent
    $cleanupRunId = "20260720t120000001z-$PID-cafebabe"
    $cleanupRoot = New-SmokeRunRoot -TempRoot $cleanupParent -RunId $cleanupRunId
    $null = New-Item -ItemType Directory -Path (Join-Path $cleanupRoot 'vendor')
    $null = New-Item -ItemType Directory -Path (Join-Path $cleanupRoot 'vendor-archive-context')
    [IO.File]::WriteAllText((Join-Path $cleanupRoot 'vendor/payload'), 'owned')
    [IO.File]::WriteAllText((Join-Path $cleanupRoot 'vendor-archive-context/vendor.tar.gz'), 'owned')
    [IO.File]::WriteAllText((Join-Path $cleanupRoot 'Dockerfile.gateway-runtime'), 'owned')
    [IO.File]::WriteAllText((Join-Path $cleanupRoot 'client-smoke.log'), 'retain')
    [IO.File]::WriteAllText((Join-Path $cleanupRoot 'file.txt'), 'retain')
    $unrelated = Join-Path $cleanupRoot 'unrelated'
    $null = New-Item -ItemType Directory -Path $unrelated
    [IO.File]::WriteAllText((Join-Path $unrelated 'sentinel'), 'retain')
    Remove-OwnedBuildArtifacts -TempRoot $cleanupParent -RunRoot $cleanupRoot -RunId $cleanupRunId
    foreach ($removed in @('vendor', 'vendor-archive-context', 'Dockerfile.gateway-runtime')) {
        Assert-True (-not (Test-Path -LiteralPath (Join-Path $cleanupRoot $removed))) "Owned artifact survived cleanup: $removed"
    }
    foreach ($retained in @('client-smoke.log', 'file.txt', 'unrelated/sentinel')) {
        Assert-True (Test-Path -LiteralPath (Join-Path $cleanupRoot $retained)) "Cleanup deleted a non-owned artifact: $retained"
    }

    $outsideParent = Join-Path $testRoot 'outside-parent'
    $null = New-Item -ItemType Directory -Path $outsideParent
    $outsideRoot = Join-Path $outsideParent "ipfs-s3-client-smoke-$cleanupRunId"
    $outsideVendor = Join-Path $outsideRoot 'vendor'
    $null = New-Item -ItemType Directory -Path $outsideVendor
    [IO.File]::WriteAllText((Join-Path $outsideVendor 'sentinel'), 'retain')
    $outsideRejected = $false
    try {
        Remove-OwnedBuildArtifacts -TempRoot $cleanupParent -RunRoot $outsideRoot -RunId $cleanupRunId
    } catch {
        $outsideRejected = $true
    }
    Assert-True $outsideRejected 'Cleanup accepted a RunRoot outside TempRoot'
    Assert-True ([IO.File]::Exists((Join-Path $outsideVendor 'sentinel'))) 'Rejected cleanup deleted the outside sentinel'

    Write-Host 'native runner isolation behavior: 4 passed; 0 failed'
} finally {
    try {
        if ($null -ne $independentSleeper) {
            try {
                $independentSleeper.Refresh()
                if (-not $independentSleeper.HasExited) {
                    $independentSleeper.Kill($true)
                    if (-not $independentSleeper.WaitForExit(10000)) {
                        throw 'Independent control sleeper did not terminate during cleanup'
                    }
                }
            } finally {
                $independentSleeper.Dispose()
            }
        }
    } finally {
        if (Test-Path -LiteralPath $testRoot) {
            $canonicalTestRoot = [IO.Path]::GetFullPath($testRoot)
            if (-not [IO.Path]::GetDirectoryName($canonicalTestRoot).Equals($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
                -not [IO.Path]::GetFileName($canonicalTestRoot).StartsWith('ipfs-s3-native-runner-tests-', [StringComparison]::Ordinal)) {
                throw "Refusing to remove unexpected native runner test root: $canonicalTestRoot"
            }
            [IO.Directory]::Delete($canonicalTestRoot, $true)
        }
    }
}
