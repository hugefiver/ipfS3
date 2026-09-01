[CmdletBinding()]
param(
    [switch]$Run,
    [switch]$PostgresOnly,
    [switch]$DiagnoseAws,
    [switch]$DiagnoseBrowser
)

$selectedModeCount = @(
    @($Run.IsPresent, $PostgresOnly.IsPresent, $DiagnoseAws.IsPresent, $DiagnoseBrowser.IsPresent) | Where-Object { $_ }
).Count
if ($selectedModeCount -gt 1) {
    throw "Bucket CORS runner modes are mutually exclusive"
}
if ($selectedModeCount -eq 0) {
    Write-Output "Bucket CORS validation: NOT RUN"
    return
}

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\tests\compose.cors-validation.yml"))
$GatewayRuntimeBaseImage = "ghcr.io/hugefiver/ipfs3:latest"
$KuboImage = "ghcr.io/hugefiver/ipfs3-kubo:latest"
$AwsImage = "amazon/aws-cli:latest"
$DockerCommandTimeout = [TimeSpan]::FromMinutes(5)
$ComposeStartupTimeout = [TimeSpan]::FromMinutes(6)
$OfflineBuildTimeout = [TimeSpan]::FromMinutes(30)
$RustTestTimeout = [TimeSpan]::FromMinutes(20)
$HttpTimeout = [TimeSpan]::FromSeconds(30)
$TouchedEnvironmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "IPFS_S3_CORS_POSTGRES_PORT",
    "IPFS_S3_CORS_KUBO_PORT",
    "IPFS_S3_CORS_GATEWAY_PORT",
    "IPFS_S3_CORS_IMAGE",
    "IPFS_S3_CORS_PROJECT_LABEL",
    "IPFS_S3_CORS_RUN_LABEL",
    "IPFS_S3_TEST_POSTGRES_URL"
)

function Write-CorsEvidence {
    param(
        [Parameter(Mandatory)][ValidateSet("stage", "command", "assertion", "cleanup", "result")][string]$Category,
        [Parameter(Mandatory)][string]$Value
    )
    if ($Value -notmatch '^[A-Za-z0-9._:=/ -]+$') {
        throw "Bucket CORS evidence value is not safely redacted"
    }
    $allowed = switch ($Category) {
        "stage" { $Value -match '^stage=(?:preflight|lib|cors|integration|static|images|offline-build|compose-up|health|postgres|aws|browser|cleanup)$' }
        "command" { $Value -in @(
            "cargo test --lib --locked --offline",
            "cargo test --test cors --locked --offline -- --test-threads=1",
            "cargo test --test integration --locked --offline -- --test-threads=1",
            "pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1",
            "cargo test --test postgres_cors --locked --offline -- --nocapture --test-threads=1"
        ) }
        "assertion" { $Value -match '^(?:(?:lib|cors|integration|postgres)=passed|static=passed|postgres17=passed|aws-image=cached|aws-management=passed|aws-substage=(?:files-written|bucket-created|initial-put|initial-get|initial-assert|replacement-put|replacement-get|replacement-assert|deleted|absent-verified|final-put|management-passed)-(?:start|pass)|browser-substage=(?:valid-preflight|wildcard-preflight|disallowed-preflight|partial-preflight|plain-options|signed-actual|signed-actual-error|custom-import-preflight|decompress-preflight|health-exclusion|ready-exclusion|parity)-(?:start|pass)|browser-(?:valid-preflight|wildcard-preflight|disallowed-preflight|partial-preflight|plain-options|signed-actual|signed-actual-error|custom-import-preflight|decompress-preflight|health-ready-exclusion|parity)=passed|browser-failure=(?:valid-preflight|wildcard-preflight|disallowed-preflight|partial-preflight|plain-options|signed-actual|signed-actual-error|custom-import-preflight|decompress-preflight|health-exclusion|ready-exclusion)-(?:transport|status|cors-presence|allow-origin|credentials|allow-method|allow-headers|max-age|expose-headers|vary))$' }
        "cleanup" { $Value -match '^(?:(?:project|image)-ownership-retry=(?:owned|absent|failed|not-needed)|compose-logs=captured|compose-down=passed|environment-restore=(?:passed|failed)|residual-(?:containers|networks|volumes|images|temp-root)-exit=[0-9]+-count=[0-9]+|cleanup-errors=[0-9]+)$' }
        "result" { $Value -match '^work=(?:passed|failed)$' }
    }
    if (-not $allowed) { throw "Bucket CORS evidence value is not an allowlisted fixed receipt" }
    return "[$Category] $Value"
}

function Write-CorsReceipts {
    param([Parameter(Mandatory)][hashtable]$State)
    foreach ($receipt in $State.Receipts) {
        if ($receipt -notmatch '^\[(?:stage|command|assertion|cleanup|result)\] [A-Za-z0-9._:=/ -]+$') {
            throw "Bucket CORS receipt does not have the fixed safe grammar"
        }
        Write-Output "Bucket CORS receipt: $receipt"
    }
}

function Set-CorsStage {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("preflight", "lib", "cors", "integration", "static", "images", "offline-build", "compose-up", "health", "postgres", "aws", "browser", "cleanup")][string]$Stage
    )
    $State.Stage = $Stage
    $State.Receipts.Add((Write-CorsEvidence -Category "stage" -Value "stage=$Stage"))
}

function New-CorsRunId {
    $bytes = [byte[]]::new(16)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    $runId = [Convert]::ToHexString($bytes).ToLowerInvariant()
    if ($runId -cnotmatch '^[0-9a-f]{32}$') {
        throw "Generated invalid Bucket CORS RunId"
    }
    return $runId
}

function Test-CorsLoopbackPortAvailable {
    param([Parameter(Mandatory)][int]$Port)
    if ($Port -lt 49152 -or $Port -gt 65535) { return $false }
    $listener = $null
    try {
        $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)
        $listener.Start()
        return $true
    } catch {
        return $false
    } finally {
        if ($null -ne $listener) { $listener.Stop() }
    }
}

function New-CorsLoopbackPorts {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9a-f]{32}$') { throw "Invalid Bucket CORS RunId for ports" }
    $bytes = [Convert]::FromHexString($RunId)
    $rangeStart = 49152
    $rangeEnd = 65535
    $rangeSize = $rangeEnd - $rangeStart + 1
    $ports = [Collections.Generic.List[int]]::new()
    for ($index = 0; $index -lt 3; $index++) {
        $candidate = $rangeStart + ([BitConverter]::ToUInt16($bytes, $index * 2) % $rangeSize)
        $attempt = 0
        while ($ports.Contains($candidate) -or -not (Test-CorsLoopbackPortAvailable -Port $candidate)) {
            $attempt++
            if ($attempt -ge $rangeSize) { throw "No bindable Bucket CORS loopback port is available" }
            $candidate = if ($candidate -eq $rangeEnd) { $rangeStart } else { $candidate + 1 }
        }
        $ports.Add($candidate)
    }
    $DistinctPorts = @($ports | Sort-Object -Unique)
    if ($DistinctPorts.Count -ne 3 -or @($DistinctPorts | Where-Object { $_ -lt $rangeStart -or $_ -gt $rangeEnd }).Count -ne 0) {
        throw "Generated Bucket CORS ports are not distinct high loopback ports"
    }
    return [pscustomobject]@{ Postgres = $ports[0]; Kubo = $ports[1]; Gateway = $ports[2] }
}

function New-CorsProjectName {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9a-f]{32}$') { throw "Invalid Bucket CORS RunId" }
    $project = "ipfs3-cors-$RunId"
    if ($project -cnotmatch '^[a-z0-9][a-z0-9_-]*$') { throw "Invalid Bucket CORS Compose project" }
    return $project
}

function New-CorsGatewayImage {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9a-f]{32}$') { throw "Invalid Bucket CORS RunId" }
    $image = "ipfs3-cors-gateway:$RunId"
    if ($image -cnotmatch '^[a-z0-9][a-z0-9._:-]*$') { throw "Invalid Bucket CORS gateway image" }
    return $image
}

function New-CorsBucketName {
    param([Parameter(Mandatory)][string]$RunId)
    if ($RunId -cnotmatch '^[0-9a-f]{32}$') { throw "Invalid Bucket CORS RunId" }
    $bucket = "ipfs3-cors-$RunId"
    if ($bucket.Length -gt 63 -or $bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$') {
        throw "Invalid Bucket CORS bucket name"
    }
    return $bucket
}

function Assert-CanonicalChildPath {
    param(
        [Parameter(Mandatory)][string]$ParentPath,
        [Parameter(Mandatory)][string]$ChildPath
    )
    $canonicalParent = [IO.Path]::GetFullPath($ParentPath).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    $canonicalChild = [IO.Path]::GetFullPath($ChildPath)
    $prefix = $canonicalParent + [IO.Path]::DirectorySeparatorChar
    if (-not $canonicalChild.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path is outside the owned Bucket CORS root"
    }
    return $canonicalChild
}

function Get-CanonicalExistingDirectory {
    param([Parameter(Mandatory)][string]$Path)
    $canonicalPath = [IO.Path]::GetFullPath($Path).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $canonicalPath -PathType Container)) {
        throw "Required directory is unavailable"
    }
    return $canonicalPath
}

function New-CorsRunRoot {
    param([Parameter(Mandatory)][hashtable]$State)
    if ($State.RunId -cnotmatch '^[0-9a-f]{32}$') { throw "Invalid Bucket CORS RunId for temporary root" }
    $canonicalTemp = Get-CanonicalExistingDirectory -Path $State.TempRoot
    $runRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath (Join-Path $canonicalTemp "ipfs-s3-bucket-cors-$($State.RunId)")
    if (-not [IO.Path]::GetDirectoryName($runRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "RunRoot must be a direct child of the temporary root"
    }
    $State.RunRootPreexisting = Test-Path -LiteralPath $runRoot
    if ($State.RunRootPreexisting) { throw "BLOCKED Bucket CORS RunRoot already exists" }
    $State.RunRootPreexisting = $false
    $created = New-Item -ItemType Directory -Path $runRoot -ErrorAction Stop
    $State.RunRoot = $runRoot
    $State.RunRootCreated = $true
    if (-not $created.FullName.Equals($runRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Bucket CORS RunRoot creation returned an unexpected location"
    }
    return $runRoot
}

function New-CorsOwnershipReceipt {
    param(
        [Parameter(Mandatory)][string]$RunRoot,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][string]$Project,
        [Parameter(Mandatory)][string]$GatewayImage
    )
    $receipt = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot "ownership-receipt")
    $content = "bucket-cors-owned:$RunId`nproject=$Project`nimage=$GatewayImage`nlabel=ipfs3.cors.run=$RunId`n"
    $stream = $null
    try {
        $stream = [IO.File]::Open($receipt, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes($content)
        $stream.Write($bytes, 0, $bytes.Length)
    } catch {
        throw "BLOCKED Bucket CORS ownership receipt could not be claimed"
    } finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }
    return [pscustomobject]@{ Path = $receipt; Content = $content }
}

function Save-EnvironmentState {
    param([Parameter(Mandatory)][string[]]$Names)
    $state = [Collections.Generic.List[object]]::new()
    foreach ($name in $Names) {
        $path = "Env:$name"
        $present = Test-Path -LiteralPath $path
        $state.Add([pscustomobject]@{
            Name = $name
            Present = $present
            Value = if ($present) { (Get-Item -LiteralPath $path).Value } else { $null }
        })
    }
    return @($state)
}

function Restore-EnvironmentState {
    param([Parameter(Mandatory)][object[]]$State)
    foreach ($entry in $State) {
        $name = [string]$entry.Name
        if ([bool]$entry.Present) {
            Set-Item -LiteralPath "Env:$name" -Value ([string]$entry.Value)
        } elseif (Test-Path -LiteralPath "Env:$name") {
            Remove-Item -LiteralPath "Env:$name" -Force
        }
    }
}

function Test-CorsEnvironmentStateRestored {
    param([Parameter(Mandatory)][object[]]$Snapshot)
    foreach ($entry in $Snapshot) {
        $name = [string]$entry.Name
        $path = "Env:$name"
        $present = Test-Path -LiteralPath $path
        if ([bool]$entry.Present) {
            if (-not $present -or (Get-Item -LiteralPath $path).Value -cne [string]$entry.Value) {
                return $false
            }
        } elseif ($present) {
            return $false
        }
    }
    return $true
}

function Set-CorsEnvironment {
    param([Parameter(Mandatory)][string]$Name, [Parameter(Mandatory)][string]$Value)
    Set-Item -LiteralPath "Env:$Name" -Value $Value
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][TimeSpan]$Timeout,
        [Parameter(Mandatory)][string]$Stage,
        [Parameter(Mandatory)][string]$RunRoot,
        [int[]]$AllowedExitCodes = @(0),
        [string]$WorkingDirectory
    )
    if ($Stage -notin @("preflight", "lib", "cors", "integration", "static", "images", "offline-build", "compose-up", "health", "postgres", "aws", "browser", "cleanup") -or
        $Timeout -le [TimeSpan]::Zero -or $Timeout.TotalMilliseconds -gt [int]::MaxValue -or $AllowedExitCodes.Count -eq 0) {
        throw "Bucket CORS native command has invalid bounds"
    }
    $rawOutputPath = Assert-CanonicalChildPath -ParentPath $RunRoot -ChildPath (Join-Path $RunRoot ("raw-output-{0}-{1}.txt" -f $Stage, [Guid]::NewGuid().ToString("N")))
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true
    if (-not [string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $startInfo.WorkingDirectory = Get-CanonicalExistingDirectory -Path $WorkingDirectory
    }
    foreach ($argument in $ArgumentList) { $null = $startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) { throw "Bucket CORS $Label could not start" }
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        $timeoutMilliseconds = [int][Math]::Ceiling($Timeout.TotalMilliseconds)
        if (-not $process.WaitForExit($timeoutMilliseconds)) {
            $process.Kill($true)
            if (-not $process.WaitForExit(10000)) { throw "Bucket CORS $Label timed out and did not terminate" }
            throw "Bucket CORS $Label timed out"
        }
        $stdoutText = $stdoutTask.GetAwaiter().GetResult()
        $stderrText = $stderrTask.GetAwaiter().GetResult()
        [IO.File]::WriteAllText($rawOutputPath, "STDOUT`n$stdoutText`nSTDERR`n$stderrText", [Text.UTF8Encoding]::new($false))
        $stdout = @($stdoutText -split "\r?\n" | Where-Object { $_.Length -gt 0 })
        $stderr = @($stderrText -split "\r?\n" | Where-Object { $_.Length -gt 0 })
        if ($process.ExitCode -notin $AllowedExitCodes) { throw "Bucket CORS $Label failed" }
        return [pscustomobject]@{ ExitCode = $process.ExitCode; StdOut = $stdout; StdErr = $stderr; RawOutputPath = $rawOutputPath }
    } finally {
        $process.Dispose()
    }
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Stage,
        [string]$Label = "Docker operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-NativeCommand -FilePath "docker" -ArgumentList $Arguments -Label $Label -Timeout $Timeout -Stage $Stage -RunRoot $State.RunRoot -AllowedExitCodes $AllowedExitCodes
}

function Invoke-Compose {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Stage,
        [string]$Label = "Compose operation",
        [TimeSpan]$Timeout = $DockerCommandTimeout,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker -State $State -Arguments (@("compose", "--project-name", $State.Project, "--file", $ComposeFile) + $Arguments) -Stage $Stage -Label $Label -Timeout $Timeout -AllowedExitCodes $AllowedExitCodes
}

function Assert-RequiredTools {
    foreach ($tool in @("pwsh", "cargo", "docker", "tar.exe")) {
        if ($null -eq (Get-Command $tool -ErrorAction SilentlyContinue)) {
            throw "Required local tool is unavailable"
        }
    }
}

function Assert-DockerDaemon {
    param([Parameter(Mandatory)][hashtable]$State)
    $result = Invoke-Docker -State $State -Arguments @("info", "--format", "{{.ServerVersion}}") -Stage "preflight" -Label "Docker daemon preflight"
    if ((@($result.StdOut | Where-Object { $_ -match '^[0-9]+\.' })).Count -ne 1) {
        throw "Docker daemon preflight returned an invalid version"
    }
}

function Assert-ComposeVersion {
    param([Parameter(Mandatory)][hashtable]$State)
    $result = Invoke-Docker -State $State -Arguments @("compose", "version", "--short") -Stage "preflight" -Label "Docker Compose version preflight"
    $versionText = (@($result.StdOut) -join "`n")
    $composeVersionMatch = [regex]::Match($versionText, '^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$')
    $composeVersion = $null
    if (-not $composeVersionMatch.Success -or
        -not [Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion) -or
        $composeVersion -lt [Version]"2.23.1") {
        throw "Docker Compose 2.23.1 or newer is required"
    }
}

function Assert-LoopbackPortsFree {
    param([Parameter(Mandatory)][int[]]$Ports)
    $listeners = [Collections.Generic.List[System.Net.Sockets.TcpListener]]::new()
    try {
        foreach ($port in $Ports) {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
            $listener.Start()
            $listeners.Add($listener)
        }
    } catch {
        throw "Required Bucket CORS loopback port is unavailable"
    } finally {
        foreach ($listener in $listeners) { $listener.Stop() }
    }
}

function Test-LocalImage {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Image, [Parameter(Mandatory)][string]$Stage)
    $result = Invoke-NativeCommand -FilePath "docker" -ArgumentList @("image", "inspect", $Image, "--format", "{{.Id}}") -Label "inspect local image" -Timeout $DockerCommandTimeout -Stage $Stage -RunRoot $State.RunRoot -AllowedExitCodes @(0, 1)
    if ($result.ExitCode -ne 0) { return [pscustomobject]@{ Exists = $false; ImageId = $null } }
    $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($ids.Count -ne 1 -or $ids[0] -notmatch '^sha256:[0-9a-f]{64}$') {
        throw "Local image inspection returned an invalid image identity"
    }
    return [pscustomobject]@{ Exists = $true; ImageId = $ids[0] }
}

function Assert-RequiredLocalImages {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][ValidateSet("full", "postgres", "aws", "browser")][string]$Mode)
    $images = if ($Mode -eq "postgres") {
        @("postgres:17")
    } else {
        @("postgres:17", $KuboImage, $GatewayRuntimeBaseImage, "rust:latest", $AwsImage)
    }
    $identities = @{}
    foreach ($image in $images) {
        $inspection = Test-LocalImage -State $State -Image $image -Stage "images"
        if (-not $inspection.Exists) { throw "Required local image is unavailable" }
        $identities[$image] = $inspection.ImageId
    }
    return $identities
}

function Assert-ProjectResourcesAbsent {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$Project,
        [Parameter(Mandatory)][string]$RunId,
        [Parameter(Mandatory)][ValidateSet("full", "postgres", "aws", "browser")][string]$Mode
    )
    $queries = @(
        [pscustomobject]@{ Name = "containers"; Arguments = @("ps", "-aq", "--filter", "label=com.docker.compose.project=$Project") },
        [pscustomobject]@{ Name = "networks"; Arguments = @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project") },
        [pscustomobject]@{ Name = "volumes"; Arguments = @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$Project") },
        [pscustomobject]@{ Name = "images"; Arguments = @("image", "ls", "-q", "--filter", "label=ipfs3.cors.run=$RunId") }
    )
    foreach ($query in $queries) {
        $result = Invoke-Docker -State $State -Arguments $query.Arguments -Stage "images" -Label "Bucket CORS project-label preflight"
        if (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 0) {
            throw "BLOCKED Bucket CORS project/image label preflight"
        }
    }
    if ($Mode -in @("full", "aws", "browser")) {
        $tag = Invoke-Docker -State $State -Arguments @("image", "inspect", $State.GatewayImage, "--format", "{{.Id}}") -Stage "images" -Label "Bucket CORS gateway tag preflight" -AllowedExitCodes @(0, 1)
        if ($tag.ExitCode -eq 0) { throw "BLOCKED Bucket CORS gateway tag already exists" }
        $State.GatewayImagePreflightAbsent = $true
    }
}

function Test-CorsProjectResourcesExist {
    param([Parameter(Mandatory)][hashtable]$State)
    foreach ($arguments in @(
        @("ps", "-aq", "--filter", "label=com.docker.compose.project=$($State.Project)"),
        @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)"),
        @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)")
    )) {
        $result = Invoke-Docker -State $State -Arguments $arguments -Stage "cleanup" -Label "Bucket CORS ownership probe"
        if (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) { return $true }
    }
    return $false
}

function Claim-CorsProjectOwnership {
    param([Parameter(Mandatory)][hashtable]$State)
    if (Test-CorsProjectResourcesExist -State $State) { $State.ProjectOwned = $true }
}

function Copy-CorsBuildInputs {
    param([Parameter(Mandatory)][string]$BuildContext)
    $allowlist = @("Cargo.toml", "Cargo.lock", "src", "tests")
    foreach ($relativePath in $allowlist) {
        $source = Join-Path $RepoRoot $relativePath
        if (-not (Test-Path -LiteralPath $source)) { throw "Bucket CORS offline build input is absent" }
        Copy-Item -LiteralPath $source -Destination (Join-Path $BuildContext $relativePath) -Recurse -Force
    }
}

function Invoke-OfflineGatewayBuild {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$RunId
    )
    $BuildContext = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "gateway-build-context")
    $vendorPath = Assert-CanonicalChildPath -ParentPath $BuildContext -ChildPath (Join-Path $BuildContext "vendor")
    $cargoConfigDirectory = Assert-CanonicalChildPath -ParentPath $BuildContext -ChildPath (Join-Path $BuildContext ".cargo")
    $cargoConfig = Assert-CanonicalChildPath -ParentPath $cargoConfigDirectory -ChildPath (Join-Path $cargoConfigDirectory "config.toml")
    $ValidationDockerfile = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "Dockerfile.cors-validation")
    if (Test-Path -LiteralPath $BuildContext) { throw "Bucket CORS offline build context already exists" }
    $null = New-Item -ItemType Directory -Path $BuildContext -ErrorAction Stop
    Copy-CorsBuildInputs -BuildContext $BuildContext
    Invoke-NativeCommand -FilePath "cargo" -ArgumentList @("vendor", "--locked", "--offline", $vendorPath) -Label "offline Bucket CORS Cargo vendoring" -Timeout $OfflineBuildTimeout -Stage "offline-build" -RunRoot $State.RunRoot -WorkingDirectory $RepoRoot | Out-Null
    $null = New-Item -ItemType Directory -Path $cargoConfigDirectory -ErrorAction Stop
    [IO.File]::WriteAllText($cargoConfig, @"
[net]
offline = true
[source.crates-io]
replace-with = "vendored-sources"
[source.vendored-sources]
directory = "/vendor"
"@, [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($ValidationDockerfile, @"
FROM rust:latest AS builder
WORKDIR /app
COPY vendor /vendor
COPY . .
RUN cargo build --release --locked --offline --bin ipfs-s3-gateway

FROM $GatewayRuntimeBaseImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@, [Text.UTF8Encoding]::new($false))
    Invoke-Docker -State $State -Arguments @(
        "build", "--pull=false", "--network", "none", "--quiet",
        "--label", "ipfs3.cors.run=$RunId", "--file", $ValidationDockerfile,
        "--tag", $State.GatewayImage, $BuildContext
    ) -Stage "offline-build" -Label "offline Bucket CORS gateway image build" -Timeout $OfflineBuildTimeout | Out-Null
    $image = Test-LocalImage -State $State -Image $State.GatewayImage -Stage "offline-build"
    if (-not $image.Exists) { throw "Bucket CORS unique gateway image was not built" }
    $State.GatewayImageId = $image.ImageId
    $State.GatewayImageOwned = $true
}

function Get-CorsServiceContainer {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("postgres", "kubo", "gateway")][string]$Service
    )
    $result = Invoke-Compose -State $State -Arguments @("ps", "-q", $Service) -Stage "health" -Label "Bucket CORS Compose service lookup"
    $ids = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($ids.Count -ne 1 -or $ids[0].Trim() -notmatch '^[0-9a-f]{12,64}$') {
        throw "Bucket CORS service lookup was not unique"
    }
    return $ids[0].Trim()
}

function Wait-CorsTopologyHealthy {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("postgres", "kubo", "gateway")][string[]]$Services
    )
    foreach ($service in $Services) {
        $container = Get-CorsServiceContainer -State $State -Service $service
        $healthy = $false
        for ($attempt = 0; $attempt -lt 36; $attempt++) {
            $result = Invoke-Docker -State $State -Arguments @("inspect", "--format", "{{.State.Health.Status}}", $container) -Stage "health" -Label "Bucket CORS health probe"
            $status = (@($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join "").Trim()
            if ($status -eq "healthy") { $healthy = $true; break }
            if ($status -eq "unhealthy") { throw "Bucket CORS topology healthcheck is unhealthy" }
            Start-Sleep -Seconds 5
        }
        if (-not $healthy) { throw "Bucket CORS topology did not become healthy" }
    }
}

function Assert-Postgres17Ready {
    param([Parameter(Mandatory)][hashtable]$State)
    $result = Invoke-Compose -State $State -Arguments @(
        "exec", "-T", "postgres", "psql", "-X", "-U", "ipfs3", "-d", "ipfs3",
        "-A", "-t", "-c", "SHOW server_version_num"
    ) -Stage "postgres" -Label "Bucket CORS PostgreSQL 17 probe"
    $versions = @($result.StdOut | Where-Object { $_ -match '^[0-9]+$' })
    if ($versions.Count -ne 1) { throw "Bucket CORS PostgreSQL version probe was invalid" }
    [int]$version = 0
    if (-not [int]::TryParse($versions[0], [ref]$version) -or $version -lt 170000 -or $version -ge 180000) {
        throw "Bucket CORS PostgreSQL 17 is required"
    }
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "postgres17=passed"))
}

function Assert-RustSuiteExecuted {
    param(
        [Parameter(Mandatory)][object]$Result,
        [Parameter(Mandatory)][string]$Name
    )
    $output = (@($Result.StdOut) + @($Result.StdErr)) -join "`n"
    $runningMatches = @([regex]::Matches($output, '(?m)^running (?<running>[1-9][0-9]*) tests?$'))
    $summaryMatches = @([regex]::Matches($output, '(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$'))
    if ($runningMatches.Count -ne 1 -or $summaryMatches.Count -ne 1) { throw "$Name did not return one complete Rust summary" }
    [int]$runningCount = 0
    [int]$passedCount = 0
    [decimal]$seconds = 0
    if (-not [int]::TryParse($runningMatches[0].Groups['running'].Value, [ref]$runningCount) -or
        -not [int]::TryParse($summaryMatches[0].Groups['passed'].Value, [ref]$passedCount) -or
        -not [decimal]::TryParse($summaryMatches[0].Groups['seconds'].Value, [Globalization.NumberStyles]::AllowDecimalPoint, [Globalization.CultureInfo]::InvariantCulture, [ref]$seconds) -or
        $runningCount -ne $passedCount -or $seconds -gt [decimal]3600) {
        throw "$Name did not prove a bounded positive Rust test count"
    }
}

function Invoke-CorsRustCommand {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$Stage,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$CommandReceipt,
        [Parameter(Mandatory)][string[]]$Arguments
    )
    $State.Receipts.Add((Write-CorsEvidence -Category "command" -Value $CommandReceipt))
    $result = Invoke-NativeCommand -FilePath "cargo" -ArgumentList $Arguments -Label $Name -Timeout $RustTestTimeout -Stage $Stage -RunRoot $State.RunRoot -WorkingDirectory $RepoRoot
    Assert-RustSuiteExecuted -Result $result -Name $Name
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "$Stage=passed"))
}

function Invoke-CorsRustSuites {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("full")][string]$Mode
    )
    Set-CorsStage -State $State -Stage "lib"
    Invoke-CorsRustCommand -State $State -Stage "lib" -Name "Bucket CORS library tests" -CommandReceipt "cargo test --lib --locked --offline" -Arguments @("test", "--lib", "--locked", "--offline")
    Set-CorsStage -State $State -Stage "cors"
    Invoke-CorsRustCommand -State $State -Stage "cors" -Name "Bucket CORS tests" -CommandReceipt "cargo test --test cors --locked --offline -- --test-threads=1" -Arguments @("test", "--test", "cors", "--locked", "--offline", "--", "--test-threads=1")
    Set-CorsStage -State $State -Stage "integration"
    Invoke-CorsRustCommand -State $State -Stage "integration" -Name "Bucket CORS integration tests" -CommandReceipt "cargo test --test integration --locked --offline -- --test-threads=1" -Arguments @("test", "--test", "integration", "--locked", "--offline", "--", "--test-threads=1")
    Set-CorsStage -State $State -Stage "static"
    $State.Receipts.Add((Write-CorsEvidence -Category "command" -Value "pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1"))
    $staticResult = Invoke-NativeCommand -FilePath "pwsh" -ArgumentList @("-NoLogo", "-NoProfile", "-File", "tests/client-smoke.Tests.ps1") -Label "Bucket CORS static contract" -Timeout $RustTestTimeout -Stage "static" -RunRoot $State.RunRoot -WorkingDirectory $RepoRoot
    if ($staticResult.ExitCode -ne 0) { throw "Bucket CORS static contract failed" }
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "static=passed"))
    # cargo test --test postgres_cors --locked --offline -- --nocapture --test-threads=1
}

function Invoke-CorsPostgresSuite {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-CorsEnvironment -Name "IPFS_S3_TEST_POSTGRES_URL" -Value "postgres://ipfs3:ipfs3@127.0.0.1:$($State.Ports.Postgres)/ipfs3"
    Invoke-CorsRustCommand -State $State -Stage "postgres" -Name "Bucket CORS PostgreSQL tests" -CommandReceipt "cargo test --test postgres_cors --locked --offline -- --nocapture --test-threads=1" -Arguments @("test", "--test", "postgres_cors", "--locked", "--offline", "--", "--nocapture", "--test-threads=1")
}

function Get-CorsComposeNetwork {
    param([Parameter(Mandatory)][hashtable]$State)
    $gateway = Get-CorsServiceContainer -State $State -Service "gateway"
    $result = Invoke-Docker -State $State -Arguments @("inspect", "--format", "{{json .NetworkSettings.Networks}}", $gateway) -Stage "health" -Label "Bucket CORS gateway network lookup"
    try { $networks = ((@($result.StdOut) -join "") | ConvertFrom-Json) } catch { throw "Bucket CORS gateway network lookup returned invalid JSON" }
    $names = @($networks.PSObject.Properties.Name)
    if ($names.Count -ne 1 -or $names[0] -notmatch '^[a-z0-9_-]+$') { throw "Bucket CORS gateway network lookup was not unique" }
    return $names[0]
}

function New-CorsAwsConfig {
    param([Parameter(Mandatory)][hashtable]$State)
    $path = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "aws-config")
    [IO.File]::WriteAllText($path, "[default]`nregion = us-east-1`ns3 =`n    addressing_style = path`n", [Text.UTF8Encoding]::new($false))
    return $path
}

function Invoke-CorsAws {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][string]$Network,
        [Parameter(Mandatory)][string[]]$Arguments,
        [int[]]$AllowedExitCodes = @(0)
    )
    return Invoke-Docker -State $State -Arguments (@(
        "run", "--rm", "--pull=never", "--network", $Network,
        "-e", "AWS_ACCESS_KEY_ID=test", "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "AWS_DEFAULT_REGION=us-east-1", "-e", "AWS_EC2_METADATA_DISABLED=true",
        "-e", "AWS_CONFIG_FILE=/work/aws-config",
        "--mount", "type=bind,src=$($State.RunRoot),dst=/work", $AwsImage,
        "--endpoint-url", "http://gateway:9000"
    ) + $Arguments) -Stage "aws" -Label "Bucket CORS AWS CLI management parity" -AllowedExitCodes $AllowedExitCodes -Timeout $RustTestTimeout
}

function Assert-CorsAwsAbsent {
    param([Parameter(Mandatory)][object]$Result)
    if ($Result.ExitCode -eq 0) { throw "Bucket CORS absent GET unexpectedly succeeded" }
    $output = (@($Result.StdOut) + @($Result.StdErr)) -join "`n"
    if ($output -notmatch 'NoSuchCORSConfiguration') { throw "Bucket CORS absent GET did not return its fixed code" }
}

function Assert-CorsManagementConfiguration {
    param(
        [Parameter(Mandatory)][object]$Result,
        [Parameter(Mandatory)][int]$ExpectedRuleCount,
        [Parameter(Mandatory)][bool]$RequireExposeHeader
    )
    $text = (@($Result.StdOut) -join "`n")
    try { $document = $text | ConvertFrom-Json -NoEnumerate } catch { throw "Bucket CORS management GET did not return JSON" }
    if ($document -isnot [pscustomobject] -or @($document.PSObject.Properties.Match("CORSRules")).Count -ne 1) {
        throw "Bucket CORS management GET did not return one CORSRules property"
    }
    $rules = @($document.CORSRules)
    if ($rules.Count -ne $ExpectedRuleCount) { throw "Bucket CORS management GET returned an unexpected rule count" }
    foreach ($rule in $rules) {
        if ($rule -isnot [pscustomobject] -or @($rule.PSObject.Properties.Match("AllowedMethods")).Count -ne 1 -or @($rule.AllowedMethods).Count -eq 0) {
            throw "Bucket CORS management GET returned an invalid rule shape"
        }
    }
    $first = $rules[0]
    $exposePresent = @($first.PSObject.Properties.Match("ExposeHeaders")).Count -eq 1
    if ($exposePresent -ne $RequireExposeHeader) { throw "Bucket CORS management GET expose-header shape changed" }
}

function Add-CorsAwsSubstageReceipt {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("files-written", "bucket-created", "initial-put", "initial-get", "initial-assert", "replacement-put", "replacement-get", "replacement-assert", "deleted", "absent-verified", "final-put", "management-passed")][string]$Name,
        [Parameter(Mandatory)][ValidateSet("start", "pass")][string]$Outcome
    )
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "aws-substage=$Name-$Outcome"))
}

function Add-CorsBrowserSubstageReceipt {
    param(
        [Parameter(Mandatory)][hashtable]$State,
        [Parameter(Mandatory)][ValidateSet("valid-preflight", "wildcard-preflight", "disallowed-preflight", "partial-preflight", "plain-options", "signed-actual", "signed-actual-error", "custom-import-preflight", "decompress-preflight", "health-exclusion", "ready-exclusion", "parity")][string]$Name,
        [Parameter(Mandatory)][ValidateSet("start", "pass")][string]$Outcome
    )
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "browser-substage=$Name-$Outcome"))
}

function Invoke-CorsAwsParity {
    param([Parameter(Mandatory)][hashtable]$State, [Parameter(Mandatory)][string]$Network)
    $awsImage = "amazon/aws-cli:latest"
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "aws-image=cached"))
    $configPath = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "cors-policy.json")
    $replacementPath = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "cors-policy-replacement.json")
    $initialPolicy = [ordered]@{
        CORSRules = @(
            [ordered]@{
                AllowedOrigins = @("https://allowed.example")
                AllowedMethods = @("GET", "POST", "PUT")
                AllowedHeaders = @("x-probe")
                ExposeHeaders = @("x-visible")
                MaxAgeSeconds = 60
            },
            [ordered]@{
                AllowedOrigins = @("*")
                AllowedMethods = @("GET")
                AllowedHeaders = @()
                ExposeHeaders = @()
                MaxAgeSeconds = 0
            }
        )
    }
    $replacementPolicy = [ordered]@{
        CORSRules = @(
            [ordered]@{
                AllowedOrigins = @("https://allowed.example")
                AllowedMethods = @("GET", "POST", "PUT")
                AllowedHeaders = @("x-probe")
                ExposeHeaders = @("x-visible")
                MaxAgeSeconds = 30
            },
            [ordered]@{
                AllowedOrigins = @("*")
                AllowedMethods = @("GET")
                AllowedHeaders = @()
                ExposeHeaders = @()
                MaxAgeSeconds = 0
            }
        )
    }
    Add-CorsAwsSubstageReceipt -State $State -Name "files-written" -Outcome "start"
    [IO.File]::WriteAllText($configPath, ($initialPolicy | ConvertTo-Json -Depth 6 -Compress), [Text.UTF8Encoding]::new($false))
    [IO.File]::WriteAllText($replacementPath, ($replacementPolicy | ConvertTo-Json -Depth 6 -Compress), [Text.UTF8Encoding]::new($false))
    Add-CorsAwsSubstageReceipt -State $State -Name "files-written" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "bucket-created" -Outcome "start"
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "create-bucket", "--bucket", $State.Bucket) | Out-Null
    Add-CorsAwsSubstageReceipt -State $State -Name "bucket-created" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-put" -Outcome "start"
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "put-bucket-cors", "--bucket", $State.Bucket, "--cors-configuration", "file:///work/cors-policy.json") | Out-Null
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-put" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-get" -Outcome "start"
    $initialGet = Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "get-bucket-cors", "--bucket", $State.Bucket, "--output", "json")
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-get" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-assert" -Outcome "start"
    Assert-CorsManagementConfiguration -Result $initialGet -ExpectedRuleCount 2 -RequireExposeHeader $true
    Add-CorsAwsSubstageReceipt -State $State -Name "initial-assert" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-put" -Outcome "start"
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "put-bucket-cors", "--bucket", $State.Bucket, "--cors-configuration", "file:///work/cors-policy-replacement.json") | Out-Null
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-put" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-get" -Outcome "start"
    $replacementGet = Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "get-bucket-cors", "--bucket", $State.Bucket, "--output", "json")
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-get" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-assert" -Outcome "start"
    Assert-CorsManagementConfiguration -Result $replacementGet -ExpectedRuleCount 2 -RequireExposeHeader $true
    Add-CorsAwsSubstageReceipt -State $State -Name "replacement-assert" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "deleted" -Outcome "start"
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "delete-bucket-cors", "--bucket", $State.Bucket) | Out-Null
    Add-CorsAwsSubstageReceipt -State $State -Name "deleted" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "absent-verified" -Outcome "start"
    $absent = Invoke-CorsAws -State $State -Network $Network -Arguments @("--cli-error-format", "json", "s3api", "get-bucket-cors", "--bucket", $State.Bucket) -AllowedExitCodes @(0, 1, 2, 252, 253, 254, 255)
    Assert-CorsAwsAbsent -Result $absent
    Add-CorsAwsSubstageReceipt -State $State -Name "absent-verified" -Outcome "pass"
    Add-CorsAwsSubstageReceipt -State $State -Name "final-put" -Outcome "start"
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "put-bucket-cors", "--bucket", $State.Bucket, "--cors-configuration", "file:///work/cors-policy-replacement.json") | Out-Null
    Add-CorsAwsSubstageReceipt -State $State -Name "final-put" -Outcome "pass"
    $objectPath = Assert-CanonicalChildPath -ParentPath $State.RunRoot -ChildPath (Join-Path $State.RunRoot "browser-object.txt")
    Add-CorsAwsSubstageReceipt -State $State -Name "management-passed" -Outcome "start"
    [IO.File]::WriteAllText($objectPath, "browser", [Text.UTF8Encoding]::new($false))
    Invoke-CorsAws -State $State -Network $Network -Arguments @("s3api", "put-object", "--bucket", $State.Bucket, "--key", "browser-object.txt", "--body", "/work/browser-object.txt") | Out-Null
    $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "aws-management=passed"))
    Add-CorsAwsSubstageReceipt -State $State -Name "management-passed" -Outcome "pass"
    if ($awsImage -cne $AwsImage) { throw "Bucket CORS AWS image identity changed" }
}

function Get-CorsHmacHex {
    param([Parameter(Mandatory)][byte[]]$Key, [Parameter(Mandatory)][string]$Text)
    $hmac = [Security.Cryptography.HMACSHA256]::new($Key)
    try { return ([Convert]::ToHexString($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($Text)))).ToLowerInvariant() } finally { $hmac.Dispose() }
}

function Get-CorsHmacBytes {
    param([Parameter(Mandatory)][byte[]]$Key, [Parameter(Mandatory)][string]$Text)
    $hmac = [Security.Cryptography.HMACSHA256]::new($Key)
    try { return $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($Text)) } finally { $hmac.Dispose() }
}

function Get-CorsSha256Hex {
    param([Parameter(Mandatory)][AllowEmptyString()][string]$Text)
    $hash = [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($Text))
    return ([Convert]::ToHexString($hash)).ToLowerInvariant()
}

function New-CorsSignedRequest {
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][uri]$Uri,
        [Parameter(Mandatory)][string]$Origin
    )
    $date = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ", [Globalization.CultureInfo]::InvariantCulture)
    $day = $date.Substring(0, 8)
    $payloadHash = Get-CorsSha256Hex -Text ""
    $canonicalHeaders = "host:$($Uri.Authority)`nx-amz-content-sha256:$payloadHash`nx-amz-date:$date`n"
    $canonicalRequest = "$Method`n$($Uri.AbsolutePath)`n$($Uri.Query.TrimStart('?'))`n$canonicalHeaders`nhost;x-amz-content-sha256;x-amz-date`n$payloadHash"
    $scope = "$day/us-east-1/s3/aws4_request"
    $stringToSign = "AWS4-HMAC-SHA256`n$date`n$scope`n$(Get-CorsSha256Hex -Text $canonicalRequest)"
    $dateKey = Get-CorsHmacBytes -Key ([Text.Encoding]::UTF8.GetBytes("AWS4test")) -Text $day
    $regionKey = Get-CorsHmacBytes -Key $dateKey -Text "us-east-1"
    $serviceKey = Get-CorsHmacBytes -Key $regionKey -Text "s3"
    $signingKey = Get-CorsHmacBytes -Key $serviceKey -Text "aws4_request"
    $signature = Get-CorsHmacHex -Key $signingKey -Text $stringToSign
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::new($Method), $Uri)
    $request.Headers.TryAddWithoutValidation("Origin", $Origin) | Out-Null
    $request.Headers.TryAddWithoutValidation("x-amz-content-sha256", $payloadHash) | Out-Null
    $request.Headers.TryAddWithoutValidation("x-amz-date", $date) | Out-Null
    $request.Headers.TryAddWithoutValidation("Authorization", "AWS4-HMAC-SHA256 Credential=test/$scope, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=$signature") | Out-Null
    return $request
}

function Invoke-CorsHttp {
    param(
        [Parameter(Mandatory)][Net.Http.HttpClient]$Client,
        [Parameter(Mandatory)][Net.Http.HttpRequestMessage]$Request,
        [Parameter(Mandatory)][string]$Scenario,
        [Parameter(Mandatory)][int[]]$ExpectedStatus,
        [Parameter(Mandatory)][bool]$ExpectCors
    )
    $response = $null
    $completed = $false
    try {
        $response = $Client.Send($Request)
        if ([int]$response.StatusCode -notin $ExpectedStatus) { throw "Bucket CORS browser scenario returned an unexpected status" }
        $corsHeaders = @($response.Headers | Where-Object { $_.Key -like 'Access-Control-*' })
        if (($corsHeaders.Count -gt 0) -ne $ExpectCors) { throw "Bucket CORS browser scenario returned an unexpected CORS header set" }
        $completed = $true
        return $response
    } finally {
        $Request.Dispose()
        if (-not $completed -and $null -ne $response) {
            $response.Dispose()
        }
    }
}

function Get-CorsHttpHeaderValues {
    param([Parameter(Mandatory)][Net.Http.HttpResponseMessage]$Response, [Parameter(Mandatory)][string]$Name)
    if (-not $Response.Headers.Contains($Name)) { return @() }
    return @($Response.Headers.GetValues($Name))
}

function Assert-CorsHttpHeader {
    param(
        [Parameter(Mandatory)][Net.Http.HttpResponseMessage]$Response,
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string]$ExpectedValue
    )
    $values = @(Get-CorsHttpHeaderValues -Response $Response -Name $Name)
    if ($values.Count -ne 1 -or $values[0] -cne $ExpectedValue) {
        throw "Bucket CORS browser header assertion failed"
    }
}

function Assert-CorsVaryTokens {
    param([Parameter(Mandatory)][Net.Http.HttpResponseMessage]$Response)
    $joined = (@(Get-CorsHttpHeaderValues -Response $Response -Name "Vary") -join ",")
    foreach ($token in @("Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers")) {
        if ($joined -notmatch "(?:^|,)\s*$([regex]::Escape($token))(?:\s*,|$)") {
            throw "Bucket CORS browser Vary assertion failed"
        }
    }
}

function Invoke-CorsBrowserParity {
    param([Parameter(Mandatory)][hashtable]$State)
    $client = [Net.Http.HttpClient]::new()
    $client.Timeout = $HttpTimeout
    $endpoint = "http://127.0.0.1:$($State.Ports.Gateway)"
    $currentScenario = $null
    $currentExpectedAssertionCategory = "transport"
    $response = $null
    try {
        $currentScenario = "valid-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "valid-preflight" -Outcome "start"
        $valid = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt")
        $valid.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $valid.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET") | Out-Null
        $valid.Headers.TryAddWithoutValidation("Access-Control-Request-Headers", "X-Probe") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $valid -Scenario "valid-preflight" -ExpectedStatus @(200) -ExpectCors $true
        $currentExpectedAssertionCategory = "allow-origin"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Origin" -ExpectedValue "https://allowed.example"
        $currentExpectedAssertionCategory = "credentials"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Credentials" -ExpectedValue "true"
        $currentExpectedAssertionCategory = "allow-method"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Methods" -ExpectedValue "GET"
        $currentExpectedAssertionCategory = "allow-headers"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Headers" -ExpectedValue "X-Probe"
        $currentExpectedAssertionCategory = "max-age"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Max-Age" -ExpectedValue "30"
        $currentExpectedAssertionCategory = "vary"
        Assert-CorsVaryTokens -Response $response
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "valid-preflight" -Outcome "pass"

        $currentScenario = "wildcard-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "wildcard-preflight" -Outcome "start"
        $wildcard = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt")
        $wildcard.Headers.TryAddWithoutValidation("Origin", "https://wildcard.example") | Out-Null
        $wildcard.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $wildcard -Scenario "wildcard-preflight" -ExpectedStatus @(200) -ExpectCors $true
        $currentExpectedAssertionCategory = "allow-origin"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Origin" -ExpectedValue "*"
        $currentExpectedAssertionCategory = "credentials"
        if (@(Get-CorsHttpHeaderValues -Response $response -Name "Access-Control-Allow-Credentials").Count -ne 0) {
            throw "Bucket CORS wildcard rule must not emit credentials"
        }
        $currentExpectedAssertionCategory = "vary"
        Assert-CorsVaryTokens -Response $response
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "wildcard-preflight" -Outcome "pass"

        $currentScenario = "disallowed-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "disallowed-preflight" -Outcome "start"
        $disallowed = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt")
        $disallowed.Headers.TryAddWithoutValidation("Origin", "https://disallowed.example") | Out-Null
        $disallowed.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "DELETE") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $disallowed -Scenario "disallowed-preflight" -ExpectedStatus @(403) -ExpectCors $false
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "disallowed-preflight" -Outcome "pass"

        $currentScenario = "partial-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "partial-preflight" -Outcome "start"
        $partial = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt")
        $partial.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $partial -Scenario "partial-preflight" -ExpectedStatus @(403) -ExpectCors $false
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "partial-preflight" -Outcome "pass"

        $currentScenario = "plain-options"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "plain-options" -Outcome "start"
        $plain = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt")
        $response = Invoke-CorsHttp -Client $client -Request $plain -Scenario "plain-options" -ExpectedStatus @(400, 403, 404, 405, 501) -ExpectCors $false
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "plain-options" -Outcome "pass"

        $currentScenario = "signed-actual"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "signed-actual" -Outcome "start"
        $signedSuccess = New-CorsSignedRequest -Method "GET" -Uri ([uri]"$endpoint/$($State.Bucket)/browser-object.txt") -Origin "https://allowed.example"
        $response = Invoke-CorsHttp -Client $client -Request $signedSuccess -Scenario "signed-actual" -ExpectedStatus @(200) -ExpectCors $true
        $currentExpectedAssertionCategory = "allow-origin"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Origin" -ExpectedValue "https://allowed.example"
        $currentExpectedAssertionCategory = "credentials"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Credentials" -ExpectedValue "true"
        $currentExpectedAssertionCategory = "expose-headers"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Expose-Headers" -ExpectedValue "x-visible"
        $currentExpectedAssertionCategory = "vary"
        Assert-CorsVaryTokens -Response $response
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "signed-actual" -Outcome "pass"

        $currentScenario = "signed-actual-error"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "signed-actual-error" -Outcome "start"
        $signedError = New-CorsSignedRequest -Method "GET" -Uri ([uri]"$endpoint/$($State.Bucket)/missing-object.txt") -Origin "https://allowed.example"
        $response = Invoke-CorsHttp -Client $client -Request $signedError -Scenario "signed-actual-error" -ExpectedStatus @(404) -ExpectCors $true
        $currentExpectedAssertionCategory = "allow-origin"
        Assert-CorsHttpHeader -Response $response -Name "Access-Control-Allow-Origin" -ExpectedValue "https://allowed.example"
        $currentExpectedAssertionCategory = "vary"
        Assert-CorsVaryTokens -Response $response
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "signed-actual-error" -Outcome "pass"

        $currentScenario = "custom-import-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "custom-import-preflight" -Outcome "start"
        $customImport = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt?ipfs3-import")
        $customImport.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $customImport.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "POST") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $customImport -Scenario "custom-import-preflight" -ExpectedStatus @(200) -ExpectCors $true
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "custom-import-preflight" -Outcome "pass"

        $currentScenario = "decompress-preflight"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "decompress-preflight" -Outcome "start"
        $decompress = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/$($State.Bucket)/browser-object.txt?decompress-zip")
        $decompress.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $decompress.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "PUT") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $decompress -Scenario "decompress-preflight" -ExpectedStatus @(200) -ExpectCors $true
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "decompress-preflight" -Outcome "pass"

        $currentScenario = "health-exclusion"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "health-exclusion" -Outcome "start"
        $health = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/health")
        $health.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $health.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $health -Scenario "health-ready-exclusion" -ExpectedStatus @(200, 403, 405) -ExpectCors $false
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "health-exclusion" -Outcome "pass"

        $currentScenario = "ready-exclusion"
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "ready-exclusion" -Outcome "start"
        $ready = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Options, "$endpoint/ready")
        $ready.Headers.TryAddWithoutValidation("Origin", "https://allowed.example") | Out-Null
        $ready.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET") | Out-Null
        $response = Invoke-CorsHttp -Client $client -Request $ready -Scenario "health-ready-exclusion" -ExpectedStatus @(200, 403, 405) -ExpectCors $false
        $response.Dispose()
        $response = $null
        Add-CorsBrowserSubstageReceipt -State $State -Name "ready-exclusion" -Outcome "pass"

        $currentScenario = $null
        $currentExpectedAssertionCategory = "transport"
        Add-CorsBrowserSubstageReceipt -State $State -Name "parity" -Outcome "start"
        foreach ($scenario in @("valid-preflight", "wildcard-preflight", "disallowed-preflight", "partial-preflight", "plain-options", "signed-actual", "signed-actual-error", "custom-import-preflight", "decompress-preflight", "health-ready-exclusion")) {
            $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "browser-$scenario=passed"))
        }
        $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "browser-parity=passed"))
        Add-CorsBrowserSubstageReceipt -State $State -Name "parity" -Outcome "pass"
    } catch {
        if ($null -ne $currentScenario) {
            $failureCategory = switch ($_.Exception.Message) {
                "Bucket CORS browser scenario returned an unexpected status" { "status" }
                "Bucket CORS browser scenario returned an unexpected CORS header set" { "cors-presence" }
                default { $currentExpectedAssertionCategory }
            }
            $State.Receipts.Add((Write-CorsEvidence -Category "assertion" -Value "browser-failure=$currentScenario-$failureCategory"))
        }
        if ($null -ne $response) {
            $response.Dispose()
        }
        throw "Bucket CORS browser parity failed"
    } finally {
        $client.Dispose()
    }
}

function Remove-OwnedCorsRunRoot {
    param([Parameter(Mandatory)][hashtable]$State)
    if (-not $State.RunRootCreated -or $State.RunRootPreexisting) {
        throw "Bucket CORS root cleanup rejected an unowned root"
    }
    $canonicalTemp = Get-CanonicalExistingDirectory -Path $State.TempRoot
    $canonicalRoot = Assert-CanonicalChildPath -ParentPath $canonicalTemp -ChildPath $State.RunRoot
    $expectedRoot = [IO.Path]::GetFullPath((Join-Path $canonicalTemp "ipfs-s3-bucket-cors-$($State.RunId)"))
    if (-not $canonicalRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase) -or -not [IO.Path]::GetDirectoryName($canonicalRoot).Equals($canonicalTemp, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Bucket CORS root cleanup rejected a noncanonical direct child"
    }
    if ($State.ReceiptOwned) {
        if ($null -eq $State.Receipt -or -not (Test-Path -LiteralPath $State.Receipt.Path -PathType Leaf) -or [IO.File]::ReadAllText($State.Receipt.Path) -cne $State.Receipt.Content) {
            throw "Bucket CORS root cleanup rejected an absent or foreign receipt"
        }
    } elseif (-not $State.RunRootCreated -or $State.RunRootPreexisting) {
        throw "Bucket CORS root cleanup rejected an unowned partial root"
    }
    Remove-Item -LiteralPath $canonicalRoot -Recurse -Force
}

function Test-CorsCleanupResiduals {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    $queries = @(
        [pscustomobject]@{ Name = "containers"; Arguments = @("ps", "-aq", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "networks"; Arguments = @("network", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "volumes"; Arguments = @("volume", "ls", "-q", "--filter", "label=com.docker.compose.project=$($State.Project)") },
        [pscustomobject]@{ Name = "images"; Arguments = @("image", "ls", "-q", "--filter", "label=ipfs3.cors.run=$($State.RunId)") }
    )
    foreach ($query in $queries) {
        try {
            $result = Invoke-Docker -State $State -Arguments $query.Arguments -Stage "cleanup" -Label "Bucket CORS cleanup residual query"
            $Count = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count
            $receipt = [pscustomobject]@{ Name = $query.Name; ExitCode = $result.ExitCode; Count = $Count }
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "residual-$($receipt.Name)-exit=$($receipt.ExitCode)-count=$($receipt.Count)"))
            if ($receipt.ExitCode -ne 0 -or $receipt.Count -ne 0) { $errors.Add("residual-$($query.Name)") }
        } catch {
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "residual-$($query.Name)-exit=1-count=0"))
            $errors.Add("residual-$($query.Name)")
        }
    }
    return @($errors)
}

function Remove-OwnedCorsResources {
    param([Parameter(Mandatory)][hashtable]$State)
    $errors = [Collections.Generic.List[string]]::new()
    if ($State.ProjectProbeUncertain) {
        try {
            Claim-CorsProjectOwnership -State $State
            if ($State.ProjectOwned) {
                $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "project-ownership-retry=owned"))
            } else {
                $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "project-ownership-retry=absent"))
            }
        } catch {
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "project-ownership-retry=failed"))
            $errors.Add("ownership-retry")
        }
    } else {
        $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "project-ownership-retry=not-needed"))
    }
    if ($State.ProjectOwned) {
        try {
            Invoke-Compose -State $State -Arguments @("logs", "--no-color") -Stage "cleanup" -Label "Bucket CORS logs-first cleanup" -AllowedExitCodes @(0, 1) | Out-Null
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "compose-logs=captured"))
        } catch { $errors.Add("compose-logs") }
        try {
            Invoke-Compose -State $State -Arguments @("down", "--volumes", "--remove-orphans") -Stage "cleanup" -Label "Bucket CORS Compose teardown" | Out-Null
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "compose-down=passed"))
        } catch { $errors.Add("compose-down") }
    }

    if ($State.Mode -in @("full", "aws", "browser") -and $State.GatewayImagePreflightAbsent -and -not $State.GatewayImageOwned) {
        try {
            $retry = Invoke-Docker -State $State -Arguments @("image", "inspect", $State.GatewayImage, "--format", '{{.Id}} {{ index .Config.Labels "ipfs3.cors.run"}}') -Stage "cleanup" -Label "Bucket CORS gateway image ownership retry" -AllowedExitCodes @(0, 1)
            if ($retry.ExitCode -eq 0) {
                $line = (@($retry.StdOut) -join "").Trim()
                if ($line -notmatch '^(?<id>sha256:[0-9a-f]{64}) (?<label>[0-9a-f]{32})$' -or $Matches['label'] -cne $State.RunId) {
                    throw "Bucket CORS gateway image retry ownership did not match"
                }
                $State.GatewayImageId = $Matches['id']
                $State.GatewayImageOwned = $true
                $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "image-ownership-retry=owned"))
            } else {
                $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "image-ownership-retry=absent"))
            }
        } catch {
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "image-ownership-retry=failed"))
            $errors.Add("image-ownership-retry")
        }
    } else {
        $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "image-ownership-retry=not-needed"))
    }
    if ($State.Mode -in @("full", "aws", "browser") -and $State.GatewayImageOwned) {
        try {
            $inspection = Invoke-Docker -State $State -Arguments @("image", "inspect", $State.GatewayImage, "--format", '{{.Id}} {{ index .Config.Labels "ipfs3.cors.run"}}') -Stage "cleanup" -Label "Bucket CORS owned gateway image check"
            $line = (@($inspection.StdOut) -join "").Trim()
            if ($line -ne "$($State.GatewayImageId) $($State.RunId)") { throw "Bucket CORS gateway image ownership did not match" }
            Invoke-Docker -State $State -Arguments @("image", "rm", $State.GatewayImage) -Stage "cleanup" -Label "Bucket CORS owned gateway image removal" | Out-Null
        } catch { $errors.Add("gateway-image") }
    }
    if ($State.EnvironmentOwned) {
        try {
            Restore-EnvironmentState -State $State.EnvironmentState
            if (-not (Test-CorsEnvironmentStateRestored -Snapshot $State.EnvironmentState)) {
                throw "Environment state restoration verification failed"
            }
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "environment-restore=passed"))
        } catch {
            $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "environment-restore=failed"))
            $errors.Add("environment")
        }
    }
    foreach ($residual in (Test-CorsCleanupResiduals -State $State)) { $errors.Add($residual) }
    if ($State.RunRootCreated -and -not $State.RunRootPreexisting) {
        try { Remove-OwnedCorsRunRoot -State $State } catch { $errors.Add("run-root") }
    }
    if ($State.RunRootCreated -and -not $State.RunRootPreexisting) {
        $tempReceipt = [pscustomobject]@{ Name = "temp-root"; ExitCode = 0; Count = [int](Test-Path -LiteralPath $State.RunRoot) }
        $State.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "residual-$($tempReceipt.Name)-exit=$($tempReceipt.ExitCode)-count=$($tempReceipt.Count)"))
        if ($tempReceipt.ExitCode -ne 0 -or $tempReceipt.Count -ne 0) { $errors.Add("residual-temp-root") }
    }
    return @($errors)
}

function Invoke-CorsPostgresOnly {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-CorsStage -State $State -Stage "images"
    $null = Assert-RequiredLocalImages -State $State -Mode "postgres"
    Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode "postgres"
    Assert-LoopbackPortsFree -Ports @($State.Ports.Postgres)
    Set-CorsStage -State $State -Stage "compose-up"
    try {
        Invoke-Compose -State $State -Arguments @("up", "--detach", "--pull", "never", "--no-build", "postgres") -Stage "compose-up" -Label "Bucket CORS PostgreSQL-only startup" -Timeout $ComposeStartupTimeout | Out-Null
    } finally {
        try { Claim-CorsProjectOwnership -State $State } catch { $State.ProjectProbeUncertain = $true }
    }
    if (-not $State.ProjectOwned) { throw "Bucket CORS PostgreSQL-only project ownership was not claimed" }
    Set-CorsStage -State $State -Stage "health"
    Wait-CorsTopologyHealthy -State $State -Services @("postgres")
    Set-CorsStage -State $State -Stage "postgres"
    Assert-Postgres17Ready -State $State
    Invoke-CorsPostgresSuite -State $State
}

function Invoke-CorsAwsDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)
    $network = Get-CorsComposeNetwork -State $State
    Set-CorsStage -State $State -Stage "aws"
    $null = New-CorsAwsConfig -State $State
    Invoke-CorsAwsParity -State $State -Network $network
}

function Invoke-CorsBrowserDiagnostic {
    param([Parameter(Mandatory)][hashtable]$State)
    $network = Get-CorsComposeNetwork -State $State
    Set-CorsStage -State $State -Stage "aws"
    $null = New-CorsAwsConfig -State $State
    Invoke-CorsAwsParity -State $State -Network $network
    Set-CorsStage -State $State -Stage "browser"
    Invoke-CorsBrowserParity -State $State
}

function Invoke-CorsMain {
    param([Parameter(Mandatory)][hashtable]$State)
    Set-CorsStage -State $State -Stage "preflight"
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) { throw "Bucket CORS validation Compose file is unavailable" }
    Assert-RequiredTools
    Assert-DockerDaemon -State $State
    Assert-ComposeVersion -State $State
    if ($State.Mode -eq "postgres") {
        Invoke-CorsPostgresOnly -State $State
        return
    }
    if ($State.Mode -eq "full") {
        Invoke-CorsRustSuites -State $state -Mode "full"
    } elseif ($State.Mode -notin @("aws", "browser")) {
        throw "Bucket CORS mode is invalid"
    }
    Set-CorsStage -State $State -Stage "images"
    if ($State.Mode -in @("aws", "browser")) {
        $null = Assert-RequiredLocalImages -State $state -Mode $State.Mode
        Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode $State.Mode
    } else {
        $null = Assert-RequiredLocalImages -State $state -Mode "full"
        Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode "full"
    }
    Assert-LoopbackPortsFree -Ports @($State.Ports.Postgres, $State.Ports.Kubo, $State.Ports.Gateway)
    Set-CorsStage -State $State -Stage "offline-build"
    Invoke-OfflineGatewayBuild -State $State -RunId $State.RunId
    Set-CorsStage -State $State -Stage "compose-up"
    try {
        Invoke-Compose -State $State -Arguments @("up", "--detach", "--pull", "never", "--no-build") -Stage "compose-up" -Label "Bucket CORS full topology startup" -Timeout $ComposeStartupTimeout | Out-Null
    } finally {
        try { Claim-CorsProjectOwnership -State $State } catch { $State.ProjectProbeUncertain = $true }
    }
    if (-not $State.ProjectOwned) { throw "Bucket CORS full topology ownership was not claimed" }
    Set-CorsStage -State $State -Stage "health"
    Wait-CorsTopologyHealthy -State $state -Services @('postgres', 'kubo', 'gateway')
    Set-CorsStage -State $State -Stage "postgres"
    Assert-Postgres17Ready -State $State
    if ($State.Mode -eq "aws") {
        Invoke-CorsAwsDiagnostic -State $State
        return
    } elseif ($State.Mode -eq "browser") {
        Invoke-CorsBrowserDiagnostic -State $State
        return
    }
    Invoke-CorsPostgresSuite -State $state
    $network = Get-CorsComposeNetwork -State $State
    Set-CorsStage -State $State -Stage "aws"
    $null = New-CorsAwsConfig -State $State
    Invoke-CorsAwsParity -State $state -Network $network
    Set-CorsStage -State $State -Stage "browser"
    Invoke-CorsBrowserParity -State $state
}

$runId = New-CorsRunId
$ports = New-CorsLoopbackPorts -RunId $runId
$state = @{
    TempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
    Mode = if ($PostgresOnly) { "postgres" } elseif ($DiagnoseAws) { "aws" } elseif ($DiagnoseBrowser) { "browser" } else { "full" }
    RunId = $runId
    Ports = $ports
    Project = $null
    GatewayImage = $null
    GatewayImageId = $null
    Bucket = $null
    RunRoot = $null
    Receipt = $null
    EnvironmentState = @()
    EnvironmentOwned = $false
    RunRootPreexisting = $false
    RunRootCreated = $false
    ReceiptOwned = $false
    ProjectOwned = $false
    ProjectProbeUncertain = $false
    GatewayImagePreflightAbsent = $false
    GatewayImageOwned = $false
    Stage = "preflight"
    Receipts = [Collections.Generic.List[string]]::new()
}
$workSucceeded = $false
$cleanupErrors = [Collections.Generic.List[string]]::new()
try {
    $state.Project = New-CorsProjectName -RunId $state.RunId
    $state.GatewayImage = New-CorsGatewayImage -RunId $state.RunId
    $state.Bucket = New-CorsBucketName -RunId $state.RunId
    $null = New-CorsRunRoot -State $state
    $state.Receipt = New-CorsOwnershipReceipt -RunRoot $state.RunRoot -RunId $state.RunId -Project $state.Project -GatewayImage $state.GatewayImage
    $state.ReceiptOwned = $true
    $state.EnvironmentState = Save-EnvironmentState -Names $TouchedEnvironmentNames
    $state.EnvironmentOwned = $true
    Set-CorsEnvironment -Name "COMPOSE_DISABLE_ENV_FILE" -Value "1"
    Set-CorsEnvironment -Name "IPFS_S3_CORS_POSTGRES_PORT" -Value ([string]$state.Ports.Postgres)
    Set-CorsEnvironment -Name "IPFS_S3_CORS_KUBO_PORT" -Value ([string]$state.Ports.Kubo)
    Set-CorsEnvironment -Name "IPFS_S3_CORS_GATEWAY_PORT" -Value ([string]$state.Ports.Gateway)
    Set-CorsEnvironment -Name "IPFS_S3_CORS_IMAGE" -Value $state.GatewayImage
    Set-CorsEnvironment -Name "IPFS_S3_CORS_PROJECT_LABEL" -Value $state.Project
    Set-CorsEnvironment -Name "IPFS_S3_CORS_RUN_LABEL" -Value $state.RunId
    Invoke-CorsMain -State $state
    $workSucceeded = $true
} catch {
    $workSucceeded = $false
} finally {
    Set-CorsStage -State $state -Stage "cleanup"
    try {
        foreach ($cleanupError in (Remove-OwnedCorsResources -State $state)) { $cleanupErrors.Add($cleanupError) }
    } catch {
        $cleanupErrors.Add("cleanup")
    }
}

if ($cleanupErrors.Count -ne 0) { $workSucceeded = $false }
$state.Receipts.Add((Write-CorsEvidence -Category "cleanup" -Value "cleanup-errors=$($cleanupErrors.Count)"))
$state.Receipts.Add((Write-CorsEvidence -Category "result" -Value $(if ($workSucceeded) { "work=passed" } else { "work=failed" })))
Write-CorsReceipts -State $state
if ($workSucceeded) {
    if ($PostgresOnly) {
        Write-Output "Bucket CORS PostgreSQL validation: PASS"
    } elseif ($DiagnoseAws) {
        Write-Output "Bucket CORS AWS diagnostic: PASS"
    } elseif ($DiagnoseBrowser) {
        Write-Output "Bucket CORS browser diagnostic: PASS"
    } else {
        Write-Output "Bucket CORS validation: PASS"
    }
    exit 0
}
if ($DiagnoseAws) {
    Write-Output "Bucket CORS AWS diagnostic: FAILED"
} elseif ($DiagnoseBrowser) {
    Write-Output "Bucket CORS browser diagnostic: FAILED"
} else {
    Write-Output "Bucket CORS validation: FAILED"
}
exit 1
