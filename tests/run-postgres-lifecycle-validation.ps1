#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string]$PostgresUrl,
    [string]$MultiGatewayDatabaseUrl,
    [switch]$SkipTeardown
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ManifestPath = Join-Path $RepoRoot "Cargo.toml"
$ResultsPath = Join-Path $RepoRoot "tests/results/postgres-lifecycle-validation"
[void][IO.Directory]::CreateDirectory($ResultsPath)
$runId = [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssfffffffZ") + "-" + [guid]::NewGuid().ToString("N")
$SummaryPath = Join-Path $ResultsPath "$runId.summary.json"
$DiagnosticPath = Join-Path $ResultsPath "$runId.diagnostics.log"
if ([string]::IsNullOrWhiteSpace($MultiGatewayDatabaseUrl)) { $MultiGatewayDatabaseUrl = $PostgresUrl }
$exitCode = 1
$summary = [ordered]@{
    run_id = $runId
    started_utc = [DateTime]::UtcNow.ToString("o")
    finished_utc = $null
    status = "FAIL"
    exit_code = 1
    skip_teardown = [bool]$SkipTeardown
    cleanup = "Runner owns no services or schemas; Rust fixtures perform their own cleanup."
    diagnostics = $DiagnosticPath
    steps = [ordered]@{}
}
foreach ($step in @('compile-postgres_lifecycle', 'compile-multi_gateway', 'test-postgres_lifecycle', 'test-multi_gateway')) {
    $summary.steps[$step] = [ordered]@{ status = "NOT RUN"; exit_code = $null; stdout = $null; stderr = $null }
}
$savedEnvironment = @{}
foreach ($name in @('IPFS_S3_TEST_POSTGRES_URL', 'IPFS_S3_MULTI_GATEWAY_DATABASE_URL', 'CARGO_NET_OFFLINE', 'RUSTUP_AUTO_INSTALL')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}

function Protect-Diagnostic {
    param([AllowEmptyString()][string]$Text)
    foreach ($url in @($PostgresUrl, $MultiGatewayDatabaseUrl)) {
        $Text = $Text.Replace($url, '[REDACTED_DATABASE_URL]')
    }
    return [regex]::Replace($Text, '(?i)postgres(?:ql)?://[^\s"''<>]+', '[REDACTED_DATABASE_URL]')
}

function Write-Diagnostic {
    param([string]$Message)
    $safe = Protect-Diagnostic $Message
    [IO.File]::AppendAllText($DiagnosticPath, "$safe`n")
    Write-Host $safe
}

function Assert-PostgresReachable {
    param([string]$Url)
    $uri = $null
    if (-not [Uri]::TryCreate($Url, [UriKind]::Absolute, [ref]$uri) -or
        $uri.Scheme -notin @('postgres', 'postgresql') -or [string]::IsNullOrWhiteSpace($uri.Host)) {
        throw "A postgres:// or postgresql:// URL with an explicit host is required."
    }
    $port = if ($uri.Port -lt 0) { 5432 } else { $uri.Port }
    $client = [Net.Sockets.TcpClient]::new()
    try {
        $connection = $client.ConnectAsync($uri.DnsSafeHost, $port)
        if (-not $connection.Wait([TimeSpan]::FromSeconds(5))) { throw "Connection deadline exceeded" }
        $connection.GetAwaiter().GetResult()
        $stream = $client.GetStream()
        $stream.ReadTimeout = 5000
        $stream.WriteTimeout = 5000
        # An SSLRequest distinguishes PostgreSQL from an arbitrary open TCP port without authenticating or mutating it.
        $request = [byte[]]@(0, 0, 0, 8, 4, 210, 22, 47)
        $stream.Write($request, 0, $request.Length)
        $response = $stream.ReadByte()
        if ($response -notin @(83, 78)) { throw "Not a PostgreSQL SSL negotiation response" }
    }
    catch {
        throw "PostgreSQL reachability probe failed. Prepare a reachable PostgreSQL 17 instance yourself; no infrastructure will be provisioned."
    }
    finally { $client.Dispose() }
    Write-Diagnostic "PostgreSQL protocol reachable; authentication, server major version and gateway/database pairing remain Task 9 prerequisites."
}

function Assert-MultiGatewayEnvironment {
    foreach ($name in @(
        'IPFS_S3_MULTI_GATEWAY_A_ENDPOINT',
        'IPFS_S3_MULTI_GATEWAY_B_ENDPOINT',
        'IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT',
        'IPFS_S3_MULTI_GATEWAY_KUBO_URL'
    )) {
        $value = [Environment]::GetEnvironmentVariable($name, "Process")
        $uri = $null
        if ([string]::IsNullOrWhiteSpace($value) -or
            -not [Uri]::TryCreate($value, [UriKind]::Absolute, [ref]$uri) -or
            $uri.Scheme -notin @('http', 'https') -or $value -match '(?i)localhost') {
            throw "$name must be an explicit HTTP(S) endpoint without localhost; prepare the isolated Task 9 topology first."
        }
    }
}

function Invoke-CargoStep {
    param(
        [string]$Name,
        [ValidateSet("postgres_lifecycle", "multi_gateway")]
        [string]$Target,
        [switch]$Compile
    )
    $step = $summary.steps[$Name]
    $step.status = "FAIL"
    $step.stdout = Join-Path $ResultsPath "$runId.$Name.stdout.log"
    $step.stderr = Join-Path $ResultsPath "$runId.$Name.stderr.log"
    $arguments = @('--test', $Target)
    if ($Compile) { $arguments += @("--no-run", "--message-format=json") }
    else { $arguments += @('--', "--nocapture", "--test-threads=1") }
    Write-Diagnostic "Starting $Name; stdout=$($step.stdout); stderr=$($step.stderr)"
    $stdout = [IO.StreamWriter]::new($step.stdout, $false)
    $stderr = [IO.StreamWriter]::new($step.stderr, $false)
    try {
        & $CargoPath test --locked --offline --manifest-path $ManifestPath @arguments 2>&1 | ForEach-Object {
            $line = Protect-Diagnostic $_.ToString()
            if ($_ -is [Management.Automation.ErrorRecord]) { $stderr.WriteLine($line); $stderr.Flush() }
            else { $stdout.WriteLine($line); $stdout.Flush() }
        }
        $code = $LASTEXITCODE
        $step.exit_code = $code
        if ($code -ne 0) {
            $script:exitCode = $code
            throw "$Name failed with exit code $code"
        }
    }
    finally { $stdout.Dispose(); $stderr.Dispose() }

    if ($Compile) {
        $artifacts = @(
            foreach ($line in [IO.File]::ReadLines($step.stdout)) {
                $message = ConvertFrom-Json -InputObject $line
                if ($message.reason -eq 'compiler-artifact' -and $message.target.name -ceq $Target -and
                    'test' -in $message.target.kind -and $message.profile.test -and $null -ne $message.executable) {
                    $message.executable
                }
            }
        )
        if ($artifacts.Count -ne 1 -or -not [IO.File]::Exists($artifacts[0])) {
            throw "$Name did not produce exactly one existing local test executable."
        }
    }
    else {
        $output = [IO.File]::ReadAllText($step.stdout) + [IO.File]::ReadAllText($step.stderr)
        if ($output -match '(?i)\bskipping\b' -or
            $output -notmatch 'test result: ok\. [1-9][0-9]* passed; 0 failed; 0 ignored; 0 measured; 0 filtered out') {
            throw "$Name did not prove a positive, unfiltered, non-skipped test run."
        }
    }
    $step.status = "PASS"
    Write-Diagnostic "$Name PASS (exit code $code)"
}

$locationPushed = $false
try {
    Write-Diagnostic "Evidence: $SummaryPath"
    if ($SkipTeardown) {
        Write-Diagnostic "SkipTeardown skips runner-level teardown only and does not disable Rust fixture cleanup; logs and external services are always retained."
    }
    # The fixture guard is the only authority that knows its schema identity; repeating cleanup could delete another run's data.
    Write-Diagnostic $summary.cleanup
    Assert-PostgresReachable $PostgresUrl
    if ($MultiGatewayDatabaseUrl -cne $PostgresUrl) { Assert-PostgresReachable $MultiGatewayDatabaseUrl }
    Assert-MultiGatewayEnvironment
    # Get-Command can surface multiple cargo shims on PATH (e.g. scoop current + persist junctions);
    # -First pins one deterministic executable instead of an array that stringifies into an invalid path.
    $CargoPath = @(Get-Command cargo -CommandType Application -ErrorAction Stop)[0].Source
    $env:IPFS_S3_TEST_POSTGRES_URL = $PostgresUrl
    $env:IPFS_S3_MULTI_GATEWAY_DATABASE_URL = $MultiGatewayDatabaseUrl
    $env:CARGO_NET_OFFLINE = 'true'
    $env:RUSTUP_AUTO_INSTALL = '0'
    Push-Location -LiteralPath $RepoRoot
    $locationPushed = $true
    Invoke-CargoStep "compile-postgres_lifecycle" "postgres_lifecycle" -Compile
    Invoke-CargoStep "compile-multi_gateway" "multi_gateway" -Compile
    Invoke-CargoStep "test-postgres_lifecycle" "postgres_lifecycle"
    Invoke-CargoStep "test-multi_gateway" "multi_gateway"
    $exitCode = 0
    $summary.status = "PASS"
}
catch {
    Write-Diagnostic ("FAIL: " + $_.Exception.Message)
    Write-Diagnostic "Stopped; later steps remain NOT RUN. Collected output paths are in $SummaryPath"
}
finally {
    foreach ($name in $savedEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], "Process")
    }
    if ($locationPushed) { Pop-Location }
    $summary.finished_utc = [DateTime]::UtcNow.ToString("o")
    $summary.exit_code = $exitCode
    [IO.File]::WriteAllText($SummaryPath, ($summary | ConvertTo-Json -Depth 6))
    Write-Host "Result: $($summary.status); exit code: $exitCode; evidence: $SummaryPath"
}
exit $exitCode
