#requires -Version 7.0
<#
Stage5 leaf RPC/DAG/stored-byte acceptance. Not worker/config/S3/account/main QA.
Only cached ipfs/kubo:v0.43.0 is used. No pull/install, no external swarm, no
existing-environment operations. CID uses explicit unpinned CAR preseed.
The checked-in v0.43.0 isolation values were officially confirmed by the parent.
Config alone is not egress proof: Docker internal network is mandatory, along
with fresh repositories (no 0.43.0 legacy bootstrap backup) and offline daemons.
Logs and summary remain under OutputParent; only this run's labeled Docker
containers/volumes/network are removed, even on test failure.
#>
[CmdletBinding()]
param(
    [string]$IsolationConfig = (Join-Path $PSScriptRoot 'ipfs_rpc_real.kubo-config.json'),
    [string]$OutputParent = (Join-Path ([IO.Path]::GetTempPath()) 'opencode'),
    [switch]$PreseedProbe
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
Set-StrictMode -Version Latest
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (-not (Test-Path -LiteralPath $OutputParent -PathType Container)) { throw 'Exact temporary output parent must already exist.' }
$OutputParent = (Resolve-Path -LiteralPath $OutputParent).Path
$RunId = [guid]::NewGuid().ToString('N')
$Prefix = "ipfs-s3-rpc-$RunId"
$ResultDir = Join-Path $OutputParent $Prefix
[void][IO.Directory]::CreateDirectory($ResultDir)
$Label = "ipfs_s3.rpc_real.run=$RunId"
$Image = 'ipfs/kubo:v0.43.0'
$SourceName = "$Prefix-source"
$TargetName = "$Prefix-target"
$SourceVolume = "$Prefix-source-data"
$TargetVolume = "$Prefix-target-data"
$NetworkName = "$Prefix-net"
$Diagnostic = Join-Path $ResultDir 'runner.log'
$DaemonProcesses = @()
$Relays = @()
$ResourcesAuthorized = $false
$SavedEnvironment = @{}
$Summary = [ordered]@{
    run_id = $RunId; status = 'FAIL'; scope = 'leaf-provider/DAG/stored-bytes only'
    image = $Image; image_id = $null; compile = 'NOT RUN'; test = 'NOT RUN'; test_exit_code = $null
    default_tests = 'NOT RUN'; missing_urls = 'NOT RUN'
    cleanup = 'NOT RUN'; output = $ResultDir; failure = $null
}
if ($PreseedProbe) { $Summary.scope = 'preseed import diagnostic only; NOT full provider acceptance' }
$RealTestName = if ($PreseedProbe) { 'rpc_preseed_import_contract_probe' } else { 'rpc_provider_real_transports_and_local_dags' }

function Write-Log([string]$Text) {
    [IO.File]::AppendAllText($Diagnostic, "$Text`n")
    $Text
}

function Invoke-Docker([string[]]$Arguments) {
    $text = & docker @Arguments 2>&1
    $code = $LASTEXITCODE
    $result = ($text | ForEach-Object { $_.ToString() }) -join "`n"
    [IO.File]::AppendAllText($Diagnostic, "docker $($Arguments -join ' ') [exit=$code]`n$result`n")
    if ($code -ne 0) { throw "Docker command/inventory failed (exit=$code): $($Arguments -join ' ')" }
    return $result
}

function Get-Inventory([ValidateSet('container', 'volume', 'network')][string]$Kind, [string]$Name = '') {
    $arguments = @($Kind, 'ls')
    if ($Kind -eq 'container') { $arguments += '--all' }
    $filter = if ([string]::IsNullOrEmpty($Name)) { "label=$Label" } else { "name=$Name" }
    $arguments += @('--filter', $filter, '--format', '{{.Name}}')
    if ($Kind -eq 'container') { $arguments[-1] = '{{.Names}}' }
    $text = Invoke-Docker $arguments
    return @($text -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Assert-ResourceLabel([string]$Kind, [string]$Name) {
    $items = @(Invoke-Docker @($Kind, 'inspect', $Name) | ConvertFrom-Json)
    if ($items.Count -ne 1) { throw "Ambiguous inventory: $Kind $Name" }
    $labels = if ($Kind -eq 'container') { $items[0].Config.Labels } else { $items[0].Labels }
    if ($labels.'ipfs_s3.rpc_real.run' -cne $RunId) { throw "Refusing to operate on a foreign resource: $Name" }
    return $items[0]
}

function Read-IsolationConfig {
    $config = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $IsolationConfig).Path) | ConvertFrom-Json
    if ($config.version -cne '0.43.0' -or $config.official_confirmation -ne $true -or @($config.documentation).Count -eq 0) {
        throw 'Need parent-supplied officially confirmed Kubo 0.43.0 isolation config; no node was started.'
    }
    $purposes = @('telemetry_disabled', 'bootstrap_empty', 'mdns_disabled', 'routing_disabled', 'autoconf_disabled')
    $actual = @($config.environment | ForEach-Object { $_.purpose }) + @($config.settings | ForEach-Object { $_.purpose })
    foreach ($purpose in $purposes) {
        if (@($actual | Where-Object { $_ -ceq $purpose }).Count -ne 1) { throw "One confirmed setting/environment entry required for $purpose" }
    }
    foreach ($entry in $config.environment) {
        if ($entry.name -cnotmatch '^IPFS_[A-Z0-9_]+$' -or $entry.value -isnot [string]) { throw 'Invalid isolation environment entry.' }
    }
    foreach ($entry in $config.settings) {
        if ($entry.key -cnotmatch '^[A-Za-z][A-Za-z0-9_.]+$') { throw 'Invalid isolation config key.' }
        if ($entry.purpose -ceq 'bootstrap_empty' -and ($entry.key -cne 'Bootstrap' -or (ConvertTo-Json -InputObject $entry.value -Compress) -cne '[]')) {
            throw 'Bootstrap must be exactly an empty list.'
        }
    }
    return $config
}

function Start-Node([string]$Name, [string]$Volume, $Config) {
    $arguments = @('run', '--detach', '--pull=never', '--name', $Name, '--label', $Label,
        '--network', $NetworkName, '--mount', "type=volume,source=$Volume,target=/data/ipfs",
        '--entrypoint', '/bin/sleep')
    foreach ($entry in $Config.environment) { $arguments += @('--env', "$($entry.name)=$($entry.value)") }
    $arguments += @($Image, 'infinity')
    [void](Invoke-Docker $arguments)
    [void](Invoke-Docker @('exec', $Name, 'ipfs', 'init'))
    foreach ($entry in $Config.settings) {
        $json = ConvertTo-Json -InputObject $entry.value -Depth 20 -Compress
        [void](Invoke-Docker @('exec', $Name, 'ipfs', 'config', '--json', $entry.key, $json))
    }
    [void](Invoke-Docker @('exec', $Name, 'ipfs', 'config', 'Addresses.API', '/ip4/0.0.0.0/tcp/5001'))
    foreach ($entry in $Config.settings) {
        $actual = Invoke-Docker @('exec', $Name, 'ipfs', 'config', $entry.key)
        $expected = ConvertTo-Json -InputObject $entry.value -Depth 20 -Compress
        if ($entry.value -is [string]) {
            if ($actual.Trim() -cne $entry.value -and $actual.Trim() -cne $expected) { throw "Isolation readback mismatch: $($entry.key)" }
        } else {
            $normalized = ConvertTo-Json -InputObject (ConvertFrom-Json -InputObject $actual -NoEnumerate) -Depth 20 -Compress
            if ($normalized -cne $expected) { throw "Isolation readback mismatch: $($entry.key)" }
        }
    }
    # Attach daemon stdout/stderr to host logs before any test or cleanup, without
    # shell commands/redirection inside the container and without a third node.
    $daemon = Start-Process -FilePath (Get-Command docker).Source -ArgumentList @('exec', $Name, 'ipfs', 'daemon', '--offline') `
        -RedirectStandardOutput (Join-Path $ResultDir "$Name.daemon.stdout.log") `
        -RedirectStandardError (Join-Path $ResultDir "$Name.daemon.stderr.log") -PassThru
    $script:DaemonProcesses += $daemon
    $container = Assert-ResourceLabel 'container' $Name
    foreach ($property in $container.NetworkSettings.Ports.PSObject.Properties) {
        if (@($property.Value).Count -ne 0 -and $null -ne $property.Value) { throw 'Internal node unexpectedly publishes a port.' }
    }
    # Docker internal networks do not publish ports in Engine 28.3.3. Keep the
    # no-egress boundary and forward unmodified TCP via exact own-container exec.
    [void](Invoke-Docker @('exec', $Name, 'nc', '--help'))
    $relay = [IpfsRpcRealRelay]::new((Get-Command docker).Source, $Name, (Join-Path $ResultDir "$Name.relay.log"))
    $script:Relays += $relay
    return $relay.Url
}

function Wait-Rpc([string]$Url) {
    $deadline = [DateTime]::UtcNow.AddSeconds(45)
    $handler = [Net.Http.HttpClientHandler]::new()
    $handler.UseProxy = $false
    $client = [Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(2)
    try {
        while ([DateTime]::UtcNow -lt $deadline) {
            try {
                $response = $client.PostAsync("$Url/api/v0/version", $null).GetAwaiter().GetResult()
                try {
                    if ($response.IsSuccessStatusCode) {
                        $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
                        if ($body.Version -cne '0.43.0') { throw 'Wrong Kubo version.' }
                        return
                    }
                } finally { $response.Dispose() }
            } catch [Net.Http.HttpRequestException] { } catch [Threading.Tasks.TaskCanceledException] { }
            Start-Sleep -Milliseconds 250
        }
        throw 'Owned Kubo RPC readiness deadline exceeded.'
    } finally { $client.Dispose() }
}

function Remove-OwnedResources {
    $failed = $false
    $containers = if ($ResourcesAuthorized) { @($SourceName, $TargetName) } else { @() }
    # First collect BOTH nodes' logs; only then remove any Docker resource.
    foreach ($name in $containers) {
        try {
            $inventory = @(Get-Inventory 'container')
            if ($inventory -ccontains $name) {
                [void](Assert-ResourceLabel 'container' $name)
                # Collect container logs BEFORE removal; attached daemon logs
                # were already streamed into ResultDir from process startup.
                $logs = Invoke-Docker @('logs', $name)
                [IO.File]::WriteAllText((Join-Path $ResultDir "$name.container.log"), $logs)
            }
        } catch { $failed = $true; Write-Log "cleanup/log failure: $($_.Exception.Message)" }
    }
    foreach ($relay in $Relays) {
        try { $relay.Dispose() }
        catch { $failed = $true; Write-Log "owned loopback relay cleanup failure: $($_.Exception.Message)" }
    }
    foreach ($name in $containers) {
        try {
            if (@(Get-Inventory 'container') -ccontains $name) {
                [void](Assert-ResourceLabel 'container' $name)
                [void](Invoke-Docker @('container', 'rm', '--force', $name))
            }
        } catch { $failed = $true; Write-Log "container cleanup failure: $($_.Exception.Message)" }
    }
    foreach ($kind in @('volume', 'network')) {
        $names = if (-not $ResourcesAuthorized) { @() } elseif ($kind -eq 'volume') { @($SourceVolume, $TargetVolume) } else { @($NetworkName) }
        foreach ($name in $names) {
            try {
                $inventory = @(Get-Inventory $kind)
                if ($inventory -ccontains $name) {
                    [void](Assert-ResourceLabel $kind $name)
                    [void](Invoke-Docker @($kind, 'rm', $name))
                }
            } catch { $failed = $true; Write-Log "cleanup failure: $($_.Exception.Message)" }
        }
    }
    foreach ($kind in @('container', 'volume', 'network')) {
        try {
            if (@(Get-Inventory $kind).Count -ne 0) { throw "Run-labeled $kind resources remain." }
        } catch { $failed = $true; Write-Log "final inventory failure: $($_.Exception.Message)" }
    }
    foreach ($daemon in $DaemonProcesses) {
        if (-not $daemon.WaitForExit(10000)) { $failed = $true; Write-Log 'Attached own-daemon logging process did not exit.' }
        $daemon.Dispose()
    }
    if ($failed) { throw 'Log collection/cleanup/final inventory is not proven; acceptance FAIL.' }
}

try {
    foreach ($name in @('RUSTUP_AUTO_INSTALL', 'IPFS_S3_REAL_RPC_SOURCE_URL', 'IPFS_S3_REAL_RPC_TARGET_URL', 'IPFS_S3_REAL_RPC_FIXTURE',
        'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY')) {
        $SavedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    $env:RUSTUP_AUTO_INSTALL = '0'
    foreach ($name in @('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY')) {
        [Environment]::SetEnvironmentVariable($name, [NullString]::Value, 'Process')
    }
    $env:NO_PROXY = '127.0.0.1,localhost'
    $manifest = Join-Path $RepoRoot 'Cargo.toml'
    $compileLog = Join-Path $ResultDir 'compile.jsonl'
    $artifacts = @()
    & cargo test --locked --offline --manifest-path $manifest --test ipfs_rpc_real --no-run --message-format=json 2>&1 | ForEach-Object {
        $line = $_.ToString()
        [IO.File]::AppendAllText($compileLog, "$line`n")
        if ($line.StartsWith('{')) {
            $record = $line | ConvertFrom-Json
            if ($record.reason -eq 'compiler-artifact' -and $record.target.name -eq 'ipfs_rpc_real' -and $record.executable) {
                $artifacts += $record.executable
            }
        }
    }
    if ($LASTEXITCODE -ne 0 -or $artifacts.Count -ne 1) {
        $Summary.compile = 'FAIL'
        throw 'Real fixture compilation failed; retained files/logs, no retries and no Docker nodes started.'
    }
    $Summary.compile = 'PASS'
    $defaultLog = Join-Path $ResultDir 'default-tests.log'
    & $artifacts[0] --nocapture --test-threads=1 2>&1 | ForEach-Object {
        [IO.File]::AppendAllText($defaultLog, "$($_.ToString())`n")
    }
    if ($LASTEXITCODE -ne 0) { $Summary.default_tests = 'FAIL'; throw 'Default fixture tests failed.' }
    $Summary.default_tests = 'PASS (real RPC test ignored)'
    foreach ($name in @('IPFS_S3_REAL_RPC_SOURCE_URL', 'IPFS_S3_REAL_RPC_TARGET_URL', 'IPFS_S3_REAL_RPC_FIXTURE')) {
        [Environment]::SetEnvironmentVariable($name, [NullString]::Value, 'Process')
    }
    foreach ($missing in @('SOURCE', 'TARGET')) {
        $missingLog = Join-Path $ResultDir "missing-$($missing.ToLowerInvariant())-url.log"
        $lines = @()
        & $artifacts[0] --ignored --exact rpc_provider_real_transports_and_local_dags --nocapture --test-threads=1 2>&1 | ForEach-Object {
            $line = $_.ToString(); $lines += $line
            [IO.File]::AppendAllText($missingLog, "$line`n")
        }
        if ($LASTEXITCODE -eq 0 -or ($lines -join "`n") -notmatch "IPFS_S3_REAL_RPC_$($missing)_URL is required") {
            $Summary.missing_urls = 'FAIL'; throw 'Explicit missing-URL invocation did not fail at its required precondition.'
        }
        $env:IPFS_S3_REAL_RPC_SOURCE_URL = 'http://127.0.0.1:1'
    }
    [Environment]::SetEnvironmentVariable('IPFS_S3_REAL_RPC_SOURCE_URL', [NullString]::Value, 'Process')
    $Summary.missing_urls = 'PASS (source and target missing each exit nonzero)'
    $config = Read-IsolationConfig
    Add-Type -Path (Join-Path $PSScriptRoot 'support/ipfs_rpc_real_relay.cs')
    $Summary.image_id = Invoke-Docker @('image', 'inspect', $Image, '--format', '{{.Id}}')
    foreach ($kind in @('container', 'volume', 'network')) {
        if (@(Get-Inventory $kind).Count -ne 0) { throw 'Fresh run label unexpectedly exists; do not touch that environment.' }
    }
    foreach ($resource in @(@('container', $SourceName), @('container', $TargetName),
        @('volume', $SourceVolume), @('volume', $TargetVolume), @('network', $NetworkName))) {
        if (@(Get-Inventory $resource[0] $resource[1]) -ccontains $resource[1]) {
            throw "Exact resource name already exists; refusing to reuse or operate on it: $($resource[1])"
        }
    }
    $ResourcesAuthorized = $true
    [void](Invoke-Docker @('network', 'create', '--internal', '--label', $Label, $NetworkName))
    foreach ($volume in @($SourceVolume, $TargetVolume)) {
        [void](Invoke-Docker @('volume', 'create', '--label', $Label, $volume))
    }
    $sourceUrl = Start-Node $SourceName $SourceVolume $config
    $targetUrl = Start-Node $TargetName $TargetVolume $config
    Wait-Rpc $sourceUrl
    Wait-Rpc $targetUrl
    $fixturePath = Join-Path $ResultDir 'fixture.json'
    $fixture = [ordered]@{ run_id = $RunId; network = $NetworkName; source = $SourceName; target = $TargetName;
        source_volume = $SourceVolume; target_volume = $TargetVolume;
        host_forwarding = 'loopback-docker-exec-nc'; source_url = $sourceUrl; target_url = $targetUrl }
    [IO.File]::WriteAllText($fixturePath, ($fixture | ConvertTo-Json))
    $env:IPFS_S3_REAL_RPC_SOURCE_URL = $sourceUrl
    $env:IPFS_S3_REAL_RPC_TARGET_URL = $targetUrl
    $env:IPFS_S3_REAL_RPC_FIXTURE = $fixturePath
    $testLog = Join-Path $ResultDir 'acceptance.log'
    & $artifacts[0] --ignored --exact $RealTestName --nocapture --test-threads=1 2>&1 | ForEach-Object {
        $line = $_.ToString()
        [IO.File]::AppendAllText($testLog, "$line`n")
        $line
    }
    $Summary.test_exit_code = $LASTEXITCODE
    if ($Summary.test_exit_code -ne 0) { $Summary.test = 'FAIL'; throw 'Real RPC acceptance failed; see acceptance.log.' }
    $Summary.test = 'PASS'
} catch {
    $Summary.failure = $_.Exception.Message
    Write-Log "FAIL: $($Summary.failure)"
} finally {
    try { Remove-OwnedResources; $Summary.cleanup = 'PASS (zero run-labeled resources)' }
    catch { $Summary.cleanup = 'FAIL'; $Summary.failure = "$($Summary.failure) $($_.Exception.Message)" }
    foreach ($entry in $SavedEnvironment.GetEnumerator()) {
        $value = if ($null -eq $entry.Value) { [NullString]::Value } else { $entry.Value }
        [Environment]::SetEnvironmentVariable($entry.Key, $value, 'Process')
    }
    if ($Summary.compile -eq 'PASS' -and $Summary.test -eq 'PASS' -and $Summary.cleanup.StartsWith('PASS')) { $Summary.status = 'PASS' }
    [IO.File]::WriteAllText((Join-Path $ResultDir 'summary.json'), ($Summary | ConvertTo-Json -Depth 10))
    Write-Log "status=$($Summary.status) cleanup=$($Summary.cleanup) evidence=$ResultDir"
}
if ($Summary.status -ne 'PASS') { exit 1 }
