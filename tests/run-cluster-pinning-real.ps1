#requires -Version 7.0
<#
Stage6 isolated Cluster leaf acceptance, NOT complete Stage6 QA.
From the repository root: pwsh -NoProfile -File tests/run-cluster-pinning-real.ps1
Independent-source provider increment: add -ProviderPhase (does not rerun old matrix).
Requires the two exact images already cached, Docker and the existing Rust tools.
No pull/install/accounts/production resources. Two allocated fresh Kubo repositories
and two fresh Cluster identities, plus an independent source in ProviderPhase,
on one internal network. No Docker ports are
published: random host-loopback raw TCP relays use exact owned docker exec nc.
No gateway, DB or physical GC. The Stage5 relay source is read-only; a
strict name/port adaptation is compiled only inside this run's unique temp dir.

Online Kubo is intentional: offline Kubo rejects swarm APIs used by Cluster.
Kubo swarm listeners are empty; routing/discovery/autoconf/telemetry are disabled.
The Docker internal network is the egress boundary, not config alone. All config
syntax/source references are pinned in cluster-pinning-real.kubo-config.json.
Cluster uses explicit CRDT bootstrap, no mDNS, no automatic repinning, long
automatic recovery interval and an explicit CID-scoped recovery in the test.

Compilation is attempted ONCE before creating resources, offline/locked. The
provider phase runs only its new bounded contracts and exact ignored real case.
Do not invoke ProviderPhase until the parent confirms the integrated tree is ready.
Logs precede deletion, cleanup touches only exact names with this unique label,
and exit-0 empty inventories are mandatory. Logs/summary stay in OS temp/opencode.
#>
[CmdletBinding()]
param([switch]$ProviderPhase, [string]$OutputParent = (Join-Path ([IO.Path]::GetTempPath()) 'opencode'))

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
Set-StrictMode -Version Latest
$parseTokens = $null
$parseErrors = $null
[void][Management.Automation.Language.Parser]::ParseFile($PSCommandPath, [ref]$parseTokens, [ref]$parseErrors)
if ($parseErrors.Count -ne 0) { throw 'Runner Parser check failed; no resources were created.' }
if (-not (Test-Path -LiteralPath $OutputParent -PathType Container)) { throw 'Output parent must already exist.' }
$OutputParent = (Resolve-Path -LiteralPath $OutputParent).Path
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ((Get-Location).Path -ne $RepoRoot) { throw 'Run from the repository root so Cargo uses the checked-in toolchain/config.' }
$ConfigPath = Join-Path $PSScriptRoot 'cluster-pinning-real.kubo-config.json'
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) { throw 'Missing checked-in isolation config.' }
$RunId = [guid]::NewGuid().ToString('N')
$Prefix = "ipfs-s3-cluster-real-$RunId"
$ResultDir = Join-Path $OutputParent $Prefix
if (Test-Path -LiteralPath $ResultDir) { throw 'Exact output directory collision; refusing reuse.' }
[void][IO.Directory]::CreateDirectory($ResultDir)
if (-not (Test-Path -LiteralPath $ResultDir -PathType Container)) { throw 'Output directory was not created.' }
$Diagnostic = Join-Path $ResultDir 'runner.log'
$LabelKey = 'ipfs_s3.cluster_real.run'
$Label = "$LabelKey=$RunId"
$KuboImage = 'ipfs/kubo:v0.43.0'
$ClusterDigest = 'sha256:a83266c524f1c0bc81d14fe3c8b46c5b83a7b2d8432fb8a50300f27d3c863dcd'
$ClusterImage = "ipfs/ipfs-cluster:v1.1.6@$ClusterDigest"
$NetworkName = "$Prefix-net"
$KuboA = "$Prefix-kubo-a"
$KuboB = "$Prefix-kubo-b"
$Source = "$Prefix-source"
$ClusterA = "$Prefix-cluster-a"
$ClusterB = "$Prefix-cluster-b"
$Containers = @($KuboA, $KuboB, $ClusterA, $ClusterB)
if ($ProviderPhase) { $Containers += $Source }
$Volumes = @($Containers | ForEach-Object { "$_-data" })
$ResourcesAuthorized = $false
$DaemonProcesses = @()
$Relays = @()
$SavedEnvironment = @{}
$Summary = [ordered]@{
    run_id = $RunId; status = 'FAIL'; scope = 'Cluster REST replica verifier leaf only'
    kubo_image = $KuboImage; cluster_image = $ClusterImage
    kubo_image_id = $null; cluster_image_id = $null
    compile = 'NOT RUN'; test = 'NOT RUN'; cleanup = 'NOT RUN'; failure = $null
    account_writes = 'NOT RUN (no account authorization; isolated anonymous nodes only)'
    default_tests = 'NOT RUN'; missing_endpoints = 'NOT RUN'; relay_compile = 'NOT RUN'
    host_forwarding = 'loopback-docker-exec-nc'; relay_cleanup = 'NOT RUN'
    wire_capture = 'unmodified Cluster request/response TCP bytes, max 32 MiB per connection/direction, run-owned files only'
    assertions = '1/2 -> 2/2 single raw CID; local bytes; allocated Kubo outage; supersession; one logical pinset across two REST addresses'
    exclusions = 'provider/worker/Proxy/CAR/S3/account integration; disaster recovery; continuous N/N; physical reclamation'
    output = $ResultDir
}
if ($ProviderPhase) {
    $Summary.scope = 'Independent-source Proxy+REST provider transport and fresh REST 2/2 leaf'
    $Summary.assertions = 'ten original DAGs absent on both targets; independent CAR file/nested directory/product-crypto and upload/plain+product-crypto stored bytes; two real chunker mismatches preserve unexpected CID/no original fallback; two source-only CID roots never copied; source stopped; both targets offline twelve full reachable hashverified DAGs/bytes including unexpected roots; cid-only re-pin; fresh 2/2; ownership Unknown; new CAR fault contracts'
    $Summary.exclusions = 'full Stage6; worker/registry/S3/auth/account; continuous availability; disaster recovery; GC'
}
$Target = if ($ProviderPhase) { 'cluster_provider_real' } else { 'cluster_replica_verifier' }
$TestName = if ($ProviderPhase) { 'cluster_provider_real_independent_source_car' } else { 'cluster_replica_verifier_real_two_peer_lifecycle' }
$EnvironmentNames = @('RUSTUP_AUTO_INSTALL', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
    'IPFS_S3_REAL_CLUSTER', 'IPFS_S3_REAL_CLUSTER_FIXTURE', 'IPFS_S3_REAL_CLUSTER_REST_A',
    'IPFS_S3_REAL_CLUSTER_REST_B', 'IPFS_S3_REAL_CLUSTER_RPC_A', 'IPFS_S3_REAL_CLUSTER_RPC_B',
    'IPFS_S3_REAL_CLUSTER_PROVIDER', 'IPFS_S3_REAL_CLUSTER_SOURCE_RPC', 'IPFS_S3_REAL_CLUSTER_PROXY_A')

function Write-Log([string]$Text) {
    [IO.File]::AppendAllText($Diagnostic, "$Text`n")
    $Text
}

function Invoke-Docker([string[]]$Arguments) {
    $output = & docker @Arguments 2>&1
    $code = $LASTEXITCODE
    $text = ($output | ForEach-Object { $_.ToString() }) -join "`n"
    [IO.File]::AppendAllText($Diagnostic, "docker $($Arguments -join ' ') [exit=$code]`n$text`n")
    if ($code -ne 0) { throw "Docker command/inventory failed (exit=$code): $($Arguments -join ' ')" }
    return $text
}

function Get-Inventory([ValidateSet('container', 'volume', 'network')][string]$Kind, [string]$Name = '') {
    $arguments = @($Kind, 'ls')
    if ($Kind -eq 'container') { $arguments += '--all' }
    $filter = if ($Name.Length -eq 0) { "label=$Label" } else { "name=$Name" }
    $format = if ($Kind -eq 'container') { '{{.Names}}' } else { '{{.Name}}' }
    $text = Invoke-Docker ($arguments + @('--filter', $filter, '--format', $format))
    return @($text -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Assert-Owned([string]$Kind, [string]$Name) {
    $items = @(Invoke-Docker @($Kind, 'inspect', $Name) | ConvertFrom-Json)
    if ($items.Count -ne 1) { throw "Ambiguous inspect: $Kind $Name" }
    $item = $items[0]
    $labels = if ($Kind -eq 'container') { $item.Config.Labels } else { $item.Labels }
    if ($labels.$LabelKey -cne $RunId -or $item.Name.TrimStart('/') -cne $Name) { throw "Foreign resource: $Name" }
    return $item
}

function Set-Field($Object, [string]$Name, $Value) {
    # Only fixed v1.1.6 JSON fields below; handles officially optional fields.
    $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value -Force
}

function Replace-RelayLiteral([string]$Source, [string]$Old, [string]$Replacement, [int]$Expected = 1) {
    if ([regex]::Matches($Source, [regex]::Escape($Old)).Count -ne $Expected) {
        throw 'Read-only Stage5 relay template changed; refusing an unverified adaptation.'
    }
    return $Source.Replace($Old, $Replacement)
}

function Initialize-Relay {
    $sourcePath = Join-Path $PSScriptRoot 'support/ipfs_rpc_real_relay.cs'
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) { throw 'Missing read-only Stage5 raw TCP relay source.' }
    $source = [IO.File]::ReadAllText($sourcePath)
    $source = Replace-RelayLiteral $source 'IpfsRpcRealRelay' 'ClusterPinningRealRelay' 2
    $source = Replace-RelayLiteral $source '^ipfs-s3-rpc-[a-f0-9]{32}-(source|target)$' '^ipfs-s3-cluster-real-[a-f0-9]{32}-(source|kubo-a|kubo-b|cluster-a|cluster-b)$'
    $source = Replace-RelayLiteral $source 'string ownedNode, string logPath)' 'string ownedNode, string logPath, int targetPort)'
    $source = Replace-RelayLiteral $source 'private readonly string docker, node, log;' "private readonly string docker, node, log;`n    private readonly int port;"
    $assignment = @'
docker = dockerPath; node = ownedNode; log = logPath; port = targetPort;
        if (!((Regex.IsMatch(node, "-(source|kubo-[ab])$") && port == 5001) ||
              (Regex.IsMatch(node, "-cluster-[ab]$") && port == 9094) ||
              (Regex.IsMatch(node, "-cluster-a$") && port == 9095)))
            throw new ArgumentException("Not an exact fixture node/port pairing.");
'@
    $source = Replace-RelayLiteral $source 'docker = dockerPath; node = ownedNode; log = logPath;' $assignment
    $source = Replace-RelayLiteral $source ' + " destination=127.0.0.1:5001"' ' + " destination=127.0.0.1:" + port'
    $source = Replace-RelayLiteral $source 'node, "nc", "127.0.0.1", "5001"' 'node, "nc", "127.0.0.1", port.ToString(System.Globalization.CultureInfo.InvariantCulture)'
    # Capture only anonymous, isolated Cluster wire. Tee raw bytes unchanged;
    # never parse/translate DTOs or capture external/account credentials.
    $wireCopy = @'
    private async Task CopyWire(Stream input, Stream output, string path)
    {
        using (var evidence = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.Read))
        {
            byte[] buffer = new byte[16384];
            long total = 0;
            while (true)
            {
                int count = await input.ReadAsync(buffer.AsMemory(), stop.Token);
                if (count == 0) return;
                total += count;
                if (total > 32L * 1024 * 1024) throw new IOException("Owned wire capture exceeds budget.");
                await evidence.WriteAsync(buffer.AsMemory(0, count), stop.Token);
                await evidence.FlushAsync(stop.Token);
                await output.WriteAsync(buffer.AsMemory(0, count), stop.Token);
            }
        }
    }

    private async Task Forward(TcpClient client)
'@
    $source = Replace-RelayLiteral $source '    private async Task Forward(TcpClient client)' $wireCopy
    $wireUpload = @'
bool capture = Regex.IsMatch(node, "-cluster-[ab]$") && (port == 9094 || port == 9095);
                    string wire = log + "." + Guid.NewGuid().ToString("N");
                    if (capture) Write("exec_wire=" + id + " request=" + wire + ".request.wire response=" + wire + ".response.wire");
                    Task upload = capture ? CopyWire(socket, process.StandardInput.BaseStream, wire + ".request.wire") : socket.CopyToAsync(process.StandardInput.BaseStream, stop.Token);
'@
    $source = Replace-RelayLiteral $source 'Task upload = socket.CopyToAsync(process.StandardInput.BaseStream, stop.Token);' $wireUpload
    $source = Replace-RelayLiteral $source 'Task download = process.StandardOutput.BaseStream.CopyToAsync(socket, stop.Token);' 'Task download = capture ? CopyWire(process.StandardOutput.BaseStream, socket, wire + ".response.wire") : process.StandardOutput.BaseStream.CopyToAsync(socket, stop.Token);'
    if (-not (Test-Path -LiteralPath $ResultDir -PathType Container)) { throw 'Owned relay output parent vanished.' }
    $derivedPath = Join-Path $ResultDir 'cluster-pinning-real-relay.cs'
    $assemblyPath = Join-Path $ResultDir 'cluster-pinning-real-relay.dll'
    if ((Test-Path -LiteralPath $derivedPath) -or (Test-Path -LiteralPath $assemblyPath)) { throw 'Exact relay artifact collision.' }
    [IO.File]::WriteAllText($derivedPath, $source)
    Add-Type -Path $derivedPath -OutputAssembly $assemblyPath -ErrorAction Stop
    [void][Reflection.Assembly]::LoadFrom($assemblyPath)
    $Summary.relay_compile = 'PASS (read-only template, owned temp assembly)'
    Write-Log "relay compiled in owned directory=$ResultDir; shared source unchanged"
}

function Start-OwnedRelay([string]$Name, [int]$Port) {
    [void](Assert-Owned 'container' $Name)
    [void](Invoke-Docker @('exec', $Name, 'nc', '--help'))
    $relay = [ClusterPinningRealRelay]::new((Get-Command docker -ErrorAction Stop).Source, $Name, (Join-Path $ResultDir "$Name.relay.log"), $Port)
    $script:Relays += [pscustomobject]@{ Node = $Name; Port = $Port; Url = $relay.Url; Relay = $relay }
    return $relay.Url
}

function Read-IsolationConfig {
    $config = [IO.File]::ReadAllText($ConfigPath) | ConvertFrom-Json
    if ($config.version -cne '0.43.0' -or $config.daemon_mode -cne 'online-without-content-swarm') { throw 'Isolation version/mode mismatch.' }
    if ($ProviderPhase -and $config.source_daemon_mode -cne 'offline') { throw 'Provider source must be explicitly offline.' }
    if ($config.environment.Count -ne 1 -or $config.environment[0].name -cne 'IPFS_TELEMETRY' -or $config.environment[0].value -cne 'off') {
        throw 'Official telemetry-off environment is required before init and daemon.'
    }
    $required = [ordered]@{
        'Plugins.Plugins.telemetry.Disabled' = 'true'; 'Bootstrap' = '[]'; 'AutoConf.Enabled' = 'false'
        'Discovery.MDNS.Enabled' = 'false'; 'Routing.Type' = '"none"'; 'Routing.DelegatedRouters' = '[]'
        'DNS.Resolvers' = '{}'; 'Ipns.DelegatedPublishers' = '[]'; 'AutoTLS.Enabled' = 'false'
        'Swarm.RelayClient.Enabled' = 'false'; 'Swarm.Transports.Network.Relay' = 'false'
        'Swarm.DisableNatPortMap' = 'true'
        'Addresses.Swarm' = '[]'; 'Addresses.API' = '"/ip4/0.0.0.0/tcp/5001"'
    }
    if ($config.settings.Count -ne $required.Count) { throw 'Unexpected isolation settings.' }
    foreach ($entry in $required.GetEnumerator()) {
        $found = @($config.settings | Where-Object { $_.key -ceq $entry.Key })
        if ($found.Count -ne 1 -or (ConvertTo-Json -InputObject $found[0].value -Depth 20 -Compress) -cne $entry.Value) {
            throw "Required isolation value mismatch: $($entry.Key)"
        }
    }
    return $config
}

function New-Node([string]$Name, [string]$Image, [string]$DataPath, [bool]$Kubo) {
    $arguments = @('run', '--detach', '--pull=never', '--name', $Name, '--label', $Label,
        '--network', $NetworkName, '--mount', "type=volume,source=$Name-data,target=$DataPath",
        '--entrypoint', '/bin/sleep')
    if ($Kubo) { $arguments += @('--env', 'IPFS_PATH=/data/ipfs', '--env', 'IPFS_TELEMETRY=off') }
    else { $arguments += @('--env', 'IPFS_CLUSTER_PATH=/data/ipfs-cluster') }
    $imageId = if ($Kubo) { $Summary.kubo_image_id } else { $Summary.cluster_image_id }
    if ([string]::IsNullOrWhiteSpace($imageId) -or -not $imageId.StartsWith('sha256:')) { throw 'Cached immutable image ID is required.' }
    [void](Invoke-Docker ($arguments + @($imageId, 'infinity')))
    $node = Assert-Owned 'container' $Name
    if ($node.Image -cne $imageId) { throw "Node image changed from inspected cache identity: $Name ($Image)" }
    if (@($node.NetworkSettings.Networks.PSObject.Properties).Count -ne 1 -or $null -eq $node.NetworkSettings.Networks.$NetworkName) {
        throw 'Node attached to an unexpected network.'
    }
    foreach ($binding in $node.NetworkSettings.Ports.PSObject.Properties) {
        if ($null -ne $binding.Value -and @($binding.Value).Count -ne 0) { throw 'Internal node unexpectedly publishes a port.' }
    }
    if ($null -ne $node.HostConfig.PortBindings -and @($node.HostConfig.PortBindings.PSObject.Properties).Count -ne 0) {
        throw 'Published-port configuration is forbidden for this internal fixture.'
    }
}

function Start-Daemon([string]$Name, [string[]]$Arguments) {
    $process = Start-Process -FilePath (Get-Command docker -ErrorAction Stop).Source `
        -ArgumentList (@('exec', $Name) + $Arguments) `
        -RedirectStandardOutput (Join-Path $ResultDir "$Name.daemon.stdout.log") `
        -RedirectStandardError (Join-Path $ResultDir "$Name.daemon.stderr.log") -PassThru
    $script:DaemonProcesses += [pscustomobject]@{ Name = $Name; Process = $process }
}

function Start-Kubo([string]$Name, $Config, [bool]$Offline = $false) {
    [void](New-Node $Name $KuboImage '/data/ipfs' $true)
    [void](Invoke-Docker @('exec', $Name, 'ipfs', 'init'))
    foreach ($entry in $Config.settings) {
        $json = ConvertTo-Json -InputObject $entry.value -Depth 20 -Compress
        if ($entry.value -is [string]) {
            [void](Invoke-Docker @('exec', $Name, 'ipfs', 'config', $entry.key, $entry.value))
        } else {
            [void](Invoke-Docker @('exec', $Name, 'ipfs', 'config', '--json', $entry.key, $json))
        }
        $readback = Invoke-Docker @('exec', $Name, 'ipfs', 'config', $entry.key)
        $normalized = if ($entry.value -is [string] -and $readback.Trim() -ceq $entry.value) { $json }
            else { ConvertTo-Json -InputObject (ConvertFrom-Json -InputObject $readback -NoEnumerate) -Depth 20 -Compress }
        if ($normalized -cne $json) { throw "Kubo config readback differs: $($entry.key)" }
    }
    $arguments = @('ipfs', 'daemon')
    if ($Offline) { $arguments += '--offline' }
    Start-Daemon $Name $arguments
    return Start-OwnedRelay $Name 5001
}

function Start-Cluster([string]$Name, [string]$KuboName, [string]$Secret, [string]$Bootstrap = '') {
    [void](New-Node $Name $ClusterImage '/data/ipfs-cluster' $false)
    [void](Invoke-Docker @('exec', $Name, 'ipfs-cluster-service', 'init', '--consensus', 'crdt'))
    $configFile = Join-Path $ResultDir "$Name.service.json"
    [void](Invoke-Docker @('cp', "${Name}:/data/ipfs-cluster/service.json", $configFile))
    $config = [IO.File]::ReadAllText($configFile) | ConvertFrom-Json
    Set-Field $config.cluster 'secret' $Secret
    Set-Field $config.cluster 'peername' $Name
    Set-Field $config.cluster 'listen_multiaddress' @('/ip4/0.0.0.0/tcp/9096')
    Set-Field $config.cluster 'mdns_interval' '0s'
    Set-Field $config.cluster 'disable_repinning' $true
    Set-Field $config.cluster 'replication_factor_min' 2
    Set-Field $config.cluster 'replication_factor_max' 2
    Set-Field $config.cluster 'pin_recover_interval' '1h'
    Set-Field $config.cluster 'state_sync_interval' '2s'
    Set-Field $config.consensus.crdt 'cluster_name' $Prefix
    Set-Field $config.consensus.crdt 'trusted_peers' @('*')
    # User allocations remain priorities, not an allocator readiness bypass.
    # v1.1.6 balanced/config.go defines allocate_by; wait for both real metrics.
    Set-Field $config.allocator.balanced 'allocate_by' @('freespace')
    Set-Field $config.api.restapi 'http_listen_multiaddress' @('/ip4/0.0.0.0/tcp/9094')
    if ($ProviderPhase) {
        Set-Field $config.api.ipfsproxy 'listen_multiaddress' @('/ip4/127.0.0.1/tcp/9095')
        Set-Field $config.api.ipfsproxy 'node_multiaddress' "/dns4/$KuboName/tcp/5001"
        Set-Field $config.api.ipfsproxy 'log_file' '/data/ipfs-cluster/proxy-access.log'
    }
    Set-Field $config.ipfs_connector.ipfshttp 'node_multiaddress' "/dns4/$KuboName/tcp/5001"
    Set-Field $config.ipfs_connector.ipfshttp 'connect_swarms_delay' '1h'
    Set-Field $config.ipfs_connector.ipfshttp 'ipfs_request_timeout' '3s'
    Set-Field $config.ipfs_connector.ipfshttp 'pin_timeout' '3s'
    Set-Field $config.ipfs_connector.ipfshttp 'unpin_disable' $true
    [IO.File]::WriteAllText($configFile, ($config | ConvertTo-Json -Depth 50))
    [void](Invoke-Docker @('cp', $configFile, "${Name}:/data/ipfs-cluster/service.json"))
    $readbackFile = Join-Path $ResultDir "$Name.service.readback.json"
    [void](Invoke-Docker @('cp', "${Name}:/data/ipfs-cluster/service.json", $readbackFile))
    if ([IO.File]::ReadAllText($readbackFile) -cne [IO.File]::ReadAllText($configFile)) { throw 'Cluster config readback differs.' }
    $arguments = @('ipfs-cluster-service', 'daemon')
    if ($Bootstrap.Length -ne 0) { $arguments += @('--bootstrap', $Bootstrap) }
    Start-Daemon $Name $arguments
    return Start-OwnedRelay $Name 9094
}

function Wait-Identity([string]$Url, [bool]$Kubo) {
    $handler = [Net.Http.HttpClientHandler]::new()
    $handler.UseProxy = $false
    $handler.AllowAutoRedirect = $false
    $client = [Net.Http.HttpClient]::new($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(3)
    $client.MaxResponseContentBufferSize = 65536
    $deadline = [DateTime]::UtcNow.AddSeconds(60)
    try {
        while ([DateTime]::UtcNow -lt $deadline) {
            foreach ($daemon in $DaemonProcesses) {
                if ($daemon.Process.HasExited) { throw "Daemon exited before readiness: $($daemon.Name) exit=$($daemon.Process.ExitCode)" }
            }
            try {
                $task = if ($Kubo) { $client.PostAsync("$Url/api/v0/id", $null) } else { $client.GetAsync("$Url/id") }
                $response = $task.GetAwaiter().GetResult()
                try {
                    $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
                    [IO.File]::AppendAllText((Join-Path $ResultDir 'readiness.log'), "$Url status=$([int]$response.StatusCode) body=$($body.Substring(0, [Math]::Min(4096, $body.Length)))`n")
                    if ($response.IsSuccessStatusCode) {
                        $identity = $body | ConvertFrom-Json
                        if ($Kubo) {
                            if ([string]::IsNullOrWhiteSpace($identity.ID)) { throw 'Empty Kubo identity.' }
                        } else {
                            if ($identity.version -cnotmatch '^1\.1\.6(?:\+[A-Za-z0-9.-]+)?$') { throw 'Wrong Cluster version.' }
                            if ([string]::IsNullOrWhiteSpace($identity.id) -or $identity.error.Length -ne 0) { throw 'Invalid Cluster identity.' }
                        }
                        return $identity
                    }
                } finally { $response.Dispose() }
            } catch [Net.Http.HttpRequestException] {
                [IO.File]::AppendAllText((Join-Path $ResultDir 'readiness.log'), "$Url transport=$($_.Exception.GetType().Name)`n")
            } catch [Threading.Tasks.TaskCanceledException] {
                [IO.File]::AppendAllText((Join-Path $ResultDir 'readiness.log'), "$Url timeout=3s`n")
            }
            Start-Sleep -Milliseconds 250
        }
        throw 'Owned node readiness deadline exceeded.'
    } finally { $client.Dispose() }
}

function Remove-OwnedResources {
    $failed = $false
    # Collect ALL logs before removing ANY resource, including the stopped B.
    foreach ($name in $Containers) {
        if (-not $ResourcesAuthorized) { continue }
        try {
            if (@(Get-Inventory 'container') -ccontains $name) {
                [void](Assert-Owned 'container' $name)
                [IO.File]::WriteAllText((Join-Path $ResultDir "$name.container.log"), (Invoke-Docker @('logs', $name)))
                [IO.File]::WriteAllText((Join-Path $ResultDir "$name.inspect.json"), (Invoke-Docker @('container', 'inspect', $name)))
                if ($ProviderPhase -and $name -in @($ClusterA, $ClusterB)) {
                    # Cluster's own proxy access log proves /api/v0/add, not dag/import.
                    [void](Invoke-Docker @('cp', "${name}:/data/ipfs-cluster/proxy-access.log", (Join-Path $ResultDir "$name.proxy-access.log")))
                }
            }
        } catch { $failed = $true; Write-Log "log collection failure: $($_.Exception.Message)" }
    }
    $relayFailure = $false
    foreach ($entry in $Relays) {
        try { $entry.Relay.Dispose(); Write-Log "owned relay stopped: $($entry.Node):$($entry.Port) children=0" }
        catch { $failed = $true; $relayFailure = $true; Write-Log "owned relay cleanup failure: $($_.Exception.Message)" }
    }
    $Summary.relay_cleanup = if ($relayFailure) { 'FAIL' } else { 'PASS (owned listeners stopped, exec children=0)' }
    if ($failed) {
        # An unlogged node must not be deleted. Retain exact labeled resources
        # for diagnosis, but close only this runner's attached host loggers.
        foreach ($daemon in $DaemonProcesses) {
            if (-not $daemon.Process.HasExited) { $daemon.Process.Kill(); $daemon.Process.WaitForExit() }
            $daemon.Process.Dispose()
        }
        foreach ($kind in @('container', 'volume', 'network')) { [void](Get-Inventory $kind) }
        throw 'Logs/relay cleanup incomplete; refusing resource deletion. Owned inventory retained; acceptance FAIL.'
    }
    foreach ($kind in @('container', 'volume', 'network')) {
        $names = if (-not $ResourcesAuthorized) { @() } elseif ($kind -eq 'container') { $Containers }
            elseif ($kind -eq 'volume') { $Volumes } else { @($NetworkName) }
        foreach ($name in $names) {
            try {
                if (@(Get-Inventory $kind) -ccontains $name) {
                    [void](Assert-Owned $kind $name)
                    $arguments = @($kind, 'rm')
                    if ($kind -eq 'container') { $arguments += '--force' }
                    [void](Invoke-Docker ($arguments + @($name)))
                } elseif (@(Get-Inventory $kind $name) -ccontains $name) { throw "Foreign exact-name resource; refusing removal: $name" }
            } catch { $failed = $true; Write-Log "cleanup failure: $($_.Exception.Message)" }
        }
    }
    foreach ($daemon in $DaemonProcesses) {
        if (-not $daemon.Process.WaitForExit(10000)) {
            $failed = $true
            Write-Log "Attached logger did not exit: $($daemon.Name); terminating only this owned host process"
            $daemon.Process.Kill()
            $daemon.Process.WaitForExit()
            Write-Log "owned timed-out logger exit=$($daemon.Process.ExitCode)"
        }
        else {
            # docker exec is terminated by the deliberate B stop or owned rm.
            # Check/document this exit rather than pretending it is a daemon PASS.
            $code = $daemon.Process.ExitCode
            Write-Log "owned daemon logger $($daemon.Name) exit=$code after stop/removal"
            if ($code -notin @(0, 137, 143)) { $failed = $true }
        }
        $daemon.Process.Dispose()
    }
    foreach ($kind in @('container', 'volume', 'network')) {
        try {
            if (@(Get-Inventory $kind).Count -ne 0) { throw "Run-labeled $kind resources remain." }
            $names = if ($kind -eq 'container') { $Containers } elseif ($kind -eq 'volume') { $Volumes } else { @($NetworkName) }
            if ($ResourcesAuthorized) {
                foreach ($name in $names) {
                    if (@(Get-Inventory $kind $name) -ccontains $name) { throw "Exact resource still exists: $name" }
                }
            }
        } catch { $failed = $true; Write-Log "final inventory failure: $($_.Exception.Message)" }
    }
    if ($failed) { throw 'Log/cleanup/inventory evidence is incomplete; acceptance FAIL.' }
}

try {
    [IO.File]::WriteAllText((Join-Path $ResultDir 'qa-journal.txt'), @"
Entry: pwsh -NoProfile -File tests/run-cluster-pinning-real.ps1 -ProviderPhase:$ProviderPhase (one authorized phase attempt)
H1 fixture transport/config: readiness.log, per-node daemon/relay logs and exact owned inspect distinguish unavailable endpoints/config errors.
H2 asynchronous membership/allocation/pin convergence: bounded real /peers, freespace metrics, allocation/global snapshots distinguish pending vs actual 1/2 and 2/2.
H3 leaf interpretation/freshness defect: observations.jsonl compares raw allocation/global snapshots with provider observations and original/target hashverified CAR blocks and bytes.
Artifacts planned before launch: exact run=$RunId, exact nodes=$($Containers -join ','), their data volumes, $NetworkName internal network, own loopback relays/exec children, attached daemon loggers. Provider source is independent offline, never allocated A or B.
Derived relay source/assembly and logs are retained ONLY here: $ResultDir. Stage5 relay source is read-only. No extra repository journal or Git writes.
Cluster-only raw TCP request/response tee files are retained here, with exact relay/exec pairing and a 32 MiB per-direction connection budget. No response interpretation/translation. Anonymous fixture only, never account auth.
Cleanup: collect all node logs first, dispose only our relays/children, remove exact labeled owned nodes/volumes/network, require exit-0 empty inventories.
Stop rule: retain first unexpected FAIL; only a narrow evidenced fixture fix and one related new run are authorized. No unchanged old-matrix loops.
"@)
    $config = Read-IsolationConfig
    foreach ($name in $EnvironmentNames) {
        $SavedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    }
    $env:RUSTUP_AUTO_INSTALL = '0'
    foreach ($name in @('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY')) { [Environment]::SetEnvironmentVariable($name, [NullString]::Value, 'Process') }
    $env:NO_PROXY = '127.0.0.1,localhost'
    $manifestPath = Join-Path $RepoRoot 'Cargo.toml'
    $artifacts = @()
    & cargo test --locked --offline --manifest-path $manifestPath --test $Target --no-run --message-format=json 2>&1 | ForEach-Object {
        $line = $_.ToString()
        [IO.File]::AppendAllText((Join-Path $ResultDir 'compile.jsonl'), "$line`n")
        if ($line.StartsWith('{')) {
            $record = $line | ConvertFrom-Json
            if ($record.reason -eq 'compiler-artifact' -and $record.target.name -eq $Target -and $record.executable) { $artifacts += $record.executable }
        }
    }
    if ($LASTEXITCODE -ne 0 -or $artifacts.Count -ne 1) { $Summary.compile = 'FAIL'; throw 'Compilation failed; no retry and no nodes started.' }
    $Summary.compile = 'PASS'
    $listed = & $artifacts[0] --ignored --exact $TestName --list 2>&1
    if ($LASTEXITCODE -ne 0 -or @($listed | Where-Object { $_.ToString() -ceq "${TestName}: test" }).Count -ne 1) { throw 'Exact real test not registered; zero tests is not success.' }
    $defaultArguments = @('--nocapture', '--test-threads=1')
    if ($ProviderPhase) { $defaultArguments += @('--skip', 'cluster::') }
    $defaultLines = [Collections.Generic.List[string]]::new()
    & $artifacts[0] @defaultArguments 2>&1 | ForEach-Object {
        $defaultLines.Add($_.ToString())
        [IO.File]::AppendAllText((Join-Path $ResultDir 'default-tests.log'), "$($_.ToString())`n")
    }
    if ($LASTEXITCODE -ne 0 -or @($defaultLines | Where-Object { $_ -match '^test result: ok\. [1-9][0-9]* passed; 0 failed;' }).Count -ne 1) {
        $Summary.default_tests = 'FAIL'; throw 'Default bounded contracts failed or zero tests ran.'
    }
    $Summary.default_tests = 'PASS (real case ignored)'
    if ($ProviderPhase) {
        # Missing-env proof runs before image checks, relays and owned Docker/content writes.
        $required = @('IPFS_S3_REAL_CLUSTER_PROVIDER', 'IPFS_S3_REAL_CLUSTER', 'IPFS_S3_REAL_CLUSTER_FIXTURE',
            'IPFS_S3_REAL_CLUSTER_REST_A', 'IPFS_S3_REAL_CLUSTER_REST_B', 'IPFS_S3_REAL_CLUSTER_RPC_A',
            'IPFS_S3_REAL_CLUSTER_RPC_B', 'IPFS_S3_REAL_CLUSTER_SOURCE_RPC', 'IPFS_S3_REAL_CLUSTER_PROXY_A')
        foreach ($name in $required) { [Environment]::SetEnvironmentVariable($name, 'not-a-real-fixture', 'Process') }
        $env:IPFS_S3_REAL_CLUSTER_PROVIDER = '1'
        $env:IPFS_S3_REAL_CLUSTER = '1'
        $env:IPFS_S3_REAL_CLUSTER_FIXTURE = Join-Path $ResultDir 'not-created-gate-fixture.json'
        $port = 64001
        foreach ($name in $required[3..8]) { [Environment]::SetEnvironmentVariable($name, "http://127.0.0.1:$port", 'Process'); $port++ }
        foreach ($name in $required) {
            $current = [Environment]::GetEnvironmentVariable($name, 'Process')
            foreach ($case in @('missing', 'empty')) {
                $value = if ($case -eq 'missing') { [NullString]::Value } else { ' ' }
                [Environment]::SetEnvironmentVariable($name, $value, 'Process')
                $negative = & $artifacts[0] --ignored --exact $TestName --nocapture --test-threads=1 2>&1
                $negativeExit = $LASTEXITCODE
                [Environment]::SetEnvironmentVariable($name, $current, 'Process')
                $negativeText = ($negative | ForEach-Object { $_.ToString() }) -join "`n"
                [IO.File]::WriteAllText((Join-Path $ResultDir "$case-$name.log"), "exit=$negativeExit`n$negativeText`n")
                if ($negativeExit -eq 0 -or $negativeText -notmatch "(?:missing|empty) explicit fixture env: $name" -or $negativeText -notmatch 'test result: FAILED\. 0 passed; 1 failed;') {
                    $Summary.missing_endpoints = 'FAIL'; throw "Provider env did not fail closed ($case): $name"
                }
            }
        }
        $Summary.missing_endpoints = 'PASS (nine required envs each missing+empty fail before manifest/Docker/network/write; selector/origin/duplicate-port negatives in pure default contracts)'
    }
    Initialize-Relay
    $kuboInspect = @(Invoke-Docker @('image', 'inspect', $KuboImage) | ConvertFrom-Json)
    $clusterInspect = @(Invoke-Docker @('image', 'inspect', $ClusterImage) | ConvertFrom-Json)
    if ($kuboInspect.Count -ne 1 -or $clusterInspect.Count -ne 1 -or @($clusterInspect[0].RepoDigests | Where-Object { $_ -ceq "ipfs/ipfs-cluster@$ClusterDigest" }).Count -ne 1) {
        throw 'Cached image identity/digest mismatch; no pull permitted.'
    }
    $Summary.kubo_image_id = $kuboInspect[0].Id
    $Summary.cluster_image_id = $clusterInspect[0].Id
    foreach ($kind in @('container', 'volume', 'network')) {
        if (@(Get-Inventory $kind).Count -ne 0) { throw 'Run-label collision; refusing reuse.' }
        $names = if ($kind -eq 'container') { $Containers } elseif ($kind -eq 'volume') { $Volumes } else { @($NetworkName) }
        foreach ($name in $names) {
            if (@(Get-Inventory $kind $name) -ccontains $name) { throw "Exact-name collision; refusing reuse: $name" }
        }
    }
    $ResourcesAuthorized = $true
    [void](Invoke-Docker @('network', 'create', '--internal', '--label', $Label, $NetworkName))
    if ((Assert-Owned 'network' $NetworkName).Internal -ne $true) { throw 'Network is not internal.' }
    foreach ($volume in $Volumes) {
        [void](Invoke-Docker @('volume', 'create', '--label', $Label, $volume))
        [void](Assert-Owned 'volume' $volume)
    }
    $rpcA = Start-Kubo $KuboA $config
    $rpcB = Start-Kubo $KuboB $config
    $kuboAId = (Wait-Identity $rpcA $true).ID
    $kuboBId = (Wait-Identity $rpcB $true).ID
    if ($kuboAId -ceq $kuboBId) { throw 'Kubo identities are not independent.' }
    if ($ProviderPhase) {
        $sourceRpc = Start-Kubo $Source $config $true
        $sourceId = (Wait-Identity $sourceRpc $true).ID
        if ($sourceId -in @($kuboAId, $kuboBId)) { throw 'Source identity must be independent of both allocations.' }
        # Official offline Kubo rejects this online-only API. Capture that proof.
        [void](Invoke-Docker @('exec', $Source, 'ipfs', 'version'))
        [void](Invoke-Docker @('top', $Source, '-eo', 'pid,args'))
    }
    $secret = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
    $restA = Start-Cluster $ClusterA $KuboA $secret
    $clusterAId = (Wait-Identity $restA $false).id
    $restB = Start-Cluster $ClusterB $KuboB $secret "/dns4/$ClusterA/tcp/9096/p2p/$clusterAId"
    $clusterBId = (Wait-Identity $restB $false).id
    if ($clusterAId -ceq $clusterBId) { throw 'Cluster identities are not independent.' }
    if ($ProviderPhase) { $proxyA = Start-OwnedRelay $ClusterA 9095 }
    # Readiness is not convergence. The helper requires both real /peers views to
    # converge to this exact two-peer membership before making any content write.
    $fixturePath = Join-Path $ResultDir 'fixture.json'
    $fixture = [ordered]@{
        run_id = $RunId; network = $NetworkName; kubo_a = $KuboA; kubo_b = $KuboB
        cluster_a = $ClusterA; cluster_b = $ClusterB; kubo_a_id = $kuboAId; kubo_b_id = $kuboBId
        cluster_a_id = $clusterAId; cluster_b_id = $clusterBId; rest_a = $restA; rest_b = $restB; rpc_a = $rpcA; rpc_b = $rpcB
        host_forwarding = 'loopback-docker-exec-nc'
        kubo_image_id = $Summary.kubo_image_id; cluster_image_id = $Summary.cluster_image_id
        relays = @($Relays | ForEach-Object { [ordered]@{ node = $_.Node; port = $_.Port; url = $_.Url } })
    }
    if ($ProviderPhase) {
        $fixture.source = $Source; $fixture.source_id = $sourceId; $fixture.source_rpc = $sourceRpc
        $fixture.source_mode = 'offline'; $fixture.proxy_a = $proxyA
        $Summary.nodes = [ordered]@{ source = $sourceId; kubo_a = $kuboAId; kubo_b = $kuboBId; cluster_a = $clusterAId; cluster_b = $clusterBId }
    }
    [IO.File]::WriteAllText($fixturePath, ($fixture | ConvertTo-Json))
    $env:IPFS_S3_REAL_CLUSTER = '1'
    $env:IPFS_S3_REAL_CLUSTER_FIXTURE = $fixturePath
    $env:IPFS_S3_REAL_CLUSTER_REST_A = $restA
    $env:IPFS_S3_REAL_CLUSTER_REST_B = $restB
    $env:IPFS_S3_REAL_CLUSTER_RPC_A = $rpcA
    $env:IPFS_S3_REAL_CLUSTER_RPC_B = $rpcB
    if ($ProviderPhase) {
        $env:IPFS_S3_REAL_CLUSTER_PROVIDER = '1'
        $env:IPFS_S3_REAL_CLUSTER_SOURCE_RPC = $sourceRpc
        $env:IPFS_S3_REAL_CLUSTER_PROXY_A = $proxyA
    }
    # Every missing-origin case must fail in from_environment before any HTTP,
    # Docker inspection or content write. These are expected negative cases,
    # not extra acceptance attempts or retries of a failed fixture.
    if (-not $ProviderPhase) { foreach ($name in @('IPFS_S3_REAL_CLUSTER_REST_A', 'IPFS_S3_REAL_CLUSTER_REST_B', 'IPFS_S3_REAL_CLUSTER_RPC_A', 'IPFS_S3_REAL_CLUSTER_RPC_B')) {
        $current = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, [NullString]::Value, 'Process')
        $negative = & $artifacts[0] --ignored --exact $TestName --nocapture --test-threads=1 2>&1
        $negativeExit = $LASTEXITCODE
        [Environment]::SetEnvironmentVariable($name, $current, 'Process')
        $negativeText = ($negative | ForEach-Object { $_.ToString() }) -join "`n"
        [IO.File]::WriteAllText((Join-Path $ResultDir "missing-$name.log"), "exit=$negativeExit`n$negativeText`n")
        if ($negativeExit -eq 0 -or $negativeText -notmatch "(?:missing|empty) explicit fixture env: $name" -or $negativeText -notmatch 'test result: FAILED\. 0 passed; 1 failed;') {
            $Summary.missing_endpoints = 'FAIL'; throw "Missing-origin invocation did not fail closed: $name"
        }
    }
    $Summary.missing_endpoints = 'PASS (four missing origins each fail before network/write)' }
    $testLines = [Collections.Generic.List[string]]::new()
    & $artifacts[0] --ignored --exact $TestName --nocapture --test-threads=1 2>&1 | ForEach-Object {
        $line = $_.ToString()
        $testLines.Add($line)
        [IO.File]::AppendAllText((Join-Path $ResultDir 'acceptance.log'), "$line`n")
        $line
    }
    if ($LASTEXITCODE -ne 0 -or @($testLines | Where-Object { $_ -match '^test result: ok\. 1 passed; 0 failed;' }).Count -ne 1) {
        $Summary.test = 'FAIL'; throw 'Real acceptance did not prove exactly one passed case.'
    }
    foreach ($daemon in $DaemonProcesses) {
        $expectedStop = if ($ProviderPhase) { $Source } else { $KuboB }
        if ($daemon.Name -cne $expectedStop -and $daemon.Process.HasExited) { throw "Unexpected daemon exit=$($daemon.Process.ExitCode): $($daemon.Name)" }
    }
    $Summary.test = 'PASS'
} catch {
    $Summary.failure = $_.Exception.Message
    Write-Log "FAIL: $($Summary.failure)"
} finally {
    try { Remove-OwnedResources; $Summary.cleanup = 'PASS (exit-0 inventories empty)' }
    catch { $Summary.cleanup = 'FAIL'; $Summary.failure = "$($Summary.failure) $($_.Exception.Message)" }
    foreach ($entry in $SavedEnvironment.GetEnumerator()) {
        $value = if ($null -eq $entry.Value) { [NullString]::Value } else { $entry.Value }
        [Environment]::SetEnvironmentVariable($entry.Key, $value, 'Process')
    }
    if ($Summary.compile -eq 'PASS' -and $Summary.default_tests.StartsWith('PASS') -and $Summary.relay_compile.StartsWith('PASS') -and $Summary.missing_endpoints.StartsWith('PASS') -and $Summary.test -eq 'PASS' -and $Summary.cleanup.StartsWith('PASS')) { $Summary.status = 'PASS' }
    [IO.File]::WriteAllText((Join-Path $ResultDir 'summary.json'), ($Summary | ConvertTo-Json -Depth 10))
    Write-Log "status=$($Summary.status) cleanup=$($Summary.cleanup) evidence=$ResultDir"
}
if ($Summary.status -ne 'PASS') { exit 1 }
exit 0
