#requires -Version 7.0
[CmdletBinding()]
param(
    [ValidateSet('all', 'live', 'regression', 'remaining', 'quality', 'stress')][string]$Phase = 'all',
    [string]$ResumeFromTest = ''
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$PSNativeCommandUseErrorActionPreference = $false
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$RunId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ') + '-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$Project = 'ipfs3-f1-' + $RunId.ToLowerInvariant()
$Results = Join-Path $PSScriptRoot "results/lifecycle-transition/$RunId"
[void][IO.Directory]::CreateDirectory($Results)
$Compose = Join-Path $PSScriptRoot 'compose.lifecycle-transition-validation.yml'
$script:Secrets = [Collections.Generic.List[string]]::new()
$script:Children = [Collections.Generic.List[object]]::new()
$script:Saved = @{}
$summary = [ordered]@{ run_id=$RunId; phase=$Phase; status='FAIL'; started_utc=[DateTime]::UtcNow.ToString('o'); steps=@(); topology=@{}; cleanup=@(); remaining_gates=@() }
$owned = $false
$script:ResumePending = -not [string]::IsNullOrEmpty($ResumeFromTest)
. (Join-Path $PSScriptRoot 'support/lifecycle-transition-runner.ps1')
$inputManifest = $null

function Protect-Text([string]$Text) {
    $Text = [regex]::Replace($Text, '\x1b\[[0-9;]*m', '')
    if (Get-Command Protect-MachinePaths -CommandType Function -ErrorAction SilentlyContinue) {
        $Text = Protect-MachinePaths $Text $RepoRoot
    }
    foreach ($secret in $script:Secrets) { if ($secret) { $Text = $Text.Replace($secret, '[REDACTED]') } }
    $Text = [regex]::Replace($Text, '(?i)((?:AWS_SECRET_ACCESS_KEY|IPFS_S3_SECRET_ACCESS_KEY|POSTGRES_PASSWORD)=)[^\s"'']+', '$1[REDACTED]')
    $Text = [regex]::Replace($Text, '(?i)postgres(?:ql)?://[^\s"''<>]+', '[REDACTED_DATABASE_URL]')
    $Text = [regex]::Replace($Text, '(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\b[a-z_]+_[0-9a-f]{32}\b|\b[0-9a-f]{64}\b|\b[0-9a-f]{32}\b', '[REDACTED_INTERNAL_ID]')
    $Text = [regex]::Replace($Text, '(?i)(action_id|object_id|version_row_id|worker_id|schema_name)[=: ]+[^,\s}]+', '$1=[REDACTED_INTERNAL_ID]')
    return $Text
}
function Set-RunEnv([string]$Name, [string]$Value) {
    if (-not $script:Saved.ContainsKey($Name)) { $script:Saved[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process') }
    [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}
function Invoke-Step {
    param([string]$Name, [string]$File, [string[]]$Arguments, [switch]$Tests, [int]$ExpectedIgnored=0)
    $info = [Diagnostics.ProcessStartInfo]::new($File)
    $info.WorkingDirectory = $RepoRoot
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true; $info.RedirectStandardError = $true
    foreach ($arg in $Arguments) { [void]$info.ArgumentList.Add($arg) }
    $p = [Diagnostics.Process]::Start($info)
    $out = $p.StandardOutput.ReadToEndAsync(); $err = $p.StandardError.ReadToEndAsync()
    if (-not $p.WaitForExit(1200000)) { $p.Kill($true); $p.WaitForExit() }
    $text = $out.GetAwaiter().GetResult() + "`n" + $err.GetAwaiter().GetResult()
    $code = $p.ExitCode; $p.Dispose()
    $safe = Protect-Text $text
    [IO.File]::WriteAllText((Join-Path $Results "$Name.log"), $safe)
    $step = [ordered]@{ name=$Name; command=(Protect-Text ($File + ' ' + ($Arguments -join ' '))); exit_code=$code; status='FAIL'; counts=@() }
    $summary.steps += $step
    if ($code -ne 0) { throw "$Name exited $code; see $Name.log" }
    if ($Tests) {
        $matches = [regex]::Matches($safe, 'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out')
        if ($matches.Count -eq 0 -or $safe -match '(?i)\bskipping\b') { throw "$Name did not prove real execution" }
        foreach ($m in $matches) {
            $counts = @{passed=[int]$m.Groups[1].Value; failed=[int]$m.Groups[2].Value; ignored=[int]$m.Groups[3].Value; filtered=[int]$m.Groups[5].Value}
            $step.counts += $counts
            if ($counts.passed -le 0 -or $counts.failed -ne 0 -or $counts.ignored -ne $ExpectedIgnored) { throw "$Name unexpected test counts" }
        }
    }
    $step.status = 'PASS'
    Write-Host "$Name PASS"
    return $text
}
function Dc([string]$Name, [string[]]$Arguments) {
    Invoke-Step $Name docker (@('compose', '-f', $Compose, '-p', $Project) + $Arguments)
}
function Free-Port {
    $l = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0); $l.Start()
    $port = $l.LocalEndpoint.Port; $l.Stop(); return $port
}
function Wait-Http([string]$Url, [switch]$Post) {
    $deadline = [DateTime]::UtcNow.AddSeconds(90)
    do {
        try {
            $method = if ($Post) {'POST'} else {'GET'}
            return Invoke-RestMethod -Uri $Url -Method $method -UserAgent 'ipfs3-f1-validator' -TimeoutSec 3
        } catch { $lastError = $_.Exception.Message; Start-Sleep -Milliseconds 500 }
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Owned service did not become ready: $Url : $lastError"
}
function Start-Owned([string]$Name, [string]$File, [string[]]$Arguments) {
    $info = [Diagnostics.ProcessStartInfo]::new($File)
    $info.WorkingDirectory = $RepoRoot; $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true; $info.RedirectStandardError = $true
    foreach ($arg in $Arguments) { [void]$info.ArgumentList.Add($arg) }
    $p = [Diagnostics.Process]::Start($info)
    $script:Children.Add(@{name=$Name; process=$p; stdout=$p.StandardOutput.ReadToEndAsync(); stderr=$p.StandardError.ReadToEndAsync()})
}
function Cargo-Test([string]$Target, [string]$Filter='', [switch]$Ignored, [int]$ExpectedIgnored=0) {
    if ($script:ResumePending) {
        if ($Target -ceq $ResumeFromTest -or $Filter -ceq $ResumeFromTest) { $script:ResumePending = $false }
        else { $summary.steps += @{name=$Target; status='NOT RUN'; reason='explicit resume'; exit_code=$null}; return }
    }
    $arguments = @('test','--locked','--offline','--test',$Target)
    if ($Filter) { $arguments += $Filter }
    $arguments += @('--','--nocapture','--test-threads=1')
    if ($Ignored) { $arguments += '--ignored' }
    if ($Filter) { $arguments += '--exact' }
    $name = if ($Filter) {"$Target-$Filter"} else {$Target}
    $null = Invoke-Step $name cargo $arguments -Tests -ExpectedIgnored $ExpectedIgnored
    if ($Filter -eq 'real_lifecycle_transition_integrity_empty_and_multiblock_cross_range') {
        $peaks = @($script:Children | Where-Object { $_.name -in @('gateway-a','gateway-b') } | ForEach-Object { $_.process.Refresh(); $_.process.PeakWorkingSet64 })
        $budget = 192MB
        if ($peaks.Count -ne 2 -or @($peaks | Where-Object { $_ -ge $budget }).Count) { throw 'Gateway memory exceeded 192 MiB budget for 256 MiB fixture' }
        $car = Wait-Http "$script:CarProxyUrl/__car_proxy/metrics"
        if ($car.max_car_bytes -le $budget -or $car.parse_failures -ne 0 -or $car.in_flight -ne 0) { throw 'Actual completed CAR part did not exceed the gateway memory budget' }
        $summary.memory = @{ object_bytes=256MB; actual_car=$car; per_gateway_budget_bytes=$budget; gateway_peak_working_set_bytes=$peaks; measured='OS peak working set (Windows resident working set high-water mark), both production gateways; test client/proxies/Kubo excluded' }
    }
}

try {
    Set-RunEnv 'RUSTUP_AUTO_INSTALL' '0'; Set-RunEnv 'CARGO_NET_OFFLINE' 'true'
    $inputManifest = Get-F1InputManifest $RepoRoot
    [IO.File]::WriteAllText((Join-Path $Results 'inputs.manifest.json'), ($inputManifest | ConvertTo-Json -Depth 6))
    $summary.inputs = @{manifest='inputs.manifest.json'; unchanged=$false; runner='tests/run-lifecycle-transition-validation.ps1'; compose='tests/compose.lifecycle-transition-validation.yml'}
    if ($Phase -eq 'quality') {
        $null = Invoke-Step 'runner-contracts' pwsh @('-NoProfile','-File',(Join-Path $PSScriptRoot 'lifecycle-transition.Tests.ps1'))
        $null = Invoke-Step 'fmt' cargo @('fmt','--check')
        $null = Invoke-Step 'clippy' cargo @('clippy','--locked','--offline','--all-targets','--','-D','warnings')
        $null = Invoke-Step 'rust-1.92' cargo @('+1.92.0-x86_64-pc-windows-gnu','check','--locked','--offline','--all-targets')
        $null = Invoke-Step 'diff-check' git @('diff','--check')
        $summary.status = 'PARTIAL'
    } else {
    $null = Invoke-Step 'revision' git @('rev-parse','HEAD')
    $null = Invoke-Step 'dirty-paths' git @('status','--short')
    $null = Invoke-Step 'rust-version' rustc @('--version','--verbose')
    $null = Invoke-Step 'cargo-version' cargo @('--version')
    $null = Invoke-Step 's3s-version' cargo @('tree','--locked','--offline','-p','s3s','--depth','0')
    $null = Invoke-Step 'sdk-version' cargo @('tree','--locked','--offline','-p','rust-s3','--depth','0')
    $null = Invoke-Step 'docker-version' docker @('version','--format','{{.Server.Version}}')
    $null = Invoke-Step 'cached-images' docker @('image','inspect','postgres:17','ipfs/kubo:v0.43.0','amazon/aws-cli:latest','--format','{{.RepoTags}}')
    $null = Invoke-Step 'aws-version' docker @('run','--rm','--pull=never','amazon/aws-cli:latest','--version')
    $password = [guid]::NewGuid().ToString('N'); $script:Secrets.Add($password)
    Set-RunEnv 'IPFS_S3_F1_PASSWORD' $password
    $pg = Free-Port; $hot = Free-Port; $cold = Free-Port; $a = Free-Port; $b = Free-Port; $lb = Free-Port
    Set-RunEnv 'IPFS_S3_F1_POSTGRES_PORT' "$pg"; Set-RunEnv 'IPFS_S3_F1_HOT_PORT' "$hot"; Set-RunEnv 'IPFS_S3_F1_COLD_PORT' "$cold"
    $existing = docker ps -aq --filter "label=com.docker.compose.project=$Project"
    if ($LASTEXITCODE -ne 0 -or $existing) { throw 'Cannot establish exclusive compose ownership' }
    $existing = docker volume ls -q --filter "label=com.docker.compose.project=$Project"
    if ($LASTEXITCODE -ne 0 -or $existing) { throw 'Cannot establish exclusive volume ownership' }
    $existing = docker network ls -q --filter "label=com.docker.compose.project=$Project"
    if ($LASTEXITCODE -ne 0 -or $existing) { throw 'Cannot establish exclusive network ownership' }
    $owned = $true
    $null = Dc 'topology-up' @('up','-d','--pull','never','--wait','--wait-timeout','90')
    $hotUrl = "http://127.0.0.1:$hot"; $coldUrl = "http://127.0.0.1:$cold"
    $hotId = Wait-Http "$hotUrl/api/v0/id" -Post; $coldId = Wait-Http "$coldUrl/api/v0/id" -Post
    if ($hotId.ID -ceq $coldId.ID) { throw 'Kubo identities must be independent' }
    $hotContainer = (Dc 'hot-container' @('ps','-q','hot')).Trim()
    $coldContainer = (Dc 'cold-container' @('ps','-q','cold')).Trim()
    $hotVolume = (Invoke-Step 'hot-volume' docker @('inspect',$hotContainer,'--format','{{range .Mounts}}{{.Name}}{{end}}')).Trim()
    $coldVolume = (Invoke-Step 'cold-volume' docker @('inspect',$coldContainer,'--format','{{range .Mounts}}{{.Name}}{{end}}')).Trim()
    if (-not $hotVolume -or -not $coldVolume -or $hotVolume -ceq $coldVolume) { throw 'Kubo volumes must be independent' }
    $summary.topology = @{postgres_major=17; hot_node=$hotId.ID; cold_node=$coldId.ID; hot_volume=$hotVolume; cold_volume=$coldVolume; swarm='cold daemon --offline; hot --routing=none for deterministic local imports'; gateways='two native production OS processes'; load_balancer='test-only TCP round robin'}
    $null = Dc 'postgres-version' @('exec','-T','postgres','psql','-U','ipfs3','-d','ipfs3','-Atc','SHOW server_version')
    $null = Dc 'kubo-version' @('exec','-T','hot','ipfs','version')
    $dbUrl = "postgres://ipfs3:${password}@127.0.0.1:$pg/ipfs3"; $script:Secrets.Add($dbUrl)
    Cargo-Test 'lifecycle_transition_car_proxy' -ExpectedIgnored 1
    $proxyBuild = Invoke-Step 'build-car-proxy' cargo @('test','--locked','--offline','--test','lifecycle_transition_car_proxy','--no-run','--message-format=json')
    $proxyArtifacts = @($proxyBuild -split "`n" | Where-Object { $_.StartsWith('{') } | ForEach-Object { ConvertFrom-Json $_ } | Where-Object { $_.reason -eq 'compiler-artifact' -and $_.target.name -eq 'lifecycle_transition_car_proxy' -and $_.executable })
    if ($proxyArtifacts.Count -ne 1) { throw 'Missing CAR proxy artifact' }
    $proxyPort = Free-Port; $script:CarProxyUrl = "http://127.0.0.1:$proxyPort"
    Set-RunEnv 'IPFS_S3_TRANSITION_CAR_PROXY_BIND' "127.0.0.1:$proxyPort"
    Set-RunEnv 'IPFS_S3_TRANSITION_CAR_PROXY_UPSTREAM_URL' $coldUrl
    Set-RunEnv 'IPFS_S3_TRANSITION_CAR_PROXY_URL' $script:CarProxyUrl
    $proxyImage = Join-Path $Results 'car-proxy-fixture.exe'
    [IO.File]::Copy($proxyArtifacts[0].executable, $proxyImage)
    Start-Owned 'car-proxy' $proxyImage @('--ignored','--exact','lifecycle_transition_car_proxy','--nocapture')
    $null = Wait-Http "$script:CarProxyUrl/__car_proxy/health"
    Set-RunEnv 'IPFS_S3_TEST_POSTGRES_URL' $dbUrl
    Set-RunEnv 'IPFS_S3_TRANSITION_HOT_URL' $hotUrl; Set-RunEnv 'IPFS_S3_TRANSITION_COLD_URL' $coldUrl
    Set-RunEnv 'IPFS_S3_TRANSITION_A_ENDPOINT' "http://127.0.0.1:$a"; Set-RunEnv 'IPFS_S3_TRANSITION_B_ENDPOINT' "http://127.0.0.1:$b"
    Set-RunEnv 'IPFS_S3_TRANSITION_FIXTURE_ID' $RunId.ToLowerInvariant()
    Set-RunEnv 'IPFS_S3_TRANSITION_LARGE_BYTES' '268435456'
    $null = Invoke-Step 'build-gateway' cargo @('build','--locked','--offline','--bin','ipfs-s3-gateway')
    # Windows locks a running image; Cargo test must be free to relink its bin target.
    $gatewayImage = Join-Path $Results 'gateway-fixture.exe'
    [IO.File]::Copy((Join-Path $RepoRoot 'target/debug/ipfs-s3-gateway.exe'), $gatewayImage)
    Set-RunEnv 'IPFS_S3_DATABASE_URL' $dbUrl; Set-RunEnv 'IPFS_S3_KUBO_RPC_URL' $hotUrl; Set-RunEnv 'IPFS_S3_COLD_KUBO_RPC_URL' $script:CarProxyUrl
    Set-RunEnv 'IPFS_S3_CONFIG' (Join-Path $Results 'absent-config.toml')
    Set-RunEnv 'IPFS_S3_ACCESS_KEY_ID' 'test'; Set-RunEnv 'IPFS_S3_SECRET_ACCESS_KEY' 'test'
    $masterKey = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'; $script:Secrets.Add($masterKey)
    Set-RunEnv 'IPFS_S3_MASTER_KEY' $masterKey; Set-RunEnv 'RUST_LOG' 'warn'
    Set-RunEnv 'IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS' '100'; Set-RunEnv 'IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS' '3'; Set-RunEnv 'IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS' '3'
    Set-RunEnv 'IPFS_S3_BIND' "0.0.0.0:$a"
    Start-Owned 'gateway-a' $gatewayImage @()
    $null = Wait-Http "http://127.0.0.1:$a/health"
    Set-RunEnv 'IPFS_S3_BIND' "0.0.0.0:$b"
    Start-Owned 'gateway-b' $gatewayImage @()
    $null = Wait-Http "http://127.0.0.1:$b/health"
    $null = Invoke-Step 'aws-signed-list-buckets' docker @('run','--rm','--pull=never','-e','AWS_ACCESS_KEY_ID=test','-e','AWS_SECRET_ACCESS_KEY=test','-e','AWS_DEFAULT_REGION=us-east-1','amazon/aws-cli:latest','--endpoint-url',"http://host.docker.internal:$a",'s3api','list-buckets')
    if ($Phase -in @('all','live')) {
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_current_plain_encrypted_copy_and_reporting_matrix' -Ignored
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_noncurrent_public_and_null_version_matrix' -Ignored
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_backend_stop_1_prepare_fixture' -Ignored
        $null = Dc 'stop-hot' @('stop','hot')
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_backend_stop_2_hot_down_cold_read_succeeds' -Ignored
        $null = Dc 'start-hot' @('start','hot'); $null = Wait-Http "$hotUrl/api/v0/id" -Post
        $null = Dc 'stop-cold' @('stop','cold')
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_backend_stop_3_cold_down_fails_without_hot_fallback' -Ignored
        $null = Dc 'start-cold' @('start','cold'); $null = Wait-Http "$coldUrl/api/v0/id" -Post
        Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_backend_stop_4_cleanup_fixture' -Ignored
    }
    if ($Phase -in @('all','live','remaining')) {
        foreach ($name in @('real_lifecycle_transition_integrity_nondefault_raw_leaf_import_to_ia','real_lifecycle_transition_integrity_empty_and_multiblock_cross_range','real_lifecycle_transition_integrity_multipart_root_to_ia')) { Cargo-Test 'lifecycle_transition' $name -Ignored }
        Cargo-Test 'lifecycle_transition_process' 'lifecycle_transition_process_crash_restart_f1' -Ignored
    }
    if ($Phase -eq 'stress') { Cargo-Test 'lifecycle_transition' 'real_lifecycle_transition_integrity_empty_and_multiblock_cross_range' -Ignored }
    if ($Phase -in @('all','regression','remaining')) {
        Cargo-Test 'lifecycle_transition' -ExpectedIgnored 9
        Cargo-Test 'lifecycle_transition_process' -ExpectedIgnored 1
        foreach ($target in @('postgres_residency','postgres_residency_concurrency','postgres_lifecycle_transition','postgres_transition_saga')) { Cargo-Test $target -Ignored }
        foreach ($target in @('postgres_lifecycle','postgres_versioning','postgres_cors')) { Cargo-Test $target }
        Cargo-Test 'residency_publication' 'postgres_reuses_publication_and_delete_residency_scenarios' -Ignored
        if (-not $script:ResumePending) { $null = Invoke-Step 'lib' cargo @('test','--locked','--offline','--lib') -Tests }
        foreach ($target in @('integration','residency','lifecycle_transition_schema','cors')) { Cargo-Test $target }
        Cargo-Test 'residency_publication' -ExpectedIgnored 1
        $build = Invoke-Step 'build-balancer' cargo @('test','--locked','--offline','--test','lifecycle_transition_balancer','--no-run','--message-format=json')
        $artifacts = @($build -split "`n" | Where-Object { $_.StartsWith('{') } | ForEach-Object { ConvertFrom-Json $_ } | Where-Object { $_.reason -eq 'compiler-artifact' -and $_.target.name -eq 'lifecycle_transition_balancer' -and $_.executable })
        if ($artifacts.Count -ne 1) { throw 'Missing balancer artifact' }
        Set-RunEnv 'IPFS_S3_F1_BALANCER_BIND' "127.0.0.1:$lb"; Set-RunEnv 'IPFS_S3_F1_GATEWAY_A_BIND' "127.0.0.1:$a"; Set-RunEnv 'IPFS_S3_F1_GATEWAY_B_BIND' "127.0.0.1:$b"
        Start-Owned 'balancer' $artifacts[0].executable @('--ignored','--exact','serve_f1_balancer','--nocapture')
        $null = Wait-Http "http://127.0.0.1:$lb/health"
        Set-RunEnv 'IPFS_S3_MULTI_GATEWAY_A_ENDPOINT' "http://127.0.0.1:$a"; Set-RunEnv 'IPFS_S3_MULTI_GATEWAY_B_ENDPOINT' "http://127.0.0.1:$b"
        Set-RunEnv 'IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT' "http://127.0.0.1:$lb"; Set-RunEnv 'IPFS_S3_MULTI_GATEWAY_KUBO_URL' $hotUrl; Set-RunEnv 'IPFS_S3_MULTI_GATEWAY_DATABASE_URL' $dbUrl
        Cargo-Test 'multi_gateway'
        $null = Invoke-Step 'runner-contracts' pwsh @('-NoProfile','-File',(Join-Path $PSScriptRoot 'lifecycle-transition.Tests.ps1'))
        $null = Invoke-Step 'fmt' cargo @('fmt','--check')
        $null = Invoke-Step 'clippy' cargo @('clippy','--locked','--offline','--all-targets','--','-D','warnings')
        $null = Invoke-Step 'rust-1.92' cargo @('+1.92.0-x86_64-pc-windows-gnu','check','--locked','--offline','--all-targets')
        $null = Invoke-Step 'diff-check' git @('diff','--check')
    }
    # This runner never promotes partial evidence into the full design gate.
    if ($script:ResumePending) { throw 'Resume test was not reached' }
    $summary.status = if ($Phase -eq 'all' -and -not $ResumeFromTest) {'PASS'} else {'PARTIAL'}
    }
} catch {
    $summary.status = 'FAIL'; $summary.error = Protect-Text $_.Exception.Message
    Write-Host $summary.error
} finally {
    # Read all owned process logs before removal; no pre-existing PID is ever targeted.
    foreach ($child in $script:Children) {
        try {
            if (-not $child.process.HasExited) { $child.process.Kill($true); $child.process.WaitForExit() }
            $text = $child.stdout.GetAwaiter().GetResult() + "`n" + $child.stderr.GetAwaiter().GetResult()
            [IO.File]::WriteAllText((Join-Path $Results ($child.name + '.log')), (Protect-Text $text))
            $summary.cleanup += "$($child.name): logs captured, process tree stopped"
            $child.process.Dispose()
        } catch { $summary.status='FAIL'; $summary.cleanup += "$($child.name): cleanup FAILED" }
    }
    $ownedImage = Join-Path $Results 'gateway-fixture.exe'
    try { if ([IO.File]::Exists($ownedImage)) { [IO.File]::Delete($ownedImage) } }
    catch { $summary.status='FAIL'; $summary.cleanup += 'owned executable cleanup FAILED' }
    $ownedProxyImage = Join-Path $Results 'car-proxy-fixture.exe'
    try { if ([IO.File]::Exists($ownedProxyImage)) { [IO.File]::Delete($ownedProxyImage) } }
    catch { $summary.status='FAIL'; $summary.cleanup += 'owned proxy executable cleanup FAILED' }
    if ($owned) {
        try { $null = Dc 'topology-logs' @('logs','--no-color') } catch { $summary.status='FAIL'; $summary.cleanup += 'container logs FAILED' }
        try {
            $null = Dc 'topology-down' @('down','--volumes','--remove-orphans','--timeout','10')
            $inventories = @(Get-OwnedCleanupInventory $Project)
            foreach ($inventory in $inventories) { $inventory.output = Protect-Text $inventory.output }
            Assert-OwnedCleanupInventory $summary $inventories
            $summary.cleanup += 'compose-owned containers/networks/volumes removed; label-scoped inventory empty'
        } catch { $summary.status='FAIL'; $summary.cleanup += 'compose cleanup FAILED' }
    }
    foreach ($name in $script:Saved.Keys) { [Environment]::SetEnvironmentVariable($name, $script:Saved[$name], 'Process') }
    if ($null -ne $inputManifest) {
        try {
            Assert-F1InputsUnchanged $inputManifest (Get-F1InputManifest $RepoRoot)
            $summary.inputs.unchanged = $true
        } catch { $summary.status='FAIL'; $summary.error = Protect-Text $_.Exception.Message }
    }
    $requiredSteps = @('postgres_residency','postgres_residency_concurrency','postgres_lifecycle_transition','postgres_transition_saga','postgres_lifecycle','postgres_versioning','postgres_cors','residency_publication-postgres_reuses_publication_and_delete_residency_scenarios','lib','integration','residency','lifecycle_transition_schema','cors','residency_publication','multi_gateway','runner-contracts','fmt','clippy','rust-1.92','diff-check','lifecycle_transition_process-lifecycle_transition_process_crash_restart_f1')
    $requiredSteps += @('lifecycle_transition','lifecycle_transition_process','lifecycle_transition_car_proxy')
    $suite = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'lifecycle_transition.rs'))
    foreach ($match in [regex]::Matches($suite, 'async fn (real_lifecycle_transition_\w+)\(')) { $requiredSteps += 'lifecycle_transition-' + $match.Groups[1].Value }
    $passedSteps = @($summary.steps | Where-Object { $_.status -eq 'PASS' } | ForEach-Object { $_.name })
    $summary.remaining_gates = @($requiredSteps | Where-Object { $_ -notin $passedSteps })
    if ($summary.status -eq 'PASS' -and $summary.remaining_gates.Count) { $summary.status = 'FAIL'; $summary.error = 'Full gate has unexecuted required steps' }
    $summary.finished_utc = [DateTime]::UtcNow.ToString('o')
    [IO.File]::WriteAllText((Join-Path $Results 'summary.json'), ($summary | ConvertTo-Json -Depth 9))
    Write-Host "Result $($summary.status): $Results"
}
if ($summary.status -eq 'PASS') { exit 0 }
if ($summary.status -eq 'PARTIAL') { exit 2 }
exit 1
