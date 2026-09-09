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

# Object-versioning client evidence is intentionally static here: this test
# must stay Docker-free even though the runner it contracts can opt into live work.
$VersioningRunnerPath = Join-Path $RepoRoot "scripts/object-versioning-smoke.ps1"
$VersioningComposePath = Join-Path $RepoRoot "tests/compose.object-versioning-validation.yml"
$VersioningEvidencePath = Join-Path $RepoRoot "docs/object-versioning-evidence-2026-08-25.log"
$ReadmePath = Join-Path $RepoRoot "README.md"
$RoadmapPath = Join-Path $RepoRoot "ROADMAP.md"
$CargoManifestPath = Join-Path $RepoRoot "Cargo.toml"
$missingVersioningContracts = @(
    @($VersioningRunnerPath, $VersioningComposePath, $VersioningEvidencePath) | Where-Object {
        -not (Test-Path -LiteralPath $_ -PathType Leaf)
    }
)
if ($missingVersioningContracts.Count -ne 0) {
    throw "Object-versioning static contracts are missing: $($missingVersioningContracts -join '; ')"
}

$versioningTokens = $null
$versioningParseErrors = $null
$VersioningRunnerAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $VersioningRunnerPath,
    [ref]$versioningTokens,
    [ref]$versioningParseErrors
)
if ($versioningParseErrors.Count -ne 0) {
    $versioningParseErrors | Format-List | Out-String | Write-Host
    throw "scripts/object-versioning-smoke.ps1 has parse errors"
}
$VersioningRunnerSource = [IO.File]::ReadAllText($VersioningRunnerPath)
$VersioningComposeSource = [IO.File]::ReadAllText($VersioningComposePath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$VersioningEvidenceSource = [IO.File]::ReadAllText($VersioningEvidencePath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$VersioningEvidenceBytes = [IO.File]::ReadAllBytes($VersioningEvidencePath)
$VersioningEvidenceRaw = [Text.UTF8Encoding]::new($false, $true).GetString($VersioningEvidenceBytes)
$ReadmeSource = [IO.File]::ReadAllText($ReadmePath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$RoadmapSource = [IO.File]::ReadAllText($RoadmapPath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$CargoManifestSource = [IO.File]::ReadAllText($CargoManifestPath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")

function Get-VersioningRunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)
    $matches = @($VersioningRunnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one object-versioning function named $Name, found $($matches.Count)" }
    return $matches[0].Extent.Text
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

# Lifecycle-expiration evidence is intentionally a Docker-free source/AST contract.
# It is added before the artifacts so its first execution is a causal RED.
$LifecycleRunnerPath = Join-Path $RepoRoot "scripts/lifecycle-expiration-smoke.ps1"
$LifecycleComposePath = Join-Path $RepoRoot "tests/compose.lifecycle-expiration-validation.yml"
$LifecycleEvidencePath = Join-Path $RepoRoot "docs/lifecycle-expiration-evidence-2026-08-26.log"
$missingLifecycleContracts = @(
    @($LifecycleRunnerPath, $LifecycleComposePath, $LifecycleEvidencePath) | Where-Object {
        -not (Test-Path -LiteralPath $_ -PathType Leaf)
    }
)
if ($missingLifecycleContracts.Count -ne 0) {
    throw "Lifecycle-expiration static contracts are missing: $($missingLifecycleContracts -join '; ')"
}

$lifecycleTokens = $null
$lifecycleParseErrors = $null
$LifecycleRunnerAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $LifecycleRunnerPath,
    [ref]$lifecycleTokens,
    [ref]$lifecycleParseErrors
)
if ($lifecycleParseErrors.Count -ne 0) {
    $lifecycleParseErrors | Format-List | Out-String | Write-Host
    throw "scripts/lifecycle-expiration-smoke.ps1 has parse errors"
}
$LifecycleRunnerSource = [IO.File]::ReadAllText($LifecycleRunnerPath)
$LifecycleComposeSource = [IO.File]::ReadAllText($LifecycleComposePath).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$LifecycleEvidenceBytes = [IO.File]::ReadAllBytes($LifecycleEvidencePath)
$LifecycleEvidenceRaw = [Text.UTF8Encoding]::new($false, $true).GetString($LifecycleEvidenceBytes)

function Get-LifecycleRunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)
    $matches = @($LifecycleRunnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one lifecycle-expiration function named $Name, found $($matches.Count)" }
    return $matches[0].Extent.Text
}

Assert-Contains $LifecycleRunnerSource '[switch]$Run' "Lifecycle runner must expose the opt-in Run switch"
Assert-Contains $LifecycleRunnerSource '"[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"' "Lifecycle runner must retain the exact no-run receipt"

# Task 12 correction RED: optional ListObjectVersions properties are genuinely
# absent when empty, and pin_jobs cannot prove a Kubo request boundary.
$lifecycleCorrectionRedFailures = [Collections.Generic.List[string]]::new()
foreach ($directProperty in @('$versions.Versions', '$versions.DeleteMarkers')) {
    if ($LifecycleRunnerSource.Contains($directProperty, [StringComparison]::Ordinal)) {
        $lifecycleCorrectionRedFailures.Add("direct optional ListObjectVersions property remains: $directProperty")
    }
}
foreach ($weakCondition in @('Count -le 1', 'Assert-NoPinRm', 'zero-pin-rm', "pin_jobs WHERE operation = 'pin_rm'")) {
    if ($LifecycleRunnerSource.Contains($weakCondition, [StringComparison]::Ordinal)) {
        $lifecycleCorrectionRedFailures.Add("weak lifecycle evidence remains: $weakCondition")
    }
}
foreach ($requiredCorrectionFunction in @(
    'Get-AwsOptionalListEntries',
    'Get-AwsRequiredProperty',
    'Get-LifecycleVersionList',
    'Assert-LifecycleCurrentDaysState',
    'Assert-LifecycleNoncurrentState',
    'Assert-LifecycleConfigurationShape'
)) {
    if (-not $LifecycleRunnerSource.Contains("function $requiredCorrectionFunction", [StringComparison]::Ordinal)) {
        $lifecycleCorrectionRedFailures.Add("required lifecycle assertion helper is absent: $requiredCorrectionFunction")
    }
}
if ($lifecycleCorrectionRedFailures.Count -ne 0) {
    throw "Lifecycle Task 12 correction RED: $($lifecycleCorrectionRedFailures -join '; ')"
}

foreach ($name in @(
    "Write-LifecycleEvidence",
    "New-LifecycleRunId",
    "New-LifecycleProjectName",
    "New-LifecycleBucketName",
    "Assert-CanonicalChildPath",
    "New-LifecycleRunRoot",
    "New-LifecycleOwnershipReceipt",
    "Invoke-NativeCommand",
    "Test-LocalImage",
    "Assert-ComposeVersion",
    "Assert-ProjectResourcesAbsent",
    "Assert-LoopbackPortsFree",
    "Save-EnvironmentState",
    "Restore-EnvironmentState",
    "Invoke-OfflineGatewayBuild",
    "Wait-TopologyHealthy",
    "Get-ComposeNetwork",
    "Assert-RustSuiteExecuted",
    "Invoke-LifecycleRustSuites",
    "Invoke-LifecycleAwsEvidence",
    "Get-AwsOptionalListEntries",
    "Get-AwsRequiredProperty",
    "Get-LifecycleVersionList",
    "Assert-LifecycleCurrentDaysState",
    "Assert-LifecycleNoncurrentState",
    "Assert-LifecycleConfigurationShape",
    "Test-LifecycleCleanupResiduals",
    "Remove-OwnedLifecycleResources",
    "Invoke-LifecycleMain"
)) {
    $null = Get-LifecycleRunnerFunctionSource $name
}

$lifecycleRunIdSource = Get-LifecycleRunnerFunctionSource "New-LifecycleRunId"
Assert-Contains $lifecycleRunIdSource "^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$" "Lifecycle RunId grammar changed"
$lifecycleProjectSource = Get-LifecycleRunnerFunctionSource "New-LifecycleProjectName"
$lifecycleBucketSource = Get-LifecycleRunnerFunctionSource "New-LifecycleBucketName"
Assert-Contains $lifecycleProjectSource '"ipfs3-lifecycle-$RunId"' "Lifecycle project grammar is missing"
Assert-Contains $lifecycleBucketSource '[string]$Prefix = "ipfs3-lifecycle"' "Lifecycle bucket grammar is missing"
Assert-Contains $lifecycleBucketSource '"$Prefix-$RunId"' "Lifecycle bucket identity must derive from its generated run identity"
Assert-Contains $lifecycleProjectSource "'^[a-z0-9][a-z0-9_-]*$'" "Lifecycle project grammar must be anchored"
Assert-Contains $lifecycleBucketSource "'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$'" "Lifecycle bucket grammar must be anchored"
$lifecycleRootSource = Get-LifecycleRunnerFunctionSource "New-LifecycleRunRoot"
foreach ($fragment in @('"ipfs-s3-lifecycle-expiration-$RunId"', 'RunRoot must be a direct child')) {
    Assert-Contains $lifecycleRootSource $fragment "Lifecycle root direct-child guard is missing: $fragment"
}
$lifecycleReceiptSource = Get-LifecycleRunnerFunctionSource "New-LifecycleOwnershipReceipt"
foreach ($fragment in @('[IO.FileMode]::CreateNew', 'ownership-receipt')) {
    Assert-Contains $lifecycleReceiptSource $fragment "Lifecycle receipt ownership guard is missing: $fragment"
}

$lifecyclePreflightSource = Get-LifecycleRunnerFunctionSource "Assert-ProjectResourcesAbsent"
foreach ($fragment in @(
    '"ps", "-aq"',
    '"network", "ls", "-q"',
    '"volume", "ls", "-q"',
    '"label=com.docker.compose.project=$Project"',
    'BLOCKED'
)) {
    Assert-Contains $lifecyclePreflightSource $fragment "Lifecycle project-label preflight is incomplete: $fragment"
}
$lifecycleImageSource = Get-LifecycleRunnerFunctionSource "Test-LocalImage"
Assert-Contains $lifecycleImageSource '"image", "inspect", $Image, "--format", "{{.Id}}"' "Lifecycle local image inspection is not exact"
foreach ($image in @("postgres:17", "ghcr.io/hugefiver/ipfs3-kubo:latest", "ghcr.io/hugefiver/ipfs3:latest", "rust:latest", "amazon/aws-cli:latest", "nginx:1.28.0-alpine")) {
    Assert-Contains $LifecycleRunnerSource $image "Lifecycle exact local image prerequisite is missing: $image"
}
foreach ($forbidden in @("docker pull", "Install-Module", "choco install", "winget install", "scoop install", "Invoke-WebRequest", "--pull=always")) {
    Assert-NotContains $LifecycleRunnerSource $forbidden "Lifecycle runner may not install or pull: $forbidden"
}

$lifecycleComposeVersionSource = Get-LifecycleRunnerFunctionSource "Assert-ComposeVersion"
foreach ($fragment in @(
    '"compose", "version", "--short"',
    "'^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$'",
    '[Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion)',
    '[Version]"2.23.1"'
)) {
    Assert-Contains $lifecycleComposeVersionSource $fragment "Lifecycle strict Compose version preflight is missing: $fragment"
}
Assert-NotContains $lifecycleComposeVersionSource '"compose", "version", "--format", "{{.Version}}"' "Lifecycle Compose version preflight must use --short"

$lifecyclePortsSource = Get-LifecycleRunnerFunctionSource "Assert-LoopbackPortsFree"
foreach ($port in @("55437", "55004", "59004", "59005", "59006")) {
    Assert-Contains ($lifecyclePortsSource + $LifecycleRunnerSource) $port "Lifecycle loopback port is missing: $port"
}
Assert-Contains $lifecyclePortsSource "TcpListener" "Lifecycle port preflight must bind-probe loopback"
Assert-Contains $lifecyclePortsSource "IPAddress]::Loopback" "Lifecycle port preflight must use loopback"

$lifecycleOfflineSource = Get-LifecycleRunnerFunctionSource "Invoke-OfflineGatewayBuild"
foreach ($fragment in @(
    '"vendor", "--locked", "--offline"',
    '"build", "--pull=false", "--network", "none", "--quiet"',
    '"--build-context", "vendor-archive=$archiveContext"'
)) {
    Assert-Contains $lifecycleOfflineSource $fragment "Lifecycle offline build contract is missing: $fragment"
}
$lifecycleMainSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleMain"
foreach ($fragment in @(
    '"config", "--quiet"',
    '"up", "--detach", "--pull", "never", "--no-build"',
    '"down", "--volumes", "--remove-orphans"',
    'cargo test --test postgres_versioning -- --nocapture --test-threads=1',
    'cargo test --test postgres_lifecycle -- --nocapture --test-threads=1',
    'cargo test --test e2e -- --nocapture --test-threads=1',
    'cargo test --test multi_gateway -- --nocapture --test-threads=1'
)) {
    Assert-Contains $LifecycleRunnerSource $fragment "Lifecycle execution contract is missing: $fragment"
}
foreach ($environmentName in @(
    "COMPOSE_DISABLE_ENV_FILE",
    "IPFS_S3_TEST_POSTGRES_URL",
    "IPFS_S3_E2E_ENDPOINT",
    "IPFS_S3_E2E_KUBO_URL",
    "IPFS_S3_MULTI_GATEWAY_A_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_B_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT",
    "IPFS_S3_MULTI_GATEWAY_KUBO_URL"
)) {
    Assert-Contains $LifecycleRunnerSource ('"' + $environmentName + '"') "Lifecycle runner must own and restore $environmentName"
}

$lifecycleEvidenceFunctionSource = Get-LifecycleRunnerFunctionSource "Write-LifecycleEvidence"
Assert-Contains $lifecycleEvidenceFunctionSource "'^[A-Za-z0-9._:=/ -]+$'" "Lifecycle evidence must have a safe fixed charset"
$lifecycleStageSource = Get-LifecycleRunnerFunctionSource "Set-LifecycleStage"
foreach ($stage in @("preflight", "config", "offline-build", "compose-up", "health", "network", "postgres", "e2e", "multi-gateway", "integration", "aws", "cleanup")) {
    Assert-Contains $lifecycleStageSource ('"' + $stage + '"') "Lifecycle stage allowlist is missing: $stage"
}

$optionalEntriesSource = Get-LifecycleRunnerFunctionSource "Get-AwsOptionalListEntries"
$requiredPropertySource = Get-LifecycleRunnerFunctionSource "Get-AwsRequiredProperty"
foreach ($fragment in @(
    '[Parameter(Mandatory)][object]$Document',
    '[ValidateSet("Versions", "DeleteMarkers", "Rules")][string]$PropertyName',
    '$Document -isnot [pscustomobject]',
    '$Document.PSObject.Properties.Match($PropertyName)',
    '$properties.Count -eq 0',
    '$properties.Count -ne 1',
    '$entries.Count -gt 1000',
    '$entry -isnot [pscustomobject]'
)) {
    Assert-Contains $optionalEntriesSource $fragment "Lifecycle optional-list parser is incomplete: $fragment"
}
foreach ($fragment in @(
    '$Document -isnot [pscustomobject]',
    '$Document.PSObject.Properties.Match($PropertyName)',
    '$properties.Count -ne 1'
)) {
    Assert-Contains $requiredPropertySource $fragment "Lifecycle required-property parser is incomplete: $fragment"
}
foreach ($forbidden in @('$versions.Versions', '$versions.DeleteMarkers', 'Count -le 1', 'Assert-NoPinRm', 'zero-pin-rm', "pin_jobs WHERE operation = 'pin_rm'")) {
    Assert-NotContains $LifecycleRunnerSource $forbidden "Lifecycle runner retains unsafe or false evidence: $forbidden"
}

$versionListSource = Get-LifecycleRunnerFunctionSource "Get-LifecycleVersionList"
foreach ($fragment in @(
    'Invoke-AwsJson',
    'Get-AwsOptionalListEntries -Document $document -PropertyName "Versions"',
    'Get-AwsOptionalListEntries -Document $document -PropertyName "DeleteMarkers"'
)) {
    Assert-Contains $versionListSource $fragment "Lifecycle version-list parser does not use the safe optional helper: $fragment"
}
$currentDaysSource = Get-LifecycleRunnerFunctionSource "Assert-LifecycleCurrentDaysState"
foreach ($fragment in @(
    '[ValidateSet("unversioned", "enabled", "suspended")][string]$Versioning',
    '$list.Versions.Count -ne 0 -or $list.DeleteMarkers.Count -ne 0',
    '$list.Versions.Count -ne 1 -or $list.DeleteMarkers.Count -ne 1',
    '$list.Versions.Count -ne 0 -or $list.DeleteMarkers.Count -ne 1',
    'Get-AwsRequiredProperty -Document $list.Versions[0] -PropertyName "IsLatest"',
    'Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "IsLatest"',
    'Get-AwsRequiredProperty -Document $list.DeleteMarkers[0] -PropertyName "VersionId"',
    '-cne "null"',
    '-ceq "null"'
)) {
    Assert-Contains $currentDaysSource $fragment "Lifecycle current-Days state assertion is incomplete: $fragment"
}
$noncurrentSource = Get-LifecycleRunnerFunctionSource "Assert-LifecycleNoncurrentState"
foreach ($fragment in @(
    '$list.Versions.Count -ne 1',
    '$list.DeleteMarkers.Count -ne 0',
    'Get-AwsRequiredProperty -Document $list.Versions[0] -PropertyName "IsLatest"',
    'Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $Endpoint -Arguments @("s3api", "head-object", "--bucket", $Bucket, "--key", $Key)'
)) {
    Assert-Contains $noncurrentSource $fragment "Lifecycle NVE final-state assertion is incomplete: $fragment"
}
$controlPlaneSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleControlPlaneEvidence"
Assert-Contains $controlPlaneSource 'Assert-LifecycleConfigurationShape -Document $initialConfiguration -ExpectedKind "current-days"' "Lifecycle initial GET must assert its canonical shape"
Assert-Contains $controlPlaneSource 'Assert-LifecycleConfigurationShape -Document $replacementConfiguration -ExpectedKind "noncurrent"' "Lifecycle replacement GET must assert its canonical shape"
$configurationShapeSource = Get-LifecycleRunnerFunctionSource "Assert-LifecycleConfigurationShape"
foreach ($fragment in @(
    'Get-AwsOptionalListEntries -Document $Document -PropertyName "Rules"',
    '$rules.Count -ne 1',
    'Get-AwsRequiredProperty -Document $rule -PropertyName "Status"',
    'Get-AwsRequiredProperty -Document $rule -PropertyName "Expiration"',
    'Get-AwsRequiredProperty -Document $rule -PropertyName "NoncurrentVersionExpiration"'
)) {
    Assert-Contains $configurationShapeSource $fragment "Lifecycle GET shape assertion is incomplete: $fragment"
}
foreach ($semanticSource in @($optionalEntriesSource, $requiredPropertySource, $versionListSource, $currentDaysSource, $noncurrentSource, $configurationShapeSource)) {
    Assert-NotContains $semanticSource 'Write-LifecycleEvidence' "Lifecycle semantic helpers must not emit raw response content"
    Assert-NotContains $semanticSource 'Write-Host' "Lifecycle semantic helpers must not emit raw response content"
}

$rustSuiteSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleRustSuites"
$rustSuiteAssertionSource = Get-LifecycleRunnerFunctionSource "Assert-RustSuiteExecuted"
Invoke-Expression $rustSuiteAssertionSource
function Test-LifecycleRustReceiptFixture {
    param([Parameter(Mandatory)][AllowEmptyString()][string[]]$Lines)
    $result = [pscustomobject]@{ StdOut = @($Lines); StdErr = @() }
    try {
        Assert-RustSuiteExecuted -Result $result -Name "static fixture"
        return $true
    } catch {
        return $false
    }
}
$singularRustReceipt = @(
    "running 1 test",
    "",
    "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
)
$pluralRustReceipt = @(
    "running 16 tests",
    "",
    "test result: ok. 16 passed; 0 failed; 2 ignored; 0 measured; 3 filtered out; finished in 12.34s"
)
Assert-True (Test-LifecycleRustReceiptFixture -Lines $singularRustReceipt) "Lifecycle Rust receipt must accept one complete executed test"
Assert-True (Test-LifecycleRustReceiptFixture -Lines $pluralRustReceipt) "Lifecycle Rust receipt must accept plural complete executed tests"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok."))) "Lifecycle Rust receipt must reject a bare result marker"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 2 tests", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject mismatched running and passed counts"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 0 tests", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject zero running tests"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject zero passed tests"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject failed tests"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok. 1 passed; 0 failed; -1 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject negative summary counters"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3601.00s"))) "Lifecycle Rust receipt must reject unbounded duration"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "running 1 test", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject duplicate running lines"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("running 1 test", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s"))) "Lifecycle Rust receipt must reject duplicate summary lines"
Assert-True (-not (Test-LifecycleRustReceiptFixture -Lines @("prefix running 1 test", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"))) "Lifecycle Rust receipt must reject malformed substring output"
foreach ($fragment in @(
    "'(?m)^running (?<running>[1-9][0-9]*) tests?$'",
    "'(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$'",
    '$runningMatches.Count -ne 1',
    '$summaryMatches.Count -ne 1',
    '$runningCount -ne $passedCount',
    '$seconds -gt [decimal]3600'
)) {
    Assert-Contains $rustSuiteAssertionSource $fragment "Lifecycle Rust receipt grammar is missing: $fragment"
}
$expectedLifecycleRustCommands = @(
    'cargo test --test postgres_versioning -- --nocapture --test-threads=1',
    'cargo test --test postgres_lifecycle -- --nocapture --test-threads=1',
    'cargo test --test e2e -- --nocapture --test-threads=1',
    'cargo test --test multi_gateway -- --nocapture --test-threads=1',
    'cargo test --test integration lifecycle_expiration_invariants -- --nocapture --test-threads=1'
)
$actualLifecycleRustCommands = @([regex]::Matches($rustSuiteSource, '(?m)^\s*Write-LifecycleEvidence -Category "command" -Value "(?<command>cargo test [^"]+)"\s*$') | ForEach-Object { $_.Groups['command'].Value })
Assert-True (($actualLifecycleRustCommands -join "`n") -ceq ($expectedLifecycleRustCommands -join "`n")) "Lifecycle Rust suite commands must be exactly the four plan binaries followed by the signed pin-rm invariant"
Assert-Contains $rustSuiteSource 'Write-LifecycleEvidence -Category "assertion" -Value "pin-rm-request=zero-signed-integration"' "Lifecycle pin-rm receipt must be tied to the signed integration invariant"

$pinRmMatches = @(& rg -n --glob "*.rs" "pin_rm\(" src)
if ($LASTEXITCODE -gt 1) { throw "Lifecycle production pin-rm scan failed" }
Assert-True ($pinRmMatches.Count -eq 4) "Unexpected pin_rm call-site count in src: $($pinRmMatches -join '; ')"
Assert-True (@($pinRmMatches | Where-Object { $_ -match '^src[\\/]kubo[\\/]pin\.rs:[0-9]+:' }).Count -eq 3) "pin_rm must remain limited to its Kubo definition and unit tests"
Assert-True (@($pinRmMatches | Where-Object { $_ -match '^src[\\/]s3[\\/]ops[\\/]object\.rs:[0-9]+:\s*async fn delete_never_calls_pin_rm\(\)' }).Count -eq 1) "The only non-Kubo pin_rm match must be the existing no-call test name"

$lifecycleCleanupSource = Get-LifecycleRunnerFunctionSource "Remove-OwnedLifecycleResources"
$lifecycleResidualSource = Get-LifecycleRunnerFunctionSource "Test-LifecycleCleanupResiduals"
foreach ($fragment in @(
    '"down", "--volumes", "--remove-orphans"',
    '"image", "rm", $State.GatewayImage',
    'Test-LifecycleCleanupResiduals -State $State',
    'Remove-OwnedLifecycleRunRoot'
)) {
    Assert-Contains $lifecycleCleanupSource $fragment "Lifecycle exact cleanup contract is missing: $fragment"
}
Assert-Contains $LifecycleRunnerSource 'Restore-EnvironmentState -State $state.EnvironmentState' "Lifecycle cleanup must exactly restore its owned environment"
foreach ($residual in @("containers", "networks", "volumes")) {
    Assert-Contains $lifecycleResidualSource ('Name = "' + $residual + '"') "Lifecycle cleanup must independently query $residual"
}
Assert-Contains $lifecycleResidualSource '"residual-$($query.Name)=zero"' "Lifecycle cleanup must prove each queried project residual is zero"
Assert-Contains $lifecycleResidualSource '"residual-image=zero"' "Lifecycle cleanup must prove image residual is zero"
foreach ($forbidden in @('"image", "rm", "*"', '"image", "rm", "-f"', 'system prune', 'compose", "down", "-v"')) {
    Assert-NotContains $LifecycleRunnerSource $forbidden "Lifecycle cleanup may not be broad: $forbidden"
}

foreach ($fragment in @(
    'services:',
    '  postgres:',
    '  kubo:',
    '  gateway-a:',
    '  gateway-b:',
    '  load-balancer:',
    'image: postgres:17',
    'image: ghcr.io/hugefiver/ipfs3-kubo:latest',
    'image: "${IPFS_S3_LIFECYCLE_IMAGE:?required}"',
    'image: nginx:1.28.0-alpine',
    '../deploy/nginx/multi-gateway.conf:/etc/nginx/nginx.conf:ro',
    'IPFS_S3_LIFECYCLE_POLL_INTERVAL_MS: "100"',
    'IPFS_S3_LIFECYCLE_SCAN_LEASE_SECS: "3"',
    'IPFS_S3_LIFECYCLE_ACTION_LEASE_SECS: "3"',
    'IPFS_S3_MASTER_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"',
    'postgres_data:',
    'kubo_data:'
)) {
    Assert-Contains $LifecycleComposeSource $fragment "Lifecycle Compose topology is missing: $fragment"
}
Assert-True (([regex]::Matches($LifecycleComposeSource, '(?m)^  (?:postgres|kubo|gateway-a|gateway-b|load-balancer):$')).Count -eq 5) "Lifecycle Compose must contain exactly five services"
Assert-True (([regex]::Matches($LifecycleComposeSource, '(?m)^\s*- "127\.0\.0\.1:\$\{IPFS_S3_LIFECYCLE_[A-Z_]+_PORT:\?required\}:(?:5432|5001|9000)"$')).Count -eq 5) "Lifecycle Compose must contain exactly five required loopback ports"
Assert-NotContains $LifecycleComposeSource "container_name:" "Lifecycle Compose must not set container_name"

$expectedLifecycleEvidenceLines = @(
    'LIFECYCLE EXPIRATION REAL CLIENT: PASSED',
    'Completed: 2026-08-31',
    'Expiration spec SHA-256: 0c11f4df0b4f6e81e9834fde45368df5b330e1229dc2cdff4e8d8ab573f35742',
    'Lifecycle program spec SHA-256: fdcfbb22447ea7c7bfae9c549b2722664a2e8df859076e7fd3c2d20b3b0e4574',
    'Package: 0.1.0',
    'Candidate base HEAD: f42515a',
    'Focused lifecycle actions regression: PASSED 14/14',
    'Lifecycle admission priority regressions: PASSED 2/2',
    'Complete nonlive matrix: PASSED 880 library / 143 integration',
    'AWS-only lifecycle diagnostic: PASSED',
    'Final normal lifecycle validation: PASSED',
    'postgres_versioning: PASSED',
    'postgres_lifecycle hard-loss and reclaim: PASSED',
    'e2e: PASSED',
    'multi_gateway: PASSED',
    'signed integration lifecycle_expiration_invariants: PASSED',
    'AWS lifecycle control plane: PASSED',
    'AWS current expiration unversioned enabled suspended: PASSED',
    'AWS noncurrent content and marker expiration: PASSED',
    'AWS timed sole-marker and EODM cleanup: PASSED',
    'Kubo /api/v0/pin/rm requests: ZERO',
    'Owned cleanup and independent residual checks: PASSED',
    'HOSTED lifecycle-expiration: NOT RUN'
)
$expectedLifecycleEvidence = ($expectedLifecycleEvidenceLines -join "`n") + "`n"
Assert-True ($LifecycleEvidenceRaw -ceq $expectedLifecycleEvidence) "Lifecycle evidence must be the exact initial sanitized NOT RUN receipt"
Assert-True ($LifecycleEvidenceBytes.Count -lt 3 -or -not ($LifecycleEvidenceBytes[0] -eq 0xef -and $LifecycleEvidenceBytes[1] -eq 0xbb -and $LifecycleEvidenceBytes[2] -eq 0xbf)) "Lifecycle evidence must be UTF-8 without a BOM"
Assert-True (-not $LifecycleEvidenceRaw.Contains("`r", [StringComparison]::Ordinal)) "Lifecycle evidence must use portable LF line endings"
Assert-True ([regex]::IsMatch($LifecycleEvidenceRaw, '\A[\x20-\x7E\n]*\z')) "Lifecycle evidence must contain only portable sanitized text"
Assert-Contains $LifecycleEvidenceRaw "LIFECYCLE EXPIRATION REAL CLIENT: PASSED" "Lifecycle evidence must claim the accepted LOCAL result"
Assert-NotContains $LifecycleRunnerSource "README.md" "Task 12 runner must not promote README without live PASS"
Assert-NotContains $LifecycleRunnerSource "ROADMAP.md" "Task 12 runner must not promote ROADMAP without live PASS"

# Task 13 diagnostic surface is a Docker-free parser and topology contract.
# This block intentionally precedes runner implementation so its first execution
# records the causal RED for the missing diagnostic switch and receipt parser.
$multiGatewayDiagnosticFailures = [Collections.Generic.List[string]]::new()
function Test-MultiGatewayDiagnosticContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $multiGatewayDiagnosticFailures.Add($Message) }
}

$lifecycleParameterNames = @($LifecycleRunnerAst.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
Test-MultiGatewayDiagnosticContract (
    ($lifecycleParameterNames -join ",") -ceq "Run,DiagnoseMultiGateway,DiagnoseLifecycleRaceExact,DiagnoseLifecycleRaceStability,DiagnoseLifecycleAws" -and
    $LifecycleRunnerSource.Contains('[switch]$Run', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('[switch]$DiagnoseMultiGateway', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('[switch]$DiagnoseLifecycleRaceExact', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('[switch]$DiagnoseLifecycleRaceStability', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('[switch]$DiagnoseLifecycleAws', [StringComparison]::Ordinal)
) "Lifecycle runner must expose exactly five diagnostic/run switches"
Test-MultiGatewayDiagnosticContract (
    $LifecycleRunnerSource.Contains('$selectedModeCount = @(', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('$DiagnoseLifecycleRaceExact.IsPresent', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('$DiagnoseLifecycleRaceStability.IsPresent', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('$DiagnoseLifecycleAws.IsPresent', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('if ($selectedModeCount -gt 1)', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('throw "Lifecycle runner modes are mutually exclusive"', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('if ($selectedModeCount -eq 0)', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('"[RESULT] lifecycle-expiration=NOT RUN reason=execution-not-requested"', [StringComparison]::Ordinal)
) "Lifecycle modes must be mutually exclusive while retaining the exact no-run receipt"

$multiGatewayParserMatches = @($LifecycleRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Get-MultiGatewayFailureReceipt"
}, $true))
$multiGatewayDiagnosticMatches = @($LifecycleRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Invoke-LifecycleMultiGatewayDiagnostic"
}, $true))
$exactRaceParserMatches = @($LifecycleRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Get-LifecycleRaceExactReceipt"
}, $true))
$exactRaceDiagnosticMatches = @($LifecycleRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Invoke-LifecycleRaceExactDiagnostic"
}, $true))
$stabilityDiagnosticMatches = @($LifecycleRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Invoke-LifecycleRaceStabilityDiagnostic"
}, $true))
$multiGatewayParserSource = if ($multiGatewayParserMatches.Count -eq 1) { $multiGatewayParserMatches[0].Extent.Text } else { "" }
$multiGatewayDiagnosticSource = if ($multiGatewayDiagnosticMatches.Count -eq 1) { $multiGatewayDiagnosticMatches[0].Extent.Text } else { "" }
Test-MultiGatewayDiagnosticContract ($multiGatewayParserMatches.Count -eq 1) "Get-MultiGatewayFailureReceipt must exist exactly once"
Test-MultiGatewayDiagnosticContract ($multiGatewayDiagnosticMatches.Count -eq 1) "Invoke-LifecycleMultiGatewayDiagnostic must exist exactly once"
Test-MultiGatewayDiagnosticContract ($exactRaceParserMatches.Count -eq 1) "Get-LifecycleRaceExactReceipt must exist exactly once"
Test-MultiGatewayDiagnosticContract ($exactRaceDiagnosticMatches.Count -eq 1) "Invoke-LifecycleRaceExactDiagnostic must exist exactly once"
Test-MultiGatewayDiagnosticContract ($stabilityDiagnosticMatches.Count -eq 1) "Invoke-LifecycleRaceStabilityDiagnostic must exist exactly once"

if ($multiGatewayParserMatches.Count -eq 1) {
    $multiGatewayParserParameterNames = @($multiGatewayParserMatches[0].Body.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
    Test-MultiGatewayDiagnosticContract (
        ($multiGatewayParserParameterNames -join ",") -ceq "Result" -and
        $multiGatewayParserSource.Contains('[Parameter(Mandatory)][object]$Result', [StringComparison]::Ordinal)
    ) "Multi-gateway receipt parser must take exactly one mandatory Result"
    foreach ($fragment in @(
        "'(?m)^running (?<running>[1-9][0-9]*) tests?$'",
        "'(?m)^test result: FAILED\. (?<passed>[0-9]+) passed; (?<failed>[1-9][0-9]*) failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$'",
        "'(?m)^test (?<name>[A-Za-z0-9_:]+) \.\.\. FAILED$'",
        '$runningMatches.Count -ne 1',
        '$summaryMatches.Count -ne 1',
        '$running -ne ($passed + $failed + $ignored + $measured)',
        '$names.Count -ne $failed',
        '@($names | Sort-Object -Unique).Count -ne $names.Count',
        '$seconds -gt 3600',
        "timeout = '(?i)\b(timed out|deadline has elapsed)\b'",
        "assertion = '(?i)\b(assertion|panicked at)\b'",
        "'http-status' = '(?i)\b(http|status code)\b'",
        "connection = '(?i)\b(connection|connect|refused)\b'",
        "database = '(?i)\b(database|postgres|sqlx)\b'",
        "'process-exit'",
        "'(?m)^\[LIFECYCLE-RACE-STAGE\].*\r?$'",
        "'(?m)^\[LIFECYCLE-RACE-STAGE\] test=multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome stage=(?<stage>bucket-created|versioning-enabled|lifecycle-configured|predecessor-created|race-started|successor-request-dispatched|successor-request-complete|observer-loop-entered|observer-get-response|observer-list-response|observer-list-status-ok|observer-successor-visible|successor-response|successor-observed|lifecycle-config-deleted|terminal-wait-entered|terminal-state-evaluation|successor-read|version-cleanup|bucket-delete)\r?$'",
        '$stagePrefixMatches.Count -ne $stageMatches.Count',
        '$names -contains $raceTestName -and $lastStage -ceq "not-reached"',
        'LastStage = $lastStage'
    )) {
        Test-MultiGatewayDiagnosticContract $multiGatewayParserSource.Contains($fragment, [StringComparison]::Ordinal) "Multi-gateway receipt parser grammar is incomplete: $fragment"
    }
    Test-MultiGatewayDiagnosticContract (
        -not $multiGatewayParserSource.Contains('Write-LifecycleEvidence', [StringComparison]::Ordinal) -and
        -not [regex]::IsMatch($multiGatewayParserSource, '(?m)^\s*(?:Write-Host|Write-Output|Write-Error)\b')
    ) "Multi-gateway receipt parser must not emit raw process output"
}

if ($multiGatewayDiagnosticMatches.Count -eq 1) {
    $diagnosticNativeCalls = @($multiGatewayDiagnosticMatches[0].FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.CommandAst] -and
            $node.GetCommandName() -eq "Invoke-NativeCommand"
    }, $true))
    Test-MultiGatewayDiagnosticContract (
        $diagnosticNativeCalls.Count -eq 1 -and
        $multiGatewayDiagnosticSource.Contains('Set-LifecycleEndpointEnvironment', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('Set-LifecycleStage -State $State -Stage "multi-gateway"', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('-FilePath "cargo"', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('-ArgumentList @("test", "--test", "multi_gateway", "--", "--nocapture", "--test-threads=1")', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('-AllowedExitCodes @(0, 101)', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('-WorkingDirectory $RepoRoot', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('Assert-RustSuiteExecuted -Result $result -Name "Owned multi-gateway diagnostic"', [StringComparison]::Ordinal) -and
        $multiGatewayDiagnosticSource.Contains('cargo test --test multi_gateway -- --nocapture --test-threads=1', [StringComparison]::Ordinal)
    ) "Multi-gateway diagnostic must run exactly the owned cargo test command with exits 0 and 101"
    Test-MultiGatewayDiagnosticContract (
        -not $multiGatewayDiagnosticSource.Contains('Invoke-LifecycleRustSuites', [StringComparison]::Ordinal) -and
        -not $multiGatewayDiagnosticSource.Contains('Invoke-LifecycleAwsEvidence', [StringComparison]::Ordinal) -and
        -not $multiGatewayDiagnosticSource.Contains('StdOut', [StringComparison]::Ordinal) -and
        -not $multiGatewayDiagnosticSource.Contains('StdErr', [StringComparison]::Ordinal) -and
        -not [regex]::IsMatch($multiGatewayDiagnosticSource, '(?m)^\s*(?:Write-Host|Write-Output|Write-Error)\b')
    ) "Multi-gateway diagnostic must not run normal suites/AWS or emit raw process output"
    foreach ($fragment in @(
        '$State.DiagnosticOutcome = "not-reproduced"',
        '"multi-gateway=not-reproduced"',
        'Get-MultiGatewayFailureReceipt -Result $result',
        '"multi-gateway-running=$($receipt.Running)"',
        '"multi-gateway-passed=$($receipt.Passed)"',
        '"multi-gateway-failed=$($receipt.Failed)"',
        '"multi-gateway-failed-test=$name"',
        '"multi-gateway-first-error-category=$($receipt.FirstErrorCategory)"',
        '"multi-gateway-last-stage=$($receipt.LastStage)"',
        '$State.DiagnosticOutcome = "captured"',
        'throw "Owned multi-gateway diagnostic captured a safe failure receipt"'
    )) {
        Test-MultiGatewayDiagnosticContract $multiGatewayDiagnosticSource.Contains($fragment, [StringComparison]::Ordinal) "Multi-gateway diagnostic safe receipt is incomplete: $fragment"
    }
}

$lifecycleMainSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleMain"
$metadataIndex = $lifecycleMainSource.IndexOf('Set-LifecycleStage -State $State -Stage "metadata"', [StringComparison]::Ordinal)
$awsBranchIndex = $lifecycleMainSource.IndexOf('if ($DiagnoseLifecycleAws)', [StringComparison]::Ordinal)
$stabilityBranchIndex = $lifecycleMainSource.IndexOf('elseif ($DiagnoseLifecycleRaceStability)', [StringComparison]::Ordinal)
$exactBranchIndex = $lifecycleMainSource.IndexOf('elseif ($DiagnoseLifecycleRaceExact)', [StringComparison]::Ordinal)
$diagnosticBranchIndex = $lifecycleMainSource.IndexOf('elseif ($DiagnoseMultiGateway)', [StringComparison]::Ordinal)
$exactCallIndex = $lifecycleMainSource.IndexOf('Invoke-LifecycleRaceExactDiagnostic -State $State', [StringComparison]::Ordinal)
$diagnosticCallIndex = $lifecycleMainSource.IndexOf('Invoke-LifecycleMultiGatewayDiagnostic -State $State', [StringComparison]::Ordinal)
$normalSuiteIndex = $lifecycleMainSource.IndexOf('Invoke-LifecycleRustSuites -State $State', [StringComparison]::Ordinal)
$normalAwsIndex = if ($normalSuiteIndex -ge 0) {
    $lifecycleMainSource.IndexOf('Invoke-LifecycleAwsEvidence -State $State -Network $network', $normalSuiteIndex, [StringComparison]::Ordinal)
} else {
    -1
}
$diagnosticBranchPattern = '(?s)if \(\$DiagnoseLifecycleAws\) \{\s*Set-LifecycleStage -State \$State -Stage "aws"\s*Invoke-LifecycleAwsEvidence -State \$State -Network \$network\s*\$State\.DiagnosticOutcome = "aws-diagnostic-passed"\s*\} elseif \(\$DiagnoseLifecycleRaceStability\) \{\s*Invoke-LifecycleRaceStabilityDiagnostic -State \$State\s*\} elseif \(\$DiagnoseLifecycleRaceExact\) \{\s*Invoke-LifecycleRaceExactDiagnostic -State \$State\s*\} elseif \(\$DiagnoseMultiGateway\) \{\s*Invoke-LifecycleMultiGatewayDiagnostic -State \$State\s*\} else \{\s*Invoke-LifecycleRustSuites -State \$State\s*Set-LifecycleStage -State \$State -Stage "aws"\s*Invoke-LifecycleAwsEvidence -State \$State -Network \$network\s*\}'
Test-MultiGatewayDiagnosticContract (
    $metadataIndex -ge 0 -and
    $awsBranchIndex -gt $metadataIndex -and
    $stabilityBranchIndex -gt $awsBranchIndex -and
    $exactBranchIndex -gt $stabilityBranchIndex -and
    $exactCallIndex -gt $exactBranchIndex -and
    $diagnosticBranchIndex -gt $exactCallIndex -and
    $diagnosticCallIndex -gt $diagnosticBranchIndex -and
    $normalSuiteIndex -gt $diagnosticCallIndex -and
    $normalAwsIndex -gt $normalSuiteIndex -and
    $lifecycleMainSource.Contains('Invoke-LifecycleAwsEvidence -State $State -Network $network', [StringComparison]::Ordinal)
) "Lifecycle main must branch after metadata so diagnostic mode cannot overlap normal suites or AWS"

$awsEvidenceSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleAwsEvidence"
$awsSubstages = @("control-plane", "current-unversioned", "current-enabled", "current-suspended", "noncurrent-content", "noncurrent-marker", "sole-marker-timed", "sole-marker-eodm")
$previousAwsStage = -1
foreach ($awsSubstage in $awsSubstages) {
    $startReceipt = '"aws-substage=' + $awsSubstage + '"'
    $passReceipt = '"aws-substage=' + $awsSubstage + '-passed"'
    $startIndex = $awsEvidenceSource.IndexOf($startReceipt, [StringComparison]::Ordinal)
    $passIndex = $awsEvidenceSource.IndexOf($passReceipt, [StringComparison]::Ordinal)
    Test-MultiGatewayDiagnosticContract ($startIndex -gt $previousAwsStage -and $passIndex -gt $startIndex) "AWS lifecycle substage receipts are missing or out of order: $awsSubstage"
    $previousAwsStage = $passIndex
}
Test-MultiGatewayDiagnosticContract ($LifecycleRunnerSource.Contains('"[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=aws-passed"', [StringComparison]::Ordinal)) "AWS-only diagnostic result is missing"
$awsControlSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleControlPlaneEvidence"
$awsControlSubstages = @("files-written", "bucket-created", "initial-put", "initial-get-shape", "initial-revision", "unsupported-rejected", "revision-unchanged", "replacement-put", "replacement-get-shape", "lifecycle-deleted", "absent-get-verified")
$previousControlStage = -1
foreach ($controlStage in $awsControlSubstages) {
    $passedReceipt = '"aws-control-plane-substage=' + $controlStage + '-passed"'
    $passedIndex = $awsControlSource.IndexOf($passedReceipt, [StringComparison]::Ordinal)
    $startReceipt = '"aws-control-plane-substage=' + $controlStage + '"'
    $startIndex = $awsControlSource.IndexOf($startReceipt, [StringComparison]::Ordinal)
    Test-MultiGatewayDiagnosticContract ($startIndex -gt $previousControlStage -and $passedIndex -gt $startIndex) "AWS control-plane receipt is missing or out of order: $controlStage"
    $previousControlStage = $passedIndex
}
Test-MultiGatewayDiagnosticContract (-not $LifecycleRunnerSource.Contains('.xml', [StringComparison]::Ordinal) -and $LifecycleRunnerSource.Contains('$Kind.json', [StringComparison]::Ordinal)) "AWS lifecycle CLI configurations must be strict JSON files, not REST XML"
$nveSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleNoncurrentScenario"
$nveStageOrder = @("setup-complete", "age-applied", "wait-complete", "final-assert-passed")
$previousNveStage = -1
foreach ($nveStage in $nveStageOrder) {
    $fragment = '"aws-nve-$Target-substage=' + $nveStage + '"'
    $index = $nveSource.IndexOf($fragment, [StringComparison]::Ordinal)
    Test-MultiGatewayDiagnosticContract ($index -gt $previousNveStage) "NVE safe substage receipt is missing or out of order: $nveStage"
    $previousNveStage = $index
}
$revisionSource = Get-LifecycleRunnerFunctionSource "Get-LifecycleRevision"
$composeSource = Get-LifecycleRunnerFunctionSource "Invoke-Compose"
$lifecycleSqlSource = Get-LifecycleRunnerFunctionSource "Invoke-LifecycleSql"
Test-MultiGatewayDiagnosticContract ($composeSource.Contains('[int[]]$AllowedExitCodes = @(0)', [StringComparison]::Ordinal) -and $composeSource.Contains('-AllowedExitCodes $AllowedExitCodes', [StringComparison]::Ordinal)) "Compose wrapper must preserve default exits and pass explicit allowed exits"
foreach ($fragment in @(
    '-AllowedExitCodes @(0, 1)',
    '"missing-relation"',
    '"connection"',
    '"other"',
    '"lifecycle-sql-outcome=failed"',
    '"lifecycle-sql-error-category=$errorCategory"',
    '"lifecycle-sql-outcome=passed"',
    'throw "Lifecycle SQL command failed"'
)) {
    Test-MultiGatewayDiagnosticContract $lifecycleSqlSource.Contains($fragment, [StringComparison]::Ordinal) "Lifecycle SQL fixed classification is incomplete: $fragment"
}
foreach ($fragment in @(
    '$Bucket.Length -gt 63',
    "'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$'",
    '$Key.Length -gt 256',
    "'^[A-Za-z0-9][A-Za-z0-9._/-]{0,255}$'",
    'throw "Lifecycle SQL bucket is invalid"',
    'throw "Lifecycle SQL key is invalid"',
    '"-c", $query'
)) {
    Test-MultiGatewayDiagnosticContract $lifecycleSqlSource.Contains($fragment, [StringComparison]::Ordinal) "Lifecycle SQL validator-first literal contract is incomplete: $fragment"
}
Test-MultiGatewayDiagnosticContract (-not $lifecycleSqlSource.Contains("`:'bucket'", [StringComparison]::Ordinal) -and -not $lifecycleSqlSource.Contains("`:'key'", [StringComparison]::Ordinal) -and -not $lifecycleSqlSource.Contains('"bucket=$Bucket"', [StringComparison]::Ordinal) -and -not $lifecycleSqlSource.Contains('"key=$Key"', [StringComparison]::Ordinal)) "Lifecycle SQL must not use psql variable substitution"
Test-MultiGatewayDiagnosticContract (-not $lifecycleSqlSource.Contains('Write-Host', [StringComparison]::Ordinal) -and -not $lifecycleSqlSource.Contains('Write-Output', [StringComparison]::Ordinal)) "Lifecycle SQL classifier must not emit raw output"
foreach ($fragment in @(
    '$rows = @(Invoke-LifecycleSql -State $State -Statement "revision" -Bucket $Bucket -Key "lifecycle-control.txt")',
    '$rowCountReceipt = if ($rows.Count -eq 0) { "0" } elseif ($rows.Count -eq 1) { "1" } else { "many" }',
    '$shapeValid = $rows.Count -eq 1',
    '"revision-row-count=$rowCountReceipt"',
    '"revision-shape-valid=$($shapeValid.ToString().ToLowerInvariant())"',
    'if (-not $shapeValid) { throw "Lifecycle revision receipt is invalid" }'
)) {
    Test-MultiGatewayDiagnosticContract $revisionSource.Contains($fragment, [StringComparison]::Ordinal) "Lifecycle revision safe receipt is incomplete: $fragment"
}
Test-MultiGatewayDiagnosticContract (-not $revisionSource.Contains('$rows[0].Trim())"', [StringComparison]::Ordinal)) "Lifecycle revision receipt must not emit the revision value"
Test-MultiGatewayDiagnosticContract (
    $LifecycleRunnerSource.Contains('DiagnosticOutcome = $null', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('"[RESULT] lifecycle-expiration=DIAGNOSTIC outcome=not-reproduced"', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('"[RESULT] lifecycle-expiration=FAILED reason=multi-gateway-diagnostic-captured"', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('"[RESULT] lifecycle-expiration=PASSED"', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('Remove-OwnedLifecycleResources -State $state', [StringComparison]::Ordinal) -and
    $LifecycleRunnerSource.Contains('Restore-EnvironmentState -State $state.EnvironmentState', [StringComparison]::Ordinal)
) "Diagnostic mode must reuse lifecycle ownership/cleanup and retain normal Run terminal behavior"

if ($multiGatewayParserMatches.Count -eq 1) {
    Invoke-Expression $multiGatewayParserSource
    function Test-MultiGatewayFailureReceiptRejected {
        param([Parameter(Mandatory)][string[]]$Lines)
        try {
            $null = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{ StdOut = @($Lines); StdErr = @() })
            return $false
        } catch {
            return $true
        }
    }

    $acceptedReceipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 3 tests",
            "test multi_gateway::replica_a ... FAILED",
            "test multi_gateway::replica_b ... FAILED",
            "test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.34s"
        )
        StdErr = @("connection refused")
    })
    Test-MultiGatewayDiagnosticContract (
        $acceptedReceipt.Running -eq 3 -and
        $acceptedReceipt.Passed -eq 1 -and
        $acceptedReceipt.Failed -eq 2 -and
        ($acceptedReceipt.FailedNames -join ",") -ceq "multi_gateway::replica_a,multi_gateway::replica_b" -and
        $acceptedReceipt.FirstErrorCategory -ceq "connection" -and
        $acceptedReceipt.LastStage -ceq "not-reached"
    ) "Multi-gateway receipt parser must accept one bounded anchored failed-suite receipt"
    foreach ($fixture in @(
        [pscustomobject]@{ Category = "timeout"; Text = "deadline has elapsed" },
        [pscustomobject]@{ Category = "assertion"; Text = "panicked at fixture assertion" },
        [pscustomobject]@{ Category = "http-status"; Text = "HTTP status code 500" },
        [pscustomobject]@{ Category = "connection"; Text = "connection refused" },
        [pscustomobject]@{ Category = "database"; Text = "postgres database error" },
        [pscustomobject]@{ Category = "process-exit"; Text = "opaque safe fixture" }
    )) {
        $receipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
            StdOut = @(
                "running 1 test",
                "test multi_gateway::receipt_fixture ... FAILED",
                "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
            )
            StdErr = @($fixture.Text)
        })
        Test-MultiGatewayDiagnosticContract ($receipt.FirstErrorCategory -ceq $fixture.Category) "Multi-gateway receipt parser category changed: $($fixture.Category)"
    }
    foreach ($rejected in @(
        @("running 1 test", "test multi-gateway::unsafe ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "test multi_gateway::zero ... FAILED", "test result: FAILED. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 2 tests", "test multi_gateway::duplicate ... FAILED", "test multi_gateway::duplicate ... FAILED", "test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 2 tests", "test multi_gateway::mismatch ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "test multi_gateway::slow ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3601s"),
        @("prefix running 1 test", "test multi_gateway::substring ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "running 1 test", "test multi_gateway::duplicate_running ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s")
    )) {
        Test-MultiGatewayDiagnosticContract (Test-MultiGatewayFailureReceiptRejected -Lines $rejected) "Multi-gateway receipt parser accepted an invalid fixture"
    }

    $raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
    $safeStages = @(
        "terminal-state-evaluation",
        "successor-read",
        "version-cleanup",
        "bucket-delete"
    )
    foreach ($stage in $safeStages) {
        $receipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
            StdOut = @(
                "running 1 test",
                "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=$stage",
                "test $raceTestName ... FAILED",
                "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"
            )
            StdErr = @("fixture assertion")
        })
        Test-MultiGatewayDiagnosticContract ($receipt.LastStage -ceq $stage) "Multi-gateway receipt parser rejected safe stage $stage"
    }
    $repeatedReceipt = Get-MultiGatewayFailureReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 1 test",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=terminal-state-evaluation",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-read",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
            "test $raceTestName ... FAILED",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s"
        )
        StdErr = @("fixture assertion")
    })
    Test-MultiGatewayDiagnosticContract ($repeatedReceipt.LastStage -ceq "bucket-delete") "Multi-gateway receipt parser did not retain the last bounded stage"
    foreach ($rejectedStage in @(
        @("running 1 test", "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=unknown", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete payload=raw", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s"),
        @("running 1 test", "test $raceTestName ... FAILED", "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s")
    )) {
        Test-MultiGatewayDiagnosticContract (Test-MultiGatewayFailureReceiptRejected -Lines $rejectedStage) "Multi-gateway receipt parser accepted an unsafe stage fixture"
    }
}

if ($exactRaceParserMatches.Count -eq 1 -and $exactRaceDiagnosticMatches.Count -eq 1) {
    $exactParserSource = $exactRaceParserMatches[0].Extent.Text
    $exactDiagnosticSource = $exactRaceDiagnosticMatches[0].Extent.Text
    foreach ($fragment in @(
        '[Parameter(Mandatory)][object]$Result',
        '[Parameter(Mandatory)][ValidateSet(0, 101)][int]$ExitCode',
        '$outcome = if ($ExitCode -eq 0',
        'Outcome = $outcome',
        'LastStage = $lastStage',
        'FirstErrorCategory = $errorCategory',
        'FailedName = if ($ExitCode -eq 101)',
        'CommandOutcome = if ($ExitCode -eq 0)',
        'ParserFailureCategory',
        'CountShape',
        "'(?m)\[LIFECYCLE-RACE-STAGE\][^\r\n]*'",
        'stage-rejected',
        'output-shape-rejected'
        'SuccessorRequestDispatchedSeen'
        'SuccessorRequestCompleteSeen'
        'ObserverGetResponseSeen'
        'ObserverListResponseSeen'
        'ObserverSuccessorVisibleSeen'
    )) {
        Test-MultiGatewayDiagnosticContract $exactParserSource.Contains($fragment, [StringComparison]::Ordinal) "Exact lifecycle-race parser is incomplete: $fragment"
    }
    foreach ($fragment in @(
        'cargo test --test multi_gateway multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome -- --exact --nocapture --test-threads=1',
        '"multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"',
        'Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode',
        '"exact-race-running=$($receipt.Running)"',
        '"exact-race-passed=$($receipt.Passed)"',
        '"exact-race-failed=$($receipt.Failed)"',
        '"exact-race-failed-test=$($receipt.FailedName)"',
        '"exact-race-first-error-category=$($receipt.FirstErrorCategory)"',
        '"exact-race-last-stage=$($receipt.LastStage)"',
        '"exact-race-command-outcome=$($receipt.CommandOutcome)"',
        '"exact-race-parser-failure-category=$($receipt.ParserFailureCategory)"',
        '"exact-race-count-shape=$($receipt.CountShape)"'
        '"exact-race-successor-request-dispatched-seen=$($receipt.SuccessorRequestDispatchedSeen.ToString().ToLowerInvariant())"'
        '"exact-race-successor-request-complete-seen=$($receipt.SuccessorRequestCompleteSeen.ToString().ToLowerInvariant())"'
        '"exact-race-observer-get-response-seen=$($receipt.ObserverGetResponseSeen.ToString().ToLowerInvariant())"'
        '"exact-race-observer-list-response-seen=$($receipt.ObserverListResponseSeen.ToString().ToLowerInvariant())"'
        '"exact-race-observer-successor-visible-seen=$($receipt.ObserverSuccessorVisibleSeen.ToString().ToLowerInvariant())"'
    )) {
        Test-MultiGatewayDiagnosticContract $exactDiagnosticSource.Contains($fragment, [StringComparison]::Ordinal) "Exact lifecycle-race diagnostic is incomplete: $fragment"
    }
    Test-MultiGatewayDiagnosticContract (-not $exactDiagnosticSource.Contains('Invoke-LifecycleRustSuites', [StringComparison]::Ordinal) -and -not $exactDiagnosticSource.Contains('Invoke-LifecycleAwsEvidence', [StringComparison]::Ordinal)) "Exact lifecycle-race diagnostic must not run normal suites or AWS"

    Invoke-Expression $exactParserSource
    $raceTestName = "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome"
    $passReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 1 test",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=terminal-state-evaluation",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-read",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=bucket-delete",
            "test $raceTestName ... ok",
            "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out; finished in 0.02s"
        ); StdErr = @()
    }) -ExitCode 0
    Test-MultiGatewayDiagnosticContract ($passReceipt.Outcome -ceq "passed" -and $passReceipt.LastStage -ceq "bucket-delete") "Exact lifecycle-race parser rejected safe pass"
    $failReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 1 test",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup",
            "test $raceTestName ... FAILED",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s"
        ); StdErr = @("fixture assertion")
    }) -ExitCode 101
    Test-MultiGatewayDiagnosticContract ($failReceipt.Outcome -ceq "failed" -and $failReceipt.FailedName -ceq $raceTestName -and $failReceipt.LastStage -ceq "version-cleanup") "Exact lifecycle-race parser rejected safe failure"
    $prefixedReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "test-harness-prefix: [LIFECYCLE-RACE-STAGE] test=$raceTestName stage=version-cleanup"
        ); StdErr = @()
    }) -ExitCode 101
    Test-MultiGatewayDiagnosticContract (
        $prefixedReceipt.LastStage -ceq "version-cleanup" -and
        $prefixedReceipt.CountShape -ceq "unavailable" -and
        $prefixedReceipt.ParserFailureCategory -ceq "none" -and
        $prefixedReceipt.CommandOutcome -ceq "allowed-exit-101"
    ) "Exact lifecycle-race parser rejected prefixed stage with unavailable counts"
    $booleanReceipt = Get-LifecycleRaceExactReceipt -Result ([pscustomobject]@{
        StdOut = @(
            "running 1 test",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-request-dispatched",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=successor-request-complete",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-get-response",
            "[LIFECYCLE-RACE-STAGE] test=$raceTestName stage=observer-list-response",
            "test $raceTestName ... FAILED",
            "test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s"
        ); StdErr = @("fixture assertion")
    }) -ExitCode 101
    Test-MultiGatewayDiagnosticContract (
        $booleanReceipt.SuccessorRequestDispatchedSeen -and
        $booleanReceipt.SuccessorRequestCompleteSeen -and
        $booleanReceipt.ObserverGetResponseSeen -and
        $booleanReceipt.ObserverListResponseSeen -and
        -not $booleanReceipt.ObserverSuccessorVisibleSeen
    ) "Exact lifecycle-race parser did not preserve fixed stage-presence booleans"
}

if ($stabilityDiagnosticMatches.Count -eq 1) {
    $stabilitySource = $stabilityDiagnosticMatches[0].Extent.Text
    foreach ($fragment in @(
        'foreach ($iteration in 1..5)',
        'Owned lifecycle-race stability process',
        'Get-LifecycleRaceExactReceipt -Result $result -ExitCode $result.ExitCode',
        'exact-race-stability-iteration=$iteration',
        'exact-race-command-outcome=$($receipt.CommandOutcome)',
        'exact-race-parser-failure-category=$($receipt.ParserFailureCategory)',
        'exact-race-count-shape=$($receipt.CountShape)',
        'exact-race-last-stage=$($receipt.LastStage)',
        'exact-race-successor-request-dispatched-seen=$($receipt.SuccessorRequestDispatchedSeen.ToString().ToLowerInvariant())',
        'exact-race-successor-request-complete-seen=$($receipt.SuccessorRequestCompleteSeen.ToString().ToLowerInvariant())',
        'exact-race-observer-get-response-seen=$($receipt.ObserverGetResponseSeen.ToString().ToLowerInvariant())',
        'exact-race-observer-list-response-seen=$($receipt.ObserverListResponseSeen.ToString().ToLowerInvariant())',
        'exact-race-observer-successor-visible-seen=$($receipt.ObserverSuccessorVisibleSeen.ToString().ToLowerInvariant())',
        '$State.DiagnosticOutcome = "exact-race-stability-passed"'
    )) {
        Test-MultiGatewayDiagnosticContract $stabilitySource.Contains($fragment, [StringComparison]::Ordinal) "Lifecycle race stability mode is incomplete: $fragment"
    }
    Test-MultiGatewayDiagnosticContract (-not $stabilitySource.Contains('Invoke-LifecycleRustSuites', [StringComparison]::Ordinal) -and -not $stabilitySource.Contains('Invoke-LifecycleAwsEvidence', [StringComparison]::Ordinal)) "Lifecycle race stability mode must not run normal suites or AWS"
}
if ($multiGatewayDiagnosticFailures.Count -ne 0) {
    throw "Lifecycle Task 13 multi-gateway diagnostic contracts are missing: $($multiGatewayDiagnosticFailures -join '; ')"
}

# Lifecycle promotion may change README only. Bucket CORS promotion is checked
# later as an exact v0.6 section, preserving the unchecked Lifecycle item.
$protectedLifecyclePaths = @(
    ".github/workflows/release-validation.yml",
    "docker-compose.yml",
    "docker-compose.postgres.yml",
    "docker-compose.override.yml",
    "docker-compose.cluster.yml",
    "docker-compose.multi-gateway.yml"
)
foreach ($protectedPath in $protectedLifecyclePaths) {
    & git diff --quiet HEAD -- $protectedPath
    Assert-True ($LASTEXITCODE -eq 0) "Lifecycle work changed protected path: $protectedPath"
}

foreach ($name in @(
    "New-VersioningRunId",
    "New-VersioningProjectName",
    "Assert-CanonicalChildPath",
    "Get-CanonicalExistingDirectory",
    "New-VersioningRunRoot",
    "New-VersioningOwnershipReceipt",
    "Assert-ProjectResourcesAbsent",
    "Claim-VersioningProjectOwnership",
    "Assert-LoopbackPortsFree",
    "Test-LocalImage",
    "Assert-ComposeVersion",
    "Invoke-OfflineGatewayBuild",
    "Invoke-AwsVersioningSmoke",
    "Write-Evidence",
    "Set-VersioningStage",
    "Remove-OwnedEmptyVersioningRunRoot",
    "Remove-OwnedVersioningResources",
    "Restore-EnvironmentState"
)) {
    $null = Get-VersioningRunnerFunctionSource $name
}

$versioningRunIdSource = Get-VersioningRunnerFunctionSource "New-VersioningRunId"
Assert-Contains $versioningRunIdSource "^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$" "Object-versioning RunId grammar changed"
$projectSource = Get-VersioningRunnerFunctionSource "New-VersioningProjectName"
Assert-Contains $projectSource '"ipfs3-ver-$RunId"' "Object-versioning project grammar is missing"
$runRootSource = Get-VersioningRunnerFunctionSource "New-VersioningRunRoot"
foreach ($fragment in @(
    '"ipfs-s3-object-versioning-$RunId"',
    'RunRoot must be a direct child'
)) {
    Assert-Contains $runRootSource $fragment "Object-versioning root/receipt guard missing: $fragment"
}
$receiptSource = Get-VersioningRunnerFunctionSource "New-VersioningOwnershipReceipt"
foreach ($fragment in @('[IO.FileMode]::CreateNew', 'ownership-receipt')) {
    Assert-Contains $receiptSource $fragment "Object-versioning receipt guard missing: $fragment"
}

$labelPreflightSource = Get-VersioningRunnerFunctionSource "Assert-ProjectResourcesAbsent"
foreach ($fragment in @(
    '"ps", "-aq"',
    '"network", "ls", "-q"',
    '"volume", "ls", "-q"',
    '"label=com.docker.compose.project=$Project"',
    'BLOCKED'
)) {
    Assert-Contains $labelPreflightSource $fragment "Project label preflight guard missing: $fragment"
}
$imageSource = Get-VersioningRunnerFunctionSource "Test-LocalImage"
Assert-Contains $imageSource '"image", "inspect", $Image, "--format", "{{.Id}}"' "Local image inspection is not exact"
foreach ($fragment in @("postgres:17", "ghcr.io/hugefiver/ipfs3-kubo:latest", "ghcr.io/hugefiver/ipfs3:latest", "rust:latest", "amazon/aws-cli:latest")) {
    Assert-Contains $VersioningRunnerSource $fragment "Required exact local image is missing: $fragment"
}
foreach ($forbidden in @("docker pull", "Install-Module", "choco install", "winget install", "scoop install", "Invoke-WebRequest")) {
    Assert-NotContains $VersioningRunnerSource $forbidden "Runner may not pull or install dependencies: $forbidden"
}

$composeVersionSource = Get-VersioningRunnerFunctionSource "Assert-ComposeVersion"
foreach ($fragment in @(
    '"compose", "version", "--short"',
    '(@($result.StdOut) -join "`n")',
    "'^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$'",
    '[Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion)',
    '[Version]"2.23.1"'
)) {
    Assert-Contains $composeVersionSource $fragment "Strict Compose version preflight missing: $fragment"
}
Assert-NotContains $composeVersionSource '"compose", "version", "--format", "{{.Version}}"' "Compose version preflight must use --short"
Assert-NotContains $composeVersionSource '^v?(?<core>' "Compose version preflight must reject a leading v"

function Test-ComposeVersionFixture {
    param([Parameter(Mandatory)][string]$Text)
    $match = [regex]::Match($Text, '^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$')
    $version = $null
    return $match.Success -and
        [Version]::TryParse($match.Groups["core"].Value, [ref]$version) -and
        $version -ge [Version]"2.23.1"
}
foreach ($fixture in @("2.39.2-desktop.1", "2.39.2+desktop.1", "2.39.2-alpha.1+build.7")) {
    Assert-True (Test-ComposeVersionFixture $fixture) "Compose version fixture must be accepted: $fixture"
}
foreach ($fixture in @("Docker Compose version v2.39.2-desktop.1", "2.39", "v2.39.2", "v2.39.2+desktop.1", "2.39.2 trailing", " 2.39.2", "2.39.2 ", "2.39.2-alpha..1", "2.39.2+build..1")) {
    Assert-True (-not (Test-ComposeVersionFixture $fixture)) "Compose version fixture must be rejected: $fixture"
}
$portSource = Get-VersioningRunnerFunctionSource "Assert-LoopbackPortsFree"
foreach ($fragment in @("55436", "55003", "59003", "TcpListener", "IPAddress]::Loopback")) {
    Assert-Contains ($portSource + $VersioningRunnerSource) $fragment "Independent loopback port proof is missing: $fragment"
}
$offlineSource = Get-VersioningRunnerFunctionSource "Invoke-OfflineGatewayBuild"
foreach ($fragment in @(
    '"vendor", "--locked", "--offline"',
    '"build", "--pull=false", "--network", "none", "--quiet"',
    '"--build-context", "vendor-archive=$archiveContext"'
)) {
    Assert-Contains $offlineSource $fragment "Offline build contract missing: $fragment"
}
Assert-Contains $offlineSource '-ArgumentList @("vendor", "--locked", "--offline", $vendorPath)' "Offline Cargo invocation must bind the declared ArgumentList parameter"
Assert-NotContains $offlineSource '-Arguments @("vendor", "--locked", "--offline", $vendorPath)' "Offline Cargo invocation must not bind nonexistent Arguments"
$invokeNativeFunctionAst = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq "Invoke-NativeCommand"
}, $true))[0]
$invokeNativeParameterNames = @($invokeNativeFunctionAst.Body.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
foreach ($call in @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq "Invoke-NativeCommand"
}, $true))) {
    $unknownParameters = @($call.CommandElements |
        Where-Object { $_ -is [System.Management.Automation.Language.CommandParameterAst] } |
        ForEach-Object { $_.ParameterName } |
        Where-Object { $_ -notin $invokeNativeParameterNames })
    Assert-True ($unknownParameters.Count -eq 0) "Invoke-NativeCommand binds undeclared parameter(s): $($unknownParameters -join ', ')"
}
$mainSource = Get-VersioningRunnerFunctionSource "Invoke-VersioningMain"
foreach ($fragment in @(
    '"config", "--quiet"',
    '"up", "--detach", "--pull", "never", "--no-build"',
    '"down", "--volumes", "--remove-orphans"',
    'cargo test --test postgres_versioning -- --nocapture --test-threads=1',
    'cargo test --test e2e -- --nocapture --test-threads=1'
)) {
    Assert-Contains $VersioningRunnerSource $fragment "Main lifecycle contract missing: $fragment"
}

$awsSource = Get-VersioningRunnerFunctionSource "Invoke-AwsVersioningSmoke"
$snapshotStages = @(
    "after-second-enabled-put",
    "after-simple-marker",
    "after-exact-marker-delete-and-restored-get",
    "after-versioning-suspension",
    "after-first-null-put",
    "after-second-null-put",
    "after-suspended-list-parse"
)
$snapshotFunctionMatches = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq "Write-PostgresVersionSnapshot"
}, $true))
$snapshotFunctionAst = if ($snapshotFunctionMatches.Count -eq 1) { $snapshotFunctionMatches[0] } else { $null }
$snapshotSource = if ($null -ne $snapshotFunctionAst) { $snapshotFunctionAst.Extent.Text } else { "" }
$snapshotContractFailures = [Collections.Generic.List[string]]::new()
function Test-SnapshotContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $snapshotContractFailures.Add($Message) }
}

$snapshotParameterNames = if ($null -ne $snapshotFunctionAst) {
    @($snapshotFunctionAst.Body.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
} else { @() }
$expectedSnapshotValidateSet = '[ValidateSet("' + ($snapshotStages -join '", "') + '")]'
Test-SnapshotContract ($snapshotFunctionMatches.Count -eq 1) "Write-PostgresVersionSnapshot must exist exactly once"
Test-SnapshotContract (
    ($snapshotParameterNames -join ',') -ceq 'State,SnapshotStage,Bucket,Key' -and
    $snapshotSource.Contains('[Parameter(Mandatory)][hashtable]$State', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('[Parameter(Mandatory)]' + $expectedSnapshotValidateSet + '[string]$SnapshotStage', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('[Parameter(Mandatory)][string]$Bucket', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('[Parameter(Mandatory)][string]$Key', [StringComparison]::Ordinal)
) "DB snapshot function must take the exact mandatory State, SnapshotStage, Bucket, Key contract"
Test-SnapshotContract (
    $snapshotSource.Contains($expectedSnapshotValidateSet, [StringComparison]::Ordinal) -and
    $snapshotSource.Contains("`$Bucket -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])`$'", [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$Key -cnotmatch ''^versioned-object\.txt$''', [StringComparison]::Ordinal)
) "DB snapshot must allowlist the exact fixed stages and validate generated Bucket plus constant Key"

$snapshotComposeCalls = if ($null -ne $snapshotFunctionAst) {
    @($snapshotFunctionAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.CommandAst] -and
            $node.GetCommandName() -eq "Invoke-Compose"
    }, $true))
} else { @() }
$psqlArgumentPattern = '(?s)-Arguments\s+@\(\s*"exec",\s*"-T",\s*"postgres",\s*"psql",\s*"-X",\s*"-U",\s*"ipfs3",\s*"-d",\s*"ipfs3",\s*"-v",\s*"ON_ERROR_STOP=1",\s*"-A",\s*"-t",\s*"-F",\s*"\|",\s*"-v",\s*"bucket=\$Bucket",\s*"-v",\s*"key=\$Key",\s*"-c",\s*\$query\s*\)'
Test-SnapshotContract (
    $snapshotComposeCalls.Count -eq 1 -and
    [regex]::IsMatch($snapshotSource, $psqlArgumentPattern) -and
    $snapshotSource.Contains('Invoke-Compose -Project $State.Project', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('-Timeout ([TimeSpan]::FromSeconds(30))', [StringComparison]::Ordinal)
) "DB snapshot must use one owned bounded Invoke-Compose psql call with the exact noninteractive argv"
foreach ($fragment in @(
    'FROM object_versions AS versions',
    'LEFT JOIN objects AS linked ON linked.id = versions.object_id',
    'FROM objects AS current_objects',
    'versions.bucket = :''bucket'' AND versions.key = :''key''',
    'current_objects.bucket = :''bucket'' AND current_objects.key = :''key''',
    "versions.kind = 'delete_marker'",
    'versions.is_latest',
    'versions.object_id IS NOT NULL',
    "string_agg(versions.sequence::text, '.' ORDER BY versions.sequence ASC)",
    "'none'"
)) {
    Test-SnapshotContract $snapshotSource.Contains($fragment, [StringComparison]::Ordinal) "DB snapshot query contract is missing: $fragment"
}
$snapshotQueryStart = $snapshotSource.IndexOf('$query = @"', [StringComparison]::Ordinal)
$snapshotQueryEnd = if ($snapshotQueryStart -ge 0) { $snapshotSource.IndexOf('"@', $snapshotQueryStart + 1, [StringComparison]::Ordinal) } else { -1 }
$snapshotQuerySource = if ($snapshotQueryEnd -gt $snapshotQueryStart) {
    $snapshotSource.Substring($snapshotQueryStart, $snapshotQueryEnd - $snapshotQueryStart)
} else { "" }
Test-SnapshotContract (
    -not [regex]::IsMatch($snapshotQuerySource, '(?i)\b(?:INSERT|UPDATE|DELETE|ALTER|DROP|CREATE)\b') -and
    -not $snapshotQuerySource.Contains('$Bucket', [StringComparison]::Ordinal) -and
    -not $snapshotQuerySource.Contains('$Key', [StringComparison]::Ordinal) -and
    -not $snapshotQuerySource.Contains('buckets', [StringComparison]::Ordinal) -and
    -not $snapshotQuerySource.Contains('bucket_state', [StringComparison]::Ordinal) -and
    -not $snapshotQuerySource.Contains('versioning_status', [StringComparison]::Ordinal)
) "DB snapshot query must be read-only, bind Bucket/Key through psql variables, and use only version/object tables"
Test-SnapshotContract (
    $snapshotSource.Contains('$rows = @($result.StdOut | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$rows.Count -ne 1', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains("`$fields = `$rows[0].Split('|')", [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$fields.Count -ne 8', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('foreach ($index in 0..6)', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains("`$fields[`$index] -cnotmatch '^[0-9]+`$'", [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('[Int64]::TryParse', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$parsedCount -lt 0', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$null = $counts.Add($parsedCount)', [StringComparison]::Ordinal)
) "DB snapshot must require exactly one row with seven nonnegative integer fields and one sequence field"
Test-SnapshotContract (
    $snapshotSource.Contains("`$sequence.Contains(',')", [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$sequence -cnotmatch ''^[1-9][0-9]*(?:\.[1-9][0-9]*)*$''', [StringComparison]::Ordinal) -and
    $snapshotSource.Contains('$currentSequence -le $previousSequence', [StringComparison]::Ordinal) -and
    -not $snapshotSource.Contains('$bucketState', [StringComparison]::Ordinal) -and
    -not $snapshotSource.Contains('nine fields', [StringComparison]::Ordinal)
) "DB snapshot must reject comma, malformed, or non-increasing sequences without a bucket-state field"

$snapshotEvidenceCalls = if ($null -ne $snapshotFunctionAst) {
    @($snapshotFunctionAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.CommandAst] -and
            $node.GetCommandName() -eq "Write-Evidence"
    }, $true))
} else { @() }
$expectedSnapshotEvidence = @(
    'Write-Evidence -Category "diagnostic" -Value "db-snapshot-stage=$SnapshotStage"',
    'Write-Evidence -Category "diagnostic" -Value "db-total-version-rows=$versionCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-opaque-rows=$opaqueCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-null-rows=$nullCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-delete-marker-rows=$markerCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-latest-rows=$latestCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-linked-object-rows=$linkedCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-object-rows=$objectCount"',
    'Write-Evidence -Category "diagnostic" -Value "db-sequences=$sequence"'
)
Test-SnapshotContract (
    $snapshotEvidenceCalls.Count -eq 9 -and
    ((@($snapshotEvidenceCalls | ForEach-Object { $_.Extent.Text.Trim() }) -join "`n") -ceq ($expectedSnapshotEvidence -join "`n")) -and
    -not $snapshotSource.Contains('db-bucket-state=', [StringComparison]::Ordinal) -and
    -not $snapshotSource.Contains('StdErr', [StringComparison]::Ordinal) -and
    -not [regex]::IsMatch($snapshotSource, '(?m)^\s*(?:Write-Host|Write-Output|Write-Error)\b')
) "DB snapshot evidence must be exactly the fixed safe fields and must not expose raw psql output"

$snapshotCallPattern = '(?m)^\s*Write-PostgresVersionSnapshot -State \$State -SnapshotStage "(?<stage>[a-z0-9-]+)" -Bucket \$bucket -Key \$key\s*$'
$snapshotCalls = @([regex]::Matches($awsSource, $snapshotCallPattern))
$allSnapshotCallLines = @([regex]::Matches($awsSource, '(?m)^\s*Write-PostgresVersionSnapshot\b'))
$actualSnapshotStages = @($snapshotCalls | ForEach-Object { $_.Groups['stage'].Value })
Test-SnapshotContract (
    ($actualSnapshotStages -join ',') -ceq ($snapshotStages -join ',') -and
    $snapshotCalls.Count -eq 7 -and
    $allSnapshotCallLines.Count -eq 7
) "AWS smoke must make exactly seven fixed direct DB snapshot calls in approved stage order"
Test-SnapshotContract (
    $awsSource.Contains('[Parameter(Mandatory)][hashtable]$State', [StringComparison]::Ordinal) -and
    $mainSource.Contains('Invoke-AwsVersioningSmoke -State $State -Network $network -RunRoot $State.RunRoot -RunId $State.RunId', [StringComparison]::Ordinal)
) "AWS DB snapshot calls must receive the owned state from main"
if ($snapshotCalls.Count -eq 7) {
    $enabledIdentityAnchor = $awsSource.IndexOf('if ($firstId -ceq $secondId) { throw "Opaque object VersionIds must be distinct" }', [StringComparison]::Ordinal)
    $markerCreationAnchor = $awsSource.IndexOf('throw "Delete marker VersionId must be a third opaque identity"', [StringComparison]::Ordinal)
    $markerRestoreAnchor = $awsSource.IndexOf('throw "Exact delete marker did not restore second body"', [StringComparison]::Ordinal)
    $suspendAnchor = $awsSource.IndexOf('"Status=Suspended"', [StringComparison]::Ordinal)
    $nullFirstAnchor = $awsSource.IndexOf('if ([string]$nullFirst.VersionId -cne "null") { throw "Suspended upload did not return VersionId null" }', [StringComparison]::Ordinal)
    $nullSecondAnchor = $awsSource.IndexOf('if ([string]$nullSecond.VersionId -cne "null") { throw "Suspended null-slot overwrite did not return VersionId null" }', [StringComparison]::Ordinal)
    $suspendedParseAnchor = $awsSource.IndexOf('if ($null -eq $suspendedList -or $suspendedList -isnot [pscustomobject]) { throw "Suspended list JSON was not an object" }', [StringComparison]::Ordinal)
    $suspendedCountAnchor = $awsSource.IndexOf('[int]$versionCount = $suspendedVersions.Count', [StringComparison]::Ordinal)
    $suspendedAssertAnchor = $awsSource.IndexOf('Write-AwsSubstage -Label "suspended-list-assert"', [StringComparison]::Ordinal)
    Test-SnapshotContract ($snapshotCalls[0].Index -gt $enabledIdentityAnchor -and $snapshotCalls[0].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "simple-delete"', [StringComparison]::Ordinal)) "after-second-enabled-put snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[1].Index -gt $markerCreationAnchor -and $snapshotCalls[1].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "list-enabled"', [StringComparison]::Ordinal)) "after-simple-marker snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[2].Index -gt $markerRestoreAnchor -and $snapshotCalls[2].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "suspend-versioning"', [StringComparison]::Ordinal)) "after-exact-marker-delete-and-restored-get snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[3].Index -gt $suspendAnchor -and $snapshotCalls[3].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "put-null-first"', [StringComparison]::Ordinal)) "after-versioning-suspension snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[4].Index -gt $nullFirstAnchor -and $snapshotCalls[4].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "put-null-second"', [StringComparison]::Ordinal)) "after-first-null-put snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[5].Index -gt $nullSecondAnchor -and $snapshotCalls[5].Index -lt $awsSource.IndexOf('Write-AwsSubstage -Label "suspended-list"', [StringComparison]::Ordinal)) "after-second-null-put snapshot placement changed"
    Test-SnapshotContract ($snapshotCalls[6].Index -gt $suspendedParseAnchor -and $snapshotCalls[6].Index -lt $suspendedCountAnchor -and $snapshotCalls[6].Index -lt $suspendedAssertAnchor) "after-suspended-list-parse snapshot placement changed"
}
if ($snapshotContractFailures.Count -ne 0) {
    throw "Object-versioning PostgreSQL snapshot contracts are missing: $($snapshotContractFailures -join '; ')"
}

foreach ($fragment in @(
    'create-bucket',
    'put-bucket-versioning',
    'Status=Enabled',
    'put-object',
    'Assert-CanonicalOpaqueVersionId',
    'DeleteMarker',
    'list-object-versions',
    'get-object',
    'head-object',
    'NoSuchKey',
    'marker-head-405',
    'Status=Suspended',
    'VersionId -ceq "null"',
    '"--version-id", $versionId',
    'delete-bucket',
    'http://gateway:9000'
)) {
    Assert-Contains $awsSource $fragment "AWS versioning assertion is missing: $fragment"
}
$awsContractFailures = [Collections.Generic.List[string]]::new()
function Test-AwsContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $awsContractFailures.Add($Message) }
}

$expectedAwsConfigContent = '"[default]`nregion = us-east-1`ns3 =`n    addressing_style = path`n"'
$awsConfigFunctionMatches = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq "New-VersioningAwsConfig"
}, $true))
$awsConfigSource = if ($awsConfigFunctionMatches.Count -eq 1) { $awsConfigFunctionMatches[0].Extent.Text } else { "" }
Test-AwsContract (
    $VersioningRunnerSource.Contains('function New-VersioningAwsConfig', [StringComparison]::Ordinal) -and
    $VersioningRunnerSource.Contains('Join-Path $RunRoot "aws-config"', [StringComparison]::Ordinal) -and
    $VersioningRunnerSource.Contains($expectedAwsConfigContent, [StringComparison]::Ordinal) -and
    $VersioningRunnerSource.Contains('[IO.File]::WriteAllText($configPath, $configContent, [Text.UTF8Encoding]::new($false))', [StringComparison]::Ordinal) -and
    $mainSource.Contains('New-VersioningAwsConfig -RunRoot $State.RunRoot', [StringComparison]::Ordinal) -and
    -not [regex]::IsMatch($awsConfigSource, '(?i)credential|endpoint|token|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY')
) "AWS config must be a strict UTF-8-no-BOM /work/aws-config file created after receipt ownership"

$invokeAwsSource = Get-VersioningRunnerFunctionSource "Invoke-Aws"
$invokeAwsJsonSource = Get-VersioningRunnerFunctionSource "Invoke-AwsJson"
Test-AwsContract (
    $invokeAwsSource.Contains('"-e", "AWS_CONFIG_FILE=/work/aws-config"', [StringComparison]::Ordinal) -and
    ([regex]::Matches($invokeAwsSource, 'AWS_CONFIG_FILE=/work/aws-config')).Count -eq 1 -and
    -not $VersioningRunnerSource.Contains('AWS_S3_FORCE_PATH_STYLE', [StringComparison]::Ordinal) -and
    -not $invokeAwsSource.Contains('PathStyleEnvironment', [StringComparison]::Ordinal)
) "Every AWS container must use /work/aws-config and must not use AWS_S3_FORCE_PATH_STYLE"
Test-AwsContract (
    $invokeAwsJsonSource.Contains('if ($Arguments.Count -eq 0 -or $Arguments[0] -ne "s3api" -or', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('@($Arguments | Where-Object { $_ -eq "--output" }).Count -ne 0', [StringComparison]::Ordinal) -and
    ([regex]::Matches($invokeAwsJsonSource, [regex]::Escape('@("--output", "json") + $Arguments'))).Count -eq 2
) "Each Invoke-AwsJson branch must prepend exactly one global --output json before s3api"

$expectedAwsSubstages = @(
    "cli-version", "create-bucket", "enable-versioning", "put-first", "assert-first", "put-second", "assert-second",
    "simple-delete", "assert-delete-marker", "list-enabled", "assert-enabled-order", "get-historical", "assert-historical",
    "current-get-nosuchkey", "current-head-404", "marker-head-405", "delete-marker-exact", "get-promoted", "assert-promoted",
    "suspend-versioning", "put-null-first", "assert-null-first", "put-null-second", "assert-null-second", "suspended-list",
    "suspended-list-assert", "exact-cleanup-delete", "empty-list", "assert-empty", "delete-bucket", "complete"
)
$actualAwsSubstages = @([regex]::Matches($awsSource, '(?m)^\s*Write-AwsSubstage -Label "(?<label>[a-z0-9-]+)"\s*$') | ForEach-Object { $_.Groups['label'].Value })
$expectedAwsValidateSet = '[ValidateSet("' + ($expectedAwsSubstages -join '", "') + '")]'
Test-AwsContract (
    $VersioningRunnerSource.Contains('function Write-AwsSubstage', [StringComparison]::Ordinal) -and
    $VersioningRunnerSource.Contains($expectedAwsValidateSet, [StringComparison]::Ordinal) -and
    ($actualAwsSubstages -join ',') -ceq ($expectedAwsSubstages -join ',') -and
    -not [regex]::IsMatch($awsSource, '(?m)^\s*Write-AwsSubstage -Label \$')
) "AWS substage evidence must have exactly the approved 31 fixed-safe labels in order"
$exactCleanupLoopIndex = $awsSource.IndexOf('foreach ($entry in @($finalVersions + $finalMarkers)) {', [StringComparison]::Ordinal)
$exactCleanupSubstageIndex = $awsSource.IndexOf('Write-AwsSubstage -Label "exact-cleanup-delete"', $exactCleanupLoopIndex, [StringComparison]::Ordinal)
$exactCleanupDeleteIndex = $awsSource.IndexOf('Invoke-Aws -Network $Network -RunRoot $RunRoot -Endpoint $endpoint -Arguments @(', $exactCleanupSubstageIndex, [StringComparison]::Ordinal)
Test-AwsContract (
    $exactCleanupLoopIndex -ge 0 -and
    $exactCleanupSubstageIndex -gt $exactCleanupLoopIndex -and
    $exactCleanupDeleteIndex -gt $exactCleanupSubstageIndex
) "Only exact-cleanup-delete may repeat at runtime and it must precede every retained-entry delete"
Test-AwsContract (
    $VersioningRunnerSource.Contains('Write-Evidence -Category "result" -Value "failed-stage=$($state.Stage)"', [StringComparison]::Ordinal) -and
    -not [regex]::IsMatch($awsSource, 'Write-Evidence[^\r\n]*(?:\$bucket|\$key|\$RunRoot|\$firstId|\$secondId|\$markerId|/work/)') -and
    -not $VersioningRunnerSource.Contains('Write-Host $_', [StringComparison]::Ordinal)
) "AWS failure evidence must remain generic and exclude response identifiers or paths"
if ($awsContractFailures.Count -ne 0) {
    throw "Object-versioning AWS client contracts are missing: $($awsContractFailures -join '; ')"
}

$suspendedReceiptFailures = [Collections.Generic.List[string]]::new()
function Test-SuspendedReceiptContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $suspendedReceiptFailures.Add($Message) }
}

$suspendedListStart = $awsSource.IndexOf('$suspendedList = Invoke-AwsJson', [StringComparison]::Ordinal)
$suspendedAssertStart = $awsSource.IndexOf('Write-AwsSubstage -Label "suspended-list-assert"', [StringComparison]::Ordinal)
$suspendedReceiptBlock = if ($suspendedListStart -ge 0 -and $suspendedAssertStart -gt $suspendedListStart) {
    $awsSource.Substring($suspendedListStart, $suspendedAssertStart - $suspendedListStart)
} else { "" }
$expectedSuspendedReceiptLines = @(
    '$suspendedVersions = Get-AwsEntries $suspendedList.Versions',
    '$suspendedMarkers = Get-AwsEntries $suspendedList.DeleteMarkers',
    '[int]$versionCount = $suspendedVersions.Count',
    '[int]$markerCount = $suspendedMarkers.Count',
    '[int]$nullCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq "null" }).Count',
    '[int]$firstMatchCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq $firstId }).Count',
    '[int]$secondMatchCount = @($suspendedVersions | Where-Object { [string]$_.VersionId -ceq $secondId }).Count',
    'Write-Evidence -Category "assertion" -Value "suspended-version-count=$versionCount"',
    'Write-Evidence -Category "assertion" -Value "suspended-marker-count=$markerCount"',
    'Write-Evidence -Category "assertion" -Value "suspended-null-count=$nullCount"',
    'Write-Evidence -Category "assertion" -Value "suspended-first-match-count=$firstMatchCount"',
    'Write-Evidence -Category "assertion" -Value "suspended-second-match-count=$secondMatchCount"'
)
$receiptLineIndexes = @($expectedSuspendedReceiptLines | ForEach-Object { $suspendedReceiptBlock.IndexOf($_, [StringComparison]::Ordinal) })
Test-SuspendedReceiptContract (
    $receiptLineIndexes.Count -eq 12 -and
    @($receiptLineIndexes | Where-Object { $_ -lt 0 }).Count -eq 0 -and
    (@($receiptLineIndexes | Sort-Object) -join ',') -ceq ($receiptLineIndexes -join ',')
) "Suspended list must emit the exact ordered count receipts before its assertion substage"
Test-SuspendedReceiptContract (
    ([regex]::Matches($awsSource, [regex]::Escape('Get-AwsEntries $suspendedList.Versions'))).Count -eq 1 -and
    ([regex]::Matches($awsSource, [regex]::Escape('Get-AwsEntries $suspendedList.DeleteMarkers'))).Count -eq 1 -and
    $awsSource.Contains('$finalVersions = $suspendedVersions', [StringComparison]::Ordinal) -and
    $awsSource.Contains('$finalMarkers = $suspendedMarkers', [StringComparison]::Ordinal)
) "Suspended versions and markers must be computed once and reused for exact cleanup"
$suspendedAssertionBlock = if ($suspendedAssertStart -ge 0) { $awsSource.Substring($suspendedAssertStart) } else { "" }
Test-SuspendedReceiptContract (
    $suspendedAssertionBlock.Contains('$versionCount -ne 3', [StringComparison]::Ordinal) -and
    $suspendedAssertionBlock.Contains('$markerCount -ne 0', [StringComparison]::Ordinal) -and
    $suspendedAssertionBlock.Contains('$nullCount -ne 1', [StringComparison]::Ordinal) -and
    $suspendedAssertionBlock.Contains('$firstMatchCount -ne 1', [StringComparison]::Ordinal) -and
    $suspendedAssertionBlock.Contains('$secondMatchCount -ne 1', [StringComparison]::Ordinal)
) "Suspended list pass/fail assertion must reuse all five receipt count variables"
Test-SuspendedReceiptContract (
    -not [regex]::IsMatch($suspendedReceiptBlock, 'Write-Evidence[^\r\n]*(?:VersionId|ETag|\$firstId|\$secondId|\$markerId|/work/|<|>)')
) "Suspended count receipts must not expose identifiers, XML, bodies, or paths"
if ($suspendedReceiptFailures.Count -ne 0) {
    throw "Object-versioning suspended-list receipt contracts are missing: $($suspendedReceiptFailures -join '; ')"
}

$awsJsonReceiptFailures = [Collections.Generic.List[string]]::new()
function Test-AwsJsonReceiptContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $awsJsonReceiptFailures.Add($Message) }
}

$safeReceiptAllowlist = '@(0, 1, 2, 252, 253, 254, 255)'
$expectedAwsJsonReceiptEvidence = @(
    'Write-Evidence -Category "diagnostic" -Value "aws-json-command-exit=$($result.ExitCode)"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-parse=passed"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-stdout-lines=$stdoutLineCount"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-stdout-chars=$stdoutCharacterCount"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-shape=$shape"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-versions-property=$versionsPropertyCount"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-markers-property=$markersPropertyCount"',
    'Write-Evidence -Category "diagnostic" -Value "aws-json-parse=failed"'
)
$awsJsonReceiptEvidenceCalls = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -eq "Write-Evidence" -and
        $node.Extent.Text.Contains('aws-json-', [StringComparison]::Ordinal)
}, $true))
$awsJsonReceiptEvidence = @($awsJsonReceiptEvidenceCalls | ForEach-Object { $_.Extent.Text.Trim() })
$invokeAwsJsonFunctionMatches = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq "Invoke-AwsJson"
}, $true))
$normalAwsJsonBranches = if ($invokeAwsJsonFunctionMatches.Count -eq 1) {
    @($invokeAwsJsonFunctionMatches[0].Body.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.IfStatementAst] -and
            $node.Clauses.Count -eq 1 -and
            $node.Clauses[0].Item1.Extent.Text -ceq '[string]::IsNullOrEmpty($SafeReceipt)'
    }, $true))
} else { @() }
$normalAwsJsonSource = if ($normalAwsJsonBranches.Count -eq 1) { $normalAwsJsonBranches[0].Clauses[0].Item2.Extent.Text } else { "" }
$safeReceiptAllowlistIndex = $invokeAwsJsonSource.IndexOf("-AllowedExitCodes $safeReceiptAllowlist", [StringComparison]::Ordinal)
$commandReceiptIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[0], [StringComparison]::Ordinal)
$nonzeroThrowIndex = $invokeAwsJsonSource.IndexOf('throw "AWS JSON receipt request failed"', [StringComparison]::Ordinal)
$stdoutReadIndex = $invokeAwsJsonSource.IndexOf('$json = (@($result.StdOut) -join "`n")', $nonzeroThrowIndex)
$parsePassedIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[1], [StringComparison]::Ordinal)
$stdoutLinesIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[2], [StringComparison]::Ordinal)
$stdoutCharsIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[3], [StringComparison]::Ordinal)
$convertIndex = $invokeAwsJsonSource.IndexOf('$parsedJson = $json | ConvertFrom-Json -NoEnumerate', [StringComparison]::Ordinal)
$shapeIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[4], [StringComparison]::Ordinal)
$versionsPropertyIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[5], [StringComparison]::Ordinal)
$markersPropertyIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[6], [StringComparison]::Ordinal)
$catchIndex = $invokeAwsJsonSource.IndexOf('} catch {', $convertIndex)
$parseFailedIndex = $invokeAwsJsonSource.IndexOf($expectedAwsJsonReceiptEvidence[7], $catchIndex)
$parseRethrowIndex = $invokeAwsJsonSource.IndexOf('throw "AWS CLI returned non-JSON where JSON was required"', $catchIndex)
$parseTryIndex = $invokeAwsJsonSource.LastIndexOf('try {', $convertIndex, [StringComparison]::Ordinal)
$parseTrySource = if ($parseTryIndex -ge 0 -and $catchIndex -gt $parseTryIndex) {
    $invokeAwsJsonSource.Substring($parseTryIndex, $catchIndex - $parseTryIndex)
} else { "" }
Test-AwsJsonReceiptContract (
    ([regex]::Matches($invokeAwsJsonSource, '\[ValidateSet\("suspended-list"\)\]\[string\]\$SafeReceipt\s*=\s*""')).Count -eq 1 -and
    -not [regex]::IsMatch($invokeAwsJsonSource, '\[Parameter\(Mandatory\)\][^\r\n]*\$SafeReceipt')
) "Invoke-AwsJson must use only the optional suspended-list SafeReceipt parameter with an empty default"
Test-AwsJsonReceiptContract (
    $normalAwsJsonBranches.Count -eq 1 -and
    $normalAwsJsonSource.Contains('return $json | ConvertFrom-Json', [StringComparison]::Ordinal) -and
    -not $normalAwsJsonSource.Contains('-NoEnumerate', [StringComparison]::Ordinal) -and
    -not $normalAwsJsonSource.Contains('Write-Evidence', [StringComparison]::Ordinal) -and
    -not $normalAwsJsonSource.Contains('-AllowedExitCodes', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains("-AllowedExitCodes $safeReceiptAllowlist", [StringComparison]::Ordinal)
) "Invoke-AwsJson must keep the IsNullOrEmpty no-receipt path exit-zero-only and receipt-free"
Test-AwsJsonReceiptContract (
    $invokeAwsJsonSource.Contains('if ($SafeReceipt -cne "suspended-list") { throw "AWS JSON receipt is not allowlisted" }', [StringComparison]::Ordinal) -and
    -not $invokeAwsJsonSource.Contains('$null -eq $SafeReceipt', [StringComparison]::Ordinal) -and
    -not $invokeAwsJsonSource.Contains('$null -ne $SafeReceipt', [StringComparison]::Ordinal)
) "Only the literal suspended-list branch may receive safe AWS JSON receipts"
Test-AwsJsonReceiptContract (
    $safeReceiptAllowlistIndex -ge 0 -and
    $commandReceiptIndex -gt $safeReceiptAllowlistIndex -and
    $invokeAwsJsonSource.Substring($safeReceiptAllowlistIndex + ("-AllowedExitCodes $safeReceiptAllowlist").Length).TrimStart().StartsWith($expectedAwsJsonReceiptEvidence[0], [StringComparison]::Ordinal) -and
    $nonzeroThrowIndex -gt $commandReceiptIndex -and
    $stdoutReadIndex -gt $nonzeroThrowIndex
) "Suspended-list command receipt must immediately follow its return and reject nonzero before stdout parsing"
Test-AwsJsonReceiptContract (
    $invokeAwsJsonSource.Contains('$stdoutLineCount = @($result.StdOut).Count', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('$stdoutCharacterCount = $json.Length', [StringComparison]::Ordinal) -and
    $convertIndex -ge 0 -and
    $convertIndex -gt $stdoutReadIndex -and
    $parsePassedIndex -gt $convertIndex -and
    $stdoutLinesIndex -gt $parsePassedIndex -and
    $stdoutCharsIndex -gt $stdoutLinesIndex -and
    $shapeIndex -gt $stdoutCharsIndex -and
    $versionsPropertyIndex -gt $shapeIndex -and
    $markersPropertyIndex -gt $versionsPropertyIndex -and
    $catchIndex -gt $convertIndex -and
    $catchIndex -lt $parsePassedIndex -and
    $parseFailedIndex -gt $catchIndex -and
    $parseRethrowIndex -gt $parseFailedIndex
) "Suspended-list must parse before every successful metric or structural receipt and isolate parse failure"
Test-AwsJsonReceiptContract (
    $parseTrySource.Contains('$parsedJson = $json | ConvertFrom-Json -NoEnumerate', [StringComparison]::Ordinal) -and
    ([regex]::Matches($parseTrySource, 'ConvertFrom-Json')).Count -eq 1 -and
    -not $parseTrySource.Contains('Write-Evidence', [StringComparison]::Ordinal) -and
    -not $parseTrySource.Contains('$shape', [StringComparison]::Ordinal)
) "Only ConvertFrom-Json may be classified as an AWS JSON parse failure"
Test-AwsJsonReceiptContract (
    (@($awsJsonReceiptEvidence | Where-Object { $_ -notlike '*aws-json-parse=failed*' }) -join "`n") -ceq (($expectedAwsJsonReceiptEvidence[0..6]) -join "`n") -and
    (@($awsJsonReceiptEvidence | Where-Object { $_ -like '*aws-json-parse=failed*' }).Count -eq 1) -and
    -not $invokeAwsJsonSource.Contains('StdErr', [StringComparison]::Ordinal) -and
    -not [regex]::IsMatch($invokeAwsJsonSource, '(?m)^\s*(?:Write-Host|Write-Output|Write-Error)\b')
) "AWS JSON receipts must contain only the fixed safe diagnostic grammar without raw output"
Test-AwsJsonReceiptContract (
    $invokeAwsJsonSource.Contains('$trimmedJson = $json.Trim()', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('$shape = if ($trimmedJson -ceq "null")', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('elseif ($trimmedJson.StartsWith("{", [StringComparison]::Ordinal))', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('elseif ($trimmedJson.StartsWith("[", [StringComparison]::Ordinal))', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('"null"', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('"object"', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('"array"', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('"scalar"', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('if ($shape -ceq "object")', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('$propertyNames = @($parsedJson.PSObject.Properties.Name)', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('if ($propertyNames -ccontains "Versions") { 1 } else { 0 }', [StringComparison]::Ordinal) -and
    $invokeAwsJsonSource.Contains('if ($propertyNames -ccontains "DeleteMarkers") { 1 } else { 0 }', [StringComparison]::Ordinal)
) "Suspended-list JSON shape and property receipts must be safe and exhaustive"

$safeReceiptCallPattern = '(?m)^\s*\$suspendedList = Invoke-AwsJson -Network \$Network -RunRoot \$RunRoot -Endpoint \$endpoint -SafeReceipt "suspended-list" -Arguments @\(\s*$'
$safeReceiptCalls = @([regex]::Matches($awsSource, $safeReceiptCallPattern))
$allAwsJsonCallLines = @([regex]::Matches($awsSource, '(?m)^\s*(?:\$[A-Za-z][A-Za-z0-9]*\s*=\s*)?Invoke-AwsJson\b'))
Test-AwsJsonReceiptContract (
    $safeReceiptCalls.Count -eq 1 -and
    ([regex]::Matches($awsSource, '-SafeReceipt\s+"suspended-list"')).Count -eq 1 -and
    $allAwsJsonCallLines.Count -eq 8
) "Only the suspended list call may request the AWS JSON safe receipts"
Test-AwsJsonReceiptContract (
    $awsSource.Contains('if ($null -eq $suspendedList -or $suspendedList -isnot [pscustomobject]) { throw "Suspended list JSON was not an object" }', [StringComparison]::Ordinal) -and
    $awsSource.Contains('$suspendedVersionProperties = @($suspendedList.PSObject.Properties.Match("Versions"))', [StringComparison]::Ordinal) -and
    $awsSource.Contains('if ($suspendedVersionProperties.Count -ne 1) { throw "Suspended list JSON omitted Versions" }', [StringComparison]::Ordinal) -and
    $awsSource.IndexOf('$suspendedVersionProperties = @($suspendedList.PSObject.Properties.Match("Versions"))', [StringComparison]::Ordinal) -lt $awsSource.IndexOf('Get-AwsEntries $suspendedList.Versions', [StringComparison]::Ordinal) -and
    $awsSource.Contains('$suspendedMarkerProperties = @($suspendedList.PSObject.Properties.Match("DeleteMarkers"))', [StringComparison]::Ordinal) -and
    $awsSource.Contains('if ($suspendedMarkerProperties.Count -eq 0) {', [StringComparison]::Ordinal) -and
    $awsSource.Contains('$suspendedMarkers = @()', [StringComparison]::Ordinal) -and
    $awsSource.Contains('$suspendedMarkers = Get-AwsEntries $suspendedList.DeleteMarkers', [StringComparison]::Ordinal)
) "Suspended list must reject unsafe JSON shapes, require Versions, and map missing DeleteMarkers to empty"
Test-AwsJsonReceiptContract (
    ($actualSnapshotStages -join ',') -ceq ($snapshotStages -join ',') -and
    $snapshotCalls.Count -eq 7 -and
    $receiptLineIndexes.Count -eq 12 -and
    @($receiptLineIndexes | Where-Object { $_ -lt 0 }).Count -eq 0 -and
    $suspendedAssertionBlock.Contains('$versionCount -ne 3', [StringComparison]::Ordinal) -and
    $suspendedAssertionBlock.Contains('$secondMatchCount -ne 1', [StringComparison]::Ordinal)
) "Optional AWS JSON receipts must not change DB snapshots, suspended count receipts, or list assertions"
if ($awsJsonReceiptFailures.Count -ne 0) {
    throw "Object-versioning AWS JSON receipt contracts are missing: $($awsJsonReceiptFailures -join '; ')"
}

$emptyListContractFailures = [Collections.Generic.List[string]]::new()
function Test-EmptyListContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $emptyListContractFailures.Add($Message) }
}

$emptyListStart = $awsSource.IndexOf('$emptyList = Invoke-AwsJson', [StringComparison]::Ordinal)
$emptyListDeleteBucketStart = $awsSource.IndexOf('Write-AwsSubstage -Label "delete-bucket"', [StringComparison]::Ordinal)
$emptyListBlock = if ($emptyListStart -ge 0 -and $emptyListDeleteBucketStart -gt $emptyListStart) {
    $awsSource.Substring($emptyListStart, $emptyListDeleteBucketStart - $emptyListStart)
} else { "" }
$emptyObjectGuard = 'if ($null -eq $emptyList -or $emptyList -isnot [pscustomobject]) { throw "Empty version list JSON was not an object" }'
$emptyVersionMatch = '$emptyVersionProperties = @($emptyList.PSObject.Properties.Match("Versions"))'
$emptyMarkerMatch = '$emptyMarkerProperties = @($emptyList.PSObject.Properties.Match("DeleteMarkers"))'
$emptyVersionPresent = '$emptyVersions = Get-AwsEntries $emptyList.Versions'
$emptyMarkerPresent = '$emptyMarkers = Get-AwsEntries $emptyList.DeleteMarkers'
$emptyVersionDuplicate = 'throw "Empty version list JSON has duplicate Versions"'
$emptyMarkerDuplicate = 'throw "Empty version list JSON has duplicate DeleteMarkers"'
$emptyObjectGuardIndex = $emptyListBlock.IndexOf($emptyObjectGuard, [StringComparison]::Ordinal)
$emptyVersionMatchIndex = $emptyListBlock.IndexOf($emptyVersionMatch, [StringComparison]::Ordinal)
$emptyMarkerMatchIndex = $emptyListBlock.IndexOf($emptyMarkerMatch, [StringComparison]::Ordinal)
$emptyVersionPresentIndex = $emptyListBlock.IndexOf($emptyVersionPresent, [StringComparison]::Ordinal)
$emptyMarkerPresentIndex = $emptyListBlock.IndexOf($emptyMarkerPresent, [StringComparison]::Ordinal)
$emptyVersionPresentGuardIndex = $emptyListBlock.IndexOf('elseif ($emptyVersionProperties.Count -eq 1) {', [StringComparison]::Ordinal)
$emptyMarkerPresentGuardIndex = $emptyListBlock.IndexOf('elseif ($emptyMarkerProperties.Count -eq 1) {', [StringComparison]::Ordinal)
$emptyVersionDuplicateIndex = $emptyListBlock.IndexOf($emptyVersionDuplicate, [StringComparison]::Ordinal)
$emptyMarkerDuplicateIndex = $emptyListBlock.IndexOf($emptyMarkerDuplicate, [StringComparison]::Ordinal)
Test-EmptyListContract (
    $emptyObjectGuardIndex -ge 0 -and
    $emptyVersionMatchIndex -gt $emptyObjectGuardIndex -and
    $emptyMarkerMatchIndex -gt $emptyVersionMatchIndex
) "Empty list must validate a nonnull JSON object before property inspection"
Test-EmptyListContract (
    $emptyListBlock.Contains('if ($emptyVersionProperties.Count -eq 0) {', [StringComparison]::Ordinal) -and
    $emptyListBlock.Contains('$emptyVersions = @()', [StringComparison]::Ordinal) -and
    $emptyListBlock.Contains('elseif ($emptyVersionProperties.Count -eq 1) {', [StringComparison]::Ordinal) -and
    $emptyVersionPresentGuardIndex -gt $emptyVersionMatchIndex -and
    $emptyVersionPresentIndex -gt $emptyVersionPresentGuardIndex -and
    $emptyVersionDuplicateIndex -gt $emptyVersionPresentIndex -and
    $emptyListBlock.Contains('if ($emptyMarkerProperties.Count -eq 0) {', [StringComparison]::Ordinal) -and
    $emptyListBlock.Contains('$emptyMarkers = @()', [StringComparison]::Ordinal) -and
    $emptyListBlock.Contains('elseif ($emptyMarkerProperties.Count -eq 1) {', [StringComparison]::Ordinal) -and
    $emptyMarkerPresentGuardIndex -gt $emptyMarkerMatchIndex -and
    $emptyMarkerPresentIndex -gt $emptyMarkerPresentGuardIndex -and
    $emptyMarkerDuplicateIndex -gt $emptyMarkerPresentIndex
) "Empty list must map absent properties to empty arrays, read exactly one property, and reject duplicates"
Test-EmptyListContract (
    ([regex]::Matches($emptyListBlock, '\$emptyList\.Versions')).Count -eq 1 -and
    ([regex]::Matches($emptyListBlock, '\$emptyList\.DeleteMarkers')).Count -eq 1 -and
    $emptyListBlock.Contains('if ($emptyVersions.Count -ne 0 -or $emptyMarkers.Count -ne 0) {', [StringComparison]::Ordinal) -and
    $emptyListBlock.Contains('throw "Version list was not empty after exact deletes"', [StringComparison]::Ordinal) -and
    -not $emptyListBlock.Contains('Write-Evidence', [StringComparison]::Ordinal)
) "Empty list must only access guarded present properties and retain the strict zero assertion without receipts"
Test-EmptyListContract (
    $awsSource.Contains('if ($suspendedVersionProperties.Count -ne 1) { throw "Suspended list JSON omitted Versions" }', [StringComparison]::Ordinal) -and
    $awsSource.Contains('if ($suspendedMarkerProperties.Count -eq 0) {', [StringComparison]::Ordinal) -and
    ($actualSnapshotStages -join ',') -ceq ($snapshotStages -join ',') -and
    $snapshotCalls.Count -eq 7
) "Empty-list correction must not alter suspended optional-property handling or DB snapshots"
if ($emptyListContractFailures.Count -ne 0) {
    throw "Object-versioning empty-list JSON contracts are missing: $($emptyListContractFailures -join '; ')"
}

$awsErrorContractFailures = [Collections.Generic.List[string]]::new()
function Test-AwsErrorContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $awsErrorContractFailures.Add($Message) }
}

$awsExpectedFailureSource = Get-VersioningRunnerFunctionSource "Assert-AwsExpectedFailure"
$awsErrorParserMatches = @($VersioningRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq "Get-AwsCliErrorCode"
}, $true))
$awsErrorParserSource = if ($awsErrorParserMatches.Count -eq 1) { $awsErrorParserMatches[0].Extent.Text } else { "" }
Test-AwsErrorContract (
    $awsExpectedFailureSource.Contains('-Arguments (@("--cli-error-format", "json") + $Arguments)', [StringComparison]::Ordinal) -and
    $awsExpectedFailureSource.Contains('if ($result.ExitCode -eq 0)', [StringComparison]::Ordinal) -and
    $awsExpectedFailureSource.Contains('Get-AwsCliErrorCode -Stderr $result.StdErr', [StringComparison]::Ordinal) -and
    $awsExpectedFailureSource.Contains('Write-Evidence -Category "assertion" -Value "aws-error-code=$code"', [StringComparison]::Ordinal) -and
    -not $awsExpectedFailureSource.Contains('ExpectedClass', [StringComparison]::Ordinal) -and
    -not $awsExpectedFailureSource.Contains('ExpectedStatus', [StringComparison]::Ordinal) -and
    -not $awsExpectedFailureSource.Contains('aws-error=', [StringComparison]::Ordinal)
) "Expected AWS failures must request CLI JSON, require nonzero exit, and evidence only the parsed Code"
Test-AwsErrorContract (
    $awsErrorParserMatches.Count -eq 1 -and
    $awsErrorParserSource.Contains('ConvertFrom-Json -NoEnumerate', [StringComparison]::Ordinal) -and
    $awsErrorParserSource.Contains('$document.Code', [StringComparison]::Ordinal) -and
    $awsErrorParserSource.Contains('$code -isnot [string]', [StringComparison]::Ordinal) -and
    $awsErrorParserSource.Contains('"NoSuchKey", "404", "405"', [StringComparison]::Ordinal) -and
    $awsErrorParserSource.Contains('$documentText.Length -gt 8192', [StringComparison]::Ordinal) -and
    -not $awsErrorParserSource.Contains('Error.Code', [StringComparison]::Ordinal) -and
    -not $awsErrorParserSource.Contains('Message', [StringComparison]::Ordinal) -and
    -not $awsErrorParserSource.Contains('Write-', [StringComparison]::Ordinal)
) "AWS CLI error parser must read one bounded top-level string Code without raw output"
foreach ($fragment in @(
    '-ExpectedCode "NoSuchKey"',
    '-ExpectedCode "404"',
    '-ExpectedCode "405"'
)) {
    Test-AwsErrorContract $awsSource.Contains($fragment, [StringComparison]::Ordinal) "AWS expected-failure call site is missing: $fragment"
}
if ($awsErrorParserMatches.Count -eq 1) {
    Invoke-Expression $awsErrorParserSource
    $awsCliErrorFixtures = @(
        [pscustomobject]@{ Code = "NoSuchKey"; Message = "fixture missing object" },
        [pscustomobject]@{ Code = "404"; Message = "fixture current head" },
        [pscustomobject]@{ Code = "405"; Message = "fixture marker head" }
    )
    Assert-True ($awsCliErrorFixtures.Count -eq 3) "AWS CLI error fixtures must contain exactly three top-level Code cases"
    foreach ($fixture in $awsCliErrorFixtures) {
        $fixtureDocument = @{ Code = $fixture.Code; Message = $fixture.Message } | ConvertTo-Json -Compress
        Assert-True ((Get-AwsCliErrorCode -Stderr @($fixtureDocument)) -ceq $fixture.Code) "AWS CLI error parser must return only fixture Code"
    }
    foreach ($invalidDocument in @(
        '{"Message":"missing"}',
        '{"Code":"Unknown","Message":"unknown"}',
        '{"Code":404,"Message":"non-string"}',
        '{"Code":"NoSuchKey"',
        "{`"Code`":`"NoSuchKey`"}`n{`"Code`":`"404`"}",
        '[{"Code":"NoSuchKey"}]'
    )) {
        $rejected = $false
        try {
            $null = Get-AwsCliErrorCode -Stderr @($invalidDocument)
        } catch {
            $rejected = $true
        }
        Assert-True $rejected "AWS CLI error parser accepted an invalid document"
    }
}
if ($awsErrorContractFailures.Count -ne 0) {
    throw "Object-versioning AWS error contracts are missing: $($awsErrorContractFailures -join '; ')"
}
$evidenceSource = Get-VersioningRunnerFunctionSource "Write-Evidence"
foreach ($fragment in @("aws-cli", "gateway-image-id", "package=0.1.0", "spec-sha256", "postgres-versioning", "e2e", "cleanup")) {
    Assert-Contains ($evidenceSource + $VersioningRunnerSource) $fragment "Sanitized evidence field missing: $fragment"
}
Assert-NotContains $VersioningRunnerSource "Start-Transcript" "Runner must not capture unsanitized transcripts"
$cleanupSource = Get-VersioningRunnerFunctionSource "Remove-OwnedVersioningResources"
$cleanupResidualSource = Get-VersioningRunnerFunctionSource "Test-VersioningCleanupResiduals"
foreach ($fragment in @('$State.ProjectOwned', '$State.GatewayImageOwned', '$State.RunRootReceiptOwned', '"image", "rm", $State.GatewayImage', 'Remove-Item -LiteralPath "Env:$name"')) {
    Assert-Contains ($cleanupSource + (Get-VersioningRunnerFunctionSource "Restore-EnvironmentState")) $fragment "Owned cleanup or exact environment restoration missing: $fragment"
}
foreach ($fragment in @(
    'label=com.docker.compose.project=$($State.Project)',
    '"ps", "-aq"',
    '"network", "ls", "-q"',
    '"volume", "ls", "-q"',
    'Test-LocalImage -Image $State.GatewayImage',
    'Name = "containers"',
    'Name = "networks"',
    'Name = "volumes"',
    'residual-$($query.Name)=',
    'residual-image=',
    'query-failed',
    '=nonzero',
    '=zero'
)) {
    Assert-Contains $cleanupResidualSource $fragment "Independent cleanup residual proof is missing: $fragment"
}
foreach ($forbidden in @("system prune", '"image", "rm", "-f"', "Remove-Item -Path", "Remove-Item *")) {
    Assert-NotContains $VersioningRunnerSource $forbidden "Runner contains broad cleanup: $forbidden"
}
$versioningDirectNative = @($VersioningRunnerAst.FindAll({
    param($node)
    if ($node -isnot [System.Management.Automation.Language.CommandAst]) { return $false }
    $name = $node.GetCommandName()
    return $name -in @("docker", "cargo", "tar.exe")
}, $true))
Assert-True ($versioningDirectNative.Count -eq 0) "Object-versioning runner has a direct native entrypoint: $($versioningDirectNative.Extent.Text -join '; ')"

Assert-NotContains $VersioningComposeSource "container_name:" "Object-versioning Compose must not fix container names"
Assert-NotContains $VersioningComposeSource "cloudflared:" "Object-versioning Compose must not include cloudflared"
Assert-True (-not [regex]::IsMatch($VersioningComposeSource, '(?m)^name:\s*')) "Object-versioning Compose must not set a top-level name"
$servicesMatch = [regex]::Match($VersioningComposeSource, '(?ms)^services:\n(?<body>.*?)(?=^volumes:\n)')
Assert-True $servicesMatch.Success "Object-versioning Compose must have services followed by volumes"
$serviceNames = @([regex]::Matches($servicesMatch.Groups["body"].Value, '(?m)^  ([a-z0-9_-]+):\s*$') | ForEach-Object { $_.Groups[1].Value })
Assert-True (($serviceNames -join ",") -ceq "postgres,kubo,gateway") "Object-versioning Compose services must be exactly postgres,kubo,gateway"
foreach ($line in @(
    '      - "127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_POSTGRES_PORT:?required}:5432"',
    '      - "127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_KUBO_PORT:?required}:5001"',
    '      - "127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_GATEWAY_PORT:?required}:9000"',
    '    image: "${IPFS_S3_OBJECT_VERSION_IMAGE:?required}"',
    '    image: postgres:17',
    '    image: ghcr.io/hugefiver/ipfs3-kubo:latest',
    '      IPFS_S3_DATABASE_URL: "postgres://ipfs3:ipfs3@postgres:5432/ipfs3"',
    '      IPFS_S3_ACCESS_KEY_ID: test',
    '      IPFS_S3_SECRET_ACCESS_KEY: test',
    '  postgres_data:',
    '  kubo_data:'
)) {
    Assert-Contains $VersioningComposeSource $line "Object-versioning Compose contract missing: $line"
}
Assert-True (([regex]::Matches($VersioningComposeSource, '(?m)^      - "127\.0\.0\.1:')).Count -eq 3) "Object-versioning Compose must publish exactly three loopback ports"
Assert-True (([regex]::Matches($VersioningComposeSource, '(?m)^        condition: service_healthy\s*$')).Count -eq 2) "Gateway must health-gate PostgreSQL and Kubo"

$versioningCorrectionFailures = [Collections.Generic.List[string]]::new()
function Test-VersioningCorrectionContract {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) { $versioningCorrectionFailures.Add($Message) }
}

$nativeCommandSource = Get-VersioningRunnerFunctionSource "Invoke-NativeCommand"
$composeSource = Get-VersioningRunnerFunctionSource "Invoke-Compose"
$mainSource = Get-VersioningRunnerFunctionSource "Invoke-VersioningMain"
$cleanupSource = Get-VersioningRunnerFunctionSource "Remove-OwnedVersioningResources"
$cleanupResidualSource = Get-VersioningRunnerFunctionSource "Test-VersioningCleanupResiduals"
$newRootSource = Get-VersioningRunnerFunctionSource "New-VersioningRunRoot"
$topLevelSource = $VersioningRunnerSource

Test-VersioningCorrectionContract (
    $offlineSource.Contains('if ((Test-Path -LiteralPath $vendorPath) -or (Test-Path -LiteralPath $archiveContext))', [StringComparison]::Ordinal) -and
    -not $offlineSource.Contains('if (Test-Path -LiteralPath $vendorPath -or Test-Path -LiteralPath $archiveContext)', [StringComparison]::Ordinal)
) "Offline build must parenthesize both Test-Path operands before -or"
Test-VersioningCorrectionContract (
    $nativeCommandSource.Contains('[string]$WorkingDirectory', [StringComparison]::Ordinal) -and
    $nativeCommandSource.Contains('Get-CanonicalExistingDirectory -Path $WorkingDirectory', [StringComparison]::Ordinal) -and
    $offlineSource.Contains('-WorkingDirectory $RepoRoot', [StringComparison]::Ordinal)
) "Cargo vendor must use the canonical existing RepoRoot working directory through Invoke-NativeCommand"

$expectedDisposableMasterKey = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
$masterKeyMatches = @([regex]::Matches($VersioningComposeSource, '(?m)^      IPFS_S3_MASTER_KEY: "(?<key>[0-9a-f]{64})"\s*$'))
Test-VersioningCorrectionContract (
    $masterKeyMatches.Count -eq 1 -and
    $masterKeyMatches[0].Groups['key'].Value -ceq $expectedDisposableMasterKey -and
    $masterKeyMatches[0].Groups['key'].Value -cnotmatch '^0{64}$'
) "Object-versioning Compose must use the exact nonzero 64-lowercase-hex disposable master key"
Test-VersioningCorrectionContract (
    $topLevelSource.Contains('function Set-VersioningStage', [StringComparison]::Ordinal) -and
    $topLevelSource.Contains('[ValidateSet("preflight", "config", "offline-build", "compose-up", "health", "network", "metadata", "postgres", "e2e", "aws", "cleanup")]', [StringComparison]::Ordinal) -and
    $topLevelSource.Contains('$State.Stage = $Stage', [StringComparison]::Ordinal) -and
    $topLevelSource.Contains('failed-stage=$($state.Stage)', [StringComparison]::Ordinal) -and
    -not $mainSource.Contains('Exception.Message', [StringComparison]::Ordinal)
) "Runner must keep an allowlisted stage state and emit only failed-stage evidence from its catch"
Test-VersioningCorrectionContract (
    $topLevelSource.Contains('function New-VersioningOwnershipReceipt', [StringComparison]::Ordinal) -and
    $topLevelSource.Contains('function Remove-OwnedEmptyVersioningRunRoot', [StringComparison]::Ordinal) -and
    $mainSource.Contains('$State.RunRootDirectoryOwned = $true', [StringComparison]::Ordinal) -and
    $cleanupSource.Contains('$State.RunRootDirectoryOwned', [StringComparison]::Ordinal)
) "RunRoot directory and ownership receipt must have separate partial-ownership cleanup seams"
$offlineBuildStageIndex = $mainSource.IndexOf('Set-VersioningStage -State $State -Stage "offline-build"', [StringComparison]::Ordinal)
$offlineBuildIndex = $mainSource.IndexOf('Invoke-OfflineGatewayBuild -RunRoot $State.RunRoot -GatewayImage $State.GatewayImage', $offlineBuildStageIndex, [StringComparison]::Ordinal)
$postBuildProbeIndex = $mainSource.IndexOf('$postBuildGatewayImage = Test-LocalImage -Image $State.GatewayImage', $offlineBuildIndex, [StringComparison]::Ordinal)
$imageOwnershipIndex = $mainSource.IndexOf('$State.GatewayImageOwned = $true', $postBuildProbeIndex, [StringComparison]::Ordinal)
Test-VersioningCorrectionContract (
    $offlineBuildStageIndex -ge 0 -and
    $offlineBuildIndex -gt $offlineBuildStageIndex -and
    $postBuildProbeIndex -gt $offlineBuildIndex -and
    $imageOwnershipIndex -gt $postBuildProbeIndex -and
    $mainSource.Substring($offlineBuildStageIndex, $postBuildProbeIndex - $offlineBuildStageIndex).Contains('try {', [StringComparison]::Ordinal) -and
    $mainSource.Substring($offlineBuildIndex, $postBuildProbeIndex - $offlineBuildIndex + '$postBuildGatewayImage = Test-LocalImage -Image $State.GatewayImage'.Length).Contains('finally {', [StringComparison]::Ordinal)
) "Gateway image ownership must be claimed only from the exact post-build/failure probe in finally"
Test-VersioningCorrectionContract (
    $topLevelSource.Contains('function Claim-VersioningProjectOwnership', [StringComparison]::Ordinal) -and
    $mainSource.Contains('finally {', [StringComparison]::Ordinal) -and
    $mainSource.Contains('Claim-VersioningProjectOwnership -State $State', [StringComparison]::Ordinal)
) "Compose up attempts must independently claim exact-project resources in finally"
Test-VersioningCorrectionContract (
    $mainSource.Contains('$State.EnvironmentStateOwned = $true', [StringComparison]::Ordinal) -and
    $topLevelSource.Contains('if ($state.EnvironmentStateOwned)', [StringComparison]::Ordinal)
) "Environment snapshots must become owned immediately and always restore"
foreach ($fragment in @(
    'stage=$Stage',
    'diagnostics=attempted',
    'compose-down=passed',
    'compose-down=not-owned',
    'compose-down=failed',
    'project-ownership-retry=owned',
    'project-ownership-retry=absent',
    'project-ownership-retry=failed',
    'project-ownership-retry=not-needed',
    'image-remove=passed',
    'image-remove=not-owned',
    'image-remove=failed',
    'temp-remove=passed',
    'temp-remove=not-owned',
    'temp-remove=failed',
    'environment-restore=passed',
    'environment-restore=not-needed',
    'environment-restore=failed',
    'Name = "containers"',
    'Name = "networks"',
    'Name = "volumes"',
    'residual-$($query.Name)=',
    'residual-image=',
    'cleanup-errors='
)) {
    Test-VersioningCorrectionContract $topLevelSource.Contains($fragment, [StringComparison]::Ordinal) "Cleanup/stage evidence is missing: $fragment"
}
$projectOwnershipRetryCondition = 'if (-not $State.Blocked -and -not [string]::IsNullOrWhiteSpace($State.Project) -and -not $State.ProjectOwned -and $State.ProjectOwnershipProbeFailed) {'
$projectOwnershipRetryIndex = $cleanupSource.IndexOf($projectOwnershipRetryCondition, [StringComparison]::Ordinal)
$projectOwnershipProbeIndex = $cleanupSource.IndexOf('Claim-VersioningProjectOwnership -State $State', [Math]::Max(0, $projectOwnershipRetryIndex), [StringComparison]::Ordinal)
$projectOwnershipClaimIndex = $cleanupSource.IndexOf('$State.ProjectOwned', [Math]::Max(0, $projectOwnershipProbeIndex), [StringComparison]::Ordinal)
$composeDownIndex = $cleanupSource.IndexOf('compose-down=passed', [StringComparison]::Ordinal)
$fallbackImageOwnershipCondition = 'if (-not $State.Blocked -and -not [string]::IsNullOrWhiteSpace($State.GatewayImage) -and -not $State.GatewayImageOwned) {'
$fallbackImageOwnershipIndex = $cleanupSource.IndexOf($fallbackImageOwnershipCondition, [StringComparison]::Ordinal)
$fallbackImageProbeIndex = $cleanupSource.IndexOf('$fallbackGatewayImage = Test-LocalImage -Image $State.GatewayImage', [Math]::Max(0, $fallbackImageOwnershipIndex), [StringComparison]::Ordinal)
$fallbackImageClaimIndex = $cleanupSource.IndexOf('$State.GatewayImageOwned = $true', [Math]::Max(0, $fallbackImageProbeIndex), [StringComparison]::Ordinal)
$imageRemovalIndex = $cleanupSource.IndexOf('image-remove=passed', [StringComparison]::Ordinal)
$residualProofIndex = $cleanupSource.IndexOf('Test-VersioningCleanupResiduals -State $State', [StringComparison]::Ordinal)
Test-VersioningCorrectionContract (
    $projectOwnershipRetryIndex -ge 0 -and
    $projectOwnershipProbeIndex -gt $projectOwnershipRetryIndex -and
    $projectOwnershipClaimIndex -gt $projectOwnershipProbeIndex -and
    $composeDownIndex -gt $projectOwnershipClaimIndex -and
    ([regex]::Matches($cleanupSource, 'project-ownership-retry=(?:owned|absent|failed|not-needed)')).Count -eq 4 -and
    $fallbackImageOwnershipIndex -gt $composeDownIndex -and
    $fallbackImageProbeIndex -gt $fallbackImageOwnershipIndex -and
    $fallbackImageClaimIndex -gt $fallbackImageProbeIndex -and
    $imageRemovalIndex -gt $fallbackImageClaimIndex -and
    $residualProofIndex -gt $imageRemovalIndex -and
    $cleanupSource.Contains('Write-Evidence -Category "cleanup" -Value "image-ownership-probe=failed"', [StringComparison]::Ordinal) -and
    $cleanupSource.Contains('$errors.Add("image-ownership-probe")', [StringComparison]::Ordinal) -and
    -not $cleanupSource.Contains('Assert-ProjectResourcesAbsent', [StringComparison]::Ordinal) -and
    -not $cleanupSource.Contains('"image", "rm", "*"', [StringComparison]::Ordinal) -and
    -not $cleanupSource.Contains('"image", "rm", "-f"', [StringComparison]::Ordinal)
) "Cleanup must retry uncertain exact-project ownership before Compose teardown, then image cleanup and independent residual proof"
Test-VersioningCorrectionContract (
    $topLevelSource.Contains('$ComposeStartupTimeout = [TimeSpan]::FromMinutes(6)', [StringComparison]::Ordinal) -and
    $composeSource.Contains('[TimeSpan]$Timeout = $DockerCommandTimeout', [StringComparison]::Ordinal) -and
    $mainSource.Contains('-Timeout $ComposeStartupTimeout', [StringComparison]::Ordinal)
) "Compose startup must use a bounded timeout with margin beyond its 300-second wait"
if ($versioningCorrectionFailures.Count -ne 0) {
    throw "Object-versioning deterministic live-run contracts are missing: $($versioningCorrectionFailures -join '; ')"
}

$expectedVersioningEvidenceLines = @(
    'OBJECT VERSIONING REAL CLIENT: PASSED',
    'Validated: 2026-08-26',
    'Spec SHA-256: 56e53b0983ac9f244c5379a50454c6f7e64ab4105ceccc45f9c5ed5cf34ef489',
    'Package: 0.1.0',
    'Git HEAD: 59051485b4e9efe52121d9a213b82b8c8a6f5684',
    'AWS CLI: 2.36.9',
    'AWS CLI image: sha256:00dee8ccaf669721f8163944ea9e6ac13183851b23d5b7a9868a608bc932cfdb',
    'postgres_versioning=PASSED',
    'existing_e2e=PASSED',
    'enable_two_writes_delete_list=PASSED',
    'historical_get=PASSED',
    'marker_404_405=PASSED',
    'exact_marker_delete=PASSED',
    'suspended_null_overwrite=PASSED',
    'version_aware_cleanup=PASSED',
    'cleanup_verification=PASSED',
    'HOSTED object-versioning: NOT RUN'
)
$expectedVersioningEvidence = ($expectedVersioningEvidenceLines -join "`n") + "`n"
Assert-True ($VersioningEvidenceRaw -ceq $expectedVersioningEvidence) "Object-versioning evidence must be the exact final sanitized LOCAL receipt"
Assert-True ($VersioningEvidenceBytes.Count -lt 3 -or -not (
    $VersioningEvidenceBytes[0] -eq 0xef -and
    $VersioningEvidenceBytes[1] -eq 0xbb -and
    $VersioningEvidenceBytes[2] -eq 0xbf
)) "Object-versioning evidence must be UTF-8 without a BOM"
Assert-True (-not $VersioningEvidenceRaw.Contains("`r", [StringComparison]::Ordinal)) "Object-versioning evidence must use portable LF line endings"
Assert-True ([regex]::IsMatch($VersioningEvidenceRaw, '\A[\x20-\x7E\n]*\z')) "Object-versioning evidence must contain only portable sanitized text"
foreach ($forbidden in @(
    'gateway-image-id', 'failed-stage=', 'aws-json-', 'db-snapshot=', 'ownership-receipt',
    'AWS_SECRET_ACCESS_KEY', 'IPFS_S3_', 'Authorization:', 'VersionId', '<', '>',
    'SELECT ', 'INSERT ', 'UPDATE ', 'DELETE ', '/work/', '\\'
)) {
    Assert-NotContains $VersioningEvidenceRaw $forbidden "Object-versioning evidence contains a forbidden raw identifier or diagnostic: $forbidden"
}

$expectedVersioningFeatureBullet = '- **Object Versioning** — Unversioned, Enabled, and Suspended bucket states with S3-style version IDs, delete markers, and `ListObjectVersions`'
$featuresStart = $ReadmeSource.IndexOf("## Features`n", [StringComparison]::Ordinal)
$quickStartStart = $ReadmeSource.IndexOf("## Quick Start`n", [StringComparison]::Ordinal)
$featuresSource = if ($featuresStart -ge 0 -and $quickStartStart -gt $featuresStart) {
    $ReadmeSource.Substring($featuresStart, $quickStartStart - $featuresStart)
} else { '' }
Assert-True (([regex]::Matches($featuresSource, [regex]::Escape($expectedVersioningFeatureBullet))).Count -eq 1) "README Features must contain exactly one object-versioning bullet"
Assert-True (([regex]::Matches($ReadmeSource, '(?m)^## Object versioning$')).Count -eq 1) "README must contain exactly one Object versioning section"
$ReadmeContractSource = [regex]::Replace($ReadmeSource, '\s+', ' ').Trim()
foreach ($fragment in @(
    '[approved design](docs/superpowers/specs/2026-08-25-object-versioning-design.md)',
    '[sanitized LOCAL evidence](docs/object-versioning-evidence-2026-08-25.log)',
    '**Unversioned** buckets overwrite the current object; **Enabled** assigns opaque VersionIds to each write and can thereafter only be **Suspended**; **Suspended** overwrites the literal `null` version while retaining prior opaque versions.',
    'Deletes in versioned states create a delete marker. A current read of a marker returns `404` (`NoSuchKey`); `HeadObject` or `GetObject` targeting that marker explicitly returns `405`. Deleting the exact marker restores the previous version.',
    '`GetObject`, `HeadObject`, `CopyObject`, object tagging, and `DeleteObject` support current or exact-version requests.',
    '`ListObjectVersions` returns versions and delete markers in combined order; its key-marker and version-id-marker pagination continue that same order.',
    '`PutObject`, `CopyObject`, completed multipart uploads, `ipfs3-import`, and ZIP extraction all publish version-aware objects.',
    'Each version retains `ETag = CID` and its encryption metadata. Deleting a version removes only public metadata: gateway Kubo pins are retained and `pin/rm` is not called.',
    'Bucket deletion requires exact removal of every public version and delete marker.',
    'Non-goals: MFA Delete, Object Lock, pin reclamation, and replication.'
)) {
    Assert-Contains $ReadmeContractSource $fragment "README object-versioning contract is missing: $fragment"
}
Assert-True (([regex]::Matches($ReadmeSource, '(?m)^## Lifecycle expiration$')).Count -eq 1) "README must contain exactly one Lifecycle expiration section"
foreach ($fragment in @(
    '[approved expiration design](docs/superpowers/specs/2026-08-26-lifecycle-expiration-design.md)',
    '[sanitized LOCAL evidence](docs/lifecycle-expiration-evidence-2026-08-26.log)',
    '`PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, and `DeleteBucketLifecycle` support strict, atomic replacement of expiration rules with expected-owner enforcement.',
    'Supported actions are current-version `Expiration` by date or days, `NoncurrentVersionExpiration` for content and delete markers, and `ExpiredObjectDeleteMarker`.',
    'Eligibility uses database UTC and UTC-midnight semantics.',
    'Lifecycle deletion retains Kubo pins and never calls `pin/rm`.',
    '`AbortIncompleteMultipartUpload` is supported with all-objects or prefix selectors.',
    '`DaysAfterInitiation=N` becomes due at the next UTC midnight after N full days',
    'an explicit abort of an absent upload still returns `NoSuchUpload`',
    'a lifecycle action observing the same absence succeeds idempotently.',
    'Abort response headers (`x-amz-abort-date`, `x-amz-abort-rule-id`) and',
    '`ListMultipartUploads` are not implemented.',
    '`Transition` and `NoncurrentVersionTransition` remain unsupported'
)) {
    Assert-Contains $ReadmeContractSource $fragment "README lifecycle expiration contract is missing: $fragment"
}

$expectedRoadmapVersioningSection = @'
## v0.6 — Versioning & Lifecycle

- [x] Object versioning (enable/suspend on bucket)
- [x] ListObjectVersions
- [x] DeleteMarker support
- [ ] Lifecycle rules (expiration, transition)
- [x] Bucket CORS configuration
'@
$roadmapVersioningSection = [regex]::Match($RoadmapSource, '(?ms)^## v0\.6 — Versioning & Lifecycle\n.*?(?=^## \S|\z)').Value.TrimEnd("`n")
Assert-True ($roadmapVersioningSection -ceq $expectedRoadmapVersioningSection.TrimEnd("`n")) "ROADMAP v0.6 must promote Bucket CORS while retaining Lifecycle as unchecked"
Assert-True (([regex]::Matches($RoadmapSource, '(?m)^- \[x\] (?:Object versioning \(enable/suspend on bucket\)|ListObjectVersions|DeleteMarker support|Bucket CORS configuration)$')).Count -eq 4) "ROADMAP must contain exactly four completed versioning and CORS checkboxes"
Assert-True (([regex]::Matches($CargoManifestSource, '(?m)^version = "0\.1\.0"$')).Count -eq 1) "Cargo package version must remain 0.1.0"

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
        $versioningDryResult = Invoke-NativeCommand `
            -FilePath $pwshPath `
            -ArgumentList @("-NoProfile", "-File", $VersioningRunnerPath) `
            -Label "object-versioning runner no-run contract" `
            -Timeout ([TimeSpan]::FromSeconds(20))
        Assert-True ($versioningDryResult.ExitCode -eq 0) "Object-versioning no-Run contract was nonzero"
        Assert-True ((@($versioningDryResult.StdOut) -join "`n") -ceq "[RESULT] object-versioning-client=NOT RUN reason=execution-not-requested") "Object-versioning no-Run result changed or touched Docker"

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

# Bucket CORS Task 6 is a dependency-free safety contract.  Its presence is
# deliberately checked before its artifacts are read so the first run records
# the causal RED instead of an incidental parser or existing-runner failure.
$CorsRunnerPath = Join-Path $RepoRoot "scripts/bucket-cors-smoke.ps1"
$CorsComposePath = Join-Path $RepoRoot "tests/compose.cors-validation.yml"
$CorsEvidencePath = Join-Path $RepoRoot "docs/bucket-cors-evidence-2026-08-31.log"
$missingCorsContracts = @(
    @($CorsRunnerPath, $CorsComposePath, $CorsEvidencePath) | Where-Object {
        -not (Test-Path -LiteralPath $_ -PathType Leaf)
    }
)
if ($missingCorsContracts.Count -ne 0) {
    throw "Bucket CORS static contracts are missing: $($missingCorsContracts -join '; ')"
}

$corsTokens = $null
$corsParseErrors = $null
$CorsRunnerAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $CorsRunnerPath,
    [ref]$corsTokens,
    [ref]$corsParseErrors
)
if ($corsParseErrors.Count -ne 0) {
    $corsParseErrors | Format-List | Out-String | Write-Host
    throw "scripts/bucket-cors-smoke.ps1 has parse errors"
}
$CorsRunnerSource = [IO.File]::ReadAllText($CorsRunnerPath)
$CorsComposeSource = [IO.File]::ReadAllText($CorsComposePath).Replace("`r`n", "`n").Replace("`r", "`n")
$CorsEvidenceBytes = [IO.File]::ReadAllBytes($CorsEvidencePath)
$CorsEvidenceRaw = [Text.UTF8Encoding]::new($false, $true).GetString($CorsEvidenceBytes)

# Task 7 promotion and post-review correction contracts are installed before
# their evidence so the first run is a causal RED on missing correction receipts.
$expectedPromotedCorsEvidence = @(
    'Bucket CORS local validation: PASS',
    'Hosted Bucket CORS validation: NOT RUN',
    'Base HEAD: 4224b6da2c74e9dd7288e4751afd78a2f52e9c39',
    'Runtime input identity: sha256:0a646f3e389e8d19b2d2ec7c8d1be0396216ed08d5e0115ef6f08f4be1c8e832',
    'Spec SHA-256: 824f5c515ce070cceecf4c7f2171e83e1e4fc039b963fda6e10780cb2dbf9ac5',
    'Plan SHA-256: f99612c7b8f87416c89d832802dcefcf96533f9f7fa6919bc041c31b0870fbc0',
    'Package: 0.1.0',
    'PowerShell: 7.5.4',
    'Cargo: 1.98.0',
    'Rustc: 1.98.0',
    'Docker Server: 28.3.3',
    'Compose: 2.39.2-desktop.1',
    'AWS CLI: 2.36.34',
    'Command: cargo test --lib --locked --offline',
    'Passed count: 936',
    'Command: cargo test --test cors --locked --offline -- --test-threads=1',
    'Passed count: 7',
    'Command: cargo test --test integration --locked --offline -- --test-threads=1',
    'Passed count: 143',
    'Command: pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1',
    'Static client-smoke: PASS',
    'Command: cargo test --test postgres_cors --locked --offline -- --nocapture --test-threads=1',
    'Passed count: 4',
    'PostgreSQL 17: PASS',
    'AWS management CRUD: PASS',
    'AWS default CRC64NVME PUT parity: PASS',
    'Browser valid-preflight: PASS',
    'Browser wildcard-preflight: PASS',
    'Browser disallowed-preflight: PASS',
    'Browser partial-preflight: PASS',
    'Browser plain-options fallthrough: PASS',
    'Browser signed-actual success: PASS',
    'Browser signed-actual S3 error: PASS',
    'Browser custom-import preflight: PASS',
    'Browser decompress preflight: PASS',
    'Browser health exclusion: PASS',
    'Browser ready exclusion: PASS',
    'Browser overall parity: PASS',
    'Cleanup logs captured: PASS',
    'Cleanup Compose down: PASS',
    'Cleanup environment restore: PASS',
    'Cleanup residual containers: exit=0 count=0',
    'Cleanup residual networks: exit=0 count=0',
    'Cleanup residual volumes: exit=0 count=0',
    'Cleanup residual images: exit=0 count=0',
    'Cleanup residual temp: exit=0 count=0',
    'Cleanup errors: 0',
    'Documentation promotion: PASS',
    'Post-review current artifact: PASS',
    'Duplicate SDK checksum algorithm cardinality: PASS',
    'Valid MD5 signed regression RED: 200',
    'Valid MD5 signed regression GREEN: 400 InvalidRequest',
    'Focused exact regression: PASS1',
    'Final nonlive matrix: PASS lib937/cors8/integration143',
    'Clippy: PASS',
    'Fmt: PASS',
    'Static: PASS',
    'Diff: PASS'
) -join "`n"
$expectedPromotedCorsEvidence += "`n"
Assert-True ($CorsEvidenceRaw -ceq $expectedPromotedCorsEvidence) 'Bucket CORS post-review correction causal RED: evidence must contain the exact promoted sanitized PASS receipt'

# CRC64NVME integrity support is intentionally contracted here before its
# implementation so the first static run records the causal RED.
$CorsChecksumPath = Join-Path $RepoRoot 'src/cors/checksum.rs'
$CorsModulePath = Join-Path $RepoRoot 'src/cors/mod.rs'
$CorsHttpPath = Join-Path $RepoRoot 'src/cors/http.rs'
$CorsOpsPath = Join-Path $RepoRoot 'src/s3/ops/cors.rs'
$CorsTestsPath = Join-Path $RepoRoot 'tests/cors.rs'
$corsChecksumContractFailures = [Collections.Generic.List[string]]::new()
foreach ($path in @($CorsChecksumPath, $CorsModulePath, $CorsHttpPath, $CorsOpsPath)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        $corsChecksumContractFailures.Add("CRC64NVME contract path is missing: $path")
    }
}
if ($corsChecksumContractFailures.Count -eq 0) {
    $CorsChecksumSource = [IO.File]::ReadAllText($CorsChecksumPath)
    $CorsModuleSource = [IO.File]::ReadAllText($CorsModulePath)
    $CorsHttpChecksumSource = [IO.File]::ReadAllText($CorsHttpPath)
    $CorsOpsChecksumSource = [IO.File]::ReadAllText($CorsOpsPath)
    foreach ($fragment in @(
        '0x9a6c9329ac4bc9b5', '[u8; 8]', 'to_be_bytes', '123456789',
        'ae8b14860a799888', 'rosUhgp5mIg='
    )) {
        if (-not $CorsChecksumSource.Contains($fragment, [StringComparison]::Ordinal)) {
            $corsChecksumContractFailures.Add("CRC64NVME checksum contract is missing: $fragment")
        }
    }
    foreach ($fragment in @('pub(crate) mod checksum;', 'Crc64NvmeHeader', 'x-amz-checksum-crc64nvme', 'CRC64NVME', 'ct_eq')) {
        $source = switch ($fragment) {
            'pub(crate) mod checksum;' { $CorsModuleSource }
            'Crc64NvmeHeader' { $CorsModuleSource + "`n" + $CorsHttpChecksumSource }
            'x-amz-checksum-crc64nvme' { $CorsHttpChecksumSource }
            default { $CorsOpsChecksumSource }
        }
        if (-not $source.Contains($fragment, [StringComparison]::Ordinal)) {
            $corsChecksumContractFailures.Add("CRC64NVME integrity contract is missing: $fragment")
        }
    }
}
if ($CorsRunnerSource.Contains('AWS_REQUEST_CHECKSUM_CALCULATION', [StringComparison]::Ordinal)) {
    $corsChecksumContractFailures.Add('Bucket CORS runner must use the modern AWS CLI checksum default without an override')
}
if ($corsChecksumContractFailures.Count -ne 0) {
    throw "Bucket CORS CRC64NVME causal RED: $($corsChecksumContractFailures -join '; ')"
}

$corsReviewCorrectionFailures = [Collections.Generic.List[string]]::new()
if (-not (Test-Path -LiteralPath $CorsTestsPath -PathType Leaf)) {
    $corsReviewCorrectionFailures.Add('focused CORS regression source is missing')
} else {
    $CorsTestsSource = [IO.File]::ReadAllText($CorsTestsPath)
    $corsReviewCorrectionMatches = @([regex]::Matches(
        $CorsTestsSource,
        '(?s)async fn signed_management_rejects_duplicate_sdk_checksum_algorithm_with_valid_md5\(\) \{(?<body>.*?)\n\}\n\n#\[tokio::test\]'
    ))
    if ($corsReviewCorrectionMatches.Count -ne 1) {
        $corsReviewCorrectionFailures.Add('focused duplicate SDK checksum regression must exist exactly once')
    } else {
        $corsReviewCorrectionSource = $corsReviewCorrectionMatches[0].Groups['body'].Value
        foreach ($fragment in @(
            'let mut headers = md5_headers(&body);',
            'SDK_CHECKSUM_ALGORITHM,',
            'HeaderValue::from_static("CRC64NVME")',
            'assert_eq!(response.status(), StatusCode::BAD_REQUEST);',
            'assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidRequest").await;'
        )) {
            if (-not $corsReviewCorrectionSource.Contains($fragment, [StringComparison]::Ordinal)) {
                $corsReviewCorrectionFailures.Add("focused duplicate SDK checksum regression is missing: $fragment")
            }
        }
        if (([regex]::Matches($corsReviewCorrectionSource, [regex]::Escape('headers.append('))).Count -ne 2 -or
            ([regex]::Matches($corsReviewCorrectionSource, [regex]::Escape('SDK_CHECKSUM_ALGORITHM,'))).Count -ne 2) {
            $corsReviewCorrectionFailures.Add('duplicate SDK checksum regression must prove cardinality two with a valid MD5')
        }
    }
}
if ($corsReviewCorrectionFailures.Count -ne 0) {
    throw "Bucket CORS post-review correction static assertion failed: $($corsReviewCorrectionFailures -join '; ')"
}

function Get-CorsRunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)
    $matches = @($CorsRunnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { throw "Expected one Bucket CORS function named $Name, found $($matches.Count)" }
    return $matches[0].Extent.Text
}

$corsParameterNames = @($CorsRunnerAst.ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
Assert-True (($corsParameterNames -join ",") -ceq "Run,PostgresOnly,DiagnoseAws,DiagnoseBrowser") "Bucket CORS runner must expose exactly Run, PostgresOnly, DiagnoseAws, and DiagnoseBrowser switches"
foreach ($fragment in @(
    '[CmdletBinding()]', '[switch]$Run', '[switch]$PostgresOnly', '[switch]$DiagnoseAws', '[switch]$DiagnoseBrowser',
    '$selectedModeCount = @(', '$Run.IsPresent', '$PostgresOnly.IsPresent', '$DiagnoseAws.IsPresent', '$DiagnoseBrowser.IsPresent',
    'if ($selectedModeCount -gt 1)',
    'throw "Bucket CORS runner modes are mutually exclusive"',
    'if ($selectedModeCount -eq 0)',
    'Write-Output "Bucket CORS validation: NOT RUN"',
    'return'
)) {
    Assert-Contains $CorsRunnerSource $fragment "Bucket CORS entry-point contract is missing: $fragment"
}
$corsMutualIndex = $CorsRunnerSource.IndexOf('if ($selectedModeCount -gt 1)', [StringComparison]::Ordinal)
$corsNoRunIndex = $CorsRunnerSource.IndexOf('if ($selectedModeCount -eq 0)', [StringComparison]::Ordinal)
$corsFirstSideEffectIndex = @(
    $CorsRunnerSource.IndexOf('Get-Command', [StringComparison]::Ordinal),
    $CorsRunnerSource.IndexOf('Invoke-NativeCommand', [StringComparison]::Ordinal),
    $CorsRunnerSource.IndexOf('TcpListener', [StringComparison]::Ordinal),
    $CorsRunnerSource.IndexOf('New-CorsRunRoot', [StringComparison]::Ordinal)
    | Where-Object { $_ -ge 0 } | Measure-Object -Minimum
).Minimum
Assert-True ($corsMutualIndex -ge 0 -and $corsNoRunIndex -gt $corsMutualIndex -and $corsFirstSideEffectIndex -gt $corsNoRunIndex) "Bucket CORS no-run/mutual exclusion must precede every tool, port, image, Docker, Cargo, or network action"

foreach ($name in @(
    'Write-CorsEvidence', 'Set-CorsStage', 'New-CorsRunId', 'New-CorsProjectName',
    'New-CorsGatewayImage', 'New-CorsBucketName', 'Assert-CanonicalChildPath',
    'New-CorsRunRoot', 'New-CorsOwnershipReceipt', 'Save-EnvironmentState',
    'Restore-EnvironmentState', 'Invoke-NativeCommand', 'Invoke-Docker',
    'Invoke-Compose', 'Assert-RequiredTools', 'Assert-ComposeVersion',
    'Test-CorsLoopbackPortAvailable', 'Assert-LoopbackPortsFree', 'Test-LocalImage', 'Assert-ProjectResourcesAbsent',
    'Assert-RequiredLocalImages', 'Invoke-OfflineGatewayBuild', 'Wait-CorsTopologyHealthy',
    'Assert-Postgres17Ready', 'Assert-RustSuiteExecuted', 'Invoke-CorsRustSuites',
    'Add-CorsAwsSubstageReceipt', 'Add-CorsBrowserSubstageReceipt', 'Invoke-CorsAwsParity', 'Invoke-CorsAwsDiagnostic', 'Invoke-CorsBrowserDiagnostic', 'Invoke-CorsBrowserParity', 'Test-CorsCleanupResiduals',
    'Remove-OwnedCorsResources', 'Invoke-CorsMain'
)) {
    $null = Get-CorsRunnerFunctionSource $name
}

foreach ($name in @('Get-CorsHmacHex', 'Get-CorsHmacBytes', 'Get-CorsSha256Hex', 'New-CorsSignedRequest')) {
    $null = Get-CorsRunnerFunctionSource $name
}
$corsSha256Source = Get-CorsRunnerFunctionSource 'Get-CorsSha256Hex'
Assert-Contains $corsSha256Source '[AllowEmptyString()]' 'Bucket CORS SHA-256 helper must accept the empty GET payload used by the signer'
foreach ($name in @('Get-CorsHmacHex', 'Get-CorsHmacBytes', 'Get-CorsSha256Hex', 'New-CorsSignedRequest')) {
    Invoke-Expression (Get-CorsRunnerFunctionSource $name)
}
$corsSignedFixture = New-CorsSignedRequest -Method 'GET' -Uri ([uri]'http://127.0.0.1:59000/fixed-bucket/fixed-object') -Origin 'https://allowed.example'
try {
    Assert-True ($corsSignedFixture.Method.Method -ceq 'GET') 'Bucket CORS signer fixture must construct GET'
    foreach ($headerName in @('Authorization', 'Origin', 'x-amz-date', 'x-amz-content-sha256')) {
        Assert-True $corsSignedFixture.Headers.Contains($headerName) "Bucket CORS signer fixture is missing $headerName"
    }
} finally {
    $corsSignedFixture.Dispose()
}

$corsRunIdSource = Get-CorsRunnerFunctionSource 'New-CorsRunId'
$corsProjectSource = Get-CorsRunnerFunctionSource 'New-CorsProjectName'
$corsImageSource = Get-CorsRunnerFunctionSource 'New-CorsGatewayImage'
$corsBucketSource = Get-CorsRunnerFunctionSource 'New-CorsBucketName'
$corsRootSource = Get-CorsRunnerFunctionSource 'New-CorsRunRoot'
$corsReceiptSource = Get-CorsRunnerFunctionSource 'New-CorsOwnershipReceipt'
foreach ($fragment in @(
    'RandomNumberGenerator', "'^[0-9a-f]{32}$'", 'ToLowerInvariant()'
)) {
    Assert-Contains $corsRunIdSource $fragment "Bucket CORS cryptographic lowercase RunId contract is missing: $fragment"
}
foreach ($fragment in @(
    '"ipfs3-cors-$RunId"', "'^[a-z0-9][a-z0-9_-]*$'"
)) {
    Assert-Contains $corsProjectSource $fragment "Bucket CORS project grammar is missing: $fragment"
}
foreach ($fragment in @(
    '"ipfs3-cors-gateway:$RunId"', "'^[a-z0-9][a-z0-9._:-]*$'"
)) {
    Assert-Contains $corsImageSource $fragment "Bucket CORS gateway-image grammar is missing: $fragment"
}
foreach ($fragment in @(
    '"ipfs3-cors-$RunId"', "'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$'"
)) {
    Assert-Contains $corsBucketSource $fragment "Bucket CORS bucket grammar is missing: $fragment"
}
foreach ($fragment in @('"ipfs-s3-bucket-cors-$($State.RunId)"', 'RunRoot must be a direct child')) {
    Assert-Contains $corsRootSource $fragment "Bucket CORS direct-child root guard is missing: $fragment"
}
foreach ($fragment in @('[IO.FileMode]::CreateNew', 'ownership-receipt', 'ipfs3.cors.run')) {
    Assert-Contains $corsReceiptSource $fragment "Bucket CORS ownership receipt contract is missing: $fragment"
}

$corsNativeSource = Get-CorsRunnerFunctionSource 'Invoke-NativeCommand'
foreach ($fragment in @(
    '[Diagnostics.ProcessStartInfo]::new()', '$startInfo.ArgumentList.Add($argument)',
    'WaitForExit($timeoutMilliseconds)', 'Kill($true)', '[int[]]$AllowedExitCodes',
    '[Parameter(Mandatory)][TimeSpan]$Timeout', '[Parameter(Mandatory)][string]$Stage',
    'raw-output'
)) {
    Assert-Contains $corsNativeSource $fragment "Bucket CORS bounded native helper is incomplete: $fragment"
}
Assert-NotContains $CorsRunnerSource 'Start-Job' 'Bucket CORS runner must not use unbounded background jobs'
$corsDirectNative = @($CorsRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -in @('docker', 'cargo', 'tar.exe', 'pwsh', 'curl', 'curl.exe')
}, $true))
Assert-True ($corsDirectNative.Count -eq 0) "Bucket CORS runner may invoke native tools only through Invoke-NativeCommand"
foreach ($forbidden in @(
    'docker pull', 'pull_policy:', '--pull=always', 'Install-Module', 'choco install',
    'winget install', 'scoop install', 'Invoke-WebRequest', 'curl.exe', 'docker login',
    'system prune', 'container_name:', 'cloudflared'
)) {
    Assert-NotContains ($CorsRunnerSource + "`n" + $CorsComposeSource) $forbidden "Bucket CORS validation must not pull, install, remotely inspect, or use a forbidden topology feature: $forbidden"
}

$corsToolsSource = Get-CorsRunnerFunctionSource 'Assert-RequiredTools'
Assert-Contains $corsToolsSource '@("pwsh", "cargo", "docker", "tar.exe")' 'Bucket CORS preflight must require pwsh, cargo, docker, and tar'
$corsComposeVersionSource = Get-CorsRunnerFunctionSource 'Assert-ComposeVersion'
foreach ($fragment in @(
    '"compose", "version", "--short"',
    "'^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$'",
    '[Version]::TryParse($composeVersionMatch.Groups["core"].Value, [ref]$composeVersion)',
    '[Version]"2.23.1"'
)) {
    Assert-Contains $corsComposeVersionSource $fragment "Bucket CORS strict Compose version preflight is missing: $fragment"
}
$corsPortsSource = Get-CorsRunnerFunctionSource 'Assert-LoopbackPortsFree'
$corsGeneratedPortsSource = Get-CorsRunnerFunctionSource 'New-CorsLoopbackPorts'
$corsPortAvailabilityMatches = @($CorsRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq 'Test-CorsLoopbackPortAvailable'
}, $true))
$corsPortAvailabilitySource = if ($corsPortAvailabilityMatches.Count -eq 1) { $corsPortAvailabilityMatches[0].Extent.Text } else { '' }
Assert-True ($corsPortAvailabilityMatches.Count -eq 1) 'Bucket CORS port-availability helper must exist exactly once'
foreach ($fragment in @(
    'param([Parameter(Mandatory)][int]$Port)',
    '$Port -lt 49152 -or $Port -gt 65535',
    '[System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $Port)',
    '$listener.Start()', 'return $true', 'return $false', 'finally', '$listener.Stop()'
)) {
    Assert-Contains $corsPortAvailabilitySource $fragment "Bucket CORS port-availability helper contract is missing: $fragment"
}
foreach ($fragment in @('TcpListener', 'IPAddress]::Loopback')) {
    Assert-Contains $corsPortsSource $fragment "Bucket CORS loopback port contract is missing: $fragment"
}
foreach ($fragment in @(
    '49152', '65535', '[Convert]::FromHexString($RunId)', 'DistinctPorts',
    'while ($ports.Contains($candidate) -or -not (Test-CorsLoopbackPortAvailable -Port $candidate))',
    '$attempt = 0', '$attempt++', '$attempt -ge $rangeSize',
    '$candidate -eq $rangeEnd',
    'No bindable Bucket CORS loopback port is available'
)) {
    Assert-Contains $corsGeneratedPortsSource $fragment "Bucket CORS generated-port contract is missing: $fragment"
}
$corsPortFixtureRunId = '00112233445566778899aabbccddeeff'
$corsPortFixtureBytes = [Convert]::FromHexString($corsPortFixtureRunId)
$corsPortFixtureInitial = 49152 + ([BitConverter]::ToUInt16($corsPortFixtureBytes, 0) % (65535 - 49152 + 1))
$corsPortFixtureOccupied = $null
$corsPortFixture = $null
try {
    Invoke-Expression $corsPortAvailabilitySource
    Invoke-Expression $corsGeneratedPortsSource
    $corsPortFixtureOccupied = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $corsPortFixtureInitial)
    $corsPortFixtureOccupied.Start()
    $corsPortFixture = New-CorsLoopbackPorts -RunId $corsPortFixtureRunId
    $corsPortFixturePorts = @($corsPortFixture.Postgres, $corsPortFixture.Kubo, $corsPortFixture.Gateway)
    Assert-True ($corsPortFixture.Postgres -ne $corsPortFixtureInitial) 'Bucket CORS generated first port must skip its occupied deterministic candidate'
    Assert-True ((@($corsPortFixturePorts | Sort-Object -Unique).Count -eq 3) -and (@($corsPortFixturePorts | Where-Object { $_ -lt 49152 -or $_ -gt 65535 }).Count -eq 0)) 'Bucket CORS generated ports must remain three distinct high ports'
} finally {
    if ($null -ne $corsPortFixtureOccupied) { $corsPortFixtureOccupied.Stop() }
}
$corsPortFixtureReboundListeners = [Collections.Generic.List[System.Net.Sockets.TcpListener]]::new()
$corsPortFixtureReboundListener = $null
try {
    foreach ($port in @($corsPortFixture.Postgres, $corsPortFixture.Kubo, $corsPortFixture.Gateway)) {
        $corsPortFixtureReboundListener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $port)
        $corsPortFixtureReboundListener.Start()
        $corsPortFixtureReboundListeners.Add($corsPortFixtureReboundListener)
        $corsPortFixtureReboundListener = $null
    }
} finally {
    if ($null -ne $corsPortFixtureReboundListener) { $corsPortFixtureReboundListener.Stop() }
    foreach ($listener in $corsPortFixtureReboundListeners) { $listener.Stop() }
}
$corsLocalImageSource = Get-CorsRunnerFunctionSource 'Test-LocalImage'
Assert-Contains $corsLocalImageSource '"image", "inspect", $Image, "--format", "{{.Id}}"' 'Bucket CORS cached-image inspection must use the exact local image ID query'
foreach ($image in @('postgres:17', 'ghcr.io/hugefiver/ipfs3-kubo:latest', 'ghcr.io/hugefiver/ipfs3:latest', 'rust:latest', 'amazon/aws-cli:latest')) {
    Assert-Contains $CorsRunnerSource $image "Bucket CORS required cached image is missing: $image"
}
$corsPreflightSource = Get-CorsRunnerFunctionSource 'Assert-ProjectResourcesAbsent'
foreach ($fragment in @(
    '"ps", "-aq"', '"network", "ls", "-q"', '"volume", "ls", "-q"',
    '"image", "ls", "-q"', '"label=com.docker.compose.project=$Project"',
    '"label=ipfs3.cors.run=$RunId"', 'BLOCKED'
)) {
    Assert-Contains $corsPreflightSource $fragment "Bucket CORS project/image ownership preflight is incomplete: $fragment"
}

$corsOfflineSource = (Get-CorsRunnerFunctionSource 'Invoke-OfflineGatewayBuild') + "`n" + (Get-CorsRunnerFunctionSource 'Copy-CorsBuildInputs')
foreach ($fragment in @(
    '"vendor", "--locked", "--offline"', 'Copy-CorsBuildInputs',
    'Cargo.toml', 'Cargo.lock', 'src', 'tests',
    'replace-with = "vendored-sources"',
    '"build", "--pull=false", "--network", "none", "--quiet"',
    '"--label", "ipfs3.cors.run=$RunId"', '--file', '$BuildContext'
)) {
    Assert-Contains $corsOfflineSource $fragment "Bucket CORS offline candidate build contract is missing: $fragment"
}

$corsRustReceiptSource = Get-CorsRunnerFunctionSource 'Assert-RustSuiteExecuted'
foreach ($fragment in @(
    "'(?m)^running (?<running>[1-9][0-9]*) tests?$'",
    "'(?m)^test result: ok\. (?<passed>[1-9][0-9]*) passed; 0 failed; (?<ignored>[0-9]+) ignored; (?<measured>[0-9]+) measured; (?<filtered>[0-9]+) filtered out; finished in (?<seconds>[0-9]{1,4}(?:\.[0-9]{1,3})?)s$'",
    '$runningMatches.Count -ne 1', '$summaryMatches.Count -ne 1',
    '$runningCount -ne $passedCount', '$seconds -gt [decimal]3600'
)) {
    Assert-Contains $corsRustReceiptSource $fragment "Bucket CORS Rust summary parser is incomplete: $fragment"
}
Invoke-Expression $corsRustReceiptSource
function Test-CorsRustReceiptFixture {
    param([Parameter(Mandatory)][string[]]$Lines)
    try {
        Assert-RustSuiteExecuted -Result ([pscustomobject]@{ StdOut = @($Lines); StdErr = @() }) -Name 'static fixture'
        return $true
    } catch { return $false }
}
Assert-True (Test-CorsRustReceiptFixture -Lines @('running 1 test', 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s')) 'Bucket CORS Rust parser must accept one executed test'
Assert-True (Test-CorsRustReceiptFixture -Lines @('running 2 tests', 'test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 3600.00s')) 'Bucket CORS Rust parser must accept bounded plural results'
Assert-True (-not (Test-CorsRustReceiptFixture -Lines @('running 2 tests', 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s'))) 'Bucket CORS Rust parser must reject mismatched counts'
Assert-True (-not (Test-CorsRustReceiptFixture -Lines @('running 1 test', 'test result: ok. 1 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s'))) 'Bucket CORS Rust parser must reject failed tests'

$corsSuitesSource = Get-CorsRunnerFunctionSource 'Invoke-CorsRustSuites'
$expectedCorsRustCommands = @(
    'cargo test --lib --locked --offline',
    'cargo test --test cors --locked --offline -- --test-threads=1',
    'cargo test --test integration --locked --offline -- --test-threads=1',
    'pwsh -NoLogo -NoProfile -File tests/client-smoke.Tests.ps1',
    'cargo test --test postgres_cors --locked --offline -- --nocapture --test-threads=1'
)
foreach ($command in $expectedCorsRustCommands) {
    Assert-Contains $corsSuitesSource $command "Bucket CORS required locked/offline command is missing: $command"
}
$corsMainSource = Get-CorsRunnerFunctionSource 'Invoke-CorsMain'
foreach ($fragment in @(
    '"up", "--detach", "--pull", "never", "--no-build"',
    'Invoke-CorsAwsParity', 'Invoke-CorsBrowserParity'
)) {
    Assert-Contains $corsMainSource $fragment "Bucket CORS execution/main contract is missing: $fragment"
}
$corsPostgresSuiteSource = Get-CorsRunnerFunctionSource 'Invoke-CorsPostgresSuite'
Assert-Contains $corsPostgresSuiteSource 'IPFS_S3_TEST_POSTGRES_URL' 'Bucket CORS PostgreSQL suite must set the owned PostgreSQL URL'
foreach ($terminalReceipt in @('Bucket CORS PostgreSQL validation: PASS', 'Bucket CORS validation: PASS', 'Bucket CORS AWS diagnostic: PASS', 'Bucket CORS AWS diagnostic: FAILED', 'Bucket CORS browser diagnostic: PASS', 'Bucket CORS browser diagnostic: FAILED')) {
    Assert-Contains $CorsRunnerSource $terminalReceipt "Bucket CORS terminal receipt is missing: $terminalReceipt"
}
$fullOrder = @(
    $corsMainSource.IndexOf('Invoke-CorsRustSuites -State $state -Mode "full"', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Assert-RequiredLocalImages -State $state -Mode "full"', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Invoke-OfflineGatewayBuild', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf("Wait-CorsTopologyHealthy -State `$state -Services @('postgres', 'kubo', 'gateway')", [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Invoke-CorsPostgresSuite -State $state', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Invoke-CorsAwsParity -State $state', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Invoke-CorsBrowserParity -State $state', [StringComparison]::Ordinal)
)
Assert-True (@($fullOrder | Where-Object { $_ -lt 0 }).Count -eq 0 -and $fullOrder -join ',' -ceq (@($fullOrder | Sort-Object) -join ',')) 'Bucket CORS full mode must run Docker-free gates, owned topology, PostgreSQL, AWS, then browser parity in order'
$corsAwsDiagnosticSource = Get-CorsRunnerFunctionSource 'Invoke-CorsAwsDiagnostic'
$corsBrowserDiagnosticSource = Get-CorsRunnerFunctionSource 'Invoke-CorsBrowserDiagnostic'
Assert-Contains $CorsRunnerSource 'Mode = if ($PostgresOnly) { "postgres" } elseif ($DiagnoseAws) { "aws" } elseif ($DiagnoseBrowser) { "browser" } else { "full" }' 'Bucket CORS state mode must select postgres, aws, browser, or full'
foreach ($source in @(
    (Get-CorsRunnerFunctionSource 'Assert-RequiredLocalImages'),
    (Get-CorsRunnerFunctionSource 'Assert-ProjectResourcesAbsent')
)) {
    Assert-Contains $source '"full", "postgres", "aws", "browser"' 'Bucket CORS internal mode validation must accept aws and browser alongside full and postgres'
}
Assert-Contains $corsMainSource 'elseif ($State.Mode -notin @("aws", "browser"))' 'Bucket CORS main mode validation must reject an internal mode other than full, postgres, aws, or browser'
foreach ($fragment in @(
    '$State.Mode -eq "aws"', 'Invoke-CorsAwsDiagnostic -State $State', 'Get-CorsComposeNetwork -State $State',
    'Set-CorsStage -State $State -Stage "aws"', 'New-CorsAwsConfig -State $State',
    'Invoke-CorsAwsParity -State $State -Network $network'
)) {
    Assert-Contains ($corsMainSource + "`n" + $corsAwsDiagnosticSource) $fragment "Bucket CORS AWS diagnostic branch is incomplete: $fragment"
}
foreach ($forbidden in @('Invoke-CorsRustSuites', 'Invoke-CorsPostgresSuite', 'Invoke-CorsBrowserParity')) {
    Assert-NotContains $corsAwsDiagnosticSource $forbidden "Bucket CORS AWS diagnostic must not invoke $forbidden"
}
$corsDiagnosticNetworkIndex = $corsAwsDiagnosticSource.IndexOf('Get-CorsComposeNetwork -State $State', [StringComparison]::Ordinal)
$corsDiagnosticStageIndex = $corsAwsDiagnosticSource.IndexOf('Set-CorsStage -State $State -Stage "aws"', [StringComparison]::Ordinal)
$corsDiagnosticConfigIndex = $corsAwsDiagnosticSource.IndexOf('New-CorsAwsConfig -State $State', [StringComparison]::Ordinal)
$corsDiagnosticAwsIndex = $corsAwsDiagnosticSource.IndexOf('Invoke-CorsAwsParity -State $State -Network $network', [StringComparison]::Ordinal)
Assert-True ($corsDiagnosticNetworkIndex -ge 0 -and $corsDiagnosticStageIndex -gt $corsDiagnosticNetworkIndex -and $corsDiagnosticConfigIndex -gt $corsDiagnosticStageIndex -and $corsDiagnosticAwsIndex -gt $corsDiagnosticConfigIndex) 'Bucket CORS AWS diagnostic must discover its network before the redacted AWS parity call'
$corsAwsBranchIndex = $corsMainSource.IndexOf('if ($State.Mode -eq "aws")', [StringComparison]::Ordinal)
$corsAwsBranchCallIndex = $corsMainSource.IndexOf('Invoke-CorsAwsDiagnostic -State $State', [StringComparison]::Ordinal)
$corsAwsBranchPostgresIndex = $corsMainSource.IndexOf('Assert-Postgres17Ready -State $State', [StringComparison]::Ordinal)
Assert-True ($corsAwsBranchIndex -gt $corsAwsBranchPostgresIndex -and $corsAwsBranchCallIndex -gt $corsAwsBranchIndex -and ([regex]::Matches($corsMainSource, [regex]::Escape('if ($State.Mode -eq "aws")')).Count -eq 1)) 'Bucket CORS must have one AWS diagnostic branch after PostgreSQL 17 readiness'
$corsAwsDiagnosticBranches = @($CorsRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Clauses.Count -eq 2 -and
        $node.Clauses[0].Item1.Extent.Text -ceq '$State.Mode -eq "aws"'
}, $true))
Assert-True ($corsAwsDiagnosticBranches.Count -eq 1) 'Bucket CORS AST must contain exactly one AWS diagnostic branch'
$corsAwsDiagnosticBranchSource = $corsAwsDiagnosticBranches[0].Clauses[0].Item2.Extent.Text
Assert-True ($corsAwsDiagnosticBranchSource.Contains('Invoke-CorsAwsDiagnostic -State $State', [StringComparison]::Ordinal) -and $corsAwsDiagnosticBranchSource.Contains('return', [StringComparison]::Ordinal)) 'Bucket CORS AWS branch must return after its isolated diagnostic'
foreach ($forbidden in @('Invoke-CorsRustSuites', 'Invoke-CorsPostgresSuite', 'Invoke-CorsBrowserParity')) {
    Assert-NotContains $corsAwsDiagnosticBranchSource $forbidden "Bucket CORS AWS branch must not invoke $forbidden"
}
foreach ($fragment in @(
    'if ($State.Mode -in @("aws", "browser"))',
    'Assert-RequiredLocalImages -State $state -Mode $State.Mode',
    'Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode $State.Mode',
    'Assert-LoopbackPortsFree -Ports @($State.Ports.Postgres, $State.Ports.Kubo, $State.Ports.Gateway)',
    'Invoke-OfflineGatewayBuild -State $State -RunId $State.RunId',
    '"up", "--detach", "--pull", "never", "--no-build"',
    "Wait-CorsTopologyHealthy -State `$state -Services @('postgres', 'kubo', 'gateway')",
    'Assert-Postgres17Ready -State $State'
)) {
    Assert-Contains $corsMainSource $fragment "Bucket CORS AWS diagnostic must retain full-topology setup: $fragment"
}
$corsBrowserBranchIndex = $corsMainSource.IndexOf('elseif ($State.Mode -eq "browser")', [StringComparison]::Ordinal)
$corsBrowserBranchCallIndex = $corsMainSource.IndexOf('Invoke-CorsBrowserDiagnostic -State $State', [StringComparison]::Ordinal)
Assert-True ($corsBrowserBranchIndex -gt $corsAwsBranchCallIndex -and $corsBrowserBranchCallIndex -gt $corsBrowserBranchIndex -and ([regex]::Matches($corsMainSource, [regex]::Escape('elseif ($State.Mode -eq "browser")')).Count -eq 1)) 'Bucket CORS must have one browser diagnostic branch after the AWS diagnostic branch'
$corsBrowserDiagnosticBranches = @($CorsRunnerAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Clauses.Count -eq 2 -and
        $node.Clauses[1].Item1.Extent.Text -ceq '$State.Mode -eq "browser"'
}, $true))
Assert-True ($corsBrowserDiagnosticBranches.Count -eq 1) 'Bucket CORS AST must contain exactly one browser diagnostic branch'
$corsBrowserDiagnosticBranchSource = $corsBrowserDiagnosticBranches[0].Clauses[1].Item2.Extent.Text
Assert-True ($corsBrowserDiagnosticBranchSource.Contains('Invoke-CorsBrowserDiagnostic -State $State', [StringComparison]::Ordinal) -and $corsBrowserDiagnosticBranchSource.Contains('return', [StringComparison]::Ordinal)) 'Bucket CORS browser branch must return after its isolated diagnostic'
foreach ($forbidden in @('Invoke-CorsRustSuites', 'Invoke-CorsPostgresSuite')) {
    Assert-NotContains $corsBrowserDiagnosticBranchSource $forbidden "Bucket CORS browser branch must not invoke $forbidden"
}
foreach ($fragment in @(
    'Get-CorsComposeNetwork -State $State', 'Set-CorsStage -State $State -Stage "aws"',
    'New-CorsAwsConfig -State $State', 'Invoke-CorsAwsParity -State $State -Network $network',
    'Set-CorsStage -State $State -Stage "browser"', 'Invoke-CorsBrowserParity -State $State'
)) {
    Assert-Contains $corsBrowserDiagnosticSource $fragment "Bucket CORS browser diagnostic is incomplete: $fragment"
}
foreach ($forbidden in @('Invoke-CorsRustSuites', 'Invoke-CorsPostgresSuite', 'Write-Host', 'Write-Output', 'StdOut', 'StdErr')) {
    Assert-NotContains $corsBrowserDiagnosticSource $forbidden "Bucket CORS browser diagnostic must not invoke or expose $forbidden"
}
$corsBrowserDiagnosticOrder = @(
    $corsBrowserDiagnosticSource.IndexOf('Get-CorsComposeNetwork -State $State', [StringComparison]::Ordinal),
    $corsBrowserDiagnosticSource.IndexOf('Set-CorsStage -State $State -Stage "aws"', [StringComparison]::Ordinal),
    $corsBrowserDiagnosticSource.IndexOf('New-CorsAwsConfig -State $State', [StringComparison]::Ordinal),
    $corsBrowserDiagnosticSource.IndexOf('Invoke-CorsAwsParity -State $State -Network $network', [StringComparison]::Ordinal),
    $corsBrowserDiagnosticSource.IndexOf('Set-CorsStage -State $State -Stage "browser"', [StringComparison]::Ordinal),
    $corsBrowserDiagnosticSource.IndexOf('Invoke-CorsBrowserParity -State $State', [StringComparison]::Ordinal)
)
Assert-True (@($corsBrowserDiagnosticOrder | Where-Object { $_ -lt 0 }).Count -eq 0 -and $corsBrowserDiagnosticOrder -join ',' -ceq (@($corsBrowserDiagnosticOrder | Sort-Object) -join ',')) 'Bucket CORS browser diagnostic must perform AWS setup before browser parity'
$postgresOnlySource = Get-CorsRunnerFunctionSource 'Invoke-CorsPostgresOnly'
# Causal RED: Compose interpolates every service definition, including Kubo and
# gateway, before it starts PostgreSQL-only.  All interpolation values therefore
# must be established outside the full-topology mode branch before Compose runs.
$corsEnvironmentSnapshotIndex = $CorsRunnerSource.IndexOf('$state.EnvironmentState = Save-EnvironmentState -Names $TouchedEnvironmentNames', [StringComparison]::Ordinal)
$corsMainInvocationIndex = $CorsRunnerSource.IndexOf('Invoke-CorsMain -State $state', [StringComparison]::Ordinal)
Assert-True ($corsEnvironmentSnapshotIndex -ge 0 -and $corsMainInvocationIndex -gt $corsEnvironmentSnapshotIndex) 'Bucket CORS environment setup must follow its snapshot and precede main execution'
$corsEnvironmentSetupSource = $CorsRunnerSource.Substring($corsEnvironmentSnapshotIndex, $corsMainInvocationIndex - $corsEnvironmentSnapshotIndex)
foreach ($fragment in @(
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_POSTGRES_PORT" -Value ([string]$state.Ports.Postgres)',
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_KUBO_PORT" -Value ([string]$state.Ports.Kubo)',
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_GATEWAY_PORT" -Value ([string]$state.Ports.Gateway)',
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_IMAGE" -Value $state.GatewayImage',
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_PROJECT_LABEL" -Value $state.Project',
    'Set-CorsEnvironment -Name "IPFS_S3_CORS_RUN_LABEL" -Value $state.RunId'
)) {
    Assert-Contains $corsEnvironmentSetupSource $fragment "Bucket CORS Compose interpolation value must be set before Invoke-CorsMain: $fragment"
}
Assert-NotContains $corsEnvironmentSetupSource 'if ($state.Mode -in @("full", "aws", "browser"))' 'Bucket CORS Compose interpolation setup must not be conditional on full-topology modes'
$corsFullPortGateOrder = @(
    $corsMainSource.IndexOf('Assert-RequiredLocalImages -State $state -Mode "full"', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode "full"', [StringComparison]::Ordinal),
    $corsMainSource.IndexOf('Assert-LoopbackPortsFree -Ports @($State.Ports.Postgres, $State.Ports.Kubo, $State.Ports.Gateway)', [StringComparison]::Ordinal)
)
$corsPostgresPortGateOrder = @(
    $postgresOnlySource.IndexOf('Assert-RequiredLocalImages -State $State -Mode "postgres"', [StringComparison]::Ordinal),
    $postgresOnlySource.IndexOf('Assert-ProjectResourcesAbsent -State $State -Project $State.Project -RunId $State.RunId -Mode "postgres"', [StringComparison]::Ordinal),
    $postgresOnlySource.IndexOf('Assert-LoopbackPortsFree -Ports @($State.Ports.Postgres)', [StringComparison]::Ordinal)
)
Assert-True (@($corsFullPortGateOrder | Where-Object { $_ -lt 0 }).Count -eq 0 -and $corsFullPortGateOrder -join ',' -ceq (@($corsFullPortGateOrder | Sort-Object) -join ',')) 'Bucket CORS full-mode second bind gate must follow image and ownership preflights'
Assert-True (@($corsPostgresPortGateOrder | Where-Object { $_ -lt 0 }).Count -eq 0 -and $corsPostgresPortGateOrder -join ',' -ceq (@($corsPostgresPortGateOrder | Sort-Object) -join ',')) 'Bucket CORS PostgreSQL-only second bind gate must follow image and ownership preflights'
foreach ($forbidden in @('kubo', 'gateway', 'Invoke-CorsAwsParity', 'Invoke-CorsBrowserParity', 'Invoke-OfflineGatewayBuild')) {
    Assert-NotContains $postgresOnlySource $forbidden "Bucket CORS PostgreSQL-only mode must not invoke $forbidden"
}
foreach ($fragment in @('"up", "--detach", "--pull", "never", "--no-build", "postgres"', 'Assert-Postgres17Ready', 'Invoke-CorsPostgresSuite')) {
    Assert-Contains $postgresOnlySource $fragment "Bucket CORS PostgreSQL-only mode is incomplete: $fragment"
}

$corsEvidenceSource = Get-CorsRunnerFunctionSource 'Write-CorsEvidence'
$corsAwsSource = (Get-CorsRunnerFunctionSource 'Invoke-CorsAwsParity') + "`n" + (Get-CorsRunnerFunctionSource 'Invoke-CorsAws') + "`n" + (Get-CorsRunnerFunctionSource 'New-CorsAwsConfig') + "`n" + (Get-CorsRunnerFunctionSource 'Assert-CorsAwsAbsent')
foreach ($fragment in @(
    'amazon/aws-cli:latest', '"run", "--rm", "--pull=never"',
    'AWS_CONFIG_FILE=/work/aws-config', 'addressing_style = path',
    'create-bucket', 'put-bucket-cors', 'get-bucket-cors', 'delete-bucket-cors',
    'NoSuchCORSConfiguration'
)) {
    Assert-Contains $corsAwsSource $fragment "Bucket CORS AWS management parity contract is missing: $fragment"
}
Assert-NotContains ($CorsRunnerSource + "`n" + $CorsComposeSource) 'AWS_REQUEST_CHECKSUM_CALCULATION' 'Bucket CORS runner must preserve the modern AWS CLI checksum default globally'
$corsAwsParitySource = Get-CorsRunnerFunctionSource 'Invoke-CorsAwsParity'
$corsAwsReceiptHelperSource = Get-CorsRunnerFunctionSource 'Add-CorsAwsSubstageReceipt'
$corsAwsSubstages = @('files-written', 'bucket-created', 'initial-put', 'initial-get', 'initial-assert', 'replacement-put', 'replacement-get', 'replacement-assert', 'deleted', 'absent-verified', 'final-put', 'management-passed')
foreach ($source in @($corsEvidenceSource, $corsAwsReceiptHelperSource)) {
    Assert-Contains $source 'aws-substage=' 'Bucket CORS AWS substage receipt grammar is missing'
}
$corsAwsPreviousReceiptIndex = -1
foreach ($substage in $corsAwsSubstages) {
    $startCall = '-Name "' + $substage + '" -Outcome "start"'
    $passCall = '-Name "' + $substage + '" -Outcome "pass"'
    $startIndex = $corsAwsParitySource.IndexOf($startCall, [StringComparison]::Ordinal)
    $passIndex = $corsAwsParitySource.IndexOf($passCall, [StringComparison]::Ordinal)
    Assert-True ($startIndex -gt $corsAwsPreviousReceiptIndex -and $passIndex -gt $startIndex) "Bucket CORS AWS substage receipts are missing or out of order: $substage"
    $corsAwsPreviousReceiptIndex = $passIndex
}
Invoke-Expression $corsEvidenceSource
Invoke-Expression $corsAwsReceiptHelperSource
function Test-CorsAwsSubstageReceiptFixture {
    param([Parameter(Mandatory)][string[]]$Events)
    $fixtureState = @{ Receipts = [Collections.Generic.List[string]]::new() }
    try {
        foreach ($event in $Events) {
            $parts = $event.Split(':', 2)
            Add-CorsAwsSubstageReceipt -State $fixtureState -Name $parts[0] -Outcome $parts[1]
        }
        return @($fixtureState.Receipts)
    } catch {
        return @()
    }
}
$corsAwsFixtureEvents = foreach ($substage in $corsAwsSubstages) { "${substage}:start"; "${substage}:pass" }
$corsAwsExpectedReceipts = foreach ($substage in $corsAwsSubstages) { "[assertion] aws-substage=${substage}-start"; "[assertion] aws-substage=${substage}-pass" }
$corsAwsActualReceipts = @(Test-CorsAwsSubstageReceiptFixture -Events @($corsAwsFixtureEvents))
Assert-True (($corsAwsActualReceipts -join "`n") -ceq ($corsAwsExpectedReceipts -join "`n")) 'Bucket CORS AWS receipt fixture must preserve the exact fixed substage grammar and order'
Assert-True (@(Test-CorsAwsSubstageReceiptFixture -Events @('unknown:start')).Count -eq 0) 'Bucket CORS AWS receipt fixture must reject an unknown substage'
Assert-True (@(Test-CorsAwsSubstageReceiptFixture -Events @('files-written:unknown')).Count -eq 0) 'Bucket CORS AWS receipt fixture must reject an unknown outcome'
try {
    $null = Write-CorsEvidence -Category 'assertion' -Value 'aws-substage=unknown-start'
    throw 'Bucket CORS AWS receipt grammar accepted an unknown substage'
} catch [System.Management.Automation.RuntimeException] {
    if ($_.Exception.Message -eq 'Bucket CORS AWS receipt grammar accepted an unknown substage') { throw }
}
$corsBrowserSource = Get-CorsRunnerFunctionSource 'Invoke-CorsBrowserParity'
$corsStrictModeHeaderValuesFixture = {
    param(
        [Parameter(Mandatory)][Net.Http.HttpResponseMessage]$Response,
        [Parameter(Mandatory)][string]$Name
    )
    Set-StrictMode -Version Latest
    if (-not $Response.Headers.Contains($Name)) { return @() }
    return @($Response.Headers.GetValues($Name))
}
$corsStrictModeResponse = [Net.Http.HttpResponseMessage]::new()
try {
    $corsStrictModeDirectCountThrew = & {
        Set-StrictMode -Version Latest
        try {
            $null = (& $corsStrictModeHeaderValuesFixture -Response $corsStrictModeResponse -Name 'Access-Control-Allow-Credentials').Count
            return $false
        } catch {
            return $true
        }
    }
    Assert-True $corsStrictModeDirectCountThrew 'Bucket CORS StrictMode fixture must prove direct empty output Count throws before the array-wrapper fix'
    $corsStrictModeAbsentValues = @(& $corsStrictModeHeaderValuesFixture -Response $corsStrictModeResponse -Name 'Access-Control-Allow-Credentials')
    Assert-True ($corsStrictModeAbsentValues.Count -eq 0) 'Bucket CORS StrictMode fixture must count absent header output as zero via an array wrapper'
    $null = $corsStrictModeResponse.Headers.TryAddWithoutValidation('Access-Control-Allow-Credentials', 'true')
    $corsStrictModePresentValues = @(& $corsStrictModeHeaderValuesFixture -Response $corsStrictModeResponse -Name 'Access-Control-Allow-Credentials')
    Assert-True ($corsStrictModePresentValues.Count -eq 1 -and $corsStrictModePresentValues[0] -ceq 'true') 'Bucket CORS StrictMode fixture must count present header values as one via an array wrapper'
} finally {
    $corsStrictModeResponse.Dispose()
}
$expectedWildcardCredentialsCheck = '@(Get-CorsHttpHeaderValues -Response $response -Name "Access-Control-Allow-Credentials").Count'
$directWildcardCredentialsCheck = '(Get-CorsHttpHeaderValues -Response $response -Name "Access-Control-Allow-Credentials").Count'
$corsWildcardCredentialsStaticFailures = [Collections.Generic.List[string]]::new()
if (-not $corsBrowserSource.Contains($expectedWildcardCredentialsCheck, [StringComparison]::Ordinal)) {
    $corsWildcardCredentialsStaticFailures.Add('wildcard credentials absence check must count the array subexpression')
}
if ([regex]::IsMatch($corsBrowserSource, '(?<!@)' + [regex]::Escape($directWildcardCredentialsCheck))) {
    $corsWildcardCredentialsStaticFailures.Add('wildcard credentials absence check must reject the direct parenthesized Count form')
}
if ($corsWildcardCredentialsStaticFailures.Count -ne 0) {
    throw "Bucket CORS wildcard credentials static RED: $($corsWildcardCredentialsStaticFailures -join '; ')"
}
foreach ($fragment in @('valid-preflight', 'disallowed-preflight', 'partial-preflight', 'plain-options', 'signed-actual', 'custom-import-preflight', 'decompress-preflight')) {
    Assert-Contains $corsBrowserSource $fragment "Bucket CORS browser parity scenario is missing: $fragment"
}
Assert-Contains $corsBrowserSource '$disallowed.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "DELETE")' 'Bucket CORS disallowed preflight must use a method rejected by both the exact and wildcard rules'
Assert-NotContains $corsBrowserSource '$disallowed.Headers.TryAddWithoutValidation("Access-Control-Request-Method", "GET")' 'Bucket CORS disallowed preflight must not expect wildcard GET to be rejected'
Assert-Contains $corsBrowserSource '-Scenario "plain-options" -ExpectedStatus @(400, 403, 404, 405, 501) -ExpectCors $false' 'Bucket CORS plain OPTIONS must accept the normal s3s NotImplemented 501 while preserving the no-CORS assertion'
Assert-NotContains $corsBrowserSource '-Scenario "plain-options" -ExpectedStatus @(400, 403, 404, 405) -ExpectCors $false' 'Bucket CORS plain OPTIONS must not reject the normal s3s NotImplemented 501'
foreach ($forbiddenPlainStatus in @(200, 500, 502, 503, 504)) {
    Assert-True (-not [regex]::IsMatch($corsBrowserSource, '-Scenario "plain-options" -ExpectedStatus @\([^\)]*\b' + $forbiddenPlainStatus + '\b')) "Bucket CORS plain OPTIONS must reject unsafe status $forbiddenPlainStatus"
}
$corsBrowserReceiptHelperSource = Get-CorsRunnerFunctionSource 'Add-CorsBrowserSubstageReceipt'
$corsBrowserSubstages = @(
    'valid-preflight', 'wildcard-preflight', 'disallowed-preflight', 'partial-preflight',
    'plain-options', 'signed-actual', 'signed-actual-error', 'custom-import-preflight',
    'decompress-preflight', 'health-exclusion', 'ready-exclusion', 'parity'
)
foreach ($source in @($corsEvidenceSource, $corsBrowserReceiptHelperSource)) {
    Assert-Contains $source 'browser-substage=' 'Bucket CORS browser substage receipt grammar is missing'
}
Assert-Contains $corsEvidenceSource 'browser-substage=(?:valid-preflight|wildcard-preflight|disallowed-preflight|partial-preflight|plain-options|signed-actual|signed-actual-error|custom-import-preflight|decompress-preflight|health-exclusion|ready-exclusion|parity)-(?:start|pass)' 'Bucket CORS browser substage grammar must allow exactly the fixed 12 names and paired outcomes'
foreach ($fragment in @(
    '[ValidateSet("valid-preflight", "wildcard-preflight", "disallowed-preflight", "partial-preflight", "plain-options", "signed-actual", "signed-actual-error", "custom-import-preflight", "decompress-preflight", "health-exclusion", "ready-exclusion", "parity")][string]$Name',
    '[ValidateSet("start", "pass")][string]$Outcome',
    'Write-CorsEvidence -Category "assertion" -Value "browser-substage=$Name-$Outcome"'
)) {
    Assert-Contains $corsBrowserReceiptHelperSource $fragment "Bucket CORS browser substage helper contract is missing: $fragment"
}
foreach ($forbidden in @('Write-Host', 'Write-Output', 'StdOut', 'StdErr')) {
    Assert-NotContains $corsBrowserReceiptHelperSource $forbidden "Bucket CORS browser substage helper must not expose raw values: $forbidden"
}
$corsBrowserPreviousReceiptIndex = -1
foreach ($substage in $corsBrowserSubstages) {
    $startCall = '-Name "' + $substage + '" -Outcome "start"'
    $passCall = '-Name "' + $substage + '" -Outcome "pass"'
    $startIndex = $corsBrowserSource.IndexOf($startCall, [StringComparison]::Ordinal)
    $passIndex = $corsBrowserSource.IndexOf($passCall, [StringComparison]::Ordinal)
    Assert-True ($startIndex -gt $corsBrowserPreviousReceiptIndex -and $passIndex -gt $startIndex) "Bucket CORS browser substage receipts are missing or out of order: $substage"
    $corsBrowserPreviousReceiptIndex = $passIndex
}
Invoke-Expression $corsBrowserReceiptHelperSource
function Test-CorsBrowserSubstageReceiptFixture {
    param([Parameter(Mandatory)][string[]]$Events)
    $fixtureState = @{ Receipts = [Collections.Generic.List[string]]::new() }
    try {
        foreach ($event in $Events) {
            $parts = $event.Split(':', 2)
            Add-CorsBrowserSubstageReceipt -State $fixtureState -Name $parts[0] -Outcome $parts[1]
        }
        return @($fixtureState.Receipts)
    } catch {
        return @()
    }
}
$corsBrowserFixtureEvents = foreach ($substage in $corsBrowserSubstages) { "${substage}:start"; "${substage}:pass" }
$corsBrowserExpectedReceipts = foreach ($substage in $corsBrowserSubstages) { "[assertion] browser-substage=${substage}-start"; "[assertion] browser-substage=${substage}-pass" }
$corsBrowserActualReceipts = @(Test-CorsBrowserSubstageReceiptFixture -Events @($corsBrowserFixtureEvents))
Assert-True (($corsBrowserActualReceipts -join "`n") -ceq ($corsBrowserExpectedReceipts -join "`n")) 'Bucket CORS browser receipt fixture must preserve the exact fixed 12-substage grammar and order'
Assert-True (@(Test-CorsBrowserSubstageReceiptFixture -Events @('unknown:start')).Count -eq 0) 'Bucket CORS browser receipt fixture must reject an unknown substage'
Assert-True (@(Test-CorsBrowserSubstageReceiptFixture -Events @('valid-preflight:unknown')).Count -eq 0) 'Bucket CORS browser receipt fixture must reject an unknown outcome'
try {
    $null = Write-CorsEvidence -Category 'assertion' -Value 'browser-substage=unknown-start'
    throw 'Bucket CORS browser receipt grammar accepted an unknown substage'
} catch [System.Management.Automation.RuntimeException] {
    if ($_.Exception.Message -eq 'Bucket CORS browser receipt grammar accepted an unknown substage') { throw }
}
foreach ($scenario in @(
    'valid-preflight', 'wildcard-preflight', 'disallowed-preflight', 'partial-preflight', 'plain-options',
    'signed-actual', 'signed-actual-error', 'custom-import-preflight', 'decompress-preflight', 'health-ready-exclusion'
)) {
    Assert-Contains $corsBrowserSource ('"' + $scenario + '"') "Bucket CORS browser success receipt scenario must remain available in normal and diagnostic mode: $scenario"
}
Assert-Contains $corsBrowserSource 'Value "browser-$scenario=passed"' 'Bucket CORS legacy browser scenario pass receipts must remain available in normal and diagnostic mode'
Assert-Contains $corsBrowserSource 'Value "browser-parity=passed"' 'Bucket CORS legacy browser parity pass receipt must remain available in normal and diagnostic mode'

$corsBrowserFailureScenarios = @(
    'valid-preflight', 'wildcard-preflight', 'disallowed-preflight', 'partial-preflight',
    'plain-options', 'signed-actual', 'signed-actual-error', 'custom-import-preflight',
    'decompress-preflight', 'health-exclusion', 'ready-exclusion'
)
$corsBrowserFailureCategories = @(
    'transport', 'status', 'cors-presence', 'allow-origin', 'credentials',
    'allow-method', 'allow-headers', 'max-age', 'expose-headers', 'vary'
)
$corsBrowserTrackedAssertionCategories = @(
    'transport', 'allow-origin', 'credentials', 'allow-method', 'allow-headers',
    'max-age', 'expose-headers', 'vary'
)
$corsBrowserFailureGrammar = 'browser-failure=(?:valid-preflight|wildcard-preflight|disallowed-preflight|partial-preflight|plain-options|signed-actual|signed-actual-error|custom-import-preflight|decompress-preflight|health-exclusion|ready-exclusion)-(?:transport|status|cors-presence|allow-origin|credentials|allow-method|allow-headers|max-age|expose-headers|vary)'
Assert-Contains $corsEvidenceSource $corsBrowserFailureGrammar 'Bucket CORS browser failure receipt grammar must allow exactly the fixed scenarios and categories'
foreach ($category in $corsBrowserFailureCategories) {
    $value = "browser-failure=valid-preflight-$category"
    Assert-True ((Write-CorsEvidence -Category 'assertion' -Value $value) -ceq "[assertion] $value") 'Bucket CORS browser failure receipt fixture must accept each fixed category for a safe scenario'
}
foreach ($value in @(
    'browser-failure=unknown-transport',
    'browser-failure=valid-preflight-unknown',
    'browser-failure=valid-preflight-transport-suffix',
    'browser-failure=valid-preflight-transport raw-payload'
)) {
    $rejected = $false
    try {
        $null = Write-CorsEvidence -Category 'assertion' -Value $value
    } catch {
        $rejected = $true
    }
    Assert-True $rejected 'Bucket CORS browser failure receipt fixture must reject unknown, suffixed, or raw values'
}
$corsBrowserFailureSource = Get-CorsRunnerFunctionSource 'Invoke-CorsBrowserParity'
foreach ($fragment in @(
    '$currentScenario = $null',
    '$currentExpectedAssertionCategory = "transport"',
    'if ($null -ne $currentScenario)',
    '"Bucket CORS browser scenario returned an unexpected status" { "status" }',
    '"Bucket CORS browser scenario returned an unexpected CORS header set" { "cors-presence" }',
    'Write-CorsEvidence -Category "assertion" -Value "browser-failure=$currentScenario-$failureCategory"',
    'throw "Bucket CORS browser parity failed"'
)) {
    Assert-Contains $corsBrowserFailureSource $fragment "Bucket CORS browser failure catch is missing: $fragment"
}
Assert-True (([regex]::Matches($corsBrowserFailureSource, [regex]::Escape('browser-failure=$currentScenario-$failureCategory'))).Count -eq 1) 'Bucket CORS browser parity must emit exactly one failure receipt'
foreach ($scenario in $corsBrowserFailureScenarios) {
    Assert-Contains $corsBrowserFailureSource ('$currentScenario = "' + $scenario + '"') "Bucket CORS browser failure tracking must identify $scenario"
}
foreach ($category in $corsBrowserTrackedAssertionCategories) {
    Assert-Contains $corsBrowserFailureSource ('$currentExpectedAssertionCategory = "' + $category + '"') "Bucket CORS browser failure tracking must identify $category"
}
$browserFailureReceiptIndex = $corsBrowserFailureSource.IndexOf('Write-CorsEvidence -Category "assertion" -Value "browser-failure=$currentScenario-$failureCategory"', [StringComparison]::Ordinal)
$browserFailureThrowIndex = $corsBrowserFailureSource.IndexOf('throw "Bucket CORS browser parity failed"', [StringComparison]::Ordinal)
Assert-True ($browserFailureReceiptIndex -ge 0 -and $browserFailureThrowIndex -gt $browserFailureReceiptIndex) 'Bucket CORS browser failure receipt must precede the fixed generic error'
Assert-NotContains $corsBrowserFailureSource 'throw $_' 'Bucket CORS browser failure must not rethrow raw exception data'
$corsHttpSource = Get-CorsRunnerFunctionSource 'Invoke-CorsHttp'
foreach ($fragment in @('$completed = $false', '$completed = $true', 'if (-not $completed -and $null -ne $response)', '$response.Dispose()')) {
    Assert-Contains $corsHttpSource $fragment "Bucket CORS failed request disposal is missing: $fragment"
}

$corsCleanupSource = Get-CorsRunnerFunctionSource 'Remove-OwnedCorsResources'
$corsResidualSource = Get-CorsRunnerFunctionSource 'Test-CorsCleanupResiduals'
foreach ($fragment in @('"logs", "--no-color"', '"down", "--volumes", "--remove-orphans"', '"image", "rm", $State.GatewayImage', 'Remove-OwnedCorsRunRoot', 'Restore-EnvironmentState -State $State.EnvironmentState')) {
    Assert-Contains $corsCleanupSource $fragment "Bucket CORS logs-first cleanup contract is incomplete: $fragment"
}
foreach ($fragment in @('containers', 'networks', 'volumes', 'images', 'ExitCode', 'Count')) {
    Assert-Contains $corsResidualSource $fragment "Bucket CORS residual proof is incomplete: $fragment"
}
foreach ($fragment in @('temp-root', 'cleanup-errors=$($cleanupErrors.Count)')) {
    Assert-Contains ($corsCleanupSource + "`n" + $CorsRunnerSource) $fragment "Bucket CORS final cleanup proof is incomplete: $fragment"
}
foreach ($fragment in @('$State.Mode -in @("full", "aws", "browser")', 'GatewayImagePreflightAbsent', 'GatewayImageOwned')) {
    Assert-Contains $corsCleanupSource $fragment "Bucket CORS diagnostics must retain full-mode image cleanup: $fragment"
}
foreach ($environmentName in @('COMPOSE_DISABLE_ENV_FILE', 'IPFS_S3_CORS_POSTGRES_PORT', 'IPFS_S3_CORS_KUBO_PORT', 'IPFS_S3_CORS_GATEWAY_PORT', 'IPFS_S3_CORS_IMAGE', 'IPFS_S3_TEST_POSTGRES_URL')) {
    Assert-Contains $CorsRunnerSource ('"' + $environmentName + '"') "Bucket CORS runner must snapshot and restore $environmentName"
}
$corsEvidenceSource = Get-CorsRunnerFunctionSource 'Write-CorsEvidence'
Assert-Contains $corsEvidenceSource "'^[A-Za-z0-9._:=/ -]+$'" 'Bucket CORS evidence must use a fixed safe grammar'
foreach ($forbidden in @('StdOut', 'StdErr', 'Write-Host', 'Write-Output')) {
    Assert-NotContains $corsEvidenceSource $forbidden "Bucket CORS evidence writer must never emit raw process data: $forbidden"
}

foreach ($fragment in @(
    'services:', '  postgres:', '  kubo:', '  gateway:', 'image: postgres:17',
    'image: ghcr.io/hugefiver/ipfs3-kubo:latest', 'image: "${IPFS_S3_CORS_IMAGE:?required}"',
    '127.0.0.1:${IPFS_S3_CORS_POSTGRES_PORT:?required}:5432',
    '127.0.0.1:${IPFS_S3_CORS_KUBO_PORT:?required}:5001',
    '127.0.0.1:${IPFS_S3_CORS_GATEWAY_PORT:?required}:9000',
    'IPFS_S3_DATABASE_URL: postgres://ipfs3:ipfs3@postgres:5432/ipfs3',
    'IPFS_S3_KUBO_RPC_URL: http://kubo:5001', 'IPFS_S3_ACCESS_KEY_ID: test',
    'IPFS_S3_SECRET_ACCESS_KEY: test',
    'IPFS_S3_MASTER_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"',
    'ipfs3.cors.project: "${IPFS_S3_CORS_PROJECT_LABEL:?required}"',
    'ipfs3.cors.run: "${IPFS_S3_CORS_RUN_LABEL:?required}"',
    'networks:', 'cors_validation:', 'volumes:', 'postgres_data:', 'kubo_data:'
)) {
    Assert-Contains $CorsComposeSource $fragment "Bucket CORS isolated Compose topology is missing: $fragment"
}
Assert-True (([regex]::Matches($CorsComposeSource, '(?m)^  (?:postgres|kubo|gateway):$')).Count -eq 3) 'Bucket CORS Compose must contain exactly PostgreSQL, Kubo, and gateway services'
Assert-True (([regex]::Matches($CorsComposeSource, '(?m)^\s*- "127\.0\.0\.1:\$\{IPFS_S3_CORS_[A-Z_]+_PORT:\?required\}:(?:5432|5001|9000)"$')).Count -eq 3) 'Bucket CORS Compose must contain exactly three required loopback ports'
foreach ($forbidden in @('container_name:', 'pull_policy:', 'external:', 'profiles:', 'cloudflared')) {
    Assert-NotContains $CorsComposeSource $forbidden "Bucket CORS Compose must not use $forbidden"
}

$mainSource = [IO.File]::ReadAllText((Join-Path $RepoRoot 'src/main.rs')).Replace("`r`n", "`n")
$corsHttpSource = [IO.File]::ReadAllText((Join-Path $RepoRoot 'src/cors/http.rs')).Replace("`r`n", "`n")
$bridgeIndex = $mainSource.IndexOf('s3::http::bridge_chunked_content_length', [StringComparison]::Ordinal)
$corsLayerIndex = $mainSource.IndexOf('ipfs_s3_gateway::cors::http::bucket_cors', [StringComparison]::Ordinal)
Assert-True ($bridgeIndex -ge 0 -and $corsLayerIndex -gt $bridgeIndex) 'Bucket CORS middleware must remain outside bridge_chunked_content_length'
foreach ($fragment in @(
    'matches!(request.uri().path(), "/health" | "/ready")',
    'is_put_bucket_cors(&request, bucket_path.bucket_only)',
    'Request::from_parts(parts, Body::from(bytes))',
    'fn classify_bucket_path(path: &str)', 'parse_path_style(&format!("/{bucket}"))'
)) {
    Assert-Contains $corsHttpSource $fragment "Bucket CORS middleware safety contract is missing: $fragment"
}
foreach ($forbidden in @('request.headers().get(HOST)', 'HeaderName::from_static("host")', 'Host')) {
    Assert-NotContains $corsHttpSource $forbidden "Bucket CORS path classifier must not infer a bucket from Host"
}

Assert-True ($CorsEvidenceBytes.Count -lt 3 -or -not ($CorsEvidenceBytes[0] -eq 0xef -and $CorsEvidenceBytes[1] -eq 0xbb -and $CorsEvidenceBytes[2] -eq 0xbf)) 'Bucket CORS evidence must be UTF-8 without a BOM'
Assert-True (-not $CorsEvidenceRaw.Contains("`r", [StringComparison]::Ordinal)) 'Bucket CORS evidence must use LF line endings'
Assert-True ([regex]::IsMatch($CorsEvidenceRaw, '\A[\x20-\x7E\n]*\z')) 'Bucket CORS evidence must remain portable fixed text'
$corsReadmePromoted = $ReadmeSource.Contains('## Bucket CORS', [StringComparison]::Ordinal)
$corsRoadmapPromoted = $RoadmapSource.Contains('- [x] Bucket CORS configuration', [StringComparison]::Ordinal)
Assert-True ($corsReadmePromoted -and $corsRoadmapPromoted) 'Bucket CORS README and ROADMAP promotion must be atomic after evidence PASS'
Assert-True ($RoadmapSource.Contains('- [ ] Lifecycle rules (expiration, transition)', [StringComparison]::Ordinal)) 'Bucket CORS promotion must leave Lifecycle unchecked'
Assert-True (([regex]::Matches($ReadmeSource, '(?m)^## Bucket CORS$')).Count -eq 1) 'README must contain exactly one Bucket CORS section'
$readmeCorsContractSource = [regex]::Replace($ReadmeSource, '\s+', ' ').Trim()
foreach ($fragment in @(
    '[approved Bucket CORS design](docs/superpowers/specs/2026-08-31-bucket-cors-design.md)',
    '[sanitized LOCAL evidence](docs/bucket-cors-evidence-2026-08-31.log)',
    'path-style Bucket CORS', 'native signed management CRUD', 'MD5 or the AWS CLI default `CRC64NVME`',
    'unsigned preflight', 'signed actual responses, including S3 errors', 'custom import and decompress routes',
    'first matching rule wins', '`/health` and `/ready` are excluded', 'Non-goals: virtual-hosted-style routing'
)) {
    Assert-Contains $readmeCorsContractSource $fragment "README Bucket CORS promotion contract is missing: $fragment"
}

$protectedHashes = @{
    'Cargo.toml' = '86f5c654c8b54e57d4b324da0faf75df8f0353c4fa2df208da924dddc49f8764'
    '.github/workflows/ci.yml' = 'e86856e8d49671bb7eee070fcf44eb288dec4649ea107ce926512cf6a6611c34'
    '.github/workflows/docker.yml' = '433fdd62b297e4f416d16900406e3a4ddc689691cb84cdc6c0da5b2fceeed640'
    '.github/workflows/release-validation.yml' = 'eb8dc01d50eb77a988254429ba453ddb915191ed852e634e24c14b533e9e51a7'
}
foreach ($relativePath in $protectedHashes.Keys) {
    $protectedPath = Join-Path $RepoRoot $relativePath
    Assert-True (Test-Path -LiteralPath $protectedPath -PathType Leaf) "Protected CORS path is missing: $relativePath"
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $protectedPath).Hash.ToLowerInvariant()
    Assert-True ($actualHash -ceq $protectedHashes[$relativePath]) "Protected CORS path changed: $relativePath"
}
foreach ($expected in @(
    '824f5c515ce070cceecf4c7f2171e83e1e4fc039b963fda6e10780cb2dbf9ac5',
    '981a9abf7e344ddf11826c1c9c45c04c07cbbede3fefe46f2cf7fd1e6b7f7fd8'
)) {
    $path = if ($expected.StartsWith('824f', [StringComparison]::Ordinal)) {
        Join-Path $RepoRoot 'docs/superpowers/specs/2026-08-31-bucket-cors-design.md'
    } else {
        Join-Path $RepoRoot 'docs/superpowers/plans/2026-08-31-bucket-cors.md'
    }
    Assert-True (((Get-FileHash -Algorithm SHA256 -LiteralPath $path).Hash.ToLowerInvariant()) -ceq $expected) "Bucket CORS approved document hash changed: $path"
}

# Task 6 correction causal RED: these guards are intentionally added before
# their runner changes so stale fixed ports, XML policy files, and partial
# ownership cleanup cannot be mistaken for a live-validation result.
$corsCorrectionFailures = [Collections.Generic.List[string]]::new()
function Test-CorsCorrectionContract {
    param([Parameter(Mandatory)][bool]$Condition, [Parameter(Mandatory)][string]$Message)
    if (-not $Condition) { $corsCorrectionFailures.Add($Message) }
}
function Get-OptionalCorsRunnerFunctionSource {
    param([Parameter(Mandatory)][string]$Name)
    $matches = @($CorsRunnerAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $Name
    }, $true))
    if ($matches.Count -ne 1) { return '' }
    return $matches[0].Extent.Text
}

$corsPortSource = Get-OptionalCorsRunnerFunctionSource 'New-CorsLoopbackPorts'
$corsReceiptEmissionSource = Get-OptionalCorsRunnerFunctionSource 'Write-CorsReceipts'
$corsEnvironmentSource = Get-OptionalCorsRunnerFunctionSource 'Restore-EnvironmentState'
$corsEnvironmentVerifierSource = Get-OptionalCorsRunnerFunctionSource 'Test-CorsEnvironmentStateRestored'
$corsRootCleanupSource = Get-OptionalCorsRunnerFunctionSource 'Remove-OwnedCorsRunRoot'
$corsRootOwnershipSource = $corsRootCleanupSource + "`n" + (Get-CorsRunnerFunctionSource 'New-CorsRunRoot')
Test-CorsCorrectionContract ($corsPortSource.Length -gt 0) 'New-CorsLoopbackPorts is absent'
foreach ($fragment in @('49152', '65535', '[Convert]::FromHexString($RunId)', 'Distinct')) {
    Test-CorsCorrectionContract $corsPortSource.Contains($fragment, [StringComparison]::Ordinal) "RunId-derived high-loopback port contract is missing: $fragment"
}
foreach ($oldPort in @('55438', '55005', '59007')) {
    Test-CorsCorrectionContract (-not $CorsRunnerSource.Contains($oldPort, [StringComparison]::Ordinal)) "obsolete fixed operational port remains: $oldPort"
}
$corsPostgresSuiteSource = Get-CorsRunnerFunctionSource 'Invoke-CorsPostgresSuite'
Test-CorsCorrectionContract ($corsPostgresSuiteSource.Contains('$State.Ports.Postgres', [StringComparison]::Ordinal)) 'PostgreSQL URL is not derived from the owned generated port'
Test-CorsCorrectionContract ($corsBrowserSource.Contains('$State.Ports.Gateway', [StringComparison]::Ordinal)) 'Browser endpoint is not derived from the owned generated gateway port'
Test-CorsCorrectionContract (-not [regex]::IsMatch(($corsPostgresSuiteSource + "`n" + $corsBrowserSource), '127\.0\.0\.1:[0-9]{2,5}')) 'Operational PostgreSQL/browser URL contains a fixed numeric port'
Test-CorsCorrectionContract ($CorsRunnerSource.Contains('IPFS_S3_CORS_POSTGRES_PORT" -Value ([string]$state.Ports.Postgres', [StringComparison]::Ordinal) -and $CorsRunnerSource.Contains('IPFS_S3_CORS_KUBO_PORT" -Value ([string]$state.Ports.Kubo', [StringComparison]::Ordinal) -and $CorsRunnerSource.Contains('IPFS_S3_CORS_GATEWAY_PORT" -Value ([string]$state.Ports.Gateway', [StringComparison]::Ordinal)) 'Compose environment ports are not derived from the owned generated ports'
Test-CorsCorrectionContract (-not [regex]::IsMatch($CorsRunnerSource, 'Set-CorsEnvironment -Name "IPFS_S3_CORS_(?:POSTGRES|KUBO|GATEWAY)_PORT" -Value "[0-9]{2,5}"')) 'Operational Compose environment contains a fixed numeric port'
Test-CorsCorrectionContract (-not $CorsRunnerSource.Contains('if ($state.Mode -in @("full", "aws", "browser")) {', [StringComparison]::Ordinal) -and $postgresOnlySource.Contains('$State.Ports.Postgres', [StringComparison]::Ordinal) -and -not $postgresOnlySource.Contains('$State.Ports.Kubo', [StringComparison]::Ordinal) -and -not $postgresOnlySource.Contains('$State.Ports.Gateway', [StringComparison]::Ordinal)) 'PostgreSQL-only mode must own only its generated PostgreSQL port while Compose interpolation remains mode-independent'

$corsAwsParitySource = Get-CorsRunnerFunctionSource 'Invoke-CorsAwsParity'
foreach ($fragment in @('cors-policy.json', 'cors-policy-replacement.json', 'file:///work/cors-policy.json', 'file:///work/cors-policy-replacement.json', 'CORSRules', 'AllowedOrigins', 'AllowedMethods', 'AllowedHeaders', 'ExposeHeaders', 'MaxAgeSeconds', '"GET", "POST", "PUT"')) {
    Test-CorsCorrectionContract $corsAwsParitySource.Contains($fragment, [StringComparison]::Ordinal) "AWS CLI JSON CORS policy contract is missing: $fragment"
}
Test-CorsCorrectionContract (-not $CorsRunnerSource.Contains('.xml', [StringComparison]::Ordinal)) 'AWS CLI CORS policy must not use REST XML files'
foreach ($fragment in @(
    'browser-object.txt?ipfs3-import',
    'browser-object.txt?decompress-zip',
    '[Net.Http.HttpMethod]::Options',
    '"Access-Control-Request-Method", "POST"',
    '"Access-Control-Request-Method", "PUT"'
)) {
    Test-CorsCorrectionContract $corsBrowserSource.Contains($fragment, [StringComparison]::Ordinal) "Custom-route browser method mapping is missing: $fragment"
}

Test-CorsCorrectionContract ($corsReceiptEmissionSource.Length -gt 0 -and $corsReceiptEmissionSource.Contains('foreach ($receipt in $State.Receipts)', [StringComparison]::Ordinal) -and $corsReceiptEmissionSource.Contains('Write-Output', [StringComparison]::Ordinal)) 'State.Receipts deterministic emission is absent'
Test-CorsCorrectionContract ($corsReceiptEmissionSource.Contains('^\[(?:stage|command|assertion|cleanup|result)\]', [StringComparison]::Ordinal)) 'Receipt emission does not enforce a fixed receipt grammar'
Test-CorsCorrectionContract ($CorsRunnerSource.Contains('Write-CorsReceipts -State $state', [StringComparison]::Ordinal)) 'Final runner does not emit receipts before its terminal result'
Test-CorsCorrectionContract ($CorsRunnerSource.Contains('RunRootPreexisting = $false', [StringComparison]::Ordinal) -and $CorsRunnerSource.Contains('RunRootCreated = $false', [StringComparison]::Ordinal) -and $CorsRunnerSource.Contains('ReceiptOwned = $false', [StringComparison]::Ordinal) -and $CorsRunnerSource.Contains('$state.ReceiptOwned = $true', [StringComparison]::Ordinal)) 'Partial RunRoot ownership state flags are absent'
foreach ($fragment in @('$State.RunRootCreated = $true', '$State.RunRootPreexisting = $false', 'if ($State.ReceiptOwned)', 'elseif (-not $State.RunRootCreated -or $State.RunRootPreexisting)')) {
    Test-CorsCorrectionContract $corsRootOwnershipSource.Contains($fragment, [StringComparison]::Ordinal) "Partial RunRoot cleanup guard is missing: $fragment"
}
foreach ($fragment in @('GatewayImagePreflightAbsent', 'image-ownership-retry=owned', 'image-ownership-retry=absent', 'image-ownership-retry=failed', 'image-ownership-retry=not-needed', 'AllowedExitCodes @(0, 1)')) {
    Test-CorsCorrectionContract $corsCleanupSource.Contains($fragment, [StringComparison]::Ordinal) "Partial image ownership retry is missing: $fragment"
}
Test-CorsCorrectionContract ($corsEnvironmentVerifierSource.Length -gt 0) 'Test-CorsEnvironmentStateRestored is absent'
foreach ($fragment in @('$entry.Present', '$entry.Value', '-cne', 'return $false', 'return $true')) {
    Test-CorsCorrectionContract $corsEnvironmentVerifierSource.Contains($fragment, [StringComparison]::Ordinal) "Environment verifier is incomplete: $fragment"
}
$restoreIndex = $corsCleanupSource.IndexOf('Restore-EnvironmentState -State $State.EnvironmentState', [StringComparison]::Ordinal)
$verifyIndex = $corsCleanupSource.IndexOf('Test-CorsEnvironmentStateRestored -Snapshot $State.EnvironmentState', [StringComparison]::Ordinal)
$environmentPassedIndex = $corsCleanupSource.IndexOf('environment-restore=passed', [StringComparison]::Ordinal)
Test-CorsCorrectionContract ($restoreIndex -ge 0 -and $verifyIndex -gt $restoreIndex -and $environmentPassedIndex -gt $verifyIndex) 'Cleanup must restore, verify, then emit the environment success receipt'
Test-CorsCorrectionContract (-not $corsCleanupSource.Contains('$entry.Name', [StringComparison]::Ordinal) -and -not $corsCleanupSource.Contains('$entry.Value', [StringComparison]::Ordinal)) 'Cleanup environment receipts must not emit names or values'
foreach ($fragment in @('environment-restore=passed', 'environment-restore=failed', 'project-ownership-retry=owned', 'project-ownership-retry=absent', 'project-ownership-retry=failed', 'project-ownership-retry=not-needed')) {
    Test-CorsCorrectionContract $corsCleanupSource.Contains($fragment, [StringComparison]::Ordinal) "Cleanup receipt is missing: $fragment"
}
Test-CorsCorrectionContract (([regex]::Matches($CorsRunnerSource, 'cleanup-errors=\$\(\$cleanupErrors\.Count\)')).Count -eq 1) 'cleanup-errors must be emitted exactly once after all cleanup work'
Test-CorsCorrectionContract (-not $corsResidualSource.Contains('cleanup-errors=', [StringComparison]::Ordinal)) 'Residual helper emits a premature cleanup-errors receipt'
$rootCreationIndex = $corsRootOwnershipSource.IndexOf('$created = New-Item -ItemType Directory -Path $runRoot', [StringComparison]::Ordinal)
$rootOwnedIndex = $corsRootOwnershipSource.IndexOf('$State.RunRootCreated = $true', [StringComparison]::Ordinal)
Test-CorsCorrectionContract ($rootCreationIndex -ge 0 -and $rootOwnedIndex -gt $rootCreationIndex) 'RunRoot must be marked created immediately after its owned directory is made'
$projectRetryIndex = $corsCleanupSource.IndexOf('project-ownership-retry=', [StringComparison]::Ordinal)
$composeDownIndex = $corsCleanupSource.IndexOf('"down", "--volumes", "--remove-orphans"', [StringComparison]::Ordinal)
Test-CorsCorrectionContract ($projectRetryIndex -ge 0 -and $composeDownIndex -gt $projectRetryIndex) 'Project ownership retry must occur before exact Compose down'
$cleanupCountIndex = $CorsRunnerSource.LastIndexOf('cleanup-errors=$($cleanupErrors.Count)', [StringComparison]::Ordinal)
$receiptEmitIndex = $CorsRunnerSource.LastIndexOf('Write-CorsReceipts -State $state', [StringComparison]::Ordinal)
$terminalIndex = $CorsRunnerSource.LastIndexOf('Write-Output "Bucket CORS validation: FAILED"', [StringComparison]::Ordinal)
Test-CorsCorrectionContract ($cleanupCountIndex -ge 0 -and $receiptEmitIndex -gt $cleanupCountIndex -and $terminalIndex -gt $receiptEmitIndex) 'Cleanup count, receipt emission, and terminal result are not ordered fail-closed'
if ($corsCorrectionFailures.Count -ne 0) {
    throw "Bucket CORS Task 6 correction RED: $($corsCorrectionFailures -join '; ')"
}

$corsPwsh = (Get-Command pwsh -ErrorAction Stop).Source
$corsNoRun = @(& $corsPwsh -NoLogo -NoProfile -File $CorsRunnerPath 2>&1) -join "`n"
Assert-True ($LASTEXITCODE -eq 0 -and $corsNoRun -ceq 'Bucket CORS validation: NOT RUN') 'Bucket CORS no-run receipt changed or reached a preflight side effect'
foreach ($arguments in @(
    @('-Run', '-PostgresOnly'), @('-Run', '-DiagnoseAws'), @('-Run', '-DiagnoseBrowser'),
    @('-PostgresOnly', '-DiagnoseAws'), @('-PostgresOnly', '-DiagnoseBrowser'), @('-DiagnoseAws', '-DiagnoseBrowser'),
    @('-Run', '-PostgresOnly', '-DiagnoseAws'), @('-Run', '-PostgresOnly', '-DiagnoseBrowser'),
    @('-Run', '-DiagnoseAws', '-DiagnoseBrowser'), @('-PostgresOnly', '-DiagnoseAws', '-DiagnoseBrowser'),
    @('-Run', '-PostgresOnly', '-DiagnoseAws', '-DiagnoseBrowser')
)) {
    $corsMutual = @(& $corsPwsh -NoLogo -NoProfile -File $CorsRunnerPath @arguments 2>&1) -join "`n"
    Assert-True ($LASTEXITCODE -ne 0 -and $corsMutual.Contains('Bucket CORS runner modes are mutually exclusive', [StringComparison]::Ordinal) -and -not $corsMutual.Contains('Required local tool', [StringComparison]::Ordinal)) "Bucket CORS mutual exclusion must reject $($arguments -join ' + ') before external tools"
}
Write-Host 'Bucket CORS static contracts: PASSED'
