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
    'Non-goals: Lifecycle, CORS, MFA Delete, Object Lock, pin reclamation, and replication.'
)) {
    Assert-Contains $ReadmeContractSource $fragment "README object-versioning contract is missing: $fragment"
}

$expectedRoadmapVersioningSection = @'
## v0.6 — Versioning & Lifecycle

- [x] Object versioning (enable/suspend on bucket)
- [x] ListObjectVersions
- [x] DeleteMarker support
- [ ] Lifecycle rules (expiration, transition)
- [ ] Bucket CORS configuration
'@
$roadmapVersioningSection = [regex]::Match($RoadmapSource, '(?ms)^## v0\.6 — Versioning & Lifecycle\n.*?(?=^## \S|\z)').Value.TrimEnd("`n")
Assert-True ($roadmapVersioningSection -ceq $expectedRoadmapVersioningSection.TrimEnd("`n")) "ROADMAP v0.6 must mark exactly the three delivered versioning items complete and retain Lifecycle/CORS as unchecked"
Assert-True (([regex]::Matches($RoadmapSource, '(?m)^- \[x\] (?:Object versioning \(enable/suspend on bucket\)|ListObjectVersions|DeleteMarker support)$')).Count -eq 3) "ROADMAP must contain exactly three completed object-versioning checkboxes"
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
