#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$runnerPath = Join-Path $PSScriptRoot 'run-lifecycle-transition-validation.ps1'
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($runnerPath, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Runner syntax invalid' }
$redactor = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Protect-Text'
}, $true))
if ($redactor.Count -ne 1) { throw 'Protect-Text contract not found' }

. (Join-Path $PSScriptRoot 'support/lifecycle-transition-runner.ps1')
. ([scriptblock]::Create($redactor[0].Extent.Text))
$script:Secrets = [Collections.Generic.List[string]]::new()
$RepoRoot = 'C:\Users\fixture-user\source\ipfS3'
$profileRoot = [Environment]::GetFolderPath('UserProfile').TrimEnd('\', '/')
$escapedRepo = $RepoRoot.Replace('\', '\\')
$forwardRepo = $RepoRoot.Replace('\', '/')
$input = @"
PASS exit_code=0 failed=1 tests/input.rs sha256=abc
repo=$RepoRoot\tests\input.rs
extended=\\?\$RepoRoot\tests\input.rs
json={"manifest_path":"$escapedRepo\\Cargo.toml"}
uri=path+file:///$forwardRepo#ipfs-s3-gateway@0.1.0
home=$profileRoot\.cargo\registry\fixture.rs
"@

$safe = Protect-Text $input
foreach ($privatePrefix in @($RepoRoot, $escapedRepo, $forwardRepo, $profileRoot)) {
    if ($safe.Contains($privatePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Machine path prefix was not redacted: $privatePrefix"
    }
}
foreach ($preserved in @('PASS', 'exit_code=0', 'failed=1', 'tests/input.rs', 'sha256=abc')) {
    if (-not $safe.Contains($preserved, [StringComparison]::Ordinal)) {
        throw "Non-path evidence changed: $preserved"
    }
}
if ($safe -notmatch '\[REPO\]' -or $safe -notmatch '\[USER_HOME\]') {
    throw 'Stable path placeholders were not emitted'
}
foreach ($expected in @('[REPO]\tests\input.rs', '"manifest_path":"[REPO]\\Cargo.toml"', 'file:///[REPO]#', '[USER_HOME]\.cargo\registry\fixture.rs')) {
    if (-not $safe.Contains($expected, [StringComparison]::Ordinal)) {
        throw "Path was not represented by the expected placeholder: $expected"
    }
}

Write-Host 'path redaction contracts: 1 passed; 0 failed; 0 ignored'
