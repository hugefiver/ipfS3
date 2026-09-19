#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$path = Join-Path $PSScriptRoot 'run-lifecycle-transition-validation.ps1'
$runner = [IO.File]::ReadAllText($path)
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Runner syntax invalid' }
foreach ($required in @('--pull=never', "'--pull','never'", 'RUSTUP_AUTO_INSTALL', '--offline', 'ExpectedIgnored', 'counts.passed -le 0', 'counts.ignored -ne', 'PARTIAL', 'exit 2', 'topology-logs', 'topology-down', 'hotVolume -ceq', 'hotId.ID -ceq', 'process.Kill($true)', 'label=com.docker.compose.project=$Project')) {
    if (-not $runner.Contains($required)) { throw "Missing fail-closed contract: $required" }
}
if ($runner -match '(?i)git\s+(add|commit|reset|checkout|clean)|docker\s+pull|rustup\s+(update|install)') { throw 'Forbidden mutating command' }
if ($runner.IndexOf("Dc 'topology-logs'") -gt $runner.IndexOf("Dc 'topology-down'")) { throw 'Logs must precede removal' }
$suite = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'lifecycle_transition.rs'))
foreach ($match in [regex]::Matches($suite, 'async fn (real_lifecycle_transition_\w+)\(')) {
    if (-not $runner.Contains($match.Groups[1].Value)) { throw 'Runner omitted a live test' }
}
if (-not $runner.Contains('Full gate has unexecuted required steps')) { throw 'Full gate must reject missing steps' }
$redactor = @($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Protect-Text'}, $true))
. ([scriptblock]::Create($redactor[0].Extent.Text))
$script:Secrets = [Collections.Generic.List[string]]::new()
$script:Secrets.Add('secret-key-fixture')
$safe = Protect-Text 'secret-key-fixture postgres://private:password@host/db object_id=private-object 12345678-1234-1234-1234-123456789abc'
if ($safe -match 'secret-key-fixture|password|private-object|12345678') { throw 'Sensitive diagnostic was not redacted' }
$compose = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'compose.lifecycle-transition-validation.yml'))
if ([regex]::Matches($compose, 'pull_policy: never').Count -ne 3 -or [regex]::Matches($compose, '"--offline"').Count -ne 1) { throw 'Topology must use cached images and offline cold Kubo' }
Write-Host 'lifecycle-transition runner contracts: 1 passed; 0 failed; 0 ignored'
& (Join-Path $PSScriptRoot 'cleanup-inventory.Tests.ps1')
. (Join-Path $PSScriptRoot 'support/lifecycle-transition-runner.ps1')
$before = @{head='fixture-head'; files=@(@{path='tests/fixture.rs'; sha256='before'; tracked=$false})}
Assert-F1InputsUnchanged $before $before
$changed = @{head='fixture-head'; files=@(@{path='tests/fixture.rs'; sha256='after'; tracked=$false})}
$rejected = $false
try { Assert-F1InputsUnchanged $before $changed } catch { $rejected = $true }
if (-not $rejected) { throw 'Changed untracked test input must invalidate full PASS' }
Write-Host 'input identity contracts: 2 passed; 0 failed; 0 ignored'
