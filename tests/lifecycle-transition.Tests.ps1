#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$runnerPath = Join-Path $PSScriptRoot 'run-lifecycle-transition-validation.ps1'
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($runnerPath, [ref]$tokens, [ref]$errors)
if ($errors.Count -ne 0) { throw 'Lifecycle transition runner has syntax errors' }

. (Join-Path $PSScriptRoot 'support/lifecycle-transition-runner.ps1')
$redactors = @($ast.FindAll({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Protect-Text'
}, $true))
if ($redactors.Count -ne 1) { throw 'Lifecycle transition redactor is unavailable' }
. ([scriptblock]::Create($redactors[0].Extent.Text))

$script:Secrets = [Collections.Generic.List[string]]::new()
$script:Secrets.Add('secret-key-fixture')
$RepoRoot = 'C:\Users\fixture-user\source\ipfS3'
$safe = Protect-Text 'secret-key-fixture postgres://private:password@host/db object_id=private-object 12345678-1234-1234-1234-123456789abc'
if ($safe -match 'secret-key-fixture|password|private-object|12345678') {
    throw 'Lifecycle transition diagnostics exposed sensitive data'
}

$before = @{ head = 'fixture-head'; files = @(@{ path = 'tests/fixture.rs'; sha256 = 'before'; tracked = $false }) }
Assert-F1InputsUnchanged $before $before
$changed = @{ head = 'fixture-head'; files = @(@{ path = 'tests/fixture.rs'; sha256 = 'after'; tracked = $false }) }
$rejected = $false
try { Assert-F1InputsUnchanged $before $changed } catch { $rejected = $true }
if (-not $rejected) { throw 'Changed lifecycle transition input did not invalidate the result' }

Write-Host 'lifecycle transition behavior: 3 passed; 0 failed'
