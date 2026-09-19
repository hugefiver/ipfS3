# No Docker daemon is called: this executable shadows docker for this scope.
$ErrorActionPreference = 'Stop'
$runner = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'run-lifecycle-transition-validation.ps1'))
$oldBlock = [regex]::Match($runner, '(?s)\$remainingContainers = docker ps.*?if \(\$remainingContainers.*?\{ throw ''Owned resources remain'' \}')
function docker {
    param([Parameter(ValueFromRemainingArguments)][string[]]$Arguments)
    $global:LASTEXITCODE = $script:ExitCodes[$script:CommandIndex]
    $script:CommandIndex++
}
foreach ($codes in @(@(7,0,0), @(0,8,0))) {
    $script:CommandIndex = 0; $script:ExitCodes = $codes
    $failed = $false; $Project = 'contract-owned'
    if ($oldBlock.Success) {
        try { . ([scriptblock]::Create($oldBlock.Value)) } catch { $failed = $true }
    } else {
        . (Join-Path $PSScriptRoot 'support/lifecycle-transition-runner.ps1')
        $receipt = @{status='PASS'}
        $inventories = @(Get-OwnedCleanupInventory $Project)
        try { Assert-OwnedCleanupInventory $receipt $inventories } catch { $failed = $true }
        if ($receipt.status -ne 'FAIL') { throw 'Cleanup query failure must change full PASS to FAIL' }
        if ($inventories.Count -ne 3) { throw 'Every inventory result must be captured' }
        for ($i = 0; $i -lt 3; $i++) {
            if ($inventories[$i].exit_code -ne $codes[$i]) { throw 'Inventory exit status was overwritten' }
        }
    }
    if (-not $failed) { throw "Cleanup accepted a failed inventory followed by success: $($codes -join ',')" }
}
Write-Host 'cleanup inventory contracts: 2 passed; 0 failed; 0 ignored'
