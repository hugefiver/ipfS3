# Pure input identity and fail-closed cleanup helpers for the test runner.
function Protect-MachinePaths(
    [string]$Text,
    [string]$RepositoryRoot,
    [string]$UserProfile = [Environment]::GetFolderPath('UserProfile')
) {
    $roots = @(
        @{value=$RepositoryRoot; replacement='[REPO]'},
        @{value=$UserProfile; replacement='[USER_HOME]'}
    )
    foreach ($root in $roots) {
        if ([string]::IsNullOrWhiteSpace($root.value)) { continue }
        $normal = $root.value.TrimEnd('\', '/')
        $parts = @($normal -split '[\\/]' | ForEach-Object { [regex]::Escape($_) })
        $pattern = $parts -join '(?:\\{1,2}|/)'
        $Text = [regex]::Replace($Text, $pattern, $root.replacement, [Text.RegularExpressions.RegexOptions]::IgnoreCase)
    }
    $Text = [regex]::Replace($Text, '(?:\\{2,4}\?\\{1,2}|//\?/)(?=\[(?:REPO|USER_HOME)\])', '')
    return $Text
}

function Get-F1InputManifest([string]$Root) {
    $head = @(git -C $Root rev-parse HEAD)
    $headExit = $LASTEXITCODE
    if ($headExit -ne 0 -or $head.Count -ne 1) { throw 'Cannot freeze HEAD identity' }
    $tracked = @(git -C $Root ls-files --cached)
    $trackedExit = $LASTEXITCODE
    if ($trackedExit -ne 0) { throw 'Cannot inventory tracked inputs' }
    $paths = @(git -C $Root ls-files --cached --others --exclude-standard)
    $pathsExit = $LASTEXITCODE
    if ($pathsExit -ne 0) { throw 'Cannot inventory source inputs' }
    $files = @()
    foreach ($path in @($paths | Sort-Object -Unique)) {
        if ($path.StartsWith('tests/results/')) { continue }
        if ($path -notmatch '^(src/|tests/|\.cargo/|examples/|benches/|Cargo\.(toml|lock)$|build\.rs$|rust-toolchain(?:\.toml)?$|config[^/]*\.toml$)') { continue }
        $absolute = Join-Path $Root $path
        if (-not [IO.File]::Exists($absolute)) {
            $files += [ordered]@{path=$path; tracked=($path -cin $tracked); state='deleted'; bytes=0; sha256=$null}
            continue
        }
        $stream = [IO.File]::OpenRead($absolute)
        try {
            $length = $stream.Length
            $digest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($stream)).ToLowerInvariant()
        } finally { $stream.Dispose() }
        $files += [ordered]@{path=$path; tracked=($path -cin $tracked); state='present'; bytes=$length; sha256=$digest}
    }
    return [ordered]@{format=1; head=$head[0]; identity='SHA-256 of exact complete file bytes, tracked and untracked'; exclusions='tests/results (avoids evidence self-reference), non-build documentation and secret environment values'; files=$files}
}

function Assert-F1InputsUnchanged($Before, $After) {
    if (($Before | ConvertTo-Json -Depth 6 -Compress) -cne ($After | ConvertTo-Json -Depth 6 -Compress)) {
        throw 'Source/runner/config inputs changed during this invocation; no authoritative PASS'
    }
}

function Get-OwnedCleanupInventory([string]$Project) {
    $containers = @(docker ps -aq --filter "label=com.docker.compose.project=$Project" 2>&1)
    $containerExit = $LASTEXITCODE
    $volumes = @(docker volume ls -q --filter "label=com.docker.compose.project=$Project" 2>&1)
    $volumeExit = $LASTEXITCODE
    $networks = @(docker network ls -q --filter "label=com.docker.compose.project=$Project" 2>&1)
    $networkExit = $LASTEXITCODE
    return @(
        @{kind='containers'; command='docker ps -aq --filter <owned-project-label>'; output=($containers -join "`n"); exit_code=$containerExit},
        @{kind='volumes'; command='docker volume ls -q --filter <owned-project-label>'; output=($volumes -join "`n"); exit_code=$volumeExit},
        @{kind='networks'; command='docker network ls -q --filter <owned-project-label>'; output=($networks -join "`n"); exit_code=$networkExit}
    )
}

function Assert-OwnedCleanupInventory([Collections.IDictionary]$Receipt, [object[]]$Inventories) {
    $Receipt.cleanup_inventory = $Inventories
    $invalid = $Inventories.Count -ne 3
    foreach ($inventory in $Inventories) {
        if ($inventory.exit_code -ne 0 -or -not [string]::IsNullOrWhiteSpace($inventory.output)) { $invalid = $true }
    }
    if ($invalid) {
        $Receipt.status = 'FAIL'
        throw 'Owned cleanup inventory failed or resources remain'
    }
}
