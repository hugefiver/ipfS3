#requires -Version 7.0
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$entrypoint = Join-Path ([IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))) 'ipfs/private-swarm-entrypoint.sh'
if (-not [IO.File]::Exists($entrypoint)) { throw 'Private swarm entry point is missing' }
$sh = (Get-Command sh -CommandType Application -ErrorAction Stop).Source
$temporaryKey = [IO.Path]::GetTempFileName()
try {
    $validKey = "/key/swarm/psk/1.0.0/`n/base16/`n" + ('a' * 64) + "`n"
    [IO.File]::WriteAllText($temporaryKey, $validKey, [Text.Encoding]::ASCII)
    & $sh -c '. "$1"; validate_swarm_key "$2"' sh $entrypoint $temporaryKey
    if ($LASTEXITCODE -ne 0) { throw 'Private swarm entry point rejected a valid key' }

    [IO.File]::WriteAllText($temporaryKey, $validKey.Replace(('a' * 64), ('A' + ('a' * 63))), [Text.Encoding]::ASCII)
    & $sh -c '. "$1"; validate_swarm_key "$2"' sh $entrypoint $temporaryKey
    if ($LASTEXITCODE -eq 0) { throw 'Private swarm entry point accepted uppercase key material' }

    $cid = 'Qm' + ('c' * 44)
    $filtered = @(& $sh -c '. "$1"; printf "%s\n" "Swarm key fingerprint: 0123456789abcdef0123456789abcdef" "ordinary 0123456789abcdef0123456789abcdef" "cid=$2" | redact_swarm_fingerprint' sh $entrypoint $cid)
    $expected = "Swarm key fingerprint: [redacted]`nordinary 0123456789abcdef0123456789abcdef`ncid=$cid"
    if (($filtered -join "`n") -cne $expected) { throw 'Private swarm fingerprint redaction changed safe content or exposed the fingerprint' }

    foreach ($case in @(
        @{ Daemon = 37; Filter = 0; Fifo = 0; Expected = 37 },
        @{ Daemon = 0; Filter = 1; Fifo = 0; Expected = 1 },
        @{ Daemon = 0; Filter = 0; Fifo = 1; Expected = 1 }
    )) {
        & $sh -c '. "$1"; select_supervisor_exit "$2" "$3" "$4"' sh $entrypoint $case.Daemon $case.Filter $case.Fifo
        if ($LASTEXITCODE -ne $case.Expected) { throw 'Private swarm supervisor selected the wrong exit status' }
    }
} finally {
    Remove-Item -LiteralPath $temporaryKey -Force -ErrorAction SilentlyContinue
}

Write-Host 'cluster entrypoint behavior: 6 passed; 0 failed'
