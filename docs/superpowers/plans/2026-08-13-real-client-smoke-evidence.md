# Real Client Smoke Evidence Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the existing offline/no-pull rclone, MinIO mc, and AWS CLI smoke runner once inside a fail-closed Compose ownership envelope, retain truthful UTF-8 evidence, and synchronize only the compatibility documentation justified by that evidence.

**Architecture:** Keep `scripts/client-smoke.ps1` and the release-validation workflow unchanged. A PowerShell ownership envelope revalidates that the two fixed container names are absent, supplies one unique `COMPOSE_PROJECT_NAME`, captures the child runner plus project-scoped diagnostics and cleanup output with an explicit UTF-8 writer, restores the ambient environment in `finally`, and accepts the run only after exact result/evidence parsing and resource-absence checks. Documentation consumes only the accepted transcript and matching local image inspection; a failed, skipped, ambiguous, or incompletely cleaned run stops the plan before documentation changes.

**Tech Stack:** PowerShell 7, .NET `System.IO`/`System.Text` APIs, Docker Engine, Docker Compose v2, the existing `scripts/client-smoke.ps1`, rclone `1.74.4`, MinIO mc, AWS CLI v2, and Git read-only inspection commands.

**Global Constraints:**
- The authoritative specification is `docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md`; it is approved and its self-review has no unresolved ambiguity.
- Use Windows PowerShell 7 syntax for every command. Do not use Bash `export`, `&&`, `/dev/null`, or an alternate shell.
- Do not modify production Rust, S3 behavior, `docker-compose.yml`, image tags, `scripts/client-smoke.ps1`, client command semantics, tests, or the release-validation workflow.
- Do not add rclone, mc, or AWS CLI execution to `.github/workflows/release-validation.yml`; that workflow remains infrastructure-only.
- Do not pull images, install software, publish images, release, push, or tag. Every missing command, daemon, image, or offline dependency is a blocking failure, not permission to fetch it.
- The known starting state is that `ipfs-s3-kubo` and `ipfs-s3-gateway` are absent in all container states and all six required local images—three clients, two services, and `rust:latest`—exist. Revalidate both facts immediately before the run; never trust the earlier observation as the live safety receipt.
- The runner must execute exactly once as `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run` against a gateway built from the current source tree.
- Acceptance requires exactly one `PASSED` result for each of Rclone, Mc, and Aws; Mc and Aws require same-client `dual_head=PASSED`, while Rclone remains `dual_head=NOT_RUN`.
- Any runner exit failure, `status=FAILED`, `status=SKIPPED`, missing/duplicate result, missing dual-endpoint evidence, diagnostics failure, cleanup failure, environment-restoration failure, or residual owned resource blocks all later tasks. Retain that transcript unchanged; do not rerun over it, edit it, or convert it into a pass, except for the documented one-time post-review representation-only derivation of the accepted historical raw bytes.
- The tracked transcript must be written as strict UTF-8 without a BOM using .NET APIs. Do not use PowerShell `>` or `*>` file redirection for the tracked artifact because its encoding would be implicit.
- Save whether `COMPOSE_PROJECT_NAME` existed and its exact prior value, override it only inside the ownership envelope, use the same unique lowercase project for every Compose operation, and restore prior presence/value even if diagnostics or cleanup fails.
- Cleanup is fail-closed: if the fixed-name preflight refuses the run, do not clean anything; after an attempted owned run, capture non-coloured project logs before `down --volumes --remove-orphans`, target only the claimed project, and never use broad `docker rm`, prune, or unrelated volume/network deletion.
- Update matrix versions, image IDs, result fields, dates, and evidence references only from the accepted transcript and exact `docker image inspect` matches.
- Current `ROADMAP.md` has no unchecked item whose exact acceptance condition is this AWS live smoke; line 28 is already checked for the older artifact-only state. Therefore this implementation must leave `ROADMAP.md` byte-for-byte unchanged rather than editing a checked or broader milestone.
- Expected implementation changes are limited to `docs/client-smoke-evidence-2026-08-13.log` and `docs/client-compatibility.md`; the approved specification and this plan are the only accompanying process artifacts.
- Implementation workers must not stage, commit, push, tag, or perform another Git write. After identity-bound final review, return an exact commit handoff to the orchestrator; no push or tag is authorized.

**Authoritative spec:** `docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md`

## File Map

- Create `docs/client-smoke-evidence-2026-08-13.log`: immutable UTF-8 transcript containing the ownership-envelope markers, the runner's outer stdout/stderr, client image identities and versions, portable commands, dual-endpoint evidence, diagnostics, cleanup, and cleanup verification.
- Modify `docs/client-compatibility.md`: replace the stale 2026-07-19 client snapshot with values proven by the new transcript and correct the rerun procedure so it describes external project-scoped cleanup instead of the nonexistent `-CleanupVolumes` parameter.
- Verify unchanged `ROADMAP.md`: its currently checked artifact-only AWS item is not an open exact AWS smoke checkbox.
- Verify unchanged `scripts/client-smoke.ps1`: remains the sole implementation of the three client smoke paths and retains no-pull/offline/fail-closed temporary-artifact behavior.
- Verify unchanged `.github/workflows/release-validation.yml`, `tests/client-smoke.Tests.ps1`, `tests/release-validation.Tests.ps1`, and `docker-compose.yml`: real clients remain outside CI and no production/runtime/test surface changes.
- Include `docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md` and this plan in the final review/commit boundary; do not alter the approved spec during implementation.

## Post-review portable-transcript correction

The originally captured raw bytes failed portable/non-coloured review because
they contain 520 documented Cargo-registry host-prefix paths and ANSI CSI
sequences in diagnostics. The retained raw SHA-256 is
`2ee5df03e308dfb4bb4a96b6da86a19ef2fcac9639ef4633b5353766ba791d3c`.
Without rerunning Docker or a client, one deterministic representation-only
derivation is allowed: replace only the exact documented Cargo-registry prefix,
strip CSI only from the diagnostics segment, prepend a provenance receipt, and
write strict UTF-8 without a BOM. The receipt must record the raw SHA-256, exact
redaction and ANSI counts, no live rerun, `client_image_identities=3`, and that
the six-image check passed but is preflight-only. The transcript must contain
exactly the three rclone, mc, and AWS client image IDs; service/gateway/rust
identities must not be added.

The provenance receipt is also the durable restoration receipt for this
historical run: it records `compose_project_restoration=PASSED` with the
conditional `exact-case-sensitive-prior-value-or-restored-absence` check and
`orchestrator-envelope-acceptance` source, while explicitly recording the
concrete prior presence/value as unavailable. It must not invent that state.
Once transformation checks pass, the derived transcript is immutable and all
final manifests and compatibility references use its SHA-256 rather than the
retained raw source identity.

---

### Task 1: Lock the repository boundary and pass the no-side-effect preflight

**Files:**
- Verify: `docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md`
- Verify: `docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md`
- Verify: `scripts/client-smoke.ps1`
- Verify: `tests/client-smoke.Tests.ps1`
- Verify: `tests/release-validation.Tests.ps1`
- Verify absent: `docs/client-smoke-evidence-2026-08-13.log`

**Interfaces:**
- Consumes: the approved spec, the current clean implementation baseline, Docker/Compose read-only inspection surfaces, and the six exact image references hard-coded by the runner.
- Produces: a binary preflight receipt proving static contracts are GREEN, no unapproved implementation path is already dirty or staged, the fixed container names are empty, required commands are available, all required images resolve locally, and the destination transcript does not already exist.

- [ ] **Step 1: Confirm the starting Git boundary before any Docker command**

Run from the repository root:

```powershell
$allowedInitialPaths = @(
    "docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md",
    "docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md"
)
$stagedPathOutput = @(git diff --cached --name-only)
$stagedPathExit = $LASTEXITCODE
if ($stagedPathExit -ne 0) { throw "Could not inspect the staged path set" }
$stagedPaths = @($stagedPathOutput) |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
if ($stagedPaths.Count -ne 0) {
    throw "Preflight requires an empty index; staged paths: $($stagedPaths -join ', ')"
}

$workingPathOutput = @(git diff --name-only)
$workingPathExit = $LASTEXITCODE
if ($workingPathExit -ne 0) { throw "Could not inspect unstaged tracked paths" }
$untrackedPathOutput = @(git ls-files --others --exclude-standard)
$untrackedPathExit = $LASTEXITCODE
if ($untrackedPathExit -ne 0) { throw "Could not inspect untracked paths" }
$initialPaths = @($workingPathOutput) + @($untrackedPathOutput) |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    Sort-Object -Unique
$unexpectedInitialPaths = @(
    $initialPaths | Where-Object { $allowedInitialPaths -cnotcontains $_ }
)
if ($unexpectedInitialPaths.Count -ne 0) {
    throw "Unexpected pre-existing changed paths: $($unexpectedInitialPaths -join ', ')"
}
foreach ($requiredPath in $allowedInitialPaths) {
    if ($initialPaths -cnotcontains $requiredPath) {
        throw "Required approved planning artifact is absent from the working tree: $requiredPath"
    }
}
git status --short
$statusExit = $LASTEXITCODE
if ($statusExit -ne 0) { throw "Could not inspect the initial Git status" }
```

Expected: exit zero; the index is empty; only the approved spec and this plan appear as changed/untracked paths. Any other path blocks the live run until ownership of that change is resolved; do not discard or clean it.

- [ ] **Step 2: Run the two dependency-free static safety contracts**

Run sequentially:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "client-smoke infrastructure contract failed" }

pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "release-validation contract failed" }
```

Expected: both commands exit zero; the first prints `client-smoke infrastructure tests: PASSED`, and the second prints `release-validation workflow contract tests: PASSED`. The second test also proves that release validation does not invoke the real runner or a real rclone, mc, or AWS client. A failure blocks all Docker/live work; do not weaken either test.

- [ ] **Step 3: Verify command, daemon, Compose, fixed-name, image, and destination prerequisites without pulling or starting anything**

Run this read-only preflight:

```powershell
$requiredCommands = @("pwsh", "docker", "cargo", "tar.exe")
foreach ($requiredCommand in $requiredCommands) {
    if ($null -eq (Get-Command $requiredCommand -ErrorAction SilentlyContinue)) {
        throw "Required local command is missing; do not install it in this task: $requiredCommand"
    }
}

docker info --format "{{.ServerVersion}}"
if ($LASTEXITCODE -ne 0) { throw "Docker daemon preflight failed" }
docker compose version
if ($LASTEXITCODE -ne 0) { throw "Docker Compose v2 preflight failed" }

$allContainerNames = @(docker ps --all --format "{{.Names}}")
$containerListExit = $LASTEXITCODE
if ($containerListExit -ne 0) { throw "Could not enumerate all Docker container states" }
$fixedContainerNames = @("ipfs-s3-kubo", "ipfs-s3-gateway")
$fixedContainers = @(
    $allContainerNames | Where-Object { $fixedContainerNames -ccontains $_ }
)
if ($fixedContainers.Count -ne 0) {
    throw "BLOCKED: fixed smoke container names already exist; no ownership was claimed and no cleanup is allowed: $($fixedContainers -join ', ')"
}

$requiredImages = @(
    "rclone/rclone:1.74.4",
    "minio/mc:latest",
    "amazon/aws-cli:latest",
    "ghcr.io/hugefiver/ipfs3-kubo:latest",
    "ghcr.io/hugefiver/ipfs3:latest",
    "rust:latest"
)
$imageInventory = foreach ($image in $requiredImages) {
    $imageIdLines = @(docker image inspect $image --format "{{.Id}}")
    $imageInspectExit = $LASTEXITCODE
    if ($imageInspectExit -ne 0 -or $imageIdLines.Count -ne 1 -or
        $imageIdLines[0] -cnotmatch '^sha256:[0-9a-f]{64}$') {
        throw "Required image is not available locally; do not pull it: $image"
    }
    [pscustomobject]@{ Image = $image; Id = $imageIdLines[0] }
}
$imageInventory | Format-Table -AutoSize

if (Test-Path -LiteralPath "docs/client-smoke-evidence-2026-08-13.log") {
    throw "Evidence destination already exists; never overwrite or convert an earlier run: docs/client-smoke-evidence-2026-08-13.log"
}
```

Expected: exit zero; fixed-container count is exactly zero; the table contains exactly six rows with one `sha256:<64 lowercase hex>` ID each; the evidence destination is absent. The current known state should satisfy these checks. A missing image remains a hard stop—there is no `docker pull` or installation fallback.

---

### Task 2: Execute the owned live run, retain UTF-8 evidence, and prove exact acceptance

**Files:**
- Create: `docs/client-smoke-evidence-2026-08-13.log`
- Execute unchanged: `scripts/client-smoke.ps1`
- Reference unchanged: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 1's immediately preceding preflight, exact fixed names `ipfs-s3-kubo`/`ipfs-s3-gateway`, the base Compose services `kubo`/`gateway`, and the runner's anchored `[RESULT]`, `[EVIDENCE]`, client image-identity, and version-line contracts.
- Produces: one immutable strict-UTF-8 transcript and its SHA-256 identity; exact Rclone/Mc/Aws PASS receipt; Mc/Aws dual-endpoint receipt; project-scoped diagnostics; successful owned cleanup; fixed/project resource absence; and exact restoration of `COMPOSE_PROJECT_NAME`.

- [ ] **Step 1: Run the one-shot PowerShell ownership envelope**

Run the complete block below once from the repository root. It repeats the fixed-name check and separately proves that the random project label has no containers, networks, or volumes before ownership is claimed or the evidence file is created. Its best-effort UTF-8-no-BOM writer can fail without interrupting child-output capture, owned-resource discovery, diagnostics, cleanup, or residual checks; any such recording failure still rejects acceptance. Writer disposal is isolated, and exact environment restoration is the outermost independent `finally`.

```powershell
$ErrorActionPreference = "Stop"
$evidencePath = [IO.Path]::GetFullPath("docs/client-smoke-evidence-2026-08-13.log")
$composeFile = [IO.Path]::GetFullPath("docker-compose.yml")
$fixedContainerNames = @("ipfs-s3-kubo", "ipfs-s3-gateway")
$composeProject = "ipfs3-client-smoke-20260813-$PID-$([Guid]::NewGuid().ToString('N').Substring(0, 8))".ToLowerInvariant()
if ($composeProject -cnotmatch '^[a-z0-9][a-z0-9_-]+$') {
    throw "Generated invalid Compose project name: $composeProject"
}

$hadComposeProject = Test-Path Env:\COMPOSE_PROJECT_NAME
$oldComposeProject = if ($hadComposeProject) { $env:COMPOSE_PROJECT_NAME } else { $null }
$ownershipClaimed = $false
$runAttempted = $false
$stackAttempted = $false
$ownedStackEstablished = $false
$runnerExit = $null
$diagnosticsExit = $null
$cleanupExit = $null
$postCleanupVerified = $false
$primaryFailure = $null
$cleanupFailure = $null
$script:writer = $null
$script:writerFailure = $null
$script:writerCreationFailure = $null
$script:writerDisposeFailure = $null

function Convert-CapturedItemToText {
    param([AllowNull()][object]$Item)
    if ($null -eq $Item) { return "" }
    if ($Item -is [System.Management.Automation.ErrorRecord]) {
        $message = $Item.Exception.Message
        if (-not [string]::IsNullOrWhiteSpace($message) -and
            $message -cne "System.Management.Automation.RemoteException") {
            return $message
        }
    }
    return $Item.ToString()
}

function Write-CapturedLine {
    param(
        [AllowEmptyString()][string]$Text
    )
    Write-Host $Text
    if ($null -eq $script:writer -or $null -ne $script:writerFailure) { return }
    try {
        $script:writer.WriteLine($Text)
        $script:writer.Flush()
    } catch {
        $script:writerFailure = $_.Exception.Message
        Write-Warning "Transcript writer failed; console output and cleanup continue: $script:writerFailure"
    }
}

function Invoke-CapturedNative {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$ArgumentList
    )
    $lines = [Collections.Generic.List[string]]::new()
    & $FilePath @ArgumentList 2>&1 | ForEach-Object {
        $text = Convert-CapturedItemToText $_
        $lines.Add($text)
        Write-CapturedLine -Text $text
    }
    $nativeExit = $LASTEXITCODE
    return [pscustomobject]@{
        ExitCode = [int]$nativeExit
        Lines = $lines.ToArray()
    }
}

try {
try {
    $allNamesResult = @(docker ps --all --format "{{.Names}}")
    $allNamesExit = $LASTEXITCODE
    if ($allNamesExit -ne 0) { throw "Could not repeat the fixed-name preflight" }
    $existingFixed = @(
        $allNamesResult | Where-Object { $fixedContainerNames -ccontains $_ }
    )
    if ($existingFixed.Count -ne 0) {
        throw "BLOCKED: fixed smoke container names appeared before ownership; no cleanup is allowed: $($existingFixed -join ', ')"
    }

    $projectContainerPreflight = @(
        docker ps --all --filter "label=com.docker.compose.project=$composeProject" --format "{{.Names}}"
    )
    $projectContainerPreflightExit = $LASTEXITCODE
    if ($projectContainerPreflightExit -ne 0) {
        throw "Could not inspect exact-project containers before ownership"
    }
    $projectNetworkPreflight = @(
        docker network ls --filter "label=com.docker.compose.project=$composeProject" --format "{{.Name}}"
    )
    $projectNetworkPreflightExit = $LASTEXITCODE
    if ($projectNetworkPreflightExit -ne 0) {
        throw "Could not inspect exact-project networks before ownership"
    }
    $projectVolumePreflight = @(
        docker volume ls --filter "label=com.docker.compose.project=$composeProject" --format "{{.Name}}"
    )
    $projectVolumePreflightExit = $LASTEXITCODE
    if ($projectVolumePreflightExit -ne 0) {
        throw "Could not inspect exact-project volumes before ownership"
    }
    $projectContainerCollisions = @(
        $projectContainerPreflight | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $projectNetworkCollisions = @(
        $projectNetworkPreflight | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $projectVolumeCollisions = @(
        $projectVolumePreflight | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    if ($projectContainerCollisions.Count -ne 0 -or
        $projectNetworkCollisions.Count -ne 0 -or
        $projectVolumeCollisions.Count -ne 0) {
        throw "BLOCKED: random Compose project label already owns resources; no ownership was claimed and no cleanup is allowed: containers=$($projectContainerCollisions -join ',') networks=$($projectNetworkCollisions -join ',') volumes=$($projectVolumeCollisions -join ',')"
    }

    $evidenceStream = [IO.FileStream]::new(
        $evidencePath,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
        [IO.FileShare]::Read
    )
    try {
        $script:writer = [IO.StreamWriter]::new($evidenceStream, [Text.UTF8Encoding]::new($false))
        $script:writer.AutoFlush = $true
    } catch {
        $script:writerCreationFailure = $_.Exception.Message
        $evidenceStream.Dispose()
        throw
    }

    $env:COMPOSE_PROJECT_NAME = $composeProject
    $ownershipClaimed = $true
    Write-CapturedLine -Text "[ENVELOPE] compose_project=$composeProject ownership=CLAIMED fixed_preflight=EMPTY project_label_preflight=EMPTY"
    Write-CapturedLine -Text "[ENVELOPE] command=pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run"

    $runAttempted = $true
    $runnerResult = Invoke-CapturedNative `
        -FilePath "pwsh" `
        -ArgumentList @("-NoProfile", "-File", "scripts/client-smoke.ps1", "-Client", "All", "-Run")
    $runnerExit = $runnerResult.ExitCode
    $stackAttemptLines = @(
        $runnerResult.Lines | Where-Object {
            $_ -cmatch '^docker compose -f <repo>[\\/]docker-compose\.yml up -d --pull never --no-build kubo gateway$'
        }
    )
    $stackAttempted = $stackAttemptLines.Count -eq 1
    Write-CapturedLine -Text "[ENVELOPE] runner_exit=$runnerExit"
} catch {
    $primaryFailure = $_.Exception.Message
    Write-CapturedLine -Text "[ENVELOPE] primary_failure=$primaryFailure"
} finally {
    try {
        if ($runAttempted -and $ownershipClaimed) {
            $ownedContainersResult = Invoke-CapturedNative `
                -FilePath "docker" `
                -ArgumentList @(
                    "ps", "--all", "--filter", "label=com.docker.compose.project=$composeProject",
                    "--format", "{{.Names}}"
                )
            $ownedNetworksResult = Invoke-CapturedNative `
                -FilePath "docker" `
                -ArgumentList @(
                    "network", "ls", "--filter", "label=com.docker.compose.project=$composeProject",
                    "--format", "{{.Name}}"
                )
            $ownedVolumesResult = Invoke-CapturedNative `
                -FilePath "docker" `
                -ArgumentList @(
                    "volume", "ls", "--filter", "label=com.docker.compose.project=$composeProject",
                    "--format", "{{.Name}}"
                )
            if ($ownedContainersResult.ExitCode -ne 0 -or
                $ownedNetworksResult.ExitCode -ne 0 -or
                $ownedVolumesResult.ExitCode -ne 0) {
                throw "Could not determine whether the unique Compose project owns resources"
            }
            $ownedContainerCount = @(
                $ownedContainersResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            ).Count
            $ownedNetworkCount = @(
                $ownedNetworksResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            ).Count
            $ownedVolumeCount = @(
                $ownedVolumesResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
            ).Count
            $ownedStackEstablished =
                $ownedContainerCount -gt 0 -or
                $ownedNetworkCount -gt 0 -or
                $ownedVolumeCount -gt 0
            Write-CapturedLine -Text "[ENVELOPE] stack_attempted=$($stackAttempted.ToString().ToUpperInvariant()) owned_stack=$($ownedStackEstablished.ToString().ToUpperInvariant()) owned_containers=$ownedContainerCount owned_networks=$ownedNetworkCount owned_volumes=$ownedVolumeCount"

            if ($stackAttempted -or $ownedStackEstablished) {
                try {
                    Write-CapturedLine -Text "[ENVELOPE] diagnostics=BEGIN"
                    $diagnosticsResult = Invoke-CapturedNative `
                        -FilePath "docker" `
                        -ArgumentList @(
                            "compose", "--project-name", $composeProject,
                            "-f", $composeFile, "logs", "--no-color", "kubo", "gateway"
                        )
                    $diagnosticsExit = $diagnosticsResult.ExitCode
                    Write-CapturedLine -Text "[ENVELOPE] diagnostics_exit=$diagnosticsExit"
                } catch {
                    $diagnosticsExit = -1
                    Write-CapturedLine -Text "[ENVELOPE] diagnostics_failure=$($_.Exception.Message)"
                }
            }

            if ($ownedStackEstablished) {
                try {
                    Write-CapturedLine -Text "[ENVELOPE] cleanup=BEGIN"
                    $cleanupResult = Invoke-CapturedNative `
                        -FilePath "docker" `
                        -ArgumentList @(
                            "compose", "--project-name", $composeProject,
                            "-f", $composeFile, "down", "--volumes", "--remove-orphans"
                        )
                    $cleanupExit = $cleanupResult.ExitCode
                    Write-CapturedLine -Text "[ENVELOPE] cleanup_exit=$cleanupExit"
                } catch {
                    $cleanupExit = -1
                    Write-CapturedLine -Text "[ENVELOPE] cleanup_failure=$($_.Exception.Message)"
                }
            } else {
                Write-CapturedLine -Text "[ENVELOPE] cleanup=NOT_RUN reason=OWNERSHIP_NOT_ESTABLISHED"
            }

            try {
                $projectContainersResult = Invoke-CapturedNative `
                    -FilePath "docker" `
                    -ArgumentList @(
                        "ps", "--all", "--filter", "label=com.docker.compose.project=$composeProject",
                        "--format", "{{.Names}}"
                    )
                $projectNetworksResult = Invoke-CapturedNative `
                    -FilePath "docker" `
                    -ArgumentList @(
                        "network", "ls", "--filter", "label=com.docker.compose.project=$composeProject",
                        "--format", "{{.Name}}"
                    )
                $projectVolumesResult = Invoke-CapturedNative `
                    -FilePath "docker" `
                    -ArgumentList @(
                        "volume", "ls", "--filter", "label=com.docker.compose.project=$composeProject",
                        "--format", "{{.Name}}"
                    )
                $fixedNamesResult = Invoke-CapturedNative `
                    -FilePath "docker" `
                    -ArgumentList @("ps", "--all", "--format", "{{.Names}}")

                $remainingProjectContainers = @(
                    $projectContainersResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
                )
                $remainingProjectNetworks = @(
                    $projectNetworksResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
                )
                $remainingProjectVolumes = @(
                    $projectVolumesResult.Lines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
                )
                $remainingFixed = @(
                    $fixedNamesResult.Lines | Where-Object { $fixedContainerNames -ccontains $_ }
                )
                $postCleanupVerified =
                    $projectContainersResult.ExitCode -eq 0 -and
                    $projectNetworksResult.ExitCode -eq 0 -and
                    $projectVolumesResult.ExitCode -eq 0 -and
                    $fixedNamesResult.ExitCode -eq 0 -and
                    $remainingProjectContainers.Count -eq 0 -and
                    $remainingProjectNetworks.Count -eq 0 -and
                    $remainingProjectVolumes.Count -eq 0 -and
                    $remainingFixed.Count -eq 0
                $cleanupVerification = if ($postCleanupVerified) { "PASSED" } else { "FAILED" }
                Write-CapturedLine -Text "[ENVELOPE] cleanup_verification=$cleanupVerification project_containers=$($remainingProjectContainers.Count) project_networks=$($remainingProjectNetworks.Count) project_volumes=$($remainingProjectVolumes.Count) fixed_containers=$($remainingFixed.Count)"
            } catch {
                $postCleanupVerified = $false
                Write-CapturedLine -Text "[ENVELOPE] cleanup_verification_failure=$($_.Exception.Message)"
            }
        }
    } catch {
        $cleanupFailure = $_.Exception.Message
        Write-CapturedLine -Text "[ENVELOPE] cleanup_envelope_failure=$cleanupFailure"
    } finally {
        try {
            if ($null -ne $script:writer) { $script:writer.Dispose() }
        } catch {
            $script:writerDisposeFailure = $_.Exception.Message
            Write-Warning "Transcript writer disposal failed: $script:writerDisposeFailure"
        }
    }
}
} finally {
    if ($hadComposeProject) {
        $env:COMPOSE_PROJECT_NAME = $oldComposeProject
    } else {
        Remove-Item Env:\COMPOSE_PROJECT_NAME -ErrorAction SilentlyContinue
    }
}

$composeProjectRestored = if ($hadComposeProject) {
    (Test-Path Env:\COMPOSE_PROJECT_NAME) -and
        $env:COMPOSE_PROJECT_NAME -ceq $oldComposeProject
} else {
    -not (Test-Path Env:\COMPOSE_PROJECT_NAME)
}

if (-not (Test-Path -LiteralPath $evidencePath -PathType Leaf)) {
    throw "Live envelope did not create the evidence file: $primaryFailure"
}

$acceptanceErrors = [Collections.Generic.List[string]]::new()
if ($null -ne $primaryFailure) { $acceptanceErrors.Add("primary failure: $primaryFailure") }
if ($null -ne $cleanupFailure) { $acceptanceErrors.Add("cleanup envelope failure: $cleanupFailure") }
if ($null -ne $script:writerCreationFailure) { $acceptanceErrors.Add("transcript writer creation failure: $script:writerCreationFailure") }
if ($null -ne $script:writerFailure) { $acceptanceErrors.Add("transcript writer failure: $script:writerFailure") }
if ($null -ne $script:writerDisposeFailure) { $acceptanceErrors.Add("transcript writer disposal failure: $script:writerDisposeFailure") }
if ($runnerExit -ne 0) { $acceptanceErrors.Add("runner exit was $runnerExit, expected 0") }
if (-not $stackAttempted) { $acceptanceErrors.Add("runner did not emit exactly one stack-attempt command") }
if (-not $ownedStackEstablished) { $acceptanceErrors.Add("unique Compose project ownership was never established") }
if ($diagnosticsExit -ne 0) { $acceptanceErrors.Add("diagnostics exit was $diagnosticsExit, expected 0") }
if ($cleanupExit -ne 0) { $acceptanceErrors.Add("cleanup exit was $cleanupExit, expected 0") }
if (-not $postCleanupVerified) { $acceptanceErrors.Add("owned/fixed resource absence was not proven") }
if (-not $composeProjectRestored) { $acceptanceErrors.Add("COMPOSE_PROJECT_NAME was not restored exactly") }

$evidenceBytes = [IO.File]::ReadAllBytes($evidencePath)
if ($evidenceBytes.Length -ge 3 -and
    $evidenceBytes[0] -eq 0xEF -and $evidenceBytes[1] -eq 0xBB -and $evidenceBytes[2] -eq 0xBF) {
    $acceptanceErrors.Add("evidence contains an unexpected UTF-8 BOM")
}
try {
    $evidenceText = [Text.UTF8Encoding]::new($false, $true).GetString($evidenceBytes).
        Replace("`r`n", "`n").
        Replace("`r", "`n")
} catch {
    $evidenceText = ""
    $acceptanceErrors.Add("evidence is not strict UTF-8: $($_.Exception.Message)")
}

$evidenceLines = @($evidenceText -split "`n")
$resultLines = @($evidenceLines | Where-Object { $_ -cmatch '^\[RESULT\] client=' })
$expectedResultLines = @(
    "[RESULT] client=Rclone status=PASSED dual_head=NOT_RUN detail=all commands and assertions completed evidence=client-smoke.log",
    "[RESULT] client=Mc status=PASSED dual_head=PASSED detail=all commands and assertions completed evidence=client-smoke.log",
    "[RESULT] client=Aws status=PASSED dual_head=PASSED detail=all commands and assertions completed evidence=client-smoke.log"
)
if ($resultLines.Count -ne 3) {
    $acceptanceErrors.Add("result-line count was $($resultLines.Count), expected 3")
} elseif (($resultLines -join "`n") -cne ($expectedResultLines -join "`n")) {
    $acceptanceErrors.Add("result lines were not the exact ordered Rclone/Mc/Aws PASS receipt")
}
$blockingResults = @(
    $resultLines | Where-Object { $_ -cmatch ' status=(?:FAILED|SKIPPED)(?: |$)' }
)
if ($blockingResults.Count -ne 0) {
    $acceptanceErrors.Add("transcript contains FAILED/SKIPPED result: $($blockingResults -join ' | ')")
}

$dualEvidencePatterns = @(
    '^\[EVIDENCE\] client=Mc verifier=Mc dual_head=PASSED key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=(?:Qm|baf)[A-Za-z0-9]+ content_length=[0-9]+$',
    '^\[EVIDENCE\] client=Aws verifier=Aws dual_head=PASSED key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=(?:Qm|baf)[A-Za-z0-9]+ content_length=[0-9]+$'
)
foreach ($dualEvidencePattern in $dualEvidencePatterns) {
    $dualMatches = @($evidenceLines | Where-Object { $_ -cmatch $dualEvidencePattern })
    if ($dualMatches.Count -ne 1) {
        $acceptanceErrors.Add("dual-endpoint evidence count was $($dualMatches.Count), expected 1: $dualEvidencePattern")
    }
}

$identityPatterns = @(
    '^client=Rclone image=rclone/rclone:1\.74\.4 image_id=sha256:[0-9a-f]{64}$',
    '^client=Mc image=minio/mc:latest image_id=sha256:[0-9a-f]{64}$',
    '^client=Aws image=amazon/aws-cli:latest image_id=sha256:[0-9a-f]{64}$'
)
foreach ($identityPattern in $identityPatterns) {
    $identityMatches = @($evidenceLines | Where-Object { $_ -cmatch $identityPattern })
    if ($identityMatches.Count -ne 1) {
        $acceptanceErrors.Add("client image identity count was $($identityMatches.Count), expected 1: $identityPattern")
    }
}
foreach ($versionPattern in @(
    '^rclone v1\.74\.4$',
    '^mc version RELEASE\.[^ ]+ \(commit-id=[0-9a-f]+\)$',
    '^aws-cli/[^ ]+ .+$'
)) {
    $versionMatches = @($evidenceLines | Where-Object { $_ -cmatch $versionPattern })
    if ($versionMatches.Count -ne 1) {
        $acceptanceErrors.Add("client version count was $($versionMatches.Count), expected 1: $versionPattern")
    }
}

foreach ($requiredFragment in @(
    "ownership=CLAIMED fixed_preflight=EMPTY project_label_preflight=EMPTY",
    "cargo vendor --locked --offline",
    "docker build --pull=false --network none",
    "docker compose -f <repo>",
    "--pull never --no-build kubo gateway",
    "[ENVELOPE] diagnostics_exit=0",
    "[ENVELOPE] cleanup_exit=0",
    "[ENVELOPE] cleanup_verification=PASSED"
)) {
    if (-not $evidenceText.Contains($requiredFragment, [StringComparison]::Ordinal)) {
        $acceptanceErrors.Add("required transcript fragment is missing: $requiredFragment")
    }
}
if ([regex]::IsMatch($evidenceText, '(?m)^docker pull(?: |$)')) {
    $acceptanceErrors.Add("transcript contains a forbidden docker pull command")
}

if ($acceptanceErrors.Count -ne 0) {
    throw "LIVE EVIDENCE REJECTED; retain the log and stop before docs: $($acceptanceErrors -join '; ')"
}

$evidenceSha256 = (Get-FileHash -LiteralPath $evidencePath -Algorithm SHA256).Hash.ToLowerInvariant()
Write-Host "LIVE EVIDENCE ACCEPTED sha256=$evidenceSha256 compose_project=$composeProject"
```

Expected success: the block exits zero and prints exactly one final `LIVE EVIDENCE ACCEPTED sha256=<64 lowercase hex> compose_project=<unique project>` line. Before ownership and file creation, all three exact project-label queries pass and return empty; the transcript records `fixed_preflight=EMPTY project_label_preflight=EMPTY`, one ordered PASS result per client, one Mc and one Aws dual-endpoint line, three client image IDs, all three client versions, diagnostics before cleanup, `cleanup_exit=0`, and `cleanup_verification=PASSED`. No fixed/project container, network, or volume remains; writer creation/write/flush/disposal has no failure; the previous presence and value of `COMPOSE_PROJECT_NAME` are exactly restored.

Expected failure: a fixed-name or random-project-label collision before `$ownershipClaimed = $true` exits without creating the evidence file and without diagnostics or cleanup. After `$runAttempted = $true`, the envelope captures diagnostics if the runner emitted its Compose-up command or exact project labels prove resources, but executes `down` only when those unique project labels establish ownership; a partial owned stack is therefore cleaned while a pre-Compose build failure cannot trigger cleanup. Writer failure switches logging to console only, never aborts discovery/diagnostics/down/residual checks or outermost environment restoration, and independently blocks acceptance. A rejected transcript remains at `docs/client-smoke-evidence-2026-08-13.log`, and Tasks 3–4 must not relabel it, overwrite it, edit it, or continue toward a commit.

---

### Task 3: Derive the compatibility snapshot and apply the minimum documentation sync

**Files:**
- Modify: `docs/client-compatibility.md`
- Consume unchanged: `docs/client-smoke-evidence-2026-08-13.log`
- Verify unchanged: `ROADMAP.md`

**Interfaces:**
- Consumes: Task 2's accepted strict-UTF-8 transcript, exact client image/version lines, exact PASS/dual-endpoint lines, and matching local image IDs.
- Produces: a truthful 2026-08-13 compatibility snapshot for the three Docker clients, corrected external-envelope rerun guidance, no stale AWS skip claim, no nonexistent runner switch, and a byte-for-byte unchanged ROADMAP.

- [ ] **Step 1: Extract one exact version and image ID per client and cross-check the current local tags**

Run:

```powershell
$evidencePath = "docs/client-smoke-evidence-2026-08-13.log"
$evidence = [Text.UTF8Encoding]::new($false, $true).
    GetString([IO.File]::ReadAllBytes($evidencePath)).
    Replace("`r`n", "`n").
    Replace("`r", "`n")

function Get-SingleEvidenceCapture {
    param(
        [Parameter(Mandatory)][string]$Pattern,
        [Parameter(Mandatory)][string]$Group,
        [Parameter(Mandatory)][string]$Label
    )
    $matches = [regex]::Matches($evidence, $Pattern)
    if ($matches.Count -ne 1) {
        throw "$Label evidence count was $($matches.Count), expected exactly one"
    }
    return $matches[0].Groups[$Group].Value
}

$observed = [ordered]@{
    Rclone = [pscustomobject]@{
        Image = "rclone/rclone:1.74.4"
        ImageId = Get-SingleEvidenceCapture '(?m)^client=Rclone image=rclone/rclone:1\.74\.4 image_id=(?<value>sha256:[0-9a-f]{64})$' 'value' 'Rclone image'
        Version = Get-SingleEvidenceCapture '(?m)^rclone v(?<value>1\.74\.4)$' 'value' 'Rclone version'
    }
    Mc = [pscustomobject]@{
        Image = "minio/mc:latest"
        ImageId = Get-SingleEvidenceCapture '(?m)^client=Mc image=minio/mc:latest image_id=(?<value>sha256:[0-9a-f]{64})$' 'value' 'Mc image'
        Version = Get-SingleEvidenceCapture '(?m)^mc version (?<value>RELEASE\.[^ ]+) \(commit-id=[0-9a-f]+\)$' 'value' 'Mc version'
    }
    Aws = [pscustomobject]@{
        Image = "amazon/aws-cli:latest"
        ImageId = Get-SingleEvidenceCapture '(?m)^client=Aws image=amazon/aws-cli:latest image_id=(?<value>sha256:[0-9a-f]{64})$' 'value' 'AWS image'
        Version = Get-SingleEvidenceCapture '(?m)^aws-cli/(?<value>[^ ]+) .+$' 'value' 'AWS version'
    }
}

foreach ($entry in $observed.GetEnumerator()) {
    $localIdLines = @(docker image inspect $entry.Value.Image --format "{{.Id}}")
    $inspectExit = $LASTEXITCODE
    if ($inspectExit -ne 0 -or $localIdLines.Count -ne 1) {
        throw "Could not inspect the accepted local image tag: $($entry.Value.Image)"
    }
    if ($localIdLines[0] -cne $entry.Value.ImageId) {
        throw "Local image identity changed after the accepted run: $($entry.Value.Image) transcript=$($entry.Value.ImageId) local=$($localIdLines[0])"
    }
}
$observed.GetEnumerator() | ForEach-Object {
    [pscustomobject]@{
        Client = $_.Key
        Version = $_.Value.Version
        Image = $_.Value.Image
        ImageId = $_.Value.ImageId
    }
} | Format-Table -AutoSize
```

Expected: exit zero and exactly three rows. Every local ID exactly equals the ID captured during the accepted run. A changed/missing tag blocks documentation rather than silently mixing two image identities; do not pull the old image.

- [ ] **Step 2: Update only fields supported by the accepted evidence**

Edit `docs/client-compatibility.md` with these exact semantic changes:

1. Change `Compatibility matrix — evidence snapshot 2026-07-19` to `Compatibility matrix — evidence snapshot 2026-08-13`.
2. In the endpoint contract, state that `http://127.0.0.1:9000` is used by both Mc and AWS same-client dual-endpoint HEAD/stat verification; keep rclone explicitly Compose-network-only.
3. In the rclone row, use the extracted `Rclone.Version` and `Rclone.ImageId`, retain the fixed tag `rclone/rclone:1.74.4`, retain the existing operation/options/limitation scope, set result/date to `PASSED`/`2026-08-13`, and cite `docs/client-smoke-evidence-2026-08-13.log` plus `[RESULT] client=Rclone status=PASSED dual_head=NOT_RUN`.
4. In the MinIO mc row, use the extracted `Mc.Version` and `Mc.ImageId`, retain the operation/config/known-short-secret explanation actually exercised by the runner, set result/date to `PASSED`/`2026-08-13`, and cite both `[RESULT] client=Mc status=PASSED dual_head=PASSED` and `[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED` from the new log.
5. Replace the stale AWS absent/SKIPPED row with the extracted `Aws.Version` and `Aws.ImageId`; retain its actual operations (`mb`, `cp`, `ls`, `get-bucket-location`, ListObjects v1, `head-object`, `delete-objects`, `rm`, `rb`), set result/date to `PASSED`/`2026-08-13`, and cite both `[RESULT] client=Aws status=PASSED dual_head=PASSED` and `[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED`. Remove only the old absent-image limitation; do not add unexecuted claims.
6. Leave the Rust integration harness row and its 2026-07-19 evidence date unchanged because this live transcript does not re-execute or version that harness.
7. Replace the rerun section with a concise external-envelope procedure that requires: dependency-free static tests first; an all-state fixed-name refusal before ownership; all six local image inspections with no pull/install fallback; one unique lowercase `COMPOSE_PROJECT_NAME`; exact prior-presence/value restoration in `finally`; strict UTF-8 capture with `[IO.StreamWriter]`/`[Text.UTF8Encoding]::new($false)` rather than `>`/`*>`; exact three-result and Mc/Aws dual-evidence parsing; project-scoped non-coloured logs before `docker compose --project-name <claimed-project> -f docker-compose.yml down --volumes --remove-orphans`; and residual-resource verification. State that FAILED/SKIPPED/nonzero/cleanup failure blocks acceptance and that a failed transcript is retained unchanged.
8. Remove the nonexistent `-CleanupVolumes` instruction and every implication that the runner itself cleans Compose resources. Do not document a new runner parameter.

Do not alter `ROADMAP.md`. Current repository inspection found no unchecked exact AWS smoke item, so the spec's conditional ROADMAP branch is false. If the ROADMAP state changes before execution, stop and request a plan refresh rather than editing a checked item or broader milestone by inference.

- [ ] **Step 3: Verify the documentation agrees exactly with runtime evidence**

Run this binary documentation audit in the same PowerShell session as Step 1 so `$observed` is defined:

```powershell
$compatibility = [IO.File]::ReadAllText("docs/client-compatibility.md").
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$compatibilityLines = @($compatibility -split "`n")

if (-not $compatibility.Contains(
    "## Compatibility matrix — evidence snapshot 2026-08-13",
    [StringComparison]::Ordinal
)) { throw "Compatibility snapshot date was not updated exactly" }

$rows = [ordered]@{
    Rclone = @($compatibilityLines | Where-Object { $_ -cmatch '^\| rclone \|' })
    Mc = @($compatibilityLines | Where-Object { $_ -cmatch '^\| MinIO mc \|' })
    Aws = @($compatibilityLines | Where-Object { $_ -cmatch '^\| AWS CLI \|' })
    Rust = @($compatibilityLines | Where-Object { $_ -cmatch '^\| Rust integration harness \|' })
}
foreach ($row in $rows.GetEnumerator()) {
    if ($row.Value.Count -ne 1) {
        throw "$($row.Key) matrix row count was $($row.Value.Count), expected one"
    }
}

foreach ($client in @("Rclone", "Mc", "Aws")) {
    $row = $rows[$client][0]
    foreach ($requiredValue in @(
        $observed[$client].Version,
        $observed[$client].ImageId,
        "PASSED",
        "2026-08-13",
        "docs/client-smoke-evidence-2026-08-13.log"
    )) {
        if (-not $row.Contains($requiredValue, [StringComparison]::Ordinal)) {
            throw "$client row is missing accepted value: $requiredValue"
        }
    }
}
if ($rows.Rclone[0] -cnotmatch 'dual_head=NOT_RUN') { throw "Rclone row lost its NOT_RUN scope" }
if ($rows.Mc[0] -cnotmatch 'client=Mc verifier=Mc.*dual_head=PASSED') { throw "Mc row lacks dual-endpoint evidence" }
if ($rows.Aws[0] -cnotmatch 'client=Aws verifier=Aws.*dual_head=PASSED') { throw "AWS row lacks dual-endpoint evidence" }
if ($rows.Aws[0] -cmatch 'SKIPPED|absent|not executed') { throw "AWS row retains stale skip language" }
if ($rows.Rust[0] -cnotmatch 'PASSED \| 2026-07-19 \|') { throw "Unexecuted Rust evidence date changed" }
if ($compatibility.Contains("CleanupVolumes", [StringComparison]::Ordinal)) {
    throw "Compatibility docs still reference the nonexistent CleanupVolumes parameter"
}

git diff --exit-code -- ROADMAP.md
$roadmapDiffExit = $LASTEXITCODE
if ($roadmapDiffExit -ne 0) { throw "ROADMAP changed even though no exact unchecked AWS smoke item exists" }

Write-Host "compatibility evidence synchronization: PASSED"
```

Expected: exit zero and print `compatibility evidence synchronization: PASSED`; the three Docker rows contain the exact observed values, the AWS row is no longer stale, the Rust row remains historically honest, `CleanupVolumes` is absent, and `ROADMAP.md` has no diff.

---

### Task 4: Run static verification, enforce the final boundary, and hand off identity-bound review/commit

**Files:**
- Verify: `docs/client-smoke-evidence-2026-08-13.log`
- Verify: `docs/client-compatibility.md`
- Verify unchanged: `ROADMAP.md`
- Verify unchanged: `scripts/client-smoke.ps1`
- Verify unchanged: `.github/workflows/release-validation.yml`
- Verify unchanged: `tests/client-smoke.Tests.ps1`
- Verify unchanged: `tests/release-validation.Tests.ps1`
- Review artifacts: `docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md`, `docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md`

**Interfaces:**
- Consumes: Task 2's accepted immutable evidence SHA-256 and Task 3's evidence-derived compatibility snapshot.
- Produces: GREEN static contracts, whitespace and exact-path receipts, a HEAD-plus-file-hash review identity, final acceptance-review approval for that exact identity, and a commit-ready handoff that authorizes no push/tag and performs no Git write in an implementation worker.

- [ ] **Step 1: Re-run both static contracts and prove forbidden implementation surfaces are unchanged**

Run sequentially:

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "client-smoke infrastructure contract failed" }

pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "release-validation contract failed" }

git diff --exit-code -- `
    scripts/client-smoke.ps1 `
    docker-compose.yml `
    .github/workflows/release-validation.yml `
    tests/client-smoke.Tests.ps1 `
    tests/release-validation.Tests.ps1
$forbiddenSurfaceDiffExit = $LASTEXITCODE
if ($forbiddenSurfaceDiffExit -ne 0) { throw "A forbidden runner/Compose/workflow/test surface changed" }
```

Expected: both tests exit zero with their exact `PASSED` messages; `git diff --exit-code` emits nothing and exits zero. In particular, no real clients have been added to release validation and the runner has not gained cleanup or other new behavior.

- [ ] **Step 2: Re-parse the retained transcript independently and verify UTF-8, binary results, and evidence identity**

Run:

```powershell
$evidencePath = "docs/client-smoke-evidence-2026-08-13.log"
$bytes = [IO.File]::ReadAllBytes($evidencePath)
if ($bytes.Length -eq 0) { throw "Evidence file is empty" }
if ($bytes.Length -ge 3 -and $bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) {
    throw "Evidence file unexpectedly has a BOM"
}
$text = [Text.UTF8Encoding]::new($false, $true).GetString($bytes).
    Replace("`r`n", "`n").
    Replace("`r", "`n")
$lines = @($text -split "`n")
$expectedResults = @(
    "[RESULT] client=Rclone status=PASSED dual_head=NOT_RUN detail=all commands and assertions completed evidence=client-smoke.log",
    "[RESULT] client=Mc status=PASSED dual_head=PASSED detail=all commands and assertions completed evidence=client-smoke.log",
    "[RESULT] client=Aws status=PASSED dual_head=PASSED detail=all commands and assertions completed evidence=client-smoke.log"
)
$actualResults = @($lines | Where-Object { $_ -cmatch '^\[RESULT\] client=' })
if (($actualResults -join "`n") -cne ($expectedResults -join "`n")) {
    throw "Final transcript result receipt is not the exact three-client PASS sequence"
}
foreach ($client in @("Mc", "Aws")) {
    $dualLines = @(
        $lines | Where-Object {
            $_ -cmatch "^\[EVIDENCE\] client=$client verifier=$client dual_head=PASSED "
        }
    )
    if ($dualLines.Count -ne 1) { throw "$client final dual-evidence count was $($dualLines.Count)" }
}
if ($actualResults -cmatch 'status=(?:FAILED|SKIPPED)') { throw "Final transcript contains a blocking result" }
foreach ($marker in @(
    "ownership=CLAIMED fixed_preflight=EMPTY project_label_preflight=EMPTY",
    "[ENVELOPE] runner_exit=0",
    "[ENVELOPE] diagnostics_exit=0",
    "[ENVELOPE] cleanup_exit=0",
    "[ENVELOPE] cleanup_verification=PASSED"
)) {
    if (-not $text.Contains($marker, [StringComparison]::Ordinal)) {
        throw "Final transcript is missing envelope marker: $marker"
    }
}
$finalEvidenceSha256 = (Get-FileHash -LiteralPath $evidencePath -Algorithm SHA256).Hash.ToLowerInvariant()
Write-Host "final transcript verification: PASSED sha256=$finalEvidenceSha256"
```

Expected: exit zero and one `final transcript verification: PASSED sha256=<64 lowercase hex>` line. The SHA-256 must equal the accepted Task 2 identity; a mismatch means the evidence changed after acceptance and invalidates documentation/review.

- [ ] **Step 3: Run whitespace and exact changed-path boundary audits without staging**

Run:

```powershell
git diff --check
$diffCheckExit = $LASTEXITCODE
if ($diffCheckExit -ne 0) { throw "Git diff whitespace check failed" }

$allowedPaths = @(
    "docs/client-compatibility.md",
    "docs/client-smoke-evidence-2026-08-13.log",
    "docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md",
    "docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md"
)
$workingPathOutput = @(git diff --name-only)
$workingPathExit = $LASTEXITCODE
if ($workingPathExit -ne 0) { throw "Could not inspect unstaged tracked paths" }
$stagedPathOutput = @(git diff --cached --name-only)
$stagedPathExit = $LASTEXITCODE
if ($stagedPathExit -ne 0) { throw "Could not inspect staged paths" }
$untrackedPathOutput = @(git ls-files --others --exclude-standard)
$untrackedPathExit = $LASTEXITCODE
if ($untrackedPathExit -ne 0) { throw "Could not inspect untracked paths" }
$changedPaths = @($workingPathOutput) + @($stagedPathOutput) + @($untrackedPathOutput) |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
    Sort-Object -Unique
$unexpectedPaths = @($changedPaths | Where-Object { $allowedPaths -cnotcontains $_ })
if ($unexpectedPaths.Count -ne 0) {
    throw "Out-of-scope changed paths: $($unexpectedPaths -join ', ')"
}
foreach ($requiredPath in $allowedPaths) {
    if ($changedPaths -cnotcontains $requiredPath) {
        throw "Expected final path is absent: $requiredPath"
    }
}

$stagedPaths = @($stagedPathOutput) |
    Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
if ($stagedPaths.Count -ne 0) {
    throw "Implementation worker staged files before final review: $($stagedPaths -join ', ')"
}

foreach ($path in @(
    "docs/client-compatibility.md",
    "docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md",
    "docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md"
)) {
    $lineNumber = 0
    foreach ($line in [IO.File]::ReadAllLines($path)) {
        $lineNumber++
        if ($line -cmatch '[\t ]+$') {
            throw "Trailing whitespace in ${path}:$lineNumber"
        }
    }
}

git diff --exit-code -- ROADMAP.md
$roadmapDiffExit = $LASTEXITCODE
if ($roadmapDiffExit -ne 0) { throw "ROADMAP is outside the current conditional boundary" }
git status --short
$statusExit = $LASTEXITCODE
if ($statusExit -ne 0) { throw "Could not inspect final Git status" }
Write-Host "repository boundary verification: PASSED"
```

Expected: `git diff --check` exits zero; exactly the four approved paths are changed/untracked; no path is staged; no approved artifact has trailing whitespace; `ROADMAP.md` is unchanged; the final line is `repository boundary verification: PASSED`.

- [ ] **Step 4: Record the binary evidence matrix**

Return this completed matrix to the orchestrator and final reviewer, using only `PASS` or `FAIL`—there is no accepted `SKIP`/`UNVERIFIED` state for this task:

```text
STATIC client-smoke infrastructure: PASS | FAIL
STATIC release-validation contract: PASS | FAIL
PREFLIGHT fixed containers empty: PASS | FAIL
PREFLIGHT six local images present: PASS | FAIL
LIVE runner exit zero: PASS | FAIL
LIVE Rclone PASSED/NOT_RUN: PASS | FAIL
LIVE Mc PASSED/PASSED + verifier evidence: PASS | FAIL
LIVE Aws PASSED/PASSED + verifier evidence: PASS | FAIL
DIAGNOSTICS project logs captured: PASS | FAIL
CLEANUP project down + zero residual resources: PASS | FAIL
ENV COMPOSE_PROJECT_NAME restored exactly: PASS | FAIL
DOCS matrix/runtime agreement: PASS | FAIL
ROADMAP unchanged under closed conditional: PASS | FAIL
BOUNDARY/whitespace/status: PASS | FAIL
```

Expected: every row is `PASS`. Any `FAIL`, live `SKIPPED`, or missing receipt blocks final review and commit; do not continue while describing the task as complete.

- [ ] **Step 5: Bind final implementation review to the exact HEAD and file bytes**

Generate a read-only review identity:

```powershell
$reviewHeadOutput = @(git rev-parse HEAD)
$reviewHeadExit = $LASTEXITCODE
if ($reviewHeadExit -ne 0 -or $reviewHeadOutput.Count -ne 1) {
    throw "Could not resolve the review HEAD"
}
$reviewHead = $reviewHeadOutput[0].Trim()
if ($reviewHead -cnotmatch '^[0-9a-f]{40}$') { throw "Review HEAD has an invalid shape" }
$reviewPaths = @(
    "docs/client-compatibility.md",
    "docs/client-smoke-evidence-2026-08-13.log",
    "docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md",
    "docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md"
)
$reviewManifest = foreach ($path in $reviewPaths) {
    [pscustomobject]@{
        Path = $path
        Sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    }
}
Write-Host "review_head=$reviewHead"
$reviewManifest | Format-Table -AutoSize
git status --short
$reviewStatusExit = $LASTEXITCODE
if ($reviewStatusExit -ne 0) { throw "Could not inspect status for the review identity" }
```

Expected: one 40-character HEAD, exactly four path/hash rows, and the same four-path status boundary from Step 3. The orchestrator must request the final implementation acceptance review against the authoritative spec, this complete plan, this `review_head`, this exact SHA-256 manifest, the binary evidence matrix, and the complete working tree. The reviewer must specifically verify safety preflight, ownership, UTF-8 capture, immutable live truth, exact results/dual evidence, diagnostics-before-cleanup, residual-resource checks, environment restoration, docs/runtime agreement, ROADMAP conditional handling, and no release-workflow expansion.

Any edit after review starts invalidates that review identity. Re-run all affected static/document/boundary checks, regenerate the full manifest, and obtain a fresh final review before commit handoff.

- [ ] **Step 6: Hand off the single authorized commit after review; perform no push or tag**

Only after the final reviewer approves the exact Step 5 identity, hand the orchestrator these exact commit contents:

```text
docs/client-compatibility.md
docs/client-smoke-evidence-2026-08-13.log
docs/superpowers/plans/2026-08-13-real-client-smoke-evidence.md
docs/superpowers/specs/2026-08-13-real-client-smoke-evidence-design.md
```

Recommended semantic commit:

```text
docs: refresh real client smoke evidence

Record passing rclone, mc, and AWS CLI smoke output and align the compatibility snapshot with the verified local images.
```

Implementation workers do not run `git add` or `git commit`. The orchestrator may use the user's existing authorization for one commit only after confirming that the review identity still matches; it must stage only the four listed paths, inspect the staged diff, create the semantic commit, and report its hash. No push, tag, release, image publication, amend, or additional commit is authorized.

## Verification Waves and Acceptance Boundary

1. **Wave 1—static and ownership preflight:** both PowerShell contracts pass; initial path boundary is clean except spec/plan; fixed names are empty; all commands and six images exist locally; evidence destination is absent.
2. **Wave 2—one live run:** the existing runner executes once under one unique Compose project, output is strict UTF-8, all three clients pass, Mc/Aws dual-endpoint evidence exists, diagnostics precede exact-project cleanup, no owned/fixed resource remains, and the ambient Compose variable is restored.
3. **Wave 3—evidence-derived docs:** local image IDs still match the transcript; only the three client rows and rerun guidance are refreshed; Rust evidence remains historical; ROADMAP stays unchanged because its exact conditional is closed.
4. **Wave 4—static/boundary/final review:** contracts rerun, forbidden surfaces remain unchanged, transcript is reparsed, path and whitespace gates pass, and final approval is bound to HEAD plus four SHA-256 file identities before commit handoff.

Acceptance is all-or-nothing. In particular, an AWS `SKIPPED`, any client `FAILED`, a nonzero runner, an unproven cleanup, or an environment-restoration mismatch is retained as failure evidence and cannot be repaired by documentation edits or a second run over the same file.

## Risks and Assumptions

- The base Compose file has fixed container names and host ports. The all-state preflight protects an existing developer stack, while an unavoidable post-preflight race is handled by exact project scoping and refusal to delete a fixed container not proven through that project.
- The runner rebuilds `ghcr.io/hugefiver/ipfs3:latest` from current source using the local Cargo cache and `rust:latest`; it can fail if an offline dependency is absent. That remains a truthful live failure and does not authorize network access.
- The tracked log appends clearly marked ownership-envelope diagnostics after the runner's unmodified outer output. Result parsing uses only exact anchored runner lines, so Compose/application log text cannot manufacture a PASS.
- `minio/mc:latest` and `amazon/aws-cli:latest` are mutable tags. The transcript's immutable image IDs, immediate post-run `docker image inspect` equality, and final file SHA-256 identity prevent documentation from silently describing a different local image.
- Static tests prove runner/workflow shape but not real-client compatibility. Only the accepted live transcript can make the three compatibility rows PASS.
- The approved spec and plan are presently untracked process artifacts; they are deliberately included in the four-path final review and single commit boundary.
