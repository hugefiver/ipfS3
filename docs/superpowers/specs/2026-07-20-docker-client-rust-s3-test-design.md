# Docker Client Test Runner Safety Remediation Design

**Status:** Ready for remediation planning

## 1. Goal

Close the final runtime-safety and repeatability findings in `scripts/client-smoke.ps1` without changing gateway product behavior or the completed Rust Docker suite. The repaired runner must be safe to execute from the current dirty tree, must always use a local-only archive build when `-Run` is present, must prevent every client `docker run` from falling back to Docker's default pull policy, must bound every native child process, and must leave only its evidence log and small diagnostic inputs in its unique run directory.

## 2. Frozen starting point

- Repository HEAD remains `a3158cf17a89dab054a45bd6ac2be0af1254a00a` on `master`.
- The intended dirty tree is:

  ```text
   M scripts/client-smoke.ps1
   M tests/e2e.rs
  ?? docs/superpowers/plans/2026-07-20-docker-client-rust-s3-tests.md
  ?? docs/superpowers/specs/2026-07-20-docker-client-rust-s3-test-design.md
  ```

- `tests/e2e.rs` is complete, has 11 `#[tokio::test]` cases, passed 11/11 serially, and has Git blob hash `6756f469a5d23f3f6ac5722e3112f548d401e302`. This remediation must not modify it.
- The current runner already proved the archive transport with real Rclone and MinIO `mc`, but that evidence predates the safety remediation and must be regenerated after the runner changes.
- The current runner blob hash is `0ac8e3c88bde617df60b018c0b4988a810055ac8`; it is the dirty-tree input to the remediation, not an accepted final state.

## 3. Non-goals and constraints

- No gateway production code, `tests/e2e.rs`, Compose file, Cargo manifest/lockfile, or integration-test behavior changes.
- No dependency upgrade, installation, image pull, Git staging, commit, push, tag, or other Git write.
- No Docker execution while revising this design and its plan.
- No `docker compose down -v`, service stop, or volume removal in the runner or verification flow.
- No deletion of the evidence log.
- No reuse of a pre-existing run directory or archive context.
- No `Start-Job`, background-job timeout wrapper, shell command-string construction, or timeout implementation that cannot terminate descendants.

## 4. Considered approaches

### 4.1 Recommended: focused runner hardening plus a self-contained PowerShell test

Keep the client operations in `scripts/client-smoke.ps1`, introduce small testable helpers for identity, native process execution, exit classification, and owned-artifact cleanup, and add `tests/client-smoke.Tests.ps1`. The test reads the runner through the PowerShell AST and imports only named helper definitions, so it can exercise process and filesystem behavior without starting Docker.

This approach directly tests the dangerous boundaries, preserves the established Rclone/`mc` workflow, and adds no third-party test dependency.

### 4.2 Split the runner into a module and thin entry point

This would make imports conventional, but it expands the change into an additional production script/module and increases review surface for a focused remediation.

### 4.3 Source guards only

AST and text guards can prove branch shape, but cannot prove timeout behavior, descendant termination, collision handling, or containment-safe cleanup. This is insufficient for the reported runtime risks.

The implementation uses approach 4.1.

## 5. Unique run identity

One immutable `RunId` is created per invocation with this exact lowercase format:

```text
yyyyMMddtHHmmssfffz-<decimal PID>-<8 lowercase hexadecimal GUID characters>
```

The canonical regular expression is:

```regex
^[0-9]{8}t[0-9]{9}z-[0-9]+-[0-9a-f]{8}$
```

Example: `20260720t143012417z-24816-a1b2c3d4`.

The timestamp is UTC with milliseconds. The PID and GUID suffix prevent same-millisecond collisions. The same `RunId` is used by the run-directory name, fixture text, evidence placeholder, bucket names, and cleanup diagnostics; the old `Stamp` variable is removed.

`RunRoot` is the direct child `<temp>/ipfs-s3-client-smoke-$RunId`. The temporary root must already exist. Creation is two-part guarded: fail if the path already exists, then call `New-Item -ItemType Directory` without `-Force`. A race that creates the path between the check and creation therefore also fails. The runner never adopts or pre-cleans a colliding directory.

Bucket names are `ipfs-s3-rclone-$RunId`, `ipfs-s3-mc-$RunId`, and `ipfs-s3-aws-$RunId`. Each is validated against the S3-safe lowercase/digit/hyphen grammar and the 63-character limit before use. With the frozen format, the longest possible name remains below 63 characters even for a 10-digit Windows PID.

Evidence uses `<temp>/ipfs-s3-client-smoke-$RunId`. Verification regexes use the canonical RunId pattern and never assume a fixed 14-digit timestamp.

## 6. Bounded native process boundary

All runner-owned executions of Docker, client containers, `docker info`, `docker image inspect`, Compose, `cargo vendor`, `tar.exe`, and the offline Docker build go through one `Invoke-NativeCommand` helper.

The helper contract is:

```powershell
Invoke-NativeCommand `
    -FilePath <executable> `
    -ArgumentList <string[]> `
    -Label <stable operation label> `
    -Timeout <TimeSpan> `
    [-AllowedExitCodes <int[]>]
```

It must:

1. construct `System.Diagnostics.ProcessStartInfo` with `UseShellExecute = $false`;
2. append every argument through `ProcessStartInfo.ArgumentList`, never through a quoted command string;
3. redirect stdout and stderr and start both `ReadToEndAsync()` operations immediately after process start;
4. call `WaitForExit(timeoutMilliseconds)`;
5. on timeout, call `Kill($true)`, wait for process-tree termination, drain both readers, and throw an error containing the operation label and timeout;
6. on a disallowed exit code, throw an error containing the label, exit code, and bounded captured diagnostic text;
7. return a structured object containing `ExitCode`, `StdOut`, and `StdErr` on success or an explicitly allowed nonzero exit.

The fixed timeout budgets are:

| Operation | Timeout |
|---|---:|
| Ordinary Docker command, Compose command, or client container | 5 minutes |
| `cargo vendor --locked --offline` | 5 minutes |
| `tar.exe` archive creation | 5 minutes |
| Offline `docker build` | 30 minutes |

The outer live-verification harness adds a 45-minute bound around each complete runner invocation. This is a second containment layer, not a replacement for per-process bounds.

## 7. Forced local-only build path

When `-Run` is present and at least one selected client is runnable, the runner always follows one path:

1. require local service images `ghcr.io/hugefiver/ipfs3-kubo:latest` and `ghcr.io/hugefiver/ipfs3:latest`;
2. require local `rust:latest`;
3. require the selected local client image for every client that will run;
4. run `cargo vendor --locked --offline`;
5. create a fresh archive-only context and `vendor.tar.gz`;
6. run the offline Docker build with `--pull=false --network none` and `vendor-archive=<context>`;
7. start only `kubo` and `gateway` with Compose `up -d --pull never --no-build`.

`StandardBuildImages`, `$missingStandardBuild`, `$useOfflineGatewayBuild`, and the ordinary Compose `--build` branch are removed. Cached standard images can no longer change the selected path. The runner never pulls.

The runner has exactly six source-level `docker run` entry points: the Rclone, MinIO `mc`, and AWS wrappers, plus one version probe for each client. Every entry point uses the exact argument prefix `"run", "--rm", "--pull=never"`; `--pull=never` is one argument and immediately follows `--rm`. This closes the preflight race in which a locally inspected client image disappears before `docker run`, whose default `missing` policy could otherwise contact a registry. No `docker run` entry point may rely on Docker's default pull policy.

The archive context is checked before creation. If it already exists, the runner throws. It never uses `Remove-Item` to make the context appear fresh.

## 8. Owned-artifact cleanup

The runner owns exactly these build artifacts under its invocation-specific `RunRoot`:

- `vendor/`
- `vendor-archive-context/`
- `Dockerfile.gateway-runtime`

Cleanup derives those three paths internally; callers cannot supply an arbitrary deletion list. Before every deletion, the helper canonicalizes the temporary root, `RunRoot`, and candidate with `System.IO.Path.GetFullPath`, proves `RunRoot` is the expected direct child of the temporary root, proves the candidate is a strict child of `RunRoot`, and proves its full path equals one of the three derived owned paths.

Cleanup runs in the outermost `finally`, after `Stop-Transcript`, whether preflight, build, client execution, timeout, or another exception fails. A cleanup error forces exit 1. Cleanup does not remove `RunRoot`, `client-smoke.log`, fixtures, client configuration, any unrelated file, containers, services, or volumes.

## 9. Exit semantics

| Invocation state | Result lines | Exit |
|---|---|---:|
| No `-Run`; prerequisite unavailable or execution not requested | Accurate `SKIPPED` | 0 |
| `-Run`; Compose file, Docker command/daemon, service image, or `rust:latest` unavailable | Accurate `SKIPPED` for selected clients | nonzero |
| `-Run`; every selected client image missing | Accurate `SKIPPED` for every selected client | nonzero |
| `-Run -Client All`; one or more client images missing, at least one runnable client succeeds, and no runnable client fails | Missing clients `SKIPPED`; runnable clients `PASSED` | 0 |
| Any stack setup, timeout, client assertion, or cleanup failure | Affected runnable clients `FAILED` where applicable | nonzero |

The runner does not print or execute a pull command as a fallback. Missing local prerequisites are reported as local-only blockers.

## 10. Executable PowerShell tests

Create `tests/client-smoke.Tests.ps1` as a dependency-free PowerShell 7 executable test. It parses the runner once and evaluates only named function definitions. Native helpers are imported in dependency order—`Convert-NativeTextToLines`, then `Get-NativeDiagnostic`, then `Invoke-NativeCommand`—before the first child process is launched; the test supplies only the minimal variables required by an imported helper and does not dot-source or execute runner top-level code. It must prove:

1. `Invoke-NativeCommand` preserves an argument containing spaces and shell metacharacters as one literal argument.
2. A fake native parent that records its own PID and launches a sleeping child times out within a bounded wall-clock interval and reports its label. After timeout, both recorded tree PIDs are absent, while a control sleeper launched independently by the test process remains alive; the outer test `finally` cleans only that independent control sleeper and the unique test temporary directory.
3. Eight concurrent attempts to create the same valid `RunRoot` yield exactly one success and seven collision failures.
4. Generated RunIds match the canonical regex, remain unique, and produce bucket-safe names no longer than 63 characters.
5. Source guards find exactly six `"run", "--rm", "--pull=never"` prefixes and prove there is no `"run", "--rm"` occurrence whose next argument is not the single argument `"--pull=never"`.
6. Forced-offline source/AST guards find one archive build path, Compose `--pull never --no-build`, and the exact `-Timeout $OfflineBuildTimeout` override inside `Invoke-OfflineGatewayBuild`; they reject `StandardBuildImages`, conditional build selection, ordinary Compose `--build`, `docker pull`, and `down -v`.
7. The archive function fails on an existing context and contains no pre-delete of that context.
8. Cleanup removes only the three owned artifacts, retains the evidence log and unrelated files, and rejects an out-of-root cleanup attempt without deleting its sentinel.
9. AST inspection finds no direct `docker`, `cargo`, or `tar.exe` command invocation and no `Start-Job`; all such native paths are routed through `Invoke-NativeCommand` directly or through `Invoke-Docker`.
10. Exit-classification tests cover no-`-Run` skip/zero, `-Run` unavailable/nonzero, all-clients-missing/nonzero, and partial-client availability without a preflight failure.
11. Launching the runner with an empty child `PATH` proves unavailable Docker is `SKIPPED`+exit 0 without `-Run` and `SKIPPED`+nonzero with `-Run`.

## 11. Real verification and review evidence

After the six-entry pull lock and offline-build timeout guard are implemented and the non-Docker tests/static guards pass, rerun Rclone and MinIO `mc` separately from fresh RunRoots. Evidence from before this fix is not acceptance. Each full runner process has a 45-minute outer timeout, while all child processes retain their fixed internal bounds.

Each run must prove:

- a RunId matching the canonical pattern;
- exactly one archive creation and one `vendor-archive` offline build;
- Compose `--pull never --no-build` and no ordinary Compose build;
- no legacy `vendor=$vendorPath` context;
- every emitted Rclone/`mc` `docker run` command begins `docker run --rm --pull=never`, with no unlocked client run;
- owned build artifacts absent after process exit while `client-smoke.log` remains;
- one exact successful client `RESULT` line;
- for `mc`, one exact same-client dual-endpoint `EVIDENCE` line;
- no `-CleanupVolumes`, `down -v`, pull, service stop, or volume deletion.

The final review package sent by the orchestrator to Oracle and reviewer contains the complete current spec and plan, complete runner/test diffs, PowerShell test output, AST/source-guard output, both fresh live-run outputs, cleanup checks, final status, and whitespace checks. The historical pre-remediation client passes are context only and cannot satisfy this acceptance gate.

## 12. Acceptance criteria

- Only `scripts/client-smoke.ps1`, new `tests/client-smoke.Tests.ps1`, and the two requested documents are changed by the remediation; the existing `tests/e2e.rs` blob remains `6756f469a5d23f3f6ac5722e3112f548d401e302`.
- The unique identity, no-adoption collision behavior, bucket grammar, and evidence regex use one frozen RunId contract.
- Every runner-owned native process is injection-safe, asynchronously drained, time-bounded, and tree-killed on timeout.
- Every `-Run` uses the archive-only offline build and Compose `--pull never --no-build` with local images only.
- All six client `docker run` source entry points place the single argument `--pull=never` immediately after `run --rm`, and the offline build passes the 30-minute `$OfflineBuildTimeout` override explicitly.
- Cleanup is containment-checked, exact, unconditional, post-transcript, and preserves evidence, services, and volumes.
- Exit codes distinguish dry preflight skips, unexecutable requested runs, all-missing requested runs, and successful partial `All` runs.
- `pwsh -NoProfile -File tests/client-smoke.Tests.ps1` passes.
- Fresh Rclone and `mc` runs pass and retain their required `RESULT`/`EVIDENCE` semantics.
- `git diff --check` and no-index whitespace checks for both untracked documents and the new untracked test pass.
- No Git write is performed. Final plan receipt status remains `waiting for receipt` until the orchestrator reviews the current revision.
