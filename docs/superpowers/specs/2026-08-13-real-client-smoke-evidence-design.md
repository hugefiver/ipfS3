# Real Client Smoke Evidence Design

**Status:** Approved design

## Problem

The repository has a real Docker-based client runner for rclone, MinIO mc, and
AWS CLI, but the tracked evidence is dated 2026-07-19. That snapshot records
rclone and mc as passed while AWS CLI is skipped because its image was absent.
All required images are now present locally, so the remaining P0 work is to run
the existing client surface and record current, truthful evidence.

The runner intentionally never pulls images and preserves Compose resources for
diagnosis. It also uses fixed container names from `docker-compose.yml`. A live
run therefore needs an external ownership envelope so it cannot attach to or
clean up a developer stack.

## Goals

- Execute the existing rclone, mc, and AWS CLI smoke paths against a gateway
  built from the current source tree.
- Require all three result lines to be `PASSED`; a `SKIPPED` or `FAILED` result
  is not acceptance.
- Preserve a portable tracked transcript with client image identities, commands,
  assertions, and dual-endpoint evidence.
- Update the compatibility matrix from that transcript and remove stale rerun
  instructions that reference the nonexistent `-CleanupVolumes` parameter.
- Clean up only resources owned by this invocation after diagnostics are
  captured.

## Non-goals

- Do not change production Rust, S3 behavior, Docker Compose topology, image
  tags, or the client runner.
- Do not add real clients to blocking GitHub Actions; the release-validation
  workflow remains infrastructure-only.
- Do not pull images, install software, publish images, release, push, or tag.
- Do not claim multi-node, provider, cloud, or production compatibility.

## Approaches Considered

### 1. External ownership envelope around the existing runner — selected

Preflight the two fixed container names in all states, set a unique
`COMPOSE_PROJECT_NAME`, run `scripts/client-smoke.ps1 -Client All -Run`, capture
its outer output, then collect Compose logs and run `down --volumes
--remove-orphans` for that exact project. This exercises the already-reviewed
client commands without changing their semantics and makes ownership explicit.

### 2. Add ownership and cleanup parameters to the runner

This would make future runs more convenient, but it expands the code and test
surface before the current evidence gap is closed. The existing runner already
honors `COMPOSE_PROJECT_NAME`, and the fixed-name preflight can be enforced by
the orchestrated run.

### 3. Run all real clients in GitHub Actions

This would require changing the runner's no-pull/offline assumptions or
preloading several images on a fresh hosted runner. It is a separate portability
and reproducibility project, not required to obtain the current local evidence.

## Design

### Ownership and environment

Before the run, inspect all Docker containers for the exact names
`ipfs-s3-kubo` and `ipfs-s3-gateway`. If either exists, stop without starting or
cleaning anything. Otherwise create a unique lowercase Compose project name and
set `COMPOSE_PROJECT_NAME` only for the invocation. Save whether the variable
previously existed and restore its original state after cleanup.

The runner uses the repository's explicit base Compose file and starts only
`kubo` and `gateway`. The unique project name scopes the network and named
volumes; the fixed-name preflight establishes ownership of the containers and
published ports.

### Live execution and evidence

Capture the outer stdout and stderr of:

```powershell
pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run
```

to `docs/client-smoke-evidence-2026-08-13.log` as UTF-8. The transcript must
include image IDs and the runner's portable command paths. Acceptance requires
exactly one result for each client:

- `client=Rclone status=PASSED`
- `client=Mc status=PASSED dual_head=PASSED`
- `client=Aws status=PASSED dual_head=PASSED`

The mc and AWS runs must also emit their same-client dual-endpoint evidence.
rclone remains scoped to the Compose-network endpoint and therefore keeps
`dual_head=NOT_RUN`.

### Post-review portable derivation

The originally captured raw bytes failed portable/non-coloured review because
Cargo vendoring emitted host-absolute paths and diagnostics contained ANSI CSI
sequences. For the historical run only, a one-time deterministic,
representation-only derivation is permitted without another live run. It must
record the raw SHA-256 and exact host-prefix/ANSI transformation counts, redact
only the documented Cargo-registry prefix, strip CSI only from diagnostics, and
become immutable once all checks pass. The canonical transcript contains exactly
the three rclone, mc, and AWS client image IDs; all six local image identities
remain preflight-only evidence and must not be added to the transcript.

The derivation must include a durable restoration receipt. It may record that
the conditional exact restoration check passed while explicitly recording that
the concrete prior `COMPOSE_PROJECT_NAME` presence/value is unavailable for this
historical run; it must not invent that state.

### Diagnostics and cleanup

After any attempted stack run, capture project-scoped, non-coloured Compose logs
before cleanup. Then execute project-scoped `docker compose ... down --volumes
--remove-orphans`. Confirm that the fixed containers and project resources no
longer exist. If stack ownership was never established, do not run cleanup.

Temporary runner build artifacts remain governed by the runner's existing
fail-closed cleanup. The tracked evidence file is retained even on failure so a
failed command cannot be converted into a pass.

### Documentation

Update `docs/client-compatibility.md` to a 2026-08-13 snapshot using only values
observed in the transcript and local image inspection. Replace the stale AWS
`SKIPPED` row only if AWS actually passes. Correct rerun instructions to describe
the external project-scoped cleanup rather than the nonexistent
`-CleanupVolumes` switch.

If the ROADMAP contains an unchecked item whose exact acceptance condition is
the now-executed AWS CLI smoke, mark only that item complete. Do not change any
broader milestone or release status.

## Verification

- `pwsh -NoProfile -File tests/client-smoke.Tests.ps1`
- `pwsh -NoProfile -File tests/release-validation.Tests.ps1`
- Three exact `PASSED` result lines and mc/AWS dual-endpoint evidence in the new
  tracked transcript.
- Canonical transcript provenance records the retained raw SHA-256, exact
  redaction/ANSI counts, three client image identities, six-image preflight-only
  scope, no live rerun, and the historical restoration receipt.
- No `status=FAILED` or `status=SKIPPED` result in the executed transcript.
- Compatibility matrix image/version/result values agree with the transcript
  and `docker image inspect`.
- `git diff --check` and a path-boundary audit pass.
- Final identity-bound implementation review approves the complete working tree
  before the authorized commit.

## Change Boundary

Expected changes are limited to:

- `docs/client-smoke-evidence-2026-08-13.log` (new)
- `docs/client-compatibility.md`
- `ROADMAP.md` only if its exact AWS smoke checkbox is still open
- this specification and its implementation plan

No Git push or tag is authorized.
