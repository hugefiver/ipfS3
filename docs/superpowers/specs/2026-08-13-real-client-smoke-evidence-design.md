# Real Client Smoke Evidence Design

**Status:** Approved design

## Evidence boundary

The Docker-based client runner exercises rclone, MinIO mc and AWS CLI. Each
receipt describes its actual client images and observed results; local image
availability isn't a permanent environment requirement or a compatibility pass.

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

## Ownership envelope

Preflight the two fixed container names in all states, set a unique
`COMPOSE_PROJECT_NAME`, run `scripts/client-smoke.ps1 -Client All -Run`, capture
its outer output, then collect Compose logs and run `down --volumes
--remove-orphans` for that exact project. This exercises the already-reviewed
client commands without changing their semantics and makes ownership explicit.

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

### Recorded transcript provenance

The 2026-08-13 receipt has a deterministic, representation-only derivation.
Its provenance records the raw SHA-256 and exact host-prefix/ANSI transformation
counts. It redacts only the documented Cargo-registry prefix and strips CSI
only from diagnostics; it doesn't represent a new live execution. The
canonical transcript is immutable and contains exactly
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
- The 2026-08-13 canonical transcript retains its raw SHA-256, exact
  redaction/ANSI counts, three client image identities, six-image preflight-only
  scope and historical restoration receipt, without implying a new live run.
- No `status=FAILED` or `status=SKIPPED` result in the executed transcript.
- Compatibility matrix image/version/result values agree with the transcript
  and `docker image inspect`.
