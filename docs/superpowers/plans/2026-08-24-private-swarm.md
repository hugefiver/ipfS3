# Private IPFS Swarm Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the approved same-host Cluster profile with a fail-closed shared Kubo PSK, deterministic A/B peering bootstrap, wrong-key rejection evidence, and complete static/live/documentation gates without changing gateway application behavior.

**Architecture:** A Cluster-only Kubo entrypoint validates a required Compose-secret file, reapplies closed-discovery Kubo configuration, installs `swarm.key` mode `0400`, and supervises Kubo through a narrow FIFO line filter that replaces only Kubo v0.43's unconditional private-network fingerprint line at the log source; the unchanged shared entrypoint remains the boundary for all other profiles. A one-shot `swarm-bootstrap` service discovers the two Kubo IDs in memory, installs both persistent peerings, connects both directions, and verifies Kubo's real two-line CLI peering representation before either Cluster peer starts. Rust independently validates the real `Peers[{ID,Addrs}]` HTTP representation with transport-only addresses, while PowerShell owns secrets, process/filesystem/signal assertions, unique Compose lifecycle, defense-in-depth diagnostics, cleanup, environment restoration, and the existing Cluster scenario sequence.

**Tech Stack:** POSIX `/bin/sh`, Kubo v0.43.0, IPFS Cluster v1.1.6 CRDT, PostgreSQL 17, Docker Compose v2.23.1+, Rust 2024 (MSRV 1.92), Tokio 1, reqwest 0.13, serde/serde_json, PowerShell 7, and GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-24-private-swarm-design.md` (runtime-revised approved SHA256 `a55d6b0064fc51ed55b909dcf4b19bfd5f14230e3e9537fcc22b48030a0b921f`)

**Global Constraints:**
- The supported deployment is exactly one gateway, PostgreSQL 17, two Kubo v0.43.0 daemons, two IPFS Cluster v1.1.6 peers, and one one-shot bootstrap service.
- Cluster remains CRDT with replication factor `2/2` and remains eventually consistent.
- `CLUSTER_SECRET` authenticates IPFS Cluster membership only. It is not read by Kubo and does not enable libp2p private-network mode.
- Kubo private networking requires the repository-local `swarm.key` PSK file plus `LIBP2P_FORCE_PNET=1`.
- The selected approach is deliberately bounded: it changes Cluster-profile infrastructure, validation, and documentation without changing Cargo files, Rust application behavior, or the shared Kubo entrypoint.
- The production profile is **same-host Docker Compose only** and contains these seven services: `postgres`, `kubo-a`, `kubo-b`, `swarm-bootstrap`, `cluster-a`, `cluster-b`, and `gateway`.
- `swarm-bootstrap` starts only after `kubo-a` and `kubo-b` are healthy.
- Both Cluster services require `swarm-bootstrap` to complete successfully, as well as retaining their paired-Kubo readiness dependency.
- The profile retains CRDT and `replication_factor_min=2` and `replication_factor_max=2`.
- Two allocations are an eventual target, not a synchronous S3 publication barrier.
- Production publishes only the existing gateway S3 port, with its fixed loopback host-bind guard.
- It must not publish Kubo TCP 4001, Kubo API 5001, Kubo Gateway 8080, Cluster REST 9094, Cluster proxy 9095, PostgreSQL, or any other Cluster or libp2p port.
- `docker-compose.cluster.yml` adds a top-level `swarm_key` secret whose `file` is required through `${IPFS_S3_SWARM_KEY_FILE:?IPFS_S3_SWARM_KEY_FILE is required}`.
- `kubo-a` and `kubo-b` each mount that same Compose secret at `/run/secrets/swarm_key` and set `IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key` and `LIBP2P_FORCE_PNET=1`.
- There is no raw swarm-key environment variable, tracked fallback, or secret default.
- Compose secrets take precedence over any raw environment mechanism.
- `ipfs/cluster.Dockerfile` remains based on `ipfs/kubo:v0.43.0`, but copies and executes `ipfs/private-swarm-entrypoint.sh`.
- The shared `ipfs/entrypoint.sh` remains byte-for-byte unchanged for this work.
- The private wrapper must read strict UTF-8, LF-only, no-BOM text with exactly three lines: `/key/swarm/psk/1.0.0/`, `/base16/`, and exactly 64 lowercase hexadecimal characters, including the required final LF and no extra bytes.
- On every start, after initialization and configuration, the wrapper installs the validated source at `$IPFS_PATH/swarm.key` with mode `0400` before starting Kubo.
- The wrapper enforces `Bootstrap=[]`, `Routing.Type=none`, boolean `Discovery.MDNS.Enabled=false`, boolean `AutoConf.Enabled=false`, `Swarm.AddrFilters=[]`, string-array `Gateway.HTTPHeaders.Cache-Control=["public, max-age=29030400, immutable"]`, integer `Datastore.BloomFilterSize=0`, and an `Addresses.Swarm` list containing only `/ip4/0.0.0.0/tcp/4001`; all non-string values use Kubo v0.43 `ipfs config ... --json`, the Cluster profile's internal API/Gateway behavior is preserved, and the daemon starts with garbage collection disabled. AutoConf must be disabled before daemon launch so a private-network repository never consults or validates the default mainnet AutoConf service. The `server` init profile installs RFC1918 dial filters that include Docker bridge ranges, so this Cluster-only same-host wrapper clears `Swarm.AddrFilters` on every start; the internal-only Swarm port plus PSK remain the membership boundary. Clearing these dial filters is not container egress isolation and creates no egress-blocking claim.
- The wrapper returns nonzero with exactly `private swarm startup rejected` on validation, installation, or configuration failure and never emits the key, its digest, a key-bearing command line, repository-key data, or shell temporary data.
- Kubo v0.43 unconditionally writes `Swarm key fingerprint: <32 lowercase hex>` during private daemon startup. The Cluster-only wrapper is the primary disclosure barrier: it replaces only that exact line with `Swarm key fingerprint: [redacted]` before container logging, preserves every other line byte-for-byte apart from line termination, remains PID 1, forwards `TERM`, `INT`, and `HUP`, and uses a trap-owned per-wait interruption flag rather than `kill -0` to decide whether a daemon wait must be repeated. It returns Kubo's exact exit status, reaps its filter, removes its FIFO, and fails closed on filter/FIFO cleanup only when Kubo itself succeeded.
- `swarm-bootstrap` succeeds only when each daemon reports the other as its sole swarm peer and both persistent Peering entries exist; every failure emits exactly `private swarm bootstrap failed` and returns nonzero.
- Kubo v0.43 `ipfs swarm peering ls` text is one peer-ID line followed by one tab-indented transport-address line per peer. Each side must have exactly two nonempty lines: the expected other ID once and the exact trimmed `/dns4/kubo-{other}/tcp/4001` once, with no extra ID, peer, or address; it must not be treated as a one-line `/p2p/<id>` record.
- `swarm-bootstrap` exposes no ports, writes no tracked state, and emits no peer IDs, multiaddresses, secret material, or request bodies in normal logs.
- There is no fallback to mDNS, public bootstrap peers, DNS discovery beyond the two declared service names, a random peer, or partially configured Cluster startup.
- The gateway keeps its unchanged one-way runtime path through Cluster A's Kubo-compatible proxy; no application read fallback to Kubo B is added.
- S3 deletion remains metadata-only; no Cluster unpin, Kubo `pin/rm`, or garbage-collection lifecycle change is added.
- Kubo v0.43 `/api/v0/swarm/peering/ls` is decoded as `Peers[{ID,Addrs}]`: `ID` equals the other Kubo ID and `Addrs` contains only the exact transport `/dns4/kubo-{other}/tcp/4001`, never `/p2p/<id>`; valid, extra-peer, extra-address, wrong-ID, and wrong-address fixtures freeze this contract.
- Logs and diagnostic capture retain a prefix-specific `Swarm key fingerprint:` redaction plus PSK-format and standalone 64-character lowercase-hex redaction as defense in depth while preserving ordinary 32-hex text and CID output. Post-capture workflow redaction is not the primary fingerprint barrier.
- The PSK protects swarm membership only. It does not isolate containers from the internet, prevent outbound container traffic, encrypt REST or proxy traffic, authenticate a published API, or provide host, database, Kubo, Cluster, gateway, or load-balancer availability.
- `tests/compose.cluster-validation.yml` remains an override and adds only validation-only `swarm_key_wrong` and `kubo-c`, whose loopback Kubo API port is `55102`; Kubo C is not a Cluster member and has no production counterpart.
- Base validation ports remain gateway `59100`, PostgreSQL `55435`, Kubo A/B `55100`/`55101`, Cluster A/B REST `59101`/`59102`, and Cluster A proxy `59103`.
- Rust proves protocol-visible Kubo configuration, identities, exact peer sets, persistent peerings, and wrong-key connect rejection; outer PowerShell/Compose owns container lifecycle, filesystem mode/content-digest comparison, and process exit assertions.
- Main and wrong-key files are disposable random keys created atomically with `FileMode.CreateNew`, UTF-8 without BOM, LF-only final newlines, and 32 random bytes rendered lowercase; neither key nor its digest is displayed, serialized, uploaded, committed, or reused.
- Validation sets `COMPOSE_DISABLE_ENV_FILE=1`, restores exact prior process-environment presence and case-sensitive values, validates missing-key failure before full startup, owns a unique Compose project, checks every fixed validation port, and removes only owned resources and files.
- Cleanup-command failure, residual owned resources, generated-file deletion failure, or environment-restoration mismatch fails the validation.
- `.github/workflows/release-validation.yml` extends the existing blocking `cluster-pinset-replication` job and must not create a second private-swarm job.
- README and ROADMAP are edited only after all local static, runtime, and regression gates pass; the initial hosted result is `HOSTED cluster-pinset-replication: NOT RUN`.
- Production documentation uses only `docker compose -f docker-compose.cluster.yml down --remove-orphans`; it never recommends `down --volumes`.
- No implementation agent may edit the approved spec. Only the orchestrator may mark plan checkboxes, and any semantic plan edit invalidates plan review.
- Do not install software or explicitly pull images. Declared Compose `build`/`up` may resolve required images normally; unavailable Docker/network/image evidence is `UNVERIFIED`, never PASS.
- Every command issued by the implementation workflow uses Windows PowerShell syntax. POSIX syntax appears only inside the shipped Compose bootstrap, `ipfs/private-swarm-entrypoint.sh`, and the two test-only `/bin/sh` here-string fixtures that PowerShell passes to disposable containers for source-filter and supervisor proof.
- Implementation agents do not stage, commit, push, tag, amend, or perform another Git write. After the exact implementation identity receives both Oracle and Reviewer approvals, the orchestrator may use the user's existing authorization for one focused semantic commit; no push or tag is authorized.

---

## File Map

### Create

- `ipfs/private-swarm-entrypoint.sh` — Cluster-only strict PSK validator, closed Kubo configuration, mode-`0400` install, exact fingerprint filter, and PID-1 daemon supervisor.
- `docs/superpowers/plans/2026-08-24-private-swarm.md` — this execution plan and a member of the final immutable review manifest.

### Modify

- `docker-compose.cluster.yml` — required `swarm_key` secret, private A/B Kubo wiring, one-shot `swarm-bootstrap`, seven-service dependency graph, and unchanged production publication boundary.
- `ipfs/cluster.Dockerfile` — copy and select only `private-swarm-entrypoint.sh` while retaining exact Kubo v0.43.0.
- `tests/compose.cluster-validation.yml` — validation-only wrong-key secret and profile-gated Kubo C at loopback `55102` while retaining all current mappings.
- `tests/cluster.Tests.ps1` — entrypoint/Compose/Dockerfile/Rust/workflow/docs contracts, real CLI/REST shape assertions, source-level fingerprint/supervisor assertions, protected hashes, redaction fixtures, service/job causality, and embedded-PowerShell AST checks.
- `tests/support/cluster.rs` — bounded loopback Kubo HTTP observations, real transport-only Peering DTO fixtures, and pure private-config validation alongside existing Cluster polling support.
- `tests/cluster.rs` — two private-swarm live tests added without filesystem/process ownership, while retaining the five current Cluster scenarios.
- `.github/workflows/release-validation.yml` — extend the existing Cluster job with generated secret lifecycle, wrapper negatives, filter/signal/exit proofs, direct raw-log assertion, Kubo C, bootstrap/filesystem evidence, A/B restart recovery, and fail-closed cleanup.
- `tests/release-validation.Tests.ps1` — shared exact six-job contract extended in place for private-swarm causal order, secret safety, cleanup, and no-new-job guarantees.
- `README.md` — evidence-gated same-host private-swarm operation and explicit security/availability/non-destructive-shutdown limits.
- `ROADMAP.md` — check only `Private swarm (swarm.key) for node-to-node communication` after complete local PASS.

### Protected / Verify Unchanged

- `docs/superpowers/specs/2026-08-24-private-swarm-design.md` at the approved SHA above.
- `ipfs/entrypoint.sh`, `ipfs/Dockerfile`, root `Dockerfile`, `Cargo.toml`, `Cargo.lock`, `config.docker.toml`, and every `src/**` application file.
- `docker-compose.yml`, `docker-compose.postgres.yml`, `docker-compose.multi-gateway.yml`, and their validation overrides.
- Existing workflow job set and names; every job other than `cluster-pinset-replication` remains byte-for-byte unchanged, and `client-smoke-infrastructure` keeps its exact five static commands.
- Existing Cluster proxy, topology, replication, delete-retention, outage, and recovery behavior except for the required private-swarm prerequisites and A/B restart ordering.
- `.env`, `.env.example`, provider configuration, deployment systems outside same-host Compose, and unrelated specs/plans.

## Execution Protocol

- Execute Tasks 1-6 in order with a fresh implementation worker per task; the orchestrator checks exact files, interfaces, RED/GREEN evidence, protected boundaries, and unstaged status after each task.
- Task 3 consumes the exact Task 1 service/interface names and Task 2 test names. Task 4 is blocked until Tasks 1-3 are static/compile GREEN. Task 5 is blocked until every Task 4 LOCAL receipt is PASS. Task 6 is blocked until Tasks 1-5 have no unresolved failure.
- Tests are written or strengthened before implementation in every behavior-changing task. Expected RED/GREEN text below is prospective: workers must record actual command, exit, and failure category and must never report an unexecuted expectation as a result.
- No task-level commit exists. The implementation remains one integrated identity through Oracle and Reviewer review.

---

### Task 1: Add the Cluster-only entrypoint, Compose secret, bootstrap, and wrong-key Kubo C

**Files:**
- Create: `ipfs/private-swarm-entrypoint.sh`
- Modify: `ipfs/cluster.Dockerfile`
- Modify: `docker-compose.cluster.yml`
- Modify: `tests/compose.cluster-validation.yml`
- Test: `tests/cluster.Tests.ps1`
- Verify unchanged: `ipfs/entrypoint.sh`, all Cargo/application files, default/single-PG/multi-gateway Compose files

**Interfaces:**
- Consumes: current exact Kubo image `ipfs/kubo:v0.43.0`; Kubo v0.43 typed config schema (`Discovery.MDNS.Enabled: bool`, `AutoConf.Enabled: bool`, `Swarm.AddrFilters: []string`, `Gateway.HTTPHeaders.Cache-Control: []string`, `Datastore.BloomFilterSize: int`), the `server` profile's RFC1918/Docker-bridge filters, and private-network rejection of the default mainnet AutoConf URL; official v0.43 `daemon.go` fingerprint line `fmt.Printf("Swarm key fingerprint: %x\n", node.PNetFingerprint)`; official v0.43 `swarm.go` normal peer multiaddress output and peering text encoder that writes an ID line then tab-indented transport-address lines; current A/B volumes and health checks; paired Cluster dependencies; fixed-loopback gateway publication; Compose secret-file interpolation; existing dependency-free PowerShell YAML helpers and protected hashes.
- Produces: executable/sourceable `/private-swarm-entrypoint.sh`; exact typed Kubo config commands for boolean MDNS, boolean AutoConf disablement, empty Swarm AddrFilters, string-array Gateway Cache-Control, and integer BloomFilterSize; exact source-level fingerprint replacement; FIFO PID-1 Kubo supervisor with `interrupt_daemon_wait`, `wait_for_daemon_exit`, `wait_for_filter_exit`, and `select_supervisor_status`; exact `37`/forwarded-`43`/real-`143` status preservation; filter/FIFO fail-closed precedence; required `swarm_key`; exact seven-service production set; one-shot `swarm-bootstrap` with one-line peer-multiaddress suffix parser plus unchanged two-line peering parser; validation profile `private-swarm-validation`; required `swarm_key_wrong`; Kubo C endpoint `http://127.0.0.1:55102`; fixed wrapper/bootstrap error strings; Task 2's Kubo API topology.

- [ ] **Step 1: Extend the static contract first and capture focused RED**

In `tests/cluster.Tests.ps1`, add `$PrivateEntrypointPath`, read it only after `Require-File`, change the production service set to `postgres,kubo-a,kubo-b,swarm-bootstrap,cluster-a,cluster-b,gateway`, and change the validation set to those seven plus `kubo-c`. Add exact assertions for: Dockerfile source selection; all five typed Kubo v0.43 config lines below; exactly one `Swarm.AddrFilters` config command and rejection of its absence, nonempty value, or any non-exact form; strict 96-byte/three-line source validation; fixed wrapper error; source guard; exact prefix/32-lowercase fingerprint filter and fixed marker; no generic 32-hex masker; `mkfifo`; background filter/daemon PIDs; `TERM`/`INT`/`HUP` traps setting `wait_interrupted=1` before forwarding the corresponding signal; reset of that flag before each daemon/filter wait; `set +e` before every nonzero wait/status-selection capture and no `set -e`; a repeat branch whose only condition is `interrupted == 1 && status > 128`; no `kill -0` in either re-wait decision; exact daemon statuses `37`, forwarded/handled `43`, and unhandled daemon `143`; daemon-status precedence over filter/FIFO status; filter/FIFO nonzero mapped to `1` only for daemon `0`; filter reap; FIFO cleanup on normal/abnormal paths; absence of `exec ipfs daemon`; required secret interpolation; same secret mounted by A/B; `LIBP2P_FORCE_PNET: "1"`; bootstrap's healthy dependencies, bounded loop, peering-add/connect/sole-peer checks, normal `swarm peers` without `-q`, one nonempty full-multiaddress line ending exactly in `/p2p/$expected_id`, rejection of bare-ID equality, unchanged exact two-nonempty-line peering parser, fixed error/no ports/no volumes; Cluster `service_completed_successfully`; wrong-key secret, profile, tmpfs repository, port `55102`; and unchanged protected hashes/publications. Retain every current Cluster assertion unless its expected count/order is intentionally revised here.

Add this exact line-based contract after loading the private entrypoint. The old forms are exact forbidden lines, not broad substrings, so the correct typed lines cannot match them accidentally.

```powershell
$privateEntrypointText = [IO.File]::ReadAllText((Resolve-Path -LiteralPath $PrivateEntrypointPath))
$privateEntrypointLines = @($privateEntrypointText -split "\r?\n" | ForEach-Object { $_.Trim() })
$requiredTypedConfigLines = @(
    'ipfs config Discovery.MDNS.Enabled --json false >/dev/null 2>&1 || return 1',
    'ipfs config AutoConf.Enabled --json false >/dev/null 2>&1 || return 1',
    'ipfs config Swarm.AddrFilters --json ''[]'' >/dev/null 2>&1 || return 1',
    'ipfs config Gateway.HTTPHeaders.Cache-Control --json ''["public, max-age=29030400, immutable"]'' >/dev/null 2>&1 || return 1',
    'ipfs config Datastore.BloomFilterSize --json 0 >/dev/null 2>&1 || return 1'
)
$forbiddenUntypedConfigLines = @(
    'ipfs config Discovery.MDNS.Enabled false >/dev/null 2>&1 || return 1',
    'ipfs config AutoConf.Enabled false >/dev/null 2>&1 || return 1',
    'ipfs config Gateway.HTTPHeaders.Cache-Control "public, max-age=29030400, immutable" >/dev/null 2>&1 || return 1',
    'ipfs config Datastore.BloomFilterSize 0 >/dev/null 2>&1 || return 1'
)
foreach ($requiredLine in $requiredTypedConfigLines) {
    if ($privateEntrypointLines -cnotcontains $requiredLine) { throw "Required typed Kubo config line is absent" }
}
foreach ($forbiddenLine in $forbiddenUntypedConfigLines) {
    if ($privateEntrypointLines -ccontains $forbiddenLine) { throw "Untyped Kubo config line remains" }
}
$addrFilterCommand = 'ipfs config Swarm.AddrFilters --json ''[]'' >/dev/null 2>&1 || return 1'
$addrFilterLines = @($privateEntrypointLines | Where-Object { $_ -cmatch '^ipfs config Swarm\.AddrFilters(?:\s|$)' })
if ($addrFilterLines.Count -ne 1 -or $addrFilterLines[0] -cne $addrFilterCommand) {
    throw "Swarm.AddrFilters must be cleared exactly once with the exact typed command"
}
$requiredPeerParserFragments = @(
    'peers_a="$$(ipfs --api="$$api_a" swarm peers 2>/dev/null)" || return 1',
    'peers_b="$$(ipfs --api="$$api_b" swarm peers 2>/dev/null)" || return 1',
    'nonempty="$$(printf ''%s\n'' "$$observed" | sed ''/^[[:space:]]*$$/d'')" || return 1',
    '[ "$$(line_count "$$nonempty")" = "1" ] || return 1',
    '*"/p2p/$$expected_id") return 0 ;;'
)
foreach ($fragment in $requiredPeerParserFragments) {
    if (-not $Compose.Contains($fragment, [StringComparison]::Ordinal)) {
        throw "Normal swarm peers exact-suffix parser fragment is absent"
    }
}
if ($Compose -cmatch '(?m)\bswarm peers\s+-q(?:\s|$)') {
    throw "Kubo v0.43 swarm peers quiet option is unsupported"
}
if ($Compose -cmatch '\[\s*"\$\$(?:peer_line|peers_[ab])"\s*=\s*"\$\$(?:expected_id|id_[ab])"\s*\]') {
    throw "Bootstrap must not compare normal swarm peers output to a bare peer ID"
}
```

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
$task1Red = $LASTEXITCODE
if ($task1Red -eq 0) { throw "Task 1 static contract unexpectedly passed before private-swarm files exist" }
```

Expected RED: the first failure identifies the absent entrypoint/service contract, missing AutoConf disablement, absent/nonempty/non-exact AddrFilters clearing, unsupported `swarm peers -q`, bare-ID peer equality, or one of the exact old untyped Kubo config lines; it must not fail because an existing protected hash was weakened. After all five typed source lines and the one-line peer-suffix parser are present, the typed/static contract and source-by-`/bin/sh` fixture turn GREEN before any fresh live topology claim.

- [ ] **Step 2: Create the exact fail-closed private entrypoint**

Create `ipfs/private-swarm-entrypoint.sh` with this complete POSIX `/bin/sh` source. Configuration is re-applied on every start so a persisted repository cannot reopen bootstrap, routing, mDNS, IPv6 swarm, API, or Gateway behavior. The script can be sourced by the pure-shell filter fixture without starting Kubo, but its installed Docker invocation runs `main`. Every pre-daemon wrapper failure is collapsed to the one fixed message; once Kubo starts, the wrapper returns Kubo's exact status.

```sh
#!/bin/sh

daemon_pid=
filter_pid=
log_fifo=
daemon_started=0
wait_interrupted=0

reject_private_swarm() {
    printf '%s\n' 'private swarm startup rejected' >&2
    exit 1
}

filter_daemon_logs() {
    while IFS= read -r log_line || [ -n "$log_line" ]; do
        case "$log_line" in
            'Swarm key fingerprint: '*)
                fingerprint="${log_line#Swarm key fingerprint: }"
                if [ "${#fingerprint}" -eq 32 ]; then
                    case "$fingerprint" in
                        *[!0-9a-f]*) ;;
                        *)
                            printf '%s\n' 'Swarm key fingerprint: [redacted]'
                            continue
                            ;;
                    esac
                fi
                ;;
        esac
        printf '%s\n' "$log_line"
    done
}

interrupt_daemon_wait() {
    signal_name="$1"
    wait_interrupted=1
    if [ -n "$daemon_pid" ] && kill -0 "$daemon_pid" 2>/dev/null; then
        kill "-$signal_name" "$daemon_pid" 2>/dev/null || :
    fi
}

wait_for_daemon_exit() {
    while :; do
        wait_interrupted=0
        set +e
        wait "$daemon_pid"
        daemon_status=$?
        set +e
        if [ "$wait_interrupted" -eq 1 ] && [ "$daemon_status" -gt 128 ]; then
            continue
        fi
        break
    done
}

wait_for_filter_exit() {
    while :; do
        wait_interrupted=0
        set +e
        wait "$filter_pid"
        filter_status=$?
        set +e
        if [ "$wait_interrupted" -eq 1 ] && [ "$filter_status" -gt 128 ]; then
            continue
        fi
        break
    done
}

select_supervisor_status() {
    selected_daemon_status="$1"
    selected_filter_status="$2"
    selected_fifo_status="$3"
    if [ "$selected_daemon_status" -ne 0 ]; then
        return "$selected_daemon_status"
    fi
    if [ "$selected_filter_status" -ne 0 ] || [ "$selected_fifo_status" -ne 0 ]; then
        return 1
    fi
    return 0
}

cleanup_runtime() {
    cleanup_status=$?
    trap - 0 HUP INT TERM
    if [ -n "$daemon_pid" ]; then
        if kill -0 "$daemon_pid" 2>/dev/null; then
            kill -TERM "$daemon_pid" 2>/dev/null || :
        fi
        wait "$daemon_pid" 2>/dev/null || :
    fi
    if [ -n "$filter_pid" ]; then
        if kill -0 "$filter_pid" 2>/dev/null; then
            kill -TERM "$filter_pid" 2>/dev/null || :
        fi
        wait "$filter_pid" 2>/dev/null || :
    fi
    if [ -n "$log_fifo" ]; then
        rm -f "$log_fifo" >/dev/null 2>&1 || :
    fi
    exit "$cleanup_status"
}

validate_swarm_key() {
    source_file="$1"
    [ -f "$source_file" ] || return 1
    [ -r "$source_file" ] || return 1

    byte_count="$(wc -c < "$source_file" 2>/dev/null | tr -d '[:space:]')" || return 1
    [ "$byte_count" = "96" ] || return 1

    line_one=
    line_two=
    line_three=
    extra_line=
    {
        IFS= read -r line_one || return 1
        IFS= read -r line_two || return 1
        IFS= read -r line_three || return 1
        if IFS= read -r extra_line; then
            return 1
        fi
    } < "$source_file"

    [ "$line_one" = "/key/swarm/psk/1.0.0/" ] || return 1
    [ "$line_two" = "/base16/" ] || return 1
    [ "${#line_three}" -eq 64 ] || return 1
    case "$line_three" in
        *[!0-9a-f]*) return 1 ;;
    esac
}

apply_private_config() {
    if [ ! -d "$IPFS_PATH/blocks" ]; then
        ipfs init --empty-repo --profile=server >/dev/null 2>&1 || return 1
    fi

    ipfs config Addresses.API "/ip4/0.0.0.0/tcp/5001" >/dev/null 2>&1 || return 1
    ipfs config Addresses.Gateway "/ip4/0.0.0.0/tcp/8080" >/dev/null 2>&1 || return 1
    ipfs config Addresses.Swarm --json '["/ip4/0.0.0.0/tcp/4001"]' >/dev/null 2>&1 || return 1
    ipfs config Bootstrap --json '[]' >/dev/null 2>&1 || return 1
    ipfs config Routing.Type none >/dev/null 2>&1 || return 1
    ipfs config Discovery.MDNS.Enabled --json false >/dev/null 2>&1 || return 1
    ipfs config AutoConf.Enabled --json false >/dev/null 2>&1 || return 1
    ipfs config Swarm.AddrFilters --json '[]' >/dev/null 2>&1 || return 1
    ipfs config Gateway.NoDNSLink --json true >/dev/null 2>&1 || return 1
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Origin --json '["*"]' >/dev/null 2>&1 || return 1
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","HEAD","OPTIONS"]' >/dev/null 2>&1 || return 1
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Headers --json '["Range","Content-Type"]' >/dev/null 2>&1 || return 1
    ipfs config Gateway.HTTPHeaders.Cache-Control --json '["public, max-age=29030400, immutable"]' >/dev/null 2>&1 || return 1
    ipfs config API.HTTPHeaders.Access-Control-Allow-Origin --json '["*"]' >/dev/null 2>&1 || return 1
    ipfs config API.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","POST","OPTIONS"]' >/dev/null 2>&1 || return 1
    ipfs config API.HTTPHeaders.Access-Control-Allow-Headers --json '["Authorization","Content-Type"]' >/dev/null 2>&1 || return 1
    ipfs config Datastore.BloomFilterSize --json 0 >/dev/null 2>&1 || return 1
}

prepare_private_kubo() {
    IPFS_PATH="${IPFS_PATH:-/data/ipfs}"
    export IPFS_PATH
    source_file="${IPFS_SWARM_KEY_FILE:-}"
    [ -n "$source_file" ] || return 1
    [ "${LIBP2P_FORCE_PNET:-}" = "1" ] || return 1
    command -v ipfs >/dev/null 2>&1 || return 1
    validate_swarm_key "$source_file" || return 1
    apply_private_config || return 1

    umask 077
    rm -f "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    cp "$source_file" "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    chmod 0400 "$IPFS_PATH/swarm.key" >/dev/null 2>&1 || return 1
    export IPFS_TELEMETRY=off
}

run_private_daemon() {
    log_fifo="$IPFS_PATH/.private-swarm-daemon-log.fifo"
    rm -f "$log_fifo" >/dev/null 2>&1 || return 1
    umask 077
    mkfifo "$log_fifo" >/dev/null 2>&1 || return 1
    trap cleanup_runtime 0
    trap 'interrupt_daemon_wait TERM' TERM
    trap 'interrupt_daemon_wait INT' INT
    trap 'interrupt_daemon_wait HUP' HUP

    filter_daemon_logs < "$log_fifo" &
    filter_pid=$!
    ipfs daemon --migrate=true --enable-gc=false > "$log_fifo" 2>&1 &
    daemon_pid=$!
    daemon_started=1

    set +e
    wait_for_daemon_exit
    daemon_pid=
    wait_for_filter_exit
    filter_pid=
    rm -f "$log_fifo" >/dev/null 2>&1
    fifo_cleanup_status=$?
    if [ "$fifo_cleanup_status" -eq 0 ]; then
        log_fifo=
    fi
    trap - 0 HUP INT TERM
    set +e
    select_supervisor_status "$daemon_status" "$filter_status" "$fifo_cleanup_status"
    supervisor_status=$?
    set +e
    return "$supervisor_status"
}

main() {
    if ! prepare_private_kubo; then
        reject_private_swarm
    fi
    run_private_daemon
    daemon_status=$?
    if [ "$daemon_started" -ne 1 ]; then
        reject_private_swarm
    fi
    exit "$daemon_status"
}

case "${0##*/}" in
    private-swarm-entrypoint.sh) main "$@" ;;
esac
```

- [ ] **Step 3: Switch only the Cluster Dockerfile**

Replace `ipfs/cluster.Dockerfile` with the exact source below; do not edit `ipfs/entrypoint.sh`.

```dockerfile
FROM ipfs/kubo:v0.43.0

COPY private-swarm-entrypoint.sh /private-swarm-entrypoint.sh
RUN chmod 0755 /private-swarm-entrypoint.sh

ENTRYPOINT ["/private-swarm-entrypoint.sh"]
```

- [ ] **Step 4: Wire the production secret, A/B wrappers, and exact one-shot bootstrap**

In `docker-compose.cluster.yml`, add the required top-level secret, mount it into both Kubos, and set the two exact environment values. Add `swarm-bootstrap` between Kubo and Cluster service declarations. Preserve all current image versions, volumes, health checks, Cluster settings, gateway settings, and sole fixed-loopback gateway publication.

```yaml
  kubo-a:
    secrets:
      - source: swarm_key
        target: swarm_key
    environment:
      IPFS_PATH: /data/ipfs
      IPFS_SWARM_KEY_FILE: /run/secrets/swarm_key
      LIBP2P_FORCE_PNET: "1"

  kubo-b:
    secrets:
      - source: swarm_key
        target: swarm_key
    environment:
      IPFS_PATH: /data/ipfs
      IPFS_SWARM_KEY_FILE: /run/secrets/swarm_key
      LIBP2P_FORCE_PNET: "1"

  swarm-bootstrap:
    image: ipfs/kubo:v0.43.0
    entrypoint: ["/bin/sh", "-c"]
    command: |
      fail_bootstrap() {
        printf '%s\n' 'private swarm bootstrap failed' >&2
        exit 1
      }
      line_count() {
        printf '%s\n' "$$1" | sed '/^$$/d' | wc -l | tr -d '[:space:]'
      }
      sole_peer() {
        observed="$$1"
        expected_id="$$2"
        nonempty="$$(printf '%s\n' "$$observed" | sed '/^[[:space:]]*$$/d')" || return 1
        [ "$$(line_count "$$nonempty")" = "1" ] || return 1
        peer_line="$$(printf '%s\n' "$$nonempty" | sed -n '1p')" || return 1
        case "$$peer_line" in
          *"/p2p/$$expected_id") return 0 ;;
          *) return 1 ;;
        esac
      }
      exact_peering_text() {
        observed="$$1"
        expected_id="$$2"
        expected_transport="$$3"
        nonempty="$$(printf '%s\n' "$$observed" | sed '/^[[:space:]]*$$/d')" || return 1
        [ "$$(line_count "$$nonempty")" = "2" ] || return 1
        id_line="$$(printf '%s\n' "$$nonempty" | sed -n '1p')" || return 1
        address_line="$$(printf '%s\n' "$$nonempty" | sed -n '2p')" || return 1
        [ "$$id_line" = "$$expected_id" ] || return 1
        tab="$$(printf '\t')"
        case "$$address_line" in
          "$$tab"*) ;;
          *) return 1 ;;
        esac
        trimmed_address="$$(printf '%s\n' "$$address_line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$$//')" || return 1
        [ "$$trimmed_address" = "$$expected_transport" ]
      }
      ensure_peering() {
        api="$$1"
        address="$$2"
        expected_id="$$3"
        expected_transport="$$4"
        observed="$$(ipfs --api="$$api" swarm peering ls 2>/dev/null)" || return 1
        observed_count="$$(line_count "$$observed")"
        if [ "$$observed_count" = "0" ]; then
          ipfs --api="$$api" swarm peering add "$$address" >/dev/null 2>&1 || return 1
          return 0
        fi
        exact_peering_text "$$observed" "$$expected_id" "$$expected_transport"
      }
      bootstrap_private_pair() {
        api_a=/dns4/kubo-a/tcp/5001
        api_b=/dns4/kubo-b/tcp/5001
        id_a="$$(ipfs --api="$$api_a" id -f='<id>' 2>/dev/null)" || return 1
        id_b="$$(ipfs --api="$$api_b" id -f='<id>' 2>/dev/null)" || return 1
        [ -n "$$id_a" ] || return 1
        [ -n "$$id_b" ] || return 1
        [ "$$id_a" != "$$id_b" ] || return 1
        transport_a=/dns4/kubo-a/tcp/4001
        transport_b=/dns4/kubo-b/tcp/4001
        address_a="/dns4/kubo-a/tcp/4001/p2p/$$id_a"
        address_b="/dns4/kubo-b/tcp/4001/p2p/$$id_b"
        ensure_peering "$$api_a" "$$address_b" "$$id_b" "$$transport_b" || return 1
        ensure_peering "$$api_b" "$$address_a" "$$id_a" "$$transport_a" || return 1
        ipfs --api="$$api_a" swarm connect "$$address_b" >/dev/null 2>&1 || return 1
        ipfs --api="$$api_b" swarm connect "$$address_a" >/dev/null 2>&1 || return 1
        attempt=0
        while [ "$$attempt" -lt 60 ]; do
          peers_a="$$(ipfs --api="$$api_a" swarm peers 2>/dev/null)" || return 1
          peers_b="$$(ipfs --api="$$api_b" swarm peers 2>/dev/null)" || return 1
          peering_a="$$(ipfs --api="$$api_a" swarm peering ls 2>/dev/null)" || return 1
          peering_b="$$(ipfs --api="$$api_b" swarm peering ls 2>/dev/null)" || return 1
          if sole_peer "$$peers_a" "$$id_b" &&
             sole_peer "$$peers_b" "$$id_a" &&
             exact_peering_text "$$peering_a" "$$id_b" "$$transport_b" &&
             exact_peering_text "$$peering_b" "$$id_a" "$$transport_a"; then
            return 0
          fi
          attempt=$$((attempt + 1))
          sleep 1
        done
        return 1
      }
      if ! bootstrap_private_pair; then
        fail_bootstrap
      fi
    depends_on:
      kubo-a:
        condition: service_healthy
      kubo-b:
        condition: service_healthy
    restart: "no"

secrets:
  swarm_key:
    file: "${IPFS_S3_SWARM_KEY_FILE:?IPFS_S3_SWARM_KEY_FILE is required}"
```

Add this dependency to both `cluster-a` and `cluster-b`, retaining each paired-Kubo health dependency:

```yaml
      swarm-bootstrap:
        condition: service_completed_successfully
```

- [ ] **Step 5: Add validation-only Kubo C and its different required secret**

Extend `tests/compose.cluster-validation.yml` without changing any current mapping. Kubo C uses the same Cluster image and private wrapper, a tmpfs repository, no Cluster peer, no production counterpart, and an opt-in validation profile.

```yaml
  kubo-c:
    profiles: ["private-swarm-validation"]
    build:
      context: ./ipfs
      dockerfile: cluster.Dockerfile
    image: ghcr.io/hugefiver/ipfs3-kubo-cluster:v0.43.0
    ports:
      - "127.0.0.1:55102:5001"
    tmpfs:
      - /data/ipfs
    secrets:
      - source: swarm_key_wrong
        target: swarm_key
    environment:
      IPFS_PATH: /data/ipfs
      IPFS_SWARM_KEY_FILE: /run/secrets/swarm_key
      LIBP2P_FORCE_PNET: "1"
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 20
      start_period: 15s
    restart: unless-stopped

secrets:
  swarm_key_wrong:
    file: "${IPFS_S3_SWARM_KEY_WRONG_FILE:?IPFS_S3_SWARM_KEY_WRONG_FILE is required}"
```

Compose resolves this override with the base file from the repository root, so the build context is exactly `./ipfs`. The static and `config --quiet` contracts lock that value; no second image, alternate context, or moved Dockerfile is allowed.

- [ ] **Step 6: Turn the Task 1 static contract GREEN**

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Task 1 Cluster static contract failed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Task 1 whitespace check failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Task 1 must not stage files" }
```

Expected GREEN: the five exact typed Kubo config lines are present, AddrFilters clearing occurs exactly once with `[]`, all forbidden untyped/nonempty forms are absent, source loading/syntax succeeds, seven exact production services and eight validation services with profile-gated C are retained, and the strict entrypoint, narrow source-level fingerprint filter, per-wait-flag POSIX FIFO/PID-1 supervisor with exact `37`/`43`/`143` precedence, normal Kubo v0.43 `swarm peers` one-line exact-suffix parser, unchanged two-line peering parser, bootstrap causality, secrets, publications, protected hashes, and clean index all pass. The static contract rejects `swarm peers -q` and any bare-ID peer equality. This static/source GREEN authorizes a fresh live attempt; it is not live topology PASS.

---

### Task 2: Add bounded Kubo HTTP support and private-swarm live tests

**Files:**
- Modify: `tests/support/cluster.rs`
- Modify: `tests/cluster.rs`
- Test: `tests/cluster.Tests.ps1`
- Verify unchanged: `Cargo.toml`, `Cargo.lock`, every `src/**` file

**Interfaces:**
- Consumes: Task 1 loopback Kubo APIs A=`55100`, B=`55101`, C=`55102`; Kubo RPC paths `/api/v0/config/show`, `/api/v0/id`, `/api/v0/swarm/peers`, `/api/v0/swarm/peering/ls`, `/api/v0/swarm/connect`; exact Kubo config casing `AutoConf.Enabled` and `Swarm.AddrFilters`; existing `ProbeError`, bounded reqwest pattern, `endpoint`, and five Cluster tests.
- Produces: `KuboApiClient::new(endpoint: &str) -> Result<Self>`; `config_probe`, `identity_probe`, `swarm_peers_probe`, `peering_probe`, `connect_probe`; required `KuboAutoConf { enabled: bool }` nested under `KuboConfig.auto_conf`; required `KuboSwarmConfig { addr_filters: Vec<String> }` nested under `KuboConfig.swarm`; narrowly applied `deserialize_null_vec_as_empty`; pure `validate_exact_peering(peers, expected_id, expected_transport)`; `wait_for_private_swarm`; `prove_wrong_key_rejection`; pure units `private_kubo_config_contract_rejects_open_discovery`, `private_swarm_peers_json_contract_accepts_null_as_empty`, and `private_peering_json_contract_matches_kubo_v0_43_addrinfo`; Tokio tests `private_swarm_configuration_and_peering` and `private_swarm_wrong_key_rejected`; exact seven Tokio tests total.

- [ ] **Step 1: Add Rust/static expectations and capture RED before helper code**

Extend `tests/cluster.Tests.ps1` to require the exact environment `IPFS_S3_CLUSTER_KUBO_C_URL`, the two new Tokio names before the existing five, seven Tokio tests total, the three private pure-unit names, the support signatures/types below, exact Rust source casing `#[serde(rename = "AutoConf")]`, `#[serde(rename = "Enabled")]`, `#[serde(rename = "Swarm")]`, `#[serde(rename = "AddrFilters")]`, `pub enabled: bool`, and `pub addr_filters: Vec<String>`. Require validator branches rejecting `config.auto_conf.enabled` and nonempty `config.swarm.addr_filters`, plus exact closed/true/missing/malformed AutoConf and empty/nonempty/missing Swarm fixtures. Reject `Option<KuboAutoConf>`, `Option<bool>`, `Option<KuboSwarmConfig>`, `Option<Vec<String>>`, and any serde `default` on the AutoConf/Enabled or Swarm/AddrFilters fields so absence cannot silently decode as false or empty.

For the runtime-confirmed Kubo v0.43 peer wrapper, statically require exact helper `deserialize_null_vec_as_empty`, exact `KuboSwarmPeers.peers` attribute `#[serde(rename = "Peers", default, deserialize_with = "deserialize_null_vec_as_empty")]`, and fixtures for `{"Peers":null}`, missing `Peers`, empty/nonempty arrays, and malformed nonnull/nonarray values. Require exactly one `deserialize_with = "deserialize_null_vec_as_empty"` use in `tests/support/cluster.rs`, on `KuboSwarmPeers.peers` only. Keep `KuboPeeringPeers.peers` as its existing `#[serde(rename = "Peers", default)]` field: no runtime or official evidence authorizes null coercion there. In the `KuboSwarmPeers`/`swarm_peers_probe` source region, reject `StreamDeserializer`, `into_iter::<`, `.lines()`, `split('\n')`, and any NDJSON decoder; official Kubo emits one JSON object wrapper. Also require exact `Peers[{ID,Addrs}]` casing, transport-only expected addresses, valid/extra-peer/extra-address/wrong-ID/wrong-address fixtures, POST-only Kubo paths, bounded timeouts, body-discarding connect failures, and no process/filesystem access in either new live test. Assert zero Rust expectation strings that search `Addrs` for `/p2p/<id>`. Keep the existing recovery receipt filesystem code permitted only in its current support boundary.

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
$task2StaticRed = $LASTEXITCODE
cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact
$task2RustRed = $LASTEXITCODE
cargo test --test cluster cluster_support::private_swarm_peers_json_contract_accepts_null_as_empty -- --exact
$task2NullPeersRed = $LASTEXITCODE
cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact
$task2PeeringRed = $LASTEXITCODE
if ($task2StaticRed -eq 0 -or $task2RustRed -eq 0 -or $task2NullPeersRed -eq 0 -or $task2PeeringRed -eq 0) {
    throw "Task 2 contracts must be RED before Kubo HTTP support exists"
}
```

Expected RED: the new null-slice unit/static source contract fails because Kubo C's single JSON wrapper `{"Peers":null}` currently maps to runtime category `kubo_swarm_peers_malformed_json`; this is not an NDJSON, dependency, application, or live-topology failure.

- [ ] **Step 2: Add exact protocol DTOs and pure private-config validation**

In `tests/support/cluster.rs`, reuse existing `reqwest::{Client, StatusCode, Url}`, serde, `HashSet`, and timeout constants. Add these DTOs and pure validator; do not derive `Serialize` or `Debug` for identity-bearing evidence.

```rust
#[derive(Clone, Deserialize)]
pub struct KuboConfig {
    #[serde(rename = "Bootstrap", default)]
    pub bootstrap: Vec<String>,
    #[serde(rename = "Routing")]
    pub routing: KuboRouting,
    #[serde(rename = "Discovery")]
    pub discovery: KuboDiscovery,
    #[serde(rename = "AutoConf")]
    pub auto_conf: KuboAutoConf,
    #[serde(rename = "Swarm")]
    pub swarm: KuboSwarmConfig,
    #[serde(rename = "Addresses")]
    pub addresses: KuboAddresses,
}

#[derive(Clone, Deserialize)]
pub struct KuboRouting {
    #[serde(rename = "Type")]
    pub kind: String,
}

#[derive(Clone, Deserialize)]
pub struct KuboDiscovery {
    #[serde(rename = "MDNS")]
    pub mdns: KuboMdns,
}

#[derive(Clone, Deserialize)]
pub struct KuboMdns {
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

#[derive(Clone, Deserialize)]
pub struct KuboAutoConf {
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

#[derive(Clone, Deserialize)]
pub struct KuboSwarmConfig {
    #[serde(rename = "AddrFilters")]
    pub addr_filters: Vec<String>,
}

#[derive(Clone, Deserialize)]
pub struct KuboAddresses {
    #[serde(rename = "Swarm", default)]
    pub swarm: Vec<String>,
}

#[derive(Deserialize)]
pub struct KuboIdentity {
    #[serde(rename = "ID")]
    pub id: String,
}

fn deserialize_null_vec_as_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    let value = Option::<Vec<T>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}

#[derive(Deserialize)]
struct KuboSwarmPeers {
    #[serde(rename = "Peers", default, deserialize_with = "deserialize_null_vec_as_empty")]
    peers: Vec<KuboSwarmPeer>,
}

#[derive(Deserialize)]
struct KuboSwarmPeer {
    #[serde(rename = "Peer")]
    peer: String,
}

#[derive(Deserialize)]
struct KuboPeeringPeers {
    #[serde(rename = "Peers", default)]
    peers: Vec<KuboPeeringPeer>,
}

#[derive(Deserialize)]
pub struct KuboPeeringPeer {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Addrs", default)]
    pub addrs: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ConnectObservation {
    Connected,
    Rejected,
}

pub struct PrivateSwarmEvidence {
    pub a_id: String,
    pub b_id: String,
}

fn validate_private_kubo_config(config: &KuboConfig) -> ProbeResult<()> {
    if !config.bootstrap.is_empty()
        || config.routing.kind != "none"
        || config.discovery.mdns.enabled
        || config.auto_conf.enabled
        || !config.swarm.addr_filters.is_empty()
        || config.addresses.swarm.len() != 1
        || config.addresses.swarm[0] != "/ip4/0.0.0.0/tcp/4001"
    {
        return Err(ProbeError::Terminal("private_kubo_config_open"));
    }
    Ok(())
}
```

Add the narrowly scoped peer-wrapper fixture unit immediately beside these DTOs. Kubo v0.43 emits one JSON object; C legitimately has no peers and encodes the Go nil slice as JSON `null`. Missing and null `Peers` become empty, a real array remains unchanged, and every nonnull/nonarray value remains a serde error.

```rust
#[test]
fn private_swarm_peers_json_contract_accepts_null_as_empty() {
    fn decode(json: &str) -> Result<Vec<String>, serde_json::Error> {
        serde_json::from_str::<KuboSwarmPeers>(json).map(|response| {
            response
                .peers
                .into_iter()
                .map(|peer| peer.peer)
                .collect()
        })
    }

    assert_eq!(decode(r#"{"Peers":null}"#).unwrap(), Vec::<String>::new());
    assert_eq!(decode(r#"{}"#).unwrap(), Vec::<String>::new());
    assert_eq!(decode(r#"{"Peers":[]}"#).unwrap(), Vec::<String>::new());
    assert_eq!(
        decode(r#"{"Peers":[{"Peer":"peer-fixture"}]}"#).unwrap(),
        vec!["peer-fixture".to_owned()]
    );

    for malformed in [
        r#"{"Peers":{}}"#,
        r#"{"Peers":"peer-fixture"}"#,
        r#"{"Peers":1}"#,
        r#"{"Peers":false}"#,
    ] {
        assert!(decode(malformed).is_err());
    }
}
```

Do not attach `deserialize_null_vec_as_empty` to `KuboPeeringPeers`: its existing DTO and five Peering fixtures remain unchanged unless later official or sanitized runtime evidence demonstrates a null Peering wrapper. Preserve the current `kubo_swarm_peers_malformed_json` category for nonnull/nonarray peer wrappers; only null and missing stop producing that category.

Add `private_kubo_config_contract_rejects_open_discovery` with explicit decoded JSON fixtures. Every otherwise-valid fixture includes exact `"AutoConf":{"Enabled":false}` and `"Swarm":{"AddrFilters":[]}`. The exact closed configuration passes; nonempty Bootstrap, routing `dht`, mDNS `true`, `"AutoConf":{"Enabled":true}`, a nonempty server-profile filter such as `"/ip4/10.0.0.0/ipcidr/8"`, and an extra IPv6 swarm address each decode and return `Terminal("private_kubo_config_open")`. Four additional fixtures must fail typed decoding before validation: missing `AutoConf`, malformed string AutoConf Enabled, missing `Swarm`, and present `Swarm` with missing `AddrFilters`. Assert each with `serde_json::from_str::<KuboConfig>(...).is_err()`; do not add serde defaults or options to make missing data decode as false or empty. Fixtures contain no real key, peer ID, or digest.

Use these exact AutoConf fragments and outcomes in the pure test:

```rust
let closed = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":false},"Swarm":{"AddrFilters":[]},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let enabled = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":true},"Swarm":{"AddrFilters":[]},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let nonempty_filter = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":false},"Swarm":{"AddrFilters":["/ip4/10.0.0.0/ipcidr/8"]},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let missing = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"Swarm":{"AddrFilters":[]},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let malformed = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":"false"},"Swarm":{"AddrFilters":[]},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let missing_swarm = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":false},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;
let missing_addr_filters = r#"{"Bootstrap":[],"Routing":{"Type":"none"},"Discovery":{"MDNS":{"Enabled":false}},"AutoConf":{"Enabled":false},"Swarm":{},"Addresses":{"Swarm":["/ip4/0.0.0.0/tcp/4001"]}}"#;

let closed_config: KuboConfig = serde_json::from_str(closed)
    .unwrap_or_else(|_| panic!("private_kubo_closed_fixture_invalid"));
assert!(validate_private_kubo_config(&closed_config).is_ok());
let enabled_config: KuboConfig = serde_json::from_str(enabled)
    .unwrap_or_else(|_| panic!("private_kubo_autoconf_enabled_fixture_invalid"));
assert!(matches!(
    validate_private_kubo_config(&enabled_config),
    Err(ProbeError::Terminal("private_kubo_config_open"))
));
let nonempty_filter_config: KuboConfig = serde_json::from_str(nonempty_filter)
    .unwrap_or_else(|_| panic!("private_kubo_addr_filters_fixture_invalid"));
assert!(matches!(
    validate_private_kubo_config(&nonempty_filter_config),
    Err(ProbeError::Terminal("private_kubo_config_open"))
));
assert!(serde_json::from_str::<KuboConfig>(missing).is_err());
assert!(serde_json::from_str::<KuboConfig>(malformed).is_err());
assert!(serde_json::from_str::<KuboConfig>(missing_swarm).is_err());
assert!(serde_json::from_str::<KuboConfig>(missing_addr_filters).is_err());
```

`KuboApiClient::config_probe` must run this validator before identity, peer, Peering, or connect convergence can produce `PrivateSwarmEvidence`. Thus the private live test observes actual `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]` through `/api/v0/config/show` before topology assertions.

Add this pure parser and fixture test beside it. The fixture mirrors Kubo v0.43's `peer.AddrInfo` JSON: the peer identity lives only in `ID`; each `Addrs` value is a transport multiaddress and therefore has no `/p2p/<id>` suffix.

```rust
fn validate_exact_peering(
    peers: &[KuboPeeringPeer],
    expected_id: &str,
    expected_transport: &str,
) -> ProbeResult<()> {
    if peers.len() != 1 {
        return Err(ProbeError::Terminal("private_peering_count_invalid"));
    }
    let peer = &peers[0];
    if peer.id != expected_id {
        return Err(ProbeError::Terminal("private_peering_id_invalid"));
    }
    if peer.addrs.len() != 1 || peer.addrs[0] != expected_transport {
        return Err(ProbeError::Terminal("private_peering_address_invalid"));
    }
    Ok(())
}

#[test]
fn private_peering_json_contract_matches_kubo_v0_43_addrinfo() {
    fn decode(json: &str) -> Vec<KuboPeeringPeer> {
        serde_json::from_str::<KuboPeeringPeers>(json)
            .unwrap_or_else(|_| panic!("private_peering_fixture_invalid"))
            .peers
    }

    let expected_id = "peer-b-fixture";
    let expected_transport = "/dns4/kubo-b/tcp/4001";
    let valid = decode(r#"{"Peers":[{"ID":"peer-b-fixture","Addrs":["/dns4/kubo-b/tcp/4001"]}]}"#);
    let extra_peer = decode(r#"{"Peers":[{"ID":"peer-b-fixture","Addrs":["/dns4/kubo-b/tcp/4001"]},{"ID":"peer-c-fixture","Addrs":["/dns4/kubo-c/tcp/4001"]}]}"#);
    let extra_address = decode(r#"{"Peers":[{"ID":"peer-b-fixture","Addrs":["/dns4/kubo-b/tcp/4001","/ip4/127.0.0.1/tcp/4001"]}]}"#);
    let wrong_id = decode(r#"{"Peers":[{"ID":"peer-c-fixture","Addrs":["/dns4/kubo-b/tcp/4001"]}]}"#);
    let wrong_address = decode(r#"{"Peers":[{"ID":"peer-b-fixture","Addrs":["/dns4/kubo-b/tcp/4001/p2p/peer-b-fixture"]}]}"#);

    assert!(validate_exact_peering(&valid, expected_id, expected_transport).is_ok());
    assert!(matches!(
        validate_exact_peering(&extra_peer, expected_id, expected_transport),
        Err(ProbeError::Terminal("private_peering_count_invalid"))
    ));
    assert!(matches!(
        validate_exact_peering(&extra_address, expected_id, expected_transport),
        Err(ProbeError::Terminal("private_peering_address_invalid"))
    ));
    assert!(matches!(
        validate_exact_peering(&wrong_id, expected_id, expected_transport),
        Err(ProbeError::Terminal("private_peering_id_invalid"))
    ));
    assert!(matches!(
        validate_exact_peering(&wrong_address, expected_id, expected_transport),
        Err(ProbeError::Terminal("private_peering_address_invalid"))
    ));
}
```

- [ ] **Step 3: Add the bounded loopback-only Kubo API client**

Add `KuboApiClient` beside `ClusterClient`. `post_json` accepts only a static path and query pairs, uses `Url::query_pairs_mut`, POSTs through the existing bounded reqwest client, requires success, caps body reads through reqwest's existing call timeout, and maps all parse/status failures to fixed categories without formatting response bodies.

```rust
pub struct KuboApiClient {
    base_url: String,
    http: Client,
}

impl KuboApiClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        let base_url = validate_loopback_http(endpoint, "kubo_endpoint_not_loopback_http")?;
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(CALL_TIMEOUT)
            .build()
            .map_err(|_| anyhow!("kubo_client_build_failed"))?;
        Ok(Self { base_url, http })
    }

    async fn post(&self, path: &'static str, query: &[(&str, &str)]) -> ProbeResult<reqwest::Response> {
        let mut url = Url::parse(&format!("{}{}", self.base_url, path))
            .map_err(|_| ProbeError::Terminal("kubo_url_invalid"))?;
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        self.http
            .post(url)
            .send()
            .await
            .map_err(|_| ProbeError::Transient("kubo_request_transport"))
    }

    async fn post_json<T: DeserializeOwned>(&self, path: &'static str) -> ProbeResult<T> {
        let response = self.post(path, &[]).await?;
        if !response.status().is_success() {
            return Err(ProbeError::Terminal("kubo_status_not_success"));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|_| ProbeError::Transient("kubo_response_body_transport"))?;
        serde_json::from_slice(&bytes).map_err(|_| ProbeError::Terminal("kubo_malformed_json"))
    }

    pub async fn config_probe(&self) -> ProbeResult<KuboConfig> {
        let config = self.post_json("/api/v0/config/show").await?;
        validate_private_kubo_config(&config)?;
        Ok(config)
    }

    pub async fn identity_probe(&self) -> ProbeResult<KuboIdentity> {
        let identity: KuboIdentity = self.post_json("/api/v0/id").await?;
        if identity.id.is_empty() {
            return Err(ProbeError::Terminal("kubo_identity_empty"));
        }
        Ok(identity)
    }

    pub async fn swarm_peers_probe(&self) -> ProbeResult<Vec<String>> {
        let response: KuboSwarmPeers = self.post_json("/api/v0/swarm/peers").await?;
        let peers = response.peers.into_iter().map(|peer| peer.peer).collect::<Vec<_>>();
        if peers.iter().any(String::is_empty) {
            return Err(ProbeError::Terminal("kubo_swarm_peer_empty"));
        }
        Ok(sorted_set(&peers))
    }

    pub async fn peering_probe(&self) -> ProbeResult<Vec<KuboPeeringPeer>> {
        self.post_json::<KuboPeeringPeers>("/api/v0/swarm/peering/ls")
            .await
            .map(|response| response.peers)
    }

    pub async fn connect_probe(&self, multiaddr: &str) -> ProbeResult<ConnectObservation> {
        let response = self.post("/api/v0/swarm/connect", &[("arg", multiaddr)]).await?;
        let status = response.status();
        drop(response);
        if status.is_success() {
            Ok(ConnectObservation::Connected)
        } else {
            Ok(ConnectObservation::Rejected)
        }
    }
}
```

The implementation must not print or return an error containing `multiaddr`, an ID, response bytes, or URL query. If Kubo's actual JSON field differs, adjust only the DTO rename backed by a sanitized captured shape and add a pure fixture before changing it; do not bypass typed decoding.

- [ ] **Step 4: Add private-pair convergence and wrong-key proof helpers**

Implement `wait_for_private_swarm(a, b, c, timeout)` to call A and B `config_probe` first and require explicit decoded `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]` along with all other closed-config fields before accepting any identity, peer, Peering, or connect evidence. It then polls identity/peer/peering views. A ready result requires A peers exactly `[B]`, B exactly `[A]`, C empty, `validate_exact_peering(a_peerings, b_id, "/dns4/kubo-b/tcp/4001")`, `validate_exact_peering(b_peerings, a_id, "/dns4/kubo-a/tcp/4001")`, and an empty C Peering list. The HTTP Peering `Addrs` comparisons are exact transport-only comparisons and never search for `/p2p/<id>`. Return `PrivateSwarmEvidence` only in memory. AutoConf true or nonempty AddrFilters is terminal `private_kubo_config_open`; missing/malformed AutoConf, Swarm, or AddrFilters is terminal typed decode failure; transient transport probes sleep via `wait_or_timeout`.

Implement `prove_wrong_key_rejection(a, b, c, evidence)` to call C's connect endpoint for `/dns4/kubo-a/tcp/4001/p2p/{a_id}` and `/dns4/kubo-b/tcp/4001/p2p/{b_id}`, require `Rejected` for both, then re-read peer sets and require A=`[B]`, B=`[A]`, C=`[]`. It discards both connect bodies and returns only fixed `ProbeError` categories.

```rust
pub async fn wait_for_private_swarm(
    a: &KuboApiClient,
    b: &KuboApiClient,
    c: &KuboApiClient,
    timeout: Duration,
) -> ProbeResult<PrivateSwarmEvidence>;

pub async fn prove_wrong_key_rejection(
    a: &KuboApiClient,
    b: &KuboApiClient,
    c: &KuboApiClient,
    evidence: &PrivateSwarmEvidence,
) -> ProbeResult<()>;
```

- [ ] **Step 5: Add two HTTP-only Tokio scenarios before the existing five**

In `tests/cluster.rs`, add `IPFS_S3_CLUSTER_KUBO_C_URL`, import the Task 2 interfaces, and add exactly these tests before `cluster_topology_converges`. Neither test reads files, inspects modes/digests, launches processes, invokes Docker, or prints identities.

```rust
#[tokio::test]
async fn private_swarm_configuration_and_peering() {
    let kubo_a = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_A_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_a_client_invalid"));
    let kubo_b = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_B_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_b_client_invalid"));
    let kubo_c = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_C_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_c_client_invalid"));
    expect_cluster_result(
        wait_for_private_swarm(&kubo_a, &kubo_b, &kubo_c, CONVERGENCE_TIMEOUT).await,
        "private_swarm_convergence_failed",
    );
    println!("private_swarm_peers=2 wrong_key_peers=0");
}

#[tokio::test]
async fn private_swarm_wrong_key_rejected() {
    let kubo_a = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_A_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_a_client_invalid"));
    let kubo_b = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_B_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_b_client_invalid"));
    let kubo_c = KuboApiClient::new(&endpoint("IPFS_S3_CLUSTER_KUBO_C_URL"))
        .unwrap_or_else(|_| panic!("private_kubo_c_client_invalid"));
    let evidence = expect_cluster_result(
        wait_for_private_swarm(&kubo_a, &kubo_b, &kubo_c, CONVERGENCE_TIMEOUT).await,
        "wrong_key_precondition_failed",
    );
    expect_cluster_result(
        prove_wrong_key_rejection(&kubo_a, &kubo_b, &kubo_c, &evidence).await,
        "wrong_key_rejection_failed",
    );
    println!("wrong_key_connect=rejected");
}
```

- [ ] **Step 6: Run pure unit, compile, static, and protected-boundary GREEN**

```powershell
cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo config unit contract failed" }
cargo test --test cluster cluster_support::private_swarm_peers_json_contract_accepts_null_as_empty -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo null peer-list contract failed" }
cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo Peering JSON contract failed" }
cargo test --test cluster --no-run
if ($LASTEXITCODE -ne 0) { throw "Cluster target compile failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Task 2 static contract failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Task 2 must not stage files" }
```

Expected GREEN: all three private pure fixture suites PASS: config closure, the single-wrapper peer list (`null`/missing/empty/nonempty plus malformed nonarrays), and five Peering shapes. Target compiles with seven Tokio tests; static source locks exactly one null-vector deserializer use on `KuboSwarmPeers.peers`, rejects NDJSON decoding, and leaves `KuboPeeringPeers` unchanged. Cargo/application hashes remain unchanged. No live test is claimed here.

---

### Task 3: Extend the existing Cluster workflow and shared static contracts

**Files:**
- Modify: `.github/workflows/release-validation.yml`
- Modify: `tests/cluster.Tests.ps1`
- Modify: `tests/release-validation.Tests.ps1`
- Verify unchanged: all other workflow jobs; `tests/postgres-production-baseline.Tests.ps1`; `tests/multi-gateway.Tests.ps1`; exact five client static commands

**Interfaces:**
- Consumes: Task 1 service/profile/secret names, sourceable `filter_daemon_logs`/`select_supervisor_status`, per-wait interruption-flag PID-1 supervisor, fixed marker/error strings, and exact CLI peering shape; Task 2's three private pure-unit and two live-test names; current unique-project/state-receipt workflow; current sanitized diagnostic functions; current five Cluster scenarios and causal receipts.
- Produces: generated `IPFS_S3_SWARM_KEY_FILE` and `IPFS_S3_SWARM_KEY_WRONG_FILE`; independent ownership markers; pure-shell source-filter/status-precedence fixture; stub-daemon exact `37`, handled-TERM `43`, and unhandled-TERM `143` runtime receipts; post-exit FIFO-absence/filter-reap evidence; direct real-Kubo source-redaction receipt; wrapper-negative, bootstrap, outer-filesystem, private-Rust, topology, proxy, replication, outage, and recovery receipts; A/B restart ordering; prefix-specific defense-in-depth redactor; exact same six jobs and five client static commands.

- [ ] **Step 1: Make both static contracts RED on the new causal workflow**

In both static scripts, require: no new job; two runner-temp path environment entries; key generation before Compose config; all five exact typed Kubo config lines, exactly one empty AddrFilters command, and rejection of missing/nonempty/non-exact forms; exact nonoptional/nondefault Rust `AutoConf.Enabled: bool` and `Swarm.AddrFilters: Vec<String>` source plus false/true/missing/malformed/empty/nonempty fixtures; exactly one `deserialize_null_vec_as_empty` serde use on `KuboSwarmPeers.peers`, null/missing/empty/nonempty/malformed peer-wrapper fixtures, no helper on `KuboPeeringPeers`, and no NDJSON decoder; bootstrap normal `swarm peers` capture, one nonempty full-multiaddress line, exact `/p2p/$expected_id` suffix, no `-q`, no bare-ID equality, and unchanged two-line Peering parser; missing-source and `LIBP2P_FORCE_PNET!=1` wrapper negatives before startup; a source-based pure-shell filter/status-precedence fixture; normal stub daemon exit `37`; PID-1 `TERM` forwarding to a handling daemon producing exact `43`; PID-1 `TERM` forwarding to an unhandled daemon producing exact `143` after a second wait with a cleared interruption flag; no `kill -0`-based re-wait branch; bounded completion; post-exit `docker diff` FIFO absence; source-level filter wait/reset; exact project labels and stopped-container cleanup for all three proof containers; profile-gated C; startup of seven production services plus C; direct unsanitized Kubo A/B/C log capture that rejects a raw `Swarm key fingerprint: <32hex>` and requires the fixed redacted marker before diagnostic redaction; successful bootstrap inspect before Cluster evidence; mode/digest-in-memory assertions; all three private pure Rust units and private Rust tests before existing Cluster tests; A/B stop, Kubo A/B restart, bootstrap rerun, then Cluster A/B restart; logs including bootstrap/C; owned secret deletion; residual zero; exact environment restoration; no rendered config/key/digest/PeerID; and all workflow `shell: pwsh` bodies AST parse.

AutoConf and AddrFilters extend the existing private-config unit and live test; the peer-output correction stays inside the existing bootstrap stage. The null-slice correction adds one pure support-unit invocation immediately before the live private-swarm test but no workflow job or new causal stage. The existing private-config command remains before `private_swarm_configuration_and_peering`, whose helper validates A/B AutoConf false and AddrFilters empty before peer topology. Bootstrap must still complete successfully before Cluster evidence.

Lock exactly twelve Cluster-target cargo invocations: four plain support units (existing release-version, private config, private swarm-peer null-wrapper JSON, and private Peering JSON), two initial private live tests, the existing five live scenarios, and one post-restart repeat of `private_swarm_configuration_and_peering`. The repeated command is allowed only after the bootstrap-recovery receipt; every other existing command remains single-occurrence and in its current relative order.

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
$releaseRed = $LASTEXITCODE
pwsh -NoProfile -File tests/cluster.Tests.ps1
$clusterRed = $LASTEXITCODE
if ($releaseRed -eq 0 -or $clusterRed -eq 0) {
    throw "Both workflow contracts must be RED before private-swarm workflow steps exist"
}
```

Expected RED: missing peer-null support-unit command/static source contract while generation/wrapper/bootstrap/private-test causality, job count, and existing command order remain valid.

- [ ] **Step 2: Add job-scoped paths and strict secret creation without emitting content**

Add only these non-secret path values to the existing Cluster job `env`; do not put key text or digest in YAML/environment.

```yaml
      IPFS_S3_SWARM_KEY_FILE: ${{ runner.temp }}/ipfs3-swarm-${{ github.run_id }}-${{ github.run_attempt }}.key
      IPFS_S3_SWARM_KEY_WRONG_FILE: ${{ runner.temp }}/ipfs3-swarm-wrong-${{ github.run_id }}-${{ github.run_attempt }}.key
      IPFS_S3_CLUSTER_KUBO_C_URL: http://127.0.0.1:55102
```

Insert a `Generate private-swarm key files` PowerShell step before Compose configuration. It validates both parents equal the canonical runner temp directory, refuses pre-existing paths, uses the following function twice, confirms the two 96-byte payloads differ in memory, validates the exact regex after strict UTF-8 decoding, and writes each ownership marker only after its own flush succeeds.

```powershell
function New-PrivateSwarmKeyFile {
    param([Parameter(Mandatory)][string]$Path)
    $bytes = [byte[]]::new(32)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    $hex = [Convert]::ToHexString($bytes).ToLowerInvariant()
    $text = "/key/swarm/psk/1.0.0/`n/base16/`n$hex`n"
    $encoding = [Text.UTF8Encoding]::new($false, $true)
    $payload = $encoding.GetBytes($text)
    if ($payload.Length -ne 96) { throw "Generated swarm key length is invalid" }
    $stream = $null
    try {
        $stream = [IO.File]::Open($Path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $stream.Write($payload, 0, $payload.Length)
        $stream.Flush($true)
    } finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }
}

$runnerTempValue = [Environment]::GetEnvironmentVariable("RUNNER_TEMP", "Process")
if ([string]::IsNullOrWhiteSpace($runnerTempValue)) { throw "RUNNER_TEMP is absent" }
$runnerTemp = [IO.Path]::GetFullPath($runnerTempValue)
if (-not [IO.Directory]::Exists($runnerTemp)) { throw "Runner temp directory is absent" }
$mainPath = [IO.Path]::GetFullPath($env:IPFS_S3_SWARM_KEY_FILE)
$wrongPath = [IO.Path]::GetFullPath($env:IPFS_S3_SWARM_KEY_WRONG_FILE)
foreach ($path in @($mainPath, $wrongPath)) {
    if ([IO.Path]::GetFullPath([IO.Path]::GetDirectoryName($path)) -cne $runnerTemp) { throw "Swarm key parent is not runner temp" }
    if (Test-Path -LiteralPath $path) { throw "Swarm key path already exists" }
}
New-PrivateSwarmKeyFile -Path $mainPath
"SWARM_KEY_MAIN_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
New-PrivateSwarmKeyFile -Path $wrongPath
"SWARM_KEY_WRONG_OWNED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
$strictUtf8 = [Text.UTF8Encoding]::new($false, $true)
$mainText = $strictUtf8.GetString([IO.File]::ReadAllBytes($mainPath))
$wrongText = $strictUtf8.GetString([IO.File]::ReadAllBytes($wrongPath))
$pattern = '^/key/swarm/psk/1\.0\.0/\n/base16/\n[0-9a-f]{64}\n\z'
if ($mainText -notmatch $pattern -or $wrongText -notmatch $pattern -or $mainText -ceq $wrongText) {
    throw "Generated swarm key contract failed"
}
"PRIVATE_SWARM_KEYS_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

- [ ] **Step 3: Extend configuration probes and run two wrapper negatives before startup**

Add both path variables to required-presence/missing-variable Compose checks, preserving exact restoration in `finally`; never render Compose config. After `config --quiet`, build `kubo-a`, mark the unique project attempted, then use project-owned `docker compose run --no-deps --rm` twice: first point `IPFS_SWARM_KEY_FILE` to an absent in-container path, then set `LIBP2P_FORCE_PNET=0`. Capture native output, run it through the expanded redactor before any emission, require nonzero and exactly one fixed wrapper message per invocation, require no readiness text, and then write `PRIVATE_SWARM_WRAPPER_NEGATIVE_GREEN=true`.

```powershell
$compose = @(
    "--project-name", $env:COMPOSE_PROJECT_NAME,
    "--profile", "private-swarm-validation",
    "-f", "docker-compose.cluster.yml",
    "-f", "tests/compose.cluster-validation.yml"
)
docker compose @compose build kubo-a
if ($LASTEXITCODE -ne 0) { throw "Cluster Kubo image build failed" }
"CLUSTER_PINSET_ATTEMPTED=true" | Add-Content -LiteralPath $env:GITHUB_ENV
$missingOutput = @(docker compose @compose run --no-deps --rm -e IPFS_SWARM_KEY_FILE=/run/secrets/absent kubo-a 2>&1)
$missingExit = $LASTEXITCODE
$forceOutput = @(docker compose @compose run --no-deps --rm -e LIBP2P_FORCE_PNET=0 kubo-a 2>&1)
$forceExit = $LASTEXITCODE
if ($missingExit -eq 0 -or $forceExit -eq 0) { throw "Private wrapper accepted an invalid invocation" }
$combined = @($missingOutput + $forceOutput | ForEach-Object { Protect-ClusterDiagnosticLine -Line "$_" })
if ((@($combined | Where-Object { $_ -ceq "private swarm startup rejected" })).Count -ne 2) {
    throw "Private wrapper did not return its fixed redacted error twice"
}
if (($combined -join "`n") -match '(?i)daemon is ready|/key/swarm/psk/1\.0\.0/|(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])') {
    throw "Private wrapper negative output leaked or reached readiness"
}
"PRIVATE_SWARM_WRAPPER_NEGATIVE_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

In the same step, source the installed script under `/bin/sh` and exercise its real filter and status-selection functions without starting the wrapper. The matching fixture fingerprint must disappear, the fixed marker must appear, and an unrelated 32-hex string plus a CID-bearing line must remain exact. The status matrix must prove daemon `37` wins over filter/FIFO failures, daemon `0` plus filter failure maps to `1`, daemon `0` plus FIFO failure maps to `1`, and all-zero status remains `0`. Then run three uniquely named, exactly labelled disposable containers against the built Cluster Kubo image and the real wrapper: normal daemon exit `37`; PID-1 `TERM` forwarded to a daemon that handles it and exits `43`; and PID-1 `TERM` forwarded to a daemon that does not handle it and therefore yields real child status `143` on the next, non-interrupted wait. Before each exact `docker wait`, poll stopped state with a 30-second bound; require exact exit/PID `0`, no final FIFO path in captured `docker diff`, and the source-level `wait_for_filter_exit`/`filter_pid=` reap sequence. Every proof output stays captured. The `finally` block inspects the exact project label before stopping/removing all proof containers and fails closed on residue.

```powershell
$filterFixture = @'
. /private-swarm-entrypoint.sh
actual="$(printf '%s\n' \
  'Swarm key fingerprint: 0123456789abcdef0123456789abcdef' \
  'ordinary32=fedcba9876543210fedcba9876543210' \
  'cid=bafybeigdyrzt5sfp7udm7hu76uh7y26nf3ftejnv3m2q7wz4l5xw6abcde' | filter_daemon_logs)"
expected="$(printf '%s\n' \
  'Swarm key fingerprint: [redacted]' \
  'ordinary32=fedcba9876543210fedcba9876543210' \
  'cid=bafybeigdyrzt5sfp7udm7hu76uh7y26nf3ftejnv3m2q7wz4l5xw6abcde')"
[ "$actual" = "$expected" ]
set +e
select_supervisor_status 37 9 9
[ "$?" -eq 37 ] || exit 81
select_supervisor_status 0 9 0
[ "$?" -eq 1 ] || exit 82
select_supervisor_status 0 0 9
[ "$?" -eq 1 ] || exit 83
select_supervisor_status 0 0 0
[ "$?" -eq 0 ] || exit 84
'@
$fixtureOutput = @(docker compose @compose run --no-deps --rm --entrypoint /bin/sh kubo-a -c $filterFixture 2>&1)
$fixtureExit = $LASTEXITCODE
if ($fixtureExit -ne 0) { throw "Private fingerprint filter shell fixture failed" }
$fixtureOutput = $null
"PRIVATE_SWARM_FILTER_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV

$stubSource = @'
#!/bin/sh
case "${1:-}" in
    init)
        mkdir -p "$IPFS_PATH/blocks"
        exit 0
        ;;
    config)
        exit 0
        ;;
    daemon)
        printf '%s\n' 'Swarm key fingerprint: 0123456789abcdef0123456789abcdef'
        printf '%s\n' 'ordinary32=fedcba9876543210fedcba9876543210'
        if [ -n "${STUB_EXIT_STATUS:-}" ]; then
            exit "$STUB_EXIT_STATUS"
        fi
        case "${STUB_TERM_MODE:-handled}" in
            handled)
                trap 'exit 43' TERM
                trap 'exit 44' INT
                trap 'exit 45' HUP
                ;;
            unhandled) ;;
            *) exit 94 ;;
        esac
        while :; do sleep 1; done
        ;;
esac
exit 0
'@
$stubBytes = [Text.Encoding]::UTF8.GetBytes(($stubSource -replace "`r`n", "`n"))
$stubBase64 = [Convert]::ToBase64String($stubBytes)
$launcher = @'
mkdir -p /tmp/private-swarm-test-bin || exit 91
printf '%s' "$IPFS_STUB_BASE64" | base64 -d > /tmp/private-swarm-test-bin/ipfs || exit 92
chmod 0700 /tmp/private-swarm-test-bin/ipfs || exit 93
PATH="/tmp/private-swarm-test-bin:$PATH"
export PATH
exec /private-swarm-entrypoint.sh
'@
$imageLines = @(docker compose @compose images --quiet kubo-a 2>&1)
$imageExit = $LASTEXITCODE
$imageIds = @($imageLines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim() } | Sort-Object -Unique)
if ($imageExit -ne 0 -or $imageIds.Count -ne 1) { throw "Cluster Kubo image identity query failed" }
$imageId = $imageIds[0]
$exitContainer = "$($env:COMPOSE_PROJECT_NAME)-supervisor-exit"
$handledSignalContainer = "$($env:COMPOSE_PROJECT_NAME)-supervisor-term-handled"
$unhandledSignalContainer = "$($env:COMPOSE_PROJECT_NAME)-supervisor-term-unhandled"
$proofContainers = [Collections.Generic.List[string]]::new()
$proofCleanupErrors = [Collections.Generic.List[string]]::new()
$proofFailure = $null

function Assert-ProofContainerCompletion {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][int]$ExpectedExit
    )
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    $finalState = $null
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        $stateLines = @(docker inspect --format '{{.State.Running}}|{{.State.ExitCode}}|{{.State.Pid}}' $Name 2>&1)
        $stateExit = $LASTEXITCODE
        if ($stateExit -ne 0) { throw "Supervisor proof state query failed" }
        $state = ($stateLines -join "").Trim()
        if ($state.StartsWith("false|", [StringComparison]::Ordinal)) { $finalState = $state; break }
        Start-Sleep -Milliseconds 250
    }
    if ($null -eq $finalState) { throw "Supervisor proof container did not stop within 30 seconds" }
    if ($finalState -cne "false|$ExpectedExit|0") { throw "Supervisor proof final state mismatch" }
    $waitOutput = @(docker wait $Name 2>&1)
    $waitExit = $LASTEXITCODE
    $waitValues = @($waitOutput | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim() })
    if ($waitExit -ne 0 -or $waitValues.Count -ne 1 -or $waitValues[0] -cne "$ExpectedExit") { throw "Supervisor proof wait status mismatch" }
    $diffOutput = @(docker diff $Name 2>&1)
    $diffExit = $LASTEXITCODE
    if ($diffExit -ne 0) { throw "Supervisor proof filesystem query failed" }
    if (($diffOutput -join "`n") -match '(?m)[\\/]\.private-swarm-daemon-log\.fifo$') { throw "Supervisor FIFO remains after wrapper exit" }
}

function Invoke-TermSupervisorProof {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][ValidateSet("handled", "unhandled")][string]$Mode,
        [Parameter(Mandatory)][int]$ExpectedExit
    )
    $proofContainers.Add($Name)
    $idLines = @(docker run --detach --name $Name --label "com.docker.compose.project=$($env:COMPOSE_PROJECT_NAME)" --entrypoint /bin/sh --mount "type=bind,src=$($env:IPFS_S3_SWARM_KEY_FILE),dst=/run/secrets/swarm_key,readonly" --env IPFS_PATH=/tmp/ipfsrepo --env IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key --env LIBP2P_FORCE_PNET=1 --env "IPFS_STUB_BASE64=$stubBase64" --env "STUB_TERM_MODE=$Mode" $imageId -c $launcher 2>&1)
    $runExit = $LASTEXITCODE
    if ($runExit -ne 0 -or @($idLines | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }).Count -ne 1) { throw "Signal-proof container did not start" }
    $readyDeadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    $ready = $false
    while ([DateTimeOffset]::UtcNow -lt $readyDeadline) {
        $logLines = @(docker logs $Name 2>&1)
        $logExit = $LASTEXITCODE
        if ($logExit -ne 0) { throw "Signal-proof log capture failed" }
        $logText = $logLines -join "`n"
        if ($logText -match '(?i)Swarm key fingerprint:\s*[0-9a-f]{32}(?![0-9a-f])') { throw "Signal-proof log leaked raw fingerprint" }
        if ($logText -match 'Swarm key fingerprint: \[redacted\]') { $ready = $true; break }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw "Signal-proof wrapper did not become ready" }
    $killOutput = @(docker kill --signal TERM $Name 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "Signal-proof TERM delivery failed" }
    Assert-ProofContainerCompletion -Name $Name -ExpectedExit $ExpectedExit
}

try {
    $proofContainers.Add($exitContainer)
    $exitOutput = @(docker run --name $exitContainer --label "com.docker.compose.project=$($env:COMPOSE_PROJECT_NAME)" --entrypoint /bin/sh --mount "type=bind,src=$($env:IPFS_S3_SWARM_KEY_FILE),dst=/run/secrets/swarm_key,readonly" --env IPFS_PATH=/tmp/ipfsrepo --env IPFS_SWARM_KEY_FILE=/run/secrets/swarm_key --env LIBP2P_FORCE_PNET=1 --env "IPFS_STUB_BASE64=$stubBase64" --env STUB_EXIT_STATUS=37 $imageId -c $launcher 2>&1)
    $exitStatus = $LASTEXITCODE
    if ($exitStatus -ne 37) { throw "Wrapper did not preserve daemon exit 37" }
    Assert-ProofContainerCompletion -Name $exitContainer -ExpectedExit 37
    $exitLogs = @(docker logs $exitContainer 2>&1)
    $exitLogStatus = $LASTEXITCODE
    if ($exitLogStatus -ne 0) { throw "Exit-proof log capture failed" }
    $exitText = $exitLogs -join "`n"
    if ($exitText -match '(?i)Swarm key fingerprint:\s*[0-9a-f]{32}(?![0-9a-f])') { throw "Exit-proof log leaked raw fingerprint" }
    if ($exitText -notmatch 'Swarm key fingerprint: \[redacted\]' -or $exitText -notmatch 'ordinary32=fedcba9876543210fedcba9876543210') { throw "Exit-proof log filter contract failed" }

    Invoke-TermSupervisorProof -Name $handledSignalContainer -Mode handled -ExpectedExit 43
    Invoke-TermSupervisorProof -Name $unhandledSignalContainer -Mode unhandled -ExpectedExit 143
} catch {
    $proofFailure = $_.Exception
} finally {
    foreach ($containerName in $proofContainers) {
        $labelLines = @(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' $containerName 2>$null)
        $labelExit = $LASTEXITCODE
        if ($labelExit -ne 0) { continue }
        $label = ($labelLines -join "").Trim()
        if ($label -cne $env:COMPOSE_PROJECT_NAME) {
            $proofCleanupErrors.Add("proof container label mismatch: $containerName")
            continue
        }
        $runningLines = @(docker inspect --format '{{.State.Running}}' $containerName 2>&1)
        $runningExit = $LASTEXITCODE
        if ($runningExit -ne 0) {
            $proofCleanupErrors.Add("proof container state query failed: $containerName")
            continue
        }
        if (($runningLines -join "").Trim() -ceq "true") {
            $stopOutput = @(docker stop --time 10 $containerName 2>&1)
            if ($LASTEXITCODE -ne 0) { $proofCleanupErrors.Add("proof container stop failed: $containerName"); continue }
        }
        $removeOutput = @(docker rm $containerName 2>&1)
        if ($LASTEXITCODE -ne 0) { $proofCleanupErrors.Add("proof container removal failed: $containerName") }
    }
}
$stubBytes = $null
$stubBase64 = $null
$exitOutput = $null
$exitLogs = $null
$exitText = $null
if ($null -ne $proofFailure) { throw $proofFailure }
if ($proofCleanupErrors.Count -ne 0) { throw ($proofCleanupErrors -join "; ") }
"PRIVATE_SWARM_SUPERVISOR_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

- [ ] **Step 4: Expand the redactor, startup, bootstrap receipt, and outer-only assertions**

Extend every copy of `Protect-ClusterDiagnosticLine` with a prefix-specific Kubo fingerprint replacement before exact PSK-format and standalone 64-lowercase-hex redaction, while retaining current PeerID logic plus ordinary-32-hex and CID-preservation fixtures. This is defense in depth only; the direct-log receipt below proves the container log source is already redacted.

```powershell
$safe = [regex]::Replace($safe, '(?i)(Swarm key fingerprint:\s*)[0-9a-f]{32}(?![0-9a-f])', '$1[redacted]')
$safe = [regex]::Replace($safe, '(?i)/key/swarm/psk/1\.0\.0/|/base16/', '[REDACTED_SWARM_KEY_FORMAT]')
$safe = [regex]::Replace($safe, '(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])', '[REDACTED_64_HEX]')
```

Require wrapper-negative and filter/supervisor receipts, then start exactly `postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway` with the validation profile and `--wait`. Before applying any diagnostic redactor, capture direct Kubo A/B/C logs into memory, reject any raw prefixed 32-hex fingerprint, require at least three exact fixed redacted markers, clear the raw variables, and write `PRIVATE_SWARM_DIRECT_LOG_GREEN=true`. Inspect the one bootstrap container by Compose project/service labels, require `exited` and exit `0`, and only then write `PRIVATE_SWARM_BOOTSTRAP_GREEN=true`. Capture `stat -c %a` and `sha256sum` outputs into PowerShell variables, require mode `400` for A/B and equal A/B digest only in memory, clear variables, and write `PRIVATE_SWARM_OUTER_GREEN=true`. No mode/digest command output is emitted.

Use this exact workflow run body after the expanded redactor is available in the step:

```powershell
if ($env:PRIVATE_SWARM_WRAPPER_NEGATIVE_GREEN -ne "true") { throw "Wrapper-negative receipt is required" }
if ($env:PRIVATE_SWARM_FILTER_GREEN -ne "true" -or $env:PRIVATE_SWARM_SUPERVISOR_GREEN -ne "true") { throw "Filter/supervisor receipts are required" }
$compose = @(
    "--project-name", $env:COMPOSE_PROJECT_NAME,
    "--profile", "private-swarm-validation",
    "-f", "docker-compose.cluster.yml",
    "-f", "tests/compose.cluster-validation.yml"
)
docker compose @compose up --detach --build --wait --wait-timeout 300 postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway
if ($LASTEXITCODE -ne 0) { throw "Private Cluster topology did not become healthy" }
$directKuboLogs = @(docker compose @compose logs --no-color kubo-a kubo-b kubo-c 2>&1)
$directKuboLogExit = $LASTEXITCODE
if ($directKuboLogExit -ne 0) { throw "Direct Kubo log capture failed" }
$directKuboText = $directKuboLogs -join "`n"
if ($directKuboText -match '(?i)Swarm key fingerprint:\s*[0-9a-f]{32}(?![0-9a-f])') { throw "Raw Kubo swarm fingerprint reached container logs" }
$redactedFingerprintCount = ([regex]::Matches($directKuboText, 'Swarm key fingerprint: \[redacted\]')).Count
if ($redactedFingerprintCount -lt 3) { throw "Source-redacted Kubo fingerprint marker is absent" }
$directKuboLogs = $null
$directKuboText = $null
$redactedFingerprintCount = $null
"PRIVATE_SWARM_DIRECT_LOG_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
$bootstrapIds = @(docker compose @compose ps --all --quiet swarm-bootstrap | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($LASTEXITCODE -ne 0 -or $bootstrapIds.Count -ne 1) { throw "Expected one bootstrap container" }
$bootstrapId = $bootstrapIds[0].Trim()
$projectLabel = (docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}' $bootstrapId).Trim()
if ($LASTEXITCODE -ne 0 -or $projectLabel -cne $env:COMPOSE_PROJECT_NAME) { throw "Bootstrap project label mismatch" }
$serviceLabel = (docker inspect --format '{{ index .Config.Labels "com.docker.compose.service" }}' $bootstrapId).Trim()
if ($LASTEXITCODE -ne 0 -or $serviceLabel -cne "swarm-bootstrap") { throw "Bootstrap service label mismatch" }
$bootstrapState = (docker inspect --format '{{.State.Status}}|{{.State.ExitCode}}' $bootstrapId).Trim()
if ($LASTEXITCODE -ne 0 -or $bootstrapState -cne "exited|0") { throw "Bootstrap did not complete successfully" }
"PRIVATE_SWARM_BOOTSTRAP_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
$modeALines = @(docker compose @compose exec -T kubo-a stat -c "%a" /data/ipfs/swarm.key 2>&1)
$modeAExit = $LASTEXITCODE
$modeBLines = @(docker compose @compose exec -T kubo-b stat -c "%a" /data/ipfs/swarm.key 2>&1)
$modeBExit = $LASTEXITCODE
if ($modeAExit -ne 0 -or $modeBExit -ne 0) { throw "swarm.key mode query failed" }
$modeA = ($modeALines -join "").Trim()
$modeB = ($modeBLines -join "").Trim()
if ($modeA -cne "400" -or $modeB -cne "400") { throw "swarm.key mode is not 0400" }
$digestALines = @(docker compose @compose exec -T kubo-a sha256sum /data/ipfs/swarm.key 2>&1)
$digestAExit = $LASTEXITCODE
$digestBLines = @(docker compose @compose exec -T kubo-b sha256sum /data/ipfs/swarm.key 2>&1)
$digestBExit = $LASTEXITCODE
if ($digestAExit -ne 0 -or $digestBExit -ne 0) { throw "swarm.key digest query failed" }
$digestAMatch = [regex]::Match(($digestALines -join "`n"), '^(?<digest>[0-9a-f]{64})\s')
$digestBMatch = [regex]::Match(($digestBLines -join "`n"), '^(?<digest>[0-9a-f]{64})\s')
if (-not $digestAMatch.Success -or -not $digestBMatch.Success -or
    $digestAMatch.Groups["digest"].Value -cne $digestBMatch.Groups["digest"].Value) {
    throw "A/B swarm.key content differs"
}
$digestALines = $null
$digestBLines = $null
$digestAMatch = $null
$digestBMatch = $null
"PRIVATE_SWARM_OUTER_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

- [ ] **Step 5: Insert private Rust gates before the existing five Cluster scenarios**

After all private pure support units, execute `private_swarm_configuration_and_peering` then `private_swarm_wrong_key_rejected`, each exact/serial/nocapture with immediate exit checks. Write `PRIVATE_SWARM_RUST_GREEN=true` only after both. Require that receipt before the current topology gate; retain topology GREEN before proxy GREEN, proxy GREEN before replication, and all current state-receipt ownership checks.

```yaml
      - name: Prove private Kubo configuration and persistent peerings
        shell: pwsh
        run: |
          if ($env:PRIVATE_SWARM_OUTER_GREEN -ne "true") { throw "Outer private-swarm receipt is required" }
          if ($env:PRIVATE_SWARM_DIRECT_LOG_GREEN -ne "true") { throw "Direct source-redacted log receipt is required" }
          cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact
          if ($LASTEXITCODE -ne 0) { throw "Private Kubo config unit contract failed" }
          cargo test --test cluster cluster_support::private_swarm_peers_json_contract_accepts_null_as_empty -- --exact
          if ($LASTEXITCODE -ne 0) { throw "Private Kubo null peer-list contract failed" }
          cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact
          if ($LASTEXITCODE -ne 0) { throw "Private Kubo Peering JSON contract failed" }
          cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "Private swarm configuration and peering failed" }

      - name: Prove wrong-key Kubo rejection
        shell: pwsh
        run: |
          cargo test --test cluster private_swarm_wrong_key_rejected -- --exact --nocapture --test-threads=1
          if ($LASTEXITCODE -ne 0) { throw "Wrong-key Kubo rejection failed" }
          "PRIVATE_SWARM_RUST_GREEN=true" | Add-Content -LiteralPath $env:GITHUB_ENV
```

- [ ] **Step 6: Replace peer-B-only restart orchestration with outage-then-full-A/B recovery**

Retain the existing sequence through B stop and `cluster_peer_b_outage_contract`. Then stop `cluster-a` and `kubo-a`, start only `kubo-a kubo-b`, wait until both are healthy, restart the completed `swarm-bootstrap` container, poll its exact state to `exited/0` with a 120-second bound, start `cluster-a cluster-b`, rerun `private_swarm_configuration_and_peering`, and finally run existing `cluster_peer_b_restart_recovery`. This proves both wrappers reinstall the supplied key, the one-shot bootstrap recreates/verifies peerings before Cluster restart, and existing recovery state survives the same volumes.

Use this exact recovery body after the existing outage test passes:

```powershell
$compose = @(
    "--project-name", $env:COMPOSE_PROJECT_NAME,
    "--profile", "private-swarm-validation",
    "-f", "docker-compose.cluster.yml",
    "-f", "tests/compose.cluster-validation.yml"
)
docker compose @compose stop cluster-a kubo-a
if ($LASTEXITCODE -ne 0) { throw "Peer A stop failed" }
docker compose @compose start kubo-a kubo-b
if ($LASTEXITCODE -ne 0) { throw "Kubo A/B restart failed" }
$healthDeadline = [DateTimeOffset]::UtcNow.AddSeconds(300)
foreach ($service in @("kubo-a", "kubo-b")) {
    $healthy = $false
    while ([DateTimeOffset]::UtcNow -lt $healthDeadline) {
        $ids = @(docker compose @compose ps --quiet $service | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($LASTEXITCODE -ne 0 -or $ids.Count -ne 1) { throw "Kubo restart identity query failed" }
        $health = (docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ids[0]).Trim()
        if ($LASTEXITCODE -ne 0) { throw "Kubo restart health query failed" }
        if ($health -ceq "healthy") { $healthy = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $healthy) { throw "Kubo restart health timeout" }
}
docker compose @compose start swarm-bootstrap
if ($LASTEXITCODE -ne 0) { throw "Bootstrap restart failed" }
$bootstrapDeadline = [DateTimeOffset]::UtcNow.AddSeconds(120)
$bootstrapGreen = $false
while ([DateTimeOffset]::UtcNow -lt $bootstrapDeadline) {
    $bootstrapIds = @(docker compose @compose ps --all --quiet swarm-bootstrap | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($LASTEXITCODE -ne 0 -or $bootstrapIds.Count -ne 1) { throw "Bootstrap recovery identity query failed" }
    $state = (docker inspect --format '{{.State.Status}}|{{.State.ExitCode}}' $bootstrapIds[0]).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Bootstrap recovery state query failed" }
    if ($state -ceq "exited|0") { $bootstrapGreen = $true; break }
    if ($state.StartsWith("exited|", [StringComparison]::Ordinal)) { throw "Bootstrap recovery exited nonzero" }
    Start-Sleep -Seconds 2
}
if (-not $bootstrapGreen) { throw "Bootstrap recovery timeout" }
docker compose @compose start cluster-a cluster-b
if ($LASTEXITCODE -ne 0) { throw "Cluster A/B restart failed" }
cargo test --test cluster private_swarm_configuration_and_peering -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Private swarm did not recover after A/B restart" }
if ($env:CLUSTER_STATE_RECEIPT_OWNED -ne "true") { throw "Owned state receipt is required before Cluster recovery" }
cargo test --test cluster cluster_peer_b_restart_recovery -- --exact --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Existing Cluster recovery contract failed" }
```

- [ ] **Step 7: Make diagnostics and cleanup fail closed for all new owned surfaces**

Every diagnostics call includes `postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway`, captures raw native output in memory, saves `$LASTEXITCODE`, applies the prefix-specific fingerprint defense plus existing key/PeerID defenses to each line, then emits. The earlier direct-log receipt remains mandatory and cannot be satisfied by this sanitizer. Cleanup remains `if: ${{ always() }}` and logs run first when attempted. It aggregates: project-scoped `down --volumes --remove-orphans`; container/network/volume label-query exits and residual counts (including proof-container residue); main/wrong key existence then deletion under independent ownership markers; state receipt deletion; and exact environment restoration checks. It throws once after attempting every cleanup. Production README remains forbidden from `--volumes`.

Use this exact cleanup body after the always-on sanitized diagnostics step. Missing-variable probes already restore exact process values in their own `finally`; cleanup owns project/file residues.

```powershell
$errors = [Collections.Generic.List[string]]::new()
$project = $env:COMPOSE_PROJECT_NAME
if ($env:CLUSTER_PINSET_OWNED -eq "true" -and $env:CLUSTER_PINSET_ATTEMPTED -eq "true") {
    docker compose --project-name $project --profile private-swarm-validation -f docker-compose.cluster.yml -f tests/compose.cluster-validation.yml down --volumes --remove-orphans
    $downExit = $LASTEXITCODE
    if ($downExit -ne 0) { $errors.Add("down exited $downExit") }
    $containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $containerExit = $LASTEXITCODE
    $networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $networkExit = $LASTEXITCODE
    $volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $volumeExit = $LASTEXITCODE
    if ($containerExit -ne 0) { $errors.Add("container residual query exited $containerExit") }
    if ($networkExit -ne 0) { $errors.Add("network residual query exited $networkExit") }
    if ($volumeExit -ne 0) { $errors.Add("volume residual query exited $volumeExit") }
    if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { $errors.Add("project-labelled residual resources remain") }
}
foreach ($ownedFile in @(
    [pscustomobject]@{ Marker = $env:SWARM_KEY_MAIN_OWNED; Path = $env:IPFS_S3_SWARM_KEY_FILE; Label = "main swarm key" },
    [pscustomobject]@{ Marker = $env:SWARM_KEY_WRONG_OWNED; Path = $env:IPFS_S3_SWARM_KEY_WRONG_FILE; Label = "wrong swarm key" },
    [pscustomobject]@{ Marker = $env:CLUSTER_STATE_RECEIPT_OWNED; Path = $env:IPFS_S3_CLUSTER_STATE_PATH; Label = "Cluster state receipt" }
)) {
    if ($ownedFile.Marker -ne "true") { continue }
    try {
        if (-not (Test-Path -LiteralPath $ownedFile.Path -PathType Leaf)) { throw "$($ownedFile.Label) is absent before cleanup" }
        Remove-Item -LiteralPath $ownedFile.Path -ErrorAction Stop
        if (Test-Path -LiteralPath $ownedFile.Path) { throw "$($ownedFile.Label) remains" }
    } catch {
        $errors.Add("$($ownedFile.Label) cleanup failed: $($_.Exception.Message)")
    }
}
if ($errors.Count -ne 0) { throw ($errors -join "; ") }
```

- [ ] **Step 8: Turn both workflow contracts GREEN and record hosted truthfully**

```powershell
$scripts = @(
    "tests/release-validation.Tests.ps1",
    "tests/cluster.Tests.ps1"
)
foreach ($script in $scripts) {
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        (Resolve-Path -LiteralPath $script),
        [ref]$tokens,
        [ref]$parseErrors
    ) | Out-Null
    if ($parseErrors.Count -ne 0) { throw "$script parse failed: $($parseErrors.Message -join '; ')" }
    pwsh -NoProfile -File $script
    if ($LASTEXITCODE -ne 0) { throw "Static workflow contract failed: $script" }
}
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Task 3 must not stage files" }
```

Expected GREEN: six unchanged independent/blocking jobs, one extended Cluster job, five unchanged static commands, five exact typed Kubo config lines with exactly one empty AddrFilters command and zero forbidden forms, normal peer-output exact-suffix parsing with no quiet/bare-ID form, twelve exact Cluster cargo invocations including the narrow peer-null unit, all embedded PowerShell AST valid, pure private-config AutoConf/AddrFilters and single-wrapper null-slice evidence before peer topology, pure-shell status precedence plus exact `37`/`43`/`143`/PID/FIFO/filter proofs and direct-log evidence ordered before bootstrap/Cluster evidence, exact private-before-Cluster causality, secret-safe logs/cleanup, and `HOSTED cluster-pinset-replication: NOT RUN`.

---

### Task 4: Execute one unique owned LOCAL workflow-parity run

**Files:**
- Exercise: all Task 1-3 runtime/static files
- Create temporarily outside repository: two random key files and the existing empty recovery receipt under the validated OS temp parent
- Delete before completion: both key files, recovery receipt, and every owned Compose resource
- Modify: none

**Interfaces:**
- Consumes: Task 1's exact typed Kubo config lines including `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]`; Task 1's normal `swarm peers` exact-suffix parser; Task 2's typed `KuboAutoConf`/`KuboSwarmConfig` protocol validator and narrow Kubo swarm-peer null-vector decoder; Task 3's exact Compose args, generated-key algorithm, pure-shell filter/status fixture, three-case stub-supervisor proof, prefix-specific redactor, receipt order, three private pure units, seven Rust test names, existing state receipt, and cleanup contract; all four historical blocked receipts below as RED evidence only.
- Produces: one fifth-project LOCAL matrix for typed-config/private-AutoConf/empty-AddrFilters startup, strict key creation, missing/invalid wrapper rejection, narrow source filter, status precedence, exact daemon `37`/handled-TERM `43`/unhandled-TERM `143`, FIFO absence/filter reap, direct real-Kubo source-redacted logs, eight-service validation startup, normal-output bootstrap completion, `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]` HTTP evidence before topology, Kubo C null peer-wrapper observation as empty, mode/digest memory checks, A/B/C private protocol state, existing five Cluster scenarios, full A/B restart, logs-first cleanup, residual zero, file deletion, and exact environment restoration; no hosted claim and no conversion/reuse of any historical blocked run, partial PASS, or diagnostic toggle into acceptance.

#### Recorded Task 4 Runtime RED — do not rewrite as PASS

The first real-Kubo topology attempt used unique project `ipfs3-ps-10938cc173b1e250` and stopped in `apply_private_config` before daemon/topology readiness. Preserve this exact receipt as historical RED evidence:

```text
LOCAL Task 4 topology: BLOCKED
REAL_KUBO_STEPS validate=0 mkdir=0 init=0 copy=0 chmod=0 config=1
KUBO_CONFIG_UNMARSHAL Discovery.MDNS.Enabled target=bool
KUBO_CONFIG_UNMARSHAL Gateway.HTTPHeaders.Cache-Control target=[]string
KUBO_CONFIG_UNMARSHAL Datastore.BloomFilterSize target=int
LOCAL blocked-run cleanup residual resources=0: PASS
LOCAL blocked-run environment restoration=19/19: PASS
```

The three unmarshal categories prove that untyped CLI values were decoded as strings against Kubo v0.43 typed fields. Cleanup and `19/19` restoration prove only safe teardown of the blocked run; they do not prove startup, topology, private peerings, wrong-key rejection, Cluster scenarios, restart recovery, or the current implementation identity.

#### Recorded Task 4 Runtime RED #2 — private AutoConf default-mainnet rejection

After the typed config commands passed static/source validation, the second fresh unique-project attempt reached daemon startup but Kubo v0.43 rejected the default mainnet AutoConf URL because a private-network key was active. Preserve this exact sanitized receipt permanently:

```text
LOCAL Task 4 fresh unique project attempt=2 topology: BLOCKED
REAL_KUBO_STEPS validate=0 mkdir=0 init=0 copy=0 chmod=0 config=0 daemon=1
KUBO_DAEMON_CATEGORY=PRIVATE_AUTOCONF_DEFAULT_MAINNET_REJECTED
KUBO_DAEMON_SANITIZED=AutoConf cannot use the default mainnet URL (https://conf.ipfs-mainnet.org/autoconf.json) on a private network (swarm.key or LIBP2P_FORCE_PNET detected). Either disable AutoConf by setting AutoConf.Enabled=false, or configure AutoConf.URL to point to a configuration service specific to your private swarm
LOCAL blocked-run-2 cleanup residual resources=0: PASS
LOCAL blocked-run-2 generated key/state files absent: PASS
LOCAL blocked-run-2 environment restoration=19/19: PASS
```

This receipt proves the typed config chain completed and the daemon then failed closed on private AutoConf validation. Its proof artifacts, cleanup, file absence, and environment restoration remain scoped to blocked attempt 2 and must not populate the current matrix.

#### Recorded Task 4 Runtime RED #3 — bootstrap CLI and server-profile dial filters

The third fresh unique-project attempt completed typed configuration and private AutoConf startup but `swarm-bootstrap` exited `1`. Preserve this runtime-confirmed, sanitized receipt permanently:

```text
LOCAL Task 4 fresh unique project attempt=3 topology: BLOCKED
REAL_KUBO_STEPS validate=0 mkdir=0 init=0 copy=0 chmod=0 config=0 daemon=0 bootstrap=1
BOOTSTRAP_EXIT=1
BOOTSTRAP_DEBUG swarm_peers_quiet_option=unsupported
BOOTSTRAP_DEBUG server_profile_rfc1918_filters=blocked_docker_bridge_before_pnet
BOOTSTRAP_DEBUG equal_keys_in_memory=true tcp_4001_listeners_a_b=true persistent_peering_a_b=true
BOOTSTRAP_TOGGLE clear_addr_filters_and_restart=true connect_a=0 connect_b=0 peers_a=1 peers_b=1
```

The diagnostic toggle isolates both blockers: Kubo v0.43 requires normal `swarm peers` output parsing, and the `server` profile's RFC1918 filters reject Docker bridge dials before PSK negotiation. Equal keys, listeners, and persistent Peering entries did not prove a connected swarm. Clearing AddrFilters and restarting made both connect commands exit `0` and both peer counts become `1`, but that debug result is not an acceptance PASS and cannot populate any later full-matrix line.

#### Recorded Task 4 Runtime RED #4 — Kubo nil peer slice serialized as null

Fresh project `ipfs3-ps-1d2e392face595b00066800d151d56c5` passed eight-service startup, direct fingerprint markers, bootstrap, mode/digest, and all three then-existing pure support units before the first private live Rust test failed. Preserve this exact current RED receipt:

```text
LOCAL Task 4 fresh unique project attempt=4 project=ipfs3-ps-1d2e392face595b00066800d151d56c5 topology: BLOCKED
LOCAL eight-service validation startup: PASS
LOCAL direct Kubo logs raw fingerprint=0 redacted markers>=3: PASS
LOCAL bootstrap exited 0 before Cluster evidence: PASS
LOCAL swarm.key mode 0400 and A/B digest equal in memory: PASS
LOCAL pure support units=3: PASS
RUST_TEST private_swarm_configuration_and_peering exit=101
RUST_CATEGORY=kubo_swarm_peers_malformed_json
KUBO_V0_43_SWARM_PEERS_WRAPPER={"Peers":null}
LOCAL blocked-run-4 cleanup residual resources=0: PASS
LOCAL blocked-run-4 environment restoration=19/19: PASS
```

Official Kubo v0.43 source emits one JSON object wrapper. Kubo C has no peers, so its nil Go slice serializes as `{"Peers":null}`. This is semantically the required empty peer set, not NDJSON and not malformed topology. The partial startup/bootstrap/mode/pure-unit receipts, cleanup `0`, and restoration `19/19` remain scoped to blocked attempt 4 and cannot populate the next full matrix.

- [ ] **Step 1: Claim one unique project, fixed ports, temp paths, and environment snapshot**

Run the fifth local parity attempt in one PowerShell 7 process with `$ErrorActionPreference="Stop"` and strict mode. Generate a new cryptographically random project name in this process; do not supply or reuse a literal project name from any blocked attempt. It must differ from `ipfs3-ps-10938cc173b1e250`, `ipfs3-ps-1d2e392face595b00066800d151d56c5`, and attempts 2 and 3's runtime identities, and the new identity must be recorded before `up`. No container, volume, key, state receipt, causal receipt, partial PASS, debug toggle, or Rust receipt from any blocked attempt may be reused. Snapshot exact presence/value for these 19 names before setting them: `COMPOSE_DISABLE_ENV_FILE`, `COMPOSE_PROJECT_NAME`, `POSTGRES_PASSWORD`, `IPFS_S3_ACCESS_KEY_ID`, `IPFS_S3_SECRET_ACCESS_KEY`, `IPFS_S3_MASTER_KEY`, `IPFS_S3_CLUSTER_SECRET`, `IPFS_S3_GATEWAY_BIND`, `IPFS_S3_GATEWAY_PORT`, six current Cluster endpoints, `IPFS_S3_CLUSTER_KUBO_C_URL`, `IPFS_S3_CLUSTER_STATE_PATH`, `IPFS_S3_SWARM_KEY_FILE`, and `IPFS_S3_SWARM_KEY_WRONG_FILE`. Validate the canonical OS temp parent exists, choose three unique child paths, query exact project labels across all container states/networks/volumes, and bind-probe `55435,55100,55101,55102,59100,59101,59102,59103`. Refuse any pre-existing project resource, listener, or file; do not kill or delete unowned state.

Use this exact initialization in that one process; retain `$savedEnvironment`, `$environmentNames`, `$projectOwned`, and the three file-ownership booleans through the outer `finally`:

```powershell
$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
$environmentNames = @(
    "COMPOSE_DISABLE_ENV_FILE",
    "COMPOSE_PROJECT_NAME",
    "POSTGRES_PASSWORD",
    "IPFS_S3_ACCESS_KEY_ID",
    "IPFS_S3_SECRET_ACCESS_KEY",
    "IPFS_S3_MASTER_KEY",
    "IPFS_S3_CLUSTER_SECRET",
    "IPFS_S3_GATEWAY_BIND",
    "IPFS_S3_GATEWAY_PORT",
    "IPFS_S3_CLUSTER_GATEWAY_ENDPOINT",
    "IPFS_S3_CLUSTER_A_REST_URL",
    "IPFS_S3_CLUSTER_B_REST_URL",
    "IPFS_S3_CLUSTER_A_PROXY_URL",
    "IPFS_S3_CLUSTER_KUBO_A_URL",
    "IPFS_S3_CLUSTER_KUBO_B_URL",
    "IPFS_S3_CLUSTER_KUBO_C_URL",
    "IPFS_S3_CLUSTER_STATE_PATH",
    "IPFS_S3_SWARM_KEY_FILE",
    "IPFS_S3_SWARM_KEY_WRONG_FILE"
)
$savedEnvironment = @{}
foreach ($name in $environmentNames) {
    $savedEnvironment[$name] = [pscustomobject]@{
        Present = Test-Path -LiteralPath "Env:$name"
        Value = [Environment]::GetEnvironmentVariable($name, "Process")
    }
}
function New-RandomHex {
    param([Parameter(Mandatory)][int]$ByteCount)
    $bytes = [byte[]]::new($ByteCount)
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    [Convert]::ToHexString($bytes).ToLowerInvariant()
}
$suffix = New-RandomHex -ByteCount 8
$tempParent = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
if (-not [IO.Directory]::Exists($tempParent)) { throw "OS temp parent is absent" }
$env:COMPOSE_DISABLE_ENV_FILE = "1"
$env:COMPOSE_PROJECT_NAME = "ipfs3-ps-$suffix"
$blockedProjectNames = @(
    "ipfs3-ps-10938cc173b1e250",
    "ipfs3-ps-1d2e392face595b00066800d151d56c5"
)
if ($blockedProjectNames -ccontains $env:COMPOSE_PROJECT_NAME) { throw "Fresh Task 4 project collided with a recorded blocked runtime project" }
$env:POSTGRES_PASSWORD = "cl-$suffix"
$env:IPFS_S3_ACCESS_KEY_ID = "test"
$env:IPFS_S3_SECRET_ACCESS_KEY = "test"
$env:IPFS_S3_MASTER_KEY = New-RandomHex -ByteCount 32
$env:IPFS_S3_CLUSTER_SECRET = New-RandomHex -ByteCount 32
$env:IPFS_S3_GATEWAY_BIND = "127.0.0.1"
$env:IPFS_S3_GATEWAY_PORT = "59100"
$env:IPFS_S3_CLUSTER_GATEWAY_ENDPOINT = "http://127.0.0.1:59100"
$env:IPFS_S3_CLUSTER_A_REST_URL = "http://127.0.0.1:59101"
$env:IPFS_S3_CLUSTER_B_REST_URL = "http://127.0.0.1:59102"
$env:IPFS_S3_CLUSTER_A_PROXY_URL = "http://127.0.0.1:59103"
$env:IPFS_S3_CLUSTER_KUBO_A_URL = "http://127.0.0.1:55100"
$env:IPFS_S3_CLUSTER_KUBO_B_URL = "http://127.0.0.1:55101"
$env:IPFS_S3_CLUSTER_KUBO_C_URL = "http://127.0.0.1:55102"
$env:IPFS_S3_CLUSTER_STATE_PATH = Join-Path $tempParent "ipfs3-cluster-$suffix.json"
$env:IPFS_S3_SWARM_KEY_FILE = Join-Path $tempParent "ipfs3-swarm-$suffix.key"
$env:IPFS_S3_SWARM_KEY_WRONG_FILE = Join-Path $tempParent "ipfs3-swarm-wrong-$suffix.key"
$projectOwned = $false
$projectAttempted = $false
$mainKeyOwned = $false
$wrongKeyOwned = $false
$stateReceiptOwned = $false
$errors = [Collections.Generic.List[string]]::new()
$project = $env:COMPOSE_PROJECT_NAME
$containers = @(docker ps --all --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$containerExit = $LASTEXITCODE
$networks = @(docker network ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$networkExit = $LASTEXITCODE
$volumes = @(docker volume ls --quiet --filter "label=com.docker.compose.project=$project" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
$volumeExit = $LASTEXITCODE
if ($containerExit -ne 0 -or $networkExit -ne 0 -or $volumeExit -ne 0) { throw "Project ownership preflight query failed" }
if (($containers.Count + $networks.Count + $volumes.Count) -ne 0) { throw "Unique project already owns resources" }
foreach ($port in @(55435, 55100, 55101, 55102, 59100, 59101, 59102, 59103)) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
    try { $listener.Start() } catch { throw "Fixed validation port is occupied: $port" } finally { $listener.Stop() }
}
foreach ($path in @($env:IPFS_S3_CLUSTER_STATE_PATH, $env:IPFS_S3_SWARM_KEY_FILE, $env:IPFS_S3_SWARM_KEY_WRONG_FILE)) {
    if ([IO.Path]::GetFullPath([IO.Path]::GetDirectoryName($path)) -cne $tempParent) { throw "Owned file parent is not OS temp" }
    if (Test-Path -LiteralPath $path) { throw "Owned temp path already exists" }
}
$projectOwned = $true
```

- [ ] **Step 2: Create and validate main/wrong key files plus the empty recovery receipt**

Use Task 3's `New-PrivateSwarmKeyFile` twice with `FileMode.CreateNew`; set independent local ownership booleans only after each flush. Create the recovery receipt with `CreateNew`, zero length, and its own ownership boolean. Strict-decode both key files, require exact 96-byte regex and inequality, then discard in-memory key text. Do not write a digest or key to the console/transcript.

- [ ] **Step 3: Run static/config/build, wrapper negatives, and filter/supervisor proofs**

Run the five static contracts in release → PostgreSQL → multi-gateway → Cluster → client order and require the exact typed-config contract to reject all prior untyped/missing forms, require exactly one empty AddrFilters command, and reject nonempty AddrFilters. Require the bootstrap source contract to reject `swarm peers -q` and bare-ID equality while locking normal-output one-line exact `/p2p/$expected_id` suffix parsing plus the unchanged two-line Peering parser. Run the private config, swarm-peer null-wrapper, and Peering pure units; require null/missing/empty to decode empty, nonempty arrays unchanged, malformed nonarrays rejected, exactly one helper annotation on `KuboSwarmPeers`, no Peering broadening, and no NDJSON path. Source the current entrypoint under `/bin/sh` and require syntax/filter/status fixture GREEN. Run Compose `config --quiet` with the validation profile, build `kubo-a`, set the attempted marker immediately before the first project-owned `compose run`, and execute the missing-source and force-PNET-invalid probes from Task 3. Require fixed sanitized failures and no daemon readiness. Then run Task 3's exact source-based pure-shell filter/status fixture and three exact-label stub-daemon containers: preserve normal daemon `37`; forward `TERM` and preserve handled daemon `43`; forward `TERM`, re-wait after the interrupted wait, and preserve unhandled daemon `143`; capture every log; prove raw prefixed fingerprint absence and fixed marker/ordinary-32-hex preservation; bound all stops; prove final PID `0` and FIFO absence; and remove all proof containers only after exact ownership-label checks. These current-run receipts supersede nothing until Step 8 records the complete fifth-project matrix.

- [ ] **Step 4: Start all seven production services plus optional C and prove outer-only state**

Start `postgres kubo-a kubo-b kubo-c swarm-bootstrap cluster-a cluster-b gateway` with `--detach --build --wait --wait-timeout 300`. Treat any repeated typed-config error, `PRIVATE_AUTOCONF_DEFAULT_MAINNET_REJECTED`, nonempty/missing AddrFilters, unsupported `swarm peers -q`, bootstrap nonzero, or missing healthy service as current-run RED. Before topology assertions, run all three private pure units, then require the live Kubo config probes for A and B to decode explicit `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]`; require C's single `{"Peers":null}` wrapper to decode as the empty peer set without `kubo_swarm_peers_malformed_json`. Missing/malformed config is terminal decode failure, true/nonempty config is `private_kubo_config_open`, and a nonnull/nonarray peer wrapper remains malformed JSON. Before workflow diagnostic sanitization, directly capture Kubo A/B/C container logs, reject every raw `Swarm key fingerprint: <32hex>`, require at least three `Swarm key fingerprint: [redacted]` markers, and clear the capture. Inspect bootstrap by exact project/service labels, require `exited/0`, and only then permit peer/Peering topology evidence. Bootstrap success must come from normal full-multiaddress peer output with exactly one nonempty A/B line and exact other-ID suffix, never from attempt 3's toggle. Capture A/B `swarm.key` mode and SHA-256 output into PowerShell variables; require `400`/`400` and equality, never emit either digest, and clear all digest variables. Confirm production Compose still has only gateway publication and validation mappings are loopback-only through the static/rendered-quiet contracts rather than rendering secret-bearing config.

- [ ] **Step 5: Run private-swarm protocol gates and the first three existing Cluster scenarios**

Run in this exact order with immediate exit checks: pure private-config unit; pure swarm-peer null-wrapper unit; pure private-Peering JSON unit; existing release-version unit; `private_swarm_configuration_and_peering`; `private_swarm_wrong_key_rejected`; `cluster_topology_converges`; `cluster_proxy_compatibility`; `cluster_replication_and_retention`. Require each causal receipt before its consumer. Preserve current output categories only; no ID, CID, request body, key, or digest is printed.

- [ ] **Step 6: Prove existing outage, then full A/B private-swarm restart recovery**

Capture sanitized logs, stop `cluster-b kubo-b`, and run `cluster_peer_b_outage_contract`. Capture sanitized stopped-state logs. Stop `cluster-a kubo-a`; start Kubo A/B and wait healthy; restart `swarm-bootstrap` and poll exact exit `0`; start Cluster A/B and wait healthy; rerun `private_swarm_configuration_and_peering`; then run `cluster_peer_b_restart_recovery`. This one sequence satisfies both the existing five-scenario contract and the approved A/B-with-existing-volumes private-swarm recovery contract.

- [ ] **Step 7: In an outer `finally`, sanitize logs before owned cleanup and restore every environment value**

Regardless of primary failure, if attempted, capture/sanitize all eight service logs first. If and only if project ownership and attempted markers are true, run project-scoped `down --volumes --remove-orphans`, independently capture down/container/network/volume query exits, and require zero exact-label residuals. Independently delete each key/state file only under its own ownership boolean and require absence. Then restore all 19 environment entries: prior presence uses `SetEnvironmentVariable(...,"Process")` with exact case-sensitive comparison; prior absence uses `Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop` and verifies absence. Aggregate primary, diagnostics, Docker cleanup, file cleanup, and restoration errors and throw once after every attempt.

The final restoration loop is exact and runs after all Docker/file cleanup attempts, even when those attempts failed:

```powershell
foreach ($name in $environmentNames) {
    try {
        $saved = $savedEnvironment[$name]
        if ($saved.Present) {
            [Environment]::SetEnvironmentVariable($name, $saved.Value, "Process")
            if (-not (Test-Path -LiteralPath "Env:$name")) { throw "Environment presence was not restored" }
            if ([Environment]::GetEnvironmentVariable($name, "Process") -cne $saved.Value) { throw "Environment value was not restored" }
        } else {
            if (Test-Path -LiteralPath "Env:$name") {
                Remove-Item -LiteralPath "Env:$name" -ErrorAction Stop
            }
            if (Test-Path -LiteralPath "Env:$name") { throw "Environment absence was not restored" }
            if ($null -ne [Environment]::GetEnvironmentVariable($name, "Process")) { throw "Environment process value remains" }
        }
    } catch {
        $errors.Add("environment restoration failed for $name")
    }
}
if ($errors.Count -ne 0) { throw ($errors -join "; ") }
```

- [ ] **Step 8: Record the complete LOCAL matrix without converting missing evidence to PASS**

Record all lines below only from the fifth fresh full run's actual receipts. All four historical `BLOCKED` receipts remain verbatim above. Previously passing, partial, or diagnostic-toggle output may be attached as supplementary evidence only after this fifth full matrix independently passes; it may not fill, skip, or turn any current line into PASS.

```text
LOCAL static contracts: PASS
LOCAL Kubo v0.43 typed config/source syntax: PASS
LOCAL Kubo AutoConf.Enabled=false decoded before topology: PASS
LOCAL Kubo Swarm.AddrFilters=[] decoded before topology: PASS
LOCAL Kubo peer wrapper null/missing=empty, array unchanged, malformed rejected: PASS
LOCAL key files strict/different/CreateNew: PASS
LOCAL wrapper missing-source/force-PNET rejection: PASS
LOCAL source filter ordinary/fingerprint fixture: PASS
LOCAL PID1 daemon exit=37, TERM-handled exit=43, TERM-unhandled exit=143: PASS
LOCAL supervisor FIFO absent and filter reaped after all three exits: PASS
LOCAL seven-service production + validation-only C startup: PASS
LOCAL direct Kubo logs raw fingerprint=0 redacted markers>=3: PASS
LOCAL bootstrap exited 0 before Cluster evidence: PASS
LOCAL bootstrap normal swarm peers one-line exact-suffix parsing: PASS
LOCAL swarm.key mode 0400 and A/B digest equal in memory: PASS
LOCAL A={B}, B={A}, C={} and persistent peerings: PASS
LOCAL wrong-key connect rejection: PASS
LOCAL existing Cluster topology/proxy/replication-retention/outage/recovery: PASS
LOCAL full A/B same-volume private-swarm recovery: PASS
LOCAL logs-first sanitized cleanup and residual resources=0: PASS
LOCAL generated key/state files deleted: PASS
LOCAL environment restoration=19/19: PASS
HOSTED cluster-pinset-replication: NOT RUN
```

Any repeated typed-config unmarshal error, private AutoConf default-mainnet rejection, absent/malformed/true AutoConf, absent/malformed/nonempty AddrFilters, unsupported quiet peer command, bare-ID/suffix ambiguity, `kubo_swarm_peers_malformed_json` for null/missing, accepted nonarray peer wrapper, NDJSON decoder, unavailable Docker, occupied port, wrapper ambiguity, bootstrap nonzero, skipped Rust test, mode/digest uncertainty, cleanup/query/file deletion error, residual, or restoration below `19/19` is `UNVERIFIED`/FAIL and blocks Task 5. All four historical attempts remain `BLOCKED`, never PASS; no prior partial PASS or diagnostic toggle is reusable acceptance evidence.

---

### Task 5: Update README and only the private-swarm ROADMAP item after LOCAL GREEN

**Files:**
- Modify: `README.md`
- Modify: `ROADMAP.md`
- Test: `tests/cluster.Tests.ps1`
- Verify unchanged: every other ROADMAP checkbox and every non-Cluster README section

**Interfaces:**
- Consumes: the fifth-project Task 4 LOCAL matrix with every current line PASS and hosted `NOT RUN`; all four historical blocked attempts, partial receipts, and diagnostic toggles are insufficient; approved same-host/security/non-goal language; production secret-file interface; non-destructive shutdown contract.
- Produces: one updated `### IPFS Cluster pinset replication` section describing exact seven-service private swarm and safe key-file operation; exactly one ROADMAP checkbox change; static documentation receipt.

- [ ] **Step 1: Strengthen documentation static expectations and capture RED**

Extend only the existing README/ROADMAP assertions in `tests/cluster.Tests.ps1`. Require: seven roles and one-shot bootstrap; same shared PSK distinct from Cluster secret; exact strict file contract; `IPFS_S3_SWARM_KEY_FILE` path interface; no raw key environment value; private A/B sole-peer/persistent-peering evidence; wrong-key C is validation-only; same-host membership-only claim; no egress isolation/encryption/HA/multi-host/rotation; hosted NOT RUN; and production `down --remove-orphans` without `--volumes`. Require only the private-swarm ROADMAP line to change from unchecked to checked.

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
$docsRed = $LASTEXITCODE
if ($docsRed -eq 0) { throw "Documentation contract unexpectedly passed before README/ROADMAP updates" }
```

Expected RED: missing bounded private-swarm documentation or unchecked private-swarm item.

- [ ] **Step 2: Update the existing Cluster README section without exposing secret material**

Revise `README.md` in place. Keep current Cluster, proxy, replication, deletion, and outage statements; change six roles to seven and mDNS-only to explicit private A/B peering. Document creation of a strict key file using `RandomNumberGenerator`, UTF-8 no BOM, LF, and `CreateNew` without ever printing the key. Set only the path through `$env:IPFS_S3_SWARM_KEY_FILE`; explain that `CLUSTER_SECRET` is separate, Kubo C/`55102` are validation-only, key rotation requires coordinated downtime and is not automated, and the PSK does not provide egress isolation, transport encryption, API auth, multi-host discovery, or HA. Keep hosted status `NOT RUN` and the exact non-destructive production shutdown command.

- [ ] **Step 3: Check exactly one ROADMAP item**

Change only:

```markdown
- [x] Private swarm (swarm.key) for node-to-node communication
```

Do not modify package version, release assignment, neighboring checkboxes, or any other roadmap text.

- [ ] **Step 4: Turn docs/static GREEN and preserve the evidence gate**

```powershell
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Private-swarm documentation contract failed" }
git diff -- README.md ROADMAP.md
if ($LASTEXITCODE -ne 0) { throw "Documentation diff query failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Task 5 must not stage files" }
```

Expected GREEN: README states only the approved bounded support, ROADMAP has only the private-swarm checkbox change, hosted remains NOT RUN, and production docs contain no `down --volumes`, key value, digest, PeerID, or wrong-key material.

---

### Task 6: Run full verification, freeze identity, and hand off Oracle/Reviewer and commit

**Files:**
- Verify: all exact final-manifest paths below
- Modify: none after identity generation; any edit restarts affected verification and invalidates review receipts

**Interfaces:**
- Consumes: Tasks 1-5 receipts; complete fifth-project Task 4 LOCAL matrix including AutoConf-disabled/AddrFilters-empty config observation, Kubo C null peer-wrapper acceptance, normal peer-output bootstrap, filter/status fixture, exact `37`/`43`/`143` supervisor exits, FIFO absence/filter reap, direct real-Kubo fingerprint assertion, and Peering JSON fixtures; all four historical `BLOCKED` matrices are attached only as RED context; approved runtime-revised spec SHA; base HEAD `ad377bff760b6611496d46a6fc7fdae7aa2f6281` unless the orchestrator records an authorized base change; LSP diagnostics; orchestrator-owned Oracle and Reviewer profiles.
- Produces: static/two-private-unit/Rust/lib/integration/check/fmt/Clippy/LSP/diff/boundary receipts; exact 13-path SHA manifest; one identity-bound Oracle receipt; one identity-bound Reviewer receipt; orchestrator-only focused commit handoff; no push/tag; hosted still NOT RUN.

- [ ] **Step 1: Parse and run all five static contracts in production order**

```powershell
$scripts = @(
    "tests/release-validation.Tests.ps1",
    "tests/postgres-production-baseline.Tests.ps1",
    "tests/multi-gateway.Tests.ps1",
    "tests/cluster.Tests.ps1",
    "tests/client-smoke.Tests.ps1"
)
foreach ($script in $scripts) {
    $tokens = $null
    $parseErrors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        (Resolve-Path -LiteralPath $script),
        [ref]$tokens,
        [ref]$parseErrors
    ) | Out-Null
    if ($parseErrors.Count -ne 0) { throw "$script parse failed: $($parseErrors.Message -join '; ')" }
    pwsh -NoProfile -File $script
    if ($LASTEXITCODE -ne 0) { throw "Static contract failed: $script" }
}
```

Expected: five PASS lines in exact order; Cluster static also parses every embedded workflow PowerShell block and protects current default/single-PG/multi-gateway/shared-entrypoint hashes.

- [ ] **Step 2: Run complete non-Docker Rust and quality gates**

Task 4's fifth-project matrix is the only live Docker run that can satisfy acceptance; all four recorded blocked attempts, attempt 3's diagnostic toggle, and attempt 4's partial PASS lines are historical RED context and do not count. With no relevant edits after the fifth-project Task 4 PASS:

```powershell
cargo test --test cluster cluster_support::release_version_validator_accepts_exact_release_and_build_metadata -- --exact
if ($LASTEXITCODE -ne 0) { throw "Cluster release unit failed" }
cargo test --test cluster cluster_support::private_kubo_config_contract_rejects_open_discovery -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo config unit failed" }
cargo test --test cluster cluster_support::private_swarm_peers_json_contract_accepts_null_as_empty -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo null peer-list unit failed" }
cargo test --test cluster cluster_support::private_peering_json_contract_matches_kubo_v0_43_addrinfo -- --exact
if ($LASTEXITCODE -ne 0) { throw "Private Kubo Peering JSON unit failed" }
cargo test --bin ipfs-s3-gateway
if ($LASTEXITCODE -ne 0) { throw "Binary tests failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "Library tests failed" }
cargo test --test integration
if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
cargo test --test cluster --no-run
if ($LASTEXITCODE -ne 0) { throw "Cluster target compile failed" }
cargo check --all-targets
if ($LASTEXITCODE -ne 0) { throw "cargo check failed" }
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { throw "cargo fmt check failed" }
cargo clippy --all-targets -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "Clippy failed" }
```

Expected: all exit zero. If a source, Compose, workflow, wrapper, or live-input edit occurred after Task 4, rerun the affected Task 4 evidence before continuing.

- [ ] **Step 3: Require clean LSP diagnostics for both changed Rust files**

Use `lsp_diagnostics` with severity `all` on `tests/support/cluster.rs` and `tests/cluster.rs`. Expected: zero errors and zero warnings. LSP unavailability is `UNVERIFIED`; compile/Clippy is not a substitute for this receipt.

- [ ] **Step 4: Verify spec SHA, whitespace, clean index, and exact 13-path boundary**

```powershell
$specSha = (Get-FileHash -Algorithm SHA256 -LiteralPath "docs/superpowers/specs/2026-08-24-private-swarm-design.md").Hash.ToLowerInvariant()
        if ($specSha -cne "a55d6b0064fc51ed55b909dcf4b19bfd5f14230e3e9537fcc22b48030a0b921f") { throw "Runtime-revised private-swarm spec changed" }
git diff --check
if ($LASTEXITCODE -ne 0) { throw "git diff --check failed" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Implementation workers must not stage files" }
$allowed = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.cluster.yml",
    "docs/superpowers/plans/2026-08-24-private-swarm.md",
    "docs/superpowers/specs/2026-08-24-private-swarm-design.md",
    "ipfs/cluster.Dockerfile",
    "ipfs/private-swarm-entrypoint.sh",
    "tests/cluster.Tests.ps1",
    "tests/cluster.rs",
    "tests/compose.cluster-validation.yml",
    "tests/release-validation.Tests.ps1",
    "tests/support/cluster.rs"
)
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Tracked path query failed" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Untracked path query failed" }
$changed = @($tracked + $untracked | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
$unexpected = @($changed | Where-Object { $allowed -cnotcontains $_ })
if ($unexpected.Count -ne 0) { throw "Out-of-scope changed paths: $($unexpected -join ', ')" }
foreach ($required in $allowed) {
    if ($changed -cnotcontains $required) { throw "Final manifest path is absent: $required" }
}
git status --short
if ($LASTEXITCODE -ne 0) { throw "Git status query failed" }
```

Expected: exact approved spec, whitespace clean, no staged files, and all and only the 13 approved manifest paths changed/untracked. Any application/Cargo/default/single-PG/multi-gateway/shared-entrypoint path blocks review.

- [ ] **Step 5: Freeze one identity from HEAD and every approved path SHA**

```powershell
$head = (git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw "HEAD query failed" }
$manifest = @(
    ".github/workflows/release-validation.yml",
    "README.md",
    "ROADMAP.md",
    "docker-compose.cluster.yml",
    "docs/superpowers/plans/2026-08-24-private-swarm.md",
    "docs/superpowers/specs/2026-08-24-private-swarm-design.md",
    "ipfs/cluster.Dockerfile",
    "ipfs/private-swarm-entrypoint.sh",
    "tests/cluster.Tests.ps1",
    "tests/cluster.rs",
    "tests/compose.cluster-validation.yml",
    "tests/release-validation.Tests.ps1",
    "tests/support/cluster.rs"
)
$rows = @($manifest | Sort-Object | ForEach-Object {
    if (-not (Test-Path -LiteralPath $_ -PathType Leaf)) { throw "Manifest file missing: $_" }
    $sha = (Get-FileHash -Algorithm SHA256 -LiteralPath $_).Hash.ToLowerInvariant()
    "$sha  $_"
})
$identityText = "HEAD $head`n" + ($rows -join "`n") + "`n"
$identityBytes = [Text.Encoding]::UTF8.GetBytes($identityText)
$identitySha = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($identityBytes)).ToLowerInvariant()
Write-Host "REVIEW_IDENTITY_SHA256 $identitySha"
Write-Host $identityText
```

Any path content or HEAD change invalidates both implementation-review receipts and requires affected verification plus a new identity.

- [ ] **Step 6: Hand the same complete identity to Oracle and Reviewer**

The orchestrator, not an implementation worker, dispatches one Oracle and one Reviewer. Both receive exact `REVIEW_IDENTITY_SHA256`, HEAD, all 13 SHA rows, approved spec/plan SHA, complete LOCAL matrix, `HOSTED cluster-pinset-replication: NOT RUN`, all static/Rust/quality/LSP/diff/boundary receipts, and explicit non-goals. Each verdict must state that it applies to that exact identity. Timeout, partial output, an older identity, or pre-final-edit approval is not a receipt.

- [ ] **Step 7: Recompute identity, then allow only the orchestrator's authorized focused commit**

After both identity-bound approvals, rerun Step 5 and require the exact same identity. Implementation workers still perform no Git write. The user has authorized the orchestrator to inspect `git status`, `git diff`, `git log --oneline -10`, stage exactly the 13 manifest paths, inspect the staged path set/diff, and create one semantic commit `feat: add private IPFS swarm` with a brief body describing the Cluster-only PSK wrapper, deterministic peering bootstrap, wrong-key evidence, and fail-closed validation. Do not push, tag, amend, force, or include any other path.

- [ ] **Step 8: Return final evidence without overstating hosted status**

Return task/step completion counts, exact manifest, spec/plan SHA-256, implementation review identity, both review receipts, Task 4 LOCAL matrix, static/Rust/LSP/diff results, pre/post-commit Git status boundary, and commit hash if the orchestrator committed. Keep `HOSTED cluster-pinset-replication: NOT RUN`; a real hosted result requires a later authorized push/PR and is outside this implementation run.

---

## Verification Waves and Acceptance Boundary

1. **Wave 1 — Cluster image/Compose static TDD:** entrypoint/secret/seven-service/bootstrap/C and typed-config assertions RED; lock exact MDNS `--json false`, AutoConf `--json false`, AddrFilters `--json '[]'` exactly once, Cache-Control `--json '["public, max-age=29030400, immutable"]'`, and BloomFilterSize `--json 0` while rejecting missing/nonempty/non-exact forms; reject `swarm peers -q` and bare-ID equality; then require source syntax, strict wrapper, exact prefix-only 32-hex filter, FIFO/PID-1 lifecycle, normal peer-output one-line exact-ID-suffix parser, unchanged real two-line CLI Peering parser, Cluster Dockerfile, required secrets, exact causality, protected profiles/hashes, and loopback surfaces GREEN.
2. **Wave 2 — Rust protocol TDD:** current `kubo_swarm_peers_malformed_json` RED for Kubo C `{"Peers":null}`; add exactly one `deserialize_null_vec_as_empty` serde use on `KuboSwarmPeers.peers`; prove null/missing/empty become empty, nonempty arrays remain unchanged, malformed nonnull/nonarrays fail, no NDJSON decoder exists, and `KuboPeeringPeers` remains unchanged. Retain nonoptional config DTOs, five real `Peers[{ID,Addrs}]` Peering fixtures, closed-config validation before topology, two private live tests, existing five tests, and compile/static GREEN.
3. **Wave 3 — workflow static TDD:** missing peer-null support-unit command/static contract RED; same blocking job, exact generated-file lifecycle, five typed config commands, empty AddrFilters and normal peer-output parser contract, exactly twelve Cluster-target cargo invocations including four support units, pure-shell source/status fixture, exact stub exits `37`/`43`/`143`, direct raw-fingerprint-zero receipt, wrapper/process/filesystem ownership, A/B restart order, cleanup/restoration, and embedded PowerShell AST GREEN.
4. **Wave 4 — four recorded RED attempts then a fifth full LOCAL live gate:** retain attempts 1-3 exactly as recorded; retain project `ipfs3-ps-1d2e392face595b00066800d151d56c5` as `BLOCKED` after eight-service/direct-marker/bootstrap/mode/pure3 PASS because `private_swarm_configuration_and_peering` exited `101` with `kubo_swarm_peers_malformed_json` on Kubo C's `{"Peers":null}`; retain its cleanup residual `0` and environment restoration `19/19` only as blocked-run evidence. After the narrow DTO/fixture/static fix, generate a fifth unique project and rerun static/source/all pure units, strict random keys, wrapper negatives, `37`/`43`/`143` supervisor proofs, seven services plus C, A/B closed config, direct logs, bootstrap, mode/digest, null peer-wrapper decoding, full private tests, existing five Cluster scenarios, full A/B recovery, logs-first cleanup, residual zero, generated-file deletion, and restoration `19/19`; only this complete fifth matrix may PASS.
5. **Wave 5 — evidence-gated docs:** README bounded same-host membership/security contract and exactly one private-swarm ROADMAP check after Wave 4; hosted remains NOT RUN.
6. **Wave 6 — final acceptance:** five static contracts, Rust units/bin/lib/integration/cluster compile/check/fmt/Clippy, LSP, diff, spec SHA, exact 13-path boundary/identity, identity-bound Oracle+Reviewer, then orchestrator-only authorized commit.

No LOCAL line may be inferred. No historical blocked cleanup/restoration/debug/partial-PASS receipt satisfies the fifth matrix. Docker/runtime unavailability, `kubo_swarm_peers_malformed_json` for null/missing, acceptance of a malformed nonarray, an NDJSON decoder, Peering DTO broadening without evidence, any earlier config/bootstrap blocker, cleanup uncertainty, a stale or mixed-project Task 4 receipt after relevant edits, an LSP gap, any extra changed path, or missing current-identity review receipt blocks commit.

## Requirement-to-Task Coverage

| Requirement | Coverage |
| --- | --- |
| Cluster-only strict key wrapper, fixed error, closed Kubo config including `AutoConf.Enabled=false` and `Swarm.AddrFilters=[]`, mode `0400`, exact fingerprint source filter, PID-1 signal/exit/cleanup, shared entrypoint protected | Task 1; Task 2 typed config DTO/fixtures; Task 3 filter/supervisor/direct-log assertions; Task 4 live |
| Required Compose secret, exact seven production services, no extra production publication | Task 1; Tasks 3-4 static/rendered-quiet/live |
| One-shot bounded A/B ID discovery, normal `swarm peers` one-line exact-ID-suffix parsing without `-q`/bare equality, real two-line CLI persistent-Peering parsing, bidirectional connect, sole-peer verification before Cluster | Task 1; Task 3 causality; Task 4 live |
| Validation-only wrong-key Kubo C at loopback `55102`, no production counterpart | Task 1; Task 2; Tasks 3-4 |
| Rust observes required config plus Kubo swarm peers/Peering/connect only; the single swarm-peer wrapper treats null/missing `Peers` as empty while preserving arrays and rejecting nonarrays; no NDJSON or unsupported Peering coercion; filesystem/process remain PowerShell-owned | Task 2 DTO/unit/static guards; Task 3 command contract; Task 4 live order |
| Wrong-key rejection with A={B}, B={A}, C={}, no key/digest/ID disclosure | Tasks 2-4 |
| Existing topology/proxy/replication/delete-retention/outage/recovery all retained | Tasks 2-4 |
| A/B same-volume wrapper reinstall, bootstrap rerun, peering/Cluster recovery | Tasks 3-4 |
| Existing Cluster CI job only, key generation/CreateNew, no `.env`, exact causal receipts, fail-closed cleanup/restoration | Task 3; Task 4 parity |
| README/ROADMAP only after complete LOCAL PASS; same-host and security limits; hosted NOT RUN | Task 5 |
| Cargo/application/default/single-PG/multi/shared-entrypoint protected; no provider/egress/multi-host/HA/rotation scope | Global Constraints; Tasks 1, 3, 6 |
| Full static/Rust/quality/LSP/diff, exact spec+plan manifest, Oracle+Reviewer identity, no push/tag | Task 6 |

## Risks and Assumptions

- Kubo v0.43.0 official `core/commands/swarm.go` writes each `swarm peering ls` entry as `ID\n`, then each transport address as `\tADDRESS\n`; Task 1 accepts exactly one expected ID line plus one exact trimmed `/dns4/kubo-{other}/tcp/4001` line. CLI output remains captured and never logged. A runtime mismatch fails closed and may be fixed only inside this frozen two-line interface with a sanitized fixture.
- Kubo v0.43.0 `swarm peers` has no `-q` option and writes full peer multiaddresses. Task 1 captures normal output, removes empty/whitespace-only lines, requires exactly one line, and accepts it only when its suffix is exactly `/p2p/$expected_id`; it never prints the captured line or compares the whole output to a bare ID. This peer parser is distinct from the frozen two-line `swarm peering ls` parser above.
- Kubo v0.43.0's `server` init profile installs RFC1918 address filters, including Docker bridge ranges, and those filters reject same-host bridge dials before pnet negotiation. The Cluster-only wrapper applies exact `ipfs config Swarm.AddrFilters --json '[]'` on every start. This is limited to an internal-only Swarm port protected by the PSK; it does not isolate egress or claim that outbound traffic is blocked.
- Kubo v0.43.0's REST representation is pinned as `Peers[{ID,Addrs}]`, where `ID` carries identity and `Addrs` contains transport addresses without `/p2p/<id>`. Five pure fixtures cover valid, extra-peer, extra-address, wrong-ID, and wrong-address cases. No raw response body is logged and no application code changes.
- Kubo v0.43.0's `/api/v0/swarm/peers` command emits one JSON object wrapper. A nil Go peer slice serializes as `{"Peers":null}`, observed on wrong-key Kubo C with no peers. Exactly one serde field, `KuboSwarmPeers.peers`, uses `deserialize_null_vec_as_empty`; missing and null become empty, arrays retain their contents, and nonnull/nonarrays still fail. This is not NDJSON. `KuboPeeringPeers` remains unchanged absent separate official or runtime evidence.
- Kubo v0.43.0 official `cmd/ipfs/kubo/daemon.go` unconditionally formats the private-network line as `Swarm key fingerprint: %x`. The wrapper recognizes only the exact prefix plus 32 lowercase hex, replaces it at the FIFO source, and preserves ordinary 32-hex/CID lines. Pure-shell, stub-exit/signal, and direct real-log receipts jointly prevent workflow post-redaction from being mistaken for the primary barrier.
- The first real topology run established a concrete Kubo v0.43 typed-config failure, not an environmental unknown: `REAL_KUBO_STEPS validate=0 mkdir=0 init=0 copy=0 chmod=0 config=1`, followed by string-to-`bool`, string-to-`[]string`, and string-to-`int` unmarshal failures for `Discovery.MDNS.Enabled`, `Gateway.HTTPHeaders.Cache-Control`, and `Datastore.BloomFilterSize`. The typed source corrections are the three exact commands already recorded in Task 1; the additional exact `ipfs config AutoConf.Enabled --json false` command closes the distinct second runtime blocker required by the revised spec.
- The second fresh attempt passed typed configuration but failed at daemon startup with `config=0 daemon=1`, category `PRIVATE_AUTOCONF_DEFAULT_MAINNET_REJECTED`, and Kubo's exact sanitized default-mainnet/private-network rejection. Task 1 disables AutoConf fail-closed; Task 2 requires a nonoptional/nondefault `bool`, rejects true/missing/malformed fixtures, and checks A/B false before topology. Any recurrence is current-run RED, not a retriable environmental condition.
- The third fresh attempt reached healthy private daemons but `swarm-bootstrap` exited `1`: debug confirmed unsupported `swarm peers -q`, active server-profile RFC1918 filters blocking Docker bridge dials before pnet, equal keys in memory, both TCP 4001 listeners, and both persistent Peering entries. Clearing filters plus restarting yielded connect exits `0/0` and peer counts `1/1`; this toggle isolates the blocker but is not a full-matrix PASS and cannot be reused.
- Fresh project `ipfs3-ps-1d2e392face595b00066800d151d56c5` passed eight-service startup, direct markers, bootstrap, mode/digest, and the then-existing three pure support units, then `private_swarm_configuration_and_peering` exited `101` with `kubo_swarm_peers_malformed_json`. Cleanup reached residual `0` and environment restoration `19/19`; those receipts and every preceding PASS remain partial evidence only.
- All four topology attempts remain permanently `BLOCKED`. A current full live claim requires a fifth newly generated unique project after static/source/Rust GREEN. Previously passing, partial, or diagnostic-toggle artifacts may be attached only after the complete fifth run passes and may never replace a fresh causal receipt or promote any old run.
- POSIX `wait` may return a signal-derived status when the wrapper trap interrupts it before the daemon has been reaped. `interrupt_daemon_wait` therefore sets `wait_interrupted=1` before forwarding the signal; every loop clears the flag immediately before `wait`; and a loop repeats only when that iteration observed both flag `1` and status `>128`. A raced real child status `37` or `43` is accepted immediately because it is `<=128`; an interrupted `143` is re-waited, and a subsequent real daemon `143` with the cleared flag is accepted. `kill -0` is not a re-wait criterion.
- The daemon result has priority: nonzero daemon status is returned unchanged even if filter/FIFO cleanup also fails. When the daemon returned `0`, nonzero filter wait or FIFO removal maps to wrapper status `1`; all waits and status captures remain under `set +e`, with no `set -e` in the wrapper. Static source assertions plus bounded `37`/`43`/`143`, stopped PID `0`, `docker diff` FIFO absence, and status-selection fixtures are the acceptance evidence; no runtime result is inferred during planning.
- Compose build-context resolution across a base file and override can differ by invocation. Task 1 uses `config --quiet` to choose the one rendered-valid `./ipfs`-equivalent path and then freezes it statically; it may not add a second image or move the Dockerfile.
- `docker compose start swarm-bootstrap` must rerun its already-completed container. The workflow polls exact state/exit and fails closed; it never treats `start` return alone as successful bootstrap evidence.
- `sha256sum` output and Kubo identities exist only in captured variables. Static tests reject console interpolation; source filtering protects only the exact Kubo fingerprint line, while diagnostics separately redact that prefix, standalone lowercase 64-hex, and PeerID forms without changing ordinary 32-hex or content CIDs.
- Fixed ports can be occupied by unrelated processes. Validation refuses without killing a process or cleaning a resource it does not own.
- Disposable live cleanup intentionally uses `down --volumes` only under unique project ownership and attempted markers. Production documentation never uses volume deletion because the five named data volumes are durable.
- The approved spec is currently untracked and is deliberately included with this plan in the exact 13-path final identity. Implementation workers may not edit either artifact's semantics or mark plan checkboxes.
- Base HEAD is `ad377bff760b6611496d46a6fc7fdae7aa2f6281` at planning time. An authorized base change must be disclosed, rerun affected checks, and appear in the frozen identity.

## Plan Self-Review

- Spec coverage: every acceptance criterion and expected file boundary in unchanged spec SHA256 `a55d6b0064fc51ed55b909dcf4b19bfd5f14230e3e9537fcc22b48030a0b921f` maps to Tasks 1-6; the runtime-only null-slice adapter stays inside the approved Rust HTTP observation boundary and introduces no product, Peering, NDJSON, provider, egress, multi-host, HA, rotation, or new-job scope.
- Placeholder scan: no unresolved marker or abbreviated implementation instruction remains; code-changing steps include concrete shell/Rust/PowerShell/YAML interfaces and exact RED/GREEN commands.
- Type/name consistency: `swarm_key`, `swarm_key_wrong`, `private-swarm-validation`, Kubo C `55102`, config DTOs, `deserialize_null_vec_as_empty`, `KuboSwarmPeers.peers`, `private_swarm_peers_json_contract_accepts_null_as_empty`, all environment names, test names, markers, and the 13-path manifest are consistent across tasks.
- Ownership consistency: Rust owns only loopback HTTP observations and pure JSON fixtures; outer PowerShell owns key generation, source/status/supervisor runtime proof, bounded PID/FIFO assertions, mode/digest, process lifecycle, Docker labels, raw-log assertions, defense-in-depth diagnostics, cleanup, and environment restoration.
- Causal consistency: key creation → config/build → wrapper negatives → supervisor proof → A/B/C startup → closed config → bootstrap `0` → direct logs/outer assertions → config/null-wrapper/Peering/release pure units → private peer/Peering topology → wrong-key → existing scenarios → full restart/recovery → logs-first cleanup. The null-wrapper unit adds one command inside the existing Rust gate, not a job or causal stage.
- Security consistency: no key or digest enters environment/logs/docs/manifest text; PeerIDs and full peer multiaddresses remain memory-only; source filtering is prefix-exact and diagnostics are defense in depth; ordinary 32-hex/CID fixtures remain unchanged; both secret files and state receipt use independent CreateNew ownership/deletion gates. Empty AddrFilters is confined to the internal-only PSK-protected Cluster Swarm port and is never described as egress isolation.
- Regression consistency: five existing Cluster scenarios, six workflow jobs, five client static commands, current protected hashes, and non-Cluster profiles remain guarded.
- Frozen-blocker closure: Task 1 retains separate peer/Peering CLI contracts; Task 2 now freezes the official single swarm-peer wrapper with null/missing/array/malformed fixtures and exactly one deserializer annotation, retains the five Peering fixtures unchanged, and statically rejects NDJSON or evidence-free Peering broadening.
- Current runtime-blocker closure: no daemon/filter re-wait decision uses `kill -0`; trap-owned per-wait flags distinguish interrupted waits from real `37`, `43`, and second-wait `143`; daemon status outranks filter/FIFO failures; daemon-success cleanup failures map to `1`; and the three-container matrix requires bounded stop, PID `0`, FIFO absence, source-level filter reap, exact ownership, and no diagnostic emission.
- Runtime evidence consistency: Task 4 preserves all four RED receipts, including exact project `ipfs3-ps-1d2e392face595b00066800d151d56c5`, exit `101`, category `kubo_swarm_peers_malformed_json`, wrapper `{"Peers":null}`, cleanup `0`, and restoration `19/19`. Task 2 applies the smallest matching DTO fix, and only a fifth full unique-project matrix can satisfy docs or final handoff.
- Mechanical consistency: six tasks contain forty contiguous checkbox steps and six complete Consumes/Produces pairs; all twenty-two PowerShell fences parse with zero AST errors; placeholder and stale-assertion scans return zero; all forty-two code fences are balanced and `git diff --check` passes. These are planning-time checks only, not fabricated implementation GREEN evidence.
- Review/Git consistency: plan stage performs no Git write; implementation workers never write Git; final commit is orchestrator-only after both receipts for one unchanged identity; push/tag remain forbidden.

## Plan Review Status

- Receipt: `waiting for receipt`
- Review owner: orchestrator
- Any edit to this plan invalidates a later receipt and requires review of the complete current revision.
