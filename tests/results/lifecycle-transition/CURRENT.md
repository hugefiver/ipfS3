# Lifecycle transition F1 — authoritative single-run receipt

**PASS — full runner exit 0.** The sole authoritative run is
[`20260913T011934Z-091eb31c/summary.json`](20260913T011934Z-091eb31c/summary.json).
No successful steps from previous partial/failed runs are used for this conclusion.
Historical receipts remain retained and superseded.

```powershell
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1
```

Executed without phase/resume arguments, 2026-09-13 01:19:34–01:25:55 UTC.
Every required step passed; `remaining_gates` is empty. All native step exit codes
are 0. Cleanup and input-identity checks are included in the final PASS decision.

## Frozen inputs

[`inputs.manifest.json`](20260913T011934Z-091eb31c/inputs.manifest.json) freezes
**229** tracked/untracked source, test, runner, Compose/config and Cargo input paths:
path, tracked state, existence, byte length and SHA-256 of complete file bytes.
HEAD is `c438e3cad67bfbd9634f1c93e8bd89e430872f8c`.
The manifest is recomputed before completion; `inputs.unchanged = true`.
The runner and Compose paths are explicitly recorded in the summary and covered
by the manifest. This is an exact-content identity manifest, not a redacted source
copy that could silently change compilation inputs.

Evidence directories are deliberately excluded to avoid self-reference. Source
content and secret environment values are not copied into the manifest. It is
not a dependency-registry snapshot: Cargo.lock, offline invocation, compiler and
SDK versions remain the dependency/toolchain evidence.

## Actual CAR and resident-memory gate

The stress object is 268,435,456 bytes, generated deterministically using a
SHA-256-seeded SplitMix64 stream and a distinct index prefix per 256-KiB chunk.
It does not repeat after the old 251-chunk period.

A test-only streaming proxy sits on the **production gateways' cold RPC path**.
It counts the actual `export.car` multipart file-part bytes while forwarding the
request to real cold Kubo; HTTP framing, multipart headers and boundaries are
excluded. The parser is tested with fragmented input. No whole CAR/request body
is collected by the proxy. The stress test resets the counters before transition
and rejects incomplete parsing, in-flight imports or a largest CAR ≤192 MiB.

| Measured fact | Actual result |
|---|---:|
| Completed imports | 2: empty object and stress object |
| Empty-object CAR bytes | 96 |
| **Stress CAR bytes actually uploaded** | **268,527,299** |
| Total completed CAR bytes | 268,527,395 |
| Parse failures / still in flight | 0 / 0 |
| Required lower bound for one CAR | **>201,326,592** bytes |
| Production gateway A peak resident working set | **40,329,216** bytes |
| Production gateway B peak resident working set | **38,526,976** bytes |
| Required per-production-gateway upper bound | **<201,326,592** bytes |

Both inequalities are enforced by the runner, not merely reported. Windows
`PeakWorkingSet64` is the resident working-set high-water mark of each production
gateway process. Test client, test proxies and Kubo memory are **not included**;
this is not a total-system-memory claim. Cold local DAG/CID and reconstructed
bytes, signed GET/HEAD/Range and exact length/ETag assertions also passed.
See `summary.json:memory` and the stress test log in this same run directory.

## Process recovery: database, bytes and signed gateway agree

Process-local publication now sets the actual payload length. The shared
`pg_residency` helper is unchanged. These fixtures use suspended buckets to expose
the public `null` version, rather than assuming an unversioned response must
expose it.

The native worker children still undergo real OS kill/restart and PostgreSQL
checkpoint/lease/epoch control. A test gateway serves the **real axum+s3s+
GatewayAuth+S3Impl HTTP stack** on the isolated PG schema (hosted in the parent
test process, not a direct handler call). The main topology separately retains
two native production gateway processes.

| Scenario | Signed GET + HEAD and backend assertion |
|---|---|
| Committed publish before cleanup recovery | IA, 70 bytes, CID/ETag, Content-Length, public `null`; cold local CID/bytes verified |
| Completed cleanup recovery | Same IA object and 70 bytes; all headers and cold bytes preserved |
| Replacement publication after stale worker fencing | IA, 65 bytes; headers/version and cold local bytes verified |
| Persisted retry recovery | IA, 67 bytes; headers/version and cold local bytes verified |
| Deleted policy cancels stale action | STANDARD, 70 bytes; signed reads and hot local bytes correct; no publication |
| Eight-attempt exhaustion | STANDARD, 69 bytes; signed reads and hot local bytes correct; no publication |
| Deleted target cancels action | Signed GET returns `NoSuchKey`, HEAD returns 404; no incorrect publication |

The full process parent passed. Four workers were intentionally killed/reaped,
takeover epochs advance 1→2→3→4→5, the old still-alive actor is
fenced, and a 31-second outstanding copy IO has a renewed 2-second lease.
There are eight successful child harness exits in addition to the parent result;
they are not mislabeled as extra parent scenarios. Shutdown, proxy audit
(`pin/rm` requests = 0), gateway teardown and schema cleanup all completed.
Detailed `event=gateway-read`, restart, cancellation and exhaustion records are
in `lifecycle_transition_process-lifecycle_transition_process_crash_restart_f1.log`.

## Exact test counts in this invocation

`P/F/I` denotes passed/failed/ignored. Integration binaries include their compiled
support-module tests; these are execution counts, not globally unique names.

| Suite / gate | P/F/I |
|---|---:|
| Nine real lifecycle transition cases | 9/0/0 across nine exact calls, each 9 filtered |
| Full process-recovery parent | 1/0/0, 1 filtered; child exits separately recorded |
| Stress-data unit regression | 1/0/9 (all nine live cases separately executed above) |
| Process-size unit regression | 1/0/1 (live parent separately executed above) |
| CAR-part parser unit regression | 1/0/1 (ignored long-lived proxy explicitly started and later stopped) |
| postgres_residency | 2/0/0 |
| postgres_residency_concurrency | 6/0/0 |
| postgres_lifecycle_transition | 2/0/0 |
| postgres_transition_saga | 1/0/0 |
| postgres_lifecycle | 9/0/0 |
| postgres_versioning | 31/0/0 |
| postgres_cors | 4/0/0 |
| residency_publication explicit PG case | 1/0/0, 5 filtered |
| lib | 1118/0/0 |
| integration | 164/0/0 |
| residency | 10/0/0 |
| lifecycle_transition_schema | 4/0/0 |
| cors | 8/0/0 |
| residency_publication default | 5/0/1; PG case executed separately above |
| multi_gateway | 28/0/0 |
| Runner / cleanup / input identity contracts | 1 + 2 + 2 passed |
| fmt; all-target clippy `-D warnings`; Rust 1.92 all-target check; diff-check | each exit 0 |

The same invocation also verifies real PostgreSQL 17, distinct hot/cold node IDs
and backing volumes, cold offline local integrity, hot-stop readable / cold-stop
no-fallback behavior, SigV4 XML/API encryption/version/listing/COPY coverage,
fresh/upgrade migrations and AWS CLI signed access. Runtime versions, dirty paths,
exact commands, exit codes and logs are in this run directory only.

## Cleanup and boundaries

The runner immediately captures **each** final container/volume/network query's
stdout/stderr and exit code. `summary.json:cleanup_inventory` shows three empty
outputs and **0/0/0** exit codes. A failure in any query changes full PASS to FAIL,
even if subsequent queries succeed. Contracts explicitly exercise `(7,0,0)` and
`(0,8,0)` and verify failure status plus all three retained results.

Container logs were captured before topology removal; all owned gateway, CAR
proxy and balancer process trees were stopped after pipe capture. Worker children,
their test gateway and isolated schema were cleaned. Both copied executable paths
are absent. A final additional Docker/process inventory found **zero** F1-owned
containers, volumes, networks or processes. No existing resources were removed.

No production code, F2/public README/config/deployment Compose/ROADMAP, Git state,
software installation or image acquisition was changed. Historical failures and
partials remain. Remaining scope limits: this certifies the tested PG17/Kubo43/
Windows-native topology, not other platforms, registry reproducibility or combined
client/proxy/Kubo memory. The existing `proc-macro-error2` future-incompatibility
notice remains; current clippy and Rust 1.92 checks pass.
