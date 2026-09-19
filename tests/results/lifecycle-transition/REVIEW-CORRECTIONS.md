# Four Important review findings — RED/GREEN record

All changes are F1 tests/runners/evidence only. The first complete GREEN after the
review corrections is `20260913T002808Z-e4cb2a89`, exit 0; see the latest authoritative run in
[`CURRENT.md`](CURRENT.md). CURRENT does not reuse old partials.

| Finding | Verified RED | Correction and GREEN |
|---|---|---|
| Periodic stress data allowed a deduplicated small CAR | New chunk regression on the old generator observed **251 unique chunks instead of 252**. Logical size was therefore not sufficient memory evidence. | Deterministic seeded high-entropy data with nonduplicate chunk prefixes; live streaming multipart CAR-part counter. Full run actual largest uploaded CAR **268,527,299 bytes**, gateway peak working sets **39,862,272 / 38,907,904 bytes**, enforced against **201,326,592**. Generator/parser tests and real stress passed. |
| Process fixture metadata size was 7 | `process_publication_size_matches_actual_payload` initially failed **left 7 / right 45**. | Process-local correctly sized publication (shared helper unchanged). Unit regression passed; full process phase passed signed GET/HEAD/length/CID/ETag/class/public-version and local backend bytes for successful recovery, cancellation/exhaustion and target deletion. |
| Composite receipt did not freeze inputs | Old receipt mixed commands from different dirty states; it was demoted before correction, not silently promoted. | Full manifest records HEAD and exact-byte identities of 229 tracked/untracked inputs, excludes evidence self-reference/secrets, and checks unchanged inputs at end. Identity contract rejects modified untracked input. One complete invocation exited 0 with `inputs.unchanged=true`, empty remaining gates. |
| Earlier cleanup query errors could be overwritten | Executing the original cleanup block with mocked inventory exits `(7,0,0)` failed the new contract: **Cleanup accepted a failed inventory followed by success: 7,0,0**; command exited 1. No real Docker call occurred in this regression. | Immediate per-command capture and a shared gate. `(7,0,0)` and `(0,8,0)` both set receipt FAIL while preserving all results. Full run contracts passed; real inventories empty, exits `(0,0,0)`. |

Focused GREEN command before full execution:

```powershell
cargo test --locked --offline --test lifecycle_transition --test lifecycle_transition_car_proxy --test lifecycle_transition_process
pwsh -NoProfile -File tests/lifecycle-transition.Tests.ps1
cargo fmt --check
```

Results: three Rust unit tests passed; 9+1+1 prerequisite/long-lived tests explicitly
ignored in this unit-only invocation. Runner contracts passed 1, cleanup 2,
input identity 2. The ignored live cases were then explicitly run by the full
runner; they were not counted as unit-pass evidence.

Files corrected/added for these findings:

- `tests/support/lifecycle_transition_real.rs`
- `tests/lifecycle_transition_car_proxy.rs` (new streaming counter utility)
- `tests/support/lifecycle_transition_process.rs`
- `tests/support/lifecycle_transition_process_gateway.rs` (new signed surface)
- `tests/run-lifecycle-transition-validation.ps1`
- `tests/support/lifecycle-transition-runner.ps1` (new input/cleanup helpers)
- `tests/lifecycle-transition.Tests.ps1`
- `tests/cleanup-inventory.Tests.ps1` (new failure-injection contracts)
- F1 evidence files only; historical receipts retained.

The process gateway is an actual HTTP axum+s3s/SigV4 stack hosted by the test
parent on the isolated schema; the workers being killed are native child OS
processes. This distinction is intentional and recorded rather than calling the
test-hosted gateway a third production executable.
