# Shared Renewal Test Isolation Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent parallel tests using the shared `object-1` fixture from contaminating the lifecycle events asserted by the shared-renewal canonical-target test.

**Architecture:** Keep the production lifecycle recorder and renewal implementation unchanged. Isolate the affected test by creating a dedicated latest object, assigning only its renewed lease to that object, and using the dedicated ID consistently in recorder filters, renewal input, and expected events.

**Tech Stack:** Rust 2024, Tokio tests, SeaORM, in-memory SQLite, Cargo, PowerShell 7.

**Global Constraints:**
- Modify only `src/store/pinning/leases.rs`, specifically `renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target`.
- Do not change production renewal, locking, projection, pinning, or recorder behavior.
- Preserve the two targets' shared `bafy-shared-renewal` CID, timestamps, states, and canonical-selection assertions.
- Do not serialize the complete test suite or change schema, dependencies, public APIs, or timeouts.
- Use PowerShell syntax for every command.
- Do not run `git add`, `git commit`, `git push`, `git tag`, or any other Git write command.

**Authoritative spec:** `docs/superpowers/specs/2026-08-12-shared-renewal-test-isolation-design.md`

---

### Task 1: Isolate the shared-renewal owner fixture and verify regression stability

**Files:**
- Modify: `src/store/pinning/leases.rs:9784-9895`
- Verify: `docs/superpowers/specs/2026-08-12-shared-renewal-test-isolation-design.md`

**Interfaces:**
- Consumes: existing test helpers `setup()`, `seed_remote(...)`, `seed_lease_target(...)`, `start_lifecycle_order_recording_with_desired_target_reads(...)`, `include_owner_in_lifecycle_order_recording(...)`, `renew_manual_lease(...)`, and `finish_lifecycle_order_recording()`.
- Produces: the unchanged test function `renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target()` with a collision-free owner fixture; no production interface changes.

- [ ] **Step 1: Preserve the captured RED evidence**

Use the already captured full-suite failure as RED evidence. The failing event list contained two `OwnerLock("object-1")` entries while the expected list contained one. The isolated test subsequently passed 10/10, proving the failure is parallel fixture contamination and avoiding a deliberate reintroduction of nondeterminism.

- [ ] **Step 2: Insert and bind a dedicated owner fixture**

Immediately after `let db = setup().await;`, insert a dedicated latest object:

```rust
db.execute_unprepared(
    "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
     VALUES ('object-shared-renewal-order', 'bucket', 'shared-renewal-order-key', \
             'bafy-shared-renewal-owner', 100, 'bafy-shared-renewal-owner', TRUE)",
)
.await
.unwrap();
```

After both `seed_lease_target(...)` calls and before timestamp adjustments, reassign only the renewed lease and prove the update was exact:

```rust
let updated = db
    .execute_unprepared(
        "UPDATE pin_leases SET owner_object_id = 'object-shared-renewal-order' \
         WHERE id = 'lease-renewed-shared'",
    )
    .await
    .unwrap();
assert_eq!(updated.rows_affected(), 1);
```

Do not alter `lease-unrelated-shared`, either target row, or the shared remote CID.

- [ ] **Step 3: Use the dedicated owner throughout recording and renewal**

Replace the affected test's owner-specific statements exactly:

```rust
include_owner_in_lifecycle_order_recording("object-shared-renewal-order").await;

assert_eq!(
    renew_manual_lease(
        &db,
        "object-shared-renewal-order",
        "lease-renewed-shared",
        time(200),
        time(3),
    )
    .await
    .unwrap(),
    ManualLeaseRenewalOutcome::Extended { generation: 2 }
);
```

In the expected event vector, replace only:

```rust
test_gates::LifecycleOrderEvent::OwnerLock("object-shared-renewal-order".to_owned())
test_gates::LifecycleOrderEvent::OwnerGuard("object-shared-renewal-order".to_owned())
```

Keep every desired-target, lease, target, remote, CAS, work, and projection event unchanged and in the same order.

- [ ] **Step 4: Format and run the exact GREEN test**

Run:

```powershell
cargo fmt --all
cargo test --lib store::pinning::leases::tests::renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target -- --exact --nocapture
```

Expected: formatting exits 0; the exact test passes with one dedicated owner lock and one dedicated owner guard.

- [ ] **Step 5: Stress the exact test and related renewal group**

Run ten isolated repetitions, failing the command on any non-zero iteration:

```powershell
$failedIterations = @()
1..10 | ForEach-Object {
    cargo test --lib store::pinning::leases::tests::renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target -- --exact
    if ($LASTEXITCODE -ne 0) { $failedIterations += $_ }
}
if ($failedIterations.Count -gt 0) {
    throw "Shared-renewal isolation failed on iterations: $($failedIterations -join ', ')"
}
```

Then run:

```powershell
cargo test --lib manual_renewal -- --nocapture
```

Expected: 10/10 exact repetitions pass and every selected manual-renewal test passes.

- [ ] **Step 6: Audit the fixture isolation and retained scenario**

Run:

```powershell
$leaseSource = [IO.File]::ReadAllText("src/store/pinning/leases.rs")
$testMatch = [regex]::Match(
    $leaseSource,
    '(?s)async fn renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target\(\) \{.*?\r?\n    \}\r?\n\r?\n    #\[tokio::test\]'
)
if (-not $testMatch.Success) { throw "Unable to isolate the shared-renewal test body" }
$testRegion = $testMatch.Value
if ($testRegion -notmatch 'object-shared-renewal-order') { throw "Dedicated owner fixture is missing" }
if ($testRegion -match 'include_owner_in_lifecycle_order_recording\("object-1"\)|renew_manual_lease\(\s*&db,\s*"object-1"|Owner(?:Lock|Guard)\("object-1"') {
    throw "The shared-renewal test still records, renews, or expects shared owner object-1"
}
if (([regex]::Matches($testRegion, 'bafy-shared-renewal')).Count -lt 6) {
    throw "The shared remote CID scenario was unexpectedly reduced"
}
if ($testRegion -notmatch 'target-renewed-shared' -or $testRegion -notmatch 'target-unrelated-shared') {
    throw "The two-target canonical selection scenario is incomplete"
}
```

Expected: no exception. The test uses the dedicated owner while retaining both shared-CID targets.

- [ ] **Step 7: Run complete automated gates**

Run sequentially:

```powershell
cargo test --lib
cargo test --test integration
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
git diff --check
git status --short
```

Expected: 705 library tests and 124 integration tests pass with zero failures or ignored tests; formatting, Clippy, and diff checks exit 0. Git status lists only the approved specification, plan, and `src/store/pinning/leases.rs` edit.

- [ ] **Step 8: Checkpoint without Git writes**

Record the focused repetitions, complete gate results, changed files, and any residual risk. Do not stage or commit. After final implementation acceptance review, hand control back to the orchestrator to begin a separate design/plan cycle for the next roadmap item, the P0 release validation matrix.
