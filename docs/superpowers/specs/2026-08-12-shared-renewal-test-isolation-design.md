# Shared Renewal Test Isolation Design

**Date:** 2026-08-12
**Status:** Approved design
**Scope:** Remove lifecycle-recorder cross-test contamination from one pinning renewal-order test without changing production behavior.

## Problem

`cargo test --lib --test integration` produced one intermittent failure in
`store::pinning::leases::tests::renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target`.
The recorded event list contained an unexpected second `OwnerLock("object-1")`.
The target test passed in ten isolated repetitions, the integration suite passed
124/124, and a later full library run passed 705/705. This is evidence of a
parallel test-fixture collision rather than a production renewal failure.

The lifecycle recorder intentionally filters events by IDs, but this test adds
the shared fixture ID `object-1` to its owner filter. Other parallel tests also
operate on `object-1`, so their owner events can enter this test's global
recorder while it is active. The existing `LIFECYCLE_ORDER_TEST_LOCK` protects
tests that explicitly acquire it; it cannot exclude unrelated tests that use
the same fixture ID without using the recorder.

## Goals

1. Give the affected renewal-order test an owner ID not used by parallel tests.
2. Preserve its shared-CID and canonical-target-selection scenario exactly.
3. Preserve the expected lifecycle event order and all production behavior.
4. Prove stability under focused repetition and the existing project gates.

## Non-goals

- Do not change production renewal, locking, projection, or pinning behavior.
- Do not redesign the global lifecycle recorder or add recorder session tokens.
- Do not serialize the complete library test suite.
- Do not change database migrations, public APIs, dependencies, or timeouts.
- Do not include the next roadmap item, the release validation matrix, in this change.

## Design

Modify only the test
`renewal_projects_only_its_locked_targets_but_uses_shared_oldest_canonical_target`
in `src/store/pinning/leases.rs`.

The test will insert a dedicated latest object named
`object-shared-renewal-order`, using a dedicated key and CID. After seeding
`lease-renewed-shared`, it will update that lease's `owner_object_id` to the
dedicated object and assert that exactly one row changed. The unrelated lease
and both targets will continue to share `bafy-shared-renewal`; their timestamps,
states, and ordering remain unchanged so the canonical-target assertion still
tests the same behavior.

The lifecycle recorder owner filter, `renew_manual_lease` call, and expected
`OwnerLock`/`OwnerGuard` events will all use
`object-shared-renewal-order`. Because no unrelated parallel test uses this ID,
their owner events cannot satisfy the recorder filter.

This follows the established repository fix in commit `cd88a17`, which isolated
the adjacent `manual_renewal_orders_owner_then_lease_target_and_remote_work`
test with its own owner fixture.

## Data and Error Flow

The change affects only in-memory SQLite test setup and test-only recorder
filters:

1. Insert the dedicated object fixture.
2. Seed the existing remote, renewed lease/target, and unrelated lease/target.
3. Reassign only the renewed lease to the dedicated owner and assert one update.
4. Start lifecycle recording with existing lease, target, and remote filters.
5. Add the dedicated owner filter and renew that owner's manual lease.
6. Assert the unchanged lifecycle ordering with the dedicated owner ID.

Fixture insertion or owner reassignment failures remain immediate test failures
through the existing `unwrap` and explicit `rows_affected` assertion. No runtime
error mapping changes.

## Verification

The change is accepted only when all of the following pass:

1. The exact affected test under focused repetition.
2. The pinning manual-renewal test group under normal parallel execution.
3. `cargo test --lib`.
4. `cargo test --test integration`.
5. `cargo fmt --all -- --check`.
6. `cargo clippy --all-targets -- -D warnings`.

Source audit must confirm that the affected test no longer records, renews, or
expects owner events for `object-1`, while its shared remote CID and two-target
canonical selection remain intact.

## Repository Boundary

The specification, implementation plan, and test edit may be written to the
working tree. No Git commit, push, tag, or other Git write operation is
authorized by this design.
