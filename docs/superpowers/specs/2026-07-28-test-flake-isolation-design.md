# Test Flake Isolation Design

## Goal

Make the existing Rust test suite reliable under normal parallel execution and under high-concurrency runs with panic backtraces enabled. Preserve public behavior while fixing the SQLite publication lock-upgrade flaw exposed by the regression run.

## Confirmed causes

1. `manual_renewal_orders_owner_then_lease_target_and_remote_work` uses a process-global lifecycle recorder filtered by fixture IDs. Its owner ID, `object-1`, is also used by concurrent renewal tests, so unrelated owner lock and guard events enter the expected sequence.
2. `panicked_provider_task_releases_the_worker_slot_for_the_next_provider` verifies eventual progress but imposes a two-second deadline. Printing the intentional panic's backtrace under heavy test concurrency can consume that deadline even though isolated runs and single-threaded full-suite runs prove the worker releases the slot correctly.
3. The V3 and legacy ambiguous-upload tests use a 20-millisecond request deadline around a streaming multipart upload. Under full-suite load the client can expire before Wiremock finishes receiving and recording the request, so the tests observe zero uploads instead of the intended one accepted request with an ambiguous response.
4. SQLite publication starts as a deferred transaction, reads lifecycle state, and only later attempts a write. Two concurrent publications can therefore both hold read snapshots and fail immediately when upgrading to a writer; SQLite may bypass `busy_timeout` for this deadlock-shaped conflict. The existing outer retries reduce the probability but do not remove the upgrade window.

## Considered approaches

### 1. Isolate fixtures, widen non-performance deadlines, and acquire SQLite write intent before reads — selected

- Give the lifecycle-order test a unique owner object and point its lease at that owner.
- Increase the worker test's eventual-progress timeout from 2 seconds to 10 seconds.
- Give both ambiguous-upload tests one second to transmit their streaming request while delaying the mock response for two seconds.
- After pure publication request validation and attachment planning, but before any database read, issue a SQLite-only idempotent update against the target bucket. This makes the first database operation a write and lets the existing five-second busy timeout serialize SQLite writers. PostgreSQL remains unchanged.

This removes the two test timing races while retaining bounded failure and exact request-count assertions. The SQLite write-intent operation removes the deferred read-to-write upgrade window rather than relying on increasingly probable retries.

### 2. Serialize affected tests

Adding more global locks, changing the publication tests to one connection, or requiring `--test-threads=1` would avoid the symptoms but would conceal unsafe test isolation and real SQLite concurrency behavior. This is rejected.

### 3. Use WAL or increase retry counts

WAL improves reader/writer concurrency but still allows stale snapshots to fail when upgraded to writers. More fixed retries reduce the failure probability while leaving the same lockstep upgrade conflict. Neither is accepted as the primary fix.

### 4. Use a lower-level native `BEGIN IMMEDIATE`

Native `BEGIN IMMEDIATE` expresses the desired SQLite transaction mode directly, but SeaORM 1.1 does not expose it for SQLite. Reaching through to a SQLx connection would broaden the transaction abstraction and dependency surface. A first-statement idempotent update provides the same write intent within the existing transaction API and is selected instead.

### 5. Rebuild the test instrumentation

Scoping the recorder by database instance or task and replacing every wall-clock wait with a deterministic scheduler would be architecturally stronger, but it would require broad production-signature or test-harness changes unrelated to the confirmed causes. This is rejected as unnecessary scope.

## Changes

The implementation is limited to four existing test modules and the publication transaction entry point.

- The renewal order test inserts a uniquely named object fixture, updates its uniquely named lease to reference that object, and records/asserts owner events against the unique ID.
- The worker panic-progress test retains its current notification-based flow and error message but waits up to 10 seconds for the second provider to enter.
- The V3 and legacy ambiguous-upload tests preserve their recovery and request-count assertions but use a one-second upload deadline and a two-second delayed response.
- SQLite publication performs an idempotent target-bucket update after pure validation and attachment planning, and before its first database read. A controlled test holds an external writer briefly and proves the publication waits and commits atomically after release.

No database schema, dependency, public API, error mapping, PostgreSQL behavior, or production timeout changes. SQLite publications acquire their existing single-writer lock earlier, which may queue concurrent writers sooner but does not alter committed data.

## Verification

The existing failures provide RED evidence. After the change:

1. `cargo test --lib manual_renewal` must pass with default parallelism.
2. With `RUST_BACKTRACE=1`, `cargo test --lib pinning::worker::tests:: -- --test-threads=64` must pass.
3. Both ambiguous-upload tests and the full Pinata test module must pass under high test-thread concurrency.
4. The controlled SQLite writer test and both existing concurrent publication tests must pass, preserving all state assertions.
5. `cargo test --lib` must pass repeatedly with default parallelism, and `cargo test --lib -- --test-threads=1` must pass once as the control.
6. `cargo test --test integration`, PowerShell client-smoke infrastructure tests, Compose E2E tests, and runnable Docker client smoke tests must pass.
7. Formatting, strict clippy, language-server diagnostics, and a final diff-scope audit must report no new issues.

## Cleanup

The debug journal and temporary deepwork notepad are removed after verification. Docker services, volumes, and temporary test artifacts started by this session are also removed.
