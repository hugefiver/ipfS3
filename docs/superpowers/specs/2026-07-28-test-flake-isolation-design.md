# Parallel test isolation and SQLite publication

## Requirements

The Rust suite must preserve its behavioral assertions under normal parallel execution and high concurrency with panic backtraces enabled. Fixture isolation must not require serializing the complete suite.

Lifecycle-order tests use dedicated owner objects and leases. A process-global recorder filtered by IDs must not capture unrelated tests' owner-lock or guard events. Shared-CID and canonical-target-selection assertions remain intact.

The worker panic-progress test keeps its notification-based flow and a bounded 10-second eventual-progress deadline. It verifies that a second provider enters after the first task panics, including when backtrace output is enabled.

V3 and legacy ambiguous-upload tests use a one-second request deadline and a two-second delayed mock response. They must establish the intended accepted upload with an ambiguous response while retaining recovery and exact request-count assertions. A request that never reaches the mock doesn't prove that state.

## SQLite transaction boundary

After pure publication-request validation and attachment planning, but before any database read, SQLite publication performs an idempotent update against the target bucket. The first database operation therefore acquires write intent, and the existing five-second busy timeout serializes writers without a deferred read-to-write upgrade window.

PostgreSQL behavior, public APIs, schema, error mapping and production timeouts remain unchanged. Earlier writer acquisition can queue concurrent SQLite publications sooner; it doesn't change committed data.

## Acceptance

- Renewal-order tests pass with default parallelism and isolated recorder identities.
- Worker progress passes with `RUST_BACKTRACE=1` under high concurrency.
- Both ambiguous-upload cases and the Pinata test group preserve their assertions under high concurrency.
- A controlled SQLite test holds an external writer and proves publication waits, then commits atomically after release. Concurrent publication cases retain all state assertions.
- Library tests pass repeatedly with default parallelism, with a serial run as a control. Integration and runnable deployment/client checks retain their own evidence boundaries.

Test cleanup removes only resources owned by that invocation. Passing an isolated or serial run doesn't by itself establish parallel-suite stability.
