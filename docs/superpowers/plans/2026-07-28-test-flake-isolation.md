# Test Flake Isolation Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the already implemented renewal/worker test fixes, stabilize both Pinata ambiguous-upload tests under full-suite load, and remove SQLite publication's deferred read-to-write upgrade race with an early write-intent statement and deterministic regression coverage.

**Architecture:** Keep the renewal and worker changes test-only, and widen only the two Pinata tests' upload-client timing window while preserving the ambiguous-response recovery contract. In publication, validate the in-memory request and entries and derive attachment pairs first, then make the first database statement a SQLite-only idempotent update of the target bucket so the existing busy timeout serializes writers before any read snapshot; PostgreSQL returns immediately from the helper. Prove each cause with focused RED→GREEN evidence, then run stress, complete non-Docker, Compose, real-client, and exact artifact-cleanup gates.

**Tech Stack:** Rust 2024 (MSRV 1.92), Tokio, SeaORM 1.1, SQLite, reqwest, Wiremock, Cargo, PowerShell 7, Docker Compose, rust-s3 E2E tests.

**Global Constraints:**
- The only Rust files that may change are `src/store/pinning/leases.rs`, `src/pinning/worker.rs`, `src/pinning/pinata.rs`, `src/store/pinning/publication.rs`, and `src/store/pinning/publication/tests.rs`.
- Task 1's `object-renewal-order` isolation and Task 2's 2-to-10-second worker timeout are already present in the worktree. Retain them; their full steps remain below so a fresh agent can verify or reproduce them without inventing behavior.
- In `src/pinning/pinata.rs`, modify only the two named ambiguous-upload tests. Do not change either production HTTP client or any other timeout/delay.
- In publication, perform all existing pure request/entry validation and `publication_attachment_pairs` planning before acquiring SQLite write intent, and acquire that intent before any database read.
- `acquire_sqlite_publication_write_intent<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<()>` must be a PostgreSQL/non-SQLite no-op and a SQLite idempotent `buckets.owner = buckets.owner` update filtered by bucket name. Propagate database errors and intentionally ignore `rows_affected` so missing-bucket behavior remains owned by the existing publication transaction.
- Do not change `MAX_TRANSACTION_RETRIES`, the 10/20/40 ms retry delays, the five-second SQLite busy timeout, database schema, public API, error mapping, production timeout, or dependencies.
- Do not serialize tests as a fix, enable WAL, increase retry counts, upgrade dependencies, or reach through SeaORM/SQLx to issue native `BEGIN IMMEDIATE`.
- Preserve Pinata's `ProviderErrorClass::Ambiguous`, `find` recovery, request-ID/status assertions, and exactly-one-upload-POST assertions.
- Preserve the renewal lifecycle event order and the worker notification flow and existing timeout error text.
- Use PowerShell syntax for every shell command. Do not use Bash environment syntax, `&&`, `/dev/null`, or unquoted paths with spaces.
- Do not print, copy, or embed values from local configuration or credential environment variables.
- Do not run `git add`, `git commit`, `git push`, `git tag`, or any other Git write command. The design and this plan remain untracked.
- Run artifact cleanup last and delete only `.debug-journal.md` and `C:\Users\hugefiver\AppData\Local\Temp\opencode\dw-20260728-124536-47cef3.md`.

---

## Existing Worktree Status

- Task 1 implementation is already present: `manual_renewal_orders_owner_then_lease_target_and_remote_work` uses `object-renewal-order` consistently.
- Task 2 implementation is already present: `panicked_provider_task_releases_the_worker_slot_for_the_next_provider` waits 10 seconds rather than 2 seconds.
- These two tasks may be marked implemented after their exact source audits pass, but their focused GREEN gates still run as part of this plan.
- The Pinata and publication changes are not yet implemented. Task 4 must add and run its deterministic test before adding the production helper.

## File Map

- Modify `src/store/pinning/leases.rs`, only `manual_renewal_orders_owner_then_lease_target_and_remote_work` (currently around lines 9228-9289) — retain unique renewal owner fixture isolation.
- Modify `src/pinning/worker.rs`, only the timeout around `fast_entered` in `panicked_provider_task_releases_the_worker_slot_for_the_next_provider` (currently around lines 7324-7368) — retain the test-only 10-second deadline.
- Modify `src/pinning/pinata.rs`, only `v3_upload_recovers_an_ambiguous_submit_through_find` and `legacy_upload_recovers_an_ambiguous_submit_through_find` (currently around lines 2375-2557) — change two mock response delays and two test-only upload-client deadlines.
- Modify `src/store/pinning/publication.rs:4-22, 298-358` — import `Expr` and the `bucket` entity, add the SQLite write-intent helper, and invoke it after pure validation but before the first database read.
- Modify `src/store/pinning/publication/tests.rs:1-5` and the file-backed publication test area beginning at `setup_file_backed`/the concurrent tests — import `TransactionTrait` and add `sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot`.
- Verify without modification: `tests/integration.rs`, `tests/e2e.rs`, `tests/client-smoke.Tests.ps1`, `scripts/client-smoke.ps1`, `docker-compose.yml`, and `config.docker.toml`.
- Delete last: `.debug-journal.md` and `C:\Users\hugefiver\AppData\Local\Temp\opencode\dw-20260728-124536-47cef3.md`.

---

### Task 1: Retain the implemented manual-renewal fixture isolation

**Files:**
- Modify/verify: `src/store/pinning/leases.rs`, only `manual_renewal_orders_owner_then_lease_target_and_remote_work`
- Reference without modification: existing `setup`, `seed_remote`, and `seed_lease_target` test helpers in the same file
- Test: `src/store/pinning/leases.rs`, the named renewal-order test and `manual_renewal` filter group

**Interfaces:**
- Consumes: existing `setup()`, `seed_remote(...)`, `seed_lease_target(...)`, `start_lifecycle_order_recording(...)`, `include_owner_in_lifecycle_order_recording(...)`, and `renew_manual_lease(...)` test helpers.
- Produces: one lifecycle-order test whose inserted object, persisted lease owner, recorder filter, renewal call, and owner events all use `object-renewal-order`.

- [ ] **Step 1: Preserve the captured RED evidence**

Historical RED command:

```powershell
cargo test --lib manual_renewal
```

Expected historical failure: ordinary parallel execution can inject unrelated `OwnerLock`/`OwnerGuard` events for shared fixture ID `object-1` into the process-global lifecycle recorder. This is captured flake evidence; do not revert the fix merely to reproduce it.

- [ ] **Step 2: Verify or restore the exact isolated test**

The named test must equal this implementation. If it already does, do not rewrite it; mark this step complete after comparison.

```rust
#[tokio::test]
async fn manual_renewal_orders_owner_then_lease_target_and_remote_work() {
    let _order_test_guard = test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
    let db = setup().await;
    db.execute_unprepared(
        "INSERT INTO objects (id, bucket, key, cid, size, etag, is_latest) \
         VALUES ('object-renewal-order', 'bucket', 'renewal-order-key', \
                 'bafy-renewal-order', 100, 'bafy-renewal-order', TRUE)",
    )
    .await
    .unwrap();
    seed_remote(
        &db,
        "pinata",
        "bafy-renewal-order",
        "pinned",
        Some("request-order"),
        1,
    )
    .await;
    seed_lease_target(
        &db,
        "lease-renewal-order",
        "manual",
        "all",
        "active",
        1,
        "target-renewal-order",
        "pinata",
        "bafy-renewal-order",
        "pinned",
        time(10),
    )
    .await;
    let updated = db
        .execute_unprepared(
            "UPDATE pin_leases SET owner_object_id = 'object-renewal-order' \
             WHERE id = 'lease-renewal-order'",
        )
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    start_lifecycle_order_recording(
        &["lease-renewal-order"],
        &["target-renewal-order"],
        &[("pinata", "bafy-renewal-order")],
    )
    .await;
    include_owner_in_lifecycle_order_recording("object-renewal-order").await;

    renew_manual_lease(
        &db,
        "object-renewal-order",
        "lease-renewal-order",
        time(20),
        time(1),
    )
    .await
    .unwrap();

    assert_eq!(
        finish_lifecycle_order_recording().await,
        vec![
            test_gates::LifecycleOrderEvent::OwnerLock("object-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::LeaseLock("lease-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::TargetLock("target-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::RemoteLock(
                "pinata".to_owned(),
                "bafy-renewal-order".to_owned(),
            ),
            test_gates::LifecycleOrderEvent::OwnerGuard("object-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::LeaseCas("lease-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::TargetCas("target-renewal-order".to_owned()),
            test_gates::LifecycleOrderEvent::RemoteWork(
                "pinata".to_owned(),
                "bafy-renewal-order".to_owned(),
            ),
            test_gates::LifecycleOrderEvent::TargetProjection(
                "target-renewal-order".to_owned(),
            ),
        ]
    );
}
```

- [ ] **Step 3: Run the focused renewal GREEN gate**

```powershell
cargo test --lib manual_renewal
```

Expected: exit code 0; every selected renewal test passes under default parallelism with the lifecycle order unchanged.

- [ ] **Step 4: Audit complete owner-ID isolation**

```powershell
$leaseSource = [IO.File]::ReadAllText("src/store/pinning/leases.rs")
$testMatch = [regex]::Match(
    $leaseSource,
    '(?s)async fn manual_renewal_orders_owner_then_lease_target_and_remote_work\(\) \{.*?\r?\n    \}\r?\n\r?\n    #\[tokio::test\]'
)
if (-not $testMatch.Success) { throw "Unable to isolate the renewal-order test body" }
$testRegion = $testMatch.Value
if ($testRegion -notmatch 'object-renewal-order') { throw "Unique renewal owner is missing" }
if ($testRegion -match 'include_owner_in_lifecycle_order_recording\("object-1"\)|renew_manual_lease\(\s*&db,\s*"object-1"|Owner(?:Lock|Guard)\("object-1"') {
    throw "The renewal-order test still records, renews, or asserts shared owner object-1"
}
```

Expected: no exception; the test uses the unique owner in every collision-sensitive position.

---

### Task 2: Retain the implemented worker test deadline

**Files:**
- Modify/verify: `src/pinning/worker.rs`, only the `fast_entered` timeout in `panicked_provider_task_releases_the_worker_slot_for_the_next_provider`
- Test: the named test and `pinning::worker::tests::` module

**Interfaces:**
- Consumes: the existing `Notify` future `fast_entered` and `tokio::time::timeout(Duration, future)` assertion.
- Produces: the same worker-slot release assertion and message, with a test-only 10-second diagnostic-pressure deadline.

- [ ] **Step 1: Preserve the captured high-pressure RED evidence**

Historical RED command:

```powershell
$env:RUST_BACKTRACE = '1'; cargo test --lib pinning::worker::tests:: -- --test-threads=64
```

Expected historical failure: while the intentional provider panic prints a backtrace under full load, the old two-second deadline can reach `JoinError leaked the worker slot and blocked the next provider`. Isolated and single-thread evidence shows no production slot leak.

- [ ] **Step 2: Verify or restore the exact timeout block**

Only this duration changes; if already present, do not rewrite neighboring code.

```rust
tokio::time::timeout(std::time::Duration::from_secs(10), fast_entered)
    .await
    .expect("JoinError leaked the worker slot and blocked the next provider");
```

- [ ] **Step 3: Run the exact and pressure GREEN gates with environment restoration**

```powershell
cargo test --lib panicked_provider_task_releases_the_worker_slot_for_the_next_provider
if ($LASTEXITCODE -ne 0) { throw "Exact worker regression failed" }

$rustBacktraceWasSet = Test-Path Env:RUST_BACKTRACE
$previousRustBacktrace = $env:RUST_BACKTRACE
try {
    $env:RUST_BACKTRACE = '1'
    cargo test --lib pinning::worker::tests:: -- --test-threads=64
    if ($LASTEXITCODE -ne 0) { throw "64-thread worker suite failed with RUST_BACKTRACE=1" }
} finally {
    if ($rustBacktraceWasSet) {
        $env:RUST_BACKTRACE = $previousRustBacktrace
    } else {
        Remove-Item Env:RUST_BACKTRACE -ErrorAction SilentlyContinue
    }
}
```

Expected: both commands pass; `RUST_BACKTRACE` returns to its inherited state.

- [ ] **Step 4: Prove the worker diff is only 2→10 seconds**

```powershell
$workerDiff = git diff -- src/pinning/worker.rs
$changedLines = @($workerDiff -split "`r?`n" | Where-Object { $_ -match '^[+-]' -and $_ -notmatch '^[-+]{3}' })
$expectedLines = @(
    '-        tokio::time::timeout(std::time::Duration::from_secs(2), fast_entered)',
    '+        tokio::time::timeout(std::time::Duration::from_secs(10), fast_entered)'
)
$delta = @(Compare-Object -ReferenceObject $expectedLines -DifferenceObject $changedLines)
if ($delta.Count -ne 0) { $workerDiff; throw "worker.rs changed beyond the approved test timeout" }
```

Expected: no exception and no other worker change.

---

### Task 3: Stabilize Pinata ambiguous-upload test timing

**Files:**
- Modify: `src/pinning/pinata.rs`, only `v3_upload_recovers_an_ambiguous_submit_through_find`
- Modify: `src/pinning/pinata.rs`, only `legacy_upload_recovers_an_ambiguous_submit_through_find`
- Test: the two named tests and `pinning::pinata::tests::`

**Interfaces:**
- Consumes: each test's Wiremock upload response, test-only `upload_http` reqwest client, `submit`, `find`, and request recorder.
- Produces: both tests still force an ambiguous timed-out response after Wiremock has enough time to receive the streaming multipart POST, then recover via `find` and prove exactly one POST.

- [ ] **Step 1: Preserve the captured full-suite RED evidence**

Captured failures:

- V3 failed at `src/pinning/pinata.rs:2458` with upload count `left: 0`, `right: 1`.
- Legacy failed near `src/pinning/pinata.rs:2550` with upload count `left: 0`, `right: 1`.
- Each exact test and the Pinata module passed in ten isolated runs, identifying a full-load request-transmission timing race rather than a production recovery defect.

Do not restore the old values just to reproduce a probabilistic RED.

- [ ] **Step 2: Apply the exact V3 test-only replacements**

Within `v3_upload_recovers_an_ambiguous_submit_through_find`, replace only these expressions:

```rust
ResponseTemplate::new(200)
    .set_delay(Duration::from_secs(2))
    .set_body_json(json!({ "data": { "id": "file-7", "cid": "bafy-target" } }))
```

```rust
reqwest::Client::builder()
    .timeout(Duration::from_secs(1))
    .build()
    .unwrap(),
```

The first replaces `.set_delay(Duration::from_millis(200))`; the second replaces only the final test-only upload client `.timeout(Duration::from_millis(20))`. Keep the separate 30-second control client unchanged.

- [ ] **Step 3: Apply the exact legacy test-only replacements**

Within `legacy_upload_recovers_an_ambiguous_submit_through_find`, replace only these expressions:

```rust
ResponseTemplate::new(200)
    .set_delay(Duration::from_secs(2))
    .set_body_json(json!({ "IpfsHash": "bafy-target", "ID": "legacy-file-7" }))
```

```rust
reqwest::Client::builder()
    .timeout(Duration::from_secs(1))
    .build()
    .unwrap(),
```

Again, keep the separate 30-second control client and all production clients unchanged.

- [ ] **Step 4: Run both exact GREEN tests**

```powershell
cargo test --lib v3_upload_recovers_an_ambiguous_submit_through_find
if ($LASTEXITCODE -ne 0) { throw "V3 ambiguous-upload regression failed" }
cargo test --lib legacy_upload_recovers_an_ambiguous_submit_through_find
if ($LASTEXITCODE -ne 0) { throw "Legacy ambiguous-upload regression failed" }
```

Expected: both exact tests pass, including `ProviderErrorClass::Ambiguous`, `find` recovery, request ID, pinned status, and exactly one upload POST.

- [ ] **Step 5: Run the Pinata module at 64 threads and repeat it ten times**

```powershell
cargo test --lib pinning::pinata::tests:: -- --test-threads=64
if ($LASTEXITCODE -ne 0) { throw "Pinata module 64-thread gate failed" }

for ($iteration = 1; $iteration -le 10; $iteration++) {
    cargo test --lib pinning::pinata::tests:: -- --test-threads=64
    if ($LASTEXITCODE -ne 0) { throw "Pinata module stress failed on iteration $iteration" }
}
```

Expected: the initial run and all ten stress iterations pass.

- [ ] **Step 6: Audit the Pinata diff and retained assertions**

```powershell
$pinataSource = [IO.File]::ReadAllText("src/pinning/pinata.rs")
foreach ($testName in @(
    "v3_upload_recovers_an_ambiguous_submit_through_find",
    "legacy_upload_recovers_an_ambiguous_submit_through_find"
)) {
    $pattern = '(?s)async fn ' + [regex]::Escape($testName) + '\(\) \{.*?\r?\n    \}\r?\n\r?\n    #\[tokio::test\]'
    $match = [regex]::Match($pinataSource, $pattern)
    if (-not $match.Success) { throw "Unable to isolate Pinata test $testName" }
    $region = $match.Value
    if ($region -notmatch 'set_delay\(Duration::from_secs\(2\)\)') { throw "$testName lacks the two-second response delay" }
    if ($region -notmatch 'timeout\(Duration::from_secs\(1\)\)') { throw "$testName lacks the one-second upload timeout" }
    if ($region -notmatch 'ProviderErrorClass::Ambiguous') { throw "$testName lost the ambiguous-error assertion" }
    if ($region -notmatch '\.find\(') { throw "$testName lost find recovery" }
    if ($region -notmatch '\.count\(\),\s*1') { throw "$testName lost the exactly-one-POST assertion" }
}

$pinataDiff = git diff -- src/pinning/pinata.rs
$changedLines = @($pinataDiff -split "`r?`n" | Where-Object { $_ -match '^[+-]' -and $_ -notmatch '^[-+]{3}' })
if ($changedLines.Count -ne 8) { $pinataDiff; throw "Pinata diff must contain exactly four replacements" }
foreach ($expectation in @(
    @{ Pattern = '^-.*set_delay\(Duration::from_millis\(200\)\)'; Count = 2 },
    @{ Pattern = '^\+.*set_delay\(Duration::from_secs\(2\)\)'; Count = 2 },
    @{ Pattern = '^-.*timeout\(Duration::from_millis\(20\)\)'; Count = 2 },
    @{ Pattern = '^\+.*timeout\(Duration::from_secs\(1\)\)'; Count = 2 }
)) {
    $actual = @($changedLines | Where-Object { $_ -match $expectation.Pattern }).Count
    if ($actual -ne $expectation.Count) { $pinataDiff; throw "Unexpected Pinata replacement set" }
}
```

Expected: no exception; exactly two delay and two upload-timeout replacements exist, all within the named tests.

---

### Task 4: Acquire SQLite publication write intent before reads

**Files:**
- Modify: `src/store/pinning/publication/tests.rs`, SeaORM test import and new deterministic test
- Modify: `src/store/pinning/publication.rs`, production imports, helper, and one call in `publish_in_transaction`
- Test: new `sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot`
- Test: existing `concurrent_different_keys_sharing_a_remote_retry_rolled_back_sqlite_conflicts`
- Test: existing `concurrent_same_key_overwrites_retry_and_leave_one_consistent_lifecycle`

**Interfaces:**
- Consumes: `setup_file_backed`, `DatabaseConnection::begin` through `TransactionTrait`, the existing five-second SQLite busy timeout, `publish_object`, and `PublicationRequest.object.bucket`.
- Produces: `acquire_sqlite_publication_write_intent<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<()>`, called after pure validation and attachment planning but before any database read; a deterministic regression that holds an external writer for 250 ms and requires publication to wait then commit.

- [ ] **Step 1: Add the deterministic test before changing production code**

Add `TransactionTrait` to the existing SeaORM test import:

```rust
use sea_orm::{
    ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, Statement, TransactionTrait,
};
```

Add this test after `setup_file_backed` and before publication concurrency regressions. Do not add sleeps to production code.

```rust
#[tokio::test]
async fn sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot() {
    let (_directory, db) = setup_file_backed("publication-write-intent.sqlite").await;
    let blocker = db.begin().await.unwrap();
    blocker
        .execute_unprepared("UPDATE buckets SET owner = owner WHERE name = 'bucket'")
        .await
        .unwrap();

    let provider_limits = limits();
    let publication = request(
        object(
            "write-intent-publication",
            "write-intent-key",
            "bafy-write-intent",
            7,
        ),
        vec![],
        vec![],
    );
    let publish = publish_object(&db, publication, &provider_limits);
    let release_blocker = async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        blocker.rollback().await.unwrap();
    };

    let (published, ()) = tokio::join!(publish, release_blocker);
    assert!(
        published.is_ok(),
        "publication did not wait for the existing SQLite writer: {published:?}"
    );
    let latest = crate::store::object::get_latest(&db, "bucket", "write-intent-key")
        .await
        .unwrap();
    assert_eq!(
        (latest.id.as_str(), latest.cid.as_str(), latest.is_latest),
        ("write-intent-publication", "bafy-write-intent", true)
    );
}
```

`Duration` remains available through the test module's existing `use super::*`; add no second duration alias.

- [ ] **Step 2: Run the new test alone to verify RED**

```powershell
cargo test --lib sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot -- --nocapture
```

Expected RED on the old production implementation: after four fast deferred read-to-write upgrade attempts separated by the existing 10/20/40 ms delays, `published` is `Err` with SQLite `database is locked`/code 5 before the blocker rolls back at 250 ms, and the `published.is_ok()` assertion fails. If the old implementation unexpectedly passes, stop and investigate the fixture rather than weakening the timing.

- [ ] **Step 3: Add the production imports**

At the top of `src/store/pinning/publication.rs`, add `Expr` and `bucket` without changing other imports:

```rust
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionError, TransactionTrait,
};
```

```rust
store::{
    entities::{bucket, multipart_upload, object, pin_lease, pin_lease_target, remote_pin},
    multipart::{CommitCompletedUploadError, ReconciledCommitOutcome},
    object::LatestObjectRow,
},
```

- [ ] **Step 4: Add the SQLite-only write-intent helper**

Place this helper immediately before `publish_in_transaction`:

```rust
async fn acquire_sqlite_publication_write_intent<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
) -> AppResult<()> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        return Ok(());
    }

    bucket::Entity::update_many()
        .col_expr(
            bucket::Column::Owner,
            Expr::col(bucket::Column::Owner).into(),
        )
        .filter(bucket::Column::Name.eq(bucket_name))
        .exec(db)
        .await?;
    Ok(())
}
```

Do not capture or inspect the update result. Zero affected rows must not create a new missing-bucket branch.

- [ ] **Step 5: Invoke the helper after pure validation and attachment planning, before all database reads**

The beginning of `publish_in_transaction` must be exactly this ordering:

```rust
async fn publish_in_transaction<C: ConnectionTrait>(
    db: &C,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    upload_id: Option<&str>,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    validate_request(&request)?;
    for entry in &entries {
        if entry.logical_size < 0 {
            return Err(invalid_publication(
                "logical object size cannot be negative",
            ));
        }
    }
    let attachment_pairs = publication_attachment_pairs(&request, &entries, limits)?;
    acquire_sqlite_publication_write_intent(db, &request.object.bucket).await?;
    let publication_time = Utc::now();
    let object_id = request.object.id.clone();

    let previous_owner_ids =
        lock_previous_publication_owners(db, &request.object, &entries).await?;
```

Remove the old `let attachment_pairs = publication_attachment_pairs(...)` line after `lock_previous_publication_owners`; the value is now computed once before write intent. Leave the remainder of the function unchanged. Validation, entry-size checks, and attachment planning remain pure; the helper is the first database operation.

- [ ] **Step 6: Run the new deterministic GREEN test**

```powershell
cargo test --lib sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot -- --nocapture
```

Expected GREEN: the first SQLite update waits for the blocker rollback, publication returns `Ok`, and the expected object is latest. The test remains bounded by the configured five-second busy timeout.

- [ ] **Step 7: Run both existing concurrent publication regressions**

```powershell
cargo test --lib concurrent_different_keys_sharing_a_remote_retry_rolled_back_sqlite_conflicts
if ($LASTEXITCODE -ne 0) { throw "Different-key publication concurrency regression failed" }
cargo test --lib concurrent_same_key_overwrites_retry_and_leave_one_consistent_lifecycle
if ($LASTEXITCODE -ne 0) { throw "Same-key publication concurrency regression failed" }
```

Expected: both tests pass with all existing object, lease, target, remote epoch, quota, and job-state assertions unchanged.

- [ ] **Step 8: Stress the concurrent publication group twenty times**

```powershell
for ($iteration = 1; $iteration -le 20; $iteration++) {
    cargo test --lib publication::tests::concurrent -- --test-threads=64
    if ($LASTEXITCODE -ne 0) { throw "Publication concurrency stress failed on iteration $iteration" }
}
```

Expected: both existing tests selected by `publication::tests::concurrent` pass in all twenty iterations without code 5 failures.

- [ ] **Step 9: Audit write-intent placement and unchanged retry policy**

```powershell
$publicationSource = [IO.File]::ReadAllText("src/store/pinning/publication.rs")
$body = [regex]::Match(
    $publicationSource,
    '(?s)async fn publish_in_transaction<C: ConnectionTrait>\(.*?\r?\n\}'
).Value
if ([string]::IsNullOrWhiteSpace($body)) { throw "Unable to isolate publish_in_transaction" }
$validationIndex = $body.IndexOf('validate_request(&request)?;')
$attachmentsIndex = $body.IndexOf('let attachment_pairs = publication_attachment_pairs(&request, &entries, limits)?;')
$intentIndex = $body.IndexOf('acquire_sqlite_publication_write_intent(db, &request.object.bucket).await?;')
$firstReadIndex = $body.IndexOf('lock_previous_publication_owners')
if ($validationIndex -lt 0 -or $attachmentsIndex -le $validationIndex -or $intentIndex -le $attachmentsIndex -or $firstReadIndex -le $intentIndex) {
    throw "Publication write intent is not after pure planning and before the first database read"
}
if ($publicationSource -notmatch 'const MAX_TRANSACTION_RETRIES: usize = 3;') { throw "Publication retry count changed" }
if ($publicationSource -notmatch '10_u64\.checked_shl\(retry\.min\(2\) as u32\)') { throw "Publication retry delays changed" }
```

Expected: no exception; the helper precedes the first read and retry policy remains 3 retries with 10/20/40 ms delays.

---

### Task 5: Run complete non-Docker regression and quality gates

**Files:**
- Verify: all five approved Rust files
- Verify without modification: `tests/integration.rs`, `tests/client-smoke.Tests.ps1`, and `scripts/client-smoke.ps1`

**Interfaces:**
- Consumes: Tasks 1-4 completed, including the new deterministic publication test.
- Produces: repeated default-parallel, single-thread control, integration, PowerShell infrastructure, formatting, strict lint, five-file diagnostics, whitespace, and exact tracked-scope evidence.

- [ ] **Step 1: Check formatting**

```powershell
cargo fmt --check
```

Expected: exit code 0 and no formatting diff.

- [ ] **Step 2: Re-run every focused regression surface**

```powershell
cargo test --lib manual_renewal
if ($LASTEXITCODE -ne 0) { throw "Manual-renewal targeted gate failed" }
cargo test --lib panicked_provider_task_releases_the_worker_slot_for_the_next_provider
if ($LASTEXITCODE -ne 0) { throw "Exact worker targeted gate failed" }
cargo test --lib v3_upload_recovers_an_ambiguous_submit_through_find
if ($LASTEXITCODE -ne 0) { throw "V3 ambiguous-upload targeted gate failed" }
cargo test --lib legacy_upload_recovers_an_ambiguous_submit_through_find
if ($LASTEXITCODE -ne 0) { throw "Legacy ambiguous-upload targeted gate failed" }
cargo test --lib sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot
if ($LASTEXITCODE -ne 0) { throw "SQLite write-intent targeted gate failed" }
cargo test --lib concurrent_different_keys_sharing_a_remote_retry_rolled_back_sqlite_conflicts
if ($LASTEXITCODE -ne 0) { throw "Different-key publication targeted gate failed" }
cargo test --lib concurrent_same_key_overwrites_retry_and_leave_one_consistent_lifecycle
if ($LASTEXITCODE -ne 0) { throw "Same-key publication targeted gate failed" }

cargo test --lib pinning::pinata::tests:: -- --test-threads=64
if ($LASTEXITCODE -ne 0) { throw "Pinata 64-thread module gate failed" }

$rustBacktraceWasSet = Test-Path Env:RUST_BACKTRACE
$previousRustBacktrace = $env:RUST_BACKTRACE
try {
    $env:RUST_BACKTRACE = '1'
    cargo test --lib pinning::worker::tests:: -- --test-threads=64
    if ($LASTEXITCODE -ne 0) { throw "Worker 64-thread backtrace gate failed" }
} finally {
    if ($rustBacktraceWasSet) {
        $env:RUST_BACKTRACE = $previousRustBacktrace
    } else {
        Remove-Item Env:RUST_BACKTRACE -ErrorAction SilentlyContinue
    }
}
```

Expected: every exact test passes; Pinata passes at 64 threads; worker passes at 64 threads with backtraces; environment state is restored.

- [ ] **Step 3: Run the default-parallel library suite five times**

```powershell
for ($iteration = 1; $iteration -le 5; $iteration++) {
    cargo test --lib
    if ($LASTEXITCODE -ne 0) { throw "Default-parallel library suite failed on iteration $iteration" }
}
```

Expected: all five runs exit 0. The current baseline contains 527 library tests; after Task 4 adds one deterministic test, Cargo should report all 528 passing, which proves all 527 pre-existing tests plus the new regression pass on every run.

- [ ] **Step 4: Run the single-threaded library control**

```powershell
cargo test --lib -- --test-threads=1
```

Expected: exit code 0 and all 528 library tests pass sequentially.

- [ ] **Step 5: Run the integration suite**

```powershell
cargo test --test integration
```

Expected: exit code 0 with no failed integration tests.

- [ ] **Step 6: Run the PowerShell client-smoke infrastructure tests**

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
```

Expected: exit code 0 and final output `client-smoke infrastructure tests: PASSED`; this preflight does not contact Docker.

- [ ] **Step 7: Run strict Clippy**

```powershell
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: exit code 0 and no warning promoted to an error.

- [ ] **Step 8: Run language-server diagnostics for all five changed Rust files**

Use the diagnostics tool with these exact arguments:

```text
lsp_diagnostics({ filePath: "C:\\Users\\hugefiver\\source\\ipfS3\\src\\store\\pinning\\leases.rs", severity: "all" })
lsp_diagnostics({ filePath: "C:\\Users\\hugefiver\\source\\ipfS3\\src\\pinning\\worker.rs", severity: "all" })
lsp_diagnostics({ filePath: "C:\\Users\\hugefiver\\source\\ipfS3\\src\\pinning\\pinata.rs", severity: "all" })
lsp_diagnostics({ filePath: "C:\\Users\\hugefiver\\source\\ipfS3\\src\\store\\pinning\\publication.rs", severity: "all" })
lsp_diagnostics({ filePath: "C:\\Users\\hugefiver\\source\\ipfS3\\src\\store\\pinning\\publication\\tests.rs", severity: "all" })
```

Expected: no new error or warning diagnostic in any of the five files.

- [ ] **Step 9: Enforce whitespace and the five-file tracked boundary**

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Whitespace errors found" }

$allowedTrackedChanges = @(
    "src/pinning/pinata.rs",
    "src/pinning/worker.rs",
    "src/store/pinning/leases.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs"
)
$changedTracked = @(git diff --name-only)
$unexpectedTracked = @($changedTracked | Where-Object { $_ -notin $allowedTrackedChanges })
if ($unexpectedTracked.Count -ne 0) {
    $unexpectedTracked | ForEach-Object { $_ }
    throw "Implementation changed tracked files outside the approved five-file scope"
}
git diff --stat -- @allowedTrackedChanges
```

Expected: no whitespace error and no tracked modification outside the five approved Rust files. The untracked design and plan are allowed and must not be staged.

---

### Task 6: Run owned Compose, E2E, Docker-client smoke, and stack cleanup

**Files:**
- Execute without modification: `docker-compose.yml`
- Execute without modification: `tests/e2e.rs`
- Execute without modification: `scripts/client-smoke.ps1`
- Verify without modification: `config.docker.toml`

**Interfaces:**
- Consumes: all Task 5 gates, an available Docker daemon, required local configuration supplied outside command output, and locally available build/client images.
- Produces: healthy Kubo/gateway readiness, all 11 E2E tests, at least one runnable client `PASSED`, no client `FAILED`, cleanup of only owned Compose/temp resources, and restored process environment.

- [ ] **Step 1: Run the complete real-surface gate in one cleanup-safe PowerShell block**

Run from the repository root. The block refuses to adopt pre-existing fixed containers or namespaced resources, never prints the master key, uses a unique temp parent, and performs cleanup/environment restoration in `finally`.

```powershell
$composeWasClaimed = $false
$smokeTempWasClaimed = $false
$smokeOutput = @()
$bodyError = $null
$cleanupFailures = [System.Collections.Generic.List[string]]::new()
$baseTemp = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar
)
$runSuffix = [Guid]::NewGuid().ToString("N").Substring(0, 12)
$composeProject = "ipfs3-regression-$PID-$runSuffix"
$smokeTempParent = Join-Path $baseTemp $composeProject
$composeProjectWasSet = Test-Path Env:COMPOSE_PROJECT_NAME
$previousComposeProject = $env:COMPOSE_PROJECT_NAME
$tempWasSet = Test-Path Env:TEMP
$previousTemp = $env:TEMP
$tmpWasSet = Test-Path Env:TMP
$previousTmp = $env:TMP

try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { throw "Docker daemon is unavailable" }

    foreach ($containerName in @("ipfs-s3-kubo", "ipfs-s3-gateway")) {
        docker container inspect $containerName *> $null
        if ($LASTEXITCODE -eq 0) {
            throw "Fixed Compose container already exists and will not be adopted: $containerName"
        }
    }
    foreach ($resource in @(
        @{ Kind = "volume"; Name = "${composeProject}_ipfs_data" },
        @{ Kind = "volume"; Name = "${composeProject}_gateway_data" },
        @{ Kind = "network"; Name = "${composeProject}_default" }
    )) {
        & docker $resource.Kind inspect $resource.Name *> $null
        if ($LASTEXITCODE -eq 0) {
            throw "Namespaced Docker resource already exists and will not be adopted: $($resource.Kind) $($resource.Name)"
        }
    }
    if (-not (Test-Path Env:IPFS_S3_MASTER_KEY)) {
        throw "Required Compose master-key environment value is absent; supply it without printing it"
    }
    if (Test-Path -LiteralPath $smokeTempParent) {
        throw "Dedicated smoke temp parent already exists: $smokeTempParent"
    }
    $null = New-Item -ItemType Directory -Path $smokeTempParent -ErrorAction Stop
    $smokeTempWasClaimed = $true

    $env:COMPOSE_PROJECT_NAME = $composeProject
    $env:TEMP = $smokeTempParent
    $env:TMP = $smokeTempParent

    $composeWasClaimed = $true
    docker compose up -d --build kubo gateway
    if ($LASTEXITCODE -ne 0) { throw "Compose startup failed" }

    $deadline = [DateTime]::UtcNow.AddMinutes(3)
    do {
        $kuboHealth = (& docker inspect --format '{{.State.Health.Status}}' ipfs-s3-kubo 2>$null | Out-String).Trim()
        $gatewayHealth = (& docker inspect --format '{{.State.Health.Status}}' ipfs-s3-gateway 2>$null | Out-String).Trim()
        if ($kuboHealth -eq "healthy" -and $gatewayHealth -eq "healthy") { break }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($kuboHealth -ne "healthy" -or $gatewayHealth -ne "healthy") {
        docker compose ps
        throw "Compose readiness deadline expired: kubo=$kuboHealth gateway=$gatewayHealth"
    }

    $health = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:9000/health" -TimeoutSec 5
    if ($health.StatusCode -ne 200 -or $health.Content.Trim() -cne "OK") {
        throw "Gateway /health did not return HTTP 200 with body OK"
    }
    docker compose ps
    if ($LASTEXITCODE -ne 0) { throw "Compose status inspection failed" }

    cargo test --test e2e -- --nocapture --test-threads=1
    if ($LASTEXITCODE -ne 0) { throw "Compose E2E suite failed" }

    $smokeOutput = @(& pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run 2>&1)
    $smokeExit = $LASTEXITCODE
    $smokeOutput | ForEach-Object { $_ }
    if ($smokeExit -ne 0) { throw "Runnable Docker client smoke exited $smokeExit" }
    if (@($smokeOutput | Where-Object { $_ -match '^\[RESULT\] client=\S+ status=FAILED\b' }).Count -ne 0) {
        throw "Runnable Docker client smoke reported FAILED"
    }
    if (@($smokeOutput | Where-Object { $_ -match '^\[RESULT\] client=\S+ status=PASSED\b' }).Count -eq 0) {
        throw "Runnable Docker client smoke produced no client PASS"
    }
} catch {
    $bodyError = $_
} finally {
    try {
        if ($composeWasClaimed) {
            docker compose down -v --remove-orphans
            if ($LASTEXITCODE -ne 0) {
                $cleanupFailures.Add("Owned Compose cleanup failed") | Out-Null
            }
        }

        if ($smokeTempWasClaimed -and (Test-Path -LiteralPath $smokeTempParent)) {
            try {
                $canonicalParent = [IO.Path]::GetFullPath($smokeTempParent)
                $expectedParent = [IO.Path]::GetFullPath((Join-Path $baseTemp $composeProject))
                if (-not $canonicalParent.Equals($expectedParent, [StringComparison]::OrdinalIgnoreCase)) {
                    $cleanupFailures.Add("Refusing cleanup outside the dedicated smoke temp parent: $canonicalParent") | Out-Null
                } else {
                    Remove-Item -LiteralPath $canonicalParent -Recurse -Force
                }
            } catch {
                $cleanupFailures.Add("Dedicated smoke temp cleanup failed: $($_.Exception.Message)") | Out-Null
            }
        }

        if ($composeWasClaimed) {
            foreach ($containerName in @("ipfs-s3-kubo", "ipfs-s3-gateway")) {
                docker container inspect $containerName *> $null
                if ($LASTEXITCODE -eq 0) {
                    $cleanupFailures.Add("Owned Compose container remains: $containerName") | Out-Null
                }
            }
            foreach ($resource in @(
                @{ Kind = "volume"; Name = "${composeProject}_ipfs_data" },
                @{ Kind = "volume"; Name = "${composeProject}_gateway_data" },
                @{ Kind = "network"; Name = "${composeProject}_default" }
            )) {
                & docker $resource.Kind inspect $resource.Name *> $null
                if ($LASTEXITCODE -eq 0) {
                    $cleanupFailures.Add("Owned namespaced Docker resource remains: $($resource.Kind) $($resource.Name)") | Out-Null
                }
            }
        }
        if ($smokeTempWasClaimed -and (Test-Path -LiteralPath $smokeTempParent)) {
            $cleanupFailures.Add("Dedicated smoke temp parent remains after cleanup: $smokeTempParent") | Out-Null
        }
    } catch {
        $cleanupFailures.Add("Unexpected cleanup failure: $($_.Exception.Message)") | Out-Null
    } finally {
        try {
            $value = if ($composeProjectWasSet) { $previousComposeProject } else { $null }
            [Environment]::SetEnvironmentVariable(
                "COMPOSE_PROJECT_NAME",
                $value,
                [EnvironmentVariableTarget]::Process
            )
        } catch {
            $cleanupFailures.Add("COMPOSE_PROJECT_NAME restoration failed: $($_.Exception.Message)") | Out-Null
        }
        try {
            $value = if ($tempWasSet) { $previousTemp } else { $null }
            [Environment]::SetEnvironmentVariable("TEMP", $value, [EnvironmentVariableTarget]::Process)
        } catch {
            $cleanupFailures.Add("TEMP restoration failed: $($_.Exception.Message)") | Out-Null
        }
        try {
            $value = if ($tmpWasSet) { $previousTmp } else { $null }
            [Environment]::SetEnvironmentVariable("TMP", $value, [EnvironmentVariableTarget]::Process)
        } catch {
            $cleanupFailures.Add("TMP restoration failed: $($_.Exception.Message)") | Out-Null
        }
    }
}

if ($null -ne $bodyError) {
    if ($cleanupFailures.Count -ne 0) {
        throw "$($bodyError.Exception.Message); cleanup failures: $($cleanupFailures -join '; ')"
    }
    throw $bodyError
}
if ($cleanupFailures.Count -ne 0) {
    throw "Regression cleanup failures: $($cleanupFailures -join '; ')"
}
```

Expected GREEN evidence:

1. `ipfs-s3-kubo` and `ipfs-s3-gateway` both become `healthy` within three minutes.
2. `GET http://127.0.0.1:9000/health` returns HTTP 200 and exact body `OK`.
3. `cargo test --test e2e -- --nocapture --test-threads=1` exits 0 with all 11 E2E tests passing.
4. `scripts/client-smoke.ps1 -Client All -Run` exits 0, emits no `FAILED`, and emits at least one `PASSED`; unavailable local client images may be accurately `SKIPPED` and must not be pulled separately.
5. `docker compose down -v --remove-orphans` removes only the unique project's stack and namespaced resources; the block confirms their absence, removes only the dedicated smoke temp parent, and restores `COMPOSE_PROJECT_NAME`, `TEMP`, and `TMP` before surfacing failures.

- [ ] **Step 2: Confirm real-surface verification preserved source scope**

```powershell
$allowedTrackedChanges = @(
    "src/pinning/pinata.rs",
    "src/pinning/worker.rs",
    "src/store/pinning/leases.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs"
)
$changedTracked = @(git diff --name-only)
$unexpectedTracked = @($changedTracked | Where-Object { $_ -notin $allowedTrackedChanges })
if ($unexpectedTracked.Count -ne 0) {
    $unexpectedTracked | ForEach-Object { $_ }
    throw "Real-surface verification changed tracked files outside scope"
}
git status --short
```

Expected: no tracked modification outside the five Rust files. Compose/temp ownership and environment restoration were already proved by Step 1. The design and plan may remain untracked; do not stage them.

---

### Task 7: Remove the two exact debug-session artifacts last

**Files:**
- Delete: `.debug-journal.md`
- Delete: `C:\Users\hugefiver\AppData\Local\Temp\opencode\dw-20260728-124536-47cef3.md`

**Interfaces:**
- Consumes: passing Tasks 1-6 and the two exact artifact paths recorded by the debugging session.
- Produces: no session debug journal/notepad, while all product source and untracked design/plan remain available for review.

- [ ] **Step 1: Delete only the two recorded artifacts**

Use exact literal paths with no wildcard and no recursive parent deletion:

```powershell
Remove-Item -LiteralPath ".debug-journal.md" -Force -ErrorAction Stop
Remove-Item -LiteralPath "C:\Users\hugefiver\AppData\Local\Temp\opencode\dw-20260728-124536-47cef3.md" -Force -ErrorAction Stop
```

Expected: both exact files are removed; no directory and no other temp file is removed.

- [ ] **Step 2: Verify artifact absence and final five-file scope**

```powershell
if (Test-Path -LiteralPath ".debug-journal.md") { throw "Debug journal cleanup failed" }
if (Test-Path -LiteralPath "C:\Users\hugefiver\AppData\Local\Temp\opencode\dw-20260728-124536-47cef3.md") {
    throw "Deepwork notepad cleanup failed"
}
$allowedTrackedChanges = @(
    "src/pinning/pinata.rs",
    "src/pinning/worker.rs",
    "src/store/pinning/leases.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs"
)
$unexpectedTracked = @(git diff --name-only | Where-Object { $_ -notin $allowedTrackedChanges })
if ($unexpectedTracked.Count -ne 0) {
    $unexpectedTracked | ForEach-Object { $_ }
    throw "Final tracked scope contains an unapproved file"
}
git status --short
```

Expected: both artifacts are absent; tracked changes are limited to the five approved Rust files; the design and this plan remain untracked; no staging or commit occurs.

---

## Requirement-to-Task Coverage

| Latest design/spec requirement | Plan coverage |
|---|---|
| Unique renewal owner `object-renewal-order` retained | Task 1 Steps 2 and 4 |
| Worker diagnostic timeout retained at 10 seconds without flow/message change | Task 2 Steps 2-4 |
| V3 mock delay 200 ms→2 s and upload timeout 20 ms→1 s | Task 3 Step 2 |
| Legacy mock delay 200 ms→2 s and upload timeout 20 ms→1 s | Task 3 Step 3 |
| Preserve ambiguous error, `find`, request ID/status, and exactly one POST | Task 3 Steps 4 and 6 |
| Pinata exact tests, 64-thread module, ten-run module stress | Task 3 Steps 4-5 |
| New deterministic SQLite blocker test is RED before production change | Task 4 Steps 1-2 |
| Fixed helper name/signature and SQLite idempotent bucket-owner update | Task 4 Steps 3-4 |
| Non-SQLite no-op, DB error propagation, no `rows_affected` check | Task 4 Step 4 exact helper |
| Pure validation before write intent; write intent before any DB read | Task 4 Step 5 and placement audit Step 9 |
| Preserve missing-bucket semantics and PostgreSQL behavior | Global Constraints; Task 4 Step 4 |
| Do not change retries, busy timeout, schema, API, dependencies, or production timeout | Global Constraints; Task 4 Step 9; Task 5 scope audit |
| New SQLite test plus both existing concurrent tests | Task 4 Steps 6-7 |
| Publication concurrent stress at least twenty runs | Task 4 Step 8 |
| Formatting and all targeted gates | Task 5 Steps 1-2 |
| Default full library suite repeated five times and single-thread control | Task 5 Steps 3-4 |
| Integration and PowerShell infrastructure smoke | Task 5 Steps 5-6 |
| Strict Clippy and diagnostics for all five changed Rust files | Task 5 Steps 7-8 |
| Diff scope limited to the five approved Rust files | Task 5 Step 9; Task 6 Step 2; Task 7 Step 2 |
| Compose health, 11 E2E tests, at least one client PASS, no client FAIL | Task 6 Step 1 |
| Cleanup only owned Compose/temp resources and restore environment | Task 6 Step 1 |
| Delete the two exact debug artifacts last | Task 7 |
| No Git write operation and planning artifacts remain untracked | Global Constraints; all tasks omit commit/stage steps |

## Plan Self-Review

- **Specification coverage:** PASS — all four confirmed causes, both already implemented fixes, Pinata test-only timing changes, SQLite production root fix, deterministic TDD test, stress/full/real-surface gates, and final cleanup map to explicit tasks.
- **Placeholder scan:** PASS — no deferred implementation marker or “similar to above” instruction remains; every code-changing step includes an exact replacement block or complete implementation.
- **Type/name consistency:** PASS — `acquire_sqlite_publication_write_intent<C: ConnectionTrait>(db: &C, bucket_name: &str) -> AppResult<()>`, `sqlite_publication_waits_for_existing_writer_before_starting_read_snapshot`, both Pinata test names, and both existing publication concurrency names are consistent in code, commands, and coverage.
- **TDD/order review:** PASS — Task 4 adds and runs the deterministic test against old production code before adding the helper; targeted GREEN and stress follow implementation; Task 5 and Task 6 are ordered after focused work; artifact deletion is last.
- **PowerShell review:** PASS — commands use PowerShell variable/environment/error syntax, avoid Bash operators, preserve `RUST_BACKTRACE`, restore Compose/temp environment, and do not expose credential values.
- **File-scope review:** PASS — implementation is limited to exactly five Rust files; Cargo/config/schema/other docs remain untouched; only two exact debug artifacts are deleted after verification.
- **Git review:** PASS — the plan contains read-only `git diff`/`git status` checks and no stage, commit, push, tag, reset, checkout, or other Git write command.
- **Suite-count consistency:** PASS — the source baseline has 527 library tests; Task 4 adds one, so post-change full-suite expectations correctly require 528 total while explicitly proving all 527 pre-existing tests pass.

Receipt status: `[OKAY-UNAMBIGUOUS]` for this saved revision.
