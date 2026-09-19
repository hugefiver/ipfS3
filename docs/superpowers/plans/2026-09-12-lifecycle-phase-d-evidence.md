# Lifecycle Phase D — implementation and evidence

Date: 2026-09-12. Scope: Phase D only, on top of the existing Phase C working tree.
Local implementation/regressions passed. **Real dual Kubo and real PostgreSQL: NOT RUN.**
This is not permission to enable Transition or mark the C–F release gate complete.

## Protocol rulings

Pinned official implementation evidence:

- [Kubo v0.43.0 dag/export](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/dag/export.go): POST `arg=<CID>&offline=true&progress=false`. Export traverses the original DAG as CAR; do not use `local-only=true`, which can omit missing blocks. No cat/add reconstruction, decryption or CID recomputation.
- [dag/import](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/dag/import.go) and [response structs](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/dag/dag.go): multipart file upload, `pin-roots=true`, `stats=true`, `encoding=json`, `stream-channels=true`; fast-provide options are disabled. Require exactly one `Root.Cid` IPLD link (`{"/":"<CID>"}`), an explicitly empty `PinErrorMsg`, one Stats record and clean response EOF. Unexpected/error records, missing roots, multiple roots and failed pins fail closed.
- Kubo's importer calls `gocarv2.NewBlockReader(reader)` without trusted-CAR options. Its pinned [go-car v2.17.0 block reader](https://github.com/ipld/go-car/blob/v2.17.0/v2/block_reader.go) validates block bytes against their CIDs by default. This is not a substitute for complete-DAG verification: import allows roots that were not in the CAR and reports recursive pin failures separately.
- [go-ipfs-cmds v0.16.1 HTTP response emitter](https://github.com/ipfs/go-ipfs-cmds/blob/v0.16.1/http/responseemitter.go): late failures use `X-Stream-Error` trailers, including after HTTP 200. The implementation examines HTTP body frames and trailers, not just `bytes_stream()` or status. [reqwest v0.13.4 Response-to-Body](https://github.com/seanmonstar/reqwest/blob/v0.13.4/src/async_impl/response.rs) and [Body frame forwarding](https://github.com/seanmonstar/reqwest/blob/v0.13.4/src/async_impl/body.rs) preserve these frames; no additional dependency is required.
- [Kubo files/stat](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/files.go): use `/ipfs/<CID>`, `with-local=true`, `offline=true`, require matching parsed Hash, `WithLocality=true` and `Local=true`. This complete local DAG proof is bracketed by targeted `pin/ls?type=recursive&offline=true` and node identity checks. A pin listing alone is not a DAG proof; ordinary online cat is not a local proof. The combined targeted checks avoid an unrelated all-pins `pin/verify` scan.
- CID comparisons use parsed full CID identity, including version/codec/hash. Public stored CID/ETag spelling is never rewritten. No provider pin result is used as local Kubo evidence.
- Different peer IDs are necessary, but do not prove independent backing volumes. Kubo RPC cannot inspect deployment volume topology. Independent repos/volumes and swarm-isolated readability remain mandatory real-environment acceptance evidence, **NOT RUN**, not inferred from mock success.

## Delivered boundaries

- Existing `Config.kubo`, `[kubo]`, `IPFS_S3_KUBO_RPC_URL` and `AppState.kubo` remain hot. Optional `Config.cold_kubo` / `[cold_kubo].rpc_url` / `IPFS_S3_COLD_KUBO_RPC_URL` construct `AppState.cold_kubo`. URL validation emits fixed errors, not credentials or endpoint strings. Unavailable optional cold does not block startup.
- `residency::router::TierClients` selects the client from the resolved immutable version residency, not from a key/CID lookup or a remote provider. Cold requires verified receipt/CID/node binding; absent cold, invalid evidence or node replacement fail closed. Pending/legacy hot retains the existing non-probing behavior. Verified hot receipts are also checked against the configured node identity before reuse by the reader.
- `stream_copy_verified` verifies source local DAG/pin and distinct/bound identities, streams CAR with backpressure to import, requires both export and full multipart upload EOF on every success path, validates the complete bounded import response, rechecks identities and verifies destination local DAG/pin. Upload progress renews an idle watchdog; there is no total-duration timeout. Export and response waits are also idle-bounded. Cancellation drops the in-flight I/O; no detached copy task, full CAR buffer, full temporary file, unpin or GC is used.
- GET/HEAD and COPY preserve immutable version identity through selection. Plain, SSE-S3 and SSE-C (fingerprint and legacy), full/Range and explicit version/null reads use the selected client. Initial cat failures return S3 errors before response construction. Late body failures remain stream errors. Legacy SSE-C authentication drains instead of collecting the full plaintext; encrypted Range decrypts the full stream and slices with bounded memory, withholding its final selected chunk until tail authentication completes.
- Plain Range uses Kubo `offset=start&length=end-start`; both the unit fixture and shared integration Kubo fixture now implement the actual API.
- Cold COPY performs verified cold→hot CAR copying before the existing publication transaction creates a new hot STANDARD version. The receipt is consumed under the existing ownership/version/lease/hot-frontier fence: pending/failed hot rows can become verified, identical verified bindings can be reused, and conflicting bindings are rejected atomically. Source residency, CID, ETag, envelope and public version ID remain unchanged. A network receipt alone is not database publication or a lifecycle checkpoint.
- Audited ZIP/import: direct ZIP archive and multipart-completed ZIP roots are read before publication; import artifacts are also read before publication. These remain hot. Published archives/extracted entries use normal object reads. Multipart parts remain hot. Pinata/provider lease reads remain hot/provider semantics.
- Existing hot backfill wiring and `/health` liveness stay unchanged. Lifecycle API still rejects Transition. No saga, lifecycle scheduling, IA reporting, public config examples, Compose, README or ROADMAP changes are included.

## Changed files attributable to D

Behavior and focused tests:

- `src/config.rs`, `src/state.rs`, `src/error.rs`
- `src/residency/mod.rs`, new `src/residency/router.rs`
- `src/kubo/mod.rs`, `src/kubo/cat.rs`, `src/kubo/verification.rs`, new `src/kubo/health.rs`, new `src/kubo/tier_copy.rs`
- `src/s3/ops/object.rs`
- `tests/integration.rs`, `tests/support/mod.rs`, `tests/support/decompress.rs`, new `tests/support/tier_reads.rs`
- This internal evidence file.

Additional `cold_kubo: None` constructor-only adaptations:

- `src/main.rs`, `src/cors/http.rs`, `src/lifecycle/config.rs`, `src/zip/extract.rs`
- `src/import/decompress/tests.rs`, `src/import/pipeline.rs`, `src/import/worker.rs`
- `src/s3/ops/{bucket,cors,lifecycle,multipart,tagging,versioning}.rs`
- `src/s3/route/{decompress_zip,gateway}.rs`, `src/s3/route/import_object/tests.rs`
- `tests/postgres_versioning.rs`, `tests/support/{cors,import,lifecycle,pinning}.rs`

Pre-existing Phase C schema/store/backfill changes remain in the working tree and are not newly attributed to D. No Git write operations were performed.

## Verification

TDD evidence includes missing cold-config/router interfaces, failing cold read fixtures before integration, transport failure/EOF/trailer/cancellation cases, and the old shared integration Range fixture exposing `0123` instead of requested `2345` until changed to offset/length.

Final commands/results:

| Command/suite | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo test --offline --lib --test integration --test residency --test residency_publication --test cors --quiet` | PASS for executed tests; see counts below |
| lib | 1042 passed, 0 failed, 0 ignored |
| integration | 149 passed, 0 failed, 0 ignored |
| residency | 10 passed, 0 failed, 0 ignored |
| residency_publication | 5 passed, 0 failed, **1 ignored (real PostgreSQL)** |
| cors | 8 passed, 0 failed, 0 ignored |
| `cargo clippy --offline --all-targets -- -D warnings` | PASS; dependency future-incompatibility notice for existing `proc-macro-error2 v2.0.1` |
| `git diff --check` | PASS |
| Protected public files diff | Empty |

The new signed tier fixture drives actual axum+s3s SigV4 HTTP: immutable historical cold vs current hot, current/explicit null cold, nonzero plain range bytes/headers, ETag/version ID, redacted cold failure XML and no hot fallback. Kubo and database backends in this fixture are wiremock and SQLite, **not real Kubo/PG**. Direct handler tests additionally cover cold SSE-S3/SSE-C full/Range, legacy SSE-C HEAD, faithful cold COPY and failure without publication. Raw local HTTP fixtures test CAR error trailers, early import replies, stream/header stalls, cancellation and bounded backpressure.

Environment preflight: no configured PG or dual-Kubo test endpoints; `docker ps` returned no running containers; no `ipfs` or AWS CLI on PATH. No infrastructure was started and no software was installed. Real dual-Kubo layouts/raw leaves/empty/multiblock/ciphertext/CID preservation, swarm isolation, memory-budget measurement and restart acceptance remain **NOT RUN**. Real PG remains **NOT RUN**, including the ignored publication case.

## Phase E interfaces and obligations

```rust
TierClients { hot: &state.kubo, cold: state.cold_kubo.as_ref() }
    .client_for_tier(tier) -> AppResult<&KuboClient>
    // async, takes an immutable residency snapshot:
    .resolve_read_source(&residency) -> AppResult<KuboClient>

KuboClient::local_node_identity() -> AppResult<String> // async
KuboClient::verify_local_residency(cid)
    -> AppResult<LocalResidencyVerificationReceipt> // async

stream_copy_verified(
    source: &KuboClient,
    destination: &KuboClient,
    cid: &str,
    expected_source_node: Option<&str>,
    expected_destination_node: Option<&str>,
    cancel: &CancellationToken,
) -> AppResult<LocalResidencyVerificationReceipt> // async
```

`TierError` has safe classifications for cold-not-configured, unavailable, CID mismatch, incomplete local copy, node identity mismatch, same node and cancellation. S3 XML exposes only the fixed internal storage backend error.

Cold COPY additionally uses `publish_standard_object_with_hot_receipt` (existing `PublicationRequest` and `StandardMutationGuard`, plus `LocalResidencyVerificationReceipt`). It validates CID/node evidence and consumes it within the existing fenced publication transaction; it is not an IA publication API.

E must bind receipts to saga/action/source residency revision and expected node identities, renew/fence claims and cancel I/O on fence loss, revalidate ownership/rules/winners before atomic publication, and reverify after restart before publishing. A successful transport receipt is **not** a durable transition checkpoint, database publication or authorization to release another owner's reference. Only E's fenced transaction can publish IA; cleanup is logical references only, never `pin_rm`. IA response/list reporting and accepting Transition remain deferred to E after its full prerequisites.

## Phase D review corrections (same session)

All six Important findings and the Minor evidence gap were verified against code/API and regression tests. No Transition or release scope was opened.

1. **Separate export/multipart EOF:** the previous early-response branch checked export EOF only. `ensure_complete_transfer` is now applied to every success path and requires both states. `early_success_after_export_eof_without_multipart_closing_boundary_is_incomplete` covers completed export with an unfinished multipart body; the initial RED completion-state regression returned success, and the final raw HTTP regression rejects the early success response.
2. **Upload inactivity, not total duration:** `execute_import_with_idle_watchdog` observes non-empty multipart frames consumed by reqwest, renewing the deadline only on progress. After upload EOF it bounds the response-header wait. A caller without external cancellation now fails on stalled upload progress. `stalled_in_flight_destination_times_out_without_external_cancellation` was RED (`Elapsed(())`) and is GREEN. `active_upload_longer_than_idle_timeout_succeeds` sends 8 MiB through a slow reader and proves transfer duration exceeds one idle interval while completing successfully. Body-frame demand is the available reqwest transport progress signal, not a claim about remote durable storage.
3. **Receipt-fenced hot publication:** the receipt is no longer discarded. Publication validates it, locks the existing hot frontier and applies `apply_hot_publication_verification` before object/version writes; existing verified node/receipt conflicts fail closed instead of being rebound. The foreign-hot-node regression previously published `destination-object`; it now rolls back. Matching-node COPY→GET succeeds and uses verified hot. Receipt mismatch, identical reuse, pending-row attach and forced late rollback have store tests. The existing lock order is retained; real PostgreSQL concurrent validation remains NOT RUN.
4. **Cold reads cannot use swarm repair:** selected cold clients carry a request-scoped local-read policy and cat adds `offline=true`; hot/temp/provider clients do not. Official [Kubo v0.43 GetApi](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/cmdenv/env.go) reads the global option and [CoreAPI.WithOptions](https://github.com/ipfs/kubo/blob/v0.43.0/core/coreapi/coreapi.go) replaces the exchange/blockservice/DAG with an offline exchange. [cat](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/cat.go) → [Unixfs.Get](https://github.com/ipfs/kubo/blob/v0.43.0/core/coreapi/unixfs.go) uses that DAG for roots and linked/raw leaves. The pinned [Boxo v0.42.1 offline exchange](https://github.com/ipfs/boxo/blob/v0.42.1/exchange/offline/offline.go) accesses only the local blockstore and errors on a miss. Bare CID requests do not resolve mutable IPNS names. The signed cold flag assertion was RED (`cold cat must not fetch missing blocks from swarm`), then GREEN. Plain, SSE-S3, fingerprinted SSE-C and legacy SSE-C missing-local/online-available fixtures now fail safely; encrypted Range still uses full offline ciphertext cat with no offset/length.
5. **Fixed-length GET final-byte gate:** new `object_body::finish_get_body` holds one final response byte until clean upstream EOF, validates the expected byte count, and drains zero-length bodies before response construction. It does not collect object/range contents. Real axum+s3s SigV4 tests send all expected Kubo bytes, hold the terminal chunk behind a handshake, then send `X-Stream-Error`. Before the fix, plain full/Range, SSE-S3 full, fingerprinted SSE-C full and empty SSE-C reported successful completion before trailer release. They now fail without a complete successful body. Existing encrypted Range tail authentication and legacy SSE-C first-read authentication already rejected this failure; those protections remain and the shared response boundary also covers their actual GET bodies. Clean delayed EOF remains successful.
6. **In-flight test evidence:** cancellation/backpressure tests spawn and concurrently poll the COPY future, await a raw destination handshake proving receipt of actual distinctive CAR bytes after request headers, assert nonzero source transfer, then cancel or stop reading. Cancellation joins within 2 seconds; stopped consumption causes bounded idle failure while the 256 MiB source remains incompletely buffered. These replace the prior vacuous pre-poll timing assumptions. Test-owned servers/tasks are aborted/joined at completion; no production tasks are detached.
7. **Minor publication evidence:** `cold_copy_transport_failures_leave_no_destination_publication_rows` injects export, import and destination pin-verification failures, checks object/version/residency/reference row counts unchanged, destination absent and source still present. The same assertions cover foreign verified-hot bindings. This is SQLite/mock evidence, not real Kubo/PG evidence.

Additional changed files for this correction:

- `src/kubo/{client,cat,tier_copy}.rs`, `src/residency/router.rs`
- `src/s3/ops/{mod,object}.rs`, new `src/s3/ops/object_body.rs`, new `src/s3/ops/object/tests/copy_hot_receipt_tests.rs`
- `src/store/pinning/publication.rs`, new `src/store/pinning/publication/hot_receipt.rs`, `src/store/pinning/publication/tests.rs`, new `src/store/pinning/publication/tests/hot_receipt_tests.rs`, `src/store/residency/references.rs`
- `tests/integration.rs`, `tests/support/tier_reads.rs`, new `tests/support/tier_reads/eof.rs`
- This evidence file.

Latest full validation used the commands in the table above: lib **1042**, integration **149**, residency **10**, residency_publication **5 + 1 PG ignored**, cors **8**; fmt, all-target clippy and diff-check passed. Docker still has no running containers and PG/dual-Kubo endpoints remain unconfigured, so real dual Kubo/swarm-isolation and real PG are **NOT RUN**. No software installation, Git writes, public config/Compose/README/ROADMAP modifications, full-object/CAR collection or unpin was added.
