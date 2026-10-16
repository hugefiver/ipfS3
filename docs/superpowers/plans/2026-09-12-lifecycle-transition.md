# Lifecycle transition delivery requirements

## Scope

The [lifecycle program](../specs/2026-08-26-lifecycle-program-design.md) defines current and noncurrent `STANDARD -> STANDARD_IA` transitions to an independent cold Kubo. Expiration, incomplete multipart abort, versioning and CORS retain their own contracts. Transition doesn't include archive/restore, direct IA writes, reverse lifecycle, remote-provider tiers, physical unpin or GC.

The dated single-run acceptance receipt is [Lifecycle transition F1](../../../tests/results/lifecycle-transition/CURRENT.md). It certifies its frozen inputs and tested topology, not later changes or other platforms. [Testing](../../testing.md) describes current verification entry points.

## Immutable residency and publication

Each immutable content version has its own primary residency and class. Physical content is deduplicated by tier/CID, but references retain distinct owners. Delete markers and incomplete multipart uploads don't receive content residency. Public version IDs, stored CID/ETag spelling, object age, tags and encryption envelopes remain unchanged by transition.

New content publishes to hot STANDARD. Hot configuration retains `[kubo]`, `IPFS_S3_KUBO_RPC_URL` and `AppState.kubo`; cold is optional through `[cold_kubo].rpc_url`, `IPFS_S3_COLD_KUBO_RPC_URL` and `AppState.cold_kubo`. Cold unavailability doesn't prevent unrelated startup or expiration/abort work.

Schema upgrades preserve existing lifecycle actions, revision tombstones, cursor state and immutable identities. Legacy STANDARD backfill is initially unverified: SQL metadata alone doesn't prove local bytes. Verification uses bounded, lease-fenced progress and attaches references only while the exact version still exists. It must not revive a deleted version. A valid transition waiting for hot verification keeps its conflict priority and resumes under the same configuration revision without exhausting ordinary action attempts.

All publication, deletion, promotion and reference changes share the existing ownership/version/lease fences. Residency frontiers are acquired in stable order after those fences. Network copying runs outside transactions. Destructive downgrade is refused when IA, unfinished sagas or residency state can't be preserved.

## Exact-DAG copy protocol

Re-adding `cat` bytes can't preserve arbitrary imported DAGs. Copy streams the original CAR with backpressure, without decryption, a complete CAR buffer or a complete temporary file.

The protocol identities and checks are:

- [Kubo v0.43.0 dag/export](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/dag/export.go): POST with `arg=<CID>`, `offline=true`, `progress=false`. `local-only=true` isn't a complete-DAG proof.
- [Kubo v0.43.0 dag/import](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/dag/import.go): multipart file upload with `pin-roots=true`, `stats=true`, `encoding=json`, `stream-channels=true`. Require exactly one expected `Root.Cid` IPLD link, explicitly empty `PinErrorMsg`, one Stats record and clean response EOF. Unexpected/error records fail closed. Both export and complete multipart-upload EOF are required, even if the destination responds early.
- [go-car v2.17.0 block reader](https://github.com/ipld/go-car/blob/v2.17.0/v2/block_reader.go) validates block bytes against CIDs by default. This doesn't establish complete-DAG availability or successful recursive pinning.
- [go-ipfs-cmds v0.16.1 response emitter](https://github.com/ipfs/go-ipfs-cmds/blob/v0.16.1/http/responseemitter.go) can report late failures through `X-Stream-Error` trailers after HTTP 200. Consume body frames and trailers, not just status or a bytes-only stream.
- [Kubo v0.43.0 files/stat](https://github.com/ipfs/kubo/blob/v0.43.0/core/commands/files.go): `/ipfs/<CID>`, `with-local=true`, `offline=true`, matching parsed Hash, `WithLocality=true`, `Local=true`. Bracket this with targeted recursive offline pin checks and node identity checks. Neither pin listing alone nor online cat proves a complete local DAG.

CID comparison uses full parsed identity, including version, codec and hash. Receipts bind CID and node identity; different peer IDs alone don't prove separate repositories or volumes. Source and destination require independent backing storage and swarm-isolated readability evidence. Copy progress is idle-bounded, not subject to a whole-transfer deadline. Cancellation or fence loss drops in-flight I/O without detached tasks.

## Durable saga and eligibility

`prepare -> copy -> verify -> publish -> cleanup` checkpoints survive I/O success followed by a missing database commit. Lifecycle actions remain separate from provider `pin_jobs`. Durable state retains the exact target, source residency revision, tier/CID, expected nodes, verification/publication receipts and ownership generation.

Claims and renewal use database time, worker identity, epoch and an unexpired lease. Expired workers can't renew, publish, complete another claim or clear another owner's guard. Before publication, repeat configuration revision, enabled rule, exact target, filter/tags/size, current/noncurrent state, age/count threshold, residency and conflict-winner checks. Actual permanent deletion outranks transition, which outranks current marker creation.

Publication atomically attaches verified cold residency, changes only the selected version's primary/class and stores its receipt. Before publication, invalidated work cancels safely. After publication, replay completes logical cleanup from the receipt even if the policy changes; it doesn't reverse the committed class. Retry exhaustion must retain a visible, recoverable cleanup obligation rather than discard published state.

Cleanup releases only this action's logical references. Shared versions, active leases, provider targets, legacy/unknown references and other staging holds remain protected. Even zero references don't authorize `pin_rm`, GC or block removal. Extra retained storage is a real constraint, not promised reclaimed space.

## S3 reads and rules

Configuration replacement is atomic. Only STANDARD_IA is a transition destination. Without cold configuration, a transition PUT is rejected without modifying the prior configuration; expiration/abort-only rules remain usable. Temporary cold failure doesn't make an already-valid configuration syntactically invalid. No hidden minimum size, residence period or AWS billing policy is introduced.

GET, HEAD, Range, COPY and all class-bearing object/version listings use the same selected immutable residency snapshot. Cold reads are offline and never fall back to hot or swarm repair. Plain Range uses Kubo `offset=start&length=end-start`. Encrypted Range retains full-stream authentication and slicing; this isn't chunk-level encrypted Range support.

Cold-source COPY verifies a faithful cold-to-hot transfer before publishing a new STANDARD destination. The receipt is consumed under the hot publication frontier; conflicting node bindings roll back. Multipart parts and unpublished ZIP/import artifacts remain hot.

Fixed-length GET withholds its final byte until clean upstream EOF and validates the byte count; zero-length bodies drain before response construction. This prevents a late trailer failure from appearing as a complete successful body. Public errors and asynchronous diagnostics redact backend details and secrets.

## Acceptance and operations

Acceptance covers SQLite and real PostgreSQL fresh/upgrade migrations, signed axum+s3s API/XML, version/class/listing consistency, shared-CID ownership, exact-DAG encrypted/plain/empty/multiblock copies, and no-unpin request auditing. Real dual-Kubo proof includes distinct backing volumes, offline cold integrity, hot-stop readability and cold-stop failure without fallback. Large-CAR measurement must distinguish gateway memory from client, proxy and Kubo memory.

Process-kill/restart evidence covers every checkpoint, successful I/O before checkpoint persistence, publication before cleanup, lease takeover, retry exhaustion, long-copy renewal and stale-worker fencing. Database class, local bytes and signed S3 responses must agree. A skipped or ignored test isn't a runtime pass; mock and compilation results don't replace real PostgreSQL/Kubo/client evidence.

Before enabling transition, all serving and worker instances must understand residency and share the same tier bindings. Old readers always selecting hot and old writers bypassing residency can't safely coexist with enabled transition. Stopping new rules doesn't remove committed IA: keep the cold configuration/data, tier-aware reader and schema. Migration back to hot or downgrade requires its own design. This delivery makes no physical-space-reclamation, full-stack HA or cross-platform validation claim.
