# Atomic v0.6 Object Versioning Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver bucket enable/suspend state, public object versions, `ListObjectVersions`, and delete markers as one atomic v0.6 feature without exposing legacy non-latest rows or changing content, encryption, pinning, authentication, deployment, or package-version contracts.

**Architecture:** Add an additive `object_versions` public index beside immutable `objects`, while retaining `objects.is_latest` as the current-content projection used by ordinary S3 reads and listings. Centralize every content publication and simple/exact deletion in the existing pinning publication transaction so version-index state, current projection, mutation fences, tags, leases, quota, and jobs move atomically on SQLite and PostgreSQL. Use the locked s3s 0.14 trait and DTO surface for bucket versioning and version listing; keep marker errors, exact selection, pagination, and client evidence at the S3 boundary.

**Tech Stack:** Rust 2024 (MSRV 1.92), s3s 0.14.0, SeaORM/SeaORM Migration 1, SQLite, PostgreSQL 17, Tokio 1, Axum 0.8, reqwest 0.13, UUID v4, chrono 0.4, wiremock 0.6, PowerShell 7, Docker Compose v2.23.1+, AWS CLI container, Kubo, and the existing pinning/import/ZIP subsystems.

**Spec:** `docs/superpowers/specs/2026-08-25-object-versioning-design.md` (approved SHA-256 `56e53b0983ac9f244c5379a50454c6f7e64ab4105ceccc45f9c5ed5cf34ef489`)

**Global Constraints:**
- The selected design keeps the current S3, encryption, and pinning boundaries intact.
- No public version identifier is an internal object identifier.
- `NULL` means **Unversioned**, not an unknown or partially migrated state.
- The only stored non-null values are the case-sensitive S3 values `Enabled` and `Suspended`.
- PutBucketVersioning accepts only `Enabled` and `Suspended`; it never restores `NULL`.
- The only public null version ID is the literal query and XML value `null`.
- It is never generated as a UUID and is never represented to clients as an empty string, database `NULL`, or the internal object ID.
- `objects` remains the authoritative immutable content record.
- It gains no public version ID.
- Tags, pin leases, targets, and remote-pin accounting remain owned by `objects.id`.
- A version-index row neither owns a lease nor creates a pin target.
- A marker has no content, CID, tag set, or lease.
- Legacy non-latest `objects` rows receive no index entry.
- They remain hidden from explicit reads, version listing, ordinary listings, tags, and deletion.
- No migration may infer public history from them.
- Every content-producing path must converge on a new version-aware publication transaction, replacing direct calls that write only the latest `objects` row.
- Kubo `pin/add` remains before metadata publication and there is still no compensating Kubo `pin/rm`.
- Version retention does not alter the no-pin-rm deletion policy.
- All marker responses use S3 error XML and the normal request identifiers.
- They do not call Kubo, accept Range, expose a CID, or attempt SSE-C authentication.
- Exact deletion never creates a replacement marker.
- Repeated entries are processed in request order, not deduplicated by key, because different version IDs can name different retained versions.
- Copy does not share an object ID, tag set, or lease with its source.
- All ordinary GetObject, HeadObject, ListObjects, and ListObjectsV2 queries must continue to select only `objects.is_latest = true`.
- No handwritten route or XML serializer may bypass s3s SigV4 routing and DTO serialization.
- Unversioned buckets return a valid empty ListObjectVersions result.
- The v0.6 implementation rejects MFA Delete configuration elements rather than ignoring them.
- Neither backend uses a read-then-write sequence outside the transaction to select the next sequence, latest state, or null slot.
- All public errors are S3 errors with redacted backend detail.
- README and ROADMAP may be updated only after all static, unit, integration, database, and required real-client evidence gates pass.
- The roadmap boxes are checked together in the same reviewed change because the three public behaviors are one atomic feature.
- MFA Delete configuration or MFA device verification.
- Lifecycle rules.
- CORS configuration.
- Object Lock, retention, or legal holds.
- Pin reclamation, Kubo `pin/rm`, garbage collection, or a content deletion lifecycle.
- Replication, cross-region version replication, restoration APIs, inventory, or event notifications.
- Exposing legacy non-latest `objects` rows as public history.
- Changing S3 authentication, encryption formats, ETag-as-CID behavior, or the existing tag and lease ownership boundary.
- It must not stage, commit, push, tag, or otherwise write Git history without separate explicit user authorization.

---

## Execution Protocol

- Execute tasks in order with a fresh implementation worker per task and integrate each returned change before starting the next task. Do not make any S3 versioning method reachable until its migration, store primitive, error mapping, and focused tests are present in the same integrated working tree.
- Use TDD for every behavior change: add the named failing test, run the narrow command and confirm the stated causal failure, write the minimum implementation, then rerun the narrow and neighboring regression commands.
- Do not stage or commit per task. The user's continuing authorization permits one integrated commit only after Task 13's final identity review succeeds; never push or tag.
- Preserve package `ipfs-s3-gateway` at version `0.1.0`, Rust edition 2024, MSRV 1.92, locked `s3s 0.14.0`, existing dependencies, and `Cargo.lock`. Do not add a dependency; use the existing `uuid`, `chrono`, `http`, and s3s APIs.
- Preserve ETag = CID, all current plaintext/SSE-S3/SSE-C formats and headers, streaming request bodies, full-decrypt encrypted ranges, SigV4 authentication, no Kubo `pin/rm`, and internal `objects.id` ownership of tags and pin lifecycle rows.
- Preserve the default SQLite, PostgreSQL, multi-gateway, Cluster, and private-swarm deployment files and behavior. The dedicated evidence Compose file is validation-only and must not modify or replace any production profile.
- If s3s 0.14.0 does not compile with the exact API contract below, or live AWS CLI behavior contradicts the approved marker/pagination contract, stop for a design revision. Do not add a custom route, custom XML, compatibility fallback, or partial public feature.
- Lifecycle, CORS, MFA Delete/device verification, Object Lock, retention/legal hold, pin reclamation/GC, replication/restoration/inventory/events, and exposure of legacy non-latest rows are outside this plan.

## Locked s3s 0.14.0 API Contract

The implementation must compile directly against the installed `s3s-0.14.0` source and use these exact DTO names and fields:

```rust
async fn get_bucket_versioning(
    &self,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>>;

async fn put_bucket_versioning(
    &self,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>>;

async fn list_object_versions(
    &self,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>>;
```

- `GetBucketVersioningInput { bucket, expected_bucket_owner }`; `GetBucketVersioningOutput { mfa_delete: Option<MFADeleteStatus>, status: Option<BucketVersioningStatus> }`.
- `PutBucketVersioningInput { bucket, checksum_algorithm, content_md5, expected_bucket_owner, mfa, versioning_configuration }`; `VersioningConfiguration { mfa_delete, status }`; `PutBucketVersioningOutput` has no feature fields. Reject a present request `mfa`, present configuration `mfa_delete`, absent `status`, or a status other than `BucketVersioningStatus::ENABLED`/`BucketVersioningStatus::SUSPENDED`.
- `ListObjectVersionsInput { bucket, delimiter, encoding_type, expected_bucket_owner, key_marker, max_keys, optional_object_attributes, prefix, request_payer, version_id_marker }`.
- `ListObjectVersionsOutput { common_prefixes, delete_markers, delimiter, encoding_type, is_truncated, key_marker, max_keys, name, next_key_marker, next_version_id_marker, prefix, request_charged, version_id_marker, versions }`.
- The content entry type is `ObjectVersion`, not `Version`: `ObjectVersion { checksum_algorithm, checksum_type, e_tag, is_latest, key, last_modified, owner, restore_status, size, storage_class, version_id }`. The field is `storage_class: Option<ObjectVersionStorageClass>`, while `ObjectVersionStorageClass::STANDARD` is `&'static str`; set `e_tag = ETag::Strong(cid)`, `storage_class = Some(ObjectVersionStorageClass::from_static(ObjectVersionStorageClass::STANDARD))`, and `owner = None` unless existing owner projection is explicitly reused.
- `DeleteMarkerEntry { is_latest, key, last_modified, owner, version_id }`; the list aliases are `ObjectVersionList` and `DeleteMarkers`.
- `ObjectIdentifier { e_tag, key, last_modified_time, size, version_id }`; `DeleteObjectOutput { delete_marker, request_charged, version_id }`; `DeletedObject { delete_marker, delete_marker_version_id, key, version_id }`; per-item `Error` includes `version_id`.
- `GetObjectInput.version_id`, `HeadObjectInput.version_id`, and their outputs' `delete_marker`/`version_id` fields are available. Get/Put/DeleteObjectTagging outputs expose `version_id`.
- `PutObjectOutput.version_id`, `CompleteMultipartUploadOutput.version_id`, `CopyObjectOutput.version_id`, and `CopyObjectOutput.copy_source_version_id` are available.
- `CopySource::Bucket { bucket, key, version_id: Option<Box<str>> }` already parses URL-decoded `?versionId=`; do not parse the header a second time.
- `S3Error::set_status_code`, `S3Error::set_headers`, and `S3Error::headers` are public. Use `Timestamp::from(SystemTime)` and `Timestamp::format(TimestampFormat::HttpDate, &mut writer)` to create the RFC1123 `Last-Modified` marker header without a new dependency.
- `S3Response::with_headers(output, HeaderMap)` merges custom success headers. Prefer standard DTO fields for standard version/delete response headers and retain the existing custom IPFS CID/URL headers.

## File Map and Exact Changed-Path Allowlist

The final implementation review must reject every changed or untracked path outside this exact allowlist and require every path marked **required**. A path marked **conditional** may remain unchanged only if its named call site compiles unchanged and the corresponding cross-path test proves it enters the central primitive.

**Create:**
- `src/store/entities/object_version.rs` — SeaORM model for public version-index rows. **Required**
- `src/store/migrations/m20260825_000001_object_versioning.rs` — additive SQLite/PostgreSQL schema, backfill verification, and fail-closed down migration. **Required**
- `src/store/object_version.rs` — public-version value types, locked selectors, transition primitives, promotion, and ordered scans. **Required**
- `src/s3/ops/versioning.rs` — bucket versioning and `ListObjectVersions` operations using s3s DTOs. **Required**
- `tests/postgres_versioning.rs` — PostgreSQL migration, invariant, rollback, and contention tests. **Required**
- `tests/compose.object-versioning-validation.yml` — isolated PostgreSQL/Kubo/gateway validation topology with no production ports or fixed container names. **Required**
- `scripts/object-versioning-smoke.ps1` — no-pull AWS CLI evidence runner with exact project/resource ownership and cleanup. **Required**
- `docs/object-versioning-evidence-2026-08-25.log` — starts honestly as `NOT RUN`; changes to a sanitized passing receipt only after the live gate. **Required**
- `docs/superpowers/plans/2026-08-25-object-versioning.md` — this implementation plan. **Required**

**Modify:**
- `src/store/entities/bucket.rs` — nullable `versioning_status`. **Required**
- `src/store/entities/mod.rs` — register `object_version`. **Required**
- `src/store/migrations/mod.rs` — register the migration module. **Required**
- `src/store/mod.rs` — export `object_version`, register the ninth migration, and update schema tests from fifteen to sixteen application tables. **Required**
- `src/store/bucket.rs` — state read/lock/write and deletion checks for versions, markers, multipart, and imports. **Required**
- `src/store/object.rs` — immutable-ID lookup and transaction-owned current-projection helpers; ordinary list filtering remains unchanged. **Required**
- `src/store/pinning/publication.rs` — central version-aware publication and simple/exact deletion transaction. **Required**
- `src/store/pinning/publication/tests.rs` — publication, lease, rollback, mutation-fence, ZIP, and concurrency regression matrix. **Required**
- `src/error.rs` — redacted version errors and marker response headers. **Required**
- `src/s3/handler.rs` — three exact s3s trait delegations. **Required**
- `src/s3/ops/mod.rs` — register `versioning`. **Required**
- `src/s3/ops/object.rs` — put/get/head/copy/delete/multi-delete/version response integration and shared listing helpers. **Required**
- `src/s3/ops/tagging.rs` — current/exact version selection and immutable-owner revalidation. **Required**
- `src/s3/ops/multipart.rs` — return completed destination public version and test encrypted/plain completion. **Required**
- `src/s3/route/decompress_zip.rs` — direct and multipart ZIP publication/version response tests. **Required**
- `src/s3/route/import_object/tests.rs` — adapt DeleteBucket regression fixtures to explicitly supersede active import work while preserving deleted-bucket idempotency and no-resolver assertions. **Required**
- `src/import/worker.rs` — test-only in-memory SQLite fixture isolation with one pooled connection plus a deterministic connection-occupancy regression; production/file-backed worker helpers remain unchanged. **Required**
- `src/store/import/ownership.rs` — test-only (`#[cfg(test)]`) active-import supersession before fixture bucket deletion; production ownership behavior outside the test configuration remains unchanged. **Required**
- `tests/integration.rs` — signed s3s operation matrix, marker headers/XML, all publication paths, pagination, and SQLite races. **Required**
- `tests/client-smoke.Tests.ps1` — static AST/source contract for the isolated object-versioning runner; it remains Docker-free. **Required**
- `tests/cluster.Tests.ps1` — refresh exactly the `src/s3/ops/object.rs` and `src/s3/ops/multipart.rs` protected baseline hashes after object-versioning code gates; no other protected hash or Cluster assertion may change. **Required**
- `README.md` — document the atomic feature only after every evidence gate passes. **Required**
- `ROADMAP.md` — check exactly the first three v0.6 boxes together after evidence. **Required**

**Verify/Conditional unchanged:**
- `src/import/publication.rs` — direct import already enters the central publication primitive; Task 10 proves the path without editing this file. **Conditional**
- `src/import/publication/zip.rs` — import archive/entry publication already enters the central primitive; Task 10 proves both paths without editing this file. **Conditional**
- `src/s3/ops/bucket.rs` — the handler already delegates bucket deletion unchanged; Task 4 bucket-versioning tests and Task 9 store/signed deletion tests prove the required behavior without editing this file. **Conditional**
- `src/s3/route/import_object.rs` — propagate the archive result version only if its current success contract has a version header surface; otherwise Task 10 proves the unchanged route reaches the central primitive. **Conditional**

**Approved pre-existing input:**
- `docs/superpowers/specs/2026-08-25-object-versioning-design.md` — approved design at the hash in the header; never edit it. **Required in final allowlist**

**Protected and expected unchanged:**
- `Cargo.toml`, `Cargo.lock`, `.github/workflows/release-validation.yml`, `docker-compose.yml`, `docker-compose.postgres.yml`, `docker-compose.multi-gateway.yml`, `docker-compose.cluster.yml`, `tests/compose.postgres-production-validation.yml`, `tests/compose.multi-gateway-validation.yml`, `tests/compose.cluster-validation.yml`, `ipfs/entrypoint.sh`, and `ipfs/private-swarm-entrypoint.sh`.

## Shared Domain Interfaces

Tasks 1-8 use these names exactly. If compilation forces a signature change, update every consuming task and this section in one plan revision before implementation continues.

```rust
pub const NULL_VERSION_ID: &str = "null";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketVersioningState {
    Unversioned,
    Enabled,
    Suspended,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicVersionId {
    Null,
    Opaque(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionKind {
    Object,
    DeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionSelector {
    Current,
    Exact(PublicVersionId),
}

#[derive(Clone, Debug)]
pub struct ResolvedVersion {
    pub public_version_id: String,
    pub kind: VersionKind,
    pub object: Option<crate::store::entities::object::Model>,
    pub is_latest: bool,
    pub sequence: i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionCursor {
    pub key: String,
    pub sequence: i64,
    pub public_version_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationResult {
    pub object_id: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteVersionResult {
    pub version_id: Option<String>,
    pub deleted_delete_marker: bool,
    pub created_delete_marker: bool,
}
```

`PublicVersionId::parse_s3(&str)` accepts only literal `null` or a canonical UUID string and returns `InvalidArgument` for empty/malformed values. `as_s3_str()` returns `null` or the opaque UUID; `as_db_value()` returns `None` only for `Null`. `ResolvedVersion.object` is `Some` only for `VersionKind::Object`; constructors reject every other kind/object combination.

### Task 1: Add the additive schema, entity, backfill, and fail-closed down migration

**Files:**
- Create: `src/store/entities/object_version.rs`
- Create: `src/store/migrations/m20260825_000001_object_versioning.rs`
- Modify: `src/store/entities/bucket.rs`
- Modify: `src/store/entities/mod.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/mod.rs`
- Test: `src/store/migrations/m20260825_000001_object_versioning.rs`
- Test: `src/store/mod.rs`
- Test: `tests/postgres_versioning.rs`

**Interfaces:**
- Consumes: current `buckets`, immutable `objects`, `idx_objects_latest`, `Migrator`, PostgreSQL advisory migration lock `(1229997651, 1395879239)`, and SQLite migration transaction support.
- Produces: nullable `bucket::Model.versioning_status: Option<String>`; `object_version::{Entity, Model, ActiveModel, Column}`; migration `m20260825_000001_object_versioning`; all indexes/checks/backfill required by the approved schema.

- [ ] **Step 1: Add causal RED migration and entity tests**

Add SQLite tests named `object_versioning_migration_adds_status_table_indexes_and_checks`, `object_versioning_backfills_only_legacy_latest_as_hidden_null`, `object_versioning_backfill_count_must_match`, `object_versioning_down_allows_only_hidden_null_rows`, and `object_versioning_down_refuses_public_state_versions_and_markers`. Add PostgreSQL test declarations in `tests/postgres_versioning.rs` for the same invariants and SQL types. Assert the ninth migration is registered last and the schema count is sixteen.

```powershell
cargo test --lib object_versioning_migration -- --nocapture
$redMigration = $LASTEXITCODE
cargo test --lib postgres_json_columns_migration_is_registered_last -- --nocapture
$redRegistration = $LASTEXITCODE
if ($redMigration -eq 0 -or $redRegistration -eq 0) { throw "Migration RED was not causal" }
```

Expected: the migration module/entity/column do not exist and the registered migration/table assertions fail; unrelated existing migration tests still compile after the failing test declarations are complete.

- [ ] **Step 2: Define the exact entity and database constraints**

Create `object_version::Model` with fields `id`, `bucket`, `key`, `version_id: Option<String>`, `kind`, `object_id: Option<String>`, `sequence: i64`, `is_latest`, `created_at`, and `updated_at`. Add `versioning_status: Option<String>` to the bucket entity and to every `bucket::ActiveModel` initializer.

The migration must create the logical schema and names below on SQLite and PostgreSQL:

```sql
CHECK (versioning_status IS NULL OR versioning_status IN ('Enabled', 'Suspended'))
CHECK (kind IN ('object', 'delete_marker'))
CHECK ((kind = 'object' AND object_id IS NOT NULL) OR
       (kind = 'delete_marker' AND object_id IS NULL))

CREATE UNIQUE INDEX uq_object_versions_latest
    ON object_versions(bucket, key) WHERE is_latest = TRUE;
CREATE UNIQUE INDEX uq_object_versions_sequence
    ON object_versions(bucket, key, sequence);
CREATE UNIQUE INDEX uq_object_versions_null_slot
    ON object_versions(bucket, key) WHERE version_id IS NULL;
CREATE UNIQUE INDEX uq_object_versions_public_id
    ON object_versions(bucket, key, version_id) WHERE version_id IS NOT NULL;
CREATE INDEX idx_object_versions_exact
    ON object_versions(bucket, key, version_id);
CREATE INDEX idx_object_versions_key_order
    ON object_versions(bucket, key, sequence DESC);
CREATE INDEX idx_object_versions_bucket_order
    ON object_versions(bucket, key ASC, sequence DESC, version_id);
```

Use foreign keys from `object_versions.bucket` to `buckets.name` and from non-null `object_id` to `objects.id`. Do not add a public ID column to `objects` and do not change `idx_objects_latest`.

- [ ] **Step 3: Implement exact backfill and destructive-down refusal**

Within the migration transaction, select only `objects.is_latest = true`, sort deterministically by `(bucket, key, id)`, insert one row per key with a fresh internal row UUID, database `version_id = NULL`, `kind = "object"`, `object_id = objects.id`, `sequence = 1`, `is_latest = true`, and timestamps copied from `objects.created_at`. Count selected latest rows and inserted rows and return `DbErr::Migration("object_versions backfill count mismatch")` before commit if they differ. Never query or insert legacy `is_latest = false` rows.

Before down, refuse with `DbErr::Migration("object versioning schema contains public state")` if any bucket status is non-null, any public `version_id` is non-null, or any row is a delete marker. If only hidden null object rows exist, drop indexes/table and then the bucket column. Add tests that an injected insert failure rolls the whole migration back.

- [ ] **Step 4: Register the migration and run narrow GREEN tests**

Register it after `m20260813_000001_postgres_json_columns`, rename the registration test to `object_versioning_migration_is_registered_last`, add `object_versions` to the table query/expected set, and change the assertion text to sixteen tables.

```powershell
cargo test --lib object_versioning_migration -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Object-versioning migration tests failed" }
cargo test --lib migration_is_registered_last -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Migration registration test failed" }
cargo test --lib test_migration_runs -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Schema inventory test failed" }
```

Expected: all named tests pass; SQLite rejects duplicate latest/sequence/null/public-ID rows and invalid kind/object pairs; down refusal preserves every row.

- [ ] **Step 5: Review boundary**

Confirm the migration is additive, `objects` has no public version column, existing migrations were not edited, the new status defaults to database `NULL`, and `Cargo.toml`/`Cargo.lock` are unchanged.

### Task 2: Build bucket-state and version-index store primitives

**Files:**
- Create: `src/store/object_version.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/bucket.rs`
- Modify: `src/store/object.rs`
- Test: `src/store/object_version.rs`
- Test: `src/store/bucket.rs`
- Test: `src/store/object.rs`

**Interfaces:**
- Consumes: Task 1 entities/constraints; `object::LatestObjectRow`; SeaORM `ConnectionTrait`; PostgreSQL `lock_exclusive`; SQLite transaction ownership.
- Produces: the Shared Domain Interfaces; `get_versioning_state`, `lock_versioning_state`, `set_versioning_state`; locked current/exact selectors; version transition, promotion, and scan helpers used by Tasks 3-8.

- [ ] **Step 1: Add RED tests for state parsing and public-ID isolation**

Add tests named `bucket_state_null_enabled_suspended_round_trip`, `bucket_state_rejects_corrupt_database_value`, `public_version_id_accepts_null_or_canonical_uuid_only`, `current_and_exact_selectors_never_expose_legacy_nonlatest`, `locked_next_sequence_and_null_slot_obey_constraints`, and `promotion_rebuilds_only_the_selected_object_projection`.

```powershell
cargo test --lib store::object_version::tests -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Object-version store RED unexpectedly passed" }
```

Expected: failure is caused by the absent module/types/functions, not by an existing migration failure.

- [ ] **Step 2: Implement exact bucket-state APIs**

Use these signatures:

```rust
pub async fn get_versioning_state<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
) -> AppResult<BucketVersioningState>;

pub async fn lock_versioning_state<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
) -> AppResult<BucketVersioningState>;

pub async fn set_versioning_state(
    db: &DatabaseConnection,
    bucket: &str,
    state: BucketVersioningState,
) -> AppResult<()>;
```

`get` returns `NoSuchBucket` before interpreting status. `lock` uses `FOR UPDATE` on PostgreSQL and a no-op bucket update on SQLite before loading status. `set` accepts only `Enabled` or `Suspended`, locks the bucket in its transaction, is idempotent for the same value, never writes `NULL`, and changes no version rows.

- [ ] **Step 3: Implement exact selectors and immutable object lookup**

Add `object::get_by_id<C: ConnectionTrait>(db, object_id) -> AppResult<object::Model>` and transaction-owned `set_only_latest<C: ConnectionTrait>(db, bucket, key, object_id: Option<&str>) -> AppResult<()>`. The latter clears every current projection and sets exactly the named immutable object when present, validating bucket/key ownership and affected-row counts.

In `object_version.rs`, expose:

```rust
pub async fn resolve_version<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    selector: &VersionSelector,
) -> AppResult<ResolvedVersion>;

pub async fn lock_resolved_version<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
    selector: &VersionSelector,
) -> AppResult<ResolvedVersion>;
```

Both load bucket state first. Unversioned current resolves only `objects.is_latest`; unversioned exact returns `InvalidArgument`. Enabled/Suspended current resolves the unique latest index row and validates its projection; exact resolves only an index row. Missing current content is `NoSuchKey`, unknown exact is `NoSuchVersion`, and a marker becomes a `ResolvedVersion` with no object so the S3 layer can form the required error. Never fall back from exact to latest and never inspect unindexed legacy rows.

- [ ] **Step 4: Implement transaction-only transition helpers**

Keep these helpers `pub(crate)` and require a caller-owned transaction:

```rust
pub(crate) async fn lock_current_row<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<Option<object_version::Model>>;

pub(crate) async fn allocate_next_sequence<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<i64>;

pub(crate) async fn install_content_version<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    object: &object::Model,
    now: DateTime<Utc>,
) -> AppResult<String>;

pub(crate) async fn install_delete_marker<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    bucket: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<String>;

pub(crate) async fn remove_and_promote<C: ConnectionTrait>(
    db: &C,
    selected: &object_version::Model,
) -> AppResult<Option<ResolvedVersion>>;
```

`allocate_next_sequence` runs only after the current row/key range is locked and uses checked `i64` addition. Enabled installation generates a new opaque UUID; Suspended and Unversioned replace the sole database-null slot and render it as literal `null`; unversioned rows stay hidden at selectors/list boundaries. Removal promotes highest `(sequence, public ID)` retained row and calls `set_only_latest` with its object ID or `None` for a marker/absence. Constructors validate kind/object pairs in application code before writes.

- [ ] **Step 5: Implement ordered list scan primitives**

Add:

```rust
pub async fn validate_version_cursor<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    prefix: &str,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> AppResult<Option<VersionCursor>>;

pub async fn scan_versions<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    prefix: &str,
    cursor: Option<&VersionCursor>,
    limit: u64,
) -> AppResult<Vec<ResolvedVersion>>;
```

No markers means start at the first row. `key_marker` without `version_id_marker` is the AWS exclusive key boundary (`key > key_marker`). `version_id_marker` without `key_marker` is `InvalidArgument`. A pair must resolve an indexed visible row for the same bucket/key/prefix and is an inclusive first-unreturned cursor; malformed UUIDs, hidden unversioned rows, deleted rows, and mismatched pairs are `InvalidArgument`. Scan order is key ascending, sequence descending, public ID deterministic; it starts at the paired row exactly and fetches a bounded chunk for the operation layer.

- [ ] **Step 6: Run store GREEN and ordinary-listing regressions**

```powershell
cargo test --lib store::object_version::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Object-version store tests failed" }
cargo test --lib store::object::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Object projection tests failed" }
cargo test --lib store::bucket::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Bucket store tests failed" }
```

Expected: public IDs never equal internal object IDs, legacy non-latest rows cannot resolve, hidden null maintenance is queryable only by internal helpers, and `object::list` still filters only `is_latest = true`.

### Task 3: Make pinning publication the single version-aware write boundary

**Files:**
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Modify: `src/store/object.rs`
- Modify: `src/s3/ops/object.rs`
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/s3/route/decompress_zip.rs`
- Verify/Conditional unchanged: `src/import/publication.rs` — existing direct import enters the central primitive; Task 10 proves the path
- Verify/Conditional unchanged: `src/import/publication/zip.rs` — existing archive/entry import enters the central primitive; Task 10 proves both paths
- Test: the same module-local test files

**Interfaces:**
- Consumes: Task 2 locked state/current/sequence/null-slot helpers; `PublicationObject`, `PublicationRequest`, `ZipPublicationRequest`, standard/import guards, lifecycle lock order, tags/quota/jobs, completed-upload reconciliation.
- Produces: expanded `PublicationResult { object_id, version_id }`; one version-aware `publish_in_transaction` path used by put/copy/multipart/direct import/import ZIP/direct ZIP/multipart ZIP; retained prior-version leases in Enabled and guarded displaced-null release in Suspended/Unversioned.

- [ ] **Step 1: Add RED publication matrix tests before changing production code**

Add named tests for `unversioned_publication_replaces_hidden_null_and_ends_displaced_lease`, `enabled_publication_retains_prior_version_tags_and_lease`, `suspended_publication_replaces_null_and_ends_only_displaced_null_lease`, `publication_rollback_preserves_index_projection_tags_leases_quota_and_jobs`, `stale_standard_and_import_guards_publish_no_version`, `zip_archive_and_entries_share_one_atomic_version_transition`, and `completed_multipart_reconciliation_checks_internal_object_not_public_version`.

```powershell
cargo test --lib store::pinning::publication::tests::enabled_publication -- --nocapture
$redEnabled = $LASTEXITCODE
cargo test --lib store::pinning::publication::tests::suspended_publication -- --nocapture
$redSuspended = $LASTEXITCODE
if ($redEnabled -eq 0 -or $redSuspended -eq 0) { throw "Publication RED was not causal" }
```

Expected: failures show `PublicationResult.version_id` and version rows are absent; existing guard/lifecycle tests continue compiling.

- [ ] **Step 2: Replace latest-only writing inside the existing transaction**

Expand `PublicationResult` exactly as declared in Shared Domain Interfaces. Select bucket state only after handler/request validation; then lock and revalidate that state as the first database step of the publication transaction, after SQLite write intent is acquired where applicable. Lock each affected key in the existing deterministic location order. This satisfies both the pre-publication state decision and the rule that current/sequence/null-slot choices are never trusted from a read outside the transaction. Preserve the canonical order: ownership/mutation guard → bucket/version current rows → lease → target → remote → quota → object/version projection → tags/new leases/jobs → guard completion.

Replace `write_object_and_end_previous` with a version-aware internal function:

```rust
async fn write_object_version_and_update_lifecycle<C: ConnectionTrait>(
    db: &C,
    object: &PublicationObject,
    state: BucketVersioningState,
    now: DateTime<Utc>,
) -> AppResult<PublicationResult>;
```

It inserts the immutable object exactly once, then:

- Unversioned: end only the displaced current object's active leases using the existing guarded helper, replace hidden null index, and project the new object latest.
- Enabled: demote the index/latest projection, insert an opaque version, project the new object, and do not end prior tags/leases.
- Suspended: if a null object exists, end only its active leases and remove only its object-owned tags before replacing its null row; if the null row is a marker, remove it without lifecycle/tag work; preserve every opaque version; install the new null object/projection.

Unversioned overwrite likewise removes only the displaced hidden-null object's tags after its guarded lease release. Enabled publication preserves every prior version's tags and leases. All tag deletion/replacement remains keyed by internal `objects.id`, never the public version value.

The function returns no public version for Unversioned, opaque UUID for Enabled, and literal `null` for Suspended. Do not pass public version IDs to tags, leases, targets, quota, jobs, mutation guards, or multipart reconciliation.

- [ ] **Step 3: Audit every producer and response consumer**

Search every `PublicationObject::from_put`, every `PublicationObject {`, and every `publish_*` call. Ensure standard PutObject, CopyObject destination, direct ZIP archive/entries, multipart archive/entries, direct import, and import ZIP archive/entries all reach the central function. Remove or convert `publish_plain_object` so no production helper calls `object::upsert` outside the central transaction. Keep test fixture upserts internal and explicitly hidden.

Set the destination public ID in `PutObjectOutput.version_id`, `CopyObjectOutput.version_id`, and `CompleteMultipartUploadOutput.version_id`; set `CopyObjectOutput.copy_source_version_id` from the selected source in Task 5. For direct ZIP, merge `x-amz-version-id` for the archive when version-aware. For the custom import route, set a version header only if the existing response contract admits object-publication headers; otherwise keep the version observable through standard version APIs and prove that in integration tests.

- [ ] **Step 4: Preserve Kubo and encryption boundaries**

Keep `stream_add`/copy re-pin and `pin_add` before the database transaction. A database error, stale guard, uniqueness retry, or ZIP rejection must not call `pin/rm`. Do not alter object IDs generated before encryption, SSE-S3 wrapped-key format, SSE-C fingerprint claim, multipart encryption object ID, CID/ETag, request streaming, or full-decrypt encrypted Range behavior.

- [ ] **Step 5: Run all publication-path GREEN tests**

```powershell
cargo test --lib store::pinning::publication -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Central publication tests failed" }
cargo test --lib s3::ops::multipart -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Multipart publication tests failed" }
cargo test --lib s3::route::decompress_zip -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "ZIP publication tests failed" }
cargo test --lib import::publication -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Import publication tests failed" }
```

Expected: all paths create correct version IDs/state, opaque prior leases survive Enabled writes, displaced null leases end in Suspended/Unversioned, rollback leaves no partial index/projection/lifecycle change, and no mock observes `pin/rm`.

- [ ] **Step 6: Review boundary**

Use repository search to enumerate all producers again. Record the list in the task receipt and fail the task if any content-producing production call bypasses `publish_in_transaction` or if `PublicationResult.version_id` is synthesized in an S3 handler.

### Task 4: Expose bucket versioning through s3s and add redacted marker/version errors

**Files:**
- Modify: `src/error.rs`
- Modify: `src/s3/handler.rs`
- Modify: `src/s3/ops/mod.rs`
- Create: `src/s3/ops/versioning.rs`
- Test: `src/error.rs`
- Test: `src/s3/ops/versioning.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 2 bucket-state APIs; locked s3s DTO contract; `S3Error` headers/status APIs; `TimestampFormat::HttpDate`.
- Produces: exact trait handlers for Get/PutBucketVersioning; `AppError::NoSuchVersion`, `AppError::InvalidArgument`, and marker-aware `S3Error` constructors reused by Tasks 5-8; status XML and validation behavior.

- [ ] **Step 1: Add RED error/header and bucket-operation tests**

Add unit tests named `version_errors_are_redacted_and_distinct`, `current_marker_error_is_404_with_required_headers`, `explicit_marker_error_is_405_with_rfc1123_last_modified`, `get_bucket_versioning_omits_unversioned_status`, `put_bucket_versioning_accepts_only_enabled_or_suspended`, `put_bucket_versioning_rejects_mfa_headers_and_configuration`, and `put_bucket_versioning_is_idempotent_without_rewriting_versions`. Add signed integration assertions for serialized XML values and missing-bucket behavior.

```powershell
cargo test --lib current_marker_error_is_404_with_required_headers -- --nocapture
$redError = $LASTEXITCODE
cargo test --lib s3::ops::versioning -- --nocapture
$redBucket = $LASTEXITCODE
if ($redError -eq 0 -or $redBucket -eq 0) { throw "Bucket/error RED was not causal" }
```

Expected: the error helpers/module/trait methods do not exist; failure is not an unrelated publication regression.

- [ ] **Step 2: Add exact application errors without backend leakage**

Add variants carrying only public-safe context:

```rust
NoSuchVersion { bucket: String, key: String, version_id: String },
InvalidArgument(String),
DeleteMarker {
    version_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    current: bool,
},
```

Map `NoSuchVersion` to `NoSuchVersion`/404, `InvalidArgument` to `InvalidArgument`/400, and `DeleteMarker` to `NoSuchKey`/404 when `current`, otherwise `MethodNotAllowed`/405. Build a fresh `HeaderMap` containing exact lowercase semantic values `x-amz-delete-marker: true` and `x-amz-version-id: <public value>`. For explicit markers only, format `Last-Modified` with s3s `TimestampFormat::HttpDate`; reject an impossible header value as redacted `InternalError`. Keep database, Kubo, provider, encryption material, and internal IDs out of response text and structured error fields.

- [ ] **Step 3: Implement the two bucket versioning operations**

Use exact functions:

```rust
pub async fn get_bucket_versioning(
    state: &Arc<AppState>,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>>;

pub async fn put_bucket_versioning(
    state: &Arc<AppState>,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>>;
```

Get returns `status = None`/`mfa_delete = None` for Unversioned, and exact s3s Enabled/Suspended values otherwise. Put validates bucket existence before status mutation; rejects any `input.mfa`, any `versioning_configuration.mfa_delete`, missing status, and unknown status as `InvalidArgument`; then calls `set_versioning_state`. It never edits index rows, so first enable exposes only the maintained hidden null and all opaque history survives later state changes.

- [ ] **Step 4: Register exact trait delegations with no alternate route**

Add `pub mod versioning;` and the exact `get_bucket_versioning`/`put_bucket_versioning` trait methods from the locked API block to `S3Impl`. Do not change `GatewayRoute`, `main.rs`, XML serializers, request authentication, or middleware.

- [ ] **Step 5: Run bucket/error GREEN and signed XML integration**

```powershell
cargo test --lib error::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Version error tests failed" }
cargo test --lib s3::ops::versioning -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Bucket versioning operation tests failed" }
cargo test --test integration bucket_versioning -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Signed bucket versioning integration tests failed" }
```

Expected: XML has no `<Status>` while unversioned, uses exact `Enabled`/`Suspended`, invalid/MFA requests are 400 and non-mutating, missing buckets are `NoSuchBucket`, and repeated status writes leave all row timestamps/history intact.

### Task 5: Resolve current/exact GetObject, HeadObject, and CopyObject versions

**Files:**
- Modify: `src/s3/ops/object.rs`
- Modify: `src/store/object_version.rs`
- Test: `src/s3/ops/object.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 2 `resolve_version`; Task 3 `PublicationResult`; Task 4 marker errors; existing SSE-C/SSE-S3/range/Kubo/copy-tag helpers; parsed `CopySource::Bucket.version_id`.
- Produces: one shared content selector for Get/Head/Copy source; exact immutable-object reads; correct response public IDs; marker short-circuit before Kubo/range/SSE-C; independent destination version publication.

- [ ] **Step 1: Add RED read/copy tests covering content, markers, and encryption**

Add unit and signed integration tests named `get_and_head_current_content_return_public_version`, `get_and_head_exact_historical_plain_sse_s3_and_sse_c_versions`, `current_marker_returns_404_headers_without_kubo`, `explicit_marker_returns_405_headers_without_range_or_sse_c`, `unknown_exact_is_no_such_version`, `unversioned_exact_is_invalid_argument`, `copy_source_exact_version_uses_selected_content_and_tags`, `copy_source_marker_uses_current_404_or_explicit_405`, and `copy_destination_gets_independent_object_and_public_ids`.

```powershell
cargo test --lib s3::ops::object::tests::get_and_head_exact -- --nocapture
$redReads = $LASTEXITCODE
cargo test --lib s3::ops::object::tests::copy_source_exact -- --nocapture
$redCopy = $LASTEXITCODE
if ($redReads -eq 0 -or $redCopy -eq 0) { throw "Read/copy RED was not causal" }
```

Expected: inputs are currently ignored/latest-only and marker tests fail before any implementation change.

- [ ] **Step 2: Create one S3 selection adapter and short-circuit markers**

Add an object-operation helper:

```rust
async fn select_s3_object(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<(crate::store::entities::object::Model, Option<String>)>;
```

Parse `Some` through `PublicVersionId::parse_s3`, resolve `VersionSelector::Exact`; resolve `None` as current. Return `(object, None)` for Unversioned current and `(object, Some(public ID))` for aware current/exact. Convert a current marker to the Task 4 404 error and an explicit marker to the 405 error before range parsing, SSE-C extraction/authentication, Kubo cat, CID/IPFS headers, or metadata restoration.

- [ ] **Step 3: Refactor GetObject and HeadObject without changing content behavior**

Use `req.input.version_id.as_deref()` and the selected immutable object. Populate `GetObjectOutput.version_id`/`HeadObjectOutput.version_id` for Enabled/Suspended content and omit for Unversioned. Preserve existing ETag/CID, Last-Modified, metadata, content length/range, streaming, SSE-S3, and SSE-C response fields. The legacy SSE-C fingerprint claim must update by selected immutable `object.id`, never by latest key lookup, and must re-read that same object ID.

- [ ] **Step 4: Select CopyObject source version and publish destination independently**

Destructure `CopySource::Bucket { bucket, key, version_id }`; pass its value to the shared selector. Run source SSE-C authentication and COPY-tag lookup against the selected immutable object. Preserve REPLACE semantics. Generate a new destination internal object ID regardless of source CID, call the central publisher, then set `copy_source_version_id` to the selected source public ID when aware and `version_id` to the destination publication result. Never share source tags, object ID, leases, or public ID.

- [ ] **Step 5: Run read/copy GREEN and no-backend-call assertions**

```powershell
cargo test --lib s3::ops::object::tests::get_and_head -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Get/Head version tests failed" }
cargo test --lib s3::ops::object::tests::copy -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Copy version tests failed" }
cargo test --test integration object_version_read -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Version read integration tests failed" }
```

Expected: historical encryption metadata selects the right key/fingerprint; marker probes observe zero Kubo calls and exact headers/status/XML; current marker stays absent from ordinary content APIs.

### Task 6: Implement atomic simple/exact DeleteObject and ordered DeleteObjects

**Files:**
- Modify: `src/store/pinning/publication.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Modify: `src/store/object_version.rs`
- Modify: `src/s3/ops/object.rs`
- Test: `src/s3/ops/object.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 2 transition/promotion and locked selectors; Task 3 lifecycle transaction/guard retries; Task 4 errors; exact s3s delete DTO fields.
- Produces: `delete_version_with_leases_guarded(..., VersionSelector, ...) -> AppResult<DeleteVersionResult>`; Enabled/Suspended marker creation; exact row removal/promotion; per-entry ordered multi-delete without deduplication.

- [ ] **Step 1: Add RED delete matrix tests**

Add tests named `unversioned_simple_delete_is_idempotent_and_removes_hidden_null`, `enabled_simple_delete_always_creates_new_opaque_marker`, `suspended_simple_delete_replaces_null_and_releases_only_displaced_null`, `exact_content_delete_ends_only_selected_internal_owner`, `exact_marker_delete_has_no_lease_work`, `exact_latest_delete_promotes_next_object_or_marker`, `delete_objects_preserves_duplicates_order_quiet_and_per_item_errors`, `delete_race_loses_to_newer_mutation_fence`, and `delete_never_calls_pin_rm`.

```powershell
cargo test --lib s3::ops::object::tests::enabled_simple_delete -- --nocapture
$redSimple = $LASTEXITCODE
cargo test --lib s3::ops::object::tests::delete_objects_preserves_duplicates -- --nocapture
$redMulti = $LASTEXITCODE
if ($redSimple -eq 0 -or $redMulti -eq 0) { throw "Delete RED was not causal" }
```

Expected: current code ignores version IDs, returns NoSuchKey for missing simple deletes, and deduplicates multi-delete keys.

- [ ] **Step 2: Replace latest-only deletion with one transaction primitive**

Expose:

```rust
pub async fn delete_version_with_leases_guarded(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    selector: VersionSelector,
    guard: StandardMutationGuard,
    now: DateTime<Utc>,
) -> AppResult<DeleteVersionResult>;
```

Within the existing bounded retry loop, acquire SQLite write intent or PostgreSQL locks, verify the guard, lock bucket state/current/selected row, and then:

- Unversioned simple: if current exists, clear projection, end its leases, remove hidden null; if absent, complete the guard successfully; return no version/marker fields. Unversioned exact returns `InvalidArgument` without mutation.
- Enabled simple: demote any latest, clear projection, allocate sequence, insert a fresh opaque marker even if the key is absent/current marker, keep all retained content tags/leases, and return `created_delete_marker = true`.
- Suspended simple: end only a displaced null object's leases/tags ownership as existing delete semantics require, remove the null slot, preserve opaque rows, insert latest null marker, and return literal `null`.
- Exact: lock/revalidate the indexed row; unknown is `NoSuchVersion`; end tags/leases only for its internal object; marker does no lifecycle work; remove exactly that row; if latest, atomically promote next retained row and rebuild projection; never create a marker.

Complete the standard mutation guard in the same transaction. Keep lifecycle lock ordering and retry only current documented busy/serialization/deadlock/unique cases.

- [ ] **Step 3: Map single-delete response fields exactly**

For simple Enabled/Suspended marker creation, set `DeleteObjectOutput.delete_marker = Some(true)` and `version_id = Some(new marker public ID)`. For exact deletion, set `version_id = Some(requested public ID)`; also set `delete_marker = Some(true)` only if the deleted row was a marker. Unversioned simple returns default fields and success for absent keys.

- [ ] **Step 4: Process multi-delete strictly in request order**

Retain the 1,000-item limit and bucket precheck. Remove `BTreeMap`/`HashMap` key deduplication and bulk unique-key admission. For each `ObjectIdentifier` in order, admit a fresh mutation guard and invoke the central primitive with simple/current or parsed exact selector. Preserve duplicates as separate mutations and outputs.

For a successful simple marker, emit `DeletedObject { key, delete_marker: Some(true), delete_marker_version_id: Some(new ID), version_id: None, .. }`. For successful exact deletion, emit `version_id = Some(requested ID)` and, when it was a marker, both `delete_marker = Some(true)` and `delete_marker_version_id = Some(requested ID)`. Quiet mode suppresses all successes but not errors. Map per-item `NoSuchVersion`, `InvalidArgument`, `StaleContentMutation`→`OperationAborted`, and redacted internal errors with the incoming `version_id`; continue subsequent items.

- [ ] **Step 5: Run delete GREEN and lease/pin regressions**

```powershell
cargo test --lib store::pinning::publication::tests::delete -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Delete transaction tests failed" }
cargo test --lib s3::ops::object::tests::delete -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Delete operation tests failed" }
cargo test --test integration object_version_delete -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Version delete integration tests failed" }
```

Expected: exact deletion can restore historical content by promotion; prior Enabled leases survive simple marker creation; displaced null lease ends exactly once; duplicate requests have distinct ordered effects; no path issues `pin/rm`.

### Task 7: Make object tagging current/exact-version aware without changing owner IDs

**Files:**
- Modify: `src/s3/ops/tagging.rs`
- Modify: `src/store/object_version.rs`
- Test: `src/s3/ops/tagging.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 2 `VersionSelector`, `resolve_version`, and `lock_resolved_version`; Task 4 marker errors; existing manual lease snapshot/policy/CAS and tag storage keyed by `objects.id`.
- Produces: Get/Put/DeleteObjectTagging for current or exact content; public version response IDs; selected-row/object/lease revalidation under one transaction.

- [ ] **Step 1: Replace version-rejection tests with RED exact-version tests**

Add tests named `get_tagging_reads_exact_historical_owner`, `put_and_delete_tagging_mutate_only_exact_internal_owner`, `current_tagging_marker_is_404`, `exact_tagging_marker_is_method_not_allowed`, `unknown_tagging_version_is_no_such_version`, `unversioned_tagging_version_is_invalid_argument`, `tagging_revalidates_version_and_lease_under_lock`, and `tagging_never_uses_public_id_as_owner_object_id`.

```powershell
cargo test --lib s3::ops::tagging::tests::get_tagging_reads_exact -- --nocapture
$redGetTag = $LASTEXITCODE
cargo test --lib s3::ops::tagging::tests::tagging_never_uses_public_id -- --nocapture
$redOwner = $LASTEXITCODE
if ($redGetTag -eq 0 -or $redOwner -eq 0) { throw "Tagging RED was not causal" }
```

Expected: current `require_unversioned` rejects the new exact requests.

- [ ] **Step 2: Resolve reads and write selectors consistently**

Delete `require_unversioned`. Get calls the common resolver and lists tags by selected immutable `object.id`; it returns the public version ID for aware buckets and `None` for Unversioned. Current/explicit markers use Task 4 errors; exact missing and unversioned exact retain their distinct errors.

Change the write helper signature to:

```rust
async fn replace_tag_set(
    state: &Arc<AppState>,
    bucket: String,
    key: String,
    selector: VersionSelector,
    replacement: Vec<ObjectTag>,
    now: DateTime<Utc>,
) -> AppResult<Option<String>>;
```

Inside its transaction, call `lock_resolved_version`, require content, then load/evaluate/revalidate the manual lease snapshot and replace tags by that same immutable object ID. Revalidate the selected index row/object relationship after any test pause; never call latest-key fallback. Keep lease generation CAS and policy errors unchanged. Return the public version for output DTOs.

- [ ] **Step 3: Run tagging GREEN and concurrent revalidation tests**

```powershell
cargo test --lib s3::ops::tagging -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Version tagging tests failed" }
cargo test --test integration object_version_tagging -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Version tagging integration tests failed" }
```

Expected: changing historical tags leaves current tags untouched, output IDs are public values, owner columns remain internal UUIDs, and a concurrent exact delete/publication yields `NoSuchVersion` or the existing conflict rather than mutating a replacement.

### Task 8: Implement ListObjectVersions and preserve ordinary listing projection

**Files:**
- Modify: `src/s3/ops/versioning.rs`
- Modify: `src/s3/handler.rs`
- Modify: `src/s3/ops/object.rs`
- Modify: `src/store/object_version.rs`
- Test: `src/s3/ops/versioning.rs`
- Test: `src/s3/ops/object.rs`
- Test: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 2 cursor/scan types; Task 4 handler module; exact s3s `ObjectVersion`, `DeleteMarkerEntry`, and output fields; ordinary URL encoding/max-key/common-prefix conventions.
- Produces: s3s `list_object_versions` trait delegation; stable first-unreturned pagination; shared max-key/encoding helpers without behavior change to ListObjects/V2.

- [ ] **Step 1: Add RED ordering, cursor, delimiter, encoding, and invisibility tests**

Add tests named `list_versions_orders_key_asc_sequence_desc`, `list_versions_projects_content_and_markers`, `list_versions_next_pair_is_first_unreturned_and_resume_is_inclusive`, `list_versions_rejects_version_marker_without_key_or_mismatched_pair`, `list_versions_key_marker_alone_is_exclusive_key_boundary`, `list_versions_delimiter_prefix_consumes_one_budget_and_skips_group`, `list_versions_url_encodes_keys_prefixes_and_markers`, `unversioned_list_is_empty_and_hides_backfill`, `ordinary_lists_hide_noncurrent_and_markers`, and `list_versions_write_delete_race_has_no_duplicate_or_loop`.

```powershell
cargo test --lib s3::ops::versioning::tests::list_versions -- --nocapture
$redVersions = $LASTEXITCODE
cargo test --test integration list_object_versions -- --nocapture --test-threads=1
$redSigned = $LASTEXITCODE
if ($redVersions -eq 0 -or $redSigned -eq 0) { throw "ListObjectVersions RED was not causal" }
```

Expected: trait delegation/list operation are absent and signed API requests fail through s3s rather than a custom route.

- [ ] **Step 2: Share only stable ordinary-listing helpers**

Move or expose as `pub(crate)` the existing `normalized_max_keys` and URL-encoding helpers needed by both listing operations. Keep ListObjects/ListObjectsV2 query, cursor, common-prefix, delimiter, encoding, and output semantics unchanged; do not route ordinary listings through `object_versions`.

- [ ] **Step 3: Build the exact version-list page algorithm**

Implement `list_object_versions` in the dedicated module:

1. Validate bucket existence and load state. For Unversioned, return a valid empty output with request echoes and no versions/markers/common prefixes.
2. Normalize `prefix = ""`, `max_keys` to the existing 1..=1000 convention, and validate marker input using Task 2. Reject version-only or inconsistent pairs as `InvalidArgument`.
3. Scan visible index rows ordered by key ASC, sequence DESC, public ID. Keep the cursor tuple internal; database-null IDs render only as literal `null`.
4. Merge versions/delete markers/common prefixes into one page budget. A delimiter group emits one `CommonPrefix` per page and consumes one slot; scan past every row in that group before considering the next output.
5. Fetch/scan one visible output beyond the budget. Set `is_truncated = true` and `next_key_marker`/`next_version_id_marker` from the first version/marker row not returned. The next request pair is inclusive and starts exactly there. If the first unreturned output is a common prefix, use its first hidden row as the pair; after that page emits the prefix, advance beyond the whole group so the prefix cannot repeat forever.
6. Map content to `ObjectVersion` with CID ETag, immutable size/time, `storage_class = Some(ObjectVersionStorageClass::from_static(ObjectVersionStorageClass::STANDARD))`, public ID, and exact `is_latest`. Map marker to `DeleteMarkerEntry` with key/time/public ID/is_latest and no content fields. Split the merged page into s3s `versions`, `delete_markers`, and `common_prefixes` while preserving each category's relative order and shared-budget decision.
7. Apply `encoding-type=url` consistently to returned key, prefix, delimiter/common prefix, key markers, and next key marker; version IDs are not URL-mutated in DTO values.

- [ ] **Step 4: Register the exact trait method**

Add the `list_object_versions` signature from the locked API contract to `S3Impl` and delegate to `ops::versioning`. Do not add route/query/XML code elsewhere.

- [ ] **Step 5: Run listing GREEN and ordinary regressions**

```powershell
cargo test --lib s3::ops::versioning -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Version listing tests failed" }
cargo test --lib s3::ops::object::tests::list -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Ordinary listing regressions failed" }
cargo test --test integration list_object_versions -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Signed ListObjectVersions tests failed" }
```

Expected: no page duplicates/skips visible versions, delimiter pages terminate, malformed cursors fail closed, unversioned backfill is private, and ordinary lists expose only current content.

### Task 9: Block bucket deletion correctly and prove SQLite/PostgreSQL concurrency

**Files:**
- Modify: `src/store/bucket.rs`
- Verify/Conditional unchanged: `src/s3/ops/bucket.rs` — existing handler delegation stays unchanged; Task 4 and Task 9 tests prove behavior
- Modify: `src/store/object_version.rs`
- Modify: `src/store/pinning/publication.rs`
- Modify: `tests/integration.rs`
- Create/Modify: `tests/postgres_versioning.rs`
- Test/Modify: `src/s3/route/import_object/tests.rs`
- Test/Modify: `src/store/import/ownership.rs` (`#[cfg(test)]` only)
- Test: `src/store/bucket.rs`
- Test: `src/store/pinning/publication/tests.rs`

**Interfaces:**
- Consumes: Tasks 1-8 schema, locked transition/delete/tag APIs, existing import ownership and multipart entities, SQLite busy timeout/retry classifier, PostgreSQL lifecycle lock order.
- Produces: complete bucket emptiness check; file-backed SQLite and PostgreSQL contention receipts proving one latest/no leaked null/no stale resurrection/no deadlock; migration upgrade/backfill/down coverage on both engines.

- [ ] **Step 1: Add RED bucket-deletion and contention tests**

Add tests named `delete_bucket_blocks_active_multipart`, `delete_bucket_blocks_active_import`, `delete_bucket_blocks_current_unversioned_object`, `delete_bucket_blocks_retained_public_object_and_marker`, `delete_bucket_allows_legacy_hidden_nonlatest_cleanup`, `sqlite_parallel_publish_delete_tag_has_one_latest_and_no_null_leak`, `postgres_parallel_publish_delete_tag_serializes_without_deadlock`, `stale_import_cannot_resurrect_after_delete`, and `failed_transition_rolls_back_index_projection_and_leases`.

```powershell
cargo test --test postgres_versioning --no-run
if ($LASTEXITCODE -ne 0) { throw "Task 1 PostgreSQL version test target no longer compiles" }
cargo test --lib store::bucket::tests::delete_bucket_blocks_retained_public_object_and_marker -- --nocapture
if ($LASTEXITCODE -eq 0) { throw "Retained public-version bucket-deletion RED was not causal" }
```

Expected: the Task 1 PostgreSQL target compiles successfully; the concrete bucket-deletion test then fails because deletion still checks only `objects.is_latest` and does not yet reject a retained noncurrent public version or marker.

- [ ] **Step 2: Expand the locked bucket emptiness transaction**

After `lock_bucket_for_ownership`, check in one transaction for:

1. active multipart upload rows for the bucket;
2. active/import-owned work using the existing ownership/import status definitions;
3. any current `objects.is_latest = true` row;
4. for Enabled/Suspended, any `object_versions` row, whether content, noncurrent, null, or marker;
5. for Unversioned, any hidden null/current index row.

Return `BucketNotEmpty` for any public/current obligation. Do not make legacy unindexed `objects.is_latest = false` rows a public obligation; retain the existing foreign-key/cascade cleanup ownership for those rows. Supersede import ownership and delete the bucket only after every check passes. Adapt the regression fixtures in `src/s3/route/import_object/tests.rs` and the `#[cfg(test)]` section of `src/store/import/ownership.rs` to explicitly supersede active import work before fixture bucket deletion; preserve the original deleted-bucket idempotency and no-resolver assertions, and do not change production ownership behavior outside `#[cfg(test)]`. Tests must prove exact deletion of all retained versions/markers is required before a version-aware bucket can be removed.

- [ ] **Step 3: Add deterministic file-backed SQLite contention tests**

Use two independent `connect_database` connections to one temp SQLite file. Synchronize tasks with `Barrier`/`Notify` around existing test gates; do not use sleeps as the correctness oracle. Race Enabled writes, Suspended null writes, simple/exact delete, tag replacement, and stale import publication. Assert bounded retries, one ordered winner sequence, at most one latest, at most one null slot, projection agreement, no stale resurrection, selected immutable owner IDs, and unchanged retained leases. Inject a failure after index demotion and prove transaction rollback restores every table.

- [ ] **Step 4: Implement PostgreSQL migration and contention tests**

`tests/postgres_versioning.rs` reads only `IPFS_S3_TEST_POSTGRES_URL`; if absent, each test prints one explicit skip line and returns without claiming pass evidence. With a URL, create a unique schema/table namespace derived from a UUID, run the full migrator, seed legacy current/nonlatest rows, and prove backfill/constraints/down refusal. Run parallel connections against one key and the pin lifecycle frontier; enforce a timeout and assert no deadlock, one latest, unique sequence/null slot, exact-owner tag/lease behavior, and stale mutation loss. Drop only the test-owned schema in `Drop`/finally-equivalent cleanup.

Add named regression `postgres_suspended_null_replacement_preserves_opaque_versions`: publish two Enabled opaque versions followed by two Suspended null publications against fresh PostgreSQL 17, then scan the public index and assert exactly three rows, sequences `[1, 2, 4]`, one latest literal-null row, and both opaque versions retained. The test must execute rather than skip under the owned PostgreSQL URL and clean only its owned target/schema.

- [ ] **Step 5: Keep retry and lock policy bounded**

Reuse the existing publication retry classifier and maximum. Add only SQLSTATE/SQLite categories already documented for serialization, busy/locked, deadlock, or unique contention; do not retry validation, `NoSuchVersion`, stale guards, marker errors, or arbitrary database errors. PostgreSQL locks bucket/version current rows before lifecycle rows in the canonical order; SQLite acquires write intent before selecting current/sequence/null.

- [ ] **Step 6: Run local SQLite GREEN; compile PostgreSQL target without claiming live coverage**

```powershell
cargo test --lib sqlite_parallel_publish_delete_tag -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "File-backed SQLite contention tests failed" }
cargo test --lib store::bucket::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Bucket deletion tests failed" }
cargo test --lib store::import::ownership -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Import ownership bucket-deletion fixture regressions failed" }
cargo test --lib s3::route::import_object -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Import route bucket-deletion fixture regressions failed" }
cargo test --test postgres_versioning --no-run
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL version test target did not compile" }
```

Expected: SQLite concurrency/rollback is proven; PostgreSQL test binary compiles but remains honestly unverified until Task 12 supplies the isolated database URL.

### Task 10: Complete the signed cross-path integration matrix

**Files:**
- Modify: `tests/integration.rs`
- Modify: `src/store/pinning/publication/tests.rs`
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/s3/route/decompress_zip.rs`
- Verify/Conditional unchanged: `src/import/publication.rs` — already enters the central primitive; this task proves the direct-import path
- Verify/Conditional unchanged: `src/import/publication/zip.rs` — already enters the central primitive; this task proves archive/entry paths
- Verify/Conditional unchanged: `src/s3/route/import_object.rs` — unchanged unless its existing response contract exposes a version header; this task proves central publication reachability

**Interfaces:**
- Consumes: all production interfaces from Tasks 1-9; in-memory and file-backed SQLite fixtures; wiremock Kubo; signed s3s integration harness; existing direct/import/ZIP/multipart helpers.
- Produces: one executable acceptance matrix proving atomic feature reachability only after every path and error boundary is integrated.

- [ ] **Step 1: Upgrade fixtures without manufacturing legacy history**

Add fixture helpers that create a bucket state, publish through the central primitive, select public IDs from `object_versions`, and sign requests containing `versionId`. Keep one explicit migration fixture that seeds pre-v0.6 latest/nonlatest objects before running Task 1 migration. Do not update general fixtures by inserting public rows for historical `is_latest = false` objects.

- [ ] **Step 2: Add and pass the complete publication-path acceptance table**

For Unversioned, Enabled, and Suspended states, cover standard put, copy destination, completed multipart, direct CID/HTTPS import, import ZIP archive/entry, direct ZIP archive/entry, and multipart ZIP archive/entry. For each path assert internal object UUID, public ID kind, index sequence/latest, ordinary projection, ETag=CID, encryption metadata, tags/internal owner, leases/targets/jobs/quota, previous-version retention/release, and no `pin/rm`. For public S3 responses assert version IDs wherever the locked DTO exposes them.

```powershell
cargo test --test integration versioning_all_publication_paths -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Cross-path publication acceptance matrix failed" }
```

Expected: the complete matrix is GREEN because the causal RED tests and production wiring were completed in Tasks 3-9; every named producer and response satisfies its version assertion without weakening the table.

- [ ] **Step 3: Add the complete operation/error matrix**

Using signed requests, prove bucket transition XML/MFA rejection; current/exact plain/SSE-S3/SSE-C Get/Head; current/explicit marker 404/405 headers and XML; versioned copy/tag; simple/exact/multi-delete order/quiet/errors; ordinary-list invisibility; version-list ordering/cursor/delimiter/URL; bucket deletion; missing bucket/key/version and invalid arguments; and rollback/races. Assert marker operations make zero Kubo calls and create no object/tag/lease/target/job row.

- [ ] **Step 4: Run the integrated SQLite acceptance suite**

```powershell
cargo test --test integration versioning_ -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Integrated object-versioning acceptance tests failed" }
cargo test --test integration -- --nocapture --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "Full signed integration suite regressed" }
```

Expected: every matrix row passes, existing unversioned clients retain behavior, and no partial public method is left unsupported.

- [ ] **Step 5: Review boundary**

Map each of the eight numbered test requirements in the approved spec to concrete test names in the task receipt. Confirm no assertion treats an internal object ID as a public version ID and no fixture indexes legacy non-latest rows.

### Task 11: Add a static-contracted, isolated AWS CLI evidence runner

**Files:**
- Create: `tests/compose.object-versioning-validation.yml`
- Create: `scripts/object-versioning-smoke.ps1`
- Create: `docs/object-versioning-evidence-2026-08-25.log`
- Modify: `tests/client-smoke.Tests.ps1`
- Verify unchanged: `.github/workflows/release-validation.yml` already runs `tests/client-smoke.Tests.ps1`; all production Compose files remain byte-for-byte unchanged.

**Interfaces:**
- Consumes: current source tree; local-only `postgres:17`, Kubo/runtime/build, Rust builder, and `amazon/aws-cli:latest` images; PowerShell AST; existing offline vendor/build pattern; `IPFS_S3_TEST_POSTGRES_URL` from Task 9.
- Produces: Docker-free static safety contract; opt-in `-Run` live runner; unique project/image/temp/port ownership; bounded stage attribution; partial-ownership recovery; existing Rust E2E plus PostgreSQL plus AWS client evidence; absent-safe suspended-list-only AWS JSON exit/parse/shape receipts; sanitized `NOT RUN`/PASSED evidence; exact diagnostics/cleanup/residual receipts.

**Runtime parser correction record:** invocation 2 emitted `stage=preflight` and `failed-stage=preflight` before any ownership. A direct read-only query proved that `docker compose version --format '{{.Version}}'` returned decorated text `Docker Compose version v2.39.2-desktop.1`, while the old anchored parser expected a numeric version at byte 0. The corrected pure toggle uses `docker compose version --short` and records `VERSION=<2.39.2-desktop.1> CORE=<2.39.2> ACCEPTED=True`; this is a runner preflight parser defect, not topology or product evidence.

**Runtime offline-binding correction record:** invocation 3 completed every nonlive gate, entered `stage=offline-build`, and failed before Cargo launch, image ownership, or project ownership. Root cause: `Invoke-NativeCommand` declares `ArgumentList`, but the offline Cargo call supplied nonexistent named parameter `Arguments`, so PowerShell parameter binding stopped the stage. An owned build-only diagnostic independently proved the unchanged toolchain/build path: Cargo vendor exit `0` in `59.041s`, tar exit `0` in `23.401s`, exact runner-equivalent Docker build exit `0` in `222.107s`, and exact image inspect/removal exit `0`; the diagnostic root/image were absent afterward. This is a runner binding defect, not a Cargo, tar, Docker build, toolchain, topology, or product failure.

**Runtime AWS observability correction record:** invocation 4 was the first actual topology attempt. Topology, metadata, live PostgreSQL, and existing E2E all passed, then `stage=aws` failed before any AWS assertion receipt; the only command label was `aws-cli-versioning-path-style`. Official/read-only AWS CLI v2 configuration evidence does not support `AWS_S3_FORCE_PATH_STYLE=true`; supported S3 configuration uses profile config `s3.addressing_style = path`. The existing `Invoke-AwsJson` also relies on implicit output format, and the coarse command label cannot identify the first failing AWS operation. Classify this as AWS runner configuration/observability debt, not a product verdict.

**Runtime AWS error-representation correction record:** invocation 5 passed topology, live PostgreSQL/E2E, bucket/versioning setup, two puts and ID/ETag assertions, delete marker/list ordering, and historical GET bytes. It then failed immediately after substage `current-get-nosuchkey` because the runner required a numeric HTTP substring from default human stderr. AWS CLI/botocore contracts do not guarantee `ResponseMetadata.HTTPStatusCode` there: `get-object` NoSuchKey may omit `404`, and HEAD failures expose generic service codes `404`/`405`. AWS CLI v2 `--cli-error-format json` provides stable top-level structured code at `$document.Code` but still no HTTP metadata. Signed integration remains the authoritative proof of current-marker HTTP 404, explicit-marker HTTP 405, and required headers.

**Runtime invocation 8 AWS JSON boundary record:** the fresh topology, PostgreSQL suite, existing E2E suite, and AWS path through the second suspended null PUT passed. The configured seven-snapshot contract was present, and the six snapshots reached before the final list proved the exact expected database states; the seventh `after-suspended-list-parse` snapshot was not reached. The runner emitted `aws-substage=suspended-list`, then failed before any suspended-list count, `aws-json-command-exit`, `aws-json-parse`, final snapshot, or `suspended-list-assert` receipt. Cleanup and every residual check completed with `cleanup-errors=0`. This refutes database-state loss at the six observed boundaries and narrows the unobserved failure to the AWS command invocation or bounded JSON parse boundary; it does not establish a transient or product root cause.

**Runtime invocation 9 SafeReceipt correction record:** invocation 9 proved the full topology/storage path and every pre-list database state remained correct, then the explicit suspended-list call emitted `aws-json-command-exit=0` and `aws-json-parse=passed` before failing prior to the suspended counts and `suspended-list-parsed` snapshot. The same receipt pair also appeared on ordinary `Invoke-AwsJson` calls that omitted `SafeReceipt`. Exact source/runtime evidence confirms an omitted PowerShell `[string]$SafeReceipt` is the empty string rather than `$null`, so the current `$null -eq $SafeReceipt` absent branch is wrong. This is a runner receipt-scoping defect and leaves the successful parse's top-level JSON shape unobserved; it is not product evidence and does not establish a transient root cause.

**Runtime invocation 10 empty-list correction record:** invocation 10 passed full topology, PostgreSQL, existing E2E, suspended-list JSON shape/property validation, all five suspended counts, the seventh database snapshot, the suspended assertion, and exact cleanup. It then reached `empty-list` and failed before `delete-bucket` or `complete`. The successful empty `ListObjectVersions` response validly omitted both optional `Versions` and `DeleteMarkers` properties, while the runner directly evaluated `$emptyList.Versions` and `$emptyList.DeleteMarkers` under `Set-StrictMode`. That deterministic source path throws before the zero-count assertion. This is a runner-only optional-property handling defect; the product's empty response is correct and no product change is authorized.

**Runtime invocation 11 completion record:** the fresh owned topology passed PostgreSQL, existing E2E, and every AWS substage from create through `complete`. All seven database snapshots, suspended-list structure/counts, exact cleanup, empty-list normalization, bucket deletion, and cleanup/residual checks passed. The runner emitted final `PASSED`; evidence and docs were promoted only afterward. Invocation 11 is consumed, no invocation 12 is authorized, and no further live runner execution belongs to this plan revision.

- [ ] **Step 1: Write and observe causal RED static assertions before repairing the runner/Compose file**

Extend `tests/client-smoke.Tests.ps1` to parse the new script separately and assert exact functions/guards for project-name grammar, canonical temp root, CreateNew ownership receipt, project-label preflight, local-image inspection, no pull, no install, offline build, strict Compose parsing/config, exact three-port preflight, AWS assertions, sanitized evidence, owned cleanup, and residual resource checks. Add these causal checks before changing the runner:

- `Assert-ComposeVersion` invokes exactly `docker compose version --short`; reject the old `--format` call and decorated `Docker Compose version v...` input. Match the complete short output with `^(?<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`, parse only the `core` capture with `[version]::TryParse`, and require it to be at least `2.23.1`;
- pure Docker-free parser cases require `2.39.2-desktop.1` → core `2.39.2` → accepted, reject decorated/leading/trailing text and illegal suffixes, and retain valid numeric core plus legal prerelease/build suffix support;
- parse the `Invoke-NativeCommand` declaration and every call AST, require each named argument to match a declared parameter exactly, require the offline Cargo call to use `-ArgumentList`, and reject nonexistent `-Arguments` before any live run;
- require an owned AWS config file under `RunRoot`, written with strict UTF-8 without BOM and exact LF content `[default]`, `region = us-east-1`, `s3 =`, and four-space-indented `addressing_style = path`; require `AWS_CONFIG_FILE=/work/aws-config` on every AWS container invocation, prove the file contains no credential/key/token fields, and reject every `AWS_S3_FORCE_PATH_STYLE` occurrence;
- require `Invoke-AwsJson` to prepend exactly one global `--output json` pair before the `s3api` service token; reject implicit output, duplicate output flags, or placement inside/after operation-specific arguments;
- define the optional parameter exactly as `[string]$SafeReceipt = ""`. Branch on `[string]::IsNullOrEmpty($SafeReceipt)`: the true branch is the existing ordinary `Invoke-Aws` path with default exit-code handling and emits no `aws-json-*` receipt; the false branch first requires case-sensitive literal `suspended-list` or throws one fixed validation error. Exactly the suspended-list call supplies `-SafeReceipt "suspended-list"`; every other call omits it. Forbid `$null -eq $SafeReceipt`, `$null -ne $SafeReceipt`, truthiness, or any other absent test that misclassifies the omitted empty string;
- only the explicit suspended-list branch invokes AWS with the fixed allowed native exit-code set `@(0,1,2,252,253,254,255)`. After the command returns, emit exactly one integer receipt `aws-json-command-exit=<n>` and reject any exit outside that set. If the allowed exit is nonzero, throw one fixed error immediately without parsing or emitting stdout, stderr, structured error content, or any later `aws-json-*` receipt;
- retain the existing bounded stdout capture and bounded single-document `ConvertFrom-Json` parse. On zero exit, parse before inspecting shape; emit exactly `aws-json-parse=passed` on success or `aws-json-parse=failed` before throwing one fixed parse error. A parse failure emits no stdout metric, shape, or property receipt;
- after a successful explicit suspended-list parse and before returning, emit these safe receipts in exact order: `aws-json-stdout-lines=<nonnegative integer>`, using `@($result.StdOut).Count`; `aws-json-stdout-chars=<nonnegative integer>`, using the length of the same newline-joined bounded JSON string; and `aws-json-shape=null|object|array|scalar`. Both numeric values must match exactly `^[0-9]+$` with no sign, whitespace, grouping, or suffix. Determine shape without changing the returned value: trimmed valid JSON equal to `null` is `null`, leading `{` is `object`, leading `[` is `array`, and every other successfully parsed document is `scalar`;
- only for `aws-json-shape=object`, inspect `@($parsedJson.PSObject.Properties.Name)` without dereferencing a possibly absent property and then emit exactly `aws-json-versions-property=0|1` followed by `aws-json-markers-property=0|1`. Missing `Versions` or `DeleteMarkers` must produce `0` without throwing inside `Invoke-AwsJson`; null, array, and scalar shapes emit neither property receipt. No safe receipt or shape branch may alter the parsed document returned to the caller;
- static AST/source tests lock the exact optional default and `[string]::IsNullOrEmpty` absent branch, prove ordinary calls emit zero `aws-json-*` receipts, require exactly one explicit suspended-list caller, and lock the exit/parse/line/char/shape/property receipt order and grammar. They also require fixed errors, nonthrowing missing-property inspection, unchanged bounded parsing, no raw output, and byte-for-byte-equivalent existing list/count/snapshot assertions. The existing generic `Write-Evidence` charset already accepts all fixed labels/values and must not be broadened;
- require every expected-failure AWS call to prepend exactly one global `--cli-error-format json` pair before `s3api`, require a nonzero exit, parse only top-level `$document.Code`, and reject a nested Error object, default-human parsing, numeric HTTP substring matching, `ResponseMetadata` assumptions, or successful exit;
- lock pure parser fixtures to exactly `{ "Code": "NoSuchKey", "Message": "redacted fixture" }` → `NoSuchKey`, `{ "Code": "404", "Message": "redacted fixture" }` → `404`, and `{ "Code": "405", "Message": "redacted fixture" }` → `405`. Each fixture asserts only the returned Code; the parser must discard and never emit/retain Message or raw JSON. Reject absent Code, unknown Code, non-string Code, malformed JSON, and multiple JSON documents;
- allow exactly those three expected codes by call site: current GET `NoSuchKey`, current HEAD `404`, and explicit marker HEAD `405`. Emit only `aws-error-code=<allowlisted-code>`; never emit/store raw JSON, Message, request ID, response body, stderr/stdout, bucket/key/version/path, or parser exception;
- require a validated `Write-AwsSubstage` helper that emits only `aws-substage=<allowlisted-label>` through the safe evidence charset immediately before every AWS operation/assertion boundary. Static AST/source tests require exactly 31 ordered call sites with this allowlist:

```text
cli-version
create-bucket
enable-versioning
put-first
assert-first
put-second
assert-second
simple-delete
assert-delete-marker
list-enabled
assert-enabled-order
get-historical
assert-historical
current-get-nosuchkey
current-head-404
marker-head-405
delete-marker-exact
get-promoted
assert-promoted
suspend-versioning
put-null-first
assert-null-first
put-null-second
assert-null-second
suspended-list
suspended-list-assert
exact-cleanup-delete
empty-list
assert-empty
delete-bucket
complete
```

Only `exact-cleanup-delete` may repeat at runtime, once per listed retained entry. No label may contain or derive from bucket, key, version ID, path, response, container/image ID, or exception text; the top-level catch remains generic;
- immediately after parsing suspended-list JSON and before `suspended-list-assert`, compute exactly once and retain five integer variables: total Versions, total DeleteMarkers, literal-null matches, first opaque-version matches, and second opaque-version matches. Emit them in this exact order as `suspended-version-count=<n>`, `suspended-marker-count=<n>`, `suspended-null-count=<n>`, `suspended-first-match-count=<n>`, and `suspended-second-match-count=<n>`;
- use those same five variables for the suspended-list assertion; do not recompute filters. Static tests lock the five labels/order, integer-only values, and placement between JSON parse and assertion, and reject any bucket/key/version ID/XML/body/path/response-derived evidence value;
- add a causal empty-list static contract that rejects direct `$emptyList.Versions` or `$emptyList.DeleteMarkers` access before property existence is proven. Require non-null `PSCustomObject`, inspect exactly `$emptyList.PSObject.Properties.Match("Versions")` and `.Match("DeleteMarkers")`, map an absent property independently to `@()`, and call `Get-AwsEntries` only when that matching property exists. Assert both resulting collection counts are exactly zero with the existing fixed empty-list error; emit no new receipt and never output/interpolate the document, property values, IDs, paths, body, XML, query, or errors;
- implement the empty-list checks locally rather than adding a shared helper: only two properties are involved, and suspended-list intentionally retains the stricter rule that `Versions` must exist. Static tests lock that suspended-list required-`Versions` behavior unchanged, allow absent `DeleteMarkers` there, and prove the empty-list branch alone tolerates both properties being absent;
- require one diagnostic-only `Write-PostgresVersionSnapshot` function with mandatory `State`, `SnapshotStage`, `Bucket`, and `Key` parameters. `SnapshotStage` has exactly this seven-value allowlist and call order:

```text
after-second-enabled-put
after-simple-marker
after-exact-marker-delete-and-restored-get
after-versioning-suspension
after-first-null-put
after-second-null-put
after-suspended-list-parse
```

- the function invokes only the owned validation project's PostgreSQL service through project-scoped `docker compose --project-name <owned> -f tests/compose.object-versioning-validation.yml exec -T postgres psql -X -U ipfs3 -d ipfs3 -v ON_ERROR_STOP=1 -A -t -F "|"`, uses a maximum 30-second native-command timeout, binds the generated bucket/key as psql variables, and runs one read-only aggregate query over `object_versions` plus `objects`. Every snapshot invocation uses that exact psql argument sequence; it must not read `.env`, log its command/SQL, or use an unowned project/service;
- require psql's stdout to contain exactly one nonempty pipe-delimited row and no second document/row. Parse seven decimal nonnegative integers in this exact order: total version rows, opaque rows, null rows, delete-marker rows, latest rows, linked-object rows, and object rows for that bucket/key. The read-only query serializes ascending positive sequences with `.` rather than `,`; parse the eighth field only when it is literal `none` or matches exactly `^[1-9][0-9]*(?:\.[1-9][0-9]*)*$`, then require parsed values to remain strictly ascending. Reject commas, zero/negative elements, duplicates, descending values, and every other character. Any timeout, nonzero exit, stderr, malformed field, extra row, or trailing raw output fails generically without emitting command, SQL, identifiers, paths, or raw errors;
- each snapshot emits only these nine fixed evidence shapes in order: `db-snapshot-stage=<allowlisted-stage>`, `db-total-version-rows=<n>`, `db-opaque-rows=<n>`, `db-null-rows=<n>`, `db-delete-marker-rows=<n>`, `db-latest-rows=<n>`, `db-linked-object-rows=<n>`, `db-object-rows=<n>`, and `db-sequences=<dot-list|none>`. The only valid sequence examples are `db-sequences=1.2.4` and `db-sequences=none`; comma-separated output is forbidden. All counts are parsed integers; no evidence may contain bucket/key/version ID/object ID/CID/tag/body/XML/SQL/path/raw response/raw error;
- static tests lock the function signature, read-only query shape, exact project-scoped Compose argv ending `exec -T postgres psql -X -U ipfs3 -d ipfs3 -v ON_ERROR_STOP=1 -A -t -F "|"`, 30-second bound, strict one-row parser, exact dot-sequence regex or `none`, comma rejection, nine-field evidence allowlist/order, exact seven call stages/order, and call placement after the specified AWS boundaries. Calls are standalone diagnostics: the function returns no value, its local parsed values never leave the function except through safe evidence, and no snapshot result may feed AWS commands, S3 assertions, cleanup, retries, or product behavior. Do not broaden the existing `Write-Evidence` charset;
- the `Invoke-OfflineGatewayBuild` condition has exactly two `Test-Path` command ASTs, each enclosed by its own parenthesized expression, and no `Test-Path` command element is parsed as argument-mode `-or`; require the source shape `if ((Test-Path -LiteralPath $vendorPath) -or (Test-Path -LiteralPath $archiveContext))` and reject `Test-Path -LiteralPath $vendorPath -or Test-Path`;
- the Cargo vendor native process sets `ProcessStartInfo.WorkingDirectory` to `$RepoRoot` and its static test proves that value is passed for `vendor --locked --offline`;
- the Compose `IPFS_S3_MASTER_KEY` is exactly one fixed lowercase nonzero 64-hex disposable test value; reject missing, malformed, uppercase, or `0{64}` values;
- `$State.Stage` is assigned only from `preflight`, `config`, `offline-build`, `compose-up`, `health`, `network`, `metadata`, `postgres`, `e2e`, `aws`, and `cleanup`, and is assigned immediately before each corresponding major stage;
- the top-level catch emits only `failed-stage=<allowlisted-stage>` plus a generic reason, never `$_`, exception message/type/stack, command output, IDs, paths, or secrets;
- static source assertions require distinct directory/receipt/environment/image/project ownership flags to be set immediately after each resource is observed created and require cleanup to handle every partial combination;
- both success and failure paths emit bounded safe receipt keys for diagnostics, Compose down, image removal, temp-root removal, environment restoration, and independent residual container/network/volume/image zero checks.

Parse the validation YAML as source and require no `container_name`, loopback-only PostgreSQL/gateway/Kubo validation ports, required environment interpolation, unique gateway image interpolation, and the exact fixed test-only master key.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -eq 0) { throw "Static evidence-runner RED unexpectedly passed" }
```

Expected: the Docker-free test first fails causally on invocation 10's direct missing-property access at empty-list while retaining every prior SafeReceipt, parser, snapshot, count, cleanup, and safety assertion. It becomes GREEN only when the empty object maps absent `Versions` and absent `DeleteMarkers` independently to empty arrays, present properties still flow through `Get-AwsEntries`, both counts must remain zero, suspended-list still requires `Versions`, and no raw data can escape. The test must not call Docker.

- [ ] **Step 2: Define the isolated validation topology**

The Compose file contains exactly `postgres`, `kubo`, and `gateway`, plus project-scoped PostgreSQL/Kubo data volumes. It has no top-level fixed project name and no `container_name`. It publishes only loopback validation ports: `127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_POSTGRES_PORT:?required}:5432`, `127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_KUBO_PORT:?required}:5001`, and `127.0.0.1:${IPFS_S3_OBJECT_VERSIONING_GATEWAY_PORT:?required}:9000`. Use exact runner-owned values 55436, 55003, and 59003 after proving all three are free. Gateway uses `${IPFS_S3_OBJECT_VERSION_IMAGE:?required}`, PostgreSQL 17, existing Kubo image, database URL on `postgres`, existing test credentials, and fixed disposable test master key `0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef`. This nonzero 64-hex value is validation-only, is not a production secret, and must never enter a production profile. Dependencies remain health-gated, and no default/PG/multi/Cluster/private-swarm file changes.

- [ ] **Step 3: Implement fail-closed preflight and ownership**

`scripts/object-versioning-smoke.ps1` has `[switch]$Run` and defaults to no live work. Before `-Run`, print `[RESULT] object-versioning-client=NOT RUN reason=execution-not-requested` and exit zero. With `-Run`:

- require Docker, Cargo, `tar.exe`, Compose >=2.23.1, and all exact local images; obtain Compose text only with `docker compose version --short`, parse its anchored numeric core/legal suffix as locked in Step 1, use `docker image inspect`, and never pull or install;
- generate `RunId` with the existing canonical grammar, project `ipfs3-ver-$RunId`, unique image tag, bucket, and direct-child temp root claimed with `FileMode.CreateNew`;
- after run-root ownership, create only `RunRoot/aws-config` with `[Text.UTF8Encoding]::new($false)`, exact LF/final-newline content `[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n`, validate it contains no credentials, and rely on owned temp-root cleanup to remove it;
- require project-label queries for containers/networks/volumes to succeed and return zero before ownership; require exact loopback ports 55436, 55003, and 59003 to bind independently before releasing them;
- preserve exact prior presence/value of every touched environment variable and restore absence with `Remove-Item -LiteralPath "Env:$name"`;
- run `docker compose ... config --quiet` before build/up; invoke Cargo vendor from the local cache with `ProcessStartInfo.WorkingDirectory = $RepoRoot` (or an equally exact manifest-root argument locked by the static test), build with `--pull=false --network none`, then `up --pull never --no-build`;
- parenthesize both offline-build `Test-Path` calls independently so `-or` remains the PowerShell boolean operator and cannot become a `Test-Path` argument;
- initialize `$State.Stage = "preflight"`, restrict all assignments to the exact stage allowlist, and set it before config, offline build, Compose up, health, network, metadata, PostgreSQL, E2E, AWS, and cleanup work;
- mark run-root directory ownership immediately after directory creation, receipt ownership immediately after `CreateNew`, environment-state ownership immediately after capture and before the first mutation, image ownership immediately after the unique image exists (including a post-failure exact probe), and project ownership immediately after any project-labeled resource exists (including partial `compose up`);
- clean partial ownership safely: an exact newly created direct-child run root may be removed under its directory flag even if receipt creation failed, while a receipt-owned root must additionally validate its exact receipt; image/project/environment cleanup uses only its matching flag;
- never use `docker system prune`, broad image deletion, wildcard filesystem deletion, a production Compose project, package installation, or new native dependencies.

- [ ] **Step 4: Implement exact PostgreSQL and AWS CLI evidence sequence**

After health checks, set `IPFS_S3_TEST_POSTGRES_URL` to the owned loopback database and run `cargo test --test postgres_versioning -- --nocapture --test-threads=1`. Then set `IPFS_S3_E2E_ENDPOINT=http://127.0.0.1:59003` and `IPFS_S3_E2E_KUBO_URL=http://127.0.0.1:55003` and run `cargo test --test e2e -- --nocapture --test-threads=1`. Restore all three variables exactly and abort before AWS if either Rust suite fails. Run the locally present AWS CLI image on the project network with endpoint `http://gateway:9000`, test credentials supplied only as container environment, owned config supplied only as `AWS_CONFIG_FILE=/work/aws-config`, and no `AWS_S3_FORCE_PATH_STYLE`. `Invoke-AwsJson` must place `--output json` before `s3api`; only the suspended-list invocation supplies `-SafeReceipt "suspended-list"`. Emit the exact allowlisted substage immediately before each operation/assertion boundary, then execute these assertions:

1. create a unique bucket and enable versioning;
2. upload distinct first/second bodies, capture two non-null canonical UUID `VersionId` values, require they differ, and require ETag values to be CID-shaped;
3. simple delete, require `DeleteMarker=true` and a third opaque marker ID;
4. list versions, require descending marker/second/first order and exact one latest marker;
5. explicit get of the first version, require first-body bytes;
6. current GET returns top-level `$document.Code=NoSuchKey`, current HEAD returns top-level `$document.Code=404`, and explicit marker HEAD returns top-level `$document.Code=405`, each with nonzero exit and safe `aws-error-code=<code>` evidence only. Do not retain Message/raw JSON or infer HTTP status from CLI text; signed integration tests remain the HTTP 404/405 and marker-header proof;
7. exact-delete the marker and require prior second content becomes current;
8. suspend versioning, upload a third body, require literal `VersionId = "null"`, then overwrite that null slot once and prove only one null entry remains while both opaque versions remain;
9. enumerate every returned content version and marker, exact-delete each in listed order without simple deletes, require the resulting list document to be a non-null `PSCustomObject`, safely map omitted optional `Versions` and `DeleteMarkers` properties to separate empty arrays, require both counts to be zero, then delete the bucket.

Insert the seven standalone snapshots at these exact boundaries: after second Enabled PUT ID/ETag/distinctness assertions; after simple marker creation assertions; after exact marker deletion plus restored GET byte assertion; after the versioning-suspension operation; after first null PUT ID assertion; after second null PUT ID assertion; and immediately after final suspended-list JSON parse but before its five safe counts and `suspended-list-assert`. Before every major call, set `$State.Stage` to its exact allowlisted value; inside `aws`, emit the exact safe substage before each boundary. For suspended-list only, emit `aws-substage=suspended-list`, invoke `Invoke-AwsJson -SafeReceipt "suspended-list"`, and require the helper's ordered command-exit, parse, stdout metric, shape, and conditional property receipts before it returns. The caller rejects null or any value not typed as `PSCustomObject` with fixed error `Suspended list JSON must be an object`; for an object, collect `@($suspendedList.PSObject.Properties.Name)`, reject absent `Versions` with fixed error `Suspended list JSON is missing Versions`, and never directly dereference a property before proving its name exists. Treat absent `DeleteMarkers` as `@()` and continue using the existing `Versions`, null/opaque filters, five count variables, final required counts, and `suspended-list-assert` unchanged. Perform all checks without outputting or interpolating the document. Only after caller shape validation emit the final DB snapshot, then the five integer count receipts. Snapshot data and safe receipt data are diagnostic-only and cannot change the AWS operation sequence or assertion inputs. Record AWS CLI version, AWS image ID, gateway image ID, package `0.1.0`, Git HEAD, spec SHA-256, command categories, redacted assertions, PostgreSQL test result, existing Rust E2E result, and cleanup result. Do not record credentials, authorization/signatures, request bodies, bucket/key/version/path values, internal IDs, database SQL, provider/Kubo raw responses, raw stdout/stderr, raw JSON values, raw exception text, or full filesystem paths.

At `empty-list`, reject null or non-`PSCustomObject` with fixed error `Empty version list JSON must be an object`. Evaluate `@($emptyList.PSObject.Properties.Match("Versions"))` and `@($emptyList.PSObject.Properties.Match("DeleteMarkers"))` before any property dereference. For each property independently, assign `@()` when its match count is zero; when present, pass only that proven property value through `Get-AwsEntries`. Require both resulting counts to equal zero or throw the existing fixed `Version list was not empty after exact deletes` error. Do not add a helper or receipt, do not weaken suspended-list's required `Versions`, and do not expose the document or any response-derived value.

- [ ] **Step 5: Make cleanup independent and exact**

Set `$State.Stage = "cleanup"` before diagnostics or teardown. On failure, attempt sanitized `docker compose ps`/logs only when project resources are owned or independently observed; report only `diagnostics=attempted`, `diagnostics=not-owned`, or `diagnostics=failed`, never log contents/IDs/paths. If project ownership is held, run project-scoped `down --volumes --remove-orphans`; remove only the exact owned unique gateway image; restore the captured environment; and remove only the canonical direct-child temp root under the directory/receipt rules from Step 3. Emit one bounded receipt for each outcome: `compose-down=passed|not-owned|failed`, `image-remove=passed|not-owned|failed`, `temp-remove=passed|not-owned|failed`, and `environment-restore=passed|not-needed|failed`. Independently query project-prefix containers, networks, volumes, and exact unique images and emit only zero/nonzero status; success requires all four residual counts to be zero. A cleanup or residual failure forces a nonzero exit and prevents PASSED evidence. A pre-existing receipt/resource is BLOCKED and untouched; no broad cleanup is permitted.

The top-level catch must retain no raw exception. It emits safe evidence `failed-stage=<allowlisted-stage>` and final result `reason=execution-failed` (or `reason=blocked` for pre-existing ownership) only. Both success and failure emit the cleanup receipts after cleanup completes.

- [ ] **Step 6: Create honest initial evidence and make static contract GREEN**

Create the evidence file with spec hash, package version, and exactly `OBJECT VERSIONING REAL CLIENT: NOT RUN`; no passing assertion may appear yet.

```powershell
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Client/evidence runner static contracts failed" }
$tokens = $null
$errors = $null
[System.Management.Automation.Language.Parser]::ParseFile("scripts/object-versioning-smoke.ps1", [ref]$tokens, [ref]$errors) | Out-Null
if ($errors.Count -ne 0) { throw "Object-versioning runner has PowerShell parse errors" }
$noRunOutput = @(& pwsh -NoProfile -File "scripts/object-versioning-smoke.ps1" 2>&1)
if ($LASTEXITCODE -ne 0) { throw "Object-versioning runner no-run check failed" }
$noRunText = ($noRunOutput | ForEach-Object { [string]$_ }) -join "`n"
if ($noRunText -cne "[RESULT] object-versioning-client=NOT RUN reason=execution-not-requested") {
    throw "Object-versioning runner no-run receipt changed"
}
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tracked runner/static diff check failed" }
foreach ($path in @(
    "scripts/object-versioning-smoke.ps1",
    "docs/superpowers/plans/2026-08-25-object-versioning.md"
)) {
    git -c core.autocrlf=false diff --no-index --check -- NUL $path
    if ($LASTEXITCODE -gt 1) { throw "Untracked runner/plan whitespace check failed: $path" }
}
```

Expected: the causal empty-list StrictMode RED is repaired and the Docker-free static suite, ParseFile, no-run receipt, tracked diff, and untracked runner/plan whitespace checks pass without Docker. Empty-list safely accepts omitted optional arrays but still rejects any nonempty result; suspended-list retains its required `Versions` rule and all fixed receipts/counts. All prior runner safety, assertions, snapshots, cleanup contracts, evidence `NOT RUN`, workflow hashes, and production Compose hashes remain unchanged.

### Task 12: Run all gates, collect real evidence, then update README and exactly three roadmap boxes

**Files:**
- Modify after a fail-closed nonlive lib stop: `src/import/worker.rs` (`#[cfg(test)]` fixture/regression only)
- Modify after nonlive Rust code gates and before Cluster static validation: `tests/cluster.Tests.ps1`
- Modify after all earlier gates: `docs/object-versioning-evidence-2026-08-25.log`
- Modify after passing live evidence: `README.md`
- Modify after passing live evidence: `ROADMAP.md`
- Verify: all Rust/source/test/script/Compose paths in the allowlist

**Interfaces:**
- Consumes: Tasks 1-11 complete working tree; isolated live runner; existing static deployment contracts; repository docs.
- Produces: complete automated/database/live evidence receipt; accurate README feature/limitation text; exactly the first three v0.6 boxes checked together.

**Observed runtime receipt for the current candidate:** all nonlive gates are GREEN: fmt/build, library tests `783`, integration tests `135`, clippy `0`, and release/PostgreSQL/multi-gateway/Cluster/client static contracts all passed after the approved Cluster two-hash refresh. Invocation 1 emitted only `[RESULT] object-versioning-client=FAILED reason=execution-failed`, emitted no evidence lines, and remains classified `FAILED-UNVERIFIED stage=unavailable`. Invocation 2 emitted `stage=preflight` and `failed-stage=preflight`; classify it as `FAILED-PREFLIGHT`. It owned no project, image, temp root/receipt, or environment state, and reported `cleanup-errors=0`. Direct read-only inspection then proved the decorated Compose output/parser defect and the corrected pure result `VERSION=<2.39.2-desktop.1> CORE=<2.39.2> ACCEPTED=True`. Both invocations failed in runner preflight before any owned resource or topology, so neither tested PostgreSQL, E2E, AWS, gateway startup, or object-versioning behavior. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, required images remain `5/5`, all three fixed ports are free, and project-prefix containers/networks/volumes/images are each `0`; no live PASS is claimed.

The confirmed causes are runner/validation defects: malformed boolean `Test-Path` syntax at offline-build entry, an all-zero release-rejected test master key, evidence beginning too late to attribute the failure, and ownership flags that can miss partial root/image/project creation. The existing client runner already proves the same offline Dockerfile/build shape; repair these contracts without package installation, dependency changes, or new native tools. An ignored temporary `.debug-journal.md` may record local diagnosis but is never a product artifact, changed path, evidence receipt, allowlist entry, staged path, or commit path.

**Observed nonlive fixture isolation receipt before invocation 3:** a standard `cargo test --lib` run stopped fail-closed at `782/783` when `import::worker::tests::process_shutdown_interrupts_without_mutation_and_leaves_lease_reclaimable` received SQLite `no such table: import_jobs`; no Docker/live command ran. The unchanged signature then passed on a full rerun `783/783`, exact rerun `1/1`, and import-worker scope `22/22`, confirming an intermittent fixture. Root cause: test helper `test_state` pooled `sqlite::memory:`, so independent pool connections could observe separate empty schemas. The test-only TDD correction sets `ConnectOptions.max_connections(1).min_connections(1)` and adds deterministic connection occupancy: a two-connection negative control is RED, the one-connection fixture is GREEN, shutdown stress is `10/10`, worker scope is now `23/23`, and standard parallel library runs are `784/784` twice. Formatting, diff, and LSP are clean. Production worker behavior and file-backed helpers remain unchanged; this gate evidence was completed before invocation 3.

**Observed invocation 3 receipt:** shutdown exact `1/1`, worker scope `23/23`, library `784/784`, integration `135/135`, fmt/build/clippy, and every release/PostgreSQL/multi-gateway/Cluster/client static contract passed. The runner then emitted `stage=offline-build` and `failed-stage=offline-build`; classify it as `FAILED-OFFLINE-BINDING`. PowerShell rejected nonexistent `-Arguments` before Cargo launched. No gateway image or Compose project was owned; cleanup removed the receipt-owned temp root, independently checked residuals, attempted environment restoration, and reported `cleanup-errors=0`. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, and no PostgreSQL/E2E/AWS/topology assertion ran. The owned build-only diagnostic listed in Task 11 proved vendor/tar/exact Docker build/image cleanup all GREEN; its root, image, timing output, and any temporary logs are diagnosis-only and never enter evidence, changed-path, allowlist, staged, or commit manifests. Invocations 1 and 2 failed in preflight and invocation 3 failed in parameter binding, so the actual topology-attempt count remains zero.

**Observed invocation 4 receipt:** every prelive gate passed: shutdown `1/1`, worker `23/23`, library `784/784`, integration `135/135`, fmt/build/clippy, diff, and all static contracts. It was the first actual topology attempt: topology/metadata, live `postgres-versioning=passed`, and `e2e=passed` completed. The runner entered `stage=aws` and failed before any assertion receipt with only coarse label `aws-cli-versioning-path-style`; classify it as `FAILED-AWS-UNOBSERVED`. Diagnostics, Compose down, residual checks, image removal, temp-root removal, and environment restoration were attempted and `cleanup-errors=0`. Evidence remained `NOT RUN`, README/ROADMAP remained unchanged, and HOSTED remained `NOT RUN`; no output was promoted before invocation 5.

**Observed invocation 5 receipt:** every prelive gate passed: shutdown `1/1`, worker `23/23`, library `784/784`, integration `135/135`, fmt/build/clippy, diff, and all static contracts. Topology, live PostgreSQL, and existing E2E passed. AWS substages passed create/enable, first and second puts, opaque IDs/CID ETags, marker creation, version-list ordering, and historical GET byte equality. The first failure occurred immediately after `current-get-nosuchkey`; classify it as `FAILED-AWS-ERROR-REPRESENTATION`. The product path through historical version retrieval passed, but the runner asserted non-contractual default stderr/HTTP text before emitting an expected-error assertion receipt. Diagnostics, Compose down, residual checks, image removal, temp-root removal, and environment restoration were attempted with `cleanup-errors=0`. Evidence remained `NOT RUN`, README/ROADMAP remained unchanged, HOSTED remained `NOT RUN`, and no output was promoted before invocation 6.

**Observed invocation 6 receipt and independent diagnostics:** every prelive gate passed: shutdown `1/1`, worker `23/23`, library `784/784`, integration `135/135`, fmt/build/clippy, diff, and all static contracts. Topology, live PostgreSQL, and existing E2E passed. AWS execution completed successfully through substage `suspended-list`, including structured current/marker error codes, create/enable, two puts with opaque IDs/CID ETags, marker/list order, and historical GET bytes. The runner then emitted/reached marker `suspended-list-assert` but failed inside that assertion; emitted/reached does not mean the substage completed. It did not reach `exact-cleanup-delete`, `empty-list`, `delete-bucket`, or `complete`. Classify it as `FAILED-AWS-SUSPENDED-COUNTS-UNOBSERVED`. Cleanup diagnostics/down/residual/image/temp/environment all completed with `cleanup-errors=0`; evidence remains `NOT RUN`, README/ROADMAP remain unchanged, and HOSTED remains `NOT RUN`. Three independent diagnostics are GREEN: (1) signed XML regression returns three Versions with one latest null and two opaque versions; (2) cached AWS CLI against fixed equivalent XML maps all three `VersionId` values to strings and equivalent PowerShell count expressions return `1/1/1`; (3) fresh PostgreSQL 17 named regression `postgres_suspended_null_replacement_preserves_opaque_versions` passes with rows `3`, sequences `[1, 2, 4]`, one latest null, two retained opaque versions, target `7/7`, and cleanup `0`. Only the five safe actual-live suspended-list counts are unavailable.

**Observed invocation 7 receipt and diagnostic boundary:** invocation 7 was consumed and failed safely inside `suspended-list-assert`. Its safe actual-live counts were versions `2`, markers `0`, null `1`, first opaque match `0`, and second opaque match `1`; cleanup completed with `cleanup-errors=0`, evidence remained `NOT RUN`, README/ROADMAP remained unchanged, and no promotion occurred. Independent signed SQLite, direct PostgreSQL, PostgreSQL-backed HTTP, cached AWS CLI fixed-XML mapping, and equivalent PowerShell parsing all produce three versions with one null and both opaque versions. The real runner was then the sole reproducer, but these facts did not prove a root cause.

**Observed invocation 8 receipt and final diagnostic boundary:** invocation 8 used a fresh owned topology and passed PostgreSQL, existing E2E, and the AWS sequence through the second suspended null PUT. Six of the seven configured database snapshots were reached and proved these exact states in field order `total/opaque/null/delete-marker/latest/linked-object/object/sequences`: after the second Enabled PUT `2/2/0/0/1/2/2/1.2` with bucket state Enabled; after simple marker creation `3/3/0/1/1/2/2/1.2.3`; after exact marker deletion and restored GET `2/2/0/0/1/2/2/1.2`; after suspension `2/2/0/0/1/2/2/1.2`; after first null PUT `3/2/1/0/1/3/3/1.2.3`; and after second null PUT `3/2/1/0/1/3/4/1.2.4`. It then emitted `aws-substage=suspended-list` and failed before list count receipts, before the seventh `after-suspended-list-parse` database snapshot, and before `suspended-list-assert`. Diagnostics, Compose down, residual project/image/temp checks, and environment restoration were attempted; `cleanup-errors=0` and project/image/temp residuals were zero. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, and HOSTED remains `NOT RUN`. These receipts refute database-state loss at every observed boundary and narrow the missing observation to AWS command invocation or bounded JSON parsing; they do not establish a transient root cause.

**Observed invocation 9 receipt and absent-string boundary:** invocation 9 used a fresh owned topology and passed PostgreSQL, existing E2E, and the AWS path through the second suspended null PUT. All six pre-list database-state receipts through null-second matched invocation 8; those six table snapshots retained exact field sequences `2/2/0/0/1/2/2/1.2`, `3/3/0/1/1/2/2/1.2.3`, `2/2/0/0/1/2/2/1.2`, `2/2/0/0/1/2/2/1.2`, `3/2/1/0/1/3/3/1.2.3`, and `3/2/1/0/1/3/4/1.2.4`. At `aws-substage=suspended-list`, it emitted `aws-json-command-exit=0` and `aws-json-parse=passed`, then failed before suspended count receipts, before the seventh `suspended-list-parsed` database snapshot, and before `suspended-list-assert`. The same diagnostic receipts appeared on every ordinary `Invoke-AwsJson` call, proving the omitted `[string]$SafeReceipt` was empty rather than null and the runner's absent branch was defective. Cleanup diagnostics/down/residual/image/temp/environment were attempted, `cleanup-errors=0`, and project/image/temp residuals were zero. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, and HOSTED remains `NOT RUN`. The database and successful parse receipts narrow the remaining unobserved boundary to parsed JSON shape/caller validation, but do not prove a transient or product root cause.

**Observed invocation 10 receipt and deterministic empty-list boundary:** invocation 10 used a fresh owned topology and passed PostgreSQL, existing E2E, and every AWS assertion through suspended-list plus exact cleanup. All six pre-list database snapshots matched the six exact count/sequence states recorded above for invocation 9. Suspended-list returned command exit `0`, bounded stdout lines `33` and characters `1161`, parse `passed`, shape `object`, `Versions` property `1`, and `DeleteMarkers` property `0`; the unchanged counts were versions `3`, markers `0`, null `1`, first opaque match `1`, and second opaque match `1`. The seventh snapshot then proved total versions `3`, opaque `2`, null `1`, and sequences `1.2.4`, and `suspended-list-assert` passed. The runner reached `exact-cleanup-delete`, completed exact deletion, reached `empty-list`, and then failed before `delete-bucket` or `complete`. Under `Set-StrictMode`, its direct access to omitted optional `$emptyList.Versions`/`.DeleteMarkers` deterministically threw even though the product correctly serialized empty vectors as absent properties. Cleanup diagnostics/down/residual/image/temp/environment were attempted, `cleanup-errors=0`, and all residuals were zero. Evidence remains `NOT RUN`, README/ROADMAP remain unchanged, and HOSTED remains `NOT RUN`. This is a confirmed runner-only cause, not a product failure or a transient-root-cause claim.

**Observed invocation 11 PASS and promotion receipt:** invocation 11 used a fresh owned topology. Live PostgreSQL and existing E2E passed; AWS completed every ordered substage from bucket creation through `complete`. All six pre-list snapshots remained correct. Suspended-list emitted command exit `0`, stdout lines `33`, stdout characters `1161`, parse `passed`, shape `object`, `Versions` property `1`, and `DeleteMarkers` property `0`; counts were versions `3`, markers `0`, null `1`, first opaque match `1`, and second opaque match `1`. The seventh snapshot retained versions `3`, opaque `2`, null `1`, and sequences `1.2.4`; suspended assertion, exact cleanup, normalized empty list, bucket deletion, and `complete` all passed. Cleanup diagnostics were not required; Compose down, independent container/network/volume/image residual checks, image removal, temp removal, and environment restoration completed with `cleanup-errors=0` and residual counts `0/0/0/0`. The runner result was `PASSED`.

The sanitized evidence was then promoted at `docs/object-versioning-evidence-2026-08-25.log` with SHA-256 `07510bf6c0b9e62366cfc13d910410d39ee996277a0bc9c797a642fd5f254d40`; all required local PASS lines are present and `HOSTED object-versioning: NOT RUN` remains explicit. README now contains exactly one `## Object versioning` section linking the approved spec and sanitized evidence. ROADMAP v0.6 has exactly Object versioning, ListObjectVersions, and DeleteMarker checked while Lifecycle and CORS remain unchecked. Post-promotion static contract, PowerShell parse, and diff checks passed. This receipt makes no hosted, release, push, or tag claim.

**Current execution cursor:** Task 12 is complete. Invocations 1 through 11 are consumed, no invocation 12 or further live rerun is authorized, and neither evidence nor README/ROADMAP may be promoted again. Do not replay any Task 12 command in this revision. Continue only with Task 13 final LSP/regression/boundary/identity/review gates on the passing promoted tree.

- [ ] **Step 1: Run formatting, compile, unit, and signed integration gates**

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "cargo fmt check failed" }
cargo build --locked
if ($LASTEXITCODE -ne 0) { throw "locked build failed" }
cargo test --lib import::worker::tests::process_shutdown_interrupts_without_mutation_and_leaves_lease_reclaimable -- --exact
if ($LASTEXITCODE -ne 0) { throw "Import worker shutdown regression failed" }
cargo test --lib import::worker::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "Import worker test scope failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "first full library stability run failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "second full library stability run failed" }
cargo test --test integration -- --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "integration tests failed" }
cargo clippy --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "clippy failed" }
```

Expected: exact shutdown regression is `1/1`, import-worker scope is at least the current `23/23`, both consecutive standard parallel library runs execute and pass at least `784` tests, and every remaining command exits zero. Any intermittent schema error stops before a live invocation and makes no additional live-service claim.

- [ ] **Step 2: Refresh exactly two causal Cluster baselines, then run every Docker-free static contract**

After Step 1 is green, update only these two values in the existing `$protectedHashes` map in `tests/cluster.Tests.ps1`:

```text
"src/s3/ops/object.rs": c2c56a1b0ceb8cb3fd577be83bdd464018657b08843773c4b09056d45966e3e6
"src/s3/ops/multipart.rs": 6a29e4204f59a413423ca25b8a6c79304a78ff3f2f69acc440408e73ccaba55a
```

Do not modify any other protected hash, function, assertion, workflow expectation, Compose expectation, Cluster/private-swarm contract, or production Cluster file. Verify the source identities and the exact four-line diff before running static contracts:

```powershell
$expectedSourceHashes = [ordered]@{
    "src/s3/ops/object.rs" = "c2c56a1b0ceb8cb3fd577be83bdd464018657b08843773c4b09056d45966e3e6"
    "src/s3/ops/multipart.rs" = "6a29e4204f59a413423ca25b8a6c79304a78ff3f2f69acc440408e73ccaba55a"
}
foreach ($entry in $expectedSourceHashes.GetEnumerator()) {
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $entry.Key).Hash.ToLowerInvariant()
    if ($actualHash -cne $entry.Value) { throw "Candidate source changed after Cluster baseline ruling: $($entry.Key)" }
}
$clusterDiff = @(git diff --unified=0 -- "tests/cluster.Tests.ps1")
if ($LASTEXITCODE -ne 0) { throw "Could not inspect Cluster static baseline diff" }
$changedLines = @($clusterDiff | Where-Object { $_ -cmatch '^[+-](?![+-])' } | Sort-Object)
$expectedLines = @(
    '-    "src/s3/ops/multipart.rs" = "3e08ab1dca7ded499193bee88e9c0014fe34a744d907259ac60573e57bcda4cf"',
    '+    "src/s3/ops/multipart.rs" = "6a29e4204f59a413423ca25b8a6c79304a78ff3f2f69acc440408e73ccaba55a"',
    '-    "src/s3/ops/object.rs" = "3cef9c6b169a717720b721399ea35d28e405943529a2fe7b680c16a01624296e"',
    '+    "src/s3/ops/object.rs" = "c2c56a1b0ceb8cb3fd577be83bdd464018657b08843773c4b09056d45966e3e6"'
) | Sort-Object
if ((Compare-Object $expectedLines $changedLines).Count -ne 0) {
    throw "tests/cluster.Tests.ps1 changed outside the two approved protected baselines"
}
```

Any later edit to either Rust file invalidates both the stored baseline receipt and all later reviews: recompute the affected SHA-256, update only its matching protected-hash line under a fresh explicit ruling, rerun this diff guard and `tests/cluster.Tests.ps1`, then rerun every verification affected by the code change before review.

```powershell
pwsh -NoProfile -File tests/release-validation.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "release workflow contract failed" }
pwsh -NoProfile -File tests/postgres-production-baseline.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "PostgreSQL deployment contract failed" }
pwsh -NoProfile -File tests/multi-gateway.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "multi-gateway deployment contract failed" }
pwsh -NoProfile -File tests/cluster.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "Cluster/private-swarm deployment contract failed" }
pwsh -NoProfile -File tests/client-smoke.Tests.ps1
if ($LASTEXITCODE -ne 0) { throw "client smoke infrastructure contract failed" }
```

Expected: default, PostgreSQL, multi-gateway, Cluster, private-swarm, and release workflow contracts remain green with no production-profile change.

- [ ] **Step 3: Record consumed invocation 11 PASS; authorize no further live run**

Invocation 11 is already consumed and PASSED with the exact receipt above. This step is historical state, not executable authorization. Do not call `scripts/object-versioning-smoke.ps1 -Run` again, do not allocate another RunId/project/image/temp root, and do not create invocation 12. The user's delegation does not override this closed live boundary.

```powershell
# Historical receipt only. Invocation 11 is consumed and PASSED.
# No live command is authorized here; continue at Task 13 Step 1.
```

Expected: no command runs. Invocation 11 remains the sole final local live PASS, invocation 12 remains unauthorized, and execution advances only to Task 13.

- [ ] **Step 4: Lock the promoted evidence identity and required lines**

```powershell
$evidence = [IO.File]::ReadAllText("docs/object-versioning-evidence-2026-08-25.log")
$evidenceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath "docs/object-versioning-evidence-2026-08-25.log").Hash.ToLowerInvariant()
if ($evidenceHash -cne "07510bf6c0b9e62366cfc13d910410d39ee996277a0bc9c797a642fd5f254d40") {
    throw "Promoted object-versioning evidence identity changed"
}
foreach ($required in @(
    "OBJECT VERSIONING REAL CLIENT: PASSED",
    "postgres_versioning=PASSED",
    "existing_e2e=PASSED",
    "enable_two_writes_delete_list=PASSED",
    "historical_get=PASSED",
    "marker_404_405=PASSED",
    "exact_marker_delete=PASSED",
    "suspended_null_overwrite=PASSED",
    "version_aware_cleanup=PASSED",
    "cleanup_verification=PASSED",
    "HOSTED object-versioning: NOT RUN"
)) {
    $linePattern = '(?m)^' + [regex]::Escape($required) + '$'
    if ([regex]::Matches($evidence, $linePattern).Count -ne 1) { throw "Evidence receipt line is missing or duplicated: $required" }
}
if ($evidence -match '(?i)authorization:|aws_secret|secret_access|wrapped_key|owner_object_id') {
    throw "Evidence contains a forbidden sensitive/internal field"
}
```

Expected: evidence has the exact promoted SHA-256, every required local PASS/HOSTED-NOT-RUN line appears exactly once, and no forbidden field is present. This is a read-only identity contract already satisfied by the promoted tree; do not rewrite evidence.

- [ ] **Step 5: Record the promoted README and ROADMAP state without further edits**

README already has exactly one `## Object versioning` section documenting Unversioned/Enabled/Suspended behavior, opaque IDs and literal `null`, simple markers, current/exact reads/deletes/copy/tagging, `ListObjectVersions`, marker 404/405 behavior, ETag=CID, encryption continuity, no `pin/rm`, and exact deletion required before bucket deletion. It links `docs/superpowers/specs/2026-08-25-object-versioning-design.md` and `docs/object-versioning-evidence-2026-08-25.log` and makes no Lifecycle, CORS, MFA Delete, Object Lock, reclamation, hosted, release, or replication claim.

`ROADMAP.md` now contains exactly this v0.6 state:

```text
- [x] Object versioning (enable/suspend on bucket)
- [x] ListObjectVersions
- [x] DeleteMarker support
- [ ] Lifecycle rules (expiration, transition)
- [ ] Bucket CORS configuration
```

This promotion is complete. Do not edit README/ROADMAP again in Task 12, do not alter package version `0.1.0`, and do not change any other roadmap checkbox.

- [ ] **Step 6: Record completed post-promotion static gates and hand off to Task 13**

```powershell
# Historical receipt: release, Cluster, client/evidence static contracts,
# PowerShell parse, diff/whitespace checks, and the required post-doc gate passed.
# Do not replay Task 12. Continue only with Task 13 Step 1.
```

Expected: Task 12 remains complete with the exact promoted evidence/docs identity. No hosted/release/tag claim is added and the only next executable work is Task 13.

### Task 13: Perform LSP, regression, boundary, identity, and final-review gates

**Files:**
- Verify every allowlisted path
- Verify every protected path remains unchanged
- Verify `docs/object-versioning-evidence-2026-08-25.log` has SHA-256 `07510bf6c0b9e62366cfc13d910410d39ee996277a0bc9c797a642fd5f254d40`, every required local PASS line exactly once, and explicit `HOSTED object-versioning: NOT RUN`
- Verify README contains exactly one `## Object versioning` section with the approved spec/evidence links, and ROADMAP has exactly the first three v0.6 boxes checked while Lifecycle/CORS remain unchecked
- Verify `tests/cluster.Tests.ps1` differs from HEAD only in the two approved object/multipart protected baseline values
- Verify `src/s3/route/import_object/tests.rs` and only the `#[cfg(test)]` region of `src/store/import/ownership.rs` contain the approved active-import fixture supersession, with deleted-bucket idempotency/no-resolver assertions preserved
- Verify `src/import/worker.rs` changes only the in-memory test fixture and deterministic connection-occupancy regression; production and file-backed worker helpers remain unchanged
- Verify `tests/postgres_versioning.rs` includes and executes `postgres_suspended_null_replacement_preserves_opaque_versions` with the exact three-row/sequence/null/opaque assertions under PostgreSQL 17
- Verify the permanent runner/static snapshot contract contains exactly seven read-only placements and nine safe evidence fields; every call uses exact `psql -X -U ipfs3 -d ipfs3 -v ON_ERROR_STOP=1 -A -t -F "|"`, sequence evidence is only dot-separated positive ascending values such as `1.2.4` or literal `none`, commas are rejected, `Write-Evidence` is not broadened, and no diagnostic value influences AWS/product behavior or leaks forbidden identifiers/content
- Verify `Invoke-AwsJson` declares `[string]$SafeReceipt = ""`, uses `[string]::IsNullOrEmpty($SafeReceipt)` for the ordinary no-receipt branch, and has exactly one explicit literal `suspended-list` caller. Lock the diagnostic exit allowlist to `@(0,1,2,252,253,254,255)` and the exact successful order `aws-json-command-exit`, `aws-json-parse`, stdout line count, stdout character count, shape, then object-only Versions/DeleteMarkers presence flags. Require fixed errors/no parse on nonzero, no metrics after parse failure, nonthrowing missing-property inspection, zero receipts for ordinary calls, no raw output, no receipt influence on assertions/control flow, and byte-for-byte-equivalent existing count/list/snapshot assertions
- Verify empty-list rejects null/non-`PSCustomObject`, calls `PSObject.Properties.Match` for both `Versions` and `DeleteMarkers` before any dereference, maps each absent optional property independently to `@()`, sends each present property through `Get-AwsEntries`, and requires both counts to be zero. Require no helper/new receipt/raw output, preserve suspended-list's mandatory `Versions` rule and optional `DeleteMarkers` rule, and reject any weakening of exact cleanup, empty-list, bucket deletion, or completion assertions
- Verify owned build-only diagnostic roots/images/logs/timing artifacts are absent and were never added to evidence or final manifests
- Verify `.debug-journal.md` remains ignored temporary diagnosis state during debugging, is absent from tracked/staged/allowlist/evidence/commit manifests, and is removed before final identity review
- No product edit is permitted after the current-revision final review receipt except a fresh fix→full verification→fresh review cycle

**Interfaces:**
- Consumes: completed Tasks 1-12 passing promoted tree, approved spec hash, exact promoted evidence hash, and orchestrator-owned final code review; no live rerun or Task 12 replay.
- Produces: exact changed-path/hash identity, clean diagnostics/regressions, one reviewable integrated commit if authorization remains in force, and no push/tag.

- [ ] **Step 1: Run LSP diagnostics on every changed Rust file**

Open each changed `.rs` path with `lsp_diagnostics` and require zero errors and zero warnings, explicitly including `src/import/worker.rs`, `src/s3/route/import_object/tests.rs`, `src/store/import/ownership.rs`, and `tests/postgres_versioning.rs`. Use `lsp_find_references` for `PublicationResult`, `resolve_version`, `delete_version_with_leases_guarded`, and `list_object_versions`; require all producers/consumers to match the Shared Domain Interfaces and no old latest-only production mutation path to remain. Inspect the ownership diff and require every object-versioning adaptation there to remain inside `#[cfg(test)]`; production ownership symbols and behavior must be byte-for-byte unchanged. Inspect `src/import/worker.rs` and require only test fixture/regression changes: production and file-backed helper code must be byte-for-byte unchanged. Require the named PostgreSQL suspended-null regression to compile and retain its exact row/sequence/null/opaque assertions.

- [ ] **Step 2: Run the final regression matrix once on the unchanged candidate**

```powershell
cargo fmt --check
if ($LASTEXITCODE -ne 0) { throw "final fmt failed" }
cargo test --lib import::worker::tests::process_shutdown_interrupts_without_mutation_and_leaves_lease_reclaimable -- --exact
if ($LASTEXITCODE -ne 0) { throw "final import worker shutdown regression failed" }
cargo test --lib import::worker::tests -- --nocapture
if ($LASTEXITCODE -ne 0) { throw "final import worker scope failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "final first library stability run failed" }
cargo test --lib
if ($LASTEXITCODE -ne 0) { throw "final second library stability run failed" }
cargo test --test integration -- --test-threads=1
if ($LASTEXITCODE -ne 0) { throw "final integration tests failed" }
cargo clippy --all-targets --all-features -- -D warnings
if ($LASTEXITCODE -ne 0) { throw "final clippy failed" }
```

Expected: exact shutdown test is `1/1`, import-worker scope is at least `23/23`, both standard parallel library runs pass at least the current `784` tests, and every remaining command has zero failures on the exact candidate submitted for final review. Do not rerun the live gate unless relevant inputs changed after its passing receipt.

- [ ] **Step 3: Enforce spec/package/protected-surface identity**

```powershell
$specPath = "docs/superpowers/specs/2026-08-25-object-versioning-design.md"
$specHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $specPath).Hash.ToLowerInvariant()
if ($specHash -cne "56e53b0983ac9f244c5379a50454c6f7e64ab4105ceccc45f9c5ed5cf34ef489") {
    throw "Approved spec identity changed"
}
$manifest = Get-Content -Raw -LiteralPath "Cargo.toml"
if ($manifest -notmatch '(?m)^name = "ipfs-s3-gateway"$' -or
    $manifest -notmatch '(?m)^version = "0\.1\.0"$' -or
    $manifest -notmatch '(?m)^edition = "2024"$' -or
    $manifest -notmatch '(?m)^s3s = "0\.14"$') {
    throw "Package or locked s3s contract changed"
}
$clusterBaselinePath = "tests/cluster.Tests.ps1"
$clusterBaselineText = [IO.File]::ReadAllText($clusterBaselinePath)
$crossFeatureBaselines = [ordered]@{
    "src/s3/ops/object.rs" = "c2c56a1b0ceb8cb3fd577be83bdd464018657b08843773c4b09056d45966e3e6"
    "src/s3/ops/multipart.rs" = "6a29e4204f59a413423ca25b8a6c79304a78ff3f2f69acc440408e73ccaba55a"
}
foreach ($entry in $crossFeatureBaselines.GetEnumerator()) {
    $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $entry.Key).Hash.ToLowerInvariant()
    if ($actualHash -cne $entry.Value) { throw "Reviewed source hash changed: $($entry.Key)" }
    $expectedBaselineLine = '    "' + $entry.Key + '" = "' + $entry.Value + '"'
    if (-not $clusterBaselineText.Contains($expectedBaselineLine, [StringComparison]::Ordinal)) {
        throw "Cluster protected baseline does not match reviewed source: $($entry.Key)"
    }
}
$finalClusterDiff = @(git diff --unified=0 -- $clusterBaselinePath)
if ($LASTEXITCODE -ne 0) { throw "Could not inspect final Cluster baseline diff" }
$finalClusterChangedLines = @($finalClusterDiff | Where-Object { $_ -cmatch '^[+-](?![+-])' } | Sort-Object)
$approvedClusterChangedLines = @(
    '-    "src/s3/ops/multipart.rs" = "3e08ab1dca7ded499193bee88e9c0014fe34a744d907259ac60573e57bcda4cf"',
    '+    "src/s3/ops/multipart.rs" = "6a29e4204f59a413423ca25b8a6c79304a78ff3f2f69acc440408e73ccaba55a"',
    '-    "src/s3/ops/object.rs" = "3cef9c6b169a717720b721399ea35d28e405943529a2fe7b680c16a01624296e"',
    '+    "src/s3/ops/object.rs" = "c2c56a1b0ceb8cb3fd577be83bdd464018657b08843773c4b09056d45966e3e6"'
) | Sort-Object
if ((Compare-Object $approvedClusterChangedLines $finalClusterChangedLines).Count -ne 0) {
    throw "Final Cluster contract diff exceeds the two approved protected baselines"
}
```

Expected: exact approved spec and package surface are unchanged; the two object-versioning source hashes equal their reviewed candidate identities, `tests/cluster.Tests.ps1` stores those exact values, and its entire diff is exactly two removed/added baseline pairs with no Cluster assertion weakening.

- [ ] **Step 4: Enforce the exact changed-path allowlist and required set**

Use this exact audit list, which includes the approved untracked spec and this plan:

```powershell
$allowed = @(
    "README.md",
    "ROADMAP.md",
    "docs/object-versioning-evidence-2026-08-25.log",
    "docs/superpowers/plans/2026-08-25-object-versioning.md",
    "docs/superpowers/specs/2026-08-25-object-versioning-design.md",
    "scripts/object-versioning-smoke.ps1",
    "src/error.rs",
    "src/import/publication.rs",
    "src/import/publication/zip.rs",
    "src/import/worker.rs",
    "src/s3/handler.rs",
    "src/s3/ops/bucket.rs",
    "src/s3/ops/mod.rs",
    "src/s3/ops/multipart.rs",
    "src/s3/ops/object.rs",
    "src/s3/ops/tagging.rs",
    "src/s3/ops/versioning.rs",
    "src/s3/route/decompress_zip.rs",
    "src/s3/route/import_object.rs",
    "src/s3/route/import_object/tests.rs",
    "src/store/bucket.rs",
    "src/store/entities/bucket.rs",
    "src/store/entities/mod.rs",
    "src/store/entities/object_version.rs",
    "src/store/import/ownership.rs",
    "src/store/migrations/m20260825_000001_object_versioning.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "src/store/object.rs",
    "src/store/object_version.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs",
    "tests/client-smoke.Tests.ps1",
    "tests/cluster.Tests.ps1",
    "tests/compose.object-versioning-validation.yml",
    "tests/integration.rs",
    "tests/postgres_versioning.rs"
)
$conditional = @(
    "src/import/publication.rs",
    "src/import/publication/zip.rs",
    "src/s3/ops/bucket.rs",
    "src/s3/route/import_object.rs"
)
$required = @($allowed | Where-Object { $_ -notin $conditional })
if ($allowed.Count -ne 37 -or $conditional.Count -ne 4 -or $required.Count -ne 33) {
    throw "Changed-path manifest count is inconsistent"
}
$tracked = @(git diff --name-only)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate tracked changes" }
$untracked = @(git ls-files --others --exclude-standard)
if ($LASTEXITCODE -ne 0) { throw "Could not enumerate untracked changes" }
$actual = @($tracked + $untracked | Where-Object { $_ } | Sort-Object -Unique)
$unexpected = @($actual | Where-Object { $_ -notin $allowed })
$missing = @($required | Where-Object { $_ -notin $actual })
$conditionalChanged = @($actual | Where-Object { $_ -in $conditional })
if ($unexpected.Count -ne 0) { throw "Unexpected changed paths: $($unexpected -join ', ')" }
if ($missing.Count -ne 0) { throw "Required changed paths absent: $($missing -join ', ')" }
if ($conditionalChanged.Count -ne 0) { throw "Audited conditional paths must remain unchanged: $($conditionalChanged -join ', ')" }
if ($actual.Count -ne 33) { throw "Final changed-path count is not exactly 33" }
git diff --cached --quiet
if ($LASTEXITCODE -ne 0) { throw "Staged changes exist before final review" }
git ls-files --error-unmatch -- ".debug-journal.md" *> $null
if ($LASTEXITCODE -eq 0) { throw ".debug-journal.md must never be tracked" }
if (Test-Path -LiteralPath ".debug-journal.md") {
    git check-ignore --quiet -- ".debug-journal.md"
    if ($LASTEXITCODE -ne 0) { throw ".debug-journal.md exists but is not ignored" }
    Remove-Item -LiteralPath ".debug-journal.md" -Force
}
if (Test-Path -LiteralPath ".debug-journal.md") { throw ".debug-journal.md survived final temporary-state cleanup" }
```

Expected: allowed `37`, conditional `4`, required/actual `33`, with no unexpected, missing, conditional-changed, or staged path. `.debug-journal.md` was never manifested and is absent after exact final cleanup. Task 10 proves both import publication paths and the import route enter the central primitive unchanged; Task 4/9 tests prove the unchanged bucket ops handler behavior.

- [ ] **Step 5: Run whitespace, placeholder, signature, and secret scans**

```powershell
git diff --check
if ($LASTEXITCODE -ne 0) { throw "Tracked diff whitespace check failed" }
foreach ($path in @(git ls-files --others --exclude-standard)) {
    git -c core.autocrlf=false diff --no-index --check -- NUL $path
    if ($LASTEXITCODE -gt 1) { throw "Untracked whitespace check failed: $path" }
}
$forbidden = @("T" + "BD", "T" + "ODO", "implement" + " later", "fill in" + " details", "similar to" + " Task")
$planText = [IO.File]::ReadAllText("docs/superpowers/plans/2026-08-25-object-versioning.md")
foreach ($term in $forbidden) {
    if ($planText.Contains($term, [StringComparison]::OrdinalIgnoreCase)) { throw "Plan placeholder term found: $term" }
}
rg -n "owner_object_id.*version_id|version_id.*owner_object_id|object_id.*x-amz-version-id|pin::rm|pin/rm" src tests
if ($LASTEXITCODE -gt 1) { throw "Boundary scan failed to execute" }
```

Manually inspect every boundary-scan match: tests/docs may state forbidden mappings or no-pin-rm assertions, but production must not map public IDs to owners or call removal. Compare every declared function/type field against Shared Domain Interfaces and the s3s contract; fix all inconsistent names before review.

- [ ] **Step 6: Record final identity and obtain final implementation review**

Record `git rev-parse HEAD`, `git status --short`, SHA-256 of the spec, plan, evidence, `Cargo.toml`, `Cargo.lock`, every changed file, and the exact commands/results from Tasks 12-13. Submit that exact identity to the orchestrator-owned requesting-code-review/final acceptance workflow. Any edit after submission invalidates the receipt and requires rerunning affected verification plus a fresh review.

- [ ] **Step 7: Create the single authorized commit only after final review**

Only after a passing current-identity final review and while the user's explicit commit authorization remains in force:

```powershell
$commitPaths = @(
    "README.md",
    "ROADMAP.md",
    "docs/object-versioning-evidence-2026-08-25.log",
    "docs/superpowers/plans/2026-08-25-object-versioning.md",
    "docs/superpowers/specs/2026-08-25-object-versioning-design.md",
    "scripts/object-versioning-smoke.ps1",
    "src/error.rs",
    "src/import/worker.rs",
    "src/s3/handler.rs",
    "src/s3/ops/mod.rs",
    "src/s3/ops/multipart.rs",
    "src/s3/ops/object.rs",
    "src/s3/ops/tagging.rs",
    "src/s3/ops/versioning.rs",
    "src/s3/route/decompress_zip.rs",
    "src/s3/route/import_object/tests.rs",
    "src/store/bucket.rs",
    "src/store/entities/bucket.rs",
    "src/store/entities/mod.rs",
    "src/store/entities/object_version.rs",
    "src/store/import/ownership.rs",
    "src/store/migrations/m20260825_000001_object_versioning.rs",
    "src/store/migrations/mod.rs",
    "src/store/mod.rs",
    "src/store/object.rs",
    "src/store/object_version.rs",
    "src/store/pinning/publication.rs",
    "src/store/pinning/publication/tests.rs",
    "tests/client-smoke.Tests.ps1",
    "tests/cluster.Tests.ps1",
    "tests/compose.object-versioning-validation.yml",
    "tests/integration.rs",
    "tests/postgres_versioning.rs"
)
if ($commitPaths.Count -ne 33) { throw "Commit manifest must contain exactly 33 reviewed changed paths" }
git add -- $commitPaths
if ($LASTEXITCODE -ne 0) { throw "Staging the reviewed allowlist failed" }
git diff --cached --check
if ($LASTEXITCODE -ne 0) { throw "Staged whitespace check failed" }
git commit -m "feat: add atomic object versioning" -m "Add bucket version state, public versions, delete markers, exact operations, version listing, and verified SQLite/PostgreSQL client evidence."
if ($LASTEXITCODE -ne 0) { throw "Integrated object-versioning commit failed" }
```

Do not amend, push, or tag. After commit, require `git status --short` to be empty; if it is not, report the residual paths and stop.

## Spec Coverage and Self-Review Record

- Data model, indexes, hidden-null backfill, permanent legacy-history privacy, and destructive-down refusal: Tasks 1-2.
- Unversioned/Enabled/Suspended state matrix and first-enable behavior: Tasks 2-4.
- Central put/copy/multipart/import/direct-ZIP/import-ZIP publication, projection, tags, leases, quota, jobs, guard, encryption, and no-pin-rm safety: Task 3 plus Task 10.
- Current/exact Get/Head/Copy, immutable encryption selection, marker 404/405 headers, and redacted errors: Tasks 4-5.
- Simple/exact DeleteObject and ordered/quiet/per-item DeleteObjects with promotion and lease ownership: Task 6.
- Current/exact version tagging and lease snapshot revalidation: Task 7.
- Ordinary-list projection and `ListObjectVersions` DTO/order/prefix/delimiter/first-unreturned pagination: Task 8.
- Bucket deletion, rollback, SQLite/PostgreSQL migration/concurrency, and the two active-import DeleteBucket regression-fixture adaptations: Task 9.
- Full signed operation/publication acceptance matrix: Task 10.
- Safe unique Docker/PostgreSQL/AWS evidence, static contract, cleanup ownership, and honest initial status: Task 11.
- Full regression/deployment gates, fail-closed in-memory import-worker fixture isolation, the exact two-line Cluster protected-baseline refresh, and README/three ROADMAP boxes only after evidence: Task 12.
- LSP, signature/boundary/secret scans, exact changed-path identity including spec+plan, final review, and one authorized commit with no push/tag: Task 13.

Self-review found no intentionally deferred behavior: every approved requirement maps to a task; non-goals remain excluded; every named shared type/function has one definition and explicit consumers; all command blocks use PowerShell syntax; package/dependency/deployment boundaries are protected; implementation receipt status is **waiting for receipt**.
