# Decompress-Zip Upload Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the non-standard `decompress-zip` upload extension so PutObject and Multipart uploads store the archive and stream-extract safe zip entries into the same bucket prefix.

**Architecture:** The implementation keeps standard S3 behavior on the existing `s3s` DTO path and adds a custom `S3Route` only where decoded query access or bounded Complete XML handling is required. One shared query module percent-decodes names and values exactly once using the same form semantics as s3s canonicalization: raw `+` becomes a space and `%2B` remains a literal plus. PutObject uses an archive-first streaming flow, Multipart UploadPart pins then atomically upserts its composite-key row, and Complete builds a pinned root before extraction. Kubo recursive pins are CID-wide and have no reference count, so archive/root/entry and every old/new Multipart part CID are conservatively retained across replacement, DB failure, Complete, and Abort; publication atomically replaces the latest object and deletes the upload (cascading parts), while each Complete attempt uses a fresh reconciliation identity distinct from the upload's stable encryption identity.

**Tech Stack:** Rust 2024, axum 0.8, s3s 0.14, SeaORM 1, Kubo RPC via reqwest 0.13, `async_zip 0.0.18`, `tokio-util::io::{ReaderStream, StreamReader}`, `tokio-util::compat`, `futures-io 0.3`, `quick-xml 0.41`, `http-body-util 0.1`, wiremock, reqwest-based SigV4 test client, rust-s3.

**Global Constraints:**
- Preserve existing S3 behavior when the `decompress-zip` query parameter is absent.
- ETag remains the IPFS CID for archives and extracted entries.
- Do not collect a whole archive or a whole zip entry into memory. CompleteMultipartUpload XML is the only buffered control body: read it frame-by-frame with a hard `4 * 1024 * 1024` byte limit; do not invoke an unbounded request-input collector or an equivalent API.
- Because every `POST ?uploadId=...` crosses the raw Complete route, it must preserve standard s3s 0.14 `CompletedPart` compatibility: accept each optional `ChecksumCRC32`, `ChecksumCRC32C`, `ChecksumCRC64NVME`, `ChecksumSHA1`, and `ChecksumSHA256` element at most once per `Part`, copy its entity/character-reference-decoded String value into the DTO without trimming its boundary whitespace, and map an attribute-free self-closing checksum element to `Some("")`. ETag parsing must call `ETag::parse_http_header`: retain typed strong/weak successes, retain the exact decoded String as `Strong` on `InvalidFormat`, map `InvalidChar` to `MalformedXML`, and map an attribute-free `<ETag/>` through that same empty-String helper; no ETag trim or quote stripping is permitted. Structural whitespace outside fields remains allowed; unknown XML, invalid nesting, field-internal Empty, duplicate checksum or ETag forms, and field-external content remain `MalformedXML`.
- `decompress-zip` requests with any SSE-S3 or SSE-C header return `400 InvalidArgument`; normal non-decompress SSE behavior remains unchanged.
- Unsafe zip paths and unsupported stream-unsafe zip entries are global rejects: return 400 and do not publish this request's new archive or entry DB records.
- A successfully staged entry whose final key equals the archive key is also a global reject. Scan only `ExtractOutcome.entries`, return `400 InvalidParameterValue` with `zip entry collides with archive key: {archive_key}`, and run the shared route helper before Put archive/entry publication or Multipart finalize/entry publication. Failed entries without a staged CID do not participate.
- Single safe-entry Kubo/pin/DB failures are partial failures recorded in `DecompressZipResult`; successful archive and other entries remain published.
- Kubo recursive `pin/add` and `pin/rm` operate by CID without reference counting. A CID may already back another object, equal both a single Multipart part and its completed root, or be shared by concurrent same-upload Complete attempts. No UploadPart replacement/DB-failure, Complete success/failure/outcome-unknown, or Abort path may unpin an old/new part CID; archive/root/entry failure cleanup is equally conservative. Redundant pins are accepted, and this plan does not claim to solve pin leakage.
- Do not add CID reference counting, an exclusive pin lease, or any migration beyond Task 1's already-planned decompression metadata columns; safe GC/reference-counting is separate future work.
- UploadPart must call `pin_add(new_cid)` and then one SeaORM/SeaQuery `ON CONFLICT (upload_id, part_number) DO UPDATE` statement for `cid/size/etag/uploaded_at`; remove the old `pin_rm + delete_part + insert_part` sequence. A DB error retains the new pin and leaves the old row intact. Do not add a part/refcount migration.
- `TransactionError::Transaction` means the SeaORM transaction body failed and rollback completed; `TransactionError::Connection` leaves commit outcome unknown. Every `complete_multipart_upload_inner` call creates a fresh `completion_attempt_id` used as `LatestObjectRow.id` and the exact reconciliation key. The persisted `upload.object_id` remains `encryption_object_id` for existing SSE key/nonce derivation only; it must never identify a completion attempt.
- Build migration up/down with SeaORM/SeaQuery `Table::alter()` statements. Use one alter option per statement for SQLite compatibility; down drops the two decompression columns in two calls and never rebuilds, drops, renames, or reparents `multipart_uploads` or touches `multipart_parts`.
- Custom query tuples must exist before SigV4 canonical-query signing. Keep both header Authorization and presigned-query real HTTP coverage; any post-sign change that alters final `decompress-zip` semantics must fail with `403 SignatureDoesNotMatch` before Kubo/DB mutation.
- Use `async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }` and enable `tokio-util`'s `compat` feature; reject formats not enabled by these features.
- Use PowerShell-compatible commands in verification notes.
- Git write commands are manual checkpoints only. Do not run `git commit`, `git push`, `git tag`, or other git write commands unless the user explicitly approves them in the active conversation.
- The complete implementation is one final changeset and one final commit only. Per-task checkpoints record manual progress; they never stage or commit files.

**Spec:** `docs/superpowers/specs/2026-07-07-decompress-zip-upload-design.md`

**Verified API evidence:** `async_zip 0.0.18` stream source and `ZipFileReader` docs at `https://docs.rs/async_zip/0.0.18/src/async_zip/base/read/stream.rs.html` and `https://docs.rs/async_zip/0.0.18/async_zip/base/read/stream/struct.ZipFileReader.html`; `quick-xml 0.41.0` emits entity and character references as `Event::GeneralRef(BytesRef)`, `BytesRef::resolve_char_ref() -> Result<Option<char>, Error>` resolves decimal/hex character references, and `BytesRef::decode()` exposes named-reference text, as documented at `https://docs.rs/quick-xml/0.41.0/quick_xml/events/enum.Event.html` and `https://docs.rs/quick-xml/0.41.0/quick_xml/events/struct.BytesRef.html`; s3s 0.14 `CompletedPart` has optional String-alias fields `checksum_crc32`, `checksum_crc32c`, `checksum_crc64nvme`, `checksum_sha1`, and `checksum_sha256`, corresponding to XML `ChecksumCRC32`, `ChecksumCRC32C`, `ChecksumCRC64NVME`, `ChecksumSHA1`, and `ChecksumSHA256`; locked `http-body-util 0.1.3` documents `BodyExt::frame(&mut self)` with `Self: Unpin` and one next-frame result at `https://docs.rs/http-body-util/0.1.3/http_body_util/trait.BodyExt.html#method.frame`, while `s3s 0.14.0 Body` implements both `http_body::Body<Data = Bytes>` and `Unpin` at `https://docs.rs/s3s/0.14.0/s3s/struct.Body.html`; locked SeaQuery 0.32.7 documents `Table::alter().add_column/drop_column` at `https://docs.rs/sea-query/0.32.7/sea_query/table/struct.TableAlterStatement.html`, and its SQLite builder rejects more than one alter option per statement, so migration up/down use separate calls per column.

**ETag parity evidence:** s3s 0.14 `crates/s3s/src/xml/mod.rs` deserializes ETag by calling `ETag::parse_http_header(value.as_bytes())`; a typed success is retained, `ParseETagError::InvalidFormat` becomes `ETag::Strong(value)`, and `ParseETagError::InvalidChar` becomes XML invalid content. The raw Complete parser uses that same public rule, with the invalid-content branch mapped to gateway `MalformedXML`.

---

## File Structure

- Modify `Cargo.toml` to add `async_zip` with `tokio` and `deflate` features, add direct `futures-io` and `http-body-util` dependencies, and enable `tokio-util`'s `compat` feature.
- Modify `src/lib.rs` and `src/main.rs` in Task 2, in the same step that creates the non-empty `src/zip/mod.rs`, to add `pub mod zip;` and `mod zip;` respectively; Task 1 must compile without declaring a nonexistent module.
- Modify `src/main.rs` again in Task 5 and modify `tests/integration.rs` to register `DecompressZipRoute` with `S3ServiceBuilder`.
- Modify `src/error.rs` to add zip/query/path variants and map them to 400-class S3 errors.
- Create `src/zip/mod.rs` for the zip module boundary.
- Create `src/zip/sanitize.rs` for target prefix and entry-name validation.
- Create `src/zip/response.rs` for `DecompressZipResult` and standard Complete XML serialization helpers.
- Create `src/zip/local_header.rs` for a bounded Tokio `AsyncBufRead` observer that exposes each local header's general-purpose flags and compression method without buffering entry data.
- Create `src/zip/extract.rs` for `async_zip` streaming extraction and duplex-to-Kubo entry uploads; failed/global-reject paths retain every successfully pinned entry CID.
- Modify `src/kubo/cat.rs` so its response-owned byte stream can meet the extractor's `'static` stream boundary under Rust 2024.
- Create `src/s3/http.rs` for the narrow signed decoded-length bridge used only after Hyper has decoded HTTP/1.1 chunk framing.
- Create `src/s3/query.rs` for one form-compatible query decoder shared by custom PutObject, Multipart Create, route matching, and raw Complete.
- Create `src/s3/route/mod.rs` and `src/s3/route/decompress_zip.rs` for custom PutObject and raw CompleteMultipartUpload routing, including the frame-wise `MAX_COMPLETE_MULTIPART_XML_BYTES` collector and deterministic over-limit/body-error mapping.
- Modify `src/s3/mod.rs` to export `pub mod http;`, `pub mod query;`, and `pub mod route;`.
- Modify `src/main.rs` to apply the decoded-length bridge outside the production fallback S3 service.
- Modify `src/s3/ops/object.rs` to expose plaintext Kubo add/pin and DB publish helpers without changing encrypted standard PutObject behavior.
- Modify `src/s3/ops/multipart.rs` to persist create-time decompression metadata; replace UploadPart's delete/insert and all part `pin_rm` paths with pin-then-upsert; refactor Complete into a reusable inner function carrying separate encryption/attempt identities; make Abort validate then delete only.
- Modify `src/store/object.rs` to extract the transaction-local latest-object update/insert primitive while preserving `upsert`'s current last-writer-wins retry behavior.
- Modify `src/store/multipart.rs` to replace `insert_part`/`delete_part` with atomic `upsert_part`, add `commit_completed_upload`, explicit rolled-back/outcome-unknown errors, and exact attempt-ID read-after-error reconciliation for the object update/insert plus upload deletion transaction.
- Modify `src/store/entities/multipart_upload.rs`, `src/store/multipart.rs`, `src/store/migrations/mod.rs`, and `src/store/mod.rs` for decompression metadata columns and migration registration.
- Create `src/store/migrations/m20260707_000001_decompress_zip.rs` for two schema-builder add statements and two schema-builder drop statements that preserve the existing `multipart_uploads` table identity, rows, and `multipart_parts` cascade FK.
- Create `tests/support/mod.rs` and modify `tests/support/sigv4.rs` and `tests/support/decompress.rs` for the real axum+s3s harness, deterministic header SigV4 and query presigning, HTTP/1.1 unknown-length streaming requests, the signed decoded-length bridge, inbound wire-header observation, scripted Kubo responses, ZIP fixtures, and request-log assertions.
- Modify `tests/integration.rs` for real-service signed decompression, multipart, failure, conservative pin-safety, authentication, encryption, abort, and standard-operation regressions.

---

### Task 1: Foundation dependencies, schema, and errors

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/error.rs`
- Modify: `src/store/entities/multipart_upload.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/mod.rs`
- Create: `src/store/migrations/m20260707_000001_decompress_zip.rs`

**Interfaces:**
- Consumes: existing `store::multipart::create_upload`, `store::run_migrations`, `AppError -> S3Error` conversion.
- Produces: `AppError::{InvalidZipParameter, InvalidZipEntry, ZipSlip, UnsupportedZipEntry, ZipArchiveRejected}`, multipart upload fields `decompress_zip_target: Option<String>` and `decompress_zip_result: bool`, `create_upload(..., decompress_zip_target: Option<&str>, decompress_zip_result: bool)`, and a reversible migration whose two up/two down `TableAlterStatement`s preserve the original parent table, upload/part data, and `multipart_parts` cascade FK on SQLite while emitting PostgreSQL `ALTER TABLE ... ADD/DROP COLUMN` SQL.

- [ ] **Step 1: Write the failing migration/entity/store tests**

Add these assertions to `src/store/mod.rs` tests so the new columns must exist after migrations:

```rust
#[tokio::test]
async fn test_multipart_upload_decompress_columns_exist() {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    run_migrations(&db).await.unwrap();

    let rows = db
        .query_all(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "PRAGMA table_info(multipart_uploads)",
            [],
        ))
        .await
        .unwrap();

    let names: Vec<String> = rows
        .iter()
        .map(|row| row.try_get::<String>("", "name").unwrap())
        .collect();

    assert!(
        names.contains(&"decompress_zip_target".to_string()),
        "multipart_uploads must persist the decompression target prefix"
    );
    assert!(
        names.contains(&"decompress_zip_result".to_string()),
        "multipart_uploads must persist whether Complete returns DecompressZipResult XML"
    );
}
```

Register `pub mod m20260707_000001_decompress_zip;` in `src/store/migrations/mod.rs`, then start `src/store/migrations/m20260707_000001_decompress_zip.rs` with the following red tests. They deliberately reference the four statement builders and `MigrationTrait` implementation added in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{
        ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement, TryGetable,
    };
    use sea_orm_migration::SchemaManager;

    use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;

    async fn upload_snapshot(db: &DatabaseConnection) -> (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ) {
        let row = db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT upload_id, object_id, bucket, key, \
                    CAST(created_at AS TEXT) AS created_at_text, encryption_mode, \
                    key_wrap, content_type, metadata \
             FROM multipart_uploads WHERE upload_id = 'upload-1'",
        )).await.unwrap().unwrap();
        (
            row.try_get("", "upload_id").unwrap(),
            row.try_get("", "object_id").unwrap(),
            row.try_get("", "bucket").unwrap(),
            row.try_get("", "key").unwrap(),
            row.try_get("", "created_at_text").unwrap(),
            row.try_get("", "encryption_mode").unwrap(),
            row.try_get("", "key_wrap").unwrap(),
            row.try_get("", "content_type").unwrap(),
            row.try_get("", "metadata").unwrap(),
        )
    }

    async fn part_snapshot(db: &DatabaseConnection) -> (String, i32, String, i64, String, String) {
        let row = db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT upload_id, part_number, cid, size, etag, \
                    CAST(uploaded_at AS TEXT) AS uploaded_at_text \
             FROM multipart_parts WHERE upload_id = 'upload-1' AND part_number = 1",
        )).await.unwrap().unwrap();
        (
            row.try_get("", "upload_id").unwrap(),
            row.try_get("", "part_number").unwrap(),
            row.try_get("", "cid").unwrap(),
            row.try_get("", "size").unwrap(),
            row.try_get("", "etag").unwrap(),
            row.try_get("", "uploaded_at_text").unwrap(),
        )
    }

    #[tokio::test]
    async fn up_down_preserves_upload_part_data_and_cascade_fk_on_sqlite() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON").await.unwrap();
        let manager = SchemaManager::new(&db);
        InitMigration.up(&manager).await.unwrap();

        db.execute_unprepared(
            "INSERT INTO buckets (name, owner) VALUES ('bucket', 'owner')",
        ).await.unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_uploads \
             (upload_id, object_id, bucket, key, encryption_mode, content_type, metadata) \
             VALUES ('upload-1', 'object-1', 'bucket', 'archive.zip', 'none', \
                     'application/zip', '{\"source\":\"seed\"}')",
        ).await.unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_parts \
             (upload_id, part_number, cid, size, etag) \
             VALUES ('upload-1', 1, 'QmPart', 7, 'QmPart')",
        ).await.unwrap();

        let upload_before = upload_snapshot(&db).await;
        let part_before = part_snapshot(&db).await;

        Migration.up(&manager).await.unwrap();
        let added = db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT decompress_zip_target, decompress_zip_result \
             FROM multipart_uploads WHERE upload_id = 'upload-1'",
        )).await.unwrap().unwrap();
        assert_eq!(added.try_get::<Option<String>>("", "decompress_zip_target").unwrap(), None);
        assert!(added.try_get::<bool>("", "decompress_zip_result").unwrap());

        Migration.down(&manager).await.unwrap();
        assert_eq!(upload_snapshot(&db).await, upload_before);
        assert_eq!(part_snapshot(&db).await, part_before);

        let foreign_keys = db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_key_list(multipart_parts)",
        )).await.unwrap();
        assert!(foreign_keys.iter().any(|row| {
            row.try_get::<String>("", "table").unwrap() == "multipart_uploads"
                && row.try_get::<String>("", "from").unwrap() == "upload_id"
                && row.try_get::<String>("", "to").unwrap() == "upload_id"
                && row.try_get::<String>("", "on_delete").unwrap() == "CASCADE"
        }));

        db.execute_unprepared(
            "DELETE FROM multipart_uploads WHERE upload_id = 'upload-1'",
        ).await.unwrap();
        let remaining = db.query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM multipart_parts WHERE upload_id = 'upload-1'",
        )).await.unwrap().unwrap();
        assert_eq!(remaining.try_get::<i64>("", "count").unwrap(), 0);
    }

    #[test]
    fn postgres_builders_emit_alter_column_sql_without_parent_rebuild() {
        let sql = [
            add_target_column().to_string(PostgresQueryBuilder),
            add_result_column().to_string(PostgresQueryBuilder),
            drop_result_column().to_string(PostgresQueryBuilder),
            drop_target_column().to_string(PostgresQueryBuilder),
        ];
        for statement in &sql {
            assert!(statement.starts_with("ALTER TABLE \"multipart_uploads\" "));
            assert!(!statement.contains("CREATE TABLE"));
            assert!(!statement.contains("DROP TABLE"));
            assert!(!statement.contains("multipart_parts"));
        }
        assert!(sql[0].contains("ADD COLUMN \"decompress_zip_target\" text"));
        assert!(sql[1].to_ascii_uppercase().contains(
            "ADD COLUMN \"DECOMPRESS_ZIP_RESULT\" BOOLEAN NOT NULL DEFAULT TRUE"
        ));
        assert!(sql[2].contains("DROP COLUMN \"decompress_zip_result\""));
        assert!(sql[3].contains("DROP COLUMN \"decompress_zip_target\""));
    }
}
```

Add this test to `src/store/multipart.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "test-bucket", None).await.unwrap();
        db
    }

    #[tokio::test]
    async fn create_upload_persists_decompress_metadata() {
        let db = setup().await;

        create_upload(
            &db,
            "upload-1",
            "object-1",
            "test-bucket",
            "archive.zip",
            "none",
            None,
            Some("application/zip"),
            None,
            Some("prefix/"),
            false,
        )
        .await
        .unwrap();

        let upload = get_upload(&db, "upload-1").await.unwrap();
        assert_eq!(upload.decompress_zip_target.as_deref(), Some("prefix/"));
        assert!(!upload.decompress_zip_result);
    }
}
```

Add this test to `src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_validation_errors_map_to_client_errors() {
        let err: S3Error = AppError::ZipSlip("../escape.txt".to_string()).into();
        assert_eq!(err.code().as_str(), "InvalidParameterValue");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        let err: S3Error = AppError::InvalidZipParameter("bad prefix".to_string()).into();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        for error in [
            AppError::InvalidZipEntry("bad.txt".to_string()),
            AppError::UnsupportedZipEntry("encrypted.txt".to_string()),
            AppError::ZipArchiveRejected("archive is corrupt".to_string()),
        ] {
            let err: S3Error = error.into();
            assert_eq!(err.code().as_str(), "InvalidParameterValue");
            assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test store::tests::test_multipart_upload_decompress_columns_exist --lib
cargo test store::migrations::m20260707_000001_decompress_zip::tests::up_down_preserves_upload_part_data_and_cascade_fk_on_sqlite --lib
cargo test store::migrations::m20260707_000001_decompress_zip::tests::postgres_builders_emit_alter_column_sql_without_parent_rebuild --lib
cargo test store::multipart::tests::create_upload_persists_decompress_metadata --lib
cargo test error::tests::zip_validation_errors_map_to_client_errors --lib
```

Expected: the migration tests fail to compile because the four `TableAlterStatement` builders and `MigrationTrait` implementation do not exist; the schema/store tests fail because the columns, model fields, and new `create_upload` parameters do not exist; the error test fails because the zip variants do not exist.

- [ ] **Step 3: Add dependency, errors, migration, entity fields, and store parameters**

Add `async_zip`, `futures-io`, and direct `http-body-util` dependencies to `Cargo.toml`, and replace the existing `tokio-util` dependency line so it enables both `io` and `compat`:

```toml
async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }
tokio-util = { version = "0.7", features = ["io", "compat"] }
futures-io = "0.3"
http-body-util = "0.1"
```

Add these variants to `AppError` in `src/error.rs`:

```rust
#[error("invalid decompress-zip parameter: {0}")]
InvalidZipParameter(String),

#[error("invalid zip entry: {0}")]
InvalidZipEntry(String),

#[error("zip entry escapes target prefix: {0}")]
ZipSlip(String),

#[error("unsupported zip entry: {0}")]
UnsupportedZipEntry(String),

#[error("zip archive rejected: {0}")]
ZipArchiveRejected(String),
```

Import `S3ErrorCode` alongside `S3Error`, add this helper, then update `From<AppError> for S3Error` with explicit client-error mappings before the catch-all arm. s3s 0.14 has no `InvalidParameterValue` macro variant, so the gateway must construct that S3 code explicitly and mark it as a 400:

```rust
use s3s::{S3Error, S3ErrorCode};

fn invalid_parameter_value(error: &AppError) -> S3Error {
    let mut s3_error = S3Error::with_message(
        S3ErrorCode::Custom("InvalidParameterValue".into()),
        error.to_string(),
    );
    s3_error.set_status_code(http::StatusCode::BAD_REQUEST);
    s3_error
}

// In From<AppError> for S3Error:
AppError::InvalidZipParameter(_) => s3_error!(InvalidArgument, "{}", e),
AppError::InvalidZipEntry(_)
| AppError::ZipSlip(_)
| AppError::UnsupportedZipEntry(_)
| AppError::ZipArchiveRejected(_) => invalid_parameter_value(&e),
```

Create `src/store/migrations/m20260707_000001_decompress_zip.rs`:

```rust
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn add_target_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column(
            ColumnDef::new(Alias::new("decompress_zip_target"))
                .text(),
        )
        .to_owned()
}

fn add_result_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column(
            ColumnDef::new_with_type(
                Alias::new("decompress_zip_result"),
                ColumnType::custom("BOOLEAN"),
            )
                .not_null()
                .default(true),
        )
        .to_owned()
}

fn drop_result_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("decompress_zip_result"))
        .to_owned()
}

fn drop_target_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("decompress_zip_target"))
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(add_target_column()).await?;
        manager.alter_table(add_result_column()).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(drop_result_column()).await?;
        manager.alter_table(drop_target_column()).await?;
        Ok(())
    }
}
```

Keep the Step 1 test module in the same file below this implementation. The target column intentionally omits `.not_null()` and a default, so it is nullable; the result column is `BOOLEAN NOT NULL DEFAULT TRUE`. The two separate up calls and two separate down calls are required: SeaQuery 0.32.7's SQLite builder panics when one `TableAlterStatement` contains multiple alter options. PostgreSQL and modern SQLite both execute these backend-built `ALTER TABLE` statements; no branch creates a replacement parent table, copies rows, changes table identity, or modifies `multipart_parts`.

Register it in `src/store/migrations/mod.rs`:

```rust
pub mod m20250701_000001_init;
pub mod m20260707_000001_decompress_zip;
```

Update the internal migrator in `src/store/mod.rs`:

```rust
use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;
use crate::store::migrations::m20260707_000001_decompress_zip::Migration as DecompressZipMigration;
use sea_orm_migration::prelude::*;

pub struct Migrator;
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(InitMigration), Box::new(DecompressZipMigration)]
    }
}
```

Add fields to `src/store/entities/multipart_upload.rs`:

```rust
pub decompress_zip_target: Option<String>,
pub decompress_zip_result: bool,
```

Update `src/store/multipart.rs::create_upload` signature and model:

```rust
#[allow(clippy::too_many_arguments)]
pub async fn create_upload<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    object_id: &str,
    bucket: &str,
    key: &str,
    encryption_mode: &str,
    key_wrap: Option<&str>,
    content_type: Option<&str>,
    metadata: Option<JsonValue>,
    decompress_zip_target: Option<&str>,
    decompress_zip_result: bool,
) -> AppResult<()> {
    let model = multipart_upload::ActiveModel {
        upload_id: Set(upload_id.to_owned()),
        object_id: Set(object_id.to_owned()),
        bucket: Set(bucket.to_owned()),
        key: Set(key.to_owned()),
        created_at: Set(Utc::now()),
        encryption_mode: Set(encryption_mode.to_owned()),
        key_wrap: Set(key_wrap.map(|s| s.to_owned())),
        content_type: Set(content_type.map(|s| s.to_owned())),
        metadata: Set(metadata),
        decompress_zip_target: Set(decompress_zip_target.map(str::to_owned)),
        decompress_zip_result: Set(decompress_zip_result),
    };

    multipart_upload::Entity::insert(model).exec(db).await?;
    Ok(())
}
```

Update existing `create_upload` callers to pass `None, true` until Task 6 adds decompression parsing.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test store::tests::test_multipart_upload_decompress_columns_exist --lib
cargo test store::migrations::m20260707_000001_decompress_zip::tests --lib
cargo test store::multipart::tests::create_upload_persists_decompress_metadata --lib
cargo test error::tests::zip_validation_errors_map_to_client_errors --lib
cargo test store --lib
cargo check --lib
cargo check --bin ipfs-s3-gateway
```

Expected: all commands exit 0. Task 1 does not mention or declare `zip`, so both the library and the binary independently compile before `src/zip/mod.rs` exists. The SQLite round trip proves the seeded upload/part values and same-parent cascade FK survive down; the PostgreSQL builder test proves both add and drop paths remain backend-built `ALTER TABLE` statements with `BOOLEAN NOT NULL DEFAULT TRUE` and no parent-table rebuild.

- [ ] **Step 5: Manual progress marker**

Record Task 1 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 2: Zip path sanitizer and XML response primitives

**Files:**
- Modify: `src/lib.rs`
- Modify: `src/main.rs`
- Create: `src/zip/mod.rs`
- Create: `src/zip/sanitize.rs`
- Create: `src/zip/response.rs`

**Interfaces:**
- Consumes: `AppError::{InvalidZipParameter, InvalidZipEntry, ZipSlip}` from Task 1.
- Produces: a real `crate::zip` module in both crate roots (`pub mod zip;` in the library and `mod zip;` in the binary), `normalize_target_prefix(prefix: &str) -> AppResult<String>`, `sanitize_entry(name: &str, target_prefix: &str) -> AppResult<SanitizedEntry>`, `DecompressZipResult`, `ExtractedEntry`, `ExtractFailure`, `decompress_result_xml(&DecompressZipResult) -> String`, and `complete_multipart_result_xml(bucket: &str, key: &str, etag: &str) -> String`.

- [ ] **Step 1: Write failing sanitizer and XML tests**

Create `src/zip/mod.rs`:

```rust
pub mod response;
pub mod sanitize;
```

In this same step, after the non-empty module file above exists, add `pub mod zip;` to `src/lib.rs` and `mod zip;` to `src/main.rs`. The binary has an independent module tree and cannot reach the library export through `crate::zip`. Do not add either declaration in Task 1 and do not create an empty zip-module scaffold.

Create `src/zip/sanitize.rs` with tests first:

```rust
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanitizedEntry {
    File { key: String },
    Directory,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_prefix_adds_trailing_slash() {
        assert_eq!(normalize_target_prefix("prefix").unwrap(), "prefix/");
        assert_eq!(normalize_target_prefix("prefix/").unwrap(), "prefix/");
        assert_eq!(normalize_target_prefix("").unwrap(), "");
    }

    #[test]
    fn normalize_prefix_rejects_escape_segments() {
        assert!(matches!(normalize_target_prefix("../x"), Err(AppError::InvalidZipParameter(_))));
        assert!(matches!(normalize_target_prefix("/abs"), Err(AppError::InvalidZipParameter(_))));
        assert!(matches!(normalize_target_prefix("C:/x"), Err(AppError::InvalidZipParameter(_))));
        assert!(matches!(normalize_target_prefix("dir\\x"), Err(AppError::InvalidZipParameter(_))));
    }

    #[test]
    fn sanitize_safe_file_under_prefix() {
        assert_eq!(
            sanitize_entry("foo/bar.txt", "prefix/").unwrap(),
            SanitizedEntry::File { key: "prefix/foo/bar.txt".to_string() }
        );
    }

    #[test]
    fn sanitize_safe_directory_skips_it() {
        assert_eq!(sanitize_entry("foo/", "prefix/").unwrap(), SanitizedEntry::Directory);
    }

    #[test]
    fn sanitize_rejects_unsafe_names() {
        for name in ["", "../escape.txt", "/etc/passwd", "C:/Windows/x", "dir\\file.txt", "a/./b", "a/../b"] {
            assert!(matches!(sanitize_entry(name, "prefix/"), Err(AppError::ZipSlip(_)) | Err(AppError::InvalidZipEntry(_))), "{name} should reject");
        }
    }
}
```

Create `src/zip/response.rs` with tests first:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedEntry {
    pub key: String,
    pub cid: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractFailure {
    pub entry_name: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecompressZipResult {
    pub archive_key: String,
    pub archive_cid: String,
    pub archive_size: i64,
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_escapes_result_fields_and_counts_entries() {
        let xml = decompress_result_xml(&DecompressZipResult {
            archive_key: "archive&.zip".to_string(),
            archive_cid: "QmArchive".to_string(),
            archive_size: 12,
            entries: vec![ExtractedEntry { key: "prefix/a<&>.txt".to_string(), cid: "QmEntry".to_string(), size: 5 }],
            failures: vec![ExtractFailure { entry_name: "bad&name".to_string(), code: "KuboAddFailed".to_string(), message: "pin <failed>".to_string() }],
        });

        assert!(xml.contains("<DecompressZipResult>"));
        assert!(xml.contains("<ArchiveKey>archive&amp;.zip</ArchiveKey>"));
        assert!(xml.contains("<ExtractedCount>1</ExtractedCount>"));
        assert!(xml.contains("<FailedCount>1</FailedCount>"));
        assert!(xml.contains("prefix/a&lt;&amp;&gt;.txt"));
        assert!(xml.contains("pin &lt;failed&gt;"));
    }

    #[test]
    fn complete_xml_matches_s3_shape() {
        let xml = complete_multipart_result_xml("bucket", "archive.zip", "QmRoot");
        assert!(xml.contains("<CompleteMultipartUploadResult>"));
        assert!(xml.contains("<Bucket>bucket</Bucket>"));
        assert!(xml.contains("<Key>archive.zip</Key>"));
        assert!(xml.contains("<ETag>\"QmRoot\"</ETag>"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test zip::sanitize::tests --lib
cargo test zip::response::tests --lib
```

Expected: tests fail because sanitizer and XML functions are not implemented.

- [ ] **Step 3: Implement sanitizer and XML serialization**

Replace the sanitizer functions with deterministic validation:

```rust
fn has_windows_drive(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic()
}

fn validate_segments(value: &str, parameter: bool) -> AppResult<()> {
    if value.contains('\\') || value.starts_with('/') || has_windows_drive(value) {
        return if parameter {
            Err(AppError::InvalidZipParameter(value.to_string()))
        } else {
            Err(AppError::ZipSlip(value.to_string()))
        };
    }
    for segment in value.split('/') {
        if segment == "." || segment == ".." {
            return if parameter {
                Err(AppError::InvalidZipParameter(value.to_string()))
            } else {
                Err(AppError::ZipSlip(value.to_string()))
            };
        }
    }
    Ok(())
}

pub fn normalize_target_prefix(prefix: &str) -> AppResult<String> {
    if prefix.is_empty() {
        return Ok(String::new());
    }
    validate_segments(prefix, true)?;
    let trimmed = prefix.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::InvalidZipParameter(prefix.to_string()));
    }
    Ok(format!("{trimmed}/"))
}

pub fn sanitize_entry(name: &str, target_prefix: &str) -> AppResult<SanitizedEntry> {
    if name.is_empty() {
        return Err(AppError::InvalidZipEntry("empty entry name".to_string()));
    }
    validate_segments(name, false)?;

    let is_directory = name.ends_with('/');
    let trimmed = name.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AppError::InvalidZipEntry(name.to_string()));
    }

    if is_directory {
        return Ok(SanitizedEntry::Directory);
    }

    let key = format!("{target_prefix}{trimmed}");
    if !target_prefix.is_empty() && !key.starts_with(target_prefix) {
        return Err(AppError::ZipSlip(name.to_string()));
    }
    Ok(SanitizedEntry::File { key })
}
```

Implement response serialization with `quick_xml::escape::escape`:

```rust
fn esc(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}

pub fn decompress_result_xml(result: &DecompressZipResult) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<DecompressZipResult>");
    xml.push_str(&format!("<ArchiveKey>{}</ArchiveKey>", esc(&result.archive_key)));
    xml.push_str(&format!("<ArchiveETag>{}</ArchiveETag>", esc(&result.archive_cid)));
    xml.push_str(&format!("<ArchiveSize>{}</ArchiveSize>", result.archive_size));
    xml.push_str(&format!("<ExtractedCount>{}</ExtractedCount>", result.entries.len()));
    xml.push_str(&format!("<FailedCount>{}</FailedCount>", result.failures.len()));
    xml.push_str("<Entries>");
    for entry in &result.entries {
        xml.push_str("<Entry>");
        xml.push_str(&format!("<Key>{}</Key>", esc(&entry.key)));
        xml.push_str(&format!("<ETag>{}</ETag>", esc(&entry.cid)));
        xml.push_str(&format!("<Size>{}</Size>", entry.size));
        xml.push_str("</Entry>");
    }
    xml.push_str("</Entries><Failures>");
    for failure in &result.failures {
        xml.push_str("<Failure>");
        xml.push_str(&format!("<EntryName>{}</EntryName>", esc(&failure.entry_name)));
        xml.push_str(&format!("<Code>{}</Code>", esc(&failure.code)));
        xml.push_str(&format!("<Message>{}</Message>", esc(&failure.message)));
        xml.push_str("</Failure>");
    }
    xml.push_str("</Failures></DecompressZipResult>");
    xml
}

pub fn complete_multipart_result_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
        esc(bucket),
        esc(key),
        esc(etag)
    )
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test zip::sanitize::tests --lib
cargo test zip::response::tests --lib
cargo check --bin ipfs-s3-gateway
```

Expected: all sanitizer and response tests pass, and the binary independently compiles with its own `mod zip;` declaration now that `src/zip/mod.rs` exists.

- [ ] **Step 5: Manual progress marker**

Record Task 2 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 3: Plain object Kubo staging and DB publish helpers

**Files:**
- Modify: `src/s3/ops/object.rs`

**Interfaces:**
- Consumes: existing `ByteCounter`, `kubo::add::stream_add`, `kubo::pin::pin_add`, and `store::object::upsert`.
- Produces: `StoredObject { cid: String, size: i64 }`, `add_plain_object_stream(...) -> S3Result<StoredObject>`, `publish_plain_object(...) -> S3Result<()>`, and `put_plain_object_stream(...) -> S3Result<StoredObject>`.

- [ ] **Step 1: Write failing helper tests**

Add tests to the existing `#[cfg(test)] mod tests` at the bottom of `src/s3/ops/object.rs`:

```rust
#[tokio::test]
async fn add_plain_object_stream_counts_pins_and_returns_cid() {
    use futures_util::stream;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmPlain\",\"Size\":\"5\"}\n"))
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmPlain\"]}"))
        .mount(&kubo)
        .await;

    let state = test_state(kubo.uri()).await;
    let stored = add_plain_object_stream(
        &state,
        stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"hello"))]),
    )
    .await
    .unwrap();

    assert_eq!(stored.cid, "QmPlain");
    assert_eq!(stored.size, 5);
}

#[tokio::test]
async fn publish_plain_object_writes_latest_metadata() {
    use futures_util::stream;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmPlain\",\"Size\":\"5\"}\n"))
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmPlain\"]}"))
        .mount(&kubo)
        .await;

    let state = test_state(kubo.uri()).await;
    crate::store::bucket::create(state.store.db(), "test-bucket", None).await.unwrap();
    let stored = add_plain_object_stream(
        &state,
        stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from_static(b"hello"))]),
    )
    .await
    .unwrap();

    publish_plain_object(&state, "test-bucket", "prefix/file.txt", None, None, &stored, false)
        .await
        .unwrap();

    let obj = crate::store::object::get_latest(state.store.db(), "test-bucket", "prefix/file.txt")
        .await
        .unwrap();
    assert_eq!(obj.cid, "QmPlain");
    assert_eq!(obj.size, 5);
    assert!(!obj.encrypted);
}
```

Add this private test helper inside the same test module:

```rust
async fn test_state(kubo_uri: String) -> std::sync::Arc<crate::state::AppState> {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    std::sync::Arc::new(crate::state::AppState {
        kubo: crate::kubo::KuboClient::new(kubo_uri),
        store: crate::store::Store::new(db),
        credentials: std::collections::HashMap::new(),
        master_key: crate::crypto::key::MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap(),
    })
}
```

Add `pin_add_error_does_not_remove_a_possibly_shared_cid` by scripting `/add -> QmShared`, `/pin/add -> 500`, and a permissive `/pin/rm` recorder; expect `InternalError` and assert `QmShared` never appears under `/api/v0/pin/rm`. Add `publish_failure_keeps_the_successfully_pinned_cid` by dropping or invalidating the test DB after `add_plain_object_stream` returns `QmShared`, calling `publish_plain_object`, expecting an error, and asserting no `/pin/rm?arg=QmShared`. These tests lock the conservative rule even when pin success or CID exclusivity cannot be proven.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test s3::ops::object::tests::add_plain_object_stream_counts_pins_and_returns_cid --lib
cargo test s3::ops::object::tests::publish_plain_object_writes_latest_metadata --lib
cargo test s3::ops::object::tests::pin_add_error_does_not_remove_a_possibly_shared_cid --lib
cargo test s3::ops::object::tests::publish_failure_keeps_the_successfully_pinned_cid --lib
```

Expected: tests fail because the helper functions and `StoredObject` do not exist; against any transitional implementation that cleans up on error, the two conservative-pin tests fail on an observed `/pin/rm` call.

- [ ] **Step 3: Implement helper functions without changing SSE branches**

Add near `ByteCounter` in `src/s3/ops/object.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub cid: String,
    pub size: i64,
}

pub async fn add_plain_object_stream<S, E>(
    state: &Arc<AppState>,
    stream: S,
) -> S3Result<StoredObject>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let (counter, count_handle) = ByteCounter::new();
    let counted = counter.wrap(stream);
    let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;

    if let Err(e) = crate::kubo::pin::pin_add(&state.kubo, &cid).await {
        return Err(s3s::s3_error!(InternalError, "pin: {e}"));
    }

    Ok(StoredObject {
        cid,
        size: count_handle.load(Ordering::Relaxed) as i64,
    })
}

pub async fn publish_plain_object(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    metadata: Option<serde_json::Value>,
    stored: &StoredObject,
    multipart: bool,
) -> S3Result<()> {
    let object_id = uuid::Uuid::new_v4().to_string();
    if let Err(e) = crate::store::object::upsert(
        state.store.db(),
        &object_id,
        bucket,
        key,
        &stored.cid,
        stored.size,
        content_type,
        &stored.cid,
        metadata,
        false,
        None,
        multipart,
    )
    .await
    {
        return Err(e.into());
    }
    Ok(())
}

#[allow(dead_code)]
pub async fn put_plain_object_stream<S, E>(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    content_type: Option<&str>,
    metadata: Option<serde_json::Value>,
    stream: S,
    multipart: bool,
) -> S3Result<StoredObject>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    let stored = add_plain_object_stream(state, stream).await?;
    publish_plain_object(state, bucket, key, content_type, metadata, &stored, multipart).await?;
    Ok(stored)
}
```

Do not refactor the existing `put_object` arms in this task. Add only the new helpers; the standard plaintext, SSE-S3, and SSE-C code paths remain byte-for-byte unchanged and are covered again in Task 8. `put_plain_object_stream` remains a deliberately retained composition API for a later consumer, so its currently unused status is explicitly allowed. Task 5 instead calls staged add and publish helpers separately, which lets it reject a global archive-key collision before publishing any object.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test s3::ops::object::tests::add_plain_object_stream_counts_pins_and_returns_cid --lib
cargo test s3::ops::object::tests::publish_plain_object_writes_latest_metadata --lib
cargo test s3::ops::object::tests::pin_add_error_does_not_remove_a_possibly_shared_cid --lib
cargo test s3::ops::object::tests::publish_failure_keeps_the_successfully_pinned_cid --lib
cargo test s3::ops::object --lib
```

Expected: helper tests pass, both error tests observe zero `/pin/rm` calls for `QmShared`, and existing object operation tests still pass.

- [ ] **Step 5: Manual progress marker**

Record Task 3 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 4: Streaming zip extractor with bounded local-header observation

**Files:**
- Modify: `src/zip/mod.rs`
- Create: `src/zip/local_header.rs`
- Create: `src/zip/extract.rs`

**Interfaces:**
- Consumes: `sanitize_entry`, `add_plain_object_stream`, `StoredObject`, and `ExtractedEntry`/`ExtractFailure` from previous tasks; `async_zip 0.0.18::base::read::stream::ZipFileReader::with_tokio`.
- Produces: `LocalHeaderMeta { general_purpose_flags: u16, compression_method: u16 }`, `LocalHeaderObserver<R>`, `LocalHeaderProbe::{begin,take}`, `ExtractOutcome { entries, failures }`, and `extract_zip_stream<S,E>(...) -> S3Result<ExtractOutcome>`. Extraction exposes successful entry CIDs through `entries` but has no failure-cleanup API: even a CID first staged by this request may equal an existing object or concurrent request CID, so request-local tracking does not prove pin ownership.

- [ ] **Step 1: Add deterministic local-header fixtures and failing policy tests**

Update `src/zip/mod.rs`:

```rust
pub mod extract;
pub mod local_header;
pub mod response;
pub mod sanitize;
```

Build all three single-entry fixtures in the `src/zip/extract.rs` test module. This helper emits a complete ZIP with a local header, optional descriptor, central directory, and EOCD; it never relies on a host ZIP tool:

```rust
fn push_u16(out: &mut Vec<u8>, value: u16) { out.extend_from_slice(&value.to_le_bytes()); }
fn push_u32(out: &mut Vec<u8>, value: u32) { out.extend_from_slice(&value.to_le_bytes()); }

fn single_entry_zip(method: u16, descriptor: bool) -> Vec<u8> {
    const NAME: &[u8] = b"file.txt";
    const CRC32_HELLO: u32 = 0x3610_a686;
    const STORED: &[u8] = b"hello";
    const DEFLATED: &[u8] = &[0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00];
    let compressed = match method { 0 => STORED, 8 => DEFLATED, _ => panic!("fixture method") };
    let flags = if descriptor { 1u16 << 3 } else { 0 };
    let mut out = Vec::new();

    push_u32(&mut out, 0x0403_4b50);
    push_u16(&mut out, 20); push_u16(&mut out, flags); push_u16(&mut out, method);
    push_u16(&mut out, 0); push_u16(&mut out, 0);
    push_u32(&mut out, if descriptor { 0 } else { CRC32_HELLO });
    push_u32(&mut out, if descriptor { 0 } else { compressed.len() as u32 });
    push_u32(&mut out, if descriptor { 0 } else { STORED.len() as u32 });
    push_u16(&mut out, NAME.len() as u16); push_u16(&mut out, 0);
    out.extend_from_slice(NAME); out.extend_from_slice(compressed);
    if descriptor {
        push_u32(&mut out, 0x0807_4b50); push_u32(&mut out, CRC32_HELLO);
        push_u32(&mut out, compressed.len() as u32); push_u32(&mut out, STORED.len() as u32);
    }

    let central_offset = out.len() as u32;
    push_u32(&mut out, 0x0201_4b50);
    push_u16(&mut out, 20); push_u16(&mut out, 20); push_u16(&mut out, flags); push_u16(&mut out, method);
    push_u16(&mut out, 0); push_u16(&mut out, 0); push_u32(&mut out, CRC32_HELLO);
    push_u32(&mut out, compressed.len() as u32); push_u32(&mut out, STORED.len() as u32);
    push_u16(&mut out, NAME.len() as u16); push_u16(&mut out, 0); push_u16(&mut out, 0);
    push_u16(&mut out, 0); push_u16(&mut out, 0); push_u32(&mut out, 0); push_u32(&mut out, 0);
    out.extend_from_slice(NAME);
    let central_size = out.len() as u32 - central_offset;
    push_u32(&mut out, 0x0605_4b50); push_u16(&mut out, 0); push_u16(&mut out, 0);
    push_u16(&mut out, 1); push_u16(&mut out, 1); push_u32(&mut out, central_size);
    push_u32(&mut out, central_offset); push_u16(&mut out, 0);
    out
}
```

Add these tests with a one-second timeout around each extractor call and wiremock `/add` + `/pin/add` success responses for accepted entries:

```rust
#[tokio::test]
async fn stored_without_descriptor_is_accepted() {
    let outcome = extract_fixture(single_entry_zip(0, false)).await.unwrap();
    assert_eq!(outcome.entries[0].key, "prefix/file.txt");
    assert!(outcome.failures.is_empty());
}

#[tokio::test]
async fn deflate_with_descriptor_is_accepted() {
    let outcome = extract_fixture(single_entry_zip(8, true)).await.unwrap();
    assert_eq!(outcome.entries[0].size, 5);
    assert!(outcome.failures.is_empty());
}

#[tokio::test]
async fn stored_with_descriptor_is_rejected_before_entry_upload() {
    let (state, kubo) = extractor_state_with_add_expectation(0).await;
    let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(
        bytes::Bytes::from(single_entry_zip(0, true)),
    )]);
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        extract_zip_stream(&state, "prefix/", stream),
    ).await.expect("Stored+descriptor must reject before reading an unbounded entry").unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidParameterValue");
    assert!(kubo.received_requests().await.unwrap().iter().all(|r| r.url.path() != "/api/v0/add"));
}
```

`extractor_state_with_add_expectation(n)` mounts `/api/v0/add` with `.expect(n)`, `/api/v0/pin/add` with `.expect(n)`, runs migrations, and returns `(Arc<AppState>, MockServer)`. `extract_fixture` calls it with `n = 1`, wraps the fixture in `stream::iter`, and applies the same one-second timeout. Retain the legal multi-entry, traversal, and corrupt-entry tests; add `entry_upload_failure_drains_entry_and_continues` with a two-entry fixture, first `/add` = 500 and second `/add` = `QmSecond`, asserting one `EntryUploadFailed` plus a successfully staged second entry. Add `global_reject_after_one_entry_keeps_the_entry_pin`: the first safe entry adds/pins `QmSharedEntry`, the next name is traversal, the extractor returns 400, and the Kubo log contains no `/pin/rm?arg=QmSharedEntry`. Add `entry_read_failure_after_pin_keeps_the_entry_pin` with a truncated entry stream that reaches add/pin before copy/done fails; assert an `EntryReadFailed` outcome and no removal of `QmSharedEntry`.

- [ ] **Step 2: Run the tests and verify the missing observer/policy fails**

```powershell
cargo test zip::extract::tests::stored_without_descriptor_is_accepted --lib
cargo test zip::extract::tests::deflate_with_descriptor_is_accepted --lib
cargo test zip::extract::tests::stored_with_descriptor_is_rejected_before_entry_upload --lib
cargo test zip::extract::tests::entry_upload_failure_drains_entry_and_continues --lib
cargo test zip::extract::tests::global_reject_after_one_entry_keeps_the_entry_pin --lib
cargo test zip::extract::tests::entry_read_failure_after_pin_keeps_the_entry_pin --lib
```

Expected: compilation fails because `LocalHeaderObserver`, `extract_zip_stream`, and the transfer/drain behavior are not implemented; no nonexistent `ZipEntry` descriptor accessor is referenced.

- [ ] **Step 3: Implement the bounded local-header observer**

Create `src/zip/local_header.rs` with this public contract:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalHeaderMeta {
    pub general_purpose_flags: u16,
    pub compression_method: u16,
}

impl LocalHeaderMeta {
    pub fn uses_descriptor(self) -> bool {
        self.general_purpose_flags & (1 << 3) != 0
    }
}

pub struct LocalHeaderObserver<R> {
    inner: tokio::io::BufReader<R>,
    shared: std::sync::Arc<std::sync::Mutex<ProbeState>>,
    seen_generation: u64,
    fill_observed: usize,
}

#[derive(Clone)]
pub struct LocalHeaderProbe {
    shared: std::sync::Arc<std::sync::Mutex<ProbeState>>,
}

pub fn observe_local_headers<R>(reader: R) -> (LocalHeaderObserver<R>, LocalHeaderProbe)
where
    R: tokio::io::AsyncRead + Unpin;

impl LocalHeaderProbe {
    pub fn begin(&self);
    pub fn take(&self) -> std::io::Result<LocalHeaderMeta>;
}
```

Implementation invariants are exact:

1. `ProbeState` contains `generation: u64`, `armed: bool`, and `header: Vec<u8>` with capacity 30. `begin()` increments the generation, clears the vector, and arms it.
2. `LocalHeaderObserver` implements both `tokio::io::AsyncRead` and `tokio::io::AsyncBufRead`. It wraps `tokio::io::BufReader<R>` **inside** the observer, so BufReader read-ahead cannot bypass observation. It copies at most `30 - header.len()` bytes from the current logical reader position while armed; archive and entry bodies are never copied into observer state.
3. `poll_read` observes only the newly filled `ReadBuf` range. `poll_fill_buf` tracks `fill_observed`, observes only the unobserved prefix, and `consume(amt)` reduces `fill_observed` with `saturating_sub(amt)`. A generation change resets `fill_observed` before the first read for the next `next_with_entry()` call.
4. `take()` disarms, requires exactly 30 captured bytes, requires bytes `0..4 == [0x50, 0x4b, 0x03, 0x04]`, parses flags from little-endian bytes `6..8`, and parses compression method from little-endian bytes `8..10`. A short/mismatched header returns `InvalidData`.

Add observer-only tests that feed fragmented 1-byte chunks through `tokio_util::io::StreamReader`, call `begin()`, read a complete local header, and assert `(flags, method)` is `(0, 0)`, `(8, 8)`, and `(8, 0)` for the three fixtures. This proves observation is independent of source chunk boundaries.

- [ ] **Step 4: Implement extraction with `with_tokio`, strict local-header policy, and correct error bounds**

Use these imports and bounds in `src/zip/extract.rs`:

```rust
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};

pub async fn extract_zip_stream<S, E>(
    state: &std::sync::Arc<crate::state::AppState>,
    target_prefix: &str,
    stream: S,
) -> s3s::S3Result<ExtractOutcome>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let source = StreamReader::new(stream.map_err(std::io::Error::other));
    let (source, local_headers) = crate::zip::local_header::observe_local_headers(source);
    let mut zip = async_zip::base::read::stream::ZipFileReader::with_tokio(source);
    let mut entries = Vec::new();
    let mut failures = Vec::new();
```

Continue the same function with this loop; call `local_headers.begin()` immediately before consuming each next local header:

```rust
loop {
    local_headers.begin();
    let next = match zip.next_with_entry().await {
        Ok(next) => next,
        Err(error) => {
            return Err(crate::error::AppError::ZipArchiveRejected(format!(
                "invalid zip archive: {error}"
            ))
            .into());
        }
    };
    let Some(mut entry_reader) = next else { break };
    let local = match local_headers.take() {
        Ok(local) => local,
        Err(error) => {
            return Err(crate::error::AppError::ZipArchiveRejected(format!(
                "invalid zip local header: {error}"
            ))
            .into());
        }
    };
    let entry = entry_reader.reader().entry().clone();

    let supported = matches!(
        (entry.compression(), local.compression_method),
        (async_zip::Compression::Stored, 0) | (async_zip::Compression::Deflate, 8)
    );
    if !supported {
        return Err(crate::error::AppError::UnsupportedZipEntry(
            "local-header compression method must match Stored(0) or Deflate(8)".to_string(),
        ).into());
    }
    if local.compression_method == 0 && local.uses_descriptor() {
        return Err(crate::error::AppError::UnsupportedZipEntry(
            "Stored entry uses general-purpose bit 3 (data descriptor)".to_string(),
        ).into());
    }
    let name = entry.filename().as_str()
        .map_err(|_| crate::error::AppError::InvalidZipEntry("entry name is not valid UTF-8".to_string()))?
        .to_string();
    let sanitized = crate::zip::sanitize::sanitize_entry(&name, target_prefix)
        .map_err(s3s::S3Error::from)?;
```

After this policy segment, dispatch `sanitized` through Step 5's directory/file state machine. When the loop reaches central-directory EOF, return `Ok(ExtractOutcome { entries, failures })`.

This uses the public compression metadata from `ZipEntry` only for the method cross-check; descriptor detection comes exclusively from observed local-header bit 3. It accepts Stored without bit 3 and Deflate with or without bit 3, and rejects Stored+bit 3 before reading or uploading the entry. async_zip 0.0.18 exposes no public descriptor accessor on `ZipEntry`.

- [ ] **Step 5: Implement the duplex transfer and drain-on-upload-failure state transition**

Use a typed transfer error so Kubo failures remain partial failures while decompression/read failures stop later entries:

```rust
enum EntryTransferError {
    Upload(s3s::S3Error),
    Read(std::io::Error),
}

async fn upload_entry_to_kubo<R>(
    state: &std::sync::Arc<crate::state::AppState>,
    reader: &mut R,
) -> Result<crate::s3::ops::object::StoredObject, EntryTransferError>
where
    R: futures_io::AsyncRead + Unpin + Send,
{
    let (duplex_reader, mut duplex_writer) = tokio::io::duplex(64 * 1024);
    let upload = crate::s3::ops::object::add_plain_object_stream(
        state,
        ReaderStream::new(duplex_reader),
    );
    let copy = async {
        let mut tokio_reader = reader.compat();
        tokio::io::copy(&mut tokio_reader, &mut duplex_writer).await?;
        duplex_writer.shutdown().await
    };
    let (upload_result, copy_result) = tokio::join!(upload, copy);
    match (upload_result, copy_result) {
        (Ok(stored), Ok(())) => Ok(stored),
        (Ok(_stored), Err(copy)) => Err(EntryTransferError::Read(copy)),
        (Err(upload), Err(copy)) if copy.kind() == std::io::ErrorKind::BrokenPipe => {
            Err(EntryTransferError::Upload(upload))
        }
        (Err(_upload), Err(copy)) => Err(EntryTransferError::Read(copy)),
        (Err(upload), Ok(())) => Err(EntryTransferError::Upload(upload)),
    }
}
```

On `EntryTransferError::Upload`, record `EntryUploadFailed`, then stream the remainder of `entry_reader.reader_mut().compat()` to `tokio::io::sink()` and call `entry_reader.done().await`. If both succeed, assign the returned Ready reader to `zip` and continue with the next local header. If drain or `done()` fails, append `EntryReadFailed` and return the current `ExtractOutcome`, matching the spec's “partial failure, stop later entries” rule. On `EntryTransferError::Read`, append `EntryReadFailed` and return the current outcome. On success, call `done()` before adding the entry to `entries`; if `done()` fails, retain any successfully pinned CID and return an `EntryReadFailed` outcome. `add_plain_object_stream` also retains a CID when `pin_add` reports failure because the RPC result and CID sharing cannot be proven safe for removal.

Directories call `skip().await`; a directory read error is `EntryReadFailed` and stops later entries. Every path/name/compression policy error is a global reject: return 400 without publishing DB rows and retain every archive/entry CID that may have been pinned. Neither archive nor entry bytes are collected.

- [ ] **Step 6: Run extractor and streaming-policy verification**

```powershell
cargo test zip::local_header::tests --lib
cargo test zip::extract::tests --lib
cargo test zip --lib
```

Expected: Stored without descriptor passes; Deflate+descriptor passes; Stored+descriptor returns 400 within one second with zero entry `/add` calls; Kubo failure on one safe entry is reported while the following entry succeeds; traversal remains a global reject; every successfully pinned entry CID is absent from `/pin/rm`; all commands exit 0.

- [ ] **Step 7: Manual progress marker**

Record Task 4 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 5: PutObject custom route and archive-first publish flow

**Files:**
- Modify: `src/main.rs`
- Modify: `src/s3/mod.rs`
- Modify: `src/kubo/cat.rs`
- Create: `src/s3/query.rs`
- Create: `src/s3/route/mod.rs`
- Create: `src/s3/route/decompress_zip.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: `percent_encoding::percent_decode_str`, `add_plain_object_stream`, `publish_plain_object`, `extract_zip_stream`, `normalize_target_prefix`, `decompress_result_xml`, `DecompressZipResult`, `ExtractedEntry`, and the response-owned `stream_cat` byte stream.
- Produces: `decoded_query_pairs(uri: &http::Uri) -> S3Result<Vec<(String, String)>>`, `query_key_is_present(uri: &http::Uri, name: &str) -> bool`, `reject_archive_key_collision(archive_key: &str, entries: &[ExtractedEntry]) -> S3Result<()>`, `DecompressZipRoute::new(state: Arc<AppState>)`, PutObject `?decompress-zip` support, and route registration in app/test harness. Task 7 consumes the collision helper before Multipart finalize; Tasks 6 and 7 consume the shared query APIs; no consumer percent-decodes a returned name or value again.

- [ ] **Step 1: Write failing shared-query and route unit tests**

Create `src/s3/query.rs` with tests first:

```rust
use http::Uri;
use s3s::S3Result;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_pairs_decode_slashes_apply_form_plus_semantics_and_keep_empty_values() {
        let uri = "/bucket/key?decompress-zip=prefix%2Fnested%2F&empty=&uploads&space=a+b&literal=a%2Bb"
            .parse::<Uri>()
            .unwrap();
        let pairs = decoded_query_pairs(&uri).unwrap();
        assert!(pairs.contains(&("decompress-zip".to_string(), "prefix/nested/".to_string())));
        assert!(pairs.contains(&("empty".to_string(), String::new())));
        assert!(pairs.contains(&("uploads".to_string(), String::new())));
        assert!(pairs.contains(&("space".to_string(), "a b".to_string())));
        assert!(pairs.contains(&("literal".to_string(), "a+b".to_string())));
    }

    #[test]
    fn invalid_query_utf8_is_invalid_argument_but_route_key_is_still_detectable() {
        let uri = "/bucket/key?decompress-zip=%FF".parse::<Uri>().unwrap();
        assert!(query_key_is_present(&uri, "decompress-zip"));
        assert_eq!(decoded_query_pairs(&uri).unwrap_err().code().as_str(), "InvalidArgument");
    }
}
```

Create `src/s3/route/mod.rs`:

```rust
pub mod decompress_zip;
```

Update `src/s3/mod.rs`:

```rust
pub mod handler;
pub mod ops;
pub mod query;
pub mod route;
```

Create `src/s3/route/decompress_zip.rs` with route skeleton and tests first:

```rust
use std::sync::Arc;

use http::{HeaderMap, Method, Uri};
use s3s::{Body, S3Request, S3Response, S3Result};
use s3s::route::S3Route;

use crate::state::AppState;

pub struct DecompressZipRoute {
    state: Arc<AppState>,
}

impl DecompressZipRoute {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

```

Add tests in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn dummy_route() -> DecompressZipRoute {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let state = Arc::new(crate::state::AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_string()),
            store: crate::store::Store::new(db),
            credentials: std::collections::HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        });
        DecompressZipRoute::new(state)
    }

    #[tokio::test]
    async fn route_matches_decompress_put_but_not_complete_before_task_7() {
        let route = dummy_route().await;
        let mut ext = http::Extensions::new();
        assert!(route.is_match(
            &Method::PUT,
            &"/bucket/archive.zip?decompress-zip=prefix/".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut ext,
        ));
        assert!(!route.is_match(
            &Method::POST,
            &"/bucket/archive.zip?uploadId=abc".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut ext,
        ));
        assert!(!route.is_match(
            &Method::PUT,
            &"/bucket/archive.zip".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut ext,
        ));
        assert!(!route.is_match(
            &Method::POST,
            &"/bucket/archive.zip?uploads&decompress-zip=prefix/".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut ext,
        ));
    }

    #[test]
    fn parse_path_and_query_decodes_bucket_key_prefix_and_result_flag_once() {
        let parsed = parse_decompress_put_uri(&"/bucket/folder%20a/archive.zip?decompress-zip=prefix%2Fnested%2F&decompress-zip-result=false".parse::<Uri>().unwrap()).unwrap();
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key, "folder a/archive.zip");
        assert_eq!(parsed.target_prefix, "prefix/nested/");
        assert!(!parsed.return_result_xml);
    }

    #[test]
    fn parse_decompress_put_accepts_empty_prefix_and_rejects_invalid_utf8() {
        let empty = parse_decompress_put_uri(
            &"/bucket/archive.zip?decompress-zip=".parse::<Uri>().unwrap(),
        ).unwrap();
        assert_eq!(empty.target_prefix, "");

        let invalid = parse_decompress_put_uri(
            &"/bucket/archive.zip?decompress-zip=%FF".parse::<Uri>().unwrap(),
        ).unwrap_err();
        assert_eq!(invalid.code().as_str(), "InvalidArgument");
    }

    #[test]
    fn archive_key_collision_returns_fixed_invalid_parameter_value() {
        let entries = vec![crate::zip::response::ExtractedEntry {
            key: "archive.zip".to_string(),
            cid: "QmEntry".to_string(),
            size: 5,
        }];
        let error = reject_archive_key_collision("archive.zip", &entries).unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(
            error.message(),
            Some("zip entry collides with archive key: archive.zip")
        );
    }

    #[test]
    fn archive_key_collision_allows_non_matching_successful_entries() {
        let entries = vec![crate::zip::response::ExtractedEntry {
            key: "prefix/file.txt".to_string(),
            cid: "QmEntry".to_string(),
            size: 5,
        }];
        reject_archive_key_collision("archive.zip", &entries).unwrap();
    }
}
```

- [ ] **Step 2: Run route tests to verify they fail**

Run:

```powershell
cargo test s3::query::tests --lib
cargo test s3::route::decompress_zip::tests --lib
```

Expected: tests fail because the shared decoder, route parse helpers, and archive-key collision helper are not implemented.

- [ ] **Step 3: Implement PutObject route parsing and response flow**

Before the first route call that passes a Kubo cat stream to `extract_zip_stream`, modify `src/kubo/cat.rs` so `stream_cat` has this exact Rust 2024 return type:

```rust
pub async fn stream_cat(
    kubo: &KuboClient,
    cid: &str,
    range: Option<(u64, u64)>,
) -> Result<
    impl Stream<Item = Result<Bytes, std::io::Error>> + use<>,
    Box<dyn std::error::Error + Send + Sync>,
>
```

Keep the existing URL construction, status handling, and `resp.bytes_stream().map(...)` behavior unchanged. The explicit `use<>` prevents Rust 2024's implicit opaque-type capture of the `&KuboClient` and `&str` lifetimes. The returned stream owns the reqwest response bytes, so this makes that fact visible to the compiler and lets it satisfy `extract_zip_stream`'s `'static` stream bound without extending a borrow of the client or CID.

Implement the shared decoder in `src/s3/query.rs`. It first maps raw `+` to a space, then applies one percent decode, matching s3s's form-style canonical-query treatment; `%2B` therefore remains a literal plus. Both names and values are decoded exactly once; a component without `=` has an empty value; invalid UTF-8 in either component returns `InvalidArgument`:

```rust
fn decode_component(raw: &str) -> S3Result<String> {
    percent_encoding::percent_decode_str(&raw.replace('+', " "))
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| s3s::s3_error!(InvalidArgument, "query component is not valid UTF-8"))
}

pub fn decoded_query_pairs(uri: &Uri) -> S3Result<Vec<(String, String)>> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            Ok((decode_component(name)?, decode_component(value)?))
        })
        .collect()
}

pub fn query_key_is_present(uri: &Uri, expected: &str) -> bool {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .any(|part| {
            let raw_name = part.split_once('=').map_or(part, |(name, _)| name);
            percent_encoding::percent_decode_str(raw_name)
                .decode_utf8()
                .is_ok_and(|name| name == expected)
        })
}
```

`query_key_is_present` is only a route-selection hint: it decodes the parameter name but never exposes or validates a value. This lets `?decompress-zip=%FF` reach the custom route, where `decoded_query_pairs` returns the required `InvalidArgument`. All behavior logic uses the returned owned pairs and never decodes them again. Duplicate parameters are processed in wire order and the final occurrence wins.

Use that module in the route parser:

```rust
struct ParsedDecompressPut {
    bucket: String,
    key: String,
    target_prefix: String,
    return_result_xml: bool,
}

fn parse_path_bucket_key(uri: &Uri) -> S3Result<(String, String)> {
    let path = uri.path().trim_start_matches('/');
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "path-style bucket and key are required"))?;
    let bucket = percent_encoding::percent_decode_str(bucket)
        .decode_utf8()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "bucket is not valid UTF-8"))?
        .to_string();
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "key is not valid UTF-8"))?
        .to_string();
    if bucket.is_empty() || key.is_empty() {
        return Err(s3s::s3_error!(InvalidArgument, "bucket and key are required"));
    }
    Ok((bucket, key))
}

fn parse_decompress_put_uri(uri: &Uri) -> S3Result<ParsedDecompressPut> {
    let (bucket, key) = parse_path_bucket_key(uri)?;
    let mut target = None;
    let mut return_result_xml = true;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        if name == "decompress-zip" {
            target = Some(value);
        } else if name == "decompress-zip-result" {
            return_result_xml = value != "false";
        }
    }
    let target_prefix = crate::zip::sanitize::normalize_target_prefix(
        target.as_deref().unwrap_or(""),
    )?;
    Ok(ParsedDecompressPut { bucket, key, target_prefix, return_result_xml })
}

fn has_sse_header(headers: &HeaderMap) -> bool {
    headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-MD5")
}
```

Add the shared route-layer global-reject helper. It scans only successful entries because `ExtractOutcome.failures` has no staged CID and is intentionally not an argument:

```rust
fn reject_archive_key_collision(
    archive_key: &str,
    entries: &[crate::zip::response::ExtractedEntry],
) -> S3Result<()> {
    if entries.iter().any(|entry| entry.key == archive_key) {
        let mut error = s3s::S3Error::with_message(
            s3s::S3ErrorCode::Custom("InvalidParameterValue".into()),
            format!("zip entry collides with archive key: {archive_key}"),
        );
        error.set_status_code(http::StatusCode::BAD_REQUEST);
        return Err(error);
    }
    Ok(())
}
```

Implement `is_match`:

```rust
fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut http::Extensions) -> bool {
    *method == Method::PUT && crate::s3::query::query_key_is_present(uri, "decompress-zip")
}
```

Implement `call` PutObject branch:

```rust
async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    self.check_access(&mut req).await?;
    if req.method != Method::PUT {
        return Err(s3s::s3_error!(MethodNotAllowed, "decompress route only accepts PUT in Task 5"));
    }
    self.call_put(req).await
}

async fn call_put(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    let parsed = parse_decompress_put_uri(&req.uri)?;
    if has_sse_header(&req.headers) {
        return Err(s3s::s3_error!(InvalidArgument, "decompress-zip does not support server-side encryption in MVP"));
    }
    if !crate::store::bucket::exists(self.state.store.db(), &parsed.bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", parsed.bucket));
    }

    let content_type = req.headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let metadata = crate::s3::ops::object::extract_custom_metadata(&req.headers);

    let archive = crate::s3::ops::object::add_plain_object_stream(&self.state, req.input).await?;
    let archive_stream = match crate::kubo::cat::stream_cat(&self.state.kubo, &archive.cid, None).await {
        Ok(stream) => stream,
        Err(err) => {
            return Err(s3s::s3_error!(InternalError, "cat archive: {err}"));
        }
    };

    let outcome = match crate::zip::extract::extract_zip_stream(&self.state, &parsed.target_prefix, archive_stream).await {
        Ok(outcome) => outcome,
        Err(err) => {
            return Err(err);
        }
    };

    reject_archive_key_collision(&parsed.key, &outcome.entries)?;

    if let Err(err) = crate::s3::ops::object::publish_plain_object(
        &self.state,
        &parsed.bucket,
        &parsed.key,
        content_type.as_deref(),
        metadata,
        &archive,
        false,
    )
    .await
    {
        return Err(err);
    }

    let mut published = Vec::new();
    let mut failures = outcome.failures;
    for entry in outcome.entries {
        let stored = crate::s3::ops::object::StoredObject { cid: entry.cid.clone(), size: entry.size };
        match crate::s3::ops::object::publish_plain_object(&self.state, &parsed.bucket, &entry.key, None, None, &stored, false).await {
            Ok(()) => published.push(entry),
            Err(err) => failures.push(crate::zip::response::ExtractFailure {
                entry_name: entry.key,
                code: "EntryPublishFailed".to_string(),
                message: err.to_string(),
            }),
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert(http::header::ETAG, http::HeaderValue::from_str(&format!("\"{}\"", archive.cid)).unwrap());
    if parsed.return_result_xml {
        let result = crate::zip::response::DecompressZipResult {
            archive_key: parsed.key,
            archive_cid: archive.cid,
            archive_size: archive.size,
            entries: published,
            failures,
        };
        let xml = crate::zip::response::decompress_result_xml(&result);
        headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/xml"));
        Ok(S3Response::with_headers(Body::from(xml), headers))
    } else {
        Ok(S3Response::with_headers(Body::empty(), headers))
    }
}
```

The collision check is after extraction so it sees every successful staged `ExtractedEntry`, but before archive DB publish and before the entry publish loop. A collision therefore returns the fixed 400 with no archive or entry latest; archive and successful entry pins remain. Failed entries without staged CIDs exist only in `outcome.failures` and do not participate. Every error branch after `add_plain_object_stream` retains the archive pin. Extraction global rejects and archive/entry DB publish failures also retain every successfully pinned entry CID; the route has no failure-cleanup helper. This is intentional because recursive pins are CID-wide and may already be shared. The 400/500 response and DB invisibility contract is independent from pin reclamation.

Task 5 does not match CompleteMultipartUpload at all. Task 7 atomically adds the POST match and the fully implemented `call_complete`; therefore every intermediate revision preserves standard multipart routing and contains no unimplemented Complete branch.

Register the route in `src/main.rs` and `tests/integration.rs` before `builder.build()`:

```rust
builder.set_route(ipfs_s3_gateway::s3::route::decompress_zip::DecompressZipRoute::new(state.clone()));
```

In `src/main.rs`, use `crate::s3::route::decompress_zip::DecompressZipRoute::new(state.clone())` instead of the crate-qualified path.

- [ ] **Step 4: Add failing route-level PutObject behavior tests**

Add direct route tests in `src/s3/route/decompress_zip.rs` using `S3Request<Body>` with `credentials: Some(...)` to avoid raw SigV4 construction. The success test should upload any body bytes because the route stores archive to Kubo and then reads the legal zip from mocked `/api/v0/cat`:

```rust
#[tokio::test]
async fn put_decompress_zip_returns_xml_and_publishes_archive_and_entries() {
    let (route, state) = route_with_mock_kubo_returning_legal_zip().await;
    crate::store::bucket::create(state.store.db(), "bucket", None).await.unwrap();

    let req = signed_route_request(
        Method::PUT,
        "/bucket/archive.zip?decompress-zip=prefix/",
        Body::from("archive bytes".to_string()),
    );

    let response = route.call(req).await.unwrap();
    assert_eq!(response.status.unwrap_or(http::StatusCode::OK), http::StatusCode::OK);

    let archive = crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip").await.unwrap();
    assert_eq!(archive.cid, "QmArchive");
    let entry = crate::store::object::get_latest(state.store.db(), "bucket", "prefix/foo/bar.txt").await.unwrap();
    assert_eq!(entry.cid, "QmEntry");
}

#[tokio::test]
async fn put_decompress_zip_rejects_sse_headers() {
    let (route, state) = route_with_mock_kubo_returning_legal_zip().await;
    crate::store::bucket::create(state.store.db(), "bucket", None).await.unwrap();

    let mut req = signed_route_request(
        Method::PUT,
        "/bucket/archive.zip?decompress-zip=prefix/",
        Body::from("archive bytes".to_string()),
    );
    req.headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("AES256"));

    let err = route.call(req).await.unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidArgument");
}
```

Add `put_decompress_zip_global_reject_keeps_archive_and_entry_pins`: script an archive `QmArchive` whose ZIP stages `QmSharedEntry` before a traversal entry, expect `InvalidParameterValue`, assert no archive/entry latest row, and forbid both CIDs under `/api/v0/pin/rm`. Add `put_decompress_zip_entry_publish_failure_keeps_shared_pin_and_other_objects_readable`: install a narrow SQLite trigger that rejects only the first target entry insert, let the second entry publish, expect 200 with one `EntryPublishFailed`, verify archive and second entry with `get_latest`, stream both through the normal GetObject path, and forbid `QmArchive`, the failed entry CID, and the successful entry CID under `/pin/rm`.

The helper `signed_route_request` must fill the public `S3Request` fields and set credentials:

```rust
fn signed_route_request(method: Method, uri: &str, body: Body) -> S3Request<Body> {
    S3Request {
        input: body,
        method,
        uri: uri.parse().unwrap(),
        headers: HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: Some(s3s::auth::Credentials {
            access_key: "test".to_string(),
            secret_key: s3s::auth::SecretKey::from("test"),
        }),
        region: Some("us-east-1".parse().unwrap()),
        service: Some("s3".to_string()),
        trailing_headers: None,
    }
}
```

Use the legal zip fixture from Task 4 in the mock Kubo helper. Configure `/api/v0/add` for archive and entry to return a valid newline-delimited JSON body and `/api/v0/cat` to return the legal zip bytes.

- [ ] **Step 5: Run PutObject route tests to verify they pass**

Run:

```powershell
cargo test s3::query::tests --lib
cargo test s3::route::decompress_zip::tests::archive_key_collision_ --lib
cargo test s3::route::decompress_zip::tests --lib
cargo test kubo::cat::tests::test_stream_cat_returns_bytes --lib
cargo check --bin ipfs-s3-gateway
cargo test test_create_and_put_and_get_plain_object --test integration
```

Expected: the cat unit test preserves cat behavior, and the binary check proves the Rust 2024 `use<>` stream reaches the `'static` extractor call site. Shared-query and route tests pass; collision/non-collision helper tests prove the fixed error contract; global reject/publish failure tests observe DB invisibility or partial-success visibility exactly as specified with no archive/entry CID removals; and the existing standard PutObject integration test still passes, proving non-decompress PutObject remains on the standard path.

- [ ] **Step 6: Manual progress marker**

Record Task 5 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 6: Atomic Multipart part records, conservative pins, and per-Complete identity

**Files:**
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/store/object.rs`

**Interfaces:**
- Consumes: Task 5 `decoded_query_pairs`, `normalize_target_prefix`, existing Multipart validation/concat/encryption logic, SeaORM `ConnectionTrait + TransactionTrait`, `sea_orm::sea_query::OnConflict`, `Uuid::new_v4`, and exact object/upload entity reads. No Task 6 product path consumes Kubo `pin_rm`.
- Produces: persisted `decompress_zip_target`/`decompress_zip_result`; `store::multipart::upsert_part<C: ConnectionTrait>(...) -> AppResult<()>`; `store::object::LatestObjectRow`; transaction-local `store::object::write_latest_in_transaction`; `CommitCompletedUploadError::{RolledBack { completion_attempt_id, source }, OutcomeUnknown { completion_attempt_id, source }}`; `ReconciledCommitOutcome::{Committed, NotCommitted, Unknown}`; `store::multipart::commit_completed_upload<C: ConnectionTrait + TransactionTrait>(...) -> Result<(), CommitCompletedUploadError>`; `store::multipart::reconcile_completion_attempt<C: ConnectionTrait>(...) -> ReconciledCommitOutcome`; narrow testable `CompletedUploadFinalizerStore`; `CompletedMultipartArchive { encryption_object_id, completion_attempt_id, ... }`; `complete_multipart_upload_inner(...) -> S3Result<CompletedMultipartArchive>`; `finalize_completed_multipart_archive(...) -> S3Result<()>`; and standard UploadPart/Complete/Abort responses with DB semantics unchanged but no part-CID removal. `upsert` and `commit_completed_upload` consume the same transaction-local object writer; `CompletedMultipartArchive` exposes no part-cleanup list or action.

- [ ] **Step 1: Write failing atomic part-record and conservative-pin tests**

Add two store tests to `src/store/multipart.rs`:

- `upsert_part_replaces_all_mutable_fields_atomically`: seed `(upload-1, 1, QmOld, 3, QmOld)`, capture `uploaded_at`, call `upsert_part(..., "QmNew", 7, "QmNew")`, and assert exactly one row remains with new `cid/size/etag` and a nondecreasing `uploaded_at`.
- `upsert_part_failure_preserves_previous_row`: seed `QmOld`, install `BEFORE UPDATE ON multipart_parts` trigger `RAISE(FAIL, 'forced part upsert failure')`, call `upsert_part` with `QmNew`, expect `AppError::Database`, and assert the old row remains byte-for-byte unchanged. This proves failure cannot leave a delete/insert gap.

Add operation tests to `src/s3/ops/multipart.rs` with a permissive `/api/v0/pin/rm` recorder:

- `upload_part_replacement_keeps_old_and_new_part_pins`: upload part 1 as `QmOldPart`, re-upload part 1 as `QmNewPart`, assert the DB row is `QmNewPart`, both `/pin/add` calls occurred, and neither CID appears under `/pin/rm`.
- `upload_part_initial_db_failure_keeps_new_pin`: with no existing part row, install a `BEFORE INSERT ON multipart_parts` trigger, script add/pin `QmNewPart`, expect `InternalError`, assert no part row exists, require `/pin/add(QmNewPart)`, and assert zero `/pin/rm?arg=QmNewPart`.
- `upload_part_db_failure_keeps_new_pin_and_old_row`: seed old part `QmOldPart`, install a `BEFORE UPDATE ON multipart_parts` trigger, script add/pin `QmNewPart`, expect `InternalError`, assert DB still names `QmOldPart`, and assert zero removals for both CIDs.
- `shared_part_cid_survives_upload_part_replacement`, `shared_part_cid_survives_abort`, and `shared_part_cid_survives_complete`: first publish an ordinary plaintext object whose CID is `QmSharedPart`; use separate seeded uploads to exercise each operation with a part row referencing that CID; after each operation read the ordinary object through the normal GetObject operation and assert `/pin/rm?arg=QmSharedPart` occurred zero times.
- `complete_single_part_equal_root_keeps_readable_cid`: script UploadPart and root add to return the same `QmPart`, complete through the standard wrapper, assert latest object `cid=QmPart`, upload/part DB rows are gone, a normal GetObject returns the scripted bytes, and `/pin/rm?arg=QmPart` occurred zero times.

The failure expectation is structural: these tests fail against the current `get_part → pin_rm(old) → delete_part → insert_part`, DB-failure cleanup, Complete part loop, and Abort part loop even if all HTTP/DB happy-path assertions pass.

- [ ] **Step 2: Write failing Create consumer and dual-identity inner tests**

Add these tests to `src/s3/ops/multipart.rs`:

```rust
#[tokio::test]
async fn create_multipart_upload_decodes_shared_query_once() {
    let state = test_state_with_bucket("test-bucket").await;
    let mut req = multipart_create_request("test-bucket", "archive.zip");
    req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=prefix%2Fnested%2F&decompress-zip-result=false"
        .parse()
        .unwrap();
    create_multipart_upload(&state, req).await.unwrap();
    let upload = crate::store::entities::multipart_upload::Entity::find()
        .one(state.store.db()).await.unwrap().unwrap();
    assert_eq!(upload.decompress_zip_target.as_deref(), Some("prefix/nested/"));
    assert!(!upload.decompress_zip_result);
}

#[tokio::test]
async fn create_multipart_upload_rejects_invalid_query_utf8() {
    let state = test_state_with_bucket("test-bucket").await;
    let mut req = multipart_create_request("test-bucket", "archive.zip");
    req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=%FF".parse().unwrap();
    assert_eq!(create_multipart_upload(&state, req).await.unwrap_err().code().as_str(), "InvalidArgument");
}

#[tokio::test]
async fn create_multipart_upload_rejects_decompress_sse() {
    let state = test_state_with_bucket("test-bucket").await;
    let mut req = multipart_create_request("test-bucket", "archive.zip");
    req.uri = "/test-bucket/archive.zip?uploads=&decompress-zip=prefix%2F".parse().unwrap();
    req.headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("AES256"));
    assert_eq!(create_multipart_upload(&state, req).await.unwrap_err().code().as_str(), "InvalidArgument");
}
```

Add `complete_inner_returns_archive_with_distinct_encryption_and_attempt_identities`. Seed `upload.object_id="encryption-object-1"`, invoke `complete_multipart_upload_inner` twice without finalizing, and script the same root for both. Assert every returned field used by finalize, plus:

```rust
assert_eq!(first.encryption_object_id, "encryption-object-1");
assert_eq!(second.encryption_object_id, "encryption-object-1");
uuid::Uuid::parse_str(&first.completion_attempt_id).unwrap();
uuid::Uuid::parse_str(&second.completion_attempt_id).unwrap();
assert_ne!(first.completion_attempt_id, second.completion_attempt_id);
assert_eq!(first.root_cid, "QmRoot");
assert_eq!(second.root_cid, "QmRoot");
assert!(crate::store::entities::object::Entity::find_by_id(&first.completion_attempt_id)
    .one(db).await.unwrap().is_none());
assert!(crate::store::entities::object::Entity::find_by_id(&second.completion_attempt_id)
    .one(db).await.unwrap().is_none());
assert!(crate::store::multipart::get_upload(db, "upload-1").await.is_ok());
assert!(crate::store::multipart::get_part(db, "upload-1", 1).await.is_ok());
```

For an SSE fixture, additionally assert root re-encryption still receives `encryption-object-1` as the nonce/key context and never receives either completion attempt ID. The inner result has no `part_cids` field because no downstream layer may remove those pins.

Add this parser regression in `src/s3/ops/multipart.rs`. It proves duplicate targets use wire-order last-value-wins semantics before validation: an earlier unsafe target is discarded when the final target is valid, but a final unsafe target fails normalization.

```rust
#[test]
fn decompress_upload_options_normalize_only_the_final_target_value() {
    let valid_final =
        "/test-bucket/archive.zip?uploads&decompress-zip=../bad&decompress-zip=prefix%2F"
            .parse::<http::Uri>()
            .unwrap();
    assert_eq!(
        parse_decompress_upload_options(&valid_final).unwrap(),
        (Some("prefix/".to_owned()), true)
    );

    let invalid_final =
        "/test-bucket/archive.zip?uploads&decompress-zip=prefix%2F&decompress-zip=../bad"
            .parse::<http::Uri>()
            .unwrap();
    assert_eq!(
        parse_decompress_upload_options(&invalid_final)
            .unwrap_err()
            .code()
            .as_str(),
        "InvalidArgument"
    );
}
```

- [ ] **Step 3: Write rollback, exact reconciliation, and outcome-unknown race tests**

Use a real SQLite connection with migrations and wiremock Kubo. Seed an existing latest object `old-object/QmOld`, upload `upload-1`, and part `QmPart`. Call Complete inner once to obtain `QmRootFirst`, then install:

```sql
CREATE TRIGGER fail_multipart_upload_delete
BEFORE DELETE ON multipart_uploads
BEGIN
  SELECT RAISE(FAIL, 'forced multipart delete failure');
END;
```

Name the test `finalize_delete_trigger_reports_rolled_back_and_keeps_all_pins`. The store-level assertion destructures `CommitCompletedUploadError::RolledBack { completion_attempt_id, .. }` and requires it equals `first.completion_attempt_id`; the operation-level finalize returns `InternalError`. The old object remains latest; no row exists at `first.completion_attempt_id`; upload/part rows remain; `QmRootFirst` and `QmPart` both have zero `/pin/rm`. Drop the trigger, run Complete inner again as `second` with `QmRootRetry`, finalize successfully, and assert latest `id=second.completion_attempt_id`, the old row is non-latest, upload/parts are absent, and `QmRootFirst`, `QmRootRetry`, and `QmPart` all have zero removals. Both failed and successful attempts retain pins.

Add `commit_completed_upload_requires_exactly_one_upload_row` in `src/store/multipart.rs`: pass a `LatestObjectRow` whose ID is `attempt-missing-upload` but a missing upload ID, expect `CommitCompletedUploadError::RolledBack { completion_attempt_id: "attempt-missing-upload", .. }`, then assert the prior latest row is still latest and the attempt row does not exist.

Add pure classification tests around `classify_completion_attempt_state(expected_attempt, object, upload)`:

- `unknown_commit_exact_attempt_and_missing_upload_is_committed`: exact attempt row and absent upload yields `Committed`.
- `unknown_commit_missing_attempt_is_not_committed_even_when_upload_is_absent`: both `(object=None, upload=Some)` and `(object=None, upload=None)` yield `NotCommitted`; the second case is the different-attempt winner boundary.
- `unknown_commit_present_but_mismatched_attempt_is_unknown`: wrong root/fields and exact attempt plus present upload yield `Unknown(AppError::Internal(...))`.

Keep `CompletedUploadFinalizerStore` deliberately narrow. A hand-written fake returns configured commit/reconciliation results; every configured `RolledBack`/`OutcomeUnknown` carries the current archive's `completion_attempt_id`. With the Kubo request log add:

- `outcome_unknown_committed_reconciliation_succeeds_without_pin_removal`: `OutcomeUnknown → Committed` returns `Ok` and neither root nor part CID is removed.
- `outcome_unknown_not_committed_returns_error_without_pin_removal`: `OutcomeUnknown → NotCommitted` returns `InternalError` with no removals.
- `outcome_unknown_query_failure_returns_error_without_pin_removal`: `OutcomeUnknown → Unknown(forced query failure)` returns `InternalError` with no removals.
- `finalizer_rejects_error_for_different_completion_attempt`: fake commit returns `OutcomeUnknown { completion_attempt_id: "other-attempt", ... }`; finalize returns `InternalError`, never calls reconcile, and performs no CID removal.

Add the exact race `outcome_unknown_attempt_a_does_not_adopt_attempt_b_commit`. Build archives A/B for one upload and root with different IDs `attempt-a` and `attempt-b`. A uses a fake whose `commit` sends a one-shot signal and returns `OutcomeUnknown { completion_attempt_id: "attempt-a", source: forced_error }` without writing; its `reconcile` sends a second signal, waits on `release_a_reconcile`, then delegates to the real `reconcile_completion_attempt` with A's row. After the coordinator observes A blocked in reconciliation, finalize B through the real store, assert B succeeds, and release A. Assert:

```rust
assert!(a_result.is_err());
assert!(b_result.is_ok());
assert!(object::Entity::find_by_id("attempt-a").one(db).await.unwrap().is_none());
let winner = object::Entity::find_by_id("attempt-b").one(db).await.unwrap().unwrap();
assert!(winner.is_latest);
assert_eq!(winner.cid, "QmRoot");
assert!(crate::store::multipart::get_upload(db, "upload-1").await.is_err());
assert_pin_rm_calls(&kubo, &[], &["QmRoot", "QmPart"]).await;
```

This seam proves A classifies its missing attempt row as `NotCommitted`; it does not query by bucket/key or the shared `encryption_object_id`, so B cannot be mistaken for A.

- [ ] **Step 4: Write the true same-upload concurrent Complete test**

Add `concurrent_complete_same_upload_uses_distinct_attempt_ids`. Use one file-backed temporary SQLite DB with at least two pooled connections and one seeded upload/part. Spawn two full Complete tasks for the same `upload_id` and request. Each task independently calls `complete_multipart_upload_inner`, so both read the same upload/parts, concatenate, receive `QmRoot`, and complete `/pin/add?arg=QmRoot`. Return each generated `completion_attempt_id` to its task closure, then wait at the same `Arc<tokio::sync::Barrier>` immediately before calling finalize. The coordinator is the third barrier participant and releases both only after both inners have completed; no serial substitute is allowed.

Collect `(attempt_id, finalize_result)` from each task and require the attempt IDs differ, exactly one result is `Ok`, and exactly one is `Err`. Derive `winner_attempt_id` and `loser_attempt_id` from those results, then assert:

```rust
let latest = crate::store::object::get_latest(db, "test-bucket", "archive.zip").await.unwrap();
assert_eq!(latest.id, winner_attempt_id);
assert_eq!(latest.cid, "QmRoot");
assert!(object::Entity::find_by_id(loser_attempt_id).one(db).await.unwrap().is_none());
assert!(crate::store::multipart::get_upload(db, "upload-1").await.is_err());
assert!(crate::store::multipart::list_parts(db, "upload-1").await.unwrap().is_empty());
assert_pin_add_count(&kubo, "QmRoot", 2).await;
assert_pin_rm_calls(&kubo, &[], &["QmRoot", "QmPart"]).await;
```

Also assert both inner results preserved the same `encryption_object_id="encryption-object-1"`; only their attempt IDs differ.

- [ ] **Step 5: Run tests and verify the current delete/unpin/shared-identity behavior fails**

```powershell
cargo test store::multipart::tests::upsert_part_ --lib
cargo test s3::ops::multipart::tests::upload_part_ --lib
cargo test s3::ops::multipart::tests::shared_part_cid_ --lib
cargo test s3::ops::multipart::tests::complete_single_part_equal_root_keeps_readable_cid --lib
cargo test s3::ops::multipart::tests::decompress_upload_options_normalize_only_the_final_target_value --lib
cargo test s3::ops::multipart::tests::complete_inner_returns_archive_with_distinct_encryption_and_attempt_identities --lib
cargo test store::multipart::tests::unknown_commit_ --lib
cargo test s3::ops::multipart::tests::finalize_delete_trigger_reports_rolled_back_and_keeps_all_pins --lib
cargo test s3::ops::multipart::tests::outcome_unknown_ --lib
cargo test s3::ops::multipart::tests::finalizer_rejects_error_for_different_completion_attempt --lib
cargo test s3::ops::multipart::tests::concurrent_complete_same_upload_uses_distinct_attempt_ids --lib
```

Expected: tests fail because `upsert_part`, dual identities, attempt-exact reconciliation, and the finalizer seam do not exist, while current UploadPart/Complete/Abort code issues forbidden part removals.

- [ ] **Step 6: Persist Create options through the shared decoded pairs**

Implement only this Multipart-specific interpretation; do not split or percent-decode the URI locally:

```rust
fn parse_decompress_upload_options(uri: &http::Uri) -> S3Result<(Option<String>, bool)> {
    let mut raw_target = None;
    let mut result = true;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        match name.as_str() {
            "decompress-zip" => {
                raw_target = Some(value);
            }
            "decompress-zip-result" => result = value != "false",
            _ => {}
        }
    }
    let target = raw_target
        .as_deref()
        .map(crate::zip::sanitize::normalize_target_prefix)
        .transpose()?;
    Ok((target, result))
}
```

Before `determine_encryption_mode`, parse options and return `InvalidArgument` when a decompression target coexists with any SSE-S3 or SSE-C header. Pass `decompress_zip_target.as_deref()` and `decompress_zip_result` as the final two arguments to Task 1's `create_upload`. The loop records only the last raw decoded target, then normalizes that final value once after the loop. It must not validate or normalize an earlier duplicate target.

- [ ] **Step 7: Replace part delete/insert with one atomic upsert and remove all part removals**

In `src/store/multipart.rs`, import `sea_orm::sea_query::OnConflict`, replace `insert_part` with this API, and delete `delete_part` because no caller may create a replacement gap:

```rust
pub async fn upsert_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
    cid: &str,
    size: i64,
    etag: &str,
) -> AppResult<()> {
    let model = multipart_part::ActiveModel {
        upload_id: Set(upload_id.to_owned()),
        part_number: Set(part_number),
        cid: Set(cid.to_owned()),
        size: Set(size),
        etag: Set(etag.to_owned()),
        uploaded_at: Set(Utc::now()),
    };

    multipart_part::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([
                multipart_part::Column::UploadId,
                multipart_part::Column::PartNumber,
            ])
            .update_columns([
                multipart_part::Column::Cid,
                multipart_part::Column::Size,
                multipart_part::Column::Etag,
                multipart_part::Column::UploadedAt,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}
```

The existing composite primary key is the conflict target on SQLite and PostgreSQL; no migration is added. In `upload_part`, replace lines 166–187's old lookup/removal/delete/insert sequence with exactly:

```rust
crate::kubo::pin::pin_add(&state.kubo, &cid)
    .await
    .map_err(|e| s3s::s3_error!(InternalError, "pin: {e}"))?;

crate::store::multipart::upsert_part(
    db,
    upload_id,
    part_number,
    &cid,
    part_size,
    &cid,
)
.await?;
```

There is intentionally no old-part lookup and no error cleanup after `pin_add`. On DB failure the new pin remains and an existing row remains unchanged; on success the old row is atomically replaced while the old pin remains. Remove the Complete loop at current lines 446–449 and the Abort list/pin loop at current lines 492–502. Do not alter `src/kubo/pin.rs`; unrelated legacy callers may retain the module.

- [ ] **Step 8: Extract one transaction-local latest-object writer**

In `src/store/object.rs`, add an owned row type covering every inserted object column; `is_latest` is fixed to `true` by contract:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct LatestObjectRow {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub multipart: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub(crate) async fn write_latest_in_transaction<C: ConnectionTrait>(
    db: &C,
    row: LatestObjectRow,
) -> Result<(), sea_orm::DbErr> {
    let bucket = row.bucket.clone();
    let key = row.key.clone();
    object::Entity::update_many()
        .col_expr(object::Column::IsLatest, false.into())
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;

    object::Entity::insert(object::ActiveModel {
        id: Set(row.id),
        bucket: Set(row.bucket),
        key: Set(row.key),
        cid: Set(row.cid),
        size: Set(row.size),
        content_type: Set(row.content_type),
        etag: Set(row.etag),
        metadata: Set(row.metadata),
        encrypted: Set(row.encrypted),
        key_wrap: Set(row.key_wrap),
        multipart: Set(row.multipart),
        is_latest: Set(true),
        created_at: Set(row.created_at),
    })
    .exec(db)
    .await?;
    Ok(())
}
```

Keep `object::upsert`'s public signature unchanged. Construct one `LatestObjectRow` from its current arguments with `created_at: Utc::now()`, clone it into each existing retry transaction, and call `write_latest_in_transaction`. Retain `MAX_RETRIES = 3` and retry only lowercased DB errors containing `unique` or `constraint`; connection and other errors return immediately.

- [ ] **Step 9: Add the atomic completion transaction and attempt-exact reconciliation**

In `src/store/multipart.rs`, preserve the SeaORM transaction error boundary:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CommitCompletedUploadError {
    #[error("completion attempt {completion_attempt_id} transaction rolled back: {source}")]
    RolledBack {
        completion_attempt_id: String,
        #[source]
        source: AppError,
    },
    #[error("completion attempt {completion_attempt_id} commit outcome is unknown: {source}")]
    OutcomeUnknown {
        completion_attempt_id: String,
        #[source]
        source: AppError,
    },
}

#[derive(Debug)]
pub enum ReconciledCommitOutcome {
    Committed,
    NotCommitted,
    Unknown(AppError),
}

pub async fn commit_completed_upload<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    upload_id: &str,
    attempt: crate::store::object::LatestObjectRow,
) -> Result<(), CommitCompletedUploadError> {
    let completion_attempt_id = attempt.id.clone();
    const MAX_RETRIES: usize = 3;
    for retry in 0..=MAX_RETRIES {
        let upload_id = upload_id.to_owned();
        let attempt = attempt.clone();
        let result = db.transaction(|txn| {
            Box::pin(async move {
                crate::store::object::write_latest_in_transaction(txn, attempt).await?;
                let deleted = multipart_upload::Entity::delete_by_id(upload_id.clone())
                    .exec(txn)
                    .await?;
                if deleted.rows_affected != 1 {
                    return Err(sea_orm::DbErr::RecordNotFound(format!(
                        "multipart upload not found: {upload_id}"
                    )));
                }
                Ok::<_, sea_orm::DbErr>(())
            })
        }).await;

        match result {
            Ok(()) => return Ok(()),
            Err(sea_orm::TransactionError::Transaction(db_err)) => {
                let message = db_err.to_string().to_lowercase();
                if (message.contains("unique") || message.contains("constraint"))
                    && retry < MAX_RETRIES
                {
                    continue;
                }
                return Err(CommitCompletedUploadError::RolledBack {
                    completion_attempt_id,
                    source: AppError::from(db_err),
                });
            }
            Err(sea_orm::TransactionError::Connection(db_err)) => {
                return Err(CommitCompletedUploadError::OutcomeUnknown {
                    completion_attempt_id,
                    source: AppError::from(db_err),
                });
            }
        }
    }
    unreachable!("retry loop exhausted without returning")
}
```

The closure order remains mark prior latest false → insert this attempt row → delete exactly one upload and cascade parts → commit. Never retry `OutcomeUnknown`; never delete an object after the transaction.

Reconcile only the current attempt ID. If that row is absent, return `NotCommitted` immediately without querying the upload or any latest row belonging to another attempt:

```rust
fn classify_completion_attempt_state(
    expected: &crate::store::object::LatestObjectRow,
    object: Option<&crate::store::entities::object::Model>,
    upload: Option<&multipart_upload::Model>,
) -> ReconciledCommitOutcome {
    let Some(row) = object else {
        return ReconciledCommitOutcome::NotCommitted;
    };
    let exact = row.id == expected.id
        && row.bucket == expected.bucket
        && row.key == expected.key
        && row.cid == expected.cid
        && row.size == expected.size
        && row.content_type == expected.content_type
        && row.etag == expected.etag
        && row.metadata == expected.metadata
        && row.encrypted == expected.encrypted
        && row.key_wrap == expected.key_wrap
        && row.multipart == expected.multipart
        && row.is_latest;
    if exact && upload.is_none() {
        ReconciledCommitOutcome::Committed
    } else {
        ReconciledCommitOutcome::Unknown(AppError::Internal(format!(
            "mixed completion-attempt state for completion_attempt_id={} upload_id={}",
            expected.id,
            upload.map_or("absent", |row| row.upload_id.as_str()),
        )))
    }
}

pub async fn reconcile_completion_attempt<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    expected: &crate::store::object::LatestObjectRow,
) -> ReconciledCommitOutcome {
    let object = match crate::store::entities::object::Entity::find_by_id(expected.id.clone())
        .one(db)
        .await
    {
        Ok(Some(object)) => object,
        Ok(None) => return ReconciledCommitOutcome::NotCommitted,
        Err(error) => return ReconciledCommitOutcome::Unknown(AppError::from(error)),
    };
    let upload = match multipart_upload::Entity::find_by_id(upload_id.to_owned())
        .one(db)
        .await
    {
        Ok(upload) => upload,
        Err(error) => return ReconciledCommitOutcome::Unknown(AppError::from(error)),
    };
    classify_completion_attempt_state(expected, Some(&object), upload.as_ref())
}
```

Exact attempt row plus absent upload is the only success. Missing A row is `NotCommitted` even when B deleted the upload; present-but-mismatched A row, A row plus upload, or a query error is `Unknown`.

- [ ] **Step 10: Refactor Complete identities/finalize and reduce Abort to DB deletion**

Add the non-finalizing result with no part cleanup field:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct CompletedMultipartArchive {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub encryption_object_id: String,
    pub completion_attempt_id: String,
    pub root_cid: String,
    pub total_size: i64,
    pub content_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub decompress_zip_target: Option<String>,
    pub decompress_zip_result: bool,
    pub server_side_encryption: Option<ServerSideEncryption>,
}
```

At the start of every `complete_multipart_upload_inner` call, before reading upload/parts, generate exactly one attempt ID:

```rust
let completion_attempt_id = uuid::Uuid::new_v4().to_string();
```

After loading the upload, set `let encryption_object_id = upload.object_id.clone();`. Keep UploadPart encryption and Complete root re-encryption keyed by this stable value: replace only the local Complete encryption variable with `encryption_object_id.clone()` when calling `encrypt_chunk_stream`. Never substitute `completion_attempt_id` into crypto. Move existing validation, concat/decrypt/re-encrypt, root add, and root pin into the inner function; return both identities and metadata, but do not write an object row, delete upload/parts, expose part CIDs, or call `pin_rm`.

The finalizer seam names the row as an attempt and reconciles that exact row:

```rust
#[async_trait::async_trait]
pub(crate) trait CompletedUploadFinalizerStore: Send + Sync {
    async fn commit(
        &self,
        upload_id: &str,
        attempt: crate::store::object::LatestObjectRow,
    ) -> Result<(), crate::store::multipart::CommitCompletedUploadError>;

    async fn reconcile(
        &self,
        upload_id: &str,
        expected_attempt: &crate::store::object::LatestObjectRow,
    ) -> crate::store::multipart::ReconciledCommitOutcome;
}
```

`DatabaseCompletedUploadFinalizer` delegates to `commit_completed_upload` and `reconcile_completion_attempt`:

```rust
struct DatabaseCompletedUploadFinalizer<'a> {
    db: &'a sea_orm::DatabaseConnection,
}

#[async_trait::async_trait]
impl CompletedUploadFinalizerStore for DatabaseCompletedUploadFinalizer<'_> {
    async fn commit(
        &self,
        upload_id: &str,
        attempt: crate::store::object::LatestObjectRow,
    ) -> Result<(), crate::store::multipart::CommitCompletedUploadError> {
        crate::store::multipart::commit_completed_upload(self.db, upload_id, attempt).await
    }

    async fn reconcile(
        &self,
        upload_id: &str,
        expected_attempt: &crate::store::object::LatestObjectRow,
    ) -> crate::store::multipart::ReconciledCommitOutcome {
        crate::store::multipart::reconcile_completion_attempt(
            self.db,
            upload_id,
            expected_attempt,
        )
        .await
    }
}
```

Finalize performs DB decisions only—there is no Kubo state or CID-reclamation loop in the seam:

```rust
pub async fn finalize_completed_multipart_archive(
    state: &Arc<AppState>,
    completed: &CompletedMultipartArchive,
) -> S3Result<()> {
    let store = DatabaseCompletedUploadFinalizer { db: state.store.db() };
    finalize_completed_multipart_archive_with_store(completed, &store).await
}

async fn finalize_completed_multipart_archive_with_store<S: CompletedUploadFinalizerStore + ?Sized>(
    completed: &CompletedMultipartArchive,
    store: &S,
) -> S3Result<()> {
    let attempt = crate::store::object::LatestObjectRow {
        id: completed.completion_attempt_id.clone(),
        bucket: completed.bucket.clone(),
        key: completed.key.clone(),
        cid: completed.root_cid.clone(),
        size: completed.total_size,
        content_type: completed.content_type.clone(),
        etag: completed.root_cid.clone(),
        metadata: completed.metadata.clone(),
        encrypted: completed.encrypted,
        key_wrap: completed.key_wrap.clone(),
        multipart: true,
        created_at: chrono::Utc::now(),
    };

    match store.commit(&completed.upload_id, attempt.clone()).await {
        Ok(()) => Ok(()),
        Err(crate::store::multipart::CommitCompletedUploadError::RolledBack {
            completion_attempt_id,
            source,
        }) => {
            if completion_attempt_id != completed.completion_attempt_id {
                return Err(s3s::s3_error!(InternalError, "completion attempt mismatch"));
            }
            Err(source.into())
        }
        Err(crate::store::multipart::CommitCompletedUploadError::OutcomeUnknown {
            completion_attempt_id,
            source,
        }) => {
            if completion_attempt_id != completed.completion_attempt_id {
                return Err(s3s::s3_error!(InternalError, "completion attempt mismatch"));
            }
            match store.reconcile(&completed.upload_id, &attempt).await {
                crate::store::multipart::ReconciledCommitOutcome::Committed => Ok(()),
                crate::store::multipart::ReconciledCommitOutcome::NotCommitted => Err(source.into()),
                crate::store::multipart::ReconciledCommitOutcome::Unknown(reconcile_error) => {
                    Err(s3s::s3_error!(
                        InternalError,
                        "commit outcome unknown ({source}); reconciliation failed ({reconcile_error})"
                    ))
                }
            }
        }
    }
}
```

The standard Complete wrapper calls inner then finalize and returns the existing output; the decompression route finalizes only after extraction has no global reject. Direct success, reconciled success, rollback, not-committed, unknown, and all route errors retain root and part pins.

Reduce Abort to upload validation plus cascade deletion:

```rust
let upload = crate::store::multipart::get_upload(db, upload_id).await?;
if upload.bucket != *bucket || upload.key != *key {
    return Err(s3s::s3_error!(InvalidArgument, "bucket/key mismatch for upload_id"));
}
crate::store::multipart::delete_upload(db, upload_id).await?;
Ok(S3Response::new(AbortMultipartUploadOutput::default()))
```

Do not list or cat parts during Abort. The cascade removes DB rows; pins remain for future safe GC.

- [ ] **Step 11: Run Multipart/store verification and source guard scans**

```powershell
cargo test store::object --lib
cargo test store::multipart --lib
cargo test s3::ops::multipart --lib

$unsafeMultipart = rg -n "crate::kubo::pin::pin_rm\s*\(|\b(delete_part|insert_part)\s*\(" "src/s3/ops/multipart.rs" "src/store/multipart.rs"
if ($LASTEXITCODE -eq 0) { $unsafeMultipart; throw "multipart path still contains unsafe part cleanup or non-atomic insert" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }
```

Expected: all tests exit 0; replacement and DB-failure tests prove atomic row behavior with retained pins; single-part/root equality stays readable; shared-object tests survive replacement/Abort/Complete; rollback and all outcome branches issue zero root/part removals; A never adopts B's row; the barrier test yields one winner with its attempt ID and no loser row; the source guard exits successfully with no real `crate::kubo::pin::pin_rm(...)` product call and no old CRUD call. Test helpers such as `assert_pin_rm_calls` are explicitly allowed and do not match this guard. `src/kubo/pin.rs` may still define `pin_rm` and unrelated modules may still call it.

- [ ] **Step 12: Manual progress marker**

Record Task 6 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 7: Raw CompleteMultipartUpload route and multipart decompression

**Files:**
- Modify: `src/s3/route/decompress_zip.rs`
- Modify: `src/zip/response.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: Task 5 `reject_archive_key_collision`, `decoded_query_pairs`, `query_key_is_present`, `complete_multipart_upload_inner`, attempt-aware atomic `finalize_completed_multipart_archive`, `CompletedMultipartArchive` with separate encryption/attempt identities, `extract_zip_stream`, `publish_plain_object`, `complete_multipart_result_xml`, and `decompress_result_xml`.
- Produces: `MAX_COMPLETE_MULTIPART_XML_BYTES: usize = 4 * 1024 * 1024`, `collect_complete_xml(body: &mut s3s::Body) -> S3Result<Vec<u8>>`, `parse_complete_etag(value: String) -> S3Result<ETag>`, quick-xml 0.41 `Text` + `GeneralRef(BytesRef)` + restricted `Empty` CompleteMultipartUpload XML parser that preserves typed s3s 0.14 ETags and all five per-part checksum DTO fields after entity decoding, including explicit leading/trailing checksum whitespace, custom route handling for all `POST ?uploadId=...` requests, standard Complete XML for non-decompress uploads, and `DecompressZipResult` XML for decompress uploads. Only Complete XML crosses the bounded collector; archive/entry streams never do.

- [ ] **Step 1: Write failing Complete route tests**

Add parser tests to `src/s3/route/decompress_zip.rs`:

```rust
#[test]
fn parse_complete_multipart_xml_preserves_standard_checksum_fields() {
    let xml = r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag-1</ETag><ChecksumCRC32>crc32-value</ChecksumCRC32><ChecksumCRC32C>crc32c-value</ChecksumCRC32C><ChecksumCRC64NVME>crc64nvme-value</ChecksumCRC64NVME><ChecksumSHA1>sha1-value</ChecksumSHA1><ChecksumSHA256>sha256-value</ChecksumSHA256></Part></CompleteMultipartUpload>"#;
    let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();

    assert_eq!(parts[0].checksum_crc32.as_deref(), Some("crc32-value"));
    assert_eq!(parts[0].checksum_crc32c.as_deref(), Some("crc32c-value"));
    assert_eq!(parts[0].checksum_crc64nvme.as_deref(), Some("crc64nvme-value"));
    assert_eq!(parts[0].checksum_sha1.as_deref(), Some("sha1-value"));
    assert_eq!(parts[0].checksum_sha256.as_deref(), Some("sha256-value"));
}

#[test]
fn parse_complete_multipart_xml_preserves_checksum_text_boundaries() {
    let xml = br#"<CompleteMultipartUpload>
        <Part>
            <PartNumber> 1 </PartNumber>
            <ETag>etag-1</ETag>
            <ChecksumCRC32>  crc&amp;32  </ChecksumCRC32>
        </Part>
    </CompleteMultipartUpload>"#;

    let parts = parse_complete_multipart_xml(xml).unwrap();
    assert_eq!(parts[0].checksum_crc32.as_deref(), Some("  crc&32  "));
}

#[test]
fn parse_complete_multipart_xml_accepts_self_closing_checksum_as_empty_string() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag-1</ETag><ChecksumCRC32/><ChecksumCRC32C/><ChecksumCRC64NVME/><ChecksumSHA1/><ChecksumSHA256/></Part></CompleteMultipartUpload>"#;

    let parts = parse_complete_multipart_xml(xml).unwrap();
    assert_eq!(parts[0].checksum_crc32.as_deref(), Some(""));
    assert_eq!(parts[0].checksum_crc32c.as_deref(), Some(""));
    assert_eq!(parts[0].checksum_crc64nvme.as_deref(), Some(""));
    assert_eq!(parts[0].checksum_sha1.as_deref(), Some(""));
    assert_eq!(parts[0].checksum_sha256.as_deref(), Some(""));
}

#[test]
fn parse_complete_multipart_xml_rejects_duplicate_checksum_field() {
    assert_malformed_complete_xml(
        br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag</ETag><ChecksumSHA256/><ChecksumSHA256>second</ChecksumSHA256></Part></CompleteMultipartUpload>"#,
    );
}

#[test]
fn parse_complete_multipart_xml_extracts_parts_in_order() {
    let xml = r#"<CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
        <Part><PartNumber>2</PartNumber><ETag>etag-2</ETag></Part>
    </CompleteMultipartUpload>"#;

    let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].part_number, Some(1));
    assert_eq!(parts[0].e_tag.as_ref().map(|etag| etag.value()), Some("etag-1"));
    assert_eq!(parts[1].part_number, Some(2));
    assert_eq!(parts[1].e_tag.as_ref().map(|etag| etag.value()), Some("etag-2"));
}

#[test]
fn parse_complete_multipart_xml_accumulates_quoted_ampersand_general_refs() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;etag&amp;1&quot;</ETag></Part></CompleteMultipartUpload>"#;
    let parts = parse_complete_multipart_xml(xml).unwrap();
    assert_eq!(parts[0].e_tag.as_ref().map(|etag| etag.value()), Some("etag&1"));
}

#[test]
fn parse_complete_multipart_xml_preserves_weak_etag() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>W/&quot;QmPart&quot;</ETag></Part></CompleteMultipartUpload>"#;
    let parts = parse_complete_multipart_xml(xml).unwrap();
    assert!(matches!(parts[0].e_tag.as_ref(), Some(ETag::Weak(value)) if value == "QmPart"));
    assert_eq!(parts[0].e_tag.as_ref().map(ETag::value), Some("QmPart"));
}

#[test]
fn parse_complete_multipart_xml_falls_back_to_raw_strong_etag_on_invalid_format() {
    for (xml_value, expected) in [(r#"&quot;QmPart"#, r#""QmPart"#), ("W/QmPart", "W/QmPart")] {
        let xml = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{xml_value}</ETag></Part></CompleteMultipartUpload>");
        let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();
        assert!(matches!(parts[0].e_tag.as_ref(), Some(ETag::Strong(value)) if value == expected));
    }
}

#[test]
fn parse_complete_multipart_xml_rejects_invalid_character_in_etag() {
    assert_malformed_complete_xml("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"µ\"</ETag></Part></CompleteMultipartUpload>".as_bytes());
}

#[test]
fn parse_complete_multipart_xml_accepts_self_closing_etag_as_empty_strong() {
    let parts = parse_complete_multipart_xml(br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag/></Part></CompleteMultipartUpload>"#).unwrap();
    assert!(matches!(parts[0].e_tag.as_ref(), Some(ETag::Strong(value)) if value.is_empty()));
}

#[test]
fn parse_complete_multipart_xml_rejects_duplicate_etag_including_self_closing_form() {
    assert_malformed_complete_xml(br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag/><ETag>QmPart</ETag></Part></CompleteMultipartUpload>"#);
}

#[test]
fn parse_complete_multipart_xml_resolves_decimal_and_hex_numeric_refs() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>&#49;</PartNumber><ETag>&#34;etag&#38;&#x31;&#x22;</ETag></Part></CompleteMultipartUpload>"#;
    let parts = parse_complete_multipart_xml(xml).unwrap();
    assert_eq!(parts[0].part_number, Some(1));
    assert_eq!(parts[0].e_tag.as_ref().map(|etag| etag.value()), Some("etag&1"));
}

#[test]
fn parse_complete_multipart_xml_rejects_unknown_entity() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&bogus;</ETag></Part></CompleteMultipartUpload>"#;
    assert_eq!(parse_complete_multipart_xml(xml).unwrap_err().code().as_str(), "MalformedXML");
}

#[test]
fn parse_complete_multipart_xml_rejects_invalid_numeric_ref() {
    let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&#xZZ;</ETag></Part></CompleteMultipartUpload>"#;
    assert_eq!(parse_complete_multipart_xml(xml).unwrap_err().code().as_str(), "MalformedXML");
}

fn assert_malformed_complete_xml(xml: &[u8]) {
    assert_eq!(
        parse_complete_multipart_xml(xml)
            .unwrap_err()
            .code()
            .as_str(),
        "MalformedXML"
    );
}

#[test]
fn parse_complete_multipart_xml_rejects_wrong_root() {
    assert_malformed_complete_xml(
        br#"<WrongRoot><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></WrongRoot>"#,
    );
}

#[test]
fn parse_complete_multipart_xml_rejects_general_ref_outside_a_field() {
    assert_malformed_complete_xml(
        br#"<CompleteMultipartUpload>&bogus;<Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
    );
}

#[test]
fn parse_complete_multipart_xml_rejects_nested_element_inside_a_field() {
    assert_malformed_complete_xml(
        br#"<CompleteMultipartUpload><Part><PartNumber>1<Unexpected/></PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
    );
}

#[test]
fn parse_complete_multipart_xml_rejects_non_whitespace_text_outside_a_field() {
    assert_malformed_complete_xml(
        br#"<CompleteMultipartUpload>unexpected<Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
    );
}
```

The boundary test proves that disabled reader trimming does not consume checksum-leading/trailing whitespace and that `GeneralRef` is appended at its original character boundary; its indented `PartNumber` also locks whitespace-tolerant numeric parsing. The self-closing checksum test proves all five legal attribute-free checksum elements have the same empty-String DTO meaning as s3s 0.14. The ETag tests lock s3s 0.14 parity: escaped weak tags remain typed `Weak`, bad HTTP-ETag format retains the exact decoded raw `Strong` value, invalid ETag characters become `MalformedXML`, and an attribute-free self-closing ETag is `Strong("")`; a second ETag remains a duplicate error. These negative tests lock the strict state machine: `parse_complete_multipart_xml_rejects_wrong_root` rejects a root other than `CompleteMultipartUpload`; `parse_complete_multipart_xml_rejects_general_ref_outside_a_field` rejects entity references outside an open field; `parse_complete_multipart_xml_rejects_nested_element_inside_a_field` rejects unknown/nested elements; and `parse_complete_multipart_xml_rejects_non_whitespace_text_outside_a_field` rejects text between structural elements. The checksum duplicate test proves the five accepted checksum fields are still individually single-use, including a self-closing form followed by Start/End or vice versa. The implementation also rejects unknown root children, duplicate or missing required fields, `PartNumber` Empty, field-internal Empty, attributes outside the root namespace declaration, mismatched closing tags, and unsupported trailing content.

Add frame-level collector tests in the same module. `framed_complete_body` uses `http_body_util::StreamBody`, `hyper::body::Frame`, and `Body::http_body_unsync` so one test crosses several data frames and a trailer rather than relying on a single in-memory frame:

```rust
fn framed_complete_body(
    frames: Vec<Result<hyper::body::Frame<bytes::Bytes>, std::io::Error>>,
) -> Body {
    Body::http_body_unsync(http_body_util::StreamBody::new(
        futures_util::stream::iter(frames),
    ))
}

#[tokio::test]
async fn complete_xml_collector_accepts_exactly_four_mib_across_frames() {
    let first = bytes::Bytes::from(vec![b' '; 2 * 1024 * 1024]);
    let second = bytes::Bytes::from(vec![b' '; 2 * 1024 * 1024]);
    let mut trailers = http::HeaderMap::new();
    trailers.insert("x-test-trailer", http::HeaderValue::from_static("ignored"));
    let mut body = framed_complete_body(vec![
        Ok(hyper::body::Frame::data(first)),
        Ok(hyper::body::Frame::data(second)),
        Ok(hyper::body::Frame::trailers(trailers)),
    ]);

    let bytes = collect_complete_xml(&mut body).await.unwrap();
    assert_eq!(bytes.len(), MAX_COMPLETE_MULTIPART_XML_BYTES);
}

#[tokio::test]
async fn complete_xml_collector_rejects_one_byte_over_limit() {
    let mut body = framed_complete_body(vec![
        Ok(hyper::body::Frame::data(bytes::Bytes::from(vec![
            b'x'; MAX_COMPLETE_MULTIPART_XML_BYTES
        ]))),
        Ok(hyper::body::Frame::data(bytes::Bytes::from_static(b"x"))),
    ]);

    let error = collect_complete_xml(&mut body).await.unwrap_err();
    assert_eq!(error.code().as_str(), "InvalidRequest");
    assert_eq!(error.message(), Some("CompleteMultipartUpload XML exceeds 4 MiB"));
}

#[tokio::test]
async fn complete_xml_collector_maps_frame_error_to_incomplete_body() {
    let mut body = framed_complete_body(vec![Err(std::io::Error::other("broken body"))]);
    let error = collect_complete_xml(&mut body).await.unwrap_err();
    assert_eq!(error.code().as_str(), "IncompleteBody");
    assert!(error.to_string().contains("failed to read CompleteMultipartUpload XML: broken body"));
}
```

The exactly-4-MiB test is a collector-boundary test, not an XML-validity claim; the existing normal XML parser test proves a normal control document passes into parsing. The 4-MiB+1 test's two-frame shape locks the required check-before-extend behavior at a frame boundary, and the trailer proves non-data frames are ignored.

Replace Task 5's transitional `route_matches_decompress_put_but_not_complete_before_task_7` test with the final route contract:

```rust
#[tokio::test]
async fn route_matches_decompress_put_and_complete_but_not_create() {
    let route = dummy_route().await;
    let mut ext = http::Extensions::new();
    assert!(route.is_match(
        &Method::PUT,
        &"/bucket/archive.zip?decompress-zip=prefix%2F".parse::<Uri>().unwrap(),
        &HeaderMap::new(),
        &mut ext,
    ));
    assert!(route.is_match(
        &Method::POST,
        &"/bucket/archive.zip?uploadId=upload-1".parse::<Uri>().unwrap(),
        &HeaderMap::new(),
        &mut ext,
    ));
    assert!(!route.is_match(
        &Method::POST,
        &"/bucket/archive.zip?uploads=&decompress-zip=prefix%2F".parse::<Uri>().unwrap(),
        &HeaderMap::new(),
        &mut ext,
    ));
}
```

Add a route test for non-decompress uploads to ensure intercepting all completes does not break standard Complete:

```rust
#[tokio::test]
async fn complete_route_returns_standard_xml_for_non_decompress_upload() {
    let (route, state) = route_with_seeded_sse_multipart(false).await;
    let req = signed_route_request(
        Method::POST,
        "/bucket/archive.zip?uploadId=upload-1",
        Body::from("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_string()),
    );

    let response = route.call(req).await.unwrap();
    assert_eq!(response.status.unwrap_or(http::StatusCode::OK), http::StatusCode::OK);
    assert_eq!(response.headers.get(http::header::ETAG).unwrap(), "\"QmRoot\"");
    assert_eq!(response.headers.get("x-amz-server-side-encryption").unwrap(), "AES256");
    let body = collect_body_string(response.output).await;
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert!(body.contains("<ETag>\"QmRoot\"</ETag>"));
}
```

`route_with_seeded_sse_multipart(false)` seeds a non-decompress upload whose `encryption_mode` is `sse_s3` and whose completed archive returns `server_side_encryption: Some(ServerSideEncryption::from_static("AES256"))`; this proves the raw route preserves the standard Complete SSE response header. `route_with_seeded_plain_multipart(true)` uses plaintext because decompress uploads reject SSE at Create time.

Add a route test for decompress uploads:

```rust
#[tokio::test]
async fn complete_route_extracts_when_upload_has_decompress_target() {
    let (route, state) = route_with_seeded_plain_multipart(true).await;
    let req = signed_route_request(
        Method::POST,
        "/bucket/archive.zip?uploadId=upload-1",
        Body::from("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_string()),
    );

    let response = route.call(req).await.unwrap();
    assert_eq!(response.status.unwrap_or(http::StatusCode::OK), http::StatusCode::OK);
    assert_eq!(response.headers.get(http::header::ETAG).unwrap(), "\"QmRoot\"");
    let body = collect_body_string(response.output).await;
    assert!(body.contains("<DecompressZipResult>"));
    crate::store::object::get_latest(state.store.db(), "bucket", "prefix/foo/bar.txt")
        .await
        .unwrap();
}
```

Add `complete_route_global_reject_keeps_root_and_staged_entry_pins`: Complete inner and extraction use a ZIP whose first safe entry pins `QmSharedEntry` and whose next entry traverses; expect `InvalidParameterValue`, no archive/entry latest rows, upload/part rows still present, and zero `/pin/rm` calls for `QmRoot`, `QmSharedEntry`, or `QmPart`. Add `complete_route_entry_publish_failure_keeps_shared_pins_and_success_visible`: install a SQLite trigger that rejects only one entry insert after archive finalize, expect a 200 partial result, assert archive plus the other entry remain readable, assert upload/part DB rows were deleted by the committed transaction, and forbid `QmPart`, root, and all entry CIDs under `/pin/rm`.

Add `complete_route_archive_key_collision_preserves_retry_state`: seed a decompress upload with `decompress_zip_target=Some("")`; extract one successful `ExtractedEntry { key: "archive.zip", cid: "QmCollisionEntry", ... }`; expect the fixed `InvalidParameterValue` code/message, archive/entry latest absence, unchanged upload/part rows and original part ETag, and zero `/pin/rm` for `QmRoot`, `QmPart`, and `QmCollisionEntry`. The unchanged upload/parts prove finalize was not reached. This route-level test locks the consumer order before Task 8 proves it over HTTP.

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_extracts_parts_in_order --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_preserves_standard_checksum_fields --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_preserves_checksum_text_boundaries --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_accepts_self_closing_checksum_as_empty_string --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_duplicate_checksum_field --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_accumulates_quoted_ampersand_general_refs --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_preserves_weak_etag --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_falls_back_to_raw_strong_etag_on_invalid_format --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_invalid_character_in_etag --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_accepts_self_closing_etag_as_empty_strong --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_duplicate_etag_including_self_closing_form --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_resolves_decimal_and_hex_numeric_refs --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_unknown_entity --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_invalid_numeric_ref --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_wrong_root --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_general_ref_outside_a_field --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_nested_element_inside_a_field --lib
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_rejects_non_whitespace_text_outside_a_field --lib
cargo test s3::route::decompress_zip::tests::complete_xml_collector_accepts_exactly_four_mib_across_frames --lib
cargo test s3::route::decompress_zip::tests::complete_xml_collector_rejects_one_byte_over_limit --lib
cargo test s3::route::decompress_zip::tests::complete_xml_collector_maps_frame_error_to_incomplete_body --lib
cargo test s3::route::decompress_zip::tests::complete_route_returns_standard_xml_for_non_decompress_upload --lib
cargo test s3::route::decompress_zip::tests::complete_route_extracts_when_upload_has_decompress_target --lib
cargo test s3::route::decompress_zip::tests::complete_route_global_reject_keeps_root_and_staged_entry_pins --lib
cargo test s3::route::decompress_zip::tests::complete_route_archive_key_collision_preserves_retry_state --lib
cargo test s3::route::decompress_zip::tests::complete_route_entry_publish_failure_keeps_shared_pins_and_success_visible --lib
```

Expected: tests fail because the bounded frame collector and parser do not exist, `call_complete` and its pre-finalize collision check are not implemented, and conservative multipart root/entry pin behavior is not wired.

- [ ] **Step 3: Implement strict Complete XML parsing**

Use the following explicit whitelist state machine. Import `use s3s::dto::{ETag, ParseETagError};`. The root is exactly `CompleteMultipartUpload`, where only `xmlns` or `xmlns:*` attributes are allowed. Its direct children are `Part`; each `Part` requires one nonempty `PartNumber` and one present `ETag`, and may contain at most one each of `ChecksumCRC32`, `ChecksumCRC32C`, `ChecksumCRC64NVME`, `ChecksumSHA1`, and `ChecksumSHA256`; no `Part` field has attributes. Store `current_etag` as `Option<ETag>`. Reader text trimming is disabled: in an open checksum field, append decoded `Text` and `GeneralRef(BytesRef)` exactly in event order. Outside fields, accept only whitespace text via `decoded.trim().is_empty()`, a declaration before the root, and comments while no field is open. `Event::Empty` is accepted only while inside `Part`, with no field open, no prior occurrence, and no attributes: it stores `Some(String::new())` for any legal checksum or calls `parse_complete_etag(String::new())` for ETag. Every other event is `MalformedXML`.

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompleteField {
    PartNumber,
    ETag,
    ChecksumCRC32,
    ChecksumCRC32C,
    ChecksumCRC64NVME,
    ChecksumSHA1,
    ChecksumSHA256,
}

fn append_general_ref(
    value: &mut String,
    reference: quick_xml::events::BytesRef<'_>,
) -> S3Result<()> {
    if reference.is_char_ref() {
        let character = reference
            .resolve_char_ref()
            .map_err(|error| {
                s3s::s3_error!(MalformedXML, "invalid numeric XML reference: {error}")
            })?
            .ok_or_else(|| s3s::s3_error!(MalformedXML, "invalid numeric XML reference"))?;
        value.push(character);
        return Ok(());
    }

    let name = reference
        .decode()
        .map_err(|error| s3s::s3_error!(MalformedXML, "invalid XML entity encoding: {error}"))?;
    let replacement = match name.as_ref() {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "apos" => '\'',
        "quot" => '"',
        _ => return Err(s3s::s3_error!(MalformedXML, "unknown XML entity: {name}")),
    };
    value.push(replacement);
    Ok(())
}

fn malformed_complete_xml(message: impl std::fmt::Display) -> s3s::S3Error {
    s3s::s3_error!(MalformedXML, "{message}")
}

fn parse_complete_etag(value: String) -> S3Result<ETag> {
    match ETag::parse_http_header(value.as_bytes()) {
        Ok(etag) => Ok(etag),
        Err(ParseETagError::InvalidFormat) => Ok(ETag::Strong(value)),
        Err(ParseETagError::InvalidChar) => Err(malformed_complete_xml("invalid ETag character")),
    }
}

fn validate_root_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    for attribute in event.attributes() {
        let attribute = attribute.map_err(|error| {
            malformed_complete_xml(format!(
                "invalid CompleteMultipartUpload attribute: {error}"
            ))
        })?;
        let name = attribute.key.as_ref();
        if name != b"xmlns" && !name.starts_with(b"xmlns:") {
            return Err(malformed_complete_xml(
                "CompleteMultipartUpload only permits xmlns attributes",
            ));
        }
        if attribute.value.as_ref().contains(&b'&') {
            return Err(malformed_complete_xml(
                "CompleteMultipartUpload namespace contains an entity reference",
            ));
        }
    }
    Ok(())
}

fn reject_element_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    if let Some(attribute) = event.attributes().next() {
        attribute.map_err(|error| {
            malformed_complete_xml(format!(
                "invalid CompleteMultipartUpload attribute: {error}"
            ))
        })?;
        return Err(malformed_complete_xml(
            "Part and CompleteMultipartUpload fields must not have attributes",
        ));
    }
    Ok(())
}

fn parse_complete_multipart_xml(bytes: &[u8]) -> S3Result<Vec<s3s::dto::CompletedPart>> {
    let mut reader = quick_xml::Reader::from_reader(bytes);
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut parts = Vec::new();
    let mut saw_declaration = false;
    let mut saw_root = false;
    let mut closed_root = false;
    let mut in_part = false;
    let mut current_part_number = None;
    let mut current_etag = None;
    let mut current_checksum_crc32 = None;
    let mut current_checksum_crc32c = None;
    let mut current_checksum_crc64nvme = None;
    let mut current_checksum_sha1 = None;
    let mut current_checksum_sha256 = None;
    let mut current_field = None;
    let mut field_value = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(event)) => match event.name().as_ref() {
                b"CompleteMultipartUpload" if !saw_root && !closed_root => {
                    validate_root_attributes(&event)?;
                    saw_root = true;
                }
                b"Part" if saw_root && !closed_root && !in_part && current_field.is_none() => {
                    reject_element_attributes(&event)?;
                    in_part = true;
                    current_part_number = None;
                    current_etag = None;
                    current_checksum_crc32 = None;
                    current_checksum_crc32c = None;
                    current_checksum_crc64nvme = None;
                    current_checksum_sha1 = None;
                    current_checksum_sha256 = None;
                }
                b"PartNumber"
                    if in_part && current_field.is_none() && current_part_number.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::PartNumber);
                    field_value.clear();
                }
                b"ETag" if in_part && current_field.is_none() && current_etag.is_none() => {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ETag);
                    field_value.clear();
                }
                b"ChecksumCRC32"
                    if in_part && current_field.is_none() && current_checksum_crc32.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC32);
                    field_value.clear();
                }
                b"ChecksumCRC32C"
                    if in_part && current_field.is_none() && current_checksum_crc32c.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC32C);
                    field_value.clear();
                }
                b"ChecksumCRC64NVME"
                    if in_part
                        && current_field.is_none()
                        && current_checksum_crc64nvme.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC64NVME);
                    field_value.clear();
                }
                b"ChecksumSHA1"
                    if in_part && current_field.is_none() && current_checksum_sha1.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumSHA1);
                    field_value.clear();
                }
                b"ChecksumSHA256"
                    if in_part && current_field.is_none() && current_checksum_sha256.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumSHA256);
                    field_value.clear();
                }
                _ => {
                    return Err(malformed_complete_xml(
                        "unexpected CompleteMultipartUpload element or nesting",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Text(text)) => {
                let decoded = text.decode().map_err(|error| {
                    s3s::s3_error!(
                        MalformedXML,
                        "invalid CompleteMultipartUpload text encoding: {error}"
                    )
                })?;
                if current_field.is_some() {
                    field_value.push_str(decoded.as_ref());
                } else if !decoded.trim().is_empty() {
                    return Err(malformed_complete_xml(
                        "non-whitespace text outside CompleteMultipartUpload fields",
                    ));
                }
            }
            Ok(quick_xml::events::Event::GeneralRef(reference)) => match current_field {
                Some(_) => append_general_ref(&mut field_value, reference)?,
                None => {
                    return Err(malformed_complete_xml(
                        "entity reference outside CompleteMultipartUpload fields",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Empty(event)) => {
                reject_element_attributes(&event)?;
                match event.name().as_ref() {
                    b"ChecksumCRC32"
                        if in_part && current_field.is_none() && current_checksum_crc32.is_none() =>
                    {
                        current_checksum_crc32 = Some(String::new());
                    }
                    b"ChecksumCRC32C"
                        if in_part && current_field.is_none() && current_checksum_crc32c.is_none() =>
                    {
                        current_checksum_crc32c = Some(String::new());
                    }
                    b"ChecksumCRC64NVME"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_crc64nvme.is_none() =>
                    {
                        current_checksum_crc64nvme = Some(String::new());
                    }
                    b"ChecksumSHA1"
                        if in_part && current_field.is_none() && current_checksum_sha1.is_none() =>
                    {
                        current_checksum_sha1 = Some(String::new());
                    }
                    b"ChecksumSHA256"
                        if in_part && current_field.is_none() && current_checksum_sha256.is_none() =>
                    {
                        current_checksum_sha256 = Some(String::new());
                    }
                    b"ETag" if in_part && current_field.is_none() && current_etag.is_none() => {
                        current_etag = Some(parse_complete_etag(String::new())?);
                    }
                    _ => {
                        return Err(malformed_complete_xml(
                            "unexpected self-closing CompleteMultipartUpload element or nesting",
                        ));
                    }
                }
            }
            Ok(quick_xml::events::Event::End(event)) => match event.name().as_ref() {
                b"PartNumber" if in_part && current_field == Some(CompleteField::PartNumber) => {
                    let value = std::mem::take(&mut field_value);
                    current_part_number = Some(
                        value
                            .trim()
                            .parse::<i32>()
                            .map_err(|_| s3s::s3_error!(MalformedXML, "invalid PartNumber"))?,
                    );
                    current_field = None;
                }
                b"ETag" if in_part && current_field == Some(CompleteField::ETag) => {
                    let value = std::mem::take(&mut field_value);
                    current_etag = Some(parse_complete_etag(value)?);
                    current_field = None;
                }
                b"ChecksumCRC32"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC32) =>
                {
                    current_checksum_crc32 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumCRC32C"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC32C) =>
                {
                    current_checksum_crc32c = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumCRC64NVME"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC64NVME) =>
                {
                    current_checksum_crc64nvme = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumSHA1"
                    if in_part && current_field == Some(CompleteField::ChecksumSHA1) =>
                {
                    current_checksum_sha1 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumSHA256"
                    if in_part && current_field == Some(CompleteField::ChecksumSHA256) =>
                {
                    current_checksum_sha256 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"Part" if in_part && current_field.is_none() => {
                    let part_number = current_part_number
                        .ok_or_else(|| malformed_complete_xml("PartNumber is required"))?;
                    let e_tag = current_etag
                        .take()
                        .ok_or_else(|| malformed_complete_xml("ETag is required"))?;
                    parts.push(s3s::dto::CompletedPart {
                        part_number: Some(part_number),
                        e_tag: Some(e_tag),
                        checksum_crc32: current_checksum_crc32.take(),
                        checksum_crc32c: current_checksum_crc32c.take(),
                        checksum_crc64nvme: current_checksum_crc64nvme.take(),
                        checksum_sha1: current_checksum_sha1.take(),
                        checksum_sha256: current_checksum_sha256.take(),
                    });
                    in_part = false;
                }
                b"CompleteMultipartUpload"
                    if saw_root && !closed_root && !in_part && current_field.is_none() =>
                {
                    closed_root = true;
                }
                _ => {
                    return Err(malformed_complete_xml(
                        "mismatched or unexpected CompleteMultipartUpload closing element",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Decl(_)) if !saw_root && !saw_declaration => {
                saw_declaration = true;
            }
            Ok(quick_xml::events::Event::Comment(_)) if current_field.is_none() => {}
            Ok(quick_xml::events::Event::Eof) => {
                if saw_root && closed_root && !in_part && current_field.is_none() {
                    break;
                }
                return Err(malformed_complete_xml(
                    "incomplete CompleteMultipartUpload document",
                ));
            }
            Err(error) => {
                return Err(s3s::s3_error!(
                    MalformedXML,
                    "invalid CompleteMultipartUpload XML: {error}"
                ));
            }
            Ok(_) => {
                return Err(malformed_complete_xml(
                    "unsupported CompleteMultipartUpload XML content",
                ));
            }
        }
        buf.clear();
    }

    Ok(parts)
}
```

Do not apply a second unescape pass to decoded text. Reader-produced `Text` contains ordinary character data, and every entity/reference boundary is handled by the field-only `GeneralRef` arm. Unknown named entities, invalid decimal/hex references, reference decoding errors, text decoding errors, XML reader errors, declarations after the root, comments inside a field, and all unsupported events map to `MalformedXML`.

- [ ] **Step 4: Implement `call_complete` with standard and decompress responses**

Implement a hard-bounded frame collector for Complete XML only. Import `bytes::Buf` and `http_body_util::BodyExt`; locked `s3s::Body` implements `http_body::Body<Data = Bytes> + Unpin`, satisfying `BodyExt::frame(&mut self)`. The size check is performed after `checked_add(data.remaining())` and before `reserve`/`extend_from_slice`; body errors and trailers follow the fixed contract:

```rust
const MAX_COMPLETE_MULTIPART_XML_BYTES: usize = 4 * 1024 * 1024;

fn complete_xml_too_large() -> s3s::S3Error {
    s3s::s3_error!(
        InvalidRequest,
        "CompleteMultipartUpload XML exceeds 4 MiB"
    )
}

async fn collect_complete_xml(body: &mut Body) -> S3Result<Vec<u8>> {
    use bytes::Buf as _;
    use http_body_util::BodyExt as _;

    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            s3s::s3_error!(
                IncompleteBody,
                "failed to read CompleteMultipartUpload XML: {error}"
            )
        })?;
        let Ok(mut data) = frame.into_data() else {
            continue; // trailers and future non-data frame kinds are not XML bytes
        };
        let data_len = data.remaining();
        let next_len = bytes
            .len()
            .checked_add(data_len)
            .ok_or_else(complete_xml_too_large)?;
        if next_len > MAX_COMPLETE_MULTIPART_XML_BYTES {
            return Err(complete_xml_too_large());
        }

        bytes.reserve(data_len);
        while data.has_remaining() {
            let chunk = data.chunk();
            let chunk_len = chunk.len();
            bytes.extend_from_slice(chunk);
            data.advance(chunk_len);
        }
    }
    Ok(bytes)
}
```

Do not use `Content-Length` as the enforcement mechanism. An optional early rejection may be added later, but every request—including HTTP/1.1 chunked requests with no `Content-Length`—must cross the frame counter above. The Complete XML vector is the only allowed bounded control-body buffer in the feature.

Use it before any call that reads parts, writes DB state, or touches Kubo:

```rust
async fn call_complete(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    let (bucket, key) = parse_path_bucket_key(&req.uri)?;
    let upload_id = crate::s3::query::decoded_query_pairs(&req.uri)?
        .into_iter()
        .filter_map(|(name, value)| (name == "uploadId").then_some(value))
        .last()
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "uploadId is required"))?;

    let body_bytes = collect_complete_xml(&mut req.input).await?;
    let parts = parse_complete_multipart_xml(&body_bytes)?;

    let input = s3s::dto::CompleteMultipartUploadInput {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        multipart_upload: Some(s3s::dto::CompletedMultipartUpload { parts: Some(parts), ..Default::default() }),
        ..Default::default()
    };
    let inner_req = S3Request {
        input,
        method: req.method,
        uri: req.uri,
        headers: req.headers,
        extensions: req.extensions,
        credentials: req.credentials,
        region: req.region,
        service: req.service,
        trailing_headers: req.trailing_headers,
    };

    let completed = crate::s3::ops::multipart::complete_multipart_upload_inner(&self.state, inner_req).await?;
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/xml"));
    headers.insert(http::header::ETAG, http::HeaderValue::from_str(&format!("\"{}\"", completed.root_cid)).unwrap());
    if let Some(ref sse) = completed.server_side_encryption {
        headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_str(sse.as_str()).unwrap(),
        );
    }

    if let Some(target_prefix) = completed.decompress_zip_target.clone() {
        let archive_stream = match crate::kubo::cat::stream_cat(&self.state.kubo, &completed.root_cid, None).await {
            Ok(stream) => stream,
            Err(e) => {
                return Err(s3s::s3_error!(InternalError, "cat completed archive: {e}"));
            }
        };
        let outcome = match crate::zip::extract::extract_zip_stream(&self.state, &target_prefix, archive_stream).await {
            Ok(outcome) => outcome,
            Err(err) => {
                return Err(err);
            }
        };

        reject_archive_key_collision(&completed.key, &outcome.entries)?;

        if let Err(err) = crate::s3::ops::multipart::finalize_completed_multipart_archive(&self.state, &completed).await {
            return Err(err);
        }

        let mut published = Vec::new();
        let mut failures = outcome.failures;
        for entry in outcome.entries {
            let stored = crate::s3::ops::object::StoredObject { cid: entry.cid.clone(), size: entry.size };
            match crate::s3::ops::object::publish_plain_object(&self.state, &completed.bucket, &entry.key, None, None, &stored, false).await {
                Ok(()) => published.push(entry),
                Err(err) => failures.push(crate::zip::response::ExtractFailure {
                    entry_name: entry.key,
                    code: "EntryPublishFailed".to_string(),
                    message: err.to_string(),
                }),
            }
        }

        if completed.decompress_zip_result {
            let result = crate::zip::response::DecompressZipResult {
                archive_key: completed.key,
                archive_cid: completed.root_cid,
                archive_size: completed.total_size,
                entries: published,
                failures,
            };
            let xml = crate::zip::response::decompress_result_xml(&result);
            return Ok(S3Response::with_headers(Body::from(xml), headers));
        }

        let xml = crate::zip::response::complete_multipart_result_xml(&completed.bucket, &completed.key, &completed.root_cid);
        return Ok(S3Response::with_headers(Body::from(xml), headers));
    }

    crate::s3::ops::multipart::finalize_completed_multipart_archive(&self.state, &completed).await?;

    let xml = crate::zip::response::complete_multipart_result_xml(&completed.bucket, &completed.key, &completed.root_cid);
    Ok(S3Response::with_headers(Body::from(xml), headers))
}
```

Task 1 adds `http-body-util = "0.1"` as a direct dependency rather than relying on s3s's transitive dependency. No archive/entry path imports `BodyExt` for collection, and the Step 5 source guard rejects the obsolete unbounded request-body expression.

In the same step, atomically extend `is_match` and `call` from Task 5:

```rust
fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut http::Extensions) -> bool {
    (*method == Method::PUT && crate::s3::query::query_key_is_present(uri, "decompress-zip"))
        || (*method == Method::POST
            && crate::s3::query::query_key_is_present(uri, "uploadId")
            && !crate::s3::query::query_key_is_present(uri, "uploads"))
}

async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    self.check_access(&mut req).await?;
    if req.method == Method::PUT {
        return self.call_put(req).await;
    }
    if req.method == Method::POST {
        return self.call_complete(req).await;
    }
    Err(s3s::s3_error!(MethodNotAllowed, "unsupported decompress route method"))
}
```

The shared collision helper runs after extraction but before `finalize_completed_multipart_archive` and before entry publication. A collision therefore leaves upload/parts untouched for retry and publishes no archive or entry latest. Cat failure, extraction path/format/collision global reject, finalize `RolledBack`, reconciled `NotCommitted`, and reconciled `Unknown` all return without removing root, part, or any successfully pinned entry CID. Direct/reconciled finalize success deletes upload/part DB rows transactionally but also retains every part pin. This route has neither staged-pin reclamation nor any part-reclamation path.

- [ ] **Step 5: Run Complete route tests to verify they pass**

Run:

```powershell
cargo test s3::route::decompress_zip::tests --lib
cargo test s3::ops::multipart --lib

$unsafeCompleteCollector = rg -n -F -- ("req.input." + "collect") "src/s3/route/decompress_zip.rs"
if ($LASTEXITCODE -eq 0) { $unsafeCompleteCollector; throw "Complete route still uses an unbounded request-input collector" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }
```

Expected: collector tests prove exactly 4 MiB succeeds, 4 MiB + 1 returns the fixed `InvalidRequest` before appending the final frame, body errors map to `IncompleteBody`, and trailers are ignored; route Complete tests and multipart operation tests still pass; the source guard exits 1 for no match and the PowerShell block succeeds.

- [ ] **Step 6: Manual progress marker**

Record Task 7 as complete in this checklist. Do not stage or commit; the user-requested implementation has one final commit only.

---

### Task 8: Real-service integration and acceptance coverage

**Files:**
- Create: `src/s3/http.rs`
- Modify: `src/s3/mod.rs`
- Modify: `src/main.rs`
- Create: `tests/support/mod.rs`
- Modify: `tests/support/sigv4.rs`
- Modify: `tests/support/decompress.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: real `axum::Router` + `S3ServiceBuilder` + `GatewayAuth`, `DecompressZipRoute`, in-memory SQLite, wiremock Kubo, Task 4 ZIP fixtures, and regular dependencies `hmac`, `sha2`, `hex`, `chrono`, `percent-encoding`, `reqwest`, and `quick-xml`.
- Produces: `bridge_chunked_content_length`, the production and test registrations for that bridge, `TestHarness`, `KuboScript`, `archive_key_collision_zip() -> Vec<u8>`, `duplicate_entry_zip() -> Vec<u8>`, header signer `send_sigv4`, `send_sigv4_chunked_http1`, query signer `presign_sigv4_query`, multipart request helpers including raw signed Complete XML, inbound HTTP header/Kubo request-log assertions, and server-level acceptance tests. No Task 8 test may construct `S3Request`, invoke `DecompressZipRoute::call`, or call an operation function directly; header Authorization tests do not substitute for presigned-query tests or vice versa.

- [ ] **Step 1: Replace the tuple harness with one real-service harness**

Create `tests/support/mod.rs`:

```rust
pub mod decompress;
pub mod sigv4;
```

Create `src/s3/http.rs` and export it from `src/s3/mod.rs` with `pub mod http;`. Its only middleware is:

```rust
pub async fn bridge_chunked_content_length(mut request: Request, next: Next) -> Response {
    let is_chunked = request
        .headers()
        .get(TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        });

    if is_chunked
        && !request.headers().contains_key(CONTENT_LENGTH)
        && let Some(length) = request
            .headers()
            .get("x-amz-decoded-content-length")
            .cloned()
        && length.as_bytes().iter().all(|byte| byte.is_ascii_digit())
    {
        request.headers_mut().insert(CONTENT_LENGTH, length);
    }

    next.run(request).await
}
```

Hyper has already completed HTTP framing and decoded the HTTP/1.1 chunk body when this middleware receives the request. The bridge inspects the one `Transfer-Encoding` value returned by `HeaderMap::get`, accepts a comma-separated, case-insensitive `chunked` token, requires no internal `Content-Length`, then reads one `x-amz-decoded-content-length` value and accepts it only when every byte is an ASCII digit. It inserts that value as an internal `Content-Length` so s3s's single-chunk SigV4 length, payload-hash, and EOF checks can run. It does not alter wire framing, buffer the body, parse chunks, or replace s3s authentication. It does not enumerate duplicate decoded-length headers or claim behavior for Transfer-Encoding cases beyond the source check above. If an internal request already has both transfer encoding and content length, the bridge does nothing, so it cannot become a second HTTP parser.

The decoded-length header is not trusted authentication data. In the test helper it is inserted before canonicalization and signing, so it appears in `SignedHeaders`; s3s still checks the actual streamed length, EOF, and payload hash.

Extend `tests/support/decompress.rs` with:

```rust
pub enum AddReply {
    Ok(&'static str),
    Error(http::StatusCode, &'static str),
}

pub struct KuboScript {
    pub add_replies: Vec<AddReply>,
    pub cat_bodies: std::collections::HashMap<String, Vec<u8>>,
}

impl KuboScript {
    pub fn repeated_add(cid: &'static str, calls: usize, cat_bodies: std::collections::HashMap<String, Vec<u8>>) -> Self;
}

pub struct TestHarness {
    pub endpoint: String,
    pub bucket: String,
    pub state: std::sync::Arc<ipfs_s3_gateway::state::AppState>,
    pub kubo: wiremock::MockServer,
    pub observed_http: std::sync::Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
}

#[derive(Clone)]
pub struct ObservedHttpRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub headers: http::HeaderMap,
}

pub async fn start_harness(script: KuboScript) -> TestHarness;
pub async fn assert_pin_calls(harness: &TestHarness, path: &str, required: &[&str], forbidden: &[&str]);
pub async fn assert_no_kubo_calls(harness: &TestHarness);
pub async fn latest_observed_request(harness: &TestHarness) -> ObservedHttpRequest;
pub fn legal_single_entry_zip() -> Vec<u8>;
pub fn legal_two_entry_zip() -> Vec<u8>;
pub fn duplicate_entry_zip() -> Vec<u8>;
pub fn traversal_zip() -> Vec<u8>;
pub fn archive_key_collision_zip() -> Vec<u8>;
```

`start_harness` uses a `ScriptedAddResponder` backed by `Arc<Mutex<VecDeque<AddReply>>>` so successive `/api/v0/add` calls deterministically return archive, part/root, entry success, or one injected 500. A `ScriptedCatResponder` selects bytes by the `arg` query parameter. Mount generic successful `/api/v0/pin/add` and `/api/v0/pin/rm` mocks. After migrations and bucket creation, build the real service exactly as production does:

```rust
let s3_impl = S3Impl::new(state.clone());
let mut builder = S3ServiceBuilder::new(s3_impl);
builder.set_auth(GatewayAuth::new(state.clone()));
builder.set_route(DecompressZipRoute::new(state.clone()));
let s3_service = HandleError::new(builder.build(), handle_s3_error);
let app = axum::Router::new()
    .fallback_service(s3_service)
    .layer(axum::middleware::from_fn(
        ipfs_s3_gateway::s3::http::bridge_chunked_content_length,
    ))
    .layer(axum::middleware::from_fn_with_state(
        observed_http.clone(),
        observe_request,
    ));
```

Implement the observer without polling or replacing the body:

```rust
async fn observe_request(
    axum::extract::State(observed): axum::extract::State<
        std::sync::Arc<tokio::sync::Mutex<Vec<ObservedHttpRequest>>>,
    >,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    observed.lock().await.push(ObservedHttpRequest {
        method: request.method().clone(),
        uri: request.uri().clone(),
        headers: request.headers().clone(),
    });
    next.run(request).await
}
```

Register the bridge outside the fallback service in both environments. Production uses `Router::new().route("/health", get(health_check)).fallback_service(s3_service).layer(axum::middleware::from_fn(crate::s3::http::bridge_chunked_content_length))`; the test harness uses the equivalent fallback-service layer shown above. The wire observer remains outside the bridge, so it records the client's `Transfer-Encoding: chunked` and missing `Content-Length` before the bridge supplies the internal header for s3s. Bind `127.0.0.1:0`, spawn `axum::serve`, and return the endpoint plus `Arc<AppState>` so tests can assert DB visibility and prove the chunked/no-`Content-Length` wire shape without buffering payloads. `assert_pin_calls` reads `received_requests()`, filters the supplied `/api/v0/pin/add` or `/api/v0/pin/rm` path, extracts query `arg`, and enforces required/forbidden counts. Update every existing tuple-style `harness()` caller to hold a `TestHarness`; existing rust-s3 tests create their bucket from `harness.endpoint` and `harness.bucket`. Use `KuboScript::repeated_add("QmTestCid", expected_add_calls, { "QmTestCid" -> b"hello world" })` for those regressions. All custom-query and multipart acceptance requests use Step 2's signers.

- [ ] **Step 2: Implement deterministic header and presigned-query SigV4 helpers**

Extend `tests/support/sigv4.rs` with:

```rust
pub async fn send_sigv4(
    method: reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    extra_headers: http::HeaderMap,
    secret_key: &str,
) -> reqwest::Response;

pub async fn send_sigv4_chunked_http1(
    method: reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    query: &[(&str, &str)],
    chunks: Vec<bytes::Bytes>,
    extra_headers: http::HeaderMap,
    secret_key: &str,
) -> reqwest::Response;

pub fn presign_sigv4_query(
    method: &reqwest::Method,
    endpoint: &str,
    bucket: &str,
    key: &str,
    custom_query: &[(&str, &str)],
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
    expires: u32,
    now: chrono::DateTime<chrono::Utc>,
) -> String;
```

Implement the shared RFC 3986 encoder, canonical URI builder, HMAC key derivation, and canonical query sorter once. Header Authorization signing remains:

1. Percent-encode each UTF-8 byte except `[A-Za-z0-9-_.~]`; preserve `/` only between encoded key segments, never inside query names or values. Thus the tuple `("decompress-zip", "prefix/nested/")` produces `decompress-zip=prefix%2Fnested%2F`. Sort encoded query `(name,value)` pairs lexicographically and use the same canonical query in both URL and canonical request.
2. Set `host`, `x-amz-date` from `chrono::Utc::now().format("%Y%m%dT%H%M%SZ")`, and `x-amz-content-sha256` to lowercase SHA-256 of the exact body. Lowercase every extra header name, trim/collapse ASCII whitespace in values, sort with `BTreeMap`, and include every header in `canonical_headers` and `signed_headers`.
3. Build `canonical_request = METHOD + "\n" + canonical_uri + "\n" + canonical_query + "\n" + canonical_headers + "\n" + signed_headers + "\n" + payload_hash`.
4. Build scope `{date}/us-east-1/s3/aws4_request`; derive `kDate`, `kRegion`, `kService`, and `kSigning` using `Hmac<Sha256>`; sign the string-to-sign and set `Authorization: AWS4-HMAC-SHA256 Credential=test/{scope}, SignedHeaders={...}, Signature={...}`.
5. Send with `reqwest::Client` to the real endpoint. `secret_key="test"` creates a valid request; `secret_key="wrong"` creates a structurally valid but incorrect signature.

`send_sigv4_chunked_http1` computes both the SHA-256 and the checked total byte length across every supplied chunk. Before canonicalization it removes any `Content-Length`, inserts `x-amz-decoded-content-length` with that total, and then calls the canonicalizer/signer, so the decoded-length header is present in `SignedHeaders`. It sends `reqwest::Body::wrap_stream(futures_util::stream::iter(...))` with `reqwest::Version::HTTP_11`. Because the body stream has no exact size hint, reqwest/hyper emits `Transfer-Encoding: chunked`; it does not send `Content-Length`. The real server observer must assert that wire header and the missing wire `Content-Length`.

`presign_sigv4_query` has a separate canonical-request contract:

1. Start the query vector with every `custom_query` tuple. Before any signature is computed, add `X-Amz-Algorithm=AWS4-HMAC-SHA256`, `X-Amz-Credential={access_key}/{date}/us-east-1/s3/aws4_request`, `X-Amz-Date={timestamp}`, `X-Amz-Expires={expires}`, and `X-Amz-SignedHeaders=host`. If `session_token` is `Some`, also add `X-Amz-Security-Token`; no new temporary test credential is required.
2. RFC 3986 encode every name/value byte independently with only `[A-Za-z0-9-_.~]` unescaped, sort by encoded name then encoded value, preserve duplicate tuples, and join as the canonical query. Custom tuples therefore already participate in the signed bytes; none may be appended later except the signature itself.
3. Use `canonical_headers = "host:{authority}\n"`, `signed_headers = "host"`, and the exact canonical payload hash string `UNSIGNED-PAYLOAD`. Build and HMAC the normal SigV4 string-to-sign for `{date}/us-east-1/s3/aws4_request`.
4. Append only `X-Amz-Signature={lowercase_hex_signature}` to the already encoded canonical query and return the final path-style URL. The helper must not call the header signer and must not add an `Authorization` header.

Add `presign_sigv4_query_includes_custom_query_before_signature`, a deterministic unit test with a fixed `now` that parses the returned URL and proves the unsigned custom tuples plus Algorithm/Credential/Date/Expires/SignedHeaders are present before `X-Amz-Signature`, that slash-containing values are `%2F` encoded, and that changing a custom tuple changes the signature. This test validates producer construction; Step 4 supplies the authoritative real-service accept/reject proof.

Add `test_sigv4_valid_request_reaches_decompress_route` and `test_sigv4_wrong_signature_is_rejected_before_kubo`: the first performs a legal signed decompression PUT and expects 200; the second signs the same request with `wrong`, expects 403 plus `SignatureDoesNotMatch`, asserts no archive DB row, and calls `assert_no_kubo_calls`.

Add `test_sigv4_query_tuple_decodes_prefix_once` using `legal_single_entry_zip()` whose sole entry is `file.txt`. It must call the signer with the unencoded tuple and prove both the wire form and DB form:

```rust
let query = [("decompress-zip", "prefix/nested/")];
let response = send_sigv4(
    reqwest::Method::PUT,
    &harness.endpoint,
    &harness.bucket,
    "archive.zip",
    &query,
    b"archive bytes".to_vec(),
    http::HeaderMap::new(),
    "test",
).await;
assert_eq!(response.status(), http::StatusCode::OK);
assert!(response.url().as_str().contains("decompress-zip=prefix%2Fnested%2F"));
ipfs_s3_gateway::store::object::get_latest(
    harness.state.store.db(), &harness.bucket, "prefix/nested/file.txt",
).await.unwrap();
assert!(ipfs_s3_gateway::store::object::get_latest(
    harness.state.store.db(), &harness.bucket, "prefix%2Fnested%2Ffile.txt",
).await.is_err());
```

This test is the real HTTP producer/consumer proof for Task 5's shared decoder: the signer owns encoding, while route and Multipart consumers receive decoded values exactly once.

- [ ] **Step 3: Add signed multipart helpers that always cross HTTP**

Add these helpers to `tests/support/decompress.rs`; each calls `send_sigv4` against `TestHarness.endpoint`:

```rust
pub async fn create_multipart(h: &TestHarness, key: &str, options: &[(&str, &str)]) -> String;
pub async fn upload_part(h: &TestHarness, key: &str, upload_id: &str, part_number: i32, body: Vec<u8>) -> String;
pub async fn complete_multipart(h: &TestHarness, key: &str, upload_id: &str, parts: &[(i32, String)]) -> reqwest::Response;
pub async fn complete_multipart_xml(h: &TestHarness, key: &str, upload_id: &str, xml: String) -> reqwest::Response;
pub async fn abort_multipart(h: &TestHarness, key: &str, upload_id: &str) -> reqwest::Response;
```

`create_multipart` sends `POST` with query `uploads=` plus the supplied options and parses `<UploadId>` from the XML response using quick-xml. `upload_part` sends `PUT ?partNumber=N&uploadId=...` and returns the unquoted ETag header. `complete_multipart` emits its simple PartNumber/ETag control XML with one `<Part>` per tuple in ascending order, then delegates to `complete_multipart_xml`; that raw-body helper owns the one signed `POST ?uploadId=...` path and enables standard legal checksum fields without duplicating SigV4 signing:

```rust
let mut xml = String::from("<CompleteMultipartUpload>");
for (number, etag) in parts {
    xml.push_str(&format!(
        "<Part><PartNumber>{number}</PartNumber><ETag>\"{}\"</ETag></Part>",
        quick_xml::escape::escape(etag),
    ));
}
xml.push_str("</CompleteMultipartUpload>");
```

`abort_multipart` sends `DELETE ?uploadId=...`. No helper bypasses SigV4, axum, s3s routing, DTO parsing, or custom route matching.

- [ ] **Step 4: Write the failing PutObject/auth/failure acceptance tests**

Add these exact tests to `tests/integration.rs`:

- `test_presigned_put_signs_custom_query_and_lists_gets_objects`: call `presign_sigv4_query(PUT, ..., [("decompress-zip", "prefix/"), ("decompress-zip-result", "true")], "test", "test", None, 900, Utc::now())`, then issue `reqwest::Client::put(url).body(archive_bytes).send()` with no `Authorization` header. Expect 200 and `DecompressZipResult`; use the existing real signed/rust-s3 client to run ListObjectsV2 and GetObject, require the listing to contain both `archive.zip` and `prefix/file.txt`, and compare both GET bodies with the scripted archive/entry bytes. Inspect the Kubo log to prove archive and entry add/pin/cat were exercised.
- `test_presigned_space_target_raw_plus_rewrite_is_semantically_stable`: sign the custom target `reports Q3/`, whose presigned wire tuple is `decompress-zip=reports%20Q3%2F`; rewrite only that `%20` to raw `+` before sending the real TCP PUT. s3s authentication must still accept the request, and the shared business decoder must preserve the same form semantic target. Require a 200 response, the observed inbound URI to contain `decompress-zip=reports+Q3%2F`, a latest row at `reports Q3/file.txt`, and no latest or listed key at `reports+Q3/file.txt`.
- `test_presigned_put_tampered_decompress_query_is_rejected_without_mutation`: on a fresh harness, generate the same valid URL, then append `&decompress-zip=other%2F` after `X-Amz-Signature`. This duplicate would change the route's final-occurrence target if authentication allowed it. Send the PUT and require 403 plus `SignatureDoesNotMatch`; assert no latest archive, `prefix/file.txt`, or `other/file.txt` row, no upload/part row, and zero Kubo requests. Do not send the untampered URL in this test, so the no-mutation assertion has a clean baseline.
- `test_put_decompress_zip_signed_default_result`: Kubo add script `QmArchive,QmEntry1,QmEntry2`; cat `QmArchive -> legal_two_entry_zip`; expect 200, ETag `QmArchive`, `DecompressZipResult`, `ExtractedCount=2`, archive plus both entry latest rows.
- `test_put_duplicate_entry_key_last_wins`: `duplicate_entry_zip()` contains two ordered Stored `duplicate.txt` entries with distinct `first duplicate bytes` and `second duplicate bytes`. Send a real signed `PUT /bucket/archive.zip?decompress-zip=prefix/`; script archive/entry adds in exact FIFO order as `QmArchive,QmFirstDuplicate,QmSecondDuplicate`, map `QmArchive` to the ZIP and each entry CID to its distinct bytes. Require 200, latest `prefix/duplicate.txt`.cid `QmSecondDuplicate`, and signed GET bytes `second duplicate bytes`. Require one `/pin/add` for archive and both entry CIDs and zero `/pin/rm` for all three; the scripted FIFO add sequence proves both duplicate entries were staged.
- `test_sigv4_query_tuple_decodes_prefix_once`: the Step 2 `%2F` wire assertion plus exact decoded/not-encoded DB-key assertions.
- `test_put_decompress_zip_signed_result_false`: same behavior with `decompress-zip-result=false`; expect 200, ETag, empty body, and all three DB rows.
- `test_put_decompress_zip_traversal_hides_db_and_keeps_archive_pin`: add `QmArchive`; cat traversal fixture; expect 400 `InvalidParameterValue`; archive, `escape.txt`, and prefix listings have no latest rows; require `pin/add(QmArchive)` and forbid `pin/rm(QmArchive)`.
- `test_put_decompress_zip_archive_key_collision_is_global_reject`: send a real signed `PUT /bucket/archive.zip?decompress-zip=` using `archive_key_collision_zip()` whose only successful entry name is `archive.zip`; script `/add` as `QmArchive,QmCollisionEntry` and `/cat?arg=QmArchive` as that fixture. A 200 response is forbidden. Require 400, code `InvalidParameterValue`, and exact message `zip entry collides with archive key: archive.zip`. The identical archive/entry DB key has no latest row and the bucket object list is empty, proving no other entry was published. Require `/pin/add` for both successful CIDs and zero `/pin/rm` for both.
- `test_put_decompress_zip_one_entry_kubo_failure_is_partial`: add script `QmArchive, 500 entry failure, QmEntry2`; expect 200, `FailedCount=1`, `EntryUploadFailed`, archive and second entry visible, first entry absent; signed GETs for archive and second entry return their scripted bytes, and neither successful CID appears in `/pin/rm`.
- `test_put_decompress_zip_one_entry_publish_failure_keeps_pins`: install a SQLite trigger that rejects the first target entry insert only, add `QmArchive,QmEntry1,QmEntry2`, expect 200 with one `EntryPublishFailed`, archive and second entry visible/readable by signed GET, failed first entry absent, and forbid all three CIDs under `/pin/rm` because the failed entry CID may be shared.
- `test_put_decompress_zip_rejects_sse_s3`: signed decompression PUT with `x-amz-server-side-encryption: AES256`; expect 400 and zero Kubo calls.
- `test_put_decompress_zip_rejects_sse_c`: signed decompression PUT with `x-amz-server-side-encryption-customer-algorithm: AES256`, base64 `x-amz-server-side-encryption-customer-key`, and base64-MD5 `x-amz-server-side-encryption-customer-key-md5`; expect 400 and zero Kubo calls.
- `test_sigv4_wrong_signature_is_rejected_before_kubo`: the Step 2 negative-auth assertions.

Use `store::object::get_latest` and `store::object::list` through `harness.state.store.db()` for DB assertions, signed GET requests for successful-object accessibility, and `received_requests()` for Kubo pin assertions; HTTP status/body alone is insufficient evidence.

These presigned-query cases are true SigV4 authentication, not header-signer aliases: successful requests have `X-Amz-*` query fields and no `Authorization` header, the raw-plus case retains its signed semantic target, and the tamper test modifies decompression semantics only after signing. Keep the existing valid/wrong header Authorization tests to cover the second authentication surface independently.

Implement the raw-plus semantic-stability case as this separate integration test, rather than folding it into a coverage table or a unit-level URI parser assertion:

```rust
#[tokio::test]
async fn test_presigned_space_target_raw_plus_rewrite_is_semantically_stable() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let signed_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "reports Q3/")],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    assert!(signed_url.contains("decompress-zip=reports%20Q3%2F"));
    let rewritten_url = signed_url.replacen(
        "decompress-zip=reports%20Q3%2F",
        "decompress-zip=reports+Q3%2F",
        1,
    );
    assert_ne!(rewritten_url, signed_url);

    let response = reqwest::Client::new()
        .put(rewritten_url)
        .body(archive)
        .send()
        .await
        .expect("rewritten presigned PUT");

    assert_eq!(response.status(), StatusCode::OK);
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("decompress-zip=reports+Q3%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "reports Q3/file.txt",
    )
    .await
    .expect("space-decoded entry DB row");
    assert_latest_absent(&harness, "reports+Q3/file.txt").await;
    assert!(
        !listed_db_keys(&harness)
            .await
            .iter()
            .any(|key| key == "reports+Q3/file.txt")
    );
}
```

- [ ] **Step 5: Write complete real-service multipart acceptance tests**

Add:

- `test_multipart_decompress_signed_default_result`: Create with `decompress-zip=prefix/`, UploadPart containing the legal ZIP, Complete with returned ETag; script add `QmPart,QmRoot,QmEntry1,QmEntry2`, cat `QmPart` and `QmRoot` to legal ZIP; expect `DecompressZipResult`, archive/entries visible, upload/parts gone, and zero `/pin/rm` for root, part, and entries.
- `test_multipart_duplicate_entry_key_last_wins`: use `duplicate_entry_zip()` and real signed Create (`decompress-zip=prefix/`) → UploadPart → Complete. Script exact add order `QmPart,QmRoot,QmFirstDuplicate,QmSecondDuplicate`; map both `QmPart` and `QmRoot` cats to the duplicate fixture and map each entry CID to its distinct bytes. Require successful Complete, latest `prefix/duplicate.txt`.cid `QmSecondDuplicate`, and signed GET bytes `second duplicate bytes`; require the upload and parts to be deleted, one `/pin/add` for part, root, and both entry CIDs, and zero `/pin/rm` for all four. This independently locks Complete's publication loop rather than inheriting PutObject coverage.
- `test_multipart_decompress_signed_result_false`: Create also with `decompress-zip-result=false`; run full Create/UploadPart/Complete; expect `CompleteMultipartUploadResult`, ETag `QmRoot`, no `DecompressZipResult`, and the same archive/entry publication.
- `test_complete_xml_content_length_over_limit_rejected_without_complete_mutation`: create a multipart upload and upload one part through the real signed helpers, then snapshot the upload model, part model, latest-object absence, and complete Kubo request log. Build a `4 * 1024 * 1024 + 1` byte body and send it through `send_sigv4`, whose `Vec<u8>` request emits `Content-Length`; require the observer to show a declared length greater than 4 MiB. Expect 400, error code `InvalidRequest`, and message `CompleteMultipartUpload XML exceeds 4 MiB`. Require upload and part models to equal the snapshots, no archive latest row, and the Kubo log to equal the pre-Complete snapshot—no root add/pin/cat occurs.
- `test_complete_xml_chunked_over_limit_rejected_without_complete_mutation`: this is the authoritative real-chain test for the decoded-length bridge. Perform the same HTTP Create/UploadPart setup and snapshots, split a `4 * 1024 * 1024 + 1` byte body into at least three `Bytes` chunks, and send with `send_sigv4_chunked_http1`. The outer observer must show HTTP/1.1 `Transfer-Encoding: chunked`, no wire `Content-Length`, and the signed `x-amz-decoded-content-length`. The bridge then gives s3s the internal length it requires, s3s completes its length/hash/EOF validation and enters the custom Complete route, and that route rejects 4 MiB + 1 with the fixed 400. Require the same code/message, identical upload/part/latest DB state, and no Kubo request after the baseline. This proves the route-level limit works through the chunked/no-wire-length chain rather than depending on a client `Content-Length`.
- `test_multipart_traversal_keeps_root_pin_and_retry_state`: upload traversal ZIP and Complete; expect 400, no archive/entry latest rows, root pin add with root forbidden under pin/rm, no part pin rm, and upload/part records still present for a client retry.
- `test_multipart_archive_key_collision_is_global_reject_and_retryable`: Create `archive.zip` with the exact empty-prefix tuple `decompress-zip=`, UploadPart with `archive_key_collision_zip()`, then Complete through real signed HTTP. Script `/add` as `QmPart,QmRoot,QmCollisionEntry`, and map both part/root cats needed by concat/extraction to the collision fixture. A 200 response is forbidden. Require 400, code `InvalidParameterValue`, and exact message `zip entry collides with archive key: archive.zip`; archive/entry latest is absent and the bucket object list is empty. Assert the upload row and part row still exist with the original part ETag so the same Complete can be retried. Require `/pin/add` for `QmPart`, `QmRoot`, and `QmCollisionEntry`, with zero `/pin/rm` for all three.
- `test_multipart_abort_signed_removes_rows_and_keeps_part_pin`: Create decompression upload, UploadPart, then Abort; expect 204, upload/part rows absent, `QmPart` forbidden under `/pin/rm`, no `/api/v0/cat`, and no archive/entry DB rows.
- `test_multipart_single_part_equal_root_remains_readable`: script both UploadPart and Complete root add as `QmPart`, Complete a standard upload, expect standard XML and latest object `cid=QmPart`, then signed GET the completed object and compare exact bytes; upload/parts rows are absent and `QmPart` has zero `/pin/rm`.
- `test_multipart_shared_part_cid_survives_replace_abort_and_complete`: first signed-PUT `shared.bin` so its latest row is `QmSharedPart`; then use three separate signed Multipart uploads. For the first, upload part 1 as `QmSharedPart`, replace it with `QmReplacement`, and GET `shared.bin`. For the second, upload `QmSharedPart`, Abort, and GET `shared.bin`. For the third, upload `QmSharedPart`, Complete to `QmRoot`, and GET `shared.bin`. Require each GET to return the original shared bytes and require zero `/pin/rm?arg=QmSharedPart` across all three operations.
- `test_upload_part_db_failure_keeps_new_pin_and_old_record`: signed-upload part 1 as `QmOldPart`, install a SQLite `BEFORE UPDATE` trigger on `multipart_parts`, signed-upload the same part number as `QmNewPart`, expect 500, assert the DB row remains `QmOldPart`, require `/pin/add(QmNewPart)`, and forbid both CIDs under `/pin/rm`.

These tests must use all four HTTP helpers from Step 3. Direct route or operation calls are forbidden in Task 8 even when a client crate lacks custom-query support.

- [ ] **Step 6: Add separate standard behavior regressions**

Keep existing `test_create_and_put_and_get_plain_object` and add:

- `test_standard_put_sse_s3_still_succeeds`: signed PUT without decompression query and `x-amz-server-side-encryption: AES256`; expect 200/SSE header and latest DB row `encrypted=true`, `key_wrap=Some`.
- `test_standard_put_sse_c_still_succeeds`: signed PUT without decompression query using a base64 32-byte key, `AES256`, and base64 MD5; expect 200 and latest DB row `encrypted=true`, `key_wrap=None`.
- `test_standard_multipart_signed_still_succeeds`: signed Create without decompression options, signed UploadPart, signed Complete; expect standard `CompleteMultipartUploadResult`, latest multipart archive, deleted upload/parts, readable completed bytes, and zero part/root `/pin/rm`.
- `test_standard_multipart_complete_accepts_weak_part_etag_and_checksums`: real signed Create without decompression options, UploadPart, then `complete_multipart_xml` with the actual part number, XML-escaped `W/"<actual ETag>"`, and all five legal s3s 0.14 checksum XML elements. Require 200 and standard `CompleteMultipartUploadResult`, latest `QmRoot`, deleted upload/parts, signed Get of the completed bytes, and zero part/root `/pin/rm`. This real SigV4/TCP regression proves operation use of the typed weak ETag's `.value()` while retaining checksum DTO fields; it does not require checksum-value validation because the existing Complete operation does not validate those values.

These are distinct tests so SSE-S3, SSE-C, standard PutObject, and standard Multipart failures are independently diagnosable.

- [ ] **Step 7: Run focused tests to verify they initially fail**

```powershell
cargo test test_sigv4_valid_request_reaches_decompress_route --test integration
cargo test test_sigv4_query_tuple_decodes_prefix_once --test integration
cargo test test_sigv4_wrong_signature_is_rejected_before_kubo --test integration
cargo test presign_sigv4_query_includes_custom_query_before_signature --test integration
cargo test test_presigned_put_signs_custom_query_and_lists_gets_objects --test integration
cargo test test_presigned_space_target_raw_plus_rewrite_is_semantically_stable --test integration
cargo test test_presigned_put_tampered_decompress_query_is_rejected_without_mutation --test integration
cargo test test_put_decompress_zip_traversal_hides_db_and_keeps_archive_pin --test integration
cargo test test_put_decompress_zip_archive_key_collision_is_global_reject --test integration
cargo test test_put_decompress_zip_one_entry_kubo_failure_is_partial --test integration
cargo test test_put_decompress_zip_one_entry_publish_failure_keeps_pins --test integration
cargo test test_put_duplicate_entry_key_last_wins --test integration
cargo test test_multipart_decompress_signed_default_result --test integration
cargo test test_multipart_duplicate_entry_key_last_wins --test integration
cargo test test_multipart_decompress_signed_result_false --test integration
cargo test test_complete_xml_content_length_over_limit_rejected_without_complete_mutation --test integration
cargo test test_complete_xml_chunked_over_limit_rejected_without_complete_mutation --test integration
cargo test test_multipart_traversal_keeps_root_pin_and_retry_state --test integration
cargo test test_multipart_archive_key_collision_is_global_reject_and_retryable --test integration
cargo test test_multipart_abort_signed_removes_rows_and_keeps_part_pin --test integration
cargo test test_multipart_single_part_equal_root_remains_readable --test integration
cargo test test_multipart_shared_part_cid_survives_replace_abort_and_complete --test integration
cargo test test_upload_part_db_failure_keeps_new_pin_and_old_record --test integration
cargo test test_standard_multipart_signed_still_succeeds --test integration
cargo test test_standard_multipart_complete_accepts_weak_part_etag_and_checksums --test integration
```

Expected: tests fail until the support modules, real route registration, signing, scripted Kubo responses, and all preceding product tasks are complete.

- [ ] **Step 8: Run full automated and real-surface verification**

```powershell
cargo check --bin ipfs-s3-gateway
cargo test --lib
cargo test --test integration
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: every command exits 0. The integration suite itself is the real-surface QA: it binds a real TCP listener and crosses reqwest → SigV4 → axum → s3s → route/handler → SQLite/wiremock Kubo.

Run spec/plan guard scans from the repository root:

```powershell
$docs = @(
    "docs/superpowers/specs/2026-07-07-decompress-zip-upload-design.md",
    "docs/superpowers/plans/2026-07-07-decompress-zip-upload.md"
)
$hits = rg -n "entry\.data_descriptor\(|reader\.trim_text\(|[I]f .*supports|i[f] .*exposes|[l]owest layer|T[B]D|TO[D]O" $docs
if ($LASTEXITCODE -eq 0) { $hits; throw "plan contains a blocked API or unresolved branch" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }

$invalidParameterValueMacro = rg -n -U 's3_error!\(\s*InvalidParameterValue' "docs/superpowers/plans/2026-07-07-decompress-zip-upload.md"
if ($LASTEXITCODE -eq 0) { $invalidParameterValueMacro; throw "plan product code uses the nonexistent InvalidParameterValue macro variant" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }

$blocked = @(
    ("delete_object_" + "if_id"),
    ("multipart_query_" + "pairs"),
    ("fn query_" + "pairs"),
    ("rollback_completed_" + "multipart_archive"),
    ("quick_xml::escape::" + "unescape"),
    ("cleanup_" + "staged_cids"),
    ("Qm" + "Writer"),
    ("Arc<tokio::sync::" + "Notify>"),
    ("pin_rm(&state.kubo, &completed." + "root_cid)"),
    ("pin_rm(&self.state.kubo, &completed." + "root_cid)"),
    ("pin_rm(&self.state.kubo, &archive." + "cid)"),
    ("pin_rm(&state.kubo, &stored." + "cid)"),
    ("pin_rm(&state.kubo, &old_part." + "cid)"),
    ("pin_rm(&state.kubo, &part." + "cid)"),
    ("pin_rm(&state.kubo, part_" + "cid)"),
    ("for cid in &completed." + "part_cids"),
    ("id: completed." + "object_id.clone()"),
    ("reconcile_" + "completed_upload"),
    ("unpins_only_" + "parts"),
    ("post-commit part " + "cleanup"),
    ("part " + "unpinned"),
    ("part pin " + "removal"),
    ("req.input." + "collect"),
    ("generate_" + "presigned_url"),
    ("multipart_uploads" + "_new"),
    ("DROP TABLE multipart_" + "uploads"),
    ("ALTER TABLE multipart_" + "uploads_new RENAME TO multipart_" + "uploads")
)
foreach ($term in $blocked) {
    rg -n -F -- $term $docs
    if ($LASTEXITCODE -eq 0) { throw "plan contains obsolete parser/finalize or unsafe CID cleanup text: $term" }
    if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }
}

$unsafeMultipart = rg -n "crate::kubo::pin::pin_rm\s*\(|\b(delete_part|insert_part)\s*\(" "src/s3/ops/multipart.rs" "src/store/multipart.rs"
if ($LASTEXITCODE -eq 0) { $unsafeMultipart; throw "multipart product path still contains real pin removal or old part CRUD" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }

$legacyCompleteEtag = rg -n -U 'value\.trim\(\)\.trim_matches\(|e_tag:\s*Some\(s3s::dto::ETag::Strong' "src/s3/route/decompress_zip.rs" $docs
if ($LASTEXITCODE -eq 0) { $legacyCompleteEtag; throw "raw Complete still contains legacy ETag normalization or forced Strong construction" }
if ($LASTEXITCODE -ne 1) { throw "rg failed with exit code $LASTEXITCODE" }
```

Expected: every negative `rg` invocation exits 1 (no matches), and the PowerShell block exits successfully. In particular, the current documents contain no unbounded Complete request-input collector, high-level presigner followed by custom-query append, migration text that rebuilds/drops/renames the `multipart_uploads` parent, or product-code use of s3s's nonexistent `InvalidParameterValue` macro variant. The final product-source guard matches only a real `crate::kubo::pin::pin_rm(...)` call or the removed `delete_part(...)`/`insert_part(...)` CRUD calls; assertion helpers such as `assert_pin_rm_calls` remain allowed.

- [ ] **Step 9: Final manual commit checkpoint**

After all verification passes, report the complete changed-file list and ask for explicit Git-write permission. The entire feature is one final commit with semantic message `feat: add streaming decompress zip uploads`; no per-task commit is created.

---

## Spec Coverage and Self-Review

**Spec coverage:**

| Spec requirement | Plan evidence |
|---|---|
| PutObject archive-first, streaming archive/entries | Tasks 3–5; Task 4 uses `StreamReader`, `with_tokio`, duplex, and no archive/entry collect |
| Stored, Deflate, descriptor safety | Task 4 observes local-header flags/method; three deterministic fixtures prove Stored/no descriptor accepted, Deflate+descriptor accepted, Stored+descriptor rejected before upload |
| Query/path/SSE rules | Tasks 2, 5, 6; `src/s3/query.rs` decodes both components once with raw `+` as space and `%2B` as a literal plus, maps `%FF` to `InvalidArgument`, and Task 8's `test_presigned_space_target_raw_plus_rewrite_is_semantically_stable` proves that a signed `%20` target rewritten to raw `+` remains `reports Q3/` after real s3s authentication, with no `reports+Q3/file.txt` publication; Task 8 also covers SSE-S3 and SSE-C rejection |
| Archive/entry key collision global reject | Task 5 produces and unit-tests `reject_archive_key_collision`, then calls it before Put publication; Task 7 consumes it before Multipart finalize; Task 8 uses empty-prefix real HTTP Put and Multipart fixtures to prove fixed 400 errors, zero latest publication, retained retry state for Multipart, successful add/pin, and zero pin/rm |
| Partial safe-entry failure | Task 4 drains after Kubo failure and continues; Task 8 proves 200 + Failures + archive/other entry visibility |
| Global reject visibility and conservative pin safety | Tasks 3–5, Task 7, and Task 8 assert DB absence while archive/root/entry CIDs are forbidden under `/pin/rm`; redundant pins are explicitly accepted because no exclusive lease exists |
| Duplicate final ZIP key last-wins publication | Task 8's `duplicate_entry_zip()` has two ordered Stored `duplicate.txt` entries with different bytes. Independent real signed PUT and Create/UploadPart/Complete tests script distinct first/second entry CIDs, require latest `prefix/duplicate.txt`.cid and signed GET bytes from the second CID, and retain every archive/part/root/entry pin with zero `/pin/rm` |
| Multipart create/default/result=false/abort | Tasks 1, 6–8; Task 8 crosses real Create/UploadPart/Complete/Abort HTTP requests |
| Migration reversibility and parent/FK identity | Task 1 uses two schema-builder adds and reverse-order single-option drops; a real SQLite init/seed/up/down test preserves every upload/part value and cascade behavior, while PostgreSQL builder assertions forbid replacement-parent SQL |
| Multipart part record and pin safety | Task 6 replaces delete/insert with composite-key `upsert_part`, proves replacement and DB failure are atomic, and forbids part removal on replacement, Complete, or Abort; Task 8 proves single-part `part_cid == root_cid`, ordinary-object sharing, DB failure, Abort, and standard Multipart over real signed HTTP |
| Multipart finalize failure consistency | Task 6 puts prior-latest update, attempt-row insert, and upload delete/cascade in one transaction; the SQLite trigger proves body rollback leaves the old latest, upload, parts, root pin, and part pins intact, while direct success and every outcome-unknown branch also retain all part pins |
| Per-attempt reconciliation | Task 6 preserves `upload.object_id` as `encryption_object_id`, generates a fresh `completion_attempt_id` per inner call, writes it to `LatestObjectRow.id`, and proves A `OutcomeUnknown`/not committed cannot adopt B's different-attempt winner row |
| Same-upload Complete race | Task 6 runs two full inners through identical `QmRoot` add+pin, proves distinct attempt IDs, blocks both at one Barrier immediately before finalize, releases them together, requires one `Ok`/one `Err`, winner-ID latest, loser row absent, upload/parts deleted, and zero root/part removals |
| Complete XML parsing on quick-xml 0.41 and s3s 0.14 ETag parity | Task 7 uses an explicit root/Part/field whitelist state machine, appends `Text.decode()` output and resolves field-only `GeneralRef(BytesRef)` predefined/numeric references, uses `ETag::parse_http_header` to retain typed strong/weak ETags, falls back to raw `Strong` only on `InvalidFormat`, maps `InvalidChar` to `MalformedXML`, accepts attribute-free `<ETag/>` as empty `Strong`, and maps wrong roots, unknown/nested elements, out-of-field references/text, duplicate/missing fields, mismatched ends, trailing unsupported content, and invalid references to `MalformedXML` |
| Complete XML 4 MiB hard limit | Task 7 iterates `BodyExt::frame`, checks `checked_add(data.remaining())` before extending, accepts exactly 4 MiB, maps frame errors, and ignores trailers; Task 8 proves both declared-length and HTTP/1.1 chunked/no-length overflows return the fixed 400 with unchanged DB/Kubo snapshots |
| Rust 2024 cat-stream lifetime | Task 5 changes `stream_cat` to `Result<impl Stream<Item = Result<Bytes, std::io::Error>> + use<>, Box<dyn Error + Send + Sync>>` without changing cat behavior; its focused cat test and binary check prove the response-owned stream reaches `extract_zip_stream`'s `'static` boundary |
| Chunked signed decoded-length bridge | Task 8 adds `bridge_chunked_content_length` outside the production and test fallback S3 services. The bridge neither authenticates nor parses wire chunks, while the outer observer and authoritative oversized-Complete test prove wire `Transfer-Encoding: chunked`, no wire `Content-Length`, a signed decoded-length header, s3s validation, custom-route entry, and unchanged DB/Kubo complete state after the route rejects the oversized XML |
| SigV4 and standard regressions | Task 8 independently tests valid/invalid header Authorization and presigned-query authentication, successful presigned Put/List/Get, post-sign custom-query semantic tampering, plain PutObject, standard Multipart including a raw Complete XML body with XML-escaped weak `W/"<actual ETag>"` plus all five legal s3s 0.14 per-part checksums, SSE-S3, and SSE-C |

**Placeholder scan:** Real-service tests never substitute direct route calls, there is no conditional library/API branch or unresolved marker, and the plan uses no deliberate panic stub or nonexistent `ZipEntry` descriptor accessor. The plan names the Rust 2024 `stream_cat + use<>` return type and both production/test registrations for `bridge_chunked_content_length`, including the outer wire observer. Guard scans reject the obsolete unbounded request-input collector, high-level presigner/custom-query-after-sign flow, and replacement-parent migration SQL. Failing-test steps compile-fail on genuinely missing symbols until their immediately following implementation steps add the complete interfaces.

**API and type consistency:** s3s 0.14 has no `InvalidParameterValue` macro variant. Task 1's `invalid_parameter_value` and Task 5's collision helper therefore use `S3ErrorCode::Custom("InvalidParameterValue".into())`, attach the intended message, set `StatusCode::BAD_REQUEST`, and return the constructed error. ZIP validation tests compare the custom code through `error.code().as_str()` or `err.code().as_str()`. SeaQuery 0.32.7 uses `ColumnDef::new_with_type(..., ColumnType::custom("BOOLEAN"))` for `decompress_zip_result`, rather than `.boolean()`, so the SQLite migration behavior and the PostgreSQL `BOOLEAN NOT NULL DEFAULT TRUE` builder SQL remain stable across backends. `LocalHeaderMeta`, `LocalHeaderObserver<R>`, `LocalHeaderProbe`, `StoredObject`, `SanitizedEntry`, `ExtractedEntry`, `ExtractFailure`, `DecompressZipResult`, `ExtractOutcome`, `stream_cat -> Result<impl Stream<Item = Result<Bytes, std::io::Error>> + use<>, Box<dyn Error + Send + Sync>>`, `reject_archive_key_collision(archive_key: &str, entries: &[ExtractedEntry]) -> S3Result<()>`, `LatestObjectRow`, `upsert_part`, `CommitCompletedUploadError`, `ReconciledCommitOutcome`, `CompletedUploadFinalizerStore`, `commit_completed_upload`, `reconcile_completion_attempt`, `CompletedMultipartArchive.encryption_object_id`, `CompletedMultipartArchive.completion_attempt_id`, `decoded_query_pairs`, `query_key_is_present`, `MAX_COMPLETE_MULTIPART_XML_BYTES`, `collect_complete_xml`, `DecompressZipRoute`, `bridge_chunked_content_length`, `TestHarness`, `ObservedHttpRequest`, `KuboScript`, `archive_key_collision_zip`, `send_sigv4`, `send_sigv4_chunked_http1`, `presign_sigv4_query`, and `test_complete_xml_chunked_over_limit_rejected_without_complete_mutation` have one spelling and one producer/consumer chain throughout the plan. The collision helper consumes only successful `ExtractOutcome.entries`; failures without staged CIDs cannot enter its slice.

**Ordering consistency:** Task 1 compiles both crate roots without a `zip` declaration; Task 2 creates a non-empty `src/zip/mod.rs` and only then adds `pub mod zip;`/`mod zip;` in the same step. Task 5 changes `stream_cat` before passing its response-owned stream to `extract_zip_stream`, then extracts → rejects archive-key collision → publishes Put archive/entries. Task 7 completes/pins root → extracts → rejects collision → finalizes archive/upload transaction → publishes entries. Task 8 registers `bridge_chunked_content_length` outside both fallback S3 services, keeps the wire observer outside that bridge, and signs decoded length before canonicalization. Task 6 has one part-write sequence (`pin_add(new)` → atomic `upsert_part`) and one finalize sequence (inner generates attempt ID and pins root → one DB transaction updates old latest/inserts that attempt/deletes upload+cascaded parts → direct result or attempt-exact reconciliation). No sequence removes old/new part, root, archive, or entry pins; Abort validates then deletes upload only, and Task 7 has no duplicate cleanup path.

**Guard consistency:** Task 6 and final verification search only real `crate::kubo::pin::pin_rm(...)` product calls and obsolete `delete_part(...)`/`insert_part(...)` calls in the two Multipart product files. The regex does not match `assert_pin_rm_calls`, so test assertions remain required rather than being mistaken for unsafe cleanup.

**Memory consistency:** Only CompleteMultipartUpload control XML is buffered, and its collector stops at 4 MiB by checking every data frame before extending. The declared-length and chunked/no-length HTTP tests share this limit. The decoded-length bridge only changes an internal header after Hyper decodes framing; it does not buffer or parse the body. Local-header observation is capped at 30 bytes, entry transfer uses a 64 KiB duplex, and archive/entry payloads remain streaming with no full-body collector.

**Signing consistency:** Header Authorization canonical requests hash their exact body. The chunked helper first removes `Content-Length`, calculates the full chunk total, inserts `x-amz-decoded-content-length`, and signs that header before streaming HTTP/1.1 chunks. The bridge is not an authenticator, and s3s continues to validate actual length, EOF, and payload hash. Presigned query canonical requests use `UNSIGNED-PAYLOAD` and include every custom tuple plus Algorithm/Credential/Date/Expires/SignedHeaders (and optional token) before HMAC. Only `X-Amz-Signature` is appended afterward. `test_presigned_space_target_raw_plus_rewrite_is_semantically_stable` proves s3s accepts the raw-plus equivalent and the business decoder writes the space key only; the semantic-tamper test requires authentication failure before mutation.

**Migration consistency:** Up and down each execute one SeaQuery alter option per statement. The down path drops only `decompress_zip_result` then `decompress_zip_target`; it preserves the original parent table identity, existing upload/part rows, and the initial migration's `multipart_parts.upload_id ON DELETE CASCADE` relation on SQLite and emits PostgreSQL-compatible alter SQL.

**Commit guard:** Per-task checkpoints are manual progress markers. The implementation has one final commit only, and no Git write runs without explicit user permission.

**Plan-critic receipt:** waiting for receipt; the orchestrator must submit this saved current revision because any edit invalidates an older receipt.
