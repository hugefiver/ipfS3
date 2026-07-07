# Decompress-Zip Upload Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the non-standard `decompress-zip` upload extension so PutObject and Multipart uploads store the archive and stream-extract safe zip entries into the same bucket prefix.

**Architecture:** The implementation keeps standard S3 behavior on the existing `s3s` DTO path and adds a custom `S3Route` only where raw query/body handling is required. PutObject uses an archive-first flow: stream archive bytes into Kubo, stream the stored CID back through `async_zip`, stage extracted entry CIDs, then publish DB records only after no global reject. Multipart Create records decompression metadata, while Complete is routed through a raw-body complete handler that delegates to a refactored complete-inner function; decompress uploads defer archive DB publication, upload deletion, and part unpin until extraction has no global reject.

**Tech Stack:** Rust 2024, axum 0.8, s3s 0.14, SeaORM 1, Kubo RPC via reqwest 0.13, `async_zip 0.0.18`, `tokio-util::io::ReaderStream`, `tokio-util::compat`, `futures-io 0.3`, `quick-xml 0.41`, wiremock, rust-s3.

**Global Constraints:**
- Preserve existing S3 behavior when the `decompress-zip` query parameter is absent.
- ETag remains the IPFS CID for archives and extracted entries.
- Do not collect a whole archive or a whole zip entry into memory.
- `decompress-zip` requests with any SSE-S3 or SSE-C header return `400 InvalidArgument`; normal non-decompress SSE behavior remains unchanged.
- Unsafe zip paths and unsupported stream-unsafe zip entries are global rejects: return 400 and do not publish this request's new archive or entry DB records.
- Single safe-entry Kubo/pin/DB failures are partial failures recorded in `DecompressZipResult`; successful archive and other entries remain published.
- Use `async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }` and enable `tokio-util`'s `compat` feature; reject formats not enabled by these features.
- Use PowerShell-compatible commands in verification notes.
- Git write commands are manual checkpoints only. Do not run `git commit`, `git push`, `git tag`, or other git write commands unless the user explicitly approves them in the active conversation.

**Spec:** `docs/superpowers/specs/2026-07-07-decompress-zip-upload-design.md`

---

## File Structure

- Modify `Cargo.toml` to add `async_zip` with `tokio` and `deflate` features and enable `tokio-util`'s `compat` feature.
- Modify `src/lib.rs` to export `pub mod zip;` and modify `src/main.rs` to declare `mod zip;` for the binary crate module tree.
- Modify `src/main.rs` and `tests/integration.rs` to register `DecompressZipRoute` with `S3ServiceBuilder`.
- Modify `src/error.rs` to add zip/query/path variants and map them to 400-class S3 errors.
- Create `src/zip/mod.rs` for the zip module boundary.
- Create `src/zip/sanitize.rs` for target prefix and entry-name validation.
- Create `src/zip/response.rs` for `DecompressZipResult` and standard Complete XML serialization helpers.
- Create `src/zip/extract.rs` for `async_zip` streaming extraction, duplex-to-Kubo entry uploads, and cleanup of staged CIDs.
- Create `src/s3/route/mod.rs` and `src/s3/route/decompress_zip.rs` for custom PutObject and raw CompleteMultipartUpload routing.
- Modify `src/s3/mod.rs` to export `pub mod route;`.
- Modify `src/s3/ops/object.rs` to expose plaintext Kubo add/pin and DB publish helpers without changing encrypted standard PutObject behavior.
- Modify `src/s3/ops/multipart.rs` to persist create-time decompression metadata and refactor Complete into a reusable inner function returning archive metadata.
- Modify `src/store/entities/multipart_upload.rs`, `src/store/multipart.rs`, `src/store/migrations/mod.rs`, and `src/store/mod.rs` for decompression metadata columns and migration registration.
- Create `src/store/migrations/m20260707_000001_decompress_zip.rs` for the new multipart upload columns.
- Modify `tests/integration.rs` for route registration and signed/raw decompression upload coverage.

---

### Task 1: Foundation schema, errors, and module wiring

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/lib.rs`
- Modify: `src/main.rs`
- Modify: `src/error.rs`
- Modify: `src/store/entities/multipart_upload.rs`
- Modify: `src/store/multipart.rs`
- Modify: `src/store/migrations/mod.rs`
- Modify: `src/store/mod.rs`
- Create: `src/store/migrations/m20260707_000001_decompress_zip.rs`

**Interfaces:**
- Consumes: existing `store::multipart::create_upload`, `store::run_migrations`, `AppError -> S3Error` conversion.
- Produces: `AppError::{InvalidZipParameter, InvalidZipEntry, ZipSlip, UnsupportedZipEntry, ZipArchiveRejected}`, multipart upload fields `decompress_zip_target: Option<String>` and `decompress_zip_result: bool`, and `create_upload(..., decompress_zip_target: Option<&str>, decompress_zip_result: bool)`.

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
        assert_eq!(err.code(), "InvalidParameterValue");

        let err: S3Error = AppError::InvalidZipParameter("bad prefix".to_string()).into();
        assert_eq!(err.code(), "InvalidArgument");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test store::tests::test_multipart_upload_decompress_columns_exist --lib
cargo test store::multipart::tests::create_upload_persists_decompress_metadata --lib
cargo test error::tests::zip_validation_errors_map_to_client_errors --lib
```

Expected: the first test fails because the columns do not exist, the second fails because `create_upload` does not accept the new parameters or model fields, and the third fails because the zip error variants do not exist.

- [ ] **Step 3: Add dependency, errors, migration, entity fields, and store parameters**

Add `async_zip` and `futures-io` to `Cargo.toml`, and replace the existing `tokio-util` dependency line so it enables both `io` and `compat`:

```toml
async_zip = { version = "0.0.18", features = ["tokio", "deflate"] }
tokio-util = { version = "0.7", features = ["io", "compat"] }
futures-io = "0.3"
```

Add `pub mod zip;` to `src/lib.rs`. Add `mod zip;` to `src/main.rs` because the binary crate has its own module tree and cannot see `src/lib.rs` exports through `crate::zip`.

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

Update `From<AppError> for S3Error` with explicit client-error mappings before the fallback arm:

```rust
AppError::InvalidZipParameter(_) => s3_error!(InvalidArgument, "{}", e),
AppError::InvalidZipEntry(_)
| AppError::ZipSlip(_)
| AppError::UnsupportedZipEntry(_)
| AppError::ZipArchiveRejected(_) => s3_error!(InvalidParameterValue, "{}", e),
```

Create `src/store/migrations/m20260707_000001_decompress_zip.rs`:

```rust
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"ALTER TABLE multipart_uploads
                   ADD COLUMN decompress_zip_target TEXT"#,
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r#"ALTER TABLE multipart_uploads
                   ADD COLUMN decompress_zip_result BOOLEAN NOT NULL DEFAULT TRUE"#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE multipart_uploads_new (
                    upload_id TEXT PRIMARY KEY NOT NULL,
                    object_id TEXT NOT NULL,
                    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                    key TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    encryption_mode TEXT NOT NULL DEFAULT 'none',
                    key_wrap TEXT,
                    content_type TEXT,
                    metadata TEXT
                )"#,
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(
                r#"INSERT INTO multipart_uploads_new
                   (upload_id, object_id, bucket, key, created_at, encryption_mode, key_wrap, content_type, metadata)
                   SELECT upload_id, object_id, bucket, key, created_at, encryption_mode, key_wrap, content_type, metadata
                   FROM multipart_uploads"#,
            )
            .await?;

        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE multipart_uploads"#)
            .await?;

        manager
            .get_connection()
            .execute_unprepared(r#"ALTER TABLE multipart_uploads_new RENAME TO multipart_uploads"#)
            .await?;

        Ok(())
    }
}
```

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
cargo test store::multipart::tests::create_upload_persists_decompress_metadata --lib
cargo test error::tests::zip_validation_errors_map_to_client_errors --lib
cargo test store --lib
```

Expected: all commands exit 0.

- [ ] **Step 5: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit only these files with message `feat: add decompress zip schema foundation`.

---

### Task 2: Zip path sanitizer and XML response primitives

**Files:**
- Create: `src/zip/mod.rs`
- Create: `src/zip/sanitize.rs`
- Create: `src/zip/response.rs`

**Interfaces:**
- Consumes: `AppError::{InvalidZipParameter, InvalidZipEntry, ZipSlip}` from Task 1.
- Produces: `normalize_target_prefix(prefix: &str) -> AppResult<String>`, `sanitize_entry(name: &str, target_prefix: &str) -> AppResult<SanitizedEntry>`, `DecompressZipResult`, `ExtractedEntry`, `ExtractFailure`, `decompress_result_xml(&DecompressZipResult) -> String`, and `complete_multipart_result_xml(bucket: &str, key: &str, etag: &str) -> String`.

- [ ] **Step 1: Write failing sanitizer and XML tests**

Create `src/zip/mod.rs`:

```rust
pub mod response;
pub mod sanitize;
```

Create `src/zip/sanitize.rs` with tests first:

```rust
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanitizedEntry {
    File { key: String },
    Directory,
}

pub fn normalize_target_prefix(prefix: &str) -> AppResult<String> {
    unreachable!("implemented after failing tests")
}

pub fn sanitize_entry(name: &str, target_prefix: &str) -> AppResult<SanitizedEntry> {
    unreachable!("implemented after failing tests")
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

pub fn decompress_result_xml(result: &DecompressZipResult) -> String {
    unreachable!("implemented after failing tests")
}

pub fn complete_multipart_result_xml(bucket: &str, key: &str, etag: &str) -> String {
    unreachable!("implemented after failing tests")
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
```

Expected: all sanitizer and response tests pass.

- [ ] **Step 5: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit only these files with message `feat: add decompress zip primitives`.

---

### Task 3: Plain object Kubo staging and DB publish helpers

**Files:**
- Modify: `src/s3/ops/object.rs`

**Interfaces:**
- Consumes: existing `ByteCounter`, `kubo::add::stream_add`, `kubo::pin::{pin_add,pin_rm}`, and `store::object::upsert`.
- Produces: `StoredObject { cid: String, size: i64 }`, `add_plain_object_stream(...) -> S3Result<StoredObject>`, `publish_plain_object(...) -> S3Result<()>`, and `put_plain_object_stream(...) -> S3Result<StoredObject>`.

- [ ] **Step 1: Write failing helper tests**

Add tests to `src/s3/ops/object.rs` under the existing `#[cfg(test)]` module, or create the module if none exists near the bottom:

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

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test s3::ops::object::tests::add_plain_object_stream_counts_pins_and_returns_cid --lib
cargo test s3::ops::object::tests::publish_plain_object_writes_latest_metadata --lib
```

Expected: tests fail because the helper functions and `StoredObject` do not exist.

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
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    let (counter, count_handle) = ByteCounter::new();
    let counted = counter.wrap(stream);
    let cid = crate::kubo::add::stream_add(&state.kubo, counted, 1)
        .await
        .map_err(|e| s3s::s3_error!(InternalError, "kubo add: {e}"))?;

    if let Err(e) = crate::kubo::pin::pin_add(&state.kubo, &cid).await {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &cid).await;
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
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &stored.cid).await;
        return Err(e.into());
    }
    Ok(())
}

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
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    let stored = add_plain_object_stream(state, stream).await?;
    publish_plain_object(state, bucket, key, content_type, metadata, &stored, multipart).await?;
    Ok(stored)
}
```

Optionally update the `EncryptionMode::None` arm in `put_object` to call `put_plain_object_stream`; keep SSE-S3 and SSE-C branches unchanged. If this optional refactor is done, preserve the existing response and error behavior exactly.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test s3::ops::object::tests::add_plain_object_stream_counts_pins_and_returns_cid --lib
cargo test s3::ops::object::tests::publish_plain_object_writes_latest_metadata --lib
cargo test s3::ops::object --lib
```

Expected: helper tests pass and existing object operation tests still pass.

- [ ] **Step 5: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit only `src/s3/ops/object.rs` with message `refactor: expose plaintext object staging helpers`.

---

### Task 4: Streaming zip extractor with duplex Kubo uploads

**Files:**
- Modify: `src/zip/mod.rs`
- Create: `src/zip/extract.rs`

**Interfaces:**
- Consumes: `sanitize_entry`, `normalize_target_prefix`, `add_plain_object_stream`, `StoredObject`, and `ExtractedEntry`/`ExtractFailure` from previous tasks.
- Produces: `ExtractOutcome { entries, failures, staged_cids }`, `extract_zip_stream(state, target_prefix, stream) -> S3Result<ExtractOutcome>`, and `cleanup_staged_cids(state, cids) -> impl Future<Output = ()>`.

- [ ] **Step 1: Write failing extractor tests**

Update `src/zip/mod.rs`:

```rust
pub mod extract;
pub mod response;
pub mod sanitize;
```

Create `src/zip/extract.rs` with tests first. Use this legal fixture inside the test module:

```rust
const LEGAL_ZIP_B64: &str = "UEsDBBQAAAAIAAB751yGphA2BwAAAAUAAAALAAAAZm9vL2Jhci50eHTLSM3JyQcAUEsDBBQAAAAIAAB751wAAAAAAgAAAAAAAAAJAAAAZW1wdHkudHh0AwBQSwECFAAUAAAACAAAe+dchqYQNgcAAAAFAAAACwAAAAAAAAAAAAAAgAEAAAAAZm9vL2Jhci50eHRQSwECFAAUAAAACAAAe+dcAAAAAAIAAAAAAAAACQAAAAAAAAAAAAAAgAEwAAAAZW1wdHkudHh0UEsFBgAAAAACAAIAcAAAAFkAAAAAAA==";
const TRAVERSAL_ZIP_B64: &str = "UEsDBBQAAAAIAAB751z7OSuCBQAAAAMAAAANAAAALi4vZXNjYXBlLnR4dEtKTAEAUEsBAhQAFAAAAAgAAHvnXPs5K4IFAAAAAwAAAA0AAAAAAAAAAAAAAIABAAAAAC4uL2VzY2FwZS50eHRQSwUGAAAAAAEAAQA7AAAAMAAAAAAA";
const CORRUPT_SAFE_ENTRY_ZIP_B64: &str = "UEsDBBQAAAAIAG2G51yFEUoNDQAAAAsAAAAIAAAAc2FmZS50eHTMSM3JyVcozy/KSQEAUEsBAhQAFAAAAAgAbYbnXIURSg0NAAAACwAAAAgAAAAAAAAAAAAAAIABAAAAAHNhZmUudHh0UEsFBgAAAAABAAEANgAAADMAAAAAAA==";
```

Add these tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use futures_util::stream;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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

    #[tokio::test]
    async fn extracts_safe_entries_to_staged_cids() {
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmEntry\"]}"))
            .mount(&kubo)
            .await;

        let bytes = base64::engine::general_purpose::STANDARD.decode(LEGAL_ZIP_B64).unwrap();
        let state = test_state(kubo.uri()).await;
        let outcome = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(bytes))]),
        )
        .await
        .unwrap();

        assert_eq!(outcome.failures, Vec::<crate::zip::response::ExtractFailure>::new());
        assert_eq!(outcome.entries.len(), 2);
        assert_eq!(outcome.entries[0].key, "prefix/foo/bar.txt");
        assert_eq!(outcome.entries[1].key, "prefix/empty.txt");
        assert_eq!(outcome.staged_cids.len(), 2);
    }

    #[tokio::test]
    async fn traversal_entry_rejects_whole_archive() {
        let kubo = MockServer::start().await;
        let bytes = base64::engine::general_purpose::STANDARD.decode(TRAVERSAL_ZIP_B64).unwrap();
        let state = test_state(kubo.uri()).await;
        let err = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(bytes))]),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, s3s::S3Error { .. }));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[tokio::test]
    async fn corrupt_safe_entry_is_partial_failure_not_global_reject() {
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Hash\":\"QmCorrupt\",\"Size\":\"5\"}\n"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmCorrupt\"]}"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmCorrupt\"]}"))
            .mount(&kubo)
            .await;

        let bytes = base64::engine::general_purpose::STANDARD.decode(CORRUPT_SAFE_ENTRY_ZIP_B64).unwrap();
        let state = test_state(kubo.uri()).await;
        let outcome = extract_zip_stream(
            &state,
            "prefix/",
            stream::iter(vec![Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(bytes))]),
        )
        .await
        .unwrap();

        assert!(outcome.entries.is_empty(), "corrupt safe entry must not publish a staged entry");
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.failures[0].entry_name, "safe.txt");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test zip::extract::tests --lib
```

Expected: tests fail because `extract_zip_stream` and `ExtractOutcome` do not exist.

- [ ] **Step 3: Implement streaming extraction and cleanup**

Implement `src/zip/extract.rs` with these public types and functions:

```rust
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{Stream, TryFutureExt};
use s3s::S3Result;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::error::AppError;
use crate::s3::ops::object::add_plain_object_stream;
use crate::state::AppState;
use crate::zip::response::{ExtractFailure, ExtractedEntry};
use crate::zip::sanitize::{sanitize_entry, SanitizedEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractOutcome {
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
    pub staged_cids: Vec<String>,
}

pub async fn cleanup_staged_cids(state: &Arc<AppState>, cids: &[String]) {
    for cid in cids {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, cid).await;
    }
}
```

Use `StreamReader` plus `tokio_util::compat` to keep archive streaming. `async_zip 0.0.18` stream readers are based on the futures IO traits; do not pass its entry reader directly to `tokio::io::copy` without compatibility conversion:

```rust
pub async fn extract_zip_stream<S, E>(
    state: &Arc<AppState>,
    target_prefix: &str,
    stream: S,
) -> S3Result<ExtractOutcome>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    let io_stream = stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let reader = BufReader::new(StreamReader::new(io_stream)).compat();
    let mut zip = async_zip::base::read::stream::ZipFileReader::new(reader);

    let mut entries = Vec::new();
    let mut failures = Vec::new();
    let mut staged_cids = Vec::new();

    while let Some(mut entry_reader) = match zip.next_with_entry().await {
        Ok(next) => next,
        Err(e) => {
            cleanup_staged_cids(state, &staged_cids).await;
            return Err(s3s::s3_error!(InvalidParameterValue, "invalid zip archive: {e}"));
        }
    } {
        let entry = entry_reader.reader().entry().clone();
        let name = match entry.filename().as_str() {
            Ok(name) => name.to_string(),
            Err(_) => {
                cleanup_staged_cids(state, &staged_cids).await;
                return Err(AppError::InvalidZipEntry("entry name is not valid UTF-8".to_string()).into());
            }
        };

        if entry.compression() != async_zip::Compression::Deflate
            && entry.compression() != async_zip::Compression::Stored
        {
            cleanup_staged_cids(state, &staged_cids).await;
            return Err(AppError::UnsupportedZipEntry(format!("{name}: unsupported compression")).into());
        }
        if entry.compression() == async_zip::Compression::Stored && entry.data_descriptor() {
            cleanup_staged_cids(state, &staged_cids).await;
            return Err(AppError::UnsupportedZipEntry(format!("{name}: stored entry with data descriptor")).into());
        }

        let sanitized = match sanitize_entry(&name, target_prefix) {
            Ok(sanitized) => sanitized,
            Err(err) => {
                cleanup_staged_cids(state, &staged_cids).await;
                return Err(err.into());
            }
        };

        match sanitized {
            SanitizedEntry::Directory => {
                zip = match entry_reader.skip().await {
                    Ok(next) => next,
                    Err(e) => {
                        failures.push(ExtractFailure {
                            entry_name: name,
                            code: "EntryReadFailed".to_string(),
                            message: format!("zip directory read failed: {e}"),
                        });
                        return Ok(ExtractOutcome { entries, failures, staged_cids });
                    }
                };
            }
            SanitizedEntry::File { key } => {
                match upload_entry_to_kubo(state, entry_reader.reader_mut()).await {
                    Ok(stored) => {
                        match entry_reader.done().await {
                            Ok(next) => {
                                staged_cids.push(stored.cid.clone());
                                entries.push(ExtractedEntry { key, cid: stored.cid, size: stored.size });
                                zip = next;
                            }
                            Err(e) => {
                                let _ = crate::kubo::pin::pin_rm(&state.kubo, &stored.cid).await;
                                failures.push(ExtractFailure {
                                    entry_name: name,
                                    code: "EntryReadFailed".to_string(),
                                    message: format!("zip entry read failed: {e}"),
                                });
                                return Ok(ExtractOutcome { entries, failures, staged_cids });
                            }
                        }
                    }
                    Err(err) => {
                        failures.push(ExtractFailure {
                            entry_name: name.clone(),
                            code: "EntryUploadFailed".to_string(),
                            message: err.to_string(),
                        });
                        zip = match entry_reader.done().await {
                            Ok(next) => next,
                            Err(e) => {
                                failures.push(ExtractFailure {
                                    entry_name: name,
                                    code: "EntryReadFailed".to_string(),
                                    message: format!("zip entry read failed: {e}"),
                                });
                                return Ok(ExtractOutcome { entries, failures, staged_cids });
                            }
                        };
                    }
                }
            }
        }
    }

    Ok(ExtractOutcome { entries, failures, staged_cids })
}
```

Implement the duplex bridge so the `async_zip` entry reader is not moved into a `'static` stream. The entry reader implements futures IO, so convert it to Tokio IO with `compat()` before copying into the Tokio duplex writer:

```rust
async fn upload_entry_to_kubo<R>(
    state: &Arc<AppState>,
    reader: &mut R,
) -> S3Result<crate::s3::ops::object::StoredObject>
where
    R: futures_io::AsyncRead + Unpin + Send,
{
    let (duplex_reader, mut duplex_writer) = tokio::io::duplex(64 * 1024);
    let upload = add_plain_object_stream(state, ReaderStream::new(duplex_reader));
    let copy = async {
        let mut tokio_reader = reader.compat();
        tokio::io::copy(&mut tokio_reader, &mut duplex_writer).await?;
        duplex_writer.shutdown().await?;
        Ok::<(), std::io::Error>(())
    };

    let (stored, _) = futures_util::try_join!(upload, copy.map_err(|e| s3s::s3_error!(InternalError, "zip entry read: {e}")))?;
    Ok(stored)
}
```

If the actual `async_zip` API returns the reader state type from `next_with_entry()` differently than shown, keep the same control flow: read metadata, validate, copy entry reader into duplex writer, call `done()` or `skip()` before the next loop iteration.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test zip::extract::tests --lib
cargo test zip --lib
```

Expected: extractor, sanitizer, and response tests pass.

- [ ] **Step 5: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit `src/zip/mod.rs` and `src/zip/extract.rs` with message `feat: stream zip entries into kubo`.

---

### Task 5: PutObject custom route and archive-first publish flow

**Files:**
- Modify: `src/main.rs`
- Modify: `src/s3/mod.rs`
- Create: `src/s3/route/mod.rs`
- Create: `src/s3/route/decompress_zip.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: `add_plain_object_stream`, `publish_plain_object`, `extract_zip_stream`, `cleanup_staged_cids`, `normalize_target_prefix`, `decompress_result_xml`, and `DecompressZipResult`.
- Produces: `DecompressZipRoute::new(state: Arc<AppState>)`, path/query parsing helpers, PutObject `?decompress-zip` support, and route registration in app/test harness.

- [ ] **Step 1: Write failing route unit tests**

Create `src/s3/route/mod.rs`:

```rust
pub mod decompress_zip;
```

Update `src/s3/mod.rs`:

```rust
pub mod handler;
pub mod ops;
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

impl S3Route for DecompressZipRoute {
    fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut http::Extensions) -> bool {
        unreachable!("implemented after failing tests")
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        unreachable!("implemented after failing tests")
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
    async fn route_matches_only_decompress_put_and_complete_posts() {
        let route = dummy_route().await;
        let mut ext = http::Extensions::new();
        assert!(route.is_match(
            &Method::PUT,
            &"/bucket/archive.zip?decompress-zip=prefix/".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut ext,
        ));
        assert!(route.is_match(
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
    fn parse_path_and_query_decodes_bucket_key_and_result_flag() {
        let parsed = parse_decompress_put_uri(&"/bucket/folder%20a/archive.zip?decompress-zip=prefix/&decompress-zip-result=false".parse::<Uri>().unwrap()).unwrap();
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key, "folder a/archive.zip");
        assert_eq!(parsed.target_prefix, "prefix/");
        assert!(!parsed.return_result_xml);
    }
}
```

- [ ] **Step 2: Run route tests to verify they fail**

Run:

```powershell
cargo test s3::route::decompress_zip::tests --lib
```

Expected: tests fail because match and parse helpers are not implemented.

- [ ] **Step 3: Implement PutObject route parsing and response flow**

Implement query parsing helpers:

```rust
struct ParsedDecompressPut {
    bucket: String,
    key: String,
    target_prefix: String,
    return_result_xml: bool,
}

fn query_pairs(uri: &Uri) -> impl Iterator<Item = (&str, &str)> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| part.split_once('=').unwrap_or((part, "")))
}

fn has_query_key(uri: &Uri, name: &str) -> bool {
    query_pairs(uri).any(|(key, _)| key == name)
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
    for (name, value) in query_pairs(uri) {
        if name == "decompress-zip" {
            target = Some(value);
        } else if name == "decompress-zip-result" && value == "false" {
            return_result_xml = false;
        }
    }
    let target_prefix = crate::zip::sanitize::normalize_target_prefix(target.unwrap_or(""))?;
    Ok(ParsedDecompressPut { bucket, key, target_prefix, return_result_xml })
}

fn has_sse_header(headers: &HeaderMap) -> bool {
    headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-MD5")
}
```

Implement `is_match`:

```rust
fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut http::Extensions) -> bool {
    (*method == Method::PUT && has_query_key(uri, "decompress-zip"))
        || (*method == Method::POST && has_query_key(uri, "uploadId") && !has_query_key(uri, "uploads"))
}
```

Implement `call` PutObject branch:

```rust
async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    self.check_access(&mut req).await?;
    if req.method == Method::PUT {
        return self.call_put(req).await;
    }
    self.call_complete(req).await
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
            let _ = crate::kubo::pin::pin_rm(&self.state.kubo, &archive.cid).await;
            return Err(s3s::s3_error!(InternalError, "cat archive: {err}"));
        }
    };

    let outcome = match crate::zip::extract::extract_zip_stream(&self.state, &parsed.target_prefix, archive_stream).await {
        Ok(outcome) => outcome,
        Err(err) => {
            let _ = crate::kubo::pin::pin_rm(&self.state.kubo, &archive.cid).await;
            return Err(err);
        }
    };

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
        let _ = crate::kubo::pin::pin_rm(&self.state.kubo, &archive.cid).await;
        crate::zip::extract::cleanup_staged_cids(&self.state, &outcome.staged_cids).await;
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

Leave `call_complete` returning `unreachable!("implemented in Task 7")` until Task 7, but keep `is_match` complete matching so route match tests encode the final routing shape.

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
    assert_eq!(err.code(), "InvalidArgument");
}
```

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
cargo test s3::route::decompress_zip::tests --lib
cargo test test_create_and_put_and_get_plain_object --test integration
```

Expected: route tests pass and the existing standard PutObject integration test still passes, proving non-decompress PutObject remains on the standard path.

- [ ] **Step 6: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit route files and registration changes with message `feat: add decompress zip put route`.

---

### Task 6: Multipart create metadata and complete-inner refactor

**Files:**
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/store/multipart.rs` callers from Task 1

**Interfaces:**
- Consumes: `normalize_target_prefix`, `has_query_key`/query parsing shape from Task 5, existing Multipart validation logic.
- Produces: CreateMultipartUpload persistence of `decompress_zip_target` and `decompress_zip_result`, `CompletedMultipartArchive`, `complete_multipart_upload_inner(state, req) -> S3Result<CompletedMultipartArchive>`, `finalize_completed_multipart_archive(state, &CompletedMultipartArchive) -> S3Result<()>`, `rollback_completed_multipart_archive(state, &CompletedMultipartArchive) -> impl Future<Output = ()>`, and a standard wrapper preserving `S3Response<CompleteMultipartUploadOutput>`.

- [ ] **Step 1: Write failing multipart create and complete-refactor tests**

Add tests to `src/s3/ops/multipart.rs`:

```rust
#[tokio::test]
async fn create_multipart_upload_records_decompress_query() {
    let state = test_state_with_bucket("test-bucket").await;
    let mut req = multipart_create_request("test-bucket", "archive.zip");
    req.uri = "/test-bucket/archive.zip?uploads&decompress-zip=prefix/&decompress-zip-result=false".parse().unwrap();

    create_multipart_upload(&state, req).await.unwrap();
    let uploads = crate::store::entities::multipart_upload::Entity::find()
        .all(state.store.db())
        .await
        .unwrap();

    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].decompress_zip_target.as_deref(), Some("prefix/"));
    assert!(!uploads[0].decompress_zip_result);
}

#[tokio::test]
async fn create_multipart_upload_rejects_decompress_sse() {
    let state = test_state_with_bucket("test-bucket").await;
    let mut req = multipart_create_request("test-bucket", "archive.zip");
    req.uri = "/test-bucket/archive.zip?uploads&decompress-zip=prefix/".parse().unwrap();
    req.headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("AES256"));

    let err = create_multipart_upload(&state, req).await.unwrap_err();
    assert_eq!(err.code(), "InvalidArgument");
}
```

Add this helper result compile check in a test after seeding an upload and parts, using existing complete test patterns if present:

```rust
#[tokio::test]
async fn complete_inner_returns_archive_metadata_for_standard_wrapper() {
    let (state, req) = complete_request_with_two_seeded_parts().await;
    let completed = complete_multipart_upload_inner(&state, req).await.unwrap();
    assert_eq!(completed.bucket, "test-bucket");
    assert_eq!(completed.key, "archive.zip");
    assert_eq!(completed.root_cid, "QmRootCid");
    assert!(completed.total_size > 0);
}

#[tokio::test]
async fn finalize_rolls_back_visible_archive_if_upload_delete_fails() {
    let (state, completed) = completed_archive_with_missing_upload().await;

    let err = finalize_completed_multipart_archive(&state, &completed).await.unwrap_err();
    assert!(err.to_string().contains("multipart upload not found"));
    let visible = crate::store::object::get_latest(state.store.db(), &completed.bucket, &completed.key).await;
    assert!(visible.is_err(), "failed finalize must not leave the completed archive visible");
}

async fn completed_archive_with_missing_upload() -> (std::sync::Arc<crate::state::AppState>, CompletedMultipartArchive) {
    let state = test_state_with_bucket("test-bucket").await;
    let completed = CompletedMultipartArchive {
        bucket: "test-bucket".to_string(),
        key: "archive.zip".to_string(),
        upload_id: "missing-upload".to_string(),
        object_id: "object-1".to_string(),
        root_cid: "QmRootCid".to_string(),
        total_size: 5,
        content_type: Some("application/zip".to_string()),
        metadata: None,
        encrypted: false,
        key_wrap: None,
        part_cids: Vec::new(),
        decompress_zip_target: None,
        decompress_zip_result: true,
        server_side_encryption: None,
    };
    (state, completed)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test s3::ops::multipart::tests::create_multipart_upload_records_decompress_query --lib
cargo test s3::ops::multipart::tests::create_multipart_upload_rejects_decompress_sse --lib
cargo test s3::ops::multipart::tests::complete_inner_returns_archive_metadata_for_standard_wrapper --lib
cargo test s3::ops::multipart::tests::finalize_rolls_back_visible_archive_if_upload_delete_fails --lib
```

Expected: tests fail because create does not parse query metadata, `complete_multipart_upload_inner` does not exist, and finalize rollback is not implemented.

- [ ] **Step 3: Implement create query parsing and SSE rejection**

Add local helper functions in `src/s3/ops/multipart.rs`:

```rust
fn multipart_query_pairs(uri: &http::Uri) -> impl Iterator<Item = (&str, &str)> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| part.split_once('=').unwrap_or((part, "")))
}

fn parse_decompress_upload_options(uri: &http::Uri) -> S3Result<(Option<String>, bool)> {
    let mut target = None;
    let mut result = true;
    for (name, value) in multipart_query_pairs(uri) {
        if name == "decompress-zip" {
            target = Some(crate::zip::sanitize::normalize_target_prefix(value)?);
        } else if name == "decompress-zip-result" && value == "false" {
            result = false;
        }
    }
    Ok((target, result))
}

fn has_sse_header(headers: &http::HeaderMap) -> bool {
    headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-MD5")
}
```

In `create_multipart_upload`, parse before `determine_encryption_mode` side effects:

```rust
let (decompress_zip_target, decompress_zip_result) = parse_decompress_upload_options(&req.uri)?;
if decompress_zip_target.is_some() && has_sse_header(&req.headers) {
    return Err(s3s::s3_error!(InvalidArgument, "decompress-zip does not support server-side encryption in MVP"));
}
```

Update the `store::multipart::create_upload` call:

```rust
crate::store::multipart::create_upload(
    db,
    &upload_id,
    &object_id,
    bucket,
    key,
    enc_mode.as_str(),
    key_wrap.as_deref(),
    content_type.as_deref(),
    metadata,
    decompress_zip_target.as_deref(),
    decompress_zip_result,
)
.await?;
```

- [ ] **Step 4: Refactor Complete into an inner archive result**

Add the result type near the top of `src/s3/ops/multipart.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedMultipartArchive {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub object_id: String,
    pub root_cid: String,
    pub total_size: i64,
    pub content_type: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub part_cids: Vec<String>,
    pub decompress_zip_target: Option<String>,
    pub decompress_zip_result: bool,
    pub server_side_encryption: Option<ServerSideEncryption>,
}
```

Move the body of `complete_multipart_upload` into:

```rust
pub async fn complete_multipart_upload_inner(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<CompletedMultipartArchive> {
    // Existing validation, part ordering, part size, Kubo concat/re-encrypt, root stream_add,
    // and root pin logic move here unchanged.
    // Do not object::upsert, unpin part CIDs, or delete the upload here.
    // Return all data needed to either finalize or roll back the completed archive.
}
```

Add explicit finalize and rollback helpers:

```rust
pub async fn finalize_completed_multipart_archive(
    state: &Arc<AppState>,
    completed: &CompletedMultipartArchive,
) -> S3Result<()> {
    crate::store::object::upsert(
        state.store.db(),
        &completed.object_id,
        &completed.bucket,
        &completed.key,
        &completed.root_cid,
        completed.total_size,
        completed.content_type.as_deref(),
        &completed.root_cid,
        completed.metadata.clone(),
        completed.encrypted,
        completed.key_wrap.as_deref(),
        true,
    )
    .await?;

    for cid in &completed.part_cids {
        let _ = crate::kubo::pin::pin_rm(&state.kubo, cid).await;
    }
    if let Err(err) = crate::store::multipart::delete_upload(state.store.db(), &completed.upload_id).await {
        let _ = crate::store::object::delete_latest(state.store.db(), &completed.bucket, &completed.key).await;
        let _ = crate::kubo::pin::pin_rm(&state.kubo, &completed.root_cid).await;
        return Err(err.into());
    }
    Ok(())
}

pub async fn rollback_completed_multipart_archive(
    state: &Arc<AppState>,
    completed: &CompletedMultipartArchive,
) {
    let _ = crate::kubo::pin::pin_rm(&state.kubo, &completed.root_cid).await;
}
```

The `delete_upload` failure branch is required: archive upsert happens before upload deletion, so a post-upsert failure must call `object::delete_latest(bucket, key)` and `pin_rm(root_cid)` before returning the error. This prevents a failed finalize from leaving the new archive visible.

Keep the public standard handler as a wrapper:

```rust
pub async fn complete_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let completed = complete_multipart_upload_inner(state, req).await?;
    finalize_completed_multipart_archive(state, &completed).await?;
    Ok(S3Response::new(CompleteMultipartUploadOutput {
        bucket: Some(completed.bucket.clone()),
        key: Some(completed.key.clone()),
        e_tag: Some(ETag::Strong(completed.root_cid.clone())),
        server_side_encryption: completed.server_side_encryption.clone(),
        ..Default::default()
    }))
}
```

The inner function must capture `upload.object_id`, `upload.content_type`, `upload.metadata`, `upload.key_wrap`, `upload.encryption_mode`, `upload.decompress_zip_target.clone()`, `upload.decompress_zip_result`, and the list of completed part CIDs before returning. It must not call `delete_upload` or unpin part CIDs. The standard wrapper finalizes immediately; the decompress route finalizes only after extraction has no global reject.

- [ ] **Step 5: Run multipart tests to verify they pass**

Run:

```powershell
cargo test s3::ops::multipart --lib
cargo test store::multipart --lib
```

Expected: multipart tests pass, including existing UploadPart and Complete behavior.

- [ ] **Step 6: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit multipart files with message `feat: persist decompress zip multipart metadata`.

---

### Task 7: Raw CompleteMultipartUpload route and multipart decompression

**Files:**
- Modify: `src/s3/route/decompress_zip.rs`
- Modify: `src/zip/response.rs`
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: `complete_multipart_upload_inner`, `CompletedMultipartArchive`, `extract_zip_stream`, `publish_plain_object`, `complete_multipart_result_xml`, and `decompress_result_xml`.
- Produces: raw CompleteMultipartUpload XML parser, custom route handling for all `POST ?uploadId=...` requests, standard Complete XML for non-decompress uploads, and `DecompressZipResult` XML for decompress uploads.

- [ ] **Step 1: Write failing Complete route tests**

Add parser tests to `src/s3/route/decompress_zip.rs`:

```rust
#[test]
fn parse_complete_multipart_xml_extracts_parts_in_order() {
    let xml = r#"<CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
        <Part><PartNumber>2</PartNumber><ETag>etag-2</ETag></Part>
    </CompleteMultipartUpload>"#;

    let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].part_number, 1);
    assert_eq!(parts[0].e_tag.as_ref().map(|etag| etag.value()), Some("etag-1"));
    assert_eq!(parts[1].part_number, 2);
    assert_eq!(parts[1].e_tag.as_ref().map(|etag| etag.value()), Some("etag-2"));
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

`route_with_seeded_sse_multipart(false)` must seed a non-decompress upload whose `encryption_mode` is `sse-s3` and whose completed archive returns `server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256))`; this proves the raw route preserves the standard Complete SSE response header. `route_with_seeded_plain_multipart(true)` keeps using plaintext because decompress uploads reject SSE at Create time.

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

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test s3::route::decompress_zip::tests::parse_complete_multipart_xml_extracts_parts_in_order --lib
cargo test s3::route::decompress_zip::tests::complete_route_returns_standard_xml_for_non_decompress_upload --lib
cargo test s3::route::decompress_zip::tests::complete_route_extracts_when_upload_has_decompress_target --lib
```

Expected: tests fail because the parser and `call_complete` are not implemented.

- [ ] **Step 3: Implement Complete XML parsing**

Add a parser that produces `s3s::dto::CompletedPart` values:

```rust
fn parse_complete_multipart_xml(bytes: &[u8]) -> S3Result<Vec<s3s::dto::CompletedPart>> {
    let mut reader = quick_xml::Reader::from_reader(bytes);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut parts = Vec::new();
    let mut current_part_number: Option<i32> = None;
    let mut current_etag: Option<String> = None;
    let mut current_tag = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(event)) => {
                current_tag = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if current_tag == "Part" {
                    current_part_number = None;
                    current_etag = None;
                }
            }
            Ok(quick_xml::events::Event::Text(text)) => {
                let value = text.unescape().map_err(|e| s3s::s3_error!(MalformedXML, "invalid CompleteMultipartUpload XML: {e}"))?.into_owned();
                match current_tag.as_str() {
                    "PartNumber" => {
                        current_part_number = Some(value.parse::<i32>().map_err(|_| s3s::s3_error!(MalformedXML, "invalid PartNumber"))?);
                    }
                    "ETag" => {
                        current_etag = Some(value.trim_matches('"').to_string());
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(event)) => {
                let tag = String::from_utf8_lossy(event.name().as_ref()).to_string();
                if tag == "Part" {
                    parts.push(s3s::dto::CompletedPart {
                        part_number: current_part_number,
                        e_tag: current_etag.take().map(s3s::dto::ETag::Strong),
                        ..Default::default()
                    });
                }
                current_tag.clear();
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(s3s::s3_error!(MalformedXML, "invalid CompleteMultipartUpload XML: {e}")),
            _ => {}
        }
        buf.clear();
    }

    Ok(parts)
}
```

- [ ] **Step 4: Implement `call_complete` with standard and decompress responses**

Collect the raw body into bytes for Complete XML only. This is acceptable because Complete XML contains part numbers and ETags, not object bytes:

```rust
async fn call_complete(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
    let (bucket, key) = parse_path_bucket_key(&req.uri)?;
    let upload_id = query_pairs(&req.uri)
        .find_map(|(name, value)| (name == "uploadId").then_some(value.to_string()))
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "uploadId is required"))?;

    let body_bytes = req.input
        .collect()
        .await
        .map_err(|e| s3s::s3_error!(IncompleteBody, "complete body: {e}"))?
        .to_bytes();
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
                crate::s3::ops::multipart::rollback_completed_multipart_archive(&self.state, &completed).await;
                return Err(s3s::s3_error!(InternalError, "cat completed archive: {e}"));
            }
        };
        let outcome = match crate::zip::extract::extract_zip_stream(&self.state, &target_prefix, archive_stream).await {
            Ok(outcome) => outcome,
            Err(err) => {
                crate::s3::ops::multipart::rollback_completed_multipart_archive(&self.state, &completed).await;
                return Err(err);
            }
        };

        if let Err(err) = crate::s3::ops::multipart::finalize_completed_multipart_archive(&self.state, &completed).await {
            crate::s3::ops::multipart::rollback_completed_multipart_archive(&self.state, &completed).await;
            crate::zip::extract::cleanup_staged_cids(&self.state, &outcome.staged_cids).await;
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

Import `http_body_util::BodyExt` if the `Body::collect()` method requires the trait. If `http_body_util` is not already available transitively for direct import, add `http-body-util = "0.1"` to `Cargo.toml` in this task.

- [ ] **Step 5: Run Complete route tests to verify they pass**

Run:

```powershell
cargo test s3::route::decompress_zip::tests --lib
cargo test s3::ops::multipart --lib
```

Expected: route complete tests pass and multipart operation tests still pass.

- [ ] **Step 6: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit route and multipart response files with message `feat: decompress completed multipart uploads`.

---

### Task 8: Integration and acceptance coverage

**Files:**
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: app route registration, PutObject route, Multipart create/complete behavior, legal/traversal zip fixtures, and existing integration harness.
- Produces: integration coverage for success, `decompress-zip-result=false`, traversal global reject, SSE rejection, multipart success, multipart result=false, abort no extraction, and standard-operation regressions.

- [ ] **Step 1: Write integration test helpers and failing acceptance tests**

Add fixture constants to `tests/integration.rs`:

```rust
const LEGAL_ZIP_B64: &str = "UEsDBBQAAAAIAAB751yGphA2BwAAAAUAAAALAAAAZm9vL2Jhci50eHTLSM3JyQcAUEsDBBQAAAAIAAB751wAAAAAAgAAAAAAAAAJAAAAZW1wdHkudHh0AwBQSwECFAAUAAAACAAAe+dchqYQNgcAAAAFAAAACwAAAAAAAAAAAAAAgAEAAAAAZm9vL2Jhci50eHRQSwECFAAUAAAACAAAe+dcAAAAAAIAAAAAAAAACQAAAAAAAAAAAAAAgAEwAAAAZW1wdHkudHh0UEsFBgAAAAACAAIAcAAAAFkAAAAAAA==";
const TRAVERSAL_ZIP_B64: &str = "UEsDBBQAAAAIAAB751z7OSuCBQAAAAMAAAANAAAALi4vZXNjYXBlLnR4dEtKTAEAUEsBAhQAFAAAAAgAAHvnXPs5K4IFAAAAAwAAAA0AAAAAAAAAAAAAAIABAAAAAC4uL2VzY2FwZS50eHRQSwUGAAAAAAEAAQA7AAAAMAAAAAAA";
```

Update the harness Kubo stubs so `/api/v0/cat` can return legal zip bytes in decompress tests. The simplest route is to add a second harness function that accepts cat bytes:

```rust
async fn harness_with_cat_body(cat_body: Vec<u8>) -> (String, String, MockServer) {
    // Same body as harness(), but /api/v0/cat responds with cat_body and builder registers DecompressZipRoute.
}
```

Register route in both harnesses:

```rust
builder.set_route(ipfs_s3_gateway::s3::route::decompress_zip::DecompressZipRoute::new(state.clone()));
```

Add a raw signed request helper. If `rust-s3` exposes a presign method that preserves custom query parameters, use it. If not, call `DecompressZipRoute` directly in route unit tests and keep integration tests focused on route registration plus standard regressions. For server-level decompression tests, implement a small SigV4 helper only for path-style PUT/POST using access key `test`, secret `test`, region `us-east-1`, service `s3`, and `UNSIGNED-PAYLOAD`.

Add these acceptance tests:

```rust
#[tokio::test]
async fn test_put_decompress_zip_returns_result_xml() {
    let zip = base64::engine::general_purpose::STANDARD.decode(LEGAL_ZIP_B64).unwrap();
    let (addr, bucket_name, _kubo) = harness_with_cat_body(zip).await;
    let response = signed_put(&addr, &bucket_name, "archive.zip", "decompress-zip=prefix/", b"archive bytes", HeaderMap::new()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(body.contains("<DecompressZipResult>"));
    assert!(body.contains("<ArchiveKey>archive.zip</ArchiveKey>"));
    assert!(body.contains("prefix/foo/bar.txt"));
}

#[tokio::test]
async fn test_put_decompress_zip_result_false_returns_empty_body() {
    let zip = base64::engine::general_purpose::STANDARD.decode(LEGAL_ZIP_B64).unwrap();
    let (addr, bucket_name, _kubo) = harness_with_cat_body(zip).await;
    let response = signed_put(&addr, &bucket_name, "archive.zip", "decompress-zip=prefix/&decompress-zip-result=false", b"archive bytes", HeaderMap::new()).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(http::header::ETAG).is_some());
    assert_eq!(response.bytes().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_put_decompress_zip_traversal_rejects_request() {
    let zip = base64::engine::general_purpose::STANDARD.decode(TRAVERSAL_ZIP_B64).unwrap();
    let (addr, bucket_name, _kubo) = harness_with_cat_body(zip).await;
    let response = signed_put(&addr, &bucket_name, "archive.zip", "decompress-zip=prefix/", b"archive bytes", HeaderMap::new()).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response.text().await.unwrap();
    assert!(body.contains("InvalidParameterValue"));
}

#[tokio::test]
async fn test_put_decompress_zip_sse_rejected() {
    let zip = base64::engine::general_purpose::STANDARD.decode(LEGAL_ZIP_B64).unwrap();
    let (addr, bucket_name, _kubo) = harness_with_cat_body(zip).await;
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("AES256"));
    let response = signed_put(&addr, &bucket_name, "archive.zip", "decompress-zip=prefix/", b"archive bytes", headers).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
```

Add multipart tests using existing rust-s3 multipart helpers when they can attach custom query to Create. If rust-s3 cannot attach the custom Create query, call the operation functions directly for Create and the custom Complete route for Complete in `src/s3/route/decompress_zip.rs` unit tests, then keep `tests/integration.rs` regression coverage for standard multipart unchanged.

- [ ] **Step 2: Run integration tests to verify they fail before final wiring**

Run:

```powershell
cargo test test_put_decompress_zip_returns_result_xml --test integration
cargo test test_put_decompress_zip_result_false_returns_empty_body --test integration
cargo test test_put_decompress_zip_traversal_rejects_request --test integration
cargo test test_put_decompress_zip_sse_rejected --test integration
```

Expected: tests fail until harness route registration, raw signing, and response collection are complete.

- [ ] **Step 3: Complete integration helpers and multipart acceptance tests**

Implement `harness_with_cat_body` by copying `harness()` and replacing only the `/api/v0/cat` response body. Keep `/api/v0/add` and `/api/v0/pin/add` mocks mounted for all calls. For `signed_put`, use `reqwest::Client` and the SigV4 helper selected in Step 1; ensure the final URL is:

```text
http://{addr}/{bucket_name}/{key}?{query}
```

Add multipart acceptance tests at the lowest layer that supports the custom query without broad test-only infrastructure:

- `create_multipart_upload_records_decompress_query` remains a lib test from Task 6.
- `complete_route_extracts_when_upload_has_decompress_target` remains a route test from Task 7.
- Add `test_multipart_abort_still_does_not_extract` in integration or lib tests by creating an upload with `decompress_zip_target=Some("prefix/")`, uploading one part, aborting through the existing handler, and asserting no `prefix/foo/bar.txt` object exists.

- [ ] **Step 4: Run full verification**

Run:

```powershell
cargo test --lib
cargo test --test integration
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: all commands exit 0. If clippy reports warnings in newly added tests or route code, fix the warnings without changing the public behavior.

- [ ] **Step 5: Manual E2E smoke test**

After tests pass, run the docker stack only if the environment already has Docker available and the user approves running long-lived services:

```powershell
docker compose up -d --build
```

Then create a presigned URL that includes `decompress-zip=prefix/` using an SDK or a direct SigV4 helper, upload the legal zip bytes with `curl.exe`, and verify:

```powershell
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/prefix/
curl.exe -X POST "http://localhost:5001/api/v0/cat?arg=<ENTRY_CID>"
docker compose down -v
```

Expected: `aws s3 ls` shows the extracted entries and `ipfs cat` returns the original entry bytes. Always run `docker compose down -v` after the smoke test.

- [ ] **Step 6: Manual commit checkpoint**

Do not run git writes unless explicitly approved. If approval is granted, commit integration tests with message `test: cover decompress zip uploads`.

---

## Self-Review

**Spec coverage:** This plan covers the spec's dependency and compat requirements, schema, error mapping, query parsing, path sanitizer, XML response, archive-first PutObject, safe-entry partial read/decompression failures, multipart metadata, raw Complete route, standard Complete SSE header preservation, deferred multipart archive publication for global rejects, finalize rollback after archive upsert, route registration, and test strategy.

**Placeholder scan:** The plan contains no unresolved marker phrases, no intentionally vague edge handling, and every task has concrete files, interfaces, tests, commands, and expected results.

**Type consistency:** `StoredObject`, `SanitizedEntry`, `ExtractedEntry`, `ExtractFailure`, `DecompressZipResult`, `ExtractOutcome`, `CompletedMultipartArchive`, and `DecompressZipRoute` names are consistent across producer and consumer tasks.

**Commit guard:** Commit steps are manual checkpoints only and explicitly require user approval before any git write command is run.
