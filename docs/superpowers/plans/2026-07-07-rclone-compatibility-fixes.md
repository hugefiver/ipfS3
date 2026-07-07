# Rclone Compatibility Fixes Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix S3-compatible directory listing behavior for clients that rely on `ListObjectsV2` delimiter semantics, and lock signed `HeadObject` requests for nested keys with regression coverage.

**Architecture:** Keep the existing `s3s` DTO path and implement compatibility inside `src/s3/ops/object.rs`. `ListObjectsV2` remains backed by the existing SeaORM object store, but the S3 operation layer folds sorted object keys into `Contents` and `CommonPrefixes` before returning the DTO. Signed nested-key `HeadObject` behavior is guarded at the integration-test layer because authentication happens before `head_object` reaches application logic.

**Tech Stack:** Rust 2024, axum 0.8, s3s 0.14, SeaORM 1, Kubo RPC via reqwest 0.13, rust-s3 0.37 integration tests, wiremock, tokio.

**Global Constraints:**
- Preserve existing object storage, ETag=CID, encryption, copy, delete, get, and non-delimited list behavior.
- Implement `ListObjectsV2` delimiter support only for the existing v2 operation; do not add `ListObjects` v1 in this plan.
- `max_keys` limits the combined count of `Contents` plus `CommonPrefixes`.
- `ContinuationToken` continues to be an opaque string that clients only round-trip; internally it may remain the last consumed key.
- `StartAfter` applies only when `ContinuationToken` is absent.
- Keep store reads ordered by key and do delimiter folding in the S3 operation layer.
- Do not introduce a new dependency for listing; use standard collections and existing s3s DTOs.
- Use PowerShell-compatible verification commands.
- Git write commands are manual checkpoints only. Do not run `git commit`, `git push`, `git tag`, or other git write commands unless the user explicitly approves them in the active conversation.

---

## File Structure

- Modify `src/s3/ops/object.rs` for `ListObjectsV2` folding helpers, `list_objects_v2` response fields, and unit tests.
- Modify `tests/integration.rs` for delimiter/CommonPrefixes integration coverage and signed nested-key `HeadObject` regression coverage.
- No schema or Kubo changes are required.

---

### Task 1: ListObjectsV2 folding model and unit tests

**Files:**
- Modify: `src/s3/ops/object.rs`

**Interfaces:**
- Consumes: `crate::store::entities::object::Model`, `s3s::dto::{CommonPrefix, Object}`.
- Produces: `ListObjectsV2Entry`, `ListObjectsV2Page`, `fold_list_objects_v2_rows(rows, prefix, delimiter, max_keys) -> ListObjectsV2Page`.

- [ ] **Step 1: Write failing unit tests for delimiter folding**

Add these imports inside `#[cfg(test)] mod tests` in `src/s3/ops/object.rs`:

```rust
use chrono::Utc;
use crate::store::entities::object;
```

Add this helper and tests after the existing range tests:

```rust
fn object_model(key: &str) -> object::Model {
    object::Model {
        id: format!("id-{key}"),
        bucket: "test-bkt".to_string(),
        key: key.to_string(),
        cid: format!("cid-{key}"),
        size: 10,
        content_type: None,
        etag: format!("cid-{key}"),
        metadata: None,
        encrypted: false,
        key_wrap: None,
        multipart: false,
        is_latest: true,
        created_at: Utc::now(),
    }
}

#[test]
fn list_v2_fold_without_delimiter_returns_objects_only() {
    let rows = vec![object_model("a.txt"), object_model("photos/cat.jpg")];

    let page = fold_list_objects_v2_rows(rows, "", None, 1000);

    assert_eq!(page.entries.len(), 2);
    assert_eq!(page.entries[0].key(), "a.txt");
    assert_eq!(page.entries[1].key(), "photos/cat.jpg");
    assert!(page.common_prefixes().is_empty());
    assert!(!page.is_truncated);
    assert_eq!(page.next_token, None);
}

#[test]
fn list_v2_fold_with_delimiter_returns_direct_children_and_common_prefixes() {
    let rows = vec![
        object_model("a.txt"),
        object_model("photos/cat.jpg"),
        object_model("photos/dog.jpg"),
        object_model("videos/clip.mp4"),
    ];

    let page = fold_list_objects_v2_rows(rows, "", Some("/"), 1000);

    assert_eq!(page.object_keys(), vec!["a.txt"]);
    assert_eq!(page.common_prefixes(), vec!["photos/", "videos/"]);
    assert!(!page.is_truncated);
}

#[test]
fn list_v2_fold_with_prefix_and_delimiter_scopes_common_prefixes() {
    let rows = vec![
        object_model("photos/2024/jan.jpg"),
        object_model("photos/2024/feb.jpg"),
        object_model("photos/cat.jpg"),
        object_model("photos/dog.jpg"),
    ];

    let page = fold_list_objects_v2_rows(rows, "photos/", Some("/"), 1000);

    assert_eq!(page.object_keys(), vec!["photos/cat.jpg", "photos/dog.jpg"]);
    assert_eq!(page.common_prefixes(), vec!["photos/2024/"]);
}

#[test]
fn list_v2_fold_counts_common_prefixes_toward_max_keys() {
    let rows = vec![
        object_model("a.txt"),
        object_model("photos/cat.jpg"),
        object_model("photos/dog.jpg"),
        object_model("videos/clip.mp4"),
    ];

    let page = fold_list_objects_v2_rows(rows, "", Some("/"), 2);

    assert_eq!(page.object_keys(), vec!["a.txt"]);
    assert_eq!(page.common_prefixes(), vec!["photos/"]);
    assert!(page.is_truncated);
    assert_eq!(page.next_token.as_deref(), Some("photos/dog.jpg"));
}
```

- [ ] **Step 2: Run the tests and verify they fail**

Run:

```powershell
cargo test --lib list_v2_fold -- --nocapture
```

Expected: FAIL because `fold_list_objects_v2_rows`, `ListObjectsV2Page`, and helper accessors do not exist.

- [ ] **Step 3: Implement the folding types and pure helper**

Add this import near the top of `src/s3/ops/object.rs`:

```rust
use std::collections::HashMap;
```

Add these types and helper functions above `pub async fn list_objects_v2`:

```rust
#[derive(Clone, Debug)]
enum ListObjectsV2Entry {
    Object(crate::store::entities::object::Model),
    CommonPrefix { prefix: String, continuation_key: String },
}

impl ListObjectsV2Entry {
    #[cfg(test)]
    fn key(&self) -> &str {
        match self {
            Self::Object(obj) => &obj.key,
            Self::CommonPrefix { prefix, .. } => prefix,
        }
    }
}

#[derive(Clone, Debug)]
struct ListObjectsV2Page {
    entries: Vec<ListObjectsV2Entry>,
    is_truncated: bool,
    next_token: Option<String>,
}

impl ListObjectsV2Page {
    #[cfg(test)]
    fn object_keys(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                ListObjectsV2Entry::Object(obj) => Some(obj.key.as_str()),
                ListObjectsV2Entry::CommonPrefix { .. } => None,
            })
            .collect()
    }

    #[cfg(test)]
    fn common_prefixes(&self) -> Vec<&str> {
        self.entries
            .iter()
            .filter_map(|entry| match entry {
                ListObjectsV2Entry::Object(_) => None,
                ListObjectsV2Entry::CommonPrefix { prefix, .. } => Some(prefix.as_str()),
            })
            .collect()
    }
}

fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|d| !d.is_empty())?;
    let rest = key.strip_prefix(prefix).unwrap_or(key);
    let delimiter_index = rest.find(delimiter)?;
    let end = prefix.len() + delimiter_index + delimiter.len();
    Some(key[..end].to_string())
}

enum PushListEntryResult {
    Continue,
    PageComplete,
}

struct ListObjectsV2PageBuilder {
    prefix: String,
    delimiter: Option<String>,
    max_keys: usize,
    entries: Vec<ListObjectsV2Entry>,
    common_prefix_positions: HashMap<String, usize>,
    last_returned_token: Option<String>,
    is_truncated: bool,
}

impl ListObjectsV2PageBuilder {
    fn new(prefix: &str, delimiter: Option<&str>, max_keys: usize) -> Self {
        Self {
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            max_keys,
            entries: Vec::new(),
            common_prefix_positions: HashMap::new(),
            last_returned_token: None,
            is_truncated: false,
        }
    }

    fn push_row(&mut self, obj: crate::store::entities::object::Model) -> PushListEntryResult {
        let key = obj.key.clone();
        if let Some(common_prefix) = common_prefix_for_key(&key, &self.prefix, self.delimiter.as_deref()) {
            if let Some(index) = self.common_prefix_positions.get(&common_prefix).copied() {
                if let Some(ListObjectsV2Entry::CommonPrefix { continuation_key, .. }) = self.entries.get_mut(index) {
                    *continuation_key = key.clone();
                }
                self.last_returned_token = Some(key);
                return PushListEntryResult::Continue;
            }

            if self.entries.len() >= self.max_keys {
                self.is_truncated = true;
                return PushListEntryResult::PageComplete;
            }

            self.common_prefix_positions
                .insert(common_prefix.clone(), self.entries.len());
            self.entries.push(ListObjectsV2Entry::CommonPrefix {
                prefix: common_prefix,
                continuation_key: key.clone(),
            });
            self.last_returned_token = Some(key);
            return PushListEntryResult::Continue;
        }

        if self.entries.len() >= self.max_keys {
            self.is_truncated = true;
            return PushListEntryResult::PageComplete;
        }

        self.entries.push(ListObjectsV2Entry::Object(obj));
        self.last_returned_token = Some(key);
        PushListEntryResult::Continue
    }

    fn finish(self) -> ListObjectsV2Page {
        ListObjectsV2Page {
            entries: self.entries,
            is_truncated: self.is_truncated,
            next_token: self.is_truncated.then_some(self.last_returned_token).flatten(),
        }
    }
}

#[cfg(test)]
fn fold_list_objects_v2_rows(
    rows: Vec<crate::store::entities::object::Model>,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
) -> ListObjectsV2Page {
    let mut builder = ListObjectsV2PageBuilder::new(prefix, delimiter, max_keys);
    for obj in rows {
        if matches!(builder.push_row(obj), PushListEntryResult::PageComplete) {
            break;
        }
    }
    builder.finish()
}
```

- [ ] **Step 4: Run the folding tests and verify they pass**

Run:

```powershell
cargo test --lib list_v2_fold -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Manual checkpoint**

Do not commit unless explicitly approved. If approved later, stage only `src/s3/ops/object.rs` for this task.

---

### Task 2: Wire ListObjectsV2 delimiter/start-after response semantics

**Files:**
- Modify: `src/s3/ops/object.rs`

**Interfaces:**
- Consumes: `ListObjectsV2PageBuilder`, `store::object::list(db, bucket, prefix, continuation_token, max_keys)`.
- Produces: `list_objects_v2(state, req) -> S3Result<S3Response<ListObjectsV2Output>>` with `common_prefixes`, `delimiter`, `prefix`, `start_after`, and correct `key_count`.

- [ ] **Step 1: Add async unit tests for the public operation**

Add this test setup inside `#[cfg(test)] mod tests` in `src/s3/ops/object.rs`:

```rust
use std::collections::HashMap;
use s3s::auth::SecretKey;
use crate::crypto::key::MasterKey;
use crate::kubo::KuboClient;
use crate::state::AppState;
use crate::store::Store;

async fn list_state_with_keys(keys: &[&str]) -> Arc<AppState> {
    let db = sea_orm::Database::connect("sqlite::memory:")
        .await
        .expect("in-memory sqlite");
    crate::store::run_migrations(&db).await.expect("migrations");
    crate::store::bucket::create(&db, "test-bkt", None)
        .await
        .expect("bucket");

    for key in keys {
        crate::store::object::upsert(
            &db,
            &format!("id-{key}"),
            "test-bkt",
            key,
            &format!("cid-{key}"),
            10,
            None,
            &format!("cid-{key}"),
            None,
            false,
            None,
            false,
        )
        .await
        .expect("insert object");
    }

    let mut credentials = HashMap::new();
    credentials.insert("test".to_string(), SecretKey::from("test"));

    Arc::new(AppState {
        kubo: KuboClient::new("http://127.0.0.1:5001".to_string()),
        store: Store::new(db),
        credentials,
        master_key: MasterKey::from_hex("0000000000000000000000000000000000000000000000000000000000000000")
            .expect("master key"),
    })
}

fn list_v2_request(input: ListObjectsV2Input) -> S3Request<ListObjectsV2Input> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: http::Uri::from_static("/test-bkt?list-type=2"),
        headers: http::HeaderMap::new(),
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}
```

Add operation tests:

```rust
#[tokio::test]
async fn list_objects_v2_sets_common_prefixes_when_delimiter_is_present() {
    let state = list_state_with_keys(&[
        "a.txt",
        "photos/cat.jpg",
        "photos/dog.jpg",
        "videos/clip.mp4",
    ])
    .await;

    let response = list_objects_v2(
        &state,
        list_v2_request(ListObjectsV2Input {
            bucket: "test-bkt".to_string(),
            delimiter: Some("/".to_string()),
            max_keys: Some(1000),
            ..Default::default()
        }),
    )
    .await
    .expect("list objects v2");

    let output = response.output;
    let contents = output.contents.expect("contents");
    let prefixes = output.common_prefixes.expect("common prefixes");

    assert_eq!(contents.iter().map(|o| o.key.as_deref()).collect::<Vec<_>>(), vec![Some("a.txt")]);
    assert_eq!(prefixes.iter().map(|p| p.prefix.as_deref()).collect::<Vec<_>>(), vec![Some("photos/"), Some("videos/")]);
    assert_eq!(output.key_count, Some(3));
    assert_eq!(output.delimiter.as_deref(), Some("/"));
    assert_eq!(output.prefix.as_deref(), Some(""));
}

#[tokio::test]
async fn list_objects_v2_uses_start_after_when_no_continuation_token_exists() {
    let state = list_state_with_keys(&["a.txt", "b.txt", "c.txt"]).await;

    let response = list_objects_v2(
        &state,
        list_v2_request(ListObjectsV2Input {
            bucket: "test-bkt".to_string(),
            start_after: Some("a.txt".to_string()),
            max_keys: Some(1000),
            ..Default::default()
        }),
    )
    .await
    .expect("list objects v2");

    let output = response.output;
    let contents = output.contents.expect("contents");
    assert_eq!(contents.iter().map(|o| o.key.as_deref()).collect::<Vec<_>>(), vec![Some("b.txt"), Some("c.txt")]);
    assert_eq!(output.start_after.as_deref(), Some("a.txt"));
}

#[tokio::test]
async fn list_objects_v2_scans_past_duplicate_prefix_rows_to_detect_truncation() {
    let state = list_state_with_keys(&[
        "a.txt",
        "photos/cat.jpg",
        "photos/dog.jpg",
        "videos/clip.mp4",
    ])
    .await;

    let first = list_objects_v2(
        &state,
        list_v2_request(ListObjectsV2Input {
            bucket: "test-bkt".to_string(),
            delimiter: Some("/".to_string()),
            max_keys: Some(2),
            ..Default::default()
        }),
    )
    .await
    .expect("first page");

    assert_eq!(first.output.key_count, Some(2));
    assert_eq!(first.output.is_truncated, Some(true));
    assert_eq!(first.output.next_continuation_token.as_deref(), Some("photos/dog.jpg"));

    let second = list_objects_v2(
        &state,
        list_v2_request(ListObjectsV2Input {
            bucket: "test-bkt".to_string(),
            delimiter: Some("/".to_string()),
            continuation_token: first.output.next_continuation_token,
            max_keys: Some(2),
            ..Default::default()
        }),
    )
    .await
    .expect("second page");

    let prefixes = second.output.common_prefixes.expect("second page prefixes");
    assert_eq!(prefixes.iter().map(|p| p.prefix.as_deref()).collect::<Vec<_>>(), vec![Some("videos/")]);
    assert_eq!(second.output.is_truncated, Some(false));
}
```

- [ ] **Step 2: Run the operation tests and verify they fail**

Run:

```powershell
cargo test --lib list_objects_v2_ -- --nocapture
```

Expected: FAIL because `list_objects_v2` does not set `common_prefixes`, `delimiter`, `prefix`, or `start_after`, does not use `start_after` as the initial cursor, and cannot detect truncation after duplicate rows folded into one common prefix.

- [ ] **Step 3: Update `list_objects_v2`**

Replace the current body after bucket-existence validation with this shape. The loop intentionally fetches sorted rows in batches until it either proves that the response is truncated or reaches the end of the store cursor; `max_keys + 1` rows are not enough when many rows fold into one `CommonPrefix`.

```rust
let delimiter = req.input.delimiter.clone();
let encoding_type = req.input.encoding_type.clone();
let start_after = req.input.start_after.clone();
let mut cursor = continuation_token
    .as_deref()
    .filter(|token| !token.is_empty())
    .or_else(|| start_after.as_deref().filter(|token| !token.is_empty()))
    .map(str::to_string);

let mut builder = ListObjectsV2PageBuilder::new(
    prefix.as_deref().unwrap_or(""),
    delimiter.as_deref(),
    max_keys as usize,
);
let batch_limit = 1000;

'paging: loop {
    let objects = crate::store::object::list(
        db,
        bucket,
        prefix.as_deref(),
        cursor.as_deref(),
        batch_limit,
    )
    .await?;

    if objects.is_empty() {
        break;
    }

    let fetched = objects.len();
    for obj in objects {
        let row_key = obj.key.clone();
        match builder.push_row(obj) {
            PushListEntryResult::Continue => cursor = Some(row_key),
            PushListEntryResult::PageComplete => break 'paging,
        }
    }

    if fetched < batch_limit as usize {
        break;
    }
}

let page = builder.finish();

let mut contents = Vec::new();
let mut common_prefixes = Vec::new();

for entry in page.entries {
    match entry {
        ListObjectsV2Entry::Object(m) => contents.push(Object {
            key: Some(m.key),
            size: Some(m.size),
            e_tag: Some(ETag::Strong(m.etag)),
            last_modified: Some(Timestamp::from(SystemTime::from(m.created_at))),
            ..Default::default()
        }),
        ListObjectsV2Entry::CommonPrefix { prefix, .. } => {
            common_prefixes.push(CommonPrefix { prefix: Some(prefix) });
        }
    }
}

let key_count = contents.len() + common_prefixes.len();

Ok(S3Response::new(ListObjectsV2Output {
    contents: Some(contents),
    common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
    is_truncated: Some(page.is_truncated),
    continuation_token,
    next_continuation_token: page.next_token,
    key_count: Some(key_count as i32),
    max_keys: Some(max_keys as i32),
    name: Some(bucket.clone()),
    prefix: Some(prefix.unwrap_or_default()),
    delimiter,
    encoding_type,
    start_after,
    ..Default::default()
}))
```

Keep the existing `NoSuchBucket` handling and `max_keys` clamp.

- [ ] **Step 4: Run targeted library tests**

Run:

```powershell
cargo test --lib list_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run the full library tests**

Run:

```powershell
cargo test --lib
```

Expected: PASS.

- [ ] **Step 6: Manual checkpoint**

Do not commit unless explicitly approved. If approved later, stage only `src/s3/ops/object.rs` for this task.

---

### Task 3: Integration coverage for directory-style listing

**Files:**
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: `rust_s3::Bucket::list(prefix, delimiter)`, current integration `harness()` and `test_bucket()` helpers.
- Produces: integration tests proving `CommonPrefixes` is visible through the HTTP/S3 serialization boundary.

- [ ] **Step 1: Write failing integration tests**

Add these tests after `test_list_objects` in `tests/integration.rs`:

```rust
#[tokio::test]
async fn test_list_objects_with_delimiter_returns_common_prefixes() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket.put_object("a.txt", b"hello world").await.expect("put a");
    bucket.put_object("photos/cat.jpg", b"hello world").await.expect("put cat");
    bucket.put_object("photos/dog.jpg", b"hello world").await.expect("put dog");
    bucket.put_object("videos/clip.mp4", b"hello world").await.expect("put clip");

    let pages = bucket
        .list(String::new(), Some("/".to_string()))
        .await
        .expect("list with delimiter");

    let object_keys: Vec<String> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|obj| obj.key.clone()))
        .collect();
    let mut common_prefixes: Vec<String> = pages
        .iter()
        .flat_map(|page| page.common_prefixes.clone().unwrap_or_default())
        .map(|prefix| prefix.prefix)
        .collect();
    common_prefixes.sort();

    assert_eq!(object_keys, vec!["a.txt".to_string()]);
    assert_eq!(common_prefixes, vec!["photos/".to_string(), "videos/".to_string()]);
}

#[tokio::test]
async fn test_list_objects_with_prefix_and_delimiter_returns_one_level() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket.put_object("photos/cat.jpg", b"hello world").await.expect("put cat");
    bucket.put_object("photos/dog.jpg", b"hello world").await.expect("put dog");
    bucket.put_object("photos/2024/jan.jpg", b"hello world").await.expect("put jan");

    let pages = bucket
        .list("photos/".to_string(), Some("/".to_string()))
        .await
        .expect("list prefix with delimiter");

    let mut object_keys: Vec<String> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|obj| obj.key.clone()))
        .collect();
    object_keys.sort();
    let common_prefixes: Vec<String> = pages
        .iter()
        .flat_map(|page| page.common_prefixes.clone().unwrap_or_default())
        .map(|prefix| prefix.prefix)
        .collect();

    assert_eq!(object_keys, vec!["photos/cat.jpg".to_string(), "photos/dog.jpg".to_string()]);
    assert_eq!(common_prefixes, vec!["photos/2024/".to_string()]);
}
```

- [ ] **Step 2: Run the new integration tests and verify they fail before Task 2**

Run before Task 2 is implemented if possible:

```powershell
cargo test --test integration test_list_objects_with_ -- --nocapture
```

Expected before the fix: FAIL because `common_prefixes` is absent and nested keys appear as flat contents.

- [ ] **Step 3: Run the tests after Task 2**

Run:

```powershell
cargo test --test integration test_list_objects_with_ -- --nocapture
```

Expected after the fix: PASS.

- [ ] **Step 4: Verify existing list behavior still works**

Run:

```powershell
cargo test --test integration test_list_objects -- --exact --nocapture
```

Expected: PASS; listing without delimiter still returns the flat object list.

- [ ] **Step 5: Manual checkpoint**

Do not commit unless explicitly approved. If approved later, stage `tests/integration.rs` with the object-listing implementation.

---

### Task 4: Signed HeadObject nested-key regression coverage

**Files:**
- Modify: `tests/integration.rs`

**Interfaces:**
- Consumes: existing integration `harness()`, `test_bucket()`, `Bucket::put_object`, `Bucket::head_object`.
- Produces: a regression test proving signed `HEAD /bucket/path/to/key` reaches the object handler and returns object metadata.

- [ ] **Step 1: Write the signed nested-key HeadObject test**

Add this test after the listing tests in `tests/integration.rs`:

```rust
#[tokio::test]
async fn test_head_object_signed_nested_key_succeeds() {
    let (addr, bucket_name, _kubo) = harness().await;
    let bucket = test_bucket(&addr, &bucket_name);

    bucket
        .put_object("nested/path/file.txt", b"hello world")
        .await
        .expect("put nested object");

    let (head, status_code) = bucket
        .head_object("nested/path/file.txt")
        .await
        .expect("head nested object");

    assert_eq!(status_code, 200);
    let etag = head.e_tag.expect("etag header");
    assert!(
        etag.contains("QmTestCid"),
        "expected ETag to include mocked CID, got {etag}"
    );
}
```

- [ ] **Step 2: Run the regression test**

Run:

```powershell
cargo test --test integration test_head_object_signed_nested_key_succeeds -- --nocapture
```

Expected after the listing fix: PASS. If this fails with `SignatureDoesNotMatch` or `403`, do not change `head_object`; authentication failed before the operation handler. In that case, create a minimal failing test that prints the request path and signed headers, then evaluate an `s3s` patch or dependency upgrade in a separate task before touching application handlers.

- [ ] **Step 3: Preserve existing HeadObject metadata behavior**

If the nested-key test passes, make no production code change for `head_object`. The handler already returns `content_length`, `content_type`, `e_tag`, `last_modified`, `server_side_encryption`, and restored user metadata from the DB model.

- [ ] **Step 4: Manual checkpoint**

Do not commit unless explicitly approved. If approved later, stage `tests/integration.rs` with the other integration tests.

---

### Task 5: Final verification and acceptance

**Files:**
- Modify: no additional files unless previous tasks reveal compilation issues.

**Interfaces:**
- Consumes: all changes from Tasks 1-4.
- Produces: verified fix ready for review.

- [ ] **Step 1: Format the code**

Run:

```powershell
cargo fmt --check
```

Expected: exit code 0.

- [ ] **Step 2: Run full unit tests**

Run:

```powershell
cargo test --lib
```

Expected: PASS.

- [ ] **Step 3: Run integration tests**

Run:

```powershell
cargo test --test integration
```

Expected: PASS.

- [ ] **Step 4: Record the non-blocking lint baseline**

Run:

```powershell
cargo clippy --lib --tests -- -D warnings
```

Expected in the current baseline: this may fail on an unrelated pre-existing `clippy::collapsible_if` warning in `src/config.rs`. That warning is outside this compatibility fix. Do not treat that baseline warning as a failure of this plan, and do not expand this fix to unrelated cleanup unless separately requested. The blocking verification for this plan is `cargo fmt --check`, `cargo test --lib`, and `cargo test --test integration`.

- [ ] **Step 5: Optional local client smoke test**

Only run this if the local gateway and Kubo stack are already approved to start:

```powershell
$env:AWS_ACCESS_KEY_ID = "test"; $env:AWS_SECRET_ACCESS_KEY = "test"; $env:AWS_DEFAULT_REGION = "us-east-1"
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/a.txt
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/photos/cat.jpg
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/videos/clip.mp4
rclone lsf s3remote:test-bucket/
```

Expected `rclone lsf` output:

```text
a.txt
photos/
videos/
```

- [ ] **Step 6: Manual checkpoint**

Do not commit unless explicitly approved. If approved later, inspect `git status --short`, inspect the diff, stage only the fix and test files, and use a semantic message such as:

```text
fix(s3): support delimiter prefixes in list objects v2
```

---

## Self-Review

- **Coverage:** Tasks cover delimiter/CommonPrefixes folding, prefix+delimiter behavior, `max_keys` counting, `start_after`, integration XML serialization through rust-s3, existing non-delimited listing behavior, and signed nested-key HeadObject regression coverage.
- **Placeholder scan:** No placeholders remain; every task names exact files, functions, commands, and expected outcomes.
- **Type consistency:** Plan uses s3s 0.14 DTO names `ListObjectsV2Input`, `ListObjectsV2Output`, `Object`, `CommonPrefix`, `common_prefixes`, `delimiter`, `encoding_type`, and `start_after` as defined by the installed crate.
- **Scope check:** The plan fixes the S3 v2 listing behavior and adds a HeadObject regression guard without adding ListObjects v1, changing storage schema, or changing unrelated object operations.
