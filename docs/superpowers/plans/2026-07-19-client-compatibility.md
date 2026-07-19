# Client Compatibility v0.2 Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete all ten `ROADMAP.md` v0.2 client-compatibility items with standard `s3s` operations, deterministic TCP regressions, safe Compose startup, executable Docker-client smoke coverage, and an evidence-backed compatibility matrix.

**Architecture:** Keep standard S3 traffic on the existing axum → `s3s` → `S3Impl` path. Add thin handler/DTO adapters around one listing page builder and SeaORM helpers; keep Docker/configuration and smoke concerns outside domain code. Reuse the real TCP harness and SigV4 signer for wire proof, and report Docker smoke separately from Rust tests.

**Tech Stack:** Rust 2024 (MSRV 1.92), axum 0.8, `s3s` 0.14.0, SeaORM 1/SQLite, reqwest 0.13, rust-s3 0.37.2, wiremock 0.6, tokio 1, Docker Engine 28.3.3, Docker Compose 2.39.2, PowerShell 7, rclone 1.74.4.

**Status (2026-07-20):** `[WAITING-RECEIPT]` — amended after final-review corrections for list URL projection and client evidence. The required RED→GREEN, real-stack smoke, and all listed gates must be rerun from this amended state. This task performs no stage, commit, push, tag, image pull, installation, or volume deletion.

**Global Constraints:**
- Start from HEAD `5e854e4`; stop and re-plan if `git rev-parse HEAD` differs.
- Implement all ten v0.2 rows and no v0.3+ behavior.
- Preserve ETag=CID, plaintext Kubo `cat`, S3 decryption, streaming, v2, copy, SSE, multipart, and decompress-zip behavior.
- `us-east-1` serializes as empty/absent `LocationConstraint`, never literal `us-east-1`.
- Listing cursors are strict (`key > cursor`); clamp `max_keys` to 1..=1000 and count `Contents + CommonPrefixes`.
- List v1/v2 use one page/folding implementation; v1 has no continuation tokens, while v2 token precedence over `start_after` stays unchanged.
- `DeleteObjects` preserves request order, treats missing/duplicate keys as success, honors quiet mode, and continues after per-key DB errors.
- Never call `kubo::pin::pin_rm`; deletes only change database visibility.
- Never read, modify, stage, or depend on ignored `config.toml`; create tracked `config.docker.toml`.
- Missing/empty `IPFS_S3_MASTER_KEY` preserves the file value; non-empty overrides; invalid non-empty input fails before listener bind.
- Host commands are PowerShell. The only POSIX shell expression in this plan is the explicitly identified Linux-container healthcheck.
- Do not install software, pull images, or execute a Compose build without separate authorization.
- Re-check images at execution time. Current read-only evidence: Kubo, gateway, `rclone/rclone:1.74.4`, and `minio/mc:latest` are present; `amazon/aws-cli:latest` is absent.
- Results are exact: `PASSED` ran and passed, `FAILED` ran and failed, `SKIPPED` did not run. Image presence, script syntax, and Rust tests are not smoke success.
- Any client that was actually executed and returned `FAILED` blocks that client's ROADMAP checkbox and blocks the final commit. A client may be checked while `SKIPPED` only when the smoke artifact is implemented but not executed because authorization or a required local image is absent; its matrix row must remain `SKIPPED` with the exact reason.
- ROADMAP v0.2 item 9 is a separate hard gate: before staging or committing, Task 6 must run a real Mc smoke that proves its own signed `stat` for the same `nested/path/file.txt` object through both `http://127.0.0.1:9000` and `http://gateway:9000`, emits `client=Mc verifier=Mc`, `RESULT dual_head=PASSED`, and matching `EVIDENCE dual_head=PASSED` in `docs/client-smoke-evidence-2026-07-19.log`. Rclone `PASSED dual_head=NOT_RUN` is valid but cannot satisfy item 9; AWS may accurately remain `SKIPPED` when its image is missing. Task 4's localhost Rust test alone never satisfies this gate.
- Tasks 1-5 do not commit. Task 6 creates exactly one commit only after explicit Git-write authorization.

---

## File Structure

### Create
- `config.docker.toml` — tracked Compose config: network Kubo URL, createable file SQLite URL, test credentials, non-empty development master key.
- `scripts/client-smoke.ps1` — safe preflight/execution runner, portable `client-smoke.log` evidence, local-only gateway fallback, temporary `test`/`test` mc aliases, isolated rclone/mc/AWS flows, Mc/AWS same-client dual-endpoint checks, and exact statuses.
- `docs/client-compatibility.md` — recommended rclone config, compatibility matrix, evidence, status definitions, limitations.
- `docs/client-smoke-evidence-2026-07-19.log` — the only approved tracked actual-smoke receipt. Task 6 creates it only during the mandatory real `-Run` smoke by writing its captured stdout/stderr as UTF-8; it contains only `test`/`test` scenarios and no local sensitive configuration. Preflight, Rust tests, source-shape checks, and temporary transcripts neither create nor satisfy it.

### Modify
- `src/config.rs` — testable environment overlay and non-empty master-key override.
- `src/s3/handler.rs` — delegates for `get_bucket_location`, `list_objects`, `delete_objects`.
- `src/s3/ops/bucket.rs` — fixed-region location operation/tests.
- `src/s3/ops/object.rs` — shared listing model, v1/v2 adapters, batch delete/tests.
- `src/store/object.rs` — idempotent latest-row delete helper without changing strict single delete.
- `tests/support/sigv4.rs` — bucket URI canonicalization.
- `tests/integration.rs` — real TCP location/v1/delete/nested-HEAD coverage.
- `docker-compose.yml` — tracked config mount and gateway health/startup dependency.
- `config.example.toml` — SQLite create mode.
- `ROADMAP.md` — client smoke artifacts may be checked as `PASSED` or accurately documented artifact-only `SKIPPED`, but item 9 stays unchecked until tracked-log- and matrix-backed Mc same-client `dual_head=PASSED`; any actual `FAILED`, all-`SKIPPED` output, Rclone-only `PASSED/NOT_RUN`, or missing Mc proof stops the release commit.

### Reuse unchanged
- `tests/support/decompress.rs` — `start_harness`, `TestHarness`, production service wiring, observable Kubo log.
- `tests/e2e.rs` — optional authorized real-stack suite; mandatory automated regression remains `tests/integration.rs`.
- `docs/superpowers/plans/2026-07-07-rclone-compatibility-fixes.md` — history only. Do not reuse its v2-only scope, old `ListObjectsV2*` implementation, obsolete response-field failures, or old clippy exception.

---

## Execution and commit boundaries

1. Run Tasks 1-6 in order with a fresh implementation subagent per task and inspect integration after each return.
2. “Checkpoint” means inspect and continue with one uncommitted change set; never stage/commit in Tasks 1-5.
3. Task 6 owns mandatory authorized dual-head execution, UTF-8 capture to the tracked evidence log, result/evidence parsing, matrix/ROADMAP result gating, review, explicit Git authorization, staging, and the single commit; any actually executed `FAILED` client, all-`SKIPPED` result set, Rclone-only `PASSED/NOT_RUN`, absent matching Mc/Mc `dual_head=PASSED`, or missing evidence log stops before staging.

---

### Task 1: GetBucketLocation, config precedence, and Compose startup

**Files:**
- Modify: `src/config.rs:137-198`
- Modify: `src/s3/handler.rs:22-128`
- Modify: `src/s3/ops/bucket.rs:1-76`
- Modify: `docker-compose.yml:22-48`
- Modify: `config.example.toml:7-18`
- Create: `config.docker.toml`
- Test: `src/config.rs`, `src/s3/ops/bucket.rs`

**Interfaces:**
- Consumes: `AppState::new(&Config)`, `store::bucket::exists`, `GetBucketLocationInput/Output`, current default → file → environment precedence.
- Produces: `Config::apply_env_overrides<F>(&mut self, F) -> anyhow::Result<()>`, `ops::bucket::get_bucket_location(&Arc<AppState>, S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>>`, matching `S3Impl` delegate, tracked Compose config, gateway listener health.

- [ ] **Step 1: Add failing config tests**

Append to `src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const FILE_KEY: &str =
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ENV_KEY: &str =
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    fn file_config() -> Config {
        let mut config = Config::build_default();
        config.crypto.master_key = FILE_KEY.to_owned();
        config
    }

    #[test]
    fn missing_master_key_env_preserves_file_value() {
        let mut config = file_config();
        config.apply_env_overrides(|_| None).unwrap();
        assert_eq!(config.crypto.master_key, FILE_KEY);
    }

    #[test]
    fn empty_master_key_env_preserves_file_value() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| (name == "IPFS_S3_MASTER_KEY").then(String::new))
            .unwrap();
        assert_eq!(config.crypto.master_key, FILE_KEY);
    }

    #[test]
    fn non_empty_master_key_env_replaces_file_value() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| {
                (name == "IPFS_S3_MASTER_KEY").then(|| ENV_KEY.to_owned())
            })
            .unwrap();
        assert_eq!(config.crypto.master_key, ENV_KEY);
    }

    #[tokio::test]
    async fn non_empty_invalid_master_key_fails_state_initialization() {
        let mut config = file_config();
        config
            .apply_env_overrides(|name| {
                (name == "IPFS_S3_MASTER_KEY").then(|| "not-hex".to_owned())
            })
            .unwrap();
        let result = crate::state::AppState::new(&config).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("invalid master key hex"));
    }
}
```

- [ ] **Step 2: Add failing location operation tests**

Append to `src/s3/ops/bucket.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;
    use std::collections::HashMap;

    async fn state_with_bucket() -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None).await.unwrap();
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            ).unwrap(),
        })
    }

    fn location_request(bucket: &str) -> S3Request<GetBucketLocationInput> {
        S3Request {
            input: GetBucketLocationInput {
                bucket: bucket.to_owned(),
                expected_bucket_owner: None,
            },
            method: http::Method::GET,
            uri: format!("/{bucket}?location").parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn get_bucket_location_returns_empty_constraint_for_us_east_1() {
        let output = get_bucket_location(&state_with_bucket().await, location_request("bucket"))
            .await.unwrap().output;
        assert_eq!(output.location_constraint, None);
    }

    #[tokio::test]
    async fn get_bucket_location_rejects_missing_bucket() {
        let error = get_bucket_location(&state_with_bucket().await, location_request("missing"))
            .await.unwrap_err();
        assert_eq!(error.code().as_str(), "NoSuchBucket");
        assert_eq!(error.message(), Some("bucket not found: missing"));
    }
}
```

- [ ] **Step 3: Run RED tests**

```powershell
cargo test --lib master_key_env -- --nocapture
cargo test --lib get_bucket_location_ -- --nocapture
```

Expected: compile failures name `apply_env_overrides` and `get_bucket_location`; no unrelated baseline failure.

- [ ] **Step 4: Implement testable environment overlays**

Add after `Config::build_default`:

```rust
    fn apply_env_overrides<F>(&mut self, get_env: F) -> anyhow::Result<()>
    where
        F: Fn(&str) -> Option<String>,
    {
        if let Some(bind) = get_env("IPFS_S3_BIND") {
            self.server.bind = bind.parse()?;
        }
        if let Some(rpc_url) = get_env("IPFS_S3_KUBO_RPC_URL") {
            self.kubo.rpc_url = rpc_url;
        }
        if let Some(database_url) = get_env("IPFS_S3_DATABASE_URL") {
            self.storage.database_url = database_url;
        }
        if let (Some(access_key), Some(secret_key)) = (
            get_env("IPFS_S3_ACCESS_KEY_ID"),
            get_env("IPFS_S3_SECRET_ACCESS_KEY"),
        ) && !access_key.is_empty() && !secret_key.is_empty() {
            self.auth.credentials = vec![Credential { access_key, secret_key }];
        }
        if let Some(master_key) = get_env("IPFS_S3_MASTER_KEY").filter(|value| !value.is_empty()) {
            self.crypto.master_key = master_key;
        }
        Ok(())
    }
```

Replace the old inline environment block at the end of `load` with:

```rust
        config.apply_env_overrides(|name| std::env::var(name).ok())?;
        Ok(config)
```

Retain precedence documentation and state that an empty master-key value is ignored.

- [ ] **Step 5: Implement GetBucketLocation and delegate**

Add after `head_bucket` in `src/s3/ops/bucket.rs`:

```rust
pub async fn get_bucket_location(
    state: &Arc<AppState>,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    let bucket = &req.input.bucket;
    if !crate::store::bucket::exists(state.store.db(), bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }
    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint: None,
    }))
}
```

Add after `head_bucket` in `src/s3/handler.rs`:

```rust
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        super::ops::bucket::get_bucket_location(&self.state, req).await
    }
```

- [ ] **Step 6: Add tracked Docker configuration and healthy startup wiring**

Create `config.docker.toml`:

```toml
[server]
bind = "0.0.0.0:9000"

[kubo]
rpc_url = "http://kubo:5001"

[storage]
database_url = "sqlite:///data/ipfs-s3.db?mode=rwc"

[[auth.credentials]]
access_key = "test"
secret_key = "test"

[crypto]
# Development-only key. Supply a non-empty IPFS_S3_MASTER_KEY in production.
master_key = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"

[pinning]
provider = "noop"
```

Set `config.example.toml` storage to:

```toml
[storage]
database_url = "sqlite:///data/ipfs-s3.db?mode=rwc"
```

In `docker-compose.yml`, mount `./config.docker.toml:/data/config.toml:ro` instead of `./config.toml`. Add this to `gateway` after `depends_on`; the command is `/bin/sh` inside Linux, not a host command:

```yaml
    healthcheck:
      test: ["CMD-SHELL", "while read -r _ local _; do if [ \"$$local\" = \"00000000:2328\" ]; then exit 0; fi; done < /proc/net/tcp; exit 1"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 10s
```

Replace cloudflared's list dependency with:

```yaml
    depends_on:
      gateway:
        condition: service_healthy
```

Do not inspect or edit ignored `config.toml`.

- [ ] **Step 7: Run GREEN and static Compose checks**

```powershell
cargo test --lib master_key_env -- --nocapture
cargo test --lib get_bucket_location_ -- --nocapture
docker compose -f docker-compose.yml config --quiet
$renderedCompose = (docker compose -f docker-compose.yml config) -join "`n"; if ($LASTEXITCODE -ne 0) { throw "docker compose config failed with exit code $LASTEXITCODE" }; if (-not $renderedCompose.Contains('$$local')) { throw 'Compose config output must preserve the escaped $$local token for container-side shell expansion' }
```

Expected: matching Rust tests pass; both Compose render commands exit 0 without starting/building/pulling anything; the source and Compose 2.39.2 rendered output contain escaped `$$local`. Task 6's real healthy-container result proves Compose passes a usable `$local` expression to the container shell.

- [ ] **Step 8: Checkpoint without committing**

```powershell
git diff -- src/config.rs src/s3/handler.rs src/s3/ops/bucket.rs docker-compose.yml config.example.toml config.docker.toml
git status --short
```

Expected: only intended files plus approved spec/plan; `config.toml` is absent. Do not stage or commit.

---

### Task 2: Shared listing page and ListObjects v1

**Files:**
- Modify: `src/s3/handler.rs:80-100`
- Modify: `src/s3/ops/object.rs:615-903`
- Test: `src/s3/ops/object.rs:946-1407`

**Interfaces:**
- Consumes: strict `store::object::list<C: ConnectionTrait>(&C, &str, Option<&str>, Option<&str>, u64) -> AppResult<Vec<object::Model>>`, current v2 builder/tests, `ListObjectsInput/Output`, `ListObjectsV2Input/Output`.
- Produces: `ListingRequest<'a>`, `ListingEntry`, `ListingPage`, `ListingPageBuilder`, `build_listing_page`, `listing_dtos`, `list_objects`, preserved `list_objects_v2`, and `S3Impl::list_objects`.

- [ ] **Step 1: Add List v1 request helper and failing operation tests**

Add after `list_v2_request` in the existing `src/s3/ops/object.rs` test module:

```rust
    fn list_v1_request(input: ListObjectsInput) -> S3Request<ListObjectsInput> {
        S3Request {
            input,
            method: http::Method::GET,
            uri: http::Uri::from_static("/bucket"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }
```

Add these tests before the v2 tests:

```rust
    #[tokio::test]
    async fn list_objects_v1_marker_is_exclusive_and_fields_are_echoed() {
        let state = list_state_with_keys(&["a", "b", "c"]).await;
        let output = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some(String::new()),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                marker: Some("a".to_owned()),
                max_keys: Some(1),
                prefix: Some(String::new()),
                ..Default::default()
            }),
        ).await.unwrap().output;

        assert_eq!(
            output.contents.as_ref().unwrap().iter()
                .filter_map(|object| object.key.as_deref()).collect::<Vec<_>>(),
            vec!["b"]
        );
        assert_eq!(output.name.as_deref(), Some("bucket"));
        assert_eq!(output.prefix.as_deref(), Some(""));
        assert_eq!(output.delimiter.as_deref(), Some(""));
        assert_eq!(output.marker.as_deref(), Some("a"));
        assert_eq!(output.max_keys, Some(1));
        assert_eq!(output.encoding_type.as_ref().map(EncodingType::as_str), Some("url"));
        assert_eq!(output.is_truncated, Some(true));
        assert_eq!(output.next_marker.as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn list_objects_v1_untruncated_page_omits_next_marker() {
        let state = list_state_with_keys(&["a", "b"]).await;
        let output = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                marker: Some("a".to_owned()),
                max_keys: Some(1000),
                ..Default::default()
            }),
        ).await.unwrap().output;
        assert_eq!(output.is_truncated, Some(false));
        assert_eq!(output.next_marker, None);
        assert_eq!(
            output.contents.unwrap().into_iter().filter_map(|object| object.key).collect::<Vec<_>>(),
            vec!["b"]
        );
    }

    #[tokio::test]
    async fn list_objects_v1_delimiter_next_marker_tracks_last_consumed_row() {
        let state = list_state_with_keys(&["a", "photos/1", "photos/2", "videos/1"]).await;
        let first = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some("/".to_owned()),
                max_keys: Some(2),
                ..Default::default()
            }),
        ).await.unwrap().output;
        assert_eq!(
            first.contents.as_ref().unwrap().iter()
                .filter_map(|object| object.key.as_deref()).collect::<Vec<_>>(),
            vec!["a"]
        );
        assert_eq!(
            first.common_prefixes.as_ref().unwrap().iter()
                .filter_map(|prefix| prefix.prefix.as_deref()).collect::<Vec<_>>(),
            vec!["photos/"]
        );
        assert_eq!(first.is_truncated, Some(true));
        assert_eq!(first.next_marker.as_deref(), Some("photos/2"));

        let second = list_objects(
            &state,
            list_v1_request(ListObjectsInput {
                bucket: "bucket".to_owned(),
                delimiter: Some("/".to_owned()),
                marker: first.next_marker,
                max_keys: Some(2),
                ..Default::default()
            }),
        ).await.unwrap().output;
        assert_eq!(
            second.common_prefixes.unwrap().into_iter()
                .filter_map(|prefix| prefix.prefix).collect::<Vec<_>>(),
            vec!["videos/"]
        );
        assert_eq!(second.is_truncated, Some(false));
        assert_eq!(second.next_marker, None);
    }

    #[tokio::test]
    async fn list_objects_v2_continuation_token_still_precedes_start_after() {
        let state = list_state_with_keys(&["a", "b", "c", "d"]).await;
        let output = list_objects_v2(
            &state,
            list_v2_request(ListObjectsV2Input {
                bucket: "bucket".to_owned(),
                continuation_token: Some("b".to_owned()),
                start_after: Some("c".to_owned()),
                max_keys: Some(1000),
                ..Default::default()
            }),
        ).await.unwrap().output;
        assert_eq!(
            output.contents.unwrap().into_iter().filter_map(|object| object.key).collect::<Vec<_>>(),
            vec!["c", "d"]
        );
        assert_eq!(output.continuation_token.as_deref(), Some("b"));
        assert_eq!(output.start_after.as_deref(), Some("c"));
    }
```

- [ ] **Step 2: Run RED**

```powershell
cargo test --lib list_objects_v1_ -- --nocapture
```

Expected: compile failure because `list_objects` is undefined; existing v2 tests are not the intended failure.

- [ ] **Step 3: Generalize the current v2 builder and add both DTO adapters**

Replace `src/s3/ops/object.rs` from `pub async fn list_objects_v2` through `fold_list_objects_v2_rows` with this current-HEAD-based implementation:

```rust
use crate::store::entities::object;

struct ListingRequest<'a> {
    bucket: &'a str,
    prefix: &'a str,
    delimiter: Option<&'a str>,
    cursor: Option<&'a str>,
    max_keys: usize,
}

#[derive(Clone, Debug)]
enum ListingEntry {
    Object(object::Model),
    CommonPrefix { prefix: String, continuation_key: String },
}

#[derive(Clone, Debug)]
struct ListingPage {
    entries: Vec<ListingEntry>,
    is_truncated: bool,
    next_cursor: Option<String>,
}

fn normalized_max_keys(value: Option<i32>) -> usize {
    value.unwrap_or(1000).clamp(1, 1000) as usize
}

async fn build_listing_page(
    state: &Arc<AppState>,
    request: ListingRequest<'_>,
) -> S3Result<ListingPage> {
    let db = state.store.db();
    if !crate::store::bucket::exists(db, request.bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", request.bucket));
    }
    let mut cursor = request.cursor.filter(|value| !value.is_empty()).map(str::to_owned);
    let prefix_filter = (!request.prefix.is_empty()).then_some(request.prefix);
    let delimiter = request.delimiter.filter(|value| !value.is_empty());
    let batch_limit = (request.max_keys as u64 + 1).max(1000);
    let mut builder = ListingPageBuilder::new(request.prefix, delimiter, request.max_keys);

    'paging: loop {
        let rows = crate::store::object::list(
            db, request.bucket, prefix_filter, cursor.as_deref(), batch_limit,
        ).await?;
        let exhausted = rows.len() < batch_limit as usize;
        for row in rows {
            let row_key = row.key.clone();
            if builder.push_row(row) == PushListEntryResult::PageComplete {
                break 'paging;
            }
            cursor = Some(row_key);
        }
        if exhausted {
            break;
        }
    }
    Ok(builder.finish())
}

fn listing_dtos(entries: &[ListingEntry]) -> (Vec<Object>, Vec<CommonPrefix>) {
    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in entries {
        match entry {
            ListingEntry::Object(model) => contents.push(Object {
                key: Some(model.key.clone()),
                size: Some(model.size),
                e_tag: Some(ETag::Strong(model.etag.clone())),
                last_modified: Some(Timestamp::from(SystemTime::from(model.created_at))),
                ..Default::default()
            }),
            ListingEntry::CommonPrefix { prefix, .. } => common_prefixes.push(CommonPrefix {
                prefix: Some(prefix.clone()),
            }),
        }
    }
    (contents, common_prefixes)
}

pub async fn list_objects(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsInput>,
) -> S3Result<S3Response<ListObjectsOutput>> {
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let marker = req.input.marker.clone();
    let encoding_type = req.input.encoding_type.clone();
    let max_keys = normalized_max_keys(req.input.max_keys);
    let page = build_listing_page(state, ListingRequest {
        bucket: &bucket,
        prefix: prefix.as_deref().unwrap_or(""),
        delimiter: delimiter.as_deref(),
        cursor: marker.as_deref(),
        max_keys,
    }).await?;
    let next_marker = page.is_truncated.then(|| page.next_cursor.clone()).flatten();
    let (contents, common_prefixes) = listing_dtos(&page.entries);
    Ok(S3Response::new(ListObjectsOutput {
        name: Some(bucket),
        prefix: Some(prefix.unwrap_or_default()),
        marker,
        max_keys: Some(max_keys as i32),
        is_truncated: Some(page.is_truncated),
        contents: Some(contents),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        delimiter,
        next_marker,
        encoding_type,
        request_charged: None,
    }))
}

pub async fn list_objects_v2(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let bucket = req.input.bucket.clone();
    let prefix = req.input.prefix.clone();
    let delimiter = req.input.delimiter.clone();
    let encoding_type = req.input.encoding_type.clone();
    let start_after = req.input.start_after.clone();
    let continuation_token = req.input.continuation_token.clone();
    let max_keys = normalized_max_keys(req.input.max_keys);
    let cursor = continuation_token.as_deref().filter(|value| !value.is_empty())
        .or_else(|| start_after.as_deref().filter(|value| !value.is_empty()));
    let page = build_listing_page(state, ListingRequest {
        bucket: &bucket,
        prefix: prefix.as_deref().unwrap_or(""),
        delimiter: delimiter.as_deref(),
        cursor,
        max_keys,
    }).await?;
    let (contents, common_prefixes) = listing_dtos(&page.entries);
    Ok(S3Response::new(ListObjectsV2Output {
        contents: Some(contents),
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        is_truncated: Some(page.is_truncated),
        continuation_token,
        next_continuation_token: page.next_cursor,
        key_count: Some(page.entries.len() as i32),
        max_keys: Some(max_keys as i32),
        name: Some(bucket),
        prefix: Some(prefix.unwrap_or_default()),
        delimiter,
        encoding_type,
        start_after,
        ..Default::default()
    }))
}

fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|value| !value.is_empty())?;
    let rest = key.strip_prefix(prefix)?;
    let index = rest.find(delimiter)?;
    Some(format!("{}{}", prefix, &rest[..index + delimiter.len()]))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushListEntryResult { Continue, PageComplete }

struct ListingPageBuilder {
    prefix: String,
    delimiter: Option<String>,
    max_keys: usize,
    entries: Vec<ListingEntry>,
    common_prefix_positions: HashMap<String, usize>,
    last_consumed_key: Option<String>,
    is_truncated: bool,
}

impl ListingPageBuilder {
    fn new(prefix: &str, delimiter: Option<&str>, max_keys: usize) -> Self {
        Self {
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(str::to_owned),
            max_keys,
            entries: Vec::new(),
            common_prefix_positions: HashMap::new(),
            last_consumed_key: None,
            is_truncated: false,
        }
    }

    fn push_row(&mut self, row: object::Model) -> PushListEntryResult {
        let key = row.key.clone();
        if let Some(common_prefix) =
            common_prefix_for_key(&key, &self.prefix, self.delimiter.as_deref())
        {
            if let Some(&position) = self.common_prefix_positions.get(&common_prefix) {
                if let ListingEntry::CommonPrefix { ref mut continuation_key, .. } = self.entries[position] {
                    *continuation_key = key.clone();
                }
                self.last_consumed_key = Some(key);
                return PushListEntryResult::Continue;
            }
            if self.entries.len() >= self.max_keys {
                self.is_truncated = true;
                return PushListEntryResult::PageComplete;
            }
            let position = self.entries.len();
            self.entries.push(ListingEntry::CommonPrefix {
                prefix: common_prefix.clone(),
                continuation_key: key.clone(),
            });
            self.common_prefix_positions.insert(common_prefix, position);
            self.last_consumed_key = Some(key);
            return PushListEntryResult::Continue;
        }
        if self.entries.len() >= self.max_keys {
            self.is_truncated = true;
            return PushListEntryResult::PageComplete;
        }
        self.entries.push(ListingEntry::Object(row));
        self.last_consumed_key = Some(key);
        PushListEntryResult::Continue
    }

    fn finish(mut self) -> ListingPage {
        let next_cursor = if self.is_truncated { self.last_consumed_key.take() } else { None };
        ListingPage { entries: self.entries, is_truncated: self.is_truncated, next_cursor }
    }
}

#[cfg(test)]
fn fold_listing_rows(
    rows: Vec<object::Model>, prefix: &str, delimiter: Option<&str>, max_keys: usize,
) -> ListingPage {
    let mut builder = ListingPageBuilder::new(prefix, delimiter, max_keys);
    for row in rows {
        if builder.push_row(row) == PushListEntryResult::PageComplete { break; }
    }
    builder.finish()
}
```

Replace the old test accessor impl with:

```rust
#[cfg(test)]
impl ListingPage {
    fn object_keys(&self) -> Vec<&str> {
        self.entries.iter().filter_map(|entry| match entry {
            ListingEntry::Object(model) => Some(model.key.as_str()),
            ListingEntry::CommonPrefix { .. } => None,
        }).collect()
    }
    fn common_prefixes(&self) -> Vec<&str> {
        self.entries.iter().filter_map(|entry| match entry {
            ListingEntry::Object(_) => None,
            ListingEntry::CommonPrefix { prefix, .. } => Some(prefix.as_str()),
        }).collect()
    }
}
```

Replace the four old pure folding tests with this exact shared-helper suite. Do not leave a second v2-only builder.

```rust
    #[test]
    fn listing_fold_without_delimiter_returns_flat_objects() {
        let rows = vec![object_model("a.txt"), object_model("b.txt"), object_model("c.txt")];
        let page = fold_listing_rows(rows, "", None, 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt", "c.txt"]);
        assert!(page.common_prefixes().is_empty());
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_with_delimiter_returns_objects_and_common_prefixes() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("b.txt"),
        ];
        let page = fold_listing_rows(rows, "", Some("/"), 1000);
        assert_eq!(page.object_keys(), vec!["a.txt", "b.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_with_prefix_and_delimiter_scopes_common_prefixes() {
        let rows = vec![
            object_model("photos/2024/jan.jpg"),
            object_model("photos/2024/feb.jpg"),
            object_model("photos/2025/mar.jpg"),
        ];
        let page = fold_listing_rows(rows, "photos/", Some("/"), 1000);
        assert!(page.object_keys().is_empty());
        assert_eq!(page.common_prefixes(), vec!["photos/2024/", "photos/2025/"]);
        assert!(!page.is_truncated);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn listing_fold_counts_prefix_once_and_tracks_last_consumed_row() {
        let rows = vec![
            object_model("a.txt"),
            object_model("photos/cat.jpg"),
            object_model("photos/dog.jpg"),
            object_model("videos/clip.mp4"),
        ];
        let page = fold_listing_rows(rows, "", Some("/"), 2);
        assert_eq!(page.object_keys(), vec!["a.txt"]);
        assert_eq!(page.common_prefixes(), vec!["photos/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_cursor.as_deref(), Some("photos/dog.jpg"));
    }
```

- [ ] **Step 4: Add the List v1 trait delegate**

Add immediately before `list_objects_v2` in `src/s3/handler.rs`:

```rust
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        super::ops::object::list_objects(&self.state, req).await
    }
```

- [ ] **Step 5: Run GREEN v1/shared/v2 tests**

```powershell
cargo test --lib listing_fold_ -- --nocapture
cargo test --lib list_objects_v1_ -- --nocapture
cargo test --lib list_objects_v2_ -- --nocapture
```

Expected: all pass; v1 `NextMarker=photos/2`, no duplicate/omission, and v2 token precedence/folding remain correct.

- [ ] **Step 6: Checkpoint without committing**

```powershell
git diff -- src/s3/handler.rs src/s3/ops/object.rs
git status --short
```

Expected: one `build_listing_page` path and no obsolete `ListObjectsV2PageBuilder`. Do not stage or commit.

---

### Task 2a: `encoding-type=url` wire projection without cursor mutation

**Files:**
- Modify: `src/s3/ops/object.rs`
- Modify: `tests/integration.rs`

**Boundary:** `ListingPage`, `ListingEntry`, DB rows, v1 marker lookup, and v2 continuation/start-after lookup stay raw. Only DTO strings are projected after `build_listing_page` returns. `Name`, `continuation_token`, and `next_continuation_token` are not projected.

- [ ] **Step 1: Add RED unit tests**

Add `rfc3986_url_encoding_uses_utf8_uppercase_hex_and_unreserved_passthrough` to the existing `object.rs` module. Its required assertion is:

```rust
assert_eq!(
    rfc3986_url_encode("AZaz09-._~/ %()é"),
    "AZaz09-._~%2F%20%25%28%29%C3%A9"
);
```

Add `list_objects_url_encoding_projects_wire_fields_without_changing_raw_cursors`. Seed `prefix/a%2F(é)`, `prefix/dir%2F(é)/one`, and `prefix/z`; request v1 with `prefix=prefix/`, `delimiter=/`, raw `marker=prefix/`, `max_keys=2`, and `EncodingType::URL`. Assert encoded `Prefix`, `Delimiter`, `Marker`, contents key, common prefix, and `NextMarker`; request the second page with raw marker `prefix/dir%2F(é)/one` and assert only encoded `prefix%2Fz` remains. For v2 assert the same projected contents/common prefix plus projected `StartAfter`, but raw `ContinuationToken` and `NextContinuationToken`.

Run:

```powershell
cargo test --lib rfc3986_url_encoding_uses_utf8_uppercase_hex_and_unreserved_passthrough -- --nocapture
cargo test --lib list_objects_url_encoding_projects_wire_fields_without_changing_raw_cursors -- --nocapture
```

Expected RED: the encoder symbol is absent and existing list DTOs return raw values.

- [ ] **Step 2: Implement the DTO-only projection**

Add these helpers immediately before `listing_dtos`:

```rust
fn rfc3986_url_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn url_encoding_requested(encoding_type: Option<&EncodingType>) -> bool {
    encoding_type.is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL)
}

fn project_listing_field(value: &str, url_encode: bool) -> String {
    if url_encode { rfc3986_url_encode(value) } else { value.to_owned() }
}

fn project_optional_listing_field(value: Option<String>, url_encode: bool) -> Option<String> {
    value.map(|value| project_listing_field(&value, url_encode))
}
```

Pass `url_encode` into `listing_dtos`, projecting `Object.key` and `CommonPrefix.prefix`. In `list_objects`, compute the flag after cloning input fields but before constructing the output; project only output `prefix`, `delimiter`, `marker`, and `next_marker`. In `list_objects_v2`, project only output `prefix`, `delimiter`, and `start_after`; leave both continuation fields untouched. The raw cloned v1 marker and v2 cursor must enter `build_listing_page` before any projection.

- [ ] **Step 3: Add RED real TCP/SigV4 wire coverage**

Add `test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination` to `tests/integration.rs`. Use `start_harness` and `send_sigv4`, not an in-process handler. Seed the same three raw keys; issue signed v1 and v2 requests with `encoding-type=url`. Assert XML `<Name>` stays `test-bkt`, required encoded fields contain `prefix%2F`, `%252F`, `%28`, `%29`, `%C3%A9`, and the v2 XML continuation tokens remain raw. Issue the second v1 signed request using raw `marker=prefix/dir%2F(é)/one`; assert XML contains only `prefix%2Fz` with no `NextMarker`.

Run:

```powershell
cargo test --test integration test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination -- --nocapture
```

Expected RED before Step 2: XML response fields are raw (for example `<Prefix>prefix/</Prefix>`). Expected GREEN after Step 2: the exact encoded XML assertions pass and the raw second-page marker prevents replay.

- [ ] **Step 4: GREEN regression commands**

```powershell
cargo test --lib rfc3986_url_encoding_uses_utf8_uppercase_hex_and_unreserved_passthrough -- --nocapture
cargo test --lib list_objects_url_encoding_projects_wire_fields_without_changing_raw_cursors -- --nocapture
cargo test --test integration test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination -- --nocapture
```

### Task 3: Idempotent store delete and DeleteObjects

**Files:**
- Modify: `src/store/object.rs:136-150,192-258`
- Modify: `src/s3/handler.rs:70-90`
- Modify: `src/s3/ops/object.rs:538-551` and tests

**Interfaces:**
- Consumes: strict `delete_latest` behavior for single `DeleteObject`, `DeleteObjectsInput.delete.objects`, `Delete.quiet`, `DeletedObject`, `s3s::dto::Error`.
- Produces: `delete_latest_if_present<C: ConnectionTrait>(&C, &str, &str) -> AppResult<bool>`, unchanged strict `delete_latest<C: ConnectionTrait>(&C, &str, &str) -> AppResult<()>`, `delete_objects(&Arc<AppState>, S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>>`, and `S3Impl::delete_objects`.

- [ ] **Step 1: Add failing store tests for idempotent deletion**

Append inside `src/store/object.rs` tests:

```rust
    async fn seed_key(db: &sea_orm::DatabaseConnection, key: &str) {
        upsert(
            db,
            &format!("id-{key}"),
            "test-bucket",
            key,
            &format!("cid-{key}"),
            4,
            None,
            &format!("cid-{key}"),
            None,
            false,
            None,
            false,
        ).await.unwrap();
    }

    #[tokio::test]
    async fn delete_latest_if_present_is_idempotent() {
        let db = setup().await;
        seed_key(&db, "a.txt").await;
        assert!(delete_latest_if_present(&db, "test-bucket", "a.txt").await.unwrap());
        assert!(!delete_latest_if_present(&db, "test-bucket", "a.txt").await.unwrap());
        assert!(get_latest(&db, "test-bucket", "a.txt").await.is_err());
    }

    #[tokio::test]
    async fn strict_delete_latest_still_rejects_missing_key() {
        let db = setup().await;
        let error = delete_latest(&db, "test-bucket", "missing").await.unwrap_err();
        assert!(matches!(error, AppError::NoSuchKey(_)));
    }
```

- [ ] **Step 2: Add failing DeleteObjects DTO tests**

Add to `src/s3/ops/object.rs` tests:

```rust
    fn delete_objects_request(keys: &[&str], quiet: Option<bool>) -> S3Request<DeleteObjectsInput> {
        S3Request {
            input: DeleteObjectsInput {
                bucket: "bucket".to_owned(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: Delete {
                    objects: keys.iter().map(|key| ObjectIdentifier {
                        key: (*key).to_owned(),
                        ..Default::default()
                    }).collect(),
                    quiet,
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            },
            method: http::Method::POST,
            uri: http::Uri::from_static("/bucket?delete"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn delete_objects_reports_existing_missing_and_duplicate_keys_as_ordered_successes() {
        let state = list_state_with_keys(&["a", "b"]).await;
        let output = delete_objects(
            &state,
            delete_objects_request(&["a", "missing", "a", "b"], None),
        ).await.unwrap().output;
        assert_eq!(
            output.deleted.unwrap().into_iter().filter_map(|item| item.key).collect::<Vec<_>>(),
            vec!["a", "missing", "a", "b"]
        );
        assert!(output.errors.is_none());
        assert!(crate::store::object::get_latest(state.store.db(), "bucket", "a").await.is_err());
        assert!(crate::store::object::get_latest(state.store.db(), "bucket", "b").await.is_err());
    }

    #[tokio::test]
    async fn delete_objects_quiet_executes_deletes_and_omits_deleted_entries() {
        let state = list_state_with_keys(&["a", "b"]).await;
        let output = delete_objects(
            &state,
            delete_objects_request(&["a", "missing", "b"], Some(true)),
        ).await.unwrap().output;
        assert!(output.deleted.is_none());
        assert!(output.errors.is_none());
        assert!(crate::store::object::get_latest(state.store.db(), "bucket", "a").await.is_err());
        assert!(crate::store::object::get_latest(state.store.db(), "bucket", "b").await.is_err());
    }

    #[tokio::test]
    async fn delete_objects_missing_bucket_is_request_level_error() {
        let state = list_state_with_keys(&[]).await;
        let mut request = delete_objects_request(&["a"], None);
        request.input.bucket = "missing".to_owned();
        let error = delete_objects(&state, request).await.unwrap_err();
        assert_eq!(error.code().as_str(), "NoSuchBucket");
    }
```

- [ ] **Step 3: Run RED tests**

```powershell
cargo test --lib delete_latest_if_present -- --nocapture
cargo test --lib delete_objects_ -- --nocapture
```

Expected: missing helper and operation cause compile failures; strict single-delete test remains valid once compilation reaches it.

- [ ] **Step 4: Implement idempotent store deletion and preserve strict single deletion**

Replace `delete_latest` in `src/store/object.rs` with:

```rust
pub async fn delete_latest_if_present<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<bool> {
    let result = object::Entity::update_many()
        .col_expr(object::Column::IsLatest, false.into())
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;
    Ok(result.rows_affected > 0)
}

pub async fn delete_latest<C: ConnectionTrait>(db: &C, bucket: &str, key: &str) -> AppResult<()> {
    if !delete_latest_if_present(db, bucket, key).await? {
        return Err(AppError::NoSuchKey(format!("{bucket}/{key}")));
    }
    Ok(())
}
```

- [ ] **Step 5: Implement ordered, quiet-aware, partially successful DeleteObjects**

Add after `delete_object` in `src/s3/ops/object.rs`:

```rust
pub async fn delete_objects(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let input = req.input;
    let bucket = input.bucket;
    let db = state.store.db();
    if !crate::store::bucket::exists(db, &bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    let quiet = input.delete.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for identifier in input.delete.objects {
        let key = identifier.key;
        match crate::store::object::delete_latest_if_present(db, &bucket, &key).await {
            Ok(_) => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key: Some(key),
                        ..Default::default()
                    });
                }
            }
            Err(error) => {
                tracing::error!(bucket = %bucket, key = %key, error = %error, "batch delete item failed");
                errors.push(Error {
                    code: Some("InternalError".to_owned()),
                    key: Some(key),
                    message: Some("failed to delete object".to_owned()),
                    version_id: None,
                });
            }
        }
    }

    Ok(S3Response::new(DeleteObjectsOutput {
        deleted: (!quiet && !deleted.is_empty()).then_some(deleted),
        errors: (!errors.is_empty()).then_some(errors),
        request_charged: None,
    }))
}
```

The requested `version_id` is intentionally ignored: v0.2 has no version selection or delete markers. The generic response message prevents DB details from leaking.

Add after `delete_object` in `src/s3/handler.rs`:

```rust
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        super::ops::object::delete_objects(&self.state, req).await
    }
```

- [ ] **Step 6: Run GREEN delete tests and no-unpin source guard**

```powershell
cargo test --lib delete_latest -- --nocapture
cargo test --lib delete_objects_ -- --nocapture
rg -n "pin_rm|/api/v0/pin/rm" src/store/object.rs src/s3/ops/object.rs
```

Expected: tests pass; `rg` may show pre-existing negative-test assertions elsewhere in `object.rs`, but shows no `pin_rm` call in either new delete implementation.

- [ ] **Step 7: Checkpoint without committing**

```powershell
git diff -- src/store/object.rs src/s3/handler.rs src/s3/ops/object.rs
git status --short
```

Expected: strict `DeleteObject` still maps a missing key to `NoSuchKey`; batch delete uses the independent bool-returning helper. Do not stage or commit.

---

### Task 4: Real TCP and SigV4 compatibility integration

**Files:**
- Modify: `tests/support/sigv4.rs:206-213,356-427`
- Modify: `tests/integration.rs:1-338`
- Reuse unchanged: `tests/support/decompress.rs:46-171`

**Interfaces:**
- Consumes: `start_harness(KuboScript) -> TestHarness`, `send_sigv4` over reqwest real TCP, `s3s::xml::Deserializer::new(&[u8])`, `<GetBucketLocationOutput as s3s::xml::Deserialize>::deserialize(&mut Deserializer)`, Tasks 1-3 operations, `ConnectionTrait::execute_unprepared`.
- Produces: canonical bucket path `/{bucket}`, `seed_latest`, XML body/header helpers, and `test_client_compat_*` wire tests for exact location DTO decoding, v1, delete, and nested HEAD.

- [ ] **Step 1: Add a failing bucket canonical-URI test**

Add inside `tests/support/sigv4.rs`'s existing test module:

```rust
#[test]
fn canonical_uri_omits_trailing_slash_for_bucket_operations() {
    assert_eq!(canonical_uri("test-bkt", ""), "/test-bkt");
    assert_eq!(
        canonical_uri("test-bkt", "nested/path/file.txt"),
        "/test-bkt/nested/path/file.txt"
    );
}
```

Run:

```powershell
cargo test --test integration canonical_uri_omits_trailing_slash_for_bucket_operations -- --nocapture
```

Expected: FAIL because the current empty-key path is `/test-bkt/`.

- [ ] **Step 2: Fix empty-key canonical paths without changing nested-key encoding**

Replace `canonical_uri` with:

```rust
fn canonical_uri(bucket: &str, key: &str) -> String {
    let bucket = rfc3986_encode(bucket);
    if key.is_empty() {
        return format!("/{bucket}");
    }
    let key = key.split('/').map(rfc3986_encode).collect::<Vec<_>>().join("/");
    format!("/{bucket}/{key}")
}
```

- [ ] **Step 3: Add deterministic DB, XML, and DeleteObjects request helpers**

Add after `kubo_query_args` in `tests/integration.rs`:

```rust
async fn seed_latest(harness: &TestHarness, key: &str, cid: &str, size: i64) {
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        size,
        Some("text/plain"),
        cid,
        None,
        false,
        None,
        false,
    ).await.expect("seed latest object");
}

fn xml_sections(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    let mut values = Vec::new();
    while let Some(start) = rest.find(&open) {
        let content = &rest[start + open.len()..];
        let Some(end) = content.find(&close) else { break };
        values.push(content[..end].to_owned());
        rest = &content[end + close.len()..];
    }
    values
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_sections(xml, tag).into_iter().next()
}

fn delete_xml(keys: &[&str], quiet: bool) -> Vec<u8> {
    let objects = keys.iter().map(|key| format!("<Object><Key>{key}</Key></Object>"))
        .collect::<String>();
    format!(
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{objects}<Quiet>{quiet}</Quiet></Delete>"
    ).into_bytes()
}

fn delete_headers(body: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(body).0);
    headers.insert("content-md5", HeaderValue::from_str(&digest).unwrap());
    headers
}

async fn signed_delete_objects(
    harness: &TestHarness,
    keys: &[&str],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_xml(keys, quiet);
    send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    ).await
}
```

These helpers use fixed test keys without XML metacharacters, so string extraction is sufficient and does not introduce another XML dependency.

- [ ] **Step 4: Add GetBucketLocation and List v1 real-wire tests**

Add before the decompress-zip test section:

```rust
#[tokio::test]
async fn test_client_compat_get_bucket_location_is_standard_us_east_1() {
    let harness = start_harness(standard_script(0)).await;
    let raw = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    ).await;
    assert_eq!(raw.status(), StatusCode::OK);
    let body = raw.bytes().await.expect("GetBucketLocation body");
    let mut deserializer = s3s::xml::Deserializer::new(body.as_ref());
    let decoded = <s3s::dto::GetBucketLocationOutput as s3s::xml::Deserialize>::deserialize(
        &mut deserializer,
    )
    .expect("decode GetBucketLocationOutput with s3s 0.14 restXml");
    deserializer.expect_eof().expect("GetBucketLocation XML EOF");
    assert_eq!(decoded.location_constraint, None);

    let body_text = std::str::from_utf8(body.as_ref()).expect("GetBucketLocation UTF-8 XML");
    assert!(body_text.contains("<LocationConstraint"), "{body_text}");
    assert!(!body_text.contains("us-east-1"), "{body_text}");

    let missing = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    ).await;
    assert_s3_error(
        missing,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    ).await;
}

#[tokio::test]
async fn test_client_compat_list_v1_delimiter_marker_pages_without_replay() {
    let harness = start_harness(standard_script(0)).await;
    for key in ["a", "photos/1", "photos/2", "videos/1"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delimiter", "/"), ("max-keys", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    ).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.text().await.unwrap();
    assert_eq!(xml_sections(&first_body, "Key"), vec!["a"]);
    assert_eq!(
        xml_sections(&first_body, "Prefix").into_iter().filter(|value| !value.is_empty()).collect::<Vec<_>>(),
        vec!["photos/"]
    );
    assert_eq!(xml_text(&first_body, "IsTruncated").as_deref(), Some("true"));
    let marker = xml_text(&first_body, "NextMarker").expect("NextMarker");
    assert_eq!(marker, "photos/2");

    let second = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delimiter", "/"), ("marker", marker.as_str()), ("max-keys", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    ).await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = second.text().await.unwrap();
    assert!(xml_sections(&second_body, "Key").is_empty());
    assert_eq!(
        xml_sections(&second_body, "Prefix").into_iter().filter(|value| !value.is_empty()).collect::<Vec<_>>(),
        vec!["videos/"]
    );
    assert_eq!(xml_text(&second_body, "IsTruncated").as_deref(), Some("false"));
    assert!(xml_text(&second_body, "NextMarker").is_none());
}
```

The reqwest `send_sigv4` request proves a valid bucket-level request traverses the real `TcpListener` → axum → `s3s` SigV4 path. Its response is decoded by the locked `s3s 0.14.0` public restXml API and must yield `GetBucketLocationOutput { location_constraint: None }`, while the UTF-8 body must still contain no literal `us-east-1`. rust-s3 0.37.2 `Bucket::location()` encodes `?location` into an object path, so it is not used as this protocol proof.

API lock evidence for this code:

- [`s3s::xml::Deserializer::new(xml: &[u8])`](https://docs.rs/s3s/0.14.0/s3s/xml/struct.Deserializer.html#method.new) is public.
- [`s3s::xml::Deserialize::deserialize(&mut Deserializer) -> DeResult<Self>`](https://docs.rs/s3s/0.14.0/s3s/xml/trait.Deserialize.html#tymethod.deserialize) is public.
- [`s3s 0.14.0` implements `Deserialize` for `GetBucketLocationOutput`](https://docs.rs/s3s/0.14.0/src/s3s/xml/mod.rs.html#40-58), converting an empty `LocationConstraint` to `None`.

- [ ] **Step 5: Add normal, quiet, missing, duplicate, partial-error, and no-unpin wire tests**

Add:

```rust
#[tokio::test]
async fn test_client_compat_delete_objects_is_retry_safe_and_ordered() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "a", "QmA", 1).await;
    seed_latest(&harness, "b", "QmB", 1).await;
    let response = signed_delete_objects(&harness, &["a", "missing", "a", "b"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    let deleted = xml_sections(&body, "Deleted").into_iter()
        .map(|section| xml_text(&section, "Key").unwrap()).collect::<Vec<_>>();
    assert_eq!(deleted, vec!["a", "missing", "a", "b"]);
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "a").await;
    assert_latest_absent(&harness, "b").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_quiet_hides_successes() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "quiet", "QmQuiet", 5).await;
    let response = signed_delete_objects(&harness, &["quiet", "missing"], true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(xml_sections(&body, "Deleted").is_empty());
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "quiet").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_continues_after_store_error() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "before", "QmBefore", 6).await;
    seed_latest(&harness, "fail", "QmFail", 4).await;
    seed_latest(&harness, "after", "QmAfter", 5).await;
    harness.state.store.db().execute_unprepared(
        "CREATE TRIGGER fail_one_batch_delete BEFORE UPDATE OF is_latest ON objects \
         WHEN OLD.bucket = 'test-bkt' AND OLD.key = 'fail' AND NEW.is_latest = FALSE \
         BEGIN SELECT RAISE(FAIL, 'injected delete failure'); END"
    ).await.unwrap();

    let response = signed_delete_objects(&harness, &["before", "fail", "after"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    let deleted = xml_sections(&body, "Deleted").into_iter()
        .map(|section| xml_text(&section, "Key").unwrap()).collect::<Vec<_>>();
    assert_eq!(deleted, vec!["before", "after"]);
    let errors = xml_sections(&body, "Error");
    assert_eq!(errors.len(), 1);
    assert_eq!(xml_text(&errors[0], "Key").as_deref(), Some("fail"));
    assert_eq!(xml_text(&errors[0], "Code").as_deref(), Some("InternalError"));
    assert_eq!(xml_text(&errors[0], "Message").as_deref(), Some("failed to delete object"));
    assert_latest_absent(&harness, "before").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "fail")
        .await.expect("failed item remains latest");
    assert_latest_absent(&harness, "after").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}
```

- [ ] **Step 6: Strengthen nested localhost HEAD to explicit SigV4 metadata assertions**

Replace `test_head_object_signed_nested_key_succeeds` with:

```rust
#[tokio::test]
async fn test_client_compat_head_nested_key_signed_on_localhost() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "nested/path/file.txt", "QmNestedCid", 11).await;
    let response = send_sigv4(
        reqwest::Method::HEAD,
        &harness.endpoint,
        &harness.bucket,
        "nested/path/file.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    ).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(http::header::CONTENT_LENGTH).unwrap(), "11");
    assert!(response.headers().get(http::header::ETAG).unwrap().to_str().unwrap().contains("QmNestedCid"));
}
```

- [ ] **Step 7: Run the real-wire GREEN suite and existing v2 tests**

```powershell
cargo test --test integration canonical_uri_omits_trailing_slash_for_bucket_operations -- --nocapture
cargo test --test integration test_client_compat_ -- --nocapture
cargo test --test integration test_list_objects -- --nocapture
```

Expected: all pass. The targeted suite uses a real `TcpListener`, reqwest `send_sigv4` with actual `s3s` SigV4 authentication, exact `s3s 0.14.0` `GetBucketLocationOutput` decoding, HTTP XML serialization, SQLite trigger failure, and zero pin/rm calls. Its localhost-only nested HEAD is regression coverage, not ROADMAP item 9 evidence; only Task 5/6 real-client `dual_head=PASSED` can satisfy that hard gate.

- [ ] **Step 8: Checkpoint without committing**

```powershell
git diff -- tests/support/sigv4.rs tests/integration.rs
git status --short
```

Expected: `tests/support/decompress.rs` remains unchanged, no unsigned request substitutes for the new compatibility operations, and no source is staged/committed.

---

### Task 5: Docker client smoke, compatibility docs, and ROADMAP completion

**Files:**
- Create: `scripts/client-smoke.ps1`
- Create: `docs/client-compatibility.md`
- Modify: `ROADMAP.md:16-31`
- Read: `docker-compose.yml`, `config.docker.toml`

**Interfaces:**
- Consumes: healthy Compose stack, path-style `test/test` credentials, local images, Tasks 1-4 operations, explicit `-Run`/`-CleanupVolumes` operator intent.
- Produces: one safe smoke entry point, timestamped buckets, a portable temporary transcript whose output replaces `RunRoot` and `RepoRoot` with `<temp>` and `<repo>`, rclone/mc/AWS flows, Mc/AWS same-client localhost and `gateway:9000` signed stat/HEAD evidence, `RESULT` and final evidence references fixed to `client-smoke.log`, exact status lines, compatibility matrix, and conditionally completed client-artifact rows. Rclone runs only its own Compose-network operations and reports `PASSED/NOT_RUN`; it never calls Mc. The temporary transcript is not a tracked release receipt; Task 6 alone creates the approved actual evidence log.

- [ ] **Step 1: Prove the delivery files do not yet exist (RED artifact contract)**

Run:

```powershell
$required = @("scripts/client-smoke.ps1", "docs/client-compatibility.md", "config.docker.toml"); $missing = @($required | Where-Object { -not (Test-Path -LiteralPath $_) }); if ($missing.Count -gt 0) { throw "Missing v0.2 artifacts: $($missing -join ', ')" }
```

Expected before this task: FAIL listing at least the script and compatibility document. `config.docker.toml` already exists from Task 1.

- [ ] **Step 2: Create the exact PowerShell smoke runner**

Create `scripts/client-smoke.ps1` with this content:

```powershell
[CmdletBinding()]
param(
    [ValidateSet("All", "Rclone", "Mc", "Aws")]
    [string]$Client = "All",
    [switch]$Run,
    [switch]$CleanupVolumes
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path Env:IPFS_S3_MASTER_KEY)) { $env:IPFS_S3_MASTER_KEY = "" }
if (-not (Test-Path Env:CLOUDFLARE_TUNNEL_TOKEN)) { $env:CLOUDFLARE_TUNNEL_TOKEN = "" }
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$ComposeFile = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\docker-compose.yml"))
$Stamp = Get-Date -Format "yyyyMMddHHmmss"
$RunRoot = Join-Path ([IO.Path]::GetTempPath()) "ipfs-s3-client-smoke-$Stamp"
$null = New-Item -ItemType Directory -Path $RunRoot -Force
$LogPath = Join-Path $RunRoot "client-smoke.log"
$FixturePath = Join-Path $RunRoot "file.txt"
$FixtureText = "ipfs-s3-client-smoke-$Stamp"
$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$Stamp"
$EvidenceRepoRoot = "<repo>"
$EvidenceLogPath = "client-smoke.log"
[IO.File]::WriteAllText($FixturePath, $FixtureText, [Text.UTF8Encoding]::new($false))

function Convert-ToDockerDesktopPath {
    param([Parameter(Mandatory)][string]$Path)
    $fullPath = [IO.Path]::GetFullPath($Path)
    $drive = $fullPath.Substring(0, 1).ToLowerInvariant()
    $rest = $fullPath.Substring(2).Replace("\", "/")
    return "/run/desktop/mnt/host/$drive$rest"
}

$PortablePathReplacements = @(
    [pscustomobject]@{ Actual = $RunRoot; Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = $RunRoot.Replace("\", "/"); Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = (Convert-ToDockerDesktopPath $RunRoot); Portable = $EvidenceRunRoot },
    [pscustomobject]@{ Actual = $RepoRoot; Portable = $EvidenceRepoRoot },
    [pscustomobject]@{ Actual = $RepoRoot.Replace("\", "/"); Portable = $EvidenceRepoRoot },
    [pscustomobject]@{ Actual = (Convert-ToDockerDesktopPath $RepoRoot); Portable = $EvidenceRepoRoot }
)

function Convert-ToPortableEvidence {
    param([AllowNull()][AllowEmptyString()][string]$Text)
    if ($null -eq $Text) { return "" }
    $portable = $Text
    foreach ($replacement in $PortablePathReplacements) {
        $portable = $portable.Replace($replacement.Actual, $replacement.Portable)
    }
    return $portable
}

function Convert-NativeOutputItem {
    param([AllowNull()][object]$Item)
    if ($null -eq $Item) { return $null }
    if ($Item -is [System.Management.Automation.ErrorRecord]) {
        $message = $Item.Exception.Message
        if (-not [string]::IsNullOrWhiteSpace($message) -and
            $message -ne "System.Management.Automation.RemoteException") {
            return $message
        }
    }
    $text = $Item.ToString()
    if ($text -eq "System.Management.Automation.RemoteException") { return $null }
    return $text
}

$Images = @{
    Rclone = "rclone/rclone:1.74.4"
    Mc = "minio/mc:latest"
    Aws = "amazon/aws-cli:latest"
}
$ServiceImages = @(
    "ghcr.io/hugefiver/ipfs3-kubo:latest",
    "ghcr.io/hugefiver/ipfs3:latest"
)
$StandardBuildImages = @(
    "ipfs/kubo:latest",
    "rust:latest",
    "debian:trixie-slim"
)
$Selected = if ($Client -eq "All") { @("Rclone", "Mc", "Aws") } else { @($Client) }

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $displayCommand = Convert-ToPortableEvidence ("docker " + ($Arguments -join " "))
    Write-Host $displayCommand
    $output = @(& docker @Arguments 2>&1)
    $exitCode = $LASTEXITCODE
    $lines = @($output | ForEach-Object { Convert-NativeOutputItem $_ })
    $lines | ForEach-Object { Write-Host (Convert-ToPortableEvidence $_) }
    if ($exitCode -ne 0) {
        throw "docker exited ${exitCode}: $displayCommand"
    }
    return $lines
}

function Test-LocalImage {
    param([Parameter(Mandatory)][string]$Image)
    & docker image inspect $Image --format "{{.Id}}" *> $null
    return $LASTEXITCODE -eq 0
}

function Write-Result {
    param(
        [Parameter(Mandatory)][string]$Name,
        [ValidateSet("PASSED", "FAILED", "SKIPPED")][string]$Status,
        [Parameter(Mandatory)][string]$Detail,
        [ValidateSet("PASSED", "NOT_RUN")][string]$DualHead = "NOT_RUN"
    )
    $portableDetail = Convert-ToPortableEvidence $Detail
    Write-Host "[RESULT] client=$Name status=$Status dual_head=$DualHead detail=$portableDetail evidence=$EvidenceLogPath"
}

function Get-ImageEvidence {
    param([Parameter(Mandatory)][string]$Image)
    $lines = @(Invoke-Docker @("image", "inspect", $Image, "--format", "{{.Id}}"))
    if ($lines.Count -eq 0) { throw "docker image inspect returned no lines for $Image" }
    return $lines[-1]
}

function Get-ComposeNetwork {
    $lines = @(Invoke-Docker @("compose", "-f", $ComposeFile, "ps", "-q", "gateway"))
    if ($lines.Count -eq 0) { throw "docker compose ps returned no gateway lines" }
    $gatewayId = $lines[-1].Trim()
    if ([string]::IsNullOrWhiteSpace($gatewayId)) { throw "gateway container id is empty" }
    $lines = @(Invoke-Docker @("inspect", "--format", "{{json .NetworkSettings.Networks}}", $gatewayId))
    if ($lines.Count -eq 0) { throw "docker inspect returned no gateway network JSON" }
    $networks = $lines[-1] | ConvertFrom-Json
    $networkNames = @($networks.PSObject.Properties.Name)
    if ($networkNames.Count -eq 0) { throw "gateway network list is empty" }
    $network = $networkNames[0]
    if ([string]::IsNullOrWhiteSpace($network)) { throw "gateway network is empty" }
    return $network
}

function Wait-GatewayHealthy {
    $lines = @(Invoke-Docker @("compose", "-f", $ComposeFile, "ps", "-q", "gateway"))
    if ($lines.Count -eq 0) { throw "docker compose ps returned no gateway lines" }
    $gatewayId = $lines[-1].Trim()
    if ([string]::IsNullOrWhiteSpace($gatewayId)) { throw "gateway container id is empty" }
    for ($attempt = 0; $attempt -lt 36; $attempt++) {
        $lines = @(Invoke-Docker @("inspect", "--format", "{{.State.Health.Status}}", $gatewayId))
        if ($lines.Count -eq 0) { throw "docker inspect returned no health lines" }
        $health = $lines[-1].Trim()
        if ($health -eq "healthy") { return }
        if ($health -eq "unhealthy") { throw "gateway healthcheck is unhealthy" }
        Start-Sleep -Seconds 5
    }
    throw "gateway did not become healthy within 180 seconds"
}

function Invoke-OfflineGatewayBuild {
    $runtimeImage = "ghcr.io/hugefiver/ipfs3:latest"
    $vendorPath = Join-Path $RunRoot "vendor"
    $dockerfile = Join-Path $RunRoot "Dockerfile.gateway-runtime"
    Write-Host (Convert-ToPortableEvidence "cargo vendor --locked --offline $vendorPath")
    $vendorOutput = @(& cargo vendor --locked --offline $vendorPath 2>&1)
    $vendorExit = $LASTEXITCODE
    if ($vendorExit -ne 0) {
        $vendorOutput | ForEach-Object {
            $line = Convert-NativeOutputItem $_
            if ($null -ne $line) { Write-Host (Convert-ToPortableEvidence $line) }
        }
        throw "cargo vendor exited $vendorExit"
    }
    Write-Host "cargo vendor completed from the local cache"
    [IO.File]::WriteAllText(
        $dockerfile,
        @"
FROM rust:latest AS builder
WORKDIR /app
COPY --from=vendor . /vendor
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo --config 'source.crates-io.replace-with="vendored-sources"' --config 'source.vendored-sources.directory="/vendor"' build --release --locked --offline --bin ipfs-s3-gateway

FROM $runtimeImage
COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway
"@,
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @(
        "build", "--pull=false", "--network", "none", "--quiet",
        "--build-context", "vendor=$vendorPath", "--tag", $runtimeImage,
        "--file", $dockerfile, $RepoRoot
    ) | Out-Null
}

function BindMount {
    param([string]$Source, [string]$Target, [switch]$ReadOnly)
    $suffix = if ($ReadOnly) { ",readonly" } else { "" }
    return "type=bind,src=$Source,dst=$Target$suffix"
}

function Invoke-Rclone {
    param([string]$Network, [string]$ConfigPath, [string[]]$Arguments)
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "--mount", (BindMount $ConfigPath "/config/rclone.conf" -ReadOnly),
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        $Images.Rclone,
        "--config", "/config/rclone.conf"
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Invoke-Mc {
    param([string]$Network, [string]$ConfigDir, [string[]]$Arguments)
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "--mount", (BindMount $ConfigDir "/root/.mc"),
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        $Images.Mc
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Initialize-McAliases {
    param([string]$ConfigDir)
    $null = New-Item -ItemType Directory -Path $ConfigDir -Force
    $config = [ordered]@{
        version = "10"
        aliases = [ordered]@{
            network = [ordered]@{
                url = "http://gateway:9000"
                accessKey = "test"
                secretKey = "test"
                api = "S3v4"
                path = "on"
            }
            localhost = [ordered]@{
                url = "http://127.0.0.1:9000"
                accessKey = "test"
                secretKey = "test"
                api = "S3v4"
                path = "on"
            }
        }
    } | ConvertTo-Json -Depth 4
    [IO.File]::WriteAllText(
        (Join-Path $ConfigDir "config.json"),
        "$config$([Environment]::NewLine)",
        [Text.UTF8Encoding]::new($false)
    )
}

function Assert-McHead {
    param([string]$Network, [string]$ConfigDir, [string]$Alias, [string]$Bucket)
    $lines = @(Invoke-Mc $Network $ConfigDir @("stat", "--json", "$Alias/$Bucket/nested/path/file.txt"))
    $jsonLines = @($lines | Where-Object { $_.TrimStart().StartsWith("{") })
    if ($jsonLines.Count -eq 0) { throw "mc stat returned no JSON lines" }
    $jsonLine = $jsonLines[-1]
    $stat = $jsonLine | ConvertFrom-Json
    if ([int64]$stat.size -ne [Text.Encoding]::UTF8.GetByteCount($FixtureText)) {
        throw "HeadObject Content-Length mismatch: $($stat.size)"
    }
    $etag = ([string]$stat.etag).Trim('"')
    if ([string]::IsNullOrWhiteSpace($etag) -or $etag -notmatch "^(Qm|baf)") {
        throw "HeadObject ETag is not an IPFS CID: $etag"
    }
    return $etag
}

function Invoke-Aws {
    param([string]$Network, [string]$Endpoint, [string[]]$Arguments)
    $deletePath = Join-Path $RunRoot "delete.json"
    $dockerArgs = @(
        "run", "--rm", "--network", $Network,
        "-e", "AWS_ACCESS_KEY_ID=test",
        "-e", "AWS_SECRET_ACCESS_KEY=test",
        "-e", "AWS_DEFAULT_REGION=us-east-1",
        "-e", "AWS_EC2_METADATA_DISABLED=true",
        "--mount", (BindMount $FixturePath "/work/file.txt" -ReadOnly),
        "--mount", (BindMount $deletePath "/work/delete.json" -ReadOnly),
        $Images.Aws,
        "--endpoint-url", $Endpoint
    ) + $Arguments
    return Invoke-Docker $dockerArgs
}

function Assert-AwsHead {
    param([string]$Network, [string]$Endpoint, [string]$Bucket)
    $json = (Invoke-Aws $Network $Endpoint @(
        "s3api", "head-object", "--bucket", $Bucket, "--key", "nested/path/file.txt", "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if ([int64]$json.ContentLength -ne [Text.Encoding]::UTF8.GetByteCount($FixtureText)) {
        throw "AWS HeadObject ContentLength mismatch: $($json.ContentLength)"
    }
    $etag = ([string]$json.ETag).Trim('"')
    if ([string]::IsNullOrWhiteSpace($etag) -or $etag -notmatch "^(Qm|baf)") {
        throw "AWS HeadObject ETag is not an IPFS CID: $etag"
    }
    return $etag
}

function Invoke-RcloneSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-rclone-$Stamp"
    $config = Join-Path $RunRoot "rclone.conf"
    [IO.File]::WriteAllText($config, @"
[ipfs-s3]
type = s3
provider = Other
endpoint = http://gateway:9000
access_key_id = test
secret_access_key = test
region = us-east-1
force_path_style = true
list_version = 2
use_server_modtime = true
"@, [Text.UTF8Encoding]::new($false))
    Invoke-Docker @("run", "--rm", $Images.Rclone, "version") | Out-Null
    Write-Host "rclone effective options: list_version=2 use_server_modtime=true"
    Invoke-Rclone $Network $config @("mkdir", "ipfs-s3:$bucket") | Out-Null
    Invoke-Rclone $Network $config @("copy", "/work/file.txt", "ipfs-s3:$bucket/nested/path") | Out-Null
    $listed = (Invoke-Rclone $Network $config @("ls", "ipfs-s3:$bucket")) -join "`n"
    if (-not $listed.Contains("nested/path/file.txt")) { throw "rclone ls omitted nested object" }
    $cat = ((Invoke-Rclone $Network $config @("cat", "ipfs-s3:$bucket/nested/path/file.txt")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "rclone cat content mismatch" }

    Invoke-Rclone $Network $config @("deletefile", "ipfs-s3:$bucket/nested/path/file.txt") | Out-Null
    Invoke-Rclone $Network $config @("rmdir", "ipfs-s3:$bucket") | Out-Null
}

function Invoke-McSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-mc-$Stamp"
    $configDir = Join-Path $RunRoot "mc"
    Initialize-McAliases $configDir
    Invoke-Docker @("run", "--rm", $Images.Mc, "--version") | Out-Null
    Invoke-Mc $Network $configDir @("alias", "list", "network") | Out-Null
    Invoke-Mc "host" $configDir @("alias", "list", "localhost") | Out-Null
    Invoke-Mc $Network $configDir @("mb", "network/$bucket") | Out-Null
    Invoke-Mc $Network $configDir @("cp", "/work/file.txt", "network/$bucket/nested/path/file.txt") | Out-Null
    $listed = (Invoke-Mc $Network $configDir @("ls", "network/$bucket/nested/path/")) -join "`n"
    if (-not $listed.Contains("file.txt")) { throw "mc ls omitted file.txt" }
    $cat = ((Invoke-Mc $Network $configDir @("cat", "network/$bucket/nested/path/file.txt")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "mc cat content mismatch" }
    $networkEtag = Assert-McHead $Network $configDir "network" $bucket
    $localhostEtag = Assert-McHead "host" $configDir "localhost" $bucket
    if ($networkEtag -ne $localhostEtag) { throw "mc object ETag differs across endpoints" }
    $contentLength = [Text.Encoding]::UTF8.GetByteCount($FixtureText)
    Write-Host "[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000 etag=$networkEtag content_length=$contentLength"
    Invoke-Mc $Network $configDir @("rm", "network/$bucket/nested/path/file.txt") | Out-Null
    Invoke-Mc $Network $configDir @("rb", "network/$bucket") | Out-Null
}

function Invoke-AwsSmoke {
    param([string]$Network)
    $bucket = "ipfs-s3-aws-$Stamp"
    [IO.File]::WriteAllText(
        (Join-Path $RunRoot "delete.json"),
        '{"Objects":[{"Key":"a.txt"},{"Key":"nested/path/file.txt"},{"Key":"videos/file.txt"},{"Key":"missing"}],"Quiet":false}',
        [Text.UTF8Encoding]::new($false)
    )
    Invoke-Docker @("run", "--rm", $Images.Aws, "--version") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "mb", "s3://$bucket") | Out-Null
    foreach ($key in @("a.txt", "nested/path/file.txt", "videos/file.txt")) {
        Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "/work/file.txt", "s3://$bucket/$key") | Out-Null
    }
    $listed = (Invoke-Aws $Network "http://gateway:9000" @("s3", "ls", "s3://$bucket", "--recursive")) -join "`n"
    if (-not $listed.Contains("nested/path/file.txt")) { throw "AWS s3 ls omitted nested object" }
    $cat = ((Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "s3://$bucket/nested/path/file.txt", "-")) -join "`n").TrimEnd()
    if ($cat -ne $FixtureText) { throw "AWS s3 cp download mismatch" }

    $location = (Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "get-bucket-location", "--bucket", $bucket, "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if ($null -ne $location.LocationConstraint) { throw "us-east-1 LocationConstraint was not null" }

    $page = (Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "list-objects", "--bucket", $bucket, "--delimiter", "/", "--max-keys", "2", "--output", "json"
    )) -join "`n" | ConvertFrom-Json
    if (-not $page.IsTruncated -or [string]::IsNullOrWhiteSpace([string]$page.NextMarker)) {
        throw "AWS ListObjects v1 did not return a truncated page and NextMarker"
    }

    $networkEtag = Assert-AwsHead $Network "http://gateway:9000" $bucket
    $localhostEtag = Assert-AwsHead "host" "http://127.0.0.1:9000" $bucket
    if ($networkEtag -ne $localhostEtag) { throw "AWS object ETag differs across endpoints" }
    $contentLength = [Text.Encoding]::UTF8.GetByteCount($FixtureText)
    Write-Host "[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000 etag=$networkEtag content_length=$contentLength"

    Invoke-Aws $Network "http://gateway:9000" @(
        "s3api", "delete-objects", "--bucket", $bucket, "--delete", "file:///work/delete.json"
    ) | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "cp", "/work/file.txt", "s3://$bucket/cleanup.txt") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "rm", "s3://$bucket/cleanup.txt") | Out-Null
    Invoke-Aws $Network "http://gateway:9000" @("s3", "rb", "s3://$bucket") | Out-Null
}

function Invoke-SmokeMain {
    if (-not (Test-Path -LiteralPath $ComposeFile -PathType Leaf)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker-compose.yml is missing" }
        return $false
    }
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "docker command is missing" }
        return $false
    }
    & docker info --format "{{.ServerVersion}}" *> $null
    if ($LASTEXITCODE -ne 0) {
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "Docker daemon is unavailable" }
        return $false
    }

    $missingClients = @{}
    foreach ($name in $Selected) {
        if (-not (Test-LocalImage $Images[$name])) {
            $missingClients[$name] = $Images[$name]
            Write-Host "Required command: docker pull $($Images[$name])"
        }
    }
    if (-not $Run) {
        foreach ($name in $Selected) {
            if ($missingClients.ContainsKey($name)) {
                Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
            } else {
                Write-Result $name "SKIPPED" "execution not requested; rerun with -Run after authorization"
            }
        }
        return $false
    }

    $missingServices = @($ServiceImages | Where-Object { -not (Test-LocalImage $_) })
    if ($missingServices.Count -gt 0) {
        $missingServices | ForEach-Object { Write-Host "Required command: docker pull $_" }
        Write-Host (Convert-ToPortableEvidence "Required startup: docker compose -f `"$ComposeFile`" up -d --build --pull never kubo gateway")
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "local service image missing: $($missingServices -join ', ')" }
        return $false
    }
    if (-not (Test-LocalImage "rust:latest")) {
        Write-Host "Required command: docker pull rust:latest"
        foreach ($name in $Selected) { Write-Result $name "SKIPPED" "local build image missing: rust:latest" }
        return $false
    }
    $missingStandardBuild = @($StandardBuildImages | Where-Object { -not (Test-LocalImage $_) })
    $useOfflineGatewayBuild = $missingStandardBuild.Count -gt 0

    $runnable = @($Selected | Where-Object { -not $missingClients.ContainsKey($_) })
    foreach ($name in $Selected | Where-Object { $missingClients.ContainsKey($_) }) {
        Write-Result $name "SKIPPED" "local image missing: $($missingClients[$name])"
    }
    if ($runnable.Count -eq 0) { return $false }

    try {
        if ($useOfflineGatewayBuild) {
            Write-Host "Standard Compose build images unavailable: $($missingStandardBuild -join ', ')"
            Write-Host "Building the current gateway source from an offline vendored dependency context."
            Invoke-OfflineGatewayBuild
            Invoke-Docker @("compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway") | Out-Null
        } else {
            Invoke-Docker @("compose", "-f", $ComposeFile, "up", "-d", "--build", "--pull", "never", "kubo", "gateway") | Out-Null
        }
        Wait-GatewayHealthy
        Invoke-Docker @("compose", "-f", $ComposeFile, "ps") | Out-Null
        $network = Get-ComposeNetwork
    } catch {
        $stackError = Convert-ToPortableEvidence $_.Exception.Message
        foreach ($name in $runnable) {
            Write-Result $name "FAILED" "stack setup failed: $stackError"
        }
        Write-Host "Stack setup failed before client execution; per-client smoke loop was not entered."
        Write-Host "Compose volumes preserved for diagnosis. Explicit cleanup requires -CleanupVolumes after the stack issue is resolved."
        return $true
    }
    $failed = $false

    foreach ($name in $runnable) {
        try {
            $imageId = Get-ImageEvidence $Images[$name]
            Write-Host "client=$name image=$($Images[$name]) image_id=$imageId"
            switch ($name) {
                "Rclone" { Invoke-RcloneSmoke $network }
                "Mc" { Invoke-McSmoke $network }
                "Aws" { Invoke-AwsSmoke $network }
            }
            $dualHead = if ($name -eq "Rclone") { "NOT_RUN" } else { "PASSED" }
            Write-Result -Name $name -Status "PASSED" -Detail "all commands and assertions completed" -DualHead $dualHead
        } catch {
            $failed = $true
            Write-Result $name "FAILED" (Convert-ToPortableEvidence $_.Exception.Message)
            Write-Host "Manual cleanup endpoint: http://127.0.0.1:9000 bucket prefix ipfs-s3-$($name.ToLower())-$Stamp"
        }
    }

    if ($CleanupVolumes) {
        Invoke-Docker @("compose", "-f", $ComposeFile, "down", "-v") | Out-Null
    } else {
        Write-Host "Compose volumes preserved. Explicit cleanup: pwsh -NoProfile -File scripts/client-smoke.ps1 -Run -CleanupVolumes"
    }
    return $failed
}

Start-Transcript -LiteralPath $LogPath -Force | Out-Null
try {
    $failed = Invoke-SmokeMain
} finally {
    Stop-Transcript | Out-Null
}
Write-Host "Evidence log: $EvidenceLogPath"
if ($failed) { exit 1 }
exit 0
```

The script never executes `docker pull`, though it prints the required pull command when a local image is missing. Without `-Run` it does not start or build the stack, reports `dual_head=NOT_RUN`, and cannot satisfy ROADMAP item 9. With authorized `-Run`, it first requires local service images and `rust:latest`. If a standard Compose build image is absent, `Invoke-OfflineGatewayBuild` runs `cargo vendor --locked --offline`, builds the current repository with Docker `--pull=false --network none`, then starts Compose with `--pull never --no-build`; otherwise Compose starts with `--build --pull never`. `Initialize-McAliases` writes only a temporary `test`/`test` config for `network` and `localhost`. `Convert-ToPortableEvidence` rewrites `RunRoot` and `RepoRoot` as `<temp>` and `<repo>` before Docker output and RESULT details are printed, while RESULT and final evidence use `client-smoke.log`. Rclone performs only its own Compose-network `mkdir/copy/ls/cat/deletefile/rmdir` flow and on success returns `dual_head=NOT_RUN`; it does not depend on a Mc image or write EVIDENCE. Mc and AWS perform their own same-client two-endpoint stat/HEAD checks and write their own EVIDENCE only after both pass. Compose up, health wait, service status, and network discovery are one stack-level try/catch: on failure each runnable client receives exactly one `FAILED dual_head=NOT_RUN`, already-skipped clients receive no second result, the per-client loop is bypassed, the outer `finally` saves the transcript, and the script exits 1. Only explicit `-CleanupVolumes` executes `down -v`.

- [ ] **Step 3: Parse-check and run non-executing preflight**

```powershell
$scriptText = [IO.File]::ReadAllText("scripts/client-smoke.ps1")
$tokens = $null
$parseErrors = $null
$null = [System.Management.Automation.Language.Parser]::ParseInput($scriptText, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -ne 0) { throw "PowerShell parse errors: $($parseErrors.Message -join '; ')" }
if ($scriptText -match '\(Invoke-Docker[^\r\n]*\)\s*\[-1\]') { throw 'Invoke-Docker output must be materialized as an array before [-1]' }
$requiredShapes = @(
    '$lines = @(Invoke-Docker @("image", "inspect"',
    '$lines = @(Invoke-Docker @("compose"',
    '$lines = @(Invoke-Docker @("inspect", "--format", "{{json .NetworkSettings.Networks}}"',
    '$lines = @(Invoke-Docker @("inspect", "--format"',
    '$lines = @(Invoke-Mc',
    '$jsonLines = @($lines | Where-Object'
)
foreach ($shape in $requiredShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing array-shape guard: $shape" } }
$single = @("only")
$multiple = @("first", "last")
if ($single.Count -ne 1 -or $single[-1] -ne "only") { throw 'single-line array shape failed' }
if ($multiple.Count -ne 2 -or $multiple[-1] -ne "last") { throw 'multi-line array shape failed' }
$stackShapes = @(
    'Write-Result $name "FAILED" "stack setup failed: $stackError"',
    'Stack setup failed before client execution; per-client smoke loop was not entered.',
    'return $true',
    'Stop-Transcript | Out-Null',
    'if ($failed) { exit 1 }'
)
foreach ($shape in $stackShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing stack-failure guard: $shape" } }
$stackFailureWrites = [regex]::Matches($scriptText, [regex]::Escape('Write-Result $name "FAILED" "stack setup failed: $stackError"')).Count
$runnableLoops = [regex]::Matches($scriptText, [regex]::Escape('foreach ($name in $runnable)')).Count
if ($stackFailureWrites -ne 1 -or $runnableLoops -ne 2) { throw "Unexpected stack/per-client result shape: stack writes=$stackFailureWrites runnable loops=$runnableLoops" }
$stackWriteIndex = $scriptText.IndexOf('Write-Result $name "FAILED" "stack setup failed: $stackError"')
$stackReturnIndex = $scriptText.IndexOf('return $true', $stackWriteIndex)
$perClientLoopIndex = $scriptText.LastIndexOf('foreach ($name in $runnable)')
if ($stackWriteIndex -lt 0 -or $stackReturnIndex -le $stackWriteIndex -or $perClientLoopIndex -le $stackReturnIndex) { throw 'Stack failure must return before the per-client loop' }
$dualHeadShapes = @(
    '[RESULT] client=$Name status=$Status dual_head=$DualHead',
    '[EVIDENCE] client=Mc verifier=Mc dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000',
    '[EVIDENCE] client=Aws verifier=Aws dual_head=PASSED key=nested/path/file.txt localhost=http://127.0.0.1:9000 network=http://gateway:9000',
    '$dualHead = if ($name -eq "Rclone") { "NOT_RUN" } else { "PASSED" }',
    'Write-Result -Name $name -Status "PASSED" -Detail "all commands and assertions completed" -DualHead $dualHead'
)
foreach ($shape in $dualHeadShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing dual-head evidence shape: $shape" } }
$dualHeadEvidenceWrites = [regex]::Matches($scriptText, '\[EVIDENCE\].*dual_head=PASSED').Count
if ($dualHeadEvidenceWrites -ne 2) { throw "Expected 2 same-client dual-head evidence writers, got $dualHeadEvidenceWrites" }
$offlineAndPortableShapes = @(
    'function Convert-ToPortableEvidence {',
    '$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$Stamp"',
    '$EvidenceRepoRoot = "<repo>"',
    '$EvidenceLogPath = "client-smoke.log"',
    'function Invoke-OfflineGatewayBuild {',
    'cargo vendor --locked --offline',
    '"build", "--pull=false", "--network", "none", "--quiet"',
    '"compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway"',
    'function Initialize-McAliases {',
    'accessKey = "test"',
    'secretKey = "test"',
    'Write-Host "[RESULT] client=$Name status=$Status dual_head=$DualHead detail=$portableDetail evidence=$EvidenceLogPath"',
    'Write-Host "Evidence log: $EvidenceLogPath"'
)
foreach ($shape in $offlineAndPortableShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing offline/portable-evidence guard: $shape" } }
if ($scriptText -match '(?m)^\s*(?:&\s+)?docker\s+pull(?:\s|$)' -or $scriptText -match 'Invoke-Docker\s+@\([^\)]*"pull"') { throw 'Smoke script must not execute docker pull' }
if ($scriptText.Contains('"alias", "set"')) { throw 'mc aliases must be written as the temporary test/test config, not through alias set' }
if ([regex]::Matches($scriptText, [regex]::Escape('Initialize-McAliases ')).Count -ne 2) { throw 'Expected the helper definition and one temporary mc alias initialization' }
pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All
```

Expected: parser/static guard exits 0, proves one- and multi-line outputs retain array shape, rejects an inline `Invoke-Docker` call followed directly by `[-1]`, finds exactly two machine-readable same-client dual-head evidence writers (Mc and AWS), requires `cargo vendor --locked --offline`, Docker `--pull=false --network none`, and Compose `--pull never --no-build`, and rejects an executed `docker pull`. It also requires `Convert-ToPortableEvidence`, `<temp>`/`<repo>` normalization, `client-smoke.log` on RESULT/final evidence, and `Initialize-McAliases` writing only temporary `test`/`test` aliases. Preflight exits 0, starts no container, prints exactly one `[RESULT]` per client with `dual_head=NOT_RUN`, all `SKIPPED`; AWS names missing `amazon/aws-cli:latest`, while rclone/mc say execution was not requested. No `PASSED`, `FAILED`, or `dual_head=PASSED` line is allowed. An authorized `-Run` stack setup failure must instead emit exactly one `FAILED dual_head=NOT_RUN` for each runnable client, emit no second result for already-skipped clients, skip the per-client loop, preserve the transcript, and exit 1.

Do not run with `-Run`, install software, or pull/build images unless separately authorized. If authorized, run `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run`, then copy actual status/evidence into the matrix; never upgrade a skipped row without the corresponding log.

- [ ] **Step 4: Create compatibility guidance and initial evidence matrix**

Create `docs/client-compatibility.md`:

````markdown
# S3 Client Compatibility

## Endpoint and authentication contract

- Path-style endpoint, SigV4 service `s3`, region `us-east-1`.
- Credentials in development examples are `test` / `test`.
- Host path: `http://127.0.0.1:9000`.
- Compose-network path: `http://gateway:9000`.
- ETag is the IPFS CID, not an MD5 digest. Options that require an MD5 ETag are unsupported.

## Recommended rclone configuration

```ini
[ipfs-s3]
type = s3
provider = Other
endpoint = http://gateway:9000
access_key_id = test
secret_access_key = test
region = us-east-1
force_path_style = true
list_version = 2
use_server_modtime = true
```

`list_version = 2` is the recommended rclone path, but the gateway also implements ListObjects v1 for SDK and legacy-client compatibility. `use_server_modtime = true` uses S3 `LastModified` rather than treating the CID ETag as MD5 metadata.

## Result definitions

- `PASSED`: the listed commands and assertions actually executed successfully.
- `FAILED`: prerequisites were available and execution started, but a command or assertion failed.
- `SKIPPED`: execution did not run because an image/tool/authorization was absent.

Rust integration results and Docker-client results are separate evidence. A compiled script or passing Rust suite does not make a client row `PASSED`.

An actually executed client with `FAILED` blocks its ROADMAP checkbox and the final v0.2 commit. A `SKIPPED` row may support checking the ROADMAP item only as “smoke artifact implemented but not executed,” and the row must remain `SKIPPED` with its authorization/image reason.

## Compatibility matrix — evidence snapshot 2026-07-19

| Client | Version | Transport | Endpoint path | Auth/region | Operations | Recommended options | Result | Evidence date | Evidence command or log | Known limitation |
|---|---|---|---|---|---|---|---|---|---|---|
| rclone | 1.74.4 image present | Docker | Compose network (`http://gateway:9000`) only | SigV4, us-east-1 | mkdir, copy, ls, cat, deletefile, rmdir | `list_version = 2`, `use_server_modtime = true` | SKIPPED | 2026-07-19 | `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client Rclone` | Execution was not authorized; no localhost/nested-HEAD or cross-client verifier is used; CID ETag is not MD5. |
| MinIO mc | `minio/mc:latest` local image; runtime version not executed | Docker | localhost + Compose network | S3v4, us-east-1 | alias, mb, cp, ls, cat, stat, rm, rb | path-style alias | SKIPPED | 2026-07-19 | `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client Mc` | Execution was not authorized. |
| AWS CLI | `amazon/aws-cli:latest` absent | Docker | localhost + Compose network | SigV4, us-east-1 | mb, cp, ls, get-bucket-location, list-objects v1, head-object, delete-objects, rm, rb | path-style endpoint URL | SKIPPED | 2026-07-19 | `docker image inspect amazon/aws-cli:latest`; script prints `docker pull amazon/aws-cli:latest` but does not execute it | Local image is absent and pull is not authorized. |
| Rust integration harness | reqwest SigV4 helper + rust-s3 0.37.2 | real TCP to in-process axum+s3s | localhost listener | SigV4, us-east-1 | reqwest SigV4 GetBucketLocation/restXml, ListObjects v1, DeleteObjects, nested HEAD; rust-s3 v2 list and object/SSE/multipart/ZIP regressions | path style for rust-s3 coverage | SKIPPED | 2026-07-19 | Full `cargo test --test integration` verification is performed before ROADMAP completion | rust-s3 0.37.2 `location()` encodes `?location` as an object path; Task 4 does not use it as protocol evidence. |

## Re-running and updating evidence

1. Run preflight without side effects: `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All`.
2. After explicit build/start authorization, run: `pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run`.
3. Before changing ROADMAP or committing, update every actually executed client row from its transcript, including the exact `PASSED` or `FAILED` result; do not retain a stale `SKIPPED` row for a client that ran.
4. Stop on any `FAILED`; leave that client's ROADMAP checkbox open and do not create the final commit.
5. Keep volumes unless cleanup is explicitly selected with `-CleanupVolumes`.

Do not install a host AWS CLI or mc for this matrix, do not pull images without approval, and do not translate `SKIPPED` into compatibility success.
````

- [ ] **Step 5: Verify all ten rows and apply the smoke-result gate before ROADMAP checks**

Before editing, confirm Tasks 1-4 are GREEN and run the complete suites here so ROADMAP is never marked ahead of regression evidence:

```powershell
cargo test --lib
cargo test --test integration
```

Expected: both pass, including all v2 and decompress-zip tests. After that actual pass, replace the Rust integration harness matrix row with this exact evidence row:

```markdown
| Rust integration harness | reqwest SigV4 helper + rust-s3 0.37.2 | real TCP to in-process axum+s3s | localhost listener | SigV4, us-east-1 | reqwest SigV4 GetBucketLocation/restXml, ListObjects v1, DeleteObjects, nested HEAD; rust-s3 v2 list and object/SSE/multipart/ZIP regressions | path style for rust-s3 coverage | PASSED | 2026-07-19 | `cargo test --test integration` | Wiremock Kubo; rust-s3 0.37.2 `location()` encodes `?location` as an object path and is not Task 4 protocol evidence. |
```

Before touching ROADMAP, update every client row from any authorized `-Run` transcript. A client that actually ran must be `PASSED` or `FAILED`, never stale `SKIPPED`. Then run this gate:

```powershell
$matrixLines = [IO.File]::ReadAllLines("docs/client-compatibility.md")
$clientRows = @($matrixLines | Where-Object { $_ -match '^\| (rclone|MinIO mc|AWS CLI) \|' })
if ($clientRows.Count -ne 3) { throw "Expected 3 client matrix rows, got $($clientRows.Count)" }
$clientStatuses = @{}
foreach ($row in $clientRows) {
    if ($row -notmatch '^\| (?<client>rclone|MinIO mc|AWS CLI) \|.*\| (?<status>PASSED|FAILED|SKIPPED) \| 2026-07-19 \|') {
        throw "Malformed client matrix row: $row"
    }
    $client = $Matches.client
    $status = $Matches.status
    if ($status -eq "SKIPPED" -and $row -notmatch '(?i)(not authorized|authorization.*absent|local image.*(absent|missing)|required.*image.*missing|pull.*not authorized)') {
        throw "SKIPPED is not eligible for ROADMAP completion: $row"
    }
    $clientStatuses[$client] = $status
}
$failedClients = @($clientStatuses.GetEnumerator() | Where-Object Value -eq "FAILED" | ForEach-Object Key)
if ($failedClients.Count -gt 0) {
    throw "ROADMAP and final commit blocked by FAILED client(s): $($failedClients -join ', ')"
}
```

If no authorized client execution occurred, all three client rows remain accurately `SKIPPED`; replace only the v0.2 list with this artifact-only wording:

```markdown
- [x] Fix docker-compose SQLite file database startup (`sqlite:///data/ipfs-s3.db`)
- [x] GetBucketLocation (`us-east-1`) for MinIO `mc` and SDK preflight checks
- [x] ListObjects v1 compatibility by reusing the ListObjectsV2 listing logic
- [x] DeleteObjects (batch delete) for clients that remove multiple keys at once
- [x] rclone smoke artifact implemented but not executed: mkdir, copy, ls, cat, deletefile, rmdir (`SKIPPED`: execution not authorized; see compatibility matrix)
- [x] MinIO `mc` smoke artifact implemented but not executed: alias, mb, cp, ls, cat, stat, rm, rb (`SKIPPED`: execution not authorized; see compatibility matrix)
- [x] AWS CLI smoke artifact implemented but not executed: baseline regression coverage (`SKIPPED`: local image absent and pull not authorized; see compatibility matrix)
- [x] Document recommended rclone options when exact S3 behavior differs (`list_version=2`, `use_server_modtime`)
- [ ] Verify HeadObject signatures for nested keys through direct docker networking and localhost (dual_head=NOT_RUN; release blocked)
- [x] Track client compatibility matrix in docs
```

For each matrix row that is actually `PASSED`, use the corresponding exact ROADMAP line instead of its artifact-only line:

```markdown
- [x] rclone smoke test PASSED: mkdir, copy, ls, cat, deletefile, rmdir (see compatibility matrix)
- [x] MinIO `mc` smoke test PASSED: alias, mb, cp, ls, cat, stat, rm, rb (see compatibility matrix)
- [x] AWS CLI smoke test PASSED: baseline regression coverage (see compatibility matrix)
```

The all-`SKIPPED` template has exactly 19 checked ROADMAP rows (the 10 existing v0.1 rows plus nine v0.2 rows); item 9 is deliberately its sole unchecked v0.2 row. It must not enter Task 6 staging or final commit. Do not change any v0.3+ row. A smoke-script implementation plus an accurate authorization/image `SKIPPED` may check only the explicitly artifact-only client ROADMAP wording; it does not claim compatibility success. Any actual `FAILED` leaves that client's original ROADMAP checkbox unchecked and stops Task 5 before the all-checked block is applied.

- [ ] **Step 6: Validate artifacts and exact status vocabulary**

```powershell
$required = @("scripts/client-smoke.ps1", "docs/client-compatibility.md", "config.docker.toml"); $missing = @($required | Where-Object { -not (Test-Path -LiteralPath $_ -PathType Leaf) }); if ($missing.Count -gt 0) { throw "Missing v0.2 artifacts: $($missing -join ', ')" }
rg -n "PASSED|FAILED|SKIPPED|list_version = 2|use_server_modtime = true|127.0.0.1:9000|gateway:9000" scripts/client-smoke.ps1 docs/client-compatibility.md
rg -n "^- \[x\]" ROADMAP.md
docker compose -f docker-compose.yml config --quiet
$renderedCompose = (docker compose -f docker-compose.yml config) -join "`n"; if ($LASTEXITCODE -ne 0) { throw "docker compose config failed with exit code $LASTEXITCODE" }; if (-not $renderedCompose.Contains('$$local')) { throw 'Compose config output must preserve the escaped $$local token for container-side shell expansion' }
$matrixLines = [IO.File]::ReadAllLines("docs/client-compatibility.md")
$roadmapLines = [IO.File]::ReadAllLines("ROADMAP.md")
$rules = @{
    "rclone" = '^- \[x\] rclone smoke'
    "MinIO mc" = '^- \[x\] MinIO `mc` smoke'
    "AWS CLI" = '^- \[x\] AWS CLI smoke'
}
foreach ($client in $rules.Keys) {
    $row = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($client)) \|" })
    if ($row.Count -ne 1 -or $row[0] -notmatch '\| (?<status>PASSED|FAILED|SKIPPED) \| 2026-07-19 \|') { throw "Invalid matrix state for $client" }
    $status = $Matches.status
    if ($status -eq "FAILED") { throw "ROADMAP and commit blocked by FAILED client: $client" }
    if ($status -eq "SKIPPED" -and $row[0] -notmatch '(?i)(not authorized|authorization.*absent|local image.*(absent|missing)|required.*image.*missing|pull.*not authorized)') { throw "SKIPPED reason cannot complete ROADMAP for $client" }
    $roadmapItem = @($roadmapLines | Where-Object { $_ -match $rules[$client] })
    if ($roadmapItem.Count -ne 1) { throw "Expected one checked ROADMAP item for $client" }
    if ($status -eq "SKIPPED" -and (-not $roadmapItem[0].Contains("artifact implemented but not executed") -or -not $roadmapItem[0].Contains("SKIPPED"))) { throw "SKIPPED ROADMAP wording is inaccurate for $client" }
    if ($status -eq "PASSED" -and -not $roadmapItem[0].Contains("smoke test PASSED")) { throw "PASSED ROADMAP wording is inaccurate for $client" }
}
$item9 = @($roadmapLines | Where-Object { $_ -match 'Verify HeadObject signatures for nested keys through direct docker networking and localhost' })
if ($item9.Count -ne 1 -or $item9[0] -notmatch '^- \[ \].*dual_head=NOT_RUN; release blocked') { throw 'All-SKIPPED ROADMAP must leave exactly one item 9 unchecked and release-blocked' }
$checkedCount = @($roadmapLines | Where-Object { $_ -match '^- \[x\]' }).Count
if ($checkedCount -ne 19) { throw "All-SKIPPED ROADMAP must have 19 checked rows, got $checkedCount" }
```

Expected: all artifacts exist; required terms/endpoints are present; after Task 5's non-executing state, ROADMAP shows exactly 19 checked rows (10 existing v0.1 + nine v0.2), and the unique item 9 is `[ ]` with `dual_head=NOT_RUN; release blocked`; v0.3+ remains unchecked. Both Compose render commands exit 0 without starting services and preserve the escaped `$$local` token. The semantic gate rejects every `FAILED`, rejects `SKIPPED` reasons other than missing authorization/local image, requires eligible checked `SKIPPED` clients to say “artifact implemented but not executed,” and makes clear that the static/source result writers are not execution evidence.

- [ ] **Step 7: Checkpoint without committing**

```powershell
git diff -- scripts/client-smoke.ps1 docs/client-compatibility.md ROADMAP.md
git status --short
```

Expected: no generated temp transcript is inside the repository, no client result is falsely `PASSED`, and no source is staged/committed. The only allowed future tracked real-smoke receipt is `docs/client-smoke-evidence-2026-07-19.log`, generated by Task 6's mandatory execution rather than by this task.

---

### Task 6: Final regression, review, and the single commit

**Files:**
- Review/commit: `docs/superpowers/specs/2026-07-19-client-compatibility-design.md`
- Review/commit: `docs/superpowers/plans/2026-07-19-client-compatibility.md`
- Review/commit: `src/config.rs`
- Review/commit: `src/s3/handler.rs`
- Review/commit: `src/s3/ops/bucket.rs`
- Review/commit: `src/s3/ops/object.rs`
- Review/commit: `src/store/object.rs`
- Review/commit: `tests/support/sigv4.rs`
- Review/commit: `tests/integration.rs`
- Review/commit: `docker-compose.yml`
- Review/commit: `config.example.toml`
- Review/commit: `config.docker.toml`
- Review/commit: `scripts/client-smoke.ps1`
- Review/commit: `docs/client-compatibility.md`
- Review/commit: `docs/client-smoke-evidence-2026-07-19.log`
- Review/commit: `ROADMAP.md`

**Interfaces:**
- Consumes: all outputs from Tasks 1-5 and approved design `docs/superpowers/specs/2026-07-19-client-compatibility-design.md`.
- Produces: formatted code, complete lib/integration evidence including v2/ZIP regressions, reviewed scope, clean diff, one tracked mandatory real-smoke receipt with parsed dual-head proof, and exactly one commit `feat: complete v0.2 client compatibility`.

- [ ] **Step 1: Reconfirm baseline ancestry and account for every worktree change**

```powershell
git rev-parse HEAD
git status --short
git diff --stat
git log --oneline -10
```

Expected before the final commit: HEAD is still `5e854e46e46cd2ea6511d055ba5e60320117d289`; all changes are the approved spec, this plan, and Tasks 1-5. Stop if unrelated user work appears. Do not read ignored `config.toml`.

- [ ] **Step 2: Format and run complete Rust verification**

```powershell
cargo fmt --all
cargo fmt --all -- --check
cargo test --lib
cargo test --test integration
cargo test --test e2e --no-run
cargo clippy --all-targets -- -D warnings
```

Expected: format check, e2e compile, and all-target clippy with `-D warnings` exit 0; all library and integration tests pass. The full integration command must include and pass standard Put/Get/Head/Delete, CopyObject, v2 delimiter/pagination, SSE-S3, SSE-C, multipart, the complete decompress-zip set (valid extraction, traversal rejection, archive-key collision, partial entry failure, bounded Complete XML, header signing, and presigned signing), and the URL-encoding wire test. Do not replace this command with only targeted compatibility tests.

- [ ] **Step 3: Prove the required regression names are present in the executed binary**

```powershell
$tests = cargo test --test integration -- --list; $required = @("test_client_compat_get_bucket_location_is_standard_us_east_1", "test_client_compat_list_v1_delimiter_marker_pages_without_replay", "test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination", "test_client_compat_delete_objects_is_retry_safe_and_ordered", "test_client_compat_delete_objects_quiet_hides_successes", "test_client_compat_delete_objects_continues_after_store_error", "test_client_compat_head_nested_key_signed_on_localhost", "test_list_objects_with_delimiter_returns_common_prefixes", "test_put_decompress_zip_archive_key_collision_is_global_reject"); foreach ($name in $required) { if (-not ($tests -match [regex]::Escape($name))) { throw "Missing integration test: $name" } }
```

Expected: exit 0 with all named compatibility, v2, and ZIP anchors found.

- [ ] **Step 4: Verify non-Rust surfaces without starting the stack**

```powershell
docker compose -f docker-compose.yml config --quiet
$renderedCompose = (docker compose -f docker-compose.yml config) -join "`n"; if ($LASTEXITCODE -ne 0) { throw "docker compose config failed with exit code $LASTEXITCODE" }; if (-not $renderedCompose.Contains('$$local')) { throw 'Compose config output must preserve the escaped $$local token for container-side shell expansion' }
$scriptText = [IO.File]::ReadAllText("scripts/client-smoke.ps1"); $tokens = $null; $parseErrors = $null; $null = [System.Management.Automation.Language.Parser]::ParseInput($scriptText, [ref]$tokens, [ref]$parseErrors); if ($parseErrors.Count -ne 0) { throw "PowerShell parse errors: $($parseErrors.Message -join '; ')" }; if ($scriptText -match '\(Invoke-Docker[^\r\n]*\)\s*\[-1\]') { throw 'Invoke-Docker output must be materialized as an array before [-1]' }
$requiredShapes = @('$lines = @(Invoke-Docker @("image", "inspect"', '$lines = @(Invoke-Docker @("compose"', '$lines = @(Invoke-Docker @("inspect", "--format", "{{json .NetworkSettings.Networks}}"', '$lines = @(Invoke-Docker @("inspect", "--format"', '$lines = @(Invoke-Mc', '$jsonLines = @($lines | Where-Object'); foreach ($shape in $requiredShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing array-shape guard: $shape" } }
$single = @("only"); $multiple = @("first", "last"); if ($single.Count -ne 1 -or $single[-1] -ne "only" -or $multiple.Count -ne 2 -or $multiple[-1] -ne "last") { throw 'PowerShell array-shape unit guard failed' }
$stackFailureWrites = [regex]::Matches($scriptText, [regex]::Escape('Write-Result $name "FAILED" "stack setup failed: $stackError"')).Count; $runnableLoops = [regex]::Matches($scriptText, [regex]::Escape('foreach ($name in $runnable)')).Count; $stackWriteIndex = $scriptText.IndexOf('Write-Result $name "FAILED" "stack setup failed: $stackError"'); $stackReturnIndex = $scriptText.IndexOf('return $true', $stackWriteIndex); $perClientLoopIndex = $scriptText.LastIndexOf('foreach ($name in $runnable)'); if ($stackFailureWrites -ne 1 -or $runnableLoops -ne 2 -or $stackWriteIndex -lt 0 -or $stackReturnIndex -le $stackWriteIndex -or $perClientLoopIndex -le $stackReturnIndex -or -not $scriptText.Contains('Stop-Transcript | Out-Null') -or -not $scriptText.Contains('if ($failed) { exit 1 }')) { throw 'Stack-level failure/result/transcript guard failed' }
$dualHeadEvidenceWrites = [regex]::Matches($scriptText, '\[EVIDENCE\].*dual_head=PASSED').Count; if ($dualHeadEvidenceWrites -ne 2 -or -not $scriptText.Contains('[RESULT] client=$Name status=$Status dual_head=$DualHead') -or -not $scriptText.Contains('$dualHead = if ($name -eq "Rclone") { "NOT_RUN" } else { "PASSED" }') -or -not $scriptText.Contains('Write-Result -Name $name -Status "PASSED" -Detail "all commands and assertions completed" -DualHead $dualHead')) { throw 'Dual-head result/evidence static guard failed' }
$offlinePortableShapes = @('function Convert-ToPortableEvidence {', '$EvidenceRunRoot = "<temp>/ipfs-s3-client-smoke-$Stamp"', '$EvidenceRepoRoot = "<repo>"', '$EvidenceLogPath = "client-smoke.log"', 'function Invoke-OfflineGatewayBuild {', 'cargo vendor --locked --offline', '"build", "--pull=false", "--network", "none", "--quiet"', '"compose", "-f", $ComposeFile, "up", "-d", "--pull", "never", "--no-build", "kubo", "gateway"', 'function Initialize-McAliases {', 'accessKey = "test"', 'secretKey = "test"', 'Write-Host "[RESULT] client=$Name status=$Status dual_head=$DualHead detail=$portableDetail evidence=$EvidenceLogPath"', 'Write-Host "Evidence log: $EvidenceLogPath"'); foreach ($shape in $offlinePortableShapes) { if (-not $scriptText.Contains($shape)) { throw "Missing offline/portable-evidence guard: $shape" } }; if ($scriptText -match '(?m)^\s*(?:&\s+)?docker\s+pull(?:\s|$)' -or $scriptText -match 'Invoke-Docker\s+@\([^\)]*"pull"') { throw 'Smoke script must not execute docker pull' }; if ($scriptText.Contains('"alias", "set"') -or [regex]::Matches($scriptText, [regex]::Escape('Initialize-McAliases ')).Count -ne 2) { throw 'mc aliases must be initialized exactly once from the temporary test/test config helper' }
pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All
```

Expected: both Compose render commands and every parser, array-shape, dual-head, offline-build, portable-evidence, and temporary-mc-alias guard exit 0. The source must include `cargo vendor --locked --offline`, Docker `--pull=false --network none`, Compose `--pull never --no-build`, `Convert-ToPortableEvidence`, `<temp>`/`<repo>` replacements, and `client-smoke.log` on RESULT/final evidence, while rejecting executed `docker pull` and `mc alias set`. No inline `Invoke-Docker` call is followed directly by `[-1]`; source and Compose 2.39.2 rendered output retain escaped `$$local`. Preflight emits three `SKIPPED dual_head=NOT_RUN` rows, no `PASSED`/`FAILED`, starts no service, performs no build/pull, and writes a temp transcript outside the repository. Static writer count is source-shape evidence only, not execution evidence; this dry run cannot satisfy ROADMAP item 9.


- [ ] **Step 5: Run mandatory real-stack dual-head smoke before any commit**

This step is mandatory before staging or committing, but it still requires separate authorization to build/start the stack. If authorization is absent, a required stack/client image is unavailable, or the run cannot produce the required Mc/Mc proof, stop with item 9 unchecked and do not continue to Step 8. AWS may be `SKIPPED` only because `amazon/aws-cli:latest` is missing; Rclone may pass with `dual_head=NOT_RUN`, but Mc must actually pass its same-client dual-endpoint stat.

Run the authorized smoke through an outer redirection. The tracked file is the UTF-8 outer stdout/stderr capture itself—there is no transcript reconstruction, filtering, line conversion, or post-processing. The command does not read, render, or copy ignored `config.toml`; it only exercises the `test`/`test` smoke scenario. The runtime guard rejects a local master key, ignored config reference, non-test AWS credential assignment, host-absolute path, or PowerShell build-noise marker after the original capture has been preserved for diagnosis.

```powershell
$evidenceLog = "docs/client-smoke-evidence-2026-07-19.log"
& pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run *> $evidenceLog
$smokeExit = $LASTEXITCODE
if ($null -eq $smokeExit) { throw "Mandatory real smoke did not return an exit code" }
$evidenceText = [IO.File]::ReadAllText($evidenceLog)
if ($evidenceText -match '(?i)IPFS_S3_MASTER_KEY|config\.toml|AWS_ACCESS_KEY_ID=(?!test(?:\s|$))|AWS_SECRET_ACCESS_KEY=(?!test(?:\s|$))') { throw "Tracked evidence log contains local sensitive configuration instead of test/test-only smoke data" }
$hostDrivePattern = '(?i)(?<![a-z])[a-z]:[\\/]'
if ('http://127.0.0.1:9000' -match $hostDrivePattern) { throw 'Host path guard must not match URI schemes' }
if ('C:\Users\example' -notmatch $hostDrivePattern) { throw 'Host path guard must match Windows drive paths' }
$forbiddenEvidencePatterns = @("(?:$hostDrivePattern|/run/desktop/mnt/host/[a-z]/|/(?:Users|home)/)", '(?im)^\s*(?:At line:\d+ char:\d+|CategoryInfo\s*:|FullyQualifiedErrorId\s*:|ScriptStackTrace\s*:|System\.Management\.Automation\.RemoteException)\b')
foreach ($pattern in $forbiddenEvidencePatterns) { if ($evidenceText -match $pattern) { throw "Tracked evidence log contains a host-absolute path or PowerShell build-noise pattern: $pattern" } }
$evidenceLines = @([IO.File]::ReadAllLines($evidenceLog))
$resultLines = @($evidenceLines | Where-Object { $_ -match '^\[RESULT\] client=' })
if ($resultLines.Count -ne 3) { throw "Expected exactly 3 client RESULT lines, got $($resultLines.Count)" }
$resultRecords = foreach ($line in $resultLines) {
    if ($line -notmatch '^\[RESULT\] client=(?<client>Rclone|Mc|Aws) status=(?<status>PASSED|FAILED|SKIPPED) dual_head=(?<dualHead>PASSED|NOT_RUN) detail=.* evidence=client-smoke\.log$') { throw "Malformed RESULT line: $line" }
    [pscustomobject]@{ Client = $Matches.client; Status = $Matches.status; DualHead = $Matches.dualHead }
}
if (@($resultRecords.Client | Sort-Object -Unique).Count -ne 3) { throw "RESULT lines must name Rclone, Mc, and Aws exactly once" }
$evidenceRecords = foreach ($line in @($evidenceLines | Where-Object { $_ -match '^\[EVIDENCE\] client=' })) {
    if ($line -notmatch '^\[EVIDENCE\] client=(?<client>Mc|Aws) verifier=(?<verifier>Mc|Aws) dual_head=(?<dualHead>PASSED) key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=\S+ content_length=\d+$') { throw "Malformed EVIDENCE line: $line" }
    if ($Matches.verifier -ne $Matches.client) { throw "EVIDENCE verifier must equal client: $line" }
    [pscustomobject]@{ Client = $Matches.client; Verifier = $Matches.verifier; DualHead = $Matches.dualHead }
}
if ($evidenceRecords.Count -ne 1 -or $evidenceRecords[0].Client -ne "Mc" -or $evidenceRecords[0].Verifier -ne "Mc") { throw "Expected exactly one Mc/Mc runtime EVIDENCE; AWS was not executed" }
$failedResults = @($resultRecords | Where-Object { $_.Status -eq "FAILED" })
if ($failedResults.Count -gt 0) { throw "Client smoke FAILED: $($failedResults.Client -join ', '); retain the tracked log, mark those matrix rows FAILED, leave their ROADMAP items open, and block the final commit" }
if ($smokeExit -ne 0) { throw "Mandatory client smoke exited $smokeExit; retain the tracked log and block the final commit" }
$expectedResults = @{ Rclone = @("PASSED", "NOT_RUN"); Mc = @("PASSED", "PASSED"); Aws = @("SKIPPED", "NOT_RUN") }
foreach ($result in $resultRecords) { if ($result.Status -ne $expectedResults[$result.Client][0] -or $result.DualHead -ne $expectedResults[$result.Client][1]) { throw "Unexpected mandatory smoke result for $($result.Client): $($result.Status)/$($result.DualHead)" } }
$passedDualResults = @($resultRecords | Where-Object { $_.Client -eq "Mc" -and $_.Status -eq "PASSED" -and $_.DualHead -eq "PASSED" })
if ($passedDualResults.Count -ne 1) { throw "Rclone PASSED/NOT_RUN and AWS SKIPPED/NOT_RUN cannot satisfy item 9; exactly Mc PASSED/PASSED is required" }
$provedResults = @($passedDualResults | Where-Object {
    $candidate = $_.Client
    @($evidenceRecords | Where-Object { $_.Client -eq $candidate -and $_.Verifier -eq $candidate -and $_.DualHead -eq "PASSED" }).Count -eq 1
})
if ($provedResults.Count -ne 1) { throw "Mc PASSED/PASSED must have exactly one matching Mc/Mc EVIDENCE dual_head=PASSED" }
$provedClient = $provedResults[0].Client
```

After the command passes, update every actually executed matrix row from the tracked log before proceeding. Rclone must cite only its `RESULT status=PASSED dual_head=NOT_RUN` and must not claim localhost, nested HEAD, or EVIDENCE. Mc must cite `docs/client-smoke-evidence-2026-07-19.log`, its own `RESULT status=PASSED dual_head=PASSED`, and exactly one `client=Mc verifier=Mc` EVIDENCE. AWS remains `SKIPPED` only when the result and the exact missing-image reason say so. Synchronize the three client ROADMAP lines using Task 5's `smoke test PASSED`, artifact-only `SKIPPED`, or unchecked `smoke test FAILED` wording. Set item 9 exactly once to the checked `MinIO mc same-client dual endpoint stat PASSED` line containing `client=Mc verifier=Mc` and the tracked log path; no `SKIPPED` template or Rclone-only success may check item 9.

Runtime note from the authorized Task 6 execution: `debian:trixie-slim` was absent as a tagged local image even though the service/runtime images and host Cargo cache were present. `Invoke-OfflineGatewayBuild` now executes `cargo vendor --locked --offline`, passes its temporary vendor directory as a named Docker build context, builds the current source under `--network none --pull=false`, and starts Compose with `--pull never --no-build`. `Initialize-McAliases` writes `$RunRoot`-scoped `config.json` aliases for `network` and `localhost`, both with only `test`/`test`, because the observed mc release rejects the intentionally short secret in `alias set`. Both behaviors fail closed when local prerequisites are absent and do not change production credentials or tracked configuration.

Before continuing, re-read the tracked log and verify the matrix/ROADMAP synchronization instead of relying on the script source or a writer count:

```powershell
$evidenceLog = "docs/client-smoke-evidence-2026-07-19.log"
if (-not (Test-Path -LiteralPath $evidenceLog -PathType Leaf)) { throw "Missing mandatory tracked evidence log: $evidenceLog" }
$evidenceText = [IO.File]::ReadAllText($evidenceLog)
$forbiddenEvidencePatterns = @('(?i)(?:(?<![a-z])[a-z]:[\\/]|/run/desktop/mnt/host/[a-z]/|/(?:Users|home)/)', '(?im)^\s*(?:At line:\d+ char:\d+|CategoryInfo\s*:|FullyQualifiedErrorId\s*:|ScriptStackTrace\s*:|System\.Management\.Automation\.RemoteException)\b')
foreach ($pattern in $forbiddenEvidencePatterns) { if ($evidenceText -match $pattern) { throw "Tracked evidence log contains a host-absolute path or PowerShell build-noise pattern: $pattern" } }
$evidenceLines = @([IO.File]::ReadAllLines($evidenceLog))
$resultLines = @($evidenceLines | Where-Object { $_ -match '^\[RESULT\] client=' })
if ($resultLines.Count -ne 3) { throw "Expected exactly 3 client RESULT lines, got $($resultLines.Count)" }
$resultRecords = foreach ($line in $resultLines) {
    if ($line -notmatch '^\[RESULT\] client=(?<client>Rclone|Mc|Aws) status=(?<status>PASSED|FAILED|SKIPPED) dual_head=(?<dualHead>PASSED|NOT_RUN) detail=.* evidence=client-smoke\.log$') { throw "Malformed RESULT line: $line" }
    [pscustomobject]@{ Client = $Matches.client; Status = $Matches.status; DualHead = $Matches.dualHead }
}
if (@($resultRecords.Client | Sort-Object -Unique).Count -ne 3) { throw "RESULT lines must name Rclone, Mc, and Aws exactly once" }
$evidenceRecords = foreach ($line in @($evidenceLines | Where-Object { $_ -match '^\[EVIDENCE\] client=' })) {
    if ($line -notmatch '^\[EVIDENCE\] client=(?<client>Mc|Aws) verifier=(?<verifier>Mc|Aws) dual_head=(?<dualHead>PASSED) key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=\S+ content_length=\d+$') { throw "Malformed EVIDENCE line: $line" }
    if ($Matches.verifier -ne $Matches.client) { throw "EVIDENCE verifier must equal client: $line" }
    [pscustomobject]@{ Client = $Matches.client; Verifier = $Matches.verifier; DualHead = $Matches.dualHead }
}
if ($evidenceRecords.Count -ne 1 -or $evidenceRecords[0].Client -ne "Mc" -or $evidenceRecords[0].Verifier -ne "Mc") { throw "Final commit blocked: expected exactly one Mc/Mc runtime EVIDENCE" }
if (@($resultRecords | Where-Object { $_.Status -eq "FAILED" }).Count -gt 0) { throw "Final commit blocked by FAILED client result in tracked evidence" }
$expectedResults = @{ Rclone = @("PASSED", "NOT_RUN"); Mc = @("PASSED", "PASSED"); Aws = @("SKIPPED", "NOT_RUN") }
foreach ($result in $resultRecords) { if ($result.Status -ne $expectedResults[$result.Client][0] -or $result.DualHead -ne $expectedResults[$result.Client][1]) { throw "Unexpected mandatory smoke result for $($result.Client): $($result.Status)/$($result.DualHead)" } }
$provedResults = @($resultRecords | Where-Object {
    $candidate = $_.Client
    $_.Client -eq "Mc" -and $_.Status -eq "PASSED" -and $_.DualHead -eq "PASSED" -and @($evidenceRecords | Where-Object { $_.Client -eq $candidate -and $_.Verifier -eq $candidate -and $_.DualHead -eq "PASSED" }).Count -eq 1
})
if ($provedResults.Count -ne 1) { throw "Final commit blocked: tracked evidence requires one Mc/Mc RESULT and EVIDENCE dual_head=PASSED pair" }
$matrixNames = @{ Rclone = "rclone"; Mc = "MinIO mc"; Aws = "AWS CLI" }
$matrixLines = [IO.File]::ReadAllLines("docs/client-compatibility.md")
foreach ($result in $resultRecords) {
    $matrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$result.Client])) \|" })
    if ($matrixRow.Count -ne 1 -or $matrixRow[0] -notmatch "\| $($result.Status) \| 2026-07-19 \|") { throw "Matrix was not updated to $($result.Status) for $($result.Client)" }
}
$provedClient = $provedResults[0].Client
$provedMatrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$provedClient])) \|" })
if ($provedMatrixRow.Count -ne 1 -or $provedMatrixRow[0] -notmatch 'docs/client-smoke-evidence-2026-07-19\.log' -or $provedMatrixRow[0] -notmatch 'dual_head=PASSED' -or $provedMatrixRow[0] -notmatch "client=$provedClient") { throw "PASSED dual-head matrix row must cite the tracked log and same-client dual_head=PASSED evidence" }
$roadmapLines = [IO.File]::ReadAllLines("ROADMAP.md")
$item9 = @($roadmapLines | Where-Object { $_ -match 'Verify HeadObject signatures for nested keys through direct docker networking and localhost' })
if ($item9.Count -ne 1 -or $item9[0] -notmatch '^- \[x\].*MinIO `mc` same-client dual endpoint `stat` PASSED.*client=Mc verifier=Mc.*docs/client-smoke-evidence-2026-07-19\.log') { throw "Item 9 must be the unique checked Mc/Mc dual-endpoint stat line backed by the tracked log" }
```

Expected under the currently observed local images: Rclone reports `PASSED/NOT_RUN`, Mc reports `PASSED/PASSED`, and AWS reports `SKIPPED/NOT_RUN` because `amazon/aws-cli:latest` is absent. The script must not pull it. The tracked log has exactly three RESULT records whose `evidence=client-smoke.log`, exactly one runtime EVIDENCE record `client=Mc verifier=Mc`, and neither host-absolute paths nor PowerShell build noise. Any `FAILED`, including stack-level setup failure, preserves the evidence log, updates the applicable matrix/ROADMAP failure state, and stops Task 6 before staging. All three `SKIPPED`, AWS-only success, Rclone-only success, a stale matrix row, or a static-only writer count also stops Task 6. Only the Mc same-client RESULT/EVIDENCE pair can open item 9.

If the mandatory smoke leaves the stack running, the optional legacy real-stack suite is:

```powershell
cargo test --test e2e -- --nocapture --test-threads=1
```

Record its actual outcome separately. Do not run it against a nonexistent stack, and do not treat it as a substitute for `cargo test --test integration`. Execute volume cleanup only when explicitly selected:

```powershell
pwsh -NoProfile -File scripts/client-smoke.ps1 -Client All -Run -CleanupVolumes
```

- [ ] **Step 6: Review security, scope, and all ten specification outcomes**

Before running this step, synchronize each smoke ROADMAP line with the current matrix: `PASSED` uses the exact `smoke test PASSED` wording from Task 5; an unexecuted `SKIPPED` uses the exact `smoke artifact implemented but not executed` wording; `FAILED` remains unchecked and stops execution. This review must directly read the Task 6 tracked real-smoke log; script source and static writer counts are not execution evidence.

```powershell
rg -n "crate::kubo::pin::pin_rm" src/s3/ops/object.rs src/store/object.rs
rg -n "location_constraint: None|fn list_objects\(|fn list_objects_v2\(|fn delete_objects\(|delete_latest_if_present|config.docker.toml|mode=rwc" src tests docker-compose.yml config.example.toml config.docker.toml
git diff -- ROADMAP.md docs/client-compatibility.md docs/client-smoke-evidence-2026-07-19.log scripts/client-smoke.ps1
$evidenceLog = "docs/client-smoke-evidence-2026-07-19.log"
if (-not (Test-Path -LiteralPath $evidenceLog -PathType Leaf)) { throw "Missing mandatory tracked evidence log: $evidenceLog" }
$evidenceText = [IO.File]::ReadAllText($evidenceLog)
$forbiddenEvidencePatterns = @('(?i)(?:(?<![a-z])[a-z]:[\\/]|/run/desktop/mnt/host/[a-z]/|/(?:Users|home)/)', '(?im)^\s*(?:At line:\d+ char:\d+|CategoryInfo\s*:|FullyQualifiedErrorId\s*:|ScriptStackTrace\s*:|System\.Management\.Automation\.RemoteException)\b')
foreach ($pattern in $forbiddenEvidencePatterns) { if ($evidenceText -match $pattern) { throw "Review blocks final commit: tracked evidence contains a host-absolute path or PowerShell build-noise pattern: $pattern" } }
$evidenceLines = @([IO.File]::ReadAllLines($evidenceLog))
$resultLines = @($evidenceLines | Where-Object { $_ -match '^\[RESULT\] client=' })
if ($resultLines.Count -ne 3) { throw "Expected exactly 3 client RESULT lines, got $($resultLines.Count)" }
$resultRecords = foreach ($line in $resultLines) {
    if ($line -notmatch '^\[RESULT\] client=(?<client>Rclone|Mc|Aws) status=(?<status>PASSED|FAILED|SKIPPED) dual_head=(?<dualHead>PASSED|NOT_RUN) detail=.* evidence=client-smoke\.log$') { throw "Malformed RESULT line: $line" }
    [pscustomobject]@{ Client = $Matches.client; Status = $Matches.status; DualHead = $Matches.dualHead }
}
if (@($resultRecords.Client | Sort-Object -Unique).Count -ne 3) { throw "RESULT lines must name Rclone, Mc, and Aws exactly once" }
$evidenceRecords = foreach ($line in @($evidenceLines | Where-Object { $_ -match '^\[EVIDENCE\] client=' })) {
    if ($line -notmatch '^\[EVIDENCE\] client=(?<client>Mc|Aws) verifier=(?<verifier>Mc|Aws) dual_head=(?<dualHead>PASSED) key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=\S+ content_length=\d+$') { throw "Malformed EVIDENCE line: $line" }
    if ($Matches.verifier -ne $Matches.client) { throw "EVIDENCE verifier must equal client: $line" }
    [pscustomobject]@{ Client = $Matches.client; Verifier = $Matches.verifier; DualHead = $Matches.dualHead }
}
if ($evidenceRecords.Count -ne 1 -or $evidenceRecords[0].Client -ne "Mc" -or $evidenceRecords[0].Verifier -ne "Mc") { throw "Review blocks final commit: expected exactly one Mc/Mc runtime EVIDENCE" }
if (@($resultRecords | Where-Object { $_.Status -eq "FAILED" }).Count -gt 0) { throw "Review blocks final commit: tracked evidence contains FAILED" }
$expectedResults = @{ Rclone = @("PASSED", "NOT_RUN"); Mc = @("PASSED", "PASSED"); Aws = @("SKIPPED", "NOT_RUN") }
foreach ($result in $resultRecords) { if ($result.Status -ne $expectedResults[$result.Client][0] -or $result.DualHead -ne $expectedResults[$result.Client][1]) { throw "Review blocks final commit: unexpected mandatory result for $($result.Client)" } }
$provedResults = @($resultRecords | Where-Object {
    $candidate = $_.Client
    $_.Client -eq "Mc" -and $_.Status -eq "PASSED" -and $_.DualHead -eq "PASSED" -and @($evidenceRecords | Where-Object { $_.Client -eq $candidate -and $_.Verifier -eq $candidate -and $_.DualHead -eq "PASSED" }).Count -eq 1
})
if ($provedResults.Count -ne 1) { throw "Review blocks final commit: exactly one Mc/Mc RESULT and EVIDENCE dual_head=PASSED pair is required" }
$matrixNames = @{ Rclone = "rclone"; Mc = "MinIO mc"; Aws = "AWS CLI" }
$matrixLines = [IO.File]::ReadAllLines("docs/client-compatibility.md")
foreach ($result in $resultRecords) {
    $matrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$result.Client])) \|" })
    if ($matrixRow.Count -ne 1 -or $matrixRow[0] -notmatch "\| $($result.Status) \| 2026-07-19 \|") { throw "Matrix was not updated to $($result.Status) for $($result.Client)" }
}
$provedClient = $provedResults[0].Client
$provedMatrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$provedClient])) \|" })
if ($provedMatrixRow.Count -ne 1 -or $provedMatrixRow[0] -notmatch 'docs/client-smoke-evidence-2026-07-19\.log' -or $provedMatrixRow[0] -notmatch 'dual_head=PASSED' -or $provedMatrixRow[0] -notmatch "client=$provedClient") { throw "PASSED matrix row must cite the tracked log and same-client dual_head=PASSED" }
$roadmapLines = [IO.File]::ReadAllLines("ROADMAP.md")
$rules = @{ "rclone" = '^- \[x\] rclone smoke'; "MinIO mc" = '^- \[x\] MinIO `mc` smoke'; "AWS CLI" = '^- \[x\] AWS CLI smoke' }
foreach ($client in $rules.Keys) {
    $row = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($client)) \|" })
    if ($row.Count -ne 1 -or $row[0] -notmatch '\| (?<status>PASSED|FAILED|SKIPPED) \| 2026-07-19 \|') { throw "Invalid matrix state for $client" }
    $status = $Matches.status
    if ($status -eq "FAILED") { throw "ROADMAP and final commit blocked by FAILED client: $client" }
    if ($status -eq "SKIPPED" -and $row[0] -notmatch '(?i)(not authorized|authorization.*absent|local image.*(absent|missing)|required.*image.*missing|pull.*not authorized)') { throw "SKIPPED reason cannot complete ROADMAP for $client" }
    $roadmapItem = @($roadmapLines | Where-Object { $_ -match $rules[$client] })
    if ($roadmapItem.Count -ne 1) { throw "Expected one checked ROADMAP item for $client" }
    if ($status -eq "SKIPPED" -and (-not $roadmapItem[0].Contains("artifact implemented but not executed") -or -not $roadmapItem[0].Contains("SKIPPED"))) { throw "SKIPPED ROADMAP wording is inaccurate for $client" }
    if ($status -eq "PASSED" -and -not $roadmapItem[0].Contains("smoke test PASSED")) { throw "PASSED ROADMAP wording is inaccurate for $client" }
}
$item9 = @($roadmapLines | Where-Object { $_ -match 'Verify HeadObject signatures for nested keys through direct docker networking and localhost' })
if ($item9.Count -ne 1 -or $item9[0] -notmatch '^- \[x\].*MinIO `mc` same-client dual endpoint `stat` PASSED.*client=Mc verifier=Mc.*docs/client-smoke-evidence-2026-07-19\.log') { throw "Review requires one checked Mc/Mc dual-endpoint stat item 9 with tracked-log evidence" }
```

Expected: the first command may show pre-existing upload rollback calls earlier in `object.rs`, but none in `delete_object`, `delete_objects`, or store delete code; Task 4's observed Kubo test is the executable no-unpin proof. The second command locates every core surface. The docs/ROADMAP diff shows all ten v0.2 outcomes and no v0.3 change. The review directly rejects a missing log, host-absolute paths, PowerShell build noise, anything other than exactly three RESULT records with `evidence=client-smoke.log`, duplicate/malformed clients, any actual `FAILED`, all-`SKIPPED` output, Rclone-only `PASSED/NOT_RUN`, a missing or cross-client Mc/Mc EVIDENCE pair, a matrix row without the tracked path and `dual_head=PASSED`, or a non-unique/non-checked item 9. Static writer count is not execution evidence.

Use this review map:

| ROADMAP v0.2 outcome | Implementation evidence |
|---|---|
| SQLite Compose startup | Task 1 `config.docker.toml`, `?mode=rwc`, tracked mount, listener health |
| GetBucketLocation | Tasks 1 and 4; reqwest SigV4 real TCP, `None` DTO, exact s3s 0.14 restXml decode, missing bucket |
| ListObjects v1 | Tasks 2 and 4; shared page, strict marker, delimiter/NextMarker |
| DeleteObjects | Tasks 3 and 4; idempotency, order, quiet, partial error, no unpin |
| rclone smoke | Task 5 isolated Compose-network mkdir/copy/ls/cat/deletefile/rmdir plus Task 6 `RESULT PASSED dual_head=NOT_RUN`; it produces no dual-head evidence; actual `FAILED` blocks |
| mc smoke | Task 5 alias/mb/cp/ls/cat/stat/rm/rb plus Task 6 `RESULT PASSED dual_head=PASSED` and exactly one `client=Mc verifier=Mc` evidence; actual `FAILED` blocks |
| AWS CLI baseline | Task 5 executable branch and accurate absent-image artifact-only `SKIPPED`; actual execution must update matrix and cannot fail |
| rclone options | Task 5 config/docs |
| nested HEAD two paths | Task 4 localhost TCP + Task 6 mandatory real Mc localhost/Compose-network `stat`, Mc/Mc `RESULT`/`EVIDENCE` and tracked log |
| compatibility matrix | Task 5 exact-status matrix/evidence procedure and Task 6 tracked-log/matrix/item-9 pre-commit semantic gate |

- [ ] **Step 7: Run final diff hygiene and ensure the ignored local config is untouched**

```powershell
git diff --check
git diff --name-only
git status --short
```

Expected: `git diff --check` exits 0; the name list is limited to the approved spec/plan and File Structure entries, including `docs/client-smoke-evidence-2026-07-19.log` only when Task 6's mandatory real smoke generated it. `config.toml`, generated DB files, temporary logs, and client credentials outside `test/test` are absent.

- [ ] **Step 8: Request Git-write authorization, then create exactly one commit**

Do not run this step merely because earlier tasks passed. Ask for explicit permission to stage and commit. Once granted, run:

```powershell
$evidenceLog = "docs/client-smoke-evidence-2026-07-19.log"
if (-not (Test-Path -LiteralPath $evidenceLog -PathType Leaf)) { throw "Final commit blocked: missing mandatory tracked evidence log: $evidenceLog" }
$evidenceText = [IO.File]::ReadAllText($evidenceLog)
$forbiddenEvidencePatterns = @('(?i)(?:(?<![a-z])[a-z]:[\\/]|/run/desktop/mnt/host/[a-z]/|/(?:Users|home)/)', '(?im)^\s*(?:At line:\d+ char:\d+|CategoryInfo\s*:|FullyQualifiedErrorId\s*:|ScriptStackTrace\s*:|System\.Management\.Automation\.RemoteException)\b')
foreach ($pattern in $forbiddenEvidencePatterns) { if ($evidenceText -match $pattern) { throw "Final commit blocked: tracked evidence contains a host-absolute path or PowerShell build-noise pattern: $pattern" } }
$evidenceLines = @([IO.File]::ReadAllLines($evidenceLog))
$resultLines = @($evidenceLines | Where-Object { $_ -match '^\[RESULT\] client=' })
if ($resultLines.Count -ne 3) { throw "Final commit blocked: expected exactly 3 client RESULT lines, got $($resultLines.Count)" }
$resultRecords = foreach ($line in $resultLines) {
  if ($line -notmatch '^\[RESULT\] client=(?<client>Rclone|Mc|Aws) status=(?<status>PASSED|FAILED|SKIPPED) dual_head=(?<dualHead>PASSED|NOT_RUN) detail=.* evidence=client-smoke\.log$') { throw "Final commit blocked by malformed RESULT line: $line" }
  [pscustomobject]@{ Client = $Matches.client; Status = $Matches.status; DualHead = $Matches.dualHead }
}
if (@($resultRecords.Client | Sort-Object -Unique).Count -ne 3) { throw "Final commit blocked: RESULT lines must name Rclone, Mc, and Aws exactly once" }
$evidenceRecords = foreach ($line in @($evidenceLines | Where-Object { $_ -match '^\[EVIDENCE\] client=' })) {
  if ($line -notmatch '^\[EVIDENCE\] client=(?<client>Mc|Aws) verifier=(?<verifier>Mc|Aws) dual_head=(?<dualHead>PASSED) key=nested/path/file\.txt localhost=http://127\.0\.0\.1:9000 network=http://gateway:9000 etag=\S+ content_length=\d+$') { throw "Final commit blocked by malformed EVIDENCE line: $line" }
  if ($Matches.verifier -ne $Matches.client) { throw "Final commit blocked: EVIDENCE verifier must equal client: $line" }
  [pscustomobject]@{ Client = $Matches.client; Verifier = $Matches.verifier; DualHead = $Matches.dualHead }
}
if ($evidenceRecords.Count -ne 1 -or $evidenceRecords[0].Client -ne "Mc" -or $evidenceRecords[0].Verifier -ne "Mc") { throw "Final commit blocked: expected exactly one Mc/Mc runtime EVIDENCE" }
if (@($resultRecords | Where-Object { $_.Status -eq "FAILED" }).Count -gt 0) { throw "Final commit blocked by FAILED client result in tracked evidence" }
$expectedResults = @{ Rclone = @("PASSED", "NOT_RUN"); Mc = @("PASSED", "PASSED"); Aws = @("SKIPPED", "NOT_RUN") }
foreach ($result in $resultRecords) { if ($result.Status -ne $expectedResults[$result.Client][0] -or $result.DualHead -ne $expectedResults[$result.Client][1]) { throw "Final commit blocked: unexpected mandatory result for $($result.Client)" } }
$provedResults = @($resultRecords | Where-Object {
  $candidate = $_.Client
  $_.Client -eq "Mc" -and $_.Status -eq "PASSED" -and $_.DualHead -eq "PASSED" -and @($evidenceRecords | Where-Object { $_.Client -eq $candidate -and $_.Verifier -eq $candidate -and $_.DualHead -eq "PASSED" }).Count -eq 1
})
if ($provedResults.Count -ne 1) { throw "Final commit blocked: exact Rclone PASSED/NOT_RUN, Mc PASSED/PASSED, Aws SKIPPED/NOT_RUN requires one Mc/Mc evidence pair" }
$matrixNames = @{ Rclone = "rclone"; Mc = "MinIO mc"; Aws = "AWS CLI" }
$matrixLines = [IO.File]::ReadAllLines("docs/client-compatibility.md")
foreach ($result in $resultRecords) {
  $matrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$result.Client])) \|" })
  if ($matrixRow.Count -ne 1 -or $matrixRow[0] -notmatch "\| $($result.Status) \| 2026-07-19 \|") { throw "Final commit blocked: matrix was not updated to $($result.Status) for $($result.Client)" }
}
$provedClient = $provedResults[0].Client
$provedMatrixRow = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($matrixNames[$provedClient])) \|" })
if ($provedMatrixRow.Count -ne 1 -or $provedMatrixRow[0] -notmatch 'docs/client-smoke-evidence-2026-07-19\.log' -or $provedMatrixRow[0] -notmatch 'dual_head=PASSED' -or $provedMatrixRow[0] -notmatch "client=$provedClient") { throw "Final commit blocked: PASSED matrix row must cite the tracked log and same-client dual_head=PASSED evidence" }
$roadmapLines = [IO.File]::ReadAllLines("ROADMAP.md")
$rules = @{ "rclone" = '^- \[x\] rclone smoke'; "MinIO mc" = '^- \[x\] MinIO `mc` smoke'; "AWS CLI" = '^- \[x\] AWS CLI smoke' }
foreach ($client in $rules.Keys) {
  $row = @($matrixLines | Where-Object { $_ -match "^\| $([regex]::Escape($client)) \|" })
  if ($row.Count -ne 1 -or $row[0] -notmatch '\| (?<status>PASSED|FAILED|SKIPPED) \| 2026-07-19 \|') { throw "Final commit blocked by invalid matrix state for $client" }
  $status = $Matches.status
  if ($status -eq "FAILED") { throw "Final commit blocked by FAILED client: $client" }
  if ($status -eq "SKIPPED" -and $row[0] -notmatch '(?i)(not authorized|authorization.*absent|local image.*(absent|missing)|required.*image.*missing|pull.*not authorized)') { throw "Final commit blocked by ineligible SKIPPED reason for $client" }
  $roadmapItem = @($roadmapLines | Where-Object { $_ -match $rules[$client] })
  if ($roadmapItem.Count -ne 1) { throw "Final commit requires one checked ROADMAP item for $client" }
  if ($status -eq "SKIPPED" -and (-not $roadmapItem[0].Contains("artifact implemented but not executed") -or -not $roadmapItem[0].Contains("SKIPPED"))) { throw "Final commit blocked by inaccurate SKIPPED wording for $client" }
  if ($status -eq "PASSED" -and -not $roadmapItem[0].Contains("smoke test PASSED")) { throw "Final commit blocked by inaccurate PASSED wording for $client" }
}
$item9 = @($roadmapLines | Where-Object { $_ -match 'Verify HeadObject signatures for nested keys through direct docker networking and localhost' })
if ($item9.Count -ne 1 -or $item9[0] -notmatch '^- \[x\].*MinIO `mc` same-client dual endpoint `stat` PASSED.*client=Mc verifier=Mc.*docs/client-smoke-evidence-2026-07-19\.log') { throw "Final commit blocked: item 9 must be uniquely checked with Mc/Mc dual-endpoint stat and tracked-log evidence" }
$paths = @(
  "docs/superpowers/specs/2026-07-19-client-compatibility-design.md",
  "docs/superpowers/plans/2026-07-19-client-compatibility.md",
  "src/config.rs",
  "src/s3/handler.rs",
  "src/s3/ops/bucket.rs",
  "src/s3/ops/object.rs",
  "src/store/object.rs",
  "tests/support/sigv4.rs",
  "tests/integration.rs",
  "docker-compose.yml",
  "config.example.toml",
  "config.docker.toml",
  "scripts/client-smoke.ps1",
  "docs/client-compatibility.md",
  "docs/client-smoke-evidence-2026-07-19.log",
  "ROADMAP.md"
)
git add -- $paths
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "feat: complete v0.2 client compatibility" -m "Add standard compatibility operations, Docker client smoke coverage, and documented verification evidence."
git log -1 --oneline
git status --short
```

Expected: the final pre-stage guard directly reads the tracked log and passes only when it contains no host-absolute path or PowerShell build noise, finds exactly three RESULT records ending in `evidence=client-smoke.log` with Rclone `PASSED/NOT_RUN`, Mc `PASSED/PASSED`, Aws `SKIPPED/NOT_RUN`, finds no actual `FAILED`, and finds exactly one Mc/Mc `RESULT`/`EVIDENCE` pair; it also proves that the Mc matrix row is `PASSED` and cites the log/dual-head fact, while the Rclone row claims only `NOT_RUN`, and that item 9 is uniquely `[x]` with `MinIO mc same-client dual endpoint stat PASSED` plus `client=Mc verifier=Mc`. All-`SKIPPED`, Rclone-only success, static-only writer evidence, a stale matrix row, a missing log, or an unmatched evidence line blocks staging. The paths include the approved evidence log and diff hygiene permits it. Then exactly one new commit has subject `feat: complete v0.2 client compatibility`, with no intermediate commits, no footers, and a clean worktree. If any client failed, a skip reason is ineligible, the real proof is absent, or Git authorization is not granted, leave the files uncommitted and report that state instead.

---

## Self-Review

- **Spec coverage:** Tasks 1-5 map one-to-one to all ten v0.2 rows. Task 5's all-`SKIPPED` template has 19 checks and item 9 `[ ] dual_head=NOT_RUN; release blocked`; Task 6 alone can complete item 9 after actual Mc/Mc dual-head proof in the tracked log. Rclone `PASSED/NOT_RUN` remains a valid client success but is insufficient.
- **Placeholder scan:** Every code/config/script/doc step contains concrete content, paths, commands, and expected outcomes. Task 5 reproduces the verified runner with named `Convert-ToPortableEvidence`, `Invoke-OfflineGatewayBuild`, and `Initialize-McAliases` helpers rather than referring to an unspecified script revision; the only tracked real-smoke path is `docs/client-smoke-evidence-2026-07-19.log`.
- **Type consistency:** The plan consistently uses `GetBucketLocationInput/Output`, `ListObjectsInput/Output`, `DeleteObjectsInput/Output`, `ListingPage.next_cursor`, and `delete_latest_if_present -> AppResult<bool>` as verified against `s3s` 0.14.0/current source.
- **Dependency consistency:** Task 2 builds the shared page before Task 4 wire tests; Task 3 builds idempotent delete before partial-error injection; Task 5 materializes Docker output arrays, normalizes `RunRoot`/`RepoRoot`, uses the offline vendor build only when standard Compose build images are absent, writes temporary `test`/`test` mc aliases, and isolates stack setup failure before per-client execution; Task 6 captures real output before inspecting it, rejects host paths and PowerShell build noise, parses exactly three `client-smoke.log` results, matches same-client evidence, then independently repeats that log/matrix/item-9 gate during review and immediately before staging.
- **Scope:** No schema migration, dependency addition, custom raw S3 route, client installation, image pull, automatic volume deletion, v0.3 feature, or new pin removal is planned. The sole additional tracked artifact is the test/test-only actual evidence log.

## Residual risks for the implementer

1. Docker Desktop host networking must support `--network host`; if it does not, the localhost client assertion must report `FAILED`, not silently switch to `host.docker.internal` because that would change the signed Host surface.
2. `minio/mc:latest` is intentionally recorded by image ID/runtime output when executed; its mutable tag makes the captured image ID mandatory evidence.
3. The Compose healthcheck reads `/proc/net/tcp`, uses source `$$local` for container-side shell expansion, and assumes gateway binds IPv4 `0.0.0.0:9000`, which `config.docker.toml` fixes explicitly. Compose 2.39.2 retains `$$local` in `config` serialization; Task 6's real healthy-container result is the runtime proof.
4. The AWS branch is executable but may remain `SKIPPED` while its local image is absent; it cannot satisfy item 9. Rclone `PASSED/NOT_RUN` cannot satisfy it either. Mc must leave the single same-client `client=Mc verifier=Mc` `RESULT`/`EVIDENCE` `dual_head=PASSED` pair in the tracked log.
5. The committed evidence log is intentionally limited to `test`/`test` smoke output; the Task 6 redaction guard rejects local configuration indicators, host-absolute paths, and PowerShell build noise after preserving output, so an unexpected emission blocks staging rather than being silently filtered.
6. The plan was written without compiling future edits; each task's RED/GREEN commands are mandatory and any `s3s` serialization mismatch must be fixed at the DTO adapter, not bypassed with a raw route.
