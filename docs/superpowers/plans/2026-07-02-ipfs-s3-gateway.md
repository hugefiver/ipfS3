# IPFS S3 Gateway Implementation Plan

> **For agentic workers:** Use the subagent-driven-development skill to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an S3-compatible gateway on top of IPFS (Kubo), supporting per-object/per-bucket encryption and Multipart Upload, verified end-to-end via `aws cli` against a local `docker compose` stack.

**Architecture:** Single binary `ipfs-s3-gateway`. Three layers: axum (HTTP server + `/health`) → s3s (SigV4 auth + S3 routing + DTO) → business logic holding `Arc<AppState>` (Kubo reqwest client, sea-orm DB, credentials, Master Key). Metadata in sea-orm (SQLite dev / Postgres prod, single migration). Content in Kubo via RPC (`:5001`). AES-256-GCM per-chunk encryption optional per-object.

**Tech Stack:** Rust (edition 2024, MSRV 1.92), axum 0.8+, s3s 0.13+, sea-orm 1+, reqwest 0.12+, aes-gcm 0.10+, tokio 1+, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-07-02-ipfs-s3-gateway-design.md`

---

## File Structure

```
ipfS3/
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── .dockerignore
├── AGENTS.md
├── config.example.toml
├── migrations/
│   └── m20250701_000001_init.rs        # single PG/SQLite-compatible migration
├── src/
│   ├── main.rs                          # entry point: load config, init state, build server, serve
│   ├── config.rs                        # toml + env config, serde structs
│   ├── state.rs                         # AppState: Kubo client, DB, creds, MK
│   ├── error.rs                         # AppError enum + From<AppError> for S3Error
│   ├── auth.rs                          # S3Auth impl: access_key → secret_key lookup
│   ├── kubo/
│   │   ├── mod.rs                       # KuboClient struct, re-exports
│   │   ├── client.rs                    # reqwest::Client wrapper, base URL, shared headers
│   │   ├── add.rs                       # stream_add(): POST /api/v0/add (chunked, wrap_stream)
│   │   ├── cat.rs                       # stream_cat(): GET /api/v0/cat?arg=CID (stream response)
│   │   └── pin.rs                       # pin_add(), pin_rm(): POST /api/v0/pin/{add,rm}
│   ├── store/
│   │   ├── mod.rs                       # Store struct (wraps DatabaseConnection), re-exports
│   │   ├── entities.rs                  # sea-orm entities: bucket, object, multipart_upload, multipart_part
│   │   ├── bucket.rs                    # bucket CRUD
│   │   ├── object.rs                    # object upsert/get/delete/list
│   │   └── multipart.rs                 # multipart_upload + multipart_part CRUD
│   ├── crypto/
│   │   ├── mod.rs                       # public API: encrypt_stream, decrypt_stream, EncryptionMode
│   │   ├── key.rs                       # MasterKey, ObjectKey, wrap/unwrap, HKDF nonce derivation
│   │   ├── aes_gcm.rs                   # AES-256-GCM encrypt/decrypt single chunk
│   │   └── chunker.rs                   # 256 KiB chunk iterator over byte stream
│   ├── pinning/
│   │   ├── mod.rs                       # PinningService trait
│   │   └── noop.rs                      # NoopPinningService (does nothing)
│   └── s3/
│       ├── mod.rs                       # S3Impl struct (holds Arc<AppState>), re-exports
│       ├── handler.rs                   # impl S3 for S3Impl — delegates to ops/*
│       └── ops/
│           ├── mod.rs
│           ├── bucket.rs                # create/delete/head/list_buckets
│           ├── object.rs                # put/get/head/delete/copy/list_objects_v2
│           └── multipart.rs             # create_multipart/upload_part/complete/abort/list_parts
└── tests/
    └── integration.rs                   # axum + s3s + SQLite + mock Kubo (wiremock)
```

---

## Task 1: Project Scaffold + Dependencies

**Files:**
- Create: `Cargo.toml`
- Create: `.gitignore`
- Create: `src/main.rs`
- Create: `src/lib.rs`

- [ ] **Step 1: Create `.gitignore`**

```
/target
/Cargo.lock
*.db
*.db-*
.env
config.toml
```

- [ ] **Step 2: Create `Cargo.toml`**

```toml
[package]
name = "ipfs-s3-gateway"
version = "0.1.0"
edition = "2024"
rust-version = "1.92"

[[bin]]
name = "ipfs-s3-gateway"
path = "src/main.rs"

[lib]
path = "src/lib.rs"

[dependencies]
# Web server
axum = "0.8"
tokio = { version = "1", features = ["full"] }
tower = "0.5"
hyper = { version = "1", features = ["full"] }
hyper-util = { version = "0.1", features = ["server-auto", "server-graceful", "tokio"] }

# S3 protocol
s3s = "0.13"

# HTTP client (Kubo RPC)
reqwest = { version = "0.12", features = ["stream", "json"] }

# Database
sea-orm = { version = "1", features = ["sqlx-sqlite", "sqlx-postgres", "runtime-tokio-rustls", "macros", "with-json", "with-uuid", "with-chrono"] }
sea-orm-migration = { version = "1", features = ["sqlx-sqlite", "sqlx-postgres", "runtime-tokio-rustls"] }

# Crypto
aes-gcm = "0.10"
sha2 = "0.10"
hmac = "0.12"
hkdf = "0.12"
rand = "0.8"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"

# Utils
uuid = { version = "1", features = ["v4"] }
bytes = "1"
futures-util = "0.3"
tokio-util = { version = "0.7", features = ["io"] }
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
async-trait = "0.1"
thiserror = "2"
hex = "0.4"
base64 = "0.22"
percent-encoding = "2"
http = "1"

[dev-dependencies]
wiremock = "0.6"
tempfile = "3"
```

- [ ] **Step 3: Create minimal `src/main.rs`**

```rust
fn main() {
    println!("ipfs-s3-gateway scaffold");
}
```

- [ ] **Step 4: Create minimal `src/lib.rs`**

```rust
//! ipfs-s3-gateway library root.
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check`
Expected: compiles with 0 errors (warnings OK).

- [ ] **Step 6: Upgrade all dependencies to latest**

Run: `cargo upgrade --incompatible`
Expected: all crate versions bumped to latest compatible/incompatible releases. If `cargo upgrade` is not found, install with `cargo install cargo-edit` then re-run.

- [ ] **Step 7: Verify it still compiles after upgrade**

Run: `cargo check`
Expected: compiles with 0 errors. If version conflicts arise, adjust `Cargo.toml` minimally until `cargo check` passes.

- [ ] **Step 8: Commit**

```
git add Cargo.toml Cargo.lock .gitignore src/main.rs src/lib.rs
git commit -m "chore: project scaffold with dependencies"
```

---

## Task 2: s3s + axum Integration Verification

**Goal:** Prove s3s + axum integration works before building business logic. Stand up a minimal server with `/health` + a DummyS3 that returns NotImplemented for all S3 ops.

**Files:**
- Modify: `src/main.rs`
- Create: `src/s3/mod.rs`
- Create: `src/s3/handler.rs`

- [ ] **Step 1: Create `src/s3/mod.rs`**

```rust
pub mod handler;
```

- [ ] **Step 2: Create `src/s3/handler.rs` with DummyS3**

```rust
use s3s::S3;
use s3s::S3Request;
use s3s::s3_error;

/// Minimal S3 implementation that returns NotImplemented for every operation.
/// Used only to verify the s3s + axum integration compiles and serves.
#[derive(Debug, Clone)]
pub struct DummyS3;

#[async_trait::async_trait]
impl S3 for DummyS3 {
    // Override nothing — all methods default to NotImplemented.
    // We explicitly list the ones we will implement later as no-ops for documentation.
}
```

- [ ] **Step 3: Rewrite `src/main.rs` with axum + s3s server**

```rust
use std::net::SocketAddr;

use axum::Router;
use axum::error_handling::HandleError;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use s3s::service::S3ServiceBuilder;

mod s3;

async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

async fn handle_s3_error(err: axum::Error) -> impl IntoResponse {
    tracing::error!(?err, "s3 service error");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3::handler::DummyS3);
        builder.build()
    };

    let s3_service = HandleError::new(s3_service, handle_s3_error);

    let app = Router::new()
        .route("/health", get(health_check))
        .fallback_service(s3_service);

    let addr: SocketAddr = "0.0.0.0:9000".parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check`
Expected: 0 errors. If `S3ServiceBuilder::new` signature differs from spec, consult `cargo doc --open -p s3s` or read `s3s::service` source.

- [ ] **Step 5: Verify it starts and `/health` works**

Run in one terminal: `cargo run`
Run in another: `curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/health`
Expected: `200`
Also test S3 path returns an S3 XML error: `curl -s http://localhost:9000/test-bucket`
Expected: XML body containing `NotImplemented` or `MethodNotAllowed` — confirms s3s is handling requests.
Stop the server with Ctrl+C.

- [ ] **Step 6: Commit**

```
git add src/main.rs src/s3/
git commit -m "feat: verify s3s + axum integration with DummyS3"
```

---

## Task 3: Config, Error, State Infrastructure

**Goal:** Build the three foundational modules: config loading (toml + env), error types (AppError → S3Error mapping), and AppState.

**Files:**
- Create: `src/config.rs`
- Create: `src/error.rs`
- Create: `src/state.rs`
- Modify: `src/main.rs`
- Modify: `src/lib.rs`
- Create: `config.example.toml`

- [ ] **Step 1: Create `src/error.rs`**

```rust
use s3s::S3Error;
use s3s::s3_error;

/// Application-level errors. Converted to S3Error at the S3 handler boundary.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("bucket not found: {0}")]
    NoSuchBucket(String),

    #[error("key not found: {0}")]
    NoSuchKey(String),

    #[error("bucket already exists: {0}")]
    BucketAlreadyExists(String),

    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),

    #[error("multipart upload not found: {0}")]
    NoSuchUpload(String),

    #[error("invalid part: {0}")]
    InvalidPart(String),

    #[error("invalid part order")]
    InvalidPartOrder,

    #[error("entity too small")]
    EntityTooSmall,

    #[error("invalid range")]
    InvalidRange,

    #[error("access denied: {0}")]
    AccessDenied(String),

    #[error("kubo rpc error: {0}")]
    KuboRpc(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("crypto error: {0}")]
    Crypto(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<AppError> for S3Error {
    fn from(e: AppError) -> Self {
        match &e {
            AppError::NoSuchBucket(_) => s3_error!(NoSuchBucket, "{}", e),
            AppError::NoSuchKey(_) => s3_error!(NoSuchKey, "{}", e),
            AppError::BucketAlreadyExists(_) => s3_error!(BucketAlreadyOwnedByYou, "{}", e),
            AppError::BucketNotEmpty(_) => s3_error!(BucketNotEmpty, "{}", e),
            AppError::NoSuchUpload(_) => s3_error!(NoSuchUpload, "{}", e),
            AppError::InvalidPart(_) => s3_error!(InvalidPart, "{}", e),
            AppError::InvalidPartOrder => s3_error!(InvalidPartOrder, "{}", e),
            AppError::EntityTooSmall => s3_error!(EntityTooSmall, "{}", e),
            AppError::InvalidRange => s3_error!(InvalidRange, "{}", e),
            AppError::AccessDenied(_) => s3_error!(AccessDenied, "{}", e),
            _ => s3_error!(InternalError, "{}", e),
        }
    }
}

/// Convenience type alias.
pub type AppResult<T> = Result<T, AppError>;

// From impls for wrapping external error types
impl From<sea_orm::DbErr> for AppError {
    fn from(e: sea_orm::DbErr) -> Self {
        AppError::Database(e.to_string())
    }
}

impl From<reqwest::Error> for AppError {
    fn from(e: reqwest::Error) -> Self {
        AppError::KuboRpc(e.to_string())
    }
}
```

- [ ] **Step 2: Create `src/config.rs`**

```rust
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub kubo: KuboConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub crypto: CryptoConfig,
    #[serde(default)]
    pub pinning: PinningConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,
}

fn default_bind() -> SocketAddr {
    "0.0.0.0:9000".parse().unwrap()
}

#[derive(Debug, Deserialize, Clone)]
pub struct KuboConfig {
    pub rpc_url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub database_url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// List of (access_key, secret_key) credential pairs.
    pub credentials: Vec<Credential>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Credential {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CryptoConfig {
    /// Master key as hex-encoded 32 bytes (256 bits). Required.
    pub master_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PinningConfig {
    #[serde(default = "default_pinning_provider")]
    pub provider: String,
}

fn default_pinning_provider() -> String {
    "noop".to_string()
}

impl Config {
    /// Load config from a toml file, then override with env vars.
    /// Env vars: IPFS_S3_CONFIG (path), or individual overrides:
    ///   IPFS_S3_BIND, IPFS_S3_KUBO_RPC_URL, IPFS_S3_DATABASE_URL,
    ///   IPFS_S3_ACCESS_KEY_ID, IPFS_S3_SECRET_ACCESS_KEY, IPFS_S3_MASTER_KEY
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("IPFS_S3_CONFIG").unwrap_or_else(|_| "config.toml".to_string());

        let mut cfg = if std::path::Path::new(&path).exists() {
            let content = std::fs::read_to_string(&path)?;
            toml::from_str::<Config>(&content)?
        } else {
            // Fallback: build from env vars only
            Config {
                server: ServerConfig {
                    bind: std::env::var("IPFS_S3_BIND")
                        .unwrap_or_else(|_| "0.0.0.0:9000".to_string())
                        .parse()?,
                },
                kubo: KuboConfig {
                    rpc_url: std::env::var("IPFS_S3_KUBO_RPC_URL")
                        .unwrap_or_else(|_| "http://127.0.0.1:5001".to_string()),
                },
                storage: StorageConfig {
                    database_url: std::env::var("IPFS_S3_DATABASE_URL")
                        .unwrap_or_else(|_| "sqlite::memory:".to_string()),
                },
                auth: AuthConfig {
                    credentials: vec![Credential {
                        access_key: std::env::var("IPFS_S3_ACCESS_KEY_ID")
                            .unwrap_or_else(|_| "test".to_string()),
                        secret_key: std::env::var("IPFS_S3_SECRET_ACCESS_KEY")
                            .unwrap_or_else(|_| "test".to_string()),
                    }],
                },
                crypto: CryptoConfig {
                    master_key: std::env::var("IPFS_S3_MASTER_KEY")
                        .unwrap_or_else(|_| {
                            // Dev default: 32 zero bytes hex-encoded. NOT for production.
                            "00".repeat(32)
                        }),
                },
                pinning: PinningConfig {
                    provider: "noop".to_string(),
                },
            }
        };

        // Env overrides take priority
        if let Ok(v) = std::env::var("IPFS_S3_BIND") {
            cfg.server.bind = v.parse()?;
        }
        if let Ok(v) = std::env::var("IPFS_S3_KUBO_RPC_URL") {
            cfg.kubo.rpc_url = v;
        }
        if let Ok(v) = std::env::var("IPFS_S3_DATABASE_URL") {
            cfg.storage.database_url = v;
        }
        if let Ok(v) = std::env::var("IPFS_S3_ACCESS_KEY_ID") {
            if let Ok(s) = std::env::var("IPFS_S3_SECRET_ACCESS_KEY") {
                cfg.auth.credentials = vec![Credential { access_key: v, secret_key: s }];
            }
        }
        if let Ok(v) = std::env::var("IPFS_S3_MASTER_KEY") {
            cfg.crypto.master_key = v;
        }

        Ok(cfg)
    }
}
```

- [ ] **Step 3: Create `src/state.rs`**

```rust
use std::collections::HashMap;
use std::sync::Arc;

use s3s::auth::SecretKey;
use sea_orm::DatabaseConnection;

use crate::config::Config;
use crate::crypto::key::MasterKey;
use crate::kubo::KuboClient;
use crate::store::Store;

/// Shared application state, wrapped in Arc and passed to the S3 handler.
pub struct AppState {
    pub kubo: KuboClient,
    pub store: Store,
    pub credentials: HashMap<String, SecretKey>,
    pub master_key: MasterKey,
}

impl AppState {
    pub async fn new(cfg: &Config) -> anyhow::Result<Arc<Self>> {
        let kubo = KuboClient::new(cfg.kubo.rpc_url.clone());

        let db = sea_orm::Database::connect(&cfg.storage.database_url).await?;
        // Run migrations
        crate::store::run_migrations(&db).await?;
        let store = Store::new(db);

        let mut credentials = HashMap::new();
        for cred in &cfg.auth.credentials {
            credentials.insert(cred.access_key.clone(), SecretKey::from(&cred.secret_key));
        }

        let master_key = MasterKey::from_hex(&cfg.crypto.master_key)?;

        Ok(Arc::new(Self {
            kubo,
            store,
            credentials,
            master_key,
        }))
    }
}
```

- [ ] **Step 4: Create `config.example.toml`**

```toml
[server]
bind = "0.0.0.0:9000"

[kubo]
rpc_url = "http://127.0.0.1:5001"

[storage]
database_url = "sqlite:///data/ipfs-s3.db"

[[auth.credentials]]
access_key = "test"
secret_key = "test"

[crypto]
master_key = "0000000000000000000000000000000000000000000000000000000000000000"

[pinning]
provider = "noop"
```

- [ ] **Step 5: Update `src/lib.rs` to declare modules**

```rust
pub mod config;
pub mod error;
pub mod state;
pub mod s3;
pub mod auth;
pub mod kubo;
pub mod store;
pub mod crypto;
pub mod pinning;
```

- [ ] **Step 6: Add `anyhow` dependency**

Run: `cargo add anyhow`
Expected: added to Cargo.toml.

- [ ] **Step 7: Verify it compiles**

Run: `cargo check`
Expected: errors about missing modules (kubo, store, crypto, pinning, auth). These will be resolved in subsequent tasks. For now, comment out the `pub mod` lines in `lib.rs` that don't exist yet, keeping only `config`, `error`, `s3`. Re-run `cargo check`.
Expected: 0 errors.

- [ ] **Step 8: Commit**

```
git add src/config.rs src/error.rs src/state.rs src/lib.rs config.example.toml Cargo.toml Cargo.lock
git commit -m "feat: config loading, error types, AppState skeleton"
```

---

## Task 4: S3Auth Implementation

**Goal:** Implement `S3Auth` trait to provide SigV4 credential lookup from AppState.

**Files:**
- Create: `src/auth.rs`

- [ ] **Step 1: Create `src/auth.rs`**

```rust
use std::sync::Arc;

use s3s::auth::{S3Auth, SecretKey};
use s3s::S3Result;

use crate::state::AppState;

pub struct GatewayAuth {
    state: Arc<AppState>,
}

impl GatewayAuth {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3Auth for GatewayAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.state
            .credentials
            .get(access_key)
            .cloned()
            .ok_or_else(|| s3s::s3_error!(InvalidAccessKeyId, "unknown access key: {access_key}"))
    }
}
```

- [ ] **Step 2: Uncomment `pub mod auth;` in `src/lib.rs`**

- [ ] **Step 3: Verify it compiles**

Run: `cargo check`
Expected: 0 errors (may still have warnings about unused code).

- [ ] **Step 4: Commit**

```
git add src/auth.rs src/lib.rs
git commit -m "feat: S3Auth implementation with credential lookup"
```

---

## Task 5: sea-orm Entities + Migration

**Goal:** Define all 4 entities and a single migration that creates all tables with PG/SQLite-compatible DDL.

**Files:**
- Create: `src/store/entities.rs`
- Create: `migrations/m20250701_000001_init.rs`
- Create: `src/store/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create entity files under `src/store/entities/`**

sea-orm requires each entity in its own module (each `#[derive(DeriveEntityModel)]` generates `Entity`, `Column`, `ActiveModel` types that must not collide). Create 5 files:

**File structure:**
```
src/store/entities/
├── mod.rs               // pub mod bucket; pub mod object; pub mod multipart_upload; pub mod multipart_part;
├── bucket.rs
├── object.rs
├── multipart_upload.rs
└── multipart_part.rs
```

**`src/store/entities/mod.rs`:**
```rust
pub mod bucket;
pub mod object;
pub mod multipart_upload;
pub mod multipart_part;
```

**`src/store/entities/bucket.rs`:**
```rust
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "buckets")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub name: String,
    pub created_at: DateTimeUtc,
    pub owner: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
```

**`src/store/entities/object.rs`:**
```rust
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "objects")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    /// Plaintext size in bytes.
    pub size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<Json>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub multipart: bool,
    pub is_latest: bool,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::bucket::Entity",
        from = "Column::Bucket",
        to = "super::bucket::Column::Name"
    )]
    Bucket,
}

impl Related<super::bucket::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Bucket.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
```

**`src/store/entities/multipart_upload.rs`:**
```rust
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "multipart_uploads")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub upload_id: String,
    pub object_id: String,
    pub bucket: String,
    pub key: String,
    pub created_at: DateTimeUtc,
    pub encryption_mode: String,  // "none" | "sse_s3" | "sse_c"
    pub key_wrap: Option<String>,
    pub content_type: Option<String>,
    pub metadata: Option<Json>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
```

**`src/store/entities/multipart_part.rs`:**
```rust
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "multipart_parts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub upload_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub part_number: i32,
    pub cid: String,
    pub size: i64,
    pub etag: String,
    pub uploaded_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
```

- [ ] **Step 2: Create migration file `migrations/m20250701_000001_init.rs`**

This migration uses raw SQL via `sea_orm_migration::sea_query` to ensure PG/SQLite compatibility.

```rust
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // buckets
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS buckets (
                    name TEXT PRIMARY KEY NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    owner TEXT
                )"#,
            )
            .await?;

        // objects
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS objects (
                    id TEXT PRIMARY KEY NOT NULL,
                    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                    key TEXT NOT NULL,
                    cid TEXT NOT NULL,
                    size BIGINT NOT NULL,
                    content_type TEXT,
                    etag TEXT NOT NULL,
                    metadata TEXT,
                    encrypted BOOLEAN NOT NULL DEFAULT FALSE,
                    key_wrap TEXT,
                    multipart BOOLEAN NOT NULL DEFAULT FALSE,
                    is_latest BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bucket, key, id)
                )"#,
            )
            .await?;

        // Partial unique index: only one latest per (bucket, key)
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_latest
                   ON objects (bucket, key) WHERE is_latest = TRUE"#,
            )
            .await?;

        // multipart_uploads
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS multipart_uploads (
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

        // multipart_parts
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS multipart_parts (
                    upload_id TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
                    part_number INTEGER NOT NULL,
                    cid TEXT NOT NULL,
                    size BIGINT NOT NULL,
                    etag TEXT NOT NULL,
                    uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (upload_id, part_number)
                )"#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS multipart_parts"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS multipart_uploads"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS objects"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS buckets"#)
            .await?;
        Ok(())
    }
}
```

- [ ] **Step 3: Create `src/store/mod.rs`**

```rust
pub mod entities;
pub mod bucket;
pub mod object;
pub mod multipart;

use sea_orm::DatabaseConnection;

pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }
}

/// Run all pending migrations.
pub async fn run_migrations(db: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    use sea_orm_migration::MigratorTrait;

    // Define a Migrator that lists our migration
    mod migrator {
        use sea_orm_migration::prelude::*;
        use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;

        pub struct Migrator;
        impl MigratorTrait for Migrator {
            fn migrations() -> Vec<Box<dyn MigrationTrait>> {
                vec![Box::new(InitMigration)]
            }
        }
    }

    migrator::Migrator::up(db, None).await
}
```

**Note:** The migration file needs to be accessible. Create `src/store/migrations/` directory with `mod.rs` re-exporting `m20250701_000001_init`. Move the migration file from `migrations/` to `src/store/migrations/m20250701_000001_init.rs`. Update `src/store/mod.rs` to declare `pub mod migrations;`.

- [ ] **Step 4: Reorganize migration into `src/store/migrations/`**

Create `src/store/migrations/mod.rs`:
```rust
pub mod m20250701_000001_init;
```

Move the migration file to `src/store/migrations/m20250701_000001_init.rs`.

Update `src/store/mod.rs` to add `pub mod migrations;` at the top.

Update the `run_migrations` function to reference `crate::store::migrations::m20250701_000001_init::Migration`.

- [ ] **Step 5: Comment out unimplemented store submodules**

In `src/store/mod.rs`, do NOT declare `bucket`, `object`, `multipart` yet (they are created in Task 6). The `mod.rs` should only have:

```rust
pub mod entities;
pub mod migrations;

use sea_orm::DatabaseConnection;

pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub fn db(&self) -> &DatabaseConnection {
        &self.db
    }
}

pub async fn run_migrations(db: &DatabaseConnection) -> Result<(), sea_orm::DbErr> {
    use sea_orm_migration::MigratorTrait;

    mod migrator {
        use sea_orm_migration::prelude::*;
        use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;

        pub struct Migrator;
        impl MigratorTrait for Migrator {
            fn migrations() -> Vec<Box<dyn MigrationTrait>> {
                vec![Box::new(InitMigration)]
            }
        }
    }

    migrator::Migrator::up(db, None).await
}
```

Task 6 will add `pub mod bucket; pub mod object; pub mod multipart;` after those files are created.

- [ ] **Step 6: Verify it compiles**

Run: `cargo check`
Expected: 0 errors. If sea-orm `Json` type import is missing, add `use sea_orm::prelude::*` which includes `Json`.

- [ ] **Step 7: Test migration runs on in-memory SQLite**

Create `src/store/mod.rs` test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_migration_runs() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        // Verify tables exist
        let result: i64 = db
            .query_one(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
                [],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by(0)
            .unwrap();
        assert!(result >= 4, "expected at least 4 tables, got {result}");
    }
}
```

- [ ] **Step 8: Run the test**

Run: `cargo test --lib store::tests::test_migration_runs`
Expected: PASS.

- [ ] **Step 9: Commit**

```
git add src/store/ src/lib.rs
git commit -m "feat: sea-orm entities and migration for all 4 tables"
```

---

## Task 6: Store CRUD Operations

**Goal:** Implement bucket, object, and multipart CRUD operations on top of sea-orm entities.

**Files:**
- Create: `src/store/bucket.rs`
- Create: `src/store/object.rs`
- Create: `src/store/multipart.rs`
- Modify: `src/store/mod.rs` (uncomment modules)
- Modify: `src/lib.rs` (uncomment `pub mod store;`)

- [ ] **Step 1: Create `src/store/bucket.rs`**

```rust
use chrono::Utc;
use sea_orm::*;

use super::entities::bucket;
use super::entities::bucket::Entity as BucketEntity;
use crate::error::{AppError, AppResult};

pub async fn create<C: ConnectionTrait>(db: &C, name: &str, owner: Option<&str>) -> AppResult<()> {
    let model = bucket::ActiveModel {
        name: Set(name.to_string()),
        created_at: Set(Utc::now()),
        owner: Set(owner.map(|s| s.to_string())),
    };
    match model.insert(db).await {
        Ok(_) => Ok(()),
        Err(DbErr::RecordNotInserted) | Err(DbErr::Query(RuntimeErr::SqlxError(_))) => {
            Err(AppError::BucketAlreadyExists(name.to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

pub async fn exists<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<bool> {
    let exists = BucketEntity::find()
        .filter(bucket::Column::Name.eq(name))
        .one(db)
        .await?
        .is_some();
    Ok(exists)
}

pub async fn delete<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<()> {
    // Check bucket is empty (no latest objects)
    let obj_count: i64 = super::entities::object::Entity::find()
        .filter(super::entities::object::Column::Bucket.eq(name))
        .filter(super::entities::object::Column::IsLatest.eq(true))
        .count(db)
        .await? as i64;
    if obj_count > 0 {
        return Err(AppError::BucketNotEmpty(name.to_string()));
    }

    let result = BucketEntity::delete_by_id(name.to_string())
        .exec(db)
        .await?;
    if result.rows_affected == 0 {
        return Err(AppError::NoSuchBucket(name.to_string()));
    }
    Ok(())
}

pub async fn list<C: ConnectionTrait>(db: &C) -> AppResult<Vec<bucket::Model>> {
    Ok(BucketEntity::find().all(db).await?)
}

pub async fn get<C: ConnectionTrait>(db: &C, name: &str) -> AppResult<bucket::Model> {
    BucketEntity::find_by_id(name.to_string())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchBucket(name.to_string()))
}
```

- [ ] **Step 2: Create `src/store/object.rs`**

```rust
use chrono::Utc;
use sea_orm::*;
use serde_json::Value as JsonValue;

use super::entities::object;
use super::entities::object::Entity as ObjectEntity;
use crate::error::{AppError, AppResult};

/// Upsert a new object version: mark previous latest as non-latest, insert new record.
pub async fn upsert<C: ConnectionTrait>(
    db: &C,
    id: &str,
    bucket: &str,
    key: &str,
    cid: &str,
    size: i64,
    content_type: Option<&str>,
    etag: &str,
    metadata: Option<JsonValue>,
    encrypted: bool,
    key_wrap: Option<&str>,
    multipart: bool,
) -> AppResult<()> {
    // Mark previous latest as non-latest
    ObjectEntity::update_many()
        .col_expr(object::Column::IsLatest, Expr::value(false))
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;

    // Insert new record
    let model = object::ActiveModel {
        id: Set(id.to_string()),
        bucket: Set(bucket.to_string()),
        key: Set(key.to_string()),
        cid: Set(cid.to_string()),
        size: Set(size),
        content_type: Set(content_type.map(|s| s.to_string())),
        etag: Set(etag.to_string()),
        metadata: Set(metadata),
        encrypted: Set(encrypted),
        key_wrap: Set(key_wrap.map(|s| s.to_string())),
        multipart: Set(multipart),
        is_latest: Set(true),
        created_at: Set(Utc::now()),
    };
    model.insert(db).await?;
    Ok(())
}

pub async fn get_latest<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<object::Model> {
    ObjectEntity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchKey(format!("{bucket}/{key}")))
}

pub async fn delete_latest<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<()> {
    // Mark as non-latest (soft delete for future versioning support)
    let result = ObjectEntity::update_many()
        .col_expr(object::Column::IsLatest, Expr::value(false))
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true))
        .exec(db)
        .await?;
    if result.rows_affected == 0 {
        return Err(AppError::NoSuchKey(format!("{bucket}/{key}")));
    }
    Ok(())
}

pub async fn list<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
    max_keys: u64,
) -> AppResult<Vec<object::Model>> {
    let mut query = ObjectEntity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::IsLatest.eq(true));

    if let Some(pfx) = prefix {
        query = query.filter(object::Column::Key.starts_with(pfx));
    }

    // Simple pagination: if continuation_token provided, filter key > token
    if let Some(token) = continuation_token {
        query = query.filter(object::Column::Key.gt(token));
    }

    Ok(query
        .order_by_asc(object::Column::Key)
        .limit(max_keys)
        .all(db)
        .await?)
}

pub async fn count<C: ConnectionTrait>(db: &C, bucket: &str) -> AppResult<u64> {
    Ok(ObjectEntity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::IsLatest.eq(true))
        .count(db)
        .await?)
}
```

- [ ] **Step 3: Create `src/store/multipart.rs`**

```rust
use chrono::Utc;
use sea_orm::*;
use serde_json::Value as JsonValue;

use super::entities::{multipart_part, multipart_upload};
use super::entities::multipart_upload::Entity as UploadEntity;
use super::entities::multipart_part::Entity as PartEntity;
use crate::error::{AppError, AppResult};

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
) -> AppResult<()> {
    let model = multipart_upload::ActiveModel {
        upload_id: Set(upload_id.to_string()),
        object_id: Set(object_id.to_string()),
        bucket: Set(bucket.to_string()),
        key: Set(key.to_string()),
        created_at: Set(Utc::now()),
        encryption_mode: Set(encryption_mode.to_string()),
        key_wrap: Set(key_wrap.map(|s| s.to_string())),
        content_type: Set(content_type.map(|s| s.to_string())),
        metadata: Set(metadata),
    };
    model.insert(db).await?;
    Ok(())
}

pub async fn get_upload<C: ConnectionTrait>(db: &C, upload_id: &str) -> AppResult<multipart_upload::Model> {
    UploadEntity::find_by_id(upload_id.to_string())
        .one(db)
        .await?
        .ok_or_else(|| AppError::NoSuchUpload(upload_id.to_string()))
}

pub async fn delete_upload<C: ConnectionTrait>(db: &C, upload_id: &str) -> AppResult<()> {
    let result = UploadEntity::delete_by_id(upload_id.to_string())
        .exec(db)
        .await?;
    if result.rows_affected == 0 {
        return Err(AppError::NoSuchUpload(upload_id.to_string()));
    }
    Ok(())
}

pub async fn insert_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
    cid: &str,
    size: i64,
    etag: &str,
) -> AppResult<()> {
    let model = multipart_part::ActiveModel {
        upload_id: Set(upload_id.to_string()),
        part_number: Set(part_number),
        cid: Set(cid.to_string()),
        size: Set(size),
        etag: Set(etag.to_string()),
        uploaded_at: Set(Utc::now()),
    };
    model.insert(db).await?;
    Ok(())
}

pub async fn list_parts<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
) -> AppResult<Vec<multipart_part::Model>> {
    Ok(PartEntity::find()
        .filter(multipart_part::Column::UploadId.eq(upload_id))
        .order_by_asc(multipart_part::Column::PartNumber)
        .all(db)
        .await?)
}

pub async fn get_part<C: ConnectionTrait>(
    db: &C,
    upload_id: &str,
    part_number: i32,
) -> AppResult<multipart_part::Model> {
    PartEntity::find()
        .filter(multipart_part::Column::UploadId.eq(upload_id))
        .filter(multipart_part::Column::PartNumber.eq(part_number))
        .one(db)
        .await?
        .ok_or_else(|| AppError::InvalidPart(format!("part {part_number} not found")))
}

pub async fn delete_parts<C: ConnectionTrait>(db: &C, upload_id: &str) -> AppResult<()> {
    PartEntity::delete_many()
        .filter(multipart_part::Column::UploadId.eq(upload_id))
        .exec(db)
        .await?;
    Ok(())
}
```

- [ ] **Step 4: Uncomment modules in `src/store/mod.rs`**

Change:
```rust
// pub mod bucket;
// pub mod object;
// pub mod multipart;
```
to:
```rust
pub mod bucket;
pub mod object;
pub mod multipart;
```

- [ ] **Step 5: Uncomment `pub mod store;` in `src/lib.rs`**

- [ ] **Step 6: Verify it compiles**

Run: `cargo check`
Expected: 0 errors. Fix any import issues (e.g., `sea_orm::prelude::*` for `Json`, `DateTimeUtc`).

- [ ] **Step 7: Write unit test for bucket CRUD**

Add to `src/store/bucket.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::run_migrations;

    async fn setup() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        run_migrations(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_bucket_crud() {
        let db = setup().await;

        // Create
        create(&db, "test-bucket", Some("owner")).await.unwrap();
        assert!(exists(&db, "test-bucket").await.unwrap());

        // Duplicate → error
        assert!(create(&db, "test-bucket", None).await.is_err());

        // List
        let buckets = list(&db).await.unwrap();
        assert_eq!(buckets.len(), 1);

        // Delete empty bucket
        delete(&db, "test-bucket").await.unwrap();
        assert!(!exists(&db, "test-bucket").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonempty_bucket_fails() {
        let db = setup().await;
        create(&db, "b1", None).await.unwrap();
        crate::store::object::upsert(
            &db, "id1", "b1", "key1", "cid1", 100, None, "etag1", None, false, None, false,
        ).await.unwrap();
        let result = delete(&db, "b1").await;
        assert!(result.is_err());
    }
}
```

- [ ] **Step 8: Run tests**

Run: `cargo test --lib store`
Expected: all tests PASS.

- [ ] **Step 9: Commit**

```
git add src/store/ src/lib.rs
git commit -m "feat: store CRUD for buckets, objects, multipart uploads"
```

---

## Task 7: Kubo RPC Client

**Goal:** Implement streaming `add`/`cat`/`pin_add`/`pin_rm` against Kubo HTTP RPC.

**Files:**
- Create: `src/kubo/mod.rs`
- Create: `src/kubo/client.rs`
- Create: `src/kubo/add.rs`
- Create: `src/kubo/cat.rs`
- Create: `src/kubo/pin.rs`

- [ ] **Step 1: Create `src/kubo/mod.rs`**

```rust
pub mod client;
pub mod add;
pub mod cat;
pub mod pin;

pub use client::KuboClient;
```

- [ ] **Step 2: Create `src/kubo/client.rs`**

```rust
use reqwest::Client;
use std::sync::Arc;

#[derive(Clone)]
pub struct KuboClient {
    base_url: Arc<str>,
    http: Client,
}

impl KuboClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.into(),
            http: Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn http(&self) -> &Client {
        &self.http
    }
}
```

- [ ] **Step 3: Create `src/kubo/add.rs`**

```rust
use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt};
use reqwest::Body as ReqwestBody;
use serde::Deserialize;

use super::client::KuboClient;
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
pub struct AddResponse {
    #[serde(rename = "Hash")]
    pub hash: String,
    #[serde(rename = "Size", default)]
    pub size: String,
}

/// Stream bytes to Kubo `/api/v0/add`. Returns the root CID.
///
/// The stream is consumed chunk-by-chunk; Kubo's /add endpoint reads the
/// multipart body and returns newline-delimited JSON. The final line is the
/// root object.
pub async fn stream_add<S, E>(
    kubo: &KuboClient,
    stream: S,
    cid_version: u8,
) -> AppResult<String>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    // Wrap the byte stream into a reqwest body
    let mapped = stream.map_ok(|b| b).map_err(|e| {
        let boxed: Box<dyn std::error::Error + Send + Sync> = e.into();
        boxed
    });
    let body = ReqwestBody::wrap_stream(mapped);

    let url = format!(
        "{}/api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false",
        kubo.base_url()
    );

    let resp = kubo.http().post(&url).body(body).send().await?;

    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::KuboRpc(format!("add failed: {text}")));
    }

    // Response is newline-delimited JSON; last line is the root
    let text = resp.text().await?;
    let mut last_hash: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: AddResponse = serde_json::from_str(line)
            .map_err(|e| AppError::KuboRpc(format!("parse add response: {e}")))?;
        last_hash = Some(parsed.hash);
    }

    last_hash.ok_or_else(|| AppError::KuboRpc("empty add response".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_stream_add_parses_cid() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Hash\":\"QmRoot\",\"Size\":\"100\"}\n",
            ))
            .mount(&server)
            .await;

        let kubo = KuboClient::new(server.uri());
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"hello"))]);
        let cid = stream_add(&kubo, stream, 1).await.unwrap();
        assert_eq!(cid, "QmRoot");
    }

    #[tokio::test]
    async fn test_stream_add_error_on_non_200() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(500).set_body_string("kubo error"))
            .mount(&server)
            .await;

        let kubo = KuboClient::new(server.uri());
        let stream = futures_util::stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(b"hello"))]);
        let result = stream_add(&kubo, stream, 1).await;
        assert!(result.is_err());
    }
}
```

- [ ] **Step 4: Create `src/kubo/cat.rs`**

```rust
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::io::StreamReader;

use super::client::KuboClient;
use crate::error::{AppError, AppResult};

/// Stream bytes from Kubo `/api/v0/cat?arg=CID`.
/// Optionally with a byte range [start, end) (HTTP Range semantics, end exclusive).
///
/// Returns a stream of byte chunks. The caller should pass this directly to
/// s3s Body::from_stream without collecting.
pub async fn stream_cat(
    kubo: &KuboClient,
    cid: &str,
    range: Option<(u64, u64)>,
) -> AppResult<impl Stream<Item = Result<Bytes, std::io::Error>>> {
    let url = if let Some((start, end)) = range {
        // Kubo /cat supports bytes=start-end (inclusive end)
        format!(
            "{}/api/v0/cat?arg={cid}&bytes={start}-{}",
            kubo.base_url(),
            end.saturating_sub(1)
        )
    } else {
        format!("{}/api/v0/cat?arg={cid}", kubo.base_url())
    };

    let resp = kubo.http().post(&url).send().await?;

    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::KuboRpc(format!("cat failed: {text}")));
    }

    // Convert reqwest response stream into a byte stream
    let stream = resp.bytes_stream().map(|r| {
        r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    });

    Ok(stream)
}

/// Helper: stream a CID into a Vec<u8>. Only for small objects (e.g. part concatenation).
pub async fn cat_to_vec(kubo: &KuboClient, cid: &str) -> AppResult<Vec<u8>> {
    let stream = stream_cat(kubo, cid, None).await?;
    let mut buf = Vec::new();
    tokio::pin!(stream);
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_stream_cat_returns_bytes() {
        let server = MockServer::start().await;
        let body = b"hello world".to_vec();

        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;

        let kubo = KuboClient::new(server.uri());
        let data = cat_to_vec(&kubo, "QmTest").await.unwrap();
        assert_eq!(data, body);
    }
}
```

- [ ] **Step 5: Create `src/kubo/pin.rs`**

```rust
use serde::Deserialize;

use super::client::KuboClient;
use crate::error::{AppError, AppResult};

#[derive(Debug, Deserialize)]
struct PinResponse {
    #[serde(rename = "Pins", default)]
    pins: Vec<String>,
}

/// Add a recursive pin for the given CID.
pub async fn pin_add(kubo: &KuboClient, cid: &str) -> AppResult<()> {
    let url = format!("{}/api/v0/pin/add?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;

    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::KuboRpc(format!("pin add failed: {text}")));
    }
    Ok(())
}

/// Remove a pin. Note: Kubo pin API has no reference counting — callers must
/// ensure the CID is not referenced by other objects before calling this.
pub async fn pin_rm(kubo: &KuboClient, cid: &str) -> AppResult<()> {
    let url = format!("{}/api/v0/pin/rm?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;

    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::KuboRpc(format!("pin rm failed: {text}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_pin_add_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let kubo = KuboClient::new(server.uri());
        pin_add(&kubo, "QmTest").await.unwrap();
    }

    #[tokio::test]
    async fn test_pin_rm_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let kubo = KuboClient::new(server.uri());
        pin_rm(&kubo, "QmTest").await.unwrap();
    }
}
```

- [ ] **Step 6: Uncomment `pub mod kubo;` in `src/lib.rs`**

- [ ] **Step 7: Verify it compiles**

Run: `cargo check`
Expected: 0 errors.

- [ ] **Step 8: Run tests**

Run: `cargo test --lib kubo`
Expected: all tests PASS.

- [ ] **Step 9: Commit**

```
git add src/kubo/ src/lib.rs
git commit -m "feat: Kubo RPC client with streaming add/cat/pin"
```

---

## Task 8: Crypto Module (AES-256-GCM)

**Goal:** Implement per-chunk AES-256-GCM encryption with HKDF-derived nonces and MasterKey/ObjectKey wrap/unwrap.

**Files:**
- Create: `src/crypto/mod.rs`
- Create: `src/crypto/key.rs`
- Create: `src/crypto/aes_gcm.rs`
- Create: `src/crypto/chunker.rs`

- [ ] **Step 1: Create `src/crypto/key.rs`**

```rust
use aes_gcm::{Aes256Gcm, KeyInit};
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::{AppError, AppResult};

/// 32-byte master key for SSE-S3 encryption.
pub struct MasterKey {
    bytes: [u8; 32],
}

impl MasterKey {
    /// Create from hex-encoded 32 bytes (64 hex chars).
    pub fn from_hex(hex_str: &str) -> AppResult<Self> {
        let bytes = hex::decode(hex_str)
            .map_err(|e| AppError::Crypto(format!("invalid master key hex: {e}")))?;
        if bytes.len() != 32 {
            return Err(AppError::Crypto(format!(
                "master key must be 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self { bytes: arr })
    }

    /// Generate a random object key (256 bits).
    pub fn generate_object_key(&self) -> ObjectKey {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        ObjectKey { bytes }
    }

    /// Wrap (encrypt) an object key with the master key.
    /// Returns hex-encoded nonce||ciphertext||tag.
    pub fn wrap(&self, ok: &ObjectKey) -> AppResult<String> {
        let cipher = Aes256Gcm::new_from_slice(&self.bytes)
            .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;

        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, ok.bytes.as_ref())
            .map_err(|e| AppError::Crypto(format!("wrap: {e}")))?;

        // nonce(12) || ciphertext+tag(32+16=48) = 60 bytes
        let mut combined = Vec::with_capacity(12 + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);

        Ok(hex::encode(&combined))
    }

    /// Unwrap (decrypt) a wrapped object key.
    pub fn unwrap(&self, wrapped_hex: &str) -> AppResult<ObjectKey> {
        let combined = hex::decode(wrapped_hex)
            .map_err(|e| AppError::Crypto(format!("invalid key_wrap hex: {e}")))?;
        if combined.len() < 12 + 16 {
            return Err(AppError::Crypto("wrapped key too short".into()));
        }

        let cipher = Aes256Gcm::new_from_slice(&self.bytes)
            .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;

        let nonce = aes_gcm::Nonce::from_slice(&combined[..12]);
        let plaintext = cipher
            .decrypt(nonce, &combined[12..])
            .map_err(|_| AppError::AccessDenied("failed to unwrap object key".into()))?;

        if plaintext.len() != 32 {
            return Err(AppError::Crypto(format!(
                "unwrapped key not 32 bytes: {}",
                plaintext.len()
            )));
        }

        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&plaintext);
        Ok(ObjectKey { bytes })
    }
}

/// 32-byte object key, one per object.
pub struct ObjectKey {
    pub bytes: [u8; 32],
}

impl ObjectKey {
    /// Derive a 12-byte GCM nonce from (object_id, chunk_index).
    /// Uses HKDF-SHA256 with info = "nonce" to mix in the inputs.
    pub fn derive_nonce(&self, object_id: &str, chunk_index: u64) -> [u8; 12] {
        let hk = Hkdf::<Sha256>::new(None, &self.bytes);
        let mut info = Vec::with_capacity(object_id.len() + 9);
        info.extend_from_slice(object_id.as_bytes());
        info.extend_from_slice(&chunk_index.to_be_bytes());
        let mut nonce = [0u8; 12];
        // HKDF expand with a label
        hk.expand(b"nonce-v1", &mut nonce)
            .expect("hkdf expand to 12 bytes always succeeds");
        // Mix in object_id + chunk_index via info parameter
        let mut full_nonce = [0u8; 12];
        let hk2 = Hkdf::<Sha256>::new(Some(&nonce), &info);
        hk2.expand(b"finalize", &mut full_nonce)
            .expect("hkdf expand to 12 bytes always succeeds");
        full_nonce
    }
}
```

**Note on nonce derivation:** The implementation above uses two rounds of HKDF to mix object_id, chunk_index, and object key. The spec says "object_id UTF-8 bytes (44 bytes for UUID string) || chunk_index (8 bytes u64) → HKDF-SHA256 → 12 bytes". A simpler, spec-faithful implementation:

```rust
pub fn derive_nonce(&self, object_id: &str, chunk_index: u64) -> [u8; 12] {
    let hk = Hkdf::<Sha256>::new(None, &self.bytes);
    let mut info = Vec::with_capacity(object_id.len() + 8);
    info.extend_from_slice(object_id.as_bytes());
    info.extend_from_slice(&chunk_index.to_be_bytes());
    let mut nonce = [0u8; 12];
    hk.expand(&info, &mut nonce)
        .expect("hkdf expand to 12 bytes always succeeds");
    nonce
}
```

Use the simpler version. Replace the body of `derive_nonce` with the simpler version above.

- [ ] **Step 2: Create `src/crypto/aes_gcm.rs`**

```rust
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use bytes::Bytes;

use crate::error::{AppError, AppResult};

use super::key::ObjectKey;

/// Encrypt a single chunk. Returns nonce||ciphertext||tag as one Bytes.
pub fn encrypt_chunk(ok: &ObjectKey, nonce: &[u8; 12], plaintext: &[u8]) -> AppResult<Bytes> {
    let cipher = Aes256Gcm::new_from_slice(&ok.bytes)
        .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;
    let nonce = Nonce::from_slice(nonce);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| AppError::Crypto(format!("encrypt: {e}")))?;

    // Output: nonce(12) || ciphertext+tag
    let mut out = Vec::with_capacity(12 + ciphertext.len());
    out.extend_from_slice(nonce);
    out.extend_from_slice(&ciphertext);
    Ok(Bytes::from(out))
}

/// Decrypt a single chunk. Input format: nonce(12) || ciphertext+tag.
pub fn decrypt_chunk(ok: &ObjectKey, encrypted: &[u8]) -> AppResult<Bytes> {
    if encrypted.len() < 12 + 16 {
        return Err(AppError::Crypto("encrypted chunk too short".into()));
    }
    let cipher = Aes256Gcm::new_from_slice(&ok.bytes)
        .map_err(|e| AppError::Crypto(format!("aes init: {e}")))?;
    let nonce = Nonce::from_slice(&encrypted[..12]);
    let plaintext = cipher
        .decrypt(nonce, &encrypted[12..])
        .map_err(|_| AppError::AccessDenied("decryption failed".into()))?;
    Ok(Bytes::from(plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let ok = ObjectKey { bytes: [0x42; 32] };
        let nonce = [0u8; 12];
        let plaintext = b"hello world, this is a test chunk";
        let encrypted = encrypt_chunk(&ok, &nonce, plaintext).unwrap();
        assert_ne!(&encrypted[..], plaintext);
        let decrypted = decrypt_chunk(&ok, &encrypted).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let ok1 = ObjectKey { bytes: [0x42; 32] };
        let ok2 = ObjectKey { bytes: [0x99; 32] };
        let nonce = [0u8; 12];
        let encrypted = encrypt_chunk(&ok1, &nonce, b"secret").unwrap();
        assert!(decrypt_chunk(&ok2, &encrypted).is_err());
    }
}
```

- [ ] **Step 3: Create `src/crypto/chunker.rs`**

```rust
use bytes::Bytes;
use futures_util::{Stream, StreamExt};

/// Chunk size: 256 KiB, aligned with Kubo's default chunking.
pub const CHUNK_SIZE: usize = 256 * 1024;

/// Collect a byte stream into chunks of CHUNK_SIZE.
/// The final chunk may be smaller.
pub async fn chunk_stream<S, E>(mut stream: S) -> impl Stream<Item = Result<Bytes, E>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    let mut buf = Vec::with_capacity(CHUNK_SIZE);
    async_stream::stream! {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    buf.extend_from_slice(&bytes);
                    while buf.len() >= CHUNK_SIZE {
                        let split = buf.split_off(CHUNK_SIZE);
                        yield Ok(Bytes::from(std::mem::take(&mut buf)));
                        buf = split;
                    }
                }
                Err(e) => yield Err(e),
            }
        }
        if !buf.is_empty() {
            yield Ok(Bytes::from(buf));
        }
    }
}

/// Encrypt a chunked stream. Each chunk is encrypted independently.
/// Yields encrypted chunks (nonce||ciphertext||tag).
pub fn encrypt_chunk_stream<S, E>(
    stream: S,
    ok: std::sync::Arc<super::key::ObjectKey>,
    object_id: String,
) -> impl Stream<Item = Result<Bytes, crate::error::AppError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    use crate::crypto::aes_gcm::encrypt_chunk;
    let mut chunk_index: u64 = 0;
    async_stream::stream! {
        let mut chunked = Box::pin(chunk_stream::<_, E>(stream));
        while let Some(chunk) = chunked.next().await {
            match chunk {
                Ok(bytes) => {
                    let nonce = ok.derive_nonce(&object_id, chunk_index);
                    let encrypted = encrypt_chunk(&ok, &nonce, &bytes)?;
                    chunk_index += 1;
                    yield Ok(encrypted);
                }
                Err(e) => {
                    yield Err(crate::error::AppError::Internal(e.into().to_string()));
                }
            }
        }
    }
}

/// Decrypt a stream of encrypted chunks. Yields plaintext chunks.
pub fn decrypt_chunk_stream<S, E>(
    stream: S,
    ok: std::sync::Arc<super::key::ObjectKey>,
) -> impl Stream<Item = Result<Bytes, crate::error::AppError>>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    use crate::crypto::aes_gcm::decrypt_chunk;
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let plaintext = decrypt_chunk(&ok, &bytes)?;
                    yield Ok(plaintext);
                }
                Err(e) => {
                    yield Err(crate::error::AppError::Internal(e.into().to_string()));
                }
            }
        }
    }
}
```

- [ ] **Step 4: Add `async-stream` dependency**

Run: `cargo add async-stream`
Expected: added to Cargo.toml.

- [ ] **Step 5: Create `src/crypto/mod.rs`**

```rust
pub mod key;
pub mod aes_gcm;
pub mod chunker;

pub use key::{MasterKey, ObjectKey};

/// Encryption mode for an S3 operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    /// No encryption. Object is plaintext in IPFS.
    None,
    /// SSE-S3: gateway-managed key (MasterKey → ObjectKey, wrapped in DB).
    SseS3,
    /// SSE-C: customer-provided key (ObjectKey = customer key, not stored).
    SseC,
}

impl EncryptionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SseS3 => "sse_s3",
            Self::SseC => "sse_c",
        }
    }
}
```

- [ ] **Step 6: Add `subtle` dependency for constant-time comparison**

Run: `cargo add subtle`
Expected: added.

- [ ] **Step 5b: Uncomment `pub mod crypto;` in `src/lib.rs`**

- [ ] **Step 7: Verify it compiles**

Run: `cargo check`
Expected: 0 errors. Fix any import issues.

- [ ] **Step 8: Run tests**

Run: `cargo test --lib crypto`
Expected: all tests PASS. Specifically:
- `test_encrypt_decrypt_roundtrip` PASS
- `test_decrypt_with_wrong_key_fails` PASS

- [ ] **Step 9: Commit**

```
git add src/crypto/ Cargo.toml Cargo.lock src/lib.rs
git commit -m "feat: AES-256-GCM crypto with per-chunk encryption and HKDF nonces"
```

---

## Task 9: Pinning Service (Noop)

**Goal:** Define the `PinningService` trait and a Noop implementation for MVP.

**Files:**
- Create: `src/pinning/mod.rs`
- Create: `src/pinning/noop.rs`

- [ ] **Step 1: Create `src/pinning/mod.rs`**

```rust
pub mod noop;

use crate::error::AppResult;

/// Pinning service abstraction. In MVP, only NoopPinningService is used.
/// Future implementations (Pinata, etc.) can plug in here.
#[async_trait::async_trait]
pub trait PinningService: Send + Sync + 'static {
    /// Pin a CID on the remote pinning service.
    async fn pin(&self, cid: &str, name: Option<&str>) -> AppResult<()>;

    /// Unpin a CID from the remote pinning service.
    async fn unpin(&self, cid: &str) -> AppResult<()>;
}
```

- [ ] **Step 2: Create `src/pinning/noop.rs`**

```rust
use crate::error::AppResult;

pub struct NoopPinningService;

impl NoopPinningService {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopPinningService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl super::PinningService for NoopPinningService {
    async fn pin(&self, _cid: &str, _name: Option<&str>) -> AppResult<()> {
        // Noop: relies on local Kubo pin only.
        Ok(())
    }

    async fn unpin(&self, _cid: &str) -> AppResult<()> {
        Ok(())
    }
}
```

- [ ] **Step 3: Uncomment `pub mod pinning;` in `src/lib.rs`**

- [ ] **Step 4: Verify it compiles**

Run: `cargo check`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```
git add src/pinning/ src/lib.rs
git commit -m "feat: PinningService trait and Noop implementation"
```

---

## Task 10: S3 Bucket Operations

**Goal:** Implement `create_bucket`, `delete_bucket`, `head_bucket`, `list_buckets` S3 operations.

**Files:**
- Create: `src/s3/ops/mod.rs`
- Create: `src/s3/ops/bucket.rs`
- Modify: `src/s3/handler.rs` (replace DummyS3)

- [ ] **Step 1: Create `src/s3/ops/mod.rs`**

```rust
pub mod bucket;
pub mod object;
pub mod multipart;
```

- [ ] **Step 2: Create `src/s3/ops/bucket.rs`**

```rust
use std::sync::Arc;

use s3s::dto::*;
use s3s::S3Result;
use s3s::S3Request;
use s3s::S3Response;

use crate::state::AppState;

pub async fn create_bucket(
    state: &Arc<AppState>,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let input = req.input;
    let bucket = input.bucket.ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;

    crate::store::bucket::create(state.store.db(), &bucket, None)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    Ok(S3Response::new(CreateBucketOutput::default()))
}

pub async fn delete_bucket(
    state: &Arc<AppState>,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    let input = req.input;
    let bucket = input.bucket.ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;

    crate::store::bucket::delete(state.store.db(), &bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    Ok(S3Response::new(DeleteBucketOutput::default()))
}

pub async fn head_bucket(
    state: &Arc<AppState>,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    let input = req.input;
    let bucket = input.bucket.ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;

    let exists = crate::store::bucket::exists(state.store.db(), &bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
    }

    Ok(S3Response::new(HeadBucketOutput::default()))
}

pub async fn list_buckets(
    state: &Arc<AppState>,
    _req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let models = crate::store::bucket::list(state.store.db())
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    let buckets: Vec<Bucket> = models
        .into_iter()
        .map(|m| Bucket {
            name: Some(m.name),
            creation_date: Some(s3s::dto::Timestamp::from(m.created_at)),
        })
        .collect();

    let output = ListBucketsOutput {
        buckets: Some(buckets),
        owner: None,
        continuation_token: None,
    };

    Ok(S3Response::new(output))
}
```

- [ ] **Step 3: Update `src/s3/handler.rs` to hold AppState and delegate bucket ops**

Replace the entire `src/s3/handler.rs` with:

```rust
use std::sync::Arc;

use s3s::S3;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::*;

use crate::state::AppState;

mod ops;

/// S3 implementation backed by IPFS (Kubo) + sea-orm metadata.
#[derive(Clone)]
pub struct S3Impl {
    state: Arc<AppState>,
}

impl S3Impl {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3 for S3Impl {
    // ── Bucket operations ──

    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        ops::bucket::create_bucket(&self.state, req).await
    }

    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        ops::bucket::delete_bucket(&self.state, req).await
    }

    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        ops::bucket::head_bucket(&self.state, req).await
    }

    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        ops::bucket::list_buckets(&self.state, req).await
    }
}
```

**Note:** The `ops` module needs to be accessible from `handler.rs`. Restructure:

```
src/s3/
├── mod.rs           // pub mod handler; pub mod ops;
├── handler.rs       // S3Impl + impl S3
└── ops/
    ├── mod.rs       // pub mod bucket; pub mod object; pub mod multipart;
    ├── bucket.rs
    ├── object.rs    // Task 11
    └── multipart.rs // Task 12
```

Update `src/s3/mod.rs`:
```rust
pub mod handler;
pub mod ops;
```

Move `ops/mod.rs` content into the `src/s3/ops/mod.rs` (already created in Step 1). Remove the `mod ops;` line from `handler.rs` — it should be `use super::ops;` instead.

Corrected `handler.rs` header:
```rust
use std::sync::Arc;

use s3s::S3;
use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::*;

use crate::state::AppState;

/// S3 implementation backed by IPFS (Kubo) + sea-orm metadata.
#[derive(Clone)]
pub struct S3Impl {
    state: Arc<AppState>,
}

impl S3Impl {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl S3 for S3Impl {
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        super::ops::bucket::create_bucket(&self.state, req).await
    }
    // ... etc
}
```

- [ ] **Step 4: Create placeholder `src/s3/ops/object.rs` and `src/s3/ops/multipart.rs`**

For now, empty files so `ops/mod.rs` compiles:

`src/s3/ops/object.rs`:
```rust
// Implemented in Task 11
```

`src/s3/ops/multipart.rs`:
```rust
// Implemented in Task 12
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check`
Expected: 0 errors.

- [ ] **Step 6: Commit**

```
git add src/s3/
git commit -m "feat: S3 bucket operations (create/delete/head/list)"
```

---

## Task 11: S3 Object Operations

**Goal:** Implement `put_object`, `get_object`, `head_object`, `delete_object`, `copy_object`, `list_objects_v2`.

**Files:**
- Modify: `src/s3/ops/object.rs`
- Modify: `src/s3/handler.rs` (add object method delegations)

This is the largest task. Implement operations in order: PutObject (plain) → GetObject (plain) → HeadObject → DeleteObject → CopyObject → ListObjectsV2 → PutObject/GetObject (encrypted).

- [ ] **Step 1: Write `src/s3/ops/object.rs` — PutObject (plain)**

```rust
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use s3s::dto::*;
use s3s::{Body as S3Body, S3Request, S3Response, S3Result};

use crate::crypto::EncryptionMode;
use crate::error::AppError;
use crate::state::AppState;

/// Determine encryption mode from S3 headers.
fn determine_encryption_mode(headers: &http::HeaderMap) -> Result<EncryptionMode, s3s::S3Error> {
    if let Some(val) = headers.get("x-amz-server-side-encryption-customer-algorithm") {
        // SSE-C: customer provides key
        if val == "AES256" {
            return Ok(EncryptionMode::SseC);
        }
        return Err(s3s::s3_error!(InvalidArgument, "unsupported SSE-C algorithm"));
    }
    if let Some(val) = headers.get("x-amz-server-side-encryption") {
        if val == "AES256" {
            return Ok(EncryptionMode::SseS3);
        }
        return Err(s3s::s3_error!(InvalidArgument, "unsupported SSE value"));
    }
    // Default: no encryption (plain)
    Ok(EncryptionMode::None)
}

/// Extract custom metadata from x-amz-meta-* headers.
fn extract_custom_metadata(headers: &http::HeaderMap) -> Option<serde_json::Value> {
    let mut map = serde_json::Map::new();
    for (key, value) in headers.iter() {
        let key_str = key.as_str();
        if let Some(rest) = key_str.strip_prefix("x-amz-meta-") {
            if let Ok(v) = value.to_str() {
                map.insert(rest.to_string(), serde_json::Value::String(v.to_string()));
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(map))
    }
}

pub async fn put_object(
    state: &Arc<AppState>,
    req: S3Request<PutObjectInput>,
) -> S3Result<S3Response<PutObjectOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let content_type = input.content_type.clone();

    // Check bucket exists
    if !crate::store::bucket::exists(state.store.db(), &bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?
    {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
    }

    // Determine encryption mode
    let enc_mode = determine_encryption_mode(&req.headers)?;

    // Extract custom metadata
    let metadata = extract_custom_metadata(&req.headers);

    // Generate object ID (UUID string, used for nonce derivation)
    let object_id = uuid::Uuid::new_v4().to_string();

    // Convert S3 body to a byte stream and wrap with a counter
    let body = req.input.body;
    let raw_stream = body.into_byte_stream();

    // ByteCounter wraps the stream and counts plaintext bytes as they pass through.
    let (counter, count_handle) = ByteCounter::new();
    let stream = counter.wrap(raw_stream);

    // Upload to Kubo
    let (cid, encrypted, key_wrap) = match enc_mode {
        EncryptionMode::None => {
            // Plain: stream directly to Kubo add
            let cid = crate::kubo::add::stream_add(state.kubo.clone(), stream, 1)
                .await
                .map_err(Into::<s3s::S3Error>::into)?;
            (cid, false, None)
        }
        EncryptionMode::SseS3 => {
            let ok = state.master_key.generate_object_key();
            let wrapped = state.master_key.wrap(&ok)
                .map_err(Into::<s3s::S3Error>::into)?;
            let enc_stream = crate::crypto::chunker::encrypt_chunk_stream(
                stream,
                Arc::new(ok),
                object_id.clone(),
            );
            let cid = crate::kubo::add::stream_add(state.kubo.clone(), enc_stream, 1)
                .await
                .map_err(Into::<s3s::S3Error>::into)?;
            (cid, true, Some(wrapped))
        }
        EncryptionMode::SseC => {
            // Extract customer key from header x-amz-server-side-encryption-customer-key
            let key_b64 = req.headers
                .get("x-amz-server-side-encryption-customer-key")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing SSE-C customer key"))?;
            let key_bytes = base64::engine::general_purpose::STANDARD.decode(key_b64)
                .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key: {e}"))?;
            if key_bytes.len() != 32 {
                return Err(s3s::s3_error!(InvalidArgument, "SSE-C key must be 32 bytes"));
            }
            let mut ok_arr = [0u8; 32];
            ok_arr.copy_from_slice(&key_bytes);
            let ok = crate::crypto::ObjectKey { bytes: ok_arr };
            let enc_stream = crate::crypto::chunker::encrypt_chunk_stream(
                stream,
                Arc::new(ok),
                object_id.clone(),
            );
            let cid = crate::kubo::add::stream_add(state.kubo.clone(), enc_stream, 1)
                .await
                .map_err(Into::<s3s::S3Error>::into)?;
            (cid, true, None) // SSE-C: key_wrap is None (customer holds key)
        }
    };

    // Plaintext size = total bytes that flowed through the counter
    let size = count_handle.load(std::sync::atomic::Ordering::Relaxed) as i64;

    // Pin the CID in Kubo
    crate::kubo::pin::pin_add(state.kubo.clone(), &cid)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Store metadata
    crate::store::object::upsert(
        state.store.db(),
        &object_id,
        &bucket,
        &key,
        &cid,
        size,
        content_type.as_deref(),
        &cid,  // ETag = CID
        metadata,
        encrypted,
        key_wrap.as_deref(),
        false,  // multipart = false
    )
    .await
    .map_err(Into::<s3s::S3Error>::into)?;

    let output = PutObjectOutput {
        e_tag: Some(cid.clone().into()),
        ..Default::default()
    };
    Ok(S3Response::new(output))
}
```

The `ByteCounter` struct wraps the input stream and counts plaintext bytes as they pass through, so `size` is always the plaintext size (not the ciphertext size).

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct ByteCounter {
    count: Arc<AtomicU64>,
}

impl ByteCounter {
    pub fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (Self { count: count.clone() }, count)
    }

    pub fn wrap<S, E>(&self, stream: S) -> impl Stream<Item = Result<Bytes, E>>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
    {
        let count = self.count.clone();
        async_stream::stream! {
            let mut s = Box::pin(stream);
            while let Some(chunk) = s.next().await {
                if let Ok(ref b) = chunk {
                    count.fetch_add(b.len() as u64, Ordering::Relaxed);
                }
                yield chunk;
            }
        }
    }
}
```

Add this struct to `src/s3/ops/object.rs` (at the top, after the imports). It is also used by `multipart.rs` via `use super::object::ByteCounter`.

- [ ] **Step 2: Add GetObject (plain + encrypted) to `src/s3/ops/object.rs`**

Append to the file:

```rust
pub async fn get_object(
    state: &Arc<AppState>,
    req: S3Request<GetObjectInput>,
) -> S3Result<S3Response<GetObjectOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;

    // Get object metadata from DB
    let obj = crate::store::object::get_latest(state.store.db(), &bucket, &key)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Parse range
    let range = input.range.as_deref();
    let (start, end) = parse_range(range, obj.size as u64)
        .map_err(Into::<s3s::S3Error>::into)?;

    let body_stream = if obj.encrypted {
        // Encrypted: fetch full ciphertext from Kubo, decrypt, then apply range
        let ok = if let Some(ref wrapped) = obj.key_wrap {
            // SSE-S3: unwrap from DB
            state.master_key.unwrap(wrapped)
                .map_err(Into::<s3s::S3Error>::into)?
        } else {
            // SSE-C: extract key from request headers
            let key_b64 = req.headers
                .get("x-amz-server-side-encryption-customer-key")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| s3s::s3_error!(AccessDenied, "SSE-C object requires customer key"))?;
            let key_bytes = base64::engine::general_purpose::STANDARD.decode(key_b64)
                .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key: {e}"))?;
            if key_bytes.len() != 32 {
                return Err(s3s::s3_error!(InvalidArgument, "SSE-C key must be 32 bytes"));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&key_bytes);
            crate::crypto::ObjectKey { bytes: arr }
        };

        let cat_stream = crate::kubo::cat::stream_cat(state.kubo.clone(), &obj.cid, None)
            .await
            .map_err(Into::<s3s::S3Error>::into)?;

        let decrypted = crate::crypto::chunker::decrypt_chunk_stream(cat_stream, Arc::new(ok));

        // For encrypted objects, MVP decrypts fully then applies range.
        // Collect, slice, re-stream. (Optimized in v0.9.)
        let collected: Vec<u8> = decrypted
            .collect::<Result<Vec<_>, _>>()
            .await
            .map_err(|e| s3s::s3_error!(InternalError, "decrypt: {e}"))?;

        let sliced = collected[start as usize..end as usize].to_vec();
        futures_util::stream::iter(vec![Ok(Bytes::from(sliced))])
    } else {
        // Plain: stream directly from Kubo with range
        let kubo_range = if range.is_some() {
            Some((start, end))
        } else {
            None
        };
        let s = crate::kubo::cat::stream_cat(state.kubo.clone(), &obj.cid, kubo_range)
            .await
            .map_err(Into::<s3s::S3Error>::into)?;
        s
    };

    // Convert to S3 body
    let body = S3Body::from(body_stream);

    let mut output = GetObjectOutput {
        body,
        content_length: Some((end - start) as i64),
        content_type: obj.content_type.as_deref().map(Into::into),
        e_tag: Some(obj.etag.clone().into()),
        last_modified: Some(s3s::dto::Timestamp::from(obj.created_at)),
        ..Default::default()
    };

    // Set encryption headers
    if obj.encrypted {
        if obj.key_wrap.is_some() {
            // SSE-S3
            output.server_side_encryption = Some(ServerSideEncryption::from_static("AES256"));
        }
        // SSE-C: no server_side_encryption field, but could set customer headers
    }

    // Apply range to output
    if range.is_some() {
        output.content_range = Some(format!("bytes {start}-{}/{obj.size}"));
    }

    Ok(S3Response::new(output))
}

/// Parse a Range header value "bytes=start-end" into (start, end_exclusive).
/// Returns (0, total_size) if range is None.
fn parse_range(range: Option<&str>, total_size: u64) -> Result<(u64, u64), AppError> {
    match range {
        None => Ok((0, total_size)),
        Some(r) => {
            let r = r.trim();
            let bytes_part = r.strip_prefix("bytes=")
                .ok_or_else(|| AppError::InvalidRange)?;
            let parts: Vec<&str> = bytes_part.split('-').collect();
            if parts.len() != 2 {
                return Err(AppError::InvalidRange);
            }
            let start = if parts[0].is_empty() {
                // Suffix range: bytes=-N → last N bytes
                let n: u64 = parts[1].parse().map_err(|_| AppError::InvalidRange)?;
                if n > total_size { 0 } else { total_size - n }
            } else {
                parts[0].parse().map_err(|_| AppError::InvalidRange)?
            };
            let end = if parts[1].is_empty() {
                total_size
            } else {
                let e: u64 = parts[1].parse().map_err(|_| AppError::InvalidRange)?;
                (e + 1).min(total_size)
            };
            if start >= end {
                return Err(AppError::InvalidRange);
            }
            Ok((start, end))
        }
    }
}
```

- [ ] **Step 3: Add HeadObject, DeleteObject, CopyObject, ListObjectsV2**

Append to `src/s3/ops/object.rs`:

```rust
pub async fn head_object(
    state: &Arc<AppState>,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;

    let obj = crate::store::object::get_latest(state.store.db(), &bucket, &key)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    let mut output = HeadObjectOutput {
        content_length: Some(obj.size),
        content_type: obj.content_type.as_deref().map(Into::into),
        e_tag: Some(obj.etag.clone().into()),
        last_modified: Some(s3s::dto::Timestamp::from(obj.created_at)),
        ..Default::default()
    };

    if obj.encrypted && obj.key_wrap.is_some() {
        output.server_side_encryption = Some(ServerSideEncryption::from_static("AES256"));
    }

    Ok(S3Response::new(output))
}

pub async fn delete_object(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectInput>,
) -> S3Result<S3Response<DeleteObjectOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;

    // Only delete DB record. Do NOT pin_rm — Kubo has no refcount.
    crate::store::object::delete_latest(state.store.db(), &bucket, &key)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    Ok(S3Response::new(DeleteObjectOutput::default()))
}

pub async fn copy_object(
    state: &Arc<AppState>,
    req: S3Request<CopyObjectInput>,
) -> S3Result<S3Response<CopyObjectOutput>> {
    let input = req.input;
    let dst_bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let dst_key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let copy_source = input.copy_source.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing copy_source"))?;

    // Parse CopySource — format: "bucket/key"
    let src = match copy_source {
        CopySource::Bucket { bucket, key } => (bucket.to_string(), key.to_string()),
        _ => return Err(s3s::s3_error!(InvalidArgument, "unsupported copy source format")),
    };
    let (src_bucket, src_key) = src;

    // Get source object
    let src_obj = crate::store::object::get_latest(state.store.db(), &src_bucket, &src_key)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Check dst bucket exists
    if !crate::store::bucket::exists(state.store.db(), &dst_bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?
    {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {dst_bucket}"));
    }

    // Re-pin the CID (idempotent)
    crate::kubo::pin::pin_add(state.kubo.clone(), &src_obj.cid)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Insert new object record pointing to same CID
    let new_id = uuid::Uuid::new_v4().to_string();
    crate::store::object::upsert(
        state.store.db(),
        &new_id,
        &dst_bucket,
        &dst_key,
        &src_obj.cid,
        src_obj.size,
        src_obj.content_type.as_deref(),
        &src_obj.cid,
        src_obj.metadata.clone(),
        src_obj.encrypted,
        src_obj.key_wrap.as_deref(),
        src_obj.multipart,
    )
    .await
    .map_err(Into::<s3s::S3Error>::into)?;

    let output = CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: Some(src_obj.etag.clone().into()),
            last_modified: Some(s3s::dto::Timestamp::from(chrono::Utc::now())),
        }),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

pub async fn list_objects_v2(
    state: &Arc<AppState>,
    req: S3Request<ListObjectsV2Input>,
) -> S3Result<S3Response<ListObjectsV2Output>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let prefix = input.prefix.as_deref();
    let continuation_token = input.continuation_token.as_deref();
    let max_keys = input.max_keys.unwrap_or(1000).clamp(1, 1000) as u64;

    // Check bucket exists
    if !crate::store::bucket::exists(state.store.db(), &bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?
    {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
    }

    let objects = crate::store::object::list(
        state.store.db(),
        &bucket,
        prefix,
        continuation_token,
        max_keys,
    )
    .await
    .map_err(Into::<s3s::S3Error>::into)?;

    let contents: Vec<Object> = objects
        .iter()
        .map(|m| Object {
            key: Some(m.key.clone().into()),
            size: Some(m.size),
            e_tag: Some(m.etag.clone().into()),
            last_modified: Some(s3s::dto::Timestamp::from(m.created_at)),
            ..Default::default()
        })
        .collect();

    let is_truncated = objects.len() as u64 == max_keys;
    let next_token = if is_truncated {
        objects.last().map(|m| m.key.clone())
    } else {
        None
    };

    let output = ListObjectsV2Output {
        contents: Some(contents),
        is_truncated: Some(is_truncated),
        continuation_token: continuation_token.map(Into::into),
        next_continuation_token: next_token.map(Into::into),
        key_count: Some(objects.len() as i64),
        max_keys: Some(max_keys as i64),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}
```

- [ ] **Step 4: Update `src/s3/handler.rs` to delegate object ops**

Add to the `impl S3 for S3Impl` block:

```rust
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        super::ops::object::put_object(&self.state, req).await
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        super::ops::object::get_object(&self.state, req).await
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        super::ops::object::head_object(&self.state, req).await
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        super::ops::object::delete_object(&self.state, req).await
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        super::ops::object::copy_object(&self.state, req).await
    }

    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        super::ops::object::list_objects_v2(&self.state, req).await
    }
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check`
Expected: 0 errors. Fix any type mismatches (s3s DTO field names, `into()` on string types).

- [ ] **Step 6: Commit**

```
git add src/s3/ops/object.rs src/s3/handler.rs
git commit -m "feat: S3 object operations (put/get/head/delete/copy/list)"
```

---

## Task 12: S3 Multipart Operations

**Goal:** Implement `create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`, `list_parts`.

**Files:**
- Modify: `src/s3/ops/multipart.rs`
- Modify: `src/s3/handler.rs` (add multipart delegations)

- [ ] **Step 1: Write `src/s3/ops/multipart.rs`**

```rust
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use s3s::dto::*;
use s3s::{Body as S3Body, S3Request, S3Response, S3Result};

use crate::crypto::EncryptionMode;
use crate::error::AppError;
use crate::state::AppState;

use super::object::{ByteCounter, determine_encryption_mode, extract_custom_metadata};

pub async fn create_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<CreateMultipartUploadInput>,
) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let content_type = input.content_type.clone();

    // Check bucket exists
    if !crate::store::bucket::exists(state.store.db(), &bucket)
        .await
        .map_err(Into::<s3s::S3Error>::into)?
    {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {bucket}"));
    }

    // Determine encryption mode
    let enc_mode = determine_encryption_mode(&req.headers)?;
    let metadata = extract_custom_metadata(&req.headers);

    // Pre-generate object_id and upload_id
    let object_id = uuid::Uuid::new_v4().to_string();
    let upload_id = uuid::Uuid::new_v4().to_string();

    // Compute key_wrap for SSE-S3
    let key_wrap = match enc_mode {
        EncryptionMode::SseS3 => {
            let ok = state.master_key.generate_object_key();
            let wrapped = state.master_key.wrap(&ok)
                .map_err(Into::<s3s::S3Error>::into)?;
            Some(wrapped)
        }
        _ => None,
    };

    // Store upload record
    crate::store::multipart::create_upload(
        state.store.db(),
        &upload_id,
        &object_id,
        &bucket,
        &key,
        enc_mode.as_str(),
        key_wrap.as_deref(),
        content_type.as_deref(),
        metadata,
    )
    .await
    .map_err(Into::<s3s::S3Error>::into)?;

    let output = CreateMultipartUploadOutput {
        bucket: Some(bucket.into()),
        key: Some(key.into()),
        upload_id: Some(upload_id.into()),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

pub async fn upload_part(
    state: &Arc<AppState>,
    req: S3Request<UploadPartInput>,
) -> S3Result<S3Response<UploadPartOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let upload_id = input.upload_id.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing upload_id"))?;
    let part_number = input.part_number.unwrap_or(1) as i32;

    // Get upload record
    let upload = crate::store::multipart::get_upload(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    if upload.bucket != bucket || upload.key != key {
        return Err(s3s::s3_error!(InvalidArgument, "bucket/key mismatch"));
    }

    let body = req.input.body;
    let stream = body.into_byte_stream();

    let (counter, count_handle) = ByteCounter::new();
    let counted = counter.wrap(stream);

    let enc_mode = EncryptionMode::from_str(upload.encryption_mode.as_str());

    let cid = match enc_mode {
        EncryptionMode::None => {
            crate::kubo::add::stream_add(state.kubo.clone(), counted, 1)
                .await
                .map_err(Into::<s3s::S3Error>::into)?
        }
        EncryptionMode::SseS3 | EncryptionMode::SseC => {
            let ok = match enc_mode {
                EncryptionMode::SseS3 => {
                    let wrapped = upload.key_wrap.as_ref()
                        .ok_or_else(|| s3s::s3_error!(InternalError, "missing key_wrap for SSE-S3 upload"))?;
                    state.master_key.unwrap(wrapped)
                        .map_err(Into::<s3s::S3Error>::into)?
                }
                EncryptionMode::SseC => {
                    let key_b64 = req.headers
                        .get("x-amz-server-side-encryption-customer-key")
                        .and_then(|v| v.to_str().ok())
                        .ok_or_else(|| s3s::s3_error!(AccessDenied, "SSE-C upload requires customer key"))?;
                    let key_bytes = base64::engine::general_purpose::STANDARD.decode(key_b64)
                        .map_err(|e| s3s::s3_error!(InvalidArgument, "invalid SSE-C key: {e}"))?;
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&key_bytes);
                    crate::crypto::ObjectKey { bytes: arr }
                }
                _ => unreachable!(),
            };
            let enc_stream = crate::crypto::chunker::encrypt_chunk_stream(
                counted,
                Arc::new(ok),
                upload.object_id.clone(),
            );
            crate::kubo::add::stream_add(state.kubo.clone(), enc_stream, 1)
                .await
                .map_err(Into::<s3s::S3Error>::into)?
        }
    };

    let part_size = count_handle.load(std::sync::atomic::Ordering::Relaxed) as i64;

    // Pin the part CID (direct pin — will be removed after complete or on abort)
    crate::kubo::pin::pin_add(state.kubo.clone(), &cid)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Store part record. If DB fails, attempt to roll back the pin.
    if let Err(e) = crate::store::multipart::insert_part(
        state.store.db(),
        &upload_id,
        part_number,
        &cid,
        part_size,
        &cid,
    )
    .await
    {
        // Best-effort rollback
        let _ = crate::kubo::pin::pin_rm(state.kubo.clone(), &cid).await;
        return Err(Into::<s3s::S3Error>::into(AppError::from(e)));
    }

    let output = UploadPartOutput {
        e_tag: Some(cid.into()),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}
```

Add `EncryptionMode::from_str` to `src/crypto/mod.rs`:

```rust
impl EncryptionMode {
    pub fn from_str(s: &str) -> Self {
        match s {
            "sse_s3" => Self::SseS3,
            "sse_c" => Self::SseC,
            _ => Self::None,
        }
    }
}
```

- [ ] **Step 2: Add `complete_multipart_upload`**

Append to `src/s3/ops/multipart.rs`:

```rust
pub async fn complete_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<CompleteMultipartUploadInput>,
) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let upload_id = input.upload_id.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing upload_id"))?;

    // Parse the client-supplied part list from the request body (XML)
    let body = req.input.body;
    let body_bytes = body
        .store_all_limited(1024 * 1024)  // 1 MiB max for the part list XML
        .await
        .map_err(|e| s3s::s3_error!(InvalidArgument, "failed to read complete body: {e}"))?;

    let part_list: Vec<(i32, String)> = parse_complete_xml(&body_bytes)
        .map_err(|e| s3s::s3_error!(MalformedXML, "invalid complete XML: {e}"))?;

    if part_list.is_empty() {
        return Err(s3s::s3_error!(MalformedXML, "empty part list"));
    }

    // Validate part order (must be ascending by PartNumber, gaps allowed)
    let mut prev = 0;
    for (pn, _) in &part_list {
        if *pn <= prev {
            return Err(s3s::s3_error!(InvalidPartOrder, "parts must be in ascending order"));
        }
        prev = *pn;
    }

    // Get upload record
    let upload = crate::store::multipart::get_upload(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    if upload.bucket != bucket || upload.key != key {
        return Err(s3s::s3_error!(InvalidArgument, "bucket/key mismatch"));
    }

    // Validate each part's ETag
    let mut parts_to_concat = Vec::new();
    for (pn, client_etag) in &part_list {
        let part = crate::store::multipart::get_part(state.store.db(), &upload_id, *pn)
            .await
            .map_err(Into::<s3s::S3Error>::into)?;
        if part.etag != *client_etag {
            return Err(s3s::s3_error!(InvalidPart, "etag mismatch for part {pn}"));
        }
        parts_to_concat.push((part.cid.clone(), part.size));
    }

    // Concatenate all parts into a single stream and add to Kubo
    let kubo = state.kubo.clone();
    let concat_stream = async_stream::stream! {
        for (cid, _size) in &parts_to_concat {
            let part_stream = crate::kubo::cat::stream_cat(&kubo, cid, None)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            tokio::pin!(part_stream);
            while let Some(chunk) = part_stream.next().await {
                yield chunk;
            }
        }
    };

    let root_cid = crate::kubo::add::stream_add(state.kubo.clone(), concat_stream, 1)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Pin the root CID
    crate::kubo::pin::pin_add(state.kubo.clone(), &root_cid)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    // Compute total size (plaintext)
    let total_size: i64 = parts_to_concat.iter().map(|(_, s)| s).sum();

    // Insert the final object record
    let enc_mode = EncryptionMode::from_str(&upload.encryption_mode);
    let (encrypted, key_wrap) = match enc_mode {
        EncryptionMode::None => (false, None),
        EncryptionMode::SseS3 => (true, upload.key_wrap.clone()),
        EncryptionMode::SseC => (true, None),
    };

    crate::store::object::upsert(
        state.store.db(),
        &upload.object_id,
        &bucket,
        &key,
        &root_cid,
        total_size,
        upload.content_type.as_deref(),
        &root_cid,
        upload.metadata.clone(),
        encrypted,
        key_wrap.as_deref(),
        true,  // multipart = true
    )
    .await
    .map_err(Into::<s3s::S3Error>::into)?;

    // Clean up: remove parts direct pins (safe — part CIDs are unique to this upload)
    for (part_cid, _) in &parts_to_concat {
        let _ = crate::kubo::pin::pin_rm(state.kubo.clone(), part_cid).await;
    }

    // Delete multipart upload + parts records
    let _ = crate::store::multipart::delete_parts(state.store.db(), &upload_id).await;
    let _ = crate::store::multipart::delete_upload(state.store.db(), &upload_id).await;

    let output = CompleteMultipartUploadOutput {
        bucket: Some(bucket.into()),
        key: Some(key.into()),
        e_tag: Some(root_cid.clone().into()),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}

/// Parse the CompleteMultipartUpload XML body.
/// Expected format:
/// <CompleteMultipartUpload>
///   <Part><PartNumber>1</PartNumber><ETag>"abc"</ETag></Part>
///   ...
/// </CompleteMultipartUpload>
fn parse_complete_xml(xml: &[u8]) -> Result<Vec<(i32, String)>, Box<dyn std::error::Error>> {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);

    let mut parts: Vec<(i32, String)> = Vec::new();
    let mut current_part_number: Option<i32> = None;
    let mut current_etag: Option<String> = None;
    let mut current_tag: Option<String> = None;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                current_tag = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
            }
            Ok(Event::End(_)) => {
                if current_tag.as_deref() == Some("Part") {
                    if let (Some(pn), Some(etag)) = (current_part_number.take(), current_etag.take()) {
                        // Strip surrounding quotes from ETag if present
                        let etag = etag.trim_matches('"').to_string();
                        parts.push((pn, etag));
                    }
                }
                current_tag = None;
            }
            Ok(Event::Text(t)) => {
                let text = t.unescape_and_decode(&reader)?;
                match current_tag.as_deref() {
                    Some("PartNumber") => current_part_number = Some(text.parse()?),
                    Some("ETag") => current_etag = Some(text),
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => return Err(Box::new(e)),
        }
        buf.clear();
    }

    Ok(parts)
}
```

- [ ] **Step 3: Add `abort_multipart_upload` and `list_parts`**

Append to `src/s3/ops/multipart.rs`:

```rust
pub async fn abort_multipart_upload(
    state: &Arc<AppState>,
    req: S3Request<AbortMultipartUploadInput>,
) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
    let input = req.input;
    let upload_id = input.upload_id.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing upload_id"))?;

    // Get all parts and unpin them
    let parts = crate::store::multipart::list_parts(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    for part in parts {
        let _ = crate::kubo::pin::pin_rm(state.kubo.clone(), &part.cid).await;
    }

    // Delete DB records
    let _ = crate::store::multipart::delete_parts(state.store.db(), &upload_id).await;
    crate::store::multipart::delete_upload(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    Ok(S3Response::new(AbortMultipartUploadOutput::default()))
}

pub async fn list_parts(
    state: &Arc<AppState>,
    req: S3Request<ListPartsInput>,
) -> S3Result<S3Response<ListPartsOutput>> {
    let input = req.input;
    let bucket = input.bucket.clone().ok_or_else(|| s3s::s3_error!(InvalidBucketName, "missing bucket"))?;
    let key = input.key.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing key"))?;
    let upload_id = input.upload_id.clone().ok_or_else(|| s3s::s3_error!(InvalidArgument, "missing upload_id"))?;

    let upload = crate::store::multipart::get_upload(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    if upload.bucket != bucket || upload.key != key {
        return Err(s3s::s3_error!(InvalidArgument, "bucket/key mismatch"));
    }

    let parts = crate::store::multipart::list_parts(state.store.db(), &upload_id)
        .await
        .map_err(Into::<s3s::S3Error>::into)?;

    let parts_dto: Vec<Part> = parts
        .into_iter()
        .map(|p| Part {
            part_number: Some(p.part_number),
            e_tag: Some(p.etag.into()),
            size: Some(p.size),
            last_modified: Some(s3s::dto::Timestamp::from(p.uploaded_at)),
            ..Default::default()
        })
        .collect();

    let output = ListPartsOutput {
        bucket: Some(bucket.into()),
        key: Some(key.into()),
        upload_id: Some(upload_id.into()),
        parts: Some(parts_dto),
        ..Default::default()
    };

    Ok(S3Response::new(output))
}
```

- [ ] **Step 4: Add `quick-xml` dependency**

Run: `cargo add quick-xml`
Expected: added.

- [ ] **Step 5: Move `ByteCounter` and helpers to a shared location**

The `ByteCounter` and `determine_encryption_mode` / `extract_custom_metadata` are defined in `object.rs` but used by `multipart.rs`. Make them `pub` in `object.rs` (already done in Step 1) and import them in `multipart.rs` (already done via `use super::object::{...}`).

- [ ] **Step 6: Update `src/s3/handler.rs` to delegate multipart ops**

Add to the `impl S3 for S3Impl` block:

```rust
    async fn create_multipart_upload(&self, req: S3Request<CreateMultipartUploadInput>) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        super::ops::multipart::create_multipart_upload(&self.state, req).await
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        super::ops::multipart::upload_part(&self.state, req).await
    }

    async fn complete_multipart_upload(&self, req: S3Request<CompleteMultipartUploadInput>) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        super::ops::multipart::complete_multipart_upload(&self.state, req).await
    }

    async fn abort_multipart_upload(&self, req: S3Request<AbortMultipartUploadInput>) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        super::ops::multipart::abort_multipart_upload(&self.state, req).await
    }

    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        super::ops::multipart::list_parts(&self.state, req).await
    }
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo check`
Expected: 0 errors.

- [ ] **Step 8: Commit**

```
git add src/s3/ops/multipart.rs src/s3/handler.rs src/crypto/mod.rs Cargo.toml Cargo.lock
git commit -m "feat: S3 multipart operations (create/upload/complete/abort/list)"
```

---

## Task 13: Wire Up main.rs (Full Integration)

**Goal:** Replace the DummyS3-based `main.rs` with full integration: load config, init AppState, build S3Impl + GatewayAuth, wire into axum, serve.

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Rewrite `src/main.rs`**

```rust
use std::net::SocketAddr;

use axum::Router;
use axum::error_handling::HandleError;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use s3s::service::S3ServiceBuilder;

mod auth;
mod config;
mod crypto;
mod error;
mod kubo;
mod pinning;
mod s3;
mod state;
mod store;

use auth::GatewayAuth;
use config::Config;
use s3::handler::S3Impl;
use state::AppState;

async fn health_check() -> impl IntoResponse {
    StatusCode::OK
}

async fn handle_s3_error(err: axum::Error) -> impl IntoResponse {
    tracing::error!(?err, "s3 service error");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let cfg = Config::load()?;
    tracing::info!(bind = %cfg.server.bind, kubo = %cfg.kubo.rpc_url, "starting ipfs-s3-gateway");

    let state = AppState::new(&cfg).await?;

    let s3_impl = S3Impl::new(state.clone());
    let gateway_auth = GatewayAuth::new(state.clone());

    let s3_service = {
        let mut builder = S3ServiceBuilder::new(s3_impl);
        builder.set_auth(gateway_auth);
        builder.build()
    };

    let s3_service = HandleError::new(s3_service, handle_s3_error);

    let app = Router::new()
        .route("/health", get(health_check))
        .fallback_service(s3_service);

    let listener = tokio::net::TcpListener::bind(cfg.server.bind).await?;
    tracing::info!("listening on {}", cfg.server.bind);
    axum::serve(listener, app).await?;

    Ok(())
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```
git add src/main.rs
git commit -m "feat: wire up main.rs with full S3 + auth integration"
```

---

## Task 14: Integration Tests (Mock Kubo)

**Goal:** Write integration tests that start the full axum+s3s server with in-memory SQLite and a wiremock-mocked Kubo, then exercise S3 ops via aws-sdk-s3.

**Files:**
- Modify: `Cargo.toml` (add aws-sdk-s3 dev-dependency)
- Create: `tests/integration.rs`

- [ ] **Step 1: Add aws-sdk-s3 dev-dependency**

Run: `cargo add aws-sdk-s3 --dev`
Also: `cargo add aws-credential-types --dev`

- [ ] **Step 2: Create `tests/integration.rs`**

```rust
//! Integration tests: full axum + s3s + SQLite + wiremock Kubo.
//! Tests exercise the real S3 protocol path with aws-sdk-s3.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client as S3Client;
use s3s::service::S3ServiceBuilder;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;

/// Test harness: spins up mock Kubo + full gateway on a random port.
struct TestServer {
    pub s3: S3Client,
    pub addr: String,
    pub kubo: Arc<wiremock::MockServer>,
    _tx: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    async fn start() -> Self {
        let kubo = Arc::new(MockServer::start().await);

        // Configure gateway state with mock Kubo URL + in-memory SQLite
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
        let store = ipfs_s3_gateway::store::Store::new(db);

        let mut credentials = std::collections::HashMap::new();
        credentials.insert(
            "test".to_string(),
            s3s::auth::SecretKey::from("test"),
        );

        let master_key = ipfs_s3_gateway::crypto::MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        ).unwrap();

        let state = Arc::new(ipfs_s3_gateway::state::AppState {
            kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
            store,
            credentials,
            master_key,
        });

        let s3_impl = ipfs_s3_gateway::s3::handler::S3Impl::new(state.clone());
        let auth = ipfs_s3_gateway::auth::GatewayAuth::new(state.clone());

        let s3_service = {
            let mut b = S3ServiceBuilder::new(s3_impl);
            b.set_auth(auth);
            b.build()
        };

        let s3_service = axum::error_handling::HandleError::new(s3_service, |err: axum::Error| async move {
            tracing::error!(?err, "s3 error");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        });

        let app = axum::Router::new().fallback_service(s3_service);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
            let _ = rx.await;
        });

        let s3_config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-east-1"))
            .endpoint_url(format!("http://{addr}"))
            .credentials_provider(Credentials::new("test", "test", None, None, "static"))
            .behavior_version_latest()
            .force_path_style(true)
            .build();
        let s3 = S3Client::from_conf(s3_config);

        Self { s3, addr: addr.to_string(), kubo, _tx: tx }
    }
}

#[tokio::test]
async fn test_create_and_list_buckets() {
    let server = TestServer::start().await;

    // Create bucket
    server.s3.create_bucket().bucket("test-bkt").send().await.unwrap();

    // List buckets
    let resp = server.s3.list_buckets().send().await.unwrap();
    let names: Vec<_> = resp.buckets().iter().map(|b| b.name()).collect();
    assert!(names.contains(&"test-bkt"));
}

#[tokio::test]
async fn test_put_get_plain_object() {
    let server = TestServer::start().await;

    server.s3.create_bucket().bucket("bkt").send().await.unwrap();

    // Mock Kubo /add to return a CID
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "{\"Hash\":\"QmTestCid\",\"Size\":\"11\"}\n",
        ))
        .mount(&server.kubo)
        .await;

    // Mock Kubo /pin/add
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTestCid\"]}"))
        .mount(&server.kubo)
        .await;

    // Mock Kubo /cat to return the body
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello world".to_vec()))
        .mount(&server.kubo)
        .await;

    // PutObject (plain)
    let body = aws_sdk_s3::primitives::ByteStream::from_static(b"hello world");
    server.s3.put_object().bucket("bkt").key("hello.txt").body(body).send().await.unwrap();

    // GetObject
    let resp = server.s3.get_object().bucket("bkt").key("hello.txt").send().await.unwrap();
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), b"hello world");
    assert_eq!(resp.e_tag.as_deref(), Some("QmTestCid"));
}

#[tokio::test]
async fn test_wrong_credentials_rejected() {
    let server = TestServer::start().await;

    // Build a second S3 client with wrong credentials, pointed at the same gateway
    let bad_config = aws_sdk_s3::Config::builder()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{}", server.addr))
        .credentials_provider(Credentials::new("test", "wrong", None, None, "static"))
        .behavior_version_latest()
        .force_path_style(true)
        .build();
    let bad_client = S3Client::from_conf(bad_config);

    let result = bad_client.list_buckets().send().await;
    assert!(result.is_err(), "wrong credentials should be rejected");
}
```

**Note:** The `TestServer` struct needs to expose `addr` for the wrong-credentials test. Add a field `pub addr: String` to `TestServer` and capture it from `listener.local_addr()` in `start()`. Update the struct definition accordingly.

The integration test harness needs the gateway's public modules to be accessible. Ensure `src/lib.rs` exports them all as `pub mod`. Remove the `mod common;` line if no common helpers are used.

- [ ] **Step 3: Run integration tests**

Run: `cargo test --test integration`
Expected: Tests PASS. If aws-sdk-s3 version conflicts arise, pin to a compatible version.

- [ ] **Step 4: Commit**

```
git add tests/integration.rs Cargo.toml Cargo.lock
git commit -m "test: integration tests with mock Kubo and aws-sdk-s3"
```

---

## Task 15: Dockerfile

**Goal:** Multi-stage Dockerfile that builds the gateway binary and runs it.

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Create `.dockerignore`**

```
/target
.git
*.md
docs/
tests/
config.toml
*.db
*.db-*
```

- [ ] **Step 2: Create `Dockerfile`**

```dockerfile
# Stage 1: Build
FROM rust:1.92-bookworm AS builder

WORKDIR /app

# Install required system libraries for sea-orm (sqlite) and reqwest (tls)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs so we can build dependencies first
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs

# Build dependencies only
RUN cargo build --release || true

# Copy actual source
COPY src/ src/

# Touch main.rs to force rebuild of the binary
RUN touch src/main.rs

# Build the actual binary
RUN cargo build --release --bin ipfs-s3-gateway

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libsqlite3-0 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/ipfs-s3-gateway /app/ipfs-s3-gateway

EXPOSE 9000

ENV IPFS_S3_BIND=0.0.0.0:9000

ENTRYPOINT ["/app/ipfs-s3-gateway"]
```

- [ ] **Step 3: Verify the Dockerfile builds (optional, if Docker is available)**

Run: `docker build -t ipfs-s3-gateway:test .`
Expected: builds successfully. If Docker is not available, skip this step and verify in Task 16.

- [ ] **Step 4: Commit**

```
git add Dockerfile .dockerignore
git commit -m "infra: multi-stage Dockerfile for gateway"
```

---

## Task 16: docker-compose.yml

**Goal:** Dev compose with Kubo + gateway, fully offline-capable.

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  kubo:
    image: ipfs/kubo:latest
    container_name: ipfs-s3-kubo
    ports:
      - "5001:5001"   # RPC (internal, do not expose to public internet in prod)
      - "8080:8080"   # Gateway (optional, for direct IPFS access verification)
    volumes:
      - ipfs_data:/data/ipfs
    environment:
      - IPFS_PATH=/data/ipfs
      # Disable automatic GC to prevent deletion of pinned content
      - IPFS_GC_INTERVAL=never
    command: ["daemon", "--migrate=true", "--enable-gc=false"]
    healthcheck:
      test: ["CMD", "ipfs", "id"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s
    restart: unless-stopped

  gateway:
    build: .
    container_name: ipfs-s3-gateway
    ports:
      - "9000:9000"
    volumes:
      - gateway_data:/data
    environment:
      - IPFS_S3_BIND=0.0.0.0:9000
      - IPFS_S3_KUBO_RPC_URL=http://kubo:5001
      - IPFS_S3_DATABASE_URL=sqlite:///data/ipfs-s3.db
      - IPFS_S3_ACCESS_KEY_ID=test
      - IPFS_S3_SECRET_ACCESS_KEY=test
      - IPFS_S3_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000
      - RUST_LOG=info
    depends_on:
      kubo:
        condition: service_healthy
    restart: unless-stopped

volumes:
  ipfs_data:
  gateway_data:
```

- [ ] **Step 2: Commit**

```
git add docker-compose.yml
git commit -m "infra: docker-compose with kubo + gateway"
```

---

## Task 17: AGENTS.md

**Goal:** Write an AGENTS.md describing the project architecture, module boundaries, conventions, and how to test/deploy.

**Files:**
- Create: `AGENTS.md`

- [ ] **Step 1: Create `AGENTS.md`**

```markdown
# IPFS S3 Gateway — Agent Guide

## Overview

`ipfs-s3-gateway` is an S3-compatible gateway backed by IPFS (Kubo). It translates S3 API calls into Kubo RPC operations, with optional per-object AES-256-GCM encryption.

## Architecture

```
aws cli / sdk
    │  (SigV4)
    ▼
axum (HTTP :9000) ── /health ──► health_check
    │
    ▼ (fallback_service)
s3s (SigV4 verify + S3 route + DTO)
    │
    ▼
S3Impl (impl S3 trait) ── holds Arc<AppState>
    │
    ├── ops/bucket.rs     → store/bucket.rs   (sea-orm)
    ├── ops/object.rs     → store/object.rs   + kubo/add,cat,pin + crypto
    └── ops/multipart.rs  → store/multipart.rs + kubo + crypto
```

**AppState** holds: `KuboClient` (reqwest), `Store` (sea-orm DatabaseConnection), `credentials` (HashMap<access_key, SecretKey>), `master_key` (MasterKey).

## Module Map

| Module | Responsibility |
|--------|---------------|
| `main.rs` | Entry point: config, state, server |
| `config.rs` | Toml + env config loading |
| `state.rs` | AppState struct + init |
| `error.rs` | AppError → S3Error mapping |
| `auth.rs` | S3Auth impl (credential lookup) |
| `kubo/` | Kubo RPC client (add/cat/pin) |
| `store/` | sea-orm entities + CRUD |
| `crypto/` | AES-256-GCM + key wrap + chunker |
| `pinning/` | PinningService trait + Noop |
| `s3/handler.rs` | S3Impl: impl S3, delegates to ops |
| `s3/ops/` | Per-operation implementations |

## Key Design Decisions

1. **ETag = CID.** The S3 ETag for each object is its IPFS CID string (not MD5). This is a deliberate trade-off: enables `ipfs cat <cid>` for plain objects, but deviates from S3 standard. Clients with strict ETag-MD5 validation may need configuration.
2. **Default plain.** No encryption headers = plaintext storage. `x-amz-server-side-encryption: AES256` triggers SSE-S3 (gateway-managed key). SSE-C via customer key headers.
3. **Metadata in DB, content in IPFS.** All object metadata (key→CID mapping, size, encryption state) lives in sea-orm. Content lives in Kubo. This provides S3-strong-consistency via DB ACID + IPFS content addressing.
4. **No pin::rm on delete.** Kubo's pin API has no reference counting. Deleting an object only removes the DB record; the CID may still be referenced by other keys (e.g. via CopyObject). GC is disabled in dev.
5. **Multipart Complete = overall add.** Parts are individually added (each gets a part-CID). On Complete, all parts are concatenated and re-added as a single UnixFS file, producing a new root CID. This avoids manual dag-pb construction.
6. **Encrypted Range = full decrypt + slice.** MVP decrypts the entire object then slices to the requested range. v0.9 will optimize to chunk-level Range.

## Conventions

- **Rust edition 2024, MSRV 1.92.**
- **TDD:** write failing test first, then minimal implementation.
- **Streaming:** never `.collect()` an entire request body. Use `wrap_stream` / `from_stream` patterns.
- **Errors:** return `AppResult<T>` from store/kubo/crypto layers. Convert to `S3Error` at the S3 handler boundary via `Into`.
- **Commits:** semantic style (`feat:`, `fix:`, `chore:`, `test:`, `docs:`).
- **File boundaries:** one responsibility per file. If a file exceeds ~250 LOC, consider splitting.

## Testing

### Unit tests

```powershell
cargo test --lib
```

Tests live in each module's `#[cfg(test)] mod tests`. Kubo is mocked with `wiremock`. DB uses in-memory SQLite.

### Integration tests

```powershell
cargo test --test integration
```

Full axum + s3s + SQLite + mock Kubo. Uses `aws-sdk-s3` as the client.

### End-to-end (docker compose)

```powershell
docker compose up -d --build
```

Then use `aws cli`:

```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"

aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/file.txt
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/file.txt -
```

Verify plain object is accessible via IPFS:

```powershell
curl -X POST "http://localhost:5001/api/v0/cat?arg=<CID>"
```

Cleanup:

```powershell
docker compose down -v
```

## Configuration

Config is loaded from `config.toml` (if present) then overridden by environment variables. See `config.example.toml` for the full schema.

Key env vars:
- `IPFS_S3_BIND` — bind address (default `0.0.0.0:9000`)
- `IPFS_S3_KUBO_RPC_URL` — Kubo RPC URL (default `http://127.0.0.1:5001`)
- `IPFS_S3_DATABASE_URL` — sea-orm database URL (SQLite or Postgres)
- `IPFS_S3_ACCESS_KEY_ID` / `IPFS_S3_SECRET_ACCESS_KEY` — S3 credentials
- `IPFS_S3_MASTER_KEY` — hex-encoded 32-byte master key for SSE-S3

## Spec

See `docs/superpowers/specs/2026-07-02-ipfs-s3-gateway-design.md` for the full design document.
```

- [ ] **Step 2: Commit**

```
git add AGENTS.md
git commit -m "docs: AGENTS.md with architecture and conventions"
```

---

## Task 18: End-to-End Verification

**Goal:** Verify the full stack via docker compose + aws cli against all 15 acceptance criteria from the spec.

**Files:** none (verification only)

- [ ] **Step 1: Build and start the stack**

Run: `docker compose up -d --build`
Expected: both `kubo` and `gateway` containers reach healthy/running state.

Verify: `docker compose ps`
Expected: both services `Up`.

- [ ] **Step 2: Configure aws cli credentials**

```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "us-east-1"
```

- [ ] **Step 3: Acceptance #1 — docker compose up**

Already verified in Step 1. ✅

- [ ] **Step 4: Acceptance #2 — aws mb (create bucket)**

Run: `aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket`
Expected: `make_bucket: test-bucket`

- [ ] **Step 5: Acceptance #3 — cp upload + CID verifiable**

Create a test file, upload it, then verify the CID exists in Kubo:

```powershell
"hello world" | Out-File -Encoding utf8 testfile.txt
aws --endpoint-url http://localhost:9000 s3 cp testfile.txt s3://test-bucket/testfile.txt
```

Get the CID from the object metadata:

```powershell
aws --endpoint-url http://localhost:9000 s3api head-object --bucket test-bucket --key testfile.txt
```

Expected: response contains `ETag` field with a CID-like string (e.g. `Qm...` or `bafy...`).

Verify in Kubo:

```powershell
curl -s -X POST "http://localhost:5001/api/v0/cat?arg=<ETAG_VALUE>" | cat
```

Expected: prints the file content.

- [ ] **Step 6: Acceptance #4 — cp download matches upload**

Run: `aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/testfile.txt -`
Expected: prints `hello world` (or the file content).

- [ ] **Step 7: Acceptance #5 — ls (list objects)**

Run: `aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/`
Expected: lists `testfile.txt` with size and timestamp.

- [ ] **Step 8: Acceptance #6 — rm then get 404**

Run: `aws --endpoint-url http://localhost:9000 s3 rm s3://test-bucket/testfile.txt`
Then: `aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/`
Expected: empty listing.

- [ ] **Step 9: Acceptance #7 — CopyObject same CID**

```powershell
"copy source" | Out-File -Encoding utf8 src.txt
aws --endpoint-url http://localhost:9000 s3 cp src.txt s3://test-bucket/src.txt
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/src.txt s3://test-bucket/dst.txt
```

Get ETags of both:

```powershell
aws --endpoint-url http://localhost:9000 s3api head-object --bucket test-bucket --key src.txt
aws --endpoint-url http://localhost:9000 s3api head-object --bucket test-bucket --key dst.txt
```

Expected: both ETags are the same CID.

- [ ] **Step 10: Acceptance #8 — curl Range (plain object)**

Upload a larger file (e.g. 1 MB), then test Range:

```powershell
aws --endpoint-url http://localhost:9000 s3 cp largefile.bin s3://test-bucket/largefile.bin
```

Get the CID, then range-fetch from Kubo:

```powershell
curl -X POST "http://localhost:5001/api/v0/cat?arg=<CID>&bytes=0-99"
```

Expected: returns first 100 bytes.

Alternatively via S3 API:

```powershell
aws --endpoint-url http://localhost:9000 s3api get-object --bucket test-bucket --key largefile.bin --range bytes=0-99 output.bin
```

Expected: 100-byte `output.bin`.

- [ ] **Step 11: Acceptance #9 — wrong credentials 403**

```powershell
$env:AWS_SECRET_ACCESS_KEY = "wrong"
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
```

Expected: `403 Forbidden` or `SignatureDoesNotMatch` error.

Reset: `$env:AWS_SECRET_ACCESS_KEY = "test"`

- [ ] **Step 12: Acceptance #10 — Multipart 100MB / 10 parts**

Create a 100 MB file, upload with multipart:

```powershell
# Create 100 MB file
fsutil file createnew bigfile.bin 104857600

aws --endpoint-url http://localhost:9000 s3 cp bigfile.bin s3://test-bucket/bigfile.bin
```

aws cli automatically uses multipart for large files. Expected: upload succeeds.

Verify:

```powershell
aws --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/bigfile.bin
```

Expected: size = 104857600.

- [ ] **Step 13: Acceptance #11 — encrypted: ipfs cat ciphertext + GET plaintext**

Upload with SSE-S3:

```powershell
"secret data" | Out-File -Encoding utf8 secret.txt
aws --endpoint-url http://localhost:9000 s3 cp secret.txt s3://test-bucket/secret.txt --sse AES256
```

Get the CID from ETag, then:

```powershell
curl -X POST "http://localhost:5001/api/v0/cat?arg=<CID>"
```

Expected: returns ciphertext (binary, not "secret data").

Via S3 API:

```powershell
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/secret.txt -
```

Expected: returns `secret data`.

- [ ] **Step 14: Acceptance #12 — plain: ipfs cat plaintext**

Upload a plain object:

```powershell
"plain data" | Out-File -Encoding utf8 plain.txt
aws --endpoint-url http://localhost:9000 s3 cp plain.txt s3://test-bucket/plain.txt
```

Get the CID, then:

```powershell
curl -X POST "http://localhost:5001/api/v0/cat?arg=<CID>"
```

Expected: returns `plain data`.

- [ ] **Step 15: Acceptance #13 — SSE-C correct key OK + wrong key fails**

```powershell
# Generate a 32-byte customer key
$key = [byte[]]@(1..32)
$keyB64 = [Convert]::ToBase64String($key)

"customer encrypted" | Out-File -Encoding utf8 cust.txt
aws --endpoint-url http://localhost:9000 s3 cp cust.txt s3://test-bucket/cust.txt `
    --sse-customer-algorithm AES256 `
    --sse-customer-key $keyB64

# Get with correct key
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/cust.txt - `
    --sse-customer-algorithm AES256 `
    --sse-customer-key $keyB64
```

Expected: returns `customer encrypted`.

```powershell
# Get with wrong key
$wrongKey = [byte[]]@(255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255;255)
$wrongB64 = [Convert]::ToBase64String($wrongKey)
aws --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/cust.txt - `
    --sse-customer-algorithm AES256 `
    --sse-customer-key $wrongB64
```

Expected: fails with access denied / decryption error.

- [ ] **Step 16: Acceptance #14 — ETag = CID**

Already verified in Steps 5 and 9. ✅

- [ ] **Step 17: Acceptance #15 — docker compose down -v cleanup**

Run: `docker compose down -v`
Expected: containers and volumes removed.

- [ ] **Step 18: Final commit (if any fixes were made during verification)**

If any bugs were found and fixed during verification, commit them:

```
git add -A
git commit -m "fix: issues found during e2e verification"
```

---

## Self-Review

### Spec Coverage

| Spec Section | Task(s) | Status |
|---|---|---|
| 2.1 Architecture (axum→s3s→AppState) | Task 2, 13 | ✅ |
| 2.2 Tech selection (s3s, sea-orm, reqwest, aes-gcm) | Task 1, 3, 7, 8 | ✅ |
| 2.3 Module structure | Task 1 (file structure), 3-12 | ✅ |
| 3.0 Bucket operations data flow | Task 10 | ✅ |
| 3.1 PutObject (plain + SSE-S3 + SSE-C) | Task 11 | ✅ |
| 3.2 GetObject (plain + encrypted + Range) | Task 11 | ✅ |
| 3.3 HeadObject/HeadBucket | Task 10 (HeadBucket), 11 (HeadObject) | ✅ |
| 3.4 CopyObject | Task 11 | ✅ |
| 3.5 DeleteObject | Task 11 | Task 11 |
| 3.5 ListObjectsV2 | Task 11 | ✅ |
| 4.3 Nonce derivation (HKDF from object_id) | Task 8 | ✅ |
| 4.4 SSE mapping (default plain) | Task 11 | ✅ |
| 5.1 Multipart overview | Task 12 | ✅ |
| 5.2 CreateMultipartUpload, UploadPart, Complete, Abort, ListParts | Task 12 | ✅ |
| 5.3 Multipart + encryption interaction | Task 12 | ✅ |
| 6 DB Schema (4 tables) | Task 5 | ✅ |
| 7 Error mapping | Task 3 (AppError), 5-12 (per-op) | ✅ |
| 9.1 docker compose (kubo + gateway) | Task 16 | ✅ |
| 10 Config (toml + env) | Task 3 | ✅ |
| 11.4 Acceptance criteria (15) | Task 18 | ✅ |
| PinningService trait + Noop | Task 9 | ✅ |
| AGENTS.md | Task 17 | ✅ |

### Placeholder Scan

Searched for: "TBD", "TODO", "implement later", "fill in details", "Add appropriate error handling", "handle edge cases".

Found and resolved:
- Task 5 Step 5 originally said "TODO: implemented in Task 6" — corrected to comment out store submodules until Task 6.
- Task 11 Step 1 originally used `size = 0` placeholder — corrected with integrated `ByteCounter` wrapper that counts plaintext bytes as they flow through the stream.
- Task 5 entities originally in a single file — corrected to 5-file structure (mod.rs + 4 entity files) matching sea-orm requirements.
- Task 14 `test_wrong_credentials_rejected` was a placeholder — corrected to use `server.addr` field and a second S3 client with wrong credentials.
- Task 12 multipart_part entity lacked composite primary key annotations — corrected with `#[sea_orm(primary_key, auto_increment = false)]` on both `upload_id` and `part_number`.

### Type Consistency

Checked across tasks:
- `KuboClient` — consistent signature in `client.rs`, `add.rs`, `cat.rs`, `pin.rs`, `state.rs` ✅
- `Store` — consistent `Store::new(db)` + `store.db()` ✅
- `MasterKey::from_hex`, `generate_object_key`, `wrap`, `unwrap` — consistent in `key.rs`, `state.rs`, `ops/object.rs`, `ops/multipart.rs` ✅
- `ObjectKey { bytes: [u8; 32] }` — consistent in `key.rs`, `aes_gcm.rs`, `chunker.rs`, `ops/object.rs`, `ops/multipart.rs` ✅
- `EncryptionMode` enum — consistent in `mod.rs`, `ops/object.rs`, `ops/multipart.rs` ✅
- `ByteCounter` — defined in `ops/object.rs`, used in `ops/multipart.rs` via `use super::object::ByteCounter` ✅
- `AppError` variants — consistent in `error.rs` and all `From` impls ✅

One inconsistency found and resolved: `EncryptionMode::from_str` was used in Task 12 but only defined as a method on `EncryptionMode` in Task 12 Step 1's note. Ensure it is added to `src/crypto/mod.rs` during Task 12.

---

## Execution Handoff

After saving this plan, proceed to the subagent-driven-development skill to execute it.

**Execution:**
- Fresh subagent per task + two-stage review (spec compliance, then code quality)
- Continuous execution — no pause between tasks

