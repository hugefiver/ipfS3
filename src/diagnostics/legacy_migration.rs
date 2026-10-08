//! Formal early local CLI. Parsing and JSON validation precede the sole DB connection.
use anyhow::{Result, anyhow};
use ipfs_s3_gateway::{
    config::Config,
    store::pinning::legacy_migration::{
        self as migration, Confirmation, Declaration, MAX_BYTES, Receipt,
    },
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    DryRun {
        declaration: String,
    },
    Apply {
        declaration: String,
        confirmation: String,
    },
    Rollback {
        receipt: String,
        confirmation: String,
    },
}

fn invalid() -> anyhow::Error {
    anyhow!("legacy migration input invalid (details withheld)")
}
fn unavailable() -> anyhow::Error {
    anyhow!("legacy migration database unavailable or migration refused (details withheld)")
}
fn store_error(error: ipfs_s3_gateway::error::AppError) -> anyhow::Error {
    match error {
        ipfs_s3_gateway::error::AppError::InvalidPinningRequest(reason)
            if reason.starts_with("legacy migration refused: ") =>
        {
            anyhow!(reason)
        }
        _ => unavailable(),
    }
}

pub fn parse(args: &[String]) -> Result<Option<Command>> {
    if !args
        .first()
        .is_some_and(|a| a == "--pinning-migrate-legacy" || a == "--rollback")
    {
        return Ok(None);
    }
    if args.iter().any(|s| s.is_empty() || s.len() > 4096) {
        return Err(invalid());
    }
    match args {
        [flag, decl] if flag == "--pinning-migrate-legacy" => Ok(Some(Command::DryRun {
            declaration: decl.clone(),
        })),
        [flag, decl, apply, confirmation]
            if flag == "--pinning-migrate-legacy" && apply == "--apply" =>
        {
            Ok(Some(Command::Apply {
                declaration: decl.clone(),
                confirmation: confirmation.clone(),
            }))
        }
        [flag, receipt, confirmation] if flag == "--rollback" => Ok(Some(Command::Rollback {
            receipt: receipt.clone(),
            confirmation: confirmation.clone(),
        })),
        _ => Err(invalid()),
    }
}

async fn load<T: DeserializeOwned>(path: &str) -> Result<T> {
    let file = tokio::fs::File::open(path).await.map_err(|_| invalid())?;
    if file.metadata().await.map_err(|_| invalid())?.len() > MAX_BYTES as u64 {
        return Err(invalid());
    }
    let mut bytes = vec![];
    file.take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| invalid())?;
    if bytes.len() > MAX_BYTES {
        return Err(invalid());
    }
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    depth(&value, 0)?;
    serde_json::from_value(value).map_err(|_| invalid())
}

fn depth(value: &Value, level: usize) -> Result<()> {
    if level > 32 {
        return Err(invalid());
    }
    match value {
        Value::Array(items) => {
            for v in items {
                depth(v, level + 1)?;
            }
        }
        Value::Object(items) => {
            for v in items.values() {
                depth(v, level + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn connect(url: &str, readonly: bool) -> Result<DatabaseConnection> {
    if url.starts_with("sqlite:")
        && (url.starts_with("sqlite::memory:") || url.contains("mode=memory"))
    {
        return Err(unavailable());
    }
    if !url.starts_with("sqlite:")
        && !url.starts_with("postgres://")
        && !url.starts_with("postgresql://")
    {
        return Err(unavailable());
    }
    let mut options = ConnectOptions::new(url.to_owned());
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    if url.starts_with("sqlite:") {
        options.map_sqlx_sqlite_opts(move |opts| {
            opts.read_only(readonly)
                .create_if_missing(false)
                .busy_timeout(std::time::Duration::from_secs(1))
        });
    }
    let db = Database::connect(options)
        .await
        .map_err(|_| unavailable())?;
    if db.get_database_backend() == DatabaseBackend::Sqlite && readonly {
        db.execute_unprepared("PRAGMA query_only=ON")
            .await
            .map_err(|_| unavailable())?;
    }
    if db.get_database_backend() == DatabaseBackend::Postgres {
        db.execute_unprepared("SET TIME ZONE 'UTC'")
            .await
            .map_err(|_| unavailable())?;
        db.execute_unprepared("SET statement_timeout='20s'")
            .await
            .map_err(|_| unavailable())?;
        if readonly {
            db.execute_unprepared("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
                .await
                .map_err(|_| unavailable())?;
        }
    }
    Ok(db)
}

pub async fn run(command: Command) -> Result<()> {
    let result = tokio::time::timeout(std::time::Duration::from_secs(30), execute(command))
        .await
        .map_err(|_| unavailable())??;
    println!(
        "{}",
        serde_json::to_string_pretty(&result).map_err(|_| invalid())?
    );
    Ok(())
}

async fn execute(command: Command) -> Result<Value> {
    // All local input is parsed before config loading/DB access. No AppState,
    // Store::new, Migrator, route registration, providers or worker construction.
    let (declaration, confirmation, receipt_id) = match &command {
        Command::DryRun { declaration } => (load::<Declaration>(declaration).await?, None, None),
        Command::Apply {
            declaration,
            confirmation,
        } => (
            load::<Declaration>(declaration).await?,
            Some(load::<Confirmation>(confirmation).await?),
            None,
        ),
        Command::Rollback {
            receipt,
            confirmation,
        } => {
            let confirmation = load::<Confirmation>(confirmation).await?;
            let id = if uuid::Uuid::parse_str(receipt).is_ok() {
                receipt.clone()
            } else {
                load::<Receipt>(receipt).await?.id
            };
            (
                confirmation.declaration.clone(),
                Some(confirmation),
                Some(id),
            )
        }
    };
    let readonly = matches!(command, Command::DryRun { .. });
    migration::validate_declaration(&declaration).map_err(|_| invalid())?;
    if readonly
        && declaration
            .confirmation_file
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(anyhow!(
            "dry-run requires declaration.confirmation_file for private native before/after evidence"
        ));
    }
    if !readonly
        && confirmation
            .as_ref()
            .is_none_or(|c| c.declaration != declaration)
    {
        return Err(invalid());
    }
    let config = Config::load().map_err(|_| invalid())?;
    let db = connect(&config.storage.database_url, readonly).await?;
    if readonly {
        let plan = migration::inventory(&db, &declaration)
            .await
            .map_err(store_error)?;
        let path=declaration.confirmation_file.as_deref().ok_or_else(||anyhow!("dry-run requires declaration.confirmation_file for private native before/after evidence"))?;
        let bytes = serde_json::to_vec_pretty(&plan).map_err(|_| invalid())?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid());
        }
        // Never clobber unknown files. Confirmation contains private DB evidence.
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .await
            .map_err(|_| invalid())?;
        file.write_all(&bytes).await.map_err(|_| invalid())?;
        file.sync_all().await.map_err(|_| invalid())?;
        return Ok(
            json!({"mode":"dry_run","evidence":"untrusted_proposal_not_receipt","schema":plan.schema,"db_clock":plan.db_clock,"expires_at":plan.expires_at,"confirmation_exported":true,"row_count":plan.before.iter().map(|i|i.rows.len()).sum::<usize>(),"pending_bindings":declaration.namespaces.iter().enumerate().map(|(i,n)|json!({"namespace_index":i,"row_bindings":n.bindings.iter().enumerate().map(|(j,b)|json!({"binding_index":j,"table":b.table,"original_row_identity":"private_confirmation_file"})).collect::<Vec<_>>()})).collect::<Vec<_>>() }),
        );
    }
    let confirmation = confirmation.ok_or_else(invalid)?;
    let receipt = if let Some(id) = receipt_id {
        migration::rollback(&db, &id, &confirmation).await
    } else {
        migration::apply(&db, &declaration, &confirmation).await
    }
    .map_err(store_error)?;
    serde_json::to_value(receipt).map_err(|_| invalid())
}
