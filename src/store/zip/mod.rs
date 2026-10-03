//! Durable ZIP ownership. No archive/object FK: a batch outlives both its source
//! and any later overwrite/deletion of the published output versions.
mod batch;
pub mod execution;
pub mod import_intake;
mod manifest;
mod publication;
pub mod recovery;
mod root;

pub use batch::{BatchAdmission, admit, read, snapshot};
pub use manifest::{ManifestItem, prepare_manifest};
pub use publication::{
    RootOutcome, VersionBinding, binding_for_published_object, publish, settle_root_retry,
};
pub use root::{
    RootClaim, claim_root, mark_invoked, mark_reconciling, mark_unknown, recovery, renew_claim,
    retain_candidate, root_existence, verify_root,
};

use super::entities::zip_batch;
use crate::error::{AppError, AppResult};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, QuerySelect};

pub struct BatchSnapshot {
    pub batch: zip_batch::Model,
    pub entries: Vec<super::entities::zip_manifest_entry::Model>,
    pub builds: Vec<super::entities::zip_root_build::Model>,
    pub references: Vec<super::entities::zip_root_reference::Model>,
}

fn stale() -> AppError {
    AppError::Internal("stale ZIP batch ownership or root claim".into())
}

fn invalid() -> AppError {
    AppError::InvalidZipParameter("invalid ZIP batch data".into())
}

fn safe_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
}

fn required(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096
}

fn supported(db: &impl ConnectionTrait) -> AppResult<()> {
    if matches!(
        db.get_database_backend(),
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite
    ) {
        Ok(())
    } else {
        Err(AppError::Internal(
            "ZIP batches require SQLite or PostgreSQL".into(),
        ))
    }
}

/// First operation in a batch mutation transaction. PostgreSQL serializes on
/// the batch row; SQLite acquires write intent before a snapshot read.
async fn lock_batch(tx: &DatabaseTransaction, id: &str) -> AppResult<zip_batch::Model> {
    use sea_orm::{ColumnTrait, QueryFilter, sea_query::Expr};
    supported(tx)?;
    if tx.get_database_backend() == DatabaseBackend::Sqlite {
        let changed = zip_batch::Entity::update_many()
            .col_expr(
                zip_batch::Column::UpdatedAt,
                Expr::col(zip_batch::Column::UpdatedAt).into(),
            )
            .filter(zip_batch::Column::Id.eq(id))
            .exec(tx)
            .await?;
        if changed.rows_affected != 1 {
            return Err(stale());
        }
    }
    let query = zip_batch::Entity::find_by_id(id);
    let row = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(tx).await?
    } else {
        query.one(tx).await?
    };
    row.ok_or_else(stale)
}
