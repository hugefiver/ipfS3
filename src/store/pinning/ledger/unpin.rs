//! The worker's owned transaction between its business fences and effect write.
use super::*;
use crate::store::pinning::{
    legacy_migration,
    management::{self, records::DecodeWork},
};

fn effect(cleanup: bool) -> &'static str {
    if cleanup {
        "cleanup_pending"
    } else {
        "retained"
    }
}

pub(crate) async fn prepare_runtime_unpin_effect(
    tx: crate::store::RuntimeTransaction,
    provider: &str,
    cid: &str,
    work: DecodeWork<'_>,
) -> AppResult<(crate::store::RuntimeTransaction, bool)> {
    if tx.db().get_database_backend() != DatabaseBackend::Sqlite {
        let cleanup = cleanup_allowed(tx.db(), provider, cid).await?;
        mark_effect(tx.db(), provider, cid, effect(cleanup)).await?;
        return Ok((tx, cleanup));
    }
    let catalog = tx.catalog(work);
    let cleanup = cleanup_allowed_catalog(catalog, provider, cid, work).await?;
    // Effect publication reads its own fresh binding and ledger row.
    let candidates = catalog
        .load(
            &legacy_migration::resource_receipt_selectors(provider, cid),
            work,
        )
        .await
        .map_err(management::admission::work_error)?;
    let row = get_from_catalog(provider, cid, &candidates, work, catalog).await?;
    write_effect(tx.db(), row, effect(cleanup)).await?;
    Ok((tx, cleanup))
}

#[cfg(test)]
mod tests;
