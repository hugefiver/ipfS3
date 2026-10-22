//! Detect out-of-batch writes to retained canonical caches. This is not authority.
use super::{
    spool::{exec, get, one},
    *,
};
use sea_orm::ConnectionTrait;

pub(super) async fn install(db: &impl ConnectionTrait, tables: &[&str]) -> Result<()> {
    exec(db,"CREATE TABLE IF NOT EXISTS canonical_cache_guard(id INTEGER PRIMARY KEY CHECK(id=1),dirty INTEGER NOT NULL)",vec![]).await?;
    exec(
        db,
        "INSERT INTO canonical_cache_guard VALUES(1,0) ON CONFLICT(id) DO NOTHING",
        vec![],
    )
    .await?;
    check(db).await?;
    for table in tables {
        for event in ["INSERT", "UPDATE", "DELETE"] {
            exec(db,&format!("CREATE TRIGGER IF NOT EXISTS canonical_guard_{table}_{event} AFTER {event} ON {table} BEGIN UPDATE canonical_cache_guard SET dirty=1 WHERE id=1; END"),vec![]).await?;
        }
    }
    Ok(())
}

pub(super) async fn check(db: &impl ConnectionTrait) -> Result<()> {
    if one(db,"SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='canonical_cache_guard'",vec![]).await?.is_none() {
        return Ok(());
    }
    let row = one(
        db,
        "SELECT dirty FROM canonical_cache_guard WHERE id=1",
        vec![],
    )
    .await?
    .ok_or(ManagementError::InvalidEvidence)?;
    if get::<i64>(&row, "dirty")? != 0 {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

/// Called inside the transaction which produced the canonical batch and cursor.
pub(super) async fn accept(db: &impl ConnectionTrait) -> Result<()> {
    if one(db,"SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name='canonical_cache_guard'",vec![]).await?.is_some() {
        exec(db,"UPDATE canonical_cache_guard SET dirty=0 WHERE id=1",vec![]).await?;
    }
    Ok(())
}
