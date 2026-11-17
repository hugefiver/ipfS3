use sea_orm::{ConnectionTrait, DbErr, Statement};
use sea_orm_migration::prelude::*;

pub const VERSION: &str = "m20261109_000001_pin_cid_comparisons";

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        VERSION
    }
}

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        install(manager.get_connection()).await
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }

    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "CID comparisons require authorized forward repair".into(),
        ))
    }
}

pub(crate) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute_unprepared(
        "CREATE TABLE pin_cid_comparisons (cid TEXT NOT NULL PRIMARY KEY, canonical_cid TEXT); \
         CREATE INDEX idx_pin_cid_comparisons_canonical_cid_cid \
         ON pin_cid_comparisons(canonical_cid,cid)",
    )
    .await?;

    let mut after: Option<String> = None;
    loop {
        let (predicate, values) = match after.as_ref() {
            Some(cid) => ("WHERE cid>$1", vec![cid.clone().into()]),
            None => ("", Vec::new()),
        };
        let rows = db
            .query_all(Statement::from_sql_and_values(
                db.get_database_backend(),
                format!(
                    "SELECT cid FROM (SELECT cid FROM remote_pins UNION SELECT cid FROM remote_pin_ledger) AS historical {predicate} ORDER BY cid LIMIT 256"
                ),
                values,
            ))
            .await?;
        if rows.is_empty() {
            break;
        }
        let cids = rows
            .iter()
            .map(|row| row.try_get::<String>("", "cid"))
            .collect::<Result<Vec<_>, _>>()?;
        crate::store::pinning::cid_comparison::ensure_cid_comparisons(db, &cids)
            .await
            .map_err(|error| DbErr::Migration(error.to_string()))?;
        after = cids.last().cloned();
    }
    Ok(())
}
