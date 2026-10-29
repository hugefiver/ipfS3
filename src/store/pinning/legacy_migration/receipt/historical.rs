//! R26 read-only frontier on an independently validated historical prefix.
use super::super::*;
use crate::store::pinning::legacy_migration::MAX_ROWS;

pub(super) async fn active_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    versions: &[String],
    work: &W,
) -> Result<Vec<operation::Model>, W::Error> {
    if !crate::store::migrations::history::has_stage(versions, SCHEMA) {
        return Ok(vec![]);
    }
    let table = crate::store::pinning::management::native::current_catalog()
        .iter()
        .find(|table| table.table == "pin_legacy_migrations")
        .ok_or_else(|| W::application(refused("receipt catalog unavailable")))?;
    work.cells(1, 0)?;
    let columns = table
        .columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(",");
    if db.query_one(sea_orm::Statement::from_string(db.get_database_backend(),format!("SELECT 1 AS present FROM (SELECT {columns} FROM pin_legacy_migrations LIMIT 1) AS header"))).await.map_err(|error|W::application(error.into()))?.is_none() {
        return Ok(vec![]);
    }
    budget(db, 0).await.map_err(W::application)?;
    let terms = table
        .columns
        .iter()
        .map(|column| {
            if db.get_database_backend() == sea_orm::DatabaseBackend::Sqlite {
                format!("length(CAST(COALESCE(CAST(\"{column}\" AS TEXT),'') AS BLOB))")
            } else {
                format!("octet_length(COALESCE(CAST(\"{column}\" AS TEXT),''))::bigint")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    let sum = format!("CAST(COALESCE(SUM({terms}),0) AS BIGINT)");
    let measured = db
        .query_one(sea_orm::Statement::from_string(
            db.get_database_backend(),
            format!("SELECT COUNT(*) AS count,{sum} AS bytes FROM pin_legacy_migrations"),
        ))
        .await
        .map_err(|error| W::application(error.into()))?
        .ok_or_else(|| W::application(refused("receipt budget unavailable")))?;
    let count = usize::try_from(
        measured
            .try_get::<i64>("", "count")
            .map_err(|error| W::application(error.into()))?,
    )
    .map_err(|_| W::application(refused("receipt row count invalid")))?;
    let bytes = usize::try_from(
        measured
            .try_get::<i64>("", "bytes")
            .map_err(|error| W::application(error.into()))?,
    )
    .map_err(|_| W::application(refused("receipt byte count invalid")))?;
    if count > MAX_ROWS {
        return Err(W::application(refused("receipt resolver budget exceeded")));
    }
    for _ in 0..2 {
        work.cells(1 + count * table.columns.len(), bytes)?;
    }
    let rows = operation::Entity::find().from_raw_sql(sea_orm::Statement::from_string(db.get_database_backend(),format!("SELECT {columns} FROM pin_legacy_migrations WHERE (SELECT COUNT(*) FROM pin_legacy_migrations)<={count} AND (SELECT {sum} FROM pin_legacy_migrations)<={bytes} ORDER BY id LIMIT {}",MAX_ROWS+1))).all(db).await.map_err(|error|W::application(error.into()))?;
    if rows.len() != count {
        return Err(W::application(refused("receipt frontier changed")));
    }
    if rows.len() > MAX_ROWS {
        return Err(W::application(refused("receipt resolver budget exceeded")));
    }
    let mut reversed = BTreeSet::new();
    for id in rows.iter().filter_map(|row| row.reverses.as_deref()) {
        work.copy(id)?;
        reversed.insert(id);
    }
    let mut active = Vec::new();
    for row in rows
        .iter()
        .filter(|row| row.direction == "apply" && !reversed.contains(row.id.as_str()))
    {
        work.copy(&(
            &row.id,
            &row.direction,
            &row.reverses,
            &row.database_scope,
            &row.db_clock,
            &row.schema_version,
            &row.declaration,
            &row.confirmation,
            row.item_count,
        ))?;
        active.push(row.clone());
    }
    for row in &active {
        complete_with_work(db, row, work).await?;
    }
    Ok(active)
}
