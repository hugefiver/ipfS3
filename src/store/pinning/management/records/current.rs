//! Metered current registry rows; private cells are never projected or duplicated.
use super::*;
use sea_orm::{EntityTrait, FromQueryResult};
use std::sync::Arc;

pub(crate) async fn current_management_rows<C: ConnectionTrait>(
    db: &C,
    work: DecodeWork<'_>,
) -> Result<Vec<Arc<native::NativeRow>>> {
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == super::super::schema::RECORDS)
        .ok_or(ManagementError::Schema)?;
    let candidates = work.with(archive::sql_rows)?;
    work.with(|work| work.nodes(1 + candidates.len()))?;
    let mut result = Vec::with_capacity(candidates.len());
    let backend = db.get_database_backend();
    let equal = if backend == sea_orm::DatabaseBackend::Sqlite {
        "IS"
    } else {
        "IS NOT DISTINCT FROM"
    };
    for row in candidates {
        let bytes = row.values().flatten().try_fold(0usize, |total, value| {
            total
                .checked_add(value.len())
                .ok_or(ManagementError::Budget)
        })?;
        work.index_cells(row.len(), bytes)?;
        let predicate = row
            .keys()
            .enumerate()
            .map(|(index, column)| format!("CAST(\"{column}\" AS TEXT) {equal} ${}", index + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let matched = db
            .query_one(Statement::from_sql_and_values(
                backend,
                format!("SELECT COUNT(*) AS count FROM pin_management_records WHERE {predicate}"),
                row.values()
                    .map(|value| value.clone().into())
                    .collect::<Vec<sea_orm::Value>>(),
            ))
            .await
            .map_err(|_| ManagementError::Database)?
            .ok_or(ManagementError::Database)?
            .try_get::<i64>("", "count")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        if matched == 1 {
            result.push(row);
        } else if matched != 0 {
            return Err(ManagementError::InvalidEvidence);
        }
    }
    let mut values = Vec::new();
    let mut known = Vec::new();
    work.with(|work| work.nodes(2 + result.len() * 5))?;
    for row in &result {
        let offset = values.len();
        known.push(format!(
            "(${},${},${},${})",
            offset + 1,
            offset + 2,
            offset + 3,
            offset + 4
        ));
        for column in &table.primary_key {
            let value = native::value(row, column).ok_or(ManagementError::InvalidEvidence)?;
            work.copy(value)?;
            values.push(value.into());
        }
    }
    let predicate = if known.is_empty() {
        "TRUE".into()
    } else {
        format!(
            "NOT EXISTS (SELECT 1 FROM (VALUES {}) AS loaded WHERE loaded.column1=database_scope AND loaded.column2=plan_id AND loaded.column3=item_id AND loaded.column4=kind)",
            known.join(",")
        )
    };
    work.with(|work| work.nodes(1 + table.columns.len()))?;
    let columns = table
        .columns
        .iter()
        .map(|column| (column.as_str(), column.as_str()))
        .collect::<Vec<_>>();
    let (rows, bytes) = index::bounded_projection(
        db,
        &format!("FROM pin_management_records WHERE {predicate}"),
        &columns,
        "database_scope,plan_id,item_id,kind",
        values,
        (native::MAX_ROWS - result.len(), native::MAX_BYTES),
        work,
    )
    .await?;
    work.index_cells(rows.len() * table.columns.len(), bytes)?;
    work.with(|work| work.nodes(rows.len()))?;
    for row in rows {
        work.copy(&table.columns)?;
        let original = table
            .columns
            .iter()
            .map(|column| {
                Ok((
                    column.clone(),
                    row.try_get::<Option<String>>("", column)
                        .map_err(|_| ManagementError::InvalidEvidence)?,
                ))
            })
            .collect::<Result<native::NativeRow>>()?;
        let original = work.with(|work| archive::retain_row(original, work))?;
        work.with(|work| archive::remember_sql_row(original.clone(), work))?;
        result.push(original);
    }
    result.sort_by(|left, right| {
        table
            .primary_key
            .iter()
            .map(|column| native::value(left, column))
            .cmp(
                table
                    .primary_key
                    .iter()
                    .map(|column| native::value(right, column)),
            )
    });
    Ok(result)
}

pub(crate) async fn current_model<E, C>(
    db: &C,
    key: &[&str],
    work: DecodeWork<'_>,
) -> Result<Option<E::Model>>
where
    E: EntityTrait + Default,
    E::Model: FromQueryResult,
    C: ConnectionTrait,
{
    let entity = E::default();
    let name = entity.table_name();
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == name)
        .ok_or(ManagementError::Schema)?;
    if key.len() != table.primary_key.len() {
        return Err(ManagementError::Schema);
    }
    for _ in 0..2 {
        work.copy(key)?;
    }
    let predicate = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(index, column)| format!("\"{column}\"=${}", index + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let source = format!("FROM \"{name}\" WHERE {predicate}");
    let values = table
        .primary_key
        .iter()
        .zip(key)
        .map(|(column, value)| native::scope::bound_value(table, column, value))
        .collect::<Result<Vec<_>>>()?;
    if values.iter().zip(key).any(|(value, original)| match value {
        sea_orm::Value::BigInt(Some(value)) => value.to_string() != *original,
        sea_orm::Value::Int(Some(value)) => value.to_string() != *original,
        _ => false,
    }) {
        return Ok(None);
    }
    let mut rows = current_models::<E, _>(
        db,
        &source,
        &table
            .primary_key
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(","),
        values,
        1,
        work,
    )
    .await?;
    Ok(rows.pop())
}

pub(crate) async fn current_models<E, C>(
    db: &C,
    source: &str,
    order: &str,
    values: Vec<sea_orm::Value>,
    limit: usize,
    work: DecodeWork<'_>,
) -> Result<Vec<E::Model>>
where
    E: EntityTrait + Default,
    E::Model: FromQueryResult,
    C: ConnectionTrait,
{
    let entity = E::default();
    let name = entity.table_name();
    let table = native::current_catalog()
        .iter()
        .find(|table| table.table == name)
        .ok_or(ManagementError::Schema)?;
    let backend = db.get_database_backend();
    let terms = table
        .columns
        .iter()
        .map(|column| {
            if backend == sea_orm::DatabaseBackend::Sqlite {
                format!("LENGTH(CAST(COALESCE(CAST(\"{name}\".\"{column}\" AS TEXT),'') AS BLOB))")
            } else {
                format!("OCTET_LENGTH(COALESCE(CAST(\"{name}\".\"{column}\" AS TEXT),''))::bigint")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    let sum = format!("CAST(COALESCE(SUM({terms}),0) AS BIGINT)");
    let measured = db
        .query_one(Statement::from_sql_and_values(
            backend,
            format!("SELECT COUNT(*) AS count,{sum} AS bytes {source}"),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?;
    let count = usize::try_from(
        measured
            .try_get::<i64>("", "count")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    let bytes = usize::try_from(
        measured
            .try_get::<i64>("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    if count > limit || count > native::MAX_ROWS || bytes > native::MAX_BYTES {
        return Err(ManagementError::Budget);
    }
    for _ in 0..2 {
        work.index_cells(count * table.columns.len(), bytes)?;
    }
    work.with(|work| work.nodes(1 + count))?;
    let fields = table
        .columns
        .iter()
        .map(|column| format!("\"{name}\".\"{column}\" AS \"{column}\""))
        .collect::<Vec<_>>()
        .join(",");
    let rows=db.query_all(Statement::from_sql_and_values(backend,format!("SELECT {fields} {source} AND (SELECT COUNT(*) {source})<={count} AND (SELECT {sum} {source})<={bytes} ORDER BY {order} LIMIT {}",count+1),values)).await.map_err(|_|ManagementError::Database)?;
    if rows.len() != count {
        return Err(ManagementError::Changed);
    }
    rows.into_iter()
        .map(|row| {
            E::Model::from_query_result(&row, "").map_err(|_| ManagementError::InvalidEvidence)
        })
        .collect()
}

#[cfg(test)]
#[path = "current_tests.rs"]
mod model_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn current_model_preserves_private_and_null_cells_under_the_shared_budget() {
        use crate::store::entities::object;
        use native::scope::{ScopeSpec, Selector, collect_scope};

        let fixture =
            crate::store::pinning::management::actions::tests::fixture("current-originals").await;
        fixture
            .db
            .execute_unprepared("UPDATE objects SET metadata='{\"raw\":\"é\"}',key_wrap='' ")
            .await
            .unwrap();
        let expected_model = object::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let id = expected_model.id.as_str();
        let spec = ScopeSpec::new(vec![Selector::Exact(native::NativeKey {
            table: "objects".into(),
            values: vec![id.into()],
        })])
        .unwrap();
        let mut budget = native::meter::Budget::default();
        let frame = collect_scope(&fixture.db, &spec, &mut budget)
            .await
            .unwrap()
            .into_native_frame(&mut budget)
            .unwrap();
        let original = FrozenRows::Frame(&frame).row("objects", &[id]).unwrap();
        assert_eq!(original.cell("metadata"), Some("{\"raw\":\"é\"}"));
        assert_eq!(original.cell("key_wrap"), Some(""));
        assert_eq!(original.get("sse_c_key_fingerprint"), Some(&None));
        let work = SharedDecodeBudget::default();
        let before_model = work.lock().unwrap().usage();
        let model =
            current_model::<object::Entity, _>(&fixture.db, &[id], DecodeWork::shared(&work))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(model.metadata, Some(serde_json::json!({"raw": "é"})));
        assert_eq!(model.key_wrap.as_deref(), Some(""));
        assert_eq!(model.sse_c_key_fingerprint, None);
        assert_eq!(model, expected_model);
        let after_model = work.lock().unwrap().usage();
        assert!(after_model.0 > before_model.0 && after_model.1 > before_model.1);
        let exhausted = SharedDecodeBudget::default();
        exhausted
            .lock()
            .unwrap()
            .consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 1)
            .unwrap();
        assert!(matches!(
            current_model::<object::Entity, _>(&fixture.db, &[id], DecodeWork::shared(&exhausted))
                .await,
            Err(ManagementError::Budget)
        ));
        let unchanged = collect_scope(&fixture.db, &spec, &mut budget)
            .await
            .unwrap()
            .into_native_frame(&mut budget)
            .unwrap();
        assert!(frame == unchanged);
    }
}
