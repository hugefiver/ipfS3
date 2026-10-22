//! Metered current registry rows; private cells are never projected or duplicated.
use super::*;
use sea_orm::{
    ColumnTrait, EntityTrait, FromQueryResult, Iterable,
    sea_query::{ColumnType, Iden},
};
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
    let backend = db.get_database_backend();
    let predicate = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let entity_column = E::Column::iter()
                .find(|candidate| Iden::to_string(candidate) == *column)
                .ok_or(ManagementError::Schema)?;
            let expression = if backend == sea_orm::DatabaseBackend::Sqlite
                && matches!(
                    entity_column.def().get_column_type(),
                    ColumnType::String(_) | ColumnType::Text
                ) {
                format!("\"{column}\"")
            } else {
                format!("CAST(\"{column}\" AS TEXT)")
            };
            Ok(format!("{expression}=${}", index + 1))
        })
        .collect::<Result<Vec<_>>>()?
        .join(" AND ");
    let source = format!("FROM \"{name}\" WHERE {predicate}");
    let values = key
        .iter()
        .copied()
        .map(Into::into)
        .collect::<Vec<sea_orm::Value>>();
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

pub(crate) struct CurrentRows {
    scope: String,
    identity: String,
    tables: Vec<Vec<Arc<RowHandle>>>,
}
impl CurrentRows {
    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }
    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }
    pub(crate) fn rows(&self) -> impl Iterator<Item = (&native::NativeTable, RowView<'_>)> {
        native::current_catalog()
            .iter()
            .zip(&self.tables)
            .flat_map(|(table, rows)| rows.iter().map(move |row| (table, RowView::Handle(row))))
    }
    pub(crate) async fn capture<C: ConnectionTrait>(db: &C, work: DecodeWork<'_>) -> Result<Self> {
        super::super::schema::validate(db).await?;
        let catalog = native::current_catalog();
        super::super::catalog::validate(db, catalog).await?;
        native::check_raw_inventory(db).await?;
        work.with(|work| work.nodes(1 + catalog.len()))?;
        let mut tables = Vec::with_capacity(catalog.len());
        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        for table in catalog {
            if table.table == super::super::schema::RECORDS {
                let rows = current_management_rows(db, work).await?;
                total_rows += rows.len();
                for row in &rows {
                    for value in row.values().flatten() {
                        total_bytes = total_bytes
                            .checked_add(value.len())
                            .ok_or(ManagementError::Budget)?;
                    }
                }
                if total_rows > native::MAX_ROWS || total_bytes > native::MAX_BYTES {
                    return Err(ManagementError::Budget);
                }
                work.with(|work| work.nodes(1 + rows.len() * 2))?;
                tables.push(
                    rows.into_iter()
                        .map(|row| Arc::new(RowHandle::Sql(row)))
                        .collect(),
                );
                continue;
            }
            work.with(|work| work.nodes(1 + table.columns.len()))?;
            let columns = table
                .columns
                .iter()
                .map(|column| (column.as_str(), column.as_str()))
                .collect::<Vec<_>>();
            let order = table
                .primary_key
                .iter()
                .map(|column| format!("\"{column}\""))
                .collect::<Vec<_>>()
                .join(",");
            let (rows, bytes) = index::bounded_projection(
                db,
                &format!("FROM \"{}\" WHERE TRUE", table.table),
                &columns,
                &order,
                vec![],
                (
                    native::MAX_ROWS - total_rows,
                    native::MAX_BYTES - total_bytes,
                ),
                work,
            )
            .await?;
            total_rows += rows.len();
            total_bytes += bytes;
            work.index_cells(
                rows.len()
                    .checked_mul(table.columns.len())
                    .ok_or(ManagementError::Budget)?,
                bytes,
            )?;
            work.with(|work| work.nodes(1 + rows.len() * 2))?;
            for _ in &rows {
                work.copy(&table.columns)?;
            }
            let mut members = Vec::with_capacity(rows.len());
            for row in rows {
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
                members.push(Arc::new(RowHandle::Sql(Arc::new(original))));
            }
            tables.push(members);
        }
        let mut result = Self {
            scope: String::new(),
            identity: String::new(),
            tables,
        };
        let mut scopes = result.rows().filter(|(table, row)| {
            table.table == super::super::schema::RECORDS
                && row.cell("kind") == Some("database_scope")
        });
        let scope = scopes
            .next()
            .and_then(|(_, row)| row.cell("database_scope"))
            .ok_or(ManagementError::Schema)?;
        if scopes.next().is_some() {
            return Err(ManagementError::Schema);
        }
        work.copy(scope)?;
        let scope = scope.to_owned();
        drop(scopes);
        result.scope = scope;
        result.identity = native::database_identity(db).await?;
        work.copy(&result.identity)?;
        native::check_raw_inventory(db).await?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "current_tests.rs"]
mod model_tests;

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn current_registry_preserves_private_and_null_cells_under_the_shared_budget() {
        let fixture =
            crate::store::pinning::management::actions::tests::fixture("current-originals").await;
        fixture
            .db
            .execute_unprepared("UPDATE objects SET metadata='{\"raw\":\"é\"}',key_wrap='' ")
            .await
            .unwrap();
        let expected = native::capture(&fixture.db).await.unwrap();
        let work = SharedDecodeBudget::default();
        let current = CurrentRows::capture(&fixture.db, DecodeWork::shared(&work))
            .await
            .unwrap();
        for (table, row) in current.rows() {
            let key = table
                .primary_key
                .iter()
                .map(|column| row.cell(column).unwrap())
                .collect::<Vec<_>>();
            for (column, value) in row.iter() {
                assert_eq!(
                    value.as_deref(),
                    expected.original_value(&table.table, &key, column)
                );
            }
        }
        let id = native::value(&expected.table("objects").unwrap().rows[0], "id").unwrap();
        let before_model = work.lock().unwrap().usage();
        let model = current_model::<crate::store::entities::object::Entity, _>(
            &fixture.db,
            &[id],
            DecodeWork::shared(&work),
        )
        .await
        .unwrap();
        let expected_model = crate::store::entities::object::Entity::find_by_id(id.to_owned())
            .one(&fixture.db)
            .await
            .unwrap();
        assert_eq!(model, expected_model);
        let after_model = work.lock().unwrap().usage();
        assert!(after_model.0 > before_model.0 && after_model.1 > before_model.1);
        let usage = work.lock().unwrap().usage();
        assert!(usage.0 > 0 && usage.1 > 0);
        let exhausted = SharedDecodeBudget::default();
        exhausted
            .lock()
            .unwrap()
            .consume(0, crate::pinning::management::wire::MAX_WIRE_BYTES - 1)
            .unwrap();
        assert!(matches!(
            CurrentRows::capture(&fixture.db, DecodeWork::shared(&exhausted)).await,
            Err(ManagementError::Budget)
        ));
        assert!(matches!(
            current_model::<crate::store::entities::object::Entity, _>(
                &fixture.db,
                &[id],
                DecodeWork::shared(&exhausted)
            )
            .await,
            Err(ManagementError::Budget)
        ));
        assert!(expected == native::capture(&fixture.db).await.unwrap());
    }
}
