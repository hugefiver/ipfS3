//! Metadata candidate lookup followed by complete original binding verification.
//! Ordinary worker jobs do not decode unrelated multi-MiB protocol records.
use super::*;
use sea_orm::{DatabaseBackend, QueryResult, Value};

pub(crate) async fn actions_for_plan_with_work<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    plan: &str,
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    auth.check(db).await?;
    DecodeWork::shared(&work).copy(&(auth.database_scope(), plan))?;
    let actions = select(
        db,
        "database_scope=$1 AND plan_id=$2",
        vec![auth.database_scope().into(), plan.into()],
        0,
        0,
        work,
    )
    .await?;
    if actions.iter().any(|trusted| {
        trusted.binding().database_identity != auth.database_identity()
            || trusted.binding().principal != auth.principal()
    }) {
        return Err(ManagementError::Unauthorized);
    }
    Ok(actions)
}
pub(crate) async fn action_at_key_with_work<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
    work: SharedDecodeBudget,
) -> Result<Option<TrustedAppliedAction>> {
    if key.table != super::super::schema::RECORDS
        || key.values.len() != 4
        || key.values[3] != "action"
    {
        return Err(ManagementError::InvalidEvidence);
    }
    DecodeWork::shared(&work).index_copy(&(&key.values[..3], &key.values[..3]))?;
    let mut actions = select(
        db,
        "database_scope=$1 AND plan_id=$2 AND item_id=$3",
        key.values[..3].iter().cloned().map(Into::into).collect(),
        0,
        0,
        work,
    )
    .await?;
    if actions.len() > 1 {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(actions.pop())
}
pub(crate) async fn actions_at_keys_with_work<C: ConnectionTrait>(
    db: &C,
    keys: &[native::NativeKey],
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    if keys.is_empty() {
        return Ok(vec![]);
    }
    let scope = keys[0]
        .values
        .first()
        .ok_or(ManagementError::Confirmation)?;
    if keys.iter().any(|key| {
        key.table != super::super::schema::RECORDS
            || key.values.len() != 4
            || key.values[0] != *scope
            || key.values[3] != "action"
    }) {
        return Err(ManagementError::Confirmation);
    }
    let context = DecodeWork::shared(&work);
    context.copy(scope)?;
    context.with(|work| work.nodes(1 + keys.len() * 2))?;
    let mut values = vec![scope.clone().into()];
    let mut tuples = Vec::with_capacity(keys.len());
    for key in keys {
        context.copy(&(&key.values[1], &key.values[2]))?;
        let first = values.len() + 1;
        tuples.push(format!("(${first},${})", first + 1));
        values.push(key.values[1].clone().into());
        values.push(key.values[2].clone().into());
    }
    select(
        db,
        &format!(
            "database_scope=$1 AND (plan_id,item_id) IN ({})",
            tuples.join(",")
        ),
        values,
        0,
        0,
        work,
    )
    .await
}
async fn select<C: ConnectionTrait>(
    db: &C,
    predicate: &str,
    values: Vec<Value>,
    candidate_rows: usize,
    candidate_bytes: usize,
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    let context = DecodeWork::shared(&work);
    let remaining = native::MAX_ROWS
        .checked_sub(candidate_rows)
        .ok_or(ManagementError::Budget)?;
    let remaining_bytes = native::MAX_BYTES
        .checked_sub(candidate_bytes)
        .ok_or(ManagementError::Budget)?;
    let (rows,locator_bytes)=bounded_projection(db,&format!("FROM pin_management_records WHERE kind='action' AND state='applied' AND completed_at IS NOT NULL AND ({predicate})"),&[("database_scope","database_scope"),("plan_id","plan_id"),("item_id","item_id"),("kind","kind")],"plan_id,item_id",values,(remaining,remaining_bytes),context).await?;
    if rows.is_empty() {
        return Ok(vec![]);
    }
    let identity = native::database_identity(db).await?;
    if identity.starts_with("diagnostic:") {
        return Err(ManagementError::Unauthorized);
    }
    let (scopes, scope_bytes) = bounded_projection(
        db,
        "FROM pin_management_records WHERE kind='database_scope'",
        &[("database_scope", "database_scope")],
        "database_scope",
        vec![],
        (remaining - rows.len(), remaining_bytes - locator_bytes),
        context,
    )
    .await?;
    context.index_cells(scopes.len(), scope_bytes)?;
    let scope = scopes
        .first()
        .ok_or(ManagementError::Schema)?
        .try_get::<String>("", "database_scope")
        .map_err(|_| ManagementError::Schema)?;
    context.index_cells(
        rows.len().checked_mul(4).ok_or(ManagementError::Budget)?,
        locator_bytes,
    )?;
    for _ in &rows {
        context.index_copy(&(super::super::schema::RECORDS, [(); 4]))?;
    }
    let keys = rows
        .iter()
        .map(|row| {
            Ok(native::NativeKey {
                table: super::super::schema::RECORDS.into(),
                values: ["database_scope", "plan_id", "item_id", "kind"]
                    .into_iter()
                    .map(|column| {
                        row.try_get::<String>("", column)
                            .map_err(|_| ManagementError::InvalidEvidence)
                    })
                    .collect::<Result<_>>()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let source = std::sync::Arc::new(
        evidence::Evidence::load_indexed(
            db,
            &scope,
            &identity,
            &keys,
            candidate_rows + scopes.len(),
            candidate_bytes + scope_bytes,
            work.clone(),
        )
        .await?,
    );
    context.index_copy(&(&scope, &identity))?;
    context.with(|work| work.nodes(1 + keys.len()))?;
    let batch_work = work.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut validation = ValidationContext::with_work(batch_work);
        let mut output = Vec::with_capacity(keys.len());
        for key in keys {
            let action = results::trusted_from_record_with_context(
                &source,
                &scope,
                &identity,
                source.row(&key)?,
                &mut validation,
            )?
            .ok_or(ManagementError::Confirmation)?;
            output.push(action);
        }
        Ok(output)
    })
    .await
    .map_err(|_| ManagementError::InvalidEvidence)?;
    // Exhaustion remains a work failure even when a structural authority check
    // represents a failed optional decode as a non-match.
    context.with(|work| work.check()).and(result)
}

pub(crate) async fn bounded_projection<C: ConnectionTrait>(
    db: &C,
    source: &str,
    columns: &[(&str, &str)],
    order: &str,
    values: Vec<Value>,
    (remaining_rows, remaining_bytes): (usize, usize),
    work: DecodeWork<'_>,
) -> Result<(Vec<QueryResult>, usize)> {
    let backend = db.get_database_backend();
    let terms = columns
        .iter()
        .map(|(column, _)| {
            if backend == DatabaseBackend::Sqlite {
                format!("LENGTH(CAST(COALESCE(CAST({column} AS TEXT),'') AS BLOB))")
            } else {
                format!("OCTET_LENGTH(COALESCE(CAST({column} AS TEXT),''))::bigint")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    let sum = format!("CAST(COALESCE(SUM({terms}),0) AS BIGINT)");
    let measured = db
        .query_one(Statement::from_sql_and_values(
            backend,
            format!("SELECT COUNT(*) AS rows,{sum} AS bytes {source}"),
            values.clone(),
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?;
    let count = usize::try_from(
        measured
            .try_get::<i64>("", "rows")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    let bytes = usize::try_from(
        measured
            .try_get::<i64>("", "bytes")
            .map_err(|_| ManagementError::InvalidEvidence)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    if count > remaining_rows || bytes > remaining_bytes {
        return Err(ManagementError::Budget);
    }
    work.index_cells(
        count
            .checked_mul(columns.len())
            .ok_or(ManagementError::Budget)?,
        bytes,
    )?;
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, (column, alias))| {
            let length = if backend == DatabaseBackend::Sqlite {
                format!("LENGTH(CAST(COALESCE(CAST({column} AS TEXT),'') AS BLOB))")
            } else {
                format!("OCTET_LENGTH(COALESCE(CAST({column} AS TEXT),''))::bigint")
            };
            format!("CAST({column} AS TEXT) AS \"{alias}\",{length} AS \"__work_bytes_{index}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    let rows=db.query_all(Statement::from_sql_and_values(backend,format!("SELECT {fields} {source} AND (SELECT {sum} {source})<={bytes} AND (SELECT COUNT(*) {source})<={count} ORDER BY {order} LIMIT {}",count+1),values)).await.map_err(|_|ManagementError::Database)?;
    if rows.len() != count {
        return Err(ManagementError::Changed);
    }
    let actual = rows.iter().try_fold(0usize, |sum, row| {
        // Lengths come from the same fetched row, so concurrent changes cannot
        // underpay the String copies while preserving the reader's row/byte caps.
        let row_bytes = (0..columns.len()).try_fold(0usize, |sum, index| {
            let bytes = usize::try_from(
                row.try_get::<i64>("", &format!("__work_bytes_{index}"))
                    .map_err(|_| ManagementError::InvalidEvidence)?,
            )
            .map_err(|_| ManagementError::Budget)?;
            sum.checked_add(bytes).ok_or(ManagementError::Budget)
        })?;
        work.index_cells(columns.len(), row_bytes)?;
        columns.iter().try_fold(sum, |sum, (_, alias)| {
            sum.checked_add(
                row.try_get::<Option<String>>("", alias)
                    .map_err(|_| ManagementError::InvalidEvidence)?
                    .as_ref()
                    .map_or(0, String::len),
            )
            .ok_or(ManagementError::Budget)
        })
    })?;
    if actual > remaining_bytes {
        return Err(ManagementError::Budget);
    }
    Ok((rows, actual))
}
