//! Metadata candidate lookup followed by complete original binding verification.
//! Ordinary worker jobs do not decode unrelated multi-MiB protocol records.
use super::*;
use sea_orm::{DatabaseBackend, QueryResult, Value};

fn membership(backend: DatabaseBackend, column: &str, parameter: usize) -> String {
    if backend == DatabaseBackend::Postgres {
        format!("{column}::jsonb ? ${parameter}")
    } else {
        format!("EXISTS (SELECT 1 FROM json_each({column}) WHERE value=${parameter})")
    }
}

#[cfg(test)]
pub(crate) async fn actions_for_job<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
) -> Result<Vec<TrustedAppliedAction>> {
    actions_for_job_with_work(db, job, SharedDecodeBudget::default()).await
}
pub(crate) async fn actions_for_job_with_work<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    let context = DecodeWork::shared(&work);
    let backend = db.get_database_backend();
    context.index_copy(&(&job.id, &job.id))?;
    let mut predicate = membership(backend, "job_ids", 1);
    let mut values = vec![job.id.clone().into()];
    let mut candidate_rows = 0;
    let mut candidate_bytes = 0;
    if job.operation == "poll"
        && let Some(lease) = &job.lease_id
    {
        context.index_copy(&(lease, lease))?;
        predicate = format!("({predicate} OR {})", membership(backend, "intent_ids", 2));
        values.push(lease.clone().into());
        let originals = poll_submit_ids(db, job, context).await?;
        if !originals.is_empty() {
            candidate_rows = originals.len();
            context.index_copy(&originals)?;
            context.index_copy(&originals)?;
            let encoded =
                serde_json::to_string(&originals).map_err(|_| ManagementError::InvalidEvidence)?;
            candidate_bytes = encoded.len();
            if candidate_bytes > native::MAX_BYTES {
                return Err(ManagementError::Budget);
            }
            let original_membership = if backend == DatabaseBackend::Postgres {
                "job_ids::jsonb ?| ARRAY(SELECT jsonb_array_elements_text($3::jsonb))"
            } else {
                "EXISTS (SELECT 1 FROM json_each(job_ids) AS recorded JOIN json_each($3) AS original ON recorded.value=original.value)"
            };
            predicate = format!("({predicate} OR {original_membership})");
            values.push(encoded.into());
        }
    }
    select(
        db,
        &predicate,
        values,
        candidate_rows,
        candidate_bytes,
        work,
    )
    .await
}

/// Native target/generation continuity locates original Submit IDs only. The
/// selected receipts still supply all authority; this query grants none.
#[cfg(test)]
pub(crate) async fn poll_discovery_with_work<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    work: SharedDecodeBudget,
) -> Result<Vec<String>> {
    poll_submit_ids(db, job, DecodeWork::shared(&work)).await
}
async fn poll_submit_ids<C: ConnectionTrait>(
    db: &C,
    job: &crate::store::entities::pin_job::Model,
    work: DecodeWork<'_>,
) -> Result<Vec<String>> {
    use crate::store::entities::remote_pin;
    let (Some(lease), Some(target), Some(generation)) =
        (&job.lease_id, &job.target_id, job.expected_generation)
    else {
        return Ok(vec![]);
    };
    if job.expected_remote_epoch.is_some() {
        return Ok(vec![]);
    }
    let Some(remote) =
        super::current_model::<remote_pin::Entity, _>(db, &[&job.provider, &job.cid], work).await?
    else {
        return Ok(vec![]);
    };
    if remote.request_id.is_none() {
        return Ok(vec![]);
    }
    // Correlation selects candidates only. Every selected successor rechecks
    // the original request, route, source and generation before deriving a grant.
    // Both rows are persisted native work. Operational provider equality is a
    // locator, not an alias: original full routes/rekey points are checked later.
    for _ in 0..2 {
        work.index_copy(&(&job.id, lease, target, generation, &job.provider, &job.cid))?;
    }
    let (rows,raw_bytes)=bounded_projection(db,"FROM pin_jobs AS original JOIN pin_jobs AS poll ON poll.id=$1 WHERE poll.operation='poll' AND poll.lease_id=$2 AND poll.target_id=$3 AND poll.expected_generation=$4 AND poll.provider=$5 AND poll.cid=$6 AND poll.expected_remote_epoch IS NULL AND original.operation='submit' AND original.lease_id=poll.lease_id AND original.target_id=poll.target_id AND original.expected_generation=poll.expected_generation AND original.provider=poll.provider AND original.cid=poll.cid AND original.expected_remote_epoch IS NULL",&[("original.id","id")],"original.id",vec![job.id.clone().into(),lease.clone().into(),target.clone().into(),generation.into(),job.provider.clone().into(),job.cid.clone().into()],(native::MAX_ROWS,native::MAX_BYTES),work).await?;
    work.index_cells(rows.len(), raw_bytes)?;
    let mut bytes = 0usize;
    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row
            .try_get::<String>("", "id")
            .map_err(|_| ManagementError::InvalidEvidence)?;
        bytes = bytes.checked_add(id.len()).ok_or(ManagementError::Budget)?;
        if bytes > native::MAX_BYTES {
            return Err(ManagementError::Budget);
        }
        ids.push(id);
    }
    Ok(ids)
}
pub(crate) async fn actions_for_lease_with_work<C: ConnectionTrait>(
    db: &C,
    lease: &str,
    work: SharedDecodeBudget,
) -> Result<Vec<TrustedAppliedAction>> {
    DecodeWork::shared(&work).index_copy(&(lease, lease))?;
    select(
        db,
        &membership(db.get_database_backend(), "intent_ids", 1),
        vec![lease.into()],
        0,
        0,
        work,
    )
    .await
}
pub(crate) async fn lease_has_selectors<C: ConnectionTrait>(
    db: &C,
    lease: &str,
    work: DecodeWork<'_>,
) -> Result<bool> {
    for _ in 0..2 {
        work.copy(lease)?;
    }
    work.with(|work| {
        work.nodes(2)?;
        work.bytes(1)
    })?;
    let backend = db.get_database_backend();
    let predicate = membership(backend, "intent_ids", 1);
    db.query_one(Statement::from_sql_and_values(backend,format!("SELECT EXISTS(SELECT 1 FROM pin_management_records WHERE kind='action' AND state='applied' AND completed_at IS NOT NULL AND ({predicate})) AS present"),[lease.into()])).await.map_err(|_|ManagementError::Database)?.ok_or(ManagementError::Database)?.try_get("","present").map_err(|_|ManagementError::InvalidEvidence)
}
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
#[cfg(test)]
pub(crate) async fn action_at_key<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
) -> Result<Option<TrustedAppliedAction>> {
    action_at_key_with_work(db, key, SharedDecodeBudget::default()).await
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
