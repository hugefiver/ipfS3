//! Ownership fences read hot state; only extraction needs the frozen prefix.
use super::*;
use sea_orm::{FromQueryResult, sea_query::Iden};

#[derive(FromQueryResult)]
pub(super) struct JobState {
    pub bucket: String,
    pub key: String,
    pub state: String,
}

pub(super) async fn find_job_for_update<C: ConnectionTrait>(
    txn: &C,
    job_id: &str,
) -> AppResult<Option<JobState>> {
    let mut query = import_job::Entity::find_state().select_only();
    for column in [
        import_job::Column::Bucket,
        import_job::Column::Key,
        import_job::Column::State,
    ] {
        let name = Iden::to_string(&column);
        let bytes = if txn.get_database_backend() == DatabaseBackend::Sqlite {
            format!("length(CAST(\"{name}\" AS BLOB))")
        } else {
            format!("octet_length(\"{name}\")")
        };
        query = query.column_as(
            Expr::cust(format!("CASE WHEN {bytes}<=65536 THEN \"{name}\" END")),
            column,
        );
    }
    query = query.filter(import_job::Column::Id.eq(job_id));
    if txn.get_database_backend() == DatabaseBackend::Postgres {
        query = query.lock_exclusive();
    }
    Ok(query.into_model::<JobState>().one(txn).await?)
}

pub(super) async fn prefix<C: ConnectionTrait>(txn: &C, job_id: &str) -> AppResult<Option<String>> {
    let mut values = crate::store::import::requests::read_cells(
        txn,
        &[job_id.to_owned()],
        &[import_job::Column::DecompressPrefix],
    )
    .await?;
    values
        .remove(job_id)
        .and_then(|mut row| row.pop())
        .ok_or(AppError::StaleImportOwnership)
}

pub(super) async fn active<C: ConnectionTrait>(
    txn: &C,
    filter: Condition,
    bucket_name: &str,
) -> AppResult<()> {
    let mut query = import_job::Entity::find_state()
        .select_only()
        .column_as(Expr::value(1), "present")
        .filter(filter)
        .filter(import_job::Column::Bucket.eq(bucket_name));
    if txn.get_database_backend() == DatabaseBackend::Postgres {
        query = query.lock_exclusive();
    }
    if query.into_tuple::<i32>().one(txn).await?.is_none() {
        return Err(AppError::StaleImportOwnership);
    }
    Ok(())
}

pub(super) async fn active_ids<C: ConnectionTrait>(
    txn: &C,
    bucket: &str,
    after: Option<&str>,
) -> AppResult<Vec<String>> {
    let backend = txn.get_database_backend();
    let bytes = if backend == DatabaseBackend::Sqlite {
        "length(CAST(id AS BLOB))"
    } else {
        "octet_length(id)"
    };
    let lock = if backend == DatabaseBackend::Postgres {
        " FOR UPDATE OF j"
    } else {
        ""
    };
    let rows = txn.query_all(Statement::from_sql_and_values(backend, format!(
        "WITH candidates AS (SELECT id,{bytes} AS bytes FROM import_jobs WHERE bucket=$1 AND state IN ('queued','running') AND (CAST($2 AS TEXT) IS NULL OR id>$2) ORDER BY id LIMIT {OWNERSHIP_BATCH_SIZE}), sized AS (SELECT id,bytes,SUM(bytes+32) OVER(ORDER BY id) AS total,ROW_NUMBER() OVER(ORDER BY id) AS position FROM candidates) SELECT CASE WHEN p.bytes<=65536 THEN j.id END AS id FROM import_jobs j JOIN sized p ON p.id=j.id WHERE p.total<=1048576 OR p.position=1 ORDER BY j.id{lock}"),
        [bucket.into(), after.map(str::to_owned).into()])).await?;
    rows.into_iter()
        .map(|row| row.try_get("", "id").map_err(Into::into))
        .collect()
}
