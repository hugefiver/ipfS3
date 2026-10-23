//! Bounded transport for unusually large frozen import requests.
use crate::store::entities::import_job;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DbErr, Iterable, QueryFilter, QuerySelect,
    Statement,
    sea_query::{Expr, Iden},
};
use std::collections::BTreeMap;

fn octets(backend: DatabaseBackend, name: &str) -> String {
    if backend == DatabaseBackend::Sqlite {
        format!("CAST(r.\"{name}\" AS BLOB)")
    } else {
        format!("convert_to(r.\"{name}\",'UTF8')")
    }
}

pub(super) async fn load_large<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
) -> Result<Vec<import_job::Model>, DbErr> {
    let mut jobs = BTreeMap::new();
    // Hot rows have already passed the 64KiB projection budget. Fifteen rows
    // leave room for IDs and SQL framing inside each 1MiB page.
    for page in ids.chunks(15) {
        let mut query = import_job::Entity::find_state().select_only();
        for column in import_job::Column::iter() {
            query = if column.is_request() {
                let value = if matches!(
                    column,
                    import_job::Column::SourceValue
                        | import_job::Column::MetadataJson
                        | import_job::Column::TagsJson
                ) {
                    Expr::value(String::new())
                } else {
                    Expr::value(Option::<String>::None)
                };
                query.column_as(value, column)
            } else {
                query.column(column)
            };
        }
        for job in query
            .filter(import_job::Column::Id.is_in(page.iter().cloned()))
            .all(db)
            .await?
        {
            jobs.insert(job.id.clone(), job);
        }
    }
    if jobs.len() != ids.len() {
        return Err(DbErr::RecordNotFound(
            "claimed import job disappeared".into(),
        ));
    }
    let columns = import_job::Column::iter()
        .filter(|column| column.is_request())
        .collect::<Vec<_>>();
    for (id, values) in read_cells(db, ids, &columns).await? {
        let job = jobs
            .get_mut(&id)
            .ok_or_else(|| DbErr::RecordNotFound("claimed import job disappeared".into()))?;
        for (column, value) in columns.iter().zip(values) {
            let required = |value: Option<String>| {
                value.ok_or_else(|| DbErr::Custom("missing import request value".into()))
            };
            match column {
                import_job::Column::SourceValue => job.source_value = required(value)?,
                import_job::Column::MetadataJson => job.metadata_json = required(value)?,
                import_job::Column::TagsJson => job.tags_json = required(value)?,
                import_job::Column::ObjectContentType => job.object_content_type = value,
                import_job::Column::PinDecisionJson => job.pin_decision_json = value,
                import_job::Column::ZipIntentJson => job.zip_intent_json = value,
                import_job::Column::DecompressPrefix => job.decompress_prefix = value,
                import_job::Column::RootCaptureJson => job.root_capture_json = value,
                _ => unreachable!("request columns only"),
            }
        }
    }
    Ok(jobs.into_values().collect())
}

pub(super) async fn read_cells<C: ConnectionTrait>(
    db: &C,
    ids: &[String],
    columns: &[import_job::Column],
) -> Result<BTreeMap<String, Vec<Option<String>>>, DbErr> {
    let backend = db.get_database_backend();
    let longest_id = ids.iter().map(String::len).max().unwrap_or(0);
    let metadata_bytes = ids
        .iter()
        .map(|id| id.len() + 128 + columns.len() * 32)
        .sum::<usize>();
    if ids.len() > 256 || longest_id > 65536 || metadata_bytes > 1024 * 1024 {
        return Err(DbErr::Custom(
            "import request identity page exceeds budget".into(),
        ));
    }
    let payload_rows = (1024 * 1024 / (65536 + longest_id + 128)).min(15);
    let id_bytes = format!("length({})", octets(backend, "job_id"));
    let mut result: BTreeMap<_, _> = ids
        .iter()
        .cloned()
        .map(|id| (id, Vec::with_capacity(columns.len())))
        .collect();
    let lengths = columns
        .iter()
        .map(|column| {
            let name = Iden::to_string(column);
            format!(
                "CAST(length({}) AS BIGINT) AS \"{name}\"",
                octets(backend, &name)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let slots = (1..=ids.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let lock = if backend == DatabaseBackend::Postgres {
        " FOR SHARE OF r"
    } else {
        ""
    };
    let rows = db.query_all(Statement::from_sql_and_values(backend,
        format!("SELECT CASE WHEN {id_bytes}<=65536 THEN r.job_id END AS job_id,{lengths} FROM import_job_requests r WHERE r.job_id IN ({slots}){lock}"),
        ids.iter().cloned().map(Into::into))).await?;
    if rows.len() != ids.len() {
        return Err(DbErr::RecordNotFound("import request disappeared".into()));
    }
    let mut resident = ids.iter().fold(0_i64, |bytes, id| {
        bytes.saturating_add((id.len() as i64).saturating_mul(8).saturating_add(16384))
    });
    for row in &rows {
        for column in columns {
            let bytes = row
                .try_get::<Option<i64>>("", &Iden::to_string(column))?
                .unwrap_or(0);
            resident = resident.saturating_add(bytes.saturating_mul(2));
        }
    }
    if resident > 32 * 1024 * 1024 {
        return Err(DbErr::Custom("import request resident budget".into()));
    }
    for &column in columns {
        let name = Iden::to_string(&column);
        let bytes_sql = octets(backend, &name);
        let mut values = BTreeMap::new();
        let mut maximum = 0;
        for row in &rows {
            let id: String = row.try_get("", "job_id")?;
            let length: Option<i64> = row.try_get("", &name)?;
            let value = if let Some(length) = length {
                if !(0..=32 * 1024 * 1024).contains(&length) {
                    return Err(DbErr::Custom("import request resident budget".into()));
                }
                maximum = maximum.max(length as usize);
                Some((length as usize, Vec::with_capacity(length as usize)))
            } else {
                None
            };
            values.insert(id, value);
        }
        for offset in (0..maximum).step_by(65536) {
            let pending = values
                .iter()
                .filter_map(|(id, value)| {
                    value
                        .as_ref()
                        .filter(|(length, _)| *length > offset)
                        .map(|(length, _)| (id.clone(), *length))
                })
                .collect::<Vec<_>>();
            for page in pending.chunks(payload_rows) {
                let tuples = (0..page.len())
                    .map(|i| format!("(${},${})", i * 2 + 1, i * 2 + 2))
                    .collect::<Vec<_>>()
                    .join(",");
                let bindings = page
                    .iter()
                    .flat_map(|(id, length)| [id.clone().into(), (*length as i64).into()])
                    .collect::<Vec<_>>();
                let chunks = db.query_all(Statement::from_sql_and_values(backend,
                    format!("WITH requested(job_id,bytes) AS (VALUES {tuples}) SELECT CASE WHEN {id_bytes}<=65536 THEN r.job_id END AS job_id,CASE WHEN length({bytes_sql})=requested.bytes THEN substr({bytes_sql},{},65536) END AS payload FROM import_job_requests r JOIN requested ON requested.job_id=r.job_id", offset + 1), bindings)).await?;
                if chunks.len() != page.len() {
                    return Err(DbErr::RecordNotFound("import request disappeared".into()));
                }
                for row in chunks {
                    let id: String = row.try_get("", "job_id")?;
                    let chunk: Vec<u8> = row.try_get("", "payload")?;
                    let (length, bytes) = values
                        .get_mut(&id)
                        .and_then(Option::as_mut)
                        .ok_or_else(|| DbErr::Custom("unexpected import request cell".into()))?;
                    if bytes.len() != offset
                        || chunk.is_empty()
                        || chunk.len() > 65536
                        || bytes.len() + chunk.len() > *length
                    {
                        return Err(DbErr::Custom("import request changed".into()));
                    }
                    bytes.extend_from_slice(&chunk);
                }
            }
        }
        for (id, value) in values {
            let value = value
                .map(|(length, bytes)| {
                    if bytes.len() != length {
                        return Err(DbErr::Custom("incomplete import request".into()));
                    }
                    String::from_utf8(bytes)
                        .map_err(|_| DbErr::Custom("invalid import request UTF-8".into()))
                })
                .transpose()?;
            let cells = result
                .get_mut(&id)
                .ok_or_else(|| DbErr::RecordNotFound("import request disappeared".into()))?;
            cells.push(value);
        }
    }
    Ok(result)
}
