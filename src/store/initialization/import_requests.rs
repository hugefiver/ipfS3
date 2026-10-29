//! Lossless vertical partition. Cold cells stay SQL text/NULL, never JSON casts.
use crate::store::{entities::import_job, pinning::management::schema::storage};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Iterable, Statement, sea_query::Iden};

async fn source_page<C: ConnectionTrait>(
    db: &C,
    after: Option<&str>,
) -> Result<Vec<String>, DbErr> {
    let length = if db.get_database_backend() == DatabaseBackend::Sqlite {
        "length(CAST(id AS BLOB))"
    } else {
        "octet_length(id)"
    };
    let (predicate, values) = if let Some(after) = after {
        ("WHERE id>$1", vec![after.into()])
    } else {
        ("", Vec::new())
    };
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        format!("WITH candidates AS (SELECT id,{length} AS bytes FROM import_jobs {predicate} ORDER BY id LIMIT 256), bounded AS (SELECT CASE WHEN bytes<=65536 THEN id END AS id,SUM(CASE WHEN bytes<=65536 THEN bytes ELSE 65536 END+1024) OVER (ORDER BY id) AS page_bytes FROM candidates) SELECT id FROM bounded WHERE page_bytes<=1048576 ORDER BY id"), values)).await?;
    rows.into_iter().map(|row| row.try_get("", "id")).collect()
}

pub(crate) async fn migrate<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let columns = import_job::Column::iter()
        .filter(|column| column.is_request())
        .map(|column| Iden::to_string(&column))
        .collect::<Vec<_>>();
    let names = columns.join(",");
    if db.get_database_backend() == DatabaseBackend::Postgres {
        let conflicts = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT DISTINCT i.relname AS name FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=i.oid JOIN pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid WHERE x.indrelid='import_jobs'::regclass AND a.attrelid=x.indrelid AND a.attname=ANY(string_to_array($1,',')) ORDER BY i.relname",
            [names.clone().into()])).await?;
        if !conflicts.is_empty() {
            let names = conflicts
                .into_iter()
                .map(|r| r.try_get::<String>("", "name"))
                .collect::<Result<Vec<_>, _>>()?;
            return Err(DbErr::Migration(format!(
                "import request columns have dependent indexes: {}",
                names.join(", ")
            )));
        }
    }
    db.execute_unprepared("CREATE TABLE import_job_requests (job_id TEXT NOT NULL PRIMARY KEY, source_value TEXT NOT NULL, object_content_type TEXT, metadata_json TEXT NOT NULL, tags_json TEXT NOT NULL, pin_decision_json TEXT, zip_intent_json TEXT, decompress_prefix TEXT, root_capture_json TEXT, FOREIGN KEY(job_id) REFERENCES import_jobs(id) ON DELETE CASCADE)").await?;
    let mut after: Option<String> = None;
    loop {
        let ids = source_page(db, after.as_deref()).await?;
        if ids.is_empty() {
            break;
        }
        let slots = (1..=ids.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(",");
        let inserted = db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            format!("INSERT INTO import_job_requests(job_id,{names}) SELECT id,{names} FROM import_jobs WHERE id IN ({slots})"),
            ids.iter().cloned().map(Into::into))).await?;
        if inserted.rows_affected() != ids.len() as u64 {
            return Err(DbErr::Migration("import request source changed".into()));
        }
        after = ids.last().cloned();
    }
    // Copy cells in the database without decoding, normalization or driver
    // materialization. The isolated schema transaction retains the source on
    // failure and publishes neither a partial layout nor partial jobs.
    for column in columns {
        db.execute_unprepared(&format!("ALTER TABLE import_jobs DROP COLUMN {column}"))
            .await?;
    }
    db.execute_unprepared("INSERT INTO pin_management_revision VALUES ('import_job_requests',0)")
        .await?;
    for (name, table, event, body) in storage::triggers(db.get_database_backend())
        .into_iter()
        .filter(|(_, table, _, _)| table == "import_job_requests")
    {
        for ddl in storage::trigger_ddl(db.get_database_backend(), &name, &table, event, &body) {
            db.execute_unprepared(&ddl).await?;
        }
    }
    if db.get_database_backend() == DatabaseBackend::Postgres {
        db.execute_unprepared("CREATE TRIGGER s75_import_job_requests_truncate BEFORE TRUNCATE ON import_job_requests FOR EACH STATEMENT EXECUTE FUNCTION s75_no_truncate()").await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migration_key_pages_bound_utf8_bytes_before_returning_rows() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("CREATE TABLE import_jobs(id TEXT NOT NULL PRIMARY KEY)")
            .await
            .unwrap();
        let suffix = "界".repeat(21000);
        for index in 0..257 {
            db.execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                "INSERT INTO import_jobs(id) VALUES ($1)",
                [format!("{index:06}{suffix}").into()],
            ))
            .await
            .unwrap();
        }
        let mut after = None;
        let mut count = 0;
        loop {
            let rows = source_page(&db, after.as_deref()).await.unwrap();
            if rows.is_empty() {
                break;
            }
            assert!(rows.len() <= 256);
            assert!(rows.iter().map(String::len).sum::<usize>() <= 1024 * 1024);
            for id in &rows {
                assert_eq!(id, &format!("{count:06}{suffix}"));
                count += 1;
            }
            after = rows.last().cloned();
        }
        assert_eq!(count, 257);
    }
}
