//! Live controls on the actual transaction, independent of cached layout facts.
use super::*;
use sea_orm_migration::MigratorTrait;
use std::collections::BTreeSet;

pub(super) struct Compiled {
    pub(super) known: Vec<String>,
    control_query: String,
    versions_query: Option<String>,
}

impl Compiled {
    pub(super) fn new() -> Self {
        let known = crate::store::migrator::Migrator::migrations()
            .iter()
            .map(|migration| migration.name().to_owned())
            .collect();
        Self::compile(known)
    }

    fn compile(known: Vec<String>) -> Self {
        let shadow = shadow_query();
        let live = format!(
            "{shadow},live AS MATERIALIZED (SELECT present AS shadow,\
             CASE WHEN present=1 THEN 0 ELSE \
             EXISTS(SELECT 1 FROM main.pin_management_readiness \
             WHERE id=1 AND format='S75A1' AND state='ready') END AS ready FROM shadow)"
        );
        let compact = compact_queries(&live, &known);
        let (control_query, versions_query) = match compact {
            Some((control, versions)) => (control, Some(versions)),
            None => (format!("{live} SELECT shadow,ready FROM live"), None),
        };
        Self {
            known,
            control_query,
            versions_query,
        }
    }
}

fn shadow_query() -> String {
    let mut names: BTreeSet<&str> = native::current_catalog()
        .iter()
        .map(|table| table.table.as_str())
        .collect();
    let storage = management::schema::storage::tables(DatabaseBackend::Sqlite);
    names.extend(storage.iter().map(|(name, _)| *name));
    names.extend(management::schema::storage::SOURCE_TABLES.iter().copied());
    names.extend([
        management::schema::RECORDS,
        management::schema::RETIREMENTS,
        "seaql_migrations",
    ]);
    let names = names
        .into_iter()
        .map(|name| format!("X'{}'", hex::encode(name.to_ascii_lowercase().as_bytes())))
        .collect::<Vec<_>>()
        .join(",");
    // Identifier folding is ASCII; BLOB equality cannot invoke overridden collations.
    format!(
        "WITH shadow AS MATERIALIZED (SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema \
         WHERE type IN ('table','view') AND CAST(lower(name) AS BLOB) IN ({names})) AS present)"
    )
}

fn compact_queries(live: &str, known: &[String]) -> Option<(String, String)> {
    if !(1..=64).contains(&known.len()) {
        return None;
    }
    let count = known.len();
    let max = known.iter().map(String::len).max().unwrap_or(0);
    let control = format!(
        "{live} SELECT shadow,ready,CASE WHEN shadow<>0 OR ready<>1 THEN 0 ELSE \
         (WITH bounded AS MATERIALIZED (SELECT CASE WHEN typeof(version)='text' THEN \
         CASE WHEN length(CAST(version AS BLOB))<={max} THEN 1 ELSE 0 END ELSE 0 END AS valid \
         FROM main.seaql_migrations ORDER BY seaql_migrations.version LIMIT 65) \
         SELECT CASE WHEN COUNT(*)={count} AND COALESCE(MIN(valid),0)=1 THEN 1 ELSE 0 END \
         FROM bounded) END AS bounded_input FROM live"
    );
    let expected = known
        .iter()
        .enumerate()
        .map(|(position, version)| {
            format!("({},X'{}')", position + 1, hex::encode(version.as_bytes()))
        })
        .collect::<Vec<_>>()
        .join(",");
    let versions = format!(
        "WITH expected(position,bytes) AS (VALUES {expected}),\
         actual AS MATERIALIZED (SELECT ROW_NUMBER() OVER (ORDER BY seaql_migrations.version) AS position,\
         CASE WHEN typeof(version)='text' THEN CASE WHEN length(CAST(version AS BLOB))<={max} \
         THEN CAST(version AS BLOB) ELSE NULL END ELSE NULL END AS bytes FROM main.seaql_migrations) \
         SELECT CASE WHEN COUNT(*)={count} AND COALESCE(MIN(CASE WHEN actual.bytes=expected.bytes \
         THEN 1 ELSE 0 END),0)=1 THEN 1 ELSE 0 END AS versions \
         FROM actual LEFT JOIN expected ON expected.position=actual.position"
    );
    Some((control, versions))
}

/// The caller supplies its actual transaction and does no intervening business
/// work: the first query's whole-source bound protects the second query's window.
pub(super) async fn validate<C: ConnectionTrait>(db: &C, compiled: &Compiled) -> AppResult<()> {
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            &compiled.control_query,
        ))
        .await?
        .ok_or_else(invalid)?;
    if row.try_get::<i64>("", "shadow")? != 0 || row.try_get::<i64>("", "ready")? != 1 {
        return Err(invalid());
    }
    if let Some(query) = &compiled.versions_query {
        if row.try_get::<i64>("", "bounded_input")? != 1 {
            return Err(invalid());
        }
        let row = db
            .query_one(Statement::from_string(DatabaseBackend::Sqlite, query))
            .await?
            .ok_or_else(invalid)?;
        if row.try_get::<i64>("", "versions")? != 1 {
            return Err(invalid());
        }
    } else {
        management::schema::read_versions(db)
            .await
            .map_err(work_error)?;
    }
    if compiled.known.last().map(String::as_str) != Some(management::schema::VERSION) {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_query_eligibility_preserves_the_bounded_source_proof() {
        for count in [0, 1, 36, 64, 65, 66] {
            let known = vec![management::schema::VERSION.to_owned(); count];
            assert_eq!(
                compact_queries("", &known).is_some(),
                (1..=64).contains(&count)
            );
        }
    }

    #[tokio::test]
    async fn full_decoder_fallback_accepts_current_versions_and_rejects_corruption() {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let tx = db.begin().await.unwrap();
        let mut compiled = Compiled::new();
        compiled.control_query = format!(
            "{},live AS MATERIALIZED (SELECT present AS shadow,CASE WHEN present=1 THEN 0 ELSE \
             EXISTS(SELECT 1 FROM main.pin_management_readiness WHERE id=1 AND format='S75A1' \
             AND state='ready') END AS ready FROM shadow) SELECT shadow,ready FROM live",
            shadow_query()
        );
        compiled.versions_query = None;
        validate(&tx, &compiled).await.unwrap();
        tx.execute_unprepared("DELETE FROM seaql_migrations WHERE version=(SELECT min(version) FROM seaql_migrations)")
            .await.unwrap();
        assert!(validate(&tx, &compiled).await.is_err());
        tx.rollback().await.unwrap();
    }
}
