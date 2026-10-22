//! Fresh, bounded structural catalog reads for the storage tables in one validation.
use super::*;
use native::NativeTable;
use sea_orm::{QueryResult, Value};

fn grouped(rows: Vec<QueryResult>) -> Result<BTreeMap<String, Vec<QueryResult>>> {
    let mut tables = BTreeMap::<String, Vec<QueryResult>>::new();
    for row in rows {
        let name = row
            .try_get("", "table_name")
            .map_err(|_| ManagementError::Schema)?;
        tables.entry(name).or_default().push(row);
    }
    Ok(tables)
}

pub(crate) async fn validate<C: ConnectionTrait>(db: &C, tables: &[NativeTable]) -> Result<()> {
    let backend = DatabaseBackend::Postgres;
    let names = parameters(tables.len());
    let values = tables
        .iter()
        .map(|table| Value::from(table.table.clone()))
        .collect::<Vec<_>>();
    let mut shadow_values = values.clone();
    shadow_values.push("seaql_migrations".into());
    if db.query_one(Statement::from_sql_and_values(backend, format!(
        "SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace=pg_my_temp_schema() AND relname IN ({}) LIMIT 1",
        parameters(shadow_values.len())), shadow_values)).await.map_err(|_| ManagementError::Schema)?.is_some() {
        return Err(ManagementError::Schema);
    }
    let shapes = db.query_all(Statement::from_sql_and_values(backend, format!(
        "SELECT c.relname::text AS table_name,c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.relhassubclass FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname IN ({names}) LIMIT {}",
        tables.len() + 1), values.clone())).await.map_err(|_| ManagementError::Schema)?;
    if shapes.len() != tables.len() {
        return Err(ManagementError::Schema);
    }
    let shapes = grouped(shapes)?;
    let column_count = tables
        .iter()
        .map(|table| table.definitions.len())
        .sum::<usize>();
    let columns = db.query_all(Statement::from_sql_and_values(backend, format!(
        "SELECT c.relname::text AS table_name,a.attname::text AS name,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,a.attisdropped,a.attgenerated::text AS generated FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname IN ({names}) AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attname LIMIT {}",
        column_count + 1), values.clone())).await.map_err(|_| ManagementError::Schema)?;
    if columns.len() != column_count {
        return Err(ManagementError::Schema);
    }
    let columns = grouped(columns)?;
    let key_count = tables
        .iter()
        .map(|table| table.primary_key.len())
        .sum::<usize>();
    let keys = db.query_all(Statement::from_sql_and_values(backend, format!(
        "SELECT c.relname::text AS table_name,a.attname::text AS name FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(k.conkey) WITH ORDINALITY AS key(attnum,ordinal) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=key.attnum WHERE n.nspname=current_schema() AND c.relname IN ({names}) AND k.contype='p' AND k.convalidated AND NOT k.condeferrable ORDER BY c.relname,key.ordinal LIMIT {}",
        key_count + 1), values)).await.map_err(|_| ManagementError::Schema)?;
    if keys.len() != key_count {
        return Err(ManagementError::Schema);
    }
    let keys = grouped(keys)?;
    for table in tables {
        let shape = shapes
            .get(&table.table)
            .filter(|rows| rows.len() == 1)
            .and_then(|rows| rows.first())
            .ok_or(ManagementError::Schema)?;
        if shape
            .try_get::<String>("", "kind")
            .map_err(|_| ManagementError::Schema)?
            != "r"
            || shape
                .try_get::<String>("", "persistence")
                .map_err(|_| ManagementError::Schema)?
                != "p"
        {
            return Err(ManagementError::Schema);
        }
        for flag in [
            "relrowsecurity",
            "relforcerowsecurity",
            "relispartition",
            "relhassubclass",
        ] {
            if shape
                .try_get::<bool>("", flag)
                .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
        }
        let columns = columns.get(&table.table).ok_or(ManagementError::Schema)?;
        if columns.len() != table.definitions.len() {
            return Err(ManagementError::Schema);
        }
        for (row, expected) in columns.iter().zip(&table.definitions) {
            if row
                .try_get::<String>("", "name")
                .map_err(|_| ManagementError::Schema)?
                != expected.name
                || row
                    .try_get::<String>("", "type")
                    .map_err(|_| ManagementError::Schema)?
                    != expected.postgres_type
                || row
                    .try_get::<bool>("", "attnotnull")
                    .map_err(|_| ManagementError::Schema)?
                    == expected.nullable
                || row
                    .try_get::<bool>("", "attisdropped")
                    .map_err(|_| ManagementError::Schema)?
                || !row
                    .try_get::<String>("", "generated")
                    .map_err(|_| ManagementError::Schema)?
                    .is_empty()
            {
                return Err(ManagementError::Schema);
            }
        }
        let keys = keys.get(&table.table).ok_or(ManagementError::Schema)?;
        let keys = keys
            .iter()
            .map(|row| {
                row.try_get::<String>("", "name")
                    .map_err(|_| ManagementError::Schema)
            })
            .collect::<Result<Vec<_>>>()?;
        if keys != *table.primary_key {
            return Err(ManagementError::Schema);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::store::pinning::management::schema::storage;
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, Statement, TransactionTrait,
    };

    #[tokio::test]
    #[ignore = "requires parent-owned initialized exclusive IPFS_S3_STAGE75_STORAGE_DATABASE_URL"]
    async fn postgres_storage_structure_is_rechecked_per_table() {
        let url = std::env::var("IPFS_S3_STAGE75_STORAGE_DATABASE_URL").unwrap();
        let mut options = ConnectOptions::new(url);
        options
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false);
        let db = Database::connect(options).await.unwrap();
        let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
            "SELECT current_schema()::text AS name,pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(),0)) AS claimed"))
            .await.unwrap().unwrap();
        assert!(
            row.try_get::<String>("", "name")
                .unwrap()
                .starts_with("stage7_")
        );
        assert!(row.try_get::<bool>("", "claimed").unwrap());
        storage::validate(&db).await.unwrap();
        for damage in [
            "CREATE TEMP VIEW seaql_migrations AS SELECT 'shadow'::text AS version",
            "ALTER TABLE pin_management_revision ENABLE ROW LEVEL SECURITY",
            "ALTER TABLE pin_management_revision ALTER COLUMN revision DROP NOT NULL",
            "ALTER TABLE pin_management_revision ALTER COLUMN revision TYPE numeric",
            "ALTER TABLE pin_management_revision ADD COLUMN surplus text; ALTER TABLE pin_management_pending_scope DROP COLUMN locator_generation",
            "ALTER TABLE pin_management_locator DROP CONSTRAINT pin_management_locator_pkey; ALTER TABLE pin_management_locator ADD PRIMARY KEY(source_key,selector_kind,selector_key)",
        ] {
            let tx = db.begin().await.unwrap();
            tx.execute_unprepared(damage).await.unwrap();
            let rejected = storage::validate(&tx).await.is_err();
            tx.rollback().await.unwrap();
            assert!(rejected, "structural changes must reject authority");
            storage::validate(&db).await.unwrap();
        }
        db.close().await.unwrap();
    }
}
