//! Only program-defined SQL is retained; installed catalog rows are always fresh.
use super::*;
use std::sync::OnceLock;

pub(super) struct SqliteObject {
    pub kind: &'static str,
    pub table: String,
    ddl: String,
    definition: String,
    record: bool,
}

impl SqliteObject {
    fn new(kind: &'static str, table: &str, ddl: String, record: bool) -> Self {
        let definition = if record {
            normalized(&ddl)
        } else {
            storage_sql(&ddl)
        };
        Self {
            kind,
            table: table.to_owned(),
            ddl,
            definition,
            record,
        }
    }

    pub fn matches(&self, sql: &str) -> bool {
        self.ddl == sql
            || self.definition
                == if self.record {
                    normalized(sql)
                } else {
                    storage_sql(sql)
                }
    }
}

pub(super) struct SqliteCatalog {
    pub objects: BTreeMap<String, SqliteObject>,
    pub query: String,
    pub cap: usize,
}

pub(super) fn expected(records: bool) -> &'static SqliteCatalog {
    static STORAGE: OnceLock<SqliteCatalog> = OnceLock::new();
    static SCHEMA: OnceLock<SqliteCatalog> = OnceLock::new();
    let cache = if records { &SCHEMA } else { &STORAGE };
    cache.get_or_init(|| build(records))
}

fn build(records: bool) -> SqliteCatalog {
    let backend = DatabaseBackend::Sqlite;
    let mut tables = tables(backend);
    let mut shadow_names = tables.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    if records {
        shadow_names.extend([RECORDS, RETIREMENTS, "seaql_migrations"]);
        tables.extend([
            (RECORDS, records_ddl(backend)),
            (RETIREMENTS, retirements_ddl(backend)),
        ]);
    }
    let scoped_tables = tables.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let mut expected = BTreeMap::new();
    for (name, ddl) in tables {
        expected.insert(
            name.to_owned(),
            SqliteObject::new("table", name, ddl, matches!(name, RECORDS | RETIREMENTS)),
        );
    }
    for (kind, objects) in [("index", indexes()), ("trigger", sqlite_protections())] {
        for (name, ddl) in objects {
            let suffix = ddl.split_once(" ON ").expect("static object SQL").1;
            let table = suffix.split(['(', ' ']).next().expect("static table");
            expected.insert(
                name.to_owned(),
                SqliteObject::new(kind, table, ddl.to_owned(), false),
            );
        }
    }
    for (name, table, event, body) in triggers(backend) {
        let ddl = trigger_ddl(backend, &name, &table, event, &body).remove(0);
        expected.insert(name, SqliteObject::new("trigger", &table, ddl, false));
    }
    let cap = expected.len() + scoped_tables.len();
    let query = catalog_query(&expected, &scoped_tables, &shadow_names, cap);
    SqliteCatalog {
        objects: expected,
        query,
        cap,
    }
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn catalog_query(
    objects: &BTreeMap<String, SqliteObject>,
    tables: &[&str],
    shadow_names: &[&str],
    cap: usize,
) -> String {
    let expected = objects
        .iter()
        .map(|(name, object)| {
            format!(
                "({},{},{},{})",
                literal(name),
                literal(object.kind),
                literal(&object.table),
                literal(&object.ddl)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let tables = tables
        .iter()
        .map(|name| literal(name))
        .collect::<Vec<_>>()
        .join(",");
    let shadow_names = shadow_names
        .iter()
        .map(|name| literal(name))
        .collect::<Vec<_>>()
        .join(",");
    // Exact matches are checked inside this fresh catalog statement, not returned
    // as owned SQLite rows. Only differences need Rust's layout normalization.
    format!(
        "WITH expected(name,kind,table_name,sql) AS (VALUES {expected}),\
        actual AS MATERIALIZED (\
            SELECT substr(name,1,{NAME_BYTES}) AS name,substr(type,1,16) AS type,substr(tbl_name,1,{NAME_BYTES}) AS tbl_name,\
            CASE WHEN length(CAST(sql AS BLOB))<={SQL_BYTES} THEN sql ELSE NULL END AS sql,\
            length(CAST(name AS BLOB))<={NAME_BYTES} AND length(CAST(tbl_name AS BLOB))<={NAME_BYTES} AND length(type)<=16 AND (sql IS NULL OR length(CAST(sql AS BLOB))<={SQL_BYTES}) AS bounded \
            FROM main.sqlite_schema WHERE name IN (SELECT name FROM expected) OR tbl_name IN ({tables}) LIMIT {limit}\
        ), catalog AS (\
        SELECT a.name,a.type,a.tbl_name,a.sql,a.bounded FROM actual a LEFT JOIN expected e ON e.name=a.name \
        WHERE a.bounded IS NOT 1 \
            OR (e.name IS NULL AND NOT (a.type='index' AND a.sql IS NULL AND a.tbl_name IN ({tables}))) \
            OR (e.name IS NOT NULL AND (a.type IS NOT e.kind OR a.tbl_name IS NOT e.table_name OR a.sql IS NOT e.sql)) \
        UNION ALL SELECT e.name,NULL,NULL,NULL,0 FROM expected e WHERE (SELECT COUNT(*) FROM actual a WHERE a.name=e.name)<>1 \
        UNION ALL SELECT '',NULL,NULL,NULL,0 WHERE (SELECT COUNT(*) FROM actual)>{cap} LIMIT {limit}\
        ), shadow AS MATERIALIZED (\
            SELECT EXISTS(SELECT 1 FROM sqlite_temp_schema WHERE type IN ('table','view') AND name IN ({shadow_names})) AS present\
        ) \
        SELECT 'catalog' AS row_kind,name,type,tbl_name,sql,bounded,NULL AS shadow,NULL AS ready FROM catalog \
        UNION ALL SELECT 'control',NULL,NULL,NULL,NULL,NULL,present,\
            CASE WHEN present=1 THEN 0 ELSE EXISTS(SELECT 1 FROM pin_management_readiness WHERE id=1 AND format='S75A1' AND state=$1) END FROM shadow",
        limit = cap + 1
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;

    #[tokio::test]
    async fn control_does_not_consume_catalog_capacity() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("CREATE TABLE sample (value TEXT)")
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE TABLE pin_management_readiness (id INTEGER,format TEXT,state TEXT)",
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO pin_management_readiness VALUES (1,'S75A1','ready')")
            .await
            .unwrap();
        let mut catalog = SqliteCatalog {
            objects: BTreeMap::from([(
                "sample".into(),
                SqliteObject::new(
                    "table",
                    "sample",
                    "CREATE TABLE sample (\nvalue TEXT)".into(),
                    false,
                ),
            )]),
            query: String::new(),
            cap: 1,
        };
        catalog.query = catalog_query(&catalog.objects, &["sample"], &["sample"], catalog.cap);
        let read = || {
            Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                catalog.query.clone(),
                ["ready".into()],
            )
        };
        let rows = db.query_all(read()).await.unwrap();
        assert_eq!(rows.len(), catalog.cap + 1);
        super::super::sqlite_rows(&rows, &catalog).unwrap();

        db.execute_unprepared("CREATE INDEX extra ON sample(value)")
            .await
            .unwrap();
        let rows = db.query_all(read()).await.unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| row.try_get::<String>("", "row_kind").unwrap() == "catalog")
                .count(),
            catalog.cap + 1
        );
        assert_eq!(rows.len(), catalog.cap + 2);
        assert!(super::super::sqlite_rows(&rows, &catalog).is_err());
    }
}
