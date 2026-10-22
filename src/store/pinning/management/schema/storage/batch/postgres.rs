//! Bounded reads for ordinary revision/truncate triggers and storage indexes.
use super::*;
use std::sync::OnceLock;

// Preserve the independent guard/constraint validators, but reject oversized
// deparsed metadata before those validators request the original text.
pub(crate) async fn validate_bounds<C: ConnectionTrait>(
    db: &C,
    tables: &[(&str, String)],
) -> Result<()> {
    let names = tables
        .iter()
        .map(|(name, _)| *name)
        .chain([RECORDS, RETIREMENTS]);
    let values = names.map(sea_orm::Value::from).collect::<Vec<_>>();
    let parameters = parameters(values.len());
    let oversized = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres, format!(
        "SELECT 1 WHERE EXISTS (\
            SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid \
            WHERE n.nspname=current_schema() AND c.relname IN ({parameters}) AND (octet_length(p.prosrc)>{SQL_BYTES} OR octet_length(pg_get_triggerdef(t.oid,false))>{SQL_BYTES})\
        ) OR EXISTS (\
            SELECT 1 FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace \
            WHERE n.nspname=current_schema() AND c.relname IN ({parameters}) AND octet_length(pg_get_constraintdef(k.oid,false))>{SQL_BYTES}\
        ) OR EXISTS (\
            SELECT 1 FROM pg_attrdef d JOIN pg_class c ON c.oid=d.adrelid JOIN pg_namespace n ON n.oid=c.relnamespace \
            WHERE n.nspname=current_schema() AND c.relname IN ({parameters}) AND octet_length(pg_get_expr(d.adbin,d.adrelid,false))>{SQL_BYTES}\
        )"), values)).await.map_err(|_| ManagementError::Schema)?;
    if oversized.is_some() {
        return Err(ManagementError::Schema);
    }
    Ok(())
}

struct Trigger {
    table: String,
    kind: i32,
    function: String,
    body: String,
}

fn expected_triggers() -> &'static BTreeMap<String, Trigger> {
    // Only compiled expectations are retained; every installed trigger is reread.
    static EXPECTED: OnceLock<BTreeMap<String, Trigger>> = OnceLock::new();
    EXPECTED.get_or_init(build_triggers)
}

fn build_triggers() -> BTreeMap<String, Trigger> {
    let mut expected = BTreeMap::new();
    for (name, table, event, body) in triggers(DatabaseBackend::Postgres) {
        let kind = match event {
            "INSERT" => 5,
            "UPDATE" => 17,
            _ => 9,
        };
        expected.insert(
            name.clone(),
            Trigger {
                table,
                kind,
                function: name,
                body: format!(" BEGIN {body} RETURN NULL; END "),
            },
        );
    }
    for table in native::registry() {
        expected.insert(
            format!("s75_{}_truncate", table.table),
            Trigger {
                table: table.table,
                kind: 34,
                function: "s75_no_truncate".into(),
                body: " BEGIN RAISE EXCEPTION 'controlled migration required'; END ".into(),
            },
        );
    }
    expected
}

pub(crate) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    indexes_catalog(db).await?;
    let expected = expected_triggers();
    let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres, format!(
        "SELECT t.tgname::text AS name,c.relname::text AS table_name,t.tgenabled::text AS enabled,t.tgtype::integer AS kind,p.proname::text AS function_name,\
        CASE WHEN octet_length(p.prosrc)<={SQL_BYTES} THEN p.prosrc ELSE NULL END AS prosrc, \
        t.tgqual IS NULL AND t.tgattr=''::int2vector AND t.tgnargs=0 AND NOT t.tgisinternal AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AND NOT p.prosecdef AND p.proconfig IS NULL AND pn.nspname=current_schema() AND l.lanname='plpgsql' AS complete \
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang \
        WHERE n.nspname=current_schema() AND t.tgname IN ({}) LIMIT {}",
        parameters(expected.len()), expected.len() + 1),
        expected.keys().cloned().map(sea_orm::Value::from))).await.map_err(|_| ManagementError::Schema)?;
    if rows.len() != expected.len() {
        return Err(ManagementError::Schema);
    }
    let mut seen = BTreeSet::new();
    for row in rows {
        let name: String = row
            .try_get("", "name")
            .map_err(|_| ManagementError::Schema)?;
        let trigger = expected.get(&name).ok_or(ManagementError::Schema)?;
        if !seen.insert(name) {
            return Err(ManagementError::Schema);
        }
        if row
            .try_get::<String>("", "table_name")
            .map_err(|_| ManagementError::Schema)?
            != trigger.table
            || row
                .try_get::<String>("", "enabled")
                .map_err(|_| ManagementError::Schema)?
                != "O"
            || row
                .try_get::<i32>("", "kind")
                .map_err(|_| ManagementError::Schema)?
                != trigger.kind
            || !row
                .try_get::<bool>("", "complete")
                .map_err(|_| ManagementError::Schema)?
            || row
                .try_get::<String>("", "function_name")
                .map_err(|_| ManagementError::Schema)?
                != trigger.function
            || row
                .try_get::<String>("", "prosrc")
                .map_err(|_| ManagementError::Schema)?
                != trigger.body
        {
            return Err(ManagementError::Schema);
        }
    }
    Ok(())
}

async fn indexes_catalog<C: ConnectionTrait>(db: &C) -> Result<()> {
    let indexes = indexes();
    let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres, format!(
        "SELECT x.relname::text AS name,t.relname::text AS table_name,i.indisvalid,i.indisready,i.indpred IS NULL AND i.indexprs IS NULL AS ordinary,\
        CASE WHEN octet_length(columns.names)<={SQL_BYTES} THEN columns.names ELSE NULL END AS columns \
        FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace \
        CROSS JOIN LATERAL (SELECT string_agg(a.attname,',' ORDER BY k.n) AS names FROM unnest(i.indkey) WITH ORDINALITY k(attnum,n) JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum) columns \
        WHERE n.nspname=current_schema() AND x.relname IN ({}) LIMIT {}",
        parameters(indexes.len()), indexes.len() + 1),
        indexes.iter().map(|(name, _)| sea_orm::Value::from(*name)))).await.map_err(|_| ManagementError::Schema)?;
    if rows.len() != indexes.len() {
        return Err(ManagementError::Schema);
    }
    let mut expected = indexes.into_iter().collect::<BTreeMap<_, _>>();
    for row in rows {
        let name: String = row
            .try_get("", "name")
            .map_err(|_| ManagementError::Schema)?;
        let ddl = expected
            .remove(name.as_str())
            .ok_or(ManagementError::Schema)?;
        let (_, suffix) = ddl.split_once(" ON ").ok_or(ManagementError::Schema)?;
        let (table, columns) = suffix.split_once('(').ok_or(ManagementError::Schema)?;
        for flag in ["indisvalid", "indisready", "ordinary"] {
            if !row
                .try_get::<bool>("", flag)
                .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
        }
        if row
            .try_get::<String>("", "table_name")
            .map_err(|_| ManagementError::Schema)?
            != table
            || row
                .try_get::<String>("", "columns")
                .map_err(|_| ManagementError::Schema)?
                != columns.trim_end_matches(')')
        {
            return Err(ManagementError::Schema);
        }
    }
    Ok(())
}
