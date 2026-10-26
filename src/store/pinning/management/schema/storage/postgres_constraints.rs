//! Read-only PostgreSQL constraints and defaults for `storage::tables`.
use super::*;
use batch::SQL_BYTES;
use sea_orm::QueryResult;
use std::collections::{BTreeMap, BTreeSet};

const TABLES: &str = "(VALUES ('pin_management_archive_identity'),('pin_management_readiness'),('pin_management_source_control'),('pin_management_locator'),('pin_management_pending_scope'),('pin_management_revision'),('pin_management_archive'),('pin_management_archive_chunks'),('pin_management_inline_archive')) AS wanted(table_name)";

fn grouped(
    rows: Vec<QueryResult>,
    names: &[&str],
    per_table_cap: usize,
) -> Result<BTreeMap<String, Vec<QueryResult>>> {
    if rows.len() > names.len() * per_table_cap {
        return Err(ManagementError::Schema);
    }
    let mut groups: BTreeMap<String, Vec<QueryResult>> = names
        .iter()
        .map(|name| ((*name).to_owned(), Vec::new()))
        .collect();
    for row in rows {
        let name: String = row
            .try_get("", "table_name")
            .map_err(|_| ManagementError::Schema)?;
        let group = groups.get_mut(&name).ok_or(ManagementError::Schema)?;
        group.push(row);
        if group.len() > per_table_cap {
            return Err(ManagementError::Schema);
        }
    }
    Ok(groups)
}

fn definitions(table: &str, source_control: &str, archive: &str) -> BTreeSet<String> {
    let definitions = match table {
        "pin_management_archive_identity" => vec!["PRIMARY KEY (archive_id)".into()],
        "pin_management_readiness" => vec![
            "PRIMARY KEY (id)".into(),
            "CHECK ((id = 1))".into(),
            "CHECK ((format = 'S75A1'::text))".into(),
            "CHECK ((state = ANY (ARRAY['backfill'::text, 'ready'::text])))".into(),
        ],
        "pin_management_source_control" => vec![
            "PRIMARY KEY (source_key)".into(),
            "CHECK (((source_generation >= 0) AND (source_generation < '9223372036854775807'::bigint)))".into(),
            "CHECK (((locator_generation >= 0) AND (locator_generation < '9223372036854775807'::bigint)))".into(),
        ],
        "pin_management_locator" => vec![
            "PRIMARY KEY (selector_kind, selector_key, source_key)".into(),
            format!("FOREIGN KEY (source_key) REFERENCES {source_control}(source_key)"),
        ],
        "pin_management_pending_scope" => vec![
            "PRIMARY KEY (selector_kind, selector_key, source_key)".into(),
        ],
        "pin_management_revision" => vec![
            "PRIMARY KEY (source_table)".into(),
            "CHECK (((revision >= 0) AND (revision < '9223372036854775807'::bigint)))".into(),
        ],
        "pin_management_archive" => vec![
            "PRIMARY KEY (archive_id)".into(),
            "CHECK ((format = 'S75A1'::text))".into(),
            "CHECK ((state = ANY (ARRAY['staging'::text, 'sealed'::text])))".into(),
        ],
        "pin_management_archive_chunks" => vec![
            "PRIMARY KEY (archive_id, ordinal)".into(),
            "CHECK ((ordinal >= 0))".into(),
            "CHECK ((length(payload) <= 65536))".into(),
            format!("FOREIGN KEY (archive_id) REFERENCES {archive}(archive_id)"),
        ],
        "pin_management_inline_archive" => vec![
            "PRIMARY KEY (archive_id)".into(),
            "CHECK ((format = 'S75I2'::text))".into(),
            "CHECK (((bytes >= 0) AND (bytes <= 65536)))".into(),
            "CHECK ((length(payload) = bytes))".into(),
        ],
        _ => vec![],
    };
    definitions.into_iter().collect()
}

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    // Origin mode is required for ordinary RI and revision triggers to execute.
    // regclass renders the schema-qualified target exactly as the deparser does,
    // while resolving it from current_schema rather than an arbitrary search path.
    let targets = db.query_one(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT format('%I.%I',current_schema(),'pin_management_source_control')::regclass::text AS source_control, format('%I.%I',current_schema(),'pin_management_archive')::regclass::text AS archive WHERE current_setting('session_replication_role')='origin'"))
        .await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
    let source_control: String = targets
        .try_get("", "source_control")
        .map_err(|_| ManagementError::Schema)?;
    let archive: String = targets
        .try_get("", "archive")
        .map_err(|_| ManagementError::Schema)?;
    let tables = tables(DatabaseBackend::Postgres);
    let names = tables.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    let rows = db.query_all(Statement::from_string(DatabaseBackend::Postgres, format!(
        "SELECT wanted.table_name, checks.* FROM {TABLES} CROSS JOIN LATERAL (\
         SELECT CASE WHEN octet_length(pg_get_constraintdef(k.oid,false))<={SQL_BYTES} THEN pg_get_constraintdef(k.oid,false) ELSE NULL END AS definition, \
         k.convalidated AND NOT k.condeferrable AND NOT k.condeferred AND k.conislocal AND k.coninhcount=0 AND k.conparentid=0 AND k.contypid=0 AND k.connamespace=n.oid AND k.connoinherit=(k.contype<>'c') AS ordinary \
         FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace \
         WHERE n.nspname=current_schema() AND c.relname=wanted.table_name LIMIT 32) checks")))
        .await.map_err(|_| ManagementError::Schema)?;
    let groups = grouped(rows, &names, 32)?;
    for table in names.iter().copied() {
        let expected = definitions(table, &source_control, &archive);
        if expected.is_empty() {
            return Err(ManagementError::Schema);
        }
        let rows = groups.get(table).ok_or(ManagementError::Schema)?;
        if rows.len() != expected.len() {
            return Err(ManagementError::Schema);
        }
        let mut actual = BTreeSet::new();
        for row in rows {
            if !row
                .try_get::<bool>("", "ordinary")
                .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
            // Compare the complete deparsed SQL, preserving quoted literals.
            if !actual.insert(
                row.try_get::<String>("", "definition")
                    .map_err(|_| ManagementError::Schema)?,
            ) {
                return Err(ManagementError::Schema);
            }
        }
        if actual != expected {
            return Err(ManagementError::Schema);
        }
    }
    foreign_key_triggers(db).await?;
    defaults(db, &names).await?;
    Ok(())
}

async fn foreign_key_triggers<C: ConnectionTrait>(db: &C) -> Result<()> {
    let rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"SELECT wanted.table_name, checks.* FROM
            (VALUES ('pin_management_locator'),('pin_management_archive_chunks')) AS wanted(table_name)
            CROSS JOIN LATERAL (SELECT
                CASE
                    WHEN t.tgrelid=k.conrelid AND t.tgconstrrelid=k.confrelid
                        AND t.tgtype=5 AND t.tgfoid='pg_catalog."RI_FKey_check_ins"()'::regprocedure THEN 1
                    WHEN t.tgrelid=k.conrelid AND t.tgconstrrelid=k.confrelid
                        AND t.tgtype=17 AND t.tgfoid='pg_catalog."RI_FKey_check_upd"()'::regprocedure THEN 2
                    WHEN t.tgrelid=k.confrelid AND t.tgconstrrelid=k.conrelid
                        AND t.tgtype=9 AND t.tgfoid='pg_catalog."RI_FKey_noaction_del"()'::regprocedure THEN 3
                    WHEN t.tgrelid=k.confrelid AND t.tgconstrrelid=k.conrelid
                        AND t.tgtype=17 AND t.tgfoid='pg_catalog."RI_FKey_noaction_upd"()'::regprocedure THEN 4
                    ELSE 0
                END AS enforcement,
                t.tgenabled='O' AND t.tgisinternal
                    AND NOT t.tgdeferrable AND NOT t.tginitdeferred
                    AND t.tgconstrindid=k.conindid AND t.tgparentid=0
                    AND t.tgnargs=0 AND octet_length(t.tgargs)=0 AND t.tgattr::text=''
                    AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AS ordinary
            FROM pg_constraint k
            JOIN pg_class c ON c.oid=k.conrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            JOIN pg_trigger t ON t.tgconstraint=k.oid
            WHERE n.nspname=current_schema() AND c.relname=wanted.table_name AND k.contype='f'
            LIMIT 8) checks"#,
        ))
        .await
        .map_err(|_| ManagementError::Schema)?;
    let groups = grouped(
        rows,
        &["pin_management_locator", "pin_management_archive_chunks"],
        8,
    )?;
    for rows in groups.values() {
        if rows.len() != 4 {
            return Err(ManagementError::Schema);
        }
        let mut roles = BTreeSet::new();
        for row in rows {
            let role: i32 = row
                .try_get("", "enforcement")
                .map_err(|_| ManagementError::Schema)?;
            if !row
                .try_get::<bool>("", "ordinary")
                .map_err(|_| ManagementError::Schema)?
                || !(1..=4).contains(&role)
                || !roles.insert(role)
            {
                return Err(ManagementError::Schema);
            }
        }
    }
    Ok(())
}

async fn defaults<C: ConnectionTrait>(db: &C, names: &[&str]) -> Result<()> {
    let rows = db.query_all(Statement::from_string(DatabaseBackend::Postgres, format!(
        "SELECT wanted.table_name, defaults.* FROM {TABLES} CROSS JOIN LATERAL (\
         SELECT a.attname::text AS name, CASE WHEN octet_length(pg_get_expr(d.adbin,d.adrelid,false))<={SQL_BYTES} THEN pg_get_expr(d.adbin,d.adrelid,false) ELSE NULL END AS definition, a.attidentity='' AND a.attgenerated='' AS ordinary \
         FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum \
         WHERE n.nspname=current_schema() AND c.relname=wanted.table_name AND a.attnum>0 AND NOT a.attisdropped AND (a.atthasdef OR a.attidentity<>'') ORDER BY a.attname LIMIT 32) defaults ORDER BY wanted.table_name,defaults.name")))
        .await.map_err(|_| ManagementError::Schema)?;
    let groups = grouped(rows, names, 32)?;
    for table in names.iter().copied() {
        let expected: &[&str] = match table {
            "pin_management_source_control" => &["locator_generation", "source_generation"],
            "pin_management_archive" => &["bytes", "chunks"],
            _ => &[],
        };
        let rows = groups.get(table).ok_or(ManagementError::Schema)?;
        if rows.len() != expected.len() {
            return Err(ManagementError::Schema);
        }
        let mut seen = BTreeSet::new();
        for (row, name) in rows.iter().zip(expected) {
            if !seen.insert(
                row.try_get::<String>("", "name")
                    .map_err(|_| ManagementError::Schema)?,
            ) {
                return Err(ManagementError::Schema);
            }
            if row
                .try_get::<String>("", "name")
                .map_err(|_| ManagementError::Schema)?
                != *name
                || row
                    .try_get::<String>("", "definition")
                    .map_err(|_| ManagementError::Schema)?
                    != "0"
                || !row
                    .try_get::<bool>("", "ordinary")
                    .map_err(|_| ManagementError::Schema)?
            {
                return Err(ManagementError::Schema);
            }
        }
    }
    Ok(())
}
