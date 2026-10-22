use super::{RegularIndex, expected, invalid};
use crate::error::AppResult;
use sea_orm::{DatabaseBackend, QueryResult, Statement, Value};
use std::sync::OnceLock;

// Separate slots keep each active value's original String/i32/Option<String>
// decoding semantics, even when another branch supplies NULL in that slot.
pub(super) const SLOTS: &[&str] = &[
    "table_sql",
    "column_cid",
    "column_name",
    "column_type",
    "column_notnull",
    "column_default",
    "column_pk",
    "column_hidden",
    "index_name",
    "index_unique",
    "index_origin",
    "index_partial",
    "fk_table",
    "fk_from",
    "fk_to",
    "fk_update",
    "fk_delete",
    "fk_match",
    "trigger_name",
    "trigger_sql",
    "special_sql",
];

fn branch(kind: &str, owner: &str, source: &str, fields: &[(&str, &str)]) -> String {
    let mut projection = format!("SELECT '{kind}' AS kind,{owner} AS owner");
    for slot in SLOTS {
        let value = fields
            .iter()
            .find_map(|(name, value)| (name == slot).then_some(*value))
            .unwrap_or("NULL");
        projection.push_str(&format!(",{value} AS {slot}"));
    }
    format!("{projection} FROM ({source})")
}

pub(super) fn metadata_statement(backend: DatabaseBackend) -> Statement {
    static TEMPLATE: OnceLock<(String, Vec<Value>)> = OnceLock::new();
    let (sql, values) = TEMPLATE.get_or_init(|| {
        let mut branches = Vec::new();
        let mut values = vec![expected::OPERATIONS.into(), expected::ITEMS.into()];
        for index in 0..2 {
            let owner = format!("${}", index + 1);
            branches.push(branch(
                "table",
                &owner,
                &format!("SELECT sql FROM sqlite_schema WHERE type='table' AND name={owner} LIMIT 2"),
                &[("table_sql", "sql")],
            ));
            branches.push(branch("column", &owner,
                &format!("SELECT cid,name,type,\"notnull\",dflt_value,pk,hidden FROM pragma_table_xinfo({owner}) ORDER BY cid LIMIT 12"),
                &[("column_cid", "cid"), ("column_name", "name"), ("column_type", "type"),
                  ("column_notnull", "\"notnull\""), ("column_default", "dflt_value"),
                  ("column_pk", "pk"), ("column_hidden", "hidden")]));
            branches.push(branch(
                "index",
                &owner,
                &format!(
                    "SELECT name,\"unique\",origin,partial FROM pragma_index_list({owner}) LIMIT 4"
                ),
                &[
                    ("index_name", "name"),
                    ("index_unique", "\"unique\""),
                    ("index_origin", "origin"),
                    ("index_partial", "partial"),
                ],
            ));
            branches.push(branch("fk", &owner,
                &format!("SELECT \"table\",\"from\",\"to\",on_update,on_delete,\"match\" FROM pragma_foreign_key_list({owner}) LIMIT 2"),
                &[("fk_table", "\"table\""), ("fk_from", "\"from\""), ("fk_to", "\"to\""),
                  ("fk_update", "on_update"), ("fk_delete", "on_delete"), ("fk_match", "\"match\"")]));
        }
        for (index, table) in [expected::OPERATIONS, expected::ITEMS]
            .into_iter()
            .enumerate()
        {
            let owner = format!("${}", index + 1);
            let mut names = vec![format!("{table}_no_update"), format!("{table}_no_delete")];
            if table == expected::ITEMS {
                names.push("pin_legacy_item_bound".into());
            }
            for name in names {
                values.push(name.into());
                branches.push(branch("trigger", &owner,
                    &format!("SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND name=${} AND tbl_name={owner} LIMIT 2", values.len()),
                    &[("trigger_name", "name"), ("trigger_sql", "sql")]));
            }
        }
        values.push("s75_legacy_reverses".into());
        branches.push(branch(
            "special",
            "$1",
            &format!(
                "SELECT sql FROM sqlite_schema WHERE type='index' AND tbl_name=$1 AND name=${} LIMIT 2",
                values.len()
            ),
            &[("special_sql", "sql")],
        ));
        (branches.join(" UNION ALL "), values)
    });
    Statement::from_sql_and_values(backend, sql.clone(), values.clone())
}

pub(super) fn keys_statement(backend: DatabaseBackend, indexes: &[RegularIndex]) -> Statement {
    let mut values = Vec::new();
    let mut branches = Vec::new();
    for index in indexes {
        values.push(index.table.into());
        let owner = values.len();
        values.push(index.name.clone().into());
        let name = values.len();
        branches.push(format!(
            "SELECT 'key' AS kind,${owner} AS owner,${name} AS index_name,seqno AS key_seqno,name AS key_name FROM (SELECT seqno,name FROM pragma_index_info(${name}) ORDER BY seqno LIMIT 4)"
        ));
    }
    Statement::from_sql_and_values(backend, branches.join(" UNION ALL "), values)
}

#[derive(Default)]
pub(super) struct Table {
    pub ddl: Vec<String>,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub foreign_keys: Vec<[String; 6]>,
    pub triggers: Vec<(String, String)>,
    pub special: Vec<String>,
}

pub(super) struct Column {
    pub cid: i32,
    pub name: String,
    pub ty: String,
    pub nonnull: i32,
    pub default: Option<String>,
    pub pk: i32,
    pub hidden: i32,
}

pub(super) struct Index {
    pub name: String,
    pub unique: i32,
    pub origin: String,
    pub partial: i32,
}

impl Table {
    pub(super) fn push(&mut self, row: &QueryResult) -> AppResult<()> {
        let kind: String = row.try_get("", "kind")?;
        match kind.as_str() {
            "table" => self.ddl.push(row.try_get("", "table_sql")?),
            "column" => self.columns.push(Column {
                cid: row.try_get("", "column_cid")?,
                name: row.try_get("", "column_name")?,
                ty: row.try_get("", "column_type")?,
                nonnull: row.try_get("", "column_notnull")?,
                default: row.try_get("", "column_default")?,
                pk: row.try_get("", "column_pk")?,
                hidden: row.try_get("", "column_hidden")?,
            }),
            "index" => self.indexes.push(Index {
                name: row.try_get("", "index_name")?,
                unique: row.try_get("", "index_unique")?,
                origin: row.try_get("", "index_origin")?,
                partial: row.try_get("", "index_partial")?,
            }),
            "fk" => self.foreign_keys.push([
                row.try_get("", "fk_table")?,
                row.try_get("", "fk_from")?,
                row.try_get("", "fk_to")?,
                row.try_get("", "fk_update")?,
                row.try_get("", "fk_delete")?,
                row.try_get("", "fk_match")?,
            ]),
            "trigger" => self.triggers.push((
                row.try_get("", "trigger_name")?,
                row.try_get("", "trigger_sql")?,
            )),
            "special" => self.special.push(row.try_get("", "special_sql")?),
            _ => return Err(invalid()),
        }
        Ok(())
    }
}
