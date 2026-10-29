//! Native catalogs prove receipt integrity without creating probe tables or rows.
use super::invalid;
use crate::{error::AppResult, store::migrations::legacy_receipts::schema as expected};
use sea_orm::{ConnectionTrait, Statement};
use std::collections::BTreeSet;

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> AppResult<()> {
    for table in [expected::OPERATIONS, expected::ITEMS] {
        let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.relhassubclass FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1",[table.into()])).await?.ok_or_else(invalid)?;
        if row.try_get::<String>("", "kind")? != "r"
            || row.try_get::<String>("", "persistence")? != "p"
        {
            return Err(invalid());
        }
        for flag in [
            "relrowsecurity",
            "relforcerowsecurity",
            "relispartition",
            "relhassubclass",
        ] {
            if row.try_get::<bool>("", flag)? {
                return Err(invalid());
            }
        }
        columns(db, table).await?;
        constraints(db, table).await?;
    }
    for table in [expected::OPERATIONS, expected::ITEMS] {
        trigger(
            db,
            table,
            &format!("{table}_immutable"),
            "reject_legacy_migration_mutation",
            27,
            expected::PG_IMMUTABLE_BODY,
        )
        .await?;
    }
    trigger(
        db,
        expected::ITEMS,
        "pin_legacy_item_bound",
        "bound_legacy_migration_item",
        7,
        expected::PG_BOUND_BODY,
    )
    .await
}

async fn columns<C: ConnectionTrait>(db: &C, table: &str) -> AppResult<()> {
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT a.attname::text AS name,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,a.attisdropped,a.attidentity::text AS identity,a.attgenerated::text AS generated,pg_get_expr(d.adbin,d.adrelid) AS default_expr,CASE WHEN a.atttypid='text'::regtype THEN a.attcollation='pg_catalog.\"default\"'::regcollation ELSE a.attcollation=0 END AS known_collation FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relname=$1 AND a.attnum>0 ORDER BY a.attnum LIMIT 12",[table.into()])).await?;
    let expected = expected::columns(table);
    if rows.len() != expected.len() {
        return Err(invalid());
    }
    for (row, (name, ty, nonnull, _)) in rows.iter().zip(expected) {
        let ty = if ty == "TEXT" { "text" } else { "integer" };
        if row.try_get::<String>("", "name")? != name
            || row.try_get::<String>("", "type")? != ty
            || row.try_get::<bool>("", "attnotnull")? != nonnull
            || row.try_get::<bool>("", "attisdropped")?
            || !row.try_get::<bool>("", "known_collation")?
            || !row.try_get::<String>("", "identity")?.is_empty()
            || !row.try_get::<String>("", "generated")?.is_empty()
            || row.try_get::<Option<String>>("", "default_expr")?.is_some()
        {
            return Err(invalid());
        }
    }
    Ok(())
}

fn compact(sql: &str) -> String {
    sql.chars().filter(|c| !c.is_whitespace()).collect()
}

fn definitions(table: &str) -> BTreeSet<String> {
    let definitions = if table == expected::OPERATIONS {
        vec![
            "PRIMARY KEY (id)",
            "UNIQUE (reverses)",
            "FOREIGN KEY (reverses) REFERENCES pin_legacy_migrations(id) ON DELETE RESTRICT",
            "CHECK ((direction = ANY (ARRAY['apply'::text, 'rollback'::text])))",
            "CHECK (((item_count > 0) AND (item_count <= 10100)))",
            "CHECK ((((direction = 'apply'::text) AND (reverses IS NULL)) OR ((direction = 'rollback'::text) AND (reverses IS NOT NULL))))",
        ]
    } else {
        vec![
            "PRIMARY KEY (operation_id, ordinal)",
            "UNIQUE (operation_id, table_kind, original_key)",
            "FOREIGN KEY (operation_id) REFERENCES pin_legacy_migrations(id) ON DELETE RESTRICT",
            "CHECK ((ordinal >= 0))",
        ]
    };
    definitions.into_iter().map(compact).collect()
}

async fn constraints<C: ConnectionTrait>(db: &C, table: &str) -> AppResult<()> {
    let rows=db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT k.contype::text AS kind,pg_get_constraintdef(k.oid,false) AS definition,k.convalidated,k.condeferrable,k.condeferred,k.conislocal,k.connoinherit,CASE WHEN k.contype='f' THEN rn.nspname=current_schema() ELSE true END AS local_reference FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_class rc ON rc.oid=k.confrelid LEFT JOIN pg_namespace rn ON rn.oid=rc.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 LIMIT 8",[table.into()])).await?;
    let expected = definitions(table);
    if rows.len() != expected.len() {
        return Err(invalid());
    }
    let mut actual = BTreeSet::new();
    for row in rows {
        for flag in ["convalidated", "conislocal", "local_reference"] {
            if !row.try_get::<bool>("", flag)? {
                return Err(invalid());
            }
        }
        for flag in ["condeferrable", "condeferred"] {
            if row.try_get::<bool>("", flag)? {
                return Err(invalid());
            }
        }
        // NO INHERIT is a CHECK property. PK/UNIQUE/FK are already non-inherited
        // by SQL semantics; their internal catalog flag is not this CHECK fence.
        if row.try_get::<String>("", "kind")? == "c" && row.try_get::<bool>("", "connoinherit")? {
            return Err(invalid());
        }
        actual.insert(compact(&row.try_get::<String>("", "definition")?));
    }
    if actual != expected {
        return Err(invalid());
    }
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT COUNT(*) AS count,COALESCE(bool_and(i.indisvalid AND i.indisready AND i.indislive AND i.indpred IS NULL AND i.indexprs IS NULL),false) AS valid FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND (i.indisunique OR i.indisprimary)",[table.into()])).await?.ok_or_else(invalid)?;
    if row.try_get::<i64>("", "count")? != 2 || !row.try_get::<bool>("", "valid")? {
        return Err(invalid());
    }
    Ok(())
}

async fn trigger<C: ConnectionTrait>(
    db: &C,
    table: &str,
    name: &str,
    function: &str,
    flags: i32,
    body: &str,
) -> AppResult<()> {
    let row=db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT t.tgtype::integer AS flags,t.tgenabled::text AS enabled,t.tgattr::text AS columns,encode(t.tgargs,'hex') AS args,t.tgqual IS NULL AS unconditional,p.proname::text AS function,p.prosrc,p.prosecdef,p.proconfig IS NULL AS default_config,p.pronargs=0 AS no_args,p.prorettype='trigger'::regtype AS returns_trigger,p.prokind='f' AS ordinary_function,l.lanname='plpgsql' AS plpgsql,pn.nspname=current_schema() AS local_function FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=current_schema() AND c.relname=$1 AND t.tgname=$2 AND NOT t.tgisinternal",[table.into(),name.into()])).await?.ok_or_else(invalid)?;
    if row.try_get::<i32>("", "flags")? != flags
        || row.try_get::<String>("", "enabled")? != "O"
        || !row.try_get::<String>("", "columns")?.is_empty()
        || !row.try_get::<String>("", "args")?.is_empty()
        || row.try_get::<String>("", "function")? != function
        || row.try_get::<String>("", "prosrc")?.trim() != body
        || row.try_get::<bool>("", "prosecdef")?
    {
        return Err(invalid());
    }
    for flag in [
        "unconditional",
        "default_config",
        "no_args",
        "returns_trigger",
        "ordinary_function",
        "plpgsql",
        "local_function",
    ] {
        if !row.try_get::<bool>("", flag)? {
            return Err(invalid());
        }
    }
    Ok(())
}
