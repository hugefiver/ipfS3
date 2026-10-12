use super::*;
use std::collections::BTreeSet;

fn compact(sql: &str) -> String { sql.chars().filter(|c| !c.is_whitespace()).collect() }

fn definitions(table: &str) -> BTreeSet<String> {
    let definitions = if table == RECORDS {
        vec![
            "PRIMARY KEY (database_scope, plan_id, item_id, kind)",
            "CHECK ((kind = ANY (ARRAY['database_scope'::text, 'evidence_preparation'::text, 'action'::text])))",
            "CHECK ((state = ANY (ARRAY['identity'::text, 'prepared'::text, 'applied'::text, 'stopped'::text, 'needs_attention'::text])))",
            "CHECK (((length(database_scope) > 0) AND (length(plan_id) > 0) AND (length(item_id) > 0)))",
            "CHECK ((((kind = 'database_scope'::text) AND (state = 'identity'::text) AND (principal = ''::text) AND (expires_at IS NULL) AND (completed_at IS NULL) AND (result IS NULL) AND (plan_id = 'schema'::text) AND (item_id = 'identity'::text)) OR ((kind <> 'database_scope'::text) AND (state <> 'identity'::text) AND (length(principal) > 0) AND (length(native_identity) > 0) AND (expires_at IS NOT NULL))))",
            "CHECK ((((kind = 'action'::text) AND (state = 'prepared'::text) AND (result IS NULL) AND (completed_at IS NULL)) OR ((kind = 'evidence_preparation'::text) AND (state = 'prepared'::text) AND (result IS NOT NULL) AND (completed_at IS NOT NULL)) OR ((kind = 'action'::text) AND (state = 'applied'::text) AND (result IS NOT NULL) AND (completed_at IS NOT NULL)) OR (state = ANY (ARRAY['identity'::text, 'stopped'::text, 'needs_attention'::text]))))",
        ]
    } else {
        vec![
            "PRIMARY KEY (allocation_domain)",
            "CHECK (((length(allocation_domain) > 0) AND (length(database_scope) > 0) AND (length(principal) > 0)))",
            "CHECK ((disposition = ANY (ARRAY['retain'::text, 'migrate'::text, 'controlled_cleanup'::text])))",
            "CHECK ((state = ANY (ARRAY['retired'::text, 'needs_attention'::text])))",
            "CHECK ((record_kind = 'action'::text))",
            "FOREIGN KEY (database_scope, plan_id, item_id, record_kind) REFERENCES pin_management_records(database_scope, plan_id, item_id, kind)",
        ]
    };
    definitions.into_iter().map(compact).collect()
}

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    for table in [RECORDS, RETIREMENTS] {
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT c.relkind::text AS kind,c.relpersistence::text AS persistence,c.relrowsecurity,c.relforcerowsecurity,c.relispartition,c.relhassubclass FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1", [table.into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
        if row.try_get::<String>("","kind").map_err(|_| ManagementError::Schema)? != "r" || row.try_get::<String>("","persistence").map_err(|_| ManagementError::Schema)? != "p" { return Err(ManagementError::Schema); }
        for flag in ["relrowsecurity","relforcerowsecurity","relispartition","relhassubclass"] { if row.try_get::<bool>("",flag).map_err(|_| ManagementError::Schema)? { return Err(ManagementError::Schema); } }
        columns(db,table).await?; constraints(db,table).await?;
        let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
            "SELECT COUNT(*) AS count FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND NOT t.tgisinternal", [table.into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
        if row.try_get::<i64>("","count").map_err(|_| ManagementError::Schema)? != 0 { return Err(ManagementError::Schema); }
    }
    Ok(())
}

async fn columns<C: ConnectionTrait>(db: &C, table: &str) -> Result<()> {
    let expected: Vec<(&str, &str, bool)> = if table == RECORDS {
        vec![("database_scope","text",true),("plan_id","text",true),("item_id","text",true),("kind","text",true),("principal","text",true),("native_identity","text",true),("binding","text",true),("native_before","text",true),("result","text",false),("intent_ids","text",true),("job_ids","text",true),("state","text",true),("created_at","timestamp with time zone",true),("expires_at","timestamp with time zone",false),("completed_at","timestamp with time zone",false)]
    } else {
        vec![("allocation_domain","text",true),("database_scope","text",true),("plan_id","text",true),("item_id","text",true),("principal","text",true),("native_identity","text",true),("binding","text",true),("inventory","text",true),("disposition","text",true),("unresolved","text",true),("state","text",true),("created_at","timestamp with time zone",true),("record_kind","text",true)]
    };
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT a.attname::text AS name,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull,a.attisdropped,a.attidentity::text AS identity,a.attgenerated::text AS generated,pg_get_expr(d.adbin,d.adrelid) AS default_expr,CASE WHEN a.atttypid='text'::regtype THEN a.attcollation='pg_catalog.\"default\"'::regcollation ELSE a.attcollation=0 END AS known_collation FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relname=$1 AND a.attnum>0 ORDER BY a.attnum LIMIT 32", [table.into()])).await.map_err(|_| ManagementError::Schema)?;
    if rows.len() != expected.len() { return Err(ManagementError::Schema); }
    for (row,(name,ty,nonnull)) in rows.iter().zip(expected) {
        if row.try_get::<String>("","name").map_err(|_| ManagementError::Schema)? != name || row.try_get::<String>("","type").map_err(|_| ManagementError::Schema)? != ty || row.try_get::<bool>("","attnotnull").map_err(|_| ManagementError::Schema)? != nonnull || row.try_get::<bool>("","attisdropped").map_err(|_| ManagementError::Schema)? || !row.try_get::<bool>("","known_collation").map_err(|_| ManagementError::Schema)? || !row.try_get::<String>("","identity").map_err(|_| ManagementError::Schema)?.is_empty() || !row.try_get::<String>("","generated").map_err(|_| ManagementError::Schema)?.is_empty() { return Err(ManagementError::Schema); }
        let default: Option<String> = row.try_get("","default_expr").map_err(|_| ManagementError::Schema)?;
        if default.as_deref() != if name == "record_kind" { Some("'action'::text") } else { None } { return Err(ManagementError::Schema); }
    }
    Ok(())
}

async fn constraints<C: ConnectionTrait>(db: &C, table: &str) -> Result<()> {
    let rows = db.query_all(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT k.contype::text AS kind,pg_get_constraintdef(k.oid,false) AS definition,k.convalidated,k.condeferrable,k.condeferred,k.conislocal,k.connoinherit,CASE WHEN k.contype='f' THEN rn.nspname=current_schema() ELSE true END AS local_reference FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_class rc ON rc.oid=k.confrelid LEFT JOIN pg_namespace rn ON rn.oid=rc.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 LIMIT 16",[table.into()])).await.map_err(|_| ManagementError::Schema)?;
    let expected = definitions(table); if rows.len() != expected.len() { return Err(ManagementError::Schema); }
    let mut actual = BTreeSet::new();
    for row in rows {
        for flag in ["convalidated","conislocal","local_reference"] { if !row.try_get::<bool>("",flag).map_err(|_| ManagementError::Schema)? { return Err(ManagementError::Schema); } }
        for flag in ["condeferrable","condeferred"] { if row.try_get::<bool>("",flag).map_err(|_| ManagementError::Schema)? { return Err(ManagementError::Schema); } }
        if row.try_get::<String>("","kind").map_err(|_| ManagementError::Schema)? == "c" && row.try_get::<bool>("","connoinherit").map_err(|_| ManagementError::Schema)? { return Err(ManagementError::Schema); }
        actual.insert(compact(&row.try_get::<String>("","definition").map_err(|_| ManagementError::Schema)?));
    }
    if actual != expected { return Err(ManagementError::Schema); }
    let row = db.query_one(Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT COUNT(*) AS count,COALESCE(bool_and(i.indisvalid AND i.indisready AND i.indislive AND i.indpred IS NULL AND i.indexprs IS NULL),false) AS valid FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND (i.indisunique OR i.indisprimary)",[table.into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
    if row.try_get::<i64>("","count").map_err(|_| ManagementError::Schema)? != 1 || !row.try_get::<bool>("","valid").map_err(|_| ManagementError::Schema)? { return Err(ManagementError::Schema); }
    Ok(())
}
