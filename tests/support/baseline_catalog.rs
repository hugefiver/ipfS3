use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use std::collections::BTreeMap;

// Compare catalog expressions, not CREATE statement spelling or physical OIDs.
fn expression(sql: &str) -> String {
    let mut quoted = false;
    sql.chars()
        .filter_map(|c| {
            if c == '\'' {
                quoted = !quoted;
            }
            if !quoted && (c.is_whitespace() || c == '"') {
                None
            } else if quoted {
                Some(c)
            } else {
                Some(c.to_ascii_lowercase())
            }
        })
        .collect()
}

fn checks(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut at = 0;
    let mut quoted = false;
    let mut result = Vec::new();
    while at < bytes.len() {
        if bytes[at] == b'\'' {
            quoted = !quoted;
        }
        if !quoted
            && (at == 0 || !(bytes[at - 1].is_ascii_alphanumeric() || bytes[at - 1] == b'_'))
            && sql
                .get(at..at + 5)
                .is_some_and(|s| s.eq_ignore_ascii_case("check"))
            && bytes
                .get(at + 5)
                .is_some_and(|c| c.is_ascii_whitespace() || *c == b'(')
        {
            at += 5;
            while bytes[at].is_ascii_whitespace() {
                at += 1;
            }
            assert_eq!(bytes[at], b'(');
            let start = at;
            let mut depth = 0;
            loop {
                if bytes[at] == b'\'' {
                    quoted = !quoted;
                }
                if !quoted {
                    if bytes[at] == b'(' {
                        depth += 1;
                    }
                    if bytes[at] == b')' {
                        depth -= 1;
                    }
                }
                at += 1;
                if depth == 0 {
                    break;
                }
            }
            let normalized = expression(&sql[start..at]);
            let mut inner = normalized.as_str();
            while inner.starts_with('(') && inner.ends_with(')') {
                let mut nesting = 0;
                let mut literal = false;
                let wraps_all = inner.char_indices().all(|(i, c)| {
                    if c == '\'' {
                        literal = !literal;
                    }
                    if !literal {
                        if c == '(' {
                            nesting += 1;
                        }
                        if c == ')' {
                            nesting -= 1;
                        }
                    }
                    nesting != 0 || i == inner.len() - 1
                });
                if !wraps_all {
                    break;
                }
                inner = &inner[1..inner.len() - 1];
            }
            result.push(inner.to_owned());
        } else {
            at += 1;
        }
    }
    result.sort();
    result
}

pub async fn sqlite(db: &DatabaseConnection) -> BTreeMap<String, Vec<String>> {
    let rows = db.query_all(Statement::from_string(db.get_database_backend(), "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE type IN ('table','index') AND sql IS NOT NULL AND tbl_name NOT IN ('seaql_migrations','sqlite_sequence','_ipfs_s3_migration_lock') ORDER BY name")).await.unwrap();
    let mut result = BTreeMap::new();
    for row in rows {
        let kind: String = row.try_get("", "type").unwrap();
        let name: String = row.try_get("", "name").unwrap();
        let sql: String = row.try_get("", "sql").unwrap();
        if kind == "table" {
            result.insert(format!("checks:{name}"), checks(&sql));
        } else {
            // Preserve index expressions and WHERE predicates in addition to PRAGMA metadata.
            let start = sql.find('(').unwrap();
            result.insert(format!("index:{name}"), vec![expression(&sql[start..])]);
        }
    }
    for (kind, sql) in [
        (
            "columns",
            "SELECT json_array(s.name,p.name,upper(p.type),p.\"notnull\",p.dflt_value,p.pk) AS value FROM sqlite_schema s JOIN pragma_table_info(s.name) p WHERE s.type='table' AND s.name NOT IN ('sqlite_sequence','seaql_migrations','_ipfs_s3_migration_lock') ORDER BY value",
        ),
        (
            "foreign_keys",
            "SELECT json_array(s.name,f.seq,f.\"table\",f.\"from\",f.\"to\",f.on_update,f.on_delete,f.match) AS value FROM sqlite_schema s JOIN pragma_foreign_key_list(s.name) f WHERE s.type='table' ORDER BY value",
        ),
        (
            "indexes",
            "SELECT json_array(s.name,i.\"unique\",i.origin,i.partial,(SELECT json_group_array(json_array(x.name,x.desc,x.coll)) FROM (SELECT * FROM pragma_index_xinfo(i.name) WHERE key=1 ORDER BY seqno) x)) AS value FROM sqlite_schema s JOIN pragma_index_list(s.name) i WHERE s.type='table' AND s.name NOT IN ('seaql_migrations','_ipfs_s3_migration_lock') ORDER BY value",
        ),
        (
            "triggers",
            "SELECT json_array(tbl_name,name) AS value FROM sqlite_schema WHERE type='trigger' ORDER BY value",
        ),
    ] {
        let rows = db
            .query_all(Statement::from_string(db.get_database_backend(), sql))
            .await
            .unwrap();
        result.insert(
            kind.into(),
            rows.into_iter()
                .map(|row| row.try_get("", "value").unwrap())
                .collect(),
        );
    }
    result
}

pub async fn postgres(db: &DatabaseConnection) -> BTreeMap<String, Vec<String>> {
    let mut result = BTreeMap::new();
    for (kind, sql) in [
        (
            "columns",
            "SELECT jsonb_build_array(c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid))::text AS value FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relkind='r' AND c.relname<>'seaql_migrations' AND a.attnum>0 AND NOT a.attisdropped ORDER BY value",
        ),
        (
            "constraints",
            "SELECT jsonb_build_array(c.relname,k.contype,k.condeferrable,k.condeferred,k.convalidated,k.confupdtype,k.confdeltype,k.confmatchtype,k.connoinherit,(SELECT array_agg(a.attname ORDER BY u.ordinal) FROM unnest(k.conkey) WITH ORDINALITY u(num,ordinal) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=u.num),p.relname,(SELECT array_agg(a.attname ORDER BY u.ordinal) FROM unnest(k.confkey) WITH ORDINALITY u(num,ordinal) JOIN pg_attribute a ON a.attrelid=p.oid AND a.attnum=u.num),pg_get_expr(k.conbin,k.conrelid))::text AS value FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_class p ON p.oid=k.confrelid WHERE n.nspname=current_schema() AND c.relname<>'seaql_migrations' ORDER BY value",
        ),
        (
            "indexes",
            "SELECT jsonb_build_array(t.relname,i.indisunique,i.indisprimary,i.indisvalid,i.indisready,am.amname,(SELECT array_agg(pg_get_indexdef(i.indexrelid,s,true) ORDER BY s) FROM generate_series(1,i.indnkeyatts) s),pg_get_expr(i.indpred,i.indrelid))::text AS value FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid JOIN pg_class x ON x.oid=i.indexrelid JOIN pg_am am ON am.oid=x.relam JOIN pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=current_schema() AND t.relname<>'seaql_migrations' ORDER BY value",
        ),
        (
            "triggers",
            "SELECT jsonb_build_array(c.relname,t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,p.proname,encode(t.tgargs,'hex'),pg_get_expr(t.tgqual,t.tgrelid),(SELECT array_agg(a.attname ORDER BY a.attname) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum=ANY(t.tgattr)))::text AS value FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid WHERE n.nspname=current_schema() AND NOT t.tgisinternal ORDER BY value",
        ),
        (
            "functions",
            "SELECT jsonb_build_array(p.proname,pg_get_function_identity_arguments(p.oid),pg_get_function_result(p.oid),l.lanname,p.provolatile,p.proisstrict,p.prosecdef,p.proleakproof,p.proparallel,p.proconfig)::text AS value FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=current_schema() ORDER BY value",
        ),
        (
            "sequences",
            "SELECT jsonb_build_array(s.relname,format_type(q.seqtypid,NULL),q.seqstart,q.seqincrement,q.seqmin,q.seqmax,q.seqcache,q.seqcycle,c.relname,a.attname,d.deptype)::text AS value FROM pg_sequence q JOIN pg_class s ON s.oid=q.seqrelid JOIN pg_namespace n ON n.oid=s.relnamespace LEFT JOIN pg_depend d ON d.objid=s.oid AND d.classid='pg_class'::regclass AND d.deptype IN ('a','i') LEFT JOIN pg_class c ON c.oid=d.refobjid LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=d.refobjsubid WHERE n.nspname=current_schema() ORDER BY value",
        ),
    ] {
        let rows = db
            .query_all(Statement::from_string(db.get_database_backend(), sql))
            .await
            .unwrap();
        result.insert(
            kind.into(),
            rows.into_iter()
                .map(|r| r.try_get("", "value").unwrap())
                .collect(),
        );
    }
    result
}
