use std::sync::{Arc, Mutex};

/// Records actual SeaORM execution, including nested generic receipt readers.
#[derive(Clone, Default)]
pub struct SqlTrace(Arc<Mutex<Vec<String>>>);

impl SqlTrace {
    pub fn attach(&self, db: &mut sea_orm::DatabaseConnection) {
        let statements = self.0.clone();
        db.set_metric_callback(move |info| {
            statements.lock().unwrap().push(info.statement.sql.clone());
        });
    }

    pub fn take(&self, phase: &str, warm: bool) -> (usize, usize) {
        let statements = std::mem::take(&mut *self.0.lock().unwrap());
        let structural: Vec<_> = statements.iter().filter(|sql| {
            let sql = sql.trim_start().to_ascii_lowercase();
            if !sql.starts_with("select ") && !sql.starts_with("with ") && !sql.starts_with("pragma ") {
                return false;
            }
            // This permanent fact-relation binding is not a layout scan. The
            // response transaction independently checks its captured OID/schema.
            if sql.starts_with("select n.nspname::text as schema_name, c.oid::bigint as relation_oid")
                && sql.contains("c.relname='pin_submit_observations'") {
                return false;
            }
            // Fresh physical database/scope binding and its privilege check are
            // business authority inputs, not table/constraint layout scans.
            if sql.starts_with("select case when inet_server_addr() is null or inet_server_port() is null")
                || sql.starts_with("select has_function_privilege('pg_catalog.pg_control_system()','execute')")
                || sql.starts_with("select system_identifier::text as identity from pg_catalog.pg_control_system()") {
                return false;
            }
            ["seaql_migrations", "pin_management_readiness", "sqlite_schema", "sqlite_master", "sqlite_temp", "pragma_table", "pragma_index", "pragma_foreign_key", "pg_catalog.", "pg_class", "pg_namespace", "information_schema"].iter().any(|needle| sql.contains(needle))
        }).map(|sql| sql.chars().take(180).collect::<String>()).collect();
        eprintln!(
            "{phase}: observed SQL={}, structural={}",
            statements.len(),
            structural.len()
        );
        assert!(
            !statements.is_empty(),
            "SQL observer must capture actual work"
        );
        if warm {
            assert!(structural.is_empty(), "{phase}: {structural:#?}");
        } else {
            assert!(!structural.is_empty(), "cold validation must be observed");
        }
        (statements.len(), structural.len())
    }
}
