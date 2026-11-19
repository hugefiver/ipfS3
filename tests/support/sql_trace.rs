use std::sync::{Arc, Mutex};

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
        let structural: Vec<_> = statements
            .iter()
            .filter(|sql| {
                let sql = sql.trim_start().to_ascii_lowercase();
                if !sql.starts_with("select ")
                    && !sql.starts_with("with ")
                    && !sql.starts_with("pragma ")
                {
                    return false;
                }
                [
                    "seaql_migrations",
                    "sqlite_schema",
                    "sqlite_master",
                    "sqlite_temp",
                    "pragma_table",
                    "pragma_index",
                    "pragma_foreign_key",
                    "pg_catalog.",
                    "pg_class",
                    "pg_namespace",
                    "information_schema",
                ]
                .iter()
                .any(|needle| sql.contains(needle))
            })
            .map(|sql| sql.chars().take(180).collect::<String>())
            .collect();
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
