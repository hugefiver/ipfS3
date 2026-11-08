use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::prelude::*;

pub const VERSION: &str = "m20261108_000001_current_pinning_config";
pub struct Migration;
impl MigrationName for Migration {
    fn name(&self) -> &str {
        VERSION
    }
}
#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        install(manager.get_connection()).await
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, _: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "current pinning config requires authorized forward repair".into(),
        ))
    }
}

const TABLES: &[&str] = &[
    "pin_extension_decisions",
    "pin_lease_targets",
    "pin_jobs",
    "zip_v2_mpu_intakes",
    "zip_v2_mpu_completions",
];

fn original_definition(table: &str, pg: bool) -> String {
    let source = if table.starts_with("zip_") {
        include_str!("baseline/zip.sql")
    } else {
        include_str!("baseline/pinning.sql")
    };
    source
        .lines()
        .find(|line| line.starts_with(&format!("CREATE TABLE {table} (")))
        .expect("published baseline table")
        .replace("@TIME@", if pg { "TIMESTAMPTZ" } else { "TIMESTAMP" })
        .replace(
            "@TZ@",
            if pg {
                "TIMESTAMPTZ"
            } else {
                "timestamp_with_timezone_text"
            },
        )
}

fn definition(table: &str, pg: bool) -> String {
    original_definition(table, pg)
        .replace(", config_revision TEXT NOT NULL", "")
        .replace(
            ", CONSTRAINT ck_pin_extension_decisions_revision CHECK(length(config_revision)=64)",
            "",
        )
        .replace(", rule_revision TEXT NOT NULL", "")
        .replace(" AND length(rule_revision)>0", "")
        .replace(
            "'pending','running','done'",
            "'pending','running','done','error'",
        )
        .replace("'evicted','released'", "'evicted','released','error'")
        .replace(
            "provider TEXT NOT NULL, state TEXT NOT NULL, created_at",
            "provider TEXT NOT NULL, state TEXT NOT NULL, last_error TEXT, created_at",
        )
}

/// Caller owns the maintenance/migrator transaction, including the migration ledger.
pub(crate) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    match db.get_database_backend() {
        DatabaseBackend::Sqlite => sqlite(db).await,
        DatabaseBackend::Postgres => postgres(db).await,
        _ => Err(DbErr::Migration("unsupported pinning database".into())),
    }
}

async fn sqlite<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    let group = TABLES
        .iter()
        .map(|table| format!("'{table}'"))
        .collect::<Vec<_>>()
        .join(",");
    if db.query_one(Statement::from_string(DatabaseBackend::Sqlite, format!(
        "SELECT m.name FROM main.sqlite_schema m JOIN pragma_foreign_key_list(m.name) f WHERE m.type='table' AND lower(f.\"table\") IN ({group}) AND lower(m.name) NOT IN ({group}) LIMIT 1"
    ))).await?.is_some() {
        return Err(DbErr::Migration("pinning migration has external foreign key dependents".into()));
    }
    for table in TABLES {
        let actual: String = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT sql FROM main.sqlite_schema WHERE type='table' AND name=$1",
                [(*table).into()],
            ))
            .await?
            .ok_or_else(|| DbErr::Migration("pinning migration source table missing".into()))?
            .try_get("", "sql")?;
        let shape = crate::store::pinning::management::schema::table_shape;
        let invalid =
            |_| DbErr::Migration("pinning migration source table shape unsupported".into());
        if shape(&actual).map_err(invalid)?
            != shape(&original_definition(table, false)).map_err(invalid)?
        {
            return Err(DbErr::Migration(
                "pinning migration source table shape unsupported".into(),
            ));
        }
    }
    let mut retained = Vec::new();
    let mut columns = Vec::new();
    for table in TABLES {
        let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "SELECT sql FROM sqlite_schema WHERE tbl_name=$1 AND type IN ('index','trigger') AND sql IS NOT NULL ORDER BY type,name", [(*table).into()])).await?;
        for row in rows {
            retained.push(row.try_get::<String>("", "sql")?);
        }
        let rows = db
            .query_all(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT name FROM pragma_table_info($1) ORDER BY cid",
                [(*table).into()],
            ))
            .await?;
        let names = rows
            .iter()
            .map(|row| row.try_get::<String>("", "name"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|name| name != "config_revision" && name != "rule_revision")
            .map(|name| format!("\"{}\"", name.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(",");
        db.execute_unprepared(&format!(
            "CREATE TEMP TABLE current_pinning_{table} AS SELECT {names} FROM {table}"
        ))
        .await?;
        columns.push(names);
    }
    // The completion FK is RESTRICT. Its transaction-local copy has no FK, so
    // neither foreign_keys=OFF nor a rename that rewrites child FKs is needed.
    for table in TABLES.iter().rev() {
        db.execute_unprepared(&format!("DROP TABLE {table}"))
            .await?;
    }
    for (table, names) in TABLES.iter().zip(columns) {
        db.execute_unprepared(&definition(table, false)).await?;
        db.execute_unprepared(&format!(
            "INSERT INTO {table} ({names}) SELECT {names} FROM current_pinning_{table}"
        ))
        .await?;
        db.execute_unprepared(&format!("DROP TABLE current_pinning_{table}"))
            .await?;
    }
    for sql in retained {
        db.execute_unprepared(&sql).await?;
    }
    let violations = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA foreign_key_check",
        ))
        .await?;
    if !violations.is_empty() {
        return Err(DbErr::Migration(
            "pinning migration foreign key violation".into(),
        ));
    }
    Ok(())
}

fn original_state_constraint_matches(definition: &str, states: &str) -> Result<bool, DbErr> {
    let old_states = states
        .strip_suffix(",'error'")
        .ok_or_else(|| DbErr::Migration("invalid compiled state constraint".into()))?;
    let array = old_states
        .split(',')
        .map(|state| format!("{state}::text"))
        .collect::<Vec<_>>()
        .join(",");
    let expected = pg_constraint_tokens(&format!("CHECK ((state = ANY (ARRAY[{array}])))"))?;
    Ok(pg_constraint_tokens(definition)? == expected)
}

// Only pg_get_constraintdef output is compared here; SQLite bracket quoting is unrelated.
fn pg_constraint_tokens(sql: &str) -> Result<Vec<String>, DbErr> {
    let mut chars = sql.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            continue;
        }
        let mut token = c.to_string();
        if matches!(c, '\'' | '"') {
            let escape_string = c == '\'' && tokens.last().is_some_and(|t| t == "e");
            loop {
                let next = chars
                    .next()
                    .ok_or_else(|| DbErr::Migration("invalid installed state constraint".into()))?;
                token.push(next);
                if next == '\\' && escape_string {
                    token.push(chars.next().ok_or_else(|| {
                        DbErr::Migration("invalid installed state constraint".into())
                    })?);
                } else if next == c {
                    if chars.peek() == Some(&c) {
                        token.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }
        } else if c.is_ascii_alphanumeric() || c == '_' {
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
            {
                token.push(chars.next().unwrap());
            }
            token.make_ascii_lowercase();
        }
        tokens.push(token);
    }
    Ok(tokens)
}

async fn postgres<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute_unprepared("ALTER TABLE pin_extension_decisions DROP CONSTRAINT ck_pin_extension_decisions_revision, DROP COLUMN config_revision; ALTER TABLE zip_v2_mpu_intakes DROP CONSTRAINT ck_zip_v2_mpu_identity, DROP COLUMN rule_revision; ALTER TABLE pin_lease_targets ADD COLUMN last_error TEXT").await?;
    let intake = definition("zip_v2_mpu_intakes", true);
    let identity = intake
        .split_once("CONSTRAINT ck_zip_v2_mpu_identity ")
        .expect("identity constraint")
        .1
        .split_once(", CONSTRAINT")
        .expect("following constraint")
        .0;
    db.execute_unprepared(&format!(
        "ALTER TABLE zip_v2_mpu_intakes ADD CONSTRAINT ck_zip_v2_mpu_identity {identity}"
    ))
    .await?;
    for (table, states) in [
        ("pin_jobs", "'pending','running','done','error'"),
        (
            "pin_lease_targets",
            "'waiting','submitted','pinned','degraded','quota_waiting','quota_blocked','evicted','released','error'",
        ),
    ] {
        let constraints = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT c.conname,pg_get_constraintdef(c.oid,false) AS definition FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace JOIN pg_attribute a ON a.attrelid=t.oid AND a.attname='state' WHERE n.nspname=current_schema() AND t.relname=$1 AND c.contype='c' AND c.convalidated AND NOT c.condeferrable AND a.attnum=ANY(c.conkey)", [table.into()])).await?;
        let mut matches = Vec::new();
        for row in constraints {
            let definition: String = row.try_get("", "definition")?;
            if original_state_constraint_matches(&definition, states)? {
                matches.push(row.try_get::<String>("", "conname")?);
            }
        }
        let [name] = matches.as_slice() else {
            return Err(DbErr::Migration(
                "pinning migration requires one exact original state constraint".into(),
            ));
        };
        db.execute_unprepared(&format!(
            "ALTER TABLE {table} DROP CONSTRAINT \"{}\"",
            name.replace('"', "\"\"")
        ))
        .await?;
        db.execute_unprepared(&format!(
            "ALTER TABLE {table} ADD CONSTRAINT ck_{table}_state CHECK(state IN ({states}))"
        ))
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::original_state_constraint_matches;

    const PIN_JOB_STATES: &str = "'pending','running','done','error'";
    const PIN_TARGET_STATES: &str = "'waiting','submitted','pinned','degraded','quota_waiting','quota_blocked','evicted','released','error'";

    #[test]
    fn pg_state_constraints_match_with_array_element_spacing() {
        let spaced =
            "CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text])))";
        let compact = "CHECK ((state = ANY (ARRAY['pending'::text,'running'::text,'done'::text])))";
        assert!(original_state_constraint_matches(spaced, PIN_JOB_STATES).unwrap());
        assert!(original_state_constraint_matches(compact, PIN_JOB_STATES).unwrap());
        let targets = "CHECK ((state = ANY (ARRAY['waiting'::text, 'submitted'::text, 'pinned'::text, 'degraded'::text, 'quota_waiting'::text, 'quota_blocked'::text, 'evicted'::text, 'released'::text])))";
        assert!(original_state_constraint_matches(targets, PIN_TARGET_STATES).unwrap());
        for (definition, states) in [(spaced, PIN_JOB_STATES), (targets, PIN_TARGET_STATES)] {
            let formatted = definition
                .replace("ARRAY[", "ARRAY [\n\t")
                .replace(", ", ",\n\t ")
                .replace("::text", " :: TEXT ");
            assert!(original_state_constraint_matches(&formatted, states).unwrap());
        }
    }

    #[test]
    fn pg_state_constraints_preserve_literal_and_identifier_tokens() {
        let original =
            "CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text])))";
        for changed in [
            original.replace("pending", "pend ing"),
            original.replace("pending", "Pending"),
            original.replace("pending", "pend\t ing"),
            original.replace("pending", "pend''ing"),
            original.replace("'pending'", r"E'pend\'ing'"),
            original.replace("'pending'", r"'pend\ing'"),
            original.replace("state", "\"sta\"\"te\""),
            original.replace("state", "sta te"),
            original.replace("state", "\"STATE\""),
            original.replace("state", "\"sta te\""),
            original.replace("done", "unknown"),
            original.replace("'done'::text", "'done'::text, 'done'::text"),
            original.replace("state = ANY", "state <> ANY"),
            original.replace(")))", ")) AND attempts > 0)"),
            original.replace(")))", ")) OR true)"),
        ] {
            assert!(
                !original_state_constraint_matches(&changed, PIN_JOB_STATES).unwrap(),
                "{changed}"
            );
        }
        let targets = "CHECK ((state = ANY (ARRAY['waiting'::text, 'submitted'::text, 'pinned'::text, 'degraded'::text, 'quota_waiting'::text, 'quota_blocked'::text, 'evicted'::text, 'released'::text])))";
        assert!(
            !original_state_constraint_matches(
                &targets.replace("quota_waiting", "quota_ waiting"),
                PIN_TARGET_STATES
            )
            .unwrap()
        );
    }

    #[test]
    fn pg_state_constraints_reject_already_upgraded_or_partial_lists() {
        let with_error = "CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text, 'error'::text])))";
        assert!(!original_state_constraint_matches(with_error, PIN_JOB_STATES).unwrap());
        let missing = "CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text])))";
        assert!(!original_state_constraint_matches(missing, PIN_JOB_STATES).unwrap());
    }
}
