//! Durable locators are an index, never an authorization source.
use super::*;
use crate::store::pinning::management::native;
mod batch;
mod postgres_constraints;
mod postgres_guards;

// Fold SQL layout only outside quoted values and identifiers. Literal spelling
// is part of the trigger and constraint behavior, not formatting.
fn storage_sql(sql: &str) -> String {
    let mut quoted = None;
    let mut result = String::with_capacity(sql.len());
    for character in sql.chars() {
        if let Some(delimiter) = quoted {
            result.push(character);
            if character == delimiter {
                quoted = None;
            }
        } else if matches!(character, '\'' | '"') {
            quoted = Some(character);
            result.push(character);
        } else if !character.is_whitespace() {
            result.extend(character.to_lowercase());
        }
    }
    result
}

pub const SOURCE_TABLES: &[&str] = &[
    "pin_management_records",
    "pin_management_retirements",
    "pin_provider_routes",
    "pin_legacy_migrations",
    "pin_legacy_migration_items",
];
/// Export-only collections. They are deliberately outside the native authority
/// catalog: capturing an archive must never recursively capture its own storage.
pub const EXPORT_TABLES: &[&str] = &["pin_management_archive", "pin_management_archive_chunks"];
const APPLIED_NO_DELETE: &str = "CREATE TRIGGER s75_applied_no_delete BEFORE DELETE ON pin_management_records WHEN OLD.state='applied' BEGIN SELECT RAISE(ABORT,'immutable applied record'); END";
const APPLIED_NO_UPDATE: &str = "CREATE TRIGGER s75_applied_no_update BEFORE UPDATE ON pin_management_records WHEN OLD.state='applied' BEGIN SELECT RAISE(ABORT,'immutable applied record'); END";
const ARCHIVE_GUARD_BODY: &str = " BEGIN IF OLD.state='sealed' OR NEW.archive_id IS DISTINCT FROM OLD.archive_id OR NEW.format IS DISTINCT FROM OLD.format OR NEW.database_scope IS DISTINCT FROM OLD.database_scope OR NEW.native_identity IS DISTINCT FROM OLD.native_identity OR NEW.header IS DISTINCT FROM OLD.header THEN RAISE EXCEPTION 'immutable archive'; END IF; RETURN NEW; END ";

fn sqlite_protections() -> Vec<(&'static str, &'static str)> {
    vec![
        ("s75_applied_no_delete", APPLIED_NO_DELETE),
        ("s75_applied_no_update", APPLIED_NO_UPDATE),
        (
            "s75_archive_no_update",
            "CREATE TRIGGER s75_archive_no_update BEFORE UPDATE ON pin_management_archive WHEN OLD.state='sealed' OR NEW.archive_id IS NOT OLD.archive_id OR NEW.format IS NOT OLD.format OR NEW.database_scope IS NOT OLD.database_scope OR NEW.native_identity IS NOT OLD.native_identity OR NEW.header IS NOT OLD.header BEGIN SELECT RAISE(ABORT,'immutable archive'); END",
        ),
        (
            "s75_archive_no_delete",
            "CREATE TRIGGER s75_archive_no_delete BEFORE DELETE ON pin_management_archive BEGIN SELECT RAISE(ABORT,'immutable archive'); END",
        ),
        (
            "s75_chunk_no_update",
            "CREATE TRIGGER s75_chunk_no_update BEFORE UPDATE ON pin_management_archive_chunks BEGIN SELECT RAISE(ABORT,'immutable chunk'); END",
        ),
        (
            "s75_chunk_no_delete",
            "CREATE TRIGGER s75_chunk_no_delete BEFORE DELETE ON pin_management_archive_chunks BEGIN SELECT RAISE(ABORT,'immutable chunk'); END",
        ),
        (
            "s75_chunk_sealed",
            "CREATE TRIGGER s75_chunk_sealed BEFORE INSERT ON pin_management_archive_chunks WHEN (SELECT state FROM pin_management_archive WHERE archive_id=NEW.archive_id)='sealed' BEGIN SELECT RAISE(ABORT,'immutable archive'); END",
        ),
    ]
}

pub fn tables(backend: DatabaseBackend) -> Vec<(&'static str, String)> {
    let blob = if backend == DatabaseBackend::Postgres {
        "BYTEA"
    } else {
        "BLOB"
    };
    vec![
        ("pin_management_readiness", "CREATE TABLE pin_management_readiness (id INTEGER NOT NULL PRIMARY KEY CHECK(id=1), format TEXT NOT NULL CHECK(format='S75A1'), state TEXT NOT NULL CHECK(state IN ('backfill','ready')))".into()),
        ("pin_management_source_control", "CREATE TABLE pin_management_source_control (source_key TEXT NOT NULL PRIMARY KEY, source_generation BIGINT NOT NULL DEFAULT 0 CHECK(source_generation>=0 AND source_generation<9223372036854775807), locator_generation BIGINT NOT NULL DEFAULT 0 CHECK(locator_generation>=0 AND locator_generation<9223372036854775807), sealed_source BIGINT, sealed_locator BIGINT)".into()),
        ("pin_management_locator", "CREATE TABLE pin_management_locator (selector_kind TEXT NOT NULL, selector_key TEXT NOT NULL, source_key TEXT NOT NULL, PRIMARY KEY(selector_kind,selector_key,source_key), FOREIGN KEY(source_key) REFERENCES pin_management_source_control(source_key))".into()),
        ("pin_management_pending_scope", "CREATE TABLE pin_management_pending_scope (selector_kind TEXT NOT NULL, selector_key TEXT NOT NULL, source_key TEXT NOT NULL, source_generation BIGINT NOT NULL, locator_generation BIGINT NOT NULL, PRIMARY KEY(selector_kind,selector_key,source_key))".into()),
        ("pin_management_revision", "CREATE TABLE pin_management_revision (source_table TEXT NOT NULL PRIMARY KEY, revision BIGINT NOT NULL CHECK(revision>=0 AND revision<9223372036854775807))".into()),
        ("pin_management_archive", "CREATE TABLE pin_management_archive (archive_id TEXT NOT NULL PRIMARY KEY, format TEXT NOT NULL CHECK(format='S75A1'), database_scope TEXT NOT NULL, native_identity TEXT NOT NULL, header TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('staging','sealed')), chunks BIGINT NOT NULL DEFAULT 0, bytes BIGINT NOT NULL DEFAULT 0, digest TEXT)".into()),
        ("pin_management_archive_chunks", format!("CREATE TABLE pin_management_archive_chunks (archive_id TEXT NOT NULL, ordinal BIGINT NOT NULL CHECK(ordinal>=0), payload {blob} NOT NULL CHECK(length(payload)<=65536), PRIMARY KEY(archive_id,ordinal), FOREIGN KEY(archive_id) REFERENCES pin_management_archive(archive_id))")),
    ]
}

pub fn indexes() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "s75_locator_source",
            "CREATE INDEX s75_locator_source ON pin_management_locator(source_key,selector_kind,selector_key)",
        ),
        (
            "s75_pending_source",
            "CREATE INDEX s75_pending_source ON pin_management_pending_scope(source_key,source_generation,locator_generation)",
        ),
        (
            "s75_record_kind",
            "CREATE INDEX s75_record_kind ON pin_management_records(kind,database_scope)",
        ),
        (
            "s75_version_public",
            "CREATE INDEX s75_version_public ON object_versions(bucket,key,version_id,id)",
        ),
        (
            "s75_lease_owner",
            "CREATE INDEX s75_lease_owner ON pin_leases(owner_object_id,id)",
        ),
        (
            "s75_target_resource",
            "CREATE INDEX s75_target_resource ON pin_lease_targets(provider,cid,id)",
        ),
        (
            "s75_manifest_version",
            "CREATE INDEX s75_manifest_version ON zip_manifest_entries(version_row_id,batch_id,path)",
        ),
        (
            "s75_jobs_resource",
            "CREATE INDEX s75_jobs_resource ON pin_jobs(provider,cid,id)",
        ),
        (
            "s75_submit_resource",
            "CREATE INDEX s75_submit_resource ON pin_submit_observations(provider,expected_cid,id)",
        ),
        (
            "s75_replica_resource",
            "CREATE INDEX s75_replica_resource ON pin_replica_observations(provider,expected_cid,sequence)",
        ),
        (
            "s75_legacy_reverses",
            "CREATE INDEX s75_legacy_reverses ON pin_legacy_migrations(reverses,id)",
        ),
        (
            "s75_lifecycle_version",
            "CREATE INDEX s75_lifecycle_version ON lifecycle_actions(target_version_row_id,id)",
        ),
        (
            "s75_transition_version",
            "CREATE INDEX s75_transition_version ON lifecycle_transitions(target_version_row_id,id)",
        ),
        (
            "s75_residency_version",
            "CREATE INDEX s75_residency_version ON residency_references(version_row_id,owner_kind,owner_id,reason,tier,cid)",
        ),
    ]
}

fn source_expression(
    backend: DatabaseBackend,
    table: &native::NativeTable,
    prefix: &str,
) -> String {
    let keys = table
        .primary_key
        .iter()
        .map(|c| format!("CAST({prefix}.\"{c}\" AS TEXT)"))
        .collect::<Vec<_>>()
        .join(",");
    let json = if backend == DatabaseBackend::Sqlite {
        format!("json_array({keys})")
    } else {
        format!("array_to_json(ARRAY[{keys}])::text")
    };
    format!("'{}:' || {json}", table.table)
}

fn dirty_source(key: &str) -> String {
    format!(
        "INSERT INTO pin_management_source_control(source_key,source_generation) VALUES ({key},1) ON CONFLICT(source_key) DO UPDATE SET source_generation=pin_management_source_control.source_generation+1,sealed_source=NULL,sealed_locator=NULL;\
        INSERT INTO pin_management_pending_scope SELECT selector_kind,selector_key,c.source_key,c.source_generation,c.locator_generation FROM pin_management_locator l JOIN pin_management_source_control c ON c.source_key=l.source_key WHERE c.source_key={key} ON CONFLICT(selector_kind,selector_key,source_key) DO UPDATE SET source_generation=excluded.source_generation,locator_generation=excluded.locator_generation;\
        INSERT INTO pin_management_pending_scope SELECT 'unknown','*',source_key,source_generation,locator_generation FROM pin_management_source_control WHERE source_key={key} ON CONFLICT(selector_kind,selector_key,source_key) DO UPDATE SET source_generation=excluded.source_generation,locator_generation=excluded.locator_generation;"
    )
}
fn dirty_locator(prefix: &str) -> String {
    format!(
        "UPDATE pin_management_source_control SET locator_generation=locator_generation+1,sealed_source=NULL,sealed_locator=NULL WHERE source_key={prefix}.source_key;\
        INSERT INTO pin_management_pending_scope SELECT {prefix}.selector_kind,{prefix}.selector_key,source_key,source_generation,locator_generation FROM pin_management_source_control WHERE source_key={prefix}.source_key ON CONFLICT(selector_kind,selector_key,source_key) DO UPDATE SET source_generation=excluded.source_generation,locator_generation=excluded.locator_generation;"
    )
}

/// (name, table, operation, body); bodies are checked on every schema validation.
pub fn triggers(backend: DatabaseBackend) -> Vec<(String, String, &'static str, String)> {
    let mut triggers = Vec::new();
    for table in native::registry() {
        for (event, prefixes) in [
            ("INSERT", vec!["NEW"]),
            ("UPDATE", vec!["OLD", "NEW"]),
            ("DELETE", vec!["OLD"]),
        ] {
            let mut body = format!(
                "UPDATE pin_management_revision SET revision=revision+1 WHERE source_table='{}';",
                table.table
            );
            if SOURCE_TABLES.contains(&table.table.as_str()) {
                for prefix in prefixes {
                    body.push_str(&dirty_source(&source_expression(backend, &table, prefix)));
                }
            }
            triggers.push((
                format!("s75_{}_{}", table.table, event.to_lowercase()),
                table.table.clone(),
                event,
                body,
            ));
        }
    }
    for table in EXPORT_TABLES {
        for event in ["INSERT", "UPDATE", "DELETE"] {
            triggers.push((
                format!("s75_export_{table}_{}", event.to_lowercase()),
                (*table).into(),
                event,
                format!("UPDATE pin_management_revision SET revision=revision+1 WHERE source_table='{table}';"),
            ));
        }
    }
    for (event, prefixes) in [
        ("INSERT", vec!["NEW"]),
        ("UPDATE", vec!["OLD", "NEW"]),
        ("DELETE", vec!["OLD"]),
    ] {
        triggers.push((
            format!("s75_locator_{}", event.to_lowercase()),
            "pin_management_locator".into(),
            event,
            prefixes.into_iter().map(dirty_locator).collect::<String>(),
        ));
    }
    triggers
}
pub fn trigger_ddl(
    backend: DatabaseBackend,
    name: &str,
    table: &str,
    event: &str,
    body: &str,
) -> Vec<String> {
    if backend == DatabaseBackend::Sqlite {
        vec![format!(
            "CREATE TRIGGER {name} AFTER {event} ON \"{table}\" FOR EACH ROW BEGIN {body} END"
        )]
    } else {
        vec![
            format!(
                "CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $s75$ BEGIN {body} RETURN NULL; END $s75$"
            ),
            format!(
                "CREATE TRIGGER {name} AFTER {event} ON \"{table}\" FOR EACH ROW EXECUTE FUNCTION {name}()"
            ),
        ]
    }
}
pub fn known_object(sql: Option<&str>, backend: DatabaseBackend) -> bool {
    let Some(sql) = sql else {
        return false;
    };
    let sql = storage_sql(sql);
    sqlite_protections()
        .iter()
        .any(|(_, ddl)| sql == storage_sql(ddl))
        || indexes().iter().any(|(_, ddl)| sql == storage_sql(ddl))
        || triggers(backend).iter().any(|(n, t, e, b)| {
            trigger_ddl(backend, n, t, e, b)
                .iter()
                .any(|ddl| sql == storage_sql(ddl))
        })
}

pub async fn install<C: ConnectionTrait>(db: &C) -> std::result::Result<(), sea_orm::DbErr> {
    let backend = db.get_database_backend();
    for (_, ddl) in tables(backend) {
        db.execute_unprepared(&ddl).await?;
    }
    for (_, ddl) in indexes() {
        db.execute_unprepared(ddl).await?;
    }
    for table in native::registry() {
        db.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO pin_management_revision VALUES ($1,0)",
            [table.table.into()],
        ))
        .await?;
    }
    for table in EXPORT_TABLES {
        db.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO pin_management_revision VALUES ($1,0)",
            [(*table).into()],
        ))
        .await?;
    }
    for (name, table, event, body) in triggers(backend) {
        for ddl in trigger_ddl(backend, &name, &table, event, &body) {
            db.execute_unprepared(&ddl).await?;
        }
    }
    if backend == DatabaseBackend::Postgres {
        db.execute_unprepared("CREATE FUNCTION s75_no_truncate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled migration required'; END $$").await?;
        for table in native::registry() {
            db.execute_unprepared(&format!("CREATE TRIGGER s75_{}_truncate BEFORE TRUNCATE ON \"{}\" FOR EACH STATEMENT EXECUTE FUNCTION s75_no_truncate()",table.table,table.table)).await?;
        }
        for table in ["pin_management_archive", "pin_management_archive_chunks"] {
            db.execute_unprepared(&format!("CREATE TRIGGER s75_{table}_truncate BEFORE TRUNCATE ON \"{table}\" FOR EACH STATEMENT EXECUTE FUNCTION s75_no_truncate()")).await?;
        }
    }
    // Original applied facts cannot disappear, even when there are no locators.
    if backend == DatabaseBackend::Sqlite {
        for (_, ddl) in sqlite_protections() {
            db.execute_unprepared(ddl).await?;
        }
    } else {
        db.execute_unprepared("CREATE FUNCTION s75_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable archive'; END $$").await?;
        db.execute_unprepared(&format!("CREATE FUNCTION s75_archive_guard() RETURNS trigger LANGUAGE plpgsql AS $${ARCHIVE_GUARD_BODY}$$")).await?;
        db.execute_unprepared("CREATE TRIGGER s75_archive_no_update BEFORE UPDATE ON pin_management_archive FOR EACH ROW EXECUTE FUNCTION s75_archive_guard()").await?;
        db.execute_unprepared("CREATE TRIGGER s75_archive_no_delete BEFORE DELETE ON pin_management_archive FOR EACH ROW EXECUTE FUNCTION s75_immutable()").await?;
        db.execute_unprepared("CREATE TRIGGER s75_chunk_immutable BEFORE UPDATE OR DELETE ON pin_management_archive_chunks FOR EACH ROW EXECUTE FUNCTION s75_immutable()").await?;
        db.execute_unprepared("CREATE TRIGGER s75_applied_no_delete BEFORE DELETE ON pin_management_records FOR EACH ROW WHEN (OLD.state='applied') EXECUTE FUNCTION s75_immutable()").await?;
        db.execute_unprepared("CREATE TRIGGER s75_applied_no_update BEFORE UPDATE ON pin_management_records FOR EACH ROW WHEN (OLD.state='applied') EXECUTE FUNCTION s75_immutable()").await?;
        db.execute_unprepared("CREATE FUNCTION s75_chunk_guard() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE status TEXT; BEGIN SELECT state INTO STRICT status FROM pin_management_archive WHERE archive_id=NEW.archive_id FOR SHARE NOWAIT; IF status='sealed' THEN RAISE EXCEPTION 'immutable archive'; END IF; RETURN NEW; END $$").await?;
        db.execute_unprepared("CREATE TRIGGER s75_chunk_sealed BEFORE INSERT ON pin_management_archive_chunks FOR EACH ROW EXECUTE FUNCTION s75_chunk_guard()").await?;
    }
    db.execute_unprepared("INSERT INTO pin_management_readiness VALUES (1,'S75A1','backfill')")
        .await?;
    // Migration publication is allowed only for a fresh management inventory.
    let existing = db.query_one(Statement::from_string(backend,"SELECT 1 WHERE EXISTS (SELECT 1 FROM pin_management_records WHERE kind<>'database_scope') OR EXISTS (SELECT 1 FROM pin_management_retirements) OR EXISTS (SELECT 1 FROM pin_provider_routes) OR EXISTS (SELECT 1 FROM pin_legacy_migrations) OR EXISTS (SELECT 1 FROM pin_legacy_migration_items)")).await?;
    if existing.is_none() {
        db.execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1")
            .await?;
    }
    Ok(())
}

pub async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    validate_state(db, false).await
}

pub(super) async fn validate_state<C: ConnectionTrait>(db: &C, backfill: bool) -> Result<()> {
    validate_inner(db, backfill, false).await
}

pub(super) async fn validate_schema<C: ConnectionTrait>(db: &C, backfill: bool) -> Result<()> {
    validate_inner(db, backfill, true).await
}

async fn validate_inner<C: ConnectionTrait>(db: &C, backfill: bool, records: bool) -> Result<()> {
    let backend = db.get_database_backend();
    if !matches!(backend, DatabaseBackend::Sqlite | DatabaseBackend::Postgres) {
        return Err(ManagementError::Schema);
    }
    let expected_state = if backfill { "backfill" } else { "ready" };
    if backend == DatabaseBackend::Sqlite {
        batch::sqlite(db, records, expected_state).await?;
    } else {
        let tables = tables(backend);
        batch::reject_shadows(db, &tables, records).await?;
        if db
            .query_one(Statement::from_sql_and_values(
                backend,
                "SELECT 1 FROM pin_management_readiness WHERE id=1 AND format='S75A1' AND state=$1",
                [expected_state.into()],
            ))
            .await
            .map_err(|_| ManagementError::Schema)?
            .is_none()
        {
            return Err(ManagementError::Schema);
        }
        batch::postgres_bounds(db, &tables).await?;
        postgres_catalog(db).await?;
        postgres_constraints::validate(db).await?;
        postgres_guards::validate(db).await?;
        batch::postgres(db).await?;
    }
    Ok(())
}

async fn postgres_catalog<C: ConnectionTrait>(db: &C) -> Result<()> {
    use crate::store::pinning::management::catalog::NativeColumn;
    let mut expected = Vec::new();
    for (name, columns, pk) in [
        ("pin_management_readiness", "id:i,format:t,state:t", "id"),
        (
            "pin_management_source_control",
            "source_key:t,source_generation:b,locator_generation:b,sealed_source:b?,sealed_locator:b?",
            "source_key",
        ),
        (
            "pin_management_locator",
            "selector_kind:t,selector_key:t,source_key:t",
            "selector_kind,selector_key,source_key",
        ),
        (
            "pin_management_pending_scope",
            "selector_kind:t,selector_key:t,source_key:t,source_generation:b,locator_generation:b",
            "selector_kind,selector_key,source_key",
        ),
        (
            "pin_management_revision",
            "source_table:t,revision:b",
            "source_table",
        ),
        (
            "pin_management_archive",
            "archive_id:t,format:t,database_scope:t,native_identity:t,header:t,state:t,chunks:b,bytes:b,digest:t?",
            "archive_id",
        ),
        (
            "pin_management_archive_chunks",
            "archive_id:t,ordinal:b,payload:x",
            "archive_id,ordinal",
        ),
    ] {
        let mut definitions = columns
            .split(',')
            .map(|column| {
                let (name, ty) = column.split_once(':').expect("static storage column");
                NativeColumn {
                    name: name.into(),
                    sqlite_type: String::new(),
                    postgres_type: match ty.trim_end_matches('?') {
                        "i" => "integer",
                        "b" => "bigint",
                        "x" => "bytea",
                        _ => "text",
                    }
                    .into(),
                    nullable: ty.ends_with('?'),
                }
            })
            .collect::<Vec<_>>();
        definitions.sort_by(|a, b| a.name.cmp(&b.name));
        let table = native::NativeTable {
            table: name.into(),
            columns: definitions
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .into(),
            primary_key: pk.split(',').map(str::to_owned).collect::<Vec<_>>().into(),
            rows: vec![],
            definitions: definitions.into(),
        };
        expected.push(table);
    }
    batch::postgres_tables(db, &expected).await
}
