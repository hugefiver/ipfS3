//! PostgreSQL immutable storage guards are part of the authority boundary.
use super::*;

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    for (table, expected) in [
        ("pin_management_readiness", 0_i64),
        ("pin_management_source_control", 0),
        ("pin_management_locator", 3),
        ("pin_management_pending_scope", 0),
        ("pin_management_revision", 0),
        ("pin_management_archive", 6),
        ("pin_management_archive_chunks", 6),
    ] {
        let count: i64 = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT COUNT(*) AS count FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND NOT t.tgisinternal",
            [table.into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?
            .try_get("", "count").map_err(|_| ManagementError::Schema)?;
        if count != expected {
            return Err(ManagementError::Schema);
        }
    }
    let schema: String = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT quote_ident(current_schema()) AS name",
        ))
        .await
        .map_err(|_| ManagementError::Schema)?
        .ok_or(ManagementError::Schema)?
        .try_get("", "name")
        .map_err(|_| ManagementError::Schema)?;
    for (name, table, event, condition, function, body) in [
        (
            "s75_pin_management_archive_truncate",
            "pin_management_archive",
            "TRUNCATE",
            "",
            "s75_no_truncate",
            " BEGIN RAISE EXCEPTION 'controlled migration required'; END ",
        ),
        (
            "s75_pin_management_archive_chunks_truncate",
            "pin_management_archive_chunks",
            "TRUNCATE",
            "",
            "s75_no_truncate",
            " BEGIN RAISE EXCEPTION 'controlled migration required'; END ",
        ),
        (
            "s75_archive_no_update",
            "pin_management_archive",
            "UPDATE",
            "",
            "s75_archive_guard",
            ARCHIVE_GUARD_BODY,
        ),
        (
            "s75_archive_no_delete",
            "pin_management_archive",
            "DELETE",
            "",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
        (
            "s75_chunk_immutable",
            "pin_management_archive_chunks",
            "DELETE OR UPDATE",
            "",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
        (
            "s75_applied_no_delete",
            "pin_management_records",
            "DELETE",
            " WHEN ((old.state = 'applied'::text))",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
        (
            "s75_applied_no_update",
            "pin_management_records",
            "UPDATE",
            " WHEN ((old.state = 'applied'::text))",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
        (
            "s75_chunk_sealed",
            "pin_management_archive_chunks",
            "INSERT",
            "",
            "s75_chunk_guard",
            " DECLARE status TEXT; BEGIN SELECT state INTO STRICT status FROM pin_management_archive WHERE archive_id=NEW.archive_id FOR SHARE NOWAIT; IF status='sealed' THEN RAISE EXCEPTION 'immutable archive'; END IF; RETURN NEW; END ",
        ),
    ] {
        let row = db.query_one(Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT pg_get_triggerdef(t.oid,false) AS definition, t.tgenabled::text AS enabled, p.prosrc, NOT p.prosecdef AND p.proconfig IS NULL AND pn.nspname=current_schema() AND l.lanname='plpgsql' AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND NOT t.tgisinternal AND t.tgnargs=0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AS safe FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=current_schema() AND c.relname=$1 AND t.tgname=$2",
            [table.into(), name.into()])).await.map_err(|_| ManagementError::Schema)?.ok_or(ManagementError::Schema)?;
        let granularity = if event == "TRUNCATE" {
            "STATEMENT"
        } else {
            "ROW"
        };
        let expected = format!(
            "CREATE TRIGGER {name} BEFORE {event} ON {schema}.{table} FOR EACH {granularity}{condition} EXECUTE FUNCTION {function}()"
        );
        if row
            .try_get::<String>("", "enabled")
            .map_err(|_| ManagementError::Schema)?
            != "O"
            || !row
                .try_get::<bool>("", "safe")
                .map_err(|_| ManagementError::Schema)?
            || row
                .try_get::<String>("", "definition")
                .map_err(|_| ManagementError::Schema)?
                != expected
            || row
                .try_get::<String>("", "prosrc")
                .map_err(|_| ManagementError::Schema)?
                != body
        {
            return Err(ManagementError::Schema);
        }
    }
    Ok(())
}
