//! PostgreSQL immutable storage guards are part of the authority boundary.
use super::*;
use batch::SQL_BYTES;
use std::collections::BTreeMap;

pub(super) async fn validate<C: ConnectionTrait>(db: &C) -> Result<()> {
    let mut counts = BTreeMap::from([
        ("pin_management_readiness", 0_i64),
        ("pin_management_source_control", 0),
        ("pin_management_locator", 3),
        ("pin_management_pending_scope", 0),
        ("pin_management_revision", 0),
        ("pin_management_archive", 7),
        ("pin_management_archive_chunks", 6),
        ("pin_management_inline_archive", 6),
        ("pin_management_archive_identity", 2),
    ]);
    let rows = db.query_all(Statement::from_string(DatabaseBackend::Postgres,
        "SELECT wanted.table_name, counts.count FROM (VALUES ('pin_management_readiness'),('pin_management_source_control'),('pin_management_locator'),('pin_management_pending_scope'),('pin_management_revision'),('pin_management_archive'),('pin_management_archive_chunks'),('pin_management_inline_archive'),('pin_management_archive_identity')) AS wanted(table_name) CROSS JOIN LATERAL (SELECT COUNT(*) AS count FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=wanted.table_name AND NOT t.tgisinternal) counts"))
        .await.map_err(|_| ManagementError::Schema)?;
    if rows.len() != counts.len() {
        return Err(ManagementError::Schema);
    }
    for row in rows {
        let table: String = row
            .try_get("", "table_name")
            .map_err(|_| ManagementError::Schema)?;
        let expected = counts
            .remove(table.as_str())
            .ok_or(ManagementError::Schema)?;
        let count: i64 = row
            .try_get("", "count")
            .map_err(|_| ManagementError::Schema)?;
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
    let guards = [
        (
            "s75_archive_id_segmented",
            "pin_management_archive",
            "INSERT",
            "",
            "s75_archive_id_claim",
            archive_identity::PG_BODY,
        ),
        (
            "s75_archive_id_inline",
            "pin_management_inline_archive",
            "INSERT",
            "",
            "s75_archive_id_claim",
            archive_identity::PG_BODY,
        ),
        (
            "s75_archive_id_immutable",
            "pin_management_archive_identity",
            "DELETE OR UPDATE",
            "",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
        (
            "s75_archive_id_truncate",
            "pin_management_archive_identity",
            "TRUNCATE",
            "",
            "s75_no_truncate",
            " BEGIN RAISE EXCEPTION 'controlled migration required'; END ",
        ),
        (
            "s75_pin_management_inline_archive_truncate",
            "pin_management_inline_archive",
            "TRUNCATE",
            "",
            "s75_no_truncate",
            " BEGIN RAISE EXCEPTION 'controlled migration required'; END ",
        ),
        (
            "s75_inline_immutable",
            "pin_management_inline_archive",
            "DELETE OR UPDATE",
            "",
            "s75_immutable",
            " BEGIN RAISE EXCEPTION 'immutable archive'; END ",
        ),
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
    ];
    let pairs = (0..guards.len())
        .map(|i| format!("(${}::text,${}::text)", i * 2 + 1, i * 2 + 2))
        .collect::<Vec<_>>()
        .join(",");
    let values = guards
        .iter()
        .flat_map(|(name, table, ..)| [sea_orm::Value::from(*table), sea_orm::Value::from(*name)]);
    let rows = db.query_all(Statement::from_sql_and_values(DatabaseBackend::Postgres, format!(
        "SELECT c.relname::text AS table_name,t.tgname::text AS name, \
         CASE WHEN octet_length(pg_get_triggerdef(t.oid,false))<={SQL_BYTES} THEN pg_get_triggerdef(t.oid,false) ELSE NULL END AS definition, t.tgenabled::text AS enabled, \
         CASE WHEN octet_length(p.prosrc)<={SQL_BYTES} THEN p.prosrc ELSE NULL END AS prosrc, \
         NOT p.prosecdef AND p.proconfig IS NULL AND pn.nspname=current_schema() AND l.lanname='plpgsql' AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND NOT t.tgisinternal AND t.tgnargs=0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL AS safe \
         FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang \
         WHERE n.nspname=current_schema() AND (c.relname,t.tgname) IN (VALUES {pairs}) LIMIT 15"), values))
        .await.map_err(|_| ManagementError::Schema)?;
    if rows.len() != guards.len() {
        return Err(ManagementError::Schema);
    }
    let mut expected = guards
        .into_iter()
        .map(|(name, table, event, condition, function, body)| {
            (
                (table.to_owned(), name.to_owned()),
                (event, condition, function, body),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for row in rows {
        let table: String = row
            .try_get("", "table_name")
            .map_err(|_| ManagementError::Schema)?;
        let name: String = row
            .try_get("", "name")
            .map_err(|_| ManagementError::Schema)?;
        let key = (table, name);
        let (event, condition, function, body) =
            expected.remove(&key).ok_or(ManagementError::Schema)?;
        let (table, name) = key;
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
