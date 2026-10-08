//! The two R26 receipt tables only. Used by formal upgrade and read-only validation.
pub(crate) const OPERATIONS: &str = "pin_legacy_migrations";
pub(crate) const ITEMS: &str = "pin_legacy_migration_items";
pub(crate) const OPERATIONS_SQL: &str = "CREATE TABLE pin_legacy_migrations (id TEXT PRIMARY KEY NOT NULL, direction TEXT NOT NULL CHECK(direction IN ('apply','rollback')), reverses TEXT UNIQUE REFERENCES pin_legacy_migrations(id) ON DELETE RESTRICT, database_scope TEXT NOT NULL, db_clock TEXT NOT NULL, schema_version TEXT NOT NULL, declaration TEXT NOT NULL, confirmation TEXT NOT NULL, item_count INTEGER NOT NULL CHECK(item_count>0 AND item_count<=10100), CHECK ((direction='apply' AND reverses IS NULL) OR (direction='rollback' AND reverses IS NOT NULL)))";
pub(crate) const ITEMS_SQL: &str = "CREATE TABLE pin_legacy_migration_items (operation_id TEXT NOT NULL REFERENCES pin_legacy_migrations(id) ON DELETE RESTRICT, ordinal INTEGER NOT NULL CHECK(ordinal>=0), table_kind TEXT NOT NULL, original_key TEXT NOT NULL, before_image TEXT NOT NULL, after_image TEXT NOT NULL, bindings TEXT NOT NULL, PRIMARY KEY(operation_id,ordinal), UNIQUE(operation_id,table_kind,original_key))";
pub(crate) const SQLITE_BOUND: &str = "CREATE TRIGGER pin_legacy_item_bound BEFORE INSERT ON pin_legacy_migration_items WHEN NEW.ordinal >= (SELECT item_count FROM pin_legacy_migrations WHERE id=NEW.operation_id) BEGIN SELECT RAISE(ABORT,'sealed migration receipt item range'); END";
pub(crate) const PG_BOUND_BODY: &str = "BEGIN IF NEW.ordinal >= (SELECT item_count FROM pin_legacy_migrations WHERE id=NEW.operation_id) THEN RAISE EXCEPTION 'sealed migration receipt item range'; END IF; RETURN NEW; END";
pub(crate) const PG_IMMUTABLE_BODY: &str =
    "BEGIN RAISE EXCEPTION 'immutable legacy migration receipt'; END";

pub(crate) fn sqlite_immutable(table: &str, operation: &str) -> String {
    format!(
        "CREATE TRIGGER {table}_no_{} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable legacy migration receipt'); END",
        operation.to_ascii_lowercase()
    )
}

pub(crate) fn columns(table: &str) -> Vec<(&'static str, &'static str, bool, i32)> {
    if table == OPERATIONS {
        vec![
            ("id", "TEXT", true, 1),
            ("direction", "TEXT", true, 0),
            ("reverses", "TEXT", false, 0),
            ("database_scope", "TEXT", true, 0),
            ("db_clock", "TEXT", true, 0),
            ("schema_version", "TEXT", true, 0),
            ("declaration", "TEXT", true, 0),
            ("confirmation", "TEXT", true, 0),
            ("item_count", "INTEGER", true, 0),
        ]
    } else {
        vec![
            ("operation_id", "TEXT", true, 1),
            ("ordinal", "INTEGER", true, 2),
            ("table_kind", "TEXT", true, 0),
            ("original_key", "TEXT", true, 0),
            ("before_image", "TEXT", true, 0),
            ("after_image", "TEXT", true, 0),
            ("bindings", "TEXT", true, 0),
        ]
    }
}

pub(crate) fn unique_keys(table: &str) -> Vec<Vec<&'static str>> {
    if table == OPERATIONS {
        vec![vec!["id"], vec!["reverses"]]
    } else {
        vec![
            vec!["operation_id", "ordinal"],
            vec!["operation_id", "table_kind", "original_key"],
        ]
    }
}
