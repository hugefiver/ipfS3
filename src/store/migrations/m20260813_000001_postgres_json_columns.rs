use sea_orm::{ConnectionTrait, DatabaseBackend};
use sea_orm_migration::prelude::*;

const POSTGRES_UP_STATEMENTS: [&str; 8] = [
    r#"DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM objects
        WHERE metadata IS NOT NULL
          AND NOT pg_input_is_valid(metadata, 'jsonb')
    ) THEN
        RAISE EXCEPTION 'invalid JSON in objects.metadata' USING ERRCODE = '22023';
    END IF;
END
$$;"#,
    r#"DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM multipart_uploads
        WHERE metadata IS NOT NULL
          AND NOT pg_input_is_valid(metadata, 'jsonb')
    ) THEN
        RAISE EXCEPTION 'invalid JSON in multipart_uploads.metadata' USING ERRCODE = '22023';
    END IF;
END
$$;"#,
    r#"DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM multipart_uploads
        WHERE tags_json IS NOT NULL
          AND NOT pg_input_is_valid(tags_json, 'jsonb')
    ) THEN
        RAISE EXCEPTION 'invalid JSON in multipart_uploads.tags_json' USING ERRCODE = '22023';
    END IF;
END
$$;"#,
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json DROP DEFAULT",
    "ALTER TABLE objects ALTER COLUMN metadata TYPE JSONB USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::jsonb END",
    "ALTER TABLE multipart_uploads ALTER COLUMN metadata TYPE JSONB USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::jsonb END",
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json TYPE JSONB USING tags_json::jsonb",
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json SET DEFAULT '[]'::jsonb",
];

const POSTGRES_DOWN_STATEMENTS: [&str; 5] = [
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json DROP DEFAULT",
    "ALTER TABLE objects ALTER COLUMN metadata TYPE TEXT USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::text END",
    "ALTER TABLE multipart_uploads ALTER COLUMN metadata TYPE TEXT USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::text END",
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json TYPE TEXT USING tags_json::text",
    "ALTER TABLE multipart_uploads ALTER COLUMN tags_json SET DEFAULT '[]'::text",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            statements_for_backend(
                manager.get_connection().get_database_backend(),
                Direction::Up,
            ),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            statements_for_backend(
                manager.get_connection().get_database_backend(),
                Direction::Down,
            ),
        )
        .await
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Up,
    Down,
}

async fn execute_statements(
    manager: &SchemaManager<'_>,
    statements: Vec<&'static str>,
) -> Result<(), DbErr> {
    for statement in statements {
        manager
            .get_connection()
            .execute_unprepared(statement)
            .await?;
    }
    Ok(())
}

fn statements_for_backend(backend: DatabaseBackend, direction: Direction) -> Vec<&'static str> {
    if backend != DatabaseBackend::Postgres {
        return Vec::new();
    }

    match direction {
        Direction::Up => POSTGRES_UP_STATEMENTS.to_vec(),
        Direction::Down => POSTGRES_DOWN_STATEMENTS.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_json_columns_migration_has_sanitized_fail_closed_up_statements() {
        let statements = statements_for_backend(DatabaseBackend::Postgres, Direction::Up);

        assert_eq!(statements.len(), 8);
        assert_eq!(
            &statements[..3],
            [
                POSTGRES_UP_STATEMENTS[0],
                POSTGRES_UP_STATEMENTS[1],
                POSTGRES_UP_STATEMENTS[2],
            ]
        );
        for (statement, (table, column)) in statements[..3].iter().zip([
            ("objects", "metadata"),
            ("multipart_uploads", "metadata"),
            ("multipart_uploads", "tags_json"),
        ]) {
            assert!(statement.contains(&format!("SELECT 1 FROM {table}")));
            assert!(statement.contains(&format!("{column} IS NOT NULL")));
            assert!(statement.contains(&format!("pg_input_is_valid({column}, 'jsonb')")));
            assert!(statement.contains(&format!("invalid JSON in {table}.{column}")));
            assert!(statement.contains("ERRCODE = '22023'"));
            assert!(!statement.contains("pg_input_error_info"));
        }
        assert_eq!(
            &statements[3..],
            [
                "ALTER TABLE multipart_uploads ALTER COLUMN tags_json DROP DEFAULT",
                "ALTER TABLE objects ALTER COLUMN metadata TYPE JSONB USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::jsonb END",
                "ALTER TABLE multipart_uploads ALTER COLUMN metadata TYPE JSONB USING CASE WHEN metadata IS NULL THEN NULL ELSE metadata::jsonb END",
                "ALTER TABLE multipart_uploads ALTER COLUMN tags_json TYPE JSONB USING tags_json::jsonb",
                "ALTER TABLE multipart_uploads ALTER COLUMN tags_json SET DEFAULT '[]'::jsonb",
            ]
        );
    }

    #[test]
    fn postgres_json_columns_down_migration_restores_text_shape() {
        assert_eq!(
            statements_for_backend(DatabaseBackend::Postgres, Direction::Down),
            POSTGRES_DOWN_STATEMENTS
        );
    }

    #[test]
    fn postgres_json_columns_migration_is_a_non_postgres_noop() {
        for backend in [DatabaseBackend::Sqlite, DatabaseBackend::MySql] {
            assert!(statements_for_backend(backend, Direction::Up).is_empty());
            assert!(statements_for_backend(backend, Direction::Down).is_empty());
        }
    }
}
