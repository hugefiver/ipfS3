use sea_orm::{ConnectionTrait, DatabaseBackend};
use sea_orm_migration::prelude::*;

const TIMESTAMP_COLUMNS: [(&str, &str); 4] = [
    ("buckets", "created_at"),
    ("objects", "created_at"),
    ("multipart_uploads", "created_at"),
    ("multipart_parts", "uploaded_at"),
];

#[derive(Clone, Copy)]
enum ConversionDirection {
    Up,
    Down,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            timestamp_statements_for_backend(
                manager.get_connection().get_database_backend(),
                ConversionDirection::Up,
            ),
        )
        .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_statements(
            manager,
            timestamp_statements_for_backend(
                manager.get_connection().get_database_backend(),
                ConversionDirection::Down,
            ),
        )
        .await
    }
}

async fn execute_statements(
    manager: &SchemaManager<'_>,
    statements: Vec<String>,
) -> Result<(), DbErr> {
    for statement in statements {
        manager
            .get_connection()
            .execute_unprepared(&statement)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
fn postgres_up_statements() -> Vec<String> {
    postgres_timestamp_statements(ConversionDirection::Up)
}

#[cfg(test)]
fn postgres_down_statements() -> Vec<String> {
    postgres_timestamp_statements(ConversionDirection::Down)
}

fn timestamp_statements_for_backend(
    backend: DatabaseBackend,
    direction: ConversionDirection,
) -> Vec<String> {
    if backend == DatabaseBackend::Postgres {
        postgres_timestamp_statements(direction)
    } else {
        Vec::new()
    }
}

fn postgres_timestamp_statements(direction: ConversionDirection) -> Vec<String> {
    let (from_type, to_type) = match direction {
        ConversionDirection::Up => ("timestamp without time zone", "TIMESTAMPTZ"),
        ConversionDirection::Down => ("timestamp with time zone", "TIMESTAMP"),
    };
    TIMESTAMP_COLUMNS
        .iter()
        .map(|(table, column)| {
            format!(
                r#"DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = '{table}'
          AND column_name = '{column}'
          AND data_type = '{from_type}'
    ) THEN
        ALTER TABLE "{table}"
            ALTER COLUMN "{column}" TYPE {to_type}
            USING "{column}" AT TIME ZONE 'UTC';
    END IF;
END
$$;"#
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use sea_orm::DatabaseBackend;

    use super::*;

    #[test]
    fn postgres_utc_timestamp_migration_has_guarded_utc_conversion_sql() {
        let statements = postgres_up_statements();
        assert_eq!(statements.len(), TIMESTAMP_COLUMNS.len());
        for (statement, (table, column)) in statements.iter().zip(TIMESTAMP_COLUMNS) {
            assert!(statement.contains("information_schema.columns"));
            assert!(statement.contains("table_schema = current_schema()"));
            assert!(statement.contains(&format!("table_name = '{table}'")));
            assert!(statement.contains(&format!("column_name = '{column}'")));
            assert!(statement.contains("data_type = 'timestamp without time zone'"));
            assert!(statement.contains(&format!(
                "ALTER TABLE \"{table}\"\n            ALTER COLUMN \"{column}\" TYPE TIMESTAMPTZ"
            )));
            assert!(statement.contains(&format!("USING \"{column}\" AT TIME ZONE 'UTC'")));
            assert!(!statement.contains("CREATE TABLE"));
            assert!(!statement.contains("DROP TABLE"));
        }
    }

    #[test]
    fn down_migration_uses_explicit_utc_conversion() {
        let statements = postgres_down_statements();
        assert_eq!(statements.len(), TIMESTAMP_COLUMNS.len());
        for (statement, (table, column)) in statements.iter().zip(TIMESTAMP_COLUMNS) {
            assert!(statement.contains("data_type = 'timestamp with time zone'"));
            assert!(statement.contains(&format!(
                "ALTER TABLE \"{table}\"\n            ALTER COLUMN \"{column}\" TYPE TIMESTAMP"
            )));
            assert!(statement.contains(&format!("USING \"{column}\" AT TIME ZONE 'UTC'")));
        }
    }

    #[test]
    fn non_postgres_backends_are_noops() {
        for backend in [DatabaseBackend::Sqlite, DatabaseBackend::MySql] {
            assert!(timestamp_statements_for_backend(backend, ConversionDirection::Up).is_empty());
            assert!(
                timestamp_statements_for_backend(backend, ConversionDirection::Down).is_empty()
            );
        }
    }
}
