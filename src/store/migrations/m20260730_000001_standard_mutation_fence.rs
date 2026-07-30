use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn add_mutation_id_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("import_destinations"))
        .add_column(ColumnDef::new(Alias::new("mutation_id")).text())
        .to_owned()
}

fn add_mutation_prefix_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("import_destinations"))
        .add_column(ColumnDef::new(Alias::new("mutation_prefix")).text())
        .to_owned()
}

fn active_mutation_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("idx_import_destinations_active_mutation")
        .table(Alias::new("import_destinations"))
        .col(Alias::new("bucket"))
        .col(Alias::new("mutation_id"))
        .col(Alias::new("key"))
        .to_owned()
}

fn mutation_prefix_index() -> IndexCreateStatement {
    Index::create()
        .if_not_exists()
        .name("idx_import_destinations_mutation_prefix")
        .table(Alias::new("import_destinations"))
        .col(Alias::new("bucket"))
        .col(Alias::new("mutation_prefix"))
        .col(Alias::new("key"))
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(add_mutation_id_column()).await?;
        manager.alter_table(add_mutation_prefix_column()).await?;
        manager.create_index(active_mutation_index()).await?;
        manager.create_index(mutation_prefix_index()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_import_destinations_mutation_prefix")
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .if_exists()
                    .name("idx_import_destinations_active_mutation")
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("import_destinations"))
                    .drop_column(Alias::new("mutation_prefix"))
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Alias::new("import_destinations"))
                    .drop_column(Alias::new("mutation_id"))
                    .to_owned(),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_sql_is_additive_and_indexes_active_rows() {
        let columns = [
            add_mutation_id_column().to_string(PostgresQueryBuilder),
            add_mutation_prefix_column().to_string(PostgresQueryBuilder),
        ];
        assert_eq!(
            columns,
            [
                "ALTER TABLE \"import_destinations\" ADD COLUMN \"mutation_id\" text",
                "ALTER TABLE \"import_destinations\" ADD COLUMN \"mutation_prefix\" text",
            ]
        );
        assert_eq!(
            active_mutation_index().to_string(PostgresQueryBuilder),
            "CREATE INDEX IF NOT EXISTS \"idx_import_destinations_active_mutation\" ON \"import_destinations\" (\"bucket\", \"mutation_id\", \"key\")"
        );
        assert_eq!(
            mutation_prefix_index().to_string(PostgresQueryBuilder),
            "CREATE INDEX IF NOT EXISTS \"idx_import_destinations_mutation_prefix\" ON \"import_destinations\" (\"bucket\", \"mutation_prefix\", \"key\")"
        );
    }
}
