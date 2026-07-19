use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn add_target_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column(ColumnDef::new(Alias::new("decompress_zip_target")).text())
        .to_owned()
}

fn add_result_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column(
            ColumnDef::new_with_type(
                Alias::new("decompress_zip_result"),
                ColumnType::custom("BOOLEAN"),
            )
            .not_null()
            .default(true),
        )
        .to_owned()
}

fn drop_result_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("decompress_zip_result"))
        .to_owned()
}

fn drop_target_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("decompress_zip_target"))
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(add_target_column()).await?;
        manager.alter_table(add_result_column()).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(drop_result_column()).await?;
        manager.alter_table(drop_target_column()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

    use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;

    async fn upload_snapshot(
        db: &DatabaseConnection,
    ) -> (
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ) {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT upload_id, object_id, bucket, key, \
                        CAST(created_at AS TEXT) AS created_at_text, encryption_mode, \
                        key_wrap, content_type, metadata \
                 FROM multipart_uploads WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap();
        (
            row.try_get("", "upload_id").unwrap(),
            row.try_get("", "object_id").unwrap(),
            row.try_get("", "bucket").unwrap(),
            row.try_get("", "key").unwrap(),
            row.try_get("", "created_at_text").unwrap(),
            row.try_get("", "encryption_mode").unwrap(),
            row.try_get("", "key_wrap").unwrap(),
            row.try_get("", "content_type").unwrap(),
            row.try_get("", "metadata").unwrap(),
        )
    }

    async fn part_snapshot(db: &DatabaseConnection) -> (String, i32, String, i64, String, String) {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT upload_id, part_number, cid, size, etag, \
                        CAST(uploaded_at AS TEXT) AS uploaded_at_text \
                 FROM multipart_parts WHERE upload_id = 'upload-1' AND part_number = 1",
            ))
            .await
            .unwrap()
            .unwrap();
        (
            row.try_get("", "upload_id").unwrap(),
            row.try_get("", "part_number").unwrap(),
            row.try_get("", "cid").unwrap(),
            row.try_get("", "size").unwrap(),
            row.try_get("", "etag").unwrap(),
            row.try_get("", "uploaded_at_text").unwrap(),
        )
    }

    #[tokio::test]
    async fn up_down_preserves_upload_part_data_and_cascade_fk_on_sqlite() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        let manager = SchemaManager::new(&db);
        InitMigration.up(&manager).await.unwrap();

        db.execute_unprepared("INSERT INTO buckets (name, owner) VALUES ('bucket', 'owner')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_uploads \
             (upload_id, object_id, bucket, key, encryption_mode, content_type, metadata) \
             VALUES ('upload-1', 'object-1', 'bucket', 'archive.zip', 'none', \
                     'application/zip', '{\"source\":\"seed\"}')",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_parts \
             (upload_id, part_number, cid, size, etag) \
             VALUES ('upload-1', 1, 'QmPart', 7, 'QmPart')",
        )
        .await
        .unwrap();

        let upload_before = upload_snapshot(&db).await;
        let part_before = part_snapshot(&db).await;

        Migration.up(&manager).await.unwrap();
        let added = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT decompress_zip_target, decompress_zip_result \
                 FROM multipart_uploads WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            added
                .try_get::<Option<String>>("", "decompress_zip_target")
                .unwrap(),
            None
        );
        assert!(added.try_get::<bool>("", "decompress_zip_result").unwrap());

        Migration.down(&manager).await.unwrap();
        assert_eq!(upload_snapshot(&db).await, upload_before);
        assert_eq!(part_snapshot(&db).await, part_before);

        let foreign_keys = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA foreign_key_list(multipart_parts)",
            ))
            .await
            .unwrap();
        assert!(foreign_keys.iter().any(|row| {
            row.try_get::<String>("", "table").unwrap() == "multipart_uploads"
                && row.try_get::<String>("", "from").unwrap() == "upload_id"
                && row.try_get::<String>("", "to").unwrap() == "upload_id"
                && row.try_get::<String>("", "on_delete").unwrap() == "CASCADE"
        }));

        db.execute_unprepared("DELETE FROM multipart_uploads WHERE upload_id = 'upload-1'")
            .await
            .unwrap();
        let remaining = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM multipart_parts WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remaining.try_get::<i64>("", "count").unwrap(), 0);
    }

    #[test]
    fn postgres_builders_emit_alter_column_sql_without_parent_rebuild() {
        let sql = [
            add_target_column().to_string(PostgresQueryBuilder),
            add_result_column().to_string(PostgresQueryBuilder),
            drop_result_column().to_string(PostgresQueryBuilder),
            drop_target_column().to_string(PostgresQueryBuilder),
        ];
        for statement in &sql {
            assert!(statement.starts_with("ALTER TABLE \"multipart_uploads\" "));
            assert!(!statement.contains("CREATE TABLE"));
            assert!(!statement.contains("DROP TABLE"));
            assert!(!statement.contains("multipart_parts"));
        }
        assert!(sql[0].contains("ADD COLUMN \"decompress_zip_target\" text"));
        assert!(
            sql[1]
                .to_ascii_uppercase()
                .contains("ADD COLUMN \"DECOMPRESS_ZIP_RESULT\" BOOLEAN NOT NULL DEFAULT TRUE"),
            "unexpected PostgreSQL SQL: {}",
            sql[1]
        );
        assert!(sql[2].contains("DROP COLUMN \"decompress_zip_result\""));
        assert!(sql[3].contains("DROP COLUMN \"decompress_zip_target\""));
    }
}
