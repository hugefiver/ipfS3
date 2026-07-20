use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn add_object_fingerprint_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("objects"))
        .add_column(ColumnDef::new(Alias::new("sse_c_key_fingerprint")).text())
        .to_owned()
}

fn add_upload_fingerprint_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column(ColumnDef::new(Alias::new("sse_c_key_fingerprint")).text())
        .to_owned()
}

fn drop_upload_fingerprint_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("sse_c_key_fingerprint"))
        .to_owned()
}

fn drop_object_fingerprint_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("objects"))
        .drop_column(Alias::new("sse_c_key_fingerprint"))
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager.alter_table(add_object_fingerprint_column()).await?;
        manager.alter_table(add_upload_fingerprint_column()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(drop_upload_fingerprint_column())
            .await?;
        manager.alter_table(drop_object_fingerprint_column()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

    use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;
    use crate::store::migrations::m20260707_000001_decompress_zip::Migration as DecompressZipMigration;

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
        Option<String>,
        bool,
    ) {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT upload_id, object_id, bucket, key, \
                        CAST(created_at AS TEXT) AS created_at_text, encryption_mode, \
                        key_wrap, content_type, metadata, decompress_zip_target, decompress_zip_result \
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
            row.try_get("", "decompress_zip_target").unwrap(),
            row.try_get("", "decompress_zip_result").unwrap(),
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

    #[derive(Debug, PartialEq)]
    struct ObjectSnapshot {
        id: String,
        bucket: String,
        key: String,
        cid: String,
        size: i64,
        content_type: Option<String>,
        etag: String,
        metadata: Option<String>,
        encrypted: bool,
        key_wrap: Option<String>,
        multipart: bool,
        is_latest: bool,
        created_at_text: String,
    }

    async fn object_snapshot(db: &DatabaseConnection) -> ObjectSnapshot {
        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT id, bucket, key, cid, size, content_type, etag, metadata, encrypted, \
                        key_wrap, multipart, is_latest, CAST(created_at AS TEXT) AS created_at_text \
                 FROM objects WHERE id = 'stored-object'",
            ))
            .await
            .unwrap()
            .unwrap();
        ObjectSnapshot {
            id: row.try_get("", "id").unwrap(),
            bucket: row.try_get("", "bucket").unwrap(),
            key: row.try_get("", "key").unwrap(),
            cid: row.try_get("", "cid").unwrap(),
            size: row.try_get("", "size").unwrap(),
            content_type: row.try_get("", "content_type").unwrap(),
            etag: row.try_get("", "etag").unwrap(),
            metadata: row.try_get("", "metadata").unwrap(),
            encrypted: row.try_get("", "encrypted").unwrap(),
            key_wrap: row.try_get("", "key_wrap").unwrap(),
            multipart: row.try_get("", "multipart").unwrap(),
            is_latest: row.try_get("", "is_latest").unwrap(),
            created_at_text: row.try_get("", "created_at_text").unwrap(),
        }
    }

    #[tokio::test]
    async fn migration_up_down_preserves_objects_uploads_parts_and_cascade_on_sqlite() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        let manager = SchemaManager::new(&db);
        InitMigration.up(&manager).await.unwrap();
        DecompressZipMigration.up(&manager).await.unwrap();

        db.execute_unprepared("INSERT INTO buckets (name, owner) VALUES ('bucket', 'owner')")
            .await
            .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects \
             (id, bucket, key, cid, size, content_type, etag, metadata, encrypted, key_wrap, \
              multipart, is_latest) \
             VALUES ('stored-object', 'bucket', 'secure.bin', 'QmObject', 7, \
                     'application/octet-stream', 'QmObject', '{\"source\":\"seed\"}', \
                     TRUE, NULL, FALSE, TRUE)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_uploads \
             (upload_id, object_id, bucket, key, encryption_mode, key_wrap, content_type, metadata, \
              decompress_zip_target, decompress_zip_result) \
             VALUES ('upload-1', 'object-1', 'bucket', 'archive.zip', 'sse_c', 'wrapped', \
                     'application/zip', '{\"source\":\"seed\"}', 'prefix/', FALSE)",
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

        let object_before = object_snapshot(&db).await;
        let upload_before = upload_snapshot(&db).await;
        let part_before = part_snapshot(&db).await;

        Migration.up(&manager).await.unwrap();
        let fingerprint = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT sse_c_key_fingerprint FROM multipart_uploads WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fingerprint
                .try_get::<Option<String>>("", "sse_c_key_fingerprint")
                .unwrap(),
            None
        );
        let object_fingerprint = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT sse_c_key_fingerprint FROM objects WHERE id = 'stored-object'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            object_fingerprint
                .try_get::<Option<String>>("", "sse_c_key_fingerprint")
                .unwrap(),
            None
        );

        Migration.down(&manager).await.unwrap();
        assert_eq!(object_snapshot(&db).await, object_before);
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
    fn postgres_builders_only_alter_objects_and_multipart_uploads() {
        let sql = [
            add_object_fingerprint_column().to_string(PostgresQueryBuilder),
            add_upload_fingerprint_column().to_string(PostgresQueryBuilder),
            drop_upload_fingerprint_column().to_string(PostgresQueryBuilder),
            drop_object_fingerprint_column().to_string(PostgresQueryBuilder),
        ];
        for statement in &sql {
            assert!(statement.starts_with("ALTER TABLE "));
            assert!(!statement.contains("CREATE TABLE"));
            assert!(!statement.contains("DROP TABLE"));
            assert!(!statement.contains("multipart_parts"));
        }
        assert!(sql[0].starts_with("ALTER TABLE \"objects\" "));
        assert!(sql[1].starts_with("ALTER TABLE \"multipart_uploads\" "));
        assert!(sql[2].starts_with("ALTER TABLE \"multipart_uploads\" "));
        assert!(sql[3].starts_with("ALTER TABLE \"objects\" "));
        assert!(sql[0].contains("ADD COLUMN \"sse_c_key_fingerprint\" text"));
        assert!(sql[1].contains("ADD COLUMN \"sse_c_key_fingerprint\" text"));
        assert!(sql[2].contains("DROP COLUMN \"sse_c_key_fingerprint\""));
        assert!(sql[3].contains("DROP COLUMN \"sse_c_key_fingerprint\""));
    }
}
