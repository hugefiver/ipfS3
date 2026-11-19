use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("object_tags"))
                    .if_not_exists()
                    .col(ColumnDef::new(Alias::new("object_id")).text().not_null())
                    .col(ColumnDef::new(Alias::new("key")).text().not_null())
                    .col(ColumnDef::new(Alias::new("value")).text().not_null())
                    .primary_key(
                        Index::create()
                            .name("pk_object_tags")
                            .col(Alias::new("object_id"))
                            .col(Alias::new("key")),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_object_tags_object_id")
                            .from(Alias::new("object_tags"), Alias::new("object_id"))
                            .to(Alias::new("objects"), Alias::new("id"))
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        if !manager.has_column("multipart_uploads", "tags_json").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("multipart_uploads"))
                        .add_column(
                            ColumnDef::new(Alias::new("tags_json"))
                                .text()
                                .not_null()
                                .default("[]"),
                        )
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("object_tags"))
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        if manager.has_column("multipart_uploads", "tags_json").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("multipart_uploads"))
                        .drop_column(Alias::new("tags_json"))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement};

    use crate::store::migrations::m20250701_000001_init::Migration as InitMigration;
    use crate::store::migrations::m20260707_000001_decompress_zip::Migration as DecompressZipMigration;
    use crate::store::migrations::m20260720_000001_sse_c_key_fingerprint::Migration as SseCKeyFingerprintMigration;

    async fn seed_previous_schema(db: &DatabaseConnection) {
        db.execute_unprepared(
            "INSERT INTO buckets (name, created_at, owner) \
             VALUES ('bucket', '2026-07-21 00:00:00+00:00', 'owner')",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO objects \
             (id, bucket, key, cid, size, content_type, etag, metadata, encrypted, key_wrap, \
              multipart, is_latest, created_at, sse_c_key_fingerprint) \
             VALUES ('object-1', 'bucket', 'key', 'QmObject', 7, 'text/plain', 'QmObject', \
                     '{\"source\":\"seed\"}', TRUE, 'wrapped', FALSE, TRUE, \
                     '2026-07-21 00:01:00+00:00', 'fingerprint')",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_uploads \
             (upload_id, object_id, bucket, key, created_at, encryption_mode, key_wrap, \
              sse_c_key_fingerprint, content_type, metadata, decompress_zip_target, \
              decompress_zip_result) \
             VALUES ('upload-1', 'object-1', 'bucket', 'key', '2026-07-21 00:02:00+00:00', \
                     'sse_c', 'wrapped', 'fingerprint', 'text/plain', '{\"source\":\"seed\"}', \
                     'prefix/', FALSE)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_parts (upload_id, part_number, cid, size, etag, uploaded_at) \
             VALUES ('upload-1', 1, 'QmPart', 7, 'QmPart', '2026-07-21 00:03:00+00:00')",
        )
        .await
        .unwrap();
    }

    async fn snapshot_seeded_rows(db: &DatabaseConnection) -> Vec<Vec<String>> {
        let statements = [
            "SELECT name || '|' || CAST(created_at AS TEXT) || '|' || owner FROM buckets WHERE name = 'bucket'",
            "SELECT id || '|' || bucket || '|' || key || '|' || cid || '|' || size || '|' || \
             content_type || '|' || etag || '|' || metadata || '|' || encrypted || '|' || key_wrap || '|' || \
             multipart || '|' || is_latest || '|' || CAST(created_at AS TEXT) || '|' || sse_c_key_fingerprint \
             FROM objects WHERE id = 'object-1'",
            "SELECT upload_id || '|' || object_id || '|' || bucket || '|' || key || '|' || \
             CAST(created_at AS TEXT) || '|' || encryption_mode || '|' || key_wrap || '|' || \
             sse_c_key_fingerprint || '|' || content_type || '|' || metadata || '|' || \
             decompress_zip_target || '|' || decompress_zip_result \
             FROM multipart_uploads WHERE upload_id = 'upload-1'",
            "SELECT upload_id || '|' || part_number || '|' || cid || '|' || size || '|' || etag || '|' || \
             CAST(uploaded_at AS TEXT) FROM multipart_parts WHERE upload_id = 'upload-1'",
        ];

        let mut rows = Vec::new();
        for statement in statements {
            let row = db
                .query_one(Statement::from_string(DatabaseBackend::Sqlite, statement))
                .await
                .unwrap()
                .unwrap();
            let value: String = row.try_get_by(0).unwrap();
            rows.push(vec![value]);
        }
        rows
    }

    #[tokio::test]
    async fn up_down_preserves_seeded_rows_and_multipart_part_cascade_on_sqlite() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        let manager = SchemaManager::new(&db);
        InitMigration.up(&manager).await.unwrap();
        DecompressZipMigration.up(&manager).await.unwrap();
        SseCKeyFingerprintMigration.up(&manager).await.unwrap();
        seed_previous_schema(&db).await;
        let before = snapshot_seeded_rows(&db).await;

        Migration.up(&manager).await.unwrap();
        Migration.up(&manager).await.unwrap();
        let tags = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT tags_json FROM multipart_uploads WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tags.try_get::<String>("", "tags_json").unwrap(), "[]");

        Migration.down(&manager).await.unwrap();
        Migration.down(&manager).await.unwrap();
        assert_eq!(snapshot_seeded_rows(&db).await, before);

        let upload_columns = db
            .query_all(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA table_info(multipart_uploads)",
            ))
            .await
            .unwrap();
        assert!(
            !upload_columns
                .iter()
                .any(|row| { row.try_get::<String>("", "name").unwrap() == "tags_json" })
        );

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
        let part_count: i64 = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT COUNT(*) FROM multipart_parts WHERE upload_id = 'upload-1'",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by(0)
            .unwrap();
        assert_eq!(part_count, 0);
    }
}
