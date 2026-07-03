use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // buckets
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS buckets (
                name TEXT PRIMARY KEY NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                owner TEXT
            )"#,
            )
            .await?;

        // objects
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS objects (
                id TEXT PRIMARY KEY NOT NULL,
                bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                key TEXT NOT NULL,
                cid TEXT NOT NULL,
                size BIGINT NOT NULL,
                content_type TEXT,
                etag TEXT NOT NULL,
                metadata TEXT,
                encrypted BOOLEAN NOT NULL DEFAULT FALSE,
                key_wrap TEXT,
                multipart BOOLEAN NOT NULL DEFAULT FALSE,
                is_latest BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(bucket, key, id)
            )"#,
            )
            .await?;

        // Partial unique index: only one latest per (bucket, key)
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_objects_latest
               ON objects (bucket, key) WHERE is_latest = TRUE"#,
            )
            .await?;

        // multipart_uploads
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS multipart_uploads (
                upload_id TEXT PRIMARY KEY NOT NULL,
                object_id TEXT NOT NULL,
                bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
                key TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                encryption_mode TEXT NOT NULL DEFAULT 'none',
                key_wrap TEXT,
                content_type TEXT,
                metadata TEXT
            )"#,
            )
            .await?;

        // multipart_parts
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE TABLE IF NOT EXISTS multipart_parts (
                upload_id TEXT NOT NULL REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
                part_number INTEGER NOT NULL,
                cid TEXT NOT NULL,
                size BIGINT NOT NULL,
                etag TEXT NOT NULL,
                uploaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (upload_id, part_number)
            )"#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS multipart_parts"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS multipart_uploads"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS objects"#)
            .await?;
        manager
            .get_connection()
            .execute_unprepared(r#"DROP TABLE IF EXISTS buckets"#)
            .await?;
        Ok(())
    }
}
