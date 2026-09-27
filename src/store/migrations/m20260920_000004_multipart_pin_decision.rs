use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // NULL is intentionally unknown: legacy uploads have no captured authority,
        // regardless of the reserved tags they may contain.
        let column_type = if manager.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
            "JSONB"
        } else {
            "TEXT"
        };
        manager
            .get_connection()
            .execute_unprepared(&format!(
                "ALTER TABLE multipart_uploads ADD COLUMN pin_decision_json {column_type}"
            ))
            .await?;
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "multipart decisions cannot be downgraded without losing pending upload intent".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{ConnectionTrait, Database, Statement};

    #[tokio::test]
    async fn upgrade_leaves_historical_control_tags_without_a_decision() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(
            "CREATE TABLE multipart_uploads (upload_id TEXT PRIMARY KEY, tags_json TEXT NOT NULL)",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO multipart_uploads (upload_id, tags_json) VALUES \
             ('legacy', '[{\"key\":\"ipfs-s3:pin\",\"value\":\"true\"}]')",
        )
        .await
        .unwrap();
        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap();
        let row = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT tags_json, pin_decision_json FROM multipart_uploads WHERE upload_id = 'legacy'"
                    .to_owned(),
            ))
            .await
            .unwrap()
            .unwrap();
        let tags: String = row.try_get("", "tags_json").unwrap();
        let decision: Option<String> = row.try_get("", "pin_decision_json").unwrap();
        assert!(tags.contains("ipfs-s3:pin"));
        assert!(decision.is_none());
        assert!(Migration.down(&manager).await.is_err());
    }
}
