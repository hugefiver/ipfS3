use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        // No backfill: historical raw tags are not executable evidence. A foreign
        // key to the *internal* version row survives ambiguous public null IDs.
        manager.get_connection().execute_unprepared(
            "CREATE UNIQUE INDEX uq_object_versions_decision_owner ON object_versions(id, object_id)",
        ).await?;
        manager.get_connection().execute_unprepared(
            "CREATE TABLE pin_extension_decisions (\
                version_row_id TEXT PRIMARY KEY NOT NULL, \
                object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE, \
                control_revision TEXT NOT NULL UNIQUE, \
                config_revision TEXT NOT NULL, \
                effect TEXT NOT NULL CHECK (effect IN ('accepted', 'skipped', 'no_intent')), \
                snapshot TEXT NOT NULL, \
                CONSTRAINT ck_pin_extension_decisions_revision CHECK (length(config_revision) = 64), \
                CONSTRAINT fk_pin_extension_decisions_owner FOREIGN KEY (version_row_id, object_id) \
                    REFERENCES object_versions(id, object_id) ON DELETE CASCADE\
             )",
        ).await?;
        Ok(())
    }
}
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        super::bridge::SharedMigration::up_body(self, &super::bridge::Manager::native(manager))
            .await
    }
    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Custom(
            "extension decisions cannot be downgraded without losing historical intent evidence"
                .into(),
        ))
    }
}
