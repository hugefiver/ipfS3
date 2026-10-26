use sea_orm_migration::prelude::*;
#[path = "m20261007_000001_legacy_pinning_migration/schema.rs"]
pub(crate) mod schema;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl super::bridge::SharedMigration for Migration {
    async fn up_body(&self, manager: &super::bridge::Manager<'_>) -> Result<(), DbErr> {
        let db = manager.get_connection();
        db.execute_unprepared(schema::OPERATIONS_SQL).await?;
        db.execute_unprepared(schema::ITEMS_SQL).await?;
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Sqlite => {
                db.execute_unprepared(schema::SQLITE_BOUND).await?;
                for table in ["pin_legacy_migrations", "pin_legacy_migration_items"] {
                    for op in ["UPDATE", "DELETE"] {
                        db.execute_unprepared(&schema::sqlite_immutable(table, op))
                            .await?;
                    }
                }
            }
            sea_orm::DatabaseBackend::Postgres => {
                db.execute_unprepared(&format!("CREATE FUNCTION bound_legacy_migration_item() RETURNS trigger LANGUAGE plpgsql AS $$ {} $$",schema::PG_BOUND_BODY)).await?;
                db.execute_unprepared("CREATE TRIGGER pin_legacy_item_bound BEFORE INSERT ON pin_legacy_migration_items FOR EACH ROW EXECUTE FUNCTION bound_legacy_migration_item()").await?;
                db.execute_unprepared(&format!("CREATE FUNCTION reject_legacy_migration_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ {} $$",schema::PG_IMMUTABLE_BODY)).await?;
                for table in ["pin_legacy_migrations", "pin_legacy_migration_items"] {
                    db.execute_unprepared(&format!("CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION reject_legacy_migration_mutation()")).await?;
                }
            }
            _ => {
                return Err(DbErr::Migration(
                    "unsupported legacy migration database".into(),
                ));
            }
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
        let db = manager.get_connection();
        if manager.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
            db.execute_unprepared("LOCK TABLE pin_legacy_migrations, pin_legacy_migration_items IN ACCESS EXCLUSIVE MODE NOWAIT").await?;
        }
        if db
            .query_one(sea_orm::Statement::from_string(
                manager.get_database_backend(),
                "SELECT 1 FROM pin_legacy_migrations LIMIT 1",
            ))
            .await?
            .is_some()
        {
            return Err(DbErr::Migration(
                "legacy migration downgrade would erase receipts".into(),
            ));
        }
        for table in ["pin_legacy_migration_items", "pin_legacy_migrations"] {
            db.execute_unprepared(&format!("DROP TABLE {table}"))
                .await?;
        }
        if manager.get_database_backend() == sea_orm::DatabaseBackend::Postgres {
            db.execute_unprepared("DROP FUNCTION reject_legacy_migration_mutation()")
                .await?;
            db.execute_unprepared("DROP FUNCTION bound_legacy_migration_item()")
                .await?;
        }
        Ok(())
    }
}
