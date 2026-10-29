use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr};
pub(crate) mod schema;

pub(super) async fn install<C: ConnectionTrait>(db: &C) -> Result<(), DbErr> {
    db.execute_unprepared(schema::OPERATIONS_SQL).await?;
    db.execute_unprepared(schema::ITEMS_SQL).await?;
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.execute_unprepared(schema::SQLITE_BOUND).await?;
        for table in [schema::OPERATIONS, schema::ITEMS] {
            for event in ["UPDATE", "DELETE"] {
                db.execute_unprepared(&schema::sqlite_immutable(table, event))
                    .await?;
            }
        }
    } else {
        db.execute_unprepared(&format!("CREATE FUNCTION bound_legacy_migration_item() RETURNS trigger LANGUAGE plpgsql AS $$ {} $$",schema::PG_BOUND_BODY)).await?;
        db.execute_unprepared("CREATE TRIGGER pin_legacy_item_bound BEFORE INSERT ON pin_legacy_migration_items FOR EACH ROW EXECUTE FUNCTION bound_legacy_migration_item()").await?;
        db.execute_unprepared(&format!("CREATE FUNCTION reject_legacy_migration_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ {} $$",schema::PG_IMMUTABLE_BODY)).await?;
        for table in [schema::OPERATIONS, schema::ITEMS] {
            db.execute_unprepared(&format!("CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION reject_legacy_migration_mutation()")).await?;
        }
    }
    Ok(())
}
