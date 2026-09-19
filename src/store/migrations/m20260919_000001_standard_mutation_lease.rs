//! Upgrade requires ALL old writers to be stopped/drained before migration.
//! Legacy tokens are preserved and quarantined for 120 database-clock seconds,
//! not unconditionally cleared. They cannot publish through the new fence after
//! expiry. Never run old writers alongside this schema: they bypass the lease.
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let timestamp = match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Postgres => "TIMESTAMPTZ",
            _ => "TEXT",
        };
        db.execute_unprepared(&format!(
            "CREATE TABLE standard_mutation_leases (\
             bucket TEXT NOT NULL, key TEXT NOT NULL, mutation_id TEXT NOT NULL, \
             generation BIGINT NOT NULL, lease_until {timestamp} NOT NULL, \
             PRIMARY KEY (bucket, key), \
             FOREIGN KEY (bucket, key) REFERENCES import_destinations(bucket, key) ON DELETE CASCADE)"
        )).await?;
        let deadline = match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Postgres => "clock_timestamp() + INTERVAL '120 seconds'",
            _ => "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+120 seconds')",
        };
        db.execute_unprepared(&format!(
            "INSERT INTO standard_mutation_leases (bucket, key, mutation_id, generation, lease_until) \
             SELECT bucket, key, mutation_id, generation, {deadline} \
             FROM import_destinations WHERE mutation_id IS NOT NULL"
        )).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE standard_mutation_leases")
            .await?;
        Ok(())
    }
}
