use sea_orm_migration::prelude::*;

const PIN_JOB_SCOPE_CHECK: &str = "((operation IN ('submit', 'poll') AND lease_id IS NOT NULL AND target_id IS NOT NULL AND expected_generation IS NOT NULL AND expected_remote_epoch IS NULL) OR (operation IN ('unpin', 'reconcile') AND lease_id IS NULL AND target_id IS NULL AND expected_generation IS NULL AND expected_remote_epoch IS NOT NULL))";
const PIN_JOB_SUBMIT_PHASE_CHECK: &str = "((operation = 'submit' AND submit_phase IS NOT NULL AND submit_phase IN ('ready', 'calling', 'recovering', 'recovery_backoff')) OR (operation <> 'submit' AND submit_phase IS NULL))";

#[derive(DeriveMigrationName)]
pub struct Migration;

fn required_text(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.text().not_null();
    column
}

fn nullable_text(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.text();
    column
}

fn required_timestamp(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.timestamp_with_time_zone().not_null();
    column
}

fn nullable_timestamp(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.timestamp_with_time_zone();
    column
}

fn required_timestamp_default(name: &str) -> ColumnDef {
    let mut column = required_timestamp(name);
    column.default(Expr::current_timestamp());
    column
}

fn required_big_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.big_integer().not_null();
    column
}

fn nullable_big_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.big_integer();
    column
}

fn required_integer(name: &str) -> ColumnDef {
    let mut column = ColumnDef::new(Alias::new(name));
    column.integer().not_null();
    column
}

fn create_object_tags_table() -> TableCreateStatement {
    let mut object_fk = ForeignKey::create();
    object_fk
        .name("fk_object_tags_object_id")
        .from(Alias::new("object_tags"), Alias::new("object_id"))
        .to(Alias::new("objects"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut table = Table::create();
    table
        .table(Alias::new("object_tags"))
        .if_not_exists()
        .col(required_text("object_id"))
        .col(required_text("key"))
        .col(required_text("value"))
        .primary_key(
            Index::create()
                .name("pk_object_tags")
                .col(Alias::new("object_id"))
                .col(Alias::new("key")),
        )
        .foreign_key(&mut object_fk);
    table.to_owned()
}

fn create_pin_leases_table() -> TableCreateStatement {
    let mut object_fk = ForeignKey::create();
    object_fk
        .name("fk_pin_leases_owner_object_id")
        .from(Alias::new("pin_leases"), Alias::new("owner_object_id"))
        .to(Alias::new("objects"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut id = required_text("id");
    id.primary_key();
    let mut table = Table::create();
    table
        .table(Alias::new("pin_leases"))
        .if_not_exists()
        .col(id)
        .col(required_text("owner_object_id"))
        .col(required_text("source"))
        .col(required_text("policy_id"))
        .col(required_text("provider_mode"))
        .col(required_text("content_mode"))
        .col(required_timestamp("created_at"))
        .col(required_timestamp("last_touched_at"))
        .col(required_timestamp("expires_at"))
        .col(required_big_integer("generation"))
        .col(required_text("state"))
        .foreign_key(&mut object_fk)
        .check(Expr::col(Alias::new("state")).is_in(["active", "expired", "cancelled", "evicted"]));
    table.to_owned()
}

fn create_pin_lease_targets_table() -> TableCreateStatement {
    let mut lease_fk = ForeignKey::create();
    lease_fk
        .name("fk_pin_lease_targets_lease_id")
        .from(Alias::new("pin_lease_targets"), Alias::new("lease_id"))
        .to(Alias::new("pin_leases"), Alias::new("id"))
        .on_delete(ForeignKeyAction::Cascade);

    let mut id = required_text("id");
    id.primary_key();
    let mut table = Table::create();
    table
        .table(Alias::new("pin_lease_targets"))
        .if_not_exists()
        .col(id)
        .col(required_text("lease_id"))
        .col(required_text("cid"))
        .col(required_big_integer("logical_size"))
        .col(required_text("provider"))
        .col(required_text("state"))
        .col(required_timestamp("created_at"))
        .col(required_timestamp("last_touched_at"))
        .foreign_key(&mut lease_fk)
        .check(Expr::col(Alias::new("state")).is_in([
            "waiting",
            "submitted",
            "pinned",
            "degraded",
            "quota_waiting",
            "quota_blocked",
            "evicted",
            "released",
        ]));
    table.to_owned()
}

fn create_remote_pins_table() -> TableCreateStatement {
    let mut table = Table::create();
    table
        .table(Alias::new("remote_pins"))
        .if_not_exists()
        .col(required_text("provider"))
        .col(required_text("cid"))
        .col(nullable_text("request_id"))
        .col(required_big_integer("cid_size"))
        .col(required_text("status"))
        .col(required_big_integer("epoch"))
        .col({
            let mut column = required_integer("failure_attempts");
            column.default(0);
            column
        })
        .col(nullable_timestamp("next_retry_at"))
        .col(nullable_text("last_failed_request_id"))
        .col(required_timestamp("last_touched_at"))
        .col(nullable_text("last_error_class"))
        .col(nullable_text("last_error_text"))
        .primary_key(
            Index::create()
                .name("pk_remote_pins")
                .col(Alias::new("provider"))
                .col(Alias::new("cid")),
        )
        .check(Expr::col(Alias::new("status")).is_in([
            "reserved", "queued", "pinning", "pinned", "failed", "absent",
        ]))
        .check(Expr::col(Alias::new("epoch")).gte(1))
        .check(Expr::col(Alias::new("failure_attempts")).gte(0));
    table.to_owned()
}

fn create_pin_jobs_table() -> TableCreateStatement {
    let mut id = required_text("id");
    id.primary_key();
    let mut table = Table::create();
    table
        .table(Alias::new("pin_jobs"))
        .if_not_exists()
        .col(id)
        .col(required_text("operation"))
        .col(required_text("provider"))
        .col(required_text("cid"))
        .col(nullable_text("lease_id"))
        .col(nullable_text("target_id"))
        .col(nullable_big_integer("expected_generation"))
        .col(nullable_big_integer("expected_remote_epoch"))
        .col({
            let mut column = required_text("state");
            column.default("pending");
            column
        })
        .col({
            let mut column = required_integer("attempts");
            column.default(0);
            column
        })
        .col(required_timestamp("next_attempt_at"))
        .col(nullable_timestamp("locked_until"))
        .col({
            let mut column = nullable_text("submit_phase");
            column.default("ready");
            column
        })
        .col(nullable_text("last_error"))
        .col(required_timestamp_default("created_at"))
        .col(required_timestamp_default("updated_at"))
        .check(Expr::col(Alias::new("state")).is_in(["pending", "running", "done"]))
        .check(Expr::cust(PIN_JOB_SCOPE_CHECK))
        .check(Expr::cust(PIN_JOB_SUBMIT_PHASE_CHECK));
    table.to_owned()
}

fn create_pin_provider_usage_table() -> TableCreateStatement {
    let mut provider = required_text("provider");
    provider.primary_key();
    let mut table = Table::create();
    table
        .table(Alias::new("pin_provider_usage"))
        .if_not_exists()
        .col(provider)
        .col({
            let mut column = required_big_integer("reserved_bytes");
            column.default(0);
            column
        })
        .col({
            let mut column = required_big_integer("reserved_pins");
            column.default(0);
            column
        })
        .col(nullable_big_integer("observed_bytes"))
        .col(nullable_big_integer("observed_pins"))
        .col(nullable_timestamp("observed_at"));
    table.to_owned()
}

fn create_tables() -> [TableCreateStatement; 6] {
    [
        create_object_tags_table(),
        create_pin_leases_table(),
        create_pin_lease_targets_table(),
        create_remote_pins_table(),
        create_pin_jobs_table(),
        create_pin_provider_usage_table(),
    ]
}

fn create_indexes() -> [IndexCreateStatement; 6] {
    [
        Index::create()
            .if_not_exists()
            .name("uq_pin_leases_owner_object_source")
            .table(Alias::new("pin_leases"))
            .col(Alias::new("owner_object_id"))
            .col(Alias::new("source"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("uq_pin_lease_targets_lease_cid_provider")
            .table(Alias::new("pin_lease_targets"))
            .col(Alias::new("lease_id"))
            .col(Alias::new("cid"))
            .col(Alias::new("provider"))
            .unique()
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_pin_jobs_state_next_attempt_locked")
            .table(Alias::new("pin_jobs"))
            .col(Alias::new("state"))
            .col(Alias::new("next_attempt_at"))
            .col(Alias::new("locked_until"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_remote_pins_provider_last_touched")
            .table(Alias::new("remote_pins"))
            .col(Alias::new("provider"))
            .col(Alias::new("last_touched_at"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_pin_lease_targets_provider_cid_state")
            .table(Alias::new("pin_lease_targets"))
            .col(Alias::new("provider"))
            .col(Alias::new("cid"))
            .col(Alias::new("state"))
            .to_owned(),
        Index::create()
            .if_not_exists()
            .name("idx_pin_leases_state_expires")
            .table(Alias::new("pin_leases"))
            .col(Alias::new("state"))
            .col(Alias::new("expires_at"))
            .to_owned(),
    ]
}

fn drop_indexes() -> [IndexDropStatement; 6] {
    [
        Index::drop()
            .if_exists()
            .name("idx_pin_jobs_state_next_attempt_locked")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_pin_lease_targets_provider_cid_state")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("uq_pin_lease_targets_lease_cid_provider")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_pin_leases_state_expires")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("uq_pin_leases_owner_object_source")
            .to_owned(),
        Index::drop()
            .if_exists()
            .name("idx_remote_pins_provider_last_touched")
            .to_owned(),
    ]
}

fn drop_tables() -> [TableDropStatement; 6] {
    [
        Table::drop()
            .table(Alias::new("pin_lease_targets"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("object_tags"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("pin_jobs"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("remote_pins"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("pin_leases"))
            .if_exists()
            .to_owned(),
        Table::drop()
            .table(Alias::new("pin_provider_usage"))
            .if_exists()
            .to_owned(),
    ]
}

fn add_multipart_tags_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .add_column({
            let mut column = required_text("tags_json");
            column.default("[]");
            column
        })
        .to_owned()
}

fn drop_multipart_tags_column() -> TableAlterStatement {
    Table::alter()
        .table(Alias::new("multipart_uploads"))
        .drop_column(Alias::new("tags_json"))
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for table in create_tables() {
            manager.create_table(table).await?;
        }
        for index in create_indexes() {
            manager.create_index(index).await?;
        }
        if !manager.has_column("multipart_uploads", "tags_json").await? {
            manager.alter_table(add_multipart_tags_column()).await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index in drop_indexes() {
            manager.drop_index(index).await?;
        }
        for table in drop_tables() {
            manager.drop_table(table).await?;
        }
        if manager.has_column("multipart_uploads", "tags_json").await? {
            manager.alter_table(drop_multipart_tags_column()).await?;
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

    const PINNING_TABLES: [&str; 6] = [
        "object_tags",
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "pin_jobs",
        "pin_provider_usage",
    ];

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

    async fn sqlite_table_names(db: &DatabaseConnection) -> Vec<String> {
        db.query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE type = 'table' \
             AND name IN ('object_tags', 'pin_leases', 'pin_lease_targets', \
                          'remote_pins', 'pin_jobs', 'pin_provider_usage') \
             ORDER BY name",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let name: String = row.try_get_by(0).unwrap();
            name
        })
        .collect()
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
        let mut expected_tables = PINNING_TABLES.map(str::to_owned).to_vec();
        expected_tables.sort();
        assert_eq!(sqlite_table_names(&db).await, expected_tables);
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
        assert!(sqlite_table_names(&db).await.is_empty());
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

    #[tokio::test]
    async fn deleting_bucket_cascades_objects_leases_and_targets_on_sqlite() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        let manager = SchemaManager::new(&db);
        InitMigration.up(&manager).await.unwrap();
        DecompressZipMigration.up(&manager).await.unwrap();
        SseCKeyFingerprintMigration.up(&manager).await.unwrap();
        seed_previous_schema(&db).await;
        Migration.up(&manager).await.unwrap();

        db.execute_unprepared(
            "INSERT INTO pin_leases \
             (id, owner_object_id, source, policy_id, provider_mode, content_mode, created_at, \
              last_touched_at, expires_at, generation, state) \
             VALUES ('lease-1', 'object-1', 'historical', 'policy', 'all', 'full', \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, 'expired')",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO pin_lease_targets \
             (id, lease_id, cid, logical_size, provider, state, created_at, last_touched_at) \
             VALUES ('target-1', 'lease-1', 'QmTarget', 7, 'pinata', 'released', \
                     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        db.execute_unprepared("DELETE FROM buckets WHERE name = 'bucket'")
            .await
            .unwrap();
        for table in ["objects", "pin_leases", "pin_lease_targets"] {
            let count: i64 = db
                .query_one(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!("SELECT COUNT(*) FROM {table}"),
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get_by(0)
                .unwrap();
            assert_eq!(count, 0, "{table} must be deleted with its bucket");
        }
    }

    #[test]
    fn postgres_rendering_covers_schema_indexes_and_reversible_alter() {
        let creates: Vec<String> = create_tables()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        assert_eq!(creates.len(), PINNING_TABLES.len());
        for table in PINNING_TABLES {
            assert_eq!(
                creates
                    .iter()
                    .filter(|statement| {
                        statement.starts_with(&format!("CREATE TABLE IF NOT EXISTS \"{table}\""))
                    })
                    .count(),
                1,
                "expected exactly one PostgreSQL create statement for {table}: {creates:#?}"
            );
        }

        let object_tags = creates
            .iter()
            .find(|statement| statement.contains("\"object_tags\""))
            .unwrap();
        assert!(object_tags.contains("PRIMARY KEY (\"object_id\", \"key\")"));
        assert!(object_tags.contains(
            "FOREIGN KEY (\"object_id\") REFERENCES \"objects\" (\"id\") ON DELETE CASCADE"
        ));

        let leases = creates
            .iter()
            .find(|statement| statement.contains("\"pin_leases\""))
            .unwrap();
        assert!(leases.contains("\"generation\" bigint NOT NULL"));
        assert!(leases.contains(
            "FOREIGN KEY (\"owner_object_id\") REFERENCES \"objects\" (\"id\") ON DELETE CASCADE"
        ));
        assert!(
            leases.contains("CHECK (\"state\" IN ('active', 'expired', 'cancelled', 'evicted'))")
        );

        let targets = creates
            .iter()
            .find(|statement| statement.contains("\"pin_lease_targets\""))
            .unwrap();
        assert!(targets.contains("\"logical_size\" bigint NOT NULL"));
        assert!(targets.contains("CHECK (\"state\" IN ('waiting', 'submitted', 'pinned', 'degraded', 'quota_waiting', 'quota_blocked', 'evicted', 'released'))"));

        let remote = creates
            .iter()
            .find(|statement| statement.contains("\"remote_pins\""))
            .unwrap();
        for required in [
            "PRIMARY KEY (\"provider\", \"cid\")",
            "\"request_id\" text",
            "\"cid_size\" bigint NOT NULL",
            "\"next_retry_at\" timestamp with time zone",
            "\"last_failed_request_id\" text",
            "\"last_error_class\" text",
            "\"last_error_text\" text",
            "\"failure_attempts\" integer NOT NULL DEFAULT 0",
            "CHECK (\"epoch\" >= 1)",
            "CHECK (\"failure_attempts\" >= 0)",
            "CHECK (\"status\" IN ('reserved', 'queued', 'pinning', 'pinned', 'failed', 'absent'))",
        ] {
            assert!(remote.contains(required), "missing {required} in {remote}");
        }

        let jobs = creates
            .iter()
            .find(|statement| statement.contains("\"pin_jobs\""))
            .unwrap();
        for required in [
            "\"next_attempt_at\" timestamp with time zone NOT NULL",
            "\"expected_remote_epoch\" bigint",
            "\"submit_phase\" text DEFAULT 'ready'",
            "CHECK (\"state\" IN ('pending', 'running', 'done'))",
            PIN_JOB_SCOPE_CHECK,
            PIN_JOB_SUBMIT_PHASE_CHECK,
        ] {
            assert!(jobs.contains(required), "missing {required} in {jobs}");
        }

        let usage = creates
            .iter()
            .find(|statement| statement.contains("\"pin_provider_usage\""))
            .unwrap();
        assert!(usage.contains("\"reserved_bytes\" bigint NOT NULL DEFAULT 0"));
        assert!(usage.contains("\"observed_at\" timestamp with time zone"));

        let indexes: Vec<String> = create_indexes()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        for required in [
            "CREATE UNIQUE INDEX IF NOT EXISTS \"uq_pin_leases_owner_object_source\" ON \"pin_leases\" (\"owner_object_id\", \"source\")",
            "CREATE UNIQUE INDEX IF NOT EXISTS \"uq_pin_lease_targets_lease_cid_provider\" ON \"pin_lease_targets\" (\"lease_id\", \"cid\", \"provider\")",
            "CREATE INDEX IF NOT EXISTS \"idx_pin_jobs_state_next_attempt_locked\" ON \"pin_jobs\" (\"state\", \"next_attempt_at\", \"locked_until\")",
            "CREATE INDEX IF NOT EXISTS \"idx_remote_pins_provider_last_touched\" ON \"remote_pins\" (\"provider\", \"last_touched_at\")",
            "CREATE INDEX IF NOT EXISTS \"idx_pin_lease_targets_provider_cid_state\" ON \"pin_lease_targets\" (\"provider\", \"cid\", \"state\")",
            "CREATE INDEX IF NOT EXISTS \"idx_pin_leases_state_expires\" ON \"pin_leases\" (\"state\", \"expires_at\")",
        ] {
            assert!(
                indexes.iter().any(|statement| statement == required),
                "missing PostgreSQL index SQL: {required}; actual: {indexes:#?}"
            );
        }

        let drops: Vec<String> = drop_tables()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        assert_eq!(drops.len(), PINNING_TABLES.len());
        for table in PINNING_TABLES {
            assert!(
                drops
                    .iter()
                    .any(|statement| statement == &format!("DROP TABLE IF EXISTS \"{table}\"")),
                "missing PostgreSQL drop statement for {table}: {drops:#?}"
            );
        }
        let index_drops: Vec<String> = drop_indexes()
            .iter()
            .map(|statement| statement.to_string(PostgresQueryBuilder))
            .collect();
        assert_eq!(index_drops.len(), indexes.len());
        for index in [
            "idx_pin_jobs_state_next_attempt_locked",
            "idx_pin_lease_targets_provider_cid_state",
            "uq_pin_lease_targets_lease_cid_provider",
            "idx_pin_leases_state_expires",
            "uq_pin_leases_owner_object_source",
            "idx_remote_pins_provider_last_touched",
        ] {
            assert!(
                index_drops
                    .iter()
                    .any(|statement| statement == &format!("DROP INDEX IF EXISTS \"{index}\"")),
                "missing PostgreSQL drop index for {index}: {index_drops:#?}"
            );
        }

        let add_tags = add_multipart_tags_column().to_string(PostgresQueryBuilder);
        let drop_tags = drop_multipart_tags_column().to_string(PostgresQueryBuilder);
        assert_eq!(
            add_tags,
            "ALTER TABLE \"multipart_uploads\" ADD COLUMN \"tags_json\" text NOT NULL DEFAULT '[]'"
        );
        assert_eq!(
            drop_tags,
            "ALTER TABLE \"multipart_uploads\" DROP COLUMN \"tags_json\""
        );
    }
}
