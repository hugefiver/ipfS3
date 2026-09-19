use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

const DOWN_REFUSAL: &str = "residency schema contains non-reconstructable state";
const BACKFILL_COUNT_MISMATCH: &str = "residency reference backfill count mismatch";

const INDEX_NAMES: [&str; 7] = [
    "uq_object_versions_residency_owner",
    "uq_objects_residency_content",
    "idx_version_residencies_object",
    "idx_version_residencies_physical",
    "idx_physical_residencies_verification",
    "idx_residency_references_physical",
    "idx_residency_references_version",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if !matches!(
            connection.get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            return Err(DbErr::Migration(
                "residency migration supports only SQLite and PostgreSQL".to_owned(),
            ));
        }
        let transaction = connection.begin().await?;
        let result = apply_up(&transaction).await;
        finish_transaction(transaction, result).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        if !matches!(
            connection.get_database_backend(),
            DatabaseBackend::Sqlite | DatabaseBackend::Postgres
        ) {
            return Err(DbErr::Migration(
                "residency down migration supports only SQLite and PostgreSQL".to_owned(),
            ));
        }
        let transaction = connection.begin().await?;
        let result = apply_down(&transaction).await;
        finish_transaction(transaction, result).await
    }
}

async fn finish_transaction(
    transaction: sea_orm::DatabaseTransaction,
    result: Result<(), DbErr>,
) -> Result<(), DbErr> {
    match result {
        Ok(()) => transaction.commit().await,
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

fn timestamp_type(backend: DatabaseBackend) -> &'static str {
    if backend == DatabaseBackend::Postgres {
        "TIMESTAMPTZ"
    } else {
        "TIMESTAMP"
    }
}

fn schema_statements(backend: DatabaseBackend) -> Vec<String> {
    let timestamp = timestamp_type(backend);
    vec![
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_object_versions_residency_owner \
         ON object_versions(id, object_id)"
            .to_owned(),
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_objects_residency_content \
         ON objects(id, cid)"
            .to_owned(),
        format!(
            "CREATE TABLE IF NOT EXISTS physical_residencies (\
                 tier TEXT NOT NULL, \
                 cid TEXT NOT NULL, \
                 node_identity TEXT, \
                 verification_state TEXT NOT NULL, \
                 verification_receipt TEXT, \
                 verified_at {timestamp}, \
                 created_at {timestamp} NOT NULL, \
                 updated_at {timestamp} NOT NULL, \
                 PRIMARY KEY (tier, cid), \
                 CONSTRAINT ck_physical_residencies_tier CHECK (tier IN ('hot', 'cold')), \
                 CONSTRAINT ck_physical_residencies_cid CHECK (length(cid) > 0), \
                 CONSTRAINT ck_physical_residencies_verification_state \
                     CHECK (verification_state IN ('pending', 'verified', 'failed')), \
                 CONSTRAINT ck_physical_residencies_verification_shape CHECK (\
                     (verification_state = 'verified' \
                      AND node_identity IS NOT NULL AND length(node_identity) > 0 \
                      AND verification_receipt IS NOT NULL AND length(verification_receipt) > 0 \
                      AND verified_at IS NOT NULL) \
                     OR \
                     (verification_state IN ('pending', 'failed') \
                      AND node_identity IS NULL AND verification_receipt IS NULL \
                      AND verified_at IS NULL))\
             )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS version_residencies (\
                 version_row_id TEXT PRIMARY KEY NOT NULL, \
                 object_id TEXT NOT NULL, \
                 primary_tier TEXT NOT NULL, \
                 storage_class TEXT NOT NULL, \
                 cid TEXT NOT NULL, \
                 revision BIGINT NOT NULL, \
                 created_at {timestamp} NOT NULL, \
                 updated_at {timestamp} NOT NULL, \
                 CONSTRAINT fk_version_residencies_owner \
                     FOREIGN KEY (version_row_id, object_id) \
                     REFERENCES object_versions(id, object_id) ON DELETE CASCADE, \
                 CONSTRAINT fk_version_residencies_object_content \
                     FOREIGN KEY (object_id, cid) \
                     REFERENCES objects(id, cid) ON DELETE CASCADE, \
                 CONSTRAINT fk_version_residencies_physical \
                     FOREIGN KEY (primary_tier, cid) \
                     REFERENCES physical_residencies(tier, cid), \
                 CONSTRAINT ck_version_residencies_revision CHECK (revision > 0), \
                 CONSTRAINT ck_version_residencies_primary CHECK (\
                     (primary_tier = 'hot' AND storage_class = 'STANDARD') OR \
                     (primary_tier = 'cold' AND storage_class = 'STANDARD_IA'))\
             )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS residency_references (\
                 owner_kind TEXT NOT NULL, \
                 owner_id TEXT NOT NULL, \
                 reason TEXT NOT NULL, \
                 version_row_id TEXT NOT NULL, \
                 object_id TEXT NOT NULL, \
                 tier TEXT NOT NULL, \
                 cid TEXT NOT NULL, \
                 created_at {timestamp} NOT NULL, \
                 PRIMARY KEY (owner_kind, owner_id, reason, tier, cid), \
                 CONSTRAINT fk_residency_references_version \
                     FOREIGN KEY (version_row_id, object_id) \
                     REFERENCES object_versions(id, object_id) ON DELETE CASCADE, \
                 CONSTRAINT fk_residency_references_object_content \
                     FOREIGN KEY (object_id, cid) \
                     REFERENCES objects(id, cid) ON DELETE CASCADE, \
                 CONSTRAINT fk_residency_references_physical \
                     FOREIGN KEY (tier, cid) \
                     REFERENCES physical_residencies(tier, cid), \
                 CONSTRAINT ck_residency_references_owner_kind \
                     CHECK (owner_kind IN ('version', 'transition')), \
                 CONSTRAINT ck_residency_references_reason \
                     CHECK (reason IN ('retained_version', 'transition_staging', \
                                       'transition_cleanup_hold')), \
                 CONSTRAINT ck_residency_references_owner_shape CHECK (\
                     (owner_kind = 'version' AND reason = 'retained_version' \
                      AND owner_id = version_row_id) OR \
                     (owner_kind = 'transition' \
                      AND reason IN ('transition_staging', 'transition_cleanup_hold') \
                      AND length(owner_id) > 0))\
             )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS residency_backfill (\
                 id TEXT PRIMARY KEY NOT NULL, \
                 cursor_version_row_id TEXT, \
                 claim_epoch BIGINT NOT NULL DEFAULT 0, \
                 lease_until {timestamp}, \
                 claimed_by TEXT, \
                 completed BOOLEAN NOT NULL DEFAULT FALSE, \
                 updated_at {timestamp} NOT NULL, \
                 CONSTRAINT ck_residency_backfill_singleton CHECK (id = 'hot_verification'), \
                 CONSTRAINT ck_residency_backfill_cursor \
                     CHECK (cursor_version_row_id IS NULL OR length(cursor_version_row_id) > 0), \
                 CONSTRAINT ck_residency_backfill_epoch CHECK (claim_epoch >= 0), \
                 CONSTRAINT ck_residency_backfill_claim CHECK (\
                     (lease_until IS NULL AND claimed_by IS NULL) OR \
                     (lease_until IS NOT NULL AND claimed_by IS NOT NULL \
                      AND length(claimed_by) > 0))\
             )"
        ),
        "CREATE INDEX IF NOT EXISTS idx_version_residencies_object \
         ON version_residencies(object_id, version_row_id)"
            .to_owned(),
        "CREATE INDEX IF NOT EXISTS idx_version_residencies_physical \
         ON version_residencies(primary_tier, cid, version_row_id)"
            .to_owned(),
        "CREATE INDEX IF NOT EXISTS idx_physical_residencies_verification \
         ON physical_residencies(tier, verification_state, cid)"
            .to_owned(),
        "CREATE INDEX IF NOT EXISTS idx_residency_references_physical \
         ON residency_references(tier, cid, reason, owner_id)"
            .to_owned(),
        "CREATE INDEX IF NOT EXISTS idx_residency_references_version \
         ON residency_references(version_row_id, reason, owner_id)"
            .to_owned(),
    ]
}

async fn execute_statements(
    connection: &impl ConnectionTrait,
    statements: impl IntoIterator<Item = String>,
) -> Result<(), DbErr> {
    for statement in statements {
        connection.execute_unprepared(&statement).await?;
    }
    Ok(())
}

async fn backfill(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    let backend = connection.get_database_backend();
    for sql in [
        "INSERT INTO physical_residencies \
         (tier, cid, verification_state, created_at, updated_at) \
         SELECT 'hot', object.cid, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP \
         FROM object_versions version \
         JOIN objects object ON object.id = version.object_id \
         WHERE version.kind = 'object' \
         GROUP BY object.cid \
         ON CONFLICT (tier, cid) DO NOTHING",
        "INSERT INTO version_residencies \
         (version_row_id, object_id, primary_tier, storage_class, cid, revision, created_at, updated_at) \
         SELECT version.id, object.id, 'hot', 'STANDARD', object.cid, 1, \
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP \
         FROM object_versions version \
         JOIN objects object ON object.id = version.object_id \
         WHERE version.kind = 'object' \
         ON CONFLICT (version_row_id) DO NOTHING",
        "INSERT INTO residency_references \
         (owner_kind, owner_id, reason, version_row_id, object_id, tier, cid, created_at) \
         SELECT 'version', version.id, 'retained_version', version.id, object.id, \
                'hot', object.cid, CURRENT_TIMESTAMP \
         FROM object_versions version \
         JOIN objects object ON object.id = version.object_id \
         WHERE version.kind = 'object' \
         ON CONFLICT (owner_kind, owner_id, reason, tier, cid) DO NOTHING",
        "INSERT INTO residency_backfill \
         (id, claim_epoch, completed, updated_at) \
         VALUES ('hot_verification', 0, FALSE, CURRENT_TIMESTAMP) \
         ON CONFLICT (id) DO NOTHING",
    ] {
        connection
            .execute(Statement::from_string(backend, sql))
            .await?;
    }

    let expected = count(connection, "object_versions version JOIN objects object ON object.id = version.object_id WHERE version.kind = 'object'").await?;
    let residencies = count(connection, "version_residencies").await?;
    let references = count(
        connection,
        "residency_references WHERE reason = 'retained_version'",
    )
    .await?;
    if residencies != expected || references != expected {
        return Err(DbErr::Migration(BACKFILL_COUNT_MISMATCH.to_owned()));
    }
    Ok(())
}

async fn count(connection: &impl ConnectionTrait, from: &str) -> Result<i64, DbErr> {
    connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            format!("SELECT COUNT(*) AS count FROM {from}"),
        ))
        .await?
        .ok_or_else(|| DbErr::Migration("residency count query returned no row".to_owned()))?
        .try_get("", "count")
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    execute_statements(
        connection,
        schema_statements(connection.get_database_backend()),
    )
    .await?;
    backfill(connection).await
}

async fn contains_non_reconstructable_state(
    connection: &impl ConnectionTrait,
) -> Result<bool, DbErr> {
    Ok(connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            "SELECT 1 WHERE \
                 EXISTS (SELECT 1 FROM physical_residencies \
                         WHERE verification_state <> 'pending' OR tier <> 'hot') \
                 OR EXISTS (SELECT 1 FROM version_residencies \
                            WHERE primary_tier <> 'hot' OR storage_class <> 'STANDARD' \
                               OR revision <> 1) \
                 OR EXISTS (SELECT 1 FROM residency_references \
                            WHERE reason <> 'retained_version')",
        ))
        .await?
        .is_some())
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    if connection.get_database_backend() == DatabaseBackend::Postgres {
        // Take the locks DROP itself needs before reading the safety predicate.
        // A weaker write barrier would require a lock upgrade after the check,
        // potentially deadlocking with row-locking readers or another down.
        // Use the residency frontier's version/reference/physical order, then
        // the backfill checkpoint (as in verification writeback). Only these
        // four tables are locked; the migration transaction holds them through
        // the check and DROP. Any lock failure aborts without dropping state.
        connection.execute_unprepared(
            "LOCK TABLE version_residencies, residency_references, physical_residencies, residency_backfill IN ACCESS EXCLUSIVE MODE",
        ).await?;
    }
    if contains_non_reconstructable_state(connection).await? {
        return Err(DbErr::Migration(DOWN_REFUSAL.to_owned()));
    }
    let statements = vec![
        "DROP TABLE residency_backfill".to_owned(),
        "DROP TABLE residency_references".to_owned(),
        "DROP TABLE version_residencies".to_owned(),
        "DROP TABLE physical_residencies".to_owned(),
        format!("DROP INDEX IF EXISTS {}", INDEX_NAMES[0]),
        format!("DROP INDEX IF EXISTS {}", INDEX_NAMES[1]),
    ];
    execute_statements(connection, statements).await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StopAtFirstStatement {
        backend: DatabaseBackend,
        statements: std::sync::Mutex<Vec<String>>,
    }

    impl StopAtFirstStatement {
        fn record(&self, sql: &str) -> DbErr {
            self.statements.lock().unwrap().push(sql.to_owned());
            DbErr::Custom("injected lock/query failure".to_owned())
        }
    }

    #[async_trait::async_trait]
    impl ConnectionTrait for StopAtFirstStatement {
        fn get_database_backend(&self) -> DatabaseBackend {
            self.backend
        }
        async fn execute(&self, statement: Statement) -> Result<sea_orm::ExecResult, DbErr> {
            Err(self.record(&statement.sql))
        }
        async fn execute_unprepared(&self, sql: &str) -> Result<sea_orm::ExecResult, DbErr> {
            Err(self.record(sql))
        }
        async fn query_one(
            &self,
            statement: Statement,
        ) -> Result<Option<sea_orm::QueryResult>, DbErr> {
            Err(self.record(&statement.sql))
        }
        async fn query_all(
            &self,
            statement: Statement,
        ) -> Result<Vec<sea_orm::QueryResult>, DbErr> {
            Err(self.record(&statement.sql))
        }
    }

    #[tokio::test]
    async fn postgres_down_locks_before_check_and_lock_failure_never_drops() {
        let connection = StopAtFirstStatement {
            backend: DatabaseBackend::Postgres,
            statements: std::sync::Mutex::new(Vec::new()),
        };
        assert!(apply_down(&connection).await.is_err());
        let statements = connection.statements.lock().unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0],
            "LOCK TABLE version_residencies, residency_references, physical_residencies, residency_backfill IN ACCESS EXCLUSIVE MODE"
        );
    }

    #[tokio::test]
    async fn sqlite_down_still_checks_without_postgres_table_locks() {
        let connection = StopAtFirstStatement {
            backend: DatabaseBackend::Sqlite,
            statements: std::sync::Mutex::new(Vec::new()),
        };
        assert!(apply_down(&connection).await.is_err());
        let statements = connection.statements.lock().unwrap();
        assert_eq!(statements.len(), 1);
        assert!(statements[0].starts_with("SELECT 1 WHERE"));
    }

    #[test]
    fn postgres_schema_uses_utc_timestamps_and_required_checks() {
        let statements = schema_statements(DatabaseBackend::Postgres);
        let schema = statements.join("\n");
        assert!(schema.contains("TIMESTAMPTZ"));
        assert!(schema.contains("STANDARD_IA"));
        assert!(schema.contains("transition_cleanup_hold"));
        assert!(schema.contains("verification_receipt"));
        for index in INDEX_NAMES {
            assert!(schema.contains(index), "missing index {index}");
        }
    }
}
