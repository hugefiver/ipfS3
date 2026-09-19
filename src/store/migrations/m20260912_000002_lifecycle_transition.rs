use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

const DOWN_REFUSAL: &str = "lifecycle transition schema contains durable state";
const COPY_COUNT_MISMATCH: &str = "lifecycle transition action copy count mismatch";

const ACTION_INDEX_STATEMENTS: [&str; 5] = [
    "CREATE INDEX idx_lifecycle_actions_due \
     ON lifecycle_actions(state, next_attempt_at, due_at, id)",
    "CREATE INDEX idx_lifecycle_actions_reclaim ON lifecycle_actions(state, lease_until, id)",
    "CREATE INDEX idx_lifecycle_actions_bucket_revision \
     ON lifecycle_actions(bucket, config_revision, id)",
    "CREATE INDEX idx_lifecycle_actions_target \
     ON lifecycle_actions(bucket, object_key, target_version_row_id)",
    "CREATE INDEX idx_lifecycle_actions_multipart_target \
     ON lifecycle_actions(bucket, object_key, target_upload_id, target_upload_created_at)",
];

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        ensure_supported(connection.get_database_backend())?;
        let transaction = connection.begin().await?;
        let result = apply_up(&transaction).await;
        finish_transaction(transaction, result).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection();
        ensure_supported(connection.get_database_backend())?;
        let transaction = connection.begin().await?;
        let result = apply_down(&transaction).await;
        finish_transaction(transaction, result).await
    }
}

fn ensure_supported(backend: DatabaseBackend) -> Result<(), DbErr> {
    if matches!(backend, DatabaseBackend::Sqlite | DatabaseBackend::Postgres) {
        Ok(())
    } else {
        Err(DbErr::Migration(
            "lifecycle transition migration supports only SQLite and PostgreSQL".to_owned(),
        ))
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

fn action_table_statement(table: &str, transitions: bool) -> String {
    let transition_kinds = if transitions {
        ", 'transition_current', 'transition_noncurrent'"
    } else {
        ""
    };
    let transition_target_clause = if transitions {
        " OR \
         (action_kind IN ('transition_current', 'transition_noncurrent') \
          AND target_type = 'version' \
          AND length(target_version_row_id) > 0 \
          AND length(target_public_version_id) > 0 \
          AND target_object_id IS NOT NULL AND length(target_object_id) > 0)"
    } else {
        ""
    };
    format!(
        "CREATE TABLE {table} (\
             id TEXT PRIMARY KEY NOT NULL, \
             idempotency_key TEXT NOT NULL UNIQUE, \
             bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE, \
             object_key TEXT NOT NULL, \
             config_revision BIGINT NOT NULL, \
             rule_id TEXT NOT NULL, \
             action_kind TEXT NOT NULL, \
             target_type TEXT NOT NULL, \
             target_version_row_id TEXT, \
             target_public_version_id TEXT, \
             target_object_id TEXT, \
             target_sequence BIGINT, \
             target_upload_id TEXT, \
             target_upload_created_at TIMESTAMP, \
             due_at TIMESTAMP NOT NULL, \
             state TEXT NOT NULL, \
             attempts BIGINT NOT NULL DEFAULT 0, \
             next_attempt_at TIMESTAMP NOT NULL, \
             claim_epoch BIGINT NOT NULL DEFAULT 0, \
             lease_until TIMESTAMP, \
             claimed_by TEXT, \
             failure_class TEXT, \
             last_error_redacted TEXT, \
             created_at TIMESTAMP NOT NULL, \
             updated_at TIMESTAMP NOT NULL, \
             finished_at TIMESTAMP, \
             CONSTRAINT ck_lifecycle_actions_config_revision CHECK (config_revision > 0), \
             CONSTRAINT ck_lifecycle_actions_action_kind CHECK (\
                 action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker', \
                                 'abort_incomplete_multipart_upload'{transition_kinds})), \
             CONSTRAINT ck_lifecycle_actions_target_sequence CHECK (\
                 target_sequence IS NULL OR target_sequence >= 0), \
             CONSTRAINT ck_lifecycle_actions_target_shape CHECK (\
                 (target_type = 'version' \
                  AND target_version_row_id IS NOT NULL \
                  AND target_public_version_id IS NOT NULL \
                  AND target_sequence IS NOT NULL \
                  AND target_upload_id IS NULL \
                  AND target_upload_created_at IS NULL) \
                 OR \
                 (target_type = 'multipart_upload' \
                  AND target_version_row_id IS NULL \
                  AND target_public_version_id IS NULL \
                  AND target_object_id IS NULL \
                  AND target_sequence IS NULL \
                  AND target_upload_id IS NOT NULL \
                  AND target_upload_created_at IS NOT NULL)), \
             CONSTRAINT ck_lifecycle_actions_kind_target CHECK (\
                 (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker') \
                  AND target_type = 'version') \
                 OR \
                 (action_kind = 'abort_incomplete_multipart_upload' \
                  AND target_type = 'multipart_upload'){transition_target_clause}), \
             CONSTRAINT ck_lifecycle_actions_state CHECK (\
                 state IN ('pending', 'claimed', 'succeeded', 'cancelled', 'failed_safe')), \
             CONSTRAINT ck_lifecycle_actions_attempts CHECK (attempts >= 0), \
             CONSTRAINT ck_lifecycle_actions_claim_epoch CHECK (claim_epoch >= 0), \
             CONSTRAINT ck_lifecycle_action_claim CHECK (\
                 (state = 'claimed' AND lease_until IS NOT NULL AND claimed_by IS NOT NULL) OR \
                 (state <> 'claimed' AND lease_until IS NULL AND claimed_by IS NULL)), \
             CONSTRAINT ck_lifecycle_action_terminal CHECK (\
                 (state IN ('succeeded', 'cancelled', 'failed_safe') AND finished_at IS NOT NULL) OR \
                 (state IN ('pending', 'claimed') AND finished_at IS NULL))\
         )"
    )
}

fn transition_table_statement(backend: DatabaseBackend) -> String {
    let timestamp = timestamp_type(backend);
    format!(
        "CREATE TABLE lifecycle_transitions (\
             id TEXT PRIMARY KEY NOT NULL, \
             action_id TEXT NOT NULL UNIQUE REFERENCES lifecycle_actions(id), \
             action_kind TEXT NOT NULL, \
             bucket TEXT NOT NULL REFERENCES buckets(name), \
             object_key TEXT NOT NULL, \
             config_revision BIGINT NOT NULL, \
             rule_id TEXT NOT NULL, \
             target_version_row_id TEXT NOT NULL, \
             target_public_version_id TEXT NOT NULL, \
             target_object_id TEXT NOT NULL, \
             target_sequence BIGINT NOT NULL, \
             source_tier TEXT NOT NULL, \
             destination_tier TEXT NOT NULL, \
             source_cid TEXT NOT NULL, \
             destination_cid TEXT NOT NULL, \
             source_residency_revision BIGINT NOT NULL, \
             expected_source_node_identity TEXT NOT NULL, \
             expected_destination_node_identity TEXT NOT NULL, \
             ownership_generation BIGINT NOT NULL, \
             checkpoint TEXT NOT NULL, \
             verification_receipt TEXT, \
             publication_receipt TEXT, \
             settlement_kind TEXT, \
             created_at {timestamp} NOT NULL, \
             updated_at {timestamp} NOT NULL, \
             completed_at {timestamp}, \
             CONSTRAINT fk_lifecycle_transitions_source_physical \
                 FOREIGN KEY (source_tier, source_cid) \
                 REFERENCES physical_residencies(tier, cid), \
             CONSTRAINT ck_lifecycle_transitions_kind CHECK (\
                 action_kind IN ('transition_current', 'transition_noncurrent')), \
             CONSTRAINT ck_lifecycle_transitions_target CHECK (\
                 config_revision > 0 AND target_sequence >= 0 \
                 AND length(target_version_row_id) > 0 \
                 AND length(target_public_version_id) > 0 \
                 AND length(target_object_id) > 0), \
             CONSTRAINT ck_lifecycle_transitions_direction CHECK (\
                 source_tier = 'hot' AND destination_tier = 'cold' \
                 AND source_cid = destination_cid AND length(source_cid) > 0), \
             CONSTRAINT ck_lifecycle_transitions_residency_revision \
                 CHECK (source_residency_revision > 0), \
             CONSTRAINT ck_lifecycle_transitions_node_identities CHECK (\
                 length(expected_source_node_identity) > 0 \
                 AND length(expected_destination_node_identity) > 0 \
                 AND expected_source_node_identity <> expected_destination_node_identity), \
             CONSTRAINT ck_lifecycle_transitions_ownership_generation \
                 CHECK (ownership_generation > 0), \
             CONSTRAINT ck_lifecycle_transitions_checkpoint CHECK (\
                 checkpoint IN ('prepare', 'copy', 'verify', 'publish', 'cleanup')), \
             CONSTRAINT ck_lifecycle_transitions_receipts CHECK (\
                 (checkpoint IN ('prepare', 'copy') \
                  AND verification_receipt IS NULL AND publication_receipt IS NULL) OR \
                 (checkpoint = 'verify' \
                  AND verification_receipt IS NOT NULL \
                  AND length(verification_receipt) > 0 \
                  AND publication_receipt IS NULL) OR \
                 (checkpoint IN ('publish', 'cleanup') \
                  AND verification_receipt IS NOT NULL \
                  AND length(verification_receipt) > 0 \
                  AND publication_receipt IS NOT NULL \
                  AND length(publication_receipt) > 0)), \
             CONSTRAINT ck_lifecycle_transitions_settlement CHECK (\
                 (settlement_kind IS NULL AND completed_at IS NULL \
                  AND checkpoint <> 'cleanup') OR \
                 (settlement_kind IS NOT NULL AND settlement_kind = 'cancelled' \
                  AND completed_at IS NOT NULL \
                  AND checkpoint IN ('prepare', 'copy', 'verify') \
                  AND publication_receipt IS NULL) OR \
                 (settlement_kind IS NOT NULL AND settlement_kind = 'cleanup_complete' \
                  AND completed_at IS NOT NULL \
                  AND checkpoint = 'cleanup'))\
         )"
    )
}

fn immutable_snapshot_statements(backend: DatabaseBackend) -> Vec<String> {
    match backend {
        DatabaseBackend::Sqlite => vec![
            "CREATE TRIGGER ck_lifecycle_transitions_immutable_snapshot \
             BEFORE UPDATE OF action_id, action_kind, bucket, object_key, config_revision, rule_id, \
                              target_version_row_id, target_public_version_id, target_object_id, \
                              target_sequence, source_tier, destination_tier, source_cid, \
                              destination_cid, source_residency_revision, \
                              expected_source_node_identity, expected_destination_node_identity, \
                              ownership_generation \
             ON lifecycle_transitions \
             BEGIN \
                 SELECT RAISE(ABORT, 'lifecycle transition snapshot is immutable'); \
             END"
                .to_owned(),
        ],
        DatabaseBackend::Postgres => vec![
            "CREATE FUNCTION ipfs_s3_reject_lifecycle_transition_snapshot_update() \
             RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
                 IF ROW(OLD.action_id, OLD.action_kind, OLD.bucket, OLD.object_key, \
                        OLD.config_revision, OLD.rule_id, OLD.target_version_row_id, \
                        OLD.target_public_version_id, OLD.target_object_id, OLD.target_sequence, \
                        OLD.source_tier, OLD.destination_tier, OLD.source_cid, OLD.destination_cid, \
                        OLD.source_residency_revision, OLD.expected_source_node_identity, \
                        OLD.expected_destination_node_identity, OLD.ownership_generation) \
                    IS DISTINCT FROM \
                    ROW(NEW.action_id, NEW.action_kind, NEW.bucket, NEW.object_key, \
                        NEW.config_revision, NEW.rule_id, NEW.target_version_row_id, \
                        NEW.target_public_version_id, NEW.target_object_id, NEW.target_sequence, \
                        NEW.source_tier, NEW.destination_tier, NEW.source_cid, NEW.destination_cid, \
                        NEW.source_residency_revision, NEW.expected_source_node_identity, \
                        NEW.expected_destination_node_identity, NEW.ownership_generation) THEN \
                     RAISE EXCEPTION 'lifecycle transition snapshot is immutable' \
                         USING ERRCODE = '23514'; \
                 END IF; \
                 RETURN NEW; \
             END; \
             $$"
                .to_owned(),
            "CREATE TRIGGER ck_lifecycle_transitions_immutable_snapshot \
             BEFORE UPDATE ON lifecycle_transitions \
             FOR EACH ROW EXECUTE FUNCTION ipfs_s3_reject_lifecycle_transition_snapshot_update()"
                .to_owned(),
        ],
        DatabaseBackend::MySql => Vec::new(),
    }
}

fn guarded_delete_statements(backend: DatabaseBackend) -> Vec<String> {
    match backend {
        DatabaseBackend::Sqlite => vec![
            "CREATE TRIGGER ck_lifecycle_transitions_guarded_delete \
             BEFORE DELETE ON lifecycle_transitions \
             WHEN (\
                 ((OLD.settlement_kind = 'cancelled' \
                   AND EXISTS (SELECT 1 FROM lifecycle_actions \
                               WHERE id = OLD.action_id AND state = 'cancelled')) \
                  OR \
                  (OLD.settlement_kind = 'cleanup_complete' \
                   AND EXISTS (SELECT 1 FROM lifecycle_actions \
                               WHERE id = OLD.action_id AND state = 'succeeded'))) \
                 AND NOT EXISTS (SELECT 1 FROM residency_references \
                                 WHERE owner_kind = 'transition' \
                                   AND owner_id IN (OLD.id, OLD.action_id))\
             ) IS NOT TRUE \
             BEGIN \
                 SELECT RAISE(ABORT, 'lifecycle transition is not safely settled'); \
             END"
            .to_owned(),
        ],
        DatabaseBackend::Postgres => vec![
            "CREATE FUNCTION ipfs_s3_guard_lifecycle_transition_delete() \
             RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
                 IF (\
                     ((OLD.settlement_kind = 'cancelled' \
                       AND EXISTS (SELECT 1 FROM lifecycle_actions \
                                   WHERE id = OLD.action_id AND state = 'cancelled')) \
                      OR \
                      (OLD.settlement_kind = 'cleanup_complete' \
                       AND EXISTS (SELECT 1 FROM lifecycle_actions \
                                   WHERE id = OLD.action_id AND state = 'succeeded'))) \
                     AND NOT EXISTS (SELECT 1 FROM residency_references \
                                     WHERE owner_kind = 'transition' \
                                       AND owner_id IN (OLD.id, OLD.action_id))\
                 ) IS DISTINCT FROM TRUE THEN \
                     RAISE EXCEPTION 'lifecycle transition is not safely settled' \
                         USING ERRCODE = '23514'; \
                 END IF; \
                 RETURN OLD; \
             END; \
             $$"
            .to_owned(),
            "CREATE TRIGGER ck_lifecycle_transitions_guarded_delete \
             BEFORE DELETE ON lifecycle_transitions \
             FOR EACH ROW EXECUTE FUNCTION ipfs_s3_guard_lifecycle_transition_delete()"
                .to_owned(),
        ],
        DatabaseBackend::MySql => Vec::new(),
    }
}

async fn execute_statements(
    connection: &impl ConnectionTrait,
    statements: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), DbErr> {
    for statement in statements {
        connection.execute_unprepared(statement.as_ref()).await?;
    }
    Ok(())
}

async fn count(connection: &impl ConnectionTrait, table: &str) -> Result<i64, DbErr> {
    connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            format!("SELECT COUNT(*) AS count FROM {table}"),
        ))
        .await?
        .ok_or_else(|| DbErr::Migration("lifecycle transition count query returned no row".into()))?
        .try_get("", "count")
}

async fn rebuild_sqlite_actions(
    connection: &impl ConnectionTrait,
    transitions: bool,
) -> Result<(), DbErr> {
    const REBUILD: &str = "lifecycle_actions_transition_rebuild";
    let before = count(connection, "lifecycle_actions").await?;
    connection
        .execute_unprepared(&action_table_statement(REBUILD, transitions))
        .await?;
    connection
        .execute_unprepared(
            "INSERT INTO lifecycle_actions_transition_rebuild (\
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_type, target_version_row_id, target_public_version_id, target_object_id, \
                 target_sequence, target_upload_id, target_upload_created_at, due_at, state, \
                 attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
                 last_error_redacted, created_at, updated_at, finished_at\
             ) SELECT \
                 id, idempotency_key, bucket, object_key, config_revision, rule_id, action_kind, \
                 target_type, target_version_row_id, target_public_version_id, target_object_id, \
                 target_sequence, target_upload_id, target_upload_created_at, due_at, state, \
                 attempts, next_attempt_at, claim_epoch, lease_until, claimed_by, failure_class, \
                 last_error_redacted, created_at, updated_at, finished_at \
             FROM lifecycle_actions",
        )
        .await?;
    if count(connection, REBUILD).await? != before {
        return Err(DbErr::Migration(COPY_COUNT_MISMATCH.to_owned()));
    }
    connection
        .execute_unprepared("DROP TABLE lifecycle_actions")
        .await?;
    connection
        .execute_unprepared(
            "ALTER TABLE lifecycle_actions_transition_rebuild RENAME TO lifecycle_actions",
        )
        .await?;
    execute_statements(connection, ACTION_INDEX_STATEMENTS).await?;
    if count(connection, "lifecycle_actions").await? != before {
        return Err(DbErr::Migration(COPY_COUNT_MISMATCH.to_owned()));
    }
    Ok(())
}

fn postgres_action_check_statements(transitions: bool) -> Vec<String> {
    let transition_kinds = if transitions {
        ", 'transition_current', 'transition_noncurrent'"
    } else {
        ""
    };
    let transition_target_clause = if transitions {
        " OR \
         (action_kind IN ('transition_current', 'transition_noncurrent') \
          AND target_type = 'version' \
          AND length(target_version_row_id) > 0 \
          AND length(target_public_version_id) > 0 \
          AND target_object_id IS NOT NULL AND length(target_object_id) > 0)"
    } else {
        ""
    };
    vec![
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_action_kind".into(),
        "ALTER TABLE lifecycle_actions DROP CONSTRAINT ck_lifecycle_actions_kind_target".into(),
        format!(
            "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_action_kind CHECK (\
                 action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker', \
                                 'abort_incomplete_multipart_upload'{transition_kinds}))"
        ),
        format!(
            "ALTER TABLE lifecycle_actions ADD CONSTRAINT ck_lifecycle_actions_kind_target CHECK (\
                 (action_kind IN ('expire_current', 'expire_noncurrent', 'delete_expired_marker') \
                  AND target_type = 'version') OR \
                 (action_kind = 'abort_incomplete_multipart_upload' \
                  AND target_type = 'multipart_upload'){transition_target_clause})"
        ),
    ]
}

async fn apply_up(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    match connection.get_database_backend() {
        DatabaseBackend::Sqlite => rebuild_sqlite_actions(connection, true).await?,
        DatabaseBackend::Postgres => {
            execute_statements(connection, postgres_action_check_statements(true)).await?
        }
        backend => return ensure_supported(backend),
    }
    execute_statements(
        connection,
        [
            transition_table_statement(connection.get_database_backend()),
            "CREATE INDEX idx_lifecycle_transitions_checkpoint \
             ON lifecycle_transitions(checkpoint, updated_at, id)"
                .to_owned(),
            "CREATE INDEX idx_lifecycle_transitions_target \
             ON lifecycle_transitions(target_version_row_id, target_object_id)"
                .to_owned(),
        ],
    )
    .await?;
    execute_statements(
        connection,
        immutable_snapshot_statements(connection.get_database_backend()),
    )
    .await?;
    execute_statements(
        connection,
        guarded_delete_statements(connection.get_database_backend()),
    )
    .await
}

async fn contains_transition_state(connection: &impl ConnectionTrait) -> Result<bool, DbErr> {
    Ok(connection
        .query_one(Statement::from_string(
            connection.get_database_backend(),
            "SELECT 1 WHERE EXISTS (SELECT 1 FROM lifecycle_transitions) \
             OR EXISTS (SELECT 1 FROM lifecycle_actions \
                        WHERE action_kind IN ('transition_current', 'transition_noncurrent'))",
        ))
        .await?
        .is_some())
}

async fn apply_down(connection: &impl ConnectionTrait) -> Result<(), DbErr> {
    if connection.get_database_backend() == DatabaseBackend::Postgres {
        connection
            .execute_unprepared(
                "LOCK TABLE lifecycle_actions, lifecycle_transitions IN ACCESS EXCLUSIVE MODE",
            )
            .await?;
    }
    if contains_transition_state(connection).await? {
        return Err(DbErr::Migration(DOWN_REFUSAL.to_owned()));
    }
    connection
        .execute_unprepared("DROP TABLE lifecycle_transitions")
        .await?;
    if connection.get_database_backend() == DatabaseBackend::Postgres {
        connection
            .execute_unprepared(
                "DROP FUNCTION ipfs_s3_reject_lifecycle_transition_snapshot_update()",
            )
            .await?;
        connection
            .execute_unprepared("DROP FUNCTION ipfs_s3_guard_lifecycle_transition_delete()")
            .await?;
    }
    match connection.get_database_backend() {
        DatabaseBackend::Sqlite => rebuild_sqlite_actions(connection, false).await,
        DatabaseBackend::Postgres => {
            execute_statements(connection, postgres_action_check_statements(false)).await
        }
        backend => ensure_supported(backend),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_schema_uses_utc_and_fixed_verified_transition_shape() {
        let schema = transition_table_statement(DatabaseBackend::Postgres);
        assert!(schema.contains("TIMESTAMPTZ"));
        assert!(schema.contains("source_tier = 'hot'"));
        assert!(schema.contains("destination_tier = 'cold'"));
        assert!(schema.contains("source_cid = destination_cid"));
        assert!(schema.contains("REFERENCES physical_residencies"));
        assert!(!schema.contains("REFERENCES object_versions"));
        assert!(!schema.contains("REFERENCES objects"));
        assert!(!schema.contains("REFERENCES lifecycle_actions(id) ON DELETE CASCADE"));
        assert!(!schema.contains("REFERENCES buckets(name) ON DELETE CASCADE"));
        assert!(schema.contains("verification_receipt"));
        assert!(schema.contains("publication_receipt"));
        assert!(schema.contains("ownership_generation"));
    }
}
