use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn schema(backend: DatabaseBackend) -> Result<Vec<String>, DbErr> {
    let time = match backend {
        DatabaseBackend::Postgres => "TIMESTAMPTZ",
        DatabaseBackend::Sqlite => "TIMESTAMP",
        _ => {
            return Err(DbErr::Migration(
                "ZIP batches require SQLite or PostgreSQL".into(),
            ));
        }
    };
    Ok(vec![
        format!(r#"CREATE TABLE zip_batches (
            id TEXT PRIMARY KEY NOT NULL, owner TEXT NOT NULL, source TEXT NOT NULL,
            token TEXT NOT NULL, fingerprint TEXT NOT NULL, bucket TEXT NOT NULL,
            archive_key TEXT NOT NULL, input_identity TEXT NOT NULL, captured_options TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'open', manifest_prepared BOOLEAN NOT NULL DEFAULT FALSE,
            source_published BOOLEAN NOT NULL DEFAULT FALSE, terminal_result TEXT,
            root_status TEXT NOT NULL DEFAULT 'pending', root_error_code TEXT,
            root_cid TEXT, root_revision BIGINT NOT NULL DEFAULT 0, root_epoch BIGINT NOT NULL DEFAULT 0,
            created_at {time} NOT NULL, updated_at {time} NOT NULL,
            CONSTRAINT ck_zip_batch_identity CHECK (length(id)>0 AND length(owner)>0 AND length(token)>0
                AND length(fingerprint)>0 AND length(bucket)>0 AND length(archive_key)>0
                AND length(input_identity)>0 AND length(captured_options)>0),
            CONSTRAINT ck_zip_batch_source CHECK (source IN ('direct','mpu','import')),
            CONSTRAINT ck_zip_batch_state CHECK (state IN ('open','published')),
            CONSTRAINT ck_zip_batch_root_status CHECK (root_status IN
                ('pending','disabled','empty','failed','complete','partial')),
            CONSTRAINT ck_zip_batch_revision CHECK (root_revision>=0 AND root_epoch>=0),
            CONSTRAINT ck_zip_batch_terminal CHECK ((state='open' AND terminal_result IS NULL) OR
                (state='published' AND terminal_result IS NOT NULL)),
            CONSTRAINT ck_zip_batch_root_shape CHECK (
                (root_status IN ('complete','partial') AND root_cid IS NOT NULL
                    AND length(root_cid)>0 AND root_error_code IS NULL) OR
                (root_status='failed' AND root_cid IS NULL AND root_error_code IS NOT NULL
                    AND length(root_error_code)>0) OR
                (root_status IN ('pending','disabled','empty') AND root_cid IS NULL
                    AND root_error_code IS NULL))
        )"#),
        "CREATE UNIQUE INDEX uq_zip_batch_intent ON zip_batches(owner, source, token)".into(),
        "CREATE INDEX idx_zip_batch_recovery ON zip_batches(state, root_status, id)".into(),
        format!(r#"CREATE TABLE zip_manifest_entries (
            batch_id TEXT NOT NULL REFERENCES zip_batches(id) ON DELETE RESTRICT,
            path TEXT NOT NULL, object_key TEXT, cid TEXT, size BIGINT,
            version_row_id TEXT, error_code TEXT, created_at {time} NOT NULL,
            PRIMARY KEY (batch_id, path),
            CONSTRAINT ck_zip_manifest_path CHECK (length(path)>0),
            CONSTRAINT ck_zip_manifest_shape CHECK (
                (object_key IS NOT NULL AND length(object_key)>0 AND cid IS NOT NULL
                    AND length(cid)>0 AND size IS NOT NULL AND size>=0 AND error_code IS NULL) OR
                (object_key IS NULL AND cid IS NULL AND size IS NULL AND version_row_id IS NULL
                    AND error_code IS NOT NULL AND length(error_code)>0)),
            CONSTRAINT ck_zip_manifest_version CHECK (version_row_id IS NULL OR length(version_row_id)>0)
        )"#),
        "CREATE UNIQUE INDEX uq_zip_manifest_version ON zip_manifest_entries(version_row_id) WHERE version_row_id IS NOT NULL".into(),
        format!(r#"CREATE TABLE zip_root_builds (
            batch_id TEXT NOT NULL REFERENCES zip_batches(id) ON DELETE RESTRICT,
            revision BIGINT NOT NULL, epoch BIGINT NOT NULL, worker TEXT NOT NULL,
            lease_until {time} NOT NULL, status TEXT NOT NULL, error_code TEXT,
            created_at {time} NOT NULL, updated_at {time} NOT NULL,
            PRIMARY KEY (batch_id,revision,epoch),
            CONSTRAINT ck_zip_build_fence CHECK (revision>0 AND epoch>0 AND length(worker)>0),
            CONSTRAINT ck_zip_build_status CHECK (status IN ('intent','reconciling','invoked','unknown','verified','failed')),
            CONSTRAINT ck_zip_build_failure CHECK ((status='failed' AND error_code IS NOT NULL
                AND length(error_code)>0) OR (status<>'failed' AND error_code IS NULL))
        )"#),
        "CREATE INDEX idx_zip_build_recovery ON zip_root_builds(status, lease_until, batch_id)".into(),
        format!(r#"CREATE TABLE zip_root_references (
            batch_id TEXT NOT NULL, revision BIGINT NOT NULL, epoch BIGINT NOT NULL,
            node_identity TEXT NOT NULL, tier TEXT NOT NULL, cid TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'retained', verification_receipt TEXT,
            created_at {time} NOT NULL, updated_at {time} NOT NULL,
            PRIMARY KEY (batch_id, revision, epoch, node_identity, tier, cid),
            CONSTRAINT fk_zip_root_intent FOREIGN KEY (batch_id,revision,epoch)
                REFERENCES zip_root_builds(batch_id,revision,epoch) ON DELETE RESTRICT,
            CONSTRAINT ck_zip_root_identity CHECK (length(node_identity)>0 AND length(cid)>0
                AND tier IN ('hot','cold')),
            CONSTRAINT ck_zip_root_state CHECK (state IN ('retained','adopted')),
            CONSTRAINT ck_zip_root_receipt CHECK (verification_receipt IS NULL OR length(verification_receipt)>0),
            CONSTRAINT ck_zip_root_adopted CHECK (state='retained' OR verification_receipt IS NOT NULL)
        )"#),
        "CREATE UNIQUE INDEX uq_zip_root_adopted ON zip_root_references(batch_id) WHERE state='adopted'".into(),
        "CREATE INDEX idx_zip_root_physical ON zip_root_references(tier,node_identity,cid,state)".into(),
    ])
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let tx = manager.get_connection().begin().await?;
        for sql in schema(manager.get_database_backend())? {
            tx.execute_unprepared(&sql).await?;
        }
        tx.commit().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let connection = manager.get_connection().begin().await?;
        schema(manager.get_database_backend())?;
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            connection.execute_unprepared(
                "LOCK TABLE zip_root_references, zip_root_builds, zip_manifest_entries, zip_batches IN ACCESS EXCLUSIVE MODE"
            ).await?;
        }
        let rows = connection.query_one(Statement::from_string(manager.get_database_backend(),
            "SELECT 1 WHERE EXISTS(SELECT 1 FROM zip_batches) OR EXISTS(SELECT 1 FROM zip_manifest_entries) OR EXISTS(SELECT 1 FROM zip_root_builds) OR EXISTS(SELECT 1 FROM zip_root_references)")).await?;
        if rows.is_some() {
            return Err(DbErr::Migration(
                "ZIP batch downgrade would erase durable owner/recovery evidence".into(),
            ));
        }
        for table in [
            "zip_root_references",
            "zip_root_builds",
            "zip_manifest_entries",
            "zip_batches",
        ] {
            connection
                .execute_unprepared(&format!("DROP TABLE {table}"))
                .await?;
        }
        connection.commit().await
    }
}
