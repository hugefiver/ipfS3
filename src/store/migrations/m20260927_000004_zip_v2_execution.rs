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
                "ZIP v2 execution requires SQLite or PostgreSQL".into(),
            ));
        }
    };
    let fingerprint_hex = if backend == DatabaseBackend::Postgres {
        "request_fingerprint ~ '^[0-9a-f]{64}$'"
    } else {
        "request_fingerprint NOT GLOB '*[^0-9a-f]*'"
    };
    let input_hex = if backend == DatabaseBackend::Postgres {
        "input_sha256 ~ '^[0-9a-f]{64}$'"
    } else {
        "input_sha256 NOT GLOB '*[^0-9a-f]*'"
    };
    Ok(vec![
        format!(r#"CREATE TABLE zip_v2_executions (
            id TEXT PRIMARY KEY NOT NULL, owner TEXT NOT NULL, source TEXT NOT NULL,
            token TEXT NOT NULL, request_fingerprint TEXT NOT NULL, request_contract TEXT NOT NULL,
            bucket TEXT NOT NULL, source_key TEXT NOT NULL, captured_options TEXT NOT NULL,
            input_sha256 TEXT, input_art_cid TEXT, input_art_size BIGINT,
            state TEXT NOT NULL DEFAULT 'pending', epoch BIGINT NOT NULL DEFAULT 0,
            worker TEXT, lease_until {time}, terminal_result TEXT,
            created_at {time} NOT NULL, updated_at {time} NOT NULL,
            CONSTRAINT ck_zip_v2_identity CHECK (length(id)>0 AND length(owner)>0 AND length(token)>0
                AND length(bucket)>0 AND length(source_key)>0 AND length(captured_options)>0
                AND length(request_fingerprint)=64 AND {fingerprint_hex} AND length(request_contract)>0
                AND source IN ('direct','mpu','import')),
            CONSTRAINT ck_zip_v2_input CHECK ((input_sha256 IS NULL AND input_art_cid IS NULL AND input_art_size IS NULL)
                OR (input_sha256 IS NOT NULL AND length(input_sha256)=64 AND {input_hex}
                    AND input_art_cid IS NOT NULL AND length(input_art_cid)>0
                    AND input_art_size IS NOT NULL AND input_art_size>=0)),
            CONSTRAINT ck_zip_v2_state CHECK (state IN ('pending','admitted','completed','fenced')
                AND (state IN ('pending','fenced') OR input_sha256 IS NOT NULL)
                AND ((state IN ('pending','admitted') AND terminal_result IS NULL)
                    OR (state IN ('completed','fenced') AND terminal_result IS NOT NULL AND length(terminal_result)>0))),
            CONSTRAINT ck_zip_v2_lease CHECK (epoch>=0 AND
                ((epoch=0 AND worker IS NULL AND lease_until IS NULL) OR
                 (epoch>0 AND worker IS NOT NULL AND length(worker)>0 AND lease_until IS NOT NULL)))
        )"#),
        "CREATE UNIQUE INDEX uq_zip_v2_intent ON zip_v2_executions(owner,source,token)".into(),
        "CREATE INDEX idx_zip_v2_recovery ON zip_v2_executions(state,lease_until,id)".into(),
        format!(r#"CREATE TABLE zip_v2_manifest (
            batch_id TEXT NOT NULL REFERENCES zip_v2_executions(id) ON DELETE RESTRICT,
            path TEXT NOT NULL, object_key TEXT, cid TEXT, size BIGINT, error_code TEXT,
            created_at {time} NOT NULL, PRIMARY KEY(batch_id,path),
            CONSTRAINT ck_zip_v2_manifest_path CHECK (length(path)>0),
            CONSTRAINT ck_zip_v2_manifest_shape CHECK (
                (object_key IS NOT NULL AND length(object_key)>0 AND cid IS NOT NULL
                    AND length(cid)>0 AND size IS NOT NULL AND size>=0 AND error_code IS NULL) OR
                (object_key IS NULL AND cid IS NULL AND size IS NULL AND error_code IS NOT NULL
                    AND length(error_code)>0))
        )"#),
        "CREATE UNIQUE INDEX uq_zip_v2_manifest_output ON zip_v2_manifest(batch_id,object_key)".into(),
        r#"CREATE TABLE zip_v2_targets (
            batch_id TEXT NOT NULL REFERENCES zip_v2_executions(id) ON DELETE RESTRICT,
            object_key TEXT NOT NULL, mutation_id TEXT NOT NULL, expected_generation BIGINT NOT NULL,
            epoch BIGINT NOT NULL, PRIMARY KEY(batch_id,object_key),
            CONSTRAINT ck_zip_v2_target_guard CHECK (length(object_key)>0 AND length(mutation_id)>0
                AND expected_generation>=0 AND epoch>0),
            CONSTRAINT fk_zip_v2_target_manifest FOREIGN KEY(batch_id,object_key)
                REFERENCES zip_v2_manifest(batch_id,object_key) ON DELETE RESTRICT
        )"#.into(),
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
        schema(manager.get_database_backend())?;
        let tx = manager.get_connection().begin().await?;
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            tx.execute_unprepared("LOCK TABLE zip_v2_targets, zip_v2_manifest, zip_v2_executions IN ACCESS EXCLUSIVE MODE").await?;
        }
        let evidence = tx.query_one(Statement::from_string(manager.get_database_backend(),
            "SELECT 1 WHERE EXISTS(SELECT 1 FROM zip_v2_executions) OR EXISTS(SELECT 1 FROM zip_v2_manifest) OR EXISTS(SELECT 1 FROM zip_v2_targets)"))
            .await?;
        if evidence.is_some() {
            return Err(DbErr::Migration(
                "ZIP v2 execution downgrade would erase durable evidence".into(),
            ));
        }
        for table in ["zip_v2_targets", "zip_v2_manifest", "zip_v2_executions"] {
            tx.execute_unprepared(&format!("DROP TABLE {table}"))
                .await?;
        }
        tx.commit().await
    }
}
