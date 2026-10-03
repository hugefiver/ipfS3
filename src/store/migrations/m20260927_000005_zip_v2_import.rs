use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement, TransactionTrait};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const TABLE: &str = r#"CREATE TABLE zip_v2_import_requests (
    batch_id TEXT PRIMARY KEY NOT NULL REFERENCES zip_v2_executions(id) ON DELETE RESTRICT,
    owner TEXT NOT NULL, bucket TEXT NOT NULL, source_key TEXT NOT NULL, client_token TEXT NOT NULL,
    prefix TEXT NOT NULL, source_descriptor TEXT NOT NULL, expected_sha256 TEXT,
    request_contract TEXT NOT NULL, job_state TEXT NOT NULL DEFAULT 'pending',
    receipt_metadata TEXT,
    CHECK (length(owner)>0 AND length(bucket)>0 AND length(source_key)>0 AND length(client_token)>0
        AND length(prefix)>0 AND length(source_descriptor)>0 AND length(request_contract)>0),
    CHECK (job_state IN ('pending','failed','ready')),
    CHECK ((job_state='pending' AND receipt_metadata IS NULL) OR job_state<>'pending')
)"#;

fn statements(backend: DatabaseBackend) -> Result<Vec<String>, DbErr> {
    let expected_hex = if backend == DatabaseBackend::Postgres {
        "expected_sha256 ~ '^[0-9a-f]{64}$'"
    } else {
        "expected_sha256 NOT GLOB '*[^0-9a-f]*'"
    };
    let table = TABLE.replace(
        "CHECK (job_state IN ('pending','failed','ready'))",
        &format!("CHECK (expected_sha256 IS NULL OR (length(expected_sha256)=64 AND {expected_hex})), CHECK (job_state IN ('pending','failed','ready'))"),
    );
    match backend {
        DatabaseBackend::Sqlite => Ok(vec![
            &table,
            "CREATE UNIQUE INDEX uq_zip_v2_import_token ON zip_v2_import_requests(owner,client_token)",
            "CREATE INDEX idx_zip_v2_import_pending ON zip_v2_import_requests(job_state,batch_id)",
            "CREATE TRIGGER zip_v2_import_legacy_conflict BEFORE INSERT ON zip_v2_import_requests WHEN EXISTS (SELECT 1 FROM import_jobs WHERE bucket=NEW.bucket AND key=NEW.source_key AND client_token=NEW.client_token) BEGIN SELECT RAISE(ABORT, 'zip_v2_import_token_conflict'); END",
            "CREATE TRIGGER zip_v2_legacy_import_conflict BEFORE INSERT ON import_jobs WHEN NEW.client_token IS NOT NULL AND EXISTS (SELECT 1 FROM zip_v2_import_requests WHERE bucket=NEW.bucket AND source_key=NEW.key AND client_token=NEW.client_token) BEGIN SELECT RAISE(ABORT, 'zip_v2_import_token_conflict'); END",
            "CREATE TRIGGER zip_v2_legacy_import_update_conflict BEFORE UPDATE OF bucket,key,client_token ON import_jobs WHEN NEW.client_token IS NOT NULL AND EXISTS (SELECT 1 FROM zip_v2_import_requests WHERE bucket=NEW.bucket AND source_key=NEW.key AND client_token=NEW.client_token) BEGIN SELECT RAISE(ABORT, 'zip_v2_import_token_conflict'); END",
        ].into_iter().map(str::to_owned).collect()),
        DatabaseBackend::Postgres => Ok(vec![
            &table,
            "CREATE UNIQUE INDEX uq_zip_v2_import_token ON zip_v2_import_requests(owner,client_token)",
            "CREATE INDEX idx_zip_v2_import_pending ON zip_v2_import_requests(job_state,batch_id)",
            // Both writers acquire the bucket lock before inserting. The triggers
            // enforce the invariant even for other DB clients that follow that lock.
            "CREATE FUNCTION zip_v2_import_guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF TG_TABLE_NAME='import_jobs' THEN IF NEW.client_token IS NOT NULL AND EXISTS (SELECT 1 FROM zip_v2_import_requests WHERE bucket=NEW.bucket AND source_key=NEW.key AND client_token=NEW.client_token) THEN RAISE EXCEPTION 'zip_v2_import_token_conflict'; END IF; ELSE IF EXISTS (SELECT 1 FROM import_jobs WHERE bucket=NEW.bucket AND key=NEW.source_key AND client_token=NEW.client_token) THEN RAISE EXCEPTION 'zip_v2_import_token_conflict'; END IF; END IF; RETURN NEW; END $$",
            "CREATE TRIGGER zip_v2_import_legacy_conflict BEFORE INSERT ON zip_v2_import_requests FOR EACH ROW EXECUTE FUNCTION zip_v2_import_guard()",
            "CREATE TRIGGER zip_v2_legacy_import_conflict BEFORE INSERT OR UPDATE OF bucket,key,client_token ON import_jobs FOR EACH ROW EXECUTE FUNCTION zip_v2_import_guard()",
        ].into_iter().map(str::to_owned).collect()),
        _ => Err(DbErr::Migration("ZIP v2 import requires SQLite or PostgreSQL".into())),
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let tx = manager.get_connection().begin().await?;
        for sql in statements(manager.get_database_backend())? {
            tx.execute_unprepared(&sql).await?;
        }
        tx.commit().await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        statements(backend)?;
        let tx = manager.get_connection().begin().await?;
        if backend == DatabaseBackend::Postgres {
            tx.execute_unprepared(
                "LOCK TABLE zip_v2_import_requests, import_jobs IN ACCESS EXCLUSIVE MODE",
            )
            .await?;
        } else {
            tx.execute_unprepared(
                "UPDATE zip_v2_import_requests SET job_state=job_state WHERE 1=0",
            )
            .await?;
        }
        let evidence = tx
            .query_one(Statement::from_string(
                backend,
                "SELECT 1 FROM zip_v2_import_requests LIMIT 1",
            ))
            .await?;
        if evidence.is_some() {
            return Err(DbErr::Migration(
                "ZIP v2 import downgrade would erase durable requests".into(),
            ));
        }
        if backend == DatabaseBackend::Postgres {
            tx.execute_unprepared(
                "DROP TRIGGER zip_v2_import_legacy_conflict ON zip_v2_import_requests",
            )
            .await?;
            tx.execute_unprepared("DROP TRIGGER zip_v2_legacy_import_conflict ON import_jobs")
                .await?;
            tx.execute_unprepared("DROP FUNCTION zip_v2_import_guard()")
                .await?;
        } else {
            for trigger in [
                "zip_v2_import_legacy_conflict",
                "zip_v2_legacy_import_conflict",
                "zip_v2_legacy_import_update_conflict",
            ] {
                tx.execute_unprepared(&format!("DROP TRIGGER {trigger}"))
                    .await?;
            }
        }
        tx.execute_unprepared("DROP TABLE zip_v2_import_requests")
            .await?;
        tx.commit().await
    }
}
