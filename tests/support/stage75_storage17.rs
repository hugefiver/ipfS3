//! Isolated predecessor layout with real runtime-generated provider originals.
//! Archive bodies and action originals are retained, not reconstructed as legacy DTOs.
#[path = "stage75_legacy_import.rs"]
mod legacy_import;
use ipfs_s3_gateway::store::pinning::management::{records::segmented::ArchiveHeader, schema};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use sha2::{Digest, Sha256};

pub async fn restore(db: &DatabaseConnection) {
    assert_eq!(db.get_database_backend(), DatabaseBackend::Sqlite);
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("DROP TRIGGER s75_archive_id_segmented; DROP TRIGGER s75_archive_id_inline; DROP TABLE pin_management_archive_identity").await.unwrap();
    let mut after = String::new();
    loop {
        let row = tx.query_one(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "SELECT archive_id,CASE WHEN length(CAST(header AS BLOB))+length(payload)<=1048576 THEN header END AS header,payload FROM pin_management_inline_archive WHERE archive_id>$1 ORDER BY archive_id LIMIT 1", [after.clone().into()])).await.unwrap();
        let Some(row) = row else {
            break;
        };
        let id: String = row.try_get("", "archive_id").unwrap();
        let raw: String = row.try_get("", "header").unwrap();
        let payload: Vec<u8> = row.try_get("", "payload").unwrap();
        let header: ArchiveHeader = serde_json::from_str(&raw).unwrap();
        let mut digest = Sha256::new();
        digest.update(b"S75A1\0");
        digest.update((raw.len() as u64).to_be_bytes());
        digest.update(raw.as_bytes());
        tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES($1,'S75A1',$2,$3,$4,'staging')",
            [id.clone().into(),header.database_scope.into(),header.database_identity.into(),raw.into()])).await.unwrap();
        if !payload.is_empty() {
            digest.update(0_u64.to_be_bytes());
            digest.update((payload.len() as u64).to_be_bytes());
            digest.update(&payload);
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "INSERT INTO pin_management_archive_chunks(archive_id,ordinal,payload) VALUES($1,0,$2)",
                [id.clone().into(),payload.clone().into()])).await.unwrap();
        }
        tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE pin_management_archive SET state='sealed',chunks=$1,bytes=$2,digest=$3 WHERE archive_id=$4",
            [i64::from(!payload.is_empty()).into(),(payload.len() as i64).into(),hex::encode(digest.finalize()).into(),id.clone().into()])).await.unwrap();
        after = id;
    }
    tx.execute_unprepared("DROP TABLE pin_management_inline_archive")
        .await
        .unwrap();
    legacy_import::restore(&tx).await;
    tx.execute_unprepared("DELETE FROM pin_management_revision WHERE source_table IN ('import_job_requests','pin_management_inline_archive')").await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM seaql_migrations WHERE version=$1",
        [schema::VERSION.into()],
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
}
