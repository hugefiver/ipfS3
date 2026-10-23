//! Reconstruct the predecessor import layout in an isolated migration fixture.
use ipfs_s3_gateway::store::entities::import_job;
use sea_orm::{ColumnTrait, ConnectionTrait, Iterable, sea_query::Iden};

pub async fn restore<C: ConnectionTrait>(db: &C) {
    for column in import_job::Column::iter().filter(|column| column.is_request()) {
        let name = Iden::to_string(&column);
        let nullable = if column.def().is_null() {
            ""
        } else {
            " NOT NULL DEFAULT ''"
        };
        db.execute_unprepared(&format!(
            "ALTER TABLE import_jobs ADD COLUMN \"{name}\" TEXT{nullable}"
        ))
        .await
        .unwrap();
        db.execute_unprepared(&format!("UPDATE import_jobs SET \"{name}\"=(SELECT \"{name}\" FROM import_job_requests r WHERE r.job_id=import_jobs.id)"))
            .await
            .unwrap();
    }
    db.execute_unprepared("DROP TABLE import_job_requests")
        .await
        .unwrap();
}
