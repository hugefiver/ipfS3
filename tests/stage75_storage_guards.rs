use ipfs_s3_gateway::store::{self, pinning::management::schema};
use sea_orm::{ConnectionTrait, Database, TransactionTrait};

#[tokio::test]
async fn sqlite_literal_changes_cannot_disable_immutable_storage_guards() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    schema::validate(&db).await.unwrap();
    for state in ["SEALED", "sealed "] {
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("DROP TRIGGER s75_archive_no_update")
            .await
            .unwrap();
        tx.execute_unprepared(&format!(
            "CREATE TRIGGER s75_archive_no_update BEFORE UPDATE ON pin_management_archive WHEN OLD.state='{state}' BEGIN SELECT RAISE(ABORT,'immutable archive'); END"
        ))
        .await
        .unwrap();
        let rejected = schema::validate(&tx).await.is_err();
        tx.rollback().await.unwrap();
        assert!(
            rejected,
            "case-sensitive sealed state must remain protected"
        );
        schema::validate(&db).await.unwrap();
    }
}
