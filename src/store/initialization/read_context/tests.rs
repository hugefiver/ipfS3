use super::*;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, TransactionTrait};
use std::sync::Arc;

async fn backfill_database(path: &std::path::Path) -> DatabaseConnection {
    let mut options = ConnectOptions::new(format!(
        "sqlite://{}?mode=rwc",
        path.to_string_lossy().replace('\\', "/")
    ));
    options.max_connections(2).min_connections(2);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    db.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    db
}

#[tokio::test]
async fn conversion_read_capability_is_exact_and_does_not_escape_its_task() {
    let directory = tempfile::tempdir().unwrap();
    let db = backfill_database(&directory.path().join("source.sqlite")).await;
    let foreign = backfill_database(&directory.path().join("foreign.sqlite")).await;
    let tx = Arc::new(db.begin().await.unwrap());
    let another = db.begin().await.unwrap();
    let foreign_tx = foreign.begin().await.unwrap();
    let (scope, identity) = native::scope::binding(tx.as_ref()).await.unwrap();
    assert!(!active(tx.as_ref()).await.unwrap());
    run(tx.as_ref(), &scope, &identity, async {
        assert!(active(tx.as_ref()).await?);
        assert!(!active(&another).await?);
        assert!(!active(&foreign_tx).await?);
        assert!(schema::validate(tx.as_ref()).await.is_err());
        let same_transaction = tx.clone();
        assert!(
            !tokio::spawn(async move { active(same_transaction.as_ref()).await })
                .await
                .unwrap()?
        );
        assert!(active(tx.as_ref()).await?);
        Ok(())
    })
    .await
    .unwrap();
    assert!(!active(tx.as_ref()).await.unwrap());
    let failed: Result<()> = run(tx.as_ref(), &scope, &identity, async {
        assert!(active(tx.as_ref()).await?);
        Err(ManagementError::InvalidEvidence)
    })
    .await;
    assert!(matches!(failed, Err(ManagementError::InvalidEvidence)));
    assert!(!active(tx.as_ref()).await.unwrap());
    assert!(schema::validate(tx.as_ref()).await.is_err());
    run::<()>(tx.as_ref(), &scope, "wrong-native-identity", async {
        panic!("identity rejection must precede capability publication")
    })
    .await
    .expect_err("wrong identity must not enter conversion");
    another.rollback().await.unwrap();
    foreign_tx.rollback().await.unwrap();
    Arc::try_unwrap(tx).unwrap().rollback().await.unwrap();
    foreign.close().await.unwrap();
    db.close().await.unwrap();
}
