use super::*;
use sea_orm::TransactionTrait;

#[tokio::test]
async fn upgrade_rebuild_requires_initializations_transaction_capability() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let key = NativeKey {
        table: "pin_provider_routes".into(),
        values: vec!["route".into()],
    };
    let tx = db.begin().await.unwrap();
    assert_eq!(
        rebuild_source_for_upgrade(&tx, &key, &mut Budget::default()).await,
        Err(ManagementError::Unauthorized)
    );
    tx.execute_unprepared("UPDATE pin_management_readiness SET state='backfill' WHERE id=1")
        .await
        .unwrap();
    assert_eq!(
        rebuild_source_for_upgrade(&tx, &key, &mut Budget::default()).await,
        Err(ManagementError::Unauthorized)
    );
    tx.rollback().await.unwrap();
}
