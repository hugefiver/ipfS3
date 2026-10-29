use super::*;
use sea_orm_migration::MigratorTrait;

#[tokio::test]
async fn historical_cleanup_uses_the_full_receipt_catalog() {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    let count = crate::store::migrations::Historical::migrations()
        .iter()
        .position(|migration| migration.name() == legacy_migration::SCHEMA)
        .unwrap()
        + 1;
    crate::store::migrations::Historical::up(&db, Some(count as u32))
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    let catalog = legacy_migration::ReceiptCatalog::full(&tx);
    let versions = catalog.classify().await.unwrap();
    assert_eq!(
        versions.last().map(String::as_str),
        Some(legacy_migration::SCHEMA)
    );
    let work = management::records::SharedDecodeBudget::default();
    assert_eq!(
        cleanup_allowed_catalog(catalog, "provider", "cid", DecodeWork::shared(&work))
            .await
            .unwrap(),
        cleanup_allowed(&tx, "provider", "cid").await.unwrap(),
    );
    tx.rollback().await.unwrap();
}
