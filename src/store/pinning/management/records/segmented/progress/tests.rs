use super::*;
use sea_orm::TransactionTrait;

async fn database() -> sea_orm::DatabaseConnection {
    let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    for (name, ddl) in
        crate::store::pinning::management::schema::storage::tables(sea_orm::DatabaseBackend::Sqlite)
    {
        if crate::store::pinning::management::schema::storage::EXPORT_TABLES.contains(&name) {
            db.execute_unprepared(&ddl).await.unwrap();
        }
    }
    db
}

#[tokio::test]
async fn committed_page_response_loss_recovers_full_bytes_and_cas_coordinates() {
    let db = database().await;
    let tx = db.begin().await.unwrap();
    let before = create(&tx, "page", "binding", "scope", "physical", "before")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let payload = vec![0x80; CELL_SLICE + 19];
    let tx = db.begin().await.unwrap();
    let after = append(&tx, "page", "binding", &before, "after", &payload, false)
        .await
        .unwrap();
    // The database has committed while the caller has retained its before-CAS
    // coordinates, exactly as when the commit response is unavailable.
    tx.commit().await.unwrap();
    assert!(
        committed(&db, "page", "binding", &before, &after, &payload)
            .await
            .unwrap()
    );
    let mut different = payload.clone();
    different[CELL_SLICE + 1] ^= 1;
    assert!(matches!(
        committed(&db, "page", "binding", &before, &after, &different).await,
        Err(ManagementError::InvalidEvidence)
    ));
    let tx = db.begin().await.unwrap();
    assert!(
        append(&tx, "page", "binding", &before, "after", &payload, false)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(load(&db, "page", "other binding", false).await.is_err());
}

#[tokio::test]
async fn rolled_back_page_is_not_reported_as_a_committed_prefix() {
    let db = database().await;
    let tx = db.begin().await.unwrap();
    let before = create(&tx, "page", "binding", "scope", "physical", "before")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let after = append(
        &tx,
        "page",
        "binding",
        &before,
        "after",
        b"private original",
        false,
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert!(
        !committed(&db, "page", "binding", &before, &after, b"private original")
            .await
            .unwrap()
    );
}
