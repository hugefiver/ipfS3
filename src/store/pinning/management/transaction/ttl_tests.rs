use super::*;
use sea_orm::Statement;

#[tokio::test]
async fn database_clock_ttl_shortens_engine_budget_and_preserves_expired_error() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    db.execute_unprepared("CREATE TABLE ttl_marker(id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let mut tx = ManagementTransaction::begin(&db, FINAL_WRITE_LIMIT, CancellationToken::new())
        .await
        .unwrap();
    let failure = tx.run(|tx, deadline| Box::pin(async move {
        tx.execute_unprepared("INSERT INTO ttl_marker VALUES(1)").await.map_err(|_| ManagementError::Database)?;
        let now = crate::store::database_clock::database_now(tx).await.map_err(|_| ManagementError::Database)?;
        deadline.business_expiry(tx, now + Duration::from_millis(90)).await?;
        tx.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) AS total FROM n"))
            .await.map_err(|_| ManagementError::Database)?;
        Ok(())
    })).await.unwrap_err();
    assert_eq!(
        failure.cause,
        StopCause::Management(ManagementError::Expired)
    );
    assert!(failure.rollback_confirmed);
    assert_eq!(failure.management_error(), ManagementError::Expired);
    tokio::time::timeout(
        Duration::from_secs(2),
        db.execute_unprepared("INSERT INTO ttl_marker VALUES(2)"),
    )
    .await
    .unwrap()
    .unwrap();
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT count(*) AS n FROM ttl_marker WHERE id=1",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
}
