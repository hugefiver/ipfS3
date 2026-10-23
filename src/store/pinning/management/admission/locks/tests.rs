use super::*;
use crate::error::AppError;
use sea_orm::{Database, TransactionTrait};

#[tokio::test]
async fn unavailable_catalog_and_authority_remain_database_errors() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.clone().close().await.unwrap();
    assert!(matches!(
        AdmissionSchema::read(&db).await,
        Err(AppError::Database(_))
    ));
    assert!(matches!(
        super::super::work_error(crate::store::pinning::management::ManagementError::Database),
        AppError::Database(_)
    ));
    assert!(matches!(
        crate::store::pinning::management::actions::job_authority::safe(
            crate::store::pinning::management::ManagementError::Database
        ),
        AppError::Database(_)
    ));
}

#[tokio::test]
async fn sqlite_writer_contention_is_a_database_error_and_rollback_releases_the_fence() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("admission.sqlite").display()
    );
    let first = Database::connect(&url).await.unwrap();
    let second = Database::connect(&url).await.unwrap();
    first
        .execute_unprepared("CREATE TABLE pin_provider_routes(provider TEXT PRIMARY KEY)")
        .await
        .unwrap();
    let writer = first.begin().await.unwrap();
    lock_sqlite_writer_in_caller_transaction(&writer)
        .await
        .unwrap();
    let contender = second.begin().await.unwrap();
    contender
        .execute_unprepared("PRAGMA busy_timeout=0")
        .await
        .unwrap();
    let error = lock_sqlite_writer_in_caller_transaction(&contender)
        .await
        .unwrap_err();
    contender.rollback().await.unwrap();
    writer.rollback().await.unwrap();
    let contender = second.begin().await.unwrap();
    lock_sqlite_writer_in_caller_transaction(&contender)
        .await
        .unwrap();
    contender.rollback().await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
    assert!(matches!(error, AppError::Database(_)), "{error:?}");
}
