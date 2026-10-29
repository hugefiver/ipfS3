use super::*;
use std::sync::{Arc, atomic::AtomicBool};
use tokio::sync::Notify;

async fn connection_returning_to_pool(path: &Path) -> (DatabaseConnection, Arc<Notify>) {
    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .after_release({
            let armed = armed.clone();
            let entered = entered.clone();
            let resume = resume.clone();
            move |_, _| {
                let pause = armed.swap(false, Ordering::SeqCst);
                let entered = entered.clone();
                let resume = resume.clone();
                Box::pin(async move {
                    if pause {
                        entered.notify_one();
                        resume.notified().await;
                    }
                    Ok(true)
                })
            }
        })
        .connect_with(sqlx::sqlite::SqliteConnectOptions::new().filename(path))
        .await
        .unwrap();
    let connection = pool.acquire().await.unwrap();
    armed.store(true, Ordering::SeqCst);
    drop(connection);
    entered.notified().await;
    (
        sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool),
        resume,
    )
}

#[tokio::test]
async fn inventory_close_waits_for_return_before_immediate_workspace_cleanup() {
    let root = tempfile::tempdir().unwrap();
    eprintln!(
        "inventory close regression workspace parent={}",
        root.path().display()
    );
    let mut workspace = Workspace::create_at(&root.path().join("inventory")).unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let mut spool = InventorySpool::allocate(&path, file, None).await.unwrap();
    spool
        .db
        .get_sqlite_connection_pool()
        .acquire()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
    spool.db.clone().close().await.unwrap();
    let (db, resume) = connection_returning_to_pool(&path).await;
    spool.db = db;
    let closing = async {
        let (first, second) = tokio::join!(spool.close(), spool.close());
        first.and(second)
    };
    tokio::pin!(closing);
    assert!(futures_util::poll!(&mut closing).is_pending());
    resume.notify_one();
    closing.await.unwrap();
    let remaining = spool.db.get_sqlite_connection_pool().size();
    let cleanup = workspace.cleanup();
    spool.close().await.unwrap();
    root.close().unwrap();
    cleanup.unwrap();
    assert_eq!(remaining, 0);
    assert!(matches!(
        spool.db.get_sqlite_connection_pool().acquire().await,
        Err(sqlx::Error::PoolClosed)
    ));
}

#[tokio::test]
async fn union_close_waits_for_return_before_immediate_workspace_cleanup() {
    let root = tempfile::tempdir().unwrap();
    eprintln!(
        "union close regression workspace parent={}",
        root.path().display()
    );
    let mut workspace = Workspace::create_at(&root.path().join("inventory")).unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    drop(file);
    let (db, resume) = connection_returning_to_pool(&path).await;
    let union = PlanUnion { db };
    let closing = union.close();
    tokio::pin!(closing);
    assert!(futures_util::poll!(&mut closing).is_pending());
    resume.notify_one();
    closing.await.unwrap();
    let remaining = union.db.get_sqlite_connection_pool().size();
    let cleanup = workspace.cleanup();
    union.close().await.unwrap();
    root.close().unwrap();
    cleanup.unwrap();
    assert_eq!(remaining, 0);
    assert!(matches!(
        union.db.get_sqlite_connection_pool().acquire().await,
        Err(sqlx::Error::PoolClosed)
    ));
}

#[tokio::test]
async fn failed_open_preserves_error_after_immediate_workspace_cleanup() {
    let root = tempfile::tempdir().unwrap();
    eprintln!(
        "failed open regression workspace parent={}",
        root.path().display()
    );
    let mut workspace = Workspace::create_at(&root.path().join("inventory")).unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    drop(file);
    let (db, resume) = connection_returning_to_pool(&path).await;
    let closing = close_failed_open::<()>(&db, Err(ManagementError::InvalidEvidence));
    tokio::pin!(closing);
    assert!(futures_util::poll!(&mut closing).is_pending());
    resume.notify_one();
    let result = closing.await;
    let cleanup = workspace.cleanup();
    close_file(&db).await.unwrap();
    root.close().unwrap();
    assert_eq!(result, Err(ManagementError::InvalidEvidence));
    cleanup.unwrap();
}
