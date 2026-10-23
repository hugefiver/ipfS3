use super::*;
use sea_orm::{
    DatabaseBackend, DatabaseConnection, DatabaseTransaction, DbErr, ExecResult, QueryResult,
    Statement,
};
use std::sync::atomic::{AtomicBool, Ordering};

struct ChangeReceiptAfterListing<'a> {
    snapshot: &'a DatabaseTransaction,
    writer: &'a DatabaseConnection,
    changed: AtomicBool,
}

#[async_trait::async_trait]
impl ConnectionTrait for ChangeReceiptAfterListing<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.snapshot.get_database_backend()
    }

    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.snapshot.execute(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.snapshot.execute_unprepared(sql).await
    }

    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.snapshot.query_one(statement).await
    }

    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        let rows = self.snapshot.query_all(statement).await?;
        if !self.changed.swap(true, Ordering::Relaxed) {
            self.writer.execute_unprepared(
                "UPDATE physical_residencies SET verification_receipt = '{\"cid\":\"other\",\"node_identity\":\"node\"}' WHERE tier = 'cold'",
            ).await?;
        }
        Ok(rows)
    }
}

#[tokio::test]
async fn listing_and_storage_class_share_snapshot_and_validate_cold_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("listing.sqlite");
    let url = format!(
        "sqlite://{}?mode=rwc",
        path.display().to_string().replace('\\', "/")
    );
    let mut options = sea_orm::ConnectOptions::new(url);
    options.max_connections(2).min_connections(1);
    let db = sea_orm::Database::connect(options).await.unwrap();
    db.execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    object::Entity::insert(object::ActiveModel {
        id: Set("cold-object".to_owned()),
        bucket: Set("bucket".to_owned()),
        key: Set("写真/猫.png".to_owned()),
        cid: Set(cid.to_owned()),
        etag: Set(cid.to_owned()),
        size: Set(13),
        ..Default::default()
    })
    .exec(&db)
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO object_versions (id,bucket,key,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('cold-version','bucket','写真/猫.png','object','cold-object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let receipt = serde_json::to_string(&crate::kubo::LocalResidencyVerificationReceipt {
        cid: cid.to_owned(),
        node_identity: "node".to_owned(),
    })
    .unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO physical_residencies (tier,cid,node_identity,verification_state,verification_receipt,verified_at,created_at,updated_at) VALUES ('cold',$1,'node','verified',$2,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [cid.into(), receipt.into()],
    )).await.unwrap();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO version_residencies (version_row_id,object_id,primary_tier,storage_class,cid,revision,created_at,updated_at) VALUES ('cold-version','cold-object','cold','STANDARD_IA',$1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [cid.into()],
    )).await.unwrap();

    let txn = crate::s3::ops::storage_class::begin_read_snapshot(&db)
        .await
        .unwrap();
    let connection = ChangeReceiptAfterListing {
        snapshot: &txn,
        writer: &db,
        changed: AtomicBool::new(false),
    };
    let (_, objects, _) = listing_page_in_snapshot(
        &connection,
        ListingRequest {
            bucket: "bucket",
            prefix: "写真/",
            delimiter: Some("/"),
            cursor: None,
            max_keys: 1,
        },
        true,
    )
    .await
    .unwrap();
    assert!(connection.changed.load(Ordering::Relaxed));
    assert_eq!(objects.len(), 1);
    assert_eq!(
        objects[0].key.as_deref(),
        Some("%E5%86%99%E7%9C%9F%2F%E7%8C%AB.png")
    );
    assert_eq!(objects[0].size, Some(13));
    assert_eq!(objects[0].e_tag.as_ref().unwrap().value(), cid);
    assert_eq!(
        objects[0].storage_class.as_ref().unwrap().as_str(),
        "STANDARD_IA"
    );
    txn.commit().await.unwrap();

    let txn = crate::s3::ops::storage_class::begin_read_snapshot(&db)
        .await
        .unwrap();
    let error = listing_page_in_snapshot(
        &txn,
        ListingRequest {
            bucket: "bucket",
            prefix: "写真/",
            delimiter: None,
            cursor: None,
            max_keys: 1,
        },
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code().as_str(), "InternalError");
    txn.rollback().await.unwrap();
    db.close().await.unwrap();
}
