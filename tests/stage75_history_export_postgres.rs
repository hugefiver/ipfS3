use ipfs_s3_gateway::{
    pinning::management::{
        NativeKey,
        wire::v3::{
            files,
            history::{self, Export, Progress},
        },
    },
    store::{
        self,
        pinning::management::{
            native,
            records::segmented::{self, ArchiveHeader},
            schema,
        },
    },
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait,
};
use std::io::Cursor;
use tokio_util::sync::CancellationToken;

async fn revision(db: &impl ConnectionTrait, table: &str) -> i64 {
    db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT revision FROM pin_management_revision WHERE source_table=$1",
        [table.into()],
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get("", "revision")
    .unwrap()
}

async fn complete(db: &DatabaseConnection) -> Export {
    let mut export = Export::start(
        db,
        "postgres history operator",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    while export
        .advance(db, 1, CancellationToken::new())
        .await
        .unwrap()
        == Progress::Pending
    {}
    export
}

#[tokio::test]
#[ignore = "requires parent-selected fresh exclusive Stage7 PostgreSQL schema"]
async fn postgres_history_export_preserves_private_cells_and_detects_archive_collection_drift() {
    let url = std::env::var("IPFS_S3_STAGE75_HISTORY_DATABASE_URL").unwrap();
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options).await.unwrap();
    let fixture = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AND NOT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=current_schema()::regnamespace) AS empty"))
        .await.unwrap().unwrap();
    assert!(
        fixture
            .try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(fixture.try_get::<bool>("", "empty").unwrap());
    let exclusive: bool = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT pg_try_advisory_lock(hashtextextended(current_schema(), 75)) AS held",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "held")
        .unwrap();
    assert!(exclusive);
    store::run_migrations(&db).await.unwrap();
    schema::validate(&db).await.unwrap();
    let original_binding = native::scope::binding(&db).await.unwrap();
    assert!(!original_binding.1.starts_with("diagnostic:"));
    let raw = "  {\"private\":\"foreign 原文\"} \n";
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_management_records VALUES('foreign-scope','foreign-plan','item',\
         'action','foreign-principal','foreign-db',' raw binding ',$1,NULL,'[]','[]',\
         'stopped','2020-01-01 01:02:03.123456+00','2020-01-02 01:02:03.123456+00',NULL)",
        [raw.into()],
    ))
    .await
    .unwrap();
    db.execute_unprepared(
        "INSERT INTO pin_management_retirements VALUES('foreign-domain','foreign-scope',\
         'foreign-plan','item','foreign-principal','foreign-db','binding',\
         ' original inventory 私密 ','retain','[]','retired',\
         '2020-02-01 01:02:03.123456789','action')",
    )
    .await
    .unwrap();
    let original_expiry: String = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT expires_at::text AS value FROM pin_management_records WHERE database_scope='foreign-scope' AND plan_id='foreign-plan' AND item_id='item' AND kind='action'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "value")
        .unwrap();
    let header = ArchiveHeader {
        database_scope: original_binding.0.clone(),
        database_identity: original_binding.1.clone(),
        principal: "original operator".into(),
        catalog: vec!["original catalog".into()],
        request: "null".into(),
        configuration: "null".into(),
        acknowledgements: vec![],
        created_at: "2020-01-01 01:02:03.123456789".into(),
        expires_at: Some("2020-01-02 01:02:03.000000001".into()),
    };
    let body = vec![0xab; 300_001];
    let tx = db.begin().await.unwrap();
    let reference = segmented::write_archive(
        &tx,
        "unreferenced-original",
        &header,
        &mut Cursor::new(&body),
        &mut files::stream::budget(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let archive_revision = revision(&db, "pin_management_archive").await;
    let chunk_revision = revision(&db, "pin_management_archive_chunks").await;
    assert!(archive_revision > 0 && chunk_revision > 0);

    let mut stale = Export::start(
        &db,
        "postgres history operator",
        files::stream::budget(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(
        stale
            .advance(&db, 1, CancellationToken::new())
            .await
            .unwrap(),
        Progress::Pending
    );
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "INSERT INTO pin_management_archive VALUES('rolled-back','S75A1',\
         'scope','identity','{}','staging',0,0,NULL)",
    )
    .await
    .unwrap();
    assert!(revision(&tx, "pin_management_archive").await > archive_revision);
    tx.rollback().await.unwrap();
    assert_eq!(
        revision(&db, "pin_management_archive").await,
        archive_revision
    );
    assert_eq!(
        revision(&db, "pin_management_archive_chunks").await,
        chunk_revision
    );
    db.execute_unprepared(
        "INSERT INTO pin_management_archive VALUES('committed-new-history','S75A1',\
         'scope','identity','{}','staging',0,0,NULL)",
    )
    .await
    .unwrap();
    assert!(
        stale
            .advance(&db, 1, CancellationToken::new())
            .await
            .is_err()
    );
    let directory = tempfile::tempdir().unwrap();
    let invalid = directory.path().join("stale.v3");
    assert!(
        stale
            .write_private(invalid.to_str().unwrap())
            .await
            .is_err()
    );
    stale.close().await.unwrap();

    let mut export = complete(&db).await;
    let output = directory.path().join("complete.v3");
    export
        .write_private(output.to_str().unwrap())
        .await
        .unwrap();
    let mut recovered = history::restore_private(output.to_str().unwrap(), files::stream::budget())
        .await
        .unwrap();
    assert!(recovered.head.request.is_none());
    assert_eq!(recovered.head.database_identity, original_binding.1);
    let key = NativeKey {
        table: schema::RECORDS.into(),
        values: ["foreign-scope", "foreign-plan", "item", "action"]
            .map(str::to_owned)
            .to_vec(),
    };
    let mut restored = Vec::new();
    recovered
        .cell(&key, "native_before", &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, raw.as_bytes());
    restored.clear();
    assert!(
        recovered
            .cell(&key, "result", &mut restored)
            .await
            .unwrap()
            .is_none()
    );
    assert!(restored.is_empty());
    recovered
        .cell(&key, "expires_at", &mut restored)
        .await
        .unwrap();
    assert_eq!(restored, original_expiry.as_bytes());
    restored.clear();
    recovered
        .cell(
            &NativeKey {
                table: schema::RETIREMENTS.into(),
                values: vec!["foreign-domain".into()],
            },
            "inventory",
            &mut restored,
        )
        .await
        .unwrap();
    assert_eq!(restored, " original inventory 私密 ".as_bytes());
    restored.clear();
    assert!(
        recovered
            .read_archive(&reference, &mut restored)
            .await
            .unwrap()
            == header
    );
    assert_eq!(restored, body);
    assert!(recovered.archive("committed-new-history").await.is_err());
    assert_eq!(native::scope::binding(&db).await.unwrap(), original_binding);
    recovered.close().await.unwrap();
    export.close().await.unwrap();

    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "DROP TRIGGER s75_export_pin_management_archive_insert ON pin_management_archive",
    )
    .await
    .unwrap();
    assert!(schema::validate(&tx).await.is_err());
    tx.rollback().await.unwrap();
    schema::validate(&db).await.unwrap();
    db.close().await.unwrap();
}
