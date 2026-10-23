use ipfs_s3_gateway::store::{
    self,
    pinning::management::{
        native::meter::Budget,
        records::segmented::{self, ArchiveHeader},
        schema,
    },
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement, TransactionTrait};

fn header() -> ArchiveHeader {
    ArchiveHeader {
        database_scope: "foreign scope".into(),
        database_identity: "private identity".into(),
        principal: "original principal".into(),
        catalog: vec!["original catalog".into()],
        request: " {\"key\":1,\"key\":2} ".into(),
        configuration: " private credential bytes ".into(),
        acknowledgements: vec!["item".into()],
        created_at: "2026-10-01T01:02:03.123456789Z".into(),
        expires_at: Some("2026-10-01T01:03:03.123456789Z".into()),
    }
}

async fn collision(db: &DatabaseConnection, id: &str, first: usize, second: usize) {
    let bytes = vec![7_u8; first];
    let tx = db.begin().await.unwrap();
    let original = segmented::write_archive(
        &tx,
        id,
        &header(),
        &mut bytes.as_slice(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    let extra = format!("{id}-rollback");
    segmented::write_archive(
        &tx,
        &extra,
        &header(),
        &mut b"rollback".as_slice(),
        &mut Budget::default(),
    )
    .await
    .unwrap();
    assert!(
        segmented::write_archive(
            &tx,
            id,
            &header(),
            &mut vec![9_u8; second].as_slice(),
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let mut restored = Vec::new();
    segmented::read_archive(db, &original, &mut restored, &mut Budget::default())
        .await
        .unwrap();
    assert_eq!(restored, bytes);
    let count: i64 = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_management_archive_identity WHERE archive_id=$1",
            [extra.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn archive_identity_excludes_both_representations_and_staging() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    collision(&db, "small-first", 1, 65537).await;
    collision(&db, "large-first", 65537, 1).await;
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared("INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES('staging','S75A1','scope','identity','{}','staging')").await.unwrap();
    tx.commit().await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        segmented::write_archive(
            &tx,
            "staging",
            &header(),
            &mut b"small".as_slice(),
            &mut Budget::default()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let count: i64 = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_management_inline_archive WHERE archive_id='staging'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(count, 0);
    let tx = db.begin().await.unwrap();
    assert!(tx.execute_unprepared("INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES('small-first','S75A1','scope','identity','{}','staging')").await.is_err());
    tx.rollback().await.unwrap();
    schema::validate(&db).await.unwrap();
}

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn postgres_archive_identity_serializes_competing_representations() {
    use ipfs_s3_gateway::store::pinning::management::{
        ManagementError, records::segmented::ArchiveRef,
    };
    use sea_orm::{ConnectOptions, DatabaseBackend, DatabaseTransaction};
    async fn insert(
        tx: &DatabaseTransaction,
        id: &str,
        size: Option<usize>,
    ) -> Result<Option<ArchiveRef>, ManagementError> {
        if let Some(size) = size {
            segmented::write_archive(
                tx,
                id,
                &header(),
                &mut vec![3_u8; size].as_slice(),
                &mut Budget::default(),
            )
            .await
            .map(Some)
        } else {
            tx.execute(Statement::from_sql_and_values(DatabaseBackend::Postgres, "INSERT INTO pin_management_archive(archive_id,format,database_scope,native_identity,header,state) VALUES($1,'S75A1','scope','identity','{}','staging')", [id.into()])).await.map_err(|_| ManagementError::Database)?;
            Ok(None)
        }
    }
    let url = std::env::var("IPFS_S3_STAGE75_STORAGE_DATABASE_URL")
        .expect("parent-owned empty schema required");
    let mut options = ConnectOptions::new(&url);
    options.max_connections(1).min_connections(1);
    let db = Database::connect(options.clone()).await.unwrap();
    let row = db.query_one(Statement::from_string(DatabaseBackend::Postgres, "SELECT current_schema() AS name, NOT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace) AS empty, pg_try_advisory_lock(hashtextextended(current_database() || ':' || current_schema(),0)) AS claimed")).await.unwrap().unwrap();
    assert!(
        row.try_get::<String>("", "name")
            .unwrap()
            .starts_with("stage7_")
    );
    assert!(row.try_get::<bool>("", "empty").unwrap());
    assert!(row.try_get::<bool>("", "claimed").unwrap());
    store::run_migrations(&db).await.unwrap();
    let other = Database::connect(options).await.unwrap();
    let pid: i32 = other
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT pg_backend_pid() AS pid",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "pid")
        .unwrap();
    for (number, (first, second)) in [
        (Some(1), Some(65537)),
        (Some(65537), Some(1)),
        (None, Some(1)),
        (Some(1), None),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("competing-{number}");
        let tx = db.begin().await.unwrap();
        let reference = insert(&tx, &id, first).await.unwrap();
        let other_db = other.clone();
        let other_id = id.clone();
        let loser = tokio::spawn(async move {
            let tx = other_db.begin().await.unwrap();
            tx.execute_unprepared("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await
                .unwrap();
            // Establish a snapshot before the winning transaction commits.
            tx.query_one(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT COUNT(*) FROM pin_management_archive_identity",
            ))
            .await
            .unwrap();
            insert(&tx, &format!("{other_id}-rollback"), second)
                .await
                .unwrap();
            let result = insert(&tx, &other_id, second).await;
            tx.rollback().await.unwrap();
            result
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let blocked: bool = tx
                    .query_one(Statement::from_sql_and_values(
                        DatabaseBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1))>0 AS blocked",
                        [pid.into()],
                    ))
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get("", "blocked")
                    .unwrap();
                if blocked {
                    break;
                }
                assert!(
                    !loser.is_finished(),
                    "competing insert must wait on the shared identity"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(loser.await.unwrap().is_err());
        let count: i64 = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT COUNT(*) AS n FROM pin_management_archive_identity WHERE archive_id=$1",
                [format!("{id}-rollback").into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(count, 0);
        if let Some(reference) = reference {
            let mut payload = Vec::new();
            segmented::read_archive(&db, &reference, &mut payload, &mut Budget::default())
                .await
                .unwrap();
            assert_eq!(payload, vec![3; first.unwrap()]);
        }
    }
    schema::validate(&db).await.unwrap();
    other.close().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn document_boundary_preserves_bytes_and_inline_is_one_read() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    for size in [0, 65536, 65537] {
        let bytes = (0..size).map(|i| (i % 256) as u8).collect::<Vec<_>>();
        let tx = db.begin().await.unwrap();
        let mut write = Budget::default();
        let reference = segmented::write_archive(
            &tx,
            &format!("document-{size}"),
            &header(),
            &mut bytes.as_slice(),
            &mut write,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut output = Vec::new();
        let mut read = Budget::default();
        let restored = segmented::read_archive(&db, &reference, &mut output, &mut read)
            .await
            .unwrap();
        assert!(restored == header());
        assert_eq!(output, bytes);
        if size <= 65536 {
            assert_eq!(reference.chunks, 0);
            assert_eq!(write.work.sql, 1);
            assert_eq!(read.work.sql, 1);
        } else {
            assert!(reference.chunks > 0);
        }
        let mut wrong = reference.clone();
        wrong.bytes += 1;
        assert!(
            segmented::read_archive(&db, &wrong, &mut Vec::new(), &mut Budget::default())
                .await
                .is_err()
        );
    }
    assert!(
        db.execute_unprepared(
            "UPDATE pin_management_inline_archive SET payload=X'01' WHERE archive_id='document-0'"
        )
        .await
        .is_err()
    );
    assert!(
        db.execute_unprepared(
            "DELETE FROM pin_management_inline_archive WHERE archive_id='document-0'"
        )
        .await
        .is_err()
    );
    schema::validate(&db).await.unwrap();
}
