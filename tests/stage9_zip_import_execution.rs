//! Read-only execution identity lookup for authenticated replay consumers.
use ipfs_s3_gateway::store::zip::execution;
use sea_orm::{
    ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};

async fn database() -> DatabaseConnection {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
    ipfs_s3_gateway::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    db
}

fn request(id: &str, owner: &str, source: &str, token: &str) -> execution::Admission {
    let contract = format!(
        "{{ \"signed_headers\":\"host;x-ipfs3-zip-expected-sha256\", \"expected_sha256\":\"{}\", \"source\":\"{source}\" }}",
        "a".repeat(64),
    );
    execution::Admission {
        id: id.to_owned(),
        owner: owner.to_owned(),
        source: source.to_owned(),
        token: token.to_owned(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
        request_contract: contract,
        bucket: "bucket".to_owned(),
        source_key: format!("{id}.zip"),
        captured_options: format!(
            "{{ \"options\":{{\"expected_sha256\":\"{}\"}}, \"signed_headers\":\"host;x-ipfs3-zip-expected-sha256\" }}",
            "a".repeat(64),
        ),
    }
}

#[tokio::test]
async fn identity_lookup_is_read_only_and_preserves_full_historical_snapshot_in_every_state() {
    let db = database().await;
    let request = request("historical", "principal", "direct", "token");
    assert!(
        execution::read_by_identity(&db, &request.owner, &request.source, &request.token)
            .await
            .unwrap()
            .is_none()
    );
    let count = db
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS total FROM zip_v2_executions".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count.try_get::<i64>("", "total").unwrap(), 0);
    execution::admit(&db, &request).await.unwrap();

    for state in ["pending", "admitted", "completed", "fenced"] {
        let terminal =
            matches!(state, "completed" | "fenced").then(|| "original-receipt".to_owned());
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "UPDATE zip_v2_executions SET state=?,input_sha256=?,input_art_cid='original-artifact',input_art_size=132,epoch=7,worker='original-worker',lease_until='2000-01-01 00:00:00',terminal_result=? WHERE id=?",
            vec![state.into(), "b".repeat(64).into(), terminal.clone().into(), request.id.clone().into()])).await.unwrap();
        let before = execution::read(&db, &request.id).await.unwrap().unwrap();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA query_only=ON".to_owned(),
        ))
        .await
        .unwrap();
        let actual =
            execution::read_by_identity(&db, &request.owner, &request.source, &request.token)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(actual, before);
        assert_eq!(actual.request_fingerprint, request.request_fingerprint);
        assert_eq!(actual.request_contract, request.request_contract);
        assert_eq!(actual.captured_options, request.captured_options);
        assert_eq!(
            actual.input_sha256.as_deref(),
            Some("b".repeat(64).as_str())
        );
        assert_eq!(actual.terminal_result, terminal);
        assert!(
            execution::read_by_identity(&db, "other-principal", &request.source, &request.token)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            execution::read_by_identity(&db, &request.owner, "mpu", &request.token)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            execution::read_by_identity(&db, &request.owner, &request.source, "other-token")
                .await
                .unwrap()
                .is_none()
        );
        let tx = db.begin().await.unwrap();
        assert_eq!(
            execution::read_by_identity(&tx, &request.owner, &request.source, &request.token)
                .await
                .unwrap(),
            Some(before.clone())
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            execution::read(&db, &request.id).await.unwrap(),
            Some(before)
        );
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA query_only=OFF".to_owned(),
        ))
        .await
        .unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn identity_lookup_separates_principal_and_entry_kind_with_the_same_token() {
    let db = database().await;
    for (id, owner, source) in [
        ("direct-a", "principal-a", "direct"),
        ("direct-b", "principal-b", "direct"),
        ("mpu-a", "principal-a", "mpu"),
        ("import-a", "principal-a", "import"),
    ] {
        let expected = execution::admit(&db, &request(id, owner, source, "shared-token"))
            .await
            .unwrap();
        assert_eq!(
            execution::read_by_identity(&db, owner, source, "shared-token")
                .await
                .unwrap(),
            Some(expected)
        );
    }
    assert_eq!(
        execution::read_by_identity(&db, "principal-a", "direct", "shared-token")
            .await
            .unwrap()
            .unwrap()
            .id,
        "direct-a"
    );
    assert_eq!(
        execution::read_by_identity(&db, "principal-b", "direct", "shared-token")
            .await
            .unwrap()
            .unwrap()
            .id,
        "direct-b"
    );
    assert_eq!(
        execution::read_by_identity(&db, "principal-a", "mpu", "shared-token")
            .await
            .unwrap()
            .unwrap()
            .id,
        "mpu-a"
    );
    assert_eq!(
        execution::read_by_identity(&db, "principal-a", "import", "shared-token")
            .await
            .unwrap()
            .unwrap()
            .id,
        "import-a"
    );
    db.close().await.unwrap();
}
