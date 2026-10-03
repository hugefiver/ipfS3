use std::collections::BTreeMap;
use std::{future::Future, panic::AssertUnwindSafe};

use futures_util::FutureExt;
use ipfs_s3_gateway::{
    import::SupersedeReason,
    store::{self, import::ownership},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
    TransactionTrait,
};
use sea_orm_migration::{MigrationTrait, SchemaManager};
use sha2::{Digest, Sha256};

use ipfs_s3_gateway::store::{
    migrations::m20260927_000004_zip_v2_execution as migration, zip::execution,
};

async fn setup() -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("v2.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let a = store::connect_database(&url).await.unwrap();
    store::run_migrations(&a).await.unwrap();
    store::bucket::create(&a, "bucket", None).await.unwrap();
    let b = store::connect_database(&url).await.unwrap();
    (dir, a, b)
}

// Real PG17 must be explicitly requested. Touches only a fresh UUID schema.
async fn pg_isolated<F, Fut>(body: F)
where
    F: FnOnce(DatabaseConnection, DatabaseConnection) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("NOT RUN: set IPFS_S3_TEST_POSTGRES_URL to an authorized PG17 database");
    let admin = Database::connect(&url).await.unwrap();
    let version: String = admin
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "server_version")
        .unwrap();
    assert!(version.starts_with("17."), "requires PostgreSQL 17");
    let schema = format!("zip_v2_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    assert!(
        admin
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM pg_namespace WHERE nspname=$1",
                [schema.clone().into()]
            ))
            .await
            .unwrap()
            .is_none()
    );
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let mut conns = Vec::new();
        for _ in 0..2 {
            let mut opts = ConnectOptions::new(&url);
            opts.min_connections(1).max_connections(1);
            let db = Database::connect(opts).await.unwrap();
            db.execute_unprepared(&format!("SET search_path TO {schema}"))
                .await
                .unwrap();
            db.execute_unprepared("SET statement_timeout TO '8s'")
                .await
                .unwrap();
            conns.push(db);
        }
        store::run_migrations(&conns[0]).await.unwrap();
        store::bucket::create(&conns[0], "bucket", None)
            .await
            .unwrap();
        body(conns[0].clone(), conns[1].clone()).await;
        for db in conns {
            db.close().await.unwrap();
        }
    })
    .catch_unwind()
    .await;
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(
        admin
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT 1 FROM pg_namespace WHERE nspname=$1",
                [schema.into()]
            ))
            .await
            .unwrap()
            .is_none()
    );
    admin.close().await.unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

fn request(token: &str) -> execution::Admission {
    execution::Admission {
        id: format!("id-{token}"),
        owner: "owner".into(),
        source: "direct".into(),
        token: token.into(),
        request_fingerprint: hex::encode(Sha256::digest(b"{\"canonical\":true}")),
        request_contract: "{\"canonical\":true}".into(),
        bucket: "bucket".into(),
        source_key: "source.zip".into(),
        captured_options: "{}".into(),
    }
}

async fn admitted(db: &DatabaseConnection, token: &str) -> execution::Claim {
    let id = request(token).id;
    execution::admit(db, &request(token)).await.unwrap();
    let claim = execution::claim(db, &id, "gateway-A", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &"b".repeat(64), "art-cid", 42)
        .await
        .unwrap();
    claim
}

async fn targets(db: &DatabaseConnection, claim: &execution::Claim, keys: &[&str]) {
    let txn = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&txn, "bucket")
        .await
        .unwrap();
    let items = keys
        .iter()
        .enumerate()
        .map(|(i, key)| execution::ManifestItem::Success {
            path: format!("path-{i}"),
            object_key: (*key).into(),
            cid: format!("cid-{i}"),
            size: i as i64,
        })
        .collect::<Vec<_>>();
    let ids = keys
        .iter()
        .map(|key| ((*key).into(), format!("mutation-{key}")))
        .collect::<BTreeMap<_, _>>();
    execution::admit_manifest_in_transaction(&txn, claim, &items, &ids)
        .await
        .unwrap();
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn signed_token_freezes_headers_and_clean_input_bytes() {
    let (_dir, a, b) = setup().await;
    execution::admit(&a, &request("same")).await.unwrap();
    let mut changed = request("same");
    changed.id = "another-id".into();
    changed.request_contract = "{}".into();
    changed.request_fingerprint = hex::encode(Sha256::digest(changed.request_contract.as_bytes()));
    assert!(execution::admit(&b, &changed).await.is_err());
    let mut policy_changed = request("same");
    policy_changed.captured_options = "{\"changed_policy\":true}".into();
    assert_eq!(
        execution::admit(&b, &policy_changed)
            .await
            .unwrap()
            .captured_options,
        "{}"
    );
    let claim = execution::claim(&a, "id-same", "A", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(&a, &claim, &"b".repeat(64), "cid", 3)
        .await
        .unwrap();
    assert!(
        execution::bind_clean_input(&b, &claim, &"c".repeat(64), "cid", 3)
            .await
            .is_err()
    );
    assert!(
        execution::read_for_replay(&b, &request("same"), &"c".repeat(64))
            .await
            .is_err()
    );
    // A random CID cannot masquerade as an input SHA-256.
    assert!(
        execution::bind_clean_input(&a, &claim, "QmNotADigest", "cid", 3)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn same_token_different_complete_bytes_conflicts_even_with_identical_output() {
    let (_dir, a, b) = setup().await;
    let claim = admitted(&a, "bytes").await;
    targets(&a, &claim, &["out/same"]).await;
    execution::complete(&a, &claim, "{\"output\":\"out/same\"}")
        .await
        .unwrap();
    let replay = execution::read_for_replay(&b, &request("bytes"), &"b".repeat(64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay.terminal_result.as_deref(),
        Some("{\"output\":\"out/same\"}")
    );
    assert!(
        execution::read_for_replay(&b, &request("bytes"), &"c".repeat(64))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sqlite_schema_rejects_invalid_state_digest_and_target_fk() {
    let (_dir, a, _) = setup().await;
    execution::admit(&a, &request("schema")).await.unwrap();
    for sql in [
        "UPDATE zip_v2_executions SET state='completed' WHERE id='id-schema'",
        "UPDATE zip_v2_executions SET input_sha256='random-id',input_art_cid='cid',input_art_size=1 WHERE id='id-schema'",
        "INSERT INTO zip_v2_targets(batch_id,object_key,mutation_id,expected_generation,epoch) VALUES ('id-schema','unknown','m',0,1)",
    ] {
        assert!(
            a.execute_unprepared(sql).await.is_err(),
            "schema accepted {sql}"
        );
    }
    assert!(
        migration::Migration
            .down(&SchemaManager::new(&a))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn two_gateways_compete_for_only_one_lease_and_partial_admission_rolls_back() {
    let (_dir, a, b) = setup().await;
    execution::admit(&a, &request("race")).await.unwrap();
    let (one, two) = tokio::join!(
        execution::claim(&a, "id-race", "gateway-A", 30),
        execution::claim(&b, "id-race", "gateway-B", 30)
    );
    let claims = [one.unwrap(), two.unwrap()];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);
    let claim = claims.into_iter().flatten().next().unwrap();
    execution::bind_clean_input(&a, &claim, &"b".repeat(64), "cid", 3)
        .await
        .unwrap();
    let tx = a.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let items = [execution::ManifestItem::Success {
        path: "p".into(),
        object_key: "out/p".into(),
        cid: "cid".into(),
        size: 2,
    }];
    let ids = [("out/p".into(), "m".into())].into_iter().collect();
    execution::admit_manifest_in_transaction(&tx, &claim, &items, &ids)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let row = execution::read(&b, "id-race").await.unwrap().unwrap();
    assert_eq!(row.state, "pending");
    assert!(
        execution::read_targets(&b, &claim)
            .await
            .unwrap()
            .is_empty()
    );
    // A fresh one-shot transaction can admit after rollback, but not after
    // any subsequent lost guard: that path is terminal, never re-admitted.
    targets(&b, &claim, &["out/p"]).await;
}

#[tokio::test]
async fn pending_failure_fences_without_fake_legacy_publication() {
    let (_dir, a, _) = setup().await;
    execution::admit(&a, &request("failed")).await.unwrap();
    let claim = execution::claim(&a, "id-failed", "gateway-A", 30)
        .await
        .unwrap()
        .unwrap();
    assert!(execution::fence(&a, &claim).await.unwrap());
    assert_eq!(
        execution::read(&a, "id-failed")
            .await
            .unwrap()
            .unwrap()
            .terminal_result
            .as_deref(),
        Some("fenced")
    );
    assert!(
        execution::claim(&a, "id-failed", "gateway-B", 30)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires explicitly authorized IPFS_S3_TEST_POSTGRES_URL to PG17"]
async fn pg17_v2_schema_two_gateways_takeover_and_stale_cleanup() {
    pg_isolated(|a,b| async move {
        execution::admit(&a,&request("pg")).await.unwrap();
        assert!(a.execute_unprepared("UPDATE zip_v2_executions SET state='completed' WHERE id='id-pg'").await.is_err());
        assert!(a.execute_unprepared("UPDATE zip_v2_executions SET input_sha256='not-sha',input_art_cid='cid',input_art_size=1 WHERE id='id-pg'").await.is_err());
        let (one,two) = tokio::join!(
            execution::claim(&a,"id-pg","A",30),
            execution::claim(&b,"id-pg","B",30)
        );
        let claims=[one.unwrap(),two.unwrap()];
        assert_eq!(claims.iter().filter(|v|v.is_some()).count(),1);
        let first=claims.into_iter().flatten().next().unwrap();
        execution::bind_clean_input(&a,&first,&"b".repeat(64),"art",12).await.unwrap();
        targets(&a,&first,&["out/a","out/b"]).await;
        let guards=execution::read_targets(&a,&first).await.unwrap();
        a.execute_unprepared("UPDATE zip_v2_executions SET lease_until='2000-01-01T00:00:00Z' WHERE id='id-pg'").await.unwrap();
        let second=execution::claim(&b,"id-pg","successor",30).await.unwrap().unwrap();
        assert_ne!(guards[0].mutation_id,execution::read_targets(&b,&second).await.unwrap()[0].mutation_id);
        assert!(!ownership::release_standard_mutation(&a,&guards[0]).await.unwrap());
        assert!(!execution::renew(&a,&first,30).await.unwrap());
        assert!(execution::renew(&b,&second,30).await.unwrap());
        execution::complete(&b,&second,"{\"output\":true}").await.unwrap();
        assert!(execution::read_for_replay(&a,&request("pg"),&"c".repeat(64)).await.is_err());
        assert!(migration::Migration.down(&SchemaManager::new(&a)).await.is_err());
    }).await;
}

#[tokio::test]
async fn takeover_fences_old_cleanup_and_lost_output_is_never_readmitted() {
    let (_dir, a, b) = setup().await;
    let first = admitted(&a, "takeover").await;
    targets(&a, &first, &["out/a", "out/b"]).await;
    let old_guards = execution::read_targets(&a, &first).await.unwrap();
    a.execute_unprepared(
        "UPDATE zip_v2_executions SET lease_until='2000-01-01T00:00:00Z' WHERE id='id-takeover'",
    )
    .await
    .unwrap();
    let second = execution::claim(&b, "id-takeover", "gateway-B", 30)
        .await
        .unwrap()
        .unwrap();
    assert!(second.epoch > first.epoch);
    assert!(
        !ownership::release_standard_mutation(&a, &old_guards[0])
            .await
            .unwrap()
    );
    assert!(!execution::renew(&a, &first, 30).await.unwrap());
    assert!(
        execution::complete(&a, &first, "{\"ok\":true}")
            .await
            .is_err()
    );
    let guard = execution::read_targets(&b, &second).await.unwrap();
    assert_eq!(guard.len(), 2);
    assert_ne!(guard[0].mutation_id, old_guards[0].mutation_id);
    assert!(execution::renew(&b, &second, 30).await.unwrap());
    let txn = b.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&txn, "bucket")
        .await
        .unwrap();
    // Simulate a newer writer taking over one exact key.
    let keys = ["out/a".to_string()].into_iter().collect();
    let ids = [("out/a".into(), "foreign-writer".into())]
        .into_iter()
        .collect();
    ownership::admit_zip_outputs_without_source_in_transaction(
        &txn,
        "bucket",
        "other-source.zip",
        &keys,
        &ids,
        store::database_clock::database_now(&txn).await.unwrap(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert!(!execution::renew(&b, &second, 30).await.unwrap());
    assert!(
        execution::claim(&a, "id-takeover", "gateway-A", 30)
            .await
            .unwrap()
            .is_none()
    );
    assert!(execution::complete(&b, &second, "{}").await.is_err());
    assert_eq!(
        execution::read(&a, "id-takeover")
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
}

#[tokio::test]
async fn displaced_guard_before_takeover_yields_fenced_terminal_not_new_admission() {
    let (_dir, a, b) = setup().await;
    let first = admitted(&a, "lost-before-takeover").await;
    targets(&a, &first, &["out/a", "out/b"]).await;
    let old = execution::read_targets(&a, &first).await.unwrap();
    a.execute_unprepared("UPDATE zip_v2_executions SET lease_until='2000-01-01T00:00:00Z' WHERE id='id-lost-before-takeover'").await.unwrap();
    let tx = b.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let keys = ["out/b".into()].into_iter().collect();
    let ids = [("out/b".into(), "newer".into())].into_iter().collect();
    ownership::admit_zip_outputs_without_source_in_transaction(
        &tx,
        "bucket",
        "unrelated.zip",
        &keys,
        &ids,
        store::database_clock::database_now(&tx).await.unwrap(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        execution::claim(&b, &first.batch_id, "gateway-B", 30)
            .await
            .unwrap()
            .is_none()
    );
    let terminal = execution::read(&a, &first.batch_id).await.unwrap().unwrap();
    assert_eq!(terminal.state, "fenced");
    assert_eq!(terminal.terminal_result.as_deref(), Some("fenced"));
    assert_eq!(terminal.epoch, first.epoch);
    assert_eq!(execution::read_targets(&a, &first).await.unwrap(), old);
    assert!(
        execution::claim(&a, &first.batch_id, "gateway-A", 30)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn source_prefix_guard_is_not_revoked_by_v2_output_admission() {
    let (_dir, a, _) = setup().await;
    let mut request = request("source-safe");
    request.source_key = "prefix/source.zip".into();
    execution::admit(&a, &request).await.unwrap();
    let claim = execution::claim(&a, &request.id, "A", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(&a, &claim, &"b".repeat(64), "art", 10)
        .await
        .unwrap();
    let source_guard = ownership::admit_content_and_prefix_mutation(
        &a,
        "bucket",
        "prefix/source.zip",
        "prefix/",
        SupersedeReason::DecompressZip,
        store::database_clock::database_now(&a).await.unwrap(),
    )
    .await
    .unwrap();
    let tx = a.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let output = [execution::ManifestItem::Success {
        path: "a".into(),
        object_key: "prefix/out/a".into(),
        cid: "leaf".into(),
        size: 2,
    }];
    let ids = [("prefix/out/a".into(), "mutation".into())]
        .into_iter()
        .collect();
    assert!(
        execution::admit_manifest_in_transaction(&tx, &claim, &output, &ids)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    ownership::renew_standard_mutation(&a, &source_guard)
        .await
        .unwrap();
    assert_eq!(
        execution::read(&a, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "pending"
    );
    assert!(
        execution::read_targets(&a, &claim)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn empty_success_set_still_has_a_durable_terminal_result() {
    let (_dir, a, _) = setup().await;
    let claim = admitted(&a, "empty").await;
    let txn = a.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&txn, "bucket")
        .await
        .unwrap();
    execution::admit_manifest_in_transaction(
        &txn,
        &claim,
        &[execution::ManifestItem::Failure {
            path: "bad".into(),
            code: "invalid_zip".into(),
        }],
        &BTreeMap::new(),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    execution::complete(&a, &claim, "{\"failed\":1}")
        .await
        .unwrap();
    assert_eq!(
        execution::read(&a, "id-empty")
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
}
