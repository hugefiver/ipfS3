use super::*;
use sea_orm::Statement;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

struct Mutation {
    sql: Option<&'static str>,
    cancel: Option<CancellationToken>,
    reached: Arc<AtomicBool>,
}

tokio::task_local! {
    static AFTER_INSERT: Mutation;
}

pub(super) async fn after_attestation_insert(tx: &sea_orm::DatabaseTransaction) {
    let Ok((sql, cancel, reached)) =
        AFTER_INSERT.try_with(|probe| (probe.sql, probe.cancel.clone(), probe.reached.clone()))
    else {
        return;
    };
    let row = tx.query_one(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT COUNT(*) AS n FROM pin_management_records WHERE plan_id='proof-source' AND kind='evidence_preparation'"))
        .await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 1);
    if let Some(sql) = sql {
        assert_eq!(tx.execute_unprepared(sql).await.unwrap().rows_affected(), 1);
    }
    if let Some(cancel) = cancel {
        cancel.cancel();
    }
    reached.store(true, Ordering::SeqCst);
}

#[tokio::test]
async fn final_attestation_fence_rolls_back_mutation_or_cancellation() {
    const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    for sql in [
        Some("UPDATE objects SET metadata='{\"changed\":true}' WHERE key='proof'"),
        Some("UPDATE object_versions SET version_id='replacement' WHERE key='proof'"),
        None,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("proof.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = crate::store::connect_database(&url).await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO objects (id,bucket,key,cid,size,etag,encrypted,multipart,is_latest,created_at) VALUES ('object','bucket','proof',$1,10,$1,FALSE,FALSE,TRUE,CURRENT_TIMESTAMP)", [CID.into()])).await.unwrap();
        db.execute_unprepared("INSERT INTO object_versions (id,bucket,key,version_id,kind,object_id,sequence,is_latest,lifecycle_age_started_at,created_at,updated_at) VALUES ('version','bucket','proof',NULL,'object','object',1,TRUE,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":CID})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
            .mount(&server)
            .await;
        let mut raw: crate::config::Config = toml::from_str("").unwrap();
        raw.kubo.rpc_url = server.uri();
        let config = crate::pinning::management::local_config::compile(&raw, |_| None)
            .unwrap()
            .with_source_node(CID.into())
            .unwrap();
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        let request = PrepareRequest {
            record_id: "proof-source".into(),
            identity: ExactObject {
                bucket: "bucket".into(),
                key: "proof".into(),
                object_id: "object".into(),
                private_version_id: "version".into(),
                public_version: super::super::PublicVersionId::Null,
                cid: crate::pinning::provider::canonical_resource_cid(CID).unwrap(),
            },
            expected_node: CID.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        };
        let client = KuboClient::new(server.uri());
        let reached = Arc::new(AtomicBool::new(false));
        let cancel = CancellationToken::new();
        let result = AFTER_INSERT
            .scope(
                Mutation {
                    sql,
                    cancel: sql.is_none().then(|| cancel.clone()),
                    reached: reached.clone(),
                },
                prepare(&db, &config, &auth, &client, &request, cancel),
            )
            .await;
        assert!(reached.load(Ordering::SeqCst));
        if sql.is_some() {
            assert!(matches!(result, Err(ManagementError::Changed)));
        } else {
            assert!(matches!(result, Err(ManagementError::Expired)));
        }
        let row = db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT (SELECT COUNT(*) FROM pin_management_records WHERE plan_id='proof-source') AS attestations,(SELECT metadata FROM objects WHERE key='proof') AS metadata,(SELECT version_id FROM object_versions WHERE key='proof') AS version_id"))
            .await.unwrap().unwrap();
        assert_eq!(row.try_get::<i64>("", "attestations").unwrap(), 0);
        assert_eq!(row.try_get::<Option<String>>("", "metadata").unwrap(), None);
        assert_eq!(
            row.try_get::<Option<String>>("", "version_id").unwrap(),
            None
        );
        assert_eq!(
            prepare(
                &db,
                &config,
                &auth,
                &client,
                &request,
                CancellationToken::new()
            )
            .await
            .unwrap()
            .stored_bytes,
            Some(10)
        );
        db.close().await.unwrap();
    }
}
