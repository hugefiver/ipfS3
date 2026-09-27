use std::{collections::HashMap, sync::Arc};

use http::{HeaderMap, Method, StatusCode};
use http_body_util::BodyExt as _;
use ipfs_s3_gateway::{
    crypto::key::MasterKey,
    import::{
        ImportConfig, ImportSource, downloader::SourceDownloader, pipeline::ImportCoordinator,
    },
    kubo::KuboClient,
    pinning::{coordinator::PinningCoordinator, decision::ExtensionDecision},
    s3::route::import_object::ImportObjectRoute,
    state::AppState,
    store::{
        Store,
        entities::{import_destination, import_job},
        import::{jobs::NewImportJob, ownership},
    },
};
use s3s::route::S3Route;
use s3s::{Body, S3Request};
use sea_orm::{ActiveModelTrait, Database, EntityTrait, Set};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
// Stage2 canonical JSON: ["cid",CID,"application/octet-stream",{"owner":"alice"},{"project":"alpha"},"prefix/"]
const STAGE2_FINGERPRINT: &str =
    "sha256:e4f119f7744ba6e7153a93aa3a5ce6a0bda435f1d4a5f89adbc7bf1cc25d0dc9";

fn submit_request(principal: &str, metadata_owner: &str) -> S3Request<Body> {
    let mut headers = HeaderMap::new();
    headers.insert("content-type", "application/xml".parse().unwrap());
    headers.insert("x-ipfs3-client-token", "legacy-token".parse().unwrap());
    headers.insert(
        "x-ipfs3-object-content-type",
        "application/octet-stream".parse().unwrap(),
    );
    headers.insert("x-amz-tagging", "project=alpha".parse().unwrap());
    headers.insert("x-amz-meta-owner", metadata_owner.parse().unwrap());
    S3Request {
        input: Body::from(format!(
            "<IPFS3ImportRequest><CID>{CID}</CID></IPFS3ImportRequest>"
        )),
        method: Method::POST,
        uri: "/bucket/key?ipfs3-import&decompress-zip=prefix%2F"
            .parse()
            .unwrap(),
        headers,
        extensions: http::Extensions::new(),
        credentials: Some(s3s::auth::Credentials {
            access_key: principal.to_owned(),
            secret_key: s3s::auth::SecretKey::from("test"),
        }),
        region: Some("us-east-1".parse().unwrap()),
        service: Some("s3".to_owned()),
        trailing_headers: None,
    }
}

#[tokio::test]
async fn stage2_token_replay_is_read_only_and_cannot_be_used_by_another_principal() {
    let db = Database::connect("sqlite::memory:").await.unwrap();
    ipfs_s3_gateway::store::run_migrations(&db).await.unwrap();
    ipfs_s3_gateway::store::bucket::create(&db, "bucket", Some("test"))
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let legacy = NewImportJob {
        id: id.clone(),
        bucket: "bucket".into(),
        key: "key".into(),
        source: ImportSource::Cid(CID.into()),
        request_fingerprint: STAGE2_FINGERPRINT.into(),
        client_token: Some("legacy-token".into()),
        object_content_type: Some("application/octet-stream".into()),
        metadata: HashMap::from([("owner".into(), "alice".into())]),
        tags: vec![ipfs_s3_gateway::pinning::tags::ObjectTag::new(
            "project", "alpha",
        )],
        decompress_prefix: Some("prefix/".into()),
    };
    ownership::submit(&db, legacy, chrono::Utc::now())
        .await
        .unwrap();
    let before = import_job::Entity::find_by_id(&id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let destination_before =
        import_destination::Entity::find_by_id(("bucket".into(), "key".into()))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
    assert!(before.pin_decision_json.is_none());
    let config = ImportConfig::default().validate().unwrap();
    let coordinator = ImportCoordinator::new(
        config.clone(),
        SourceDownloader::production(Arc::new(config)),
    );
    let state = Arc::new(AppState {
        kubo: KuboClient::new("http://127.0.0.1:1".into()),
        cold_kubo: None,
        store: Store::new(db),
        credentials: HashMap::new(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: PinningCoordinator::disabled_for_test(),
    });
    let route = ImportObjectRoute::new(state.clone(), coordinator);

    let response = route.call(submit_request("test", "alice")).await.unwrap();
    assert_eq!(response.status, Some(StatusCode::ACCEPTED));
    assert_eq!(response.headers["x-ipfs3-import-job-id"], id);
    assert!(!response.headers.contains_key("x-ipfs3-pin-warning"));
    let xml =
        String::from_utf8(response.output.collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(xml.contains("<State>queued</State>"));
    for req in [
        submit_request("other", "alice"),
        submit_request("test", "bob"),
    ] {
        let conflict = route.call(req).await.unwrap_err();
        assert_eq!(conflict.code().as_str(), "IdempotentParameterMismatch");
        assert_eq!(conflict.status_code(), Some(StatusCode::CONFLICT));
    }
    assert_eq!(
        import_job::Entity::find_by_id(&id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        import_destination::Entity::find_by_id(("bucket".into(), "key".into()))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap(),
        destination_before
    );

    let mut completed: import_job::ActiveModel = before.into();
    completed.state = Set("completed".into());
    completed.update(state.store.db()).await.unwrap();
    let replay = route.call(submit_request("test", "alice")).await.unwrap();
    assert_eq!(replay.headers["x-ipfs3-import-job-id"], id);
    let xml =
        String::from_utf8(replay.output.collect().await.unwrap().to_bytes().to_vec()).unwrap();
    assert!(xml.contains("<State>completed</State>"));
    assert!(
        import_job::Entity::find_by_id(id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
            .pin_decision_json
            .is_none()
    );

    let mut fresh = submit_request("test", "alice");
    fresh.uri = "/bucket/new-key?ipfs3-import&decompress-zip=prefix%2F"
        .parse()
        .unwrap();
    fresh
        .headers
        .insert("x-ipfs3-client-token", "new-token".parse().unwrap());
    let accepted = route.call(fresh).await.unwrap();
    let new_id = accepted.headers["x-ipfs3-import-job-id"].to_str().unwrap();
    let new_job = import_job::Entity::find_by_id(new_id)
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
    let decision: ExtensionDecision =
        serde_json::from_str(new_job.pin_decision_json.as_deref().unwrap()).unwrap();
    assert_eq!(decision.origin.principal_id, "test");
    assert_eq!(decision.origin.request_id, new_id);
    assert_eq!(
        new_job.request_fingerprint,
        "sha256:1406a95661268b7313f2a779378b0c494f8f344f6c5c48ea84a731bac19b08e1"
    );
    assert_ne!(new_job.request_fingerprint, STAGE2_FINGERPRINT);
    let mut new_replay = submit_request("test", "alice");
    new_replay.uri = "/bucket/new-key?ipfs3-import&decompress-zip=prefix%2F"
        .parse()
        .unwrap();
    new_replay
        .headers
        .insert("x-ipfs3-client-token", "new-token".parse().unwrap());
    let replay = route.call(new_replay).await.unwrap();
    assert_eq!(replay.headers["x-ipfs3-import-job-id"], new_id);
    let mut other = submit_request("other", "alice");
    other.uri = "/bucket/new-key?ipfs3-import&decompress-zip=prefix%2F"
        .parse()
        .unwrap();
    other
        .headers
        .insert("x-ipfs3-client-token", "new-token".parse().unwrap());
    assert_eq!(
        route.call(other).await.unwrap_err().code().as_str(),
        "IdempotentParameterMismatch"
    );
    assert_eq!(
        import_job::Entity::find_by_id(new_id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap(),
        new_job
    );
}
