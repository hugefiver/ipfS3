use super::*;
use crate::{
    import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
    pinning::{
        config::{LeaseDuration, ProviderMode},
        policy::{LeaseIntent, LeaseSource},
        tags::ContentMode,
    },
    s3::{handler::S3Impl, route::gateway::GatewayRoute},
    store::pinning::publication::PinTargetSpec,
};
use axum::error_handling::HandleError;
use s3s::service::S3ServiceBuilder;

#[tokio::test]
async fn signed_status_reports_the_selected_files_resource_acknowledgments() {
    let mut versions = versioned_fixture().await;
    Arc::get_mut(&mut versions.fixture.state)
        .unwrap()
        .credentials = HashMap::from([("test".into(), s3s::auth::SecretKey::from("test"))]).into();
    seed_tags_for_owner(
        &versions.fixture,
        &versions.historical_object_id,
        &[("team", "historical")],
    )
    .await;
    seed_tags_for_owner(
        &versions.fixture,
        &versions.current_object_id,
        &[("team", "current")],
    )
    .await;
    set_access_policy(
        &mut versions.fixture,
        &format!(
            r#"
            [[policy]]
            effect = 'allow'
            user = 'test'
            actions = ['GetPinningStatus']
            version = '{}'
            existing_tags.team = 'historical'

            [[policy]]
            effect = 'allow'
            user = 'test'
            actions = ['GetPinningStatus']
            version = '{}'
            existing_tags.team = 'current'
            "#,
            versions.historical_version_id, versions.current_version_id,
        ),
    );
    let state = &versions.fixture.state;
    let db = state.store.db();
    let txn = db.begin().await.unwrap();
    let now = Utc::now();
    let cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    resources::attach(
        &txn,
        &versions.historical_object_id,
        &PinTargetSpec {
            cid: cid.into(),
            logical_size: 10,
        },
        &LeaseIntent {
            source: LeaseSource::Manual,
            policy_id: versions.fixture.policy_id.clone(),
            provider_mode: ProviderMode::All,
            providers: vec!["alpha".into(), "beta".into()],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("4h").unwrap(),
        },
        state.pinning.provider_limits(),
        now,
    )
    .await
    .unwrap();
    txn.execute_unprepared("UPDATE pin_resources SET status='accepted', handle='private-handle', action=NULL WHERE provider='alpha'").await.unwrap();
    txn.commit().await.unwrap();

    let config = ImportConfig::default().validate().unwrap();
    let imports = ImportCoordinator::new(
        config.clone(),
        SourceDownloader::production(Arc::new(config)),
    );
    let mut builder = S3ServiceBuilder::new(S3Impl::new(state.clone()));
    builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
    builder.set_route(GatewayRoute::new(state.clone(), imports));
    let app = axum::Router::new().fallback_service(HandleError::new(
        builder.build(),
        |_: s3s::HttpError| async {
            http::Response::builder()
                .status(500)
                .body(s3s::Body::empty())
                .unwrap()
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let get = async |version: Option<&str>| {
        let mut query = vec![("ipfs3-pin-status", "")];
        if let Some(version) = version {
            query.push(("versionId", version));
        }
        sigv4::send_sigv4(
            reqwest::Method::GET,
            &endpoint,
            BUCKET,
            KEY,
            &query,
            vec![],
            http::HeaderMap::new(),
            "test",
        )
        .await
    };
    assert_eq!(get(None).await.status(), reqwest::StatusCode::FORBIDDEN);
    let response = get(Some(&versions.historical_version_id)).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers()["x-amz-version-id"],
        versions.historical_version_id
    );
    let json: serde_json::Value = response.json().await.unwrap();
    let refs = json["references"].as_array().unwrap();
    assert_eq!(refs.len(), 2);
    assert_eq!(refs[0]["provider"], "alpha");
    assert_eq!(refs[0]["status"], "accepted");
    assert_eq!(refs[0]["cid"], cid);
    assert!(refs[0].get("handle").is_none());
    assert_eq!(refs[1]["status"], "waiting");
    let current: serde_json::Value = get(Some(&versions.current_version_id))
        .await
        .json()
        .await
        .unwrap();
    assert!(current["references"].as_array().unwrap().is_empty());
    server.abort();
}
