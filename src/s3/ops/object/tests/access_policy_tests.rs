mod signed;

use super::*;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn policy_state() -> (Arc<AppState>, MockServer, String) {
    let (mut state, kubo) = versioned_read_state().await;
    let mut config = crate::config::Config::default_for_test();
    config.auth = toml::from_str(
        r#"
[[credentials]]
access_key = 'test-owner'
secret_key = 'test'

[[policy]]
effect = 'allow'
user = 'test-owner'
bucket = 'bucket'
actions = ['GetObject', 'HeadObject', 'DeleteObject']
existing_tags = { owner = 'team' }

[[policy]]
effect = 'allow'
user = 'test-owner'
bucket = 'bucket'
actions = ['PutObject', 'CopyObject']
request_tags = { owner = 'team' }

[[policy]]
effect = 'deny'
user = 'test-owner'
bucket = 'bucket'
actions = ['DeleteObject']
prefix = 'blocked/'
"#,
    )
    .unwrap();
    Arc::get_mut(&mut state).unwrap().credentials =
        crate::auth::GatewayCredentials::from_config(&config).unwrap();
    let historical = publish_versioned_read_object(
        &state,
        "policy-historical",
        "source.txt",
        "QmPolicyHistorical",
        false,
        None,
        None,
        vec![ObjectTag::new("owner", "team")],
    )
    .await;
    publish_versioned_read_object(
        &state,
        "policy-current",
        "source.txt",
        "QmPolicyCurrent",
        false,
        None,
        None,
        vec![ObjectTag::new("owner", "another-team")],
    )
    .await;
    (state, kubo, historical.version_id.unwrap())
}

#[tokio::test]
async fn anonymous_public_put_publishes_ordinary_tags_and_automatic_pins_only() {
    use axum::{body::Body, error_handling::HandleError};
    use http::{Method, Request, StatusCode};
    use tower::ServiceExt as _;

    let kubo = kubo_server(PIN_CID).await;
    let mut state = test_state(kubo.uri()).await;
    Arc::get_mut(&mut state).unwrap().pinning =
        configured_coordinator(&kubo.uri(), "always", "all", "");
    let db = state.store.db();
    crate::store::bucket::create_with_acl(
        db,
        "bucket",
        Some("test-owner"),
        Some("public-read-write"),
    )
    .await
    .unwrap();
    let mut builder =
        s3s::service::S3ServiceBuilder::new(crate::s3::handler::S3Impl::new(state.clone()));
    builder.set_auth(crate::auth::GatewayAuth::new(state.clone()));
    builder.set_access(access::GatewayAccess(state.clone()));
    let app = axum::Router::new().fallback_service(HandleError::new(
        builder.build(),
        |_: s3s::HttpError| async { StatusCode::INTERNAL_SERVER_ERROR },
    ));

    for request in [
        Request::put("/bucket/pin")
            .header("x-amz-tagging", "ipfs-s3%3Apin=true")
            .header("content-length", "4")
            .body(Body::from("body"))
            .unwrap(),
        Request::put("/bucket/zip")
            .header("x-ipfs3-zip-contract", "v2")
            .header("content-length", "4")
            .body(Body::from("body"))
            .unwrap(),
    ] {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
    assert!(kubo.received_requests().await.unwrap().is_empty());

    let request = Request::put("/bucket/plain")
        .header("x-amz-tagging", "team=writers")
        .header("content-length", "4")
        .body(Body::from("body"))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    for (method, uri) in [
        (Method::GET, "/bucket/plain?tagging"),
        (Method::DELETE, "/bucket/plain"),
    ] {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }
    let version = crate::store::object_version::resolve_version(
        db,
        "bucket",
        "plain",
        &VersionSelector::Current,
    )
    .await
    .unwrap();
    let decision = crate::store::pinning::decision::read_for_version(db, &version.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.origin.principal_id, access::ANONYMOUS_ORIGIN);
    let owner = version.object.unwrap().id;
    assert_eq!(
        crate::store::pinning::tags::list_object_tags(db, &owner)
            .await
            .unwrap(),
        vec![ObjectTag::new("team", "writers")]
    );
    let references = pin_references(db, &owner).await;
    assert_eq!(references.len(), 2);
    assert!(
        references
            .iter()
            .all(|reference| reference.source == "automatic")
    );
}

#[tokio::test]
async fn unbound_session_is_rejected_for_object_reads_and_writes() {
    let (mut state, kubo) = versioned_read_state().await;
    let mut cfg = crate::config::Config::default_for_test();
    cfg.auth.session_signing_key = Some("bc".repeat(32));
    Arc::get_mut(&mut state).unwrap().credentials =
        crate::auth::GatewayCredentials::from_config(&cfg).unwrap();
    let session = state.credentials.issue_session("test", 60).unwrap();
    let credentials = s3s::auth::Credentials {
        access_key: session.access_key_id,
        secret_key: session.secret_access_key.into(),
    };
    let mut get = get_object_request("file", None, Default::default());
    get.credentials = Some(credentials.clone());
    let mut put = put_request("file", None);
    put.credentials = Some(credentials);
    for error in [
        get_object(&state, get).await.unwrap_err(),
        put_object(&state, put).await.unwrap_err(),
    ] {
        assert_eq!(error.code().as_str(), "InvalidToken");
    }
    assert!(kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn reads_and_copy_authorize_the_selected_version_and_write_request_tags() {
    let (state, kubo, historical) = policy_state().await;
    let source = "source.txt";
    let destination = "copy.txt";
    let destination_bucket = "copy-target";
    crate::store::bucket::create_with_acl(
        state.store.db(),
        destination_bucket,
        Some("test-owner"),
        Some("private"),
    )
    .await
    .unwrap();
    let version = Some(historical.as_str());
    let mut replacement = http::HeaderMap::new();
    replacement.insert("x-amz-tagging-directive", "REPLACE".parse().unwrap());
    replacement.insert("x-amz-tagging", "owner=team".parse().unwrap());
    let denied = [
        get_object(&state, get_object_request(source, None, Default::default()))
            .await
            .unwrap_err(),
        head_object(
            &state,
            head_object_request(source, None, Default::default()),
        )
        .await
        .unwrap_err(),
        copy_object(
            &state,
            copy_request(source, destination, Some("REPLACE"), Some("owner=team")),
        )
        .await
        .unwrap_err(),
        copy_object(
            &state,
            copy_request_version(source, version, destination, Default::default()),
        )
        .await
        .unwrap_err(),
        copy_object(
            &state,
            copy_request_version("missing", version, destination, replacement.clone()),
        )
        .await
        .unwrap_err(),
        put_object(&state, put_request("put.txt", Some("owner=another-team")))
            .await
            .unwrap_err(),
    ];
    for error in denied {
        assert_eq!(error.code().as_str(), "AccessDenied");
    }
    assert!(kubo.received_requests().await.unwrap().is_empty());

    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("arg", "QmPolicyHistorical"))
        .respond_with(ResponseTemplate::new(200).set_body_string("body"))
        .mount(&kubo)
        .await;
    let get = get_object(
        &state,
        get_object_request(source, version, Default::default()),
    )
    .await
    .unwrap();
    assert_eq!(get.output.version_id.as_deref(), version);
    assert_eq!(read_get_body(get).await, b"body");
    let head = head_object(
        &state,
        head_object_request(source, version, Default::default()),
    )
    .await
    .unwrap();
    assert_eq!(head.output.version_id.as_deref(), version);

    let mut denied_copy = copy_request_version(source, None, destination, replacement.clone());
    denied_copy.input.bucket = destination_bucket.to_owned();
    assert_eq!(
        copy_object(&state, denied_copy)
            .await
            .unwrap_err()
            .code()
            .as_str(),
        "AccessDenied"
    );
    let mut request = copy_request_version(source, version, destination, replacement);
    request.input.bucket = destination_bucket.to_owned();
    let copy = copy_object(&state, request).await.unwrap();
    assert_eq!(copy.output.copy_source_version_id.as_deref(), version);
    assert_eq!(
        crate::store::object::get_latest(state.store.db(), destination_bucket, destination)
            .await
            .unwrap()
            .cid,
        "QmPolicyHistorical"
    );
}

#[tokio::test]
async fn object_reads_query_tags_only_when_policy_needs_them() {
    let (mut state, kubo, historical) = policy_state().await;
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = statements.clone();
    let mut db = state.store.db().clone();
    db.set_metric_callback(move |info| {
        observed.lock().unwrap().push(info.statement.sql.clone());
    });
    Arc::get_mut(&mut state).unwrap().store = crate::store::Store::new(db);
    let configured = state.credentials.clone();
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("arg", "QmPolicyHistorical"))
        .respond_with(ResponseTemplate::new(200).set_body_string("body"))
        .mount(&kubo)
        .await;

    for (credentials, tag_queries) in [(Default::default(), 0), (configured, 2)] {
        Arc::get_mut(&mut state).unwrap().credentials = credentials;
        let version = Some(historical.as_str());
        let get = get_object(
            &state,
            get_object_request("source.txt", version, Default::default()),
        )
        .await
        .unwrap();
        assert_eq!(read_get_body(get).await, b"body");
        let head = head_object(
            &state,
            head_object_request("source.txt", version, Default::default()),
        )
        .await
        .unwrap();
        assert_eq!(head.output.version_id.as_deref(), version);
        let statements = std::mem::take(&mut *statements.lock().unwrap());
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.contains("object_tags"))
                .count(),
            tag_queries,
        );
    }
}

#[tokio::test]
async fn definite_delete_denials_preserve_error_order_without_mutating() {
    let (mut state, _kubo, _) = policy_state().await;
    let key = "blocked/object";
    publish_versioned_read_object(
        &state,
        "blocked-owner",
        key,
        "QmBlocked",
        false,
        None,
        None,
        vec![ObjectTag::new("owner", "team")],
    )
    .await;
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = statements.clone();
    let mut db = state.store.db().clone();
    db.set_metric_callback(move |info| {
        observed.lock().unwrap().push(info.statement.sql.clone());
    });
    Arc::get_mut(&mut state).unwrap().store = crate::store::Store::new(db);

    for (bucket, version, expected) in [
        ("missing", Some("not-a-version"), "InvalidArgument"),
        ("missing", None, "NoSuchBucket"),
        ("bucket", None, "AccessDenied"),
    ] {
        let mut request = delete_object_version_request(key, version);
        request.input.bucket = bucket.to_owned();
        assert_eq!(
            delete_object(&state, request)
                .await
                .unwrap_err()
                .code()
                .as_str(),
            expected
        );
        let sql = std::mem::take(&mut *statements.lock().unwrap());
        assert!(!sql.iter().any(|sql| {
            sql.contains("object_versions")
                || sql.contains("object_tags")
                || sql.starts_with("UPDATE \"buckets\"")
        }));
    }

    let batch = delete_objects(
        &state,
        delete_objects_version_request(
            vec![
                (key.to_owned(), None),
                (key.to_owned(), Some("not-a-version".to_owned())),
                (key.to_owned(), None),
            ],
            false,
        ),
    )
    .await
    .unwrap()
    .output;
    assert!(batch.deleted.is_none());
    let errors = batch.errors.unwrap();
    assert_eq!(
        errors
            .iter()
            .map(|error| error.code.as_deref())
            .collect::<Vec<_>>(),
        [
            Some("AccessDenied"),
            Some("InvalidArgument"),
            Some("AccessDenied")
        ]
    );
    let sql = std::mem::take(&mut *statements.lock().unwrap());
    // Only the shared bucket lookup is needed when every item is denied or malformed.
    assert_eq!(
        sql.iter()
            .filter(|sql| sql.starts_with("SELECT") && sql.contains("buckets"))
            .count(),
        1
    );
    assert!(!sql.iter().any(|sql| {
        sql.contains("object_versions")
            || sql.contains("object_tags")
            || sql.starts_with("UPDATE \"buckets\"")
    }));
    let current = crate::store::object::get_latest(state.store.db(), "bucket", key)
        .await
        .unwrap();
    assert_eq!(current.id, "blocked-owner");
}

#[tokio::test]
async fn delete_objects_authorizes_each_requested_version() {
    let (state, _kubo, historical) = policy_state().await;
    let output = delete_objects(
        &state,
        delete_objects_version_request(
            vec![
                ("source.txt".to_owned(), None),
                ("source.txt".to_owned(), Some(historical.clone())),
            ],
            false,
        ),
    )
    .await
    .unwrap()
    .output;
    let [denial] = output.errors.as_deref().unwrap() else {
        panic!("expected one denied item");
    };
    assert_eq!(denial.code.as_deref(), Some("AccessDenied"));
    assert_eq!(denial.version_id, None);
    let [deleted] = output.deleted.as_deref().unwrap() else {
        panic!("expected one deleted item");
    };
    assert_eq!(deleted.version_id.as_deref(), Some(historical.as_str()));
    let current = crate::store::object::get_latest(state.store.db(), "bucket", "source.txt")
        .await
        .unwrap();
    assert_eq!(current.id, "policy-current");
}
