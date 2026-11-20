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
    let version = Some(historical.as_str());
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

    let mut headers = http::HeaderMap::new();
    headers.insert(
        "x-amz-tagging-directive",
        http::HeaderValue::from_static("REPLACE"),
    );
    headers.insert(
        "x-amz-tagging",
        http::HeaderValue::from_static("owner=team"),
    );
    let copy = copy_object(
        &state,
        copy_request_version(source, version, destination, headers),
    )
    .await
    .unwrap();
    assert_eq!(copy.output.copy_source_version_id.as_deref(), version);
    assert_eq!(
        crate::store::object::get_latest(state.store.db(), "bucket", destination)
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
        assert!(statements.iter().any(|sql| sql.contains("object_versions")));
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
