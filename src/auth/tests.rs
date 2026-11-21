use http::{Extensions, HeaderMap, HeaderValue, Method, Uri, header::AUTHORIZATION};
use s3s::{S3Request, auth::Credentials};

use super::*;
use crate::access::{bind_custom, subject};

fn config() -> Config {
    let mut config = Config::default_for_test();
    config.auth.session_signing_key = Some("ab".repeat(32));
    config
}

#[test]
fn auth_config_rejects_unknown_fields_and_unknown_policy_users() {
    assert!(toml::from_str::<Config>("[auth]\npolciy = []").is_err());
    let mut cfg: Config = toml::from_str(
        r#"
        [[auth.policy]]
        effect = 'deny'
        actions = ['GetObject']
        user = 'reader'
    "#,
    )
    .unwrap();
    assert!(GatewayCredentials::from_config(&cfg).is_err());
    cfg.auth.credentials[0].access_key = "reader".to_owned();
    assert!(GatewayCredentials::from_config(&cfg).is_ok());
}

#[test]
fn anonymous_provenance_is_reserved_without_blocking_the_legacy_static_key() {
    let mut cfg = config();
    let legacy_key = "ipfs-s3:anonymous";
    cfg.auth.credentials[0].access_key = legacy_key.to_owned();
    let credentials = GatewayCredentials::from_config(&cfg).unwrap();
    assert_eq!(credentials.secret_key(legacy_key).unwrap().expose(), "test");
    assert_eq!(
        credentials
            .principal(legacy_key, &HeaderMap::new(), &Uri::from_static("/"))
            .unwrap(),
        legacy_key
    );
    assert!(credentials.issue_session(legacy_key, 60).is_ok());

    cfg.auth.credentials[0].access_key = crate::access::ANONYMOUS_ORIGIN.to_owned();
    assert!(GatewayCredentials::from_config(&cfg).is_err());
}

#[test]
fn issuance_requires_known_subject_valid_duration_and_separate_key_namespace() {
    let mut cfg = config();
    let credentials = GatewayCredentials::from_config(&cfg).unwrap();
    assert!(credentials.issue_session("unknown", 60).is_err());
    assert!(credentials.issue_session("test", 0).is_err());
    assert!(credentials.issue_session("test", 43_201).is_err());
    let session = credentials.issue_session("test", 43_200).unwrap();
    cfg.auth.credentials[0]
        .access_key
        .clone_from(&session.access_key_id);
    assert!(GatewayCredentials::from_config(&cfg).is_err());

    cfg.auth.session_signing_key = None;
    let static_only = GatewayCredentials::from_config(&cfg).unwrap();
    assert!(
        static_only
            .issue_session(&session.access_key_id, 60)
            .is_err()
    );
    assert_eq!(
        static_only
            .secret_key(&session.access_key_id)
            .unwrap()
            .expose(),
        "test"
    );
    assert_eq!(
        static_only
            .principal(
                &session.access_key_id,
                &HeaderMap::new(),
                &Uri::from_static("/")
            )
            .unwrap(),
        session.access_key_id
    );
    cfg.auth.session_signing_key = Some("not-hex".into());
    assert!(GatewayCredentials::from_config(&cfg).is_err());
}

#[tokio::test]
async fn custom_access_binds_only_verified_session_subjects() {
    let mut cfg = config();
    cfg.storage.database_url = "sqlite::memory:".into();
    let state = AppState::new_with_env(&cfg, |_| None).await.unwrap();
    let mut req = S3Request {
        input: (),
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::new(),
        credentials: Some(Credentials {
            access_key: "test".into(),
            secret_key: SecretKey::from("test"),
        }),
        region: None,
        service: None,
        trailing_headers: None,
    };
    bind_custom(&state, &mut req).unwrap();
    assert!(req.extensions.is_empty());
    assert_eq!(subject(&state, &req).unwrap(), Some("test"));

    req.credentials.as_mut().unwrap().access_key = "unknown".into();
    assert_eq!(
        bind_custom(&state, &mut req).unwrap_err().code().as_str(),
        "InvalidAccessKeyId"
    );

    let session = state.credentials.issue_session("test", 60).unwrap();
    req.credentials = Some(Credentials {
        access_key: session.access_key_id,
        secret_key: session.secret_access_key.into(),
    });
    assert_eq!(
        bind_custom(&state, &mut req).unwrap_err().code().as_str(),
        "InvalidToken"
    );
    req.uri = format!(
        "/?X-Amz-Signature=sig&X-Amz-Security-Token={}",
        session.session_token
    )
    .parse()
    .unwrap();
    assert_eq!(
        subject(&state, &req).unwrap_err().code().as_str(),
        "InvalidToken"
    );
    bind_custom(&state, &mut req).unwrap();
    assert_eq!(subject(&state, &req).unwrap(), Some("test"));
}

#[test]
fn header_tokens_require_coverage_and_must_not_mix_with_presigned_tokens() {
    let auth = HeaderValue::from_static(
        "AWS4-HMAC-SHA256 Credential=id, SignedHeaders=host;x-amz-security-token, Signature=sig",
    );
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, auth.clone());
    let uri = Uri::from_static("/");
    assert!(session::request_token(&headers, &uri).is_err());
    headers.insert("x-amz-security-token", HeaderValue::from_static("token"));
    assert_eq!(session::request_token(&headers, &uri).unwrap(), "token");
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=id, SignedHeaders=host, Signature=sig",
        ),
    );
    assert!(session::request_token(&headers, &uri).is_err());
    headers.insert(AUTHORIZATION, auth);
    headers.append("x-amz-security-token", HeaderValue::from_static("second"));
    assert!(session::request_token(&headers, &uri).is_err());
    let query = Uri::from_static("/?X-Amz-Signature=sig&X-Amz-Security%2DToken=token");
    assert!(session::request_token(&headers, &query).is_err());
    headers.clear();
    assert_eq!(session::request_token(&headers, &query).unwrap(), "token");
    for invalid in [
        "/?X-Amz-Security-Token=token",
        "/?X-Amz-Signature=sig",
        "/?X-Amz-Signature=sig&X-Amz-Security-Token=one&X-Amz-Security-Token=two",
    ] {
        assert!(session::request_token(&headers, &invalid.parse().unwrap()).is_err());
    }
}

#[test]
fn token_binds_key_subject_and_expiry_across_gateways() {
    let mut cfg = config();
    let issuer = GatewayCredentials::from_config(&cfg).unwrap();
    let receiver = GatewayCredentials::from_config(&cfg).unwrap();
    let first = issuer.issue_session("test", 60).unwrap();
    let second = issuer.issue_session("test", 60).unwrap();
    assert_ne!(first.access_key_id, second.access_key_id);
    assert_ne!(first.secret_access_key, second.secret_access_key);
    assert_eq!(
        first.secret_access_key,
        receiver.secret_key(&first.access_key_id).unwrap().expose()
    );
    let uri = format!(
        "/?X-Amz-Signature=sig&X-Amz-Security-Token={}",
        first.session_token
    )
    .parse::<Uri>()
    .unwrap();
    let headers = HeaderMap::new();
    assert_eq!(
        receiver
            .principal(&first.access_key_id, &headers, &uri)
            .unwrap(),
        "test"
    );
    assert!(
        receiver
            .principal(&second.access_key_id, &headers, &uri)
            .is_err()
    );
    let expiration = chrono::DateTime::parse_from_rfc3339(&first.expiration)
        .unwrap()
        .timestamp();
    let signer = receiver.sessions.as_ref().unwrap();
    assert_eq!(
        signer
            .verify(&first.access_key_id, &first.session_token, expiration - 1)
            .unwrap(),
        "test"
    );
    assert_eq!(
        signer
            .verify(&first.access_key_id, &first.session_token, expiration)
            .unwrap_err()
            .code()
            .as_str(),
        "ExpiredToken"
    );
    let (_, tag) = first.session_token.split_once('.').unwrap();
    assert!(
        signer
            .verify(
                &first.access_key_id,
                &format!("Y2hhbmdlZA.{tag}"),
                expiration - 1
            )
            .is_err()
    );

    cfg.auth.credentials.clear();
    assert!(
        GatewayCredentials::from_config(&cfg)
            .unwrap()
            .principal(&first.access_key_id, &headers, &uri)
            .is_err()
    );
    cfg = config();
    cfg.auth.session_signing_key = Some("cd".repeat(32));
    assert!(
        GatewayCredentials::from_config(&cfg)
            .unwrap()
            .principal(&first.access_key_id, &headers, &uri)
            .is_err()
    );
}
