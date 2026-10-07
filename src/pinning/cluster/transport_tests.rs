use super::{tests::*, *};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn http_failures_never_become_empty_complete_queries_or_leak_error_text() {
    for (status, expected) in [
        (401, VerificationCode::Authentication),
        (403, VerificationCode::UnknownForbidden),
        (404, VerificationCode::NotFound),
        (429, VerificationCode::RateLimited),
        (503, VerificationCode::HttpStatus),
    ] {
        let server = MockServer::start().await;
        mounts(
            &server,
            allocation(&["a", "b"]),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path(format!("/pins/{CID}")))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_string(format!("{} token-sentinel", server.uri())),
            )
            .with_priority(1)
            .mount(&server)
            .await;
        let result = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(result.outcome(), ReplicaOutcome::Unknown);
        let error = result.failure().unwrap();
        assert_eq!(error.code, expected);
        assert_eq!(error.http_status, Some(status));
        assert!(!format!("{result:?} {error}").contains("token-sentinel"));
        assert!(!format!("{result:?} {error}").contains(&server.uri()));
    }
}

#[tokio::test]
async fn redirects_are_not_followed_even_to_the_same_origin() {
    for status in [301, 302, 303, 307, 308] {
        let origin = MockServer::start().await;
        let other = MockServer::start().await;
        for location in [
            format!("{}/version", other.uri()),
            format!("{}/another", origin.uri()),
        ] {
            origin.reset().await;
            Mock::given(method("GET"))
                .and(path("/version"))
                .respond_with(ResponseTemplate::new(status).insert_header("Location", location))
                .mount(&origin)
                .await;
            let mut p = profile(&origin.uri(), 2);
            p.authorization = Some(reqwest::header::HeaderValue::from_static(
                "Bearer token-sentinel",
            ));
            let result = ClusterReplicaVerifier::new(p).unwrap().observe(CID).await;
            assert_eq!(
                result.failure().unwrap().code,
                VerificationCode::RedirectRejected
            );
            assert_eq!(origin.received_requests().await.unwrap().len(), 1);
            assert!(other.received_requests().await.unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn authentication_is_redacted_and_only_sent_to_the_fixed_rest_origin() {
    for auth in [
        ClusterAuth::Bearer("token-sentinel".into()),
        ClusterAuth::Basic {
            username: "user-sentinel".into(),
            password: "password-sentinel".into(),
        },
    ] {
        let server = MockServer::start().await;
        mounts(
            &server,
            allocation(&["a", "b"]),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        )
        .await;
        let p = ClusterRestProfile::new_with_network_policy(
            ClusterPinset::new("cluster", "pinset").unwrap(),
            Some(&server.uri()),
            Some(auth.clone()),
            2,
            RestLimits::default(),
            ClusterNetworkPolicy::AllowPrivate,
        )
        .unwrap();
        let debug = format!("{auth:?} {p:?}");
        for secret in ["token-sentinel", "user-sentinel", "password-sentinel"] {
            assert!(!debug.contains(secret));
        }
        let result = ClusterReplicaVerifier::new(p).unwrap().observe(CID).await;
        assert_eq!(result.outcome(), ReplicaOutcome::Complete);
        for request in server.received_requests().await.unwrap() {
            assert!(request.headers.contains_key("authorization"));
            assert_eq!(request.url.host_str(), Some("localhost"));
        }
    }
}

#[tokio::test]
async fn malformed_wrong_version_and_oversized_bodies_are_unknown() {
    for (body, code) in [
        ("{bad".to_owned(), VerificationCode::MalformedResponse),
        (
            json!({"version": "1.1.7"}).to_string(),
            VerificationCode::UnsupportedVersion,
        ),
        (
            json!({"version": "1.1.6-rc1"}).to_string(),
            VerificationCode::UnsupportedVersion,
        ),
        (
            "x".repeat(256 * 1024 + 1),
            VerificationCode::ResponseTooLarge,
        ),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/version"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let result = ClusterReplicaVerifier::new(profile(&server.uri(), 2))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(result.outcome(), ReplicaOutcome::Unknown);
        assert_eq!(result.failure().unwrap().code, code);
    }
}

#[test]
fn configuration_rejects_userinfo_query_fragments_and_unbounded_limits() {
    for endpoint in [
        "https://user:token@localhost",
        "https://localhost?local=true",
        "https://localhost#token",
        "file:///tmp/rest",
    ] {
        let error = ClusterRestProfile::new(
            ClusterPinset::new("cluster", "pinset").unwrap(),
            Some(endpoint),
            None,
            2,
            RestLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code, VerificationCode::InvalidConfiguration);
        assert!(!format!("{error:?} {error}").contains(endpoint));
    }
    let limits = RestLimits {
        max_body_bytes: usize::MAX,
        ..RestLimits::default()
    };
    assert!(
        ClusterRestProfile::new(
            ClusterPinset::new("cluster", "pinset").unwrap(),
            Some("http://localhost"),
            None,
            2,
            limits
        )
        .is_err()
    );
}
