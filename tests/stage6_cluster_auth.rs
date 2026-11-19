//! Authentication and response redaction at the registered Cluster write boundary.
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::PinningCoordinator,
        provider::{ProviderErrorClass, SubmitPin},
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ERROR_BODY: &str = "private-rest-auth-body-token-do-not-persist";

fn config(rest: &MockServer) -> Config {
    toml::from_str(&format!(
        r#"
[[pinning.providers]]
name = "auth-cluster"
kind = "cluster"
strategy = "cid"
priority = 1
max_bytes = 1048576
max_pins = 10
[pinning.providers.cluster]
protocol = "rest"
rest_endpoint = "{}"
allow_private_network = true
[pinning_identity]
primary_storage_domain = "kubo-primary"
[[pinning_identity.providers]]
config_name = "auth-cluster"
provider_id = "stage6-auth-cluster"
display_name = "Auth scope"
backend = "cluster"
scope = "backup-pinset"
storage_domain = "cluster-independent"
credential_revision = 1
endpoint_revision = 1
api_profile = "cluster-rest"
strategy = "cid"
"#,
        rest.uri()
    ))
    .unwrap()
}

#[tokio::test]
async fn registered_cluster_writes_classify_failures_and_redact_response_bodies() {
    for (status, class) in [
        (401, ProviderErrorClass::Authentication),
        (403, ProviderErrorClass::UnknownForbidden),
        (404, ProviderErrorClass::Protocol),
        (429, ProviderErrorClass::RateLimited),
        (503, ProviderErrorClass::Protocol),
    ] {
        let rest = MockServer::start().await;
        for verb in ["POST", "DELETE"] {
            Mock::given(method(verb))
                .and(path(format!("/pins/{CID}")))
                .respond_with(ResponseTemplate::new(status).set_body_string(ERROR_BODY))
                .expect(1)
                .mount(&rest)
                .await;
        }
        let coordinator = PinningCoordinator::build(
            ValidatedPinningConfig::from_config(&config(&rest), |_| None).unwrap(),
        )
        .unwrap();
        let provider = coordinator
            .provider(&coordinator.effective_config().providers[0].name)
            .unwrap();
        let submitted = provider
            .submit(SubmitPin {
                cid: CID.into(),
                name: "opaque".into(),
                metadata: Default::default(),
            })
            .await
            .unwrap_err();
        let deleted = provider.unpin(CID).await.unwrap_err();
        for error in [submitted, deleted] {
            assert_eq!(error.class, class);
            assert!(!error.message.contains(ERROR_BODY));
            assert!(!error.message.contains(&rest.uri()));
        }
        assert_eq!(rest.received_requests().await.unwrap().len(), 2);
    }
}
