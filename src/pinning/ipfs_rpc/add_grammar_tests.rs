use super::tests::{CID, OTHER, provider, source_file, submit};
use super::*;
use crate::pinning::provider::{PinningProvider, ProviderErrorClass, RemotePin};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ZERO_PROGRESS: &str = "{\"Name\":\"object\"}\n";

fn root(cid: &str) -> String {
    format!("{{\"Name\":\"object\",\"Hash\":\"{cid}\",\"Size\":\"0\"}}\n")
}

async fn add_ack(profile: RpcProfile, records: &str) -> Result<RemotePin, ProviderError> {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source, CID, b"").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(records))
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, profile, RpcStrategy::Upload);
    let result = p.submit(submit(CID)).await;
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
    result
}

#[tokio::test]
async fn kubo_accepts_name_only_and_explicit_initial_zero_progress() {
    for records in [
        format!("{ZERO_PROGRESS}{}", root(CID)),
        format!("{{\"Bytes\":0}}\n{}", root(CID)),
        format!(
            "{{\"Bytes\":0}}\n{ZERO_PROGRESS}{ZERO_PROGRESS}{}",
            root(CID)
        ),
        format!("{ZERO_PROGRESS}{{\"Bytes\":1}}\n{}", root(CID)),
        format!("{ZERO_PROGRESS}{}", root(CID).trim_end()),
    ] {
        assert_eq!(add_ack(RpcProfile::Kubo, &records).await.unwrap().cid, CID);
    }
}

#[tokio::test]
async fn kubo_rejects_malformed_name_only_records() {
    for invalid in [
        "{}",
        "[]",
        "null",
        "{\"Unknown\":0}",
        "{\"Name\":\"\"}",
        "{\"Name\":\"other\"}",
        "{\"Name\":null}",
        "{\"Name\":0}",
        "{\"Name\":\"object\",\"Bytes\":null}",
        "{\"Name\":\"object\",\"Hash\":null}",
        "{\"Name\":\"object\",\"Size\":null}",
        "{\"Name\":\"object\",\"Size\":\"0\"}",
        "{\"Name\":\"object\",\"Unknown\":true}",
        "{\"Name\":\"object\",\"Message\":\"failure\",\"Code\":0}",
        "{\"Name\":\"object\",\"Name\":\"object\"}",
    ] {
        let records = format!("{invalid}\n{}", root(CID));
        let error = add_ack(RpcProfile::Kubo, &records).await.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol, "{records}");
    }
}

#[tokio::test]
async fn filebase_does_not_support_kubos_name_only_progress() {
    let records = format!("{ZERO_PROGRESS}{}", root(CID));
    let error = add_ack(RpcProfile::Filebase, &records).await.unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Protocol);
}

#[tokio::test]
async fn kubo_requires_monotonic_progress_followed_by_one_matching_root() {
    for records in [
        ZERO_PROGRESS.to_owned(),
        format!("{{\"Bytes\":1}}\n{ZERO_PROGRESS}{}", root(CID)),
        format!("{{\"Bytes\":2}}\n{{\"Bytes\":1}}\n{}", root(CID)),
        format!("{}{ZERO_PROGRESS}", root(CID)),
        format!("{}{{\"Bytes\":0}}\n", root(CID)),
        format!("{}{}", root(CID), root(OTHER)),
        format!("{}{}", root(CID), root(CID)),
        root(OTHER),
    ] {
        let error = add_ack(RpcProfile::Kubo, &records).await.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol, "{records}");
    }
}
