use super::tests::{CID, OTHER, local, provider, source_file, submit};
use super::*;
use crate::pinning::{identity::Ownership, provider::ProviderErrorClass};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const ZERO_PROGRESS: &str = "{\"Name\":\"object\"}\n";

fn root(cid: &str) -> String {
    format!("{{\"Name\":\"object\",\"Hash\":\"{cid}\",\"Size\":\"0\"}}\n")
}

async fn add_observation(
    profile: RpcProfile,
    records: &str,
) -> (RpcSubmitObservation, Vec<Request>) {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source, CID, b"").await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(records))
        .expect(1)
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})))
        .mount(&target)
        .await;
    local(&target, CID).await;
    let p = provider(&target, &source, profile, RpcStrategy::Upload);
    let observation = p.submit_observed(submit(CID)).await;
    (observation, target.received_requests().await.unwrap())
}

#[tokio::test]
async fn kubo_043_empty_add_accepts_name_only_zero_progress_before_root() {
    // Exact complete NDJSON captured from Kubo 0.43 add?progress=true&pin=false.
    let records = format!("{ZERO_PROGRESS}{}", root(CID));
    let (observation, requests) = add_observation(RpcProfile::Kubo, &records).await;
    assert!(observation.result.is_ok(), "{observation:?}");
    assert_eq!(observation.effect, RpcSubmitEffect::Observed);
    assert_eq!(observation.resources.len(), 1);
    let resource = &observation.resources[0];
    assert_eq!(resource.cid, CID);
    assert_eq!(resource.status, RpcResourceStatus::RecursiveVerified);
    assert_eq!(resource.ownership, Ownership::Unknown);
    let commands: Vec<_> = requests.iter().map(|r| r.url.path()).collect();
    assert_eq!(&commands[..2], ["/api/v0/add", "/api/v0/pin/add"]);
    let query: Vec<_> = requests[0].url.query_pairs().collect();
    assert!(query.iter().any(|(k, v)| k == "progress" && v == "true"));
    assert!(query.iter().any(|(k, v)| k == "pin" && v == "false"));
    assert!(!commands.contains(&"/api/v0/pin/rm"));
}

#[tokio::test]
async fn name_only_and_explicit_zero_progress_share_initial_zero_semantics() {
    for records in [
        format!("{{\"Bytes\":0}}\n{}", root(CID)),
        format!(
            "{{\"Bytes\":0}}\n{ZERO_PROGRESS}{ZERO_PROGRESS}{}",
            root(CID)
        ),
        format!("{ZERO_PROGRESS}{{\"Bytes\":1}}\n{}", root(CID)),
        format!("{ZERO_PROGRESS}{}", root(CID).trim_end()),
    ] {
        let (observation, _) = add_observation(RpcProfile::Kubo, &records).await;
        assert!(observation.result.is_ok(), "{records}: {observation:?}");
        assert_eq!(observation.effect, RpcSubmitEffect::Observed);
    }
}

#[tokio::test]
async fn name_only_exception_rejects_empty_unknown_null_and_extra_fields() {
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
        for before_root in [true, false] {
            let records = if before_root {
                format!("{invalid}\n{}", root(CID))
            } else {
                format!("{}{invalid}\n", root(CID))
            };
            let (observation, requests) = add_observation(RpcProfile::Kubo, &records).await;
            let error = observation.result.unwrap_err();
            assert_eq!(error.class, ProviderErrorClass::Protocol, "{records}");
            assert!(!error.definitely_not_submitted());
            assert_eq!(observation.effect, RpcSubmitEffect::Unknown, "{records}");
            if before_root {
                assert!(observation.resources.is_empty(), "{records}");
            } else {
                assert_eq!(observation.resources.len(), 1, "{records}");
                assert_eq!(observation.resources[0].cid, CID);
                assert_eq!(observation.resources[0].status, RpcResourceStatus::Reported);
                assert_eq!(observation.resources[0].ownership, Ownership::Unknown);
            }
            assert_eq!(requests.len(), 1, "must stop after add: {records}");
        }
    }
}

#[tokio::test]
async fn name_only_zero_progress_cannot_regress_bytes_or_follow_a_root() {
    for (records, reported) in [
        (
            format!("{{\"Bytes\":1}}\n{ZERO_PROGRESS}{}", root(CID)),
            false,
        ),
        (format!("{}{ZERO_PROGRESS}", root(CID)), true),
        (format!("{}{ZERO_PROGRESS}{}", root(CID), root(CID)), true),
    ] {
        let (observation, requests) = add_observation(RpcProfile::Kubo, &records).await;
        let error = observation.result.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        assert!(!error.definitely_not_submitted());
        assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
        assert_eq!(observation.resources.len(), usize::from(reported));
        if reported {
            assert_eq!(observation.resources[0].status, RpcResourceStatus::Reported);
            assert_eq!(observation.resources[0].ownership, Ownership::Unknown);
        }
        assert_eq!(requests.len(), 1);
    }
}

#[tokio::test]
async fn filebase_no_progress_contract_does_not_gain_name_only_grammar() {
    for (records, success) in [
        (root(CID), true),
        (format!("{ZERO_PROGRESS}{}", root(CID)), false),
    ] {
        let (observation, requests) = add_observation(RpcProfile::Filebase, &records).await;
        assert_eq!(observation.result.is_ok(), success, "{observation:?}");
        let keys: Vec<_> = requests[0]
            .url
            .query_pairs()
            .map(|(k, _)| k.into_owned())
            .collect();
        assert_eq!(keys, ["cid-version", "wrap-with-directory"]);
        assert!(requests.iter().all(|r| r.url.path() != "/api/v0/pin/add"));
        if success {
            assert_eq!(observation.effect, RpcSubmitEffect::Observed);
            assert_eq!(
                observation.resources[0].status,
                RpcResourceStatus::RecursiveVerified
            );
        } else {
            assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
            assert!(observation.resources.is_empty());
            assert_eq!(requests.len(), 1);
        }
    }
}

#[tokio::test]
async fn name_only_progress_still_requires_one_terminal_matching_root() {
    for (records, expected_effect, expected_roots) in [
        (ZERO_PROGRESS.to_owned(), RpcSubmitEffect::Unknown, 0),
        (
            format!("{ZERO_PROGRESS}{}{{\"Bytes\":0}}\n", root(CID)),
            RpcSubmitEffect::Observed,
            1,
        ),
        (
            format!("{ZERO_PROGRESS}{}{}", root(CID), root(OTHER)),
            RpcSubmitEffect::Observed,
            2,
        ),
        (
            format!("{ZERO_PROGRESS}{}{}", root(CID), root(CID)),
            RpcSubmitEffect::Observed,
            1,
        ),
        (
            format!("{ZERO_PROGRESS}{}", root(OTHER)),
            RpcSubmitEffect::Observed,
            1,
        ),
    ] {
        let (observation, requests) = add_observation(RpcProfile::Kubo, &records).await;
        let error = observation.result.unwrap_err();
        assert!(!error.definitely_not_submitted(), "{records}");
        assert_eq!(observation.effect, expected_effect, "{records}");
        assert_eq!(observation.resources.len(), expected_roots, "{records}");
        assert!(observation.resources.iter().all(|r| {
            r.status == RpcResourceStatus::Stored && r.ownership == Ownership::Unknown
        }));
        assert_eq!(requests.len(), 1, "must not pin invalid result: {records}");
    }
}
