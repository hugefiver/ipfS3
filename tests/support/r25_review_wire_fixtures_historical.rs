//! Original source-only URL promise, admitted through the native historical seam.
//! This creates a pending request; only the real worker may attest/bind/settle it.
use ipfs_s3_gateway::{
    config::OptionalPinControlMode,
    import::ImportSource,
    pinning::{
        config::ValidatedPinningConfig,
        decision::DecisionOrigin,
        policy::{PinPolicyEvaluator, PublicationContext},
        tags::ObjectTag,
        zip_policy::{ValidatedZipOutputRules, decode_legacy_v2_intent},
    },
    store::zip::{execution, import_intake},
    zip::options::{ZipTargets, ZipV2Options},
};
use sha2::{Digest, Sha256};

pub fn source_only_request(
    original: &ValidatedPinningConfig,
    bucket: &str,
    token: &str,
    source: ImportSource,
    expected: &str,
) -> import_intake::Request {
    let id = uuid::Uuid::new_v4().to_string();
    let tags = vec![
        ObjectTag::new("label", "source"),
        ObjectTag::new("ipfs-s3:pin", "true"),
    ];
    // `original` is the authorized first-native fixture policy, supplied BEFORE
    // admit. No stored/pending job is upgraded using a later configuration.
    let mut decision = PinPolicyEvaluator::new(original)
        .evaluate_publication_decision(
            PublicationContext {
                bucket,
                key: "archive.zip",
                tags: &tags,
                is_decompress_zip: true,
            },
            DecisionOrigin::new("test", &id),
        )
        .unwrap()
        .1;
    decision
        .capture_durable_revision(original, OptionalPinControlMode::Strict)
        .unwrap();
    assert_eq!(decision.effective_intents.len(), 1);
    assert_eq!(
        decision.effective_intents[0].source,
        ipfs_s3_gateway::pinning::policy::LeaseSource::Manual
    );
    let providers = decision.effective_intents[0]
        .providers
        .iter()
        .map(|name| {
            let provider = original
                .providers
                .iter()
                .find(|provider| &provider.name == name)
                .unwrap();
            serde_json::json!({"selected":{"config_name":name,"route":provider.route_snapshot(),
            "enabled":provider.limits.enabled && !provider.identity.retired},
            "limits":provider.limits,"requests_per_second":provider.requests_per_second})
        })
        .collect::<Vec<_>>();
    let options = ZipV2Options {
        publish_source: true,
        publish_extracted: false,
        targets: ZipTargets::None,
        token: token.into(),
        root_override: None,
        root_enabled: true,
        result_version: 2,
    };
    let rules = ValidatedZipOutputRules::compile(&[], original).unwrap();
    let old = serde_json::json!({"bucket":bucket,"source_key":"archive.zip","target_prefix":"out/","options":options,
        "source_decision":decision,"providers":providers,"pin_plan":{"rule_revision":rules.revision(),
            "targets":{"source":false,"extracted":false},"outputs":[]}});
    let capture = decode_legacy_v2_intent(&old.to_string()).unwrap();
    assert_eq!(capture.v2_options(), Some(options.clone()));
    let descriptor = match source {
        ImportSource::Cid(cid) => serde_json::to_string(&("cid", cid)),
        ImportSource::Url(url) => serde_json::to_string(&("url", url.as_str())),
    }
    .unwrap();
    let contract =
        serde_json::json!(["import-zip-v2","test",bucket,"archive.zip","out/",descriptor,
        expected,true,false,"none",token,null,2,"application/zip",{"label":"signed-source"},
        {"label":"source","ipfs-s3:pin":"true"}])
        .to_string();
    import_intake::Request {
        admission: execution::Admission {
            id, owner:"test".into(), source:"import".into(), token:token.into(),
            request_fingerprint:hex::encode(Sha256::digest(contract.as_bytes())), request_contract:contract,
            bucket:bucket.into(), source_key:"archive.zip".into(),
            captured_options:serde_json::json!({"options":options,"legacy_zip_intent":old,
                "target_prefix":"out/","source_tags":tags,"rule_revision":rules.revision(),
                "source_content_type":"application/zip","source_metadata":{"label":"signed-source"}}).to_string(),
        },
        prefix:"out/".into(), source_descriptor:descriptor, expected_sha256:Some(expected.into()),
    }
}
