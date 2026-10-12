//! Known original empty v2 authority, admitted through the native historical
//! store seam. No current configuration, SQL terminal or input proof is invented.
use ipfs_s3_gateway::{
    import::ImportSource,
    pinning::{
        config::ValidatedPinningConfig,
        decision::DecisionOrigin,
        policy::{PinPolicyEvaluator, PublicationContext},
        tags::ObjectTag,
        zip_policy::{ValidatedZipOutputRules, decode_legacy_v2_intent},
    },
    state::AppState,
    store::{
        entities::{
            object, object_version, pin_job, pin_lease, pin_lease_target, pin_provider_usage,
            remote_pin_ledger,
        },
        zip::{execution, import_intake},
    },
    zip::options::{ZipTargets, ZipV2Options},
};
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait, Statement};
use sha2::{Digest, Sha256};

pub async fn historical_empty(
    state: &AppState,
    bucket: &str,
    key: &str,
    token: &str,
    source: ImportSource,
    expected: &str,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let original = ValidatedPinningConfig::from_raw(
        &ipfs_s3_gateway::config::PinningConfig::default(),
        |_| None,
    )
    .unwrap();
    let tags = vec![ObjectTag::new("ipfs-s3:zip-root", "false")];
    let mut decision = PinPolicyEvaluator::new(&original)
        .evaluate_publication_decision(
            PublicationContext {
                bucket,
                key,
                tags: &tags,
                is_decompress_zip: true,
            },
            DecisionOrigin::new("test", &id),
        )
        .unwrap()
        .1;
    decision
        .capture_durable_revision(
            &original,
            ipfs_s3_gateway::config::OptionalPinControlMode::Strict,
        )
        .unwrap();
    assert!(decision.effective_intents.is_empty());
    let options = ZipV2Options {
        publish_source: false,
        publish_extracted: true,
        targets: ZipTargets::None,
        token: token.into(),
        root_override: Some(false),
        root_enabled: false,
        result_version: 2,
    };
    let rules = ValidatedZipOutputRules::compile(&[], &original).unwrap();
    let old = serde_json::json!({"bucket":bucket,"source_key":key,"target_prefix":"out/","options":options,
        "source_decision":decision,"providers":[],"pin_plan":{"rule_revision":rules.revision(),
            "targets":{"source":false,"extracted":false},"outputs":[]}});
    let capture = decode_legacy_v2_intent(&old.to_string()).unwrap();
    assert_eq!(capture.v2_options().as_ref(), Some(&options));
    let descriptor = match source {
        ImportSource::Cid(cid) => serde_json::to_string(&("cid", cid)),
        ImportSource::Url(url) => serde_json::to_string(&("url", url.as_str())),
    }
    .unwrap();
    let contract = serde_json::json!(["import-zip-v2","test",bucket,key,"out/",descriptor,
        expected,false,true,"none",token,false,2,null,{}, {"ipfs-s3:zip-root":"false"}])
    .to_string();
    let request = import_intake::Request {
        admission: execution::Admission {
            id: id.clone(),
            owner: "test".into(),
            source: "import".into(),
            token: token.into(),
            request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())),
            request_contract: contract,
            bucket: bucket.into(),
            source_key: key.into(),
            captured_options: serde_json::json!({"options":options,"legacy_zip_intent":old,
            "target_prefix":"out/","source_tags":tags,"rule_revision":rules.revision()})
            .to_string(),
        },
        prefix: "out/".into(),
        source_descriptor: descriptor,
        expected_sha256: Some(expected.into()),
    };
    assert_eq!(
        import_intake::admit(state.store.db(), &request)
            .await
            .unwrap()
            .id,
        id
    );
    let stored = execution::read(state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, "pending");
    assert!(stored.input_sha256.is_none());
    assert!(stored.input_art_cid.is_none());
    id
}

pub async fn execution_count(state: &AppState) -> i64 {
    state
        .store
        .db()
        .query_one(Statement::from_string(
            state.store.db().get_database_backend(),
            "SELECT COUNT(*) AS n FROM zip_v2_executions",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

pub async fn assert_no_publication_or_charge(state: &AppState) {
    let db = state.store.db();
    let counts = [
        object::Entity::find().count(db).await.unwrap(),
        object_version::Entity::find().count(db).await.unwrap(),
        pin_lease::Entity::find().count(db).await.unwrap(),
        pin_lease_target::Entity::find().count(db).await.unwrap(),
        pin_job::Entity::find().count(db).await.unwrap(),
        remote_pin_ledger::Entity::find().count(db).await.unwrap(),
    ];
    assert_eq!(
        counts, [0; 6],
        "rejection/intake must not publish or allocate remote work"
    );
    for usage in pin_provider_usage::Entity::find().all(db).await.unwrap() {
        assert_eq!(usage.reserved_bytes, 0);
        assert_eq!(usage.reserved_pins, 0);
    }
}
