use chrono::Utc;
use ipfs_s3_gateway::{
    config::{Config, OptionalPinControlMode},
    pinning::{
        config::{
            LeaseDuration, PolicyTrigger, ProviderMode, ValidatedPinningConfig, ValidatedPolicy,
            ValidatedProvider,
        },
        decision::{DecisionOrigin, ExtensionDecision},
        policy::{PinPolicyEvaluator, PublicationContext, PublicationPolicy},
        provider::canonical_resource_cid,
    },
    store::{
        self,
        entities::{object, object_version},
        object_version::BucketVersioningState,
        pinning::publication::{
            self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
        },
        residency::resolve_version_residency,
    },
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
#[path = "support/pin_rows.rs"]
mod pin_rows;

const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

fn provider(scope: &str, rpc: bool, priority: u32) -> ValidatedProvider {
    let backend = if rpc { "filebase" } else { "noop" };
    let profile = if rpc { "filebase-rpc" } else { "noop" };
    let strategy = if rpc { "upload" } else { "cid" };
    let provider_options = if rpc {
        "token_env = 'PUBLICATION_TOKEN'\napi = 'rpc'\nstrategy = 'upload'\nendpoint = 'https://rpc.example.test'"
    } else {
        ""
    };
    let identity_secret = if rpc {
        "secret_ref = 'env:PUBLICATION_TOKEN'"
    } else {
        ""
    };
    let rpc_options = if rpc {
        format!(
            "[[pinning_rpc.providers]]\nconfig_name = '{scope}'\nprofile = 'filebase'\nauth = 'bearer'"
        )
    } else {
        String::new()
    };
    let raw: Config = toml::from_str(&format!(
        r#"
        [[pinning.providers]]
        name = "{scope}"
        kind = "{backend}"
        priority = {priority}
        max_bytes = 7
        max_pins = 1
        {provider_options}
        [pinning_identity]
        primary_storage_domain = "primary"
        [[pinning_identity.providers]]
        config_name = "{scope}"
        provider_id = "publication-{scope}"
        display_name = "{scope}"
        backend = "{backend}"
        scope = "{scope}"
        storage_domain = "remote"
        credential_revision = 1
        endpoint_revision = 1
        api_profile = "{profile}"
        strategy = "{strategy}"
        cleanup = "managed"
        {identity_secret}
        {rpc_options}
        "#,
    ))
    .unwrap();
    let mut provider = ValidatedPinningConfig::from_config(&raw, |name| {
        (name == "PUBLICATION_TOKEN").then(|| "publication-fixture-token".into())
    })
    .unwrap()
    .providers
    .remove(0);
    provider.name = provider.identity.allocation_key();
    provider
}

fn config(providers: Vec<ValidatedProvider>, mode: ProviderMode) -> ValidatedPinningConfig {
    ValidatedPinningConfig {
        worker_interval: LeaseDuration::parse("1s").unwrap(),
        worker_concurrency: 1,
        provider_limits: providers
            .iter()
            .map(|p| (p.name.clone(), p.limits.clone()))
            .collect(),
        policies: vec![ValidatedPolicy {
            identity: "policy:rpc-publication".into(),
            bucket: "bucket".into(),
            prefix: String::new(),
            trigger: PolicyTrigger::Always,
            provider_mode: mode,
            providers: providers.iter().map(|p| p.name.clone()).collect(),
            default_duration: LeaseDuration::parse("1h").unwrap(),
            max_duration: LeaseDuration::parse("2h").unwrap(),
            allow_decompressed: true,
        }],
        providers,
    }
}

async fn database() -> DatabaseConnection {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    store::bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
        .await
        .unwrap();
    db
}

fn published(id: &str, key: &str, cid: &str) -> PublicationObject {
    PublicationObject::from_put(
        id.into(),
        "bucket",
        key,
        cid.into(),
        7,
        None,
        None,
        false,
        None,
        None,
        Utc::now(),
    )
}

fn request(object: PublicationObject, policy: PublicationPolicy) -> PublicationRequest {
    PublicationRequest {
        object_target: PinTargetSpec {
            cid: object.cid.clone(),
            logical_size: object.logical_size,
        },
        object,
        tags: policy.tags.clone(),
        policy,
    }
}

fn decided_request(
    config: &ValidatedPinningConfig,
    id: &str,
    key: &str,
    cid: &str,
) -> (PublicationRequest, ExtensionDecision) {
    let (policy, decision) = PinPolicyEvaluator::new(config)
        .evaluate_publication_decision(
            PublicationContext {
                bucket: "bucket",
                key,
                tags: &[],
                is_decompress_zip: false,
            },
            DecisionOrigin::new("principal", id),
        )
        .unwrap();
    (request(published(id, key, cid), policy), decision)
}

fn captured<'a>(
    config: &'a ValidatedPinningConfig,
    decision: &'a ExtensionDecision,
) -> DecidedPublish<'a> {
    DecidedPublish {
        decision,
        config,
        mode: OptionalPinControlMode::Strict,
        limits: &config.provider_limits,
    }
}

async fn assert_original_version(db: &DatabaseConnection, id: &str, cid: &str) {
    let object = object::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(object.cid, cid);
    assert_eq!(object.etag, cid);
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(id))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert!(version.version_id.is_some());
    let residency = resolve_version_residency(db, &version.id).await.unwrap();
    assert_eq!(residency.identity.cid, cid);
    assert_eq!(residency.identity.object_id, id);
}

#[tokio::test]
async fn equivalent_cids_keep_public_versions_and_share_resources_per_service() {
    let canonical = canonical_resource_cid(V0).unwrap();
    for mode in [ProviderMode::One, ProviderMode::All] {
        let config = config(
            vec![provider("rpc", true, 1), provider("noop", false, 2)],
            mode,
        );
        let db = database().await;
        for (id, cid) in [("v0", V0), ("v1", canonical.as_str())] {
            let (request, decision) = decided_request(&config, id, "same-key", cid);
            publication::publish_decided_object(&db, request, captured(&config, &decision))
                .await
                .unwrap();
            assert_original_version(&db, id, cid).await;
        }
        let resources = pin_rows::resources(&db).await;
        let refs = pin_rows::refs(&db).await;
        let selected = if mode == ProviderMode::All { 2 } else { 1 };
        assert_eq!(resources.len(), selected);
        assert_eq!(refs.len(), selected * 2);
        for resource in resources {
            assert_eq!((&resource.cid, resource.logical_size), (&canonical, 7));
            assert_eq!(
                refs.iter()
                    .filter(|r| r.provider == resource.provider && r.cid == canonical)
                    .map(|r| r.owner_object_id.as_str())
                    .collect::<std::collections::BTreeSet<_>>(),
                ["v0", "v1"].into()
            );
        }
    }
}

#[tokio::test]
async fn one_provider_prefers_an_available_service_for_new_content() {
    let mut rpc = provider("rpc", true, 1);
    rpc.limits.max_bytes = 6;
    let config = config(vec![rpc, provider("noop", false, 2)], ProviderMode::One);
    let db = database().await;
    let (request, decision) = decided_request(&config, "object", "key", V0);
    publication::publish_decided_object(&db, request, captured(&config, &decision))
        .await
        .unwrap();
    let rows = pin_rows::resources(&db).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].provider, config.providers[1].name);
    assert_original_version(&db, "object", V0).await;
}
