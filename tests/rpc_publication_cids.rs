//! CID allocation regressions through real publication transactions. No worker,
//! provider I/O, or invented creation/cleanup evidence is used by these fixtures.
use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use ipfs_s3_gateway::{
    config::{Config, OptionalPinControlMode},
    error::AppError,
    pinning::{
        config::{
            LeaseDuration, PolicyTrigger, ProviderMode, ValidatedPinningConfig, ValidatedPolicy,
            ValidatedProvider,
        },
        decision::{DecisionOrigin, ExtensionDecision},
        policy::{PinPolicyEvaluator, PublicationContext, PublicationPolicy},
        provider::canonical_resource_cid,
        tags::ObjectTag,
        zip_policy::{
            ValidatedZipOutputRules, ZipOutputRuleConfig, ZipPublishedOutput, ZipRuleEffect,
            ZipTargets as PolicyTargets,
        },
    },
    store::{
        self,
        entities::{
            object, object_version, pin_extension_decision, pin_lease, pin_lease_target,
            pin_provider_usage, remote_pin, remote_pin_ledger,
        },
        import::ownership,
        object_version::BucketVersioningState,
        pinning::{
            ledger,
            publication::{
                self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
                ZipPublicationRequest, ZipV2Publication, ZipV2Success, v2_execution as execution,
            },
            quota,
        },
        residency::resolve_version_residency,
        zip,
    },
    zip::options::ZipTargets,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, Set,
    TransactionTrait,
};
use sha2::{Digest, Sha256};

const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
const OTHER: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

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

async fn database(config: &ValidatedPinningConfig) -> DatabaseConnection {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    store::bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
        .await
        .unwrap();
    for provider in &config.providers {
        ledger::register_route(&db, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
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

async fn assert_remote(db: &DatabaseConnection, provider: &str, cid: &str, refs: usize) {
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(provider))
        .all(db)
        .await
        .unwrap();
    assert_eq!(targets.len(), refs);
    assert!(targets.iter().all(|t| t.cid == cid && t.state == "waiting"));
    assert_eq!(
        targets
            .iter()
            .map(|t| &t.lease_id)
            .collect::<BTreeSet<_>>()
            .len(),
        refs
    );
    let remotes = remote_pin::Entity::find()
        .filter(remote_pin::Column::Provider.eq(provider))
        .all(db)
        .await
        .unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0].cid, cid);
    assert_eq!(remotes[0].epoch, refs as i64);
    let usage = quota::read_usage(db, provider).await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    let evidence = ledger::get(db, provider, cid).await.unwrap().unwrap();
    assert_eq!(evidence.ownership, "unknown");
    assert_eq!(evidence.effect, "reserved");
    assert!(!ledger::cleanup_allowed(db, provider, cid).await.unwrap());
}

#[tokio::test]
async fn rpc_equivalent_versions_keep_original_cids_and_share_one_reservation_with_two_refs() {
    let canonical = canonical_resource_cid(V0).unwrap();
    for mode in [ProviderMode::One, ProviderMode::All] {
        let config = config(vec![provider("rpc", true, 1)], mode);
        let db = database(&config).await;
        for (id, cid) in [("v0", V0), ("v1", canonical.as_str())] {
            let (request, decision) = decided_request(&config, id, "same-key", cid);
            publication::publish_decided_object(&db, request, captured(&config, &decision))
                .await
                .unwrap();
            assert_original_version(&db, id, cid).await;
        }
        assert_remote(&db, &config.providers[0].name, &canonical, 2).await;
        assert_eq!(
            pin_extension_decision::Entity::find()
                .count(&db)
                .await
                .unwrap(),
            2
        );
    }
}

#[tokio::test]
async fn all_provider_targets_are_scoped_and_noop_keeps_raw_aliases_without_cleanup_authority() {
    let canonical = canonical_resource_cid(V0).unwrap();
    let mut noop = provider("noop", false, 2);
    noop.limits.max_bytes = 14;
    noop.limits.max_pins = 2;
    let config = config(vec![provider("rpc", true, 1), noop], ProviderMode::All);
    let db = database(&config).await;
    for (id, cid) in [("v0", V0), ("v1", canonical.as_str())] {
        let (request, decision) = decided_request(&config, id, id, cid);
        publication::publish_decided_object(&db, request, captured(&config, &decision))
            .await
            .unwrap();
    }
    assert_remote(&db, &config.providers[0].name, &canonical, 2).await;
    let noop = &config.providers[1].name;
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(noop))
        .all(&db)
        .await
        .unwrap();
    assert_eq!(
        targets
            .iter()
            .map(|t| t.cid.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([V0, canonical.as_str()])
    );
    let usage = quota::read_usage(&db, noop).await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (14, 2));
    for cid in [V0, canonical.as_str()] {
        assert!(!ledger::cleanup_allowed(&db, noop, cid).await.unwrap());
    }
}

#[tokio::test]
async fn one_provider_fallback_and_first_unavailable_use_the_selected_provider_target() {
    let canonical = canonical_resource_cid(V0).unwrap();
    for all_blocked in [false, true] {
        let mut rpc = provider("rpc", true, 1);
        rpc.limits.max_bytes = 6;
        let mut noop = provider("noop", false, 2);
        if all_blocked {
            noop.limits.max_bytes = 6;
        }
        let config = config(vec![rpc, noop], ProviderMode::One);
        let db = database(&config).await;
        let (request, decision) = decided_request(&config, "object", "key", V0);
        publication::publish_decided_object(&db, request, captured(&config, &decision))
            .await
            .unwrap();
        let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
        assert_eq!(targets.len(), 1);
        let target = &targets[0];
        if all_blocked {
            assert_eq!(target.provider, config.providers[0].name);
            assert_eq!(target.cid, canonical);
            assert_eq!(target.state, "quota_blocked");
            assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 0);
        } else {
            assert_eq!(target.provider, config.providers[1].name);
            assert_eq!(target.cid, V0);
            assert_remote(&db, &target.provider, V0, 1).await;
        }
        assert_original_version(&db, "object", V0).await;
    }
}

#[tokio::test]
async fn legacy_zip_equivalent_entries_dedup_rpc_targets_but_not_non_rpc_targets() {
    let canonical = canonical_resource_cid(V0).unwrap();
    for mode in [ProviderMode::One, ProviderMode::All] {
        let mut noop = provider("noop", false, 2);
        noop.limits.max_bytes = 14;
        noop.limits.max_pins = 2;
        let mut config = config(vec![provider("rpc", true, 1), noop], mode);
        config.policies[0].trigger = PolicyTrigger::Request;
        let db = database(&config).await;
        let tags = vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("ipfs-s3:content", "decompressed"),
        ];
        let policy = PinPolicyEvaluator::new(&config)
            .evaluate_publication(PublicationContext {
                bucket: "bucket",
                key: "archive.zip",
                tags: &tags,
                is_decompress_zip: true,
            })
            .unwrap();
        publication::publish_zip(
            &db,
            ZipPublicationRequest {
                archive: request(published("archive", "archive.zip", OTHER), policy),
                entries: vec![
                    published("v0", "out/v0", V0),
                    published("v1", "out/v1", &canonical),
                ],
            },
            &config.provider_limits,
        )
        .await
        .unwrap();
        assert_remote(&db, &config.providers[0].name, &canonical, 1).await;
        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Provider.eq(&config.providers[1].name))
            .all(&db)
            .await
            .unwrap();
        if mode == ProviderMode::All {
            assert_eq!(
                targets
                    .iter()
                    .map(|t| t.cid.as_str())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([V0, canonical.as_str()])
            );
        } else {
            assert!(targets.is_empty());
        }
        assert_original_version(&db, "archive", OTHER).await;
        assert_original_version(&db, "v0", V0).await;
        assert_original_version(&db, "v1", &canonical).await;
    }
}

#[tokio::test]
async fn unknown_historical_alias_rejects_new_publication_and_keeps_old_debt_unchanged() {
    let canonical = canonical_resource_cid(V0).unwrap();
    for remote_only in [true, false] {
        let config = config(vec![provider("rpc", true, 1)], ProviderMode::One);
        let db = database(&config).await;
        let provider = &config.providers[0].name;
        if remote_only {
            remote_pin::Entity::insert(remote_pin::ActiveModel {
                provider: Set(provider.clone()),
                cid: Set(V0.into()),
                request_id: Set(None),
                cid_size: Set(7),
                status: Set("reserved".into()),
                epoch: Set(4),
                failure_attempts: Set(0),
                next_retry_at: Set(None),
                last_failed_request_id: Set(None),
                last_touched_at: Set(Utc::now()),
                last_error_class: Set(None),
                last_error_text: Set(None),
            })
            .exec(&db)
            .await
            .unwrap();
        } else {
            remote_pin_ledger::Entity::insert(remote_pin_ledger::ActiveModel {
                provider: Set(provider.clone()),
                cid: Set(V0.into()),
                route: Set(None),
                ownership: Set("unknown".into()),
                effect: Set("unknown".into()),
                ..Default::default()
            })
            .exec(&db)
            .await
            .unwrap();
        }
        pin_provider_usage::Entity::insert(pin_provider_usage::ActiveModel {
            provider: Set(provider.clone()),
            reserved_bytes: Set(7),
            reserved_pins: Set(1),
            observed_bytes: Set(None),
            observed_pins: Set(None),
            observed_at: Set(None),
        })
        .exec(&db)
        .await
        .unwrap();
        let old_remote = remote_pin::Entity::find().all(&db).await.unwrap();
        let old_ledger = remote_pin_ledger::Entity::find().all(&db).await.unwrap();
        let old_usage = quota::read_usage(&db, provider).await.unwrap();
        let (request, decision) = decided_request(&config, "new", "new-key", &canonical);
        let error = publication::publish_decided_object(&db, request, captured(&config, &decision))
            .await
            .unwrap_err();
        assert!(
            matches!(error, AppError::InvalidPinningRequest(ref message) if message.contains("historical RPC"))
        );
        assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(object_version::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            pin_extension_decision::Entity::find()
                .count(&db)
                .await
                .unwrap(),
            0
        );
        assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
        assert_eq!(
            pin_lease_target::Entity::find().count(&db).await.unwrap(),
            0
        );
        assert_eq!(
            remote_pin::Entity::find().all(&db).await.unwrap(),
            old_remote
        );
        assert_eq!(
            remote_pin_ledger::Entity::find().all(&db).await.unwrap(),
            old_ledger
        );
        assert_eq!(quota::read_usage(&db, provider).await.unwrap(), old_usage);
    }
}

#[tokio::test]
async fn canonical_planning_does_not_bypass_the_captured_route_fence() {
    let config = config(vec![provider("rpc", true, 1)], ProviderMode::One);
    let db = database(&config).await;
    let (request, decision) = decided_request(&config, "object", "key", V0);
    let mut changed = config.providers[0].identity.clone();
    changed.endpoint_revision += 1;
    ledger::register_route(&db, &config.providers[0].name, &changed)
        .await
        .unwrap();
    let error = publication::publish_decided_object(&db, request, captured(&config, &decision))
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::InvalidPinningRequest(ref message) if message.contains("route no longer matches"))
    );
    assert_eq!(object::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(
        pin_extension_decision::Entity::find()
            .count(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(pin_lease::Entity::find().count(&db).await.unwrap(), 0);
    assert_eq!(remote_pin::Entity::find().count(&db).await.unwrap(), 0);
    assert!(
        quota::read_usage(&db, &config.providers[0].name)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn zip_v2_source_and_outputs_share_the_canonical_rpc_frontier_and_keep_original_versions() {
    let canonical = canonical_resource_cid(V0).unwrap();
    let config = config(vec![provider("rpc", true, 1)], ProviderMode::One);
    let db = database(&config).await;
    let rules = ValidatedZipOutputRules::compile(
        &[ZipOutputRuleConfig {
            name: "all".into(),
            priority: 1,
            bucket: "bucket".into(),
            prefix: String::new(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(config.policies[0].identity.clone()),
        }],
        &config,
    )
    .unwrap();
    let admission = execution::Admission {
        id: "v2".into(),
        owner: "principal".into(),
        source: "direct".into(),
        token: "token".into(),
        request_fingerprint: hex::encode(Sha256::digest(b"contract")),
        request_contract: "contract".into(),
        bucket: "bucket".into(),
        source_key: "archive.zip".into(),
        captured_options: serde_json::json!({"options": {
            "publish_source": true, "publish_extracted": true, "targets": "both", "token": "token",
            "root_override": null, "root_enabled": false, "result_version": 2,
        }, "rule_revision": rules.revision()})
        .to_string(),
    };
    execution::admit(&db, &admission).await.unwrap();
    let claim = execution::claim(&db, &admission.id, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(&db, &claim, &"a".repeat(64), V0, 7)
        .await
        .unwrap();
    zip::admit(
        &db,
        &zip::BatchAdmission {
            id: admission.id.clone(),
            owner: admission.owner.clone(),
            source: admission.source.clone(),
            token: admission.token.clone(),
            fingerprint: "pending".into(),
            bucket: "bucket".into(),
            archive_key: "archive.zip".into(),
            input_identity: "pending".into(),
            captured_options: admission.captured_options.clone(),
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        &db,
        &admission.id,
        &[zip::ManifestItem::Success {
            path: "file".into(),
            object_key: "out/file".into(),
            cid: canonical.clone(),
            size: 7,
        }],
    )
    .await
    .unwrap();
    let tx = db.begin().await.unwrap();
    ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let source_guard = ownership::admit_zip_v2_source_in_transaction(
        &tx,
        "bucket",
        "archive.zip",
        &BTreeSet::from(["out/file".into()]),
        "zip-v2-source:v2",
        Utc::now(),
    )
    .await
    .unwrap();
    execution::admit_manifest_in_transaction(
        &tx,
        &claim,
        &[execution::ManifestItem::Success {
            path: "file".into(),
            object_key: "out/file".into(),
            cid: canonical.clone(),
            size: 7,
        }],
        &BTreeMap::from([("out/file".into(), "output-guard".into())]),
    )
    .await
    .unwrap();
    ownership::capture_zip_v2_source_guard_in_transaction(&tx, &claim, &source_guard)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let source = ZipPublishedOutput {
        bucket: "bucket".into(),
        key: "archive.zip".into(),
        version_id: "prepublication-source".into(),
        cid: V0.into(),
    };
    let output = ZipPublishedOutput {
        bucket: "bucket".into(),
        key: "out/file".into(),
        version_id: "prepublication-output".into(),
        cid: canonical.clone(),
    };
    let plan = rules
        .plan(
            PolicyTargets {
                source: true,
                extracted: true,
            },
            Some(source),
            &[output],
        )
        .unwrap();
    let policy = |key: &str| PublicationPolicy {
        tags: vec![],
        leases: plan
            .outputs
            .iter()
            .find(|o| o.output.key == key)
            .unwrap()
            .intents
            .iter()
            .map(|i| i.intent.clone())
            .collect(),
    };
    let result = publication::publish_zip_v2(
        &db,
        ZipV2Publication {
            claim,
            source: Some(published("source", "archive.zip", V0)),
            source_policy: Some(policy("archive.zip")),
            source_guard: Some(source_guard),
            successes: vec![ZipV2Success {
                object: published("output", "out/file", &canonical),
                path: "file".into(),
                object_key: "out/file".into(),
                cid: canonical.clone(),
                size: 7,
                policy: policy("out/file"),
            }],
            targets: ZipTargets::Both,
            captured_rule_revision: Some(rules.revision().into()),
            root_outcome: zip::RootOutcome::Disabled,
            terminal_result: "{\"result_version\":2}".into(),
        },
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    assert!(
        matches!(result, publication::ZipV2PublicationResult::Published(rows) if rows.len() == 2)
    );
    assert_remote(&db, &config.providers[0].name, &canonical, 2).await;
    assert_original_version(&db, "source", V0).await;
    assert_original_version(&db, "output", &canonical).await;
    assert_eq!(
        execution::read(&db, "v2").await.unwrap().unwrap().state,
        "completed"
    );
}
