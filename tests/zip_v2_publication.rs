#[path = "support/pin_rows.rs"]
mod pin_rows;
mod stage9_zip_support;

use chrono::Utc;
use stage9_zip_support::{ARCHIVE_CID, OUTPUT_CID, fixture_cid};
const SECOND_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
use ipfs_s3_gateway::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    import::SupersedeReason,
    pinning::{
        config::ValidatedPinningConfig,
        policy::PublicationPolicy,
        zip_policy::{ValidatedZipOutputRules, ZipOutputRuleConfig, ZipRuleEffect},
    },
    store::{
        self,
        entities::{object, object_version},
        pinning::publication::{
            self, PublicationObject, ZipV2Publication, ZipV2Success, v2_execution as execution,
        },
        zip,
    },
    zip::options::ZipTargets,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, TransactionTrait};
use sha2::{Digest, Sha256};

#[path = "support/r25_review_publication.rs"]
mod r25_review_publication;

#[path = "support/r25_review_publication_regressions.rs"]
mod r25_review_publication_regressions;

async fn setup() -> (tempfile::TempDir, DatabaseConnection, DatabaseConnection) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path()
            .join("publication.db")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let first = store::connect_database(&url).await.unwrap();
    store::run_migrations(&first).await.unwrap();
    store::bucket::create(&first, "bucket", None).await.unwrap();
    let second = store::connect_database(&url).await.unwrap();
    (dir, first, second)
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

async fn prepared(db: &DatabaseConnection, successes: &[(&str, &str, &str)]) -> execution::Claim {
    prepared_with_root(db, successes, false).await
}

async fn prepared_with_root(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    root_enabled: bool,
) -> execution::Claim {
    prepared_with_controls(db, successes, root_enabled, ZipTargets::None).await
}

async fn prepared_with_controls(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    root_enabled: bool,
    targets: ZipTargets,
) -> execution::Claim {
    prepared_manifest_with_controls(db, successes, &[], root_enabled, targets).await
}

async fn prepared_manifest_with_controls(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    failures: &[(&str, &str)],
    root_enabled: bool,
    targets: ZipTargets,
) -> execution::Claim {
    let capture = r25_review_publication::capture(root_enabled, targets);
    let captured_options = serde_json::json!({
        "options": capture.v2_options().unwrap(), "target_prefix": capture.target_prefix(),
        "zip_intent": capture,
    })
    .to_string();
    r25_review_publication::prepare_with_envelope(db, successes, failures, captured_options).await
}

fn request(claim: execution::Claim, successes: Vec<ZipV2Success>) -> ZipV2Publication {
    ZipV2Publication {
        claim,
        source: None,
        source_policy: None,
        source_guard: None,
        successes,
        targets: ZipTargets::None,
        root_outcome: zip::RootOutcome::Disabled,
        terminal_result: "{\"result_version\":2}".into(),
    }
}

fn success(path: &str, key: &str, cid: &str, id: &str) -> ZipV2Success {
    ZipV2Success {
        object: published(id, key, cid),
        path: path.into(),
        object_key: key.into(),
        cid: cid.into(),
        size: 7,
        policy: PublicationPolicy {
            tags: vec![],
            leases: vec![],
        },
    }
}

fn pin_config() -> ValidatedPinningConfig {
    let providers = ["a", "b"]
        .into_iter()
        .map(|name| ProviderConfig {
            cluster: None,
            name: name.into(),
            kind: "noop".into(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority: 1,
            max_bytes: 100,
            max_pins: 10,
            requests_per_second: None,
        })
        .collect();
    let policies = ["a", "b"]
        .into_iter()
        .map(|provider| PolicyConfig {
            bucket: "bucket".into(),
            prefix: "out/".into(),
            trigger: "always".into(),
            provider_mode: "one".into(),
            providers: vec![provider.into()],
            default_duration: "1h".into(),
            max_duration: "2h".into(),
            allow_decompressed: false,
        })
        .collect();
    ValidatedPinningConfig::from_raw(
        &PinningConfig {
            providers,
            policies,
            ..Default::default()
        },
        |_| None,
    )
    .unwrap()
}

fn output_rule_config(config: &ValidatedPinningConfig) -> Vec<ZipOutputRuleConfig> {
    vec![
        ZipOutputRuleConfig {
            name: "a".into(),
            priority: 1,
            bucket: "bucket".into(),
            prefix: "out/a/".into(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(config.policies[0].identity.clone()),
        },
        ZipOutputRuleConfig {
            name: "b".into(),
            priority: 1,
            bucket: "bucket".into(),
            prefix: "out/b/".into(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(config.policies[1].identity.clone()),
        },
        ZipOutputRuleConfig {
            name: "private".into(),
            priority: 0,
            bucket: "bucket".into(),
            prefix: "out/private/".into(),
            effect: ZipRuleEffect::Deny,
            policy_id: None,
        },
    ]
}

fn output_rules(config: &ValidatedPinningConfig) -> ValidatedZipOutputRules {
    ValidatedZipOutputRules::compile(&output_rule_config(config), config).unwrap()
}

#[tokio::test]
async fn each_output_gets_independent_automatic_policy_private_gets_no_lease_and_shared_cid_dedups_remotes()
 {
    let (_dir, first, second) = setup().await;
    let config = pin_config();
    let rules = output_rules(&config);
    let claim = prepared_with_controls(
        &first,
        &[
            ("a-one", "out/a/one", OUTPUT_CID),
            ("a-two", "out/a/two", OUTPUT_CID),
            ("b-one", "out/b/one", OUTPUT_CID),
            ("private", "out/private/secret", OUTPUT_CID),
        ],
        false,
        ZipTargets::Extracted,
    )
    .await;
    let mut publication = request(
        claim,
        vec![
            success("a-one", "out/a/one", OUTPUT_CID, "one"),
            success("a-two", "out/a/two", OUTPUT_CID, "two"),
            success("b-one", "out/b/one", OUTPUT_CID, "three"),
            success("private", "out/private/secret", OUTPUT_CID, "four"),
        ],
    );
    publication.targets = ZipTargets::Extracted;
    publication::publish_zip_v2(
        &second,
        publication,
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 4);
    let refs = pin_rows::refs(&first).await;
    assert_eq!(
        refs.iter()
            .map(|r| (r.owner_object_id.as_str(), r.provider.as_str()))
            .collect::<Vec<_>>(),
        [("one", "a"), ("three", "b"), ("two", "a")]
    );
    assert!(
        refs.iter()
            .all(|r| r.cid == OUTPUT_CID && r.source == "automatic")
    );
    let resources = pin_rows::resources(&first).await;
    assert_eq!(resources.len(), 2);
    for (resource, provider) in resources.iter().zip(["a", "b"]) {
        assert_eq!(
            (
                resource.provider.as_str(),
                resource.cid.as_str(),
                resource.logical_size
            ),
            (provider, OUTPUT_CID, 7)
        );
        assert_eq!(
            (resource.status.as_str(), resource.action.as_deref()),
            ("waiting", Some("submit"))
        );
    }
    for version in object_version::Entity::find().all(&first).await.unwrap() {
        let decision: ipfs_s3_gateway::pinning::decision::ExtensionDecision =
            serde_json::from_str(version.pin_decision.as_deref().unwrap()).unwrap();
        assert_eq!(
            decision.effective_intents.len(),
            usize::from(version.object_id.as_deref() != Some("four"))
        );
    }
}

#[tokio::test]
async fn source_absent_publishes_only_final_successes_from_two_connections() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one.txt", "out/one.txt", OUTPUT_CID)]).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    publication::publish_zip_v2(
        &second,
        request(
            claim,
            vec![success("one.txt", "out/one.txt", OUTPUT_CID, "output-1")],
        ),
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    let objects = object::Entity::find().all(&first).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, "out/one.txt");
    assert_eq!(
        object_version::Entity::find().count(&second).await.unwrap(),
        1
    );
    assert_eq!(pin_rows::count(&second, "pin_refs").await, 0);
    assert_eq!(
        execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
    assert!(
        !zip::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .source_published
    );
}

#[tokio::test]
async fn mismatched_final_cid_fences_the_claim_without_publishing() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one.txt", "out/one.txt", OUTPUT_CID)]).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(matches!(
        publication::publish_zip_v2(
            &second,
            request(
                claim,
                vec![success("one.txt", "out/one.txt", SECOND_CID, "output-1")]
            ),
            &rules,
            &config,
            &config.provider_limits
        )
        .await,
        Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
    ));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
    assert_eq!(
        execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
    assert_eq!(
        zip::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "open"
    );
}

#[tokio::test]
async fn a_mirror_with_actual_digest_instead_of_pending_is_not_a_v2_mirror() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one", "out/one", OUTPUT_CID)]).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    for (column, value, original) in [
        (
            "fingerprint",
            hex::encode(Sha256::digest(b"{\"contract\":\"v2\"}")),
            "pending",
        ),
        ("input_identity", "a".repeat(64), "pending"),
        ("owner", "another-principal".into(), "principal"),
    ] {
        let sql = format!("UPDATE zip_batches SET {column}=? WHERE id=?");
        let tx = first.begin().await.unwrap();
        tx.execute(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            sql,
            [value.into(), claim.batch_id.clone().into()],
        ))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            publication::publish_zip_v2(
                &second,
                request(
                    claim.clone(),
                    vec![success("one", "out/one", OUTPUT_CID, "output-1")]
                ),
                &rules,
                &config,
                &config.provider_limits,
            )
            .await
            .is_err()
        );
        assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
        first
            .execute(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                format!("UPDATE zip_batches SET {column}=? WHERE id=?"),
                [original.into(), claim.batch_id.clone().into()],
            ))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn bucket_lock_precedes_transaction_snapshot_and_rechecks_immutable_execution_owner() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one", "out/one", OUTPUT_CID)]).await;
    first
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mode: String = first
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "PRAGMA journal_mode",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "journal_mode")
        .unwrap();
    assert_eq!(mode, "wal");
    let tx = first.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();

    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    let published = tokio::spawn(async move {
        publication::publish_zip_v2(
            &second,
            request(
                claim,
                vec![success("one", "out/one", OUTPUT_CID, "output-1")],
            ),
            &rules,
            &config,
            &config.provider_limits,
        )
        .await
    });
    // The second connection may read an old hint but cannot admit it while
    // this bucket writer owns serialization; both immutable owner rows move.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !published.is_finished(),
        "publication did not wait for the bucket lock"
    );
    for table in ["zip_v2_executions", "zip_batches"] {
        tx.execute(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            format!("UPDATE {table} SET owner=? WHERE id=?"),
            ["changed-principal".into(), "v2-execution".into()],
        ))
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), published)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        result,
        Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
    ));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(pin_rows::count(&first, "pin_refs").await, 0);
}

#[tokio::test]
async fn all_failed_manifest_seals_disabled_root_and_execution_without_objects() {
    let (_dir, first, second) = setup().await;
    let claim = prepared_manifest_with_controls(
        &first,
        &[],
        &[("failed/file", "invalid_zip")],
        false,
        ZipTargets::None,
    )
    .await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(matches!(
        publication::publish_zip_v2(
            &second,
            request(claim, vec![]),
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .unwrap(),
        publication::ZipV2PublicationResult::Published(results) if results.is_empty()
    ));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
    let execution = execution::read(&second, "v2-execution")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.state, "completed");
    assert_eq!(
        execution.terminal_result.as_deref(),
        Some("{\"result_version\":2}")
    );
    let batch = zip::snapshot(&first, "v2-execution")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.batch.state, "published");
    assert_eq!(batch.batch.root_status, "disabled");
    assert_eq!(batch.entries.len(), 1);
    assert!(batch.entries[0].version_row_id.is_none());
    assert_eq!(pin_rows::count(&first, "pin_refs").await, 0);
    let admitted = execution::Admission {
        id: execution.id,
        owner: execution.owner,
        source: execution.source,
        token: execution.token,
        request_fingerprint: execution.request_fingerprint,
        request_contract: execution.request_contract,
        bucket: execution.bucket,
        source_key: execution.source_key,
        captured_options: execution.captured_options,
    };
    let replay = execution::read_for_replay(&second, &admitted, &"a".repeat(64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.state, "completed");
    assert_eq!(
        replay.terminal_result.as_deref(),
        Some("{\"result_version\":2}")
    );
}

#[tokio::test]
async fn all_failed_manifest_with_root_enabled_seals_empty_without_a_cid() {
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    for reject_first in [true, false] {
        let (_dir, first, second) = setup().await;
        let claim = prepared_manifest_with_controls(
            &first,
            &[],
            &[("failed/file", "invalid_zip")],
            true,
            ZipTargets::None,
        )
        .await;
        if reject_first {
            assert!(matches!(
                publication::publish_zip_v2(
                    &second,
                    request(claim.clone(), vec![]),
                    &rules,
                    &config,
                    &config.provider_limits,
                )
                .await,
                Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
            ));
        }
        let mut valid = request(claim, vec![]);
        valid.root_outcome = zip::RootOutcome::Empty;
        let result =
            publication::publish_zip_v2(&second, valid, &rules, &config, &config.provider_limits)
                .await;
        assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
        let batch = zip::snapshot(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.entries.len(), 1);
        assert!(batch.batch.root_cid.is_none());
        let execution = execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap();
        if reject_first {
            assert!(result.is_err());
            assert_eq!(execution.state, "fenced");
            assert_eq!(batch.batch.state, "open");
        } else {
            assert!(
                matches!(result.unwrap(), publication::ZipV2PublicationResult::Published(results) if results.is_empty())
            );
            assert_eq!(execution.state, "completed");
            assert_eq!(batch.batch.state, "published");
            assert_eq!(batch.batch.root_status, "empty");
        }
    }
}

#[tokio::test]
async fn every_durable_target_must_be_written_or_the_entire_publication_rolls_back() {
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    for reject_first in [true, false] {
        let (_dir, first, second) = setup().await;
        let claim = prepared(
            &first,
            &[
                ("one", "out/one", OUTPUT_CID),
                ("two", "out/two", SECOND_CID),
            ],
        )
        .await;
        let outputs = vec![
            success("one", "out/one", OUTPUT_CID, "output-1"),
            success("two", "out/two", SECOND_CID, "output-2"),
        ];
        if reject_first {
            assert!(matches!(
                publication::publish_zip_v2(
                    &second,
                    request(claim.clone(), outputs[..1].to_vec()),
                    &rules,
                    &config,
                    &config.provider_limits,
                )
                .await,
                Err(ipfs_s3_gateway::error::AppError::InvalidPinningRequest(_))
            ));
        }
        let result = publication::publish_zip_v2(
            &second,
            request(claim, outputs),
            &rules,
            &config,
            &config.provider_limits,
        )
        .await;
        let execution = execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap();
        let expected_rows = if reject_first {
            assert!(result.is_err());
            assert_eq!(execution.state, "fenced");
            0
        } else {
            assert!(
                matches!(result.unwrap(), publication::ZipV2PublicationResult::Published(rows) if rows.len() == 2)
            );
            assert_eq!(execution.state, "completed");
            2
        };
        assert_eq!(
            object::Entity::find().count(&first).await.unwrap(),
            expected_rows
        );
        assert_eq!(
            object_version::Entity::find().count(&first).await.unwrap(),
            expected_rows
        );
    }
}

#[tokio::test]
async fn lost_exact_output_guard_fences_old_claim_instead_of_recapturing_a_key() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one", "out/one", OUTPUT_CID)]).await;
    store::import::ownership::admit_content_mutation(
        &second,
        "bucket",
        "out/one",
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(
        publication::publish_zip_v2(
            &first,
            request(
                claim,
                vec![success("one", "out/one", OUTPUT_CID, "output-1")]
            ),
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(object::Entity::find().count(&second).await.unwrap(), 0);
    assert_eq!(
        execution::read(&second, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "fenced"
    );
}

#[tokio::test]
async fn failed_root_is_durable_but_does_not_erase_successful_output() {
    let (_dir, first, second) = setup().await;
    let claim = prepared_with_root(&first, &[("one", "out/one", OUTPUT_CID)], true).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    let mut result = request(
        claim,
        vec![success("one", "out/one", OUTPUT_CID, "output-1")],
    );
    result.root_outcome = zip::RootOutcome::Failed {
        code: "build_failed",
    };
    assert!(matches!(
        publication::publish_zip_v2(&second, result, &rules, &config, &config.provider_limits)
            .await
            .unwrap(),
        publication::ZipV2PublicationResult::Published(_)
    ));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 1);
    let root = zip::read(&second, "v2-execution").await.unwrap().unwrap();
    assert_eq!(root.root_status, "failed");
    assert_eq!(root.root_error_code.as_deref(), Some("build_failed"));
    assert!(root.root_cid.is_none());
}

#[tokio::test]
async fn unverified_root_cid_cannot_commit_written_output_leases_or_guard_completion() {
    let (_dir, first, second) = setup().await;
    let config = pin_config();
    let rules = output_rules(&config);
    let claim = prepared_with_controls(
        &first,
        &[("one", "out/a/one", OUTPUT_CID)],
        true,
        ZipTargets::Extracted,
    )
    .await;
    let mut publication = request(
        claim.clone(),
        vec![success("one", "out/a/one", OUTPUT_CID, "one")],
    );
    publication.targets = ZipTargets::Extracted;
    publication.root_outcome = zip::RootOutcome::Verified {
        claim: zip::RootClaim {
            batch_id: claim.batch_id.clone(),
            revision: 1,
            epoch: 1,
            worker: "claimant".into(),
        },
        node_identity: "hot".into(),
        tier: "hot".into(),
        cid: fixture_cid("unverified-candidate"),
    };
    assert!(
        publication::publish_zip_v2(
            &second,
            publication,
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&first).await.unwrap(),
        0
    );
    assert_eq!(pin_rows::count(&first, "pin_refs").await, 0);
    assert_eq!(pin_rows::count(&first, "pin_resources").await, 0);
    assert_eq!(
        execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        "admitted"
    );
}

#[tokio::test]
async fn source_key_is_never_replaced_or_versioned_for_any_bucket_versioning_state() {
    for state in [
        store::object_version::BucketVersioningState::Unversioned,
        store::object_version::BucketVersioningState::Enabled,
        store::object_version::BucketVersioningState::Suspended,
    ] {
        let (_dir, first, second) = setup().await;
        if state != store::object_version::BucketVersioningState::Unversioned {
            store::bucket::set_versioning_state(&first, "bucket", state)
                .await
                .unwrap();
        }
        let old = published(
            "prior-source",
            "source.zip",
            &fixture_cid("source-original"),
        );
        publication::publish_object(
            &first,
            publication::PublicationRequest {
                object_target: publication::PinTargetSpec {
                    cid: old.cid.clone(),
                    logical_size: old.logical_size,
                },
                object: old,
                tags: vec![],
                policy: PublicationPolicy {
                    tags: vec![],
                    leases: vec![],
                },
            },
            &Default::default(),
        )
        .await
        .unwrap();
        let source_before = object::Entity::find_by_id("prior-source")
            .one(&first)
            .await
            .unwrap()
            .unwrap();
        let count_before = object_version::Entity::find().count(&first).await.unwrap();
        let claim = prepared(&first, &[("one", "out/one", OUTPUT_CID)]).await;
        let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
        let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
        publication::publish_zip_v2(
            &second,
            request(
                claim,
                vec![success("one", "out/one", OUTPUT_CID, "output-1")],
            ),
            &rules,
            &config,
            &config.provider_limits,
        )
        .await
        .unwrap();
        let source_after = object::Entity::find_by_id("prior-source")
            .one(&second)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(source_after, source_before);
        assert_eq!(
            object_version::Entity::find().count(&second).await.unwrap(),
            count_before + 1
        );
        assert_eq!(object::Entity::find().count(&second).await.unwrap(), 2);
    }
}
