use std::collections::BTreeMap;

use chrono::Utc;
use ipfs_s3_gateway::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    import::SupersedeReason,
    pinning::{
        config::ValidatedPinningConfig,
        policy::PublicationPolicy,
        zip_policy::{
            ValidatedZipOutputRules, ZipOutputRuleConfig, ZipPublishedOutput, ZipRuleEffect,
            ZipTargets as PolicyTargets,
        },
    },
    store::{
        self,
        entities::{
            object, object_version, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
        },
        pinning::{
            ledger,
            publication::{
                self, PublicationObject, ZipV2Publication, ZipV2Success, v2_execution as execution,
            },
        },
        zip,
    },
    zip::options::ZipTargets,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, TransactionTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};
use sha2::{Digest, Sha256};

#[path = "../src/store/migrations/m20260927_000004_zip_v2_execution.rs"]
mod v2_schema;

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
    if first
        .query_one(sea_orm::Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT name FROM sqlite_master WHERE name='zip_v2_executions'",
        ))
        .await
        .unwrap()
        .is_none()
    {
        v2_schema::Migration
            .up(&SchemaManager::new(&first))
            .await
            .unwrap();
    }
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
    prepared_with_controls(db, successes, root_enabled, ZipTargets::None, None).await
}

async fn prepared_with_controls(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    root_enabled: bool,
    targets: ZipTargets,
    rule_revision: Option<&str>,
) -> execution::Claim {
    prepared_manifest_with_controls(db, successes, &[], root_enabled, targets, rule_revision).await
}

async fn prepared_manifest_with_controls(
    db: &DatabaseConnection,
    successes: &[(&str, &str, &str)],
    failures: &[(&str, &str)],
    root_enabled: bool,
    targets: ZipTargets,
    rule_revision: Option<&str>,
) -> execution::Claim {
    let contract = "{\"contract\":\"v2\"}";
    let admission = execution::Admission {
        id: "v2-execution".into(), owner: "principal".into(), source: "direct".into(), token: "v2-token".into(),
        request_fingerprint: hex::encode(Sha256::digest(contract.as_bytes())), request_contract: contract.into(),
        bucket: "bucket".into(), source_key: "source.zip".into(),
        captured_options: serde_json::json!({"options":{
            "publish_source":false,"publish_extracted":true,"targets":targets,"token":"v2-token",
            "root_override":null,"root_enabled":root_enabled,"result_version":2},"rule_revision":rule_revision}).to_string(),
    };
    execution::admit(db, &admission).await.unwrap();
    let claim = execution::claim(db, &admission.id, "worker", 30)
        .await
        .unwrap()
        .unwrap();
    execution::bind_clean_input(db, &claim, &"a".repeat(64), "input-cid", 99)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let items = successes
        .iter()
        .map(|(path, key, cid)| execution::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .chain(
            failures
                .iter()
                .map(|(path, code)| execution::ManifestItem::Failure {
                    path: (*path).into(),
                    code: (*code).into(),
                }),
        )
        .collect::<Vec<_>>();
    let mutations = successes
        .iter()
        .map(|(_, key, _)| ((*key).into(), format!("guard-{key}")))
        .collect::<BTreeMap<_, _>>();
    execution::admit_manifest_in_transaction(&tx, &claim, &items, &mutations)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    zip::admit(
        db,
        &zip::BatchAdmission {
            id: admission.id,
            owner: admission.owner,
            source: admission.source,
            token: admission.token,
            fingerprint: "pending".into(),
            bucket: "bucket".into(),
            archive_key: "source.zip".into(),
            input_identity: "pending".into(),
            captured_options: admission.captured_options,
        },
    )
    .await
    .unwrap();
    let entries = successes
        .iter()
        .map(|(path, key, cid)| zip::ManifestItem::Success {
            path: (*path).into(),
            object_key: (*key).into(),
            cid: (*cid).into(),
            size: 7,
        })
        .chain(
            failures
                .iter()
                .map(|(path, code)| zip::ManifestItem::Failure {
                    path: (*path).into(),
                    code: (*code).into(),
                }),
        )
        .collect::<Vec<_>>();
    zip::prepare_manifest(db, &claim.batch_id, &entries)
        .await
        .unwrap();
    claim
}

fn request(claim: execution::Claim, successes: Vec<ZipV2Success>) -> ZipV2Publication {
    ZipV2Publication {
        claim,
        source: None,
        source_policy: None,
        source_guard: None,
        successes,
        targets: ZipTargets::None,
        captured_rule_revision: None,
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

fn output_rules(config: &ValidatedPinningConfig) -> ValidatedZipOutputRules {
    ValidatedZipOutputRules::compile(
        &[
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
        ],
        config,
    )
    .unwrap()
}

fn planned_success(
    rules: &ValidatedZipOutputRules,
    path: &str,
    key: &str,
    cid: &str,
    id: &str,
) -> ZipV2Success {
    let mut result = success(path, key, cid, id);
    let output = ZipPublishedOutput {
        bucket: "bucket".into(),
        key: key.into(),
        version_id: "only-a-fixture-version".into(),
        cid: cid.into(),
    };
    let plan = rules
        .plan(
            PolicyTargets {
                source: false,
                extracted: true,
            },
            None,
            &[output],
        )
        .unwrap();
    result.policy.leases = plan.outputs[0]
        .intents
        .iter()
        .map(|planned| planned.intent.clone())
        .collect();
    result
}

#[tokio::test]
async fn each_output_gets_independent_automatic_policy_private_gets_no_lease_and_shared_cid_dedups_remotes()
 {
    let (_dir, first, second) = setup().await;
    let config = pin_config();
    let rules = output_rules(&config);
    for provider in &config.providers {
        ledger::register_route(&first, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
    let claim = prepared_with_controls(
        &first,
        &[
            ("a-one", "out/a/one", "shared-cid"),
            ("a-two", "out/a/two", "shared-cid"),
            ("b-one", "out/b/one", "shared-cid"),
            ("private", "out/private/secret", "shared-cid"),
        ],
        false,
        ZipTargets::Extracted,
        Some(rules.revision()),
    )
    .await;
    let mut publication = request(
        claim,
        vec![
            planned_success(&rules, "a-one", "out/a/one", "shared-cid", "one"),
            planned_success(&rules, "a-two", "out/a/two", "shared-cid", "two"),
            planned_success(&rules, "b-one", "out/b/one", "shared-cid", "three"),
            planned_success(
                &rules,
                "private",
                "out/private/secret",
                "shared-cid",
                "four",
            ),
        ],
    );
    publication.targets = ZipTargets::Extracted;
    publication.captured_rule_revision = Some(rules.revision().into());
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
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 3);
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(&first)
            .await
            .unwrap(),
        3
    );
    assert_eq!(remote_pin::Entity::find().count(&first).await.unwrap(), 2);
    for name in ["a", "b"] {
        let usage = pin_provider_usage::Entity::find_by_id(name)
            .one(&first)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (7, 1));
    }
    let targets = pin_lease_target::Entity::find().all(&first).await.unwrap();
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.provider == "a")
            .count(),
        2
    );
    assert_eq!(
        targets
            .iter()
            .filter(|target| target.provider == "b")
            .count(),
        1
    );
    assert!(
        pin_lease::Entity::find()
            .all(&first)
            .await
            .unwrap()
            .iter()
            .all(|lease| lease.source == "automatic")
    );
}

#[tokio::test]
async fn captured_rule_revision_cannot_be_replanned_with_new_permissions() {
    let (_dir, first, second) = setup().await;
    let config = pin_config();
    let rules = output_rules(&config);
    let claim = prepared_with_controls(
        &first,
        &[("one", "out/a/one", "cid-1")],
        false,
        ZipTargets::Extracted,
        Some(&"0".repeat(64)),
    )
    .await;
    let mut publication = request(
        claim,
        vec![planned_success(&rules, "one", "out/a/one", "cid-1", "one")],
    );
    publication.targets = ZipTargets::Extracted;
    publication.captured_rule_revision = Some("0".repeat(64));
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
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 0);
}

#[tokio::test]
async fn source_absent_publishes_only_final_successes_from_two_connections() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one.txt", "out/one.txt", "cid-1")]).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    publication::publish_zip_v2(
        &second,
        request(
            claim,
            vec![success("one.txt", "out/one.txt", "cid-1", "output-1")],
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
    assert_eq!(pin_lease::Entity::find().count(&second).await.unwrap(), 0);
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
async fn mismatched_final_cid_cannot_publish_any_object_or_complete_guards() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one.txt", "out/one.txt", "cid-1")]).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(
        publication::publish_zip_v2(
            &second,
            request(
                claim,
                vec![success("one.txt", "out/one.txt", "wrong", "output-1")]
            ),
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
    assert_eq!(
        execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "admitted"
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
    let claim = prepared(&first, &[("one", "out/one", "cid-1")]).await;
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
                    vec![success("one", "out/one", "cid-1", "output-1")]
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
    let claim = prepared(&first, &[("one", "out/one", "cid-1")]).await;
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
            request(claim, vec![success("one", "out/one", "cid-1", "output-1")]),
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
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 0);
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
        None,
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
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 0);
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
    let (_dir, first, second) = setup().await;
    let claim = prepared_manifest_with_controls(
        &first,
        &[],
        &[("failed/file", "invalid_zip")],
        true,
        ZipTargets::None,
        None,
    )
    .await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(
        publication::publish_zip_v2(
            &second,
            request(claim.clone(), vec![]),
            &rules,
            &config,
            &config.provider_limits
        )
        .await
        .is_err()
    );
    let mut request = request(claim, vec![]);
    request.root_outcome = zip::RootOutcome::Empty;
    assert!(matches!(
        publication::publish_zip_v2(&second, request, &rules, &config, &config.provider_limits)
            .await
            .unwrap(),
        publication::ZipV2PublicationResult::Published(results) if results.is_empty()
    ));
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 0);
    let batch = zip::snapshot(&first, "v2-execution")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.batch.state, "published");
    assert_eq!(batch.batch.root_status, "empty");
    assert!(batch.batch.root_cid.is_none());
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(
        execution::read(&first, "v2-execution")
            .await
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
}

#[tokio::test]
async fn every_durable_target_must_be_written_or_the_entire_publication_rolls_back() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(
        &first,
        &[("one", "out/one", "cid-1"), ("two", "out/two", "cid-2")],
    )
    .await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    assert!(
        publication::publish_zip_v2(
            &second,
            request(
                claim.clone(),
                vec![success("one", "out/one", "cid-1", "output-1")]
            ),
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
    let completed = publication::publish_zip_v2(
        &second,
        request(
            claim,
            vec![
                success("one", "out/one", "cid-1", "output-1"),
                success("two", "out/two", "cid-2", "output-2"),
            ],
        ),
        &rules,
        &config,
        &config.provider_limits,
    )
    .await
    .unwrap();
    assert!(
        matches!(completed, publication::ZipV2PublicationResult::Published(rows) if rows.len() == 2)
    );
    assert_eq!(object::Entity::find().count(&first).await.unwrap(), 2);
}

#[tokio::test]
async fn lost_exact_output_guard_fences_old_claim_instead_of_recapturing_a_key() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one", "out/one", "cid-1")]).await;
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
            request(claim, vec![success("one", "out/one", "cid-1", "output-1")]),
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
    let claim = prepared_with_root(&first, &[("one", "out/one", "cid-1")], true).await;
    let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
    let mut result = request(claim, vec![success("one", "out/one", "cid-1", "output-1")]);
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
    for provider in &config.providers {
        ledger::register_route(&first, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
    let claim = prepared_with_controls(
        &first,
        &[("one", "out/a/one", "cid-1")],
        true,
        ZipTargets::Extracted,
        Some(rules.revision()),
    )
    .await;
    let mut publication = request(
        claim.clone(),
        vec![planned_success(&rules, "one", "out/a/one", "cid-1", "one")],
    );
    publication.targets = ZipTargets::Extracted;
    publication.captured_rule_revision = Some(rules.revision().into());
    publication.root_outcome = zip::RootOutcome::Verified {
        claim: zip::RootClaim {
            batch_id: claim.batch_id.clone(),
            revision: 1,
            epoch: 1,
            worker: "claimant".into(),
        },
        node_identity: "hot".into(),
        tier: "hot".into(),
        cid: "unverified-candidate".into(),
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
    assert_eq!(pin_lease::Entity::find().count(&first).await.unwrap(), 0);
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(&first)
            .await
            .unwrap(),
        0
    );
    assert_eq!(remote_pin::Entity::find().count(&first).await.unwrap(), 0);
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
        let old = published("prior-source", "source.zip", "source-original");
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
        let claim = prepared(&first, &[("one", "out/one", "cid-1")]).await;
        let config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
        let rules = ValidatedZipOutputRules::compile(&[], &config).unwrap();
        publication::publish_zip_v2(
            &second,
            request(claim, vec![success("one", "out/one", "cid-1", "output-1")]),
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
