//! Registered Stage 7 read-only native capture. No CLI or action executor.
use ipfs_s3_gateway::pinning::management::{self, db_snapshot};

use chrono::Utc;
use db_snapshot::{EvidenceGap, SnapshotError, collect};
pub use ipfs_s3_gateway::{pinning, store};
use management::*;
use pinning::{
    config::{
        LeaseDuration, ProviderKind, ProviderLimits, ValidatedPinningConfig, ValidatedProvider,
    },
    identity::{CleanupMode as DbCleanup, ProviderIdentity},
    policy::{LeaseIntent, LeaseSource, PublicationPolicy},
    provider::canonical_resource_cid,
    tags::ContentMode,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement,
    TransactionTrait,
};
use store::{
    entities::{object, object_version, pin_lease, pin_lease_target},
    pinning::{
        ledger,
        publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};

const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
const OTHER: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn config() -> ValidatedPinningConfig {
    let identity = ProviderIdentity {
        provider_id: "snapshot-provider".into(),
        display_name: "Fixture".into(),
        backend: "fixture".into(),
        scope: "isolated".into(),
        storage_domain: "independent".into(),
        credential_revision: 1,
        endpoint_revision: 2,
        secret_ref: Some("env:DO_NOT_OUTPUT_SECRET_REF".into()),
        api_profile: "psa".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: DbCleanup::Retain,
    };
    let limits = ProviderLimits {
        priority: 1,
        max_bytes: 10_000,
        max_pins: 100,
        enabled: true,
    };
    let key = identity.allocation_key();
    ValidatedPinningConfig {
        worker_interval: LeaseDuration::parse("1s").unwrap(),
        worker_concurrency: 1,
        providers: vec![ValidatedProvider {
            cluster: None,
            name: "display-alias".into(),
            identity,
            kind: ProviderKind::Noop,
            token: None,
            endpoint: None,
            pinata: None,
            rpc: None,
            limits: limits.clone(),
            requests_per_second: None,
        }],
        policies: vec![],
        provider_limits: [(key, limits)].into(),
    }
}

fn route(config: &ValidatedPinningConfig) -> RouteIdentity {
    let id = &config.providers[0].identity;
    RouteIdentity {
        provider_id: id.provider_id.clone(),
        backend: id.backend.clone(),
        scope: id.scope.clone(),
        storage_domain: id.storage_domain.clone(),
        api_profile: id.api_profile.clone(),
        strategy: id.strategy.clone(),
        resource_type: match id.resource_type() {
            pinning::identity::RemoteResourceType::RpcPin => ResourceType::RpcPin,
            pinning::identity::RemoteResourceType::PsaRequest => ResourceType::PsaRequest,
            pinning::identity::RemoteResourceType::HostedFile => ResourceType::HostedFile,
            pinning::identity::RemoteResourceType::ClusterPin => ResourceType::ClusterPin,
        },
        credential_revision: id.credential_revision,
        endpoint_revision: id.endpoint_revision,
        original: None,
    }
}

async fn setup(db: &DatabaseConnection, config: &ValidatedPinningConfig) {
    store::run_migrations(db).await.unwrap();
    store::bucket::create(db, "bucket", None).await.unwrap();
    ledger::register_route(
        db,
        &config.providers[0].identity.allocation_key(),
        &config.providers[0].identity,
    )
    .await
    .unwrap();
}

async fn publish(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    key: &str,
    cid: &str,
    with_lease: bool,
) -> ExactObject {
    let id = uuid::Uuid::new_v4().to_string();
    let provider = config.providers[0].identity.allocation_key();
    let leases = if with_lease {
        vec![LeaseIntent {
            source: LeaseSource::Automatic,
            policy_id: "fixture-policy".into(),
            provider_mode: pinning::config::ProviderMode::All,
            providers: vec![provider],
            content_mode: ContentMode::Object,
            duration: LeaseDuration::parse("1h").unwrap(),
        }]
    } else {
        vec![]
    };
    publication::publish_object(
        db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.clone(),
                "bucket",
                key,
                cid.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases,
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 10,
            },
        },
        &config.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&id))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    ExactObject {
        bucket: "bucket".into(),
        key: key.into(),
        public_version: version
            .version_id
            .map(PublicVersionId::Version)
            .unwrap_or(PublicVersionId::Null),
        private_version_id: version.id,
        object_id: id,
        cid: canonical_resource_cid(cid).unwrap(),
    }
}

fn request(
    identity: ExactObject,
    config: &ValidatedPinningConfig,
    target: Option<String>,
) -> PlanRequest {
    let intent = Intent {
        started_at: 1,
        expires_at: i64::MAX,
        requirements: Requirements {
            mode: ProviderMode::All,
            provider_ids: vec![route(config).provider_id],
            levels: vec![ConfirmationLevel::RemotePinned],
            min_replicas: 1,
            max_age_secs: None,
            policy_revision: "explicit-policy".into(),
        },
    };
    PlanRequest {
        plan_id: "snapshot-plan".into(),
        items: vec![ItemRequest {
            item_id: "selected-object".into(),
            action: if target.is_some() {
                Action::Retry
            } else {
                Action::Backfill(intent)
            },
            subject: Subject::Object {
                identity,
                target: target
                    .map(|target_id| TargetSelection::Existing {
                        target_id,
                        source: None,
                    })
                    .unwrap_or(TargetSelection::New(route(config))),
            },
        }],
    }
}

/// Every persistent table, including migration metadata. No hand-built schema.
async fn contents(db: &DatabaseConnection) -> Vec<(String, Vec<String>)> {
    let backend = db.get_database_backend();
    let tables = db
        .query_all(Statement::from_string(
            backend,
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name".to_owned(),
        ))
        .await
        .unwrap();
    let mut result = Vec::new();
    for table in tables {
        let name: String = table.try_get("", "name").unwrap();
        let table_name = name.replace('"', "\"\"");
        let columns = db
            .query_all(Statement::from_string(
                backend,
                format!("PRAGMA table_info(\"{table_name}\")"),
            ))
            .await
            .unwrap();
        let columns: Vec<_> = columns
            .into_iter()
            .map(|row| {
                let column: String = row.try_get("", "name").unwrap();
                let column = format!("\"{}\"", column.replace('"', "\"\""));
                format!("CASE WHEN typeof({column}) = 'blob' THEN hex({column}) ELSE {column} END")
            })
            .collect();
        let rows = db
            .query_all(Statement::from_string(
                backend,
                format!(
                    "SELECT json_array({}) AS content FROM \"{table_name}\"",
                    columns.join(",")
                ),
            ))
            .await
            .unwrap();
        let mut rows: Vec<String> = rows
            .into_iter()
            .map(|row| row.try_get("", "content").unwrap())
            .collect();
        rows.sort();
        result.push((name, rows));
    }
    result
}

#[tokio::test]
async fn snapshot_adapter_reads_real_facts_and_refuses_missing_fences_without_db_writes() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "old-entry", CID, true).await;
    let target = pin_lease_target::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let lease = pin_lease::Entity::find_by_id(target.lease_id.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let before = contents(&db).await;
    let result = collect(
        &db,
        &config,
        &request(identity.clone(), &config, Some(target.id.clone())),
    )
    .await
    .unwrap();
    assert!(result.objects[0].identity == identity);
    assert_eq!(result.objects[0].original_cid, CID);
    assert_eq!(result.leases[0].generation, lease.generation);
    assert_eq!(result.targets[0].id, target.id);
    assert!(result.applied.is_empty());
    let output = serde_json::to_string(&result).unwrap();
    assert!(!output.contains("DO_NOT_OUTPUT_SECRET_REF"));
    assert!(!output.contains("secret_ref"));
    let planning = result.planning_snapshot();
    assert!(
        planning.is_ok(),
        "native capture must be plannable, with per-item evidence gaps"
    );
    let planning = planning.unwrap();
    assert_eq!(planning.targets[0].native_tuple.len(), 8);
    assert!(planning.native_frame.is_some());
    assert!(before == contents(&db).await);
    assert!(
        object::Entity::find_by_id(identity.object_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .cid
            == CID
    );
}

#[tokio::test]
async fn snapshot_adapter_null_version_never_falls_back_to_replacement() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let old = publish(&db, &config, "same-key", CID, false).await;
    let _new = publish(&db, &config, "same-key", OTHER, false).await;
    let before = contents(&db).await;
    assert_eq!(
        collect(&db, &config, &request(old, &config, None))
            .await
            .err(),
        Some(SnapshotError::MissingExactObject)
    );
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_file_two_connections_hold_one_snapshot_across_concurrent_commit() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("snapshot.sqlite").display()
    );
    let reader = store::connect_database(&url).await.unwrap();
    let config = config();
    setup(&reader, &config).await;
    reader
        .execute_unprepared("PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    let writer = store::connect_database(&url).await.unwrap();
    let identity = publish(&writer, &config, "entry", CID, true).await;
    let before = contents(&reader).await;
    let result = collect(&reader, &config, &request(identity, &config, None))
        .await
        .unwrap();
    let tx = reader.begin().await.unwrap();
    let original_native = store::pinning::management::native::capture(&tx)
        .await
        .unwrap();
    let mut changed = config.providers[0].identity.clone();
    changed.endpoint_revision += 1;
    ledger::register_route(&writer, &changed.allocation_key(), &changed)
        .await
        .unwrap();
    assert!(
        original_native
            == store::pinning::management::native::capture(&tx)
                .await
                .unwrap()
    );
    tx.rollback().await.unwrap();
    assert!(
        result
            .routes
            .iter()
            .any(|row| row.identity.endpoint_revision == 2 && row.allocation_allowed)
    );
    let after = contents(&writer).await;
    assert!(before != after);
    let again = collect(
        &reader,
        &config,
        &request(result.objects[0].identity.clone(), &config, None),
    )
    .await
    .unwrap();
    assert!(
        again
            .routes
            .iter()
            .any(|row| row.identity.endpoint_revision == 3 && !row.available)
    );
    assert!(after == contents(&reader).await);
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_adapter_public_historical_version_is_bound_to_immutable_object_not_current_key() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    store::bucket::set_versioning_state(
        &db,
        "bucket",
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    let old = publish(&db, &config, "same-key", CID, false).await;
    assert!(matches!(old.public_version, PublicVersionId::Version(_)));
    let latest = publish(&db, &config, "same-key", OTHER, false).await;
    let before = contents(&db).await;
    let captured = collect(&db, &config, &request(old.clone(), &config, None))
        .await
        .unwrap();
    assert!(captured.objects[0].identity == old);
    assert!(captured.objects[0].identity != latest);
    assert_eq!(captured.objects[0].original_cid, CID);
    assert!(
        !object_version::Entity::find_by_id(old.private_version_id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .is_latest
    );
    let mut substituted = old;
    substituted.public_version = latest.public_version;
    assert_eq!(
        collect(&db, &config, &request(substituted, &config, None))
            .await
            .err(),
        Some(SnapshotError::MissingExactObject)
    );
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_formally_parses_cids_and_does_not_echo_input_or_db_errors() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let mut identity = publish(&db, &config, "entry", CID, false).await;
    identity.cid = "bafyinvalidnotacid".into(); // Passes pure syntax checks, NOT the CID parser.
    let before = contents(&db).await;
    let error = collect(&db, &config, &request(identity, &config, None))
        .await
        .err()
        .unwrap();
    assert_eq!(error, SnapshotError::InvalidCid);
    assert!(!error.to_string().contains("bafyinvalidnotacid"));
    assert!(before == contents(&db).await);
    let empty = store::connect_database("sqlite::memory:").await.unwrap(); // Deliberately not migrated.
    let error = collect(
        &empty,
        &config,
        &PlanRequest {
            plan_id: "provider-read".into(),
            items: vec![ItemRequest {
                item_id: "provider".into(),
                subject: Subject::Provider {
                    route: route(&config),
                },
                action: Action::Retire,
            }],
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error, SnapshotError::UnsupportedEvidence);
    assert!(!error.to_string().contains("object_versions"));
}

#[tokio::test]
async fn snapshot_adapter_legacy_and_skipped_are_candidates_with_real_decision_revisions() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "old-zip-entry", CID, false).await;
    let selected = request(identity.clone(), &config, None);
    let before = contents(&db).await;
    let legacy = collect(&db, &config, &selected).await.unwrap();
    assert!(legacy.objects[0].legacy_unknown);
    assert_eq!(legacy.objects[0].decision, None);
    assert!(legacy.objects[0].selection == SelectionEvidence::LegacyZipCandidate);
    assert!(legacy.objects[0].manifest_bindings.is_empty());
    assert!(legacy.targets.is_empty() && legacy.jobs.is_empty());
    assert!(before == contents(&db).await);

    let tags = vec![pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")];
    let evaluator = pinning::policy::PinPolicyEvaluator::with_mode(
        &config,
        ipfs_s3_gateway::config::OptionalPinControlMode::Warn,
    );
    let (_, decision) = evaluator
        .evaluate_publication_decision(
            pinning::policy::PublicationContext {
                bucket: "bucket",
                key: "old-zip-entry",
                tags: &tags,
                is_decompress_zip: true,
            },
            pinning::decision::DecisionOrigin::new("authorized-fixture", "admission-fixture"),
        )
        .unwrap();
    let tx = db.begin().await.unwrap();
    store::pinning::tags::replace_object_tags(&tx, &identity.object_id, &tags)
        .await
        .unwrap();
    store::pinning::decision::write_for_object_in_transaction(&tx, &identity.object_id, &decision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = contents(&db).await;
    let skipped = collect(&db, &config, &selected).await.unwrap();
    assert_eq!(skipped.objects[0].decision, Some(DecisionState::Skipped));
    assert_eq!(
        skipped.objects[0].control_revision.as_deref(),
        Some(decision.control_revision.as_str())
    );
    assert_eq!(
        skipped.objects[0].config_revision.as_deref(),
        Some(decision.config_revision.as_str())
    );
    assert!(skipped.objects[0].selection == SelectionEvidence::LegacyZipCandidate);
    assert!(skipped.targets.is_empty() && skipped.jobs.is_empty());
    assert!(skipped.applied.is_empty());
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_wire_request_does_not_supply_snapshot_authority_or_receipts() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "entry", CID, false).await;
    let selected = request(identity, &config, None);
    let Subject::Object { identity, .. } = &selected.items[0].subject else {
        unreachable!()
    };
    let selected_route = route(&config);
    // ATTACK INPUT ONLY. These claims are not written to the DB or treated as
    // facts by this fixture. In particular no ApplicationCreated is invented.
    let mut untrusted = Snapshot {
        observed_at: 100,
        objects: vec![ObjectSnapshot {
            identity: identity.clone(),
            logical_bytes: 10,
            readable: Some(true),
            read_evidence: None,
            permitted: Some(true),
            allowed_routes: vec![selected_route.clone()],
            control_revision: Some("client-claim".into()),
            config_revision: Some("client-claim".into()),
            decision: Some(DecisionState::None),
            evidence: SelectionEvidence::ExplicitObject,
        }],
        routes: vec![RouteSnapshot {
            identity: selected_route.clone(),
            original: None,
            available: true,
            allocation_allowed: true,
            cleanup: CleanupMode::Retain,
            exclusive_management: false,
        }],
        ledger_domains: vec![LedgerDomainSnapshot {
            backend: selected_route.backend,
            scope: selected_route.scope,
            resource_type: selected_route.resource_type,
            frontier: vec![NativeKey {
                table: "pin_jobs".into(),
                values: vec!["forged".into()],
            }],
            complete: true,
        }],
        ..Snapshot::default()
    };
    let plan = dry_run(&selected, &untrusted).unwrap();
    assert!(!matches!(plan.items()[0].status, PlanStatus::Blocked(_)));
    untrusted.applied.push(AppliedItem {
        binding: plan.items()[0].clone(),
    });
    let plan = dry_run(&selected, &untrusted).unwrap();
    use management::wire::v3;
    let mut writer = v3::Writer::new(
        Vec::new(),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    v3::planning::prefix(
        &mut writer,
        v3::planning::Header {
            projection: v3::Projection::Review,
            database_scope: "client-scope",
            database_identity: "client-identity",
            principal: "client-principal",
            catalog: &["client-catalog".into()],
            created_at: "client-time".into(),
            expires_at: None,
        },
        &selected,
        &untrusted.configuration,
        plan.items(),
        *plan.summary(),
        None,
    )
    .unwrap();
    v3::planning::fact(
        &mut writer,
        v3::Section::AppliedClaim,
        "management_applied",
        0,
        &untrusted.applied[0],
    )
    .unwrap();
    let bytes = writer.finish().unwrap();
    let (imported, _, _) = v3::document::inspect(
        std::io::Cursor::new(bytes),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    assert!(imported.applied_claims);
    assert!(imported.acknowledgements.is_none());
    let before = contents(&db).await;
    // The only accepted wire input is its request, never its supplied Snapshot.
    let actual = collect(&db, &config, imported.request.as_ref().unwrap())
        .await
        .unwrap();
    assert_eq!(actual.objects.len(), 1);
    assert!(actual.applied.is_empty());
    let native = actual.planning_snapshot().unwrap();
    assert!(native.applied.is_empty());
    assert_eq!(native.objects[0].readable, None);
    assert!(native.native_frame.is_some());
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_rpc_inflight_unknown_resources_and_historical_route_do_not_become_ownership()
 {
    use pinning::provider::{
        ObservedResource, ObservedResourceStatus, ProviderError, ProviderErrorClass, SubmitEffect,
        SubmitObservation,
    };
    use store::pinning::jobs;
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let mut config = config();
    config.providers[0].identity.api_profile = "kubo".into();
    let provider = config.providers[0].identity.allocation_key();
    config.provider_limits = [(provider.clone(), config.providers[0].limits.clone())].into();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "entry", CID, true).await;
    let target = pin_lease_target::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let selected = request(identity, &config, Some(target.id.clone()));
    let now = Utc::now() + chrono::Duration::seconds(2);
    let claimed = jobs::claim_due_jobs(&db, now, chrono::Duration::minutes(1), 1)
        .await
        .unwrap()
        .remove(0);
    jobs::record_submit_invocation(&db, &claimed, "kubo", "cid", now)
        .await
        .unwrap();
    let observation_id = ledger::submission::begin(&db, &claimed, now)
        .await
        .unwrap()
        .unwrap();
    let stored = store::entities::pin_submit_observation::Entity::find_by_id(&observation_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let before = contents(&db).await;
    let inflight = collect(&db, &config, &selected).await.unwrap();
    assert!(inflight.submissions[0].inflight);
    assert_eq!(inflight.submissions[0].remote_epoch, stored.remote_epoch);
    assert_eq!(inflight.jobs[0].invocation_epoch, Some(stored.remote_epoch));
    assert_eq!(inflight.remotes[0].effect, SideEffect::InFlight);
    assert_eq!(inflight.remotes[0].ownership, Ownership::Unknown);
    assert_eq!(inflight.usage[0].reserved_bytes, 10);
    assert_eq!(inflight.usage[0].reserved_pins, 1);
    assert!(
        inflight
            .needs_attention
            .contains(&EvidenceGap::SubmissionProjectionUnavailable)
    );
    assert!(before == contents(&db).await);

    let resources = [CID, OTHER]
        .into_iter()
        .map(|cid| ObservedResource {
            replica_evidence: None,
            resource_type: pinning::identity::RemoteResourceType::RpcPin,
            cid: cid.into(),
            request_id: cid.into(),
            status: ObservedResourceStatus::PinAccepted,
            ownership: pinning::identity::Ownership::Unknown,
        })
        .collect();
    ledger::submission::record(
        &db,
        &claimed,
        &observation_id,
        &SubmitObservation {
            effect: SubmitEffect::Unknown,
            resources,
            result: Err(ProviderError {
                class: ProviderErrorClass::Transient,
                message: "DO_NOT_OUTPUT_PROVIDER_ERROR_OR_TOKEN".into(),
                retry_after: None,
            }),
        },
        now,
    )
    .await
    .unwrap();
    // Real route replacement; immutable invocation/resource evidence stays old.
    config.providers[0].identity.endpoint_revision += 1;
    ledger::register_route(&db, &provider, &config.providers[0].identity)
        .await
        .unwrap();
    let before = contents(&db).await;
    let unknown = collect(&db, &config, &selected).await.unwrap();
    assert_eq!(unknown.submissions[0].effect, SubmitEffect::Unknown);
    assert!(!unknown.submissions[0].inflight);
    assert!(unknown.submissions[0].needs_attention);
    assert_eq!(unknown.submissions[0].resources.len(), 2);
    assert!(
        unknown.submissions[0]
            .resources
            .iter()
            .all(|resource| resource.ownership == Ownership::Unknown)
    );
    assert_eq!(unknown.submissions[0].resources[0].original_cid, CID);
    assert!(
        unknown.submissions[0]
            .resources
            .iter()
            .any(|resource| resource.cid == canonical_resource_cid(OTHER).unwrap())
    );
    assert_eq!(
        unknown.targets[0]
            .registered_route
            .as_ref()
            .unwrap()
            .endpoint_revision,
        3
    );
    assert_eq!(
        unknown.jobs[0]
            .historical_route
            .as_ref()
            .unwrap()
            .endpoint_revision,
        2
    );
    assert!(
        unknown
            .routes
            .iter()
            .any(|route| route.identity.endpoint_revision == 2 && !route.available)
    );
    assert!(
        unknown
            .routes
            .iter()
            .any(|route| route.identity.endpoint_revision == 3 && route.allocation_allowed)
    );
    assert_eq!(unknown.usage[0].reserved_bytes, 10);
    assert!(unknown.applied.is_empty());
    let safe_output = serde_json::to_string(&unknown).unwrap();
    assert!(!safe_output.contains("DO_NOT_OUTPUT_PROVIDER_ERROR_OR_TOKEN"));
    assert!(!safe_output.contains("DO_NOT_OUTPUT_SECRET_REF"));
    assert!(unknown.planning_snapshot().unwrap().native_frame.is_some());
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_retained_cleanup_generation_and_historical_epoch_remain_distinct() {
    use store::pinning::{leases, quota};
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "entry", CID, true).await;
    let target = pin_lease_target::Entity::find()
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let selected = request(identity, &config, Some(target.id.clone()));
    let initial = collect(&db, &config, &selected).await.unwrap();
    let generation = initial.leases[0].generation;
    // No provider exists in this fixture and no invocation was dispatched. The
    // production lifecycle/quota primitives create the history, not raw inserts.
    leases::cancel_lease(&db, &target.lease_id, Utc::now())
        .await
        .unwrap();
    ledger::mark_effect(&db, &target.provider, &target.cid, "retained")
        .await
        .unwrap();
    let before = contents(&db).await;
    let retained = collect(&db, &config, &selected).await.unwrap();
    assert!(retained.leases[0].generation > generation);
    assert_eq!(
        retained.targets[0].state,
        db_snapshot::StoredTargetState::Released
    );
    assert!(retained.remotes[0].retained);
    assert_eq!(retained.remotes[0].ownership, Ownership::Unknown);
    assert_eq!(retained.remotes[0].accounted_bytes, Some(10));
    assert_eq!(retained.usage[0].reserved_bytes, 10);
    assert!(before == contents(&db).await);
    ledger::mark_effect(&db, &target.provider, &target.cid, "cleanup_pending")
        .await
        .unwrap();
    let before = contents(&db).await;
    let cleanup = collect(&db, &config, &selected).await.unwrap();
    assert!(cleanup.remotes[0].cleanup_pending);
    assert_eq!(cleanup.usage[0].reserved_bytes, 10);
    assert!(before == contents(&db).await);

    let tx = db.begin().await.unwrap();
    let remote = store::entities::remote_pin::Entity::find_by_id((
        target.provider.clone(),
        target.cid.clone(),
    ))
    .one(&tx)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        quota::confirmed_release(&tx, &target.provider, &target.cid, remote.epoch, Utc::now())
            .await
            .unwrap(),
        quota::ConfirmedReleaseOutcome::Released
    );
    tx.commit().await.unwrap();
    let before = contents(&db).await;
    let native_before = store::pinning::management::native::capture(&db)
        .await
        .unwrap();
    let archived = collect(&db, &config, &selected).await.unwrap();
    assert_eq!(archived.historical_remotes.len(), 1);
    assert_eq!(archived.historical_remotes[0].epoch, Some(remote.epoch));
    assert_eq!(archived.historical_remotes[0].accounted_bytes, None);
    assert_eq!(archived.historical_remotes[0].application_references, None);
    assert_eq!(archived.historical_remotes[0].inflight_jobs, None);
    assert!(archived.historical_remotes[0].confirmed_absent);
    assert!(
        archived
            .needs_attention
            .contains(&EvidenceGap::HistoricalAccountingUnavailable)
    );
    let native = archived.planning_snapshot().unwrap();
    // Current and archive share the released epoch. Neither projection is an
    // execution authority; every original PK/private history remains bound.
    assert!(native.remotes.is_empty());
    assert!(native.historical_remotes.is_empty());
    let frame = native.native_frame.as_ref().unwrap();
    let epoch = remote.epoch.to_string();
    for key in [
        NativeKey {
            table: "remote_pins".into(),
            values: vec![remote.provider.clone(), remote.cid.clone()],
        },
        NativeKey {
            table: "remote_pin_ledger".into(),
            values: vec![remote.provider.clone(), remote.cid.clone()],
        },
        NativeKey {
            table: "pin_resource_history".into(),
            values: vec![remote.provider.clone(), remote.cid.clone(), epoch.clone()],
        },
    ] {
        assert!(frame.frontier.contains(&key));
        assert!(native.unknown_resources.contains(&key));
        let values = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        assert!(
            frame.row(&key.table, &values).unwrap()
                == native_before.row(&key.table, &values).unwrap()
        );
    }
    let history_key = [
        remote.provider.as_str(),
        remote.cid.as_str(),
        epoch.as_str(),
    ];
    assert!(
        store::pinning::management::native::value(
            frame.row("pin_resource_history", &history_key).unwrap(),
            "ledger"
        )
        .is_none()
    );
    let original_history = native_before
        .original_value("pin_resource_history", &history_key, "ledger")
        .unwrap();
    assert!(
        frame.original_value("pin_resource_history", &history_key, "ledger")
            == Some(original_history)
    );
    assert!(frame == &native_before);
    assert!(
        frame
            == &store::pinning::management::native::capture(&db)
                .await
                .unwrap()
    );
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_provider_inventory_preserves_real_reservations_but_has_no_invented_frontier()
 {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    let identity = publish(&db, &config, "entry", CID, true).await;
    let mut retired = config.providers[0].identity.clone();
    retired.retired = true;
    ledger::register_route(&db, &retired.allocation_key(), &retired)
        .await
        .unwrap();
    let selected = PlanRequest {
        plan_id: "provider-retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            subject: Subject::Provider {
                route: route(&config),
            },
            action: Action::Retire,
        }],
    };
    let before = contents(&db).await;
    let inventory = collect(&db, &config, &selected).await.unwrap();
    assert!(inventory.objects.is_empty()); // No implicit prefix/key discovery.
    assert_eq!(inventory.targets.len(), 1);
    assert_eq!(inventory.leases[0].object_id, identity.object_id);
    assert_eq!(inventory.usage[0].reserved_bytes, 10);
    assert_eq!(inventory.usage[0].reserved_pins, 1);
    assert!(!inventory.routes[0].allocation_allowed);
    let native = inventory.planning_snapshot().unwrap();
    assert!(!native.inventories[0].frontier.is_empty());
    assert!(native.native_frame.is_some());
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn snapshot_adapter_inventory_budget_counts_utf8_bytes_without_returning_database_text() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    // Production writer, migrated schema. Fewer characters than MAX_TEXT_BYTES,
    // more UTF-8 bytes; a character-count budget would incorrectly accept it.
    let mut changed = config.providers[0].identity.clone();
    changed.display_name = "界".repeat(MAX_TEXT_BYTES / 2 + 1);
    ledger::register_route(&db, &changed.allocation_key(), &changed)
        .await
        .unwrap();
    let selected = PlanRequest {
        plan_id: "provider-retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            subject: Subject::Provider {
                route: route(&config),
            },
            action: Action::Retire,
        }],
    };
    let before = contents(&db).await;
    let error = collect(&db, &config, &selected).await.err().unwrap();
    assert_eq!(error, SnapshotError::LimitExceeded);
    assert!(!error.to_string().contains('界'));
    assert!(before == contents(&db).await);
}

#[tokio::test]
async fn management_root_release_binds_exact_reference_owner_build_and_native_history() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    let config = config();
    setup(&db, &config).await;
    for batch in ["selected-root", "shared-root"] {
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,state,manifest_prepared,terminal_result,root_status,root_cid,root_revision,root_epoch,created_at,updated_at) VALUES($1,$2,'direct',$1,'fingerprint','bucket','archive.zip','{}','{}','published',TRUE,'{}','complete',$3,1,2,'2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
            [batch.into(), format!("owner-{batch}").into(), CID.into()])).await.unwrap();
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO zip_root_builds(batch_id,revision,epoch,worker,lease_until,status,created_at,updated_at) VALUES($1,1,2,'root-worker','2026-10-12T00:01:00Z','verified','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
            [batch.into()])).await.unwrap();
        db.execute(Statement::from_sql_and_values(db.get_database_backend(),
            "INSERT INTO zip_root_references(batch_id,revision,epoch,node_identity,tier,cid,state,verification_receipt,created_at,updated_at) VALUES($1,1,2,'node-a','hot',$2,$3,'verified-root-fixture','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')",
            [batch.into(), CID.into(), if batch == "selected-root" {"adopted"} else {"retained"}.into()])).await.unwrap();
    }
    let key = NativeKey {
        table: "zip_root_references".into(),
        values: vec![
            "selected-root".into(),
            "1".into(),
            "2".into(),
            "node-a".into(),
            "hot".into(),
            CID.into(),
        ],
    };
    let action: Action = serde_json::from_value(serde_json::json!({"retire_with":{"selection":{"release_root_references":{"references":[key]}}}})).unwrap();
    let request = PlanRequest {
        plan_id: "root-release".into(),
        items: vec![ItemRequest {
            item_id: "release".into(),
            action,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(config.providers[0].route_snapshot()),
            },
        }],
    };
    use retirement::{InventoryMembership, MembershipCursor, RetirementMutationScope};
    let directory = tempfile::tempdir().unwrap();
    let spool = root_inventory(&db, &config, directory.path()).await;
    let review = spool.frozen_review().await.unwrap();
    let mut shared = key.clone();
    shared.values[0] = "shared-root".into();
    assert_eq!(review.totals().roots, 2);
    assert!(review.contains_root_responsibility(&key).await.unwrap());
    assert!(review.contains_root_responsibility(&shared).await.unwrap());
    let mut cursor = MembershipCursor::default();
    let mut roots = Vec::new();
    let mut root_eof = false;
    while let Some(member) = spool.read_membership(&mut cursor).await.unwrap() {
        match member {
            InventoryMembership::Member(key) if key.table == "zip_root_references" => {
                roots.push(key)
            }
            InventoryMembership::End { table } if table == "zip_root_references" => root_eof = true,
            _ => {}
        }
    }
    assert_eq!(roots, vec![key.clone(), shared.clone()]);
    assert!(root_eof);
    let snapshot = selected_root_snapshot(&db, &config, &key).await;
    let plan = root_plan(&request, &snapshot, &spool, directory.path())
        .await
        .unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    assert_eq!(plan.summary().physical_release_bytes, 0);
    let Some(Capture::Provider(capture)) = &plan.items()[0].capture else {
        panic!("provider")
    };
    assert_eq!(capture.totals.roots, 2);
    assert_eq!(capture.mutation_scope.root_references.len(), 1);
    assert!(capture.mutation_scope.root_references[0].key == key);
    assert_eq!(
        capture.mutation_scope.root_references[0].owner,
        "owner-selected-root"
    );
    assert_eq!(review.step().root_references, 2);

    let mut duplicate = request.clone();
    duplicate.items[0].action = Action::RetireWith {
        selection: RetirementSelection::ReleaseRootReferences {
            references: vec![key.clone(), key.clone()],
        },
    };
    assert!(matches!(
        root_plan(&duplicate, &snapshot, &spool, directory.path()).await,
        Err(SafeError::DuplicateIdentity)
    ));
    let mut overlapping = request.clone();
    let mut other = overlapping.items[0].clone();
    other.item_id = "other-provider".into();
    overlapping.items.push(other);
    assert!(matches!(
        root_plan(&overlapping, &snapshot, &spool, directory.path()).await,
        Err(SafeError::DuplicateIdentity)
    ));
    let mut unproved = snapshot.clone();
    unproved.native_frame = None;
    assert!(
        root_plan(&request, &unproved, &spool, directory.path())
            .await
            .is_err()
    );
    let mut missing_marker = snapshot.clone();
    missing_marker.root_references[0]
        .reference_tuple
        .remove("logical_released_at");
    assert!(
        root_plan(&request, &missing_marker, &spool, directory.path())
            .await
            .is_err()
    );

    db.execute_unprepared("UPDATE zip_batches SET owner='changed-owner' WHERE id='selected-root'")
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    assert!(matches!(
        review.seal(&tx, &root_limits()).await,
        Err(store::pinning::management::ManagementError::Changed)
    ));
    tx.rollback().await.unwrap();
    let changed = root_inventory(&db, &config, directory.path()).await;
    let changed_capture = changed
        .frozen_review()
        .await
        .unwrap()
        .capture(&request.items[0], RetirementMutationScope::default())
        .await
        .unwrap();
    assert_eq!(
        changed_capture.mutation_scope.root_references[0].owner,
        "changed-owner"
    );
    assert!(changed_capture != **capture);

    db.execute_unprepared("UPDATE zip_root_references SET logical_released_at='2026-10-13T00:00:00Z' WHERE batch_id='selected-root'").await.unwrap();
    let released = root_inventory(&db, &config, directory.path()).await;
    let released_review = released.frozen_review().await.unwrap();
    assert_eq!(released_review.totals().roots, 1);
    assert!(
        !released_review
            .contains_root_responsibility(&key)
            .await
            .unwrap()
    );
    assert!(
        released_review
            .contains_root_responsibility(&shared)
            .await
            .unwrap()
    );
    assert!(
        released_review
            .capture(&request.items[0], RetirementMutationScope::default())
            .await
            .is_err()
    );
    let mut cursor = retirement::ArchiveCursor::default();
    let mut release = Vec::new();
    let mut original_shared = Vec::new();
    while let Some(part) = released.read_part(&mut cursor).await.unwrap() {
        if part.key == key && part.column == "logical_released_at" {
            release.extend(part.bytes);
        } else if part.key == shared {
            original_shared.push(part);
        }
    }
    assert_eq!(release, b"2026-10-13T00:00:00Z");
    let mut cursor = retirement::ArchiveCursor::default();
    let mut frozen_shared = Vec::new();
    while let Some(part) = spool.read_part(&mut cursor).await.unwrap() {
        if part.key == shared {
            frozen_shared.push(part);
        }
    }
    assert!(frozen_shared == original_shared);

    db.execute_unprepared(
        "UPDATE zip_root_references SET verification_receipt=NULL WHERE batch_id='shared-root'",
    )
    .await
    .unwrap();
    let missing_proof = root_inventory(&db, &config, directory.path()).await;
    let missing_review = missing_proof.frozen_review().await.unwrap();
    let mut unproved_request = request.items[0].clone();
    unproved_request.action = Action::RetireWith {
        selection: RetirementSelection::ReleaseRootReferences {
            references: vec![shared.clone()],
        },
    };
    assert!(
        missing_review
            .capture(&unproved_request, RetirementMutationScope::default())
            .await
            .is_err()
    );
    let mut retained = request.clone();
    retained.items[0].action = Action::Retire;
    let default_plan = root_plan(&retained, &snapshot, &missing_proof, directory.path())
        .await
        .unwrap();
    let Some(Capture::Provider(capture)) = &default_plan.items()[0].capture else {
        panic!("provider")
    };
    assert!(capture.mutation_scope.root_references.is_empty());
    assert_eq!(capture.totals.roots, 1);
    assert!(
        missing_review
            .contains_root_responsibility(&shared)
            .await
            .unwrap()
    );
    assert_eq!(missing_review.step().root_references, 1);
}

fn root_limits() -> retirement::RunLimits {
    let mut limits = retirement::RunLimits::default();
    limits.pages = 512;
    limits
}
fn root_reserve(path: &std::path::Path) -> std::fs::File {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).unwrap()
}
async fn root_inventory(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    directory: &std::path::Path,
) -> retirement::InventorySpool {
    let tx = db.begin().await.unwrap();
    let cp = retirement::SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(config.providers[0].route_snapshot()),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let path = directory.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
    let spool = retirement::InventorySpool::create(&path, root_reserve(&path), &cp)
        .await
        .unwrap();
    while spool.scan(db, &root_limits()).await.unwrap() == retirement::Progress::Pending {}
    while spool
        .reduce(&root_configuration(config), &root_limits())
        .await
        .unwrap()
        == retirement::Progress::Pending
    {}
    spool
}
fn root_configuration(config: &ValidatedPinningConfig) -> ConfigurationBinding {
    ConfigurationBinding {
        routes: config
            .providers
            .iter()
            .map(|p| p.route_snapshot())
            .collect(),
        policies: config.policies.clone(),
        limits: config.provider_limits.clone(),
        zip_rules: None,
        source_node: None,
    }
}
async fn selected_root_snapshot(
    db: &DatabaseConnection,
    config: &ValidatedPinningConfig,
    key: &NativeKey,
) -> Snapshot {
    use store::pinning::management::native::{
        self,
        meter::Budget,
        scope::{ScopeSpec, Selector, collect_scope},
    };
    let tx = db.begin().await.unwrap();
    let mut budget = Budget::default();
    let reads = collect_scope(
        &tx,
        &ScopeSpec::new(vec![
            Selector::Exact(key.clone()),
            Selector::Exact(NativeKey {
                table: "zip_root_builds".into(),
                values: key.values[..3].to_vec(),
            }),
            Selector::Exact(NativeKey {
                table: "zip_batches".into(),
                values: key.values[..1].to_vec(),
            }),
        ])
        .unwrap(),
        &mut budget,
    )
    .await
    .unwrap();
    let frame = native::NativeFrame::from_scoped_reads(&[reads], &[], &mut budget).unwrap();
    tx.rollback().await.unwrap();
    let values = key.values.iter().map(String::as_str).collect::<Vec<_>>();
    let reference_tuple = frame.row(&key.table, &values).unwrap().clone();
    let build_tuple = frame.row("zip_root_builds", &values[..3]).unwrap().clone();
    let batch_tuple = frame.row("zip_batches", &values[..1]).unwrap().clone();
    let root = RootReferenceSnapshot {
        key: key.clone(),
        owner: native::value(&batch_tuple, "owner").unwrap().into(),
        verification_recorded: frame
            .original_value(&key.table, &values, "verification_receipt")
            .is_some_and(|v| !v.is_empty()),
        reference_tuple,
        build_tuple,
        batch_tuple,
    };
    let now = Utc::now();
    Snapshot {
        observed_at: now.timestamp(),
        database_now: Some(now),
        configuration: Some(root_configuration(config)),
        root_references: vec![root],
        native_frame: Some(frame),
        ..Default::default()
    }
}
async fn root_plan(
    request: &PlanRequest,
    local: &Snapshot,
    spool: &retirement::InventorySpool,
    directory: &std::path::Path,
) -> Result<DryRunPlan, SafeError> {
    let path = directory.join(format!("{}.sqlite", uuid::Uuid::new_v4()));
    let union = retirement::PlanUnion::create(&path, root_reserve(&path))
        .await
        .unwrap();
    loop {
        if let Some(plan) = dry_run_retirement(
            request,
            local,
            &[spool.frozen_review().await.unwrap()],
            &[],
            &union,
            &root_limits(),
        )
        .await?
        {
            return Ok(plan);
        }
    }
}
