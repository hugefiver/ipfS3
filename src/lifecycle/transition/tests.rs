use std::time::Duration as StdDuration;

use chrono::{Duration, TimeZone, Utc};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    TransactionTrait, sea_query::Expr,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

use super::*;

mod exhaustion;
use crate::{
    config::{LifecycleWorkerConfig, ValidatedLifecycleConfig},
    error::AppError,
    import::SupersedeReason,
    kubo::{KuboClient, LocalResidencyVerificationReceipt, tier_copy::stream_copy_verified},
    lifecycle::{
        config::canonical_json,
        evaluator::schedule_claimed_scan_page,
        model::{
            CanonicalFilter, CanonicalLifecycleConfiguration, CanonicalLifecycleRule,
            CanonicalRuleSelector, CanonicalTag, ClaimedLifecycleAction, ClaimedLifecycleScan,
            CurrentTransition, LifecycleRuleStatus, NoncurrentExpiration, NoncurrentTransition,
        },
    },
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::{ContentMode, ObjectTag},
    },
    residency::{KuboTier, PhysicalVerification, StorageClass, VersionResidencyIdentity},
    store::{
        self, Store,
        entities::{
            bucket_lifecycle_config, lifecycle_action, lifecycle_transition, object,
            object_version, physical_residency, pin_lease, pin_lease_target, residency_reference,
        },
        import::ownership::{
            StandardMutationGuard, admit_content_mutation, try_admit_lifecycle_mutation,
        },
        lifecycle_action::claim_due,
        lifecycle_config::{delete_configuration, put_configuration},
        lifecycle_transition::{
            TransitionPrepareResult, TransitionPublishResult, prepare,
            publish as publish_transition, record_copy, record_verified,
        },
        object_version::{BucketVersioningState, PublicVersionId, VersionSelector},
        pinning::{
            publication::{
                PinTargetSpec, PublicationObject, PublicationRequest,
                delete_version_with_leases_guarded, publish_object, publish_standard_object,
            },
            tags::{list_object_tags, replace_object_tags},
        },
    },
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
const HOT_NODE: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
const COLD_NODE: &str = "QmPChd2hVbrJ6i1a7aDPgS6G9X4YuJ5sS7cGqf6ZkK3vYq";
const REPLACEMENT_NODE: &str = "QmNLei78zC5X8YwFoVrQwMYM4Q7jKuCDNRQuk3KfHk7zXq";
const BUCKET: &str = "bucket";
const KEY: &str = "primary";
const RULE_ID: &str = "transition";

#[tokio::test]
async fn ownership_dependency_wait_survives_transition_failure_budget() {
    let mut fixture = fixture(Selector::All).await;
    let id = fixture.claim.action.id.clone();
    let writer = admit_content_mutation(
        fixture.db(),
        BUCKET,
        KEY,
        None,
        SupersedeReason::PutObject,
        Utc::now(),
    )
    .await
    .unwrap();
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    let mut config = worker_config();
    config.max_attempts = 2;
    for _ in 0..5 {
        execute(
            fixture.db(),
            &fixture.claim,
            &harness.clients(),
            &config,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let action = stored_action(&fixture).await;
        assert_eq!(action.state, "pending");
        assert_eq!(
            action.attempts, 0,
            "ownership dependency must not spend transition failure attempts"
        );
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::NextAttemptAt,
                Expr::value(Utc::now() - Duration::seconds(1)),
            )
            .filter(lifecycle_action::Column::Id.eq(&id))
            .exec(fixture.db())
            .await
            .unwrap();
        fixture.claim = store::lifecycle_action::claim_due_with_max_attempts(
            fixture.db(),
            "wait-worker",
            Duration::seconds(30),
            2,
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(fixture.claim.action.id, id);
    }
    store::import::ownership::release_standard_mutation(fixture.db(), &writer)
        .await
        .unwrap();
    execute(
        fixture.db(),
        &fixture.claim,
        &harness.clients(),
        &config,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(
        residency(&fixture, &fixture.version.id).await.storage_class,
        StorageClass::StandardIa
    );
}

#[tokio::test]
async fn expired_claim_cannot_install_an_ownership_admission() {
    let fixture = fixture(Selector::All).await;
    lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
        )
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .exec(fixture.db())
        .await
        .unwrap();
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();
    let destination = store::entities::import_destination::Entity::find_by_id((
        BUCKET.to_owned(),
        KEY.to_owned(),
    ))
    .one(fixture.db())
    .await
    .unwrap();
    assert!(destination.is_none_or(|destination| destination.mutation_id.is_none()));
    assert!(
        harness
            .hot_server
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        harness
            .cold_server
            .received_requests()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn replay_repairs_a_missing_cold_copy_before_publication() {
    let fixture = fixture(Selector::All).await;
    let guard = admit(&fixture).await;
    let prepared = prepare_checkpoint(&fixture, &guard).await;
    let txn = fixture.db().begin().await.unwrap();
    record_copy(&txn, &fixture.claim, &prepared)
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    // The durable copy checkpoint survived, but the destination lost its pin.
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(500))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&harness.cold_server)
        .await;
    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();
    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(
        residency(&fixture, &fixture.version.id).await.storage_class,
        StorageClass::StandardIa
    );
    assert!(
        harness
            .cold_server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/dag/import")
    );
}

#[derive(Clone, Copy)]
enum Selector {
    All,
    Tagged,
}

struct Fixture {
    _directory: tempfile::TempDir,
    store: Store,
    configuration: CanonicalLifecycleConfiguration,
    version: object_version::Model,
    claim: ClaimedLifecycleAction,
}

impl Fixture {
    fn db(&self) -> &DatabaseConnection {
        self.store.db()
    }
}

#[derive(Clone, Copy)]
enum ImportBehavior {
    Success,
    Delayed,
    DelayedLong,
    Failure,
    WrongRoot,
}

struct TierHarness {
    hot_server: MockServer,
    cold_server: MockServer,
    hot: KuboClient,
    cold: KuboClient,
}

impl TierHarness {
    async fn new(import: ImportBehavior, hot_node: &str) -> Self {
        let hot_server = MockServer::start().await;
        let cold_server = MockServer::start().await;
        mount_identity(&hot_server, hot_node).await;
        mount_identity(&cold_server, COLD_NODE).await;
        mount_local_verification(&hot_server).await;
        mount_local_verification(&cold_server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .and(query_param("arg", CID))
            .and(query_param("offline", "true"))
            .and(query_param("progress", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"faithful-car"))
            .mount(&hot_server)
            .await;
        let response = match import {
            ImportBehavior::Failure => ResponseTemplate::new(503),
            ImportBehavior::WrongRoot => {
                ResponseTemplate::new(200).set_body_string(successful_import(OTHER_CID))
            }
            ImportBehavior::Delayed => ResponseTemplate::new(200)
                .set_body_string(successful_import(CID))
                .set_delay(StdDuration::from_millis(300)),
            ImportBehavior::DelayedLong => ResponseTemplate::new(200)
                .set_body_string(successful_import(CID))
                .set_delay(StdDuration::from_millis(1_600)),
            ImportBehavior::Success => {
                ResponseTemplate::new(200).set_body_string(successful_import(CID))
            }
        };
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .and(query_param("pin-roots", "true"))
            .and(query_param("stats", "true"))
            .respond_with(response)
            .mount(&cold_server)
            .await;
        let hot = KuboClient::new(hot_server.uri());
        let cold = KuboClient::new(cold_server.uri());
        Self {
            hot_server,
            cold_server,
            hot,
            cold,
        }
    }

    fn clients(&self) -> crate::residency::router::TierClients<'_> {
        crate::residency::router::TierClients {
            hot: &self.hot,
            cold: Some(&self.cold),
        }
    }
}

fn worker_config() -> ValidatedLifecycleConfig {
    worker_config_with_action_lease(30)
}

fn worker_config_with_action_lease(action_lease_secs: u64) -> ValidatedLifecycleConfig {
    LifecycleWorkerConfig {
        poll_interval_ms: 1,
        scan_page_size: 10,
        scan_lease_secs: 30,
        action_lease_secs,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 60,
    }
    .validate()
    .unwrap()
}

fn transition_configuration(selector: Selector) -> CanonicalLifecycleConfiguration {
    CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some(RULE_ID.to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: match selector {
                Selector::All => CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::All,
                },
                Selector::Tagged => CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::Tag {
                        tag: CanonicalTag {
                            key: "archive".to_owned(),
                            value: "true".to_owned(),
                        },
                    },
                },
            },
            expiration: None,
            noncurrent_version_expiration: None,
            transition: Some(CurrentTransition::Date {
                utc_midnight: Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap(),
            }),
            noncurrent_version_transition: None,
            abort_incomplete_multipart_upload: None,
        }],
    }
}

fn noncurrent_transition_configuration() -> CanonicalLifecycleConfiguration {
    CanonicalLifecycleConfiguration {
        schema_version: 1,
        rules: vec![CanonicalLifecycleRule {
            id: Some(RULE_ID.to_owned()),
            status: LifecycleRuleStatus::Enabled,
            selector: CanonicalRuleSelector::Modern {
                filter: CanonicalFilter::All,
            },
            expiration: None,
            noncurrent_version_expiration: None,
            transition: None,
            noncurrent_version_transition: Some(NoncurrentTransition {
                noncurrent_days: 1,
                newer_noncurrent_versions: None,
            }),
            abort_incomplete_multipart_upload: None,
        }],
    }
}

async fn sqlite_store() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().unwrap();
    let database_url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("transition.sqlite")
            .display()
            .to_string()
            .replace('\\', "/")
    );
    let db = store::connect_database(&database_url).await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, BUCKET, None).await.unwrap();
    store::bucket::set_versioning_state(&db, BUCKET, BucketVersioningState::Enabled)
        .await
        .unwrap();
    (directory, Store::new(db))
}

async fn fixture(selector: Selector) -> Fixture {
    let (directory, store) = sqlite_store().await;
    let db = store.db();

    let configuration = transition_configuration(selector);
    let json = canonical_json(&configuration).unwrap();
    let revision = put_configuration(db, BUCKET, &json).await.unwrap();
    assert_eq!(revision, 1);
    let tags = match selector {
        Selector::All => Vec::new(),
        Selector::Tagged => vec![ObjectTag::new("archive", "true")],
    };
    let version = publish(db, "primary-object", KEY, tags).await;
    verify_hot(db, &version).await;

    let now = store::database_clock::database_now(db).await.unwrap();
    let scan = ClaimedLifecycleScan {
        bucket: BUCKET.to_owned(),
        config_revision: revision,
        canonical_json: json,
        cursor: None,
        lease_epoch: 1,
        database_now: now,
        lease_until: now + Duration::seconds(30),
    };
    schedule_claimed_scan_page(db, &scan, 10).await.unwrap();
    let mut claims = claim_due(db, "transition-worker", Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1, "E1 must schedule exactly one due action");
    let claim = claims.pop().unwrap();
    assert_eq!(claim.action.action_kind, "transition_current");

    Fixture {
        _directory: directory,
        store,
        configuration,
        version,
        claim,
    }
}

async fn noncurrent_fixture() -> (Fixture, object_version::Model) {
    let (directory, store) = sqlite_store().await;
    let db = store.db();
    let configuration = noncurrent_transition_configuration();
    let json = canonical_json(&configuration).unwrap();
    let revision = put_configuration(db, BUCKET, &json).await.unwrap();
    let old = publish(db, "old-object", KEY, Vec::new()).await;
    let current = publish(db, "current-object", KEY, Vec::new()).await;
    let became_noncurrent_at =
        store::database_clock::database_now(db).await.unwrap() - Duration::days(3);
    let updated = object_version::Entity::update_many()
        .col_expr(
            object_version::Column::BecameNoncurrentAt,
            Expr::value(Some(became_noncurrent_at)),
        )
        .filter(object_version::Column::Id.eq(&old.id))
        .exec(db)
        .await
        .unwrap();
    assert_eq!(updated.rows_affected, 1);
    let old = object_version::Entity::find_by_id(&old.id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    verify_hot(db, &old).await;
    verify_hot(db, &current).await;

    let now = store::database_clock::database_now(db).await.unwrap();
    schedule_claimed_scan_page(
        db,
        &ClaimedLifecycleScan {
            bucket: BUCKET.to_owned(),
            config_revision: revision,
            canonical_json: json,
            cursor: None,
            lease_epoch: 1,
            database_now: now,
            lease_until: now + Duration::seconds(30),
        },
        10,
    )
    .await
    .unwrap();
    let mut claims = claim_due(db, "noncurrent-worker", Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().unwrap();
    assert_eq!(claim.action.action_kind, "transition_noncurrent");
    assert_eq!(
        claim.action.target_version_row_id.as_deref(),
        Some(old.id.as_str())
    );
    (
        Fixture {
            _directory: directory,
            store,
            configuration,
            version: old,
            claim,
        },
        current,
    )
}

async fn current_with_predecessor_fixture() -> (Fixture, object_version::Model) {
    let (directory, store) = sqlite_store().await;
    let db = store.db();
    let configuration = transition_configuration(Selector::All);
    let json = canonical_json(&configuration).unwrap();
    let revision = put_configuration(db, BUCKET, &json).await.unwrap();
    let predecessor = publish_with_manual_lease(db, "leased-predecessor", KEY).await;
    let target = publish(db, "delete-target", KEY, Vec::new()).await;
    verify_hot(db, &predecessor).await;
    verify_hot(db, &target).await;

    let now = store::database_clock::database_now(db).await.unwrap();
    schedule_claimed_scan_page(
        db,
        &ClaimedLifecycleScan {
            bucket: BUCKET.to_owned(),
            config_revision: revision,
            canonical_json: json,
            cursor: None,
            lease_epoch: 1,
            database_now: now,
            lease_until: now + Duration::seconds(30),
        },
        10,
    )
    .await
    .unwrap();
    let mut claims = claim_due(db, "delete-race-worker", Duration::seconds(30), 10)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    let claim = claims.pop().unwrap();
    assert_eq!(
        claim.action.target_version_row_id.as_deref(),
        Some(target.id.as_str())
    );
    (
        Fixture {
            _directory: directory,
            store,
            configuration,
            version: target,
            claim,
        },
        predecessor,
    )
}

async fn publish(
    db: &DatabaseConnection,
    object_id: &str,
    key: &str,
    tags: Vec<ObjectTag>,
) -> object_version::Model {
    publish_request(
        db,
        publication_request(object_id, key, CID, tags, Vec::new()),
        &Default::default(),
    )
    .await
}

fn publication_request(
    object_id: &str,
    key: &str,
    cid: &str,
    tags: Vec<ObjectTag>,
    leases: Vec<LeaseIntent>,
) -> PublicationRequest {
    let value = PublicationObject::from_put(
        object_id.to_owned(),
        BUCKET,
        key,
        cid.to_owned(),
        12,
        Some("application/octet-stream".to_owned()),
        Some(serde_json::json!({"immutable": true})),
        false,
        None,
        None,
        Utc::now(),
    );
    PublicationRequest {
        object_target: PinTargetSpec {
            cid: cid.to_owned(),
            logical_size: 12,
        },
        object: value,
        tags: tags.clone(),
        policy: PublicationPolicy { tags, leases },
    }
}

async fn publish_request(
    db: &DatabaseConnection,
    request: PublicationRequest,
    limits: &ProviderLimitMap,
) -> object_version::Model {
    let object_id = request.object.id.clone();
    publish_object(db, request, limits).await.unwrap();
    object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .one(db)
        .await
        .unwrap()
        .unwrap()
}

fn manual_limits() -> ProviderLimitMap {
    ProviderLimitMap::from([(
        "manual-provider".to_owned(),
        ProviderLimits {
            priority: 1,
            max_bytes: 1_000,
            max_pins: 100,
            enabled: true,
        },
    )])
}

async fn publish_with_manual_lease(
    db: &DatabaseConnection,
    object_id: &str,
    key: &str,
) -> object_version::Model {
    publish_request(
        db,
        publication_request(
            object_id,
            key,
            CID,
            Vec::new(),
            vec![LeaseIntent {
                source: LeaseSource::Manual,
                policy_id: "policy:manual-fixture".to_owned(),
                provider_mode: ProviderMode::One,
                providers: vec!["manual-provider".to_owned()],
                content_mode: ContentMode::Object,
                duration: LeaseDuration::parse("1h").unwrap(),
            }],
        ),
        &manual_limits(),
    )
    .await
}

async fn guarded_publish_replacement(
    fixture: &Fixture,
    reason: SupersedeReason,
    object_id: &str,
) -> object_version::Model {
    let now = store::database_clock::database_now(fixture.db())
        .await
        .unwrap();
    let guard = admit_content_mutation(fixture.db(), BUCKET, KEY, None, reason, now)
        .await
        .unwrap();
    let request = publication_request(object_id, KEY, OTHER_CID, Vec::new(), Vec::new());
    publish_standard_object(fixture.db(), request, guard, &Default::default())
        .await
        .unwrap();
    object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(object_id))
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap()
}

async fn verify_hot(db: &DatabaseConnection, version: &object_version::Model) {
    let txn = db.begin().await.unwrap();
    store::residency::attach_hot_in_transaction(
        &txn,
        &VersionResidencyIdentity::new(
            version.id.clone(),
            version.object_id.as_deref().unwrap(),
            CID,
        ),
        &PhysicalVerification::verified(HOT_NODE, "verified-hot-fixture"),
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
}

async fn mount_identity(server: &MockServer, node: &str) {
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .and(query_param("peerid-base", "b58mh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID": node})))
        .mount(server)
        .await;
}

async fn mount_local_verification(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .and(query_param("arg", CID))
        .and(query_param("type", "recursive"))
        .and(query_param("offline", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#)),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{CID}")))
        .and(query_param("with-local", "true"))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            r#"{{"Hash":"{CID}","WithLocality":true,"Local":true}}"#
        )))
        .mount(server)
        .await;
}

fn successful_import(cid: &str) -> String {
    format!(
        "{{\"Root\":{{\"Cid\":{{\"/\":\"{cid}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":12}}}}\n"
    )
}

async fn stored_action(fixture: &Fixture) -> lifecycle_action::Model {
    lifecycle_action::Entity::find_by_id(&fixture.claim.action.id)
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap()
}

async fn stored_saga(fixture: &Fixture) -> lifecycle_transition::Model {
    lifecycle_transition::Entity::find()
        .filter(lifecycle_transition::Column::ActionId.eq(&fixture.claim.action.id))
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap()
}

async fn residency(
    fixture: &Fixture,
    version_id: &str,
) -> crate::residency::ResolvedVersionResidency {
    store::residency::resolve_version_residency(fixture.db(), version_id)
        .await
        .unwrap()
}

async fn admit(fixture: &Fixture) -> StandardMutationGuard {
    try_admit_lifecycle_mutation(
        fixture.db(),
        BUCKET,
        KEY,
        &fixture.claim.action.id,
        fixture.claim.claim_epoch,
        store::database_clock::database_now(fixture.db())
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn prepare_checkpoint(
    fixture: &Fixture,
    guard: &StandardMutationGuard,
) -> lifecycle_transition::Model {
    let txn = fixture.db().begin().await.unwrap();
    let TransitionPrepareResult::Prepared(saga) =
        prepare(&txn, &fixture.claim, guard, HOT_NODE, COLD_NODE)
            .await
            .unwrap()
    else {
        panic!("verified E1 fixture must prepare")
    };
    txn.commit().await.unwrap();
    assert_eq!(saga.checkpoint, "prepare");
    *saga
}

async fn copy_receipt(harness: &TierHarness) -> LocalResidencyVerificationReceipt {
    stream_copy_verified(
        &harness.hot,
        &harness.cold,
        CID,
        Some(HOT_NODE),
        Some(COLD_NODE),
        &CancellationToken::new(),
    )
    .await
    .unwrap()
}

async fn record_copy_checkpoint(
    fixture: &Fixture,
    saga: &lifecycle_transition::Model,
) -> lifecycle_transition::Model {
    let txn = fixture.db().begin().await.unwrap();
    let copied = record_copy(&txn, &fixture.claim, saga)
        .await
        .unwrap()
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(copied.checkpoint, "copy");
    copied
}

async fn expire_and_reclaim(fixture: &Fixture) -> ClaimedLifecycleAction {
    expire_and_reclaim_with_lease(fixture, "replacement-worker", Duration::seconds(30)).await
}

async fn expire_and_reclaim_with_lease(
    fixture: &Fixture,
    worker: &str,
    lease: Duration,
) -> ClaimedLifecycleAction {
    let past = store::database_clock::database_now(fixture.db())
        .await
        .unwrap()
        - Duration::seconds(1);
    let updated = lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            Expr::value(Some(past)),
        )
        .col_expr(lifecycle_action::Column::NextAttemptAt, Expr::value(past))
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(updated.rows_affected, 1);
    let reclaimed = claim_due(fixture.db(), worker, lease, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reclaimed.claim_epoch, fixture.claim.claim_epoch + 1);
    reclaimed
}

async fn execute_with(
    db: &DatabaseConnection,
    claim: &ClaimedLifecycleAction,
    harness: &TierHarness,
) -> Result<(), AppError> {
    execute(
        db,
        claim,
        &harness.clients(),
        &worker_config(),
        &CancellationToken::new(),
    )
    .await
}

async fn assert_unpublished(fixture: &Fixture) {
    let current = residency(fixture, &fixture.version.id).await;
    assert_eq!(current.primary.tier, KuboTier::Hot);
    assert_eq!(current.storage_class, StorageClass::Standard);
    assert_eq!(current.identity.cid, CID);
    assert_ne!(stored_action(fixture).await.state, "succeeded");
}

async fn wait_for_import(server: &MockServer) {
    tokio::time::timeout(StdDuration::from_secs(5), async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.url.path() == "/api/v0/dag/import")
            {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(5)).await;
        }
    })
    .await
    .expect("transition copy must reach cold import");
}

fn spawn_execute(
    fixture: &Fixture,
    harness: &TierHarness,
    config: ValidatedLifecycleConfig,
) -> tokio::task::JoinHandle<Result<(), AppError>> {
    let db = fixture.db().clone();
    let claim = fixture.claim.clone();
    let hot = harness.hot.clone();
    let cold = harness.cold.clone();
    tokio::spawn(async move {
        execute(
            &db,
            &claim,
            &crate::residency::router::TierClients {
                hot: &hot,
                cold: Some(&cold),
            },
            &config,
            &CancellationToken::new(),
        )
        .await
    })
}

#[tokio::test]
async fn successful_transition_changes_only_target_residency_and_preserves_shared_cid() {
    let fixture = fixture(Selector::Tagged).await;
    let shared = publish(fixture.db(), "shared-object", "shared", Vec::new()).await;
    verify_hot(fixture.db(), &shared).await;
    let before_object = object::Entity::find_by_id("primary-object")
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap();
    let before_version = fixture.version.clone();
    let before_tags = list_object_tags(fixture.db(), "primary-object")
        .await
        .unwrap();
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;

    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    let target = residency(&fixture, &fixture.version.id).await;
    assert_eq!(target.primary.tier, KuboTier::Cold);
    assert_eq!(target.storage_class, StorageClass::StandardIa);
    assert_eq!(target.identity.cid, CID);
    let shared_residency = residency(&fixture, &shared.id).await;
    assert_eq!(shared_residency.primary.tier, KuboTier::Hot);
    assert_eq!(shared_residency.storage_class, StorageClass::Standard);
    assert_eq!(
        object::Entity::find_by_id("primary-object")
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        before_object
    );
    assert_eq!(
        object_version::Entity::find_by_id(&fixture.version.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        before_version
    );
    assert_eq!(
        list_object_tags(fixture.db(), "primary-object")
            .await
            .unwrap(),
        before_tags
    );
    assert!(
        physical_residency::Entity::find_by_id(("hot".to_owned(), CID.to_owned()))
            .one(fixture.db())
            .await
            .unwrap()
            .is_some(),
        "shared hot physical state must survive target cleanup"
    );
    assert_eq!(
        residency_reference::Entity::find()
            .filter(residency_reference::Column::OwnerId.eq(&shared.id))
            .filter(residency_reference::Column::Tier.eq("hot"))
            .count(fixture.db())
            .await
            .unwrap(),
        1
    );
    let cold_requests = harness.cold_server.received_requests().await.unwrap();
    let import = cold_requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/dag/import")
        .expect("successful saga must stream a CAR to cold");
    assert!(String::from_utf8_lossy(&import.body).contains("faithful-car"));
    for server in [&harness.hot_server, &harness.cold_server] {
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm")
        );
    }
}

#[tokio::test]
async fn successful_noncurrent_transition_preserves_current_version_and_immutable_rows() {
    let (fixture, current) = noncurrent_fixture().await;
    let before_object = object::Entity::find_by_id("old-object")
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap();
    let before_old = fixture.version.clone();
    let before_current = current.clone();
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;

    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    let transitioned = residency(&fixture, &fixture.version.id).await;
    assert_eq!(transitioned.primary.tier, KuboTier::Cold);
    assert_eq!(transitioned.storage_class, StorageClass::StandardIa);
    let retained_current = residency(&fixture, &current.id).await;
    assert_eq!(retained_current.primary.tier, KuboTier::Hot);
    assert_eq!(retained_current.storage_class, StorageClass::Standard);
    assert_eq!(
        object::Entity::find_by_id("old-object")
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        before_object
    );
    assert_eq!(
        object_version::Entity::find_by_id(&fixture.version.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        before_old
    );
    assert_eq!(
        object_version::Entity::find_by_id(&current.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        before_current
    );
}

#[tokio::test]
async fn missing_cold_retries_without_publishing() {
    let fixture = fixture(Selector::All).await;
    let hot = KuboClient::new("http://127.0.0.1:1".to_owned());
    let clients = crate::residency::router::TierClients {
        hot: &hot,
        cold: None,
    };

    execute(
        fixture.db(),
        &fixture.claim,
        &clients,
        &worker_config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_unpublished(&fixture).await;
    let action = stored_action(&fixture).await;
    assert_eq!(action.state, "pending");
    assert!(action.failure_class.is_some());
}

#[tokio::test]
async fn import_root_and_node_failures_never_publish() {
    for (behavior, hot_node, failure) in [
        (ImportBehavior::Failure, HOT_NODE, "import"),
        (ImportBehavior::WrongRoot, HOT_NODE, "root"),
        (ImportBehavior::Success, REPLACEMENT_NODE, "node"),
    ] {
        let fixture = fixture(Selector::All).await;
        let harness = TierHarness::new(behavior, hot_node).await;

        execute_with(fixture.db(), &fixture.claim, &harness)
            .await
            .unwrap();

        assert_unpublished(&fixture).await;
        let action = stored_action(&fixture).await;
        assert_eq!(action.state, "pending", "failure={failure}");
        assert!(action.failure_class.is_some(), "failure={failure}");
        if failure != "node" {
            assert!(
                harness
                    .cold_server
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .any(|request| request.url.path() == "/api/v0/dag/import"),
                "failure={failure} must traverse real saga copy I/O"
            );
        }
    }
}

#[tokio::test]
async fn exhausted_action_waits_for_hot_verification_without_spending_an_attempt() {
    let mut fixture = fixture(Selector::All).await;
    let config = worker_config();
    let now = store::database_clock::database_now(fixture.db())
        .await
        .unwrap();
    let physical = physical_residency::Entity::update_many()
        .col_expr(
            physical_residency::Column::VerificationState,
            Expr::value("pending"),
        )
        .col_expr(
            physical_residency::Column::NodeIdentity,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            physical_residency::Column::VerificationReceipt,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            physical_residency::Column::VerifiedAt,
            Expr::value(Option::<chrono::DateTime<Utc>>::None),
        )
        .col_expr(physical_residency::Column::UpdatedAt, Expr::value(now))
        .filter(physical_residency::Column::Tier.eq("hot"))
        .filter(physical_residency::Column::Cid.eq(CID))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(physical.rows_affected, 1);
    let action = lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::Attempts,
            Expr::value(config.max_attempts),
        )
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(action.rows_affected, 1);
    fixture.claim.action.attempts = config.max_attempts;
    let hot = KuboClient::new("http://127.0.0.1:1".to_owned());

    execute(
        fixture.db(),
        &fixture.claim,
        &crate::residency::router::TierClients {
            hot: &hot,
            cold: None,
        },
        &config,
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    let waiting = stored_action(&fixture).await;
    assert_eq!(waiting.state, "pending");
    assert_eq!(waiting.attempts, config.max_attempts - 1);
    assert_eq!(
        waiting.config_revision,
        fixture.claim.action.config_revision
    );
    assert!(waiting.failure_class.is_none());
    verify_hot(fixture.db(), &fixture.version).await;
    let past = store::database_clock::database_now(fixture.db())
        .await
        .unwrap()
        - Duration::seconds(1);
    lifecycle_action::Entity::update_many()
        .col_expr(lifecycle_action::Column::NextAttemptAt, Expr::value(past))
        .filter(lifecycle_action::Column::Id.eq(&fixture.claim.action.id))
        .exec(fixture.db())
        .await
        .unwrap();
    fixture.claim = claim_due(fixture.db(), "verified-hot-worker", config.action_lease, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(fixture.claim.action.attempts, config.max_attempts);
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;

    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(
        residency(&fixture, &fixture.version.id).await.primary.tier,
        KuboTier::Cold
    );
}

#[tokio::test]
async fn disabled_or_deleted_configuration_cancels_before_tier_io() {
    for delete in [false, true] {
        let fixture = fixture(Selector::All).await;
        if delete {
            delete_configuration(fixture.db(), BUCKET).await.unwrap();
        } else {
            let mut disabled = fixture.configuration.clone();
            disabled.rules[0].status = LifecycleRuleStatus::Disabled;
            put_configuration(fixture.db(), BUCKET, &canonical_json(&disabled).unwrap())
                .await
                .unwrap();
        }
        let hot = KuboClient::new("http://127.0.0.1:1".to_owned());

        execute(
            fixture.db(),
            &fixture.claim,
            &crate::residency::router::TierClients {
                hot: &hot,
                cold: None,
            },
            &worker_config(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(stored_action(&fixture).await.state, "cancelled");
        assert_unpublished(&fixture).await;
        assert!(
            lifecycle_transition::Entity::find()
                .filter(lifecycle_transition::Column::ActionId.eq(&fixture.claim.action.id))
                .one(fixture.db())
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn higher_priority_permanent_expiration_cancels_noncurrent_transition() {
    let (fixture, _current) = noncurrent_fixture().await;
    let mut replacement = fixture.configuration.clone();
    replacement.rules[0].noncurrent_version_expiration = Some(NoncurrentExpiration {
        noncurrent_days: 1,
        newer_noncurrent_versions: None,
    });
    let updated = bucket_lifecycle_config::Entity::update_many()
        .col_expr(
            bucket_lifecycle_config::Column::CanonicalJson,
            Expr::value(Some(canonical_json(&replacement).unwrap())),
        )
        .filter(bucket_lifecycle_config::Column::Bucket.eq(BUCKET))
        .exec(fixture.db())
        .await
        .unwrap();
    assert_eq!(updated.rows_affected, 1);
    let hot = KuboClient::new("http://127.0.0.1:1".to_owned());

    execute(
        fixture.db(),
        &fixture.claim,
        &crate::residency::router::TierClients {
            hot: &hot,
            cold: None,
        },
        &worker_config(),
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "cancelled");
    assert_unpublished(&fixture).await;
    assert!(
        object_version::Entity::find_by_id(&fixture.version.id)
            .one(fixture.db())
            .await
            .unwrap()
            .is_some(),
        "the winning expiration is executed by its own action"
    );
}

#[derive(Clone, Copy, Debug)]
enum EligibilityMutation {
    ReplaceConfiguration,
    RemoveTag,
}

#[tokio::test]
async fn eligibility_mutation_during_copy_cancels_before_publication() {
    for mutation in [
        EligibilityMutation::ReplaceConfiguration,
        EligibilityMutation::RemoveTag,
    ] {
        let fixture = fixture(Selector::Tagged).await;
        let harness = TierHarness::new(ImportBehavior::Delayed, HOT_NODE).await;
        let db = fixture.db().clone();
        let claim = fixture.claim.clone();
        let hot = harness.hot.clone();
        let cold = harness.cold.clone();
        let task = tokio::spawn(async move {
            let clients = crate::residency::router::TierClients {
                hot: &hot,
                cold: Some(&cold),
            };
            execute(
                &db,
                &claim,
                &clients,
                &worker_config(),
                &CancellationToken::new(),
            )
            .await
        });
        wait_for_import(&harness.cold_server).await;

        match mutation {
            EligibilityMutation::ReplaceConfiguration => {
                let replacement = canonical_json(&fixture.configuration).unwrap();
                assert_eq!(
                    put_configuration(fixture.db(), BUCKET, &replacement)
                        .await
                        .unwrap(),
                    2
                );
            }
            EligibilityMutation::RemoveTag => {
                replace_object_tags(fixture.db(), "primary-object", &[])
                    .await
                    .unwrap();
            }
        }

        task.await.unwrap().unwrap();
        assert_unpublished(&fixture).await;
        assert_eq!(
            stored_action(&fixture).await.state,
            "cancelled",
            "{mutation:?}"
        );
        let saga = stored_saga(&fixture).await;
        assert_eq!(saga.settlement_kind.as_deref(), Some("cancelled"));
        assert!(saga.publication_receipt.is_none());
        assert_eq!(
            residency_reference::Entity::find()
                .filter(residency_reference::Column::OwnerKind.eq("transition"))
                .filter(residency_reference::Column::OwnerId.eq(&saga.id))
                .count(fixture.db())
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn guarded_put_and_copy_during_io_supersede_without_publishing_old_transition() {
    for (reason, replacement_id) in [
        (SupersedeReason::PutObject, "put-replacement"),
        (SupersedeReason::CopyObject, "copy-replacement"),
    ] {
        let fixture = fixture(Selector::All).await;
        let harness = TierHarness::new(ImportBehavior::Delayed, HOT_NODE).await;
        let task = spawn_execute(&fixture, &harness, worker_config());
        wait_for_import(&harness.cold_server).await;

        let replacement = guarded_publish_replacement(&fixture, reason, replacement_id).await;
        task.await.unwrap().unwrap();

        let replacement = object_version::Entity::find_by_id(&replacement.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap();
        assert!(replacement.is_latest, "producer={reason}");
        assert_eq!(replacement.object_id.as_deref(), Some(replacement_id));
        let old = object_version::Entity::find_by_id(&fixture.version.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap();
        assert!(!old.is_latest, "producer={reason}");
        let old_residency = residency(&fixture, &old.id).await;
        assert_eq!(old_residency.primary.tier, KuboTier::Hot);
        assert_eq!(old_residency.storage_class, StorageClass::Standard);
        let saga = stored_saga(&fixture).await;
        assert!(saga.publication_receipt.is_none(), "producer={reason}");
    }
}

#[tokio::test]
async fn guarded_exact_delete_promotes_shared_leased_version_without_revival() {
    let (fixture, predecessor) = current_with_predecessor_fixture().await;
    let lease_before = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq("leased-predecessor"))
        .filter(pin_lease::Column::Source.eq("manual"))
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap();
    let targets_before = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
        .all(fixture.db())
        .await
        .unwrap();
    let harness = TierHarness::new(ImportBehavior::Delayed, HOT_NODE).await;
    let task = spawn_execute(&fixture, &harness, worker_config());
    wait_for_import(&harness.cold_server).await;

    let now = store::database_clock::database_now(fixture.db())
        .await
        .unwrap();
    let guard = admit_content_mutation(
        fixture.db(),
        BUCKET,
        KEY,
        None,
        SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();
    let public_version = PublicVersionId::parse_s3(
        fixture
            .version
            .version_id
            .as_deref()
            .expect("enabled target has a public version"),
    )
    .unwrap();
    delete_version_with_leases_guarded(
        fixture.db(),
        BUCKET,
        KEY,
        VersionSelector::Exact(public_version),
        guard,
        now,
    )
    .await
    .unwrap();
    task.await.unwrap().unwrap();

    assert!(
        object_version::Entity::find_by_id(&fixture.version.id)
            .one(fixture.db())
            .await
            .unwrap()
            .is_none(),
        "the stale transition must not revive its deleted immutable target"
    );
    let promoted = object_version::Entity::find_by_id(&predecessor.id)
        .one(fixture.db())
        .await
        .unwrap()
        .unwrap();
    assert!(promoted.is_latest);
    assert_eq!(promoted.object_id.as_deref(), Some("leased-predecessor"));
    let promoted_residency = residency(&fixture, &promoted.id).await;
    assert_eq!(promoted_residency.primary.tier, KuboTier::Hot);
    assert_eq!(promoted_residency.storage_class, StorageClass::Standard);
    assert_eq!(
        pin_lease::Entity::find_by_id(&lease_before.id)
            .one(fixture.db())
            .await
            .unwrap()
            .unwrap(),
        lease_before
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(&lease_before.id))
            .all(fixture.db())
            .await
            .unwrap(),
        targets_before
    );
    let saga = stored_saga(&fixture).await;
    assert!(saga.publication_receipt.is_none());
}

#[tokio::test]
async fn heartbeat_renews_a_claim_during_copy_longer_than_its_lease() {
    let mut fixture = fixture(Selector::All).await;
    fixture.claim =
        expire_and_reclaim_with_lease(&fixture, "short-lease-worker", Duration::seconds(1)).await;
    let original_lease = fixture.claim.action.lease_until.unwrap();
    let harness = TierHarness::new(ImportBehavior::DelayedLong, HOT_NODE).await;
    let db = fixture.db().clone();
    let claim = fixture.claim.clone();
    let hot = harness.hot.clone();
    let cold = harness.cold.clone();
    let started = tokio::time::Instant::now();
    let task = tokio::spawn(async move {
        execute(
            &db,
            &claim,
            &crate::residency::router::TierClients {
                hot: &hot,
                cold: Some(&cold),
            },
            &worker_config_with_action_lease(1),
            &CancellationToken::new(),
        )
        .await
    });
    wait_for_import(&harness.cold_server).await;
    tokio::time::timeout(StdDuration::from_secs(2), async {
        loop {
            let lease = lifecycle_action::Entity::find_by_id(&fixture.claim.action.id)
                .one(fixture.db())
                .await
                .unwrap()
                .unwrap()
                .lease_until
                .unwrap();
            if lease > original_lease {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
    })
    .await
    .expect("heartbeat must advance the database-owned lease during copy");
    task.await.unwrap().unwrap();

    assert!(started.elapsed() > StdDuration::from_secs(1));
    assert_eq!(stored_action(&fixture).await.state, "succeeded");
}

#[tokio::test]
async fn lost_epoch_fences_old_copy_writeback_and_replacement_claim_completes() {
    let mut fixture = fixture(Selector::All).await;
    let old_harness = TierHarness::new(ImportBehavior::Delayed, HOT_NODE).await;
    let db = fixture.db().clone();
    let old_claim = fixture.claim.clone();
    let hot = old_harness.hot.clone();
    let cold = old_harness.cold.clone();
    let old_task = tokio::spawn(async move {
        execute(
            &db,
            &old_claim,
            &crate::residency::router::TierClients {
                hot: &hot,
                cold: Some(&cold),
            },
            &worker_config(),
            &CancellationToken::new(),
        )
        .await
    });
    wait_for_import(&old_harness.cold_server).await;
    let replacement =
        expire_and_reclaim_with_lease(&fixture, "takeover-worker", Duration::seconds(30)).await;
    old_task.await.unwrap().unwrap();

    let after_old = stored_action(&fixture).await;
    assert_eq!(after_old.state, "claimed");
    assert_eq!(after_old.claimed_by.as_deref(), Some("takeover-worker"));
    assert_eq!(after_old.claim_epoch, replacement.claim_epoch);
    assert_eq!(stored_saga(&fixture).await.checkpoint, "prepare");
    assert_unpublished(&fixture).await;

    fixture.claim = replacement;
    let replacement_harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    execute_with(fixture.db(), &fixture.claim, &replacement_harness)
        .await
        .unwrap();
    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(stored_saga(&fixture).await.checkpoint, "cleanup");
}

#[tokio::test]
async fn reclaimed_prepare_checkpoint_replays_io_and_completes() {
    let mut fixture = fixture(Selector::All).await;
    let guard = admit(&fixture).await;
    prepare_checkpoint(&fixture, &guard).await;
    fixture.claim = expire_and_reclaim(&fixture).await;
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;

    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(stored_saga(&fixture).await.checkpoint, "cleanup");
    assert_eq!(
        residency(&fixture, &fixture.version.id).await.primary.tier,
        KuboTier::Cold
    );
}

#[tokio::test]
async fn reclaimed_copy_checkpoint_reverifies_and_completes() {
    let mut fixture = fixture(Selector::All).await;
    let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
    let guard = admit(&fixture).await;
    let prepared = prepare_checkpoint(&fixture, &guard).await;
    copy_receipt(&harness).await;
    record_copy_checkpoint(&fixture, &prepared).await;
    let before_requests = harness.cold_server.received_requests().await.unwrap();
    let imports_before_reclaim = before_requests
        .iter()
        .filter(|request| request.url.path() == "/api/v0/dag/import")
        .count();
    let verifications_before_reclaim = before_requests
        .iter()
        .filter(|request| request.url.path() == "/api/v0/files/stat")
        .count();
    fixture.claim = expire_and_reclaim(&fixture).await;

    execute_with(fixture.db(), &fixture.claim, &harness)
        .await
        .unwrap();

    assert_eq!(stored_action(&fixture).await.state, "succeeded");
    assert_eq!(stored_saga(&fixture).await.checkpoint, "cleanup");
    let requests = harness.cold_server.received_requests().await.unwrap();
    let imports = requests
        .iter()
        .filter(|request| request.url.path() == "/api/v0/dag/import")
        .count();
    let verifications = requests
        .iter()
        .filter(|request| request.url.path() == "/api/v0/files/stat")
        .count();
    assert_eq!(
        imports, imports_before_reclaim,
        "a durable copy checkpoint may reuse completed CAR transfer"
    );
    assert!(
        verifications > verifications_before_reclaim,
        "the new claim must independently reverify cold local completeness"
    );
    let saga = stored_saga(&fixture).await;
    let durable: serde_json::Value =
        serde_json::from_str(saga.verification_receipt.as_deref().unwrap()).unwrap();
    assert_eq!(durable["claim_epoch"], fixture.claim.claim_epoch);
}

#[tokio::test]
async fn publication_receipt_drives_cleanup_after_configuration_deletion() {
    for legacy_worker in [false, true] {
        let mut fixture = fixture(Selector::All).await;
        let harness = TierHarness::new(ImportBehavior::Success, HOT_NODE).await;
        let guard = admit(&fixture).await;
        let prepared = prepare_checkpoint(&fixture, &guard).await;
        let receipt = copy_receipt(&harness).await;
        let copied = record_copy_checkpoint(&fixture, &prepared).await;
        let txn = fixture.db().begin().await.unwrap();
        record_verified(&txn, &fixture.claim, &copied, &receipt)
            .await
            .unwrap()
            .unwrap();
        txn.commit().await.unwrap();
        let txn = fixture.db().begin().await.unwrap();
        let TransitionPublishResult::Published(published) =
            publish_transition(&txn, &fixture.claim, &guard, &receipt)
                .await
                .unwrap()
        else {
            panic!("verified transition must publish")
        };
        txn.commit().await.unwrap();
        assert_eq!(published.checkpoint, "publish");
        assert!(published.publication_receipt.is_some());
        delete_configuration(fixture.db(), BUCKET).await.unwrap();
        fixture.claim = expire_and_reclaim(&fixture).await;

        if legacy_worker {
            let mut config = worker_config();
            config.max_attempts = 1;
            crate::lifecycle::worker::execute_claimed_action(
                fixture.db(),
                &fixture.claim,
                &config,
                None,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        } else {
            execute_with(fixture.db(), &fixture.claim, &harness)
                .await
                .unwrap();
        }

        let saga = stored_saga(&fixture).await;
        assert_eq!(saga.checkpoint, "cleanup");
        assert_eq!(saga.settlement_kind.as_deref(), Some("cleanup_complete"));
        assert!(saga.publication_receipt.is_some());
        assert_eq!(stored_action(&fixture).await.state, "succeeded");
        assert_eq!(
            residency(&fixture, &fixture.version.id).await.primary.tier,
            KuboTier::Cold
        );
        assert_eq!(
            residency_reference::Entity::find()
                .filter(residency_reference::Column::OwnerKind.eq("transition"))
                .filter(residency_reference::Column::OwnerId.eq(&saga.id))
                .count(fixture.db())
                .await
                .unwrap(),
            0
        );
    }
}
