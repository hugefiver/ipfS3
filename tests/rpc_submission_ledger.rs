//! Stage 5 evidence/quota contract. PostgreSQL is explicitly opt-in, never a
//! silent pass without an endpoint. Worker/wiremock fixtures below use no account.
use std::collections::BTreeMap;

use chrono::{Duration as ChronoDuration, Utc};
use ipfs_s3_gateway::{
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        identity::{CleanupMode, Ownership, ProviderIdentity, RemoteResourceType},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        provider::{
            ObservedResource, ObservedResourceStatus, ProviderError, ProviderErrorClass, RemotePin,
            RemotePinStatus, SubmitEffect, SubmitObservation, canonical_resource_cid,
        },
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{pin_invocation_route, pin_job, pin_resource_history, remote_pin},
        pinning::{
            jobs::{self, ClaimedPinJob},
            ledger::{self, submission, submission_entity},
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, Statement, TransactionTrait,
    sea_query::Expr,
};
use sea_orm_migration::{MigrationTrait, SchemaManager};

use ipfs_s3_gateway::store::migrations::m20261004_000001_rpc_submission_ledger as stage5_migration;

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

#[test]
fn typed_observation_contract_roundtrips_snake_case() {
    for api in ["rpc", "kubo", "filebase-rpc"] {
        assert!(submission::is_rpc_api(api));
    }
    for api in ["psa", "pinata_v3", "noop", "unknown"] {
        assert!(!submission::is_rpc_api(api));
    }
    for (effect, text) in [
        (SubmitEffect::NotSubmitted, "not_submitted"),
        (SubmitEffect::Observed, "observed"),
        (SubmitEffect::Unknown, "unknown"),
    ] {
        let value = serde_json::to_value(effect).unwrap();
        assert_eq!(value, text);
        assert_eq!(
            serde_json::from_value::<SubmitEffect>(value).unwrap(),
            effect
        );
    }
    for (status, text) in [
        (ObservedResourceStatus::Reported, "reported"),
        (ObservedResourceStatus::Stored, "stored"),
        (ObservedResourceStatus::PinAccepted, "pin_accepted"),
        (ObservedResourceStatus::PinError, "pin_error"),
        (
            ObservedResourceStatus::RecursiveVerified,
            "recursive_verified",
        ),
    ] {
        let resource = ObservedResource {
            resource_type: RemoteResourceType::RpcPin,
            cid: CID.into(),
            request_id: "historical-receipt".into(),
            status,
            ownership: Ownership::Unknown,
        };
        let value = serde_json::to_value(&resource).unwrap();
        assert_eq!(value["resource_type"], "rpc_pin");
        assert_eq!(value["request_id"], "historical-receipt");
        assert_eq!(value["status"], text);
        assert_eq!(value["ownership"], "unknown");
        assert_eq!(
            serde_json::from_value::<ObservedResource>(value).unwrap(),
            resource
        );
    }
}

fn identity() -> ProviderIdentity {
    ProviderIdentity {
        provider_id: "stage5".into(),
        display_name: "Stage 5".into(),
        backend: "filebase".into(),
        scope: "isolated-bucket".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: Some("env:STAGE5_TOKEN".into()),
        api_profile: "filebase-rpc".into(),
        strategy: "upload".into(),
        retired: false,
        cleanup: CleanupMode::Managed,
    }
}

fn limits(provider: &str) -> ProviderLimitMap {
    BTreeMap::from([(
        provider.into(),
        ProviderLimits {
            priority: 1,
            max_bytes: 100,
            max_pins: 1,
            enabled: true,
        },
    )])
}

async fn assert_evidence_migration(db: &sea_orm::DatabaseConnection) {
    let manager = SchemaManager::new(db);
    assert!(
        manager.has_table("pin_submit_observations").await.unwrap(),
        "production migrations must create RPC submission evidence"
    );
}

async fn database() -> (Store, String) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    assert_evidence_migration(&db).await;
    let provider = identity().allocation_key();
    ledger::register_route(&db, &provider, &identity())
        .await
        .unwrap();
    (Store::new(db), provider)
}

/// Production publication creates the object, lease, target and pending Submit;
/// production claiming supplies the actual owner and previous queue state.
async fn invocation(store: &Store, provider: &str, cid: &str) -> (ClaimedPinJob, String) {
    if !store::bucket::exists(store.db(), "stage5").await.unwrap() {
        store::bucket::create(store.db(), "stage5", None)
            .await
            .unwrap();
    }
    let object_id = uuid::Uuid::new_v4().to_string();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                object_id.clone(),
                "stage5",
                object_id.as_str(),
                cid.into(),
                100,
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
                leases: vec![LeaseIntent {
                    source: LeaseSource::Automatic,
                    policy_id: "rpc-ledger-contract".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider.into()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 100,
            },
        },
        &limits(provider),
    )
    .await
    .unwrap();
    let now = Utc::now();
    let claimed = jobs::claim_due_jobs(store.db(), now, ChronoDuration::minutes(1), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.model.operation, "submit");
    assert_eq!(claimed.previous_state, "pending");
    assert!(!claimed.reclaimed);
    assert_eq!(claimed.object_id.as_deref(), Some(object_id.as_str()));
    jobs::record_submit_invocation(store.db(), &claimed, "rpc", "upload", now)
        .await
        .unwrap();
    let id = submission::begin(store.db(), &claimed, now)
        .await
        .unwrap()
        .unwrap();
    (claimed, id)
}

fn receipt(cid: &str, status: ObservedResourceStatus) -> ObservedResource {
    ObservedResource {
        resource_type: RemoteResourceType::RpcPin,
        cid: cid.into(),
        request_id: "rpc:opaque-receipt".into(),
        status,
        ownership: Ownership::Unknown,
    }
}

fn failure(effect: SubmitEffect, resources: Vec<ObservedResource>) -> SubmitObservation {
    SubmitObservation {
        result: Err(ProviderError {
            class: ProviderErrorClass::Authentication,
            message: "redacted rejection".into(),
            retry_after: None,
        }),
        resources,
        effect,
    }
}

fn matched(cid: &str) -> SubmitObservation {
    SubmitObservation {
        result: Ok(RemotePin {
            request_id: "rpc:opaque-receipt".into(),
            cid: cid.into(),
            status: RemotePinStatus::Pinned,
            raw_status: "pinned".into(),
            failure_reason: None,
        }),
        resources: vec![receipt(cid, ObservedResourceStatus::RecursiveVerified)],
        effect: SubmitEffect::Observed,
    }
}

#[tokio::test]
async fn typed_postdispatch_rejection_is_debt_not_not_created_and_survives_terminal_job() {
    for resources in [
        vec![],
        vec![receipt(OTHER, ObservedResourceStatus::Reported)],
        vec![receipt(OTHER, ObservedResourceStatus::PinAccepted)],
    ] {
        let (store, provider) = database().await;
        let (claim, id) = invocation(&store, &provider, CID).await;
        assert!(
            submission::record(
                store.db(),
                &claim,
                &id,
                &failure(SubmitEffect::Unknown, resources.clone()),
                Utc::now()
            )
            .await
            .unwrap()
        );
        let row = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.effect, "unknown");
        assert!(row.needs_attention);
        let safe: serde_json::Value =
            serde_json::from_str(row.safe_error.as_deref().unwrap()).unwrap();
        assert_eq!(safe["effect"], "unknown");
        let actual: Vec<submission::ResourceEvidence> =
            serde_json::from_str(&row.resources).unwrap();
        assert_eq!(actual.len(), resources.len());
        assert_eq!(
            quota::reserve_unique(
                store.db(),
                &provider,
                OTHER,
                100,
                &limits(&provider),
                Utc::now()
            )
            .await
            .unwrap(),
            quota::ReservationOutcome::QuotaBlocked
        );
        pin_job::Entity::delete_by_id(claim.model.id.clone())
            .exec(store.db())
            .await
            .unwrap();
        assert_eq!(
            submission::observations(store.db(), &provider)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(submission::has_debt(store.db(), &provider).await.unwrap());
        // A fresh job identity cannot turn unresolved effects into a new POST.
        let now = Utc::now();
        let replacement_id = uuid::Uuid::new_v4().to_string();
        jobs::enqueue_job(
            store.db(),
            jobs::NewPinJob::Target(jobs::TargetPinJob {
                id: replacement_id.clone(),
                operation: jobs::TargetJobOperation::Submit,
                provider: claim.model.provider.clone(),
                cid: claim.model.cid.clone(),
                lease_id: claim.model.lease_id.clone().unwrap(),
                target_id: claim.model.target_id.clone().unwrap(),
                expected_generation: claim.model.expected_generation.unwrap(),
                next_attempt_at: now,
            }),
        )
        .await
        .unwrap();
        let replacement = jobs::claim_due_jobs(store.db(), now, ChronoDuration::minutes(1), 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(replacement.previous_state, "pending");
        assert_eq!(replacement.model.id, replacement_id);
        assert_eq!(replacement.object_id, claim.object_id);
        let captured = pin_invocation_route::Entity::find_by_id(replacement.model.id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(captured.route, row.route);
        assert_eq!(captured.remote_epoch, row.remote_epoch);
        assert!(
            submission::begin(store.db(), &replacement, Utc::now())
                .await
                .is_err()
        );
        assert_eq!(
            quota::confirmed_release(store.db(), &provider, CID, row.remote_epoch, Utc::now())
                .await
                .unwrap(),
            quota::ConfirmedReleaseOutcome::Stale
        );
        let usage = quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    }
}

#[tokio::test]
async fn matching_equivalent_single_root_reuses_original_reservation_without_delete_authority() {
    let (store, provider) = database().await;
    let canonical = canonical_resource_cid(V0).unwrap();
    let (claim, id) = invocation(&store, &provider, &canonical).await;
    assert!(
        submission::record(store.db(), &claim, &id, &matched(V0), Utc::now())
            .await
            .unwrap()
    );
    let row = submission::latest(store.db(), &claim.model.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.outcome, "matched");
    assert!(!row.needs_attention);
    let roots: Vec<submission::ResourceEvidence> = serde_json::from_str(&row.resources).unwrap();
    assert_eq!(roots[0].resource.cid, V0); // actual protocol evidence is not rewritten
    assert_eq!(roots[0].key.as_ref().unwrap().resource_id, canonical);
    assert_eq!(
        quota::reserve_unique(
            store.db(),
            &provider,
            &canonical,
            100,
            &limits(&provider),
            Utc::now()
        )
        .await
        .unwrap(),
        quota::ReservationOutcome::Reused
    );
    let usage = quota::read_usage(store.db(), &provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    assert!(
        !ledger::cleanup_allowed(store.db(), &provider, &canonical)
            .await
            .unwrap()
    );
    assert_eq!(
        ledger::allocation_cid(store.db(), &provider, V0)
            .await
            .unwrap(),
        canonical
    );
    // A caller that omitted canonical target planning cannot double charge v0.
    let parsed = cid::Cid::try_from(V0).unwrap();
    let v1 = cid::Cid::new_v1(parsed.codec(), *parsed.hash());
    for base in [
        cid::multibase::Base::Base58Btc,
        cid::multibase::Base::Base32Upper,
    ] {
        let alternate = v1.to_string_of_base(base).unwrap();
        assert_eq!(
            ledger::allocation_cid(store.db(), &provider, &alternate)
                .await
                .unwrap(),
            canonical
        );
        assert!(ipfs_s3_gateway::pinning::provider::cids_equivalent(V0, &alternate).unwrap());
    }
    let different_codec = cid::Cid::new_v1(0x55, *v1.hash());
    assert!(
        !ipfs_s3_gateway::pinning::provider::cids_equivalent(V0, &different_codec.to_string())
            .unwrap()
    );
    assert!(
        quota::reserve_unique(
            store.db(),
            &provider,
            V0,
            100,
            &limits(&provider),
            Utc::now()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn mismatch_multiple_roots_and_pin_receipts_never_become_matched_or_managed() {
    for observation in [
        failure(
            SubmitEffect::Observed,
            vec![receipt(OTHER, ObservedResourceStatus::PinAccepted)],
        ),
        failure(
            SubmitEffect::Observed,
            vec![
                receipt(CID, ObservedResourceStatus::PinAccepted),
                receipt(OTHER, ObservedResourceStatus::PinAccepted),
            ],
        ),
        failure(
            SubmitEffect::Unknown,
            vec![receipt(CID, ObservedResourceStatus::Reported)],
        ),
    ] {
        let (store, provider) = database().await;
        let (claim, id) = invocation(&store, &provider, CID).await;
        submission::record(store.db(), &claim, &id, &observation, Utc::now())
            .await
            .unwrap();
        let row = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(row.outcome, "matched");
        assert!(row.needs_attention);
        assert_eq!(
            serde_json::from_str::<Vec<submission::ResourceEvidence>>(&row.resources)
                .unwrap()
                .len(),
            observation.resources.len()
        );
        assert!(
            !ledger::cleanup_allowed(store.db(), &provider, CID)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn stale_claim_and_archived_epoch_keep_actual_evidence_but_reject_projection() {
    for steal_claim in [false, true] {
        let (store, provider) = database().await;
        let (claim, id) = invocation(&store, &provider, CID).await;
        let captured = pin_invocation_route::Entity::find_by_id(claim.model.id.clone())
            .one(store.db())
            .await
            .unwrap()
            .unwrap();
        if steal_claim {
            pin_job::Entity::update_many()
                .col_expr(
                    pin_job::Column::LockedUntil,
                    Expr::value(Some(Utc::now() + ChronoDuration::hours(1))),
                )
                .filter(pin_job::Column::Id.eq(&claim.model.id))
                .exec(store.db())
                .await
                .unwrap();
        } else {
            pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
                provider: Set(provider.clone()),
                cid: Set(CID.into()),
                epoch: Set(captured.remote_epoch),
                ledger: Set("{}".into()),
            })
            .exec(store.db())
            .await
            .unwrap();
            remote_pin::Entity::update_many()
                .col_expr(
                    remote_pin::Column::Epoch,
                    Expr::value(captured.remote_epoch + 1),
                )
                .filter(remote_pin::Column::Provider.eq(&provider))
                .exec(store.db())
                .await
                .unwrap();
        }
        assert!(
            !submission::record(store.db(), &claim, &id, &matched(CID), Utc::now())
                .await
                .unwrap()
        );
        let row = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.route, captured.route);
        assert_eq!(row.remote_epoch, captured.remote_epoch);
        assert!(row.needs_attention);
        assert_eq!(
            serde_json::from_str::<Vec<submission::ResourceEvidence>>(&row.resources).unwrap()[0]
                .resource
                .cid,
            CID
        );
        assert_eq!(
            remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
                .one(store.db())
                .await
                .unwrap()
                .unwrap()
                .status,
            "reserved"
        );
    }
}

#[tokio::test]
async fn observation_write_failure_rolls_back_to_durable_unknown_footprint_and_can_be_repaired() {
    let (store, provider) = database().await;
    let (claim, id) = invocation(&store, &provider, CID).await;
    store.db().execute(Statement::from_string(store.db().get_database_backend(),
        "CREATE TRIGGER fail_rpc_observation BEFORE UPDATE OF resources ON pin_submit_observations BEGIN SELECT RAISE(ABORT, 'injected ledger failure'); END".to_owned())).await.unwrap();
    let txn = store.db().begin().await.unwrap();
    assert!(
        submission::record(&txn, &claim, &id, &matched(CID), Utc::now())
            .await
            .is_err()
    );
    txn.rollback().await.unwrap();
    let before = submission::latest(store.db(), &claim.model.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.outcome, "in_flight");
    assert!(before.needs_attention);
    assert!(before.observed_at.is_none());
    store
        .db()
        .execute(Statement::from_string(
            store.db().get_database_backend(),
            "DROP TRIGGER fail_rpc_observation".to_owned(),
        ))
        .await
        .unwrap();
    assert!(
        submission::record(store.db(), &claim, &id, &matched(CID), Utc::now())
            .await
            .unwrap()
    );
    assert_eq!(
        submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap()
            .outcome,
        "matched"
    );
}

#[tokio::test]
async fn explicit_not_submitted_has_no_debt_but_reported_resources_override_contradiction() {
    for roots in [
        vec![],
        vec![receipt(OTHER, ObservedResourceStatus::Reported)],
    ] {
        let (store, provider) = database().await;
        let (claim, id) = invocation(&store, &provider, CID).await;
        submission::record(
            store.db(),
            &claim,
            &id,
            &failure(SubmitEffect::NotSubmitted, roots.clone()),
            Utc::now(),
        )
        .await
        .unwrap();
        let row = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.needs_attention, !roots.is_empty());
        assert_eq!(row.outcome == "not_submitted", roots.is_empty());
    }
}

#[tokio::test]
async fn historical_rpc_unknown_and_old_class_based_not_created_are_not_creation_proof() {
    for effect in ["unknown", "not_created", "confirmed", "retained"] {
        let (store, provider) = database().await;
        let (claim, id) = invocation(&store, &provider, CID).await;
        // Model pre-append-migration data: an old class-derived effect exists,
        // but the newly introduced typed observation table has no evidence.
        submission_entity::Entity::delete_by_id(id)
            .exec(store.db())
            .await
            .unwrap();
        ledger::mark_effect(store.db(), &provider, CID, effect)
            .await
            .unwrap();
        assert!(submission::has_debt(store.db(), &provider).await.unwrap());
        for cid in [CID, OTHER] {
            assert_eq!(
                quota::reserve_unique(
                    store.db(),
                    &provider,
                    cid,
                    100,
                    &limits(&provider),
                    Utc::now()
                )
                .await
                .unwrap(),
                quota::ReservationOutcome::QuotaBlocked
            );
        }
        assert!(
            jobs::record_submit_invocation(store.db(), &claim, "rpc", "upload", Utc::now())
                .await
                .is_err()
        );
        assert!(
            !ledger::cleanup_allowed(store.db(), &provider, CID)
                .await
                .unwrap()
        );
        assert_eq!(
            ledger::get(store.db(), &provider, CID)
                .await
                .unwrap()
                .unwrap()
                .effect,
            effect
        );
    }
}

#[tokio::test]
async fn unsafe_down_is_rejected_and_old_rpc_alias_never_inherits_current_scope_or_ownership() {
    let (store, provider) = database().await;
    assert!(
        stage5_migration::Migration
            .down(&SchemaManager::new(store.db()))
            .await
            .is_err()
    );
    let canonical = canonical_resource_cid(V0).unwrap();
    quota::reserve_unique(
        store.db(),
        &provider,
        &canonical,
        100,
        &limits(&provider),
        Utc::now(),
    )
    .await
    .unwrap();
    remote_pin::Entity::update_many()
        .col_expr(remote_pin::Column::Cid, Expr::value(V0))
        .filter(remote_pin::Column::Provider.eq(&provider))
        .exec(store.db())
        .await
        .unwrap();
    assert!(
        ledger::allocation_cid(store.db(), &provider, V0)
            .await
            .is_err()
    );
    let old = remote_pin::Entity::find_by_id((provider.clone(), V0.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.cid, V0);
    assert_eq!(old.status, "reserved");
}

#[tokio::test]
#[ignore = "opt-in: requires IPFS_S3_TEST_POSTGRES_URL to a dedicated authorized PostgreSQL database"]
async fn postgres_rpc_submission_evidence_is_real_not_silently_skipped() {
    use futures_util::FutureExt;
    use sea_orm::{ConnectOptions, Database};
    let endpoint = std::env::var("IPFS_S3_TEST_POSTGRES_URL")
        .expect("opt-in PostgreSQL test requires IPFS_S3_TEST_POSTGRES_URL; NOT RUN is not PASS");
    let connection = store::connect_database(&endpoint).await.unwrap();
    let schema = format!("stage5_rpc_{}", uuid::Uuid::new_v4().simple());
    assert!(
        schema.starts_with("stage5_rpc_")
            && schema
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    connection
        .execute(Statement::from_string(
            connection.get_database_backend(),
            format!("CREATE SCHEMA {schema}"),
        ))
        .await
        .unwrap();
    let result = std::panic::AssertUnwindSafe(async {
        let mut options = ConnectOptions::new(endpoint);
        options.min_connections(1).max_connections(1);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared(&format!("SET search_path TO {schema}"))
            .await
            .unwrap();
        db.execute_unprepared("SET statement_timeout TO '15s'")
            .await
            .unwrap();
        store::run_migrations(&db).await.unwrap();
        assert_evidence_migration(&db).await;
        let provider = identity().allocation_key();
        ledger::register_route(&db, &provider, &identity())
            .await
            .unwrap();
        let store = Store::new(db.clone());
        let (claim, id) = invocation(&store, &provider, CID).await;
        let txn = store.db().begin().await.unwrap();
        assert!(
            submission::record(&txn, &claim, &id, &matched(CID), Utc::now())
                .await
                .unwrap()
        );
        txn.rollback().await.unwrap();
        let footprint = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(footprint.outcome, "in_flight");
        assert!(footprint.needs_attention);
        assert!(
            submission::record(
                store.db(),
                &claim,
                &id,
                &failure(
                    SubmitEffect::Unknown,
                    vec![receipt(OTHER, ObservedResourceStatus::Reported)]
                ),
                Utc::now()
            )
            .await
            .unwrap()
        );
        let row = submission::latest(store.db(), &claim.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.effect, "unknown");
        assert!(row.needs_attention);
        let generous = BTreeMap::from([(
            provider.clone(),
            ProviderLimits {
                priority: 1,
                max_bytes: 1000,
                max_pins: 10,
                enabled: true,
            },
        )]);
        assert_eq!(
            quota::reserve_unique(store.db(), &provider, OTHER, 100, &generous, Utc::now())
                .await
                .unwrap(),
            quota::ReservationOutcome::QuotaBlocked
        );
        pin_job::Entity::delete_by_id(claim.model.id.clone())
            .exec(store.db())
            .await
            .unwrap();
        assert_eq!(
            submission::observations(store.db(), &provider)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            quota::confirmed_release(store.db(), &provider, CID, row.remote_epoch, Utc::now())
                .await
                .unwrap(),
            quota::ConfirmedReleaseOutcome::Stale
        );
        assert!(
            stage5_migration::Migration
                .down(&SchemaManager::new(store.db()))
                .await
                .is_err()
        );
        drop(store);
        db.close().await.unwrap();
    })
    .catch_unwind()
    .await;
    // Fail-closed exact UUID schema cleanup, including failed assertions.
    connection
        .execute(Statement::from_string(
            connection.get_database_backend(),
            format!("DROP SCHEMA {schema} CASCADE"),
        ))
        .await
        .unwrap();
    let cleanup = connection
        .query_one(Statement::from_sql_and_values(
            connection.get_database_backend(),
            "SELECT COUNT(*) AS remaining FROM pg_namespace WHERE nspname = $1",
            [schema.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cleanup.try_get::<i64>("", "remaining").unwrap(),
        0,
        "isolated Stage5 PostgreSQL schema was not cleaned: {schema}"
    );
    println!("Stage5 PostgreSQL schema cleanup verified: {schema}");
    connection.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
