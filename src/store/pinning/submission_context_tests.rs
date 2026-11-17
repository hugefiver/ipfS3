use super::*;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderLimitMap, ProviderLimits, ProviderMode},
        identity::{CleanupMode, ProviderIdentity},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self,
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};
use sea_orm::{ActiveModelTrait, DatabaseConnection, IntoActiveModel};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
const FOREIGN: &str = "foreign-resource";

struct Fixture {
    db: DatabaseConnection,
    provider: String,
    job: String,
    target: String,
    lease: String,
    receipt: String,
    claim: ClaimedPinJob,
}

async fn publish(db: &DatabaseConnection, provider: &str, id: &str, cid: &str) {
    let limits = ProviderLimitMap::from([(
        provider.to_owned(),
        ProviderLimits {
            priority: 1,
            max_bytes: 10,
            max_pins: 1,
            enabled: true,
        },
    )]);
    publication::publish_object(
        db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.into(),
                "bucket",
                id,
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
                leases: vec![LeaseIntent {
                    source: LeaseSource::Automatic,
                    policy_id: "context".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider.to_owned()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 10,
            },
        },
        &limits,
    )
    .await
    .unwrap();
}

fn context_identity() -> ProviderIdentity {
    ProviderIdentity {
        provider_id: "context-rpc".into(),
        display_name: "context-rpc".into(),
        backend: "kubo".into(),
        scope: "isolated-node".into(),
        storage_domain: "context-domain".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "kubo".into(),
        strategy: "cid".into(),
        retired: false,
        cleanup: CleanupMode::Managed,
    }
}

fn foreign_identity() -> ProviderIdentity {
    ProviderIdentity {
        provider_id: "context-foreign".into(),
        display_name: "context-foreign".into(),
        scope: "foreign-node".into(),
        storage_domain: "foreign-domain".into(),
        ..context_identity()
    }
}

async fn database() -> (DatabaseConnection, String) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let identity = context_identity();
    let provider = identity.allocation_key();
    crate::store::pinning::ledger::register_route(&db, &provider, &identity)
        .await
        .unwrap();
    (db, provider)
}

async fn fixture() -> Fixture {
    let (db, provider) = database().await;
    publish(&db, &provider, "held-object", CID).await;
    let claim = jobs::claim_due_jobs(&db, Utc::now(), chrono::Duration::seconds(30), 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(!claim.reclaimed);
    let job = claim.model.id.clone();
    let target = claim.model.target_id.clone().unwrap();
    let lease = claim.model.lease_id.clone().unwrap();
    let tx = db.begin().await.unwrap();
    assert_eq!(
        jobs::prepare_submit_call(&tx, &claim, Utc::now())
            .await
            .unwrap(),
        jobs::SubmitCallDecision::ReadyToCall
    );
    jobs::record_submit_invocation(&tx, &claim, "kubo", "cid", Utc::now())
        .await
        .unwrap();
    let receipt = begin(&tx, &claim, Utc::now()).await.unwrap().unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        admission_barrier(&db, &provider).await.unwrap(),
        AdmissionBarrier::Temporary
    );
    Fixture {
        db,
        provider,
        job,
        target,
        lease,
        receipt,
        claim,
    }
}

async fn migrated_settled_alias(
    released: bool,
    rolled_back: bool,
) -> (DatabaseConnection, String, String, String) {
    let fixture = fixture().await;
    record(
        &fixture.db,
        &fixture.claim,
        &fixture.receipt,
        &SubmitObservation {
            effect: SubmitEffect::NotSubmitted,
            resources: vec![],
            result: Err(ProviderError {
                class: crate::pinning::provider::ProviderErrorClass::Protocol,
                message: "not dispatched".into(),
                retry_after: None,
            }),
        },
        Utc::now(),
    )
    .await
    .unwrap();
    jobs::complete_job(
        &fixture.db,
        &fixture.job,
        fixture.claim.model.locked_until.unwrap(),
        Utc::now(),
    )
    .await
    .unwrap();
    let source = "legacy-context";
    let alias_job = "legacy-context-job";
    let alias_receipt = "legacy-context-receipt";
    let foreign_source = "foreign-context";
    let foreign_job_id = "foreign-context-job";
    let foreign_receipt = "foreign-context-receipt";
    let route = context_identity().route_snapshot();
    let foreign = foreign_identity();
    let foreign_provider = foreign.allocation_key();
    let foreign_route = foreign.route_snapshot();
    crate::store::pinning::ledger::register_route(&fixture.db, &foreign_provider, &foreign)
        .await
        .unwrap();
    let current_job = pin_job::Entity::find_by_id(&fixture.job)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let mut job = current_job.clone().into_active_model();
    job.id = Set(alias_job.into());
    job.provider = Set(source.into());
    job.cid = Set(OTHER.into());
    job.state = Set("done".into());
    job.locked_until = Set(None);
    job.insert(&fixture.db).await.unwrap();
    let mut foreign_job = current_job.into_active_model();
    foreign_job.id = Set(foreign_job_id.into());
    foreign_job.provider = Set(foreign_source.into());
    foreign_job.cid = Set(FOREIGN.into());
    foreign_job.state = Set("done".into());
    foreign_job.locked_until = Set(None);
    foreign_job.insert(&fixture.db).await.unwrap();
    let current_ledger =
        remote_pin_ledger::Entity::find_by_id((fixture.provider.clone(), CID.into()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
    let mut ledger = current_ledger.clone().into_active_model();
    ledger.provider = Set(source.into());
    ledger.cid = Set(OTHER.into());
    ledger.insert(&fixture.db).await.unwrap();
    let mut foreign_ledger = current_ledger.into_active_model();
    foreign_ledger.provider = Set(foreign_source.into());
    foreign_ledger.cid = Set(FOREIGN.into());
    foreign_ledger.route = Set(Some(serde_json::to_string(&foreign_route).unwrap()));
    foreign_ledger.insert(&fixture.db).await.unwrap();
    let current_evidence = evidence::Entity::find_by_id(&fixture.receipt)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let remote_epoch = current_evidence.remote_epoch;
    let mut observation = current_evidence.clone().into_active_model();
    observation.id = Set(alias_receipt.into());
    observation.job_id = Set(alias_job.into());
    observation.provider = Set(source.into());
    observation.expected_cid = Set(OTHER.into());
    observation.insert(&fixture.db).await.unwrap();
    let mut foreign_observation = current_evidence.into_active_model();
    foreign_observation.id = Set(foreign_receipt.into());
    foreign_observation.job_id = Set(foreign_job_id.into());
    foreign_observation.provider = Set(foreign_source.into());
    foreign_observation.expected_cid = Set(FOREIGN.into());
    foreign_observation.route = Set(serde_json::to_string(&foreign_route).unwrap());
    foreign_observation.insert(&fixture.db).await.unwrap();
    if released {
        pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
            provider: Set(source.into()),
            cid: Set(OTHER.into()),
            epoch: Set(remote_epoch),
            ledger: Set("{}".into()),
        })
        .exec(&fixture.db)
        .await
        .unwrap();
    }

    let mut bindings = vec![
        crate::store::pinning::legacy_migration::Binding {
            table: "remote_pin_ledger".into(),
            key: [
                ("provider".into(), source.into()),
                ("cid".into(), OTHER.into()),
            ]
            .into(),
            provider: source.into(),
            pointer: None,
            historical_route: Some(route.clone()),
        },
        crate::store::pinning::legacy_migration::Binding {
            table: "pin_jobs".into(),
            key: [("id".into(), alias_job.into())].into(),
            provider: source.into(),
            pointer: None,
            historical_route: None,
        },
        crate::store::pinning::legacy_migration::Binding {
            table: "pin_submit_observations".into(),
            key: [("id".into(), alias_receipt.into())].into(),
            provider: source.into(),
            pointer: None,
            historical_route: Some(route.clone()),
        },
    ];
    if released {
        bindings.push(crate::store::pinning::legacy_migration::Binding {
            table: "pin_resource_history".into(),
            key: [
                ("provider".into(), source.into()),
                ("cid".into(), OTHER.into()),
                ("epoch".into(), remote_epoch.to_string()),
            ]
            .into(),
            provider: source.into(),
            pointer: None,
            historical_route: None,
        });
    }
    let foreign_bindings = vec![
        crate::store::pinning::legacy_migration::Binding {
            table: "remote_pin_ledger".into(),
            key: [
                ("provider".into(), foreign_source.into()),
                ("cid".into(), FOREIGN.into()),
            ]
            .into(),
            provider: foreign_source.into(),
            pointer: None,
            historical_route: Some(foreign_route.clone()),
        },
        crate::store::pinning::legacy_migration::Binding {
            table: "pin_jobs".into(),
            key: [("id".into(), foreign_job_id.into())].into(),
            provider: foreign_source.into(),
            pointer: None,
            historical_route: None,
        },
        crate::store::pinning::legacy_migration::Binding {
            table: "pin_submit_observations".into(),
            key: [("id".into(), foreign_receipt.into())].into(),
            provider: foreign_source.into(),
            pointer: None,
            historical_route: Some(foreign_route.clone()),
        },
    ];
    let declaration = crate::store::pinning::legacy_migration::Declaration {
        version: 1,
        database_scope: crate::store::pinning::management::native::scope::binding(&fixture.db)
            .await
            .unwrap()
            .0,
        maintenance: crate::store::pinning::legacy_migration::Maintenance {
            all_writers_stopped: true,
            all_workers_drained: true,
        },
        ttl_seconds: 300,
        confirmation_file: None,
        rollback_receipt: None,
        namespaces: vec![
            crate::store::pinning::legacy_migration::Namespace {
                source: source.into(),
                target: fixture.provider.clone(),
                backend: route.backend.clone(),
                scope: route.scope.clone(),
                resource_type: route.resource_type(),
                binding_only: false,
                target_route: route,
                bindings,
            },
            crate::store::pinning::legacy_migration::Namespace {
                source: foreign_source.into(),
                target: foreign_provider,
                backend: foreign_route.backend.clone(),
                scope: foreign_route.scope.clone(),
                resource_type: foreign_route.resource_type(),
                binding_only: false,
                target_route: foreign_route,
                bindings: foreign_bindings,
            },
        ],
    };
    let plan = crate::store::pinning::legacy_migration::inventory(&fixture.db, &declaration)
        .await
        .unwrap();
    let applied = crate::store::pinning::legacy_migration::apply(&fixture.db, &declaration, &plan)
        .await
        .unwrap();
    if rolled_back {
        let mut inverse = declaration.clone();
        inverse.rollback_receipt = Some(applied.id.clone());
        inverse.ttl_seconds = 600;
        let reverse_plan =
            crate::store::pinning::legacy_migration::inventory(&fixture.db, &inverse)
                .await
                .unwrap();
        crate::store::pinning::legacy_migration::rollback(&fixture.db, &applied.id, &reverse_plan)
            .await
            .unwrap();
        let old = remote_pin_ledger::Entity::find_by_id((source.to_owned(), OTHER.to_owned()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let mut current = old.into_active_model();
        current.provider = Set(fixture.provider.clone());
        current.insert(&fixture.db).await.unwrap();
    }
    assert_eq!(
        remote_pin_ledger::Entity::find_by_id((source.to_owned(), OTHER.to_owned()))
            .one(&fixture.db)
            .await
            .unwrap()
            .is_some(),
        rolled_back
    );
    assert_eq!(
        pin_job::Entity::find_by_id(alias_job)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .provider,
        if rolled_back {
            source
        } else {
            fixture.provider.as_str()
        }
    );
    assert_eq!(
        evidence::Entity::find_by_id(alias_receipt)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .provider,
        source
    );
    (
        fixture.db,
        fixture.provider,
        alias_job.into(),
        alias_receipt.into(),
    )
}

#[tokio::test]
async fn admission_context_refreshes_own_writes_and_preserves_reserved_rpc_exception() {
    let fixture = fixture().await;
    let tx = fixture.db.begin().await.unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Temporary
    );
    let original = evidence::Entity::find_by_id(&fixture.receipt)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let mut missing_job = original.into_active_model();
    missing_job.id = Set("missing-job-evidence".into());
    missing_job.job_id = Set("missing-job".into());
    missing_job.insert(&tx).await.unwrap();
    assert_eq!(
        AdmissionSnapshot::read(&tx, &fixture.provider)
            .await
            .unwrap()
            .evidence
            .len(),
        2
    );
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        admission_barrier(&fixture.db, &fixture.provider)
            .await
            .unwrap(),
        AdmissionBarrier::Temporary
    );
    let staged = remote_pin_ledger::Entity::find_by_id((fixture.provider.clone(), CID.to_owned()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(staged.effect, "unknown");
    let tx = fixture.db.begin().await.unwrap();
    evidence::Entity::delete_many()
        .filter(evidence::Column::Provider.eq(&fixture.provider))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    let (reserved_db, reserved_provider) = database().await;
    publish(
        &reserved_db,
        &reserved_provider,
        "never-started-object",
        CID,
    )
    .await;
    let reserved =
        remote_pin_ledger::Entity::find_by_id((reserved_provider.clone(), CID.to_owned()))
            .one(&reserved_db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(reserved.effect, "reserved");
    assert_eq!(
        admission_barrier(&reserved_db, &reserved_provider)
            .await
            .unwrap(),
        AdmissionBarrier::Clear
    );
    for route in [
        None,
        Some("{".to_owned()),
        Some(r#"{"api_profile":"kubo""#.to_owned()),
        Some(r#"{"api_profile":"kubo"}"#.to_owned()),
    ] {
        let tx = reserved_db.begin().await.unwrap();
        remote_pin_ledger::Entity::update_many()
            .col_expr(remote_pin_ledger::Column::Route, Expr::value(route))
            .filter(remote_pin_ledger::Column::Provider.eq(&reserved_provider))
            .exec(&tx)
            .await
            .unwrap();
        assert_eq!(
            admission_barrier(&tx, &reserved_provider).await.unwrap(),
            AdmissionBarrier::Operator
        );
        tx.rollback().await.unwrap();
    }
    let tx = reserved_db.begin().await.unwrap();
    remote_pin_ledger::Entity::update_many()
        .col_expr(remote_pin_ledger::Column::Effect, Expr::value("confirmed"))
        .filter(remote_pin_ledger::Column::Provider.eq(&reserved_provider))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &reserved_provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    record(
        &fixture.db,
        &fixture.claim,
        &fixture.receipt,
        &SubmitObservation {
            effect: SubmitEffect::NotSubmitted,
            resources: vec![],
            result: Err(ProviderError {
                class: crate::pinning::provider::ProviderErrorClass::Protocol,
                message: "not dispatched".into(),
                retry_after: None,
            }),
        },
        Utc::now(),
    )
    .await
    .unwrap();
    let snapshot = AdmissionSnapshot::read(&fixture.db, &fixture.provider)
        .await
        .unwrap();
    assert!(snapshot.evidence.is_empty());
    assert!(snapshot.jobs.is_empty());
    assert!(snapshot.targets.is_empty());
    assert_eq!(
        admission_barrier(&fixture.db, &fixture.provider)
            .await
            .unwrap(),
        AdmissionBarrier::Clear
    );
}

#[tokio::test]
async fn admission_context_missing_references_deny_and_decode_errors_propagate() {
    let fixture = fixture().await;
    for column in [pin_job::Column::TargetId, pin_job::Column::LeaseId] {
        let tx = fixture.db.begin().await.unwrap();
        pin_job::Entity::update_many()
            .col_expr(column, Expr::value("missing"))
            .filter(pin_job::Column::Id.eq(&fixture.job))
            .exec(&tx)
            .await
            .unwrap();
        assert_eq!(
            admission_barrier(&tx, &fixture.provider).await.unwrap(),
            AdmissionBarrier::Operator
        );
        tx.rollback().await.unwrap();
    }
    for column in [evidence::Column::JobId, evidence::Column::ExpectedCid] {
        let tx = fixture.db.begin().await.unwrap();
        evidence::Entity::update_many()
            .col_expr(column, Expr::value("missing"))
            .filter(evidence::Column::Id.eq(&fixture.receipt))
            .exec(&tx)
            .await
            .unwrap();
        assert_eq!(
            admission_barrier(&tx, &fixture.provider).await.unwrap(),
            AdmissionBarrier::Operator
        );
        tx.rollback().await.unwrap();
    }
    let tx = fixture.db.begin().await.unwrap();
    pin_lease_target::Entity::update_many()
        .col_expr(pin_lease_target::Column::Provider, Expr::value("outside"))
        .filter(pin_lease_target::Column::Id.eq(&fixture.target))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    pin_lease_target::Entity::update_many()
        .col_expr(
            pin_lease_target::Column::CreatedAt,
            Expr::value("not-a-timestamp"),
        )
        .filter(pin_lease_target::Column::Id.eq(&fixture.target))
        .exec(&tx)
        .await
        .unwrap();
    assert!(admission_barrier(&tx, &fixture.provider).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(
        admission_barrier(&fixture.db, &fixture.provider)
            .await
            .unwrap(),
        AdmissionBarrier::Temporary
    );
}

#[tokio::test]
async fn admission_context_preserves_lease_union_and_full_invocation_context() {
    let fixture = fixture().await;
    publish(&fixture.db, &fixture.provider, "waiting-object", OTHER).await;
    let other = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Cid.eq(OTHER))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let snapshot = AdmissionSnapshot::read(&fixture.db, &fixture.provider)
        .await
        .unwrap();
    assert_eq!(snapshot.jobs.len(), 1);
    assert_eq!(snapshot.remotes.len(), 1);
    assert_eq!(snapshot.targets.len(), 1);
    assert_eq!(snapshot.leases.len(), 1);
    let tx = fixture.db.begin().await.unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LeaseId,
            Expr::value(other.lease_id.clone()),
        )
        .filter(pin_job::Column::Id.eq(&fixture.job))
        .exec(&tx)
        .await
        .unwrap();
    let snapshot = AdmissionSnapshot::read(&tx, &fixture.provider)
        .await
        .unwrap();
    assert!(snapshot.leases.contains_key(&fixture.lease));
    assert!(snapshot.leases.contains_key(&other.lease_id));
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::Provider, Expr::value("outside"))
        .filter(pin_job::Column::Id.eq(&fixture.job))
        .exec(&tx)
        .await
        .unwrap();
    let snapshot = AdmissionSnapshot::read(&tx, &fixture.provider)
        .await
        .unwrap();
    assert!(snapshot.jobs.is_empty());
    assert!(snapshot.histories.contains_key(&fixture.job));
    assert!(snapshot.captures.contains_key(&fixture.job));
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        admission_barrier(&fixture.db, &fixture.provider)
            .await
            .unwrap(),
        AdmissionBarrier::Temporary
    );
}

#[tokio::test]
async fn admission_context_retains_release_generation_and_historical_ttl_fences() {
    let fixture = fixture().await;
    let tx = fixture.db.begin().await.unwrap();
    pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
        provider: Set(fixture.provider.clone()),
        cid: Set(CID.into()),
        epoch: Set(1),
        ledger: Set("{}".into()),
    })
    .exec(&tx)
    .await
    .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::Generation, Expr::value(2_i64))
        .filter(pin_lease::Column::Id.eq(&fixture.lease))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    pin_lease::Entity::update_many()
        .col_expr(
            pin_lease::Column::ExpiresAt,
            Expr::value(Utc::now() - chrono::Duration::seconds(1)),
        )
        .filter(pin_lease::Column::Id.eq(&fixture.lease))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Temporary
    );
    pin_lease::Entity::update_many()
        .col_expr(pin_lease::Column::State, Expr::value("expired"))
        .filter(pin_lease::Column::Id.eq(&fixture.lease))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        admission_barrier(&fixture.db, &fixture.provider)
            .await
            .unwrap(),
        AdmissionBarrier::Temporary
    );
}

#[tokio::test]
async fn admission_context_projects_settled_alias_witnesses_jobs_and_releases() {
    let (db, provider, job, receipt) = migrated_settled_alias(false, false).await;
    assert!(
        remote_pin_ledger::Entity::find_by_id((provider.clone(), OTHER.into()))
            .one(&db)
            .await
            .unwrap()
            .is_some()
    );
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(&format!(
        "UPDATE pin_submit_observations SET claim_until='not-a-timestamp',resources='not-json' \
         WHERE id='{receipt}'"
    ))
    .await
    .unwrap();
    assert_eq!(
        admission_barrier(&tx, &provider).await.unwrap(),
        AdmissionBarrier::Clear
    );
    tx.rollback().await.unwrap();

    let tx = db.begin().await.unwrap();
    let foreign_provider = foreign_identity().allocation_key();
    let foreign_ledger = remote_pin_ledger::Entity::find_by_id((foreign_provider, FOREIGN.into()))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let mut unrelated = foreign_ledger.into_active_model();
    unrelated.provider = Set(provider.clone());
    unrelated.insert(&tx).await.unwrap();
    assert_eq!(
        admission_barrier(&tx, &provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();

    let tx = db.begin().await.unwrap();
    evidence::Entity::update_many()
        .col_expr(evidence::Column::Outcome, Expr::value("matched"))
        .col_expr(evidence::Column::Effect, Expr::value("observed"))
        .filter(evidence::Column::Id.eq(&receipt))
        .exec(&tx)
        .await
        .unwrap();
    remote_pin_ledger::Entity::update_many()
        .col_expr(remote_pin_ledger::Column::Effect, Expr::value("confirmed"))
        .filter(remote_pin_ledger::Column::Provider.eq(&provider))
        .filter(remote_pin_ledger::Column::Cid.eq(OTHER))
        .exec(&tx)
        .await
        .unwrap();
    pin_job::Entity::update_many()
        .col_expr(pin_job::Column::State, Expr::value("error"))
        .filter(pin_job::Column::Id.eq(&job))
        .exec(&tx)
        .await
        .unwrap();
    assert_eq!(
        admission_barrier(&tx, &provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();

    let (released_db, released_provider, _, _) = migrated_settled_alias(true, false).await;
    assert_eq!(
        admission_barrier(&released_db, &released_provider)
            .await
            .unwrap(),
        AdmissionBarrier::Operator
    );

    let (rolled_back_db, rolled_back_provider, _, _) = migrated_settled_alias(false, true).await;
    assert_eq!(
        admission_barrier(&rolled_back_db, &rolled_back_provider)
            .await
            .unwrap(),
        AdmissionBarrier::Operator
    );
}

#[tokio::test]
async fn admission_context_reads_only_unsettled_resource_candidates() {
    let fixture = fixture().await;
    record(
        &fixture.db,
        &fixture.claim,
        &fixture.receipt,
        &SubmitObservation {
            effect: SubmitEffect::NotSubmitted,
            resources: vec![],
            result: Err(ProviderError {
                class: crate::pinning::provider::ProviderErrorClass::Protocol,
                message: "not dispatched".into(),
                retry_after: None,
            }),
        },
        Utc::now(),
    )
    .await
    .unwrap();
    let settled = evidence::Entity::find_by_id(&fixture.receipt)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let ledger = remote_pin_ledger::Entity::find_by_id((fixture.provider.clone(), CID.to_owned()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    for index in 0..64 {
        let cid = format!("settled-{index:02}");
        let mut observation = settled.clone().into_active_model();
        observation.id = Set(format!("settled-receipt-{index:02}"));
        observation.job_id = Set(format!("settled-job-{index:02}"));
        observation.expected_cid = Set(cid.clone());
        observation.insert(&tx).await.unwrap();

        let mut held = ledger.clone().into_active_model();
        held.cid = Set(cid);
        held.insert(&tx).await.unwrap();
    }
    tx.execute_unprepared(
        "UPDATE pin_submit_observations SET claim_until='not-a-timestamp' \
         WHERE id='settled-receipt-00'",
    )
    .await
    .unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Clear
    );

    let mut unresolved = ledger.into_active_model();
    unresolved.cid = Set(OTHER.into());
    unresolved.effect = Set("confirmed".into());
    unresolved.insert(&tx).await.unwrap();
    assert_eq!(
        admission_barrier(&tx, &fixture.provider).await.unwrap(),
        AdmissionBarrier::Operator
    );
    tx.rollback().await.unwrap();
}
