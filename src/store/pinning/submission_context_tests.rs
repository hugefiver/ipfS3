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

async fn database() -> (DatabaseConnection, String) {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let identity = ProviderIdentity {
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
    };
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
    assert_eq!(snapshot.evidence.len(), 1);
    assert!(!snapshot.evidence[0].needs_attention);
    assert!(snapshot.jobs.contains_key(&fixture.job));
    assert!(snapshot.targets.contains_key(&fixture.target));
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
