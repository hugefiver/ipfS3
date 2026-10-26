use super::*;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderMode},
        management::{
            self, Action, ItemRequest, PlanRequest, PlanStatus, RetirementResource,
            RetirementSelection, RouteIdentity, Subject, local_config, retirement,
        },
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::pinning::{
        ledger,
        management::{ManagementJobScope, actions, records},
        publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};

const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

pub(super) async fn ordinary_with_database(
    db: sea_orm::DatabaseConnection,
) -> (super::super::Fixture, jobs::ClaimedPinJob) {
    let fixture = super::super::fixture_with_database([], db, 10_000, 100, None, "1s").await;
    fixture
        .store
        .db()
        .execute_unprepared(
            "UPDATE pin_leases SET state='cancelled',generation=generation+1; \
         UPDATE pin_lease_targets SET state='released'; \
         UPDATE remote_pins SET status='pinned',request_id='ordinary-retained'; \
         INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) \
         VALUES('noop','bafy-worker','unknown','confirmed')",
        )
        .await
        .unwrap();
    jobs::enqueue_job(
        fixture.store.db(),
        jobs::unpin_job("noop", "bafy-worker", 1, Utc::now()),
    )
    .await
    .unwrap();
    let claimed = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    (fixture, claimed)
}

pub(super) async fn single_connection() -> sea_orm::DatabaseConnection {
    let mut options = sea_orm::ConnectOptions::new("sqlite::memory:");
    options.max_connections(1).test_before_acquire(false);
    sea_orm::Database::connect(options).await.unwrap()
}

pub(super) async fn warm(store: &Store) {
    let work = SharedDecodeBudget::default();
    store
        .qualify_runtime_schema(records::DecodeWork::shared(&work))
        .await
        .unwrap();
}

pub(super) async fn assert_unchanged_and_released(
    fixture: &super::super::Fixture,
    claimed: &jobs::ClaimedPinJob,
    before: &NativeEffects,
) {
    let tx = fixture.store.db().begin().await.unwrap();
    tx.execute_unprepared("UPDATE pin_jobs SET attempts=attempts WHERE id IS NOT NULL")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        &NativeEffects::read(&fixture.store, &claimed.model.id).await,
        before
    );
    assert_eq!(
        pin_job::Entity::find_by_id(claimed.model.id.clone())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap(),
        claimed.model
    );
    assert_eq!(fixture.provider.unpins.load(Ordering::SeqCst), 0);
}

pub(super) struct Fixture {
    pub(super) store: Store,
    pub(super) claimed: jobs::ClaimedPinJob,
    pub(super) authority: records::JobAuthority,
    pub(super) server: wiremock::MockServer,
    _directory: tempfile::TempDir,
}

pub(super) async fn confirmed_cleanup() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("cleanup.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = crate::store::connect_database(&url).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let server = wiremock::MockServer::start().await;
    let mut raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="cleanup"
        kind="pinata"
        token_env="WORKER_CLEANUP_TEST_TOKEN"
        priority=1
        max_bytes=1000000
        max_pins=1000
        [[pinning.policies]]
        bucket="bucket"
        prefix=""
        trigger="request"
        provider_mode="all"
        providers=["cleanup"]
        default_duration="1h"
        max_duration="2h"
        allow_decompressed=true
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="cleanup"
        provider_id="cleanup-provider"
        display_name="Cleanup"
        backend="pinata"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        secret_ref="env:WORKER_CLEANUP_TEST_TOKEN"
        api_profile="pinata-psa"
        strategy="cid"
        cleanup="managed"
    "#,
    )
    .unwrap();
    raw.pinning.providers[0].endpoint = Some(format!("{}/psa", server.uri()));
    let config = local_config::compile(&raw, |_| Some("worker-test-token".into()))
        .unwrap()
        .with_source_node(CID.into())
        .unwrap();
    let provider = &config.pinning.providers[0];
    ledger::register_route(&db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                "cleanup-object".into(),
                "bucket",
                "owned",
                CID.into(),
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
                    policy_id: config.pinning.policies[0].identity.clone(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider.name.clone()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 10,
            },
        },
        &config.pinning.provider_limits,
    )
    .await
    .unwrap();

    // Establish the native confirmed, application-owned fixture resource, then
    // release its references before asking normal retirement planning for the
    // exact ExistingCleanup privilege.
    let tx = db.begin().await.unwrap();
    tx.execute_unprepared(
        "UPDATE remote_pins SET status='pinned',request_id='cleanup-resource'; \
         UPDATE remote_pin_ledger SET ownership='application_created',effect='confirmed', \
         first_observed_at=CURRENT_TIMESTAMP,last_observed_at=CURRENT_TIMESTAMP,remote_pinned_at=CURRENT_TIMESTAMP; \
         UPDATE pin_jobs SET state='done',locked_until=NULL; \
         UPDATE pin_leases SET state='cancelled',generation=generation+1; \
         UPDATE pin_lease_targets SET state='released'",
    ).await.unwrap();
    tx.commit().await.unwrap();
    let remote = remote_pin::Entity::find().one(&db).await.unwrap().unwrap();
    let route = RouteIdentity::from_original(provider.route_snapshot());
    let canonical = crate::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
    let request = PlanRequest {
        plan_id: "worker-cleanup-authority".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            subject: Subject::Provider {
                route: route.clone(),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup {
                    resources: vec![RetirementResource {
                        key: route.remote_key(&canonical),
                        route,
                        epoch: remote.epoch,
                        resource_id: remote.request_id.clone().unwrap(),
                    }],
                },
            },
        }],
    };
    let snapshot = management::db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = retirement::PreparedPlanning::collect(
        &db,
        &request,
        &snapshot,
        SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert!(
        prepared
            .plan
            .items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_)))
    );
    let ack = prepared
        .plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect::<Vec<_>>();
    let binding = records::confirm_with_session(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &prepared.plan),
        &ack,
        chrono::Duration::minutes(5),
        SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    prepared.close().await.unwrap();
    let applied = actions::apply(&db, &config, &auth, &binding).await.unwrap();
    assert_eq!(applied.items[0].jobs.len(), 1);
    let authority = applied.items[0].jobs[0].clone();
    assert_eq!(authority.directive, records::JobDirective::ExistingCleanup);
    let scope = ManagementJobScope::from_applied(&db, &auth, &binding)
        .await
        .unwrap();
    let mut claims = jobs::claim_due_management_jobs(
        &Store::new(db.clone()),
        &scope,
        Utc::now(),
        chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap();
    assert_eq!(claims.len(), 1);
    let claimed = claims.pop().unwrap();
    assert_eq!(claimed.model.id, authority.job_id);
    Fixture {
        store: Store::new(db),
        claimed,
        authority,
        server,
        _directory: directory,
    }
}
