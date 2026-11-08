//! Real configuration + publication + claim + worker and leaf provider I/O.
use super::*;
use crate::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        config::{LeaseDuration, ValidatedPinningConfig},
        identity::Ownership,
        ipfs_rpc::{RpcProfile, RpcStrategy},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self,
        entities::{pin_provider_usage, pin_replica_observation, pin_resource_history},
        pinning::{
            management::records,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ConnectionTrait, Set};
use sea_orm_migration::SchemaManager;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const OTHER: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";

struct RpcFixture {
    store: Store,
    coordinator: Arc<PinningCoordinator>,
    provider: String,
    policy_id: String,
    raw: Config,
}

async fn source_file(source: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":CID,"Type":"file"})),
        )
        .mount(source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"stored-object-bytes"))
        .mount(source)
        .await;
}

async fn recursive(target: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Keys":{(CID):{"Type":"recursive"}}})),
        )
        .mount(target)
        .await;
}

async fn fixture(
    endpoint: String,
    source: &MockServer,
    profile: RpcProfile,
    strategy: RpcStrategy,
) -> RpcFixture {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    fixture_on_database(endpoint, source, profile, strategy, db).await
}

fn rpc_config(
    endpoint: &str,
    source_endpoint: &str,
    profile: RpcProfile,
    strategy: RpcStrategy,
) -> Config {
    let (kind, profile_name) = match profile {
        RpcProfile::Filebase => ("filebase", "filebase"),
        RpcProfile::Kubo => ("ipfs_rpc", "kubo"),
    };
    let api_profile = match profile {
        RpcProfile::Filebase => "filebase-rpc",
        RpcProfile::Kubo => "kubo",
    };
    let mut raw: Config = toml::from_str(&format!(
        r#"
        [[pinning.providers]]
        name = 'rpc-fixture'
        kind = '{kind}'
        api = 'rpc'
        endpoint = '{endpoint}'
        strategy = '{}'
        token_env = 'TEST_RPC_SECRET'
        priority = 1
        max_bytes = 100
        max_pins = 1

        [[pinning.policies]]
        bucket = 'rpc'
        prefix = ''
        trigger = 'always'
        provider_mode = 'all'
        providers = ['rpc-fixture']
        default_duration = '1h'
        max_duration = '2h'

        [[pinning_rpc.providers]]
        config_name = 'rpc-fixture'
        profile = '{profile_name}'
        auth = 'bearer'
        allow_private_network = true
        connect_timeout_seconds = 1
        control_timeout_seconds = 2
        idle_timeout_seconds = 2

        [pinning_identity]
        primary_storage_domain = 'local'
        [[pinning_identity.providers]]
        config_name = 'rpc-fixture'
        provider_id = 'rpc-fixture'
        display_name = 'RPC fixture'
        backend = '{profile_name}'
        scope = 'isolated-node'
        storage_domain = 'remote'
        credential_revision = 1
        endpoint_revision = 1
        secret_ref = 'env:TEST_RPC_SECRET'
        api_profile = '{api_profile}'
        strategy = '{}'
        cleanup = 'managed'
        "#,
        strategy.as_str(),
        strategy.as_str(),
    ))
    .unwrap();
    raw.kubo.rpc_url = source_endpoint.into();
    raw.decompress_zip.auto_pin_outputs = Some(false);
    raw
}

async fn fixture_on_database(
    endpoint: String,
    source: &MockServer,
    profile: RpcProfile,
    strategy: RpcStrategy,
    db: sea_orm::DatabaseConnection,
) -> RpcFixture {
    let raw = rpc_config(&endpoint, &source.uri(), profile, strategy);
    let validated = ValidatedPinningConfig::from_config(&raw, |name| {
        (name == "TEST_RPC_SECRET").then(|| "private-fixture-token".into())
    })
    .unwrap();
    let key = validated.providers[0].identity.allocation_key();
    let policy_id = validated.policies[0].identity.clone();
    let coordinator =
        PinningCoordinator::build_with_kubo(validated, Some(KuboClient::new(source.uri())))
            .unwrap();
    store::run_migrations(&db).await.unwrap();
    let manager = SchemaManager::new(&db);
    assert!(
        manager.has_table("pin_submit_observations").await.unwrap(),
        "production migrations must create RPC submission evidence"
    );
    store::bucket::create(&db, "rpc", None).await.unwrap();
    let store = Store::new(db);
    coordinator.register_identities(&store).await.unwrap();
    let fixture = RpcFixture {
        store,
        coordinator,
        provider: key,
        policy_id,
        raw,
    };
    fixture.publish("first", CID).await;
    fixture
}

impl RpcFixture {
    async fn publish(&self, key: &str, cid: &str) {
        publication::application::publish_object(
            &self.store,
            PublicationRequest {
                object: PublicationObject::from_put(
                    uuid::Uuid::new_v4().to_string(),
                    "rpc",
                    key,
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
                        policy_id: self.policy_id.clone(),
                        provider_mode: ProviderMode::All,
                        providers: vec![self.provider.clone()],
                        content_mode: ContentMode::Object,
                        duration: LeaseDuration::parse("1h").unwrap(),
                    }],
                },
                object_target: PinTargetSpec {
                    cid: cid.into(),
                    logical_size: 100,
                },
            },
            self.coordinator.provider_limits(),
        )
        .await
        .unwrap();
    }

    async fn claim(&self) -> Option<ClaimedPinJob> {
        let pending = pin_job::Entity::find()
            .filter(pin_job::Column::State.eq("pending"))
            .order_by_asc(pin_job::Column::NextAttemptAt)
            .one(self.store.db())
            .await
            .unwrap()?;
        jobs::claim_due_worker_jobs_store(
            &self.store,
            pending.next_attempt_at,
            ChronoDuration::seconds(30),
            1,
            jobs::WorkerClaimSchedule {
                in_flight_by_provider: &Default::default(),
                last_served_ticket_by_provider: &Default::default(),
                configured_priorities: &Default::default(),
                active_job_ids: &Default::default(),
            },
        )
        .await
        .unwrap()
        .pop()
    }

    async fn run(&self, claim: ClaimedPinJob) -> AppResult<()> {
        tokio::time::timeout(
            Duration::from_secs(15),
            execute_claimed_job(
                &self.store,
                &self.coordinator,
                &Arc::new(Semaphore::new(2)),
                claim,
            ),
        )
        .await
        .expect("RPC fixture worker exceeded its bounded I/O budget")
    }

    async fn drain(&self) {
        for _ in 0..10 {
            let Some(claim) = self.claim().await else {
                return;
            };
            self.run(claim).await.unwrap();
        }
        panic!("RPC worker did not reach a stable bounded state");
    }

    async fn evidence(&self) -> Vec<ledger::submission_entity::Model> {
        ledger::submission::observations(self.store.db(), &self.provider)
            .await
            .unwrap()
    }

    async fn assert_held(&self) {
        let usage = pin_provider_usage::Entity::find_by_id(self.provider.clone())
            .one(self.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_bytes, usage.reserved_pins), (100, 1));
    }
}

async fn no_delete(target: &MockServer) {
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != "DELETE" && request.url.path() != "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn worker_matched_preexisting_shared_cid_reuses_one_hold_and_retain_never_deletes() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await; // external preexisting pin is not creation proof
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    fixture.drain().await;
    let evidence = fixture.evidence().await;
    assert_eq!(
        evidence.len(),
        1,
        "RPC submit evidence missing; jobs={:?}; targets={:?}",
        pin_job::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap(),
        pin_lease_target::Entity::find()
            .all(fixture.store.db())
            .await
            .unwrap(),
    );
    assert_eq!(evidence[0].outcome, "matched");
    fixture.publish("second", CID).await;
    fixture.drain().await;
    let targets = pin_lease_target::Entity::find()
        .all(fixture.store.db())
        .await
        .unwrap();
    assert_eq!(targets.len(), 2);
    assert!(targets.iter().all(|target| target.state == "pinned"));
    fixture.assert_held().await;
    assert!(
        !ledger::cleanup_allowed(fixture.store.db(), &fixture.provider, CID)
            .await
            .unwrap()
    );
    for lease in pin_lease::Entity::find()
        .all(fixture.store.db())
        .await
        .unwrap()
    {
        let txn = fixture.store.db().begin().await.unwrap();
        leases::cancel_lease(&txn, &lease.id, Utc::now())
            .await
            .unwrap();
        txn.commit().await.unwrap();
    }
    fixture.drain().await;
    fixture.assert_held().await;
    assert!(!matches!(
        store_quota::reserve_unique(
            fixture.store.db(),
            &fixture.provider,
            OTHER,
            100,
            fixture.coordinator.provider_limits(),
            Utc::now()
        )
        .await
        .unwrap(),
        store_quota::ReservationOutcome::Reserved | store_quota::ReservationOutcome::Reused
    ));
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_kubo_cid_keeps_exact_api_and_unknown_ownership_without_double_counting() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"node-target"})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{CID}")))
        .and(query_param("offline", "true"))
        .and(query_param("with-local", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash":CID,"WithLocality":true,"Local":true})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", CID))
        .and(query_param("recursive", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    fixture.drain().await;
    let evidence = fixture.evidence().await;
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].outcome, "matched");
    assert!(!evidence[0].needs_attention);
    let history = jobs::submission_history(fixture.store.db(), &evidence[0].job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (history.api.as_str(), history.strategy.as_str()),
        ("kubo", "cid")
    );
    let roots: Vec<ledger::submission::ResourceEvidence> =
        serde_json::from_str(&evidence[0].resources).unwrap();
    assert_eq!(roots[0].resource.ownership, Ownership::Unknown);
    fixture.assert_held().await;
    assert!(source.received_requests().await.unwrap().is_empty());
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_matched_receipt_survives_overwrite_object_and_bucket_delete() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    fixture.drain().await;
    let receipt = fixture.evidence().await.pop().unwrap();
    assert_eq!(receipt.outcome, "matched");

    // A retained RPC pin is not a permanent metadata owner. Overwrite with no
    // new pin intent, then use the same guarded delete used by the S3 boundary.
    publication::publish_object(
        fixture.store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "rpc",
                "first",
                OTHER.into(),
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
                leases: vec![],
            },
            object_target: PinTargetSpec {
                cid: OTHER.into(),
                logical_size: 100,
            },
        },
        fixture.coordinator.provider_limits(),
    )
    .await
    .unwrap();
    let now = Utc::now();
    let guard = store::import::ownership::admit_content_mutation(
        fixture.store.db(),
        "rpc",
        "first",
        None,
        crate::import::SupersedeReason::DeleteObject,
        now,
    )
    .await
    .unwrap();
    publication::delete_version_with_leases_guarded(
        fixture.store.db(),
        "rpc",
        "first",
        store::object_version::VersionSelector::Current,
        guard,
        now,
    )
    .await
    .unwrap();
    store::bucket::delete(fixture.store.db(), "rpc")
        .await
        .unwrap();
    assert!(
        !store::bucket::exists(fixture.store.db(), "rpc")
            .await
            .unwrap()
    );
    assert_eq!(fixture.evidence().await, vec![receipt]);
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_mismatch_and_multiple_roots_keep_actual_resources_without_expected_find_or_retry() {
    for roots in [vec![OTHER], vec![CID, OTHER]] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        source_file(&source).await;
        recursive(&target).await; // must not "repair" mismatch using somebody else's pin
        let body = roots
            .iter()
            .map(|cid| format!("{{\"Hash\":\"{cid}\"}}\n"))
            .collect::<String>();
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(&target)
            .await;
        let fixture = fixture(
            target.uri(),
            &source,
            RpcProfile::Filebase,
            RpcStrategy::Upload,
        )
        .await;
        fixture.drain().await;
        let evidence = fixture.evidence().await;
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].outcome, "cid_mismatch");
        assert!(evidence[0].needs_attention);
        let actual: Vec<ledger::submission::ResourceEvidence> =
            serde_json::from_str(&evidence[0].resources).unwrap();
        assert_eq!(actual.len(), roots.len());
        assert!(actual.iter().any(|resource| resource.resource.cid == OTHER));
        assert!(
            pin_lease_target::Entity::find()
                .all(fixture.store.db())
                .await
                .unwrap()
                .iter()
                .all(|target| target.state == "error")
        );
        fixture.assert_held().await;
        assert!(
            target
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() == "/api/v0/add")
        );
        no_delete(&target).await;
    }
}

#[tokio::test]
async fn worker_unknown_empty_reported_and_timeout_keep_debt_and_never_retry_http() {
    for response in [
        ResponseTemplate::new(403),
        ResponseTemplate::new(400),
        ResponseTemplate::new(200)
            .set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n{{broken-json}}\n")),
        ResponseTemplate::new(200)
            .set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n"))
            .set_delay(Duration::from_secs(3)),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        source_file(&source).await;
        recursive(&target).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(response)
            .expect(1)
            .mount(&target)
            .await;
        let fixture = fixture(
            target.uri(),
            &source,
            RpcProfile::Filebase,
            RpcStrategy::Upload,
        )
        .await;
        fixture.drain().await;
        let evidence = fixture.evidence().await;
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].effect, "unknown");
        assert!(evidence[0].needs_attention);
        let safe: serde_json::Value =
            serde_json::from_str(evidence[0].safe_error.as_deref().unwrap()).unwrap();
        assert_eq!(safe["effect"], "unknown");
        assert!(
            jobs::submission_history(fixture.store.db(), &evidence[0].job_id)
                .await
                .unwrap()
                .is_some_and(|h| h.submit_calls == 1
                    && h.recovery_queries == 0
                    && h.state == "needs_attention")
        );
        fixture.assert_held().await;
        assert!(
            target
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() == "/api/v0/add")
        );
        no_delete(&target).await;
    }
}

#[tokio::test]
async fn parked_unknown_cid_receipt_recovers_from_one_bounded_verified_read() {
    use crate::store::entities::pin_replica_observation;

    let source = MockServer::start().await;
    let target = MockServer::start().await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"node-target"})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{CID}")))
        .and(query_param("offline", "true"))
        .and(query_param("with-local", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash":CID,"WithLocality":true,"Local":true})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let claim = fixture.claim().await.unwrap();
    let job_id = claim.model.id.clone();
    fixture.run(claim).await.unwrap();
    let original = fixture.evidence().await.pop().unwrap();
    assert_eq!(
        (original.effect.as_str(), original.outcome.as_str()),
        ("unknown", "unknown")
    );
    assert!(original.needs_attention);

    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    let recovery = fixture
        .claim()
        .await
        .expect("bounded maintenance must wake the exact parked cid receipt");
    fixture.run(recovery).await.unwrap();

    let settled = fixture.evidence().await.pop().unwrap();
    assert_eq!(settled.id, original.id);
    assert_eq!(settled.submit_call, original.submit_call);
    assert_eq!(settled.route, original.route);
    assert_eq!(settled.remote_epoch, original.remote_epoch);
    assert_eq!(settled.effect, "unknown");
    assert_eq!(settled.outcome, "unknown");
    assert_eq!(settled.resources, original.resources);
    assert_eq!(settled.observed_at, original.observed_at);
    assert!(!settled.needs_attention);
    let recovery_facts = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&job_id))
        .order_by_asc(pin_replica_observation::Column::Sequence)
        .all(fixture.store.db())
        .await
        .unwrap();
    assert_eq!(recovery_facts.len(), 2);
    let recovery_fact = &recovery_facts[0];
    assert_eq!(recovery_fact.route, original.route);
    assert_eq!(recovery_fact.remote_epoch, original.remote_epoch);
    assert_eq!(recovery_fact.phase, "submit");
    assert!(!recovery_fact.projectable);
    let closure = &recovery_facts[1];
    assert_eq!(closure.phase, "submit_recovery");
    assert!(closure.outcome.starts_with("VerifiedPinned:"));
    assert!(closure.projectable);
    assert_eq!(closure.route, original.route);
    assert_eq!(closure.remote_epoch, original.remote_epoch);
    let resources: Vec<crate::pinning::provider::ObservedResource> =
        serde_json::from_str(&recovery_fact.resources).unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].cid, CID);
    assert_eq!(
        resources[0].status,
        crate::pinning::provider::ObservedResourceStatus::RecursiveVerified
    );
    assert_eq!(resources[0].ownership, Ownership::Unknown);
    let history = jobs::submission_history(fixture.store.db(), &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((history.submit_calls, history.recovery_queries), (1, 1));
    assert_eq!(
        (history.effect.as_str(), history.state.as_str()),
        ("created", "settled")
    );
    assert_eq!(
        ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
            .await
            .unwrap(),
        ledger::submission::AdmissionBarrier::Clear
    );
    assert_eq!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn historical_single_pin_accepted_unknown_requires_verified_read_before_recovery() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"node-target"})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash":CID,"WithLocality":true,"Local":true})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID,CID]})),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    fixture.run(fixture.claim().await.unwrap()).await.unwrap();
    let original = fixture.evidence().await.pop().unwrap();
    assert_eq!(
        (original.effect.as_str(), original.outcome.as_str()),
        ("observed", "unknown")
    );
    let resources: Vec<ledger::submission::ResourceEvidence> =
        serde_json::from_str(&original.resources).unwrap();
    assert!(matches!(resources.as_slice(), [resource]
        if resource.resource.status == crate::pinning::provider::ObservedResourceStatus::PinAccepted));
    assert!(original.needs_attention);
    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    fixture.run(fixture.claim().await.unwrap()).await.unwrap();
    let settled = fixture.evidence().await.pop().unwrap();
    assert_eq!(settled.effect, "observed");
    assert_eq!(settled.outcome, "unknown");
    assert_eq!(settled.resources, original.resources);
    assert_eq!(settled.observed_at, original.observed_at);
    assert!(!settled.needs_attention);
    assert_eq!(
        ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
            .await
            .unwrap(),
        ledger::submission::AdmissionBarrier::Clear
    );
    let requests = target.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
    assert!(
        requests
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/ls")
    );
    no_delete(&target).await;
}

#[tokio::test]
async fn confirmed_management_reconcile_without_id_recovers_exact_unknown_cid_receipt() {
    confirmed_management_cid_reconcile(false, false, false).await;
}

#[tokio::test]
async fn confirmed_management_reconcile_with_id_reports_late_conflicting_debt() {
    confirmed_management_cid_reconcile(true, false, false).await;
}

#[tokio::test]
async fn confirmed_read_settles_terminal_late_matched_cid_without_waking_original() {
    confirmed_management_cid_reconcile(true, true, false).await;
}

#[tokio::test]
async fn confirmed_management_reconcile_rejects_private_target_drift() {
    confirmed_management_cid_reconcile(false, false, true).await;
}

async fn confirmed_management_cid_reconcile(
    explicit_resource: bool,
    late_matched: bool,
    tamper_private_target: bool,
) {
    use crate::pinning::management::*;
    use crate::store::entities::{object, object_version};

    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":CID,"Type":"file"})),
        )
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 100]))
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"SourceNode"})),
        )
        .mount(&source)
        .await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":"TargetNode"})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"Hash":CID,"WithLocality":true,"Local":true})),
        )
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(if late_matched {
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]}))
        } else {
            ResponseTemplate::new(503)
        })
        .expect(1)
        .mount(&target)
        .await;
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("confirmed-reconcile.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    let fixture = fixture_on_database(
        target.uri(),
        &source,
        RpcProfile::Kubo,
        RpcStrategy::Cid,
        db,
    )
    .await;
    let original_claim = fixture.claim().await.unwrap();
    let original_job_id = original_claim.model.id.clone();
    if late_matched {
        let dispatched = dispatch_submit(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            original_claim,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
        jobs::park_submit_store(
            &fixture.store,
            &dispatched.claimed,
            "needs_attention",
            jobs::RPC_RECOVERY_PARK_REASON,
            Utc::now(),
        )
        .await
        .unwrap();
        ledger::submission::complete_fact(
            &fixture.store,
            dispatched.rpc_invocation.as_ref().unwrap(),
            &dispatched.observation,
            Utc::now(),
        )
        .await
        .unwrap();
    } else {
        fixture.run(original_claim).await.unwrap();
        fixture.store.db().execute_unprepared(
            "UPDATE pin_submit_history SET recovery_queries=8 WHERE job_id IN (SELECT id FROM pin_jobs WHERE operation='submit')",
        ).await.unwrap();
        let recovery = fixture
            .claim()
            .await
            .expect("the already scheduled original recovery must become due");
        assert_eq!(recovery.model.id, original_job_id);
        assert_eq!(recovery.model.submit_phase.as_deref(), Some("recovering"));
        fixture.run(recovery).await.unwrap();
    }
    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(&original_job_id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert!(fixture.claim().await.is_none());
    fixture.assert_held().await;
    let original_receipt = fixture.evidence().await.pop().unwrap();
    assert!(original_receipt.needs_attention);
    assert!(
        !target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/ls")
    );

    let original_route: crate::pinning::identity::ProviderRouteSnapshot =
        serde_json::from_str(&original_receipt.route).unwrap();
    let canonical_request =
        crate::pinning::ipfs_rpc::canonical_cid_request_id(&original_route, CID)
            .unwrap()
            .unwrap();
    if explicit_resource {
        remote_pin::Entity::update_many()
            .col_expr(
                remote_pin::Column::RequestId,
                sea_orm::sea_query::Expr::value(canonical_request.clone()),
            )
            .filter(remote_pin::Column::Provider.eq(&fixture.provider))
            .filter(remote_pin::Column::Cid.eq(CID))
            .exec(fixture.store.db())
            .await
            .unwrap();
    }
    let object = object::Entity::find()
        .filter(object::Column::Bucket.eq("rpc"))
        .filter(object::Column::Key.eq("first"))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: object.bucket.clone(),
        key: object.key.clone(),
        object_id: object.id.clone(),
        private_version_id: version.id,
        public_version: version
            .version_id
            .map_or(PublicVersionId::Null, PublicVersionId::Version),
        cid: crate::pinning::provider::canonical_resource_cid(&object.cid).unwrap(),
    };
    let config = local_config::compile(&fixture.raw, |name| {
        (name == "TEST_RPC_SECRET").then(|| "private-fixture-token".into())
    })
    .unwrap()
    .with_source_node("SourceNode".into())
    .unwrap();
    let auth = records::authorize_local(&fixture.raw, fixture.store.db())
        .await
        .unwrap();
    let source_client = KuboClient::new(source.uri());
    assert_eq!(source_client.base_url(), config.source_endpoint);
    assert_eq!(
        source_client.local_node_identity().await.unwrap(),
        "SourceNode"
    );
    assert_eq!(object.size, 100);
    assert!(!object.encrypted);
    let prepared = evidence::prepare(
        fixture.store.db(),
        &config,
        &auth,
        &source_client,
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: "SourceNode".into(),
            max_stored_bytes: 100,
            ttl: ChronoDuration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.stored_bytes, Some(100));
    let target_row = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Cid.eq(CID))
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    let request = PlanRequest {
        plan_id: "rpc-unknown-reconcile".into(),
        items: vec![ItemRequest {
            item_id: "unknown-cid".into(),
            action: Action::Reconcile,
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target_row.id.clone(),
                    source: None,
                },
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(fixture.store.db(), &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items().len(), 1);
    assert!(
        matches!(
            plan.items()[0].status,
            PlanStatus::Ready | PlanStatus::Candidate
        ),
        "readonly reconcile plan blocked: {:?}; target={}",
        plan.items()[0].status,
        target_row.state
    );
    assert!(
        matches!(&plan.items()[0].step, Some(PlannedStep::Object(step)) if step.source == RemoteDirective::ReconcileHistorical),
        "expected a bound historical readonly observation"
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        fixture.store.db(),
        &config,
        &auth,
        (&request, &snapshot, &plan),
        ack,
        ChronoDuration::minutes(5),
    )
    .await
    .unwrap();
    let check = fixture.store.db().begin().await.unwrap();
    let checked = records::revalidate_confirmed_in_transaction(&check, &config, &auth, &binding)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "readonly confirmation revalidation failed: {error:?}; plan={:?}",
                plan.items()[0].status
            )
        });
    assert_eq!(checked.len(), 1);
    assert!(matches!(
        checked[0].outcome,
        RevalidationOutcome::Checked(_)
    ));
    check.rollback().await.unwrap();
    if tamper_private_target {
        pin_lease_target::Entity::update_many()
            .col_expr(
                pin_lease_target::Column::LastError,
                sea_orm::sea_query::Expr::value("changed private target diagnostic"),
            )
            .filter(pin_lease_target::Column::Id.eq(&target_row.id))
            .exec(fixture.store.db())
            .await
            .unwrap();
        let failure = match crate::store::pinning::management::actions::apply_cancellable(
            fixture.store.db(),
            &config,
            &auth,
            &binding,
            CancellationToken::new(),
        )
        .await
        {
            Err(failure) => failure,
            Ok(_) => {
                panic!("changed private target cell must invalidate the confirmed observation")
            }
        };
        assert_eq!(
            failure.failure.cause,
            crate::store::pinning::management::ManagementError::Changed
        );
        assert!(
            failure
                .transaction
                .is_some_and(|failure| failure.rollback_confirmed)
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Id.like("management-observe:%"))
                .all(fixture.store.db())
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            pin_job::Entity::find_by_id(&original_job_id)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "error"
        );
        fixture.assert_held().await;
        no_delete(&target).await;
        fixture.store.db().clone().close().await.unwrap();
        return;
    }
    let applied = crate::store::pinning::management::actions::apply_cancellable(
        fixture.store.db(), &config, &auth, &binding, CancellationToken::new(),
    ).await.unwrap_or_else(|failure| {
        let states = failure.failure.items.iter().map(|item| (&item.item_id, item.state)).collect::<Vec<_>>();
        panic!("readonly apply failed after checked confirmation: cause={:?}; transaction={:?}; items={states:?}; plan={:?}; target={}", failure.failure.cause, failure.transaction, plan.items()[0].status, target_row.state)
    });
    let authority = applied.items[0]
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::ReadOnlyObserve)
        .unwrap();
    assert_eq!(
        authority.resource_id.as_deref(),
        explicit_resource.then_some(canonical_request.as_str())
    );
    let scope = ManagementJobScope::from_applied(fixture.store.db(), &auth, &binding)
        .await
        .unwrap();
    let report = management_tick(
        &fixture.coordinator,
        &fixture.store,
        &scope,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(report.items[0].state, records::ActionState::ReadOnly);
    assert!(report.replica_observations.iter().any(|fact| {
        fact.job_id == authority.job_id
            && fact.phase == "reconcile"
            && fact.outcome == "ReportedPinned"
            && !fact.projectable
    }));

    let recovery = pin_job::Entity::find_by_id(&original_job_id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovery.state, "error");
    assert!(recovery.locked_until.is_none());
    assert_eq!(
        pin_lease_target::Entity::find_by_id(recovery.target_id.clone().unwrap())
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .state,
        "error"
    );
    assert!(fixture.claim().await.is_none());
    let receipt = fixture.evidence().await.pop().unwrap();
    assert_eq!(
        (receipt.effect.as_str(), receipt.outcome.as_str()),
        if late_matched {
            ("observed", "matched")
        } else {
            ("unknown", "unknown")
        }
    );
    assert_eq!(receipt.resources, original_receipt.resources);
    assert_eq!(receipt.observed_at, original_receipt.observed_at);
    assert!(!receipt.needs_attention);
    assert_eq!(
        ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
            .await
            .unwrap(),
        ledger::submission::AdmissionBarrier::Clear
    );
    let final_report = scope.report(&fixture.store).await.unwrap();
    assert_eq!(final_report.items[0].state, records::ActionState::ReadOnly);
    pin_replica_observation::Entity::insert(pin_replica_observation::ActiveModel {
        sequence: Default::default(),
        job_id: Set(original_job_id.clone()),
        provider: Set(fixture.provider.clone()),
        expected_cid: Set(CID.into()),
        route: Set(receipt.route.clone()),
        remote_epoch: Set(receipt.remote_epoch),
        claim_until: Set(Utc::now()),
        lease_id: Set(recovery.lease_id.clone()),
        target_id: Set(recovery.target_id.clone()),
        expected_generation: Set(recovery.expected_generation),
        phase: Set("submit".into()),
        resources: Set("[]".into()),
        outcome: Set("Unknown".into()),
        safe_error: Set(Some("late conflicting read".into())),
        projectable: Set(false),
        observed_at: Set(Utc::now()),
    })
    .exec(fixture.store.db())
    .await
    .unwrap();
    assert_eq!(
        ledger::submission::admission_barrier(fixture.store.db(), &fixture.provider)
            .await
            .unwrap(),
        ledger::submission::AdmissionBarrier::Operator
    );
    assert_eq!(
        scope.report(&fixture.store).await.unwrap().items[0].state,
        records::ActionState::NeedsAttention
    );
    assert!(fixture.claim().await.is_none());
    assert_eq!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
    fixture.assert_held().await;
    no_delete(&target).await;
    fixture.store.db().clone().close().await.unwrap();
}

#[tokio::test]
async fn cid_unknown_with_extra_root_never_enters_read_recovery() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID,OTHER]})),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    fixture.run(fixture.claim().await.unwrap()).await.unwrap();
    let receipt = fixture.evidence().await.pop().unwrap();
    assert_eq!(receipt.outcome, "cid_mismatch");
    assert!(receipt.needs_attention);
    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    assert!(fixture.claim().await.is_none());
    assert!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() == "/api/v0/pin/add")
    );
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn absent_cid_recovery_exhausts_original_budget_without_claiming_not_submitted() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&target)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
            "Message": format!("path '{CID}' is not pinned"),
            "Code": 0,
            "Type": "error"
        })))
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let claim = fixture.claim().await.unwrap();
    let job_id = claim.model.id.clone();
    fixture.run(claim).await.unwrap();
    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    for _ in 0..jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS {
        let claim = fixture
            .claim()
            .await
            .expect("each remaining original recovery opportunity must become due");
        fixture.run(claim).await.unwrap();
    }
    let receipt = fixture.evidence().await.pop().unwrap();
    assert_eq!(
        (receipt.effect.as_str(), receipt.outcome.as_str()),
        ("unknown", "unknown")
    );
    assert!(receipt.needs_attention);
    let history = jobs::submission_history(fixture.store.db(), &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.submit_calls, 1);
    assert_eq!(history.recovery_queries, jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS);
    let terminal = pin_job::Entity::find_by_id(&job_id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state, "error");
    assert!(terminal.locked_until.is_none());
    assert_eq!(
        (history.effect.as_str(), history.state.as_str()),
        ("unknown", "needs_attention")
    );
    for _ in 0..2 {
        recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
            .await
            .unwrap();
        assert!(fixture.claim().await.is_none());
    }
    let requests = target.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/ls")
            .count(),
        usize::try_from(jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS).unwrap()
    );
    assert!(
        pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.eq(job_id))
            .all(fixture.store.db())
            .await
            .unwrap()
            .iter()
            .all(|fact| !fact.projectable && fact.outcome == "Unknown")
    );
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn zero_call_submit_with_no_receipt_resets_before_one_fresh_post() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let mut claim = fixture.claim().await.unwrap();
    let job_id = claim.model.id.clone();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::SubmitPhase,
            sea_orm::sea_query::Expr::value("recovering"),
        )
        .filter(pin_job::Column::Id.eq(&job_id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    claim.model.submit_phase = Some("recovering".into());
    fixture.run(claim).await.unwrap();
    assert!(
        jobs::submission_history(fixture.store.db(), &job_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(target.received_requests().await.unwrap().is_empty());
    let reset = fixture
        .claim()
        .await
        .expect("zero-call proof must restore ready work");
    assert_eq!(reset.model.submit_phase.as_deref(), Some("ready"));
    fixture.run(reset).await.unwrap();
    assert_eq!(fixture.evidence().await[0].outcome, "matched");
    let history = jobs::submission_history(fixture.store.db(), &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.submit_calls, 1);
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn maintenance_resets_only_exact_zero_call_missing_receipt_placeholder() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[CID]})))
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let mut claim = fixture.claim().await.unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::SubmitPhase,
            sea_orm::sea_query::Expr::value("recovering"),
        )
        .filter(pin_job::Column::Id.eq(&claim.model.id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    claim.model.submit_phase = Some("recovering".into());
    jobs::history::Entity::insert(jobs::history::ActiveModel {
        job_id: Set(claim.model.id.clone()),
        correlation: Set(None),
        api: Set("unknown".into()),
        strategy: Set("unknown".into()),
        effect: Set("unknown".into()),
        state: Set("needs_attention".into()),
        first_error: Set(Some(jobs::RPC_MISSING_RECEIPT_PARK_REASON.into())),
        last_error: Set(Some(jobs::RPC_MISSING_RECEIPT_PARK_REASON.into())),
        submit_calls: Set(0),
        recovery_queries: Set(0),
        started_at: Set(Utc::now()),
    })
    .exec(fixture.store.db())
    .await
    .unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            sea_orm::sea_query::Expr::value(Option::<DateTime<Utc>>::None),
        )
        .col_expr(
            pin_job::Column::LastError,
            sea_orm::sea_query::Expr::value(jobs::RPC_MISSING_RECEIPT_PARK_REASON),
        )
        .filter(pin_job::Column::Id.eq(&claim.model.id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let history = jobs::submission_history(fixture.store.db(), &claim.model.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((history.submit_calls, history.recovery_queries), (0, 0));
    recover_missing_receipt_parks(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    assert!(
        jobs::submission_history(fixture.store.db(), &claim.model.id)
            .await
            .unwrap()
            .is_none()
    );
    let reset = fixture
        .claim()
        .await
        .expect("exact placeholder must be restored");
    assert_eq!(reset.model.submit_phase.as_deref(), Some("ready"));
    fixture.run(reset).await.unwrap();
    assert_eq!(fixture.evidence().await[0].outcome, "matched");
    assert_eq!(
        target
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/api/v0/pin/add")
            .count(),
        1
    );
    no_delete(&target).await;
}

#[tokio::test]
async fn committed_submit_call_without_receipt_never_resets_or_posts_again() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let fixture = fixture(target.uri(), &source, RpcProfile::Kubo, RpcStrategy::Cid).await;
    let mut claim = fixture.claim().await.unwrap();
    let job_id = claim.model.id.clone();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::SubmitPhase,
            sea_orm::sea_query::Expr::value("recovering"),
        )
        .filter(pin_job::Column::Id.eq(&job_id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    claim.model.submit_phase = Some("recovering".into());
    jobs::record_submit_invocation(fixture.store.db(), &claim, "kubo", "cid", Utc::now())
        .await
        .unwrap();
    fixture.run(claim).await.unwrap();
    let parked = pin_job::Entity::find_by_id(&job_id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parked.state, "error");
    assert!(parked.locked_until.is_none());
    assert_eq!(
        jobs::submission_history(fixture.store.db(), &parked.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
    recover_missing_receipt_parks(&fixture.coordinator, &fixture.store, &mut String::new())
        .await
        .unwrap();
    assert!(fixture.claim().await.is_none());
    assert!(target.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn worker_explicit_not_submitted_retries_only_after_a_safe_source_failure() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&source)
        .await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    fixture.run(fixture.claim().await.unwrap()).await.unwrap();
    assert_eq!(fixture.evidence().await[0].outcome, "not_submitted");
    assert!(target.received_requests().await.unwrap().is_empty());
    source.reset().await;
    source_file(&source).await;
    fixture.drain().await;
    let evidence = fixture.evidence().await;
    assert_eq!(evidence.len(), 2);
    assert_eq!(evidence[1].outcome, "matched");
    assert!(!evidence.iter().any(|row| row.needs_attention));
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_observation_write_failure_recovers_to_attention_without_a_second_post() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    fixture.store.db().execute_unprepared("CREATE TRIGGER fail_receipt BEFORE UPDATE OF resources ON pin_submit_observations BEGIN SELECT RAISE(ABORT, 'receipt failure'); END").await.unwrap();
    let claim = fixture.claim().await.unwrap();
    let id = claim.model.id.clone();
    assert!(fixture.run(claim).await.is_err());
    let evidence = fixture.evidence().await;
    assert_eq!(evidence[0].outcome, "in_flight");
    assert!(evidence[0].needs_attention);
    fixture
        .store
        .db()
        .execute_unprepared("DROP TRIGGER fail_receipt")
        .await
        .unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Some(Utc::now() - ChronoDuration::seconds(1))),
        )
        .filter(pin_job::Column::Id.eq(&id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let reclaimed = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        ChronoDuration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert!(reclaimed.reclaimed);
    fixture.run(reclaimed).await.unwrap();
    assert!(
        jobs::submission_history(fixture.store.db(), &id)
            .await
            .unwrap()
            .is_some_and(|h| h.state == "needs_attention" && h.submit_calls == 1)
    );
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_changed_historical_route_refuses_io_without_guessing_an_endpoint() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    let old = fixture
        .coordinator
        .provider_identity(&fixture.provider)
        .unwrap()
        .clone();
    let mut changed = old.clone();
    changed.endpoint_revision += 1;
    ledger::register_route(fixture.store.db(), &fixture.provider, &changed)
        .await
        .unwrap();
    // Publication also creates resource-scoped coordination jobs. Claim through
    // production code, then exercise this test's Submit rather than whichever
    // same-time job happens to be first in UUID order.
    let claim = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        ChronoDuration::seconds(30),
        10,
    )
    .await
    .unwrap()
    .into_iter()
    .find(|claim| claim.model.operation == "submit")
    .expect("publication must have enqueued a real Submit");
    let submit_id = claim.model.id.clone();
    fixture.run(claim).await.unwrap();
    let historical = ledger::get(fixture.store.db(), &fixture.provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ledger::decode_route(&historical).unwrap(),
        old.route_snapshot()
    );
    assert!(target.received_requests().await.unwrap().is_empty());
    assert!(source.received_requests().await.unwrap().is_empty());
    // Final dispatch preflight rejects the changed registration and records
    // explicit non-dispatch under the original route, before finishing the job.
    let evidence = fixture.evidence().await;
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].job_id, submit_id);
    assert_eq!(evidence[0].outcome, "not_submitted");
    assert_eq!(evidence[0].effect, "not_created");
    assert!(!evidence[0].needs_attention);
    assert_eq!(evidence[0].resources, "[]");
    assert_eq!(
        serde_json::from_str::<crate::pinning::identity::ProviderRouteSnapshot>(&evidence[0].route)
            .unwrap(),
        old.route_snapshot()
    );
    fixture.assert_held().await;
}

#[tokio::test]
async fn worker_projection_failure_cannot_rollback_actual_mismatch_resources() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    fixture.store.db().execute_unprepared("CREATE TRIGGER fail_projection BEFORE UPDATE OF state ON pin_submit_history WHEN NEW.state='needs_attention' BEGIN SELECT RAISE(ABORT, 'projection failure'); END").await.unwrap();
    let claim = fixture.claim().await.unwrap();
    let id = claim.model.id.clone();
    assert!(fixture.run(claim).await.is_err());
    let evidence = fixture.evidence().await;
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].outcome, "cid_mismatch");
    assert!(evidence[0].resources.contains(OTHER));
    assert!(evidence[0].needs_attention);
    fixture
        .store
        .db()
        .execute_unprepared("DROP TRIGGER fail_projection")
        .await
        .unwrap();
    pin_job::Entity::update_many()
        .col_expr(
            pin_job::Column::LockedUntil,
            Expr::value(Some(Utc::now() - ChronoDuration::seconds(1))),
        )
        .filter(pin_job::Column::Id.eq(&id))
        .exec(fixture.store.db())
        .await
        .unwrap();
    let reclaimed = jobs::claim_due_jobs(
        fixture.store.db(),
        Utc::now(),
        ChronoDuration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    fixture.run(reclaimed).await.unwrap();
    assert_eq!(fixture.evidence().await[0].outcome, "cid_mismatch");
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_release_context_read_failure_cannot_erase_completed_rpc_response() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
    )
    .await;
    let claim = fixture.claim().await.unwrap();
    let dispatched = dispatch_submit(
        &fixture.store,
        &fixture.coordinator,
        &Arc::new(Semaphore::new(2)),
        claim,
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .unwrap();
    let job_id = dispatched.claimed.model.id.clone();
    // A malformed persisted epoch makes release-context decoding fail, without
    // changing the invocation, the receipt table, or the current lifecycle.
    fixture.store.db().execute( sea_orm::Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_resource_history (provider,cid,epoch,ledger) VALUES ($1,$2,'invalid-epoch','{}')",
        [fixture.provider.clone().into(), CID.into()],
    )).await.unwrap();
    assert!(
        finish_submit_observation(
            &fixture.store,
            &fixture.coordinator,
            dispatched.claimed,
            dispatched.observation,
            dispatched.capture,
            dispatched.rpc_invocation
        )
        .await
        .is_err()
    );
    let evidence = ledger::submission::latest(fixture.store.db(), &job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.outcome, "matched");
    assert!(evidence.observed_at.is_some() && evidence.needs_attention);
    let resources: Vec<ledger::submission::ResourceEvidence> =
        serde_json::from_str(&evidence.resources).unwrap();
    assert_eq!(resources[0].resource.cid, CID);
    assert_ne!(
        remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.into()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pinned"
    );
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
async fn scheduler_receipt_candidates_cannot_authorize_stale_lifetime_retired_provider_or_live_claim()
 {
    for boundary in ["generation", "retired", "live_claim"] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        source_file(&source).await;
        recursive(&target).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
            )
            .expect(1)
            .mount(&target)
            .await;
        let fixture = fixture(
            target.uri(),
            &source,
            RpcProfile::Filebase,
            RpcStrategy::Upload,
        )
        .await;
        let claim = fixture.claim().await.unwrap();
        let dispatched = dispatch_submit(
            &fixture.store,
            &fixture.coordinator,
            &Arc::new(Semaphore::new(2)),
            claim,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .unwrap();
        let job_id = dispatched.claimed.model.id.clone();
        let tx = fixture.store.db().begin().await.unwrap();
        jobs::park_submit(
            &tx,
            &dispatched.claimed,
            "needs_attention",
            jobs::RPC_RECOVERY_PARK_REASON,
            Utc::now(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let evidence = ledger::submission::complete_fact(
            &fixture.store,
            dispatched.rpc_invocation.as_ref().unwrap(),
            &dispatched.observation,
            Utc::now(),
        )
        .await
        .unwrap();
        match boundary {
            "generation" => {
                pin_lease::Entity::update_many()
                    .col_expr(
                        pin_lease::Column::Generation,
                        Expr::value(dispatched.claimed.model.expected_generation.unwrap() + 1),
                    )
                    .filter(
                        pin_lease::Column::Id.eq(dispatched
                            .claimed
                            .model
                            .lease_id
                            .clone()
                            .unwrap()),
                    )
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            "retired" => {
                let tx = fixture.store.db().begin().await.unwrap();
                pin_provider_route::Entity::update_many()
                    .col_expr(pin_provider_route::Column::Retired, Expr::value(true))
                    .filter(pin_provider_route::Column::Provider.eq(&fixture.provider))
                    .exec(&tx)
                    .await
                    .unwrap();
                crate::store::pinning::management::records::locator::rebuild_source(
                    &tx,
                    &crate::store::pinning::management::native::NativeKey {
                        table: "pin_provider_routes".into(),
                        values: vec![fixture.provider.clone()],
                    },
                    &mut crate::store::pinning::management::native::meter::Budget::default(),
                )
                .await
                .unwrap();
                tx.commit().await.unwrap();
            }
            "live_claim" => {
                pin_job::Entity::update_many()
                    .col_expr(
                        pin_job::Column::LockedUntil,
                        Expr::value(Some(Utc::now() + ChronoDuration::minutes(5))),
                    )
                    .filter(pin_job::Column::Id.eq(&job_id))
                    .exec(fixture.store.db())
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = pin_job::Entity::find_by_id(&job_id)
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap();
        recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut String::new())
            .await
            .unwrap();
        assert_eq!(
            pin_job::Entity::find_by_id(&job_id)
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap(),
            before,
            "{boundary}"
        );
        assert_eq!(fixture.evidence().await, vec![evidence]);
        assert_ne!(
            remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.into()))
                .one(fixture.store.db())
                .await
                .unwrap()
                .unwrap()
                .status,
            "pinned"
        );
        fixture.assert_held().await;
        no_delete(&target).await;
    }
}

#[tokio::test]
async fn scheduler_keeps_terminal_receipt_debt_without_repeating_provider_post() {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    observed_terminal_receipt(db).await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_warm_rpc_fact_park_and_recovery_do_not_revalidate_structure() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("explicit test PostgreSQL URL");
    let admin = sea_orm::Database::connect(&url).await.unwrap();
    let schema = format!("rpc_warm_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .max_connections(4)
        .set_schema_search_path(schema.clone());
    let db = sea_orm::Database::connect(options).await.unwrap();
    let outcome = std::panic::AssertUnwindSafe(observed_terminal_receipt(db.clone()))
        .catch_unwind()
        .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn observed_terminal_receipt(mut db: sea_orm::DatabaseConnection) {
    let trace = crate::sql_trace::SqlTrace::default();
    trace.attach(&mut db);
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let fixture = fixture_on_database(
        target.uri(),
        &source,
        RpcProfile::Filebase,
        RpcStrategy::Upload,
        db,
    )
    .await;
    trace.take("RPC cold fixture", false);
    let claim = fixture.claim().await.unwrap();
    let dispatched = dispatch_submit(
        &fixture.store,
        &fixture.coordinator,
        &Arc::new(Semaphore::new(2)),
        claim,
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .unwrap();
    let job_id = dispatched.claimed.model.id.clone();
    jobs::park_submit_store(
        &fixture.store,
        &dispatched.claimed,
        "needs_attention",
        jobs::RPC_RECOVERY_PARK_REASON,
        Utc::now(),
    )
    .await
    .unwrap();
    let receipt = ledger::submission::complete_fact(
        &fixture.store,
        dispatched.rpc_invocation.as_ref().unwrap(),
        &dispatched.observation,
        chrono::DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap(),
    )
    .await
    .unwrap();
    let mut cursor = String::new();
    let parked = pin_job::Entity::find_by_id(&job_id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parked.state, "error");
    assert!(parked.locked_until.is_none());
    assert_eq!(
        ledger::submission::latest(fixture.store.db(), &job_id)
            .await
            .unwrap()
            .unwrap(),
        receipt
    );
    recover_parked_receipts(&fixture.coordinator, &fixture.store, &mut cursor)
        .await
        .unwrap();
    let due = pin_job::Entity::find_by_id(&job_id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(due.state, "error");
    assert!(fixture.claim().await.is_none());
    fixture.drain().await;
    trace.take("RPC warm terminal receipt maintenance", true);
    let settled = fixture.evidence().await;
    assert_eq!(settled[0].resources, receipt.resources);
    assert_eq!(settled[0].observed_at, receipt.observed_at);
    assert!(settled[0].needs_attention);
    fixture.assert_held().await;
    no_delete(&target).await;
}

#[tokio::test]
#[ignore = "opt-in: requires a new authorized isolated IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL"]
async fn postgres_shared_cid_receipt_projection_orders_all_leases_before_remote() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_RPC_RECOVERY_POSTGRES_URL")
        .expect("authorized isolated PostgreSQL endpoint required");
    let parsed = url::Url::parse(&url).unwrap();
    assert!(matches!(parsed.scheme(), "postgres" | "postgresql"));
    assert!(matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1")
    ));
    assert_ne!(parsed.port(), Some(55467));
    let admin = store::connect_database(&url).await.unwrap();
    let schema = format!("rpc_frontier_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options.set_schema_search_path(schema.clone());
    let db = sea_orm::Database::connect(options.clone()).await.unwrap();
    let peer = sea_orm::Database::connect(options).await.unwrap();
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n")),
        )
        .expect(1)
        .mount(&target)
        .await;
    let mut completion = None;
    let result = std::panic::AssertUnwindSafe(async {
        let fixture = Arc::new(fixture_on_database(target.uri(), &source, RpcProfile::Filebase, RpcStrategy::Upload, db.clone()).await);
        fixture.publish("shared", CID).await;
        let claimed = fixture.claim().await.unwrap();
        let original_lease = claimed.model.lease_id.clone().unwrap();
        let other = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::Cid.eq(CID))
            .filter(pin_lease_target::Column::LeaseId.ne(original_lease))
            .one(fixture.store.db()).await.unwrap().unwrap();
        assert_eq!(other.state, "waiting");
        let dispatched = dispatch_submit(&fixture.store, &fixture.coordinator, &Arc::new(Semaphore::new(2)), claimed, &CancellationToken::new()).await.unwrap().unwrap();
        let peer_tx = peer.begin().await.unwrap();
        let peer_pid = peer_tx.query_one(sea_orm::Statement::from_string(DatabaseBackend::Postgres, "SELECT pg_backend_pid() AS pid")).await.unwrap().unwrap().try_get::<i32>("", "pid").unwrap();
        peer_tx.query_one(sea_orm::Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT id FROM pin_leases WHERE id=$1 FOR UPDATE", [other.lease_id.clone().into()])).await.unwrap();
        let task_fixture = fixture.clone();
        completion = Some(tokio::spawn(async move {
            finish_submit_observation(&task_fixture.store, &task_fixture.coordinator, dispatched.claimed, dispatched.observation, dispatched.capture, dispatched.rpc_invocation).await
        }));
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let waiting = db.query_one(sea_orm::Statement::from_sql_and_values(DatabaseBackend::Postgres,
                    "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE $1=ANY(pg_catalog.pg_blocking_pids(a.pid))) AS waiting", [peer_pid.into()])).await.unwrap().unwrap().try_get::<bool>("", "waiting").unwrap();
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("receipt projection must reach the other lease's lock");
        peer_tx.query_one(sea_orm::Statement::from_sql_and_values(DatabaseBackend::Postgres,
            "SELECT cid FROM remote_pins WHERE provider=$1 AND cid=$2 FOR UPDATE NOWAIT", [fixture.provider.clone().into(), CID.into()])).await.expect("receipt replay must not hold the remote while waiting for another lease");
        // Ending the other lease must still acquire the remote. A projection
        // that holds that remote while waiting for this lease forms a cycle.
        assert_eq!(tokio::time::timeout(Duration::from_secs(15), leases::cancel_lease(&peer_tx, &other.lease_id, Utc::now())).await.expect("shared CID lifecycle lock cycle").unwrap(), leases::GenerationDecision::Current);
        peer_tx.commit().await.unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(15), completion.take().unwrap()).await.unwrap().unwrap();
        let receipt = fixture.evidence().await;
        assert_eq!(receipt[0].outcome, "matched");
        if completed.is_err() {
            assert!(receipt[0].needs_attention, "a stale lifecycle snapshot must retain attention");
            assert_ne!(remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.into())).one(fixture.store.db()).await.unwrap().unwrap().status, "pinned");
        }
        fixture.assert_held().await;
        no_delete(&target).await;
    }).catch_unwind().await;
    if let Some(task) = completion {
        task.abort();
        let _ = task.await;
    }
    peer.close().await.unwrap();
    db.close().await.unwrap();
    let suffix = schema.strip_prefix("rpc_frontier_").unwrap();
    assert!(suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit()));
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn worker_late_receipt_records_original_epoch_but_cannot_publish_into_released_lifetime() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    source_file(&source).await;
    recursive(&target).await;
    let submitted = Arc::new(tokio::sync::Notify::new());
    let notify = submitted.clone();
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(move |_: &wiremock::Request| {
            notify.notify_one();
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{CID}\"}}\n"))
                .set_delay(Duration::from_millis(300))
        })
        .expect(1)
        .mount(&target)
        .await;
    let fixture = Arc::new(
        fixture(
            target.uri(),
            &source,
            RpcProfile::Filebase,
            RpcStrategy::Upload,
        )
        .await,
    );
    let claim = fixture.claim().await.unwrap();
    let id = claim.model.id.clone();
    let worker = {
        let fixture = fixture.clone();
        tokio::spawn(async move { fixture.run(claim).await })
    };
    tokio::time::timeout(Duration::from_secs(5), submitted.notified())
        .await
        .expect("claimed RPC worker did not dispatch the fixture request");
    let captured = crate::store::entities::pin_invocation_route::Entity::find_by_id(id)
        .one(fixture.store.db())
        .await
        .unwrap()
        .unwrap();
    pin_resource_history::Entity::insert(pin_resource_history::ActiveModel {
        provider: Set(fixture.provider.clone()),
        cid: Set(CID.into()),
        epoch: Set(captured.remote_epoch),
        ledger: Set("{}".into()),
    })
    .exec(fixture.store.db())
    .await
    .unwrap();
    remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Epoch,
            Expr::value(captured.remote_epoch + 1),
        )
        .filter(remote_pin::Column::Provider.eq(&fixture.provider))
        .exec(fixture.store.db())
        .await
        .unwrap();
    worker.await.unwrap().unwrap();
    let evidence = fixture.evidence().await;
    assert_eq!(evidence[0].remote_epoch, captured.remote_epoch);
    assert_eq!(evidence[0].route, captured.route);
    assert!(evidence[0].needs_attention);
    assert!(evidence[0].resources.contains(CID));
    assert_ne!(
        remote_pin::Entity::find_by_id((fixture.provider.clone(), CID.into()))
            .one(fixture.store.db())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pinned"
    );
    no_delete(&target).await;
}

#[tokio::test]
async fn worker_hash_then_actual_http_error_trailer_persists_reported_root_not_success() {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    let source = MockServer::start().await;
    source_file(&source).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let fixture = fixture(endpoint, &source, RpcProfile::Filebase, RpcStrategy::Upload).await;
    let server = tokio::spawn(async move {
        let interaction = async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                headers.push_str(&line);
                if line == "\r\n" {
                    break;
                }
                assert!(headers.len() < 16 * 1024);
            }
            let headers = headers.to_ascii_lowercase();
            assert!(headers.starts_with("post /api/v0/add?"));
            if headers.contains("transfer-encoding: chunked") {
                loop {
                    let mut line = String::new();
                    socket.read_line(&mut line).await.unwrap();
                    let length =
                        usize::from_str_radix(line.trim().split(';').next().unwrap(), 16).unwrap();
                    assert!(length < 1024 * 1024);
                    if length == 0 {
                        let mut end = String::new();
                        socket.read_line(&mut end).await.unwrap();
                        assert_eq!(end, "\r\n");
                        break;
                    }
                    socket.read_exact(&mut vec![0; length + 2]).await.unwrap();
                }
            } else if let Some(length) = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
            {
                let length = length.trim().parse::<usize>().unwrap();
                assert!(length < 1024 * 1024);
                socket.read_exact(&mut vec![0; length]).await.unwrap();
            }
            let record = format!("{{\"Hash\":\"{CID}\"}}\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:X}\r\n{record}\r\n0\r\nX-Stream-Error: private-trailer-marker\r\n\r\n",
                record.len()
            );
            socket
                .get_mut()
                .write_all(response.as_bytes())
                .await
                .unwrap();
            socket.get_mut().flush().await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(10), interaction)
            .await
            .expect("RPC trailer fixture did not receive and finish a request");
    });
    fixture.drain().await;
    server.await.unwrap();
    let evidence = fixture.evidence().await;
    assert_eq!(evidence[0].effect, "unknown");
    assert!(evidence[0].needs_attention);
    let roots: Vec<ledger::submission::ResourceEvidence> =
        serde_json::from_str(&evidence[0].resources).unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].resource.cid, CID);
    assert_eq!(
        roots[0].resource.status,
        crate::pinning::provider::ObservedResourceStatus::Reported
    );
    assert!(
        !evidence[0]
            .safe_error
            .as_deref()
            .unwrap()
            .contains("private-trailer-marker")
    );
    fixture.assert_held().await;
}
