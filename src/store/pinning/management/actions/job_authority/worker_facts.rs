//! Structured adapter observations exercised through the real managed worker.
use super::*;
use crate::pinning::{coordinator::PinningCoordinator, provider::*};
use crate::store::{Store, entities::pin_replica_observation};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct TypedProvider {
    resources: Vec<ObservedResource>,
    submit_count: AtomicUsize,
    get_count: AtomicUsize,
    poll_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    poll_error: bool,
}

#[async_trait::async_trait]
impl PinningProvider for TypedProvider {
    fn name(&self) -> &str {
        "typed-controlled"
    }
    fn invocation_route(&self) -> (&'static str, &'static str) {
        ("noop", "cid")
    }
    async fn submit(&self, request: SubmitPin) -> std::result::Result<RemotePin, ProviderError> {
        self.submit_observed(request).await.result
    }
    async fn submit_observed(&self, request: SubmitPin) -> SubmitObservation {
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        SubmitObservation {
            result: Ok(RemotePin {
                cid: request.cid,
                request_id: "typed-original-request".into(),
                status: RemotePinStatus::Queued,
                raw_status: "queued".into(),
                failure_reason: None,
            }),
            resources: self.resources.clone(),
            effect: SubmitEffect::Observed,
        }
    }
    async fn get(&self, id: &str) -> std::result::Result<RemotePin, ProviderError> {
        self.get_observed(id).await.result
    }
    async fn get_observed(&self, id: &str) -> PinObservation {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        if let Some((entered, resume)) = &self.poll_gate {
            entered.notify_one();
            resume.notified().await;
        }
        let result = if self.poll_error {
            Err(ProviderError {
                class: ProviderErrorClass::Protocol,
                message: "controlled partial report".into(),
                retry_after: Some(std::time::Duration::from_secs(7)),
            })
        } else {
            Ok(RemotePin {
                cid: self.resources[0].cid.clone(),
                request_id: id.into(),
                status: RemotePinStatus::Pinned,
                raw_status: "pinned".into(),
                failure_reason: None,
            })
        };
        PinObservation {
            result,
            resources: self.resources.clone(),
        }
    }
    async fn get_observed_historical(&self, id: &str, api: &str, strategy: &str) -> PinObservation {
        assert_eq!((api, strategy), self.invocation_route());
        self.get_observed(id).await
    }
    async fn find(&self, _: FindPin) -> std::result::Result<Vec<RemotePin>, ProviderError> {
        panic!("managed Poll must not issue Find")
    }
    async fn unpin(&self, _: &str) -> std::result::Result<(), ProviderError> {
        panic!("managed observation must not issue Unpin")
    }
}

fn provider(
    cid: &str,
    gate: Option<(Arc<Notify>, Arc<Notify>)>,
    poll_error: bool,
) -> Arc<TypedProvider> {
    Arc::new(TypedProvider {
        resources: vec![ObservedResource {
            resource_type: crate::pinning::identity::RemoteResourceType::PsaRequest,
            cid: cid.into(),
            request_id: "typed-original-request".into(),
            status: ObservedResourceStatus::RecursiveVerified,
            ownership: crate::pinning::identity::Ownership::ApplicationCreated,
            replica_evidence: None,
        }],
        submit_count: AtomicUsize::new(0),
        get_count: AtomicUsize::new(0),
        poll_gate: gate,
        poll_error,
    })
}

struct Fixture {
    _directory: tempfile::TempDir,
    _server: wiremock::MockServer,
    db: sea_orm::DatabaseConnection,
    config: ManagementConfig,
    auth: records::LocalAuthorization,
    binding: records::ConfirmedBinding,
}

async fn fixture(plan_id: &str) -> Fixture {
    use crate::pinning::management::*;
    use crate::store::pinning::publication::{
        self, PinTargetSpec, PublicationObject, PublicationRequest,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };
    let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":cid})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let mut raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="fixture"
        kind="noop"
        priority=1
        max_bytes=1000000
        max_pins=1000
        [[pinning.policies]]
        bucket="bucket"
        prefix=""
        trigger="request"
        provider_mode="all"
        providers=["fixture"]
        default_duration="1h"
        max_duration="2h"
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="fixture"
        provider_id="fixture-provider"
        display_name="Fixture"
        backend="noop"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        api_profile="noop"
        strategy="cid"
        cleanup="retain"
    "#,
    )
    .unwrap();
    raw.kubo.rpc_url = server.uri();
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(cid.into())
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("worker-facts.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = crate::store::connect_database(&url).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let provider = &config.pinning.providers[0];
    ledger::register_route(&db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.clone(),
                "bucket",
                "exact",
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
            policy: crate::pinning::policy::PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 10,
            },
        },
        &config.pinning.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&id))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "exact".into(),
        object_id: id,
        private_version_id: version.id,
        public_version: version
            .version_id
            .map_or(PublicVersionId::Null, PublicVersionId::Version),
        cid: canonical_resource_cid(cid).unwrap(),
    };
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &crate::kubo::KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: cid.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let now = Utc::now().timestamp();
    let request = PlanRequest {
        plan_id: plan_id.into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action: Action::Create(Intent {
                started_at: now - 1,
                expires_at: now + 3600,
                requirements: Requirements {
                    mode: ProviderMode::All,
                    provider_ids: vec![provider.identity.provider_id.clone()],
                    levels: vec![ConfirmationLevel::RemotePinned],
                    min_replicas: 1,
                    max_age_secs: None,
                    policy_revision: config.pinning.policies[0].identity.clone(),
                },
            }),
            subject: Subject::Object {
                identity,
                target: TargetSelection::New(RouteIdentity::from_original(
                    provider.route_snapshot(),
                )),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    Fixture {
        _directory: directory,
        _server: server,
        db,
        config,
        auth,
        binding,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noncluster_poll_keeps_actual_typed_resources_and_error_facts_after_claim_loss() {
    use sea_orm::Statement;
    for late in [false, true] {
        let fixture = fixture(if late {
            "typed-claim-loss"
        } else {
            "typed-poll"
        })
        .await;
        let applied = super::super::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding,
        )
        .await
        .unwrap();
        let seed = applied.items[0]
            .jobs
            .iter()
            .find(|job| job.directive == records::JobDirective::Submit)
            .unwrap();
        let entered = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let provider = provider(
            &seed.cid,
            late.then(|| (entered.clone(), resume.clone())),
            late,
        );
        let mut coordinator = PinningCoordinator::build_with_kubo(
            fixture.config.pinning.clone(),
            Some(crate::kubo::KuboClient::new(
                fixture.config.source_endpoint.clone(),
            )),
        )
        .unwrap();
        PinningCoordinator::replace_provider_for_test(
            &mut coordinator,
            &seed.provider,
            provider.clone(),
        );
        let store = Store::new(fixture.db.clone());
        let scope = crate::pinning::worker::ManagementJobScope::from_applied(
            &fixture.db,
            &fixture.auth,
            &fixture.binding,
        )
        .await
        .unwrap();
        let (coordinator, store, scope) = tokio::spawn(async move {
            {
                let cancellation = CancellationToken::new();
                std::pin::pin!(coordinator.management_tick(&store, &scope, &cancellation))
                    .await
                    .unwrap();
            }
            (coordinator, store, scope)
        })
        .await
        .unwrap();
        assert_eq!(provider.submit_count.load(Ordering::SeqCst), 1);
        let poll = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "UPDATE pin_jobs SET next_attempt_at=$1 WHERE id=$2",
                [Utc::now().into(), poll.id.clone().into()],
            ))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            coordinator
                .management_tick(&store, &scope, &CancellationToken::new())
                .await
        });
        let mut original_claim = None;
        if late {
            tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
                .await
                .unwrap();
            let job = pin_job::Entity::find_by_id(poll.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            original_claim = job.locked_until;
            fixture
                .db
                .execute(Statement::from_sql_and_values(
                    fixture.db.get_database_backend(),
                    "UPDATE pin_jobs SET locked_until=$1 WHERE id=$2",
                    [
                        (job.locked_until.unwrap() + chrono::Duration::minutes(1)).into(),
                        poll.id.clone().into(),
                    ],
                ))
                .await
                .unwrap();
            resume.notify_one();
        }
        let _result = task.await.unwrap();
        assert_eq!(provider.get_count.load(Ordering::SeqCst), 1);
        let fact = pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.eq(&poll.id))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let resources: Vec<ObservedResource> = serde_json::from_str(&fact.resources).unwrap();
        assert_eq!(resources, provider.resources);
        assert!(!fact.projectable);
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        if late {
            assert_eq!(fact.claim_until, original_claim.unwrap());
            assert_eq!(remote.status, "queued");
            let evidence: serde_json::Value =
                serde_json::from_str(fact.safe_error.as_deref().unwrap()).unwrap();
            assert_eq!(evidence["category"], "Protocol");
            assert_eq!(evidence["retry_after_seconds"], 7);
        } else {
            assert_eq!(remote.status, "pinned");
        }
    }
}
