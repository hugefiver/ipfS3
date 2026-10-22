#[path = "stage7_management.rs"]
pub mod base;
pub use base::{CID, NODE};
use ipfs_s3_gateway::{
    config::Config, kubo::KuboClient, pinning::management::*, store::pinning::management::records,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub db: DatabaseConnection,
    pub server: MockServer,
    pub raw: Config,
    pub config: local_config::ManagementConfig,
    pub auth: records::LocalAuthorization,
}

impl Fixture {
    pub async fn new() -> Self {
        Self::new_with_cleanup(false).await
    }

    pub async fn new_with_cleanup(managed: bool) -> Self {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":NODE})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
            .mount(&server)
            .await;
        let mut raw = base::config(&server.uri());
        if managed {
            raw.pinning_identity.providers[0].cleanup =
                ipfs_s3_gateway::pinning::identity::CleanupMode::Managed;
        }
        let config = base::management_config(&raw);
        let (directory, db) = base::database().await;
        base::setup(&db, &config).await;
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        Self {
            directory,
            db,
            server,
            raw,
            config,
            auth,
        }
    }

    pub async fn publish(&self, key: &str, lease: bool) -> ExactObject {
        self.publish_cid(key, base::CID, lease).await
    }

    pub async fn publish_cid(&self, key: &str, cid: &str, lease: bool) -> ExactObject {
        self.publish_cid_with_tags(key, cid, lease, vec![]).await
    }
    pub async fn publish_cid_with_tags(
        &self,
        key: &str,
        cid: &str,
        lease: bool,
        tags: Vec<ipfs_s3_gateway::pinning::tags::ObjectTag>,
    ) -> ExactObject {
        let identity =
            base::publish_cid_with_tags(&self.db, &self.config, key, cid, lease, tags).await;
        evidence::prepare(
            &self.db,
            &self.config,
            &self.auth,
            &KuboClient::new(self.server.uri()),
            &evidence::PrepareRequest {
                record_id: uuid::Uuid::new_v4().to_string(),
                identity: identity.clone(),
                expected_node: NODE.into(),
                max_stored_bytes: 10,
                ttl: chrono::Duration::minutes(15),
                directory: false,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
        identity
    }

    pub fn create_request(
        &self,
        plan_id: &str,
        identity: ExactObject,
        backfill: bool,
    ) -> PlanRequest {
        let mut request = base::request(identity, &self.config, backfill);
        request.plan_id = plan_id.into();
        let policy = &self.config.pinning.policies[0];
        let requirements = match &mut request.items[0].action {
            Action::Create(intent) | Action::Backfill(intent) => &mut intent.requirements,
            _ => unreachable!(),
        };
        requirements.provider_ids = policy
            .providers
            .iter()
            .map(|name| {
                self.config
                    .pinning
                    .providers
                    .iter()
                    .find(|provider| provider.name == *name)
                    .unwrap()
                    .identity
                    .provider_id
                    .clone()
            })
            .collect();
        requirements.mode =
            if policy.provider_mode == ipfs_s3_gateway::pinning::config::ProviderMode::One {
                ProviderMode::One
            } else {
                ProviderMode::All
            };
        request
    }

    pub async fn confirm(&self, request: &PlanRequest) -> records::ConfirmedBinding {
        let snapshot = db_snapshot::collect_with_config(&self.db, &self.config, request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let prepared = retirement::PreparedPlanning::collect(
            &self.db,
            request,
            &snapshot,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        let plan = &prepared.plan;
        assert!(
            plan.items()
                .iter()
                .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_)))
        );
        let ack: Vec<_> = plan
            .items()
            .iter()
            .filter(|entry| entry.status == PlanStatus::Candidate)
            .map(|entry| entry.request.item_id.clone())
            .collect();
        let work = records::SharedDecodeBudget::default();
        let result = records::confirm_with_session(
            &self.db,
            &self.config,
            &self.auth,
            (request, &snapshot, plan),
            &ack,
            chrono::Duration::minutes(5),
            work.clone(),
        )
        .await;
        eprintln!(
            "confirmation {} session={:?}",
            request.plan_id,
            work.lock().unwrap().usage()
        );
        prepared.close().await.unwrap();
        result.unwrap_or_else(|error| {
            panic!("confirmation for {} failed: {error:?}", request.plan_id)
        })
    }

    pub async fn existing_request(
        &self,
        plan_id: &str,
        identity: ExactObject,
        action: Action,
    ) -> PlanRequest {
        let lease = ipfs_s3_gateway::store::entities::pin_lease::Entity::find()
            .filter(
                ipfs_s3_gateway::store::entities::pin_lease::Column::OwnerObjectId
                    .eq(&identity.object_id),
            )
            .one(&self.db)
            .await
            .unwrap()
            .unwrap();
        let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find()
            .filter(
                ipfs_s3_gateway::store::entities::pin_lease_target::Column::LeaseId.eq(&lease.id),
            )
            .one(&self.db)
            .await
            .unwrap()
            .unwrap();
        PlanRequest {
            plan_id: plan_id.into(),
            items: vec![ItemRequest {
                item_id: "object".into(),
                action,
                subject: Subject::Object {
                    identity,
                    target: TargetSelection::Existing {
                        target_id: target.id,
                        source: None,
                    },
                },
            }],
        }
    }

    pub async fn add_provider(&mut self, name: &str) {
        let mut provider = self.raw.pinning.providers[0].clone();
        provider.name = name.into();
        self.raw.pinning.providers.push(provider);
        let mut identity = self.raw.pinning_identity.providers[0].clone();
        identity.config_name = name.into();
        identity.provider_id = format!("{name}-provider");
        identity.scope = name.into();
        identity.storage_domain = name.into();
        self.raw.pinning_identity.providers.push(identity);
        self.raw.pinning.policies[0].providers.push(name.into());
        self.config = base::management_config(&self.raw);
        let provider = self
            .config
            .pinning
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == format!("{name}-provider"))
            .unwrap();
        ipfs_s3_gateway::store::pinning::ledger::register_route(
            &self.db,
            &provider.name,
            &provider.identity,
        )
        .await
        .unwrap();
    }

    pub async fn confirmed_remote(&self, provider: &str, cid: &str) {
        let now = ipfs_s3_gateway::store::database_clock::database_now(&self.db)
            .await
            .unwrap();
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE remote_pins SET status='pinned',request_id=$1 WHERE provider=$2 AND cid=$3",
                [
                    format!("fixture-resource-{}", uuid::Uuid::new_v4()).into(),
                    provider.into(),
                    cid.into(),
                ],
            ))
            .await
            .unwrap();
        self.db.execute(Statement::from_sql_and_values(self.db.get_database_backend(),"UPDATE remote_pin_ledger SET effect='confirmed',ownership='application_created',first_observed_at=$1,last_observed_at=$1,remote_pinned_at=$1 WHERE provider=$2 AND cid=$3",[now.into(),provider.into(),cid.into()])).await.unwrap();
        self.db
            .execute(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                "UPDATE pin_lease_targets SET state='pinned' WHERE provider=$1 AND cid=$2",
                [provider.into(), cid.into()],
            ))
            .await
            .unwrap();
        self.db
            .execute_unprepared("UPDATE pin_jobs SET state='done',locked_until=NULL")
            .await
            .unwrap();
    }

    /// Real controlled Noop adapter + real durable worker/lifecycle. This helper
    /// never manufactures ledger/remote completion by SQL and performs no paid IO.
    pub async fn settle_with_worker(&self, lease_id: &str) {
        let mut runtime = self.config.pinning.clone();
        runtime.worker_interval =
            ipfs_s3_gateway::pinning::config::LeaseDuration::parse("1s").unwrap();
        let coordinator =
            ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::build_with_kubo(
                runtime,
                Some(KuboClient::new(self.server.uri())),
            )
            .unwrap();
        let worker = coordinator.start(
            ipfs_s3_gateway::store::Store::new(self.db.clone()),
            CancellationToken::new(),
        );
        let completion = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            loop {
                let now = ipfs_s3_gateway::store::database_clock::database_now(&self.db).await?;
                if ipfs_s3_gateway::store::entities::pin_lease_requirement::contract::completion(
                    &self.db, lease_id, now,
                )
                .await?
                .is_some_and(|(_, complete, _)| complete)
                {
                    break Ok::<(), ipfs_s3_gateway::error::AppError>(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        worker.shutdown(std::time::Duration::from_secs(2)).await;
        completion
            .expect("controlled worker did not satisfy the complete frozen contract")
            .unwrap();
    }
    pub async fn settle_applied_with_worker(
        &self,
        binding: &records::ConfirmedBinding,
        lease_id: &str,
    ) {
        let coordinator =
            ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::build_with_kubo(
                self.config.pinning.clone(),
                Some(KuboClient::new(self.server.uri())),
            )
            .unwrap();
        let scope = ipfs_s3_gateway::pinning::worker::ManagementJobScope::from_applied(
            &self.db, &self.auth, binding,
        )
        .await
        .unwrap();
        let store = ipfs_s3_gateway::store::Store::new(self.db.clone());
        let cancellation = CancellationToken::new();
        Box::pin(coordinator.management_tick(&store, &scope, &cancellation))
            .await
            .unwrap();
        let now = ipfs_s3_gateway::store::database_clock::database_now(&self.db)
            .await
            .unwrap();
        assert!(
            ipfs_s3_gateway::store::entities::pin_lease_requirement::contract::completion(
                &self.db, lease_id, now
            )
            .await
            .unwrap()
            .is_some_and(|(_, complete, _)| complete)
        );
    }
}
