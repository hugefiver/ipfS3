use crate::{
    config::Config,
    kubo::KuboClient,
    pinning::{management::{self as management, local_config::ManagementConfig, *}, policy::PublicationPolicy},
    store::{self, entities::{object_version, pin_lease}, pinning::{ledger, management::{actions, records}, publication::{self, PinTargetSpec, PublicationObject, PublicationRequest}}},
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path, query_param}};

pub(super) enum Phase {Staged, Foundation, Applied}

pub(super) struct Fixture {
    pub directory: tempfile::TempDir,
    pub server: MockServer,
    pub db: DatabaseConnection,
    pub config: ManagementConfig,
    pub auth: records::LocalAuthorization,
    pub binding: records::ConfirmedBinding,
    pub declaration: super::migration::Declaration,
}

pub(super) async fn fixture(phase: Phase) -> Fixture {
    const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID": CID}))).mount(&server).await;
    Mock::given(method("POST")).and(path("/api/v0/cat")).and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789")).mount(&server).await;
    let mut raw: Config = toml::from_str(r#"
        [[pinning.providers]]
        name = "old"
        kind = "noop"
        priority = 1
        max_bytes = 1000000
        max_pins = 1000
        [[pinning.policies]]
        bucket = "bucket"
        prefix = ""
        trigger = "request"
        provider_mode = "all"
        providers = ["old"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
    "#).unwrap();
    raw.kubo.rpc_url = server.uri();
    let config = local_config::compile(&raw, |_| None).unwrap().with_source_node(CID.into()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", directory.path().join("typed.sqlite").to_string_lossy().replace('\\', "/"));
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let provider = &config.pinning.providers[0];
    assert_eq!(provider.identity.allocation_key(), "old");
    ledger::register_configured_route(&db, "old", provider).await.unwrap();
    let declaration = super::super::management_catalog_tests::declaration(&db).await;
    publication::publish_object(&db, PublicationRequest {
        object: PublicationObject::from_put("typed-owner".into(), "bucket", "exact", CID.into(), 10,
            None, None, false, None, None, chrono::Utc::now()),
        tags: vec![], policy: PublicationPolicy {tags: vec![], leases: vec![]},
        object_target: PinTargetSpec {cid: CID.into(), logical_size: 10},
    }, &config.pinning.provider_limits).await.unwrap();
    let version = object_version::Entity::find().filter(object_version::Column::ObjectId.eq("typed-owner"))
        .one(&db).await.unwrap().unwrap();
    let identity = ExactObject {bucket: "bucket".into(), key: "exact".into(), object_id: "typed-owner".into(),
        public_version: version.version_id.map(PublicVersionId::Version).unwrap_or(PublicVersionId::Null),
        private_version_id: version.id, cid: crate::pinning::provider::canonical_resource_cid(CID).unwrap()};
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(&db, &config, &auth, &KuboClient::new(server.uri()), &evidence::PrepareRequest {
        record_id: "typed-proof".into(), identity: identity.clone(), expected_node: CID.into(), max_stored_bytes: 10,
        ttl: chrono::Duration::minutes(5), directory: false,
    }, CancellationToken::new()).await.unwrap();
    let now = store::database_clock::database_now(&db).await.unwrap().timestamp();
    let request = PlanRequest {plan_id: "typed-plan".into(), items: vec![ItemRequest {item_id: "object".into(),
        action: Action::Create(Intent {started_at: now - 1, expires_at: now + 3600,
            requirements: Requirements {mode: ProviderMode::All, provider_ids: vec![provider.identity.provider_id.clone()],
                levels: vec![ConfirmationLevel::RemotePinned], min_replicas: 1, max_age_secs: None,
                policy_revision: config.pinning.policies[0].identity.clone()}}),
        subject: Subject::Object {identity, target: TargetSelection::New(RouteIdentity::from_original(provider.route_snapshot()))},
    }]};
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = management::dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    let binding = records::confirm(&db, &config, &auth, &request, &snapshot, &plan, vec![], chrono::Duration::minutes(2)).await.unwrap();
    match phase {
        Phase::Staged => {},
        Phase::Applied => {actions::apply(&db, &config, &auth, &binding).await.unwrap();},
        Phase::Foundation => {
            let tx = db.begin().await.unwrap();
            records::revalidate_confirmed_in_transaction(&tx, &config, &auth, &binding).await.unwrap();
            let now = store::database_clock::database_now(&tx).await.unwrap();
            pin_lease::Entity::insert(pin_lease::ActiveModel {id: Set("foundation-intent".into()), owner_object_id: Set("typed-owner".into()),
                source: Set("manual".into()), policy_id: Set(config.pinning.policies[0].identity.clone()), provider_mode: Set("all".into()),
                content_mode: Set("object".into()), created_at: Set(now), last_touched_at: Set(now), expires_at: Set(now + chrono::Duration::hours(1)), generation: Set(1), state: Set("active".into())})
                .exec(&tx).await.unwrap();
            records::write_action_receipt_in_transaction(&tx, &auth, &binding,
                &records::ActionReceipt {binding: plan.items()[0].clone(), intent_ids: vec!["foundation-intent".into()], job_ids: vec![]},
                binding.confirmation.native_frame.as_ref().unwrap()).await.unwrap();
            tx.commit().await.unwrap();
        },
    }
    Fixture {directory, server, db, config, auth, binding, declaration}
}
