use crate::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        management::{self as management, local_config::ManagementConfig, *},
        policy::PublicationPolicy,
    },
    store::{
        self,
        entities::object_version,
        pinning::{
            ledger,
            management::{actions, records},
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub(super) enum Phase {
    Staged,
    Applied,
}

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
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID": CID})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let mut raw: Config = toml::from_str(
        r#"
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
        [pinning_identity]
        primary_storage_domain = "hot"
        [[pinning_identity.providers]]
        config_name = "old"
        provider_id = "typed-provider"
        display_name = "Typed provider"
        backend = "noop"
        scope = "account"
        storage_domain = "remote"
        credential_revision = 1
        endpoint_revision = 1
        api_profile = "noop"
        strategy = "cid"
        cleanup = "retain"
    "#,
    )
    .unwrap();
    raw.kubo.rpc_url = server.uri();
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(CID.into())
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("typed.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let provider = &config.pinning.providers[0];
    ledger::register_configured_route(&db, &provider.name, provider)
        .await
        .unwrap();
    let mut declaration = super::super::management_catalog_tests::declaration(&db).await;
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE pin_provider_usage SET provider=$1 WHERE provider='old'",
        [provider.name.clone().into()],
    ))
    .await
    .unwrap();
    declaration.namespaces[0].source = provider.name.clone();
    declaration.namespaces[0].target = provider.name.clone();
    declaration.namespaces[0].binding_only = true;
    declaration.namespaces[0].target_route = provider.route_snapshot();
    declaration.namespaces[0].bindings[0].provider = provider.name.clone();
    declaration.namespaces[0].bindings[0]
        .key
        .insert("provider".into(), provider.name.clone());
    ledger::register_configured_route(&db, &provider.name, provider)
        .await
        .unwrap();
    publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                "typed-owner".into(),
                "bucket",
                "exact",
                CID.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![],
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
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq("typed-owner"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "exact".into(),
        object_id: "typed-owner".into(),
        public_version: version
            .version_id
            .map(PublicVersionId::Version)
            .unwrap_or(PublicVersionId::Null),
        private_version_id: version.id,
        cid: crate::pinning::provider::canonical_resource_cid(CID).unwrap(),
    };
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "typed-proof".into(),
            identity: identity.clone(),
            expected_node: CID.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(5),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let now = store::database_clock::database_now(&db)
        .await
        .unwrap()
        .timestamp();
    let request = PlanRequest {
        plan_id: "typed-plan".into(),
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
    let plan = management::dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Candidate);
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        vec!["object".into()],
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    match phase {
        Phase::Staged => {}
        Phase::Applied => {
            actions::apply(&db, &config, &auth, &binding).await.unwrap();
        }
    }
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,ownership,effect,route) VALUES($1,$2,'unknown','unknown',NULL) ON CONFLICT(provider,cid) DO UPDATE SET route=NULL,effect='unknown'",
        [provider.name.clone().into(),CID.into()])).await.unwrap();
    Fixture {
        directory,
        server,
        db,
        config,
        auth,
        binding,
        declaration,
    }
}
