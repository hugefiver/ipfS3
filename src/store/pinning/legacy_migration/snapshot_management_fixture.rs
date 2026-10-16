use crate::{
    config::Config,
    kubo::KuboClient,
    pinning::{
        management::{self as management, db_snapshot, evidence, local_config, *},
        policy::PublicationPolicy,
    },
    store::{
        self,
        entities::object_version,
        pinning::{
            ledger,
            management::records,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

pub(in super::super) struct Fixture {
    pub directory: tempfile::TempDir,
    pub db: DatabaseConnection,
    pub server: MockServer,
    pub declaration: super::migration::Declaration,
}

pub(in super::super) async fn fixture() -> Fixture {
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
        [decompress_zip]
        auto_pin_outputs = false
        [[pinning.providers]]
        name = "fixture"
        kind = "noop"
        priority = 1
        max_bytes = 1000000
        max_pins = 1000
        [[pinning.policies]]
        bucket = "bucket"
        prefix = ""
        trigger = "request"
        provider_mode = "all"
        providers = ["fixture"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
        [pinning_identity]
        primary_storage_domain = "hot"
        [[pinning_identity.providers]]
        config_name = "fixture"
        provider_id = "fixture-provider"
        display_name = "Fixture"
        backend = "noop"
        scope = "isolated"
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
            .join("r26-management.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let provider = &config.pinning.providers[0];
    ledger::register_route(&db, &provider.identity.allocation_key(), &provider.identity)
        .await
        .unwrap();
    publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                "private-object".into(),
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
    db.execute_unprepared(
        "UPDATE objects SET metadata='{\"private\":\"old\"}' WHERE id='private-object'",
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq("private-object"))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "exact".into(),
        public_version: version
            .version_id
            .map(PublicVersionId::Version)
            .unwrap_or(PublicVersionId::Null),
        private_version_id: version.id,
        object_id: "private-object".into(),
        cid: crate::pinning::provider::canonical_resource_cid(CID).unwrap(),
    };
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "proof".into(),
            identity,
            expected_node: CID.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(5),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let request = PlanRequest {
        plan_id: "retirement".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(provider.route_snapshot()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = management::dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    let applied = crate::store::pinning::management::actions::apply(&db, &config, &auth, &binding)
        .await
        .unwrap();
    assert!(
        records::lookup_applied(&db, &auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            == applied.items[0].receipt
    );
    assert!(
        records::read_retirement(&db, &auth, &provider.identity.allocation_key())
            .await
            .unwrap()
            .is_some()
    );
    // An independent domain is migrated; opaque "old" text is not a provider reference.
    let declaration = super::super::management_catalog_tests::declaration(&db).await;
    Fixture {
        directory,
        db,
        server,
        declaration,
    }
}
