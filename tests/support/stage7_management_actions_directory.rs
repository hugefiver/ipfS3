use super::base;
#[path = "cluster_provider_real_car.rs"]
mod car;
use ipfs_s3_gateway::{
    config::Config,
    kubo::{KuboClient, LocalDirectoryRootKind},
    pinning::{self, management::*},
    store::{
        entities::object_version,
        pinning::{
            ledger,
            management::records,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub struct Fixture {
    pub directory: tempfile::TempDir,
    pub db: DatabaseConnection,
    pub server: MockServer,
    pub config: local_config::ManagementConfig,
    pub auth: records::LocalAuthorization,
    pub cid: String,
    pub payload_bytes: u64,
    pub car_bytes: u64,
    pub kind: LocalDirectoryRootKind,
}
impl Fixture {
    pub async fn new(strategy: &str, mode: &str, alternative: Option<&str>, hamt: bool) -> Self {
        let (cid, blocks, kind) = if hamt {
            let bytes = b"\x0a\x07\x08\x05\x28\x22\x30\x80\x02".to_vec();
            let cid = car::raw_cid(&bytes, 0x70);
            (
                cid.clone(),
                vec![(cid, bytes)],
                LocalDirectoryRootKind::HamtShard,
            )
        } else {
            let child = car::raw_cid(b"hello", 0x55);
            let (_, single) = car::directory(&child);
            let mut root = single.clone();
            let mut other = single[4..].to_vec();
            let name = other
                .windows(5)
                .position(|bytes| bytes == b"entry")
                .unwrap();
            other[name..name + 5].copy_from_slice(b"other");
            root.extend(other);
            let cid = car::raw_cid(&root, 0x70);
            (
                cid.clone(),
                vec![(cid, root), (child, b"hello".to_vec())],
                LocalDirectoryRootKind::Directory,
            )
        };
        let payload_bytes = blocks.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        let encoded = car::encode(std::slice::from_ref(&cid), &blocks);
        let car_bytes = encoded.len() as u64;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":base::NODE})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .and(query_param("arg", cid.clone()))
            .and(query_param("type", "recursive"))
            .and(query_param("offline", "true"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"Keys":{(cid.clone()):{"Type":"recursive"}}}),
                ),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST")).and(path("/api/v0/files/stat")).and(query_param("arg",format!("/ipfs/{cid}"))).and(query_param("with-local","true")).and(query_param("offline","true")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"Hash":cid,"WithLocality":true,"Local":true,"SizeLocal":"999999","Type":"directory"}))).mount(&server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .and(query_param("arg", cid.clone()))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(encoded))
            .mount(&server)
            .await;
        let mut raw = base::config(&server.uri());
        raw.pinning.policies[0].provider_mode = mode.into();
        if strategy != "cid" {
            rpc(&mut raw, 0, strategy, &server.uri());
        }
        if let Some(strategy) = alternative {
            let mut provider = raw.pinning.providers[0].clone();
            provider.name = "alternative".into();
            raw.pinning.providers.push(provider);
            let mut identity = raw.pinning_identity.providers[0].clone();
            identity.config_name = "alternative".into();
            identity.provider_id = "alternative-provider".into();
            identity.scope = "node:alternative".into();
            identity.storage_domain = "alternative-domain".into();
            raw.pinning_identity.providers.push(identity);
            raw.pinning.policies[0].providers.push("alternative".into());
            rpc(&mut raw, 1, strategy, &server.uri());
        }
        let config = base::management_config(&raw);
        let (directory, db) = base::database().await;
        base::setup(&db, &config).await;
        for provider in config.pinning.providers.iter().skip(1) {
            ledger::register_route(&db, &provider.name, &provider.identity)
                .await
                .unwrap();
        }
        let auth = records::authorize_local(&raw, &db).await.unwrap();
        Self {
            directory,
            db,
            server,
            config,
            auth,
            cid,
            payload_bytes,
            car_bytes,
            kind,
        }
    }
    pub async fn publish(&self, key: &str, legacy_lease: bool) -> ExactObject {
        let id = uuid::Uuid::new_v4().to_string();
        let size = if legacy_lease {
            self.payload_bytes as i64
        } else {
            10
        };
        let leases = if legacy_lease {
            vec![pinning::policy::LeaseIntent {
                source: pinning::policy::LeaseSource::Automatic,
                policy_id: self.config.pinning.policies[0].identity.clone(),
                provider_mode: pinning::config::ProviderMode::All,
                providers: vec![self.config.pinning.providers[0].name.clone()],
                content_mode: pinning::tags::ContentMode::Object,
                duration: pinning::config::LeaseDuration::parse("1h").unwrap(),
            }]
        } else {
            vec![]
        };
        publication::publish_object(
            &self.db,
            PublicationRequest {
                object: PublicationObject::from_put(
                    id.clone(),
                    "bucket",
                    key,
                    self.cid.clone(),
                    size,
                    None,
                    None,
                    false,
                    None,
                    None,
                    chrono::Utc::now(),
                ),
                tags: vec![],
                policy: pinning::policy::PublicationPolicy {
                    tags: vec![],
                    leases,
                },
                object_target: PinTargetSpec {
                    cid: self.cid.clone(),
                    logical_size: size,
                },
            },
            &self.config.pinning.provider_limits,
        )
        .await
        .unwrap();
        let version = object_version::Entity::find()
            .filter(object_version::Column::ObjectId.eq(&id))
            .one(&self.db)
            .await
            .unwrap()
            .unwrap();
        ExactObject {
            bucket: "bucket".into(),
            key: key.into(),
            object_id: id,
            private_version_id: version.id,
            public_version: version
                .version_id
                .map_or(PublicVersionId::Null, PublicVersionId::Version),
            cid: self.cid.clone(),
        }
    }
    pub async fn prepare(&self, identity: ExactObject, rid: &str) -> evidence::ReadAttestation {
        evidence::prepare(
            &self.db,
            &self.config,
            &self.auth,
            &KuboClient::new(self.server.uri()),
            &evidence::PrepareRequest {
                record_id: rid.into(),
                identity,
                expected_node: base::NODE.into(),
                max_stored_bytes: self.payload_bytes,
                ttl: chrono::Duration::minutes(15),
                directory: true,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
    }
    pub fn request(&self, id: &str, identity: ExactObject) -> PlanRequest {
        let mut request = base::request(identity, &self.config, false);
        request.plan_id = id.into();
        let Subject::Object { target, .. } = &mut request.items[0].subject else {
            unreachable!()
        };
        *target = TargetSelection::New(RouteIdentity::from_original(
            self.config.pinning.providers[0].route_snapshot(),
        ));
        let Action::Create(intent) = &mut request.items[0].action else {
            unreachable!()
        };
        intent.requirements.mode = if self.config.pinning.policies[0].provider_mode
            == pinning::config::ProviderMode::One
        {
            ProviderMode::One
        } else {
            ProviderMode::All
        };
        intent.requirements.provider_ids = self.config.pinning.policies[0]
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
        request
    }
    pub async fn snapshot(&self, request: &PlanRequest) -> Snapshot {
        db_snapshot::collect_with_config(&self.db, &self.config, request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap()
    }
    pub async fn confirm(
        &self,
        request: &PlanRequest,
    ) -> (records::ConfirmedBinding, wire::ImportedPlan) {
        let snapshot = self.snapshot(request).await;
        let plan = dry_run(request, &snapshot).unwrap();
        assert_eq!(plan.items()[0].status, PlanStatus::Ready);
        let imported = wire::decode_plan(
            &wire::encode_plan(request, &snapshot, &plan, Some(&plan.confirmation(vec![])))
                .unwrap(),
        )
        .unwrap();
        let binding = records::confirm(
            &self.db,
            &self.config,
            &self.auth,
            (request, &snapshot, &plan),
            vec![],
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        (binding, imported)
    }
}
fn rpc(raw: &mut Config, index: usize, strategy: &str, endpoint: &str) {
    let provider = &mut raw.pinning.providers[index];
    provider.kind = "ipfs-rpc".into();
    provider.api = Some("rpc".into());
    provider.strategy = Some(strategy.into());
    provider.endpoint = Some(endpoint.into());
    let name = provider.name.clone();
    raw.pinning_rpc.providers.push(
        toml::from_str(&format!(
            "config_name='{name}'\nprofile='kubo'\nauth='none'\nallow_private_network=true"
        ))
        .unwrap(),
    );
    let identity = &mut raw.pinning_identity.providers[index];
    identity.backend = "kubo".into();
    identity.api_profile = "kubo".into();
    identity.strategy = strategy.into();
}
