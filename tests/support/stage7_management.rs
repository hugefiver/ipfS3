use ipfs_s3_gateway::{config::Config,pinning::{self,management::*,policy::{PublicationPolicy,LeaseIntent,LeaseSource}},store::{self,entities::object_version,pinning::{ledger,publication::{self,PublicationObject,PublicationRequest,PinTargetSpec}}}};
use sea_orm::{DatabaseConnection,EntityTrait,ColumnTrait,QueryFilter,TransactionTrait};
pub const CID:&str="QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
pub const NODE:&str="QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

/// Real file identity is required for authorization. In-memory databases remain
/// useful diagnostic fixtures, but cannot carry management authority.
pub async fn database() -> (tempfile::TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("management.sqlite");
    let url = format!("sqlite://{}?mode=rwc", path.to_string_lossy().replace('\\', "/"));
    let db = store::connect_database(&url).await.unwrap();
    (directory, db)
}

pub fn config(endpoint:&str)->Config {
    let mut config:Config=toml::from_str(r#"
        [decompress_zip]
        auto_pin_outputs = false
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
        allow_decompressed=true
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
    "#).unwrap();
    config.kubo.rpc_url=endpoint.into();config
}

pub fn management_config(raw:&Config)->local_config::ManagementConfig {
    local_config::compile(raw,|_|None).unwrap().with_source_node(NODE.into()).unwrap()
}

pub async fn setup(db:&DatabaseConnection,config:&local_config::ManagementConfig){
    store::run_migrations(db).await.unwrap();
    store::bucket::create(db,"bucket",None).await.unwrap();
    ledger::register_route(db,&config.pinning.providers[0].identity.allocation_key(),&config.pinning.providers[0].identity).await.unwrap();
}

pub async fn publish(db:&DatabaseConnection,config:&local_config::ManagementConfig,key:&str,with_lease:bool)->ExactObject {
    publish_cid(db,config,key,CID,with_lease).await
}

pub async fn publish_cid(db:&DatabaseConnection,config:&local_config::ManagementConfig,key:&str,cid:&str,with_lease:bool)->ExactObject {
    let id=uuid::Uuid::new_v4().to_string();
    let leases=if with_lease {vec![LeaseIntent {source:LeaseSource::Automatic,policy_id:config.pinning.policies[0].identity.clone(),provider_mode:pinning::config::ProviderMode::All,providers:vec![config.pinning.providers[0].name.clone()],content_mode:pinning::tags::ContentMode::Object,duration:pinning::config::LeaseDuration::parse("1h").unwrap()}]} else {vec![]};
    publication::publish_object(db,PublicationRequest {object:PublicationObject::from_put(id.clone(),"bucket",key,cid.into(),10,None,None,false,None,None,chrono::Utc::now()),tags:vec![],policy:PublicationPolicy{tags:vec![],leases},object_target:PinTargetSpec{cid:cid.into(),logical_size:10}},&config.pinning.provider_limits).await.unwrap();
    let row=object_version::Entity::find().filter(object_version::Column::ObjectId.eq(&id)).one(db).await.unwrap().unwrap();
    ExactObject {bucket:"bucket".into(),key:key.into(),public_version:row.version_id.map(PublicVersionId::Version).unwrap_or(PublicVersionId::Null),private_version_id:row.id,object_id:id,cid:pinning::provider::canonical_resource_cid(cid).unwrap()}
}

pub fn request(identity:ExactObject,config:&local_config::ManagementConfig,backfill:bool)->PlanRequest {
    let provider=&config.pinning.providers[0];
    let raw=provider.route_snapshot();
    let route=RouteIdentity{provider_id:raw.provider_id.clone(),backend:raw.backend.clone(),scope:raw.scope.clone(),storage_domain:raw.storage_domain.clone(),api_profile:raw.api_profile.clone(),strategy:raw.strategy.clone(),resource_type:ResourceType::PsaRequest,credential_revision:raw.credential_revision,endpoint_revision:raw.endpoint_revision,original:Some(raw)};
    let now=chrono::Utc::now().timestamp();
    let intent=Intent{started_at:now-1,expires_at:now+3600,requirements:Requirements{mode:ProviderMode::All,provider_ids:vec![route.provider_id.clone()],levels:vec![ConfirmationLevel::RemotePinned],min_replicas:1,max_age_secs:None,policy_revision:config.pinning.policies[0].identity.clone()}};
    PlanRequest {plan_id:"native-fixture".into(),items:vec![ItemRequest{item_id:"object".into(),action:if backfill{Action::Backfill(intent)}else{Action::Create(intent)},subject:Subject::Object{identity,target:TargetSelection::New(route)}}]}
}

pub async fn write_current_decision(db:&DatabaseConnection,config:&local_config::ManagementConfig,identity:&ExactObject){
    let (_,decision)=config.policy.evaluate_publication_decision(pinning::policy::PublicationContext{bucket:&identity.bucket,key:&identity.key,tags:&[],is_decompress_zip:false},pinning::decision::DecisionOrigin::new("fixture","authorized-fixture")).unwrap();
    let tx=db.begin().await.unwrap();
    store::pinning::decision::write_for_object_in_transaction(&tx,&identity.object_id,&decision).await.unwrap();tx.commit().await.unwrap();
}
