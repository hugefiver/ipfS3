use super::*;
use crate::{pinning::{management::*,policy::PublicationPolicy},store::{self,entities::{object_version,pin_job,pin_lease,pin_lease_target},pinning::publication::{self,PublicationObject,PublicationRequest,PinTargetSpec}}};
use sea_orm::{ColumnTrait,EntityTrait,QueryFilter};
use wiremock::{Mock,MockServer,ResponseTemplate,matchers::{method,path}};
use tokio_util::sync::CancellationToken;
const CID:&str="QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

pub(super) struct Fixture {_directory:tempfile::TempDir,_server:MockServer,pub(super) db:DatabaseConnection,pub(super) config:ManagementConfig,pub(super) auth:records::LocalAuthorization,pub(super) binding:records::ConfirmedBinding}
pub(super) async fn fixture(plan:&str)->Fixture {
    fixture_with_automatic(plan,false).await
}
pub(super) async fn fixture_with_automatic(plan:&str,automatic:bool)->Fixture {
    let directory=tempfile::tempdir().unwrap();let url=format!("sqlite://{}?mode=rwc",directory.path().join("actions.sqlite").to_string_lossy().replace('\\',"/"));
    let db=store::connect_database(&url).await.unwrap();store::run_migrations(&db).await.unwrap();store::bucket::create(&db,"bucket",None).await.unwrap();
    let server=MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v0/id")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":CID}))).mount(&server).await;
    Mock::given(method("POST")).and(path("/api/v0/cat")).respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789")).mount(&server).await;
    let mut raw:crate::config::Config=toml::from_str(r#"
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
    "#).unwrap();raw.kubo.rpc_url=server.uri();
    let config=local_config::compile(&raw,|_|None).unwrap().with_source_node(CID.into()).unwrap();let provider=&config.pinning.providers[0];
    crate::store::pinning::ledger::register_route(&db,&provider.name,&provider.identity).await.unwrap();
    let id=uuid::Uuid::new_v4().to_string();
    let leases=if automatic {vec![crate::pinning::policy::LeaseIntent {source:crate::pinning::policy::LeaseSource::Automatic,policy_id:config.pinning.policies[0].identity.clone(),provider_mode:crate::pinning::config::ProviderMode::All,providers:vec![provider.name.clone()],content_mode:crate::pinning::tags::ContentMode::Object,duration:crate::pinning::config::LeaseDuration::parse("1h").unwrap()}]} else {vec![]};
    publication::publish_object(&db,PublicationRequest {object:PublicationObject::from_put(id.clone(),"bucket","exact",CID.into(),10,None,None,false,None,None,chrono::Utc::now()),tags:vec![],policy:PublicationPolicy {tags:vec![],leases},object_target:PinTargetSpec {cid:CID.into(),logical_size:10}},&config.pinning.provider_limits).await.unwrap();
    let version=object_version::Entity::find().filter(object_version::Column::ObjectId.eq(&id)).one(&db).await.unwrap().unwrap();
    let identity=ExactObject {bucket:"bucket".into(),key:"exact".into(),object_id:id,private_version_id:version.id,public_version:version.version_id.map_or(PublicVersionId::Null,PublicVersionId::Version),cid:crate::pinning::provider::canonical_resource_cid(CID).unwrap()};
    let auth=records::authorize_local(&raw,&db).await.unwrap();
    evidence::prepare(&db,&config,&auth,&crate::kubo::KuboClient::new(server.uri()),&evidence::PrepareRequest {record_id:uuid::Uuid::new_v4().to_string(),identity:identity.clone(),expected_node:CID.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(15),directory:false},CancellationToken::new()).await.unwrap();
    let now=store::database_clock::database_now(&db).await.unwrap().timestamp();
    let request=PlanRequest {plan_id:plan.into(),items:vec![ItemRequest {item_id:"object".into(),action:Action::Create(Intent {started_at:now-1,expires_at:now+3600,requirements:Requirements {mode:ProviderMode::All,provider_ids:vec![provider.identity.provider_id.clone()],levels:vec![ConfirmationLevel::RemotePinned],min_replicas:1,max_age_secs:None,policy_revision:config.pinning.policies[0].identity.clone()}}),subject:Subject::Object {identity,target:TargetSelection::New(RouteIdentity::from_original(provider.route_snapshot()))}}]};
    let snapshot=db_snapshot::collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();let plan=dry_run(&request,&snapshot).unwrap();
    let ack=if automatic {plan.items().iter().filter(|entry|entry.status==PlanStatus::Candidate).map(|entry|entry.request.item_id.clone()).collect()}else{vec![]};
    let binding=records::confirm(&db,&config,&auth,&request,&snapshot,&plan,ack,chrono::Duration::minutes(5)).await.unwrap();
    Fixture {_directory:directory,_server:server,db,config,auth,binding}
}

#[tokio::test]
async fn actual_receipt_update_trigger_fault_rolls_back_effects_and_trigger_ddl() {
    let fixture=fixture("actual-fault").await;
    records::fault_injection::install(&fixture.auth,&fixture.binding,records::fault_injection::Fault::ReceiptAbort);
    let result=apply_detailed(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await;
    let failure=result.err().expect("the real receipt UPDATE must fail");assert!(matches!(failure.cause,ManagementError::Database));
    assert_eq!(failure.items.len(),1);assert_eq!(failure.items[0].state,records::ActionState::Stopped);
    assert!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().is_empty());assert!(pin_lease_target::Entity::find().all(&fixture.db).await.unwrap().is_empty());assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    assert!(records::lookup_committed(&fixture.db,&fixture.auth,&fixture.binding,"object").await.unwrap().is_none());
    assert!(apply(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.is_ok());
}

#[tokio::test]
async fn final_real_database_clock_lease_expiry_fence_rolls_back_even_written_receipts() {
    let fixture=fixture("actual-expiry").await;
    records::fault_injection::install(&fixture.auth,&fixture.binding,records::fault_injection::Fault::ExpireLease);
    let failure=apply_detailed(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.err().expect("expired business lease cannot commit");
    assert!(matches!(failure.cause,ManagementError::Expired));assert_eq!(failure.items[0].state,records::ActionState::Stopped);
    assert!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().is_empty());assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    assert!(records::lookup_committed(&fixture.db,&fixture.auth,&fixture.binding,"object").await.unwrap().is_none());
}

#[tokio::test]
async fn crate_decoder_export_preserves_business_and_legacy_receipts() {
    let fixture=fixture("decode-export").await;let applied=apply(&fixture.db,&fixture.config,&fixture.auth,&fixture.binding).await.unwrap();let action=&applied.items[0];
    // Read the actual stored encoding so the export exercises the record codec,
    // not a second independently implemented packing format.
    let row=fixture.db.query_one(sea_orm::Statement::from_sql_and_values(fixture.db.get_database_backend(),"SELECT result FROM pin_management_records WHERE plan_id=$1 AND kind='action'",[fixture.binding.request.plan_id.clone().into()])).await.unwrap().unwrap();
    let (receipt,business)=records::decode_action_result(&row.try_get::<String>("","result").unwrap()).unwrap();assert!(receipt==action.receipt);assert!(business.as_ref()==Some(action));
    let legacy=serde_json::to_string(&action.receipt).unwrap();let (receipt,business)=records::decode_action_result(&legacy).unwrap();assert!(receipt==action.receipt);assert!(business.is_none());
    assert!(records::decode_action_result("{\"state\":\"accepted\",\"state\":\"needs_attention\"}").is_err());
}
