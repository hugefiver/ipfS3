#[path="support/stage7_management_actions.rs"]
mod support;
use ipfs_s3_gateway::{pinning::{self,management::*},store::{self,entities::{pin_extension_decision,pin_job,pin_lease,pin_lease_target,remote_pin},pinning::management::{actions,records}}};
use sea_orm::{ConnectionTrait,EntityTrait,Statement,TransactionTrait};

#[tokio::test]
async fn exact_automatic_source_renew_preserves_birth_and_cancel_only_ends_that_lease() {
    let fixture=support::Fixture::new().await;
    let identity=fixture.publish("automatic",true).await;
    let original=pin_lease::Entity::find().one(&fixture.db).await.unwrap().unwrap();
    let birth=store::entities::pin_lease_requirement::Entity::find_by_id(original.id.clone()).one(&fixture.db).await.unwrap().unwrap();
    let old_jobs=pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let request=fixture.existing_request("renew",identity.clone(),Action::Renew {expires_at:original.expires_at.timestamp()+1800}).await;
    let confirmed=fixture.confirm(&request).await;
    let renewal=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    let extended=pin_lease::Entity::find_by_id(original.id.clone()).one(&fixture.db).await.unwrap().unwrap();
    assert_eq!(extended.source,original.source);assert_eq!(extended.created_at,original.created_at);assert_eq!(extended.owner_object_id,original.owner_object_id);
    assert_eq!(extended.expires_at.timestamp(),original.expires_at.timestamp()+1800);
    assert_eq!(store::entities::pin_lease_requirement::Entity::find_by_id(original.id.clone()).one(&fixture.db).await.unwrap().unwrap().contract,birth.contract);
    assert!(renewal.items[0].jobs.iter().filter(|job|job.directive==records::JobDirective::Submit).all(|job|job.lease_generation==Some(extended.generation)));
    for old in &old_jobs {
        let retained=pin_job::Entity::find_by_id(old.id.clone()).one(&fixture.db).await.unwrap().unwrap();assert_eq!(retained.expected_generation,old.expected_generation);
        assert!(!renewal.items[0].receipt.job_ids.contains(&old.id));
    }
    assert!(actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap().items==renewal.items);
    let cancel=fixture.existing_request("cancel",identity,Action::Cancel).await;
    let confirmed=fixture.confirm(&cancel).await;
    actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    let ended=pin_lease::Entity::find_by_id(original.id).one(&fixture.db).await.unwrap().unwrap();assert_eq!(ended.state,"cancelled");
    let remote=remote_pin::Entity::find().one(&fixture.db).await.unwrap().unwrap();assert_eq!(remote.cid_size,10);
    let usage=store::entities::pin_provider_usage::Entity::find().one(&fixture.db).await.unwrap().unwrap();assert_eq!(usage.reserved_bytes,10);assert_eq!(usage.reserved_pins,1);
}

#[tokio::test]
async fn legacy_backfill_has_new_owner_reference_and_shared_cid_single_charge() {
    let fixture=support::Fixture::new().await;
    let first=fixture.publish("first",true).await;
    let second=fixture.publish("explicit-old-output",false).await;
    let request=fixture.create_request("backfill",second.clone(),true);
    let snapshot=db_snapshot::collect_with_config(&fixture.db,&fixture.config,&request).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(&request,&snapshot).unwrap();assert_eq!(plan.items()[0].status,PlanStatus::Candidate);
    assert!(records::confirm(&fixture.db,&fixture.config,&fixture.auth,&request,&snapshot,&plan,vec![],chrono::Duration::minutes(5)).await.is_err());
    let confirmed=fixture.confirm(&request).await;
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    let created=pin_lease::Entity::find_by_id(applied.items[0].receipt.intent_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();assert_eq!(created.owner_object_id,second.object_id);assert_ne!(created.owner_object_id,first.object_id);
    assert_eq!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().len(),2);
    let usage=store::entities::pin_provider_usage::Entity::find().one(&fixture.db).await.unwrap().unwrap();assert_eq!(usage.reserved_bytes,10);assert_eq!(usage.reserved_pins,1);
    assert!(applied.items[0].jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve));
}

#[tokio::test]
async fn skipped_reapply_creates_a_new_decision_and_never_rewrites_old_json() {
    let fixture=support::Fixture::new().await;
    let identity=fixture.publish("skipped",false).await;
    let mut old=fixture.raw.clone();old.pinning.policies.clear();old.pinning_control.unavailable=store_unavailable_warn();
    let old=local_config::compile(&old,|_|None).unwrap();
    let tags=[pinning::tags::ObjectTag::new("ipfs-s3:pin","true")];
    let (_,decision)=old.policy.evaluate_publication_decision(pinning::policy::PublicationContext {bucket:&identity.bucket,key:&identity.key,tags:&tags,is_decompress_zip:false},pinning::decision::DecisionOrigin::new("fixture","skipped-origin")).unwrap();
    assert_eq!(decision.effect,pinning::decision::DecisionEffect::Skipped);
    let tx=fixture.db.begin().await.unwrap();store::pinning::decision::write_for_object_in_transaction(&tx,&identity.object_id,&decision).await.unwrap();tx.commit().await.unwrap();
    let before=pin_extension_decision::Entity::find_by_id(identity.private_version_id.clone()).one(&fixture.db).await.unwrap().unwrap().snapshot;
    let mut request=fixture.create_request("reapply",identity.clone(),false);let Action::Create(intent)=request.items[0].action.clone() else{unreachable!()};request.items[0].action=Action::Reapply(intent);
    let confirmed=fixture.confirm(&request).await;let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    assert!(applied.items[0].decision.is_some());
    assert_eq!(pin_extension_decision::Entity::find_by_id(identity.private_version_id).one(&fixture.db).await.unwrap().unwrap().snapshot,before);
}

fn store_unavailable_warn()->ipfs_s3_gateway::config::OptionalPinControlMode {ipfs_s3_gateway::config::OptionalPinControlMode::Warn}

#[tokio::test]
async fn encrypted_cid_and_attested_ciphertext_bytes_are_not_plaintext_metadata_budget() {
    use wiremock::{Mock,ResponseTemplate,matchers::{method,path,query_param}};
    let fixture=support::Fixture::new().await;
    let master=ipfs_s3_gateway::crypto::MasterKey::from_hex(&"17".repeat(32)).unwrap();
    let key=master.generate_object_key();let wrapped=master.wrap(&key).unwrap();
    let plaintext=b"plain";
    let ciphertext=ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(&key,&[7;12],plaintext).unwrap();
    let bytes=ciphertext.len() as i64;assert!(bytes>plaintext.len() as i64);
    Mock::given(method("POST")).and(path("/api/v0/cat")).and(query_param("offline","true")).respond_with(ResponseTemplate::new(200).set_body_bytes(ciphertext.to_vec())).with_priority(1).mount(&fixture.server).await;
    let id=uuid::Uuid::new_v4().to_string();
    store::pinning::publication::publish_object(&fixture.db,store::pinning::publication::PublicationRequest {
        object:store::pinning::publication::PublicationObject::from_put(id.clone(),"bucket","ciphertext",support::CID.into(),plaintext.len() as i64,None,None,true,Some(wrapped.clone()),None,chrono::Utc::now()),tags:vec![],
        policy:pinning::policy::PublicationPolicy {tags:vec![],leases:vec![]},object_target:store::pinning::publication::PinTargetSpec {cid:support::CID.into(),logical_size:plaintext.len() as i64},
    },&fixture.config.pinning.provider_limits).await.unwrap();
    let version=store::entities::object_version::Entity::find().one(&fixture.db).await.unwrap().unwrap();
    let identity=ExactObject {bucket:"bucket".into(),key:"ciphertext".into(),public_version:version.version_id.map(PublicVersionId::Version).unwrap_or(PublicVersionId::Null),private_version_id:version.id,object_id:id.clone(),cid:pinning::provider::canonical_resource_cid(support::CID).unwrap()};
    evidence::prepare(&fixture.db,&fixture.config,&fixture.auth,&ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri()),&evidence::PrepareRequest {record_id:uuid::Uuid::new_v4().to_string(),identity:identity.clone(),expected_node:support::NODE.into(),max_stored_bytes:bytes as u64,ttl:chrono::Duration::minutes(15),directory:false},tokio_util::sync::CancellationToken::new()).await.unwrap();
    let request=fixture.create_request("ciphertext-budget",identity,false);let binding=fixture.confirm(&request).await;
    let snapshot=db_snapshot::collect_with_config(&fixture.db,&fixture.config,&request).await.unwrap().planning_snapshot().unwrap();let plan=dry_run(&request,&snapshot).unwrap();
    let wire=wire::encode_plan(&request,&snapshot,&plan,None);
    assert!(wire.is_ok());assert!(!String::from_utf8(wire.unwrap()).unwrap().contains(&wrapped));
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&binding).await.unwrap();assert_eq!(applied.items[0].decision.as_ref().unwrap().stored_bytes,bytes as u64);
    let target=pin_lease_target::Entity::find_by_id(applied.items[0].targets[0].clone()).one(&fixture.db).await.unwrap().unwrap();assert_eq!(target.logical_size,bytes);assert_eq!(target.cid,support::CID);
    let remote=remote_pin::Entity::find_by_id((target.provider,target.cid)).one(&fixture.db).await.unwrap().unwrap();assert_eq!(remote.cid_size,bytes);
    let object=store::entities::object::Entity::find_by_id(id).one(&fixture.db).await.unwrap().unwrap();assert!(object.encrypted);assert_eq!(object.key_wrap.as_deref(),Some(wrapped.as_str()));assert_eq!(object.cid,support::CID);assert_eq!(object.size,plaintext.len() as i64);
}

#[tokio::test]
async fn unknown_reserved_retry_and_management_reconcile_are_persistently_readonly() {
    let fixture=support::Fixture::new().await;
    let identity=fixture.publish("unknown",true).await;
    // A real preexisting reservation with no invocation is unknown, not clear.
    let retry=fixture.existing_request("retry-unknown",identity.clone(),Action::Retry).await;
    let confirmed=fixture.confirm(&retry).await;
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    assert!(!applied.items[0].jobs.is_empty());assert!(applied.items[0].jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve));
    let job=pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();
    assert!(matches!(actions::job_authority::check_before_call(&fixture.db,&job).await.unwrap(),Some(actions::job_authority::JobPermit::ReadOnly(_))));
    assert!(!actions::job_authority::projection_allowed(&fixture.db,&job).await.unwrap());
    let mut forged=job.clone();forged.operation="submit".into();assert!(actions::job_authority::check_before_call(&fixture.db,&forged).await.is_err());
    let reconcile=fixture.existing_request("observe",identity,Action::Reconcile).await;
    let confirmed=fixture.confirm(&reconcile).await;let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    assert!(applied.items[0].jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve));
}

#[tokio::test]
async fn readonly_status_does_not_hide_reported_unknown_or_missing_original_resource() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("readonly-status",true).await;
    let request=fixture.existing_request("readonly-status",identity,Action::Reconcile).await;let binding=fixture.confirm(&request).await;
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&binding).await.unwrap();assert_eq!(applied.items[0].state,records::ActionState::ReadOnly);
    let authority=&applied.items[0].jobs[0];
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE remote_pin_ledger SET effect='unknown' WHERE provider=$1 AND cid=$2",[authority.provider.clone().into(),authority.cid.clone().into()])).await.unwrap();
    assert_eq!(actions::status::lookup(&fixture.db,&fixture.auth,&binding,"object").await.unwrap().unwrap().state,records::ActionState::NeedsAttention);
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE remote_pin_ledger SET effect='reserved' WHERE provider=$1 AND cid=$2",[authority.provider.clone().into(),authority.cid.clone().into()])).await.unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"DELETE FROM remote_pin_ledger WHERE provider=$1 AND cid=$2",[authority.provider.clone().into(),authority.cid.clone().into()])).await.unwrap();
    assert_eq!(actions::status::lookup(&fixture.db,&fixture.auth,&binding,"object").await.unwrap().unwrap().state,records::ActionState::NeedsAttention);
}

#[tokio::test]
async fn old_reservation_size_disagreement_cannot_be_laundered_by_backfill() {
    let fixture=support::Fixture::new().await;fixture.publish("old-charge",true).await;
    fixture.db.execute_unprepared("UPDATE remote_pins SET cid_size=9").await.unwrap();
    let identity=fixture.publish("new-reference",false).await;
    let request=fixture.create_request("size-mismatch",identity,false);let confirmed=fixture.confirm(&request).await;
    assert!(actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.is_err());
    assert_eq!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().len(),1);
    assert_eq!(store::entities::pin_provider_usage::Entity::find().one(&fixture.db).await.unwrap().unwrap().reserved_bytes,10);
}

#[tokio::test]
async fn same_file_restart_lost_response_replays_exact_receipt_without_expanding_authority() {
    let mut fixture=support::Fixture::new().await;
    let identity=fixture.publish("lost-response",false).await;
    let request=fixture.create_request("lost-response",identity,false);let confirmed=fixture.confirm(&request).await;
    let committed=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    fixture.db.clone().close().await.unwrap();
    let path=fixture.directory.path().join("management.sqlite");
    fixture.db=store::connect_database(&format!("sqlite://{}?mode=rw",path.to_string_lossy().replace('\\',"/"))).await.unwrap();
    fixture.auth=records::authorize_local(&fixture.raw,&fixture.db).await.unwrap();
    let replay=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();assert!(replay.replayed);assert!(replay.items==committed.items);
    assert_eq!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().len(),1);
    let mut forged=confirmed.clone();forged.confirmation.bindings[0].request.item_id="forged".into();assert!(actions::apply(&fixture.db,&fixture.config,&fixture.auth,&forged).await.is_err());
    let now=store::database_clock::database_now(&fixture.db).await.unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_leases SET state='expired',expires_at=$1,generation=generation+1",[now.into()])).await.unwrap();
    let replay=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();assert!(replay.replayed);
    let job=pin_job::Entity::find_by_id(committed.items[0].receipt.job_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();
    assert!(matches!(actions::job_authority::check_before_call(&fixture.db,&job).await.unwrap(),Some(actions::job_authority::JobPermit::LedgerOnly(_))));
    assert!(!actions::job_authority::projection_allowed(&fixture.db,&job).await.unwrap());
}
