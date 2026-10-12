#[path="support/stage7_management_actions.rs"]
mod support;
use ipfs_s3_gateway::{pinning::{self,management::*},store::{self,entities::{pin_extension_decision,pin_job,pin_lease,pin_lease_requirement,pin_lease_target,pin_submit_observation},pinning::management::{actions,native,records}}};
use sea_orm::{ColumnTrait,ConnectionTrait,EntityTrait,QueryFilter,TransactionTrait};

fn selected(plan:&str,identity:ExactObject,target:&pin_lease_target::Model,action:Action)->PlanRequest {
    PlanRequest {plan_id:plan.into(),items:vec![ItemRequest {item_id:"object".into(),action,subject:Subject::Object {identity,target:TargetSelection::Existing {target_id:target.id.clone(),source:None}}}]}
}

#[tokio::test]
async fn complete_same_file_business_chain_keeps_all_foreign_records_under_original_budget() {
    let mut fixture=support::Fixture::new().await;
    let identity=fixture.publish("chain",false).await;
    let create=fixture.confirm(&fixture.create_request("chain-create",identity.clone(),false)).await;
    let created=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&create).await.unwrap();
    let lease_id=created.items[0].receipt.intent_ids[0].clone();
    fixture.settle_with_worker(&lease_id).await;
    assert!(pin_submit_observation::Entity::find().all(&fixture.db).await.unwrap().iter().any(|row|created.items[0].receipt.job_ids.contains(&row.job_id)));
    let original=pin_lease::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap();
    let original_contract=pin_lease_requirement::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap().contract;
    let target=pin_lease_target::Entity::find().filter(pin_lease_target::Column::LeaseId.eq(&lease_id)).one(&fixture.db).await.unwrap().unwrap();
    let renew=fixture.confirm(&selected("chain-renew",identity.clone(),&target,Action::Renew {expires_at:original.expires_at.timestamp()+300})).await;
    actions::apply(&fixture.db,&fixture.config,&fixture.auth,&renew).await.unwrap();
    let retry=fixture.confirm(&selected("chain-retry",identity.clone(),&target,Action::Retry)).await;
    actions::apply(&fixture.db,&fixture.config,&fixture.auth,&retry).await.unwrap();
    let reconcile=fixture.confirm(&selected("chain-reconcile",identity.clone(),&target,Action::Reconcile)).await;
    let observed=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&reconcile).await.unwrap();
    assert_eq!(observed.items[0].state,records::ActionState::ReadOnly);
    assert!(observed.items[0].jobs.iter().all(|job|job.directive==records::JobDirective::ReadOnlyObserve));
    fixture.add_provider("destination").await;
    let destination=RouteIdentity::from_original(fixture.config.pinning.providers.iter().find(|provider|provider.identity.provider_id=="destination-provider").unwrap().route_snapshot());
    let migrate=fixture.confirm(&selected("chain-migrate",identity.clone(),&target,Action::Migrate {destination:destination.clone()})).await;
    let first=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&migrate).await.unwrap();
    fixture.settle_with_worker(&lease_id).await;
    let first_point=&first.items[0].migration_points[0];
    let moved=pin_lease_target::Entity::find_by_id(first_point.destination_target_id.clone()).one(&fixture.db).await.unwrap().unwrap();
    fixture.add_provider("third").await;
    let third=RouteIdentity::from_original(fixture.config.pinning.providers.iter().find(|provider|provider.identity.provider_id=="third-provider").unwrap().route_snapshot());
    // Plan IDs deliberately sort before the first migration. Receipt linkage,
    // not SQL/UUID/lexical order, determines the immutable current chain tip.
    let remigration=selected("aaa-chain-remigrate",identity.clone(),&moved,Action::Migrate {destination:third});
    let remigration_snapshot=db_snapshot::collect_with_config(&fixture.db,&fixture.config,&remigration).await.unwrap().planning_snapshot().unwrap();
    let remigration_plan=dry_run(&remigration,&remigration_snapshot).unwrap();
    let archive=wire::encode_plan(&remigration,&remigration_snapshot,&remigration_plan,Some(&remigration_plan.confirmation(vec![]))).unwrap();
    let imported_remigration=wire::decode_plan(&archive).unwrap();
    let migrate_again=records::confirm(&fixture.db,&fixture.config,&fixture.auth,&remigration,&remigration_snapshot,&remigration_plan,vec![],chrono::Duration::minutes(5)).await.unwrap();
    let second=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&migrate_again).await.unwrap();
    fixture.settle_with_worker(&lease_id).await;
    assert!(second.items[0].migration_points[0].predecessor.is_some());
    assert_eq!(pin_lease_requirement::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap().contract,original_contract);
    assert_eq!(pin_lease::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap().created_at,original.created_at);
    let cancelled=fixture.confirm(&selected("chain-cancel",identity.clone(),&target,Action::Cancel)).await;
    actions::apply(&fixture.db,&fixture.config,&fixture.auth,&cancelled).await.unwrap();
    let legacy=fixture.publish("legacy-output",false).await;
    let backfill=fixture.confirm(&fixture.create_request("chain-backfill",legacy,true)).await;
    actions::apply(&fixture.db,&fixture.config,&fixture.auth,&backfill).await.unwrap();
    let skipped=fixture.publish("skipped-output",false).await;
    let mut old=fixture.raw.clone();old.pinning.policies.clear();old.pinning_control.unavailable=ipfs_s3_gateway::config::OptionalPinControlMode::Warn;
    let old=local_config::compile(&old,|_|None).unwrap();
    let tags=[pinning::tags::ObjectTag::new("ipfs-s3:pin","true")];
    let (_,decision)=old.policy.evaluate_publication_decision(pinning::policy::PublicationContext {bucket:"bucket",key:"skipped-output",tags:&tags,is_decompress_zip:false},pinning::decision::DecisionOrigin::new("fixture","chain-skipped")).unwrap();
    let tx=fixture.db.begin().await.unwrap();store::pinning::decision::write_for_object_in_transaction(&tx,&skipped.object_id,&decision).await.unwrap();tx.commit().await.unwrap();
    let old_json=pin_extension_decision::Entity::find_by_id(skipped.private_version_id.clone()).one(&fixture.db).await.unwrap().unwrap().snapshot;
    let mut reapply=fixture.create_request("chain-reapply",skipped.clone(),false);let Action::Create(intent)=reapply.items[0].action.clone() else{unreachable!()};reapply.items[0].action=Action::Reapply(intent);
    let reapply=fixture.confirm(&reapply).await;actions::apply(&fixture.db,&fixture.config,&fixture.auth,&reapply).await.unwrap();
    assert_eq!(pin_extension_decision::Entity::find_by_id(skipped.private_version_id).one(&fixture.db).await.unwrap().unwrap().snapshot,old_json);
    let retire=PlanRequest {plan_id:"chain-retire".into(),items:vec![ItemRequest {item_id:"domain".into(),action:Action::Retire,subject:Subject::Provider {route:RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot())}}]};
    let retired=fixture.confirm(&retire).await;actions::apply(&fixture.db,&fixture.config,&fixture.auth,&retired).await.unwrap();
    let frame=native::capture(&fixture.db).await.unwrap();frame.validate().unwrap();
    let row=fixture.db.query_one(sea_orm::Statement::from_string(fixture.db.get_database_backend(),"SELECT SUM(length(binding)+length(native_before)+COALESCE(length(result),0)) AS bytes,COUNT(*) AS rows FROM pin_management_records")).await.unwrap().unwrap();
    let bytes:i64=row.try_get("","bytes").unwrap();let rows:i64=row.try_get("","rows").unwrap();
    eprintln!("complete same-file Stage7 business chain: management stored bytes={bytes}, rows={rows}; native MAX_BYTES={}, MAX_ROWS={}",native::MAX_BYTES,native::MAX_ROWS);
    assert!(bytes<native::MAX_BYTES as i64);assert_eq!(frame.table("pin_management_records").unwrap().rows.len(),rows as usize);
    // Exact durable replay survives the changed config, cancelled lease and
    // retired domain without authorizing any new object/pin or recapturing IDs.
    let replay=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&create).await.unwrap();assert!(replay.replayed);assert!(replay.items==created.items);
    // The original imported document contains the prior derived slot proof.
    // Later remigration, cancellation and retirement cannot replace that proof
    // with today's route/configuration or turn public points into authority.
    let restored=records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported_remigration).await.unwrap();assert!(restored==migrate_again);
    assert!(actions::apply(&fixture.db,&fixture.config,&fixture.auth,&restored).await.unwrap().replayed);
    let mut changed:serde_json::Value=serde_json::from_slice(&archive).unwrap();
    let points=changed["migration_points"].as_array_mut().unwrap();assert!(!points.is_empty());
    let epoch=points[0]["destination_epoch"].as_i64().unwrap();points[0]["destination_epoch"]=serde_json::json!(epoch+1);
    if let Ok(forged)=wire::decode_plan(&serde_json::to_vec(&changed).unwrap()) {
        assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&forged).await.is_err());
    }
}

#[tokio::test]
async fn current_status_is_actual_worker_evidence_and_expired_facts_cannot_revive_the_lease() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("real-worker",false).await;
    let binding=fixture.confirm(&fixture.create_request("worker-birth",identity,false)).await;
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&binding).await.unwrap();let lease=&applied.items[0].receipt.intent_ids[0];
    fixture.settle_with_worker(lease).await;
    assert_eq!(actions::status::lookup(&fixture.db,&fixture.auth,&binding,"object").await.unwrap().unwrap().state,records::ActionState::RemoteComplete);
    let job=pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();
    assert!(pin_submit_observation::Entity::find().filter(pin_submit_observation::Column::JobId.eq(&job.id)).one(&fixture.db).await.unwrap().is_some());
    let now=store::database_clock::database_now(&fixture.db).await.unwrap();
    fixture.db.execute(sea_orm::Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_leases SET expires_at=$1 WHERE id=$2",[now.into(),lease.clone().into()])).await.unwrap();
    assert_eq!(actions::status::lookup(&fixture.db,&fixture.auth,&binding,"object").await.unwrap().unwrap().state,records::ActionState::NeedsAttention);
    assert!(matches!(actions::job_authority::check_before_call(&fixture.db,&job).await.unwrap(),Some(actions::job_authority::JobPermit::LedgerOnly(_))));
    assert_eq!(pin_lease::Entity::find_by_id(lease.clone()).one(&fixture.db).await.unwrap().unwrap().expires_at,now);
}
