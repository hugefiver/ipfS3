#[path="support/stage7_management_actions.rs"]
mod support;
use ipfs_s3_gateway::{pinning::management::*,store::{self,entities::{pin_job,pin_lease,pin_lease_target,pin_provider_usage,remote_pin},pinning::management::{actions,records}}};
use sea_orm::{ColumnTrait,ConnectionTrait,EntityTrait,QueryFilter,TransactionTrait};

#[tokio::test]
async fn migration_point_is_consumed_by_one_and_all_completion_without_resetting_ttl() {
    for one in [false,true] {
        let mut fixture=support::Fixture::new().await;
        fixture.add_provider("second").await;
        if one {fixture.raw.pinning.policies[0].provider_mode="one".into();fixture.config=local_config::compile(&fixture.raw,|_|None).unwrap().with_source_node(support::NODE.into()).unwrap();}
        let identity=fixture.publish("migrate",false).await;
        let confirmed=fixture.confirm(&fixture.create_request("birth",identity.clone(),false)).await;
        let birth_result=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
        let lease_id=&birth_result.items[0].receipt.intent_ids[0];
        let original=pin_lease::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap();
        let contract_before=store::entities::pin_lease_requirement::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap().contract;
        let source=pin_lease_target::Entity::find().filter(pin_lease_target::Column::LeaseId.eq(lease_id)).one(&fixture.db).await.unwrap().unwrap();
        fixture.confirmed_remote(&source.provider,&source.cid).await;
        fixture.add_provider("destination").await;
        let destination=RouteIdentity::from_original(fixture.config.pinning.providers.iter().find(|provider|provider.identity.provider_id=="destination-provider").unwrap().route_snapshot());
        let request=PlanRequest {plan_id:"migration".into(),items:vec![ItemRequest {item_id:"object".into(),action:Action::Migrate {destination:destination.clone()},subject:Subject::Object {identity:identity.clone(),target:TargetSelection::Existing {target_id:source.id.clone(),source:None}}}]};
        let confirmed=fixture.confirm(&request).await;
        let migrated=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
        assert_eq!(migrated.items[0].migration_points.len(),1);
        let point=&migrated.items[0].migration_points[0];
        assert_eq!(point.source_target_id,source.id);assert_eq!(point.source_allocation,source.provider);
        let after=pin_lease::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap();
        assert_eq!(after.created_at,original.created_at);assert_eq!(after.expires_at,original.expires_at);assert_eq!(after.generation,original.generation);assert_eq!(after.source,original.source);
        assert_eq!(store::entities::pin_lease_requirement::Entity::find_by_id(lease_id.clone()).one(&fixture.db).await.unwrap().unwrap().contract,contract_before);
        let destination_target=pin_lease_target::Entity::find_by_id(point.destination_target_id.clone()).one(&fixture.db).await.unwrap().unwrap();
        fixture.confirmed_remote(&destination_target.provider,&destination_target.cid).await;
        let now=store::database_clock::database_now(&fixture.db).await.unwrap();
        let resolved=store::entities::pin_lease_requirement::contract::frozen_candidates_for_cid(&fixture.db,lease_id,&source.cid).await.unwrap().unwrap();
        assert!(resolved.contains(&point.destination_allocation));assert!(!resolved.contains(&point.source_allocation));
        assert!(store::entities::pin_lease_requirement::contract::target_confirmed(&fixture.db,lease_id,&point.slot,&destination_target.cid,now).await.unwrap());
        let (_,complete,_)=store::entities::pin_lease_requirement::contract::completion(&fixture.db,lease_id,now).await.unwrap().unwrap();
        assert_eq!(complete,one,"All must keep its other original slot; One completes per CID");
        assert_eq!(pin_provider_usage::Entity::find_by_id(source.provider.clone()).one(&fixture.db).await.unwrap().unwrap().reserved_bytes,10);
        assert_eq!(pin_provider_usage::Entity::find_by_id(destination_target.provider).one(&fixture.db).await.unwrap().unwrap().reserved_bytes,10);
        let followup=PlanRequest {plan_id:"public-followup".into(),items:vec![ItemRequest {item_id:"object".into(),action:Action::Retry,subject:Subject::Object {identity:identity.clone(),target:TargetSelection::Existing {target_id:point.destination_target_id.clone(),source:None}}}]};
        let snapshot=db_snapshot::collect_with_config(&fixture.db,&fixture.config,&followup).await.unwrap().planning_snapshot().unwrap();let plan=dry_run(&followup,&snapshot).unwrap();
        let encoded=wire::encode_plan(&followup,&snapshot,&plan,None).unwrap();let imported=wire::decode_plan(&encoded).unwrap();assert!(imported.plan()==&plan);
        let mut forged=migrated.items[0].clone();forged.migration_points[0].planning_projection=Some(records::NamespacePlanningProjection {legacy_record:store::pinning::management::native::NativeKey {table:"pin_legacy_migrations".into(),values:vec!["client-proposal".into()]},allocation:point.destination_allocation.clone(),execution_route:point.destination_route.clone()});
        let tx=fixture.db.begin().await.unwrap();assert!(records::write_committed_action_in_transaction(&tx,&fixture.auth,&confirmed,&forged,confirmed.confirmation.native_frame.as_ref().unwrap()).await.is_err());tx.rollback().await.unwrap();
        let source_route=RouteIdentity::from_original(point.source_route.clone());
        let retire=PlanRequest {plan_id:"retire-migrated".into(),items:vec![ItemRequest {item_id:"domain".into(),action:Action::RetireWith {selection:RetirementSelection::Migrate {targets:vec![RetirementMigration {plan_id:"migration".into(),item_id:"object".into(),lease_id:lease_id.clone(),source_target_id:source.id.clone(),cid:point.cid.clone(),destination}]}},subject:Subject::Provider {route:source_route}}]};
        let confirmed=fixture.confirm(&retire).await;
        actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
        assert_eq!(records::read_retirement(&fixture.db,&fixture.auth,&point.source_allocation).await.unwrap().unwrap().disposition,records::Disposition::Migrate);
        assert!(remote_pin::Entity::find_by_id((point.source_allocation.clone(),source.cid)).one(&fixture.db).await.unwrap().is_some());
    }
}

#[tokio::test]
async fn default_retire_retains_unknown_debt_and_stops_new_allocations() {
    let fixture=support::Fixture::new().await;
    fixture.publish("unknown-debt",true).await;
    let provider=&fixture.config.pinning.providers[0];let route=RouteIdentity::from_original(provider.route_snapshot());
    let request=PlanRequest {plan_id:"retire".into(),items:vec![ItemRequest {item_id:"domain".into(),action:Action::Retire,subject:Subject::Provider {route}}]};
    let confirmed=fixture.confirm(&request).await;let result=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    assert_eq!(result.items[0].state,records::ActionState::NeedsAttention);
    assert!(result.items[0].receipt.job_ids.is_empty());
    let retired=records::read_retirement(&fixture.db,&fixture.auth,&provider.name).await.unwrap().unwrap();assert_eq!(retired.disposition,records::Disposition::Retain);
    assert!(!retired.unresolved.is_empty());
    assert!(store::entities::pin_provider_route::Entity::find_by_id(provider.name.clone()).one(&fixture.db).await.unwrap().unwrap().retired);
    assert_eq!(pin_provider_usage::Entity::find_by_id(provider.name.clone()).one(&fixture.db).await.unwrap().unwrap().reserved_bytes,10);
}

#[tokio::test]
async fn controlled_cleanup_is_an_exact_confirmed_existing_privilege_not_an_apply_parameter() {
    let fixture=support::Fixture::new_with_cleanup(true).await;
    fixture.publish("owned",true).await;
    let provider=&fixture.config.pinning.providers[0];
    let remote=remote_pin::Entity::find().one(&fixture.db).await.unwrap().unwrap();
    fixture.confirmed_remote(&remote.provider,&remote.cid).await;
    fixture.db.execute_unprepared("UPDATE pin_leases SET state='cancelled',generation=generation+1; UPDATE pin_lease_targets SET state='released'").await.unwrap();
    let route=RouteIdentity::from_original(provider.route_snapshot());
    let remote=remote_pin::Entity::find().one(&fixture.db).await.unwrap().unwrap();
    let canonical=ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
    let request=PlanRequest {plan_id:"cleanup".into(),items:vec![ItemRequest {item_id:"domain".into(),action:Action::RetireWith {selection:RetirementSelection::ControlledCleanup {resources:vec![RetirementResource {key:route.remote_key(&canonical),route:route.clone(),epoch:remote.epoch,resource_id:remote.request_id.clone().unwrap()}]}},subject:Subject::Provider {route}}]};
    let confirmed=fixture.confirm(&request).await;let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&confirmed).await.unwrap();
    assert_eq!(applied.items[0].jobs.len(),1);assert_eq!(applied.items[0].jobs[0].directive,records::JobDirective::ExistingCleanup);
    let job=pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();
    assert!(matches!(actions::job_authority::check_before_call(&fixture.db,&job).await.unwrap(),Some(actions::job_authority::JobPermit::ExistingCleanup(_))));
    assert!(actions::job_authority::projection_allowed(&fixture.db,&job).await.unwrap());
    assert!(actions::job_authority::cleanup_settlement_allowed(&fixture.db,&job).await.unwrap());
    // No DELETE is invoked in this source fixture. Intent acceptance and physical
    // deletion/accounting release remain separate surfaces for parent/C QA.
    assert_eq!(pin_provider_usage::Entity::find_by_id(provider.name.clone()).one(&fixture.db).await.unwrap().unwrap().reserved_bytes,10);
    assert!(remote_pin::Entity::find_by_id((remote.provider,remote.cid)).one(&fixture.db).await.unwrap().is_some());
}
