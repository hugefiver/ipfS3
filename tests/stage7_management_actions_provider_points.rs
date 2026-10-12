#[path="support/stage7_management_actions.rs"]
mod support;
use ipfs_s3_gateway::{pinning::management::*,store::{entities::pin_job,pinning::management::{actions::{self,provider_points::{self,Domain,Evidence,NamespaceChange,RecordClass}},native,records}}};
use sea_orm::{ConnectionTrait,EntityTrait};

fn namespace(fixture:&support::Fixture)->NamespaceChange {
    let provider=&fixture.config.pinning.providers[0];let route=provider.route_snapshot();
    let source=Domain {allocation:provider.name.clone(),backend:route.backend.clone(),scope:route.scope.clone(),resource_type:route.resource_type()};
    let destination=Domain {allocation:"declared-r26-destination".into(),..source.clone()};
    NamespaceChange {source,destination,destination_route:route}
}

#[tokio::test]
async fn typed_record_service_has_exact_pk_full_row_and_staged_applied_evidence_roles() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("point-service",false).await;
    let request=fixture.create_request("typed-points",identity,false);
    let binding=fixture.confirm(&request).await;
    let key=native::NativeKey {table:"pin_management_records".into(),values:vec![fixture.auth.database_scope().into(),request.plan_id.clone(),"object".into(),"action".into()]};
    let original=provider_points::original_record(&fixture.db,&key).await.unwrap().unwrap();
    let ns=namespace(&fixture);
    let staged=provider_points::inspect(&fixture.db,&key,&original,&ns).await.unwrap();
    assert!(staged.class()==RecordClass::Staged);assert!(staged.points().iter().all(|point|!matches!(point.evidence,Evidence::ExecutableJob(_)|Evidence::ExecutableSlot)));
    let applied=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&binding).await.unwrap();
    assert!(provider_points::inspect(&fixture.db,&key,&original,&ns).await.is_err(),"staged before-image cannot impersonate the now-applied exact DB row");
    let original=provider_points::original_record(&fixture.db,&key).await.unwrap().unwrap();
    let verified=provider_points::inspect(&fixture.db,&key,&original,&ns).await.unwrap();
    assert!(verified.class()==RecordClass::Applied);assert_eq!(verified.database_scope(),fixture.auth.database_scope());assert_eq!(verified.database_identity(),fixture.auth.database_identity());
    let signed=verified.points().iter().find(|point|matches!(point.evidence,Evidence::ExecutableJob(records::JobDirective::Submit))).unwrap();
    assert!(signed.job_authority.as_ref()==Some(&applied.items[0].jobs[0]));assert_eq!(signed.resource_key.as_ref().unwrap().table,"remote_pins");
    let mut forged=original.clone();forged.insert("principal".into(),Some("remote_client".into()));
    assert!(provider_points::inspect(&fixture.db,&key,&forged,&ns).await.is_err());
    let mut foreign=namespace(&fixture);foreign.source.scope="another-account".into();
    assert!(provider_points::inspect(&fixture.db,&key,&original,&foreign).await.is_err());
}

#[tokio::test]
async fn ordinary_job_lookup_does_not_decode_unrelated_opaque_management_results() {
    let fixture=support::Fixture::new().await;fixture.publish("ordinary",true).await;
    let job=pin_job::Entity::find().one(&fixture.db).await.unwrap().unwrap();
    // Valid registry metadata but intentionally invalid opaque action payload.
    // This foreign history remains visible to full native apply/planning fences;
    // it is neither parsed nor discarded to authorize an unrelated ordinary job.
    fixture.db.execute_unprepared("INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) SELECT database_scope,'unrelated','foreign','action','local_configuration',native_identity,'not-a-binding','not-before','not-a-result','[]','[]','applied',created_at,'2099-01-01T00:00:00Z',created_at FROM pin_management_records WHERE kind='database_scope'").await.unwrap();
    assert!(actions::job_authority::lookup(&fixture.db,&job).await.unwrap().is_none());
    assert!(actions::job_authority::check_before_call(&fixture.db,&job).await.unwrap().is_none());
}

#[tokio::test]
async fn evidence_records_are_structured_historical_origins_not_queue_authority() {
    let fixture=support::Fixture::new().await;fixture.publish("evidence-only",false).await;
    let frame=native::capture(&fixture.db).await.unwrap();
    let key=frame.frontier.iter().find(|key|key.table=="pin_management_records"&&key.values[3]=="evidence_preparation").unwrap();
    let original=provider_points::original_record(&fixture.db,key).await.unwrap().unwrap();
    let view=provider_points::inspect(&fixture.db,key,&original,&namespace(&fixture)).await.unwrap();
    assert!(view.class()==RecordClass::Evidence);assert_eq!(view.native_origins().len(),2);assert!(view.points().is_empty());
}

fn canonical_destination(namespace:&mut NamespaceChange) {
    let route=&namespace.destination_route;
    namespace.destination.allocation=format!("domain:{}",serde_json::to_string(&(&route.backend,&route.scope,route.resource_type())).unwrap());
}

#[tokio::test]
async fn unrelated_record_preserves_r26_historical_source_bound_without_widening_provider_fields() {
    let fixture=support::Fixture::new().await;fixture.publish("namespace-bound",false).await;
    let frame=native::capture(&fixture.db).await.unwrap();
    let key=frame.frontier.iter().find(|key|key.table=="pin_management_records"&&key.values[3]=="evidence_preparation").unwrap();
    let original=provider_points::original_record(&fixture.db,key).await.unwrap().unwrap();
    let calls=fixture.server.received_requests().await.unwrap().len();
    // The full original record remains authenticated even with zero related
    // provider points; its existence must not narrow R26's legacy coordinates.
    for source in ["a".repeat(300),"a".repeat(1024),"é".repeat(512)] {
        let mut ns=namespace(&fixture);ns.source.allocation=source;canonical_destination(&mut ns);
        let view=provider_points::inspect(&fixture.db,key,&original,&ns).await.unwrap();
        assert!(view.class()==RecordClass::Evidence);assert!(view.points().is_empty());assert_eq!(view.native_origins().len(),2);
        assert!(view.record_key()==key);assert_eq!(view.database_scope(),fixture.auth.database_scope());
    }
    for source in [String::new(),"a".repeat(1025),"é".repeat(513)] {
        let mut ns=namespace(&fixture);ns.source.allocation=source;canonical_destination(&mut ns);
        assert!(provider_points::inspect(&fixture.db,key,&original,&ns).await.is_err());
    }
    // Canonical domains include JSON framing and resource type. Their complete
    // coordinate may exceed 255 while each original provider field still fits.
    let mut bounded=namespace(&fixture);bounded.source.allocation="a".repeat(300);
    bounded.source.scope="s".repeat(255);bounded.destination.scope=bounded.source.scope.clone();bounded.destination_route.scope=bounded.source.scope.clone();canonical_destination(&mut bounded);
    assert!(bounded.destination.allocation.len()>255);
    let view=provider_points::inspect(&fixture.db,key,&original,&bounded).await.unwrap();assert!(view.points().is_empty());
    let mut oversized_scope=namespace(&fixture);oversized_scope.source.scope="s".repeat(256);oversized_scope.destination.scope=oversized_scope.source.scope.clone();oversized_scope.destination_route.scope=oversized_scope.source.scope.clone();canonical_destination(&mut oversized_scope);
    assert!(provider_points::inspect(&fixture.db,key,&original,&oversized_scope).await.is_err());
    let mut oversized_backend=namespace(&fixture);oversized_backend.source.backend="b".repeat(256);oversized_backend.destination.backend=oversized_backend.source.backend.clone();oversized_backend.destination_route.backend=oversized_backend.source.backend.clone();canonical_destination(&mut oversized_backend);
    assert!(provider_points::inspect(&fixture.db,key,&original,&oversized_backend).await.is_err());
    let mut oversized_identifier=namespace(&fixture);oversized_identifier.destination.allocation="new-provider".repeat(24);
    assert!(oversized_identifier.destination.allocation.len()>255);assert!(provider_points::inspect(&fixture.db,key,&original,&oversized_identifier).await.is_err());
    let mut malformed=namespace(&fixture);canonical_destination(&mut malformed);malformed.destination.allocation.push(' ');
    assert!(provider_points::inspect(&fixture.db,key,&original,&malformed).await.is_err());
    let mut wrong_route=namespace(&fixture);canonical_destination(&mut wrong_route);wrong_route.destination_route.scope="foreign-account".into();
    assert!(provider_points::inspect(&fixture.db,key,&original,&wrong_route).await.is_err());
    let mut wrong_resource=namespace(&fixture);canonical_destination(&mut wrong_resource);wrong_resource.destination_route.api_profile="rpc".into();canonical_destination(&mut wrong_resource);
    assert!(provider_points::inspect(&fixture.db,key,&original,&wrong_resource).await.is_err());
    let mut controls=namespace(&fixture);controls.source.scope="legacy:\n".into();
    assert!(provider_points::inspect(&fixture.db,key,&original,&controls).await.is_err());
    assert!(provider_points::original_record(&fixture.db,key).await.unwrap().as_ref()==Some(&original));
    assert_eq!(fixture.server.received_requests().await.unwrap().len(),calls);
}
