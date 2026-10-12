#[path="support/stage7_management_actions.rs"]
mod support;

use ipfs_s3_gateway::{pinning::management::*,store::{self,entities::pin_lease,pinning::management::{actions,native,records,ManagementError,ManagementJobScope}}};
use sea_orm::{ConnectionTrait,EntityTrait,Statement};

fn archive(request:&PlanRequest,snapshot:&Snapshot,ack:Vec<String>)->wire::ImportedPlan {
    let plan=dry_run(request,snapshot).unwrap();
    let bytes=wire::encode_plan(request,snapshot,&plan,Some(&plan.confirmation(ack))).unwrap();
    wire::decode_plan(&bytes).unwrap()
}

async fn confirmed_archive(fixture:&support::Fixture,request:&PlanRequest,ttl:chrono::Duration)->(wire::ImportedPlan,records::ConfirmedBinding,Snapshot) {
    let snapshot=db_snapshot::collect_with_config(&fixture.db,&fixture.config,request).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(request,&snapshot).unwrap();
    let ack=plan.items().iter().filter(|entry|entry.status==PlanStatus::Candidate).map(|entry|entry.request.item_id.clone()).collect();
    let imported=archive(request,&snapshot,ack);
    records::stage_confirmation(&fixture.db,&fixture.config,&fixture.auth,request,&snapshot,&plan,ttl).await.unwrap();
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported).await.is_err(),"a prepared draft is not a persisted confirmation");
    let binding=records::confirm_imported(&fixture.db,&fixture.config,&fixture.auth,&imported).await.unwrap();
    (imported,binding,snapshot)
}

async fn wait_for_real_confirmation_expiry(fixture:&support::Fixture,binding:&records::ConfirmedBinding) {
    tokio::time::timeout(std::time::Duration::from_secs(6),async {
        while store::database_clock::database_now(&fixture.db).await.unwrap()<binding.expires_at {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("the real database clock did not cross the original confirmation expiry");
}

#[tokio::test]
async fn expired_committed_archive_restores_private_original_for_replay_status_and_exact_scope() {
    let mut fixture=support::Fixture::new().await;let identity=fixture.publish("historical-committed",false).await;
    let request=fixture.create_request("historical-committed",identity,false);
    let (imported,binding,_)=confirmed_archive(&fixture,&request,chrono::Duration::seconds(5)).await;
    let committed=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&binding).await.unwrap();
    // Legitimate post-apply business drift does not rewrite the archived tuple.
    fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"later\":true}' WHERE key='historical-committed'").await.unwrap();
    wait_for_real_confirmation_expiry(&fixture,&binding).await;
    fixture.db.clone().close().await.unwrap();
    let path=fixture.directory.path().join("management.sqlite");
    fixture.db=store::connect_database(&format!("sqlite://{}?mode=rw",path.to_string_lossy().replace('\\',"/"))).await.unwrap();
    fixture.auth=records::authorize_local(&fixture.raw,&fixture.db).await.unwrap();
    let before=native::capture(&fixture.db).await.unwrap();
    let restored=records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported).await.unwrap();
    assert!(restored==binding);assert!(restored.confirmation.native_frame.as_ref().unwrap().has_private_material());
    assert!(!imported.confirmation().unwrap().native_frame.as_ref().unwrap().has_private_material());
    assert!(native::capture(&fixture.db).await.unwrap()==before);
    assert!(matches!(restored.check(&fixture.db,&fixture.auth).await,Err(ManagementError::Expired)));
    assert!(matches!(records::confirm_imported(&fixture.db,&fixture.config,&fixture.auth,&imported).await,Err(ManagementError::Expired)));
    let replay=actions::apply(&fixture.db,&fixture.config,&fixture.auth,&restored).await.unwrap();assert!(replay.replayed);assert!(replay.items==committed.items);
    assert!(actions::status::lookup(&fixture.db,&fixture.auth,&restored,"object").await.unwrap().unwrap().receipt==committed.items[0].receipt);
    assert!(records::lookup_applied_from_imported(&fixture.db,&fixture.auth,&imported,"object").await.unwrap().unwrap()==committed.items[0].receipt);
    assert!(ManagementJobScope::from_applied(&fixture.db,&fixture.auth,&restored).await.is_ok());
    assert!(native::capture(&fixture.db).await.unwrap()==before);
}

#[tokio::test]
async fn expired_unapplied_archive_is_readable_but_cannot_grant_apply_or_worker_scope() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("historical-unapplied",false).await;
    let request=fixture.create_request("historical-unapplied",identity,false);
    let (imported,binding,_)=confirmed_archive(&fixture,&request,chrono::Duration::seconds(5)).await;
    wait_for_real_confirmation_expiry(&fixture,&binding).await;
    let before=native::capture(&fixture.db).await.unwrap();
    let restored=records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported).await.unwrap();assert!(restored==binding);
    assert!(matches!(actions::apply(&fixture.db,&fixture.config,&fixture.auth,&restored).await,Err(ManagementError::Expired)));
    assert!(ManagementJobScope::from_applied(&fixture.db,&fixture.auth,&restored).await.is_err());
    assert!(records::lookup_applied_from_imported(&fixture.db,&fixture.auth,&imported,"object").await.unwrap().is_none());
    assert!(matches!(records::confirm_imported(&fixture.db,&fixture.config,&fixture.auth,&imported).await,Err(ManagementError::Expired)));
    assert!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().is_empty());assert!(native::capture(&fixture.db).await.unwrap()==before);
}

#[tokio::test]
async fn loader_rejects_changed_public_fields_candidate_ack_and_applied_claims() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("historical-candidate",false).await;
    let request=fixture.create_request("historical-candidate",identity,true);
    let (imported,binding,snapshot)=confirmed_archive(&fixture,&request,chrono::Duration::minutes(5)).await;
    let before=native::capture(&fixture.db).await.unwrap();
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported).await.unwrap()==binding);
    let missing_ack=archive(&request,&snapshot,vec![]);
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&missing_ack).await.is_err());
    let mut changed=request.clone();changed.plan_id="other-locator".into();
    let locator=archive(&changed,&snapshot,vec!["object".into()]);
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&locator).await.is_err());
    changed=request.clone();let Action::Backfill(intent)=&mut changed.items[0].action else{unreachable!()};intent.expires_at+=1;
    let request_change=archive(&changed,&snapshot,vec!["object".into()]);
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&request_change).await.is_err());
    let mut changed_snapshot=snapshot.clone();changed_snapshot.observed_at+=1;
    let snapshot_change=archive(&request,&changed_snapshot,vec!["object".into()]);
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&snapshot_change).await.is_err());
    let mut claimed=snapshot.clone();claimed.applied=vec![AppliedItem {binding:binding.confirmation.bindings[0].clone()}];
    let claims=archive(&request,&claimed,vec!["object".into()]);assert_eq!(claims.untrusted_applied_claims().len(),1);
    assert!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&claims).await.is_err());
    assert!(ManagementJobScope::from_applied(&fixture.db,&fixture.auth,&binding).await.is_err());assert!(native::capture(&fixture.db).await.unwrap()==before);
}

#[tokio::test]
async fn copied_original_private_frame_and_forged_row_headers_do_not_authenticate_another_database() {
    let fixture=support::Fixture::new().await;let identity=fixture.publish("copy-original",false).await;
    let request=fixture.create_request("copy-original",identity,false);
    let (imported,_,_)=confirmed_archive(&fixture,&request,chrono::Duration::minutes(5)).await;
    let row=fixture.db.query_one(Statement::from_sql_and_values(fixture.db.get_database_backend(),"SELECT binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at FROM pin_management_records WHERE database_scope=$1 AND plan_id=$2 AND item_id='object' AND kind='action'",[fixture.auth.database_scope().into(),request.plan_id.clone().into()])).await.unwrap().unwrap();
    let other=support::Fixture::new().await;
    // Adversarial fixture: copy the complete original blobs and forge only their
    // indexed scalar headers. The private original still belongs to the first DB.
    other.db.execute(Statement::from_sql_and_values(other.db.get_database_backend(),"INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,principal,native_identity,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES ($1,$2,'object','action',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",[
        other.auth.database_scope().into(),request.plan_id.into(),other.auth.principal().into(),other.auth.database_identity().into(),
        row.try_get::<String>("","binding").unwrap().into(),row.try_get::<String>("","native_before").unwrap().into(),row.try_get::<Option<String>>("","result").unwrap().into(),
        row.try_get::<String>("","intent_ids").unwrap().into(),row.try_get::<String>("","job_ids").unwrap().into(),row.try_get::<String>("","state").unwrap().into(),
        row.try_get::<chrono::DateTime<chrono::Utc>>("","created_at").unwrap().into(),row.try_get::<chrono::DateTime<chrono::Utc>>("","expires_at").unwrap().into(),row.try_get::<Option<chrono::DateTime<chrono::Utc>>>("","completed_at").unwrap().into(),
    ])).await.unwrap();
    let before=native::capture(&other.db).await.unwrap();
    assert!(matches!(records::load_confirmed_from_imported(&other.db,&other.auth,&imported).await,Err(ManagementError::Unauthorized)));
    assert!(records::load_confirmed_from_imported(&other.db,&fixture.auth,&imported).await.is_err());
    assert!(native::capture(&other.db).await.unwrap()==before);
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_management_records SET principal='different-principal' WHERE plan_id=$1 AND kind='action'",["copy-original".into()])).await.unwrap();
    assert!(matches!(records::load_confirmed_from_imported(&fixture.db,&fixture.auth,&imported).await,Err(ManagementError::Unauthorized)));
}
