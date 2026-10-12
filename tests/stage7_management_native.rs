#[path="support/stage7_management.rs"]
mod support;
#[path="support/stage7_scope_isolation.rs"]
mod scope_isolation;
use support::*;

#[tokio::test]
async fn historical_missing_route_is_an_owned_unknown_item_not_unsupported_planning() {
    let raw=config("http://127.0.0.1:1");let config=management_config(&raw);
    let (_directory,db)=database().await;setup(&db,&config).await;
    let identity=publish(&db,&config,"legacy",true).await;
    let frame=native::capture(&db).await.unwrap();
    let target_id=native::value(&frame.table("pin_lease_targets").unwrap().rows[0],"id").unwrap().to_owned();
    // Reconstruct a pre-birth fixture, not a production alteration or backfill.
    db.execute_unprepared("DROP TRIGGER pin_lease_requirements_no_delete").await.unwrap();
    db.execute_unprepared("DELETE FROM pin_lease_requirements").await.unwrap();
    db.execute_unprepared("CREATE TRIGGER pin_lease_requirements_no_delete BEFORE DELETE ON pin_lease_requirements BEGIN SELECT RAISE(ABORT, 'immutable pin history'); END").await.unwrap();
    db.execute_unprepared("UPDATE remote_pin_ledger SET route=NULL").await.unwrap();
    db.execute_unprepared("DELETE FROM pin_provider_routes").await.unwrap();
    let request=PlanRequest {plan_id:"legacy-route".into(),items:vec![ItemRequest {item_id:"owned".into(),action:Action::Retry,subject:Subject::Object {identity,target:TargetSelection::Existing {target_id:target_id.clone(),source:None}}}]};
    let snapshot=db_snapshot::collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();
    assert_eq!(snapshot.targets.len(),1);assert!(snapshot.targets[0].route.is_none());
    assert!(snapshot.native_frame.as_ref().unwrap().row("pin_lease_targets",&[&target_id]).is_some());
    let plan=dry_run(&request,&snapshot).unwrap();
    assert_eq!(plan.items()[0].status,PlanStatus::Blocked(SafeError::UnknownEvidence));
}
use ipfs_s3_gateway::{pinning::management::{*,db_snapshot::{collect_with_config,collect}},store::{self,pinning::management::{native,records}}};
use sea_orm::{ConnectionTrait,Statement,DatabaseBackend};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock,MockServer,ResponseTemplate,matchers::{method,path,query_param}};

async fn source(server:&MockServer){
    Mock::given(method("POST")).and(path("/api/v0/id")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":NODE}))).mount(server).await;
    Mock::given(method("POST")).and(path("/api/v0/cat")).and(query_param("offline","true")).respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789")).mount(server).await;
}

#[tokio::test]
async fn full_inventory_unknown_alias_blocks_same_cid_but_proven_disjoint_resources_do_not() {
    for case in ["unknown-same-cid","unknown-other-cid","known-other-domain"] {
        let server=MockServer::start().await;source(&server).await;
        let raw=config(&server.uri());let config=management_config(&raw);
        let (_directory,db)=database().await;setup(&db,&config).await;
        let identity=publish(&db,&config,"selected",false).await;write_current_decision(&db,&config,&identity).await;
        let other_cid="QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX";
        let row_cid=if case=="unknown-other-cid" {other_cid}else{CID};
        let historical_route=if case=="known-other-domain" {
            let mut route=config.pinning.providers[0].route_snapshot();route.provider_id="other-provider".into();route.scope="different-account".into();
            Some(serde_json::to_string(&route).unwrap())
        }else{None};
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES('unknown-old-alias',$1,$2,'unknown','unknown')",[row_cid.into(),historical_route.into()])).await.unwrap();
        let auth=records::authorize_local(&raw,&db).await.unwrap();
        evidence::prepare(&db,&config,&auth,&ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),&evidence::PrepareRequest {record_id:"proof".into(),identity:identity.clone(),expected_node:NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false},CancellationToken::new()).await.unwrap();
        let request=support::request(identity,&config,false);let before=native::capture(&db).await.unwrap();
        let snapshot=collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();
        let plan=dry_run(&request,&snapshot).unwrap();
        let key=NativeKey {table:"remote_pin_ledger".into(),values:vec!["unknown-old-alias".into(),row_cid.into()]};
        assert!(snapshot.native_frame.as_ref().unwrap().frontier.contains(&key));
        if case=="unknown-same-cid" {
            assert!(snapshot.unknown_resources.contains(&key));
            assert_eq!(plan.items()[0].status,PlanStatus::Blocked(SafeError::UnknownEvidence));assert!(plan.items()[0].step.is_none());
        }else{
            assert!(snapshot.unknown_resources.contains(&key));
            assert_eq!(plan.items()[0].status,PlanStatus::Ready);
            let Some(PlannedStep::Object(step))=&plan.items()[0].step else{panic!("object plan")};assert_eq!(step.source,RemoteDirective::SubmitWithFence);
        }
        assert!(before==native::capture(&db).await.unwrap());
    }
}

#[tokio::test]
async fn native_trusted_manifest_backfill_is_ready_but_legacy_zip_still_requires_item_ack() {
    let server=MockServer::start().await;source(&server).await;
    let mut raw=config(&server.uri());raw.decompress_zip.auto_pin_outputs=Some(true);
    let config=management_config(&raw);let (_directory,db)=database().await;setup(&db,&config).await;
    let trusted=publish(&db,&config,"outputs/trusted",false).await;
    let legacy=publish(&db,&config,"outputs/legacy",false).await;
    db.execute_unprepared("INSERT INTO zip_batches(id,owner,source,token,fingerprint,bucket,archive_key,input_identity,captured_options,state,manifest_prepared,terminal_result,root_status,created_at,updated_at) VALUES('trusted-batch','original-archive-owner','direct','trusted-token','trusted-fingerprint','bucket','archive.zip','{}','{}','published',TRUE,'{}','disabled','2026-10-12T00:00:00Z','2026-10-12T00:00:00Z')").await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"INSERT INTO zip_manifest_entries(batch_id,path,object_key,cid,size,version_row_id,created_at) VALUES('trusted-batch','trusted',$1,$2,10,$3,'2026-10-12T00:00:00Z')",[trusted.key.clone().into(),CID.into(),trusted.private_version_id.clone().into()])).await.unwrap();
    let auth=records::authorize_local(&raw,&db).await.unwrap();
    for (id,identity) in [("trusted-proof",&trusted),("legacy-proof",&legacy)] {
        evidence::prepare(&db,&config,&auth,&ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),&evidence::PrepareRequest {record_id:id.into(),identity:identity.clone(),expected_node:NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false},CancellationToken::new()).await.unwrap();
    }
    let mut request=support::request(trusted.clone(),&config,true);request.plan_id="manifest-and-legacy".into();request.items[0].item_id="trusted".into();
    let mut legacy_item=support::request(legacy,&config,true).items.remove(0);legacy_item.item_id="legacy".into();request.items.push(legacy_item);
    let snapshot=collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();let plan=dry_run(&request,&snapshot).unwrap();
    assert!(matches!(snapshot.objects.iter().find(|object|object.identity==trusted).unwrap().evidence,SelectionEvidence::TrustedManifest {..}));
    assert_eq!(plan.items().iter().find(|entry|entry.request.item_id=="trusted").unwrap().status,PlanStatus::Ready);
    assert_eq!(plan.items().iter().find(|entry|entry.request.item_id=="legacy").unwrap().status,PlanStatus::Candidate);
    assert!(records::confirm(&db,&config,&auth,&request,&snapshot,&plan,vec![],chrono::Duration::minutes(2)).await.is_err());
    assert!(records::confirm(&db,&config,&auth,&request,&snapshot,&plan,vec!["trusted".into()],chrono::Duration::minutes(2)).await.is_err());
    records::confirm(&db,&config,&auth,&request,&snapshot,&plan,vec!["legacy".into()],chrono::Duration::minutes(2)).await.unwrap();
}

#[tokio::test]
async fn actual_metadata_then_clean_offline_eof_produces_ready_and_candidate_not_boolean_claims(){
    let server=MockServer::start().await;source(&server).await;
    let raw=config(&server.uri());let config=management_config(&raw);
    let (_directory,db)=database().await;setup(&db,&config).await;
    let identity=publish(&db,&config,"object",false).await;write_current_decision(&db,&config,&identity).await;
    let request=request(identity.clone(),&config,false);
    let before=native::capture(&db).await.unwrap();
    let snapshot=collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(&request,&snapshot).unwrap();
    assert_eq!(plan.items()[0].status,PlanStatus::Blocked(SafeError::UnreadableObject));
    assert_eq!(snapshot.objects[0].readable,None);
    assert!(before==native::capture(&db).await.unwrap());
    assert!(server.received_requests().await.unwrap().is_empty());
    let authorization=records::authorize_local(&raw,&db).await.unwrap();
    let proof=evidence::prepare(&db,&config,&authorization,&ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),&evidence::PrepareRequest{record_id:"proof".into(),identity:identity.clone(),expected_node:NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false},CancellationToken::new()).await.unwrap();
    assert_eq!(proof.stored_bytes,Some(10));
    let after=native::capture(&db).await.unwrap();
    assert!(before.evidence_inventory()==after.evidence_inventory());
    let snapshot=collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(&request,&snapshot).unwrap();
    assert_eq!(plan.items()[0].status,PlanStatus::Ready);
    let confirmed=records::confirm(&db,&config,&authorization,&request,&snapshot,&plan,vec![],chrono::Duration::minutes(1)).await.unwrap();
    confirmed.check(&db,&authorization).await.unwrap();
    assert!(records::lookup_applied(&db,&authorization,&confirmed,"object").await.unwrap().is_none());
    let mut candidate=support::request(identity,&config,true);candidate.plan_id="candidate-fixture".into();
    let snapshot=collect_with_config(&db,&config,&candidate).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(&candidate,&snapshot).unwrap();assert_eq!(plan.items()[0].status,PlanStatus::Candidate);
    assert!(records::confirm(&db,&config,&authorization,&candidate,&snapshot,&plan,vec![],chrono::Duration::minutes(1)).await.is_err());
    assert!(records::confirm(&db,&config,&authorization,&candidate,&snapshot,&plan,vec!["object".into()],chrono::Duration::minutes(1)).await.is_ok());
    assert_eq!(server.received_requests().await.unwrap().iter().filter(|r|r.url.path()=="/api/v0/cat").count(),1);
}

#[tokio::test]
async fn same_count_different_members_and_sqlite_nanoseconds_change_native_fence(){
    let raw=config("http://127.0.0.1:1");let config=management_config(&raw);
    let (_directory,db)=database().await;setup(&db,&config).await;
    let identity=publish(&db,&config,"owner",true).await;let request=request(identity,&config,false);
    let original=collect(&db,&config.pinning,&request).await.unwrap().planning_snapshot().unwrap();
    let target=original.native_frame.as_ref().unwrap().table("pin_lease_targets").unwrap().rows[0].clone();
    let id=native::value(&target,"id").unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"UPDATE pin_lease_targets SET last_touched_at='2026-10-12T00:00:00.000000001Z' WHERE id=$1",[id.into()])).await.unwrap();
    let first=native::capture(&db).await.unwrap();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"UPDATE pin_lease_targets SET last_touched_at='2026-10-12T00:00:00.000000002Z' WHERE id=$1",[id.into()])).await.unwrap();
    let second=native::capture(&db).await.unwrap();assert!(first!=second);
    assert_eq!(native::value(second.row("pin_lease_targets",&[id]).unwrap(),"last_touched_at"),Some("2026-10-12T00:00:00.000000002Z"));
    let before=native::capture(&db).await.unwrap();
    db.execute_unprepared("UPDATE pin_jobs SET id='different-job' WHERE id=(SELECT id FROM pin_jobs LIMIT 1)").await.unwrap();
    let after=native::capture(&db).await.unwrap();
    assert_eq!(before.table("pin_jobs").unwrap().rows.len(),after.table("pin_jobs").unwrap().rows.len());assert!(before.frontier!=after.frontier);
    assert!(records::recheck_frame(&db,&before).await.is_err());
}

#[tokio::test]
async fn header_failure_budget_failure_node_claim_and_changed_source_never_make_ready(){
    let server=MockServer::start().await;
    Mock::given(path("/api/v0/id")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":NODE}))).mount(&server).await;
    Mock::given(path("/api/v0/cat")).respond_with(ResponseTemplate::new(200).insert_header("X-Stream-Error","DO_NOT_OUTPUT").set_body_bytes(b"0123456789")).mount(&server).await;
    let raw=config(&server.uri());let config=management_config(&raw);
    let (_directory,db)=database().await;setup(&db,&config).await;
    let identity=publish(&db,&config,"object",false).await;
    let auth=records::authorize_local(&raw,&db).await.unwrap();let before=native::capture(&db).await.unwrap();
    let request=evidence::PrepareRequest{record_id:"failed".into(),identity:identity.clone(),expected_node:NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false};
    assert!(evidence::prepare(&db,&config,&auth,&ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),&request,CancellationToken::new()).await.is_err());
    assert!(before==native::capture(&db).await.unwrap());
    let selected=support::request(identity,&config,false);
    let snapshot=collect_with_config(&db,&config,&selected).await.unwrap().planning_snapshot().unwrap();assert_eq!(snapshot.objects[0].readable,None);
}

#[tokio::test]
async fn unknown_columns_missing_schema_and_whole_inventory_budget_fail_readonly(){
    let raw=config("http://127.0.0.1:1");let config=management_config(&raw);
    let db=store::connect_database("sqlite::memory:").await.unwrap();setup(&db,&config).await;
    db.execute_unprepared("ALTER TABLE pin_management_records ADD COLUMN unrecognized TEXT").await.unwrap();
    assert!(native::capture(&db).await.is_err());
    let empty=store::connect_database("sqlite::memory:").await.unwrap();assert!(native::capture(&empty).await.is_err());
    assert!(empty.query_all(Statement::from_string(DatabaseBackend::Sqlite,"SELECT name FROM sqlite_schema WHERE type='table'")).await.unwrap().is_empty());
}
