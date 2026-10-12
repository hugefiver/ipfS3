#[path = "support/stage7_management.rs"]
mod support;

use ipfs_s3_gateway::{
    pinning::management::{self as management, db_snapshot, evidence, wire, *},
    store::{self, pinning::management::{native, records}},
};
use sea_orm::{ConnectionTrait, EntityTrait, Set, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path, query_param}};

#[tokio::test]
async fn file_confirmation_uses_trusted_original_and_applied_receipt_not_client_locator() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE})))
        .mount(&server).await;
    Mock::given(method("POST")).and(path("/api/v0/cat")).and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server).await;
    let raw = support::config(&server.uri());
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish(&db, &config, "exact", false).await;
    db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"DO_NOT_EXPORT_NATIVE_METADATA\"}'").await.unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(&db, &config, &auth, &ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),
        &evidence::PrepareRequest {record_id:"proof".into(),identity:identity.clone(),expected_node:support::NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false},
        CancellationToken::new()).await.unwrap();
    let request = support::request(identity, &config, true);
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Candidate);
    let acknowledgement = plan.confirmation(vec!["object".into()]);
    let bytes = wire::encode_plan(&request, &snapshot, &plan, Some(&acknowledgement)).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("DO_NOT_EXPORT_NATIVE_METADATA"));
    assert!(!String::from_utf8_lossy(&bytes).contains(&server.uri()));
    let imported = wire::decode_plan(&bytes).unwrap();
    assert!(!imported.plan().native_frame().unwrap().has_private_material());
    assert!(records::confirm_imported(&db, &config, &auth, &imported).await.is_err());

    records::stage_confirmation(&db, &config, &auth, &request, &snapshot, &plan, chrono::Duration::minutes(2)).await.unwrap();
    let binding = records::confirm_imported(&db, &config, &auth, &imported).await.unwrap();
    assert!(binding.confirmation.native_frame.as_ref().unwrap().has_private_material());
    let resolved = records::confirm_imported(&db, &config, &auth, &wire::decode_plan(&bytes).unwrap()).await.unwrap();
    assert!(resolved == binding);
    let tx = db.begin().await.unwrap();
    records::recheck_confirmed_frame(&tx, &auth, &binding).await.unwrap();
    records::revalidate_confirmed_in_transaction(&tx, &config, &auth, &binding).await.unwrap();
    let forged_receipt=records::ActionReceipt {binding:plan.items()[0].clone(),intent_ids:vec!["locator-only".into()],job_ids:vec![]};
    assert!(records::write_action_receipt_in_transaction(&tx,&auth,&binding,&forged_receipt,binding.confirmation.native_frame.as_ref().unwrap()).await.is_err());
    let Subject::Object {identity,..}= &request.items[0].subject else{unreachable!()};
    let now=store::database_clock::database_now(&tx).await.unwrap();
    store::entities::pin_lease::Entity::insert(store::entities::pin_lease::ActiveModel {
        id:Set("actual-intent".into()),owner_object_id:Set(identity.object_id.clone()),source:Set("manual".into()),policy_id:Set(config.pinning.policies[0].identity.clone()),provider_mode:Set("all".into()),content_mode:Set("object".into()),created_at:Set(now),last_touched_at:Set(now),expires_at:Set(now+chrono::Duration::hours(1)),generation:Set(1),state:Set("active".into()),
    }).exec(&tx).await.unwrap();
    let receipt = records::ActionReceipt {binding:plan.items()[0].clone(),intent_ids:vec!["actual-intent".into()],job_ids:vec![]};
    records::write_action_receipt_in_transaction(&tx, &auth, &binding, &receipt, binding.confirmation.native_frame.as_ref().unwrap()).await.unwrap();
    tx.commit().await.unwrap();
    let stored = records::lookup_applied(&db, &auth, &binding, "object").await.unwrap().unwrap();
    assert!(stored == receipt);
    assert!(records::lookup_applied_from_imported(&db,&auth,&imported,"object").await.unwrap().unwrap()==receipt);
    let mut forged = binding.clone();
    forged.confirmation.candidate_item_ids.clear();
    assert!(records::lookup_applied(&db, &auth, &forged, "object").await.is_err());

    let (_other_directory, other) = support::database().await;
    support::setup(&other, &config).await;
    other.execute_unprepared(&format!("UPDATE pin_management_records SET database_scope='{}' WHERE kind='database_scope'", auth.database_scope())).await.unwrap();
    assert!(auth.check(&other).await.is_err());
    let memory = store::connect_database("sqlite::memory:").await.unwrap();
    support::setup(&memory, &config).await;
    assert!(records::authorize_local(&raw, &memory).await.is_err());
    assert!(native::capture(&memory).await.is_ok());
    let _ = management::MAX_RECORDS;
}
