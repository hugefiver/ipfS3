#[path="support/stage7_management.rs"]
mod support;
use ipfs_s3_gateway::{kubo::KuboClient,pinning::management::{*,db_snapshot,evidence},store::pinning::management::{native,records}};
use sea_orm::ConnectionTrait;
use tokio::{io::{AsyncReadExt,AsyncWriteExt},net::TcpListener};
use tokio_util::sync::CancellationToken;
use wiremock::{Mock,MockServer,ResponseTemplate,matchers::path};

async fn stream_source(case:&'static str,cancel:CancellationToken)->(String,tokio::task::JoinHandle<()>) {
    let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint=format!("http://{}",listener.local_addr().unwrap());
    let task=tokio::spawn(async move {
        loop {
            let (mut socket,_)=listener.accept().await.unwrap();let mut request=Vec::new();
            loop {let byte=socket.read_u8().await.unwrap();request.push(byte);if request.ends_with(b"\r\n\r\n") {break;} assert!(request.len()<8192);}
            if String::from_utf8_lossy(&request).contains(" /api/v0/id") {
                let body=format!("{{\"ID\":\"{}\"}}",support::NODE);
                socket.write_all(format!("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }else{
                assert!(String::from_utf8_lossy(&request).contains("offline=true"));
                socket.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\n\r\n5\r\n01234\r\n").await.unwrap();
                match case {
                    "trailer"=>socket.write_all(b"5\r\n56789\r\n0\r\nX-Stream-Error: PRIVATE_ERROR\r\n\r\n").await.unwrap(),
                    "late_eof"=>{},
                    "cancel"=>{cancel.cancel();tokio::time::sleep(std::time::Duration::from_secs(1)).await;},
                    "idle"=>{tokio::time::sleep(std::time::Duration::from_secs(1)).await;},
                    _=>unreachable!(),
                }
            }
        }
    });(endpoint,task)
}

#[tokio::test]
async fn trailer_late_eof_idle_and_cancellation_never_persist_a_trusted_attestation() {
    for case in ["trailer","late_eof","idle","cancel"] {
        let cancel=CancellationToken::new();let (endpoint,task)=stream_source(case,cancel.clone()).await;
        let raw=support::config(&endpoint);let config=support::management_config(&raw);
        let (_directory,db)=support::database().await;support::setup(&db,&config).await;
        let identity=support::publish(&db,&config,"body",false).await;
        let auth=records::authorize_local(&raw,&db).await.unwrap();let before=native::capture(&db).await.unwrap();
        let client=KuboClient::new_with_timeouts(endpoint,std::time::Duration::from_secs(1),std::time::Duration::from_millis(30));
        let request=evidence::PrepareRequest {record_id:"failed".into(),identity,expected_node:support::NODE.into(),max_stored_bytes:10,ttl:chrono::Duration::minutes(5),directory:false};
        assert!(evidence::prepare(&db,&config,&auth,&client,&request,cancel).await.is_err(),"{case}");
        assert!(before==native::capture(&db).await.unwrap(),"{case}");
        task.abort();let _=task.await;
    }
}

#[tokio::test]
async fn stored_ciphertext_count_rid_immutability_expiry_and_config_claims_are_not_metadata_booleans() {
    let server=MockServer::start().await;
    Mock::given(path("/api/v0/id")).respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE}))).mount(&server).await;
    Mock::given(path("/api/v0/cat")).respond_with(ResponseTemplate::new(200).set_body_bytes(b"ciphertext-17byte")).mount(&server).await;
    let raw=support::config(&server.uri());let config=support::management_config(&raw);
    let (_directory,db)=support::database().await;support::setup(&db,&config).await;
    let identity=support::publish(&db,&config,"encrypted",false).await;
    db.execute_unprepared("UPDATE objects SET encrypted=TRUE").await.unwrap();
    let auth=records::authorize_local(&raw,&db).await.unwrap();let client=KuboClient::new(server.uri());
    let mut request=evidence::PrepareRequest {record_id:"ciphertext".into(),identity:identity.clone(),expected_node:support::NODE.into(),max_stored_bytes:0,ttl:chrono::Duration::minutes(5),directory:false};
    let before=native::capture(&db).await.unwrap();
    assert!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.is_err());
    assert!(server.received_requests().await.unwrap().is_empty());
    request.max_stored_bytes=16;
    assert!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.is_err());
    assert!(before==native::capture(&db).await.unwrap());
    request.max_stored_bytes=17;
    let proof=evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.unwrap();
    assert_eq!(proof.stored_bytes,Some(17)); // metadata remains plaintext size 10
    assert!(before.evidence_inventory()==native::capture(&db).await.unwrap().evidence_inventory());
    request.max_stored_bytes=18;
    assert!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.is_err());
    request.max_stored_bytes=17;
    let original_frame=native::capture(&db).await.unwrap();
    assert!(original_frame.row("objects",&[&identity.object_id]).unwrap().contains_key("metadata"));
    let original_metadata:Option<String>=original_frame.original_value("objects",&[&identity.object_id],"metadata").map(str::to_owned);
    assert!(original_metadata.is_none());
    let original_sql_null=db.query_one(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,"SELECT metadata IS NULL AS is_null FROM objects WHERE id=$1",[identity.object_id.clone().into()])).await.unwrap().unwrap().try_get::<i64>("","is_null").unwrap();
    assert_eq!(original_sql_null,1);
    db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"changed-before-rid-replay\"}'").await.unwrap();
    assert!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.is_err());
    let drift_request=support::request(identity.clone(),&config,true);
    assert_eq!(db_snapshot::collect_with_config(&db,&config,&drift_request).await.unwrap().planning_snapshot().unwrap().objects[0].readable,None);
    db.execute(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,"UPDATE objects SET metadata=$1 WHERE id=$2",[original_metadata.clone().into(),identity.object_id.clone().into()])).await.unwrap();
    let restored=native::capture(&db).await.unwrap();
    assert!(restored.original_value("objects",&[&identity.object_id],"metadata")==original_metadata.as_deref());
    let restored_sql_null=db.query_one(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,"SELECT metadata IS NULL AS is_null FROM objects WHERE id=$1",[identity.object_id.clone().into()])).await.unwrap().unwrap().try_get::<i64>("","is_null").unwrap();
    assert_eq!(restored_sql_null,original_sql_null);
    let changed=local_config::compile(&raw,|_|None).unwrap().with_source_node("different-node".into()).unwrap();
    let plan_request=support::request(identity.clone(),&config,true);
    let snapshot=db_snapshot::collect_with_config(&db,&changed,&plan_request).await.unwrap().planning_snapshot().unwrap();
    assert_eq!(snapshot.objects[0].readable,None);
    let mut raw_changed=raw.clone();raw_changed.kubo.rpc_url="http://127.0.0.1:1".into();
    let changed=support::management_config(&raw_changed);
    assert_eq!(db_snapshot::collect_with_config(&db,&changed,&plan_request).await.unwrap().planning_snapshot().unwrap().objects[0].readable,None);
    db.execute_unprepared("UPDATE pin_management_records SET result=json_set(result,'$[0].expires_at','2000-01-01T00:00:00Z') WHERE kind='evidence_preparation'").await.unwrap();
    assert_eq!(db_snapshot::collect_with_config(&db,&config,&plan_request).await.unwrap().planning_snapshot().unwrap().objects[0].readable,None);
    assert!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await.is_err());
    let replacement_cid="QmYwAPJzv5CZsnAzt8auVZRnGiRAz1F7TaqzM9zECsFcdX";
    let before_cid_claim=native::capture(&db).await.unwrap();
    let rpc_before=server.received_requests().await.unwrap().len();
    request.identity.cid=ipfs_s3_gateway::pinning::provider::canonical_resource_cid(replacement_cid).unwrap();
    assert!(matches!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await,Err(ipfs_s3_gateway::store::pinning::management::ManagementError::Changed)));
    assert!(before_cid_claim==native::capture(&db).await.unwrap());
    assert_eq!(server.received_requests().await.unwrap().len(),rpc_before);

    // A real replacement preserves residency FKs; the old RID cannot attest it.
    let replacement=support::publish_cid(&db,&config,&identity.key,replacement_cid,false).await;
    assert_ne!(replacement.object_id,identity.object_id);
    assert_ne!(replacement.private_version_id,identity.private_version_id);
    assert_ne!(replacement.cid,identity.cid);
    request.identity=replacement;
    let before_rebind=native::capture(&db).await.unwrap();
    assert!(matches!(evidence::prepare(&db,&config,&auth,&client,&request,CancellationToken::new()).await,Err(ipfs_s3_gateway::store::pinning::management::ManagementError::Changed)));
    assert!(before_rebind==native::capture(&db).await.unwrap());
    assert_eq!(server.received_requests().await.unwrap().len(),rpc_before);
}
