// Use the same fixture encoders as the directory owner. The CLI/production
// proof validator, not the fixture's independent blocks() verifier, attests.
#[path = "cluster_provider_real_car.rs"]
mod car_fixture;

use super::support;
use ipfs_s3_gateway::{
    kubo::{KuboClient, LocalDirectoryRootKind},
    pinning::management::evidence::{self, ReadProof},
    store::pinning::management::{native, records, schema},
};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use wiremock::{Mock, ResponseTemplate, matchers::{method, path, query_param}};

pub struct ExportFixture {
    pub name: &'static str,
    pub cid: String,
    pub payload_bytes: u64,
    car: Option<Vec<u8>>,
}

fn directory_blocks() -> (String, Vec<(String, Vec<u8>)>) {
    let child = car_fixture::raw_cid(b"hello", 0x55);
    let (root, bytes) = car_fixture::directory(&child);
    (root.clone(), vec![(root, bytes), (child, b"hello".to_vec())])
}

pub fn rejected_exports() -> Vec<ExportFixture> {
    let (cid, blocks) = directory_blocks();
    let payload_bytes = blocks.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let mut corrupt = car_fixture::encode(std::slice::from_ref(&cid), &blocks);
    *corrupt.last_mut().unwrap() ^= 1;
    let file = car_fixture::raw_cid(b"hello", 0x55);
    let file_car = car_fixture::encode(std::slice::from_ref(&file), &[(file.clone(), b"hello".to_vec())]);
    vec![
        ExportFixture { name: "missing-export", cid: cid.clone(), payload_bytes, car: None },
        ExportFixture { name: "missing-linked-block", cid: cid.clone(), payload_bytes,
            car: Some(car_fixture::encode(std::slice::from_ref(&cid), &blocks[..1])) },
        ExportFixture { name: "corrupt-block-hash", cid, payload_bytes, car: Some(corrupt) },
        ExportFixture { name: "file-root-despite-directory-stat", cid: file, payload_bytes: 5, car: Some(file_car) },
    ]
}

pub async fn mount_source(fixture: &support::Fixture, export: &ExportFixture) {
    Mock::given(method("POST")).and(path("/api/v0/pin/ls"))
        .and(query_param("arg", &export.cid)).and(query_param("type", "recursive")).and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Keys": {(export.cid.clone()): {"Type": "recursive"}}})))
        .mount(&fixture.server).await;
    Mock::given(method("POST")).and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{}", export.cid))).and(query_param("with-local", "true")).and(query_param("offline", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "Hash": export.cid, "WithLocality": true, "Local": true, "Type": "directory", "SizeLocal": "999999",
        }))).mount(&fixture.server).await;
    let response = match &export.car {
        Some(bytes) => ResponseTemplate::new(200).set_body_bytes(bytes.clone()),
        None => ResponseTemplate::new(404).set_body_string("DIRECTORY_EXPORT_UNAVAILABLE_DO_NOT_PRINT"),
    };
    Mock::given(method("POST")).and(path("/api/v0/dag/export"))
        .and(query_param("arg", &export.cid)).and(query_param("offline", "true"))
        .respond_with(response).expect(1).mount(&fixture.server).await;
}

pub fn prepare_file(fixture: &support::Fixture, payload_bytes: u64) -> std::path::PathBuf {
    let path = fixture.prepare_file(true);
    let mut request: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    request["max_stored_bytes"] = json!(payload_bytes);
    support::write_json(&path, &request);
    path
}

pub async fn assert_readonly_proof_requests(fixture: &support::Fixture, identity_reads: usize, pin_reads: usize) {
    let requests = fixture.server.received_requests().await.unwrap();
    for request in &requests {
        assert_eq!(request.method.as_str(), "POST");
        assert!(matches!(request.url.path(), "/api/v0/id" | "/api/v0/pin/ls" | "/api/v0/files/stat" | "/api/v0/dag/export"));
        assert!(request.url.query_pairs().any(|(name, value)| name == "offline" && value == "true"));
    }
    assert_eq!(requests.iter().filter(|request| request.url.path() == "/api/v0/id").count(), identity_reads);
    assert_eq!(requests.iter().filter(|request| request.url.path() == "/api/v0/pin/ls").count(), pin_reads);
    assert_eq!(requests.iter().filter(|request| request.url.path() == "/api/v0/files/stat").count(), 1);
    assert_eq!(requests.iter().filter(|request| request.url.path() == "/api/v0/dag/export").count(), 1);
}

#[tokio::test]
async fn fresh_complete_directory_and_hamt_cars_attest_block_payload_without_business_or_file_upload_grants() {
    let (directory, blocks) = directory_blocks();
    let payload_bytes = blocks.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let directory_car = car_fixture::encode(std::slice::from_ref(&directory), &blocks);
    // Same minimal valid HAMT data used by the directory owner's proof regression.
    let hamt_bytes = b"\x0a\x07\x08\x05\x28\x22\x30\x80\x02".to_vec();
    let hamt = car_fixture::raw_cid(&hamt_bytes, 0x70);
    let hamt_car = car_fixture::encode(std::slice::from_ref(&hamt), &[(hamt.clone(), hamt_bytes)]);
    for (export, kind, block_count) in [
        (ExportFixture {name: "directory", cid: directory, payload_bytes, car: Some(directory_car)}, LocalDirectoryRootKind::Directory, 2),
        (ExportFixture {name: "hamt", cid: hamt, payload_bytes: 9, car: Some(hamt_car)}, LocalDirectoryRootKind::HamtShard, 1),
    ] {
        let mut fixture = support::Fixture::new(false).await;
        fixture.publish_directory(&export.cid).await;
        mount_source(&fixture, &export).await;
        let before = fixture.frame().await;
        let prepare = prepare_file(&fixture, export.payload_bytes);
        let result = support::output(&mut fixture.command(&["--pinning-manage", "--prepare", prepare.to_str().unwrap()])).await;
        let proof = support::report(&result);
        assert_eq!(proof["mode"], "prepare");
        assert_eq!(proof["proof"], "complete_local_directory_dag_blocks_v1");
        assert_eq!(proof["stored_bytes"], export.payload_bytes);
        assert_eq!(proof["upload_size_proven"], false);
        assert_ne!(proof["stored_bytes"], export.car.as_ref().unwrap().len());
        assert_ne!(proof["stored_bytes"], 10); // actual publication's logical metadata
        assert_eq!(proof["source_rpc"], "explicit_offline_local_proof");
        assert_eq!(proof["provider_rpc"], "not_performed");
        fixture.assert_redacted(&result);
        let after = fixture.frame().await;
        // Outside the trusted preparation record, leases/jobs/quota and every
        // other business/native row are unchanged. No plan/confirm/apply/run.
        assert!(after.evidence_inventory() == before.evidence_inventory());
        let proofs = after.table(schema::RECORDS).unwrap().rows.iter()
            .filter(|row| native::value(row, "kind") == Some("evidence_preparation")).collect::<Vec<_>>();
        assert_eq!(proofs.len(), 1);
        assert_eq!(native::value(proofs[0], "state"), Some("prepared"));
        assert_eq!(native::value(proofs[0], "intent_ids"), Some("[]"));
        assert_eq!(native::value(proofs[0], "job_ids"), Some("[]"));
        assert!(!after.table(schema::RECORDS).unwrap().rows.iter().any(|row| native::value(row, "kind") == Some("action")));
        // Retrieve the actual persisted attestation through the owner's public
        // same-RID replay API, without decoding private record framing ourselves.
        let raw: ipfs_s3_gateway::config::Config = toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
        let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
        let attestation = evidence::prepare(&fixture.db, &fixture.config, &auth, &KuboClient::new(fixture.server.uri()),
            &evidence::PrepareRequest {record_id: "explicit-cli-proof".into(), identity: fixture.identity.clone(),
                expected_node: support::NODE.into(), max_stored_bytes: export.payload_bytes, ttl: chrono::Duration::hours(1), directory: true},
            CancellationToken::new()).await.unwrap();
        assert!(fixture.frame().await == after);
        assert_eq!(json!(attestation.observed_at), proof["observed_at"]);
        assert_eq!(json!(attestation.expires_at), proof["expires_at"]);
        assert_eq!(attestation.identity.cid, export.cid);
        assert_eq!(attestation.proof, ReadProof::CompleteLocalDirectoryDagBlocksV1);
        assert!(attestation.proof.is_directory()); // never reinterpreted as FileCleanEof/file-only eligibility
        assert_eq!(attestation.directory_root_kind(), Some(kind));
        let receipt = attestation.directory_receipt.as_ref().unwrap();
        assert_eq!(receipt.stored_block_bytes(), export.payload_bytes);
        assert_eq!(receipt.serialized_car_bytes(), export.car.as_ref().unwrap().len() as u64);
        assert_eq!(receipt.block_count(), block_count);
        assert_readonly_proof_requests(&fixture, 3, 2).await;
    }
}
