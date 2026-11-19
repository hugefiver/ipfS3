use super::{
    ClusterAuth,
    protocol_tests::{RAW_EMPTY, options, request},
    provider::{
        ClusterFailureCode, ClusterProtocol, ClusterProvider, ClusterSource, ClusterStrategy,
    },
};
use crate::crypto::{ObjectKey, aes_gcm::encrypt_chunk};
use cid::Cid;
use serde_json::json;
use sha2::{Digest, Sha256};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

#[tokio::test]
async fn stored_bytes_and_separate_credentials_reach_only_the_selected_upload_endpoints() {
    let plaintext = b"plaintext never sent to Cluster";
    let ciphertext =
        encrypt_chunk(&ObjectKey { bytes: [0x37; 32] }, &[0x83; 12], plaintext).unwrap();
    let cid = Cid::new_v1(
        0x55,
        cid::multihash::Multihash::wrap(0x12, &Sha256::digest(&ciphertext)).unwrap(),
    )
    .to_string();
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(header("authorization", "Bearer source-only"))
        .and(query_param("arg", format!("/ipfs/{cid}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"Hash": cid, "Type": "file"})),
        )
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(header("authorization", "Bearer source-only"))
        .and(query_param("arg", &cid))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ciphertext.to_vec()))
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/proxy/api/v0/add"))
        .and(header("authorization", "Bearer proxy-only"))
        .and(query_param("format", "unixfs"))
        .and(query_param("stream-channels", "true"))
        .and(query_param("no-pin", "false"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("X-Chunked-Output", "1")
                .set_body_raw(
                    format!(
                        "{}\n{}\n",
                        json!({"Name":"content","Bytes":1}),
                        json!({"Name":"content","Hash":cid,"Size":"32"})
                    ),
                    "application/json",
                ),
        )
        .expect(1)
        .mount(&target)
        .await;
    let mut opts = options(
        &target.uri(),
        ClusterProtocol::Proxy,
        ClusterStrategy::Upload,
    );
    opts.source = Some(ClusterSource {
        endpoint: source.uri(),
        auth: Some(ClusterAuth::Bearer("source-only".into())),
    });
    let provider = ClusterProvider::new(opts).unwrap();
    assert_eq!(
        provider.submit(request(&cid)).await.unwrap().request_id,
        cid
    );
    assert_eq!(source.received_requests().await.unwrap().len(), 2);
    let requests = target.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let body = &requests[0].body;
    assert!(
        body.windows(ciphertext.len())
            .any(|window| window == ciphertext.as_ref())
    );
    assert!(
        !body
            .windows(plaintext.len())
            .any(|window| window == plaintext)
    );
    let body = String::from_utf8_lossy(body);
    assert!(!body.contains("object-name-sentinel") && !body.contains("metadata-sentinel"));
}

fn empty_raw_car() -> Vec<u8> {
    let cid = RAW_EMPTY.parse::<Cid>().unwrap().to_bytes();
    let mut header = b"\xa2\x65roots\x81\xd8\x2a\x58".to_vec();
    header.push((cid.len() + 1) as u8);
    header.push(0);
    header.extend(&cid);
    header.extend(b"\x67version\x01");
    let mut car = vec![header.len() as u8];
    car.extend(header);
    car.push(cid.len() as u8);
    car.extend(cid);
    car
}

#[tokio::test]
async fn car_ack_requires_full_transfer_and_the_native_add_receipt_shape() {
    for (truncate_car, ack, expected) in [
        (
            false,
            json!([{"name":"content","cid":null},{"name":"content","cid":RAW_EMPTY}]),
            None,
        ),
        (
            false,
            json!([{"Name":"content","Hash":RAW_EMPTY}]),
            Some(ClusterFailureCode::Protocol),
        ),
        (
            false,
            json!([{"name":"content","cid":RAW_EMPTY},{"name":"other","cid":RAW_EMPTY}]),
            Some(ClusterFailureCode::Protocol),
        ),
        (
            true,
            json!([{"name":"content","cid":RAW_EMPTY}]),
            Some(ClusterFailureCode::InvalidCar),
        ),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        let mut car = empty_raw_car();
        if truncate_car {
            car.pop();
        }
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .and(query_param("arg", RAW_EMPTY))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(car))
            .expect(1)
            .mount(&source)
            .await;
        let add = Mock::given(method("POST"))
            .and(path("/rest/add"))
            .and(header("authorization", "Bearer rest-only"))
            .and(query_param("format", "car"))
            .and(query_param("stream-channels", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ack));
        if !truncate_car {
            add.expect(1).mount(&target).await;
        } else {
            add.mount(&target).await;
        }
        let mut opts = options(&target.uri(), ClusterProtocol::Rest, ClusterStrategy::Car);
        opts.source = Some(ClusterSource {
            endpoint: source.uri(),
            auth: None,
        });
        opts.stream_channels = false;
        let result = ClusterProvider::new(opts)
            .unwrap()
            .submit(request(RAW_EMPTY))
            .await;
        match expected {
            Some(code) => assert_eq!(result.unwrap_err().code, code),
            None => assert_eq!(result.unwrap().request_id, RAW_EMPTY),
        }
        assert_eq!(source.received_requests().await.unwrap().len(), 1);
        let requests = target.received_requests().await.unwrap();
        assert!(requests.len() <= 1);
        assert!(requests.iter().all(|req| req.url.path() == "/rest/add"));
    }
}
