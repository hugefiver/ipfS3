use super::tests::{CID, OTHER, provider, source_file, submit};
use super::{RpcProfile, RpcResourceStatus, RpcStrategy, RpcSubmitEffect};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

/// Protocol/byte-preservation tests, not proof of real Kubo DAG construction.
#[tokio::test]
async fn upload_forwards_zero_small_raw_multiblock_and_sse_stored_bytes_exactly() {
    let mut encrypted = b"stored encrypted header\0".to_vec();
    encrypted.extend(0..=255);
    for (shape, stored) in [
        ("zero", vec![]),
        ("small", b"small bytes".to_vec()),
        ("raw", (0..=255).collect()),
        ("multiblock", vec![0x7b; 1024 * 1024 + 3]),
        ("sse", encrypted),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        source_file(&source, CID, &stored).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!("{{\"Hash\":\"{OTHER}\"}}\n")),
            )
            .expect(1)
            .mount(&target)
            .await;
        let rpc = provider(&target, &source, RpcProfile::Filebase, RpcStrategy::Upload);
        let observation = rpc.submit_observed(submit(CID)).await;
        assert!(
            observation.result.is_err(),
            "mocked different root must not confirm {shape}"
        );
        assert_eq!(observation.effect, RpcSubmitEffect::Observed);
        assert_eq!(observation.resources[0].cid, OTHER);
        assert_eq!(
            observation.resources[0].status,
            RpcResourceStatus::PinAccepted
        );
        let requests = target.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        let content_type = request
            .headers
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        let boundary = content_type
            .strip_prefix("multipart/form-data; boundary=")
            .unwrap();
        let start = request
            .body
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap()
            + 4;
        let part_headers = String::from_utf8_lossy(&request.body[..start]).to_ascii_lowercase();
        assert!(
            part_headers.contains("content-type: application/octet-stream"),
            "{shape}: a streamed upload part without a declared content type is rejected by Filebase with HTTP 500"
        );
        let footer = format!("\r\n--{boundary}--\r\n");
        assert!(request.body.ends_with(footer.as_bytes()));
        assert_eq!(
            &request.body[start..request.body.len() - footer.len()],
            stored,
            "{shape}"
        );
        assert!(
            source
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| !r.headers.contains_key("authorization"))
        );
    }
}
