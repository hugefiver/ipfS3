use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn upload_client(endpoint: String, kubo: String) -> PinataClient {
    let mut client = build_pinata_with_options(
        "pinata".into(),
        super::super::psa::test_token("test-token"),
        Some(format!("{endpoint}/v3")),
        PinataProviderOptions {
            api: PinataApi::V3,
            strategy: PinataStrategy::Upload,
            upload_endpoint: Some(format!("{endpoint}/v3")),
        },
        Some(KuboClient::new(kubo)),
    );
    client.upload_idle = Duration::from_millis(150);
    client
}

fn request() -> SubmitPin {
    SubmitPin {
        cid: "cid".into(),
        name: "name".into(),
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn stage1_review_upload_client_does_not_follow_submit_redirect() {
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data".to_vec()))
        .mount(&source)
        .await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v3/files"))
        .respond_with(
            ResponseTemplate::new(303)
                .insert_header("Location", format!("{}/redirect", target.uri())),
        )
        .mount(&target)
        .await;
    Mock::given(method("GET"))
        .and(path("/redirect"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&target)
        .await;
    let error = upload_client(target.uri(), source.uri())
        .submit(request())
        .await
        .unwrap_err();
    assert!(!error.definitely_not_submitted());
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}

async fn headers(socket: &mut tokio::net::TcpStream) {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(socket.read_u8().await.unwrap());
        assert!(bytes.len() < 16384);
    }
}

#[tokio::test]
async fn stage1_actual_multipart_socket_backpressure_is_idle_bounded() {
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![7; 32 * 1024 * 1024]))
        .mount(&source)
        .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        accepted_tx.send(()).unwrap();
        // Deliberately never read: the transport must stall even though the
        // content source has a full, immediately available large object.
        std::future::pending::<()>().await;
        drop(socket);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        upload_client(endpoint, source.uri()).submit(request()),
    )
    .await;
    server.abort();
    accepted_rx.await.unwrap();
    assert_eq!(
        result
            .expect("socket stall must terminate")
            .unwrap_err()
            .class,
        ProviderErrorClass::Ambiguous
    );
    assert_eq!(source.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn stage1_upload_response_body_stall_is_idle_bounded_until_eof() {
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"data".to_vec()))
        .mount(&source)
        .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        headers(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\nContent-Type: application/json\r\n\r\n{\"data\":").await.unwrap();
        std::future::pending::<()>().await;
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        upload_client(endpoint, source.uri()).submit(request()),
    )
    .await;
    server.abort();
    assert_eq!(
        result
            .expect("response EOF stall must terminate")
            .unwrap_err()
            .class,
        ProviderErrorClass::Ambiguous
    );
}

#[tokio::test]
async fn stage1_slow_active_upload_outlives_idle_and_succeeds() {
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v3/files"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"data":{"id":"file","cid":"cid"}})),
        )
        .mount(&target)
        .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_millis(30)).await;
            socket.write_all(b"4\r\ndata\r\n").await.unwrap();
        }
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    });
    let started = tokio::time::Instant::now();
    let remote = upload_client(target.uri(), source)
        .submit(request())
        .await
        .unwrap();
    server.await.unwrap();
    assert!(started.elapsed() > Duration::from_millis(300));
    assert_eq!(remote.cid, "cid");
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn stage1_malformed_conflicting_and_incomplete_pages_never_prove_absence() {
    for data in [
        json!({"files":{}}),
        json!({"files":[],"next_page_token":"a","nextPageToken":"b"}),
        json!({"files":[],"nextPageToken":12}),
        json!({"files":[],"count":2}),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data":data})))
            .mount(&server)
            .await;
        let mut client = build_pinata(
            "pinata".into(),
            super::super::psa::test_token("test-token"),
            Some(format!("{}/v3", server.uri())),
        );
        client.strategy = PinataStrategy::Upload;
        assert!(
            client.find(FindPin::for_job("cid", "job")).await.is_err(),
            "malformed/incomplete page must not be absent"
        );
    }
}
