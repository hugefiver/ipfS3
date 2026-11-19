use super::tests::{CID, OTHER, provider, source_file, submit};
use super::*;
use crate::pinning::provider::{PinningProvider, ProviderErrorClass};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn consume_request(socket: TcpStream) -> TcpStream {
    let mut socket = BufReader::new(socket);
    let mut headers = String::new();
    loop {
        let mut line = String::new();
        socket.read_line(&mut line).await.unwrap();
        headers.push_str(&line);
        if line == "\r\n" {
            break;
        }
        assert!(headers.len() < 16 * 1024);
    }
    let headers = headers.to_ascii_lowercase();
    if headers.contains("transfer-encoding: chunked") {
        loop {
            let mut line = String::new();
            socket.read_line(&mut line).await.unwrap();
            let length = usize::from_str_radix(line.trim().split(';').next().unwrap(), 16).unwrap();
            if length == 0 {
                let mut trailer = String::new();
                socket.read_line(&mut trailer).await.unwrap();
                assert_eq!(trailer, "\r\n");
                break;
            }
            assert!(length < 1024 * 1024);
            let mut chunk = vec![0; length + 2];
            socket.read_exact(&mut chunk).await.unwrap();
            assert!(chunk.ends_with(b"\r\n"));
        }
    } else if let Some(length) = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
    {
        let length = length.trim().parse::<usize>().unwrap();
        assert!(length < 1024 * 1024);
        socket.read_exact(&mut vec![0; length]).await.unwrap();
    }
    socket.into_inner()
}

pub(super) async fn raw_response(
    bytes: Vec<u8>,
    hold_open: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = consume_request(socket).await;
        socket.write_all(&bytes).await.unwrap();
        socket.flush().await.unwrap();
        if hold_open {
            std::future::pending::<()>().await;
        }
    });
    (endpoint, handle)
}

fn trailer_response(record: &str, trailer: &str) -> Vec<u8> {
    format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n{:X}\r\n{record}\r\n0\r\nX-Stream-Error: {trailer}\r\n\r\n", record.len()).into_bytes()
}

fn short_timeouts() -> RpcTimeouts {
    RpcTimeouts {
        connect: Duration::from_secs(1),
        control: Duration::from_secs(1),
        idle: Duration::from_millis(60),
    }
}

#[tokio::test]
async fn hash_then_error_trailer_is_not_success_or_not_created() {
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    let record = format!("{{\"Hash\":\"{CID}\"}}\n");
    let (endpoint, server) =
        raw_response(trailer_response(&record, "secret-backend-marker"), false).await;
    let p = IpfsRpcProvider::new(
        "remote".into(),
        endpoint,
        KuboClient::new(source.uri()),
        RpcProfile::Filebase,
        RpcStrategy::Upload,
        Some(RpcAuth::Bearer("secret-token".into())),
    )
    .unwrap();
    let error = p.submit(submit(CID)).await.unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Protocol);
    assert!(!error.definitely_not_submitted());
    assert!(!format!("{error:?}").contains("secret-backend-marker"));
    assert!(!format!("{error:?}").contains("secret-token"));
    server.await.unwrap();
}

#[tokio::test]
async fn pin_rm_requires_a_complete_ack_without_stream_errors() {
    let record = format!("{{\"Pins\":[\"{CID}\"]}}");
    for bytes in [
        trailer_response(&record, "failure"),
        format!("HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{record}")
            .into_bytes(),
    ] {
        let source = MockServer::start().await;
        let (endpoint, server) = raw_response(bytes, false).await;
        let p = IpfsRpcProvider::new(
            "remote".into(),
            endpoint,
            KuboClient::new(source.uri()),
            RpcProfile::Kubo,
            RpcStrategy::Cid,
            None,
        )
        .unwrap();
        let error = p.unpin(&format!("rpc-pin:kubo:{CID}")).await.unwrap_err();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn name_only_progress_does_not_mask_root_error_trailer_or_truncated_eof() {
    let records = format!(
        "{{\"Name\":\"object\"}}\n{{\"Name\":\"object\",\"Hash\":\"{CID}\",\"Size\":\"0\"}}\n"
    );
    for bytes in [
        trailer_response(&records, "private-backend-failure"),
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{records}",
            records.len() + 10
        )
        .into_bytes(),
    ] {
        let source = MockServer::start().await;
        source_file(&source, CID, b"").await;
        let (endpoint, server) = raw_response(bytes, false).await;
        let p = IpfsRpcProvider::new(
            "remote".into(),
            endpoint,
            KuboClient::new(source.uri()),
            RpcProfile::Kubo,
            RpcStrategy::Upload,
            None,
        )
        .unwrap();
        let error = p.submit(submit(CID)).await.unwrap_err();
        server.await.unwrap();
        assert_eq!(error.class, ProviderErrorClass::Protocol);
        assert!(!error.definitely_not_submitted());
        assert!(!format!("{error:?}").contains("private-backend-failure"));
    }
}

#[tokio::test]
async fn upload_stall_after_hash_does_not_accept_hash_before_eof() {
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    let record = format!("{{\"Hash\":\"{CID}\"}}\n");
    let bytes = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:X}\r\n{record}\r\n",
        record.len()
    )
    .into_bytes();
    let (endpoint, server) = raw_response(bytes, true).await;
    let p = IpfsRpcProvider::new_with_timeouts(
        "remote".into(),
        endpoint,
        KuboClient::new(source.uri()),
        RpcProfile::Filebase,
        RpcStrategy::Upload,
        Some(RpcAuth::Bearer("secret-token".into())),
        short_timeouts(),
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), p.submit(submit(CID)))
        .await
        .unwrap();
    let error = result.unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Transient);
    assert!(!error.definitely_not_submitted());
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn progress_watchdog_resets_on_real_progress_without_total_deadline() {
    let progress = streaming::Progress::new();
    let advancing = progress.clone();
    let elapsed = tokio::time::Instant::now();
    progress
        .run(Duration::from_millis(100), async move {
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                advancing.advance();
            }
            Ok(())
        })
        .await
        .unwrap();
    assert!(elapsed.elapsed() > Duration::from_millis(100));
    assert!(
        progress
            .run(
                Duration::from_millis(30),
                std::future::pending::<Result<(), _>>()
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn repeated_zero_byte_progress_records_do_not_reset_upload_idle() {
    assert_zero_progress_idle("{\"Bytes\":0}\n", RpcProfile::Filebase).await;
}

#[tokio::test]
async fn repeated_name_only_zero_progress_does_not_reset_kubo_upload_idle() {
    assert_zero_progress_idle("{\"Name\":\"object\"}\n", RpcProfile::Kubo).await;
}

async fn assert_zero_progress_idle(record: &str, profile: RpcProfile) {
    let source = MockServer::start().await;
    source_file(&source, CID, b"stored").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let chunk = format!("{:X}\r\n{record}\r\n", record.len()).into_bytes();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = consume_request(socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .await
            .unwrap();
        for _ in 0..150 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if socket.write_all(&chunk).await.is_err() {
                return;
            }
        }
        std::future::pending::<()>().await;
    });
    let p = IpfsRpcProvider::new_with_timeouts(
        "remote".into(),
        endpoint,
        KuboClient::new(source.uri()),
        profile,
        RpcStrategy::Upload,
        Some(RpcAuth::Bearer("secret-token".into())),
        short_timeouts(),
    )
    .unwrap();
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(3), p.submit(submit(CID))).await;
    server.abort();
    let error = result.unwrap().unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Transient);
    assert!(!error.definitely_not_submitted());
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "non-progress bytes must not keep an upload alive"
    );
}

#[tokio::test]
async fn malformed_multi_root_or_failed_car_imports_never_unpin() {
    for records in [
        format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"private failure\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":3}}}}\n"
        ),
        format!("{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n"),
        format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{OTHER}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":3}}}}\n"
        ),
        format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Root\":{{\"Cid\":{{\"/\":\"{OTHER}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":2,\"BlockBytesCount\":3}}}}\n"
        ),
        format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{}}}}\n"
        ),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"mock CAR"))
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .respond_with(ResponseTemplate::new(200).set_body_string(records))
            .mount(&target)
            .await;
        let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Car);
        let error = p.submit(submit(CID)).await.unwrap_err();
        assert!(!error.definitely_not_submitted());
        assert!(!format!("{error:?}").contains("private failure"));
        let requests = target.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/api/v0/dag/import");
    }
}

#[tokio::test]
async fn exact_unpin_sends_only_the_handle_cid_with_profile_parameters() {
    for (profile, strategy, id, expected_query) in [
        (
            RpcProfile::Kubo,
            RpcStrategy::Cid,
            "kubo",
            vec![("arg", CID), ("recursive", "true")],
        ),
        (
            RpcProfile::Filebase,
            RpcStrategy::Upload,
            "filebase",
            vec![("arg", CID)],
        ),
    ] {
        let source = MockServer::start().await;
        let target = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [CID]})),
            )
            .expect(1)
            .mount(&target)
            .await;
        let p = provider(&target, &source, profile, strategy);
        p.unpin(&format!("rpc-pin:{id}:{CID}")).await.unwrap();
        let requests = target.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let query: Vec<_> = requests[0]
            .url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            query,
            expected_query
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect::<Vec<_>>()
        );
        assert!(source.received_requests().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn unpin_rejects_a_different_cid_without_retrying() {
    let source = MockServer::start().await;
    let target = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/rm"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins": [OTHER]})),
        )
        .expect(1)
        .mount(&target)
        .await;
    let p = provider(&target, &source, RpcProfile::Kubo, RpcStrategy::Cid);
    let error = p.unpin(&format!("rpc-pin:kubo:{CID}")).await.unwrap_err();
    assert_eq!(error.class, ProviderErrorClass::Protocol);
    assert_eq!(target.received_requests().await.unwrap().len(), 1);
}

#[test]
fn endpoint_credentials_and_non_bucket_filebase_auth_are_rejected() {
    let source = KuboClient::new("http://127.0.0.1:1".into());
    for endpoint in [
        "http://user:secret@example.test",
        "http://example.test?token=secret",
        "http://example.test#secret",
        "ftp://example.test",
    ] {
        let error = match IpfsRpcProvider::new(
            "remote".into(),
            endpoint.into(),
            source.clone(),
            RpcProfile::Kubo,
            RpcStrategy::Cid,
            None,
        ) {
            Ok(_) => panic!("unsafe endpoint accepted"),
            Err(error) => error,
        };
        assert!(!format!("{error:?}").contains("secret"));
    }
    assert!(
        IpfsRpcProvider::new(
            "remote".into(),
            "http://example.test".into(),
            source,
            RpcProfile::Filebase,
            RpcStrategy::Upload,
            Some(RpcAuth::Basic {
                username: "user".into(),
                password: "secret".into()
            })
        )
        .is_err()
    );
}
