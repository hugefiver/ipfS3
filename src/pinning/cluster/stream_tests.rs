use super::{tests::*, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

async fn raw_response(bytes: Vec<u8>, hold: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        let mut size = 0;
        loop {
            let mut line = String::new();
            assert!(socket.read_line(&mut line).await.unwrap() > 0);
            size += line.len();
            assert!(size < 16384);
            if line == "\r\n" {
                break;
            }
        }
        socket.get_mut().write_all(&bytes).await.unwrap();
        socket.get_mut().flush().await.unwrap();
        if hold {
            std::future::pending::<()>().await;
        }
    });
    (endpoint, task)
}

fn short_profile(endpoint: &str) -> ClusterRestProfile {
    ClusterRestProfile::new_with_network_policy(
        ClusterPinset::new("cluster", "pinset").unwrap(),
        Some(endpoint),
        None,
        2,
        RestLimits {
            connect_timeout: Duration::from_millis(150),
            total_timeout: Duration::from_millis(400),
            idle_timeout: Duration::from_millis(150),
            max_body_bytes: 64,
            ..RestLimits::default()
        },
        ClusterNetworkPolicy::AllowPrivate,
    )
    .unwrap()
}

#[tokio::test]
async fn body_stall_or_truncated_json_success_never_passes_before_eof() {
    let body = r#"{"version":"1.1.6"}"#;
    for (reply, hold, code) in [
        (
            format!(
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:X}\r\n{body}\r\n",
                body.len()
            ),
            true,
            VerificationCode::IdleTimeout,
        ),
        (
            format!("HTTP/1.1 200 OK\r\nContent-Length: 40\r\nConnection: close\r\n\r\n{body}"),
            false,
            VerificationCode::Transport,
        ),
        (String::new(), true, VerificationCode::IdleTimeout),
    ] {
        let (endpoint, task) = raw_response(reply.into_bytes(), hold).await;
        let result = ClusterReplicaVerifier::new(short_profile(&endpoint))
            .unwrap()
            .observe(CID)
            .await;
        assert_eq!(result.outcome(), ReplicaOutcome::Unknown);
        assert_eq!(result.failure().unwrap().code, code);
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn actual_chunk_bytes_are_bounded_without_content_length() {
    let body = "x".repeat(65);
    let reply = format!(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:X}\r\n{body}\r\n0\r\n\r\n",
        body.len()
    );
    let (endpoint, task) = raw_response(reply.into_bytes(), false).await;
    let result = ClusterReplicaVerifier::new(short_profile(&endpoint))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(
        result.failure().unwrap().code,
        VerificationCode::ResponseTooLarge
    );
    task.await.unwrap();
}

#[tokio::test]
async fn whole_observation_deadline_is_not_reset_by_each_successful_get() {
    let server = MockServer::start().await;
    for (url, body) in [
        ("/version".to_owned(), json!({"version": "1.1.6"})),
        (format!("/allocations/{CID}"), allocation(&["a", "b"])),
        (
            format!("/pins/{CID}"),
            global(&["a", "b"], &[("a", "pinned"), ("b", "pinned")]),
        ),
    ] {
        Mock::given(method("GET"))
            .and(path(url))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(body)
                    .set_delay(Duration::from_millis(70)),
            )
            .mount(&server)
            .await;
    }
    let mut p = profile(&server.uri(), 2);
    p.limits.total_timeout = Duration::from_millis(240);
    p.limits.connect_timeout = Duration::from_millis(150);
    p.limits.idle_timeout = Duration::from_millis(150);
    let result = ClusterReplicaVerifier::new(p).unwrap().observe(CID).await;
    assert_eq!(
        result.failure().unwrap().code,
        VerificationCode::TotalTimeout
    );
}

#[tokio::test]
async fn unreachable_origin_is_unknown_and_redacted() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let result = ClusterReplicaVerifier::new(short_profile(&endpoint))
        .unwrap()
        .observe(CID)
        .await;
    assert_eq!(result.outcome(), ReplicaOutcome::Unknown);
    assert_eq!(result.failure().unwrap().code, VerificationCode::Transport);
    assert!(!format!("{result:?}").contains(&endpoint));
}

#[tokio::test]
async fn untrusted_tls_certificate_is_rejected_without_sending_rest_credentials() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(certificate.signing_key.serialize_der().into()),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        // A rejected certificate terminates TLS before any HTTP Authorization.
        assert!(acceptor.accept(socket).await.is_err());
    });
    let p = ClusterRestProfile::new_with_network_policy(
        ClusterPinset::new("cluster", "pinset").unwrap(),
        Some(&endpoint),
        Some(ClusterAuth::Bearer("token-sentinel".into())),
        2,
        RestLimits::default(),
        ClusterNetworkPolicy::AllowPrivate,
    )
    .unwrap();
    let result = ClusterReplicaVerifier::new(p).unwrap().observe(CID).await;
    assert_eq!(result.failure().unwrap().code, VerificationCode::Transport);
    assert!(!format!("{result:?}").contains("token-sentinel"));
    server.await.unwrap();
}
