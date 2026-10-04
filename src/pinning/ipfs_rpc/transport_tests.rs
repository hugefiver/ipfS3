use super::tests::{CID, submit};
use super::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn read_headers(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        socket.read_exact(&mut byte).await.unwrap();
        bytes.push(byte[0]);
        assert!(bytes.len() < 16 * 1024);
    }
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn self_signed_target_is_not_accepted_and_receives_no_authenticated_http() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(signing_key.serialize_der().into()),
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
        assert!(
            acceptor.accept(socket).await.is_err(),
            "an untrusted certificate must fail before HTTP credentials are sent"
        );
    });
    let provider = IpfsRpcProvider::new(
        "remote".into(),
        endpoint,
        KuboClient::new("http://127.0.0.1:1".into()),
        RpcProfile::Kubo,
        RpcStrategy::Cid,
        Some(RpcAuth::Bearer("tls-secret-sentinel".into())),
    )
    .unwrap();
    let observation = tokio::time::timeout(
        Duration::from_secs(5),
        provider.submit_observed(submit(CID)),
    )
    .await
    .unwrap();
    let error = observation.result.unwrap_err();
    assert!(!format!("{error:?}").contains("tls-secret-sentinel"));
    assert_eq!(observation.effect, RpcSubmitEffect::NotSubmitted);
    assert!(observation.resources.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn target_backpressure_stops_source_polling_and_times_out_as_unknown() {
    let source_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source_endpoint = format!("http://{}", source_listener.local_addr().unwrap());
    let chunks = Arc::new(AtomicUsize::new(0));
    let produced = chunks.clone();
    let source = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = source_listener.accept().await.unwrap();
            let request = read_headers(&mut socket).await;
            if request.starts_with("POST /api/v0/files/stat") {
                let body = format!("{{\"Hash\":\"{CID}\",\"Type\":\"file\"}}");
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            } else {
                assert!(request.starts_with("POST /api/v0/cat"));
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                    .await
                    .unwrap();
                let payload = vec![b'x'; 256 * 1024];
                for _ in 0..1024 {
                    if socket.write_all(b"40000\r\n").await.is_err()
                        || socket.write_all(&payload).await.is_err()
                        || socket.write_all(b"\r\n").await.is_err()
                    {
                        return;
                    }
                    produced.fetch_add(1, Ordering::SeqCst);
                }
                panic!("source was fully drained despite a blocked target");
            }
        }
    });
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", target_listener.local_addr().unwrap());
    let target = tokio::spawn(async move {
        let (mut socket, _) = target_listener.accept().await.unwrap();
        let headers = read_headers(&mut socket).await;
        assert!(headers.starts_with("POST /api/v0/add"));
        std::future::pending::<()>().await;
    });
    let provider = IpfsRpcProvider::new_with_timeouts(
        "remote".into(),
        endpoint,
        KuboClient::new(source_endpoint),
        RpcProfile::Filebase,
        RpcStrategy::Upload,
        Some(RpcAuth::Bearer("mock-token".into())),
        RpcTimeouts {
            connect: Duration::from_secs(1),
            control: Duration::from_secs(1),
            idle: Duration::from_millis(150),
        },
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        provider.submit_observed(submit(CID)),
    )
    .await;
    source.abort();
    target.abort();
    let observation = result.unwrap();
    assert!(observation.result.is_err());
    assert_eq!(observation.effect, RpcSubmitEffect::Unknown);
    assert!(observation.resources.is_empty());
    assert!(
        chunks.load(Ordering::SeqCst) < 1024,
        "backpressure must bound source reads"
    );
}

#[test]
fn global_address_policy_rejects_private_and_transition_networks() {
    for address in [
        "127.0.0.1",
        "10.0.0.1",
        "100.64.0.1",
        "169.254.1.1",
        "192.0.0.1",
        "198.18.1.1",
        "0.0.0.0",
        "240.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
        "fc00::1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "64:ff9b::7f00:1",
    ] {
        assert!(!config::public_ip(address.parse().unwrap()), "{address}");
    }
    for address in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
        assert!(config::public_ip(address.parse().unwrap()), "{address}");
    }
    let header = RpcAuth::Bearer("debug-secret".into()).header().unwrap();
    assert!(header.is_sensitive());
    assert!(!format!("{header:?}").contains("debug-secret"));
}
