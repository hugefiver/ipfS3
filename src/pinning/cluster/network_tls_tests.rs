use super::{
    ClusterNetworkPolicy, network,
    network_tests::{Answer, Dns},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

struct ServerTask(tokio::task::JoinHandle<()>);
impl Drop for ServerTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn filtered_dns_preserves_tls_sni_hostname_verification_and_auth_origin() {
    tls_origin_contract().await;
}

pub(crate) async fn tls_origin_contract() {
    // Private loopback is explicitly opted in. The owned certificate is trusted
    // only in this test client; certificate/hostname verification is NOT disabled.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let certificate =
        rcgen::generate_simple_self_signed(vec!["bound.cluster.example.com".into()]).unwrap();
    let root = reqwest::Certificate::from_der(certificate.cert.der()).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(certificate.signing_key.serialize_der().into()),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let mut server = ServerTask(tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(stream) = acceptor.accept(socket).await else {
                continue;
            };
            let sni = stream.get_ref().1.server_name().unwrap().to_owned();
            let mut stream = BufReader::new(stream);
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                // A hostname-rejected TLS peer may finish the server handshake
                // but abort before HTTP; it must not appear in captured requests.
                let Ok(Ok(n)) =
                    tokio::time::timeout(Duration::from_secs(1), stream.read_line(&mut line)).await
                else {
                    break;
                };
                if n == 0 {
                    break;
                }
                headers.push_str(&line);
                assert!(headers.len() < 16384);
                if line == "\r\n" {
                    captured
                        .lock()
                        .unwrap()
                        .push((sni, headers.to_ascii_lowercase()));
                    stream
                        .get_mut()
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .await
                        .unwrap();
                    stream.get_mut().flush().await.unwrap();
                    break;
                }
            }
        }
    }));
    let base = reqwest::Url::parse(&format!(
        "https://bound.cluster.example.com:{}",
        address.port()
    ))
    .unwrap();
    let dns = Dns::new(Answer::Addresses(vec![address]));
    let client = network::client_with_test_root(
        &base,
        ClusterNetworkPolicy::AllowPrivate,
        Duration::from_secs(1),
        dns.clone(),
        root,
    )
    .unwrap();
    assert_eq!(
        client
            .get(base.clone())
            .bearer_auth("owned-test-token")
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "ok"
    );
    let mut wrong = base.clone();
    wrong.set_host(Some("wrong.cluster.example.com")).unwrap();
    assert!(
        client
            .get(wrong)
            .bearer_auth("must-not-cross-tls")
            .send()
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), &mut server.0)
        .await
        .unwrap()
        .unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "credentials were sent before TLS hostname verification"
    );
    assert_eq!(requests[0].0, "bound.cluster.example.com");
    assert!(requests[0].1.contains(&format!(
        "host: bound.cluster.example.com:{}\r\n",
        address.port()
    )));
    assert!(
        requests[0]
            .1
            .contains("authorization: bearer owned-test-token\r\n")
    );
    assert!(!requests[0].1.contains("must-not-cross-tls"));
}
