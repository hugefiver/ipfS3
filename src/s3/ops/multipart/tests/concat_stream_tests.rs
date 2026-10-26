use super::*;
use futures_util::Stream;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use wiremock::matchers::query_param;

const LIVENESS_LIMIT: Duration = Duration::from_secs(5);
const FIRST_CHUNK: &[u8] = b"HTTP/1.1 200 OK\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\n\r\n1\r\nA\r\n";

struct CatPeer {
    endpoint: String,
    requests: mpsc::UnboundedReceiver<(String, TcpStream)>,
    task: tokio::task::JoinHandle<()>,
}

impl CatPeer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                }
                let request = String::from_utf8(headers).unwrap();
                let target = request.split_whitespace().nth(1).unwrap().to_owned();
                if sender.send((target, socket)).is_err() {
                    return;
                }
            }
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    async fn request_during_read<S>(&mut self, stream: &mut Pin<Box<S>>, cid: &str) -> TcpStream
    where
        S: Stream<Item = Result<Bytes, std::io::Error>>,
    {
        let (target, socket) = tokio::time::timeout(LIVENESS_LIMIT, async {
            tokio::select! {
                item = stream.next() => panic!("CAT response has not been served: {item:?}"),
                request = self.requests.recv() => request.unwrap(),
            }
        })
        .await
        .expect("CAT request must arrive before its response can be served");
        assert_eq!(target, format!("/api/v0/cat?arg={cid}"));
        socket
    }
}

impl Drop for CatPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn concat_consumer_pause_preserves_ordered_bytes_and_zip_audit() {
    use base64::Engine as _;

    let mut peer = CatPeer::start().await;
    let idle = Duration::from_millis(100);
    let mut second_body = b"BBBPK\x05\x06".to_vec();
    second_body.resize(25, 0);
    let parts = [
        ("first", b"AAA".as_slice()),
        ("second", second_body.as_slice()),
    ]
    .into_iter()
    .map(|(cid, bytes)| {
        (
            cid.to_owned(),
            i64::try_from(bytes.len()).unwrap(),
            Some(base64::engine::general_purpose::STANDARD.encode(Sha256::digest(bytes))),
        )
    })
    .collect();
    let audit = Arc::new(Mutex::new(ZipV2StreamAudit::new()));
    let mut stream = Box::pin(plain_parts_concat_stream(
        crate::kubo::KuboClient::new_with_timeouts(
            peer.endpoint.clone(),
            Duration::from_secs(300),
            idle,
        ),
        parts,
        Some(audit.clone()),
    ));
    let mut first = peer.request_during_read(&mut stream, "first").await;
    first.write_all(FIRST_CHUNK).await.unwrap();
    let mut actual = stream.next().await.unwrap().unwrap().to_vec();
    assert_eq!(actual, b"A");
    // The producer finishes its response while the downstream consumer is paused.
    // This exercises idle-timeout semantics, not a performance threshold.
    first.write_all(b"2\r\nAA\r\n0\r\n\r\n").await.unwrap();
    tokio::time::sleep(idle * 3).await;
    tokio::time::timeout(LIVENESS_LIMIT, async {
        while actual.len() < 3 {
            actual.extend_from_slice(&stream.next().await.unwrap().unwrap());
        }
    })
    .await
    .unwrap();
    let mut second = peer.request_during_read(&mut stream, "second").await;
    second
        .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 25\r\n\r\n")
        .await
        .unwrap();
    second.write_all(&second_body).await.unwrap();
    tokio::time::timeout(LIVENESS_LIMIT, async {
        while let Some(chunk) = stream.next().await {
            actual.extend_from_slice(&chunk.unwrap());
        }
    })
    .await
    .unwrap();
    let mut expected = b"AAA".to_vec();
    expected.extend_from_slice(&second_body);
    assert_eq!(actual, expected);
    assert_eq!(
        audit.lock().unwrap().finish(28).unwrap(),
        hex::encode(Sha256::digest(&expected))
    );
}

#[tokio::test]
async fn concat_header_failure_preserves_prior_bytes_and_fails_audit() {
    let mut peer = CatPeer::start().await;
    let parts = ["first", "second"]
        .map(|cid| (cid.to_owned(), 1, None))
        .to_vec();
    let audit = Arc::new(Mutex::new(ZipV2StreamAudit::new()));
    let mut stream = Box::pin(plain_parts_concat_stream(
        crate::kubo::KuboClient::new(peer.endpoint.clone()),
        parts,
        Some(audit.clone()),
    ));
    let mut first = peer.request_during_read(&mut stream, "first").await;
    first.write_all(FIRST_CHUNK).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"A");
    first.write_all(b"0\r\n\r\n").await.unwrap();
    let mut second = peer.request_during_read(&mut stream, "second").await;
    second
        .write_all(b"HTTP/1.1 503 Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(LIVENESS_LIMIT, stream.next())
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(stream.next().await.is_none());
    assert!(matches!(
        audit.lock().unwrap().failure,
        Some(ZipAssemblyFailure::IncompleteBody)
    ));
}

#[tokio::test]
async fn concat_late_trailer_error_preserves_provenance_and_fails_audit() {
    let mut peer = CatPeer::start().await;
    let audit = Arc::new(Mutex::new(ZipV2StreamAudit::new()));
    let mut stream = Box::pin(plain_parts_concat_stream(
        crate::kubo::KuboClient::new(peer.endpoint.clone()),
        vec![("first".to_owned(), 1, None)],
        Some(audit.clone()),
    ));
    let mut first = peer.request_during_read(&mut stream, "first").await;
    first.write_all(FIRST_CHUNK).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().as_ref(), b"A");
    first
        .write_all(b"0\r\nX-Stream-Error: late failure\r\n\r\n")
        .await
        .unwrap();
    let error = tokio::time::timeout(LIVENESS_LIMIT, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(crate::error::has_kubo_stream_provenance(&error));
    assert!(stream.next().await.is_none());
    assert!(matches!(
        audit.lock().unwrap().failure,
        Some(ZipAssemblyFailure::IncompleteBody)
    ));
}

#[tokio::test]
async fn concat_zip_audit_rejects_short_long_and_wrong_checksum_parts() {
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("arg", "first"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"AAA"))
        .mount(&kubo)
        .await;
    for (size, checksum) in [(2, None), (4, None), (3, Some("wrong".to_owned()))] {
        let audit = Arc::new(Mutex::new(ZipV2StreamAudit::new()));
        let stream = plain_parts_concat_stream(
            crate::kubo::KuboClient::new(kubo.uri()),
            vec![("first".to_owned(), size, checksum)],
            Some(audit.clone()),
        );
        tokio::pin!(stream);
        let mut failed = false;
        while let Some(chunk) = stream.next().await {
            if chunk.is_err() {
                failed = true;
            }
        }
        assert!(failed);
        let audit = audit.lock().unwrap();
        assert!(!audit.eof);
        assert!(matches!(
            audit.failure,
            Some(ZipAssemblyFailure::InvalidPart)
        ));
    }
}
