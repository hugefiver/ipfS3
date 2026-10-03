use ipfs_s3_gateway::kubo::KuboClient;
use ipfs_s3_gateway::kubo::directory::{
    DirectoryBuildError, DirectoryFile, MAX_DIRECTORY_BLOCK_BYTES, build_directory,
};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const EMPTY_RAW: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const DIRECTORY_CID: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
const NODE_ID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

fn file(path: &str, cid: &str) -> DirectoryFile {
    DirectoryFile {
        path: path.to_owned(),
        cid: cid.to_owned(),
    }
}

async fn mount_one_file_root(server: &MockServer, pin_status: u16) {
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{EMPTY_RAW}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{EMPTY_RAW}\",\"CumulativeSize\":0}}")),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"ID\":\"{NODE_ID}\"}}")),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .and(query_param("pin", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Cid\":{{\"/\":\"{DIRECTORY_CID}\"}}}}")),
        )
        .expect(1)
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", DIRECTORY_CID))
        .and(query_param("recursive", "true"))
        .respond_with(
            ResponseTemplate::new(pin_status)
                .set_body_string(format!("{{\"Pins\":[\"{DIRECTORY_CID}\"]}}")),
        )
        .expect(1)
        .mount(server)
        .await;
}

async fn mount_identity(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"ID\":\"{NODE_ID}\"}}")),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn confirmed_pin_then_bad_path_retains_known_root_without_receipt() {
    let server = MockServer::start().await;
    mount_one_file_root(&server, 200).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Path\":\"/ipfs/{DIRECTORY_CID}\"}}")),
        )
        .expect(1)
        .mount(&server)
        .await;
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &[file("a", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    let candidate = error
        .candidate()
        .expect("root emitted before failed path validation");
    assert_eq!(candidate.cid, DIRECTORY_CID);
    assert_eq!(candidate.node_identity, NODE_ID);
    assert!(matches!(error.reason(), DirectoryBuildError::Kubo(_)));
}

#[tokio::test]
async fn root_pin_failure_still_retains_candidate_without_receipt() {
    let server = MockServer::start().await;
    mount_one_file_root(&server, 503).await;
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &[file("a", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.candidate().unwrap().cid, DIRECTORY_CID);
    assert_eq!(error.candidate().unwrap().node_identity, NODE_ID);
    assert!(matches!(error.reason(), DirectoryBuildError::Kubo(_)));
}

#[tokio::test]
async fn failed_local_verification_retains_candidate_without_receipt() {
    let server = MockServer::start().await;
    mount_one_file_root(&server, 200).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Path\":\"/ipfs/{EMPTY_RAW}\"}}")),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &[file("a", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.candidate().unwrap().cid, DIRECTORY_CID);
    assert_eq!(error.candidate().unwrap().node_identity, NODE_ID);
    assert!(matches!(error.reason(), DirectoryBuildError::Kubo(_)));
}

#[tokio::test]
async fn child_directory_is_pinned_on_put_before_parent_write() {
    let server = MockServer::start().await;
    mount_identity(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{EMPTY_RAW}\",\"CumulativeSize\":0}}")),
        )
        .mount(&server)
        .await;
    let puts = std::sync::atomic::AtomicUsize::new(0);
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .and(query_param("pin", "true"))
        .respond_with(move |_: &wiremock::Request| {
            if puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Cid\":{{\"/\":\"{DIRECTORY_CID}\"}}}}"))
            } else {
                ResponseTemplate::new(503)
            }
        })
        .expect(2)
        .mount(&server)
        .await;
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &[file("nested/a", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error.candidate().is_none(),
        "failed root write has no candidate"
    );
    let requests = server.received_requests().await.unwrap();
    let puts: Vec<_> = requests
        .iter()
        .filter(|request| request.url.path() == "/api/v0/dag/put")
        .collect();
    assert_eq!(puts.len(), 2);
    assert!(puts.iter().all(|put| {
        put.url
            .query_pairs()
            .any(|(key, value)| key == "pin" && value == "true")
    }));
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test]
async fn cancellation_after_root_emission_retains_candidate_without_success() {
    let server = MockServer::start().await;
    mount_one_file_root(&server, 200).await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let started = std::sync::Mutex::new(Some(started));
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(move |_: &wiremock::Request| {
            if let Some(started) = started.lock().unwrap().take() {
                let _ = started.send(());
            }
            ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(5))
        })
        .mount(&server)
        .await;
    let cancel = CancellationToken::new();
    let child_cancel = cancel.clone();
    let endpoint = server.uri();
    let build = tokio::spawn(async move {
        build_directory(
            &KuboClient::new(endpoint),
            &[file("a", EMPTY_RAW)],
            &child_cancel,
        )
        .await
    });
    ready.await.unwrap();
    cancel.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), build)
        .await
        .expect("cancellation must interrupt path resolution")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error.reason(), DirectoryBuildError::Canceled));
    let candidate = error.candidate().expect("root emitted before cancellation");
    assert_eq!(candidate.cid, DIRECTORY_CID);
    assert_eq!(candidate.node_identity, NODE_ID);
}

#[tokio::test]
async fn empty_manifest_and_conflicting_paths_never_write_a_directory() {
    let server = MockServer::start().await;
    let client = KuboClient::new(server.uri());
    assert!(
        build_directory(&client, &[], &CancellationToken::new())
            .await
            .unwrap()
            .is_none()
    );
    let error = build_directory(
        &client,
        &[file("a", EMPTY_RAW), file("a/b", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, DirectoryBuildError::PathConflict));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn zero_byte_raw_link_uses_block_stat_not_nan_dag_stat() {
    let server = MockServer::start().await;
    mount_identity(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/stat"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/block/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Key\":\"{EMPTY_RAW}\",\"Size\":0}}")),
        )
        .expect(1)
        .mount(&server)
        .await;
    // A failed directory write is enough to inspect metadata calls and outgoing links.
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    build_directory(
        &KuboClient::new(server.uri()),
        &[file("zero", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/dag/put")
        .unwrap();
    let body = String::from_utf8_lossy(&put.body);
    assert!(body.contains("\"Tsize\":0"), "{body}");
    assert!(body.contains(EMPTY_RAW));
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path() == "/api/v0/cat"
                || request.url.path() == "/api/v0/add")
    );
}

#[tokio::test]
async fn file_stat_cumulative_size_is_authoritative_not_logical_file_length() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{EMPTY_RAW}\",\"CumulativeSize\":8192}}"
        )))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    build_directory(
        &KuboClient::new(server.uri()),
        &[file("nested/file", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/dag/put")
        .unwrap();
    assert!(String::from_utf8_lossy(&put.body).contains("\"Tsize\":8192"));
}

#[tokio::test]
async fn cid_v0_links_are_normalized_to_cid_v1_without_readding() {
    const V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let parsed = cid::Cid::try_from(V0).unwrap();
    let v1 = cid::Cid::new_v1(parsed.codec(), parsed.hash().to_owned()).to_string();
    let server = MockServer::start().await;
    mount_identity(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{V0}\",\"CumulativeSize\":123}}")),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    build_directory(
        &KuboClient::new(server.uri()),
        &[file("v0", V0)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/dag/put")
        .unwrap();
    let body = String::from_utf8_lossy(&put.body);
    assert!(body.contains(&v1));
    assert!(!body.contains(V0));
}

#[tokio::test]
async fn a_hash_before_late_stream_failure_is_not_success() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                bytes.push(byte[0]);
                if bytes.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            if bytes.starts_with(b"POST /api/v0/files/stat") {
                let body = format!("{{\"Hash\":\"{EMPTY_RAW}\",\"CumulativeSize\":7}}\n");
                socket
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len())
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(body.as_bytes()).await.unwrap();
            } else if bytes.starts_with(b"POST /api/v0/id") {
                let body = format!("{{\"ID\":\"{NODE_ID}\"}}");
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            } else {
                let content_length = String::from_utf8_lossy(&bytes)
                    .to_ascii_lowercase()
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: ").map(str::trim))
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut request_body = vec![0; content_length];
                socket.read_exact(&mut request_body).await.unwrap();
                let body = format!("{{\"Cid\":{{\"/\":\"{DIRECTORY_CID}\"}}}}\n");
                socket.write_all(format!("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\n\r\n{:X}\r\n{body}\r\n0\r\nX-Stream-Error: late commit failure\r\n\r\n", body.len()).as_bytes()).await.unwrap();
            }
        }
    });
    let error = build_directory(
        &KuboClient::new(url),
        &[file("a", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, DirectoryBuildError::Kubo(ipfs_s3_gateway::error::AppError::KuboRpc { ref detail, .. }) if detail.contains("response stream error")),
        "{error:?}"
    );
    assert!(
        error.candidate().is_none(),
        "late stream failure must not emit a root"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn zero_raw_nan_trailer_requires_confirmed_zero_sized_block() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for step in 0..5 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            loop {
                let mut byte = [0];
                socket.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
                if header.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            match step {
                0 => {
                    assert!(header.starts_with(b"POST /api/v0/files/stat"));
                    socket
                        .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                }
                1 => {
                    assert!(header.starts_with(b"POST /api/v0/dag/stat"));
                    socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\n\r\n0\r\nX-Stream-Error: json: unsupported value: NaN\r\n\r\n").await.unwrap();
                }
                2 => {
                    assert!(header.starts_with(b"POST /api/v0/block/stat"));
                    let body = format!("{{\"Key\":\"{EMPTY_RAW}\",\"Size\":0}}");
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
                3 => {
                    assert!(header.starts_with(b"POST /api/v0/id"));
                    let body = format!("{{\"ID\":\"{NODE_ID}\"}}");
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
                _ => {
                    assert!(header.starts_with(b"POST /api/v0/dag/put"));
                    socket
                        .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
                        .await
                        .unwrap();
                }
            }
        }
    });
    let error = build_directory(
        &KuboClient::new(endpoint),
        &[file("zero", EMPTY_RAW)],
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, DirectoryBuildError::Kubo(_)));
    server.await.unwrap();
}

#[tokio::test]
async fn ten_thousand_320_byte_names_write_bounded_hamt_instead_of_rejecting() {
    let server = MockServer::start().await;
    mount_identity(&server).await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/stat"))
        .respond_with(ResponseTemplate::new(200).set_body_string(""))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/block/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Key\":\"{EMPTY_RAW}\",\"Size\":0}}")),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&server)
        .await;
    let files: Vec<_> = (0..10_000)
        .map(|n| file(&format!("{n:05}{}", "x".repeat(315)), EMPTY_RAW))
        .collect();
    let started = std::time::Instant::now();
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &files,
        &CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, DirectoryBuildError::Kubo(_)), "{error:?}");
    let requests = server.received_requests().await.unwrap();
    let put = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/dag/put")
        .unwrap();
    let body = String::from_utf8_lossy(&put.body);
    assert!(
        body.contains("\"bytes\":\"CAU"),
        "first shard must be Type 5: {body}"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.url.path().starts_with("/api/v0/files/")
                && request.url.path() != "/api/v0/files/stat")
    );
    println!(
        "10k x 320-byte flat names: first HAMT write after {} RPCs in {:?}",
        requests.len(),
        started.elapsed()
    );
}

#[tokio::test]
async fn cancellation_prevents_any_kubo_calls() {
    let server = MockServer::start().await;
    let canceled = CancellationToken::new();
    canceled.cancel();
    let error = build_directory(
        &KuboClient::new(server.uri()),
        &[file("a", EMPTY_RAW)],
        &canceled,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, DirectoryBuildError::Canceled));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cancellation_releases_an_in_flight_metadata_rpc() {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (started, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(bytes.starts_with(b"POST /api/v0/files/stat"));
        let _ = started.send(());
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });
    let cancel = CancellationToken::new();
    let caller_cancel = cancel.clone();
    let build = tokio::spawn(async move {
        build_directory(
            &KuboClient::new(endpoint),
            &[file("a", EMPTY_RAW)],
            &caller_cancel,
        )
        .await
    });
    ready.await.unwrap();
    cancel.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), build)
        .await
        .expect("cancellation must interrupt an in-flight RPC")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, DirectoryBuildError::Canceled));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "requires IPFS3_DIRECTORY_TEST_KUBO_URL and an isolated Kubo node"]
async fn real_kubo_resolves_original_cids_and_bytes() {
    let endpoint = std::env::var("IPFS3_DIRECTORY_TEST_KUBO_URL")
        .expect("NOT RUN: set IPFS3_DIRECTORY_TEST_KUBO_URL to an isolated Kubo node");
    let kubo = KuboClient::new(endpoint.clone());
    let payloads = [
        ("folder/α.txt", b"external bytes 123".to_vec()),
        ("zero", vec![]),
        ("folder/large", vec![b'Z'; 400_000]),
        ("folder/a %?#.txt", b"punctuation".to_vec()),
        ("folder/zero", vec![]),
    ];
    let mut files = Vec::new();
    for (name, bytes) in &payloads {
        let url = format!("{endpoint}/api/v0/add?cid-version=1&pin=false&raw-leaves=true");
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(bytes.clone()).file_name("leaf"),
        );
        let response: serde_json::Value = kubo
            .http()
            .post(url)
            .multipart(form)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        files.push(file(name, response["Hash"].as_str().unwrap()));
    }
    let root = build_directory(&kubo, &files, &CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    let repeated = build_directory(
        &kubo,
        &files.iter().rev().cloned().collect::<Vec<_>>(),
        &CancellationToken::new(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(root.cid, repeated.cid);
    let mut get_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/dag/get")).unwrap();
    get_url.query_pairs_mut().append_pair("arg", &root.cid);
    let dag: serde_json::Value = kubo
        .http()
        .post(get_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dag["Data"]["/"]["bytes"], "CAE");
    assert_eq!(dag["Links"].as_array().unwrap().len(), 2);
    let mut ls_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/ls")).unwrap();
    ls_url.query_pairs_mut().append_pair("arg", &root.cid);
    let ls: serde_json::Value = kubo
        .http()
        .post(ls_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ls["Objects"][0]["Links"].as_array().unwrap().len(), 2);
    let folder_link = dag["Links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|link| link["Name"] == "folder")
        .unwrap();
    let folder_cid = folder_link["Hash"]["/"].as_str().unwrap();
    // dag/put pins intermediate directories as it writes them, rather than
    // relying on a later root pin to protect them from concurrent GC.
    let mut child_pin_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/pin/ls")).unwrap();
    child_pin_url
        .query_pairs_mut()
        .append_pair("arg", folder_cid)
        .append_pair("type", "recursive");
    let child_pin: serde_json::Value = kubo
        .http()
        .post(child_pin_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(child_pin["Keys"][folder_cid]["Type"], "recursive");
    let mut stat_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/dag/stat")).unwrap();
    stat_url
        .query_pairs_mut()
        .append_pair("arg", folder_cid)
        .append_pair("progress", "false");
    let stat: serde_json::Value = kubo
        .http()
        .post(stat_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(folder_link["Tsize"], stat["TotalSize"]);
    for (index, (name, bytes)) in payloads.iter().enumerate() {
        let arg = format!("/ipfs/{}/{name}", root.cid);
        let mut resolve_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/resolve")).unwrap();
        resolve_url.query_pairs_mut().append_pair("arg", &arg);
        let resolved: serde_json::Value = kubo
            .http()
            .post(resolve_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resolved["Path"], format!("/ipfs/{}", files[index].cid));
        let mut cat_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/cat")).unwrap();
        cat_url.query_pairs_mut().append_pair("arg", &arg);
        let actual = kubo
            .http()
            .post(cat_url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(actual.as_ref(), bytes);
    }
}

#[tokio::test]
#[ignore = "requires IPFS3_DIRECTORY_TEST_KUBO_URL and an isolated Kubo node"]
async fn real_kubo_hamt_cid_ls_resolve_cat_and_child_tsize() {
    let endpoint = std::env::var("IPFS3_DIRECTORY_TEST_KUBO_URL")
        .expect("NOT RUN: set IPFS3_DIRECTORY_TEST_KUBO_URL to an isolated Kubo node");
    let kubo = KuboClient::new(endpoint.clone());
    let payload = b"HAMT contents".to_vec();
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(payload.clone()).file_name("leaf"),
    );
    let added: serde_json::Value = kubo
        .http()
        .post(format!(
            "{endpoint}/api/v0/add?cid-version=1&pin=false&raw-leaves=true"
        ))
        .multipart(form)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let cid = added["Hash"].as_str().unwrap();
    let mut files: Vec<_> = (0..900)
        .map(|n| file(&format!("{n:05}{}", "x".repeat(315)), cid))
        .collect();
    files.push(file("hello", cid)); // Murmur3's first HAMT bucket is CB.
    let root = build_directory(&kubo, &files, &CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        root.cid,
        "bafybeihrxfd2dqlx5iombahj3tfucr5kcjmev4tgfr7l5nqm57bl4rsptq"
    );
    let mut dag_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/dag/get")).unwrap();
    dag_url.query_pairs_mut().append_pair("arg", &root.cid);
    let dag: serde_json::Value = kubo
        .http()
        .post(dag_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let raw = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD_NO_PAD,
        dag["Data"]["/"]["bytes"].as_str().unwrap(),
    )
    .unwrap();
    assert_eq!(&raw[..2], &[8, 5]);
    assert!(raw.ends_with(&[40, 34, 48, 128, 2]));
    assert!(
        dag["Links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|link| link["Name"].as_str().unwrap().starts_with("CB"))
    );

    for link in dag["Links"].as_array().unwrap() {
        if link["Name"].as_str().unwrap().len() != 2 {
            continue;
        }
        let child = link["Hash"]["/"].as_str().unwrap();
        let mut stat_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/files/stat")).unwrap();
        stat_url
            .query_pairs_mut()
            .append_pair("arg", &format!("/ipfs/{child}"));
        let stat: serde_json::Value = kubo
            .http()
            .post(stat_url)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(link["Tsize"], stat["CumulativeSize"]);
    }
    let mut ls_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/ls")).unwrap();
    ls_url.query_pairs_mut().append_pair("arg", &root.cid);
    let ls: serde_json::Value = kubo
        .http()
        .post(ls_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        ls["Objects"][0]["Links"].as_array().unwrap().len(),
        files.len()
    );
    for name in ["hello", &files[0].path, &files[900].path] {
        let mut cat_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/cat")).unwrap();
        cat_url
            .query_pairs_mut()
            .append_pair("arg", &format!("/ipfs/{}/{name}", root.cid));
        assert_eq!(
            kubo.http()
                .post(cat_url)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            payload
        );
    }
    println!("Kubo 0.43 HAMT 901 links: {}", root.cid);
}

#[tokio::test]
#[ignore = "requires IPFS3_DIRECTORY_LARGE_TEST_KUBO_URL and an isolated Kubo node"]
async fn real_kubo_hamt_all_ten_thousand_320_byte_paths() {
    let endpoint = std::env::var("IPFS3_DIRECTORY_LARGE_TEST_KUBO_URL")
        .expect("NOT RUN: set IPFS3_DIRECTORY_LARGE_TEST_KUBO_URL to an isolated Kubo node");
    let kubo = KuboClient::new(endpoint.clone());
    let payload = b"ten thousand links".to_vec();
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(payload.clone()).file_name("leaf"),
    );
    let added: serde_json::Value = kubo
        .http()
        .post(format!(
            "{endpoint}/api/v0/add?cid-version=1&pin=false&raw-leaves=true"
        ))
        .multipart(form)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let files: Vec<_> = (0..10_000)
        .map(|n| {
            file(
                &format!("{n:05}{}", "x".repeat(315)),
                added["Hash"].as_str().unwrap(),
            )
        })
        .collect();
    let start = std::time::Instant::now();
    let root = build_directory(&kubo, &files, &CancellationToken::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        root.cid,
        "bafybeihlir4zwyy7icmchnnjikgddyuhuhaxcwsg7oa4zpjttcegovavry"
    );
    let mut ls_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/ls")).unwrap();
    ls_url.query_pairs_mut().append_pair("arg", &root.cid);
    let ls: serde_json::Value = kubo
        .http()
        .post(ls_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(ls["Objects"][0]["Links"].as_array().unwrap().len(), 10_000);
    for index in [0, 4999, 9999] {
        let mut cat_url = reqwest::Url::parse(&format!("{endpoint}/api/v0/cat")).unwrap();
        cat_url
            .query_pairs_mut()
            .append_pair("arg", &format!("/ipfs/{}/{}", root.cid, files[index].path));
        assert_eq!(
            kubo.http()
                .post(cat_url)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            payload
        );
    }
    println!(
        "Kubo 0.43 HAMT 10k paths: {} in {:?}",
        root.cid,
        start.elapsed()
    );
}

#[tokio::test]
#[ignore = "requires IPFS3_DIRECTORY_TEST_KUBO_URL and an isolated Kubo node"]
async fn real_kubo_enforces_two_mib_dag_pb_block_boundary() {
    let endpoint = std::env::var("IPFS3_DIRECTORY_TEST_KUBO_URL")
        .expect("NOT RUN: set IPFS3_DIRECTORY_TEST_KUBO_URL to an isolated Kubo node");
    for (name_len, should_succeed) in [
        (MAX_DIRECTORY_BLOCK_BYTES - 60, true),
        (MAX_DIRECTORY_BLOCK_BYTES, false),
    ] {
        let payload = serde_json::json!({
            "Data": {"/": {"bytes": "CAE="}},
            "Links": [{"Hash": {"/": EMPTY_RAW}, "Name": "a".repeat(name_len), "Tsize": 0}]
        });
        let form = reqwest::multipart::Form::new().part(
            "file",
            reqwest::multipart::Part::bytes(serde_json::to_vec(&payload).unwrap())
                .file_name("directory.json"),
        );
        let response = KuboClient::new(endpoint.clone()).http().post(format!("{endpoint}/api/v0/dag/put?input-codec=dag-json&store-codec=dag-pb&hash=sha2-256&pin=false")).multipart(form).send().await.unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert_eq!(
            status.is_success(),
            should_succeed,
            "name_len={name_len}, status={status}"
        );
        if should_succeed {
            let cid = serde_json::from_str::<serde_json::Value>(&body).unwrap()["Cid"]["/"]
                .as_str()
                .unwrap()
                .to_owned();
            let mut url = reqwest::Url::parse(&format!("{endpoint}/api/v0/block/stat")).unwrap();
            url.query_pairs_mut().append_pair("arg", &cid);
            let block: serde_json::Value = KuboClient::new(endpoint.clone())
                .http()
                .post(url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let size = block["Size"].as_u64().unwrap();
            println!("Kubo 0.43 near-limit dag-pb block size: {size} bytes");
            assert!(
                size <= MAX_DIRECTORY_BLOCK_BYTES as u64
                    && size >= (MAX_DIRECTORY_BLOCK_BYTES - 128) as u64,
                "observed {size}"
            );
        } else {
            assert!(
                body.contains("block") || body.contains("size"),
                "unexpected error body (status {status})"
            );
        }
    }
}
