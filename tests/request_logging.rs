use std::{
    fs::{self, File},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use base64::Engine as _;
use http::{HeaderMap, HeaderValue, StatusCode};
use sea_orm::{ConnectionTrait as _, Database};

#[allow(dead_code)]
#[path = "support/sigv4.rs"]
mod sigv4;

const SENTINELS: &[&str] = &[
    "AUTHORIZATION_SECRET_SENTINEL",
    "PRESIGNED_QUERY_SENTINEL",
    "PRESIGNED_SIGNATURE_SENTINEL",
    "COOKIE_SECRET_SENTINEL",
    "SSE_C_KEY_SENTINEL",
    "IMPORT_URL_TOKEN_SENTINEL",
    "REQUEST_BODY_SENTINEL",
    "KUBO_CONFIG_TOKEN_SENTINEL",
];

#[test]
fn request_diagnostics_are_safe_under_all_supported_filter_levels() {
    for rust_log in [
        "info",
        "debug",
        "trace",
        "s3s=trace,s3s::service=trace,hyper=trace,reqwest=trace",
        "ipfs_s3_gateway=trace,s3s=trace,s3s::service=trace,hyper=trace,reqwest=trace",
    ] {
        assert_safe_gateway_logs(rust_log);
    }
}

fn assert_safe_gateway_logs(rust_log: &str) {
    let address = reserve_loopback_address();
    let temp = tempfile::tempdir().expect("create request logging temp directory");
    let stdout_path = temp.path().join("stdout.log");
    let stderr_path = temp.path().join("stderr.log");
    let mut process = spawn_gateway(
        address,
        &temp,
        rust_log,
        "sqlite::memory:",
        &stdout_path,
        &stderr_path,
    );

    wait_until_listening(address, &mut process.0, &stdout_path, &stderr_path);
    send_request(address, "/health", &[], "");
    send_request(
        address,
        "/bucket/object?X-Amz-Credential=PRESIGNED_QUERY_SENTINEL&X-Amz-Signature=PRESIGNED_SIGNATURE_SENTINEL&import-url=https%3A%2F%2Fexample.invalid%2Farchive.zip%3Ftoken%3DIMPORT_URL_TOKEN_SENTINEL",
        &[
            (
                "Authorization",
                "AWS4-HMAC-SHA256 AUTHORIZATION_SECRET_SENTINEL",
            ),
            ("Cookie", "session=COOKIE_SECRET_SENTINEL"),
            (
                "x-amz-server-side-encryption-customer-key",
                "SSE_C_KEY_SENTINEL",
            ),
        ],
        "REQUEST_BODY_SENTINEL",
    );

    thread::sleep(Duration::from_millis(100));
    process.stop();
    let logs = read_logs(&stdout_path, &stderr_path);

    assert!(
        logs.matches("request completed").count() >= 2,
        "safe request completion diagnostics missing for RUST_LOG={rust_log}"
    );
    assert!(
        logs.contains("request_id") && logs.contains("status"),
        "request id/status fields missing for RUST_LOG={rust_log}"
    );
    for sentinel in SENTINELS {
        assert!(
            !logs.contains(sentinel),
            "secret sentinel {sentinel} leaked for RUST_LOG={rust_log}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_delete_objects_failure_logs_only_a_bounded_class() {
    const KEY_SENTINEL: &str = "DELETE_BODY_KEY_SENTINEL";
    const ERROR_SENTINEL: &str = "DB_FREEFORM_ERROR_SENTINEL";

    for rust_log in [
        "info",
        "debug",
        "trace",
        "ipfs_s3_gateway=trace,s3s::service=trace,hyper=trace,reqwest=trace",
    ] {
        let address = reserve_loopback_address();
        let temp = tempfile::tempdir().expect("create authenticated logging temp directory");
        let database_path = temp.path().join("gateway.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let stdout_path = temp.path().join("stdout.log");
        let stderr_path = temp.path().join("stderr.log");
        let mut process = spawn_gateway(
            address,
            &temp,
            rust_log,
            &database_url,
            &stdout_path,
            &stderr_path,
        );
        wait_until_listening(address, &mut process.0, &stdout_path, &stderr_path);

        let db = Database::connect(&database_url)
            .await
            .expect("connect to gateway logging database");
        ipfs_s3_gateway::store::bucket::create(&db, "logging-bucket", None)
            .await
            .expect("seed logging bucket");
        db.execute_unprepared(&format!(
            "CREATE TRIGGER fail_delete_logging BEFORE INSERT ON import_destinations \
             WHEN NEW.key = '{KEY_SENTINEL}' \
             BEGIN SELECT RAISE(FAIL, '{ERROR_SENTINEL}'); END"
        ))
        .await
        .expect("install DeleteObjects logging failure trigger");

        let body = format!(
            "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Object><Key>{KEY_SENTINEL}</Key></Object></Delete>"
        )
        .into_bytes();
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(&body).0);
        headers.insert(
            "content-md5",
            HeaderValue::from_str(&digest).expect("base64 MD5 header"),
        );
        let response = sigv4::send_sigv4(
            reqwest::Method::POST,
            &format!("http://{address}"),
            "logging-bucket",
            "",
            &[("delete", "")],
            body,
            headers,
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let response_body = response.text().await.expect("read DeleteObjects response");
        assert!(response_body.contains("<Code>InternalError</Code>"));

        tokio::time::sleep(Duration::from_millis(100)).await;
        process.stop();
        let logs = read_logs(&stdout_path, &stderr_path);
        assert!(logs.contains("request completed"), "RUST_LOG={rust_log}");
        assert!(!logs.contains(ERROR_SENTINEL), "RUST_LOG={rust_log}");
        assert!(!logs.contains(KEY_SENTINEL), "RUST_LOG={rust_log}");
        assert!(logs.contains("failure_class"), "RUST_LOG={rust_log}");
        assert!(logs.contains("database"), "RUST_LOG={rust_log}");
    }
}

fn spawn_gateway(
    address: SocketAddr,
    temp: &tempfile::TempDir,
    rust_log: &str,
    database_url: &str,
    stdout_path: &std::path::Path,
    stderr_path: &std::path::Path,
) -> GatewayProcess {
    GatewayProcess(
        Command::new(env!("CARGO_BIN_EXE_ipfs-s3-gateway"))
            .env("RUST_LOG", rust_log)
            .env("IPFS_S3_CONFIG", temp.path().join("missing-config.toml"))
            .env("IPFS_S3_BIND", address.to_string())
            .env("IPFS_S3_DATABASE_URL", database_url)
            .env(
                "IPFS_S3_KUBO_RPC_URL",
                "http://127.0.0.1:1/KUBO_CONFIG_TOKEN_SENTINEL",
            )
            .env("IPFS_S3_ACCESS_KEY_ID", "test")
            .env("IPFS_S3_SECRET_ACCESS_KEY", "test")
            .env("IPFS_S3_MASTER_KEY", "0".repeat(64))
            .stdout(Stdio::from(
                File::create(stdout_path).expect("create gateway stdout capture"),
            ))
            .stderr(Stdio::from(
                File::create(stderr_path).expect("create gateway stderr capture"),
            ))
            .spawn()
            .expect("start gateway for request logging test"),
    )
}

fn read_logs(stdout_path: &std::path::Path, stderr_path: &std::path::Path) -> String {
    format!(
        "{}{}",
        fs::read_to_string(stdout_path).expect("read gateway stdout capture"),
        fs::read_to_string(stderr_path).expect("read gateway stderr capture")
    )
}

struct GatewayProcess(std::process::Child);

impl GatewayProcess {
    fn stop(&mut self) {
        if self
            .0
            .try_wait()
            .expect("inspect gateway logging fixture status")
            .is_none()
        {
            self.0.kill().expect("stop gateway logging fixture");
            self.0
                .wait()
                .expect("collect gateway logging fixture status");
        }
    }
}

impl Drop for GatewayProcess {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn reserve_loopback_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let address = listener.local_addr().expect("read reserved loopback port");
    drop(listener);
    address
}

fn wait_until_listening(
    address: SocketAddr,
    child: &mut std::process::Child,
    stdout_path: &std::path::Path,
    stderr_path: &std::path::Path,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("inspect gateway fixture status") {
            let stdout = fs::read_to_string(stdout_path).expect("read failed gateway stdout");
            let stderr = fs::read_to_string(stderr_path).expect("read failed gateway stderr");
            panic!(
                "gateway logging fixture exited before listening: {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
        assert!(
            Instant::now() < deadline,
            "gateway logging fixture did not listen within 20 seconds"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn send_request(address: SocketAddr, target: &str, headers: &[(&str, &str)], body: &str) {
    let mut stream = TcpStream::connect(address).expect("connect to gateway logging fixture");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set request logging response timeout");
    write!(
        stream,
        "PUT {target} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    )
    .expect("write request logging fixture request line");
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n").expect("write request logging fixture header");
    }
    write!(stream, "\r\n{body}").expect("write request logging fixture body");
    stream
        .flush()
        .expect("flush request logging fixture request");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read request logging fixture response");
    assert!(
        response.starts_with(b"HTTP/1.1 "),
        "gateway did not return an HTTP response"
    );
}
