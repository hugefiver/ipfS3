//! Small, bounded HTTP/ZIP helpers only; no helper selftests or support/mod.rs.

use super::{BUCKET, BenchResult};
use axum::{Router, error_handling::HandleError, middleware};
use hmac::{Hmac, KeyInit, Mac};
use ipfs_s3_gateway::{
    auth::GatewayAuth,
    import::{ImportConfig, downloader::SourceDownloader, pipeline::ImportCoordinator},
    s3::{handler::S3Impl, route::gateway::GatewayRoute},
    state::AppState,
};
use s3s::{Body, HttpError, service::S3ServiceBuilder, validation::AwsNameValidation};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct Server {
    pub port: u16,
    cancel: CancellationToken,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl Server {
    pub async fn start(app: Router) -> BenchResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "loopback bind failed")?;
        let port = listener
            .local_addr()
            .map_err(|_| "loopback address unavailable")?
            .port();
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone();
        let join = tokio::spawn(async move {
            // Never print framework errors (they can contain raw addresses).
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await;
        });
        Ok(Self {
            port,
            cancel,
            join: Some(join),
        })
    }

    pub async fn shutdown(&mut self) -> bool {
        self.cancel.cancel();
        let Some(mut join) = self.join.take() else {
            return true;
        };
        match tokio::time::timeout(Duration::from_secs(3), &mut join).await {
            Ok(result) => result.is_ok(),
            Err(_) => {
                join.abort();
                let _ = join.await;
                false
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

pub async fn gateway(state: Arc<AppState>) -> BenchResult<Server> {
    let imports = ImportConfig::default()
        .validate()
        .map_err(|_| "import fixture configuration failed")?;
    let downloader = SourceDownloader::production(Arc::new(imports.clone()));
    let imports = ImportCoordinator::new(imports, downloader);
    let mut builder = S3ServiceBuilder::new(S3Impl::new(state.clone()));
    builder.set_auth(GatewayAuth::new(state.clone()));
    builder.set_validation(AwsNameValidation::new());
    builder.set_route(GatewayRoute::new(state.clone(), imports));
    let app = Router::new()
        .fallback_service(HandleError::new(
            builder.build(),
            |_error: HttpError| async {
                http::Response::builder()
                    .status(500)
                    .body(Body::from("fixture service error".to_owned()))
                    .unwrap()
            },
        ))
        .layer(middleware::from_fn(
            ipfs_s3_gateway::s3::http::bridge_chunked_content_length,
        ))
        .layer(middleware::from_fn_with_state(
            state,
            ipfs_s3_gateway::cors::http::bucket_cors,
        ));
    Server::start(app).await
}

fn mac(key: &[u8], value: &str) -> Vec<u8> {
    let mut h = <Hmac<Sha256> as KeyInit>::new_from_slice(key).unwrap();
    h.update(value.as_bytes());
    h.finalize().into_bytes().to_vec()
}

/// Fixed ASCII fixture paths/query only. No general-purpose signing framework.
pub async fn put(port: u16, key: &str, body: Vec<u8>, zip: bool) -> BenchResult<u128> {
    put_expected(port, key, body, zip, 65).await
}

pub async fn put_expected(
    port: u16,
    key: &str,
    body: Vec<u8>,
    zip: bool,
    outputs: usize,
) -> BenchResult<u128> {
    signed_request(port, key, body, zip, outputs, false).await
}

/// Logical cancellation only. The provider fixture still rejects DELETE/unpin.
pub async fn delete(port: u16, key: &str) -> BenchResult<u128> {
    signed_request(port, key, Vec::new(), false, 0, true).await
}

async fn signed_request(
    port: u16,
    key: &str,
    body: Vec<u8>,
    zip: bool,
    outputs: usize,
    delete: bool,
) -> BenchResult<u128> {
    let started = std::time::Instant::now();
    let host = format!("127.0.0.1:{port}");
    let path = format!("/{BUCKET}/{key}");
    let query = if zip { "decompress-zip=bulk%2F" } else { "" };
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let hash = hex::encode(Sha256::digest(&body));
    let mut headers = std::collections::BTreeMap::from([
        ("host", host.clone()),
        ("x-amz-content-sha256", hash.clone()),
        ("x-amz-date", timestamp.clone()),
    ]);
    if zip {
        for (key, value) in [
            ("x-ipfs3-zip-contract", "v2"),
            ("x-ipfs3-zip-publish-source", "false"),
            ("x-ipfs3-zip-publish-extracted", "true"),
            ("x-ipfs3-zip-targets", "extracted"),
            ("x-ipfs3-zip-token", "stage8-offline-batch"),
            ("x-amz-tagging", "ipfs-s3%3Azip-root=false"),
        ] {
            headers.insert(key, value.into());
        }
    }
    let names = headers.keys().copied().collect::<Vec<_>>().join(";");
    let canonical_headers = headers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>();
    let method = if delete { "DELETE" } else { "PUT" };
    let request = format!("{method}\n{path}\n{query}\n{canonical_headers}\n{names}\n{hash}");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(request.as_bytes()))
    );
    let signing = mac(
        &mac(&mac(&mac(b"AWS4test", &date), "us-east-1"), "s3"),
        "aws4_request",
    );
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential=test/{scope}, SignedHeaders={names}, Signature={}",
        hex::encode(mac(&signing, &to_sign))
    );
    let url = format!(
        "http://{host}{path}{}",
        if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        }
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(if zip && outputs == 10_000 {
            120
        } else {
            15
        }))
        .build()
        .map_err(|_| "S3 fixture client failed")?;
    let mut request = client
        .request(
            if delete {
                reqwest::Method::DELETE
            } else {
                reqwest::Method::PUT
            },
            url,
        )
        .header("authorization", auth)
        .body(body);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request
        .send()
        .await
        .map_err(|_| "signed fixture PUT transport failed")?;
    if response.status()
        != if delete {
            reqwest::StatusCode::NO_CONTENT
        } else {
            reqwest::StatusCode::OK
        }
    {
        return Err("signed fixture PUT was rejected");
    }
    if zip {
        let text = response
            .text()
            .await
            .map_err(|_| "ZIP fixture response unavailable")?;
        if !text.contains(&format!("<PublishedCount>{outputs}</PublishedCount>"))
            || !text.contains("<RootStatus>disabled</RootStatus>")
        {
            return Err(
                "ZIP fixture did not publish the exact requested outputs with root disabled",
            );
        }
    }
    Ok(started.elapsed().as_micros())
}

pub fn payload(class: &str, index: usize) -> Vec<u8> {
    let mut bytes = format!("{class}-{index:04}").into_bytes();
    bytes.resize(32, b'.');
    bytes
}

pub fn cid(bytes: &[u8]) -> String {
    let hash = cid::multihash::Multihash::<64>::wrap(0x12, &Sha256::digest(bytes)).unwrap();
    cid::Cid::new_v1(0x55, hash).to_string()
}

fn crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}
fn u16le(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u16).to_le_bytes());
}
fn u32le(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u32).to_le_bytes());
}

/// 65 stored ZIP entries, 33 distinct 32-byte contents: logical != physical quota.
pub fn archive() -> Vec<u8> {
    archive_with_counts(65, 33)
}

pub fn archive_with_counts(outputs: usize, distinct: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for index in 0..outputs {
        let name = format!("item-{index:02}.bin");
        let bytes = payload("bulk", index % distinct);
        let checksum = crc(&bytes) as usize;
        let offset = out.len();
        u32le(&mut out, 0x04034b50);
        for v in [20, 0, 0, 0, 0] {
            u16le(&mut out, v);
        }
        for v in [checksum, bytes.len(), bytes.len()] {
            u32le(&mut out, v);
        }
        for v in [name.len(), 0] {
            u16le(&mut out, v);
        }
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&bytes);
        u32le(&mut central, 0x02014b50);
        for v in [20, 20, 0, 0, 0, 0] {
            u16le(&mut central, v);
        }
        for v in [checksum, bytes.len(), bytes.len()] {
            u32le(&mut central, v);
        }
        for v in [name.len(), 0, 0, 0, 0] {
            u16le(&mut central, v);
        }
        for v in [0, offset] {
            u32le(&mut central, v);
        }
        central.extend_from_slice(name.as_bytes());
    }
    let offset = out.len();
    let size = central.len();
    out.extend(central);
    u32le(&mut out, 0x06054b50);
    for v in [0, 0, outputs, outputs] {
        u16le(&mut out, v);
    }
    for v in [size, offset] {
        u32le(&mut out, v);
    }
    u16le(&mut out, 0);
    out
}
