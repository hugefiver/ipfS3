//! First-intake capture and complete native source proof for store-tail fixtures.
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use ipfs_s3_gateway::{
    crypto::key::MasterKey,
    import::{
        ImportConfig,
        downloader::{
            AuthorizedSource, DownloadError, DownloadLimits, DownloadStream, ImportHttpTransport,
            ImportResolver, SourceDownloader, StrictPublicAddressPolicy,
        },
        pipeline::ImportCoordinator,
        v2_source::{VerifiedZipSource, fetch_v2_zip_source},
    },
    pinning::{
        coordinator::PinningCoordinator,
        decision::DecisionOrigin,
        zip_policy::{CapturedZipIntent, ZipCaptureContext, capture_zip_intent},
    },
    state::AppState,
    store::{
        Store,
        zip::{execution, import_intake},
    },
};
use sea_orm::DatabaseConnection;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const INPUT: &[u8] = b"PK\x05\x06\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
const URL: &str = "https://host/source?credential=secret";

pub fn input_sha256() -> String {
    hex::encode(Sha256::digest(INPUT))
}

pub fn descriptor() -> String {
    serde_json::to_string(&("url", URL)).unwrap()
}

pub fn capture() -> CapturedZipIntent {
    let pinning = PinningCoordinator::disabled_for_test();
    let mut headers = http::HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "import-token"),
    ] {
        headers.insert(name, http::HeaderValue::from_static(value));
    }
    let controls = ipfs_s3_gateway::zip::options::parse_authenticated_zip_controls(
        &headers,
        &[("decompress-zip".into(), "out/".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    capture_zip_intent(
        pinning.effective_config(),
        pinning.decompress_zip_config(),
        pinning.control_mode(),
        ZipCaptureContext {
            bucket: "bucket",
            source_key: "source.zip",
            target_prefix: "out/",
            tags: &[],
        },
        &controls,
        DecisionOrigin::new("principal", "import-execution"),
    )
    .unwrap()
}

#[derive(Default)]
struct Source(AtomicUsize);

#[async_trait::async_trait]
impl ImportResolver for Source {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        assert_eq!(host, "host");
        Ok(vec![SocketAddr::from(([1, 1, 1, 1], port))])
    }
}

#[async_trait::async_trait]
impl ImportHttpTransport for Source {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(DownloadStream {
            body: Box::pin(futures_util::stream::iter([
                Ok(Bytes::copy_from_slice(&INPUT[..11])),
                Ok(Bytes::copy_from_slice(&INPUT[11..])),
            ])),
            total: Some(INPUT.len() as u64),
            content_type: Some("application/zip".into()),
        })
    }
}

pub async fn bind(db: &DatabaseConnection, claim: &execution::Claim) -> VerifiedZipSource {
    let kubo = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/add"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "{{\"Hash\":\"{CID}\",\"Size\":\"{}\"}}\n",
            INPUT.len()
        )))
        .expect(1)
        .mount(&kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{CID}\"]}}")),
        )
        .expect(1)
        .mount(&kubo)
        .await;
    let state = AppState {
        kubo: ipfs_s3_gateway::kubo::KuboClient::new(kubo.uri()),
        cold_kubo: None,
        store: Store::new(db.clone()),
        credentials: Default::default(),
        master_key: MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: PinningCoordinator::disabled_for_test(),
    };
    let source = Arc::new(Source::default());
    let config = ImportConfig {
        allowed_https_origins: vec!["https://host".into()],
        ..ImportConfig::default()
    }
    .validate()
    .unwrap();
    let downloader = SourceDownloader::with_components(
        Arc::new(config.clone()),
        source.clone(),
        Arc::new(StrictPublicAddressPolicy),
        source.clone(),
    );
    let coordinator = ImportCoordinator::new(config, downloader);
    let verified = fetch_v2_zip_source(
        &coordinator,
        &state,
        &descriptor(),
        Some(&input_sha256()),
        CancellationToken::new(),
        &state.pinning.zip_extraction_limits(),
    )
    .await
    .unwrap();
    assert_eq!(verified.sha256, input_sha256());
    assert_eq!(verified.size, INPUT.len() as u64);
    assert_eq!(verified.cid, CID);
    assert_eq!(source.0.load(Ordering::SeqCst), 1);
    let requests = kubo.received_requests().await.unwrap();
    let upload = requests
        .iter()
        .find(|request| request.url.path() == "/api/v0/add")
        .unwrap();
    assert!(
        upload.body.windows(INPUT.len()).any(|bytes| bytes == INPUT),
        "native upload drained the complete input"
    );
    import_intake::bind_verified_input(
        db,
        claim,
        &verified.sha256,
        &verified.cid,
        verified.size as i64,
    )
    .await
    .unwrap();
    kubo.verify().await;
    verified
}
