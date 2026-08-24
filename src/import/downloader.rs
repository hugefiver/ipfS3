use std::{
    error::Error as StdError,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Once},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use url::{Host, Url};

use super::{ImportFailure, ImportFailureCode, ValidatedImportConfig};

const MAX_CONTENT_TYPE_BYTES: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    #[error("source is not allowed")]
    NotAllowed,
    #[error("source name resolution failed")]
    Dns,
    #[error("source connection failed")]
    Connect,
    #[error("source TLS certificate validation failed")]
    TlsCertificate,
    #[error("source TLS transport failed")]
    TlsTransport,
    #[error("source redirected")]
    Redirect,
    #[error("source returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("source exceeded byte limit")]
    TooLarge,
    #[error("source stalled")]
    Stalled,
    #[error("source transfer was canceled")]
    Canceled,
    #[error("source response was invalid")]
    InvalidResponse,
}

impl DownloadError {
    pub fn into_import_failure(self) -> ImportFailure {
        let (code, message, retryable) = match self {
            Self::NotAllowed => (
                ImportFailureCode::SourceUnreachable,
                "source is not allowed",
                false,
            ),
            Self::Dns | Self::Connect | Self::TlsTransport => (
                ImportFailureCode::SourceUnreachable,
                "source could not be reached",
                true,
            ),
            Self::TlsCertificate => (
                ImportFailureCode::SourceUnreachable,
                "source TLS certificate validation failed",
                false,
            ),
            Self::Redirect => (
                ImportFailureCode::SourceRedirected,
                "source redirected",
                false,
            ),
            Self::HttpStatus(status) => (
                ImportFailureCode::SourceHttpError,
                if (500..=599).contains(&status) {
                    "source returned a server error"
                } else {
                    "source returned an HTTP error"
                },
                (500..=599).contains(&status),
            ),
            Self::TooLarge => (
                ImportFailureCode::SourceTooLarge,
                "source exceeded byte limit",
                false,
            ),
            Self::Stalled => (ImportFailureCode::SourceStalled, "source stalled", true),
            // Task 8 handles cancellation before conversion so it can distinguish
            // shutdown from a lost ownership lease. This fallback is retryable and
            // deliberately does not claim that the source made a terminal error.
            Self::Canceled => (
                ImportFailureCode::SourceUnreachable,
                "source transfer was canceled",
                true,
            ),
            Self::InvalidResponse => (
                ImportFailureCode::SourceUnreachable,
                "source response was invalid",
                false,
            ),
        };

        ImportFailure {
            code,
            message: message.to_owned(),
            retryable,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DownloadLimits {
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_bytes: u64,
}

#[async_trait::async_trait]
pub trait ImportResolver: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError>;
}

pub trait AddressPolicy: Send + Sync {
    fn validate(&self, addresses: &[SocketAddr]) -> Result<(), DownloadError>;
}

#[derive(Clone)]
pub struct AuthorizedSource {
    url: Url,
    server_name: String,
    addresses: Vec<SocketAddr>,
}

impl fmt::Debug for AuthorizedSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedSource { redacted: true }")
    }
}

#[async_trait::async_trait]
pub trait ImportHttpTransport: Send + Sync {
    async fn open(
        &self,
        source: AuthorizedSource,
        limits: DownloadLimits,
        progress: tokio::sync::watch::Sender<u64>,
        cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError>;
}

pub struct ReqwestImportHttpTransport {
    extra_root_certificates: Vec<reqwest::Certificate>,
    connect_timeout: Duration,
}

impl ReqwestImportHttpTransport {
    pub fn new(limits: DownloadLimits, extra_root_certificates: Vec<reqwest::Certificate>) -> Self {
        Self {
            extra_root_certificates,
            connect_timeout: limits.connect_timeout,
        }
    }

    fn client_for(
        &self,
        source: &AuthorizedSource,
        limits: DownloadLimits,
    ) -> Result<reqwest::Client, DownloadError> {
        install_rustls_crypto_provider();
        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.connect_timeout.min(limits.connect_timeout))
            .resolve_to_addrs(&source.server_name, &source.addresses);

        for certificate in &self.extra_root_certificates {
            builder = builder.add_root_certificate(certificate.clone());
        }

        builder.build().map_err(|_| DownloadError::InvalidResponse)
    }
}

fn install_rustls_crypto_provider() {
    static RUSTLS_PROVIDER: Once = Once::new();
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[async_trait::async_trait]
impl ImportHttpTransport for ReqwestImportHttpTransport {
    async fn open(
        &self,
        source: AuthorizedSource,
        limits: DownloadLimits,
        progress: tokio::sync::watch::Sender<u64>,
        cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        if cancel.is_cancelled() {
            return Err(DownloadError::Canceled);
        }

        let client = self.client_for(&source, limits)?;
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(DownloadError::Canceled),
            result = client.get(source.url.clone()).send() => result.map_err(|error| classify_reqwest_error(&error))?,
        };

        let status = response.status();
        if status.is_redirection() {
            return Err(DownloadError::Redirect);
        }
        if !status.is_success() {
            return Err(DownloadError::HttpStatus(status.as_u16()));
        }

        let total = validated_content_length(response.headers())?;
        if total.is_some_and(|length| length > limits.max_bytes) {
            return Err(DownloadError::TooLarge);
        }

        let content_type = safe_content_type(response.headers());
        let body = response
            .bytes_stream()
            .map(|frame| frame.map_err(|error| classify_reqwest_error(&error)));

        Ok(DownloadStream {
            body: bounded_body(body, limits, progress, cancel),
            total,
            content_type,
        })
    }
}

pub struct StrictPublicAddressPolicy;

impl AddressPolicy for StrictPublicAddressPolicy {
    fn validate(&self, addresses: &[SocketAddr]) -> Result<(), DownloadError> {
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|address| !is_public_address(address.ip()))
        {
            return Err(DownloadError::NotAllowed);
        }
        Ok(())
    }
}

pub struct SourceDownloader {
    config: Arc<ValidatedImportConfig>,
    resolver: Arc<dyn ImportResolver>,
    address_policy: Arc<dyn AddressPolicy>,
    transport: Arc<dyn ImportHttpTransport>,
}

impl SourceDownloader {
    pub fn production(config: Arc<ValidatedImportConfig>) -> Self {
        let limits = limits_from_config(&config);
        Self::with_components(
            config,
            Arc::new(SystemImportResolver),
            Arc::new(StrictPublicAddressPolicy),
            Arc::new(ReqwestImportHttpTransport::new(limits, Vec::new())),
        )
    }

    #[doc(hidden)]
    pub fn with_components(
        config: Arc<ValidatedImportConfig>,
        resolver: Arc<dyn ImportResolver>,
        address_policy: Arc<dyn AddressPolicy>,
        transport: Arc<dyn ImportHttpTransport>,
    ) -> Self {
        Self {
            config,
            resolver,
            address_policy,
            transport,
        }
    }

    pub async fn authorize(&self, source: &Url) -> Result<AuthorizedSource, DownloadError> {
        if source.scheme() != "https"
            || source.fragment().is_some()
            || has_userinfo(source)
            || !matches!(source.host(), Some(Host::Domain(_)))
        {
            return Err(DownloadError::NotAllowed);
        }

        let Some(host) = source.host_str().filter(|host| !host.is_empty()) else {
            return Err(DownloadError::NotAllowed);
        };
        let port = source
            .port_or_known_default()
            .ok_or(DownloadError::NotAllowed)?;

        let mut normalized = source.clone();
        if normalized.port() == Some(443) {
            normalized
                .set_port(None)
                .map_err(|_| DownloadError::NotAllowed)?;
        }
        if !self.config.allowed_origins.contains(&normalized.origin()) {
            return Err(DownloadError::NotAllowed);
        }

        let addresses = self
            .resolver
            .resolve(host, port)
            .await
            .map_err(|_| DownloadError::Dns)?;
        if addresses.is_empty() {
            return Err(DownloadError::Dns);
        }
        self.address_policy.validate(&addresses)?;

        Ok(AuthorizedSource {
            url: source.clone(),
            server_name: host.to_owned(),
            addresses,
        })
    }

    pub async fn open(
        &self,
        source: &Url,
        progress: tokio::sync::watch::Sender<u64>,
        cancel: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        if cancel.is_cancelled() {
            return Err(DownloadError::Canceled);
        }
        let authorized = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DownloadError::Canceled),
            authorized = self.authorize(source) => authorized?,
        };
        self.transport
            .open(
                authorized,
                limits_from_config(&self.config),
                progress,
                cancel,
            )
            .await
    }
}

pub struct DownloadStream {
    pub body: Pin<Box<dyn Stream<Item = Result<Bytes, DownloadError>> + Send>>,
    pub total: Option<u64>,
    pub content_type: Option<String>,
}

impl fmt::Debug for DownloadStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DownloadStream { redacted: true }")
    }
}

struct SystemImportResolver;

#[async_trait::async_trait]
impl ImportResolver for SystemImportResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        let addresses = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| DownloadError::Dns)?
            .filter(|address| address.port() == port)
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(DownloadError::Dns);
        }
        Ok(addresses)
    }
}

fn limits_from_config(config: &ValidatedImportConfig) -> DownloadLimits {
    DownloadLimits {
        connect_timeout: Duration::from_secs(config.raw.connect_timeout_secs),
        idle_timeout: Duration::from_secs(config.raw.idle_timeout_secs),
        max_bytes: config.raw.max_download_bytes,
    }
}

fn has_userinfo(source: &Url) -> bool {
    let serialized = source.as_str();
    let Some(authority) = serialized
        .strip_prefix(source.scheme())
        .and_then(|remainder| remainder.strip_prefix(":"))
        .and_then(|remainder| remainder.strip_prefix("//"))
    else {
        return true;
    };
    let authority_end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}

fn is_public_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    !matches!(
        (first, second, third),
        (0, _, _)
            | (10, _, _)
            | (100, 64..=127, _)
            | (127, _, _)
            | (169, 254, _)
            | (172, 16..=31, _)
            | (192, 0, 0)
            | (192, 0, 2)
            | (192, 31, 196)
            | (192, 52, 193)
            | (192, 88, 99)
            | (192, 168, _)
            | (192, 175, 48)
            | (198, 18..=19, _)
            | (198, 51, 100)
            | (203, 0, 113)
            | (224..=u8::MAX, _, _)
    )
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }

    let bytes = address.octets();
    if (bytes[0] & 0xe0) != 0x20 {
        return false;
    }

    !is_ipv6_prefix(address, Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23)
        && !is_ipv6_prefix(address, Ipv6Addr::new(0x2001, 2, 0, 0, 0, 0, 0, 0), 48)
        && !is_ipv6_prefix(address, Ipv6Addr::new(0x2001, 0x10, 0, 0, 0, 0, 0, 0), 28)
        && !is_ipv6_prefix(address, Ipv6Addr::new(0x2001, 0x20, 0, 0, 0, 0, 0, 0), 28)
        && !is_ipv6_prefix(address, Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32)
        && !is_ipv6_prefix(address, Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16)
}

fn is_ipv6_prefix(address: Ipv6Addr, network_start: Ipv6Addr, prefix_bits: u8) -> bool {
    let address = address.octets();
    let network_start = network_start.octets();
    let whole_bytes = usize::from(prefix_bits / 8);
    let remaining_bits = prefix_bits % 8;
    if address[..whole_bytes] != network_start[..whole_bytes] {
        return false;
    }
    if remaining_bits == 0 {
        return true;
    }
    let mask = u8::MAX << (8 - remaining_bits);
    address[whole_bytes] & mask == network_start[whole_bytes] & mask
}

fn validated_content_length(
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<u64>, DownloadError> {
    let mut length = None;
    for value in headers.get_all(reqwest::header::CONTENT_LENGTH) {
        let parsed = value
            .to_str()
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .ok_or(DownloadError::InvalidResponse)?;
        if length
            .replace(parsed)
            .is_some_and(|previous| previous != parsed)
        {
            return Err(DownloadError::InvalidResponse);
        }
    }
    Ok(length)
}

fn safe_content_type(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let value = headers.get(reqwest::header::CONTENT_TYPE)?.to_str().ok()?;
    if value.len() > MAX_CONTENT_TYPE_BYTES || !value.is_ascii() || !valid_media_type(value) {
        return None;
    }
    Some(value.to_owned())
}

fn valid_media_type(value: &str) -> bool {
    let Some(media_type) = value.split(';').next() else {
        return false;
    };
    let Some((type_, subtype)) = media_type.trim().split_once('/') else {
        return false;
    };
    !type_.is_empty()
        && !subtype.is_empty()
        && type_.bytes().all(is_http_token)
        && subtype.bytes().all(is_http_token)
}

fn is_http_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn bounded_body<S>(
    body: S,
    limits: DownloadLimits,
    progress: tokio::sync::watch::Sender<u64>,
    cancel: CancellationToken,
) -> Pin<Box<dyn Stream<Item = Result<Bytes, DownloadError>> + Send>>
where
    S: Stream<Item = Result<Bytes, DownloadError>> + Send + 'static,
{
    Box::pin(async_stream::stream! {
        let mut body = Box::pin(body);
        let mut downloaded = 0_u64;

        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    yield Err(DownloadError::Canceled);
                    return;
                }
                frame = tokio::time::timeout(limits.idle_timeout, body.next()) => frame,
            };

            let frame = match next {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(error))) => {
                    yield Err(error);
                    return;
                }
                Ok(None) => return,
                Err(_) => {
                    yield Err(DownloadError::Stalled);
                    return;
                }
            };

            let Some(next_downloaded) = downloaded.checked_add(frame.len() as u64) else {
                yield Err(DownloadError::TooLarge);
                return;
            };
            if next_downloaded > limits.max_bytes {
                yield Err(DownloadError::TooLarge);
                return;
            }
            downloaded = next_downloaded;
            let _ = progress.send(downloaded);
            yield Ok(frame);
        }
    })
}

fn classify_reqwest_error(error: &reqwest::Error) -> DownloadError {
    if error_chain_contains_invalid_certificate(error) || has_certificate_fallback(error) {
        return DownloadError::TlsCertificate;
    }
    if error_chain_has_tls_transport(error) || has_tls_transport_fallback(error) {
        return DownloadError::TlsTransport;
    }
    if error.is_timeout() {
        return DownloadError::Connect;
    }
    if error.is_connect() {
        return if error_chain_has_ordinary_connect_failure(error) {
            DownloadError::Connect
        } else {
            DownloadError::TlsTransport
        };
    }
    DownloadError::InvalidResponse
}

fn error_chain_contains_invalid_certificate(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(rustls_error) = source.downcast_ref::<rustls::Error>()
            && matches!(rustls_error, rustls::Error::InvalidCertificate(_))
        {
            return true;
        }
        current = source.source();
    }
    false
}

fn error_chain_has_tls_transport(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source.downcast_ref::<rustls::Error>().is_some() {
            return true;
        }
        if let Some(io_error) = source.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            )
        {
            return true;
        }
        current = source.source();
    }
    false
}

fn error_chain_has_ordinary_connect_failure(error: &(dyn StdError + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(io_error) = source.downcast_ref::<std::io::Error>()
            && matches!(
                io_error.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::AddrNotAvailable
                    | std::io::ErrorKind::NetworkUnreachable
                    | std::io::ErrorKind::HostUnreachable
                    | std::io::ErrorKind::TimedOut
            )
        {
            return true;
        }
        current = source.source();
    }
    false
}

// Reqwest sometimes erases the underlying rustls error type. These fallbacks
// are intentionally narrow and are never included in surfaced diagnostics.
fn has_certificate_fallback(error: &reqwest::Error) -> bool {
    error_chain_contains_text(error, &["certificate", "hostname"])
}

fn has_tls_transport_fallback(error: &reqwest::Error) -> bool {
    error_chain_contains_text(
        error,
        &[
            "tls",
            "handshake",
            "unexpected eof",
            "connection reset",
            "connection aborted",
            "broken pipe",
        ],
    )
}

fn error_chain_contains_text(error: &(dyn StdError + 'static), needles: &[&str]) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        let message = source.to_string().to_ascii_lowercase();
        if needles.iter().any(|needle| message.contains(needle)) {
            return true;
        }
        current = source.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use futures_util::StreamExt;
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::PrivateKeyDer;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_rustls::TlsAcceptor;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::*;
    use crate::import::ImportConfig;

    const SOURCE_HOST: &str = "downloads.example.test";

    struct SequenceResolver {
        answers: Mutex<VecDeque<Result<Vec<SocketAddr>, DownloadError>>>,
        calls: AtomicUsize,
        requests: Mutex<Vec<(String, u16)>>,
    }

    struct BlockingResolver {
        started: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ImportResolver for BlockingResolver {
        async fn resolve(&self, _host: &str, _port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    impl SequenceResolver {
        fn new(answers: impl IntoIterator<Item = Result<Vec<SocketAddr>, DownloadError>>) -> Self {
            Self {
                answers: Mutex::new(answers.into_iter().collect()),
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl ImportResolver for SequenceResolver {
        async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push((host.to_owned(), port));
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Err(DownloadError::Dns))
        }
    }

    struct PermitAllAddresses;

    impl AddressPolicy for PermitAllAddresses {
        fn validate(&self, _: &[SocketAddr]) -> Result<(), DownloadError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingTransport {
        calls: AtomicUsize,
        sources: Mutex<Vec<AuthorizedSource>>,
        limits: Mutex<Vec<DownloadLimits>>,
    }

    #[async_trait::async_trait]
    impl ImportHttpTransport for RecordingTransport {
        async fn open(
            &self,
            source: AuthorizedSource,
            limits: DownloadLimits,
            _: tokio::sync::watch::Sender<u64>,
            _: CancellationToken,
        ) -> Result<DownloadStream, DownloadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.sources.lock().unwrap().push(source);
            self.limits.lock().unwrap().push(limits);
            Ok(empty_download_stream())
        }
    }

    fn empty_download_stream() -> DownloadStream {
        DownloadStream {
            body: Box::pin(futures_util::stream::empty()),
            total: None,
            content_type: None,
        }
    }

    fn public_address(last: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, last)), 443)
    }

    fn limits(max_bytes: u64) -> DownloadLimits {
        DownloadLimits {
            connect_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_millis(100),
            max_bytes,
        }
    }

    fn validated_config(origins: &[&str], max_bytes: u64) -> Arc<ValidatedImportConfig> {
        let config = ImportConfig {
            allowed_https_origins: origins.iter().map(|origin| (*origin).to_owned()).collect(),
            connect_timeout_secs: 1,
            idle_timeout_secs: 1,
            max_download_bytes: max_bytes,
            ..ImportConfig::default()
        };
        Arc::new(config.validate().unwrap())
    }

    fn fake_downloader(
        config: Arc<ValidatedImportConfig>,
        resolver: Arc<dyn ImportResolver>,
        policy: Arc<dyn AddressPolicy>,
        transport: Arc<dyn ImportHttpTransport>,
    ) -> SourceDownloader {
        SourceDownloader::with_components(config, resolver, policy, transport)
    }

    fn source(path_and_query: &str) -> Url {
        Url::parse(&format!("https://{SOURCE_HOST}{path_and_query}")).unwrap()
    }

    #[tokio::test]
    async fn open_cancels_while_dns_authorization_is_blocked() {
        let resolver = Arc::new(BlockingResolver {
            started: tokio::sync::Notify::new(),
        });
        let downloader = fake_downloader(
            validated_config(&["https://downloads.example.test"], 100),
            resolver.clone(),
            Arc::new(PermitAllAddresses),
            Arc::new(RecordingTransport::default()),
        );
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let open = tokio::spawn(async move {
            let (progress, _) = tokio::sync::watch::channel(0);
            downloader
                .open(&source("/object"), progress, task_cancel)
                .await
        });
        resolver.started.notified().await;
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_millis(250), open)
            .await
            .expect("DNS authorization must be cancellation-aware")
            .unwrap()
            .expect_err("canceled DNS authorization must fail");
        assert!(matches!(error, DownloadError::Canceled));
    }

    #[tokio::test]
    async fn allows_exact_https_origin_with_default_port_and_signed_query() {
        let resolver = Arc::new(SequenceResolver::new([Ok(vec![public_address(8)])]));
        let downloader = fake_downloader(
            validated_config(&["https://downloads.example.test"], 100),
            resolver.clone(),
            Arc::new(StrictPublicAddressPolicy),
            Arc::new(RecordingTransport::default()),
        );

        let authorized = downloader
            .authorize(&source(":443/download?signature=private-token"))
            .await
            .unwrap();

        assert_eq!(authorized.server_name, SOURCE_HOST);
        assert_eq!(authorized.addresses, vec![public_address(8)]);
        assert_eq!(
            resolver.requests.lock().unwrap().as_slice(),
            &[(SOURCE_HOST.to_owned(), 443)]
        );
        let debug = format!("{authorized:?}");
        assert!(!debug.contains("private-token"));
        assert!(!debug.contains(SOURCE_HOST));
        assert!(!debug.contains("8.8.8.8"));
    }

    #[tokio::test]
    async fn rejects_non_https_ip_userinfo_fragment_and_wrong_origin_before_dns() {
        let resolver = Arc::new(SequenceResolver::new([]));
        let downloader = fake_downloader(
            validated_config(&["https://downloads.example.test"], 100),
            resolver.clone(),
            Arc::new(StrictPublicAddressPolicy),
            Arc::new(RecordingTransport::default()),
        );

        for value in [
            "http://downloads.example.test/object",
            "https://8.8.8.8/object",
            "https://user@downloads.example.test/object",
            "https://downloads.example.test/object#fragment",
            "https://other.example.test/object",
        ] {
            let error = downloader
                .authorize(&Url::parse(value).unwrap())
                .await
                .unwrap_err();
            assert!(matches!(error, DownloadError::NotAllowed), "{value}");
        }
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dns_failure_empty_answers_and_forbidden_answer_sets_are_rejected() {
        let config = validated_config(&["https://downloads.example.test"], 100);
        let source = source("/object");

        for answers in [Err(DownloadError::Connect), Ok(Vec::new())] {
            let downloader = fake_downloader(
                config.clone(),
                Arc::new(SequenceResolver::new([answers])),
                Arc::new(StrictPublicAddressPolicy),
                Arc::new(RecordingTransport::default()),
            );
            assert!(matches!(
                downloader.authorize(&source).await,
                Err(DownloadError::Dns)
            ));
        }

        let transport = Arc::new(RecordingTransport::default());
        let downloader = fake_downloader(
            config,
            Arc::new(SequenceResolver::new([Ok(vec![
                public_address(8),
                SocketAddr::from(([127, 0, 0, 1], 443)),
            ])])),
            Arc::new(StrictPublicAddressPolicy),
            transport.clone(),
        );
        let (progress, _) = tokio::sync::watch::channel(0);
        assert!(matches!(
            downloader
                .open(&source, progress, CancellationToken::new())
                .await,
            Err(DownloadError::NotAllowed)
        ));
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn strict_policy_rejects_complete_forbidden_address_matrix() {
        let policy = StrictPublicAddressPolicy;
        let forbidden = [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "192.0.0.8",
            "192.0.2.1",
            "192.31.196.1",
            "192.52.193.1",
            "192.88.99.1",
            "192.168.0.1",
            "192.175.48.1",
            "198.18.0.1",
            "198.19.255.254",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
            "fe80::1",
            "ff00::1",
            "2001:2::1",
            "2001:10::1",
            "2001:20::1",
            "2001:db8::1",
            "2002::1",
        ];
        for value in forbidden {
            let address = SocketAddr::new(value.parse().unwrap(), 443);
            assert!(
                matches!(policy.validate(&[address]), Err(DownloadError::NotAllowed)),
                "{value}"
            );
        }

        for value in ["8.8.8.8", "1.1.1.1", "2001:4860:4860::8888"] {
            let address = SocketAddr::new(value.parse().unwrap(), 443);
            assert!(policy.validate(&[address]).is_ok(), "{value}");
        }
    }

    #[tokio::test]
    async fn dns_rebinding_cannot_escape_validated_addresses() {
        let resolver = Arc::new(SequenceResolver::new([
            Ok(vec![public_address(1)]),
            Ok(vec![public_address(2)]),
        ]));
        let transport = Arc::new(RecordingTransport::default());
        let downloader = fake_downloader(
            validated_config(&["https://downloads.example.test"], 100),
            resolver.clone(),
            Arc::new(StrictPublicAddressPolicy),
            transport.clone(),
        );
        let url = source("/download?signature=private-token");

        for _ in 0..2 {
            let (progress, _) = tokio::sync::watch::channel(0);
            downloader
                .open(&url, progress, CancellationToken::new())
                .await
                .unwrap();
        }

        assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
        let sources = transport.sources.lock().unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].addresses, vec![public_address(1)]);
        assert_eq!(sources[1].addresses, vec![public_address(2)]);
        assert!(format!("{:?}", sources[0]).contains("redacted"));
    }

    #[test]
    fn import_failure_mapping_is_stable_redacted_and_retryable_only_when_required() {
        for (error, code, retryable, message) in [
            (
                DownloadError::NotAllowed,
                ImportFailureCode::SourceUnreachable,
                false,
                "source is not allowed",
            ),
            (
                DownloadError::Dns,
                ImportFailureCode::SourceUnreachable,
                true,
                "source could not be reached",
            ),
            (
                DownloadError::Connect,
                ImportFailureCode::SourceUnreachable,
                true,
                "source could not be reached",
            ),
            (
                DownloadError::TlsTransport,
                ImportFailureCode::SourceUnreachable,
                true,
                "source could not be reached",
            ),
            (
                DownloadError::TlsCertificate,
                ImportFailureCode::SourceUnreachable,
                false,
                "source TLS certificate validation failed",
            ),
            (
                DownloadError::Redirect,
                ImportFailureCode::SourceRedirected,
                false,
                "source redirected",
            ),
            (
                DownloadError::HttpStatus(404),
                ImportFailureCode::SourceHttpError,
                false,
                "source returned an HTTP error",
            ),
            (
                DownloadError::HttpStatus(503),
                ImportFailureCode::SourceHttpError,
                true,
                "source returned a server error",
            ),
            (
                DownloadError::TooLarge,
                ImportFailureCode::SourceTooLarge,
                false,
                "source exceeded byte limit",
            ),
            (
                DownloadError::Stalled,
                ImportFailureCode::SourceStalled,
                true,
                "source stalled",
            ),
            (
                DownloadError::Canceled,
                ImportFailureCode::SourceUnreachable,
                true,
                "source transfer was canceled",
            ),
            (
                DownloadError::InvalidResponse,
                ImportFailureCode::SourceUnreachable,
                false,
                "source response was invalid",
            ),
        ] {
            let failure = error.into_import_failure();
            assert_eq!(failure.code, code);
            assert_eq!(failure.retryable, retryable);
            assert_eq!(failure.message, message);
            assert!(!failure.message.contains("private-token"));
            assert!(!failure.message.contains(SOURCE_HOST));
        }
    }

    #[tokio::test]
    async fn bounded_stream_enforces_progress_limits_idle_and_cancellation() {
        let (progress, observed) = tokio::sync::watch::channel(0);
        let mut body = bounded_body(
            futures_util::stream::iter(vec![
                Ok(Bytes::from_static(b"abc")),
                Ok(Bytes::from_static(b"def")),
            ]),
            limits(5),
            progress,
            CancellationToken::new(),
        );
        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"abc")
        );
        assert_eq!(*observed.borrow(), 3);
        assert!(matches!(
            body.next().await.unwrap(),
            Err(DownloadError::TooLarge)
        ));

        let (progress, _) = tokio::sync::watch::channel(0);
        let mut idle = bounded_body(
            futures_util::stream::pending::<Result<Bytes, DownloadError>>(),
            DownloadLimits {
                idle_timeout: Duration::from_millis(10),
                ..limits(10)
            },
            progress,
            CancellationToken::new(),
        );
        assert!(matches!(
            idle.next().await.unwrap(),
            Err(DownloadError::Stalled)
        ));

        let canceled = CancellationToken::new();
        canceled.cancel();
        let (progress, _) = tokio::sync::watch::channel(0);
        let mut canceled_body = bounded_body(
            futures_util::stream::pending::<Result<Bytes, DownloadError>>(),
            limits(10),
            progress,
            canceled,
        );
        assert!(matches!(
            canceled_body.next().await.unwrap(),
            Err(DownloadError::Canceled)
        ));
    }

    #[test]
    fn response_metadata_is_validated_without_synthesizing_unknown_length() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_LENGTH, "5".parse().unwrap());
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "text/plain; charset=utf-8".parse().unwrap(),
        );
        assert_eq!(validated_content_length(&headers).unwrap(), Some(5));
        assert_eq!(
            safe_content_type(&headers).as_deref(),
            Some("text/plain; charset=utf-8")
        );

        headers.remove(reqwest::header::CONTENT_LENGTH);
        assert_eq!(validated_content_length(&headers).unwrap(), None);
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            "not-a-number".parse().unwrap(),
        );
        assert!(matches!(
            validated_content_length(&headers),
            Err(DownloadError::InvalidResponse)
        ));

        headers.remove(reqwest::header::CONTENT_LENGTH);
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "a".repeat(MAX_CONTENT_TYPE_BYTES + 1).parse().unwrap(),
        );
        assert_eq!(safe_content_type(&headers), None);
    }

    fn test_server_material(server_name: &str) -> (TlsAcceptor, reqwest::Certificate) {
        install_rustls_crypto_provider();
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "IPFS S3 downloader test CA");
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca_certificate = ca_params.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca_params, ca_key);

        let mut server_params = CertificateParams::new(vec![server_name.to_owned()]).unwrap();
        server_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate().unwrap();
        let server_certificate = server_params.signed_by(&server_key, &issuer).unwrap();
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![server_certificate.der().clone()],
                PrivateKeyDer::Pkcs8(server_key.serialize_der().into()),
            )
            .unwrap();
        let root = reqwest::Certificate::from_der(ca_certificate.der().as_ref()).unwrap();
        (TlsAcceptor::from(Arc::new(server_config)), root)
    }

    async fn tls_response_server(
        response: Vec<u8>,
    ) -> (
        SocketAddr,
        reqwest::Certificate,
        tokio::task::JoinHandle<(String, Vec<u8>)>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (acceptor, root) = test_server_material(SOURCE_HOST);
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            let server_name = tls
                .get_ref()
                .1
                .server_name()
                .map(ToOwned::to_owned)
                .unwrap_or_default();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                tls.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            tls.write_all(&response).await.unwrap();
            tls.flush().await.unwrap();
            (server_name, request)
        });
        (address, root, task)
    }

    async fn tls_stalled_body_server() -> (
        SocketAddr,
        reqwest::Certificate,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (acceptor, root) = test_server_material(SOURCE_HOST);
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                if tls.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let _ = tls
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
                .await;
            let _ = tls.flush().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        (address, root, task)
    }

    async fn tls_header_only_server() -> (
        SocketAddr,
        reqwest::Certificate,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (acceptor, root) = test_server_material(SOURCE_HOST);
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                if tls.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let _ = tls
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n")
                .await;
            let _ = tls.flush().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        (address, root, task)
    }

    fn authorized_tls_source(address: SocketAddr, path_and_query: &str) -> AuthorizedSource {
        AuthorizedSource {
            url: tls_source(SOURCE_HOST, address, path_and_query),
            server_name: SOURCE_HOST.to_owned(),
            addresses: vec![address],
        }
    }

    fn tls_downloader(
        address: SocketAddr,
        root: reqwest::Certificate,
        hostname: &str,
        download_limits: DownloadLimits,
    ) -> SourceDownloader {
        let allowed_origin = format!("https://{hostname}:{}", address.port());
        fake_downloader(
            validated_config(&[&allowed_origin], download_limits.max_bytes),
            Arc::new(SequenceResolver::new([Ok(vec![address])])),
            Arc::new(PermitAllAddresses),
            Arc::new(ReqwestImportHttpTransport::new(download_limits, vec![root])),
        )
    }

    fn tls_source(hostname: &str, address: SocketAddr, path_and_query: &str) -> Url {
        Url::parse(&format!(
            "https://{hostname}:{}{path_and_query}",
            address.port()
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn pinned_address_transport_preserves_host_sni_tls_and_strips_client_credentials() {
        let (address, root, server) = tls_response_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Type: text/plain\r\n\r\nhello"
                .to_vec(),
        )
        .await;
        let downloader = tls_downloader(address, root, SOURCE_HOST, limits(10));
        let (progress, _) = tokio::sync::watch::channel(0);
        let mut stream = downloader
            .open(
                &tls_source(SOURCE_HOST, address, "/signed?Authorization=private-token"),
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(stream.total, Some(5));
        assert_eq!(stream.content_type.as_deref(), Some("text/plain"));
        let body = stream.body.next().await.unwrap().unwrap();
        assert_eq!(body, Bytes::from_static(b"hello"));
        assert!(stream.body.next().await.is_none());

        let (sni, request) = server.await.unwrap();
        let request = String::from_utf8(request).unwrap();
        assert_eq!(sni, SOURCE_HOST);
        assert!(request.starts_with("GET /signed?Authorization=private-token HTTP/1.1\r\n"));
        let normalized_headers = request.to_ascii_lowercase();
        assert!(normalized_headers.contains(&format!(
            "host: downloads.example.test:{}\r\n",
            address.port()
        )));
        assert!(!normalized_headers.contains("cookie:"));
        assert!(!normalized_headers.contains("authorization:"));
    }

    #[tokio::test]
    async fn wrong_hostname_certificate_is_terminal_tls_certificate() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (acceptor, root) = test_server_material(SOURCE_HOST);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(socket).await;
        });
        let downloader = tls_downloader(address, root, "wrong.example.test", limits(10));
        let (progress, _) = tokio::sync::watch::channel(0);
        let error = downloader
            .open(
                &tls_source("wrong.example.test", address, "/object"),
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DownloadError::TlsCertificate));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_handshake_interruption_is_retryable_tls_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        let (_, root) = test_server_material(SOURCE_HOST);
        let downloader = tls_downloader(address, root, SOURCE_HOST, limits(10));
        let (progress, _) = tokio::sync::watch::channel(0);
        let error = downloader
            .open(
                &tls_source(SOURCE_HOST, address, "/object"),
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DownloadError::TlsTransport));
        assert!(error.into_import_failure().retryable);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn redirects_and_oversized_content_length_are_rejected_before_the_body() {
        for (response, expected) in [
            (
                b"HTTP/1.1 302 Found\r\nLocation: https://elsewhere.example/\r\nContent-Length: 0\r\n\r\n".to_vec(),
                DownloadError::Redirect,
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n".to_vec(),
                DownloadError::TooLarge,
            ),
        ] {
            let (address, root, server) = tls_response_server(response).await;
            let downloader = tls_downloader(
                address,
                root,
                SOURCE_HOST,
                limits(10),
            );
            let (progress, _) = tokio::sync::watch::channel(0);
            let error = downloader
                .open(&tls_source(SOURCE_HOST, address, "/object"), progress, CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.to_string(), expected.to_string());
            let _ = server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn http_statuses_are_typed_without_exposing_response_bodies() {
        for (status, expected_retryable) in [(404, false), (503, true)] {
            let (address, root, server) = tls_response_server(
                format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: 22\r\n\r\nprivate response body"
                )
                .into_bytes(),
            )
            .await;
            let downloader = tls_downloader(address, root, SOURCE_HOST, limits(100));
            let (progress, _) = tokio::sync::watch::channel(0);
            let error = downloader
                .open(
                    &tls_source(SOURCE_HOST, address, "/object"),
                    progress,
                    CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, DownloadError::HttpStatus(code) if code == status));
            assert!(!error.to_string().contains("private response body"));
            assert_eq!(error.into_import_failure().retryable, expected_retryable);
            let _ = server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn unknown_content_length_remains_none() {
        let (address, root, server) = tls_response_server(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"
                .to_vec(),
        )
        .await;
        let downloader = tls_downloader(address, root, SOURCE_HOST, limits(10));
        let (progress, _) = tokio::sync::watch::channel(0);
        let mut stream = downloader
            .open(
                &tls_source(SOURCE_HOST, address, "/object"),
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(stream.total, None);
        assert_eq!(
            stream.body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"hello")
        );
        let _ = server.await.unwrap();
    }

    #[tokio::test]
    async fn production_transport_bounds_inter_chunk_idle_and_honors_cancellation() {
        let (address, root, server) = tls_stalled_body_server().await;
        let transport = ReqwestImportHttpTransport::new(limits(10), vec![root]);
        let (progress, _) = tokio::sync::watch::channel(0);
        let mut stream = transport
            .open(
                authorized_tls_source(address, "/object"),
                DownloadLimits {
                    idle_timeout: Duration::from_millis(10),
                    ..limits(10)
                },
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            stream.body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
        assert!(matches!(
            stream.body.next().await.unwrap(),
            Err(DownloadError::Stalled)
        ));
        server.await.unwrap();

        let (address, root, server) = tls_header_only_server().await;
        let transport = ReqwestImportHttpTransport::new(limits(10), vec![root]);
        let canceled = CancellationToken::new();
        let (progress, _) = tokio::sync::watch::channel(0);
        let mut stream = transport
            .open(
                authorized_tls_source(address, "/object"),
                limits(10),
                progress,
                canceled.clone(),
            )
            .await
            .unwrap();
        canceled.cancel();
        assert!(matches!(
            stream.body.next().await.unwrap(),
            Err(DownloadError::Canceled)
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn production_transport_bounds_connection_setup() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let (_, root) = test_server_material(SOURCE_HOST);
        let transport = ReqwestImportHttpTransport::new(
            DownloadLimits {
                connect_timeout: Duration::from_millis(10),
                ..limits(10)
            },
            vec![root],
        );
        let (progress, _) = tokio::sync::watch::channel(0);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            transport.open(
                authorized_tls_source(address, "/object"),
                DownloadLimits {
                    connect_timeout: Duration::from_millis(10),
                    ..limits(10)
                },
                progress,
                CancellationToken::new(),
            ),
        )
        .await
        .expect("TLS setup must honor the connection timeout")
        .unwrap_err();
        assert!(matches!(error, DownloadError::Connect));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ordinary_connection_refusal_is_typed_as_connect() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let (_, root) = test_server_material(SOURCE_HOST);
        let transport = ReqwestImportHttpTransport::new(limits(10), vec![root]);
        let (progress, _) = tokio::sync::watch::channel(0);
        let error = transport
            .open(
                authorized_tls_source(address, "/object"),
                limits(10),
                progress,
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, DownloadError::Connect));
    }
}
