//! Fake HTTPS IO behind the production public-address authorization boundary.
use std::{
    net::SocketAddr,
    sync::{
        RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use ipfs_s3_gateway::import::downloader::{
    AuthorizedSource, DownloadError, DownloadLimits, DownloadStream, ImportHttpTransport,
    ImportResolver,
};
use tokio_util::sync::CancellationToken;

pub const ORIGIN: &str = "https://intake.example.test";
pub const URL: &str = "https://intake.example.test/archive.zip";

pub struct Source {
    body: RwLock<Vec<u8>>,
    calls: AtomicUsize,
}

impl Source {
    pub fn new(body: &[u8]) -> Self {
        Self {
            body: RwLock::new(body.to_vec()),
            calls: AtomicUsize::new(0),
        }
    }

    pub fn replace(&self, body: Vec<u8>) {
        *self.body.write().unwrap() = body;
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl ImportResolver for Source {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
        if host != "intake.example.test" {
            return Err(DownloadError::Dns);
        }
        // StrictPublicAddressPolicy still checks this address. Transport below
        // does not connect to it or make any other external network request.
        Ok(vec![SocketAddr::from(([1, 1, 1, 1], port))])
    }
}

#[async_trait::async_trait]
impl ImportHttpTransport for Source {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        progress: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let body = self.body.read().unwrap().clone();
        let size = body.len() as u64;
        let stream = futures_util::stream::once(async move {
            progress.send_replace(size);
            Ok(Bytes::from(body))
        });
        Ok(DownloadStream {
            body: Box::pin(stream),
            total: Some(size),
            content_type: Some("application/zip".into()),
        })
    }
}
