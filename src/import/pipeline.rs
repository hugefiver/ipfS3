use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode, ImportPhase, ValidatedImportConfig,
        decompress::decompress_import_with_context,
        downloader::SourceDownloader,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        progress::ProgressReporter,
        publication::publish_direct,
        source::{execute_cid, execute_url},
        worker::{ImportWorkerHandle, start_worker},
    },
    state::AppState,
    store::{entities::import_job, import::ownership},
};

/// Owns immutable import configuration and the hardened HTTPS downloader.
pub struct ImportCoordinator {
    config: Arc<ValidatedImportConfig>,
    downloader: Arc<SourceDownloader>,
    execution_observer: Arc<dyn ImportExecutionObserver>,
}

/// Testability hook for observing import lifecycle boundaries.
#[doc(hidden)]
#[async_trait::async_trait]
pub trait ImportExecutionObserver: Send + Sync {
    async fn before_publication(&self, job_id: &str);
}

pub(crate) struct NoopImportExecutionObserver;

#[async_trait::async_trait]
impl ImportExecutionObserver for NoopImportExecutionObserver {
    async fn before_publication(&self, _job_id: &str) {}
}

impl ImportCoordinator {
    pub fn new(config: ValidatedImportConfig, downloader: SourceDownloader) -> Arc<Self> {
        Self::new_with_observer(config, downloader, Arc::new(NoopImportExecutionObserver))
    }

    /// Constructs a coordinator with a lifecycle observer for integration testing.
    #[doc(hidden)]
    pub fn new_with_observer(
        config: ValidatedImportConfig,
        downloader: SourceDownloader,
        execution_observer: Arc<dyn ImportExecutionObserver>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            downloader: Arc::new(downloader),
            execution_observer,
        })
    }

    pub fn enabled(&self) -> bool {
        self.config.raw.enabled
    }

    pub async fn authorize_url_for_submission(&self, source: &Url) -> Result<(), AppError> {
        self.downloader
            .authorize(source)
            .await
            .map(|_| ())
            .map_err(|_| AppError::ImportUrlDenied)
    }

    pub fn start(
        self: &Arc<Self>,
        state: Arc<AppState>,
        shutdown: CancellationToken,
    ) -> ImportWorkerHandle {
        start_worker(self.clone(), state, shutdown)
    }

    pub(crate) fn config(&self) -> &ValidatedImportConfig {
        &self.config
    }

    pub(crate) fn downloader(&self) -> &SourceDownloader {
        &self.downloader
    }

    pub(crate) fn execution_observer(&self) -> &dyn ImportExecutionObserver {
        self.execution_observer.as_ref()
    }
}

#[derive(Clone)]
pub struct JobCancellation {
    pub shutdown: CancellationToken,
    pub ownership_lost: CancellationToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportArtifact {
    pub cid: String,
    pub logical_size: u64,
    pub object_content_type: Option<String>,
}

pub(crate) struct AbortOnDropTask(Option<tokio::task::JoinHandle<()>>);

impl AbortOnDropTask {
    pub(crate) fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    pub(crate) async fn join(mut self) {
        if let Some(task) = self.0.take() {
            let _ = task.await;
        }
    }
}

impl Drop for AbortOnDropTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Executes one already-claimed import and atomically publishes its direct object.
pub async fn execute_job(
    coordinator: Arc<ImportCoordinator>,
    state: Arc<AppState>,
    job: import_job::Model,
    claim: ImportClaim,
    cancellation: JobCancellation,
) -> Result<ImportArtifact, ImportExecutionError> {
    let network_cancel = CancellationToken::new();
    let relay_cancel = CancellationToken::new();
    let relay = AbortOnDropTask::new(tokio::spawn(relay_cancellation(
        cancellation.clone(),
        network_cancel.clone(),
        relay_cancel.clone(),
    )));
    let reporter = ProgressReporter::start(
        state.clone(),
        &job,
        claim.clone(),
        cancellation.clone(),
        Duration::from_millis(coordinator.config.raw.progress_flush_interval_ms),
    );

    let result = execute_job_inner(
        &coordinator,
        &state,
        &job,
        &claim,
        &cancellation,
        &network_cancel,
        &reporter,
    )
    .await;
    let reporter_result = reporter.finish(&cancellation).await;
    relay_cancel.cancel();
    network_cancel.cancel();
    relay.join().await;

    match (result, reporter_result) {
        (Ok(artifact), Ok(())) => Ok(artifact),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_job_inner(
    coordinator: &Arc<ImportCoordinator>,
    state: &Arc<AppState>,
    job: &import_job::Model,
    claim: &ImportClaim,
    cancellation: &JobCancellation,
    network_cancel: &CancellationToken,
    reporter: &ProgressReporter,
) -> Result<ImportArtifact, ImportExecutionError> {
    ensure_active(cancellation)?;
    if job.decompress_prefix.is_some() {
        ownership::reset_extracted_targets_for_attempt(
            state.store.db(),
            claim,
            &job.bucket,
            chrono::Utc::now(),
        )
        .await
        .map_err(|error| map_publication_error(error, cancellation))?;
        ensure_active(cancellation)?;
    }
    let mut artifact = match job.source_type.as_str() {
        "cid" => {
            execute_cid(
                coordinator,
                state,
                job,
                cancellation,
                network_cancel,
                reporter,
            )
            .await?
        }
        "url" => {
            execute_url(
                coordinator,
                state,
                job,
                cancellation,
                network_cancel,
                reporter,
            )
            .await?
        }
        _ => {
            return Err(terminal_failure(
                ImportFailureCode::SourceUnreachable,
                "persisted import source is invalid",
            ));
        }
    };
    if job.object_content_type.is_some() {
        artifact.object_content_type = job.object_content_type.clone();
    }

    if job.decompress_prefix.is_some() {
        decompress_import_with_context(
            state,
            job,
            artifact.clone(),
            claim,
            cancellation,
            reporter,
            crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES,
            coordinator.execution_observer(),
        )
        .await?;
        return Ok(artifact);
    }

    reporter
        .phase(ImportPhase::Publishing, cancellation)
        .await?;
    publish_direct(state, job, claim, &artifact, cancellation).await?;
    Ok(artifact)
}

async fn relay_cancellation(
    cancellation: JobCancellation,
    network_cancel: CancellationToken,
    relay_cancel: CancellationToken,
) {
    tokio::select! {
        _ = cancellation.shutdown.cancelled() => network_cancel.cancel(),
        _ = cancellation.ownership_lost.cancelled() => network_cancel.cancel(),
        _ = relay_cancel.cancelled() => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use chrono::Utc;
    use futures_util::stream;
    use sea_orm::{ColumnTrait, Database, EntityTrait, PaginatorTrait, QueryFilter};
    use tokio::sync::{oneshot, watch};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use crate::{
        import::{
            ImportConfig, ImportFailure, ImportProgress, ImportSource,
            downloader::{
                AddressPolicy, AuthorizedSource, DownloadError, DownloadLimits, DownloadStream,
                ImportHttpTransport, ImportResolver,
            },
            execution_error::{map_download_error, map_stream_add_error},
            progress::{ProgressReporter, apply_kubo_progress},
        },
        kubo::{KuboProgress, add::StreamAddError},
        store::{
            Store,
            entities::{
                import_destination, import_job, import_job_result, import_job_target,
                import_prefix_claim, object,
            },
            import::{jobs, jobs::NewImportJob, ownership},
        },
    };

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

    fn cancellation() -> JobCancellation {
        JobCancellation {
            shutdown: CancellationToken::new(),
            ownership_lost: CancellationToken::new(),
        }
    }

    async fn test_state(kubo_uri: String) -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo_uri),
            cold_kubo: None,
            store: Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    fn import_config() -> ValidatedImportConfig {
        ImportConfig {
            allowed_https_origins: vec!["https://example.com".to_owned()],
            progress_flush_interval_ms: 10,
            ..ImportConfig::default()
        }
        .validate()
        .unwrap()
    }

    async fn submit_and_claim(
        state: &AppState,
        id: &str,
        source: ImportSource,
    ) -> (import_job::Model, ImportClaim) {
        submit_and_claim_with_prefix(state, id, source, None).await
    }

    async fn submit_and_claim_with_prefix(
        state: &AppState,
        id: &str,
        source: ImportSource,
        decompress_prefix: Option<&str>,
    ) -> (import_job::Model, ImportClaim) {
        let now = Utc::now();
        let request = NewImportJob {
            id: id.to_owned(),
            bucket: "bucket".to_owned(),
            key: format!("key-{id}"),
            source,
            request_fingerprint: format!("fingerprint-{id}"),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::from([("fixture".to_owned(), "true".to_owned())]),
            tags: Vec::new(),
            decompress_prefix: decompress_prefix.map(str::to_owned),
        };
        ownership::submit(state.store.db(), request, now)
            .await
            .unwrap();
        let mut claimed = jobs::claim_due(
            state.store.db(),
            "test-worker",
            now,
            now + chrono::TimeDelta::seconds(60),
            1,
        )
        .await
        .unwrap();
        let claimed = claimed.pop().unwrap();
        (claimed.job, claimed.claim)
    }

    struct CountingResolver(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl ImportResolver for CountingResolver {
        async fn resolve(&self, _host: &str, port: u16) -> Result<Vec<SocketAddr>, DownloadError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                port,
            )])
        }
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

    struct AllowAddresses;

    impl AddressPolicy for AllowAddresses {
        fn validate(&self, _addresses: &[SocketAddr]) -> Result<(), DownloadError> {
            Ok(())
        }
    }

    struct StaticTransport;

    #[async_trait::async_trait]
    impl ImportHttpTransport for StaticTransport {
        async fn open(
            &self,
            _source: AuthorizedSource,
            _limits: DownloadLimits,
            progress: watch::Sender<u64>,
            _cancel: CancellationToken,
        ) -> Result<DownloadStream, DownloadError> {
            progress.send(5).unwrap();
            Ok(DownloadStream {
                body: Box::pin(stream::iter(vec![Ok(Bytes::from_static(b"hello"))])),
                total: Some(5),
                content_type: Some("text/plain".to_owned()),
            })
        }
    }

    struct BlockedEofTransport {
        first_chunk_written: tokio::sync::Mutex<Option<oneshot::Sender<()>>>,
        release_eof: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    }

    #[async_trait::async_trait]
    impl ImportHttpTransport for BlockedEofTransport {
        async fn open(
            &self,
            _source: AuthorizedSource,
            _limits: DownloadLimits,
            progress: watch::Sender<u64>,
            _cancel: CancellationToken,
        ) -> Result<DownloadStream, DownloadError> {
            progress.send(5).unwrap();
            let first_chunk_written = self.first_chunk_written.lock().await.take();
            let release_eof = self.release_eof.lock().await.take().unwrap();
            let body = async_stream::stream! {
                yield Ok(Bytes::from_static(b"hello"));
                if let Some(first_chunk_written) = first_chunk_written {
                    let _ = first_chunk_written.send(());
                }
                let _ = release_eof.await;
            };
            Ok(DownloadStream {
                body: Box::pin(body),
                total: Some(5),
                content_type: Some("text/plain".to_owned()),
            })
        }
    }

    fn coordinator_with_downloader(
        config: ValidatedImportConfig,
        resolver: Arc<AtomicUsize>,
    ) -> Arc<ImportCoordinator> {
        let downloader = SourceDownloader::with_components(
            Arc::new(config.clone()),
            Arc::new(CountingResolver(resolver)),
            Arc::new(AllowAddresses),
            Arc::new(StaticTransport),
        );
        ImportCoordinator::new(config, downloader)
    }

    async fn mount_pin(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("progress", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Progress\":3,\"Bytes\":7}}\n{{\"Pins\":[\"{CID}\"]}}\n"
            )))
            .expect(1)
            .mount(server)
            .await;
    }

    fn stored_zip(name: &str, data: &[u8]) -> Vec<u8> {
        fn push_u16(out: &mut Vec<u8>, value: u16) {
            out.extend_from_slice(&value.to_le_bytes());
        }
        fn push_u32(out: &mut Vec<u8>, value: u32) {
            out.extend_from_slice(&value.to_le_bytes());
        }
        fn crc32(bytes: &[u8]) -> u32 {
            let mut crc = !0_u32;
            for &byte in bytes {
                crc ^= u32::from(byte);
                for _ in 0..8 {
                    crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
                }
            }
            !crc
        }
        let mut output = Vec::new();
        push_u32(&mut output, 0x0403_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc32(data));
        push_u32(&mut output, data.len() as u32);
        push_u32(&mut output, data.len() as u32);
        push_u16(&mut output, name.len() as u16);
        push_u16(&mut output, 0);
        output.extend_from_slice(name.as_bytes());
        output.extend_from_slice(data);
        let central_offset = output.len() as u32;
        push_u32(&mut output, 0x0201_4b50);
        push_u16(&mut output, 20);
        push_u16(&mut output, 20);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u32(&mut output, crc32(data));
        push_u32(&mut output, data.len() as u32);
        push_u32(&mut output, data.len() as u32);
        push_u16(&mut output, name.len() as u16);
        for _ in 0..4 {
            push_u16(&mut output, 0);
        }
        push_u32(&mut output, 0);
        push_u32(&mut output, 0);
        output.extend_from_slice(name.as_bytes());
        let central_size = output.len() as u32 - central_offset;
        push_u32(&mut output, 0x0605_4b50);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, 1);
        push_u16(&mut output, 1);
        push_u32(&mut output, central_size);
        push_u32(&mut output, central_offset);
        push_u16(&mut output, 0);
        output
    }

    #[tokio::test]
    async fn cid_pipeline_persists_provider_pin_inspect_progress_and_guarded_publication() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "{\"Type\":0,\"Responses\":null}\n{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n",
            ))
            .expect(1)
            .mount(&server)
            .await;
        mount_pin(&server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .expect(1)
            .mount(&server)
            .await;

        let state = test_state(server.uri()).await;
        let config = import_config();
        let coordinator = coordinator_with_downloader(config, Arc::new(AtomicUsize::new(0)));
        let (job, claim) = submit_and_claim(&state, "cid", ImportSource::Cid(CID.to_owned())).await;
        let artifact = execute_job(coordinator, state.clone(), job, claim, cancellation())
            .await
            .unwrap();

        assert_eq!(artifact.cid, CID);
        assert_eq!(artifact.logical_size, 5);
        assert_eq!(artifact.object_content_type, None);
        let persisted = import_job::Entity::find_by_id("cid")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, "completed");
        assert_eq!(persisted.phase, "publishing");
        assert_eq!(persisted.providers_observed, 1);
        assert_eq!(persisted.pin_nodes_processed, 3);
        assert_eq!(persisted.pin_bytes_processed, 7);
        assert_eq!(persisted.logical_size, Some(5));
        assert_eq!(persisted.final_cid.as_deref(), Some(CID));
        let published = object::Entity::find()
            .filter(object::Column::Bucket.eq("bucket"))
            .filter(object::Column::Key.eq("key-cid"))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert!(!published.encrypted);
        assert_eq!(published.cid, CID);
    }

    #[tokio::test]
    async fn combined_cid_pipeline_decompresses_then_atomically_publishes_without_direct_publish() {
        const ENTRY_CID: &str = "QmEntry";
        let archive = stored_zip("file.txt", b"hello");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        mount_pin(&server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive.clone()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{ENTRY_CID}\",\"Size\":\"5\"}}\n")),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", ENTRY_CID))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let state = test_state(server.uri()).await;
        let coordinator =
            coordinator_with_downloader(import_config(), Arc::new(AtomicUsize::new(0)));
        let (job, claim) = submit_and_claim_with_prefix(
            &state,
            "combined-cid",
            ImportSource::Cid(CID.to_owned()),
            Some("out/"),
        )
        .await;

        let artifact = execute_job(coordinator, state.clone(), job, claim, cancellation())
            .await
            .unwrap();

        assert_eq!(artifact.cid, CID);
        assert_eq!(artifact.logical_size, archive.len() as u64);
        let published = object::Entity::find()
            .filter(object::Column::Key.eq("key-combined-cid"))
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(published.len(), 1, "archive must publish exactly once");
        assert_eq!(
            object::Entity::find()
                .filter(object::Column::Key.eq("out/file.txt"))
                .count(state.store.db())
                .await
                .unwrap(),
            1
        );
        let completed = import_job::Entity::find_by_id("combined-cid")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, "completed");
        assert_eq!(completed.phase, "publishing");
        assert_eq!(completed.entries_processed, 1);
        assert_eq!(completed.entries_succeeded, 1);
        assert_eq!(completed.decompressed_bytes, 5);
        let rows = crate::store::entities::import_job_result::Entity::find()
            .filter(crate::store::entities::import_job_result::Column::JobId.eq("combined-cid"))
            .count(state.store.db())
            .await
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.url.path() == "/api/v0/add")
                .count(),
            1,
            "the only add must be the extracted entry, never the already pinned archive"
        );
    }

    #[tokio::test]
    async fn retried_combined_pipeline_resets_old_outputs_before_changed_archive_publication() {
        const ENTRY_CID: &str = "QmChangedEntry";
        let archive = stored_zip("b.txt", b"changed");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Type\":4,\"Responses\":[{\"ID\":\"provider-a\"}]}\n"),
            )
            .expect(1)
            .mount(&server)
            .await;
        mount_pin(&server).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive.clone()))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!("{{\"Hash\":\"{ENTRY_CID}\",\"Size\":\"7\"}}\n")),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .and(query_param("arg", ENTRY_CID))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let state = test_state(server.uri()).await;
        let coordinator =
            coordinator_with_downloader(import_config(), Arc::new(AtomicUsize::new(0)));
        let (first_job, first_claim) = submit_and_claim_with_prefix(
            &state,
            "changed-membership",
            ImportSource::Cid(CID.to_owned()),
            Some("out/"),
        )
        .await;
        let old_generation = ownership::claim_extracted_target(
            state.store.db(),
            &first_claim,
            "bucket",
            "out/a.txt",
            Utc::now(),
        )
        .await
        .unwrap();
        let retry_at = Utc::now();
        jobs::retry(
            state.store.db(),
            &first_job.id,
            &first_claim.worker_id,
            first_claim.claim_epoch,
            first_claim.attempt,
            retry_at,
            &ImportFailure {
                code: ImportFailureCode::SourceUnreachable,
                message: "retry changed archive".to_owned(),
                retryable: true,
            },
            retry_at,
        )
        .await
        .unwrap();
        let mut reclaimed = jobs::claim_due(
            state.store.db(),
            "retry-worker",
            retry_at,
            retry_at + chrono::TimeDelta::seconds(60),
            1,
        )
        .await
        .unwrap();
        let reclaimed = reclaimed.pop().unwrap();
        assert!(reclaimed.claim.claim_epoch > first_claim.claim_epoch);

        execute_job(
            coordinator,
            state.clone(),
            reclaimed.job,
            reclaimed.claim,
            cancellation(),
        )
        .await
        .expect("changed archive membership must publish on the new attempt");

        let old_destination =
            import_destination::Entity::find_by_id(("bucket".to_owned(), "out/a.txt".to_owned()))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(old_destination.generation, old_generation);
        assert_eq!(old_destination.owner_job_id, None);
        assert_eq!(
            object::Entity::find()
                .filter(object::Column::Key.eq("out/a.txt"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            import_job_result::Entity::find()
                .filter(import_job_result::Column::JobId.eq("changed-membership"))
                .filter(import_job_result::Column::Key.eq("out/a.txt"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            object::Entity::find()
                .filter(object::Column::Key.eq("out/b.txt"))
                .count(state.store.db())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            import_job_result::Entity::find()
                .filter(import_job_result::Column::JobId.eq("changed-membership"))
                .filter(import_job_result::Column::Key.eq("out/b.txt"))
                .count(state.store.db())
                .await
                .unwrap(),
            1
        );
        let completed = import_job::Entity::find_by_id("changed-membership")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.state, "completed");
        let archive_destination = import_destination::Entity::find_by_id((
            "bucket".to_owned(),
            "key-changed-membership".to_owned(),
        ))
        .one(state.store.db())
        .await
        .unwrap()
        .unwrap();
        assert_eq!(archive_destination.owner_job_id, None);
        let current_destination =
            import_destination::Entity::find_by_id(("bucket".to_owned(), "out/b.txt".to_owned()))
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(current_destination.owner_job_id, None);
        assert_eq!(
            import_job_target::Entity::find()
                .filter(import_job_target::Column::JobId.eq("changed-membership"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            import_prefix_claim::Entity::find()
                .filter(import_prefix_claim::Column::JobId.eq("changed-membership"))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn corrupt_persisted_cid_is_terminal_without_kubo_or_publication_activity() {
        let server = MockServer::start().await;
        let state = test_state(server.uri()).await;
        let coordinator =
            coordinator_with_downloader(import_config(), Arc::new(AtomicUsize::new(0)));
        let (mut job, claim) =
            submit_and_claim(&state, "corrupt-cid", ImportSource::Cid(CID.to_owned())).await;
        job.source_value = "not-a-cid-and-must-not-be-logged".to_owned();
        import_job::Entity::update_many()
            .col_expr(
                import_job::Column::SourceValue,
                sea_orm::sea_query::Expr::value(job.source_value.clone()),
            )
            .filter(import_job::Column::Id.eq(&job.id))
            .exec(state.store.db())
            .await
            .unwrap();

        let error = execute_job(coordinator, state.clone(), job, claim, cancellation())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ImportExecutionError::Terminal(ImportFailure {
                code: ImportFailureCode::CidNotFound,
                retryable: false,
                ..
            })
        ));
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn url_pipeline_reauthorizes_streams_adds_pins_and_preserves_safe_content_type() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "{{\"Bytes\":5}}\n{{\"Hash\":\"{CID}\",\"Size\":\"5\"}}\n"
            )))
            .expect(1)
            .mount(&server)
            .await;
        mount_pin(&server).await;

        let state = test_state(server.uri()).await;
        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let coordinator = coordinator_with_downloader(import_config(), resolver_calls.clone());
        let (job, claim) = submit_and_claim(
            &state,
            "url",
            ImportSource::Url(Url::parse("https://example.com/file").unwrap()),
        )
        .await;
        let artifact = execute_job(coordinator, state.clone(), job, claim, cancellation())
            .await
            .unwrap();

        assert_eq!(resolver_calls.load(Ordering::SeqCst), 1);
        assert_eq!(artifact.cid, CID);
        assert_eq!(artifact.logical_size, 5);
        assert_eq!(artifact.object_content_type.as_deref(), Some("text/plain"));
        let persisted = import_job::Entity::find_by_id("url")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, "completed");
        assert_eq!(persisted.phase, "publishing");
        assert_eq!(persisted.downloaded_bytes, 5);
        assert_eq!(persisted.download_total, Some(5));
        assert_eq!(persisted.ipfs_add_bytes, 5);
        assert_eq!(persisted.pin_nodes_processed, 3);
        assert_eq!(persisted.logical_size, Some(5));
        let published = object::Entity::find()
            .filter(object::Column::Key.eq("key-url"))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.content_type.as_deref(), Some("text/plain"));
        assert!(!published.encrypted);
    }

    #[tokio::test]
    async fn progress_is_coalesced_but_phase_changes_flush_immediately() {
        let state = test_state("http://127.0.0.1:1".to_owned()).await;
        let (job, claim) =
            submit_and_claim(&state, "progress", ImportSource::Cid(CID.to_owned())).await;
        let cancellation = cancellation();
        let reporter = ProgressReporter::start(
            state.clone(),
            &job,
            claim,
            cancellation.clone(),
            Duration::from_secs(60),
        );
        reporter
            .kubo
            .send(KuboProgress::ProviderObserved {
                peer_id: "provider-a".to_owned(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let before_phase = import_job::Entity::find_by_id("progress")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before_phase.providers_observed, 0);
        assert_eq!(before_phase.phase, "queued");

        reporter
            .phase(ImportPhase::DiscoveringProviders, &cancellation)
            .await
            .unwrap();
        let after_phase = import_job::Entity::find_by_id("progress")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_phase.providers_observed, 1);
        assert_eq!(after_phase.phase, "discovering_providers");
        reporter.finish(&cancellation).await.unwrap();
    }

    #[tokio::test]
    async fn ownership_loss_cancels_in_flight_pipeline_without_job_mutation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/routing/findprovs"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        let state = test_state(server.uri()).await;
        let coordinator =
            coordinator_with_downloader(import_config(), Arc::new(AtomicUsize::new(0)));
        let (job, claim) =
            submit_and_claim(&state, "ownership-lost", ImportSource::Cid(CID.to_owned())).await;
        let cancellation = cancellation();
        let ownership_lost = cancellation.ownership_lost.clone();
        let execution = tokio::spawn(execute_job(
            coordinator,
            state.clone(),
            job,
            claim,
            cancellation,
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        ownership_lost.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), execution)
            .await
            .expect("ownership cancellation must stop the pipeline")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, ImportExecutionError::Superseded));
        let row = import_job::Entity::find_by_id("ownership-lost")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "running");
        assert_eq!(row.failure_code, None);
        assert_eq!(row.completed_at, None);
    }

    #[tokio::test]
    async fn ownership_loss_cancels_url_pipeline_blocked_in_dns_authorization() {
        let server = MockServer::start().await;
        let state = test_state(server.uri()).await;
        let config = import_config();
        let resolver = Arc::new(BlockingResolver {
            started: tokio::sync::Notify::new(),
        });
        let downloader = SourceDownloader::with_components(
            Arc::new(config.clone()),
            resolver.clone(),
            Arc::new(AllowAddresses),
            Arc::new(StaticTransport),
        );
        let coordinator = ImportCoordinator::new(config, downloader);
        let (job, claim) = submit_and_claim(
            &state,
            "dns-ownership-lost",
            ImportSource::Url(Url::parse("https://example.com/file").unwrap()),
        )
        .await;
        let cancellation = cancellation();
        let ownership_lost = cancellation.ownership_lost.clone();
        let execution = tokio::spawn(execute_job(
            coordinator,
            state.clone(),
            job,
            claim,
            cancellation,
        ));
        resolver.started.notified().await;
        ownership_lost.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), execution)
            .await
            .expect("ownership loss must cancel blocked DNS")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, ImportExecutionError::Superseded));
        assert!(server.received_requests().await.unwrap().is_empty());
        let row = import_job::Entity::find_by_id("dns-ownership-lost")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, "running");
        assert_eq!(row.failure_code, None);
    }

    #[tokio::test]
    async fn url_phase_stays_downloading_until_stream_eof_is_observed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(150))
                    .set_body_string(format!(
                        "{{\"Bytes\":5}}\n{{\"Hash\":\"{CID}\",\"Size\":\"5\"}}\n"
                    )),
            )
            .expect(1)
            .mount(&server)
            .await;
        mount_pin(&server).await;

        let state = test_state(server.uri()).await;
        let config = import_config();
        let (first_chunk_written, first_chunk_observed) = oneshot::channel();
        let (release_eof, eof_released) = oneshot::channel();
        let downloader = SourceDownloader::with_components(
            Arc::new(config.clone()),
            Arc::new(CountingResolver(Arc::new(AtomicUsize::new(0)))),
            Arc::new(AllowAddresses),
            Arc::new(BlockedEofTransport {
                first_chunk_written: tokio::sync::Mutex::new(Some(first_chunk_written)),
                release_eof: tokio::sync::Mutex::new(Some(eof_released)),
            }),
        );
        let coordinator = ImportCoordinator::new(config, downloader);
        let (job, claim) = submit_and_claim(
            &state,
            "phase-eof",
            ImportSource::Url(Url::parse("https://example.com/file").unwrap()),
        )
        .await;
        let execution = tokio::spawn(execute_job(
            coordinator,
            state.clone(),
            job,
            claim,
            cancellation(),
        ));
        first_chunk_observed.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        let downloading = import_job::Entity::find_by_id("phase-eof")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(downloading.phase, "downloading");
        assert_eq!(downloading.downloaded_bytes, 5);

        release_eof.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let phase = import_job::Entity::find_by_id("phase-eof")
                    .one(state.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .phase;
                if phase == "adding_to_ipfs" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("source EOF must durably advance the phase");
        let artifact = execution.await.unwrap().unwrap();
        assert_eq!(artifact.logical_size, 5);
    }

    #[test]
    fn artifact_keeps_plain_import_metadata() {
        let artifact = ImportArtifact {
            cid: "bafy-source".to_owned(),
            logical_size: 7,
            object_content_type: Some("text/plain".to_owned()),
        };
        assert_eq!(artifact.cid, "bafy-source");
        assert_eq!(artifact.logical_size, 7);
        assert_eq!(artifact.object_content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn exact_mid_upload_download_failure_classification_survives_boundary() {
        let cancel = cancellation();
        let too_large =
            map_stream_add_error(StreamAddError::Source(DownloadError::TooLarge), &cancel);
        let stalled = map_stream_add_error(StreamAddError::Source(DownloadError::Stalled), &cancel);
        assert!(matches!(
            too_large,
            ImportExecutionError::Terminal(ImportFailure {
                code: ImportFailureCode::SourceTooLarge,
                ..
            })
        ));
        assert!(matches!(
            stalled,
            ImportExecutionError::Retryable(ImportFailure {
                code: ImportFailureCode::SourceStalled,
                ..
            })
        ));
    }

    #[test]
    fn tls_transport_retries_but_certificate_failure_is_terminal() {
        let cancel = cancellation();
        assert!(matches!(
            map_download_error(DownloadError::TlsTransport, &cancel),
            ImportExecutionError::Retryable(_)
        ));
        assert!(matches!(
            map_download_error(DownloadError::TlsCertificate, &cancel),
            ImportExecutionError::Terminal(_)
        ));
    }

    #[test]
    fn cancellation_boundary_distinguishes_shutdown_from_ownership_loss() {
        let shutdown = cancellation();
        shutdown.shutdown.cancel();
        assert!(matches!(
            map_download_error(DownloadError::Canceled, &shutdown),
            ImportExecutionError::Interrupted
        ));

        let ownership = cancellation();
        ownership.ownership_lost.cancel();
        assert!(matches!(
            map_stream_add_error(StreamAddError::Canceled, &ownership),
            ImportExecutionError::Superseded
        ));

        let unexplained = cancellation();
        assert!(matches!(
            map_download_error(DownloadError::Canceled, &unexplained),
            ImportExecutionError::Retryable(ImportFailure {
                code: ImportFailureCode::SourceUnreachable,
                ..
            })
        ));
    }

    #[test]
    fn progress_counts_observed_providers_and_separate_kubo_counters() {
        let mut progress = ImportProgress::default();
        let mut providers = 0;
        apply_kubo_progress(
            &mut progress,
            &mut providers,
            KuboProgress::ProviderObserved {
                peer_id: "p1".to_owned(),
            },
        );
        apply_kubo_progress(
            &mut progress,
            &mut providers,
            KuboProgress::AddBytes { bytes: 11 },
        );
        apply_kubo_progress(
            &mut progress,
            &mut providers,
            KuboProgress::PinProgress {
                nodes: 3,
                bytes: 19,
            },
        );
        assert_eq!(progress.providers_observed, 1);
        assert_eq!(progress.ipfs_add_bytes, 11);
        assert_eq!(progress.pin_nodes_processed, 3);
        assert_eq!(progress.pin_bytes_processed, 19);
    }
}
