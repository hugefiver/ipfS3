//! Import-only source, binding and immutable admission receipt regressions.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}

use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use futures_util::FutureExt;
use http::{HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    import::{
        ImportConfig,
        downloader::{
            AuthorizedSource, DownloadError, DownloadLimits, DownloadStream, ImportHttpTransport,
            SourceDownloader, StrictPublicAddressPolicy,
        },
        pipeline::ImportCoordinator,
        v2_source::{V2SourceError, fetch_v2_zip_source},
    },
    store::zip::{execution, import_intake},
    zip::extract::ZipExtractionLimits,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sha2::{Digest, Sha256};
use support::{
    stage9_zip_replay::{ARCHIVE, Entry, Outcome, run},
    stage9_zip_replay_requests as request,
    stage9_zip_replay_requests::Ledger,
    stage9_zip_replay_source::{ORIGIN, URL},
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

macro_rules! scenario {
    (|mut $h:ident| $body:block) => {
        scenario!(@owned [mut], $h, $body)
    };
    (|$h:ident| $body:block) => {
        scenario!(@owned [], $h, $body)
    };
    (@owned [$($mutable:tt)*], $h:ident, $body:block) => {
        run(Entry::Import, Outcome::Complete, |$($mutable)* $h| async move {
            let result = AssertUnwindSafe(async $body).catch_unwind().await;
            ($h, result)
        })
        .await
    };
}

async fn expire_claim(h: &support::stage9_zip_replay::Fixture, id: &str) {
    h.state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE zip_v2_executions SET lease_until='2000-01-01 00:00:00' WHERE id=?",
            [id.into()],
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn url_without_client_digest_measures_complete_bytes_and_persists_original_receipt() {
    scenario!(|mut h| {
        let first = request::import(
            &h,
            &request::url_xml(URL),
            request::import_controls("measured-source"),
        )
        .await;
        assert_eq!(first.status, StatusCode::ACCEPTED);
        let id = first.id(true);
        let snapshot = execution::read(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        let capture: serde_json::Value = serde_json::from_str(&snapshot.captured_options).unwrap();
        assert_eq!(capture["accepted_response"]["version"], 1);
        assert_eq!(capture["accepted_response"]["xml"], first.body);
        let claim = execution::claim(h.state.store.db(), &id, "first-fetch", 30)
            .await
            .unwrap()
            .unwrap();
        let artifact = fetch_v2_zip_source(
            &h.imports,
            &h.state,
            &serde_json::to_string(&("url", URL)).unwrap(),
            None,
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap();
        let expected = hex::encode(Sha256::digest(Outcome::Complete.archive()));
        assert_eq!(artifact.sha256, expected);
        assert_eq!(artifact.size, Outcome::Complete.archive().len() as u64);
        assert_eq!(artifact.cid, ARCHIVE);
        eprintln!(
            "R25_IMPORT measured_size={} measured_sha256={} source_gets=1 artifact_adds=1",
            artifact.size, artifact.sha256
        );
        import_intake::bind_verified_input(
            h.state.store.db(),
            &claim,
            &artifact.sha256,
            &artifact.cid,
            artifact.size as i64,
        )
        .await
        .unwrap();
        let bound = execution::read(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bound.input_sha256.as_deref(), Some(expected.as_str()));
        assert_eq!(h.source.calls(), 1);
        assert_eq!(h.adds().await, 1);
        Ledger::read(&h).await.assert_unpublished();
        expire_claim(&h, &id).await;
        let takeover = execution::claim(h.state.store.db(), &id, "takeover", 30)
            .await
            .unwrap()
            .unwrap();
        assert!(
            import_intake::bind_verified_input(
                h.state.store.db(),
                &claim,
                &"a".repeat(64),
                ARCHIVE,
                1
            )
            .await
            .is_err()
        );
        assert_eq!(
            execution::read(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap()
                .input_sha256,
            bound.input_sha256
        );
        assert!(
            import_intake::bind_verified_input(
                h.state.store.db(),
                &takeover,
                &"a".repeat(64),
                ARCHIVE,
                1
            )
            .await
            .is_err()
        );
        assert_eq!(
            request::import(
                &h,
                &request::url_xml(URL),
                request::import_controls("measured-source")
            )
            .await,
            first
        );
        h.restart(false).await;
        assert_eq!(
            request::import(
                &h,
                &request::url_xml(URL),
                request::import_controls("measured-source")
            )
            .await,
            first
        );
        assert_eq!(h.source.calls(), 1);
    });
}

struct IncompleteSource;

#[async_trait::async_trait]
impl ImportHttpTransport for IncompleteSource {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        Ok(DownloadStream {
            body: Box::pin(futures_util::stream::iter([Ok(Bytes::from_static(b"ZIP"))])),
            total: Some(9),
            content_type: None,
        })
    }
}

struct GoneSource(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl ImportHttpTransport for GoneSource {
    async fn open(
        &self,
        _: AuthorizedSource,
        _: DownloadLimits,
        _: tokio::sync::watch::Sender<u64>,
        _: CancellationToken,
    ) -> Result<DownloadStream, DownloadError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(DownloadError::HttpStatus(404))
    }
}

#[tokio::test]
async fn restart_with_bound_input_and_source_404_publishes_original_bytes_without_get() {
    scenario!(|mut h| {
        let headers = request::import_controls("source-gone-after-bind");
        let first = request::import(&h, &request::url_xml(URL), headers.clone()).await;
        assert_eq!(first.status, StatusCode::ACCEPTED);
        let id = first.id(true);
        let claim = execution::claim(h.state.store.db(), &id, "before-crash", 30)
            .await
            .unwrap()
            .unwrap();
        let artifact = fetch_v2_zip_source(
            &h.imports,
            &h.state,
            &serde_json::to_string(&("url", URL)).unwrap(),
            None,
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap();
        import_intake::bind_verified_input(
            h.state.store.db(),
            &claim,
            &artifact.sha256,
            &artifact.cid,
            artifact.size as i64,
        )
        .await
        .unwrap();
        expire_claim(&h, &id).await;
        h.restart(false).await;
        let attempted_gets = Arc::new(AtomicUsize::new(0));
        let config = ImportConfig {
            allowed_https_origins: vec![ORIGIN.to_owned()],
            ..Default::default()
        }
        .validate()
        .unwrap();
        h.imports = ImportCoordinator::new(
            config.clone(),
            SourceDownloader::with_components(
                Arc::new(config),
                h.source.clone(),
                Arc::new(StrictPublicAddressPolicy),
                Arc::new(GoneSource(attempted_gets.clone())),
            ),
        );
        h.start_worker();
        assert_eq!(request::terminal(&h, &id).await, "ready");
        assert_eq!(
            request::get(&h, "out/file.txt").await,
            (StatusCode::OK, b"single entry bytes".to_vec())
        );
        let bound = execution::read(h.state.store.db(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            bound.input_sha256.as_deref(),
            Some(artifact.sha256.as_str())
        );
        assert_eq!(bound.input_art_cid.as_deref(), Some(artifact.cid.as_str()));
        assert_eq!(bound.input_art_size, Some(artifact.size as i64));
        let ledger = Ledger::read(&h).await;
        let adds = h.adds().await;
        assert_eq!(
            request::import(&h, &request::url_xml(URL), headers).await,
            first
        );
        assert_eq!(Ledger::read(&h).await, ledger);
        assert_eq!(h.adds().await, adds);
        assert_eq!(h.source.calls(), 1);
        assert_eq!(attempted_gets.load(Ordering::SeqCst), 0);
    });
}

#[tokio::test]
async fn no_digest_url_still_requires_clean_length_and_never_pins_incomplete_input() {
    scenario!(|h| {
        let config = ImportConfig {
            allowed_https_origins: vec![ORIGIN.to_owned()],
            ..Default::default()
        }
        .validate()
        .unwrap();
        let imports = ImportCoordinator::new(
            config.clone(),
            SourceDownloader::with_components(
                Arc::new(config),
                h.source.clone(),
                Arc::new(StrictPublicAddressPolicy),
                Arc::new(IncompleteSource),
            ),
        );
        let error = fetch_v2_zip_source(
            &imports,
            &h.state,
            &serde_json::to_string(&("url", URL)).unwrap(),
            None,
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap_err();
        assert_eq!(error, V2SourceError::SourceIncomplete);
        assert!(
            h.kubo
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.url.path() != "/api/v0/pin/add")
        );
        Ledger::read(&h).await.assert_unpublished();
    });
}

#[tokio::test]
async fn bound_artifact_changed_bytes_fail_diagnostically_without_refetch_or_publication() {
    scenario!(|mut h| {
        let headers = request::import_controls("bound-corruption");
        let first = request::import(&h, &request::url_xml(URL), headers.clone()).await;
        assert_eq!(first.status, StatusCode::ACCEPTED);
        let id = first.id(true);
        let claim = execution::claim(h.state.store.db(), &id, "before-crash", 30)
            .await
            .unwrap()
            .unwrap();
        let artifact = fetch_v2_zip_source(
            &h.imports,
            &h.state,
            &serde_json::to_string(&("url", URL)).unwrap(),
            None,
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap();
        import_intake::bind_verified_input(
            h.state.store.db(),
            &claim,
            &artifact.sha256,
            &artifact.cid,
            artifact.size as i64,
        )
        .await
        .unwrap();
        let mut changed = Outcome::Complete.archive();
        changed.push(b'!');
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", ARCHIVE))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(changed))
            .with_priority(1)
            .mount(&h.kubo)
            .await;
        expire_claim(&h, &id).await;
        h.restart(true).await;
        assert_eq!(request::terminal(&h, &id).await, "failed");
        let failure: String = h
            .state
            .store
            .db()
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT receipt_metadata FROM zip_v2_import_requests WHERE batch_id=?",
                [id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "receipt_metadata")
            .unwrap();
        assert_eq!(failure, r#"{"code":"bound_artifact_mismatch"}"#);
        assert_eq!(h.source.calls(), 1);
        assert_eq!(
            request::import(&h, &request::url_xml(URL), headers).await,
            first
        );
        Ledger::read(&h).await.assert_unpublished();
    });
}

#[tokio::test]
async fn source_only_bound_artifact_missing_fails_without_refetch_or_source_version() {
    scenario!(|mut h| {
        let mut headers = request::import_controls("missing-source-only");
        headers.insert(
            "x-ipfs3-zip-publish-source",
            HeaderValue::from_static("true"),
        );
        headers.insert(
            "x-ipfs3-zip-publish-extracted",
            HeaderValue::from_static("false"),
        );
        let first = request::import(&h, &request::url_xml(URL), headers.clone()).await;
        assert_eq!(first.status, StatusCode::ACCEPTED);
        let id = first.id(true);
        let claim = execution::claim(h.state.store.db(), &id, "before-crash", 30)
            .await
            .unwrap()
            .unwrap();
        let artifact = fetch_v2_zip_source(
            &h.imports,
            &h.state,
            &serde_json::to_string(&("url", URL)).unwrap(),
            None,
            CancellationToken::new(),
            &ZipExtractionLimits::default(),
        )
        .await
        .unwrap();
        import_intake::bind_verified_input(
            h.state.store.db(),
            &claim,
            &artifact.sha256,
            &artifact.cid,
            artifact.size as i64,
        )
        .await
        .unwrap();
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .and(query_param("arg", ARCHIVE))
            .respond_with(ResponseTemplate::new(404))
            .with_priority(1)
            .mount(&h.kubo)
            .await;
        expire_claim(&h, &id).await;
        h.restart(true).await;
        assert_eq!(request::terminal(&h, &id).await, "failed");
        let status = request::status(&h, &id, true).await;
        assert!(!status.body.contains("<ETag>"));
        let failure: String = h
            .state
            .store
            .db()
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT receipt_metadata FROM zip_v2_import_requests WHERE batch_id=?",
                [id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "receipt_metadata")
            .unwrap();
        assert_eq!(failure, r#"{"code":"bound_artifact_unavailable"}"#);
        assert_eq!(h.source.calls(), 1);
        assert_eq!(
            request::import(&h, &request::url_xml(URL), headers).await,
            first
        );
        Ledger::read(&h).await.assert_unpublished();
    });
}
