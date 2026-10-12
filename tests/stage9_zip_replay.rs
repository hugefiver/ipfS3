//! R25 prerequisites: minimal clients and immutable replay at real SigV4 HTTP.
//! Intentionally allowed to remain RED until core/adapters implement R25.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}

use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    import::v2_source::fetch_v2_zip_source,
    store::zip::{execution, import_intake},
    zip::extract::ZipExtractionLimits,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use support::{
    stage9_zip_replay::{ARCHIVE, Entry, Fixture, KEY, Outcome, run},
    stage9_zip_replay_requests as request,
    stage9_zip_replay_requests::Ledger,
    stage9_zip_replay_source::URL,
};
use tokio_util::sync::CancellationToken;

macro_rules! scenario {
    ($entry:expr, $outcome:expr, |mut $h:ident| $body:block) => {
        scenario!(@owned $entry, $outcome, [mut], $h, $body)
    };
    ($entry:expr, $outcome:expr, |$h:ident| $body:block) => {
        scenario!(@owned $entry, $outcome, [], $h, $body)
    };
    (@owned $entry:expr, $outcome:expr, [$($mutable:tt)*], $h:ident, $body:block) => {
        run($entry, $outcome, |$($mutable)* $h| async move {
            let result = AssertUnwindSafe(async $body).catch_unwind().await;
            ($h, result)
        }).await
    };
}

fn assert_success(receipt: &request::Receipt, operation: &str) {
    assert_eq!(receipt.status, StatusCode::OK, "{operation}: {receipt:?}");
}

fn assert_accepted(receipt: &request::Receipt) {
    assert_eq!(
        receipt.status,
        StatusCode::ACCEPTED,
        "minimum/no-digest import must be accepted: {receipt:?}"
    );
}

fn assert_output_receipt(receipt: &request::Receipt, outcome: Outcome) {
    assert_eq!(
        receipt.status,
        if outcome.published() == 0 {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::OK
        },
        "{outcome:?}: {receipt:?}"
    );
    assert!(
        !receipt.headers.contains_key(http::header::ETAG),
        "no virtual source ETag"
    );
    assert!(
        !receipt.headers.contains_key("x-amz-version-id"),
        "no virtual source version"
    );
    if outcome.published() > 0 {
        assert!(
            receipt.headers.contains_key("x-ipfs-s3-zip-batch-id"),
            "{receipt:?}"
        );
        assert!(receipt.body.contains("<ZipBatchResult>"), "{receipt:?}");
        assert!(
            receipt
                .body
                .contains("<SourcePublished>false</SourcePublished>"),
            "{receipt:?}"
        );
        assert!(
            receipt.body.contains(&format!(
                "<PublishedCount>{}</PublishedCount>",
                outcome.published()
            )),
            "{receipt:?}"
        );
    }
}

async fn assert_no_source(h: &Fixture) {
    assert_eq!(
        request::get(h, KEY).await.0,
        StatusCode::NOT_FOUND,
        "source=false never creates a virtual object"
    );
}

#[tokio::test]
async fn minimum_direct_needs_only_zip_key_prefix_and_contract_no_token_or_digest() {
    scenario!(Entry::Direct, Outcome::Complete, |h| {
        let receipt = request::put(
            &h,
            Outcome::Complete.archive(),
            request::minimal(None),
            "out/",
        )
        .await;
        assert_success(&receipt, "minimum direct");
        assert!(receipt.body.contains("<ZipBatchResult>"), "{receipt:?}");
        assert_eq!(
            request::get(&h, "out/file.txt").await,
            (StatusCode::OK, b"single entry bytes".to_vec())
        );
    });
}

#[tokio::test]
async fn minimum_mpu_negotiates_at_create_and_uses_legacy_parts_and_complete() {
    scenario!(Entry::Mpu, Outcome::Complete, |h| {
        let created = request::create(&h, request::minimal(None), "out/").await;
        assert_success(&created, "minimum CreateMultipartUpload");
        let id = created.upload_id();
        let etag = request::upload(&h, &id, Outcome::Complete.archive()).await;
        let receipt = request::complete(&h, &id, &etag, HeaderMap::new()).await;
        assert_success(&receipt, "minimum CompleteMultipartUpload");
        assert!(receipt.body.contains("<ZipBatchResult>"), "{receipt:?}");
        assert_eq!(
            request::get(&h, "out/file.txt").await,
            (StatusCode::OK, b"single entry bytes".to_vec())
        );
    });
}

#[tokio::test]
async fn minimum_url_import_binds_server_artifact_without_client_sha_or_optional_token() {
    scenario!(Entry::Import, Outcome::Complete, |mut h| {
        h.start_worker();
        let receipt = request::import(&h, &request::url_xml(URL), request::minimal(None)).await;
        assert_accepted(&receipt);
        let id = receipt.id(true);
        assert_eq!(request::terminal(&h, &id).await, "ready");
        request::assert_server_bound_artifact(&h, &id, Outcome::Complete.archive().len()).await;
        assert_eq!(h.source.calls(), 1);
        assert_eq!(
            request::get(&h, "out/file.txt").await,
            (StatusCode::OK, b"single entry bytes".to_vec())
        );
    });
}

// ZIP comments change original archive bytes, not member contents or paths.
fn changed_zip_bytes(mut original: Vec<u8>) -> Vec<u8> {
    let length = original.len();
    assert_eq!(&original[length - 22..length - 18], b"PK\x05\x06");
    original[length - 2..].copy_from_slice(&1u16.to_le_bytes());
    original.push(b'!');
    original
}

#[tokio::test]
async fn direct_all_terminal_outcomes_replay_exactly_and_freeze_bytes_controls_and_target() {
    for outcome in [Outcome::Complete, Outcome::Partial, Outcome::Failed] {
        scenario!(Entry::Direct, outcome, |mut h| {
            request::enable_versioning(&h).await;
            let archive = outcome.archive();
            let headers = request::controls("ordinary-direct-request");
            let first = request::put(&h, archive.clone(), headers.clone(), "out/").await;
            assert_output_receipt(&first, outcome);
            assert_no_source(&h).await;
            // Preserve the existing direct zero-output error wire: its batch
            // exists durably but is not exposed as an error-response header.
            let id = h
                .state
                .store
                .db()
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "SELECT id FROM zip_v2_executions WHERE token=?",
                    ["ordinary-direct-request".into()],
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<String>("", "id")
                .unwrap();
            let original_status = request::status(&h, &id, false).await;
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            for restarted in [false, true] {
                if restarted {
                    h.restart(false).await;
                }
                assert_eq!(
                    request::put(&h, archive.clone(), headers.clone(), "out/").await,
                    first,
                    "{outcome:?} restarted={restarted}"
                );
                assert_eq!(request::status(&h, &id, false).await, original_status);
                assert_eq!(Ledger::read(&h).await, ledger);
                assert_eq!(h.adds().await, adds);
            }
            let changed = request::put(
                &h,
                changed_zip_bytes(archive.clone()),
                headers.clone(),
                "out/",
            )
            .await;
            assert_eq!(
                changed.status,
                StatusCode::CONFLICT,
                "same extracted contents cannot replace bound raw bytes: {changed:?}"
            );
            for (name, value) in [
                ("x-ipfs3-zip-publish-source", "true"),
                ("x-ipfs3-zip-targets", "extracted"),
            ] {
                let mut modified = headers.clone();
                modified.insert(name, HeaderValue::from_static(value));
                let conflict = request::put(&h, archive.clone(), modified, "out/").await;
                assert_eq!(
                    conflict.status,
                    StatusCode::CONFLICT,
                    "changed {name}: {conflict:?}"
                );
            }
            let changed_target = request::put(&h, archive, headers, "other/").await;
            assert_eq!(
                changed_target.status,
                StatusCode::CONFLICT,
                "{changed_target:?}"
            );
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_no_source(&h).await;
        });
    }
}

#[tokio::test]
async fn mpu_all_terminal_outcomes_keep_receipt_and_reject_changed_parts_and_controls() {
    for outcome in [Outcome::Complete, Outcome::Partial, Outcome::Failed] {
        scenario!(Entry::Mpu, outcome, |mut h| {
            request::enable_versioning(&h).await;
            let headers = request::controls("ordinary-multipart-request");
            let created = request::create(&h, headers.clone(), "out/").await;
            assert_success(&created, "full-control Create");
            let id = created.upload_id();
            let etag = request::upload(&h, &id, outcome.archive()).await;
            let first = request::complete(&h, &id, &etag, HeaderMap::new()).await;
            assert_output_receipt(&first, outcome);
            assert_eq!(
                first.id(false),
                id,
                "MPU retains its batch header even for zero outputs"
            );
            assert_no_source(&h).await;
            let terminal = execution::read(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                terminal.state, "completed",
                "including zero-output failed receipts"
            );
            if outcome.published() == 0 {
                Ledger::read(&h).await.assert_unpublished();
                let batch = ipfs_s3_gateway::store::zip::snapshot(h.state.store.db(), &id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(batch.batch.root_cid.is_none());
                assert!(
                    batch
                        .entries
                        .iter()
                        .all(|entry| entry.version_row_id.is_none())
                );
                assert_eq!(first.headers["x-ipfs-s3-zip-batch-status"], "failed");
            }
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            h.restart(false).await;
            assert_eq!(
                request::complete(&h, &id, &etag, HeaderMap::new()).await,
                first
            );
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(h.adds().await, adds);
            let conflict = request::complete(&h, &id, "\"different-part\"", HeaderMap::new()).await;
            assert_eq!(
                conflict.status,
                StatusCode::CONFLICT,
                "terminal selected-part fingerprint is immutable: {conflict:?}"
            );
            for (name, value) in [
                ("x-ipfs3-zip-publish-source", "true"),
                ("x-ipfs3-zip-targets", "extracted"),
            ] {
                let mut modified = headers.clone();
                modified.insert(name, HeaderValue::from_static(value));
                let conflict = request::create(&h, modified, "out/").await;
                assert_eq!(
                    conflict.status,
                    StatusCode::CONFLICT,
                    "terminal changed {name}: {conflict:?}"
                );
            }
            let conflict = request::create(&h, headers, "other/").await;
            assert_eq!(conflict.status, StatusCode::CONFLICT, "{conflict:?}");
            assert_eq!(Ledger::read(&h).await, ledger);
        });
    }
}

#[tokio::test]
async fn url_all_terminal_outcomes_replay_original_receipts_after_restart_without_refetch() {
    for outcome in [Outcome::Complete, Outcome::Partial, Outcome::Failed] {
        scenario!(Entry::Import, outcome, |mut h| {
            request::enable_versioning(&h).await;
            h.start_worker();
            let xml = request::url_xml(URL);
            let headers = request::import_controls("ordinary-url-request");
            let first = request::import(&h, &xml, headers.clone()).await;
            assert_accepted(&first);
            let id = first.id(true);
            assert_eq!(
                request::terminal(&h, &id).await,
                "ready",
                "{outcome:?}: settled batch, including zero-output failure"
            );
            request::assert_server_bound_artifact(&h, &id, outcome.archive().len()).await;
            assert_no_source(&h).await;
            let original_status = request::status(&h, &id, true).await;
            assert_eq!(original_status.status, StatusCode::OK);
            let ledger = Ledger::read(&h).await;
            let adds = h.adds().await;
            assert_eq!(h.source.calls(), 1);
            h.source.replace(changed_zip_bytes(outcome.archive()));
            for restarted in [false, true] {
                if restarted {
                    h.restart(true).await;
                }
                assert_eq!(
                    request::import(&h, &xml, headers.clone()).await,
                    first,
                    "{outcome:?} restarted={restarted}"
                );
                assert_eq!(request::status(&h, &id, true).await, original_status);
                assert_eq!(Ledger::read(&h).await, ledger);
                assert_eq!(
                    h.source.calls(),
                    1,
                    "descriptor replay never re-fetches after artifact binding"
                );
                assert_eq!(h.adds().await, adds);
            }
            for source in [
                request::url_xml("https://intake.example.test/different.zip"),
                format!("<IPFS3ImportRequest><CID>{ARCHIVE}</CID></IPFS3ImportRequest>"),
            ] {
                let conflict = request::import(&h, &source, headers.clone()).await;
                assert_eq!(
                    conflict.status,
                    StatusCode::CONFLICT,
                    "terminal URL/descriptor cannot change: {conflict:?}"
                );
            }
            assert_eq!(Ledger::read(&h).await, ledger);
            assert_eq!(h.source.calls(), 1);
        });
    }
}

#[tokio::test]
async fn pending_cid_import_replays_without_io_or_publication_and_rejects_descriptor_changes() {
    scenario!(Entry::Import, Outcome::Complete, |mut h| {
        let xml = format!("<IPFS3ImportRequest><CID>{ARCHIVE}</CID></IPFS3ImportRequest>");
        let headers = request::import_controls("ordinary-pending-request");
        let first = request::import(&h, &xml, headers.clone()).await;
        assert_accepted(&first);
        let pending = request::status(&h, &first.id(true), true).await;
        h.restart(false).await;
        assert_eq!(request::import(&h, &xml, headers.clone()).await, first);
        assert_eq!(request::status(&h, &first.id(true), true).await, pending);
        assert_eq!(
            execution::read(h.state.store.db(), &first.id(true))
                .await
                .unwrap()
                .unwrap()
                .state,
            "pending"
        );
        Ledger::read(&h).await.assert_unpublished();
        assert_eq!(h.source.calls(), 0);
        assert!(h.kubo.received_requests().await.unwrap().is_empty());
        let conflict = request::import(&h, &request::url_xml(URL), headers).await;
        assert_eq!(
            conflict.status,
            StatusCode::CONFLICT,
            "pending token binds descriptor too: {conflict:?}"
        );
        Ledger::read(&h).await.assert_unpublished();
        assert_eq!(h.source.calls(), 0);
    });
}

#[tokio::test]
async fn fenced_import_replays_its_failure_after_restart_without_readmission() {
    scenario!(Entry::Import, Outcome::Complete, |mut h| {
        let xml = format!("<IPFS3ImportRequest><CID>{ARCHIVE}</CID></IPFS3ImportRequest>");
        let headers = request::import_controls("ordinary-fenced-request");
        let first = request::import(&h, &xml, headers.clone()).await;
        assert_accepted(&first);
        let id = first.id(true);
        let claim = execution::claim(h.state.store.db(), &id, "displaced-test-worker", 30)
            .await
            .unwrap()
            .unwrap();
        assert!(execution::fence(h.state.store.db(), &claim).await.unwrap());
        assert_eq!(request::terminal(&h, &id).await, "failed");
        let failed = request::status(&h, &id, true).await;
        assert!(failed.body.contains("<State>failed</State>"), "{failed:?}");
        h.restart(true).await;
        assert_eq!(request::import(&h, &xml, headers).await, first);
        assert_eq!(request::status(&h, &id, true).await, failed);
        assert_eq!(
            execution::read(h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "fenced"
        );
        Ledger::read(&h).await.assert_unpublished();
        assert_eq!(h.source.calls(), 0);
        assert!(h.kubo.received_requests().await.unwrap().is_empty());
    });
}

#[tokio::test]
async fn fully_fetched_bound_url_artifact_is_used_by_restart_worker_not_re_fetched() {
    scenario!(Entry::Import, Outcome::Complete, |mut h| {
        let xml = request::url_xml(URL);
        let headers = request::import_controls("ordinary-crash-request");
        let first = request::import(&h, &xml, headers.clone()).await;
        assert_accepted(&first);
        let id = first.id(true);
        // Model a worker crash at the stable binding seam, using real downloader
        // IO and measured artifact fields, never a manufactured client digest.
        let claim = execution::claim(h.state.store.db(), &id, "crashed-worker", 30)
            .await
            .unwrap()
            .unwrap();
        let descriptor = serde_json::to_string(&("url", URL)).unwrap();
        let artifact = fetch_v2_zip_source(
            &h.imports,
            &h.state,
            &descriptor,
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
            i64::try_from(artifact.size).unwrap(),
        )
        .await
        .unwrap();
        request::assert_server_bound_artifact(&h, &id, Outcome::Complete.archive().len()).await;
        assert_eq!(h.source.calls(), 1);
        Ledger::read(&h).await.assert_unpublished();
        // The same receipt is stable while a live claim owns a bound artifact,
        // as well as before claim and after terminal completion.
        assert_eq!(request::import(&h, &xml, headers.clone()).await, first);
        assert_eq!(h.source.calls(), 1);
        Ledger::read(&h).await.assert_unpublished();
        h.state
            .store
            .db()
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE zip_v2_executions SET lease_until='2000-01-01 00:00:00' WHERE id=?",
                [id.clone().into()],
            ))
            .await
            .unwrap();
        h.source
            .replace(changed_zip_bytes(Outcome::Complete.archive()));
        h.restart(true).await;
        assert_eq!(request::terminal(&h, &id).await, "ready");
        let published = Ledger::read(&h).await;
        let adds = h.adds().await;
        assert_eq!(request::import(&h, &xml, headers).await, first);
        assert_eq!(Ledger::read(&h).await, published);
        assert_eq!(h.adds().await, adds);
        assert_eq!(h.source.calls(), 1);
        assert_eq!(
            request::get(&h, "out/file.txt").await,
            (StatusCode::OK, b"single entry bytes".to_vec())
        );
        assert_no_source(&h).await;
    });
}

async fn invalid_controls(entry: Entry) {
    scenario!(entry, Outcome::Complete, |h| {
        for kind in ["malformed", "duplicate", "contradictory", "unknown"] {
            let mut headers = match entry {
                Entry::Import => request::import_controls("ordinary-rejected-request"),
                _ => request::controls("ordinary-rejected-request"),
            };
            match kind {
                "malformed" => {
                    headers.insert(
                        "x-ipfs3-zip-publish-source",
                        HeaderValue::from_static("maybe"),
                    );
                }
                "duplicate" => {
                    headers.append(
                        "x-ipfs3-zip-token",
                        HeaderValue::from_static("another-request"),
                    );
                }
                "contradictory" => {
                    headers.insert(
                        "x-ipfs3-zip-publish-extracted",
                        HeaderValue::from_static("false"),
                    );
                }
                "unknown" => {
                    headers.insert(
                        "x-ipfs3-zip-unrecognized-control",
                        HeaderValue::from_static("true"),
                    );
                }
                _ => unreachable!(),
            }
            let receipt = match entry {
                Entry::Direct => {
                    request::put(&h, Outcome::Complete.archive(), headers, "out/").await
                }
                Entry::Mpu => request::create(&h, headers, "out/").await,
                Entry::Import => {
                    request::import(
                        &h,
                        &format!("<IPFS3ImportRequest><CID>{ARCHIVE}</CID></IPFS3ImportRequest>"),
                        headers,
                    )
                    .await
                }
            };
            assert_eq!(
                receipt.status,
                StatusCode::BAD_REQUEST,
                "{entry:?} {kind}: {receipt:?}"
            );
            assert!(
                receipt.body.contains("<Code>InvalidRequest</Code>")
                    || receipt.body.contains("<Code>InvalidArgument</Code>"),
                "parser rejection, never SigV4 failure: {receipt:?}"
            );
            Ledger::read(&h).await.assert_unpublished();
            assert_eq!(h.source.calls(), 0);
            assert!(
                h.kubo.received_requests().await.unwrap().is_empty(),
                "{entry:?} {kind} must reject before IO"
            );
        }
    });
}

#[tokio::test]
async fn direct_invalid_controls_reject_without_publication_lease_or_charge() {
    invalid_controls(Entry::Direct).await;
}
#[tokio::test]
async fn mpu_invalid_controls_reject_without_publication_lease_or_charge() {
    invalid_controls(Entry::Mpu).await;
}
#[tokio::test]
async fn import_invalid_controls_reject_without_publication_lease_or_charge() {
    invalid_controls(Entry::Import).await;
}

#[tokio::test]
async fn v1_and_v2_keep_distinct_wire_and_source_false_has_no_virtual_source() {
    scenario!(Entry::Direct, Outcome::Complete, |h| {
        let legacy = request::put(&h, Outcome::Complete.archive(), HeaderMap::new(), "out/").await;
        assert_success(&legacy, "v1 legacy wire");
        assert!(legacy.body.contains("<DecompressZipResult>"), "{legacy:?}");
        assert!(!legacy.body.contains("<ZipBatchResult>"));
        assert!(legacy.headers.contains_key(http::header::ETAG));
        assert_eq!(request::get(&h, KEY).await.0, StatusCode::OK);
    });
    scenario!(Entry::Direct, Outcome::Complete, |h| {
        let v2 = request::put(
            &h,
            Outcome::Complete.archive(),
            request::controls("ordinary-wire-request"),
            "out/",
        )
        .await;
        assert_output_receipt(&v2, Outcome::Complete);
        assert!(!v2.body.contains("<DecompressZipResult>"));
        assert_no_source(&h).await;
    });
}
