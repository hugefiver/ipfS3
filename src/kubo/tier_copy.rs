use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::Stream;
use http::HeaderMap;
use http_body_util::BodyExt as _;
use reqwest::{Body as ReqwestBody, multipart};
use serde::Deserialize;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{KuboClient, LocalResidencyVerificationReceipt, NdjsonBuffer};
use crate::error::{AppError, AppResult, TierError};

const EXPORT_PENDING: u8 = 0;
const EXPORT_COMPLETE: u8 = 1;
const EXPORT_FAILED: u8 = 2;
const EXPORT_CANCELED: u8 = 3;
const MAX_IMPORT_RESPONSE_BYTES: usize = 128 * 1024;
const STREAM_ERROR_HEADER: &str = "x-stream-error";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ImportRecord {
    #[serde(rename = "Root")]
    root: Option<ImportRoot>,
    #[serde(rename = "Stats")]
    stats: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ImportRoot {
    #[serde(rename = "Cid")]
    cid: ImportCid,
    #[serde(rename = "PinErrorMsg")]
    pin_error: String,
}

#[derive(Deserialize)]
struct ImportCid {
    #[serde(rename = "/")]
    value: String,
}

#[derive(Default)]
struct ImportEvidence {
    roots: usize,
    stats: usize,
}

fn tier(error: TierError) -> AppError {
    AppError::Tier(error)
}

fn has_stream_error(headers: &HeaderMap) -> bool {
    headers.contains_key(STREAM_ERROR_HEADER)
}

fn set_export_state(state: &AtomicU8, outcome: u8) {
    state.store(outcome, Ordering::Release);
}

fn export_body_stream(
    response: reqwest::Response,
    idle_timeout: std::time::Duration,
    cancel: CancellationToken,
    state: Arc<AtomicU8>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    async_stream::stream! {
        if has_stream_error(response.headers()) {
            set_export_state(&state, EXPORT_FAILED);
            yield Err(std::io::Error::other("Kubo DAG export failed"));
            return;
        }

        let mut body: reqwest::Body = response.into();
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    set_export_state(&state, EXPORT_CANCELED);
                    yield Err(std::io::Error::other("Kubo DAG export canceled"));
                    return;
                }
                result = tokio::time::timeout(idle_timeout, body.frame()) => result,
            };
            let frame = match next {
                Ok(Some(Ok(frame))) => frame,
                Ok(Some(Err(_))) | Err(_) => {
                    set_export_state(&state, EXPORT_FAILED);
                    yield Err(std::io::Error::other("Kubo DAG export stream failed"));
                    return;
                }
                Ok(None) => {
                    set_export_state(&state, EXPORT_COMPLETE);
                    return;
                }
            };

            match frame.into_data() {
                Ok(data) => yield Ok(data),
                Err(frame) => match frame.into_trailers() {
                    Ok(trailers) if has_stream_error(&trailers) => {
                        set_export_state(&state, EXPORT_FAILED);
                        yield Err(std::io::Error::other("Kubo DAG export trailer reported failure"));
                        return;
                    }
                    Ok(_) | Err(_) => {}
                },
            }
        }
    }
}

fn observed_upload_body(
    body: reqwest::Body,
    state: Arc<AtomicU8>,
    progress: Arc<AtomicU64>,
    changed: Arc<Notify>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static {
    async_stream::stream! {
        let mut body = body;
        loop {
            let frame = match body.frame().await {
                Some(Ok(frame)) => frame,
                Some(Err(_)) => {
                    set_export_state(&state, EXPORT_FAILED);
                    changed.notify_one();
                    yield Err(std::io::Error::other("Kubo DAG import upload failed"));
                    return;
                }
                None => {
                    set_export_state(&state, EXPORT_COMPLETE);
                    changed.notify_one();
                    return;
                }
            };
            if let Ok(data) = frame.into_data() {
                if !data.is_empty() {
                    progress.fetch_add(1, Ordering::Release);
                    changed.notify_one();
                }
                yield Ok(data);
            }
        }
    }
}

/// Stream one locally complete UnixFS DAG between distinct Kubo nodes as a CAR,
/// then prove that the destination holds the requested recursive pin locally.
pub async fn stream_copy_verified(
    source: &KuboClient,
    destination: &KuboClient,
    cid: &str,
    expected_source_node: Option<&str>,
    expected_destination_node: Option<&str>,
    cancel: &CancellationToken,
) -> AppResult<LocalResidencyVerificationReceipt> {
    let expected_cid = cid::Cid::try_from(cid).map_err(|_| tier(TierError::CidMismatch))?;
    let canonical_cid = expected_cid.to_string();

    let identities = tokio::select! {
        _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
        result = async {
            tokio::try_join!(source.local_node_identity(), destination.local_node_identity())
        } => result.map_err(|_| tier(TierError::TierUnavailable))?,
    };
    ensure_identity_binding(
        &identities.0,
        &identities.1,
        expected_source_node,
        expected_destination_node,
    )?;

    let source_receipt = tokio::select! {
        _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
        result = source.verify_local_residency(cid) => {
            result.map_err(|_| tier(TierError::LocalCopyIncomplete))?
        }
    };
    let verified_source_cid = cid::Cid::try_from(source_receipt.cid.as_str())
        .map_err(|_| tier(TierError::CidMismatch))?;
    if verified_source_cid != expected_cid {
        return Err(tier(TierError::CidMismatch));
    }
    if source_receipt.node_identity != identities.0 {
        return Err(tier(TierError::NodeIdentityMismatch));
    }

    let mut export_url = reqwest::Url::parse(&format!("{}/api/v0/dag/export", source.base_url()))
        .map_err(|_| tier(TierError::TierUnavailable))?;
    export_url
        .query_pairs_mut()
        .append_pair("arg", &canonical_cid)
        .append_pair("offline", "true")
        .append_pair("progress", "false");
    let export_response = tokio::select! {
        _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
        result = source.download_http().post(export_url).send() => {
            result.map_err(|_| tier(TierError::TierUnavailable))?
        }
    };
    if !export_response.status().is_success() {
        return Err(tier(TierError::TierUnavailable));
    }

    let export_state = Arc::new(AtomicU8::new(EXPORT_PENDING));
    let export_stream = export_body_stream(
        export_response,
        source.stream_idle_timeout(),
        cancel.clone(),
        Arc::clone(&export_state),
    );
    let body = ReqwestBody::wrap_stream(export_stream);
    let part = multipart::Part::stream(body)
        .file_name("export.car")
        .mime_str("application/vnd.ipld.car")
        .expect("the CAR MIME type is valid");
    let form = multipart::Form::new().part("file", part);

    let mut import_url =
        reqwest::Url::parse(&format!("{}/api/v0/dag/import", destination.base_url()))
            .map_err(|_| tier(TierError::TierUnavailable))?;
    import_url
        .query_pairs_mut()
        .append_pair("pin-roots", "true")
        .append_pair("stats", "true")
        .append_pair("fast-provide-root", "false")
        .append_pair("fast-provide-dag", "false")
        .append_pair("fast-provide-wait", "false")
        .append_pair("encoding", "json")
        .append_pair("stream-channels", "true");

    let mut request = destination
        .upload_http()
        .post(import_url)
        .multipart(form)
        .build()
        .map_err(|_| tier(TierError::TierUnavailable))?;
    let multipart_body = request
        .body_mut()
        .take()
        .ok_or_else(|| tier(TierError::LocalCopyIncomplete))?;
    let upload_state = Arc::new(AtomicU8::new(EXPORT_PENDING));
    let upload_progress = Arc::new(AtomicU64::new(0));
    let upload_changed = Arc::new(Notify::new());
    *request.body_mut() = Some(ReqwestBody::wrap_stream(observed_upload_body(
        multipart_body,
        Arc::clone(&upload_state),
        Arc::clone(&upload_progress),
        Arc::clone(&upload_changed),
    )));
    let import_response = execute_import_with_idle_watchdog(
        destination,
        request,
        cancel,
        &export_state,
        &upload_state,
        &upload_progress,
        &upload_changed,
    )
    .await?;

    if !import_response.status().is_success() {
        return Err(tier(TierError::TierUnavailable));
    }
    ensure_complete_transfer(cancel, &export_state, &upload_state)?;
    parse_import_response(import_response, destination, &expected_cid, cancel).await?;

    let rebound = tokio::select! {
        _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
        result = async {
            tokio::try_join!(source.local_node_identity(), destination.local_node_identity())
        } => result.map_err(|_| tier(TierError::TierUnavailable))?,
    };
    if rebound != identities {
        return Err(tier(TierError::NodeIdentityMismatch));
    }
    ensure_expected_identity(&rebound.0, expected_source_node)?;
    ensure_expected_identity(&rebound.1, expected_destination_node)?;

    let receipt = tokio::select! {
        _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
        result = destination.verify_local_residency(cid) => {
            result.map_err(|_| tier(TierError::LocalCopyIncomplete))?
        }
    };
    let receipt_cid =
        cid::Cid::try_from(receipt.cid.as_str()).map_err(|_| tier(TierError::CidMismatch))?;
    if receipt_cid != expected_cid || receipt.node_identity != rebound.1 {
        return Err(tier(TierError::LocalCopyIncomplete));
    }
    Ok(receipt)
}

fn import_send_error(cancel: &CancellationToken, export_state: &AtomicU8) -> AppError {
    if cancel.is_cancelled() || export_state.load(Ordering::Acquire) == EXPORT_CANCELED {
        tier(TierError::Cancelled)
    } else {
        tier(TierError::TierUnavailable)
    }
}

async fn execute_import_with_idle_watchdog(
    destination: &KuboClient,
    request: reqwest::Request,
    cancel: &CancellationToken,
    export_state: &AtomicU8,
    upload_state: &AtomicU8,
    upload_progress: &AtomicU64,
    upload_changed: &Notify,
) -> AppResult<reqwest::Response> {
    let mut import_request = Box::pin(destination.upload_http().execute(request));
    let idle_timeout = destination.stream_idle_timeout();
    let idle_deadline = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle_deadline);
    let mut observed_progress = upload_progress.load(Ordering::Acquire);
    let mut response_wait_started = false;

    loop {
        let state_changed = upload_changed.notified();
        tokio::pin!(state_changed);

        let current_progress = upload_progress.load(Ordering::Acquire);
        if current_progress != observed_progress {
            observed_progress = current_progress;
            idle_deadline
                .as_mut()
                .reset(tokio::time::Instant::now() + idle_timeout);
        }

        match upload_state.load(Ordering::Acquire) {
            EXPORT_COMPLETE if !response_wait_started => {
                response_wait_started = true;
                idle_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + idle_timeout);
            }
            EXPORT_CANCELED => return Err(tier(TierError::Cancelled)),
            EXPORT_FAILED => return Err(import_send_error(cancel, export_state)),
            _ => {}
        }

        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
            result = &mut import_request => {
                let response = result.map_err(|_| import_send_error(cancel, export_state))?;
                ensure_complete_transfer(cancel, export_state, upload_state)?;
                return Ok(response);
            }
            _ = &mut state_changed => {}
            _ = &mut idle_deadline => return Err(tier(TierError::TierUnavailable)),
        }
    }
}

fn ensure_complete_transfer(
    cancel: &CancellationToken,
    export_state: &AtomicU8,
    upload_state: &AtomicU8,
) -> AppResult<()> {
    let export = export_state.load(Ordering::Acquire);
    let upload = upload_state.load(Ordering::Acquire);
    if cancel.is_cancelled() || export == EXPORT_CANCELED || upload == EXPORT_CANCELED {
        Err(tier(TierError::Cancelled))
    } else if export == EXPORT_FAILED || upload == EXPORT_FAILED {
        Err(tier(TierError::TierUnavailable))
    } else if export == EXPORT_COMPLETE && upload == EXPORT_COMPLETE {
        Ok(())
    } else {
        Err(tier(TierError::LocalCopyIncomplete))
    }
}

fn ensure_identity_binding(
    source: &str,
    destination: &str,
    expected_source: Option<&str>,
    expected_destination: Option<&str>,
) -> AppResult<()> {
    ensure_expected_identity(source, expected_source)?;
    ensure_expected_identity(destination, expected_destination)?;
    if source == destination {
        return Err(tier(TierError::SameNode));
    }
    Ok(())
}

fn ensure_expected_identity(actual: &str, expected: Option<&str>) -> AppResult<()> {
    if expected.is_some_and(|expected| expected != actual) {
        return Err(tier(TierError::NodeIdentityMismatch));
    }
    Ok(())
}

async fn parse_import_response(
    response: reqwest::Response,
    destination: &KuboClient,
    expected_cid: &cid::Cid,
    cancel: &CancellationToken,
) -> AppResult<()> {
    if has_stream_error(response.headers()) {
        return Err(tier(TierError::TierUnavailable));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_IMPORT_RESPONSE_BYTES as u64)
    {
        return Err(tier(TierError::LocalCopyIncomplete));
    }

    let mut body: reqwest::Body = response.into();
    let mut lines = NdjsonBuffer::new();
    let mut evidence = ImportEvidence::default();
    let mut received = 0_usize;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => return Err(tier(TierError::Cancelled)),
            result = tokio::time::timeout(destination.stream_idle_timeout(), body.frame()) => result,
        };
        let frame = match next {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(_))) | Err(_) => return Err(tier(TierError::TierUnavailable)),
            Ok(None) => break,
        };
        match frame.into_data() {
            Ok(data) => {
                received = received
                    .checked_add(data.len())
                    .filter(|total| *total <= MAX_IMPORT_RESPONSE_BYTES)
                    .ok_or_else(|| tier(TierError::LocalCopyIncomplete))?;
                lines
                    .push(data)
                    .map_err(|_| tier(TierError::LocalCopyIncomplete))?;
                while let Some(record) = lines
                    .next_record()
                    .map_err(|_| tier(TierError::LocalCopyIncomplete))?
                {
                    process_import_record(&record, expected_cid, &mut evidence)?;
                }
            }
            Err(frame) => match frame.into_trailers() {
                Ok(trailers) if has_stream_error(&trailers) => {
                    return Err(tier(TierError::TierUnavailable));
                }
                Ok(_) | Err(_) => {}
            },
        }
    }
    if let Some(record) = lines.finish() {
        process_import_record(&record, expected_cid, &mut evidence)?;
    }
    if evidence.roots != 1 || evidence.stats != 1 {
        return Err(tier(TierError::LocalCopyIncomplete));
    }
    Ok(())
}

fn process_import_record(
    record: &[u8],
    expected_cid: &cid::Cid,
    evidence: &mut ImportEvidence,
) -> AppResult<()> {
    let record: ImportRecord =
        serde_json::from_slice(record).map_err(|_| tier(TierError::LocalCopyIncomplete))?;
    match (record.root, record.stats) {
        (Some(root), None) => {
            evidence.roots = evidence
                .roots
                .checked_add(1)
                .ok_or_else(|| tier(TierError::LocalCopyIncomplete))?;
            if evidence.roots != 1 || !root.pin_error.is_empty() {
                return Err(tier(TierError::LocalCopyIncomplete));
            }
            let imported = cid::Cid::try_from(root.cid.value.as_str())
                .map_err(|_| tier(TierError::CidMismatch))?;
            if imported != *expected_cid {
                return Err(tier(TierError::CidMismatch));
            }
        }
        (None, Some(stats)) if stats.is_object() => {
            evidence.stats = evidence
                .stats
                .checked_add(1)
                .ok_or_else(|| tier(TierError::LocalCopyIncomplete))?;
            if evidence.stats != 1 {
                return Err(tier(TierError::LocalCopyIncomplete));
            }
        }
        _ => return Err(tier(TierError::LocalCopyIncomplete)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    const OTHER_CID: &str = "bafkreigh2akiscaildc6ii5zji4bq7kly5k3s7svv6q2wx2nn5rtj5xuu4";
    const SOURCE_NODE: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    const CHANGED_SOURCE_NODE: &str = "QmNLei78zC5X8YwFoVrQwMYM4Q7jKuCDNRQuk3KfHk7zXq";
    const DESTINATION_NODE: &str = "QmPChd2hVbrJ6i1a7aDPgS6G9X4YuJ5sS7cGqf6ZkK3vYq";

    async fn mount_identity(server: &MockServer, node: &str, calls: u64) {
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .and(query_param("peerid-base", "b58mh"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string(format!(r#"{{"ID":"{node}"}}"#)),
            )
            .expect(calls)
            .mount(server)
            .await;
    }

    async fn mount_export(server: &MockServer, body: &'static [u8]) {
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .and(query_param("arg", CID))
            .and(query_param("offline", "true"))
            .and(query_param("progress", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .expect(1)
            .mount(server)
            .await;
    }

    async fn mount_import(server: &MockServer, body: String) {
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .and(query_param("pin-roots", "true"))
            .and(query_param("stats", "true"))
            .and(query_param("fast-provide-root", "false"))
            .and(query_param("fast-provide-dag", "false"))
            .and(query_param("fast-provide-wait", "false"))
            .and(query_param("encoding", "json"))
            .and(query_param("stream-channels", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .expect(1)
            .mount(server)
            .await;
    }

    async fn mount_local_verification(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .and(query_param("arg", CID))
            .and(query_param("type", "recursive"))
            .and(query_param("offline", "true"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#)),
            )
            .expect(2)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/files/stat"))
            .and(query_param("arg", format!("/ipfs/{CID}")))
            .and(query_param("with-local", "true"))
            .and(query_param("offline", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                r#"{{"Hash":"{CID}","WithLocality":true,"Local":true}}"#
            )))
            .expect(1)
            .mount(server)
            .await;
    }

    fn successful_import(cid: &str) -> String {
        format!(
            "{{\"Root\":{{\"Cid\":{{\"/\":\"{cid}\"}},\"PinErrorMsg\":\"\"}}}}\n{{\"Stats\":{{\"BlockCount\":1,\"BlockBytesCount\":3}}}}\n"
        )
    }

    fn assert_tier_error(error: AppError, expected: TierError) {
        let matches = matches!(
            (error, expected),
            (AppError::Tier(TierError::SameNode), TierError::SameNode)
                | (
                    AppError::Tier(TierError::NodeIdentityMismatch),
                    TierError::NodeIdentityMismatch
                )
                | (
                    AppError::Tier(TierError::CidMismatch),
                    TierError::CidMismatch
                )
                | (
                    AppError::Tier(TierError::LocalCopyIncomplete),
                    TierError::LocalCopyIncomplete
                )
                | (
                    AppError::Tier(TierError::TierUnavailable),
                    TierError::TierUnavailable
                )
                | (AppError::Tier(TierError::Cancelled), TierError::Cancelled)
        );
        assert!(matches, "unexpected tier error");
    }

    fn assert_early_copy_error(error: AppError) {
        assert!(matches!(
            error,
            AppError::Tier(TierError::LocalCopyIncomplete | TierError::TierUnavailable)
        ));
    }

    #[tokio::test]
    async fn streams_car_with_exact_kubo_contract_and_returns_fresh_receipt() {
        let source = MockServer::start().await;
        let destination = MockServer::start().await;
        mount_identity(&source, SOURCE_NODE, 4).await;
        mount_identity(&destination, DESTINATION_NODE, 4).await;
        mount_local_verification(&source).await;
        mount_export(&source, b"car-bytes").await;
        mount_import(&destination, successful_import(CID)).await;
        mount_local_verification(&destination).await;

        let receipt = stream_copy_verified(
            &KuboClient::new(source.uri()),
            &KuboClient::new(destination.uri()),
            CID,
            Some(SOURCE_NODE),
            Some(DESTINATION_NODE),
            &CancellationToken::new(),
        )
        .await
        .expect("a complete local CAR copy must verify");

        assert_eq!(receipt.node_identity, DESTINATION_NODE);
        assert_eq!(receipt.cid, CID);
        let source_requests = source.received_requests().await.unwrap();
        let export = source_requests
            .iter()
            .find(|request| request.url.path() == "/api/v0/dag/export")
            .unwrap();
        let mut export_query: Vec<_> = export.url.query_pairs().collect();
        export_query.sort_unstable();
        assert_eq!(
            export_query,
            vec![
                ("arg".into(), CID.into()),
                ("offline".into(), "true".into()),
                ("progress".into(), "false".into()),
            ]
        );
        let requests = destination.received_requests().await.unwrap();
        let import = requests
            .iter()
            .find(|request| request.url.path() == "/api/v0/dag/import")
            .unwrap();
        let mut import_query: Vec<_> = import.url.query_pairs().collect();
        import_query.sort_unstable();
        assert_eq!(
            import_query,
            vec![
                ("encoding".into(), "json".into()),
                ("fast-provide-dag".into(), "false".into()),
                ("fast-provide-root".into(), "false".into()),
                ("fast-provide-wait".into(), "false".into()),
                ("pin-roots".into(), "true".into()),
                ("stats".into(), "true".into()),
                ("stream-channels".into(), "true".into()),
            ]
        );
        let multipart = String::from_utf8_lossy(&import.body);
        assert!(multipart.contains("filename=\"export.car\""));
        assert!(multipart.contains("car-bytes"));
    }

    #[tokio::test]
    async fn import_upload_has_no_control_plane_total_timeout() {
        let source = MockServer::start().await;
        let destination = MockServer::start().await;
        mount_identity(&source, SOURCE_NODE, 4).await;
        mount_identity(&destination, DESTINATION_NODE, 4).await;
        mount_local_verification(&source).await;
        mount_export(&source, b"car").await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/import"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(successful_import(CID))
                    .set_delay(Duration::from_millis(200)),
            )
            .expect(1)
            .mount(&destination)
            .await;
        mount_local_verification(&destination).await;

        let destination_client =
            KuboClient::new_with_request_timeout(destination.uri(), Duration::from_millis(20));
        stream_copy_verified(
            &KuboClient::new(source.uri()),
            &destination_client,
            CID,
            None,
            None,
            &CancellationToken::new(),
        )
        .await
        .expect("the CAR import must not inherit the short control-plane timeout");
    }

    #[tokio::test]
    async fn non_successful_export_or_import_is_tier_unavailable() {
        for export_status in [Some(503), None] {
            let source = MockServer::start().await;
            let destination = MockServer::start().await;
            mount_identity(&source, SOURCE_NODE, 3).await;
            mount_identity(&destination, DESTINATION_NODE, 1).await;
            mount_local_verification(&source).await;
            Mock::given(method("POST"))
                .and(path("/api/v0/dag/export"))
                .respond_with(
                    ResponseTemplate::new(export_status.unwrap_or(200)).set_body_bytes(b"car"),
                )
                .expect(1)
                .mount(&source)
                .await;
            if export_status.is_none() {
                Mock::given(method("POST"))
                    .and(path("/api/v0/dag/import"))
                    .respond_with(ResponseTemplate::new(503))
                    .expect(1)
                    .mount(&destination)
                    .await;
            }

            let error = stream_copy_verified(
                &KuboClient::new(source.uri()),
                &KuboClient::new(destination.uri()),
                CID,
                None,
                None,
                &CancellationToken::new(),
            )
            .await
            .expect_err("non-successful Kubo transport responses must fail");
            assert_tier_error(error, TierError::TierUnavailable);
        }
    }

    #[tokio::test]
    async fn rejects_same_or_unexpected_node_before_export() {
        for (destination_node, expected_destination, expected_error) in [
            (SOURCE_NODE, Some(SOURCE_NODE), TierError::SameNode),
            (
                DESTINATION_NODE,
                Some("QmUnexpectedNode"),
                TierError::NodeIdentityMismatch,
            ),
        ] {
            let source = MockServer::start().await;
            let destination = MockServer::start().await;
            mount_identity(&source, SOURCE_NODE, 1).await;
            mount_identity(&destination, destination_node, 1).await;

            let error = stream_copy_verified(
                &KuboClient::new(source.uri()),
                &KuboClient::new(destination.uri()),
                CID,
                Some(SOURCE_NODE),
                expected_destination,
                &CancellationToken::new(),
            )
            .await
            .expect_err("identity policy must fail before transport");
            assert_tier_error(error, expected_error);
        }
    }

    #[tokio::test]
    async fn rejects_incomplete_source_residency_before_export() {
        let source = MockServer::start().await;
        let destination = MockServer::start().await;
        mount_identity(&source, SOURCE_NODE, 2).await;
        mount_identity(&destination, DESTINATION_NODE, 1).await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/ls"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"Keys":{}}"#))
            .expect(1)
            .mount(&source)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/dag/export"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"must-not-export"))
            .expect(0)
            .mount(&source)
            .await;

        let error = stream_copy_verified(
            &KuboClient::new(source.uri()),
            &KuboClient::new(destination.uri()),
            CID,
            Some(SOURCE_NODE),
            Some(DESTINATION_NODE),
            &CancellationToken::new(),
        )
        .await
        .expect_err("COPY requires a fully local recursively pinned source");
        assert_tier_error(error, TierError::LocalCopyIncomplete);
    }

    #[tokio::test]
    async fn rejects_mismatched_root_pin_error_and_incomplete_response() {
        let cases = [
            (successful_import(OTHER_CID), TierError::CidMismatch),
            (
                format!(
                    "{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"pin failed\"}}}}\n{{\"Stats\":{{}}}}\n"
                ),
                TierError::LocalCopyIncomplete,
            ),
            (
                format!("{{\"Root\":{{\"Cid\":{{\"/\":\"{CID}\"}},\"PinErrorMsg\":\"\"}}}}\n"),
                TierError::LocalCopyIncomplete,
            ),
        ];

        for (response, expected_error) in cases {
            let source = MockServer::start().await;
            let destination = MockServer::start().await;
            mount_identity(&source, SOURCE_NODE, 3).await;
            mount_identity(&destination, DESTINATION_NODE, 1).await;
            mount_local_verification(&source).await;
            mount_export(&source, b"car").await;
            mount_import(&destination, response).await;

            let error = stream_copy_verified(
                &KuboClient::new(source.uri()),
                &KuboClient::new(destination.uri()),
                CID,
                None,
                None,
                &CancellationToken::new(),
            )
            .await
            .expect_err("invalid import evidence must fail closed");
            assert_tier_error(error, expected_error);
        }
    }

    #[tokio::test]
    async fn rejects_node_identity_change_after_import() {
        let source = MockServer::start().await;
        let destination = MockServer::start().await;
        let identity_call = Arc::new(AtomicUsize::new(0));
        let identity_call_for_response = Arc::clone(&identity_call);
        Mock::given(method("POST"))
            .and(path("/api/v0/id"))
            .respond_with(move |_: &wiremock::Request| {
                let node = if identity_call_for_response.fetch_add(1, Ordering::SeqCst) < 3 {
                    SOURCE_NODE
                } else {
                    CHANGED_SOURCE_NODE
                };
                ResponseTemplate::new(200).set_body_string(format!(r#"{{"ID":"{node}"}}"#))
            })
            .expect(4)
            .mount(&source)
            .await;
        mount_identity(&destination, DESTINATION_NODE, 2).await;
        mount_local_verification(&source).await;
        mount_export(&source, b"car").await;
        mount_import(&destination, successful_import(CID)).await;

        let error = stream_copy_verified(
            &KuboClient::new(source.uri()),
            &KuboClient::new(destination.uri()),
            CID,
            Some(SOURCE_NODE),
            Some(DESTINATION_NODE),
            &CancellationToken::new(),
        )
        .await
        .expect_err("identity rebinding must invalidate an otherwise valid import");
        assert_tier_error(error, TierError::NodeIdentityMismatch);
    }

    async fn one_response_server(response: Vec<u8>) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            socket.write_all(&response).await.unwrap();
            socket.shutdown().await.unwrap();
        });
        (endpoint, task)
    }

    async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
            if request.ends_with(b"\r\n\r\n") {
                return request;
            }
        }
    }

    async fn source_server(
        chunks: usize,
        complete: bool,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(AtomicUsize::new(0));
        let sent_by_server = Arc::clone(&sent);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request_headers(&mut socket).await;
                let request = String::from_utf8_lossy(&request);
                if request.starts_with("POST /api/v0/id?") {
                    let body = format!(r#"{{"ID":"{SOURCE_NODE}"}}"#);
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    continue;
                }
                if request.starts_with("POST /api/v0/pin/ls?") {
                    let body = format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#);
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    continue;
                }
                if request.starts_with("POST /api/v0/files/stat?") {
                    let body = format!(r#"{{"Hash":"{CID}","WithLocality":true,"Local":true}}"#);
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    continue;
                }
                assert!(request.starts_with("POST /api/v0/dag/export?"));
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                let chunk = vec![0xa5; 64 * 1024];
                for _ in 0..chunks {
                    if socket.write_all(b"10000\r\n").await.is_err()
                        || socket.write_all(&chunk).await.is_err()
                        || socket.write_all(b"\r\n").await.is_err()
                    {
                        return;
                    }
                    sent_by_server.fetch_add(1, Ordering::Relaxed);
                }
                if complete {
                    socket.write_all(b"0\r\n\r\n").await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
            }
        });
        (endpoint, sent, task)
    }

    async fn stalling_source_server(
        chunks: usize,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        source_server(chunks, false).await
    }

    async fn complete_source_server(
        chunks: usize,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        source_server(chunks, true).await
    }

    async fn destination_server(
        reply_early: bool,
    ) -> (
        String,
        Arc<AtomicUsize>,
        Arc<Notify>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let received = Arc::new(AtomicUsize::new(0));
        let received_by_server = Arc::clone(&received);
        let changed = Arc::new(Notify::new());
        let changed_by_server = Arc::clone(&changed);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request_headers(&mut socket).await;
                let request = String::from_utf8_lossy(&request);
                if request.starts_with("POST /api/v0/id?") {
                    let body = format!(r#"{{"ID":"{DESTINATION_NODE}"}}"#);
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    continue;
                }
                assert!(request.starts_with("POST /api/v0/dag/import?"));
                if reply_early {
                    let body = successful_import(CID);
                    socket
                        .write_all(
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len()
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                    return;
                }
                let mut body = [0_u8; 64 * 1024];
                let count = loop {
                    let count = socket.read(&mut body).await.unwrap();
                    assert_ne!(count, 0, "the upload closed before any CAR bytes arrived");
                    let car_bytes = body[..count].iter().filter(|byte| **byte == 0xa5).count();
                    if car_bytes != 0 {
                        break car_bytes;
                    }
                };
                received_by_server.fetch_add(count, Ordering::Release);
                changed_by_server.notify_waiters();
                std::future::pending::<()>().await;
            }
        });
        (endpoint, received, changed, task)
    }

    async fn wait_for_nonzero(counter: &AtomicUsize, changed: &Notify) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = changed.notified();
                if counter.load(Ordering::Acquire) != 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("the raw peer must confirm nonzero bytes consumed after request headers");
    }

    async fn read_crlf_line(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut line = Vec::new();
        loop {
            let mut byte = [0_u8; 1];
            socket.read_exact(&mut byte).await.unwrap();
            line.push(byte[0]);
            if line.ends_with(b"\r\n") {
                line.truncate(line.len() - 2);
                return line;
            }
        }
    }

    async fn read_chunked_body_slowly(
        socket: &mut tokio::net::TcpStream,
        car_bytes: &AtomicUsize,
        delay: Duration,
    ) {
        loop {
            let line = read_crlf_line(socket).await;
            let size = usize::from_str_radix(
                std::str::from_utf8(line.split(|byte| *byte == b';').next().unwrap())
                    .unwrap()
                    .trim(),
                16,
            )
            .unwrap();
            if size == 0 {
                while !read_crlf_line(socket).await.is_empty() {}
                return;
            }

            let mut remaining = size;
            let mut buffer = [0_u8; 64 * 1024];
            while remaining != 0 {
                let count = remaining.min(buffer.len());
                socket.read_exact(&mut buffer[..count]).await.unwrap();
                let observed = buffer[..count].iter().filter(|byte| **byte == 0xa5).count();
                car_bytes.fetch_add(observed, Ordering::Release);
                remaining -= count;
                tokio::time::sleep(delay).await;
            }
            assert!(read_crlf_line(socket).await.is_empty());
        }
    }

    async fn slow_complete_destination_server(
        delay: Duration,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let received = Arc::new(AtomicUsize::new(0));
        let received_by_server = Arc::clone(&received);
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request_headers(&mut socket).await;
                let request = String::from_utf8_lossy(&request);
                let body = if request.starts_with("POST /api/v0/id?") {
                    format!(r#"{{"ID":"{DESTINATION_NODE}"}}"#)
                } else if request.starts_with("POST /api/v0/pin/ls?") {
                    format!(r#"{{"Keys":{{"{CID}":{{"Type":"recursive"}}}}}}"#)
                } else if request.starts_with("POST /api/v0/files/stat?") {
                    format!(r#"{{"Hash":"{CID}","WithLocality":true,"Local":true}}"#)
                } else {
                    assert!(request.starts_with("POST /api/v0/dag/import?"));
                    assert!(
                        request
                            .to_ascii_lowercase()
                            .contains("transfer-encoding: chunked")
                    );
                    read_chunked_body_slowly(&mut socket, &received_by_server, delay).await;
                    successful_import(CID)
                };
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        (endpoint, received, task)
    }

    #[tokio::test]
    async fn early_success_after_export_eof_without_multipart_closing_boundary_is_incomplete() {
        let response_body = successful_import(CID);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        )
        .into_bytes();
        let (endpoint, server) = one_response_server(response).await;
        let client = KuboClient::new_with_timeouts(
            endpoint.clone(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let unfinished_multipart = futures_util::stream::once(async {
            Ok::<_, std::io::Error>(Bytes::from_static(
                b"--boundary\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\ncar",
            ))
        })
        .chain(futures_util::stream::pending());
        let export_state = Arc::new(AtomicU8::new(EXPORT_COMPLETE));
        let upload_state = Arc::new(AtomicU8::new(EXPORT_PENDING));
        let upload_progress = Arc::new(AtomicU64::new(0));
        let upload_changed = Arc::new(Notify::new());
        let request = client
            .upload_http()
            .post(format!("{endpoint}/import"))
            .body(ReqwestBody::wrap_stream(observed_upload_body(
                ReqwestBody::wrap_stream(unfinished_multipart),
                Arc::clone(&upload_state),
                Arc::clone(&upload_progress),
                Arc::clone(&upload_changed),
            )))
            .build()
            .unwrap();

        let error = execute_import_with_idle_watchdog(
            &client,
            request,
            &CancellationToken::new(),
            &export_state,
            &upload_state,
            &upload_progress,
            &upload_changed,
        )
        .await
        .expect_err("an importer response cannot replace the multipart closing boundary");

        assert_tier_error(error, TierError::LocalCopyIncomplete);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exporter_and_importer_x_stream_error_trailers_fail_late() {
        let trailer_response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Stream-Error\r\nConnection: close\r\n\r\n3\r\ncar\r\n0\r\nX-Stream-Error: late failure\r\n\r\n".to_vec();
        let (endpoint, server) = one_response_server(trailer_response.clone()).await;
        let client = KuboClient::new(endpoint.clone());
        let response = client
            .download_http()
            .post(format!("{endpoint}/export"))
            .send()
            .await
            .unwrap();
        let state = Arc::new(AtomicU8::new(EXPORT_PENDING));
        let stream = export_body_stream(
            response,
            Duration::from_secs(1),
            CancellationToken::new(),
            Arc::clone(&state),
        );
        tokio::pin!(stream);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Bytes::from_static(b"car")
        );
        assert!(stream.next().await.unwrap().is_err());
        assert_eq!(state.load(Ordering::Acquire), EXPORT_FAILED);
        server.await.unwrap();

        let (endpoint, server) = one_response_server(trailer_response).await;
        let client = KuboClient::new(endpoint.clone());
        let response = client
            .download_http()
            .post(format!("{endpoint}/import"))
            .send()
            .await
            .unwrap();
        let expected = cid::Cid::try_from(CID).unwrap();
        let error = parse_import_response(response, &client, &expected, &CancellationToken::new())
            .await
            .expect_err("an import error trailer must override preceding data");
        assert_tier_error(error, TierError::TierUnavailable);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn import_response_size_is_bounded_before_collection() {
        let body = vec![b'x'; MAX_IMPORT_RESPONSE_BYTES + 1];
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body)
        .collect();
        let (endpoint, server) = one_response_server(response).await;
        let client = KuboClient::new(endpoint.clone());
        let response = client
            .download_http()
            .post(format!("{endpoint}/import"))
            .send()
            .await
            .unwrap();
        let expected = cid::Cid::try_from(CID).unwrap();
        let error = parse_import_response(response, &client, &expected, &CancellationToken::new())
            .await
            .expect_err("oversized import evidence must fail closed");
        assert_tier_error(error, TierError::LocalCopyIncomplete);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn early_import_reply_cannot_succeed_before_exporter_eof() {
        let (source, _, source_task) = stalling_source_server(1).await;
        let (destination, _, _, destination_task) = destination_server(true).await;
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            stream_copy_verified(
                &KuboClient::new(source),
                &KuboClient::new(destination),
                CID,
                None,
                None,
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("an early importer reply must be rejected promptly")
        .expect_err("an importer cannot attest to a CAR whose export is incomplete");
        assert_early_copy_error(error);
        source_task.abort();
        let _ = source_task.await;
        destination_task.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_interrupts_stalled_streaming_io() {
        let (source, sent, source_task) = stalling_source_server(4096).await;
        let (destination, received, destination_changed, destination_task) =
            destination_server(false).await;
        let cancel = CancellationToken::new();
        let operation_cancel = cancel.clone();
        let source_client = KuboClient::new(source);
        let destination_client = KuboClient::new(destination);
        let operation = tokio::spawn(async move {
            stream_copy_verified(
                &source_client,
                &destination_client,
                CID,
                None,
                None,
                &operation_cancel,
            )
            .await
        });
        wait_for_nonzero(&received, &destination_changed).await;
        assert!(
            sent.load(Ordering::Acquire) != 0,
            "the source must transmit CAR bytes before cancellation"
        );
        cancel.cancel();
        let error = tokio::time::timeout(Duration::from_secs(2), operation)
            .await
            .expect("cancellation must terminate all streaming IO")
            .expect("the COPY task must join without panicking")
            .expect_err("a canceled copy cannot succeed");
        assert_tier_error(error, TierError::Cancelled);
        source_task.abort();
        destination_task.abort();
        let _ = source_task.await;
        let _ = destination_task.await;
    }

    #[tokio::test]
    async fn stalled_in_flight_destination_times_out_without_external_cancellation() {
        const CHUNKS: usize = 4096;
        let (source, sent, source_task) = stalling_source_server(CHUNKS).await;
        let (destination, received, destination_changed, destination_task) =
            destination_server(false).await;
        let source_client =
            KuboClient::new_with_timeouts(source, Duration::from_secs(5), Duration::from_secs(5));
        let destination_client = KuboClient::new_with_timeouts(
            destination,
            Duration::from_secs(5),
            Duration::from_millis(50),
        );
        let operation = tokio::spawn(async move {
            stream_copy_verified(
                &source_client,
                &destination_client,
                CID,
                None,
                None,
                &CancellationToken::new(),
            )
            .await
        });
        wait_for_nonzero(&received, &destination_changed).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            sent.load(Ordering::Acquire) != 0,
            "the source must transmit CAR bytes before backpressure"
        );
        assert!(
            sent.load(Ordering::Relaxed) < CHUNKS,
            "a non-reading importer must not cause the entire 256 MiB export to be buffered"
        );
        let error = tokio::time::timeout(Duration::from_secs(2), operation)
            .await
            .expect("upload progress idle timeout must terminate the backpressured copy")
            .expect("the COPY task must join without panicking")
            .expect_err("a stalled upload cannot succeed");
        assert_tier_error(error, TierError::TierUnavailable);
        source_task.abort();
        destination_task.abort();
        let _ = source_task.await;
        let _ = destination_task.await;
    }

    #[tokio::test]
    async fn active_upload_longer_than_idle_timeout_succeeds() {
        const CHUNKS: usize = 128;
        let (source, sent, source_task) = complete_source_server(CHUNKS).await;
        let idle_timeout = Duration::from_millis(500);
        let (destination, received, destination_task) =
            slow_complete_destination_server(Duration::from_millis(10)).await;
        let source_client = KuboClient::new(source);
        let destination_client =
            KuboClient::new_with_timeouts(destination, Duration::from_secs(5), idle_timeout);
        let started = tokio::time::Instant::now();
        let operation = tokio::spawn(async move {
            stream_copy_verified(
                &source_client,
                &destination_client,
                CID,
                None,
                None,
                &CancellationToken::new(),
            )
            .await
        });

        let receipt = tokio::time::timeout(Duration::from_secs(10), operation)
            .await
            .expect("an active upload must finish within the test bound")
            .expect("the COPY task must join without panicking")
            .expect("continuous multipart progress must renew the idle watchdog");
        assert_eq!(receipt.cid, CID);
        assert_eq!(receipt.node_identity, DESTINATION_NODE);
        assert!(
            started.elapsed() > idle_timeout,
            "the transfer must outlive one idle interval to exclude a total deadline"
        );
        assert_eq!(sent.load(Ordering::Acquire), CHUNKS);
        assert!(
            received.load(Ordering::Acquire) >= CHUNKS * 64 * 1024,
            "the destination must consume the complete large CAR"
        );

        source_task.abort();
        destination_task.abort();
        let _ = source_task.await;
        let _ = destination_task.await;
    }

    #[tokio::test]
    async fn import_response_headers_are_idle_bounded_only_after_export_eof() {
        let source = MockServer::start().await;
        mount_identity(&source, SOURCE_NODE, 3).await;
        mount_local_verification(&source).await;
        mount_export(&source, b"complete-car").await;
        let (destination, _, _, destination_task) = destination_server(false).await;
        let destination_client = KuboClient::new_with_timeouts(
            destination,
            Duration::from_secs(5),
            Duration::from_millis(50),
        );

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            stream_copy_verified(
                &KuboClient::new(source.uri()),
                &destination_client,
                CID,
                None,
                None,
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("a destination that stalls after upload EOF must not hang")
        .expect_err("missing import response headers cannot succeed");
        assert_tier_error(error, TierError::TierUnavailable);
        destination_task.abort();
        let _ = destination_task.await;
    }
}
