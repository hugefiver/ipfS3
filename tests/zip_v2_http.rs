//! ZIP v2 contract at the real authenticated Axum/s3s HTTP boundary.
//! Real SigV4, transport, replay and output ownership boundaries.
#[allow(dead_code)]
mod support;

use std::collections::{BTreeMap, HashMap};

use hmac::{Hmac, KeyInit, Mac};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    import::{ImportSource, SupersedeReason},
    pinning::{
        config::ValidatedPinningConfig,
        coordinator::normalize_validated_config,
        zip_policy::{ZipOutputRuleConfig, ZipRuleEffect},
    },
    store::{
        self,
        entities::{
            import_destination, import_job, import_prefix_claim, object, object_tag,
            object_version, pin_job, pin_lease, pin_lease_target, remote_pin, zip_batch,
        },
        import::{jobs::NewImportJob, ownership},
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, Statement,
};
use sha2::{Digest, Sha256};
use support::decompress::{
    AddReply, KuboScript, TestHarness, legal_single_entry_zip, legal_two_entry_zip,
    start_harness_with_root_default, start_kubo_harness,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SOURCE: &str = "source.zip";
const OUTPUT: &str = "out/file.txt";
const QUERY: &[(&str, &str)] = &[("decompress-zip", "out/")];

fn controls(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

fn source_controls(token: &str, extracted: bool, targets: &str) -> HeaderMap {
    let mut headers = controls(token);
    headers.insert(
        "x-ipfs3-zip-publish-source",
        HeaderValue::from_static("true"),
    );
    headers.insert(
        "x-ipfs3-zip-publish-extracted",
        HeaderValue::from_static(if extracted { "true" } else { "false" }),
    );
    headers.insert(
        "x-ipfs3-zip-targets",
        HeaderValue::from_str(targets).unwrap(),
    );
    headers
}

#[tokio::test]
async fn source_only_publishes_real_archive_without_extracting_and_replays_after_overwrite() {
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmStagedZip"), AddReply::Ok("QmReplacement")],
            cat_bodies: HashMap::new(),
        },
        true,
    )
    .await;
    let archive = legal_single_entry_zip();
    let first = signed_zip(
        &harness,
        QUERY,
        archive.clone(),
        source_controls("source-only", false, "none"),
    )
    .await;
    assert_eq!(
        first.status(),
        StatusCode::OK,
        "{}",
        first.text().await.unwrap()
    );
    let cid = store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
        .await
        .unwrap();
    assert_eq!(cid.cid, "QmStagedZip");
    assert_eq!(cid.size, archive.len() as i64);
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    let replacement = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[],
        b"replacement".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::OK);
    let replay = signed_zip(
        &harness,
        QUERY,
        archive.clone(),
        source_controls("source-only", false, "none"),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()[http::header::ETAG], "\"QmStagedZip\"");
    let xml = replay.text().await.unwrap();
    assert!(
        xml.contains("<SourcePublished>true</SourcePublished>"),
        "{xml}"
    );
    assert!(xml.contains("<RootStatus>disabled</RootStatus>"));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
            .await
            .unwrap()
            .cid,
        "QmReplacement"
    );
    assert_eq!(harness.captured_add_file_bytes().len(), 2);
    let changed = signed_zip(
        &harness,
        QUERY,
        same_entry_different_zip(archive),
        source_controls("source-only", false, "none"),
    )
    .await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn source_only_preserves_versioning_contract_for_each_bucket_state() {
    use store::object_version::BucketVersioningState as State;
    for versioning in [State::Unversioned, State::Enabled, State::Suspended] {
        let harness = start_harness_with_root_default(
            KuboScript {
                add_replies: vec![AddReply::Ok("QmStagedZip")],
                cat_bodies: HashMap::new(),
            },
            true,
        )
        .await;
        if versioning != State::Unversioned {
            store::bucket::set_versioning_state(
                harness.state.store.db(),
                &harness.bucket,
                versioning,
            )
            .await
            .unwrap();
        }
        let response = signed_zip(
            &harness,
            QUERY,
            legal_single_entry_zip(),
            source_controls(&format!("versioning-{versioning:?}"), false, "none"),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        let replay = signed_zip(
            &harness,
            QUERY,
            legal_single_entry_zip(),
            source_controls(&format!("versioning-{versioning:?}"), false, "none"),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(
            replay
                .headers()
                .get("x-amz-version-id")
                .and_then(|value| value.to_str().ok()),
            match versioning {
                State::Unversioned => None,
                State::Enabled => replay
                    .headers()
                    .get("x-amz-version-id")
                    .and_then(|value| value.to_str().ok()),
                State::Suspended => Some("null"),
            }
        );
        if versioning == State::Enabled {
            assert!(
                uuid::Uuid::parse_str(replay.headers()["x-amz-version-id"].to_str().unwrap())
                    .is_ok()
            );
        }
        assert_eq!(
            object_version::Entity::find()
                .count(harness.state.store.db())
                .await
                .unwrap(),
            1
        );
        assert_eq!(harness.captured_add_file_bytes().len(), 1);
    }
}

#[tokio::test]
async fn source_and_entries_publish_separate_versions_with_real_source_receipt() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let archive = legal_single_entry_zip();
    let response = signed_zip(
        &harness,
        QUERY,
        archive.clone(),
        source_controls("source-and-entry", true, "none"),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let source = store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
        .await
        .unwrap();
    assert_eq!(source.cid, "QmStagedZip");
    assert_eq!(source.size, archive.len() as i64);
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        "QmExtracted"
    );
    let versions = object_version::Entity::find()
        .filter(object_version::Column::Key.eq(SOURCE))
        .all(harness.state.store.db())
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    let replay = signed_zip(
        &harness,
        QUERY,
        archive,
        source_controls("source-and-entry", true, "none"),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()[http::header::ETAG], "\"QmStagedZip\"");
    assert!(replay.headers().get("x-amz-version-id").is_some());
    assert!(
        replay
            .text()
            .await
            .unwrap()
            .contains("<SourcePublished>true</SourcePublished>")
    );
    assert_eq!(harness.captured_add_file_bytes().len(), 3);
}

async fn signed_zip(
    harness: &TestHarness,
    query: &[(&str, &str)],
    archive: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        query,
        archive,
        headers,
        "test",
    )
    .await
}

fn fixture() -> KuboScript {
    KuboScript {
        add_replies: vec![
            AddReply::Ok("QmExisting"),
            AddReply::Ok("QmStagedZip"),
            AddReply::Ok("QmExtracted"),
            AddReply::Ok("QmReplacement"),
        ],
        cat_bodies: HashMap::from([
            ("QmExisting".into(), b"old source bytes".to_vec()),
            ("QmStagedZip".into(), legal_single_entry_zip()),
            ("QmReplacement".into(), b"new source bytes".to_vec()),
        ]),
    }
}

async fn new_harness() -> TestHarness {
    // Root disabled so the contract assertions do not rely on a mock DAG builder.
    start_harness_with_root_default(fixture(), false).await
}

async fn new_rejection_harness() -> TestHarness {
    start_harness_with_root_default(
        KuboScript::repeated_add(
            "QmStagedZip",
            8,
            HashMap::from([("QmStagedZip".into(), legal_single_entry_zip())]),
        ),
        false,
    )
    .await
}

async fn seed_source(harness: &TestHarness) {
    store::bucket::set_versioning_state(
        harness.state.store.db(),
        &harness.bucket,
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-tagging", HeaderValue::from_static("retained=yes"));
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[],
        b"old source bytes".to_vec(),
        headers,
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[http::header::ETAG], "\"QmExisting\"");
    assert!(response.headers().get("x-amz-version-id").is_some());
}

async fn source_snapshot(
    harness: &TestHarness,
) -> (
    Vec<object::Model>,
    Vec<object_version::Model>,
    Vec<object_tag::Model>,
    Option<import_destination::Model>,
) {
    let db = harness.state.store.db();
    let objects = object::Entity::find()
        .filter(object::Column::Bucket.eq(&harness.bucket))
        .filter(object::Column::Key.eq(SOURCE))
        .order_by_asc(object::Column::Id)
        .all(db)
        .await
        .unwrap();
    let versions = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(&harness.bucket))
        .filter(object_version::Column::Key.eq(SOURCE))
        .order_by_asc(object_version::Column::Sequence)
        .all(db)
        .await
        .unwrap();
    let tags = object_tag::Entity::find()
        .filter(object_tag::Column::ObjectId.is_in(objects.iter().map(|obj| obj.id.clone())))
        .order_by_asc(object_tag::Column::Key)
        .all(db)
        .await
        .unwrap();
    let owner = import_destination::Entity::find_by_id((harness.bucket.clone(), SOURCE.into()))
        .one(db)
        .await
        .unwrap();
    (objects, versions, tags, owner)
}

async fn remote_counts(harness: &TestHarness) -> (u64, u64, u64) {
    let db = harness.state.store.db();
    (
        pin_job::Entity::find().count(db).await.unwrap(),
        pin_lease::Entity::find().count(db).await.unwrap(),
        pin_lease_target::Entity::find().count(db).await.unwrap(),
    )
}

fn assert_v2_ack(response: &reqwest::Response) {
    assert_eq!(response.status(), StatusCode::OK, "v2 must be accepted");
    assert!(
        response.headers().get(http::header::ETAG).is_none(),
        "no archive ETag"
    );
    assert!(
        response.headers().get("x-amz-version-id").is_none(),
        "no archive version"
    );
    assert!(response.headers().get("x-ipfs-s3-zip-batch-id").is_some());
}

#[tokio::test]
async fn signed_v2_extracted_only_preserves_existing_source_versions_tags_and_owner() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let before = source_snapshot(&harness).await;
    assert_eq!(before.0.len(), 1);
    assert_eq!(before.1.len(), 1);
    assert_eq!(before.2[0].key, "retained");
    let archive = legal_single_entry_zip();
    let sha = hex::encode(Sha256::digest(&archive));
    let response = signed_zip(&harness, QUERY, archive, controls("stable-v2-token")).await;
    assert_v2_ack(&response);
    let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let body = response.text().await.unwrap();
    assert!(body.contains("<ZipBatchResult>"), "distinct v2 XML: {body}");
    assert!(body.contains(&format!("<BatchId>{batch_id}</BatchId>")));
    assert!(body.contains("<SourcePublished>false</SourcePublished>"));
    assert!(body.contains(&format!("<InputSHA256>{sha}</InputSHA256>")));
    assert!(body.contains("<PublishedCount>1</PublishedCount>"));
    assert!(
        !body.contains("<ArchiveETag>"),
        "no fictitious archive ETag: {body}"
    );
    assert!(
        !body.contains("<VersionId>"),
        "no fictitious archive version: {body}"
    );
    assert_eq!(source_snapshot(&harness).await, before);
    let source_get = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(source_get.status(), StatusCode::OK);
    assert_eq!(source_get.headers()[http::header::ETAG], "\"QmExisting\"");
    let downloaded = source_get.bytes().await.unwrap();
    assert_eq!(&downloaded[..], b"old source bytes");
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        "QmExtracted"
    );
    let snapshot = store::zip::snapshot(harness.state.store.db(), &batch_id)
        .await
        .unwrap()
        .expect("batch receipt exists independent of source version");
    assert!(!snapshot.batch.source_published);
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(
        remote_counts(&harness).await,
        (0, 0, 0),
        "targets=none is local only"
    );
}

// ZIP EOCD comment changes complete input bytes, not the extracted file.
fn same_entry_different_zip(mut original: Vec<u8>) -> Vec<u8> {
    let len = original.len();
    assert_eq!(&original[len - 2..], &[0, 0]);
    original[len - 2] = 1;
    original.push(b'x');
    original
}

#[tokio::test]
async fn signed_v2_token_replays_original_batch_after_source_overwrite_but_rejects_changed_zip_bytes()
 {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let archive = legal_single_entry_zip();
    let first = signed_zip(
        &harness,
        QUERY,
        archive.clone(),
        controls("replay-zip-token"),
    )
    .await;
    assert_v2_ack(&first);
    let first_headers = first.headers().clone();
    let first_xml = first.text().await.unwrap();
    let replacement = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[],
        b"new source bytes".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::OK);
    let source_after_overwrite = source_snapshot(&harness).await;
    let versions = object_version::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let batches = zip_batch::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let adds = harness.captured_add_file_bytes().len();
    let replay = signed_zip(
        &harness,
        QUERY,
        archive.clone(),
        controls("replay-zip-token"),
    )
    .await;
    assert_v2_ack(&replay);
    assert_eq!(
        replay.headers()["x-ipfs-s3-zip-batch-id"],
        first_headers["x-ipfs-s3-zip-batch-id"]
    );
    assert_eq!(replay.text().await.unwrap(), first_xml);
    assert_eq!(source_snapshot(&harness).await, source_after_overwrite);
    assert_eq!(
        object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        versions
    );
    assert_eq!(
        zip_batch::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        batches
    );
    assert_eq!(
        harness.captured_add_file_bytes().len(),
        adds,
        "same bytes must not re-add to Kubo"
    );

    let changed = same_entry_different_zip(archive);
    assert_ne!(
        hex::encode(Sha256::digest(&changed)),
        hex::encode(Sha256::digest(legal_single_entry_zip()))
    );
    let conflict = signed_zip(&harness, QUERY, changed, controls("replay-zip-token")).await;
    assert_eq!(
        conflict.status(),
        StatusCode::CONFLICT,
        "same extracted entry is not same input"
    );
    assert_eq!(
        object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        versions
    );
    assert_eq!(
        zip_batch::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        batches
    );
    assert_eq!(remote_counts(&harness).await, (0, 0, 0));
}

fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

/// Same canonical header-SigV4 algorithm as support::sigv4::send_sigv4,
/// exposed here for a raw HTTP/1.1 request whose body stays deliberately open.
fn signed_wire_headers(
    harness: &TestHarness,
    query: &str,
    body: &[u8],
    headers: HeaderMap,
    extra_unsigned: Option<(&str, &str)>,
) -> String {
    let host = harness.endpoint.trim_start_matches("http://");
    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
    let payload_sha = hex::encode(Sha256::digest(body));
    let mut canonical = BTreeMap::<String, Vec<String>>::new();
    for (name, value) in &headers {
        canonical
            .entry(name.to_string())
            .or_default()
            .push(value.to_str().unwrap().to_owned());
    }
    canonical.insert("host".into(), vec![host.into()]);
    canonical.insert("x-amz-date".into(), vec![timestamp.clone()]);
    canonical.insert("x-amz-content-sha256".into(), vec![payload_sha.clone()]);
    let signed_names = canonical.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical_headers = canonical
        .iter()
        .map(|(name, values)| format!("{name}:{}\n", values.join(",")))
        .collect::<String>();
    let uri = format!("/{}/{SOURCE}", harness.bucket);
    let request =
        format!("PUT\n{uri}\n{query}\n{canonical_headers}\n{signed_names}\n{payload_sha}");
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(request.as_bytes()))
    );
    let date_key = hmac(b"AWS4test", date.as_bytes());
    let region = hmac(&date_key, b"us-east-1");
    let service = hmac(&region, b"s3");
    let signing = hmac(&service, b"aws4_request");
    let signature = hex::encode(hmac(&signing, to_sign.as_bytes()));
    let mut wire = format!("PUT {uri}?{query} HTTP/1.1\r\n");
    for (name, values) in canonical {
        for value in values {
            wire.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    wire.push_str(&format!(
        "Authorization: AWS4-HMAC-SHA256 Credential=test/{scope}, SignedHeaders={signed_names}, Signature={signature}\r\n"
    ));
    if let Some((name, value)) = extra_unsigned {
        wire.push_str(&format!("{name}: {value}\r\n"));
    }
    wire.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    wire
}

async fn reject_without_sending_body(
    harness: &TestHarness,
    query: &str,
    headers: HeaderMap,
    extra_unsigned: Option<(&str, &str)>,
) {
    let body = legal_single_entry_zip();
    let wire = signed_wire_headers(harness, query, &body, headers, extra_unsigned);
    let before_adds = harness.captured_add_file_bytes().len();
    let before_batches = zip_batch::Entity::find()
        .count(harness.state.store.db())
        .await
        .unwrap();
    let address = harness.endpoint.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(wire.as_bytes()).await.unwrap();
    let mut response = [0u8; 1024];
    let early = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read(&mut response),
    )
    .await;
    let status_line = match early {
        Ok(Ok(n)) => String::from_utf8_lossy(&response[..n]).into_owned(),
        Ok(Err(error)) => panic!("early HTTP read failed: {error}"),
        Err(_) => {
            // Complete the valid SigV4 payload before failing, so the handler
            // and WireMock can finish cleanly even on the known legacy path.
            stream.write_all(&body).await.unwrap();
            let mut remainder = Vec::new();
            stream.read_to_end(&mut remainder).await.unwrap();
            panic!(
                "no header rejection before body; after EOF: {}",
                String::from_utf8_lossy(&remainder)
            );
        }
    };
    assert!(
        status_line.starts_with("HTTP/1.1 400 "),
        "early HTTP status: {status_line}"
    );
    let mut rest = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        stream.read_to_end(&mut rest),
    )
    .await
    .expect("ZIP v2 header rejection must finish without waiting for body")
    .unwrap();
    let error_xml = format!("{status_line}{}", String::from_utf8_lossy(&rest));
    assert!(
        error_xml.contains("<Code>InvalidRequest</Code>") && error_xml.contains("ZIP"),
        "ZIP v2 parser rejection, not failed authentication: {error_xml}"
    );
    assert_eq!(
        harness.captured_add_file_bytes().len(),
        before_adds,
        "invalid controls cannot add to Kubo"
    );
    assert_eq!(
        zip_batch::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        before_batches
    );
}

#[tokio::test]
async fn raw_v2_sigv4_header_signer_authenticates_on_real_s3s() {
    let harness = new_rejection_harness().await;
    // Sanity-check the raw signer with a real valid authenticated request.
    // Legacy may still answer 200; this check proves subsequent 400s are not
    // caused by an invalid Authorization header.
    let valid = signed_wire_headers(
        &harness,
        "decompress-zip=out%2F",
        &legal_single_entry_zip(),
        controls("raw-valid-token"),
        None,
    );
    let mut stream = tokio::net::TcpStream::connect(harness.endpoint.trim_start_matches("http://"))
        .await
        .unwrap();
    stream.write_all(valid.as_bytes()).await.unwrap();
    stream.write_all(&legal_single_entry_zip()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "raw signed control request must authenticate: {}",
        String::from_utf8_lossy(&response)
    );
}

#[tokio::test]
async fn signed_v2_missing_token_rejects_before_body_and_without_kubo_add() {
    let harness = new_rejection_harness().await;
    let mut missing = controls("missing-token");
    missing.remove("x-ipfs3-zip-token");
    reject_without_sending_body(&harness, "decompress-zip=out%2F", missing, None).await;
}

#[tokio::test]
async fn signed_v2_duplicate_token_rejects_before_body_and_without_kubo_add() {
    let harness = new_rejection_harness().await;
    let mut duplicate = controls("duplicate-token");
    duplicate.append(
        "x-ipfs3-zip-token",
        HeaderValue::from_static("second-token"),
    );
    reject_without_sending_body(&harness, "decompress-zip=out%2F", duplicate, None).await;
}

#[tokio::test]
async fn sigv4_valid_but_unsigned_v2_token_rejects_before_body_and_without_kubo_add() {
    let harness = new_rejection_harness().await;
    let mut missing = controls("unsigned-token");
    missing.remove("x-ipfs3-zip-token");
    // The helper signs the request without the token. Add that control only on
    // the wire, outside SignedHeaders: valid SigV4 auth must not allow it.
    reject_without_sending_body(
        &harness,
        "decompress-zip=out%2F",
        missing,
        Some(("x-ipfs3-zip-token", "unsigned-token")),
    )
    .await;
}

#[tokio::test]
async fn signed_v2_result_false_rejects_before_body_and_without_kubo_add() {
    let harness = new_rejection_harness().await;
    reject_without_sending_body(
        &harness,
        "decompress-zip=out%2F&decompress-zip-result=false",
        controls("result-disabled"),
        None,
    )
    .await;
}

#[tokio::test]
async fn unsigned_source_content_type_rejects_before_body_and_without_mutation() {
    let harness = new_rejection_harness().await;
    let before = source_snapshot(&harness).await;
    reject_without_sending_body(
        &harness,
        "decompress-zip=out%2F",
        source_controls("unsigned-content-type", false, "none"),
        Some(("Content-Type", "application/json")),
    )
    .await;
    assert_eq!(source_snapshot(&harness).await, before);
    let row = harness
        .state
        .store
        .db()
        .query_one(Statement::from_string(
            harness.state.store.db().get_database_backend(),
            "SELECT COUNT(*) AS total FROM zip_v2_executions",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "total").unwrap(), 0);
    assert_eq!(remote_counts(&harness).await, (0, 0, 0));
}

#[tokio::test]
async fn signed_source_content_type_is_published_replayed_and_contract_bound() {
    let harness = new_rejection_harness().await;
    let archive = legal_single_entry_zip();
    let mut headers = source_controls("signed-content-type", false, "none");
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    let first = signed_zip(&harness, QUERY, archive.clone(), headers.clone()).await;
    let status = first.status();
    let first_headers = first.headers().clone();
    let first_xml = first.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{first_xml}");
    let source = store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
        .await
        .unwrap();
    assert_eq!(source.content_type.as_deref(), Some("application/zip"));
    let before = source_snapshot(&harness).await;
    let adds = harness.captured_add_file_bytes().len();
    let replay = signed_zip(&harness, QUERY, archive.clone(), headers.clone()).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(
        replay.headers()["x-ipfs-s3-zip-batch-id"],
        first_headers["x-ipfs-s3-zip-batch-id"]
    );
    assert_eq!(replay.text().await.unwrap(), first_xml);
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let changed = signed_zip(&harness, QUERY, archive.clone(), headers.clone()).await;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    headers.remove(http::header::CONTENT_TYPE);
    let removed = signed_zip(&harness, QUERY, archive, headers).await;
    assert_eq!(removed.status(), StatusCode::CONFLICT);
    assert_eq!(source_snapshot(&harness).await, before);
    assert_eq!(harness.captured_add_file_bytes().len(), adds);
}

async fn assert_source_guard_survives_v2(harness: &TestHarness) {
    let before_source = source_snapshot(harness).await;
    let job = import_job::Entity::find_by_id("S-import")
        .one(harness.state.store.db())
        .await
        .unwrap();
    let claims = import_prefix_claim::Entity::find()
        .filter(import_prefix_claim::Column::JobId.eq("S-import"))
        .all(harness.state.store.db())
        .await
        .unwrap();
    let response = signed_zip(
        harness,
        QUERY,
        legal_single_entry_zip(),
        controls("guard-cannot-supersede-S"),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "output exact admission must not supersede source S"
    );
    assert_eq!(source_snapshot(harness).await, before_source);
    assert_eq!(
        import_job::Entity::find_by_id("S-import")
            .one(harness.state.store.db())
            .await
            .unwrap(),
        job
    );
    assert_eq!(
        import_prefix_claim::Entity::find()
            .filter(import_prefix_claim::Column::JobId.eq("S-import"))
            .all(harness.state.store.db())
            .await
            .unwrap(),
        claims
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    assert_eq!(remote_counts(harness).await, (0, 0, 0));
}

#[tokio::test]
async fn active_source_import_with_overlapping_prefix_is_not_indirectly_superseded_by_v2_output() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    ownership::submit(
        harness.state.store.db(),
        NewImportJob {
            id: "S-import".into(),
            bucket: harness.bucket.clone(),
            key: SOURCE.into(),
            source: ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".into(),
            ),
            request_fingerprint: "sha256:source-S".into(),
            client_token: None,
            object_content_type: None,
            metadata: HashMap::new(),
            tags: vec![],
            decompress_prefix: Some("out/".into()),
        },
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    let source_owner = source_snapshot(&harness)
        .await
        .3
        .expect("S owns source key");
    assert_eq!(source_owner.owner_job_id.as_deref(), Some("S-import"));
    assert_source_guard_survives_v2(&harness).await;
}

#[tokio::test]
async fn active_source_prefix_mutation_guard_is_not_invalidated_by_v2_output_admission() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let guard = ownership::admit_content_and_prefix_mutation(
        harness.state.store.db(),
        &harness.bucket,
        SOURCE,
        "out/",
        SupersedeReason::DecompressZip,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    let before = source_snapshot(&harness).await;
    let response = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("source-mutation-S"),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::CONFLICT,
        "v2 cannot steal S's prefix guard"
    );
    assert_eq!(source_snapshot(&harness).await, before);
    ownership::renew_standard_mutation(harness.state.store.db(), &guard)
        .await
        .unwrap();
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    assert_eq!(remote_counts(&harness).await, (0, 0, 0));
}

#[tokio::test]
async fn v2_waits_for_the_last_body_byte_and_rejects_a_changed_signed_tail_before_binding() {
    let harness = new_rejection_harness().await;
    let original = legal_single_entry_zip();
    let wire = signed_wire_headers(
        &harness,
        "decompress-zip=out%2F",
        &original,
        controls("late-body-error"),
        None,
    );
    let mut stream = tokio::net::TcpStream::connect(harness.endpoint.trim_start_matches("http://"))
        .await
        .unwrap();
    stream.write_all(wire.as_bytes()).await.unwrap();
    stream
        .write_all(&original[..original.len() - 1])
        .await
        .unwrap();
    let execution =
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            loop {
                if let Some(row) = harness.state.store.db().query_one(Statement::from_string(
                harness.state.store.db().get_database_backend(),
                "SELECT id,input_sha256 FROM zip_v2_executions WHERE token='late-body-error'",
            )).await.unwrap() { break row; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("admission precedes body EOF");
    let id: String = execution.try_get("", "id").unwrap();
    assert!(
        execution
            .try_get::<Option<String>>("", "input_sha256")
            .unwrap()
            .is_none()
    );
    assert!(
        store::zip::snapshot(harness.state.store.db(), &id)
            .await
            .unwrap()
            .is_none()
    );
    // Same content length and originally signed payload hash, but changed tail.
    stream
        .write_all(&[original[original.len() - 1] ^ 1])
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 400 "),
        "{}",
        String::from_utf8_lossy(&response)
    );
    let execution = store::pinning::publication::v2_execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        execution.input_sha256.is_none(),
        "bad tail cannot bind an input identity"
    );
    assert!(
        store::zip::snapshot(harness.state.store.db(), &id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn signed_batch_status_is_independent_of_later_source_versions_and_scoped_to_exact_key() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let first = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("status-token"),
    )
    .await;
    assert_v2_ack(&first);
    let id = first.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let source_overwrite = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[],
        b"new source bytes".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(source_overwrite.status(), StatusCode::OK);
    let query = &[("ipfs3-zip-batch", id.as_str())];
    let found = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(found.status(), StatusCode::OK);
    let xml = found.text().await.unwrap();
    assert!(xml.contains("<ZipBatchResult>"));
    assert!(xml.contains(&format!("<BatchId>{id}</BatchId>")));
    assert!(xml.contains("<SourcePublished>false</SourcePublished>"));
    let wrong_key = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "other.zip",
        query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(wrong_key.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn signed_legacy_batch_status_uses_only_current_verified_adopted_root() {
    use sea_orm::{ActiveModelTrait, IntoActiveModel, Set};
    use store::entities::{zip_root_build, zip_root_reference};

    let harness = new_rejection_harness().await;
    let db = harness.state.store.db();
    let id = uuid::Uuid::new_v4().to_string();
    let batch = store::zip::admit(
        db,
        &store::zip::BatchAdmission {
            id: id.clone(),
            owner: "test".into(),
            source: "direct".into(),
            token: "legacy-root-status".into(),
            fingerprint: "legacy-input".into(),
            bucket: harness.bucket.clone(),
            archive_key: SOURCE.into(),
            input_identity: "QmArchive".into(),
            captured_options: "{}".into(),
        },
    )
    .await
    .unwrap();
    let mut batch = batch.into_active_model();
    batch.state = Set("published".into());
    batch.terminal_result = Set(Some("{}".into()));
    batch.root_status = Set("partial".into());
    batch.root_cid = Set(Some("QmLegacyRoot".into()));
    batch.root_revision = Set(2);
    batch.root_epoch = Set(3);
    let batch = batch.update(db).await.unwrap();
    let now = chrono::Utc::now();
    let build = zip_root_build::ActiveModel {
        batch_id: Set(id.clone()),
        revision: Set(2),
        epoch: Set(3),
        worker: Set("root-worker".into()),
        lease_until: Set(now),
        status: Set("verified".into()),
        error_code: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .unwrap();
    let reference = zip_root_reference::ActiveModel {
        batch_id: Set(id.clone()),
        revision: Set(2),
        epoch: Set(3),
        node_identity: Set("local-kubo".into()),
        tier: Set("hot".into()),
        cid: Set("QmLegacyRoot".into()),
        state: Set("adopted".into()),
        verification_receipt: Set(Some("committed-proof".into())),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(db)
    .await
    .unwrap();
    for (revision, epoch, verified, adopted) in [
        (2, 3, true, true),
        (3, 3, true, true),
        (2, 4, true, true),
        (2, 3, false, true),
        (2, 3, true, false),
    ] {
        let mut current = batch.clone().into_active_model();
        current.root_revision = Set(revision);
        current.root_epoch = Set(epoch);
        current.update(db).await.unwrap();
        let mut current = build.clone().into_active_model();
        current.status = Set(if verified { "verified" } else { "unknown" }.into());
        current.update(db).await.unwrap();
        let mut current = reference.clone().into_active_model();
        current.state = Set(if adopted { "adopted" } else { "retained" }.into());
        current.verification_receipt = Set(adopted.then(|| "committed-proof".into()));
        current.update(db).await.unwrap();
        let response = support::sigv4::send_sigv4(
            reqwest::Method::GET,
            &harness.endpoint,
            &harness.bucket,
            SOURCE,
            &[("ipfs3-zip-batch", &id)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "partial");
        let cid = response
            .headers()
            .get("x-ipfs-s3-zip-root-cid")
            .map(|value| value.to_str().unwrap().to_owned());
        let xml = response.text().await.unwrap();
        assert!(xml.contains("<RootStatus>partial</RootStatus>"), "{xml}");
        let authorized = revision == 2 && epoch == 3 && verified && adopted;
        assert_eq!(cid.as_deref(), authorized.then_some("QmLegacyRoot"));
        assert_eq!(
            xml.contains("<RootCID>QmLegacyRoot</RootCID>"),
            authorized,
            "{xml}"
        );
        assert_eq!(xml.contains("<RootCID>"), cid.is_some(), "{xml}");
    }
    assert!(harness.kubo.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn v2_default_on_local_root_failure_still_publishes_outputs_without_exposing_a_candidate() {
    const VALID_LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmStagedZip"), AddReply::Ok(VALID_LEAF)],
            cat_bodies: HashMap::from([("QmStagedZip".into(), legal_single_entry_zip())]),
        },
        true,
    )
    .await;
    let response = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("root-failure-v2"),
    )
    .await;
    assert_v2_ack(&response);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "failed");
    assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
    let id = response.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<RootStatus>failed</RootStatus>")
    );
    let batch = store::zip::snapshot(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.batch.root_status, "failed");
    assert!(!batch.batch.source_published);
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        VALID_LEAF
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
            .await
            .is_err()
    );
    assert!(
        !harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/api/v0/pin/rm")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successor_takes_output_after_manifest_admission_and_batch_never_downgrades_guard_loss_to_root_warning()
 {
    use std::sync::mpsc;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    const VALID_LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmStagedZip"),
            AddReply::Ok(VALID_LEAF),
            AddReply::Ok("QmReplacement"),
        ],
        cat_bodies: HashMap::from([("QmStagedZip".into(), legal_single_entry_zip())]),
    };
    let harness = start_harness_with_root_default(script, true).await;
    let (reached_tx, reached_rx) = mpsc::channel();
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(move |_: &wiremock::Request| {
            reached_tx.send(()).unwrap();
            // WireMock Respond is synchronous; blocking it also blocks the
            // successor's Kubo add. Delay the response asynchronously instead.
            ResponseTemplate::new(503).set_delay(std::time::Duration::from_secs(5))
        })
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&harness.kubo)
        .await;
    let endpoint = harness.endpoint.clone();
    let bucket = harness.bucket.clone();
    let first = tokio::spawn(async move {
        support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &endpoint,
            &bucket,
            SOURCE,
            QUERY,
            legal_single_entry_zip(),
            controls("after-admission-race"),
            "test",
        )
        .await
    });
    tokio::task::spawn_blocking(move || {
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(8))
            .unwrap()
    })
    .await
    .unwrap();
    let batch = zip_batch::Entity::find()
        .one(harness.state.store.db())
        .await
        .unwrap()
        .unwrap();
    let execution =
        store::pinning::publication::v2_execution::read(harness.state.store.db(), &batch.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        execution.state, "admitted",
        "the race happens after exact output guards are acquired"
    );
    let successor = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        OUTPUT,
        &[],
        b"successor".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(successor.status(), StatusCode::OK);
    let failed = first.await.unwrap();
    assert_ne!(
        failed.status(),
        StatusCode::OK,
        "lost guard cannot publish a partial batch"
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        "QmReplacement"
    );
    let snapshot = store::zip::snapshot(harness.state.store.db(), &batch.id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        snapshot.batch.state, "published",
        "a root warning cannot hide lost ownership"
    );
}

#[tokio::test]
async fn mirror_failure_after_exact_admission_rolls_back_guards_and_both_manifests() {
    let harness = new_harness().await;
    seed_source(&harness).await;
    let source_guard = ownership::admit_content_and_prefix_mutation(
        harness.state.store.db(),
        &harness.bucket,
        SOURCE,
        "unrelated/",
        SupersedeReason::DecompressZip,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    let before = source_snapshot(&harness).await;
    let db = harness.state.store.db();
    // Fail only if execution has first acquired the complete exact guard set.
    // A mirror inserted before admission cannot satisfy this trigger.
    db.execute_unprepared("CREATE TRIGGER zip_v2_mirror_reject BEFORE INSERT ON zip_manifest_entries WHEN EXISTS (SELECT 1 FROM zip_v2_targets WHERE batch_id=NEW.batch_id) BEGIN SELECT RAISE(ABORT, 'mirror failure after admission'); END")
        .await.unwrap();
    let response = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("mirror-rollback-v2"),
    )
    .await;
    assert_ne!(
        response.status(),
        StatusCode::OK,
        "mirror SQL failure must roll back the whole admission"
    );
    assert_eq!(source_snapshot(&harness).await, before);
    ownership::renew_standard_mutation(db, &source_guard)
        .await
        .unwrap();
    assert!(
        store::object::get_latest(db, &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    for table in [
        "zip_batches",
        "zip_manifest_entries",
        "zip_v2_manifest",
        "zip_v2_targets",
    ] {
        let row = db
            .query_one(Statement::from_string(
                db.get_database_backend(),
                format!("SELECT COUNT(*) AS total FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.try_get::<i64>("", "total").unwrap(),
            0,
            "{table} must roll back"
        );
    }
    let owners = import_destination::Entity::find()
        .filter(import_destination::Column::Bucket.eq(&harness.bucket))
        .filter(import_destination::Column::Key.eq(OUTPUT))
        .all(db)
        .await
        .unwrap();
    assert!(
        owners.is_empty(),
        "no output mutation token can survive a failed mirror"
    );
    db.execute_unprepared("DROP TRIGGER zip_v2_mirror_reject")
        .await
        .unwrap();
}

#[tokio::test]
async fn zero_successes_commit_failed_terminal_without_source_or_fictitious_empty_root() {
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmStagedZip"),
                AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add refused"),
            ],
            cat_bodies: HashMap::from([("QmStagedZip".into(), legal_single_entry_zip())]),
        },
        false,
    )
    .await;
    let first = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("zero-output-v2"),
    )
    .await;
    assert_ne!(first.status(), StatusCode::OK);
    let id: String = harness
        .state
        .store
        .db()
        .query_one(Statement::from_string(
            harness.state.store.db().get_database_backend(),
            "SELECT id FROM zip_v2_executions WHERE token='zero-output-v2'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "id")
        .unwrap();
    let execution = store::pinning::publication::v2_execution::read(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        execution.state, "completed",
        "terminal failure must be replayable without re-add"
    );
    let batch = store::zip::snapshot(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.batch.state, "published");
    assert!(!batch.batch.source_published);
    assert!(batch.batch.root_cid.is_none());
    assert!(
        batch
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, SOURCE)
            .await
            .is_err()
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    let adds = harness.captured_add_file_bytes().len();
    let replay = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("zero-output-v2"),
    )
    .await;
    assert_ne!(
        replay.status(),
        StatusCode::OK,
        "failed terminal is never successful replay"
    );
    assert_eq!(harness.captured_add_file_bytes().len(), adds);
    let query = &[("ipfs3-zip-batch", id.as_str())];
    let status = support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    assert!(
        status
            .text()
            .await
            .unwrap()
            .contains("<State>failed</State>")
    );
}

#[tokio::test]
async fn default_on_root_with_zero_outputs_never_builds_or_exposes_an_empty_directory() {
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmStagedZip"),
                AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add refused"),
            ],
            cat_bodies: HashMap::from([("QmStagedZip".into(), legal_single_entry_zip())]),
        },
        true,
    )
    .await;
    let response = signed_zip(
        &harness,
        QUERY,
        legal_single_entry_zip(),
        controls("empty-root-on-v2"),
    )
    .await;
    assert_ne!(response.status(), StatusCode::OK);
    let row = harness
        .state
        .store
        .db()
        .query_one(Statement::from_string(
            harness.state.store.db().get_database_backend(),
            "SELECT id FROM zip_v2_executions WHERE token='empty-root-on-v2'",
        ))
        .await
        .unwrap()
        .unwrap();
    let id: String = row.try_get("", "id").unwrap();
    let snapshot = store::zip::snapshot(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "empty");
    assert!(snapshot.batch.root_cid.is_none());
    assert!(!snapshot.batch.source_published);
    assert!(
        harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .all(|request| !request.url.path().starts_with("/api/v0/files/"))
    );
}

struct RuleHarness {
    endpoint: String,
    bucket: String,
    state: std::sync::Arc<ipfs_s3_gateway::state::AppState>,
    _kubo: wiremock::MockServer,
    _server: support::decompress::S3ServerHandle,
}

async fn rule_harness(
    script: KuboScript,
    distinct_policies: bool,
    private_deny: bool,
) -> RuleHarness {
    let kubo = start_kubo_harness(script).await;
    let bucket = "test-bkt".to_owned();
    let raw = format!(
        r#"
        [kubo]
        rpc_url = {:?}
        [storage]
        database_url = "sqlite::memory:"
        [decompress_zip]
        unixfs_directory_root = false
        [[pinning.providers]]
        name = "alpha"
        kind = "noop"
        priority = 1
        max_bytes = 1000000
        max_pins = 100
        [[pinning.providers]]
        name = "beta"
        kind = "noop"
        priority = 2
        max_bytes = 1000000
        max_pins = 100
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "out/"
        trigger = "always"
        provider_mode = "one"
        providers = ["alpha"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
        [[pinning.policies]]
        bucket = "test-bkt"
        prefix = "out/"
        trigger = "always"
        provider_mode = "one"
        providers = ["beta"]
        default_duration = "1h"
        max_duration = "2h"
        allow_decompressed = true
    "#,
        kubo.server.uri()
    );
    let mut cfg: Config = toml::from_str(&raw).unwrap();
    let normalized =
        normalize_validated_config(ValidatedPinningConfig::from_config(&cfg, |_| None).unwrap())
            .unwrap();
    let mut rules = vec![ZipOutputRuleConfig {
        name: "first".into(),
        priority: 10,
        bucket: bucket.clone(),
        prefix: "out/".into(),
        effect: ZipRuleEffect::Allow,
        policy_id: Some(normalized.policies[0].identity.clone()),
    }];
    if distinct_policies {
        rules[0].prefix = "out/first.txt".into();
        rules.push(ZipOutputRuleConfig {
            name: "second".into(),
            priority: 11,
            bucket: bucket.clone(),
            prefix: "out/second.txt".into(),
            effect: ZipRuleEffect::Allow,
            policy_id: Some(normalized.policies[1].identity.clone()),
        });
    }
    if private_deny {
        rules.push(ZipOutputRuleConfig {
            name: "private".into(),
            priority: 99,
            bucket: bucket.clone(),
            prefix: "out/file.txt".into(),
            effect: ZipRuleEffect::Deny,
            policy_id: None,
        });
    }
    cfg.decompress_zip.pin_output_rules = rules;
    let state = ipfs_s3_gateway::state::AppState::new(&cfg).await.unwrap();
    store::bucket::create(state.store.db(), &bucket, None)
        .await
        .unwrap();
    let observed = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let server = support::decompress::start_s3_server(state.clone(), observed).await;
    RuleHarness {
        endpoint: server.endpoint.clone(),
        bucket,
        state,
        _kubo: kubo.server,
        _server: server,
    }
}

fn remote_controls(token: &str) -> HeaderMap {
    let mut headers = controls(token);
    headers.insert("x-ipfs3-zip-targets", HeaderValue::from_static("extracted"));
    headers
}

#[tokio::test]
async fn signed_private_output_rule_denies_remote_pin_without_dropping_local_object() {
    let harness = rule_harness(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmShared")],
            cat_bodies: HashMap::from([("QmArchive".into(), legal_single_entry_zip())]),
        },
        false,
        true,
    )
    .await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        QUERY,
        legal_single_entry_zip(),
        remote_controls("private-deny-v2"),
        "test",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        "QmShared"
    );
    assert_eq!(
        pin_job::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pin_lease::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        remote_pin::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn signed_two_outputs_use_independent_rules_and_same_cid_dedups_remote_target() {
    let script = || KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmShared"),
            AddReply::Ok("QmShared"),
        ],
        cat_bodies: HashMap::from([("QmArchive".into(), legal_two_entry_zip())]),
    };
    let distinct = rule_harness(script(), true, false).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &distinct.endpoint,
        &distinct.bucket,
        SOURCE,
        QUERY,
        legal_two_entry_zip(),
        remote_controls("distinct-output-rules"),
        "test",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let leases = pin_lease::Entity::find()
        .all(distinct.state.store.db())
        .await
        .unwrap();
    assert_eq!(leases.len(), 2);
    assert_ne!(
        leases[0].policy_id, leases[1].policy_id,
        "each full output key has its own policy"
    );
    assert_eq!(
        remote_pin::Entity::find()
            .count(distinct.state.store.db())
            .await
            .unwrap(),
        2
    );

    let shared = rule_harness(script(), false, false).await;
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &shared.endpoint,
        &shared.bucket,
        SOURCE,
        QUERY,
        legal_two_entry_zip(),
        remote_controls("shared-output-cid"),
        "test",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(
        pin_lease::Entity::find()
            .count(shared.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(shared.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        remote_pin::Entity::find()
            .count(shared.state.store.db())
            .await
            .unwrap(),
        1
    );
    let jobs = pin_job::Entity::find()
        .all(shared.state.store.db())
        .await
        .unwrap();
    assert!(
        jobs.iter()
            .all(|job| job.provider == "alpha" && job.cid == "QmShared")
    );
    assert_eq!(
        jobs.iter().filter(|job| job.operation == "submit").count(),
        1,
        "two output leases must not create two outbound submit jobs"
    );
    assert!(
        jobs.iter()
            .all(|job| matches!(job.operation.as_str(), "submit" | "reconcile")),
        "the shared CID may also need a remote-epoch reconcile job"
    );
}

#[tokio::test]
async fn signed_remote_route_drift_during_publication_rolls_back_versions_and_remote_intents() {
    let harness = rule_harness(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmShared")],
            cat_bodies: HashMap::from([("QmArchive".into(), legal_single_entry_zip())]),
        },
        false,
        false,
    )
    .await;
    let db = harness.state.store.db();
    let original = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT snapshot FROM pin_provider_routes ORDER BY provider LIMIT 1",
        ))
        .await
        .unwrap()
        .unwrap();
    let original: String = original.try_get("", "snapshot").unwrap();
    // Trigger runs *inside* the publication transaction after the preflight;
    // provider route verification must reject it, with every write rolled back.
    db.execute_unprepared("CREATE TRIGGER zip_v2_route_drift AFTER INSERT ON object_versions WHEN NEW.key='out/file.txt' BEGIN UPDATE pin_provider_routes SET snapshot='{}'; END")
        .await.unwrap();
    let response = support::sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        QUERY,
        legal_single_entry_zip(),
        remote_controls("route-drift-v2"),
        "test",
    )
    .await;
    assert_ne!(
        response.status(),
        StatusCode::OK,
        "route drift must reject the entire transaction"
    );
    assert!(
        store::object::get_latest(db, &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    assert_eq!(object_version::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(remote_pin::Entity::find().count(db).await.unwrap(), 0);
    assert_eq!(pin_lease::Entity::find().count(db).await.unwrap(), 0);
    let unchanged: String = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT snapshot FROM pin_provider_routes ORDER BY provider LIMIT 1",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "snapshot")
        .unwrap();
    assert_eq!(
        unchanged, original,
        "route drift cannot persist on rollback"
    );
    db.execute_unprepared("DROP TRIGGER zip_v2_route_drift")
        .await
        .unwrap();
}

#[tokio::test]
async fn stale_captured_remote_rules_reject_a_pending_retry_before_another_kubo_add() {
    let harness = rule_harness(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmShared")],
            cat_bodies: HashMap::from([("QmArchive".into(), legal_single_entry_zip())]),
        },
        false,
        false,
    )
    .await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER zip_v2_stale_revision_setup BEFORE INSERT ON zip_manifest_entries WHEN EXISTS (SELECT 1 FROM zip_v2_targets WHERE batch_id=NEW.batch_id) BEGIN SELECT RAISE(ABORT, 'force pending retry'); END")
        .await.unwrap();
    let send = || {
        support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &harness.bucket,
            SOURCE,
            QUERY,
            legal_single_entry_zip(),
            remote_controls("rule-revision-retry"),
            "test",
        )
    };
    let first = send().await;
    assert_ne!(first.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER zip_v2_stale_revision_setup")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT id,captured_options FROM zip_v2_executions WHERE token='rule-revision-retry'",
        ))
        .await
        .unwrap()
        .unwrap();
    let id: String = row.try_get("", "id").unwrap();
    let mut captured: serde_json::Value =
        serde_json::from_str(&row.try_get::<String>("", "captured_options").unwrap()).unwrap();
    captured["rule_revision"] = serde_json::Value::String("revoked".into());
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE zip_v2_executions SET captured_options=?,lease_until='2000-01-01T00:00:00Z' WHERE id=?",
        [captured.to_string().into(), id.into()])).await.unwrap();
    let add_count = harness
        ._kubo
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/add")
        .count();
    let retry = send().await;
    assert_eq!(retry.status(), StatusCode::CONFLICT);
    let final_count = harness
        ._kubo
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/add")
        .count();
    assert_eq!(
        final_count, add_count,
        "stale policy must be rejected before any second upload"
    );
}

#[tokio::test]
async fn local_only_pending_retry_ignores_unrelated_rule_revision() {
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmStagedZip"), AddReply::Ok("QmStagedZip")],
            cat_bodies: HashMap::new(),
        },
        true,
    )
    .await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER zip_v2_pending_rollback BEFORE INSERT ON zip_batches BEGIN SELECT RAISE(ABORT, 'force pending retry'); END")
        .await
        .unwrap();
    let archive = legal_single_entry_zip();
    let send = || {
        signed_zip(
            &harness,
            QUERY,
            archive.clone(),
            source_controls("local-rule-retry", false, "none"),
        )
    };
    assert_ne!(send().await.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER zip_v2_pending_rollback")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT id,captured_options FROM zip_v2_executions WHERE token='local-rule-retry'",
        ))
        .await
        .unwrap()
        .unwrap();
    let id: String = row.try_get("", "id").unwrap();
    let mut captured: serde_json::Value =
        serde_json::from_str(&row.try_get::<String>("", "captured_options").unwrap()).unwrap();
    captured["rule_revision"] = serde_json::Value::String("unrelated-change".into());
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE zip_v2_executions SET captured_options=?,lease_until='2000-01-01T00:00:00Z' WHERE id=?",
        [captured.to_string().into(), id.into()],
    ))
    .await
    .unwrap();
    let response = send().await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, SOURCE)
            .await
            .unwrap()
            .cid,
        "QmStagedZip"
    );
}

#[tokio::test]
async fn admitted_batch_retry_reuses_manifest_and_guards_after_publication_rollback() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmStagedZip"), AddReply::Ok("QmShared")],
            cat_bodies: HashMap::from([("QmStagedZip".into(), archive.clone())]),
        },
        false,
    )
    .await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER zip_v2_publish_rollback BEFORE INSERT ON object_versions WHEN NEW.key='out/file.txt' BEGIN SELECT RAISE(ABORT, 'force publication rollback'); END")
        .await
        .unwrap();
    let send = || signed_zip(&harness, QUERY, archive.clone(), controls("admitted-retry"));
    assert_ne!(send().await.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER zip_v2_publish_rollback")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT id,state FROM zip_v2_executions WHERE token='admitted-retry'",
        ))
        .await
        .unwrap()
        .unwrap();
    let id: String = row.try_get("", "id").unwrap();
    assert_eq!(row.try_get::<String>("", "state").unwrap(), "admitted");
    let before = harness.kubo.received_requests().await.unwrap().len();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE zip_v2_executions SET lease_until='2000-01-01T00:00:00Z' WHERE id=?",
        [id.into()],
    ))
    .await
    .unwrap();
    let retry = send().await;
    let status = retry.status();
    let body = retry.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        "QmShared"
    );
    assert!(
        store::object::get_latest(db, &harness.bucket, SOURCE)
            .await
            .is_err()
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before
    );
}

#[tokio::test]
async fn admitted_source_batch_retry_reuses_original_source_guard_after_rollback() {
    let archive = legal_single_entry_zip();
    let harness = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmStagedZip")],
            cat_bodies: HashMap::new(),
        },
        false,
    )
    .await;
    let db = harness.state.store.db();
    db.execute_unprepared("CREATE TRIGGER zip_v2_source_rollback BEFORE INSERT ON object_versions WHEN NEW.key='source.zip' BEGIN SELECT RAISE(ABORT, 'force source publication rollback'); END")
        .await
        .unwrap();
    let send = || {
        signed_zip(
            &harness,
            QUERY,
            archive.clone(),
            source_controls("admitted-source-retry", false, "none"),
        )
    };
    assert_ne!(send().await.status(), StatusCode::OK);
    db.execute_unprepared("DROP TRIGGER zip_v2_source_rollback")
        .await
        .unwrap();
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT id,state FROM zip_v2_executions WHERE token='admitted-source-retry'",
        ))
        .await
        .unwrap()
        .unwrap();
    let id: String = row.try_get("", "id").unwrap();
    assert_eq!(row.try_get::<String>("", "state").unwrap(), "admitted");
    let before = harness.kubo.received_requests().await.unwrap().len();
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "UPDATE zip_v2_executions SET lease_until='2000-01-01T00:00:00Z' WHERE id=?",
        [id.into()],
    ))
    .await
    .unwrap();
    let retry = send().await;
    let status = retry.status();
    let body = retry.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, SOURCE)
            .await
            .unwrap()
            .cid,
        "QmStagedZip"
    );
    assert!(
        store::object::get_latest(db, &harness.bucket, OUTPUT)
            .await
            .is_err()
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before
    );
}
