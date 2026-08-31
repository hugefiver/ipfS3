#[allow(dead_code)]
mod support;

use http::{HeaderMap, HeaderValue, header};
use s3::{
    bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, error::S3Error,
    region::Region,
};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use support::sigv4::send_sigv4;

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const LIFECYCLE_RACE_TIMEOUT: Duration = Duration::from_secs(60);
const IMPORT_TIMEOUT: Duration = Duration::from_secs(30);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn endpoint_from_env(name: &str) -> String {
    let endpoint = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    let endpoint = endpoint.trim_end_matches('/').to_owned();
    assert!(
        endpoint.starts_with("http://") || endpoint.starts_with("https://"),
        "{name} must be an HTTP(S) URL"
    );
    assert!(
        !endpoint.to_ascii_lowercase().contains("localhost"),
        "{name} must not use localhost"
    );
    endpoint
}

fn multi_gateway_endpoints() -> (String, String, String, String) {
    (
        endpoint_from_env("IPFS_S3_MULTI_GATEWAY_A_ENDPOINT"),
        endpoint_from_env("IPFS_S3_MULTI_GATEWAY_B_ENDPOINT"),
        endpoint_from_env("IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT"),
        endpoint_from_env("IPFS_S3_MULTI_GATEWAY_KUBO_URL"),
    )
}

fn test_credentials() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None).unwrap()
}

fn region_at(endpoint: &str) -> Region {
    Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

fn bucket_at(endpoint: &str, name: &str) -> Box<Bucket> {
    Bucket::new(name, region_at(endpoint), test_credentials())
        .unwrap()
        .with_path_style()
}

async fn s3_call<T, F>(label: &str, future: F) -> Result<T, S3Error>
where
    F: Future<Output = Result<T, S3Error>>,
{
    tokio::time::timeout(S3_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("S3 operation timed out: {label}"))
}

async fn http_call<T, F>(label: &str, future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(HTTP_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("HTTP operation timed out: {label}"))
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("mg-{scenario}-{}-{nanos:x}-{counter:x}", std::process::id());
    assert!(name.len() <= 63, "bucket name exceeds 63 characters");
    name
}

fn etag(headers: &std::collections::HashMap<String, String>) -> String {
    headers
        .get("etag")
        .or_else(|| headers.get("ETag"))
        .or_else(|| headers.get("e-tag"))
        .cloned()
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

async fn create_bucket_at(endpoint: &str, scenario: &str) -> (String, Box<Bucket>) {
    let name = unique_bucket(scenario);
    let response = s3_call(
        "create bucket",
        Bucket::create_with_path_style(
            &name,
            region_at(endpoint),
            test_credentials(),
            BucketConfiguration::default(),
        ),
    )
    .await
    .expect("create bucket must succeed");
    assert_eq!(response.response_code, 200, "create bucket must return 200");
    (name.clone(), bucket_at(endpoint, &name))
}

async fn assert_get(bucket: &Bucket, key: &str, expected: &[u8]) {
    let response = s3_call("get object", bucket.get_object(key))
        .await
        .expect("get object must succeed");
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.bytes().as_ref(), expected);
}

async fn assert_head(bucket: &Bucket, key: &str, expected_etag: &str, expected_len: usize) {
    let (head, status) = s3_call("head object", bucket.head_object(key))
        .await
        .expect("head object must succeed");
    assert_eq!(status, 200);
    assert_eq!(
        head.e_tag.unwrap_or_default().trim_matches('"'),
        expected_etag
    );
    assert_eq!(head.content_length.unwrap_or_default(), expected_len as i64);
}

async fn assert_list_contains(bucket: &Bucket, key: &str) {
    let pages = s3_call("list objects", bucket.list(String::new(), None))
        .await
        .expect("list objects must succeed");
    assert!(
        pages
            .iter()
            .flat_map(|page| page.contents.iter())
            .any(|object| object.key == key),
        "list objects omitted {key}"
    );
}

async fn delete_object(bucket: &Bucket, key: &str) {
    let response = s3_call("delete object", bucket.delete_object(key))
        .await
        .expect("delete object must succeed");
    assert_eq!(response.status_code(), 204);
}

async fn delete_bucket(bucket: &Bucket) {
    let status = s3_call("delete bucket", bucket.delete())
        .await
        .expect("delete bucket must succeed");
    assert_eq!(status, 204);
}

fn lifecycle_configuration_xml(rule_id: &str, expiration: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter/>{expiration}</Rule>\
         </LifecycleConfiguration>"
    )
}

fn lifecycle_race_configuration_xml(rule_id: &str, expiration: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>lifecycle-race</Key><Value>expire</Value></Tag></Filter>\
         {expiration}</Rule></LifecycleConfiguration>"
    )
}

async fn signed_put_lifecycle_configuration(
    endpoint: &str,
    bucket: &str,
    configuration: String,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    http_call(
        "signed PUT lifecycle configuration",
        send_sigv4(
            reqwest::Method::PUT,
            endpoint,
            bucket,
            "",
            &[("lifecycle", "")],
            configuration.into_bytes(),
            headers,
            "test",
        ),
    )
    .await
}

async fn signed_get_lifecycle_configuration(endpoint: &str, bucket: &str) -> reqwest::Response {
    http_call(
        "signed GET lifecycle configuration",
        send_sigv4(
            reqwest::Method::GET,
            endpoint,
            bucket,
            "",
            &[("lifecycle", "")],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_delete_lifecycle_configuration(endpoint: &str, bucket: &str) -> reqwest::Response {
    http_call(
        "signed DELETE lifecycle configuration",
        send_sigv4(
            reqwest::Method::DELETE,
            endpoint,
            bucket,
            "",
            &[("lifecycle", "")],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_put_bucket_versioning(endpoint: &str, bucket: &str) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    http_call(
        "signed PUT versioning configuration",
        send_sigv4(
            reqwest::Method::PUT,
            endpoint,
            bucket,
            "",
            &[("versioning", "")],
            b"<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
            headers,
            "test",
        ),
    )
    .await
}

async fn signed_put_object(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> reqwest::Response {
    http_call(
        "signed PUT object",
        send_sigv4(
            reqwest::Method::PUT,
            endpoint,
            bucket,
            key,
            &[],
            body,
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_put_object_with_tagging(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static("lifecycle-race=expire"),
    );
    http_call(
        "signed PUT tagged object",
        send_sigv4(
            reqwest::Method::PUT,
            endpoint,
            bucket,
            key,
            &[],
            body,
            headers,
            "test",
        ),
    )
    .await
}

async fn signed_get_object(endpoint: &str, bucket: &str, key: &str) -> reqwest::Response {
    http_call(
        "signed GET object",
        send_sigv4(
            reqwest::Method::GET,
            endpoint,
            bucket,
            key,
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_list_object_versions(endpoint: &str, bucket: &str, key: &str) -> reqwest::Response {
    http_call(
        "signed ListObjectVersions",
        send_sigv4(
            reqwest::Method::GET,
            endpoint,
            bucket,
            "",
            &[("versions", ""), ("prefix", key)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_delete_object_version(
    endpoint: &str,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> reqwest::Response {
    http_call(
        "signed DELETE object version",
        send_sigv4(
            reqwest::Method::DELETE,
            endpoint,
            bucket,
            key,
            &[("versionId", version_id)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

async fn signed_delete_bucket(endpoint: &str, bucket: &str) -> reqwest::Response {
    http_call(
        "signed DELETE bucket",
        send_sigv4(
            reqwest::Method::DELETE,
            endpoint,
            bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        ),
    )
    .await
}

fn version_id(response: &reqwest::Response, label: &str) -> String {
    response
        .headers()
        .get("x-amz-version-id")
        .unwrap_or_else(|| panic!("{label} must return x-amz-version-id"))
        .to_str()
        .expect("version ID must be ASCII")
        .to_owned()
}

const LIFECYCLE_RACE_TEST_NAME: &str =
    "multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleRaceStage {
    BucketCreated,
    VersioningEnabled,
    LifecycleConfigured,
    PredecessorCreated,
    RaceStarted,
    SuccessorRequestDispatched,
    SuccessorRequestComplete,
    ObserverLoopEntered,
    ObserverGetResponse,
    ObserverListResponse,
    ObserverListStatusOk,
    ObserverSuccessorVisible,
    SuccessorResponse,
    SuccessorObserved,
    LifecycleConfigDeleted,
    TerminalWaitEntered,
    TerminalStateEvaluation,
    SuccessorRead,
    VersionCleanup,
    BucketDelete,
}

impl LifecycleRaceStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BucketCreated => "bucket-created",
            Self::VersioningEnabled => "versioning-enabled",
            Self::LifecycleConfigured => "lifecycle-configured",
            Self::PredecessorCreated => "predecessor-created",
            Self::RaceStarted => "race-started",
            Self::SuccessorRequestDispatched => "successor-request-dispatched",
            Self::SuccessorRequestComplete => "successor-request-complete",
            Self::ObserverLoopEntered => "observer-loop-entered",
            Self::ObserverGetResponse => "observer-get-response",
            Self::ObserverListResponse => "observer-list-response",
            Self::ObserverListStatusOk => "observer-list-status-ok",
            Self::ObserverSuccessorVisible => "observer-successor-visible",
            Self::SuccessorResponse => "successor-response",
            Self::SuccessorObserved => "successor-observed",
            Self::LifecycleConfigDeleted => "lifecycle-config-deleted",
            Self::TerminalWaitEntered => "terminal-wait-entered",
            Self::TerminalStateEvaluation => "terminal-state-evaluation",
            Self::SuccessorRead => "successor-read",
            Self::VersionCleanup => "version-cleanup",
            Self::BucketDelete => "bucket-delete",
        }
    }
}

fn record_lifecycle_race_stage(stage: LifecycleRaceStage) {
    eprintln!(
        "[LIFECYCLE-RACE-STAGE] test={LIFECYCLE_RACE_TEST_NAME} stage={}",
        stage.as_str()
    );
}

fn version_ids_for_cleanup(versions: &str) -> Vec<String> {
    let mut remaining = versions;
    let mut version_ids = Vec::new();
    loop {
        let next_element = match (
            remaining.find("<Version>"),
            remaining.find("<DeleteMarker>"),
        ) {
            (Some(version), Some(delete_marker)) if version < delete_marker => {
                (version, "<Version>", "</Version>")
            }
            (Some(_), Some(delete_marker)) => (delete_marker, "<DeleteMarker>", "</DeleteMarker>"),
            (Some(version), None) => (version, "<Version>", "</Version>"),
            (None, Some(delete_marker)) => (delete_marker, "<DeleteMarker>", "</DeleteMarker>"),
            (None, None) => break,
        };
        let (element_start, open, close) = next_element;
        let after_open = &remaining[element_start + open.len()..];
        let Some(element_end) = after_open.find(close) else {
            break;
        };
        let element = &after_open[..element_end];
        if let Some(version_id_start) = element.find("<VersionId>") {
            let after_version_id_open = &element[version_id_start + "<VersionId>".len()..];
            if let Some(version_id_end) = after_version_id_open.find("</VersionId>") {
                let version_id = after_version_id_open[..version_id_end].trim();
                if !version_id.is_empty()
                    && !version_ids.iter().any(|existing| existing == version_id)
                {
                    version_ids.push(version_id.to_owned());
                }
            }
        }
        remaining = &after_open[element_end + close.len()..];
    }
    version_ids
}

#[test]
fn multi_gateway_lifecycle_cleanup_includes_delete_markers() {
    let terminal_versions = r#"
        <ListVersionsResult>
            <Version><VersionId>successor</VersionId></Version>
            <DeleteMarker><VersionId>marker</VersionId></DeleteMarker>
            <Version><VersionId>predecessor</VersionId></Version>
            <Version><VersionId></VersionId></Version>
            <DeleteMarker><VersionId>marker</VersionId></DeleteMarker>
            <NextVersionIdMarker>continuation</NextVersionIdMarker>
        </ListVersionsResult>
    "#;

    assert_eq!(
        version_ids_for_cleanup(terminal_versions),
        ["successor", "marker", "predecessor"]
    );
}

async fn wait_for_signed_successor(
    get_endpoint: &str,
    list_endpoint: &str,
    bucket: &str,
    key: &str,
    successor_body: &[u8],
) -> String {
    tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {
        loop {
            let response = signed_get_object(get_endpoint, bucket, key).await;
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverGetResponse);
            let response_status = response.status().as_u16();
            let response_body = http_call("read signed successor GET", response.bytes())
                .await
                .expect("read signed successor GET");
            let list = signed_list_object_versions(list_endpoint, bucket, key).await;
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverListResponse);
            assert_eq!(
                list.status().as_u16(),
                200,
                "signed ListObjectVersions status"
            );
            record_lifecycle_race_stage(LifecycleRaceStage::ObserverListStatusOk);
            let versions = http_call("read signed ListObjectVersions XML", list.text())
                .await
                .expect("read signed ListObjectVersions XML");
            if response_status == 200 && response_body.as_ref() == successor_body {
                record_lifecycle_race_stage(LifecycleRaceStage::ObserverSuccessorVisible);
                return versions;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("signed successor must become visible within 60 seconds")
}

async fn wait_for_lifecycle_race_terminal(
    get_endpoint: &str,
    list_endpoint: &str,
    bucket: &str,
    key: &str,
    successor_body: &[u8],
    successor_version: &str,
) -> String {
    tokio::time::timeout(LIFECYCLE_RACE_TIMEOUT, async {
        let mut prior_versions = None;
        let mut stable_polls = 0_u8;
        loop {
            let response = signed_get_object(get_endpoint, bucket, key).await;
            assert_eq!(
                response.status().as_u16(),
                200,
                "signed successor GET status"
            );
            assert_eq!(
                http_call("read terminal successor GET", response.bytes())
                    .await
                    .expect("read signed successor GET")
                    .as_ref(),
                successor_body,
                "successor never deleted"
            );
            let list = signed_list_object_versions(list_endpoint, bucket, key).await;
            assert_eq!(
                list.status().as_u16(),
                200,
                "signed ListObjectVersions status"
            );
            let versions = http_call("read terminal ListObjectVersions XML", list.text())
                .await
                .expect("read signed ListObjectVersions XML");
            if versions.contains(successor_version) {
                if prior_versions.as_ref() == Some(&versions) {
                    stable_polls += 1;
                } else {
                    stable_polls = 0;
                    prior_versions = Some(versions.clone());
                }
                if stable_polls >= 3 {
                    return versions;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("lifecycle race must reach a stable S3-visible state within 60 seconds")
}

async fn kubo_cat(endpoint: &str, cid: &str) -> Vec<u8> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("build Kubo client");
    let mut url = url::Url::parse(&format!("{endpoint}/api/v0/cat"))
        .expect("Kubo endpoint must be a valid URL");
    url.query_pairs_mut().append_pair("arg", cid);
    let response = http_call("Kubo cat", client.post(url).send())
        .await
        .expect("Kubo cat request must succeed");
    assert!(response.status().is_success(), "Kubo cat must succeed");
    response.bytes().await.expect("read Kubo cat body").to_vec()
}

async fn wait_for_import(endpoint: &str, bucket: &str, key: &str, job_id: &str, cid: &str) {
    tokio::time::timeout(IMPORT_TIMEOUT, async {
        loop {
            let response = http_call(
                "import status",
                send_sigv4(
                    reqwest::Method::GET,
                    endpoint,
                    bucket,
                    key,
                    &[("ipfs3-import", job_id)],
                    Vec::new(),
                    HeaderMap::new(),
                    "test",
                ),
            )
            .await;
            assert_eq!(response.status().as_u16(), 200);
            let body = response.text().await.expect("read import status XML");
            assert!(body.contains(&format!("<JobId>{job_id}</JobId>")));
            if body.contains("<State>failed</State>") {
                panic!("CID import entered failed state");
            }
            if body.contains("<State>completed</State>") {
                assert!(body.contains("<Artifact>"));
                assert!(body.contains(&format!("<CID>{cid}</CID>")));
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("CID import must complete within 30 seconds");
}

#[tokio::test]
async fn multi_gateway_cross_replica_contract() {
    let (endpoint_a, endpoint_b, _load_balancer, kubo_endpoint) = multi_gateway_endpoints();
    let (bucket_name, bucket_a) = create_bucket_at(&endpoint_a, "cross").await;
    let bucket_b = bucket_at(&endpoint_b, &bucket_name);

    let crud_body = b"written-through-a";
    let put = s3_call("A put crud.txt", bucket_a.put_object("crud.txt", crud_body))
        .await
        .expect("A put must succeed");
    assert_eq!(put.status_code(), 200);
    let crud_etag = etag(&put.headers());
    assert!(!crud_etag.is_empty());
    assert_get(&bucket_b, "crud.txt", crud_body).await;
    assert_head(&bucket_b, "crud.txt", &crud_etag, crud_body.len()).await;
    assert_list_contains(&bucket_b, "crud.txt").await;
    delete_object(&bucket_b, "crud.txt").await;
    let deleted = s3_call("A get deleted crud.txt", bucket_a.get_object("crud.txt"))
        .await
        .expect_err("deleted object must not be readable through A");
    let deleted_message = deleted.to_string();
    assert!(deleted_message.contains("404") || deleted_message.contains("NoSuchKey"));

    let first_part = vec![0x41; 5 * 1024 * 1024];
    let second_part = b"part-two-through-b".to_vec();
    let initiated = s3_call(
        "A initiate multipart",
        bucket_a.initiate_multipart_upload("multipart.bin", "application/octet-stream"),
    )
    .await
    .expect("multipart initiation through A must succeed");
    let part_one = s3_call(
        "A upload multipart part one",
        bucket_a.put_multipart_chunk(
            first_part.clone(),
            "multipart.bin",
            1,
            &initiated.upload_id,
            "application/octet-stream",
        ),
    )
    .await
    .expect("multipart part one through A must succeed");
    let part_two = s3_call(
        "B upload multipart part two",
        bucket_b.put_multipart_chunk(
            second_part.clone(),
            "multipart.bin",
            2,
            &initiated.upload_id,
            "application/octet-stream",
        ),
    )
    .await
    .expect("multipart part two through B must succeed");
    let completed = s3_call(
        "B complete multipart",
        bucket_b.complete_multipart_upload(
            "multipart.bin",
            &initiated.upload_id,
            vec![part_one, part_two],
        ),
    )
    .await
    .expect("multipart completion through B must succeed");
    assert_eq!(completed.status_code(), 200);
    let mut multipart_body = first_part;
    multipart_body.extend_from_slice(&second_part);
    assert_get(&bucket_a, "multipart.bin", &multipart_body).await;

    let race_key = "concurrent.bin";
    let payload_a = b"complete-payload-written-through-a".to_vec();
    let payload_b = b"complete-payload-written-through-b".to_vec();
    let (put_a, put_b) = tokio::join!(
        http_call(
            "concurrent PUT A",
            send_sigv4(
                reqwest::Method::PUT,
                &endpoint_a,
                &bucket_name,
                race_key,
                &[],
                payload_a.clone(),
                HeaderMap::new(),
                "test",
            )
        ),
        http_call(
            "concurrent PUT B",
            send_sigv4(
                reqwest::Method::PUT,
                &endpoint_b,
                &bucket_name,
                race_key,
                &[],
                payload_b.clone(),
                HeaderMap::new(),
                "test",
            )
        )
    );
    let statuses = [put_a.status().as_u16(), put_b.status().as_u16()];
    assert!(statuses.iter().all(|status| matches!(status, 200 | 409)));
    assert!(statuses.contains(&200));
    assert!(statuses.iter().all(|status| *status < 500));

    let get_a = s3_call("A get converged object", bucket_a.get_object(race_key))
        .await
        .expect("A converged GET must succeed");
    let get_b = s3_call("B get converged object", bucket_b.get_object(race_key))
        .await
        .expect("B converged GET must succeed");
    assert_eq!(get_a.bytes(), get_b.bytes());
    let final_body = get_a.bytes().to_vec();
    assert!(final_body == payload_a || final_body == payload_b);
    let (head_a, head_a_status) =
        s3_call("A head converged object", bucket_a.head_object(race_key))
            .await
            .expect("A converged HEAD must succeed");
    assert_eq!(head_a_status, 200);
    let (head_b, head_b_status) = s3_call("B head converged CID", bucket_b.head_object(race_key))
        .await
        .expect("B converged CID HEAD must succeed");
    assert_eq!(head_b_status, 200);
    let final_cid = head_a
        .e_tag
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned();
    let b_cid = head_b
        .e_tag
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned();
    assert!(!final_cid.is_empty());
    assert_eq!(final_cid, b_cid);
    assert_eq!(kubo_cat(&kubo_endpoint, &final_cid).await, final_body);

    let import_key = "imported.bin";
    let import_xml = format!("<IPFS3ImportRequest><CID>{final_cid}</CID></IPFS3ImportRequest>");
    let mut import_headers = HeaderMap::new();
    import_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let accepted = http_call(
        "submit CID import through A",
        send_sigv4(
            reqwest::Method::POST,
            &endpoint_a,
            &bucket_name,
            import_key,
            &[("ipfs3-import", "")],
            import_xml.into_bytes(),
            import_headers,
            "test",
        ),
    )
    .await;
    assert_eq!(accepted.status().as_u16(), 202);
    let job_id = accepted
        .headers()
        .get("x-ipfs3-import-job-id")
        .expect("accepted import must return job id")
        .to_str()
        .expect("import job id must be ASCII")
        .to_owned();
    assert!(!job_id.is_empty());
    wait_for_import(&endpoint_b, &bucket_name, import_key, &job_id, &final_cid).await;
    assert_get(&bucket_b, import_key, &final_body).await;

    for key in ["multipart.bin", race_key, import_key] {
        delete_object(&bucket_a, key).await;
    }
    delete_bucket(&bucket_a).await;
}

#[tokio::test]
async fn load_balancer_surviving_replica_crud() {
    let (_endpoint_a, _endpoint_b, load_balancer, _kubo_endpoint) = multi_gateway_endpoints();
    let (_bucket_name, bucket) = create_bucket_at(&load_balancer, "failover").await;
    let key = "after-failover.txt";
    let body = b"served-by-the-surviving-replica";
    let put = s3_call(
        "load-balancer PUT after failover",
        bucket.put_object(key, body),
    )
    .await
    .expect("load-balancer PUT after failover must succeed");
    assert_eq!(put.status_code(), 200);
    let cid = etag(&put.headers());
    assert!(!cid.is_empty());
    assert_get(&bucket, key, body).await;
    assert_head(&bucket, key, &cid, body.len()).await;
    assert_list_contains(&bucket, key).await;
    delete_object(&bucket, key).await;
    delete_bucket(&bucket).await;
}

#[tokio::test]
async fn multi_gateway_lifecycle_configuration_visible_across_replicas() {
    let (endpoint_a, endpoint_b, load_balancer, _kubo_endpoint) = multi_gateway_endpoints();
    let (bucket_name, bucket_a) = create_bucket_at(&endpoint_a, "lifecycle-config").await;

    let initial = lifecycle_configuration_xml(
        "visible-through-a",
        "<Expiration><Date>2099-01-01T00:00:00Z</Date></Expiration>",
    );
    assert_eq!(
        signed_put_lifecycle_configuration(&endpoint_a, &bucket_name, initial)
            .await
            .status()
            .as_u16(),
        200,
        "signed lifecycle PUT through A"
    );
    let through_b = signed_get_lifecycle_configuration(&endpoint_b, &bucket_name).await;
    assert_eq!(
        through_b.status().as_u16(),
        200,
        "signed lifecycle GET through B"
    );
    assert!(
        through_b
            .text()
            .await
            .expect("read lifecycle GET through B")
            .contains("visible-through-a")
    );
    let through_load_balancer =
        signed_get_lifecycle_configuration(&load_balancer, &bucket_name).await;
    assert_eq!(
        through_load_balancer.status().as_u16(),
        200,
        "signed lifecycle GET through load balancer"
    );
    assert!(
        through_load_balancer
            .text()
            .await
            .expect("read lifecycle GET through load balancer")
            .contains("visible-through-a")
    );

    let replacement = lifecycle_configuration_xml(
        "replaced-through-b",
        "<Expiration><Date>2099-01-02T00:00:00Z</Date></Expiration>",
    );
    assert_eq!(
        signed_put_lifecycle_configuration(&endpoint_b, &bucket_name, replacement)
            .await
            .status()
            .as_u16(),
        200,
        "signed lifecycle replacement through B"
    );
    let replacement_visible =
        signed_get_lifecycle_configuration(&load_balancer, &bucket_name).await;
    assert_eq!(replacement_visible.status().as_u16(), 200);
    let replacement_body = replacement_visible
        .text()
        .await
        .expect("read replacement lifecycle configuration");
    assert!(replacement_body.contains("replaced-through-b"));
    assert!(!replacement_body.contains("visible-through-a"));

    assert_eq!(
        signed_delete_lifecycle_configuration(&endpoint_a, &bucket_name)
            .await
            .status()
            .as_u16(),
        204,
        "signed lifecycle DELETE through A"
    );
    let deleted = signed_get_lifecycle_configuration(&endpoint_b, &bucket_name).await;
    assert_eq!(
        deleted.status().as_u16(),
        404,
        "deleted lifecycle GET through B"
    );
    assert!(
        deleted
            .text()
            .await
            .expect("read deleted lifecycle response")
            .contains("NoSuchLifecycleConfiguration")
    );
    delete_bucket(&bucket_a).await;
}

#[tokio::test]
async fn multi_gateway_lifecycle_publication_action_race_has_one_terminal_outcome() {
    let (endpoint_a, endpoint_b, load_balancer, _kubo_endpoint) = multi_gateway_endpoints();
    let (bucket_name, _bucket_a) = create_bucket_at(&endpoint_a, "lifecycle-race").await;
    record_lifecycle_race_stage(LifecycleRaceStage::BucketCreated);
    let key = "raced.txt";
    let predecessor_body = b"predecessor".to_vec();
    let successor_body = b"successor".to_vec();

    assert_eq!(
        signed_put_bucket_versioning(&endpoint_a, &bucket_name)
            .await
            .status()
            .as_u16(),
        200,
        "signed versioning PUT through A"
    );
    record_lifecycle_race_stage(LifecycleRaceStage::VersioningEnabled);
    assert_eq!(
        signed_put_lifecycle_configuration(
            &endpoint_a,
            &bucket_name,
            lifecycle_race_configuration_xml(
                "due-publication-race",
                "<Expiration><Date>2000-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status()
        .as_u16(),
        200,
        "signed due lifecycle PUT through A"
    );
    record_lifecycle_race_stage(LifecycleRaceStage::LifecycleConfigured);
    let predecessor =
        signed_put_object_with_tagging(&endpoint_a, &bucket_name, key, predecessor_body).await;
    assert_eq!(
        predecessor.status().as_u16(),
        200,
        "signed predecessor PUT through A"
    );
    let predecessor_version = version_id(&predecessor, "predecessor PUT");
    record_lifecycle_race_stage(LifecycleRaceStage::PredecessorCreated);

    record_lifecycle_race_stage(LifecycleRaceStage::RaceStarted);
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestDispatched);
    let successor = signed_put_object(&endpoint_b, &bucket_name, key, successor_body.clone()).await;
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRequestComplete);
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorResponse);
    assert_eq!(
        successor.status().as_u16(),
        200,
        "signed successor PUT through B"
    );
    let successor_version = version_id(&successor, "successor PUT");
    record_lifecycle_race_stage(LifecycleRaceStage::ObserverLoopEntered);
    let first_observed_versions = wait_for_signed_successor(
        &load_balancer,
        &endpoint_a,
        &bucket_name,
        key,
        &successor_body,
    )
    .await;
    assert!(
        first_observed_versions.contains(&successor_version),
        "bounded signed ListObjectVersions polling must observe the successor"
    );
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorObserved);

    assert_eq!(
        signed_delete_lifecycle_configuration(&endpoint_a, &bucket_name)
            .await
            .status()
            .as_u16(),
        204,
        "delete lifecycle configuration after the publication/action race"
    );
    record_lifecycle_race_stage(LifecycleRaceStage::LifecycleConfigDeleted);
    record_lifecycle_race_stage(LifecycleRaceStage::TerminalWaitEntered);
    let terminal_versions = wait_for_lifecycle_race_terminal(
        &endpoint_b,
        &load_balancer,
        &bucket_name,
        key,
        &successor_body,
        &successor_version,
    )
    .await;
    record_lifecycle_race_stage(LifecycleRaceStage::TerminalStateEvaluation);
    let predecessor_visible = terminal_versions.contains(&predecessor_version);
    let successor_visible = terminal_versions.contains(&successor_version);
    let terminal_outcome = match (predecessor_visible, successor_visible) {
        (false, true) => {
            "due lifecycle action expired the predecessor before successor publication"
        }
        (true, true) => "successor publication fenced the due lifecycle action",
        state => panic!("unexpected lifecycle/publication terminal S3-visible state: {state:?}"),
    };
    assert!(
        !terminal_outcome.is_empty(),
        "exactly one allowed terminal S3-visible state must hold"
    );
    record_lifecycle_race_stage(LifecycleRaceStage::SuccessorRead);
    let successor_get = signed_get_object(&load_balancer, &bucket_name, key).await;
    assert_eq!(successor_get.status().as_u16(), 200);
    assert_eq!(
        successor_get
            .bytes()
            .await
            .expect("read terminal successor")
            .as_ref(),
        successor_body.as_slice(),
        "successor never deleted"
    );

    record_lifecycle_race_stage(LifecycleRaceStage::VersionCleanup);
    for version_id in version_ids_for_cleanup(&terminal_versions) {
        let response =
            signed_delete_object_version(&endpoint_a, &bucket_name, key, &version_id).await;
        assert!(
            matches!(response.status().as_u16(), 204 | 404),
            "signed cleanup of a raced version must not fail"
        );
    }
    record_lifecycle_race_stage(LifecycleRaceStage::BucketDelete);
    assert_eq!(
        signed_delete_bucket(&endpoint_a, &bucket_name)
            .await
            .status()
            .as_u16(),
        204,
        "signed lifecycle race bucket cleanup"
    );
}
