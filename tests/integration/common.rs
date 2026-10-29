use crate::support::decompress::{AddReply, KuboScript, S3TestEndpoint, TestHarness};
use crate::support::import::ImportHarness;
use crate::support::lifecycle::LifecycleHarness;
use crate::support::pinning::PinningHarness;
use crate::support::sigv4::send_sigv4;
use base64::Engine as _;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::{
    ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, Set, TransactionTrait,
};
use std::collections::HashMap;

pub(super) async fn assert_mpu_s3_error(response: reqwest::Response, code: &str) {
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(
        status.is_client_error(),
        "expected {code}, got {status}: {body}"
    );
    assert_eq!(xml_element_values(&body, "Code"), vec![code]);
}

pub(super) const SINGLE_ENTRY_BYTES: &[u8] = b"single entry bytes";

pub(super) const FIRST_ENTRY_BYTES: &[u8] = b"first entry bytes";

pub(super) const SECOND_ENTRY_BYTES: &[u8] = b"second entry bytes";

pub(super) const IMPORT_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

pub(super) const IMPORT_TEST_CID_V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

#[derive(Clone)]
pub(super) struct OwnedTestEndpoint {
    pub(super) endpoint: String,
    pub(super) bucket: String,
}

impl From<&TestHarness> for OwnedTestEndpoint {
    fn from(harness: &TestHarness) -> Self {
        Self {
            endpoint: harness.endpoint.clone(),
            bucket: harness.bucket.clone(),
        }
    }
}

impl S3TestEndpoint for OwnedTestEndpoint {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

pub(super) fn scripted(cids: &[&'static str], cat_bodies: Vec<(&str, Vec<u8>)>) -> KuboScript {
    KuboScript {
        add_replies: cids.iter().map(|cid| AddReply::Ok(cid)).collect(),
        cat_bodies: cat_bodies
            .into_iter()
            .map(|(cid, body)| (cid.to_owned(), body))
            .collect(),
    }
}

pub(super) fn standard_script(calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        "QmTestCid",
        calls,
        HashMap::from([("QmTestCid".to_owned(), b"hello world".to_vec())]),
    )
}

/// Convenience: build a path-style rust-s3 client for the real test endpoint.
pub(super) fn test_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("test"), Some("test"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

pub(super) async fn signed_get(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    signed_get_with_headers(harness, key, HeaderMap::new()).await
}

pub(super) async fn signed_put_bucket_versioning(
    harness: &impl S3TestEndpoint,
    status: &str,
) -> reqwest::Response {
    signed_put_bucket_versioning_xml(
        harness,
        format!(
        "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
        ),
        HeaderMap::new(),
    )
    .await
}

pub(super) async fn signed_put_bucket_versioning_xml(
    harness: &impl S3TestEndpoint,
    body: String,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("versioning", "")],
        body.into_bytes(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_get_bucket_lifecycle_configuration(
    harness: &impl S3TestEndpoint,
) -> reqwest::Response {
    signed_get_bucket_lifecycle_configuration_with_headers(harness, HeaderMap::new()).await
}

pub(super) async fn signed_get_bucket_lifecycle_configuration_with_headers(
    harness: &impl S3TestEndpoint,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_put_bucket_lifecycle_configuration_xml(
    harness: &impl S3TestEndpoint,
    body: String,
) -> reqwest::Response {
    signed_put_bucket_lifecycle_configuration_xml_with_headers(harness, body, HeaderMap::new())
        .await
}

pub(super) async fn signed_put_bucket_lifecycle_configuration_xml_with_headers(
    harness: &impl S3TestEndpoint,
    body: String,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        body.into_bytes(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_delete_bucket_lifecycle_configuration(
    harness: &impl S3TestEndpoint,
) -> reqwest::Response {
    signed_delete_bucket_lifecycle_configuration_with_headers(harness, HeaderMap::new()).await
}

pub(super) async fn signed_delete_bucket_lifecycle_configuration_with_headers(
    harness: &impl S3TestEndpoint,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn stored_lifecycle_configuration_for(
    harness: &LifecycleHarness,
) -> ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Model {
    ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Entity::find_by_id(
        harness.bucket.clone(),
    )
    .one(harness.state.store.db())
    .await
    .expect("read lifecycle acceptance configuration")
    .expect("lifecycle acceptance configuration exists")
}

pub(super) async fn signed_get_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_get_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
) -> reqwest::Response {
    signed_get_version_with_headers(harness, key, version_id, HeaderMap::new()).await
}

pub(super) async fn signed_get_version_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("versionId", version_id)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_head(
    harness: &impl S3TestEndpoint,
    key: &str,
    range: Option<&str>,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if let Some(range) = range {
        headers.insert(
            http::header::RANGE,
            HeaderValue::from_str(range).expect("valid Range header"),
        );
    }
    signed_head_with_headers(harness, key, headers).await
}

pub(super) async fn signed_head_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_copy(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!("/{}/{source_key}", harness.bucket()))
            .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        destination_key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        query,
        body,
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_put_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    signed_put(harness, key, &[], body, headers).await
}

pub(super) async fn signed_get_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
) -> reqwest::Response {
    signed_get_object_tagging_version(harness, key, None).await
}

pub(super) async fn signed_get_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn signed_put_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tags: &[(&str, &str)],
) -> reqwest::Response {
    signed_put_object_tagging_version(harness, key, tags, None).await
}

pub(super) async fn signed_put_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    tags: &[(&str, &str)],
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut xml =
        String::from("<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>");
    for (tag_key, value) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&quick_xml::escape::escape(*tag_key));
        xml.push_str("</Key><Value>");
        xml.push_str(&quick_xml::escape::escape(*value));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_delete_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
) -> reqwest::Response {
    signed_delete_object_tagging_version(harness, key, None).await
}

pub(super) async fn signed_delete_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn signed_copy_with_tagging(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging-directive",
        HeaderValue::from_static("REPLACE"),
    );
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_copy(harness, source_key, destination_key, headers).await
}

pub(super) async fn signed_create_multipart_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_create_multipart_zip_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", ""), ("decompress-zip", target_prefix)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_upload_part(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> reqwest::Response {
    let part_number = part_number.to_string();
    signed_put(
        harness,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        HeaderMap::new(),
    )
    .await
}

pub(super) async fn signed_complete_multipart(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    etag: &str,
) -> reqwest::Response {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>{part_number}</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(etag)
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploadId", upload_id)],
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

pub(super) async fn signed_decompress_zip_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_put(
        harness,
        key,
        &[("decompress-zip", target_prefix)],
        body,
        headers,
    )
    .await
}

pub(super) async fn signed_delete_object(
    harness: &impl S3TestEndpoint,
    key: &str,
) -> reqwest::Response {
    signed_delete_object_version(harness, key, None).await
}

pub(super) async fn signed_delete_object_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let query = version_id
        .map(|version_id| vec![("versionId", version_id)])
        .unwrap_or_default();
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn signed_delete_bucket(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn signed_list_objects(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("list-type", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn signed_list_object_versions(
    harness: &impl S3TestEndpoint,
    query: &[(&str, &str)],
) -> reqwest::Response {
    let mut query_with_operation = vec![("versions", "")];
    query_with_operation.extend_from_slice(query);
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &query_with_operation,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

pub(super) async fn assert_signed_body(harness: &impl S3TestEndpoint, key: &str, expected: &[u8]) {
    let response = signed_get(harness, key).await;
    assert_eq!(response.status(), StatusCode::OK, "signed GET {key}");
    assert_eq!(response.bytes().await.expect("GET body").as_ref(), expected);
}

pub(super) async fn assert_s3_error(
    response: reqwest::Response,
    status: StatusCode,
    code: &str,
    message: &str,
) {
    assert_eq!(response.status(), status);
    let body = response.text().await.expect("S3 error body");
    assert!(body.contains(code), "missing error code {code}: {body}");
    assert!(
        body.contains(message),
        "missing error message {message}: {body}"
    );
}

pub(super) async fn assert_latest_absent(harness: &TestHarness, key: &str) {
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .is_err(),
        "{key} must not have a latest object row"
    );
}

pub(super) async fn listed_db_keys(harness: &TestHarness) -> Vec<String> {
    store::object::list(harness.state.store.db(), &harness.bucket, None, None, 1000)
        .await
        .expect("list latest DB objects")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

pub(super) async fn kubo_log(harness: &TestHarness) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .map(|request| format!("{request:?}"))
        .collect()
}

pub(super) async fn kubo_query_args(harness: &TestHarness, path: &str) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .filter(|request| request.url.path() == path)
        .filter_map(|request| {
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
        })
        .collect()
}

pub(super) async fn seed_latest(harness: &TestHarness, key: &str, cid: &str, size: i64) {
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        size,
        Some("text/plain"),
        cid,
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .expect("seed latest object");
    install_unversioned_content_version(harness, key, Utc::now()).await;
}

pub(super) async fn install_unversioned_content_version(
    harness: &TestHarness,
    key: &str,
    now: DateTime<Utc>,
) {
    let object = store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
        .await
        .expect("load directly seeded latest object");
    let version_id: Option<String> = match store::object_version::BucketVersioningState::Unversioned
    {
        store::object_version::BucketVersioningState::Unversioned => None,
        store::object_version::BucketVersioningState::Enabled
        | store::object_version::BucketVersioningState::Suspended => {
            unreachable!("fixture installs only an unversioned content version")
        }
    };
    harness
        .state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                let row_id = uuid::Uuid::new_v4().to_string();
                let identity = ipfs_s3_gateway::residency::VersionResidencyIdentity::new(
                    row_id.clone(),
                    object.id.clone(),
                    object.cid.clone(),
                );
                store::entities::object_version::Entity::insert(
                    store::entities::object_version::ActiveModel {
                        id: Set(row_id),
                        bucket: Set(object.bucket),
                        key: Set(object.key),
                        version_id: Set(version_id),
                        kind: Set("object".to_owned()),
                        object_id: Set(Some(object.id)),
                        sequence: Set(1),
                        is_latest: Set(true),
                        lifecycle_age_started_at: Set(now),
                        became_noncurrent_at: Set(None),
                        created_at: Set(now),
                        updated_at: Set(now),
                    },
                )
                .exec(txn)
                .await?;
                store::residency::attach_hot_in_transaction(
                    txn,
                    &identity,
                    &ipfs_s3_gateway::residency::PhysicalVerification::Pending,
                )
                .await?;
                Ok::<_, ipfs_s3_gateway::error::AppError>(())
            })
        })
        .await
        .expect("install unversioned content version");
}

pub(super) async fn seed_running_import(
    harness: &TestHarness,
    job_id: &str,
    key: &str,
    decompress_prefix: Option<&str>,
) -> ipfs_s3_gateway::import::ImportClaim {
    let now = Utc::now();
    store::import::ownership::submit(
        harness.state.store.db(),
        store::import::jobs::NewImportJob {
            id: job_id.to_owned(),
            bucket: harness.bucket.clone(),
            key: key.to_owned(),
            source: ipfs_s3_gateway::import::ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("sha256:{job_id}"),
            client_token: None,
            object_content_type: Some("application/octet-stream".to_owned()),
            metadata: HashMap::new(),
            tags: Vec::new(),
            decompress_prefix: decompress_prefix.map(str::to_owned),
        },
        now,
    )
    .await
    .expect("submit import fixture");
    let mut claimed = store::import::jobs::claim_due(
        harness.state.store.db(),
        &format!("worker-{job_id}"),
        now,
        now + ChronoDuration::minutes(10),
        1,
    )
    .await
    .expect("claim import fixture");
    assert_eq!(claimed.len(), 1, "exactly one fixture import is due");
    let claimed = claimed.pop().expect("claimed import fixture");
    assert_eq!(claimed.job.id, job_id);
    claimed.claim
}

pub(super) async fn assert_import_state(harness: &TestHarness, job_id: &str, expected: &str) {
    let job = store::entities::import_job::Entity::find_by_id(job_id.to_owned())
        .one(harness.state.store.db())
        .await
        .expect("load import fixture")
        .expect("import fixture exists");
    assert_eq!(job.state, expected, "import job {job_id}");
}

pub(super) fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_element_values(xml, tag)
        .into_iter()
        .next()
        .map(str::to_owned)
}

pub(super) fn tagging_pairs(xml: &str) -> Vec<(String, String)> {
    xml_element_values(xml, "Tag")
        .into_iter()
        .map(|tag| {
            (
                xml_text(tag, "Key").expect("Tag Key"),
                xml_text(tag, "Value").expect("Tag Value"),
            )
        })
        .collect()
}

pub(super) async fn latest_pinning_object(
    harness: &PinningHarness,
    key: &str,
) -> store::entities::object::Model {
    store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
        .await
        .expect("latest pinning object")
}

pub(super) async fn owner_leases(
    harness: &PinningHarness,
    owner_object_id: &str,
) -> Vec<store::entities::pin_lease::Model> {
    store::entities::pin_lease::Entity::find()
        .filter(store::entities::pin_lease::Column::OwnerObjectId.eq(owner_object_id))
        .order_by_asc(store::entities::pin_lease::Column::Source)
        .all(harness.state.store.db())
        .await
        .expect("owner pinning leases")
}

pub(super) async fn lease_targets(
    harness: &PinningHarness,
    lease_id: &str,
) -> Vec<store::entities::pin_lease_target::Model> {
    store::entities::pin_lease_target::Entity::find()
        .filter(store::entities::pin_lease_target::Column::LeaseId.eq(lease_id))
        .order_by_asc(store::entities::pin_lease_target::Column::Provider)
        .order_by_asc(store::entities::pin_lease_target::Column::Id)
        .all(harness.state.store.db())
        .await
        .expect("lease targets")
}

pub(super) async fn assert_no_kubo_pin_removes(harness: &PinningHarness) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/api/v0/pin/rm"),
        "remote lifecycle must not remove Kubo pins: {requests:?}"
    );
}

pub(super) const PIN_CID_PART: &str = "bafkreid7q7afsgkz6piz7vg4jtn7dgnvhmlu4fbwfqcgicwnrsxdviutpm";

pub(super) const PIN_CID_ROOT: &str = "bafkreihjxyl3buwip5ndg5r2hxfiti57ktzodculywi5c36lhk7lwq33ou";

pub(super) const PIN_CID_ENTRY1: &str =
    "bafkreifrzrmgjjyinm4ouxml5agww3xdc3v5hn2rrkbrademswu3t73qay";

pub(super) const PIN_CID_ENTRY2: &str =
    "bafkreicjejslxnsavqzukvy2a2en3wv2kr6sczpojs4glh7fjamktm7454";

pub(super) fn delete_identifiers_xml(objects: &[(&str, Option<&str>)], quiet: bool) -> Vec<u8> {
    let objects = objects
        .iter()
        .map(|(key, version_id)| match version_id {
            Some(version_id) => {
                format!("<Object><Key>{key}</Key><VersionId>{version_id}</VersionId></Object>")
            }
            None => format!("<Object><Key>{key}</Key></Object>"),
        })
        .collect::<String>();
    format!(
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{objects}<Quiet>{quiet}</Quiet></Delete>"
    )
    .into_bytes()
}

pub(super) fn delete_xml(keys: &[&str], quiet: bool) -> Vec<u8> {
    delete_identifiers_xml(
        &keys.iter().map(|key| (*key, None)).collect::<Vec<_>>(),
        quiet,
    )
}

pub(super) fn xml_element_values<'a>(xml: &'a str, element: &str) -> Vec<&'a str> {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut remaining = xml;
    let mut values = Vec::new();
    while let Some(open_at) = remaining.find(&open) {
        let value_start = open_at + open.len();
        let after_open = &remaining[value_start..];
        let Some(close_at) = after_open.find(&close) else {
            break;
        };
        values.push(&after_open[..close_at]);
        remaining = &after_open[close_at + close.len()..];
    }
    values
}

pub(super) fn delete_headers(body: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(body).0);
    headers.insert(
        "content-md5",
        HeaderValue::from_str(&digest).expect("base64 MD5 header"),
    );
    headers
}

pub(super) async fn signed_delete_objects(
    harness: &impl S3TestEndpoint,
    keys: &[&str],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_xml(keys, quiet);
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await
}

pub(super) fn sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 customer key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 customer key MD5 header"),
    );
    headers
}

pub(super) fn sse_c_headers() -> HeaderMap {
    sse_c_headers_for([7; 32])
}

pub(super) fn assert_put_cid_headers(response: &reqwest::Response, cid: &str) {
    assert_eq!(
        response.headers()[http::header::ETAG],
        HeaderValue::from_str(&format!("\"{cid}\"")).expect("CID ETag header")
    );
    assert_eq!(response.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        response.headers()["x-amz-meta-ipfs-url"],
        HeaderValue::from_str(&format!("ipfs://{cid}")).expect("IPFS URL header")
    );
}

pub(super) fn import_url_xml(url: &str) -> String {
    format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        quick_xml::escape::escape(url)
    )
}

pub(super) async fn import_result_count(harness: &ImportHarness, job_id: &str) -> u64 {
    store::entities::import_job_result::Entity::find()
        .filter(store::entities::import_job_result::Column::JobId.eq(job_id))
        .count(harness.state.store.db())
        .await
        .expect("count import result rows")
}

pub(super) fn xml_element_text(xml: &str, name: &str) -> String {
    let opening = format!("<{name}>");
    let closing = format!("</{name}>");
    let start = xml
        .find(&opening)
        .unwrap_or_else(|| panic!("missing {opening}: {xml}"))
        + opening.len();
    let end = xml[start..]
        .find(&closing)
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing {closing}: {xml}"));
    quick_xml::escape::unescape(&xml[start..end])
        .expect("XML element escaping")
        .into_owned()
}
