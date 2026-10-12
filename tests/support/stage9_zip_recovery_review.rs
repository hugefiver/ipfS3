//! Real SigV4 recovery probes with raw-byte and persistent side-effect witnesses.
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store::{self, zip::execution};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

use crate::support::{
    decompress::{AddReply, KuboScript, TestHarness, start_harness_with_root_default},
    sigv4::send_sigv4,
};

pub const ARCHIVE: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub const OUTPUT: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4";
pub const KEY: &str = "archive.zip";

pub fn controls(token: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
    ] {
        headers.insert(key, value.parse().unwrap());
    }
    headers
}

#[derive(Debug, PartialEq)]
pub struct Receipt {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

impl Receipt {
    pub async fn read(response: reqwest::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers().clone(),
            body: response.text().await.unwrap(),
        }
    }
}

pub async fn request(
    h: &TestHarness,
    method: reqwest::Method,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> Receipt {
    Receipt::read(
        send_sigv4(
            method,
            &h.endpoint,
            &h.bucket,
            key,
            query,
            body,
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn put(h: &TestHarness, token: &str, archive: Vec<u8>) -> Receipt {
    request(
        h,
        reqwest::Method::PUT,
        KEY,
        &[("decompress-zip", "out/")],
        archive,
        controls(token),
    )
    .await
}

pub async fn status(h: &TestHarness, id: &str, key: &str) -> Receipt {
    request(
        h,
        reqwest::Method::GET,
        key,
        &[("ipfs3-zip-batch", id)],
        vec![],
        HeaderMap::new(),
    )
    .await
}

pub async fn complete(h: &TestHarness, id: &str, etag: &str) -> Receipt {
    request(h, reqwest::Method::POST, KEY, &[("uploadId", id)],
        format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>").into_bytes(), HeaderMap::new()).await
}

pub fn empty_zip() -> Vec<u8> {
    let mut archive = vec![0; 22];
    archive[..4].copy_from_slice(b"PK\x05\x06");
    archive
}

pub fn with_comment(mut archive: Vec<u8>, comment: &[u8]) -> Vec<u8> {
    let end = archive.len();
    archive[end - 2..].copy_from_slice(&(comment.len() as u16).to_le_bytes());
    archive.extend_from_slice(comment);
    archive
}

pub async fn count(h: &TestHarness, table: &str) -> i64 {
    h.state
        .store
        .db()
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("SELECT COUNT(*) AS n FROM {table}"),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
pub struct Effects {
    pub rpc: Vec<(String, String)>,
    pub rows: Vec<(&'static str, i64)>,
    pub quota: Vec<(String, i64, i64)>,
}

impl Effects {
    pub async fn read(h: &TestHarness) -> Self {
        let rpc = h
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.url.path().into(),
                    r.url.query().unwrap_or_default().into(),
                )
            })
            .collect();
        let mut rows = Vec::new();
        for table in [
            "objects",
            "object_versions",
            "pin_leases",
            "pin_lease_targets",
            "pin_jobs",
            "remote_pins",
            "zip_v2_manifest",
            "zip_v2_targets",
            "zip_batches",
            "standard_mutation_leases",
        ] {
            rows.push((table, count(h, table).await));
        }
        let quota = h.state.store.db().query_all(Statement::from_string(DatabaseBackend::Sqlite,
            "SELECT provider,reserved_bytes,reserved_pins FROM pin_provider_usage ORDER BY provider".to_owned())).await.unwrap().into_iter().map(|row| (row.try_get("", "provider").unwrap(), row.try_get("", "reserved_bytes").unwrap(), row.try_get("", "reserved_pins").unwrap())).collect();
        Self { rpc, rows, quota }
    }

    pub fn rpc_count(&self, path: &str) -> usize {
        self.rpc.iter().filter(|(p, _)| p == path).count()
    }
}

pub async fn expire(h: &TestHarness, id: &str) {
    sql(
        h,
        "UPDATE zip_v2_executions SET lease_until=datetime('now','-1 minute') WHERE id=?",
        vec![id.into()],
    )
    .await;
}

pub async fn sql(h: &TestHarness, query: &str, values: Vec<sea_orm::Value>) {
    h.state
        .store
        .db()
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            query,
            values,
        ))
        .await
        .unwrap();
}

pub async fn bound_pending(token: &str, archive: &[u8]) -> (TestHarness, execution::Snapshot) {
    let h = start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok(ARCHIVE),
                AddReply::Ok(OUTPUT),
                AddReply::Ok(ARCHIVE),
            ],
            cat_bodies: Default::default(),
        },
        false,
    )
    .await;
    let first = put(&h, token, archive.to_vec()).await;
    assert!(!first.status.is_success(), "{first:?}");
    let saved = execution::read_by_identity(h.state.store.db(), "test", "direct", token)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.state, "pending");
    assert_eq!(saved.input_art_cid.as_deref(), Some(ARCHIVE));
    assert_eq!(saved.input_art_size, Some(archive.len() as i64));
    assert_eq!(
        saved.input_sha256.as_deref(),
        Some(hex::encode(sha2::Sha256::digest(archive)).as_str())
    );
    assert_eq!(count(&h, "zip_v2_manifest").await, 0);
    assert_eq!(count(&h, "zip_batches").await, 0);
    expire(&h, &saved.id).await;
    h.set_cat_body(ARCHIVE, archive.to_vec());
    (h, saved)
}

pub async fn zero_mpu(empty: bool) -> (TestHarness, String, String, Receipt) {
    let archive = if empty {
        empty_zip()
    } else {
        crate::support::decompress::legal_single_entry_zip()
    };
    let mut add_replies = vec![AddReply::Ok(ARCHIVE), AddReply::Ok(ARCHIVE)];
    if !empty {
        add_replies.push(AddReply::Error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "entry refused",
        ));
    }
    let h = start_harness_with_root_default(
        KuboScript {
            add_replies,
            cat_bodies: [(ARCHIVE.to_owned(), archive.clone())].into(),
        },
        true,
    )
    .await;
    store::bucket::set_versioning_state(
        h.state.store.db(),
        &h.bucket,
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    let create = request(
        &h,
        reqwest::Method::POST,
        KEY,
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        controls(if empty { "empty-fresh" } else { "failed-fresh" }),
    )
    .await;
    assert_eq!(create.status, StatusCode::OK, "{create:?}");
    let id = create
        .body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_owned();
    let upload = request(
        &h,
        reqwest::Method::PUT,
        KEY,
        &[("uploadId", &id), ("partNumber", "1")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(upload.status, StatusCode::OK, "{upload:?}");
    let etag = upload.headers[http::header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();
    let first = complete(&h, &id, &etag).await;
    assert_eq!(first.status, StatusCode::BAD_REQUEST, "{first:?}");
    (h, id, etag, first)
}

pub async fn admitted_pending(token: &str, archive: &[u8]) -> (TestHarness, execution::Snapshot) {
    let (h, saved) = bound_pending(token, archive).await;
    sql(&h, "CREATE TRIGGER recovery_publication_failure BEFORE INSERT ON object_versions BEGIN SELECT RAISE(ABORT, 'fixture publication rollback'); END", vec![]).await;
    let first = put(&h, token, archive.to_vec()).await;
    assert!(!first.status.is_success(), "{first:?}");
    let admitted = execution::read(h.state.store.db(), &saved.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admitted.state, "admitted");
    assert_eq!(count(&h, "zip_v2_manifest").await, 1);
    assert_eq!(count(&h, "zip_v2_targets").await, 1);
    sql(&h, "DROP TRIGGER recovery_publication_failure", vec![]).await;
    expire(&h, &saved.id).await;
    (h, admitted)
}

use sha2::Digest as _;
