//! Signed legacy/v2 requests and exact durable replay witnesses.
use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{
    entities::{
        object, object_version, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
    },
    zip::{execution, import_intake},
};
use sea_orm::{EntityTrait, QueryOrder};

use crate::support::{
    decompress::S3TestEndpoint,
    sigv4::send_sigv4,
    stage9_zip_replay::{BUCKET, Fixture, KEY},
    stage9_zip_replay_source::URL,
};

pub fn minimal(token: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-ipfs3-zip-contract", HeaderValue::from_static("v2"));
    if let Some(token) = token {
        headers.insert("x-ipfs3-zip-token", HeaderValue::from_str(token).unwrap());
    }
    headers
}

pub fn controls(token: &str) -> HeaderMap {
    let mut headers = minimal(Some(token));
    for (name, value) in [
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    headers
}

pub fn import_controls(token: &str) -> HeaderMap {
    // Existing full-controls interoperability path, not the minimum request.
    // Neither this path nor minimal() supplies a client artifact digest.
    let mut headers = controls(token);
    headers.insert(
        "x-ipfs3-client-token",
        HeaderValue::from_str(token).unwrap(),
    );
    headers
}

pub async fn put(h: &Fixture, archive: Vec<u8>, headers: HeaderMap, prefix: &str) -> Receipt {
    Receipt::read(
        send_sigv4(
            reqwest::Method::PUT,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("decompress-zip", prefix)],
            archive,
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn create(h: &Fixture, headers: HeaderMap, prefix: &str) -> Receipt {
    Receipt::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("uploads", ""), ("decompress-zip", prefix)],
            Vec::new(),
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn upload(h: &Fixture, id: &str, archive: Vec<u8>) -> String {
    let response = send_sigv4(
        reqwest::Method::PUT,
        h.endpoint(),
        BUCKET,
        KEY,
        &[("uploadId", id), ("partNumber", "1")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response.headers()[http::header::ETAG]
        .to_str()
        .unwrap()
        .to_owned()
}

pub async fn complete(h: &Fixture, id: &str, etag: &str, headers: HeaderMap) -> Receipt {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    Receipt::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("uploadId", id)],
            xml.into_bytes(),
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub fn url_xml(url: &str) -> String {
    format!("<IPFS3ImportRequest><URL>{url}</URL></IPFS3ImportRequest>")
}

pub async fn import(h: &Fixture, source: &str, mut headers: HeaderMap) -> Receipt {
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    Receipt::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            source.as_bytes().to_vec(),
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn status(h: &Fixture, id: &str, imported: bool) -> Receipt {
    let query = if imported {
        "ipfs3-import"
    } else {
        "ipfs3-zip-batch"
    };
    Receipt::read(
        send_sigv4(
            reqwest::Method::GET,
            h.endpoint(),
            BUCKET,
            KEY,
            &[(query, id)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await,
    )
    .await
}

pub async fn get(h: &Fixture, key: &str) -> (StatusCode, Vec<u8>) {
    let response = send_sigv4(
        reqwest::Method::GET,
        h.endpoint(),
        BUCKET,
        key,
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    (response.status(), response.bytes().await.unwrap().to_vec())
}

pub async fn enable_versioning(h: &Fixture) {
    let response = send_sigv4(
        reqwest::Method::PUT,
        h.endpoint(),
        BUCKET,
        "",
        &[("versioning", "")],
        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[derive(Debug, PartialEq)]
pub struct Receipt {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: String,
}

impl Receipt {
    pub async fn read(response: reqwest::Response) -> Self {
        let status = response.status();
        let mut headers = response.headers().clone();
        // Transport Date is not the durable publication receipt. All remaining
        // response headers, including batch/ETag/version, must replay exactly.
        headers.remove(http::header::DATE);
        Self {
            status,
            headers,
            body: response.text().await.unwrap(),
        }
    }

    pub fn id(&self, imported: bool) -> String {
        self.headers[if imported {
            "x-ipfs3-import-job-id"
        } else {
            "x-ipfs-s3-zip-batch-id"
        }]
        .to_str()
        .unwrap()
        .to_owned()
    }

    pub fn upload_id(&self) -> String {
        self.body
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_owned()
    }
}

#[derive(Debug, PartialEq)]
pub struct Ledger {
    objects: Vec<object::Model>,
    versions: Vec<object_version::Model>,
    leases: Vec<pin_lease::Model>,
    targets: Vec<pin_lease_target::Model>,
    remotes: Vec<remote_pin::Model>,
    usage: Vec<pin_provider_usage::Model>,
}

impl Ledger {
    pub async fn read(h: &Fixture) -> Self {
        let db = h.state.store.db();
        Self {
            objects: object::Entity::find()
                .order_by_asc(object::Column::Id)
                .all(db)
                .await
                .unwrap(),
            versions: object_version::Entity::find()
                .order_by_asc(object_version::Column::Id)
                .all(db)
                .await
                .unwrap(),
            leases: pin_lease::Entity::find()
                .order_by_asc(pin_lease::Column::Id)
                .all(db)
                .await
                .unwrap(),
            targets: pin_lease_target::Entity::find()
                .order_by_asc(pin_lease_target::Column::Id)
                .all(db)
                .await
                .unwrap(),
            remotes: remote_pin::Entity::find()
                .order_by_asc(remote_pin::Column::Provider)
                .order_by_asc(remote_pin::Column::Cid)
                .all(db)
                .await
                .unwrap(),
            usage: pin_provider_usage::Entity::find()
                .order_by_asc(pin_provider_usage::Column::Provider)
                .all(db)
                .await
                .unwrap(),
        }
    }

    pub fn assert_unpublished(&self) {
        assert!(
            self.objects.is_empty() && self.versions.is_empty(),
            "{self:?}"
        );
        assert!(
            self.leases.is_empty() && self.targets.is_empty() && self.remotes.is_empty(),
            "{self:?}"
        );
        assert!(
            self.usage
                .iter()
                .all(|usage| usage.reserved_bytes == 0 && usage.reserved_pins == 0),
            "{self:?}"
        );
    }
}

pub async fn terminal(h: &Fixture, id: &str) -> &'static str {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let row = import_intake::read_for_path(h.state.store.db(), id, "test", BUCKET, KEY)
                .await
                .unwrap()
                .unwrap();
            if row.state != "pending" {
                return row.state;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|error| panic!("import terminal observation {id} ({URL}): {error}"))
}

pub async fn assert_server_bound_artifact(h: &Fixture, id: &str, size: usize) {
    let row = execution::read(h.state.store.db(), id)
        .await
        .unwrap()
        .unwrap();
    let digest = row
        .input_sha256
        .expect("server binds a full artifact digest");
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(row.input_art_cid.is_some());
    assert_eq!(row.input_art_size, Some(size as i64));
}
