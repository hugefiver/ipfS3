//! Original business inputs; capture response bytes before any interpretation.
use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::entities::import_job;

use crate::support::{
    decompress::S3TestEndpoint,
    sigv4::send_sigv4,
    stage9_zip_replay_source::URL,
    stage9_zip_wire::{BUCKET, Fixture, KEY},
};

#[derive(Debug, PartialEq)]
pub struct Wire {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Wire {
    pub async fn read(response: reqwest::Response) -> Self {
        Self {
            status: response.status(),
            headers: response.headers().clone(),
            body: response.bytes().await.unwrap().to_vec(),
        }
    }

    pub fn id(&self, header: &str) -> String {
        self.headers[header].to_str().unwrap().to_owned()
    }

    pub fn field(&self, tag: &str) -> String {
        let text = std::str::from_utf8(&self.body).unwrap();
        let opening = format!("<{tag}>");
        let closing = format!("</{tag}>");
        assert_eq!(
            text.matches(&opening).count(),
            1,
            "unique named dynamic field"
        );
        text.split_once(&opening)
            .unwrap()
            .1
            .split_once(&closing)
            .unwrap()
            .0
            .to_owned()
    }

    pub fn protocol(&self) -> HeaderMap {
        let mut headers = self.headers.clone();
        // Date/server belong to transport, not a persisted publication receipt.
        headers.remove(http::header::DATE);
        headers.remove(http::header::SERVER);
        headers
    }
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

pub fn v2(token: &str) -> HeaderMap {
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

pub async fn put(h: &Fixture, archive: Vec<u8>, headers: HeaderMap) -> Wire {
    let mut query = vec![("decompress-zip", "out/")];
    if !headers.contains_key("x-ipfs3-zip-contract") {
        query.push(("decompress-zip-result", "false"));
    }
    Wire::read(
        send_sigv4(
            reqwest::Method::PUT,
            h.endpoint(),
            BUCKET,
            KEY,
            &query,
            archive,
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn create(h: &Fixture, headers: HeaderMap) -> Wire {
    Wire::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("uploads", ""), ("decompress-zip", "out/")],
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

pub async fn complete(h: &Fixture, id: &str, etag: &str) -> Wire {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    Wire::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("uploadId", id)],
            xml.into_bytes(),
            HeaderMap::new(),
            "test",
        )
        .await,
    )
    .await
}

pub async fn import(h: &Fixture) -> Wire {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let xml = format!("<IPFS3ImportRequest><URL>{URL}</URL></IPFS3ImportRequest>");
    Wire::read(
        send_sigv4(
            reqwest::Method::POST,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("ipfs3-import", ""), ("decompress-zip", "out/")],
            xml.into_bytes(),
            headers,
            "test",
        )
        .await,
    )
    .await
}

pub async fn status(h: &Fixture, id: &str) -> Wire {
    Wire::read(
        send_sigv4(
            reqwest::Method::GET,
            h.endpoint(),
            BUCKET,
            KEY,
            &[("ipfs3-import", id)],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await,
    )
    .await
}

pub async fn completed(h: &Fixture, id: &str) {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let job = import_job::Entity::find_by_id(id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            if matches!(job.state.as_str(), "completed" | "failed" | "cancelled") {
                assert_eq!(job.state, "completed", "{job:?}");
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if let Err(error) = result {
        let job = import_job::Entity::find_by_id(id)
            .one(h.state.store.db())
            .await
            .unwrap();
        let requests = h.kubo.received_requests().await.unwrap();
        panic!(
            "legacy import default-worker observation: {error}; job={job:?}; rpc={:?}",
            requests
                .iter()
                .map(|request| request.url.as_str())
                .collect::<Vec<_>>()
        );
    }
}
