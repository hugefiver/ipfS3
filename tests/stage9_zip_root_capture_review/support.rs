use crate::support::{decompress, sigv4};
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store::{self, zip};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

pub async fn published_mpu() -> (decompress::TestHarness, zip::BatchSnapshot, zip::RootClaim) {
    published_mpu_with_source(false).await
}

pub async fn published_mpu_with_source(
    source: bool,
) -> (decompress::TestHarness, zip::BatchSnapshot, zip::RootClaim) {
    let archive = decompress::legal_single_entry_zip();
    let harness = decompress::start_harness_with_root_default(
        decompress::KuboScript {
            add_replies: vec![
                decompress::AddReply::Ok("QmPart"),
                decompress::AddReply::Ok("QmArchive"),
                decompress::AddReply::Ok(super::LEAF),
            ],
            cat_bodies: [
                ("QmPart".into(), archive.clone()),
                ("QmArchive".into(), archive.clone()),
            ]
            .into(),
        },
        true,
    )
    .await;
    store::bucket::set_versioning_state(
        harness.state.store.db(),
        &harness.bucket,
        store::object_version::BucketVersioningState::Enabled,
    )
    .await
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ID":"localNode"})))
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"Hash":super::LEAF,"CumulativeSize":0})),
        )
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&harness.kubo)
        .await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        (
            "x-ipfs3-zip-publish-source",
            if source { "true" } else { "false" },
        ),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "mpu-root-review"),
    ] {
        headers.insert(name, value.parse().unwrap());
    }
    let create = sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
        "test",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let xml = create.text().await.unwrap();
    let id = xml
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap();
    let part = sigv4::send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", id), ("partNumber", "1")],
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let complete = sigv4::send_sigv4(reqwest::Method::POST, &harness.endpoint, &harness.bucket,
        "archive.zip", &[("uploadId", id)],
        b"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>".to_vec(),
        HeaderMap::new(), "test").await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_eq!(complete.headers()["x-ipfs-s3-zip-root-status"], "failed");
    let snapshot = zip::snapshot(harness.state.store.db(), id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.source_published, source);
    assert!(snapshot.entries[0].version_row_id.is_some());
    let claim = zip::RootClaim {
        batch_id: id.into(),
        revision: snapshot.batch.root_revision,
        epoch: snapshot.batch.root_epoch,
        worker: snapshot.builds.last().unwrap().worker.clone(),
    };
    (harness, snapshot, claim)
}

pub async fn counters(harness: &decompress::TestHarness) -> Vec<i64> {
    db_counters(harness.state.store.db()).await
}

pub async fn db_counters(db: &sea_orm::DatabaseConnection) -> Vec<i64> {
    let mut result = Vec::new();
    for table in [
        "objects",
        "object_versions",
        "pin_leases",
        "pin_lease_targets",
        "pin_jobs",
        "remote_pins",
        "remote_pin_ledger",
    ] {
        result.push(
            db.query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT COUNT(*) AS n FROM {table}"),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap(),
        );
    }
    for column in ["reserved_bytes", "reserved_pins"] {
        result.push(
            db.query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT COALESCE(SUM({column}),0) AS n FROM pin_provider_usage"),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap(),
        );
    }
    result
}

pub const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

pub async fn directory_rpc(server: &MockServer) {
    for (endpoint, body) in [
        ("/api/v0/id", json!({"ID":"localNode"})),
        (
            "/api/v0/files/stat",
            json!({"Hash":super::LEAF,"CumulativeSize":0}),
        ),
        ("/api/v0/dag/put", json!({"Cid":{"/":ROOT}})),
        (
            "/api/v0/resolve",
            json!({"Path":format!("/ipfs/{}",super::LEAF)}),
        ),
        ("/api/v0/pin/add", json!({"Pins":[ROOT]})),
        (
            "/api/v0/pin/ls",
            json!({"Keys":{ROOT:{"Type":"recursive"}}}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .with_priority(2)
            .mount(server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"Hash":ROOT,"WithLocality":true,"Local":true})),
        )
        .with_priority(1)
        .mount(server)
        .await;
}
