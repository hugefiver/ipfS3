use std::collections::HashMap;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::zip;
use sea_orm::{ConnectionTrait, Statement};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

use crate::support::{
    decompress::{self, AddReply, KuboScript, TestHarness},
    sigv4::send_sigv4,
};

const FILE_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const ROOT_CID: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";
const ARCHIVE: &str = "archive.zip";
const QUERY: &[(&str, &str)] = &[("decompress-zip", "expanded/")];

async fn directory(kubo: &wiremock::MockServer) {
    for (endpoint, body) in [
        (
            "/api/v0/files/stat",
            format!("{{\"Hash\":\"{FILE_CID}\",\"CumulativeSize\":18}}"),
        ),
        (
            "/api/v0/dag/put",
            format!("{{\"Cid\":{{\"/\":\"{ROOT_CID}\"}}}}"),
        ),
        (
            "/api/v0/resolve",
            format!("{{\"Path\":\"/ipfs/{FILE_CID}\"}}"),
        ),
        ("/api/v0/id", "{\"ID\":\"localNode\"}".into()),
        (
            "/api/v0/pin/ls",
            format!("{{\"Keys\":{{\"{ROOT_CID}\":{{\"Type\":\"recursive\"}}}}}}"),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(kubo)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("with-local", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(
                serde_json::json!({"Hash":ROOT_CID,"WithLocality":true,"Local":true}),
            ),
        )
        .with_priority(2)
        .mount(kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("recursive", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"Pins":[ROOT_CID]})),
        )
        .with_priority(2)
        .mount(kubo)
        .await;
}

async fn release(harness: &TestHarness, id: &str) {
    let db = harness.state.store.db();
    let snapshot = zip::snapshot(db, id).await.unwrap().unwrap();
    let reference = snapshot
        .references
        .iter()
        .find(|row| row.state == "adopted")
        .unwrap();
    let changed = db.execute(Statement::from_sql_and_values(db.get_database_backend(),
        "UPDATE zip_root_references SET logical_released_at=$1 WHERE batch_id=$2 AND revision=$3 AND epoch=$4 AND node_identity=$5 AND tier=$6 AND cid=$7 AND logical_released_at IS NULL",
        vec!["2026-10-13T01:02:03.456789Z".into(), id.into(), reference.revision.into(), reference.epoch.into(), reference.node_identity.clone().into(), reference.tier.clone().into(), reference.cid.clone().into()]
    )).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
    assert_eq!(
        zip::root_existence(
            db,
            id,
            &reference.node_identity,
            &reference.tier,
            &reference.cid
        )
        .await
        .unwrap(),
        "absent"
    );
    let after = zip::snapshot(db, id).await.unwrap().unwrap();
    assert_eq!(after.batch, snapshot.batch);
    assert_eq!(after.entries, snapshot.entries);
    assert_eq!(after.builds, snapshot.builds);
    assert_eq!(after.references.len(), snapshot.references.len());
    assert_eq!(after.references[0].state, "adopted");
}

async fn assert_status(harness: &TestHarness, id: &str) {
    let response = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        ARCHIVE,
        &[("ipfs3-zip-batch", id)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(response.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    assert!(
        response
            .text()
            .await
            .unwrap()
            .contains("<RootStatus>complete</RootStatus>")
    );
}

pub async fn direct_replay() {
    let archive = decompress::legal_single_entry_zip();
    let harness = decompress::start_harness_with_root_default(
        KuboScript {
            add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok(FILE_CID)],
            cat_bodies: HashMap::from([("QmArchive".into(), archive.clone())]),
        },
        true,
    )
    .await;
    directory(&harness.kubo).await;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "false"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", "root-release-replay"),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    let first = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        ARCHIVE,
        QUERY,
        archive.clone(),
        headers.clone(),
        "test",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(first.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    let id = first.headers()["x-ipfs-s3-zip-batch-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let xml = first.text().await.unwrap();
    release(&harness, &id).await;
    let before = harness.kubo.received_requests().await.unwrap().len();
    let replay = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        ARCHIVE,
        QUERY,
        archive,
        headers,
        "test",
    )
    .await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_status(&harness, &id).await;
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before
    );
}

pub async fn multipart_replay() {
    let archive = decompress::legal_single_entry_zip();
    let harness = decompress::start_harness_with_root_default(
        KuboScript {
            add_replies: vec![
                AddReply::Ok("QmPart"),
                AddReply::Ok("QmArchive"),
                AddReply::Ok(FILE_CID),
            ],
            cat_bodies: HashMap::from([
                ("QmPart".into(), archive.clone()),
                ("QmArchive".into(), archive.clone()),
            ]),
        },
        true,
    )
    .await;
    directory(&harness.kubo).await;
    let id = decompress::create_multipart(&harness, ARCHIVE, QUERY).await;
    let etag = decompress::upload_part(&harness, ARCHIVE, &id, 1, archive).await;
    let parts = [(1, etag)];
    let first = decompress::complete_multipart(&harness, ARCHIVE, &id, &parts).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.headers()["x-ipfs-s3-zip-root-status"], "complete");
    assert_eq!(first.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    let xml = first.text().await.unwrap();
    release(&harness, &id).await;
    let before = harness.kubo.received_requests().await.unwrap().len();
    let replay = decompress::complete_multipart(&harness, ARCHIVE, &id, &parts).await;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(replay.headers()["x-ipfs-s3-zip-root-cid"], ROOT_CID);
    assert_eq!(replay.text().await.unwrap(), xml);
    assert_status(&harness, &id).await;
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        before
    );
}
