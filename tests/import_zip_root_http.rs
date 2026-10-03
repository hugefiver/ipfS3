//! Signed import submission/status exercises root-only failure on the real worker.
#[allow(dead_code)]
mod support;

use std::collections::HashMap;

use http::StatusCode;
use ipfs_s3_gateway::store::{
    entities::{import_job_result, object, object_version},
    zip,
};
use sea_orm::{EntityTrait, PaginatorTrait};
use support::{
    decompress::{AddReply, KuboScript, legal_single_entry_zip},
    import::{
        ImportHarnessConfig, get_import_status, post_import, start_import_harness,
        wait_for_import_state,
    },
};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path, query_param},
};

const SOURCE: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_import_root_post_put_failures_warn_without_losing_archive_entries_or_results() {
    for failed_rpc in ["pin/add", "resolve", "pin/ls"] {
        assert_signed_import_root_failure(failed_rpc).await;
    }
}

async fn assert_signed_import_root_failure(failed_rpc: &str) {
    let archive = legal_single_entry_zip();
    let harness = start_import_harness(ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(SOURCE)],
            cat_bodies: HashMap::from([(SOURCE.to_owned(), archive)]),
        },
        max_attempts: 2,
        ..Default::default()
    })
    .await;
    let root =
        cid::Cid::new_v1(0x70, SOURCE.parse::<cid::Cid>().unwrap().hash().to_owned()).to_string();
    Mock::given(method("POST"))
        .and(path("/api/v0/files/stat"))
        .and(query_param("arg", format!("/ipfs/{SOURCE}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Hash\":\"{SOURCE}\",\"CumulativeSize\":5}}")),
        )
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{\"ID\":\"localnode\"}"))
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/dag/put"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(format!("{{\"Cid\":{{\"/\":\"{root}\"}}}}")),
        )
        .expect(1)
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .and(query_param("arg", root.clone()))
        .respond_with(if failed_rpc == "pin/add" {
            ResponseTemplate::new(500).set_body_string("private root pin failure")
        } else {
            ResponseTemplate::new(200).set_body_string(format!("{{\"Pins\":[\"{root}\"]}}"))
        })
        .with_priority(1)
        .expect(1)
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/resolve"))
        .respond_with(if failed_rpc == "resolve" {
            ResponseTemplate::new(500).set_body_string("private root resolution failure")
        } else {
            ResponseTemplate::new(200).set_body_string(format!("{{\"Path\":\"/ipfs/{SOURCE}\"}}"))
        })
        .mount(&harness.kubo)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/ls"))
        .respond_with(
            ResponseTemplate::new(500).set_body_string("private root verification failure"),
        )
        .mount(&harness.kubo)
        .await;

    let accepted = post_import(
        &harness,
        &harness.bucket,
        "archive.zip",
        "ipfs3-import&decompress-zip=out%2F",
        &format!("<IPFS3ImportRequest><CID>{SOURCE}</CID></IPFS3ImportRequest>"),
        Some("root-failure"),
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let job = wait_for_import_state(&harness, &id, &["completed", "failed"]).await;
    assert_eq!(job.state, "completed", "{failed_rpc}");
    assert_eq!(
        job.attempts, 1,
        "root failure is not an import retry: {failed_rpc}"
    );
    assert!(job.failure_code.is_none());

    let status = get_import_status(&harness, &harness.bucket, "archive.zip", &id, None, None).await;
    assert_eq!(status.status(), StatusCode::OK);
    let status_xml = String::from_utf8(status.body().clone()).unwrap();
    assert!(
        status_xml.contains("<ZipRoot><Status>failed</Status>"),
        "{status_xml}"
    );
    assert!(status_xml.contains("<ErrorCode>directory_build_failed</ErrorCode>"));
    assert!(
        !status_xml.contains(&root),
        "unverified root CID must not be public"
    );
    assert!(!status_xml.contains("private root"));

    let snapshot = zip::snapshot(harness.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "failed");
    assert!(snapshot.batch.root_cid.is_none());
    assert_eq!(snapshot.entries.len(), 1);
    assert!(snapshot.entries[0].version_row_id.is_some());
    assert_eq!(snapshot.references.len(), 1);
    assert_eq!(snapshot.references[0].state, "retained");
    assert_eq!(snapshot.references[0].cid, root);
    assert!(snapshot.references[0].verification_receipt.is_none());
    assert_eq!(
        object::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );

    let replay = post_import(
        &harness,
        &harness.bucket,
        "archive.zip",
        "ipfs3-import&decompress-zip=out%2F",
        &format!("<IPFS3ImportRequest><CID>{SOURCE}</CID></IPFS3ImportRequest>"),
        Some("root-failure"),
    )
    .await;
    assert_eq!(replay.status(), StatusCode::ACCEPTED);
    assert_eq!(replay.headers()["x-ipfs3-import-job-id"], id);
    assert_eq!(
        object_version::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        import_job_result::Entity::find()
            .count(harness.state.store.db())
            .await
            .unwrap(),
        2
    );
    harness.shutdown().await;
}
