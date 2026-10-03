//! Signed ZIP extraction keeps S3 keys whose paths cannot form a UnixFS root.
#[allow(dead_code)]
mod support;

use std::collections::HashMap;

use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store::{object, zip};
use support::decompress::{
    AddReply, KuboScript, legal_two_entry_zip, start_harness_with_root_default,
};

fn legacy_invalid_path_archive() -> Vec<u8> {
    let mut archive = legal_two_entry_zip();
    let mut renamed = 0;
    for offset in 0..archive.len() - b"first.txt".len() {
        if &archive[offset..offset + b"first.txt".len()] == b"first.txt" {
            archive[offset..offset + b"first.txt".len()].copy_from_slice(b"a//b1.txt");
            renamed += 1;
        }
    }
    assert_eq!(renamed, 2); // Local and central ZIP headers.
    archive
}

#[tokio::test]
async fn invalid_root_path_keeps_legacy_s3_outputs_when_root_on_or_off() {
    for enabled in [true, false] {
        let archive = legacy_invalid_path_archive();
        let harness = start_harness_with_root_default(
            KuboScript {
                add_replies: vec![
                    AddReply::Ok("QmArchive"),
                    AddReply::Ok("QmInvalid"),
                    AddReply::Ok("QmValid"),
                ],
                cat_bodies: HashMap::from([("QmArchive".to_owned(), archive.clone())]),
            },
            enabled,
        )
        .await;
        let response = support::sigv4::send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &harness.bucket,
            "archive.zip",
            &[("decompress-zip", "expanded/")],
            archive,
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["x-ipfs-s3-zip-root-status"],
            if enabled { "failed" } else { "disabled" }
        );
        if enabled {
            assert_eq!(
                response.headers()["x-ipfs-s3-zip-root-warning"],
                "invalid_manifest"
            );
        }
        assert!(response.headers().get("x-ipfs-s3-zip-root-cid").is_none());
        for (key, cid) in [
            ("expanded/a//b1.txt", "QmInvalid"),
            ("expanded/second.txt", "QmValid"),
        ] {
            assert_eq!(
                object::get_latest(harness.state.store.db(), &harness.bucket, key)
                    .await
                    .unwrap()
                    .cid,
                cid
            );
        }
        let batch_id = response.headers()["x-ipfs-s3-zip-batch-id"]
            .to_str()
            .unwrap();
        let snapshot = zip::snapshot(harness.state.store.db(), batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.entries.len(), 2);
        let invalid = snapshot
            .entries
            .iter()
            .find(|e| e.object_key.as_deref() == Some("expanded/a//b1.txt"))
            .unwrap();
        assert_eq!(invalid.path, "invalid/0");
        assert!(invalid.version_row_id.is_some());
        assert!(snapshot.entries.iter().all(|e| e.version_row_id.is_some()));
        assert!(
            !harness
                .kubo
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| {
                    matches!(request.url.path(), "/api/v0/dag/put" | "/api/v0/resolve")
                })
        );
    }
}
