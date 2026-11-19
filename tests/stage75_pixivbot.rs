//! Signed Pixivbot v1 publication, replay and exact-version reads.

#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}
#[path = "support/stage75_pixivbot_assert.rs"]
mod checks;
#[path = "support/stage75_pixivbot_client.rs"]
mod client;
#[path = "support/stage75_pixivbot_fixture.rs"]
mod fixture;
#[path = "support/stage75_pixivbot_node.rs"]
mod node;
#[path = "support/pin_rows.rs"]
mod pin_rows;

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{
    self,
    entities::{object_version, zip_batch, zip_manifest_entry},
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

use fixture::{BUCKET, ENTRIES, Harness, KEY, PREFIX, TAGS};

fn tags() -> HeaderMap {
    HeaderMap::from_iter([(
        "x-amz-tagging".parse().unwrap(),
        HeaderValue::from_static(TAGS),
    )])
}

fn etag(response: &reqwest::Response) -> String {
    response.headers()["etag"]
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_owned()
}

async fn upload(h: &Harness, key: &str, bytes: Vec<u8>, zip: bool) -> (String, Vec<u8>) {
    let mut query = vec![("uploads", "")];
    if zip {
        query.push(("decompress-zip", PREFIX));
    }
    let created = h
        .send(
            reqwest::Method::POST,
            key,
            &query,
            vec![],
            if zip { tags() } else { HeaderMap::new() },
        )
        .await;
    let status = created.status();
    let xml = created.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "Create: {xml}");
    let id = checks::xml_text(&xml, "UploadId");
    let part = h
        .send(
            reqwest::Method::PUT,
            key,
            &[("uploadId", &id), ("partNumber", "1")],
            bytes,
            HeaderMap::new(),
        )
        .await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = etag(&part);
    if zip {
        assert!(
            pin_rows::refs(h.state.store.db()).await.is_empty(),
            "Create/UploadPart cannot publish pin intents"
        );
        assert!(h.target.calls().await.is_empty());
    }
    (id, format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"{part_etag}\"</ETag></Part></CompleteMultipartUpload>").into_bytes())
}

async fn case(mpu: bool, root: bool, auto: bool) {
    let h = Harness::start(root, auto).await;
    let archive = support::decompress::legal_two_entry_zip();
    // requested_entries_sha256 remains client-local; no fingerprint header.
    let completion = if mpu {
        Some(upload(&h, KEY, archive.clone(), true).await)
    } else {
        None
    };
    let response = if let Some((id, body)) = &completion {
        h.send(
            reqwest::Method::POST,
            KEY,
            &[("uploadId", id)],
            body.clone(),
            HeaderMap::new(),
        )
        .await
    } else {
        h.send(
            reqwest::Method::PUT,
            KEY,
            &[("decompress-zip", PREFIX)],
            archive.clone(),
            tags(),
        )
        .await
    };
    let status = response.status();
    let headers = response.headers().clone();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "Pixivbot v1: {xml}");
    checks::publication(&h, &headers, &xml, &archive, root, auto).await;
    checks::wire_shape(&h, mpu).await;
    let targets = ENTRIES.len() * if auto { 2 } else { 1 };
    h.run_worker(targets).await;
    checks::worker_evidence(&h, auto).await;

    let db = h.state.store.db();
    let versions = object_version::Entity::find().all(db).await.unwrap();
    let refs = pin_rows::refs(db).await;
    let resources = pin_rows::resources(db).await;
    let batches = zip_batch::Entity::find().all(db).await.unwrap();
    let manifests = zip_manifest_entry::Entity::find().all(db).await.unwrap();
    let source_calls = h.source.calls().await;
    let target_calls = h.target.calls().await;
    if let Some((id, body)) = &completion {
        let replay = h
            .send(
                reqwest::Method::POST,
                KEY,
                &[("uploadId", id)],
                body.clone(),
                HeaderMap::new(),
            )
            .await;
        assert_eq!(replay.status(), StatusCode::OK);
        for name in [
            "etag",
            "x-amz-version-id",
            "x-ipfs-s3-zip-batch-id",
            "x-ipfs-s3-zip-root-status",
        ] {
            assert_eq!(replay.headers()[name], headers[name]);
        }
        assert_eq!(
            replay.headers().get("x-ipfs-s3-zip-root-cid"),
            headers.get("x-ipfs-s3-zip-root-cid")
        );
        assert_eq!(replay.text().await.unwrap(), xml);
    }
    h.run_worker(targets).await;
    assert_eq!(h.source.calls().await, source_calls);
    assert_eq!(h.target.calls().await, target_calls);
    assert_eq!(
        object_version::Entity::find().all(db).await.unwrap(),
        versions
    );
    assert_eq!(pin_rows::refs(db).await, refs);
    assert_eq!(pin_rows::resources(db).await, resources);
    assert_eq!(zip_batch::Entity::find().all(db).await.unwrap(), batches);
    assert_eq!(
        zip_manifest_entry::Entity::find().all(db).await.unwrap(),
        manifests
    );

    // Current-key replacement cannot change the exact extracted version.
    let key = format!("{PREFIX}first.txt");
    let original = store::object::get_latest(db, BUCKET, &key).await.unwrap();
    let version = versions
        .iter()
        .find(|v| v.object_id.as_deref() == Some(original.id.as_str()))
        .unwrap();
    let overwrite = h
        .send(
            reqwest::Method::PUT,
            &key,
            &[],
            b"new current bytes".to_vec(),
            HeaderMap::new(),
        )
        .await;
    assert_eq!(overwrite.status(), StatusCode::OK);
    assert_ne!(
        overwrite.headers()["x-amz-version-id"].to_str().unwrap(),
        version.version_id.as_deref().unwrap()
    );
    checks::exact_get(
        &h,
        &key,
        version.version_id.as_deref().unwrap(),
        &original.cid,
        ENTRIES[0].1,
    )
    .await;
    for (version_id, has_manual) in [
        (version.version_id.as_deref().unwrap(), true),
        (
            overwrite.headers()["x-amz-version-id"].to_str().unwrap(),
            false,
        ),
    ] {
        let status = h
            .send(
                reqwest::Method::GET,
                &key,
                &[("ipfs3-pin-status", ""), ("versionId", version_id)],
                vec![],
                HeaderMap::new(),
            )
            .await;
        assert_eq!(status.status(), StatusCode::OK);
        assert_eq!(status.headers()["x-amz-version-id"], version_id);
        let status: serde_json::Value =
            serde_json::from_slice(&status.bytes().await.unwrap()).unwrap();
        let manual = status["references"]
            .as_array()
            .unwrap()
            .iter()
            .find(|reference| reference["source"] == "manual");
        assert_eq!(
            manual.is_some(),
            has_manual,
            "manual reference belongs to the extracted version"
        );
        if let Some(manual) = manual {
            assert_eq!(manual["cid"], original.cid);
        }
    }
    h.shutdown().await;
}

macro_rules! acceptance {
    ($name:ident, $mpu:literal, $root:literal, $auto:literal) => {
        #[tokio::test]
        async fn $name() {
            Box::pin(case($mpu, $root, $auto)).await;
        }
    };
}

acceptance!(v1_put_root_off_auto_off, false, false, false);
acceptance!(v1_put_root_off_auto_on, false, false, true);
acceptance!(v1_put_root_on_auto_off, false, true, false);
acceptance!(v1_put_root_on_auto_on, false, true, true);
acceptance!(v1_mpu_root_off_auto_off, true, false, false);
acceptance!(v1_mpu_root_off_auto_on, true, false, true);
acceptance!(v1_mpu_root_on_auto_off, true, true, false);
acceptance!(v1_mpu_root_on_auto_on, true, true, true);

#[tokio::test]
async fn ordinary_put_get_mpu_copy_and_exact_version_remain_available() {
    let h = Harness::start(true, true).await;
    let original = b"ordinary first version";
    let response = h
        .send(
            reqwest::Method::PUT,
            "plain",
            &[],
            original.to_vec(),
            HeaderMap::new(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let version = response.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let cid = etag(&response);
    let replacement = b"ordinary replacement";
    let response = h
        .send(
            reqwest::Method::PUT,
            "plain",
            &[],
            replacement.to_vec(),
            HeaderMap::new(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let current = h
        .send(reqwest::Method::GET, "plain", &[], vec![], HeaderMap::new())
        .await;
    assert_eq!(current.status(), StatusCode::OK);
    assert_eq!(current.bytes().await.unwrap().as_ref(), replacement);
    checks::exact_get(&h, "plain", &version, &cid, original).await;
    let copy = h
        .send(
            reqwest::Method::PUT,
            "copy",
            &[],
            vec![],
            HeaderMap::from_iter([(
                "x-amz-copy-source".parse().unwrap(),
                HeaderValue::from_str(&format!("/{BUCKET}/plain?versionId={version}")).unwrap(),
            )]),
        )
        .await;
    assert_eq!(copy.status(), StatusCode::OK);
    let copy_version = copy.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        checks::xml_text(&copy.text().await.unwrap(), "ETag").trim_matches('"'),
        cid
    );
    checks::exact_get(&h, "copy", &copy_version, &cid, original).await;
    let bytes = b"ordinary multipart final part";
    let (id, body) = upload(&h, "plain-mpu", bytes.to_vec(), false).await;
    let completed = h
        .send(
            reqwest::Method::POST,
            "plain-mpu",
            &[("uploadId", &id)],
            body,
            HeaderMap::new(),
        )
        .await;
    assert_eq!(completed.status(), StatusCode::OK);
    let mpu_version = completed.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    checks::exact_get(
        &h,
        "plain-mpu",
        &mpu_version,
        &node::content_cid(0x55, bytes),
        bytes,
    )
    .await;
    assert!(
        zip_batch::Entity::find()
            .all(h.state.store.db())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        object_version::Entity::find()
            .filter(object_version::Column::Key.eq("plain"))
            .all(h.state.store.db())
            .await
            .unwrap()
            .len(),
        2
    );
    h.shutdown().await;
}
