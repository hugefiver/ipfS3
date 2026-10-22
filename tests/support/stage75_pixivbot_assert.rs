use std::collections::{BTreeMap, BTreeSet};

use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{
    pinning::provider::ObservedResourceStatus,
    store::{
        self,
        entities::{
            object, object_version, pin_lease, pin_lease_target, remote_pin, zip_batch,
            zip_manifest_entry, zip_root_reference,
        },
        pinning::{ledger, quota},
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

use crate::{
    fixture::{BUCKET, ENTRIES, Harness, KEY, PREFIX, TAGS},
    node::content_cid,
};

pub fn xml_text(xml: &str, field: &str) -> String {
    let mut reader = quick_xml::Reader::from_str(xml);
    loop {
        match reader.read_event().expect("valid XML response") {
            quick_xml::events::Event::Start(event) if event.name().as_ref() == field.as_bytes() => {
                let text = reader.read_text(event.name()).unwrap();
                return quick_xml::escape::unescape(std::str::from_utf8(&text).unwrap())
                    .unwrap()
                    .into_owned();
            }
            quick_xml::events::Event::Eof => panic!("missing XML field {field}: {xml}"),
            _ => {}
        }
    }
}

pub async fn exact_get(h: &Harness, key: &str, version: &str, cid: &str, bytes: &[u8]) {
    let response = h
        .send(
            reqwest::Method::GET,
            key,
            &[("versionId", version)],
            vec![],
            HeaderMap::new(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-version-id"], version);
    assert_eq!(response.headers()["etag"], format!("\"{cid}\""));
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
}

pub async fn publication(
    h: &Harness,
    headers: &HeaderMap,
    xml: &str,
    archive_bytes: &[u8],
    root: bool,
    auto: bool,
) {
    assert!(xml.contains("<DecompressZipResult>"), "v1 XML: {xml}");
    assert_eq!(xml_text(xml, "ExtractedCount"), "2");
    assert_eq!(xml_text(xml, "FailedCount"), "0");
    assert_eq!(xml_text(xml, "ArchiveKey"), KEY);
    assert_eq!(
        xml_text(xml, "ArchiveSize"),
        archive_bytes.len().to_string()
    );
    let db = h.state.store.db();
    let archive = store::object::get_latest(db, BUCKET, KEY).await.unwrap();
    assert_eq!(archive.cid, content_cid(0x55, archive_bytes));
    assert_eq!(xml_text(xml, "ArchiveETag"), archive.cid);
    assert_eq!(headers["etag"], format!("\"{}\"", archive.cid));
    let archive_version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&archive.id))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        headers["x-amz-version-id"],
        archive_version.version_id.as_deref().unwrap()
    );
    exact_get(
        h,
        KEY,
        archive_version.version_id.as_deref().unwrap(),
        &archive.cid,
        archive_bytes,
    )
    .await;
    let batch_id = headers["x-ipfs-s3-zip-batch-id"].to_str().unwrap();
    let batch = zip_batch::Entity::find_by_id(batch_id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.state, "published");
    assert!(batch.manifest_prepared && batch.source_published && batch.terminal_result.is_some());
    assert_eq!(
        batch.root_status,
        if root { "complete" } else { "disabled" }
    );
    assert_eq!(headers["x-ipfs-s3-zip-root-status"], batch.root_status);
    let manifest = zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(batch_id))
        .all(db)
        .await
        .unwrap();
    assert_eq!(manifest.len(), ENTRIES.len());
    let xml_entries: Vec<_> = xml
        .split("<Entry>")
        .skip(1)
        .map(|entry| entry.split("</Entry>").next().unwrap())
        .collect();
    assert_eq!(xml_entries.len(), ENTRIES.len());
    let mut owners = BTreeSet::new();
    for (name, bytes) in ENTRIES {
        let key = format!("{PREFIX}{name}");
        let output = store::object::get_latest(db, BUCKET, &key).await.unwrap();
        let row = manifest.iter().find(|row| row.path == name).unwrap();
        assert_eq!(row.object_key.as_deref(), Some(key.as_str()));
        assert_eq!(row.cid.as_deref(), Some(output.cid.as_str()));
        assert_eq!(row.size, Some(bytes.len() as i64));
        assert!(row.error_code.is_none());
        let version = object_version::Entity::find_by_id(row.version_row_id.as_deref().unwrap())
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version.object_id.as_deref(), Some(output.id.as_str()));
        let entry = xml_entries
            .iter()
            .find(|entry| xml_text(entry, "Key") == key)
            .unwrap();
        assert_eq!(
            xml_text(entry, "ETag"),
            output.cid,
            "Entry.ETag is CID, not MD5/local fingerprint"
        );
        assert_eq!(xml_text(entry, "Size"), bytes.len().to_string());
        assert_eq!(output.cid, content_cid(0x55, bytes));
        assert_eq!(output.size, bytes.len() as i64);
        exact_get(
            h,
            &key,
            version.version_id.as_deref().unwrap(),
            &output.cid,
            bytes,
        )
        .await;
        owners.insert(output.id);
        if root {
            let root_cid = batch.root_cid.as_deref().unwrap();
            assert_eq!(headers["x-ipfs-s3-zip-root-cid"], root_cid);
            // Exercise the actual Kubo HTTP path read, derived from dag/put links.
            let path = format!("/ipfs/{root_cid}/{name}");
            let mut url =
                reqwest::Url::parse(&format!("{}/api/v0/cat", h.source.server.uri())).unwrap();
            url.query_pairs_mut().append_pair("arg", &path);
            let response = reqwest::Client::new().post(url).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap().as_ref(), bytes);
        }
    }
    let roots = zip_root_reference::Entity::find()
        .filter(zip_root_reference::Column::BatchId.eq(batch_id))
        .all(db)
        .await
        .unwrap();
    if root {
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].cid, batch.root_cid.as_deref().unwrap());
        assert!(roots[0].verification_receipt.is_some());
        assert!(h.source.calls().await["/api/v0/dag/put"] > 0);
        assert_eq!(h.source.calls().await["/api/v0/resolve"], ENTRIES.len());
    } else {
        assert!(roots.is_empty());
        assert!(batch.root_cid.is_none());
        assert!(headers.get("x-ipfs-s3-zip-root-cid").is_none());
        assert!(!h.source.calls().await.contains_key("/api/v0/dag/put"));
    }
    let leases = pin_lease::Entity::find().all(db).await.unwrap();
    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    assert_eq!(leases.len(), 1 + if auto { ENTRIES.len() } else { 0 });
    let manual: Vec<_> = leases
        .iter()
        .filter(|lease| lease.source == "manual")
        .collect();
    assert_eq!(manual.len(), 1);
    let manual = manual[0];
    assert_eq!(manual.owner_object_id, archive.id);
    assert_eq!(manual.content_mode, "decompressed");
    assert_eq!(
        (manual.expires_at - manual.created_at).num_seconds(),
        86400,
        "Pixivbot 1d must not be clamped"
    );
    let automatic: Vec<_> = leases
        .iter()
        .filter(|lease| lease.source == "automatic")
        .collect();
    assert_eq!(
        automatic
            .iter()
            .map(|lease| lease.owner_object_id.clone())
            .collect::<BTreeSet<_>>(),
        if auto { owners } else { BTreeSet::new() }
    );
    for lease in &leases {
        assert_eq!(lease.state, "active");
        let bound: Vec<_> = targets
            .iter()
            .filter(|target| target.lease_id == lease.id)
            .collect();
        assert_eq!(
            bound.len(),
            if lease.source == "manual" {
                ENTRIES.len()
            } else {
                1
            }
        );
        for target in bound {
            let (_, bytes) = ENTRIES
                .iter()
                .find(|(_, bytes)| content_cid(0x55, bytes) == target.cid)
                .unwrap();
            assert_eq!(target.logical_size, bytes.len() as i64);
            assert_eq!(target.provider, h.provider);
            if lease.source == "automatic" {
                assert_eq!(lease.content_mode, "full");
                let output = object::Entity::find_by_id(&lease.owner_object_id)
                    .one(db)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(target.cid, output.cid);
            }
        }
    }
    assert_eq!(targets.len(), ENTRIES.len() * if auto { 2 } else { 1 });
}

pub async fn worker_evidence(h: &Harness, auto: bool) {
    let db = h.state.store.db();
    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    assert_eq!(targets.len(), ENTRIES.len() * if auto { 2 } else { 1 });
    assert!(targets.iter().all(|target| target.state == "pinned"));
    let remotes = remote_pin::Entity::find().all(db).await.unwrap();
    let observations = ledger::submission::observations(db, &h.provider)
        .await
        .unwrap();
    assert_eq!(remotes.len(), ENTRIES.len());
    assert_eq!(observations.len(), ENTRIES.len());
    for (_, bytes) in ENTRIES {
        let cid = content_cid(0x55, bytes);
        assert_eq!(h.target.stored(&cid), bytes);
        let remote = remotes.iter().find(|row| row.cid == cid).unwrap();
        assert_eq!(remote.status, "pinned");
        assert_eq!(remote.cid_size, bytes.len() as i64);
        let observation = observations
            .iter()
            .find(|row| row.expected_cid == cid)
            .unwrap();
        assert_eq!(
            (
                observation.effect.as_str(),
                observation.outcome.as_str(),
                observation.submit_call
            ),
            ("observed", "matched", 1)
        );
        assert!(!observation.needs_attention);
        assert!(observation.observed_at.is_some());
        let resources: Vec<ledger::submission::ResourceEvidence> =
            serde_json::from_str(&observation.resources).unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].resource.cid, cid);
        assert_eq!(
            resources[0].resource.status,
            ObservedResourceStatus::RecursiveVerified
        );
        assert_eq!(
            remote.request_id.as_deref(),
            Some(resources[0].resource.request_id.as_str())
        );
        let durable = ledger::get(db, &h.provider, &cid).await.unwrap().unwrap();
        assert_eq!(durable.effect, "confirmed");
        assert!(durable.remote_pinned_at.is_some());
    }
    let usage = quota::read_usage(db, &h.provider).await.unwrap().unwrap();
    assert_eq!(
        (usage.reserved_pins, usage.reserved_bytes),
        (2, ENTRIES.iter().map(|(_, bytes)| bytes.len() as i64).sum())
    );
    assert_eq!(
        h.target.calls().await,
        BTreeMap::from([
            ("/api/v0/add".into(), 2),
            ("/api/v0/pin/add".into(), 2),
            ("/api/v0/pin/ls".into(), 4),
            ("/api/v0/files/stat".into(), 2),
            ("/api/v0/id".into(), 4),
        ])
    );
}

pub async fn wire_shape(h: &Harness, mpu: bool) {
    let observed = h.observed.lock().await;
    for request in observed.iter() {
        assert!(!request.headers.contains_key("x-ipfs3-zip-contract"));
        assert!(!request.headers.contains_key("x-ipfs3-zip-expected-sha256"));
        assert!(!request.headers.contains_key("requested_entries_sha256"));
        assert!(
            request.headers["authorization"]
                .to_str()
                .unwrap()
                .starts_with("AWS4-HMAC-SHA256 ")
        );
    }
    let publication = observed
        .iter()
        .find(|r| {
            r.uri.path() == format!("/{BUCKET}/{KEY}")
                && r.uri.query().is_some_and(|q| q.contains("decompress-zip"))
        })
        .unwrap();
    assert_eq!(
        publication.method,
        if mpu {
            http::Method::POST
        } else {
            http::Method::PUT
        }
    );
    let uri = reqwest::Url::parse(&format!("http://127.0.0.1{}", publication.uri)).unwrap();
    let query: BTreeMap<String, String> = uri.query_pairs().into_owned().collect();
    let mut expected = BTreeMap::from([("decompress-zip".to_owned(), PREFIX.to_owned())]);
    if mpu {
        expected.insert("uploads".to_owned(), String::new());
    }
    assert_eq!(query, expected);
    assert_eq!(publication.headers["x-amz-tagging"], TAGS);
    for request in observed
        .iter()
        .filter(|r| r.uri.query().is_some_and(|q| q.contains("uploadId=")))
    {
        assert!(!request.uri.query().unwrap().contains("decompress-zip"));
        assert!(!request.headers.contains_key("x-amz-tagging"));
    }
}
