//! R26 real-user-surface regression, not final R26 / PostgreSQL acceptance.

#[path = "support/legacy_pinning_migration_surface.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/mod.rs"]
mod support;

use fixture::{ARCHIVE_CID, BODY, Blocker, DATA_CID, ENTRY, ENTRY_CID, Fixture, db};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::store::{self, entities::pin_lease_target};
use sea_orm::EntityTrait;
use support::decompress::{S3TestEndpoint, complete_multipart, legal_single_entry_zip};
use support::sigv4::send_sigv4;

fn manual(zip: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_static(if zip {
            "ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed"
        } else {
            "ipfs-s3%3Apin=true"
        }),
    );
    headers
}

async fn put(f: &Fixture, key: &str, zip: bool, pin: bool) -> reqwest::Response {
    let headers = if pin { manual(zip) } else { HeaderMap::new() };
    let query: &[(&str, &str)] = if zip {
        &[("decompress-zip", "expanded/"), ("return-result", "true")]
    } else {
        &[]
    };
    send_sigv4(
        reqwest::Method::PUT,
        f.endpoint(),
        f.bucket(),
        key,
        query,
        if zip {
            legal_single_entry_zip()
        } else {
            BODY.to_vec()
        },
        headers,
        "test",
    )
    .await
}

async fn copy(f: &Fixture) -> reqwest::Response {
    let mut headers = manual(false);
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_static("/test-bkt/source"),
    );
    headers.insert(
        "x-amz-tagging-directive",
        HeaderValue::from_static("REPLACE"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        f.endpoint(),
        f.bucket(),
        "copy",
        &[],
        vec![],
        headers,
        "test",
    )
    .await
}

async fn multipart(f: &Fixture) -> (String, String) {
    let response = send_sigv4(
        reqwest::Method::POST,
        f.endpoint(),
        f.bucket(),
        "mpu",
        &[("uploads", "")],
        vec![],
        manual(false),
        "test",
    )
    .await;
    let (status, _, xml) = wire(response).await;
    assert_eq!(status, StatusCode::OK, "CreateMultipartUpload: {xml}");
    let upload_id = xml_text(&xml, "UploadId");
    let response = send_sigv4(
        reqwest::Method::PUT,
        f.endpoint(),
        f.bucket(),
        "mpu",
        &[("partNumber", "1"), ("uploadId", &upload_id)],
        BODY.to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "UploadPart");
    let etag = response.headers()[http::header::ETAG]
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_owned();
    (upload_id, etag)
}

fn xml_text(xml: &str, tag: &str) -> String {
    let mut reader = quick_xml::Reader::from_str(xml);
    loop {
        match reader.read_event().unwrap() {
            quick_xml::events::Event::Start(e) if e.name().as_ref() == tag.as_bytes() => {
                let text = reader.read_text(e.name()).unwrap();
                return quick_xml::escape::unescape(std::str::from_utf8(text.as_ref()).unwrap())
                    .unwrap()
                    .into_owned();
            }
            quick_xml::events::Event::Eof => panic!("missing {tag}: {xml}"),
            _ => {}
        }
    }
}

async fn wire(response: reqwest::Response) -> (StatusCode, HeaderMap, String) {
    let status = response.status();
    let headers = response.headers().clone();
    (status, headers, response.text().await.unwrap())
}

async fn blocked(f: &Fixture, operation: &str, response: reqwest::Response, before: &db::Snapshot) {
    let (status, _, xml) = wire(response).await;
    eprintln!("{:?} before {operation}: {status} {xml}", f.blocker);
    assert_eq!(status, StatusCode::BAD_REQUEST, "{operation}: {xml}");
    assert_eq!(xml_text(&xml, "Code"), "InvalidArgument");
    assert!(xml.contains("unscoped legacy remote capacity requires explicit account migration"));
    let after = db::snapshot(f.db()).await;
    // ZIP intake is durable independently of publication. Its captured config
    // is deliberately kept and declared; it is not a published object/lease.
    for table in [
        "objects",
        "object_versions",
        "object_tags",
        "pin_extension_decisions",
        "pin_leases",
        "pin_lease_targets",
        "pin_lease_requirements",
        "pin_jobs",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
        "pin_resource_history",
        "pin_submit_history",
        "pin_invocation_routes",
        "pin_submit_observations",
        "pin_replica_observations",
        "pin_legacy_migrations",
        "pin_legacy_migration_items",
    ] {
        assert_eq!(
            after[table], before[table],
            "blocked {operation} left {table} residue"
        );
    }
}

async fn regression(blocker: Blocker) {
    let mut f = Fixture::new(blocker).await;
    // Real untagged control proves auth / gateway / Kubo are usable before migration.
    let (status, headers, xml) = wire(put(&f, "source", false, false).await).await;
    assert_eq!(status, StatusCode::OK, "untagged PUT control: {xml}");
    assert_eq!(headers[http::header::ETAG], format!("\"{DATA_CID}\""));
    let (upload_id, etag) = multipart(&f).await;
    let upload = store::multipart::get_upload(f.db(), &upload_id)
        .await
        .unwrap();
    let parts = store::multipart::list_parts(f.db(), &upload_id)
        .await
        .unwrap();
    let before = db::snapshot(f.db()).await;
    db::assert_fixture_shape(&before, blocker, &f.identity);
    blocked(&f, "PUT", put(&f, "put", false, true).await, &before).await;
    blocked(&f, "Copy", copy(&f).await, &before).await;
    blocked(
        &f,
        "CompleteMPU",
        complete_multipart(&f, "mpu", &upload_id, &[(1, etag.clone())]).await,
        &before,
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(f.db(), &upload_id)
            .await
            .unwrap(),
        upload
    );
    assert_eq!(
        store::multipart::list_parts(f.db(), &upload_id)
            .await
            .unwrap(),
        parts
    );
    blocked(
        &f,
        "legacy ZIP",
        put(&f, "archive.zip", true, true).await,
        &before,
    )
    .await;
    f.stop().await;
    let receipt = f.migrate().await;
    f.restart().await;
    db::assert_migrated(f.db(), blocker, &f.identity).await;
    let migrated = db::snapshot(f.db()).await;

    let (status, headers, xml) = wire(put(&f, "put", false, true).await).await;
    eprintln!("{blocker:?} after PUT: {status}");
    assert_eq!(status, StatusCode::OK, "after PUT: {xml}");
    assert_eq!(headers[http::header::ETAG], format!("\"{DATA_CID}\""));
    let (status, _, xml) = wire(copy(&f).await).await;
    eprintln!("{blocker:?} after Copy: {status} {xml}");
    assert_eq!(status, StatusCode::OK, "after Copy: {xml}");
    assert_eq!(xml_text(&xml, "ETag"), format!("\"{DATA_CID}\""));
    let (status, _, xml) =
        wire(complete_multipart(&f, "mpu", &upload_id, &[(1, etag)]).await).await;
    eprintln!("{blocker:?} after CompleteMPU: {status} {xml}");
    assert_eq!(status, StatusCode::OK, "after Complete: {xml}");
    assert_eq!(xml_text(&xml, "ETag"), format!("\"{DATA_CID}\""));
    let (status, headers, xml) = wire(put(&f, "archive.zip", true, true).await).await;
    eprintln!("{blocker:?} after legacy ZIP: {status} {xml}");
    assert_eq!(status, StatusCode::OK, "after legacy ZIP: {xml}");
    assert_eq!(headers[http::header::ETAG], format!("\"{ARCHIVE_CID}\""));
    assert_eq!(xml_text(&xml, "ExtractedCount"), "1");
    assert_eq!(xml_text(&xml, "FailedCount"), "0");

    let (base_bytes, base_pins) = blocker.capacity();
    // PUT / Copy / Complete share one remote resource, ZIP pins only its entry.
    db::assert_usage(
        f.db(),
        &f.identity,
        (
            base_bytes + BODY.len() as i64 + ENTRY.len() as i64,
            base_pins + 2,
        ),
    )
    .await;
    eprintln!(
        "{blocker:?} conserved quota: before={base_bytes}/{base_pins}, after={}/{}; shared PUT/Copy/MPU=one hold, ZIP=entry only",
        base_bytes + BODY.len() as i64 + ENTRY.len() as i64,
        base_pins + 2
    );
    db::assert_unknown(f.db(), blocker, &f.identity).await;
    let state = db::snapshot(f.db()).await;
    assert_eq!(
        state["pin_legacy_migrations"].len(),
        1,
        "no new receipt on publication"
    );
    assert_eq!(
        state["pin_legacy_migrations"][0]["id"].as_deref(),
        receipt["id"].as_str()
    );
    assert_eq!(
        state["pin_legacy_migration_items"], migrated["pin_legacy_migration_items"],
        "new publication must not mutate or duplicate receipt items"
    );
    assert_eq!(state["pin_leases"].len(), 4);
    assert_eq!(
        state["remote_pins"].len(),
        if matches!(blocker, Blocker::AccountedRemote) {
            3
        } else {
            2
        }
    );
    let archive = store::object::get_latest(f.db(), f.bucket(), "archive.zip")
        .await
        .unwrap();
    let entry = store::object::get_latest(f.db(), f.bucket(), "expanded/file.txt")
        .await
        .unwrap();
    let targets = pin_lease_target::Entity::find().all(f.db()).await.unwrap();
    assert_eq!(targets.len(), 4);
    assert!(
        targets
            .iter()
            .all(|t| t.provider == f.identity.allocation_key() && t.state == "waiting")
    );
    let lease = state["pin_leases"]
        .iter()
        .find(|r| r["owner_object_id"].as_deref() == Some(&archive.id))
        .unwrap();
    assert_eq!(lease["source"].as_deref(), Some("manual"));
    assert_eq!(lease["content_mode"].as_deref(), Some("decompressed"));
    assert!(
        state["pin_leases"]
            .iter()
            .all(|r| r["owner_object_id"].as_deref() != Some(&entry.id))
    );
    let target = targets
        .iter()
        .find(|t| t.lease_id == *lease["id"].as_ref().unwrap())
        .unwrap();
    assert_eq!(
        (target.provider.as_str(), target.cid.as_str()),
        (f.identity.allocation_key().as_str(), ENTRY_CID)
    );
    for (key, expected) in [
        ("put", BODY),
        ("copy", BODY),
        ("mpu", BODY),
        ("expanded/file.txt", ENTRY),
    ] {
        let response = send_sigv4(
            reqwest::Method::GET,
            f.endpoint(),
            f.bucket(),
            key,
            &[],
            vec![],
            HeaderMap::new(),
            "test",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    }
    let requests = f.kubo.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.url.path() == "/api/v0/add")
            .count(),
        10
    );
    assert!(!requests.iter().any(|r| r.url.path() == "/api/v0/pin/rm"));
    f.stop().await;
}

#[tokio::test]
async fn usage_only_debt_cli_migration_restores_real_s3_surfaces() {
    regression(Blocker::UsageOnly).await;
}

#[tokio::test]
async fn accounted_alias_remote_cli_migration_restores_real_s3_surfaces() {
    regression(Blocker::AccountedRemote).await;
}

#[tokio::test]
async fn domain_null_route_binding_only_restores_real_s3_surfaces_without_confirming_unknown() {
    regression(Blocker::DomainNullRoute).await;
}
