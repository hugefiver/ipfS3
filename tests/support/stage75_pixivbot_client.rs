//! SDK request-shape coverage for Pixivbot; not execution of the Pixivbot binary.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use http::{HeaderMap, HeaderValue, StatusCode};
use s3::request::tokio_backend::ReqwestRequest;
use s3::{Bucket, Region, command::Command, creds::Credentials, request::Request};
use wiremock::{Mock, ResponseTemplate, matchers};

use crate::{
    checks,
    fixture::{BUCKET, ENTRIES, Harness, KEY, PREFIX, TAGS},
    node::content_cid,
    support::decompress,
};

fn bucket(h: &Harness) -> Box<Bucket> {
    Bucket::new(
        BUCKET,
        Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: h.gateway.endpoint.clone(),
        },
        Credentials::new(Some("test"), Some("test"), None, None, None).unwrap(),
    )
    .unwrap()
    .with_path_style()
}

async fn create(bucket: &Bucket, key: &str, zip: bool) -> String {
    let mut create_bucket = bucket.clone();
    create_bucket.extra_headers.insert(
        "x-amz-decoded-content-length",
        HeaderValue::from_static("0"),
    );
    if zip {
        create_bucket.add_query("decompress-zip", PREFIX);
        create_bucket
            .extra_headers
            .insert("x-amz-tagging", HeaderValue::from_static(TAGS));
    }
    let created = create_bucket
        .initiate_multipart_upload(key, "application/octet-stream")
        .await
        .expect("Pixivbot SDK Create");
    assert_eq!(created.key, key);
    assert!(!created.upload_id.trim().is_empty());
    created.upload_id
}

// Pixivbot uses the low-level SDK request to retain the raw UploadPart ETag.
async fn part(bucket: &Bucket, key: &str, id: &str, number: u32, bytes: &[u8]) -> String {
    let request = ReqwestRequest::new(
        bucket,
        key,
        Command::PutObject {
            content: bytes,
            content_type: "application/octet-stream",
            custom_headers: None,
            multipart: Some(s3::command::Multipart::new(number, id)),
        },
    )
    .await
    .unwrap();
    let response = request.response().await.expect("SDK UploadPart");
    assert_eq!(response.status().as_u16(), 200);
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    response.bytes().await.unwrap();
    assert!(!etag.trim().is_empty());
    etag
}

async fn list_response(bucket: &Bucket, key: &str, id: &str) -> reqwest::Response {
    let query = HashMap::from([
        ("uploadId".to_owned(), id.to_owned()),
        ("part-number-marker".to_owned(), "0".to_owned()),
        ("max-parts".to_owned(), "1000".to_owned()),
    ]);
    let signed = bucket.presign_get(key, 60, Some(query)).await.unwrap();
    let url = reqwest::Url::parse(&signed).unwrap();
    assert!(
        url.query_pairs()
            .all(|(name, _)| !name.starts_with("decompress-"))
    );
    reqwest::Client::new().get(url).send().await.unwrap()
}

#[derive(Debug, PartialEq, Eq)]
struct ListedPart {
    number: u32,
    etag: String,
    size: u64,
}

fn root(xml: &str, expected: &str) {
    let mut reader = quick_xml::Reader::from_str(xml);
    loop {
        match reader.read_event().expect("valid response XML") {
            quick_xml::events::Event::Start(element) => {
                assert_eq!(element.name().as_ref(), expected.as_bytes());
                reader.read_to_end(element.name()).unwrap();
                break;
            }
            quick_xml::events::Event::Decl(_) | quick_xml::events::Event::Comment(_) => {}
            quick_xml::events::Event::Text(text) if text.iter().all(u8::is_ascii_whitespace) => {}
            event => panic!("expected {expected} root, got {event:?}"),
        }
    }
    loop {
        match reader.read_event().unwrap() {
            quick_xml::events::Event::Eof => break,
            quick_xml::events::Event::Text(text) if text.iter().all(u8::is_ascii_whitespace) => {}
            quick_xml::events::Event::Comment(_) => {}
            event => panic!("unexpected XML after {expected}: {event:?}"),
        }
    }
}

async fn inventory(bucket: &Bucket, key: &str, id: &str) -> Vec<ListedPart> {
    let response = list_response(bucket, key, id).await;
    let status = response.status();
    let xml = response.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "ListParts: {xml}");
    root(&xml, "ListPartsResult");
    assert_eq!(checks::xml_text(&xml, "Bucket"), BUCKET);
    assert_eq!(checks::xml_text(&xml, "Key"), key);
    assert_eq!(checks::xml_text(&xml, "UploadId"), id);
    // Current IpfS3 accepts either an unpaginated inventory or a standard final page.
    if xml.contains("<PartNumberMarker>") {
        assert_eq!(checks::xml_text(&xml, "PartNumberMarker"), "0");
        assert_eq!(checks::xml_text(&xml, "MaxParts"), "1000");
        assert_eq!(checks::xml_text(&xml, "IsTruncated"), "false");
    }
    let mut parts: Vec<_> = xml
        .split("<Part>")
        .skip(1)
        .map(|part| {
            let part = part.split("</Part>").next().unwrap();
            ListedPart {
                number: checks::xml_text(part, "PartNumber").parse().unwrap(),
                etag: checks::xml_text(part, "ETag"),
                size: checks::xml_text(part, "Size").parse().unwrap(),
            }
        })
        .collect();
    parts.sort_unstable_by_key(|part| part.number);
    parts
}

fn completed_parts(parts: Vec<ListedPart>) -> Vec<s3::serde_types::Part> {
    parts
        .into_iter()
        .map(|part| s3::serde_types::Part {
            part_number: part.number,
            etag: quick_xml::escape::escape(&part.etag).into_owned(),
        })
        .collect()
}

async fn recover_head(bucket: &Bucket, key: &str, bytes: &[u8], cid: &str) {
    let (head, status) = bucket.head_object(key).await.expect("SDK Head recovery");
    assert_eq!(status, 200);
    assert_eq!(head.content_length, Some(bytes.len() as i64));
    let etag = head.e_tag.expect("IpfS3ImageByLengthAndEtag requires ETag");
    assert_eq!(etag.trim().trim_matches('"'), cid);
    assert!(cid.parse::<cid::Cid>().is_ok());
    let get = bucket.get_object(key).await.unwrap();
    assert_eq!(get.status_code(), 200);
    assert_eq!(get.bytes().as_ref(), bytes);
}

// IpfS3's client re-signs Abort without Content-Length, which proxies may omit.
async fn abort(bucket: &Bucket, key: &str, id: &str) {
    let request = ReqwestRequest::new(bucket, key, Command::AbortMultipartUpload { upload_id: id })
        .await
        .unwrap();
    let mut headers = request.headers().await.unwrap();
    headers.remove("authorization");
    headers.remove("content-length");
    let date = headers.remove("date");
    let authorization = request.authorization(&headers).await.unwrap();
    headers.insert("authorization", authorization.parse().unwrap());
    if let Some(date) = date {
        headers.insert("date", date);
    }
    let response = bucket
        .http_client()
        .delete(request.url().unwrap())
        .headers(headers)
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    response.bytes().await.unwrap();
}

#[tokio::test]
async fn pixivbot_sdk_plain_put_returns_cid_and_head_recovery_evidence() {
    let h = Harness::start(false, false).await;
    let bucket = bucket(&h);
    let bytes = b"ordinary Pixivbot image bytes";
    let response = bucket
        .put_object_with_content_type("image.jpg", bytes, "image/jpeg")
        .await
        .expect("rust-s3 ordinary PutObject");
    assert_eq!(response.status_code(), 200);
    // This is rust-s3's ETag projection, not the raw HTTP response body.
    let cid = response.as_str().unwrap().trim().trim_matches('"');
    assert_eq!(cid, content_cid(0x55, bytes));
    recover_head(&bucket, "image.jpg", bytes, cid).await;
    assert_eq!(h.source.stored(cid), bytes);
    h.shutdown().await;
}

#[tokio::test]
async fn pixivbot_sdk_mpu_inventory_retries_replacement_complete_and_head() {
    let h = Harness::start_for_client_multipart().await;
    let bucket = bucket(&h);
    let key = "image-mpu.jpg";
    let id = create(&bucket, key, false).await;
    assert!(inventory(&bucket, key, &id).await.is_empty());
    // Pixivbot's first part is 8 MiB; only the final part can be smaller.
    let first = vec![7; 8 * 1024 * 1024];
    let tail = b"final image bytes";
    let tail_etag = part(&bucket, key, &id, 2, tail).await;
    let initial_etag = part(&bucket, key, &id, 1, &first).await;
    assert_eq!(part(&bucket, key, &id, 1, &first).await, initial_etag);
    let listed = inventory(&bucket, key, &id).await;
    assert_eq!(
        listed,
        vec![
            ListedPart {
                number: 1,
                etag: initial_etag.clone(),
                size: first.len() as u64
            },
            ListedPart {
                number: 2,
                etag: tail_etag.clone(),
                size: tail.len() as u64
            },
        ]
    );
    let replacement = vec![9; first.len()];
    let replaced_etag = part(&bucket, key, &id, 1, &replacement).await;
    assert_ne!(replaced_etag, initial_etag);
    let listed = inventory(&bucket, key, &id).await;
    assert_eq!(
        listed,
        vec![
            ListedPart {
                number: 1,
                etag: replaced_etag,
                size: replacement.len() as u64
            },
            ListedPart {
                number: 2,
                etag: tail_etag,
                size: tail.len() as u64
            },
        ]
    );
    let completed = bucket
        .complete_multipart_upload(key, &id, completed_parts(listed))
        .await
        .expect("SDK Complete with server-listed ETags");
    assert_eq!(completed.status_code(), 200);
    let xml = completed.as_str().unwrap();
    root(xml, "CompleteMultipartUploadResult");
    assert_eq!(checks::xml_text(xml, "Bucket"), BUCKET);
    assert_eq!(checks::xml_text(xml, "Key"), key);
    let mut bytes = replacement;
    bytes.extend_from_slice(tail);
    let cid = checks::xml_text(xml, "ETag");
    let cid = cid.trim_matches('"');
    assert_eq!(cid, content_cid(0x55, &bytes));
    let missing = list_response(&bucket, key, &id).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        checks::xml_text(&missing.text().await.unwrap(), "Code"),
        "NoSuchUpload"
    );
    recover_head(&bucket, key, &bytes, cid).await;
    let observed = h.observed.lock().await;
    for request in observed.iter().filter(|r| r.uri.path().ends_with(key)) {
        assert!(!request.headers.contains_key("x-amz-tagging"));
        let url = reqwest::Url::parse(&format!("http://localhost{}", request.uri)).unwrap();
        assert!(
            url.query_pairs()
                .all(|(name, _)| !name.starts_with("decompress-"))
        );
        if request.method == http::Method::GET
            && url.query_pairs().any(|(name, _)| name == "uploadId")
        {
            assert!(!request.headers.contains_key("authorization"));
            assert!(
                url.query_pairs()
                    .any(|(name, value)| name == "X-Amz-Algorithm" && value == "AWS4-HMAC-SHA256")
            );
            assert!(url.query_pairs().any(|(name, _)| name == "X-Amz-Signature"));
        }
    }
    drop(observed);
    h.shutdown().await;
}

#[tokio::test]
async fn pixivbot_sdk_abort_removes_inventory_without_publishing_object() {
    let h = Harness::start(false, false).await;
    let bucket = bucket(&h);
    let key = "aborted-image.jpg";
    let id = create(&bucket, key, false).await;
    part(&bucket, key, &id, 1, b"discarded image").await;
    assert_eq!(inventory(&bucket, key, &id).await.len(), 1);
    abort(&bucket, key, &id).await;
    let missing = list_response(&bucket, key, &id).await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        checks::xml_text(&missing.text().await.unwrap(), "Code"),
        "NoSuchUpload"
    );
    let get = h
        .send(reqwest::Method::GET, key, &[], vec![], HeaderMap::new())
        .await;
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        checks::xml_text(&get.text().await.unwrap(), "Code"),
        "NoSuchKey"
    );
    let replacement_id = create(&bucket, key, false).await;
    assert_ne!(replacement_id, id);
    assert!(inventory(&bucket, key, &replacement_id).await.is_empty());
    abort(&bucket, key, &replacement_id).await;
    h.shutdown().await;
}

#[tokio::test]
async fn pixivbot_auth_and_temporary_upload_errors_are_not_unsupported() {
    let h = Harness::start(false, false).await;
    let bucket = bucket(&h);
    let key = "retry-image.jpg";
    let id = create(&bucket, key, false).await;
    let denied = crate::support::sigv4::send_sigv4(
        reqwest::Method::GET,
        &h.gateway.endpoint,
        BUCKET,
        key,
        &[
            ("uploadId", &id),
            ("part-number-marker", "0"),
            ("max-parts", "1000"),
        ],
        vec![],
        HeaderMap::new(),
        "wrong-secret",
    )
    .await;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        checks::xml_text(&denied.text().await.unwrap(), "Code"),
        "SignatureDoesNotMatch"
    );
    let unavailable = Arc::new(AtomicBool::new(true));
    let outage = unavailable.clone();
    Mock::given(matchers::method("POST"))
        .and(matchers::path("/api/v0/add"))
        .and(move |_: &wiremock::Request| outage.load(Ordering::Acquire))
        .respond_with(ResponseTemplate::new(503))
        .with_priority(1)
        .expect(1..)
        .mount(&h.source.server)
        .await;
    let failure = bucket
        .put_multipart_chunk(b"retry me".to_vec(), key, 1, &id, "image/jpeg")
        .await
        .expect_err("backend failure must not report success");
    let s3::error::S3Error::HttpFailWithBody(status, body) = failure else {
        panic!("expected service error from real gateway: {failure:?}");
    };
    assert!((500..600).contains(&status));
    root(&body, "Error");
    let code = checks::xml_text(&body, "Code");
    assert!(!code.trim().is_empty());
    assert_ne!((status, code.as_str()), (501, "NotImplemented"));
    assert!(inventory(&bucket, key, &id).await.is_empty());
    unavailable.store(false, Ordering::Release);
    let etag = part(&bucket, key, &id, 1, b"retry me").await;
    assert_eq!(
        inventory(&bucket, key, &id).await,
        vec![ListedPart {
            number: 1,
            etag,
            size: 8,
        }]
    );
    abort(&bucket, key, &id).await;
    h.shutdown().await;
}

async fn zip_v1(mpu: bool) {
    let h = Harness::start(false, false).await;
    let bucket = bucket(&h);
    let bytes = decompress::legal_two_entry_zip();
    let response = if mpu {
        let id = create(&bucket, KEY, true).await;
        assert!(inventory(&bucket, KEY, &id).await.is_empty());
        part(&bucket, KEY, &id, 1, &bytes).await;
        let listed = inventory(&bucket, KEY, &id).await;
        bucket
            .complete_multipart_upload(KEY, &id, completed_parts(listed))
            .await
            .unwrap()
    } else {
        let mut upload_bucket = bucket.clone();
        upload_bucket.add_query("decompress-zip", PREFIX);
        upload_bucket
            .extra_headers
            .insert("x-amz-tagging", HeaderValue::from_static(TAGS));
        ReqwestRequest::new(
            &upload_bucket,
            KEY,
            Command::PutObject {
                content: &bytes,
                content_type: "application/zip",
                custom_headers: None,
                multipart: None,
            },
        )
        .await
        .unwrap()
        .response_data(false)
        .await
        .unwrap()
    };
    assert_eq!(response.status_code(), 200);
    let xml = response.as_str().unwrap();
    root(xml, "DecompressZipResult");
    assert_eq!(
        checks::xml_text(xml, "ExtractedCount"),
        ENTRIES.len().to_string()
    );
    assert_eq!(checks::xml_text(xml, "FailedCount"), "0");
    let entries: Vec<_> = xml.split("<Entry>").skip(1).collect();
    assert_eq!(entries.len(), ENTRIES.len());
    for (name, bytes) in ENTRIES {
        let key = format!("{PREFIX}{name}");
        let entry = entries
            .iter()
            .find(|entry| checks::xml_text(entry, "Key") == key)
            .unwrap();
        let etag = checks::xml_text(entry, "ETag");
        assert_eq!(etag.trim_matches('"'), content_cid(0x55, bytes));
        assert!(etag.trim_matches('"').parse::<cid::Cid>().is_ok());
        let get = bucket.get_object(&key).await.unwrap();
        assert_eq!(get.status_code(), 200);
        assert_eq!(get.bytes().as_ref(), bytes);
    }
    let observed = h.observed.lock().await;
    assert!(observed.iter().all(|r| r.method != http::Method::HEAD));
    let publications: Vec<_> = observed
        .iter()
        .filter(|r| {
            r.uri.path() == format!("/{BUCKET}/{KEY}") && r.headers.contains_key("x-amz-tagging")
        })
        .collect();
    assert_eq!(publications.len(), 1);
    assert_eq!(publications[0].headers["x-amz-tagging"], TAGS);
    assert_eq!(
        publications[0].method,
        if mpu {
            http::Method::POST
        } else {
            http::Method::PUT
        }
    );
    for request in observed
        .iter()
        .filter(|r| r.uri.path() == format!("/{BUCKET}/{KEY}"))
    {
        let url = reqwest::Url::parse(&format!("http://localhost{}", request.uri)).unwrap();
        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        if request.headers.contains_key("x-amz-tagging") {
            assert_eq!(
                query.get("decompress-zip").map(String::as_str),
                Some(PREFIX)
            );
        } else {
            assert!(!query.contains_key("decompress-zip"));
        }
    }
    drop(observed);
    h.shutdown().await;
}

#[tokio::test]
async fn pixivbot_sdk_zip_v1_put_preserves_query_tagging_and_entry_cids() {
    zip_v1(false).await;
}

#[tokio::test]
async fn pixivbot_sdk_zip_v1_mpu_preserves_query_tagging_and_entry_cids() {
    zip_v1(true).await;
}
