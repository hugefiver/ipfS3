#[allow(dead_code)]
mod support;

use http::{HeaderMap, HeaderValue, header};
use s3::{
    bucket::Bucket, bucket_ops::BucketConfiguration, creds::Credentials, error::S3Error,
    region::Region,
};
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use support::sigv4::send_sigv4;

const S3_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const IMPORT_TIMEOUT: Duration = Duration::from_secs(30);
static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn endpoint_from_env(name: &str) -> String {
    let endpoint = std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"));
    let endpoint = endpoint.trim_end_matches('/').to_owned();
    assert!(
        endpoint.starts_with("http://") || endpoint.starts_with("https://"),
        "{name} must be an HTTP(S) URL"
    );
    assert!(
        !endpoint.to_ascii_lowercase().contains("localhost"),
        "{name} must not use localhost"
    );
    endpoint
}

fn test_credentials() -> Credentials {
    Credentials::new(Some("test"), Some("test"), None, None, None).unwrap()
}

fn region_at(endpoint: &str) -> Region {
    Region::Custom {
        region: "us-east-1".to_owned(),
        endpoint: endpoint.to_owned(),
    }
}

fn bucket_at(endpoint: &str, name: &str) -> Box<Bucket> {
    Bucket::new(name, region_at(endpoint), test_credentials())
        .unwrap()
        .with_path_style()
}

async fn s3_call<T, F>(label: &str, future: F) -> Result<T, S3Error>
where
    F: Future<Output = Result<T, S3Error>>,
{
    tokio::time::timeout(S3_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("S3 operation timed out: {label}"))
}

async fn http_call<T, F>(label: &str, future: F) -> T
where
    F: Future<Output = T>,
{
    tokio::time::timeout(HTTP_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("HTTP operation timed out: {label}"))
}

fn unique_bucket(scenario: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    let counter = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let name = format!("mg-{scenario}-{}-{nanos:x}-{counter:x}", std::process::id());
    assert!(name.len() <= 63, "bucket name exceeds 63 characters");
    name
}

fn etag(headers: &std::collections::HashMap<String, String>) -> String {
    headers
        .get("etag")
        .or_else(|| headers.get("ETag"))
        .or_else(|| headers.get("e-tag"))
        .cloned()
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

async fn create_bucket_at(endpoint: &str, scenario: &str) -> (String, Box<Bucket>) {
    let name = unique_bucket(scenario);
    let response = s3_call(
        "create bucket",
        Bucket::create_with_path_style(
            &name,
            region_at(endpoint),
            test_credentials(),
            BucketConfiguration::default(),
        ),
    )
    .await
    .expect("create bucket must succeed");
    assert_eq!(response.response_code, 200, "create bucket must return 200");
    (name.clone(), bucket_at(endpoint, &name))
}

async fn assert_get(bucket: &Bucket, key: &str, expected: &[u8]) {
    let response = s3_call("get object", bucket.get_object(key))
        .await
        .expect("get object must succeed");
    assert_eq!(response.status_code(), 200);
    assert_eq!(response.bytes().as_ref(), expected);
}

async fn assert_head(bucket: &Bucket, key: &str, expected_etag: &str, expected_len: usize) {
    let (head, status) = s3_call("head object", bucket.head_object(key))
        .await
        .expect("head object must succeed");
    assert_eq!(status, 200);
    assert_eq!(
        head.e_tag.unwrap_or_default().trim_matches('"'),
        expected_etag
    );
    assert_eq!(head.content_length.unwrap_or_default(), expected_len as i64);
}

async fn assert_list_contains(bucket: &Bucket, key: &str) {
    let pages = s3_call("list objects", bucket.list(String::new(), None))
        .await
        .expect("list objects must succeed");
    assert!(
        pages
            .iter()
            .flat_map(|page| page.contents.iter())
            .any(|object| object.key == key),
        "list objects omitted {key}"
    );
}

async fn delete_object(bucket: &Bucket, key: &str) {
    let response = s3_call("delete object", bucket.delete_object(key))
        .await
        .expect("delete object must succeed");
    assert_eq!(response.status_code(), 204);
}

async fn delete_bucket(bucket: &Bucket) {
    let status = s3_call("delete bucket", bucket.delete())
        .await
        .expect("delete bucket must succeed");
    assert_eq!(status, 204);
}

async fn kubo_cat(endpoint: &str, cid: &str) -> Vec<u8> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("build Kubo client");
    let mut url = url::Url::parse(&format!("{endpoint}/api/v0/cat"))
        .expect("Kubo endpoint must be a valid URL");
    url.query_pairs_mut().append_pair("arg", cid);
    let response = http_call("Kubo cat", client.post(url).send())
        .await
        .expect("Kubo cat request must succeed");
    assert!(response.status().is_success(), "Kubo cat must succeed");
    response.bytes().await.expect("read Kubo cat body").to_vec()
}

async fn wait_for_import(endpoint: &str, bucket: &str, key: &str, job_id: &str, cid: &str) {
    tokio::time::timeout(IMPORT_TIMEOUT, async {
        loop {
            let response = http_call(
                "import status",
                send_sigv4(
                    reqwest::Method::GET,
                    endpoint,
                    bucket,
                    key,
                    &[("ipfs3-import", job_id)],
                    Vec::new(),
                    HeaderMap::new(),
                    "test",
                ),
            )
            .await;
            assert_eq!(response.status().as_u16(), 200);
            let body = response.text().await.expect("read import status XML");
            assert!(body.contains(&format!("<JobId>{job_id}</JobId>")));
            if body.contains("<State>failed</State>") {
                panic!("CID import entered failed state");
            }
            if body.contains("<State>completed</State>") {
                assert!(body.contains("<Artifact>"));
                assert!(body.contains(&format!("<CID>{cid}</CID>")));
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("CID import must complete within 30 seconds");
}

#[tokio::test]
async fn multi_gateway_cross_replica_contract() {
    let endpoint_a = endpoint_from_env("IPFS_S3_MULTI_GATEWAY_A_ENDPOINT");
    let endpoint_b = endpoint_from_env("IPFS_S3_MULTI_GATEWAY_B_ENDPOINT");
    let kubo_endpoint = endpoint_from_env("IPFS_S3_MULTI_GATEWAY_KUBO_URL");
    let (bucket_name, bucket_a) = create_bucket_at(&endpoint_a, "cross").await;
    let bucket_b = bucket_at(&endpoint_b, &bucket_name);

    let crud_body = b"written-through-a";
    let put = s3_call("A put crud.txt", bucket_a.put_object("crud.txt", crud_body))
        .await
        .expect("A put must succeed");
    assert_eq!(put.status_code(), 200);
    let crud_etag = etag(&put.headers());
    assert!(!crud_etag.is_empty());
    assert_get(&bucket_b, "crud.txt", crud_body).await;
    assert_head(&bucket_b, "crud.txt", &crud_etag, crud_body.len()).await;
    assert_list_contains(&bucket_b, "crud.txt").await;
    delete_object(&bucket_b, "crud.txt").await;
    let deleted = s3_call("A get deleted crud.txt", bucket_a.get_object("crud.txt"))
        .await
        .expect_err("deleted object must not be readable through A");
    let deleted_message = deleted.to_string();
    assert!(deleted_message.contains("404") || deleted_message.contains("NoSuchKey"));

    let first_part = vec![0x41; 5 * 1024 * 1024];
    let second_part = b"part-two-through-b".to_vec();
    let initiated = s3_call(
        "A initiate multipart",
        bucket_a.initiate_multipart_upload("multipart.bin", "application/octet-stream"),
    )
    .await
    .expect("multipart initiation through A must succeed");
    let part_one = s3_call(
        "A upload multipart part one",
        bucket_a.put_multipart_chunk(
            first_part.clone(),
            "multipart.bin",
            1,
            &initiated.upload_id,
            "application/octet-stream",
        ),
    )
    .await
    .expect("multipart part one through A must succeed");
    let part_two = s3_call(
        "B upload multipart part two",
        bucket_b.put_multipart_chunk(
            second_part.clone(),
            "multipart.bin",
            2,
            &initiated.upload_id,
            "application/octet-stream",
        ),
    )
    .await
    .expect("multipart part two through B must succeed");
    let completed = s3_call(
        "B complete multipart",
        bucket_b.complete_multipart_upload(
            "multipart.bin",
            &initiated.upload_id,
            vec![part_one, part_two],
        ),
    )
    .await
    .expect("multipart completion through B must succeed");
    assert_eq!(completed.status_code(), 200);
    let mut multipart_body = first_part;
    multipart_body.extend_from_slice(&second_part);
    assert_get(&bucket_a, "multipart.bin", &multipart_body).await;

    let race_key = "concurrent.bin";
    let payload_a = b"complete-payload-written-through-a".to_vec();
    let payload_b = b"complete-payload-written-through-b".to_vec();
    let (put_a, put_b) = tokio::join!(
        http_call(
            "concurrent PUT A",
            send_sigv4(
                reqwest::Method::PUT,
                &endpoint_a,
                &bucket_name,
                race_key,
                &[],
                payload_a.clone(),
                HeaderMap::new(),
                "test",
            )
        ),
        http_call(
            "concurrent PUT B",
            send_sigv4(
                reqwest::Method::PUT,
                &endpoint_b,
                &bucket_name,
                race_key,
                &[],
                payload_b.clone(),
                HeaderMap::new(),
                "test",
            )
        )
    );
    let statuses = [put_a.status().as_u16(), put_b.status().as_u16()];
    assert!(statuses.iter().all(|status| matches!(status, 200 | 409)));
    assert!(statuses.contains(&200));
    assert!(statuses.iter().all(|status| *status < 500));

    let get_a = s3_call("A get converged object", bucket_a.get_object(race_key))
        .await
        .expect("A converged GET must succeed");
    let get_b = s3_call("B get converged object", bucket_b.get_object(race_key))
        .await
        .expect("B converged GET must succeed");
    assert_eq!(get_a.bytes(), get_b.bytes());
    let final_body = get_a.bytes().to_vec();
    assert!(final_body == payload_a || final_body == payload_b);
    let (head_a, head_a_status) =
        s3_call("A head converged object", bucket_a.head_object(race_key))
            .await
            .expect("A converged HEAD must succeed");
    assert_eq!(head_a_status, 200);
    let (head_b, head_b_status) = s3_call("B head converged CID", bucket_b.head_object(race_key))
        .await
        .expect("B converged CID HEAD must succeed");
    assert_eq!(head_b_status, 200);
    let final_cid = head_a
        .e_tag
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned();
    let b_cid = head_b
        .e_tag
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned();
    assert!(!final_cid.is_empty());
    assert_eq!(final_cid, b_cid);
    assert_eq!(kubo_cat(&kubo_endpoint, &final_cid).await, final_body);

    let import_key = "imported.bin";
    let import_xml = format!("<IPFS3ImportRequest><CID>{final_cid}</CID></IPFS3ImportRequest>");
    let mut import_headers = HeaderMap::new();
    import_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let accepted = http_call(
        "submit CID import through A",
        send_sigv4(
            reqwest::Method::POST,
            &endpoint_a,
            &bucket_name,
            import_key,
            &[("ipfs3-import", "")],
            import_xml.into_bytes(),
            import_headers,
            "test",
        ),
    )
    .await;
    assert_eq!(accepted.status().as_u16(), 202);
    let job_id = accepted
        .headers()
        .get("x-ipfs3-import-job-id")
        .expect("accepted import must return job id")
        .to_str()
        .expect("import job id must be ASCII")
        .to_owned();
    assert!(!job_id.is_empty());
    wait_for_import(&endpoint_b, &bucket_name, import_key, &job_id, &final_cid).await;
    assert_get(&bucket_b, import_key, &final_body).await;

    for key in ["multipart.bin", race_key, import_key] {
        delete_object(&bucket_a, key).await;
    }
    delete_bucket(&bucket_a).await;
}

#[tokio::test]
async fn load_balancer_surviving_replica_crud() {
    let load_balancer = endpoint_from_env("IPFS_S3_MULTI_GATEWAY_LOAD_BALANCER_ENDPOINT");
    let (_bucket_name, bucket) = create_bucket_at(&load_balancer, "failover").await;
    let key = "after-failover.txt";
    let body = b"served-by-the-surviving-replica";
    let put = s3_call(
        "load-balancer PUT after failover",
        bucket.put_object(key, body),
    )
    .await
    .expect("load-balancer PUT after failover must succeed");
    assert_eq!(put.status_code(), 200);
    let cid = etag(&put.headers());
    assert!(!cid.is_empty());
    assert_get(&bucket, key, body).await;
    assert_head(&bucket, key, &cid, body.len()).await;
    assert_list_contains(&bucket, key).await;
    delete_object(&bucket, key).await;
    delete_bucket(&bucket).await;
}
