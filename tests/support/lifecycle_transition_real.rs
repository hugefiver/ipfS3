//! Real PostgreSQL, dual-Kubo, dual-gateway lifecycle-transition scenarios.

use std::time::{Duration, Instant};

use http::{HeaderMap, HeaderValue, StatusCode, header};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use serde::Deserialize;

use crate::lifecycle_transition_real_http as api;

const TRANSITION_TIMEOUT: Duration = Duration::from_secs(120);
const SSE_C_KEY: [u8; 32] = [7; 32];
const WRONG_SSE_C_KEY: [u8; 32] = [8; 32];
const STRESS_FIXTURE_BYTES: usize = 256 * 1024 * 1024;
const MIN_STRESS_CAR_BYTES: u64 = 192 * 1024 * 1024;

#[derive(Clone, Debug)]
struct RealEndpoints {
    gateway_a: String,
    gateway_b: String,
    hot_kubo: String,
    cold_kubo: String,
    postgres_url: String,
}

#[derive(Clone, Debug)]
struct UploadedObject {
    key: String,
    body: Vec<u8>,
    cid: String,
    version_id: String,
}

#[derive(Deserialize)]
struct CarProxyMetrics {
    import_requests: u64,
    completed_car_bytes: Vec<u64>,
    total_car_bytes: u64,
    max_car_bytes: u64,
    parse_failures: u64,
    in_flight: u64,
}

#[derive(Clone, Copy)]
enum Encryption {
    Plain,
    SseS3,
    SseC([u8; 32]),
}

impl RealEndpoints {
    fn required() -> Self {
        let gateway_a = api::endpoint_from_env("IPFS_S3_TRANSITION_A_ENDPOINT");
        let gateway_b = api::endpoint_from_env("IPFS_S3_TRANSITION_B_ENDPOINT");
        let hot_kubo = api::endpoint_from_env("IPFS_S3_TRANSITION_HOT_URL");
        let cold_kubo = api::endpoint_from_env("IPFS_S3_TRANSITION_COLD_URL");
        let postgres_url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap_or_else(|_| {
            panic!("NOT RUN: IPFS_S3_TEST_POSTGRES_URL is required for real transition tests")
        });
        assert_ne!(gateway_a, gateway_b, "two gateway endpoints are required");
        assert_ne!(hot_kubo, cold_kubo, "hot and cold Kubo URLs must differ");
        Self {
            gateway_a,
            gateway_b,
            hot_kubo,
            cold_kubo,
            postgres_url,
        }
    }

    async fn assert_all_healthy(&self) -> DatabaseConnection {
        assert_gateway_healthy(&self.gateway_a, "gateway A").await;
        assert_gateway_healthy(&self.gateway_b, "gateway B").await;
        let hot_id = kubo_id(&self.hot_kubo).await;
        let cold_id = kubo_id(&self.cold_kubo).await;
        assert_ne!(hot_id, cold_id, "hot and cold Kubo node IDs must differ");
        connect_pg17(&self.postgres_url).await
    }
}

pub async fn current_transition_matrix() {
    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = unique_bucket("current");
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        current_lifecycle_xml("real-current-transition", "current/"),
    )
    .await;
    api::assert_lifecycle(
        &endpoints.gateway_b,
        &bucket,
        &[
            "<ID>real-current-transition</ID>",
            "<Prefix>current/</Prefix>",
            "<Transition>",
            "<Days>1</Days>",
            "<StorageClass>STANDARD_IA</StorageClass>",
        ],
    )
    .await;

    let plain = upload(
        &endpoints.gateway_a,
        &bucket,
        "current/plain.bin",
        unique_body(&bucket, "plain-current"),
        api::with_metadata(HeaderMap::new(), "plain-current"),
    )
    .await;
    let sse_s3 = upload(
        &endpoints.gateway_b,
        &bucket,
        "current/sse-s3.bin",
        unique_body(&bucket, "sse-s3-current"),
        api::with_metadata(api::sse_s3_headers(), "sse-s3-current"),
    )
    .await;
    let sse_c = upload(
        &endpoints.gateway_a,
        &bucket,
        "current/sse-c.bin",
        unique_body(&bucket, "sse-c-current"),
        api::with_metadata(api::sse_c_headers(SSE_C_KEY), "sse-c-current"),
    )
    .await;
    api::put_object_tagging(
        &endpoints.gateway_b,
        &bucket,
        &plain.key,
        "null",
        "transition-suite",
        "immutable",
    )
    .await;
    for object in [&plain, &sse_s3, &sse_c] {
        assert_eq!(object.version_id, "null", "suspended PUT version ID");
        assert!(
            kubo_cat_optional(&endpoints.cold_kubo, &object.cid)
                .await
                .is_none(),
            "fresh CID unexpectedly existed on independent cold Kubo: {}",
            object.cid
        );
    }

    let plain_hot = kubo_cat(&endpoints.hot_kubo, &plain.cid).await;
    assert_eq!(plain_hot, plain.body, "plain bytes stored on hot Kubo");
    let sse_s3_ciphertext = kubo_cat(&endpoints.hot_kubo, &sse_s3.cid).await;
    let sse_c_ciphertext = kubo_cat(&endpoints.hot_kubo, &sse_c.cid).await;
    assert_ne!(
        sse_s3_ciphertext, sse_s3.body,
        "SSE-S3 must store ciphertext"
    );
    assert_ne!(sse_c_ciphertext, sse_c.body, "SSE-C must store ciphertext");

    assert_eq!(age_current_prefix(&db, &bucket, "current/").await, 3);
    for (object, encryption) in [
        (&plain, Encryption::Plain),
        (&sse_s3, Encryption::SseS3),
        (&sse_c, Encryption::SseC(SSE_C_KEY)),
    ] {
        wait_for_transition(
            &endpoints.gateway_b,
            &db,
            &bucket,
            object,
            Some("null"),
            encryption,
            "transition_current",
        )
        .await;
    }

    assert_eq!(kubo_cat(&endpoints.cold_kubo, &plain.cid).await, plain_hot);
    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &sse_s3.cid).await,
        sse_s3_ciphertext,
        "SSE-S3 envelope bytes must be copied immutably"
    );
    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &sse_c.cid).await,
        sse_c_ciphertext,
        "SSE-C envelope bytes must be copied immutably"
    );

    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &plain,
        None,
        Encryption::Plain,
        "plain-current",
    )
    .await;
    assert_full_get(
        &endpoints.gateway_b,
        &bucket,
        &plain,
        Some("null"),
        Encryption::Plain,
        "plain-current",
    )
    .await;
    assert_head(
        &endpoints.gateway_a,
        &bucket,
        &plain,
        Some("null"),
        Encryption::Plain,
        "plain-current",
    )
    .await;
    assert_range(
        &endpoints.gateway_b,
        &bucket,
        &plain,
        Some("null"),
        Encryption::Plain,
    )
    .await;

    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &sse_s3,
        None,
        Encryption::SseS3,
        "sse-s3-current",
    )
    .await;
    assert_head(
        &endpoints.gateway_b,
        &bucket,
        &sse_s3,
        Some("null"),
        Encryption::SseS3,
        "sse-s3-current",
    )
    .await;
    assert_range(
        &endpoints.gateway_a,
        &bucket,
        &sse_s3,
        Some("null"),
        Encryption::SseS3,
    )
    .await;

    assert_full_get(
        &endpoints.gateway_b,
        &bucket,
        &sse_c,
        Some("null"),
        Encryption::SseC(SSE_C_KEY),
        "sse-c-current",
    )
    .await;
    assert_head(
        &endpoints.gateway_a,
        &bucket,
        &sse_c,
        Some("null"),
        Encryption::SseC(SSE_C_KEY),
        "sse-c-current",
    )
    .await;
    assert_range(
        &endpoints.gateway_b,
        &bucket,
        &sse_c,
        Some("null"),
        Encryption::SseC(SSE_C_KEY),
    )
    .await;
    assert_wrong_sse_c_key(&endpoints.gateway_a, &bucket, &sse_c).await;

    for (endpoint, v2) in [(&endpoints.gateway_a, false), (&endpoints.gateway_b, true)] {
        let xml = api::list_objects(endpoint, &bucket, "current/", v2).await;
        for object in [&plain, &sse_s3, &sse_c] {
            assert_list_object_class(&xml, &object.key, "STANDARD_IA");
        }
    }
    let versions = api::list_versions(&endpoints.gateway_b, &bucket, "current/").await;
    for object in [&plain, &sse_s3, &sse_c] {
        assert_version_class(&versions, &object.key, "null", "STANDARD_IA");
    }
    api::sdk_assert_get(&endpoints.gateway_b, &bucket, &plain.key, &plain.body).await;
    api::assert_object_tagging(
        &endpoints.gateway_a,
        &bucket,
        &plain.key,
        "null",
        "transition-suite",
        "immutable",
    )
    .await;

    let copy = api::copy_object(
        &endpoints.gateway_b,
        &bucket,
        &plain.key,
        "copied/from-ia.bin",
    )
    .await;
    let copy_status = copy.status();
    let copy_body = copy.text().await.expect("read CopyObject response");
    assert_eq!(
        copy_status,
        StatusCode::OK,
        "IA-to-STANDARD copy: {copy_body}"
    );
    let copied = current_object_from_head(
        &endpoints.gateway_a,
        &bucket,
        "copied/from-ia.bin",
        &plain.body,
        "null",
        "STANDARD",
    )
    .await;
    assert_eq!(kubo_cat(&endpoints.hot_kubo, &copied.cid).await, plain.body);
    assert_standard_current(&endpoints.gateway_b, &bucket, &copied).await;
    assert_head(
        &endpoints.gateway_a,
        &bucket,
        &plain,
        Some("null"),
        Encryption::Plain,
        "plain-current",
    )
    .await;

    let mut ia_headers = HeaderMap::new();
    ia_headers.insert(
        "x-amz-storage-class",
        HeaderValue::from_static("STANDARD_IA"),
    );
    let rejected = api::put_object(
        &endpoints.gateway_a,
        &bucket,
        "direct-ia-rejected.bin",
        b"must-not-publish",
        ia_headers,
    )
    .await;
    let rejected_status = rejected.status();
    let rejected_body = rejected.text().await.expect("read direct IA rejection");
    assert_eq!(rejected_status, StatusCode::BAD_REQUEST, "{rejected_body}");
    api::assert_s3_error(rejected_status, &rejected_body, "InvalidRequest");
    assert!(
        rejected_body.contains("direct writes only support STANDARD storage class"),
        "unexpected direct IA rejection: {rejected_body}"
    );
    evidence(
        "current-matrix",
        "lifecycle_xml=pass headers=pass bytes=pass ranges=pass encryption_sse_s3_sse_c=pass wrong_key=pass lists=pass copy_to_standard=pass",
    );

    api::delete_lifecycle(&endpoints.gateway_a, &bucket).await;
    for object in [&plain, &sse_s3, &sse_c, &copied] {
        api::delete_version(&endpoints.gateway_b, &bucket, &object.key, "null").await;
    }
    api::sdk_delete_bucket(&endpoints.gateway_a, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn noncurrent_transition_matrix() {
    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = unique_bucket("noncurrent");
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        noncurrent_lifecycle_xml("real-noncurrent-transition", "history/"),
    )
    .await;
    api::assert_lifecycle(
        &endpoints.gateway_b,
        &bucket,
        &[
            "<ID>real-noncurrent-transition</ID>",
            "<NoncurrentVersionTransition>",
            "<NoncurrentDays>1</NoncurrentDays>",
            "<StorageClass>STANDARD_IA</StorageClass>",
        ],
    )
    .await;

    api::put_versioning(&endpoints.gateway_a, &bucket, "Enabled").await;
    let public_old = upload(
        &endpoints.gateway_a,
        &bucket,
        "history/public.bin",
        unique_body(&bucket, "public-old"),
        api::with_metadata(HeaderMap::new(), "public-old"),
    )
    .await;
    let public_current = upload(
        &endpoints.gateway_b,
        &bucket,
        "history/public.bin",
        unique_body(&bucket, "public-current"),
        api::with_metadata(HeaderMap::new(), "public-current"),
    )
    .await;
    assert_ne!(public_old.version_id, public_current.version_id);

    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    let null_old = upload(
        &endpoints.gateway_a,
        &bucket,
        "history/null.bin",
        unique_body(&bucket, "null-old"),
        api::with_metadata(HeaderMap::new(), "null-old"),
    )
    .await;
    assert_eq!(null_old.version_id, "null");
    api::put_versioning(&endpoints.gateway_a, &bucket, "Enabled").await;
    let null_current = upload(
        &endpoints.gateway_b,
        &bucket,
        "history/null.bin",
        unique_body(&bucket, "null-current"),
        api::with_metadata(HeaderMap::new(), "null-current"),
    )
    .await;

    for object in [&public_old, &null_old] {
        assert!(
            kubo_cat_optional(&endpoints.cold_kubo, &object.cid)
                .await
                .is_none(),
            "fresh noncurrent CID unexpectedly existed on cold Kubo"
        );
    }
    assert_eq!(
        age_noncurrent(&db, &bucket, &public_old.key, Some(&public_old.version_id)).await,
        1
    );
    assert_eq!(age_noncurrent(&db, &bucket, &null_old.key, None).await, 1);

    wait_for_transition(
        &endpoints.gateway_b,
        &db,
        &bucket,
        &public_old,
        Some(&public_old.version_id),
        Encryption::Plain,
        "transition_noncurrent",
    )
    .await;
    wait_for_transition(
        &endpoints.gateway_a,
        &db,
        &bucket,
        &null_old,
        Some("null"),
        Encryption::Plain,
        "transition_noncurrent",
    )
    .await;

    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &public_old.cid).await,
        public_old.body
    );
    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &null_old.cid).await,
        null_old.body
    );
    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &public_old,
        Some(&public_old.version_id),
        Encryption::Plain,
        "public-old",
    )
    .await;
    assert_range(
        &endpoints.gateway_b,
        &bucket,
        &public_old,
        Some(&public_old.version_id),
        Encryption::Plain,
    )
    .await;
    assert_head(
        &endpoints.gateway_b,
        &bucket,
        &null_old,
        Some("null"),
        Encryption::Plain,
        "null-old",
    )
    .await;
    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &null_old,
        Some("null"),
        Encryption::Plain,
        "null-old",
    )
    .await;

    for object in [&public_current, &null_current] {
        assert_standard_current(&endpoints.gateway_b, &bucket, object).await;
    }
    for (endpoint, v2) in [(&endpoints.gateway_a, false), (&endpoints.gateway_b, true)] {
        let xml = api::list_objects(endpoint, &bucket, "history/", v2).await;
        assert_list_object_class(&xml, &public_current.key, "STANDARD");
        assert_list_object_class(&xml, &null_current.key, "STANDARD");
        assert!(
            !xml.contains(&public_old.version_id),
            "ordinary list leaked version ID"
        );
    }
    let versions = api::list_versions(&endpoints.gateway_a, &bucket, "history/").await;
    assert_version_class(
        &versions,
        &public_old.key,
        &public_old.version_id,
        "STANDARD_IA",
    );
    assert_version_class(
        &versions,
        &public_current.key,
        &public_current.version_id,
        "STANDARD",
    );
    assert_version_class(&versions, &null_old.key, "null", "STANDARD_IA");
    assert_version_class(
        &versions,
        &null_current.key,
        &null_current.version_id,
        "STANDARD",
    );
    api::sdk_assert_get(
        &endpoints.gateway_b,
        &bucket,
        &public_current.key,
        &public_current.body,
    )
    .await;
    evidence(
        "noncurrent-matrix",
        "lifecycle_xml=pass headers=pass bytes=pass public_version=pass null_version=pass lists=pass",
    );

    api::delete_lifecycle(&endpoints.gateway_b, &bucket).await;
    for object in [&public_old, &public_current, &null_old, &null_current] {
        api::delete_version(
            &endpoints.gateway_a,
            &bucket,
            &object.key,
            &object.version_id,
        )
        .await;
    }
    api::sdk_delete_bucket(&endpoints.gateway_b, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn nondefault_raw_leaf_import_integrity() {
    const CHUNK_SIZE: usize = 64 * 1024;
    const BODY_SIZE: usize = 600 * 1024 + 17;

    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = unique_bucket("import-dag");
    let key = "integrity/import/nondefault.bin";
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        current_lifecycle_xml("real-nondefault-import-transition", "integrity/import/"),
    )
    .await;

    let body = deterministic_high_entropy_body(&bucket, "nondefault-import", BODY_SIZE);
    let cid = kubo_add_nondefault(&endpoints.hot_kubo, body.clone(), CHUNK_SIZE).await;
    assert_nondefault_raw_leaf_layout(&endpoints.hot_kubo, &cid, BODY_SIZE, CHUNK_SIZE).await;
    assert_local_full_dag(&endpoints.hot_kubo, &cid).await;
    assert!(
        kubo_cat_optional(&endpoints.cold_kubo, &cid)
            .await
            .is_none(),
        "nondefault import CID unexpectedly existed on cold Kubo before transition"
    );

    let job_id = api::submit_cid_import(&endpoints.gateway_a, &bucket, key, &cid).await;
    api::wait_for_cid_import(
        &endpoints.gateway_b,
        &bucket,
        key,
        &job_id,
        &cid,
        body.len(),
    )
    .await;
    let imported = UploadedObject {
        key: key.to_owned(),
        body,
        cid,
        version_id: "null".to_owned(),
    };
    assert_standard_current(&endpoints.gateway_a, &bucket, &imported).await;
    assert_eq!(
        age_current_prefix(&db, &bucket, "integrity/import/").await,
        1
    );
    wait_for_transition(
        &endpoints.gateway_b,
        &db,
        &bucket,
        &imported,
        Some("null"),
        Encryption::Plain,
        "transition_current",
    )
    .await;

    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &imported,
        Some("null"),
        Encryption::Plain,
        "nondefault-import",
    )
    .await;
    assert_range_between(
        &endpoints.gateway_b,
        &bucket,
        &imported,
        Some("null"),
        CHUNK_SIZE - 24,
        CHUNK_SIZE + 47,
    )
    .await;
    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &imported.cid).await,
        imported.body,
        "cold Kubo must reconstruct the imported nondefault UnixFS bytes"
    );
    assert_nondefault_raw_leaf_layout(&endpoints.cold_kubo, &imported.cid, BODY_SIZE, CHUNK_SIZE)
        .await;
    assert_local_full_dag(&endpoints.cold_kubo, &imported.cid).await;
    evidence(
        "nondefault-import",
        "signed_xml=pass headers=pass bytes=pass range_crosses_64k=pass layout=cidv1_size65536_raw_leaves cold_full_dag=pass",
    );

    api::delete_lifecycle(&endpoints.gateway_a, &bucket).await;
    api::delete_version(&endpoints.gateway_b, &bucket, key, "null").await;
    api::sdk_delete_bucket(&endpoints.gateway_a, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn empty_and_multiblock_integrity() {
    const DEFAULT_BLOCK_BOUNDARY: usize = 256 * 1024;
    let large_size = std::env::var("IPFS_S3_TRANSITION_LARGE_BYTES")
        .map(|value| {
            value
                .parse::<usize>()
                .expect("large fixture size is numeric")
        })
        .unwrap_or(700 * 1024 + 29);
    assert!((700 * 1024..=512 * 1024 * 1024).contains(&large_size));

    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = unique_bucket("empty-multiblock");
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        current_lifecycle_xml("real-empty-multiblock-transition", "integrity/data/"),
    )
    .await;

    let empty = upload(
        &endpoints.gateway_a,
        &bucket,
        "integrity/data/empty.bin",
        Vec::new(),
        api::with_metadata(HeaderMap::new(), "empty-object"),
    )
    .await;
    let large = upload(
        &endpoints.gateway_b,
        &bucket,
        "integrity/data/multiblock.bin",
        deterministic_high_entropy_body(&bucket, "multiblock", large_size),
        api::with_metadata(HeaderMap::new(), "multiblock-object"),
    )
    .await;
    let car_proxy_url = if large_size >= STRESS_FIXTURE_BYTES {
        let url = std::env::var("IPFS_S3_TRANSITION_CAR_PROXY_URL").unwrap_or_else(|_| {
            panic!(
                "NOT RUN: IPFS_S3_TRANSITION_CAR_PROXY_URL is required for a 256 MiB stress fixture"
            )
        });
        reset_car_proxy_metrics(&url).await;
        Some(url)
    } else {
        None
    };
    assert_eq!(age_current_prefix(&db, &bucket, "integrity/data/").await, 2);
    for object in [&empty, &large] {
        wait_for_transition(
            &endpoints.gateway_a,
            &db,
            &bucket,
            object,
            Some("null"),
            Encryption::Plain,
            "transition_current",
        )
        .await;
    }

    assert_full_get(
        &endpoints.gateway_b,
        &bucket,
        &empty,
        Some("null"),
        Encryption::Plain,
        "empty-object",
    )
    .await;
    assert_head(
        &endpoints.gateway_a,
        &bucket,
        &empty,
        Some("null"),
        Encryption::Plain,
        "empty-object",
    )
    .await;
    assert_range_between(
        &endpoints.gateway_b,
        &bucket,
        &large,
        Some("null"),
        DEFAULT_BLOCK_BOUNDARY - 37,
        DEFAULT_BLOCK_BOUNDARY + 73,
    )
    .await;
    assert_full_get(
        &endpoints.gateway_a,
        &bucket,
        &large,
        Some("null"),
        Encryption::Plain,
        "multiblock-object",
    )
    .await;
    for object in [&empty, &large] {
        assert_eq!(
            kubo_cat(&endpoints.cold_kubo, &object.cid).await,
            object.body
        );
        assert_local_full_dag(&endpoints.cold_kubo, &object.cid).await;
    }
    if let Some(proxy_url) = car_proxy_url {
        let metrics = read_car_proxy_metrics(&proxy_url).await;
        assert_eq!(metrics.in_flight, 0, "all measured DAG imports must finish");
        assert_eq!(
            metrics.parse_failures, 0,
            "the proxy must parse every measured multipart DAG import"
        );
        assert!(
            metrics.import_requests > 0,
            "the gateway must import through the CAR proxy"
        );
        assert_eq!(
            metrics.import_requests as usize,
            metrics.completed_car_bytes.len(),
            "every observed DAG import must publish an exact CAR byte count"
        );
        assert!(
            metrics.max_car_bytes > MIN_STRESS_CAR_BYTES,
            "actual largest CAR file part was {} bytes, expected more than {} bytes",
            metrics.max_car_bytes,
            MIN_STRESS_CAR_BYTES
        );
        eprintln!(
            "[LIFECYCLE-TRANSITION-EVIDENCE] scenario=empty-multiblock actual_car_file_bytes={} total_car_file_bytes={} imports={}",
            metrics.max_car_bytes, metrics.total_car_bytes, metrics.import_requests
        );
    }
    evidence(
        "empty-multiblock",
        "headers=pass empty_bytes=pass multiblock_bytes=pass range_crosses_256k=pass cold_full_dag=pass",
    );
    eprintln!(
        "[LIFECYCLE-TRANSITION-EVIDENCE] scenario=empty-multiblock logical_bytes={large_size}"
    );

    api::delete_lifecycle(&endpoints.gateway_b, &bucket).await;
    for object in [&empty, &large] {
        api::delete_version(&endpoints.gateway_a, &bucket, &object.key, "null").await;
    }
    api::sdk_delete_bucket(&endpoints.gateway_b, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

async fn reset_car_proxy_metrics(proxy_url: &str) {
    let response = reqwest::Client::new()
        .post(format!(
            "{}/__car_proxy/metrics/reset",
            proxy_url.trim_end_matches('/')
        ))
        .send()
        .await
        .expect("reset CAR proxy metrics");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

async fn read_car_proxy_metrics(proxy_url: &str) -> CarProxyMetrics {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/__car_proxy/metrics",
            proxy_url.trim_end_matches('/')
        ))
        .send()
        .await
        .expect("read CAR proxy metrics");
    assert_eq!(response.status(), StatusCode::OK);
    response
        .json()
        .await
        .expect("decode CAR proxy metrics response")
}

pub async fn multipart_root_integrity() {
    const FIRST_PART_SIZE: usize = 5 * 1024 * 1024;
    const SECOND_PART_SIZE: usize = 384 * 1024 + 53;

    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = unique_bucket("multipart-root");
    let key = "integrity/multipart/root.bin";
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        current_lifecycle_xml("real-multipart-root-transition", "integrity/multipart/"),
    )
    .await;

    let first = deterministic_high_entropy_body(&bucket, "multipart-first", FIRST_PART_SIZE);
    let second = deterministic_high_entropy_body(&bucket, "multipart-second", SECOND_PART_SIZE);
    let upload_id = api::create_multipart_upload(&endpoints.gateway_a, &bucket, key).await;
    let first_etag = api::upload_part(
        &endpoints.gateway_a,
        &bucket,
        key,
        &upload_id,
        1,
        first.clone(),
    )
    .await;
    let second_etag = api::upload_part(
        &endpoints.gateway_b,
        &bucket,
        key,
        &upload_id,
        2,
        second.clone(),
    )
    .await;
    let root_cid = api::complete_multipart_upload(
        &endpoints.gateway_b,
        &bucket,
        key,
        &upload_id,
        &[(1, first_etag), (2, second_etag)],
    )
    .await;
    let mut body = first;
    body.extend_from_slice(&second);
    let completed = UploadedObject {
        key: key.to_owned(),
        body,
        cid: root_cid,
        version_id: "null".to_owned(),
    };
    assert_standard_current(&endpoints.gateway_a, &bucket, &completed).await;
    assert!(
        kubo_cat_optional(&endpoints.cold_kubo, &completed.cid)
            .await
            .is_none(),
        "multipart root unexpectedly existed on cold Kubo before transition"
    );
    assert_eq!(
        age_current_prefix(&db, &bucket, "integrity/multipart/").await,
        1
    );
    wait_for_transition(
        &endpoints.gateway_b,
        &db,
        &bucket,
        &completed,
        Some("null"),
        Encryption::Plain,
        "transition_current",
    )
    .await;

    assert_range_between(
        &endpoints.gateway_a,
        &bucket,
        &completed,
        Some("null"),
        FIRST_PART_SIZE - 41,
        FIRST_PART_SIZE + 79,
    )
    .await;
    let response = api::get_object(
        &endpoints.gateway_b,
        &bucket,
        key,
        Some("null"),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD_IA");
    assert_eq!(
        response.headers()[header::ETAG],
        format!("\"{}\"", completed.cid)
    );
    assert_eq!(
        response.bytes().await.expect("read multipart root"),
        completed.body
    );
    assert_eq!(
        kubo_cat(&endpoints.cold_kubo, &completed.cid).await,
        completed.body,
        "cold Kubo must reconstruct the complete multipart root"
    );
    assert_local_full_dag(&endpoints.cold_kubo, &completed.cid).await;
    evidence(
        "multipart-root",
        "signed_multipart_xml=pass headers=pass bytes=pass range_crosses_part_boundary=pass cold_full_dag=pass",
    );

    api::delete_lifecycle(&endpoints.gateway_a, &bucket).await;
    api::delete_version(&endpoints.gateway_b, &bucket, key, "null").await;
    api::sdk_delete_bucket(&endpoints.gateway_a, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn prepare_backend_stop_fixture() {
    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = topology_bucket();
    api::sdk_create_bucket(&endpoints.gateway_a, &bucket).await;
    api::put_versioning(&endpoints.gateway_b, &bucket, "Suspended").await;
    api::put_lifecycle(
        &endpoints.gateway_a,
        &bucket,
        current_lifecycle_xml("backend-stop-transition", "tier/ia/"),
    )
    .await;
    let ia = upload(
        &endpoints.gateway_a,
        &bucket,
        "tier/ia/object.bin",
        topology_body("cold"),
        HeaderMap::new(),
    )
    .await;
    let hot = upload(
        &endpoints.gateway_b,
        &bucket,
        "tier/hot/object.bin",
        topology_body("hot"),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(age_current_prefix(&db, &bucket, "tier/ia/").await, 1);
    wait_for_transition(
        &endpoints.gateway_b,
        &db,
        &bucket,
        &ia,
        Some("null"),
        Encryption::Plain,
        "transition_current",
    )
    .await;
    assert_standard_current(&endpoints.gateway_a, &bucket, &hot).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn hot_stopped_fixture_read() {
    let endpoints = RealEndpoints::required();
    let db = connect_pg17(&endpoints.postgres_url).await;
    assert_gateway_healthy(&endpoints.gateway_a, "gateway A").await;
    assert_gateway_healthy(&endpoints.gateway_b, "gateway B").await;
    assert_backend_unavailable(&endpoints.hot_kubo, "hot Kubo").await;
    let _cold_id = kubo_id(&endpoints.cold_kubo).await;
    let bucket = topology_bucket();
    let expected = topology_body("cold");
    let response = api::get_object(
        &endpoints.gateway_b,
        &bucket,
        "tier/ia/object.bin",
        Some("null"),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK, "IA GET with hot stopped");
    assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD_IA");
    assert_eq!(response.bytes().await.expect("read IA body"), expected);
    api::sdk_assert_get(
        &endpoints.gateway_a,
        &bucket,
        "tier/ia/object.bin",
        &topology_body("cold"),
    )
    .await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn cold_stopped_fixture_fails_closed() {
    let endpoints = RealEndpoints::required();
    let db = connect_pg17(&endpoints.postgres_url).await;
    assert_gateway_healthy(&endpoints.gateway_a, "gateway A").await;
    assert_gateway_healthy(&endpoints.gateway_b, "gateway B").await;
    let _hot_id = kubo_id(&endpoints.hot_kubo).await;
    assert_backend_unavailable(&endpoints.cold_kubo, "cold Kubo").await;
    let bucket = topology_bucket();
    let expected = topology_body("cold");
    let response = api::get_object(
        &endpoints.gateway_a,
        &bucket,
        "tier/ia/object.bin",
        Some("null"),
        HeaderMap::new(),
    )
    .await;
    let status = response.status();
    let body = response.text().await.expect("read cold-down S3 error");
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert!(body.contains("<Code>InternalError</Code>"), "{body}");
    assert!(
        !body.contains(std::str::from_utf8(&expected).expect("fixture body is UTF-8")),
        "cold-down error leaked or hot-fell-back to IA bytes: {body}"
    );
    let hot = api::get_object(
        &endpoints.gateway_b,
        &bucket,
        "tier/hot/object.bin",
        Some("null"),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(
        hot.status(),
        StatusCode::OK,
        "STANDARD GET with cold stopped"
    );
    assert_eq!(hot.headers()["x-amz-storage-class"], "STANDARD");
    assert_eq!(
        hot.bytes().await.expect("read hot control"),
        topology_body("hot")
    );
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

pub async fn cleanup_backend_stop_fixture() {
    let endpoints = RealEndpoints::required();
    let db = endpoints.assert_all_healthy().await;
    let bucket = topology_bucket();
    api::delete_lifecycle(&endpoints.gateway_a, &bucket).await;
    for key in ["tier/ia/object.bin", "tier/hot/object.bin"] {
        api::delete_version(&endpoints.gateway_b, &bucket, key, "null").await;
    }
    api::sdk_delete_bucket(&endpoints.gateway_a, &bucket).await;
    db.close()
        .await
        .expect("close transition PostgreSQL connection");
}

async fn connect_pg17(url: &str) -> DatabaseConnection {
    let mut options = ConnectOptions::new(url.to_owned());
    options.max_connections(2).min_connections(1);
    let db = tokio::time::timeout(Duration::from_secs(15), Database::connect(options))
        .await
        .expect("PostgreSQL connection timed out")
        .expect("connect to runner-owned lifecycle-transition PostgreSQL");
    let row = db
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SHOW server_version_num".to_owned(),
        ))
        .await
        .expect("query PostgreSQL version")
        .expect("PostgreSQL version row");
    let version = row
        .try_get::<String>("", "server_version_num")
        .expect("PostgreSQL server_version_num")
        .parse::<u32>()
        .expect("numeric PostgreSQL server_version_num");
    assert!(
        (170_000..180_000).contains(&version),
        "real transition suite requires PostgreSQL 17, got server_version_num={version}"
    );
    db
}

async fn assert_gateway_healthy(endpoint: &str, label: &str) {
    let response = api::bounded_http(
        label,
        reqwest::Client::builder()
            .timeout(api::HTTP_TIMEOUT)
            .build()
            .expect("build health client")
            .get(format!("{endpoint}/health"))
            .send(),
    )
    .await
    .unwrap_or_else(|error| panic!("{label} health request failed: {error}"));
    assert!(response.status().is_success(), "{label} is not healthy");
}

async fn kubo_id(endpoint: &str) -> String {
    let response = api::bounded_http(
        "Kubo ID",
        reqwest::Client::builder()
            .timeout(api::HTTP_TIMEOUT)
            .build()
            .expect("build Kubo client")
            .post(format!("{endpoint}/api/v0/id?peerid-base=b58mh"))
            .send(),
    )
    .await
    .expect("Kubo ID request");
    assert!(response.status().is_success(), "Kubo ID request failed");
    let value: serde_json::Value = response.json().await.expect("parse Kubo ID JSON");
    value["ID"]
        .as_str()
        .filter(|id| !id.is_empty())
        .expect("Kubo ID response contains ID")
        .to_owned()
}

async fn assert_backend_unavailable(endpoint: &str, label: &str) {
    let result = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("build stopped-backend probe")
        .post(format!("{endpoint}/api/v0/id?peerid-base=b58mh"))
        .send()
        .await;
    assert!(
        result.is_err()
            || !result
                .expect("checked successful response")
                .status()
                .is_success(),
        "{label} must be stopped before this phase"
    );
}

async fn kubo_cat_optional(endpoint: &str, cid: &str) -> Option<Vec<u8>> {
    let mut url = url::Url::parse(&format!("{endpoint}/api/v0/cat")).expect("valid Kubo URL");
    url.query_pairs_mut()
        .append_pair("arg", cid)
        .append_pair("offline", "true");
    let response = api::bounded_http(
        "offline Kubo cat",
        reqwest::Client::builder()
            .timeout(api::HTTP_TIMEOUT)
            .build()
            .expect("build Kubo cat client")
            .post(url)
            .send(),
    )
    .await
    .expect("send offline Kubo cat");
    if !response.status().is_success() {
        return None;
    }
    Some(
        response
            .bytes()
            .await
            .expect("read Kubo cat bytes")
            .to_vec(),
    )
}

async fn kubo_cat(endpoint: &str, cid: &str) -> Vec<u8> {
    kubo_cat_optional(endpoint, cid)
        .await
        .unwrap_or_else(|| panic!("offline Kubo cat failed for CID {cid}"))
}

async fn kubo_add_nondefault(endpoint: &str, body: Vec<u8>, chunk_size: usize) -> String {
    let mut url = url::Url::parse(&format!("{endpoint}/api/v0/add")).expect("valid Kubo add URL");
    url.query_pairs_mut()
        .append_pair("cid-version", "1")
        .append_pair("hash", "sha2-256")
        .append_pair("raw-leaves", "true")
        .append_pair("chunker", &format!("size-{chunk_size}"))
        .append_pair("pin", "true")
        .append_pair("wrap-with-directory", "false")
        .append_pair("progress", "false");
    let form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(body).file_name("nondefault-raw-leaf.bin"),
    );
    let response = api::bounded_http(
        "nondefault Kubo add",
        reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("build nondefault Kubo add client")
            .post(url)
            .multipart(form)
            .send(),
    )
    .await
    .expect("send nondefault Kubo add");
    assert!(response.status().is_success(), "nondefault Kubo add failed");
    let response = response.text().await.expect("read Kubo add response");
    response
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|value| value["Hash"].as_str().map(str::to_owned))
        .next_back()
        .expect("nondefault Kubo add response omitted root CID")
}

async fn assert_nondefault_raw_leaf_layout(
    endpoint: &str,
    root: &str,
    logical_size: usize,
    chunk_size: usize,
) {
    let root_cid = cid::Cid::try_from(root).expect("valid nondefault root CID");
    assert_eq!(root_cid.version(), cid::Version::V1);
    assert_eq!(root_cid.codec(), 0x70, "UnixFS root must be dag-pb");
    let mut url = url::Url::parse(&format!("{endpoint}/api/v0/refs")).expect("valid refs URL");
    url.query_pairs_mut()
        .append_pair("arg", root)
        .append_pair("recursive", "true")
        .append_pair("unique", "true")
        .append_pair("offline", "true");
    let response = api::bounded_http(
        "offline recursive Kubo refs",
        reqwest::Client::builder()
            .timeout(api::HTTP_TIMEOUT)
            .build()
            .expect("build Kubo refs client")
            .post(url)
            .send(),
    )
    .await
    .expect("send Kubo refs request");
    assert!(response.status().is_success(), "offline Kubo refs failed");
    let body = response.text().await.expect("read Kubo refs response");
    let refs = body
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("Kubo refs NDJSON"))
        .map(|value| {
            assert!(
                value["Err"].as_str().unwrap_or_default().is_empty(),
                "Kubo refs reported an error"
            );
            value["Ref"]
                .as_str()
                .filter(|value| !value.is_empty())
                .expect("Kubo refs response omitted Ref")
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        refs.len(),
        logical_size.div_ceil(chunk_size),
        "size-64KiB chunker must produce the expected direct leaves"
    );
    assert!(
        refs.iter().all(|reference| {
            cid::Cid::try_from(reference.as_str())
                .is_ok_and(|cid| cid.version() == cid::Version::V1 && cid.codec() == 0x55)
        }),
        "all nondefault UnixFS child blocks must be CIDv1 raw leaves"
    );
}

async fn assert_local_full_dag(endpoint: &str, cid: &str) {
    let receipt = ipfs_s3_gateway::kubo::KuboClient::new(endpoint.to_owned())
        .verify_local_residency(cid)
        .await
        .expect("Kubo must report a recursive pin and complete local DAG");
    assert_eq!(
        cid::Cid::try_from(receipt.cid.as_str()).expect("receipt CID"),
        cid::Cid::try_from(cid).expect("requested CID"),
        "local DAG receipt must bind the requested CID"
    );
}

async fn upload(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
    headers: HeaderMap,
) -> UploadedObject {
    let response = api::put_object(endpoint, bucket, key, &body, headers).await;
    let status = response.status();
    if status != StatusCode::OK {
        let error = response.text().await.expect("read failed PUT response");
        panic!("PUT {key} failed with {status}: {error}");
    }
    let cid = api::response_etag(&response, "PUT object");
    let version_id = api::response_version(&response, "PUT object");
    assert!(!cid.is_empty(), "PUT object CID/ETag is empty");
    UploadedObject {
        key: key.to_owned(),
        body,
        cid,
        version_id,
    }
}

async fn current_object_from_head(
    endpoint: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
    version_id: &str,
    storage_class: &str,
) -> UploadedObject {
    let response = api::head_object(endpoint, bucket, key, None, HeaderMap::new()).await;
    assert_eq!(response.status(), StatusCode::OK, "HEAD copied object");
    assert_eq!(response.headers()["x-amz-storage-class"], storage_class);
    assert_eq!(response.headers()["x-amz-version-id"], version_id);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        body.len().to_string()
    );
    UploadedObject {
        key: key.to_owned(),
        body: body.to_vec(),
        cid: api::response_etag(&response, "HEAD copied object"),
        version_id: version_id.to_owned(),
    }
}

async fn age_current_prefix(db: &DatabaseConnection, bucket: &str, prefix: &str) -> u64 {
    let result = db
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE object_versions SET lifecycle_age_started_at = clock_timestamp() - INTERVAL '3 days', updated_at = clock_timestamp() WHERE bucket = $1 AND key LIKE $2 AND is_latest = TRUE AND kind = 'object'",
            [bucket.into(), format!("{prefix}%").into()],
        ))
        .await
        .expect("age exact current transition fixture rows");
    result.rows_affected()
}

async fn age_noncurrent(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> u64 {
    let statement = match version_id {
        Some(version) => Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE object_versions SET became_noncurrent_at = clock_timestamp() - INTERVAL '3 days', updated_at = clock_timestamp() WHERE bucket = $1 AND key = $2 AND version_id = $3 AND is_latest = FALSE AND kind = 'object'",
            [bucket.into(), key.into(), version.into()],
        ),
        None => Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE object_versions SET became_noncurrent_at = clock_timestamp() - INTERVAL '3 days', updated_at = clock_timestamp() WHERE bucket = $1 AND key = $2 AND version_id IS NULL AND is_latest = FALSE AND kind = 'object'",
            [bucket.into(), key.into()],
        ),
    };
    let result = db
        .execute(statement)
        .await
        .expect("age exact noncurrent transition fixture row");
    result.rows_affected()
}

async fn wait_for_transition(
    endpoint: &str,
    db: &DatabaseConnection,
    bucket: &str,
    object: &UploadedObject,
    version_id: Option<&str>,
    encryption: Encryption,
    action_kind: &str,
) {
    let started = Instant::now();
    loop {
        let response = api::head_object(
            endpoint,
            bucket,
            &object.key,
            version_id,
            request_headers(encryption),
        )
        .await;
        let last = format!(
            "status={}; class={:?}",
            response.status(),
            response.headers().get("x-amz-storage-class")
        );
        if response.status() == StatusCode::OK
            && response
                .headers()
                .get("x-amz-storage-class")
                .is_some_and(|value| value == "STANDARD_IA")
            && lifecycle_action_succeeded(db, bucket, &object.key, action_kind).await
        {
            assert_eq!(
                api::response_etag(&response, "transitioned HEAD"),
                object.cid
            );
            assert_eq!(
                response.headers()["x-amz-version-id"],
                version_id.unwrap_or("null")
            );
            assert_primary_cold(db, bucket, &object.key, version_id).await;
            return;
        }
        assert!(
            started.elapsed() < TRANSITION_TIMEOUT,
            "transition did not settle for {bucket}/{} ({action_kind}); last={last}",
            object.key
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn lifecycle_action_succeeded(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    action_kind: &str,
) -> bool {
    db.query_one(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "SELECT state FROM lifecycle_actions WHERE bucket = $1 AND object_key = $2 AND action_kind = $3 ORDER BY created_at DESC LIMIT 1",
        [bucket.into(), key.into(), action_kind.into()],
    ))
    .await
    .expect("read transition lifecycle action")
    .and_then(|row| row.try_get::<String>("", "state").ok())
    .is_some_and(|state| state == "succeeded")
}

async fn assert_primary_cold(
    db: &DatabaseConnection,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) {
    let statement = match version_id {
        Some("null") | None => Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT vr.primary_tier, vr.storage_class FROM object_versions ov JOIN version_residencies vr ON vr.version_row_id = ov.id WHERE ov.bucket = $1 AND ov.key = $2 AND ov.version_id IS NULL",
            [bucket.into(), key.into()],
        ),
        Some(version) => Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT vr.primary_tier, vr.storage_class FROM object_versions ov JOIN version_residencies vr ON vr.version_row_id = ov.id WHERE ov.bucket = $1 AND ov.key = $2 AND ov.version_id = $3",
            [bucket.into(), key.into(), version.into()],
        ),
    };
    let row = db
        .query_one(statement)
        .await
        .expect("query transitioned residency")
        .expect("transitioned residency row");
    assert_eq!(row.try_get::<String>("", "primary_tier").unwrap(), "cold");
    assert_eq!(
        row.try_get::<String>("", "storage_class").unwrap(),
        "STANDARD_IA"
    );
}

async fn assert_full_get(
    endpoint: &str,
    bucket: &str,
    object: &UploadedObject,
    version_id: Option<&str>,
    encryption: Encryption,
    metadata: &str,
) {
    let response = api::get_object(
        endpoint,
        bucket,
        &object.key,
        version_id,
        request_headers(encryption),
    )
    .await;
    assert_object_headers(
        &response,
        object,
        version_id,
        "STANDARD_IA",
        encryption,
        metadata,
    );
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.expect("read full GET"), object.body);
}

async fn assert_head(
    endpoint: &str,
    bucket: &str,
    object: &UploadedObject,
    version_id: Option<&str>,
    encryption: Encryption,
    metadata: &str,
) {
    let response = api::head_object(
        endpoint,
        bucket,
        &object.key,
        version_id,
        request_headers(encryption),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_object_headers(
        &response,
        object,
        version_id,
        "STANDARD_IA",
        encryption,
        metadata,
    );
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        object.body.len().to_string()
    );
}

async fn assert_range(
    endpoint: &str,
    bucket: &str,
    object: &UploadedObject,
    version_id: Option<&str>,
    encryption: Encryption,
) {
    assert!(object.body.len() >= 10, "range fixture is too short");
    let mut headers = request_headers(encryption);
    headers.insert(header::RANGE, HeaderValue::from_static("bytes=3-9"));
    let response = api::get_object(endpoint, bucket, &object.key, version_id, headers).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD_IA");
    assert_eq!(api::response_etag(&response, "Range GET"), object.cid);
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "7");
    assert_eq!(
        response.headers()[header::CONTENT_RANGE],
        format!("bytes 3-9/{}", object.body.len())
    );
    assert_eq!(
        response.bytes().await.expect("read Range GET"),
        &object.body[3..=9]
    );
}

async fn assert_range_between(
    endpoint: &str,
    bucket: &str,
    object: &UploadedObject,
    version_id: Option<&str>,
    start: usize,
    end: usize,
) {
    assert!(
        start < end && end < object.body.len(),
        "invalid range fixture"
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::RANGE,
        HeaderValue::from_str(&format!("bytes={start}-{end}")).expect("valid Range header"),
    );
    let response = api::get_object(endpoint, bucket, &object.key, version_id, headers).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD_IA");
    assert_eq!(
        api::response_etag(&response, "cross-block Range GET"),
        object.cid
    );
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        (end - start + 1).to_string()
    );
    assert_eq!(
        response.headers()[header::CONTENT_RANGE],
        format!("bytes {start}-{end}/{}", object.body.len())
    );
    assert_eq!(
        response.bytes().await.expect("read cross-block Range GET"),
        &object.body[start..=end]
    );
}

async fn assert_wrong_sse_c_key(endpoint: &str, bucket: &str, object: &UploadedObject) {
    let mut headers = api::sse_c_headers(WRONG_SSE_C_KEY);
    headers.insert(header::RANGE, HeaderValue::from_static("bytes=3-9"));
    let response = api::get_object(endpoint, bucket, &object.key, Some("null"), headers).await;
    let status = response.status();
    let body = response
        .text()
        .await
        .expect("read wrong-key SSE-C response");
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    api::assert_s3_error(status, &body, "AccessDenied");
    assert!(
        !body.contains(std::str::from_utf8(&object.body).expect("SSE-C fixture is UTF-8")),
        "wrong-key response leaked plaintext"
    );
}

fn assert_object_headers(
    response: &reqwest::Response,
    object: &UploadedObject,
    version_id: Option<&str>,
    storage_class: &str,
    encryption: Encryption,
    metadata: &str,
) {
    assert_eq!(response.headers()["x-amz-storage-class"], storage_class);
    assert_eq!(api::response_etag(response, "object response"), object.cid);
    assert_eq!(
        response.headers()["x-amz-version-id"],
        version_id.unwrap_or("null")
    );
    assert_eq!(response.headers()["x-amz-meta-transition-suite"], metadata);
    match encryption {
        Encryption::Plain => {
            assert!(
                response
                    .headers()
                    .get("x-amz-server-side-encryption")
                    .is_none()
            );
            assert!(
                response
                    .headers()
                    .get("x-amz-server-side-encryption-customer-algorithm")
                    .is_none()
            );
        }
        Encryption::SseS3 => {
            assert_eq!(response.headers()["x-amz-server-side-encryption"], "AES256")
        }
        Encryption::SseC(key) => {
            let expected = api::sse_c_headers(key);
            assert_eq!(
                response.headers()["x-amz-server-side-encryption-customer-algorithm"],
                "AES256"
            );
            assert_eq!(
                response.headers()["x-amz-server-side-encryption-customer-key-md5"],
                expected["x-amz-server-side-encryption-customer-key-md5"]
            );
            assert!(
                response
                    .headers()
                    .get("x-amz-server-side-encryption")
                    .is_none()
            );
        }
    }
}

async fn assert_standard_current(endpoint: &str, bucket: &str, object: &UploadedObject) {
    let response = api::get_object(endpoint, bucket, &object.key, None, HeaderMap::new()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-storage-class"], "STANDARD");
    assert_eq!(
        api::response_etag(&response, "STANDARD current GET"),
        object.cid
    );
    assert_eq!(response.headers()["x-amz-version-id"], object.version_id);
    assert_eq!(
        response.bytes().await.expect("read STANDARD current"),
        object.body
    );
}

fn request_headers(encryption: Encryption) -> HeaderMap {
    match encryption {
        Encryption::SseC(key) => api::sse_c_headers(key),
        Encryption::Plain | Encryption::SseS3 => HeaderMap::new(),
    }
}

fn assert_list_object_class(xml: &str, key: &str, class: &str) {
    let block = xml_element_for(xml, "Contents", "Key", key)
        .unwrap_or_else(|| panic!("ListObjects omitted key {key}: {xml}"));
    assert!(
        block.contains(&format!("<StorageClass>{class}</StorageClass>")),
        "ListObjects class mismatch for {key}: {block}"
    );
}

fn assert_version_class(xml: &str, key: &str, version_id: &str, class: &str) {
    let block = xml_elements(xml, "Version")
        .into_iter()
        .find(|block| {
            block.contains(&format!("<Key>{key}</Key>"))
                && block.contains(&format!("<VersionId>{version_id}</VersionId>"))
        })
        .unwrap_or_else(|| {
            panic!("ListObjectVersions omitted {key}?versionId={version_id}: {xml}")
        });
    assert!(
        block.contains(&format!("<StorageClass>{class}</StorageClass>")),
        "ListObjectVersions class mismatch for {key}?versionId={version_id}: {block}"
    );
}

fn xml_element_for<'a>(xml: &'a str, element: &str, field: &str, value: &str) -> Option<&'a str> {
    xml_elements(xml, element)
        .into_iter()
        .find(|block| block.contains(&format!("<{field}>{value}</{field}>")))
}

fn xml_elements<'a>(xml: &'a str, element: &str) -> Vec<&'a str> {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut remaining = xml;
    let mut blocks = Vec::new();
    while let Some(start) = remaining.find(&open) {
        let after_start = &remaining[start..];
        let Some(end) = after_start.find(&close) else {
            break;
        };
        let end = end + close.len();
        blocks.push(&after_start[..end]);
        remaining = &after_start[end..];
    }
    blocks
}

fn current_lifecycle_xml(rule_id: &str, prefix: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter><Prefix>{prefix}</Prefix></Filter><Transition><Days>1</Days><StorageClass>STANDARD_IA</StorageClass></Transition></Rule></LifecycleConfiguration>"
    )
}

fn noncurrent_lifecycle_xml(rule_id: &str, prefix: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter><Prefix>{prefix}</Prefix></Filter><NoncurrentVersionTransition><NoncurrentDays>1</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>"
    )
}

fn unique_bucket(scenario: &str) -> String {
    format!("lt-{scenario}-{}", uuid::Uuid::new_v4().simple())
}

fn unique_body(bucket: &str, label: &str) -> Vec<u8> {
    format!("real-transition::{bucket}::{label}::0123456789").into_bytes()
}

fn deterministic_high_entropy_body(bucket: &str, label: &str, size: usize) -> Vec<u8> {
    const CHUNK_SIZE: usize = 256 * 1024;
    const CHUNK_MAGIC: [u8; 8] = *b"IPFS3ENT";

    use sha2::{Digest as _, Sha256};

    let fixture_seed = Sha256::digest(format!("{bucket}::{label}"));
    let mut body = Vec::with_capacity(size);
    for chunk_index in 0..size.div_ceil(CHUNK_SIZE) {
        let chunk_len = (size - body.len()).min(CHUNK_SIZE);
        let mut chunk_seed = Sha256::new();
        chunk_seed.update(fixture_seed);
        chunk_seed.update((chunk_index as u64).to_le_bytes());
        let digest = chunk_seed.finalize();
        let mut state = u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 seed width"));
        let start = body.len();
        while body.len() - start < chunk_len {
            let word = splitmix64(&mut state);
            let remaining = chunk_len - (body.len() - start);
            body.extend_from_slice(&word.to_le_bytes()[..remaining.min(8)]);
        }

        // This prefix makes chunk uniqueness structural rather than probabilistic.
        let prefix_len = chunk_len.min(16);
        let mut prefix = [0_u8; 16];
        prefix[..8].copy_from_slice(&CHUNK_MAGIC);
        prefix[8..].copy_from_slice(&(chunk_index as u64).to_le_bytes());
        body[start..start + prefix_len].copy_from_slice(&prefix[..prefix_len]);
    }
    body
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn evidence(scenario: &str, assertions: &str) {
    eprintln!("[LIFECYCLE-TRANSITION-EVIDENCE] scenario={scenario} result=pass {assertions}");
}

fn topology_fixture_id() -> String {
    let id = std::env::var("IPFS_S3_TRANSITION_FIXTURE_ID").unwrap_or_else(|_| {
        panic!("NOT RUN: IPFS_S3_TRANSITION_FIXTURE_ID is required for backend-stop phases")
    });
    assert!(
        (1..=32).contains(&id.len())
            && id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
        "IPFS_S3_TRANSITION_FIXTURE_ID must be 1-32 lowercase letters, digits, or hyphens"
    );
    id
}

fn topology_bucket() -> String {
    format!("lt-stop-{}", topology_fixture_id())
}

fn topology_body(tier: &str) -> Vec<u8> {
    format!("real-transition-stop::{}::{tier}", topology_fixture_id()).into_bytes()
}

#[cfg(test)]
mod fixture_tests {
    use std::collections::HashSet;

    use sha2::{Digest as _, Sha256};

    use super::deterministic_high_entropy_body;

    #[test]
    fn stress_fixture_uses_nonduplicate_256_kib_chunks() {
        const CHUNK_SIZE: usize = 256 * 1024;
        const CHUNK_COUNT: usize = 252;
        // This reproduces the 64-byte seed used by the real stress fixture. The old
        // periodic generator repeated a UnixFS-sized chunk after 251 chunks.
        let bucket = "lt-empty-multiblock-00000000000000000000000000000000";
        let body = deterministic_high_entropy_body(bucket, "multiblock", CHUNK_SIZE * CHUNK_COUNT);
        let unique: HashSet<[u8; 32]> = body
            .chunks(CHUNK_SIZE)
            .map(|chunk| Sha256::digest(chunk).into())
            .collect();

        assert_eq!(
            unique.len(),
            CHUNK_COUNT,
            "every deterministic stress chunk must be distinct so Kubo cannot deduplicate the fixture"
        );
    }
}
