use super::common::{
    FIRST_ENTRY_BYTES, PIN_CID_ENTRY1, PIN_CID_ENTRY2, PIN_CID_PART, PIN_CID_ROOT,
    SECOND_ENTRY_BYTES, SINGLE_ENTRY_BYTES, assert_no_kubo_pin_removes, assert_put_cid_headers,
    assert_s3_error, assert_signed_body, latest_pinning_object, lease_targets, owner_leases,
    signed_complete_multipart, signed_copy, signed_copy_with_tagging,
    signed_create_multipart_upload_with_tagging, signed_create_multipart_zip_upload_with_tagging,
    signed_decompress_zip_put, signed_delete_object_tagging, signed_delete_objects, signed_get,
    signed_get_object_tagging, signed_put_object_tagging, signed_put_with_tagging,
    signed_upload_part, tagging_pairs, xml_element_values, xml_text,
};
use crate::support;
use crate::support::decompress::{
    AddReply, KuboScript, S3TestEndpoint, archive_key_collision_zip, legal_single_entry_zip,
    legal_two_entry_zip,
};
use crate::support::pinning::{
    PinningHarness, PinningHarnessConfig, PsaReply, TEST_CID, TestProviderConfig,
    start_pinning_harness,
};
use crate::support::sigv4::send_sigv4;
use chrono::{Duration as ChronoDuration, Utc};
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::config::PolicyConfig;
use ipfs_s3_gateway::store;
use sea_orm::{ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait, Statement};
use std::collections::{BTreeSet, HashMap};

async fn assert_tagging(harness: &impl S3TestEndpoint, key: &str, expected: &[(&str, &str)]) {
    let response = signed_get_object_tagging(harness, key).await;
    assert_eq!(response.status(), StatusCode::OK, "GetObjectTagging {key}");
    let body = response.text().await.expect("GetObjectTagging XML");
    let expected = expected
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(
        tagging_pairs(&body),
        expected,
        "GetObjectTagging XML: {body}"
    );
}

async fn remote_pin(
    harness: &PinningHarness,
    provider: &str,
    cid: &str,
) -> store::entities::remote_pin::Model {
    store::entities::remote_pin::Entity::find_by_id((
        harness.provider_key(provider),
        cid.to_owned(),
    ))
    .one(harness.state.store.db())
    .await
    .expect("load remote pin")
    .expect("remote pin exists")
}

async fn assert_unknown_psa_resource(
    harness: &PinningHarness,
    provider: &str,
    cid: &str,
    effect: &str,
) {
    let key = harness.provider_key(provider);
    let ledger = store::pinning::ledger::get(harness.state.store.db(), &key, cid)
        .await
        .expect("load PSA ledger")
        .expect("PSA ledger exists");
    assert_eq!(
        ledger.ownership, "unknown",
        "PSA response is not creation proof"
    );
    assert_eq!(
        ledger.effect,
        effect,
        "pinning jobs: {:?}; targets: {:?}",
        harness.pin_jobs().await,
        store::entities::pin_lease_target::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("load targets after effect mismatch"),
    );
    assert_eq!(
        store::pinning::ledger::decode_route(&ledger)
            .expect("registered PSA route")
            .cleanup,
        ipfs_s3_gateway::pinning::identity::CleanupMode::Managed,
        "managed route alone cannot prove exclusive creation"
    );
    assert!(
        !store::pinning::ledger::cleanup_allowed(harness.state.store.db(), &key, cid)
            .await
            .expect("cleanup eligibility"),
        "managed route alone cannot authorize DELETE"
    );
}

async fn assert_no_psa_delete(harness: &PinningHarness) {
    let requests = harness.provider_requests().await;
    assert!(
        requests
            .iter()
            .all(|request| request.method != http::Method::DELETE),
        "unknown PSA resource must not be deleted: {:?}",
        requests
            .iter()
            .map(|r| (&r.method, &r.path))
            .collect::<Vec<_>>()
    );
}

fn assert_submit_request(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
) {
    assert_eq!(request.method, http::Method::POST);
    assert_eq!(request.path, expected_path);
    assert!(request.has_valid_authorization(), "PSA authorization");
    let body: serde_json::Value = serde_json::from_slice(&request.body).expect("PSA submit JSON");
    assert_eq!(
        body.get("cid").and_then(serde_json::Value::as_str),
        Some(expected_cid)
    );
}

fn assert_submit_request_for_job(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
    job: &store::entities::pin_job::Model,
) {
    assert_submit_request(request, expected_path, expected_cid);
    let body: serde_json::Value = serde_json::from_slice(&request.body).expect("PSA submit JSON");
    let correlation = body
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("opaque submit correlation");
    uuid::Uuid::parse_str(correlation).expect("new submit correlation must be a UUID");
    assert_ne!(correlation, job.id);
    assert_eq!(
        body["meta"],
        serde_json::json!({ "gateway_job_id": correlation })
    );
    assert_eq!(body["name"], correlation);
}

async fn captured_submit_correlation(harness: &PinningHarness, job_id: &str) -> String {
    store::pinning::jobs::submission_history(harness.state.store.db(), job_id)
        .await
        .expect("submit history")
        .expect("submit invocation history")
        .correlation
        .expect("new invocation captured its correlation")
}

async fn submitted_job_id_for_correlation(harness: &PinningHarness, correlation: &str) -> String {
    uuid::Uuid::parse_str(correlation).expect("opaque submit correlation");
    let mut matching = Vec::new();
    for job in harness.pin_jobs().await {
        if job.operation == "submit"
            && store::pinning::jobs::submission_history(harness.state.store.db(), &job.id)
                .await
                .expect("submit history")
                .is_some_and(|history| history.correlation.as_deref() == Some(correlation))
        {
            matching.push(job.id);
        }
    }
    assert_eq!(
        matching.len(),
        1,
        "one captured Submit owns the correlation"
    );
    matching.remove(0)
}

fn assert_find_request_for_job(
    request: &support::pinning::ObservedPsaRequest,
    expected_path: &str,
    expected_cid: &str,
    job_id: &str,
) {
    assert_eq!(request.method, http::Method::GET);
    assert_eq!(request.path, expected_path);
    assert!(request.has_valid_authorization(), "PSA Find authorization");
    let query = request.query.as_deref().expect("PSA Find query");
    let url = reqwest::Url::parse(&format!("http://localhost{expected_path}?{query}"))
        .expect("PSA Find URL");
    assert_eq!(
        url.query_pairs().into_owned().collect::<Vec<_>>(),
        vec![
            ("cid".to_owned(), expected_cid.to_owned()),
            (
                "meta".to_owned(),
                serde_json::json!({ "gateway_job_id": job_id }).to_string(),
            ),
        ]
    );
}

async fn only_submit_job(harness: &PinningHarness) -> store::entities::pin_job::Model {
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 1, "one Submit must be published");
    assert_eq!(jobs[0].operation, "submit");
    jobs.into_iter().next().expect("published Submit")
}

fn pinning_policy(prefix: &str, provider_mode: &str, providers: &[&str]) -> PolicyConfig {
    PolicyConfig {
        bucket: "test-bkt".to_owned(),
        prefix: prefix.to_owned(),
        trigger: "request".to_owned(),
        provider_mode: provider_mode.to_owned(),
        providers: providers
            .iter()
            .map(|provider| (*provider).to_owned())
            .collect(),
        default_duration: "1h".to_owned(),
        max_duration: "30d".to_owned(),
        allow_decompressed: true,
    }
}

const PIN_CID_SHARED: &str = "bafkreicnbjdsp2d4w66acomo6g5hnwgt377rel5ouiyda2b2txtwu6gnam";

const PIN_CID_ARCHIVE: &str = "bafkreig7dqxut5wscm35mzikko4nwwnmussy3ncbdruvojlan4wey7qzty";

const PIN_CID_ENTRY: &str = "bafkreielg5kiqkw2y3kio7pw46jc4e2776dlpz4bqmfrks4bszvmrnvkri";

const PIN_CID_COLLISION: &str = "bafkreig6fnztpuezb6nvtcj2bvg2ioumbcgg62mlaoi55iqp2qnlnzes6e";

const PIN_CID_OLD: &str = "bafkreihs5c77ib3qjathmwp3fr6qtvq5lol3xbqti62ryf2wx3uz4oquvu";

const PIN_CID_NEW: &str = "bafkreibpf3q4pzl5lmh4huzh3szsdtx62ehcycj3n7yjtrjn63dblu2cby";

const PIN_CID_NEWER: &str = "bafkreiatkkpbrfldoyhcpm4swyqno2kgksinznqh5y32szq2x2sgme35y4";

const PIN_CID_DELETE: &str = "bafkreicm2xcpr6kccw5xmuqp27hczaoodfjvs75ixeuitaylp7zqhddl74";

const PIN_CID_BATCH_A: &str = "bafkreihtbxvbwt2ormagckqbtglq5mx43ezmudkeauo7rwcn6moaq6u2ji";

const PIN_CID_BATCH_B: &str = "bafkreiaybki67ff7sfeg6kach5drglluljtqw2lwiq2peq23klq3gbmeji";

const PIN_CID_INCOMING: &str = "bafkreidryatkq2h2wiikzwe4yqufa7w5jid7xlkhu2m3qlarwbbak67dui";

const PIN_CID_WAIT: &str = "bafkreiaywgrg2wwheiwmrv7xa7ouj6km5l644ydxzzjgzgl4x2jyqn6hka";

fn repeated_shared_kubo(add_calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        PIN_CID_SHARED,
        add_calls,
        HashMap::from([(PIN_CID_SHARED.to_owned(), b"shared".to_vec())]),
    )
}

fn two_provider_request_config(
    policies: Vec<PolicyConfig>,
    add_calls: usize,
) -> PinningHarnessConfig {
    let mut config = PinningHarnessConfig::request_one();
    config.providers = vec![
        TestProviderConfig::pinata("pinata-primary", 10),
        TestProviderConfig::filebase("filebase-primary", 20),
    ];
    config.policies = policies;
    config.kubo_script = repeated_shared_kubo(add_calls);
    config.pinata_script.clear();
    config.filebase_script.clear();
    config
}

#[tokio::test]
async fn pinning_harness_runs_signed_s3_and_async_psa_over_real_tcp() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::request_one()).await;
    let put =
        signed_put_with_tagging(&harness, "key", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    assert!(
        harness.pinata_requests().await.is_empty(),
        "provider is not on the S3 response path"
    );

    harness.run_worker_until_idle().await;
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/psa/pins"]
    );
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_automatic_put_is_async_and_eventually_pinned() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::automatic_all()).await;

    let response = signed_put_with_tagging(&harness, "happy.txt", b"happy".to_vec(), "").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_put_cid_headers(&response, TEST_CID);
    assert!(
        harness.provider_requests().await.is_empty(),
        "provider traffic must not be on the signed S3 response path"
    );

    let object = latest_pinning_object(&harness, "happy.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 1);
    let automatic = &leases[0];
    assert_eq!(automatic.owner_object_id, object.id);
    assert_eq!(automatic.source, "automatic");
    assert_eq!(automatic.state, "active");
    assert_eq!(automatic.generation, 1);
    let targets = lease_targets(&harness, &automatic.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            ("filebase-primary", TEST_CID, "waiting"),
            ("pinata-primary", TEST_CID, "waiting"),
        ]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending"), ("submit", "pending")]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| {
                (
                    usage.provider.as_str(),
                    usage.reserved_bytes,
                    usage.reserved_pins,
                )
            })
            .collect::<Vec<_>>(),
        vec![("filebase-primary", 5, 1), ("pinata-primary", 5, 1)]
    );

    harness.run_worker_until_idle().await;
    assert_eq!(
        harness.target_states("happy.txt").await,
        vec![
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ]
    );
    for (provider, request_id, path) in [
        ("filebase-primary", "filebase-request-1", "/v1/ipfs/pins"),
        ("pinata-primary", "pinata-request-1", "/psa/pins"),
    ] {
        let remote = remote_pin(&harness, provider, TEST_CID).await;
        assert_eq!(remote.status, "pinned");
        assert_eq!(remote.request_id.as_deref(), Some(request_id));
        assert_eq!(remote.epoch, 1);
        let request = harness
            .provider_requests()
            .await
            .into_iter()
            .find(|request| request.path == path)
            .expect("expected PSA submit request");
        assert_submit_request(&request, path, TEST_CID);
    }
    assert_signed_body(&harness, "happy.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_manual_header_put_and_standard_tag_round_trip() {
    let mut harness = start_pinning_harness(PinningHarnessConfig::request_one()).await;

    let response = signed_put_with_tagging(
        &harness,
        "manual.txt",
        b"body".to_vec(),
        "team=storage&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_put_cid_headers(&response, TEST_CID);
    assert_tagging(
        &harness,
        "manual.txt",
        &[
            ("ipfs-s3:duration", "1h"),
            ("ipfs-s3:pin", "true"),
            ("team", "storage"),
        ],
    )
    .await;

    let object = latest_pinning_object(&harness, "manual.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(manual.owner_object_id, object.id);
    assert_eq!(manual.source, "manual");
    assert_eq!(manual.state, "active");
    assert_eq!(manual.generation, 1);
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(targets.len(), 1);
    assert_eq!(
        (
            targets[0].provider.as_str(),
            targets[0].cid.as_str(),
            targets[0].state.as_str(),
        ),
        ("pinata-primary", TEST_CID, "waiting")
    );
    let remote_before = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            remote_before.status.as_str(),
            remote_before.request_id.as_deref(),
            remote_before.epoch,
        ),
        ("reserved", None, 1)
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| {
                (
                    job.operation.as_str(),
                    job.state.as_str(),
                    job.lease_id.as_deref(),
                    job.target_id.as_deref(),
                    job.expected_generation,
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            "submit",
            "pending",
            Some(manual.id.as_str()),
            Some(targets[0].id.as_str()),
            Some(1),
        )]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 4, 1)]
    );
    assert!(harness.provider_requests().await.is_empty());

    harness.run_worker_until_idle().await;
    let remote_after = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            remote_after.status.as_str(),
            remote_after.request_id.as_deref(),
            remote_after.epoch,
        ),
        ("pinned", Some("pinata-request-1"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", TEST_CID);
    assert_signed_body(&harness, "manual.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_copy_copies_or_replaces_tags_and_reuses_cid_usage() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok(PIN_CID_SHARED)],
        cat_bodies: HashMap::from([(PIN_CID_SHARED.to_owned(), b"shared".to_vec())]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-shared-request",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "source.txt",
        b"shared".to_vec(),
        "team=source&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, PIN_CID_SHARED);
    harness.run_worker_until_idle().await;

    let source = latest_pinning_object(&harness, "source.txt").await;
    let source_lease = owner_leases(&harness, &source.id).await.remove(0);
    assert_eq!(
        (
            source_lease.owner_object_id.as_str(),
            source_lease.source.as_str(),
            source_lease.state.as_str(),
            source_lease.generation,
        ),
        (source.id.as_str(), "manual", "active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &source_lease.id).await[0].state,
        "pinned"
    );
    let jobs_before_copy = harness
        .pin_jobs()
        .await
        .into_iter()
        .map(|job| (job.id, job.operation, job.provider, job.cid, job.state))
        .collect::<Vec<_>>();

    let copied = signed_copy(&harness, "source.txt", "copied.txt", HeaderMap::new()).await;
    assert_eq!(copied.status(), StatusCode::OK);
    let copied_xml = copied.text().await.expect("CopyObject XML");
    assert_eq!(
        xml_text(&copied_xml, "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    assert!(
        xml_text(&copied_xml, "LastModified").is_some(),
        "CopyObject XML: {copied_xml}"
    );
    assert_tagging(
        &harness,
        "copied.txt",
        &[("ipfs-s3:pin", "true"), ("team", "source")],
    )
    .await;
    assert_signed_body(&harness, "copied.txt", b"shared").await;

    let replaced = signed_copy_with_tagging(
        &harness,
        "source.txt",
        "replaced.txt",
        "team=replaced&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(replaced.status(), StatusCode::OK);
    let replaced_xml = replaced.text().await.expect("CopyObject replacement XML");
    assert_eq!(
        xml_text(&replaced_xml, "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    assert_tagging(
        &harness,
        "replaced.txt",
        &[("ipfs-s3:pin", "true"), ("team", "replaced")],
    )
    .await;
    assert_signed_body(&harness, "replaced.txt", b"shared").await;

    for key in ["copied.txt", "replaced.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        assert_ne!(object.id, source.id, "copy must have a new owner object");
        assert_eq!(object.cid, PIN_CID_SHARED);
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        assert_eq!(
            (
                lease.owner_object_id.as_str(),
                lease.source.as_str(),
                lease.state.as_str(),
                lease.generation,
            ),
            (object.id.as_str(), "manual", "active", 1)
        );
        assert_eq!(
            lease_targets(&harness, &lease.id)
                .await
                .iter()
                .map(|target| {
                    (
                        target.provider.as_str(),
                        target.cid.as_str(),
                        target.state.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![("pinata-primary", PIN_CID_SHARED, "pinned")]
        );
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 6, 1)],
        "one CID must consume one provider reservation"
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .into_iter()
            .map(|job| (job.id, job.operation, job.provider, job.cid, job.state))
            .collect::<Vec<_>>(),
        jobs_before_copy,
        "pinned CID reuse must not enqueue additional work"
    );
    let remote = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-shared-request"), 3)
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1, "copies must not submit another PSA pin");
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_SHARED);
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_requires_explicit_remote_identity_before_publication() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script.clear();
    let harness = start_pinning_harness(config).await;
    let create = signed_create_multipart_upload_with_tagging(
        &harness,
        "legacy-identity.bin",
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(create.status(), StatusCode::BAD_REQUEST);
    let body = create.text().await.expect("S3 error XML");
    assert_eq!(xml_element_values(&body, "Code"), vec!["InvalidArgument"]);
    assert!(body.contains("explicit provider identity and revisions"));
    assert_eq!(
        store::entities::multipart_upload::Entity::find()
            .count(harness.state.store.db())
            .await
            .expect("count multipart uploads"),
        0
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());
    assert!(harness.provider_requests().await.is_empty());
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_create_tags_apply_only_to_completed_root() {
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok(PIN_CID_PART), AddReply::Ok(PIN_CID_ROOT)],
        cat_bodies: HashMap::from([
            (PIN_CID_PART.to_owned(), b"part-data".to_vec()),
            (PIN_CID_ROOT.to_owned(), b"part-data".to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-root-request",
        PIN_CID_ROOT,
    )];
    let mut harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_upload_with_tagging(
        &harness,
        "root.bin",
        "team=multipart&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    assert!(
        xml_text(&create_xml, "Bucket").is_some(),
        "CreateMultipartUpload XML: {create_xml}"
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());

    let part = signed_upload_part(&harness, "root.bin", &upload_id, 1, b"part-data".to_vec()).await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    assert_eq!(part_etag, PIN_CID_PART);
    assert!(
        harness.pin_leases().await.is_empty(),
        "parts have no remote leases"
    );
    assert!(
        harness.pin_targets().await.is_empty(),
        "parts have no remote targets"
    );
    assert!(
        harness.remote_pins().await.is_empty(),
        "parts reserve no PSA CID"
    );
    assert!(
        harness.pin_jobs().await.is_empty(),
        "parts enqueue no remote work"
    );

    let complete = signed_complete_multipart(&harness, "root.bin", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_xml = complete.text().await.expect("CompleteMultipartUpload XML");
    assert_eq!(
        xml_text(&complete_xml, "ETag").as_deref(),
        Some(format!("\"{PIN_CID_ROOT}\"").as_str())
    );
    assert_eq!(xml_text(&complete_xml, "Key").as_deref(), Some("root.bin"));
    assert_tagging(
        &harness,
        "root.bin",
        &[("ipfs-s3:pin", "true"), ("team", "multipart")],
    )
    .await;
    let root = latest_pinning_object(&harness, "root.bin").await;
    assert_eq!(root.cid, PIN_CID_ROOT);
    assert!(root.multipart);
    let leases = owner_leases(&harness, &root.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (root.id.as_str(), "manual", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            harness.provider_key("pinata-primary").as_str(),
            PIN_CID_ROOT,
            "waiting"
        )]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str(), job.cid.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending", PIN_CID_ROOT)]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![(harness.provider_key("pinata-primary").as_str(), 9, 1)]
    );

    harness.run_worker_until_idle().await;
    let remote = remote_pin(&harness, "pinata-primary", PIN_CID_ROOT).await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-root-request"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_ROOT);
    assert_signed_body(&harness, "root.bin", b"part-data").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_put_tagging_renews_idempotently_and_delete_tagging_cancels_manual_only() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-renew-request",
        TEST_CID,
    )];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "filebase-renew-request",
        TEST_CID,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "renew.txt",
        b"happy".to_vec(),
        "team=initial&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, TEST_CID);
    harness.run_worker_until_idle().await;

    let object = latest_pinning_object(&harness, "renew.txt").await;
    let leases = owner_leases(&harness, &object.id).await;
    assert_eq!(leases.len(), 2);
    let automatic = leases
        .iter()
        .find(|lease| lease.source == "automatic")
        .expect("automatic lease")
        .clone();
    let manual_before = leases
        .iter()
        .find(|lease| lease.source == "manual")
        .expect("manual lease")
        .clone();
    assert_eq!(automatic.owner_object_id, object.id);
    assert_eq!(manual_before.owner_object_id, object.id);
    assert_eq!(
        (
            manual_before.state.as_str(),
            manual_before.generation,
            automatic.state.as_str(),
            automatic.generation,
        ),
        ("active", 1, "active", 1)
    );
    let manual_target_ids = lease_targets(&harness, &manual_before.id)
        .await
        .into_iter()
        .map(|target| target.id)
        .collect::<Vec<_>>();
    let mut remote_epochs_before_renewal = std::collections::BTreeMap::new();
    for provider in ["pinata-primary", "filebase-primary"] {
        remote_epochs_before_renewal.insert(
            provider,
            remote_pin(&harness, provider, TEST_CID).await.epoch,
        );
    }
    assert_eq!(
        harness.target_states("renew.txt").await,
        vec![
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("filebase-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ]
    );

    let retain_until = (manual_before.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renewal_tags = [
        ("team", "renewed"),
        ("ipfs-s3:pin", "true"),
        ("ipfs-s3:retain-until", retain_until.as_str()),
    ];
    let renewal = signed_put_object_tagging(&harness, "renew.txt", &renewal_tags).await;
    assert_eq!(renewal.status(), StatusCode::OK);
    assert_tagging(
        &harness,
        "renew.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
            ("team", "renewed"),
        ],
    )
    .await;
    let leases_after_renewal = owner_leases(&harness, &object.id).await;
    let manual_after_renewal = leases_after_renewal
        .iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("renewed manual lease");
    let automatic_after_renewal = leases_after_renewal
        .iter()
        .find(|lease| lease.id == automatic.id)
        .expect("preserved automatic lease");
    assert_eq!(
        (
            manual_after_renewal.owner_object_id.as_str(),
            manual_after_renewal.source.as_str(),
            manual_after_renewal.state.as_str(),
            manual_after_renewal.generation,
            manual_after_renewal.expires_at,
        ),
        (
            object.id.as_str(),
            "manual",
            "active",
            2,
            chrono::DateTime::parse_from_rfc3339(&retain_until)
                .expect("retain-until RFC3339")
                .with_timezone(&Utc),
        )
    );
    assert_eq!(
        (
            automatic_after_renewal.state.as_str(),
            automatic_after_renewal.generation,
        ),
        ("active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &manual_before.id)
            .await
            .into_iter()
            .map(|target| target.id)
            .collect::<Vec<_>>(),
        manual_target_ids,
        "renewal retains original targets"
    );
    let renewal_jobs = harness.pin_jobs().await;
    assert!(renewal_jobs.iter().any(|job| {
        job.operation == "reconcile"
            && job.state == "pending"
            && job.provider == "pinata-primary"
            && job.cid == TEST_CID
    }));
    assert!(renewal_jobs.iter().any(|job| {
        job.operation == "reconcile"
            && job.state == "pending"
            && job.provider == "filebase-primary"
            && job.cid == TEST_CID
    }));
    for (provider, request_id) in [
        ("pinata-primary", "pinata-renew-request"),
        ("filebase-primary", "filebase-renew-request"),
    ] {
        let remote = remote_pin(&harness, provider, TEST_CID).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            (
                "pinned",
                Some(request_id),
                remote_epochs_before_renewal[provider] + 1,
            )
        );
    }

    let jobs_before_idempotent = renewal_jobs
        .iter()
        .map(|job| job.id.clone())
        .collect::<Vec<_>>();
    let idempotent = signed_put_object_tagging(&harness, "renew.txt", &renewal_tags).await;
    assert_eq!(idempotent.status(), StatusCode::OK);
    let manual_after_idempotent = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("idempotently retained manual lease");
    assert_eq!(
        (
            manual_after_idempotent.state.as_str(),
            manual_after_idempotent.generation,
            manual_after_idempotent.expires_at,
        ),
        (
            "active",
            2,
            chrono::DateTime::parse_from_rfc3339(&retain_until)
                .expect("retain-until RFC3339")
                .with_timezone(&Utc),
        )
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| job.id.clone())
            .collect::<Vec<_>>(),
        jobs_before_idempotent,
        "equal retain-until must not enqueue more work"
    );

    let delete = signed_delete_object_tagging(&harness, "renew.txt").await;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    assert_tagging(&harness, "renew.txt", &[]).await;
    let cancelled_manual = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == manual_before.id)
        .expect("cancelled manual lease");
    let retained_automatic = owner_leases(&harness, &object.id)
        .await
        .into_iter()
        .find(|lease| lease.id == automatic.id)
        .expect("retained automatic lease");
    assert_eq!(
        (
            cancelled_manual.owner_object_id.as_str(),
            cancelled_manual.source.as_str(),
            cancelled_manual.state.as_str(),
            cancelled_manual.generation,
        ),
        (object.id.as_str(), "manual", "cancelled", 3)
    );
    assert_eq!(
        (
            retained_automatic.owner_object_id.as_str(),
            retained_automatic.source.as_str(),
            retained_automatic.state.as_str(),
            retained_automatic.generation,
        ),
        (object.id.as_str(), "automatic", "active", 1)
    );
    assert!(
        lease_targets(&harness, &cancelled_manual.id)
            .await
            .iter()
            .all(|target| target.state == "released")
    );
    assert!(
        lease_targets(&harness, &retained_automatic.id)
            .await
            .iter()
            .all(|target| target.state == "pinned")
    );
    assert!(
        harness
            .provider_requests()
            .await
            .iter()
            .all(|request| request.method != http::Method::DELETE),
        "the automatic lease keeps both remote pins desired"
    );
    assert_signed_body(&harness, "renew.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_put_tagging_reactivates_expired_lease_with_unknown_resource_retained() {
    let archive_bytes = legal_single_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok(PIN_CID_ARCHIVE), AddReply::Ok(PIN_CID_ENTRY)],
        cat_bodies: HashMap::from([
            (PIN_CID_ARCHIVE.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY.to_owned(), SINGLE_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-entry-request",
        PIN_CID_ENTRY,
    )];
    let mut harness = start_pinning_harness(config).await;
    let pinata_key = harness.provider_key("pinata-primary");

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes,
        "team=archive&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("decompress ZIP result XML");
    assert_eq!(
        xml_text(&put_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&put_xml, "ArchiveETag").as_deref(),
        Some(PIN_CID_ARCHIVE)
    );

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let leases = owner_leases(&harness, &archive.id).await;
    assert_eq!(leases.len(), 1);
    let manual = &leases[0];
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let original_targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        original_targets
            .iter()
            .map(|target| {
                (
                    target.id.as_str(),
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            original_targets[0].id.as_str(),
            pinata_key.as_str(),
            PIN_CID_ENTRY,
            "waiting",
        )]
    );
    assert_eq!(
        harness
            .pin_jobs()
            .await
            .iter()
            .map(|job| (job.operation.as_str(), job.state.as_str(), job.cid.as_str()))
            .collect::<Vec<_>>(),
        vec![("submit", "pending", PIN_CID_ENTRY)]
    );

    harness.run_worker_until_idle().await;
    let submitted_remote = remote_pin(&harness, "pinata-primary", PIN_CID_ENTRY).await;
    assert_eq!(
        (
            submitted_remote.status.as_str(),
            submitted_remote.request_id.as_deref(),
            submitted_remote.epoch,
        ),
        ("pinned", Some("pinata-entry-request"), 1)
    );
    assert_eq!(lease_targets(&harness, &manual.id).await[0].state, "pinned");
    let submit = harness.provider_requests().await;
    assert_eq!(submit.len(), 1);
    assert_submit_request(&submit[0], "/psa/pins", PIN_CID_ENTRY);

    harness.advance_past_lease_expiry("archive.zip").await;
    harness.restart_worker();
    harness.wait_for_lease_state("archive.zip", "expired").await;
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;
    let expired = owner_leases(&harness, &archive.id).await.remove(0);
    assert_eq!(
        (
            expired.owner_object_id.as_str(),
            expired.source.as_str(),
            expired.content_mode.as_str(),
            expired.state.as_str(),
            expired.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "expired", 2)
    );
    let released_targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        released_targets
            .iter()
            .map(|target| {
                (
                    target.id.as_str(),
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(
            original_targets[0].id.as_str(),
            pinata_key.as_str(),
            PIN_CID_ENTRY,
            "released",
        )]
    );
    let released_remote = remote_pin(&harness, "pinata-primary", PIN_CID_ENTRY).await;
    assert_eq!(
        (
            released_remote.status.as_str(),
            released_remote.request_id.as_deref(),
            released_remote.epoch,
        ),
        ("pinned", Some("pinata-entry-request"), 2)
    );
    let requests_after_release = harness.provider_requests().await;
    assert_eq!(requests_after_release.len(), 1);
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_ENTRY, "retained").await;
    assert_eq!(harness.provider_usage(&pinata_key).await.reserved_pins, 1);
    assert_no_psa_delete(&harness).await;
    let target_count_before_renewal = harness.pin_targets().await.len();
    let kubo_requests_before_renewal = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .len();

    let retain_until = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
    let renewal = signed_put_object_tagging(
        &harness,
        "archive.zip",
        &[
            ("team", "renewed"),
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renewal.status(), StatusCode::OK);
    let reactivated = owner_leases(&harness, &archive.id).await.remove(0);
    assert_eq!(
        (
            reactivated.id.as_str(),
            reactivated.owner_object_id.as_str(),
            reactivated.state.as_str(),
            reactivated.generation,
        ),
        (
            manual.id.as_str(),
            archive.id.as_str(),
            "active",
            expired.generation + 1,
        )
    );
    assert_eq!(
        lease_targets(&harness, &manual.id).await[0].id,
        released_targets[0].id
    );
    assert_eq!(
        harness.pin_targets().await.len(),
        target_count_before_renewal
    );
    assert_eq!(
        harness
            .kubo
            .received_requests()
            .await
            .expect("Kubo request log")
            .len(),
        kubo_requests_before_renewal,
        "reactivation must not reopen or re-extract the archive"
    );
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
            ("team", "renewed"),
        ],
    )
    .await;
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_ENTRY, "retained").await;
    assert_eq!(
        harness.provider_requests().await.len(),
        1,
        "no duplicate Submit"
    );
    assert_signed_body(&harness, "archive.zip", &legal_single_entry_zip()).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_overwrite_and_deletes_retain_unknown_remote_leases() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_OLD),
            AddReply::Ok(PIN_CID_NEW),
            AddReply::Ok(PIN_CID_DELETE),
            AddReply::Ok(PIN_CID_BATCH_A),
            AddReply::Ok(PIN_CID_BATCH_B),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_OLD.to_owned(), b"old".to_vec()),
            (PIN_CID_NEW.to_owned(), b"new".to_vec()),
            (PIN_CID_DELETE.to_owned(), b"delete".to_vec()),
            (PIN_CID_BATCH_A.to_owned(), b"batch-a".to_vec()),
            (PIN_CID_BATCH_B.to_owned(), b"batch-b".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-old-request", PIN_CID_OLD),
        PsaReply::pinned_submit("/psa/pins", "pinata-new-request", PIN_CID_NEW),
        PsaReply::pinned_submit("/psa/pins", "pinata-delete-request", PIN_CID_DELETE),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-a-request", PIN_CID_BATCH_A),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-b-request", PIN_CID_BATCH_B),
    ];
    let mut harness = start_pinning_harness(config).await;

    let old_put = signed_put_with_tagging(
        &harness,
        "overwrite.txt",
        b"old".to_vec(),
        "team=old&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(old_put.status(), StatusCode::OK);
    assert_put_cid_headers(&old_put, PIN_CID_OLD);
    harness.run_worker_until_idle().await;
    let old = latest_pinning_object(&harness, "overwrite.txt").await;
    let old_lease = owner_leases(&harness, &old.id).await.remove(0);
    assert_eq!(
        lease_targets(&harness, &old_lease.id).await[0].state,
        "pinned"
    );
    assert_eq!(
        remote_pin(&harness, "pinata-primary", PIN_CID_OLD)
            .await
            .request_id
            .as_deref(),
        Some("pinata-old-request")
    );

    let overwrite = signed_put_with_tagging(
        &harness,
        "overwrite.txt",
        b"new".to_vec(),
        "team=new&ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(overwrite.status(), StatusCode::OK);
    assert_put_cid_headers(&overwrite, PIN_CID_NEW);
    assert_tagging(
        &harness,
        "overwrite.txt",
        &[("ipfs-s3:pin", "true"), ("team", "new")],
    )
    .await;
    let replacement = latest_pinning_object(&harness, "overwrite.txt").await;
    assert_ne!(replacement.id, old.id);
    assert_eq!(replacement.cid, PIN_CID_NEW);
    let cancelled_old = owner_leases(&harness, &old.id).await.remove(0);
    assert_eq!(
        (
            cancelled_old.owner_object_id.as_str(),
            cancelled_old.source.as_str(),
            cancelled_old.state.as_str(),
            cancelled_old.generation,
        ),
        (old.id.as_str(), "manual", "cancelled", 2)
    );
    assert!(
        lease_targets(&harness, &old_lease.id)
            .await
            .iter()
            .all(|target| target.state == "released")
    );
    assert!(
        harness.pin_jobs().await.iter().any(|job| {
            job.operation == "unpin"
                && job.provider == "pinata-primary"
                && job.cid == PIN_CID_OLD
                && job.expected_remote_epoch == Some(2)
        }),
        "overwrite must enqueue remote-scoped unpin work"
    );
    harness.run_worker_until_idle().await;
    let replacement_lease = owner_leases(&harness, &replacement.id).await.remove(0);
    assert_eq!(
        (
            replacement_lease.owner_object_id.as_str(),
            replacement_lease.source.as_str(),
            replacement_lease.state.as_str(),
            replacement_lease.generation,
        ),
        (replacement.id.as_str(), "manual", "active", 1)
    );
    assert_eq!(
        lease_targets(&harness, &replacement_lease.id)
            .await
            .iter()
            .map(|target| {
                (
                    target.provider.as_str(),
                    target.cid.as_str(),
                    target.state.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        vec![("pinata-primary", PIN_CID_NEW, "pinned")]
    );
    assert_eq!(
        (
            remote_pin(&harness, "pinata-primary", PIN_CID_OLD)
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", PIN_CID_NEW)
                .await
                .request_id
                .as_deref(),
        ),
        ("pinned", Some("pinata-new-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_OLD, "retained").await;
    assert_signed_body(&harness, "overwrite.txt", b"new").await;

    for (key, body, cid) in [
        ("delete.txt", b"delete".as_slice(), PIN_CID_DELETE),
        ("batch-a.txt", b"batch-a".as_slice(), PIN_CID_BATCH_A),
        ("batch-b.txt", b"batch-b".as_slice(), PIN_CID_BATCH_B),
    ] {
        let response = signed_put_with_tagging(
            &harness,
            key,
            body.to_vec(),
            "team=remove&ipfs-s3%3Apin=true",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&response, cid);
        harness.run_worker_until_idle().await;
        let object = latest_pinning_object(&harness, key).await;
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        let target = lease_targets(&harness, &lease.id).await.remove(0);
        assert_eq!(
            (
                lease.owner_object_id.as_str(),
                lease.source.as_str(),
                lease.state.as_str(),
                lease.generation,
                target.provider.as_str(),
                target.cid.as_str(),
                target.state.as_str(),
            ),
            (
                object.id.as_str(),
                "manual",
                "active",
                1,
                "pinata-primary",
                cid,
                "pinned"
            )
        );
    }

    let delete_object_response = send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "delete.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(delete_object_response.status(), StatusCode::NO_CONTENT);
    let batch_delete =
        signed_delete_objects(&harness, &["batch-a.txt", "batch-b.txt"], false).await;
    assert_eq!(batch_delete.status(), StatusCode::OK);
    let batch_delete_xml = batch_delete.text().await.expect("DeleteObjects XML");
    assert_eq!(
        xml_element_values(&batch_delete_xml, "Deleted")
            .into_iter()
            .map(|deleted| xml_text(deleted, "Key").expect("Deleted Key"))
            .collect::<Vec<_>>(),
        vec!["batch-a.txt".to_owned(), "batch-b.txt".to_owned()],
        "DeleteObjects XML: {batch_delete_xml}"
    );
    for (key, cid) in [
        ("delete.txt", PIN_CID_DELETE),
        ("batch-a.txt", PIN_CID_BATCH_A),
        ("batch-b.txt", PIN_CID_BATCH_B),
    ] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .is_err(),
            "deleted key {key} must not remain latest"
        );
        assert!(
            harness.pin_jobs().await.iter().any(|job| {
                job.operation == "unpin"
                    && job.provider == "pinata-primary"
                    && job.cid == cid
                    && job.expected_remote_epoch == Some(2)
            }),
            "{key} must only end its own remote lease"
        );
    }
    harness.run_worker_until_idle().await;
    for cid in [PIN_CID_DELETE, PIN_CID_BATCH_A, PIN_CID_BATCH_B] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            (
                "pinned",
                Some(match cid {
                    PIN_CID_DELETE => "pinata-delete-request",
                    PIN_CID_BATCH_A => "pinata-batch-a-request",
                    _ => "pinata-batch-b-request",
                }),
                2
            ),
            "remote lifecycle for {cid}"
        );
        assert_unknown_psa_resource(&harness, "pinata-primary", cid, "retained").await;
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .iter()
            .map(|usage| (
                usage.provider.as_str(),
                usage.reserved_bytes,
                usage.reserved_pins
            ))
            .collect::<Vec<_>>(),
        vec![("pinata-primary", 26, 5)],
        "all five distinct CID reservations remain until ownership is proven"
    );
    let requests = harness.provider_requests().await;
    let delete_paths = requests
        .iter()
        .filter(|request| request.method == http::Method::DELETE)
        .map(|request| {
            assert!(
                request.has_valid_authorization(),
                "PSA DELETE authorization"
            );
            request.path.clone()
        })
        .collect::<Vec<_>>();
    assert!(
        delete_paths.is_empty(),
        "unknown PSA resources cannot be deleted"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method == http::Method::POST)
            .count(),
        5
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_worker_recovers_expired_claim_and_adopts_ambiguous_submit() {
    let mut config = PinningHarnessConfig::request_one();
    config.policies[0].provider_mode = "all".to_owned();
    let mut failed_find =
        PsaReply::find_for_job("/psa/pins", "accepted-request", TEST_CID, "pinned");
    failed_find.status = StatusCode::INTERNAL_SERVER_ERROR.as_u16();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "accepted-request", TEST_CID),
        failed_find,
        PsaReply::find_for_job("/psa/pins", "accepted-request", TEST_CID, "pinned"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "recover.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", TEST_CID, &submit);

    harness.stop_worker_without_unlocking().await;
    submit_block.release();
    let interrupted = harness.pin_job(&submit.id).await;
    assert_eq!(
        (
            interrupted.state.as_str(),
            interrupted.submit_phase.as_deref(),
            interrupted.attempts,
        ),
        ("running", Some("calling"), 0),
        "accepted response must remain unpersisted at the crash point"
    );
    assert!(
        interrupted.locked_until.is_some(),
        "calling Submit holds a lock"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let failed_find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(
        &failed_find,
        "/psa/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(
        post.sequence < failed_find.sequence,
        "recovery Find follows the accepted POST"
    );
    let retrying = harness.wait_for_job_state(&submit.id, "pending").await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(retrying.submit_phase.as_deref(), Some("recovering"));
    assert_eq!(
        harness.target_states("recover.txt").await,
        vec![("pinata-primary".to_owned(), "degraded".to_owned())],
        "a reclaimed all-mode Submit must surface a failed recovery Find before retry"
    );

    harness.advance_job_due(&submit.id).await;
    harness.restart_worker();
    let adopted_find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 2)
        .await;
    assert_find_request_for_job(
        &adopted_find,
        "/psa/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(failed_find.sequence < adopted_find.sequence);
    let recovered = harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    assert_eq!(recovered.submit_phase.as_deref(), Some("recovering"));
    let remote = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch,
        ),
        ("pinned", Some("accepted-request"), 1)
    );
    assert_eq!(
        harness.target_states("recover.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned()),]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3, "recovery must retry Find, not re-submit");
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "recover.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_reclaimed_submit_with_cancelled_target_retains_unknown_remote() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "cancelled-request", TEST_CID),
        PsaReply::find_for_job("/psa/pins", "cancelled-request", TEST_CID, "pinned"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "cancelled-recover.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", TEST_CID, &submit);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();

    let cancel = signed_delete_object_tagging(&harness, "cancelled-recover.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        harness.target_states("cancelled-recover.txt").await,
        vec![("pinata-primary".to_owned(), "released".to_owned()),]
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)],
        "cancelling an ambiguous Submit retains the reservation"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(
        &find,
        "/psa/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(
        post.sequence < find.sequence,
        "Find observes the accepted POST without proving exclusive creation"
    );
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    let adopted = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (adopted.status.as_str(), adopted.request_id.as_deref()),
        ("pinned", Some("cancelled-request"))
    );

    harness
        .run_current_reconcile("pinata-primary", TEST_CID)
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancellation publishes a fenced Unpin");
    assert_eq!(unpin.state, "done");

    let retained = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("pinned", Some("cancelled-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", TEST_CID, "retained").await;
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        2,
        "cancelled recovery must not re-submit or DELETE"
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "cancelled-recover.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_recovery_find_none_with_no_desired_target_never_posts() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "lost-request", TEST_CID),
        PsaReply::find_none_for_job("/psa/pins", TEST_CID),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "no-match.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", TEST_CID, &submit);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();

    let cancel = signed_delete_object_tagging(&harness, "no-match.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        harness.target_states("no-match.txt").await,
        vec![("pinata-primary".to_owned(), "released".to_owned()),]
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(
        &find,
        "/psa/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(
        post.sequence < find.sequence,
        "reclaimed recovery must Find before it can exit for a stale generation"
    );
    let completed_submit = harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(
        completed_submit.submit_phase.as_deref(),
        Some("ready"),
        "a zero-result recovery with no target is safe to complete"
    );

    harness
        .run_current_reconcile("pinata-primary", TEST_CID)
        .await;
    let remote = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (remote.status.as_str(), remote.request_id.as_deref()),
        ("absent", None)
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 0, 0)],
        "the no-request reservation releases exactly once"
    );
    let reconciles = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile")
        .collect::<Vec<_>>();
    assert_eq!(reconciles.len(), 1, "one stable Reconcile owns the release");
    assert_eq!(reconciles[0].state, "done");
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        2,
        "zero-result recovery must never POST again"
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "no-match.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_running_submit_blocks_quota_release_then_retains_unknown_remote() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "live-lock-request", TEST_CID),
        PsaReply::find_for_job("/psa/pins", "live-lock-request", TEST_CID, "pinned"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "live-lock.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    let submit_block = harness.block_next_submit("pinata-primary").await;
    harness.restart_worker();
    submit_block.wait_until_blocked().await;
    let post = harness
        .wait_for_provider_request("pinata-primary", http::Method::POST, "/psa/pins", 1)
        .await;
    assert_submit_request_for_job(&post, "/psa/pins", TEST_CID, &submit);

    let cancel = signed_delete_object_tagging(&harness, "live-lock.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    harness.stop_worker_without_unlocking().await;
    submit_block.release();
    let live_submit = harness.pin_job(&submit.id).await;
    assert_eq!(
        (
            live_submit.state.as_str(),
            live_submit.submit_phase.as_deref()
        ),
        ("running", Some("calling"))
    );
    let live_lock = live_submit.locked_until.expect("live calling lock");

    let no_request_unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancellation schedules current no-request cleanup");
    harness.restart_worker();
    harness
        .wait_for_job_state(&no_request_unpin.id, "done")
        .await;
    harness.stop_worker_without_unlocking().await;

    harness
        .run_current_reconcile("pinata-primary", TEST_CID)
        .await;
    let retained = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("reserved", None),
        "Reconcile cannot release a live ambiguous Submit"
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)]
    );
    let waiting_reconcile = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "reconcile")
        .expect("Reconcile remains durable while the Submit is live");
    assert_eq!(waiting_reconcile.state, "pending");
    assert!(
        waiting_reconcile.next_attempt_at >= live_lock,
        "Reconcile reschedules at the live Submit lock boundary"
    );

    harness.advance_past_job_lock().await;
    harness.restart_worker();
    let find = harness
        .wait_for_provider_request("pinata-primary", http::Method::GET, "/psa/pins", 1)
        .await;
    assert_find_request_for_job(
        &find,
        "/psa/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(post.sequence < find.sequence);
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    harness
        .run_current_reconcile("pinata-primary", TEST_CID)
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancelled remote retains a fenced Unpin");
    assert_eq!(unpin.state, "done");

    let retained = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("pinned", Some("live-lock-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", TEST_CID, "retained").await;
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 4, 1)]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        2,
        "live-lock recovery must not re-submit or DELETE"
    );
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins"),
        ]
    );
    assert_signed_body(&harness, "live-lock.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_poll_reschedules_one_stable_job_until_pinned() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "poll-request", TEST_CID, "queued"),
        PsaReply::pin_status(
            "/psa/pins/poll-request",
            "poll-request",
            TEST_CID,
            "pinning",
        ),
        PsaReply::pin_status("/psa/pins/poll-request", "poll-request", TEST_CID, "pinned"),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put =
        signed_put_with_tagging(&harness, "poll.txt", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    let submit = only_submit_job(&harness).await;
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll")
        .expect("queued Submit creates one Poll");
    assert_eq!(
        (poll.state.as_str(), poll.attempts),
        ("pending", 0),
        "the first Poll waits for its durable interval"
    );
    assert_submit_request_for_job(
        &harness.provider_requests().await[0],
        "/psa/pins",
        TEST_CID,
        &submit,
    );

    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;
    let after_first_get = harness.pin_job(&poll.id).await;
    assert_eq!(
        (after_first_get.state.as_str(), after_first_get.attempts),
        ("pending", 0),
        "pinning status reuses the same Poll without a retry"
    );
    assert_eq!(after_first_get.id, poll.id);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", TEST_CID)
            .await
            .status,
        "pinning"
    );

    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;
    let finished_poll = harness.pin_job(&poll.id).await;
    assert_eq!(
        (finished_poll.state.as_str(), finished_poll.attempts),
        ("done", 0)
    );
    assert_eq!(finished_poll.id, poll.id);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", TEST_CID)
            .await
            .status,
        "pinned"
    );
    assert_eq!(
        harness.target_states("poll.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned()),]
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/poll-request"),
            (http::Method::GET, "/psa/pins/poll-request"),
        ]
    );
    for request in &requests {
        assert!(
            request.has_valid_authorization(),
            "PSA request authorization"
        );
    }
    assert_signed_body(&harness, "poll.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_remote_projects_automatic_manual_and_copy_targets() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.providers.truncate(1);
    config.policies[0].providers = vec!["pinata-primary".to_owned()];
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-request",
        PIN_CID_SHARED,
    )];
    config.filebase_script.clear();
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "shared.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true&team=shared",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, PIN_CID_SHARED);
    let copied = signed_copy(&harness, "shared.txt", "copy.txt", HeaderMap::new()).await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    assert!(harness.provider_requests().await.is_empty());

    for key in ["shared.txt", "copy.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        let leases = owner_leases(&harness, &object.id).await;
        assert_eq!(leases.len(), 2, "automatic and manual leases for {key}");
        assert!(leases.iter().all(|lease| lease.state == "active"));
        assert_eq!(
            leases
                .iter()
                .map(|lease| lease.source.as_str())
                .collect::<Vec<_>>(),
            vec!["automatic", "manual"]
        );
        for lease in &leases {
            assert!(
                lease_targets(&harness, &lease.id)
                    .await
                    .iter()
                    .all(|target| target.state == "waiting"),
                "all targets start before the shared Submit"
            );
        }
    }
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![("pinata-primary".to_owned(), 6, 1)],
        "one shared CID reserves one provider slot"
    );

    harness.run_worker_until_idle().await;
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1, "all shared targets use one PSA Submit");
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_SHARED);
    for key in ["shared.txt", "copy.txt"] {
        assert_eq!(
            harness.target_states(key).await,
            vec![
                ("pinata-primary".to_owned(), "pinned".to_owned()),
                ("pinata-primary".to_owned(), "pinned".to_owned()),
            ],
            "the shared remote projects pinned to every target for {key}"
        );
        assert_signed_body(&harness, key, b"shared").await;
    }

    let jobs_before_later_copy = harness.pin_jobs().await;
    let later = signed_copy(&harness, "shared.txt", "later-copy.txt", HeaderMap::new()).await;
    assert_eq!(later.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&later.text().await.expect("later CopyObject XML"), "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    assert_eq!(
        harness.target_states("later-copy.txt").await,
        vec![
            ("pinata-primary".to_owned(), "pinned".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ],
        "a later copied target projects directly from the pinned remote"
    );
    assert_eq!(
        harness.pin_jobs().await,
        jobs_before_later_copy,
        "later target creates no job"
    );
    assert_eq!(
        harness.provider_requests().await.len(),
        1,
        "later target does not POST"
    );
    assert_signed_body(&harness, "later-copy.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_terminal_failure_coordinates_each_lease() {
    let mut config = two_provider_request_config(
        vec![
            pinning_policy("one-a/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("one-b/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("all/", "all", &["pinata-primary"]),
        ],
        3,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "shared-failed", PIN_CID_SHARED, "queued"),
        PsaReply::pin_status(
            "/psa/pins/shared-failed",
            "shared-failed",
            PIN_CID_SHARED,
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::submit_status(
        "/v1/ipfs/pins",
        "fallback-queued",
        PIN_CID_SHARED,
        "queued",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["one-a/key.txt", "one-b/key.txt", "all/key.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK, "signed PutObject {key}");
    }
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("one canonical pending Poll for the shared request");
    harness.advance_job_due(&poll.id).await;
    let fallback_block = harness.block_next_submit("filebase-primary").await;
    harness.restart_worker();
    fallback_block.wait_until_blocked().await;
    harness.stop_worker_without_unlocking().await;
    fallback_block.release();

    let failed = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (
            failed.status.as_str(),
            failed.request_id.as_deref(),
            failed.failure_attempts
        ),
        ("failed", Some("shared-failed"), 1)
    );
    assert!(
        failed.next_retry_at.is_some(),
        "the all-mode lease owns one bounded retry"
    );
    let primary_targets = harness
        .pin_targets()
        .await
        .into_iter()
        .filter(|target| target.provider == "pinata-primary")
        .collect::<Vec<_>>();
    assert_eq!(primary_targets.len(), 3);
    assert!(
        primary_targets
            .iter()
            .all(|target| target.state == "degraded")
    );

    for key in ["one-a/key.txt", "one-b/key.txt"] {
        let object = latest_pinning_object(&harness, key).await;
        let lease = owner_leases(&harness, &object.id).await.remove(0);
        assert_eq!(
            lease.generation, 2,
            "terminal shared failure fails over {key} exactly once"
        );
        assert_eq!(
            lease_targets(&harness, &lease.id)
                .await
                .into_iter()
                .filter(|target| target.provider == "filebase-primary")
                .map(|target| target.state)
                .collect::<Vec<_>>(),
            vec!["waiting".to_owned()]
        );
        assert_signed_body(&harness, key, b"shared").await;
    }
    let all_object = latest_pinning_object(&harness, "all/key.txt").await;
    let all_lease = owner_leases(&harness, &all_object.id).await.remove(0);
    assert_eq!(all_lease.generation, 1, "all-mode must not fail over");
    assert_eq!(
        lease_targets(&harness, &all_lease.id).await[0].state,
        "degraded"
    );
    let retries = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile" && job.provider == "pinata-primary")
        .collect::<Vec<_>>();
    assert_eq!(retries.len(), 1, "one all-mode retry is remote scoped");
    assert!(retries[0].lease_id.is_none() && retries[0].target_id.is_none());
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/shared-failed"),
            (http::Method::POST, "/v1/ipfs/pins"),
        ]
    );
    for request in &requests {
        assert!(request.has_valid_authorization());
    }
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_failed_all_unknown_remote_retains_request_without_resubmission() {
    let mut config = two_provider_request_config(
        vec![
            pinning_policy("one/", "one", &["pinata-primary", "filebase-primary"]),
            pinning_policy("all-a/", "all", &["pinata-primary"]),
            pinning_policy("all-b/", "all", &["pinata-primary"]),
        ],
        3,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "failed-all", PIN_CID_SHARED, "queued"),
        PsaReply::pin_status(
            "/psa/pins/failed-all",
            "failed-all",
            PIN_CID_SHARED,
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "one-fallback",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["one/key.txt", "all-a/key.txt", "all-b/key.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    harness.run_worker_until_idle().await;
    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("shared failed request Poll");
    harness.advance_job_due(&poll.id).await;
    harness.restart_worker();
    let fallback_post = harness
        .wait_for_provider_request("filebase-primary", http::Method::POST, "/v1/ipfs/pins", 1)
        .await;
    let fallback_job_id = serde_json::from_slice::<serde_json::Value>(&fallback_post.body)
        .expect("fallback Submit body")
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("fallback job id")
        .to_owned();
    let fallback_job_id = submitted_job_id_for_correlation(&harness, &fallback_job_id).await;
    harness.wait_for_job_state(&fallback_job_id, "done").await;
    harness.stop_worker_without_unlocking().await;

    let failed = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (failed.status.as_str(), failed.failure_attempts),
        ("failed", 1)
    );
    let failed_epoch = failed.epoch;
    assert!(failed.next_retry_at.is_some());
    assert_eq!(
        harness.provider_usages().await[0].reserved_pins,
        1,
        "failed retry retains primary capacity"
    );

    harness
        .advance_remote_retry_due("pinata-primary", PIN_CID_SHARED)
        .await;
    harness
        .run_current_reconcile("pinata-primary", PIN_CID_SHARED)
        .await;

    let retained = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (
            retained.status.as_str(),
            retained.request_id.as_deref(),
            retained.epoch
        ),
        ("failed", Some("failed-all"), failed_epoch),
        "an unowned failed request cannot be deleted to permit a replacement Submit"
    );
    assert_eq!(retained.failure_attempts, 1);
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_SHARED, "retained").await;
    assert_eq!(
        harness.provider_usages().await[0].reserved_pins,
        1,
        "failed unknown primary retains its reservation"
    );
    for key in ["all-a/key.txt", "all-b/key.txt"] {
        assert_eq!(
            harness.target_states(key).await,
            vec![("pinata-primary".to_owned(), "degraded".to_owned())]
        );
        assert_signed_body(&harness, key, b"shared").await;
    }
    let one_object = latest_pinning_object(&harness, "one/key.txt").await;
    let one_lease = owner_leases(&harness, &one_object.id).await.remove(0);
    assert_eq!(
        one_lease.generation, 3,
        "one terminal failover and its pinned-secondary convergence are the only generation advances"
    );
    assert_eq!(
        lease_targets(&harness, &one_lease.id)
            .await
            .into_iter()
            .filter(|target| target.provider == "filebase-primary")
            .map(|target| target.state)
            .collect::<Vec<_>>(),
        vec!["pinned".to_owned()]
    );
    let pinata_requests = harness.pinata_requests().await;
    assert_eq!(
        pinata_requests
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/failed-all"),
        ]
    );
    assert_no_psa_delete(&harness).await;
    assert_eq!(
        harness.filebase_requests().await.len(),
        1,
        "one fallback Submit was selected for the one lease"
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_failed_unknown_remote_stops_without_delete_or_retry_spin() {
    let mut config = PinningHarnessConfig::request_one();
    config.policies[0].provider_mode = "all".to_owned();
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "failed-cycle-1", PIN_CID_SHARED, "queued"),
        PsaReply::pin_status(
            "/psa/pins/failed-cycle-1",
            "failed-cycle-1",
            PIN_CID_SHARED,
            "failed",
        ),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "eight.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;

    let poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("failed request has one pending Poll");
    harness.advance_job_due(&poll.id).await;
    harness.run_worker_until_idle().await;
    let failed = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (
            failed.status.as_str(),
            failed.failure_attempts,
            failed.last_failed_request_id.as_deref()
        ),
        ("failed", 1, Some("failed-cycle-1"))
    );
    assert!(failed.next_retry_at.is_some());
    harness
        .run_current_reconcile("pinata-primary", PIN_CID_SHARED)
        .await;
    harness
        .advance_remote_retry_due("pinata-primary", PIN_CID_SHARED)
        .await;
    harness
        .run_current_reconcile("pinata-primary", PIN_CID_SHARED)
        .await;
    let retained = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (retained.request_id.as_deref(), retained.failure_attempts),
        (Some("failed-cycle-1"), 1)
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_SHARED, "retained").await;
    assert_eq!(
        harness.target_states("eight.txt").await,
        vec![("pinata-primary".to_owned(), "degraded".to_owned())]
    );
    assert_eq!(
        harness.provider_usage("pinata-primary").await.reserved_pins,
        1
    );
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .map(|r| &r.method)
            .collect::<Vec<_>>(),
        vec![&http::Method::POST, &http::Method::GET]
    );
    assert_no_psa_delete(&harness).await;
    assert!(
        harness
            .pin_jobs()
            .await
            .iter()
            .all(|job| job.state != "pending" || job.next_attempt_at > Utc::now()),
        "fenced retry cannot spin"
    );

    let object = latest_pinning_object(&harness, "eight.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);
    let equal_retain_until = manual.expires_at.to_rfc3339();
    let equal = signed_put_object_tagging(
        &harness,
        "eight.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", equal_retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(equal.status(), StatusCode::OK);
    let equal_remote = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (equal_remote.failure_attempts, equal_remote.next_retry_at),
        (1, retained.next_retry_at),
        "equal renewal cannot restart a failed unknown request"
    );

    let extended_retain_until = (manual.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let extended = signed_put_object_tagging(
        &harness,
        "eight.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", extended_retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(extended.status(), StatusCode::OK);
    let reset_by_extension = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        reset_by_extension.failure_attempts, 0,
        "generation-advancing extension resets failed retry state"
    );
    assert!(reset_by_extension.next_retry_at.is_some());

    let copied = signed_copy_with_tagging(
        &harness,
        "eight.txt",
        "eight-new-target.txt",
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    let reset_by_target = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        reset_by_target.failure_attempts, 0,
        "a new shared target also keeps the retry reset"
    );
    assert!(reset_by_target.next_retry_at.is_some());
    assert_eq!(
        harness.pinata_requests().await.len(),
        2,
        "no new PSA request from renewal or shared CID"
    );
    assert_no_psa_delete(&harness).await;
    assert_signed_body(&harness, "eight.txt", b"shared").await;
    assert_signed_body(&harness, "eight-new-target.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_stale_poll_owner_hands_off_without_duplicate_post() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(2);
    config.pinata_script = vec![PsaReply::submit_status(
        "/psa/pins",
        "handoff-request",
        PIN_CID_SHARED,
        "queued",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["first.txt", "second.txt"] {
        let response =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let first = latest_pinning_object(&harness, "first.txt").await;
    let first_lease = owner_leases(&harness, &first.id).await.remove(0);
    let first_target = lease_targets(&harness, &first_lease.id).await.remove(0);
    let second = latest_pinning_object(&harness, "second.txt").await;
    let second_lease = owner_leases(&harness, &second.id).await.remove(0);
    let second_target = lease_targets(&harness, &second_lease.id).await.remove(0);

    harness.run_worker_until_idle().await;
    let stale_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("the queued shared remote has one Poll");
    assert_eq!(
        stale_poll.target_id.as_deref(),
        Some(first_target.id.as_str())
    );
    assert_eq!(stale_poll.expected_generation, Some(first_lease.generation));

    let cancelled = signed_delete_object_tagging(&harness, "first.txt").await;
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        lease_targets(&harness, &first_lease.id).await[0].state,
        "released"
    );
    let remote_after_cancel = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        remote_after_cancel.epoch, 3,
        "cancelling the old Poll owner advances its remote epoch"
    );

    harness
        .run_current_reconcile("pinata-primary", PIN_CID_SHARED)
        .await;
    let handoff_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| {
            job.operation == "poll"
                && job.state == "pending"
                && job.target_id.as_deref() == Some(second_target.id.as_str())
        })
        .expect("current Reconcile hands queued polling to the next target");
    assert_eq!(
        handoff_poll.expected_generation,
        Some(second_lease.generation)
    );
    assert_ne!(handoff_poll.id, stale_poll.id);
    assert_eq!(
        harness.provider_requests().await.len(),
        1,
        "Poll handoff never re-Submits"
    );
    assert_submit_request(
        &harness.provider_requests().await[0],
        "/psa/pins",
        PIN_CID_SHARED,
    );
    assert_signed_body(&harness, "first.txt", b"shared").await;
    assert_signed_body(&harness, "second.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_stale_remote_epoch_parks_unowned_unpin_without_delete() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(1);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "epoch-pinned",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "epoch.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "epoch.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);

    let first_extension = (manual.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let first_renewal = signed_put_object_tagging(
        &harness,
        "epoch.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", first_extension.as_str()),
        ],
    )
    .await;
    assert_eq!(first_renewal.status(), StatusCode::OK);
    let second_extension = (manual.expires_at + ChronoDuration::hours(2)).to_rfc3339();
    let second_renewal = signed_put_object_tagging(
        &harness,
        "epoch.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", second_extension.as_str()),
        ],
    )
    .await;
    assert_eq!(second_renewal.status(), StatusCode::OK);
    assert_eq!(
        remote_pin(&harness, "pinata-primary", PIN_CID_SHARED)
            .await
            .epoch,
        3
    );

    let cancelled = signed_delete_object_tagging(&harness, "epoch.txt").await;
    assert_eq!(cancelled.status(), StatusCode::NO_CONTENT);
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin" && job.expected_remote_epoch == Some(4))
        .expect("cancellation publishes the epoch-four Unpin");
    assert_eq!(
        remote_pin(&harness, "pinata-primary", PIN_CID_SHARED)
            .await
            .epoch,
        4
    );

    let copied = signed_copy_with_tagging(
        &harness,
        "epoch.txt",
        "epoch-shared.txt",
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some(format!("\"{PIN_CID_SHARED}\"").as_str())
    );
    let current = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!((current.epoch, current.status.as_str()), (5, "pinned"));
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_SHARED, "confirmed").await;

    harness.restart_worker();
    let parked = harness.wait_for_job_attention(&unpin.id).await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(
        (parked.state.as_str(), parked.locked_until),
        ("running", None),
        "old Unpin is durably parked for attention rather than reaching provider I/O"
    );
    assert_eq!(
        remote_pin(&harness, "pinata-primary", PIN_CID_SHARED)
            .await
            .epoch,
        5
    );
    assert_eq!(
        harness.target_states("epoch-shared.txt").await,
        vec![("pinata-primary".to_owned(), "pinned".to_owned())]
    );
    let ledger = store::pinning::ledger::get(
        harness.state.store.db(),
        &harness.provider_key("pinata-primary"),
        PIN_CID_SHARED,
    )
    .await
    .expect("load parked resource ledger")
    .expect("ledger exists");
    assert_eq!(
        ledger.last_error.as_deref(),
        Some("historical identity unavailable; needs_attention")
    );
    assert!(
        harness
            .provider_requests()
            .await
            .iter()
            .all(|request| request.method != http::Method::DELETE),
        "epoch-four Unpin must not DELETE after epoch five becomes desired"
    );
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .filter(|r| r.method == http::Method::POST)
            .count(),
        1
    );
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(6, 1)],
        "the stale DELETE path retains the unique reservation"
    );
    assert_signed_body(&harness, "epoch.txt", b"shared").await;
    assert_signed_body(&harness, "epoch-shared.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_one_mode_failover_retains_unknown_primary_without_duplicate_post() {
    let mut config = two_provider_request_config(
        vec![pinning_policy(
            "",
            "one",
            &["pinata-primary", "filebase-primary"],
        )],
        1,
    );
    config.pinata_script = vec![
        PsaReply::submit_status("/psa/pins", "sticky-primary", PIN_CID_SHARED, "queued"),
        PsaReply::empty(
            http::Method::GET,
            "/psa/pins/sticky-primary",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::pin_status(
            "/psa/pins/sticky-primary",
            "sticky-primary",
            PIN_CID_SHARED,
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "sticky-secondary",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "sticky.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let initial_poll = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "poll" && job.state == "pending")
        .expect("queued primary Poll");
    harness.advance_job_due(&initial_poll.id).await;
    harness.run_worker_until_idle().await;

    let object = latest_pinning_object(&harness, "sticky.txt").await;
    let lease_after_transient = owner_leases(&harness, &object.id).await.remove(0);
    assert_eq!(
        lease_after_transient.generation, 1,
        "transient primary failure stays sticky"
    );
    assert!(
        lease_targets(&harness, &lease_after_transient.id)
            .await
            .iter()
            .all(|target| target.provider == "pinata-primary"),
        "transient failure must not create a fallback target"
    );
    assert!(harness.filebase_requests().await.is_empty());
    let retry_poll = harness.pin_job(&initial_poll.id).await;
    assert_eq!(
        (retry_poll.state.as_str(), retry_poll.attempts),
        ("pending", 1)
    );

    harness.advance_job_due(&retry_poll.id).await;
    harness.restart_worker();
    let fallback_post = harness
        .wait_for_provider_request("filebase-primary", http::Method::POST, "/v1/ipfs/pins", 1)
        .await;
    let fallback_job_id = serde_json::from_slice::<serde_json::Value>(&fallback_post.body)
        .expect("fallback Submit body")
        .pointer("/meta/gateway_job_id")
        .and_then(serde_json::Value::as_str)
        .expect("fallback job id")
        .to_owned();
    let fallback_job_id = submitted_job_id_for_correlation(&harness, &fallback_job_id).await;
    harness.wait_for_job_state(&fallback_job_id, "done").await;
    harness.stop_worker_without_unlocking().await;
    let converged_lease = owner_leases(&harness, &object.id).await.remove(0);
    assert_eq!(
        converged_lease.generation, 3,
        "terminal failover then pinned convergence advance the generation once each"
    );
    let targets = lease_targets(&harness, &converged_lease.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| (target.provider.as_str(), target.state.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("filebase-primary", "pinned"),
            ("pinata-primary", "released"),
        ]
    );
    let required = targets
        .iter()
        .filter(|target| {
            matches!(
                target.state.as_str(),
                "waiting" | "submitted" | "pinned" | "degraded"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        required
            .iter()
            .map(|target| target.provider.as_str())
            .collect::<Vec<_>>(),
        vec!["filebase-primary"],
        "one-mode converges to exactly one required provider without failback"
    );
    assert_eq!(
        (
            remote_pin(&harness, "filebase-primary", PIN_CID_SHARED)
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", PIN_CID_SHARED)
                .await
                .status
                .as_str()
        ),
        ("pinned", "failed")
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_SHARED, "unknown").await;
    assert_eq!(
        harness
            .pinata_requests()
            .await
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (http::Method::POST, "/psa/pins"),
            (http::Method::GET, "/psa/pins/sticky-primary"),
            (http::Method::GET, "/psa/pins/sticky-primary"),
        ]
    );
    assert_no_psa_delete(&harness).await;
    assert_eq!(
        harness.provider_usage("pinata-primary").await.reserved_pins,
        1
    );
    assert_eq!(
        harness
            .filebase_requests()
            .await
            .iter()
            .map(|request| (request.method.clone(), request.path.as_str()))
            .collect::<Vec<_>>(),
        vec![(http::Method::POST, "/v1/ipfs/pins")]
    );
    assert_signed_body(&harness, "sticky.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_all_mode_partial_success_keeps_retrying_degraded_provider() {
    let mut config = PinningHarnessConfig::automatic_all();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "partial-pinata",
        TEST_CID,
    )];
    config.filebase_script = vec![
        PsaReply::empty(
            http::Method::POST,
            "/v1/ipfs/pins",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::find_none_for_job("/v1/ipfs/pins", TEST_CID),
    ];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(&harness, "partial.txt", b"happy".to_vec(), "").await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;

    assert_eq!(
        harness.target_states("partial.txt").await,
        vec![
            ("filebase-primary".to_owned(), "degraded".to_owned()),
            ("pinata-primary".to_owned(), "pinned".to_owned()),
        ],
        "all-mode remains available through Pinata while Filebase is visibly degraded"
    );
    let pinata = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    let filebase = remote_pin(&harness, "filebase-primary", TEST_CID).await;
    assert_eq!(
        (pinata.status.as_str(), filebase.status.as_str()),
        ("pinned", "reserved")
    );
    let retry = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "submit" && job.provider == "filebase-primary")
        .expect("degraded all-mode provider retains a durable Submit recovery");
    assert_eq!(retry.state, "pending");
    assert_eq!(retry.submit_phase.as_deref(), Some("recovery_backoff"));
    assert!(retry.next_attempt_at > Utc::now());
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 3);
    assert_submit_request(&requests[0], "/psa/pins", TEST_CID);
    assert_submit_request(&requests[1], "/v1/ipfs/pins", TEST_CID);
    assert_find_request_for_job(
        &requests[2],
        "/v1/ipfs/pins",
        TEST_CID,
        &captured_submit_correlation(&harness, &retry.id).await,
    );
    assert_signed_body(&harness, "partial.txt", b"happy").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_quota_keeps_oldest_unknown_cid_reserved_after_eviction() {
    let mut config = PinningHarnessConfig::request_one();
    config.providers[0].max_bytes = 6;
    config.providers[0].max_pins = 2;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_OLD),
            AddReply::Ok(PIN_CID_NEWER),
            AddReply::Ok(PIN_CID_INCOMING),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_OLD.to_owned(), b"old".to_vec()),
            (PIN_CID_NEWER.to_owned(), b"new".to_vec()),
            (PIN_CID_INCOMING.to_owned(), b"in!".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "old-request", PIN_CID_OLD),
        PsaReply::pinned_submit("/psa/pins", "newer-request", PIN_CID_NEWER),
    ];
    let mut harness = start_pinning_harness(config).await;

    for (key, body, cid) in [
        ("old.txt", b"old".as_slice(), PIN_CID_OLD),
        ("newer.txt", b"new".as_slice(), PIN_CID_NEWER),
    ] {
        let put = signed_put_with_tagging(&harness, key, body.to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&put, cid);
        harness.run_worker_until_idle().await;
    }

    let newer = latest_pinning_object(&harness, "newer.txt").await;
    let newer_lease = owner_leases(&harness, &newer.id).await.remove(0);
    let renewed_until = (newer_lease.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "newer.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", renewed_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    assert_eq!(
        owner_leases(&harness, &newer.id).await[0].generation,
        newer_lease.generation + 1,
        "renewing the newer CID makes the older CID the eviction candidate"
    );

    let incoming = signed_put_with_tagging(
        &harness,
        "incoming.txt",
        b"in!".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(incoming.status(), StatusCode::OK);
    assert_put_cid_headers(&incoming, PIN_CID_INCOMING);
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "the incoming CID cannot reserve until a remote DELETE is confirmed"
    );

    harness
        .run_worker_until_remote_effect("pinata-primary", PIN_CID_OLD, "retained")
        .await;
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "eviction of an unknown resource cannot free its reservation"
    );
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );

    assert_eq!(
        remote_pin(&harness, "pinata-primary", PIN_CID_OLD)
            .await
            .status,
        "pinned"
    );
    assert_eq!(
        harness.remote_pins().await.len(),
        2,
        "no capacity for incoming CID"
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_OLD, "retained").await;
    assert_eq!(
        harness.target_states("incoming.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 2),
        "incoming CID cannot take an unproven resource's capacity"
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 2);
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_OLD);
    assert_submit_request(&requests[1], "/psa/pins", PIN_CID_NEWER);
    assert_no_psa_delete(&harness).await;
    assert_signed_body(&harness, "incoming.txt", b"in!").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_shared_cid_counts_once_and_blocks_unsafe_unpin() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = repeated_shared_kubo(2);
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-request",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["first.txt", "second.txt"] {
        let put =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&put, PIN_CID_SHARED);
    }
    harness.run_worker_until_idle().await;
    let first = latest_pinning_object(&harness, "first.txt").await;
    let second = latest_pinning_object(&harness, "second.txt").await;
    let first_lease = owner_leases(&harness, &first.id).await.remove(0);
    let second_lease = owner_leases(&harness, &second.id).await.remove(0);
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1),
        "two S3 keys sharing one CID reserve the provider once"
    );

    let cancel = signed_delete_object_tagging(&harness, "first.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    assert_tagging(&harness, "first.txt", &[]).await;
    harness.run_worker_until_idle().await;

    assert_eq!(
        (
            owner_leases(&harness, &first.id).await[0].id.as_str(),
            owner_leases(&harness, &first.id).await[0].state.as_str(),
            lease_targets(&harness, &first_lease.id).await[0]
                .state
                .as_str(),
        ),
        (first_lease.id.as_str(), "cancelled", "released")
    );
    assert_eq!(
        (
            owner_leases(&harness, &second.id).await[0].id.as_str(),
            owner_leases(&harness, &second.id).await[0].state.as_str(),
            lease_targets(&harness, &second_lease.id).await[0]
                .state
                .as_str(),
        ),
        (second_lease.id.as_str(), "active", "pinned")
    );
    let remote = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (remote.status.as_str(), remote.request_id.as_deref()),
        ("pinned", Some("shared-request"))
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1)
    );
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        1,
        "the remaining desired target blocks Unpin"
    );
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_SHARED);
    assert_signed_body(&harness, "first.txt", b"shared").await;
    assert_signed_body(&harness, "second.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_renewal_generation_reuses_retained_unknown_expiry_pin() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "expiry-request",
        TEST_CID,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "expiry-renew.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let initial_submit = only_submit_job(&harness).await;
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "expiry-renew.txt").await;
    let initial_lease = owner_leases(&harness, &object.id).await.remove(0);
    let initial_target = lease_targets(&harness, &initial_lease.id).await.remove(0);

    harness.advance_past_lease_expiry("expiry-renew.txt").await;
    harness.restart_worker();
    harness
        .wait_for_lease_state("expiry-renew.txt", "expired")
        .await;
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;
    let expired_lease = owner_leases(&harness, &object.id).await.remove(0);
    let expired_remote = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            expired_lease.id.as_str(),
            expired_lease.state.as_str(),
            lease_targets(&harness, &initial_lease.id).await[0]
                .state
                .as_str(),
        ),
        (initial_lease.id.as_str(), "expired", "released")
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (4, 1),
        "expiry cannot release quota on an unproven PSA request"
    );

    let retain_until = (Utc::now() + ChronoDuration::hours(1)).to_rfc3339();
    let renew = signed_put_object_tagging(
        &harness,
        "expiry-renew.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renew.status(), StatusCode::OK);
    let renewed_lease = owner_leases(&harness, &object.id).await.remove(0);
    let renewed_target = lease_targets(&harness, &renewed_lease.id).await.remove(0);
    let renewed_remote = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            renewed_lease.id.as_str(),
            renewed_lease.state.as_str(),
            renewed_target.id.as_str(),
        ),
        (
            initial_lease.id.as_str(),
            "active",
            initial_target.id.as_str()
        )
    );
    assert!(renewed_lease.generation > expired_lease.generation);
    assert!(renewed_remote.epoch > expired_remote.epoch);
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (4, 1),
        "reactivation retains the original unique reservation"
    );

    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("expiry publishes one Unpin");
    assert_eq!(
        unpin.state, "done",
        "expired unpin is fenced by reactivation"
    );
    assert!(
        harness
            .pin_jobs()
            .await
            .iter()
            .all(|job| job.operation != "submit" || job.id == initial_submit.id),
        "retained remote needs no replacement Submit"
    );

    let pinned = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (pinned.status.as_str(), pinned.request_id.as_deref()),
        ("pinned", Some("expiry-request"))
    );
    assert_eq!(
        lease_targets(&harness, &initial_lease.id).await[0].state,
        "pinned"
    );
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request_for_job(&requests[0], "/psa/pins", TEST_CID, &initial_submit);
    assert_no_psa_delete(&harness).await;
    assert_unknown_psa_resource(&harness, "pinata-primary", TEST_CID, "retained").await;
    assert_signed_body(&harness, "expiry-renew.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_new_shared_target_after_cancel_reuses_unknown_remote_without_release() {
    let mut config = PinningHarnessConfig::request_one();
    config.providers[0].max_bytes = 6;
    config.providers[0].max_pins = 1;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_SHARED),
            AddReply::Ok(PIN_CID_WAIT),
            AddReply::Ok(PIN_CID_SHARED),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_SHARED.to_owned(), b"shared".to_vec()),
            (PIN_CID_WAIT.to_owned(), b"wait".to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-before-delete",
        PIN_CID_SHARED,
    )];
    let mut harness = start_pinning_harness(config).await;

    let first = signed_put_with_tagging(
        &harness,
        "delete-race-first.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    harness.run_worker_until_idle().await;
    let initial_remote = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;

    let wait = signed_put_with_tagging(
        &harness,
        "quota-waiter.txt",
        b"wait".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(wait.status(), StatusCode::OK);
    assert_eq!(
        harness.target_states("quota-waiter.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())]
    );

    let cancel = signed_delete_object_tagging(&harness, "delete-race-first.txt").await;
    assert_eq!(cancel.status(), StatusCode::NO_CONTENT);
    harness.run_worker_until_idle().await;
    assert_unknown_psa_resource(&harness, "pinata-primary", PIN_CID_SHARED, "retained").await;

    let shared_new = signed_put_with_tagging(
        &harness,
        "delete-race-new.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(shared_new.status(), StatusCode::OK);
    assert_put_cid_headers(&shared_new, PIN_CID_SHARED);
    let before_delete_completion = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(before_delete_completion.epoch, initial_remote.epoch + 2);

    let retained = remote_pin(&harness, "pinata-primary", PIN_CID_SHARED).await;
    assert_eq!(
        (
            retained.status.as_str(),
            retained.request_id.as_deref(),
            retained.epoch,
        ),
        (
            "pinned",
            Some("shared-before-delete"),
            initial_remote.epoch + 2
        )
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (6, 1),
        "unknown shared remote keeps its reservation rather than releasing it"
    );
    assert_eq!(
        harness.target_states("quota-waiter.txt").await,
        vec![("pinata-primary".to_owned(), "quota_waiting".to_owned())],
        "the retained reservation must not wake an unrelated quota waiter"
    );
    let new_object = latest_pinning_object(&harness, "delete-race-new.txt").await;
    let new_lease = owner_leases(&harness, &new_object.id).await.remove(0);
    let new_target = lease_targets(&harness, &new_lease.id).await.remove(0);
    assert_eq!(new_target.state, "pinned");
    let requests = harness.provider_requests().await;
    assert_eq!(
        requests.len(),
        1,
        "shared CID must not need a replacement Submit"
    );
    assert_submit_request(&requests[0], "/psa/pins", PIN_CID_SHARED);
    assert_no_psa_delete(&harness).await;
    assert_signed_body(&harness, "delete-race-new.txt", b"shared").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_expiry_retains_unknown_remote_and_preserves_s3_and_kubo_pin() {
    let mut config = PinningHarnessConfig::request_one();
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "expiry-delete-request",
        TEST_CID,
    )];
    let mut harness = start_pinning_harness(config).await;

    let put = signed_put_with_tagging(
        &harness,
        "expired.txt",
        b"body".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, TEST_CID);
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "expired.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);
    let target = lease_targets(&harness, &manual.id).await.remove(0);
    assert_unknown_psa_resource(&harness, "pinata-primary", TEST_CID, "confirmed").await;

    harness.advance_past_lease_expiry("expired.txt").await;
    harness.restart_worker();
    harness.wait_for_lease_state("expired.txt", "expired").await;
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;

    let expired = owner_leases(&harness, &object.id).await.remove(0);
    let released_target = lease_targets(&harness, &manual.id).await.remove(0);
    let remote = remote_pin(&harness, "pinata-primary", TEST_CID).await;
    assert_eq!(
        (
            expired.id.as_str(),
            expired.state.as_str(),
            expired.generation,
            released_target.id.as_str(),
            released_target.state.as_str(),
        ),
        (
            manual.id.as_str(),
            "expired",
            manual.generation + 1,
            target.id.as_str(),
            "released",
        )
    );
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("expiry-delete-request"), 2)
    );
    assert_eq!(
        (
            harness
                .provider_usage("pinata-primary")
                .await
                .reserved_bytes,
            harness.provider_usage("pinata-primary").await.reserved_pins,
        ),
        (4, 1)
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", TEST_CID, "retained").await;
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", TEST_CID);
    assert_no_psa_delete(&harness).await;
    assert_signed_body(&harness, "expired.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_decompressed_pins_entries_not_archive() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_ARCHIVE),
            AddReply::Ok(PIN_CID_ENTRY1),
            AddReply::Ok(PIN_CID_ENTRY2),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_ARCHIVE.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY1.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", PIN_CID_ENTRY1),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", PIN_CID_ENTRY2),
    ];
    let mut harness = start_pinning_harness(config).await;
    let pinata_key = harness.provider_key("pinata-primary");

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=zip&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("decompress ZIP result XML");
    assert_eq!(
        xml_text(&put_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&put_xml, "ArchiveETag").as_deref(),
        Some(PIN_CID_ARCHIVE)
    );
    assert_eq!(xml_text(&put_xml, "ExtractedCount").as_deref(), Some("2"));
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:content", "decompressed"),
            ("ipfs-s3:pin", "true"),
            ("team", "zip"),
        ],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let first = latest_pinning_object(&harness, "entries/first.txt").await;
    let second = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &first.id).await.is_empty());
    assert!(owner_leases(&harness, &second.id).await.is_empty());

    let leases = owner_leases(&harness, &archive.id).await;
    assert_eq!(leases.len(), 1);
    let manual = leases.into_iter().next().expect("manual archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.provider_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (
            archive.id.as_str(),
            "manual",
            "decompressed",
            "one",
            "active",
            1
        )
    );
    let targets = lease_targets(&harness, &manual.id).await;
    let target_cids = targets
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from([PIN_CID_ENTRY1.to_owned(), PIN_CID_ENTRY2.to_owned()])
    );
    assert!(!target_cids.contains(PIN_CID_ARCHIVE));
    assert!(targets.iter().all(|target| {
        target.provider == pinata_key && target.state == "waiting" && target.lease_id == manual.id
    }));
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            pinata_key.clone(),
            (FIRST_ENTRY_BYTES.len() + SECOND_ENTRY_BYTES.len()) as i64,
            2,
        )]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 2);
    assert_eq!(
        jobs.iter()
            .map(|job| {
                (
                    job.operation.as_str(),
                    job.provider.as_str(),
                    job.cid.as_str(),
                    job.lease_id.as_deref(),
                    job.expected_generation,
                    job.expected_remote_epoch,
                    job.state.as_str(),
                )
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (
                "submit",
                pinata_key.as_str(),
                PIN_CID_ENTRY1,
                Some(manual.id.as_str()),
                Some(1),
                None,
                "pending",
            ),
            (
                "submit",
                pinata_key.as_str(),
                PIN_CID_ENTRY2,
                Some(manual.id.as_str()),
                Some(1),
                None,
                "pending",
            ),
        ])
    );
    assert_eq!(
        jobs.iter()
            .map(|job| job.target_id.as_deref())
            .collect::<BTreeSet<_>>(),
        targets
            .iter()
            .map(|target| Some(target.id.as_str()))
            .collect::<BTreeSet<_>>()
    );

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2);
    let submitted_cids = requests
        .iter()
        .map(|request| {
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .expect("PSA submit JSON")
                .get("cid")
                .and_then(serde_json::Value::as_str)
                .expect("PSA submit CID")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(submitted_cids, target_cids);
    for job in &jobs {
        let request = requests
            .iter()
            .find(|request| {
                request
                    .body
                    .windows(job.cid.len())
                    .any(|window| window == job.cid.as_bytes())
            })
            .expect("PSA request for submitted entry");
        assert_submit_request_for_job(request, "/psa/pins", &job.cid, job);
        assert_eq!(harness.pin_job(&job.id).await.state, "done");
    }
    for (cid, request_id) in [
        (PIN_CID_ENTRY1, "pinata-entry-1"),
        (PIN_CID_ENTRY2, "pinata-entry-2"),
    ] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            ("pinned", Some(request_id), 1)
        );
    }
    assert!(
        lease_targets(&harness, &manual.id)
            .await
            .iter()
            .all(|target| target.state == "pinned")
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_partial_success_targets_only_published_entries() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_ARCHIVE),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok(PIN_CID_ENTRY2),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_ARCHIVE.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-entry-2",
        PIN_CID_ENTRY2,
    )];
    let mut harness = start_pinning_harness(config).await;
    let pinata_key = harness.provider_key("pinata-primary");

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=partial&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let put_xml = put.text().await.expect("partial decompression XML");
    assert_eq!(xml_text(&put_xml, "ExtractedCount").as_deref(), Some("1"));
    assert_eq!(xml_text(&put_xml, "FailedCount").as_deref(), Some("1"));
    assert!(put_xml.contains("EntryUploadFailed"));
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_s3_error(
        signed_get(&harness, "entries/first.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let published = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &published.id).await.is_empty());
    let manual = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    assert_eq!(
        targets
            .iter()
            .map(|target| (
                target.provider.as_str(),
                target.cid.as_str(),
                target.state.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![(pinata_key.as_str(), PIN_CID_ENTRY2, "waiting")]
    );
    assert_ne!(targets[0].cid, archive.cid);
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(pinata_key.clone(), SECOND_ENTRY_BYTES.len() as i64, 1)]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        (
            jobs[0].operation.as_str(),
            jobs[0].provider.as_str(),
            jobs[0].cid.as_str(),
            jobs[0].lease_id.as_deref(),
            jobs[0].target_id.as_deref(),
            jobs[0].expected_generation,
            jobs[0].expected_remote_epoch,
            jobs[0].state.as_str(),
        ),
        (
            "submit",
            pinata_key.as_str(),
            PIN_CID_ENTRY2,
            Some(manual.id.as_str()),
            Some(targets[0].id.as_str()),
            Some(1),
            None,
            "pending",
        )
    );

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request_for_job(&requests[0], "/psa/pins", PIN_CID_ENTRY2, &jobs[0]);
    let remote = remote_pin(&harness, "pinata-primary", PIN_CID_ENTRY2).await;
    assert_eq!(
        (
            remote.status.as_str(),
            remote.request_id.as_deref(),
            remote.epoch
        ),
        ("pinned", Some("pinata-entry-2"), 1)
    );
    assert!(
        harness
            .remote_pins()
            .await
            .iter()
            .all(|remote| remote.cid != PIN_CID_ARCHIVE)
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_archive_renewal_updates_all_entry_targets() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_ARCHIVE),
            AddReply::Ok(PIN_CID_ENTRY1),
            AddReply::Ok(PIN_CID_ENTRY2),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_ARCHIVE.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY1.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", PIN_CID_ENTRY1),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", PIN_CID_ENTRY2),
    ];
    let mut harness = start_pinning_harness(config).await;
    let pinata_key = harness.provider_key("pinata-primary");

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "entries/",
        archive_bytes.clone(),
        "team=initial&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    harness.run_worker_until_idle().await;

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    let manual_before = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual archive lease");
    let targets_before = lease_targets(&harness, &manual_before.id).await;
    assert_eq!(targets_before.len(), 2);
    assert!(targets_before.iter().all(|target| target.state == "pinned"));
    let target_ids = targets_before
        .iter()
        .map(|target| target.id.clone())
        .collect::<BTreeSet<_>>();
    let target_cids = targets_before
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from([PIN_CID_ENTRY1.to_owned(), PIN_CID_ENTRY2.to_owned()])
    );
    let retain_until = (manual_before.expires_at + ChronoDuration::hours(1)).to_rfc3339();
    let renewal = signed_put_object_tagging(
        &harness,
        "archive.zip",
        &[
            ("team", "renewed"),
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
        ],
    )
    .await;
    assert_eq!(renewal.status(), StatusCode::OK);
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", retain_until.as_str()),
            ("team", "renewed"),
        ],
    )
    .await;

    let manual_after = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("renewed manual archive lease");
    let expected_expiry = chrono::DateTime::parse_from_rfc3339(&retain_until)
        .expect("retain-until RFC3339")
        .with_timezone(&Utc);
    assert_eq!(
        (
            manual_after.id.as_str(),
            manual_after.owner_object_id.as_str(),
            manual_after.source.as_str(),
            manual_after.content_mode.as_str(),
            manual_after.state.as_str(),
            manual_after.generation,
            manual_after.expires_at,
        ),
        (
            manual_before.id.as_str(),
            archive.id.as_str(),
            "manual",
            "decompressed",
            "active",
            2,
            expected_expiry,
        )
    );
    let targets_after = lease_targets(&harness, &manual_after.id).await;
    assert_eq!(
        targets_after
            .iter()
            .map(|target| target.id.clone())
            .collect::<BTreeSet<_>>(),
        target_ids
    );
    assert_eq!(
        targets_after
            .iter()
            .map(|target| target.cid.clone())
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    assert!(targets_after.iter().all(|target| {
        target.lease_id == manual_after.id
            && target.state == "pinned"
            && manual_after.generation == 2
            && manual_after.expires_at == expected_expiry
    }));
    let renewal_jobs = harness
        .pin_jobs()
        .await
        .into_iter()
        .filter(|job| job.operation == "reconcile")
        .collect::<Vec<_>>();
    assert_eq!(renewal_jobs.len(), 2);
    assert_eq!(
        renewal_jobs
            .iter()
            .map(|job| {
                (
                    job.provider.as_str(),
                    job.cid.as_str(),
                    job.lease_id.as_deref(),
                    job.target_id.as_deref(),
                    job.expected_generation,
                    job.expected_remote_epoch,
                    job.state.as_str(),
                )
            })
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (
                pinata_key.as_str(),
                PIN_CID_ENTRY1,
                None,
                None,
                None,
                Some(2),
                "pending",
            ),
            (
                pinata_key.as_str(),
                PIN_CID_ENTRY2,
                None,
                None,
                None,
                Some(2),
                "pending",
            ),
        ])
    );
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2, "renewal must not re-submit entry CIDs");
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body)
                    .expect("PSA submit JSON")
                    .get("cid")
                    .and_then(serde_json::Value::as_str)
                    .expect("PSA submit CID")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_global_reject_creates_no_manual_lease() {
    let archive_bytes = archive_key_collision_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_ARCHIVE),
            AddReply::Ok(PIN_CID_COLLISION),
        ],
        cat_bodies: HashMap::from([(PIN_CID_ARCHIVE.to_owned(), archive_bytes.clone())]),
    };
    config.pinata_script.clear();
    let harness = start_pinning_harness(config).await;

    let put = signed_decompress_zip_put(
        &harness,
        "archive.zip",
        "",
        archive_bytes,
        "team=reject&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_s3_error(
        put,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_s3_error(
        signed_get(&harness, "archive.zip").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("object rows after global reject")
            .is_empty()
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("tag rows after global reject")
            .is_empty()
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());
    assert!(harness.provider_requests().await.is_empty());
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_zip_complete_commits_entries_and_upload_delete_atomically() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_PART),
            AddReply::Ok(PIN_CID_ROOT),
            AddReply::Ok(PIN_CID_ENTRY1),
            AddReply::Ok(PIN_CID_ENTRY2),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_PART.to_owned(), archive_bytes.clone()),
            (PIN_CID_ROOT.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY1.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", PIN_CID_ENTRY1),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", PIN_CID_ENTRY2),
    ];
    let mut harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_zip_upload_with_tagging(
        &harness,
        "archive.zip",
        "entries/",
        "team=multipart&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    assert_eq!(xml_text(&create_xml, "Bucket").as_deref(), Some("test-bkt"));
    assert_eq!(xml_text(&create_xml, "Key").as_deref(), Some("archive.zip"));
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("multipart upload before complete");
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());

    let part = signed_upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        archive_bytes.clone(),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    assert_eq!(part_etag, PIN_CID_PART);
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("multipart part before complete");
    assert_eq!(parts_before.len(), 1);

    let complete =
        signed_complete_multipart(&harness, "archive.zip", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let complete_xml = complete.text().await.expect("CompleteMultipartUpload XML");
    assert_eq!(
        xml_text(&complete_xml, "ArchiveKey").as_deref(),
        Some("archive.zip")
    );
    assert_eq!(
        xml_text(&complete_xml, "ArchiveETag").as_deref(),
        Some(PIN_CID_ROOT)
    );
    assert_eq!(
        xml_text(&complete_xml, "ExtractedCount").as_deref(),
        Some("2")
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after completed ZIP publication")
            .is_empty()
    );

    let archive = latest_pinning_object(&harness, "archive.zip").await;
    assert_eq!(archive.cid, PIN_CID_ROOT);
    assert!(archive.multipart);
    assert_ne!(archive.id, upload_before.object_id);
    uuid::Uuid::parse_str(&archive.id).expect("completion_attempt_id is a UUID");
    assert_tagging(
        &harness,
        "archive.zip",
        &[
            ("ipfs-s3:content", "decompressed"),
            ("ipfs-s3:pin", "true"),
            ("team", "multipart"),
        ],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive_bytes).await;
    assert_signed_body(&harness, "entries/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "entries/second.txt", SECOND_ENTRY_BYTES).await;
    let first = latest_pinning_object(&harness, "entries/first.txt").await;
    let second = latest_pinning_object(&harness, "entries/second.txt").await;
    assert!(owner_leases(&harness, &first.id).await.is_empty());
    assert!(owner_leases(&harness, &second.id).await.is_empty());

    let manual = owner_leases(&harness, &archive.id)
        .await
        .into_iter()
        .next()
        .expect("manual decompressed archive lease");
    assert_eq!(
        (
            manual.owner_object_id.as_str(),
            manual.source.as_str(),
            manual.content_mode.as_str(),
            manual.state.as_str(),
            manual.generation,
        ),
        (archive.id.as_str(), "manual", "decompressed", "active", 1)
    );
    let targets = lease_targets(&harness, &manual.id).await;
    let target_cids = targets
        .iter()
        .map(|target| target.cid.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        target_cids,
        BTreeSet::from([PIN_CID_ENTRY1.to_owned(), PIN_CID_ENTRY2.to_owned()])
    );
    assert!(!target_cids.contains(PIN_CID_ROOT));
    let pinata_key = harness.provider_key("pinata-primary");
    assert!(targets.iter().all(|target| {
        target.provider == pinata_key && target.state == "waiting" && target.lease_id == manual.id
    }));
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            pinata_key.clone(),
            (FIRST_ENTRY_BYTES.len() + SECOND_ENTRY_BYTES.len()) as i64,
            2,
        )]
    );
    let jobs = harness.pin_jobs().await;
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(|job| {
        job.operation == "submit"
            && job.provider == pinata_key
            && job.lease_id.as_deref() == Some(manual.id.as_str())
            && job.expected_generation == Some(1)
            && job.expected_remote_epoch.is_none()
            && job.state == "pending"
            && job
                .target_id
                .as_ref()
                .is_some_and(|id| targets.iter().any(|target| &target.id == id))
    }));

    harness.run_worker_until_idle().await;
    let requests = harness.pinata_requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests
            .iter()
            .map(|request| {
                serde_json::from_slice::<serde_json::Value>(&request.body)
                    .expect("PSA submit JSON")
                    .get("cid")
                    .and_then(serde_json::Value::as_str)
                    .expect("PSA submit CID")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>(),
        target_cids
    );
    for job in &jobs {
        let request = requests
            .iter()
            .find(|request| {
                request
                    .body
                    .windows(job.cid.len())
                    .any(|window| window == job.cid.as_bytes())
            })
            .expect("PSA request for completed ZIP entry");
        assert_submit_request_for_job(request, "/psa/pins", &job.cid, job);
        assert_eq!(harness.pin_job(&job.id).await.state, "done");
    }
    for (cid, request_id) in [
        (PIN_CID_ENTRY1, "pinata-entry-1"),
        (PIN_CID_ENTRY2, "pinata-entry-2"),
    ] {
        let remote = remote_pin(&harness, "pinata-primary", cid).await;
        assert_eq!(
            (
                remote.status.as_str(),
                remote.request_id.as_deref(),
                remote.epoch
            ),
            ("pinned", Some(request_id), 1)
        );
    }
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_multipart_zip_outbox_failure_preserves_upload_and_parts() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.explicit_identity = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(PIN_CID_PART),
            AddReply::Ok(PIN_CID_ROOT),
            AddReply::Ok(PIN_CID_ENTRY1),
            AddReply::Ok(PIN_CID_ENTRY2),
        ],
        cat_bodies: HashMap::from([
            (PIN_CID_PART.to_owned(), archive_bytes.clone()),
            (PIN_CID_ROOT.to_owned(), archive_bytes.clone()),
            (PIN_CID_ENTRY1.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script.clear();
    let harness = start_pinning_harness(config).await;

    let create = signed_create_multipart_zip_upload_with_tagging(
        &harness,
        "archive.zip",
        "entries/",
        "team=rollback&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    let upload_id = xml_text(&create_xml, "UploadId").expect("CreateMultipartUpload UploadId");
    let part = signed_upload_part(&harness, "archive.zip", &upload_id, 1, archive_bytes).await;
    assert_eq!(part.status(), StatusCode::OK);
    let part_etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload before forced outbox failure");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("parts before forced outbox failure");
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_zip_outbox BEFORE INSERT ON pin_jobs \
             BEGIN SELECT RAISE(FAIL, 'forced zip outbox failure'); END;",
        ))
        .await
        .expect("install ZIP outbox failure trigger");

    let complete =
        signed_complete_multipart(&harness, "archive.zip", &upload_id, 1, &part_etag).await;
    assert_eq!(complete.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let complete_xml = complete
        .text()
        .await
        .expect("failed CompleteMultipartUpload XML");
    assert!(complete_xml.contains("<Code>InternalError</Code>"));
    assert!(!complete_xml.contains("forced zip outbox failure"));
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("upload preserved after rollback"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts preserved after rollback"),
        parts_before
    );
    assert_s3_error(
        signed_get(&harness, "archive.zip").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    for key in ["entries/first.txt", "entries/second.txt"] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .is_err(),
            "{key} must not be published after rollback"
        );
    }
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("object rows after outbox rollback")
            .is_empty()
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("tag rows after outbox rollback")
            .is_empty()
    );
    assert!(harness.pin_leases().await.is_empty());
    assert!(harness.pin_targets().await.is_empty());
    assert!(harness.remote_pins().await.is_empty());
    assert!(harness.provider_usages().await.is_empty());
    assert!(harness.pin_jobs().await.is_empty());
    assert!(harness.provider_requests().await.is_empty());
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}
