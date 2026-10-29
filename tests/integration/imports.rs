use super::common::{
    FIRST_ENTRY_BYTES, IMPORT_CID, IMPORT_TEST_CID_V0, OwnedTestEndpoint, SECOND_ENTRY_BYTES,
    assert_import_state, assert_s3_error, assert_signed_body, import_result_count, import_url_xml,
    scripted, seed_latest, seed_running_import, signed_complete_multipart, signed_copy,
    signed_create_multipart_upload_with_tagging, signed_decompress_zip_put, signed_delete_bucket,
    signed_delete_object, signed_delete_object_tagging, signed_delete_objects, signed_get,
    signed_get_object_tagging, signed_head, signed_list_objects, signed_put,
    signed_put_object_tagging, signed_upload_part, sse_c_headers_for, standard_script,
    xml_element_text,
};
use crate::support;
use crate::support::decompress::{
    AddReply, KuboBlockTarget, KuboScript, TestHarness, abort_multipart, assert_no_kubo_calls,
    complete_multipart, create_multipart, legal_single_entry_zip, legal_two_entry_zip,
    start_blocking_harness, start_harness, upload_part,
};
use crate::support::import::{
    ImportHarness, ImportHarnessConfig, ImportPublicationBlockControl, TestHttpsReply,
    get_import_status, post_import, start_import_harness, start_strict_import_harness,
    wait_for_import_state,
};
use chrono::{Duration as ChronoDuration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store;
use sea_orm::{
    ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, TransactionTrait,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

async fn import_destination(
    harness: &TestHarness,
    key: &str,
) -> store::entities::import_destination::Model {
    store::entities::import_destination::Entity::find_by_id((
        harness.bucket.clone(),
        key.to_owned(),
    ))
    .one(harness.state.store.db())
    .await
    .expect("load import destination")
    .expect("import destination exists")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standard_content_mutations_supersede_import() {
    let (put, mut put_block) =
        start_blocking_harness(scripted(&["QmTask5Put"], vec![]), KuboBlockTarget::Add).await;
    seed_running_import(&put, "put-import", "put.txt", None).await;
    let put_endpoint = OwnedTestEndpoint::from(&put);
    let put_task = tokio::spawn(async move {
        signed_put(
            &put_endpoint,
            "put.txt",
            &[],
            b"replacement".to_vec(),
            HeaderMap::new(),
        )
        .await
    });
    put_block.wait_until_blocked().await;
    assert_import_state(&put, "put-import", "superseded").await;
    put_block.release();
    let response = put_task.await.expect("join blocked PutObject");
    assert_eq!(response.status(), StatusCode::OK);

    let invalid_put = start_harness(standard_script(0)).await;
    seed_running_import(&invalid_put, "invalid-put-import", "invalid-put.txt", None).await;
    let mut invalid_sse_c = sse_c_headers_for([17; 32]);
    invalid_sse_c.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAA=="),
    );
    let response = signed_put(
        &invalid_put,
        "invalid-put.txt",
        &[],
        b"must not be admitted".to_vec(),
        invalid_sse_c,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_import_state(&invalid_put, "invalid-put-import", "running").await;
    assert_no_kubo_calls(&invalid_put).await;

    let (copy, mut copy_block) =
        start_blocking_harness(standard_script(0), KuboBlockTarget::PinAdd).await;
    seed_latest(&copy, "copy-source.txt", "QmTask5CopySource", 11).await;
    seed_running_import(&copy, "copy-source-import", "copy-source.txt", None).await;
    seed_running_import(
        &copy,
        "copy-destination-import",
        "copy-destination.txt",
        None,
    )
    .await;
    let copy_endpoint = OwnedTestEndpoint::from(&copy);
    let copy_task = tokio::spawn(async move {
        signed_copy(
            &copy_endpoint,
            "copy-source.txt",
            "copy-destination.txt",
            HeaderMap::new(),
        )
        .await
    });
    copy_block.wait_until_blocked().await;
    assert_import_state(&copy, "copy-source-import", "running").await;
    assert_import_state(&copy, "copy-destination-import", "superseded").await;
    copy_block.release();
    let response = copy_task.await.expect("join blocked CopyObject");
    assert_eq!(response.status(), StatusCode::OK);

    let delete = start_harness(standard_script(0)).await;
    seed_latest(&delete, "present.txt", "QmTask5Delete", 7).await;
    seed_running_import(&delete, "delete-present-import", "present.txt", None).await;
    let response = signed_delete_object(&delete, "present.txt").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_import_state(&delete, "delete-present-import", "superseded").await;

    seed_running_import(&delete, "delete-absent-import", "absent.txt", None).await;
    let response = signed_delete_object(&delete, "absent.txt").await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_import_state(&delete, "delete-absent-import", "superseded").await;

    let batch = start_harness(standard_script(0)).await;
    seed_running_import(&batch, "delete-batch-a-import", "batch-a.txt", None).await;
    seed_running_import(&batch, "delete-batch-b-import", "batch-b.txt", None).await;
    let response = signed_delete_objects(&batch, &["batch-a.txt", "batch-b.txt"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_import_state(&batch, "delete-batch-a-import", "superseded").await;
    assert_import_state(&batch, "delete-batch-b-import", "superseded").await;

    let (multipart, mut multipart_block) = start_blocking_harness(
        scripted(
            &["QmTask5Part", "QmTask5Root"],
            vec![("QmTask5Part", b"multipart body".to_vec())],
        ),
        KuboBlockTarget::Cat,
    )
    .await;
    let upload_id = create_multipart(&multipart, "multipart.txt", &[]).await;
    let etag = upload_part(
        &multipart,
        "multipart.txt",
        &upload_id,
        1,
        b"multipart body".to_vec(),
    )
    .await;
    seed_running_import(
        &multipart,
        "multipart-complete-import",
        "multipart.txt",
        None,
    )
    .await;
    let multipart_endpoint = OwnedTestEndpoint::from(&multipart);
    let complete_task = tokio::spawn(async move {
        complete_multipart(
            &multipart_endpoint,
            "multipart.txt",
            &upload_id,
            &[(1, etag)],
        )
        .await
    });
    multipart_block.wait_until_blocked().await;
    assert_import_state(&multipart, "multipart-complete-import", "superseded").await;
    multipart_block.release();
    let response = complete_task
        .await
        .expect("join blocked CompleteMultipartUpload");
    assert_eq!(response.status(), StatusCode::OK);

    let invalid_complete = start_harness(scripted(&["QmTask5InvalidCompletePart"], vec![])).await;
    let invalid_upload_id = create_multipart(&invalid_complete, "invalid-complete.txt", &[]).await;
    upload_part(
        &invalid_complete,
        "invalid-complete.txt",
        &invalid_upload_id,
        1,
        b"invalid completion part".to_vec(),
    )
    .await;
    seed_running_import(
        &invalid_complete,
        "invalid-complete-import",
        "invalid-complete.txt",
        None,
    )
    .await;
    let kubo_requests_before = invalid_complete
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log before invalid completion")
        .len();
    let response = complete_multipart(
        &invalid_complete,
        "invalid-complete.txt",
        &invalid_upload_id,
        &[(1, "wrong-etag".to_owned())],
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidPart", "").await;
    assert_import_state(&invalid_complete, "invalid-complete-import", "running").await;
    let kubo_requests_after = invalid_complete
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log after invalid completion")
        .len();
    assert_eq!(kubo_requests_after, kubo_requests_before);

    let archive_body = legal_single_entry_zip();
    let (decompress, mut decompress_block) = start_blocking_harness(
        scripted(
            &["QmTask5Archive", "QmTask5Entry"],
            vec![("QmTask5Archive", archive_body.clone())],
        ),
        KuboBlockTarget::Add,
    )
    .await;
    seed_running_import(
        &decompress,
        "decompress-archive-import",
        "archive.zip",
        None,
    )
    .await;
    seed_running_import(
        &decompress,
        "decompress-prefix-import",
        "outputs/claimed.txt",
        None,
    )
    .await;
    let decompress_endpoint = OwnedTestEndpoint::from(&decompress);
    let decompress_task = tokio::spawn(async move {
        signed_decompress_zip_put(
            &decompress_endpoint,
            "archive.zip",
            "outputs/",
            archive_body,
            "",
        )
        .await
    });
    decompress_block.wait_until_blocked().await;
    assert_import_state(&decompress, "decompress-archive-import", "superseded").await;
    assert_import_state(&decompress, "decompress-prefix-import", "superseded").await;
    decompress_block.release();
    let response = decompress_task
        .await
        .expect("join blocked direct decompress PUT");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn delete_bucket_blocks_active_import_until_explicitly_superseded() {
    let delete_bucket = start_harness(standard_script(0)).await;
    seed_running_import(&delete_bucket, "delete-bucket-import", "future.txt", None).await;

    let response = signed_delete_bucket(&delete_bucket).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_import_state(&delete_bucket, "delete-bucket-import", "running").await;

    let bucket = delete_bucket.bucket.clone();
    delete_bucket
        .state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                store::import::ownership::lock_bucket_for_ownership(txn, &bucket).await?;
                store::import::ownership::supersede_bucket(txn, &bucket, Utc::now()).await?;
                Ok::<_, ipfs_s3_gateway::error::AppError>(())
            })
        })
        .await
        .unwrap();
    assert_import_state(&delete_bucket, "delete-bucket-import", "superseded").await;

    let response = signed_delete_bucket(&delete_bucket).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        !store::bucket::exists(delete_bucket.state.store.db(), &delete_bucket.bucket)
            .await
            .expect("check deleted bucket")
    );

    let nonempty_bucket = start_harness(standard_script(0)).await;
    seed_latest(
        &nonempty_bucket,
        "still-present.txt",
        "QmTask5StillPresent",
        1,
    )
    .await;
    seed_running_import(
        &nonempty_bucket,
        "nonempty-bucket-import",
        "future.txt",
        None,
    )
    .await;
    let response = signed_delete_bucket(&nonempty_bucket).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_import_state(&nonempty_bucket, "nonempty-bucket-import", "running").await;
}

#[tokio::test]
async fn delete_objects_rejects_more_than_1000_before_admission() {
    let oversized = start_harness(standard_script(0)).await;
    seed_latest(&oversized, "keep.txt", "QmTask5Keep", 4).await;
    seed_running_import(&oversized, "oversized-delete-import", "claimed.txt", None).await;
    let destination_before = import_destination(&oversized, "claimed.txt").await;

    let mut oversized_keys = vec!["keep.txt".to_owned(), "claimed.txt".to_owned()];
    oversized_keys.extend((0..999).map(|index| format!("oversized-only-{index:04}.txt")));
    assert_eq!(oversized_keys.len(), 1001);
    let oversized_refs = oversized_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let response = signed_delete_objects(&oversized, &oversized_refs, true).await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "MalformedXML", "").await;

    assert_import_state(&oversized, "oversized-delete-import", "running").await;
    let destination_after = import_destination(&oversized, "claimed.txt").await;
    assert_eq!(
        (destination_after.generation, destination_after.owner_job_id,),
        (
            destination_before.generation,
            destination_before.owner_job_id,
        )
    );
    let destinations = store::entities::import_destination::Entity::find()
        .all(oversized.state.store.db())
        .await
        .expect("list destinations after oversized DeleteObjects");
    assert_eq!(destinations.len(), 1);
    assert_eq!(destinations[0].key, "claimed.txt");
    assert_eq!(
        store::object::get_latest(oversized.state.store.db(), &oversized.bucket, "keep.txt")
            .await
            .expect("oversized DeleteObjects must retain existing object")
            .cid,
        "QmTask5Keep"
    );
    assert_no_kubo_calls(&oversized).await;

    let boundary = start_harness(standard_script(0)).await;
    let boundary_keys = (0..1000)
        .map(|index| format!("boundary-{index:04}.txt"))
        .collect::<Vec<_>>();
    let boundary_refs = boundary_keys.iter().map(String::as_str).collect::<Vec<_>>();
    let response = signed_delete_objects(&boundary, &boundary_refs, true).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::entities::import_destination::Entity::find()
            .all(boundary.state.store.db())
            .await
            .expect("list destinations after boundary DeleteObjects")
            .len(),
        1000
    );
    assert_no_kubo_calls(&boundary).await;
}

#[tokio::test]
async fn non_content_operations_do_not_supersede_import() {
    let harness = start_harness(scripted(
        &["QmTask5NonContentPart"],
        vec![("QmTask5Read", b"read body".to_vec())],
    ))
    .await;
    seed_latest(&harness, "read.txt", "QmTask5Read", 9).await;
    seed_running_import(&harness, "read-import", "read.txt", None).await;

    let get = signed_get(&harness, "read.txt").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.unwrap().as_ref(), b"read body");
    assert_import_state(&harness, "read-import", "running").await;

    let head = signed_head(&harness, "read.txt", None).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_import_state(&harness, "read-import", "running").await;

    let list = signed_list_objects(&harness).await;
    assert_eq!(list.status(), StatusCode::OK);
    assert_import_state(&harness, "read-import", "running").await;

    let put_tags = signed_put_object_tagging(&harness, "read.txt", &[("fixture", "true")]).await;
    assert_eq!(put_tags.status(), StatusCode::OK);
    let get_tags = signed_get_object_tagging(&harness, "read.txt").await;
    assert_eq!(get_tags.status(), StatusCode::OK);
    let delete_tags = signed_delete_object_tagging(&harness, "read.txt").await;
    assert_eq!(delete_tags.status(), StatusCode::NO_CONTENT);
    assert_import_state(&harness, "read-import", "running").await;

    seed_running_import(
        &harness,
        "multipart-non-content-import",
        "pending.txt",
        None,
    )
    .await;
    let upload_id = create_multipart(&harness, "pending.txt", &[]).await;
    assert_import_state(&harness, "multipart-non-content-import", "running").await;
    upload_part(
        &harness,
        "pending.txt",
        &upload_id,
        1,
        b"pending part".to_vec(),
    )
    .await;
    assert_import_state(&harness, "multipart-non-content-import", "running").await;
    let abort = abort_multipart(&harness, "pending.txt", &upload_id).await;
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert_import_state(&harness, "multipart-non-content-import", "running").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multipart_decompress_admits_prefix_before_kubo() {
    let archive_body = legal_single_entry_zip();
    let (harness, mut kubo_block) = start_blocking_harness(
        scripted(
            &["QmTask5ZipPart", "QmTask5ZipRoot", "QmTask5ZipEntry"],
            vec![
                ("QmTask5ZipPart", archive_body.clone()),
                ("QmTask5ZipRoot", archive_body.clone()),
            ],
        ),
        KuboBlockTarget::Cat,
    )
    .await;
    let upload_id =
        create_multipart(&harness, "blocked.zip", &[("decompress-zip", "outputs/")]).await;
    let etag = upload_part(&harness, "blocked.zip", &upload_id, 1, archive_body).await;
    seed_running_import(&harness, "blocked-archive-import", "blocked.zip", None).await;
    seed_running_import(
        &harness,
        "blocked-prefix-import",
        "outputs/claimed.txt",
        None,
    )
    .await;

    let endpoint = OwnedTestEndpoint::from(&harness);
    let complete_task = tokio::spawn(async move {
        complete_multipart(&endpoint, "blocked.zip", &upload_id, &[(1, etag)]).await
    });
    kubo_block.wait_until_blocked().await;

    assert_import_state(&harness, "blocked-archive-import", "superseded").await;
    assert_import_state(&harness, "blocked-prefix-import", "superseded").await;

    kubo_block.release();
    let response = complete_task
        .await
        .expect("join blocked multipart decompress completion");
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_multipart_publication_loses_to_newer_import_atomically() {
    let archive_body = legal_single_entry_zip();
    let (harness, mut kubo_block) = start_blocking_harness(
        scripted(
            &["QmTask5RacePart", "QmTask5RaceRoot", "QmTask5RaceEntry"],
            vec![
                ("QmTask5RacePart", archive_body.clone()),
                ("QmTask5RaceRoot", archive_body.clone()),
                (
                    "QmTask5NewerOutputImport",
                    b"newer import body!!!!".to_vec(),
                ),
            ],
        ),
        KuboBlockTarget::Cat,
    )
    .await;
    let upload_id = create_multipart(&harness, "race.zip", &[("decompress-zip", "outputs/")]).await;
    let etag = upload_part(&harness, "race.zip", &upload_id, 1, archive_body).await;
    seed_running_import(&harness, "race-old-output-import", "outputs/file.txt", None).await;
    let old_destination = import_destination(&harness, "outputs/file.txt").await;
    assert_eq!(
        old_destination.owner_job_id.as_deref(),
        Some("race-old-output-import")
    );

    let endpoint = OwnedTestEndpoint::from(&harness);
    let blocked_upload_id = upload_id.clone();
    let complete_task = tokio::spawn(async move {
        complete_multipart(&endpoint, "race.zip", &blocked_upload_id, &[(1, etag)]).await
    });
    kubo_block.wait_until_blocked().await;
    assert_import_state(&harness, "race-old-output-import", "superseded").await;
    let admitted_destination = import_destination(&harness, "outputs/file.txt").await;
    assert_eq!(admitted_destination.owner_job_id, None);
    assert!(
        admitted_destination.generation > old_destination.generation,
        "prefix admission must advance the old output destination generation"
    );

    let newer_claim =
        seed_running_import(&harness, "race-new-output-import", "outputs/file.txt", None).await;
    let newer_destination = import_destination(&harness, "outputs/file.txt").await;
    assert_eq!(
        newer_destination.owner_job_id.as_deref(),
        Some("race-new-output-import")
    );
    assert!(
        newer_destination.generation > admitted_destination.generation,
        "newer output import must advance the admitted destination generation"
    );

    let now = Utc::now();
    store::pinning::publication::publish_import_object(
        harness.state.store.db(),
        store::pinning::publication::PublicationRequest {
            object: store::pinning::publication::PublicationObject::from_put(
                "race-new-object".to_owned(),
                &harness.bucket,
                "outputs/file.txt",
                "QmTask5NewerOutputImport".to_owned(),
                21,
                Some("application/octet-stream".to_owned()),
                None,
                false,
                None,
                None,
                now,
            ),
            tags: Vec::new(),
            policy: ipfs_s3_gateway::pinning::policy::PublicationPolicy {
                tags: Vec::new(),
                leases: Vec::new(),
            },
            object_target: store::pinning::publication::PinTargetSpec {
                cid: "QmTask5NewerOutputImport".to_owned(),
                logical_size: 21,
            },
        },
        store::import::ownership::ImportPublicationGuard {
            job_id: newer_claim.job_id,
            worker_id: newer_claim.worker_id,
            claim_epoch: newer_claim.claim_epoch,
            targets: vec![store::import::ownership::ExpectedImportTarget {
                bucket: harness.bucket.clone(),
                key: "outputs/file.txt".to_owned(),
                generation: newer_destination.generation,
            }],
        },
        Vec::new(),
        now,
        harness.state.pinning.provider_limits(),
    )
    .await
    .expect("newer guarded import publication");

    let rows_after_import = import_side_effect_counts_for_state(&harness.state).await;
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "outputs/file.txt",
        )
        .await
        .expect("newer output import published")
        .cid,
        "QmTask5NewerOutputImport"
    );
    assert_import_state(&harness, "race-new-output-import", "completed").await;

    kubo_block.release();
    let response = complete_task
        .await
        .expect("join blocked multipart race completion");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error_xml = response
        .text()
        .await
        .expect("read stale completion response");
    assert!(
        error_xml.contains("<Code>OperationAborted</Code>"),
        "unexpected stale completion response: {error_xml}"
    );

    assert_eq!(
        import_side_effect_counts_for_state(&harness.state).await,
        rows_after_import,
        "the stale standard ZIP completion must not add object/result/lease/provider-job state"
    );
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "outputs/file.txt",
        )
        .await
        .expect("newer output import remains published")
        .cid,
        "QmTask5NewerOutputImport"
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "race.zip")
            .await
            .is_err(),
        "stale multipart archive must not become visible"
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_ok(),
        "rolled-back stale completion must retain the multipart upload for reconciliation/retry"
    );
    let get = signed_get(&harness, "outputs/file.txt").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes()
            .await
            .expect("read winning import body")
            .as_ref(),
        b"newer import body!!!!"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_direct_zip_publication_loses_to_newer_import_atomically() {
    let archive_body = legal_single_entry_zip();
    let (harness, mut kubo_block) = start_blocking_harness(
        scripted(
            &["QmDirectFenceArchive", "QmDirectFenceEntry"],
            vec![
                ("QmDirectFenceArchive", archive_body.clone()),
                ("QmDirectFenceWinner", b"direct import winner!".to_vec()),
            ],
        ),
        KuboBlockTarget::Add,
    )
    .await;
    let endpoint = OwnedTestEndpoint::from(&harness);
    let direct = tokio::spawn(async move {
        signed_decompress_zip_put(&endpoint, "direct-race.zip", "outputs/", archive_body, "").await
    });
    kubo_block.wait_until_blocked().await;

    let newer_claim =
        seed_running_import(&harness, "direct-race-import", "outputs/file.txt", None).await;
    let newer_destination = import_destination(&harness, "outputs/file.txt").await;
    let now = Utc::now();
    store::pinning::publication::publish_import_object(
        harness.state.store.db(),
        store::pinning::publication::PublicationRequest {
            object: store::pinning::publication::PublicationObject::from_put(
                "direct-race-import-object".to_owned(),
                &harness.bucket,
                "outputs/file.txt",
                "QmDirectFenceWinner".to_owned(),
                21,
                Some("application/octet-stream".to_owned()),
                None,
                false,
                None,
                None,
                now,
            ),
            tags: Vec::new(),
            policy: ipfs_s3_gateway::pinning::policy::PublicationPolicy {
                tags: Vec::new(),
                leases: Vec::new(),
            },
            object_target: store::pinning::publication::PinTargetSpec {
                cid: "QmDirectFenceWinner".to_owned(),
                logical_size: 21,
            },
        },
        store::import::ownership::ImportPublicationGuard {
            job_id: newer_claim.job_id,
            worker_id: newer_claim.worker_id,
            claim_epoch: newer_claim.claim_epoch,
            targets: vec![store::import::ownership::ExpectedImportTarget {
                bucket: harness.bucket.clone(),
                key: "outputs/file.txt".to_owned(),
                generation: newer_destination.generation,
            }],
        },
        Vec::new(),
        now,
        harness.state.pinning.provider_limits(),
    )
    .await
    .expect("publish newer direct-decompress winner");
    let rows_after_import = import_side_effect_counts_for_state(&harness.state).await;

    kubo_block.release();
    let response = direct.await.expect("join blocked direct decompress");
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error_xml = response.text().await.expect("read direct stale response");
    assert!(
        error_xml.contains("<Code>OperationAborted</Code>"),
        "unexpected stale direct-decompress response: {error_xml}"
    );
    assert_eq!(
        import_side_effect_counts_for_state(&harness.state).await,
        rows_after_import
    );
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "direct-race.zip",)
            .await
            .is_err()
    );
    let get = signed_get(&harness, "outputs/file.txt").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes().await.expect("read direct winner body").as_ref(),
        b"direct import winner!"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_cid_reports_providers_pins_and_publishes() {
    let harness = start_import_harness(ImportHarnessConfig::default()).await;
    harness.set_cat_body(IMPORT_CID, b"cid import body".to_vec());

    let accepted = post_import(
        &harness,
        &harness.bucket,
        "cid-object.txt",
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    assert_eq!(accepted.status(), StatusCode::ACCEPTED);
    let job_id = accepted.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .expect("import job header")
        .to_owned();

    let job = wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_eq!(job.final_cid.as_deref(), Some(IMPORT_CID));
    assert_eq!(job.logical_size, Some(15));
    assert_eq!(job.providers_observed, 2);
    assert_eq!(job.pin_nodes_processed, 3);
    assert_eq!(job.pin_bytes_processed, 15);

    let status = get_import_status(
        &harness,
        &harness.bucket,
        "cid-object.txt",
        &job_id,
        None,
        None,
    )
    .await;
    assert_eq!(status.status(), StatusCode::OK);
    let status_xml = String::from_utf8(status.body().clone()).expect("status XML is UTF-8");
    for expected in [
        "<State>completed</State>",
        "<ProvidersObserved>2</ProvidersObserved>",
        "<PinNodesProcessed>3</PinNodesProcessed>",
        "<PinBytesProcessed>15</PinBytesProcessed>",
        &format!("<CID>{IMPORT_CID}</CID>"),
        "<Size>15</Size>",
    ] {
        assert!(
            status_xml.contains(expected),
            "missing {expected}: {status_xml}"
        );
    }
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "cid-object.txt")
            .await
            .expect("published CID object")
            .cid,
        IMPORT_CID
    );
    assert_eq!(
        harness.kubo_args("/api/v0/routing/findprovs").await,
        vec![IMPORT_CID]
    );
    assert_eq!(harness.kubo_args("/api/v0/pin/add").await, vec![IMPORT_CID]);
    assert_eq!(harness.kubo_args("/api/v0/cat").await, vec![IMPORT_CID]);

    harness.shutdown().await;
}

fn accepted_import_job_id(response: &http::Response<Vec<u8>>) -> String {
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    response.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .expect("import job header")
        .to_owned()
}

async fn wait_for_import_pin_progress(harness: &ImportHarness, job_id: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            let job = store::entities::import_job::Entity::find_by_id(job_id)
                .one(harness.state.store.db())
                .await
                .expect("query import pin progress")
                .expect("import job exists");
            if job.pin_nodes_processed == 3 && job.pin_bytes_processed == 15 {
                return;
            }
            poll.tick().await;
        }
    })
    .await
    .expect("Kubo pin progress became durable within timeout");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ImportSideEffectCounts {
    objects: u64,
    results: u64,
    leases: u64,
    provider_jobs: u64,
    remote_pins: u64,
}

async fn import_side_effect_counts(harness: &ImportHarness) -> ImportSideEffectCounts {
    import_side_effect_counts_for_state(&harness.state).await
}

async fn import_side_effect_counts_for_state(state: &Arc<AppState>) -> ImportSideEffectCounts {
    ImportSideEffectCounts {
        objects: store::entities::object::Entity::find()
            .count(state.store.db())
            .await
            .expect("count objects"),
        results: store::entities::import_job_result::Entity::find()
            .count(state.store.db())
            .await
            .expect("count import results"),
        leases: store::entities::pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .expect("count pin leases"),
        provider_jobs: store::entities::pin_job::Entity::find()
            .count(state.store.db())
            .await
            .expect("count provider jobs"),
        remote_pins: store::entities::remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .expect("count remote pins"),
    }
}

#[derive(Debug, PartialEq)]
struct RawImportRows {
    objects: Vec<store::entities::object::Model>,
    results: Vec<store::entities::import_job_result::Model>,
    leases: Vec<store::entities::pin_lease::Model>,
    provider_jobs: Vec<store::entities::pin_job::Model>,
    remote_pins: Vec<store::entities::remote_pin::Model>,
}

async fn raw_import_rows(harness: &ImportHarness) -> RawImportRows {
    RawImportRows {
        objects: store::entities::object::Entity::find()
            .order_by_asc(store::entities::object::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load raw object rows"),
        results: store::entities::import_job_result::Entity::find()
            .order_by_asc(store::entities::import_job_result::Column::JobId)
            .order_by_asc(store::entities::import_job_result::Column::Sequence)
            .all(harness.state.store.db())
            .await
            .expect("load raw import result rows"),
        leases: store::entities::pin_lease::Entity::find()
            .order_by_asc(store::entities::pin_lease::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load raw pin lease rows"),
        provider_jobs: store::entities::pin_job::Entity::find()
            .order_by_asc(store::entities::pin_job::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load raw provider job rows"),
        remote_pins: store::entities::remote_pin::Entity::find()
            .order_by_asc(store::entities::remote_pin::Column::Provider)
            .order_by_asc(store::entities::remote_pin::Column::Cid)
            .all(harness.state.store.db())
            .await
            .expect("load raw remote pin rows"),
    }
}

#[derive(Debug, PartialEq)]
struct MutationTargetSnapshot {
    objects: Vec<store::entities::object::Model>,
    destination: Option<store::entities::import_destination::Model>,
}

async fn mutation_target_snapshot(harness: &ImportHarness, key: &str) -> MutationTargetSnapshot {
    MutationTargetSnapshot {
        objects: store::entities::object::Entity::find()
            .filter(store::entities::object::Column::Bucket.eq(&harness.bucket))
            .filter(store::entities::object::Column::Key.eq(key))
            .order_by_asc(store::entities::object::Column::CreatedAt)
            .order_by_asc(store::entities::object::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load mutation target objects"),
        destination: store::entities::import_destination::Entity::find_by_id((
            harness.bucket.clone(),
            key.to_owned(),
        ))
        .one(harness.state.store.db())
        .await
        .expect("load mutation target destination"),
    }
}

#[derive(Debug, PartialEq)]
struct PinControlRows {
    leases: Vec<store::entities::pin_lease::Model>,
    provider_jobs: Vec<store::entities::pin_job::Model>,
    remote_pins: Vec<store::entities::remote_pin::Model>,
}

async fn pin_control_rows(harness: &ImportHarness) -> PinControlRows {
    PinControlRows {
        leases: store::entities::pin_lease::Entity::find()
            .order_by_asc(store::entities::pin_lease::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load pin leases"),
        provider_jobs: store::entities::pin_job::Entity::find()
            .order_by_asc(store::entities::pin_job::Column::Id)
            .all(harness.state.store.db())
            .await
            .expect("load provider jobs"),
        remote_pins: store::entities::remote_pin::Entity::find()
            .order_by_asc(store::entities::remote_pin::Column::Provider)
            .order_by_asc(store::entities::remote_pin::Column::Cid)
            .all(harness.state.store.db())
            .await
            .expect("load remote pins"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_url_streams_unknown_length_and_enforces_limit() {
    let body = b"unknown-length-body".to_vec();
    let first_source_chunk = b"u".to_vec();
    let second_source_chunk = body[first_source_chunk.len()..].to_vec();
    let config = ImportHarnessConfig {
        streaming_kubo_add: true,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_TEST_CID_V0)],
            cat_bodies: HashMap::from([(IMPORT_TEST_CID_V0.to_owned(), body.clone())]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    let ingress = harness.kubo_file_ingress_probe();
    let source_gate = harness.source.set_chunk_gated_reply(
        "/unknown",
        vec![first_source_chunk, second_source_chunk.clone()],
    );
    let url = harness.source.url("/unknown");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "unknown.txt",
        "ipfs3-import",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    source_gate.wait_for_first_chunk().await;
    let first_kubo_bytes = ingress.wait_for_first_file_bytes().await;
    assert!(!source_gate.is_released(), "source EOF remains gated");
    assert!(
        !first_kubo_bytes.is_empty() && body.starts_with(&first_kubo_bytes),
        "Kubo saw a non-empty source prefix before source EOF: {first_kubo_bytes:?}"
    );
    assert!(
        !first_kubo_bytes
            .windows(second_source_chunk.len())
            .any(|window| window == second_source_chunk),
        "Kubo ingress must precede the gated second source chunk: {first_kubo_bytes:?}"
    );
    assert_ne!(
        first_kubo_bytes, body,
        "Kubo has not received the gated tail"
    );
    source_gate.release();
    let completed = wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_eq!(completed.downloaded_bytes, body.len() as i64);
    assert_eq!(completed.download_total, None);
    assert_eq!(completed.logical_size, Some(body.len() as i64));
    assert_eq!(harness.captured_add_file_bytes(), vec![body.clone()]);
    assert_eq!(
        harness.source.server_names(),
        vec!["downloads.example.test"],
        "real TLS handshake carries the certificate hostname as SNI"
    );
    let status = get_import_status(
        &harness,
        &harness.bucket,
        "unknown.txt",
        &job_id,
        None,
        None,
    )
    .await;
    let status_xml = String::from_utf8(status.body().clone()).expect("status XML");
    assert!(status_xml.contains(&format!(
        "<DownloadedBytes>{}</DownloadedBytes>",
        body.len()
    )));
    assert!(!status_xml.contains("DownloadTotal"));
    assert!(!status_xml.to_ascii_lowercase().contains("percent"));
    assert_signed_body(&harness, "unknown.txt", &body).await;
    harness.shutdown().await;

    let limited_config = ImportHarnessConfig {
        max_download_bytes: 5,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_TEST_CID_V0)],
            cat_bodies: HashMap::new(),
        },
        ..Default::default()
    };
    let limited = start_import_harness(limited_config).await;
    let side_effects_before = import_side_effect_counts(&limited).await;
    let kubo_before = limited.kubo_total_call_count().await;
    limited.source.set_reply(
        "/too-large",
        TestHttpsReply::chunked_chunks(vec![b"abc".to_vec(), b"def".to_vec()]),
    );
    let url = limited.source.url("/too-large");
    let accepted = post_import(
        &limited,
        &limited.bucket,
        "too-large.txt",
        "ipfs3-import",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    let failed = wait_for_import_state(&limited, &job_id, &["failed"]).await;
    assert_eq!(failed.failure_code.as_deref(), Some("source_too_large"));
    assert_eq!(failed.download_total, None);
    assert!(
        store::object::get_latest(limited.state.store.db(), &limited.bucket, "too-large.txt")
            .await
            .is_err()
    );
    assert_eq!(import_result_count(&limited, &job_id).await, 0);
    assert_eq!(
        import_side_effect_counts(&limited).await,
        side_effects_before
    );
    assert_eq!(limited.kubo_total_call_count().await, kubo_before);
    assert_eq!(limited.kubo_call_count("/api/v0/add").await, 0);
    assert_eq!(limited.kubo_call_count("/api/v0/pin/add").await, 0);
    limited.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_overwrite_keeps_previous_object_visible_until_publish() {
    let old_body = b"old-visible-body".to_vec();
    let new_body = b"new-import-body".to_vec();
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_TEST_CID_V0), AddReply::Ok(IMPORT_CID)],
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), old_body.clone()),
                (IMPORT_CID.to_owned(), new_body.clone()),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    let put = signed_put(
        &harness,
        "overwrite.txt",
        &[],
        old_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let blocked = harness
        .source
        .set_blocked_chunked_reply("/overwrite", new_body.clone());
    let url = harness.source.url("/overwrite");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "overwrite.txt",
        "ipfs3-import",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    blocked.wait_until_blocked().await;
    let running = wait_for_import_state(&harness, &job_id, &["running"]).await;
    assert_eq!(running.phase, "downloading");
    assert_signed_body(&harness, "overwrite.txt", &old_body).await;
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "overwrite.txt")
            .await
            .expect("old object remains latest")
            .cid,
        IMPORT_TEST_CID_V0
    );
    blocked.release();
    blocked.wait_until_finished().await;
    wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_signed_body(&harness, "overwrite.txt", &new_body).await;
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "overwrite.txt")
            .await
            .expect("import became latest")
            .cid,
        IMPORT_CID
    );
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_new_key_is_no_such_key_until_import_publish() {
    let body = b"eventually-visible".to_vec();
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_TEST_CID_V0)],
            cat_bodies: HashMap::from([(IMPORT_TEST_CID_V0.to_owned(), body.clone())]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    let blocked = harness
        .source
        .set_blocked_chunked_reply("/new-key", body.clone());
    let url = harness.source.url("/new-key");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "new-key.txt",
        "ipfs3-import",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    blocked.wait_until_blocked().await;
    wait_for_import_state(&harness, &job_id, &["running"]).await;
    assert_s3_error(
        signed_get(&harness, "new-key.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    assert_eq!(import_result_count(&harness, &job_id).await, 0);
    blocked.release();
    blocked.wait_until_finished().await;
    wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_signed_body(&harness, "new-key.txt", &body).await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_new_import_supersedes_blocked_old_worker() {
    let old_body = b"stale-owner-body".to_vec();
    let new_body = b"new-owner-body".to_vec();
    let config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        kubo_script: KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), old_body.clone()),
                (IMPORT_CID.to_owned(), new_body.clone()),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    let mut old_pin = harness.block_pin_add_for(IMPORT_TEST_CID_V0);
    let old = post_import(
        &harness,
        &harness.bucket,
        "race.txt",
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_TEST_CID_V0}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    let old_job_id = accepted_import_job_id(&old);
    old_pin.wait_until_blocked().await;
    wait_for_import_state(&harness, &old_job_id, &["running"]).await;
    wait_for_import_pin_progress(&harness, &old_job_id).await;
    assert!(harness.captured_add_file_bytes().is_empty());
    assert_eq!(
        harness.kubo_args("/api/v0/pin/add").await,
        vec![IMPORT_TEST_CID_V0]
    );

    let new = post_import(
        &harness,
        &harness.bucket,
        "race.txt",
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    let new_job_id = accepted_import_job_id(&new);
    let old_terminal = wait_for_import_state(&harness, &old_job_id, &["superseded"]).await;
    assert_eq!(old_terminal.final_cid, None);
    let queued = wait_for_import_state(&harness, &new_job_id, &["queued"]).await;
    assert_eq!(
        queued.attempts, 0,
        "single worker slot remains occupied by old execution"
    );
    let old_pin_calls_before_release = harness
        .kubo_args("/api/v0/pin/add")
        .await
        .into_iter()
        .filter(|cid| cid == IMPORT_TEST_CID_V0)
        .count();
    assert_eq!(old_pin_calls_before_release, 1);
    old_pin.assert_not_disconnected();
    old_pin.release();
    old_pin.wait_until_response_completed().await;

    // With one worker slot, the queued replacement can complete only after the
    // stale execution consumes final Pins, is rejected by the next claim-owned
    // Inspecting phase fence, and exits.
    wait_for_import_state(&harness, &new_job_id, &["completed"]).await;
    let old_after_slot_release =
        wait_for_import_state(&harness, &old_job_id, &["superseded"]).await;
    assert_eq!(old_after_slot_release.final_cid, None);
    assert_eq!(import_result_count(&harness, &old_job_id).await, 0);
    assert_eq!(import_result_count(&harness, &new_job_id).await, 1);
    assert!(harness.captured_add_file_bytes().is_empty());
    assert_eq!(
        harness.kubo_args("/api/v0/pin/add").await,
        vec![IMPORT_TEST_CID_V0, IMPORT_CID]
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/pin/add")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        old_pin_calls_before_release,
        "stale execution makes no late pin call after its Inspecting phase fence fails"
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/cat")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        0,
        "the stale CID execution is fenced at Inspecting before cat or publication"
    );
    let race_objects = store::entities::object::Entity::find()
        .filter(store::entities::object::Column::Bucket.eq(&harness.bucket))
        .filter(store::entities::object::Column::Key.eq("race.txt"))
        .all(harness.state.store.db())
        .await
        .expect("load race target objects");
    assert_eq!(race_objects.len(), 1);
    assert_eq!(race_objects[0].cid, IMPORT_CID);
    assert!(race_objects[0].is_latest);
    assert_eq!(
        store::entities::object::Entity::find()
            .filter(store::entities::object::Column::Cid.eq(IMPORT_TEST_CID_V0))
            .count(harness.state.store.db())
            .await
            .expect("count stale CID objects"),
        0
    );
    assert_eq!(
        pin_control_rows(&harness).await,
        PinControlRows {
            leases: Vec::new(),
            provider_jobs: Vec::new(),
            remote_pins: Vec::new(),
        }
    );
    let race_destination = store::entities::import_destination::Entity::find_by_id((
        harness.bucket.clone(),
        "race.txt".to_owned(),
    ))
    .one(harness.state.store.db())
    .await
    .expect("load race destination")
    .expect("race destination exists");
    assert_ne!(
        race_destination.owner_job_id.as_deref(),
        Some(old_job_id.as_str())
    );
    assert_signed_body(&harness, "race.txt", &new_body).await;
    harness.shutdown().await;
}

async fn submit_pin_blocked_import(
    harness: &ImportHarness,
    key: &str,
) -> (String, support::import::KuboPinBlockControl) {
    let mut pin_gate = harness.block_pin_add_for(IMPORT_TEST_CID_V0);
    let accepted = post_import(
        harness,
        &harness.bucket,
        key,
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_TEST_CID_V0}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    pin_gate.wait_until_blocked().await;
    wait_for_import_state(harness, &job_id, &["running"]).await;
    wait_for_import_pin_progress(harness, &job_id).await;
    assert_eq!(
        harness.kubo_args("/api/v0/pin/add").await.last(),
        Some(&IMPORT_TEST_CID_V0.to_owned())
    );
    (job_id, pin_gate)
}

async fn wait_for_worker_slot_with_probe(harness: &ImportHarness, probe_key: &str) -> String {
    let accepted = post_import(
        harness,
        &harness.bucket,
        probe_key,
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    let probe_job_id = accepted_import_job_id(&accepted);
    let completed = wait_for_import_state(harness, &probe_job_id, &["completed"]).await;
    assert_eq!(completed.final_cid.as_deref(), Some(IMPORT_CID));
    probe_job_id
}

async fn assert_mutation_canceled_blocked_import(
    harness: &ImportHarness,
    job_id: &str,
    key: &str,
    probe_key: &str,
    blocked_pin: &mut support::import::KuboPinBlockControl,
) {
    let superseded = wait_for_import_state(harness, job_id, &["superseded"]).await;
    assert_eq!(superseded.final_cid, None);
    assert_eq!(import_result_count(harness, job_id).await, 0);
    let target_after_mutation = mutation_target_snapshot(harness, key).await;
    let destination_after_mutation = target_after_mutation.destination.clone();
    assert_ne!(
        destination_after_mutation
            .as_ref()
            .and_then(|destination| destination.owner_job_id.as_deref()),
        Some(job_id)
    );
    let old_targets = store::entities::import_job_target::Entity::find()
        .filter(store::entities::import_job_target::Column::JobId.eq(job_id))
        .order_by_asc(store::entities::import_job_target::Column::Key)
        .all(harness.state.store.db())
        .await
        .expect("load stale import targets");
    let pin_rows_after_mutation = pin_control_rows(harness).await;
    assert_eq!(
        pin_rows_after_mutation,
        PinControlRows {
            leases: Vec::new(),
            provider_jobs: Vec::new(),
            remote_pins: Vec::new(),
        }
    );
    let old_pin_calls = harness
        .kubo_args("/api/v0/pin/add")
        .await
        .into_iter()
        .filter(|cid| cid == IMPORT_TEST_CID_V0)
        .count();
    assert_eq!(old_pin_calls, 1);
    assert_eq!(
        harness
            .kubo_args("/api/v0/cat")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        0
    );

    blocked_pin.assert_not_disconnected();
    blocked_pin.release();
    blocked_pin.wait_until_response_completed().await;

    // The CID probe uses another CID and can complete with worker_concurrency=1
    // only after the stale execution consumes final Pins, is rejected by the
    // Inspecting phase fence, and releases its worker slot.
    let probe_job_id = wait_for_worker_slot_with_probe(harness, probe_key).await;
    assert_eq!(import_result_count(harness, &probe_job_id).await, 1);
    let stale_after_slot_release = wait_for_import_state(harness, job_id, &["superseded"]).await;
    assert_eq!(stale_after_slot_release.final_cid, None);
    assert_eq!(
        mutation_target_snapshot(harness, key).await,
        target_after_mutation
    );
    assert_eq!(
        store::entities::import_job_target::Entity::find()
            .filter(store::entities::import_job_target::Column::JobId.eq(job_id))
            .order_by_asc(store::entities::import_job_target::Column::Key)
            .all(harness.state.store.db())
            .await
            .expect("reload stale import targets"),
        old_targets
    );
    assert_eq!(pin_control_rows(harness).await, pin_rows_after_mutation);
    assert_eq!(import_result_count(harness, job_id).await, 0);
    assert_eq!(
        harness
            .kubo_args("/api/v0/pin/add")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        old_pin_calls
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/cat")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        0,
        "stale CID import is fenced at Inspecting before cat or publication"
    );
    assert_eq!(
        store::entities::object::Entity::find()
            .filter(store::entities::object::Column::Cid.eq(IMPORT_TEST_CID_V0))
            .count(harness.state.store.db())
            .await
            .expect("count stale CID objects"),
        0,
        "stale import never publishes its source CID"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_put_copy_delete_and_complete_supersede_blocked_import() {
    // PutObject admission supersedes the CID import before final Pins arrive.
    let put_body = b"put-wins".to_vec();
    let put_config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_CID)],
            cat_bodies: HashMap::from([(IMPORT_CID.to_owned(), put_body.clone())]),
        },
        ..Default::default()
    };
    let put_harness = start_import_harness(put_config).await;
    let (put_job, mut put_blocked) = submit_pin_blocked_import(&put_harness, "put-race.txt").await;
    let put = signed_put(
        &put_harness,
        "put-race.txt",
        &[],
        put_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_mutation_canceled_blocked_import(
        &put_harness,
        &put_job,
        "put-race.txt",
        "probe-put.txt",
        &mut put_blocked,
    )
    .await;
    assert_eq!(
        put_harness.captured_add_file_bytes(),
        vec![put_body.clone()]
    );
    assert_signed_body(&put_harness, "put-race.txt", &put_body).await;
    put_harness.shutdown().await;

    // CopyObject copies the committed source mapping over the stale CID import.
    let source_body = b"copy-source".to_vec();
    let copy_config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_CID)],
            cat_bodies: HashMap::from([(IMPORT_CID.to_owned(), source_body.clone())]),
        },
        ..Default::default()
    };
    let copy_harness = start_import_harness(copy_config).await;
    let source_put = signed_put(
        &copy_harness,
        "copy-source.txt",
        &[],
        source_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(source_put.status(), StatusCode::OK);
    let (copy_job, mut copy_blocked) =
        submit_pin_blocked_import(&copy_harness, "copy-destination.txt").await;
    let copy = signed_copy(
        &copy_harness,
        "copy-source.txt",
        "copy-destination.txt",
        HeaderMap::new(),
    )
    .await;
    assert_eq!(copy.status(), StatusCode::OK);
    assert_mutation_canceled_blocked_import(
        &copy_harness,
        &copy_job,
        "copy-destination.txt",
        "probe-copy.txt",
        &mut copy_blocked,
    )
    .await;
    assert_eq!(
        copy_harness.captured_add_file_bytes(),
        vec![source_body.clone()]
    );
    assert_signed_body(&copy_harness, "copy-destination.txt", &source_body).await;
    copy_harness.shutdown().await;

    // DeleteObject leaves no latest object and cannot be undone by the stale worker.
    let deleted_body = b"delete-existing".to_vec();
    let delete_config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_CID)],
            cat_bodies: HashMap::from([(IMPORT_CID.to_owned(), deleted_body.clone())]),
        },
        ..Default::default()
    };
    let delete_harness = start_import_harness(delete_config).await;
    let existing = signed_put(
        &delete_harness,
        "delete-race.txt",
        &[],
        deleted_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(existing.status(), StatusCode::OK);
    let (delete_job, mut delete_blocked) =
        submit_pin_blocked_import(&delete_harness, "delete-race.txt").await;
    let deleted = signed_delete_object(&delete_harness, "delete-race.txt").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_mutation_canceled_blocked_import(
        &delete_harness,
        &delete_job,
        "delete-race.txt",
        "probe-delete.txt",
        &mut delete_blocked,
    )
    .await;
    assert_eq!(
        delete_harness.captured_add_file_bytes(),
        vec![deleted_body.clone()]
    );
    assert!(
        store::object::get_latest(
            delete_harness.state.store.db(),
            &delete_harness.bucket,
            "delete-race.txt",
        )
        .await
        .is_err()
    );
    delete_harness.shutdown().await;

    // CompleteMultipartUpload is the content mutation; create/upload alone do not supersede.
    let part_body = b"multipart-wins".to_vec();
    let complete_config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        kubo_script: KuboScript {
            add_replies: vec![AddReply::Ok(IMPORT_CID), AddReply::Ok(IMPORT_CID)],
            cat_bodies: HashMap::from([(IMPORT_CID.to_owned(), part_body.clone())]),
        },
        ..Default::default()
    };
    let complete_harness = start_import_harness(complete_config).await;
    let create =
        signed_create_multipart_upload_with_tagging(&complete_harness, "complete-race.txt", "")
            .await;
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = create.text().await.expect("CreateMultipartUpload XML");
    let upload_id = xml_element_text(&create_xml, "UploadId");
    let part = signed_upload_part(
        &complete_harness,
        "complete-race.txt",
        &upload_id,
        1,
        part_body.clone(),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part.headers()[http::header::ETAG]
        .to_str()
        .expect("part ETag")
        .trim_matches('"')
        .to_owned();
    let (complete_job, mut complete_blocked) =
        submit_pin_blocked_import(&complete_harness, "complete-race.txt").await;
    let complete =
        signed_complete_multipart(&complete_harness, "complete-race.txt", &upload_id, 1, &etag)
            .await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_mutation_canceled_blocked_import(
        &complete_harness,
        &complete_job,
        "complete-race.txt",
        "probe-complete.txt",
        &mut complete_blocked,
    )
    .await;
    assert_eq!(
        complete_harness.captured_add_file_bytes(),
        vec![part_body.clone(), part_body.clone()]
    );
    assert_eq!(
        store::object::get_latest(
            complete_harness.state.store.db(),
            &complete_harness.bucket,
            "complete-race.txt",
        )
        .await
        .expect("completed multipart is latest")
        .cid,
        IMPORT_CID
    );
    assert_signed_body(&complete_harness, "complete-race.txt", &part_body).await;
    complete_harness.shutdown().await;
}

fn optional_xml_element_text(xml: &str, name: &str) -> Option<String> {
    xml.contains(&format!("<{name}>"))
        .then(|| xml_element_text(xml, name))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_combined_import_zip_publishes_once_and_pages_results() {
    let archive = legal_two_entry_zip();
    let mut config = ImportHarnessConfig::default();
    let publication_gate = ImportPublicationBlockControl::new();
    config.execution_observer = Some(Arc::new(publication_gate.clone()));
    config.streaming_kubo_add = true;
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok(IMPORT_TEST_CID_V0),
            AddReply::Ok(IMPORT_CID),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "scripted entry failure"),
        ],
        cat_bodies: HashMap::from([
            (IMPORT_TEST_CID_V0.to_owned(), archive.clone()),
            (IMPORT_CID.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
        ]),
    };
    let harness = start_import_harness(config).await;
    harness
        .source
        .set_reply("/archive.zip", TestHttpsReply::chunked(archive.clone()));
    let url = harness.source.url("/archive.zip");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "archive.zip",
        "ipfs3-import&decompress-zip=outputs",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    publication_gate.wait_until_blocked(&job_id).await;
    let running = wait_for_import_state(&harness, &job_id, &["running"]).await;
    assert_eq!(running.phase, "decompressing");
    let unpublished_rows = raw_import_rows(&harness).await;
    assert!(unpublished_rows.objects.is_empty());
    assert!(unpublished_rows.results.is_empty());
    assert!(unpublished_rows.leases.is_empty());
    assert!(unpublished_rows.provider_jobs.is_empty());
    assert!(unpublished_rows.remote_pins.is_empty());
    assert_s3_error(
        signed_get(&harness, "archive.zip").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    assert_s3_error(
        signed_get(&harness, "outputs/first.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    let unpublished_list = signed_list_objects(&harness)
        .await
        .text()
        .await
        .expect("unpublished ListObjects body");
    assert!(!unpublished_list.contains("archive.zip"));
    assert!(!unpublished_list.contains("outputs/first.txt"));
    assert_eq!(
        harness.captured_add_file_bytes(),
        vec![
            archive.clone(),
            FIRST_ENTRY_BYTES.to_vec(),
            SECOND_ENTRY_BYTES.to_vec(),
        ]
    );
    assert_eq!(
        harness.kubo_args("/api/v0/pin/add").await,
        vec![IMPORT_TEST_CID_V0, IMPORT_CID]
    );
    publication_gate.release();
    let completed = wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_eq!(completed.entries_processed, 2);
    assert_eq!(completed.entries_succeeded, 1);
    assert_eq!(completed.entries_failed, 1);
    assert_eq!(import_result_count(&harness, &job_id).await, 3);
    let published_rows = raw_import_rows(&harness).await;
    assert_eq!(published_rows.objects.len(), 2);
    assert_eq!(published_rows.results.len(), 3);
    assert!(published_rows.leases.is_empty());
    assert!(published_rows.provider_jobs.is_empty());
    assert!(published_rows.remote_pins.is_empty());
    let latest = store::object::list(harness.state.store.db(), &harness.bucket, None, None, 100)
        .await
        .expect("list atomic ZIP publication");
    assert_eq!(
        latest
            .iter()
            .map(|row| row.key.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["archive.zip", "outputs/first.txt"])
    );
    assert_eq!(
        latest.len(),
        2,
        "archive and successful entry publish exactly once"
    );
    assert!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "outputs/second.txt",
        )
        .await
        .is_err(),
        "failed entry is not published"
    );
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "outputs/first.txt", FIRST_ENTRY_BYTES).await;
    assert_s3_error(
        signed_get(&harness, "outputs/second.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;

    let mut token = None;
    let mut pages = Vec::new();
    for _ in 0..4 {
        let response = get_import_status(
            &harness,
            &harness.bucket,
            "archive.zip",
            &job_id,
            Some(1),
            token.as_deref(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let xml = String::from_utf8(response.body().clone()).expect("status page XML");
        token = optional_xml_element_text(&xml, "NextContinuationToken");
        pages.push(xml);
        if token.is_none() {
            break;
        }
    }
    assert_eq!(pages.len(), 3, "three one-row result pages");
    let all_pages = pages.join("");
    for key in ["archive.zip", "outputs/first.txt", "outputs/second.txt"] {
        assert_eq!(
            all_pages.matches(&format!("<Key>{key}</Key>")).count(),
            1,
            "{key}"
        );
    }
    assert_eq!(all_pages.matches("<Status>success</Status>").count(), 2);
    assert_eq!(all_pages.matches("<Status>failure</Status>").count(), 1);
    assert_eq!(
        all_pages
            .matches("<ErrorCode>EntryUploadFailed</ErrorCode>")
            .count(),
        1
    );
    assert_eq!(harness.kubo_call_count("/api/v0/add").await, 3);
    assert_eq!(harness.kubo_args("/api/v0/pin/add").await.len(), 2);
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_combined_import_zip_fatal_error_preserves_previous_objects() {
    let old_body = b"old preserved object".to_vec();
    let invalid_archive = b"not a zip archive".to_vec();
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: vec![
                AddReply::Ok(IMPORT_TEST_CID_V0),
                AddReply::Ok(IMPORT_TEST_CID_V0),
                AddReply::Ok(IMPORT_CID),
            ],
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), old_body.clone()),
                (IMPORT_CID.to_owned(), invalid_archive.clone()),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    assert_eq!(
        signed_put(
            &harness,
            "fatal.zip",
            &[],
            old_body.clone(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &harness,
            "fatal/file.txt",
            &[],
            old_body.clone(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_signed_body(&harness, "fatal.zip", &old_body).await;
    assert_signed_body(&harness, "fatal/file.txt", &old_body).await;
    let archive_before =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "fatal.zip")
            .await
            .expect("old archive");
    let output_before =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "fatal/file.txt")
            .await
            .expect("old output");
    let counts_before = import_side_effect_counts(&harness).await;
    harness.source.set_reply(
        "/fatal.zip",
        TestHttpsReply::chunked(invalid_archive.clone()),
    );
    let url = harness.source.url("/fatal.zip");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "fatal.zip",
        "ipfs3-import&decompress-zip=fatal",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    let failed = wait_for_import_state(&harness, &job_id, &["failed"]).await;
    assert_eq!(failed.failure_code.as_deref(), Some("invalid_archive"));
    assert_eq!(import_result_count(&harness, &job_id).await, 0);
    assert_eq!(import_side_effect_counts(&harness).await, counts_before);
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "fatal.zip")
            .await
            .expect("old archive preserved"),
        archive_before
    );
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "fatal/file.txt",)
            .await
            .expect("old output preserved"),
        output_before
    );
    assert_signed_body(&harness, "fatal.zip", &old_body).await;
    assert_signed_body(&harness, "fatal/file.txt", &old_body).await;
    assert_eq!(
        harness.captured_add_file_bytes(),
        vec![old_body.clone(), old_body, invalid_archive]
    );
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_redirect_and_forbidden_dns_fail_without_fetch_escape() {
    let strict = start_strict_import_harness().await;
    let forbidden = post_import(
        &strict,
        &strict.bucket,
        "private.txt",
        "ipfs3-import",
        &import_url_xml("https://downloads.example.test/private"),
        None,
    )
    .await;
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    let forbidden_xml =
        String::from_utf8(forbidden.body().clone()).expect("strict gateway error XML");
    assert!(
        forbidden_xml.contains("<Code>AccessDenied</Code>"),
        "stable forbidden DNS mapping: {forbidden_xml}"
    );
    assert_eq!(strict.transport_calls(), 0);
    assert_eq!(strict.kubo_total_call_count(), 0);
    assert!(
        strict
            .kubo
            .received_requests()
            .await
            .expect("strict Kubo request log")
            .is_empty()
    );
    assert_eq!(
        store::entities::import_job::Entity::find()
            .count(strict.state.store.db())
            .await
            .expect("strict import job count"),
        0
    );
    assert_eq!(
        store::entities::import_destination::Entity::find()
            .count(strict.state.store.db())
            .await
            .expect("strict destination count"),
        0
    );
    assert_eq!(
        import_side_effect_counts_for_state(&strict.state).await,
        ImportSideEffectCounts {
            objects: 0,
            results: 0,
            leases: 0,
            provider_jobs: 0,
            remote_pins: 0,
        }
    );
    strict.shutdown().await;

    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::new(),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    harness.source.set_reply(
        "/redirect",
        TestHttpsReply::redirect(&harness.source.url("/escape")),
    );
    harness.source.set_reply(
        "/escape",
        TestHttpsReply::chunked(b"must not fetch".to_vec()),
    );
    let url = harness.source.url("/redirect");
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "redirect.txt",
        "ipfs3-import",
        &import_url_xml(&url),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    let failed = wait_for_import_state(&harness, &job_id, &["failed"]).await;
    assert_eq!(failed.failure_code.as_deref(), Some("source_redirected"));
    assert_eq!(harness.source.requests(), vec!["/redirect"]);
    assert!(
        harness
            .kubo
            .received_requests()
            .await
            .expect("Kubo log")
            .is_empty()
    );
    assert_eq!(import_result_count(&harness, &job_id).await, 0);
    assert_eq!(import_side_effect_counts(&harness).await.objects, 0);
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_import_worker_restart_reclaims_without_double_publication() {
    let body = b"restart-body".to_vec();
    let probe_body = b"restart-probe-body".to_vec();
    let config = ImportHarnessConfig {
        streaming_kubo_add: true,
        worker_concurrency: 1,
        lease_duration_secs: 20,
        max_attempts: 2,
        kubo_script: KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), body),
                (IMPORT_CID.to_owned(), probe_body),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    let mut epoch1_pin = harness.block_pin_add_for(IMPORT_TEST_CID_V0);
    let accepted = post_import(
        &harness,
        &harness.bucket,
        "restart.txt",
        "ipfs3-import",
        &format!("<IPFS3ImportRequest><CID>{IMPORT_TEST_CID_V0}</CID></IPFS3ImportRequest>"),
        None,
    )
    .await;
    let job_id = accepted_import_job_id(&accepted);
    epoch1_pin.wait_until_blocked().await;
    let first_claim = wait_for_import_state(&harness, &job_id, &["running"]).await;
    assert_eq!(first_claim.attempts, 1);
    assert_eq!(first_claim.claim_epoch, 1);
    wait_for_import_pin_progress(&harness, &job_id).await;

    let second_worker = harness.start_additional_worker();
    use sea_orm::sea_query::Expr;
    use store::entities::import_job;
    let now = Utc::now();
    let expired = import_job::Entity::update_many()
        .col_expr(
            import_job::Column::LockedUntil,
            Expr::value(Some(now - ChronoDuration::seconds(1))),
        )
        .col_expr(
            import_job::Column::NextAttemptAt,
            Expr::value(now - ChronoDuration::seconds(1)),
        )
        .col_expr(import_job::Column::UpdatedAt, Expr::value(now))
        .filter(import_job::Column::Id.eq(&job_id))
        .exec(harness.state.store.db())
        .await
        .expect("expire epoch-one worker lease");
    assert_eq!(expired.rows_affected, 1);

    let completed = wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_eq!(completed.attempts, 2);
    assert_eq!(completed.claim_epoch, 2);
    assert_eq!(
        harness.kubo_args("/api/v0/pin/add").await,
        vec![IMPORT_TEST_CID_V0, IMPORT_TEST_CID_V0]
    );
    assert_eq!(
        harness.kubo_args("/api/v0/cat").await,
        vec![IMPORT_TEST_CID_V0]
    );
    assert!(harness.captured_add_file_bytes().is_empty());
    assert_eq!(import_result_count(&harness, &job_id).await, 1);
    let restart_target_before = mutation_target_snapshot(&harness, "restart.txt").await;
    assert_eq!(restart_target_before.objects.len(), 1);
    assert_eq!(restart_target_before.objects[0].cid, IMPORT_TEST_CID_V0);
    let restart_results_before = store::entities::import_job_result::Entity::find()
        .filter(store::entities::import_job_result::Column::JobId.eq(&job_id))
        .order_by_asc(store::entities::import_job_result::Column::Sequence)
        .all(harness.state.store.db())
        .await
        .expect("load epoch-two import results");
    assert_eq!(restart_results_before.len(), 1);
    let pin_rows_before = pin_control_rows(&harness).await;
    assert_eq!(
        pin_rows_before,
        PinControlRows {
            leases: Vec::new(),
            provider_jobs: Vec::new(),
            remote_pins: Vec::new(),
        }
    );

    // Stop epoch two so the distinct probe can only be claimed by the original
    // worker after its stale epoch-one execution leaves the single slot.
    second_worker
        .shutdown(std::time::Duration::from_secs(2))
        .await;
    epoch1_pin.assert_not_disconnected();
    epoch1_pin.release();
    epoch1_pin.wait_until_response_completed().await;
    let probe_job_id = wait_for_worker_slot_with_probe(&harness, "restart-probe.txt").await;
    assert_eq!(import_result_count(&harness, &probe_job_id).await, 1);

    let after_epoch1 = wait_for_import_state(&harness, &job_id, &["completed"]).await;
    assert_eq!(after_epoch1.attempts, 2);
    assert_eq!(after_epoch1.claim_epoch, 2);
    assert_eq!(
        mutation_target_snapshot(&harness, "restart.txt").await,
        restart_target_before,
        "epoch one cannot change the epoch-two publication"
    );
    assert_eq!(
        store::entities::import_job_result::Entity::find()
            .filter(store::entities::import_job_result::Column::JobId.eq(&job_id))
            .order_by_asc(store::entities::import_job_result::Column::Sequence)
            .all(harness.state.store.db())
            .await
            .expect("reload epoch-two import results"),
        restart_results_before
    );
    assert_eq!(pin_control_rows(&harness).await, pin_rows_before);
    assert_eq!(
        harness
            .kubo_args("/api/v0/pin/add")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        2,
        "both epochs may repeat source pin work"
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/cat")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_TEST_CID_V0)
            .count(),
        1,
        "only epoch two passes Inspecting; stale epoch one is fenced before cat"
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/pin/add")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_CID)
            .count(),
        1
    );
    assert_eq!(
        harness
            .kubo_args("/api/v0/cat")
            .await
            .into_iter()
            .filter(|cid| cid == IMPORT_CID)
            .count(),
        1
    );
    harness.shutdown().await;
}
