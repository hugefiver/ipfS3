use super::*;
#[path = "sql_trace.rs"]
mod sql_trace;
use sql_trace::SqlTrace;

#[tokio::test]
async fn sqlite_s3_worker_warm_paths_do_not_revalidate_structure() {
    let mut db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
    let trace = SqlTrace::default();
    trace.attach(&mut db);
    exercise(db, trace).await;
}

#[tokio::test]
#[ignore = "requires an authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_s3_worker_warm_paths_do_not_revalidate_structure() {
    use futures_util::FutureExt;
    use sea_orm::ConnectionTrait;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").expect("explicit test PostgreSQL URL");
    let admin = sea_orm::Database::connect(&url).await.unwrap();
    let schema = format!("optimistic_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .unwrap();
    let mut options = sea_orm::ConnectOptions::new(url);
    options
        .max_connections(4)
        .set_schema_search_path(schema.clone());
    let mut db = sea_orm::Database::connect(options).await.unwrap();
    let trace = SqlTrace::default();
    trace.attach(&mut db);
    let pool = db.clone();
    let outcome = std::panic::AssertUnwindSafe(exercise(db, trace))
        .catch_unwind()
        .await;
    pool.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .await
        .unwrap();
    admin.close().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn exercise(db: sea_orm::DatabaseConnection, trace: SqlTrace) {
    let mut config = PinningHarnessConfig::request_one();
    let archive = legal_two_entry_zip();
    config.kubo_script.add_replies = vec![
        AddReply::Ok(TEST_CID),
        AddReply::Ok(TEST_CID),
        AddReply::Ok(PIN_CID_PART),
        AddReply::Ok(PIN_CID_ROOT),
        AddReply::Ok(PIN_CID_ENTRY1),
        AddReply::Ok(PIN_CID_ENTRY2),
    ];
    config.kubo_script.cat_bodies.extend([
        (PIN_CID_PART.to_owned(), archive.clone()),
        (PIN_CID_ROOT.to_owned(), archive.clone()),
        (PIN_CID_ENTRY1.to_owned(), FIRST_ENTRY_BYTES.to_vec()),
        (PIN_CID_ENTRY2.to_owned(), SECOND_ENTRY_BYTES.to_vec()),
    ]);
    let harness = support::pinning::start_pinning_harness_with_database(config, db).await;
    trace.take("cold initialization", false);
    let put = signed_put_with_tagging(&harness, "warm.txt", b"body".to_vec(), "").await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, TEST_CID);
    trace.take("first PUT after initialization", true);
    let put =
        signed_put_with_tagging(&harness, "warm.txt", b"body".to_vec(), "ipfs-s3%3Apin=true").await;
    assert_eq!(put.status(), StatusCode::OK);
    trace.take("warm PUT overwrite", true);
    harness.run_until_resource_status("accepted").await;
    trace.take("warm worker submit and settlement", true);
    let requests = harness.pinata.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method.as_str(), "POST");
    let get = signed_get(&harness, "warm.txt").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.unwrap().as_ref(), b"body");
    let object = latest_pinning_object(&harness, "warm.txt").await;
    let db = harness.state.store.db();
    let manual = support::pin_rows::refs(db).await.remove(0);
    assert_eq!(manual.owner_object_id, object.id);
    let expires = chrono::DateTime::from_timestamp_micros(manual.expires_at).unwrap();
    let retain_until = (expires + ChronoDuration::hours(1)).to_rfc3339();
    let response = signed_put_object_tagging(
        &harness,
        "warm.txt",
        &[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", &retain_until),
        ],
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let renewed = support::pin_rows::refs(db).await;
    assert_eq!(renewed.len(), 1);
    assert!(renewed[0].expires_at > manual.expires_at);
    trace.take("warm tagging renewal", true);
    assert_eq!(
        signed_copy_with_tagging(&harness, "warm.txt", "copy.txt", "ipfs-s3%3Apin=true")
            .await
            .status(),
        StatusCode::OK
    );
    trace.take("warm CopyObject", true);
    assert_eq!(
        signed_delete_object(&harness, "copy.txt").await.status(),
        StatusCode::NO_CONTENT
    );
    trace.take("warm DELETE reference release", true);
    assert_eq!(
        signed_delete_object_tagging(&harness, "warm.txt")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    trace.take("warm tagging cancellation", true);
    let create = signed_create_multipart_zip_upload_with_tagging(
        &harness,
        "observed.zip",
        "observed-entries/",
        "ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
    )
    .await;
    let status = create.status();
    let created = create.text().await.unwrap();
    assert_eq!(status, StatusCode::OK, "{created}");
    let upload_id = xml_text(&created, "UploadId").unwrap();
    let part = signed_upload_part(&harness, "observed.zip", &upload_id, 1, archive).await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .trim_matches('"')
        .to_owned();
    let complete = signed_complete_multipart(&harness, "observed.zip", &upload_id, 1, &etag).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let completed = complete.text().await.unwrap();
    assert_eq!(
        xml_text(&completed, "ArchiveKey").as_deref(),
        Some("observed.zip")
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    trace.take("warm multipart ZIP publication", true);
    harness.shutdown().await;
}
