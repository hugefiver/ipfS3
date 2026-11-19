//! Signed ZIP v2 Abort races the same durable authority used by Complete.
#[path = "support/baseline_postgres.rs"]
mod baseline_postgres;
#[allow(dead_code)]
mod support;

use std::{collections::BTreeSet, time::Duration};

use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    error::AppError,
    import::SupersedeReason,
    store::{
        self,
        entities::{import_destination, standard_mutation_lease, zip_root_build},
        import::ownership::{
            admit_content_mutation, admit_zip_v2_source_in_transaction, lock_bucket_for_ownership,
            renew_standard_mutation,
        },
        zip,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    Statement, TransactionTrait, sea_query::Expr,
};
use support::decompress::{
    AddReply, KuboBlockTarget, KuboScript, TestHarness, abort_multipart, block_next_kubo_request,
    complete_multipart, create_multipart_with_headers, legal_single_entry_zip,
    start_harness_with_root_default_and_database, upload_part,
};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

const SOURCE: &str = "archive.zip";
const OUTPUT: &str = "out/file.txt";
const OLD_TOKEN: &str = "abort-original";
const NEW_TOKEN: &str = "abort-successor";
const LEAF: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

fn controls(token: &str, root: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("x-ipfs3-zip-contract", "v2"),
        ("x-ipfs3-zip-publish-source", "true"),
        ("x-ipfs3-zip-publish-extracted", "true"),
        ("x-ipfs3-zip-targets", "none"),
        ("x-ipfs3-zip-token", token),
        (
            "x-amz-tagging",
            if root {
                "ipfs-s3%3Azip-root=true"
            } else {
                "ipfs-s3%3Azip-root=false"
            },
        ),
    ] {
        headers.insert(name, HeaderValue::from_str(value).unwrap());
    }
    headers
}

async fn setup(database_url: &str) -> TestHarness {
    let archive = legal_single_entry_zip();
    start_harness_with_root_default_and_database(
        KuboScript {
            add_replies: [
                "QmPart",
                "QmArchive",
                "QmOutput",
                "QmOtherPart",
                "QmOtherArchive",
                LEAF,
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: ["QmPart", "QmArchive", "QmOtherPart", "QmOtherArchive"]
                .into_iter()
                .map(|cid| (cid.to_owned(), archive.clone()))
                .collect(),
        },
        false,
        database_url,
    )
    .await
}

async fn start_upload(harness: &TestHarness, token: &str) -> (String, [(i32, String); 1]) {
    let id = create_multipart_with_headers(
        harness,
        SOURCE,
        &[("decompress-zip", "out/")],
        controls(token, false),
    )
    .await;
    let etag = upload_part(harness, SOURCE, &id, 1, legal_single_entry_zip()).await;
    (id, [(1, etag)])
}

async fn guard_at(
    harness: &TestHarness,
    key: &str,
) -> (
    Option<import_destination::Model>,
    Option<standard_mutation_lease::Model>,
) {
    let db = harness.state.store.db();
    let id = (harness.bucket.clone(), key.to_owned());
    (
        import_destination::Entity::find_by_id(id.clone())
            .one(db)
            .await
            .unwrap(),
        standard_mutation_lease::Entity::find_by_id(id)
            .one(db)
            .await
            .unwrap(),
    )
}

async fn prepare_independent_root(harness: &TestHarness) -> zip::BatchSnapshot {
    let key = "independent.zip";
    let id = create_multipart_with_headers(
        harness,
        key,
        &[("decompress-zip", "independent/")],
        controls("independent-root", true),
    )
    .await;
    let etag = upload_part(harness, key, &id, 1, legal_single_entry_zip()).await;
    for (endpoint, status, body) in [
        ("/api/v0/id", 200, serde_json::json!({"ID":"localNode"})),
        (
            "/api/v0/files/stat",
            200,
            serde_json::json!({"Hash":LEAF,"CumulativeSize":0}),
        ),
        (
            "/api/v0/dag/put",
            503,
            serde_json::json!({"Message":"temporary root outage"}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path(endpoint))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&harness.kubo)
            .await;
    }
    let response = complete_multipart(harness, key, &id, &[(1, etag)]).await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let db = harness.state.store.db();
    let changed = zip_root_build::Entity::update_many()
        .col_expr(
            zip_root_build::Column::LeaseUntil,
            Expr::value(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH),
        )
        .filter(zip_root_build::Column::BatchId.eq(&id))
        .exec(db)
        .await
        .unwrap();
    assert_eq!(changed.rows_affected, 1);
    let snapshot = zip::snapshot(db, &id).await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.batch.root_status, "failed");
    assert_eq!(
        snapshot.batch.root_error_code.as_deref(),
        Some("directory_build_failed")
    );
    assert_eq!(zip::recovery::due_page(db).await.unwrap(), vec![id]);
    snapshot
}

async fn assert_aborted(harness: &TestHarness, id: &str) {
    let db = harness.state.store.db();
    assert!(matches!(
        store::multipart::get_upload(db, id).await,
        Err(AppError::NoSuchUpload(_))
    ));
    assert!(
        store::multipart::list_parts(db, id)
            .await
            .unwrap()
            .is_empty()
    );
    let intake = store::multipart::v2_zip::read_by_upload(db, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intake.active_upload_id, None);
    assert_eq!(intake.token, OLD_TOKEN);
    let execution = zip::execution::read(db, id).await.unwrap().unwrap();
    assert_eq!(execution.state, "fenced");
    assert_eq!(execution.terminal_result.as_deref(), Some("fenced"));
    assert!(
        zip::execution::claim(db, id, "late-worker", 30)
            .await
            .unwrap()
            .is_none()
    );
    for key in [SOURCE, OUTPUT] {
        assert!(matches!(
            store::object::get_latest(db, &harness.bucket, key).await,
            Err(AppError::NoSuchKey(_))
        ));
    }
}

async fn complete_with_forced_rollback(
    harness: &TestHarness,
    id: &str,
    parts: &[(i32, String)],
) -> reqwest::Response {
    let db = harness.state.store.db();
    let postgres = db.get_database_backend() == DatabaseBackend::Postgres;
    let (create, drop) = if postgres {
        db.execute_unprepared("CREATE FUNCTION reject_zip_complete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced Complete rollback'; END; $$").await.unwrap();
        (
            "CREATE TRIGGER reject_zip_complete BEFORE DELETE ON multipart_uploads FOR EACH ROW EXECUTE FUNCTION reject_zip_complete()",
            "DROP TRIGGER reject_zip_complete ON multipart_uploads",
        )
    } else {
        (
            "CREATE TRIGGER reject_zip_complete BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(ABORT, 'forced Complete rollback'); END",
            "DROP TRIGGER reject_zip_complete",
        )
    };
    db.execute_unprepared(create).await.unwrap();
    let response = complete_multipart(harness, SOURCE, id, parts).await;
    db.execute_unprepared(drop).await.unwrap();
    response
}

async fn abort_after_complete_rollback(
    database_url: &str,
    successor_key: &str,
    released_key: &str,
) {
    let harness = setup(database_url).await;
    let db = harness.state.store.db();
    let (id, parts) = start_upload(&harness, OLD_TOKEN).await;
    let complete = complete_with_forced_rollback(&harness, &id, &parts).await;
    assert_eq!(
        complete.status(),
        StatusCode::INTERNAL_SERVER_ERROR,
        "{}",
        complete.text().await.unwrap()
    );
    assert!(store::multipart::get_upload(db, &id).await.is_ok());
    assert_eq!(
        store::multipart::list_parts(db, &id).await.unwrap().len(),
        1
    );
    let execution = zip::execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(execution.state, "admitted");
    let claim = zip::execution::Claim {
        batch_id: id.clone(),
        epoch: execution.epoch,
        worker: execution.worker.unwrap(),
    };
    let targets = zip::execution::read_targets(db, &claim).await.unwrap();
    let [target] = targets.as_slice() else {
        panic!("expected one admitted output: {targets:?}")
    };
    assert_eq!(target.key, OUTPUT);
    let original_source_id = format!("zip-v2-source:{id}");
    let source_before = guard_at(&harness, SOURCE).await;
    let output_before = guard_at(&harness, OUTPUT).await;
    for (mutation_id, (destination, lease)) in [
        (&original_source_id, &source_before),
        (&target.mutation_id, &output_before),
    ] {
        let destination = destination.as_ref().unwrap();
        let lease = lease.as_ref().unwrap();
        assert_eq!(
            destination.mutation_id.as_deref(),
            Some(mutation_id.as_str())
        );
        assert_eq!(&lease.mutation_id, mutation_id);
        assert_eq!(lease.generation, destination.generation);
    }

    let independent = prepare_independent_root(&harness).await;
    // Abort owns the upload identity even if historical JSON bytes differ across its records.
    let param = if db.get_database_backend() == DatabaseBackend::Postgres {
        "$1"
    } else {
        "?"
    };
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!("UPDATE zip_v2_mpu_intakes SET captured_options=' ' || captured_options WHERE original_upload_id={param}"),
        [id.clone().into()],
    )).await.unwrap();
    let successor = if successor_key == SOURCE {
        let tx = db.begin().await.unwrap();
        lock_bucket_for_ownership(&tx, &harness.bucket)
            .await
            .unwrap();
        let successor = admit_zip_v2_source_in_transaction(
            &tx,
            &harness.bucket,
            SOURCE,
            &BTreeSet::from([OUTPUT.to_owned()]),
            &original_source_id,
            chrono::Utc::now(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(successor.mutation_id, original_source_id);
        assert!(successor.expected_generation > source_before.1.unwrap().generation);
        successor
    } else {
        assert!(zip::execution::fence(db, &claim).await.unwrap());
        admit_content_mutation(
            db,
            &harness.bucket,
            successor_key,
            None,
            SupersedeReason::PutObject,
            chrono::Utc::now(),
        )
        .await
        .unwrap()
    };
    let successor_before = guard_at(&harness, successor_key).await;
    let rpc_count = harness.kubo.received_requests().await.unwrap().len();
    let abort = tokio::time::timeout(
        Duration::from_secs(10),
        abort_multipart(&harness, SOURCE, &id),
    )
    .await
    .expect("Abort must not deadlock on its own guards");
    assert_eq!(
        abort.status(),
        StatusCode::NO_CONTENT,
        "{}",
        abort.text().await.unwrap()
    );
    assert_aborted(&harness, &id).await;
    let mirror = zip::read(db, &id).await.unwrap().unwrap();
    assert_eq!(mirror.root_error_code.as_deref(), Some("upload_aborted"));
    let (destination, lease) = guard_at(&harness, released_key).await;
    assert_eq!(destination.unwrap().mutation_id, None, "{released_key}");
    assert!(
        lease.is_none(),
        "{released_key} still has its aborted lease"
    );
    assert_eq!(guard_at(&harness, successor_key).await, successor_before);
    renew_standard_mutation(db, &successor).await.unwrap();
    let retry = complete_multipart(&harness, SOURCE, &id, &parts).await;
    assert_eq!(
        retry.status(),
        StatusCode::CONFLICT,
        "{}",
        retry.text().await.unwrap()
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        rpc_count
    );

    let independent_id = &independent.batch.id;
    let after = zip::snapshot(db, independent_id).await.unwrap().unwrap();
    assert_eq!(after.batch, independent.batch);
    assert_eq!(after.builds, independent.builds);
    assert_eq!(
        zip::recovery::due_page(db).await.unwrap(),
        vec![independent_id.clone()]
    );
    let root = zip::claim_root(db, independent_id, "independent-recovery", 30)
        .await
        .unwrap();
    assert!(
        zip::recovery::files(
            &zip::snapshot(db, independent_id).await.unwrap().unwrap(),
            &root
        )
        .is_ok()
    );
}

#[tokio::test]
async fn signed_abort_releases_exact_guards_after_complete_rollback() {
    for (successor, released) in [(SOURCE, OUTPUT), (OUTPUT, SOURCE)] {
        abort_after_complete_rollback("sqlite::memory:", successor, released).await;
    }
}

#[tokio::test]
async fn signed_abort_fences_claimed_complete_before_manifest_admission() {
    let harness = setup("sqlite::memory:").await;
    let db = harness.state.store.db();
    let (id, parts) = start_upload(&harness, OLD_TOKEN).await;
    let mut cat = block_next_kubo_request(
        &harness.kubo,
        KuboBlockTarget::CatCid("QmArchive"),
        ResponseTemplate::new(200).set_body_bytes(legal_single_entry_zip()),
    )
    .await;
    let complete = complete_multipart(&harness, SOURCE, &id, &parts);
    tokio::pin!(complete);
    tokio::select! {
        _ = cat.wait_until_blocked() => {},
        early = &mut complete => panic!("Complete ended before the extraction gate: {} {}", early.status(), early.text().await.unwrap()),
    }
    let execution = zip::execution::read(db, &id).await.unwrap().unwrap();
    assert_eq!(execution.state, "pending");
    assert!(execution.worker.is_some());
    assert_eq!(execution.input_art_cid.as_deref(), Some("QmArchive"));
    assert!(zip::read(db, &id).await.unwrap().is_none());

    let abort = abort_multipart(&harness, SOURCE, &id).await;
    assert_eq!(
        abort.status(),
        StatusCode::NO_CONTENT,
        "{}",
        abort.text().await.unwrap()
    );
    let successor = admit_content_mutation(
        db,
        &harness.bucket,
        SOURCE,
        None,
        SupersedeReason::PutObject,
        chrono::Utc::now(),
    )
    .await
    .unwrap();
    let successor_before = guard_at(&harness, SOURCE).await;
    cat.release();
    let late = tokio::time::timeout(Duration::from_secs(10), &mut complete)
        .await
        .expect("the blocked extraction finished after release");
    assert_eq!(
        late.status(),
        StatusCode::CONFLICT,
        "{}",
        late.text().await.unwrap()
    );
    assert_aborted(&harness, &id).await;
    assert_eq!(guard_at(&harness, SOURCE).await, successor_before);
    renew_standard_mutation(db, &successor).await.unwrap();
    assert_eq!(guard_at(&harness, OUTPUT).await, (None, None));
    assert!(zip::read(db, &id).await.unwrap().is_none());
    assert_eq!(
        harness.captured_add_file_bytes().len(),
        3,
        "late Complete must reach the manifest boundary after extraction"
    );

    let stale_create = support::sigv4::send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        SOURCE,
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        controls(OLD_TOKEN, false),
        "test",
    )
    .await;
    assert_eq!(
        stale_create.status(),
        StatusCode::CONFLICT,
        "{}",
        stale_create.text().await.unwrap()
    );
    let (fresh_id, fresh_parts) = start_upload(&harness, NEW_TOKEN).await;
    assert_ne!(fresh_id, id);
    let stale_abort = abort_multipart(&harness, SOURCE, &id).await;
    assert_eq!(stale_abort.status(), StatusCode::NOT_FOUND);
    let fresh = complete_multipart(&harness, SOURCE, &fresh_id, &fresh_parts).await;
    assert_eq!(
        fresh.status(),
        StatusCode::OK,
        "{}",
        fresh.text().await.unwrap()
    );
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, SOURCE)
            .await
            .unwrap()
            .cid,
        "QmOtherArchive"
    );
    assert_eq!(
        store::object::get_latest(db, &harness.bucket, OUTPUT)
            .await
            .unwrap()
            .cid,
        LEAF
    );
}

#[tokio::test]
#[ignore = "requires IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_signed_abort_releases_exact_guards_after_complete_rollback() {
    for (successor, released) in [(SOURCE, OUTPUT), (OUTPUT, SOURCE)] {
        baseline_postgres::with_schema(|db: DatabaseConnection| async move {
            let schema: String = db
                .query_one(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT current_schema() AS name",
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get("", "name")
                .unwrap();
            let mut url =
                url::Url::parse(&std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap()).unwrap();
            url.query_pairs_mut()
                .append_pair("options", &format!("-csearch_path={schema}"));
            abort_after_complete_rollback(url.as_str(), successor, released).await;
        })
        .await;
    }
}
