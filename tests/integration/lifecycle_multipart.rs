use super::common::{
    OwnedTestEndpoint, assert_mpu_s3_error, scripted, signed_complete_multipart, signed_copy,
    signed_create_multipart_upload_with_tagging, signed_delete_bucket_lifecycle_configuration,
    signed_get, signed_get_bucket_lifecycle_configuration, signed_put,
    signed_put_bucket_lifecycle_configuration_xml, signed_put_bucket_versioning,
    signed_upload_part, standard_script, stored_lifecycle_configuration_for, xml_element_values,
};
use crate::support;
use crate::support::decompress::{KuboBlockTarget, S3TestEndpoint};
use crate::support::lifecycle::{LifecycleHarness, start_lifecycle_harness};
use crate::support::sigv4::send_sigv4;
use chrono::Duration as ChronoDuration;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter,
    QueryOrder, Set, Statement,
};
use std::sync::Arc;

#[tokio::test]
async fn lifecycle_abort_multipart_signed_api_and_absence_semantics() {
    let mut harness = start_lifecycle_harness(standard_script(2)).await;
    let xml = abort_lifecycle_xml("Enabled", "<Prefix>logs/</Prefix>", 1);
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(&harness, xml.clone())
            .await
            .status(),
        StatusCode::OK
    );
    let response = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(response.status(), StatusCode::OK);
    let returned = response.text().await.unwrap();
    assert_eq!(xml_element_values(&returned, "ID"), vec!["abort"]);
    assert_eq!(xml_element_values(&returned, "Status"), vec!["Enabled"]);
    assert_eq!(xml_element_values(&returned, "Prefix"), vec!["logs/"]);
    assert_eq!(
        xml_element_values(&returned, "DaysAfterInitiation"),
        vec!["1"]
    );
    let before = stored_lifecycle_configuration_for(&harness).await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(&harness, returned.clone())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        stored_lifecycle_configuration_for(&harness)
            .await
            .canonical_json,
        before.canonical_json
    );
    assert_eq!(
        signed_get_bucket_lifecycle_configuration(&harness)
            .await
            .text()
            .await
            .unwrap(),
        returned
    );

    let upload = create_aged_lifecycle_upload(&harness, "logs/expired", true).await;
    let claim = scan_multipart_action(&harness).await;
    assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
    assert_multipart_absent(&harness, "logs/expired", &upload).await;

    let before = stored_lifecycle_configuration_for(&harness).await;
    for (filter, days) in [
        ("<Prefix>logs/</Prefix>", 0),
        ("<Tag><Key>env</Key><Value>test</Value></Tag>", 1),
        (
            "<And><Prefix>logs/</Prefix><Tag><Key>env</Key><Value>test</Value></Tag></And>",
            1,
        ),
        ("<ObjectSizeGreaterThan>1</ObjectSizeGreaterThan>", 1),
        ("<ObjectSizeLessThan>9</ObjectSizeLessThan>", 1),
    ] {
        assert_mpu_s3_error(
            signed_put_bucket_lifecycle_configuration_xml(
                &harness,
                abort_lifecycle_xml("Enabled", filter, days),
            )
            .await,
            "InvalidRequest",
        )
        .await;
        let after = stored_lifecycle_configuration_for(&harness).await;
        assert_eq!(after.canonical_json, before.canonical_json);
        assert_eq!(after.revision, before.revision);
    }
    assert_mpu_s3_error(
        signed_abort_upload(&harness, "logs/expired", "absent").await,
        "NoSuchUpload",
    )
    .await;
    let upload = create_aged_lifecycle_upload(&harness, "logs/explicit", true).await;
    let claim = schedule_multipart_action(&harness).await;
    assert_mpu_s3_error(
        signed_abort_upload(&harness, "wrong-key", &upload).await,
        "NoSuchUpload",
    )
    .await;
    assert_eq!(multipart_snapshot(&harness, &upload).await.1.len(), 1);
    assert_eq!(
        signed_abort_upload(&harness, "logs/explicit", &upload)
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_mpu_s3_error(
        signed_abort_upload(&harness, "logs/explicit", &upload).await,
        "NoSuchUpload",
    )
    .await;
    assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
    assert_multipart_absent(&harness, "logs/explicit", &upload).await;

    let upload = create_aged_lifecycle_upload(&harness, "logs/disabled", false).await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &harness,
            abort_lifecycle_xml("Disabled", "<Prefix>logs/</Prefix>", 1)
        )
        .await
        .status(),
        StatusCode::OK
    );
    let before = harness.action_rows().await;
    for _ in 0..3 {
        harness.run_one_scan_page().await;
    }
    assert_eq!(harness.action_rows().await, before);
    assert!(multipart_snapshot(&harness, &upload).await.0.is_some());
    harness.assert_no_pin_removal().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_abort_multipart_upload_part_race_cannot_resurrect() {
    for abort_wins in [true, false] {
        let mut harness = start_lifecycle_harness(standard_script(1)).await;
        let key = "logs/part-race";
        let upload = create_aged_lifecycle_upload(&harness, key, false).await;
        let claim = schedule_multipart_action(&harness).await;
        let response = if abort_wins {
            let mut block = support::decompress::block_next_kubo_request(
                &harness.kubo,
                KuboBlockTarget::PinAdd,
                wiremock::ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"),
            )
            .await;
            let endpoint = owned_lifecycle_endpoint(&harness);
            let id = upload.clone();
            let task = tokio::spawn(async move {
                signed_upload_part(&endpoint, key, &id, 1, b"hello world".to_vec()).await
            });
            block.wait_until_blocked().await;
            assert!(multipart_snapshot(&harness, &upload).await.1.is_empty());
            assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
            block.release();
            tokio::time::timeout(std::time::Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap()
        } else {
            let response =
                signed_upload_part(&harness, key, &upload, 1, b"hello world".to_vec()).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(multipart_snapshot(&harness, &upload).await.1.len(), 1);
            assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
            response
        };
        if abort_wins {
            assert_mpu_s3_error(response, "NoSuchUpload").await;
        } else {
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_multipart_absent(&harness, key, &upload).await;
        assert_single_multipart_success(&harness).await;
        let requests = harness.kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.path() == "/api/v0/add")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.path() == "/api/v0/pin/add"
                    && r.url
                        .query_pairs()
                        .any(|(k, v)| k == "arg" && v == "QmTestCid"))
                .count(),
            1
        );
        harness.assert_no_pin_removal().await;
        harness.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_abort_multipart_complete_race_has_both_winners() {
    for abort_wins in [true, false] {
        let mut harness = start_lifecycle_harness(scripted(
            &["QmPart", "QmRoot"],
            vec![
                ("QmPart", b"hello world".to_vec()),
                ("QmRoot", b"hello world".to_vec()),
            ],
        ))
        .await;
        assert_eq!(
            signed_put_bucket_versioning(&harness, "Enabled")
                .await
                .status(),
            StatusCode::OK
        );
        let key = "logs/complete-race";
        let upload = create_aged_lifecycle_upload(&harness, key, true).await;
        assert_eq!(
            signed_put_bucket_lifecycle_configuration_xml(
                &harness,
                abort_lifecycle_xml("Enabled", "<Prefix>logs/</Prefix>", 1)
            )
            .await
            .status(),
            StatusCode::OK
        );
        scan_multipart_pages(&harness).await;
        if abort_wins {
            let claim = harness.claim_one_action().await;
            let mut block = support::decompress::block_next_kubo_request(
                &harness.kubo,
                KuboBlockTarget::Add,
                wiremock::ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmRoot\",\"Size\":\"11\"}\n"),
            )
            .await;
            let endpoint = owned_lifecycle_endpoint(&harness);
            let id = upload.clone();
            let task = tokio::spawn(async move {
                signed_complete_multipart(&endpoint, key, &id, 1, "QmPart").await
            });
            block.wait_until_blocked().await;
            assert_eq!(multipart_snapshot(&harness, &upload).await.1.len(), 1);
            assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
            block.release();
            let response = tokio::time::timeout(std::time::Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap();
            assert_mpu_s3_error(response, "NoSuchUpload").await;
            assert_multipart_absent(&harness, key, &upload).await;
            for table in [
                "objects",
                "object_versions",
                "object_tags",
                "pin_leases",
                "pin_jobs",
            ] {
                let row = harness
                    .state
                    .store
                    .db()
                    .query_one(Statement::from_string(
                        DatabaseBackend::Sqlite,
                        format!("SELECT COUNT(*) AS count FROM {table}"),
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    row.try_get::<i64>("", "count").unwrap(),
                    0,
                    "no publication residue in {table}"
                );
            }
        } else {
            let (worker, gate) = start_multipart_claim_gate(&harness);
            let claim = tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_claim())
                .await
                .unwrap();
            worker.abort_for_test().await.unwrap_err();
            let response = signed_complete_multipart(&harness, key, &upload, 1, "QmPart").await;
            assert_eq!(response.status(), StatusCode::OK);
            let version = response
                .headers()
                .get("x-amz-version-id")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            assert_eq!(harness.version_rows(key).await.len(), 1);
            assert_eq!(
                signed_get(&harness, key).await.bytes().await.unwrap(),
                &b"hello world"[..]
            );
            let version_response = send_sigv4(
                reqwest::Method::GET,
                &harness.endpoint,
                &harness.bucket,
                key,
                &[("versionId", &version)],
                Vec::new(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(version_response.status(), StatusCode::OK);
            assert_eq!(version_response.bytes().await.unwrap(), &b"hello world"[..]);
            gate.release();
            assert_eq!(harness.execute_claim(&claim).await.state, "succeeded");
            assert!(multipart_snapshot(&harness, &upload).await.0.is_none());
            assert!(multipart_snapshot(&harness, &upload).await.1.is_empty());
            assert_eq!(harness.version_rows(key).await.len(), 1);
            assert_eq!(signed_get(&harness, key).await.status(), StatusCode::OK);
        }
        assert_single_multipart_success(&harness).await;
        let requests = harness.kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.path() == "/api/v0/add")
                .count(),
            2
        );
        for cid in ["QmPart", "QmRoot"] {
            assert!(
                requests.iter().any(|r| r.url.path() == "/api/v0/pin/add"
                    && r.url.query_pairs().any(|(k, v)| k == "arg" && v == cid)),
                "accepted CID remains pinned: {cid}"
            );
        }
        harness.assert_no_pin_removal().await;
        harness.shutdown().await;
    }
}

#[tokio::test]
async fn lifecycle_abort_multipart_revalidation_retry_and_no_pin_rm() {
    use ipfs_s3_gateway::lifecycle::{
        config::{canonical_json, from_canonical_json},
        model::{CanonicalFilter, CanonicalRuleSelector, LifecycleRuleStatus},
    };
    use sea_orm::sea_query::Expr;
    use store::entities::{
        bucket_lifecycle_config, import_destination, lifecycle_action, multipart_upload,
    };

    for case in [
        "replace",
        "delete",
        "missing_rule",
        "disabled",
        "prefix",
        "not_due",
        "stale_upload",
    ] {
        let mut harness = start_lifecycle_harness(standard_script(1)).await;
        let key = "logs/revalidate";
        let upload = create_aged_lifecycle_upload(&harness, key, true).await;
        let claim = schedule_multipart_action(&harness).await;
        match case {
            "replace" => assert_eq!(
                signed_put_bucket_lifecycle_configuration_xml(
                    &harness,
                    abort_lifecycle_xml("Enabled", "<Prefix>logs/</Prefix>", 30)
                )
                .await
                .status(),
                StatusCode::OK
            ),
            "delete" => assert_eq!(
                signed_delete_bucket_lifecycle_configuration(&harness)
                    .await
                    .status(),
                StatusCode::NO_CONTENT
            ),
            "not_due" | "stale_upload" => {
                let now = store::database_clock::database_now(harness.state.store.db())
                    .await
                    .unwrap();
                multipart_upload::Entity::update_many()
                    .col_expr(multipart_upload::Column::CreatedAt, Expr::value(now))
                    .filter(multipart_upload::Column::UploadId.eq(&upload))
                    .exec(harness.state.store.db())
                    .await
                    .unwrap();
                if case == "not_due" {
                    // A persisted candidate with a premature due boundary must not abort
                    // a newly initiated upload, even when its exact identity matches.
                    lifecycle_action::Entity::update_many()
                        .col_expr(
                            lifecycle_action::Column::TargetUploadCreatedAt,
                            Expr::value(Some(now)),
                        )
                        .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
                        .exec(harness.state.store.db())
                        .await
                        .unwrap();
                }
            }
            _ => {
                let stored = stored_lifecycle_configuration_for(&harness).await;
                let mut config =
                    from_canonical_json(stored.canonical_json.as_deref().unwrap()).unwrap();
                match case {
                    "missing_rule" => {
                        config.rules[0].id = Some("replacement-rule".to_owned());
                        config.rules[0]
                            .abort_incomplete_multipart_upload
                            .as_mut()
                            .unwrap()
                            .days_after_initiation = 30;
                    }
                    "disabled" => config.rules[0].status = LifecycleRuleStatus::Disabled,
                    "prefix" => {
                        config.rules[0].selector = CanonicalRuleSelector::Modern {
                            filter: CanonicalFilter::Prefix {
                                prefix: "other/".to_owned(),
                            },
                        }
                    }
                    _ => unreachable!(),
                }
                bucket_lifecycle_config::Entity::update_many()
                    .col_expr(
                        bucket_lifecycle_config::Column::CanonicalJson,
                        Expr::value(Some(canonical_json(&config).unwrap())),
                    )
                    .filter(bucket_lifecycle_config::Column::Bucket.eq(&harness.bucket))
                    .exec(harness.state.store.db())
                    .await
                    .unwrap();
                assert_eq!(
                    stored_lifecycle_configuration_for(&harness).await.revision,
                    stored.revision
                );
            }
        }
        let before = multipart_snapshot(&harness, &upload).await;
        let requests = harness.kubo.received_requests().await.unwrap().len();
        assert_eq!(
            harness.execute_claim(&claim).await.state,
            "cancelled",
            "{case}"
        );
        assert_eq!(
            multipart_snapshot(&harness, &upload).await,
            before,
            "{case}"
        );
        assert_eq!(harness.action_rows().await.len(), 1, "{case}");
        assert_eq!(
            harness.kubo.received_requests().await.unwrap().len(),
            requests
        );
        assert!(harness.version_rows(key).await.is_empty());
        harness.assert_no_pin_removal().await;
        harness.shutdown().await;
    }

    // Reclaim while the original production worker is held immediately after claim.
    let mut stale = start_lifecycle_harness(standard_script(1)).await;
    let upload = create_aged_lifecycle_upload(&stale, "logs/epoch", true).await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &stale,
            abort_lifecycle_xml("Enabled", "", 1)
        )
        .await
        .status(),
        StatusCode::OK
    );
    scan_multipart_pages(&stale).await;
    let (worker, gate) = start_multipart_claim_gate(&stale);
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_claim())
        .await
        .unwrap();
    expire_multipart_claim(&stale, &first.action.id).await;
    let second = stale.claim_one_action().await;
    assert!(second.claim_epoch > first.claim_epoch);
    let before = multipart_snapshot(&stale, &upload).await;
    gate.release();
    worker.shutdown(std::time::Duration::from_secs(10)).await;
    assert_eq!(stale.action_rows().await, vec![second.action.clone()]);
    assert_eq!(multipart_snapshot(&stale, &upload).await, before);
    let now = store::database_clock::database_now(stale.state.store.db())
        .await
        .unwrap();
    assert!(
        !store::lifecycle_action::mark_succeeded(stale.state.store.db(), &first, now)
            .await
            .unwrap()
    );
    assert_eq!(stale.execute_claim(&second).await.state, "succeeded");
    assert_single_multipart_success(&stale).await;
    stale.assert_no_pin_removal().await;
    stale.shutdown().await;

    for terminal_failure in [false, true] {
        let harness = start_lifecycle_harness(standard_script(1)).await;
        let key = "logs/retry";
        let upload = create_aged_lifecycle_upload(&harness, key, true).await;
        let claim = schedule_multipart_action(&harness).await;
        let now = store::database_clock::database_now(harness.state.store.db())
            .await
            .unwrap();
        let token = import_destination::ActiveModel {
            bucket: Set(harness.bucket.clone()),
            key: Set(key.to_owned()),
            generation: Set(42),
            owner_job_id: Set(None),
            mutation_id: Set(Some("foreign-standard-token:unchanged".to_owned())),
            mutation_prefix: Set(None),
            updated_at: Set(now),
        }
        .insert(harness.state.store.db())
        .await
        .unwrap();
        // Trigger errors use SQLite's real transaction/rollback path and the
        // production worker's contention classifier. No timing-based lock race.
        let sql = if terminal_failure {
            "CREATE TRIGGER mpu_fault BEFORE UPDATE OF state ON lifecycle_actions WHEN NEW.state = 'succeeded' BEGIN SELECT RAISE(FAIL, 'database is locked'); END"
        } else {
            "CREATE TRIGGER mpu_fault BEFORE DELETE ON multipart_uploads BEGIN SELECT RAISE(FAIL, 'database is locked'); END"
        };
        harness
            .state
            .store
            .db()
            .execute_unprepared(sql)
            .await
            .unwrap();
        let before = multipart_snapshot(&harness, &upload).await;
        expire_multipart_claim(&harness, &claim.action.id).await;
        let (worker, gate) = start_multipart_claim_gate(&harness);
        let retry_claim =
            tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_claim())
                .await
                .unwrap();
        gate.release();
        let pending = wait_multipart_action_state(&harness, &claim.action.id, "pending").await;
        worker.shutdown(std::time::Duration::from_secs(10)).await;
        assert_eq!(
            pending.failure_class.as_deref(),
            Some(store::lifecycle_action::FAILURE_DATABASE_CONTENTION)
        );
        assert_eq!(pending.attempts, retry_claim.action.attempts);
        assert!(pending.next_attempt_at > pending.updated_at);
        assert_eq!(multipart_snapshot(&harness, &upload).await, before);
        assert_eq!(
            import_destination::Entity::find_by_id((harness.bucket.clone(), key.to_owned()))
                .one(harness.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            token
        );
        harness
            .state
            .store
            .db()
            .execute_unprepared("DROP TRIGGER mpu_fault")
            .await
            .unwrap();
        lifecycle_action::Entity::update_many()
            .col_expr(
                lifecycle_action::Column::NextAttemptAt,
                Expr::value(now - ChronoDuration::seconds(1)),
            )
            .filter(lifecycle_action::Column::Id.eq(&claim.action.id))
            .exec(harness.state.store.db())
            .await
            .unwrap();
        let (worker, gate) = start_multipart_claim_gate(&harness);
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(10), gate.wait_claim())
            .await
            .unwrap();
        assert_eq!(recovered.claim_epoch, retry_claim.claim_epoch + 1);
        gate.release();
        let terminal = wait_multipart_action_state(&harness, &claim.action.id, "succeeded").await;
        worker.shutdown(std::time::Duration::from_secs(10)).await;
        assert_eq!(terminal.state, "succeeded");
        assert_eq!(terminal.attempts, pending.attempts + 1);
        assert_eq!(
            import_destination::Entity::find_by_id((harness.bucket.clone(), key.to_owned()))
                .one(harness.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            token
        );
        assert_multipart_absent(&harness, key, &upload).await;
        assert_single_multipart_success(&harness).await;
        harness.assert_no_pin_removal().await;
        harness.shutdown().await;
    }

    // Pending MPU work is not a standard content-admission lock on its key.
    let mut admission = start_lifecycle_harness(standard_script(3)).await;
    let key = "logs/admission";
    let upload = create_aged_lifecycle_upload(&admission, key, true).await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &admission,
            abort_lifecycle_xml("Enabled", "", 1)
        )
        .await
        .status(),
        StatusCode::OK
    );
    scan_multipart_pages(&admission).await;
    assert_eq!(admission.action_rows().await[0].state, "pending");
    assert_eq!(
        signed_put(
            &admission,
            key,
            &[],
            b"hello world".to_vec(),
            HeaderMap::new()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &admission,
            "source",
            &[],
            b"hello world".to_vec(),
            HeaderMap::new()
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_copy(&admission, "source", key, HeaderMap::new())
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(admission.action_rows().await[0].state, "pending");
    let versions = admission.version_rows(key).await;
    let claim = admission.claim_one_action().await;
    assert_eq!(admission.execute_claim(&claim).await.state, "succeeded");
    assert_eq!(admission.version_rows(key).await, versions);
    assert_eq!(
        signed_get(&admission, key).await.bytes().await.unwrap(),
        &b"hello world"[..]
    );
    assert!(multipart_snapshot(&admission, &upload).await.0.is_none());
    assert!(multipart_snapshot(&admission, &upload).await.1.is_empty());
    assert_single_multipart_success(&admission).await;
    admission.assert_no_pin_removal().await;
    admission.shutdown().await;
}

async fn expire_multipart_claim(harness: &LifecycleHarness, action_id: &str) {
    use store::entities::lifecycle_action;
    let now = store::database_clock::database_now(harness.state.store.db())
        .await
        .unwrap();
    lifecycle_action::Entity::update_many()
        .col_expr(
            lifecycle_action::Column::LeaseUntil,
            sea_orm::sea_query::Expr::value(Some(now - ChronoDuration::seconds(1))),
        )
        .filter(lifecycle_action::Column::Id.eq(action_id))
        .exec(harness.state.store.db())
        .await
        .unwrap();
}

async fn wait_multipart_action_state(
    harness: &LifecycleHarness,
    action_id: &str,
    state: &str,
) -> store::entities::lifecycle_action::Model {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let row = store::entities::lifecycle_action::Entity::find_by_id(action_id)
                .one(harness.state.store.db())
                .await
                .unwrap()
                .unwrap();
            if row.state == state {
                return row;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("production worker must reach expected action state")
}

fn abort_lifecycle_xml(status: &str, filter: &str, days: u32) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>abort</ID><Status>{status}</Status><Filter>{filter}</Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>{days}</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>"
    )
}

fn owned_lifecycle_endpoint(harness: &LifecycleHarness) -> OwnedTestEndpoint {
    OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: harness.bucket.clone(),
    }
}

async fn signed_abort_upload(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload: &str,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploadId", upload)],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn create_aged_lifecycle_upload(
    harness: &LifecycleHarness,
    key: &str,
    with_part: bool,
) -> String {
    let response = signed_create_multipart_upload_with_tagging(harness, key, "env=test").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    let upload = xml_element_values(&body, "UploadId")[0].to_owned();
    if with_part {
        assert_eq!(
            signed_upload_part(harness, key, &upload, 1, b"hello world".to_vec())
                .await
                .status(),
            StatusCode::OK
        );
    }
    let now = store::database_clock::database_now(harness.state.store.db())
        .await
        .unwrap();
    use store::entities::multipart_upload;
    multipart_upload::Entity::update_many()
        .col_expr(
            multipart_upload::Column::CreatedAt,
            sea_orm::sea_query::Expr::value(now - ChronoDuration::days(4)),
        )
        .filter(multipart_upload::Column::UploadId.eq(&upload))
        .exec(harness.state.store.db())
        .await
        .unwrap();
    upload
}

async fn multipart_snapshot(
    harness: &LifecycleHarness,
    upload: &str,
) -> (
    Option<store::entities::multipart_upload::Model>,
    Vec<store::entities::multipart_part::Model>,
) {
    use store::entities::{multipart_part, multipart_upload};
    (
        multipart_upload::Entity::find_by_id(upload)
            .one(harness.state.store.db())
            .await
            .unwrap(),
        multipart_part::Entity::find()
            .filter(multipart_part::Column::UploadId.eq(upload))
            .order_by_asc(multipart_part::Column::PartNumber)
            .all(harness.state.store.db())
            .await
            .unwrap(),
    )
}

async fn scan_multipart_pages(harness: &LifecycleHarness) {
    for _ in 0..3 {
        harness.run_one_scan_page().await;
    }
}

async fn scan_multipart_action(
    harness: &LifecycleHarness,
) -> ipfs_s3_gateway::lifecycle::model::ClaimedLifecycleAction {
    scan_multipart_pages(harness).await;
    harness.claim_one_action().await
}

async fn schedule_multipart_action(
    harness: &LifecycleHarness,
) -> ipfs_s3_gateway::lifecycle::model::ClaimedLifecycleAction {
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            harness,
            abort_lifecycle_xml("Enabled", "<Prefix>logs/</Prefix>", 1)
        )
        .await
        .status(),
        StatusCode::OK
    );
    scan_multipart_action(harness).await
}

async fn assert_multipart_absent(harness: &LifecycleHarness, key: &str, upload: &str) {
    let snapshot = multipart_snapshot(harness, upload).await;
    assert!(snapshot.0.is_none());
    assert!(snapshot.1.is_empty());
    assert!(harness.version_rows(key).await.is_empty());
    assert_mpu_s3_error(signed_get(harness, key).await, "NoSuchKey").await;
}

async fn assert_single_multipart_success(harness: &LifecycleHarness) {
    let rows = harness.action_rows().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, "succeeded");
    assert_eq!(rows[0].target_type, "multipart_upload");
    assert_eq!(rows[0].action_kind, "abort_incomplete_multipart_upload");
    assert!(rows[0].lease_until.is_none());
}

fn start_multipart_claim_gate(
    harness: &LifecycleHarness,
) -> (
    ipfs_s3_gateway::lifecycle::worker::LifecycleWorkerHandle,
    Arc<ipfs_s3_gateway::lifecycle::worker::LifecycleAfterClaimGate>,
) {
    use ipfs_s3_gateway::lifecycle::worker::{
        LifecycleAfterClaimGate, LifecycleWorkerTestControl, start_worker_for_test,
    };
    let worker_id = format!("mpu-{}", uuid::Uuid::new_v4());
    let gate = LifecycleAfterClaimGate::new(&worker_id);
    let config = ipfs_s3_gateway::config::LifecycleWorkerConfig {
        poll_interval_ms: 60_000,
        scan_page_size: 1_000,
        scan_lease_secs: 30,
        action_lease_secs: 30,
        worker_concurrency: 1,
        max_attempts: 8,
        base_backoff_secs: 1,
        max_backoff_secs: 60,
    }
    .validate()
    .unwrap();
    let worker = start_worker_for_test(
        harness.state.store.clone(),
        config,
        tokio_util::sync::CancellationToken::new(),
        LifecycleWorkerTestControl {
            worker_id,
            after_claim: Some(gate.clone()),
        },
    );
    (worker, gate)
}
