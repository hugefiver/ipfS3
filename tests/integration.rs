mod support;

#[tokio::test]
async fn tier_reads_signed_real_service_routes_by_immutable_residency() {
    support::tier_reads::assert_signed_tier_reads().await;
}

#[tokio::test]
async fn signed_fixed_length_gets_reject_late_kubo_error_trailers() {
    support::tier_reads::assert_signed_fixed_length_gets_wait_for_kubo_eof().await;
}

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

async fn assert_mpu_s3_error(response: reqwest::Response, code: &str) {
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(
        status.is_client_error(),
        "expected {code}, got {status}: {body}"
    );
    assert_eq!(xml_element_values(&body, "Code"), vec![code]);
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

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use http::{HeaderMap, HeaderValue, StatusCode};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QueryOrder, Set, Statement, TransactionTrait,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use ipfs_s3_gateway::config::PolicyConfig;
use ipfs_s3_gateway::state::AppState;
use ipfs_s3_gateway::store;
use support::decompress::{
    AddReply, KuboBlockTarget, KuboScript, S3TestEndpoint, TestHarness, abort_multipart,
    archive_key_collision_zip, assert_no_kubo_calls, assert_pin_calls, complete_multipart,
    complete_multipart_with_headers, complete_multipart_xml, create_multipart,
    create_multipart_with_headers, duplicate_entry_zip, latest_observed_request,
    legal_single_entry_zip, legal_two_entry_zip, start_blocking_harness, start_harness,
    traversal_zip, upload_part, upload_part_with_headers,
};
use support::import::{
    ImportHarness, ImportHarnessConfig, ImportPublicationBlockControl, TestHttpsReply,
    get_import_status, post_import, start_import_harness, start_strict_import_harness,
    wait_for_import_state,
};
use support::lifecycle::{LifecycleHarness, start_lifecycle_harness};
use support::pinning::{
    PinningHarness, PinningHarnessConfig, PsaReply, TestProviderConfig, start_pinning_harness,
};
use support::sigv4::{presign_sigv4_query, send_sigv4, send_sigv4_chunked_http1};

const SINGLE_ENTRY_BYTES: &[u8] = b"single entry bytes";
const FIRST_ENTRY_BYTES: &[u8] = b"first entry bytes";
const SECOND_ENTRY_BYTES: &[u8] = b"second entry bytes";
const FIRST_DUPLICATE_BYTES: &[u8] = b"first duplicate bytes";
const SECOND_DUPLICATE_BYTES: &[u8] = b"second duplicate bytes";
const IMPORT_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
const IMPORT_TEST_CID_V0: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

#[derive(Clone)]
struct OwnedTestEndpoint {
    endpoint: String,
    bucket: String,
}

impl From<&TestHarness> for OwnedTestEndpoint {
    fn from(harness: &TestHarness) -> Self {
        Self {
            endpoint: harness.endpoint.clone(),
            bucket: harness.bucket.clone(),
        }
    }
}

impl S3TestEndpoint for OwnedTestEndpoint {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

fn scripted(cids: &[&'static str], cat_bodies: Vec<(&str, Vec<u8>)>) -> KuboScript {
    KuboScript {
        add_replies: cids.iter().map(|cid| AddReply::Ok(cid)).collect(),
        cat_bodies: cat_bodies
            .into_iter()
            .map(|(cid, body)| (cid.to_owned(), body))
            .collect(),
    }
}

fn standard_script(calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        "QmTestCid",
        calls,
        HashMap::from([("QmTestCid".to_owned(), b"hello world".to_vec())]),
    )
}

/// Convenience: build a path-style rust-s3 client for the real test endpoint.
fn test_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("test"), Some("test"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

fn bad_bucket(harness: &TestHarness) -> Box<Bucket> {
    let region = Region::Custom {
        region: "us-east-1".to_string(),
        endpoint: harness.endpoint.clone(),
    };
    let credentials =
        Credentials::new(Some("wrong"), Some("wrong"), None, None, None).expect("credentials");
    Bucket::new(&harness.bucket, region, credentials)
        .expect("bucket")
        .with_path_style()
}

async fn signed_get(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    signed_get_with_headers(harness, key, HeaderMap::new()).await
}

async fn signed_get_bucket_versioning(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("versioning", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_put_bucket_versioning(
    harness: &impl S3TestEndpoint,
    status: &str,
) -> reqwest::Response {
    signed_put_bucket_versioning_xml(
        harness,
        format!(
        "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Status>{status}</Status></VersioningConfiguration>"
        ),
        HeaderMap::new(),
    )
    .await
}

async fn signed_put_bucket_versioning_xml(
    harness: &impl S3TestEndpoint,
    body: String,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("versioning", "")],
        body.into_bytes(),
        headers,
        "test",
    )
    .await
}

async fn signed_get_bucket_lifecycle_configuration(
    harness: &impl S3TestEndpoint,
) -> reqwest::Response {
    signed_get_bucket_lifecycle_configuration_with_headers(harness, HeaderMap::new()).await
}

async fn signed_get_bucket_lifecycle_configuration_with_headers(
    harness: &impl S3TestEndpoint,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_put_bucket_lifecycle_configuration_xml(
    harness: &impl S3TestEndpoint,
    body: String,
) -> reqwest::Response {
    signed_put_bucket_lifecycle_configuration_xml_with_headers(harness, body, HeaderMap::new())
        .await
}

async fn signed_put_bucket_lifecycle_configuration_xml_with_headers(
    harness: &impl S3TestEndpoint,
    body: String,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        body.into_bytes(),
        headers,
        "test",
    )
    .await
}

async fn signed_delete_bucket_lifecycle_configuration(
    harness: &impl S3TestEndpoint,
) -> reqwest::Response {
    signed_delete_bucket_lifecycle_configuration_with_headers(harness, HeaderMap::new()).await
}

async fn signed_delete_bucket_lifecycle_configuration_with_headers(
    harness: &impl S3TestEndpoint,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("lifecycle", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

fn lifecycle_configuration_xml(rule_id: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter>\
         <Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"
    )
}

fn lifecycle_current_expiration_xml(rule_id: &str, expiration: &str) -> String {
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter/>{expiration}</Rule>\
         </LifecycleConfiguration>"
    )
}

fn lifecycle_noncurrent_expiration_xml(
    rule_id: &str,
    newer_noncurrent_versions: Option<u32>,
) -> String {
    let newer = newer_noncurrent_versions.map_or(String::new(), |count| {
        format!("<NewerNoncurrentVersions>{count}</NewerNoncurrentVersions>")
    });
    format!(
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>{rule_id}</ID><Status>Enabled</Status><Filter/>\
         <NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>{newer}\
         </NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"
    )
}

fn expected_bucket_owner_headers(owner: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-expected-bucket-owner",
        HeaderValue::from_str(owner).expect("valid expected owner header"),
    );
    headers
}

async fn schedule_lifecycle_action(
    harness: &LifecycleHarness,
    configuration: String,
) -> ipfs_s3_gateway::lifecycle::model::ClaimedLifecycleAction {
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(harness, configuration)
            .await
            .status(),
        StatusCode::OK,
        "lifecycle acceptance configuration must save"
    );
    harness.run_one_scan_page().await;
    harness.claim_one_action().await
}

async fn stored_lifecycle_configuration(
    harness: &TestHarness,
) -> ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Model {
    ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Entity::find_by_id(
        harness.bucket.clone(),
    )
    .one(harness.state.store.db())
    .await
    .expect("read stored lifecycle configuration")
    .expect("stored lifecycle configuration")
}

async fn stored_lifecycle_configuration_for(
    harness: &LifecycleHarness,
) -> ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Model {
    ipfs_s3_gateway::store::entities::bucket_lifecycle_config::Entity::find_by_id(
        harness.bucket.clone(),
    )
    .one(harness.state.store.db())
    .await
    .expect("read lifecycle acceptance configuration")
    .expect("lifecycle acceptance configuration exists")
}

async fn signed_get_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_get_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
) -> reqwest::Response {
    signed_get_version_with_headers(harness, key, version_id, HeaderMap::new()).await
}

async fn signed_get_version_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("versionId", version_id)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_head(
    harness: &impl S3TestEndpoint,
    key: &str,
    range: Option<&str>,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if let Some(range) = range {
        headers.insert(
            http::header::RANGE,
            HeaderValue::from_str(range).expect("valid Range header"),
        );
    }
    signed_head_with_headers(harness, key, headers).await
}

async fn signed_head_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_head_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
) -> reqwest::Response {
    signed_head_version_with_headers(harness, key, version_id, HeaderMap::new()).await
}

async fn signed_head_version_with_headers(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: &str,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::HEAD,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("versionId", version_id)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_copy(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    mut headers: HeaderMap,
) -> reqwest::Response {
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!("/{}/{source_key}", harness.bucket()))
            .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        destination_key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_copy_version(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    source_version_id: &str,
    destination_key: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source",
        HeaderValue::from_str(&format!(
            "/{}/{source_key}?versionId={source_version_id}",
            harness.bucket()
        ))
        .expect("copy source header"),
    );
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        destination_key,
        &[],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    query: &[(&str, &str)],
    body: Vec<u8>,
    headers: HeaderMap,
) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        query,
        body,
        headers,
        "test",
    )
    .await
}

async fn signed_put_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    signed_put(harness, key, &[], body, headers).await
}

#[allow(dead_code)]
async fn signed_get_object_tagging(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    signed_get_object_tagging_version(harness, key, None).await
}

async fn signed_get_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_put_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tags: &[(&str, &str)],
) -> reqwest::Response {
    signed_put_object_tagging_version(harness, key, tags, None).await
}

async fn signed_put_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    tags: &[(&str, &str)],
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut xml =
        String::from("<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>");
    for (tag_key, value) in tags {
        xml.push_str("<Tag><Key>");
        xml.push_str(&quick_xml::escape::escape(*tag_key));
        xml.push_str("</Key><Value>");
        xml.push_str(&quick_xml::escape::escape(*value));
        xml.push_str("</Value></Tag>");
    }
    xml.push_str("</TagSet></Tagging>");
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::PUT,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_delete_object_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
) -> reqwest::Response {
    signed_delete_object_tagging_version(harness, key, None).await
}

async fn signed_delete_object_tagging_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let mut query = vec![("tagging", "")];
    if let Some(version_id) = version_id {
        query.push(("versionId", version_id));
    }
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

#[allow(dead_code)]
async fn signed_copy_with_tagging(
    harness: &impl S3TestEndpoint,
    source_key: &str,
    destination_key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging-directive",
        HeaderValue::from_static("REPLACE"),
    );
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_copy(harness, source_key, destination_key, headers).await
}

#[allow(dead_code)]
async fn signed_create_multipart_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    if !tagging.is_empty() {
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
        );
    }
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", "")],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_create_multipart_zip_upload_with_tagging(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploads", ""), ("decompress-zip", target_prefix)],
        Vec::new(),
        headers,
        "test",
    )
    .await
}

async fn signed_upload_part(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: Vec<u8>,
) -> reqwest::Response {
    let part_number = part_number.to_string();
    signed_put(
        harness,
        key,
        &[("partNumber", &part_number), ("uploadId", upload_id)],
        body,
        HeaderMap::new(),
    )
    .await
}

async fn signed_complete_multipart(
    harness: &impl S3TestEndpoint,
    key: &str,
    upload_id: &str,
    part_number: i32,
    etag: &str,
) -> reqwest::Response {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>{part_number}</PartNumber><ETag>\"{}\"</ETag></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(etag)
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        &[("uploadId", upload_id)],
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

async fn signed_decompress_zip_put(
    harness: &impl S3TestEndpoint,
    key: &str,
    target_prefix: &str,
    body: Vec<u8>,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("valid x-amz-tagging header"),
    );
    signed_put(
        harness,
        key,
        &[("decompress-zip", target_prefix)],
        body,
        headers,
    )
    .await
}

async fn signed_delete_object(harness: &impl S3TestEndpoint, key: &str) -> reqwest::Response {
    signed_delete_object_version(harness, key, None).await
}

async fn signed_delete_object_version(
    harness: &impl S3TestEndpoint,
    key: &str,
    version_id: Option<&str>,
) -> reqwest::Response {
    let query = version_id
        .map(|version_id| vec![("versionId", version_id)])
        .unwrap_or_default();
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_delete_bucket(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::DELETE,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_list_objects(harness: &impl S3TestEndpoint) -> reqwest::Response {
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("list-type", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn signed_list_object_versions(
    harness: &impl S3TestEndpoint,
    query: &[(&str, &str)],
) -> reqwest::Response {
    let mut query_with_operation = vec![("versions", "")];
    query_with_operation.extend_from_slice(query);
    send_sigv4(
        reqwest::Method::GET,
        harness.endpoint(),
        harness.bucket(),
        "",
        &query_with_operation,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await
}

async fn assert_signed_body(harness: &impl S3TestEndpoint, key: &str, expected: &[u8]) {
    let response = signed_get(harness, key).await;
    assert_eq!(response.status(), StatusCode::OK, "signed GET {key}");
    assert_eq!(response.bytes().await.expect("GET body").as_ref(), expected);
}

async fn assert_s3_error(
    response: reqwest::Response,
    status: StatusCode,
    code: &str,
    message: &str,
) {
    assert_eq!(response.status(), status);
    let body = response.text().await.expect("S3 error body");
    assert!(body.contains(code), "missing error code {code}: {body}");
    assert!(
        body.contains(message),
        "missing error message {message}: {body}"
    );
}

#[tokio::test]
async fn versioning_bucket_configuration_xml_mfa_and_missing_bucket_errors() {
    let harness = start_harness(scripted(&[], vec![])).await;

    let unversioned = signed_get_bucket_versioning(&harness).await;
    assert_eq!(unversioned.status(), StatusCode::OK);
    let unversioned_xml = unversioned.text().await.expect("unversioned XML");
    assert!(unversioned_xml.contains("VersioningConfiguration"));
    assert!(!unversioned_xml.contains("<Status>"));
    assert!(!unversioned_xml.contains("MfaDelete"));

    let enabled = signed_put_bucket_versioning(&harness, "Enabled").await;
    assert_eq!(enabled.status(), StatusCode::OK);
    let enabled_get = signed_get_bucket_versioning(&harness).await;
    assert_eq!(enabled_get.status(), StatusCode::OK);
    assert!(
        enabled_get
            .text()
            .await
            .expect("enabled XML")
            .contains("<Status>Enabled</Status>")
    );

    let suspended = signed_put_bucket_versioning(&harness, "Suspended").await;
    assert_eq!(suspended.status(), StatusCode::OK);
    let suspended_get = signed_get_bucket_versioning(&harness).await;
    assert_eq!(suspended_get.status(), StatusCode::OK);
    assert!(
        suspended_get
            .text()
            .await
            .expect("suspended XML")
            .contains("<Status>Suspended</Status>")
    );

    let mut mfa_headers = HeaderMap::new();
    mfa_headers.insert("x-amz-mfa", HeaderValue::from_static("serial 123456"));
    assert_s3_error(
        signed_put_bucket_versioning_xml(
            &harness,
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                .to_owned(),
            mfa_headers,
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "MFA delete is not supported",
    )
    .await;
    assert_s3_error(
        signed_put_bucket_versioning_xml(
            &harness,
            "<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>"
                .to_owned(),
            HeaderMap::new(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "MFA delete is not supported",
    )
    .await;
    let still_suspended = signed_get_bucket_versioning(&harness).await;
    assert_eq!(still_suspended.status(), StatusCode::OK);
    assert!(
        still_suspended
            .text()
            .await
            .expect("post-MFA versioning XML")
            .contains("<Status>Suspended</Status>")
    );

    let missing = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "missing-bucket".to_owned(),
    };
    assert_s3_error(
        signed_get_bucket_versioning(&missing).await,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "",
    )
    .await;
}

#[tokio::test]
async fn lifecycle_malformed_xml_uses_framework_error_without_write() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let saved = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("initial"),
    )
    .await;
    assert_eq!(
        saved.status(),
        StatusCode::OK,
        "save valid lifecycle configuration"
    );
    let before = stored_lifecycle_configuration(&harness).await;

    assert_s3_error(
        signed_put_bucket_lifecycle_configuration_xml(
            &harness,
            "<LifecycleConfiguration><Rule>".to_owned(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "",
    )
    .await;

    let after = stored_lifecycle_configuration(&harness).await;
    assert_eq!(after.canonical_json, before.canonical_json);
    assert_eq!(after.revision, before.revision);
}

#[tokio::test]
async fn lifecycle_nested_unknown_xml_uses_framework_error_without_write() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let saved = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("initial"),
    )
    .await;
    assert_eq!(
        saved.status(),
        StatusCode::OK,
        "save valid lifecycle configuration"
    );
    let before = stored_lifecycle_configuration(&harness).await;

    assert_s3_error(
        signed_put_bucket_lifecycle_configuration_xml(
            &harness,
            "<LifecycleConfiguration><Rule><ID>invalid</ID><Status>Enabled</Status>\
             <Filter><UnexpectedNested/></Filter><Expiration><Days>30</Days></Expiration>\
             </Rule></LifecycleConfiguration>"
                .to_owned(),
        )
        .await,
        StatusCode::BAD_REQUEST,
        "MalformedXML",
        "",
    )
    .await;

    let after = stored_lifecycle_configuration(&harness).await;
    assert_eq!(after.canonical_json, before.canonical_json);
    assert_eq!(after.revision, before.revision);
}

#[tokio::test]
async fn lifecycle_root_unknown_element_is_ignored_before_typed_handler() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>kept</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter>\
         <Expiration><Days>30</Days></Expiration></Rule><UnexpectedRoot>discarded</UnexpectedRoot>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("lifecycle GET XML");
    assert!(
        body.contains("<ID>kept</ID>"),
        "missing supported rule: {body}"
    );
    assert!(
        !body.contains("UnexpectedRoot"),
        "s3s must discard unknown direct root children before the typed handler: {body}"
    );
}

#[tokio::test]
async fn lifecycle_signed_configuration_put_get_delete_uses_s3s_operations() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let put = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("signed"),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let saved = stored_lifecycle_configuration(&harness).await;
    assert_eq!(saved.revision, 1);

    let get = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(get.status(), StatusCode::OK);
    let body = get.text().await.expect("lifecycle GET XML");
    assert!(
        body.contains("<ID>signed</ID>"),
        "missing signed rule: {body}"
    );
    assert!(
        !body.contains("Transition"),
        "unsupported action leaked: {body}"
    );

    let delete = signed_delete_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
    let tombstone = stored_lifecycle_configuration(&harness).await;
    assert_eq!(tombstone.revision, 2);
    assert_eq!(tombstone.canonical_json, None);
    assert_s3_error(
        signed_get_bucket_lifecycle_configuration(&harness).await,
        StatusCode::NOT_FOUND,
        "NoSuchLifecycleConfiguration",
        "lifecycle configuration not found",
    )
    .await;
}

#[tokio::test]
async fn versioning_list_uses_first_unreturned_pair_and_hides_markers_from_ordinary_list() {
    let harness = start_harness(scripted(&["QmListOld", "QmListNew"], vec![])).await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let old = signed_put(
        &harness,
        "listed.txt",
        &[],
        b"old".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let new = signed_put(
        &harness,
        "listed.txt",
        &[],
        b"new".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(new.status(), StatusCode::OK);
    let new_version = new.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let deleted = signed_delete_object(&harness, "listed.txt").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let marker_version = deleted.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let first = signed_list_object_versions(&harness, &[("max-keys", "1")]).await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_xml = first.text().await.expect("first version page XML");
    assert!(first_xml.contains("<IsTruncated>true</IsTruncated>"));
    assert!(first_xml.contains("<DeleteMarker>"));
    assert!(first_xml.contains(&format!("<VersionId>{marker_version}</VersionId>")));
    assert!(!first_xml.contains(&format!("<VersionId>{new_version}</VersionId>")));
    let next_key = xml_element_values(&first_xml, "NextKeyMarker")[0].to_owned();
    let next_version = xml_element_values(&first_xml, "NextVersionIdMarker")[0].to_owned();
    assert_eq!(next_key, "listed.txt");
    assert_eq!(next_version, new_version);

    let second = signed_list_object_versions(
        &harness,
        &[
            ("max-keys", "1"),
            ("key-marker", &next_key),
            ("version-id-marker", &next_version),
        ],
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_xml = second.text().await.expect("second version page XML");
    assert!(second_xml.contains(&format!("<VersionId>{new_version}</VersionId>")));
    let third_version = xml_element_values(&second_xml, "NextVersionIdMarker")[0].to_owned();
    assert_eq!(third_version, old_version);

    let ordinary = signed_list_objects(&harness).await;
    assert_eq!(ordinary.status(), StatusCode::OK);
    let ordinary_xml = ordinary.text().await.expect("ordinary list XML");
    assert!(!ordinary_xml.contains("listed.txt"));
    assert!(!ordinary_xml.contains("DeleteMarker"));
}

#[tokio::test]
async fn versioning_read_and_copy_select_current_and_exact_versions() {
    let old_body = b"historical body".to_vec();
    let new_body = b"current body".to_vec();
    let harness = start_harness(scripted(
        &["QmVersionOld", "QmVersionNew"],
        vec![
            ("QmVersionOld", old_body.clone()),
            ("QmVersionNew", new_body.clone()),
        ],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let old = signed_put(
        &harness,
        "versioned.txt",
        &[],
        old_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old
        .headers()
        .get("x-amz-version-id")
        .expect("old version header")
        .to_str()
        .expect("old version header text")
        .to_owned();

    let current = signed_put(
        &harness,
        "versioned.txt",
        &[],
        new_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current
        .headers()
        .get("x-amz-version-id")
        .expect("current version header")
        .to_str()
        .expect("current version header text")
        .to_owned();

    let current_get = signed_get(&harness, "versioned.txt").await;
    assert_eq!(current_get.status(), StatusCode::OK);
    assert_eq!(current_get.headers()["x-amz-version-id"], current_version);
    assert_eq!(
        current_get
            .bytes()
            .await
            .expect("current GET body")
            .as_ref(),
        new_body
    );
    let current_head = signed_head(&harness, "versioned.txt", None).await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(current_head.headers()["x-amz-version-id"], current_version);

    let historical_get = signed_get_version(&harness, "versioned.txt", &old_version).await;
    assert_eq!(historical_get.status(), StatusCode::OK);
    assert_eq!(historical_get.headers()["x-amz-version-id"], old_version);
    assert_eq!(
        historical_get
            .bytes()
            .await
            .expect("historical GET body")
            .as_ref(),
        old_body
    );
    let historical_head = signed_head_version(&harness, "versioned.txt", &old_version).await;
    assert_eq!(historical_head.status(), StatusCode::OK);
    assert_eq!(historical_head.headers()["x-amz-version-id"], old_version);

    let copy = signed_copy_version(
        &harness,
        "versioned.txt",
        &old_version,
        "copied-historical.txt",
    )
    .await;
    assert_eq!(copy.status(), StatusCode::OK);
    assert_eq!(copy.headers()["x-amz-copy-source-version-id"], old_version);
    let copied_version = copy
        .headers()
        .get("x-amz-version-id")
        .expect("copied object version ID")
        .to_str()
        .expect("copied object version header text")
        .to_owned();
    assert_ne!(copied_version, old_version);
    let copied = signed_get(&harness, "copied-historical.txt").await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(copied.headers()["x-amz-version-id"], copied_version);
    assert_eq!(
        copied.bytes().await.expect("copied GET body").as_ref(),
        old_body
    );
}

#[tokio::test]
async fn versioning_tagging_selects_exact_content_and_reports_marker_errors() {
    let harness = start_harness(scripted(
        &["QmTaggingHistorical", "QmTaggingCurrent"],
        vec![
            ("QmTaggingHistorical", b"historical tags".to_vec()),
            ("QmTaggingCurrent", b"current tags".to_vec()),
        ],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let historical = signed_put_with_tagging(
        &harness,
        "versioned-tagging.txt",
        b"historical tags".to_vec(),
        "owner=historical",
    )
    .await;
    assert_eq!(historical.status(), StatusCode::OK);
    let historical_version = historical.headers()["x-amz-version-id"]
        .to_str()
        .expect("historical version ID")
        .to_owned();
    let current = signed_put_with_tagging(
        &harness,
        "versioned-tagging.txt",
        b"current tags".to_vec(),
        "owner=current",
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .expect("current version ID")
        .to_owned();

    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert_eq!(
        historical_tags.headers()["x-amz-version-id"],
        historical_version
    );
    assert_eq!(
        tagging_pairs(
            &historical_tags
                .text()
                .await
                .expect("historical tagging XML")
        ),
        vec![("owner".to_owned(), "historical".to_owned())]
    );

    let put_exact = signed_put_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        &[("owner", "historical-replaced")],
        Some(&historical_version),
    )
    .await;
    assert_eq!(put_exact.status(), StatusCode::OK);
    assert_eq!(put_exact.headers()["x-amz-version-id"], historical_version);
    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert_eq!(
        tagging_pairs(&historical_tags.text().await.expect("replaced tagging XML")),
        vec![("owner".to_owned(), "historical-replaced".to_owned())]
    );
    let current_tags = signed_get_object_tagging(&harness, "versioned-tagging.txt").await;
    assert_eq!(current_tags.status(), StatusCode::OK);
    assert_eq!(current_tags.headers()["x-amz-version-id"], current_version);
    assert_eq!(
        tagging_pairs(&current_tags.text().await.expect("current tagging XML")),
        vec![("owner".to_owned(), "current".to_owned())]
    );

    let delete_exact = signed_delete_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(delete_exact.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        delete_exact.headers()["x-amz-version-id"],
        historical_version
    );
    let historical_tags = signed_get_object_tagging_version(
        &harness,
        "versioned-tagging.txt",
        Some(&historical_version),
    )
    .await;
    assert_eq!(historical_tags.status(), StatusCode::OK);
    assert!(tagging_pairs(&historical_tags.text().await.expect("empty tagging XML")).is_empty());

    let marker = signed_delete_object(&harness, "versioned-tagging.txt").await;
    assert_eq!(marker.status(), StatusCode::NO_CONTENT);
    let marker_version = marker.headers()["x-amz-version-id"]
        .to_str()
        .expect("marker version ID")
        .to_owned();
    let current_marker = signed_get_object_tagging(&harness, "versioned-tagging.txt").await;
    assert_eq!(current_marker.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_marker.headers()["x-amz-version-id"], marker_version);
    assert!(
        current_marker
            .text()
            .await
            .expect("current marker error XML")
            .contains("NoSuchKey")
    );
    let exact_marker =
        signed_get_object_tagging_version(&harness, "versioned-tagging.txt", Some(&marker_version))
            .await;
    assert_eq!(exact_marker.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_marker.headers()["x-amz-version-id"], marker_version);
    assert!(
        exact_marker
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );
    assert!(
        exact_marker
            .text()
            .await
            .expect("explicit marker error XML")
            .contains("MethodNotAllowed")
    );
    assert_s3_error(
        signed_get_object_tagging_version(
            &harness,
            "versioned-tagging.txt",
            Some("00000000-0000-0000-0000-000000000001"),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "version not found",
    )
    .await;
}

#[tokio::test]
async fn versioning_delete_promotes_content_and_orders_multi_delete_effects() {
    let old_body = b"delete old body".to_vec();
    let current_body = b"delete current body".to_vec();
    let batch_body = b"delete batch body".to_vec();
    let harness = start_harness(scripted(
        &["QmDeleteOld", "QmDeleteCurrent", "QmDeleteBatch"],
        vec![
            ("QmDeleteOld", old_body.clone()),
            ("QmDeleteCurrent", current_body.clone()),
            ("QmDeleteBatch", batch_body),
        ],
    ))
    .await;

    let missing_unversioned = signed_delete_object(&harness, "missing.txt").await;
    assert_eq!(missing_unversioned.status(), StatusCode::NO_CONTENT);
    assert!(
        missing_unversioned
            .headers()
            .get("x-amz-version-id")
            .is_none()
    );
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let old = signed_put(
        &harness,
        "version-delete.txt",
        &[],
        old_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(old.status(), StatusCode::OK);
    let old_version = old.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let current = signed_put(
        &harness,
        "version-delete.txt",
        &[],
        current_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let simple = signed_delete_object(&harness, "version-delete.txt").await;
    assert_eq!(simple.status(), StatusCode::NO_CONTENT);
    assert_eq!(simple.headers()["x-amz-delete-marker"], "true");
    let marker_version = simple.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&marker_version).unwrap();

    let delete_marker =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&marker_version)).await;
    assert_eq!(delete_marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(delete_marker.headers()["x-amz-version-id"], marker_version);
    let promoted_current = signed_get(&harness, "version-delete.txt").await;
    assert_eq!(promoted_current.status(), StatusCode::OK);
    assert_eq!(
        promoted_current.bytes().await.unwrap().as_ref(),
        current_body
    );

    let delete_current =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&current_version)).await;
    assert_eq!(delete_current.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        delete_current.headers()["x-amz-version-id"],
        current_version
    );
    assert!(
        delete_current
            .headers()
            .get("x-amz-delete-marker")
            .is_none()
    );
    let promoted_old = signed_get(&harness, "version-delete.txt").await;
    assert_eq!(promoted_old.status(), StatusCode::OK);
    assert_eq!(promoted_old.bytes().await.unwrap().as_ref(), old_body);

    assert_s3_error(
        signed_delete_object_version(
            &harness,
            "version-delete.txt",
            Some("00000000-0000-0000-0000-000000000001"),
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "version not found",
    )
    .await;

    let batch = signed_put(
        &harness,
        "batch-version-delete.txt",
        &[],
        b"delete batch body".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(batch.status(), StatusCode::OK);
    let batch_version = batch.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let unknown = "00000000-0000-0000-0000-000000000002";
    let multi = signed_delete_object_versions(
        &harness,
        &[
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", Some(&batch_version)),
            ("invalid-version.txt", Some("not-a-version")),
            ("unknown-version.txt", Some(unknown)),
        ],
        false,
    )
    .await;
    assert_eq!(multi.status(), StatusCode::OK);
    let multi_xml = multi.text().await.unwrap();
    assert_eq!(
        xml_element_values(&multi_xml, "Key"),
        vec![
            "batch-version-delete.txt",
            "batch-version-delete.txt",
            "batch-version-delete.txt",
            "invalid-version.txt",
            "unknown-version.txt",
        ]
    );
    let marker_versions = xml_element_values(&multi_xml, "DeleteMarkerVersionId");
    assert_eq!(marker_versions.len(), 2);
    assert_ne!(marker_versions[0], marker_versions[1]);
    assert!(multi_xml.contains(&format!("<VersionId>{batch_version}</VersionId>")));
    assert!(multi_xml.contains("<Code>InvalidArgument</Code>"));
    assert!(multi_xml.contains("<Code>NoSuchVersion</Code>"));

    let before_quiet = store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(&harness.bucket))
        .filter(store::entities::object_version::Column::Key.eq("batch-version-delete.txt"))
        .count(harness.state.store.db())
        .await
        .unwrap();
    let quiet = signed_delete_object_versions(
        &harness,
        &[
            ("batch-version-delete.txt", None),
            ("batch-version-delete.txt", None),
            ("invalid-version.txt", Some("still-not-a-version")),
        ],
        true,
    )
    .await;
    assert_eq!(quiet.status(), StatusCode::OK);
    let quiet_xml = quiet.text().await.unwrap();
    assert!(!quiet_xml.contains("<Deleted>"));
    assert!(quiet_xml.contains("<Code>InvalidArgument</Code>"));
    assert_eq!(
        store::entities::object_version::Entity::find()
            .filter(store::entities::object_version::Column::Bucket.eq(&harness.bucket))
            .filter(store::entities::object_version::Column::Key.eq("batch-version-delete.txt"))
            .count(harness.state.store.db())
            .await
            .unwrap(),
        before_quiet + 2
    );
    assert!(
        harness
            .kubo
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.url.path() != "/api/v0/pin/rm")
    );

    let delete_old =
        signed_delete_object_version(&harness, "version-delete.txt", Some(&old_version)).await;
    assert_eq!(delete_old.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_old.headers()["x-amz-version-id"], old_version);
}

#[tokio::test]
async fn versioning_current_and_exact_plain_sse_s3_sse_c_read_matrix() {
    let plain_body = b"versioned plain body".to_vec();
    let sse_s3_body = b"versioned SSE-S3 body".to_vec();
    let sse_c_body = b"versioned SSE-C body".to_vec();
    let harness = start_harness(scripted(
        &["QmVersionPlain", "QmVersionSseS3", "QmVersionSseC"],
        vec![("QmVersionPlain", plain_body.clone())],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );

    let plain = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        plain_body.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(plain.status(), StatusCode::OK);
    let plain_version = plain.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();

    let mut sse_s3_headers = HeaderMap::new();
    sse_s3_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let sse_s3 = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        sse_s3_body.clone(),
        sse_s3_headers,
    )
    .await;
    assert_eq!(sse_s3.status(), StatusCode::OK);
    let sse_s3_version = sse_s3.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    harness.set_cat_body(
        "QmVersionSseS3",
        harness.captured_add_file_bytes()[1].clone(),
    );

    let sse_c = signed_put(
        &harness,
        "encrypted-versions.bin",
        &[],
        sse_c_body.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(sse_c.status(), StatusCode::OK);
    let sse_c_version = sse_c.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    harness.set_cat_body(
        "QmVersionSseC",
        harness.captured_add_file_bytes()[2].clone(),
    );

    let cases = [
        (
            plain_version.as_str(),
            plain_body.as_slice(),
            HeaderMap::new(),
            None,
        ),
        (
            sse_s3_version.as_str(),
            sse_s3_body.as_slice(),
            HeaderMap::new(),
            Some("AES256"),
        ),
        (
            sse_c_version.as_str(),
            sse_c_body.as_slice(),
            sse_c_headers_for([7; 32]),
            None,
        ),
    ];
    for (version_id, expected_body, headers, expected_sse_s3) in cases {
        let get = signed_get_version_with_headers(
            &harness,
            "encrypted-versions.bin",
            version_id,
            headers.clone(),
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK, "exact GET {version_id}");
        assert_eq!(get.headers()["x-amz-version-id"], version_id);
        assert_eq!(
            get.headers()
                .get("x-amz-server-side-encryption")
                .map(|value| value.to_str().unwrap()),
            expected_sse_s3
        );
        if !headers.is_empty() {
            assert_eq!(
                get.headers()["x-amz-server-side-encryption-customer-algorithm"],
                "AES256"
            );
        }
        assert_eq!(get.bytes().await.unwrap().as_ref(), expected_body);

        let head = signed_head_version_with_headers(
            &harness,
            "encrypted-versions.bin",
            version_id,
            headers,
        )
        .await;
        assert_eq!(head.status(), StatusCode::OK, "exact HEAD {version_id}");
        assert_eq!(head.headers()["x-amz-version-id"], version_id);
    }

    let current_get = signed_get_with_headers(
        &harness,
        "encrypted-versions.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(current_get.status(), StatusCode::OK);
    assert_eq!(current_get.headers()["x-amz-version-id"], sse_c_version);
    assert_eq!(current_get.bytes().await.unwrap().as_ref(), sse_c_body);
    let current_head = signed_head_with_headers(
        &harness,
        "encrypted-versions.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(current_head.headers()["x-amz-version-id"], sse_c_version);
}

#[tokio::test]
async fn versioning_suspend_replaces_null_reenable_unique_and_bucket_cleanup() {
    let harness = start_harness(scripted(
        &[
            "QmSuspendOpaqueOne",
            "QmSuspendOpaqueTwo",
            "QmSuspendNullOne",
            "QmSuspendNullTwo",
            "QmSuspendReenabled",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let mut opaque_ids = Vec::new();
    for body in [b"opaque one".as_slice(), b"opaque two".as_slice()] {
        let response = signed_put(
            &harness,
            "suspend.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        opaque_ids.push(
            response.headers()["x-amz-version-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
    }
    assert_ne!(opaque_ids[0], opaque_ids[1]);

    assert_eq!(
        signed_put_bucket_versioning(&harness, "Suspended")
            .await
            .status(),
        StatusCode::OK
    );
    for body in [b"null one".as_slice(), b"null two".as_slice()] {
        let response = signed_put(
            &harness,
            "suspend.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-amz-version-id"], "null");
    }
    let suspended_rows =
        publication_matrix_rows(&harness.state, &harness.bucket, "suspend.txt").await;
    assert_eq!(suspended_rows.len(), 3);
    assert_eq!(
        suspended_rows
            .iter()
            .filter(|row| row.version_id.is_none())
            .count(),
        1
    );
    for opaque_id in &opaque_ids {
        assert!(
            suspended_rows
                .iter()
                .any(|row| row.version_id.as_deref() == Some(opaque_id.as_str()))
        );
    }

    let list = signed_list_object_versions(&harness, &[]).await;
    assert_eq!(
        list.status(),
        StatusCode::OK,
        "suspended version list status"
    );
    let xml = list.text().await.expect("suspended version list body");
    let version_count = xml.matches("<Version>").count();
    let delete_marker_count = xml.matches("<DeleteMarker>").count();
    let null_version_count = xml.matches("<VersionId>null</VersionId>").count();
    let latest_count = xml.matches("<IsLatest>true</IsLatest>").count();
    assert_eq!(
        version_count, 3,
        "unexpected Version entry count: {version_count}"
    );
    assert_eq!(
        delete_marker_count, 0,
        "unexpected DeleteMarker entry count: {delete_marker_count}"
    );
    assert_eq!(
        null_version_count, 1,
        "unexpected literal null VersionId count: {null_version_count}"
    );
    for opaque_id in &opaque_ids {
        let opaque_version_count = xml
            .matches(&format!("<VersionId>{opaque_id}</VersionId>"))
            .count();
        assert_eq!(
            opaque_version_count, 1,
            "unexpected opaque VersionId entry count: {opaque_version_count}"
        );
    }
    assert_eq!(
        latest_count, 1,
        "unexpected latest Version entry count: {latest_count}"
    );
    let latest_entry = xml
        .split("<Version>")
        .skip(1)
        .filter_map(|entry| entry.split_once("</Version>").map(|(entry, _)| entry))
        .find(|entry| entry.contains("<IsLatest>true</IsLatest>"))
        .expect("latest Version entry");
    assert!(
        latest_entry.contains("<VersionId>null</VersionId>"),
        "latest Version entry must use literal null VersionId"
    );

    let marker = signed_delete_object(&harness, "suspend.txt").await;
    assert_eq!(marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(marker.headers()["x-amz-delete-marker"], "true");
    assert_eq!(marker.headers()["x-amz-version-id"], "null");
    let delete_marker = signed_delete_object_version(&harness, "suspend.txt", Some("null")).await;
    assert_eq!(delete_marker.status(), StatusCode::NO_CONTENT);
    assert_eq!(delete_marker.headers()["x-amz-delete-marker"], "true");
    let restored = signed_head(&harness, "suspend.txt", None).await;
    assert_eq!(restored.status(), StatusCode::OK);
    assert_eq!(restored.headers()["x-amz-version-id"], opaque_ids[1]);

    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let reenabled = signed_put(
        &harness,
        "suspend.txt",
        &[],
        b"re-enabled".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(reenabled.status(), StatusCode::OK);
    let reenabled_id = reenabled.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&reenabled_id).unwrap();
    assert!(!opaque_ids.contains(&reenabled_id));

    assert_s3_error(
        signed_delete_bucket(&harness).await,
        StatusCode::CONFLICT,
        "BucketNotEmpty",
        "",
    )
    .await;
    for version_id in [
        reenabled_id.as_str(),
        opaque_ids[1].as_str(),
        opaque_ids[0].as_str(),
    ] {
        let response =
            signed_delete_object_version(&harness, "suspend.txt", Some(version_id)).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    assert!(
        publication_matrix_rows(&harness.state, &harness.bucket, "suspend.txt")
            .await
            .is_empty()
    );
    assert_eq!(
        signed_delete_bucket(&harness).await.status(),
        StatusCode::NO_CONTENT
    );
}

async fn marker_nonversion_side_effect_counts(state: &Arc<AppState>) -> [u64; 7] {
    [
        store::entities::object::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::object_tag::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::pin_provider_usage::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        store::entities::remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
    ]
}

#[tokio::test]
async fn versioning_marker_errors_are_exact_zero_kubo_and_zero_content_side_effects() {
    let harness = start_harness(scripted(&["QmMarkerContent"], vec![])).await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let put = signed_put_with_tagging(
        &harness,
        "marker.txt",
        b"marker content".to_vec(),
        "fixture=marker",
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let counts_before = marker_nonversion_side_effect_counts(&harness.state).await;
    let kubo_before = harness.kubo.received_requests().await.unwrap().len();

    let deleted = signed_delete_object(&harness, "marker.txt").await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    assert_eq!(deleted.headers()["x-amz-delete-marker"], "true");
    let marker_id = deleted.headers()["x-amz-version-id"]
        .to_str()
        .unwrap()
        .to_owned();
    uuid::Uuid::parse_str(&marker_id).unwrap();
    assert_eq!(
        marker_nonversion_side_effect_counts(&harness.state).await,
        counts_before,
        "marker creation owns no content/tag/pin rows"
    );

    let current_get = signed_get(&harness, "marker.txt").await;
    assert_eq!(current_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_get.headers()["x-amz-version-id"], marker_id);
    assert!(
        current_get
            .text()
            .await
            .unwrap()
            .contains("<Code>NoSuchKey</Code>")
    );
    let current_head = signed_head(&harness, "marker.txt", None).await;
    assert_eq!(current_head.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_head.headers()["x-amz-delete-marker"], "true");
    assert_eq!(current_head.headers()["x-amz-version-id"], marker_id);

    let exact_get = signed_get_version(&harness, "marker.txt", &marker_id).await;
    assert_eq!(exact_get.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_get.headers()["x-amz-version-id"], marker_id);
    assert!(
        exact_get
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );
    assert!(
        exact_get
            .text()
            .await
            .unwrap()
            .contains("<Code>MethodNotAllowed</Code>")
    );
    let exact_head = signed_head_version(&harness, "marker.txt", &marker_id).await;
    assert_eq!(exact_head.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_head.headers()["x-amz-delete-marker"], "true");
    assert_eq!(exact_head.headers()["x-amz-version-id"], marker_id);
    assert!(
        exact_head
            .headers()
            .get(http::header::LAST_MODIFIED)
            .is_some()
    );

    let current_copy = signed_copy(
        &harness,
        "marker.txt",
        "marker-copy-current.txt",
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current_copy.status(), StatusCode::NOT_FOUND);
    assert_eq!(current_copy.headers()["x-amz-delete-marker"], "true");
    assert!(current_copy.text().await.unwrap().contains("NoSuchKey"));
    let exact_copy =
        signed_copy_version(&harness, "marker.txt", &marker_id, "marker-copy-exact.txt").await;
    assert_eq!(exact_copy.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(exact_copy.headers()["x-amz-delete-marker"], "true");
    assert!(
        exact_copy
            .text()
            .await
            .unwrap()
            .contains("MethodNotAllowed")
    );

    let current_tags = signed_get_object_tagging(&harness, "marker.txt").await;
    assert_eq!(current_tags.status(), StatusCode::NOT_FOUND);
    let exact_tags =
        signed_get_object_tagging_version(&harness, "marker.txt", Some(&marker_id)).await;
    assert_eq!(exact_tags.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        marker_nonversion_side_effect_counts(&harness.state).await,
        counts_before
    );
    assert_eq!(
        harness.kubo.received_requests().await.unwrap().len(),
        kubo_before,
        "marker create/read/head/copy/tag paths never touch Kubo"
    );
}

#[tokio::test]
async fn versioning_signed_listing_url_delimiter_and_error_matrix() {
    let harness = start_harness(scripted(
        &[
            "QmListingSpaceOld",
            "QmListingSpaceNew",
            "QmListingNested",
            "QmListingZed",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&harness, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let mut spaced_versions = Vec::new();
    for body in [b"old".as_slice(), b"new".as_slice()] {
        let response = signed_put(
            &harness,
            "pre/a b.txt",
            &[],
            body.to_vec(),
            HeaderMap::new(),
        )
        .await;
        spaced_versions.push(
            response.headers()["x-amz-version-id"]
                .to_str()
                .unwrap()
                .to_owned(),
        );
    }
    for (key, body) in [
        ("pre/sub/c.txt", b"nested".as_slice()),
        ("z.txt", b"zed".as_slice()),
    ] {
        assert_eq!(
            signed_put(&harness, key, &[], body.to_vec(), HeaderMap::new())
                .await
                .status(),
            StatusCode::OK
        );
    }

    let list = signed_list_object_versions(
        &harness,
        &[
            ("prefix", "pre/"),
            ("delimiter", "/"),
            ("encoding-type", "url"),
            ("max-keys", "3"),
        ],
    )
    .await;
    assert_eq!(list.status(), StatusCode::OK);
    let xml = list.text().await.unwrap();
    assert!(xml.contains("<EncodingType>url</EncodingType>"));
    assert!(xml.contains("<Prefix>pre%2F</Prefix>"));
    assert!(xml.contains("<Delimiter>%2F</Delimiter>"));
    assert_eq!(xml.matches("<Key>pre%2Fa%20b.txt</Key>").count(), 2);
    assert!(xml.contains("<Prefix>pre%2Fsub%2F</Prefix>"));

    assert_s3_error(
        signed_list_object_versions(&harness, &[("version-id-marker", &spaced_versions[0])]).await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "",
    )
    .await;
    assert_s3_error(
        signed_list_object_versions(
            &harness,
            &[
                ("key-marker", "pre/sub/c.txt"),
                ("version-id-marker", &spaced_versions[0]),
            ],
        )
        .await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "",
    )
    .await;
    assert_s3_error(
        signed_get_version(&harness, "pre/a b.txt", "not-a-version").await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "canonical UUID",
    )
    .await;
    assert_s3_error(
        signed_get_version(
            &harness,
            "pre/a b.txt",
            "00000000-0000-0000-0000-000000000001",
        )
        .await,
        StatusCode::NOT_FOUND,
        "NoSuchVersion",
        "",
    )
    .await;
    assert_s3_error(
        signed_get(&harness, "missing-key.txt").await,
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "",
    )
    .await;
    let missing_bucket = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "missing-version-bucket".to_owned(),
    };
    assert_s3_error(
        signed_list_object_versions(&missing_bucket, &[]).await,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "",
    )
    .await;

    let unversioned = start_harness(scripted(&[], vec![])).await;
    assert_s3_error(
        signed_get_version(&unversioned, "missing-key.txt", "null").await,
        StatusCode::BAD_REQUEST,
        "InvalidArgument",
        "unversioned bucket",
    )
    .await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationMatrixState {
    Unversioned,
    Enabled,
    Suspended,
}

impl PublicationMatrixState {
    fn label(self) -> &'static str {
        match self {
            Self::Unversioned => "unversioned",
            Self::Enabled => "enabled",
            Self::Suspended => "suspended",
        }
    }
}

#[derive(Debug)]
struct PublicationMatrixBefore {
    key: String,
    rows: Vec<store::entities::object_version::Model>,
}

#[derive(Debug)]
enum PublicationVersionSurface {
    Header(Option<String>),
    DiscoverOnly,
}

fn response_version_surface(response: &reqwest::Response) -> PublicationVersionSurface {
    PublicationVersionSurface::Header(
        response
            .headers()
            .get("x-amz-version-id")
            .map(|value| value.to_str().expect("version response header").to_owned()),
    )
}

async fn publication_matrix_rows(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
) -> Vec<store::entities::object_version::Model> {
    store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(bucket))
        .filter(store::entities::object_version::Column::Key.eq(key))
        .order_by_asc(store::entities::object_version::Column::Sequence)
        .all(state.store.db())
        .await
        .expect("load publication matrix version rows")
}

async fn prepare_publication_matrix_state(
    harness: &impl S3TestEndpoint,
    state: &Arc<AppState>,
    matrix_state: PublicationMatrixState,
    keys_and_tagging: &[(&str, &str)],
) -> Vec<PublicationMatrixBefore> {
    if matrix_state == PublicationMatrixState::Enabled {
        let response = signed_put_bucket_versioning(harness, "Enabled").await;
        assert_eq!(response.status(), StatusCode::OK, "enable matrix bucket");
    }

    let publish_baselines = async {
        for (key, tagging) in keys_and_tagging {
            let response = signed_put_with_tagging(
                harness,
                key,
                format!("baseline for {key}").into_bytes(),
                tagging,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{} baseline PUT for {key}",
                matrix_state.label()
            );
        }
    };

    if matrix_state == PublicationMatrixState::Suspended {
        publish_baselines.await;
        let response = signed_put_bucket_versioning(harness, "Suspended").await;
        assert_eq!(response.status(), StatusCode::OK, "suspend matrix bucket");
        for (key, tagging) in keys_and_tagging {
            let response = signed_put_with_tagging(
                harness,
                key,
                format!("null baseline for {key}").into_bytes(),
                tagging,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "suspended null baseline PUT for {key}"
            );
            assert_eq!(response.headers()["x-amz-version-id"], "null");
        }
    } else {
        publish_baselines.await;
    }

    let mut before = Vec::with_capacity(keys_and_tagging.len());
    for (key, _) in keys_and_tagging {
        let rows = publication_matrix_rows(state, harness.bucket(), key).await;
        let expected_rows = match matrix_state {
            PublicationMatrixState::Unversioned | PublicationMatrixState::Enabled => 1,
            PublicationMatrixState::Suspended => 2,
        };
        assert_eq!(
            rows.len(),
            expected_rows,
            "{} baseline index rows for {key}",
            matrix_state.label()
        );
        assert_eq!(rows.iter().filter(|row| row.is_latest).count(), 1);
        match matrix_state {
            PublicationMatrixState::Unversioned => assert!(rows[0].version_id.is_none()),
            PublicationMatrixState::Enabled => {
                uuid::Uuid::parse_str(rows[0].version_id.as_deref().unwrap())
                    .expect("enabled baseline public UUID");
            }
            PublicationMatrixState::Suspended => {
                uuid::Uuid::parse_str(rows[0].version_id.as_deref().unwrap())
                    .expect("suspended retained opaque public UUID");
                assert!(rows[1].version_id.is_none());
            }
        }
        before.push(PublicationMatrixBefore {
            key: (*key).to_owned(),
            rows,
        });
    }
    before
}

async fn object_fixture_tag(state: &Arc<AppState>, object_id: &str) -> Option<String> {
    store::entities::object_tag::Entity::find()
        .filter(store::entities::object_tag::Column::ObjectId.eq(object_id))
        .filter(store::entities::object_tag::Column::Key.eq("fixture"))
        .one(state.store.db())
        .await
        .expect("load fixture object tag")
        .map(|tag| tag.value)
}

#[allow(clippy::too_many_arguments)]
async fn assert_publication_matrix_object(
    harness: &impl S3TestEndpoint,
    state: &Arc<AppState>,
    matrix_state: PublicationMatrixState,
    before: &PublicationMatrixBefore,
    expected_cid: &str,
    expected_multipart: bool,
    expected_encrypted: bool,
    expected_fixture_tag: Option<&str>,
    version_surface: PublicationVersionSurface,
) -> String {
    let rows = publication_matrix_rows(state, harness.bucket(), &before.key).await;
    let expected_rows = match matrix_state {
        PublicationMatrixState::Unversioned => 1,
        PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => 2,
    };
    assert_eq!(
        rows.len(),
        expected_rows,
        "{} post-publication index rows for {}",
        matrix_state.label(),
        before.key
    );
    assert_eq!(rows.iter().filter(|row| row.is_latest).count(), 1);
    assert_eq!(
        rows.iter().filter(|row| row.version_id.is_none()).count(),
        usize::from(matrix_state != PublicationMatrixState::Enabled),
        "{} null-slot count for {}",
        matrix_state.label(),
        before.key
    );

    let current = rows
        .iter()
        .find(|row| row.is_latest)
        .expect("one latest publication row");
    assert_eq!(current.kind, "object");
    assert_eq!(
        current.sequence,
        before
            .rows
            .iter()
            .map(|row| row.sequence)
            .max()
            .expect("baseline sequence")
            + 1
    );
    let object_id = current
        .object_id
        .as_deref()
        .expect("content version internal owner ID");
    uuid::Uuid::parse_str(object_id).expect("internal object UUID");
    let public_version_id = current.version_id.as_deref().unwrap_or("null");
    if let Some(opaque) = current.version_id.as_deref() {
        uuid::Uuid::parse_str(opaque).expect("opaque public version UUID");
    }
    assert_ne!(public_version_id, object_id);

    match version_surface {
        PublicationVersionSurface::Header(actual) => match matrix_state {
            PublicationMatrixState::Unversioned => assert_eq!(actual, None),
            PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => {
                assert_eq!(actual.as_deref(), Some(public_version_id));
            }
        },
        PublicationVersionSurface::DiscoverOnly => {}
    }

    let object = store::object::get_by_id(state.store.db(), object_id)
        .await
        .expect("load matrix publication object");
    assert_eq!(object.bucket, harness.bucket());
    assert_eq!(object.key, before.key);
    assert_eq!(object.cid, expected_cid);
    assert_eq!(object.etag, expected_cid);
    assert_eq!(object.multipart, expected_multipart);
    assert_eq!(object.encrypted, expected_encrypted);
    assert_eq!(object.key_wrap.is_some(), expected_encrypted);
    assert!(object.is_latest);
    assert_eq!(
        object_fixture_tag(state, object_id).await.as_deref(),
        expected_fixture_tag
    );

    let projected = store::object::get_latest(state.store.db(), harness.bucket(), &before.key)
        .await
        .expect("ordinary current projection");
    assert_eq!(projected.id, object_id);
    assert_eq!(projected.cid, expected_cid);

    let current_head = signed_head(harness, &before.key, None).await;
    assert_eq!(current_head.status(), StatusCode::OK);
    assert_eq!(
        current_head.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"'),
        expected_cid
    );
    match matrix_state {
        PublicationMatrixState::Unversioned => {
            assert!(current_head.headers().get("x-amz-version-id").is_none())
        }
        PublicationMatrixState::Enabled | PublicationMatrixState::Suspended => {
            assert_eq!(
                current_head.headers()["x-amz-version-id"],
                public_version_id
            );
            let exact = signed_head_version(harness, &before.key, public_version_id).await;
            assert_eq!(exact.status(), StatusCode::OK);
            assert_eq!(exact.headers()["x-amz-version-id"], public_version_id);
            assert_eq!(
                exact.headers()[http::header::ETAG]
                    .to_str()
                    .unwrap()
                    .trim_matches('"'),
                expected_cid
            );
        }
    }

    let previous_object_ids = before
        .rows
        .iter()
        .map(|row| row.object_id.as_deref().expect("baseline content owner"))
        .collect::<Vec<_>>();
    for previous_id in &previous_object_ids {
        let previous = store::object::get_by_id(state.store.db(), previous_id)
            .await
            .expect("retained immutable baseline object");
        assert!(!previous.is_latest);
    }
    match matrix_state {
        PublicationMatrixState::Unversioned => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0]).await,
                None
            );
            assert!(
                rows.iter()
                    .all(|row| row.object_id.as_deref() != Some(previous_object_ids[0]))
            );
        }
        PublicationMatrixState::Enabled => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0])
                    .await
                    .as_deref(),
                Some("baseline")
            );
            assert!(
                rows.iter()
                    .any(|row| row.object_id.as_deref() == Some(previous_object_ids[0]))
            );
        }
        PublicationMatrixState::Suspended => {
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[0])
                    .await
                    .as_deref(),
                Some("baseline")
            );
            assert_eq!(
                object_fixture_tag(state, previous_object_ids[1]).await,
                None
            );
            assert!(
                rows.iter()
                    .any(|row| row.object_id.as_deref() == Some(previous_object_ids[0]))
            );
            assert!(
                rows.iter()
                    .all(|row| row.object_id.as_deref() != Some(previous_object_ids[1]))
            );
        }
    }

    object_id.to_owned()
}

async fn assert_publication_matrix_pinning(
    harness: &PinningHarness,
    matrix_state: PublicationMatrixState,
    before: &PublicationMatrixBefore,
    current_object_id: &str,
) {
    let current_leases = owner_leases(harness, current_object_id).await;
    assert_eq!(current_leases.len(), 1, "one current manual lease");
    let current = &current_leases[0];
    assert_eq!(current.owner_object_id, current_object_id);
    assert_eq!(current.state, "active");
    let targets = lease_targets(harness, &current.id).await;
    assert!(!targets.is_empty(), "current lease has provider targets");
    assert!(targets.iter().all(|target| target.lease_id == current.id));
    assert!(
        harness
            .pin_jobs()
            .await
            .iter()
            .any(|job| job.lease_id.as_deref() == Some(current.id.as_str()))
    );
    let usage = harness
        .provider_usage(&harness.provider_key("pinata-primary"))
        .await;
    assert!(usage.reserved_pins > 0);
    assert!(usage.reserved_bytes > 0);

    let previous_ids = before
        .rows
        .iter()
        .map(|row| row.object_id.as_deref().expect("baseline lease owner"))
        .collect::<Vec<_>>();
    match matrix_state {
        PublicationMatrixState::Unversioned => assert!(
            owner_leases(harness, previous_ids[0])
                .await
                .iter()
                .all(|lease| lease.state == "cancelled")
        ),
        PublicationMatrixState::Enabled => assert!(
            owner_leases(harness, previous_ids[0])
                .await
                .iter()
                .all(|lease| lease.state == "active")
        ),
        PublicationMatrixState::Suspended => {
            assert!(
                owner_leases(harness, previous_ids[0])
                    .await
                    .iter()
                    .all(|lease| lease.state == "active")
            );
            assert!(
                owner_leases(harness, previous_ids[1])
                    .await
                    .iter()
                    .all(|lease| lease.state == "cancelled")
            );
        }
    }
}

async fn assert_public_ids_are_external(state: &Arc<AppState>, bucket: &str) {
    let rows = store::entities::object_version::Entity::find()
        .filter(store::entities::object_version::Column::Bucket.eq(bucket))
        .all(state.store.db())
        .await
        .expect("load all matrix version rows");
    let public_ids = rows
        .iter()
        .filter_map(|row| row.version_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        public_ids.iter().collect::<BTreeSet<_>>().len(),
        public_ids.len(),
        "every opaque public version ID is distinct"
    );
    let internal_ids = store::entities::object::Entity::find()
        .filter(store::entities::object::Column::Bucket.eq(bucket))
        .all(state.store.db())
        .await
        .expect("load all matrix internal objects")
        .into_iter()
        .map(|object| object.id)
        .collect::<BTreeSet<_>>();
    assert!(
        public_ids
            .iter()
            .all(|public_id| !internal_ids.contains(public_id)),
        "public version IDs never alias internal ownership IDs"
    );
}

async fn assert_no_remote_publication_rows(state: &Arc<AppState>) {
    assert_eq!(
        store::entities::pin_lease::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_job::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::pin_provider_usage::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store::entities::remote_pin::Entity::find()
            .count(state.store.db())
            .await
            .unwrap(),
        0
    );
}

async fn signed_post_import_matrix(
    harness: &impl S3TestEndpoint,
    key: &str,
    query: &[(&str, &str)],
    xml: String,
    tagging: &str,
) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    headers.insert(
        "x-amz-tagging",
        HeaderValue::from_str(tagging).expect("matrix import tagging header"),
    );
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        key,
        query,
        xml.into_bytes(),
        headers,
        "test",
    )
    .await
}

fn matrix_pinning_config(
    add_replies: Vec<AddReply>,
    cat_bodies: HashMap<String, Vec<u8>>,
) -> PinningHarnessConfig {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies,
        cat_bodies,
    };
    config.pinata_script.clear();
    config
}

const MATRIX_BASELINE_PIN_TAGS: &str = "fixture=baseline&ipfs-s3%3Apin=true";
const MATRIX_ACTUAL_PIN_TAGS: &str = "fixture=actual&ipfs-s3%3Apin=true";
const MATRIX_ZIP_PIN_TAGS: &str =
    "fixture=actual&ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versioning_all_publication_paths() {
    versioning_publication_path_put().await;
    versioning_publication_path_copy().await;
    versioning_publication_path_multipart().await;
    versioning_publication_path_direct_cid_import().await;
    versioning_publication_path_direct_https_import().await;
    versioning_publication_path_import_zip().await;
    versioning_publication_path_direct_zip().await;
    versioning_publication_path_multipart_zip().await;
}

async fn versioning_publication_path_put() {
    let config = matrix_pinning_config(
        [
            "QmPutUBase",
            "QmPutU",
            "QmPutEBase",
            "QmPutE",
            "QmPutSOpaque",
            "QmPutSNull",
            "QmPutS",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::new(),
    );
    let harness = start_pinning_harness(config).await;
    for (matrix_state, expected_cid) in [
        (PublicationMatrixState::Unversioned, "QmPutU"),
        (PublicationMatrixState::Enabled, "QmPutE"),
        (PublicationMatrixState::Suspended, "QmPutS"),
    ] {
        let key = format!("matrix/{}/put.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let response = signed_put_with_tagging(
            &harness,
            &key,
            format!("actual {} PUT", matrix_state.label()).into_bytes(),
            MATRIX_ACTUAL_PIN_TAGS,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[http::header::ETAG]
                .to_str()
                .unwrap()
                .trim_matches('"'),
            expected_cid
        );
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn versioning_publication_path_copy() {
    let config = matrix_pinning_config(
        [
            "QmCopyUBase",
            "QmCopyUSource",
            "QmCopyEBase",
            "QmCopyESource",
            "QmCopySOpaque",
            "QmCopySNull",
            "QmCopySSource",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::new(),
    );
    let harness = start_pinning_harness(config).await;
    for (matrix_state, expected_cid) in [
        (PublicationMatrixState::Unversioned, "QmCopyUSource"),
        (PublicationMatrixState::Enabled, "QmCopyESource"),
        (PublicationMatrixState::Suspended, "QmCopySSource"),
    ] {
        let destination = format!("matrix/{}/copy.bin", matrix_state.label());
        let source = format!("matrix/{}/copy-source.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&destination, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let source_put = signed_put(
            &harness,
            &source,
            &[],
            format!("copy source {}", matrix_state.label()).into_bytes(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(source_put.status(), StatusCode::OK);
        let response =
            signed_copy_with_tagging(&harness, &source, &destination, MATRIX_ACTUAL_PIN_TAGS).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn versioning_publication_path_multipart() {
    let mut config = matrix_pinning_config(
        [
            "QmMultipartUBase",
            "QmMultipartUPart",
            "QmMultipartURoot",
            "QmMultipartEBase",
            "QmMultipartEPart",
            "QmMultipartERoot",
            "QmMultipartSOpaque",
            "QmMultipartSNull",
            "QmMultipartSPart",
            "QmMultipartSRoot",
        ]
        .into_iter()
        .map(AddReply::Ok)
        .collect(),
        HashMap::from([
            ("QmMultipartUPart".to_owned(), b"multipart U".to_vec()),
            ("QmMultipartEPart".to_owned(), b"multipart E".to_vec()),
            ("QmMultipartSPart".to_owned(), b"multipart S".to_vec()),
        ]),
    );
    config.explicit_identity = true;
    let harness = start_pinning_harness(config).await;
    for (matrix_state, part_body, expected_cid) in [
        (
            PublicationMatrixState::Unversioned,
            b"multipart U".as_slice(),
            "QmMultipartURoot",
        ),
        (
            PublicationMatrixState::Enabled,
            b"multipart E".as_slice(),
            "QmMultipartERoot",
        ),
        (
            PublicationMatrixState::Suspended,
            b"multipart S".as_slice(),
            "QmMultipartSRoot",
        ),
    ] {
        let key = format!("matrix/{}/multipart.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, MATRIX_BASELINE_PIN_TAGS)],
        )
        .await;
        let create =
            signed_create_multipart_upload_with_tagging(&harness, &key, MATRIX_ACTUAL_PIN_TAGS)
                .await;
        assert_eq!(create.status(), StatusCode::OK);
        let upload_id = xml_element_text(&create.text().await.unwrap(), "UploadId");
        let part = signed_upload_part(&harness, &key, &upload_id, 1, part_body.to_vec()).await;
        assert_eq!(part.status(), StatusCode::OK);
        let etag = part.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let response = signed_complete_multipart(&harness, &key, &upload_id, 1, &etag).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            true,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
        assert!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .is_err()
        );
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn versioning_publication_path_direct_cid_import() {
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: [
                "QmCidImportUBase",
                "QmCidImportEBase",
                "QmCidImportSOpaque",
                "QmCidImportSNull",
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: HashMap::from([(
                IMPORT_CID.to_owned(),
                b"matrix CID import body".to_vec(),
            )]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for matrix_state in [
        PublicationMatrixState::Unversioned,
        PublicationMatrixState::Enabled,
        PublicationMatrixState::Suspended,
    ] {
        let key = format!("matrix/{}/cid-import.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, "fixture=baseline")],
        )
        .await;
        let response = signed_post_import_matrix(
            &harness,
            &key,
            &[("ipfs3-import", "")],
            format!("<IPFS3ImportRequest><CID>{IMPORT_CID}</CID></IPFS3ImportRequest>"),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let job = wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_eq!(job.final_cid.as_deref(), Some(IMPORT_CID));
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            IMPORT_CID,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_eq!(import_result_count(&harness, &job_id).await, 1);
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

async fn versioning_publication_path_direct_https_import() {
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: [
                "QmHttpsUBase",
                IMPORT_TEST_CID_V0,
                "QmHttpsEBase",
                IMPORT_TEST_CID_V0,
                "QmHttpsSOpaque",
                "QmHttpsSNull",
                IMPORT_TEST_CID_V0,
            ]
            .into_iter()
            .map(AddReply::Ok)
            .collect(),
            cat_bodies: HashMap::from([(
                IMPORT_TEST_CID_V0.to_owned(),
                b"HTTPS matrix body".to_vec(),
            )]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for (matrix_state, path, body, expected_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "/matrix-https-u",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
        (
            PublicationMatrixState::Enabled,
            "/matrix-https-e",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
        (
            PublicationMatrixState::Suspended,
            "/matrix-https-s",
            b"HTTPS matrix body".as_slice(),
            IMPORT_TEST_CID_V0,
        ),
    ] {
        let key = format!("matrix/{}/https-import.bin", matrix_state.label());
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[(&key, "fixture=baseline")],
        )
        .await;
        harness
            .source
            .set_reply(path, TestHttpsReply::chunked(body.to_vec()));
        let response = signed_post_import_matrix(
            &harness,
            &key,
            &[("ipfs3-import", "")],
            import_url_xml(&harness.source.url(path)),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            expected_cid,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_eq!(import_result_count(&harness, &job_id).await, 1);
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

async fn versioning_publication_path_import_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmImportZipUArchiveBase",
        "QmImportZipUEntryBase",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
        "QmImportZipEArchiveBase",
        "QmImportZipEEntryBase",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
        "QmImportZipSOpaqueArchive",
        "QmImportZipSOpaqueEntry",
        "QmImportZipSNullArchive",
        "QmImportZipSNullEntry",
        IMPORT_TEST_CID_V0,
        IMPORT_CID,
    ];
    let config = ImportHarnessConfig {
        kubo_script: KuboScript {
            add_replies: add_cids.into_iter().map(AddReply::Ok).collect(),
            cat_bodies: HashMap::from([
                (IMPORT_TEST_CID_V0.to_owned(), archive.clone()),
                (IMPORT_CID.to_owned(), SINGLE_ENTRY_BYTES.to_vec()),
            ]),
        },
        ..Default::default()
    };
    let harness = start_import_harness(config).await;
    for (matrix_state, path, archive_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "/matrix-import-zip-u",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
        (
            PublicationMatrixState::Enabled,
            "/matrix-import-zip-e",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
        (
            PublicationMatrixState::Suspended,
            "/matrix-import-zip-s",
            IMPORT_TEST_CID_V0,
            IMPORT_CID,
        ),
    ] {
        let archive_key = format!("matrix/{}/import.zip", matrix_state.label());
        let prefix = format!("matrix/{}/import-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, "fixture=baseline"),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        assert_no_remote_publication_rows(&harness.state).await;
        harness
            .source
            .set_reply(path, TestHttpsReply::chunked(archive.clone()));
        let response = signed_post_import_matrix(
            &harness,
            &archive_key,
            &[("ipfs3-import", ""), ("decompress-zip", &prefix)],
            import_url_xml(&harness.source.url(path)),
            "fixture=actual",
        )
        .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get("x-amz-version-id").is_none());
        let job_id = response.headers()["x-ipfs3-import-job-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let job = wait_for_import_state(&harness, &job_id, &["completed"]).await;
        assert_eq!(job.entries_succeeded, 1);
        assert_eq!(import_result_count(&harness, &job_id).await, 2);
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            archive_cid,
            false,
            false,
            Some("actual"),
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_no_remote_publication_rows(&harness.state).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert!(harness.kubo_args("/api/v0/pin/rm").await.is_empty());
    harness.shutdown().await;
}

async fn versioning_publication_path_direct_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmDirectZipUArchiveBase",
        "QmDirectZipUEntryBase",
        "QmDirectZipUArchive",
        "QmDirectZipUEntry",
        "QmDirectZipEArchiveBase",
        "QmDirectZipEEntryBase",
        "QmDirectZipEArchive",
        "QmDirectZipEEntry",
        "QmDirectZipSOpaqueArchive",
        "QmDirectZipSOpaqueEntry",
        "QmDirectZipSNullArchive",
        "QmDirectZipSNullEntry",
        "QmDirectZipSArchive",
        "QmDirectZipSEntry",
    ];
    let config = matrix_pinning_config(
        add_cids.into_iter().map(AddReply::Ok).collect(),
        HashMap::from([
            ("QmDirectZipUArchive".to_owned(), archive.clone()),
            ("QmDirectZipEArchive".to_owned(), archive.clone()),
            ("QmDirectZipSArchive".to_owned(), archive.clone()),
        ]),
    );
    let harness = start_pinning_harness(config).await;
    for (matrix_state, archive_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "QmDirectZipUArchive",
            "QmDirectZipUEntry",
        ),
        (
            PublicationMatrixState::Enabled,
            "QmDirectZipEArchive",
            "QmDirectZipEEntry",
        ),
        (
            PublicationMatrixState::Suspended,
            "QmDirectZipSArchive",
            "QmDirectZipSEntry",
        ),
    ] {
        let archive_key = format!("matrix/{}/direct.zip", matrix_state.label());
        let prefix = format!("matrix/{}/direct-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, MATRIX_BASELINE_PIN_TAGS),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        let response = signed_decompress_zip_put(
            &harness,
            &archive_key,
            &prefix,
            archive.clone(),
            MATRIX_ZIP_PIN_TAGS,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            archive_cid,
            false,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn versioning_publication_path_multipart_zip() {
    let archive = legal_single_entry_zip();
    let add_cids = [
        "QmMultipartZipUArchiveBase",
        "QmMultipartZipUEntryBase",
        "QmMultipartZipUPart",
        "QmMultipartZipURoot",
        "QmMultipartZipUEntry",
        "QmMultipartZipEArchiveBase",
        "QmMultipartZipEEntryBase",
        "QmMultipartZipEPart",
        "QmMultipartZipERoot",
        "QmMultipartZipEEntry",
        "QmMultipartZipSOpaqueArchive",
        "QmMultipartZipSOpaqueEntry",
        "QmMultipartZipSNullArchive",
        "QmMultipartZipSNullEntry",
        "QmMultipartZipSPart",
        "QmMultipartZipSRoot",
        "QmMultipartZipSEntry",
    ];
    let mut config = matrix_pinning_config(
        add_cids.into_iter().map(AddReply::Ok).collect(),
        HashMap::from([
            ("QmMultipartZipUPart".to_owned(), archive.clone()),
            ("QmMultipartZipURoot".to_owned(), archive.clone()),
            ("QmMultipartZipEPart".to_owned(), archive.clone()),
            ("QmMultipartZipERoot".to_owned(), archive.clone()),
            ("QmMultipartZipSPart".to_owned(), archive.clone()),
            ("QmMultipartZipSRoot".to_owned(), archive.clone()),
        ]),
    );
    config.explicit_identity = true;
    let harness = start_pinning_harness(config).await;
    for (matrix_state, root_cid, entry_cid) in [
        (
            PublicationMatrixState::Unversioned,
            "QmMultipartZipURoot",
            "QmMultipartZipUEntry",
        ),
        (
            PublicationMatrixState::Enabled,
            "QmMultipartZipERoot",
            "QmMultipartZipEEntry",
        ),
        (
            PublicationMatrixState::Suspended,
            "QmMultipartZipSRoot",
            "QmMultipartZipSEntry",
        ),
    ] {
        let archive_key = format!("matrix/{}/multipart.zip", matrix_state.label());
        let prefix = format!("matrix/{}/multipart-output/", matrix_state.label());
        let entry_key = format!("{prefix}file.txt");
        let before = prepare_publication_matrix_state(
            &harness,
            &harness.state,
            matrix_state,
            &[
                (&archive_key, MATRIX_BASELINE_PIN_TAGS),
                (&entry_key, "fixture=baseline"),
            ],
        )
        .await;
        let create = signed_create_multipart_zip_upload_with_tagging(
            &harness,
            &archive_key,
            &prefix,
            MATRIX_ZIP_PIN_TAGS,
        )
        .await;
        assert_eq!(create.status(), StatusCode::OK);
        let upload_id = xml_element_text(&create.text().await.unwrap(), "UploadId");
        let part = signed_upload_part(&harness, &archive_key, &upload_id, 1, archive.clone()).await;
        assert_eq!(part.status(), StatusCode::OK);
        let etag = part.headers()[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"')
            .to_owned();
        let response =
            signed_complete_multipart(&harness, &archive_key, &upload_id, 1, &etag).await;
        assert_eq!(response.status(), StatusCode::OK);
        let surface = response_version_surface(&response);
        let owner = assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[0],
            root_cid,
            true,
            false,
            Some("actual"),
            surface,
        )
        .await;
        assert_publication_matrix_object(
            &harness,
            &harness.state,
            matrix_state,
            &before[1],
            entry_cid,
            false,
            false,
            None,
            PublicationVersionSurface::DiscoverOnly,
        )
        .await;
        assert_publication_matrix_pinning(&harness, matrix_state, &before[0], &owner).await;
        assert!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .is_err()
        );
    }
    assert_public_ids_are_external(&harness.state, &harness.bucket).await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

async fn assert_latest_absent(harness: &TestHarness, key: &str) {
    assert!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .is_err(),
        "{key} must not have a latest object row"
    );
}

async fn listed_db_keys(harness: &TestHarness) -> Vec<String> {
    store::object::list(harness.state.store.db(), &harness.bucket, None, None, 1000)
        .await
        .expect("list latest DB objects")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

async fn kubo_log(harness: &TestHarness) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .map(|request| format!("{request:?}"))
        .collect()
}

async fn kubo_query_args(harness: &TestHarness, path: &str) -> Vec<String> {
    harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .iter()
        .filter(|request| request.url.path() == path)
        .filter_map(|request| {
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
        })
        .collect()
}

async fn seed_latest(harness: &TestHarness, key: &str, cid: &str, size: i64) {
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        size,
        Some("text/plain"),
        cid,
        None,
        false,
        None,
        None,
        false,
    )
    .await
    .expect("seed latest object");
    install_unversioned_content_version(harness, key, Utc::now()).await;
}

async fn install_unversioned_content_version(harness: &TestHarness, key: &str, now: DateTime<Utc>) {
    let object = store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
        .await
        .expect("load directly seeded latest object");
    let version_id: Option<String> = match store::object_version::BucketVersioningState::Unversioned
    {
        store::object_version::BucketVersioningState::Unversioned => None,
        store::object_version::BucketVersioningState::Enabled
        | store::object_version::BucketVersioningState::Suspended => {
            unreachable!("fixture installs only an unversioned content version")
        }
    };
    harness
        .state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                let row_id = uuid::Uuid::new_v4().to_string();
                let identity = ipfs_s3_gateway::residency::VersionResidencyIdentity::new(
                    row_id.clone(),
                    object.id.clone(),
                    object.cid.clone(),
                );
                store::entities::object_version::Entity::insert(
                    store::entities::object_version::ActiveModel {
                        id: Set(row_id),
                        bucket: Set(object.bucket),
                        key: Set(object.key),
                        version_id: Set(version_id),
                        kind: Set("object".to_owned()),
                        object_id: Set(Some(object.id)),
                        sequence: Set(1),
                        is_latest: Set(true),
                        lifecycle_age_started_at: Set(now),
                        became_noncurrent_at: Set(None),
                        created_at: Set(now),
                        updated_at: Set(now),
                    },
                )
                .exec(txn)
                .await?;
                store::residency::attach_hot_in_transaction(
                    txn,
                    &identity,
                    &ipfs_s3_gateway::residency::PhysicalVerification::Pending,
                )
                .await?;
                Ok::<_, ipfs_s3_gateway::error::AppError>(())
            })
        })
        .await
        .expect("install unversioned content version");
}

async fn seed_running_import(
    harness: &TestHarness,
    job_id: &str,
    key: &str,
    decompress_prefix: Option<&str>,
) -> ipfs_s3_gateway::import::ImportClaim {
    let now = Utc::now();
    store::import::ownership::submit(
        harness.state.store.db(),
        store::import::jobs::NewImportJob {
            id: job_id.to_owned(),
            bucket: harness.bucket.clone(),
            key: key.to_owned(),
            source: ipfs_s3_gateway::import::ImportSource::Cid(
                "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_owned(),
            ),
            request_fingerprint: format!("sha256:{job_id}"),
            client_token: None,
            object_content_type: Some("application/octet-stream".to_owned()),
            metadata: HashMap::new(),
            tags: Vec::new(),
            decompress_prefix: decompress_prefix.map(str::to_owned),
        },
        now,
    )
    .await
    .expect("submit import fixture");
    let mut claimed = store::import::jobs::claim_due(
        harness.state.store.db(),
        &format!("worker-{job_id}"),
        now,
        now + ChronoDuration::minutes(10),
        1,
    )
    .await
    .expect("claim import fixture");
    assert_eq!(claimed.len(), 1, "exactly one fixture import is due");
    let claimed = claimed.pop().expect("claimed import fixture");
    assert_eq!(claimed.job.id, job_id);
    claimed.claim
}

async fn assert_import_state(harness: &TestHarness, job_id: &str, expected: &str) {
    let job = store::entities::import_job::Entity::find_by_id(job_id.to_owned())
        .one(harness.state.store.db())
        .await
        .expect("load import fixture")
        .expect("import fixture exists");
    assert_eq!(job.state, expected, "import job {job_id}");
}

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

fn xml_sections(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    let mut values = Vec::new();
    while let Some(start) = rest.find(&open) {
        let content = &rest[start + open.len()..];
        let Some(end) = content.find(&close) else {
            break;
        };
        values.push(content[..end].to_owned());
        rest = &content[end + close.len()..];
    }
    values
}

fn xml_text(xml: &str, tag: &str) -> Option<String> {
    xml_sections(xml, tag).into_iter().next()
}

fn tagging_pairs(xml: &str) -> Vec<(String, String)> {
    xml_sections(xml, "Tag")
        .into_iter()
        .map(|tag| {
            (
                xml_text(&tag, "Key").expect("Tag Key"),
                xml_text(&tag, "Value").expect("Tag Value"),
            )
        })
        .collect()
}

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

async fn latest_pinning_object(
    harness: &PinningHarness,
    key: &str,
) -> store::entities::object::Model {
    store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
        .await
        .expect("latest pinning object")
}

async fn owner_leases(
    harness: &PinningHarness,
    owner_object_id: &str,
) -> Vec<store::entities::pin_lease::Model> {
    store::entities::pin_lease::Entity::find()
        .filter(store::entities::pin_lease::Column::OwnerObjectId.eq(owner_object_id))
        .order_by_asc(store::entities::pin_lease::Column::Source)
        .all(harness.state.store.db())
        .await
        .expect("owner pinning leases")
}

async fn lease_targets(
    harness: &PinningHarness,
    lease_id: &str,
) -> Vec<store::entities::pin_lease_target::Model> {
    store::entities::pin_lease_target::Entity::find()
        .filter(store::entities::pin_lease_target::Column::LeaseId.eq(lease_id))
        .order_by_asc(store::entities::pin_lease_target::Column::Provider)
        .order_by_asc(store::entities::pin_lease_target::Column::Id)
        .all(harness.state.store.db())
        .await
        .expect("lease targets")
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
    assert_eq!(ledger.effect, effect);
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

async fn assert_no_kubo_pin_removes(harness: &PinningHarness) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/api/v0/pin/rm"),
        "remote lifecycle must not remove Kubo pins: {requests:?}"
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

fn repeated_shared_kubo(add_calls: usize) -> KuboScript {
    KuboScript::repeated_add(
        "QmShared",
        add_calls,
        HashMap::from([("QmShared".to_owned(), b"shared".to_vec())]),
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

fn delete_identifiers_xml(objects: &[(&str, Option<&str>)], quiet: bool) -> Vec<u8> {
    let objects = objects
        .iter()
        .map(|(key, version_id)| match version_id {
            Some(version_id) => {
                format!("<Object><Key>{key}</Key><VersionId>{version_id}</VersionId></Object>")
            }
            None => format!("<Object><Key>{key}</Key></Object>"),
        })
        .collect::<String>();
    format!(
        "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{objects}<Quiet>{quiet}</Quiet></Delete>"
    )
    .into_bytes()
}

fn delete_xml(keys: &[&str], quiet: bool) -> Vec<u8> {
    delete_identifiers_xml(
        &keys.iter().map(|key| (*key, None)).collect::<Vec<_>>(),
        quiet,
    )
}

fn xml_element_values<'a>(xml: &'a str, element: &str) -> Vec<&'a str> {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut remaining = xml;
    let mut values = Vec::new();
    while let Some(open_at) = remaining.find(&open) {
        let value_start = open_at + open.len();
        let after_open = &remaining[value_start..];
        let Some(close_at) = after_open.find(&close) else {
            break;
        };
        values.push(&after_open[..close_at]);
        remaining = &after_open[close_at + close.len()..];
    }
    values
}

fn delete_headers(body: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let digest = base64::engine::general_purpose::STANDARD.encode(md5::compute(body).0);
    headers.insert(
        "content-md5",
        HeaderValue::from_str(&digest).expect("base64 MD5 header"),
    );
    headers
}

async fn signed_delete_objects(
    harness: &impl S3TestEndpoint,
    keys: &[&str],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_xml(keys, quiet);
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await
}

async fn signed_delete_object_versions(
    harness: &impl S3TestEndpoint,
    objects: &[(&str, Option<&str>)],
    quiet: bool,
) -> reqwest::Response {
    let body = delete_identifiers_xml(objects, quiet);
    send_sigv4(
        reqwest::Method::POST,
        harness.endpoint(),
        harness.bucket(),
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await
}

fn sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 customer key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 customer key MD5 header"),
    );
    headers
}

fn sse_c_headers() -> HeaderMap {
    sse_c_headers_for([7; 32])
}

fn copy_source_sse_c_headers_for(key: [u8; 32]) -> HeaderMap {
    let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5_b64 = base64::engine::general_purpose::STANDARD.encode(md5::compute(key).0);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES256"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&key_b64).expect("base64 copy-source key header"),
    );
    headers.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&md5_b64).expect("base64 copy-source key MD5 header"),
    );
    headers
}

fn fixed_sse_c_ciphertext(key: [u8; 32], nonce: [u8; 12], plaintext: &[u8]) -> Vec<u8> {
    ipfs_s3_gateway::crypto::aes_gcm::encrypt_chunk(
        &ipfs_s3_gateway::crypto::ObjectKey { bytes: key },
        &nonce,
        plaintext,
    )
    .expect("fixed SSE-C ciphertext")
    .to_vec()
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
    assert_put_cid_headers(&response, "QmTestCid");
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
            ("filebase-primary", "QmTestCid", "waiting"),
            ("pinata-primary", "QmTestCid", "waiting"),
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
        let remote = remote_pin(&harness, provider, "QmTestCid").await;
        assert_eq!(remote.status, "pinned");
        assert_eq!(remote.request_id.as_deref(), Some(request_id));
        assert_eq!(remote.epoch, 1);
        let request = harness
            .provider_requests()
            .await
            .into_iter()
            .find(|request| request.path == path)
            .expect("expected PSA submit request");
        assert_submit_request(&request, path, "QmTestCid");
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
    assert_put_cid_headers(&response, "QmTestCid");
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
        ("pinata-primary", "QmTestCid", "waiting")
    );
    let remote_before = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
    let remote_after = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_signed_body(&harness, "manual.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_copy_copies_or_replaces_tags_and_reuses_cid_usage() {
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmShared")],
        cat_bodies: HashMap::from([("QmShared".to_owned(), b"shared".to_vec())]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-shared-request",
        "QmShared",
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
    assert_put_cid_headers(&put, "QmShared");
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
        Some("\"QmShared\"")
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
        Some("\"QmShared\"")
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
        assert_eq!(object.cid, "QmShared");
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
            vec![("pinata-primary", "QmShared", "pinned")]
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
    let remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
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
        add_replies: vec![AddReply::Ok("QmPart"), AddReply::Ok("QmRoot")],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), b"part-data".to_vec()),
            ("QmRoot".to_owned(), b"part-data".to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-root-request",
        "QmRoot",
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
    assert_eq!(part_etag, "QmPart");
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
        Some("\"QmRoot\"")
    );
    assert_eq!(xml_text(&complete_xml, "Key").as_deref(), Some("root.bin"));
    assert_tagging(
        &harness,
        "root.bin",
        &[("ipfs-s3:pin", "true"), ("team", "multipart")],
    )
    .await;
    let root = latest_pinning_object(&harness, "root.bin").await;
    assert_eq!(root.cid, "QmRoot");
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
            "QmRoot",
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
        vec![("submit", "pending", "QmRoot")]
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
    let remote = remote_pin(&harness, "pinata-primary", "QmRoot").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmRoot");
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
        "QmTestCid",
    )];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "filebase-renew-request",
        "QmTestCid",
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
    assert_put_cid_headers(&put, "QmTestCid");
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
            remote_pin(&harness, provider, "QmTestCid").await.epoch,
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
            && job.cid == "QmTestCid"
    }));
    assert!(renewal_jobs.iter().any(|job| {
        job.operation == "reconcile"
            && job.state == "pending"
            && job.provider == "filebase-primary"
            && job.cid == "QmTestCid"
    }));
    for (provider, request_id) in [
        ("pinata-primary", "pinata-renew-request"),
        ("filebase-primary", "filebase-renew-request"),
    ] {
        let remote = remote_pin(&harness, provider, "QmTestCid").await;
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
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmEntry")],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry".to_owned(), SINGLE_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-entry-request",
        "QmEntry",
    )];
    let mut harness = start_pinning_harness(config).await;

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
        Some("QmArchive")
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
            "pinata-primary",
            "QmEntry",
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
        vec![("submit", "pending", "QmEntry")]
    );

    harness.run_worker_until_idle().await;
    let submitted_remote = remote_pin(&harness, "pinata-primary", "QmEntry").await;
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
    assert_submit_request(&submit[0], "/psa/pins", "QmEntry");

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
            "pinata-primary",
            "QmEntry",
            "released",
        )]
    );
    let released_remote = remote_pin(&harness, "pinata-primary", "QmEntry").await;
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
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmEntry", "retained").await;
    assert_eq!(
        harness.provider_usage("pinata-primary").await.reserved_pins,
        1
    );
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
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmEntry", "retained").await;
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
            AddReply::Ok("QmOld"),
            AddReply::Ok("QmNew"),
            AddReply::Ok("QmDelete"),
            AddReply::Ok("QmBatchA"),
            AddReply::Ok("QmBatchB"),
        ],
        cat_bodies: HashMap::from([
            ("QmOld".to_owned(), b"old".to_vec()),
            ("QmNew".to_owned(), b"new".to_vec()),
            ("QmDelete".to_owned(), b"delete".to_vec()),
            ("QmBatchA".to_owned(), b"batch-a".to_vec()),
            ("QmBatchB".to_owned(), b"batch-b".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-old-request", "QmOld"),
        PsaReply::pinned_submit("/psa/pins", "pinata-new-request", "QmNew"),
        PsaReply::pinned_submit("/psa/pins", "pinata-delete-request", "QmDelete"),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-a-request", "QmBatchA"),
        PsaReply::pinned_submit("/psa/pins", "pinata-batch-b-request", "QmBatchB"),
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
    assert_put_cid_headers(&old_put, "QmOld");
    harness.run_worker_until_idle().await;
    let old = latest_pinning_object(&harness, "overwrite.txt").await;
    let old_lease = owner_leases(&harness, &old.id).await.remove(0);
    assert_eq!(
        lease_targets(&harness, &old_lease.id).await[0].state,
        "pinned"
    );
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmOld")
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
    assert_put_cid_headers(&overwrite, "QmNew");
    assert_tagging(
        &harness,
        "overwrite.txt",
        &[("ipfs-s3:pin", "true"), ("team", "new")],
    )
    .await;
    let replacement = latest_pinning_object(&harness, "overwrite.txt").await;
    assert_ne!(replacement.id, old.id);
    assert_eq!(replacement.cid, "QmNew");
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
                && job.cid == "QmOld"
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
        vec![("pinata-primary", "QmNew", "pinned")]
    );
    assert_eq!(
        (
            remote_pin(&harness, "pinata-primary", "QmOld")
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", "QmNew")
                .await
                .request_id
                .as_deref(),
        ),
        ("pinned", Some("pinata-new-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmOld", "retained").await;
    assert_signed_body(&harness, "overwrite.txt", b"new").await;

    for (key, body, cid) in [
        ("delete.txt", b"delete".as_slice(), "QmDelete"),
        ("batch-a.txt", b"batch-a".as_slice(), "QmBatchA"),
        ("batch-b.txt", b"batch-b".as_slice(), "QmBatchB"),
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
        xml_sections(&batch_delete_xml, "Deleted")
            .into_iter()
            .map(|deleted| xml_text(&deleted, "Key").expect("Deleted Key"))
            .collect::<Vec<_>>(),
        vec!["batch-a.txt".to_owned(), "batch-b.txt".to_owned()],
        "DeleteObjects XML: {batch_delete_xml}"
    );
    for (key, cid) in [
        ("delete.txt", "QmDelete"),
        ("batch-a.txt", "QmBatchA"),
        ("batch-b.txt", "QmBatchB"),
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
    for cid in ["QmDelete", "QmBatchA", "QmBatchB"] {
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
                    "QmDelete" => "pinata-delete-request",
                    "QmBatchA" => "pinata-batch-a-request",
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
        PsaReply::find_for_job("/psa/pins", "accepted-request", "QmTestCid", "pinned");
    failed_find.status = StatusCode::INTERNAL_SERVER_ERROR.as_u16();
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "accepted-request", "QmTestCid"),
        failed_find,
        PsaReply::find_for_job("/psa/pins", "accepted-request", "QmTestCid", "pinned"),
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
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);

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
        "QmTestCid",
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
        "QmTestCid",
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(failed_find.sequence < adopted_find.sequence);
    let recovered = harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    assert_eq!(recovered.submit_phase.as_deref(), Some("recovering"));
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
        PsaReply::pinned_submit("/psa/pins", "cancelled-request", "QmTestCid"),
        PsaReply::find_for_job("/psa/pins", "cancelled-request", "QmTestCid", "pinned"),
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
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);
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
        "QmTestCid",
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(
        post.sequence < find.sequence,
        "Find observes the accepted POST without proving exclusive creation"
    );
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;
    let adopted = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (adopted.status.as_str(), adopted.request_id.as_deref()),
        ("pinned", Some("cancelled-request"))
    );

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancellation publishes a fenced Unpin");
    assert_eq!(unpin.state, "done");

    let retained = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("pinned", Some("cancelled-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmTestCid", "retained").await;
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
        PsaReply::pinned_submit("/psa/pins", "lost-request", "QmTestCid"),
        PsaReply::find_none_for_job("/psa/pins", "QmTestCid"),
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
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);
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
        "QmTestCid",
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
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
        PsaReply::pinned_submit("/psa/pins", "live-lock-request", "QmTestCid"),
        PsaReply::find_for_job("/psa/pins", "live-lock-request", "QmTestCid", "pinned"),
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
    assert_submit_request_for_job(&post, "/psa/pins", "QmTestCid", &submit);

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
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let retained = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
        "QmTestCid",
        &captured_submit_correlation(&harness, &submit.id).await,
    );
    assert!(post.sequence < find.sequence);
    harness.wait_for_job_state(&submit.id, "done").await;
    harness.stop_worker_without_unlocking().await;

    harness
        .run_current_reconcile("pinata-primary", "QmTestCid")
        .await;
    let unpin = harness
        .pin_jobs()
        .await
        .into_iter()
        .find(|job| job.operation == "unpin")
        .expect("cancelled remote retains a fenced Unpin");
    assert_eq!(unpin.state, "done");

    let retained = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    assert_eq!(
        (retained.status.as_str(), retained.request_id.as_deref()),
        ("pinned", Some("live-lock-request"))
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmTestCid", "retained").await;
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
        PsaReply::submit_status("/psa/pins", "poll-request", "QmTestCid", "queued"),
        PsaReply::pin_status(
            "/psa/pins/poll-request",
            "poll-request",
            "QmTestCid",
            "pinning",
        ),
        PsaReply::pin_status(
            "/psa/pins/poll-request",
            "poll-request",
            "QmTestCid",
            "pinned",
        ),
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
        "QmTestCid",
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
        remote_pin(&harness, "pinata-primary", "QmTestCid")
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
        remote_pin(&harness, "pinata-primary", "QmTestCid")
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
        "QmShared",
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
    assert_put_cid_headers(&put, "QmShared");
    let copied = signed_copy(&harness, "shared.txt", "copy.txt", HeaderMap::new()).await;
    assert_eq!(copied.status(), StatusCode::OK);
    assert_eq!(
        xml_text(&copied.text().await.expect("CopyObject XML"), "ETag").as_deref(),
        Some("\"QmShared\"")
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
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
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
        Some("\"QmShared\"")
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
        PsaReply::submit_status("/psa/pins", "shared-failed", "QmShared", "queued"),
        PsaReply::pin_status(
            "/psa/pins/shared-failed",
            "shared-failed",
            "QmShared",
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::submit_status(
        "/v1/ipfs/pins",
        "fallback-queued",
        "QmShared",
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

    let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
        PsaReply::submit_status("/psa/pins", "failed-all", "QmShared", "queued"),
        PsaReply::pin_status("/psa/pins/failed-all", "failed-all", "QmShared", "failed"),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "one-fallback",
        "QmShared",
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

    let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
        .advance_remote_retry_due("pinata-primary", "QmShared")
        .await;
    harness
        .run_current_reconcile("pinata-primary", "QmShared")
        .await;

    let retained = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmShared", "retained").await;
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
        PsaReply::submit_status("/psa/pins", "failed-cycle-1", "QmShared", "queued"),
        PsaReply::pin_status(
            "/psa/pins/failed-cycle-1",
            "failed-cycle-1",
            "QmShared",
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
    let failed = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
        .run_current_reconcile("pinata-primary", "QmShared")
        .await;
    harness
        .advance_remote_retry_due("pinata-primary", "QmShared")
        .await;
    harness
        .run_current_reconcile("pinata-primary", "QmShared")
        .await;
    let retained = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        (retained.request_id.as_deref(), retained.failure_attempts),
        (Some("failed-cycle-1"), 1)
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmShared", "retained").await;
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
    let equal_remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
    let reset_by_extension = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
        Some("\"QmShared\"")
    );
    let reset_by_target = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
        "QmShared",
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
    let remote_after_cancel = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(
        remote_after_cancel.epoch, 3,
        "cancelling the old Poll owner advances its remote epoch"
    );

    harness
        .run_current_reconcile("pinata-primary", "QmShared")
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
        "QmShared",
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
        "QmShared",
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
        remote_pin(&harness, "pinata-primary", "QmShared")
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
        remote_pin(&harness, "pinata-primary", "QmShared")
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
        Some("\"QmShared\"")
    );
    let current = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!((current.epoch, current.status.as_str()), (5, "pinned"));
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmShared", "confirmed").await;

    harness.restart_worker();
    let parked = harness.wait_for_job_attention(&unpin.id).await;
    harness.stop_worker_without_unlocking().await;
    assert_eq!(
        (parked.state.as_str(), parked.locked_until),
        ("running", None),
        "old Unpin is durably parked for attention rather than reaching provider I/O"
    );
    assert_eq!(
        remote_pin(&harness, "pinata-primary", "QmShared")
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
        "QmShared",
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
        PsaReply::submit_status("/psa/pins", "sticky-primary", "QmShared", "queued"),
        PsaReply::empty(
            http::Method::GET,
            "/psa/pins/sticky-primary",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::pin_status(
            "/psa/pins/sticky-primary",
            "sticky-primary",
            "QmShared",
            "failed",
        ),
    ];
    config.filebase_script = vec![PsaReply::pinned_submit(
        "/v1/ipfs/pins",
        "sticky-secondary",
        "QmShared",
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
            remote_pin(&harness, "filebase-primary", "QmShared")
                .await
                .status
                .as_str(),
            remote_pin(&harness, "pinata-primary", "QmShared")
                .await
                .status
                .as_str()
        ),
        ("pinned", "failed")
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmShared", "unknown").await;
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
        "QmTestCid",
    )];
    config.filebase_script = vec![
        PsaReply::empty(
            http::Method::POST,
            "/v1/ipfs/pins",
            StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        ),
        PsaReply::find_none_for_job("/v1/ipfs/pins", "QmTestCid"),
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
    let pinata = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
    let filebase = remote_pin(&harness, "filebase-primary", "QmTestCid").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_submit_request(&requests[1], "/v1/ipfs/pins", "QmTestCid");
    assert_find_request_for_job(
        &requests[2],
        "/v1/ipfs/pins",
        "QmTestCid",
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
            AddReply::Ok("QmOld"),
            AddReply::Ok("QmNewer"),
            AddReply::Ok("QmIncoming"),
        ],
        cat_bodies: HashMap::from([
            ("QmOld".to_owned(), b"old".to_vec()),
            ("QmNewer".to_owned(), b"new".to_vec()),
            ("QmIncoming".to_owned(), b"in!".to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "old-request", "QmOld"),
        PsaReply::pinned_submit("/psa/pins", "newer-request", "QmNewer"),
    ];
    let mut harness = start_pinning_harness(config).await;

    for (key, body, cid) in [
        ("old.txt", b"old".as_slice(), "QmOld"),
        ("newer.txt", b"new".as_slice(), "QmNewer"),
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
    assert_put_cid_headers(&incoming, "QmIncoming");
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

    harness.run_worker_until_idle().await;
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
        remote_pin(&harness, "pinata-primary", "QmOld").await.status,
        "pinned"
    );
    assert_eq!(
        harness.remote_pins().await.len(),
        2,
        "no capacity for incoming CID"
    );
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmOld", "retained").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmOld");
    assert_submit_request(&requests[1], "/psa/pins", "QmNewer");
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
        "QmShared",
    )];
    let mut harness = start_pinning_harness(config).await;

    for key in ["first.txt", "second.txt"] {
        let put =
            signed_put_with_tagging(&harness, key, b"shared".to_vec(), "ipfs-s3%3Apin=true").await;
        assert_eq!(put.status(), StatusCode::OK, "PUT {key}");
        assert_put_cid_headers(&put, "QmShared");
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
    let remote = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
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
        "QmTestCid",
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
    let expired_remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
    let renewed_remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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

    let pinned = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
    assert_submit_request_for_job(&requests[0], "/psa/pins", "QmTestCid", &initial_submit);
    assert_no_psa_delete(&harness).await;
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmTestCid", "retained").await;
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
            AddReply::Ok("QmShared"),
            AddReply::Ok("QmWait"),
            AddReply::Ok("QmShared"),
        ],
        cat_bodies: HashMap::from([
            ("QmShared".to_owned(), b"shared".to_vec()),
            ("QmWait".to_owned(), b"wait".to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "shared-before-delete",
        "QmShared",
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
    let initial_remote = remote_pin(&harness, "pinata-primary", "QmShared").await;

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
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmShared", "retained").await;

    let shared_new = signed_put_with_tagging(
        &harness,
        "delete-race-new.txt",
        b"shared".to_vec(),
        "ipfs-s3%3Apin=true",
    )
    .await;
    assert_eq!(shared_new.status(), StatusCode::OK);
    assert_put_cid_headers(&shared_new, "QmShared");
    let before_delete_completion = remote_pin(&harness, "pinata-primary", "QmShared").await;
    assert_eq!(before_delete_completion.epoch, initial_remote.epoch + 2);

    let retained = remote_pin(&harness, "pinata-primary", "QmShared").await;
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
    assert_submit_request(&requests[0], "/psa/pins", "QmShared");
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
        "QmTestCid",
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
    assert_put_cid_headers(&put, "QmTestCid");
    harness.run_worker_until_idle().await;
    let object = latest_pinning_object(&harness, "expired.txt").await;
    let manual = owner_leases(&harness, &object.id).await.remove(0);
    let target = lease_targets(&harness, &manual.id).await.remove(0);
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmTestCid", "confirmed").await;

    harness.advance_past_lease_expiry("expired.txt").await;
    harness.restart_worker();
    harness.wait_for_lease_state("expired.txt", "expired").await;
    harness.wait_for_worker_idle().await;
    harness.stop_worker_without_unlocking().await;

    let expired = owner_leases(&harness, &object.id).await.remove(0);
    let released_target = lease_targets(&harness, &manual.id).await.remove(0);
    let remote = remote_pin(&harness, "pinata-primary", "QmTestCid").await;
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
    assert_unknown_psa_resource(&harness, "pinata-primary", "QmTestCid", "retained").await;
    let requests = harness.provider_requests().await;
    assert_eq!(requests.len(), 1);
    assert_submit_request(&requests[0], "/psa/pins", "QmTestCid");
    assert_no_psa_delete(&harness).await;
    assert_signed_body(&harness, "expired.txt", b"body").await;
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_decompressed_pins_entries_not_archive() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
    ];
    let mut harness = start_pinning_harness(config).await;

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
        Some("QmArchive")
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
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
    );
    assert!(!target_cids.contains("QmArchive"));
    assert!(targets.iter().all(|target| {
        target.provider == "pinata-primary"
            && target.state == "waiting"
            && target.lease_id == manual.id
    }));
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            "pinata-primary".to_owned(),
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
                "pinata-primary",
                "QmEntry1",
                Some(manual.id.as_str()),
                Some(1),
                None,
                "pending",
            ),
            (
                "submit",
                "pinata-primary",
                "QmEntry2",
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
        ("QmEntry1", "pinata-entry-1"),
        ("QmEntry2", "pinata-entry-2"),
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
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![PsaReply::pinned_submit(
        "/psa/pins",
        "pinata-entry-2",
        "QmEntry2",
    )];
    let mut harness = start_pinning_harness(config).await;

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
        vec![("pinata-primary", "QmEntry2", "waiting")]
    );
    assert_ne!(targets[0].cid, archive.cid);
    assert_eq!(
        harness
            .provider_usages()
            .await
            .into_iter()
            .map(|usage| (usage.provider, usage.reserved_bytes, usage.reserved_pins))
            .collect::<Vec<_>>(),
        vec![(
            "pinata-primary".to_owned(),
            SECOND_ENTRY_BYTES.len() as i64,
            1
        )]
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
            "pinata-primary",
            "QmEntry2",
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
    assert_submit_request_for_job(&requests[0], "/psa/pins", "QmEntry2", &jobs[0]);
    let remote = remote_pin(&harness, "pinata-primary", "QmEntry2").await;
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
            .all(|remote| remote.cid != "QmArchive")
    );
    assert_no_kubo_pin_removes(&harness).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn test_pinning_zip_archive_renewal_updates_all_entry_targets() {
    let archive_bytes = legal_two_entry_zip();
    let mut config = PinningHarnessConfig::request_one();
    config.kubo_script = KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
    ];
    let mut harness = start_pinning_harness(config).await;

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
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
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
                "pinata-primary",
                "QmEntry1",
                None,
                None,
                None,
                Some(2),
                "pending",
            ),
            (
                "pinata-primary",
                "QmEntry2",
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
    config.kubo_script = KuboScript {
        add_replies: vec![AddReply::Ok("QmArchive"), AddReply::Ok("QmCollisionEntry")],
        cat_bodies: HashMap::from([("QmArchive".to_owned(), archive_bytes.clone())]),
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
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmRoot"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive_bytes.clone()),
            ("QmRoot".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    };
    config.pinata_script = vec![
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-1", "QmEntry1"),
        PsaReply::pinned_submit("/psa/pins", "pinata-entry-2", "QmEntry2"),
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
    assert_eq!(part_etag, "QmPart");
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
        Some("QmRoot")
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
    assert_eq!(archive.cid, "QmRoot");
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
        BTreeSet::from(["QmEntry1".to_owned(), "QmEntry2".to_owned()])
    );
    assert!(!target_cids.contains("QmRoot"));
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
        ("QmEntry1", "pinata-entry-1"),
        ("QmEntry2", "pinata-entry-2"),
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
            AddReply::Ok("QmPart"),
            AddReply::Ok("QmRoot"),
            AddReply::Ok("QmEntry1"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmPart".to_owned(), archive_bytes.clone()),
            ("QmRoot".to_owned(), archive_bytes.clone()),
            ("QmEntry1".to_owned(), FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
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

async fn seed_sse_c_object(
    harness: &TestHarness,
    key: &str,
    cid: &str,
    plaintext: &[u8],
    fingerprinted: bool,
    recorded_size: i64,
) {
    harness.set_cat_body(cid, fixed_sse_c_ciphertext([7; 32], [0x5a; 12], plaintext));
    let object_key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let fingerprint =
        fingerprinted.then(|| harness.state.master_key.sse_c_key_fingerprint(&object_key));
    store::object::upsert(
        harness.state.store.db(),
        &format!("id-{}", key.replace('/', "-")),
        &harness.bucket,
        key,
        cid,
        recorded_size,
        Some("application/octet-stream"),
        cid,
        None,
        true,
        None,
        fingerprint.as_deref(),
        false,
    )
    .await
    .expect("seed SSE-C object");
    install_unversioned_content_version(harness, key, Utc::now()).await;
}

fn inner_complete_request(
    harness: &TestHarness,
    key: &str,
    upload_id: &str,
    etag: &str,
    headers: HeaderMap,
) -> s3s::S3Request<s3s::dto::CompleteMultipartUploadInput> {
    s3s::S3Request {
        input: s3s::dto::CompleteMultipartUploadInput {
            bucket: harness.bucket.clone(),
            key: key.to_owned(),
            upload_id: upload_id.to_owned(),
            multipart_upload: Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(vec![s3s::dto::CompletedPart {
                    e_tag: Some(s3s::dto::ETag::Strong(etag.to_owned())),
                    part_number: Some(1),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
        method: http::Method::POST,
        uri: format!("/{}/{key}?uploadId={upload_id}", harness.bucket)
            .parse()
            .unwrap(),
        headers,
        extensions: http::Extensions::new(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn kubo_call_counts(harness: &TestHarness) -> (usize, usize, usize, usize) {
    let requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log");
    let count = |path| {
        requests
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    };
    (
        count("/api/v0/add"),
        count("/api/v0/cat"),
        count("/api/v0/pin/add"),
        count("/api/v0/pin/rm"),
    )
}

fn assert_put_cid_headers(response: &reqwest::Response, cid: &str) {
    assert_eq!(
        response.headers()[http::header::ETAG],
        HeaderValue::from_str(&format!("\"{cid}\"")).expect("CID ETag header")
    );
    assert_eq!(response.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        response.headers()["x-amz-meta-ipfs-url"],
        HeaderValue::from_str(&format!("ipfs://{cid}")).expect("IPFS URL header")
    );
}

// ---------------------------------------------------------------------------
// Retained standard behaviour regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_aws_bucket_name_validation_rejects_before_store_or_kubo() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let invalid_buckets = [
        "ab".to_owned(),
        "UPPERCASE".to_owned(),
        "under_score".to_owned(),
        ".leading-dot".to_owned(),
        "trailing-hyphen-".to_owned(),
        "adjacent..dot".to_owned(),
        "192.168.0.1".to_owned(),
        "a".repeat(64),
    ];

    for bucket in invalid_buckets {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            &bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidBucketName", "").await;
        assert!(
            !store::bucket::exists(harness.state.store.db(), &bucket)
                .await
                .expect("check bucket row"),
            "invalid bucket {bucket} must not have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_aws_bucket_name_validation_accepts_lowercase_dot_and_hyphen() {
    let harness = start_harness(scripted(&[], vec![])).await;

    for bucket in ["abc", "valid-bucket", "valid.bucket"] {
        let response = send_sigv4(
            reqwest::Method::PUT,
            &harness.endpoint,
            bucket,
            "",
            &[],
            Vec::new(),
            HeaderMap::new(),
            "test",
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK, "create bucket {bucket}");
        assert!(
            store::bucket::exists(harness.state.store.db(), bucket)
                .await
                .expect("check bucket row"),
            "valid bucket {bucket} must have a store row"
        );
    }

    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_create_and_put_and_get_plain_object() {
    let harness = start_harness(standard_script(1)).await;
    let bucket = test_bucket(&harness);

    let put = bucket
        .put_object("hello.txt", b"hello world")
        .await
        .expect("put object");
    assert_eq!(put.status_code(), 200);
    assert!(
        put.headers()
            .get("etag")
            .expect("etag header")
            .contains("QmTestCid")
    );

    let get = bucket.get_object("hello.txt").await.expect("get object");
    assert_eq!(get.status_code(), 200);
    assert_eq!(get.as_slice(), b"hello world");
}

#[tokio::test]
async fn test_harness_captures_kubo_file_bytes_and_updates_cat_body() {
    let payload = b"captured exact bytes".to_vec();
    let harness = start_harness(scripted(&["QmCaptured"], vec![])).await;

    let put = signed_put(
        &harness,
        "captured.bin",
        &[],
        payload.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body("QmCaptured", payload.clone());
    let get = signed_get(&harness, "captured.bin").await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.expect("GET body").as_ref(), &payload);
}

#[tokio::test]
async fn test_client_compat_head_nested_key_signed_on_localhost() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "nested/path/file.txt", "QmNestedCid", 11).await;
    let response = send_sigv4(
        reqwest::Method::HEAD,
        &harness.endpoint,
        &harness.bucket,
        "nested/path/file.txt",
        &[],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .expect("Content-Length"),
        "11"
    );
    assert!(
        response
            .headers()
            .get(http::header::ETAG)
            .expect("ETag")
            .to_str()
            .expect("ETag is text")
            .contains("QmNestedCid")
    );
}

#[tokio::test]
async fn test_head_range_changes_only_content_length_and_never_calls_kubo() {
    let harness = start_harness(standard_script(0)).await;
    store::object::upsert(
        harness.state.store.db(),
        "head-range-id",
        &harness.bucket,
        "range.bin",
        "QmRange",
        11,
        Some("text/plain"),
        "QmRange",
        Some(serde_json::json!({"color": "blue"})),
        true,
        Some("wrapped-fixture"),
        None,
        false,
    )
    .await
    .expect("seed ranged HEAD object");
    install_unversioned_content_version(&harness, "range.bin", Utc::now()).await;

    let full = signed_head(&harness, "range.bin", None).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.headers()[http::header::CONTENT_LENGTH], "11");
    assert_eq!(full.headers()[http::header::ETAG], "\"QmRange\"");
    assert_eq!(full.headers()[http::header::CONTENT_TYPE], "text/plain");
    assert_eq!(full.headers()["x-amz-server-side-encryption"], "AES256");
    assert_eq!(full.headers()["x-amz-meta-color"], "blue");
    assert!(full.headers().get(http::header::LAST_MODIFIED).is_some());

    let ranged = signed_head(&harness, "range.bin", Some("bytes=2-5")).await;
    assert_eq!(ranged.status(), StatusCode::OK);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(ranged.headers().get(http::header::CONTENT_RANGE).is_none());
    for name in [
        http::header::ETAG.as_str(),
        http::header::CONTENT_TYPE.as_str(),
        http::header::LAST_MODIFIED.as_str(),
        "x-amz-server-side-encryption",
        "x-amz-meta-color",
    ] {
        assert_eq!(
            ranged.headers().get(name),
            full.headers().get(name),
            "{name}"
        );
    }
    assert!(full.bytes().await.expect("full HEAD body").is_empty());
    assert!(ranged.bytes().await.expect("ranged HEAD body").is_empty());

    let unsatisfied = signed_head(&harness, "range.bin", Some("bytes=20-30")).await;
    assert_eq!(unsatisfied.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(
        unsatisfied
            .bytes()
            .await
            .expect("unsatisfied HEAD body")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_list_objects() {
    let harness = start_harness(standard_script(2)).await;
    let bucket = test_bucket(&harness);
    bucket
        .put_object("obj1.txt", b"hello world")
        .await
        .expect("put obj1");
    bucket
        .put_object("obj2.txt", b"hello world")
        .await
        .expect("put obj2");

    let pages = bucket
        .list(String::new(), None)
        .await
        .expect("list objects");
    assert_eq!(
        pages.iter().map(|page| page.contents.len()).sum::<usize>(),
        2
    );
}

#[tokio::test]
async fn test_list_objects_with_delimiter_returns_common_prefixes() {
    let harness = start_harness(standard_script(4)).await;
    let bucket = test_bucket(&harness);
    for key in [
        "a.txt",
        "photos/cat.jpg",
        "photos/dog.jpg",
        "videos/clip.mp4",
    ] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list(String::new(), Some("/".to_string()))
        .await
        .expect("list with delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["a.txt"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/", "videos/"]);
}

#[tokio::test]
async fn test_list_objects_with_prefix_and_delimiter_returns_one_level() {
    let harness = start_harness(standard_script(3)).await;
    let bucket = test_bucket(&harness);
    for key in ["photos/cat.jpg", "photos/dog.jpg", "photos/2024/jan.jpg"] {
        bucket
            .put_object(key, b"hello world")
            .await
            .unwrap_or_else(|error| panic!("put {key}: {error}"));
    }

    let pages = bucket
        .list("photos/".to_string(), Some("/".to_string()))
        .await
        .expect("list with prefix and delimiter");
    let mut keys: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.clone()))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["photos/cat.jpg", "photos/dog.jpg"]);
    let mut prefixes: Vec<_> = pages
        .iter()
        .flat_map(|page| {
            page.common_prefixes
                .iter()
                .flat_map(|prefixes| prefixes.iter().map(|prefix| prefix.prefix.clone()))
        })
        .collect();
    prefixes.sort();
    assert_eq!(prefixes, vec!["photos/2024/"]);
}

#[tokio::test]
async fn test_wrong_credentials_rejected() {
    let harness = start_harness(standard_script(0)).await;
    let result = bad_bucket(&harness)
        .put_object("hello.txt", b"hello world")
        .await;
    assert!(result.is_err(), "wrong credentials must be rejected");
}

// ---------------------------------------------------------------------------
// Task 8: PutObject, authentication, and failure acceptance coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_client_compat_get_bucket_location_is_standard_us_east_1() {
    let harness = start_harness(standard_script(0)).await;
    let raw = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(raw.status(), StatusCode::OK);
    let body = raw.bytes().await.expect("GetBucketLocation body");
    let mut deserializer = s3s::xml::Deserializer::new(body.as_ref());
    let decoded = <s3s::dto::GetBucketLocationOutput as s3s::xml::Deserialize>::deserialize(
        &mut deserializer,
    )
    .expect("decode GetBucketLocationOutput with s3s 0.14 restXml");
    deserializer
        .expect_eof()
        .expect("GetBucketLocation XML EOF");
    assert_eq!(decoded.location_constraint, None);

    let body_text = std::str::from_utf8(body.as_ref()).expect("GetBucketLocation UTF-8 XML");
    assert!(body_text.contains("<LocationConstraint"), "{body_text}");
    assert!(!body_text.contains("us-east-1"), "{body_text}");

    let missing = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("location", "")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_s3_error(
        missing,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
}

#[tokio::test]
async fn test_client_compat_list_v1_delimiter_marker_pages_without_replay() {
    let harness = start_harness(standard_script(0)).await;
    for key in ["a", "photos/1", "photos/2", "videos/1"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[("delimiter", "/"), ("max-keys", "2")],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = first.text().await.expect("first ListObjects body");
    assert_eq!(xml_sections(&first_body, "Key"), vec!["a"]);
    assert_eq!(
        xml_sections(&first_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["photos/"]
    );
    assert_eq!(
        xml_text(&first_body, "IsTruncated").as_deref(),
        Some("true")
    );
    let marker = xml_text(&first_body, "NextMarker").expect("NextMarker");
    assert_eq!(marker, "photos/2");

    let second = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("delimiter", "/"),
            ("marker", marker.as_str()),
            ("max-keys", "2"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = second.text().await.expect("second ListObjects body");
    assert!(xml_sections(&second_body, "Key").is_empty());
    assert_eq!(
        xml_sections(&second_body, "Prefix")
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>(),
        vec!["videos/"]
    );
    assert_eq!(
        xml_text(&second_body, "IsTruncated").as_deref(),
        Some("false")
    );
    assert!(xml_text(&second_body, "NextMarker").is_none());
}

#[tokio::test]
async fn test_client_compat_list_url_encoding_projects_wire_fields_and_preserves_raw_pagination() {
    let harness = start_harness(standard_script(0)).await;
    let raw_prefix = "prefix/";
    let raw_object = "prefix/a%2F(é)";
    let raw_common_key = "prefix/dir%2F(é)/one";
    for key in [raw_object, raw_common_key, "prefix/z"] {
        seed_latest(&harness, key, &format!("Qm-{}", key.replace('/', "-")), 1).await;
    }

    let first_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_prefix),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(first_v1.status(), StatusCode::OK);
    let first_v1_body = first_v1.text().await.expect("v1 URL-encoded body");
    assert_eq!(
        xml_text(&first_v1_body, "Name").as_deref(),
        Some("test-bkt")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Prefix").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Delimiter").as_deref(),
        Some("%2F")
    );
    assert_eq!(
        xml_text(&first_v1_body, "Marker").as_deref(),
        Some("prefix%2F")
    );
    assert_eq!(
        xml_sections(&first_v1_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(
        xml_sections(&first_v1_body, "Prefix")
            .contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F".to_owned())
    );
    assert_eq!(
        xml_text(&first_v1_body, "NextMarker").as_deref(),
        Some("prefix%2Fdir%252F%28%C3%A9%29%2Fone")
    );

    let second_v1 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("marker", raw_common_key),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(second_v1.status(), StatusCode::OK);
    let second_v1_body = second_v1.text().await.expect("second v1 URL-encoded body");
    assert_eq!(xml_sections(&second_v1_body, "Key"), vec!["prefix%2Fz"]);
    assert!(xml_sections(&second_v1_body, "CommonPrefixes").is_empty());
    assert!(xml_text(&second_v1_body, "NextMarker").is_none());

    let v2 = send_sigv4(
        reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        "",
        &[
            ("list-type", "2"),
            ("prefix", raw_prefix),
            ("delimiter", "/"),
            ("continuation-token", raw_prefix),
            ("start-after", "ignored/%2F(é)"),
            ("max-keys", "2"),
            ("encoding-type", "url"),
        ],
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(v2.status(), StatusCode::OK);
    let v2_body = v2.text().await.expect("v2 URL-encoded body");
    assert_eq!(xml_text(&v2_body, "Name").as_deref(), Some("test-bkt"));
    assert_eq!(xml_text(&v2_body, "Prefix").as_deref(), Some("prefix%2F"));
    assert_eq!(xml_text(&v2_body, "Delimiter").as_deref(), Some("%2F"));
    assert_eq!(
        xml_text(&v2_body, "StartAfter").as_deref(),
        Some("ignored%2F%252F%28%C3%A9%29")
    );
    assert_eq!(
        xml_text(&v2_body, "ContinuationToken").as_deref(),
        Some(raw_prefix),
        "continuation tokens remain opaque"
    );
    assert_eq!(
        xml_text(&v2_body, "NextContinuationToken").as_deref(),
        Some(raw_common_key),
        "the raw cursor identity is not URL-projected"
    );
    assert_eq!(
        xml_sections(&v2_body, "Key"),
        vec!["prefix%2Fa%252F%28%C3%A9%29"]
    );
    assert!(
        xml_sections(&v2_body, "Prefix").contains(&"prefix%2Fdir%252F%28%C3%A9%29%2F".to_owned())
    );
}

#[tokio::test]
async fn test_client_compat_delete_objects_is_retry_safe_and_ordered() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "a", "QmA", 1).await;
    seed_latest(&harness, "b", "QmB", 1).await;

    let response = signed_delete_objects(&harness, &["a", "missing", "a", "b"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("DeleteObjects body");
    let deleted = xml_sections(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(&section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["a", "missing", "a", "b"]);
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "a").await;
    assert_latest_absent(&harness, "b").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_quiet_hides_successes() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "quiet", "QmQuiet", 5).await;

    let response = signed_delete_objects(&harness, &["quiet", "missing"], true).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("quiet DeleteObjects body");
    assert!(xml_sections(&body, "Deleted").is_empty());
    assert!(xml_sections(&body, "Error").is_empty());
    assert_latest_absent(&harness, "quiet").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_continues_after_store_error() {
    let harness = start_harness(standard_script(0)).await;
    seed_latest(&harness, "before", "QmBefore", 6).await;
    seed_latest(&harness, "fail", "QmFail", 4).await;
    seed_latest(&harness, "after", "QmAfter", 5).await;
    harness
        .state
        .store
        .db()
        .execute_unprepared(
            "CREATE TRIGGER fail_one_batch_delete BEFORE UPDATE OF is_latest ON objects \
             WHEN OLD.bucket = 'test-bkt' AND OLD.key = 'fail' AND NEW.is_latest = FALSE \
             BEGIN SELECT RAISE(FAIL, 'injected delete failure'); END",
        )
        .await
        .expect("install delete failure trigger");

    let response = signed_delete_objects(&harness, &["before", "fail", "after"], false).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial DeleteObjects body");
    let deleted = xml_sections(&body, "Deleted")
        .into_iter()
        .map(|section| xml_text(&section, "Key").expect("Deleted key"))
        .collect::<Vec<_>>();
    assert_eq!(deleted, vec!["before", "after"]);
    let errors = xml_sections(&body, "Error");
    assert_eq!(errors.len(), 1);
    assert_eq!(xml_text(&errors[0], "Key").as_deref(), Some("fail"));
    assert_eq!(
        xml_text(&errors[0], "Code").as_deref(),
        Some("InternalError")
    );
    assert_eq!(
        xml_text(&errors[0], "Message").as_deref(),
        Some("failed to delete object")
    );
    assert_latest_absent(&harness, "before").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "fail")
        .await
        .expect("failed item remains latest");
    assert_latest_absent(&harness, "after").await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_client_compat_delete_objects_missing_bucket_is_request_error() {
    let harness = start_harness(standard_script(0)).await;
    let body = delete_xml(&["a"], false);
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        "missing-bkt",
        "",
        &[("delete", "")],
        body.clone(),
        delete_headers(&body),
        "test",
    )
    .await;

    assert_s3_error(
        response,
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "bucket not found: missing-bkt",
    )
    .await;
    assert!(kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty());
}

#[tokio::test]
async fn test_sigv4_valid_request_reaches_decompress_route() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;

    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress response body");
    assert!(body.contains("<DecompressZipResult>"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive DB row");
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "file.txt")
        .await
        .expect("entry DB row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
}

#[tokio::test]
async fn test_sigv4_wrong_signature_is_rejected_before_kubo() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
        "wrong",
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_sigv4_query_tuple_decodes_prefix_once() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let query = [("decompress-zip", "prefix/nested/")];
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &query,
        archive,
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .url()
            .as_str()
            .contains("decompress-zip=prefix%2Fnested%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "prefix/nested/file.txt",
    )
    .await
    .expect("decoded target key");
    assert_latest_absent(&harness, "prefix%2Fnested%2Ffile.txt").await;
}

#[tokio::test]
async fn test_presigned_put_signs_custom_query_and_lists_gets_objects() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(url)
        .body(archive.clone())
        .send()
        .await
        .expect("presigned PUT");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("X-Amz-Algorithm")
    );
    assert!(!observed.headers.contains_key(http::header::AUTHORIZATION));

    let pages = test_bucket(&harness)
        .list(String::new(), None)
        .await
        .expect("ListObjectsV2");
    let listed: Vec<_> = pages
        .iter()
        .flat_map(|page| page.contents.iter().map(|object| object.key.as_str()))
        .collect();
    assert!(listed.contains(&"archive.zip"));
    assert!(listed.contains(&"prefix/file.txt"));
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/file.txt", SINGLE_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive", "QmEntry"], &[]).await;
    let log = kubo_log(&harness).await.join("\n");
    assert!(log.contains("/api/v0/add"));
    assert!(log.contains("/api/v0/pin/add"));
    assert!(log.contains("/api/v0/cat"));
    assert!(log.contains("QmArchive"));
    assert!(log.contains("QmEntry"));
}

#[tokio::test]
async fn test_presigned_space_target_raw_plus_rewrite_is_semantically_stable() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry", SINGLE_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let signed_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("decompress-zip", "reports Q3/")],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    assert!(signed_url.contains("decompress-zip=reports%20Q3%2F"));
    let rewritten_url = signed_url.replacen(
        "decompress-zip=reports%20Q3%2F",
        "decompress-zip=reports+Q3%2F",
        1,
    );
    assert_ne!(rewritten_url, signed_url);

    let response = reqwest::Client::new()
        .put(rewritten_url)
        .body(archive)
        .send()
        .await
        .expect("rewritten presigned PUT");

    assert_eq!(response.status(), StatusCode::OK);
    let observed = latest_observed_request(&harness).await;
    assert!(
        observed
            .uri
            .query()
            .unwrap_or_default()
            .contains("decompress-zip=reports+Q3%2F")
    );
    store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "reports Q3/file.txt",
    )
    .await
    .expect("space-decoded entry DB row");
    assert_latest_absent(&harness, "reports+Q3/file.txt").await;
    assert!(
        !listed_db_keys(&harness)
            .await
            .iter()
            .any(|key| key == "reports+Q3/file.txt")
    );
}

#[tokio::test]
async fn test_presigned_put_tampered_decompress_query_is_rejected_without_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&[], vec![])).await;
    let url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "true"),
        ],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let response = reqwest::Client::new()
        .put(format!("{url}&decompress-zip=other%2F"))
        .body(archive)
        .send()
        .await
        .expect("tampered presigned PUT");
    assert_s3_error(response, StatusCode::FORBIDDEN, "SignatureDoesNotMatch", "").await;
    for key in ["archive.zip", "prefix/file.txt", "other/file.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(
        ipfs_s3_gateway::store::entities::multipart_upload::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart upload rows")
            .is_empty()
    );
    assert!(
        ipfs_s3_gateway::store::entities::multipart_part::Entity::find()
            .all(harness.state.store.db())
            .await
            .expect("multipart part rows")
            .is_empty()
    );
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_put_decompress_zip_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    let body = response.text().await.expect("decompress result body");
    assert!(body.contains("<DecompressZipResult>"));
    assert!(body.contains("<ExtractedCount>2</ExtractedCount>"));
    for key in ["archive.zip", "first.txt", "second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
}

#[tokio::test]
async fn test_put_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", ""), ("decompress-zip-result", "false")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmArchive\"");
    assert!(
        response
            .bytes()
            .await
            .expect("empty response body")
            .is_empty()
    );
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "first.txt", "second.txt"]
    );
}

#[tokio::test]
async fn test_put_decompress_zip_traversal_hides_db_and_keeps_archive_pin() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmArchive"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "prefix/")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "escape.txt", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmArchive"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive"]).await;
}

#[tokio::test]
async fn test_put_decompress_zip_archive_key_collision_is_global_reject() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmCollisionEntry"],
        vec![("QmArchive", archive.clone())],
    ))
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive,
        HeaderMap::new(),
    )
    .await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmArchive", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_one_entry_kubo_failure_is_partial() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(KuboScript {
        add_replies: vec![
            AddReply::Ok("QmArchive"),
            AddReply::Error(StatusCode::INTERNAL_SERVER_ERROR, "entry add failed"),
            AddReply::Ok("QmEntry2"),
        ],
        cat_bodies: HashMap::from([
            ("QmArchive".to_owned(), archive.clone()),
            ("QmEntry2".to_owned(), SECOND_ENTRY_BYTES.to_vec()),
        ]),
    })
    .await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("partial response body");
    assert!(body.contains("<FailedCount>1</FailedCount>"));
    assert!(body.contains("EntryUploadFailed"));
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.zip")
        .await
        .expect("archive latest row");
    assert_latest_absent(&harness, "first.txt").await;
    store::object::get_latest(harness.state.store.db(), &harness.bucket, "second.txt")
        .await
        .expect("second entry latest row");
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "second.txt", SECOND_ENTRY_BYTES).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmArchive", "QmEntry2"]).await;
}

#[tokio::test]
async fn versioning_direct_zip_publication_rollback_is_atomic_and_keeps_pins() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmArchive", "QmEntry1", "QmEntry2"],
        vec![
            ("QmArchive", archive.clone()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_first_entry BEFORE INSERT ON objects \
             WHEN NEW.key = 'first.txt' \
             BEGIN SELECT RAISE(FAIL, 'forced entry publish failure'); END;",
        ))
        .await
        .expect("install entry publish failure trigger");
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        archive.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("S3 error response body");
    assert!(
        body.contains("<Code>InternalError</Code>"),
        "expected an S3 InternalError response"
    );
    assert!(
        !body.contains("forced entry publish failure"),
        "S3 database errors must not expose internal database details"
    );

    for key in ["archive.zip", "first.txt", "second.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    let db = harness.state.store.db();
    assert!(
        store::entities::object::Entity::find()
            .all(db)
            .await
            .expect("load object rows after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object rows"
    );
    assert!(
        store::entities::object_tag::Entity::find()
            .all(db)
            .await
            .expect("load object tags after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no object tags"
    );
    assert!(
        store::entities::pin_lease::Entity::find()
            .all(db)
            .await
            .expect("load pin leases after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin leases"
    );
    assert!(
        store::entities::pin_lease_target::Entity::find()
            .all(db)
            .await
            .expect("load pin targets after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin targets"
    );
    assert!(
        store::entities::remote_pin::Entity::find()
            .all(db)
            .await
            .expect("load remote pins after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no remote pins"
    );
    assert!(
        store::entities::pin_provider_usage::Entity::find()
            .all(db)
            .await
            .expect("load provider usage after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no provider usage"
    );
    assert!(
        store::entities::pin_job::Entity::find()
            .all(db)
            .await
            .expect("load pin jobs after atomic rollback")
            .is_empty(),
        "atomic ZIP publication must leave no pin jobs"
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmArchive", "QmEntry1", "QmEntry2"],
    )
    .await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_s3() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        headers,
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

#[tokio::test]
async fn test_put_decompress_zip_rejects_sse_c() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let response = signed_put(
        &harness,
        "archive.zip",
        &[("decompress-zip", "")],
        legal_single_entry_zip(),
        sse_c_headers(),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
    assert_no_kubo_calls(&harness).await;
}

// ---------------------------------------------------------------------------
// Task 8: Multipart acceptance coverage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multipart_decompress_signed_default_result() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("multipart decompress body");
    assert!(body.contains("<DecompressZipResult>"));
    for key in ["archive.zip", "prefix/first.txt", "prefix/second.txt"] {
        store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
            .await
            .unwrap_or_else(|error| panic!("latest {key}: {error}"));
    }
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_multipart_duplicate_entry_key_last_wins() {
    let archive = duplicate_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmFirstDuplicate", FIRST_DUPLICATE_BYTES.to_vec()),
            ("QmSecondDuplicate", SECOND_DUPLICATE_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "prefix/duplicate.txt",
        )
        .await
        .expect("latest duplicate entry")
        .cid,
        "QmSecondDuplicate"
    );
    assert_signed_body(&harness, "prefix/duplicate.txt", SECOND_DUPLICATE_BYTES).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmFirstDuplicate", "QmSecondDuplicate"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_decompress_signed_result_false() {
    let archive = legal_two_entry_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        vec![
            ("QmPart", archive.clone()),
            ("QmRoot", archive.clone()),
            ("QmEntry1", FIRST_ENTRY_BYTES.to_vec()),
            ("QmEntry2", SECOND_ENTRY_BYTES.to_vec()),
        ],
    ))
    .await;
    let upload_id = create_multipart(
        &harness,
        "archive.zip",
        &[
            ("decompress-zip", "prefix/"),
            ("decompress-zip-result", "false"),
        ],
    )
    .await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive.clone()).await;
    let response = complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["etag"], "\"QmRoot\"");
    let body = response.text().await.expect("multipart response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert!(!body.contains("<DecompressZipResult>"));
    assert_eq!(
        listed_db_keys(&harness).await,
        vec!["archive.zip", "prefix/first.txt", "prefix/second.txt"]
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("parts after complete")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart", "QmRoot"]
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmEntry1", "QmEntry2"],
    )
    .await;
    assert_signed_body(&harness, "archive.zip", &archive).await;
    assert_signed_body(&harness, "prefix/first.txt", FIRST_ENTRY_BYTES).await;
    assert_signed_body(&harness, "prefix/second.txt", SECOND_ENTRY_BYTES).await;
}

#[tokio::test]
async fn test_complete_xml_content_length_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let response = send_sigv4(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        too_large_xml,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    let declared_length = observed
        .headers
        .get(http::header::CONTENT_LENGTH)
        .expect("declared Content-Length")
        .to_str()
        .expect("numeric Content-Length")
        .parse::<usize>()
        .expect("Content-Length number");
    assert!(declared_length > 4 * 1024 * 1024);
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_complete_xml_chunked_over_limit_rejected_without_complete_mutation() {
    let archive = legal_single_entry_zip();
    let harness = start_harness(scripted(&["QmPart"], vec![("QmPart", archive.clone())])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload snapshot");
    let parts_before = store::multipart::list_parts(harness.state.store.db(), &upload_id)
        .await
        .expect("part snapshot");
    let kubo_before = kubo_log(&harness).await;
    let too_large_xml = vec![b'x'; 4 * 1024 * 1024 + 1];
    let chunk_size = too_large_xml.len().div_ceil(3);
    let chunks = too_large_xml
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();
    let response = send_sigv4_chunked_http1(
        reqwest::Method::POST,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("uploadId", upload_id.as_str())],
        chunks,
        HeaderMap::new(),
        "test",
    )
    .await;
    let observed = latest_observed_request(&harness).await;
    assert_eq!(observed.method, http::Method::POST);
    assert!(
        observed
            .headers
            .get(http::header::TRANSFER_ENCODING)
            .expect("Transfer-Encoding")
            .to_str()
            .expect("Transfer-Encoding value")
            .contains("chunked")
    );
    assert!(!observed.headers.contains_key(http::header::CONTENT_LENGTH));
    assert_eq!(
        observed.headers["x-amz-decoded-content-length"],
        (4 * 1024 * 1024 + 1).to_string()
    );
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        "CompleteMultipartUpload XML exceeds 4 MiB",
    )
    .await;
    assert_eq!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged upload"),
        upload_before
    );
    assert_eq!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("unchanged parts"),
        parts_before
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_eq!(
        kubo_log(&harness).await,
        kubo_before,
        "no Complete Kubo calls"
    );
    assert_eq!(etag, "QmPart", "setup part ETag is retained");
}

#[tokio::test]
async fn test_multipart_traversal_keeps_root_pin_and_retry_state() {
    let archive = traversal_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "",
    )
    .await;
    for key in ["archive.zip", "prefix/escape.txt"] {
        assert_latest_absent(&harness, key).await;
    }
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retry part")
            .etag,
        etag
    );
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retry upload");
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmRoot"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_multipart_archive_key_collision_is_global_reject_and_retryable() {
    let archive = archive_key_collision_zip();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        vec![("QmPart", archive.clone()), ("QmRoot", archive.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.zip", &[("decompress-zip", "")]).await;
    let etag = upload_part(&harness, "archive.zip", &upload_id, 1, archive).await;
    let response =
        complete_multipart(&harness, "archive.zip", &upload_id, &[(1, etag.clone())]).await;
    assert_s3_error(
        response,
        StatusCode::BAD_REQUEST,
        "InvalidParameterValue",
        "zip entry collides with archive key: archive.zip",
    )
    .await;
    assert_latest_absent(&harness, "archive.zip").await;
    assert!(listed_db_keys(&harness).await.is_empty());
    store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable upload row");
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable part row")
            .etag,
        etag
    );
    assert_pin_calls(
        &harness,
        "/api/v0/pin/add",
        &["QmPart", "QmRoot", "QmCollisionEntry"],
        &[],
    )
    .await;
    assert_pin_calls(
        &harness,
        "/api/v0/pin/rm",
        &[],
        &["QmPart", "QmRoot", "QmCollisionEntry"],
    )
    .await;
}

#[tokio::test]
async fn test_multipart_abort_signed_removes_rows_and_keeps_part_pin() {
    let harness = start_harness(scripted(&["QmPart"], vec![])).await;
    let upload_id =
        create_multipart(&harness, "archive.zip", &[("decompress-zip", "prefix/")]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        legal_single_entry_zip(),
    )
    .await;
    let response = abort_multipart(&harness, "archive.zip", &upload_id).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_latest_absent(&harness, "archive.zip").await;
    assert_latest_absent(&harness, "prefix/file.txt").await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
    assert!(
        !kubo_log(&harness)
            .await
            .iter()
            .any(|entry| entry.contains("/api/v0/cat")),
        "abort must not cat content"
    );
}

#[tokio::test]
async fn test_multipart_single_part_equal_root_remains_readable() {
    let content = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmPart"],
        vec![("QmPart", content.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "archive.bin", &[]).await;
    let etag = upload_part(&harness, "archive.bin", &upload_id, 1, content.clone()).await;
    let response = complete_multipart(&harness, "archive.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    assert_eq!(
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "archive.bin")
            .await
            .expect("completed latest row")
            .cid,
        "QmPart"
    );
    assert_signed_body(&harness, "archive.bin", &content).await;
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart"]).await;
}

#[tokio::test]
async fn test_multipart_shared_part_cid_survives_replace_abort_and_complete() {
    let shared = b"shared bytes".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmSharedPart",
            "QmSharedPart",
            "QmReplacement",
            "QmSharedPart",
            "QmSharedPart",
            "QmRoot",
        ],
        vec![
            ("QmSharedPart", shared.clone()),
            ("QmReplacement", b"replacement bytes".to_vec()),
            ("QmRoot", shared.clone()),
        ],
    ))
    .await;
    let put = signed_put(
        &harness,
        "shared.bin",
        &[],
        shared.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);

    let replace_upload = create_multipart(&harness, "replace.bin", &[]).await;
    upload_part(&harness, "replace.bin", &replace_upload, 1, shared.clone()).await;
    upload_part(
        &harness,
        "replace.bin",
        &replace_upload,
        1,
        b"replacement bytes".to_vec(),
    )
    .await;
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let abort_upload = create_multipart(&harness, "abort.bin", &[]).await;
    upload_part(&harness, "abort.bin", &abort_upload, 1, shared.clone()).await;
    let abort = abort_multipart(&harness, "abort.bin", &abort_upload).await;
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);
    assert_signed_body(&harness, "shared.bin", &shared).await;

    let complete_upload = create_multipart(&harness, "complete.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "complete.bin",
        &complete_upload,
        1,
        shared.clone(),
    )
    .await;
    let complete =
        complete_multipart(&harness, "complete.bin", &complete_upload, &[(1, etag)]).await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_signed_body(&harness, "shared.bin", &shared).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmSharedPart"]).await;
}

#[tokio::test]
async fn test_upload_part_db_failure_keeps_new_pin_and_old_record() {
    let harness = start_harness(scripted(&["QmOldPart", "QmNewPart"], vec![])).await;
    let upload_id = create_multipart(&harness, "archive.zip", &[]).await;
    upload_part(
        &harness,
        "archive.zip",
        &upload_id,
        1,
        b"old part bytes".to_vec(),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_part_update BEFORE UPDATE ON multipart_parts \
             BEGIN SELECT RAISE(FAIL, 'forced part update failure'); END;",
        ))
        .await
        .expect("install part update failure trigger");
    let response = send_sigv4(
        reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        "archive.zip",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"new part bytes".to_vec(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response.text().await.expect("failed upload-part body");
    assert!(body.contains("InternalError"));
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("original part row")
            .cid,
        "QmOldPart"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmNewPart"], &[]).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmOldPart", "QmNewPart"]).await;
}

// ---------------------------------------------------------------------------
// Task 6: SSE-C UploadPart fingerprint validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mismatched_sse_c_upload_part_is_rejected_before_kubo_and_preserves_part() {
    let harness = start_harness(scripted(&["QmOriginalPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let initial = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"original encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(initial.status(), StatusCode::OK);
    let calls_before = kubo_call_counts(&harness).await;
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("original part row");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = signed_put(
            &harness,
            "customer-encrypted.bin",
            &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
            b"rejected replacement".to_vec(),
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected UploadPart must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected UploadPart must retain the original part row"
        );
    }
}

#[tokio::test]
async fn legacy_sse_c_upload_part_claims_fingerprint_before_add() {
    let harness = start_harness(scripted(&["QmLegacyPart"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .expect("legacy upload")
            .sse_c_key_fingerprint
            .is_none()
    );

    let response = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("claimed upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 1, 0));
}

// ---------------------------------------------------------------------------
// Task 7: SSE-C Complete fingerprint validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mismatched_sse_c_complete_is_pre_kubo_and_upload_remains_retryable() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted part");
    harness.set_cat_body("QmEncryptedPart", ciphertext);

    let calls_before = kubo_call_counts(&harness).await;
    let upload_before = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("upload before rejected completes");
    let part_before = store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
        .await
        .expect("part before rejected completes");

    let mut missing_algorithm = sse_c_headers_for([7; 32]);
    missing_algorithm.remove("x-amz-server-side-encryption-customer-algorithm");
    let mut mixed_sse = sse_c_headers_for([7; 32]);
    mixed_sse.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    for headers in [sse_c_headers_for([8; 32]), missing_algorithm, mixed_sse] {
        let response = complete_multipart_with_headers(
            &harness,
            "customer-encrypted.bin",
            &upload_id,
            &[(1, etag.clone())],
            headers,
        )
        .await;
        assert_s3_error(
            response,
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "SSE-C",
        )
        .await;
        assert_eq!(
            kubo_call_counts(&harness).await,
            calls_before,
            "rejected CompleteMultipartUpload must not call Kubo"
        );
        assert_eq!(
            store::multipart::get_upload(harness.state.store.db(), &upload_id)
                .await
                .expect("unchanged upload row"),
            upload_before,
            "rejected CompleteMultipartUpload must retain the upload row"
        );
        assert_eq!(
            store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
                .await
                .expect("unchanged part row"),
            part_before,
            "rejected CompleteMultipartUpload must retain the part row"
        );
    }

    let response = complete_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &upload_id,
        &[(1, etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn legacy_sse_c_complete_claims_before_corrupt_ciphertext_error() {
    let harness = start_harness(scripted(&["QmEncryptedPart", "QmRoot"], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part = signed_put(
        &harness,
        "legacy-customer-encrypted.bin",
        &[("partNumber", "1"), ("uploadId", upload_id.as_str())],
        b"legacy encrypted multipart part".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(part.status(), StatusCode::OK);
    let etag = part
        .headers()
        .get(http::header::ETAG)
        .expect("UploadPart ETag")
        .to_str()
        .expect("UploadPart ETag text")
        .trim_matches('"')
        .to_owned();
    harness
        .state
        .store
        .db()
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "UPDATE multipart_uploads SET sse_c_key_fingerprint = NULL WHERE upload_id = '{upload_id}'"
            ),
        ))
        .await
        .expect("clear legacy fingerprint");
    harness.set_cat_body("QmEncryptedPart", b"corrupt ciphertext".to_vec());
    let calls_before = kubo_call_counts(&harness).await;

    let response = complete_multipart_with_headers(
        &harness,
        "legacy-customer-encrypted.bin",
        &upload_id,
        &[(1, etag.clone())],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidPart", "decrypt").await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        (
            calls_before.0,
            calls_before.1 + 1,
            calls_before.2,
            calls_before.3
        ),
        "corrupt ciphertext must cat once without root add, pin, or unpin"
    );

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("retryable legacy upload");
    let expected = harness
        .state
        .master_key
        .sse_c_key_fingerprint(&ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] });
    assert_eq!(
        upload.sse_c_key_fingerprint.as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        store::multipart::get_part(harness.state.store.db(), &upload_id, 1)
            .await
            .expect("retryable legacy part")
            .etag,
        etag
    );
}

#[tokio::test]
async fn sse_c_abort_requires_no_customer_key() {
    let harness = start_harness(scripted(&[], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "customer-encrypted.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let response = abort_multipart(&harness, "customer-encrypted.bin", &upload_id).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed parts")
            .is_empty()
    );
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

// ---------------------------------------------------------------------------
// Task 8: standard operation compatibility regressions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standard_put_sse_s3_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let response = signed_put(
        &harness,
        "encrypted.bin",
        &[],
        b"encrypted bytes".to_vec(),
        headers,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-amz-server-side-encryption"], "AES256");
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "encrypted.bin")
            .await
            .expect("encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_some());
}

#[tokio::test]
async fn test_standard_put_sse_c_still_succeeds() {
    let harness = start_harness(standard_script(1)).await;
    let response = signed_put(
        &harness,
        "customer-encrypted.bin",
        &[],
        b"customer encrypted bytes".to_vec(),
        sse_c_headers(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "customer-encrypted.bin",
    )
    .await
    .expect("customer encrypted latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());
}

#[tokio::test]
async fn test_standard_multipart_signed_still_succeeds() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart.bin", &[]).await;
    let etag = upload_part(&harness, "multipart.bin", &upload_id, 1, completed.clone()).await;
    let response = complete_multipart(&harness, "multipart.bin", &upload_id, &[(1, etag)]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "multipart.bin")
            .await
            .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &["QmPart", "QmRoot"], &[]).await;
    assert_signed_body(&harness, "multipart.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

#[tokio::test]
async fn test_standard_multipart_complete_accepts_weak_part_etag_and_checksums() {
    let completed = b"standard multipart bytes".to_vec();
    let harness = start_harness(scripted(
        &["QmPart", "QmRoot"],
        vec![("QmPart", completed.clone()), ("QmRoot", completed.clone())],
    ))
    .await;
    let upload_id = create_multipart(&harness, "multipart-checksums.bin", &[]).await;
    let etag = upload_part(
        &harness,
        "multipart-checksums.bin",
        &upload_id,
        1,
        completed.clone(),
    )
    .await;
    let weak_etag = format!("W/\"{etag}\"");
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag><ChecksumCRC32>crc32-value</ChecksumCRC32><ChecksumCRC32C>crc32c-value</ChecksumCRC32C><ChecksumCRC64NVME>crc64nvme-value</ChecksumCRC64NVME><ChecksumSHA1>sha1-value</ChecksumSHA1><ChecksumSHA256>sha256-value</ChecksumSHA256></Part></CompleteMultipartUpload>",
        quick_xml::escape::escape(&weak_etag),
    );
    assert!(xml.contains(&format!("W/&quot;{etag}&quot;")));
    let response =
        complete_multipart_xml(&harness, "multipart-checksums.bin", &upload_id, xml).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.expect("complete response body");
    assert!(body.contains("<CompleteMultipartUploadResult>"));
    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "multipart-checksums.bin",
    )
    .await
    .expect("completed multipart latest row");
    assert_eq!(latest.cid, "QmRoot");
    assert!(latest.multipart);
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err()
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("removed multipart parts")
            .is_empty()
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec!["QmPart"]
    );
    assert_signed_body(&harness, "multipart-checksums.bin", &completed).await;
    assert_pin_calls(&harness, "/api/v0/pin/rm", &[], &["QmPart", "QmRoot"]).await;
}

// ---------------------------------------------------------------------------
// Task 9: standard PutObject CID response headers and presigned TCP contract
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_standard_presigned_put_get_and_tamper_contract() {
    let cid = "QmStandardPresignedCid";
    let key = "standard-presigned.bin";
    let tampered_key = "tampered-presigned.bin";
    let payload = b"standard presigned exact payload".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let client = reqwest::Client::new();
    let put_url = presign_sigv4_query(
        &reqwest::Method::PUT,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );

    let put = client
        .put(&put_url)
        .body(payload.clone())
        .send()
        .await
        .expect("presigned standard PUT");
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(
        put.headers()[http::header::ETAG],
        "\"QmStandardPresignedCid\""
    );
    assert_eq!(put.headers()["x-amz-meta-ipfs-cid"], cid);
    assert_eq!(
        put.headers()["x-amz-meta-ipfs-url"],
        "ipfs://QmStandardPresignedCid"
    );
    assert_eq!(harness.captured_add_file_bytes(), vec![payload.clone()]);

    harness.set_cat_body(cid, payload.clone());
    let get_url = presign_sigv4_query(
        &reqwest::Method::GET,
        &harness.endpoint,
        &harness.bucket,
        key,
        &[],
        "test",
        "test",
        None,
        900,
        Utc::now(),
    );
    let get = client
        .get(get_url)
        .send()
        .await
        .expect("presigned standard GET");
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes().await.expect("presigned GET body").as_ref(),
        payload
    );

    let calls_before_tampering = kubo_call_counts(&harness).await;
    let tampered_path_url =
        put_url.replacen("/standard-presigned.bin?", "/tampered-presigned.bin?", 1);
    let tampered_path = client
        .put(tampered_path_url)
        .body(b"tampered path payload".to_vec())
        .send()
        .await
        .expect("tampered presigned path PUT");
    assert_s3_error(
        tampered_path,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;

    let (signed_url_prefix, signature) = put_url
        .rsplit_once("X-Amz-Signature=")
        .expect("presigned URL includes signature");
    let replacement = if signature.starts_with('0') { '1' } else { '0' };
    let tampered_signature_url = format!(
        "{signed_url_prefix}X-Amz-Signature={replacement}{}",
        &signature[1..]
    );
    let tampered_signature = client
        .put(tampered_signature_url)
        .body(b"tampered signature payload".to_vec())
        .send()
        .await
        .expect("tampered presigned signature PUT");
    assert_s3_error(
        tampered_signature,
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "",
    )
    .await;
    assert_eq!(kubo_call_counts(&harness).await, calls_before_tampering);
    assert_latest_absent(&harness, tampered_key).await;
}

#[tokio::test]
async fn put_object_cid_headers_absent_on_failure() {
    let harness = start_harness(KuboScript {
        add_replies: vec![AddReply::Error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "forced add failure",
        )],
        cat_bodies: HashMap::new(),
    })
    .await;

    let response = signed_put(
        &harness,
        "failed-cid-header.bin",
        &[],
        b"failed put payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(response.headers().get("x-amz-meta-ipfs-cid").is_none());
    assert!(response.headers().get("x-amz-meta-ipfs-url").is_none());
    assert_latest_absent(&harness, "failed-cid-header.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (1, 0, 0, 0));
}

// ---------------------------------------------------------------------------
// Task 10: authoritative real-TCP encryption, multipart, and range matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn v03_plaintext_get_and_head_range_matrix() {
    let cid = "QmV03PlaintextRange";
    let plaintext = b"0123456789".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "plaintext-range.bin",
        &[],
        plaintext.clone(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    harness.set_cat_body(cid, plaintext.clone());

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=2-5"));
    let ranged = signed_get_with_headers(&harness, "plaintext-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "4");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 2-5/10"
    );
    assert_eq!(
        ranged.bytes().await.expect("ranged GET body").as_ref(),
        b"2345"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(cat_requests.len(), 1, "ranged plaintext GET cats once");
    assert_eq!(
        cat_requests[0]
            .url
            .query_pairs()
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect::<Vec<_>>(),
        vec![
            ("arg".to_owned(), cid.to_owned()),
            ("offset".to_owned(), "2".to_owned()),
            ("length".to_owned(), "4".to_owned()),
        ]
    );

    let calls_before_unsatisfiable = kubo_call_counts(&harness).await;
    let mut unsatisfiable_headers = HeaderMap::new();
    unsatisfiable_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=10-12"));
    let unsatisfiable =
        signed_get_with_headers(&harness, "plaintext-range.bin", unsatisfiable_headers).await;
    assert_s3_error(
        unsatisfiable,
        StatusCode::RANGE_NOT_SATISFIABLE,
        "InvalidRange",
        "",
    )
    .await;
    assert_eq!(
        kubo_call_counts(&harness).await,
        calls_before_unsatisfiable,
        "unsatisfiable range must not call Kubo"
    );

    let head = signed_head(&harness, "plaintext-range.bin", Some("bytes=2-5")).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(head.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(head.headers().get(http::header::CONTENT_RANGE).is_none());
    assert!(head.bytes().await.expect("ranged HEAD body").is_empty());
}

#[tokio::test]
async fn v03_sse_s3_put_get_and_range_matrix() {
    let cid = "QmV03SseS3";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;
    let mut encryption_headers = HeaderMap::new();
    encryption_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );

    let put = signed_put(
        &harness,
        "sse-s3-range.bin",
        &[],
        plaintext.clone(),
        encryption_headers,
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    assert_eq!(put.headers()["x-amz-server-side-encryption"], "AES256");
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-S3 ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-S3 Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let full = signed_get(&harness, "sse-s3-range.bin").await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-S3 GET body").as_ref(),
        plaintext
    );

    let mut range_headers = HeaderMap::new();
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=4-9"));
    let ranged = signed_get_with_headers(&harness, "sse-s3-range.bin", range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 4-9/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-S3 GET body")
            .as_ref(),
        b"456789"
    );

    let cat_requests = harness
        .kubo
        .received_requests()
        .await
        .expect("Kubo request log")
        .into_iter()
        .filter(|request| request.url.path() == "/api/v0/cat")
        .collect::<Vec<_>>();
    assert_eq!(
        cat_requests.len(),
        2,
        "full and ranged SSE-S3 GET cat once each"
    );
    for request in cat_requests {
        assert_eq!(
            request
                .url
                .query_pairs()
                .find(|(name, _)| name == "arg")
                .map(|(_, value)| value.into_owned())
                .as_deref(),
            Some(cid)
        );
        assert!(
            request
                .url
                .query_pairs()
                .all(|(name, _)| !matches!(name.as_ref(), "bytes" | "offset" | "length")),
            "encrypted GET must fully cat, decrypt, then slice"
        );
    }
}

#[tokio::test]
async fn v03_sse_c_put_get_and_wrong_key_range_matrix() {
    let cid = "QmV03SseC";
    let plaintext = b"0123456789abcdef".to_vec();
    let harness = start_harness(scripted(&[cid], vec![])).await;

    let put = signed_put(
        &harness,
        "sse-c-range.bin",
        &[],
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    assert_put_cid_headers(&put, cid);
    let ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured SSE-C ciphertext");
    assert_ne!(
        ciphertext, plaintext,
        "SSE-C Kubo add must receive ciphertext"
    );
    harness.set_cat_body(cid, ciphertext);

    let latest =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "sse-c-range.bin")
            .await
            .expect("SSE-C latest row");
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let full =
        signed_get_with_headers(&harness, "sse-c-range.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(
        full.bytes().await.expect("full SSE-C GET body").as_ref(),
        plaintext
    );

    let mut correct_range_headers = sse_c_headers_for([7; 32]);
    correct_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let ranged = signed_get_with_headers(&harness, "sse-c-range.bin", correct_range_headers).await;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(ranged.headers()[http::header::CONTENT_LENGTH], "6");
    assert_eq!(
        ranged.headers()[http::header::CONTENT_RANGE],
        "bytes 3-8/16"
    );
    assert_eq!(
        ranged
            .bytes()
            .await
            .expect("ranged SSE-C GET body")
            .as_ref(),
        b"345678"
    );

    let mut wrong_range_headers = sse_c_headers_for([8; 32]);
    wrong_range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=3-8"));
    let wrong_key = signed_get_with_headers(&harness, "sse-c-range.bin", wrong_range_headers).await;
    assert_eq!(wrong_key.status(), StatusCode::FORBIDDEN);
    let wrong_body = wrong_key.text().await.expect("wrong-key SSE-C error body");
    assert!(
        wrong_body.contains("AccessDenied"),
        "wrong-key SSE-C response: {wrong_body}"
    );
    assert!(
        !wrong_body.contains(std::str::from_utf8(&plaintext).expect("plaintext is UTF-8")),
        "wrong-key SSE-C error must not leak plaintext: {wrong_body}"
    );
}

#[tokio::test]
async fn v03_sse_c_multipart_round_trip_matrix() {
    let part_cid = "QmV03SseCPart";
    let root_cid = "QmV03SseCRoot";
    let plaintext = b"SSE-C multipart plaintext".to_vec();
    let harness = start_harness(scripted(&[part_cid, root_cid], vec![])).await;
    let upload_id = create_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;

    let upload = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .expect("SSE-C multipart upload row");
    let fingerprint = upload
        .sse_c_key_fingerprint
        .as_deref()
        .expect("versioned SSE-C key fingerprint");
    assert!(fingerprint.starts_with("v1:hmac-sha256:"));
    assert_eq!(fingerprint.len(), "v1:hmac-sha256:".len() + 64);
    assert!(
        !fingerprint.contains(&base64::engine::general_purpose::STANDARD.encode([7; 32])),
        "SSE-C fingerprint must not persist the raw customer key"
    );
    assert!(
        !fingerprint
            .contains(&base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0)),
        "SSE-C fingerprint must not persist the customer-key MD5"
    );

    let part_etag = upload_part_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .next()
        .expect("captured encrypted multipart part");
    assert_ne!(
        part_ciphertext, plaintext,
        "SSE-C multipart part Kubo add must receive ciphertext"
    );
    harness.set_cat_body(part_cid, part_ciphertext);

    let completed = complete_multipart_with_headers(
        &harness,
        "sse-c-multipart.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert!(
        completed
            .text()
            .await
            .expect("SSE-C complete body")
            .contains("<CompleteMultipartUploadResult>")
    );
    let root_ciphertext = harness
        .captured_add_file_bytes()
        .into_iter()
        .nth(1)
        .expect("captured encrypted multipart root");
    assert_ne!(
        root_ciphertext, plaintext,
        "SSE-C multipart root Kubo add must receive ciphertext"
    );
    harness.set_cat_body(root_cid, root_ciphertext);

    let latest = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "sse-c-multipart.bin",
    )
    .await
    .expect("SSE-C completed multipart object");
    assert_eq!(latest.cid, root_cid);
    assert!(latest.encrypted);
    assert!(latest.key_wrap.is_none());

    let get =
        signed_get_with_headers(&harness, "sse-c-multipart.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.bytes()
            .await
            .expect("SSE-C multipart GET body")
            .as_ref(),
        plaintext
    );
    assert!(
        store::multipart::get_upload(harness.state.store.db(), &upload_id)
            .await
            .is_err(),
        "completed multipart upload row must be removed"
    );
    assert!(
        store::multipart::list_parts(harness.state.store.db(), &upload_id)
            .await
            .expect("completed multipart parts")
            .is_empty(),
        "completed multipart part rows must be removed"
    );
    assert_eq!(
        kubo_call_counts(&harness).await,
        (2, 3, 2, 0),
        "SSE-C Complete pre-authenticates its part, cats it again to build the root, then GET cats the root"
    );
    assert_eq!(
        kubo_query_args(&harness, "/api/v0/cat").await,
        vec![
            part_cid.to_owned(),
            part_cid.to_owned(),
            root_cid.to_owned(),
        ],
        "SSE-C multipart must cat its part twice and its root once"
    );
    assert_pin_calls(&harness, "/api/v0/pin/add", &[part_cid, root_cid], &[]).await;
    assert!(
        kubo_query_args(&harness, "/api/v0/pin/rm").await.is_empty(),
        "SSE-C multipart success must not unpin"
    );
}

#[tokio::test]
async fn v03_put_object_cid_header_matrix() {
    let plaintext_harness = start_harness(scripted(&["QmV03PlainPut"], vec![])).await;
    let plaintext_put = signed_put(
        &plaintext_harness,
        "plain-cid.bin",
        &[],
        b"plain CID header payload".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(plaintext_put.status(), StatusCode::OK);
    assert_put_cid_headers(&plaintext_put, "QmV03PlainPut");

    let sse_s3_harness = start_harness(scripted(&["QmV03SseS3Put"], vec![])).await;
    let mut sse_s3_headers = HeaderMap::new();
    sse_s3_headers.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    let sse_s3_put = signed_put(
        &sse_s3_harness,
        "sse-s3-cid.bin",
        &[],
        b"SSE-S3 CID header payload".to_vec(),
        sse_s3_headers,
    )
    .await;
    assert_eq!(sse_s3_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_s3_put, "QmV03SseS3Put");

    let sse_c_harness = start_harness(scripted(&["QmV03SseCPut"], vec![])).await;
    let sse_c_put = signed_put(
        &sse_c_harness,
        "sse-c-cid.bin",
        &[],
        b"SSE-C CID header payload".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(sse_c_put.status(), StatusCode::OK);
    assert_put_cid_headers(&sse_c_put, "QmV03SseCPut");
}

#[tokio::test]
async fn v03_random_nonce_retries_and_part_replacement_never_reuse_nonce() {
    let plaintext = b"identical encrypted multipart payload".to_vec();
    let harness = start_harness(scripted(
        &[
            "QmNoncePart1",
            "QmNoncePart2",
            "QmNonceRoot1",
            "QmNonceRoot2",
        ],
        vec![],
    ))
    .await;
    let upload_id =
        create_multipart_with_headers(&harness, "nonce.bin", &[], sse_c_headers_for([7; 32])).await;

    upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let replacement_etag = upload_part_with_headers(
        &harness,
        "nonce.bin",
        &upload_id,
        1,
        plaintext.clone(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(1)
        .cloned()
        .expect("replacement ciphertext");
    harness.set_cat_body("QmNoncePart2", part_ciphertext);

    for _ in 0..2 {
        ipfs_s3_gateway::s3::ops::multipart::complete_multipart_upload_inner(
            &harness.state,
            inner_complete_request(
                &harness,
                "nonce.bin",
                &upload_id,
                &replacement_etag,
                sse_c_headers_for([7; 32]),
            ),
        )
        .await
        .expect("retryable CompleteMultipartUpload inner result");
    }

    let captured = harness.captured_add_file_bytes();
    assert_eq!(captured.len(), 4);
    let key = ipfs_s3_gateway::crypto::ObjectKey { bytes: [7; 32] };
    let mut nonces = std::collections::HashSet::new();
    for ciphertext in &captured {
        assert_eq!(
            ipfs_s3_gateway::crypto::aes_gcm::decrypt_chunk(&key, ciphertext)
                .expect("captured frame decrypts")
                .as_ref(),
            plaintext
        );
        nonces.insert(<[u8; 12]>::try_from(&ciphertext[..12]).unwrap());
    }
    assert_eq!(nonces.len(), captured.len());
}

#[tokio::test]
async fn fingerprinted_sse_c_get_head_wrong_key_is_zero_kubo_access_denied() {
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "fingerprinted.bin",
        "QmFingerprinted",
        b"fingerprinted body",
        true,
        18,
    )
    .await;

    let get =
        signed_get_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(get, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let head =
        signed_head_with_headers(&harness, "fingerprinted.bin", sse_c_headers_for([8; 32])).await;
    assert_eq!(head.status(), StatusCode::FORBIDDEN);
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_get_claims_after_exact_authentication_and_streams_second_cat() {
    let plaintext = b"legacy get body";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-get.bin",
        "QmLegacyGet",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response =
        signed_get_with_headers(&harness, "legacy-get.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), plaintext);

    let object =
        store::object::get_latest(harness.state.store.db(), &harness.bucket, "legacy-get.bin")
            .await
            .unwrap();
    assert!(object.sse_c_key_fingerprint.is_some());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_head_and_head_range_authenticate_once_then_zero_kubo() {
    let plaintext = b"legacy head body";
    let harness = start_harness(scripted(&[], vec![])).await;
    for (key, cid) in [
        ("legacy-head.bin", "QmLegacyHead"),
        ("legacy-head-range.bin", "QmLegacyHeadRange"),
    ] {
        seed_sse_c_object(
            &harness,
            key,
            cid,
            plaintext,
            false,
            i64::try_from(plaintext.len()).unwrap(),
        )
        .await;
    }

    let first =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(first.status(), StatusCode::OK);
    let mut range_headers = sse_c_headers_for([7; 32]);
    range_headers.insert(http::header::RANGE, HeaderValue::from_static("bytes=1-4"));
    let first_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers.clone()).await;
    assert_eq!(first_range.status(), StatusCode::OK);
    assert_eq!(first_range.headers()[http::header::CONTENT_LENGTH], "4");
    assert!(first_range.bytes().await.unwrap().is_empty());
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));

    let repeated =
        signed_head_with_headers(&harness, "legacy-head.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(repeated.status(), StatusCode::OK);
    let repeated_range =
        signed_head_with_headers(&harness, "legacy-head-range.bin", range_headers).await;
    assert_eq!(repeated_range.status(), StatusCode::OK);
    assert_eq!(kubo_call_counts(&harness).await, (0, 2, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_wrong_key_or_size_mismatch_never_claims() {
    let plaintext = b"legacy authentication";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-key.bin",
        "QmLegacyWrongKey",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    seed_sse_c_object(
        &harness,
        "legacy-wrong-size.bin",
        "QmLegacyWrongSize",
        plaintext,
        false,
        i64::try_from(plaintext.len() + 1).unwrap(),
    )
    .await;
    seed_sse_c_object(&harness, "legacy-empty.bin", "QmLegacyEmpty", b"", false, 0).await;

    let wrong_key =
        signed_get_with_headers(&harness, "legacy-wrong-key.bin", sse_c_headers_for([8; 32])).await;
    assert_s3_error(wrong_key, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    let wrong_size = signed_head_with_headers(
        &harness,
        "legacy-wrong-size.bin",
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(wrong_size.status(), StatusCode::FORBIDDEN);
    let empty =
        signed_head_with_headers(&harness, "legacy-empty.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(empty.status(), StatusCode::FORBIDDEN);

    for key in [
        "legacy-wrong-key.bin",
        "legacy-wrong-size.bin",
        "legacy-empty.bin",
    ] {
        assert!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_none(),
            "{key} must remain unclaimed"
        );
    }
}

#[tokio::test]
async fn sse_c_get_and_head_return_customer_response_fields() {
    let plaintext = b"response fields";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "response-fields.bin",
        "QmResponseFields",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    let expected_md5 = base64::engine::general_purpose::STANDARD.encode(md5::compute([7; 32]).0);

    let get =
        signed_get_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        get.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
    assert_eq!(get.bytes().await.unwrap().as_ref(), plaintext);

    let head =
        signed_head_with_headers(&harness, "response-fields.bin", sse_c_headers_for([7; 32])).await;
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-algorithm"],
        "AES256"
    );
    assert_eq!(
        head.headers()["x-amz-server-side-encryption-customer-key-md5"],
        expected_md5
    );
}

#[tokio::test]
async fn copy_sse_c_source_headers_are_required_and_wrong_key_never_publishes() {
    let plaintext = b"copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "copy-source.bin",
        "QmCopySource",
        plaintext,
        true,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let valid = copy_source_sse_c_headers_for([7; 32]);
    let mut malformed = Vec::new();
    for missing in [
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        "x-amz-copy-source-server-side-encryption-customer-key",
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
    ] {
        let mut headers = valid.clone();
        headers.remove(missing);
        malformed.push(headers);
    }
    let mut wrong_algorithm = valid.clone();
    wrong_algorithm.insert(
        "x-amz-copy-source-server-side-encryption-customer-algorithm",
        HeaderValue::from_static("AES128"),
    );
    malformed.push(wrong_algorithm);
    let mut invalid_base64 = valid.clone();
    invalid_base64.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_static("not-base64"),
    );
    malformed.push(invalid_base64);
    let mut short_key = valid.clone();
    short_key.insert(
        "x-amz-copy-source-server-side-encryption-customer-key",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([7; 31])).unwrap(),
    );
    malformed.push(short_key);
    let mut wrong_md5 = valid.clone();
    wrong_md5.insert(
        "x-amz-copy-source-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&base64::engine::general_purpose::STANDARD.encode([0; 16])).unwrap(),
    );
    malformed.push(wrong_md5);
    malformed.push(sse_c_headers_for([7; 32]));

    for (index, headers) in malformed.into_iter().enumerate() {
        let destination = format!("invalid-copy-{index}.bin");
        let import_id = format!("invalid-copy-import-{index}");
        seed_running_import(&harness, &import_id, &destination, None).await;
        let response = signed_copy(&harness, "copy-source.bin", &destination, headers).await;
        assert_s3_error(response, StatusCode::BAD_REQUEST, "InvalidArgument", "").await;
        assert_import_state(&harness, &import_id, "running").await;
        assert_latest_absent(&harness, &destination).await;
    }

    seed_running_import(
        &harness,
        "wrong-key-copy-import",
        "wrong-key-copy.bin",
        None,
    )
    .await;
    let response = signed_copy(
        &harness,
        "copy-source.bin",
        "wrong-key-copy.bin",
        copy_source_sse_c_headers_for([8; 32]),
    )
    .await;
    assert_s3_error(response, StatusCode::FORBIDDEN, "AccessDenied", "").await;
    assert_import_state(&harness, "wrong-key-copy-import", "running").await;
    assert_latest_absent(&harness, "wrong-key-copy.bin").await;
    assert_eq!(kubo_call_counts(&harness).await, (0, 0, 0, 0));
}

#[tokio::test]
async fn legacy_sse_c_copy_claims_then_copies_fingerprint() {
    let plaintext = b"legacy copy source";
    let harness = start_harness(scripted(&[], vec![])).await;
    seed_sse_c_object(
        &harness,
        "legacy-copy-source.bin",
        "QmLegacyCopySource",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;

    let response = signed_copy(
        &harness,
        "legacy-copy-source.bin",
        "legacy-copy-destination.bin",
        copy_source_sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let source = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-source.bin",
    )
    .await
    .unwrap();
    let destination = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "legacy-copy-destination.bin",
    )
    .await
    .unwrap();
    assert!(source.sse_c_key_fingerprint.is_some());
    assert_eq!(
        destination.sse_c_key_fingerprint,
        source.sse_c_key_fingerprint
    );
    assert_eq!(kubo_call_counts(&harness).await, (0, 1, 1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn legacy_sse_c_copy_admits_destination_before_cat() {
    let plaintext = b"blocked legacy copy source";
    let (harness, mut kubo_block) =
        start_blocking_harness(scripted(&[], vec![]), KuboBlockTarget::Cat).await;
    seed_sse_c_object(
        &harness,
        "blocked-legacy-copy-source.bin",
        "QmBlockedLegacyCopySource",
        plaintext,
        false,
        i64::try_from(plaintext.len()).unwrap(),
    )
    .await;
    seed_running_import(
        &harness,
        "blocked-legacy-copy-import",
        "blocked-legacy-copy-destination.bin",
        None,
    )
    .await;

    let endpoint = OwnedTestEndpoint::from(&harness);
    let copy_task = tokio::spawn(async move {
        signed_copy(
            &endpoint,
            "blocked-legacy-copy-source.bin",
            "blocked-legacy-copy-destination.bin",
            copy_source_sse_c_headers_for([7; 32]),
        )
        .await
    });
    kubo_block.wait_until_blocked().await;
    assert_import_state(&harness, "blocked-legacy-copy-import", "superseded").await;

    kubo_block.release();
    let response = copy_task.await.expect("join blocked legacy CopyObject");
    assert_eq!(response.status(), StatusCode::OK);
    let source = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "blocked-legacy-copy-source.bin",
    )
    .await
    .expect("load claimed legacy source");
    let destination = store::object::get_latest(
        harness.state.store.db(),
        &harness.bucket,
        "blocked-legacy-copy-destination.bin",
    )
    .await
    .expect("load copied destination");
    assert!(source.sse_c_key_fingerprint.is_some());
    assert_eq!(
        destination.sse_c_key_fingerprint,
        source.sse_c_key_fingerprint
    );
}

#[tokio::test]
async fn all_object_publication_paths_and_completion_reconciliation_keep_fingerprint() {
    let harness = start_harness(scripted(
        &[
            "QmPlainPublication",
            "QmSseS3Publication",
            "QmSseCPublication",
            "QmPartPublication",
            "QmRootPublication",
        ],
        vec![],
    ))
    .await;
    assert_eq!(
        signed_put(
            &harness,
            "plain-publication.bin",
            &[],
            b"plain".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let mut sse_s3 = HeaderMap::new();
    sse_s3.insert(
        "x-amz-server-side-encryption",
        HeaderValue::from_static("AES256"),
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-s3-publication.bin",
            &[],
            b"sse-s3".to_vec(),
            sse_s3,
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &harness,
            "sse-c-publication.bin",
            &[],
            b"sse-c".to_vec(),
            sse_c_headers_for([7; 32]),
        )
        .await
        .status(),
        StatusCode::OK
    );
    for (key, expected) in [
        ("plain-publication.bin", false),
        ("sse-s3-publication.bin", false),
        ("sse-c-publication.bin", true),
    ] {
        assert_eq!(
            store::object::get_latest(harness.state.store.db(), &harness.bucket, key)
                .await
                .unwrap()
                .sse_c_key_fingerprint
                .is_some(),
            expected,
            "publication path {key}"
        );
    }

    let upload_id = create_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &[],
        sse_c_headers_for([7; 32]),
    )
    .await;
    let upload_fingerprint = store::multipart::get_upload(harness.state.store.db(), &upload_id)
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .unwrap();
    let part_etag = upload_part_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        1,
        b"multipart publication".to_vec(),
        sse_c_headers_for([7; 32]),
    )
    .await;
    let part_ciphertext = harness
        .captured_add_file_bytes()
        .get(3)
        .cloned()
        .expect("multipart publication part ciphertext");
    harness.set_cat_body("QmPartPublication", part_ciphertext);
    let complete = complete_multipart_with_headers(
        &harness,
        "complete-publication.bin",
        &upload_id,
        &[(1, part_etag)],
        sse_c_headers_for([7; 32]),
    )
    .await;
    assert_eq!(complete.status(), StatusCode::OK);
    assert_eq!(
        store::object::get_latest(
            harness.state.store.db(),
            &harness.bucket,
            "complete-publication.bin",
        )
        .await
        .unwrap()
        .sse_c_key_fingerprint
        .as_deref(),
        Some(upload_fingerprint.as_str())
    );
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

fn import_url_xml(url: &str) -> String {
    format!(
        "<IPFS3ImportRequest><URL>{}</URL></IPFS3ImportRequest>",
        quick_xml::escape::escape(url)
    )
}

fn accepted_import_job_id(response: &http::Response<Vec<u8>>) -> String {
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    response.headers()["x-ipfs3-import-job-id"]
        .to_str()
        .expect("import job header")
        .to_owned()
}

async fn import_result_count(harness: &ImportHarness, job_id: &str) -> u64 {
    store::entities::import_job_result::Entity::find()
        .filter(store::entities::import_job_result::Column::JobId.eq(job_id))
        .count(harness.state.store.db())
        .await
        .expect("count import result rows")
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

fn xml_element_text(xml: &str, name: &str) -> String {
    let opening = format!("<{name}>");
    let closing = format!("</{name}>");
    let start = xml
        .find(&opening)
        .unwrap_or_else(|| panic!("missing {opening}: {xml}"))
        + opening.len();
    let end = xml[start..]
        .find(&closing)
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing {closing}: {xml}"));
    quick_xml::escape::unescape(&xml[start..end])
        .expect("XML element escaping")
        .into_owned()
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

#[tokio::test]
async fn lifecycle_signed_configuration() {
    let harness = start_lifecycle_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await;

    let absent = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(absent.status(), StatusCode::NOT_FOUND);
    assert!(
        absent
            .text()
            .await
            .expect("read absent lifecycle error")
            .contains("NoSuchLifecycleConfiguration"),
        "absent lifecycle configuration must use the S3 error code"
    );

    let put = signed_put_bucket_lifecycle_configuration_xml_with_headers(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>canonical</ID><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix>\
         <Tag><Key>class</Key><Value>archive</Value></Tag></And></Filter>\
         <Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration>"
            .to_owned(),
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(put.status(), StatusCode::OK);
    let first = stored_lifecycle_configuration_for(&harness).await;
    assert_eq!(first.revision, 1);

    let get = signed_get_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(get.status(), StatusCode::OK);
    let canonical_xml = get.text().await.expect("read canonical lifecycle XML");
    assert_eq!(xml_element_values(&canonical_xml, "ID"), vec!["canonical"]);
    assert_eq!(xml_element_values(&canonical_xml, "Prefix"), vec!["logs/"]);
    assert_eq!(xml_element_values(&canonical_xml, "Days"), vec!["3"]);
    assert!(!canonical_xml.contains("Transition"));

    let replacement = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        lifecycle_configuration_xml("replacement"),
    )
    .await;
    assert_eq!(replacement.status(), StatusCode::OK);
    let second = stored_lifecycle_configuration_for(&harness).await;
    assert_eq!(second.revision, 2);
    let replacement_get = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(replacement_get.status(), StatusCode::OK);
    let replacement_xml = replacement_get
        .text()
        .await
        .expect("read replacement lifecycle XML");
    assert_eq!(
        xml_element_values(&replacement_xml, "ID"),
        vec!["replacement"]
    );
    assert!(!replacement_xml.contains("canonical"));

    let wrong_owner = signed_get_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers("different-owner"),
    )
    .await;
    assert_eq!(wrong_owner.status(), StatusCode::FORBIDDEN);
    assert!(
        wrong_owner
            .text()
            .await
            .expect("read owner mismatch error")
            .contains("AccessDenied"),
        "mismatched expected owner must be denied"
    );
    let missing = OwnedTestEndpoint {
        endpoint: harness.endpoint.clone(),
        bucket: "lifecycle-missing-bucket".to_owned(),
    };
    let missing_response = signed_put_bucket_lifecycle_configuration_xml(
        &missing,
        lifecycle_configuration_xml("missing"),
    )
    .await;
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
    assert!(
        missing_response
            .text()
            .await
            .expect("read missing bucket error")
            .contains("NoSuchBucket"),
        "missing bucket must retain the S3 error code"
    );

    for invalid_xml in [
        "<LifecycleConfiguration><Rule>".to_owned(),
        "<LifecycleConfiguration><Rule><ID>nested</ID><Status>Enabled</Status>\
         <Filter><UnexpectedNested/></Filter><Expiration><Days>3</Days></Expiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected = signed_put_bucket_lifecycle_configuration_xml(&harness, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read framework XML rejection")
                .contains("MalformedXML"),
            "framework-invalid lifecycle XML must be rejected before write"
        );
        assert_eq!(stored_lifecycle_configuration_for(&harness).await, second);
    }

    let root_unknown = signed_put_bucket_lifecycle_configuration_xml(
        &harness,
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Rule><ID>root-kept</ID><Status>Enabled</Status><Filter/><Expiration><Days>3</Days>\
         </Expiration></Rule><IgnoredByFramework>ignored</IgnoredByFramework>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(root_unknown.status(), StatusCode::OK);
    let root_unknown_xml = signed_get_bucket_lifecycle_configuration(&harness)
        .await
        .text()
        .await
        .expect("read root-child lifecycle projection");
    assert_eq!(
        xml_element_values(&root_unknown_xml, "ID"),
        vec!["root-kept"]
    );
    assert!(!root_unknown_xml.contains("IgnoredByFramework"));
    let before_rejection = stored_lifecycle_configuration_for(&harness).await;

    for invalid_xml in [
        "<LifecycleConfiguration><Rule><ID>transition</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days></Expiration><Transition><Days>1</Days>\
         <StorageClass>STANDARD_IA</StorageClass></Transition></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>noncurrent-transition</ID><Status>Enabled</Status>\
         <Filter/><Expiration><Days>3</Days></Expiration><NoncurrentVersionTransition>\
         <NoncurrentDays>1</NoncurrentDays><StorageClass>STANDARD_IA</StorageClass>\
         </NoncurrentVersionTransition></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>abort-zero</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days></Expiration><AbortIncompleteMultipartUpload>\
         <DaysAfterInitiation>0</DaysAfterInitiation></AbortIncompleteMultipartUpload>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>conflict</ID><Status>Enabled</Status><Filter/>\
         <Expiration><Days>3</Days><Date>2020-01-01T00:00:00Z</Date></Expiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>newer-without-filter</ID><Status>Enabled</Status>\
         <Prefix>logs/</Prefix><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>\
         <NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected = signed_put_bucket_lifecycle_configuration_xml(&harness, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read typed lifecycle rejection")
                .contains("InvalidRequest"),
            "unsupported or contradictory lifecycle configuration must be rejected"
        );
        assert_eq!(
            stored_lifecycle_configuration_for(&harness).await,
            before_rejection,
            "rejected lifecycle configuration must not advance its revision"
        );
    }

    let deleted = signed_delete_bucket_lifecycle_configuration_with_headers(
        &harness,
        expected_bucket_owner_headers(&harness.owner),
    )
    .await;
    assert_eq!(deleted.status(), StatusCode::NO_CONTENT);
    let absent_after_delete = signed_get_bucket_lifecycle_configuration(&harness).await;
    assert_eq!(absent_after_delete.status(), StatusCode::NOT_FOUND);
    harness.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_current() {
    let mut unversioned = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentUnversioned",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &unversioned,
            "unversioned-current",
            &[],
            b"unversioned".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &unversioned,
            lifecycle_current_expiration_xml(
                "unversioned-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    unversioned.run_one_scan_page().await;
    let claim = unversioned.claim_one_action().await;
    assert_eq!(
        unversioned.execute_claim(&claim).await.state,
        "succeeded",
        "unversioned lifecycle expiry must settle"
    );
    assert_eq!(
        signed_get(&unversioned, "unversioned-current")
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
    assert!(
        !signed_list_objects(&unversioned)
            .await
            .text()
            .await
            .expect("read unversioned ordinary list")
            .contains("unversioned-current")
    );
    assert!(
        !signed_list_object_versions(&unversioned, &[])
            .await
            .text()
            .await
            .expect("read unversioned version list")
            .contains("unversioned-current")
    );
    assert!(
        unversioned
            .version_rows("unversioned-current")
            .await
            .is_empty()
    );
    unversioned.assert_no_pin_removal().await;
    unversioned.shutdown().await;

    let mut enabled = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentEnabled",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&enabled, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let enabled_put = signed_put(
        &enabled,
        "enabled-current",
        &[],
        b"enabled".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(enabled_put.status(), StatusCode::OK);
    let enabled_version = enabled_put.headers()["x-amz-version-id"]
        .to_str()
        .expect("enabled public version")
        .to_owned();
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &enabled,
            lifecycle_current_expiration_xml(
                "enabled-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    enabled.run_one_scan_page().await;
    let claim = enabled.claim_one_action().await;
    assert_eq!(enabled.execute_claim(&claim).await.state, "succeeded");
    let enabled_get = signed_get(&enabled, "enabled-current").await;
    assert_eq!(enabled_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(enabled_get.headers()["x-amz-delete-marker"], "true");
    assert!(
        !signed_list_objects(&enabled)
            .await
            .text()
            .await
            .expect("read enabled ordinary list")
            .contains("enabled-current")
    );
    let enabled_versions = signed_list_object_versions(&enabled, &[])
        .await
        .text()
        .await
        .expect("read enabled version list");
    assert!(enabled_versions.contains("<DeleteMarker>"));
    assert!(enabled_versions.contains(&enabled_version));
    assert_eq!(enabled.version_rows("enabled-current").await.len(), 2);
    enabled.assert_no_pin_removal().await;
    enabled.shutdown().await;

    let mut suspended = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleCurrentSuspended",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&suspended, "Suspended")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &suspended,
            "suspended-current",
            &[],
            b"suspended".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &suspended,
            lifecycle_current_expiration_xml(
                "suspended-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    suspended.run_one_scan_page().await;
    let claim = suspended.claim_one_action().await;
    assert_eq!(suspended.execute_claim(&claim).await.state, "succeeded");
    let suspended_get = signed_get(&suspended, "suspended-current").await;
    assert_eq!(suspended_get.status(), StatusCode::NOT_FOUND);
    assert_eq!(suspended_get.headers()["x-amz-delete-marker"], "true");
    assert_eq!(suspended_get.headers()["x-amz-version-id"], "null");
    let suspended_rows = suspended.version_rows("suspended-current").await;
    assert_eq!(suspended_rows.len(), 1);
    assert_eq!(suspended_rows[0].kind, "delete_marker");
    suspended.assert_no_pin_removal().await;
    suspended.shutdown().await;

    let marker_history = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleMarkerHistory",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&marker_history, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put(
            &marker_history,
            "marker-history",
            &[],
            b"history".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_delete_object(&marker_history, "marker-history")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &marker_history,
            lifecycle_current_expiration_xml(
                "marker-with-history",
                "<Expiration><Days>1</Days></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    marker_history.run_one_scan_page().await;
    assert!(marker_history.action_rows().await.is_empty());
    assert_eq!(marker_history.version_rows("marker-history").await.len(), 2);
    marker_history.assert_no_pin_removal().await;
    marker_history.shutdown().await;

    for (rule_id, expiration) in [
        (
            "timed-sole-marker",
            "<Expiration><Days>1</Days></Expiration>",
        ),
        (
            "immediate-sole-marker",
            "<Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>",
        ),
    ] {
        let mut sole_marker = start_lifecycle_harness(KuboScript {
            add_replies: Vec::new(),
            cat_bodies: HashMap::new(),
        })
        .await;
        assert_eq!(
            signed_put_bucket_versioning(&sole_marker, "Enabled")
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            signed_delete_object(&sole_marker, "sole-marker")
                .await
                .status(),
            StatusCode::NO_CONTENT
        );
        let marker = sole_marker.version_rows("sole-marker").await.remove(0);
        if rule_id == "timed-sole-marker" {
            let now =
                ipfs_s3_gateway::store::database_clock::database_now(sole_marker.state.store.db())
                    .await
                    .expect("read timed marker database clock");
            sole_marker
                .set_database_times(
                    &marker.id,
                    now - ChronoDuration::days(3),
                    marker.became_noncurrent_at,
                )
                .await;
        }
        assert_eq!(
            signed_put_bucket_lifecycle_configuration_xml(
                &sole_marker,
                lifecycle_current_expiration_xml(rule_id, expiration),
            )
            .await
            .status(),
            StatusCode::OK
        );
        sole_marker.run_one_scan_page().await;
        let claim = sole_marker.claim_one_action().await;
        assert_eq!(sole_marker.execute_claim(&claim).await.state, "succeeded");
        assert!(sole_marker.version_rows("sole-marker").await.is_empty());
        assert!(
            !signed_list_object_versions(&sole_marker, &[])
                .await
                .text()
                .await
                .expect("read sole-marker version list")
                .contains("<DeleteMarker>")
        );
        sole_marker.assert_no_pin_removal().await;
        sole_marker.shutdown().await;
    }
}

#[tokio::test]
async fn lifecycle_expiration_noncurrent() {
    let validation = start_lifecycle_harness(KuboScript {
        add_replies: Vec::new(),
        cat_bodies: HashMap::new(),
    })
    .await;
    for invalid_xml in [
        "<LifecycleConfiguration><Rule><ID>missing-days</ID><Status>Enabled</Status><Filter/>\
         <NoncurrentVersionExpiration/></Rule></LifecycleConfiguration>"
            .to_owned(),
        "<LifecycleConfiguration><Rule><ID>newer-legacy</ID><Status>Enabled</Status>\
         <Prefix>logs/</Prefix><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays>\
         <NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration>\
         </Rule></LifecycleConfiguration>"
            .to_owned(),
    ] {
        let rejected =
            signed_put_bucket_lifecycle_configuration_xml(&validation, invalid_xml).await;
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
        assert!(
            rejected
                .text()
                .await
                .expect("read noncurrent validation error")
                .contains("InvalidRequest"),
            "invalid noncurrent lifecycle configuration must be rejected"
        );
    }
    validation.shutdown().await;

    let mut marker = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleNoncurrentMarkerContent",
        2,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&marker, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let first = signed_put(
        &marker,
        "noncurrent-marker",
        &[],
        b"first".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_version = first.headers()["x-amz-version-id"]
        .to_str()
        .expect("first noncurrent version")
        .to_owned();
    let marker_response = signed_delete_object(&marker, "noncurrent-marker").await;
    assert_eq!(marker_response.status(), StatusCode::NO_CONTENT);
    let marker_version = marker_response.headers()["x-amz-version-id"]
        .to_str()
        .expect("noncurrent marker version")
        .to_owned();
    let current = signed_put(
        &marker,
        "noncurrent-marker",
        &[],
        b"current".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status(), StatusCode::OK);
    let current_version = current.headers()["x-amz-version-id"]
        .to_str()
        .expect("current noncurrent version")
        .to_owned();
    let marker_rows = marker.version_rows("noncurrent-marker").await;
    assert_eq!(
        marker_rows
            .iter()
            .map(|row| row.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["object", "delete_marker", "object"]
    );
    let marker_row = marker_rows
        .iter()
        .find(|row| row.kind == "delete_marker")
        .expect("noncurrent marker row")
        .clone();
    let now = ipfs_s3_gateway::store::database_clock::database_now(marker.state.store.db())
        .await
        .expect("read noncurrent marker database clock");
    marker
        .set_database_times(
            &marker_row.id,
            marker_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &marker,
            lifecycle_noncurrent_expiration_xml("expire-marker", None),
        )
        .await
        .status(),
        StatusCode::OK
    );
    marker.run_one_scan_page().await;
    let claim = marker.claim_one_action().await;
    assert_eq!(claim.action.action_kind, "expire_noncurrent");
    assert_eq!(marker.execute_claim(&claim).await.state, "succeeded");
    let marker_versions = signed_list_object_versions(&marker, &[])
        .await
        .text()
        .await
        .expect("read noncurrent marker version list");
    assert!(marker_versions.contains(&first_version));
    assert!(marker_versions.contains(&current_version));
    assert!(!marker_versions.contains(&marker_version));
    assert!(
        marker
            .version_rows("noncurrent-marker")
            .await
            .iter()
            .all(|row| row.kind == "object"),
        "exact noncurrent marker deletion must not create another marker"
    );
    marker.assert_no_pin_removal().await;
    marker.shutdown().await;

    let mut threshold = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleNoncurrentThreshold",
        6,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_bucket_versioning(&threshold, "Enabled")
            .await
            .status(),
        StatusCode::OK
    );
    let boundary_old = signed_put(
        &threshold,
        "strict-age",
        &[],
        b"old".to_vec(),
        HeaderMap::new(),
    )
    .await;
    let boundary_old_version = boundary_old.headers()["x-amz-version-id"]
        .to_str()
        .expect("strict age old version")
        .to_owned();
    assert_eq!(boundary_old.status(), StatusCode::OK);
    assert_eq!(
        signed_put(
            &threshold,
            "strict-age",
            &[],
            b"new".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &threshold,
            lifecycle_noncurrent_expiration_xml("strict-age", None),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let boundary_row = threshold.version_rows("strict-age").await.remove(0);
    let now = ipfs_s3_gateway::store::database_clock::database_now(threshold.state.store.db())
        .await
        .expect("read strict-age database clock");
    threshold
        .set_database_times(
            &boundary_row.id,
            boundary_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(1)),
        )
        .await;
    threshold.run_one_scan_page().await;
    assert!(
        threshold.action_rows().await.is_empty(),
        "one incomplete noncurrent day must remain before its strict UTC boundary"
    );
    threshold
        .set_database_times(
            &boundary_row.id,
            boundary_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    threshold.run_one_scan_page().await;
    let claim = threshold.claim_one_action().await;
    assert_eq!(threshold.execute_claim(&claim).await.state, "succeeded");
    assert_eq!(
        signed_get_version(&threshold, "strict-age", &boundary_old_version)
            .await
            .status(),
        StatusCode::NOT_FOUND
    );

    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &threshold,
            lifecycle_noncurrent_expiration_xml("newer-threshold", Some(1)),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let oldest = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"one".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(oldest.status(), StatusCode::OK);
    let oldest_version = oldest.headers()["x-amz-version-id"]
        .to_str()
        .expect("oldest threshold version")
        .to_owned();
    let middle = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"two".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(middle.status(), StatusCode::OK);
    let middle_version = middle.headers()["x-amz-version-id"]
        .to_str()
        .expect("middle threshold version")
        .to_owned();
    assert_eq!(
        signed_put(
            &threshold,
            "newer-threshold",
            &[],
            b"three".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let oldest_row = threshold.version_rows("newer-threshold").await.remove(0);
    threshold
        .set_database_times(
            &oldest_row.id,
            oldest_row.lifecycle_age_started_at,
            Some(now - ChronoDuration::days(3)),
        )
        .await;
    threshold.run_one_scan_page().await;
    assert!(
        threshold
            .action_rows()
            .await
            .iter()
            .all(|action| action.state != "pending"),
        "exactly N newer noncurrent versions must not expire the target"
    );
    let successor = signed_put(
        &threshold,
        "newer-threshold",
        &[],
        b"four".to_vec(),
        HeaderMap::new(),
    )
    .await;
    assert_eq!(successor.status(), StatusCode::OK);
    let successor_version = successor.headers()["x-amz-version-id"]
        .to_str()
        .expect("successor threshold version")
        .to_owned();
    threshold.run_one_scan_page().await;
    let claim = threshold.claim_one_action().await;
    assert_eq!(
        claim.action.target_public_version_id.as_deref(),
        Some(oldest_version.as_str())
    );
    assert_eq!(threshold.execute_claim(&claim).await.state, "succeeded");
    let rows_after = threshold.version_rows("newer-threshold").await;
    assert!(
        rows_after
            .iter()
            .all(|row| row.version_id.as_deref() != Some(oldest_version.as_str()))
    );
    assert!(
        rows_after.iter().any(
            |row| row.version_id.as_deref() == Some(successor_version.as_str()) && row.is_latest
        ),
        "exact noncurrent deletion must not mutate the successor"
    );
    let threshold_versions = signed_list_object_versions(&threshold, &[])
        .await
        .text()
        .await
        .expect("read newer-threshold version list");
    assert!(threshold_versions.contains(&middle_version));
    assert!(threshold_versions.contains(&successor_version));
    assert!(!threshold_versions.contains(&oldest_version));
    threshold.assert_no_pin_removal().await;
    threshold.shutdown().await;
}

#[tokio::test]
async fn lifecycle_expiration_invariants() {
    let mut deleted_config = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleDeletedConfig",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &deleted_config,
            "delete-config",
            &[],
            b"body".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &deleted_config,
        lifecycle_current_expiration_xml(
            "delete-config",
            "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
        ),
    )
    .await;
    assert_eq!(
        signed_delete_bucket_lifecycle_configuration(&deleted_config)
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        deleted_config.execute_claim(&claim).await.state,
        "cancelled"
    );
    assert_eq!(deleted_config.version_rows("delete-config").await.len(), 1);
    deleted_config.assert_no_pin_removal().await;
    deleted_config.shutdown().await;

    let mut disabled_rule = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleDisabledRule",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put(
            &disabled_rule,
            "disabled-rule",
            &[],
            b"body".to_vec(),
            HeaderMap::new(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &disabled_rule,
        lifecycle_current_expiration_xml(
            "disable-after-claim",
            "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
        ),
    )
    .await;
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &disabled_rule,
            "<LifecycleConfiguration><Rule><ID>disable-after-claim</ID><Status>Disabled</Status>\
             <Filter/><Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>\
             </Rule></LifecycleConfiguration>"
                .to_owned(),
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(disabled_rule.execute_claim(&claim).await.state, "cancelled");
    assert_eq!(disabled_rule.version_rows("disabled-rule").await.len(), 1);
    disabled_rule.assert_no_pin_removal().await;
    disabled_rule.shutdown().await;

    let mut changed_tags = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleChangedTags",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &changed_tags,
            "changed-tags",
            b"body".to_vec(),
            "class=keep"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &changed_tags,
        "<LifecycleConfiguration><Rule><ID>recheck-tags</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>class</Key><Value>keep</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(
        signed_put_object_tagging(&changed_tags, "changed-tags", &[("class", "changed")])
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(changed_tags.execute_claim(&claim).await.state, "cancelled");
    assert_eq!(changed_tags.version_rows("changed-tags").await.len(), 1);
    changed_tags.assert_no_pin_removal().await;
    changed_tags.shutdown().await;

    let mut replaced_current = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleReplaceCurrent",
        2,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &replaced_current,
            "replace-current",
            b"old".to_vec(),
            "generation=old",
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &replaced_current,
        "<LifecycleConfiguration><Rule><ID>replace-current</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>generation</Key><Value>old</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &replaced_current,
            "replace-current",
            b"replacement".to_vec(),
            "generation=new",
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        replaced_current.execute_claim(&claim).await.state,
        "cancelled"
    );
    assert_eq!(
        replaced_current.version_rows("replace-current").await.len(),
        1
    );
    replaced_current.assert_no_pin_removal().await;
    replaced_current.shutdown().await;

    for (key, mutation) in [
        ("promoted-target", "promote"),
        ("exact-target", "exact-delete"),
    ] {
        let mut noncurrent = start_lifecycle_harness(KuboScript::repeated_add(
            "QmLifecycleNoncurrentInvariant",
            2,
            HashMap::new(),
        ))
        .await;
        assert_eq!(
            signed_put_bucket_versioning(&noncurrent, "Enabled")
                .await
                .status(),
            StatusCode::OK
        );
        let old = signed_put(&noncurrent, key, &[], b"old".to_vec(), HeaderMap::new()).await;
        assert_eq!(old.status(), StatusCode::OK);
        let old_version = old.headers()["x-amz-version-id"]
            .to_str()
            .expect("old invariant version")
            .to_owned();
        let current =
            signed_put(&noncurrent, key, &[], b"current".to_vec(), HeaderMap::new()).await;
        assert_eq!(current.status(), StatusCode::OK);
        let current_version = current.headers()["x-amz-version-id"]
            .to_str()
            .expect("current invariant version")
            .to_owned();
        let old_row = noncurrent.version_rows(key).await.remove(0);
        let now = ipfs_s3_gateway::store::database_clock::database_now(noncurrent.state.store.db())
            .await
            .expect("read invariant noncurrent database clock");
        noncurrent
            .set_database_times(
                &old_row.id,
                old_row.lifecycle_age_started_at,
                Some(now - ChronoDuration::days(3)),
            )
            .await;
        let claim = schedule_lifecycle_action(
            &noncurrent,
            lifecycle_noncurrent_expiration_xml("noncurrent-invariant", None),
        )
        .await;
        assert_eq!(
            claim.action.target_public_version_id.as_deref(),
            Some(old_version.as_str())
        );
        if mutation == "promote" {
            assert_eq!(
                signed_delete_object_version(&noncurrent, key, Some(&current_version))
                    .await
                    .status(),
                StatusCode::NO_CONTENT
            );
            assert!(
                noncurrent
                    .version_rows(key)
                    .await
                    .iter()
                    .any(
                        |row| row.version_id.as_deref() == Some(old_version.as_str())
                            && row.is_latest
                    )
            );
        } else {
            assert_eq!(
                signed_delete_object_version(&noncurrent, key, Some(&old_version))
                    .await
                    .status(),
                StatusCode::NO_CONTENT
            );
            assert!(
                noncurrent
                    .version_rows(key)
                    .await
                    .iter()
                    .any(
                        |row| row.version_id.as_deref() == Some(current_version.as_str())
                            && row.is_latest
                    )
            );
        }
        assert_eq!(noncurrent.execute_claim(&claim).await.state, "cancelled");
        noncurrent.assert_no_pin_removal().await;
        noncurrent.shutdown().await;
    }

    let mut publication_delete_race = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecyclePublicationDeleteRace",
        2,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &publication_delete_race,
            "publication-delete-race",
            b"old".to_vec(),
            "generation=old",
        )
        .await
        .status(),
        StatusCode::OK
    );
    let claim = schedule_lifecycle_action(
        &publication_delete_race,
        "<LifecycleConfiguration><Rule><ID>publication-delete-race</ID><Status>Enabled</Status>\
         <Filter><Tag><Key>generation</Key><Value>old</Value></Tag></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    let endpoint = OwnedTestEndpoint {
        endpoint: publication_delete_race.endpoint.clone(),
        bucket: publication_delete_race.bucket.clone(),
    };
    let (published, deleted) = tokio::join!(
        signed_put_with_tagging(
            &endpoint,
            "publication-delete-race",
            b"replacement".to_vec(),
            "generation=new",
        ),
        signed_delete_object(&endpoint, "publication-delete-race"),
    );
    assert_eq!(published.status(), StatusCode::OK);
    assert!(
        matches!(
            deleted.status(),
            StatusCode::NO_CONTENT | StatusCode::CONFLICT
        ),
        "the concurrent delete must either serialize or report a mutation conflict"
    );
    if deleted.status() == StatusCode::CONFLICT {
        assert_eq!(
            signed_delete_object(&endpoint, "publication-delete-race")
                .await
                .status(),
            StatusCode::NO_CONTENT,
            "a serialized retry after the publication/delete race must complete"
        );
    }
    assert_eq!(
        publication_delete_race.execute_claim(&claim).await.state,
        "cancelled"
    );
    publication_delete_race.assert_no_pin_removal().await;
    publication_delete_race.shutdown().await;

    let mut shared_cid = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleSharedCid",
        2,
        HashMap::from([("QmLifecycleSharedCid".to_owned(), b"shared".to_vec())]),
    ))
    .await;
    for key in ["shared-a", "shared-b"] {
        assert_eq!(
            signed_put(&shared_cid, key, &[], b"shared".to_vec(), HeaderMap::new())
                .await
                .status(),
            StatusCode::OK
        );
    }
    let claim = schedule_lifecycle_action(
        &shared_cid,
        "<LifecycleConfiguration><Rule><ID>shared-cid</ID><Status>Enabled</Status>\
         <Filter><Prefix>shared-a</Prefix></Filter>\
         <Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration></Rule>\
         </LifecycleConfiguration>"
            .to_owned(),
    )
    .await;
    assert_eq!(shared_cid.execute_claim(&claim).await.state, "succeeded");
    assert_eq!(shared_cid.version_rows("shared-a").await.len(), 0);
    assert_signed_body(&shared_cid, "shared-b", b"shared").await;
    shared_cid.assert_no_pin_removal().await;
    shared_cid.shutdown().await;

    for (key, headers, expect_sse_s3) in [
        ("plain", HeaderMap::new(), false),
        (
            "sse-s3",
            HeaderMap::from_iter([(
                http::HeaderName::from_static("x-amz-server-side-encryption"),
                HeaderValue::from_static("AES256"),
            )]),
            true,
        ),
        ("sse-c", sse_c_headers(), false),
    ] {
        let mut encrypted = start_lifecycle_harness(KuboScript::repeated_add(
            "QmLifecycleEncrypted",
            1,
            HashMap::new(),
        ))
        .await;
        assert_eq!(
            signed_put(&encrypted, key, &[], b"protected".to_vec(), headers)
                .await
                .status(),
            StatusCode::OK
        );
        let before = store::object::get_latest(encrypted.state.store.db(), &encrypted.bucket, key)
            .await
            .expect("load lifecycle encryption target");
        assert_eq!(before.encrypted, key != "plain");
        assert_eq!(before.key_wrap.is_some(), expect_sse_s3);
        assert_eq!(before.sse_c_key_fingerprint.is_some(), key == "sse-c");
        let claim = schedule_lifecycle_action(
            &encrypted,
            lifecycle_current_expiration_xml(
                "encrypted-expire",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await;
        assert_eq!(encrypted.execute_claim(&claim).await.state, "succeeded");
        assert!(encrypted.version_rows(key).await.is_empty());
        encrypted.assert_no_pin_removal().await;
        encrypted.shutdown().await;
    }

    let mut tags_leases_retry = start_lifecycle_harness(KuboScript::repeated_add(
        "QmLifecycleLeaseRetry",
        1,
        HashMap::new(),
    ))
    .await;
    assert_eq!(
        signed_put_with_tagging(
            &tags_leases_retry,
            "tags-leases",
            b"body".to_vec(),
            "class=old"
        )
        .await
        .status(),
        StatusCode::OK
    );
    let object = store::object::get_latest(
        tags_leases_retry.state.store.db(),
        &tags_leases_retry.bucket,
        "tags-leases",
    )
    .await
    .expect("load tags and leases lifecycle object");
    let now =
        ipfs_s3_gateway::store::database_clock::database_now(tags_leases_retry.state.store.db())
            .await
            .expect("read tags and leases database clock");
    let lease_id = uuid::Uuid::new_v4().to_string();
    store::entities::pin_lease::Entity::insert(store::entities::pin_lease::ActiveModel {
        id: Set(lease_id.clone()),
        owner_object_id: Set(object.id.clone()),
        source: Set("lifecycle-acceptance".to_owned()),
        policy_id: Set("lifecycle-acceptance".to_owned()),
        provider_mode: Set("one".to_owned()),
        content_mode: Set("object".to_owned()),
        created_at: Set(now),
        last_touched_at: Set(now),
        expires_at: Set(now + ChronoDuration::days(1)),
        generation: Set(1),
        state: Set("active".to_owned()),
    })
    .exec(tags_leases_retry.state.store.db())
    .await
    .expect("seed active lifecycle lease");
    assert_eq!(
        signed_put_bucket_lifecycle_configuration_xml(
            &tags_leases_retry,
            lifecycle_current_expiration_xml(
                "tags-leases-retry",
                "<Expiration><Date>2020-01-01T00:00:00Z</Date></Expiration>",
            ),
        )
        .await
        .status(),
        StatusCode::OK
    );
    tags_leases_retry.run_one_scan_page().await;
    let claim = tags_leases_retry.stop_after_claim().await;
    let terminal = tags_leases_retry.execute_claim(&claim).await;
    assert_eq!(terminal.state, "succeeded");
    assert_eq!(
        terminal.attempts, 2,
        "stopped claimed work must be reclaimed once"
    );
    assert_eq!(
        store::entities::object_tag::Entity::find()
            .filter(store::entities::object_tag::Column::ObjectId.eq(&object.id))
            .count(tags_leases_retry.state.store.db())
            .await
            .expect("count cleared lifecycle tags"),
        0
    );
    assert_eq!(
        store::entities::pin_lease::Entity::find_by_id(lease_id)
            .one(tags_leases_retry.state.store.db())
            .await
            .expect("load ended lifecycle lease")
            .expect("seeded lifecycle lease exists")
            .state,
        "cancelled"
    );
    tags_leases_retry.assert_no_pin_removal().await;
    tags_leases_retry.shutdown().await;
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
    let job = store::entities::import_job::Entity::find_by_id(&job_id)
        .one(harness.state.store.db())
        .await
        .expect("query epoch-one import")
        .expect("epoch-one import exists");
    let mut active = job.into_active_model();
    active.locked_until = Set(Some(Utc::now() - ChronoDuration::seconds(1)));
    active.next_attempt_at = Set(Utc::now() - ChronoDuration::seconds(1));
    active.updated_at = Set(Utc::now());
    active
        .update(harness.state.store.db())
        .await
        .expect("expire epoch-one worker lease");

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
