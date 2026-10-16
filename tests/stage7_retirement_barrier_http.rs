#[allow(dead_code)]
mod support {
    pub mod cors;
    #[path = "stage7_management.rs"]
    pub mod foundation;
    pub mod sigv4;
    pub mod stage7_retirement_barrier;
    pub mod stage7_retirement_barrier_http;
}
use ipfs_s3_gateway::{
    state::AppState,
    store::{
        entities::{object, pin_provider_route},
        pinning::{jobs, ledger, management::native},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};
use support::stage7_retirement_barrier_http::{Fixture, xml_value};

#[tokio::test]
async fn signed_put_copy_tag_renew_and_normal_restart_cannot_reopen_retired_domain() {
    let fixture = Fixture::new().await;
    let put = fixture
        .request(
            reqwest::Method::PUT,
            "existing",
            &[],
            b"0123456789".to_vec(),
            Fixture::pin_headers(),
        )
        .await;
    assert_eq!(put.status(), reqwest::StatusCode::OK);
    fixture.retire().await;
    let before = native::capture(fixture.state.store.db()).await.unwrap();
    let put = fixture
        .request(
            reqwest::Method::PUT,
            "new",
            &[],
            b"0123456789".to_vec(),
            Fixture::pin_headers(),
        )
        .await;
    assert!(!put.status().is_success());
    let mut copy = http::HeaderMap::new();
    copy.insert("x-amz-copy-source", "/bucket/existing".parse().unwrap());
    let response = fixture
        .request(reqwest::Method::PUT, "copied", &[], vec![], copy)
        .await;
    assert!(!response.status().is_success());
    let renewal = b"<Tagging><TagSet><Tag><Key>ipfs-s3:pin</Key><Value>true</Value></Tag><Tag><Key>ipfs-s3:duration</Key><Value>2h</Value></Tag></TagSet></Tagging>".to_vec();
    let response = fixture
        .request(
            reqwest::Method::PUT,
            "existing",
            &[("tagging", "")],
            renewal,
            http::HeaderMap::new(),
        )
        .await;
    assert!(!response.status().is_success());
    let mut alias = fixture.raw.clone();
    alias.pinning_identity.providers[0].provider_id = "after-restart-alias".into();
    let restarted = AppState::new(&alias).await.unwrap();
    let route = pin_provider_route::Entity::find_by_id(
        fixture.config.pinning.providers[0]
            .identity
            .allocation_key(),
    )
    .one(restarted.store.db())
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        serde_json::from_str::<ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot>(
            &route.snapshot
        )
        .unwrap(),
        fixture.config.pinning.providers[0].route_snapshot()
    );
    let after = native::capture(fixture.state.store.db()).await.unwrap();
    for table in [
        "objects",
        "object_versions",
        "pin_leases",
        "pin_lease_targets",
        "pin_provider_routes",
        "remote_pins",
        "pin_provider_usage",
        "pin_jobs",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "rejected writers must preserve {table}"
        );
    }
    for key in ["new", "copied"] {
        assert!(
            object::Entity::find()
                .filter(object::Column::Key.eq(key))
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .is_none()
        );
    }
    fixture.assert_no_remote_mutation().await;
}

#[tokio::test]
async fn captured_multipart_complete_and_new_multipart_intake_both_recheck_retirement() {
    let fixture = Fixture::new().await;
    let response = fixture
        .request(
            reqwest::Method::POST,
            "mpu",
            &[("uploads", "")],
            vec![],
            Fixture::pin_headers(),
        )
        .await;
    assert!(response.status().is_success());
    let xml = response.text().await.unwrap();
    let upload = xml_value(&xml, "UploadId").to_owned();
    let response = fixture
        .request(
            reqwest::Method::PUT,
            "mpu",
            &[("partNumber", "1"), ("uploadId", &upload)],
            b"0123456789".to_vec(),
            http::HeaderMap::new(),
        )
        .await;
    assert!(response.status().is_success());
    let etag = response.headers()["etag"].to_str().unwrap().to_owned();
    fixture.retire().await;
    let complete = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>").into_bytes();
    let response = fixture
        .request(
            reqwest::Method::POST,
            "mpu",
            &[("uploadId", &upload)],
            complete,
            http::HeaderMap::new(),
        )
        .await;
    assert!(!response.status().is_success());
    let response = fixture
        .request(
            reqwest::Method::POST,
            "new-mpu",
            &[("uploads", "")],
            vec![],
            Fixture::pin_headers(),
        )
        .await;
    assert!(!response.status().is_success());
    assert!(
        object::Entity::find()
            .filter(object::Column::Key.eq("mpu"))
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .is_none()
    );
    fixture.assert_no_remote_mutation().await;
}

#[tokio::test]
async fn a_durable_started_rpc_invocation_is_not_a_new_dispatch_after_retirement() {
    let fixture = Fixture::new().await;
    let response = fixture
        .request(
            reqwest::Method::PUT,
            "queued",
            &[],
            b"0123456789".to_vec(),
            Fixture::pin_headers(),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let now = ipfs_s3_gateway::store::database_clock::database_now(fixture.state.store.db())
        .await
        .unwrap();
    let claimed = jobs::claim_due_jobs(
        fixture.state.store.db(),
        now,
        chrono::Duration::minutes(1),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap();
    assert_eq!(claimed.model.operation, "submit");
    let tx = fixture.state.store.db().begin().await.unwrap();
    assert_eq!(
        jobs::prepare_submit_call(&tx, &claimed, now).await.unwrap(),
        jobs::SubmitCallDecision::ReadyToCall
    );
    jobs::record_submit_invocation(&tx, &claimed, "kubo", "cid", now)
        .await
        .unwrap();
    let id = ledger::submission::begin(&tx, &claimed, now)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    let footprint = ledger::submission::latest(fixture.state.store.db(), &claimed.model.id)
        .await
        .unwrap()
        .unwrap();
    fixture.retire().await;
    assert!(
        !ledger::submission::dispatch_allowed(fixture.state.store.db(), &id)
            .await
            .unwrap()
    );
    let after = ledger::submission::latest(fixture.state.store.db(), &claimed.model.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        footprint, after,
        "blocking new POST cannot discard or rewrite the started invocation"
    );
    fixture.assert_no_remote_mutation().await;
}

#[tokio::test]
async fn actually_dispatched_unknown_callback_is_retained_at_live_and_late_old_epochs() {
    use ipfs_s3_gateway::{
        pinning::provider::{SubmitEffect, SubmitPin},
        store::pinning::leases,
    };
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    for late in [false, true] {
        let fixture = Fixture::new().await;
        let response = fixture
            .request(
                reqwest::Method::PUT,
                "in-flight",
                &[],
                b"0123456789".to_vec(),
                Fixture::pin_headers(),
            )
            .await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let now = ipfs_s3_gateway::store::database_clock::database_now(fixture.state.store.db())
            .await
            .unwrap();
        let claimed = jobs::claim_due_jobs(
            fixture.state.store.db(),
            now,
            chrono::Duration::minutes(1),
            1,
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        let tx = fixture.state.store.db().begin().await.unwrap();
        jobs::prepare_submit_call(&tx, &claimed, now).await.unwrap();
        jobs::record_submit_invocation(&tx, &claimed, "kubo", "cid", now)
            .await
            .unwrap();
        let id = ledger::submission::begin(&tx, &claimed, now)
            .await
            .unwrap()
            .unwrap();
        tx.commit().await.unwrap();
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let signal = started.clone();
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(move |_: &wiremock::Request| {
                signal.notify_one();
                ResponseTemplate::new(503)
            })
            .mount(&fixture.target)
            .await;
        let provider = fixture
            .state
            .pinning
            .provider(&claimed.model.provider)
            .unwrap();
        let request = SubmitPin {
            cid: claimed.model.cid.clone(),
            name: claimed.model.id.clone(),
            metadata: Default::default(),
        };
        let dispatched = tokio::spawn(async move { provider.submit_observed(request).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), started.notified())
            .await
            .unwrap();
        // The real HTTP operation has started, but its result has not been
        // persisted. Retirement must keep this responsibility, not erase it.
        fixture.retire().await;
        fixture
            .state
            .store
            .db()
            .execute_unprepared("UPDATE pin_provider_routes SET retired=TRUE")
            .await
            .unwrap();
        if late {
            let tx = fixture.state.store.db().begin().await.unwrap();
            leases::cancel_lease(&tx, claimed.model.lease_id.as_deref().unwrap(), now)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }
        let before = native::capture(fixture.state.store.db()).await.unwrap();
        let observation = dispatched.await.unwrap();
        assert_eq!(observation.effect, SubmitEffect::Unknown);
        let outcome =
            ledger::submission::record(fixture.state.store.db(), &claimed, &id, &observation, now)
                .await
                .unwrap();
        assert_eq!(outcome, !late);
        let saved = ledger::submission::latest(fixture.state.store.db(), &claimed.model.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.outcome, "unknown");
        assert!(saved.needs_attention && saved.observed_at.is_some());
        let after = native::capture(fixture.state.store.db()).await.unwrap();
        for table in [
            "remote_pins",
            "remote_pin_ledger",
            "pin_provider_usage",
            "pin_management_retirements",
        ] {
            assert!(
                before.table(table) == after.table(table),
                "Unknown fact append must not fabricate ownership/release or rewrite {table}"
            );
        }
        let requests = fixture.target.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/pin/add")
                .count(),
            1
        );
        assert!(!requests.iter().any(|request| request.method == "DELETE" || request.url.path() == "/api/v0/pin/rm"));
    }
}

#[tokio::test]
async fn signed_source_false_zip_all_three_intakes_share_the_permanent_domain_barrier() {
    let fixture = Fixture::with_trigger("always").await;
    fixture.retire().await;
    let before = native::capture(fixture.state.store.db()).await.unwrap();
    for entry in ["direct", "mpu", "import"] {
        let token = format!("retired-{entry}");
        let mut headers = http::HeaderMap::new();
        for (name, value) in [
            ("x-ipfs3-zip-contract", "v2"),
            ("x-ipfs3-zip-publish-source", "false"),
            ("x-ipfs3-zip-publish-extracted", "true"),
            ("x-ipfs3-zip-targets", "extracted"),
            ("x-ipfs3-zip-token", token.as_str()),
        ] {
            headers.insert(name, value.parse().unwrap());
        }
        let (method, query, body) = match entry {
            "direct" => (
                reqwest::Method::PUT,
                vec![("decompress-zip", "out/")],
                b"not-consumed-before-admission".to_vec(),
            ),
            "mpu" => (
                reqwest::Method::POST,
                vec![("uploads", ""), ("decompress-zip", "out/")],
                vec![],
            ),
            "import" => {
                headers.insert("content-type", "application/xml".parse().unwrap());
                headers.insert("x-ipfs3-client-token", token.parse().unwrap());
                (
                    reqwest::Method::POST,
                    vec![("ipfs3-import", ""), ("decompress-zip", "out/")],
                    format!(
                        "<IPFS3ImportRequest><CID>{}</CID></IPFS3ImportRequest>",
                        support::stage7_retirement_barrier::foundation::CID
                    )
                    .into_bytes(),
                )
            }
            _ => unreachable!(),
        };
        let response = fixture
            .request(method, "archive.zip", &query, body, headers)
            .await;
        assert!(!response.status().is_success());
        assert!(
            response.text().await.unwrap().contains("retired"),
            "not merely malformed ZIP or another unrelated rejection"
        );
    }
    let after = native::capture(fixture.state.store.db()).await.unwrap();
    for table in [
        "zip_v2_executions",
        "zip_v2_mpu_intakes",
        "zip_v2_import_requests",
        "zip_batches",
        "multipart_uploads",
        "import_jobs",
        "objects",
        "object_versions",
        "pin_provider_usage",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "new {table} intake must roll back"
        );
    }
    fixture.assert_no_remote_mutation().await;
}

#[tokio::test]
async fn regular_import_intake_rolls_back_owned_source_and_destination_claims_on_retirement() {
    use ipfs_s3_gateway::{
        import::ImportSource,
        pinning::{decision::DecisionOrigin, policy::PublicationContext, tags::ObjectTag},
        store::import::{jobs::NewImportJob, ownership},
    };
    let fixture = Fixture::new().await;
    fixture.retire().await;
    let before = native::capture(fixture.state.store.db()).await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let tags = vec![
        ObjectTag {
            key: "ipfs-s3:pin".into(),
            value: "true".into(),
        },
        ObjectTag {
            key: "ipfs-s3:duration".into(),
            value: "1h".into(),
        },
    ];
    let (_, mut decision) = fixture
        .state
        .pinning
        .policy()
        .evaluate_publication_decision(
            PublicationContext {
                bucket: "bucket",
                key: "imported",
                tags: &tags,
                is_decompress_zip: false,
            },
            DecisionOrigin::new("test", &id),
        )
        .unwrap();
    decision
        .capture_durable_revision(
            fixture.state.pinning.effective_config(),
            fixture.state.pinning.control_mode(),
        )
        .unwrap();
    let request = NewImportJob {
        id,
        bucket: "bucket".into(),
        key: "imported".into(),
        source: ImportSource::Cid(support::stage7_retirement_barrier::foundation::CID.into()),
        request_fingerprint: "17".repeat(32),
        client_token: Some("retired-regular-import".into()),
        object_content_type: None,
        metadata: Default::default(),
        tags,
        decompress_prefix: None,
    };
    assert!(
        ownership::submit_decided(
            fixture.state.store.db(),
            request,
            decision,
            chrono::Utc::now()
        )
        .await
        .is_err()
    );
    assert!(before == native::capture(fixture.state.store.db()).await.unwrap());
    fixture.assert_no_remote_mutation().await;
}
