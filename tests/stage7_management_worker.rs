#[path = "support/stage7_management_worker.rs"]
pub mod support;

use ipfs_s3_gateway::{
    pinning::{coordinator::PinningCoordinator, worker::ManagementJobScope},
    store::{
        Store,
        entities::{pin_job, remote_pin},
        pinning::management::{actions, records},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn adapter_submit_history_authorizes_only_its_exact_poll_successor() {
    for backend in ["pinata", "filebase"] {
        let fixture = support::psa_fixture_for(backend).await;
        let identity = fixture.publish("adapter-successor", false).await;
        let binding = fixture
            .confirm(&fixture.create_request("adapter-successor", identity, false))
            .await;
        let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
        let seed = applied.items[0]
            .jobs
            .iter()
            .find(|job| job.directive == records::JobDirective::Submit)
            .unwrap();
        let response = support::response("exact-request", "queued");
        Mock::given(method("POST"))
            .and(path("/psa/pins"))
            .respond_with(ResponseTemplate::new(202).set_body_json(response))
            .mount(&fixture.server)
            .await;
        Mock::given(method("GET"))
            .and(path("/psa/pins/exact-request"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(support::response("exact-request", "pinned")),
            )
            .mount(&fixture.server)
            .await;
        let coordinator = support::coordinator(&fixture);
        let store = Store::new(fixture.db.clone());
        let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap();
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
            .unwrap();
        let requests = fixture.server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "POST"
                    && request.url.path() == "/psa/pins")
                .count(),
            1
        );
        let history =
            ipfs_s3_gateway::store::pinning::jobs::submission_history(&fixture.db, &seed.job_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(history.api, "psa");
        let poll = pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        fixture
            .db
            .execute_unprepared(
                "UPDATE pin_jobs SET next_attempt_at='2000-01-01T00:00:00Z' WHERE operation='poll'",
            )
            .await
            .unwrap();
        let report = coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
            .unwrap();
        let requests = fixture.server.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "GET"
                    && request.url.path() == "/psa/pins/exact-request")
                .count(),
            1
        );
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.status, "pinned");
        assert!(
            report
                .replica_observations
                .iter()
                .any(|fact| fact.job_id == poll.id)
        );
    }
}

#[tokio::test]
async fn source_metadata_drift_prevents_a_new_post() {
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("source-drift", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("source-drift", identity, false))
        .await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE objects SET size=size+1")
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("must-not-create", "queued")),
        )
        .mount(&fixture.server)
        .await;
    let coordinator = support::coordinator(&fixture);
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    coordinator
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        0
    );
}

#[tokio::test]
async fn fresh_current_node_endpoint_policy_and_exact_version_fence_new_posts() {
    use ipfs_s3_gateway::{kubo::KuboClient, pinning::management::local_config};
    use sea_orm::{ActiveModelTrait, IntoActiveModel, Set, Statement};
    for drift in [
        "node",
        "endpoint",
        "idle",
        "policy",
        "private_version",
        "credential_revision",
        "unknown_profile",
        "missing_source",
    ] {
        let fixture = support::psa_fixture().await;
        let identity = fixture.publish(drift, false).await;
        let binding = fixture
            .confirm(&fixture.create_request(drift, identity.clone(), false))
            .await;
        let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
        let seed = &applied.items[0].jobs[0];
        let mut config = fixture.config.pinning.clone();
        let mut source = Some(KuboClient::new(fixture.server.uri()));
        let other = wiremock::MockServer::start().await;
        match drift {
            "node" => {
                Mock::given(method("POST"))
                    .and(path("/api/v0/id"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(serde_json::json!({"ID":"ControlledReplacementNode"})),
                    )
                    .with_priority(1)
                    .mount(&fixture.server)
                    .await
            }
            "endpoint" => {
                Mock::given(method("POST"))
                    .and(path("/api/v0/id"))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(serde_json::json!({"ID":support::NODE})),
                    )
                    .mount(&other)
                    .await;
                source = Some(KuboClient::new(other.uri()));
            }
            "idle" => {
                source = Some(KuboClient::new_with_timeouts(
                    fixture.server.uri(),
                    std::time::Duration::from_secs(5),
                    std::time::Duration::from_millis(17),
                ))
            }
            "policy" => {
                let mut raw = fixture.raw.clone();
                raw.pinning.policies[0].prefix = "different-prefix/".into();
                config = local_config::compile(&raw, |_| Some("controlled-worker-token".into()))
                    .unwrap()
                    .pinning;
            }
            "private_version" => {
                fixture
                    .db
                    .execute(Statement::from_sql_and_values(
                        fixture.db.get_database_backend(),
                        "UPDATE object_versions SET key=$1 WHERE id=$2",
                        [
                            format!("{}-rebound", identity.key).into(),
                            identity.private_version_id.into(),
                        ],
                    ))
                    .await
                    .unwrap();
            }
            "credential_revision" => {
                config.providers[0].identity.credential_revision += 1;
            }
            "unknown_profile" => {
                let invocation =
                    ipfs_s3_gateway::store::entities::pin_invocation_route::Entity::find_by_id(
                        seed.job_id.clone(),
                    )
                    .one(&fixture.db)
                    .await
                    .unwrap()
                    .unwrap();
                let mut route = seed.route.clone();
                route.api_profile = "unrecognized-provider-protocol".into();
                let mut active = invocation.into_active_model();
                active.route = Set(serde_json::to_string(&route).unwrap());
                active.update(&fixture.db).await.unwrap();
            }
            "missing_source" => source = None,
            _ => unreachable!(),
        }
        Mock::given(method("POST"))
            .and(path("/psa/pins"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(support::response("must-not-create", "queued")),
            )
            .mount(&fixture.server)
            .await;
        let coordinator = PinningCoordinator::build_with_kubo(config, source).unwrap();
        let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap();
        let _result = coordinator
            .management_tick(
                &Store::new(fixture.db.clone()),
                &scope,
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(
            fixture
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.url.path() == "/psa/pins")
                .count(),
            0,
            "{drift} admitted provider IO"
        );
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(
            remote.request_id.is_none(),
            "{drift} projected a new resource"
        );
    }
}

#[tokio::test]
async fn real_source_attestation_expiry_is_not_extended_by_confirmation_or_lease() {
    use ipfs_s3_gateway::{kubo::KuboClient, pinning::management::evidence};
    use sea_orm::Statement;
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("proof-expiry", false).await;
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "DELETE FROM pin_management_records WHERE kind='evidence_preparation' AND item_id=$1",
            [identity.private_version_id.clone().into()],
        ))
        .await
        .unwrap();
    let proof = evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &KuboClient::new(fixture.server.uri()),
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::seconds(3),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let binding = fixture
        .confirm(&fixture.create_request("proof-expiry", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let wait = (proof.expires_at - chrono::Utc::now())
        .to_std()
        .unwrap_or_default()
        + std::time::Duration::from_millis(50);
    tokio::time::sleep(wait).await;
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("must-not-create", "queued")),
        )
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    support::coordinator(&fixture)
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        0
    );
    let lease = ipfs_s3_gateway::store::entities::pin_lease::Entity::find_by_id(
        applied.items[0].receipt.intent_ids[0].clone(),
    )
    .one(&fixture.db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(lease.state, "active");
    assert!(lease.expires_at > chrono::Utc::now());
}

#[tokio::test]
async fn actual_finish_database_trigger_cannot_rollback_readonly_get_fact() {
    let (fixture, binding, seed) = support::readonly_fixture("finish-db-trigger").await;
    let request = seed.resource_id.as_deref().unwrap();
    let resource_path = format!("/psa/pins/{request}");
    Mock::given(method("GET"))
        .and(path(&resource_path))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(support::response(request, "queued")),
        )
        .mount(&fixture.server)
        .await;
    fixture.db.execute_unprepared("CREATE TRIGGER worker_finish_abort BEFORE UPDATE ON pin_jobs WHEN OLD.operation='reconcile' AND OLD.locked_until IS NOT NULL AND NEW.locked_until IS NULL BEGIN SELECT RAISE(ABORT,'controlled finish database fault'); END").await.unwrap();
    let before = ipfs_s3_gateway::store::pinning::management::native::capture(&fixture.db)
        .await
        .unwrap();
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let result = support::coordinator(&fixture)
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await;
    assert!(matches!(
        result,
        Err(ipfs_s3_gateway::error::AppError::Database(_))
    ));
    let facts = ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let fact = facts
        .iter()
        .find(|fact| fact.job_id == seed.job_id)
        .expect("actual response must commit before finish UPDATE");
    assert!(!fact.projectable);
    assert_eq!(fact.phase, "reconcile");
    assert_eq!(fact.outcome, "ReportedQueued");
    let after = ipfs_s3_gateway::store::pinning::management::native::capture(&fixture.db)
        .await
        .unwrap();
    for table in [
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "readonly changed {table}"
        );
    }
    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.url.path() == resource_path)
            .count(),
        1
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.url.path().starts_with("/psa/pins"))
            .all(|request| request.method.as_str() == "GET")
    );
}

#[tokio::test]
async fn late_readonly_get_uses_dispatched_route_epoch_and_claim_not_replacement_invocation() {
    use sea_orm::Statement;
    let (fixture, binding, seed) = support::readonly_fixture("old-invocation").await;
    let request = seed.resource_id.as_deref().unwrap();
    let resource_path = format!("/psa/pins/{request}");
    Mock::given(method("GET"))
        .and(path(&resource_path))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(support::response(request, "queued"))
                .set_delay(std::time::Duration::from_millis(500)),
        )
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let store = Store::new(fixture.db.clone());
    let coordinator = support::coordinator(&fixture);
    let task = tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
    });
    support::wait_for_request(&fixture.server, "GET", &resource_path).await;
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let invocation = ipfs_s3_gateway::store::entities::pin_invocation_route::Entity::find_by_id(
        seed.job_id.clone(),
    )
    .one(&fixture.db)
    .await
    .unwrap()
    .unwrap();
    let mut replacement = seed.route.clone();
    replacement.endpoint_revision += 1;
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_invocation_routes SET remote_epoch=remote_epoch+100,route=$1 WHERE job_id=$2",[serde_json::to_string(&replacement).unwrap().into(),seed.job_id.clone().into()])).await.unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET locked_until=$1 WHERE id=$2",
            [
                (job.locked_until.unwrap() + chrono::Duration::minutes(1)).into(),
                seed.job_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let _result = task.await.unwrap();
    let fact = ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find()
        .filter(
            ipfs_s3_gateway::store::entities::pin_replica_observation::Column::JobId
                .eq(&seed.job_id),
        )
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.route, invocation.route);
    assert_eq!(fact.remote_epoch, invocation.remote_epoch);
    assert_eq!(fact.claim_until, job.locked_until.unwrap());
    assert!(!fact.projectable);
    let resources: Vec<ipfs_s3_gateway::pinning::provider::ObservedResource> =
        serde_json::from_str(&fact.resources).unwrap();
    assert_eq!(resources[0].request_id, request);
    assert_eq!(
        resources[0].status,
        ipfs_s3_gateway::pinning::provider::ObservedResourceStatus::Reported
    );
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == resource_path)
            .count(),
        1
    );
}

#[tokio::test]
async fn dispatched_submit_keeps_fact_but_cannot_project_after_source_drift_or_cancellation() {
    use sea_orm::Statement;
    for drift in ["metadata", "cancel", "node_unavailable"] {
        let fixture = support::psa_fixture().await;
        let identity = fixture.publish(drift, false).await;
        let binding = fixture
            .confirm(&fixture.create_request(drift, identity, false))
            .await;
        let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
        let seed = &applied.items[0].jobs[0];
        Mock::given(method("POST"))
            .and(path("/psa/pins"))
            .respond_with(
                ResponseTemplate::new(202)
                    .set_body_json(support::response("late-submit", "queued"))
                    .set_delay(std::time::Duration::from_millis(500)),
            )
            .mount(&fixture.server)
            .await;
        let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap();
        let coordinator = support::coordinator(&fixture);
        let store = Store::new(fixture.db.clone());
        let task = tokio::spawn(async move {
            coordinator
                .management_tick(&store, &scope, &CancellationToken::new())
                .await
        });
        support::wait_for_request(&fixture.server, "POST", "/psa/pins").await;
        match drift {
            "metadata" => {
                fixture
                    .db
                    .execute_unprepared("UPDATE objects SET size=size+1")
                    .await
                    .unwrap();
            }
            "cancel" => {
                fixture
                    .db
                    .execute(Statement::from_sql_and_values(
                        fixture.db.get_database_backend(),
                        "UPDATE pin_leases SET state='cancelled' WHERE id=$1",
                        [seed.lease_id.clone().unwrap().into()],
                    ))
                    .await
                    .unwrap();
            }
            "node_unavailable" => {
                Mock::given(method("POST"))
                    .and(path("/api/v0/id"))
                    .respond_with(ResponseTemplate::new(503))
                    .with_priority(1)
                    .mount(&fixture.server)
                    .await
            }
            _ => unreachable!(),
        }
        let _result = task.await.unwrap();
        let fact = ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find()
            .filter(
                ipfs_s3_gateway::store::entities::pin_replica_observation::Column::JobId
                    .eq(&seed.job_id),
            )
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fact.phase, "submit");
        assert_eq!(fact.outcome, "ReportedQueued");
        assert!(!fact.projectable);
        let resources: Vec<ipfs_s3_gateway::pinning::provider::ObservedResource> =
            serde_json::from_str(&fact.resources).unwrap();
        assert_eq!(resources[0].request_id, "late-submit");
        let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(remote.request_id.is_none());
        assert_eq!(
            fixture
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .filter(|request| request.url.path() == "/psa/pins")
                .count(),
            1
        );
        assert_eq!(
            pin_job::Entity::find()
                .filter(pin_job::Column::Operation.eq("poll"))
                .all(&fixture.db)
                .await
                .unwrap()
                .len(),
            0
        );
    }
}

#[tokio::test]
async fn dispatched_rejection_keeps_real_not_created_effect_and_typed_error_after_permit_loss() {
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("late-rejection", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("late-rejection", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let seed = &applied.items[0].jobs[0];
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(400).set_delay(std::time::Duration::from_millis(500)))
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let coordinator = support::coordinator(&fixture);
    let store = Store::new(fixture.db.clone());
    let task = tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
    });
    support::wait_for_request(&fixture.server, "POST", "/psa/pins").await;
    fixture
        .db
        .execute_unprepared("UPDATE objects SET size=size+1")
        .await
        .unwrap();
    fixture.db.execute_unprepared("CREATE TRIGGER worker_rejection_finish_abort BEFORE UPDATE ON pin_jobs WHEN OLD.operation='submit' AND OLD.locked_until IS NOT NULL AND NEW.locked_until IS NULL BEGIN SELECT RAISE(ABORT,'controlled finish database fault'); END").await.unwrap();
    assert!(matches!(
        task.await.unwrap(),
        Err(ipfs_s3_gateway::error::AppError::Database(_))
    ));
    let fact = ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find()
        .filter(
            ipfs_s3_gateway::store::entities::pin_replica_observation::Column::JobId
                .eq(&seed.job_id),
        )
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.outcome, "Rejected");
    assert!(!fact.projectable);
    let error: serde_json::Value =
        serde_json::from_str(fact.safe_error.as_deref().unwrap()).unwrap();
    assert_eq!(error["effect"], "not_created");
    assert_eq!(error["category"], "InvalidInput");
    assert_eq!(error["http_status"], 400);
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        1
    );
}

#[tokio::test]
async fn v3_adapter_submit_and_request_bound_poll_keep_the_adapter_history_protocol() {
    use std::sync::{Arc, Mutex};
    let fixture = support::v3_fixture().await;
    let identity = fixture.publish("v3-successor", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("v3-successor", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let seed = &applied.items[0].jobs[0];
    let captured = Arc::new(Mutex::new(None::<serde_json::Value>));
    let post = captured.clone();
    Mock::given(method("POST")).and(path("/v3/files/public/pin_by_cid")).respond_with(move |request:&wiremock::Request|{
        let body:serde_json::Value=serde_json::from_slice(&request.body).unwrap();*post.lock().unwrap()=Some(body.clone());
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"data":{"id":"v3-original-request","cid":body["cid"],"status":"prechecking","keyvalues":body["keyvalues"]}}))
    }).mount(&fixture.server).await;
    Mock::given(method("GET")).and(path("/v3/files/public")).respond_with(move |_:&wiremock::Request|{
        let body=captured.lock().unwrap().clone().unwrap();
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"data":{"files":[{"id":"v3-original-request","cid":body["cid"],"keyvalues":body["keyvalues"]}]}}))
    }).mount(&fixture.server).await;
    let coordinator = support::coordinator(&fixture);
    let store = Store::new(fixture.db.clone());
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    coordinator
        .management_tick(&store, &scope, &CancellationToken::new())
        .await
        .unwrap();
    let history =
        ipfs_s3_gateway::store::pinning::jobs::submission_history(&fixture.db, &seed.job_id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(history.api, "pinata_v3");
    fixture
        .db
        .execute_unprepared(
            "UPDATE pin_jobs SET next_attempt_at='2000-01-01T00:00:00Z' WHERE operation='poll'",
        )
        .await
        .unwrap();
    coordinator
        .management_tick(&store, &scope, &CancellationToken::new())
        .await
        .unwrap();
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "pinned");
    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "POST"
                && request.url.path() == "/v3/files/public/pin_by_cid")
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method.as_str() == "GET"
                && request.url.path() == "/v3/files/public")
            .count(),
        1
    );
}

#[tokio::test]
async fn committed_confirmation_expiry_is_not_a_fresh_source_or_job_deadline() {
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("confirmation-expiry", false).await;
    let request = fixture.create_request("confirmation-expiry", identity, false);
    let binding = support::confirm_with_ttl(&fixture, &request, chrono::Duration::seconds(3)).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("confirmed-but-draft-expired", "queued")),
        )
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    support::coordinator(&fixture)
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        1
    );
}

#[tokio::test]
async fn current_durable_skipped_decision_denies_new_submit_even_with_a_live_source_proof() {
    use ipfs_s3_gateway::pinning::{
        decision::{DecisionEffect, DecisionOrigin},
        management::local_config,
        policy::PublicationContext,
        tags::ObjectTag,
    };
    use sea_orm::TransactionTrait;
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("current-deny", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("current-deny", identity.clone(), false))
        .await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let mut raw = fixture.raw.clone();
    raw.pinning.policies[0].prefix = "another-prefix/".into();
    raw.pinning_control.unavailable = ipfs_s3_gateway::config::OptionalPinControlMode::Warn;
    let denied = local_config::compile(&raw, |_| Some("controlled-worker-token".into())).unwrap();
    let tags = [ObjectTag::new("ipfs-s3:pin", "true")];
    let (_, decision) = denied
        .policy
        .evaluate_publication_decision(
            PublicationContext {
                bucket: &identity.bucket,
                key: &identity.key,
                tags: &tags,
                is_decompress_zip: false,
            },
            DecisionOrigin::new("worker-test", "current-deny"),
        )
        .unwrap();
    assert_eq!(decision.effect, DecisionEffect::Skipped);
    let tx = fixture.db.begin().await.unwrap();
    ipfs_s3_gateway::store::pinning::tags::replace_object_tags(&tx, &identity.object_id, &tags)
        .await
        .unwrap();
    ipfs_s3_gateway::store::pinning::decision::write_for_object_in_transaction(
        &tx,
        &identity.object_id,
        &decision,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("must-not-create", "queued")),
        )
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    support::coordinator(&fixture)
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        0
    );
}

#[tokio::test]
async fn global_worker_also_refuses_managed_submit_without_current_source_context() {
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("global-source-fence", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("global-source-fence", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let seed = &applied.items[0].jobs[0];
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("must-not-create", "queued")),
        )
        .mount(&fixture.server)
        .await;
    let coordinator = PinningCoordinator::build(fixture.config.pinning.clone()).unwrap();
    let worker = coordinator.start(Store::new(fixture.db.clone()), CancellationToken::new());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let job = pin_job::Entity::find_by_id(seed.job_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            if job.last_error.is_some() && job.locked_until.is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    observed.expect("global worker did not isolate the managed job");
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        0
    );
}

#[tokio::test]
async fn attestation_expiring_during_dispatched_post_retains_fact_without_projection() {
    use ipfs_s3_gateway::{kubo::KuboClient, pinning::management::evidence};
    use sea_orm::Statement;
    let fixture = support::psa_fixture().await;
    let identity = fixture.publish("late-proof-expiry", false).await;
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "DELETE FROM pin_management_records WHERE kind='evidence_preparation' AND item_id=$1",
            [identity.private_version_id.clone().into()],
        ))
        .await
        .unwrap();
    let proof = evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &KuboClient::new(fixture.server.uri()),
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::seconds(3),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let binding = fixture
        .confirm(&fixture.create_request("late-proof-expiry", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let seed = &applied.items[0].jobs[0];
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(support::response("late-expired-proof", "queued"))
                .set_delay(std::time::Duration::from_secs(4)),
        )
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let _result = support::coordinator(&fixture)
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await;
    assert!(chrono::Utc::now() >= proof.expires_at);
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path() == "/psa/pins")
            .count(),
        1
    );
    let fact = ipfs_s3_gateway::store::entities::pin_replica_observation::Entity::find()
        .filter(
            ipfs_s3_gateway::store::entities::pin_replica_observation::Column::JobId
                .eq(&seed.job_id),
        )
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(!fact.projectable);
    assert_eq!(fact.outcome, "ReportedQueued");
    assert!(fact.observed_at >= proof.expires_at);
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(remote.request_id.is_none());
    let lease = ipfs_s3_gateway::store::entities::pin_lease::Entity::find_by_id(
        seed.lease_id.clone().unwrap(),
    )
    .one(&fixture.db)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(lease.state, "active");
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .all(&fixture.db)
            .await
            .unwrap()
            .len(),
        0
    );
}
