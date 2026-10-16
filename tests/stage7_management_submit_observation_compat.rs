#[allow(dead_code)]
#[path = "support/stage7_management_worker.rs"]
mod support;

use ipfs_s3_gateway::{
    pinning::{
        identity::{CanonicalResourceKey, Ownership},
        management::{self, db_snapshot},
        provider::{ObservedResource, ObservedResourceStatus, canonical_resource_cid},
        worker::ManagementJobScope,
    },
    store::{
        Store,
        entities::{pin_job, pin_replica_observation, pin_submit_observation},
        pinning::{
            jobs, ledger,
            management::{actions, records},
        },
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, Statement};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

async fn tick(fixture: &support::Fixture, scope: &Arc<ManagementJobScope>) {
    let coordinator = support::coordinator(fixture);
    let store = Store::new(fixture.db.clone());
    let scope = scope.clone();
    tokio::spawn(async move {
        Box::pin(ipfs_s3_gateway::pinning::worker::management_tick(
            &coordinator,
            &store,
            &scope,
            &CancellationToken::new(),
        ))
        .await
        .unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn managed_noop_and_psa_submit_feed_snapshot_and_frozen_resource_consumers() {
    for psa in [true, false] {
        let fixture = if psa {
            Box::pin(support::psa_fixture()).await
        } else {
            Box::pin(support::Fixture::new()).await
        };
        let identity = Box::pin(fixture.publish("submit-envelope", false)).await;
        let request = fixture.create_request("submit-envelope", identity.clone(), false);
        let binding = Box::pin(fixture.confirm(&request)).await;
        let applied = Box::pin(actions::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &binding,
        ))
        .await
        .unwrap();
        let seed = applied.items[0]
            .jobs
            .iter()
            .find(|job| job.directive == records::JobDirective::Submit)
            .unwrap();
        // A publication change after the real provider response denies live
        // projection but must not erase or rebind that dispatch's facts.
        fixture.db.execute_unprepared("CREATE TRIGGER source_drift_after_report AFTER INSERT ON pin_submit_observations WHEN NEW.outcome IN ('ReportedQueued','ReportedPinned') BEGIN UPDATE objects SET size=size+1; END").await.unwrap();
        if psa {
            Mock::given(method("POST"))
                .and(path("/psa/pins"))
                .respond_with(
                    ResponseTemplate::new(202)
                        .set_body_json(support::response("actual-request", "queued")),
                )
                .mount(&fixture.server)
                .await;
        }
        let scope = Arc::new(
            ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
                .await
                .unwrap(),
        );
        Box::pin(tick(&fixture, &scope)).await;
        Box::pin(db_snapshot::collect_with_config(
            &fixture.db,
            &fixture.config,
            &request,
        ))
        .await
        .expect("Submit facts must be valid snapshot evidence")
        .planning_snapshot()
        .unwrap();
        let submit = pin_submit_observation::Entity::find()
            .filter(pin_submit_observation::Column::JobId.eq(&seed.job_id))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let envelopes: Vec<ledger::submission::ResourceEvidence> =
            serde_json::from_str(&submit.resources).unwrap();
        let replica = pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.eq(&seed.job_id))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let resources: Vec<ObservedResource> = serde_json::from_str(&replica.resources).unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(resources.len(), 1);
        let actual = &envelopes[0].resource;
        assert_eq!(actual.cid, resources[0].cid);
        assert_eq!(actual.request_id, resources[0].request_id);
        assert_eq!(actual.status, ObservedResourceStatus::Reported);
        assert_eq!(actual.ownership, Ownership::Unknown);
        assert!(actual.replica_evidence.is_none());
        let key = CanonicalResourceKey {
            backend: seed.route.backend.clone(),
            scope: seed.route.scope.clone(),
            resource_type: actual.resource_type,
            resource_id: canonical_resource_cid(&actual.cid).unwrap(),
        };
        assert_eq!(envelopes[0].key.as_ref(), Some(&key));
        if psa {
            assert_eq!(actual.request_id, "actual-request");
            assert_ne!(key.resource_id, actual.request_id);
        }
        let mut replacement = fixture.config.pinning.providers[0].identity.clone();
        replacement.scope = "replacement-scope".into();
        ledger::register_route(&fixture.db, &seed.provider, &replacement)
            .await
            .unwrap();
        let found = ledger::submission::resource_observations(&fixture.db, &key)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0.id, submit.id);
        assert_eq!(found[0].1.resource.cid, actual.cid);
        let mut wrong_key = key.clone();
        wrong_key.scope = replacement.scope;
        assert!(
            ledger::submission::resource_observations(&fixture.db, &wrong_key)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn definitive_rejection_retries_with_a_real_second_post_and_original_budget() {
    let fixture = Box::pin(support::psa_fixture()).await;
    let identity = Box::pin(fixture.publish("retry-observation", false)).await;
    let binding =
        Box::pin(fixture.confirm(&fixture.create_request("retry-observation", identity, false)))
            .await;
    let applied = Box::pin(actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    ))
    .await
    .unwrap();
    let seed = &applied.items[0].jobs[0];
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(429))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(429))
        .mount(&fixture.server)
        .await;
    let scope = Arc::new(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap(),
    );
    Box::pin(tick(&fixture, &scope)).await;
    let original = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.effect, "not_created");
    assert_eq!(original.submit_calls, 1);
    fixture
        .db
        .execute_unprepared(
            "UPDATE pin_jobs SET next_attempt_at='2000-01-01T00:00:00Z' WHERE operation='submit'",
        )
        .await
        .unwrap();
    Box::pin(tick(&fixture, &scope)).await;
    let after = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.submit_calls, 2);
    assert_eq!(after.started_at, original.started_at);
    assert_eq!(
        (after.api, after.strategy, after.correlation),
        (original.api, original.strategy, original.correlation)
    );
    let facts = pin_submit_observation::Entity::find()
        .filter(pin_submit_observation::Column::JobId.eq(&seed.job_id))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(facts.len(), 2);
    assert!(
        facts
            .iter()
            .any(|fact| fact.submit_call == 1 && fact.outcome == "Rejected")
    );
    assert!(
        facts
            .iter()
            .any(|fact| fact.submit_call == 2 && fact.outcome == "Rejected")
    );
    assert_eq!(fixture.server.received_requests().await.unwrap().iter().filter(|request| request.method.as_str() == "POST" && request.url.path() == "/psa/pins").count(), 2);
    assert!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(&fixture.db)
            .await
            .unwrap()
            .is_none()
    );
}

async fn parked_native() -> (
    support::Fixture,
    pin_job::Model,
    ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot,
    management::ExactObject,
) {
    let fixture = Box::pin(support::Fixture::new()).await;
    let identity = Box::pin(fixture.publish("native-compat", true)).await;
    let job = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'; UPDATE remote_pin_ledger SET effect='not_created'").await.unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",
        [job.id.clone().into(), now.into()])).await.unwrap();
    let route = fixture.config.pinning.providers[0].route_snapshot();
    (fixture, job, route, identity)
}

async fn observation_row(fixture: &support::Fixture, job: &pin_job::Model, call: i32, route: &str) {
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    pin_submit_observation::Entity::insert(pin_submit_observation::ActiveModel {
        id: Set(format!("{}:call:{call}", job.id)),
        job_id: Set(job.id.clone()),
        submit_call: Set(call),
        provider: Set(job.provider.clone()),
        expected_cid: Set(job.cid.clone()),
        route: Set(route.into()),
        remote_epoch: Set(1),
        claim_until: Set(now + chrono::Duration::minutes(1)),
        effect: Set("not_created".into()),
        resources: Set("[]".into()),
        outcome: Set("Rejected".into()),
        safe_error: Set(None),
        needs_attention: Set(false),
        started_at: Set(now),
        observed_at: Set(Some(now)),
    })
    .exec(&fixture.db)
    .await
    .unwrap();
}

#[tokio::test]
async fn valid_non_rpc_history_keeps_explicit_repair_and_latest_any_contract() {
    let (fixture, job, route, _) = Box::pin(parked_native()).await;
    observation_row(&fixture, &job, 1, &serde_json::to_string(&route).unwrap()).await;
    let any = ledger::submission::latest(&fixture.db, &job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(any.outcome, "Rejected");
    assert!(
        jobs::resume_rejected_submit_after_repair(
            &fixture.db,
            &job.id,
            "noop",
            "cid",
            chrono::Utc::now()
        )
        .await
        .unwrap()
    );
    assert_eq!(
        jobs::submission_history(&fixture.db, &job.id)
            .await
            .unwrap()
            .unwrap()
            .submit_calls,
        1
    );
}

#[tokio::test]
async fn bounded_multi_call_history_is_accepted_without_granting_an_exhausted_retry() {
    let (fixture, job, route, _) = Box::pin(parked_native()).await;
    for call in 1..=8 {
        observation_row(
            &fixture,
            &job,
            call,
            &serde_json::to_string(&route).unwrap(),
        )
        .await;
    }
    fixture
        .db
        .execute_unprepared("UPDATE pin_submit_history SET submit_calls=8")
        .await
        .unwrap();
    assert_eq!(
        ledger::submission::latest(&fixture.db, &job.id)
            .await
            .unwrap()
            .unwrap()
            .submit_call,
        8
    );
    assert!(
        !jobs::resume_rejected_submit_after_repair(
            &fixture.db,
            &job.id,
            "noop",
            "cid",
            chrono::Utc::now()
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn unsafe_historical_rows_fail_closed_in_the_repair_consumer() {
    for defect in [
        "older_rpc",
        "older_malformed",
        "missing",
        "unsupported",
        "strategy",
        "provider",
        "cid",
        "route_conflict",
        "rows",
        "bytes",
    ] {
        let (fixture, job, route, _) = Box::pin(parked_native()).await;
        let mut older = route.clone();
        if defect == "older_rpc" {
            older.api_profile = "rpc".into();
        }
        if defect == "unsupported" {
            older.api_profile = "unknown-future-profile".into();
        }
        if defect == "strategy" {
            older.strategy = "unsupported-strategy".into();
        }
        if defect == "route_conflict" {
            older.scope = "different-historical-account".into();
        }
        let raw = match defect {
            "older_malformed" => "{",
            "missing" => "{}",
            _ => "",
        };
        let older_json = serde_json::to_string(&older).unwrap();
        observation_row(
            &fixture,
            &job,
            1,
            if raw.is_empty() { &older_json } else { raw },
        )
        .await;
        observation_row(&fixture, &job, 2, &serde_json::to_string(&route).unwrap()).await;
        if defect == "provider" {
            fixture
                .db
                .execute_unprepared(
                    "UPDATE pin_submit_observations SET provider='foreign' WHERE submit_call=1",
                )
                .await
                .unwrap();
        }
        if defect == "cid" {
            fixture.db.execute_unprepared("UPDATE pin_submit_observations SET expected_cid='foreign-cid' WHERE submit_call=1").await.unwrap();
        }
        if defect == "rows" {
            for call in 3..=9 {
                observation_row(
                    &fixture,
                    &job,
                    call,
                    &serde_json::to_string(&route).unwrap(),
                )
                .await;
            }
        }
        if defect == "bytes" {
            fixture
                .db
                .execute(Statement::from_sql_and_values(
                    fixture.db.get_database_backend(),
                    "UPDATE pin_submit_observations SET resources=$1 WHERE submit_call=1",
                    ["x".repeat(
                        ipfs_s3_gateway::store::pinning::management::native::MAX_BYTES + 1,
                    )
                    .into()],
                ))
                .await
                .unwrap();
        }
        assert_eq!(
            ledger::submission::latest(&fixture.db, &job.id)
                .await
                .unwrap()
                .unwrap()
                .route,
            serde_json::to_string(&route).unwrap()
        );
        assert!(
            jobs::resume_rejected_submit_after_repair(
                &fixture.db,
                &job.id,
                "noop",
                "cid",
                chrono::Utc::now()
            )
            .await
            .is_err(),
            "{defect}"
        );
        assert_eq!(
            jobs::submission_history(&fixture.db, &job.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "blocked"
        );
        assert!(
            fixture
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| !request.url.path().starts_with("/psa/"))
        );
    }
}

#[tokio::test]
async fn explicit_retry_uses_original_native_proof_not_an_observational_outcome() {
    for unknown in [false, true] {
        let (fixture, job, route, identity) = Box::pin(parked_native()).await;
        observation_row(&fixture, &job, 1, &serde_json::to_string(&route).unwrap()).await;
        if unknown {
            fixture.db.execute_unprepared("UPDATE pin_submit_history SET effect='unknown',state='needs_attention'; UPDATE remote_pin_ledger SET effect='unknown'; UPDATE pin_submit_observations SET outcome='Unknown',effect='unknown',needs_attention=1").await.unwrap();
        }
        let request = fixture
            .existing_request("observation-repair", identity, management::Action::Retry)
            .await;
        let binding = Box::pin(fixture.confirm(&request)).await;
        let applied = Box::pin(actions::apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &binding,
        ))
        .await
        .unwrap();
        let after = pin_job::Entity::find_by_id(job.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        if unknown {
            assert_eq!(after.state, "running");
            assert_eq!(after.submit_phase.as_deref(), Some("recovering"));
            assert!(
                applied.items[0]
                    .jobs
                    .iter()
                    .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
            );
        } else {
            assert_eq!(after.state, "pending");
            assert_eq!(after.submit_phase.as_deref(), Some("ready"));
            assert_eq!(applied.items[0].receipt.job_ids, vec![job.id.clone()]);
        }
        assert_eq!(
            jobs::submission_history(&fixture.db, &job.id)
                .await
                .unwrap()
                .unwrap()
                .submit_calls,
            1
        );
    }
}

#[tokio::test]
async fn unknown_non_rpc_response_uses_historical_find_without_rpc_park_or_new_post() {
    let fixture = Box::pin(support::psa_fixture()).await;
    let identity = Box::pin(fixture.publish("unknown-find", false)).await;
    let binding =
        Box::pin(fixture.confirm(&fixture.create_request("unknown-find", identity, false))).await;
    let applied = Box::pin(actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    ))
    .await
    .unwrap();
    let seed = &applied.items[0].jobs[0];
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/psa/pins"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"count":0,"results":[]})),
        )
        .mount(&fixture.server)
        .await;
    let scope = Arc::new(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .unwrap(),
    );
    tick(&fixture, &scope).await;
    let history = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.api, "psa");
    assert_eq!(history.strategy, "cid");
    assert_eq!(history.submit_calls, 1);
    assert_eq!(history.recovery_queries, 1);
    assert_ne!(history.state, "needs_attention");
    let requests = fixture.server.received_requests().await.unwrap();
    let finds = requests
        .iter()
        .filter(|request| request.method.as_str() == "GET" && request.url.path() == "/psa/pins")
        .collect::<Vec<_>>();
    assert_eq!(finds.len(), 1);
    let metadata = finds[0]
        .url
        .query_pairs()
        .find(|(key, _)| key == "meta")
        .unwrap()
        .1
        .into_owned();
    let metadata: serde_json::Value = serde_json::from_str(&metadata).unwrap();
    assert_eq!(
        metadata["gateway_job_id"],
        history.correlation.as_deref().unwrap()
    );
    assert_eq!(requests.iter().filter(|request| request.method.as_str() == "POST" && request.url.path() == "/psa/pins").count(), 1);
    let fact = ledger::submission::latest(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.outcome, "Unknown");
    assert_eq!(fact.effect, "unknown");
    assert!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("poll"))
            .one(&fixture.db)
            .await
            .unwrap()
            .is_none()
    );
}
