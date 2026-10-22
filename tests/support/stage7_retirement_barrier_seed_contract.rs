//! Original committed scopes, not caller IDs or epoch arithmetic. HTTP responses
//! are controlled protocol fixtures, not physical PIN or exclusive ownership proof.
use super::support;
use ipfs_s3_gateway::{
    pinning::{
        coordinator::PinningCoordinator,
        management::{self, Action},
        worker::ManagementJobScope,
    },
    store::{
        Store,
        entities::{pin_job, pin_lease, remote_pin},
        pinning::{
            ledger,
            management::{actions, native, records},
        },
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Statement, TransactionTrait,
};
use tokio_util::sync::CancellationToken;

async fn confirm_with_ttl(
    fixture: &support::Fixture,
    request: &management::PlanRequest,
) -> records::ConfirmedBinding {
    let snapshot =
        management::db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    let prepared = management::retirement::PreparedPlanning::collect(
        &fixture.db,
        request,
        &snapshot,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let plan = &prepared.plan;
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == management::PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (request, &snapshot, plan),
        ack,
        chrono::Duration::seconds(5),
    )
    .await
    .unwrap();
    prepared.close().await.unwrap();
    binding
}

#[tokio::test]
async fn local_factory_restores_expired_applied_scope_but_never_an_expired_unapplied_grant() {
    for applied in [false, true] {
        let fixture = support::Fixture::new().await;
        let identity = fixture.publish("factory-original", false).await;
        let request = fixture.create_request("factory-original", identity, false);
        let binding = confirm_with_ttl(&fixture, &request).await;
        assert!(
            ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
                .await
                .is_err()
        );
        if applied {
            actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
                .await
                .unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(6), async {
            while ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
                .await
                .unwrap()
                < binding.expires_at
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("original confirmation did not expire on the real DB clock");
        let before = native::capture(&fixture.db).await.unwrap();
        let calls = fixture.server.received_requests().await.unwrap().len();
        let restored = records::load_confirmed_authority(
            &fixture.db,
            &fixture.auth,
            &request,
            records::SharedDecodeBudget::default(),
        )
        .await
        .unwrap();
        assert!(restored == binding);
        assert_eq!(
            ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &restored)
                .await
                .is_ok(),
            applied
        );
        assert!(native::capture(&fixture.db).await.unwrap() == before);
        assert_eq!(
            fixture.server.received_requests().await.unwrap().len(),
            calls
        );
    }
}

#[tokio::test]
async fn immutable_applied_authorities_reject_sql_rewrites_and_substituted_inspection_bindings() {
    use ipfs_s3_gateway::store::pinning::management::{ManagementError, actions::provider_points};
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("factory-complete", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("factory-complete", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.items[0].jobs.is_empty());
    assert!(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .is_ok()
    );
    let key = native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            fixture.auth.database_scope().into(),
            binding.request.plan_id.clone(),
            binding.request.items[0].item_id.clone(),
            "action".into(),
        ],
    };
    let original = provider_points::original_record(&fixture.db, &key)
        .await
        .unwrap()
        .unwrap();
    let error = fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_records SET result='{}' WHERE database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind='action'",
        [fixture.auth.database_scope().into(), binding.request.plan_id.clone().into(), binding.request.items[0].item_id.clone().into()]))
        .await.unwrap_err();
    match error {
        sea_orm::DbErr::Exec(sea_orm::RuntimeErr::SqlxError(sqlx::Error::Database(error))) => {
            assert_eq!(error.code().as_deref(), Some("1811"));
        }
        other => panic!("expected immutable SQLite trigger rejection, got {other:?}"),
    }
    assert!(
        provider_points::original_record(&fixture.db, &key)
            .await
            .unwrap()
            .as_ref()
            == Some(&original)
    );
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let domain = provider_points::Domain {
        allocation: fixture.config.pinning.providers[0].name.clone(),
        backend: route.backend.clone(),
        scope: route.scope.clone(),
        resource_type: route.resource_type(),
    };
    let namespace = provider_points::NamespaceChange {
        source: domain.clone(),
        destination: domain,
        destination_route: route,
    };
    let inspected = provider_points::inspect(&fixture.db, &key, &original, &namespace)
        .await
        .unwrap();
    for authority in &applied.items[0].jobs {
        assert!(
            inspected
                .points()
                .iter()
                .any(|point| point.job_authority.as_ref() == Some(authority))
        );
    }
    let mut substituted = original;
    substituted.insert("binding".into(), Some("{}".into()));
    assert!(matches!(
        provider_points::inspect(&fixture.db, &key, &substituted, &namespace).await,
        Err(ManagementError::Changed)
    ));
    assert!(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .is_ok()
    );
    assert!(
        records::lookup_committed(
            &fixture.db,
            &fixture.auth,
            &binding,
            &binding.request.items[0].item_id
        )
        .await
        .unwrap()
        .unwrap()
            == applied.items[0]
    );
}

async fn psa_fixture() -> support::Fixture {
    use ipfs_s3_gateway::pinning::identity::CleanupMode;
    let mut fixture = support::Fixture::new().await;
    let provider = &mut fixture.raw.pinning.providers[0];
    provider.kind = "pinata".into();
    provider.api = None;
    provider.endpoint = Some(format!("{}/psa", fixture.server.uri()));
    provider.token_env = Some("STAGE7_C_SEED_TOKEN".into());
    let identity = &mut fixture.raw.pinning_identity.providers[0];
    identity.provider_id = "controlled-seed-psa".into();
    identity.backend = "pinata".into();
    identity.api_profile = "pinata-psa".into();
    identity.secret_ref = Some("env:STAGE7_C_SEED_TOKEN".into());
    identity.cleanup = CleanupMode::Retain;
    fixture.config = management::local_config::compile(&fixture.raw, |_| {
        Some("controlled-fixture-token".into())
    })
    .unwrap()
    .with_source_node(support::NODE.into())
    .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    fixture
}

#[tokio::test]
async fn shared_automatic_source_and_its_request_bound_poll_keep_the_complete_original_seed() {
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    let fixture = psa_fixture().await;
    let identity = fixture.publish("shared-automatic-source", true).await;
    let automatic = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(&identity.object_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let original_job = pin_job::Entity::find()
        .filter(pin_job::Column::LeaseId.eq(&automatic.id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    // A rejected native queue is retryable; an unresolved reservation must stay
    // read-only. These seed rows are not evidence of a physical provider effect.
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected' WHERE id=$1",
        [original_job.id.clone().into()],
    )).await.unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE remote_pin_ledger SET effect='not_created' WHERE provider=$1 AND cid=$2",
            [
                original_job.provider.clone().into(),
                original_job.cid.clone().into(),
            ],
        ))
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(
        fixture.db.get_database_backend(),
        "INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'psa','cid','not_created','blocked',1,0,$2)",
        [original_job.id.clone().into(), now.into()],
    )).await.unwrap();
    let binding = fixture
        .confirm(&fixture.create_request("shared-managed-reference", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let seed = applied.items[0].jobs.iter().find(|authority|
        authority.directive == records::JobDirective::Submit && authority.lease_id.as_deref() == Some(automatic.id.as_str()))
        .unwrap_or_else(|| panic!("the shared source must retain the existing automatic job, not acquire a disguised owner: jobs={}", serde_json::to_string(&applied.items[0].jobs).unwrap())).clone();
    assert!(!applied.items[0].receipt.intent_ids.contains(&automatic.id));
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let original = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        actions::job_authority::lookup(&fixture.db, &original)
            .await
            .unwrap()
            .as_ref()
            == Some(&seed)
    );
    let response = serde_json::json!({"requestid":"original-shared-request","status":"queued",
        "created":"2026-01-01T00:00:00Z","pin":{"cid":support::CID,"name":"controlled"},"delegates":[],"info":{}});
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(202).set_body_json(response.clone()))
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/psa/pins/original-shared-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let store = Store::new(fixture.db.clone());
    coordinator
        .management_tick(&store, &scope, &CancellationToken::new())
        .await
        .unwrap();
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        remote.request_id.as_deref(),
        Some("original-shared-request")
    );
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::LeaseId.eq(&automatic.id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let mut expected = seed.clone();
    expected.job_id.clone_from(&poll.id);
    assert!(
        actions::job_authority::lookup(&fixture.db, &poll)
            .await
            .unwrap()
            .as_ref()
            == Some(&expected),
        "B must find the shared original seed even when the referring action's intent IDs do not contain its automatic source lease"
    );
    assert_eq!(expected.remote_epoch, seed.remote_epoch);
    assert_eq!(poll.expected_generation, Some(automatic.generation));
    // Move only this real Poll's due time, not its generation/epoch/claim/proof.
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET next_attempt_at=$1 WHERE id=$2",
            [chrono::Utc::now().into(), poll.id.into()],
        ))
        .await
        .unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    coordinator
        .management_tick(&store, &scope, &CancellationToken::new())
        .await
        .unwrap();
    let requests = fixture.server.received_requests().await.unwrap();
    assert_eq!(
        requests[calls..]
            .iter()
            .filter(|request| request.method.as_str() == "GET")
            .count(),
        1
    );
    assert!(
        requests[calls..]
            .iter()
            .filter(|request| request.url.path().starts_with("/psa/"))
            .all(|request| request.method.as_str() == "GET")
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(automatic.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        automatic
    );
    assert!(
        records::lookup_committed(
            &fixture.db,
            &fixture.auth,
            &binding,
            &binding.request.items[0].item_id
        )
        .await
        .unwrap()
        .unwrap()
        .jobs
        .iter()
        .any(|authority| authority == &seed)
    );
}

#[tokio::test]
async fn already_dispatched_readonly_get_keeps_nonprojecting_fact_after_exact_claim_is_completed() {
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    let fixture = psa_fixture().await;
    let identity = fixture.publish("late-readonly-source", false).await;
    let create = fixture
        .confirm(&fixture.create_request("late-readonly-create", identity.clone(), false))
        .await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &create)
        .await
        .unwrap();
    let response = serde_json::json!({"requestid":"late-readonly-request","status":"queued",
        "created":"2026-01-01T00:00:00Z","pin":{"cid":support::CID,"name":"controlled"},"delegates":[],"info":{}});
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(202).set_body_json(response.clone()))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let store = Store::new(fixture.db.clone());
    let create_scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &create)
        .await
        .unwrap();
    coordinator
        .management_tick(&store, &create_scope, &CancellationToken::new())
        .await
        .unwrap();
    let request = fixture
        .existing_request("late-readonly-observe", identity, Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let job_id = applied.items[0]
        .jobs
        .iter()
        .find(|authority| authority.directive == records::JobDirective::ReadOnlyObserve)
        .unwrap()
        .job_id
        .clone();
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    Mock::given(method("GET"))
        .and(path("/psa/pins/late-readonly-request"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(response)
                .set_delay(std::time::Duration::from_millis(250)),
        )
        .expect(1)
        .mount(&fixture.server)
        .await;
    let before = native::capture(&fixture.db).await.unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    let tick = tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(6), async {
        loop {
            if fixture.server.received_requests().await.unwrap()[calls..]
                .iter()
                .any(|request| {
                    request.method.as_str() == "GET"
                        && request.url.path() == "/psa/pins/late-readonly-request"
                })
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual readonly GET did not dispatch");
    let claimed = pin_job::Entity::find_by_id(job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let until = claimed.locked_until.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    ipfs_s3_gateway::store::pinning::jobs::complete_job(&tx, &job_id, until, now)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let report = tick.await.unwrap().unwrap();
    let fact = report
        .replica_observations
        .iter()
        .find(|fact| fact.job_id == job_id)
        .unwrap();
    assert!(!fact.projectable);
    assert_eq!(fact.phase, "reconcile");
    assert_eq!(fact.outcome, "ReportedQueued");
    assert_eq!(fact.claim_until, until);
    let after = native::capture(&fixture.db).await.unwrap();
    for table in [
        "pin_leases",
        "pin_lease_requirements",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
        "pin_resource_history",
        "pin_submit_history",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "late readonly response changed {table}"
        );
    }
    let requests = fixture.server.received_requests().await.unwrap();
    assert!(
        requests[calls..]
            .iter()
            .all(|request| request.method.as_str() == "GET")
    );
}
