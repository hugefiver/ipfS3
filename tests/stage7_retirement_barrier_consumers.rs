//! C consumer regressions. Controlled Noop/Kubo fixtures are protocol fixtures,
//! never physical remote PIN, directory bytes, replica or cleanup proof.
#[path = "support/management_wire_v3.rs"]
mod archive;
#[path = "support/stage7_retirement_barrier_packed_result.rs"]
mod packed_result;
#[path = "support/stage7_retirement_barrier_replicas.rs"]
mod replicas;
#[path = "support/stage7_retirement_barrier_seed_contract.rs"]
mod seed_contract;
#[path = "support/stage7_management_actions.rs"]
pub mod support;

use ipfs_s3_gateway::{
    pinning::{coordinator::PinningCoordinator, management::Action, worker::ManagementJobScope},
    store::{
        Store,
        entities::{pin_job, pin_lease, pin_lease_requirement},
        pinning::management::{actions, native, records},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Statement};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn confirmed_automatic_renew_keeps_native_created_at_source_and_old_claim_generation() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("native-automatic-renew", true).await;
    let original = pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(&identity.object_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let birth = pin_lease_requirement::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let original_jobs = pin_job::Entity::find()
        .filter(pin_job::Column::LeaseId.eq(&original.id))
        .all(&fixture.db)
        .await
        .unwrap();
    let native_start = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT CAST(created_at AS TEXT) AS original FROM pin_leases WHERE id=$1",
            [original.id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "original")
        .unwrap();
    let request = fixture
        .existing_request(
            "confirmed-native-renew",
            identity,
            Action::Renew {
                expires_at: original.expires_at.timestamp() + 600,
            },
        )
        .await;
    let binding = fixture.confirm(&request).await;
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let renewed = pin_lease::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.source, original.source);
    assert_eq!(renewed.owner_object_id, original.owner_object_id);
    assert_eq!(renewed.created_at, original.created_at);
    assert_eq!(renewed.generation, original.generation + 1);
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(original.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .contract,
        birth.contract
    );
    let after_start = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT CAST(created_at AS TEXT) AS original FROM pin_leases WHERE id=$1",
            [original.id.into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<String>("", "original")
        .unwrap();
    assert_eq!(after_start, native_start);
    for job in original_jobs {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id)
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap()
                .expected_generation,
            Some(original.generation)
        );
    }
    let authorities = result
        .items
        .iter()
        .flat_map(|item| &item.jobs)
        .filter(|job| job.lease_id.as_deref() == Some(renewed.id.as_str()))
        .collect::<Vec<_>>();
    assert!(
        !authorities.is_empty(),
        "renewed outbox must have real current-generation authority"
    );
    assert!(
        authorities
            .iter()
            .all(|job| job.lease_generation == Some(renewed.generation))
    );
}

#[tokio::test]
async fn execution_scope_requires_applied_private_receipt_not_a_confirmation_or_caller_id() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("unapplied-scope", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("unapplied-scope", identity, false))
        .await;
    assert!(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .is_err()
    );
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(
        ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
            .await
            .is_ok()
    );
    let bytes = archive::export(&binding);
    let mut forged = (*binding.request).clone();
    forged.items[0].item_id = "caller-chosen-job-id".into();
    assert!(
        archive::restore(&fixture.db, &fixture.auth, &forged, &bytes)
            .await
            .is_err()
    );
    let other = support::Fixture::new().await;
    assert!(
        ManagementJobScope::from_applied(&other.db, &other.auth, &binding)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn readonly_missing_original_resource_stays_attention_and_never_ticks_the_old_queue() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("old-ordinary-queue", true).await;
    let request = fixture
        .existing_request("readonly-missing-resource", identity, Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(
        applied.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let original_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let report = coordinator
        .management_tick(
            &Store::new(fixture.db.clone()),
            &scope,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let after = native::capture(&fixture.db).await.unwrap();
    for table in [
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
        "pin_resource_history",
        "pin_submit_history",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "readonly work changed {table}"
        );
    }
    for job in original_jobs
        .into_iter()
        .filter(|job| !applied.items[0].receipt.job_ids.contains(&job.id))
    {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap(),
            job
        );
    }
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        calls,
        "missing identity must not trigger a provider search/POST/DELETE"
    );
    assert!(report.submit_observations.is_empty());
    assert!(report.replica_observations.is_empty());
    // Completion aggregation belongs to B: a missing original opaque identity
    // is attention, not a successfully completed ReadOnly observation.
    assert_eq!(report.items[0].state, records::ActionState::NeedsAttention);
}

#[tokio::test]
async fn receipt_scoped_readonly_psa_get_preserves_actual_report_with_zero_new_post_or_delete() {
    use ipfs_s3_gateway::{
        pinning::{
            identity::{CleanupMode, Ownership},
            management::local_config,
            provider::ObservedResourceStatus,
        },
        store::{entities::remote_pin, pinning::ledger},
    };
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };
    let mut fixture = support::Fixture::new().await;
    let provider = &mut fixture.raw.pinning.providers[0];
    provider.kind = "pinata".into();
    provider.api = None;
    provider.endpoint = Some(format!("{}/psa", fixture.server.uri()));
    provider.token_env = Some("STAGE7_C_CONTROLLED_TOKEN".into());
    let identity = &mut fixture.raw.pinning_identity.providers[0];
    identity.provider_id = "controlled-psa".into();
    identity.backend = "pinata".into();
    identity.api_profile = "pinata-psa".into();
    identity.secret_ref = Some("env:STAGE7_C_CONTROLLED_TOKEN".into());
    identity.cleanup = CleanupMode::Retain;
    fixture.config =
        local_config::compile(&fixture.raw, |_| Some("controlled-fixture-token".into()))
            .unwrap()
            .with_source_node(support::NODE.into())
            .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let response = serde_json::json!({"requestid":"controlled-existing-request","status":"queued",
        "created":"2026-01-01T00:00:00Z","pin":{"cid":support::CID,"name":"controlled"},"delegates":[],"info":{}});
    Mock::given(method("POST"))
        .and(path("/psa/pins"))
        .respond_with(ResponseTemplate::new(202).set_body_json(response.clone()))
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path("/psa/pins/controlled-existing-request"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let object = fixture.publish("actual-readonly-report", false).await;
    let create = fixture
        .confirm(&fixture.create_request("controlled-create", object.clone(), false))
        .await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &create)
        .await
        .unwrap();
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
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        remote.request_id.as_deref(),
        Some("controlled-existing-request")
    );
    assert_eq!(remote.status, "queued"); // Actual mocked protocol response, not physical PIN proof.
    let request = fixture
        .existing_request("controlled-readonly", object, Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(
        applied.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &binding)
        .await
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let requests = fixture.server.received_requests().await.unwrap().len();
    let report = coordinator
        .management_tick(&store, &scope, &CancellationToken::new())
        .await
        .unwrap();
    let after = native::capture(&fixture.db).await.unwrap();
    for table in [
        "pin_leases",
        "pin_lease_targets",
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
        "pin_resource_history",
        "pin_submit_history",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "readonly GET changed {table}"
        );
    }
    let calls = fixture.server.received_requests().await.unwrap();
    let calls = &calls[requests..];
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].method.as_str(), "GET");
    assert!(
        calls
            .iter()
            .all(|call| !matches!(call.method.as_str(), "POST" | "DELETE"))
    );
    let fact = report
        .replica_observations
        .iter()
        .find(|fact| applied.items[0].receipt.job_ids.contains(&fact.job_id))
        .unwrap();
    assert!(!fact.projectable);
    assert_eq!(fact.phase, "reconcile");
    assert_eq!(fact.remote_epoch, remote.epoch);
    assert_eq!(fact.outcome, "ReportedQueued");
    let resources: Vec<ipfs_s3_gateway::pinning::provider::ObservedResource> =
        serde_json::from_str(&fact.resources).unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].request_id, "controlled-existing-request");
    assert_eq!(resources[0].status, ObservedResourceStatus::Reported);
    assert_eq!(resources[0].ownership, Ownership::Unknown);
    assert!(resources[0].replica_evidence.is_none());
}
