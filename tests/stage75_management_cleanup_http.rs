#[path = "support/stage75_workflow.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    pinning::{
        identity::CleanupMode,
        management::{
            Action, ItemRequest, PlanRequest, RetirementResource, RetirementSelection,
            RouteIdentity, Subject, local_config,
            retirement::{RunLimits, Workspace},
        },
    },
    store::{
        Store,
        entities::{pin_job, pin_provider_usage, remote_pin},
        pinning::{
            ledger,
            management::{ManagementJobScope, actions, records},
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn confirmed_retirement_cleanup_runs_one_scoped_delete_and_replays_the_original_result() {
    let mut fixture = support::existing::psa_fixture().await;
    fixture.raw.pinning_identity.providers[0].cleanup = CleanupMode::Managed;
    fixture.config =
        local_config::compile(&fixture.raw, |_| Some("controlled-worker-token".into()))
            .unwrap()
            .with_source_node(support::NODE.into())
            .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    // An existing, explicitly application-owned historical resource. Ordinary
    // PSA reports alone are not a grant to delete an external resource.
    fixture.publish("cleanup-owner", true).await;
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .confirmed_remote(&remote.provider, &remote.cid)
        .await;
    fixture.db.execute_unprepared("UPDATE pin_leases SET state='cancelled',generation=generation+1; UPDATE pin_lease_targets SET state='released'").await.unwrap();
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let route = RouteIdentity::from_original(provider.route_snapshot());
    let cid = ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
    let request = PlanRequest {
        plan_id: "cleanup-retirement".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            subject: Subject::Provider {
                route: route.clone(),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup {
                    resources: vec![RetirementResource {
                        key: route.remote_key(&cid),
                        route,
                        epoch: remote.epoch,
                        resource_id: remote.request_id.clone().unwrap(),
                    }],
                },
            },
        }],
    };
    let confirmed = fixture.confirm(&request).await;
    let applied = support::apply(&fixture, &confirmed).await;
    assert_eq!(applied.items[0].jobs.len(), 1);
    assert_eq!(
        applied.items[0].jobs[0].directive,
        records::JobDirective::ExistingCleanup
    );
    let document = support::export_global(&fixture, &confirmed).await;
    Mock::given(method("DELETE"))
        .and(path(format!(
            "/psa/pins/{}",
            remote.request_id.as_ref().unwrap()
        )))
        .respond_with(ResponseTemplate::new(202))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &confirmed)
        .await
        .unwrap();
    let coordinator = support::existing::coordinator(&fixture);
    let store = Store::new(fixture.db.clone());
    tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
    })
    .await
    .unwrap()
    .unwrap();
    let job = pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
    let usage = pin_provider_usage::Entity::find_by_id(provider.name.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(usage.reserved_pins, 0);
    assert_eq!(usage.reserved_bytes, 0);
    let mut workspace = Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let limits = RunLimits::default();
    let mut status = actions::status::lookup_retirement_with_work(
        &fixture.db,
        &fixture.auth,
        &confirmed,
        "retire",
        &path,
        file,
        &limits,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        status
            .advance(&fixture.db, &limits)
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::Accepted
    );
    status.close().await.unwrap();
    workspace.cleanup().unwrap();
    support::replay(&fixture, &confirmed, &applied, &document).await;
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "DELETE")
            .count(),
        1
    );
}

#[tokio::test]
async fn real_readonly_psa_reports_are_attributable_but_do_not_grant_cleanup_ownership() {
    let mut fixture = support::existing::psa_fixture().await;
    fixture.raw.pinning_identity.providers[0].cleanup = CleanupMode::Managed;
    fixture.config =
        local_config::compile(&fixture.raw, |_| Some("controlled-worker-token".into()))
            .unwrap()
            .with_source_node(support::NODE.into())
            .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let identity = fixture.publish("readonly-owner", false).await;
    let birth = fixture
        .confirm(&fixture.create_request("readonly-birth", identity, false))
        .await;
    let applied = support::apply(&fixture, &birth).await;
    support::settle(&fixture, &birth, &applied).await;
    fixture.db.execute_unprepared("UPDATE pin_leases SET state='cancelled',generation=generation+1; UPDATE pin_lease_targets SET state='released'").await.unwrap();
    let route = RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot());
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let cid = ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
    let denied = PlanRequest {
        plan_id: "readonly-cleanup-denied".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            subject: Subject::Provider {
                route: route.clone(),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup {
                    resources: vec![RetirementResource {
                        key: route.remote_key(&cid),
                        route: route.clone(),
                        epoch: remote.epoch,
                        resource_id: remote.request_id.clone().unwrap(),
                    }],
                },
            },
        }],
    };
    let snapshot = ipfs_s3_gateway::pinning::management::db_snapshot::collect_with_config(
        &fixture.db,
        &fixture.config,
        &denied,
    )
    .await
    .unwrap()
    .planning_snapshot()
    .unwrap();
    assert!(matches!(
        ipfs_s3_gateway::pinning::management::retirement::PreparedPlanning::collect(
            &fixture.db,
            &denied,
            &snapshot,
            records::SharedDecodeBudget::default()
        )
        .await,
        Err(ipfs_s3_gateway::store::pinning::management::ManagementError::Confirmation)
    ));
    let request = PlanRequest {
        plan_id: "readonly-retirement".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            subject: Subject::Provider { route },
            action: Action::RetireWith {
                selection: RetirementSelection::Retain,
            },
        }],
    };
    let confirmed = fixture.confirm(&request).await;
    let Some(ipfs_s3_gateway::pinning::management::Capture::Provider(capture)) =
        &confirmed.confirmation.bindings[0].capture
    else {
        panic!("missing inventory")
    };
    assert!(capture.totals.attribution_complete);
    let retired = support::apply(&fixture, &confirmed).await;
    assert!(retired.items[0].receipt.job_ids.is_empty());
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method.as_str() == "DELETE")
            .count(),
        0
    );
}
