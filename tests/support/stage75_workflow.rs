#[path = "stage7_management_worker.rs"]
pub mod existing;
pub use existing::{Fixture, NODE};

use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::{coordinator::PinningCoordinator, management::*, worker::ManagementJobScope},
    store::{
        self, Store,
        entities::{pin_job, pin_lease_target, pin_replica_observation, remote_pin},
        pinning::management::{actions, records},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Statement};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

pub fn compile(fixture: &Fixture) -> local_config::ManagementConfig {
    local_config::compile(&fixture.raw, |_| Some("controlled-worker-token".into()))
        .unwrap()
        .with_source_node(NODE.into())
        .unwrap()
}

pub async fn prepare(fixture: &Fixture, identity: ExactObject) {
    evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &KuboClient::new(fixture.server.uri()),
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity,
            expected_node: NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
}

pub async fn add_provider(fixture: &mut Fixture, name: &str) -> RouteIdentity {
    let mut provider = fixture.raw.pinning.providers[0].clone();
    provider.name = name.into();
    provider.endpoint = Some(format!("{}/{name}/psa", fixture.server.uri()));
    fixture.raw.pinning.providers.push(provider);
    let mut identity = fixture.raw.pinning_identity.providers[0].clone();
    identity.config_name = name.into();
    identity.provider_id = format!("{name}-provider");
    identity.scope = name.into();
    identity.storage_domain = name.into();
    fixture.raw.pinning_identity.providers.push(identity);
    fixture.raw.pinning.policies[0].providers.push(name.into());
    fixture.config = compile(fixture);
    let provider = fixture
        .config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == format!("{name}-provider"))
        .unwrap();
    store::pinning::ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    RouteIdentity::from_original(provider.route_snapshot())
}

pub fn selected(plan: &str, identity: ExactObject, target: &str, action: Action) -> PlanRequest {
    PlanRequest {
        plan_id: plan.into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action,
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target.into(),
                    source: None,
                },
            },
        }],
    }
}

pub async fn apply(fixture: &Fixture, binding: &records::ConfirmedBinding) -> actions::ApplyResult {
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, binding)
        .await
        .unwrap_or_else(|error| panic!("apply {}: {error:?}", binding.request.plan_id));
    assert!(
        !result.replayed,
        "first application must commit new business"
    );
    for item in &result.items {
        assert!(
            records::lookup_committed(
                &fixture.db,
                &fixture.auth,
                binding,
                &item.receipt.binding.request.item_id
            )
            .await
            .unwrap()
            .as_ref()
                == Some(item)
        );
    }
    result
}

pub async fn assert_complete(fixture: &Fixture, binding: &records::ConfirmedBinding, lease: &str) {
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    assert!(
        store::entities::pin_lease_requirement::contract::completion(&fixture.db, lease, now)
            .await
            .unwrap()
            .is_some_and(|(_, complete, _)| complete)
    );
    let status = actions::status::lookup(&fixture.db, &fixture.auth, binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        status.state,
        records::ActionState::RemoteComplete | records::ActionState::ReplicationComplete
    ));
}

/// Real PSA adapter and real receipt-scoped worker, with isolated HTTP on both
/// the Kubo and provider surfaces. SQL advances only the exact poll's due time.
pub async fn settle(
    fixture: &Fixture,
    binding: &records::ConfirmedBinding,
    applied: &actions::ApplyResult,
) {
    let seed = applied
        .items
        .iter()
        .flat_map(|item| &item.jobs)
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap();
    let provider = fixture
        .config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.route_snapshot() == seed.route)
        .unwrap();
    let endpoint = provider.endpoint.as_deref().unwrap();
    assert!(endpoint.starts_with(&format!("{}/", fixture.server.uri())));
    let base = endpoint.strip_prefix(&fixture.server.uri()).unwrap();
    let submit_path = format!("{base}/pins");
    let resource = format!("stage75-{}", seed.route.provider_id);
    let poll_path = format!("{submit_path}/{resource}");
    let response = |status: &str| {
        serde_json::json!({
            "requestid": resource, "status": status, "created": "2026-01-01T00:00:00Z",
            "pin": {"cid": seed.cid}, "delegates": [], "info": {}
        })
    };
    Mock::given(method("POST"))
        .and(path(&submit_path))
        .respond_with(ResponseTemplate::new(202).set_body_json(response("queued")))
        .expect(1)
        .mount(&fixture.server)
        .await;
    Mock::given(method("GET"))
        .and(path(&poll_path))
        .respond_with(ResponseTemplate::new(200).set_body_json(response("pinned")))
        .expect(1)
        .mount(&fixture.server)
        .await;
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let scope = ManagementJobScope::from_applied(&fixture.db, &fixture.auth, binding)
        .await
        .unwrap();
    let store = Store::new(fixture.db.clone());
    let (coordinator, store, scope) = tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
            .unwrap();
        (coordinator, store, scope)
    })
    .await
    .unwrap();
    let submit = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(submit.state, "done");
    let history = store::pinning::jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.api, "psa");
    let poll = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&seed.provider))
        .filter(pin_job::Column::Cid.eq(&seed.cid))
        .filter(pin_job::Column::Operation.eq("poll"))
        .filter(pin_job::Column::State.eq("pending"))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_jobs SET next_attempt_at='2000-01-01T00:00:00Z' WHERE id=$1",
            [poll.id.clone().into()],
        ))
        .await
        .unwrap();
    tokio::spawn(async move {
        coordinator
            .management_tick(&store, &scope, &CancellationToken::new())
            .await
            .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(poll.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "done"
    );
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "pinned");
    assert_eq!(remote.request_id.as_deref(), Some(resource.as_str()));
    assert_eq!(remote.epoch, seed.remote_epoch);
    let target = pin_lease_target::Entity::find_by_id(seed.target_id.clone().unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    let fact = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&poll.id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.phase, "poll");
    assert_eq!(fact.outcome, "ReportedPinned");
    assert_eq!(fact.expected_cid, seed.cid);
    assert_eq!(fact.remote_epoch, seed.remote_epoch);
    assert_eq!(fact.lease_id, seed.lease_id);
    assert_eq!(fact.target_id, seed.target_id);
    assert_eq!(fact.expected_generation, seed.lease_generation);
    let route: ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot =
        serde_json::from_str(&fact.route).unwrap();
    assert!(route == seed.route);
    assert_complete(fixture, binding, seed.lease_id.as_deref().unwrap()).await;
    let calls = fixture.server.received_requests().await.unwrap();
    assert_eq!(
        calls
            .iter()
            .filter(|r| r.method.as_str() == "POST" && r.url.path() == submit_path)
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|r| r.method.as_str() == "GET" && r.url.path() == poll_path)
            .count(),
        1
    );
    assert!(calls.iter().all(|r| r.method.as_str() != "DELETE"
        && !r.url.path().ends_with("/pin/rm")
        && !r.url.path().ends_with("/repo/gc")));
}

pub async fn reopen(fixture: &mut Fixture) {
    fixture.db.clone().close().await.unwrap();
    let path = fixture.directory.path().join("management.sqlite");
    fixture.db = store::connect_database(&format!(
        "sqlite://{}?mode=rw",
        path.to_string_lossy().replace('\\', "/")
    ))
    .await
    .unwrap();
    fixture.auth = records::authorize_local(&fixture.raw, &fixture.db)
        .await
        .unwrap();
}

pub fn export(binding: &records::ConfirmedBinding) -> Vec<u8> {
    let mut writer = wire::v3::Writer::new(
        Vec::new(),
        wire::v3::Projection::Review,
        wire::v3::files::stream::budget(),
    )
    .unwrap();
    wire::v3::planning::local_confirmed(&mut writer, binding, wire::v3::Projection::Review)
        .unwrap();
    writer.finish().unwrap()
}
pub async fn export_global(fixture: &Fixture, binding: &records::ConfirmedBinding) -> Vec<u8> {
    let mut writer = wire::v3::Writer::new(
        Vec::new(),
        wire::v3::Projection::Review,
        wire::v3::files::stream::budget(),
    )
    .unwrap();
    wire::v3::planning::confirmed(
        &fixture.db,
        &mut writer,
        binding,
        wire::v3::Projection::Review,
    )
    .await
    .unwrap();
    writer.finish().unwrap()
}

pub async fn replay(
    fixture: &Fixture,
    original: &records::ConfirmedBinding,
    expected: &actions::ApplyResult,
    bytes: &[u8],
) {
    let imported = wire::v3::archive_cursor::stage_import(
        std::io::Cursor::new(bytes),
        wire::v3::Projection::Review,
        wire::v3::files::stream::budget(),
    )
    .unwrap();
    let work = records::SharedDecodeBudget::default();
    let restored = records::load_confirmed_authority(
        &fixture.db,
        &fixture.auth,
        &original.request,
        work.clone(),
    )
    .await
    .unwrap();
    wire::v3::matching::confirmed_database(&fixture.db, imported.into_cursor(), &restored)
        .await
        .unwrap();
    assert!(restored == *original);
    let original_review = original.review();
    let restored_review = restored.review();
    let mut original_cells = 0;
    original_review.visit_original_cells(|table, key, column, value| {
        assert_eq!(
            restored_review
                .original_cell(table, key, column)
                .unwrap()
                .as_deref(),
            value
        );
        original_cells += 1;
    });
    let mut restored_cells = 0;
    restored_review.visit_original_cells(|_, _, _, _| restored_cells += 1);
    assert_eq!(original_cells, restored_cells);
    assert!(original_cells > 0);
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    let result = actions::apply_detailed_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &restored,
        work,
    )
    .await
    .map_err(|failure| failure.cause)
    .unwrap();
    assert!(result.replayed);
    assert!(result.items == expected.items);
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        jobs
    );
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        calls
    );
}
