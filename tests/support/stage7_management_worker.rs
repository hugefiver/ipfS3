#[path = "stage7_management_actions.rs"]
pub mod actions_fixture;
pub use actions_fixture::{CID, Fixture, NODE};
use ipfs_s3_gateway::{
    pinning::{identity::CleanupMode, management},
    store::pinning::ledger,
};

pub async fn psa_fixture() -> Fixture {
    psa_fixture_for("pinata").await
}

pub async fn psa_fixture_for(backend: &str) -> Fixture {
    let mut fixture = Fixture::new().await;
    let provider = &mut fixture.raw.pinning.providers[0];
    provider.kind = backend.into();
    provider.api = (backend == "filebase").then(|| "psa".into());
    provider.endpoint = Some(format!("{}/psa", fixture.server.uri()));
    provider.token_env = Some("STAGE7_W_TOKEN".into());
    let identity = &mut fixture.raw.pinning_identity.providers[0];
    identity.provider_id = format!("controlled-worker-{backend}-psa");
    identity.backend = backend.into();
    identity.api_profile = format!("{backend}-psa");
    identity.secret_ref = Some("env:STAGE7_W_TOKEN".into());
    identity.cleanup = CleanupMode::Retain;
    fixture.config =
        management::local_config::compile(&fixture.raw, |_| Some("controlled-worker-token".into()))
            .unwrap()
            .with_source_node(NODE.into())
            .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    fixture
}

pub async fn v3_fixture() -> Fixture {
    let mut fixture = psa_fixture().await;
    fixture.raw.pinning.providers[0].api = Some("v3".into());
    fixture.raw.pinning.providers[0].endpoint = Some(format!("{}/v3", fixture.server.uri()));
    fixture.raw.pinning_identity.providers[0].api_profile = "pinata-v3".into();
    fixture.config =
        management::local_config::compile(&fixture.raw, |_| Some("controlled-worker-token".into()))
            .unwrap()
            .with_source_node(NODE.into())
            .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    fixture
}

pub async fn confirm_with_ttl(
    fixture: &Fixture,
    request: &management::PlanRequest,
    ttl: chrono::Duration,
) -> ipfs_s3_gateway::store::pinning::management::records::ConfirmedBinding {
    let snapshot =
        management::db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    let plan = management::dry_run(request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == management::PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    ipfs_s3_gateway::store::pinning::management::records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (request, &snapshot, &plan),
        ack,
        ttl,
    )
    .await
    .unwrap()
}

pub fn response(request: &str, status: &str) -> serde_json::Value {
    serde_json::json!({"requestid":request,"status":status,"created":"2026-01-01T00:00:00Z","pin":{"cid":CID,"name":"controlled"},"delegates":[],"info":{}})
}

pub fn coordinator(
    fixture: &Fixture,
) -> std::sync::Arc<ipfs_s3_gateway::pinning::coordinator::PinningCoordinator> {
    ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri())),
    )
    .unwrap()
}

pub async fn wait_for_request(server: &wiremock::MockServer, verb: &str, path: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|request| request.method.as_str() == verb && request.url.path() == path)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("controlled provider request did not dispatch");
}

pub async fn readonly_fixture(
    plan: &str,
) -> (
    Fixture,
    ipfs_s3_gateway::store::pinning::management::records::ConfirmedBinding,
    ipfs_s3_gateway::store::pinning::management::records::JobAuthority,
) {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let fixture = psa_fixture().await;
    let identity = fixture.publish(plan, true).await;
    let provider = &fixture.config.pinning.providers[0];
    let target = ipfs_s3_gateway::store::entities::pin_lease_target::Entity::find()
        .filter(
            ipfs_s3_gateway::store::entities::pin_lease_target::Column::Provider.eq(&provider.name),
        )
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .confirmed_remote(&target.provider, &target.cid)
        .await;
    let request = fixture
        .existing_request(plan, identity, management::Action::Reconcile)
        .await;
    let binding = fixture.confirm(&request).await;
    let applied = ipfs_s3_gateway::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    )
    .await
    .unwrap();
    let seed=applied.items[0].jobs.iter().find(|job|job.directive==ipfs_s3_gateway::store::pinning::management::records::JobDirective::ReadOnlyObserve).unwrap().clone();
    (fixture, binding, seed)
}
