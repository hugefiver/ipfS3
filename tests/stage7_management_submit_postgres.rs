//! Opt-in native PostgreSQL Submit, using a fresh parent-owned empty exclusive
//! stage7_ schema selected by IPFS_S3_STAGE7_APPLY_DATABASE_URL.
#[allow(dead_code)]
#[path = "support/stage7_management_apply_postgres.rs"]
mod support;

use futures_util::FutureExt;
use ipfs_s3_gateway::{
    kubo::KuboClient,
    pinning::{
        coordinator::PinningCoordinator,
        identity::{CanonicalResourceKey, Ownership, ProviderRouteSnapshot},
        management::db_snapshot,
        provider::{ObservedResource, ObservedResourceStatus, canonical_resource_cid},
        worker::{ManagementJobReport, ManagementJobScope, management_tick},
    },
    store::{
        Store,
        database_clock::database_now,
        entities::{
            object, object_version, pin_invocation_route, pin_job, pin_lease,
            pin_lease_requirement::contract, pin_lease_target, pin_replica_observation,
            pin_submit_observation, remote_pin,
        },
        pinning::{
            jobs, ledger,
            management::{actions, records},
        },
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::{panic::AssertUnwindSafe, sync::Arc};
use support::Fixture;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires parent-selected empty exclusive Stage7 PostgreSQL schema"]
async fn native_postgres_managed_submit_preserves_facts_and_projection() {
    let fixture = Box::pin(Fixture::new()).await;
    let result = AssertUnwindSafe(Box::pin(managed_submit(&fixture)))
        .catch_unwind()
        .await;
    fixture.close().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn tick(fixture: &Fixture, scope: &Arc<ManagementJobScope>) -> ManagementJobReport {
    let coordinator = PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let store = Store::new(fixture.db.clone());
    let scope = scope.clone();
    tokio::spawn(async move {
        Box::pin(management_tick(
            &coordinator,
            &store,
            &scope,
            &CancellationToken::new(),
        ))
        .await
        .expect("the real scoped PostgreSQL worker must finish its first Noop Submit")
    })
    .await
    .unwrap()
}

async fn managed_submit(fixture: &Fixture) {
    let identity = Box::pin(fixture.prepared_object("native-submit")).await;
    let source = object::Entity::find_by_id(identity.object_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let version = object_version::Entity::find_by_id(identity.private_version_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let request = fixture.create_request(identity.clone());
    let binding = Box::pin(fixture.confirm(&request)).await;
    fixture.auth.check(&fixture.db).await.unwrap();
    assert_eq!(binding.database_scope, fixture.auth.database_scope());
    assert_eq!(binding.database_identity, fixture.auth.database_identity());
    let calls_before = fixture.request_counts().await;
    let applied = Box::pin(actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &binding,
    ))
    .await
    .unwrap();
    assert!(!applied.replayed);
    assert_eq!(applied.items.len(), 1);
    let item = &applied.items[0];
    assert_eq!(item.jobs.len(), 1);
    let seed = &item.jobs[0];
    assert_eq!(seed.directive, records::JobDirective::Submit);
    assert!(seed.source.as_ref() == Some(&identity));
    assert!(seed.owner.as_ref() == Some(&identity));
    let job_before = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job_before.operation, "submit");
    assert_eq!(job_before.state, "pending");
    assert_eq!(job_before.submit_phase.as_deref(), Some("ready"));
    assert_eq!(job_before.expected_generation, seed.lease_generation);
    assert!(matches!(
        actions::job_authority::check_before_call(&fixture.db, &job_before).await.unwrap(),
        Some(actions::job_authority::JobPermit::Submit(authority)) if authority == *seed
    ));
    let invocation = pin_invocation_route::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let frozen: ProviderRouteSnapshot = serde_json::from_str(&invocation.route).unwrap();
    assert_eq!(frozen, seed.route);
    assert!(!ledger::submission::is_rpc(&frozen));
    let remote_before = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote_before.status, "reserved");
    assert!(remote_before.request_id.is_none());
    assert_eq!(invocation.remote_epoch, remote_before.epoch);
    assert_eq!(invocation.remote_epoch, seed.remote_epoch);
    assert!(
        jobs::submission_history(&fixture.db, &seed.job_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        pin_submit_observation::Entity::find()
            .filter(pin_submit_observation::Column::JobId.eq(&seed.job_id))
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_replica_observation::Entity::find()
            .filter(pin_replica_observation::Column::JobId.eq(&seed.job_id))
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.request_counts().await, calls_before);

    let scope = Arc::new(
        Box::pin(ManagementJobScope::from_applied(
            &fixture.db,
            &fixture.auth,
            &binding,
        ))
        .await
        .unwrap(),
    );
    // First Submit reaches the native bounded latest-RPC empty-history query
    // through record_submit_invocation before persisting call 1.
    let report = Box::pin(tick(fixture, &scope)).await;
    let history = jobs::submission_history(&fixture.db, &seed.job_id)
        .await
        .unwrap()
        .expect("first Submit must persist native invocation history");
    assert_eq!(history.api, "noop");
    assert_eq!(history.strategy, "cid");
    assert_eq!(history.submit_calls, 1);
    assert_eq!(history.recovery_queries, 0);
    assert_eq!(history.effect, "created");
    assert_eq!(history.state, "settled");
    assert!(
        history
            .correlation
            .as_ref()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(history.first_error.is_none() && history.last_error.is_none());
    assert_eq!(report.submit_observations.len(), 1);
    assert_eq!(report.replica_observations.len(), 1);
    let submit = &report.submit_observations[0];
    let replica = &report.replica_observations[0];
    assert_facts(seed, &invocation, &history, submit, replica, &source.cid);
    assert_eq!(
        pin_submit_observation::Entity::find_by_id(submit.id.clone())
            .one(&fixture.writer)
            .await
            .unwrap()
            .as_ref(),
        Some(submit)
    );
    assert_eq!(
        pin_replica_observation::Entity::find_by_id(replica.sequence)
            .one(&fixture.writer)
            .await
            .unwrap()
            .as_ref(),
        Some(replica)
    );
    assert_eq!(
        ledger::submission::latest(&fixture.db, &seed.job_id)
            .await
            .unwrap()
            .as_ref(),
        Some(submit)
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(seed.job_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        invocation
    );

    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "pinned");
    assert_eq!(remote.request_id, Some(format!("noop:{}", source.cid)));
    assert_eq!(remote.epoch, invocation.remote_epoch);
    let lease_id = seed.lease_id.as_ref().unwrap();
    let lease = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.owner_object_id, identity.object_id);
    assert_eq!(lease.state, "active");
    assert_eq!(Some(lease.generation), seed.lease_generation);
    let target = pin_lease_target::Entity::find_by_id(seed.target_id.clone().unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.lease_id, lease.id);
    assert_eq!(target.provider, seed.provider);
    assert_eq!(target.cid, seed.cid);
    assert_eq!(target.state, "pinned");
    let ledger = ledger::get(&fixture.db, &seed.provider, &seed.cid)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ledger::decode_route(&ledger).as_ref(), Some(&frozen));
    assert_eq!(ledger.effect, "confirmed");
    assert_eq!(ledger.ownership, "unknown");
    assert!(ledger.remote_pinned_at.is_some());
    assert!(ledger.gateway_verified_at.is_none() && ledger.content_verified_at.is_none());
    assert_eq!(
        contract::completion(
            &fixture.db,
            lease_id,
            database_now(&fixture.db).await.unwrap()
        )
        .await
        .unwrap(),
        Some((true, true, 1))
    );
    assert_eq!(report.items.len(), 1);
    assert_eq!(report.items[0].state, records::ActionState::RemoteComplete);
    assert!(report.items[0].receipt == item.receipt);
    assert!(report.items[0].jobs == item.jobs);
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.state, "done");
    assert_eq!(job.submit_phase.as_deref(), Some("calling"));
    assert!(job.locked_until.is_none() && job.last_error.is_none());
    assert!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Provider.eq(&seed.provider))
            .filter(pin_job::Column::Cid.eq(&seed.cid))
            .filter(pin_job::Column::Operation.eq("poll"))
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty(),
        "Noop is already pinned on Submit"
    );
    Box::pin(db_snapshot::collect_with_config(
        &fixture.db,
        &fixture.config,
        &request,
    ))
    .await
    .expect("actual non-RPC facts must remain valid snapshot evidence")
    .planning_snapshot()
    .unwrap();

    let calls_after = fixture.request_counts().await;
    assert!(
        calls_after.ids > calls_before.ids,
        "worker must recheck the source node"
    );
    assert_eq!(calls_after.cats, calls_before.cats);
    let again = Box::pin(tick(fixture, &scope)).await;
    assert_eq!(
        jobs::submission_history(&fixture.db, &seed.job_id)
            .await
            .unwrap(),
        Some(history)
    );
    assert_eq!(again.submit_observations, report.submit_observations);
    assert_eq!(again.replica_observations, report.replica_observations);
    assert_eq!(again.items[0].state, records::ActionState::RemoteComplete);
    assert_eq!(fixture.request_counts().await, calls_after);
    let remaining = pin_job::Entity::find()
        .filter(pin_job::Column::Provider.eq(&seed.provider))
        .filter(pin_job::Column::Cid.eq(&seed.cid))
        .filter(pin_job::Column::Operation.is_in(["submit", "poll"]))
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(
        remaining,
        vec![job],
        "no extra Submit or Poll after completion"
    );
    assert_eq!(
        object::Entity::find_by_id(identity.object_id)
            .one(&fixture.db)
            .await
            .unwrap(),
        Some(source)
    );
    assert_eq!(
        object_version::Entity::find_by_id(identity.private_version_id)
            .one(&fixture.db)
            .await
            .unwrap(),
        Some(version)
    );
    fixture.auth.check(&fixture.writer).await.unwrap();
    assert!(
        records::lookup_committed(
            &fixture.writer,
            &fixture.auth,
            &binding,
            &request.items[0].item_id
        )
        .await
        .unwrap()
        .is_some_and(|stored| stored == *item)
    );
    Box::pin(ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        &binding,
    ))
    .await
    .unwrap();
}

fn assert_facts(
    seed: &records::JobAuthority,
    invocation: &pin_invocation_route::Model,
    history: &jobs::history::Model,
    submit: &pin_submit_observation::Model,
    replica: &pin_replica_observation::Model,
    returned_cid: &str,
) {
    assert_eq!(submit.job_id, seed.job_id);
    assert_eq!(submit.submit_call, 1);
    assert_eq!(submit.provider, seed.provider);
    assert_eq!(submit.expected_cid, seed.cid);
    assert_eq!(submit.route, invocation.route);
    assert_eq!(submit.remote_epoch, invocation.remote_epoch);
    assert_eq!(submit.effect, "observed");
    assert_eq!(submit.outcome, "ReportedPinned");
    assert!(submit.safe_error.is_none() && !submit.needs_attention);
    let observed_at = submit.observed_at.unwrap();
    assert!(history.started_at <= submit.started_at && submit.started_at <= observed_at);
    assert!(observed_at < submit.claim_until);
    assert_eq!(replica.job_id, seed.job_id);
    assert_eq!(replica.provider, seed.provider);
    assert_eq!(replica.expected_cid, seed.cid);
    assert_eq!(replica.route, invocation.route);
    assert_eq!(replica.remote_epoch, invocation.remote_epoch);
    assert_eq!(replica.claim_until, submit.claim_until);
    assert_eq!(replica.lease_id, seed.lease_id);
    assert_eq!(replica.target_id, seed.target_id);
    assert_eq!(replica.expected_generation, seed.lease_generation);
    assert_eq!(replica.phase, "submit");
    assert_eq!(replica.outcome, "ReportedPinned");
    assert_eq!(replica.observed_at, observed_at);
    assert!(replica.safe_error.is_none() && !replica.projectable);
    let envelopes: Vec<ledger::submission::ResourceEvidence> =
        serde_json::from_str(&submit.resources).unwrap();
    let resources: Vec<ObservedResource> = serde_json::from_str(&replica.resources).unwrap();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(resources.len(), 1);
    let actual = &envelopes[0].resource;
    assert_eq!(&resources[0], actual);
    assert_eq!(actual.resource_type, seed.route.resource_type());
    assert_eq!(actual.cid, returned_cid);
    assert_eq!(actual.request_id, format!("noop:{returned_cid}"));
    assert_eq!(actual.status, ObservedResourceStatus::Reported);
    assert_eq!(actual.ownership, Ownership::Unknown);
    assert!(actual.replica_evidence.is_none());
    let key = CanonicalResourceKey {
        backend: seed.route.backend.clone(),
        scope: seed.route.scope.clone(),
        resource_type: actual.resource_type,
        resource_id: canonical_resource_cid(&actual.cid).unwrap(),
    };
    assert_eq!(key.resource_id, canonical_resource_cid(&seed.cid).unwrap());
    assert_ne!(key.resource_id, actual.request_id);
    assert_eq!(envelopes[0].key.as_ref(), Some(&key));
}
