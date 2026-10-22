#[path = "support/retirement_domain.rs"]
#[allow(dead_code, unused_imports)]
mod domain;

use domain::existing::{self, Fixture};
use ipfs_s3_gateway::{
    pinning::management::{retirement::SourceCheckpoint, *},
    store::{
        entities::{
            pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
            zip_root_reference,
        },
        pinning::management::{actions, records},
        zip,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, Statement, TransactionTrait,
};
use std::{collections::BTreeSet, time::Instant};

fn distinct_cid(index: usize) -> String {
    let mut digest = [0; 32];
    digest[..8].copy_from_slice(&(index as u64).to_be_bytes());
    cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap(),
    )
    .to_string()
}

async fn checkpoint(fixture: &Fixture) -> SourceCheckpoint {
    let tx = fixture.db.begin().await.unwrap();
    let checkpoint = SourceCheckpoint::start(
        &tx,
        RouteIdentity::from_original(
            fixture
                .config
                .pinning
                .providers
                .iter()
                .find(|provider| provider.identity.provider_id == "fixture-provider")
                .unwrap()
                .route_snapshot(),
        ),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    checkpoint
}

async fn no_provider_mutations(fixture: &Fixture) {
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| {
                request.method.as_str() == "POST"
                    && matches!(request.url.path(), "/api/v0/id" | "/api/v0/cat")
            })
    );
}

async fn apply(fixture: &Fixture, binding: &records::ConfirmedBinding) -> actions::ApplyResult {
    let started = Instant::now();
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, binding)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{} apply failed with original limits: {error:?}",
                binding.request.plan_id
            )
        });
    eprintln!(
        "{} apply elapsed={:?}",
        binding.request.plan_id,
        started.elapsed()
    );
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == result.items);
    no_provider_mutations(fixture).await;
    result
}

async fn history(fixture: &Fixture) -> Vec<(i64, String)> {
    fixture
        .db
        .query_all(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT epoch,ledger FROM pin_resource_history ORDER BY provider,cid,epoch",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.try_get("", "epoch").unwrap(),
                row.try_get("", "ledger").unwrap(),
            )
        })
        .collect()
}

async fn retained_responsibilities(
    fixture: &Fixture,
    owners: usize,
) -> (remote_pin::Model, pin_lease::Model) {
    for index in 0..owners {
        existing::base::publish(
            &fixture.db,
            &fixture.config,
            &format!("shared-owner-{index}"),
            true,
        )
        .await;
    }
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .confirmed_remote(&remote.provider, &remote.cid)
        .await;
    let owner =
        existing::base::publish(&fixture.db, &fixture.config, "targetless-owner", false).await;
    let template = pin_lease::Entity::find()
        .filter(pin_lease::Column::Source.eq("automatic"))
        .order_by_asc(pin_lease::Column::Id)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let lifetime = template.expires_at - template.created_at;
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    let mut targetless = template.into_active_model();
    targetless.id = Set("targetless-lease".into());
    targetless.owner_object_id = Set(owner.object_id);
    targetless.created_at = Set(now);
    targetless.last_touched_at = Set(now);
    targetless.expires_at = Set(now + lifetime);
    targetless.generation = Set(1);
    targetless.state = Set("cancelled".into());
    let targetless = targetless.insert(&fixture.db).await.unwrap();
    let route: String = fixture
        .db
        .query_one(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT route FROM remote_pin_ledger WHERE provider=$1 AND cid=$2",
            [remote.provider.clone().into(), remote.cid.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "route")
        .unwrap();
    for epoch in [7_i64, 8] {
        let ledger = serde_json::json!({
            "provider":remote.provider,"cid":remote.cid,"route":route,
            "ownership":"application_created","effect":"confirmed",
            "first_observed_at":null,"last_observed_at":null,"remote_pinned_at":null,
            "gateway_verified_at":null,"content_verified_at":null,"first_error":null,"last_error":null,
        });
        let original = serde_json::json!({"ledger":ledger,"request_id":format!("historical-{epoch}"),"confirmed_absent":false}).to_string();
        fixture
            .db
            .execute(Statement::from_sql_and_values(
                fixture.db.get_database_backend(),
                "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES($1,$2,$3,$4)",
                vec![
                    remote.provider.clone().into(),
                    remote.cid.clone().into(),
                    epoch.into(),
                    original.into(),
                ],
            ))
            .await
            .unwrap();
    }
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE remote_pins SET epoch=9 WHERE provider=$1 AND cid=$2",
            [remote.provider.clone().into(), remote.cid.clone().into()],
        ))
        .await
        .unwrap();
    (remote, targetless)
}

#[tokio::test]
async fn retain_shared_owners_and_cancelled_targetless_lease_preserves_all_responsibilities() {
    let fixture = Fixture::new().await;
    let (remote, targetless) = retained_responsibilities(&fixture, 2).await;
    let leases = pin_lease::Entity::find()
        .order_by_asc(pin_lease::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let targets = pin_lease_target::Entity::find()
        .order_by_asc(pin_lease_target::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let originals = history(&fixture).await;
    assert_eq!(leases.len(), 3);
    assert_eq!(targets.len(), 2);
    assert_eq!(
        originals
            .iter()
            .map(|(epoch, _)| *epoch)
            .collect::<Vec<_>>(),
        vec![7, 8]
    );
    assert_eq!(targetless.source, "automatic");
    assert_eq!(targetless.state, "cancelled");
    assert!(targetless.expires_at > targetless.created_at);
    assert!(
        leases
            .iter()
            .filter(|lease| lease.id != targetless.id)
            .all(|lease| lease.owner_object_id != targetless.owner_object_id)
    );
    assert!(
        targets
            .iter()
            .all(|target| target.lease_id != targetless.id)
    );
    let binding = domain::confirm(
        &fixture,
        &domain::request(
            &fixture,
            "small-retain-responsibilities",
            RetirementSelection::Retain,
        ),
    )
    .await;
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert_eq!(capture.totals.leases, 3);
    assert_eq!(capture.totals.targets, 2);
    assert_eq!(capture.totals.active_targets, 2);
    assert_eq!(capture.totals.current_resources, 1);
    assert_eq!(capture.totals.historical_resources, 2);
    assert!(!capture.totals.attribution_complete);
    let result = apply(&fixture, &binding).await;
    assert_eq!(result.items[0].state, records::ActionState::NeedsAttention);
    assert!(result.items[0].receipt.job_ids.is_empty());
    assert_eq!(
        pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        leases
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .order_by_asc(pin_lease_target::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        targets
    );
    assert_eq!(history(&fixture).await, originals);
    assert_eq!(
        records::read_retirement(&fixture.db, &fixture.auth, &remote.provider)
            .await
            .unwrap()
            .unwrap()
            .disposition,
        records::Disposition::Retain
    );
}

#[tokio::test]
async fn retain_large_inventory_keeps_shared_owners_targetless_lease_and_all_epochs() {
    let fixture = Fixture::new().await;
    let (remote, _) = retained_responsibilities(&fixture, MAX_ITEMS + 1).await;
    domain::large_background(&fixture).await;
    let leases = pin_lease::Entity::find()
        .order_by_asc(pin_lease::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let targets = pin_lease_target::Entity::find()
        .order_by_asc(pin_lease_target::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let originals = history(&fixture).await;
    let usage = pin_provider_usage::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let binding = domain::confirm(
        &fixture,
        &domain::request(&fixture, "large-retain", RetirementSelection::Retain),
    )
    .await;
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert_eq!(capture.totals.targets, MAX_ITEMS + 1);
    assert!(capture.totals.leases >= MAX_ITEMS + 2);
    assert_eq!(capture.totals.unique_cids, 1);
    assert_eq!(capture.totals.current_resources, 1);
    assert_eq!(capture.totals.historical_resources, 2);
    assert!(!capture.totals.attribution_complete);
    let result = apply(&fixture, &binding).await;
    assert!(result.items[0].receipt.job_ids.is_empty());
    assert_eq!(result.items[0].state, records::ActionState::NeedsAttention);
    assert_eq!(
        pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        leases
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .order_by_asc(pin_lease_target::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        targets
    );
    assert_eq!(history(&fixture).await, originals);
    assert_eq!(
        pin_provider_usage::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        usage
    );
    assert_eq!(
        records::read_retirement(&fixture.db, &fixture.auth, &remote.provider)
            .await
            .unwrap()
            .unwrap()
            .disposition,
        records::Disposition::Retain
    );
}

/// Every selector points to a real committed migration of a real active target.
/// The large unrelated inventory is added only after the prerequisite actions.
async fn migrations(
    fixture: &mut Fixture,
    count: usize,
    shared: bool,
) -> (Vec<RetirementMigration>, Vec<String>) {
    let mut objects = Vec::new();
    for index in 0..count {
        let cid = if shared {
            existing::CID.into()
        } else {
            distinct_cid(index)
        };
        objects.push(
            fixture
                .publish_cid(&format!("migration-owner-{index}"), &cid, true)
                .await,
        );
    }
    let remotes = remote_pin::Entity::find().all(&fixture.db).await.unwrap();
    let source_allocation = remotes[0].provider.clone();
    for remote in remotes {
        fixture
            .confirmed_remote(&remote.provider, &remote.cid)
            .await;
    }
    fixture.add_provider("destination").await;
    let destination = RouteIdentity::from_original(
        fixture
            .config
            .pinning
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == "destination-provider")
            .unwrap()
            .route_snapshot(),
    );
    let mut selection = Vec::new();
    let mut destinations = Vec::new();
    for (index, identity) in objects.into_iter().enumerate() {
        let request = fixture
            .existing_request(
                &format!("migration-{index}"),
                identity,
                Action::Migrate {
                    destination: Box::new(destination.clone()),
                },
            )
            .await;
        let binding = fixture.confirm(&request).await;
        let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
        let point = &result.items[0].migration_points[0];
        selection.push(RetirementMigration {
            plan_id: request.plan_id,
            item_id: "object".into(),
            lease_id: point.lease_id.clone(),
            source_target_id: point.source_target_id.clone(),
            cid: point.cid.clone(),
            destination: destination.clone(),
        });
        destinations.push(point.destination_target_id.clone());
    }
    let active = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::Provider.eq(source_allocation))
        .all(&fixture.db)
        .await
        .unwrap();
    assert!(active.iter().all(|target| target.state == "pinned"));
    assert_eq!(
        active
            .iter()
            .map(|target| target.id.clone())
            .collect::<BTreeSet<_>>(),
        selection
            .iter()
            .map(|target| target.source_target_id.clone())
            .collect::<BTreeSet<_>>(),
        "retirement selection must cover the complete actual source target set",
    );
    (selection, destinations)
}

#[tokio::test]
async fn two_owner_shared_cid_retirement_confirms_complete_migration_selection() {
    let mut fixture = Fixture::new().await;
    let (targets, _) = migrations(&mut fixture, 2, true).await;
    assert_eq!(targets.len(), 2);
    assert_ne!(targets[0].lease_id, targets[1].lease_id);
    assert_ne!(targets[0].plan_id, targets[1].plan_id);
    assert_eq!(targets[0].cid, targets[1].cid);
    let selected = targets
        .iter()
        .map(|target| {
            (
                target.source_target_id.clone(),
                target.lease_id.clone(),
                target.cid.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(selected.len(), 2);
    let request = domain::request(
        &fixture,
        "two-owner-shared-cid-migrate",
        RetirementSelection::Migrate { targets },
    );
    let binding = domain::confirm(&fixture, &request).await;
    assert!(*binding.request == request);
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert!(capture.totals.attribution_complete);
    assert_eq!(capture.totals.roots, 0);
    assert_eq!(capture.totals.active_targets, 2);
    assert_eq!(capture.mutation_scope.targets.len(), 2);
    assert!(
        capture
            .mutation_scope
            .targets
            .iter()
            .all(|target| target.lease.state == LeaseState::Active)
    );
    assert_eq!(
        capture
            .mutation_scope
            .targets
            .iter()
            .map(|target| (
                target.id.clone(),
                target.lease.id.clone(),
                target.cid.clone(),
            ))
            .collect::<BTreeSet<_>>(),
        selected
    );
    assert_eq!(
        capture
            .mutation_scope
            .targets
            .iter()
            .map(|target| &target.lease.object_id)
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );
    no_provider_mutations(&fixture).await;
}

#[tokio::test]
async fn migrate_large_database_selects_every_active_shared_cid_owner() {
    let mut fixture = Fixture::new().await;
    let (targets, destinations) = migrations(&mut fixture, 2, true).await;
    assert_ne!(targets[0].lease_id, targets[1].lease_id);
    assert_eq!(targets[0].cid, targets[1].cid);
    domain::large_background(&fixture).await;
    let leases = pin_lease::Entity::find()
        .order_by_asc(pin_lease::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let jobs = pin_job::Entity::find()
        .order_by_asc(pin_job::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let binding = domain::confirm(
        &fixture,
        &domain::request(
            &fixture,
            "large-migrate",
            RetirementSelection::Migrate {
                targets: targets.clone(),
            },
        ),
    )
    .await;
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert_eq!(capture.totals.active_targets, targets.len());
    assert_eq!(
        capture
            .mutation_scope
            .targets
            .iter()
            .map(|target| target.id.clone())
            .collect::<BTreeSet<_>>(),
        targets
            .iter()
            .map(|target| target.source_target_id.clone())
            .collect::<BTreeSet<_>>()
    );
    let result = apply(&fixture, &binding).await;
    assert_eq!(
        result.items[0]
            .targets
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        destinations.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        leases
    );
    assert_eq!(
        pin_job::Entity::find()
            .order_by_asc(pin_job::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        jobs
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        usage
    );
    assert_eq!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            ipfs_s3_gateway::store::pinning::management::native::value(
                &capture.mutation_scope.targets[0].native_tuple,
                "provider",
            )
            .unwrap()
        )
        .await
        .unwrap()
        .unwrap()
        .disposition,
        records::Disposition::Migrate
    );
}

async fn cleanup_resources(fixture: &Fixture, count: usize) -> Vec<RetirementResource> {
    for index in 0..count {
        existing::base::publish_cid(
            &fixture.db,
            &fixture.config,
            &format!("cleanup-owner-{index}"),
            &distinct_cid(index),
            true,
        )
        .await;
    }
    let remotes = remote_pin::Entity::find()
        .order_by_asc(remote_pin::Column::Cid)
        .all(&fixture.db)
        .await
        .unwrap();
    for remote in &remotes {
        fixture
            .confirmed_remote(&remote.provider, &remote.cid)
            .await;
    }
    fixture.db.execute_unprepared("UPDATE pin_leases SET state='cancelled',generation=generation+1; UPDATE pin_lease_targets SET state='released'").await.unwrap();
    let route = RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot());
    remote_pin::Entity::find()
        .order_by_asc(remote_pin::Column::Cid)
        .all(&fixture.db)
        .await
        .unwrap()
        .into_iter()
        .map(|remote| {
            let cid =
                ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
            RetirementResource {
                key: route.remote_key(&cid),
                route: route.clone(),
                epoch: remote.epoch,
                resource_id: remote.request_id.unwrap(),
            }
        })
        .collect()
}

#[tokio::test]
async fn controlled_cleanup_large_database_schedules_all_safe_current_resources_without_io() {
    let fixture = Fixture::new_with_cleanup(true).await;
    let resources = cleanup_resources(&fixture, 2).await;
    domain::large_background(&fixture).await;
    let remotes = remote_pin::Entity::find()
        .order_by_asc(remote_pin::Column::Cid)
        .all(&fixture.db)
        .await
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let binding = domain::confirm(
        &fixture,
        &domain::request(
            &fixture,
            "large-cleanup",
            RetirementSelection::ControlledCleanup {
                resources: resources.clone(),
            },
        ),
    )
    .await;
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert_eq!(capture.totals.current_resources, resources.len());
    assert_eq!(capture.totals.historical_resources, 0);
    assert_eq!(capture.totals.roots, 0);
    let result = apply(&fixture, &binding).await;
    assert_eq!(result.items[0].jobs.len(), resources.len());
    assert_eq!(
        result.items[0]
            .jobs
            .iter()
            .map(|job| (
                job.cid.clone(),
                job.remote_epoch,
                job.resource_id.clone().unwrap()
            ))
            .collect::<BTreeSet<_>>(),
        resources
            .iter()
            .map(|resource| (
                resource.key.cid.clone(),
                resource.epoch,
                resource.resource_id.clone()
            ))
            .collect::<BTreeSet<_>>()
    );
    for authority in &result.items[0].jobs {
        assert_eq!(authority.directive, records::JobDirective::ExistingCleanup);
        let job = pin_job::Entity::find_by_id(authority.job_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            actions::job_authority::check_before_call(&fixture.db, &job)
                .await
                .unwrap(),
            Some(actions::job_authority::JobPermit::ExistingCleanup(_))
        ));
        assert_eq!(job.state, "pending");
    }
    no_provider_mutations(&fixture).await;
    assert_eq!(
        remote_pin::Entity::find()
            .order_by_asc(remote_pin::Column::Cid)
            .all(&fixture.db)
            .await
            .unwrap(),
        remotes
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        usage
    );
    assert_eq!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            &fixture.config.pinning.providers[0].name
        )
        .await
        .unwrap()
        .unwrap()
        .disposition,
        records::Disposition::ControlledCleanup
    );
}

#[tokio::test]
async fn root_release_large_database_changes_only_selected_logical_reference() {
    let fixture = Fixture::new().await;
    let selected = domain::root(&fixture, "selected").await;
    domain::root(&fixture, "shared").await;
    domain::large_background(&fixture).await;
    let before = zip::snapshot(&fixture.db, "selected")
        .await
        .unwrap()
        .unwrap();
    let shared = zip::snapshot(&fixture.db, "shared").await.unwrap().unwrap();
    let binding = domain::confirm(
        &fixture,
        &domain::request(
            &fixture,
            "large-root-release",
            RetirementSelection::ReleaseRootReferences {
                references: vec![selected.clone()],
            },
        ),
    )
    .await;
    let Some(Capture::Provider(capture)) = &binding.confirmation.bindings[0].capture else {
        panic!("missing full inventory")
    };
    assert_eq!(capture.totals.roots, 2);
    assert_eq!(capture.mutation_scope.root_references.len(), 1);
    let result = apply(&fixture, &binding).await;
    assert_eq!(result.items[0].root_releases, vec![selected]);
    assert!(result.items[0].receipt.job_ids.is_empty());
    let after = zip::snapshot(&fixture.db, "selected")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.batch, before.batch);
    assert_eq!(after.entries, before.entries);
    assert_eq!(after.builds, before.builds);
    let mut released = after.references;
    assert!(released[0].logical_released_at.take().is_some());
    assert_eq!(released, before.references);
    let unchanged = zip::snapshot(&fixture.db, "shared").await.unwrap().unwrap();
    assert_eq!(unchanged.batch, shared.batch);
    assert_eq!(unchanged.entries, shared.entries);
    assert_eq!(unchanged.builds, shared.builds);
    assert_eq!(unchanged.references, shared.references);
}

async fn reject_complete_oversize_selection(fixture: &Fixture, request: PlanRequest) {
    let before = checkpoint(fixture).await;
    let business = (
        pin_lease::Entity::find()
            .order_by_asc(pin_lease::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        pin_lease_target::Entity::find()
            .order_by_asc(pin_lease_target::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        pin_job::Entity::find()
            .order_by_asc(pin_job::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        remote_pin::Entity::find()
            .order_by_asc(remote_pin::Column::Cid)
            .all(&fixture.db)
            .await
            .unwrap(),
        pin_provider_usage::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        zip_root_reference::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap(),
        history(fixture).await,
    );
    assert!(matches!(
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request).await,
        Err(db_snapshot::SnapshotError::Validation(
            SafeError::LimitExceeded
        ))
    ));
    assert!(
        checkpoint(fixture).await.revisions == before.revisions,
        "request rejection must not publish any source-table write, including confirmation, retirement or receipt"
    );
    assert_eq!(
        business,
        (
            pin_lease::Entity::find()
                .order_by_asc(pin_lease::Column::Id)
                .all(&fixture.db)
                .await
                .unwrap(),
            pin_lease_target::Entity::find()
                .order_by_asc(pin_lease_target::Column::Id)
                .all(&fixture.db)
                .await
                .unwrap(),
            pin_job::Entity::find()
                .order_by_asc(pin_job::Column::Id)
                .all(&fixture.db)
                .await
                .unwrap(),
            remote_pin::Entity::find()
                .order_by_asc(remote_pin::Column::Cid)
                .all(&fixture.db)
                .await
                .unwrap(),
            pin_provider_usage::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap(),
            zip_root_reference::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap(),
            history(fixture).await,
        )
    );
    assert!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            &fixture
                .config
                .pinning
                .providers
                .iter()
                .find(|provider| provider.identity.provider_id == "fixture-provider")
                .unwrap()
                .name
        )
        .await
        .unwrap()
        .is_none()
    );
    no_provider_mutations(fixture).await;
}

#[tokio::test]
async fn full_129_migrated_active_targets_reject_without_any_prefix() {
    let mut fixture = Fixture::new().await;
    let (targets, destinations) = migrations(&mut fixture, MAX_ITEMS + 1, false).await;
    assert_eq!(targets.len(), MAX_ITEMS + 1);
    assert_eq!(
        targets
            .iter()
            .map(|target| &target.source_target_id)
            .collect::<BTreeSet<_>>()
            .len(),
        MAX_ITEMS + 1
    );
    assert_eq!(
        destinations.into_iter().collect::<BTreeSet<_>>().len(),
        MAX_ITEMS + 1
    );
    domain::large_background(&fixture).await;
    reject_complete_oversize_selection(
        &fixture,
        domain::request(
            &fixture,
            "oversize-migrate",
            RetirementSelection::Migrate { targets },
        ),
    )
    .await;
}

#[tokio::test]
async fn full_129_safe_current_cleanup_resources_reject_without_any_prefix() {
    let fixture = Fixture::new_with_cleanup(true).await;
    let resources = cleanup_resources(&fixture, MAX_ITEMS + 1).await;
    assert_eq!(resources.len(), MAX_ITEMS + 1);
    assert_eq!(
        resources
            .iter()
            .map(|resource| &resource.key.cid)
            .collect::<BTreeSet<_>>()
            .len(),
        MAX_ITEMS + 1
    );
    domain::large_background(&fixture).await;
    reject_complete_oversize_selection(
        &fixture,
        domain::request(
            &fixture,
            "oversize-cleanup",
            RetirementSelection::ControlledCleanup { resources },
        ),
    )
    .await;
}

#[tokio::test]
async fn full_129_verified_root_references_reject_without_any_prefix() {
    let fixture = Fixture::new().await;
    let mut references = Vec::new();
    for index in 0..=MAX_ITEMS {
        references.push(domain::root(&fixture, &format!("root-{index}")).await);
    }
    assert_eq!(
        references.iter().collect::<BTreeSet<_>>().len(),
        MAX_ITEMS + 1
    );
    assert!(
        zip_root_reference::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .iter()
            .all(|root| root.logical_released_at.is_none())
    );
    domain::large_background(&fixture).await;
    reject_complete_oversize_selection(
        &fixture,
        domain::request(
            &fixture,
            "oversize-root-release",
            RetirementSelection::ReleaseRootReferences { references },
        ),
    )
    .await;
}
