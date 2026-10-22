use super::*;

async fn confirm_migration_mix(
    fixture: &super::super::super::tests::Fixture,
    request: &PlanRequest,
) -> records::ConfirmedBinding {
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_))),
        "mixed migration proposal blocked: {:?}",
        plan.items()
            .iter()
            .map(|entry| (&entry.request.item_id, entry.status))
            .collect::<Vec<_>>()
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn renew_mixed_migration_tracks_destination_delta_and_untouched_source_pair() {
    use crate::store::entities::{pin_lease_requirement, pin_lease_target, remote_pin_ledger};
    use sea_orm::QueryOrder;

    let mut fixture =
        super::super::super::tests::fixture_with_automatic("mixed-migrate-base", true).await;
    let source = pin_lease_target::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let source_contract = pin_lease_requirement::Entity::find_by_id(source.lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let coordinator = crate::pinning::coordinator::PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(crate::kubo::KuboClient::new(
            fixture.config.source_endpoint.clone(),
        )),
    )
    .unwrap();
    let worker = coordinator.start(
        crate::store::Store::new(fixture.db.clone()),
        tokio_util::sync::CancellationToken::new(),
    );
    let completion = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let now = crate::store::database_clock::database_now(&fixture.db)
                .await
                .unwrap();
            let complete =
                pin_lease_requirement::contract::completion(&fixture.db, &source.lease_id, now)
                    .await
                    .unwrap()
                    .is_some_and(|(_, complete, _)| complete);
            let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
            if complete && jobs.iter().all(|job| job.state == "done") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    assert!(
        completion.is_ok(),
        "the real Noop worker must settle the source before migration"
    );
    let source = pin_lease_target::Entity::find_by_id(source.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let source_lease = pin_lease::Entity::find_by_id(source.lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let mut raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="fixture"
        kind="noop"
        priority=1
        max_bytes=1000000
        max_pins=1000
        [[pinning.policies]]
        bucket="bucket"
        prefix=""
        trigger="request"
        provider_mode="all"
        providers=["fixture"]
        default_duration="1h"
        max_duration="2h"
        allow_decompressed=true
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="fixture"
        provider_id="fixture-provider"
        display_name="Fixture"
        backend="noop"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        api_profile="noop"
        strategy="cid"
        cleanup="retain"
    "#,
    )
    .unwrap();
    raw.kubo.rpc_url = fixture.config.source_endpoint.clone();
    let mut provider = raw.pinning.providers[0].clone();
    provider.name = "destination".into();
    raw.pinning.providers.push(provider);
    let mut identity = raw.pinning_identity.providers[0].clone();
    identity.config_name = "destination".into();
    identity.provider_id = "destination-provider".into();
    identity.scope = "destination-account".into();
    identity.storage_domain = "destination-remote".into();
    raw.pinning_identity.providers.push(identity);
    raw.pinning.policies[0].providers.push("destination".into());
    fixture.config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(fixture.config.source_node().unwrap().into())
        .unwrap();
    for provider in &fixture.config.pinning.providers {
        ledger::register_route(&fixture.db, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
    let destination = fixture
        .config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == "destination-provider")
        .unwrap();
    let destination_route = destination.route_snapshot();
    let destination_provider = destination.name.clone();

    // A distinct All lease makes both physical pairs part of Renew's frozen frontier.
    let mut birth_request = (*fixture.binding.request).clone();
    birth_request.plan_id = "mixed-migrate-renewable-birth".into();
    let Action::Create(intent) = &mut birth_request.items[0].action else {
        unreachable!()
    };
    intent.requirements.provider_ids = fixture
        .config
        .pinning
        .providers
        .iter()
        .map(|provider| provider.identity.provider_id.clone())
        .collect();
    intent.requirements.policy_revision = fixture.config.pinning.policies[0].identity.clone();
    let birth_binding = confirm_migration_mix(&fixture, &birth_request).await;
    let birth =
        super::super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &birth_binding)
            .await
            .unwrap();
    let renewable_id = &birth.items[0].receipt.intent_ids[0];
    let renewable = pin_lease::Entity::find_by_id(renewable_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let renewable_targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(renewable_id.clone()))
        .order_by_asc(pin_lease_target::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    assert_eq!(renewable_targets.len(), 2);
    let renew_target = renewable_targets
        .iter()
        .find(|target| target.provider == destination_provider)
        .unwrap();
    let mut request = birth_request.clone();
    request.plan_id = "mixed-migrate-then-renew".into();
    request.items[0].item_id = "a-migrate".into();
    request.items[0].action = Action::Migrate {
        destination: Box::new(RouteIdentity::from_original(destination_route.clone())),
    };
    let Subject::Object { target, .. } = &mut request.items[0].subject else {
        unreachable!()
    };
    *target = TargetSelection::Existing {
        target_id: source.id.clone(),
        source: None,
    };
    let mut renew = request.items[0].clone();
    renew.item_id = "b-renew".into();
    renew.action = Action::Renew {
        expires_at: renewable.expires_at.timestamp() + 1800,
    };
    let Subject::Object { target, .. } = &mut renew.subject else {
        unreachable!()
    };
    *target = TargetSelection::Existing {
        target_id: renew_target.id.clone(),
        source: None,
    };
    request.items.push(renew);
    let binding = confirm_migration_mix(&fixture, &request).await;
    assert!(matches!(&binding.confirmation.bindings[0].step,
        Some(PlannedStep::Object(step)) if step.destination != Some(RemoteDirective::NoWrite)));
    let before = binding.view().frozen_rows().unwrap();
    let source_key = (source.provider.clone(), source.cid.clone());
    let destination_key = (destination_provider.clone(), source.cid.clone());
    let source_remote = remote_pin::Entity::find_by_id(source_key.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let destination_remote = remote_pin::Entity::find_by_id(destination_key.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let source_ledger = remote_pin_ledger::Entity::find_by_id(source_key.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let destination_ledger = remote_pin_ledger::Entity::find_by_id(destination_key.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let old_jobs = pin_job::Entity::find()
        .order_by_asc(pin_job::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let committed_before = native::capture(&fixture.db).await.unwrap();
    let tx = fixture.db.begin().await.unwrap();
    super::super::super::fence::write_intent(&tx).await.unwrap();
    let scope = super::super::super::local_scope::capture(
        &tx,
        &fixture.config,
        &binding,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    super::super::super::fence::lock(
        &tx,
        before,
        &fixture.config,
        &binding,
        &scope,
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
    .unwrap();
    let checked =
        records::revalidate_confirmed_in_transaction(&tx, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
    assert_eq!(checked.len(), 2);
    assert!(
        checked
            .iter()
            .all(|item| matches!(item.outcome, RevalidationOutcome::Checked(_)))
    );
    let mut batch = super::super::super::execution::ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &binding,
        before,
    )
    .unwrap();
    let migrated = match batch.execute_entry(&binding.confirmation.bindings[0]).await {
        Ok(migrated) => migrated,
        Err(error) => {
            let actual_source = remote_pin::Entity::find_by_id(source_key.clone())
                .one(&tx)
                .await
                .unwrap()
                .unwrap();
            let actual_destination = remote_pin::Entity::find_by_id(destination_key.clone())
                .one(&tx)
                .await
                .unwrap()
                .unwrap();
            let actual_ledger = remote_pin_ledger::Entity::find_by_id(destination_key.clone())
                .one(&tx)
                .await
                .unwrap()
                .unwrap();
            let attached_id = crate::store::pinning::leases::target_id(
                &source.lease_id,
                &destination_provider,
                &source.cid,
            );
            let actual_target = pin_lease_target::Entity::find_by_id(attached_id)
                .one(&tx)
                .await
                .unwrap();
            tx.rollback().await.unwrap();
            let failure = super::super::super::apply_detailed(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &binding,
            )
            .await
            .err()
            .expect("formal apply must expose the same rejected mixed migration");
            panic!(
                "SOURCESTOP: confirmed Migrate -> Renew; Migrate step={:?}; execute_entry={error:?}; source epoch {} -> {}; destination epoch {} -> {}; destination ledger unchanged={}; actual destination={actual_target:?}; formal apply={:?}, items={:?}",
                serde_json::to_value(&binding.confirmation.bindings[0].step).unwrap(),
                source_remote.epoch,
                actual_source.epoch,
                destination_remote.epoch,
                actual_destination.epoch,
                destination_ledger == actual_ledger,
                failure.cause,
                failure
                    .items
                    .iter()
                    .map(|item| (&item.item_id, item.state))
                    .collect::<Vec<_>>()
            );
        }
    };
    let migration = migrated.action_view();
    assert_eq!(migration.receipt.intent_ids, vec![source.lease_id.clone()]);
    assert_eq!(migration.migration_points.len(), 1);
    let point = &migration.migration_points[0];
    let attached = pin_lease_target::Entity::find_by_id(point.destination_target_id.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let expected_id = crate::store::pinning::leases::target_id(
        &source.lease_id,
        &destination_provider,
        &source.cid,
    );
    assert_ne!(source.id, expected_id);
    let mut expected_targets = vec![source.id.clone(), expected_id.clone()];
    expected_targets.sort();
    assert_eq!(migration.targets.len(), 2);
    assert_eq!(migration.targets, expected_targets);
    assert_eq!(
        (
            &attached.id,
            &attached.lease_id,
            &attached.provider,
            &attached.cid,
            attached.logical_size,
            attached.state.as_str()
        ),
        (
            &expected_id,
            &source.lease_id,
            &destination_provider,
            &source.cid,
            source.logical_size,
            "waiting"
        )
    );
    assert_eq!(point.source_target_id, source.id);
    assert_eq!(point.destination_route, destination_route);
    assert_eq!(point.destination_epoch, destination_remote.epoch + 1);
    assert_eq!(
        pin_lease::Entity::find_by_id(source.lease_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        source_lease
    );
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(source.lease_id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        source_contract
    );
    assert_eq!(
        pin_lease_target::Entity::find_by_id(source.id.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        source
    );
    assert_eq!(
        remote_pin::Entity::find_by_id(source_key.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        source_remote
    );
    let after_migrate = remote_pin::Entity::find_by_id(destination_key.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_migrate.epoch, destination_remote.epoch + 1);
    let mut unchanged = after_migrate.clone();
    unchanged.epoch = destination_remote.epoch;
    unchanged.last_touched_at = destination_remote.last_touched_at;
    assert_eq!(unchanged, destination_remote);
    assert_eq!(
        remote_pin_ledger::Entity::find_by_id(source_key.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        source_ledger
    );
    assert_eq!(
        remote_pin_ledger::Entity::find_by_id(destination_key.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap(),
        destination_ledger
    );
    use super::super::super::execution::{
        self,
        facts::{Cursor, Key, OperationRead},
    };
    let operation = migrated.operation();
    let destination = Key(
        "remote_pins".into(),
        vec![destination_key.0.clone(), destination_key.1.clone()],
    );
    assert_eq!(
        native::value(operation.before(&destination).unwrap().unwrap(), "epoch"),
        Some(destination_remote.epoch.to_string().as_str())
    );
    assert_eq!(
        native::value(operation.after(&destination).unwrap().unwrap(), "epoch"),
        Some(after_migrate.epoch.to_string().as_str())
    );
    for authority in &migration.jobs {
        migrated.validate_job(&binding, authority).unwrap();
        assert_eq!(authority.directive, records::JobDirective::ReadOnlyObserve);
    }
    for (table, column, replacement) in [
        ("remote_pins", "epoch", "999"),
        ("pin_jobs", "operation", "submit"),
        ("pin_invocation_routes", "remote_epoch", "999"),
        ("pin_invocation_routes", "route", "{}"),
    ] {
        let mut invalid = operation.facts.as_ref().clone();
        let output = invalid
            .2
            .iter_mut()
            .find(|output| output.0.0 == table)
            .unwrap();
        let column = before
            .table(table)
            .unwrap()
            .columns
            .iter()
            .position(|c| c == column)
            .unwrap();
        output.2.as_mut().unwrap()[column] = Some(replacement.into());
        let cursor = Cursor::default();
        let invalid =
            OperationRead::resolve((&binding).into(), std::sync::Arc::new(invalid), &cursor)
                .unwrap();
        assert!(
            execution::validate_operation_with_cursor(
                (&binding).into(),
                migration,
                &invalid,
                &cursor
            )
            .is_err()
        );
    }
    for change in 0..5 {
        let mut invalid = migration.clone();
        match change {
            0 => invalid.jobs[0].directive = records::JobDirective::Submit,
            1 => invalid.jobs[0].remote_epoch += 1,
            2 => invalid.jobs[0].resource_id = Some("foreign-resource".into()),
            3 => invalid.jobs[0].route.scope = "foreign-account".into(),
            _ => {
                invalid.jobs[0].source.as_mut().unwrap().private_version_id =
                    "foreign-source".into()
            }
        }
        assert!(execution::validate_operation((&binding).into(), &invalid, operation).is_err());
    }
    let migration_result =
        records::write_executed_action_in_transaction(&fixture.auth, &binding, &migrated, before)
            .await
            .unwrap();
    super::super::super::seal::executed(
        &tx,
        &fixture.auth,
        &binding,
        &migrated,
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
    .unwrap();
    let prior_jobs = pin_job::Entity::find()
        .order_by_asc(pin_job::Column::Id)
        .all(&tx)
        .await
        .unwrap();
    let renewed = batch
        .execute_entry(&binding.confirmation.bindings[1])
        .await
        .unwrap();
    let renewal = renewed.action_view();
    assert_eq!(renewal.jobs.len(), 2);
    for (key, initial, ledger) in [
        (&source_key, &source_remote, &source_ledger),
        (&destination_key, &after_migrate, &destination_ledger),
    ] {
        let after = remote_pin::Entity::find_by_id(key.clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.epoch, initial.epoch + 1);
        let mut unchanged = after.clone();
        unchanged.epoch = initial.epoch;
        unchanged.last_touched_at = initial.last_touched_at;
        assert_eq!(&unchanged, initial);
        assert_eq!(
            &remote_pin_ledger::Entity::find_by_id(key.clone())
                .one(&tx)
                .await
                .unwrap()
                .unwrap(),
            ledger
        );
        let authority = renewal
            .jobs
            .iter()
            .find(|authority| authority.provider == key.0 && authority.cid == key.1)
            .unwrap();
        assert_eq!(authority.remote_epoch, after.epoch);
        assert_eq!(authority.lease_id.as_deref(), Some(renewable.id.as_str()));
        assert_eq!(authority.lease_generation, Some(renewable.generation + 1));
        assert_eq!(authority.directive, records::JobDirective::ReadOnlyObserve);
        assert!(!prior_jobs.iter().any(|job| job.id == authority.job_id));
        renewed.validate_job(&binding, authority).unwrap();
    }
    let renewal_result =
        records::write_executed_action_in_transaction(&fixture.auth, &binding, &renewed, before)
            .await
            .unwrap();
    super::super::super::seal::executed(
        &tx,
        &fixture.auth,
        &binding,
        &renewed,
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
    .unwrap();
    for job in &prior_jobs {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&tx)
                .await
                .unwrap()
                .as_ref(),
            Some(job)
        );
    }
    let renewed_lease = pin_lease::Entity::find_by_id(renewable.id.clone())
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let renewal_now = renewed_lease.last_touched_at;
    assert!(renewable.last_touched_at < renewal_now);
    let mut expected_renewed_targets = renewable_targets.clone();
    for target in &mut expected_renewed_targets {
        let snapshot_remote = if target.provider == source_key.0 && target.cid == source_key.1 {
            &source_remote
        } else {
            assert_eq!(
                (&target.provider, &target.cid),
                (&destination_key.0, &destination_key.1)
            );
            &after_migrate
        };
        target.last_touched_at = target
            .last_touched_at
            .max(renewal_now)
            .max(snapshot_remote.last_touched_at);
    }
    assert_eq!(
        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(renewable.id.clone()))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(&tx)
            .await
            .unwrap(),
        expected_renewed_targets
    );
    let written = vec![(migrated, migration_result), (renewed, renewal_result)];
    batch.validate_final(&written).await.unwrap();
    for key in [&source_key, &destination_key] {
        let expected = native::capture(&tx).await.unwrap();
        let check = tx.begin().await.unwrap();
        check
            .execute(sea_orm::Statement::from_sql_and_values(
                check.get_database_backend(),
                "UPDATE remote_pin_ledger SET route='{}' WHERE provider=$1 AND cid=$2",
                [key.0.clone().into(), key.1.clone().into()],
            ))
            .await
            .unwrap();
        let validation = batch.validate_final(&written).await;
        assert!(
            matches!(
                validation,
                Err(crate::store::pinning::management::ManagementError::InvalidEvidence)
            ),
            "{validation:?}"
        );
        check.rollback().await.unwrap();
        assert!(native::capture(&tx).await.unwrap() == expected);
    }
    batch.validate_final(&written).await.unwrap();
    tx.rollback().await.unwrap();
    assert!(native::capture(&fixture.db).await.unwrap() == committed_before);

    let applied = super::super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.replayed);
    assert_eq!(applied.items.len(), 2);
    assert_eq!(
        remote_pin::Entity::find_by_id(source_key.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        source_remote.epoch + 1
    );
    assert_eq!(
        remote_pin::Entity::find_by_id(destination_key.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        destination_remote.epoch + 2
    );
    for job in &old_jobs {
        assert_eq!(
            pin_job::Entity::find_by_id(job.id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .as_ref(),
            Some(job)
        );
    }
    let committed_jobs = pin_job::Entity::find()
        .order_by_asc(pin_job::Column::Id)
        .all(&fixture.db)
        .await
        .unwrap();
    let committed = native::capture(&fixture.db).await.unwrap();
    super::super::super::execution::proof_tests::assert_restored(&fixture, &binding, &applied)
        .await;
    assert_eq!(
        pin_job::Entity::find()
            .order_by_asc(pin_job::Column::Id)
            .all(&fixture.db)
            .await
            .unwrap(),
        committed_jobs
    );
    assert!(native::capture(&fixture.db).await.unwrap() == committed);
}

#[tokio::test]
async fn renew_mixed_one_unselected_candidate_does_not_prove_a_resource_epoch_bump() {
    let mut fixture =
        super::super::super::tests::fixture_with_automatic("mixed-one-candidate", true).await;
    let lease = pin_lease::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = crate::store::entities::pin_lease_target::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let mut raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="fixture"
        kind="noop"
        priority=2
        max_bytes=1000000
        max_pins=1000
        [[pinning.policies]]
        bucket="bucket"
        prefix=""
        trigger="request"
        provider_mode="one"
        providers=["fixture"]
        default_duration="1h"
        max_duration="2h"
        allow_decompressed=true
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="fixture"
        provider_id="fixture-provider"
        display_name="Fixture"
        backend="noop"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        api_profile="noop"
        strategy="cid"
        cleanup="retain"
    "#,
    )
    .unwrap();
    raw.kubo.rpc_url = fixture.config.source_endpoint.clone();
    let mut other = raw.pinning.providers[0].clone();
    other.name = "preferred".into();
    other.priority = 1;
    raw.pinning.providers.push(other);
    let mut other = raw.pinning_identity.providers[0].clone();
    other.config_name = "preferred".into();
    other.provider_id = "preferred-provider".into();
    other.scope = "preferred-account".into();
    other.storage_domain = "preferred-remote".into();
    raw.pinning_identity.providers.push(other);
    raw.pinning.policies[0].providers.push("preferred".into());
    fixture.config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(fixture.config.source_node().unwrap().into())
        .unwrap();
    for provider in &fixture.config.pinning.providers {
        ledger::register_route(&fixture.db, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
    let preferred = fixture
        .config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == "preferred-provider")
        .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "mixed-one-unselected".into();
    request.items[0].item_id = "a-allocate".into();
    let Action::Create(intent) = &mut request.items[0].action else {
        unreachable!()
    };
    intent.requirements.mode = ProviderMode::One;
    intent.requirements.provider_ids = fixture.config.pinning.policies[0]
        .providers
        .iter()
        .map(|name| {
            fixture
                .config
                .pinning
                .providers
                .iter()
                .find(|provider| &provider.name == name)
                .unwrap()
                .identity
                .provider_id
                .clone()
        })
        .collect();
    intent.requirements.policy_revision = fixture.config.pinning.policies[0].identity.clone();
    let mut renew = request.items[0].clone();
    renew.item_id = "b-renew".into();
    renew.action = Action::Renew {
        expires_at: lease.expires_at.timestamp() + 1800,
    };
    let Subject::Object {
        target: selection, ..
    } = &mut renew.subject
    else {
        unreachable!()
    };
    *selection = TargetSelection::Existing {
        target_id: target.id,
        source: None,
    };
    request.items.push(renew);
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_))),
        "One candidate plan: {:?}",
        plan.items()
            .iter()
            .map(|entry| (&entry.request.item_id, entry.status))
            .collect::<Vec<_>>()
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let before = binding.view().frozen_rows().unwrap();
    let initial = remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    let mut batch = super::super::super::execution::ExecutionBatch::new(
        &tx,
        &fixture.config,
        &fixture.auth,
        &binding,
        before,
    )
    .unwrap();
    let executed_allocation = batch
        .execute_entry(&binding.confirmation.bindings[0])
        .await
        .unwrap();
    let allocation = executed_allocation.action_view();
    let allocated =
        crate::store::entities::pin_lease_target::Entity::find_by_id(allocation.targets[0].clone())
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(allocated.provider, preferred.name);
    assert_eq!(allocated.state, "waiting");
    assert_eq!(allocation.targets.len(), 1);
    let after_allocation =
        remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(after_allocation.epoch, initial.epoch);
    let executed_renewal = batch
        .execute_entry(&binding.confirmation.bindings[1])
        .await
        .unwrap();
    let renewal = executed_renewal.action_view();
    let after_renewal =
        remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(after_renewal.epoch, initial.epoch + 1);
    let authority = renewal
        .jobs
        .iter()
        .find(|authority| authority.remote_epoch == after_renewal.epoch)
        .unwrap();
    let proof = executed_renewal.validate_job(&binding, authority);
    tx.rollback().await.unwrap();
    let applied =
        super::super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await;
    assert!(
        proof.is_ok() && applied.is_ok(),
        "One unselected candidate {} -> {}, Renew -> {}; stored={proof:?}, apply={:?}",
        initial.epoch,
        after_allocation.epoch,
        after_renewal.epoch,
        applied.as_ref().err()
    );
    super::super::super::execution::proof_tests::assert_restored(
        &fixture,
        &binding,
        &applied.unwrap(),
    )
    .await;
}

#[tokio::test]
async fn renew_mixed_quota_blocked_allocation_does_not_prove_a_resource_epoch_bump() {
    let mut failures = Vec::new();
    let mode = ProviderMode::All;
    for kind in ["create", "backfill", "reapply"] {
        let mut fixture =
            super::super::super::tests::fixture_with_automatic("mixed-deferred", true).await;
        let lease = pin_lease::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let target = crate::store::entities::pin_lease_target::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let provider = fixture.config.pinning.providers[0].name.clone();
        fixture.config.pinning.providers[0].limits.max_bytes = 9;
        fixture
            .config
            .pinning
            .provider_limits
            .get_mut(&provider)
            .unwrap()
            .max_bytes = 9;
        let mut request = (*fixture.binding.request).clone();
        request.plan_id = format!("mixed-all-{kind}");
        let Action::Create(mut intent) = request.items[0].action.clone() else {
            unreachable!()
        };
        intent.requirements.mode = mode;
        request.items[0].item_id = "a-allocate".into();
        request.items[0].action = match kind {
            "create" => Action::Create(intent),
            "backfill" => Action::Backfill(intent),
            _ => Action::Reapply(intent),
        };
        let mut renew = request.items[0].clone();
        renew.item_id = "b-renew".into();
        renew.action = Action::Renew {
            expires_at: lease.expires_at.timestamp() + 1800,
        };
        let Subject::Object {
            target: selection, ..
        } = &mut renew.subject
        else {
            unreachable!()
        };
        *selection = TargetSelection::Existing {
            target_id: target.id,
            source: None,
        };
        request.items.push(renew);
        let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let plan = dry_run(&request, &snapshot).unwrap();
        assert!(
            plan.items()
                .iter()
                .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_))),
            "All/{kind}: {:?}",
            plan.items()
                .iter()
                .map(|entry| (&entry.request.item_id, entry.status))
                .collect::<Vec<_>>()
        );
        let ack = plan
            .items()
            .iter()
            .filter(|entry| entry.status == PlanStatus::Candidate)
            .map(|entry| entry.request.item_id.clone())
            .collect();
        let binding = records::confirm(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            (&request, &snapshot, &plan),
            ack,
            chrono::Duration::minutes(5),
        )
        .await
        .unwrap();
        let before = binding.view().frozen_rows().unwrap();
        let initial = remote_pin::Entity::find_by_id((provider.clone(), target.cid.clone()))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let tx = fixture.db.begin().await.unwrap();
        let mut batch = super::super::super::execution::ExecutionBatch::new(
            &tx,
            &fixture.config,
            &fixture.auth,
            &binding,
            before,
        )
        .unwrap();
        let executed_allocation = batch
            .execute_entry(&binding.confirmation.bindings[0])
            .await
            .unwrap();
        let allocation = executed_allocation.action_view();
        let remote = remote_pin::Entity::find_by_id((provider.clone(), target.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.epoch, initial.epoch);
        let allocated = crate::store::entities::pin_lease_target::Entity::find_by_id(
            allocation.targets[0].clone(),
        )
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(allocated.state, "quota_blocked");
        let executed_renewal = batch
            .execute_entry(&binding.confirmation.bindings[1])
            .await
            .unwrap();
        let renewal = executed_renewal.action_view();
        let remote = remote_pin::Entity::find_by_id((provider.clone(), target.cid.clone()))
            .one(&tx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.epoch, initial.epoch + 1);
        let authority = renewal
            .jobs
            .iter()
            .find(|authority| {
                authority.directive == records::JobDirective::ReadOnlyObserve
                    && authority.remote_epoch == remote.epoch
            })
            .unwrap();
        assert_eq!(authority.lease_generation, Some(lease.generation + 1));
        let proof = executed_renewal.validate_job(&binding, authority);
        tx.rollback().await.unwrap();
        let applied =
            super::super::super::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await;
        if proof.is_err() || applied.is_err() {
            failures.push(format!("{mode:?}/{kind}: blocked allocation {} -> {}, Renew -> {}; stored={proof:?}, apply={:?}",initial.epoch,initial.epoch,remote.epoch,applied.as_ref().err()));
        } else if let Ok(applied) = applied {
            super::super::super::execution::proof_tests::assert_restored(
                &fixture, &binding, &applied,
            )
            .await;
            let authority = applied.items[1]
                .jobs
                .iter()
                .find(|authority| {
                    authority.directive == records::JobDirective::ReadOnlyObserve
                        && authority.remote_epoch == remote.epoch
                })
                .unwrap();
            let job = pin_job::Entity::find_by_id(authority.job_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            assert!(lookup(&fixture.db, &job).await.unwrap().as_ref() == Some(authority));
        }
    }
    assert!(
        failures.is_empty(),
        "valid mixed allocations must preserve exact renewal authority: {failures:?}"
    );
}
