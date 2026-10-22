use super::{complete, migration};
use crate::pinning::management::wire;
use crate::stage7_management_test_support as support;
use crate::store::{
    entities::{pin_invocation_route, pin_job, pin_lease, remote_pin},
    pinning::{
        jobs, ledger,
        management::{
            ManagementError,
            actions::{self, job_authority, points},
            admission::authority,
            native, records,
        },
        publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};
use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set, TransactionTrait};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn ordinary_job_io_keeps_its_resolver_budget_exhausted() {
    let fixture = support::Fixture::new().await;
    let now = chrono::Utc::now();
    let job = pin_job::Model {
        id: "ordinary-budget".into(),
        operation: "poll".into(),
        provider: "ordinary".into(),
        cid: "cid".into(),
        lease_id: None,
        target_id: None,
        expected_generation: None,
        expected_remote_epoch: None,
        state: "pending".into(),
        attempts: 0,
        next_attempt_at: now,
        locked_until: None,
        submit_phase: None,
        last_error: None,
        created_at: now,
        updated_at: now,
    };
    let full = records::SharedDecodeBudget::default();
    assert!(
        migration::job_io_allowed_with_work(&fixture.db, &job, records::DecodeWork::shared(&full))
            .await
            .unwrap()
    );
    let used = full.lock().unwrap().usage();
    assert!(used.0 > 0 && used.1 > 0);
    let mut limited = records::DecodeBudget::default();
    limited
        .consume(wire::MAX_WIRE_NODES - used.0 + 1, 0)
        .unwrap();
    let limited = Arc::new(Mutex::new(limited));
    for _ in 0..2 {
        assert!(matches!(
            migration::job_io_allowed_with_work(
                &fixture.db,
                &job,
                records::DecodeWork::shared(&limited)
            )
            .await,
            Err(ManagementError::Budget)
        ));
    }
}

#[tokio::test]
async fn indexed_authority_and_poll_lineage_share_the_remaining_work_budget() {
    use crate::pinning::management::*;
    use crate::store::entities::pin_lease_target;
    use sea_orm::{ColumnTrait, ConnectionTrait, QueryFilter, Statement};
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("indexed-poll-budget", false).await;
    let create = fixture
        .confirm(&fixture.create_request("indexed-poll-create", identity.clone(), false))
        .await;
    let applied = crate::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &create,
    )
    .await
    .unwrap();
    let lease_id = &applied.items[0].receipt.intent_ids[0];
    let coordinator = crate::pinning::coordinator::PinningCoordinator::build_with_kubo(
        fixture.config.pinning.clone(),
        Some(crate::kubo::KuboClient::new(fixture.server.uri())),
    )
    .unwrap();
    let scope = crate::pinning::worker::ManagementJobScope::from_applied(
        &fixture.db,
        &fixture.auth,
        &create,
    )
    .await
    .unwrap();
    let store = crate::store::Store::new(fixture.db.clone());
    // Poll the worker independently of the receipt-decoding fixture's poll stack.
    tokio::spawn(async move {
        Box::pin(coordinator.management_tick(
            &store,
            &scope,
            &tokio_util::sync::CancellationToken::new(),
        ))
        .await
    })
    .await
    .unwrap()
    .unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    assert!(
        crate::store::entities::pin_lease_requirement::contract::completion(
            &fixture.db,
            lease_id,
            now,
        )
        .await
        .unwrap()
        .is_some_and(|(_, complete, _)| complete)
    );
    let lease = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let renew = PlanRequest {
        plan_id: "indexed-poll-renew".into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action: Action::Renew {
                expires_at: lease.expires_at.timestamp() + 300,
            },
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target.id.clone(),
                    source: None,
                },
            },
        }],
    };
    let renew = fixture.confirm(&renew).await;
    crate::store::pinning::management::actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &renew,
    )
    .await
    .unwrap();
    let remote = remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let request = remote.request_id.as_deref().unwrap();
    assert!(!request.is_empty());
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    let new = jobs::poll_job(
        &target.provider,
        &target.cid,
        lease_id,
        &target.id,
        lease.generation,
        request,
        now,
    );
    let jobs::NewPinJob::Target(poll) = &new else {
        unreachable!()
    };
    let poll_id = poll.id.clone();
    jobs::insert_new_job(&fixture.db, new, now).await.unwrap();
    let poll = pin_job::Entity::find_by_id(poll_id)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert!(
        jobs::poll_names_request(&fixture.db, &poll, request)
            .await
            .unwrap()
    );
    let provider = &fixture.config.pinning.providers[0];
    let historical_cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    crate::store::pinning::quota::reserve_unique(
        &fixture.db,
        &target.provider,
        historical_cid,
        1,
        &fixture.config.pinning.provider_limits,
        now,
    )
    .await
    .unwrap();
    // An independent historical missing-route debt permits binding-only repair;
    // the selected roots' source, invocation and request facts remain unchanged.
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE remote_pin_ledger SET route=NULL WHERE provider=$1 AND cid=$2",
            [target.provider.clone().into(), historical_cid.into()],
        ))
        .await
        .unwrap();
    let declaration:migration::Declaration=serde_json::from_value(serde_json::json!({"version":1,"database_scope":"fixture","ttl_seconds":300,"maintenance":{"all_writers_stopped":true,"all_workers_drained":true},"namespaces":[{"source":target.provider,"target":target.provider,"backend":provider.identity.backend,"scope":provider.identity.scope,"resource_type":provider.identity.resource_type(),"binding_only":true,"target_route":provider.route_snapshot(),"bindings":[]}]})).unwrap();
    let declaration = complete(&fixture.db, declaration).await;
    let confirmation = migration::inventory(&fixture.db, &declaration)
        .await
        .unwrap();
    migration::apply(&fixture.db, &declaration, &confirmation)
        .await
        .unwrap();
    let public =
        authority::actions_for_job(&fixture.db, &poll, records::SharedDecodeBudget::default())
            .await
            .unwrap();
    assert!(public.len() >= 2);
    let work = Arc::new(Mutex::new(records::DecodeBudget::default()));
    work.lock().unwrap().consume(3, 7).unwrap();
    let roots = authority::actions_for_job(&fixture.db, &poll, work.clone())
        .await
        .unwrap();
    assert_eq!(roots.len(), public.len());
    let seed = applied.items[0]
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap();
    assert!(roots.iter().any(|root| {
        root.action_view()
            .jobs
            .iter()
            .any(|job| job.job_id == seed.job_id)
    }));
    let root_usage = work.lock().unwrap().usage();
    assert!(
        job_authority::lookup_with_work(&fixture.db, &poll, work.clone())
            .await
            .unwrap()
            .is_some()
    );
    let total = work.lock().unwrap().usage();
    assert!(total.1 >= serde_json::to_string(&confirmation).unwrap().len());
    assert!(
        total.0 > 3 && total.1 > 7,
        "indexed loading and lineage validation must retain the caller's budget"
    );
    assert!(total.0 > root_usage.0 && total.1 > root_usage.1);
    for root in &roots {
        let before = work.lock().unwrap().usage();
        root.binding().copy(root.record_key()).unwrap();
        let after = work.lock().unwrap().usage();
        assert!(after.0 > before.0 && after.1 > before.1);
        for job in &root.action_view().jobs {
            root.validate_job(job).unwrap();
        }
    }
    for node_limit in [true, false] {
        let mut limited = records::DecodeBudget::default();
        limited
            .consume(
                if node_limit {
                    wire::MAX_WIRE_NODES - (root_usage.0 - 3) + 1
                } else {
                    0
                },
                if node_limit {
                    0
                } else {
                    wire::MAX_WIRE_BYTES - (root_usage.1 - 7) + 1
                },
            )
            .unwrap();
        assert!(matches!(
            authority::actions_for_job(&fixture.db, &poll, Arc::new(Mutex::new(limited))).await,
            Err(ManagementError::Budget)
        ));
        let mut limited = records::DecodeBudget::default();
        limited
            .consume(
                if node_limit {
                    wire::MAX_WIRE_NODES - (total.0 - 3) + 1
                } else {
                    0
                },
                if node_limit {
                    0
                } else {
                    wire::MAX_WIRE_BYTES - (total.1 - 7) + 1
                },
            )
            .unwrap();
        let limited = Arc::new(Mutex::new(limited));
        let result = authority::actions_for_job(&fixture.db, &poll, limited.clone()).await;
        if let Err(error) = result {
            assert_eq!(error, ManagementError::Budget);
        } else {
            assert!(matches!(
                job_authority::lookup_with_work(&fixture.db, &poll, limited).await,
                Err(ManagementError::Budget)
            ));
        }
    }
}

#[tokio::test]
async fn legacy_owned_confirmation_prepays_escaped_scratch_and_output() {
    use migration::receipt::work::Work;
    use sea_orm::{ConnectionTrait, Statement};
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    let text = format!("{}\n", "a".repeat(128_000));
    let metadata = serde_json::json!({"note":text}).to_string();
    db.execute(Statement::from_sql_and_values(db.get_database_backend(),"INSERT INTO objects(id,bucket,key,cid,size,etag,metadata) VALUES ('owner','bucket','object','cid',1,'etag',$1)",[metadata.into()])).await.unwrap();
    let declaration = super::super::management_catalog_tests::declaration(&db).await;
    let confirmation = migration::inventory(&db, &declaration).await.unwrap();
    let encoded = serde_json::to_string(&confirmation).unwrap();
    let public: migration::Confirmation = serde_json::from_str(&encoded).unwrap();
    // One escaped NativeRow string alone requires a preflight scratch pass,
    // then serde scratch plus its owned output, independently of input bytes.
    let lower_bound = encoded.len() + 3 * text.len();
    let mut limited = records::DecodeBudget::default();
    limited
        .consume(0, wire::MAX_WIRE_BYTES - (lower_bound - 1))
        .unwrap();
    let limited = Arc::new(Mutex::new(limited));
    assert!(matches!(
        records::DecodeWork::shared(&limited)
            .parse::<migration::Confirmation>(&encoded, "invalid receipt"),
        Err(ManagementError::Budget)
    ));
    let work = Arc::new(Mutex::new(records::DecodeBudget::default()));
    assert_eq!(
        records::DecodeWork::shared(&work)
            .parse::<migration::Confirmation>(&encoded, "invalid receipt")
            .unwrap(),
        public
    );
    let duplicate = encoded.replacen("\"metadata\":", "\"metadata\":null,\"metadata\":", 1);
    let public: migration::Confirmation = serde_json::from_str(&duplicate).unwrap();
    let work = Arc::new(Mutex::new(records::DecodeBudget::default()));
    assert_eq!(
        records::DecodeWork::shared(&work)
            .parse::<migration::Confirmation>(&duplicate, "invalid receipt")
            .unwrap(),
        public
    );
}

#[tokio::test]
async fn live_legacy_points_jobs_and_poll_share_remaining_receipt_work() {
    let fixture = actions::tests::fixture("live-legacy-budget").await;
    let provider = &fixture.config.pinning.providers[0];
    ledger::register_route(&fixture.db, "old", &provider.identity)
        .await
        .unwrap();
    let cid = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let limits = [(
        "old".into(),
        fixture.config.pinning.provider_limits[&provider.name].clone(),
    )]
    .into();
    publication::publish_object(
        &fixture.db,
        PublicationRequest {
            object: PublicationObject::from_put(
                "legacy-owner".into(),
                "bucket",
                "legacy",
                cid.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            tags: vec![],
            policy: crate::pinning::policy::PublicationPolicy {
                tags: vec![],
                leases: vec![crate::pinning::policy::LeaseIntent {
                    source: crate::pinning::policy::LeaseSource::Automatic,
                    policy_id: fixture.config.pinning.policies[0].identity.clone(),
                    provider_mode: crate::pinning::config::ProviderMode::All,
                    providers: vec!["old".into()],
                    content_mode: crate::pinning::tags::ContentMode::Object,
                    duration: crate::pinning::config::LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 10,
            },
        },
        &limits,
    )
    .await
    .unwrap();
    let original = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    let lease = pin_lease::Entity::find_by_id(original.lease_id.clone().unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let remote = remote_pin::Entity::find_by_id((original.provider.clone(), original.cid.clone()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    crate::store::entities::pin_resource_history::Entity::insert(crate::store::entities::pin_resource_history::ActiveModel {provider:Set(original.provider.clone()),cid:Set(original.cid.clone()),epoch:Set(remote.epoch-1),ledger:Set(serde_json::json!({"confirmed_absent":true,"request_id":null,"ledger":{"route":serde_json::to_string(&provider.route_snapshot()).unwrap()}}).to_string())}).exec(&fixture.db).await.unwrap();
    // These helpers validate operational continuity, not source-grant membership.
    let seed = records::JobAuthority {
        job_id: original.id.clone(),
        directive: records::JobDirective::Submit,
        provider: original.provider.clone(),
        cid: original.cid.clone(),
        remote_epoch: remote.epoch,
        route: provider.route_snapshot(),
        resource_key: native::NativeKey {
            table: "remote_pins".into(),
            values: vec![original.provider.clone(), original.cid.clone()],
        },
        resource_id: None,
        lease_id: original.lease_id.clone(),
        lease_generation: original.expected_generation,
        target_id: original.target_id.clone(),
        owner: None,
        source: None,
        lease_source: Some(lease.source.clone()),
        started_at: Some(lease.created_at),
        expires_at: Some(lease.expires_at),
        policy_id: Some(lease.policy_id.clone()),
        provider_mode: Some(lease.provider_mode.clone()),
        content_mode: Some(lease.content_mode.clone()),
        lease_contract: None,
    };
    let resource = native::NativeKey {
        table: "remote_pins".into(),
        values: vec![seed.provider.clone(), seed.cid.clone()],
    };
    // The live point fence checks these original destination facts independently
    // of the frozen predecessor/membership validation.
    let point = records::MigrationPoint {
        table: "pin_lease_requirements".into(),
        primary_key: native::NativeKey {
            table: "pin_lease_requirements".into(),
            values: vec![lease.id.clone()],
        },
        capture_position: "candidates/old".into(),
        original_contract: frame
            .original_row("pin_lease_requirements", &[&lease.id])
            .unwrap(),
        slot: seed.provider.clone(),
        lease_id: lease.id.clone(),
        cid: crate::pinning::provider::canonical_resource_cid(&seed.cid).unwrap(),
        source_target_id: seed.target_id.clone().unwrap(),
        source_allocation: seed.provider.clone(),
        source_route: seed.route.clone(),
        source_resource_route: seed.route.clone(),
        source_epoch: seed.remote_epoch,
        source_resource_id: None,
        source_resource_key: resource.clone(),
        source_generation: lease.generation,
        source_lease: frame.row("pin_leases", &[&lease.id]).unwrap().clone(),
        predecessor: None,
        destination_allocation: seed.provider.clone(),
        destination_route: seed.route.clone(),
        destination_target_id: seed.target_id.clone().unwrap(),
        destination_epoch: seed.remote_epoch,
        destination_resource_key: resource,
        destination_resource_id: None,
        started_at: lease.created_at,
        expires_at: lease.expires_at,
        planning_projection: None,
    };
    let tx = fixture.db.begin().await.unwrap();
    let now = crate::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    let mut running = original.into_active_model();
    running.state = Set("running".into());
    running.locked_until = Set(Some(now + chrono::Duration::minutes(1)));
    running.submit_phase = Set(Some("ready".into()));
    let claimed = jobs::ClaimedPinJob {
        model: running.update(&tx).await.unwrap(),
        reclaimed: false,
        previous_state: "pending".into(),
        object_id: seed.owner.as_ref().map(|owner| owner.object_id.clone()),
    };
    assert!(matches!(
        jobs::prepare_submit_call(&tx, &claimed, now).await.unwrap(),
        jobs::SubmitCallDecision::ReadyToCall
    ));
    jobs::record_submit_invocation(
        &tx,
        &claimed,
        &seed.route.api_profile,
        &seed.route.strategy,
        now,
    )
    .await
    .unwrap();
    let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    let mut queued = remote.into_active_model();
    queued.request_id = Set(Some("legacy-budget-request".into()));
    queued.status = Set("queued".into());
    queued.update(&tx).await.unwrap();
    let new = jobs::poll_job(
        &seed.provider,
        &seed.cid,
        seed.lease_id.as_deref().unwrap(),
        seed.target_id.as_deref().unwrap(),
        seed.lease_generation.unwrap(),
        "legacy-budget-request",
        now,
    );
    let jobs::NewPinJob::Target(target) = &new else {
        unreachable!()
    };
    let poll_id = target.id.clone();
    jobs::insert_new_job(&tx, new, now).await.unwrap();
    let mut completed = claimed.model.into_active_model();
    completed.state = Set("done".into());
    completed.locked_until = Set(None);
    completed.update(&tx).await.unwrap();
    tx.commit().await.unwrap();
    let declaration:migration::Declaration=serde_json::from_value(serde_json::json!({"version":1,"database_scope":"fixture","ttl_seconds":300,"maintenance":{"all_writers_stopped":true,"all_workers_drained":true},"namespaces":[{"source":"old","target":provider.identity.allocation_key(),"backend":provider.identity.backend,"scope":provider.identity.scope,"resource_type":provider.identity.resource_type(),"binding_only":false,"target_route":provider.route_snapshot(),"bindings":[]}]})).unwrap();
    let mut declaration = complete(&fixture.db, declaration).await;
    let mut confirmation = migration::inventory(&fixture.db, &declaration)
        .await
        .unwrap();
    // Historical managed receipts retain the original target/job route in their
    // exact bindings. Seed that persisted format using its native writer, with
    // complete real before/after images; this does not mint a source grant.
    for binding in &mut declaration.namespaces[0].bindings {
        if matches!(
            binding.table.as_str(),
            "pin_jobs" | "pin_lease_targets" | "remote_pins"
        ) {
            binding.historical_route = Some(seed.route.clone());
        }
    }
    confirmation.declaration = declaration.clone();
    let tx = fixture.db.begin().await.unwrap();
    migration::mutation::write_changes(
        &tx,
        &confirmation.before,
        &confirmation.after,
        &declaration,
        false,
    )
    .await
    .unwrap();
    let receipt = migration::receipt::append(&tx, &confirmation, None)
        .await
        .unwrap();
    let transactional_work = Arc::new(Mutex::new(records::DecodeBudget::default()));
    assert!(
        migration::resolve_legacy_binding_with_work(
            &tx,
            "pin_jobs",
            &[("id".into(), seed.job_id.clone())].into(),
            &seed.provider,
            records::BindingView::from(&fixture.binding).with_work(&transactional_work)
        )
        .await
        .unwrap()
        .is_some()
    );
    tx.commit().await.unwrap();
    let job = pin_job::Entity::find_by_id(seed.job_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let poll = pin_job::Entity::find_by_id(poll_id)
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let invocation = pin_invocation_route::Entity::find_by_id(poll.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let op = crate::store::entities::pin_legacy_migration::Entity::find_by_id(receipt.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let key = [
        ("provider".into(), seed.provider.clone()),
        ("cid".into(), seed.cid.clone()),
    ]
    .into();
    let expected =
        migration::resolve_legacy_binding(&fixture.db, "remote_pins", &key, &seed.provider)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        expected.release_epochs_at_migration,
        vec![seed.remote_epoch - 1]
    );
    for operation in 0..4 {
        let work = Arc::new(Mutex::new(records::DecodeBudget::default()));
        let view = records::BindingView::from(&fixture.binding).with_work(&work);
        match operation {
            0 => {
                let (allocation, route) = points::validate_current(&fixture.db, &point, view)
                    .await
                    .unwrap();
                assert_eq!(allocation, declaration.namespaces[0].target);
                assert_eq!(route, declaration.namespaces[0].target_route);
            }
            1 => job_authority::validate_job_resolved_with_work(
                &fixture.db,
                &job,
                &seed,
                view.decode_work(),
            )
            .await
            .unwrap(),
            2 => assert!(
                job_authority::successor::original_submit_for_poll(
                    &fixture.db,
                    &poll,
                    &seed,
                    &invocation,
                    view
                )
                .await
                .unwrap()
            ),
            _ => {
                let actual = migration::resolve_legacy_binding_with_work(
                    &fixture.db,
                    "remote_pins",
                    &key,
                    &seed.provider,
                    view,
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(
                    (
                        &actual.operational_key,
                        &actual.historical_route,
                        &actual.declaration_route,
                        &actual.receipt_id,
                        actual.source_retired,
                        actual.provenance,
                        actual.historical_epoch,
                        actual.closed_at_migration,
                        &actual.release_epochs_at_migration
                    ),
                    (
                        &expected.operational_key,
                        &expected.historical_route,
                        &expected.declaration_route,
                        &expected.receipt_id,
                        expected.source_retired,
                        expected.provenance,
                        expected.historical_epoch,
                        expected.closed_at_migration,
                        &expected.release_epochs_at_migration
                    )
                );
            }
        }
        let (nodes, bytes) = work.lock().unwrap().usage();
        assert!(
            bytes >= op.confirmation.len(),
            "live resolution must account the full original confirmation before parsing"
        );
        for node_limit in [true, false] {
            for deficit in [0, 1] {
                let mut limited = records::DecodeBudget::default();
                limited
                    .consume(
                        if node_limit {
                            wire::MAX_WIRE_NODES - nodes + deficit
                        } else {
                            0
                        },
                        if node_limit {
                            0
                        } else {
                            wire::MAX_WIRE_BYTES - bytes + deficit
                        },
                    )
                    .unwrap();
                let limited = Arc::new(Mutex::new(limited));
                let view = records::BindingView::from(&fixture.binding).with_work(&limited);
                let result = match operation {
                    0 => points::validate_current(&fixture.db, &point, view)
                        .await
                        .map(|_| ()),
                    1 => {
                        job_authority::validate_job_resolved_with_work(
                            &fixture.db,
                            &job,
                            &seed,
                            view.decode_work(),
                        )
                        .await
                    }
                    2 => job_authority::successor::original_submit_for_poll(
                        &fixture.db,
                        &poll,
                        &seed,
                        &invocation,
                        view,
                    )
                    .await
                    .map(|_| ()),
                    _ => migration::resolve_legacy_binding_with_work(
                        &fixture.db,
                        "remote_pins",
                        &key,
                        &seed.provider,
                        view,
                    )
                    .await
                    .map(|_| ()),
                };
                assert_eq!(
                    result,
                    if deficit == 0 {
                        Ok(())
                    } else {
                        Err(ManagementError::Budget)
                    }
                );
            }
        }
    }
    for reversed in [false, true] {
        let tx = fixture.db.begin().await.unwrap();
        let mut other = confirmation.clone();
        if !reversed {
            other.operation_id = uuid::Uuid::new_v4().to_string();
        }
        migration::receipt::append(&tx, &other, reversed.then_some(receipt.id.as_str()))
            .await
            .unwrap();
        let work = Arc::new(Mutex::new(records::DecodeBudget::default()));
        let result = migration::resolve_legacy_binding_with_work(
            &tx,
            "remote_pins",
            &key,
            &seed.provider,
            records::BindingView::from(&fixture.binding).with_work(&work),
        )
        .await;
        let public =
            migration::resolve_legacy_binding(&tx, "remote_pins", &key, &seed.provider).await;
        if reversed {
            assert!(result.unwrap().is_none());
            assert!(public.unwrap().is_none());
        } else {
            assert!(matches!(result, Err(ManagementError::Changed)));
            assert!(public.is_err());
        }
        tx.rollback().await.unwrap();
    }
}
