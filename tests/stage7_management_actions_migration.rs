#[path = "support/management_wire_v3.rs"]
mod archive;
#[path = "support/stage7_management_actions.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        self,
        entities::{pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
        pinning::management::{actions, records},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

#[tokio::test]
async fn retire_migrate_keeps_pending_destination_authority_without_granting_a_new_submit() {
    for shared in [false, true] {
        for poll in [false, true] {
            let mut fixture = support::Fixture::new().await;
            let identity = fixture.publish("pending-retirement", false).await;
            let birth = fixture
                .confirm(&fixture.create_request("pending-birth", identity.clone(), false))
                .await;
            let created = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &birth)
                .await
                .unwrap();
            let source = pin_lease_target::Entity::find_by_id(created.items[0].targets[0].clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            fixture
                .confirmed_remote(&source.provider, &source.cid)
                .await;
            fixture.add_provider("destination").await;
            if poll {
                let provider = fixture
                    .raw
                    .pinning
                    .providers
                    .iter_mut()
                    .find(|p| p.name == "destination")
                    .unwrap();
                provider.kind = "pinata".into();
                provider.endpoint = Some(format!("{}/psa", fixture.server.uri()));
                provider.token_env = Some("RETIRE_PENDING_TOKEN".into());
                let route = fixture
                    .raw
                    .pinning_identity
                    .providers
                    .iter_mut()
                    .find(|p| p.config_name == "destination")
                    .unwrap();
                route.backend = "pinata".into();
                route.api_profile = "pinata-psa".into();
                route.secret_ref = Some("env:RETIRE_PENDING_TOKEN".into());
                fixture.config =
                    local_config::compile(&fixture.raw, |_| Some("local-token".into()))
                        .unwrap()
                        .with_source_node(support::NODE.into())
                        .unwrap();
                let provider = fixture
                    .config
                    .pinning
                    .providers
                    .iter()
                    .find(|p| p.identity.provider_id == "destination-provider")
                    .unwrap();
                store::pinning::ledger::register_route(
                    &fixture.db,
                    &provider.name,
                    &provider.identity,
                )
                .await
                .unwrap();
            }
            let destination = RouteIdentity::from_original(
                fixture
                    .config
                    .pinning
                    .providers
                    .iter()
                    .find(|p| p.identity.provider_id == "destination-provider")
                    .unwrap()
                    .route_snapshot(),
            );
            let shared_birth = if shared {
                let mut policy = fixture.raw.pinning.policies[0].clone();
                policy.prefix = "other-destination".into();
                policy.providers = vec!["destination".into()];
                fixture.raw.pinning.policies.insert(0, policy);
                fixture.config =
                    local_config::compile(&fixture.raw, |_| Some("local-token".into()))
                        .unwrap()
                        .with_source_node(support::NODE.into())
                        .unwrap();
                let other = fixture.publish("other-destination-owner", false).await;
                let mut request = fixture.create_request("shared-destination-birth", other, false);
                let Action::Create(intent) = &mut request.items[0].action else {
                    unreachable!()
                };
                intent.requirements.provider_ids = vec!["destination-provider".into()];
                let Subject::Object { target, .. } = &mut request.items[0].subject else {
                    unreachable!()
                };
                *target = TargetSelection::New(destination.clone());
                let binding = fixture.confirm(&request).await;
                let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
                    .await
                    .unwrap();
                Some((binding, applied))
            } else {
                None
            };
            let migration = PlanRequest {
                plan_id: "pending-migration".into(),
                items: vec![ItemRequest {
                    item_id: "object".into(),
                    action: Action::Migrate {
                        destination: Box::new(destination.clone()),
                    },
                    subject: Subject::Object {
                        identity,
                        target: TargetSelection::Existing {
                            target_id: source.id.clone(),
                            source: None,
                        },
                    },
                }],
            };
            let migration_binding = fixture.confirm(&migration).await;
            let migrated = actions::apply(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &migration_binding,
            )
            .await
            .unwrap();
            let point = &migrated.items[0].migration_points[0];
            let seeded = shared_birth
                .as_ref()
                .map(|(_, applied)| &applied.items[0])
                .unwrap_or(&migrated.items[0]);
            let mut seed = seeded
                .jobs
                .iter()
                .find(|j| {
                    j.directive == records::JobDirective::Submit
                        && j.route == *destination.original.as_ref().unwrap()
                })
                .unwrap()
                .clone();
            if shared {
                assert_ne!(
                    seed.target_id.as_deref(),
                    Some(point.destination_target_id.as_str())
                );
                assert_ne!(seed.lease_id.as_deref(), Some(point.lease_id.as_str()));
            }
            if poll {
                use wiremock::{
                    Mock, ResponseTemplate,
                    matchers::{method, path},
                };
                Mock::given(method("POST")).and(path("/psa/pins")).respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({"requestid":"pending-destination-request","status":"queued","created":"2026-01-01T00:00:00Z","pin":{"cid":support::CID},"delegates":[],"info":{}}))).mount(&fixture.server).await;
                let coordinator =
                    ipfs_s3_gateway::pinning::coordinator::PinningCoordinator::build_with_kubo(
                        fixture.config.pinning.clone(),
                        Some(ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri())),
                    )
                    .unwrap();
                let work_binding = shared_birth
                    .as_ref()
                    .map(|(binding, _)| binding)
                    .unwrap_or(&migration_binding);
                let scope = ipfs_s3_gateway::pinning::worker::ManagementJobScope::from_applied(
                    &fixture.db,
                    &fixture.auth,
                    work_binding,
                )
                .await
                .unwrap();
                coordinator
                    .management_tick(
                        &store::Store::new(fixture.db.clone()),
                        &scope,
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await
                    .unwrap();
                let successor = pin_job::Entity::find()
                    .filter(pin_job::Column::Operation.eq("poll"))
                    .one(&fixture.db)
                    .await
                    .unwrap()
                    .unwrap();
                seed.job_id = successor.id;
                assert_eq!(
                    fixture
                        .server
                        .received_requests()
                        .await
                        .unwrap()
                        .iter()
                        .filter(|r| r.method.as_str() == "POST" && r.url.path() == "/psa/pins")
                        .count(),
                    1
                );
            }
            let pending = pin_job::Entity::find_by_id(seed.job_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pending.state, "pending");
            let request = PlanRequest {
                plan_id: "retire-pending-destination".into(),
                items: vec![ItemRequest {
                    item_id: "domain".into(),
                    action: Action::RetireWith {
                        selection: RetirementSelection::Migrate {
                            targets: vec![RetirementMigration {
                                plan_id: migration.plan_id,
                                item_id: "object".into(),
                                lease_id: point.lease_id.clone(),
                                source_target_id: source.id,
                                cid: point.cid.clone(),
                                destination,
                            }],
                        },
                    },
                    subject: Subject::Provider {
                        route: RouteIdentity::from_original(point.source_route.clone()),
                    },
                }],
            };
            let confirmed = fixture.confirm(&request).await;
            let retired = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
                .await
                .unwrap();
            assert!(retired.items[0].jobs.iter().any(|j| j == &seed));
            assert!(!matches!(
                retired.items[0].state,
                records::ActionState::RemoteComplete | records::ActionState::ReplicationComplete
            ));
            assert_eq!(
                pin_job::Entity::find_by_id(seed.job_id.clone())
                    .one(&fixture.db)
                    .await
                    .unwrap()
                    .unwrap(),
                pending
            );
            assert!(
                actions::job_authority::lookup(&fixture.db, &pending)
                    .await
                    .unwrap()
                    .as_ref()
                    == Some(&seed)
            );
            for changed in [0, 1, 2] {
                let mut wrong = pending.clone();
                match changed {
                    0 => wrong.id = "management-unrelated-job".into(),
                    1 => wrong.lease_id = Some("wrong-source".into()),
                    _ => wrong.expected_generation = wrong.expected_generation.map(|g| g + 1),
                }
                assert!(!matches!(
                    actions::job_authority::check_before_call(&fixture.db, &wrong).await,
                    Ok(Some(actions::job_authority::JobPermit::Submit(_)))
                ));
            }
            for corruption in [0, 1, 2] {
                let tx = fixture.db.begin().await.unwrap();
                if corruption == 0 {
                    let mut route = seed.route.clone();
                    route.scope = "foreign-account".into();
                    tx.execute(sea_orm::Statement::from_sql_and_values(
                        tx.get_database_backend(),
                        "UPDATE pin_invocation_routes SET route=$1 WHERE job_id=$2",
                        [
                            serde_json::to_string(&route).unwrap().into(),
                            pending.id.clone().into(),
                        ],
                    ))
                    .await
                    .unwrap();
                } else {
                    let epoch = if corruption == 1 {
                        seed.remote_epoch - 1
                    } else {
                        seed.remote_epoch + 100
                    };
                    tx.execute(sea_orm::Statement::from_sql_and_values(
                        tx.get_database_backend(),
                        "UPDATE pin_invocation_routes SET remote_epoch=$1 WHERE job_id=$2",
                        [epoch.into(), pending.id.clone().into()],
                    ))
                    .await
                    .unwrap();
                }
                assert!(!matches!(
                    actions::job_authority::check_before_call(&tx, &pending).await,
                    Ok(Some(actions::job_authority::JobPermit::Submit(_)))
                ));
                tx.rollback().await.unwrap();
            }
            let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
                .await
                .unwrap();
            assert!(replay.replayed && replay.items == retired.items);
        }
    }
}

#[tokio::test]
async fn migration_point_is_consumed_by_one_and_all_completion_without_resetting_ttl() {
    for one in [false, true] {
        let mut fixture = support::Fixture::new().await;
        fixture.add_provider("second").await;
        if one {
            fixture.raw.pinning.policies[0].provider_mode = "one".into();
            fixture.config = local_config::compile(&fixture.raw, |_| None)
                .unwrap()
                .with_source_node(support::NODE.into())
                .unwrap();
        }
        let identity = fixture.publish("migrate", false).await;
        let confirmed = fixture
            .confirm(&fixture.create_request("birth", identity.clone(), false))
            .await;
        let birth_result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
            .await
            .unwrap();
        let lease_id = &birth_result.items[0].receipt.intent_ids[0];
        let original = pin_lease::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        let contract_before =
            store::entities::pin_lease_requirement::Entity::find_by_id(lease_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap()
                .contract;
        let source = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        fixture
            .confirmed_remote(&source.provider, &source.cid)
            .await;
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
        let request = PlanRequest {
            plan_id: "migration".into(),
            items: vec![ItemRequest {
                item_id: "object".into(),
                action: Action::Migrate {
                    destination: Box::new(destination.clone()),
                },
                subject: Subject::Object {
                    identity: identity.clone(),
                    target: TargetSelection::Existing {
                        target_id: source.id.clone(),
                        source: None,
                    },
                },
            }],
        };
        let confirmed = fixture.confirm(&request).await;
        let migrated = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
            .await
            .unwrap();
        assert_eq!(migrated.items[0].migration_points.len(), 1);
        let point = &migrated.items[0].migration_points[0];
        assert_eq!(point.source_target_id, source.id);
        assert_eq!(point.source_allocation, source.provider);
        let after = pin_lease::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.created_at, original.created_at);
        assert_eq!(after.expires_at, original.expires_at);
        assert_eq!(after.generation, original.generation);
        assert_eq!(after.source, original.source);
        assert_eq!(
            store::entities::pin_lease_requirement::Entity::find_by_id(lease_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap()
                .contract,
            contract_before
        );
        let destination_target =
            pin_lease_target::Entity::find_by_id(point.destination_target_id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap();
        fixture
            .confirmed_remote(&destination_target.provider, &destination_target.cid)
            .await;
        let now = store::database_clock::database_now(&fixture.db)
            .await
            .unwrap();
        let resolved = store::entities::pin_lease_requirement::contract::frozen_candidates_for_cid(
            &fixture.db,
            lease_id,
            &source.cid,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(resolved.contains(&point.destination_allocation));
        assert!(!resolved.contains(&point.source_allocation));
        assert!(
            store::entities::pin_lease_requirement::contract::target_confirmed(
                &fixture.db,
                lease_id,
                &point.slot,
                &destination_target.cid,
                now
            )
            .await
            .unwrap()
        );
        let (_, complete, _) = store::entities::pin_lease_requirement::contract::completion(
            &fixture.db,
            lease_id,
            now,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            complete, one,
            "All must keep its other original slot; One completes per CID"
        );
        assert_eq!(
            pin_provider_usage::Entity::find_by_id(source.provider.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            10
        );
        assert_eq!(
            pin_provider_usage::Entity::find_by_id(destination_target.provider)
                .one(&fixture.db)
                .await
                .unwrap()
                .unwrap()
                .reserved_bytes,
            10
        );
        let followup = PlanRequest {
            plan_id: "public-followup".into(),
            items: vec![ItemRequest {
                item_id: "object".into(),
                action: Action::Retry,
                subject: Subject::Object {
                    identity: identity.clone(),
                    target: TargetSelection::Existing {
                        target_id: point.destination_target_id.clone(),
                        source: None,
                    },
                },
            }],
        };
        let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &followup)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
        let plan = dry_run(&followup, &snapshot).unwrap();
        let candidates = plan
            .items()
            .iter()
            .filter(|entry| entry.status == PlanStatus::Candidate)
            .map(|entry| entry.request.item_id.clone())
            .collect();
        let confirmation = plan.confirmation(candidates);
        assert!(
            snapshot
                .native_frame
                .as_ref()
                .unwrap()
                .has_private_material()
        );
        let checked = revalidate(&plan, &confirmation, &snapshot).unwrap();
        assert_eq!(checked.len(), plan.items().len());
        assert!(checked[0].id == plan.items()[0].id);
        assert!(
            records::load_confirmed_authority(
                &fixture.db,
                &fixture.auth,
                &followup,
                Default::default()
            )
            .await
            .is_err()
        );
        let original_document = archive::export(&confirmed);
        let restored = archive::restore(
            &fixture.db,
            &fixture.auth,
            &confirmed.request,
            &original_document,
        )
        .await
        .unwrap();
        let restored_review = restored.review();
        confirmed
            .review()
            .visit_original_cells(|table, key, column, value| {
                assert_eq!(
                    restored_review
                        .original_cell(table, key, column)
                        .map(|cell| cell.as_deref()),
                    Some(value)
                )
            });
        let mut forged = migrated.items[0].clone();
        forged.migration_points[0].planning_projection =
            Some(records::NamespacePlanningProjection {
                legacy_record: store::pinning::management::native::NativeKey {
                    table: "pin_legacy_migrations".into(),
                    values: vec!["client-proposal".into()],
                },
                allocation: point.destination_allocation.clone(),
                execution_route: point.destination_route.clone(),
            });
        let tx = fixture.db.begin().await.unwrap();
        let row=tx.query_one(sea_orm::Statement::from_sql_and_values(tx.get_database_backend(),"SELECT result FROM pin_management_records WHERE plan_id=$1 AND item_id='object' AND kind='action'",[confirmed.request.plan_id.clone().into()])).await.unwrap().unwrap();
        let raw: String = row.try_get("", "result").unwrap();
        let mut encoded: serde_json::Value =
            serde_json::from_str(raw.strip_prefix("S7R1\n").unwrap()).unwrap();
        encoded[0] = serde_json::to_value(&forged).unwrap();
        tx.execute(sea_orm::Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET result=$1 WHERE plan_id=$2 AND item_id='object' AND kind='action'",[format!("S7R1\n{encoded}").into(),confirmed.request.plan_id.clone().into()])).await.unwrap();
        assert!(
            records::lookup_committed(&tx, &fixture.auth, &confirmed, "object")
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        let source_route = RouteIdentity::from_original(point.source_route.clone());
        let retire = PlanRequest {
            plan_id: "retire-migrated".into(),
            items: vec![ItemRequest {
                item_id: "domain".into(),
                action: Action::RetireWith {
                    selection: RetirementSelection::Migrate {
                        targets: vec![RetirementMigration {
                            plan_id: "migration".into(),
                            item_id: "object".into(),
                            lease_id: lease_id.clone(),
                            source_target_id: source.id.clone(),
                            cid: point.cid.clone(),
                            destination,
                        }],
                    },
                },
                subject: Subject::Provider {
                    route: source_route,
                },
            }],
        };
        for wrong_selector in [false, true] {
            let mut invalid = retire.clone();
            invalid.plan_id = format!("rejected-retire-{wrong_selector}");
            let Action::RetireWith {
                selection: RetirementSelection::Migrate { targets },
            } = &mut invalid.items[0].action
            else {
                unreachable!()
            };
            if wrong_selector {
                targets[0].plan_id = "birth".into();
            } else {
                targets[0].source_target_id = point.destination_target_id.clone();
            }
            let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &invalid)
                .await
                .unwrap()
                .planning_snapshot()
                .unwrap();
            let plan = dry_run(&invalid, &snapshot).unwrap();
            if plan
                .items()
                .iter()
                .all(|entry| matches!(entry.status, PlanStatus::Ready | PlanStatus::Candidate))
            {
                let ack = plan
                    .items()
                    .iter()
                    .filter(|entry| entry.status == PlanStatus::Candidate)
                    .map(|entry| entry.request.item_id.clone())
                    .collect();
                match records::confirm(
                    &fixture.db,
                    &fixture.config,
                    &fixture.auth,
                    (&invalid, &snapshot, &plan),
                    ack,
                    chrono::Duration::minutes(2),
                )
                .await
                {
                    Ok(binding) => {
                        let before = store::pinning::management::native::capture(&fixture.db)
                            .await
                            .unwrap();
                        let error = actions::apply(
                            &fixture.db,
                            &fixture.config,
                            &fixture.auth,
                            &binding,
                        )
                        .await
                        .err()
                        .expect(
                            "an incomplete selection or non-migration selector must not retire",
                        );
                        assert!(matches!(
                            error,
                            store::pinning::management::ManagementError::Confirmation
                                | store::pinning::management::ManagementError::Changed
                                | store::pinning::management::ManagementError::InvalidEvidence
                        ));
                        assert!(
                            before
                                == store::pinning::management::native::capture(&fixture.db)
                                    .await
                                    .unwrap()
                        );
                    }
                    Err(
                        store::pinning::management::ManagementError::Confirmation
                        | store::pinning::management::ManagementError::Changed
                        | store::pinning::management::ManagementError::InvalidEvidence,
                    ) => {}
                    Err(error) => panic!("unexpected selector validation failure: {error}"),
                }
            }
        }
        let confirmed = fixture.confirm(&retire).await;
        let retirement = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
            .await
            .unwrap();
        assert_eq!(
            records::read_retirement(&fixture.db, &fixture.auth, &point.source_allocation)
                .await
                .unwrap()
                .unwrap()
                .disposition,
            records::Disposition::Migrate
        );
        assert!(
            remote_pin::Entity::find_by_id((point.source_allocation.clone(), source.cid))
                .one(&fixture.db)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            records::lookup_committed(&fixture.db, &fixture.auth, &confirmed, "domain")
                .await
                .unwrap()
                .unwrap()
                == retirement.items[0]
        );
        let tx = fixture.db.begin().await.unwrap();
        tx.execute(sea_orm::Statement::from_sql_and_values(
            tx.get_database_backend(),
            "UPDATE pin_lease_targets SET state='released' WHERE id=$1",
            [point.destination_target_id.clone().into()],
        ))
        .await
        .unwrap();
        assert!(
            records::lookup_committed(&tx, &fixture.auth, &confirmed, "domain")
                .await
                .unwrap()
                .unwrap()
                == retirement.items[0]
        );
        assert_eq!(
            actions::status::lookup(&tx, &fixture.auth, &confirmed, "domain")
                .await
                .unwrap()
                .unwrap()
                .state,
            records::ActionState::NeedsAttention
        );
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn default_retire_retains_unknown_debt_and_stops_new_allocations() {
    let fixture = support::Fixture::new().await;
    fixture.publish("unknown-debt", true).await;
    let provider = &fixture.config.pinning.providers[0];
    let route = RouteIdentity::from_original(provider.route_snapshot());
    let request = PlanRequest {
        plan_id: "retire".into(),
        items: vec![ItemRequest {
            item_id: "domain".into(),
            action: Action::Retire,
            subject: Subject::Provider { route },
        }],
    };
    let confirmed = fixture.confirm(&request).await;
    let result = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    assert_eq!(result.items[0].state, records::ActionState::NeedsAttention);
    assert!(result.items[0].receipt.job_ids.is_empty());
    let retired = records::read_retirement(&fixture.db, &fixture.auth, &provider.name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retired.disposition, records::Disposition::Retain);
    assert!(!retired.unresolved.is_empty());
    assert!(
        store::entities::pin_provider_route::Entity::find_by_id(provider.name.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .retired
    );
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(provider.name.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        10
    );
}

#[tokio::test]
async fn controlled_cleanup_is_an_exact_confirmed_existing_privilege_not_an_apply_parameter() {
    let fixture = support::Fixture::new_with_cleanup(true).await;
    fixture.publish("owned", true).await;
    let provider = &fixture.config.pinning.providers[0];
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture
        .confirmed_remote(&remote.provider, &remote.cid)
        .await;
    fixture.db.execute_unprepared("UPDATE pin_leases SET state='cancelled',generation=generation+1; UPDATE pin_lease_targets SET state='released'").await.unwrap();
    let route = RouteIdentity::from_original(provider.route_snapshot());
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let canonical =
        ipfs_s3_gateway::pinning::provider::canonical_resource_cid(&remote.cid).unwrap();
    let request = PlanRequest {
        plan_id: "cleanup".into(),
        items: vec![ItemRequest {
            item_id: "domain".into(),
            action: Action::RetireWith {
                selection: RetirementSelection::ControlledCleanup {
                    resources: vec![RetirementResource {
                        key: route.remote_key(&canonical),
                        route: route.clone(),
                        epoch: remote.epoch,
                        resource_id: remote.request_id.clone().unwrap(),
                    }],
                },
            },
            subject: Subject::Provider { route },
        }],
    };
    let confirmed = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    assert_eq!(applied.items[0].jobs.len(), 1);
    assert_eq!(
        applied.items[0].jobs[0].directive,
        records::JobDirective::ExistingCleanup
    );
    let job = pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone())
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
    assert!(
        actions::job_authority::projection_allowed(&fixture.db, &job)
            .await
            .unwrap()
    );
    assert!(
        actions::job_authority::cleanup_settlement_allowed(&fixture.db, &job)
            .await
            .unwrap()
    );
    // No DELETE is invoked in this source fixture. Intent acceptance and physical
    // deletion/accounting release remain separate surfaces for parent/C QA.
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(provider.name.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        10
    );
    assert!(
        remote_pin::Entity::find_by_id((remote.provider, remote.cid))
            .one(&fixture.db)
            .await
            .unwrap()
            .is_some()
    );
}
