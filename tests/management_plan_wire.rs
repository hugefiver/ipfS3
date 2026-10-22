//! Registered native management wire: imported data never becomes authority.
use ipfs_s3_gateway::pinning::management;

use management::{wire::*, *};
use serde_json::json;

#[path = "support/stage75_workflow.rs"]
#[allow(dead_code, unused_imports)]
mod workflow;

fn root_key(batch: &str) -> NativeKey {
    NativeKey {
        table: "zip_root_references".into(),
        values: vec![
            batch.into(),
            "1".into(),
            "1".into(),
            "node".into(),
            "hot".into(),
            "cid".into(),
        ],
    }
}

fn route(provider: &str) -> RouteIdentity {
    RouteIdentity {
        provider_id: provider.into(),
        backend: "pinata".into(),
        scope: provider.into(),
        storage_domain: provider.into(),
        api_profile: "pinata-v3".into(),
        strategy: "cid".into(),
        resource_type: ResourceType::HostedFile,
        credential_revision: 1,
        endpoint_revision: 2,
        original: None,
    }
}

fn intent() -> Intent {
    Intent {
        started_at: 100,
        expires_at: 200,
        requirements: Requirements {
            mode: ProviderMode::All,
            provider_ids: vec!["provider-a".into()],
            levels: vec![ConfirmationLevel::ContentVerified],
            min_replicas: 1,
            max_age_secs: None,
            policy_revision: "policy-1".into(),
        },
    }
}

fn fixture(action: Action) -> (PlanRequest, Snapshot) {
    let source = route("provider-a");
    let destination = route("provider-b");
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "prefix/文件".into(),
        public_version: PublicVersionId::Version("public-version-1".into()),
        private_version_id: "private-version-1".into(),
        object_id: "object-1".into(),
        cid: "bafy-test-cid".into(),
    };
    let existing = matches!(
        action,
        Action::Renew { .. }
            | Action::Cancel
            | Action::Retry
            | Action::Migrate { .. }
            | Action::Reconcile
    );
    let retiring = action == Action::Retire;
    let subject = if retiring {
        Subject::Provider {
            route: source.clone(),
        }
    } else {
        Subject::Object {
            identity: identity.clone(),
            target: if existing {
                TargetSelection::Existing {
                    target_id: "target-1".into(),
                    source: None,
                }
            } else {
                TargetSelection::New(source.clone())
            },
        }
    };
    let mut snapshot = Snapshot {
        observed_at: 100,
        objects: vec![ObjectSnapshot {
            identity,
            logical_bytes: 50,
            readable: Some(true),
            read_evidence: None,
            permitted: Some(true),
            allowed_routes: vec![source.clone(), destination.clone()],
            control_revision: Some("control-1".into()),
            config_revision: Some("config-1".into()),
            decision: Some(if matches!(action, Action::Reapply(_)) {
                DecisionState::Skipped
            } else {
                DecisionState::None
            }),
            evidence: SelectionEvidence::TrustedManifest {
                batch_id: "batch-1".into(),
                entry_path: "目录/文件".into(),
                batch_tuple: [("id".into(), Some("batch-1".into()))].into(),
                entry_tuple: [("version_row_id".into(), Some("private-version-1".into()))].into(),
            },
        }],
        routes: [source.clone(), destination.clone()]
            .into_iter()
            .map(|identity| RouteSnapshot {
                identity,
                original: None,
                available: true,
                allocation_allowed: true,
                cleanup: CleanupMode::Retain,
                exclusive_management: false,
            })
            .collect(),
        ledger_domains: [source.clone(), destination]
            .into_iter()
            .map(|route| LedgerDomainSnapshot {
                backend: route.backend,
                scope: route.scope,
                resource_type: route.resource_type,
                frontier: vec![NativeKey {
                    table: "pin_jobs".into(),
                    values: vec!["job-1".into()],
                }],
                complete: true,
            })
            .collect(),
        ..Snapshot::default()
    };
    if existing {
        snapshot.targets.push(TargetSnapshot {
            id: "target-1".into(),
            native_tuple: [("id".into(), Some("target-1".into()))].into(),
            frozen_contract: None,
            lease: LeaseSnapshot {
                id: "lease-1".into(),
                object_id: "object-1".into(),
                generation: 7,
                started_at: 90,
                expires_at: 150,
                state: LeaseState::Active,
                native_tuple: None,
            },
            cid: "bafy-test-cid".into(),
            logical_bytes: 50,
            route: Some(source.clone()),
            frozen_candidates: vec![],
            requirements: Some(intent().requirements),
            state: TargetState::MissingRequirements,
        });
    }
    if retiring {
        snapshot.inventories.push(ProviderInventory {
            provider_id: source.provider_id,
            frontier: vec![NativeKey {
                table: "pin_jobs".into(),
                values: vec!["job-1".into()],
            }],
            complete: true,
            allocation_stopped: false,
            attribution_complete: true,
            leases: vec![],
            root_reference_ids: vec![root_key("root-reference-1")],
        });
    }
    (
        PlanRequest {
            plan_id: "maintenance-1".into(),
            items: vec![ItemRequest {
                item_id: "file-1".into(),
                action,
                subject,
            }],
        },
        snapshot,
    )
}

#[tokio::test]
async fn provider_action_round_trips_sealed_inventory_and_confirmation_with_v3() {
    use ipfs_s3_gateway::store::pinning::management::records;
    let fixture = workflow::existing::psa_fixture().await;
    let request = PlanRequest {
        plan_id: "wire-retirement".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::RetireWith {
                selection: RetirementSelection::Retain,
            },
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = retirement::PreparedPlanning::collect(
        &fixture.db,
        &request,
        &snapshot,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.plan.items()[0].status, PlanStatus::Ready);
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &prepared.plan),
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    prepared.close().await.unwrap();
    let bytes = workflow::export_global(&fixture, &binding).await;
    let imported = v3::archive_cursor::stage_import(
        std::io::Cursor::new(&bytes),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    let restored = records::load_confirmed_authority(
        &fixture.db,
        &fixture.auth,
        &request,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    v3::matching::confirmed_database(&fixture.db, imported.into_cursor(), &restored)
        .await
        .unwrap();
    assert!(restored == binding);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[test]
fn duplicate_requirements_routes_candidates_and_receipts_are_not_sets_by_coercion() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    for duplicate_level in [false, true] {
        let mut changed = request.clone();
        let Action::Create(intent) = &mut changed.items[0].action else {
            unreachable!()
        };
        if duplicate_level {
            intent
                .requirements
                .levels
                .push(ConfirmationLevel::ContentVerified);
        } else {
            intent.requirements.provider_ids.push("provider-a".into());
        }
        assert_eq!(
            validate_request(&changed),
            Err(SafeError::DuplicateIdentity)
        );
    }
    let mut duplicate = snapshot;
    let route = duplicate.objects[0].allowed_routes[0].clone();
    duplicate.objects[0].allowed_routes.push(route);
    assert_eq!(
        dry_run(&request, &duplicate).err(),
        Some(SafeError::DuplicateIdentity)
    );
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::LegacyZipCandidate;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(
        revalidate(
            &plan,
            &plan.confirmation(vec!["file-1".into(); 2]),
            &snapshot
        )
        .err(),
        Some(SafeError::ConfirmationMismatch)
    );
    let (request, mut snapshot) = fixture(Action::Cancel);
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.applied = vec![
        AppliedItem {
            binding: plan.items()[0].clone(),
        };
        2
    ];
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::DuplicateIdentity)
    );
}

#[test]
fn unconfirmed_candidate_and_blocked_plan_remain_non_executable() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::LegacyZipCandidate;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(
        revalidate(&plan, &plan.confirmation(vec![]), &snapshot).err(),
        Some(SafeError::CandidateConfirmationRequired)
    );
    assert!(revalidate(&plan, &plan.confirmation(vec!["file-1".into()]), &snapshot).is_ok());
    snapshot.objects[0].permitted = Some(false);
    let blocked = dry_run(&request, &snapshot).unwrap();
    assert_eq!(
        revalidate(
            &blocked,
            &blocked.confirmation(vec!["file-1".into()]),
            &snapshot
        )
        .err(),
        Some(SafeError::ConfirmationMismatch)
    );
}

#[test]
fn management_summary_quotes_full_all_and_one_candidate_universe() {
    for mode in [ProviderMode::All, ProviderMode::One] {
        let mut intent = intent();
        intent.requirements.mode = mode;
        intent.requirements.provider_ids.push("provider-b".into());
        let (mut request, mut snapshot) = fixture(Action::Create(intent));
        let mut second = snapshot.objects[0].clone();
        second.identity.object_id = "object-2".into();
        second.identity.private_version_id = "private-version-2".into();
        let mut item = request.items[0].clone();
        item.item_id = "file-2".into();
        let Subject::Object { identity, .. } = &mut item.subject else {
            panic!("object");
        };
        *identity = second.identity.clone();
        snapshot.objects.push(second);
        request.items.push(item);
        let plan = dry_run(&request, &snapshot).unwrap();
        assert!(
            plan.items()
                .iter()
                .all(|entry| entry.status == PlanStatus::Ready)
        );
        assert_eq!(plan.summary().logical_targets, 4);
        assert_eq!(plan.summary().logical_intent_bytes, 200);
        assert_eq!(plan.summary().possible_new_transfers, 2);
        assert_eq!(plan.summary().unique_resources, 2);
        snapshot.routes[1].available = false;
        snapshot.routes[1].allocation_allowed = false;
        let unavailable = dry_run(&request, &snapshot).unwrap();
        assert_eq!(unavailable.summary().logical_targets, 4);
        assert_eq!(
            unavailable.summary().possible_new_transfers,
            if mode == ProviderMode::One { 1 } else { 0 }
        );
        for entry in unavailable.items() {
            let Some(Capture::Object(capture)) = &entry.capture else {
                panic!("capture");
            };
            assert_eq!(capture.requirement_routes.len(), 2);
        }
    }
}

#[test]
fn management_root_release_selection_is_additive_and_exact_not_default_retire() {
    let action = json!({"retire_with": {"selection": {"release_root_references": {"references": [root_key("batch-1")]}}}});
    let decoded: Action = serde_json::from_value(action.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), action);
    assert!(serde_json::from_value::<Action>(json!("retire")).unwrap() == Action::Retire);
}

#[test]
fn management_same_resource_migration_preserves_slot_and_ttl_without_old_write_route() {
    let mut destination = route("provider-a");
    destination.strategy = "upload".into();
    let (request, mut snapshot) = fixture(Action::Migrate {
        destination: Box::new(destination.clone()),
    });
    snapshot.objects[0].allowed_routes.push(destination.clone());
    let mut state = snapshot.routes[0].clone();
    state.identity = destination;
    snapshot.routes.push(state);
    snapshot.routes[0].available = false;
    snapshot.routes[0].allocation_allowed = false;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    let Some(PlannedStep::Object(step)) = &plan.items()[0].step else {
        panic!("migration");
    };
    assert_eq!(step.source, RemoteDirective::NoWrite);
    assert_eq!(step.destination, Some(RemoteDirective::SubmitWithFence));
    assert!(step.preserve_source_responsibility);
    assert_eq!(plan.summary().logical_targets, 1);
    assert_eq!(plan.summary().logical_intent_bytes, 50);
    assert_eq!(plan.summary().possible_new_transfers, 1);
    let Some(Capture::Object(capture)) = &plan.items()[0].capture else {
        panic!("capture");
    };
    assert!(capture.target == Some(snapshot.targets[0].clone()));
}

#[test]
fn management_migration_cannot_merge_frozen_slots_or_lower_cluster_replication_contract() {
    let (request, mut snapshot) = fixture(Action::Migrate {
        destination: Box::new(route("provider-b")),
    });
    snapshot.targets[0]
        .requirements
        .as_mut()
        .unwrap()
        .provider_ids
        .push("provider-b".into());
    snapshot.targets[0].frozen_candidates = ["provider-a", "provider-b"]
        .into_iter()
        .map(|provider| FrozenCandidate {
            slot: provider.into(),
            allocation: provider.into(),
            route: Some(route(provider)),
            receipt_id: None,
        })
        .collect();
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::InvalidActionScope)
    );

    for (min_replicas, max_age_secs, ready) in [(2, 120, true), (1, 120, false), (2, 121, false)] {
        let original = |provider: &str, min_replicas, max_age_secs| {
            ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot {
                provider_id: provider.into(),
                backend: "cluster".into(),
                scope: provider.into(),
                storage_domain: provider.into(),
                credential_revision: 1,
                endpoint_revision: 1,
                secret_ref: None,
                api_profile: "cluster".into(),
                strategy: "cid".into(),
                cleanup: ipfs_s3_gateway::pinning::identity::CleanupMode::Retain,
                confirmation: Some(
                    ipfs_s3_gateway::pinning::identity::ConfirmationRequirement::ClusterReplicas {
                        min_replicas,
                        max_age_secs,
                    },
                ),
            }
        };
        let source = RouteIdentity::from_original(original("provider-a", 2, 120));
        let destination =
            RouteIdentity::from_original(original("provider-b", min_replicas, max_age_secs));
        let (request, mut snapshot) = fixture(Action::Migrate {
            destination: Box::new(destination.clone()),
        });
        snapshot.objects[0].allowed_routes = vec![source.clone(), destination.clone()];
        snapshot.targets[0].route = Some(source.clone());
        let requirements = snapshot.targets[0].requirements.as_mut().unwrap();
        requirements.min_replicas = 2;
        requirements.max_age_secs = Some(120);
        requirements.levels = vec![ConfirmationLevel::ReplicationComplete];
        for (state, identity) in snapshot.routes.iter_mut().zip([source, destination]) {
            state.original = identity.original.clone();
            state.identity = identity;
        }
        snapshot.ledger_domains = snapshot
            .routes
            .iter()
            .map(|route| LedgerDomainSnapshot {
                backend: route.identity.backend.clone(),
                scope: route.identity.scope.clone(),
                resource_type: route.identity.resource_type,
                frontier: vec![],
                complete: true,
            })
            .collect();
        assert_eq!(
            dry_run(&request, &snapshot).unwrap().items()[0].status,
            if ready {
                PlanStatus::Ready
            } else {
                PlanStatus::Blocked(SafeError::SafetyProhibition)
            }
        );
    }
}
