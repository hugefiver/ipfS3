use super::*;

fn route() -> RouteIdentity {
    RouteIdentity {
        provider_id: "provider-a".into(),
        backend: "pinata".into(),
        scope: "account-a".into(),
        storage_domain: "remote-a".into(),
        api_profile: "pinata-v3".into(),
        strategy: "cid".into(),
        resource_type: ResourceType::HostedFile,
        credential_revision: 1,
        endpoint_revision: 2,
        original: None,
    }
}

fn object() -> ObjectSnapshot {
    ObjectSnapshot {
        identity: ExactObject {
            bucket: "bucket".into(),
            key: "prefix/file".into(),
            public_version: PublicVersionId::Null,
            private_version_id: "version-row-1".into(),
            object_id: "object-1".into(),
            cid: "bafy-test-cid".into(),
        },
        logical_bytes: 50,
        readable: Some(true),
        read_evidence: None,
        permitted: Some(true),
        allowed_routes: vec![route()],
        control_revision: Some("control-1".into()),
        config_revision: Some("config-1".into()),
        decision: Some(DecisionState::None),
        evidence: SelectionEvidence::ExplicitObject,
    }
}

fn requirements() -> Requirements {
    Requirements {
        mode: ProviderMode::All,
        provider_ids: vec!["provider-a".into()],
        levels: vec![ConfirmationLevel::ContentVerified],
        min_replicas: 1,
        max_age_secs: None,
        policy_revision: "policy-1".into(),
    }
}

fn fixture(action: Action) -> (PlanRequest, Snapshot) {
    let object = object();
    let request = PlanRequest {
        plan_id: "maintenance-1".into(),
        items: vec![ItemRequest {
            item_id: "file-1".into(),
            action,
            subject: Subject::Object {
                identity: object.identity.clone(),
                target: TargetSelection::New(route()),
            },
        }],
    };
    let snapshot = Snapshot {
        observed_at: 100,
        objects: vec![object],
        routes: vec![RouteSnapshot {
            identity: route(),
            original: None,
            available: true,
            allocation_allowed: true,
            cleanup: CleanupMode::Retain,
            exclusive_management: false,
        }],
        ledger_domains: vec![LedgerDomainSnapshot {
            backend: route().backend,
            scope: route().scope,
            resource_type: route().resource_type,
            frontier: vec![native_key("pin_jobs", "job-1")],
            complete: true,
        }],
        ..Snapshot::default()
    };
    (request, snapshot)
}

fn intent() -> Intent {
    Intent {
        started_at: 100,
        expires_at: 200,
        requirements: requirements(),
    }
}

fn confirmation(plan: &DryRunPlan) -> Confirmation {
    plan.confirmation(vec![])
}

fn native_key(table: &str,id: &str)->NativeKey { NativeKey {table:table.into(),values:vec![id.into()]} }
fn target_tuple()->NativeRow {
    [("id","target-1"),("lease_id","lease-1"),("cid","bafy-test-cid"),("logical_size","50"),("provider","provider-a"),("state","waiting"),("created_at","2026-10-12T00:00:00.000000001Z"),("last_touched_at","2026-10-12T00:00:00.000000001Z")].into_iter().map(|(k,v)|(k.into(),Some(v.into()))).collect()
}

#[test]
fn current_key_never_substitutes_captured_null_version() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.objects[0].identity.private_version_id = "version-row-2".into();
    snapshot.objects[0].identity.object_id = "object-2".into();
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn old_zip_candidate_needs_separate_explicit_confirmation() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::LegacyZipCandidate;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Candidate);
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::CandidateConfirmationRequired)
    );
    let mut confirmed = confirmation(&plan);
    confirmed.candidate_item_ids.push("file-1".into());
    assert!(revalidate(&plan, &confirmed, &snapshot).is_ok());
}

#[test]
fn stable_id_is_not_a_replay_proof() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(
        plan.items()[0].id.as_str(),
        "management-v1:maintenance-1:file-1"
    );
    let again = dry_run(&request, &snapshot).unwrap();
    assert!(plan == again);
    let mut changed = snapshot.clone();
    let mut wrong_binding = plan.items()[0].clone();
    wrong_binding.request.action = Action::Backfill(intent());
    changed.applied.push(AppliedItem {
        binding: wrong_binding,
    });
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &changed).err(),
        Some(SafeError::IdempotencyConflict)
    );
}

fn existing(action: Action, effect: SideEffect) -> (PlanRequest, Snapshot) {
    let (mut request, mut snapshot) = fixture(action);
    if let Subject::Object { target, .. } = &mut request.items[0].subject {
        *target = TargetSelection::Existing {
            target_id: "target-1".into(),
            source: None,
        };
    }
    snapshot.targets.push(TargetSnapshot {
        id: "target-1".into(),
        native_tuple: target_tuple(),
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
        cid: object().identity.cid,
        logical_bytes: 50,
        route: Some(route()),
        frozen_candidates: vec![],
        requirements: Some(requirements()),
        state: TargetState::MissingRequirements,
    });
    snapshot.remotes.push(RemoteSnapshot {
        key: route().remote_key(&object().identity.cid),
        route: route(),
        resource_id: if matches!(effect, SideEffect::NotSubmitted | SideEffect::Rejected) {
            None
        } else {
            Some("resource-1".into())
        },
        epoch: 11,
        effect,
        ownership: Ownership::Unknown,
        application_references: Some(1),
        inflight_operations: Some(0),
        retained: !matches!(effect, SideEffect::NotSubmitted | SideEffect::Rejected),
        cleanup_pending: false,
        accounted_bytes: Some(50),
        confirmations: vec![ConfirmationLevel::Accepted],
        confirmed_replicas: Some(0),
        observation_current: Some(false),
        replica_started_at: None,
    });
    (request, snapshot)
}

fn source(plan: &DryRunPlan) -> RemoteDirective {
    match plan.items()[0].step.as_ref().unwrap() {
        PlannedStep::Object(step) => step.source,
        _ => panic!("expected object step"),
    }
}

#[test]
fn exact_native_lease_expiry_and_directory_byte_gaps_are_not_second_or_bool_authority() {
    let (request,mut snapshot)=existing(Action::Renew {expires_at:200},SideEffect::Present);
    snapshot.targets[0].lease.expires_at=100;
    snapshot.targets[0].lease.native_tuple=Some([("created_at".into(),Some("1970-01-01T00:01:30Z".into())),("expires_at".into(),Some("1970-01-01T00:01:40.000000999Z".into()))].into());
    snapshot.database_now=Some(chrono::DateTime::parse_from_rfc3339("1970-01-01T00:01:40.000000998Z").unwrap().with_timezone(&chrono::Utc));
    assert_eq!(dry_run(&request,&snapshot).unwrap().items()[0].status,PlanStatus::Ready);
    snapshot.database_now=Some(chrono::DateTime::parse_from_rfc3339("1970-01-01T00:01:40.000000999Z").unwrap().with_timezone(&chrono::Utc));
    assert_eq!(dry_run(&request,&snapshot).unwrap().items()[0].status,PlanStatus::Blocked(SafeError::LeaseNotActive));
    let (request,mut snapshot)=fixture(Action::Create(intent()));
    snapshot.objects[0].read_evidence=Some(ReadEvidence {identity:snapshot.objects[0].identity.clone(),node_identity:"node".into(),stored_bytes:None,proof:super::evidence::ReadProof::CompleteLocalDirectory,observed_at:chrono::DateTime::from_timestamp(90,0).unwrap(),expires_at:chrono::DateTime::from_timestamp(150,0).unwrap(),idle_timeout_millis:1000});
    assert_eq!(dry_run(&request,&snapshot).unwrap().items()[0].status,PlanStatus::Blocked(SafeError::StoredByteProofUnavailable));
}

#[test]
fn unknown_retry_reconciles_and_keeps_full_occupancy() {
    let (request, snapshot) = existing(Action::Retry, SideEffect::Unknown);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::ReconcileHistorical);
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    assert_eq!(plan.summary().possible_new_transfers, 0);
    assert_eq!(plan.summary().physical_release_bytes, 0);
    assert_eq!(plan.summary().unknown_resources, 1);
}

#[test]
fn only_known_not_submitted_or_rejected_can_schedule_retry_post() {
    for effect in [
        SideEffect::NotSubmitted,
        SideEffect::Rejected,
        SideEffect::Unknown,
        SideEffect::InFlight,
        SideEffect::Present,
    ] {
        let (request, snapshot) = existing(Action::Retry, effect);
        let plan = dry_run(&request, &snapshot).unwrap();
        let expected = match effect {
            SideEffect::NotSubmitted | SideEffect::Rejected => RemoteDirective::SubmitWithFence,
            SideEffect::Unknown => RemoteDirective::ReconcileHistorical,
            SideEffect::InFlight => RemoteDirective::WaitForInflight,
            SideEffect::Present => RemoteDirective::ObserveExisting,
        };
        assert_eq!(source(&plan), expected);
    }
}

#[test]
fn inflight_and_cleanup_prevent_post_even_with_prior_present_resource() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    snapshot.remotes[0].cleanup_pending = true;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::ReconcileHistorical);
    assert_eq!(plan.summary().cleanup_pending_resources, 1);
    snapshot.remotes[0].inflight_operations = Some(1);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::WaitForInflight);
    assert_eq!(plan.summary().inflight_resources, 1);
}

#[test]
fn confirmed_success_never_reuploads_and_all_is_not_lowered() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    snapshot.targets[0].state = TargetState::Confirmed;
    snapshot.targets[0]
        .requirements
        .as_mut().unwrap()
        .provider_ids
        .push("provider-b".into());
    let mut other=snapshot.routes[0].clone();other.identity.provider_id="provider-b".into();other.available=false;other.allocation_allowed=false;snapshot.routes.push(other);
    snapshot.remotes[0].confirmations = vec![ConfirmationLevel::ContentVerified];
    snapshot.remotes[0].confirmed_replicas = Some(1);
    snapshot.remotes[0].observation_current = Some(true);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::ReuseConfirmed);
    match plan.items()[0].capture.as_ref().unwrap() {
        Capture::Object(capture) => {
            let requirements = capture.target.as_ref().unwrap().requirements.as_ref().unwrap();
            assert_eq!(requirements.mode, ProviderMode::All);
            assert_eq!(requirements.provider_ids.len(), 2);
        }
        _ => panic!("expected exact target"),
    }
    assert_eq!(plan.summary().possible_new_transfers, 0);
    snapshot.remotes[0].observation_current = Some(false);
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::ObserveExisting
    );
}

#[test]
fn accepted_is_not_content_verified_or_replication_complete() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    snapshot.remotes[0].observation_current = Some(true);
    snapshot.remotes[0].confirmed_replicas = Some(1);
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::ObserveExisting
    );
    snapshot.targets[0].requirements.as_mut().unwrap().min_replicas = 2;
    snapshot.remotes[0].confirmations = vec![ConfirmationLevel::ReplicationComplete];
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::ObserveExisting
    );
}

#[test]
fn replication_evidence_does_not_imply_content_verification() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    snapshot.remotes[0].observation_current = Some(true);
    snapshot.remotes[0].confirmed_replicas = Some(1);
    snapshot.remotes[0].confirmations = vec![ConfirmationLevel::ReplicationComplete];
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::ObserveExisting
    );
}

#[test]
fn cancel_unknown_does_not_release_or_managed_clean() {
    let (request, mut snapshot) = existing(Action::Cancel, SideEffect::Unknown);
    snapshot.routes[0].cleanup = CleanupMode::Managed;
    snapshot.routes[0].exclusive_management = true;
    snapshot.remotes[0].ownership = Ownership::ApplicationCreated;
    snapshot.remotes[0].application_references = Some(0);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::NoWrite);
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    assert_eq!(plan.summary().physical_release_bytes, 0);
}

#[test]
fn renew_preserves_original_start_and_never_promises_provider_ttl() {
    let (request, snapshot) = existing(Action::Renew { expires_at: 250 }, SideEffect::Unknown);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::NoWrite);
    if let Some(Capture::Object(capture)) = &plan.items()[0].capture {
        assert_eq!(capture.target.as_ref().unwrap().lease.started_at, 90);
    } else {
        panic!("missing lease capture");
    }
    let mut late = snapshot;
    late.observed_at = 151;
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &late).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn skipped_old_intent_requires_reapply_not_create_or_backfill() {
    for action in [Action::Create(intent()), Action::Backfill(intent())] {
        let (request, mut snapshot) = fixture(action);
        snapshot.objects[0].decision = Some(DecisionState::Skipped);
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(
            plan.items()[0].status,
            PlanStatus::Blocked(SafeError::RequiresExplicitReapply)
        );
        assert_eq!(
            revalidate(&plan, &confirmation(&plan), &snapshot).err(),
            Some(SafeError::PlanBlocked)
        );
    }
    let (request, mut snapshot) = fixture(Action::Reapply(intent()));
    snapshot.objects[0].decision = Some(DecisionState::Skipped);
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Ready
    );
}

#[test]
fn trusted_manifest_keeps_exact_batch_tuple_path_and_private_version() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::TrustedManifest {
        batch_id: "batch-1".into(),
        entry_path: "dir/file".into(),
        batch_tuple: [("id".into(),Some("batch-1".into())),("state".into(),Some("published".into()))].into(),
        entry_tuple: [("version_row_id".into(),Some("version-row-1".into())),("cid".into(),Some("bafy-test-cid".into()))].into(),
    };
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    if let SelectionEvidence::TrustedManifest { entry_tuple, .. } = &mut snapshot.objects[0].evidence {
        entry_tuple.insert("version_row_id".into(),Some("other-private-version".into()));
    }
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn new_same_key_is_not_added_to_the_captured_manifest() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    let mut newer = snapshot.objects[0].clone();
    newer.identity.public_version = PublicVersionId::Version("public-2".into());
    newer.identity.private_version_id = "version-row-2".into();
    newer.identity.object_id = "object-2".into();
    newer.identity.cid = "bafy-new-cid".into();
    snapshot.objects.push(newer);
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(plan.items().len(), 1);
}

#[test]
fn every_identity_fence_and_safety_change_stops_confirmation() {
    let changes: Vec<fn(&mut Snapshot)> = vec![
        |s| s.objects[0].identity.bucket = "other-bucket".into(),
        |s| s.objects[0].identity.key = "other/key".into(),
        |s| s.objects[0].identity.public_version = PublicVersionId::Version("version-2".into()),
        |s| s.objects[0].identity.private_version_id = "private-2".into(),
        |s| s.objects[0].identity.object_id = "object-2".into(),
        |s| s.objects[0].identity.cid = "bafy-other-cid".into(),
        |s| s.objects[0].readable = Some(false),
        |s| s.objects[0].permitted = Some(false),
        |s| s.objects[0].allowed_routes.clear(),
        |s| s.objects[0].control_revision = Some("control-2".into()),
        |s| s.objects[0].config_revision = Some("config-2".into()),
        |s| { s.targets[0].native_tuple.insert("last_touched_at".into(),Some("2026-10-12T00:00:00.000000002Z".into())); },
        |s| s.targets[0].lease.generation += 1,
        |s| s.targets[0].lease.expires_at += 1,
        |s| s.targets[0].requirements.as_mut().unwrap().mode = ProviderMode::One,
        |s| s.remotes[0].epoch += 1,
        |s| s.remotes[0].resource_id = Some("resource-2".into()),
        |s| s.remotes[0].effect = SideEffect::Present,
        |s| s.remotes[0].accounted_bytes = Some(51),
        |s| s.routes[0].identity.credential_revision += 1,
        |s| s.routes[0].identity.endpoint_revision += 1,
        |s| s.routes[0].identity.scope = "other-account".into(),
        |s| s.routes[0].identity.strategy = "upload".into(),
        |s| s.routes[0].available = false,
        |s| s.routes[0].allocation_allowed = false,
        |s| s.ledger_domains[0].frontier[0].values[0] = "same-count-different-member".into(),
        |s| s.ledger_domains[0].complete = false,
        |s| s.objects.clear(),
        |s| s.targets.clear(),
        |s| s.remotes.clear(),
    ];
    let (request, snapshot) = existing(Action::Retry, SideEffect::Unknown);
    let plan = dry_run(&request, &snapshot).unwrap();
    for change in changes {
        let mut changed = snapshot.clone();
        change(&mut changed);
        assert!(revalidate(&plan, &confirmation(&plan), &changed).is_err());
    }
}

#[test]
fn unknown_never_falls_back_to_new_protocol_when_history_is_missing() {
    let (request, mut snapshot) = existing(Action::Reconcile, SideEffect::Unknown);
    snapshot.remotes[0].route.endpoint_revision = 1;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(
        plan.items()[0].status,
        PlanStatus::Blocked(SafeError::HistoricalRouteUnavailable)
    );
}

#[test]
fn historical_route_can_be_distinct_from_current_target_route() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Unknown);
    snapshot.remotes[0].route.endpoint_revision = 1;
    snapshot.remotes[0].route.strategy = "upload".into();
    let mut historical = snapshot.routes[0].clone();
    historical.identity = snapshot.remotes[0].route.clone();
    historical.allocation_allowed = false;
    snapshot.routes.push(historical);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::ReconcileHistorical);
    if let Some(Capture::Object(capture)) = &plan.items()[0].capture {
        assert_eq!(
            capture.remote_route.as_ref().unwrap().identity.strategy,
            "upload"
        );
    } else {
        panic!("missing history");
    }
}

fn add_destination(snapshot: &mut Snapshot) -> RouteIdentity {
    let mut destination = route();
    destination.provider_id = "provider-b".into();
    destination.scope = "account-b".into();
    destination.storage_domain = "remote-b".into();
    snapshot.objects[0].allowed_routes.push(destination.clone());
    let mut state = snapshot.routes[0].clone();
    state.identity = destination.clone();
    snapshot.routes.push(state);
    snapshot.ledger_domains.push(LedgerDomainSnapshot {
        backend: destination.backend.clone(),
        scope: destination.scope.clone(),
        resource_type: destination.resource_type,
        frontier: vec![native_key("pin_jobs", "destination-job")],
        complete: true,
    });
    destination
}

#[test]
fn migration_unknown_first_reconciles_source_without_destination_post() {
    let (mut request, mut snapshot) = existing(Action::Retry, SideEffect::Unknown);
    let destination = add_destination(&mut snapshot);
    request.items[0].action = Action::Migrate { destination };
    let plan = dry_run(&request, &snapshot).unwrap();
    if let Some(PlannedStep::Object(step)) = &plan.items()[0].step {
        assert_eq!(step.source, RemoteDirective::ReconcileHistorical);
        assert_eq!(step.destination, Some(RemoteDirective::NoWrite));
        assert!(step.preserve_source_responsibility);
    } else {
        panic!("missing migration");
    }
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    assert_eq!(plan.summary().possible_new_transfers, 0);
}

#[test]
fn migration_preserves_source_ttl_requirements_and_original_cid() {
    let (mut request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    let destination = add_destination(&mut snapshot);
    request.items[0].action = Action::Migrate { destination };
    let plan = dry_run(&request, &snapshot).unwrap();
    if let Some(PlannedStep::Object(step)) = &plan.items()[0].step {
        assert_eq!(step.source, RemoteDirective::NoWrite);
        assert_eq!(step.destination, Some(RemoteDirective::SubmitWithFence));
        assert!(step.preserve_source_responsibility);
    } else {
        panic!("missing migration");
    }
    if let Some(Capture::Object(capture)) = &plan.items()[0].capture {
        let target = capture.target.as_ref().unwrap();
        assert!(target.requirements == Some(requirements()));
        assert_eq!(target.lease.started_at, 90);
        assert_eq!(target.lease.expires_at, 150);
        assert_eq!(target.cid, object().identity.cid);
    } else {
        panic!("missing exact source");
    }
    assert_eq!(plan.summary().physical_release_bytes, 0);
}

#[test]
fn destination_safety_is_checked_separately() {
    let (mut request, mut snapshot) = existing(Action::Retry, SideEffect::Present);
    let destination = add_destination(&mut snapshot);
    request.items[0].action = Action::Migrate { destination };
    snapshot.objects[0].allowed_routes.pop();
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::SafetyProhibition)
    );
}

fn retirement_fixture() -> (PlanRequest, Snapshot) {
    let (_, mut snapshot) = existing(Action::Cancel, SideEffect::Unknown);
    snapshot.routes[0].available = false;
    snapshot.remotes[0].cleanup_pending = true;
    snapshot.remotes[0].inflight_operations = Some(1);
    snapshot.inventories.push(ProviderInventory {
        provider_id: route().provider_id,
        frontier: vec![native_key("pin_jobs", "job-1")],
        complete: true,
        allocation_stopped: false,
        leases: vec![snapshot.targets[0].lease.clone()],
        root_reference_ids: vec![NativeKey {table:"zip_root_references".into(),values:vec!["root-owner-1".into(),"1".into(),"1".into(),"node".into(),"hot".into(),"cid".into()]}],
        attribution_complete: true,
    });
    (
        PlanRequest {
            plan_id: "maintenance-1".into(),
            items: vec![ItemRequest {
                item_id: "file-1".into(),
                action: Action::Retire,
                subject: Subject::Provider { route: route() },
            }],
        },
        snapshot,
    )
}

#[test]
fn retirement_keeps_every_responsibility_and_surfaces_missing_credentials() {
    let (request, snapshot) = retirement_fixture();
    let plan = dry_run(&request, &snapshot).unwrap();
    if let Some(PlannedStep::Retire(step)) = &plan.items()[0].step {
        assert!(step.stop_new_allocations && step.needs_attention);
        assert_eq!(
            (
                step.leases,
                step.retained,
                step.unknown,
                step.inflight,
                step.cleanup_pending,
                step.root_references
            ),
            (1, 1, 1, 1, 1, 1)
        );
    } else {
        panic!("missing retirement");
    }
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    assert_eq!(plan.summary().physical_release_bytes, 0);
    assert!(revalidate(&plan, &confirmation(&plan), &snapshot).is_ok());
}

#[test]
fn retirement_requires_complete_inventory_and_detects_new_orphan_resources() {
    let (request, mut snapshot) = retirement_fixture();
    let plan = dry_run(&request, &snapshot).unwrap();
    let mut extra = snapshot.remotes[0].clone();
    extra.key.cid = "bafy-orphan".into();
    snapshot.remotes.push(extra);
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
    snapshot.inventories[0].complete = false;
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::IncompleteInventory)
    );
}

#[test]
fn repeat_saved_plan_with_full_receipt_cannot_repeat_upload_or_charge() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Rejected);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(revalidate(&plan, &confirmation(&plan), &snapshot).is_ok());
    snapshot.applied.push(AppliedItem {
        binding: plan.items()[0].clone(),
    });
    snapshot.targets[0].native_tuple.insert("state".into(),Some("pinned".into()));
    snapshot.targets[0].lease.generation += 1;
    snapshot.remotes[0].epoch += 1;
    let checked = revalidate(&plan, &confirmation(&plan), &snapshot).unwrap();
    assert!(checked[0].outcome == RevalidationOutcome::AlreadyApplied);
    let replay = dry_run(&request, &snapshot).unwrap();
    assert_eq!(replay.summary().already_applied, 1);
    assert_eq!(replay.summary().possible_new_transfers, 0);
    assert_eq!(replay.summary().logical_intent_bytes, 50);
    assert!(replay.items() == plan.items());
}

#[test]
fn shared_cid_is_one_resource_not_two_uploads_or_two_remote_charges() {
    let (mut request, mut snapshot) = fixture(Action::Create(intent()));
    let mut second = snapshot.objects[0].clone();
    second.identity.key = "prefix/second".into();
    second.identity.object_id = "object-2".into();
    second.identity.private_version_id = "version-row-2".into();
    let mut second_request = request.items[0].clone();
    second_request.item_id = "file-2".into();
    if let Subject::Object { identity, .. } = &mut second_request.subject {
        *identity = second.identity.clone();
    }
    request.items.push(second_request);
    snapshot.objects.push(second);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.summary().logical_targets, 2);
    assert_eq!(plan.summary().unique_cids, 1);
    assert_eq!(plan.summary().possible_new_transfers, 1);
    assert_eq!(plan.summary().logical_intent_bytes, 100);
    let (_, remote_snapshot) = existing(Action::Retry, SideEffect::Unknown);
    snapshot.remotes = remote_snapshot.remotes;
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.summary().unique_resources, 1);
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    assert_eq!(plan.summary().possible_new_transfers, 0);
}

#[test]
fn limits_and_duplicates_fail_closed_without_echoing_caller_text() {
    let (mut request, mut snapshot) = fixture(Action::Create(intent()));
    request.items = vec![request.items[0].clone(); MAX_ITEMS + 1];
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::LimitExceeded)
    );
    request.items.truncate(2);
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::DuplicateIdentity)
    );
    request.items.truncate(1);
    request.plan_id = "https://user:secret@example.test/?token=sentinel".into();
    let error = dry_run(&request, &snapshot).err().unwrap();
    assert_eq!(error, SafeError::InvalidInput);
    assert!(!format!("{error:?} {error}").contains("sentinel"));
    snapshot.remotes =
        vec![existing(Action::Retry, SideEffect::Unknown).1.remotes[0].clone(); MAX_RECORDS + 1];
    assert_eq!(validate_snapshot(&snapshot), Err(SafeError::LimitExceeded));
}

#[test]
fn corrupt_side_effect_and_route_identity_cannot_be_downgraded_to_absent() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Unknown);
    snapshot.remotes[0].effect = SideEffect::NotSubmitted;
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::InconsistentSnapshot)
    );
    snapshot.remotes[0].effect = SideEffect::Unknown;
    snapshot.remotes[0].key.scope = "different-scope".into();
    assert_eq!(
        validate_snapshot(&snapshot),
        Err(SafeError::InconsistentSnapshot)
    );
}

#[test]
fn wrong_confirmation_or_extra_candidate_ack_cannot_authorize_any_item() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    let mut confirmed = confirmation(&plan);
    confirmed.plan_id = "another-plan".into();
    assert_eq!(
        revalidate(&plan, &confirmed, &snapshot).err(),
        Some(SafeError::ConfirmationMismatch)
    );
    confirmed = confirmation(&plan);
    confirmed.candidate_item_ids.push("file-1".into());
    assert_eq!(
        revalidate(&plan, &confirmed, &snapshot).err(),
        Some(SafeError::ConfirmationMismatch)
    );
}

#[test]
fn expired_intent_stops_and_clock_advance_alone_is_allowed() {
    let (request, mut snapshot) = fixture(Action::Create(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.observed_at = 101;
    assert!(revalidate(&plan, &confirmation(&plan), &snapshot).is_ok());
    snapshot.observed_at = 200;
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn one_changed_entry_stops_whole_confirmation_without_partial_permits() {
    let (mut request, mut snapshot) = fixture(Action::Create(intent()));
    let mut second = snapshot.objects[0].clone();
    second.identity.object_id = "object-2".into();
    second.identity.private_version_id = "version-row-2".into();
    let mut item = request.items[0].clone();
    item.item_id = "file-2".into();
    if let Subject::Object { identity, .. } = &mut item.subject {
        *identity = second.identity.clone();
    }
    request.items.push(item);
    snapshot.objects.push(second);
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.objects[1].readable = Some(false);
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn confirmation_id_alone_cannot_confirm_a_substituted_plan() {
    let (mut request, mut snapshot) = fixture(Action::Create(intent()));
    let original = dry_run(&request, &snapshot).unwrap();
    let confirmed = confirmation(&original);
    snapshot.objects[0].identity.private_version_id = "version-row-2".into();
    snapshot.objects[0].identity.object_id = "object-2".into();
    if let Subject::Object { identity, .. } = &mut request.items[0].subject {
        *identity = snapshot.objects[0].identity.clone();
    }
    let substituted = dry_run(&request, &snapshot).unwrap();
    assert!(original != substituted);
    assert_eq!(
        revalidate(&substituted, &confirmed, &snapshot).err(),
        Some(SafeError::ConfirmationMismatch)
    );
}

#[test]
fn replay_cancel_does_not_hide_unknown_retained_occupancy() {
    let (request, mut snapshot) = existing(Action::Cancel, SideEffect::Unknown);
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.applied.push(AppliedItem {
        binding: plan.items()[0].clone(),
    });
    snapshot.targets[0].lease.state = LeaseState::Cancelled;
    snapshot.targets[0].lease.generation += 1;
    let replay = dry_run(&request, &snapshot).unwrap();
    assert_eq!(replay.summary().already_applied, 1);
    assert_eq!(replay.summary().accounted_remote_bytes, 50);
    assert_eq!(replay.summary().unknown_resources, 1);
    assert_eq!(replay.summary().physical_release_bytes, 0);
}

#[test]
fn old_unknown_epoch_prevents_post_even_if_latest_was_not_submitted() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::NotSubmitted);
    let mut history = existing(Action::Retry, SideEffect::Unknown)
        .1
        .remotes
        .remove(0);
    history.epoch = 10;
    history.resource_id = None;
    snapshot.historical_remotes.push(history);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(source(&plan), RemoteDirective::ReconcileHistorical);
    assert_eq!(plan.summary().possible_new_transfers, 0);
    assert_eq!(plan.summary().accounted_remote_bytes, 100);
    snapshot.historical_remotes[0].effect = SideEffect::InFlight;
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::WaitForInflight
    );
}

#[test]
fn old_known_resource_also_prevents_blind_resubmission() {
    let (request, mut snapshot) = existing(Action::Retry, SideEffect::Rejected);
    let mut history = existing(Action::Retry, SideEffect::Present)
        .1
        .remotes
        .remove(0);
    history.epoch = 10;
    snapshot.historical_remotes.push(history);
    assert_eq!(
        source(&dry_run(&request, &snapshot).unwrap()),
        RemoteDirective::ObserveExisting
    );
}

#[test]
fn history_addition_or_epoch_change_invalidates_confirmed_manifest() {
    let (request, mut snapshot) = existing(Action::Cancel, SideEffect::Present);
    let plan = dry_run(&request, &snapshot).unwrap();
    let mut history = snapshot.remotes[0].clone();
    history.epoch = 10;
    snapshot.historical_remotes.push(history);
    assert_eq!(
        revalidate(&plan, &confirmation(&plan), &snapshot).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn history_observations_of_same_resource_deduplicate_but_distinct_resources_do_not() {
    let (request, mut snapshot) = existing(Action::Cancel, SideEffect::Present);
    let mut history = snapshot.remotes[0].clone();
    history.epoch = 10;
    snapshot.historical_remotes.push(history);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.summary().unique_resources, 1);
    assert_eq!(plan.summary().accounted_remote_bytes, 50);
    snapshot.historical_remotes[0].resource_id = Some("older-resource".into());
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.summary().unique_resources, 2);
    assert_eq!(plan.summary().accounted_remote_bytes, 100);
}

#[test]
fn retirement_captures_history_even_without_live_target_or_route() {
    let (request, mut snapshot) = retirement_fixture();
    let mut history = snapshot.remotes[0].clone();
    history.epoch = 10;
    history.resource_id = Some("older-resource".into());
    history.route.credential_revision = 99;
    snapshot.historical_remotes.push(history);
    snapshot.targets.clear();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.summary().accounted_remote_bytes, 100);
    if let Some(PlannedStep::Retire(step)) = &plan.items()[0].step {
        assert!(step.needs_attention);
        assert_eq!(
            (
                step.retained,
                step.unknown,
                step.inflight,
                step.cleanup_pending
            ),
            (2, 2, 2, 2)
        );
    } else {
        panic!("missing retirement");
    }
}

#[test]
fn retiring_source_does_not_prevent_migration_to_explicit_allowed_destination() {
    let (mut request, mut snapshot) = existing(Action::Retry, SideEffect::NotSubmitted);
    snapshot.routes[0].allocation_allowed = false;
    let destination = add_destination(&mut snapshot);
    snapshot.routes[1].allocation_allowed = true;
    request.items[0].action = Action::Migrate { destination };
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    assert_eq!(source(&plan), RemoteDirective::NoWrite);
}

#[test]
fn missing_local_ledger_inventory_does_not_mean_remote_absent() {
    let (request, mut snapshot) = fixture(Action::Create(intent()));
    snapshot.ledger_domains.clear();
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::IncompleteInventory)
    );
}

#[test]
fn sums_overflow_safely_and_route_aliases_do_not_duplicate_intents() {
    let (mut request, mut snapshot) = fixture(Action::Create(intent()));
    let mut second = snapshot.objects[0].clone();
    second.identity.object_id = "object-2".into();
    second.identity.private_version_id = "version-row-2".into();
    let mut item = request.items[0].clone();
    item.item_id = "file-2".into();
    if let Subject::Object { identity, .. } = &mut item.subject {
        *identity = second.identity.clone();
    }
    request.items.push(item);
    snapshot.objects.push(second);
    snapshot.objects[0].logical_bytes = u64::MAX;
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::LimitExceeded)
    );
    request.items[1].subject = request.items[0].subject.clone();
    assert_eq!(
        dry_run(&request, &snapshot).err(),
        Some(SafeError::DuplicateIdentity)
    );
}
