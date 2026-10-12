//! Registered native management wire: imported data never becomes authority.
use ipfs_s3_gateway::pinning::management;

use management::{wire::*, *};
use serde_json::{Value, json};

fn root_key(batch:&str)->NativeKey {
    NativeKey {table:"zip_root_references".into(),values:vec![batch.into(),"1".into(),"1".into(),"node".into(),"hot".into(),"cid".into()]}
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
                batch_tuple: [("id".into(),Some("batch-1".into()))].into(),
                entry_tuple: [("version_row_id".into(),Some("private-version-1".into()))].into(),
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
                frontier: vec![NativeKey {table:"pin_jobs".into(),values:vec!["job-1".into()]}],
                complete: true,
            })
            .collect(),
        ..Snapshot::default()
    };
    if existing {
        snapshot.targets.push(TargetSnapshot {
            id: "target-1".into(),
            native_tuple: [("id".into(),Some("target-1".into()))].into(),
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
            frontier: vec![NativeKey {table:"pin_jobs".into(),values:vec!["job-1".into()]}],
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

fn document(request: &PlanRequest, snapshot: &Snapshot) -> Value {
    let plan = dry_run(request, snapshot).unwrap();
    let confirmation = plan.confirmation(vec![]);
    serde_json::from_slice(&encode_plan(request, snapshot, &plan, Some(&confirmation)).unwrap())
        .unwrap()
}

fn load(value: &Value) -> Result<ImportedPlan, SafeError> {
    decode_plan(&serde_json::to_vec(value).unwrap())
}

#[test]
fn nine_actions_round_trip_original_inputs_plan_and_full_confirmation() {
    for action in [
        Action::Create(intent()),
        Action::Renew { expires_at: 250 },
        Action::Cancel,
        Action::Reapply(intent()),
        Action::Backfill(intent()),
        Action::Retry,
        Action::Migrate {
            destination: route("provider-b"),
        },
        Action::Reconcile,
        Action::Retire,
    ] {
        let (request, snapshot) = fixture(action);
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(plan.items()[0].status, PlanStatus::Ready);
        let confirmation = plan.confirmation(vec![]);
        let bytes = encode_plan(&request, &snapshot, &plan, Some(&confirmation)).unwrap();
        let imported = decode_plan(&bytes).unwrap();
        assert!(imported.request() == &request);
        assert!(imported.snapshot_without_applied() == &snapshot);
        assert!(imported.plan() == &plan);
        assert!(imported.confirmation() == Some(&confirmation));
        assert!(imported.untrusted_applied_claims().is_empty());
        assert!(revalidate(imported.plan(), imported.confirmation().unwrap(), &snapshot).is_ok());
        assert_eq!(
            bytes,
            encode_plan(&request, &snapshot, &plan, Some(&confirmation)).unwrap()
        );
    }
}

#[test]
fn same_ids_cannot_replace_full_confirmation_binding() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let original = document(&request, &snapshot);
    for (field, replacement) in [
        ("bindings", json!([])),
        ("plan_id", json!("another-plan")),
        ("candidate_item_ids", json!(["file-1"])),
    ] {
        let mut changed = original.clone();
        changed["confirmation"][field] = replacement;
        assert_eq!(load(&changed).err(), Some(SafeError::ConfirmationMismatch));
    }
    let mut changed = original;
    changed["confirmation"]["bindings"][0]["capture"]["object"]["object"]["logical_bytes"] =
        json!(51);
    assert_eq!(load(&changed).err(), Some(SafeError::ConfirmationMismatch));
}

#[test]
fn changed_frozen_object_version_or_plan_is_rejected() {
    let (request, snapshot) = fixture(Action::Backfill(intent()));
    let original = document(&request, &snapshot);
    for field in ["object_id", "private_version_id", "cid", "key"] {
        let mut changed = original.clone();
        changed["snapshot"]["objects"][0]["identity"][field] = json!("replacement");
        assert_eq!(load(&changed).err(), Some(SafeError::IdempotencyConflict));
    }
    let mut changed = original.clone();
    changed["snapshot"]["objects"][0]["identity"]["public_version"] = json!({"version": "v2"});
    assert_eq!(load(&changed).err(), Some(SafeError::IdempotencyConflict));
    let mut changed = original;
    changed["plan"]["summary"]["logical_intent_bytes"] = json!(999);
    assert_eq!(load(&changed).err(), Some(SafeError::IdempotencyConflict));
}

#[test]
fn imported_claims_never_enter_the_preflight_snapshot() {
    let (request, mut snapshot) = fixture(Action::Cancel);
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.applied.push(AppliedItem {
        binding: plan.items()[0].clone(),
    });
    let replay_plan = dry_run(&request, &snapshot).unwrap();
    let bytes = encode_plan(&request, &snapshot, &replay_plan, None).unwrap();
    let imported = decode_plan(&bytes).unwrap();
    assert!(imported.untrusted_applied_claims() == snapshot.applied);
    assert!(imported.snapshot_without_applied().applied.is_empty());
    let checked = revalidate(
        imported.plan(),
        &imported.plan().confirmation(vec![]),
        imported.snapshot_without_applied(),
    )
    .unwrap();
    assert!(matches!(
        checked[0].outcome,
        RevalidationOutcome::Checked(_)
    ));
    let mut fresh = imported.snapshot_without_applied().clone();
    fresh.objects[0].readable = Some(false);
    assert_eq!(
        revalidate(
            imported.plan(),
            &imported.plan().confirmation(vec![]),
            &fresh
        )
        .err(),
        Some(SafeError::SnapshotChanged)
    );
    let mut changed = serde_json::from_slice::<Value>(&bytes).unwrap();
    changed["snapshot"]["applied"][0]["binding"]["request"]["action"] = json!("retry");
    assert_eq!(load(&changed).err(), Some(SafeError::IdempotencyConflict));
}

#[test]
fn unknown_fields_illegal_enums_and_wrong_types_fail_closed() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let original = document(&request, &snapshot);
    let mut unknown = original.clone();
    unknown["secret"] = json!("must-not-echo");
    let mut nested = original.clone();
    nested["snapshot"]["objects"][0]["identity"]["secret"] = json!("must-not-echo");
    let mut action = original.clone();
    action["request"]["items"][0]["action"] = json!("erase_everything");
    let mut variant = original.clone();
    variant["request"]["items"][0]["subject"]["object"]["secret"] = json!("must-not-echo");
    let mut trust = original.clone();
    trust["applied_trust"] = json!("trusted");
    let mut number = original.clone();
    number["snapshot"]["observed_at"] = json!("100");
    let mut missing = original;
    missing.as_object_mut().unwrap().remove("schema_version");
    for changed in [unknown, nested, action, variant, trust, number, missing] {
        assert_eq!(load(&changed).err(), Some(SafeError::InvalidInput));
    }
}

#[test]
fn strict_fields_apply_inside_plan_capture_step_and_confirmation() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let original = document(&request, &snapshot);
    for pointer in [
        "/request",
        "/snapshot",
        "/snapshot/routes/0",
        "/snapshot/routes/0/identity",
        "/request/items/0/action/create",
        "/request/items/0/action/create/requirements",
        "/plan",
        "/plan/items/0",
        "/plan/items/0/capture/object",
        "/plan/items/0/step/object",
        "/plan/summary",
        "/confirmation",
    ] {
        let mut changed = original.clone();
        changed
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("secret".into(), json!("must-not-echo"));
        let error = load(&changed).err().unwrap();
        assert_eq!(error, SafeError::InvalidInput);
        assert_eq!(error.to_string(), "management validation: InvalidInput");
    }
    for field in ["applied_trust", "confirmation"] {
        let mut changed = original.clone();
        changed.as_object_mut().unwrap().remove(field);
        assert_eq!(load(&changed).err(), Some(SafeError::InvalidInput));
    }
}

#[test]
fn preflight_budget_boundaries_do_not_reject_before_the_document_shape_check() {
    let nodes = format!("[{}0]", "0,".repeat(MAX_WIRE_NODES - 2));
    assert_eq!(
        decode_plan(nodes.as_bytes()).err(),
        Some(SafeError::InvalidInput)
    );
    let depth = format!(
        "{}0{}",
        "[".repeat(MAX_WIRE_DEPTH),
        "]".repeat(MAX_WIRE_DEPTH)
    );
    assert_eq!(
        decode_plan(depth.as_bytes()).err(),
        Some(SafeError::InvalidInput)
    );
    assert_eq!(
        decode_plan(&vec![b' '; MAX_WIRE_BYTES]).err(),
        Some(SafeError::InvalidInput)
    );
}

#[test]
fn malformed_duplicate_fields_and_unsupported_versions_are_safe() {
    for malformed in [
        b"".as_slice(),
        b"null",
        b"[]",
        b"{",
        b"{\"secret\":\"must-not-echo\"}",
        b"{\"schema_version\":1,\"schema_version\":1}",
        b"\xff",
    ] {
        let error = decode_plan(malformed).err().unwrap();
        assert_eq!(error, SafeError::InvalidInput);
        assert_eq!(error.to_string(), "management validation: InvalidInput");
    }
    let (request, snapshot) = fixture(Action::Create(intent()));
    let mut saved = document(&request, &snapshot);
    saved["schema_version"] = json!(WIRE_SCHEMA_VERSION + 1);
    assert_eq!(
        load(&saved).err(),
        Some(SafeError::UnsupportedSchemaVersion)
    );
    let valid = serde_json::to_vec(&document(&request, &snapshot)).unwrap();
    let mut trailing = valid.clone();
    trailing.extend_from_slice(b" null");
    assert_eq!(decode_plan(&trailing).err(), Some(SafeError::InvalidInput));
    let text = String::from_utf8(valid).unwrap();
    let duplicate = text.replacen(
        "\"observed_at\":100",
        "\"observed_at\":100,\"observed_at\":100",
        1,
    );
    assert_eq!(
        decode_plan(duplicate.as_bytes()).err(),
        Some(SafeError::InvalidInput)
    );
}

#[test]
fn byte_node_depth_and_snapshot_record_budgets_are_enforced() {
    assert_eq!(
        (MAX_ITEMS, MAX_PROVIDERS, MAX_TEXT_BYTES),
        (128, 64, 4 * 1024 * 1024)
    );
    assert_eq!(
        decode_plan(&vec![b' '; MAX_WIRE_BYTES + 1]).err(),
        Some(SafeError::LimitExceeded)
    );
    let nodes = format!("[{}0]", "0,".repeat(MAX_WIRE_NODES));
    assert_eq!(
        decode_plan(nodes.as_bytes()).err(),
        Some(SafeError::LimitExceeded)
    );
    let depth = format!(
        "{}0{}",
        "[".repeat(MAX_WIRE_DEPTH + 1),
        "]".repeat(MAX_WIRE_DEPTH + 1)
    );
    assert_eq!(
        decode_plan(depth.as_bytes()).err(),
        Some(SafeError::LimitExceeded)
    );
    let (request, snapshot) = fixture(Action::Create(intent()));
    assert!(validate_request(&request).is_ok());
    let mut saved = document(&request, &snapshot);
    // Smaller domain rows stay below the generic node budget: this independently
    // exercises the snapshot's aggregate record bound before duplicate indexing.
    let domain = saved["snapshot"]["ledger_domains"][0].clone();
    saved["snapshot"]["ledger_domains"] = Value::Array(vec![domain; MAX_RECORDS + 1]);
    assert_eq!(load(&saved).err(), Some(SafeError::LimitExceeded));
}

#[test]
fn snapshot_text_budget_is_independent_of_wire_bytes_and_records() {
    let (request, mut snapshot) = fixture(Action::Create(intent()));
    let mut saved = document(&request, &snapshot);
    let mut object = snapshot.objects[0].clone();
    object.identity.bucket = "b".repeat(255);
    object.identity.key = "k".repeat(1024);
    object.identity.public_version = PublicVersionId::Version("v".repeat(256));
    object.identity.cid = "c".repeat(512);
    object.control_revision = Some("x".repeat(256));
    object.config_revision = Some("x".repeat(256));
    object.evidence = SelectionEvidence::TrustedManifest {
        batch_id: "b".repeat(256),
        batch_tuple: [("id".into(),Some("batch-1".into()))].into(),
        entry_tuple: [("version_row_id".into(),Some("private-version-1".into()))].into(),
        entry_path: "p".repeat(1024),
    };
    let mut long_route = route("provider-a");
    long_route.backend = "a".repeat(256);
    long_route.scope = "a".repeat(256);
    long_route.storage_domain = "a".repeat(256);
    long_route.api_profile = "a".repeat(256);
    long_route.strategy = "a".repeat(256);
    object.allowed_routes = vec![long_route];
    snapshot.objects = (0..1000)
        .map(|index| {
            let mut object = object.clone();
            object.identity.private_version_id = format!("v{index:0255}");
            object.identity.object_id = format!("o{index:0255}");
            object
        })
        .collect();
    saved["snapshot"] = serde_json::to_value(&snapshot).unwrap();
    let bytes = serde_json::to_vec(&saved).unwrap();
    assert!(bytes.len() < MAX_WIRE_BYTES);
    assert_eq!(decode_plan(&bytes).err(), Some(SafeError::LimitExceeded));
    let plan = dry_run(&request, &fixture(Action::Create(intent())).1).unwrap();
    assert_eq!(
        encode_plan(&request, &snapshot, &plan, None).err(),
        Some(SafeError::LimitExceeded)
    );
}

#[test]
fn complete_snapshot_preserves_unselected_rows_history_and_retirement_inventory() {
    let (request, mut snapshot) = fixture(Action::Reconcile);
    let mut other = snapshot.objects[0].clone();
    other.identity.private_version_id = "private-version-2".into();
    other.identity.object_id = "object-2".into();
    other.identity.public_version = PublicVersionId::Null;
    other.identity.cid = "bafy-second-cid".into();
    snapshot.objects.push(other);
    let remote = RemoteSnapshot {
        key: route("provider-a").remote_key("bafy-test-cid"),
        route: route("provider-a"),
        resource_id: Some("opaque-resource-1".into()),
        epoch: 11,
        effect: SideEffect::Unknown,
        ownership: Ownership::ExternalExisting,
        application_references: Some(3),
        inflight_operations: Some(1),
        retained: true,
        cleanup_pending: true,
        accounted_bytes: Some(70),
        confirmations: vec![
            ConfirmationLevel::Accepted,
            ConfirmationLevel::GatewayVerified,
        ],
        confirmed_replicas: Some(2),
        observation_current: Some(false),
        replica_started_at: None,
    };
    let mut historical = remote.clone();
    historical.epoch = 10;
    historical.effect = SideEffect::Present;
    historical.resource_id = Some("opaque-historical-resource".into());
    snapshot.remotes.push(remote);
    snapshot.historical_remotes.push(historical);
    snapshot.inventories.push(ProviderInventory {
        provider_id: "provider-a".into(),
        frontier: vec![NativeKey {table:"pin_jobs".into(),values:vec!["job-1".into()]}],
        complete: true,
        allocation_stopped: true,
        attribution_complete: true,
        leases: vec![snapshot.targets[0].lease.clone()],
        root_reference_ids: vec![root_key("root-1"), root_key("root-2")],
    });
    let plan = dry_run(&request, &snapshot).unwrap();
    let confirmation = plan.confirmation(vec![]);
    let imported =
        decode_plan(&encode_plan(&request, &snapshot, &plan, Some(&confirmation)).unwrap())
            .unwrap();
    assert!(imported.snapshot_without_applied() == &snapshot);
    assert!(imported.plan() == &plan);
    assert_eq!(imported.plan().items().len(), 1);
    let mut current = snapshot.clone();
    current.objects[0].identity.private_version_id = "new-current-version".into();
    assert_eq!(
        revalidate(imported.plan(), imported.confirmation().unwrap(), &current).err(),
        Some(SafeError::SnapshotChanged)
    );
}

#[test]
fn duplicate_requirements_routes_candidates_and_receipts_are_not_sets_by_coercion() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let original = document(&request, &snapshot);
    let mut providers = original.clone();
    providers["request"]["items"][0]["action"]["create"]["requirements"]["provider_ids"] =
        json!(["provider-a", "provider-a"]);
    assert_eq!(load(&providers).err(), Some(SafeError::DuplicateIdentity));
    let mut levels = original.clone();
    levels["request"]["items"][0]["action"]["create"]["requirements"]["levels"] =
        json!(["content_verified", "content_verified"]);
    assert_eq!(load(&levels).err(), Some(SafeError::DuplicateIdentity));
    let mut routes = original;
    let route = routes["snapshot"]["objects"][0]["allowed_routes"][0].clone();
    routes["snapshot"]["objects"][0]["allowed_routes"]
        .as_array_mut()
        .unwrap()
        .push(route);
    assert_eq!(load(&routes).err(), Some(SafeError::DuplicateIdentity));
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::LegacyZipCandidate;
    let plan = dry_run(&request, &snapshot).unwrap();
    let confirmation = plan.confirmation(vec!["file-1".into(), "file-1".into()]);
    assert_eq!(
        encode_plan(&request, &snapshot, &plan, Some(&confirmation)).err(),
        Some(SafeError::ConfirmationMismatch)
    );
    let (request, mut snapshot) = fixture(Action::Cancel);
    let plan = dry_run(&request, &snapshot).unwrap();
    snapshot.applied = vec![
        AppliedItem {
            binding: plan.items()[0].clone()
        };
        2
    ];
    assert_eq!(
        encode_plan(&request, &snapshot, &plan, None).err(),
        Some(SafeError::DuplicateIdentity)
    );
}

#[test]
fn snapshot_duplicates_and_same_id_different_requests_are_rejected() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let original = document(&request, &snapshot);
    let mut duplicate = original.clone();
    let object = duplicate["snapshot"]["objects"][0].clone();
    duplicate["snapshot"]["objects"]
        .as_array_mut()
        .unwrap()
        .push(object);
    assert_eq!(load(&duplicate).err(), Some(SafeError::DuplicateIdentity));
    let mut changed = original;
    changed["request"]["items"][0]["action"] = json!({"backfill": {
        "started_at": 100, "expires_at": 200,
        "requirements": serde_json::to_value(intent().requirements).unwrap()
    }});
    assert_eq!(load(&changed).err(), Some(SafeError::IdempotencyConflict));
}

#[test]
fn unconfirmed_candidate_and_blocked_plan_remain_non_executable() {
    let (request, mut snapshot) = fixture(Action::Backfill(intent()));
    snapshot.objects[0].evidence = SelectionEvidence::LegacyZipCandidate;
    let plan = dry_run(&request, &snapshot).unwrap();
    let imported = decode_plan(&encode_plan(&request, &snapshot, &plan, None).unwrap()).unwrap();
    assert!(imported.confirmation().is_none());
    assert_eq!(
        revalidate(imported.plan(), &plan.confirmation(vec![]), &snapshot).err(),
        Some(SafeError::CandidateConfirmationRequired)
    );
    let confirmation = plan.confirmation(vec!["file-1".into()]);
    let imported =
        decode_plan(&encode_plan(&request, &snapshot, &plan, Some(&confirmation)).unwrap())
            .unwrap();
    assert!(imported.confirmation() == Some(&confirmation));
    snapshot.objects[0].permitted = Some(false);
    let blocked = dry_run(&request, &snapshot).unwrap();
    let imported = decode_plan(&encode_plan(&request, &snapshot, &blocked, None).unwrap()).unwrap();
    assert_eq!(
        revalidate(
            imported.plan(),
            &blocked.confirmation(vec!["file-1".into()]),
            &snapshot
        )
        .err(),
        Some(SafeError::ConfirmationMismatch)
    );
}

#[test]
fn encoder_rejects_substituted_plan_and_confirmation() {
    let (request, snapshot) = fixture(Action::Create(intent()));
    let plan = dry_run(&request, &snapshot).unwrap();
    let mut changed = snapshot.clone();
    changed.objects[0].logical_bytes += 1;
    let replacement = dry_run(&request, &changed).unwrap();
    assert_eq!(
        encode_plan(&request, &snapshot, &replacement, None).err(),
        Some(SafeError::IdempotencyConflict)
    );
    assert_eq!(
        encode_plan(
            &request,
            &snapshot,
            &plan,
            Some(&replacement.confirmation(vec![]))
        )
        .err(),
        Some(SafeError::ConfirmationMismatch)
    );
}
