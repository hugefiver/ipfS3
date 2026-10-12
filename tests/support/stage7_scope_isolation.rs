use super::*;
use ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot;
use sea_orm::{DatabaseConnection,TransactionTrait};

fn foreign_route(config: &local_config::ManagementConfig) -> ProviderRouteSnapshot {
    let mut route = config.pinning.providers[0].route_snapshot();
    route.provider_id = "foreign-provider".into();
    route.scope = "different-account".into();
    route.storage_domain = "foreign-storage".into();
    route
}

async fn insert_resource(db: &DatabaseConnection, alias: &str, cid: &str, original: Option<String>, epoch: Option<i64>) {
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'unknown','unknown')",
        [alias.into(), cid.into(), original.into()])).await.unwrap();
    if let Some(epoch) = epoch {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO remote_pins(provider,cid,request_id,status,cid_size,epoch,last_touched_at) VALUES($1,$2,$3,'failed',10,$4,$5)",
            [alias.into(), cid.into(), format!("resource-{alias}").into(), epoch.into(), chrono::Utc::now().into()])).await.unwrap();
    }
}

async fn insert_history(db: &DatabaseConnection, alias: &str, original: &ProviderRouteSnapshot) -> String {
    let history = serde_json::json!({
        "ledger": { "provider": alias, "cid": CID,
            "route": serde_json::to_string(original).unwrap(),
            "ownership": "unknown", "effect": "unknown", "first_observed_at": null, "last_observed_at": null,
            "remote_pinned_at": null, "gateway_verified_at": null, "content_verified_at": null,
            "first_error": null, "last_error": null },
        "request_id": format!("history-{alias}"), "confirmed_absent": false,
    }).to_string();
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES($1,$2,1,$3)",
        [alias.into(), CID.into(), history.clone().into()])).await.unwrap();
    history
}

async fn proof(db: &DatabaseConnection, raw: &ipfs_s3_gateway::config::Config, config: &local_config::ManagementConfig, identity: &ExactObject) {
    let auth = records::authorize_local(raw, db).await.unwrap();
    evidence::prepare(db, config, &auth, &ipfs_s3_gateway::kubo::KuboClient::new(raw.kubo.rpc_url.clone()),
        &evidence::PrepareRequest { record_id: "scope-proof".into(), identity: identity.clone(), expected_node: NODE.into(), max_stored_bytes: 10, ttl: chrono::Duration::minutes(5), directory: false },
        CancellationToken::new()).await.unwrap();
}

fn ledger_key(alias: &str, cid: &str) -> NativeKey {
    NativeKey { table: "remote_pin_ledger".into(), values: vec![alias.into(), cid.into()] }
}

async fn unchanged(db: &DatabaseConnection, before: &native::NativeFrame, snapshot: &Snapshot) {
    assert!(snapshot.native_frame.as_ref().unwrap() == before);
    assert!(*before == native::capture(db).await.unwrap());
}

#[tokio::test]
async fn scope_isolation_complete_original_foreign_route_without_epoch_same_cid_is_item_disjoint() {
    let server = MockServer::start().await; source(&server).await;
    let raw = config(&server.uri()); let config = management_config(&raw);
    let (_directory, db) = database().await; setup(&db, &config).await;
    let identity = publish(&db, &config, "selected", false).await;
    write_current_decision(&db, &config, &identity).await;
    let original = format!(" {} ", serde_json::to_string(&foreign_route(&config)).unwrap());
    insert_resource(&db, "old-foreign-alias", CID, Some(original.clone()), None).await;
    proof(&db, &raw, &config, &identity).await;
    let request = support::request(identity, &config, false);
    let before = native::capture(&db).await.unwrap();
    let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let key = ledger_key("old-foreign-alias", CID);
    assert!(snapshot.unknown_resources.contains(&key));
    assert!(snapshot.native_frame.as_ref().unwrap().frontier.contains(&key));
    assert_eq!(snapshot.native_frame.as_ref().unwrap().original_value("remote_pin_ledger", &["old-foreign-alias", CID], "route"), Some(original.as_str()));
    assert!(snapshot.remotes.is_empty()); // No invented epoch/count/accounting DTO.
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    assert!(plan.summary().unknown_resources >= 1);
    unchanged(&db, &before, &snapshot).await;
    // Decision disjointness is not a narrowed confirmation fence: even a raw
    // whitespace-only foreign tuple change invalidates the original full plan.
    db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE remote_pin_ledger SET route=$1 WHERE provider='old-foreign-alias' AND cid=$2",
        [format!("{original} ").into(), CID.into()])).await.unwrap();
    let changed = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    assert_eq!(dry_run(&request, &changed).unwrap().items()[0].status, PlanStatus::Ready);
    assert!(before.frontier == changed.native_frame.as_ref().unwrap().frontier);
    assert_eq!(revalidate(&plan, &plan.confirmation(vec![]), &changed).err(), Some(SafeError::SnapshotChanged));
}

#[tokio::test]
async fn scope_isolation_foreign_domain_two_aliases_canonical_remote_key_collision_preserves_both() {
    let server = MockServer::start().await; source(&server).await;
    let raw = config(&server.uri()); let config = management_config(&raw);
    let (_directory, db) = database().await; setup(&db, &config).await;
    let identity = publish(&db, &config, "selected", false).await;
    write_current_decision(&db, &config, &identity).await;
    let original = serde_json::to_string(&foreign_route(&config)).unwrap();
    for alias in ["foreign-alias-one", "foreign-alias-two"] {
        insert_resource(&db, alias, CID, Some(original.clone()), Some(1)).await;
    }
    proof(&db, &raw, &config, &identity).await;
    let request = support::request(identity, &config, false);
    let before = native::capture(&db).await.unwrap();
    let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    assert!(snapshot.remotes.is_empty());
    for alias in ["foreign-alias-one", "foreign-alias-two"] {
        for table in ["remote_pin_ledger", "remote_pins"] {
            let key = NativeKey { table: table.into(), values: vec![alias.into(), CID.into()] };
            assert!(snapshot.unknown_resources.contains(&key));
            assert!(snapshot.native_frame.as_ref().unwrap().frontier.contains(&key));
        }
        assert_eq!(snapshot.native_frame.as_ref().unwrap().original_value("remote_pin_ledger", &[alias, CID], "route"), Some(original.as_str()));
    }
    assert!(plan.summary().unknown_resources >= 4);
    unchanged(&db, &before, &snapshot).await;
    // Full provider inventory does not erase foreign alias uncertainty merely
    // because the object item can prove its own decision disjoint from it.
    let retire = PlanRequest { plan_id: "scope-retire".into(), items: vec![ItemRequest {
        item_id: "retire".into(), action: Action::Retire,
        subject: Subject::Provider { route: RouteIdentity::from_original(config.pinning.providers[0].route_snapshot()) },
    }] };
    let retirement = collect_with_config(&db, &config, &retire).await.unwrap().planning_snapshot().unwrap();
    let retirement_plan = dry_run(&retire, &retirement).unwrap();
    let Some(PlannedStep::Retire(step)) = &retirement_plan.items()[0].step else { panic!("retirement capture"); };
    assert!(step.needs_attention);
    assert!(step.unknown >= 4);
    assert!(retirement_plan.summary().unknown_resources >= 4);
    unchanged(&db, &before, &retirement).await;
}

#[tokio::test]
async fn scope_isolation_same_domain_unscoped_malformed_and_alias_collision_fail_closed_per_item() {
    for case in ["same-domain-no-epoch", "unscoped", "malformed-route", "malformed-route-fields", "unknown-profile", "legacy-unknown-scope", "malformed-cid", "same-domain-collision"] {
        let server = MockServer::start().await; source(&server).await;
        let raw = config(&server.uri()); let config = management_config(&raw);
        let (_directory, db) = database().await; setup(&db, &config).await;
        let identity = publish(&db, &config, "selected", false).await;
        write_current_decision(&db, &config, &identity).await;
        let mut original = config.pinning.providers[0].route_snapshot();
        if case == "malformed-route-fields" { original.scope.clear(); }
        if case == "unknown-profile" { original.api_profile = "unrecognized-prefix".into(); }
        if case == "legacy-unknown-scope" { original.provider_id = "legacy:old-provider".into(); original.scope = "legacy-other-account".into(); }
        let original = match case {
            "unscoped" => None,
            "malformed-route" => Some("{not-a-route".into()),
            _ => Some(serde_json::to_string(&original).unwrap()),
        };
        let cid = if case == "malformed-cid" { "not-a-cid" } else { CID };
        let collision = case == "same-domain-collision";
        insert_resource(&db, "uncertain-alias-one", cid, original.clone(), collision.then_some(1)).await;
        if collision { insert_resource(&db, "uncertain-alias-two", cid, original, Some(1)).await; }
        proof(&db, &raw, &config, &identity).await;
        let request = support::request(identity, &config, false); let before = native::capture(&db).await.unwrap();
        let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(plan.items()[0].status, PlanStatus::Blocked(SafeError::UnknownEvidence), "{case}");
        assert!(plan.items()[0].step.is_none());
        assert!(snapshot.unknown_resources.contains(&ledger_key("uncertain-alias-one", cid)));
        if collision { assert!(snapshot.unknown_resources.contains(&ledger_key("uncertain-alias-two", cid))); }
        assert!(plan.summary().unknown_resources >= 1);
        unchanged(&db, &before, &snapshot).await;
    }
}

#[tokio::test]
async fn scope_isolation_multiple_request_domains_are_checked_per_item_not_union_authorized() {
    let server = MockServer::start().await; source(&server).await;
    let raw = config(&server.uri()); let config = management_config(&raw);
    let (_directory, db) = database().await; setup(&db, &config).await;
    let identity = publish(&db, &config, "selected", false).await;
    write_current_decision(&db, &config, &identity).await;
    let foreign = foreign_route(&config); let original = serde_json::to_string(&foreign).unwrap();
    for alias in ["foreign-one", "foreign-two"] { insert_resource(&db, alias, CID, Some(original.clone()), Some(1)).await; }
    proof(&db, &raw, &config, &identity).await;
    let mut request = support::request(identity, &config, false);
    let mut other = request.items[0].clone(); other.item_id = "foreign-item".into();
    let Subject::Object { target, .. } = &mut other.subject else { panic!("object"); };
    *target = TargetSelection::New(RouteIdentity::from_original(foreign));
    request.items.push(other);
    let before = native::capture(&db).await.unwrap();
    let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let selected=plan.items().iter().find(|entry|entry.request.item_id=="object").unwrap();
    let foreign=plan.items().iter().find(|entry|entry.request.item_id=="foreign-item").unwrap();
    assert_eq!(selected.status, PlanStatus::Ready);
    assert_eq!(foreign.status, PlanStatus::Blocked(SafeError::UnknownEvidence));
    assert!(foreign.step.is_none());
    unchanged(&db, &before, &snapshot).await;
}

#[tokio::test]
async fn scope_isolation_migrate_destination_and_all_one_requirement_candidates_are_not_dropped() {
    for case in ["migrate", "all-candidates", "one-candidates"] {
        let server = MockServer::start().await; source(&server).await;
        let raw = config(&server.uri()); let config = management_config(&raw);
        let (_directory, db) = database().await; setup(&db, &config).await;
        let identity = publish(&db, &config, "selected", case == "migrate").await;
        write_current_decision(&db, &config, &identity).await;
        let foreign = foreign_route(&config);
        insert_resource(&db, "foreign-candidate", CID, Some(serde_json::to_string(&foreign).unwrap()), None).await;
        proof(&db, &raw, &config, &identity).await;
        let mut request = support::request(identity, &config, false);
        if case == "migrate" {
            let frame = native::capture(&db).await.unwrap();
            let target_id = native::value(&frame.table("pin_lease_targets").unwrap().rows[0], "id").unwrap().to_owned();
            let Subject::Object { target, .. } = &mut request.items[0].subject else { panic!("object"); };
            *target = TargetSelection::Existing { target_id, source: None };
            request.items[0].action = Action::Migrate { destination: RouteIdentity::from_original(foreign) };
        } else {
            let Action::Create(intent) = &mut request.items[0].action else { panic!("create"); };
            intent.requirements.provider_ids.push(foreign.provider_id.clone());
            if case == "one-candidates" { intent.requirements.mode = ProviderMode::One; }
        }
        let before = native::capture(&db).await.unwrap();
        let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(plan.items()[0].status, PlanStatus::Blocked(SafeError::UnknownEvidence), "{case}");
        assert!(plan.items()[0].step.is_none());
        assert!(snapshot.unknown_resources.contains(&ledger_key("foreign-candidate", CID)));
        unchanged(&db, &before, &snapshot).await;
    }
}

#[tokio::test]
async fn scope_isolation_conflicting_current_and_same_epoch_history_routes_do_not_select_one() {
    let server = MockServer::start().await; source(&server).await;
    let raw = config(&server.uri()); let config = management_config(&raw);
    let (_directory, db) = database().await; setup(&db, &config).await;
    let identity = publish(&db, &config, "selected", false).await;
    write_current_decision(&db, &config, &identity).await;
    insert_resource(&db, "conflicting-alias", CID, Some(serde_json::to_string(&foreign_route(&config)).unwrap()), Some(1)).await;
    let history = insert_history(&db, "conflicting-alias", &config.pinning.providers[0].route_snapshot()).await;
    proof(&db, &raw, &config, &identity).await;
    let request = support::request(identity, &config, false); let before = native::capture(&db).await.unwrap();
    let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Blocked(SafeError::UnknownEvidence));
    assert!(snapshot.unknown_resources.contains(&ledger_key("conflicting-alias", CID)));
    assert_eq!(snapshot.native_frame.as_ref().unwrap().original_value("pin_resource_history", &["conflicting-alias", CID, "1"], "ledger"), Some(history.as_str()));
    unchanged(&db, &before, &snapshot).await;
}

#[tokio::test]
async fn scope_isolation_history_alias_collision_preserves_epoch_pk_and_unknown_accounting() {
    for foreign in [true, false] {
        let server = MockServer::start().await; source(&server).await;
        let raw = config(&server.uri()); let config = management_config(&raw);
        let (_directory, db) = database().await; setup(&db, &config).await;
        let identity = publish(&db, &config, "selected", false).await;
        write_current_decision(&db, &config, &identity).await;
        let original = if foreign { foreign_route(&config) } else { config.pinning.providers[0].route_snapshot() };
        let first = insert_history(&db, "history-alias-one", &original).await;
        let second = insert_history(&db, "history-alias-two", &original).await;
        proof(&db, &raw, &config, &identity).await;
        let request = support::request(identity, &config, false); let before = native::capture(&db).await.unwrap();
        let snapshot = collect_with_config(&db, &config, &request).await.unwrap().planning_snapshot().unwrap();
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(plan.items()[0].status, if foreign { PlanStatus::Ready } else { PlanStatus::Blocked(SafeError::UnknownEvidence) });
        assert!(snapshot.historical_remotes.is_empty());
        for (alias, raw_history) in [("history-alias-one", first), ("history-alias-two", second)] {
            let key = NativeKey { table: "pin_resource_history".into(), values: vec![alias.into(), CID.into(), "1".into()] };
            assert!(snapshot.unknown_resources.contains(&key));
            assert!(snapshot.native_frame.as_ref().unwrap().frontier.contains(&key));
            assert_eq!(snapshot.native_frame.as_ref().unwrap().original_value("pin_resource_history", &[alias, CID, "1"], "ledger"), Some(raw_history.as_str()));
        }
        assert!(plan.summary().unknown_resources >= 2);
        unchanged(&db, &before, &snapshot).await;
    }
}

#[tokio::test]
async fn scope_isolation_real_confirmed_release_keeps_current_history_epoch_responsibility_without_global_error() {
    use ipfs_s3_gateway::store::pinning::quota::{self,ReservationOutcome,ConfirmedReleaseOutcome};
    let server=MockServer::start().await;source(&server).await;
    let raw=config(&server.uri());let config=management_config(&raw);
    let (_directory,db)=database().await;setup(&db,&config).await;
    let provider=&config.pinning.providers[0];let allocation=provider.identity.allocation_key();
    let released_cid=ipfs_s3_gateway::pinning::provider::canonical_resource_cid(CID).unwrap();
    let tx=db.begin().await.unwrap();
    let now=store::database_clock::database_now(&tx).await.unwrap();
    assert_eq!(quota::reserve_unique(&tx,&allocation,&released_cid,10,&config.pinning.provider_limits,now).await.unwrap(),ReservationOutcome::Reserved);
    let held=native::capture(&tx).await.unwrap();
    let remote=held.row("remote_pins",&[&allocation,&released_cid]).unwrap();
    let epoch=native::value(remote,"epoch").unwrap().parse::<i64>().unwrap();
    // This fresh fixture has no desired target: satisfy the real primitive's
    // caller-owned transaction precondition, not a simulated history SQL row.
    assert!(held.table("pin_lease_targets").unwrap().rows.is_empty());
    assert_eq!(quota::confirmed_release(&tx,&allocation,&released_cid,epoch,now).await.unwrap(),ConfirmedReleaseOutcome::Released);
    tx.commit().await.unwrap();
    let released=native::capture(&db).await.unwrap();
    let epoch_text=epoch.to_string();
    assert_eq!(native::value(released.row("remote_pins",&[&allocation,&released_cid]).unwrap(),"epoch"),Some(epoch_text.as_str()));
    assert_eq!(native::value(released.row("remote_pins",&[&allocation,&released_cid]).unwrap(),"status"),Some("absent"));
    assert!(released.row("pin_resource_history",&[&allocation,&released_cid,&epoch_text]).is_some());
    let other_cid="bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
    let identity=support::publish_cid(&db,&config,"different-content",other_cid,false).await;
    write_current_decision(&db,&config,&identity).await;proof(&db,&raw,&config,&identity).await;
    let request=support::request(identity,&config,false);let before=native::capture(&db).await.unwrap();
    let snapshot=collect_with_config(&db,&config,&request).await.unwrap().planning_snapshot().unwrap();
    let plan=dry_run(&request,&snapshot).unwrap();assert_eq!(plan.items()[0].status,PlanStatus::Ready);
    for key in [NativeKey{table:"remote_pins".into(),values:vec![allocation.clone(),released_cid.clone()]},NativeKey{table:"remote_pin_ledger".into(),values:vec![allocation.clone(),released_cid.clone()]},NativeKey{table:"pin_resource_history".into(),values:vec![allocation.clone(),released_cid.clone(),epoch_text.clone()]}] {
        assert!(snapshot.unknown_resources.contains(&key));assert!(snapshot.native_frame.as_ref().unwrap().frontier.contains(&key));
    }
    assert!(snapshot.remotes.is_empty());assert!(snapshot.historical_remotes.is_empty());
    unchanged(&db,&before,&snapshot).await;
    let retire=PlanRequest{plan_id:"released-retire".into(),items:vec![ItemRequest{item_id:"retire".into(),action:Action::Retire,subject:Subject::Provider{route:RouteIdentity::from_original(provider.route_snapshot())}}]};
    let retirement=collect_with_config(&db,&config,&retire).await.unwrap().planning_snapshot().unwrap();
    let retirement_plan=dry_run(&retire,&retirement).unwrap();
    let Some(PlannedStep::Retire(step))=&retirement_plan.items()[0].step else{panic!("retirement plan")};
    assert!(step.needs_attention);assert!(step.unknown>=3);assert!(retirement_plan.summary().unknown_resources>=3);
    unchanged(&db,&before,&retirement).await;
}
