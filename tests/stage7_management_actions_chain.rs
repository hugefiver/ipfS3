#[path = "support/stage7_management_actions.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::{self, management::*},
    store::{
        self,
        entities::{
            pin_extension_decision, pin_job, pin_lease, pin_lease_requirement, pin_lease_target,
            pin_replica_observation,
        },
        pinning::management::{actions, native, records},
    },
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};

async fn apply_counted(
    fixture: &support::Fixture,
    binding: &records::ConfirmedPlan,
) -> Result<actions::ApplyResult, store::pinning::management::ManagementError> {
    let work = records::SharedDecodeBudget::default();
    let result = actions::apply_detailed_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        binding,
        work.clone(),
    )
    .await
    .map_err(|failure| failure.cause);
    eprintln!(
        "chain apply {} session={:?}",
        binding.request.plan_id,
        work.lock().unwrap().usage()
    );
    result
}

fn selected(
    plan: &str,
    identity: ExactObject,
    target: &pin_lease_target::Model,
    action: Action,
) -> PlanRequest {
    PlanRequest {
        plan_id: plan.into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action,
            subject: Subject::Object {
                identity,
                target: TargetSelection::Existing {
                    target_id: target.id.clone(),
                    source: None,
                },
            },
        }],
    }
}

async fn assert_submit_fact(fixture: &support::Fixture, job: &records::JobAuthority) {
    let fact = pin_replica_observation::Entity::find()
        .filter(pin_replica_observation::Column::JobId.eq(&job.job_id))
        .filter(pin_replica_observation::Column::Phase.eq("submit"))
        .one(&fixture.db)
        .await
        .unwrap()
        .expect("actual managed Submit must retain its invocation fact");
    assert_eq!(fact.provider, job.provider);
    assert_eq!(fact.expected_cid, job.cid);
    assert_eq!(fact.remote_epoch, job.remote_epoch);
    assert_eq!(fact.lease_id, job.lease_id);
    assert_eq!(fact.target_id, job.target_id);
    assert_eq!(fact.expected_generation, job.lease_generation);
    let route: pinning::identity::ProviderRouteSnapshot =
        serde_json::from_str(&fact.route).unwrap();
    assert!(route == job.route);
    assert_eq!(fact.outcome, "ReportedPinned");
    assert!(!fact.projectable);
    let resources: Vec<pinning::provider::ObservedResource> =
        serde_json::from_str(&fact.resources).unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].cid, job.cid);
    assert_eq!(resources[0].request_id, format!("noop:{}", job.cid));
    assert_eq!(
        resources[0].status,
        pinning::provider::ObservedResourceStatus::Reported
    );
    assert_eq!(
        resources[0].ownership,
        pinning::identity::Ownership::Unknown
    );
}

#[tokio::test]
async fn complete_same_file_business_chain_keeps_all_foreign_records_under_original_budget() {
    let mut fixture = support::Fixture::new().await;
    let identity = fixture.publish("chain", false).await;
    fixture.publish("private-inventory", false).await;
    fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"链_PRIVATE_METADATA\"}' WHERE key='private-inventory'; INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES('foreign-scope','foreign-plan','foreign-item','action','foreign-db','foreign-principal','foreign-binding','foreign-私密-before','foreign-result','[]','[]','applied',CURRENT_TIMESTAMP,datetime(CURRENT_TIMESTAMP,'+1 hour'),CURRENT_TIMESTAMP)").await.unwrap();
    let create_request = fixture.create_request("chain-create", identity.clone(), false);
    let create_snapshot =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &create_request)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    let create_plan = dry_run(&create_request, &create_snapshot).unwrap();
    eprintln!(
        "create statuses={:?}",
        create_plan
            .items()
            .iter()
            .map(|entry| (&entry.request.item_id, &entry.status))
            .collect::<Vec<_>>()
    );
    let create = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&create_request, &create_snapshot, &create_plan),
        vec!["object".into()],
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let create_review = create.review();
    let mut create_document = Vec::new();
    let mut export_session = records::ReadSession::default();
    wire::encode_review(
        &mut export_session,
        &create_review,
        create_review.candidate_item_ids(),
        &mut create_document,
    )
    .unwrap();
    eprintln!(
        "chain create borrowed export session={:?}",
        export_session.usage()
    );
    let created = apply_counted(&fixture, &create).await.unwrap();
    let lease_id = created.items[0].receipt.intent_ids[0].clone();
    fixture.settle_with_worker(&lease_id).await;
    let submit = created.items[0]
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap();
    assert_submit_fact(&fixture, submit).await;
    let original = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let original_contract = pin_lease_requirement::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap()
        .contract;
    let target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease_id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let renew = fixture
        .confirm(&selected(
            "chain-renew",
            identity.clone(),
            &target,
            Action::Renew {
                expires_at: original.expires_at.timestamp() + 300,
            },
        ))
        .await;
    apply_counted(&fixture, &renew).await.unwrap();
    let retry = fixture
        .confirm(&selected(
            "chain-retry",
            identity.clone(),
            &target,
            Action::Retry,
        ))
        .await;
    apply_counted(&fixture, &retry).await.unwrap();
    let reconcile = fixture
        .confirm(&selected(
            "chain-reconcile",
            identity.clone(),
            &target,
            Action::Reconcile,
        ))
        .await;
    let observed = apply_counted(&fixture, &reconcile).await.unwrap();
    assert_eq!(observed.items[0].state, records::ActionState::ReadOnly);
    assert!(
        observed.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );
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
    let migrate = fixture
        .confirm(&selected(
            "chain-migrate",
            identity.clone(),
            &target,
            Action::Migrate {
                destination: Box::new(destination.clone()),
            },
        ))
        .await;
    let first = apply_counted(&fixture, &migrate).await.unwrap();
    fixture.settle_with_worker(&lease_id).await;
    let first_point = &first.items[0].migration_points[0];
    let moved = pin_lease_target::Entity::find_by_id(first_point.destination_target_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.add_provider("third").await;
    let third = RouteIdentity::from_original(
        fixture
            .config
            .pinning
            .providers
            .iter()
            .find(|provider| provider.identity.provider_id == "third-provider")
            .unwrap()
            .route_snapshot(),
    );
    // Plan IDs deliberately sort before the first migration. Receipt linkage,
    // not SQL/UUID/lexical order, determines the immutable current chain tip.
    let remigration = selected(
        "aaa-chain-remigrate",
        identity.clone(),
        &moved,
        Action::Migrate {
            destination: Box::new(third),
        },
    );
    let remigration_snapshot =
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &remigration)
            .await
            .unwrap()
            .planning_snapshot()
            .unwrap();
    let remigration_plan = dry_run(&remigration, &remigration_snapshot).unwrap();
    let remigration_ack = remigration_plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect::<Vec<_>>();
    let archive = wire::encode_plan(
        &remigration,
        &remigration_snapshot,
        &remigration_plan,
        Some(&remigration_plan.confirmation(remigration_ack.clone())),
    )
    .unwrap();
    let imported_remigration = wire::decode_plan(&archive).unwrap();
    let migrate_again = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&remigration, &remigration_snapshot, &remigration_plan),
        remigration_ack,
        chrono::Duration::seconds(30),
    )
    .await
    .unwrap();
    let second = apply_counted(&fixture, &migrate_again).await.unwrap();
    fixture.settle_with_worker(&lease_id).await;
    assert!(second.items[0].migration_points[0].predecessor.is_some());
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .contract,
        original_contract
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .created_at,
        original.created_at
    );
    let cancelled = fixture
        .confirm(&selected(
            "chain-cancel",
            identity.clone(),
            &target,
            Action::Cancel,
        ))
        .await;
    apply_counted(&fixture, &cancelled).await.unwrap();
    let legacy = fixture.publish("legacy-output", false).await;
    let backfill = fixture
        .confirm(&fixture.create_request("chain-backfill", legacy, true))
        .await;
    apply_counted(&fixture, &backfill).await.unwrap();
    let skipped = fixture
        .publish_cid_with_tags(
            "skipped-output",
            support::CID,
            false,
            vec![pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")],
        )
        .await;
    let mut old = fixture.raw.clone();
    old.pinning.policies.clear();
    old.pinning_control.unavailable = ipfs_s3_gateway::config::OptionalPinControlMode::Warn;
    let old = local_config::compile(&old, |_| None).unwrap();
    let tags = [pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")];
    let (_, decision) = old
        .policy
        .evaluate_publication_decision(
            pinning::policy::PublicationContext {
                bucket: "bucket",
                key: "skipped-output",
                tags: &tags,
                is_decompress_zip: false,
            },
            pinning::decision::DecisionOrigin::new("fixture", "chain-skipped"),
        )
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    store::pinning::decision::write_for_object_in_transaction(&tx, &skipped.object_id, &decision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let old_json = pin_extension_decision::Entity::find_by_id(skipped.private_version_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap()
        .snapshot;
    let mut reapply = fixture.create_request("chain-reapply", skipped.clone(), false);
    let Action::Create(intent) = reapply.items[0].action.clone() else {
        unreachable!()
    };
    reapply.items[0].action = Action::Reapply(intent);
    let reapply = fixture.confirm(&reapply).await;
    apply_counted(&fixture, &reapply).await.unwrap();
    assert_eq!(
        pin_extension_decision::Entity::find_by_id(skipped.private_version_id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .snapshot,
        old_json
    );
    let retire = PlanRequest {
        plan_id: "chain-retire".into(),
        items: vec![ItemRequest {
            item_id: "domain".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    };
    let retired = fixture.confirm(&retire).await;
    let retirement_original = retired.review();
    apply_counted(&fixture, &retired).await.unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    frame.validate().unwrap();
    let (rows, bytes) = native::raw_inventory_usage(&fixture.db).await.unwrap();
    eprintln!(
        "complete same-file Stage7 business chain: complete registry raw UTF8 bytes={bytes}, rows={rows}; native MAX_BYTES={}, MAX_ROWS={}",
        native::MAX_BYTES,
        native::MAX_ROWS
    );
    assert!(bytes < native::MAX_BYTES);
    assert_eq!(
        frame
            .tables
            .iter()
            .map(|table| table.rows.len())
            .sum::<usize>(),
        rows
    );
    assert_eq!(
        frame
            .table("pin_management_records")
            .unwrap()
            .rows
            .iter()
            .filter(|row| native::value(*row, "database_scope")
                == Some(fixture.auth.database_scope())
                && native::value(*row, "kind") == Some("action")
                && native::value(*row, "state") == Some("applied"))
            .count(),
        10
    );
    fixture.db.clone().close().await.unwrap();
    let path = fixture.directory.path().join("management.sqlite");
    fixture.db = store::connect_database(&format!(
        "sqlite://{}?mode=rw",
        path.to_string_lossy().replace('\\', "/")
    ))
    .await
    .unwrap();
    fixture.auth = records::authorize_local(&fixture.raw, &fixture.db)
        .await
        .unwrap();
    fixture.raw.pinning_identity.providers[0].display_name = "Changed after retirement".into();
    fixture.config = local_config::compile(&fixture.raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let expires_at = create.expires_at.max(migrate_again.expires_at);
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    if let Ok(wait) = (expires_at - now + chrono::Duration::seconds(1)).to_std() {
        tokio::time::sleep(wait).await;
    }
    assert!(create.check(&fixture.db, &fixture.auth).await.is_err());
    assert!(
        migrate_again
            .check(&fixture.db, &fixture.auth)
            .await
            .is_err()
    );
    let imported_create = wire::decode_plan(&create_document).unwrap();
    let restored_create =
        records::load_confirmed_from_imported(&fixture.db, &fixture.auth, &imported_create)
            .await
            .unwrap();
    assert!(restored_create == create);
    // Exact durable replay survives the changed config, cancelled lease and
    // retired domain without authorizing any new object/pin or recapturing IDs.
    let replay = actions::apply_detailed_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &restored_create,
        imported_create.session(),
    )
    .await
    .map_err(|failure| failure.cause)
    .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == created.items);
    eprintln!(
        "chain original create decode/import/replay session={:?}",
        imported_create.session().lock().unwrap().usage()
    );
    // The original imported document contains the prior derived slot proof.
    // Later remigration, cancellation and retirement cannot replace that proof
    // with today's route/configuration or turn public points into authority.
    let restored =
        records::load_confirmed_from_imported(&fixture.db, &fixture.auth, &imported_remigration)
            .await
            .unwrap();
    assert!(restored == migrate_again);
    assert!(
        actions::apply_detailed_with_session(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &restored,
            imported_remigration.session()
        )
        .await
        .map_err(|failure| failure.cause)
        .unwrap()
        .replayed
    );
    eprintln!(
        "chain original remigration decode/import/replay session={:?}",
        imported_remigration.session().lock().unwrap().usage()
    );
    let read_session = records::SharedDecodeBudget::default();
    let inventory = records::read_retirement_with_session(
        &fixture.db,
        &fixture.auth,
        &fixture.config.pinning.providers[0].name,
        read_session.clone(),
    )
    .await
    .unwrap()
    .unwrap()
    .inventory;
    eprintln!(
        "chain retirement restore session={:?}",
        read_session.lock().unwrap().usage()
    );
    let mut original_cells = 0;
    retirement_original.visit_original_cells(|table, key, column, value| {
        assert_eq!(
            inventory
                .original_cell(table, key, column)
                .unwrap()
                .as_deref(),
            value
        );
        original_cells += 1;
    });
    let mut restored_cells = 0;
    inventory.visit_original_cells(|_, _, _, _| {
        restored_cells += 1;
    });
    assert_eq!(original_cells, restored_cells);
    assert!(
        inventory.original_value(
            "pin_management_records",
            &["foreign-scope", "foreign-plan", "foreign-item", "action"],
            "native_before"
        ) == Some("foreign-私密-before")
    );
    let mut bytes = Vec::new();
    let mut session = records::ReadSession::default();
    wire::encode_review(
        &mut session,
        &inventory,
        inventory.candidate_item_ids(),
        &mut bytes,
    )
    .unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("链_PRIVATE_METADATA"));
    let session = std::sync::Arc::new(std::sync::Mutex::new(session));
    wire::decode_review(&bytes, session.clone()).unwrap();
    eprintln!(
        "chain retirement borrowed export/decode session={:?},cells={original_cells}",
        session.lock().unwrap().usage()
    );
    let mut changed: serde_json::Value = serde_json::from_slice(&archive).unwrap();
    let points = changed["migration_points"].as_array_mut().unwrap();
    assert!(!points.is_empty());
    let epoch = points[0]["destination_epoch"].as_i64().unwrap();
    points[0]["destination_epoch"] = serde_json::json!(epoch + 1);
    if let Ok(forged) = wire::decode_plan(&serde_json::to_vec(&changed).unwrap()) {
        assert!(
            records::load_confirmed_from_imported(&fixture.db, &fixture.auth, &forged)
                .await
                .is_err()
        );
    }
    assert!(frame == native::capture(&fixture.db).await.unwrap());
}

#[tokio::test]
async fn current_status_is_actual_worker_evidence_and_expired_facts_cannot_revive_the_lease() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("real-worker", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("worker-birth", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let lease = &applied.items[0].receipt.intent_ids[0];
    fixture.settle_with_worker(lease).await;
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::RemoteComplete
    );
    let job = pin_job::Entity::find_by_id(applied.items[0].receipt.job_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let submit = applied.items[0]
        .jobs
        .iter()
        .find(|authority| authority.job_id == job.id)
        .unwrap();
    assert_submit_fact(&fixture, submit).await;
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture
        .db
        .execute(sea_orm::Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_leases SET expires_at=$1 WHERE id=$2",
            [now.into(), lease.clone().into()],
        ))
        .await
        .unwrap();
    assert_eq!(
        actions::status::lookup(&fixture.db, &fixture.auth, &binding, "object")
            .await
            .unwrap()
            .unwrap()
            .state,
        records::ActionState::NeedsAttention
    );
    assert!(matches!(
        actions::job_authority::check_before_call(&fixture.db, &job)
            .await
            .unwrap(),
        Some(actions::job_authority::JobPermit::LedgerOnly(_))
    ));
    assert_eq!(
        pin_lease::Entity::find_by_id(lease.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        now
    );
}
