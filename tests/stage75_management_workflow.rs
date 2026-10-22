#[path = "support/stage75_workflow_inventory.rs"]
mod inventory;
#[path = "support/stage75_workflow.rs"]
pub mod support;

use ipfs_s3_gateway::{
    pinning::{self, management::*},
    store::{
        self,
        entities::{
            pin_extension_decision, pin_job, pin_lease, pin_lease_requirement, pin_lease_target,
            pin_provider_usage,
        },
        pinning::management::records,
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

#[tokio::test]
async fn provider_confirmation_apply_current_status_and_v3_replay_use_the_full_inventory() {
    let fixture = support::existing::psa_fixture().await;
    let request = PlanRequest {
        plan_id: "provider-lifecycle".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
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
    let document = support::export_global(&fixture, &binding).await;
    let applied = support::apply(&fixture, &binding).await;
    let mut workspace = retirement::Workspace::create().unwrap();
    let (path, file) = workspace.reserve().await.unwrap();
    let mut limits = retirement::RunLimits::default();
    limits.pages = 512;
    let mut status = store::pinning::management::actions::status::lookup_retirement_with_work(
        &fixture.db,
        &fixture.auth,
        &binding,
        "retire",
        &path,
        file,
        &limits,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap()
    .unwrap();
    let current = status.advance(&fixture.db, &limits).await.unwrap().unwrap();
    assert_eq!(current.state, records::ActionState::Accepted);
    status.close().await.unwrap();
    workspace.cleanup().unwrap();
    support::replay(&fixture, &binding, &applied, &document).await;
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn populated_provider_retirement_preserves_unknown_originals_and_rejects_unsealed_late_authority()
 {
    for dirty_authority in [false, true] {
        let fixture = support::existing::psa_fixture().await;
        let identity = fixture.publish("retirement-owner", false).await;
        let birth = fixture
            .confirm(&fixture.create_request("retirement-owner", identity, false))
            .await;
        support::apply(&fixture, &birth).await;
        let calls = fixture.server.received_requests().await.unwrap().len();
        let request = PlanRequest {
            plan_id: "retirement-populated".into(),
            items: vec![ItemRequest {
                item_id: "retire".into(),
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
        let planning = inventory::plan_and_restore(&fixture, &request).await;
        if dirty_authority {
            fixture.db.execute_unprepared("INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES('foreign-workflow','foreign-plan','foreign-item','action','foreign-db','foreign-principal','foreign-binding','历史_PRIVATE_ORIGINAL','foreign-result','[]','[]','applied',CURRENT_TIMESTAMP,datetime(CURRENT_TIMESTAMP,'+1 hour'),CURRENT_TIMESTAMP)").await.unwrap();
        }
        let binding = records::confirm_with_session(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            (&request, &planning.snapshot, &planning.prepared.plan),
            &[],
            chrono::Duration::minutes(5),
            planning.work.clone(),
        )
        .await;
        if dirty_authority {
            assert!(matches!(
                binding,
                Err(store::pinning::management::ManagementError::Changed)
            ));
            let count = fixture
                .db
                .query_one(sea_orm::Statement::from_string(
                    fixture.db.get_database_backend(),
                    "SELECT COUNT(*) AS n FROM pin_management_retirements",
                ))
                .await
                .unwrap()
                .unwrap()
                .try_get::<i64>("", "n")
                .unwrap();
            assert_eq!(count, 0);
            assert_eq!(
                fixture.server.received_requests().await.unwrap().len(),
                calls
            );
            planning.close().await;
            continue;
        }
        let binding = binding.unwrap();
        let applied = support::apply(&fixture, &binding).await;
        assert!(applied.items[0].receipt.job_ids.is_empty());
        support::replay(
            &fixture,
            &binding,
            &applied,
            &support::export_global(&fixture, &binding).await,
        )
        .await;
        inventory::assert_frozen(&fixture, &planning.inventory).await;
        assert_eq!(
            fixture.server.received_requests().await.unwrap().len(),
            calls
        );
        planning.close().await;
    }
}

#[tokio::test]
async fn nine_actions_ten_applies_three_receipt_scoped_http_executions_and_durable_recovery() {
    let mut fixture = support::existing::psa_fixture().await;
    let identity = fixture.publish("workflow", false).await;
    fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"工作流_PRIVATE_ORIGINAL\"}' WHERE key='workflow'")
        .await.unwrap();
    support::prepare(&fixture, identity.clone()).await;
    let request = fixture.create_request("workflow-create", identity.clone(), false);
    let birth =
        support::existing::confirm_with_ttl(&fixture, &request, chrono::Duration::seconds(30))
            .await;
    let birth_document = support::export(&birth);
    assert!(!String::from_utf8_lossy(&birth_document).contains("工作流_PRIVATE_ORIGINAL"));
    let created = support::apply(&fixture, &birth).await;
    support::settle(&fixture, &birth, &created).await;
    let lease_id = created.items[0].receipt.intent_ids[0].clone();
    let source_id = created.items[0].targets[0].clone();
    let original = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let contract = pin_lease_requirement::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap()
        .contract;

    let renewed_until = original.expires_at.timestamp() + 300;
    let renew = fixture
        .confirm(&support::selected(
            "workflow-renew",
            identity.clone(),
            &source_id,
            Action::Renew {
                expires_at: renewed_until,
            },
        ))
        .await;
    let renewed = support::apply(&fixture, &renew).await;
    let lease = pin_lease::Entity::find_by_id(lease_id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.expires_at.timestamp(), renewed_until);
    assert_eq!(lease.created_at, original.created_at);
    assert_eq!(lease.owner_object_id, original.owner_object_id);
    assert!(renewed.items[0].receipt.intent_ids.contains(&lease_id));

    let retry = fixture
        .confirm(&support::selected(
            "workflow-retry",
            identity.clone(),
            &source_id,
            Action::Retry,
        ))
        .await;
    let retried = support::apply(&fixture, &retry).await;
    let reconcile = fixture
        .confirm(&support::selected(
            "workflow-reconcile",
            identity.clone(),
            &source_id,
            Action::Reconcile,
        ))
        .await;
    let reconciled = support::apply(&fixture, &reconcile).await;
    assert_eq!(reconciled.items[0].state, records::ActionState::ReadOnly);
    assert!(
        reconciled.items[0]
            .jobs
            .iter()
            .all(|job| job.directive == records::JobDirective::ReadOnlyObserve)
    );

    let destination = support::add_provider(&mut fixture, "destination").await;
    let migrate = fixture
        .confirm(&support::selected(
            "workflow-migrate",
            identity.clone(),
            &source_id,
            Action::Migrate {
                destination: Box::new(destination),
            },
        ))
        .await;
    let migrated = support::apply(&fixture, &migrate).await;
    support::settle(&fixture, &migrate, &migrated).await;
    let first_point = &migrated.items[0].migration_points[0];
    assert_eq!(first_point.source_target_id, source_id);
    let third = support::add_provider(&mut fixture, "third").await;
    let request = support::selected(
        "aaa-workflow-remigrate",
        identity.clone(),
        &first_point.destination_target_id,
        Action::Migrate {
            destination: Box::new(third),
        },
    );
    let remigrate =
        support::existing::confirm_with_ttl(&fixture, &request, chrono::Duration::seconds(30))
            .await;
    let remigration_document = support::export(&remigrate);
    let remigrated = support::apply(&fixture, &remigrate).await;
    support::settle(&fixture, &remigrate, &remigrated).await;
    let second_point = &remigrated.items[0].migration_points[0];
    assert!(second_point.predecessor.is_some());
    assert_eq!(
        second_point.source_target_id,
        first_point.destination_target_id
    );
    assert_eq!(second_point.lease_id, lease_id);
    assert_eq!(
        pin_lease_requirement::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .contract,
        contract
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

    let cancel = fixture
        .confirm(&support::selected(
            "workflow-cancel",
            identity.clone(),
            &source_id,
            Action::Cancel,
        ))
        .await;
    let cancelled = support::apply(&fixture, &cancel).await;
    assert_eq!(
        pin_lease::Entity::find_by_id(lease_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
    for id in [
        &source_id,
        &first_point.destination_target_id,
        &second_point.destination_target_id,
    ] {
        assert!(
            pin_lease_target::Entity::find_by_id(id.clone())
                .one(&fixture.db)
                .await
                .unwrap()
                .is_some()
        );
    }
    let legacy = fixture.publish("legacy-output", false).await;
    let backfill = fixture
        .confirm(&fixture.create_request("workflow-backfill", legacy, true))
        .await;
    let backfilled = support::apply(&fixture, &backfill).await;

    let skipped = fixture
        .publish_cid_with_tags(
            "skipped-output",
            support::existing::CID,
            false,
            vec![pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")],
        )
        .await;
    let mut old = fixture.raw.clone();
    old.pinning.policies.clear();
    old.pinning_control.unavailable = ipfs_s3_gateway::config::OptionalPinControlMode::Warn;
    let old = local_config::compile(&old, |_| Some("controlled-worker-token".into())).unwrap();
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
            pinning::decision::DecisionOrigin::new("workflow", "skipped"),
        )
        .unwrap();
    assert_eq!(decision.effect, pinning::decision::DecisionEffect::Skipped);
    let tx = fixture.db.begin().await.unwrap();
    store::pinning::decision::write_for_object_in_transaction(&tx, &skipped.object_id, &decision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let historical_decision =
        pin_extension_decision::Entity::find_by_id(skipped.private_version_id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .snapshot;
    support::prepare(&fixture, skipped.clone()).await;
    let mut request = fixture.create_request("workflow-reapply", skipped.clone(), false);
    let Action::Create(intent) = request.items[0].action.clone() else {
        unreachable!()
    };
    request.items[0].action = Action::Reapply(intent);
    let reapply = fixture.confirm(&request).await;
    let reapplied = support::apply(&fixture, &reapply).await;
    assert_eq!(
        pin_extension_decision::Entity::find_by_id(skipped.private_version_id)
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .snapshot,
        historical_decision
    );

    let committed = [
        (&birth, &created),
        (&renew, &renewed),
        (&retry, &retried),
        (&reconcile, &reconciled),
        (&migrate, &migrated),
        (&remigrate, &remigrated),
        (&cancel, &cancelled),
        (&backfill, &backfilled),
        (&reapply, &reapplied),
    ];
    let documents: Vec<_> = committed
        .iter()
        .map(|(binding, _)| support::export(binding))
        .collect();
    fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"工作流_PRIVATE_CURRENT\"}' WHERE key='workflow'")
        .await.unwrap();
    support::reopen(&mut fixture).await;
    fixture.raw.pinning_identity.providers[0].display_name =
        "Current configuration after restart".into();
    fixture.config = support::compile(&fixture);
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    if let Ok(wait) = (birth.expires_at.max(remigrate.expires_at) - now
        + chrono::Duration::milliseconds(50))
    .to_std()
    {
        tokio::time::sleep(wait).await;
    }
    assert!(birth.check(&fixture.db, &fixture.auth).await.is_err());
    assert!(remigrate.check(&fixture.db, &fixture.auth).await.is_err());
    support::replay(&fixture, &birth, &created, &birth_document).await;
    support::replay(&fixture, &remigrate, &remigrated, &remigration_document).await;
    for ((binding, result), document) in committed.iter().zip(&documents) {
        support::replay(&fixture, binding, result, document).await;
    }
    let restored = records::load_confirmed_authority(
        &fixture.db,
        &fixture.auth,
        &birth.request,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        restored
            .review()
            .original_cell("objects", &[identity.object_id], "metadata")
            .unwrap()
            .as_deref(),
        Some("{\"private\":\"工作流_PRIVATE_ORIGINAL\"}")
    );
    assert_eq!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method.as_str() == "POST" && r.url.path().ends_with("/pins"))
            .count(),
        3
    );

    let retirement = PlanRequest {
        plan_id: "workflow-retire".into(),
        items: vec![ItemRequest {
            item_id: "domain".into(),
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
    let planning = inventory::plan_and_restore(&fixture, &retirement).await;
    let retired = records::confirm_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&retirement, &planning.snapshot, &planning.prepared.plan),
        &[],
        chrono::Duration::minutes(5),
        planning.work.clone(),
    )
        .await
        .expect("sealed provider inventory must support durable confirmation through the management entry point");
    let retirement_result = support::apply(&fixture, &retired).await;
    assert!(retirement_result.items[0].receipt.job_ids.is_empty());
    let stored = records::read_retirement(
        &fixture.db,
        &fixture.auth,
        &fixture.config.pinning.providers[0].name,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(stored.disposition, records::Disposition::Retain);
    assert!(
        store::entities::pin_provider_route::Entity::find_by_id(
            fixture.config.pinning.providers[0].name.clone()
        )
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap()
        .retired
    );
    assert_eq!(committed.len() + retirement_result.items.len(), 10);
    support::reopen(&mut fixture).await;
    support::replay(
        &fixture,
        &retired,
        &retirement_result,
        &support::export_global(&fixture, &retired).await,
    )
    .await;
    inventory::assert_frozen(&fixture, &planning.inventory).await;
    planning.close().await;
}

#[tokio::test]
async fn same_plan_shared_cid_owners_replay_without_duplicate_allocation_or_submission() {
    let mut fixture = support::existing::psa_fixture().await;
    let first = fixture.publish("first-owner", false).await;
    let second = fixture.publish("second-owner", false).await;
    assert_eq!(first.cid, second.cid);
    assert_ne!(first.object_id, second.object_id);
    let mut request = fixture.create_request("same-plan", first.clone(), false);
    request.items[0].item_id = "z-first".into();
    let mut next = fixture
        .create_request("same-plan", second.clone(), false)
        .items
        .remove(0);
    next.item_id = "a-second".into();
    request.items.push(next);
    let binding = fixture.confirm(&request).await;
    let document = support::export(&binding);
    let applied = support::apply(&fixture, &binding).await;
    assert_eq!(applied.items.len(), 2);
    let leases = pin_lease::Entity::find().all(&fixture.db).await.unwrap();
    assert_eq!(leases.len(), 2);
    assert!(
        leases
            .iter()
            .any(|lease| lease.owner_object_id == first.object_id)
    );
    assert!(
        leases
            .iter()
            .any(|lease| lease.owner_object_id == second.object_id)
    );
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    assert_eq!(
        jobs.iter().filter(|job| job.operation == "submit").count(),
        1
    );
    let usage =
        pin_provider_usage::Entity::find_by_id(fixture.config.pinning.providers[0].name.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(usage.reserved_bytes, 10);
    assert_eq!(usage.reserved_pins, 1);
    support::reopen(&mut fixture).await;
    support::replay(&fixture, &binding, &applied, &document).await;
    assert!(
        fixture
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| !r.url.path().ends_with("/pins"))
    );
}

#[tokio::test]
async fn zero_job_quota_waiting_cancel_keeps_live_owners_and_does_not_drain_other_plans() {
    let mut fixture = support::existing::psa_fixture().await;
    let first = fixture.publish("live-owner", false).await;
    let birth = fixture
        .confirm(&fixture.create_request("live-owner", first.clone(), false))
        .await;
    let applied = support::apply(&fixture, &birth).await;
    let unrelated = fixture
        .publish_cid(
            "unrelated",
            "bafkreigh2akiscaildcqg2tnkyg4v3v55afkjp4hoyp5vzo5ibxq3d5x4q",
            false,
        )
        .await;
    let other = fixture
        .confirm(&fixture.create_request("unrelated", unrelated, false))
        .await;
    let other_applied = support::apply(&fixture, &other).await;
    let other_job = pin_job::Entity::find_by_id(other_applied.items[0].receipt.job_ids[0].clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    support::settle(&fixture, &birth, &applied).await;
    assert_eq!(
        pin_job::Entity::find_by_id(other_job.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        other_job
    );
    fixture.raw.pinning.providers[0].max_pins = 2;
    fixture.config = local_config::compile(&fixture.raw, |_| Some("local-token".into()))
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let source_cid = cid::Cid::try_from(applied.items[0].jobs[0].cid.as_str()).unwrap();
    let waiting_cid = cid::Cid::new_v1(0x55, *source_cid.hash()).to_string();
    let shared = fixture
        .publish_cid("quota-waiting-owner", &waiting_cid, false)
        .await;
    let shared_birth = fixture
        .confirm(&fixture.create_request("quota-waiting-owner", shared.clone(), false))
        .await;
    let before_birth_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let before_birth_calls = fixture.server.received_requests().await.unwrap().len();
    let shared_applied = support::apply(&fixture, &shared_birth).await;
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        before_birth_jobs
    );
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        before_birth_calls
    );
    assert_eq!(shared_applied.items[0].receipt.intent_ids.len(), 1);
    assert_eq!(shared_applied.items[0].targets.len(), 1);
    for authority in &shared_applied.items[0].jobs {
        let original = before_birth_jobs
            .iter()
            .find(|job| job.id == authority.job_id)
            .expect("zero-job allocation may only reference an existing job");
        assert_eq!(authority.provider, original.provider);
        assert_eq!(authority.cid, original.cid);
        assert_eq!(Some(authority.remote_epoch), original.expected_remote_epoch);
        assert_eq!(authority.directive, records::JobDirective::ReadOnlyObserve);
    }
    assert_eq!(
        pin_lease_target::Entity::find_by_id(shared_applied.items[0].targets[0].clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "quota_waiting"
    );
    assert!(
        shared_applied.items[0]
            .jobs
            .iter()
            .all(|job| job.directive != records::JobDirective::Submit)
    );
    let cancel = fixture
        .confirm(&support::selected(
            "cancel-waiting",
            shared,
            &shared_applied.items[0].targets[0],
            Action::Cancel,
        ))
        .await;
    let before_jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let before_cancel_calls = fixture.server.received_requests().await.unwrap().len();
    let result = support::apply(&fixture, &cancel).await;
    assert!(result.items[0].receipt.job_ids.is_empty());
    assert!(result.items[0].jobs.is_empty());
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        before_jobs
    );
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        before_cancel_calls
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(applied.items[0].receipt.intent_ids[0].clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "active"
    );
    support::assert_complete(&fixture, &birth, &applied.items[0].receipt.intent_ids[0]).await;
    let document = support::export(&cancel);
    support::replay(&fixture, &cancel, &result, &document).await;
    let scope =
        pinning::worker::ManagementJobScope::from_applied(&fixture.db, &fixture.auth, &cancel)
            .await
            .unwrap();
    let calls = fixture.server.received_requests().await.unwrap().len();
    support::existing::coordinator(&fixture)
        .management_tick(
            &store::Store::new(fixture.db.clone()),
            &scope,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        calls
    );
    assert_eq!(
        pin_job::Entity::find_by_id(other_job.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        other_job
    );
}
