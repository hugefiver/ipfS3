use crate::stage7_management_test_support as support;
use crate::{
    pinning::management::*,
    store::{
        self,
        entities::remote_pin,
        pinning::{
            management::{actions, records},
            quota,
        },
    },
};
use sea_orm::{EntityTrait, TransactionTrait};

#[tokio::test]
async fn ordinary_admission_and_registration_do_not_spend_their_budget_on_same_domain_actions() {
    let fixture = support::Fixture::new().await;
    let provider = &fixture.config.pinning.providers[0].name;
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let mut required = Vec::new();
    for registration in [false, true] {
        let baseline = records::SharedDecodeBudget::default();
        if registration {
            assert!(
                !super::super::registration_closed_with_work(
                    &fixture.db,
                    &route,
                    records::DecodeWork::shared(&baseline),
                )
                .await
                .unwrap()
            );
        } else {
            super::super::check_provider_with_work(
                &fixture.db,
                provider,
                records::DecodeWork::shared(&baseline),
            )
            .await
            .unwrap();
        }
        required.push((registration, baseline.lock().unwrap().usage()));
    }
    let identity = fixture.publish("same-domain-action", false).await;
    let binding = fixture
        .confirm(&fixture.create_request("same-domain-action", identity, false))
        .await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(applied.items[0].jobs.iter().any(|job| {
        job.directive == records::JobDirective::Submit && job.provider == *provider
    }));
    for (registration, required) in required {
        for node_limit in [true, false] {
            let work = records::SharedDecodeBudget::default();
            work.lock()
                .unwrap()
                .consume(
                    if node_limit {
                        wire::MAX_WIRE_NODES - required.0
                    } else {
                        0
                    },
                    if node_limit {
                        0
                    } else {
                        wire::MAX_WIRE_BYTES - required.1
                    },
                )
                .unwrap();
            if registration {
                assert!(
                    !super::super::registration_closed_with_work(
                        &fixture.db,
                        &route,
                        records::DecodeWork::shared(&work),
                    )
                    .await
                    .unwrap()
                );
            } else {
                super::super::check_provider_with_work(
                    &fixture.db,
                    provider,
                    records::DecodeWork::shared(&work),
                )
                .await
                .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn post_retirement_other_domain_allocation_uses_the_callers_remaining_budget() {
    let mut fixture = support::Fixture::new().await;
    fixture.add_provider("open-domain").await;
    let retired = fixture.config.pinning.providers[0].clone();
    let open = fixture
        .config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.name != retired.name)
        .unwrap()
        .clone();
    let request = PlanRequest {
        plan_id: "retired-before-other-allocation".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(retired.route_snapshot()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = crate::pinning::management::retirement::PreparedPlanning::collect(
        &fixture.db,
        &request,
        &snapshot,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let plan = &prepared.plan;
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_))),
        "{:?}",
        plan.items()
            .iter()
            .map(|entry| &entry.status)
            .collect::<Vec<_>>()
    );
    let acknowledgements = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let confirmed = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, plan),
        acknowledgements,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    prepared.close().await.unwrap();
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    let baseline = records::SharedDecodeBudget::default();
    let tx = fixture.db.begin().await.unwrap();
    quota::reserve_unique_with_work(
        &tx,
        &open.name,
        support::CID,
        10,
        &fixture.config.pinning.provider_limits,
        now,
        records::DecodeWork::shared(&baseline),
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let required = baseline.lock().unwrap().usage();
    assert!(required.0 > 0 && required.1 > 0);
    for limited in [1, 2, 0] {
        let work = records::SharedDecodeBudget::default();
        if limited == 1 {
            work.lock()
                .unwrap()
                .consume(0, wire::MAX_WIRE_BYTES - required.1 + 1)
                .unwrap();
        }
        if limited == 2 {
            work.lock()
                .unwrap()
                .consume(wire::MAX_WIRE_NODES - required.0 + 1, 0)
                .unwrap();
        }
        let tx = fixture.db.begin().await.unwrap();
        let result = quota::reserve_unique_with_work(
            &tx,
            &open.name,
            support::CID,
            10,
            &fixture.config.pinning.provider_limits,
            now,
            records::DecodeWork::shared(&work),
        )
        .await;
        if limited != 0 {
            assert!(
                matches!(result, Err(crate::error::AppError::ManagementBudget)),
                "allocation bypassed or hid the caller's budget: {result:?}, {:?}",
                work.lock().unwrap().usage()
            );
        } else {
            assert!(matches!(
                result.unwrap(),
                quota::ReservationOutcome::Reserved
            ));
            assert!(work.lock().unwrap().usage().1 >= required.1);
        }
        tx.rollback().await.unwrap();
        assert!(
            remote_pin::Entity::find_by_id((open.name.clone(), support::CID.into()))
                .one(&fixture.db)
                .await
                .unwrap()
                .is_none()
        );
    }
    let tx = fixture.db.begin().await.unwrap();
    let work = records::SharedDecodeBudget::default();
    assert!(matches!(
        quota::reserve_unique_with_work(
            &tx,
            &retired.name,
            support::CID,
            10,
            &fixture.config.pinning.provider_limits,
            now,
            records::DecodeWork::shared(&work)
        )
        .await,
        Err(crate::error::AppError::InvalidPinningRequest(_))
    ));
    tx.rollback().await.unwrap();

    fixture.raw.pinning.policies[0].providers = vec!["open-domain".into()];
    let node = fixture.config.source_node().unwrap().to_owned();
    fixture.config = local_config::compile(&fixture.raw, |_| None)
        .unwrap()
        .with_source_node(node)
        .unwrap();
    let identity = fixture.publish("after-retirement", false).await;
    let mut request = fixture.create_request("other-domain-after-retirement", identity, false);
    let Subject::Object { target, .. } = &mut request.items[0].subject else {
        unreachable!()
    };
    *target = TargetSelection::New(RouteIdentity::from_original(open.route_snapshot()));
    let confirmed = fixture.confirm(&request).await;
    let work = records::SharedDecodeBudget::default();
    let applied = actions::apply_detailed_with_session(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &confirmed,
        work.clone(),
    )
    .await
    .map_err(|failure| failure.cause)
    .unwrap();
    assert_eq!(applied.items[0].receipt.intent_ids.len(), 1);
    let remote = remote_pin::Entity::find_by_id((open.name.clone(), support::CID.into()))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote.status, "reserved");
}
