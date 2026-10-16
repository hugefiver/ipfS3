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
use sea_orm::{ConnectionTrait, EntityTrait, Statement, TransactionTrait};

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
    let confirmed = fixture.confirm(&request).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed)
        .await
        .unwrap();
    let row=fixture.db.query_one(Statement::from_sql_and_values(fixture.db.get_database_backend(),"SELECT length(CAST(native_before AS BLOB)) AS bytes FROM pin_management_records WHERE plan_id=$1 AND kind='action'",[request.plan_id.clone().into()])).await.unwrap().unwrap();
    let minimum = usize::try_from(row.try_get::<i64>("", "bytes").unwrap()).unwrap();
    assert!(minimum > 1);
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    for limited in [1, 2, 0] {
        let work = records::SharedDecodeBudget::default();
        if limited == 1 {
            work.lock()
                .unwrap()
                .consume(0, wire::MAX_WIRE_BYTES - minimum + 1)
                .unwrap();
        }
        if limited == 2 {
            work.lock()
                .unwrap()
                .consume(wire::MAX_WIRE_NODES - 1, 0)
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
            assert!(
                work.lock().unwrap().usage().1 >= minimum,
                "retirement SQL delivery and validation were not charged to the caller"
            );
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
    eprintln!(
        "post-retirement other-domain managed allocation session={:?}",
        work.lock().unwrap().usage()
    );
}
