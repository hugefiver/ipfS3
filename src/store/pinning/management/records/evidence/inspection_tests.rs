use super::super::inspection;
use super::*;
use crate::pinning::management::{self as management, db_snapshot, wire};
use crate::store::pinning::management::actions::{self, provider_points};

#[tokio::test]
async fn typed_action_inspection_shares_remaining_work_and_rejects_substituted_original() {
    let fixture = actions::tests::fixture("inspection-result-budget").await;
    let applied = actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let domain = fixture.config.pinning.providers[0].name.clone();
    let namespace = provider_points::NamespaceChange {
        source: provider_points::Domain {
            allocation: domain.clone(),
            backend: route.backend.clone(),
            scope: route.scope.clone(),
            resource_type: route.resource_type(),
        },
        destination: provider_points::Domain {
            allocation: domain,
            backend: route.backend.clone(),
            scope: route.scope.clone(),
            resource_type: route.resource_type(),
        },
        destination_route: route,
    };
    let key = anchor_key(&fixture.binding).unwrap();
    let row = provider_points::original_record(&fixture.db, &key)
        .await
        .unwrap()
        .unwrap();
    let work = SharedDecodeBudget::default();
    work.lock()
        .unwrap()
        .consume(0, wire::MAX_WIRE_BYTES - 1)
        .unwrap();
    assert!(matches!(
        provider_points::inspect_with_work(&fixture.db, &key, &row, &namespace, work).await,
        Err(ManagementError::Budget)
    ));
    let inspected = provider_points::inspect(&fixture.db, &key, &row, &namespace)
        .await
        .unwrap();
    assert!(inspected.class() == provider_points::RecordClass::Applied);
    for authority in &applied.items[0].jobs {
        assert!(
            inspected
                .points()
                .iter()
                .any(|point| point.job_authority.as_ref() == Some(authority))
        );
    }
    let mut changed = row;
    changed.insert("result".into(), Some("{}".into()));
    assert!(matches!(
        provider_points::inspect(&fixture.db, &key, &changed, &namespace).await,
        Err(ManagementError::Changed)
    ));
}

#[tokio::test]
async fn typed_retirement_inspection_preserves_all_original_cells_and_rejects_wrong_disposition() {
    let fixture = actions::tests::fixture("inspection-retirement").await;
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let request = management::PlanRequest {
        plan_id: "inspection-retirement-plan".into(),
        items: vec![management::ItemRequest {
            item_id: "provider".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: management::RouteIdentity::from_original(route),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let prepared = management::retirement::PreparedPlanning::collect(
        &fixture.db,
        &request,
        &snapshot,
        SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let binding = super::super::confirm(
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
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    let domain = &fixture.config.pinning.providers[0].name;
    let row = frame
        .original_row(super::super::super::schema::RETIREMENTS, &[domain])
        .unwrap();
    let inspected = inspection::inspect_retirement_review_with_work(
        &fixture.db,
        &row,
        SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let expected = binding.review();
    assert!(
        inspected
            .rows()
            .all_rows()
            .map(|(table, row)| (&table.table, row))
            .eq(expected
                .rows()
                .all_rows()
                .map(|(table, row)| (&table.table, row)))
    );
    let mut changed = row;
    changed.insert("disposition".into(), Some("migrate".into()));
    assert!(matches!(
        inspection::inspect_retirement_review_with_work(
            &fixture.db,
            &changed,
            SharedDecodeBudget::default()
        )
        .await,
        Err(ManagementError::Confirmation)
    ));
}
