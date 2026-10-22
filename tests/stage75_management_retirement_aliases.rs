#[path = "support/stage7_management_actions.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    pinning::management::{Action, ItemRequest, PlanRequest, RouteIdentity, Subject},
    store::{
        entities::pin_provider_route,
        pinning::{
            ledger,
            management::{ManagementError, actions},
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};

#[tokio::test]
async fn retirement_stops_every_original_route_in_the_canonical_domain() {
    let fixture = support::Fixture::new().await;
    let original = &fixture.config.pinning.providers[0];
    let mut alias = original.identity.clone();
    alias.provider_id = "historical-alias".into();
    ledger::register_route(&fixture.db, "historical-allocation", &alias)
        .await
        .unwrap();
    let mut unrelated = alias.clone();
    unrelated.provider_id = "other-account".into();
    unrelated.scope = "separate-account".into();
    let unrelated_key = unrelated.allocation_key();
    ledger::register_route(&fixture.db, &unrelated_key, &unrelated)
        .await
        .unwrap();
    let before = pin_provider_route::Entity::find()
        .all(&fixture.db)
        .await
        .unwrap();
    let request = PlanRequest {
        plan_id: "retire-canonical-aliases".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(original.route_snapshot()),
            },
        }],
    };
    let binding = fixture.confirm(&request).await;
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(!applied.replayed);
    for mut expected in before {
        expected.retired = expected.provider != unrelated_key;
        let actual = pin_provider_route::Entity::find_by_id(expected.provider.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual, expected);
    }
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == applied.items);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn alias_published_after_confirmation_rejects_retirement_without_a_prefix() {
    let fixture = support::Fixture::new().await;
    let original = &fixture.config.pinning.providers[0];
    let request = PlanRequest {
        plan_id: "retire-before-alias".into(),
        items: vec![ItemRequest {
            item_id: "retire".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(original.route_snapshot()),
            },
        }],
    };
    let binding = fixture.confirm(&request).await;
    let mut alias = original.identity.clone();
    alias.provider_id = "late-alias".into();
    ledger::register_route(&fixture.db, "late-allocation", &alias)
        .await
        .unwrap();
    assert!(matches!(
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding).await,
        Err(ManagementError::Changed)
    ));
    assert!(
        pin_provider_route::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .iter()
            .all(|route| !route.retired)
    );
    let markers = fixture
        .db
        .query_one(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT COUNT(*) AS n FROM pin_management_retirements",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(markers.try_get::<i64>("", "n").unwrap(), 0);
    assert!(fixture.server.received_requests().await.unwrap().is_empty());
}
