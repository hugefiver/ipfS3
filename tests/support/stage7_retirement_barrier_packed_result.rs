//! Exercise the formal retirement receipt and allocation reader without a second codec.
use super::support;
use ipfs_s3_gateway::{
    error::AppError,
    pinning::{
        config::{LeaseDuration, ProviderMode},
        management::{self, Action, ItemRequest, PlanRequest, RouteIdentity, Subject},
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        entities::{object, pin_lease},
        pinning::{
            ledger,
            management::{actions, records},
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::EntityTrait;

#[tokio::test]
async fn retirement_receipt_restores_original_route_instead_of_becoming_unknown_attribution() {
    let mut fixture = support::Fixture::new().await;
    let storage = "retained-storage-domain-".repeat(10);
    fixture.raw.pinning_identity.providers[0].storage_domain = storage.clone();
    fixture.config = management::local_config::compile(&fixture.raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let provider = &fixture.config.pinning.providers[0];
    let route = provider.route_snapshot();
    ledger::register_route(&fixture.db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let request = PlanRequest {
        plan_id: "packed-retirement-result".into(),
        items: vec![ItemRequest {
            item_id: "retire-provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(route.clone()),
            },
        }],
    };
    let binding = fixture.confirm(&request).await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let stored = records::lookup_committed(&fixture.db, &fixture.auth, &binding, "retire-provider")
        .await
        .unwrap()
        .unwrap();
    let Subject::Provider { route: restored } = &stored.receipt.binding.request.subject else {
        panic!("retirement receipt lost its provider subject");
    };
    assert_eq!(restored.original.as_ref(), Some(&route));
    assert_eq!(restored.storage_domain, storage);
    let result = publication::publish_object(
        &fixture.db,
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "bucket",
                "closed-packed-domain",
                support::CID.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![LeaseIntent {
                    source: LeaseSource::Manual,
                    policy_id: fixture.config.pinning.policies[0].identity.clone(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider.name.clone()],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: support::CID.into(),
                logical_size: 10,
            },
        },
        &fixture.config.pinning.provider_limits,
    )
    .await;
    let error = match result {
        Ok(_) => panic!("closed packed domain admitted a new lease"),
        Err(error) => error,
    };
    assert!(
        matches!(error, AppError::InvalidPinningRequest(message) if message == "allocation domain is permanently retired"),
        "a complete formal receipt must be understood as retired, not corrupted/unknown attribution"
    );
    assert!(
        object::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_lease::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
}
