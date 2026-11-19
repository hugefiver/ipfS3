use super::*;
use crate::{
    pinning::{
        config::{LeaseDuration, ProviderMode},
        policy::{LeaseIntent, LeaseSource},
        tags::ContentMode,
    },
    store::pinning::publication::{
        self, DecidedPublish, PinTargetSpec, PublicationObject, PublicationRequest,
    },
};

const TEST_CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";

#[tokio::test]
async fn publication_and_renewal_use_current_provider_and_policy_limits() {
    let mut fixture = fixture().await;
    let tags = vec![ObjectTag::new("ipfs-s3:pin", "true")];
    let context = || PublicationContext {
        bucket: BUCKET,
        key: KEY,
        tags: &tags,
        is_decompress_zip: false,
    };
    let (_, decision) = fixture
        .state
        .pinning
        .policy()
        .evaluate_publication_decision(context(), DecisionOrigin::new("principal", "publication"))
        .unwrap();
    let mut config = fixture.state.pinning.effective_config().clone();
    config.policies[0].provider_mode = ProviderMode::One;
    config.policies[0].max_duration = LeaseDuration::parse("6h").unwrap();
    config.policies[0].refresh_identity(0);
    config.providers[0].limits.enabled = false;
    config.provider_limits.get_mut("alpha").unwrap().enabled = false;
    Arc::get_mut(&mut fixture.state).unwrap().pinning = PinningCoordinator::build(config).unwrap();
    let state = &fixture.state;
    let db = state.store.db();
    let policy = state
        .pinning
        .policy()
        .resolve_captured_publication(context(), &decision)
        .unwrap();
    publication::publish_decided_object(
        db,
        PublicationRequest {
            object: PublicationObject::from_put(
                "published".into(),
                BUCKET,
                KEY,
                TEST_CID.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                time(1),
            ),
            tags,
            policy,
            object_target: PinTargetSpec {
                cid: TEST_CID.into(),
                logical_size: 10,
            },
        },
        DecidedPublish {
            decision: &decision,
            config: state.pinning.effective_config(),
            mode: state.pinning.control_mode(),
            limits: state.pinning.provider_limits(),
        },
    )
    .await
    .unwrap();
    let rows = db
        .query_all(sea_orm::Statement::from_string(
            db.get_database_backend(),
            "SELECT provider FROM pin_refs WHERE owner_object_id = 'published'",
        ))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].try_get::<String>("", "provider").unwrap(), "beta");
    let now = crate::store::database_clock::database_now(db)
        .await
        .unwrap();
    let renewal = now + chrono::TimeDelta::hours(5);
    for (until, valid) in [(renewal, true), (now + chrono::TimeDelta::hours(8), false)] {
        let result = put_object_tagging_at(
            state,
            put_request(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", &until.to_rfc3339()),
            ]),
            now,
        )
        .await;
        assert_eq!(result.is_ok(), valid);
    }
    assert_eq!(
        resources::manual(db, "published", now)
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        renewal
    );
    put_object_tagging_at(state, put_request(&[("team", "private")]), now)
        .await
        .unwrap();
    assert!(
        resources::manual(db, "published", now)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exact_file_renew_and_cancel_dont_mutate_automatic_or_current_demand() {
    let versions = versioned_fixture().await;
    let state = &versions.fixture.state;
    let db = state.store.db();
    let txn = db.begin().await.unwrap();
    for (owner, source) in [
        (&versions.historical_object_id, LeaseSource::Manual),
        (&versions.historical_object_id, LeaseSource::Automatic),
        (&versions.current_object_id, LeaseSource::Manual),
    ] {
        resources::attach(
            &txn,
            owner,
            &PinTargetSpec {
                cid: TEST_CID.into(),
                logical_size: 10,
            },
            &LeaseIntent {
                source,
                policy_id: versions.fixture.policy_id.clone(),
                provider_mode: ProviderMode::All,
                providers: vec!["alpha".into(), "beta".into()],
                content_mode: ContentMode::Object,
                duration: LeaseDuration::parse("4h").unwrap(),
            },
            state.pinning.provider_limits(),
            time(1),
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
    let exact = Some(versions.historical_version_id.as_str());
    let tags = |until: &str| {
        put_version_request(
            &[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", until),
                ("team", "history"),
            ],
            exact,
        )
    };
    for _ in 0..2 {
        put_object_tagging_at(state, tags("2026-07-22T08:00:00Z"), time(3))
            .await
            .unwrap();
    }
    for until in ["2026-07-22T07:00:00Z", "2026-07-24T10:00:00Z"] {
        assert_eq!(
            put_object_tagging_at(state, tags(until), time(3))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidArgument"
        );
    }
    let read = get_object_tagging(state, get_version_request(KEY, exact))
        .await
        .unwrap()
        .output;
    assert!(tag_pairs(&read.tag_set).contains(&("ipfs-s3:retain-until", "2026-07-22T08:00:00Z")));
    assert_eq!(
        resources::manual(db, &versions.historical_object_id, time(3))
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        time(8)
    );
    delete_object_tagging_at(state, delete_version_request(KEY, exact), time(4))
        .await
        .unwrap();
    assert!(
        resources::manual(db, &versions.historical_object_id, time(4))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        resources::manual(db, &versions.current_object_id, time(4))
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        time(5)
    );
    let count = db.query_one(sea_orm::Statement::from_sql_and_values(db.get_database_backend(),
        "SELECT COUNT(*) AS n FROM pin_refs WHERE owner_object_id = $1 AND source = 'automatic' AND expires_at = $2", [versions.historical_object_id.into(), time(5).timestamp_micros().into()])).await.unwrap().unwrap();
    assert_eq!(count.try_get::<i64>("", "n").unwrap(), 2);
}
