use std::time::Duration;

use chrono::Utc;
use ipfs_s3_gateway::{
    config::Config,
    pinning::{
        config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
        coordinator::PinningCoordinator,
        policy::{LeaseIntent, LeaseSource, PublicationPolicy},
        tags::ContentMode,
    },
    store::{
        self, Store,
        entities::{pin_job, pin_lease, pin_lease_target, pin_provider_route, remote_pin},
        object_version::BucketVersioningState,
        pinning::{
            leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
            quota,
        },
    },
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set, TransactionTrait,
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const CID: &str = "bafy-stage2-reattach";

fn runtime(endpoint: &str) -> std::sync::Arc<PinningCoordinator> {
    let text = format!(
        "[pinning_identity]\nprimary_storage_domain='local'\n[[pinning_identity.providers]]\nconfig_name='account'\nprovider_id='stable'\ndisplay_name='Account'\nbackend='filebase'\nscope='shared-account'\nstorage_domain='remote'\ncredential_revision=1\nendpoint_revision=1\nsecret_ref='env:STAGE2_TOKEN'\napi_profile='filebase-psa'\nstrategy='cid'\ncleanup='retain'\n[[pinning.providers]]\nname='account'\nkind='filebase'\ntoken_env='STAGE2_TOKEN'\nendpoint='{endpoint}'\npriority=1\nmax_bytes=100\nmax_pins=1\n[pinning]\nworker_interval='1s'\n"
    );
    let config: Config = toml::from_str(&text).unwrap();
    PinningCoordinator::build(
        ValidatedPinningConfig::from_config(&config, |_| Some("test-token".into())).unwrap(),
    )
    .unwrap()
}

async fn publish(
    store: &Store,
    runtime: &PinningCoordinator,
) -> ipfs_s3_gateway::error::AppResult<()> {
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "reattach",
                "versioned-key",
                CID.into(),
                80,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases: vec![LeaseIntent {
                    source: LeaseSource::Automatic,
                    policy_id: "reattach".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 80,
            },
        },
        runtime.provider_limits(),
    )
    .await?;
    Ok(())
}

async fn wait_for<F, Fut>(mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            if ready().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("pinning state did not converge");
}

async fn exercise_reattach(db: sea_orm::DatabaseConnection) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/pins"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "requestid":"existing", "status":"pinned", "pin":{"cid":CID}
        })))
        .mount(&server)
        .await;
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "reattach", None).await.unwrap();
    store::bucket::set_versioning_state(&db, "reattach", BucketVersioningState::Enabled)
        .await
        .unwrap();
    let store = Store::new(db);
    let runtime = runtime(&server.uri());
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(&store, &runtime).await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
            .one(store.db())
            .await
            .unwrap()
            .is_some_and(|remote| remote.status == "pinned")
    })
    .await;
    let old_lease = pin_lease::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    wait_for(|| async {
        ledger::lease_status(store.db(), &old_lease.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    })
    .await;
    ledger::observe(
        store.db(),
        &provider,
        CID,
        ipfs_s3_gateway::pinning::provider::RemotePinStatus::Pinned,
        ledger::Ownership::ExternalExisting,
        Utc::now(),
    )
    .await
    .unwrap();
    let original = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.ownership, "external_existing");
    assert!(original.remote_pinned_at.is_some());
    let remote_before = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();

    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &old_lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    let retained_remote = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pinned"
    );
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        80
    );
    assert!(
        !ledger::lease_status(store.db(), &old_lease.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );

    // A route change must not adopt this already-paid-for retained pin.
    let route = pin_provider_route::Entity::find_by_id(&provider)
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    let mut changed: serde_json::Value = serde_json::from_str(&route.snapshot).unwrap();
    changed["credential_revision"] = serde_json::json!(2);
    let mut update: pin_provider_route::ActiveModel = route.clone().into();
    update.snapshot = Set(changed.to_string());
    update.update(store.db()).await.unwrap();
    assert!(publish(&store, &runtime).await.is_err());
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    let mut restore: pin_provider_route::ActiveModel =
        pin_provider_route::Entity::find_by_id(&provider)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .into();
    restore.snapshot = Set(route.snapshot);
    restore.update(store.db()).await.unwrap();

    publish(&store, &runtime).await.unwrap();
    let new_lease = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(new_lease.id, old_lease.id);
    let target = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&new_lease.id))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.state, "pinned");
    let status = ledger::lease_status(store.db(), &new_lease.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        status.any_provider_available,
        "pinned desired target must be available after reattach"
    );
    assert!(status.all_targets_pinned);
    let after = ledger::get(store.db(), &provider, CID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.effect, "confirmed");
    assert_eq!(after.ownership, original.ownership);
    assert_eq!(after.first_observed_at, original.first_observed_at);
    assert_eq!(after.remote_pinned_at, original.remote_pinned_at);
    let remote_after = remote_pin::Entity::find_by_id((provider.clone(), CID.into()))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remote_after.request_id, remote_before.request_id);
    assert_eq!(remote_after.epoch, retained_remote.epoch + 1);
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        80
    );
    assert_eq!(
        pin_lease::Entity::find_by_id(&old_lease.id)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .expires_at,
        old_lease.expires_at
    );
    let worker = runtime.start(store.clone(), CancellationToken::new());
    tokio::time::sleep(Duration::from_millis(1150)).await;
    worker.shutdown(Duration::from_secs(5)).await;
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method.as_str() == "POST")
            .count(),
        1
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );

    // A retained ledger alone is not evidence that a queued remote is pinned.
    let txn = store.db().begin().await.unwrap();
    leases::cancel_lease(&txn, &new_lease.id, Utc::now())
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let worker = runtime.start(store.clone(), CancellationToken::new());
    wait_for(|| async {
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect
            == "retained"
    })
    .await;
    worker.shutdown(Duration::from_secs(5)).await;
    remote_pin::Entity::update_many()
        .col_expr(
            remote_pin::Column::Status,
            sea_orm::sea_query::Expr::value("queued"),
        )
        .filter(remote_pin::Column::Provider.eq(&provider))
        .filter(remote_pin::Column::Cid.eq(CID))
        .exec(store.db())
        .await
        .unwrap();
    publish(&store, &runtime).await.unwrap();
    let queued = pin_lease::Entity::find()
        .filter(pin_lease::Column::State.eq("active"))
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !ledger::lease_status(store.db(), &queued.id)
            .await
            .unwrap()
            .unwrap()
            .any_provider_available
    );
    assert_eq!(
        ledger::get(store.db(), &provider, CID)
            .await
            .unwrap()
            .unwrap()
            .effect,
        "retained"
    );
    assert_eq!(
        quota::read_usage(store.db(), &provider)
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
}

#[tokio::test]
async fn sqlite_retained_pin_reattach_restores_availability_without_new_post_or_capacity() {
    exercise_reattach(store::connect_database("sqlite::memory:").await.unwrap()).await;
}

#[tokio::test]
#[ignore = "requires isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_retained_pin_reattach_restores_availability_without_new_post_or_capacity() {
    use futures_util::FutureExt;
    let url = std::env::var("IPFS_S3_TEST_POSTGRES_URL").unwrap();
    let admin = sea_orm::Database::connect(&url).await.unwrap();
    let schema = format!("stage2_reattach_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute_unprepared(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let mut scoped = url::Url::parse(&url).unwrap();
    scoped
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={schema}"));
    let db = store::connect_database(scoped.as_str()).await.unwrap();
    let result = std::panic::AssertUnwindSafe(exercise_reattach(db.clone()))
        .catch_unwind()
        .await;
    db.close().await.unwrap();
    admin
        .execute_unprepared(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}
