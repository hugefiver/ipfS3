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
        entities::{object, pin_lease_target, pin_provider_usage, remote_pin},
        pinning::{
            leases, ledger,
            publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait};
use sea_orm_migration::{MigrationTrait, SchemaManager};

fn runtime(credential_revision: u64, scope: &str) -> std::sync::Arc<PinningCoordinator> {
    let text = format!(
        r#"
[pinning_identity]
primary_storage_domain = 'local'
[[pinning_identity.providers]]
config_name = 'account'
provider_id = 'account-id'
display_name = 'Account'
backend = 'noop'
scope = '{scope}'
storage_domain = 'remote'
credential_revision = {credential_revision}
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
[[pinning.providers]]
name = 'account'
kind = 'noop'
priority = 1
max_bytes = 100
max_pins = 1
"#
    );
    let config: Config = toml::from_str(&text).unwrap();
    PinningCoordinator::build(ValidatedPinningConfig::from_config(&config, |_| None).unwrap())
        .unwrap()
}

async fn database() -> Store {
    let db = store::connect_database("sqlite::memory:").await.unwrap();
    db.execute_unprepared("PRAGMA foreign_keys = ON")
        .await
        .unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "capacity", None).await.unwrap();
    Store::new(db)
}

async fn publish(
    store: &Store,
    runtime: &PinningCoordinator,
    key: &str,
    cid: &str,
) -> ipfs_s3_gateway::error::AppResult<publication::PublicationResult> {
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publication::publish_object(
        store.db(),
        PublicationRequest {
            object: PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                "capacity",
                key,
                cid.into(),
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
                    policy_id: "capacity".into(),
                    provider_mode: ProviderMode::All,
                    providers: vec![provider],
                    content_mode: ContentMode::Object,
                    duration: LeaseDuration::parse("1h").unwrap(),
                }],
            },
            object_target: PinTargetSpec {
                cid: cid.into(),
                logical_size: 80,
            },
        },
        runtime.provider_limits(),
    )
    .await
}

#[tokio::test]
async fn changed_revision_cannot_reuse_pinned_resource_without_route_proof() {
    let store = database().await;
    let first = runtime(1, "same-account");
    first.register_identities(&store).await.unwrap();
    let provider = first.provider_limits().keys().next().unwrap().clone();
    publish(&store, &first, "old", "bafy-shared").await.unwrap();
    store.db().execute_unprepared(&format!("UPDATE remote_pins SET status='pinned', request_id='old-request' WHERE provider='{provider}' AND cid='bafy-shared'")).await.unwrap();
    let next = runtime(2, "same-account");
    next.register_identities(&store).await.unwrap();
    assert!(publish(&store, &next, "new", "bafy-shared").await.is_err());
    let old_target = pin_lease_target::Entity::find()
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert!(
        leases::project_target_from_remote(store.db(), &old_target.id, Utc::now())
            .await
            .is_err()
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        object::Entity::find().all(store.db()).await.unwrap().len(),
        1
    );
    assert_eq!(
        remote_pin::Entity::find_by_id((provider.clone(), "bafy-shared".to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pinned"
    );
    assert!(
        ledger::get(store.db(), &provider, "bafy-shared")
            .await
            .unwrap()
            .unwrap()
            .route
            .unwrap()
            .contains("\"credential_revision\":1")
    );
}

#[tokio::test]
async fn matching_history_can_share_capacity_without_another_pin() {
    let store = database().await;
    let runtime = runtime(1, "same-account");
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(&store, &runtime, "old", "bafy-shared")
        .await
        .unwrap();
    store.db().execute_unprepared(&format!("UPDATE remote_pins SET status='pinned', request_id='old-request' WHERE provider='{provider}' AND cid='bafy-shared'")).await.unwrap();
    publish(&store, &runtime, "new", "bafy-shared")
        .await
        .unwrap();
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(provider)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
}

#[tokio::test]
async fn pinned_cid_with_unknown_historical_route_cannot_be_adopted() {
    let store = database().await;
    let runtime = runtime(1, "same-account");
    runtime.register_identities(&store).await.unwrap();
    let provider = runtime.provider_limits().keys().next().unwrap().clone();
    publish(&store, &runtime, "old", "bafy-shared")
        .await
        .unwrap();
    store.db().execute_unprepared(&format!("UPDATE remote_pins SET status='pinned', request_id='old-request' WHERE provider='{provider}' AND cid='bafy-shared'")).await.unwrap();
    store.db().execute_unprepared(&format!("UPDATE remote_pin_ledger SET route=NULL, ownership='unknown', effect='unknown' WHERE provider='{provider}' AND cid='bafy-shared'")).await.unwrap();
    assert!(
        publish(&store, &runtime, "new", "bafy-shared")
            .await
            .is_err()
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        pin_provider_usage::Entity::find_by_id(provider)
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
}

#[tokio::test]
async fn migrated_unknown_legacy_account_retains_occupancy_and_blocks_unproven_new_scope() {
    let store = database().await;
    // Recreate the Stage 1 schema to exercise the actual upgrade, not a synthetic
    // Stage 2 ledger row. The old alias does not identify its account.
    for table in [
        "remote_pin_ledger",
        "pin_invocation_routes",
        "pin_resource_history",
        "pin_provider_routes",
    ] {
        store
            .db()
            .execute_unprepared(&format!("DROP TABLE {table}"))
            .await
            .unwrap();
    }
    store.db().execute_unprepared("INSERT INTO remote_pins (provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES ('former-alias','bafy-old','old-request',80,'pinned',1,0,'2026-09-19T00:00:00Z')").await.unwrap();
    store.db().execute_unprepared("INSERT INTO objects (id,bucket,key,cid,size,etag) VALUES ('old-owner','capacity','old','bafy-old',80,'bafy-old')").await.unwrap();
    store.db().execute_unprepared("INSERT INTO pin_leases (id,owner_object_id,source,policy_id,provider_mode,content_mode,created_at,last_touched_at,expires_at,generation,state) VALUES ('old-lease','old-owner','automatic','old-policy','all','object','2026-09-19T00:00:00Z','2026-09-19T00:00:00Z','2026-09-30T00:00:00Z',1,'active')").await.unwrap();
    store.db().execute_unprepared("INSERT INTO pin_lease_targets (id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) VALUES ('old-target','old-lease','bafy-old',80,'former-alias','pinned','2026-09-19T00:00:00Z','2026-09-19T00:00:00Z')").await.unwrap();
    let migration = store::migrations::m20260920_000002_pin_identity_ledger::Migration;
    migration.up(&SchemaManager::new(store.db())).await.unwrap();
    let next = runtime(1, "new-explicit-scope");
    next.register_identities(&store).await.unwrap();
    assert!(publish(&store, &next, "new", "bafy-new").await.is_err());
    assert_eq!(
        pin_provider_usage::Entity::find_by_id("former-alias")
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .reserved_pins,
        1
    );
    assert!(
        ledger::get(store.db(), "former-alias", "bafy-old")
            .await
            .unwrap()
            .unwrap()
            .route
            .is_none()
    );
    assert_eq!(
        remote_pin::Entity::find_by_id(("former-alias".to_owned(), "bafy-old".to_owned()))
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pinned"
    );
    assert_eq!(
        object::Entity::find().all(store.db()).await.unwrap().len(),
        1
    );
    let old = ledger::lease_status(store.db(), "old-lease")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.pinned_targets, 0);
    assert!(!old.all_targets_pinned);
    let historical_target = pin_lease_target::Entity::find_by_id("old-target")
        .one(store.db())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(historical_target.state, "pinned");
    assert!(
        !ledger::cleanup_allowed(store.db(), "former-alias", "bafy-old")
            .await
            .unwrap()
    );
}
