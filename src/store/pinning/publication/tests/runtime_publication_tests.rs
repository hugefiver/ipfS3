use super::*;
use crate::store::{Store, entities::pin_provider_route};

async fn fixture() -> Store {
    let mut options = ConnectOptions::new("sqlite::memory:");
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    crate::store::bucket::create(&db, "bucket", None)
        .await
        .unwrap();
    for provider in ["pinata", "filebase"] {
        ledger::register_route(
            &db,
            provider,
            &crate::pinning::identity::ProviderIdentity::legacy(
                provider, "noop", None, "noop", "cid",
            ),
        )
        .await
        .unwrap();
    }
    Store::new(db)
}

fn publication(id: &str, mode: ProviderMode) -> PublicationRequest {
    request(
        object(id, id, &format!("bafy-{id}"), 7),
        vec![],
        vec![intent(
            LeaseSource::Automatic,
            mode,
            &["pinata", "filebase"],
            ContentMode::Object,
        )],
    )
}

async fn publish(store: &Store, id: &str, mode: ProviderMode) -> AppResult<PublicationResult> {
    application::publish_object(store, publication(id, mode), &limits()).await
}

async fn assert_missing(store: &Store, id: &str) {
    assert!(
        object::Entity::find_by_id(id)
            .one(store.db())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(id))
            .one(store.db())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn warm_store_publication_rechecks_live_controls_and_transaction_temp() {
    let store = fixture().await;
    publish(&store, "cold", ProviderMode::All).await.unwrap();
    publish(&store.clone(), "warm", ProviderMode::All)
        .await
        .unwrap();
    for (alter, restore) in [
        (
            "UPDATE pin_management_readiness SET state='backfill' WHERE id=1",
            "UPDATE pin_management_readiness SET state='ready' WHERE id=1",
        ),
        (
            "UPDATE seaql_migrations SET version='bad-runtime-version' WHERE version='m20261023_000001_inline_documents'",
            "UPDATE seaql_migrations SET version='m20261023_000001_inline_documents' WHERE version='bad-runtime-version'",
        ),
        (
            "CREATE TEMP TABLE pin_management_records (state TEXT)",
            "DROP TABLE temp.pin_management_records",
        ),
    ] {
        store.db().execute_unprepared(alter).await.unwrap();
        assert!(
            publish(&store, "rejected", ProviderMode::All)
                .await
                .is_err()
        );
        assert_missing(&store, "rejected").await;
        store.db().execute_unprepared(restore).await.unwrap();
    }
    publish(&store, "restored", ProviderMode::All)
        .await
        .unwrap();
}

#[tokio::test]
async fn warm_store_publication_preserves_quota_and_fresh_route_authority() {
    for mode in [ProviderMode::All, ProviderMode::One] {
        let store = fixture().await;
        publish(&store, "cold", mode).await.unwrap();
        store
            .db()
            .execute_unprepared("UPDATE pin_provider_usage SET reserved_bytes=995")
            .await
            .unwrap();
        publish(&store, "blocked", mode).await.unwrap();
        let targets = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("lease:blocked:automatic"))
            .all(store.db())
            .await
            .unwrap();
        assert!(!targets.is_empty());
        assert!(
            targets
                .iter()
                .all(|target| target.state == TARGET_QUOTA_WAITING)
        );
        assert!(
            remote_pin::Entity::find()
                .filter(remote_pin::Column::Cid.eq("bafy-blocked"))
                .one(store.db())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            pin_provider_usage::Entity::find()
                .all(store.db())
                .await
                .unwrap()
                .iter()
                .all(|usage| usage.reserved_bytes == 995)
        );

        store
            .db()
            .execute_unprepared(
                "UPDATE pin_provider_routes SET retired=TRUE WHERE provider='pinata'",
            )
            .await
            .unwrap();
        assert!(publish(&store, "dirty-original", mode).await.is_err());
        assert_missing(&store, "dirty-original").await;
    }
}

#[tokio::test]
async fn warm_store_reservation_mutation_cannot_authorize_target_or_commit() {
    for mode in [ProviderMode::All, ProviderMode::One] {
        let store = fixture().await;
        publish(&store, "cold", mode).await.unwrap();
        store.db().execute_unprepared("CREATE TRIGGER runtime_quota_readiness AFTER INSERT ON remote_pins BEGIN UPDATE pin_management_readiness SET state='backfill' WHERE id=1; END").await.unwrap();
        assert!(publish(&store, "changed", mode).await.is_err());
        assert_missing(&store, "changed").await;
        assert!(
            remote_pin::Entity::find()
                .filter(remote_pin::Column::Cid.eq("bafy-changed"))
                .one(store.db())
                .await
                .unwrap()
                .is_none()
        );
        crate::store::pinning::management::schema::validate(store.db())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn warm_store_sealed_retirement_is_not_cached_admission() {
    let store = fixture().await;
    publish(&store, "cold", ProviderMode::All).await.unwrap();
    let mut identity =
        crate::pinning::identity::ProviderIdentity::legacy("pinata", "noop", None, "noop", "cid");
    identity.retired = true;
    ledger::register_route(store.db(), "pinata", &identity)
        .await
        .unwrap();
    assert!(
        pin_provider_route::Entity::find_by_id("pinata")
            .one(store.db())
            .await
            .unwrap()
            .unwrap()
            .retired
    );
    assert!(publish(&store, "retired", ProviderMode::All).await.is_err());
    assert_missing(&store, "retired").await;
}

#[tokio::test]
async fn cold_invalid_store_and_new_store_identity_cannot_publish() {
    let store = fixture().await;
    publish(&store, "cold", ProviderMode::All).await.unwrap();
    store
        .db()
        .execute_unprepared("DROP TRIGGER s75_applied_no_delete")
        .await
        .unwrap();
    let fresh = Store::new(store.db().clone());
    assert!(
        publish(&fresh, "fresh-invalid", ProviderMode::All)
            .await
            .is_err()
    );
    assert_missing(&fresh, "fresh-invalid").await;
    assert!(
        publish_object(
            store.db(),
            publication("full-invalid", ProviderMode::All),
            &limits()
        )
        .await
        .is_err()
    );
    assert_missing(&store, "full-invalid").await;
}

#[tokio::test]
async fn warm_store_all_unavailable_and_delayed_one_target_recheck_dirty_authority() {
    for mode in [ProviderMode::All, ProviderMode::One] {
        let store = fixture().await;
        let mut capped = limits();
        for limit in capped.values_mut() {
            limit.max_bytes = 1;
        }
        application::publish_object(&store, publication("cold", mode), &capped)
            .await
            .unwrap();
        let targets = pin_lease_target::Entity::find()
            .all(store.db())
            .await
            .unwrap();
        assert_eq!(targets.len(), if mode == ProviderMode::All { 2 } else { 1 });
        assert!(
            targets
                .iter()
                .all(|target| target.state == TARGET_QUOTA_BLOCKED)
        );
        store.db().execute_unprepared(
            "CREATE TRIGGER runtime_retire_first BEFORE INSERT ON pin_provider_usage
             WHEN NEW.provider='filebase' AND EXISTS(SELECT 1 FROM pin_leases WHERE owner_object_id='mutating')
             BEGIN UPDATE pin_provider_routes SET retired=TRUE WHERE provider='pinata'; END",
        ).await.unwrap();
        assert!(
            application::publish_object(&store, publication("mutating", mode), &capped)
                .await
                .is_err()
        );
        assert_missing(&store, "mutating").await;
        assert_eq!(
            remote_pin::Entity::find().count(store.db()).await.unwrap(),
            0
        );
        assert!(
            pin_provider_usage::Entity::find()
                .all(store.db())
                .await
                .unwrap()
                .iter()
                .all(|usage| usage.reserved_bytes == 0 && usage.reserved_pins == 0)
        );
        assert!(
            !pin_provider_route::Entity::find_by_id("pinata")
                .one(store.db())
                .await
                .unwrap()
                .unwrap()
                .retired
        );
    }
}

#[tokio::test]
async fn warm_store_reuse_keeps_unique_quota_and_advances_each_new_reference_once() {
    let store = fixture().await;
    let first = publication("cold", ProviderMode::All);
    application::publish_object(&store, first.clone(), &limits())
        .await
        .unwrap();
    let mut next = publication("warm", ProviderMode::All);
    next.object.cid = first.object.cid;
    next.object.etag = next.object.cid.clone();
    next.object_target.cid = next.object.cid.clone();
    application::publish_object(&store.clone(), next, &limits())
        .await
        .unwrap();
    let remotes = remote_pin::Entity::find().all(store.db()).await.unwrap();
    assert_eq!(remotes.len(), 2);
    assert!(remotes.iter().all(|remote| remote.epoch == 2));
    assert!(
        pin_provider_usage::Entity::find()
            .all(store.db())
            .await
            .unwrap()
            .iter()
            .all(|usage| usage.reserved_bytes == 7 && usage.reserved_pins == 1)
    );
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(store.db())
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        pin_job::Entity::find()
            .filter(pin_job::Column::Operation.eq("submit"))
            .count(store.db())
            .await
            .unwrap(),
        2
    );
}
