#[path = "support/stage7_retirement_barrier.rs"]
mod support;

use ipfs_s3_gateway::{
    pinning::provider::canonical_resource_cid,
    store::{
        entities::{pin_provider_route, remote_pin},
        pinning::{
            leases, ledger,
            management::{native, records},
            quota::{self, ReservationOutcome},
        },
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

#[tokio::test]
async fn retain_metadata_blocks_first_birth_shared_cid_reuse_and_does_not_release_capacity() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    let provider = &config.pinning.providers[0];
    let allocation = provider.identity.allocation_key();
    let cid = canonical_resource_cid(support::foundation::CID).unwrap();
    let tx = db.begin().await.unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    assert_eq!(
        quota::reserve_unique(
            &tx,
            &allocation,
            &cid,
            10,
            &config.pinning.provider_limits,
            now
        )
        .await
        .unwrap(),
        ReservationOutcome::Reserved
    );
    tx.commit().await.unwrap();
    let before = native::capture(&db).await.unwrap();
    let record = support::retire(&db, &raw, &config, "retire-held-domain").await;
    assert_eq!(record.state, records::RetirementState::NeedsAttention);
    assert!(
        !pin_provider_route::Entity::find_by_id(allocation.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .retired,
        "exercise the permanent record, not only the old registration boolean"
    );
    for requested in [
        &cid,
        "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
    ] {
        let tx = db.begin().await.unwrap();
        assert!(
            quota::reserve_unique(
                &tx,
                &allocation,
                requested,
                10,
                &config.pinning.provider_limits,
                now
            )
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
    }
    let after = native::capture(&db).await.unwrap();
    for table in [
        "remote_pins",
        "remote_pin_ledger",
        "pin_provider_usage",
        "pin_resource_history",
        "pin_submit_history",
    ] {
        assert!(
            before.table(table) == after.table(table),
            "retirement cannot rewrite {table}"
        );
    }
}

#[tokio::test]
async fn absent_reallocation_is_denied_without_rewriting_original_release_history() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    let allocation = config.pinning.providers[0].identity.allocation_key();
    let cid = canonical_resource_cid(support::foundation::CID).unwrap();
    let tx = db.begin().await.unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    quota::reserve_unique(
        &tx,
        &allocation,
        &cid,
        10,
        &config.pinning.provider_limits,
        now,
    )
    .await
    .unwrap();
    let remote = remote_pin::Entity::find_by_id((allocation.clone(), cid.clone()))
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        quota::confirmed_release(&tx, &allocation, &cid, remote.epoch, now)
            .await
            .unwrap(),
        quota::ConfirmedReleaseOutcome::Released
    );
    tx.commit().await.unwrap();
    support::retire(&db, &raw, &config, "retire-absent-domain").await;
    let before = native::capture(&db).await.unwrap();
    let tx = db.begin().await.unwrap();
    assert!(
        quota::reserve_unique(
            &tx,
            &allocation,
            &cid,
            10,
            &config.pinning.provider_limits,
            now
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    let after = native::capture(&db).await.unwrap();
    for table in [
        "remote_pins",
        "remote_pin_ledger",
        "pin_resource_history",
        "pin_provider_usage",
    ] {
        assert!(before.table(table) == after.table(table));
    }
}

#[tokio::test]
async fn active_renew_and_expired_manual_reactivation_are_both_permanently_denied() {
    for expired in [false, true] {
        let raw = support::foundation::config("http://127.0.0.1:1");
        let config = support::foundation::management_config(&raw);
        let (_directory, db) = support::foundation::database().await;
        support::foundation::setup(&db, &config).await;
        let lease = support::manual(&db, &config, "manual").await;
        let at = if expired {
            lease.expires_at + chrono::Duration::seconds(1)
        } else {
            lease.created_at
        };
        if expired {
            let tx = db.begin().await.unwrap();
            leases::expire_due_leases(&tx, at).await.unwrap();
            tx.commit().await.unwrap();
        }
        support::retire(&db, &raw, &config, "retire-manual").await;
        let before = native::capture(&db).await.unwrap();
        let tx = db.begin().await.unwrap();
        assert!(
            leases::renew_manual_lease(
                &tx,
                &lease.owner_object_id,
                &lease.id,
                lease.expires_at + chrono::Duration::hours(1),
                at
            )
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
        assert!(before == native::capture(&db).await.unwrap());
    }
}

#[tokio::test]
async fn aliases_cannot_reopen_but_a_proven_distinct_full_domain_is_not_globally_blocked() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    let original = config.pinning.providers[0].identity.clone();
    let key = original.allocation_key();
    let old = pin_provider_route::Entity::find_by_id(key.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    support::retire(&db, &raw, &config, "retire-alias-domain").await;
    let mut alias = original.clone();
    alias.provider_id = "new-alias".into();
    ledger::register_route(&db, &key, &alias).await.unwrap();
    assert_eq!(
        pin_provider_route::Entity::find_by_id(key.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        old
    );
    for family in [false, true] {
        let mut other = original.clone();
        other.provider_id = "different-domain".into();
        if family {
            other.api_profile = "rpc".into();
        } else {
            other.scope = "different-account".into();
        }
        let other_key = other.allocation_key();
        assert_ne!(
            key, other_key,
            "resource_type belongs to the domain, not just backend/scope"
        );
        ledger::register_route(&db, &other_key, &other)
            .await
            .unwrap();
        let mut limits = config.pinning.provider_limits.clone();
        limits.insert(other_key.clone(), limits[&key].clone());
        let tx = db.begin().await.unwrap();
        let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
            .await
            .unwrap();
        assert_eq!(
            quota::reserve_unique(
                &tx,
                &other_key,
                &canonical_resource_cid(support::foundation::CID).unwrap(),
                10,
                &limits,
                now
            )
            .await
            .unwrap(),
            ReservationOutcome::Reserved
        );
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn an_incomplete_actual_receipt_is_needs_attention_not_a_reopened_domain() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    support::retire(&db, &raw, &config, "retire-corrupt-receipt").await;
    db.execute_unprepared(
        "UPDATE pin_management_records SET result='{}' WHERE kind='action' AND state='applied'",
    )
    .await
    .unwrap();
    let before = native::capture(&db).await.unwrap();
    let tx = db.begin().await.unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    let error = quota::reserve_unique(
        &tx,
        &config.pinning.providers[0].identity.allocation_key(),
        &canonical_resource_cid(support::foundation::CID).unwrap(),
        10,
        &config.pinning.provider_limits,
        now,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("needs attention"));
    tx.rollback().await.unwrap();
    assert!(before == native::capture(&db).await.unwrap());
}

#[tokio::test]
async fn ordinary_admission_does_not_inherit_the_full_native_inventory_budget() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    support::foundation::publish(&db, &config, "large-metadata", false).await;
    let mut other = config.pinning.providers[0].identity.clone();
    other.scope = "other-account".into();
    other.provider_id = "other-provider".into();
    let key = other.allocation_key();
    ledger::register_route(&db, &key, &other).await.unwrap();
    support::retire(&db, &raw, &config, "retire-before-growth").await;
    db.execute_unprepared("UPDATE objects SET metadata=json_object('large',printf('%.*c',4194305,'x')) WHERE key='large-metadata'").await.unwrap();
    assert!(
        native::capture(&db).await.is_err(),
        "management capture is still complete and budgeted"
    );
    let mut limits = config.pinning.provider_limits.clone();
    limits.insert(
        key.clone(),
        limits[&config.pinning.providers[0].name].clone(),
    );
    let tx = db.begin().await.unwrap();
    let now = ipfs_s3_gateway::store::database_clock::database_now(&tx)
        .await
        .unwrap();
    assert_eq!(
        quota::reserve_unique(
            &tx,
            &key,
            &canonical_resource_cid(support::foundation::CID).unwrap(),
            10,
            &limits,
            now
        )
        .await
        .unwrap(),
        ReservationOutcome::Reserved
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn same_count_route_phantom_invalidates_confirmation_instead_of_authorizing_retirement() {
    let raw = support::foundation::config("http://127.0.0.1:1");
    let config = support::foundation::management_config(&raw);
    let (_directory, db) = support::foundation::database().await;
    support::foundation::setup(&db, &config).await;
    let (auth, binding, _) =
        support::confirmed_retirement(&db, &raw, &config, "retire-phantom").await;
    let tx = db.begin().await.unwrap();
    db.execute_unprepared(
        "UPDATE pin_provider_routes SET display_name='same-count-replaced-tuple'",
    )
    .await
    .unwrap();
    assert!(
        records::revalidate_confirmed_in_transaction(&tx, &config, &auth, &binding)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(
        native::capture(&db)
            .await
            .unwrap()
            .table("pin_management_retirements")
            .unwrap()
            .rows
            .is_empty()
    );
}
#[path = "support/stage7_management.rs"]
pub mod foundation;
