#[path = "support/stage7_management_actions.rs"]
mod support;

use ipfs_s3_gateway::{pinning::management::*, store::{self, entities::{pin_lease, pin_lease_target, pin_job}, pinning::management::{actions, records}}};
use sea_orm::{ConnectionTrait, EntityTrait, ColumnTrait, QueryFilter};

#[tokio::test]
async fn create_is_an_actual_absolute_lease_and_atomic_trusted_replay() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("exact", false).await;
    let request = fixture.create_request("create", identity.clone(), false);
    let confirmed = fixture.confirm(&request).await;
    let first = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.unwrap();
    assert!(!first.replayed);
    let result = &first.items[0];
    let lease = pin_lease::Entity::find_by_id(result.receipt.intent_ids[0].clone()).one(&fixture.db).await.unwrap().unwrap();
    let Action::Create(intent) = &request.items[0].action else { unreachable!() };
    assert_eq!(lease.created_at.timestamp(), intent.started_at);
    assert_eq!(lease.expires_at.timestamp(), intent.expires_at);
    assert_eq!(lease.owner_object_id, identity.object_id);
    assert!(lease.source.starts_with("management:"));
    assert_eq!(pin_lease_target::Entity::find().filter(pin_lease_target::Column::LeaseId.eq(&lease.id)).all(&fixture.db).await.unwrap().len(), 1);
    assert!(store::entities::pin_lease_requirement::contract::get(&fixture.db, &lease.id).await.unwrap().is_some());
    let jobs_before = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.unwrap();
    assert!(replay.replayed);
    assert!(replay.items == first.items);
    assert_eq!(pin_job::Entity::find().all(&fixture.db).await.unwrap(), jobs_before);
    let stored = records::lookup_applied(&fixture.db, &fixture.auth, &confirmed, "object").await.unwrap().unwrap();
    assert!(stored == result.receipt);
}

#[tokio::test]
async fn receipt_fault_rolls_back_the_business_and_jobs() {
    let fixture = support::Fixture::new().await;
    let identity = fixture.publish("receipt-fault", false).await;
    let confirmed = fixture.confirm(&fixture.create_request("fault", identity, false)).await;
    fixture.db.execute_unprepared("CREATE TRIGGER stage7_receipt_fault BEFORE UPDATE OF result ON pin_management_records WHEN NEW.kind='action' AND NEW.state='applied' BEGIN SELECT RAISE(ABORT,'injected receipt fault'); END").await.unwrap();
    assert!(actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.is_err());
    assert!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    fixture.db.execute_unprepared("DROP TRIGGER stage7_receipt_fault").await.unwrap();
    assert!(actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.is_ok());
}

#[tokio::test]
async fn cross_scope_private_drift_and_replacement_are_not_current_key_authority() {
    let fixture = support::Fixture::new().await;
    let exact = fixture.publish("replacement", false).await;
    let confirmed = fixture.confirm(&fixture.create_request("old-exact", exact.clone(), false)).await;
    fixture.publish("replacement", false).await;
    assert!(actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.is_err());
    assert!(pin_lease::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    let exact = fixture.publish("private", false).await;
    let confirmed = fixture.confirm(&fixture.create_request("private-drift", exact, false)).await;
    fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"drift\":true}' WHERE key='private'").await.unwrap();
    assert!(actions::apply(&fixture.db, &fixture.config, &fixture.auth, &confirmed).await.is_err());
    let other = support::Fixture::new().await;
    assert!(actions::apply(&other.db, &other.config, &other.auth, &confirmed).await.is_err());
}
