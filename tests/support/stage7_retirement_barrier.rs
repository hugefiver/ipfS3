//! Real foundation confirmation + same-transaction Retain/action receipt.
//! Fixtures do not claim that logical sizes or Noop rows prove a physical PIN.
#![allow(dead_code)]
#[path = "stage7_management.rs"]
pub mod foundation;

use chrono::Utc;
use ipfs_s3_gateway::{config::Config, pinning::{self, management::{*, local_config::ManagementConfig}, policy::{LeaseIntent, LeaseSource, PublicationPolicy}}, store::{entities::pin_lease, pinning::{management::records, publication::{self, PublicationObject, PublicationRequest, PinTargetSpec}}}};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, EntityTrait, TransactionTrait};

pub async fn confirmed_retirement(db: &DatabaseConnection, raw: &Config, config: &ManagementConfig, plan_id: &str) -> (records::LocalAuthorization, records::ConfirmedBinding, DryRunPlan) {
    let route = config.pinning.providers[0].route_snapshot();
    let request = PlanRequest { plan_id: plan_id.into(), items: vec![ItemRequest {
        item_id: "retire-provider".into(), action: Action::Retire,
        subject: Subject::Provider { route: RouteIdentity::from_original(route) },
    }] };
    let snapshot = db_snapshot::collect_with_config(db, config, &request).await.unwrap().planning_snapshot().unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let auth = records::authorize_local(raw, db).await.unwrap();
    let confirmed = records::confirm(db, config, &auth, &request, &snapshot, &plan, vec![], chrono::Duration::minutes(2)).await.unwrap();
    (auth, confirmed, plan)
}

pub async fn write_retirement(tx: &DatabaseTransaction, config: &ManagementConfig, auth: &records::LocalAuthorization, binding: &records::ConfirmedBinding, plan: &DryRunPlan) -> records::RetirementRecord {
    match tx.get_database_backend() {
        DatabaseBackend::Postgres => { tx.execute_unprepared("LOCK TABLE pin_management_retirements IN EXCLUSIVE MODE NOWAIT").await.unwrap(); }
        DatabaseBackend::Sqlite => { tx.execute_unprepared("UPDATE pin_management_retirements SET allocation_domain=allocation_domain WHERE 1=0").await.unwrap(); }
        _ => panic!("unsupported fixture database"),
    }
    records::revalidate_confirmed_in_transaction(tx, config, auth, binding).await.unwrap();
    let allocation = config.pinning.providers[0].identity.allocation_key();
    let record = records::write_retirement_in_transaction(tx, auth, binding, "retire-provider", &allocation, records::Disposition::Retain, vec![]).await.unwrap();
    records::write_action_receipt_in_transaction(tx, auth, binding, &records::ActionReceipt {
        binding: plan.items()[0].clone(), intent_ids: vec![], job_ids: vec![],
    }, binding.confirmation.native_frame.as_ref().unwrap()).await.unwrap();
    record
}

pub async fn retire(db: &DatabaseConnection, raw: &Config, config: &ManagementConfig, plan_id: &str) -> records::RetirementRecord {
    let (auth, binding, plan) = confirmed_retirement(db, raw, config, plan_id).await;
    let tx = db.begin().await.unwrap();
    let record = write_retirement(&tx, config, &auth, &binding, &plan).await;
    tx.commit().await.unwrap();
    record
}

pub async fn manual(db: &DatabaseConnection, config: &ManagementConfig, key: &str) -> pin_lease::Model {
    use sea_orm::{ColumnTrait, QueryFilter};
    let id = uuid::Uuid::new_v4().to_string();
    let intent = LeaseIntent { source: LeaseSource::Manual, policy_id: config.pinning.policies[0].identity.clone(),
        provider_mode: pinning::config::ProviderMode::All, providers: vec![config.pinning.providers[0].name.clone()],
        content_mode: pinning::tags::ContentMode::Object, duration: pinning::config::LeaseDuration::parse("1h").unwrap() };
    publication::publish_object(db, PublicationRequest {
        object: PublicationObject::from_put(id.clone(), "bucket", key, foundation::CID.into(), 10, None, None, false, None, None, Utc::now()),
        tags: vec![], policy: PublicationPolicy { tags: vec![], leases: vec![intent] },
        object_target: PinTargetSpec { cid: foundation::CID.into(), logical_size: 10 },
    }, &config.pinning.provider_limits).await.unwrap();
    pin_lease::Entity::find().filter(pin_lease::Column::OwnerObjectId.eq(id)).one(db).await.unwrap().unwrap()
}
