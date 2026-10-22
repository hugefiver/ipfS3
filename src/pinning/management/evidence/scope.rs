use super::{ExactObject, ManagementError, Result, native};
use crate::pinning::management::PublicVersionId;
use native::{
    NativeFrame, NativeKey,
    meter::Budget,
    scope::{ScopeSpec, Selector},
};
use sea_orm::{
    AccessMode, DatabaseConnection, DatabaseTransaction, IsolationLevel, TransactionTrait,
};

pub(super) fn content(identity: &ExactObject) -> Result<ScopeSpec> {
    ScopeSpec::new(vec![
        Selector::Exact(NativeKey {
            table: "objects".into(),
            values: vec![identity.object_id.clone()],
        }),
        Selector::ExactVersion {
            id: identity.private_version_id.clone(),
        },
        Selector::PublicVersionBinding {
            bucket: identity.bucket.clone(),
            key: identity.key.clone(),
            version: match &identity.public_version {
                PublicVersionId::Null => None,
                PublicVersionId::Version(version) => Some(version.clone()),
            },
        },
    ])
}

pub(super) async fn read_transaction(db: &DatabaseConnection) -> Result<DatabaseTransaction> {
    db.begin_with_config(
        Some(IsolationLevel::RepeatableRead),
        Some(AccessMode::ReadOnly),
    )
    .await
    .map_err(|_| ManagementError::Database)
}

pub(super) async fn capture(
    tx: &DatabaseTransaction,
    spec: &ScopeSpec,
    budget: &mut Budget,
) -> Result<NativeFrame> {
    native::scope::collect_scope(tx, spec, budget)
        .await?
        .into_native_frame(budget)
}
