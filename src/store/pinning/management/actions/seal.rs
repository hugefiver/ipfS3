//! Publish locator completeness for the executor's exact source writes only.
use super::*;
use sea_orm::DatabaseTransaction;
use std::collections::BTreeSet;

pub(super) async fn executed(
    tx: &DatabaseTransaction,
    auth: &records::LocalAuthorization,
    binding: &records::ConfirmedBinding,
    executed: &execution::ExecutedAction<'_>,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let mut keys = BTreeSet::from([native::NativeKey {
        table: "pin_management_records".into(),
        values: vec![
            auth.database_scope().into(),
            binding.request.plan_id.clone(),
            executed
                .action_view()
                .receipt
                .binding
                .request
                .item_id
                .clone(),
            "action".into(),
        ],
    }]);
    for output in &executed.operation().facts.2 {
        if matches!(
            output.0.0.as_str(),
            "pin_provider_routes" | "pin_management_retirements"
        ) {
            if output.2.is_none() {
                return Err(ManagementError::InvalidEvidence);
            }
            keys.insert(native::NativeKey {
                table: output.0.0.clone(),
                values: output.0.1.clone(),
            });
        }
    }
    work.copy(&keys)?;
    super::super::admission::authority::rebuild_sources(tx, &keys, work).await
}
