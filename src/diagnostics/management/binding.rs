//! Input is never substituted for the local immutable original.
use super::input::{self, ImportedFile};
use anyhow::Result;
use ipfs_s3_gateway::pinning::management::wire::v3::matching;
use ipfs_s3_gateway::store::pinning::management::records::{
    self, ConfirmedBinding, LocalAuthorization, SharedDecodeBudget,
};
use sea_orm::DatabaseConnection;

pub enum Restored {
    Current(ConfirmedBinding, SharedDecodeBudget),
    Recorded(Box<records::RecordedPlan>),
}

pub async fn restore(
    db: &DatabaseConnection,
    auth: &LocalAuthorization,
    imported: ImportedFile,
) -> Result<Restored> {
    auth.check(db).await?;
    imported.staged.budget().check()?;
    let head = &imported.head;
    if head.database_scope != auth.database_scope()
        || head.database_identity != auth.database_identity()
        || head.principal != auth.principal()
        || head.applied_claims
        || head.request.is_none()
        || head.acknowledgements.is_none()
        || head.expires_at.is_none()
    {
        return Err(input::invalid());
    }
    let request = head.request.as_ref().ok_or_else(input::invalid)?;
    let mut cursor = imported.staged.cursor()?;
    if let Some(plan) = records::load_recorded_plan(db, auth, request, cursor.budget_mut()).await? {
        matching::recorded_database(db, cursor, &plan).await?;
        return Ok(Restored::Recorded(Box::new(plan)));
    }
    let work = SharedDecodeBudget::default();
    let binding = records::load_confirmed_authority(db, auth, request, work.clone()).await?;
    let (nodes, bytes) = work.lock().map_err(|_| input::invalid())?.usage();
    cursor
        .budget_mut()
        .charge(0, 0, nodes as u64, bytes as u64)?;
    let _authority = cursor.budget_mut().reserve(bytes)?;
    matching::confirmed_database(db, cursor, &binding).await?;
    Ok(Restored::Current(binding, work))
}
