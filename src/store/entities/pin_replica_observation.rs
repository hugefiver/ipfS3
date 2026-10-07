//! Append-only scoped observations, not deserializable live provider proof.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "pin_replica_observations")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub sequence: i64,
    pub job_id: String,
    pub provider: String,
    pub expected_cid: String,
    pub route: String,
    pub remote_epoch: i64,
    pub claim_until: DateTimeUtc,
    pub lease_id: Option<String>,
    pub target_id: Option<String>,
    pub expected_generation: Option<i64>,
    pub phase: String,
    pub resources: String,
    pub outcome: String,
    pub safe_error: Option<String>,
    pub projectable: bool,
    pub observed_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}

pub mod receipt;
mod validation;
