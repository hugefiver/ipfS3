//! Immutable birth contract. No FK: history must survive ordinary S3 deletion.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "pin_lease_requirements")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub lease_id: String,
    pub owner_object_id: String,
    pub contract: String,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}

pub mod contract;
