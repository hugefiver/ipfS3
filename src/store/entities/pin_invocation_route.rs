use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_invocation_routes")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: String,
    pub route: String,
    pub remote_epoch: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
