use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_resource_history")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub provider: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub epoch: i64,
    pub ledger: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
