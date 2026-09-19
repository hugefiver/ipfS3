use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "residency_references")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub owner_kind: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub owner_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub reason: String,
    pub version_row_id: String,
    pub object_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub tier: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
