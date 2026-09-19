use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "version_residencies")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_row_id: String,
    pub object_id: String,
    pub primary_tier: String,
    pub storage_class: String,
    pub cid: String,
    pub revision: i64,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
