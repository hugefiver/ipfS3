use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_legacy_migration_items")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub ordinal: i32,
    pub table_kind: String,
    pub original_key: String,
    pub before_image: String,
    pub after_image: String,
    pub bindings: String,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
