use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_legacy_migrations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub direction: String,
    pub reverses: Option<String>,
    pub database_scope: String,
    pub db_clock: String,
    pub schema_version: String,
    pub declaration: String,
    pub confirmation: String,
    pub item_count: i32,
}
#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
