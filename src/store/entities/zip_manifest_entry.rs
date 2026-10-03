use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "zip_manifest_entries")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub batch_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub path: String,
    pub object_key: Option<String>,
    pub cid: Option<String>,
    pub size: Option<i64>,
    pub version_row_id: Option<String>,
    pub error_code: Option<String>,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
