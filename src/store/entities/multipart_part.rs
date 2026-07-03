use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "multipart_parts")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub upload_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub part_number: i32,
    pub cid: String,
    pub size: i64,
    pub etag: String,
    pub uploaded_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
