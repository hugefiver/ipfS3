use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "objects")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub cid: String,
    /// Plaintext size in bytes.
    pub size: i64,
    pub content_type: Option<String>,
    pub etag: String,
    pub metadata: Option<Json>,
    pub encrypted: bool,
    pub key_wrap: Option<String>,
    pub multipart: bool,
    pub is_latest: bool,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::bucket::Entity",
        from = "Column::Bucket",
        to = "super::bucket::Column::Name"
    )]
    Bucket,
}

impl Related<super::bucket::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Bucket.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
