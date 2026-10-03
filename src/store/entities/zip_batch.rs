use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "zip_batches")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub owner: String,
    pub source: String,
    pub token: String,
    pub fingerprint: String,
    pub bucket: String,
    pub archive_key: String,
    pub input_identity: String,
    pub captured_options: String,
    pub state: String,
    pub manifest_prepared: bool,
    pub source_published: bool,
    pub terminal_result: Option<String>,
    pub root_status: String,
    pub root_error_code: Option<String>,
    pub root_cid: Option<String>,
    pub root_revision: i64,
    pub root_epoch: i64,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
