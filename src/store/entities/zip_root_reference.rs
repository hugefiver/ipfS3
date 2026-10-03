use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "zip_root_references")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub batch_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub revision: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub epoch: i64,
    #[sea_orm(primary_key, auto_increment = false)]
    pub node_identity: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub tier: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub state: String,
    pub verification_receipt: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
