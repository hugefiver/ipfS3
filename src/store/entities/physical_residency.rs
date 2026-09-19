use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "physical_residencies")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub tier: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub node_identity: Option<String>,
    pub verification_state: String,
    pub verification_receipt: Option<String>,
    pub verified_at: Option<DateTimeUtc>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
