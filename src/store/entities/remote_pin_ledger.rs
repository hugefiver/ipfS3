use sea_orm::entity::prelude::*;

/// Companion to remote_pins; the original row continues to own epoch and quota.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, serde::Serialize, serde::Deserialize)]
#[sea_orm(table_name = "remote_pin_ledger")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub provider: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub route: Option<String>,
    pub ownership: String,
    pub effect: String,
    pub first_observed_at: Option<DateTimeUtc>,
    pub last_observed_at: Option<DateTimeUtc>,
    pub remote_pinned_at: Option<DateTimeUtc>,
    pub gateway_verified_at: Option<DateTimeUtc>,
    pub content_verified_at: Option<DateTimeUtc>,
    pub first_error: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
