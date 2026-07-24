use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "remote_pins")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub provider: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub request_id: Option<String>,
    pub cid_size: i64,
    pub status: String,
    pub epoch: i64,
    pub failure_attempts: i32,
    pub next_retry_at: Option<DateTimeUtc>,
    pub last_failed_request_id: Option<String>,
    pub last_touched_at: DateTimeUtc,
    pub last_error_class: Option<String>,
    pub last_error_text: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
