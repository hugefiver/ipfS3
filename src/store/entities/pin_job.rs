use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_jobs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub operation: String,
    pub provider: String,
    pub cid: String,
    pub lease_id: Option<String>,
    pub target_id: Option<String>,
    pub expected_generation: Option<i64>,
    pub expected_remote_epoch: Option<i64>,
    pub state: String,
    pub attempts: i32,
    pub next_attempt_at: DateTimeUtc,
    pub locked_until: Option<DateTimeUtc>,
    pub submit_phase: Option<String>,
    pub last_error: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
