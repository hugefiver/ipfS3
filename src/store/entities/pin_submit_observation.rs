use sea_orm::entity::prelude::*;

/// Deliberately has no job/lease/remote FK: terminal work is not resource evidence.
#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_submit_observations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub job_id: String,
    pub submit_call: i32,
    pub provider: String,
    pub expected_cid: String,
    pub route: String,
    pub remote_epoch: i64,
    pub claim_until: DateTimeUtc,
    pub effect: String,
    pub resources: String,
    pub outcome: String,
    pub safe_error: Option<String>,
    /// Unknown physical debt; reserved_bytes is only the original logical hold.
    pub needs_attention: bool,
    pub started_at: DateTimeUtc,
    pub observed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
impl ActiveModelBehavior for ActiveModel {}
