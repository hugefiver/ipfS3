use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "lifecycle_actions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub idempotency_key: String,
    pub bucket: String,
    pub object_key: String,
    pub config_revision: i64,
    pub rule_id: String,
    pub action_kind: String,
    pub target_version_row_id: String,
    pub target_public_version_id: String,
    pub target_object_id: Option<String>,
    pub target_sequence: i64,
    pub due_at: DateTimeUtc,
    pub state: String,
    pub attempts: i64,
    pub next_attempt_at: DateTimeUtc,
    pub claim_epoch: i64,
    pub lease_until: Option<DateTimeUtc>,
    pub claimed_by: Option<String>,
    pub failure_class: Option<String>,
    pub last_error_redacted: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub finished_at: Option<DateTimeUtc>,
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
