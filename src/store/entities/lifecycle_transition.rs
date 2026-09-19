use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "lifecycle_transitions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub action_id: String,
    pub action_kind: String,
    pub bucket: String,
    pub object_key: String,
    pub config_revision: i64,
    pub rule_id: String,
    pub target_version_row_id: String,
    pub target_public_version_id: String,
    pub target_object_id: String,
    pub target_sequence: i64,
    pub source_tier: String,
    pub destination_tier: String,
    pub source_cid: String,
    pub destination_cid: String,
    pub source_residency_revision: i64,
    pub expected_source_node_identity: String,
    pub expected_destination_node_identity: String,
    pub ownership_generation: i64,
    pub checkpoint: String,
    pub verification_receipt: Option<String>,
    pub publication_receipt: Option<String>,
    pub settlement_kind: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub completed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::lifecycle_action::Entity",
        from = "Column::ActionId",
        to = "super::lifecycle_action::Column::Id"
    )]
    Action,
    #[sea_orm(
        belongs_to = "super::bucket::Entity",
        from = "Column::Bucket",
        to = "super::bucket::Column::Name"
    )]
    Bucket,
}

impl Related<super::lifecycle_action::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Action.def()
    }
}

impl Related<super::bucket::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Bucket.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
