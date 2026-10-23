//! Frozen import input, separate from frequently updated ownership and progress.
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "import_job_requests")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: String,
    pub source_value: String,
    pub object_content_type: Option<String>,
    pub metadata_json: String,
    pub tags_json: String,
    pub pin_decision_json: Option<String>,
    pub zip_intent_json: Option<String>,
    pub decompress_prefix: Option<String>,
    pub root_capture_json: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::import_job::Entity",
        from = "Column::JobId",
        to = "super::import_job::Column::Id",
        on_delete = "Cascade"
    )]
    Job,
}
impl Related<super::import_job::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Job.def()
    }
}
impl ActiveModelBehavior for ActiveModel {}
